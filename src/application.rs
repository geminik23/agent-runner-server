//! Transport-independent asynchronous run admission and execution.

use std::{
    collections::HashMap,
    error::Error,
    fmt,
    fs::{File, OpenOptions},
    future::poll_fn,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::Poll,
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::{
    sync::{Notify, Semaphore, mpsc, oneshot, watch},
    task::JoinHandle,
};

use crate::{
    access::{
        AccessError, AuthenticatedClient, ClientAccessControl, LaunchAuthorization, Scope,
        validate_resource_id,
    },
    core::{
        BackendError, CancellationToken, CleanupState, DispatchStatus, EventSink, EventSinkError,
        ExecutionBackend, ExecutionEvent, ExecutionEventKind, ExecutionMetadata, ExecutionOutcome,
        ExecutionRequest, ExecutionResult, SessionDisposition, SessionRef, SessionRequest,
        WarningObservation, WorkingDirectory,
    },
    storage::{
        AdmissionOutcome, EventSummary, Namespace, RunSummary, SqliteStore, StorageError,
        StoredResult, TerminalState,
        service::{Candidate, ClaimedJob, ServiceAdmission, ServiceClock, StopCause},
    },
    workspace::{
        WorkspaceAccess, WorkspaceConfig, WorkspaceOwnership, WorkspacePlan, WorkspaceRegistry,
        journal::JournalKey,
    },
};

/// Limits are provided by trusted server configuration, not by a client request.
#[derive(Clone, Debug)]
pub struct RunLimits {
    pub max_outstanding: usize,
    pub max_workers: usize,
    pub database_queue: usize,
    pub event_buffer: usize,
    pub max_events_per_run: usize,
    pub max_event_bytes: usize,
    pub max_result_bytes: usize,
    pub max_page_size: usize,
    /// The deadline includes both queue waiting and backend execution.
    pub run_timeout: Duration,
    pub cleanup_grace: Duration,
    pub poll_interval: Duration,
}

impl RunLimits {
    fn validate(&self) -> Result<(), ServiceError> {
        if self.max_outstanding == 0
            || self.max_workers == 0
            || self.database_queue == 0
            || self.event_buffer == 0
            || self.max_events_per_run < 3
            || self.max_event_bytes < 256
            || self.max_result_bytes < 128
            || self.max_page_size == 0
            || self.run_timeout.as_millis() == 0
            || self.cleanup_grace.is_zero()
            || self.poll_interval.is_zero()
            || self.run_timeout.as_millis() > i64::MAX as u128
            || std::time::Instant::now()
                .checked_add(self.cleanup_grace)
                .is_none()
            || std::time::Instant::now()
                .checked_add(self.poll_interval)
                .is_none()
        {
            return Err(ServiceError::InvalidConfiguration);
        }
        Ok(())
    }
}

/// Trusted retention for new runs; existing runs keep their previously recorded policy.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionConfig {
    pub result_event_retention_ms: u64,
}

impl RetentionConfig {
    pub(crate) fn milliseconds(&self) -> Result<i64, ServiceError> {
        const TEN_YEARS_MS: u64 = 10 * 365 * 24 * 60 * 60 * 1000;
        if self.result_event_retention_ms == 0 || self.result_event_retention_ms > TEN_YEARS_MS {
            return Err(ServiceError::InvalidConfiguration);
        }
        i64::try_from(self.result_event_retention_ms)
            .map_err(|_| ServiceError::InvalidConfiguration)
    }
}

/// Binds an approved profile to a backend and its native state realm.
#[derive(Clone)]
pub struct BackendProfile {
    pub profile_id: String,
    /// An immutable revision that must change whenever execution configuration changes.
    pub revision: String,
    pub state_realm: String,
    pub backend: Arc<dyn ExecutionBackend>,
}

/// Describes a committed admission without exposing the stored manifest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunReceipt {
    pub run_id: String,
    pub session_id: Option<String>,
    pub metadata: ExecutionMetadata,
    pub replay: bool,
}

/// Describes the durable stop decision without implying cleanup has completed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CancelOutcome {
    CancelRequested,
    TimedOut,
    Interrupted,
    AlreadyTerminal,
}

/// Reports service failures without embedding request bodies or credentials.
#[derive(Debug)]
pub enum ServiceError {
    Access(AccessError),
    Storage(StorageError),
    InvalidConfiguration,
    AlreadyRunning,
    Unavailable,
}

impl fmt::Display for ServiceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Access(error) => write!(formatter, "{error}"),
            Self::Storage(error) => write!(formatter, "{error}"),
            Self::InvalidConfiguration => formatter.write_str("invalid run service configuration"),
            Self::AlreadyRunning => formatter.write_str("run service already owns this database"),
            Self::Unavailable => formatter.write_str("run service unavailable"),
        }
    }
}
impl Error for ServiceError {}
impl From<AccessError> for ServiceError {
    fn from(value: AccessError) -> Self {
        Self::Access(value)
    }
}
impl From<StorageError> for ServiceError {
    fn from(value: StorageError) -> Self {
        Self::Storage(value)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    version: u8,
    profile_revision: String,
    prompt: String,
    runtime: String,
    synthetic: bool,
    scenario: Option<String>,
    state_realm: String,
    resume: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    compiler_revision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    policy_revision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    result_schema: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    workspace: Option<WorkspacePlan>,
    #[serde(default)]
    independent_new_sessions: bool,
}

impl Manifest {
    fn metadata(&self) -> ExecutionMetadata {
        ExecutionMetadata {
            runtime: self.runtime.clone(),
            synthetic: self.synthetic,
            scenario: self.scenario.clone(),
        }
    }
}

type DbTask = Box<dyn FnOnce(&mut SqliteStore) + Send + 'static>;
enum DbMessage {
    Task(DbTask),
    JournalStart(JournalKey, oneshot::Sender<Result<bool, ServiceError>>),
    JournalSettle(JournalKey, oneshot::Sender<Result<(), ServiceError>>),
    Stop,
}
#[derive(Clone)]
struct Database {
    sender: mpsc::Sender<DbMessage>,
}

impl Database {
    async fn call<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut SqliteStore) -> Result<T, ServiceError> + Send + 'static,
    ) -> Result<T, ServiceError> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(DbMessage::Task(Box::new(move |store| {
                let _ = sender.send(operation(store));
            })))
            .await
            .map_err(|_| ServiceError::Unavailable)?;
        receiver.await.map_err(|_| ServiceError::Unavailable)?
    }

    async fn journal_start(&self, key: JournalKey) -> Result<bool, ServiceError> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(DbMessage::JournalStart(key, sender))
            .await
            .map_err(|_| ServiceError::Unavailable)?;
        receiver.await.map_err(|_| ServiceError::Unavailable)?
    }

    async fn journal_settle(&self, key: JournalKey) -> Result<(), ServiceError> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(DbMessage::JournalSettle(key, sender))
            .await
            .map_err(|_| ServiceError::Unavailable)?;
        receiver.await.map_err(|_| ServiceError::Unavailable)?
    }

    async fn stop(&self) -> Result<(), ServiceError> {
        self.sender
            .send(DbMessage::Stop)
            .await
            .map_err(|_| ServiceError::Unavailable)
    }
}

struct Shared {
    database: Database,
    access: Arc<ClientAccessControl>,
    profiles: Arc<HashMap<String, BackendProfile>>,
    workspaces: Option<Arc<WorkspaceRegistry>>,
    limits: RunLimits,
    delivery: Option<crate::delivery::DeliveryConfig>,
    retention_ms: Option<i64>,
    delivery_client: Option<reqwest::Client>,
    wake: Notify,
    changes: watch::Sender<u64>,
    intake: Arc<Semaphore>,
    launch_gate: Arc<tokio::sync::Mutex<()>>,
    active: Mutex<HashMap<(String, String, String), CancellationToken>>,
    shutdown: CancellationToken,
    available: Arc<AtomicBool>,
    failed: Arc<AtomicBool>,
}

/// Owns the workers and the exclusive service guard for one SQLite database.
pub struct RunService {
    shared: Arc<Shared>,
    workers: Vec<JoinHandle<()>>,
    database_thread: Option<thread::JoinHandle<()>>,
}

/// A request handle whose lifetime does not own workers or the database guard.
#[derive(Clone)]
pub struct RunHandle {
    shared: Arc<Shared>,
}

impl RunService {
    /// Opens a file-backed database, recovers prior work, and starts bounded workers.
    pub async fn open(
        path: impl AsRef<Path>,
        access: Arc<ClientAccessControl>,
        profiles: Vec<BackendProfile>,
        limits: RunLimits,
    ) -> Result<Self, ServiceError> {
        Self::open_with_delivery(path, access, profiles, limits, None).await
    }

    /// Opens the service with explicitly configured completion delivery limits and destinations.
    pub async fn open_with_delivery(
        path: impl AsRef<Path>,
        access: Arc<ClientAccessControl>,
        profiles: Vec<BackendProfile>,
        limits: RunLimits,
        delivery: Option<crate::delivery::DeliveryConfig>,
    ) -> Result<Self, ServiceError> {
        Self::open_with_retention(path, access, profiles, limits, delivery, None).await
    }

    /// Opens the service with optional retention pinned independently for each new admission.
    pub async fn open_with_retention(
        path: impl AsRef<Path>,
        access: Arc<ClientAccessControl>,
        profiles: Vec<BackendProfile>,
        limits: RunLimits,
        delivery: Option<crate::delivery::DeliveryConfig>,
        retention: Option<RetentionConfig>,
    ) -> Result<Self, ServiceError> {
        Self::open_with_workspaces(path, access, profiles, limits, delivery, retention, None).await
    }

    /// Opens the service with an explicit trusted workspace registry.
    pub async fn open_with_workspaces(
        path: impl AsRef<Path>,
        access: Arc<ClientAccessControl>,
        profiles: Vec<BackendProfile>,
        limits: RunLimits,
        delivery: Option<crate::delivery::DeliveryConfig>,
        retention: Option<RetentionConfig>,
        workspaces: Option<WorkspaceConfig>,
    ) -> Result<Self, ServiceError> {
        limits.validate()?;
        let workspaces = workspaces
            .map(|config| WorkspaceRegistry::prepare(config, Path::new(".")))
            .transpose()?
            .map(Arc::new);
        let retention_ms = retention
            .as_ref()
            .map(RetentionConfig::milliseconds)
            .transpose()?;
        if delivery.as_ref().is_some_and(|config| {
            !config.validate(access.identifier_limit(), limits.max_event_bytes)
        }) {
            return Err(ServiceError::InvalidConfiguration);
        }
        let delivery_client = delivery
            .as_ref()
            .map(|config| {
                reqwest::Client::builder()
                    .no_proxy()
                    .redirect(reqwest::redirect::Policy::none())
                    .timeout(Duration::from_millis(config.timeout_ms))
                    .build()
                    .map_err(|_| ServiceError::InvalidConfiguration)
            })
            .transpose()?;
        if !access.supports_service_ids() {
            return Err(ServiceError::InvalidConfiguration);
        }
        let mut profile_map = HashMap::new();
        for profile in profiles {
            if profile.profile_id.is_empty()
                || profile.revision.is_empty()
                || profile.state_realm.is_empty()
                || limits.max_events_per_run < profile.backend.minimum_events()
                || profile
                    .backend
                    .cleanup_budget()
                    .is_some_and(|budget| budget >= limits.cleanup_grace)
                || profile.state_realm != profile.backend.state_realm()
                || profile_map
                    .insert(profile.profile_id.clone(), profile)
                    .is_some()
            {
                return Err(ServiceError::InvalidConfiguration);
            }
        }
        if profile_map.is_empty()
            || (workspaces.is_none()
                && profile_map.values().any(|p| p.backend.requires_workspace()))
        {
            return Err(ServiceError::InvalidConfiguration);
        }
        let states = owned_states(&profile_map.values().cloned().collect::<Vec<_>>())?;
        let path = path.as_ref().to_path_buf();
        let registry = workspaces.clone();
        let (store, guard, workspace_guard) = tokio::task::spawn_blocking(move || {
            open_owned_store(&path, registry.as_deref(), &states)
        })
        .await
        .map_err(|_| ServiceError::Unavailable)??;
        let (sender, mut receiver) = mpsc::channel(limits.database_queue);
        let database = Database { sender };
        let available = Arc::new(AtomicBool::new(true));
        let failed = Arc::new(AtomicBool::new(false));
        let launch_gate = Arc::new(tokio::sync::Mutex::new(()));
        let shutdown = CancellationToken::new();
        let actor_available = Arc::clone(&available);
        let actor_failed = Arc::clone(&failed);

        let actor_shutdown = shutdown.clone();
        let database_thread = thread::spawn(move || {
            let _guard = guard;
            let mut workspace_guard = workspace_guard;
            let mut store = store;
            let mut poisoned = false;
            while let Some(message) = receiver.blocking_recv() {
                match message {
                    DbMessage::Task(task) if !poisoned => {
                        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            task(&mut store)
                        }))
                        .is_err()
                        {
                            poisoned = true;
                            actor_available.store(false, Ordering::Release);
                            actor_failed.store(true, Ordering::Release);
                            actor_shutdown.cancel();
                        }
                    }
                    DbMessage::Task(_) => {}
                    DbMessage::JournalStart(key, sender) => {
                        let result = if poisoned {
                            Err(ServiceError::Unavailable)
                        } else {
                            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                workspace_guard
                                    .as_mut()
                                    .ok_or(ServiceError::InvalidConfiguration)?
                                    .start(key, &mut store)
                            }))
                            .unwrap_or(Err(ServiceError::Unavailable))
                        };
                        if result.is_err() {
                            poisoned = true;
                            actor_available.store(false, Ordering::Release);
                            actor_failed.store(true, Ordering::Release);
                            actor_shutdown.cancel();
                        }
                        let _ = sender.send(result);
                    }
                    DbMessage::JournalSettle(key, sender) => {
                        let result = if poisoned {
                            Err(ServiceError::Unavailable)
                        } else {
                            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                workspace_guard
                                    .as_mut()
                                    .ok_or(ServiceError::InvalidConfiguration)?
                                    .settle(&key)
                            }))
                            .unwrap_or(Err(ServiceError::Unavailable))
                        };
                        if result.is_err() {
                            poisoned = true;
                            actor_available.store(false, Ordering::Release);
                            actor_failed.store(true, Ordering::Release);
                            actor_shutdown.cancel();
                        }
                        let _ = sender.send(result);
                    }
                    DbMessage::Stop => break,
                }
            }
        });
        if let Err(error) = database
            .call(move |store| {
                store
                    .service_recover(ServiceClock::Wall)
                    .map_err(Into::into)
            })
            .await
        {
            let _ = database.stop().await;
            let _ = tokio::task::spawn_blocking(move || database_thread.join()).await;
            return Err(error);
        }
        let (changes, _) = watch::channel(0_u64);
        let intake = Arc::new(Semaphore::new(limits.database_queue));
        let shared = Arc::new(Shared {
            database,
            access,
            profiles: Arc::new(profile_map),
            workspaces,
            limits,
            delivery,
            retention_ms,
            delivery_client,
            wake: Notify::new(),
            changes,
            intake,
            launch_gate,
            active: Mutex::new(HashMap::new()),
            shutdown,
            available,
            failed,
        });
        let mut workers: Vec<_> = (0..shared.limits.max_workers)
            .map(|_| supervise(Arc::clone(&shared), false))
            .collect();
        workers.push(supervise(Arc::clone(&shared), true));
        if let Some(config) = &shared.delivery {
            for _ in 0..config.max_workers {
                workers.push(tokio::spawn(delivery_worker(Arc::clone(&shared))));
            }
        }
        Ok(Self {
            shared,
            workers,
            database_thread: Some(database_thread),
        })
    }

    pub(crate) fn minimum_response_bytes(&self) -> Option<usize> {
        self.shared
            .limits
            .max_event_bytes
            .checked_mul(self.shared.limits.max_page_size)
            .map(|n| n.max(self.shared.limits.max_result_bytes))
            .map(|n| {
                n.max(
                    self.shared
                        .delivery
                        .as_ref()
                        .map_or(0, |delivery| delivery.max_payload_bytes),
                )
            })
            .and_then(|n| n.checked_mul(16))
            .and_then(|n| n.checked_add(8192))
    }

    pub(crate) fn access_control(&self) -> Arc<ClientAccessControl> {
        Arc::clone(&self.shared.access)
    }

    pub(crate) fn shutdown_signal(&self) -> CancellationToken {
        self.shared.shutdown.clone()
    }

    /// Creates a request handle without transferring shutdown ownership.
    pub fn handle(&self) -> RunHandle {
        RunHandle {
            shared: Arc::clone(&self.shared),
        }
    }

    /// Stops new admissions and signals workers before transport drain.
    pub fn begin_shutdown(&self) {
        stop_service(&self.shared, false);
    }

    pub async fn start(
        &self,
        client: &AuthenticatedClient,
        body: &[u8],
    ) -> Result<RunReceipt, ServiceError> {
        self.handle().start(client, body).await
    }
    pub async fn get_run(
        &self,
        client: &AuthenticatedClient,
        run_id: &str,
    ) -> Result<Option<RunSummary>, ServiceError> {
        self.handle().get_run(client, run_id).await
    }
    pub async fn get_result(
        &self,
        client: &AuthenticatedClient,
        run_id: &str,
    ) -> Result<Option<StoredResult>, ServiceError> {
        self.handle().get_result(client, run_id).await
    }
    pub async fn get_events(
        &self,
        client: &AuthenticatedClient,
        run_id: &str,
        after: Option<i64>,
        limit: usize,
    ) -> Result<Vec<EventSummary>, ServiceError> {
        self.handle().get_events(client, run_id, after, limit).await
    }
    pub async fn cancel(
        &self,
        client: &AuthenticatedClient,
        run_id: &str,
    ) -> Result<CancelOutcome, ServiceError> {
        self.handle().cancel(client, run_id).await
    }
}

impl RunHandle {
    /// Authenticates the context again and commits a new request or returns its original run.
    pub async fn start(
        &self,
        client: &AuthenticatedClient,
        body: &[u8],
    ) -> Result<RunReceipt, ServiceError> {
        self.ensure_available()?;
        let _intake = Arc::clone(&self.shared.intake)
            .try_acquire_owned()
            .map_err(|_| ServiceError::Unavailable)?;
        if body.len() > self.shared.access.request_limit() {
            return Err(ServiceError::Access(AccessError::InvalidRequest));
        }
        let access = Arc::clone(&self.shared.access);
        let skill_bounds = crate::storage::skills::SkillBounds {
            markdown: access.skill_limit(),
            metadata: access.request_limit(),
            identifier: access.identifier_limit(),
        };
        let profiles = Arc::clone(&self.shared.profiles);
        let workspaces = self.shared.workspaces.clone();
        let client = client.clone();
        let body = body.to_vec();
        let limits = self.shared.limits.clone();
        let delivery = self.shared.delivery.clone();
        let retention_ms = self.shared.retention_ms;
        let available = Arc::clone(&self.shared.available);
        let notify_shared = Arc::clone(&self.shared);
        let outcome = self
            .shared
            .database
            .call(move |store| {
                if !available.load(Ordering::Acquire) {
                    return Err(ServiceError::Unavailable);
                }
                let now = now_ms()?;
                let outcome = access
                    .with_start(
                        &client,
                        &body,
                        |namespace, credential_id, generation, request, authorize| {
                            let normalized = serde_json::to_vec(request)
                                .map_err(|_| AccessError::InvalidRequest)?;
                            if let Some(existing) = store.service_replay(
                                namespace,
                                &request.idempotency_key,
                                &normalized,
                            )? {
                                return Ok(AdmissionOutcome::Existing(existing));
                            }
                            if request.result_schema.is_some()
                                && limits.max_result_bytes
                                    < crate::validation::MIN_VALIDATED_RESULT_BYTES
                            {
                                return Err(AccessError::InvalidRequest);
                            }
                            let approved = authorize()?;
                            let generation = generation.ok_or(AccessError::InvalidConfiguration)?;
                            let profile = profiles
                                .get(approved.profile_id())
                                .ok_or(AccessError::Forbidden)?;
                            let metadata = profile.backend.metadata();
                            if metadata.runtime != approved.runtime()
                                || metadata.runtime.len().saturating_add(
                                    metadata.scenario.as_deref().map_or(0, str::len),
                                ) > limits.max_event_bytes.saturating_sub(128)
                                || (metadata.synthetic
                                    && metadata.scenario.as_deref().is_none_or(str::is_empty))
                            {
                                return Err(AccessError::Forbidden);
                            }
                            let workspace = workspaces
                                .as_ref()
                                .and_then(|registry| registry.get(approved.workspace_id()))
                                .cloned();
                            if (profile.backend.requires_workspace() && workspace.is_none())
                                || workspace.as_ref().is_some_and(|value| {
                                    value.access == WorkspaceAccess::SharedRead
                                        && !profile.backend.supports_shared_read()
                                })
                            {
                                return Err(AccessError::Forbidden);
                            }
                            let independent_new_sessions =
                                profile.backend.independent_new_sessions();
                            let manifest = Manifest {
                                version: 2,
                                profile_revision: profile.revision.clone(),
                                prompt: approved.input().to_owned(),
                                runtime: metadata.runtime.clone(),
                                synthetic: metadata.synthetic,
                                scenario: metadata.scenario.clone(),
                                state_realm: profile.state_realm.clone(),
                                resume: approved.is_resume(),
                                compiler_revision: Some(
                                    crate::storage::skills::COMPILER_REVISION.to_owned(),
                                ),
                                policy_revision: Some(
                                    crate::storage::skills::POLICY_REVISION.to_owned(),
                                ),
                                result_schema: request.result_schema.clone(),
                                workspace: workspace.clone(),
                                independent_new_sessions,
                            };
                            let manifest = serde_json::to_vec(&manifest)
                                .map_err(|_| AccessError::InvalidRequest)?;
                            let timeout = i64::try_from(limits.run_timeout.as_millis())
                                .map_err(|_| AccessError::InvalidConfiguration)?;
                            let deadline_ms = now
                                .checked_add(timeout)
                                .ok_or(AccessError::InvalidConfiguration)?;
                            store
                                .service_admit(&ServiceAdmission {
                                    namespace: namespace.clone(),
                                    idempotency_key: request.idempotency_key.clone(),
                                    normalized_request: normalized,
                                    manifest,
                                    metadata,
                                    credential_id: credential_id.to_owned(),
                                    credential_generation: generation.to_owned(),
                                    profile_id: approved.profile_id().to_owned(),
                                    profile_revision: profile.revision.clone(),
                                    workspace_id: approved.workspace_id().to_owned(),
                                    state_realm: profile.state_realm.clone(),
                                    workspace,
                                    independent_new_sessions,
                                    resume_session_id: approved
                                        .resume_session_id()
                                        .map(str::to_owned),
                                    resume_native_id: approved
                                        .resume_native_id()
                                        .map(str::to_owned),
                                    deadline_ms,
                                    retention_ms,
                                    max_outstanding: limits.max_outstanding,
                                    skills: request.skills.clone(),
                                    may_use_skills: approved.may_use_skills(),
                                    skill_bounds,
                                    hooks: request.hooks.clone(),
                                    may_use_hooks: approved.may_use_hooks(),
                                    destination_grants: approved.destinations().clone(),
                                    delivery,
                                })
                                .map_err(Into::into)
                        },
                    )
                    .map_err(ServiceError::from)?;
                notify_shared.wake.notify_one();
                signal_change(&notify_shared);
                if !available.load(Ordering::Acquire) {
                    return Err(ServiceError::Unavailable);
                }
                Ok(outcome)
            })
            .await?;
        self.shared.wake.notify_one();
        signal_change(&self.shared);
        let (run, replay) = match outcome {
            AdmissionOutcome::Accepted(run) => (run, false),
            AdmissionOutcome::Existing(run) => (run, true),
        };
        Ok(RunReceipt {
            run_id: run.run_id,
            session_id: run.session_id,
            metadata: run.metadata,
            replay,
        })
    }

    /// Reads a namespace-scoped run summary after rechecking run-read permission.
    pub async fn get_run(
        &self,
        client: &AuthenticatedClient,
        run_id: &str,
    ) -> Result<Option<RunSummary>, ServiceError> {
        let _intake = self.reserve_resource_call(run_id)?;
        let access = Arc::clone(&self.shared.access);
        let client = client.clone();
        let run_id = run_id.to_owned();
        let max_bytes = self.shared.limits.max_event_bytes;
        self.shared
            .database
            .call(move |store| {
                access
                    .with_permissions(&client, &[Scope::RunRead], |namespace, limits| {
                        validate_resource_id(&run_id, limits)?;
                        store
                            .service_run_summary(namespace, &run_id, max_bytes)
                            .map_err(Into::into)
                    })
                    .map_err(Into::into)
            })
            .await
    }

    /// Reads a namespace-scoped result after rechecking run-read permission.
    pub async fn get_result(
        &self,
        client: &AuthenticatedClient,
        run_id: &str,
    ) -> Result<Option<StoredResult>, ServiceError> {
        let _intake = self.reserve_resource_call(run_id)?;
        let access = Arc::clone(&self.shared.access);
        let client = client.clone();
        let run_id = run_id.to_owned();
        let max_bytes = self.shared.limits.max_result_bytes;
        let max_metadata_bytes = self.shared.limits.max_event_bytes;
        self.shared
            .database
            .call(move |store| {
                access
                    .with_permissions(&client, &[Scope::RunRead], |namespace, limits| {
                        validate_resource_id(&run_id, limits)?;
                        store
                            .service_result(namespace, &run_id, max_bytes, max_metadata_bytes)
                            .map_err(Into::into)
                    })
                    .map_err(Into::into)
            })
            .await
    }

    /// Reads an ordered bounded page of event summaries.
    pub async fn get_events(
        &self,
        client: &AuthenticatedClient,
        run_id: &str,
        after: Option<i64>,
        limit: usize,
    ) -> Result<Vec<EventSummary>, ServiceError> {
        let _intake = self.reserve_resource_call(run_id)?;
        if limit == 0
            || limit > self.shared.limits.max_page_size
            || after.is_some_and(|value| value < 0)
        {
            return Err(ServiceError::Access(AccessError::InvalidRequest));
        }
        let access = Arc::clone(&self.shared.access);
        let client = client.clone();
        let run_id = run_id.to_owned();
        let max_bytes = self.shared.limits.max_event_bytes;
        self.shared
            .database
            .call(move |store| {
                access
                    .with_permissions(&client, &[Scope::RunRead], |namespace, limits| {
                        validate_resource_id(&run_id, limits)?;
                        store
                            .service_events_page(namespace, &run_id, after, limit, max_bytes)
                            .map_err(Into::into)
                    })
                    .map_err(Into::into)
            })
            .await
    }

    /// Returns the durable stop decision without claiming that cleanup has completed.
    pub async fn cancel(
        &self,
        client: &AuthenticatedClient,
        run_id: &str,
    ) -> Result<CancelOutcome, ServiceError> {
        let _intake = self.reserve_resource_call(run_id)?;
        let access = Arc::clone(&self.shared.access);
        let notify_shared = Arc::clone(&self.shared);
        let client = client.clone();
        let run_id = run_id.to_owned();
        let result = self
            .shared
            .database
            .call(move |store| {
                let gate = notify_shared.launch_gate.blocking_lock();
                let (result, key) = access.with_permissions(
                    &client,
                    &[Scope::RunCancel],
                    |namespace, limits| {
                        validate_resource_id(&run_id, limits)?;
                        let result = store.service_stop(
                            namespace,
                            &run_id,
                            StopCause::Cancelled,
                            ServiceClock::Wall,
                        )?;
                        Ok((result, run_key(namespace, &run_id)))
                    },
                )?;
                if result.is_some() {
                    let active = notify_shared
                        .active
                        .lock()
                        .map_err(|_| ServiceError::Unavailable)?;
                    if let Some(token) = active.get(&key) {
                        token.cancel();
                    }
                }
                drop(gate);
                notify_shared.wake.notify_waiters();
                signal_change(&notify_shared);
                Ok(result)
            })
            .await?;
        Ok(match result {
            Some(StopCause::Cancelled) => CancelOutcome::CancelRequested,
            Some(StopCause::TimedOut) => CancelOutcome::TimedOut,
            Some(StopCause::Interrupted) => CancelOutcome::Interrupted,
            None => CancelOutcome::AlreadyTerminal,
        })
    }
}

impl RunService {
    /// Stops admission, requests active workers to stop, and releases the database guard.
    pub async fn shutdown(mut self) -> Result<(), ServiceError> {
        stop_service(&self.shared, false);
        drop(self.shared.launch_gate.lock().await);
        while let Some(worker) = self.workers.last_mut() {
            let result = worker.await;
            self.workers.pop();
            result.map_err(|_| ServiceError::Unavailable)?;
        }
        self.shared.database.stop().await?;
        if let Some(handle) = self.database_thread.take() {
            tokio::task::spawn_blocking(move || handle.join())
                .await
                .map_err(|_| ServiceError::Unavailable)?
                .map_err(|_| ServiceError::Unavailable)?;
        }
        if self.shared.failed.load(Ordering::Acquire) {
            Err(ServiceError::Unavailable)
        } else {
            Ok(())
        }
    }
}

impl RunHandle {
    /// Lists only current grants backed by an available execution profile.
    pub fn allowed_profiles(
        &self,
        client: &AuthenticatedClient,
    ) -> Result<Vec<(crate::access::ProfileGrant, ExecutionMetadata)>, ServiceError> {
        self.ensure_available()?;
        self.shared
            .access
            .allowed_profiles(client)?
            .into_iter()
            .map(|grant| {
                let profile = self
                    .shared
                    .profiles
                    .get(&grant.profile_id)
                    .ok_or(ServiceError::InvalidConfiguration)?;
                Ok((grant, profile.backend.metadata()))
            })
            .collect()
    }

    /// Reads result availability without a race between independent status and result calls.
    pub async fn get_result_view(
        &self,
        client: &AuthenticatedClient,
        run_id: &str,
    ) -> Result<crate::storage::ResultView, ServiceError> {
        let _intake = self.reserve_resource_call(run_id)?;
        let access = Arc::clone(&self.shared.access);
        let client = client.clone();
        let run_id = run_id.to_owned();
        let max_bytes = self.shared.limits.max_result_bytes;
        let max_metadata_bytes = self.shared.limits.max_event_bytes;
        self.shared
            .database
            .call(move |store| {
                access
                    .with_permissions(&client, &[Scope::RunRead], |namespace, limits| {
                        validate_resource_id(&run_id, limits)?;
                        store
                            .service_result_view(namespace, &run_id, max_bytes, max_metadata_bytes)
                            .map_err(Into::into)
                    })
                    .map_err(Into::into)
            })
            .await
    }

    /// Distinguishes an unknown run from an empty event page in the same snapshot.
    pub async fn get_event_page(
        &self,
        client: &AuthenticatedClient,
        run_id: &str,
        after: Option<i64>,
        limit: usize,
    ) -> Result<Option<Vec<EventSummary>>, ServiceError> {
        let _intake = self.reserve_resource_call(run_id)?;
        if limit == 0 || limit > self.shared.limits.max_page_size || after.is_some_and(|v| v < 0) {
            return Err(AccessError::InvalidRequest.into());
        }
        let access = Arc::clone(&self.shared.access);
        let client = client.clone();
        let run_id = run_id.to_owned();
        let max_bytes = self.shared.limits.max_event_bytes;
        self.shared
            .database
            .call(move |store| {
                access
                    .with_permissions(&client, &[Scope::RunRead], |namespace, limits| {
                        validate_resource_id(&run_id, limits)?;
                        store
                            .service_event_view(namespace, &run_id, after, limit, max_bytes)
                            .map_err(Into::into)
                    })
                    .map_err(Into::into)
            })
            .await
    }

    fn reserve_resource_call(
        &self,
        run_id: &str,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, ServiceError> {
        if run_id.is_empty() || run_id.len() > self.shared.access.identifier_limit() {
            return Err(ServiceError::Access(AccessError::InvalidRequest));
        }
        Arc::clone(&self.shared.intake)
            .try_acquire_owned()
            .map_err(|_| ServiceError::Unavailable)
    }

    fn ensure_available(&self) -> Result<(), ServiceError> {
        if self.shared.available.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(ServiceError::Unavailable)
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SkillRegisterArgs {
    skill_id: String,
    version: String,
    markdown: String,
    #[serde(default)]
    required_capabilities: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SkillListArgs {
    limit: usize,
    #[serde(default)]
    after: Option<crate::storage::SkillRef>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SkillEnabledArgs {
    skill_id: String,
    version: String,
    enabled: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SkillDefaultsArgs {
    skills: Vec<crate::storage::SkillRef>,
}

impl RunHandle {
    pub(crate) fn max_skill_page_size(&self) -> usize {
        self.shared
            .limits
            .max_page_size
            .min(crate::storage::skills::MAX_SKILLS)
    }

    pub(crate) async fn skill_operation(
        &self,
        client: &AuthenticatedClient,
        operation: &str,
        args: Value,
        max_response_bytes: usize,
    ) -> Result<Value, ServiceError> {
        self.ensure_available()?;
        let _permit = Arc::clone(&self.shared.intake)
            .try_acquire_owned()
            .map_err(|_| ServiceError::Unavailable)?;
        let request_bytes = serde_json::to_vec(&args)
            .map_err(|_| AccessError::InvalidRequest)?
            .len();
        if request_bytes > self.shared.access.request_limit()
            || (operation == "skills_register"
                && request_bytes
                    .checked_mul(16)
                    .and_then(|v| v.checked_add(8192))
                    .is_none_or(|v| v > max_response_bytes))
        {
            return Err(AccessError::InvalidRequest.into());
        }
        let access = Arc::clone(&self.shared.access);
        let client = client.clone();
        let operation = operation.to_owned();
        let max_page_size = self.shared.limits.max_page_size;
        self.shared.database.call(move |store| {
            let scopes: &[Scope] = match operation.as_str() {
                "skills_register" | "skills_set_enabled" | "skills_set_defaults" => &[Scope::SkillRead, Scope::SkillWrite],
                "skills_list" | "skills_get" | "skills_defaults" => &[Scope::SkillRead],
                _ => return Err(AccessError::InvalidRequest.into()),
            };
            access.with_permissions(&client, scopes, |ns, limits| {
                use crate::storage::{SkillRef, SkillWrite, skills::{MAX_SKILLS, SkillBounds, validate_refs}};
                let bounds = SkillBounds { markdown: limits.max_skill_bytes, metadata: limits.max_request_bytes, identifier: limits.max_identifier_bytes };
                let invalid = |_| AccessError::InvalidRequest;
                match operation.as_str() {
                    "skills_register" => {
                        let mut input: SkillRegisterArgs = serde_json::from_value(args).map_err(invalid)?;
                        let reference = SkillRef { skill_id: input.skill_id, version: input.version };
                        validate_refs(std::slice::from_ref(&reference), limits.max_identifier_bytes)?;
                        if input.markdown.is_empty() || input.markdown.len() > limits.max_skill_bytes
                            || input.required_capabilities.len() > MAX_SKILLS
                            || input.required_capabilities.iter().any(|c| c.is_empty() || c.len() > limits.max_identifier_bytes || !c.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))) {
                            return Err(AccessError::InvalidRequest);
                        }
                        input.required_capabilities.sort();
                        if input.required_capabilities.windows(2).any(|v| v[0] == v[1]) { return Err(AccessError::InvalidRequest); }
                        let result = store.register_skill(ns, &reference, &input.markdown, &input.required_capabilities, bounds)?;
                        Ok(json!({"skill_id":reference.skill_id,"version":reference.version,"created":result == SkillWrite::Inserted}))
                    }
                    "skills_get" => {
                        let reference: SkillRef = serde_json::from_value(args).map_err(invalid)?;
                        validate_refs(std::slice::from_ref(&reference), limits.max_identifier_bytes)?;
                        let record = store.skill(ns, &reference, bounds)?.ok_or(StorageError::NotFound)?;
                        if record.markdown.len() > limits.max_skill_bytes { return Err(AccessError::Storage(StorageError::InvalidInput("skill limit"))); }
                        serde_json::to_value(record).map_err(invalid)
                    }
                    "skills_list" => {
                        let input: SkillListArgs = serde_json::from_value(args).map_err(invalid)?;
                        if input.limit == 0 || input.limit > max_page_size || input.limit > MAX_SKILLS { return Err(AccessError::InvalidRequest); }
                        if let Some(after) = &input.after { validate_refs(std::slice::from_ref(after), limits.max_identifier_bytes)?; }
                        let items = store.list_skills(ns, input.after.as_ref(), input.limit, bounds.identifier)?;
                        let next_after = items.last().map(|s| json!({"skill_id":s.skill_id,"version":s.version}));
                        Ok(json!({"items":items,"next_after":next_after}))
                    }
                    "skills_set_enabled" => {
                        let input: SkillEnabledArgs = serde_json::from_value(args).map_err(invalid)?;
                        let reference = SkillRef { skill_id: input.skill_id, version: input.version };
                        validate_refs(std::slice::from_ref(&reference), limits.max_identifier_bytes)?;
                        store.set_skill_enabled(ns, &reference, input.enabled)?;
                        Ok(json!({"skill_id":reference.skill_id,"version":reference.version,"enabled":input.enabled}))
                    }
                    "skills_defaults" => {
                        let empty: std::collections::HashMap<String, Value> = serde_json::from_value(args).map_err(invalid)?;
                        if !empty.is_empty() { return Err(AccessError::InvalidRequest); }
                        Ok(json!({"skills":store.skill_defaults(ns, bounds)?}))
                    }
                    "skills_set_defaults" => {
                        let input: SkillDefaultsArgs = serde_json::from_value(args).map_err(invalid)?;
                        validate_refs(&input.skills, limits.max_identifier_bytes)?;
                        store.set_skill_defaults(ns, &input.skills, bounds)?;
                        Ok(json!({"skills":input.skills}))
                    }
                    _ => Err(AccessError::InvalidRequest),
                }
            }).map_err(Into::into)
        }).await
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HookRegisterArgs {
    hook_id: String,
    version: String,
    destination_id: String,
    events: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HookEnabledArgs {
    hook_id: String,
    version: String,
    enabled: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HookDefaultsArgs {
    hooks: Vec<crate::storage::HookRef>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HookListArgs {
    limit: usize,
    #[serde(default)]
    after: Option<crate::storage::HookRef>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InboxPageArgs {
    limit: usize,
    #[serde(default)]
    after: Option<i64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InboxAckArgs {
    inbox_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeliveryReconcileArgs {
    delivery_id: String,
    outcome: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeliveryPageArgs {
    run_id: String,
    limit: usize,
    #[serde(default)]
    after: Option<String>,
}

impl RunHandle {
    pub(crate) async fn completion_operation(
        &self,
        client: &AuthenticatedClient,
        operation: &str,
        args: Value,
        max_response_bytes: usize,
    ) -> Result<Value, ServiceError> {
        self.ensure_available()?;
        let _permit = Arc::clone(&self.shared.intake)
            .try_acquire_owned()
            .map_err(|_| ServiceError::Unavailable)?;
        if serde_json::to_vec(&args)
            .map_err(|_| AccessError::InvalidRequest)?
            .len()
            > self.shared.access.request_limit()
        {
            return Err(AccessError::InvalidRequest.into());
        }
        let access = Arc::clone(&self.shared.access);
        let client = client.clone();
        let operation = operation.to_owned();
        let config = self.shared.delivery.clone();
        let max_page = self.max_skill_page_size();
        self.shared.database.call(move |store| {
            let scopes: &[Scope] = match operation.as_str() {
                "hooks_register" | "hooks_set_enabled" | "hooks_set_defaults" => &[Scope::HookRead, Scope::HookWrite],
                "hooks_list" | "hooks_get" | "hooks_defaults" | "destinations_list" => &[Scope::HookRead],
                "inbox_list" => &[Scope::InboxRead],
                "inbox_ack" => &[Scope::InboxAck],
                "runs_deliveries" => &[Scope::RunRead, Scope::HookRead],
                "delivery_reconcile" => &[Scope::RunRead, Scope::HookRead, Scope::DeliveryReconcile],
                _ => return Err(AccessError::InvalidRequest.into()),
            };
            access.with_hook_permissions(&client, scopes, |ns, limits, grants, reconciliation_grants, actor, generation| {
                use crate::storage::{HookRef, hooks::{MAX_HOOKS, validate_events, validate_refs}};
                let invalid = |_| AccessError::InvalidRequest;
                match operation.as_str() {
                    "hooks_register" => {
                        let mut input: HookRegisterArgs = serde_json::from_value(args).map_err(invalid)?;
                        let reference = HookRef { hook_id: input.hook_id, version: input.version };
                        validate_refs(std::slice::from_ref(&reference), limits.max_identifier_bytes)?;
                        validate_events(&mut input.events)?;
                        if !grants.contains(&input.destination_id) { return Err(AccessError::Forbidden); }
                        let destination = config.as_ref().and_then(|c| c.destination(&input.destination_id)).ok_or(StorageError::HookUnavailable)?;
                        let created = store.register_hook(ns, &reference, destination, &input.events, limits.max_identifier_bytes)?;
                        Ok(json!({"hook_id":reference.hook_id,"version":reference.version,"created":created}))
                    }
                    "hooks_get" => {
                        let reference: HookRef = serde_json::from_value(args).map_err(invalid)?;
                        validate_refs(std::slice::from_ref(&reference), limits.max_identifier_bytes)?;
                        serde_json::to_value(store.hook(ns,&reference,limits.max_identifier_bytes)?.ok_or(StorageError::NotFound)?).map_err(invalid)
                    }
                    "hooks_list" => {
                        let input: HookListArgs = serde_json::from_value(args).map_err(invalid)?;
                        if input.limit == 0 || input.limit > max_page || input.limit > MAX_HOOKS { return Err(AccessError::InvalidRequest); }
                        if let Some(after) = &input.after { validate_refs(std::slice::from_ref(after),limits.max_identifier_bytes)?; }
                        let items = store.list_hooks(ns,input.after.as_ref(),input.limit,limits.max_identifier_bytes)?;
                        let next_after = items.last().map(|h|json!({"hook_id":h.hook_id,"version":h.version}));
                        Ok(json!({"items":items,"next_after":next_after}))
                    }
                    "hooks_set_enabled" => {
                        let input: HookEnabledArgs = serde_json::from_value(args).map_err(invalid)?;
                        let reference = HookRef {hook_id:input.hook_id,version:input.version};
                        validate_refs(std::slice::from_ref(&reference),limits.max_identifier_bytes)?;
                        store.set_hook_enabled(ns,&reference,input.enabled)?;
                        Ok(json!({"hook_id":reference.hook_id,"version":reference.version,"enabled":input.enabled}))
                    }
                    "hooks_defaults" => {
                        let empty: std::collections::HashMap<String,Value> = serde_json::from_value(args).map_err(invalid)?;
                        if !empty.is_empty() { return Err(AccessError::InvalidRequest); }
                        Ok(json!({"hooks":store.hook_defaults(ns,limits.max_identifier_bytes)?}))
                    }
                    "hooks_set_defaults" => {
                        let input: HookDefaultsArgs = serde_json::from_value(args).map_err(invalid)?;
                        validate_refs(&input.hooks,limits.max_identifier_bytes)?;
                        store.set_hook_defaults(ns,&input.hooks,limits.max_identifier_bytes)?;
                        Ok(json!({"hooks":input.hooks}))
                    }
                    "destinations_list" => {
                        let empty: std::collections::HashMap<String,Value> = serde_json::from_value(args).map_err(invalid)?;
                        if !empty.is_empty() { return Err(AccessError::InvalidRequest); }
                        let items: Vec<_> = config.as_ref().into_iter().flat_map(|c|&c.destinations).filter(|d|d.enabled && grants.contains(&d.id)).map(|d|json!({"id":d.id,"revision":d.revision,"kind":d.kind})).collect();
                        Ok(json!({"items":items}))
                    }
                    "inbox_list" => {
                        let input: InboxPageArgs = serde_json::from_value(args).map_err(invalid)?;
                        if input.limit == 0 || input.limit > max_page || input.after.is_some_and(|n|n<0) { return Err(AccessError::InvalidRequest); }
                        let now = now_ms().map_err(|_|AccessError::Unavailable)?;
                        let items = store.completion_inbox_page(ns,input.after.unwrap_or(0),input.limit,now,max_response_bytes.saturating_sub(8192)/16)?;
                        let next_after = items.last().and_then(|v|v["sequence"].as_i64()).or(input.after);
                        let legacy_unmanaged = store.legacy_inbox_count(ns)?;
                        Ok(json!({"items":items,"next_after":next_after,"legacy_unmanaged":legacy_unmanaged}))
                    }
                    "inbox_ack" => {
                        let input: InboxAckArgs = serde_json::from_value(args).map_err(invalid)?;
                        validate_resource_id(&input.inbox_id, limits)?;
                        let now = now_ms().map_err(|_|AccessError::Unavailable)?;
                        store.acknowledge_completion_inbox(ns,&input.inbox_id,now)?;
                        Ok(json!({"acknowledged":true}))
                    }
                    "delivery_reconcile" => {
                        let input: DeliveryReconcileArgs = serde_json::from_value(args).map_err(invalid)?;
                        validate_resource_id(&input.delivery_id, limits)?;
                        let reconciliation = crate::storage::inbox::DeliveryReconciliation { id:&input.delivery_id, actor, generation:generation.ok_or(AccessError::InvalidConfiguration)?, outcome:&input.outcome, grants:reconciliation_grants };
                        store.reconcile_delivery(ns,&reconciliation,ServiceClock::Wall)?;
                        Ok(json!({"reconciled":true}))
                    }
                    "runs_deliveries" => {
                        let input: DeliveryPageArgs = serde_json::from_value(args).map_err(invalid)?;
                        validate_resource_id(&input.run_id, limits)?;
                        if let Some(after) = &input.after { validate_resource_id(after, limits)?; }
                        if input.limit == 0 || input.limit > max_page { return Err(AccessError::InvalidRequest); }
                        let items = store.completion_deliveries(ns,&input.run_id,input.after.as_deref(),input.limit)?.ok_or(StorageError::NotFound)?;
                        let next_after = items.last().and_then(|v|v["delivery_id"].as_str()).map(str::to_owned).or(input.after);
                        let legacy_unmanaged = store.legacy_delivery_count(ns,&input.run_id)?;
                        Ok(json!({"items":items,"next_after":next_after,"legacy_unmanaged":legacy_unmanaged}))
                    }
                    _ => Err(AccessError::InvalidRequest),
                }
            }).map_err(Into::into)
        }).await
    }
}

impl Drop for RunService {
    fn drop(&mut self) {
        stop_service(&self.shared, false);
        if let Some(database_thread) = self.database_thread.take() {
            let workers = std::mem::take(&mut self.workers);
            let database = self.shared.database.clone();
            thread::spawn(move || {
                if let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                    .enable_time()
                    .build()
                {
                    runtime.block_on(async move {
                        for worker in workers {
                            let _ = worker.await;
                        }
                        let _ = database.stop().await;
                    });
                } else {
                    let _ = database.sender.blocking_send(DbMessage::Stop);
                }
                let _ = database_thread.join();
            });
        }
    }
}

/// Takes the database lock and every workspace and native state owner, then reconciles their journals.
/// The daemon and offline maintenance share this path so neither can run while the other holds the state.
pub(crate) fn open_owned_store(
    path: &Path,
    registry: Option<&WorkspaceRegistry>,
    states: &[(String, PathBuf, PathBuf)],
) -> Result<(SqliteStore, File, Option<WorkspaceOwnership>), ServiceError> {
    let (guard, canonical) = lock_database(path)?;
    let readonly = SqliteStore::open_read_only(&canonical)?;
    let mut owner = if let Some(registry) = registry {
        let owner = registry.acquire_owner(&canonical, states, &readonly)?;
        owner.verify(&readonly)?;
        Some(owner)
    } else {
        if readonly.has_unsettled_workspace_jobs()? {
            return Err(ServiceError::InvalidConfiguration);
        }
        None
    };
    drop(readonly);
    let mut store = open_locked_store(&canonical, &guard)?;
    if let Some(owner) = &mut owner {
        owner.reconcile(&mut store)?;
    }
    Ok((store, guard, owner))
}

/// Collects the native state directories whose owners must be held with the database.
pub(crate) fn owned_states(
    profiles: &[BackendProfile],
) -> Result<Vec<(String, PathBuf, PathBuf)>, ServiceError> {
    let mut states = Vec::new();
    for profile in profiles {
        if let Some(path) = profile.backend.state_directory() {
            let root = profile
                .backend
                .state_owner_root()
                .ok_or(ServiceError::InvalidConfiguration)?;
            states.push((
                profile.state_realm.clone(),
                path.to_path_buf(),
                root.to_path_buf(),
            ));
        }
    }
    Ok(states)
}

fn lock_database(path: &Path) -> Result<(File, PathBuf), ServiceError> {
    let database_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .map_err(|_| ServiceError::Unavailable)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if database_file
            .metadata()
            .map_err(|_| ServiceError::Unavailable)?
            .nlink()
            != 1
        {
            return Err(ServiceError::InvalidConfiguration);
        }
    }
    database_file
        .try_lock()
        .map_err(|_| ServiceError::AlreadyRunning)?;
    let canonical = path.canonicalize().map_err(|_| ServiceError::Unavailable)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let locked = database_file
            .metadata()
            .map_err(|_| ServiceError::Unavailable)?;
        let named = std::fs::metadata(&canonical).map_err(|_| ServiceError::Unavailable)?;
        if (locked.dev(), locked.ino()) != (named.dev(), named.ino()) {
            return Err(ServiceError::InvalidConfiguration);
        }
    }
    Ok((database_file, canonical))
}

fn open_locked_store(path: &Path, guard: &File) -> Result<SqliteStore, ServiceError> {
    #[cfg(unix)]
    let identity = {
        use std::os::unix::fs::MetadataExt;
        let locked = guard.metadata().map_err(|_| ServiceError::Unavailable)?;
        let named = std::fs::metadata(path).map_err(|_| ServiceError::Unavailable)?;
        if (locked.dev(), locked.ino()) != (named.dev(), named.ino()) {
            return Err(ServiceError::InvalidConfiguration);
        }
        (locked.dev(), locked.ino())
    };
    let store = SqliteStore::open(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let named = std::fs::metadata(path).map_err(|_| ServiceError::Unavailable)?;
        if identity != (named.dev(), named.ino()) {
            return Err(ServiceError::InvalidConfiguration);
        }
    }
    Ok(store)
}

fn now_ms() -> Result<i64, ServiceError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ServiceError::Unavailable)?;
    i64::try_from(duration.as_millis()).map_err(|_| ServiceError::Unavailable)
}

fn run_key(namespace: &Namespace, run_id: &str) -> (String, String, String) {
    (
        namespace.owner_id.clone(),
        namespace.client_id.clone(),
        run_id.to_owned(),
    )
}

struct AbortOnDrop<T>(JoinHandle<T>);
impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct ActiveExecution {
    shared: Arc<Shared>,
    key: (String, String, String),
    cancellation: CancellationToken,
}
impl Drop for ActiveExecution {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Ok(mut active) = self.shared.active.lock() {
            active.remove(&self.key);
        }
    }
}

fn signal_change(shared: &Shared) {
    shared
        .changes
        .send_modify(|value| *value = value.wrapping_add(1));
}

fn supervise(shared: Arc<Shared>, maintenance: bool) -> JoinHandle<()> {
    tokio::spawn(async move {
        let task_shared = Arc::clone(&shared);
        let task = tokio::spawn(async move {
            if maintenance {
                maintain_deadlines(task_shared).await;
            } else {
                worker(task_shared).await;
            }
        });
        if task.await.is_err() {
            fail_service(&shared);
        }
    })
}

async fn maintain_deadlines(shared: Arc<Shared>) {
    let mut changes = shared.changes.subscribe();
    while !shared.shutdown.is_cancelled() {
        let retention_shared = Arc::clone(&shared);
        let retention = shared
            .database
            .call(
                move |store| match store.run_retention_maintenance(ServiceClock::Wall) {
                    Ok(()) => Ok(()),
                    Err(error) => {
                        fail_service(&retention_shared);
                        Err(error.into())
                    }
                },
            )
            .await;
        if retention.is_err() {
            fail_service(&shared);
            return;
        }
        if shared.delivery.is_none() {
            let maintenance = shared
                .database
                .call(move |store| {
                    store.completion_disable(ServiceClock::Wall)?;
                    store
                        .completion_maintenance(ServiceClock::Wall)
                        .map_err(Into::into)
                })
                .await;
            if maintenance.is_err() {
                tracing::warn!("completion retention maintenance is unavailable");
            }
        }
        let next = match shared
            .database
            .call(move |store| {
                store
                    .service_deadline_maintenance(ServiceClock::Wall)
                    .map_err(Into::into)
            })
            .await
        {
            Ok(next) => next,
            Err(_) => {
                fail_service(&shared);
                return;
            }
        };
        let now = match now_ms() {
            Ok(now) => now,
            Err(_) => {
                fail_service(&shared);
                return;
            }
        };
        let wait = next
            .map(|deadline| Duration::from_millis(deadline.saturating_sub(now).max(0) as u64))
            .unwrap_or(shared.limits.poll_interval)
            .min(shared.limits.poll_interval);
        tokio::select! {
            _ = shared.shutdown.cancelled() => return,
            _ = changes.changed() => {},
            _ = tokio::time::sleep(wait) => {},
        }
    }
}

async fn delivery_worker(shared: Arc<Shared>) {
    while !shared.shutdown.is_cancelled() && shared.available.load(Ordering::Acquire) {
        let inner = Arc::clone(&shared);
        if matches!(
            tokio::spawn(async move { run_delivery_worker(&inner).await }).await,
            Ok(Ok(()))
        ) {
            break;
        }
        tracing::warn!(
            "completion delivery worker will retry after a storage or configuration error"
        );
        let delay = shared
            .delivery
            .as_ref()
            .map_or(shared.limits.poll_interval, |config| {
                Duration::from_millis(config.poll_ms)
            });
        tokio::select! {
            _ = shared.shutdown.cancelled() => break,
            _ = tokio::time::sleep(delay) => {},
        }
    }
}

async fn run_delivery_worker(shared: &Arc<Shared>) -> Result<(), ServiceError> {
    use crate::storage::dispatch::Observation;
    let config = shared
        .delivery
        .as_ref()
        .ok_or(ServiceError::InvalidConfiguration)?
        .clone();
    let client = shared
        .delivery_client
        .as_ref()
        .ok_or(ServiceError::InvalidConfiguration)?
        .clone();
    let lease = i64::try_from(config.lease_ms).map_err(|_| ServiceError::InvalidConfiguration)?;
    let retry = i64::try_from(config.retry_ms).map_err(|_| ServiceError::InvalidConfiguration)?;
    while !shared.shutdown.is_cancelled() {
        let access = Arc::clone(&shared.access);
        let inbox_config = config.clone();
        let processed_inbox = shared
            .database
            .call(move |store| {
                store.completion_maintenance(ServiceClock::Wall)?;
                let Some(item) = store.pending_inbox()? else {
                    return Ok(false);
                };
                if inbox_config.destination(&item.destination.id) != Some(&item.destination) {
                    store.settle_pending_inbox(
                        &item,
                        Some("destination_unavailable"),
                        ServiceClock::Wall,
                    )?;
                    return Ok(true);
                }
                match access.with_delivery_authorization(
                    &item.credential_id,
                    &item.generation,
                    &item.namespace,
                    &item.destination.id,
                    || {
                        store
                            .settle_pending_inbox(&item, None, ServiceClock::Wall)
                            .map_err(Into::into)
                    },
                ) {
                    Ok(()) => Ok(true),
                    Err(AccessError::Forbidden | AccessError::Unauthenticated) => {
                        store.settle_pending_inbox(
                            &item,
                            Some("authorization_revoked"),
                            ServiceClock::Wall,
                        )?;
                        Ok(true)
                    }
                    Err(error) => Err(error.into()),
                }
            })
            .await?;
        if processed_inbox {
            continue;
        }
        let job = shared
            .database
            .call(move |store| {
                store.completion_maintenance(ServiceClock::Wall)?;
                store
                    .completion_claim(ServiceClock::Wall, lease)
                    .map_err(Into::into)
            })
            .await?;
        let Some(job) = job else {
            tokio::select! {
                _ = shared.shutdown.cancelled() => break,
                _ = shared.wake.notified() => {},
                _ = tokio::time::sleep(Duration::from_millis(config.poll_ms)) => {},
            }
            continue;
        };
        let valid_destination = config.destination(&job.destination.id) == Some(&job.destination)
            && job.synthetic
            && job.destination.endpoint.is_some();
        let access = Arc::clone(&shared.access);
        let check = job.clone();
        let ready = shared
            .database
            .call(move |store| {
                if !valid_destination {
                    store.completion_skip(&check, ServiceClock::Wall, "destination_unavailable")?;
                    return Ok(false);
                }
                match access.with_delivery_authorization(
                    &check.credential_id,
                    &check.credential_generation,
                    &check.namespace,
                    &check.destination.id,
                    || {
                        store
                            .completion_begin_dispatch(&check, ServiceClock::Wall)
                            .map_err(Into::into)
                    },
                ) {
                    Ok(true) => Ok(true),
                    Ok(false) => {
                        store.completion_skip(
                            &check,
                            ServiceClock::Wall,
                            "hook_or_lease_unavailable",
                        )?;
                        Ok(false)
                    }
                    Err(AccessError::Forbidden | AccessError::Unauthenticated) => {
                        store.completion_skip(
                            &check,
                            ServiceClock::Wall,
                            "authorization_revoked",
                        )?;
                        Ok(false)
                    }
                    Err(error) => Err(error.into()),
                }
            })
            .await?;
        if !ready {
            continue;
        }
        let endpoint = job
            .destination
            .endpoint
            .as_ref()
            .ok_or(ServiceError::InvalidConfiguration)?;
        let mut request = client
            .post(endpoint)
            .header("content-type", "application/json")
            .header("x-delivery-id", &job.id)
            .body(job.payload.clone());
        if job.destination.kind == crate::delivery::DestinationKind::NotionMock {
            request = request.header("notion-version", "2026-03-11");
        }
        let observation = tokio::select! {
            _ = shared.shutdown.cancelled() => Observation::Ambiguous,
            response = request.send() => match response {
                Ok(response) if response.status().is_success()
                    && (job.destination.kind != crate::delivery::DestinationKind::NotionMock || response.status().as_u16() == 200)
                    => Observation::Delivered,
                Err(error) if error.is_connect() => Observation::NotDispatched,
                _ => Observation::Ambiguous,
            }
        };
        shared
            .database
            .call(move |store| {
                store
                    .completion_observe(&job, ServiceClock::Wall, retry, observation)
                    .map_err(Into::into)
            })
            .await?;
    }
    Ok(())
}

async fn worker(shared: Arc<Shared>) {
    while !shared.shutdown.is_cancelled() {
        let job = match shared
            .database
            .call(move |store| store.service_claim(ServiceClock::Wall).map_err(Into::into))
            .await
        {
            Ok(job) => job,
            Err(_) => {
                fail_service(&shared);
                break;
            }
        };
        if let Some(job) = job {
            if process_job(&shared, job).await.is_err() {
                fail_service(&shared);
                break;
            }
        } else {
            tokio::select! {
                _ = shared.shutdown.cancelled() => break,
                _ = shared.wake.notified() => {},
                _ = tokio::time::sleep(shared.limits.poll_interval) => {},
            }
        }
    }
}

fn stop_service(shared: &Shared, failed: bool) {
    shared.available.store(false, Ordering::Release);
    if failed {
        shared.failed.store(true, Ordering::Release);
    }
    shared.shutdown.cancel();
    if let Ok(active) = shared.active.lock() {
        for cancellation in active.values() {
            cancellation.cancel();
        }
    }
}

fn fail_service(shared: &Shared) {
    stop_service(shared, true);
}

async fn finish_prelaunch(
    shared: &Shared,
    job: &ClaimedJob,
    reason: &'static str,
) -> Result<(), ServiceError> {
    finish_prelaunch_with_cleanup(shared, job, reason, CleanupState::Settled).await
}

async fn finish_prelaunch_with_cleanup(
    shared: &Shared,
    job: &ClaimedJob,
    reason: &'static str,
    cleanup: CleanupState,
) -> Result<(), ServiceError> {
    let job = job.clone();
    let limit = shared.limits.max_result_bytes;
    let shutdown = shared.shutdown.clone();
    shared
        .database
        .call(move |store| {
            if shutdown.is_cancelled() {
                store.service_stop(
                    &job.namespace,
                    &job.run_id,
                    StopCause::Interrupted,
                    ServiceClock::Wall,
                )?;
            }
            store
                .service_finish(
                    &job,
                    &Candidate {
                        state: TerminalState::Failed,
                        details: json!({"error": reason}),
                        cleanup,
                        disposition: if cleanup == CleanupState::Settled {
                            SessionDisposition::NotApplicable
                        } else {
                            SessionDisposition::Uncertain
                        },
                    },
                    ServiceClock::Wall,
                    limit,
                )
                .map_err(Into::into)
        })
        .await
}

async fn finish_journaled_prelaunch(
    shared: &Shared,
    job: &ClaimedJob,
    reason: &'static str,
    journal: &Option<JournalKey>,
) -> Result<(), ServiceError> {
    finish_prelaunch(shared, job, reason).await?;
    if let Some(key) = journal {
        shared.database.journal_settle(key.clone()).await?;
    }
    Ok(())
}

async fn job_stopped(shared: &Shared, job: &ClaimedJob) -> Result<bool, ServiceError> {
    let job = job.clone();
    shared
        .database
        .call(move |store| {
            store
                .service_status(&job, ServiceClock::Wall)
                .map(|status| status.is_some())
                .map_err(Into::into)
        })
        .await
}

async fn preflight_backend(
    shared: &Shared,
    job: &ClaimedJob,
    backend: &dyn ExecutionBackend,
) -> Result<bool, ServiceError> {
    let cancellation = shared.shutdown.child_token();
    let preflight = backend.preflight(cancellation.clone());
    tokio::pin!(preflight);
    loop {
        tokio::select! {
            result = &mut preflight => return Ok(result.is_ok()),
            _ = shared.shutdown.cancelled() => { cancellation.cancel(); return Ok(false); },
            _ = tokio::time::sleep(shared.limits.poll_interval) => {
                if job_stopped(shared, job).await? { cancellation.cancel(); return Ok(false); }
            }
        }
    }
}

async fn process_job(shared: &Arc<Shared>, job: ClaimedJob) -> Result<(), ServiceError> {
    let manifest: Manifest = match serde_json::from_slice(&job.manifest) {
        Ok(value) => value,
        Err(_) => return finish_prelaunch(shared, &job, "invalid_execution_snapshot").await,
    };
    let Some(profile) = shared.profiles.get(&job.profile_id).cloned() else {
        return finish_prelaunch(shared, &job, "profile_unavailable").await;
    };
    if manifest
        .result_schema
        .as_ref()
        .is_some_and(|schema| !crate::validation::valid_schema(schema))
    {
        return finish_prelaunch(shared, &job, "invalid_execution_snapshot").await;
    }
    if !matches!(manifest.version, 1 | 2)
        || (manifest.version == 2
            && (manifest.compiler_revision.as_deref()
                != Some(crate::storage::skills::COMPILER_REVISION)
                || manifest.policy_revision.as_deref()
                    != Some(crate::storage::skills::POLICY_REVISION)))
        || manifest.profile_revision != profile.revision
        || manifest.metadata() != job.metadata
        || profile.backend.metadata() != job.metadata
        || manifest.state_realm != job.state_realm
        || profile.state_realm != job.state_realm
        || profile.backend.state_realm() != job.state_realm
        || manifest.independent_new_sessions != profile.backend.independent_new_sessions()
        || (profile.backend.requires_workspace() && manifest.workspace.is_none())
        || manifest.workspace.as_ref().is_some_and(|workspace| {
            shared
                .workspaces
                .as_ref()
                .and_then(|registry| registry.get(&job.workspace_id))
                != Some(workspace)
        })
        || (manifest.resume != job.native_id.is_some())
    {
        return finish_prelaunch(shared, &job, "execution_snapshot_unavailable").await;
    }
    if shared.shutdown.is_cancelled() {
        let namespace = job.namespace.clone();
        let run_id = job.run_id.clone();
        shared
            .database
            .call(move |store| {
                store
                    .service_stop(
                        &namespace,
                        &run_id,
                        StopCause::Interrupted,
                        ServiceClock::Wall,
                    )
                    .map(|_| ())
                    .map_err(Into::into)
            })
            .await?;
    }
    if job_stopped(shared, &job).await?
        || !preflight_backend(shared, &job, profile.backend.as_ref()).await?
    {
        return finish_prelaunch(shared, &job, "backend_compatibility_unavailable").await;
    }
    let journal_key = manifest.workspace.as_ref().map(|_| JournalKey {
        owner_id: job.namespace.owner_id.clone(),
        client_id: job.namespace.client_id.clone(),
        run_id: job.run_id.clone(),
        state_realm: job.state_realm.clone(),
    });
    if let Some(key) = &journal_key
        && !shared.database.journal_start(key.clone()).await?
    {
        return finish_prelaunch(shared, &job, "workspace_journal_capacity").await;
    }
    let working_directory = if let Some(workspace) = &manifest.workspace {
        let lookup_job = job.clone();
        let lookup_plan = workspace.clone();
        let (bound, materialization_intent) = shared
            .database
            .call(move |store| {
                store
                    .workspace_binding(&lookup_job, &lookup_plan)
                    .map_err(Into::into)
            })
            .await?;
        if bound.is_none() && materialization_intent {
            return finish_journaled_prelaunch(
                shared,
                &job,
                "workspace_binding_uncertain",
                &journal_key,
            )
            .await;
        }
        if job_stopped(shared, &job).await? {
            return finish_journaled_prelaunch(shared, &job, "workspace_not_started", &journal_key)
                .await;
        }
        let creating_managed = bound.is_none() && workspace.managed_root().is_some();
        if creating_managed {
            let intent_job = job.clone();
            shared
                .database
                .call(move |store| {
                    store
                        .workspace_mark_materialization(&intent_job)
                        .map_err(Into::into)
                })
                .await?;
        }
        if job_stopped(shared, &job).await? {
            if creating_managed {
                let clear_job = job.clone();
                shared
                    .database
                    .call(move |store| {
                        store
                            .workspace_clear_uncreated(&clear_job)
                            .map_err(Into::into)
                    })
                    .await?;
            }
            return finish_journaled_prelaunch(shared, &job, "workspace_not_started", &journal_key)
                .await;
        }
        let plan = workspace.clone();
        let session_id = job.session_id.clone();
        let mut materialization =
            tokio::task::spawn_blocking(move || plan.materialize(&session_id, bound));
        let materialized = loop {
            tokio::select! {
                result = &mut materialization => break result,
                _ = shared.shutdown.cancelled() => {
                    return finish_prelaunch_with_cleanup(shared, &job, "workspace_materialization_uncertain", CleanupState::Uncertain).await;
                }
                _ = tokio::time::sleep(shared.limits.poll_interval) => {
                    if job_stopped(shared, &job).await? {
                        return finish_prelaunch_with_cleanup(shared, &job, "workspace_materialization_uncertain", CleanupState::Uncertain).await;
                    }
                }
            }
        };
        let directory = match materialized {
            Ok(Ok(directory)) => directory,
            Ok(Err(crate::workspace::MaterializeError::NotCreated)) if creating_managed => {
                let clear_job = job.clone();
                shared
                    .database
                    .call(move |store| {
                        store
                            .workspace_clear_uncreated(&clear_job)
                            .map_err(Into::into)
                    })
                    .await?;
                return finish_journaled_prelaunch(
                    shared,
                    &job,
                    "workspace_unavailable",
                    &journal_key,
                )
                .await;
            }
            Ok(Err(crate::workspace::MaterializeError::NotCreated)) => {
                return finish_journaled_prelaunch(
                    shared,
                    &job,
                    "workspace_unavailable",
                    &journal_key,
                )
                .await;
            }
            Ok(Err(crate::workspace::MaterializeError::Uncertain)) | Err(_) => {
                return finish_prelaunch_with_cleanup(
                    shared,
                    &job,
                    "workspace_materialization_uncertain",
                    CleanupState::Uncertain,
                )
                .await;
            }
        };
        let save_job = job.clone();
        let save_plan = workspace.clone();
        let save_directory = directory.clone();
        if shared
            .database
            .call(move |store| {
                store
                    .workspace_store_binding(&save_job, &save_plan, &save_directory)
                    .map_err(Into::into)
            })
            .await
            .is_err()
        {
            return finish_prelaunch_with_cleanup(
                shared,
                &job,
                "workspace_binding_uncertain",
                CleanupState::Uncertain,
            )
            .await;
        }
        Some(WorkingDirectory {
            path: directory.path,
            device: directory.device,
            inode: directory.inode,
        })
    } else {
        None
    };
    let (prompt, requires_skill_read) = if manifest.version == 2 {
        let prepare_job = job.clone();
        let input = manifest.prompt.clone();
        let max_bytes = shared.access.request_limit();
        let bounds = crate::storage::skills::SkillBounds {
            markdown: shared.access.skill_limit(),
            metadata: max_bytes,
            identifier: shared.access.identifier_limit(),
        };
        match shared
            .database
            .call(move |store| {
                store
                    .service_prepare_prompt(
                        &prepare_job,
                        &input,
                        max_bytes,
                        bounds,
                        ServiceClock::Wall,
                    )
                    .map_err(Into::into)
            })
            .await
        {
            Ok(Some(prepared)) => prepared,
            Ok(None) => (manifest.prompt.clone(), false),
            Err(ServiceError::Storage(
                StorageError::SkillUnavailable
                | StorageError::InvalidInput(_)
                | StorageError::Database(_),
            )) => {
                return finish_journaled_prelaunch(
                    shared,
                    &job,
                    "prompt_snapshot_unavailable",
                    &journal_key,
                )
                .await;
            }
            Err(error) => return Err(error),
        }
    } else {
        (manifest.prompt.clone(), false)
    };
    let mut changes = shared.changes.subscribe();
    let launch_job = job.clone();
    let launch_shutdown = shared.shutdown.clone();
    let access = Arc::clone(&shared.access);
    let input_len = manifest.prompt.len();
    let launch = shared
        .database
        .call(move |store| {
            let now = ServiceClock::Wall;
            if launch_shutdown.is_cancelled() {
                store.service_stop(
                    &launch_job.namespace,
                    &launch_job.run_id,
                    StopCause::Interrupted,
                    now,
                )?;
                return store.service_launch(&launch_job, now).map_err(Into::into);
            }
            access
                .with_launch(
                    LaunchAuthorization {
                        credential_id: &launch_job.credential_id,
                        generation: &launch_job.credential_generation,
                        namespace: &launch_job.namespace,
                        profile_id: &launch_job.profile_id,
                        workspace_id: &launch_job.workspace_id,
                        input_len,
                        resume: launch_job.native_id.is_some(),
                        requires_skill_read,
                    },
                    || store.service_launch(&launch_job, now).map_err(Into::into),
                )
                .map_err(Into::into)
        })
        .await;
    match launch {
        Ok(false) => {
            if let Some(key) = journal_key {
                shared.database.journal_settle(key).await?;
            }
            return Ok(());
        }
        Ok(true) => {}
        Err(ServiceError::Access(AccessError::Forbidden | AccessError::Unauthenticated)) => {
            finish_prelaunch(shared, &job, "launch_not_authorized").await?;
            if let Some(key) = journal_key {
                shared.database.journal_settle(key).await?;
            }
            return Ok(());
        }
        Err(
            ServiceError::Storage(StorageError::SkillUnavailable)
            | ServiceError::Access(AccessError::Storage(StorageError::SkillUnavailable)),
        ) => {
            finish_prelaunch(shared, &job, "skill_not_authorized").await?;
            if let Some(key) = journal_key {
                shared.database.journal_settle(key).await?;
            }
            return Ok(());
        }
        Err(error) => return Err(error),
    }
    if shared.shutdown.is_cancelled() {
        let stopped_job = job.clone();
        shared
            .database
            .call(move |store| {
                store
                    .service_proven_unstarted(&stopped_job, ServiceClock::Wall)
                    .map_err(Into::into)
            })
            .await?;
        if let Some(key) = journal_key {
            shared.database.journal_settle(key).await?;
        }
        return Ok(());
    }
    let session = match &job.native_id {
        Some(native_id) => SessionRequest::Resume(SessionRef {
            runtime: job.metadata.runtime.clone(),
            state_realm: job.state_realm.clone(),
            native_id: native_id.clone(),
        }),
        None => SessionRequest::New,
    };
    let request = ExecutionRequest {
        run_id: job.run_id.clone(),
        attempt_id: job.attempt_id.clone(),
        workspace_id: job.workspace_id.clone(),
        working_directory,
        session,
        prompt,
    };
    let cancellation = CancellationToken::new();
    let key = run_key(&job.namespace, &job.run_id);
    let registered = {
        let mut active = shared
            .active
            .lock()
            .map_err(|_| ServiceError::Unavailable)?;
        if !shared.available.load(Ordering::Acquire) || shared.shutdown.is_cancelled() {
            false
        } else {
            if active.insert(key.clone(), cancellation.clone()).is_some() {
                return Err(ServiceError::Unavailable);
            }
            true
        }
    };
    if !registered {
        let stopped_job = job.clone();
        shared
            .database
            .call(move |store| {
                store
                    .service_proven_unstarted(&stopped_job, ServiceClock::Wall)
                    .map_err(Into::into)
            })
            .await?;
        if let Some(key) = journal_key {
            shared.database.journal_settle(key).await?;
        }
        return Ok(());
    }
    let _active = ActiveExecution {
        shared: Arc::clone(shared),
        key,
        cancellation: cancellation.clone(),
    };
    let status_job = job.clone();
    if shared
        .database
        .call(move |store| {
            store
                .service_status(&status_job, ServiceClock::Wall)
                .map_err(Into::into)
        })
        .await?
        .is_some()
    {
        let stopped_job = job.clone();
        shared
            .database
            .call(move |store| {
                store
                    .service_proven_unstarted(&stopped_job, ServiceClock::Wall)
                    .map_err(Into::into)
            })
            .await?;
        if let Some(key) = journal_key {
            shared.database.journal_settle(key).await?;
        }
        return Ok(());
    }
    let event_failed = Arc::new(AtomicBool::new(false));
    let (sender, receiver) = mpsc::channel(shared.limits.event_buffer);
    let sink_failure = Arc::clone(&event_failed);
    let sink_cancel = cancellation.clone();
    let event_limit = shared.limits.max_event_bytes;
    let sink: EventSink = Arc::new(move |event| {
        if !event_within_limit(&event, event_limit) || sender.try_send(event).is_err() {
            sink_failure.store(true, Ordering::Release);
            sink_cancel.cancel();
            Err(EventSinkError)
        } else {
            Ok(())
        }
    });
    let event_done = CancellationToken::new();
    let mut event_task = AbortOnDrop(tokio::spawn(collect_events(
        Arc::clone(shared),
        job.clone(),
        receiver,
        event_done.clone(),
        cancellation.clone(),
        Arc::clone(&event_failed),
    )));
    let timer_remaining = job.deadline_ms.saturating_sub(now_ms()?).max(0) as u64;
    let deadline_cancel = cancellation.clone();
    let _deadline_task = AbortOnDrop(tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(timer_remaining)).await;
        deadline_cancel.cancel();
    }));
    let backend = profile.backend;
    let backend_cancel = cancellation.clone();
    let start_cancel = cancellation.clone();
    let start_deadline = job.deadline_ms;
    let start_gate = Arc::clone(&shared.launch_gate);
    let start_shutdown = shared.shutdown.clone();
    let mut backend_task = AbortOnDrop(tokio::spawn(async move {
        let mut future = backend.execute(request, sink, backend_cancel);
        let gate = start_gate.lock().await;
        if start_shutdown.is_cancelled()
            || start_cancel.is_cancelled()
            || now_ms().map_or(true, |now| now >= start_deadline)
        {
            return None;
        }
        let first = poll_fn(|context| Poll::Ready(future.as_mut().poll(context))).await;
        drop(gate);
        match first {
            Poll::Ready(result) => Some(result),
            Poll::Pending => Some(future.await),
        }
    }));
    let mut stopping_at: Option<tokio::time::Instant> = None;
    let mut monitor_error = None;
    let mut proven_unstarted = false;
    let remaining = job
        .deadline_ms
        .saturating_sub(now_ms().unwrap_or(job.deadline_ms))
        .max(0) as u64;
    let deadline = tokio::time::sleep(Duration::from_millis(remaining));
    tokio::pin!(deadline);
    let mut deadline_armed = true;
    let outcome = loop {
        enum Signal<T> {
            Finished(T),
            Check,
            Deadline,
            Shutdown,
            EventFailure,
            GraceExpired,
        }
        let signal = tokio::select! {
            result = &mut backend_task.0 => Signal::Finished(result),
            _ = shared.shutdown.cancelled(), if stopping_at.is_none() => Signal::Shutdown,
            _ = cancellation.cancelled(), if stopping_at.is_none() => Signal::EventFailure,
            _ = changes.changed(), if monitor_error.is_none() => Signal::Check,
            _ = &mut deadline, if deadline_armed && monitor_error.is_none() => { deadline_armed = false; Signal::Deadline },
            _ = tokio::time::sleep(shared.limits.poll_interval), if monitor_error.is_none() => Signal::Check,
            _ = async { tokio::time::sleep_until(stopping_at.unwrap()).await }, if stopping_at.is_some() => Signal::GraceExpired,
        };
        match signal {
            Signal::Finished(result) => match result {
                Ok(Some(result)) => break Some(result),
                Ok(None) => {
                    proven_unstarted = true;
                    break None;
                }
                Err(_) => break None,
            },
            Signal::GraceExpired => {
                backend_task.0.abort();
                let _ = (&mut backend_task.0).await;
                break None;
            }
            Signal::EventFailure => {
                cancellation.cancel();
                stopping_at.get_or_insert_with(|| {
                    tokio::time::Instant::now() + shared.limits.cleanup_grace
                });
            }
            Signal::Shutdown => {
                cancellation.cancel();
                stopping_at.get_or_insert_with(|| {
                    tokio::time::Instant::now() + shared.limits.cleanup_grace
                });
                let namespace = job.namespace.clone();
                let run_id = job.run_id.clone();
                let stop = shared
                    .database
                    .call(move |store| {
                        store
                            .service_stop(
                                &namespace,
                                &run_id,
                                StopCause::Interrupted,
                                ServiceClock::Wall,
                            )
                            .map(|_| ())
                            .map_err(Into::into)
                    })
                    .await;
                if let Err(error) = stop {
                    fail_service(shared);
                    monitor_error = Some(error);
                }
            }
            kind @ (Signal::Check | Signal::Deadline) => {
                if matches!(kind, Signal::Deadline) {
                    cancellation.cancel();
                    stopping_at.get_or_insert_with(|| {
                        tokio::time::Instant::now() + shared.limits.cleanup_grace
                    });
                }
                let status_job = job.clone();
                let stop = shared
                    .database
                    .call(move |store| {
                        store
                            .service_status(&status_job, ServiceClock::Wall)
                            .map_err(Into::into)
                    })
                    .await;
                match stop {
                    Ok(Some(_)) => {
                        cancellation.cancel();
                        stopping_at.get_or_insert_with(|| {
                            tokio::time::Instant::now() + shared.limits.cleanup_grace
                        });
                    }
                    Ok(None) => {}
                    Err(error) => {
                        fail_service(shared);
                        monitor_error = Some(error);
                        cancellation.cancel();
                        stopping_at.get_or_insert_with(|| {
                            tokio::time::Instant::now() + shared.limits.cleanup_grace
                        });
                    }
                }
            }
        }
    };
    event_done.cancel();
    let events_ok = (&mut event_task.0)
        .await
        .map_err(|_| ServiceError::Unavailable)?;
    if let Some(error) = monitor_error {
        return Err(error);
    }
    if proven_unstarted {
        let stopped_job = job.clone();
        shared
            .database
            .call(move |store| {
                store
                    .service_proven_unstarted(&stopped_job, ServiceClock::Wall)
                    .map_err(Into::into)
            })
            .await?;
        if let Some(key) = journal_key {
            shared.database.journal_settle(key).await?;
        }
        return Ok(());
    }
    let events_ok = events_ok && !event_failed.load(Ordering::Acquire);
    if shared.shutdown.is_cancelled() {
        let namespace = job.namespace.clone();
        let run_id = job.run_id.clone();
        shared
            .database
            .call(move |store| {
                store
                    .service_stop(
                        &namespace,
                        &run_id,
                        StopCause::Interrupted,
                        ServiceClock::Wall,
                    )
                    .map(|_| ())
                    .map_err(Into::into)
            })
            .await?;
    }
    if events_ok
        && let Some(result) = outcome.as_ref()
        && let Some(candidate) =
            proven_unstarted_candidate(result, &job, shared.limits.max_result_bytes)
    {
        let unstarted_job = job.clone();
        let limit = shared.limits.max_result_bytes;
        shared
            .database
            .call(move |store| {
                store
                    .service_finish_proven_unstarted(
                        &unstarted_job,
                        &candidate,
                        ServiceClock::Wall,
                        limit,
                    )
                    .map_err(Into::into)
            })
            .await?;
        if let Some(key) = journal_key {
            shared.database.journal_settle(key).await?;
        }
        shared.wake.notify_waiters();
        signal_change(shared);
        return Ok(());
    }
    let backend_unsettled = outcome.is_none();
    if backend_unsettled {
        fail_service(shared);
    }
    let candidate = match outcome {
        Some(result) if events_ok && result.dispatch == DispatchStatus::StartedOrUnknown => {
            let session_matches =
                if result.session.as_ref().is_some_and(|native| {
                    !session_within_limit(native, shared.limits.max_event_bytes)
                }) {
                    false
                } else if let Some(native) = &result.session {
                    let namespace = job.namespace.clone();
                    let session_id = job.session_id.clone();
                    let native = native.clone();
                    shared
                        .database
                        .call(move |store| {
                            let stored = store
                                .get_session(&namespace, &session_id)?
                                .ok_or(StorageError::NotFound)?;
                            Ok(stored.session.runtime == native.runtime
                                && stored.session.state_realm == native.state_realm
                                && stored.session.native_id.as_deref() == Some(&native.native_id))
                        })
                        .await?
                } else {
                    true
                };
            if !session_matches
                || (result.session_disposition == SessionDisposition::Reusable
                    && result.session.is_none())
                || result.run.id != job.run_id
                || result.attempt.id != job.attempt_id
                || result.attempt.run_id != job.run_id
                || result.metadata != job.metadata
                || result.run.metadata != job.metadata
                || result.attempt.metadata != job.metadata
            {
                uncertain_candidate("invalid_backend_observation")
            } else if !result_within_limit(&result, shared.limits.max_result_bytes) {
                Candidate {
                    state: TerminalState::Failed,
                    details: json!({"error": "result_limit_exceeded"}),
                    cleanup: result.cleanup,
                    disposition: result.session_disposition,
                }
            } else {
                let validation_error = match (&manifest.result_schema, &result.outcome) {
                    (Some(_), ExecutionOutcome::Succeeded { output })
                        if output.len() > crate::validation::MAX_RESULT_VALIDATION_BYTES =>
                    {
                        Some("validation_limit_exceeded")
                    }
                    (Some(schema), ExecutionOutcome::Succeeded { output }) => {
                        match crate::validation::parse_unique_json(output.as_bytes()) {
                            Err(_) => Some("invalid_or_unsupported_json"),
                            Ok(value) if !crate::validation::matches_schema(schema, &value) => {
                                Some("schema_mismatch")
                            }
                            Ok(_) => None,
                        }
                    }
                    _ => None,
                };
                let validated = manifest.result_schema.is_some()
                    && matches!(&result.outcome, ExecutionOutcome::Succeeded { .. });
                let (state, details, output_omitted) = match result.outcome {
                    ExecutionOutcome::Succeeded { .. }
                        if validation_error == Some("validation_limit_exceeded") =>
                    {
                        (
                            TerminalState::Failed,
                            json!({"error":"result_validation_failed"}),
                            true,
                        )
                    }
                    ExecutionOutcome::Succeeded { output } if validation_error.is_some() => (
                        TerminalState::Failed,
                        json!({"error":"result_validation_failed", "output":output}),
                        false,
                    ),
                    ExecutionOutcome::Succeeded { output } => {
                        (TerminalState::Succeeded, json!({"output": output}), false)
                    }
                    ExecutionOutcome::Failed(error) => (
                        TerminalState::Failed,
                        json!({"error": format!("{error:?}")}),
                        false,
                    ),
                    ExecutionOutcome::Cancelled => (TerminalState::Cancelled, Value::Null, false),
                };
                let warnings: Vec<_> = result
                    .warnings
                    .into_iter()
                    .map(|warning| json!({"code": warning.code, "message": warning.message}))
                    .collect();
                let observation = match result.warning_observation {
                    WarningObservation::Complete => "complete",
                    WarningObservation::Partial => "partial",
                    WarningObservation::Unavailable => "unavailable",
                };
                let mut details = json!({"outcome": details, "warnings": warnings, "warning_observation": observation});
                if output_omitted {
                    details["output_omitted"] = Value::Bool(true);
                }
                if validated {
                    details["validation"] = match validation_error {
                        Some(reason) => json!({"status":"failed", "reason":reason}),
                        None => json!({"status":"passed"}),
                    };
                }
                Candidate {
                    state,
                    details,
                    cleanup: result.cleanup,
                    disposition: result.session_disposition,
                }
            }
        }
        _ => uncertain_candidate("backend_or_event_failure"),
    };
    let journal_safe = candidate.cleanup == CleanupState::Settled
        && candidate.disposition == SessionDisposition::Reusable;
    let limit = shared.limits.max_result_bytes;
    shared
        .database
        .call(move |store| {
            store
                .service_finish(&job, &candidate, ServiceClock::Wall, limit)
                .map_err(Into::into)
        })
        .await?;
    if journal_safe && let Some(key) = journal_key {
        shared.database.journal_settle(key).await?;
    }
    shared.wake.notify_waiters();
    signal_change(shared);
    Ok(())
}

fn session_within_limit(session: &SessionRef, limit: usize) -> bool {
    session
        .runtime
        .len()
        .saturating_add(session.state_realm.len())
        .saturating_add(session.native_id.len())
        <= limit
}

fn event_within_limit(event: &ExecutionEvent, limit: usize) -> bool {
    let mut size = 64_usize;
    let mut add = |value: &str| {
        size = size.saturating_add(value.len());
    };
    add(&event.run_id);
    add(&event.attempt_id);
    add(&event.metadata.runtime);
    if let Some(scenario) = &event.metadata.scenario {
        add(scenario);
    }
    match &event.kind {
        ExecutionEventKind::SessionObserved(session) => {
            add(&session.runtime);
            add(&session.state_realm);
            add(&session.native_id);
        }
        ExecutionEventKind::Progress { message } => add(message),
        ExecutionEventKind::Warning(warning) => {
            add(&warning.code);
            add(&warning.message);
        }
        _ => {}
    }
    size <= limit
}

fn result_within_limit(result: &ExecutionResult, limit: usize) -> bool {
    let mut size = 128_usize;
    match &result.outcome {
        ExecutionOutcome::Succeeded { output } => size = size.saturating_add(output.len()),
        ExecutionOutcome::Failed(BackendError::ExecutionFailed { code, message }) => {
            size = size
                .saturating_add(code.len())
                .saturating_add(message.len());
        }
        _ => {}
    }
    for warning in &result.warnings {
        size = size
            .saturating_add(32)
            .saturating_add(warning.code.len())
            .saturating_add(warning.message.len());
        if size > limit {
            return false;
        }
    }
    size <= limit
}

fn proven_unstarted_candidate(
    result: &ExecutionResult,
    job: &ClaimedJob,
    limit: usize,
) -> Option<Candidate> {
    if result.dispatch != DispatchStatus::ProvenUnstarted
        || result.cleanup != CleanupState::Settled
        || result.session_disposition != SessionDisposition::NotApplicable
        || result.session.is_some()
        || result.run.id != job.run_id
        || result.attempt.id != job.attempt_id
        || result.attempt.run_id != job.run_id
        || result.metadata != job.metadata
        || result.run.metadata != job.metadata
        || result.attempt.metadata != job.metadata
        || !result_within_limit(result, limit)
    {
        return None;
    }
    let (state, outcome) = match &result.outcome {
        ExecutionOutcome::Failed(error) => {
            (TerminalState::Failed, json!({"error":format!("{error:?}")}))
        }
        ExecutionOutcome::Cancelled => (TerminalState::Cancelled, Value::Null),
        ExecutionOutcome::Succeeded { .. } => return None,
    };
    let warnings: Vec<_> = result
        .warnings
        .iter()
        .map(|warning| json!({"code":warning.code,"message":warning.message}))
        .collect();
    let observation = match result.warning_observation {
        WarningObservation::Complete => "complete",
        WarningObservation::Partial => "partial",
        WarningObservation::Unavailable => "unavailable",
    };
    Some(Candidate {
        state,
        details: json!({"outcome":outcome,"warnings":warnings,"warning_observation":observation}),
        cleanup: CleanupState::Settled,
        disposition: SessionDisposition::NotApplicable,
    })
}

fn uncertain_candidate(reason: &'static str) -> Candidate {
    Candidate {
        state: TerminalState::Failed,
        details: json!({"error": reason}),
        cleanup: CleanupState::Uncertain,
        disposition: SessionDisposition::Uncertain,
    }
}

async fn collect_events(
    shared: Arc<Shared>,
    job: ClaimedJob,
    mut receiver: mpsc::Receiver<ExecutionEvent>,
    done: CancellationToken,
    cancellation: CancellationToken,
    failed: Arc<AtomicBool>,
) -> bool {
    loop {
        let event = tokio::select! {
            biased;
            _ = done.cancelled() => { receiver.close(); receiver.recv().await },
            event = receiver.recv() => event,
        };
        let Some(event) = event else {
            return true;
        };
        if event.run_id != job.run_id
            || event.attempt_id != job.attempt_id
            || event.metadata != job.metadata
        {
            failed.store(true, Ordering::Release);
            cancellation.cancel();
            return false;
        }
        let (kind, payload, session) = match event.kind {
            ExecutionEventKind::Started => ("run.started", json!({}), None),
            ExecutionEventKind::SessionObserved(session) => {
                ("run.session", json!({}), Some(session))
            }
            ExecutionEventKind::Progress { message } => {
                ("run.progress", json!({"message": message}), None)
            }
            ExecutionEventKind::Warning(warning) => (
                "run.warning",
                json!({"code": warning.code, "message": warning.message}),
                None,
            ),
            ExecutionEventKind::CleanupObserved { cleanup, session } => (
                "run.cleanup",
                json!({"cleanup": format!("{cleanup:?}"), "session": format!("{session:?}")}),
                None,
            ),
        };
        let payload = payload.to_string().into_bytes();
        if payload.len() > shared.limits.max_event_bytes {
            failed.store(true, Ordering::Release);
            cancellation.cancel();
            return false;
        }
        let event_job = job.clone();
        let max_events = shared.limits.max_events_per_run;
        if shared
            .database
            .call(move |store| {
                store
                    .service_event(&event_job, kind, &payload, session.as_ref(), max_events)
                    .map_err(Into::into)
            })
            .await
            .is_err()
        {
            failed.store(true, Ordering::Release);
            cancellation.cancel();
            return false;
        }
    }
}
