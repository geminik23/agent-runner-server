//! Transport-independent asynchronous run admission and execution.

use std::{
    collections::HashMap,
    error::Error,
    fmt,
    fs::{File, OpenOptions},
    future::poll_fn,
    path::Path,
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
        BackendError, CancellationToken, CleanupState, EventSink, EventSinkError, ExecutionBackend,
        ExecutionEvent, ExecutionEventKind, ExecutionMetadata, ExecutionOutcome, ExecutionRequest,
        ExecutionResult, SessionDisposition, SessionRef, SessionRequest, WarningObservation,
    },
    storage::{
        AdmissionOutcome, EventSummary, Namespace, RunSummary, SqliteStore, StorageError,
        StoredResult, TerminalState,
        service::{Candidate, ClaimedJob, ServiceAdmission, ServiceClock, StopCause},
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
    limits: RunLimits,
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
        limits.validate()?;
        if !access.supports_service_ids() {
            return Err(ServiceError::InvalidConfiguration);
        }
        let mut profile_map = HashMap::new();
        for profile in profiles {
            if profile.profile_id.is_empty()
                || profile.revision.is_empty()
                || profile.state_realm.is_empty()
                || profile.state_realm != profile.backend.state_realm()
                || profile_map
                    .insert(profile.profile_id.clone(), profile)
                    .is_some()
            {
                return Err(ServiceError::InvalidConfiguration);
            }
        }
        if profile_map.is_empty() {
            return Err(ServiceError::InvalidConfiguration);
        }
        let path = path.as_ref().to_path_buf();
        let (store, guard) = tokio::task::spawn_blocking(move || open_guarded(&path))
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
                    DbMessage::Stop => break,
                }
            }
        });
        let now = now_ms()?;
        if let Err(error) = database
            .call(move |store| store.service_recover(now).map_err(Into::into))
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
            limits,
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
            .and_then(|n| n.checked_mul(16))
            .and_then(|n| n.checked_add(8192))
    }

    pub(crate) fn access_control(&self) -> Arc<ClientAccessControl> {
        Arc::clone(&self.shared.access)
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
        let profiles = Arc::clone(&self.shared.profiles);
        let client = client.clone();
        let body = body.to_vec();
        let limits = self.shared.limits.clone();
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
                            let manifest = Manifest {
                                version: 1,
                                profile_revision: profile.revision.clone(),
                                prompt: approved.input().to_owned(),
                                runtime: metadata.runtime.clone(),
                                synthetic: metadata.synthetic,
                                scenario: metadata.scenario.clone(),
                                state_realm: profile.state_realm.clone(),
                                resume: approved.resume_session_id().is_some(),
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
                                    resume_session_id: approved
                                        .resume_session_id()
                                        .map(str::to_owned),
                                    deadline_ms,
                                    max_outstanding: limits.max_outstanding,
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

fn open_guarded(path: &Path) -> Result<(SqliteStore, File), ServiceError> {
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
    let identity = {
        use std::os::unix::fs::MetadataExt;
        let locked = database_file
            .metadata()
            .map_err(|_| ServiceError::Unavailable)?;
        let named = std::fs::metadata(&canonical).map_err(|_| ServiceError::Unavailable)?;
        if (locked.dev(), locked.ino()) != (named.dev(), named.ino()) {
            return Err(ServiceError::InvalidConfiguration);
        }
        (locked.dev(), locked.ino())
    };
    let store = SqliteStore::open(&canonical)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let named = std::fs::metadata(&canonical).map_err(|_| ServiceError::Unavailable)?;
        if identity != (named.dev(), named.ino()) {
            return Err(ServiceError::InvalidConfiguration);
        }
    }
    Ok((store, database_file))
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
            .unwrap_or(shared.limits.poll_interval);
        tokio::select! {
            _ = shared.shutdown.cancelled() => return,
            _ = changes.changed() => {},
            _ = tokio::time::sleep(wait) => {},
        }
    }
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
    let job = job.clone();
    let limit = shared.limits.max_result_bytes;
    shared
        .database
        .call(move |store| {
            store
                .service_finish(
                    &job,
                    &Candidate {
                        state: TerminalState::Failed,
                        details: json!({"error": reason}),
                        cleanup: CleanupState::Settled,
                        disposition: SessionDisposition::NotApplicable,
                    },
                    ServiceClock::Wall,
                    limit,
                )
                .map_err(Into::into)
        })
        .await
}

async fn process_job(shared: &Arc<Shared>, job: ClaimedJob) -> Result<(), ServiceError> {
    let manifest: Manifest = match serde_json::from_slice(&job.manifest) {
        Ok(value) => value,
        Err(_) => return finish_prelaunch(shared, &job, "invalid_execution_snapshot").await,
    };
    let Some(profile) = shared.profiles.get(&job.profile_id).cloned() else {
        return finish_prelaunch(shared, &job, "profile_unavailable").await;
    };
    if manifest.version != 1
        || manifest.profile_revision != profile.revision
        || manifest.metadata() != job.metadata
        || profile.backend.metadata() != job.metadata
        || manifest.state_realm != job.state_realm
        || profile.state_realm != job.state_realm
        || profile.backend.state_realm() != job.state_realm
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
                    },
                    || store.service_launch(&launch_job, now).map_err(Into::into),
                )
                .map_err(Into::into)
        })
        .await;
    match launch {
        Ok(false) => return Ok(()),
        Ok(true) => {}
        Err(ServiceError::Access(AccessError::Forbidden | AccessError::Unauthenticated)) => {
            return finish_prelaunch(shared, &job, "launch_not_authorized").await;
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
        session,
        prompt: manifest.prompt,
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
    let backend_unsettled = outcome.is_none();
    if backend_unsettled {
        fail_service(shared);
    }
    let candidate = match outcome {
        Some(result) if events_ok => {
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
                let (state, details) = match result.outcome {
                    ExecutionOutcome::Succeeded { output } => {
                        (TerminalState::Succeeded, json!({"output": output}))
                    }
                    ExecutionOutcome::Failed(error) => (
                        TerminalState::Failed,
                        json!({"error": format!("{error:?}")}),
                    ),
                    ExecutionOutcome::Cancelled => (TerminalState::Cancelled, Value::Null),
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
                Candidate {
                    state,
                    details: json!({"outcome": details, "warnings": warnings, "warning_observation": observation}),
                    cleanup: result.cleanup,
                    disposition: result.session_disposition,
                }
            }
        }
        _ => uncertain_candidate("backend_or_event_failure"),
    };
    let limit = shared.limits.max_result_bytes;
    shared
        .database
        .call(move |store| {
            store
                .service_finish(&job, &candidate, ServiceClock::Wall, limit)
                .map_err(Into::into)
        })
        .await?;
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
