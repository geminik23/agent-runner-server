use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use agent_runner_server::{
    access::{
        AccessLimits, ClientAccessControl, ClientRegistration, CredentialSecret, ExecutionProfile,
        Scope,
    },
    application::{BackendProfile, CancelOutcome, RunLimits, RunService, ServiceError},
    core::{
        CancellationToken, EventSink, ExecutionBackend, ExecutionMetadata, ExecutionRequest,
        ExecutionResult,
    },
    runtimes::dummy::{DummyBackend, DummyScenario},
    storage::{Namespace, StorageError},
};
use serde_json::{Value, json};
use tempfile::tempdir;
use tokio::sync::Notify;

fn access() -> Arc<ClientAccessControl> {
    Arc::new(
        ClientAccessControl::new(
            [
                ClientRegistration::new(
                    "credential",
                    CredentialSecret::new([7; 32]),
                    Namespace {
                        owner_id: "owner".into(),
                        client_id: "client".into(),
                    },
                    [
                        Scope::RunStart,
                        Scope::RunRead,
                        Scope::RunCancel,
                        Scope::SessionRead,
                    ],
                    ["profile".into()],
                )
                .with_generation("registration_1"),
                ClientRegistration::new(
                    "other_credential",
                    CredentialSecret::new([9; 32]),
                    Namespace {
                        owner_id: "owner".into(),
                        client_id: "other_client".into(),
                    },
                    [
                        Scope::RunStart,
                        Scope::RunRead,
                        Scope::RunCancel,
                        Scope::SessionRead,
                    ],
                    ["profile".into()],
                )
                .with_generation("other_registration_1"),
            ],
            [ExecutionProfile::dummy(
                "profile",
                ["workspace".into()],
                4096,
            )],
            AccessLimits {
                max_request_bytes: 8192,
                max_skill_bytes: 1024,
                max_identifier_bytes: 128,
            },
        )
        .unwrap(),
    )
}

fn limits() -> RunLimits {
    RunLimits {
        max_outstanding: 16,
        max_workers: 1,
        database_queue: 16,
        event_buffer: 16,
        max_events_per_run: 16,
        max_event_bytes: 2048,
        max_result_bytes: 4096,
        max_page_size: 16,
        run_timeout: Duration::from_secs(5),
        cleanup_grace: Duration::from_secs(1),
        poll_interval: Duration::from_millis(5),
    }
}

fn body(key: &str, text: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({"profile_id": "profile", "workspace_id": "workspace", "input": {"request": text}, "idempotency_key": key})).unwrap()
}

fn profile(backend: Arc<dyn ExecutionBackend>) -> BackendProfile {
    BackendProfile {
        profile_id: "profile".into(),
        revision: "profile_v1".into(),
        state_realm: "dummy".into(),
        backend,
    }
}

async fn wait_terminal(
    service: &RunService,
    client: &agent_runner_server::access::AuthenticatedClient,
    run_id: &str,
) -> String {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let run = service.get_run(client, run_id).await.unwrap().unwrap();
            if matches!(
                run.state.as_str(),
                "succeeded" | "failed" | "cancelled" | "timed_out" | "interrupted"
            ) {
                break run.state;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("run did not become terminal")
}

struct GateBackend {
    inner: DummyBackend,
    started: Notify,
    release: Notify,
    calls: AtomicUsize,
}
impl GateBackend {
    fn new() -> Self {
        Self {
            inner: DummyBackend::new(DummyScenario::Success, 8),
            started: Notify::new(),
            release: Notify::new(),
            calls: AtomicUsize::new(0),
        }
    }
}
impl ExecutionBackend for GateBackend {
    fn metadata(&self) -> ExecutionMetadata {
        self.inner.metadata()
    }
    fn state_realm(&self) -> &str {
        self.inner.state_realm()
    }
    fn execute<'a>(
        &'a self,
        request: ExecutionRequest,
        events: EventSink,
        cancellation: CancellationToken,
    ) -> Pin<Box<dyn Future<Output = ExecutionResult> + Send + 'a>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.started.notify_one();
            tokio::select! { _ = self.release.notified() => {}, _ = cancellation.cancelled() => {} }
            self.inner.execute(request, events, cancellation).await
        })
    }
}

struct WarningThenCancelBackend {
    inner: DummyBackend,
    ready: Notify,
}
impl ExecutionBackend for WarningThenCancelBackend {
    fn metadata(&self) -> ExecutionMetadata {
        self.inner.metadata()
    }
    fn state_realm(&self) -> &str {
        self.inner.state_realm()
    }
    fn execute<'a>(
        &'a self,
        request: ExecutionRequest,
        events: EventSink,
        cancellation: CancellationToken,
    ) -> Pin<Box<dyn Future<Output = ExecutionResult> + Send + 'a>> {
        Box::pin(async move {
            let result = self
                .inner
                .execute(request, events, CancellationToken::new())
                .await;
            self.ready.notify_one();
            cancellation.cancelled().await;
            result
        })
    }
}

#[tokio::test]
async fn cancellation_preserves_observed_warning_without_publishing_late_output() {
    let temp = tempdir().unwrap();
    let access = access();
    let client = access.authenticate("credential", &[7; 32]).unwrap();
    let backend = Arc::new(WarningThenCancelBackend {
        inner: DummyBackend::new(DummyScenario::Warning, 0),
        ready: Notify::new(),
    });
    let service = RunService::open(
        temp.path().join("warning-cancel.sqlite"),
        access,
        vec![profile(backend.clone())],
        limits(),
    )
    .await
    .unwrap();
    let run = service
        .start(&client, &body("warning-cancel", "fixture"))
        .await
        .unwrap();
    backend.ready.notified().await;
    assert_eq!(
        service.cancel(&client, &run.run_id).await.unwrap(),
        CancelOutcome::CancelRequested
    );
    assert_eq!(
        wait_terminal(&service, &client, &run.run_id).await,
        "cancelled"
    );
    let result = service
        .get_result(&client, &run.run_id)
        .await
        .unwrap()
        .unwrap();
    let result: Value = serde_json::from_slice(&result.payload).unwrap();
    assert_eq!(result["details"]["warnings"][0]["code"], "dummy_warning");
    assert_eq!(result["details"]["warning_observation"], "complete");
    assert!(result["details"].get("outcome").is_none());
    service.shutdown().await.unwrap();
}

struct OutputBackend {
    inner: DummyBackend,
    output: String,
}
impl ExecutionBackend for OutputBackend {
    fn metadata(&self) -> ExecutionMetadata {
        self.inner.metadata()
    }
    fn state_realm(&self) -> &str {
        self.inner.state_realm()
    }
    fn execute<'a>(
        &'a self,
        request: ExecutionRequest,
        events: EventSink,
        cancellation: CancellationToken,
    ) -> Pin<Box<dyn Future<Output = ExecutionResult> + Send + 'a>> {
        Box::pin(async move {
            let mut result = self.inner.execute(request, events, cancellation).await;
            result.outcome = agent_runner_server::core::ExecutionOutcome::Succeeded {
                output: self.output.clone(),
            };
            result
        })
    }
}

#[tokio::test]
async fn selected_validation_rejects_ambiguous_and_over_budget_backend_output() {
    for (output, reason) in [
        (
            r#"{"value":1,"value":2}"#.to_owned(),
            "invalid_or_unsupported_json",
        ),
        (
            format!("\"{}\"", "x".repeat(256 * 1024)),
            "validation_limit_exceeded",
        ),
    ] {
        let temp = tempdir().unwrap();
        let access = access();
        let client = access.authenticate("credential", &[7; 32]).unwrap();
        let backend = Arc::new(OutputBackend {
            inner: DummyBackend::new(DummyScenario::Success, 0),
            output,
        });
        let mut config = limits();
        config.max_result_bytes = 512 * 1024;
        let service = RunService::open(
            temp.path().join("validation.sqlite"),
            access,
            vec![profile(backend)],
            config,
        )
        .await
        .unwrap();
        let request = serde_json::to_vec(&json!({"profile_id":"profile","workspace_id":"workspace","input":{"request":"fixture"},"idempotency_key":"validate-output","result_schema":{"type":"object"}})).unwrap();
        let receipt = service.start(&client, &request).await.unwrap();
        assert_eq!(
            wait_terminal(&service, &client, &receipt.run_id).await,
            "failed"
        );
        let result = service
            .get_result(&client, &receipt.run_id)
            .await
            .unwrap()
            .unwrap();
        let result: Value = serde_json::from_slice(&result.payload).unwrap();
        assert_eq!(result["details"]["validation"]["reason"], reason);
        if reason == "validation_limit_exceeded" {
            assert_eq!(result["details"]["output_omitted"], true);
        } else {
            assert_eq!(
                result["details"]["outcome"]["output"],
                r#"{"value":1,"value":2}"#
            );
        }
        service.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn legacy_v2_database_replays_the_original_normalized_request() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("legacy.sqlite");
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute_batch(include_str!("../migrations/0001_initial.sql"))
        .unwrap();
    connection
        .execute_batch(include_str!("../migrations/0002_run_service.sql"))
        .unwrap();
    connection
        .pragma_update(None, "application_id", 0x41525353_i64)
        .unwrap();
    connection.pragma_update(None, "user_version", 2).unwrap();
    connection.execute_batch("INSERT INTO namespaces (owner_id, client_id) VALUES ('owner', 'client'); INSERT INTO sessions (owner_id, client_id, session_id, runtime, state_realm, native_id, cleanup, disposition) VALUES ('owner', 'client', 'legacy_session', 'dummy', 'dummy', 'legacy_native', 'settled', 'reusable'); INSERT INTO service_sessions (owner_id, client_id, session_id, profile_id, profile_revision, workspace_id) VALUES ('owner', 'client', 'legacy_session', 'profile', 'profile_v1', 'workspace');").unwrap();
    let normalized = br#"{"profile_id":"profile","workspace_id":"workspace","session":{"mode":"new"},"input":{"request":"literal"},"idempotency_key":"legacy-key"}"#;
    let manifest = serde_json::to_vec(&json!({"version":1,"profile_revision":"profile_v1","prompt":"literal","runtime":"dummy","synthetic":true,"scenario":"success","state_realm":"dummy","resume":false})).unwrap();
    connection.execute("INSERT INTO runs (owner_id, client_id, run_id, session_id, idempotency_key, normalized_request, manifest, runtime, synthetic, scenario, state) VALUES ('owner', 'client', 'legacy_run', 'legacy_session', 'legacy-key', ?1, ?2, 'dummy', 1, 'success', 'succeeded')", rusqlite::params![normalized.as_slice(), manifest]).unwrap();
    connection.execute_batch("INSERT INTO attempts (owner_id, client_id, run_id, attempt_id, state, spawn_intent, cleanup, disposition) VALUES ('owner', 'client', 'legacy_run', 'legacy_attempt', 'succeeded', 1, 'settled', 'reusable'); INSERT INTO service_jobs (owner_id, client_id, run_id, attempt_id, credential_id, credential_generation, profile_id, workspace_id, state_realm, deadline_ms) VALUES ('owner', 'client', 'legacy_run', 'legacy_attempt', 'credential', 'registration_1', 'profile', 'workspace', 'dummy', 9223372036854775806);").unwrap();
    drop(connection);
    let access = access();
    let client = access.authenticate("credential", &[7; 32]).unwrap();
    let backend = Arc::new(DummyBackend::new(DummyScenario::Success, 8));
    let service = RunService::open(&path, access, vec![profile(backend.clone())], limits())
        .await
        .unwrap();
    let replay = service
        .start(&client, &body("legacy-key", "literal"))
        .await
        .unwrap();
    assert_eq!(replay.run_id, "legacy_run");
    assert!(replay.replay);
    assert!(backend.captured_inputs().is_empty());
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn trusted_profile_runtime_mismatch_is_rejected_before_admission() {
    let temp = tempdir().unwrap();
    let access = Arc::new(
        ClientAccessControl::new(
            [ClientRegistration::new(
                "credential",
                CredentialSecret::new([7; 32]),
                Namespace {
                    owner_id: "owner".into(),
                    client_id: "client".into(),
                },
                [Scope::RunStart, Scope::RunRead],
                ["profile".into()],
            )
            .with_generation("registration_1")],
            [ExecutionProfile::for_runtime(
                "profile",
                "codex",
                ["workspace".into()],
                4096,
            )],
            AccessLimits {
                max_request_bytes: 8192,
                max_skill_bytes: 1024,
                max_identifier_bytes: 128,
            },
        )
        .unwrap(),
    );
    let client = access.authenticate("credential", &[7; 32]).unwrap();
    let backend = Arc::new(DummyBackend::new(DummyScenario::Success, 4));
    let service = RunService::open(
        temp.path().join("runs.sqlite"),
        access,
        vec![profile(backend.clone())],
        limits(),
    )
    .await
    .unwrap();
    assert!(matches!(
        service
            .start(&client, &body("runtime-mismatch", "literal"))
            .await,
        Err(ServiceError::Access(
            agent_runner_server::access::AccessError::Forbidden
        ))
    ));
    assert!(backend.captured_inputs().is_empty());
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn durable_admission_replay_execution_and_scoped_result() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("runs.sqlite");
    let access = access();
    let client = access.authenticate("credential", &[7; 32]).unwrap();
    let backend = Arc::new(GateBackend::new());
    let mut config = limits();
    config.max_workers = 2;
    let service = RunService::open(
        &path,
        Arc::clone(&access),
        vec![profile(backend.clone())],
        config,
    )
    .await
    .unwrap();
    let request = body("same-key", "literal {{input}}");
    let (first, second) = tokio::join!(
        service.start(&client, &request),
        service.start(&client, &request)
    );
    let (first, second) = (first.unwrap(), second.unwrap());
    assert_eq!(first.run_id, second.run_id);
    assert_ne!(first.replay, second.replay);
    assert_eq!(first.session_id, second.session_id);
    let other = access.authenticate("other_credential", &[9; 32]).unwrap();
    assert!(
        service
            .get_run(&other, &first.run_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        service
            .get_result(&other, &first.run_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        service.cancel(&other, &first.run_id).await,
        Err(ServiceError::Access(
            agent_runner_server::access::AccessError::Storage(StorageError::NotFound)
        ))
    ));
    backend.started.notified().await;
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    assert!(
        service
            .get_result(&client, &first.run_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        service.start(&client, &body("same-key", "different")).await,
        Err(ServiceError::Access(
            agent_runner_server::access::AccessError::Storage(StorageError::IdempotencyConflict)
        ))
    ));
    backend.release.notify_one();
    assert_eq!(
        wait_terminal(&service, &client, &first.run_id).await,
        "succeeded"
    );
    let result = service
        .get_result(&client, &first.run_id)
        .await
        .unwrap()
        .unwrap();
    assert!(result.metadata.synthetic);
    let payload: Value = serde_json::from_slice(&result.payload).unwrap();
    assert_eq!(payload["state"], "succeeded");
    assert_eq!(
        payload["details"]["outcome"]["output"],
        "Scripted dummy output"
    );
    assert_eq!(
        backend.inner.captured_inputs()[0].request.prompt,
        "literal {{input}}"
    );
    assert_eq!(
        backend.inner.captured_inputs()[0].request.workspace_id,
        "workspace"
    );
    let first_page = service
        .get_events(&client, &first.run_id, None, 2)
        .await
        .unwrap();
    assert_eq!(first_page.len(), 2);
    let rest = service
        .get_events(&client, &first.run_id, Some(first_page[1].sequence), 16)
        .await
        .unwrap();
    assert_eq!(rest.last().unwrap().kind, "run.finished");
    assert_eq!(
        service.cancel(&client, &first.run_id).await.unwrap(),
        CancelOutcome::AlreadyTerminal
    );
    service.shutdown().await.unwrap();
    let reopened = SqliteStoreReader::read(&path, &first.run_id);
    assert_eq!(reopened, "succeeded");
}

struct SqliteStoreReader;
impl SqliteStoreReader {
    fn read(path: &std::path::Path, run_id: &str) -> String {
        let store = agent_runner_server::storage::SqliteStore::open(path).unwrap();
        store
            .get_run_summary(
                &Namespace {
                    owner_id: "owner".into(),
                    client_id: "client".into(),
                },
                run_id,
            )
            .unwrap()
            .unwrap()
            .state
    }
}

#[tokio::test]
async fn cancellation_commits_before_backend_response() {
    let temp = tempdir().unwrap();
    let access = access();
    let client = access.authenticate("credential", &[7; 32]).unwrap();
    let backend = Arc::new(GateBackend::new());
    let service = RunService::open(
        temp.path().join("runs.sqlite"),
        access,
        vec![profile(backend.clone())],
        limits(),
    )
    .await
    .unwrap();
    let run = service
        .start(&client, &body("cancel", "work"))
        .await
        .unwrap();
    backend.started.notified().await;
    assert_eq!(
        service.cancel(&client, &run.run_id).await.unwrap(),
        CancelOutcome::CancelRequested
    );
    assert_eq!(
        wait_terminal(&service, &client, &run.run_id).await,
        "cancelled"
    );
    let payload: Value = serde_json::from_slice(
        &service
            .get_result(&client, &run.run_id)
            .await
            .unwrap()
            .unwrap()
            .payload,
    )
    .unwrap();
    assert_eq!(payload["state"], "cancelled");
    assert_eq!(payload["details"]["warnings"], json!([]));
    assert_eq!(payload["details"]["warning_observation"], "complete");
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn timeout_is_durable_and_instance_guard_prevents_a_second_server() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("runs.sqlite");
    let access = access();
    let client = access.authenticate("credential", &[7; 32]).unwrap();
    let backend = Arc::new(GateBackend::new());
    let mut config = limits();
    config.run_timeout = Duration::from_millis(80);
    let service = RunService::open(
        &path,
        Arc::clone(&access),
        vec![profile(backend.clone())],
        config.clone(),
    )
    .await
    .unwrap();
    assert!(matches!(
        RunService::open(
            &path,
            Arc::clone(&access),
            vec![profile(backend.clone())],
            config
        )
        .await,
        Err(ServiceError::AlreadyRunning)
    ));
    let run = service
        .start(&client, &body("timeout", "work"))
        .await
        .unwrap();
    backend.started.notified().await;
    assert_eq!(
        wait_terminal(&service, &client, &run.run_id).await,
        "timed_out"
    );
    let payload: Value = serde_json::from_slice(
        &service
            .get_result(&client, &run.run_id)
            .await
            .unwrap()
            .unwrap()
            .payload,
    )
    .unwrap();
    assert_eq!(payload["state"], "timed_out");
    service.shutdown().await.unwrap();
    let reopened = RunService::open(&path, access, vec![profile(backend)], limits())
        .await
        .unwrap();
    assert_eq!(
        reopened
            .get_run(&client, &run.run_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        "timed_out"
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn instance_guard_prevents_second_process() {
    const PROBE: &str = "AGENT_RUNNER_TEST_GUARD_PATH";
    if let Some(path) = std::env::var_os(PROBE) {
        let backend = Arc::new(GateBackend::new());
        assert!(matches!(
            RunService::open(path, access(), vec![profile(backend)], limits()).await,
            Err(ServiceError::AlreadyRunning)
        ));
        return;
    }
    let temp = tempdir().unwrap();
    let path = temp.path().join("runs.sqlite");
    let backend = Arc::new(GateBackend::new());
    let service = RunService::open(&path, access(), vec![profile(backend)], limits())
        .await
        .unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "instance_guard_prevents_second_process",
            "--nocapture",
        ])
        .env(PROBE, &path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "child failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn dropped_admission_future_does_not_lose_durable_work_or_wakeup() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("runs.sqlite");
    let access = access();
    let client = access.authenticate("credential", &[7; 32]).unwrap();
    let backend = Arc::new(GateBackend::new());
    let mut config = limits();
    config.poll_interval = Duration::from_secs(60);
    let service = RunService::open(&path, access, vec![profile(backend.clone())], config)
        .await
        .unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute_batch("BEGIN IMMEDIATE").unwrap();
    let request = body("dropped", "work");
    let mut pending = Box::pin(service.start(&client, &request));
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut pending)
            .await
            .is_err()
    );
    drop(pending);
    connection.execute_batch("COMMIT").unwrap();
    tokio::time::timeout(Duration::from_secs(1), backend.started.notified())
        .await
        .expect("accepted work was not woken");
    let replay = service.start(&client, &request).await.unwrap();
    assert!(replay.replay);
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    backend.release.notify_one();
    assert_eq!(
        wait_terminal(&service, &client, &replay.run_id).await,
        "succeeded"
    );
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn replay_succeeds_when_the_outstanding_queue_is_full() {
    let temp = tempdir().unwrap();
    let access = access();
    let client = access.authenticate("credential", &[7; 32]).unwrap();
    let backend = Arc::new(GateBackend::new());
    let mut config = limits();
    config.max_outstanding = 1;
    config.poll_interval = Duration::from_secs(60);
    let service = RunService::open(
        temp.path().join("runs.sqlite"),
        access,
        vec![profile(backend.clone())],
        config,
    )
    .await
    .unwrap();
    let first = service
        .start(&client, &body("first", "work"))
        .await
        .unwrap();
    backend.started.notified().await;
    let replay = service
        .start(&client, &body("first", "work"))
        .await
        .unwrap();
    assert!(replay.replay);
    assert_eq!(replay.run_id, first.run_id);
    assert!(matches!(
        service.start(&client, &body("second", "work")).await,
        Err(ServiceError::Access(
            agent_runner_server::access::AccessError::Storage(StorageError::QueueFull)
        ))
    ));
    assert_eq!(
        service.cancel(&client, &first.run_id).await.unwrap(),
        CancelOutcome::CancelRequested
    );
    assert_eq!(
        tokio::time::timeout(
            Duration::from_millis(500),
            wait_terminal(&service, &client, &first.run_id)
        )
        .await
        .unwrap(),
        "cancelled"
    );
    service.shutdown().await.unwrap();
}

struct HoldingBackend {
    inner: DummyBackend,
    started: Notify,
    release: Notify,
}
impl ExecutionBackend for HoldingBackend {
    fn metadata(&self) -> ExecutionMetadata {
        self.inner.metadata()
    }
    fn state_realm(&self) -> &str {
        self.inner.state_realm()
    }
    fn execute<'a>(
        &'a self,
        request: ExecutionRequest,
        events: EventSink,
        _: CancellationToken,
    ) -> Pin<Box<dyn Future<Output = ExecutionResult> + Send + 'a>> {
        Box::pin(async move {
            let result = self
                .inner
                .execute(request, events, CancellationToken::new())
                .await;
            self.started.notify_one();
            self.release.notified().await;
            result
        })
    }
}

#[tokio::test]
async fn uncooperative_backend_fails_closed_after_cleanup_grace() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("runs.sqlite");
    let access = access();
    let client = access.authenticate("credential", &[7; 32]).unwrap();
    let backend = Arc::new(HoldingBackend {
        inner: DummyBackend::new(DummyScenario::Success, 0),
        started: Notify::new(),
        release: Notify::new(),
    });
    let mut config = limits();
    config.cleanup_grace = Duration::from_millis(40);
    let service = RunService::open(&path, access, vec![profile(backend.clone())], config)
        .await
        .unwrap();
    let run = service
        .start(&client, &body("stubborn", "work"))
        .await
        .unwrap();
    backend.started.notified().await;
    assert_eq!(
        service.cancel(&client, &run.run_id).await.unwrap(),
        CancelOutcome::CancelRequested
    );
    assert_eq!(
        wait_terminal(&service, &client, &run.run_id).await,
        "cancelled"
    );
    let store = agent_runner_server::storage::SqliteStore::open(&path).unwrap();
    let session = store
        .get_session(
            &Namespace {
                owner_id: "owner".into(),
                client_id: "client".into(),
            },
            run.session_id.as_deref().unwrap(),
        )
        .unwrap()
        .unwrap();
    assert_eq!(session.cleanup.as_deref(), Some("uncertain"));
    assert_eq!(
        session.reserved_run_id.as_deref(),
        Some(run.run_id.as_str())
    );
    drop(store);
    assert!(matches!(
        service.start(&client, &body("after_failure", "work")).await,
        Err(ServiceError::Unavailable)
    ));
    assert!(matches!(
        service.shutdown().await,
        Err(ServiceError::Unavailable)
    ));
}

#[tokio::test]
async fn queued_deadline_expires_while_execution_slot_is_occupied() {
    let temp = tempdir().unwrap();
    let access = access();
    let client = access.authenticate("credential", &[7; 32]).unwrap();
    let backend = Arc::new(HoldingBackend {
        inner: DummyBackend::new(DummyScenario::Success, 0),
        started: Notify::new(),
        release: Notify::new(),
    });
    let mut config = limits();
    config.run_timeout = Duration::from_millis(150);
    config.cleanup_grace = Duration::from_secs(2);
    config.poll_interval = Duration::from_secs(60);
    let service = RunService::open(
        temp.path().join("runs.sqlite"),
        access,
        vec![profile(backend.clone())],
        config,
    )
    .await
    .unwrap();
    let first = service
        .start(&client, &body("first", "work"))
        .await
        .unwrap();
    backend.started.notified().await;
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if service
                .get_events(&client, &first.run_id, None, 16)
                .await
                .unwrap()
                .iter()
                .any(|event| event.kind == "run.session")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    let second = service
        .start(&client, &body("second", "work"))
        .await
        .unwrap();
    assert_eq!(
        wait_terminal(&service, &client, &second.run_id).await,
        "timed_out"
    );
    assert_eq!(
        service
            .get_run(&client, &first.run_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        "running"
    );
    backend.release.notify_one();
    assert_eq!(
        wait_terminal(&service, &client, &first.run_id).await,
        "timed_out"
    );
    service.shutdown().await.unwrap();
}
