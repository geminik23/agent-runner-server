#![cfg(target_os = "linux")]

use std::{
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use agent_runner_server::{
    access::{
        AccessLimits, ClientAccessControl, ClientRegistration, CredentialSecret, ExecutionProfile,
        Scope,
    },
    application::{BackendProfile, RunLimits, RunService, ServiceError},
    core::{
        BackendError, CancellationToken, CleanupState, EventSink, ExecutionBackend,
        ExecutionMetadata, ExecutionRequest, ExecutionResult, SessionDisposition, SessionRef,
    },
    maintenance::{Maintenance, Release, ReleaseError},
    runtimes::dummy::{DummyBackend, DummyScenario},
    storage::{Namespace, QuarantineKind, SqliteStore},
    workspace::{WorkspaceAccess, WorkspaceConfig, WorkspaceEntry, WorkspaceLocation},
};
use serde_json::json;
use tempfile::tempdir;
use tokio::sync::Notify;

struct WorkspaceBackend {
    inner: DummyBackend,
    captured: Mutex<Vec<PathBuf>>,
    changed: Notify,
    release: Notify,
    wait: bool,
    state_directory: Option<PathBuf>,
    state_owner_root: Option<PathBuf>,
}
impl ExecutionBackend for WorkspaceBackend {
    fn metadata(&self) -> ExecutionMetadata {
        self.inner.metadata()
    }
    fn state_realm(&self) -> &str {
        self.inner.state_realm()
    }
    fn state_directory(&self) -> Option<&Path> {
        self.state_directory.as_deref()
    }
    fn state_owner_root(&self) -> Option<&Path> {
        self.state_owner_root
            .as_deref()
            .or_else(|| self.state_directory.as_deref().and_then(Path::parent))
    }
    fn independent_new_sessions(&self) -> bool {
        true
    }
    fn supports_shared_read(&self) -> bool {
        true
    }
    fn requires_workspace(&self) -> bool {
        true
    }
    fn execute<'a>(
        &'a self,
        request: ExecutionRequest,
        events: EventSink,
        cancellation: CancellationToken,
    ) -> Pin<Box<dyn Future<Output = ExecutionResult> + Send + 'a>> {
        Box::pin(async move {
            let directory = request
                .working_directory
                .clone()
                .expect("workspace is pinned")
                .path;
            assert!(directory.is_dir());
            self.captured.lock().unwrap().push(directory);
            self.changed.notify_waiters();
            if self.wait {
                tokio::select! { _ = self.release.notified() => {}, _ = cancellation.cancelled() => {} }
            }
            self.inner.execute(request, events, cancellation).await
        })
    }
}

struct GatedPreflightBackend {
    inner: WorkspaceBackend,
    ready: Notify,
}
impl ExecutionBackend for GatedPreflightBackend {
    fn metadata(&self) -> ExecutionMetadata {
        self.inner.metadata()
    }
    fn state_realm(&self) -> &str {
        self.inner.state_realm()
    }
    fn independent_new_sessions(&self) -> bool {
        true
    }
    fn supports_shared_read(&self) -> bool {
        true
    }
    fn requires_workspace(&self) -> bool {
        true
    }
    fn preflight<'a>(
        &'a self,
        cancellation: CancellationToken,
    ) -> Pin<Box<dyn Future<Output = Result<(), BackendError>> + Send + 'a>> {
        Box::pin(async move {
            self.ready.notify_one();
            cancellation.cancelled().await;
            Err(BackendError::InvalidRequest)
        })
    }
    fn execute<'a>(
        &'a self,
        request: ExecutionRequest,
        events: EventSink,
        cancellation: CancellationToken,
    ) -> Pin<Box<dyn Future<Output = ExecutionResult> + Send + 'a>> {
        self.inner.execute(request, events, cancellation)
    }
}

fn prepare_directories(root: &Path) {
    use std::os::unix::fs::PermissionsExt;
    for name in ["owner", "managed", "a", "b"] {
        let path = root.join(name);
        std::fs::create_dir(&path).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
}
fn config(root: &Path) -> WorkspaceConfig {
    let existing = |id: &str| WorkspaceEntry {
        id: id.into(),
        revision: "v1".into(),
        access: WorkspaceAccess::SharedRead,
        location: WorkspaceLocation::Existing {
            path: root.join(id),
        },
    };
    WorkspaceConfig {
        ownership_directory: root.join("owner"),
        entries: vec![
            existing("a"),
            existing("b"),
            WorkspaceEntry {
                id: "scratch".into(),
                revision: "v1".into(),
                access: WorkspaceAccess::Exclusive,
                location: WorkspaceLocation::Managed {
                    root: root.join("managed"),
                    max_sessions: 3,
                },
            },
        ],
    }
}
fn access() -> Arc<ClientAccessControl> {
    Arc::new(
        ClientAccessControl::new(
            [ClientRegistration::new(
                "client",
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
            .with_generation("v1")],
            [ExecutionProfile::dummy(
                "profile",
                ["a".into(), "b".into(), "scratch".into()],
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
        max_outstanding: 8,
        max_workers: 2,
        database_queue: 32,
        event_buffer: 32,
        max_events_per_run: 32,
        max_event_bytes: 2048,
        max_result_bytes: 4096,
        max_page_size: 16,
        run_timeout: Duration::from_secs(10),
        cleanup_grace: Duration::from_secs(2),
        poll_interval: Duration::from_millis(5),
    }
}
fn body(workspace: &str, key: &str, session: serde_json::Value) -> Vec<u8> {
    serde_json::to_vec(&json!({"profile_id":"profile","workspace_id":workspace,"idempotency_key":key,"session":session,"input":{"request":"fixture"}})).unwrap()
}
async fn service(
    root: &Path,
    backend: Arc<WorkspaceBackend>,
    access: Arc<ClientAccessControl>,
) -> RunService {
    RunService::open_with_workspaces(
        root.join("runs.sqlite"),
        access,
        vec![BackendProfile {
            profile_id: "profile".into(),
            revision: "v1".into(),
            state_realm: "dummy".into(),
            backend,
        }],
        limits(),
        None,
        None,
        Some(config(root)),
    )
    .await
    .unwrap()
}
async fn terminal(
    service: &RunService,
    client: &agent_runner_server::access::AuthenticatedClient,
    id: &str,
) -> String {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let state = service.get_run(client, id).await.unwrap().unwrap().state;
            if [
                "succeeded",
                "failed",
                "cancelled",
                "timed_out",
                "interrupted",
            ]
            .contains(&state.as_str())
            {
                break state;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("terminal state")
}
fn backend(wait: bool) -> Arc<WorkspaceBackend> {
    Arc::new(WorkspaceBackend {
        inner: DummyBackend::new(DummyScenario::Success, 0),
        captured: Mutex::new(Vec::new()),
        changed: Notify::new(),
        release: Notify::new(),
        wait,
        state_directory: None,
        state_owner_root: None,
    })
}

#[tokio::test]
async fn distinct_new_sessions_in_distinct_workspaces_execute_concurrently() {
    let temp = tempdir().unwrap();
    prepare_directories(temp.path());
    let access = access();
    let client = access.authenticate("client", &[7; 32]).unwrap();
    let backend = backend(true);
    let service = service(temp.path(), backend.clone(), access).await;
    let a = service
        .start(&client, &body("a", "one", json!({"mode":"new"})))
        .await
        .unwrap();
    let b = service
        .start(&client, &body("b", "two", json!({"mode":"new"})))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let notified = backend.changed.notified();
            if backend.captured.lock().unwrap().len() == 2 {
                break;
            }
            notified.await;
        }
    })
    .await
    .expect("two backend executions must overlap");
    let captured = backend.captured.lock().unwrap().clone();
    assert!(captured.contains(&temp.path().join("a").canonicalize().unwrap()));
    assert!(captured.contains(&temp.path().join("b").canonicalize().unwrap()));
    backend.release.notify_waiters();
    assert_eq!(terminal(&service, &client, &a.run_id).await, "succeeded");
    assert_eq!(terminal(&service, &client, &b.run_id).await, "succeeded");
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn verified_shared_read_backend_runs_distinct_sessions_in_same_workspace() {
    let temp = tempdir().unwrap();
    prepare_directories(temp.path());
    let access = access();
    let client = access.authenticate("client", &[7; 32]).unwrap();
    let backend = backend(true);
    let service = service(temp.path(), backend.clone(), access).await;
    let first = service
        .start(&client, &body("a", "read-one", json!({"mode":"new"})))
        .await
        .unwrap();
    let second = service
        .start(&client, &body("a", "read-two", json!({"mode":"new"})))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let notified = backend.changed.notified();
            if backend.captured.lock().unwrap().len() == 2 {
                break;
            }
            notified.await;
        }
    })
    .await
    .expect("shared readers must overlap");
    let captured = backend.captured.lock().unwrap().clone();
    assert_eq!(captured[0], captured[1]);
    assert_ne!(first.session_id, second.session_id);
    backend.release.notify_waiters();
    assert_eq!(
        terminal(&service, &client, &first.run_id).await,
        "succeeded"
    );
    assert_eq!(
        terminal(&service, &client, &second.run_id).await,
        "succeeded"
    );
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn verified_parallel_settlement_reconciles_workspace_journal_on_restart() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let temp = tempdir().unwrap();
    prepare_directories(temp.path());
    let state = temp.path().join("native_state");
    std::fs::create_dir(&state).unwrap();
    std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o700)).unwrap();
    let make_backend = |wait| {
        Arc::new(WorkspaceBackend {
            inner: DummyBackend::new(DummyScenario::Success, 0),
            captured: Mutex::new(Vec::new()),
            changed: Notify::new(),
            release: Notify::new(),
            wait,
            state_directory: Some(state.clone()),
            state_owner_root: Some(temp.path().to_path_buf()),
        })
    };
    let access = access();
    let client = access.authenticate("client", &[7; 32]).unwrap();
    let waiting_backend = make_backend(true);
    let running = service(temp.path(), waiting_backend.clone(), Arc::clone(&access)).await;
    let a = running
        .start(&client, &body("a", "settle-a", json!({"mode":"new"})))
        .await
        .unwrap();
    let b = running
        .start(&client, &body("b", "settle-b", json!({"mode":"new"})))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let notified = waiting_backend.changed.notified();
            if waiting_backend.captured.lock().unwrap().len() == 2 {
                break;
            }
            notified.await;
        }
    })
    .await
    .unwrap();
    running.cancel(&client, &a.run_id).await.unwrap();
    running.cancel(&client, &b.run_id).await.unwrap();
    assert_eq!(terminal(&running, &client, &a.run_id).await, "cancelled");
    assert_eq!(terminal(&running, &client, &b.run_id).await, "cancelled");
    running.shutdown().await.unwrap();
    let metadata = std::fs::metadata(&state).unwrap();
    let state_journal = temp.path().join(format!(
        ".agent-runner-server-realms/i{:x}-{:x}/journal.jsonl",
        metadata.dev(),
        metadata.ino()
    ));
    assert!(temp.path().join("owner/runner-journal.jsonl").is_file());
    assert!(state_journal.is_file());

    let namespace = Namespace {
        owner_id: "owner".into(),
        client_id: "client".into(),
    };
    let mut store = SqliteStore::open(temp.path().join("runs.sqlite")).unwrap();
    let session_a = a.session_id.unwrap();
    let session_b = b.session_id.unwrap();
    let native = |id: &str| SessionRef {
        runtime: "dummy".into(),
        state_realm: "dummy".into(),
        native_id: id.into(),
    };
    let bindings = vec![
        (namespace.clone(), session_a, native("fixture-a")),
        (namespace.clone(), session_b.clone(), native("fixture-b")),
    ];
    store
        .bind_independent_sessions_for_settlement(&bindings)
        .unwrap();
    assert!(
        store
            .settle_session(
                &namespace,
                &a.run_id,
                CleanupState::Settled,
                SessionDisposition::Reusable
            )
            .unwrap()
    );
    drop(store);

    // The bound but unsettled session stays reserved; its cleanup is settled, so only its own resume is refused.
    let blocked = service(temp.path(), make_backend(false), Arc::clone(&access)).await;
    assert!(
        blocked
            .start(
                &client,
                &body(
                    "b",
                    "still-blocked",
                    json!({"mode":"resume","session_id":session_b})
                )
            )
            .await
            .is_err()
    );
    blocked.shutdown().await.unwrap();
    let mut store = SqliteStore::open(temp.path().join("runs.sqlite")).unwrap();
    assert!(
        store
            .settle_session(
                &namespace,
                &b.run_id,
                CleanupState::Settled,
                SessionDisposition::Reusable
            )
            .unwrap()
    );
    drop(store);

    let resumed = service(temp.path(), make_backend(false), access).await;
    let next = resumed
        .start(
            &client,
            &body("a", "after-settlement", json!({"mode":"new"})),
        )
        .await
        .unwrap();
    assert_eq!(terminal(&resumed, &client, &next.run_id).await, "succeeded");
    resumed.shutdown().await.unwrap();
}

#[tokio::test]
async fn managed_workspace_is_reused_on_resume_and_not_recreated_if_missing() {
    let temp = tempdir().unwrap();
    prepare_directories(temp.path());
    let access = access();
    let client = access.authenticate("client", &[7; 32]).unwrap();
    let backend = backend(false);
    let service = service(temp.path(), backend.clone(), access).await;
    let request = body("scratch", "one", json!({"mode":"new"}));
    let first = service.start(&client, &request).await.unwrap();
    assert_eq!(
        terminal(&service, &client, &first.run_id).await,
        "succeeded"
    );
    assert_eq!(
        service.start(&client, &request).await.unwrap().run_id,
        first.run_id
    );
    let session = first.session_id.unwrap();
    let second = service
        .start(
            &client,
            &body(
                "scratch",
                "two",
                json!({"mode":"resume","session_id":session}),
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        terminal(&service, &client, &second.run_id).await,
        "succeeded"
    );
    let captured = backend.captured.lock().unwrap().clone();
    assert_eq!(captured.len(), 2);
    assert_eq!(captured[0], captured[1]);
    std::fs::remove_dir(&captured[0]).unwrap();
    let third = service
        .start(
            &client,
            &body(
                "scratch",
                "three",
                json!({"mode":"resume","session_id":session}),
            ),
        )
        .await
        .unwrap();
    assert_eq!(terminal(&service, &client, &third.run_id).await, "failed");
    assert!(!captured[0].exists());
    assert_eq!(backend.captured.lock().unwrap().len(), 2);
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancellation_during_preflight_stops_before_managed_directory_creation() {
    let temp = tempdir().unwrap();
    prepare_directories(temp.path());
    let access = access();
    let client = access.authenticate("client", &[7; 32]).unwrap();
    let backend = Arc::new(GatedPreflightBackend {
        inner: WorkspaceBackend {
            inner: DummyBackend::new(DummyScenario::Success, 0),
            captured: Mutex::new(Vec::new()),
            changed: Notify::new(),
            release: Notify::new(),
            wait: false,
            state_directory: None,
            state_owner_root: None,
        },
        ready: Notify::new(),
    });
    let service = RunService::open_with_workspaces(
        temp.path().join("runs.sqlite"),
        access,
        vec![BackendProfile {
            profile_id: "profile".into(),
            revision: "v1".into(),
            state_realm: "dummy".into(),
            backend: backend.clone(),
        }],
        limits(),
        None,
        None,
        Some(config(temp.path())),
    )
    .await
    .unwrap();
    let run = service
        .start(
            &client,
            &body("scratch", "cancel-preflight", json!({"mode":"new"})),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), backend.ready.notified())
        .await
        .unwrap();
    service.cancel(&client, &run.run_id).await.unwrap();
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(2),
            terminal(&service, &client, &run.run_id)
        )
        .await
        .unwrap(),
        "cancelled"
    );
    assert_eq!(
        std::fs::read_dir(temp.path().join("managed"))
            .unwrap()
            .count(),
        0
    );
    assert!(backend.inner.captured.lock().unwrap().is_empty());
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_during_preflight_records_interruption_without_materialization() {
    let temp = tempdir().unwrap();
    prepare_directories(temp.path());
    let access = access();
    let client = access.authenticate("client", &[7; 32]).unwrap();
    let backend = Arc::new(GatedPreflightBackend {
        inner: WorkspaceBackend {
            inner: DummyBackend::new(DummyScenario::Success, 0),
            captured: Mutex::new(Vec::new()),
            changed: Notify::new(),
            release: Notify::new(),
            wait: false,
            state_directory: None,
            state_owner_root: None,
        },
        ready: Notify::new(),
    });
    let service = RunService::open_with_workspaces(
        temp.path().join("runs.sqlite"),
        access,
        vec![BackendProfile {
            profile_id: "profile".into(),
            revision: "v1".into(),
            state_realm: "dummy".into(),
            backend: backend.clone(),
        }],
        limits(),
        None,
        None,
        Some(config(temp.path())),
    )
    .await
    .unwrap();
    let run = service
        .start(
            &client,
            &body("scratch", "shutdown-preflight", json!({"mode":"new"})),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), backend.ready.notified())
        .await
        .unwrap();
    service.shutdown().await.unwrap();
    let database = rusqlite::Connection::open(temp.path().join("runs.sqlite")).unwrap();
    let state: String = database
        .query_row(
            "SELECT state FROM runs WHERE run_id=?1",
            [&run.run_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(state, "interrupted");
    assert_eq!(
        std::fs::read_dir(temp.path().join("managed"))
            .unwrap()
            .count(),
        0
    );
    assert!(backend.inner.captured.lock().unwrap().is_empty());
}

#[tokio::test]
async fn omitting_workspace_registry_cannot_recover_unsettled_workspace_jobs() {
    let temp = tempdir().unwrap();
    prepare_directories(temp.path());
    let access = access();
    let client = access.authenticate("client", &[7; 32]).unwrap();
    let backend = Arc::new(GatedPreflightBackend {
        inner: WorkspaceBackend {
            inner: DummyBackend::new(DummyScenario::Success, 0),
            captured: Mutex::new(Vec::new()),
            changed: Notify::new(),
            release: Notify::new(),
            wait: false,
            state_directory: None,
            state_owner_root: None,
        },
        ready: Notify::new(),
    });
    let database = temp.path().join("runs.sqlite");
    let service = RunService::open_with_workspaces(
        &database,
        access.clone(),
        vec![BackendProfile {
            profile_id: "profile".into(),
            revision: "v1".into(),
            state_realm: "dummy".into(),
            backend: backend.clone(),
        }],
        limits(),
        None,
        None,
        Some(config(temp.path())),
    )
    .await
    .unwrap();
    let run = service
        .start(
            &client,
            &body("scratch", "workspace-pending", json!({"mode":"new"})),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), backend.ready.notified())
        .await
        .unwrap();
    let snapshot = std::fs::read(&database).unwrap();
    service.shutdown().await.unwrap();
    std::fs::write(&database, snapshot).unwrap();
    let reopened = RunService::open(
        &database,
        access,
        vec![BackendProfile {
            profile_id: "profile".into(),
            revision: "v1".into(),
            state_realm: "dummy".into(),
            backend: Arc::new(DummyBackend::new(DummyScenario::Success, 0)),
        }],
        limits(),
    )
    .await;
    assert!(matches!(
        reopened,
        Err(agent_runner_server::application::ServiceError::InvalidConfiguration)
    ));
    let connection = rusqlite::Connection::open(&database).unwrap();
    let state: String = connection
        .query_row(
            "SELECT state FROM runs WHERE run_id=?1",
            [run.run_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(state, "preparing");
}

#[tokio::test]
async fn ownership_directory_remains_bound_to_its_first_database() {
    let temp = tempdir().unwrap();
    prepare_directories(temp.path());
    let access = access();
    let service = service(temp.path(), backend(false), access.clone()).await;
    service.shutdown().await.unwrap();
    let other_path = temp.path().join("another.sqlite");
    let connection = rusqlite::Connection::open(&other_path).unwrap();
    for migration in [
        include_str!("../migrations/0001_initial.sql"),
        include_str!("../migrations/0002_run_service.sql"),
        include_str!("../migrations/0003_skill_registry.sql"),
        include_str!("../migrations/0004_completion_delivery.sql"),
        include_str!("../migrations/0005_run_retention.sql"),
    ] {
        connection.execute_batch(migration).unwrap();
    }
    connection
        .pragma_update(None, "application_id", 0x41525353_i64)
        .unwrap();
    connection.pragma_update(None, "user_version", 5).unwrap();
    drop(connection);
    let result = RunService::open_with_workspaces(
        &other_path,
        access,
        vec![BackendProfile {
            profile_id: "profile".into(),
            revision: "v1".into(),
            state_realm: "dummy".into(),
            backend: backend(false),
        }],
        limits(),
        None,
        None,
        Some(config(temp.path())),
    )
    .await;
    assert!(matches!(
        result,
        Err(agent_runner_server::application::ServiceError::InvalidConfiguration)
    ));
    let connection = rusqlite::Connection::open(&other_path).unwrap();
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(version, 5);
}

#[tokio::test]
async fn a_missing_ownership_journal_is_not_silently_recreated() {
    let temp = tempdir().unwrap();
    prepare_directories(temp.path());
    let access = access();
    let service = service(temp.path(), backend(false), access.clone()).await;
    service.shutdown().await.unwrap();
    let journal = temp.path().join("owner/runner-journal.jsonl");
    std::fs::remove_file(&journal).unwrap();
    let result = RunService::open_with_workspaces(
        temp.path().join("runs.sqlite"),
        access,
        vec![BackendProfile {
            profile_id: "profile".into(),
            revision: "v1".into(),
            state_realm: "dummy".into(),
            backend: backend(false),
        }],
        limits(),
        None,
        None,
        Some(config(temp.path())),
    )
    .await;
    assert!(result.is_err());
    assert!(!journal.exists());
}

#[tokio::test]
async fn truncated_completed_journal_is_rejected_by_its_sequence_anchor() {
    let temp = tempdir().unwrap();
    prepare_directories(temp.path());
    let access = access();
    let client = access.authenticate("client", &[7; 32]).unwrap();
    let service = service(temp.path(), backend(false), access.clone()).await;
    let run = service
        .start(&client, &body("a", "completed", json!({"mode":"new"})))
        .await
        .unwrap();
    assert_eq!(terminal(&service, &client, &run.run_id).await, "succeeded");
    service.shutdown().await.unwrap();
    std::fs::write(
        temp.path().join("owner/runner-journal.jsonl"),
        b"agent-runner-workspace-journal-v1\n",
    )
    .unwrap();
    let result = RunService::open_with_workspaces(
        temp.path().join("runs.sqlite"),
        access,
        vec![BackendProfile {
            profile_id: "profile".into(),
            revision: "v1".into(),
            state_realm: "dummy".into(),
            backend: backend(false),
        }],
        limits(),
        None,
        None,
        Some(config(temp.path())),
    )
    .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn the_same_native_state_realm_cannot_be_claimed_via_a_different_owner_directory() {
    use std::os::unix::fs::PermissionsExt;
    let temp = tempdir().unwrap();
    prepare_directories(temp.path());
    for name in ["owner2", "native_state", "other_parent"] {
        let path = temp.path().join(name);
        std::fs::create_dir(&path).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let make_backend = || {
        Arc::new(WorkspaceBackend {
            inner: DummyBackend::new(DummyScenario::Success, 0),
            captured: Mutex::new(Vec::new()),
            changed: Notify::new(),
            release: Notify::new(),
            wait: false,
            state_directory: Some(temp.path().join("native_state")),
            state_owner_root: Some(temp.path().to_path_buf()),
        })
    };
    let profile = |backend: Arc<WorkspaceBackend>| {
        vec![BackendProfile {
            profile_id: "profile".into(),
            revision: "v1".into(),
            state_realm: "dummy".into(),
            backend,
        }]
    };
    let first = RunService::open_with_workspaces(
        temp.path().join("runs.sqlite"),
        access(),
        profile(make_backend()),
        limits(),
        None,
        None,
        Some(config(temp.path())),
    )
    .await
    .unwrap();
    let mut other_config = config(temp.path());
    other_config.ownership_directory = temp.path().join("owner2");
    let other = RunService::open_with_workspaces(
        temp.path().join("other.sqlite"),
        access(),
        profile(make_backend()),
        limits(),
        None,
        None,
        Some(other_config.clone()),
    )
    .await;
    assert!(matches!(
        other,
        Err(agent_runner_server::application::ServiceError::AlreadyRunning)
    ));
    first.shutdown().await.unwrap();
    let other = RunService::open_with_workspaces(
        temp.path().join("other.sqlite"),
        access(),
        profile(make_backend()),
        limits(),
        None,
        None,
        Some(other_config.clone()),
    )
    .await;
    assert!(matches!(
        other,
        Err(agent_runner_server::application::ServiceError::InvalidConfiguration)
    ));
    std::fs::rename(
        temp.path().join("native_state"),
        temp.path().join("other_parent/renamed_state"),
    )
    .unwrap();
    let renamed_backend = Arc::new(WorkspaceBackend {
        inner: DummyBackend::new(DummyScenario::Success, 0),
        captured: Mutex::new(Vec::new()),
        changed: Notify::new(),
        release: Notify::new(),
        wait: false,
        state_directory: Some(temp.path().join("other_parent/renamed_state")),
        state_owner_root: Some(temp.path().to_path_buf()),
    });
    let renamed = RunService::open_with_workspaces(
        temp.path().join("other.sqlite"),
        access(),
        profile(renamed_backend),
        limits(),
        None,
        None,
        Some(other_config),
    )
    .await;
    assert!(matches!(
        renamed,
        Err(agent_runner_server::application::ServiceError::InvalidConfiguration)
    ));
}

#[tokio::test]
async fn restoring_an_older_database_in_place_cannot_erase_unsettled_launch_evidence() {
    use std::os::unix::fs::MetadataExt;
    let temp = tempdir().unwrap();
    prepare_directories(temp.path());
    let access = access();
    let client = access.authenticate("client", &[7; 32]).unwrap();
    let held_backend = backend(true);
    let service = service(temp.path(), held_backend.clone(), access.clone()).await;
    let database = temp.path().join("runs.sqlite");
    let original_inode = std::fs::metadata(&database).unwrap().ino();
    let before_run = std::fs::read(&database).unwrap();
    let receipt = service
        .start(&client, &body("a", "uncertain", json!({"mode":"new"})))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let notified = held_backend.changed.notified();
            if !held_backend.captured.lock().unwrap().is_empty() {
                break;
            }
            notified.await;
        }
    })
    .await
    .unwrap();
    service.cancel(&client, &receipt.run_id).await.unwrap();
    assert_eq!(
        terminal(&service, &client, &receipt.run_id).await,
        "cancelled"
    );
    service.shutdown().await.unwrap();
    std::fs::write(&database, before_run).unwrap();
    assert_eq!(std::fs::metadata(&database).unwrap().ino(), original_inode);
    let reopened = RunService::open_with_workspaces(
        database,
        access,
        vec![BackendProfile {
            profile_id: "profile".into(),
            revision: "v1".into(),
            state_realm: "dummy".into(),
            backend: backend(false),
        }],
        limits(),
        None,
        None,
        Some(config(temp.path())),
    )
    .await;
    assert!(matches!(
        reopened,
        Err(agent_runner_server::application::ServiceError::InvalidConfiguration)
    ));
}

#[tokio::test]
async fn restoring_a_database_before_a_completed_run_is_rejected() {
    let temp = tempdir().unwrap();
    prepare_directories(temp.path());
    let access = access();
    let client = access.authenticate("client", &[7; 32]).unwrap();
    let service = service(temp.path(), backend(false), access.clone()).await;
    let database = temp.path().join("runs.sqlite");
    let before_run = std::fs::read(&database).unwrap();
    let receipt = service
        .start(
            &client,
            &body("a", "completed-before-restore", json!({"mode":"new"})),
        )
        .await
        .unwrap();
    assert_eq!(
        terminal(&service, &client, &receipt.run_id).await,
        "succeeded"
    );
    service.shutdown().await.unwrap();
    std::fs::write(&database, before_run).unwrap();
    let reopened = RunService::open_with_workspaces(
        database,
        access,
        vec![BackendProfile {
            profile_id: "profile".into(),
            revision: "v1".into(),
            state_realm: "dummy".into(),
            backend: backend(false),
        }],
        limits(),
        None,
        None,
        Some(config(temp.path())),
    )
    .await;
    assert!(reopened.is_err());
}

fn profiles(backend: Arc<WorkspaceBackend>) -> Vec<BackendProfile> {
    vec![BackendProfile {
        profile_id: "profile".into(),
        revision: "v1".into(),
        state_realm: "dummy".into(),
        backend,
    }]
}

fn maintenance(root: &Path, backend: Arc<WorkspaceBackend>) -> Result<Maintenance, ServiceError> {
    Maintenance::open(
        &root.join("runs.sqlite"),
        &profiles(backend),
        Some(config(root)),
    )
}

fn native_state_backend(root: &Path, wait: bool) -> Arc<WorkspaceBackend> {
    use std::os::unix::fs::PermissionsExt;
    let state = root.join("native_state");
    if !state.exists() {
        std::fs::create_dir(&state).unwrap();
        std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    Arc::new(WorkspaceBackend {
        inner: DummyBackend::new(DummyScenario::Success, 0),
        captured: Mutex::new(Vec::new()),
        changed: Notify::new(),
        release: Notify::new(),
        wait,
        state_directory: Some(state),
        state_owner_root: Some(root.to_path_buf()),
    })
}

#[tokio::test]
async fn maintenance_abandons_an_unbound_session_and_unblocks_the_realm() {
    let temp = tempdir().unwrap();
    prepare_directories(temp.path());
    let access = access();
    let client = access.authenticate("client", &[7; 32]).unwrap();
    let waiting = native_state_backend(temp.path(), true);
    let running = service(temp.path(), waiting.clone(), Arc::clone(&access)).await;
    let first = running
        .start(&client, &body("a", "unbound", json!({"mode":"new"})))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let notified = waiting.changed.notified();
            if !waiting.captured.lock().unwrap().is_empty() {
                break;
            }
            notified.await;
        }
    })
    .await
    .unwrap();
    running.cancel(&client, &first.run_id).await.unwrap();
    assert_eq!(
        terminal(&running, &client, &first.run_id).await,
        "cancelled"
    );
    assert!(
        running
            .start(&client, &body("b", "blocked", json!({"mode":"new"})))
            .await
            .is_err()
    );
    assert!(matches!(
        maintenance(temp.path(), native_state_backend(temp.path(), false)),
        Err(ServiceError::AlreadyRunning)
    ));
    running.shutdown().await.unwrap();

    let mut tool = maintenance(temp.path(), native_state_backend(temp.path(), false)).unwrap();
    let entries = tool.quarantined().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].run_id, first.run_id);
    assert_eq!(entries[0].kind, QuarantineKind::UnboundSession);
    assert!(entries[0].native_id.is_none());
    let namespace = entries[0].namespace.clone();
    assert_eq!(
        tool.workspace_directory(&entries[0]),
        Some(temp.path().join("a").canonicalize().unwrap())
    );
    // A process still working in the workspace keeps the run quarantined until it exits.
    let mut lingering = std::process::Command::new("sleep")
        .arg("30")
        .current_dir(temp.path().join("a"))
        .spawn()
        .unwrap();
    assert!(matches!(
        tool.release(&namespace, &first.run_id),
        Err(ReleaseError::ProcessesRemain(_, found)) if found.iter().any(|(pid, _)| *pid == lingering.id())
    ));
    lingering.kill().unwrap();
    lingering.wait().unwrap();
    assert_eq!(
        tool.release(&namespace, &first.run_id).unwrap(),
        Release::Abandoned
    );
    assert!(tool.quarantined().unwrap().is_empty());
    assert!(tool.release(&namespace, &first.run_id).is_err());
    drop(tool);

    let resumed = service(
        temp.path(),
        native_state_backend(temp.path(), false),
        access,
    )
    .await;
    let next = resumed
        .start(&client, &body("b", "after-abandon", json!({"mode":"new"})))
        .await
        .unwrap();
    assert_eq!(terminal(&resumed, &client, &next.run_id).await, "succeeded");
    let session = first.session_id.unwrap();
    assert!(
        resumed
            .start(
                &client,
                &body(
                    "a",
                    "resume-abandoned",
                    json!({"mode":"resume","session_id":session})
                )
            )
            .await
            .is_err()
    );
    resumed.shutdown().await.unwrap();
}

#[tokio::test]
async fn maintenance_makes_a_bound_session_resumable_again() {
    let temp = tempdir().unwrap();
    prepare_directories(temp.path());
    let access = access();
    let client = access.authenticate("client", &[7; 32]).unwrap();
    let delayed = Arc::new(WorkspaceBackend {
        inner: DummyBackend::new(DummyScenario::DelayedSuccess, 0),
        captured: Mutex::new(Vec::new()),
        changed: Notify::new(),
        release: Notify::new(),
        wait: false,
        state_directory: None,
        state_owner_root: None,
    });
    let running = service(temp.path(), delayed, Arc::clone(&access)).await;
    let first = running
        .start(&client, &body("a", "bound", json!({"mode":"new"})))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while running
            .get_run(&client, &first.run_id)
            .await
            .unwrap()
            .unwrap()
            .state
            != "running"
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    // The dummy reports its native session right after it starts and then sleeps.
    tokio::time::sleep(Duration::from_millis(200)).await;
    running.cancel(&client, &first.run_id).await.unwrap();
    assert_eq!(
        terminal(&running, &client, &first.run_id).await,
        "cancelled"
    );
    let session = first.session_id.clone().unwrap();
    let resume = |key: &str| body("a", key, json!({"mode":"resume","session_id":session}));
    assert!(
        running
            .start(&client, &resume("resume-blocked"))
            .await
            .is_err()
    );
    running.shutdown().await.unwrap();

    let mut tool = maintenance(temp.path(), backend(false)).unwrap();
    let entries = tool.quarantined().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].kind, QuarantineKind::BoundSession);
    assert!(entries[0].native_id.is_some());
    let namespace = entries[0].namespace.clone();
    assert_eq!(
        tool.release(&namespace, &first.run_id).unwrap(),
        Release::Resumable
    );
    drop(tool);

    let resumed = service(temp.path(), backend(false), access).await;
    let next = resumed
        .start(&client, &resume("resume-after-release"))
        .await
        .unwrap();
    assert_eq!(next.session_id.as_deref(), Some(session.as_str()));
    assert_eq!(terminal(&resumed, &client, &next.run_id).await, "succeeded");
    resumed.shutdown().await.unwrap();
}
