use std::{sync::Arc, time::Duration};

use agent_runner_server::{
    access::{
        AccessError, AccessLimits, ClientAccessControl, ClientRegistration, CredentialSecret,
        ExecutionProfile, Scope,
    },
    application::{BackendProfile, CancelOutcome, RunLimits, RunService, ServiceError},
    core::ExecutionBackend,
    runtimes::dummy::{DummyBackend, DummyScenario},
    storage::{Namespace, SqliteStore, StorageError},
};
use serde_json::json;
use tempfile::tempdir;

fn access() -> Arc<ClientAccessControl> {
    Arc::new(
        ClientAccessControl::new(
            [ClientRegistration::new(
                "client",
                CredentialSecret::new([1; 32]),
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
            .with_generation("generation_1")],
            [ExecutionProfile::dummy(
                "profile",
                ["workspace".into()],
                1024,
            )],
            AccessLimits {
                max_request_bytes: 4096,
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
        max_event_bytes: 512,
        max_result_bytes: 1024,
        max_page_size: 16,
        run_timeout: Duration::from_secs(3),
        cleanup_grace: Duration::from_millis(200),
        poll_interval: Duration::from_millis(5),
    }
}
fn profile(backend: Arc<dyn ExecutionBackend>, revision: &str) -> BackendProfile {
    BackendProfile {
        profile_id: "profile".into(),
        revision: revision.into(),
        state_realm: "dummy".into(),
        backend,
    }
}
fn body(key: &str, session: Option<&str>) -> Vec<u8> {
    let mut request = json!({"profile_id": "profile", "workspace_id": "workspace", "input": {"request": "literal"}, "idempotency_key": key});
    if let Some(id) = session {
        request["session"] = json!({"mode": "resume", "session_id": id});
    }
    serde_json::to_vec(&request).unwrap()
}
async fn terminal(
    service: &RunService,
    client: &agent_runner_server::access::AuthenticatedClient,
    run: &str,
) -> String {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let state = service.get_run(client, run).await.unwrap().unwrap().state;
            if matches!(
                state.as_str(),
                "succeeded" | "failed" | "cancelled" | "timed_out" | "interrupted"
            ) {
                break state;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn resume_is_bound_to_the_original_profile_revision() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("runs.sqlite");
    let access = access();
    let client = access.authenticate("client", &[1; 32]).unwrap();
    let backend = Arc::new(DummyBackend::new(DummyScenario::Success, 16));
    let service = RunService::open(
        &path,
        Arc::clone(&access),
        vec![profile(backend.clone(), "revision_1")],
        limits(),
    )
    .await
    .unwrap();
    let first = service.start(&client, &body("first", None)).await.unwrap();
    assert_eq!(
        terminal(&service, &client, &first.run_id).await,
        "succeeded"
    );
    let session = first.session_id.unwrap();
    let resumed = service
        .start(&client, &body("resume", Some(&session)))
        .await
        .unwrap();
    assert_eq!(
        terminal(&service, &client, &resumed.run_id).await,
        "succeeded"
    );
    assert_eq!(backend.captured_inputs().len(), 2);
    service.shutdown().await.unwrap();
    let service = RunService::open(
        &path,
        access,
        vec![profile(backend, "revision_2")],
        limits(),
    )
    .await
    .unwrap();
    assert!(matches!(
        service
            .start(&client, &body("changed_revision", Some(&session)))
            .await,
        Err(ServiceError::Access(AccessError::Storage(
            StorageError::SessionIncompatible
        )))
    ));
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn event_backpressure_never_publishes_success_and_keeps_unknown_session_reserved() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("runs.sqlite");
    let access = access();
    let client = access.authenticate("client", &[1; 32]).unwrap();
    let backend = Arc::new(DummyBackend::new(DummyScenario::Success, 4));
    let mut config = limits();
    config.event_buffer = 1;
    let service = RunService::open(&path, access, vec![profile(backend, "revision_1")], config)
        .await
        .unwrap();
    let run = service
        .start(&client, &body("overflow", None))
        .await
        .unwrap();
    assert_eq!(terminal(&service, &client, &run.run_id).await, "failed");
    let store = SqliteStore::open(&path).unwrap();
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
    assert_eq!(
        session.reserved_run_id.as_deref(),
        Some(run.run_id.as_str())
    );
    assert_eq!(session.cleanup.as_deref(), Some("uncertain"));
    drop(store);
    assert!(matches!(
        service.start(&client, &body("blocked", None)).await,
        Err(ServiceError::Access(AccessError::Storage(
            StorageError::SessionBusy
        )))
    ));
    assert_eq!(
        service.cancel(&client, &run.run_id).await.unwrap(),
        CancelOutcome::AlreadyTerminal
    );
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn legacy_result_larger_than_the_service_limit_is_not_loaded() {
    use agent_runner_server::{
        core::{CleanupState, SessionDisposition},
        storage::{Admission, Completion, TerminalState},
    };
    let temp = tempdir().unwrap();
    let path = temp.path().join("legacy.sqlite");
    let ns = Namespace {
        owner_id: "owner".into(),
        client_id: "client".into(),
    };
    let backend = Arc::new(DummyBackend::new(DummyScenario::Success, 0));
    let mut store = SqliteStore::open(&path).unwrap();
    store.create_namespace(&ns).unwrap();
    store
        .admit_run(
            &ns,
            &Admission {
                run_id: "legacy".into(),
                attempt_id: "attempt".into(),
                session_id: None,
                idempotency_key: "legacy_key".into(),
                normalized_request: vec![],
                manifest: vec![],
                metadata: backend.metadata(),
                queued_event_id: "queued".into(),
                queued_event_payload: vec![],
                skills: vec![],
                hooks: vec![],
            },
        )
        .unwrap();
    store
        .append_event(&ns, "legacy", "oversized", &"x".repeat(2048), b"{}")
        .unwrap();
    store
        .finish_run(
            &ns,
            &Completion {
                run_id: "legacy".into(),
                attempt_id: "attempt".into(),
                state: TerminalState::Succeeded,
                result_revision: "1".into(),
                result_payload: vec![1; 2048],
                terminal_event_id: "finished".into(),
                terminal_event_payload: vec![],
                cleanup: CleanupState::Settled,
                disposition: SessionDisposition::NotApplicable,
                deliveries: vec![],
                inbox: None,
            },
        )
        .unwrap();
    drop(store);
    let access = access();
    let client = access.authenticate("client", &[1; 32]).unwrap();
    let service = RunService::open(
        &path,
        access,
        vec![profile(backend, "revision_1")],
        limits(),
    )
    .await
    .unwrap();
    assert!(matches!(
        service.get_result(&client, "legacy").await,
        Err(ServiceError::Access(AccessError::Storage(
            StorageError::InvalidInput("result limit")
        )))
    ));
    assert!(matches!(
        service.get_events(&client, "legacy", Some(0), 8).await,
        Err(ServiceError::Access(AccessError::Storage(
            StorageError::InvalidInput("event limit")
        )))
    ));
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn profile_realm_mismatch_is_rejected_before_start() {
    let temp = tempdir().unwrap();
    let mut backend = profile(
        Arc::new(DummyBackend::new(DummyScenario::Success, 0)),
        "revision_1",
    );
    backend.state_realm = "not_dummy".into();
    assert!(matches!(
        RunService::open(
            temp.path().join("runs.sqlite"),
            access(),
            vec![backend],
            limits()
        )
        .await,
        Err(ServiceError::InvalidConfiguration)
    ));
}

#[tokio::test]
async fn cancelled_shutdown_eventually_releases_database_even_when_a_handle_is_retained() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("runs.sqlite");
    let access = access();
    let client = access.authenticate("client", &[1; 32]).unwrap();
    let backend = Arc::new(DummyBackend::new(DummyScenario::Success, 4));
    let service = RunService::open(
        &path,
        access.clone(),
        vec![profile(backend.clone(), "revision_1")],
        limits(),
    )
    .await
    .unwrap();
    let retained = service.handle();
    let blocking = rusqlite::Connection::open(&path).unwrap();
    blocking.execute_batch("BEGIN IMMEDIATE").unwrap();
    let pending = {
        let handle = retained.clone();
        let client = client.clone();
        tokio::spawn(async move { handle.start(&client, &body("shutdown-timeout", None)).await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(50), service.shutdown())
            .await
            .is_err()
    );
    blocking.execute_batch("COMMIT").unwrap();
    drop(blocking);
    let _ = pending.await.unwrap();
    let reopened = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match RunService::open(
                &path,
                access.clone(),
                vec![profile(backend.clone(), "revision_1")],
                limits(),
            )
            .await
            {
                Ok(service) => break service,
                Err(ServiceError::AlreadyRunning) => {
                    tokio::time::sleep(Duration::from_millis(10)).await
                }
                Err(error) => panic!("unexpected reopen error: {error}"),
            }
        }
    })
    .await
    .expect("database owner was not released");
    let replay = reopened
        .start(&client, &body("shutdown-timeout", None))
        .await
        .unwrap();
    assert!(replay.replay);
    assert_eq!(
        terminal(&reopened, &client, &replay.run_id).await,
        "interrupted"
    );
    reopened.shutdown().await.unwrap();
    drop(retained);
}

#[test]
fn worker_join_failure_does_not_leave_the_database_owner_locked() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("runs.sqlite");
    let access = access();
    let backend = Arc::new(DummyBackend::new(DummyScenario::Success, 0));
    let runtime_a = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let service = runtime_a
        .block_on(RunService::open(
            &path,
            access.clone(),
            vec![profile(backend.clone(), "revision_1")],
            limits(),
        ))
        .unwrap();
    let retained = service.handle();
    drop(runtime_a);
    let runtime_b = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    assert!(runtime_b.block_on(service.shutdown()).is_err());
    let reopened = runtime_b.block_on(async {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                match RunService::open(
                    &path,
                    access.clone(),
                    vec![profile(backend.clone(), "revision_1")],
                    limits(),
                )
                .await
                {
                    Ok(service) => break service,
                    Err(ServiceError::AlreadyRunning) => {
                        tokio::time::sleep(Duration::from_millis(10)).await
                    }
                    Err(error) => panic!("unexpected reopen error: {error}"),
                }
            }
        })
        .await
        .expect("database owner remained locked after worker join failure")
    });
    runtime_b.block_on(reopened.shutdown()).unwrap();
    drop(retained);
}
