use tempfile::tempdir;

use crate::{
    core::{CleanupState, ExecutionMetadata, SessionDisposition, SessionRef},
    storage::{
        AdmissionOutcome, Namespace, SqliteStore, StorageError, TerminalState,
        service::{Candidate, ServiceAdmission, StopCause},
    },
};
use serde_json::json;

fn admission(key: &str) -> ServiceAdmission {
    ServiceAdmission {
        namespace: Namespace {
            owner_id: "owner".into(),
            client_id: "client".into(),
        },
        idempotency_key: key.into(),
        normalized_request: key.as_bytes().to_vec(),
        manifest: b"manifest".to_vec(),
        metadata: ExecutionMetadata {
            runtime: "dummy".into(),
            synthetic: true,
            scenario: Some("success".into()),
        },
        credential_id: "credential".into(),
        credential_generation: "generation_1".into(),
        profile_id: "profile".into(),
        profile_revision: "profile_1".into(),
        workspace_id: "workspace".into(),
        state_realm: "dummy".into(),
        resume_session_id: None,
        deadline_ms: 10_000,
        max_outstanding: 8,
        skills: None,
        may_use_skills: true,
        hooks: None,
        may_use_hooks: false,
        destination_grants: Default::default(),
        delivery: None,
        skill_bounds: crate::storage::skills::SkillBounds {
            markdown: 1024,
            metadata: 4096,
            identifier: 128,
        },
    }
}

fn success() -> Candidate {
    Candidate {
        state: TerminalState::Succeeded,
        details: json!({"output": "done"}),
        cleanup: CleanupState::Settled,
        disposition: SessionDisposition::Reusable,
    }
}

#[test]
fn skill_free_prompt_respects_current_size_limit() {
    let mut store = SqliteStore::open_in_memory().unwrap();
    let mut input = admission("bounded-empty-bundle");
    input.manifest = br#"{"version":2}"#.to_vec();
    store.service_admit(&input).unwrap();
    let job = store.service_claim(1_000).unwrap().unwrap();
    assert!(matches!(
        store.service_prepare_prompt(&job, "eleven bytes", 5, input.skill_bounds, 1_001),
        Err(StorageError::InvalidInput("prompt limit"))
    ));
    let count: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM attempt_prompt_snapshots", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(count, 0);
}

#[test]
fn stored_run_skill_references_follow_their_applied_order() {
    use crate::storage::SkillRef;
    let mut store = SqliteStore::open_in_memory().unwrap();
    let mut input = admission("ordered-skills");
    let refs = vec![
        SkillRef {
            skill_id: "z".into(),
            version: "1".into(),
        },
        SkillRef {
            skill_id: "a".into(),
            version: "1".into(),
        },
    ];
    for reference in &refs {
        store
            .register_skill(
                &input.namespace,
                reference,
                "instruction",
                &[],
                input.skill_bounds,
            )
            .unwrap();
    }
    input.skills = Some(refs.clone());
    let AdmissionOutcome::Accepted(run) = store.service_admit(&input).unwrap() else {
        panic!()
    };
    assert_eq!(
        store.get_run_skills(&input.namespace, &run.run_id).unwrap(),
        refs
    );
}

#[test]
fn recovery_reuses_the_committed_prompt_before_launch() {
    use crate::storage::SkillRef;
    let mut store = SqliteStore::open_in_memory().unwrap();
    let mut input = admission("snapshot-recovery");
    let reference = SkillRef {
        skill_id: "report".into(),
        version: "1".into(),
    };
    store
        .register_skill(
            &input.namespace,
            &reference,
            "literal instructions",
            &[],
            input.skill_bounds,
        )
        .unwrap();
    input.skills = Some(vec![reference]);
    input.manifest = br#"{"version":2}"#.to_vec();
    store.service_admit(&input).unwrap();
    let first = store.service_claim(1_000).unwrap().unwrap();
    let snapshot = store
        .service_prepare_prompt(&first, "literal request", 4096, input.skill_bounds, 1_001)
        .unwrap()
        .unwrap();
    assert!(snapshot.0.contains("literal instructions"));
    assert!(snapshot.1);
    store.service_recover(1_002).unwrap();
    let recovered = store.service_claim(1_003).unwrap().unwrap();
    assert_ne!(first.claim_token, recovered.claim_token);
    assert_eq!(
        store
            .service_prepare_prompt(
                &recovered,
                "literal request",
                4096,
                input.skill_bounds,
                1_004
            )
            .unwrap()
            .unwrap(),
        snapshot
    );
    let count: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM attempt_prompt_snapshots", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(count, 1);
    assert!(store.service_launch(&recovered, 1_005).unwrap());
}

#[test]
fn claim_launch_and_cancel_are_fenced_in_the_same_store() {
    let mut store = SqliteStore::open_in_memory().unwrap();
    let first = admission("first");
    let AdmissionOutcome::Accepted(run) = store.service_admit(&first).unwrap() else {
        panic!()
    };
    assert!(matches!(
        store.service_admit(&first).unwrap(),
        AdmissionOutcome::Existing(_)
    ));
    let job = store.service_claim(1_000).unwrap().unwrap();
    assert!(store.service_claim(1_000).unwrap().is_none());
    let mut stale = job.clone();
    stale.claim_token.push('x');
    assert!(matches!(
        store.service_launch(&stale, 1_001),
        Err(StorageError::InvalidState)
    ));
    assert!(store.service_launch(&job, 1_001).unwrap());
    assert!(matches!(
        store.service_launch(&job, 1_002),
        Err(StorageError::InvalidState)
    ));
    let native = SessionRef {
        runtime: "dummy".into(),
        state_realm: "dummy".into(),
        native_id: "native".into(),
    };
    store
        .service_event(&job, "run.session", b"{}", Some(&native), 8)
        .unwrap();
    assert_eq!(
        store
            .service_stop(&first.namespace, &run.run_id, StopCause::Cancelled, 1_003)
            .unwrap(),
        Some(StopCause::Cancelled)
    );
    store.service_finish(&job, &success(), 1_004, 1024).unwrap();
    let result = store
        .get_result(&first.namespace, &run.run_id)
        .unwrap()
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&result.payload).unwrap();
    assert_eq!(payload["state"], "cancelled");
    assert!(payload["details"].is_null());
    assert_eq!(
        store
            .service_stop(&first.namespace, &run.run_id, StopCause::Cancelled, 1_005)
            .unwrap(),
        None
    );
    assert!(matches!(
        store.service_finish(&job, &success(), 1_006, 1024),
        Err(StorageError::InvalidState)
    ));
}

#[test]
fn launch_checks_wall_clock_after_waiting_for_the_database_write_lock() {
    use crate::storage::service::ServiceClock;
    use std::{
        sync::{Arc, Barrier},
        thread,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };
    let temp = tempdir().unwrap();
    let path = temp.path().join("deadline.sqlite");
    let mut input = admission("deadline");
    let mut store = SqliteStore::open(&path).unwrap();
    input.deadline_ms = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
        + 300;
    store.service_admit(&input).unwrap();
    let job = store.service_claim(ServiceClock::Wall).unwrap().unwrap();
    let lock = rusqlite::Connection::open(&path).unwrap();
    lock.execute_batch("BEGIN IMMEDIATE").unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let signal = Arc::clone(&barrier);
    let worker = thread::spawn(move || {
        signal.wait();
        store.service_launch(&job, ServiceClock::Wall).unwrap()
    });
    barrier.wait();
    thread::sleep(Duration::from_millis(400));
    lock.execute_batch("COMMIT").unwrap();
    assert!(!worker.join().unwrap());
    let store = SqliteStore::open(&path).unwrap();
    let saved = store
        .service_replay(
            &input.namespace,
            &input.idempotency_key,
            &input.normalized_request,
        )
        .unwrap()
        .unwrap();
    assert_eq!(saved.state, "timed_out");
}

#[test]
fn terminal_storage_failure_rolls_back_and_does_not_launch_again() {
    let mut store = SqliteStore::open_in_memory().unwrap();
    let input = admission("failure");
    store.service_admit(&input).unwrap();
    let job = store.service_claim(1000).unwrap().unwrap();
    store.service_launch(&job, 1001).unwrap();
    let native = SessionRef {
        runtime: "dummy".into(),
        state_realm: "dummy".into(),
        native_id: "native".into(),
    };
    store
        .service_event(&job, "run.session", b"{}", Some(&native), 8)
        .unwrap();
    store.connection.execute_batch("CREATE TRIGGER reject_result BEFORE INSERT ON results BEGIN SELECT RAISE(ABORT, 'injected result failure'); END;").unwrap();
    assert!(store.service_finish(&job, &success(), 1002, 1024).is_err());
    assert_eq!(
        store
            .get_run(&input.namespace, &job.run_id)
            .unwrap()
            .unwrap()
            .state,
        "running"
    );
    assert!(
        store
            .get_result(&input.namespace, &job.run_id)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .get_session(&input.namespace, &job.session_id)
            .unwrap()
            .unwrap()
            .reserved_run_id
            .as_deref(),
        Some(job.run_id.as_str())
    );
    assert!(store.service_claim(1003).unwrap().is_none());
    store
        .connection
        .execute_batch("DROP TRIGGER reject_result")
        .unwrap();
    store.service_finish(&job, &success(), 1004, 1024).unwrap();
    let events = store
        .get_events(&input.namespace, &job.run_id, None)
        .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == "run.finished")
            .count(),
        1
    );
}

#[test]
fn expired_deadline_wins_over_a_later_cancel() {
    let mut store = SqliteStore::open_in_memory().unwrap();
    let mut first = admission("first");
    first.deadline_ms = 1_500;
    let AdmissionOutcome::Accepted(run) = store.service_admit(&first).unwrap() else {
        panic!()
    };
    assert_eq!(
        store
            .service_stop(&first.namespace, &run.run_id, StopCause::Cancelled, 1_501)
            .unwrap(),
        Some(StopCause::TimedOut)
    );
    assert_eq!(
        store
            .get_run(&first.namespace, &run.run_id)
            .unwrap()
            .unwrap()
            .state,
        "timed_out"
    );
}

#[test]
fn recovery_requeues_unlaunched_but_never_repeats_a_launch_intent() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("recovery.sqlite");
    let first = admission("first");
    let mut store = SqliteStore::open(&path).unwrap();
    let AdmissionOutcome::Accepted(run) = store.service_admit(&first).unwrap() else {
        panic!()
    };
    let old = store.service_claim(1_000).unwrap().unwrap();
    drop(store);
    let mut store = SqliteStore::open(&path).unwrap();
    store.service_recover(1_001).unwrap();
    assert!(matches!(
        store.service_launch(&old, 1_002),
        Err(StorageError::InvalidState)
    ));
    let current = store.service_claim(1_002).unwrap().unwrap();
    assert_eq!(current.run_id, run.run_id);
    assert!(store.service_launch(&current, 1_003).unwrap());
    drop(store);
    let mut store = SqliteStore::open(&path).unwrap();
    store.service_recover(1_004).unwrap();
    assert_eq!(
        store
            .get_run(&first.namespace, &run.run_id)
            .unwrap()
            .unwrap()
            .state,
        "interrupted"
    );
    assert!(store.service_claim(1_005).unwrap().is_none());
    let session = store
        .get_session(&first.namespace, &current.session_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        session.reserved_run_id.as_deref(),
        Some(run.run_id.as_str())
    );
    assert_eq!(session.cleanup.as_deref(), Some("uncertain"));
    assert!(matches!(
        store.service_admit(&admission("other")),
        Err(StorageError::SessionBusy)
    ));
}

#[test]
fn new_unidentified_session_cannot_launch_while_known_session_is_reserved() {
    let mut store = SqliteStore::open_in_memory().unwrap();
    let first = admission("first");
    store.service_admit(&first).unwrap();
    let original = store.service_claim(1_000).unwrap().unwrap();
    store.service_launch(&original, 1_001).unwrap();
    let native = SessionRef {
        runtime: "dummy".into(),
        state_realm: "dummy".into(),
        native_id: "native".into(),
    };
    store
        .service_event(&original, "run.session", b"{}", Some(&native), 8)
        .unwrap();
    store
        .service_finish(&original, &success(), 1_002, 1024)
        .unwrap();
    let mut resume = admission("resume");
    resume.resume_session_id = Some(original.session_id.clone());
    store.service_admit(&resume).unwrap();
    let resumed = store.service_claim(1_003).unwrap().unwrap();
    store.service_launch(&resumed, 1_004).unwrap();
    store.service_admit(&admission("new")).unwrap();
    assert!(store.service_claim(1_005).unwrap().is_none());
    store
        .service_finish(&resumed, &success(), 1_006, 1024)
        .unwrap();
    assert!(store.service_claim(1_007).unwrap().is_some());
}

#[test]
fn queued_unstarted_session_does_not_block_settlement_of_uncertain_native_state() {
    let mut store = SqliteStore::open_in_memory().unwrap();
    let first = admission("first");
    store.service_admit(&first).unwrap();
    store.service_admit(&admission("second")).unwrap();
    let job = store.service_claim(1000).unwrap().unwrap();
    store.service_launch(&job, 1001).unwrap();
    let candidate = Candidate {
        state: TerminalState::Failed,
        details: json!({}),
        cleanup: CleanupState::Uncertain,
        disposition: SessionDisposition::Uncertain,
    };
    store.service_finish(&job, &candidate, 1002, 1024).unwrap();
    let native = SessionRef {
        runtime: "dummy".into(),
        state_realm: "dummy".into(),
        native_id: "observed_later".into(),
    };
    store
        .bind_native_session(&first.namespace, &job.session_id, &native)
        .unwrap();
    assert!(
        store
            .settle_session(
                &first.namespace,
                &job.run_id,
                CleanupState::Settled,
                SessionDisposition::Reusable
            )
            .unwrap()
    );
    assert!(store.service_claim(1003).unwrap().is_some());
}

#[tokio::test]
async fn recovered_work_rejects_a_reissued_credential_generation_before_backend_start() {
    use crate::{
        access::{
            AccessLimits, ClientAccessControl, ClientRegistration, CredentialSecret,
            ExecutionProfile, Scope,
        },
        application::{BackendProfile, RunLimits, RunService},
        runtimes::dummy::{DummyBackend, DummyScenario},
    };
    use std::{
        sync::Arc,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };
    let temp = tempdir().unwrap();
    let path = temp.path().join("reissued.sqlite");
    let mut input = admission("queued");
    input.credential_generation = "old_generation".into();
    input.deadline_ms = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
        + 5000;
    input.manifest = serde_json::to_vec(&json!({"version": 1, "profile_revision": "profile_1", "prompt": "literal", "runtime": "dummy", "synthetic": true, "scenario": "success", "state_realm": "dummy", "resume": false})).unwrap();
    let mut store = SqliteStore::open(&path).unwrap();
    let AdmissionOutcome::Accepted(run) = store.service_admit(&input).unwrap() else {
        panic!()
    };
    drop(store);
    let access = Arc::new(
        ClientAccessControl::new(
            [ClientRegistration::new(
                "credential",
                CredentialSecret::new([7; 32]),
                input.namespace.clone(),
                [Scope::RunStart, Scope::RunRead],
                ["profile".into()],
            )
            .with_generation("new_generation")],
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
    );
    let client = access.authenticate("credential", &[7; 32]).unwrap();
    let backend = Arc::new(DummyBackend::new(DummyScenario::Success, 8));
    let service = RunService::open(
        &path,
        access,
        vec![BackendProfile {
            profile_id: "profile".into(),
            revision: "profile_1".into(),
            state_realm: "dummy".into(),
            backend: backend.clone(),
        }],
        RunLimits {
            max_outstanding: 4,
            max_workers: 1,
            database_queue: 8,
            event_buffer: 8,
            max_events_per_run: 8,
            max_event_bytes: 512,
            max_result_bytes: 1024,
            max_page_size: 8,
            run_timeout: Duration::from_secs(5),
            cleanup_grace: Duration::from_secs(1),
            poll_interval: Duration::from_millis(5),
        },
    )
    .await
    .unwrap();
    let state = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let state = service
                .get_run(&client, &run.run_id)
                .await
                .unwrap()
                .unwrap()
                .state;
            if state == "failed" {
                break state;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(state, "failed");
    assert!(backend.captured_inputs().is_empty());
    service.shutdown().await.unwrap();
}

#[test]
fn fenced_proof_of_no_backend_poll_releases_a_committed_launch_intent() {
    let mut store = SqliteStore::open_in_memory().unwrap();
    let input = admission("first");
    store.service_admit(&input).unwrap();
    let job = store.service_claim(1000).unwrap().unwrap();
    store.service_launch(&job, 1001).unwrap();
    let mut stale = job.clone();
    stale.claim_token.push('x');
    assert!(matches!(
        store.service_proven_unstarted(&stale, 1002),
        Err(StorageError::InvalidState)
    ));
    store.service_proven_unstarted(&job, 1002).unwrap();
    let session = store
        .get_session(&input.namespace, &job.session_id)
        .unwrap()
        .unwrap();
    assert!(session.reserved_run_id.is_none());
    assert_eq!(session.disposition.as_deref(), Some("not_started"));
    assert_eq!(
        store
            .get_run(&input.namespace, &job.run_id)
            .unwrap()
            .unwrap()
            .state,
        "interrupted"
    );
}

#[test]
fn proven_unstarted_resume_releases_only_its_own_reservation() {
    let mut store = SqliteStore::open_in_memory().unwrap();
    let first = admission("first");
    store.service_admit(&first).unwrap();
    let original = store.service_claim(1_000).unwrap().unwrap();
    store.service_launch(&original, 1_001).unwrap();
    let native = SessionRef {
        runtime: "dummy".into(),
        state_realm: "dummy".into(),
        native_id: "native".into(),
    };
    store
        .service_event(&original, "run.session", b"{}", Some(&native), 8)
        .unwrap();
    store
        .service_finish(&original, &success(), 1_002, 1024)
        .unwrap();
    let mut resume = admission("resume");
    resume.resume_session_id = Some(original.session_id.clone());
    let AdmissionOutcome::Accepted(run) = store.service_admit(&resume).unwrap() else {
        panic!()
    };
    store
        .service_stop(&resume.namespace, &run.run_id, StopCause::Cancelled, 1_003)
        .unwrap();
    let session = store
        .get_session(&resume.namespace, &original.session_id)
        .unwrap()
        .unwrap();
    assert!(session.reserved_run_id.is_none());
    assert_eq!(session.disposition.as_deref(), Some("reusable"));
    let mut next = admission("next");
    next.resume_session_id = Some(original.session_id);
    assert!(matches!(
        store.service_admit(&next).unwrap(),
        AdmissionOutcome::Accepted(_)
    ));
}

fn bind_completion(
    store: &SqliteStore,
    run_id: &str,
    binding: &str,
    connector: &str,
    events: &str,
) {
    use rusqlite::params;
    let ns = ["owner", "client"];
    store.connection.execute(
        "INSERT INTO completion_hook_versions (owner_id, client_id, hook_id, version, destination_id, destination_revision, destination_snapshot, events, projection) VALUES (?1, ?2, ?3, '1', ?3, '1', X'7B7D', ?4, 'metadata')",
        params![ns[0], ns[1], binding, events],
    ).unwrap();
    store.connection.execute(
        "INSERT INTO hook_bindings (owner_id, client_id, run_id, binding_id, hook_revision, destination_revision, projection) VALUES (?1, ?2, ?3, ?4, '1', '1', X'6D65746164617461')",
        params![ns[0], ns[1], run_id, binding],
    ).unwrap();
    store.connection.execute(
        "INSERT INTO completion_bindings (owner_id, client_id, run_id, binding_id, hook_id, hook_version, destination_id, destination_revision, destination_snapshot, connector, events, projection, max_payload_bytes, max_attempts, delivery_ttl_ms, retention_ms) VALUES (?1, ?2, ?3, ?4, ?4, '1', ?4, '1', X'7B7D', ?5, ?6, 'metadata', 4096, 2, 10000, 10000)",
        params![ns[0], ns[1], run_id, binding, connector, events],
    ).unwrap();
}

#[test]
fn completion_outbox_and_inbox_follow_the_final_cancelled_state() {
    let mut store = SqliteStore::open_in_memory().unwrap();
    let input = admission("completion-cancel");
    let AdmissionOutcome::Accepted(run) = store.service_admit(&input).unwrap() else {
        panic!()
    };
    bind_completion(
        &store,
        &run.run_id,
        "a-webhook",
        "webhook_test",
        r#"["cancelled"]"#,
    );
    bind_completion(
        &store,
        &run.run_id,
        "b-success",
        "webhook_test",
        r#"["succeeded"]"#,
    );
    bind_completion(&store, &run.run_id, "z-inbox", "inbox", r#"["cancelled"]"#);
    let job = store.service_claim(1_000).unwrap().unwrap();
    store.service_launch(&job, 1_001).unwrap();
    store
        .service_stop(&input.namespace, &run.run_id, StopCause::Cancelled, 1_002)
        .unwrap();
    store.service_finish(&job, &success(), 1_003, 1024).unwrap();
    let deliveries = store.get_deliveries(&input.namespace, &run.run_id).unwrap();
    assert_eq!(deliveries.len(), 1);
    let value: serde_json::Value = serde_json::from_slice(&deliveries[0].payload).unwrap();
    assert_eq!(value["state"], "cancelled");
    assert_eq!(value["synthetic"], true);
    assert_eq!(value["result_revision"], "1");
    assert!(value.get("details").is_none());
    let inbox = store
        .get_inbox_items(&input.namespace, &run.run_id)
        .unwrap();
    assert_eq!(inbox.len(), 1);
    let value: serde_json::Value = serde_json::from_slice(&inbox[0].payload).unwrap();
    assert_eq!(value["state"], "cancelled");
    let released: i64 = store.connection.query_row(
        "SELECT charged FROM completion_bindings WHERE run_id = ?1 AND binding_id = 'b-success'",
        [&run.run_id], |row| row.get(0),
    ).unwrap();
    assert_eq!(released, 0);
}

#[test]
fn completion_inbox_failure_rolls_back_result_outbox_and_session() {
    let mut store = SqliteStore::open_in_memory().unwrap();
    let input = admission("completion-rollback");
    let AdmissionOutcome::Accepted(run) = store.service_admit(&input).unwrap() else {
        panic!()
    };
    bind_completion(
        &store,
        &run.run_id,
        "a-webhook",
        "webhook_test",
        r#"["succeeded"]"#,
    );
    bind_completion(&store, &run.run_id, "z-inbox", "inbox", r#"["succeeded"]"#);
    let job = store.service_claim(1_000).unwrap().unwrap();
    store.service_launch(&job, 1_001).unwrap();
    store.connection.execute_batch("CREATE TRIGGER reject_completion_inbox BEFORE INSERT ON completion_inbox BEGIN SELECT RAISE(ABORT, 'injected inbox failure'); END;").unwrap();
    assert!(store.service_finish(&job, &success(), 1_002, 1024).is_err());
    assert_eq!(
        store
            .get_run(&input.namespace, &run.run_id)
            .unwrap()
            .unwrap()
            .state,
        "running"
    );
    assert!(
        store
            .get_result(&input.namespace, &run.run_id)
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .get_deliveries(&input.namespace, &run.run_id)
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .get_inbox_items(&input.namespace, &run.run_id)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .get_session(&input.namespace, &job.session_id)
            .unwrap()
            .unwrap()
            .reserved_run_id
            .as_deref(),
        Some(run.run_id.as_str())
    );
    store
        .connection
        .execute_batch("DROP TRIGGER reject_completion_inbox")
        .unwrap();
    store.service_finish(&job, &success(), 1_003, 1024).unwrap();
    assert_eq!(
        store
            .get_deliveries(&input.namespace, &run.run_id)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        store
            .get_inbox_items(&input.namespace, &run.run_id)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn completion_intents_cover_queued_cancel_timeout_and_prelaunch_failure() {
    for state in ["cancelled", "timed_out", "failed"] {
        let mut store = SqliteStore::open_in_memory().unwrap();
        let mut input = admission(state);
        if state == "timed_out" {
            input.deadline_ms = 1005;
        }
        let AdmissionOutcome::Accepted(run) = store.service_admit(&input).unwrap() else {
            panic!()
        };
        bind_completion(
            &store,
            &run.run_id,
            "hook",
            "webhook_test",
            &format!("[\"{state}\"]"),
        );
        match state {
            "cancelled" => {
                store
                    .service_stop(&input.namespace, &run.run_id, StopCause::Cancelled, 1001)
                    .unwrap();
            }
            "timed_out" => {
                store.service_deadline_maintenance(1006).unwrap();
            }
            "failed" => {
                let job = store.service_claim(1000).unwrap().unwrap();
                let candidate = Candidate {
                    state: TerminalState::Failed,
                    details: json!({"error":"prelaunch"}),
                    cleanup: CleanupState::Settled,
                    disposition: SessionDisposition::NotApplicable,
                };
                store.service_finish(&job, &candidate, 1001, 1024).unwrap();
            }
            _ => unreachable!(),
        }
        let deliveries = store.get_deliveries(&input.namespace, &run.run_id).unwrap();
        assert_eq!(deliveries.len(), 1, "{state}");
        let payload: serde_json::Value = serde_json::from_slice(&deliveries[0].payload).unwrap();
        assert_eq!(payload["state"], state);
    }
}

#[test]
fn recovery_creates_interrupted_delivery_and_never_relaunches() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("completion-recovery.sqlite");
    let mut store = SqliteStore::open(&path).unwrap();
    let input = admission("completion-recovery");
    let AdmissionOutcome::Accepted(run) = store.service_admit(&input).unwrap() else {
        panic!()
    };
    bind_completion(
        &store,
        &run.run_id,
        "hook",
        "webhook_test",
        r#"["interrupted"]"#,
    );
    let job = store.service_claim(1000).unwrap().unwrap();
    store.service_launch(&job, 1001).unwrap();
    drop(store);
    let mut store = SqliteStore::open(&path).unwrap();
    store.service_recover(1002).unwrap();
    let deliveries = store.get_deliveries(&input.namespace, &run.run_id).unwrap();
    assert_eq!(deliveries.len(), 1);
    let payload: serde_json::Value = serde_json::from_slice(&deliveries[0].payload).unwrap();
    assert_eq!(payload["state"], "interrupted");
    assert_eq!(payload["cleanup"], "uncertain");
    assert!(store.service_claim(1003).unwrap().is_none());
}

#[test]
fn inbox_ack_and_expiry_preserve_namespace_and_release_quota() {
    let mut store = SqliteStore::open_in_memory().unwrap();
    let input = admission("inbox-expiry");
    let AdmissionOutcome::Accepted(run) = store.service_admit(&input).unwrap() else {
        panic!()
    };
    bind_completion(&store, &run.run_id, "hook", "inbox", r#"["cancelled"]"#);
    store
        .service_stop(&input.namespace, &run.run_id, StopCause::Cancelled, 1001)
        .unwrap();
    assert!(
        store
            .completion_inbox_page(&input.namespace, 0, 1, 1002, 4096)
            .unwrap()
            .is_empty()
    );
    store
        .connection
        .execute("UPDATE completion_inbox SET status='available'", [])
        .unwrap();
    let items = store
        .completion_inbox_page(&input.namespace, 0, 1, 1002, 4096)
        .unwrap();
    let id = items[0]["inbox_id"].as_str().unwrap();
    let other = Namespace {
        owner_id: "owner".into(),
        client_id: "other".into(),
    };
    assert!(
        store
            .completion_inbox_page(&other, 0, 1, 1002, 4096)
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        store.acknowledge_completion_inbox(&other, id, 1002),
        Err(StorageError::NotFound)
    ));
    store
        .acknowledge_completion_inbox(&input.namespace, id, 1002)
        .unwrap();
    store
        .acknowledge_completion_inbox(&input.namespace, id, 1003)
        .unwrap();
    store.completion_maintenance(11002).unwrap();
    assert!(
        store
            .completion_inbox_page(&input.namespace, 0, 1, 11002, 4096)
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        store.acknowledge_completion_inbox(&input.namespace, id, 11002),
        Err(StorageError::NotFound)
    ));
    let charged: bool = store
        .connection
        .query_row(
            "SELECT charged FROM completion_bindings WHERE run_id=?1",
            [&run.run_id],
            |r| r.get(0),
        )
        .unwrap();
    assert!(!charged);
}

#[test]
fn protected_library_inbox_does_not_expose_pending_or_skipped_items() {
    use crate::access::{
        AccessLimits, AuthorizedStore, ClientAccessControl, ClientRegistration, CredentialSecret,
        Scope,
    };
    let mut store = SqliteStore::open_in_memory().unwrap();
    let input = admission("protected-inbox");
    let AdmissionOutcome::Accepted(run) = store.service_admit(&input).unwrap() else {
        panic!()
    };
    bind_completion(&store, &run.run_id, "hook", "inbox", r#"["cancelled"]"#);
    store
        .service_stop(&input.namespace, &run.run_id, StopCause::Cancelled, 1001)
        .unwrap();
    store
        .connection
        .execute("UPDATE completion_inbox SET expires_ms=9000000000000", [])
        .unwrap();
    let access = ClientAccessControl::new(
        [ClientRegistration::new(
            "reader",
            CredentialSecret::new([7; 32]),
            input.namespace.clone(),
            [Scope::InboxRead],
            [],
        )],
        [],
        AccessLimits {
            max_request_bytes: 1024,
            max_skill_bytes: 1024,
            max_identifier_bytes: 128,
        },
    )
    .unwrap();
    let client = access.authenticate("reader", &[7; 32]).unwrap();
    for status in ["pending", "skipped", "available"] {
        store
            .connection
            .execute("UPDATE completion_inbox SET status=?1", [status])
            .unwrap();
        let authorized = AuthorizedStore::new(&access, &client, &mut store);
        let items = authorized.get_inbox_items(&run.run_id).unwrap();
        assert_eq!(items.len(), usize::from(status == "available"));
    }
}

#[test]
fn completion_filter_uses_result_limit_failure_not_backend_success() {
    let mut store = SqliteStore::open_in_memory().unwrap();
    let input = admission("result-too-large");
    let AdmissionOutcome::Accepted(run) = store.service_admit(&input).unwrap() else {
        panic!()
    };
    bind_completion(&store, &run.run_id, "hook", "webhook_test", r#"["failed"]"#);
    let job = store.service_claim(1000).unwrap().unwrap();
    store.service_launch(&job, 1001).unwrap();
    let large = Candidate {
        state: TerminalState::Succeeded,
        details: json!({"output":"x".repeat(1024)}),
        cleanup: CleanupState::Settled,
        disposition: SessionDisposition::Uncertain,
    };
    store.service_finish(&job, &large, 1002, 128).unwrap();
    let deliveries = store.get_deliveries(&input.namespace, &run.run_id).unwrap();
    assert_eq!(deliveries.len(), 1);
    let payload: serde_json::Value = serde_json::from_slice(&deliveries[0].payload).unwrap();
    assert_eq!(payload["state"], "failed");
}
