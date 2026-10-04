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
        workspace: None,
        independent_new_sessions: false,
        resume_session_id: None,
        resume_native_id: None,
        deadline_ms: 10_000,
        retention_ms: None,
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

#[test]
fn parallel_unidentified_sessions_require_complete_verified_batch_before_settlement() {
    let mut store = SqliteStore::open_in_memory().unwrap();
    let mut first = admission("independent-first");
    first.independent_new_sessions = true;
    let mut second = admission("independent-second");
    second.independent_new_sessions = true;
    store.service_admit(&first).unwrap();
    let job_a = store.service_claim(1000).unwrap().unwrap();
    store.service_launch(&job_a, 1001).unwrap();
    store.service_admit(&second).unwrap();
    let job_b = store.service_claim(1002).unwrap().unwrap();
    store.service_launch(&job_b, 1003).unwrap();
    let uncertain = Candidate {
        state: TerminalState::Failed,
        details: json!({}),
        cleanup: CleanupState::Uncertain,
        disposition: SessionDisposition::Uncertain,
    };
    store
        .service_finish(&job_a, &uncertain, 1004, 1024)
        .unwrap();
    let native_a = SessionRef {
        runtime: "dummy".into(),
        state_realm: "dummy".into(),
        native_id: "observed-a".into(),
    };
    let native_b = SessionRef {
        runtime: "dummy".into(),
        state_realm: "dummy".into(),
        native_id: "observed-b".into(),
    };
    let bindings = vec![
        (
            first.namespace.clone(),
            job_a.session_id.clone(),
            native_a.clone(),
        ),
        (second.namespace.clone(), job_b.session_id.clone(), native_b),
    ];
    assert!(matches!(
        store.bind_native_session(&first.namespace, &job_a.session_id, &native_a),
        Err(StorageError::SessionBusy)
    ));
    assert!(matches!(
        store.bind_independent_sessions_for_settlement(&bindings),
        Err(StorageError::SessionBusy)
    ));
    assert!(
        store
            .get_session(&first.namespace, &job_a.session_id)
            .unwrap()
            .unwrap()
            .session
            .native_id
            .is_none()
    );
    store
        .service_finish(&job_b, &uncertain, 1005, 1024)
        .unwrap();
    assert!(matches!(
        store.bind_independent_sessions_for_settlement(&bindings[..1]),
        Err(StorageError::SessionBusy)
    ));
    let mut duplicates = bindings.clone();
    duplicates[1].2.native_id = native_a.native_id.clone();
    assert!(matches!(
        store.bind_independent_sessions_for_settlement(&duplicates),
        Err(StorageError::InvalidInput(_))
    ));
    store
        .connection
        .execute(
            "UPDATE service_jobs SET independent_new_sessions=0 WHERE run_id=?1",
            [&job_b.run_id],
        )
        .unwrap();
    assert!(matches!(
        store.bind_independent_sessions_for_settlement(&bindings),
        Err(StorageError::InvalidState)
    ));
    assert!(
        store
            .get_session(&first.namespace, &job_a.session_id)
            .unwrap()
            .unwrap()
            .session
            .native_id
            .is_none()
    );
    store
        .connection
        .execute(
            "UPDATE service_jobs SET independent_new_sessions=1 WHERE run_id=?1",
            [&job_b.run_id],
        )
        .unwrap();
    store
        .bind_independent_sessions_for_settlement(&bindings)
        .unwrap();
    assert!(
        store
            .settle_session(
                &first.namespace,
                &job_a.run_id,
                CleanupState::Settled,
                SessionDisposition::Reusable
            )
            .unwrap()
    );
    assert!(matches!(
        store.service_admit(&admission("still-quarantined")),
        Err(StorageError::SessionBusy)
    ));
    assert!(
        store
            .settle_session(
                &second.namespace,
                &job_b.run_id,
                CleanupState::Settled,
                SessionDisposition::Reusable
            )
            .unwrap()
    );
    assert!(store.service_admit(&admission("after-settlement")).is_ok());
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

#[test]
fn native_aliases_share_the_session_reservation_and_profile_binding() {
    let mut store = SqliteStore::open_in_memory().unwrap();
    let first = admission("native-first");
    let AdmissionOutcome::Accepted(original) = store.service_admit(&first).unwrap() else {
        panic!()
    };
    let job = store.service_claim(1000).unwrap().unwrap();
    store.service_launch(&job, 1001).unwrap();
    let native = SessionRef {
        runtime: "dummy".into(),
        state_realm: "dummy".into(),
        native_id: "native-identity".into(),
    };
    store
        .service_event(&job, "run.session", b"{}", Some(&native), 8)
        .unwrap();
    let mut alias = admission("native-alias");
    alias.profile_id = "another-profile".into();
    alias.resume_native_id = Some(native.native_id.clone());
    assert!(matches!(
        store.service_admit(&alias),
        Err(StorageError::SessionBusy)
    ));
    let mut other = admission("foreign-native");
    other.namespace.client_id = "other".into();
    other.resume_native_id = Some(native.native_id.clone());
    assert!(matches!(
        store.service_admit(&other),
        Err(StorageError::NotFound)
    ));
    store.service_finish(&job, &success(), 1002, 1024).unwrap();
    assert!(matches!(
        store.service_admit(&alias),
        Err(StorageError::SessionIncompatible)
    ));
    alias.profile_id = first.profile_id.clone();
    let AdmissionOutcome::Accepted(resumed) = store.service_admit(&alias).unwrap() else {
        panic!()
    };
    assert_eq!(resumed.session_id, original.session_id);
}

#[test]
fn result_retention_expires_once_and_preserves_replay_and_session() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("retention.sqlite");
    let mut store = SqliteStore::open(&path).unwrap();
    let mut input = admission("retained-run");
    input.retention_ms = Some(100);
    let AdmissionOutcome::Accepted(run) = store.service_admit(&input).unwrap() else {
        panic!()
    };
    let job = store.service_claim(1000).unwrap().unwrap();
    store.service_launch(&job, 1001).unwrap();
    store.service_finish(&job, &success(), 1002, 1024).unwrap();
    let retention = &super::retention::expired;
    assert!(!retention(&store.connection, &input.namespace, &run.run_id, 1101).unwrap());
    assert!(retention(&store.connection, &input.namespace, &run.run_id, 1102).unwrap());
    assert!(retention(&store.connection, &input.namespace, &run.run_id, 1).unwrap());
    drop(store);
    let mut store = SqliteStore::open(&path).unwrap();
    store.run_retention_maintenance(1103).unwrap();
    assert!(
        store
            .get_result(&input.namespace, &run.run_id)
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .get_events(&input.namespace, &run.run_id, None)
            .unwrap()
            .iter()
            .all(|e| e.payload.is_empty())
    );
    assert!(matches!(
        store
            .service_result_view(&input.namespace, &run.run_id, 1024, 1024)
            .unwrap(),
        crate::storage::ResultView::Expired(_)
    ));
    assert!(matches!(
        store.service_event_view(&input.namespace, &run.run_id, None, 10, 1024),
        Err(StorageError::Expired)
    ));
    let replay = store
        .service_replay(
            &input.namespace,
            &input.idempotency_key,
            &input.normalized_request,
        )
        .unwrap()
        .unwrap();
    assert_eq!(replay.run_id, run.run_id);
    assert!(matches!(
        store.service_replay(&input.namespace, &input.idempotency_key, b"different"),
        Err(StorageError::IdempotencyConflict)
    ));
    assert_eq!(
        store
            .get_session(&input.namespace, &job.session_id)
            .unwrap()
            .unwrap()
            .reserved_run_id
            .as_deref(),
        Some(run.run_id.as_str())
    );
}

#[test]
fn result_retention_waits_for_its_own_inbox_without_blocking_another_run() {
    let mut store = SqliteStore::open_in_memory().unwrap();
    let mut first = admission("retained-inbox");
    first.retention_ms = Some(1);
    let AdmissionOutcome::Accepted(run) = store.service_admit(&first).unwrap() else {
        panic!()
    };
    bind_completion(
        &store,
        &run.run_id,
        "retained-inbox",
        "inbox",
        r#"["succeeded"]"#,
    );
    let job = store.service_claim(1000).unwrap().unwrap();
    store.service_launch(&job, 1001).unwrap();
    store.service_finish(&job, &success(), 1002, 1024).unwrap();
    let mut second = admission("other-realm");
    second.state_realm = "other".into();
    second.retention_ms = Some(1);
    let AdmissionOutcome::Accepted(other) = store.service_admit(&second).unwrap() else {
        panic!()
    };
    let other_job = store.service_claim(1003).unwrap().unwrap();
    store.service_launch(&other_job, 1004).unwrap();
    store
        .service_finish(&other_job, &success(), 1005, 1024)
        .unwrap();
    assert!(
        !super::retention::expired(&store.connection, &first.namespace, &run.run_id, 1007).unwrap()
    );
    store.run_retention_maintenance(1007).unwrap();
    assert!(
        store
            .get_result(&second.namespace, &other.run_id)
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .get_result(&first.namespace, &run.run_id)
            .unwrap()
            .is_some()
    );
    store.completion_maintenance(12000).unwrap();
    assert!(
        super::retention::expired(&store.connection, &first.namespace, &run.run_id, 12000).unwrap()
    );
    store.run_retention_maintenance(12000).unwrap();
    assert!(
        store
            .get_result(&first.namespace, &run.run_id)
            .unwrap()
            .is_none()
    );
}

#[test]
fn recovery_samples_retention_clock_after_the_write_lock() {
    use crate::storage::service::ServiceClock;
    use std::{
        sync::{Arc, Barrier},
        thread,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };
    let temp = tempdir().unwrap();
    let path = temp.path().join("recovery-retention.sqlite");
    let mut store = SqliteStore::open(&path).unwrap();
    let mut input = admission("recovery-clock");
    input.deadline_ms = i64::MAX - 10000;
    input.retention_ms = Some(2000);
    let AdmissionOutcome::Accepted(run) = store.service_admit(&input).unwrap() else {
        panic!()
    };
    let job = store.service_claim(1000).unwrap().unwrap();
    store.service_launch(&job, 1001).unwrap();
    let lock = rusqlite::Connection::open(&path).unwrap();
    lock.execute_batch("BEGIN IMMEDIATE").unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let signal = barrier.clone();
    let worker = thread::spawn(move || {
        signal.wait();
        store.service_recover(ServiceClock::Wall).unwrap();
    });
    barrier.wait();
    thread::sleep(Duration::from_millis(100));
    let release_ms = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    lock.execute_batch("COMMIT").unwrap();
    worker.join().unwrap();
    let store = SqliteStore::open(&path).unwrap();
    let expires: i64 = store
        .connection
        .query_row(
            "SELECT expires_ms FROM run_retention WHERE run_id=?1",
            [&run.run_id],
            |row| row.get(0),
        )
        .unwrap();
    assert!(expires >= release_ms + 2000);
    assert_eq!(
        store
            .get_run(&input.namespace, &run.run_id)
            .unwrap()
            .unwrap()
            .state,
        "interrupted"
    );
}

#[test]
fn result_retention_storage_failures_roll_back_terminal_and_cleanup() {
    let mut store = SqliteStore::open_in_memory().unwrap();
    let mut input = admission("retention-fault");
    input.retention_ms = Some(1);
    let AdmissionOutcome::Accepted(run) = store.service_admit(&input).unwrap() else {
        panic!()
    };
    bind_completion(
        &store,
        &run.run_id,
        "fault-inbox",
        "inbox",
        r#"["succeeded"]"#,
    );
    let job = store.service_claim(1000).unwrap().unwrap();
    store.service_launch(&job, 1001).unwrap();
    store.connection.execute_batch("CREATE TRIGGER ignore_expiry BEFORE UPDATE OF expires_ms ON run_retention BEGIN SELECT RAISE(IGNORE); END;").unwrap();
    assert!(store.service_finish(&job, &success(), 1002, 1024).is_err());
    assert!(
        store
            .get_result(&input.namespace, &run.run_id)
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .get_inbox_items(&input.namespace, &run.run_id)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .get_run(&input.namespace, &run.run_id)
            .unwrap()
            .unwrap()
            .state,
        "running"
    );
    store
        .connection
        .execute_batch("DROP TRIGGER ignore_expiry")
        .unwrap();
    store.service_finish(&job, &success(), 1002, 1024).unwrap();
    store.completion_maintenance(12000).unwrap();
    store.connection.execute_batch("CREATE TRIGGER ignore_result_delete BEFORE DELETE ON results BEGIN SELECT RAISE(IGNORE); END;").unwrap();
    assert!(store.run_retention_maintenance(12000).is_err());
    assert!(
        store
            .get_result(&input.namespace, &run.run_id)
            .unwrap()
            .is_some()
    );
    let flags: (bool, bool) = store
        .connection
        .query_row(
            "SELECT expired,purged FROM run_retention WHERE run_id=?1",
            [&run.run_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(flags, (false, false));
    store
        .connection
        .execute_batch("DROP TRIGGER ignore_result_delete")
        .unwrap();
    store.run_retention_maintenance(12000).unwrap();
    assert!(
        store
            .get_result(&input.namespace, &run.run_id)
            .unwrap()
            .is_none()
    );
}

#[test]
fn retention_admission_noop_rolls_back_the_entire_run() {
    let mut store = SqliteStore::open_in_memory().unwrap();
    let mut input = admission("retention-insert-noop");
    input.retention_ms = Some(100);
    store.connection.execute_batch("CREATE TRIGGER ignore_retention_insert BEFORE INSERT ON run_retention BEGIN SELECT RAISE(IGNORE); END;").unwrap();
    assert!(matches!(
        store.service_admit(&input),
        Err(StorageError::InvalidState)
    ));
    assert!(
        store
            .service_replay(
                &input.namespace,
                &input.idempotency_key,
                &input.normalized_request
            )
            .unwrap()
            .is_none()
    );
    let count: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM runs", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 0);
}

#[test]
fn ignored_terminal_event_insert_rolls_back_all_completion() {
    let mut store = SqliteStore::open_in_memory().unwrap();
    let input = admission("terminal-event-noop");
    let AdmissionOutcome::Accepted(run) = store.service_admit(&input).unwrap() else {
        panic!()
    };
    let job = store.service_claim(1000).unwrap().unwrap();
    store.service_launch(&job, 1001).unwrap();
    store.connection.execute_batch("CREATE TRIGGER ignore_terminal_event BEFORE INSERT ON events WHEN NEW.kind='run.finished' BEGIN SELECT RAISE(IGNORE); END;").unwrap();
    assert!(matches!(
        store.service_finish(&job, &success(), 1002, 1024),
        Err(StorageError::InvalidState)
    ));
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
        !store
            .get_events(&input.namespace, &run.run_id, None)
            .unwrap()
            .iter()
            .any(|event| event.kind == "run.finished")
    );
}

#[test]
fn ignored_terminal_notification_inserts_never_commit_partial_completion() {
    for table in [
        "delivery_intents",
        "completion_delivery_jobs",
        "inbox_items",
        "completion_inbox",
    ] {
        let mut store = SqliteStore::open_in_memory().unwrap();
        let input = admission("notification-noop");
        let AdmissionOutcome::Accepted(run) = store.service_admit(&input).unwrap() else {
            panic!()
        };
        bind_completion(
            &store,
            &run.run_id,
            "hook-webhook",
            "webhook_test",
            r#"["succeeded"]"#,
        );
        bind_completion(
            &store,
            &run.run_id,
            "hook-inbox",
            "inbox",
            r#"["succeeded"]"#,
        );
        let job = store.service_claim(1000).unwrap().unwrap();
        store.service_launch(&job, 1001).unwrap();
        store.connection.execute_batch(&format!("CREATE TRIGGER ignore_notification BEFORE INSERT ON {table} BEGIN SELECT RAISE(IGNORE); END;")).unwrap();
        assert!(
            matches!(
                store.service_finish(&job, &success(), 1002, 1024),
                Err(StorageError::InvalidState)
            ),
            "{table}"
        );
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
        assert!(
            !store
                .get_events(&input.namespace, &run.run_id, None)
                .unwrap()
                .iter()
                .any(|event| event.kind == "run.finished")
        );
    }
}

#[test]
fn result_retention_does_not_disguise_missing_terminal_data_as_expiry() {
    let mut store = SqliteStore::open_in_memory().unwrap();
    let mut input = admission("corrupt-result");
    input.retention_ms = Some(1);
    let AdmissionOutcome::Accepted(run) = store.service_admit(&input).unwrap() else {
        panic!()
    };
    let job = store.service_claim(1000).unwrap().unwrap();
    store.service_launch(&job, 1001).unwrap();
    store.service_finish(&job, &success(), 1002, 1024).unwrap();
    store
        .connection
        .execute("DELETE FROM results WHERE run_id=?1", [&run.run_id])
        .unwrap();
    assert!(matches!(
        store.service_result_view(&input.namespace, &run.run_id, 1024, 1024),
        Err(StorageError::InvalidState)
    ));
    assert!(matches!(
        store.run_retention_maintenance(1004),
        Err(StorageError::InvalidState)
    ));
}

#[test]
fn validation_failure_reason_survives_result_encoding_limit() {
    let mut store = SqliteStore::open_in_memory().unwrap();
    let input = admission("validation-limit");
    let AdmissionOutcome::Accepted(run) = store.service_admit(&input).unwrap() else {
        panic!()
    };
    let job = store.service_claim(1000).unwrap().unwrap();
    store.service_launch(&job, 1001).unwrap();
    let candidate = Candidate {
        state: TerminalState::Failed,
        details: json!({"validation":{"status":"failed","reason":"schema_mismatch"},"warnings":["x".repeat(1000)],"warning_observation":"partial"}),
        cleanup: CleanupState::Settled,
        disposition: SessionDisposition::Uncertain,
    };
    store.service_finish(&job, &candidate, 1002, 192).unwrap();
    let result = store
        .get_result(&input.namespace, &run.run_id)
        .unwrap()
        .unwrap();
    let result: serde_json::Value = serde_json::from_slice(&result.payload).unwrap();
    assert_eq!(result["details"]["validation"]["reason"], "schema_mismatch");
    assert_eq!(result["details"]["warning_observation"], "partial");
    assert_eq!(result["details"]["warnings_omitted"], true);
    assert_eq!(result["details"]["output_omitted"], true);
}

#[cfg(target_os = "linux")]
fn workspace_registry(root: &std::path::Path) -> crate::workspace::WorkspaceRegistry {
    use crate::workspace::{
        WorkspaceAccess, WorkspaceConfig, WorkspaceEntry, WorkspaceLocation, WorkspaceRegistry,
    };
    use std::os::unix::fs::PermissionsExt;
    let owner = root.join("owner");
    std::fs::create_dir(&owner).unwrap();
    std::fs::set_permissions(&owner, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::create_dir(root.join("a")).unwrap();
    std::fs::create_dir(root.join("a/sub")).unwrap();
    std::fs::create_dir(root.join("b")).unwrap();
    let entry = |id: &str, path: &str, access| WorkspaceEntry {
        id: id.into(),
        revision: "v1".into(),
        access,
        location: WorkspaceLocation::Existing {
            path: root.join(path),
        },
    };
    WorkspaceRegistry::prepare(
        WorkspaceConfig {
            ownership_directory: owner,
            entries: vec![
                entry("read_a", "a", WorkspaceAccess::SharedRead),
                entry("alias_a", "a", WorkspaceAccess::Exclusive),
                entry("sub_a", "a/sub", WorkspaceAccess::Exclusive),
                entry("read_b", "b", WorkspaceAccess::SharedRead),
            ],
        },
        std::path::Path::new("."),
    )
    .unwrap()
}

#[cfg(target_os = "linux")]
fn workspace_admission(
    key: &str,
    id: &str,
    registry: &crate::workspace::WorkspaceRegistry,
) -> ServiceAdmission {
    let mut input = admission(key);
    input.workspace_id = id.into();
    input.workspace = registry.get(id).cloned();
    input.independent_new_sessions = true;
    input
}

#[cfg(target_os = "linux")]
fn finish_before_launch(store: &mut SqliteStore, job: &crate::storage::service::ClaimedJob) {
    store
        .service_finish(
            job,
            &Candidate {
                state: TerminalState::Failed,
                details: json!({"error":"fixture"}),
                cleanup: CleanupState::Settled,
                disposition: SessionDisposition::NotApplicable,
            },
            1001,
            1024,
        )
        .unwrap();
}

#[cfg(target_os = "linux")]
#[test]
fn workspace_claims_parallelize_independent_paths_and_serialize_aliases() {
    let temp = tempdir().unwrap();
    let registry = workspace_registry(temp.path());
    let mut store = SqliteStore::open_in_memory().unwrap();
    let first = workspace_admission("workspace-a", "read_a", &registry);
    let second = workspace_admission("workspace-b", "read_b", &registry);
    let blocked = workspace_admission("workspace-alias", "alias_a", &registry);
    store.service_admit(&first).unwrap();
    store.service_admit(&second).unwrap();
    store.service_admit(&blocked).unwrap();
    let job_a = store.service_claim(1000).unwrap().unwrap();
    let job_b = store.service_claim(1000).unwrap().unwrap();
    assert_eq!(job_a.workspace_id, "read_a");
    assert_eq!(job_b.workspace_id, "read_b");
    assert!(store.service_claim(1000).unwrap().is_none());
    finish_before_launch(&mut store, &job_a);
    assert_eq!(
        store.service_claim(1002).unwrap().unwrap().workspace_id,
        "alias_a"
    );
    finish_before_launch(&mut store, &job_b);
}

#[cfg(target_os = "linux")]
#[test]
fn waiting_exclusive_workspace_prevents_new_readers_from_bypassing_it() {
    let temp = tempdir().unwrap();
    let registry = workspace_registry(temp.path());
    let mut store = SqliteStore::open_in_memory().unwrap();
    store
        .service_admit(&workspace_admission("first-reader", "read_a", &registry))
        .unwrap();
    let first = store.service_claim(1000).unwrap().unwrap();
    store
        .service_admit(&workspace_admission("writer", "sub_a", &registry))
        .unwrap();
    store
        .service_admit(&workspace_admission("later-reader", "read_a", &registry))
        .unwrap();
    assert!(store.service_claim(1000).unwrap().is_none());
    finish_before_launch(&mut store, &first);
    let writer = store.service_claim(1002).unwrap().unwrap();
    assert_eq!(writer.workspace_id, "sub_a");
    assert!(store.service_claim(1002).unwrap().is_none());
    finish_before_launch(&mut store, &writer);
    assert_eq!(
        store.service_claim(1003).unwrap().unwrap().workspace_id,
        "read_a"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn restarted_uncertain_workspace_blocks_a_different_alias() {
    let temp = tempdir().unwrap();
    let registry = workspace_registry(temp.path());
    let mut store = SqliteStore::open_in_memory().unwrap();
    store
        .service_admit(&workspace_admission("launched", "read_a", &registry))
        .unwrap();
    let first = store.service_claim(1000).unwrap().unwrap();
    store.service_launch(&first, 1001).unwrap();
    let mut waiting = workspace_admission("waiting", "alias_a", &registry);
    waiting.metadata.runtime = "other".into();
    waiting.state_realm = "other".into();
    store.service_admit(&waiting).unwrap();
    store.service_recover(1002).unwrap();
    assert!(store.service_claim(1003).unwrap().is_none());
    let (held, uncertain): (bool, bool) = store
        .connection
        .query_row(
            "SELECT held,uncertain FROM run_workspaces WHERE run_id=?1",
            [&first.run_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert!(held && uncertain);
}

#[cfg(target_os = "linux")]
#[test]
fn renamed_workspace_parent_still_conflicts_with_its_registered_child() {
    use crate::workspace::{
        WorkspaceAccess, WorkspaceConfig, WorkspaceEntry, WorkspaceLocation, WorkspaceRegistry,
    };
    let temp = tempdir().unwrap();
    let registry = workspace_registry(temp.path());
    let mut store = SqliteStore::open_in_memory().unwrap();
    store
        .service_admit(&workspace_admission("original", "read_a", &registry))
        .unwrap();
    let first = store.service_claim(1000).unwrap().unwrap();
    store.service_launch(&first, 1001).unwrap();
    std::fs::rename(temp.path().join("a"), temp.path().join("renamed")).unwrap();
    let renamed = WorkspaceRegistry::prepare(
        WorkspaceConfig {
            ownership_directory: temp.path().join("owner"),
            entries: vec![WorkspaceEntry {
                id: "renamed".into(),
                revision: "v1".into(),
                access: WorkspaceAccess::Exclusive,
                location: WorkspaceLocation::Existing {
                    path: temp.path().join("renamed/sub"),
                },
            }],
        },
        std::path::Path::new("."),
    )
    .unwrap();
    let mut other = workspace_admission("other", "renamed", &renamed);
    other.metadata.runtime = "other".into();
    other.state_realm = "other".into();
    store.service_admit(&other).unwrap();
    assert!(store.service_claim(1002).unwrap().is_none());
}

#[cfg(target_os = "linux")]
fn managed_registry(
    root: &std::path::Path,
    capacity: usize,
) -> crate::workspace::WorkspaceRegistry {
    use crate::workspace::{
        WorkspaceAccess, WorkspaceConfig, WorkspaceEntry, WorkspaceLocation, WorkspaceRegistry,
    };
    use std::os::unix::fs::PermissionsExt;
    for name in ["owner", "managed"] {
        let path = root.join(name);
        std::fs::create_dir(&path).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    WorkspaceRegistry::prepare(
        WorkspaceConfig {
            ownership_directory: root.join("owner"),
            entries: vec![WorkspaceEntry {
                id: "scratch".into(),
                revision: "v1".into(),
                access: WorkspaceAccess::Exclusive,
                location: WorkspaceLocation::Managed {
                    root: root.join("managed"),
                    max_sessions: capacity,
                },
            }],
        },
        std::path::Path::new("."),
    )
    .unwrap()
}

#[cfg(target_os = "linux")]
#[test]
fn an_uncreated_managed_directory_releases_its_capacity_without_deleting_existing_data() {
    let temp = tempdir().unwrap();
    let registry = managed_registry(temp.path(), 1);
    let mut store = SqliteStore::open_in_memory().unwrap();
    store
        .service_admit(&workspace_admission("first-managed", "scratch", &registry))
        .unwrap();
    let job = store.service_claim(1000).unwrap().unwrap();
    let plan = registry.get("scratch").unwrap();
    store.workspace_mark_materialization(&job).unwrap();
    let path = plan.resource_path(&job.session_id).unwrap();
    std::fs::create_dir(&path).unwrap();
    assert!(matches!(
        plan.materialize(&job.session_id, None),
        Err(crate::workspace::MaterializeError::NotCreated)
    ));
    store.workspace_clear_uncreated(&job).unwrap();
    finish_before_launch(&mut store, &job);
    assert!(path.is_dir());
    assert!(
        store
            .service_admit(&workspace_admission("next-managed", "scratch", &registry))
            .is_ok()
    );
}

#[cfg(target_os = "linux")]
#[test]
fn materialization_intent_without_a_binding_survives_restart_as_uncertain() {
    let temp = tempdir().unwrap();
    let registry = managed_registry(temp.path(), 1);
    let mut store = SqliteStore::open_in_memory().unwrap();
    store
        .service_admit(&workspace_admission("crash-managed", "scratch", &registry))
        .unwrap();
    let job = store.service_claim(1000).unwrap().unwrap();
    let plan = registry.get("scratch").unwrap();
    store.workspace_mark_materialization(&job).unwrap();
    let directory = plan.materialize(&job.session_id, None).unwrap();
    std::fs::remove_dir(&directory.path).unwrap();
    store.service_recover(1001).unwrap();
    let recovered = store.service_claim(1002).unwrap().unwrap();
    let (binding, intent) = store.workspace_binding(&recovered, plan).unwrap();
    assert!(binding.is_none() && intent);
    assert!(!directory.path.exists());
}

#[cfg(target_os = "linux")]
#[test]
fn uncertain_materialization_is_quarantined_until_explicit_operator_settlement() {
    let temp = tempdir().unwrap();
    let registry = managed_registry(temp.path(), 1);
    let mut store = SqliteStore::open_in_memory().unwrap();
    store
        .service_admit(&workspace_admission(
            "uncertain-materialization",
            "scratch",
            &registry,
        ))
        .unwrap();
    let job = store.service_claim(1000).unwrap().unwrap();
    let plan = registry.get("scratch").unwrap();
    store.workspace_mark_materialization(&job).unwrap();
    let directory = plan.materialize(&job.session_id, None).unwrap();
    let candidate = Candidate {
        state: TerminalState::Failed,
        details: json!({"error":"fixture"}),
        cleanup: CleanupState::Uncertain,
        disposition: SessionDisposition::Uncertain,
    };
    store.service_finish(&job, &candidate, 1001, 1024).unwrap();
    let (held, uncertain): (bool, bool) = store
        .connection
        .query_row(
            "SELECT held,uncertain FROM run_workspaces WHERE run_id=?1",
            [&job.run_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert!(held && uncertain);
    assert!(
        store
            .settle_unstarted_workspace(&job.namespace, &job.run_id)
            .unwrap()
    );
    assert!(directory.path.is_dir());
    let capacity = store.service_admit(&workspace_admission(
        "capacity-still-reserved",
        "scratch",
        &registry,
    ));
    assert!(matches!(capacity, Err(StorageError::QueueFull)));
}

#[cfg(target_os = "linux")]
#[test]
fn an_uncertain_managed_session_does_not_block_an_independent_session_directory() {
    let temp = tempdir().unwrap();
    let registry = managed_registry(temp.path(), 2);
    let mut store = SqliteStore::open_in_memory().unwrap();
    store
        .service_admit(&workspace_admission("first-managed", "scratch", &registry))
        .unwrap();
    let first = store.service_claim(1000).unwrap().unwrap();
    let plan = registry.get("scratch").unwrap();
    store.workspace_mark_materialization(&first).unwrap();
    let directory = plan.materialize(&first.session_id, None).unwrap();
    store
        .workspace_store_binding(&first, plan, &directory)
        .unwrap();
    store.service_launch(&first, 1001).unwrap();
    let mut second = workspace_admission("second-managed", "scratch", &registry);
    second.metadata.runtime = "other".into();
    second.state_realm = "other".into();
    store.service_admit(&second).unwrap();
    store.service_recover(1002).unwrap();
    let next = store.service_claim(1003).unwrap().unwrap();
    assert_ne!(next.session_id, first.session_id);
}

#[cfg(target_os = "linux")]
#[test]
fn journaled_prelaunch_crash_is_quarantined_and_explicitly_settled() {
    use crate::workspace::journal::JournalKey;
    let temp = tempdir().unwrap();
    let registry = managed_registry(temp.path(), 2);
    let path = temp.path().join("runs.sqlite");
    let mut store = SqliteStore::open(&path).unwrap();
    store
        .service_admit(&workspace_admission(
            "prelaunch-crash",
            "scratch",
            &registry,
        ))
        .unwrap();
    let job = store.service_claim(1000).unwrap().unwrap();
    let key = JournalKey {
        owner_id: job.namespace.owner_id.clone(),
        client_id: job.namespace.client_id.clone(),
        run_id: job.run_id.clone(),
        state_realm: job.state_realm.clone(),
    };
    let mut ownership = registry.acquire_owner(&path, &[], &store).unwrap();
    assert!(ownership.start(key, &mut store).unwrap());
    drop(ownership);
    drop(store);
    std::fs::write(
        temp.path().join("owner/runner-journal.seq"),
        b"00000000000000000000\n",
    )
    .unwrap();
    let mut store = SqliteStore::open(&path).unwrap();
    let mut ownership = registry.acquire_owner(&path, &[], &store).unwrap();
    ownership.verify(&store).unwrap();
    ownership.reconcile(&mut store).unwrap();
    let run = store
        .get_run_summary(&job.namespace, &job.run_id)
        .unwrap()
        .unwrap();
    assert_eq!(run.state, "interrupted");
    let (held, uncertain): (bool, bool) = store
        .connection
        .query_row(
            "SELECT held,uncertain FROM run_workspaces WHERE run_id=?1",
            [&job.run_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert!(held && uncertain);
    assert!(
        store
            .settle_unstarted_workspace(&job.namespace, &job.run_id)
            .unwrap()
    );
    ownership.reconcile(&mut store).unwrap();
}

#[cfg(target_os = "linux")]
#[test]
fn bounded_journal_checkpoints_preserve_database_rollback_detection() {
    use crate::workspace::journal::JournalKey;
    let temp = tempdir().unwrap();
    let registry = workspace_registry(temp.path());
    let path = temp.path().join("runs.sqlite");
    let mut store = SqliteStore::open(&path).unwrap();
    let before = std::fs::read(&path).unwrap();
    let mut owner = registry.acquire_owner(&path, &[], &store).unwrap();
    owner.set_journal_capacity(1024);
    for index in 0..8 {
        let input = workspace_admission(&format!("checkpoint-{index}"), "read_a", &registry);
        store.service_admit(&input).unwrap();
        let job = store.service_claim(1000).unwrap().unwrap();
        let key = JournalKey {
            owner_id: job.namespace.owner_id.clone(),
            client_id: job.namespace.client_id.clone(),
            run_id: job.run_id.clone(),
            state_realm: job.state_realm.clone(),
        };
        assert!(owner.start(key.clone(), &mut store).unwrap());
        store.service_launch(&job, 1001).unwrap();
        let native = SessionRef {
            runtime: "dummy".into(),
            state_realm: "dummy".into(),
            native_id: format!("native-{index}"),
        };
        store
            .service_event(&job, "run.session", b"{}", Some(&native), 16)
            .unwrap();
        store.service_finish(&job, &success(), 1002, 1024).unwrap();
        owner.settle(&key).unwrap();
    }
    let epoch: i64 = store
        .connection
        .query_row(
            "SELECT MAX(epoch) FROM workspace_journal_epochs",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(epoch > 0);
    drop(owner);
    drop(store);
    std::fs::write(&path, before).unwrap();
    let store = SqliteStore::open_read_only(&path).unwrap();
    let owner = registry.acquire_owner(&path, &[], &store).unwrap();
    assert!(owner.verify(&store).is_err());
}

#[cfg(target_os = "linux")]
#[test]
fn journal_checkpoint_recovers_each_interrupted_commit_boundary() {
    use crate::workspace::journal::JournalKey;
    for stage in 1..=5 {
        let temp = tempdir().unwrap();
        let registry = workspace_registry(temp.path());
        let path = temp.path().join("runs.sqlite");
        let mut store = SqliteStore::open(&path).unwrap();
        let mut owner = registry.acquire_owner(&path, &[], &store).unwrap();
        owner.set_journal_capacity(1024);
        owner.set_checkpoint_fault(stage);
        let mut interrupted_run = None;
        for index in 0..8 {
            store
                .service_admit(&workspace_admission(
                    &format!("fault-{stage}-{index}"),
                    "read_a",
                    &registry,
                ))
                .unwrap();
            let job = store.service_claim(1000).unwrap().unwrap();
            let key = JournalKey {
                owner_id: job.namespace.owner_id.clone(),
                client_id: job.namespace.client_id.clone(),
                run_id: job.run_id.clone(),
                state_realm: job.state_realm.clone(),
            };
            match owner.start(key.clone(), &mut store) {
                Ok(true) => {
                    store.service_launch(&job, 1001).unwrap();
                    let native = SessionRef {
                        runtime: "dummy".into(),
                        state_realm: "dummy".into(),
                        native_id: format!("fault-native-{stage}-{index}"),
                    };
                    store
                        .service_event(&job, "run.session", b"{}", Some(&native), 16)
                        .unwrap();
                    store.service_finish(&job, &success(), 1002, 1024).unwrap();
                    owner.settle(&key).unwrap();
                }
                Err(_) => {
                    interrupted_run = Some(job.run_id);
                    break;
                }
                Ok(false) => panic!("unexpected capacity with no pending run"),
            }
        }
        let interrupted_run = interrupted_run.expect("checkpoint fault was reached");
        drop(owner);
        drop(store);
        let mut store = SqliteStore::open(&path).unwrap();
        let mut owner = registry.acquire_owner(&path, &[], &store).unwrap();
        owner.verify(&store).unwrap();
        owner.reconcile(&mut store).unwrap();
        store.service_recover(1003).unwrap();
        let recovered = store.service_claim(1004).unwrap().unwrap();
        assert_eq!(recovered.run_id, interrupted_run);
        let key = JournalKey {
            owner_id: recovered.namespace.owner_id.clone(),
            client_id: recovered.namespace.client_id.clone(),
            run_id: recovered.run_id.clone(),
            state_realm: recovered.state_realm.clone(),
        };
        assert!(owner.start(key, &mut store).unwrap());
    }
}

#[cfg(target_os = "linux")]
#[test]
fn rejected_old_database_preserves_checkpoint_recovery_intent() {
    use crate::workspace::journal::JournalKey;
    let temp = tempdir().unwrap();
    let registry = workspace_registry(temp.path());
    let path = temp.path().join("runs.sqlite");
    let mut store = SqliteStore::open(&path).unwrap();
    let old_database = std::fs::read(&path).unwrap();
    let mut owner = registry.acquire_owner(&path, &[], &store).unwrap();
    owner.set_journal_capacity(1024);
    owner.set_checkpoint_fault(2);
    let mut interrupted = false;
    for index in 0..8 {
        store
            .service_admit(&workspace_admission(
                &format!("old-db-{index}"),
                "read_a",
                &registry,
            ))
            .unwrap();
        let job = store.service_claim(1000).unwrap().unwrap();
        let key = JournalKey {
            owner_id: job.namespace.owner_id.clone(),
            client_id: job.namespace.client_id.clone(),
            run_id: job.run_id.clone(),
            state_realm: job.state_realm.clone(),
        };
        match owner.start(key.clone(), &mut store) {
            Ok(true) => {
                store.service_launch(&job, 1001).unwrap();
                let native = SessionRef {
                    runtime: "dummy".into(),
                    state_realm: "dummy".into(),
                    native_id: format!("old-db-native-{index}"),
                };
                store
                    .service_event(&job, "run.session", b"{}", Some(&native), 16)
                    .unwrap();
                store.service_finish(&job, &success(), 1002, 1024).unwrap();
                owner.settle(&key).unwrap();
            }
            Err(_) => {
                interrupted = true;
                break;
            }
            Ok(false) => panic!("unexpected journal capacity"),
        }
    }
    assert!(interrupted);
    drop(owner);
    drop(store);
    let correct_database = std::fs::read(&path).unwrap();
    std::fs::write(&path, old_database).unwrap();
    let readonly = SqliteStore::open_read_only(&path).unwrap();
    assert!(registry.acquire_owner(&path, &[], &readonly).is_err());
    assert!(temp.path().join("owner/runner-journal.intent").exists());
    drop(readonly);
    std::fs::write(&path, correct_database).unwrap();
    let mut store = SqliteStore::open(&path).unwrap();
    let mut owner = registry.acquire_owner(&path, &[], &store).unwrap();
    owner.verify(&store).unwrap();
    owner.reconcile(&mut store).unwrap();
}

#[cfg(target_os = "linux")]
#[test]
fn rolled_back_queued_launch_quarantines_the_entire_unknown_native_realm() {
    use crate::workspace::journal::JournalKey;
    let temp = tempdir().unwrap();
    let registry = workspace_registry(temp.path());
    let path = temp.path().join("runs.sqlite");
    let mut store = SqliteStore::open(&path).unwrap();
    store
        .service_admit(&workspace_admission("before-claim", "read_a", &registry))
        .unwrap();
    let before_claim = std::fs::read(&path).unwrap();
    let job = store.service_claim(1000).unwrap().unwrap();
    let key = JournalKey {
        owner_id: job.namespace.owner_id.clone(),
        client_id: job.namespace.client_id.clone(),
        run_id: job.run_id.clone(),
        state_realm: job.state_realm.clone(),
    };
    let mut owner = registry.acquire_owner(&path, &[], &store).unwrap();
    assert!(owner.start(key, &mut store).unwrap());
    store.service_launch(&job, 1001).unwrap();
    drop(owner);
    drop(store);
    std::fs::write(&path, before_claim).unwrap();
    let mut store = SqliteStore::open(&path).unwrap();
    let mut owner = registry.acquire_owner(&path, &[], &store).unwrap();
    owner.verify(&store).unwrap();
    owner.reconcile(&mut store).unwrap();
    let run = store
        .get_run_summary(&job.namespace, &job.run_id)
        .unwrap()
        .unwrap();
    assert_eq!(run.state, "interrupted");
    for independent in [false, true] {
        let mut other = workspace_admission(
            if independent {
                "independent"
            } else {
                "ordinary"
            },
            "read_b",
            &registry,
        );
        other.independent_new_sessions = independent;
        assert!(matches!(
            store.service_admit(&other),
            Err(StorageError::SessionBusy)
        ));
    }
}

/// Finishes one launched run whose native session was observed with the given cleanup and an unproven disposition.
fn finish_bound_with(
    store: &mut SqliteStore,
    key: &str,
    native: &str,
    cleanup: CleanupState,
) -> String {
    store.service_admit(&admission(key)).unwrap();
    let job = store.service_claim(1_000).unwrap().unwrap();
    store.service_launch(&job, 1_001).unwrap();
    let session = SessionRef {
        runtime: "dummy".into(),
        state_realm: "dummy".into(),
        native_id: native.into(),
    };
    store
        .service_event(&job, "run.session", b"{}", Some(&session), 8)
        .unwrap();
    let candidate = Candidate {
        state: TerminalState::Failed,
        details: json!({"error": "cli_execution_failed"}),
        cleanup,
        disposition: SessionDisposition::Uncertain,
    };
    store.service_finish(&job, &candidate, 1_002, 1024).unwrap();
    job.session_id
}

#[test]
fn settled_cleanup_keeps_only_the_failed_session_reserved() {
    let mut store = SqliteStore::open_in_memory().unwrap();
    let session = finish_bound_with(&mut store, "failed", "native-failed", CleanupState::Settled);
    // No process from the run remains, so other new sessions in the realm are admitted and launched.
    assert!(matches!(
        store.service_admit(&admission("other")),
        Ok(AdmissionOutcome::Accepted(_))
    ));
    let other = store.service_claim(1_003).unwrap().unwrap();
    store.service_launch(&other, 1_004).unwrap();
    store
        .service_finish(&other, &success(), 1_005, 1024)
        .unwrap();
    // The failed session itself stays reserved until its reuse is proven or an operator releases it.
    let mut resume = admission("resume-failed");
    resume.resume_session_id = Some(session);
    assert!(matches!(
        store.service_admit(&resume),
        Err(StorageError::SessionBusy)
    ));
}

#[test]
fn uncertain_cleanup_still_blocks_new_sessions_in_the_realm() {
    let mut store = SqliteStore::open_in_memory().unwrap();
    finish_bound_with(
        &mut store,
        "interrupted",
        "native-interrupted",
        CleanupState::Uncertain,
    );
    assert!(matches!(
        store.service_admit(&admission("other")),
        Err(StorageError::SessionBusy)
    ));
}
