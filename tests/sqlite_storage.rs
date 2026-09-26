use std::{
    sync::{Arc, Barrier},
    thread,
};

use agent_runner_server::{
    core::{CleanupState, ExecutionMetadata, SessionDisposition, SessionRef},
    storage::{
        Admission, AdmissionOutcome, Completion, DeliveryIntent, HookBinding, Namespace,
        NewInboxItem, NewSession, SkillRef, SkillWrite, SqliteStore, StorageError, TerminalState,
    },
};
use rusqlite::Connection;
use tempfile::tempdir;

fn namespace(client: &str) -> Namespace {
    Namespace {
        owner_id: "owner".into(),
        client_id: client.into(),
    }
}

fn metadata() -> ExecutionMetadata {
    ExecutionMetadata {
        runtime: "dummy".into(),
        synthetic: true,
        scenario: Some("success".into()),
    }
}

fn skill() -> SkillRef {
    SkillRef {
        skill_id: "report".into(),
        version: "1".into(),
    }
}

fn hook() -> HookBinding {
    HookBinding {
        binding_id: "binding-1".into(),
        hook_revision: "hook-v1".into(),
        destination_revision: "destination-v1".into(),
        projection: b"projection-v1".to_vec(),
    }
}

fn session(native_id: Option<&str>) -> NewSession {
    NewSession {
        session_id: "session-1".into(),
        runtime: "dummy".into(),
        state_realm: "test-realm".into(),
        native_id: native_id.map(str::to_owned),
    }
}

fn admission(run_id: &str, key: &str, session_id: Option<&str>) -> Admission {
    Admission {
        run_id: run_id.into(),
        attempt_id: format!("attempt-{run_id}"),
        session_id: session_id.map(str::to_owned),
        idempotency_key: key.into(),
        normalized_request: b"request\0bytes".to_vec(),
        manifest: b"manifest-v1".to_vec(),
        metadata: metadata(),
        queued_event_id: format!("queued-{run_id}"),
        queued_event_payload: b"queued".to_vec(),
        skills: Vec::new(),
        hooks: Vec::new(),
    }
}

fn completion(run_id: &str, has_session: bool) -> Completion {
    Completion {
        run_id: run_id.into(),
        attempt_id: format!("attempt-{run_id}"),
        state: TerminalState::Succeeded,
        result_revision: "result-v1".into(),
        result_payload: b"result\0bytes".to_vec(),
        terminal_event_id: format!("finished-{run_id}"),
        terminal_event_payload: b"finished".to_vec(),
        cleanup: CleanupState::Settled,
        disposition: if has_session {
            SessionDisposition::Reusable
        } else {
            SessionDisposition::NotApplicable
        },
        deliveries: Vec::new(),
        inbox: None,
    }
}

fn delivery(id: &str, binding_id: &str) -> DeliveryIntent {
    DeliveryIntent {
        delivery_id: id.into(),
        binding_id: binding_id.into(),
        payload: b"payload\0bytes".to_vec(),
    }
}

#[test]
fn sqlite_prefix_lookalike_is_not_treated_as_an_internal_table() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("unrelated.sqlite");
    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE sqliteX (value TEXT); INSERT INTO sqliteX VALUES ('preserved');",
        )
        .unwrap();
    drop(connection);
    assert!(matches!(
        SqliteStore::open(&path),
        Err(StorageError::InvalidDatabase)
    ));
    let connection = Connection::open(&path).unwrap();
    let value: String = connection
        .query_row("SELECT value FROM sqliteX", [], |row| row.get(0))
        .unwrap();
    assert_eq!(value, "preserved");
}

#[test]
fn unrelated_or_unidentified_databases_are_rejected_without_modification() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("unrelated.sqlite");
    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch("CREATE TABLE other (value TEXT); INSERT INTO other VALUES ('preserved');")
        .unwrap();
    drop(connection);
    assert!(matches!(
        SqliteStore::open(&path),
        Err(StorageError::InvalidDatabase)
    ));
    let connection = Connection::open(&path).unwrap();
    let value: String = connection
        .query_row("SELECT value FROM other", [], |row| row.get(0))
        .unwrap();
    assert_eq!(value, "preserved");
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(version, 0);
    connection.pragma_update(None, "user_version", 1).unwrap();
    drop(connection);
    assert!(matches!(
        SqliteStore::open(&path),
        Err(StorageError::InvalidDatabase)
    ));
}

#[test]
fn unstarted_session_can_be_released_only_without_recorded_launch_intent() {
    let ns = namespace("client-a");
    let mut store = SqliteStore::open_in_memory().unwrap();
    store.create_namespace(&ns).unwrap();
    store.create_session(&ns, &session(None)).unwrap();
    store
        .admit_run(&ns, &admission("run-1", "key-1", Some("session-1")))
        .unwrap();
    let mut finish = completion("run-1", true);
    finish.state = TerminalState::Cancelled;
    finish.disposition = SessionDisposition::NotApplicable;
    store.finish_run(&ns, &finish).unwrap();
    assert!(
        !store
            .get_attempt(&ns, "run-1", "attempt-run-1")
            .unwrap()
            .unwrap()
            .spawn_intent
    );
    assert!(store.release_unstarted_session(&ns, "run-1").unwrap());
    let saved = store.get_session(&ns, "session-1").unwrap().unwrap();
    assert_eq!(saved.reserved_run_id, None);
    assert_eq!(saved.disposition.as_deref(), Some("not_started"));
    store
        .admit_run(&ns, &admission("run-2", "key-2", Some("session-1")))
        .unwrap();
    assert_eq!(
        store
            .get_session(&ns, "session-1")
            .unwrap()
            .unwrap()
            .disposition,
        None
    );
}

#[test]
fn recorded_launch_intent_prevents_releasing_an_unbound_session() {
    let ns = namespace("client-a");
    let mut store = SqliteStore::open_in_memory().unwrap();
    store.create_namespace(&ns).unwrap();
    store.create_session(&ns, &session(None)).unwrap();
    store
        .admit_run(&ns, &admission("run-1", "key-1", Some("session-1")))
        .unwrap();
    store
        .record_spawn_intent(&ns, "run-1", "attempt-run-1")
        .unwrap();
    assert!(
        store
            .get_attempt(&ns, "run-1", "attempt-run-1")
            .unwrap()
            .unwrap()
            .spawn_intent
    );
    let mut finish = completion("run-1", true);
    finish.state = TerminalState::Cancelled;
    finish.disposition = SessionDisposition::NotApplicable;
    store.finish_run(&ns, &finish).unwrap();
    assert!(!store.release_unstarted_session(&ns, "run-1").unwrap());
    assert_eq!(
        store
            .get_session(&ns, "session-1")
            .unwrap()
            .unwrap()
            .reserved_run_id
            .as_deref(),
        Some("run-1")
    );
    assert!(matches!(
        store.record_spawn_intent(&ns, "run-1", "attempt-run-1"),
        Err(StorageError::AlreadyTerminal)
    ));
}

#[test]
fn admission_commit_failure_rolls_back_every_write() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("storage.sqlite");
    let ns = namespace("client-a");
    let mut store = SqliteStore::open(&path).unwrap();
    store.create_namespace(&ns).unwrap();
    store
        .create_session(&ns, &session(Some("native-1")))
        .unwrap();
    let connection = Connection::open(&path).unwrap();
    connection.execute_batch("CREATE TABLE commit_fault (owner_id TEXT, client_id TEXT, FOREIGN KEY (owner_id, client_id) REFERENCES namespaces (owner_id, client_id) DEFERRABLE INITIALLY DEFERRED); CREATE TRIGGER fail_admission_commit AFTER INSERT ON events WHEN NEW.kind = 'run.queued' BEGIN INSERT INTO commit_fault VALUES ('missing', 'missing'); END;").unwrap();
    let start = admission("run-1", "key-1", Some("session-1"));
    assert!(matches!(
        store.admit_run(&ns, &start),
        Err(StorageError::Database(_))
    ));
    assert!(store.get_run(&ns, "run-1").unwrap().is_none());
    assert!(
        store
            .get_attempt(&ns, "run-1", "attempt-run-1")
            .unwrap()
            .is_none()
    );
    assert!(store.get_events(&ns, "run-1", None).unwrap().is_empty());
    assert_eq!(
        store
            .get_session(&ns, "session-1")
            .unwrap()
            .unwrap()
            .reserved_run_id,
        None
    );
    connection
        .execute_batch("DROP TRIGGER fail_admission_commit;")
        .unwrap();
    assert!(matches!(
        store.admit_run(&ns, &start).unwrap(),
        AdmissionOutcome::Accepted(_)
    ));
}

#[test]
fn completion_commit_failure_rolls_back_every_write() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("storage.sqlite");
    let ns = namespace("client-a");
    let mut store = SqliteStore::open(&path).unwrap();
    store.create_namespace(&ns).unwrap();
    store
        .create_session(&ns, &session(Some("native-1")))
        .unwrap();
    let mut start = admission("run-1", "key-1", Some("session-1"));
    start.hooks.push(hook());
    store.admit_run(&ns, &start).unwrap();
    let connection = Connection::open(&path).unwrap();
    connection.execute_batch("CREATE TABLE commit_fault (owner_id TEXT, client_id TEXT, FOREIGN KEY (owner_id, client_id) REFERENCES namespaces (owner_id, client_id) DEFERRABLE INITIALLY DEFERRED); CREATE TRIGGER fail_completion_commit AFTER INSERT ON inbox_items BEGIN INSERT INTO commit_fault VALUES ('missing', 'missing'); END;").unwrap();
    let mut finish = completion("run-1", true);
    finish.deliveries.push(delivery("delivery-1", "binding-1"));
    finish.inbox = Some(NewInboxItem {
        inbox_id: "inbox-1".into(),
        payload: b"inbox".to_vec(),
    });
    assert!(matches!(
        store.finish_run(&ns, &finish),
        Err(StorageError::Database(_))
    ));
    assert_eq!(
        store.get_run(&ns, "run-1").unwrap().unwrap().state,
        "queued"
    );
    assert_eq!(
        store
            .get_attempt(&ns, "run-1", "attempt-run-1")
            .unwrap()
            .unwrap()
            .state,
        "queued"
    );
    assert!(store.get_result(&ns, "run-1").unwrap().is_none());
    assert_eq!(store.get_events(&ns, "run-1", None).unwrap().len(), 1);
    assert!(store.get_deliveries(&ns, "run-1").unwrap().is_empty());
    assert!(store.get_inbox_items(&ns, "run-1").unwrap().is_empty());
    assert_eq!(
        store
            .get_session(&ns, "session-1")
            .unwrap()
            .unwrap()
            .reserved_run_id
            .as_deref(),
        Some("run-1")
    );
    connection
        .execute_batch("DROP TRIGGER fail_completion_commit;")
        .unwrap();
    store.finish_run(&ns, &finish).unwrap();
    assert_eq!(store.get_deliveries(&ns, "run-1").unwrap().len(), 1);
}

#[test]
fn existing_version_one_database_is_upgraded_without_losing_records() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("v1.sqlite");
    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(include_str!("../migrations/0001_initial.sql"))
        .unwrap();
    connection
        .pragma_update(None, "application_id", 0x41525353_i64)
        .unwrap();
    connection.pragma_update(None, "user_version", 1).unwrap();
    connection
        .execute(
            "INSERT INTO namespaces (owner_id, client_id) VALUES ('owner', 'client-a')",
            [],
        )
        .unwrap();
    connection.execute("INSERT INTO skill_versions (owner_id, client_id, skill_id, version, markdown) VALUES ('owner', 'client-a', 'report', '1', 'preserved')", []).unwrap();
    drop(connection);

    let store = SqliteStore::open(&path).unwrap();
    assert_eq!(
        store
            .get_skill_version(&namespace("client-a"), &skill())
            .unwrap()
            .unwrap()
            .markdown,
        "preserved"
    );
    drop(store);
    let connection = Connection::open(&path).unwrap();
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(version, 2);
    let created: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = 'service_jobs')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(created);
}

#[test]
fn migration_and_records_survive_reopening() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("storage.sqlite");
    let ns = namespace("client-a");
    let mut store = SqliteStore::open(&path).unwrap();
    store.create_namespace(&ns).unwrap();
    assert_eq!(
        store
            .put_skill_version(&ns, &skill(), "# Skill\n원문")
            .unwrap(),
        SkillWrite::Inserted
    );
    store
        .create_session(&ns, &session(Some("native-1")))
        .unwrap();
    let mut start = admission("run-1", "key-1", Some("session-1"));
    start.skills.push(skill());
    start.hooks.push(hook());
    assert!(matches!(
        store.admit_run(&ns, &start).unwrap(),
        AdmissionOutcome::Accepted(_)
    ));
    assert_eq!(
        store
            .append_event(&ns, "run-1", "progress-1", "run.progress", b"progress")
            .unwrap(),
        1
    );
    let mut finish = completion("run-1", true);
    finish.deliveries.push(delivery("delivery-1", "binding-1"));
    finish.inbox = Some(NewInboxItem {
        inbox_id: "inbox-1".into(),
        payload: b"inbox".to_vec(),
    });
    store.finish_run(&ns, &finish).unwrap();
    drop(store);

    let store = SqliteStore::open(&path).unwrap();
    let run = store.get_run(&ns, "run-1").unwrap().unwrap();
    assert_eq!(run.state, "succeeded");
    assert_eq!(run.normalized_request, start.normalized_request);
    assert_eq!(run.manifest, start.manifest);
    assert_eq!(run.metadata, metadata());
    let attempt = store
        .get_attempt(&ns, "run-1", "attempt-run-1")
        .unwrap()
        .unwrap();
    assert_eq!(attempt.state, "succeeded");
    assert_eq!(attempt.metadata, metadata());
    assert_eq!(store.get_run_skills(&ns, "run-1").unwrap(), vec![skill()]);
    assert_eq!(store.get_hook_bindings(&ns, "run-1").unwrap(), vec![hook()]);
    assert_eq!(
        store
            .get_skill_version(&ns, &skill())
            .unwrap()
            .unwrap()
            .markdown,
        "# Skill\n원문"
    );
    let events = store.get_events(&ns, "run-1", None).unwrap();
    assert_eq!(
        events
            .iter()
            .map(|event| event.kind.as_str())
            .collect::<Vec<_>>(),
        vec!["run.queued", "run.progress", "run.finished"]
    );
    assert_eq!(
        events
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert!(events.iter().all(|event| event.metadata == metadata()));
    assert_eq!(store.get_events(&ns, "run-1", Some(0)).unwrap().len(), 2);
    let result = store.get_result(&ns, "run-1").unwrap().unwrap();
    assert_eq!(result.payload, finish.result_payload);
    assert_eq!(result.metadata, metadata());
    let deliveries = store.get_deliveries(&ns, "run-1").unwrap();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].binding, hook());
    assert_eq!(deliveries[0].payload, b"payload\0bytes");
    assert_eq!(deliveries[0].metadata, metadata());
    let inbox = store.get_inbox_items(&ns, "run-1").unwrap();
    assert_eq!(inbox.len(), 1);
    assert_eq!(inbox[0].metadata, metadata());
    assert_eq!(
        store
            .get_session(&ns, "session-1")
            .unwrap()
            .unwrap()
            .reserved_run_id,
        None
    );
}

#[test]
fn replay_returns_original_before_rechecking_changed_inputs() {
    let ns = namespace("client-a");
    let mut store = SqliteStore::open_in_memory().unwrap();
    store.create_namespace(&ns).unwrap();
    store.put_skill_version(&ns, &skill(), "original").unwrap();
    let mut first = admission("run-1", "key-1", None);
    first.skills.push(skill());
    first.hooks.push(hook());
    let AdmissionOutcome::Accepted(original) = store.admit_run(&ns, &first).unwrap() else {
        panic!("expected new admission")
    };

    let mut retry = first.clone();
    retry.run_id.clear();
    retry.attempt_id.clear();
    retry.session_id = Some("missing".into());
    retry.manifest = b"changed".to_vec();
    retry.metadata.runtime.clear();
    retry.skills = vec![SkillRef {
        skill_id: "missing".into(),
        version: "2".into(),
    }];
    retry.hooks.clear();
    assert_eq!(
        store.admit_run(&ns, &retry).unwrap(),
        AdmissionOutcome::Existing(original)
    );
    assert_eq!(store.get_run_skills(&ns, "run-1").unwrap(), vec![skill()]);
    assert_eq!(store.get_hook_bindings(&ns, "run-1").unwrap(), vec![hook()]);
    retry.normalized_request = b"different".to_vec();
    assert!(matches!(
        store.admit_run(&ns, &retry),
        Err(StorageError::IdempotencyConflict)
    ));
}

#[test]
fn failed_admission_rolls_back_run_attempt_event_and_reservation() {
    let ns = namespace("client-a");
    let mut store = SqliteStore::open_in_memory().unwrap();
    store.create_namespace(&ns).unwrap();
    store.create_session(&ns, &session(None)).unwrap();
    let mut input = admission("run-1", "key-1", Some("session-1"));
    input.skills.push(skill());
    assert!(matches!(
        store.admit_run(&ns, &input),
        Err(StorageError::Database(_))
    ));
    assert!(store.get_run(&ns, "run-1").unwrap().is_none());
    assert!(
        store
            .get_attempt(&ns, "run-1", "attempt-run-1")
            .unwrap()
            .is_none()
    );
    assert!(store.get_events(&ns, "run-1", None).unwrap().is_empty());
    assert_eq!(
        store
            .get_session(&ns, "session-1")
            .unwrap()
            .unwrap()
            .reserved_run_id,
        None
    );
    input.skills.clear();
    assert!(matches!(
        store.admit_run(&ns, &input).unwrap(),
        AdmissionOutcome::Accepted(_)
    ));
}

#[test]
fn failed_completion_rolls_back_all_terminal_changes() {
    let ns = namespace("client-a");
    let mut store = SqliteStore::open_in_memory().unwrap();
    store.create_namespace(&ns).unwrap();
    store.create_session(&ns, &session(None)).unwrap();
    let mut start = admission("run-1", "key-1", Some("session-1"));
    start.hooks.push(hook());
    store.admit_run(&ns, &start).unwrap();
    let mut finish = completion("run-1", true);
    finish.deliveries = vec![
        delivery("delivery-1", "binding-1"),
        delivery("delivery-2", "binding-1"),
    ];
    finish.inbox = Some(NewInboxItem {
        inbox_id: "inbox-1".into(),
        payload: b"inbox".to_vec(),
    });
    assert!(matches!(
        store.finish_run(&ns, &finish),
        Err(StorageError::Database(_))
    ));
    assert_eq!(
        store.get_run(&ns, "run-1").unwrap().unwrap().state,
        "queued"
    );
    assert_eq!(
        store
            .get_attempt(&ns, "run-1", "attempt-run-1")
            .unwrap()
            .unwrap()
            .state,
        "queued"
    );
    assert!(store.get_result(&ns, "run-1").unwrap().is_none());
    assert_eq!(store.get_events(&ns, "run-1", None).unwrap().len(), 1);
    assert!(store.get_deliveries(&ns, "run-1").unwrap().is_empty());
    assert!(store.get_inbox_items(&ns, "run-1").unwrap().is_empty());
    assert_eq!(
        store
            .get_session(&ns, "session-1")
            .unwrap()
            .unwrap()
            .reserved_run_id
            .as_deref(),
        Some("run-1")
    );

    finish.deliveries.pop();
    store.finish_run(&ns, &finish).unwrap();
    assert!(matches!(
        store.finish_run(&ns, &finish),
        Err(StorageError::AlreadyTerminal)
    ));
    assert_eq!(store.get_events(&ns, "run-1", None).unwrap().len(), 2);
    assert_eq!(store.get_deliveries(&ns, "run-1").unwrap().len(), 1);
    assert_eq!(store.get_inbox_items(&ns, "run-1").unwrap().len(), 1);
}

#[test]
fn namespaces_isolate_runs_results_events_and_skills() {
    let a = namespace("client-a");
    let b = namespace("client-b");
    let c = namespace("client-c");
    let mut store = SqliteStore::open_in_memory().unwrap();
    for ns in [&a, &b, &c] {
        store.create_namespace(ns).unwrap();
    }
    store.put_skill_version(&a, &skill(), "a").unwrap();
    store.put_skill_version(&b, &skill(), "b").unwrap();
    let mut first = admission("same-run", "same-key", None);
    first.hooks.push(hook());
    store.admit_run(&a, &first).unwrap();
    store
        .admit_run(&b, &admission("same-run", "same-key", None))
        .unwrap();
    let mut finish = completion("same-run", false);
    finish.deliveries.push(delivery("delivery-1", "binding-1"));
    finish.inbox = Some(NewInboxItem {
        inbox_id: "inbox-1".into(),
        payload: b"inbox".to_vec(),
    });
    store.finish_run(&a, &finish).unwrap();
    assert_eq!(
        store.get_run(&a, "same-run").unwrap().unwrap().state,
        "succeeded"
    );
    assert_eq!(
        store.get_run(&b, "same-run").unwrap().unwrap().state,
        "queued"
    );
    assert!(store.get_run(&c, "same-run").unwrap().is_none());
    assert!(store.get_result(&b, "same-run").unwrap().is_none());
    assert_eq!(store.get_events(&b, "same-run", None).unwrap().len(), 1);
    assert!(store.get_deliveries(&b, "same-run").unwrap().is_empty());
    assert!(store.get_inbox_items(&b, "same-run").unwrap().is_empty());
    assert_eq!(
        store
            .get_skill_version(&a, &skill())
            .unwrap()
            .unwrap()
            .markdown,
        "a"
    );
    assert_eq!(
        store
            .get_skill_version(&b, &skill())
            .unwrap()
            .unwrap()
            .markdown,
        "b"
    );
    assert!(store.get_skill_version(&c, &skill()).unwrap().is_none());
}

#[test]
fn immutable_skill_compares_original_text_and_protects_references() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("storage.sqlite");
    let ns = namespace("client-a");
    let mut store = SqliteStore::open(&path).unwrap();
    store.create_namespace(&ns).unwrap();
    assert_eq!(
        store.put_skill_version(&ns, &skill(), "원문\n").unwrap(),
        SkillWrite::Inserted
    );
    assert_eq!(
        store.put_skill_version(&ns, &skill(), "원문\n").unwrap(),
        SkillWrite::Unchanged
    );
    assert!(matches!(
        store.put_skill_version(&ns, &skill(), "변경"),
        Err(StorageError::SkillVersionConflict)
    ));
    let mut start = admission("run-1", "key-1", None);
    start.skills.push(skill());
    store.admit_run(&ns, &start).unwrap();
    let connection = Connection::open(&path).unwrap();
    connection
        .pragma_update(None, "foreign_keys", true)
        .unwrap();
    assert!(
        connection
            .execute("UPDATE skill_versions SET markdown = 'changed'", [])
            .is_err()
    );
    assert!(
        connection
            .execute("DELETE FROM skill_versions", [])
            .is_err()
    );
    assert_eq!(
        store
            .get_skill_version(&ns, &skill())
            .unwrap()
            .unwrap()
            .markdown,
        "원문\n"
    );
}

#[test]
fn uncertain_session_stays_reserved_after_restart_until_observed_settlement() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("storage.sqlite");
    let ns = namespace("client-a");
    let mut store = SqliteStore::open(&path).unwrap();
    store.create_namespace(&ns).unwrap();
    store
        .create_session(&ns, &session(Some("native-1")))
        .unwrap();
    store
        .admit_run(&ns, &admission("run-1", "key-1", Some("session-1")))
        .unwrap();
    let mut finish = completion("run-1", true);
    finish.cleanup = CleanupState::Uncertain;
    finish.disposition = SessionDisposition::Uncertain;
    store.finish_run(&ns, &finish).unwrap();
    drop(store);

    let mut store = SqliteStore::open(&path).unwrap();
    let native = SessionRef {
        runtime: "dummy".into(),
        state_realm: "test-realm".into(),
        native_id: "native-1".into(),
    };
    assert_eq!(
        store
            .get_session_by_native(&ns, &native)
            .unwrap()
            .unwrap()
            .reserved_run_id
            .as_deref(),
        Some("run-1")
    );
    assert!(matches!(
        store.admit_run(&ns, &admission("run-2", "key-2", Some("session-1"))),
        Err(StorageError::SessionBusy)
    ));
    assert!(matches!(
        store.settle_session(
            &ns,
            "run-1",
            CleanupState::Uncertain,
            SessionDisposition::Reusable
        ),
        Err(StorageError::InvalidInput(_))
    ));
    assert!(
        store
            .settle_session(
                &ns,
                "run-1",
                CleanupState::Settled,
                SessionDisposition::Reusable
            )
            .unwrap()
    );
    assert_eq!(
        store.get_run(&ns, "run-1").unwrap().unwrap().state,
        "succeeded"
    );
    assert!(matches!(
        store
            .admit_run(&ns, &admission("run-2", "key-2", Some("session-1")))
            .unwrap(),
        AdmissionOutcome::Accepted(_)
    ));
}

#[test]
fn concurrent_admissions_share_one_idempotent_run() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("storage.sqlite");
    let ns = namespace("client-a");
    SqliteStore::open(&path)
        .unwrap()
        .create_namespace(&ns)
        .unwrap();
    let first = SqliteStore::open(&path).unwrap();
    let second = SqliteStore::open(&path).unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let mut handles = Vec::new();
    for (mut store, run_id) in [(first, "run-1"), (second, "run-2")] {
        let barrier = Arc::clone(&barrier);
        let ns = ns.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            store
                .admit_run(&ns, &admission(run_id, "shared-key", None))
                .unwrap()
        }));
    }
    let outcomes: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let accepted: Vec<_> = outcomes
        .iter()
        .filter_map(|outcome| match outcome {
            AdmissionOutcome::Accepted(run) => Some(run),
            _ => None,
        })
        .collect();
    let existing: Vec<_> = outcomes
        .iter()
        .filter_map(|outcome| match outcome {
            AdmissionOutcome::Existing(run) => Some(run),
            _ => None,
        })
        .collect();
    assert_eq!(accepted.len(), 1);
    assert_eq!(existing.len(), 1);
    assert_eq!(accepted[0], existing[0]);
}

#[test]
fn newer_schema_is_rejected_without_changing_it() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("storage.sqlite");
    drop(SqliteStore::open(&path).unwrap());
    let connection = Connection::open(&path).unwrap();
    connection.pragma_update(None, "user_version", 3).unwrap();
    drop(connection);
    assert!(matches!(
        SqliteStore::open(&path),
        Err(StorageError::UnsupportedSchema(3))
    ));
    let connection = Connection::open(&path).unwrap();
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(version, 3);
}

#[test]
fn reserved_event_names_and_events_after_completion_are_rejected() {
    let ns = namespace("client-a");
    let mut store = SqliteStore::open_in_memory().unwrap();
    store.create_namespace(&ns).unwrap();
    store
        .admit_run(&ns, &admission("run-1", "key-1", None))
        .unwrap();
    assert!(matches!(
        store.append_event(&ns, "run-1", "bad", "run.finished", b"x"),
        Err(StorageError::InvalidInput(_))
    ));
    store.finish_run(&ns, &completion("run-1", false)).unwrap();
    assert!(matches!(
        store.append_event(&ns, "run-1", "later", "run.progress", b"x"),
        Err(StorageError::AlreadyTerminal)
    ));
}

#[test]
fn foreign_skill_and_session_references_cannot_be_used() {
    let a = namespace("client-a");
    let b = namespace("client-b");
    let mut store = SqliteStore::open_in_memory().unwrap();
    store.create_namespace(&a).unwrap();
    store.create_namespace(&b).unwrap();
    store.put_skill_version(&a, &skill(), "a").unwrap();
    store
        .create_session(&a, &session(Some("native-1")))
        .unwrap();
    assert!(matches!(
        store.admit_run(&b, &admission("run-1", "key-1", Some("session-1"))),
        Err(StorageError::NotFound)
    ));
    let mut input = admission("run-1", "key-1", None);
    input.skills.push(skill());
    assert!(matches!(
        store.admit_run(&b, &input),
        Err(StorageError::Database(_))
    ));
    assert!(store.get_run(&b, "run-1").unwrap().is_none());
}

#[test]
fn native_identity_cannot_be_rebound_or_claimed_by_another_namespace() {
    let a = namespace("client-a");
    let b = namespace("client-b");
    let mut store = SqliteStore::open_in_memory().unwrap();
    store.create_namespace(&a).unwrap();
    store.create_namespace(&b).unwrap();
    store.create_session(&a, &session(None)).unwrap();
    let native = SessionRef {
        runtime: "dummy".into(),
        state_realm: "test-realm".into(),
        native_id: "native-1".into(),
    };
    store.bind_native_session(&a, "session-1", &native).unwrap();
    store.bind_native_session(&a, "session-1", &native).unwrap();
    assert!(store.get_session_by_native(&b, &native).unwrap().is_none());
    assert!(matches!(
        store.create_session(&b, &session(Some("native-1"))),
        Err(StorageError::Database(_))
    ));
    let other = SessionRef {
        native_id: "native-2".into(),
        ..native
    };
    assert!(matches!(
        store.bind_native_session(&a, "session-1", &other),
        Err(StorageError::InvalidState)
    ));
}

#[test]
fn concurrent_completions_commit_only_one_terminal_result() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("storage.sqlite");
    let ns = namespace("client-a");
    let mut setup = SqliteStore::open(&path).unwrap();
    setup.create_namespace(&ns).unwrap();
    setup
        .admit_run(&ns, &admission("run-1", "key-1", None))
        .unwrap();
    drop(setup);
    let first = SqliteStore::open(&path).unwrap();
    let second = SqliteStore::open(&path).unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let mut handles = Vec::new();
    for mut store in [first, second] {
        let barrier = Arc::clone(&barrier);
        let ns = ns.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            store.finish_run(&ns, &completion("run-1", false))
        }));
    }
    let outcomes: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert_eq!(outcomes.iter().filter(|outcome| outcome.is_ok()).count(), 1);
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, Err(StorageError::AlreadyTerminal)))
            .count(),
        1
    );
    let store = SqliteStore::open(&path).unwrap();
    assert_eq!(store.get_events(&ns, "run-1", None).unwrap().len(), 2);
    assert!(store.get_result(&ns, "run-1").unwrap().is_some());
}

#[test]
fn unbound_session_remains_reserved_until_identity_is_known_and_settled() {
    let ns = namespace("client-a");
    let mut store = SqliteStore::open_in_memory().unwrap();
    store.create_namespace(&ns).unwrap();
    store.create_session(&ns, &session(None)).unwrap();
    store
        .admit_run(&ns, &admission("run-1", "key-1", Some("session-1")))
        .unwrap();
    store.finish_run(&ns, &completion("run-1", true)).unwrap();
    assert_eq!(
        store.get_run(&ns, "run-1").unwrap().unwrap().state,
        "succeeded"
    );
    let stored = store.get_session(&ns, "session-1").unwrap().unwrap();
    assert_eq!(stored.reserved_run_id.as_deref(), Some("run-1"));
    assert_eq!(stored.disposition.as_deref(), Some("uncertain"));
    assert!(
        !store
            .settle_session(
                &ns,
                "run-1",
                CleanupState::Settled,
                SessionDisposition::Reusable
            )
            .unwrap()
    );
    let native = SessionRef {
        runtime: "dummy".into(),
        state_realm: "test-realm".into(),
        native_id: "native-1".into(),
    };
    store
        .bind_native_session(&ns, "session-1", &native)
        .unwrap();
    assert!(
        store
            .settle_session(
                &ns,
                "run-1",
                CleanupState::Settled,
                SessionDisposition::Reusable
            )
            .unwrap()
    );
    assert_eq!(
        store.get_run(&ns, "run-1").unwrap().unwrap().state,
        "succeeded"
    );
}

#[test]
fn unbound_reservation_blocks_possible_aliases_across_namespaces_after_restart() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("storage.sqlite");
    let a = namespace("client-a");
    let b = namespace("client-b");
    let mut store = SqliteStore::open(&path).unwrap();
    store.create_namespace(&a).unwrap();
    store.create_namespace(&b).unwrap();
    let mut existing = session(Some("other-native"));
    existing.session_id = "existing".into();
    store.create_session(&b, &existing).unwrap();
    let mut unbound = session(None);
    unbound.session_id = "unbound-b".into();
    store.create_session(&b, &unbound).unwrap();
    store.create_session(&a, &session(None)).unwrap();
    store
        .admit_run(&a, &admission("run-1", "key-1", Some("session-1")))
        .unwrap();
    let mut finish = completion("run-1", true);
    finish.cleanup = CleanupState::Uncertain;
    finish.disposition = SessionDisposition::Uncertain;
    store.finish_run(&a, &finish).unwrap();
    drop(store);

    let mut store = SqliteStore::open(&path).unwrap();
    assert!(matches!(
        store.admit_run(&b, &admission("run-2", "key-2", Some("existing"))),
        Err(StorageError::SessionBusy)
    ));
    let mut alias = session(Some("possible-alias"));
    alias.session_id = "new-alias".into();
    assert!(matches!(
        store.create_session(&b, &alias),
        Err(StorageError::SessionBusy)
    ));
    let competing_native = SessionRef {
        runtime: "dummy".into(),
        state_realm: "test-realm".into(),
        native_id: "possible-alias".into(),
    };
    assert!(matches!(
        store.bind_native_session(&b, "unbound-b", &competing_native),
        Err(StorageError::SessionBusy)
    ));
    let native = SessionRef {
        runtime: "dummy".into(),
        state_realm: "test-realm".into(),
        native_id: "actual-native".into(),
    };
    store.bind_native_session(&a, "session-1", &native).unwrap();
    assert!(matches!(
        store
            .admit_run(&b, &admission("run-2", "key-2", Some("existing")))
            .unwrap(),
        AdmissionOutcome::Accepted(_)
    ));
    assert_eq!(
        store
            .get_session(&a, "session-1")
            .unwrap()
            .unwrap()
            .reserved_run_id
            .as_deref(),
        Some("run-1")
    );
}

#[test]
fn unobserved_session_completion_is_recorded_without_releasing_reservation() {
    let ns = namespace("client-a");
    let mut store = SqliteStore::open_in_memory().unwrap();
    store.create_namespace(&ns).unwrap();
    store
        .create_session(&ns, &session(Some("native-1")))
        .unwrap();
    store
        .admit_run(&ns, &admission("run-1", "key-1", Some("session-1")))
        .unwrap();
    let mut finish = completion("run-1", true);
    finish.state = TerminalState::Cancelled;
    finish.disposition = SessionDisposition::NotApplicable;
    store.finish_run(&ns, &finish).unwrap();
    let attempt = store
        .get_attempt(&ns, "run-1", "attempt-run-1")
        .unwrap()
        .unwrap();
    assert_eq!(attempt.disposition.as_deref(), Some("not_applicable"));
    let saved = store.get_session(&ns, "session-1").unwrap().unwrap();
    assert_eq!(saved.reserved_run_id.as_deref(), Some("run-1"));
    assert_eq!(saved.disposition.as_deref(), Some("uncertain"));
    assert_eq!(store.get_events(&ns, "run-1", None).unwrap().len(), 2);
}

#[test]
fn direct_sql_cannot_replace_or_delete_an_unreferenced_skill_version() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("storage.sqlite");
    let ns = namespace("client-a");
    let mut store = SqliteStore::open(&path).unwrap();
    store.create_namespace(&ns).unwrap();
    store.put_skill_version(&ns, &skill(), "original").unwrap();
    let connection = Connection::open(&path).unwrap();
    connection
        .pragma_update(None, "foreign_keys", true)
        .unwrap();
    assert!(connection.execute("INSERT OR REPLACE INTO skill_versions (owner_id, client_id, skill_id, version, markdown) VALUES ('owner', 'client-a', 'report', '1', 'changed')", []).is_err());
    assert!(connection.execute("INSERT OR REPLACE INTO skill_versions (owner_id, client_id, skill_id, version, markdown) VALUES ('owner', 'client-a', 'report', '1', CAST('original' AS BLOB))", []).is_err());
    assert!(
        connection
            .execute("DELETE FROM skill_versions", [])
            .is_err()
    );
    assert_eq!(
        store
            .get_skill_version(&ns, &skill())
            .unwrap()
            .unwrap()
            .markdown,
        "original"
    );
}

#[test]
fn direct_sql_cannot_replace_a_referenced_skill_with_deferred_foreign_keys() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("storage.sqlite");
    let ns = namespace("client-a");
    let mut store = SqliteStore::open(&path).unwrap();
    store.create_namespace(&ns).unwrap();
    store.put_skill_version(&ns, &skill(), "original").unwrap();
    let mut start = admission("run-1", "key-1", None);
    start.skills.push(skill());
    store.admit_run(&ns, &start).unwrap();
    let connection = Connection::open(&path).unwrap();
    connection
        .pragma_update(None, "foreign_keys", true)
        .unwrap();
    connection
        .execute_batch("BEGIN IMMEDIATE; PRAGMA defer_foreign_keys = ON;")
        .unwrap();
    assert!(connection.execute("INSERT OR REPLACE INTO skill_versions (owner_id, client_id, skill_id, version, markdown) VALUES ('owner', 'client-a', 'report', '1', 'changed')", []).is_err());
    connection.execute_batch("ROLLBACK").unwrap();
    assert_eq!(
        store
            .get_skill_version(&ns, &skill())
            .unwrap()
            .unwrap()
            .markdown,
        "original"
    );
}
