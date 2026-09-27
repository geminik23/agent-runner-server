use std::collections::HashSet;

use crate::{
    core::{CleanupState, ExecutionMetadata, SessionDisposition},
    delivery::{DeliveryConfig, Destination, DestinationKind},
    storage::{
        AdmissionOutcome, HookRef, Namespace, SqliteStore, StorageError, TerminalState,
        dispatch::Observation,
        service::{Candidate, ServiceAdmission},
        skills::SkillBounds,
    },
};
use serde_json::json;

fn fixture(deduplication: bool, max_pending: usize) -> (SqliteStore, ServiceAdmission, String) {
    fixture_in(
        SqliteStore::open_in_memory().unwrap(),
        deduplication,
        max_pending,
    )
}

fn fixture_in(
    mut store: SqliteStore,
    deduplication: bool,
    max_pending: usize,
) -> (SqliteStore, ServiceAdmission, String) {
    let namespace = Namespace {
        owner_id: "owner".into(),
        client_id: "client".into(),
    };
    let destination = Destination {
        id: "target".into(),
        revision: "v1".into(),
        kind: DestinationKind::WebhookTest,
        endpoint: Some("http://127.0.0.1:12345/hook".into()),
        notion_parent: None,
        deduplication,
        enabled: true,
    };
    let config = DeliveryConfig {
        max_pending,
        max_payload_bytes: 32768,
        max_workers: 1,
        max_attempts: 3,
        delivery_ttl_ms: 10000,
        retention_ms: 100,
        lease_ms: 100,
        timeout_ms: 50,
        retry_ms: 10,
        poll_ms: 5,
        destinations: vec![destination.clone()],
    };
    let hook = HookRef {
        hook_id: "hook".into(),
        version: "v1".into(),
    };
    store
        .register_hook(&namespace, &hook, &destination, &["succeeded".into()], 128)
        .unwrap();
    let input = ServiceAdmission {
        namespace: namespace.clone(),
        idempotency_key: "key".into(),
        normalized_request: b"request".to_vec(),
        manifest: b"manifest".to_vec(),
        metadata: ExecutionMetadata {
            runtime: "dummy".into(),
            synthetic: true,
            scenario: Some("success".into()),
        },
        credential_id: "credential".into(),
        credential_generation: "generation".into(),
        profile_id: "profile".into(),
        profile_revision: "v1".into(),
        workspace_id: "workspace".into(),
        state_realm: "dummy".into(),
        resume_session_id: None,
        deadline_ms: 10000,
        max_outstanding: 8,
        skills: None,
        may_use_skills: false,
        skill_bounds: SkillBounds {
            markdown: 1024,
            metadata: 4096,
            identifier: 128,
        },
        hooks: Some(vec![hook]),
        may_use_hooks: true,
        destination_grants: HashSet::from(["target".into()]),
        delivery: Some(config),
    };
    let AdmissionOutcome::Accepted(run) = store.service_admit(&input).unwrap() else {
        panic!()
    };
    let job = store.service_claim(1000).unwrap().unwrap();
    store.service_launch(&job, 1001).unwrap();
    store
        .service_finish(
            &job,
            &Candidate {
                state: TerminalState::Succeeded,
                details: json!({}),
                cleanup: CleanupState::Settled,
                disposition: SessionDisposition::Uncertain,
            },
            1002,
            1024,
        )
        .unwrap();
    (store, input, run.run_id)
}

#[test]
fn dispatch_rechecks_wall_clock_after_database_lock_wait() {
    use crate::storage::service::ServiceClock;
    use std::{
        sync::{Arc, Barrier},
        thread,
        time::Duration,
    };
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("delivery-lock.sqlite");
    let (mut store, _, _) = fixture_in(SqliteStore::open(&path).unwrap(), false, 4);
    store
        .connection
        .execute(
            "UPDATE completion_delivery_jobs SET next_attempt_ms=0,expires_ms=9000000000000",
            [],
        )
        .unwrap();
    let job = store
        .completion_claim(ServiceClock::Wall, 300)
        .unwrap()
        .unwrap();
    let lock = rusqlite::Connection::open(&path).unwrap();
    lock.execute_batch("BEGIN IMMEDIATE").unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let signal = Arc::clone(&barrier);
    let worker = thread::spawn(move || {
        signal.wait();
        store
            .completion_begin_dispatch(&job, ServiceClock::Wall)
            .unwrap()
    });
    barrier.wait();
    thread::sleep(Duration::from_millis(400));
    lock.execute_batch("COMMIT").unwrap();
    assert!(!worker.join().unwrap());
}

#[test]
fn expired_undispatched_lease_is_reclaimable_not_skipped() {
    let (mut store, input, run) = fixture(false, 4);
    let first = store.completion_claim(1100, 100).unwrap().unwrap();
    assert!(!store.completion_begin_dispatch(&first, 1200).unwrap());
    store
        .completion_skip(&first, 1200, "lease_expired")
        .unwrap();
    let second = store.completion_claim(1201, 100).unwrap().unwrap();
    assert!(second.generation > first.generation);
    assert!(store.completion_begin_dispatch(&second, 1202).unwrap());
    store
        .completion_observe(&second, 1203, 10, Observation::Delivered)
        .unwrap();
    assert_eq!(
        store.get_deliveries(&input.namespace, &run).unwrap()[0].state,
        "succeeded"
    );
}

#[test]
fn uncertain_delivery_preserves_payload_until_operator_reconciliation() {
    let (mut store, input, run) = fixture(false, 4);
    let first = store.completion_claim(1100, 100).unwrap().unwrap();
    assert!(store.completion_begin_dispatch(&first, 1101).unwrap());
    assert!(store.completion_claim(1200, 100).unwrap().is_none());
    store
        .completion_observe(&first, 1201, 10, Observation::Delivered)
        .unwrap();
    let items = store.get_deliveries(&input.namespace, &run).unwrap();
    assert_eq!(items[0].state, "uncertain");
    let payload = items[0].payload.clone();
    store.completion_maintenance(30000).unwrap();
    assert_eq!(
        store.get_deliveries(&input.namespace, &run).unwrap()[0].payload,
        payload
    );
    let reconciliation = crate::storage::inbox::DeliveryReconciliation {
        id: &first.id,
        actor: "operator",
        generation: "v1",
        outcome: "confirmed_delivered",
        grants: &input.destination_grants,
    };
    store
        .reconcile_delivery(&input.namespace, &reconciliation, 30001)
        .unwrap();
    store.completion_maintenance(30102).unwrap();
    assert!(
        store.get_deliveries(&input.namespace, &run).unwrap()[0]
            .payload
            .is_empty()
    );
}

#[test]
fn stale_attempt_observation_cannot_overwrite_a_reclaimed_delivery() {
    let (mut store, input, run) = fixture(true, 4);
    let first = store.completion_claim(1100, 100).unwrap().unwrap();
    assert!(store.completion_begin_dispatch(&first, 1101).unwrap());
    let second = store.completion_claim(1200, 100).unwrap().unwrap();
    assert!(second.generation > first.generation);
    assert!(store.completion_begin_dispatch(&second, 1201).unwrap());
    store
        .completion_observe(&second, 1202, 10, Observation::Delivered)
        .unwrap();
    store
        .completion_observe(&first, 1203, 10, Observation::Ambiguous)
        .unwrap();
    assert_eq!(
        store.get_deliveries(&input.namespace, &run).unwrap()[0].state,
        "succeeded"
    );
}

#[test]
fn revoking_after_ambiguous_attempt_keeps_uncertainty_and_payload() {
    for disable in [false, true] {
        let (mut store, input, run) = fixture(true, 4);
        let first = store.completion_claim(1100, 100).unwrap().unwrap();
        assert!(store.completion_begin_dispatch(&first, 1101).unwrap());
        store
            .completion_observe(&first, 1102, 10, Observation::Ambiguous)
            .unwrap();
        if disable {
            store.completion_disable(1110).unwrap();
        } else {
            let second = store.completion_claim(1112, 100).unwrap().unwrap();
            store
                .completion_skip(&second, 1113, "authorization_revoked")
                .unwrap();
        }
        let delivery = store.get_deliveries(&input.namespace, &run).unwrap();
        assert_eq!(delivery[0].state, "uncertain");
        let payload = delivery[0].payload.clone();
        store.completion_maintenance(50000).unwrap();
        assert_eq!(
            store.get_deliveries(&input.namespace, &run).unwrap()[0].payload,
            payload
        );
    }
}

#[test]
fn hook_version_cannot_be_replaced_by_direct_sql() {
    let (store, input, _) = fixture(false, 4);
    let result=store.connection.execute("INSERT OR REPLACE INTO completion_hook_versions (owner_id,client_id,hook_id,version,destination_id,destination_revision,destination_snapshot,events,projection) VALUES ('owner','client','hook','v1','target','v1',X'7B7D','[\"failed\"]','metadata')",[]);
    assert!(result.is_err());
    let record = store
        .hook(
            &input.namespace,
            &HookRef {
                hook_id: "hook".into(),
                version: "v1".into(),
            },
            128,
        )
        .unwrap()
        .unwrap();
    assert_eq!(record.events, vec!["succeeded"]);
}

#[test]
fn changed_destination_cannot_reinterpret_a_pinned_hook_revision() {
    let (mut store, input, run) = fixture(false, 4);
    let saved = store.get_deliveries(&input.namespace, &run).unwrap()[0]
        .payload
        .clone();
    let mut changed = input;
    changed.idempotency_key = "changed".into();
    changed.normalized_request = b"changed".to_vec();
    changed.delivery.as_mut().unwrap().destinations[0].endpoint =
        Some("http://127.0.0.1:54321/other".into());
    assert!(matches!(
        store.service_admit(&changed),
        Err(StorageError::HookUnavailable)
    ));
    assert_eq!(
        store.get_deliveries(&changed.namespace, &run).unwrap()[0].payload,
        saved
    );
}

#[test]
fn revocation_after_admission_prevents_a_new_dispatch() {
    let (mut store, input, run) = fixture(false, 4);
    let job = store.completion_claim(1100, 100).unwrap().unwrap();
    store
        .set_hook_enabled(
            &input.namespace,
            &HookRef {
                hook_id: "hook".into(),
                version: "v1".into(),
            },
            false,
        )
        .unwrap();
    assert!(!store.completion_begin_dispatch(&job, 1101).unwrap());
    store.completion_skip(&job, 1102, "hook_revoked").unwrap();
    assert_eq!(
        store.get_deliveries(&input.namespace, &run).unwrap()[0].state,
        "skipped"
    );
}

#[test]
fn reserved_delivery_quota_rejects_new_admission_but_not_replay() {
    let (mut store, input, run) = fixture(false, 1);
    assert!(matches!(
        store.service_admit(&input).unwrap(),
        AdmissionOutcome::Existing(_)
    ));
    let mut next = input;
    next.idempotency_key = "other".into();
    next.normalized_request = b"other".to_vec();
    assert!(matches!(
        store.service_admit(&next),
        Err(StorageError::DeliveryQuota)
    ));
    assert_eq!(
        store.get_run(&next.namespace, &run).unwrap().unwrap().state,
        "succeeded"
    );
}
