use agent_runner_server::{
    access::{
        AccessError, AccessLimits, AuthorizedStore, ClientAccessControl, ClientRegistration,
        CredentialSecret, ExecutionProfile, Scope,
    },
    core::{CleanupState, ExecutionMetadata, SessionDisposition},
    storage::{
        Admission, Completion, DeliveryIntent, HookBinding, Namespace, NewInboxItem, NewSession,
        SkillRef, SkillWrite, SqliteStore, TerminalState,
    },
};
use serde_json::{Value, json};

fn namespace(client: &str) -> Namespace {
    Namespace {
        owner_id: "owner".into(),
        client_id: client.into(),
    }
}

fn registration(
    id: &str,
    secret: u8,
    client: &str,
    scopes: Vec<Scope>,
    profiles: &[&str],
) -> ClientRegistration {
    ClientRegistration::new(
        id,
        CredentialSecret::new([secret; 32]),
        namespace(client),
        scopes,
        profiles.iter().map(|value| (*value).to_owned()),
    )
}

fn all_scopes() -> Vec<Scope> {
    vec![
        Scope::RunStart,
        Scope::RunRead,
        Scope::RunCancel,
        Scope::SessionRead,
        Scope::SkillRead,
        Scope::SkillWrite,
        Scope::HookRead,
        Scope::HookWrite,
        Scope::InboxRead,
        Scope::InboxAck,
    ]
}

fn profiles() -> Vec<ExecutionProfile> {
    vec![
        ExecutionProfile::dummy("standard", vec!["workspace_a".into()], 16),
        ExecutionProfile::dummy("other", vec!["workspace_b".into()], 16),
    ]
}

fn limits() -> AccessLimits {
    AccessLimits {
        max_request_bytes: 512,
        max_skill_bytes: 32,
        max_identifier_bytes: 128,
    }
}

fn access() -> ClientAccessControl {
    ClientAccessControl::new(
        vec![
            registration("cred-a", 1, "client-a", all_scopes(), &["standard"]),
            registration("cred-b", 2, "client-b", all_scopes(), &["other"]),
            registration(
                "read-only",
                3,
                "client-a",
                vec![Scope::RunRead, Scope::SkillRead],
                &[],
            ),
        ],
        profiles(),
        limits(),
    )
    .unwrap()
}

#[test]
fn native_session_ids_use_request_limit_not_server_identifier_limit() {
    for identifier_limit in [40, 64, 85, 86] {
        let mut bounds = limits();
        bounds.max_identifier_bytes = identifier_limit;
        let access = ClientAccessControl::new(
            vec![registration(
                "cred-a",
                1,
                "client-a",
                all_scopes(),
                &["standard"],
            )],
            profiles(),
            bounds,
        )
        .unwrap();
        let client = access.authenticate("cred-a", &[1; 32]).unwrap();
        let mut request = start_body("standard", "workspace_a", "work");
        request["session"] = json!({"mode":"resume_native","native_id":"n".repeat(86)});
        assert!(access.authorize_start(&client, &encode(&request)).is_ok());
        request["session"] = json!({"mode":"resume_native","native_id":""});
        assert!(matches!(
            access.authorize_start(&client, &encode(&request)),
            Err(AccessError::InvalidRequest)
        ));
    }
}

fn start_body(profile: &str, workspace: &str, text: &str) -> Value {
    json!({"profile_id": profile, "workspace_id": workspace, "input": {"request": text}, "idempotency_key": "request-1"})
}

fn encode(value: &Value) -> Vec<u8> {
    serde_json::to_vec(value).unwrap()
}

#[test]
fn runtime_is_selected_by_the_trusted_profile_not_the_request() {
    let access = ClientAccessControl::new(
        [registration(
            "cred-a",
            1,
            "client-a",
            all_scopes(),
            &["standard"],
        )],
        [ExecutionProfile::for_runtime(
            "standard",
            "codex",
            ["workspace_a".into()],
            16,
        )],
        limits(),
    )
    .unwrap();
    let client = access.authenticate("cred-a", &[1; 32]).unwrap();
    let mut body = start_body("standard", "workspace_a", "work");
    let approved = access.authorize_start(&client, &encode(&body)).unwrap();
    assert_eq!(approved.runtime(), "codex");
    assert_eq!(approved.workspace_id(), "workspace_a");
    body["runtime"] = json!("claude");
    assert!(matches!(
        access.authorize_start(&client, &encode(&body)),
        Err(AccessError::InvalidRequest)
    ));
}

fn skill(id: &str) -> SkillRef {
    SkillRef {
        skill_id: id.into(),
        version: "1".into(),
    }
}

fn seed_run(store: &mut SqliteStore, client: &str, suffix: &str) {
    let ns = namespace(client);
    store.create_namespace(&ns).unwrap();
    let reference = skill(&format!("skill-{suffix}"));
    store
        .put_skill_version(&ns, &reference, &format!("skill content {suffix}"))
        .unwrap();
    let session_id = format!("session-{suffix}");
    store
        .create_session(
            &ns,
            &NewSession {
                session_id: session_id.clone(),
                runtime: "dummy".into(),
                state_realm: "realm".into(),
                native_id: Some(format!("native-{suffix}")),
            },
        )
        .unwrap();
    let run_id = format!("run-{suffix}");
    let attempt_id = format!("attempt-{suffix}");
    let binding = HookBinding {
        binding_id: "binding".into(),
        hook_revision: "hook-v1".into(),
        destination_revision: "destination-v1".into(),
        projection: b"projection".to_vec(),
    };
    store
        .admit_run(
            &ns,
            &Admission {
                run_id: run_id.clone(),
                attempt_id: attempt_id.clone(),
                session_id: Some(session_id),
                idempotency_key: format!("key-{suffix}"),
                normalized_request: format!("request-{suffix}").into_bytes(),
                manifest: format!("manifest-{suffix}").into_bytes(),
                metadata: ExecutionMetadata {
                    runtime: "dummy".into(),
                    synthetic: true,
                    scenario: Some("success".into()),
                },
                queued_event_id: format!("queued-{suffix}"),
                queued_event_payload: b"queued".to_vec(),
                skills: vec![reference],
                hooks: vec![binding],
            },
        )
        .unwrap();
    store
        .finish_run(
            &ns,
            &Completion {
                run_id,
                attempt_id,
                state: TerminalState::Succeeded,
                result_revision: "v1".into(),
                result_payload: format!("result-{suffix}").into_bytes(),
                terminal_event_id: format!("finished-{suffix}"),
                terminal_event_payload: b"finished".to_vec(),
                cleanup: CleanupState::Settled,
                disposition: SessionDisposition::Reusable,
                deliveries: vec![DeliveryIntent {
                    delivery_id: "delivery".into(),
                    binding_id: "binding".into(),
                    payload: b"delivery".to_vec(),
                }],
                inbox: Some(NewInboxItem {
                    inbox_id: "inbox".into(),
                    payload: b"inbox".to_vec(),
                }),
            },
        )
        .unwrap();
}

#[test]
fn authentication_rejects_unknown_wrong_and_malformed_credentials_uniformly() {
    let access = access();
    assert!(access.authenticate("cred-a", &[1; 32]).is_ok());
    for (id, proof) in [
        ("cred-a", vec![2; 32]),
        ("cred-a", vec![1; 31]),
        ("missing", vec![1; 32]),
        ("../bad", vec![1; 32]),
    ] {
        let error = match access.authenticate(id, &proof) {
            Ok(_) => panic!("unexpected authentication success"),
            Err(error) => error,
        };
        assert!(matches!(error, AccessError::Unauthenticated));
        assert_eq!(error.to_string(), "authentication failed");
        assert!(!format!("{error:?}").contains(id));
    }
}

#[test]
fn revoked_credentials_invalidate_existing_contexts() {
    let access = access();
    let client = access.authenticate("cred-a", &[1; 32]).unwrap();
    assert!(access.check_scope(&client, Scope::RunRead).is_ok());
    assert!(access.revoke("cred-a").unwrap());
    assert!(!access.revoke("cred-a").unwrap());
    assert!(matches!(
        access.check_scope(&client, Scope::RunRead),
        Err(AccessError::Unauthenticated)
    ));
    assert!(matches!(
        access.authenticate("cred-a", &[1; 32]),
        Err(AccessError::Unauthenticated)
    ));
    assert!(matches!(
        access.authorize_start(
            &client,
            &encode(&start_body("standard", "workspace_a", "test"))
        ),
        Err(AccessError::Unauthenticated)
    ));
}

#[test]
fn contexts_cannot_cross_authority_instances() {
    let first = access();
    let second = access();
    let client = first.authenticate("cred-a", &[1; 32]).unwrap();
    assert!(matches!(
        second.check_scope(&client, Scope::RunRead),
        Err(AccessError::Unauthenticated)
    ));
    let mut storage = SqliteStore::open_in_memory().unwrap();
    let view = AuthorizedStore::new(&second, &client, &mut storage);
    assert!(matches!(
        view.get_run("run-a"),
        Err(AccessError::Unauthenticated)
    ));
}

#[test]
fn configuration_rejects_duplicate_or_invalid_grants() {
    assert!(matches!(
        ClientAccessControl::new(
            vec![
                registration("same", 1, "a", vec![], &[]),
                registration("same", 2, "b", vec![], &[])
            ],
            profiles(),
            limits()
        ),
        Err(AccessError::InvalidConfiguration)
    ));
    assert!(matches!(
        ClientAccessControl::new(
            vec![registration("id", 1, "a", vec![], &["missing"])],
            profiles(),
            limits()
        ),
        Err(AccessError::InvalidConfiguration)
    ));
    assert!(matches!(
        ClientAccessControl::new(
            vec![],
            vec![ExecutionProfile::dummy(
                "unsafe",
                vec!["../path".into()],
                16
            )],
            limits()
        ),
        Err(AccessError::InvalidConfiguration)
    ));
    assert!(matches!(
        ClientAccessControl::new(
            vec![],
            vec![
                ExecutionProfile::dummy("same", vec!["a".into()], 16),
                ExecutionProfile::dummy("same", vec!["b".into()], 16)
            ],
            limits()
        ),
        Err(AccessError::InvalidConfiguration)
    ));
    assert!(matches!(
        ClientAccessControl::new(
            vec![],
            vec![],
            AccessLimits {
                max_request_bytes: 0,
                max_skill_bytes: 1,
                max_identifier_bytes: 1
            }
        ),
        Err(AccessError::InvalidConfiguration)
    ));
    assert!(matches!(
        ClientAccessControl::new(
            vec![],
            vec![ExecutionProfile::dummy(
                "profile",
                vec!["workspace".into()],
                1
            )],
            AccessLimits {
                max_request_bytes: 128,
                max_skill_bytes: 8,
                max_identifier_bytes: 3
            }
        ),
        Err(AccessError::InvalidConfiguration)
    ));
}

#[test]
fn approved_dummy_request_preserves_literal_input() {
    let access = access();
    let client = access.authenticate("cred-a", &[1; 32]).unwrap();
    let authorized = access
        .authorize_start(
            &client,
            &encode(&start_body("standard", "workspace_a", "literal {{x}}")),
        )
        .unwrap();
    assert_eq!(authorized.profile_id(), "standard");
    assert_eq!(authorized.workspace_id(), "workspace_a");
    assert_eq!(authorized.runtime(), "dummy");
    assert_eq!(authorized.input(), "literal {{x}}");
    assert_eq!(authorized.idempotency_key(), "request-1");
}

#[test]
fn start_request_rejects_identity_spoofing_and_unsafe_fields() {
    let access = access();
    let client = access.authenticate("cred-a", &[1; 32]).unwrap();
    for field in [
        "owner_id",
        "client_id",
        "credential",
        "executable",
        "command",
        "args",
        "env",
        "secret",
        "runtime",
        "workspace_path",
    ] {
        let mut body = start_body("standard", "workspace_a", "test");
        body.as_object_mut()
            .unwrap()
            .insert(field.into(), json!("not-allowed"));
        assert!(
            matches!(
                access.authorize_start(&client, &encode(&body)),
                Err(AccessError::InvalidRequest)
            ),
            "field {field}"
        );
    }
    let mut body = start_body("standard", "workspace_a", "test");
    body["input"]["secret"] = json!("not-allowed");
    assert!(matches!(
        access.authorize_start(&client, &encode(&body)),
        Err(AccessError::InvalidRequest)
    ));
    let duplicate = br#"{"profile_id":"standard","profile_id":"other","workspace_id":"workspace_a","input":{"request":"test"},"idempotency_key":"k"}"#;
    assert!(matches!(
        access.authorize_start(&client, duplicate),
        Err(AccessError::InvalidRequest)
    ));
}

#[test]
fn unknown_profiles_workspaces_and_missing_scopes_fail_closed() {
    let access = access();
    let a = access.authenticate("cred-a", &[1; 32]).unwrap();
    let read_only = access.authenticate("read-only", &[3; 32]).unwrap();
    assert!(matches!(
        access.authorize_start(&a, &encode(&start_body("other", "workspace_b", "x"))),
        Err(AccessError::Forbidden)
    ));
    assert!(matches!(
        access.authorize_start(&a, &encode(&start_body("missing", "workspace_a", "x"))),
        Err(AccessError::Forbidden)
    ));
    assert!(matches!(
        access.authorize_start(&a, &encode(&start_body("standard", "workspace_b", "x"))),
        Err(AccessError::Forbidden)
    ));
    assert!(matches!(
        access.authorize_start(&a, &encode(&start_body("standard", "../workspace", "x"))),
        Err(AccessError::InvalidRequest)
    ));
    assert!(matches!(
        access.authorize_start(
            &read_only,
            &encode(&start_body("standard", "workspace_a", "x"))
        ),
        Err(AccessError::Forbidden)
    ));
    let mut empty_key = start_body("standard", "workspace_a", "x");
    empty_key["idempotency_key"] = json!("");
    assert!(matches!(
        access.authorize_start(&a, &encode(&empty_key)),
        Err(AccessError::InvalidRequest)
    ));
    let mut large_key = start_body("standard", "workspace_a", "x");
    large_key["idempotency_key"] = json!("x".repeat(129));
    assert!(matches!(
        access.authorize_start(&a, &encode(&large_key)),
        Err(AccessError::InvalidRequest)
    ));
}

#[test]
fn request_and_input_limits_are_checked_in_bytes() {
    let access = access();
    let client = access.authenticate("cred-a", &[1; 32]).unwrap();
    let oversized_input = start_body("standard", "workspace_a", "한한한한한한");
    assert!(matches!(
        access.authorize_start(&client, &encode(&oversized_input)),
        Err(AccessError::InvalidRequest)
    ));
    assert!(matches!(
        access.authorize_start(&client, &vec![b' '; 513]),
        Err(AccessError::InvalidRequest)
    ));
    assert!(matches!(
        access.authorize_start(&client, b"{"),
        Err(AccessError::InvalidRequest)
    ));
}

#[test]
fn owner_and_client_ids_both_determine_storage_namespace() {
    let first = Namespace {
        owner_id: "owner-a".into(),
        client_id: "same-client".into(),
    };
    let second = Namespace {
        owner_id: "owner-b".into(),
        client_id: "same-client".into(),
    };
    let access = ClientAccessControl::new(
        [
            ClientRegistration::new(
                "first",
                CredentialSecret::new([10; 32]),
                first.clone(),
                [Scope::SkillRead],
                Vec::<String>::new(),
            ),
            ClientRegistration::new(
                "second",
                CredentialSecret::new([11; 32]),
                second.clone(),
                [Scope::SkillRead],
                Vec::<String>::new(),
            ),
        ],
        [],
        limits(),
    )
    .unwrap();
    let a = access.authenticate("first", &[10; 32]).unwrap();
    let b = access.authenticate("second", &[11; 32]).unwrap();
    let mut storage = SqliteStore::open_in_memory().unwrap();
    storage.create_namespace(&first).unwrap();
    storage.create_namespace(&second).unwrap();
    storage
        .put_skill_version(&first, &skill("shared"), "first")
        .unwrap();
    storage
        .put_skill_version(&second, &skill("shared"), "second")
        .unwrap();
    {
        let view = AuthorizedStore::new(&access, &a, &mut storage);
        assert_eq!(
            view.get_skill_version(&skill("shared"))
                .unwrap()
                .unwrap()
                .markdown,
            "first"
        );
    }
    let view = AuthorizedStore::new(&access, &b, &mut storage);
    assert_eq!(
        view.get_skill_version(&skill("shared"))
            .unwrap()
            .unwrap()
            .markdown,
        "second"
    );
}

#[test]
fn authenticated_store_scopes_all_reads_to_the_registered_namespace() {
    let access = access();
    let client = access.authenticate("cred-a", &[1; 32]).unwrap();
    let mut storage = SqliteStore::open_in_memory().unwrap();
    seed_run(&mut storage, "client-a", "a");
    seed_run(&mut storage, "client-b", "b");
    let view = AuthorizedStore::new(&access, &client, &mut storage);
    assert_eq!(view.get_run("run-a").unwrap().unwrap().run_id, "run-a");
    assert!(view.get_run("run-b").unwrap().is_none());
    assert!(view.get_attempt("run-b", "attempt-b").unwrap().is_none());
    assert!(view.get_events("run-b", None).unwrap().is_empty());
    assert!(view.get_result("run-b").unwrap().is_none());
    assert!(view.get_session("session-b").unwrap().is_none());
    assert!(view.get_skill_version(&skill("skill-b")).unwrap().is_none());
    assert!(view.get_run_skills("run-b").unwrap().is_empty());
    assert!(view.get_hook_bindings("run-b").unwrap().is_empty());
    assert!(view.get_deliveries("run-b").unwrap().is_empty());
    assert!(view.get_inbox_items("run-b").unwrap().is_empty());
    assert_eq!(
        view.get_attempt("run-a", "attempt-a")
            .unwrap()
            .unwrap()
            .state,
        "succeeded"
    );
    assert_eq!(view.get_events("run-a", None).unwrap().len(), 2);
    assert_eq!(
        view.get_result("run-a").unwrap().unwrap().payload,
        b"result-a"
    );
    assert!(view.get_session("session-a").unwrap().is_some());
    assert_eq!(
        view.get_skill_version(&skill("skill-a"))
            .unwrap()
            .unwrap()
            .markdown,
        "skill content a"
    );
    assert_eq!(view.get_run_skills("run-a").unwrap().len(), 1);
    assert_eq!(view.get_hook_bindings("run-a").unwrap().len(), 1);
    assert_eq!(view.get_deliveries("run-a").unwrap().len(), 1);
    assert_eq!(view.get_inbox_items("run-a").unwrap().len(), 1);
}

#[test]
fn insufficient_scope_blocks_operations_and_does_not_write() {
    let access = access();
    let client = access.authenticate("read-only", &[3; 32]).unwrap();
    let mut storage = SqliteStore::open_in_memory().unwrap();
    storage.create_namespace(&namespace("client-a")).unwrap();
    {
        let mut view = AuthorizedStore::new(&access, &client, &mut storage);
        assert!(matches!(
            view.put_skill_version(&skill("new"), "content"),
            Err(AccessError::Forbidden)
        ));
        assert!(matches!(
            view.get_session("missing"),
            Err(AccessError::Forbidden)
        ));
        assert!(matches!(
            view.get_hook_bindings("missing"),
            Err(AccessError::Forbidden)
        ));
        assert!(matches!(
            view.get_deliveries("missing"),
            Err(AccessError::Forbidden)
        ));
        assert!(matches!(
            view.get_inbox_items("missing"),
            Err(AccessError::Forbidden)
        ));
    }
    assert!(
        storage
            .get_skill_version(&namespace("client-a"), &skill("new"))
            .unwrap()
            .is_none()
    );
    for scope in [
        Scope::RunStart,
        Scope::RunCancel,
        Scope::SkillWrite,
        Scope::HookWrite,
        Scope::InboxAck,
    ] {
        assert!(matches!(
            access.check_scope(&client, scope),
            Err(AccessError::Forbidden)
        ));
    }
}

#[test]
fn combined_scopes_are_required_for_sensitive_cross_resource_reads() {
    let access = ClientAccessControl::new(
        vec![registration(
            "run-reader",
            4,
            "client-a",
            vec![Scope::RunRead],
            &[],
        )],
        profiles(),
        limits(),
    )
    .unwrap();
    let client = access.authenticate("run-reader", &[4; 32]).unwrap();
    let mut storage = SqliteStore::open_in_memory().unwrap();
    seed_run(&mut storage, "client-a", "a");
    let view = AuthorizedStore::new(&access, &client, &mut storage);
    assert!(view.get_run("run-a").unwrap().is_some());
    assert!(matches!(
        view.get_run_skills("run-a"),
        Err(AccessError::Forbidden)
    ));
    assert!(matches!(
        view.get_hook_bindings("run-a"),
        Err(AccessError::Forbidden)
    ));
    assert!(matches!(
        view.get_deliveries("run-a"),
        Err(AccessError::Forbidden)
    ));
}

#[test]
fn run_read_returns_no_internal_manifest_or_request() {
    let access = ClientAccessControl::new(
        vec![registration(
            "run-reader",
            4,
            "client-a",
            vec![Scope::RunRead],
            &[],
        )],
        profiles(),
        limits(),
    )
    .unwrap();
    let client = access.authenticate("run-reader", &[4; 32]).unwrap();
    let mut storage = SqliteStore::open_in_memory().unwrap();
    seed_run(&mut storage, "client-a", "a");
    let view = AuthorizedStore::new(&access, &client, &mut storage);
    let run = view.get_run("run-a").unwrap().unwrap();
    let displayed = format!("{run:?}");
    assert!(!displayed.contains("manifest"));
    assert!(!displayed.contains("normalized_request"));
    assert!(!displayed.contains("session_id"));
}

#[test]
fn event_read_does_not_expose_unclassified_payloads() {
    let access = ClientAccessControl::new(
        vec![registration(
            "run-reader",
            4,
            "client-a",
            vec![Scope::RunRead],
            &[],
        )],
        profiles(),
        limits(),
    )
    .unwrap();
    let client = access.authenticate("run-reader", &[4; 32]).unwrap();
    let mut storage = SqliteStore::open_in_memory().unwrap();
    seed_run(&mut storage, "client-a", "a");
    let view = AuthorizedStore::new(&access, &client, &mut storage);
    let events = view.get_events("run-a", None).unwrap();
    assert_eq!(events.len(), 2);
    assert!(!format!("{events:?}").contains("payload"));
}

#[test]
fn oversized_skill_identifiers_are_rejected_before_storage() {
    let access = access();
    let client = access.authenticate("cred-a", &[1; 32]).unwrap();
    let mut storage = SqliteStore::open_in_memory().unwrap();
    storage.create_namespace(&namespace("client-a")).unwrap();
    let reference = skill(&"x".repeat(513));
    {
        let mut view = AuthorizedStore::new(&access, &client, &mut storage);
        assert!(matches!(
            view.put_skill_version(&reference, "ok"),
            Err(AccessError::InvalidRequest)
        ));
    }
    assert!(
        storage
            .get_skill_version(&namespace("client-a"), &reference)
            .unwrap()
            .is_none()
    );
}

#[test]
fn write_only_scope_cannot_compare_existing_skill_content() {
    let access = ClientAccessControl::new(
        vec![registration(
            "writer",
            5,
            "client-a",
            vec![Scope::SkillWrite],
            &[],
        )],
        profiles(),
        limits(),
    )
    .unwrap();
    let client = access.authenticate("writer", &[5; 32]).unwrap();
    let mut storage = SqliteStore::open_in_memory().unwrap();
    storage.create_namespace(&namespace("client-a")).unwrap();
    storage
        .put_skill_version(&namespace("client-a"), &skill("existing"), "original")
        .unwrap();
    let mut view = AuthorizedStore::new(&access, &client, &mut storage);
    let same = format!(
        "{:?}",
        view.put_skill_version(&skill("existing"), "original")
    );
    let different = format!(
        "{:?}",
        view.put_skill_version(&skill("existing"), "different")
    );
    assert_eq!(same, different);
    assert_eq!(same, "Err(Forbidden)");
    assert!(matches!(
        view.put_skill_version(&skill("new"), "new"),
        Err(AccessError::Forbidden)
    ));
}

#[test]
fn identifier_and_total_skill_input_limits_apply_to_all_fields() {
    let access = ClientAccessControl::new(
        vec![registration(
            "writer",
            5,
            "client-a",
            vec![Scope::SkillRead, Scope::SkillWrite],
            &[],
        )],
        vec![],
        AccessLimits {
            max_request_bytes: 8,
            max_skill_bytes: 8,
            max_identifier_bytes: 8,
        },
    )
    .unwrap();
    let client = access.authenticate("writer", &[5; 32]).unwrap();
    let mut storage = SqliteStore::open_in_memory().unwrap();
    storage.create_namespace(&namespace("client-a")).unwrap();
    let reference = SkillRef {
        skill_id: "abcd".into(),
        version: "1234".into(),
    };
    {
        let mut view = AuthorizedStore::new(&access, &client, &mut storage);
        assert!(matches!(
            view.put_skill_version(&reference, "x"),
            Err(AccessError::InvalidRequest)
        ));
        assert!(matches!(
            view.get_skill_version(&SkillRef {
                skill_id: "x".repeat(9),
                version: "1".into()
            }),
            Err(AccessError::InvalidRequest)
        ));
        assert!(matches!(
            view.put_skill_version(
                &SkillRef {
                    skill_id: "id".into(),
                    version: "x".repeat(9)
                },
                "x"
            ),
            Err(AccessError::InvalidRequest)
        ));
    }
    assert!(
        storage
            .get_skill_version(&namespace("client-a"), &reference)
            .unwrap()
            .is_none()
    );
}

#[test]
fn skill_write_limits_are_applied_before_storage() {
    let access = access();
    let client = access.authenticate("cred-a", &[1; 32]).unwrap();
    let mut storage = SqliteStore::open_in_memory().unwrap();
    storage.create_namespace(&namespace("client-a")).unwrap();
    {
        let mut view = AuthorizedStore::new(&access, &client, &mut storage);
        assert!(matches!(
            view.put_skill_version(&skill("large"), &"한".repeat(11)),
            Err(AccessError::InvalidRequest)
        ));
        assert_eq!(
            view.put_skill_version(&skill("small"), "hello").unwrap(),
            SkillWrite::Inserted
        );
    }
    assert!(
        storage
            .get_skill_version(&namespace("client-a"), &skill("large"))
            .unwrap()
            .is_none()
    );
    assert!(
        storage
            .get_skill_version(&namespace("client-a"), &skill("small"))
            .unwrap()
            .is_some()
    );
}

#[test]
fn revoked_context_cannot_read_or_write_through_an_existing_view() {
    let access = access();
    let client = access.authenticate("cred-a", &[1; 32]).unwrap();
    let mut storage = SqliteStore::open_in_memory().unwrap();
    storage.create_namespace(&namespace("client-a")).unwrap();
    {
        let mut view = AuthorizedStore::new(&access, &client, &mut storage);
        assert!(access.revoke("cred-a").unwrap());
        assert!(matches!(
            view.get_run("missing"),
            Err(AccessError::Unauthenticated)
        ));
        assert!(matches!(
            view.put_skill_version(&skill("new"), "content"),
            Err(AccessError::Unauthenticated)
        ));
    }
    assert!(
        storage
            .get_skill_version(&namespace("client-a"), &skill("new"))
            .unwrap()
            .is_none()
    );
}
