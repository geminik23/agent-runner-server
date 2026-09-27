use std::{path::PathBuf, sync::Arc, time::Duration};

use agent_runner_server::{
    access::{
        AccessLimits, ClientAccessControl, ClientRegistration, CredentialSecret, ExecutionProfile,
        Scope,
    },
    application::{BackendProfile, RetentionConfig, RunLimits, RunService},
    runtimes::dummy::{DummyBackend, DummyScenario},
    server::{Server, ServerLimits},
    storage::Namespace,
};
use reqwest::{Client, Response};
use serde_json::{Value, json};
use tempfile::{TempDir, tempdir};
use tokio::{io::AsyncWriteExt, task::JoinHandle};
use tokio_util::sync::CancellationToken;

const MODERN: &str = "2026-07-28";
fn token(id: &str, byte: u8) -> String {
    format!("{id}.{}", format!("{byte:02x}").repeat(32))
}
fn start(key: &str) -> Value {
    json!({"profile_id":"profile","workspace_id":"workspace","input":{"request":"literal input"},"idempotency_key":key})
}
fn rpc(method: &str, mut params: Value, modern: bool) -> Value {
    if modern {
        params["_meta"] = json!({"io.modelcontextprotocol/protocolVersion":MODERN,"io.modelcontextprotocol/clientCapabilities":{}});
    }
    json!({"jsonrpc":"2.0","id":1,"method":method,"params":params})
}
async fn payload(r: Response) -> Value {
    let text = r.text().await.unwrap();
    if text.starts_with('{') {
        serde_json::from_str(&text).unwrap()
    } else {
        serde_json::from_str(
            text.lines()
                .filter_map(|l| l.strip_prefix("data: "))
                .find(|l| l.starts_with('{'))
                .expect(&text),
        )
        .unwrap()
    }
}
struct Fixture {
    _dir: TempDir,
    path: PathBuf,
    url: String,
    client: Client,
    stop: CancellationToken,
    task: JoinHandle<Result<(), agent_runner_server::server::ServerError>>,
    backend: Arc<DummyBackend>,
    access: Arc<ClientAccessControl>,
    run_limits: RunLimits,
    transport_limits: ServerLimits,
}
impl Fixture {
    async fn new(scenario: DummyScenario) -> Self {
        Self::with_timeout(scenario, Duration::from_secs(3)).await
    }
    async fn with_timeout(scenario: DummyScenario, request_timeout: Duration) -> Self {
        Self::with_transport(scenario, request_timeout, |_| {}).await
    }
    async fn with_transport(
        scenario: DummyScenario,
        request_timeout: Duration,
        configure: impl FnOnce(&mut ServerLimits),
    ) -> Self {
        Self::with_limits(scenario, request_timeout, |_| {}, configure).await
    }
    async fn with_limits(
        scenario: DummyScenario,
        request_timeout: Duration,
        configure_runs: impl FnOnce(&mut RunLimits),
        configure: impl FnOnce(&mut ServerLimits),
    ) -> Self {
        Self::with_retention(scenario, request_timeout, configure_runs, configure, None).await
    }
    async fn with_retention(
        scenario: DummyScenario,
        request_timeout: Duration,
        configure_runs: impl FnOnce(&mut RunLimits),
        configure: impl FnOnce(&mut ServerLimits),
        retention: Option<RetentionConfig>,
    ) -> Self {
        let dir = tempdir().unwrap();
        let path = dir.path().join("runs.sqlite");
        let namespace = Namespace {
            owner_id: "owner".into(),
            client_id: "client".into(),
        };
        let all = [
            Scope::RunStart,
            Scope::RunRead,
            Scope::RunCancel,
            Scope::SessionRead,
        ];
        let entries = [
            ClientRegistration::new(
                "full",
                CredentialSecret::new([7; 32]),
                namespace.clone(),
                all,
                ["profile".into()],
            )
            .with_generation("full_v1"),
            ClientRegistration::new(
                "start",
                CredentialSecret::new([8; 32]),
                namespace.clone(),
                [Scope::RunStart],
                ["profile".into()],
            )
            .with_generation("start_v1"),
            ClientRegistration::new(
                "read",
                CredentialSecret::new([9; 32]),
                namespace.clone(),
                [Scope::RunRead],
                [],
            )
            .with_generation("read_v1"),
            ClientRegistration::new(
                "control",
                CredentialSecret::new([11; 32]),
                namespace.clone(),
                all,
                ["profile".into()],
            )
            .with_generation("control_v1"),
            ClientRegistration::new(
                "other",
                CredentialSecret::new([10; 32]),
                Namespace {
                    owner_id: "owner".into(),
                    client_id: "other".into(),
                },
                all,
                ["profile".into()],
            )
            .with_generation("other_v1"),
            ClientRegistration::new(
                "skill_admin",
                CredentialSecret::new([12; 32]),
                namespace.clone(),
                [
                    Scope::RunStart,
                    Scope::RunRead,
                    Scope::RunCancel,
                    Scope::SessionRead,
                    Scope::SkillRead,
                    Scope::SkillWrite,
                ],
                ["profile".into()],
            )
            .with_generation("skill_admin_v1"),
            ClientRegistration::new(
                "other_skill",
                CredentialSecret::new([13; 32]),
                Namespace {
                    owner_id: "owner".into(),
                    client_id: "other".into(),
                },
                [
                    Scope::RunStart,
                    Scope::RunRead,
                    Scope::SkillRead,
                    Scope::SkillWrite,
                ],
                ["profile".into()],
            )
            .with_generation("other_skill_v1"),
            ClientRegistration::new(
                "skill_write_only",
                CredentialSecret::new([14; 32]),
                namespace.clone(),
                [Scope::SkillWrite],
                [],
            )
            .with_generation("skill_write_only_v1"),
        ];
        let access = Arc::new(
            ClientAccessControl::new(
                entries,
                [
                    ExecutionProfile::dummy(
                        "profile",
                        ["workspace".into(), "workspace-other".into()],
                        4096,
                    ),
                    ExecutionProfile::dummy("hidden", ["private".into()], 4096),
                ],
                AccessLimits {
                    max_request_bytes: 8192,
                    max_skill_bytes: 1024,
                    max_identifier_bytes: 128,
                },
            )
            .unwrap(),
        );
        let backend = Arc::new(DummyBackend::new(scenario, 64));
        let profiles = vec![
            BackendProfile {
                profile_id: "profile".into(),
                revision: "profile_v1".into(),
                state_realm: "dummy".into(),
                backend: backend.clone(),
            },
            BackendProfile {
                profile_id: "hidden".into(),
                revision: "hidden_v1".into(),
                state_realm: "dummy".into(),
                backend: backend.clone(),
            },
        ];
        let mut runs = RunLimits {
            max_outstanding: 8,
            max_workers: 1,
            database_queue: 16,
            event_buffer: 16,
            max_events_per_run: 32,
            max_event_bytes: 2048,
            max_result_bytes: 4096,
            max_page_size: 16,
            run_timeout: Duration::from_secs(10),
            cleanup_grace: Duration::from_secs(1),
            poll_interval: Duration::from_millis(5),
        };
        configure_runs(&mut runs);
        let service = RunService::open_with_retention(
            &path,
            access.clone(),
            profiles,
            runs.clone(),
            None,
            retention,
        )
        .await
        .unwrap();
        let mut limits = ServerLimits {
            max_request_bytes: 16384,
            max_response_bytes: 1048576,
            max_header_bytes: 8192,
            max_inflight: 16,
            max_connections: 16,
            connection_idle_timeout: Duration::from_secs(6),
            connection_max_lifetime: Duration::from_secs(30),
            requests_per_second: 10000,
            burst: 1000,
            request_timeout,
            shutdown_grace: Duration::from_secs(3),
            max_mcp_sessions: 4,
            mcp_session_idle: Duration::from_secs(60),
        };
        configure(&mut limits);
        let server = Server::bind("127.0.0.1:0".parse().unwrap(), service, limits.clone())
            .await
            .unwrap();
        let url = format!("http://{}", server.local_addr().unwrap());
        let stop = CancellationToken::new();
        let task = tokio::spawn(server.run(stop.clone()));
        let client = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        Self {
            _dir: dir,
            path,
            url,
            client,
            stop,
            task,
            backend,
            access,
            run_limits: runs,
            transport_limits: limits,
        }
    }
    fn post(&self, path: &str, body: &Value, credential: &str) -> reqwest::RequestBuilder {
        self.client
            .post(format!("{}{path}", self.url))
            .bearer_auth(credential)
            .json(body)
    }
    async fn modern(&self, body: &Value, credential: &str) -> Response {
        let mut request = self
            .post("/mcp", body, credential)
            .header("Accept", "application/json, text/event-stream")
            .header("MCP-Protocol-Version", MODERN)
            .header("Mcp-Method", body["method"].as_str().unwrap());
        if let Some(name) = body.pointer("/params/name").and_then(Value::as_str) {
            request = request.header("Mcp-Name", name);
        }
        request.send().await.unwrap()
    }
    async fn tool(&self, name: &str, args: Value, credential: &str) -> Value {
        let r = self
            .modern(
                &rpc("tools/call", json!({"name":name,"arguments":args}), true),
                credential,
            )
            .await;
        assert_eq!(r.status(), 200);
        payload(r).await
    }
    async fn legacy_init(&self, version: &str, credential: &str) -> String {
        let body = rpc(
            "initialize",
            json!({"protocolVersion":version,"capabilities":{},"clientInfo":{"name":"independent-http-fixture","version":"1"}}),
            false,
        );
        let r = self
            .post("/mcp", &body, credential)
            .header("Accept", "application/json, text/event-stream")
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
        let session = r.headers()["Mcp-Session-Id"].to_str().unwrap().to_owned();
        assert_eq!(payload(r).await["result"]["protocolVersion"], version);
        let r = self
            .post(
                "/mcp",
                &json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
                credential,
            )
            .header("Accept", "application/json, text/event-stream")
            .header("MCP-Protocol-Version", version)
            .header("Mcp-Session-Id", &session)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 202);
        session
    }
    async fn wait_terminal(&self, id: &str) -> Value {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let r = self
                    .client
                    .get(format!("{}/v1/runs/{id}", self.url))
                    .bearer_auth(token("full", 7))
                    .send()
                    .await
                    .unwrap();
                assert_eq!(r.status(), 200);
                let value = payload(r).await;
                if matches!(
                    value["state"].as_str(),
                    Some("succeeded" | "failed" | "cancelled" | "timed_out" | "interrupted")
                ) {
                    break value;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap()
    }
    async fn finish(self) {
        self.stop.cancel();
        tokio::time::timeout(Duration::from_secs(5), self.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        // A request handle retained by an SDK session must not retain the DB owner.
        let connection = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.path)
            .unwrap();
        connection.try_lock().unwrap();
    }
}

#[tokio::test]
async fn skill_registry_enforces_immutable_versions_scopes_and_capabilities() {
    let f = Fixture::new(DummyScenario::Success).await;
    let admin = token("skill_admin", 12);
    let markdown = "---\nname: report\n---\nUse {{literal}} and $HOME.\n";
    let registration = json!({"skill_id":"report","version":"1.0","markdown":markdown,"required_capabilities":["literal_prompt"]});
    let r = f
        .post("/v1/skills", &registration, &admin)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(payload(r).await["created"], true);
    let r = f
        .post("/v1/skills", &registration, &admin)
        .send()
        .await
        .unwrap();
    assert_eq!(payload(r).await["created"], false);
    let r = f.post("/v1/skills", &json!({"skill_id":"report","version":"1.0","markdown":"changed","required_capabilities":["literal_prompt"]}), &admin).send().await.unwrap();
    assert_eq!(r.status(), 409);
    let r = f.post("/v1/skills", &json!({"skill_id":"report","version":"1.0","markdown":markdown,"required_capabilities":[]}), &admin).send().await.unwrap();
    assert_eq!(r.status(), 409);
    let exported = f
        .tool(
            "skills_get",
            json!({"skill_id":"report","version":"1.0"}),
            &admin,
        )
        .await;
    assert_eq!(
        exported["result"]["structuredContent"]["markdown"],
        markdown
    );
    let r = f
        .client
        .get(format!("{}/v1/skills?limit=5", f.url))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    assert_eq!(payload(r).await["items"][0]["skill_id"], "report");
    let r = f
        .client
        .get(format!("{}/v1/skills/report/1.0", f.url))
        .bearer_auth(token("other_skill", 13))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
    let r = f
        .post("/v1/skills", &registration, &token("skill_write_only", 14))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);
    let r = f.post("/v1/skills", &json!({"skill_id":"unsupported","version":"1","markdown":"text","required_capabilities":["filesystem"]}), &admin).send().await.unwrap();
    assert_eq!(r.status(), 200);
    let mut request = start("unavailable-capability");
    request["skills"] = json!([{"skill_id":"unsupported","version":"1"}]);
    let r = f.post("/v1/runs", &request, &admin).send().await.unwrap();
    assert_eq!(r.status(), 409);
    let mut null_skills = start("null-skill-selection");
    null_skills["skills"] = Value::Null;
    assert_eq!(
        f.post("/v1/runs", &null_skills, &admin)
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    assert!(f.backend.captured_inputs().is_empty());
    f.finish().await;
}

#[tokio::test]
async fn skill_snapshot_is_pinned_before_ack_and_replay_ignores_changed_defaults() {
    let f = Fixture::new(DummyScenario::DelayedSuccess).await;
    let admin = token("skill_admin", 12);
    for (version, markdown) in [("1", "first {{literal}}"), ("2", "second")] {
        assert_eq!(
            f.post(
                "/v1/skills",
                &json!({"skill_id":"report","version":version,"markdown":markdown}),
                &admin
            )
            .send()
            .await
            .unwrap()
            .status(),
            200
        );
    }
    let one = json!({"skill_id":"report","version":"1"});
    let two = json!({"skill_id":"report","version":"2"});
    assert_eq!(
        f.post("/v1/skill-defaults", &json!({"skills":[one]}), &admin)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    let blocker = payload(
        f.post("/v1/runs", &start("snapshot-blocker"), &admin)
            .send()
            .await
            .unwrap(),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while f.backend.captured_inputs().is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let request = start("snapshot-queued");
    let receipt = payload(f.post("/v1/runs", &request, &admin).send().await.unwrap()).await;
    let id = receipt["run_id"].as_str().unwrap().to_owned();
    assert_eq!(
        f.post("/v1/skill-defaults", &json!({"skills":[two]}), &admin)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    let replay = f.tool("agents_start", request.clone(), &admin).await;
    assert_eq!(replay["result"]["structuredContent"]["run_id"], id);
    assert_eq!(replay["result"]["structuredContent"]["replay"], true);
    let mut changed = request.clone();
    changed["skills"] = json!([]);
    assert_eq!(
        f.post("/v1/runs", &changed, &admin)
            .send()
            .await
            .unwrap()
            .status(),
        409
    );
    tokio::time::timeout(Duration::from_secs(8), async {
        while f.backend.captured_inputs().len() < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let captured = f.backend.captured_inputs();
    assert!(captured[1].request.prompt.contains("first {{literal}}"));
    assert!(!captured[1].request.prompt.contains("second"));
    let connection = rusqlite::Connection::open(&f.path).unwrap();
    let snapshot: Vec<u8> = connection
        .query_row(
            "SELECT prompt FROM attempt_prompt_snapshots WHERE run_id = ?1",
            [&id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(snapshot, captured[1].request.prompt.as_bytes());
    drop(connection);
    let r = f
        .post(&format!("/v1/runs/{id}/cancel"), &json!({}), &admin)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_ne!(blocker["run_id"], receipt["run_id"]);
    f.finish().await;
}

#[tokio::test]
async fn resume_requires_original_skill_bundle_and_explicit_selection_overrides_defaults() {
    let f = Fixture::new(DummyScenario::Success).await;
    let admin = token("skill_admin", 12);
    for version in ["1", "2"] {
        assert_eq!(
            f.post(
                "/v1/skills",
                &json!({"skill_id":"report","version":version,"markdown":version}),
                &admin
            )
            .send()
            .await
            .unwrap()
            .status(),
            200
        );
    }
    let one = json!({"skill_id":"report","version":"1"});
    let two = json!({"skill_id":"report","version":"2"});
    assert_eq!(
        f.post(
            "/v1/skill-defaults",
            &json!({"skills":[one.clone()]}),
            &admin
        )
        .send()
        .await
        .unwrap()
        .status(),
        200
    );
    let receipt = payload(
        f.post("/v1/runs", &start("session-first"), &admin)
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        f.wait_terminal(receipt["run_id"].as_str().unwrap()).await["state"],
        "succeeded"
    );
    assert_eq!(
        f.post("/v1/skill-defaults", &json!({"skills":[two]}), &admin)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    let mut resume = start("session-mismatch");
    resume["session"] = json!({"mode":"resume","session_id":receipt["session_id"]});
    assert_eq!(
        f.post("/v1/runs", &resume, &admin)
            .send()
            .await
            .unwrap()
            .status(),
        409
    );
    resume["idempotency_key"] = json!("session-matched");
    resume["skills"] = json!([one]);
    let r = f.post("/v1/runs", &resume, &admin).send().await.unwrap();
    assert_eq!(r.status(), 202);
    let resumed = payload(r).await;
    assert_eq!(
        f.wait_terminal(resumed["run_id"].as_str().unwrap()).await["state"],
        "succeeded"
    );
    assert_eq!(f.backend.captured_inputs().len(), 2);
    f.finish().await;
}

#[tokio::test]
async fn revoked_queued_skill_is_not_launched_but_existing_replay_survives() {
    let f = Fixture::new(DummyScenario::DelayedSuccess).await;
    let admin = token("skill_admin", 12);
    assert_eq!(
        f.post(
            "/v1/skills",
            &json!({"skill_id":"report","version":"1","markdown":"instruction"}),
            &admin
        )
        .send()
        .await
        .unwrap()
        .status(),
        200
    );
    assert_eq!(
        f.post(
            "/v1/skill-defaults",
            &json!({"skills":[{"skill_id":"report","version":"1"}]}),
            &admin
        )
        .send()
        .await
        .unwrap()
        .status(),
        200
    );
    let mut blocker = start("revoke-blocker");
    blocker["skills"] = json!([]);
    assert_eq!(
        f.post("/v1/runs", &blocker, &admin)
            .send()
            .await
            .unwrap()
            .status(),
        202
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        while f.backend.captured_inputs().is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let request = start("revoke-target");
    let receipt = payload(f.post("/v1/runs", &request, &admin).send().await.unwrap()).await;
    assert_eq!(
        f.post(
            "/v1/skills/report/1/enabled",
            &json!({"enabled":false}),
            &admin
        )
        .send()
        .await
        .unwrap()
        .status(),
        200
    );
    let replay = payload(f.post("/v1/runs", &request, &admin).send().await.unwrap()).await;
    assert_eq!(replay["run_id"], receipt["run_id"]);
    assert_eq!(replay["replay"], true);
    assert_eq!(
        f.post("/v1/runs", &start("revoked-new"), &admin)
            .send()
            .await
            .unwrap()
            .status(),
        409
    );
    let id = receipt["run_id"].as_str().unwrap();
    let state = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let r = f
                .client
                .get(format!("{}/v1/runs/{id}", f.url))
                .bearer_auth(&admin)
                .send()
                .await
                .unwrap();
            let value = payload(r).await;
            if value["state"] == "failed" {
                break value;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(state["state"], "failed");
    assert_eq!(f.backend.captured_inputs().len(), 1);
    f.finish().await;
}

#[tokio::test]
async fn revocation_after_prompt_commit_fails_only_the_affected_run() {
    let f = Fixture::new(DummyScenario::Success).await;
    let admin = token("skill_admin", 12);
    assert_eq!(
        f.post(
            "/v1/skills",
            &json!({"skill_id":"report","version":"1","markdown":"instruction"}),
            &admin
        )
        .send()
        .await
        .unwrap()
        .status(),
        200
    );
    let connection = rusqlite::Connection::open(&f.path).unwrap();
    connection.execute_batch("CREATE TRIGGER revoke_after_snapshot AFTER INSERT ON attempt_prompt_snapshots BEGIN UPDATE skill_policies SET enabled = 0 WHERE skill_id = 'report' AND version = '1'; END;").unwrap();
    drop(connection);
    let mut request = start("revoke-after-prompt");
    request["skills"] = json!([{"skill_id":"report","version":"1"}]);
    let receipt = payload(f.post("/v1/runs", &request, &admin).send().await.unwrap()).await;
    assert_eq!(
        f.wait_terminal(receipt["run_id"].as_str().unwrap()).await["state"],
        "failed"
    );
    assert!(f.backend.captured_inputs().is_empty());
    let mut next = start("after-revocation");
    next["skills"] = json!([]);
    let r = f.post("/v1/runs", &next, &admin).send().await.unwrap();
    assert_eq!(r.status(), 202);
    let next = payload(r).await;
    assert_eq!(
        f.wait_terminal(next["run_id"].as_str().unwrap()).await["state"],
        "succeeded"
    );
    assert_eq!(f.backend.captured_inputs().len(), 1);
    f.finish().await;
}

#[tokio::test]
async fn skill_use_requires_read_scope_but_explicit_empty_is_allowed() {
    let f = Fixture::new(DummyScenario::Success).await;
    let admin = token("skill_admin", 12);
    let start_only = token("start", 8);
    assert_eq!(
        f.post(
            "/v1/skills",
            &json!({"skill_id":"secret","version":"1","markdown":"private"}),
            &admin
        )
        .send()
        .await
        .unwrap()
        .status(),
        200
    );
    for id in ["secret", "not-present"] {
        let mut request = start("denied");
        request["skills"] = json!([{"skill_id":id,"version":"1"}]);
        assert_eq!(
            f.post("/v1/runs", &request, &start_only)
                .send()
                .await
                .unwrap()
                .status(),
            403
        );
    }
    assert_eq!(
        f.post(
            "/v1/skill-defaults",
            &json!({"skills":[{"skill_id":"secret","version":"1"}]}),
            &admin
        )
        .send()
        .await
        .unwrap()
        .status(),
        200
    );
    assert_eq!(
        f.post("/v1/runs", &start("default-denied"), &start_only)
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    let mut existing = start("known-replay-key");
    existing["skills"] = json!([{"skill_id":"secret","version":"1"}]);
    let original = payload(f.post("/v1/runs", &existing, &admin).send().await.unwrap()).await;
    assert_eq!(
        f.wait_terminal(original["run_id"].as_str().unwrap()).await["state"],
        "succeeded"
    );
    for id in ["secret", "not-present"] {
        existing["skills"] = json!([{"skill_id":id,"version":"1"}]);
        assert_eq!(
            f.post("/v1/runs", &existing, &start_only)
                .send()
                .await
                .unwrap()
                .status(),
            403
        );
    }
    let mut empty = start("explicit-empty");
    empty["skills"] = json!([]);
    let r = f
        .post("/v1/runs", &empty, &start_only)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 202);
    let receipt = payload(r).await;
    assert_eq!(
        f.wait_terminal(receipt["run_id"].as_str().unwrap()).await["state"],
        "succeeded"
    );
    assert_eq!(
        f.backend.captured_inputs().last().unwrap().request.prompt,
        "literal input"
    );
    f.finish().await;
}

#[tokio::test]
async fn legacy_skill_identifiers_can_be_exported_selected_and_paginated() {
    let f = Fixture::new(DummyScenario::Success).await;
    let admin = token("skill_admin", 12);
    let special = json!({"skill_id":"_legacy/path&name","version":"1.0+build"});
    let registration = json!({"skill_id":"_legacy/path&name","version":"1.0+build","markdown":"legacy instruction"});
    assert_eq!(
        f.post("/v1/skills", &registration, &admin)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    let r = f
        .post("/v1/skills/get", &special, &admin)
        .send()
        .await
        .unwrap();
    assert_eq!(payload(r).await["markdown"], "legacy instruction");
    let page = payload(
        f.post("/v1/skills/list", &json!({"limit":1}), &admin)
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(page["next_after"], special);
    let next = payload(
        f.post(
            "/v1/skills/list",
            &json!({"limit":1,"after":page["next_after"]}),
            &admin,
        )
        .send()
        .await
        .unwrap(),
    )
    .await;
    assert_eq!(next["items"].as_array().unwrap().len(), 0);
    let mut request = start("legacy-skill");
    request["skills"] = json!([special]);
    let receipt = payload(f.post("/v1/runs", &request, &admin).send().await.unwrap()).await;
    assert_eq!(
        f.wait_terminal(receipt["run_id"].as_str().unwrap()).await["state"],
        "succeeded"
    );
    assert!(
        f.backend.captured_inputs()[0]
            .request
            .prompt
            .contains("legacy instruction")
    );
    f.finish().await;
}

#[tokio::test]
async fn encoded_skill_paths_and_cursors_select_the_intended_identifier() {
    let f = Fixture::new(DummyScenario::Success).await;
    let admin = token("skill_admin", 12);
    for id in ["ops/prod", "ops%2Fprod", "a/b", "a0"] {
        let r = f
            .post(
                "/v1/skills",
                &json!({"skill_id":id,"version":"1","markdown":id}),
                &admin,
            )
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
    }
    let r = f
        .post(
            "/v1/skills/ops%2Fprod/1/enabled",
            &json!({"enabled":false}),
            &admin,
        )
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(payload(r).await["skill_id"], "ops/prod");
    for (id, expected) in [("ops/prod", false), ("ops%2Fprod", true)] {
        let r = f
            .post(
                "/v1/skills/get",
                &json!({"skill_id":id,"version":"1"}),
                &admin,
            )
            .send()
            .await
            .unwrap();
        assert_eq!(payload(r).await["enabled"], expected);
    }
    let r = f
        .client
        .get(format!(
            "{}/v1/skills?limit=1&after_skill_id=a%2Fb&after_version=1",
            f.url
        ))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    assert_eq!(payload(r).await["items"][0]["skill_id"], "a0");
    f.finish().await;
}

#[tokio::test]
async fn mcp_skill_registration_and_defaults_are_shared_with_rest() {
    let f = Fixture::new(DummyScenario::Success).await;
    let admin = token("skill_admin", 12);
    let registration = f
        .tool(
            "skills_register",
            json!({"skill_id":"mcp","version":"1","markdown":"mcp instruction"}),
            &admin,
        )
        .await;
    assert_eq!(registration["result"]["structuredContent"]["created"], true);
    let defaults = f
        .tool(
            "skills_set_defaults",
            json!({"skills":[{"skill_id":"mcp","version":"1"}]}),
            &admin,
        )
        .await;
    assert_eq!(
        defaults["result"]["structuredContent"]["skills"][0]["skill_id"],
        "mcp"
    );
    let r = f
        .client
        .get(format!("{}/v1/skill-defaults", f.url))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    assert_eq!(payload(r).await["skills"][0]["skill_id"], "mcp");
    let receipt = payload(
        f.post("/v1/runs", &start("mcp-skill"), &admin)
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        f.wait_terminal(receipt["run_id"].as_str().unwrap()).await["state"],
        "succeeded"
    );
    assert!(
        f.backend.captured_inputs()[0]
            .request
            .prompt
            .contains("mcp instruction")
    );
    f.finish().await;
}

#[tokio::test]
async fn skill_operations_work_with_both_legacy_mcp_revisions() {
    let f = Fixture::new(DummyScenario::Success).await;
    let admin = token("skill_admin", 12);
    for version in ["2025-11-25", "2025-06-18"] {
        let session = f.legacy_init(version, &admin).await;
        let body = rpc(
            "tools/call",
            json!({"name":"skills_register","arguments":{"skill_id":"legacy-mcp","version":version,"markdown":"legacy instruction"}}),
            false,
        );
        let r = f
            .post("/mcp", &body, &admin)
            .header("Accept", "application/json, text/event-stream")
            .header("MCP-Protocol-Version", version)
            .header("Mcp-Session-Id", &session)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
        let result = payload(r).await;
        assert!(result["result"].get("resultType").is_none());
        assert_eq!(result["result"]["structuredContent"]["created"], true);
        let body = rpc(
            "tools/call",
            json!({"name":"skills_get","arguments":{"skill_id":"legacy-mcp","version":version}}),
            false,
        );
        let r = f
            .post("/mcp", &body, &admin)
            .header("Accept", "application/json, text/event-stream")
            .header("MCP-Protocol-Version", version)
            .header("Mcp-Session-Id", &session)
            .send()
            .await
            .unwrap();
        assert_eq!(
            payload(r).await["result"]["structuredContent"]["markdown"],
            "legacy instruction"
        );
        let r = f
            .client
            .delete(format!("{}/mcp", f.url))
            .bearer_auth(&admin)
            .header("MCP-Protocol-Version", version)
            .header("Mcp-Session-Id", session)
            .send()
            .await
            .unwrap();
        assert!(r.status().is_success());
    }
    f.finish().await;
}

#[tokio::test]
async fn mcp_skill_discovery_matches_scopes_and_the_configured_page_limit() {
    let f = Fixture::new(DummyScenario::Success).await;
    let admin = token("skill_admin", 12);
    let result = payload(f.modern(&rpc("tools/list", json!({}), true), &admin).await).await;
    let tools = result["result"]["tools"].as_array().unwrap();
    let skill_list = tools
        .iter()
        .find(|tool| tool["name"] == "skills_list")
        .unwrap();
    assert_eq!(
        skill_list["inputSchema"]["properties"]["limit"]["maximum"],
        16
    );
    let write_only = token("skill_write_only", 14);
    let result = payload(
        f.modern(&rpc("tools/list", json!({}), true), &write_only)
            .await,
    )
    .await;
    assert!(result["result"]["tools"].as_array().unwrap().is_empty());
    f.finish().await;
}

#[tokio::test]
async fn oversized_stored_identifiers_fail_before_listing_or_default_resolution() {
    let f = Fixture::new(DummyScenario::Success).await;
    let admin = token("skill_admin", 12);
    let connection = rusqlite::Connection::open(&f.path).unwrap();
    connection
        .execute(
            "INSERT OR IGNORE INTO namespaces (owner_id, client_id) VALUES ('owner', 'client')",
            [],
        )
        .unwrap();
    let id = "x".repeat(256);
    connection.execute("INSERT INTO skill_versions (owner_id, client_id, skill_id, version, markdown) VALUES ('owner', 'client', ?1, '1', 'small')", [&id]).unwrap();
    connection.execute("INSERT INTO skill_defaults (owner_id, client_id, position, skill_id, version) VALUES ('owner', 'client', 0, ?1, '1')", [&id]).unwrap();
    drop(connection);
    let r = f
        .post("/v1/skills/list", &json!({"limit":1}), &admin)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    let r = f
        .client
        .get(format!("{}/v1/skill-defaults", f.url))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    assert_eq!(
        f.post("/v1/runs", &start("oversized-default"), &admin)
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    assert_eq!(
        f.post("/v1/skill-defaults", &json!({"skills":[]}), &admin)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    f.finish().await;
}

#[tokio::test]
async fn oversized_legacy_skill_is_rejected_and_can_still_be_revoked() {
    let f = Fixture::new(DummyScenario::Success).await;
    let admin = token("skill_admin", 12);
    let connection = rusqlite::Connection::open(&f.path).unwrap();
    connection
        .execute(
            "INSERT OR IGNORE INTO namespaces (owner_id, client_id) VALUES ('owner', 'client')",
            [],
        )
        .unwrap();
    connection.execute("INSERT INTO skill_versions (owner_id, client_id, skill_id, version, markdown) VALUES ('owner', 'client', 'large', '1', ?1)", ["x".repeat(2048)]).unwrap();
    drop(connection);
    let reference = json!({"skill_id":"large","version":"1"});
    let r = f
        .post("/v1/skills/get", &reference, &admin)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    let mut request = start("oversized-skill");
    request["skills"] = json!([reference]);
    let r = f.post("/v1/runs", &request, &admin).send().await.unwrap();
    assert_eq!(r.status(), 400);
    assert!(f.backend.captured_inputs().is_empty());
    assert_eq!(
        f.post(
            "/v1/skills/large/1/enabled",
            &json!({"enabled":false}),
            &admin
        )
        .send()
        .await
        .unwrap()
        .status(),
        200
    );
    f.finish().await;
}

#[tokio::test]
async fn snapshot_storage_failure_never_launches_backend() {
    let f = Fixture::new(DummyScenario::Success).await;
    let connection = rusqlite::Connection::open(&f.path).unwrap();
    connection.execute_batch("CREATE TRIGGER fail_prompt_snapshot BEFORE INSERT ON attempt_prompt_snapshots BEGIN SELECT RAISE(ABORT, 'injected'); END;").unwrap();
    drop(connection);
    let r = f
        .post(
            "/v1/runs",
            &start("snapshot-failure"),
            &token("skill_admin", 12),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 202);
    let receipt = payload(r).await;
    assert_eq!(
        f.wait_terminal(receipt["run_id"].as_str().unwrap()).await["state"],
        "failed"
    );
    assert!(f.backend.captured_inputs().is_empty());
    f.finish().await;
}

#[tokio::test]
async fn modern_discovery_tools_and_cross_transport_replay_execute_once() {
    let f = Fixture::new(DummyScenario::Success).await;
    let full = token("full", 7);
    let r = f
        .modern(&rpc("server/discover", json!({}), true), &full)
        .await;
    assert_eq!(r.status(), 200);
    assert!(!r.headers().contains_key("Mcp-Session-Id"));
    let d = payload(r).await;
    assert_eq!(d["result"]["resultType"], "complete");
    assert_eq!(
        d["result"]["supportedVersions"],
        json!([MODERN, "2025-11-25", "2025-06-18"])
    );
    assert_eq!(d["result"]["capabilities"], json!({"tools":{}}));
    let list = f.modern(&rpc("tools/list", json!({}), true), &full).await;
    assert_eq!(
        payload(list).await["result"]["tools"]
            .as_array()
            .unwrap()
            .len(),
        6
    );
    let profiles = f.tool("agents_list", json!({}), &full).await;
    assert_eq!(
        profiles["result"]["structuredContent"]["profiles"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        profiles["result"]["structuredContent"]["profiles"][0]["profile_id"],
        "profile"
    );
    let r = f
        .post("/v1/runs", &start("rest-first"), &full)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 202);
    assert!(r.headers().contains_key("Location"));
    let receipt = payload(r).await;
    let id = receipt["run_id"].as_str().unwrap();
    assert!(receipt.get("state").is_none());
    let replay = f.tool("agents_start", start("rest-first"), &full).await;
    assert_eq!(replay["result"]["structuredContent"]["run_id"], id);
    assert_eq!(replay["result"]["structuredContent"]["replay"], true);
    assert_eq!(f.wait_terminal(id).await["state"], "succeeded");
    let r = f.tool("agents_start", start("mcp-first"), &full).await;
    let id2 = r["result"]["structuredContent"]["run_id"].as_str().unwrap();
    let r = f
        .post("/v1/runs", &start("mcp-first"), &full)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(payload(r).await["run_id"], id2);
    f.wait_terminal(id2).await;
    assert_eq!(f.backend.captured_inputs().len(), 2);
    let r = f
        .client
        .get(format!("{}/v1/runs/{id}/result", f.url))
        .bearer_auth(&full)
        .send()
        .await
        .unwrap();
    let result = payload(r).await;
    assert_eq!(result["availability"], "available");
    assert_eq!(result["encoding"], "utf8");
    assert_eq!(result["metadata"]["synthetic"], true);
    f.finish().await;
}

#[tokio::test]
async fn sdk_client_discovers_calls_and_replays_across_real_http() {
    use rmcp::{
        ClientLifecycleMode, ClientServiceExt,
        model::{CallToolRequestParams, ClientConfig, ProtocolVersion},
        transport::{
            StreamableHttpClientTransport,
            streamable_http_client::StreamableHttpClientTransportConfig,
        },
    };
    let f = Fixture::new(DummyScenario::Success).await;
    let transport = StreamableHttpClientTransport::with_client(
        f.client.clone(),
        StreamableHttpClientTransportConfig::with_uri(format!("{}/mcp", f.url))
            .auth_header(token("full", 7)),
    );
    let client = ClientConfig::default()
        .serve_with_lifecycle(
            transport,
            ClientLifecycleMode::Discover {
                preferred_versions: vec![ProtocolVersion::V_2026_07_28],
            },
        )
        .await
        .unwrap();
    assert_eq!(client.list_tools(None).await.unwrap().tools.len(), 6);
    let args = start("sdk-client").as_object().unwrap().clone();
    let result = client
        .call_tool(CallToolRequestParams::new("agents_start").with_arguments(args))
        .await
        .unwrap();
    assert_eq!(result.is_error, Some(false));
    let receipt = result.structured_content.unwrap();
    let id = receipt["run_id"].as_str().unwrap();
    f.wait_terminal(id).await;
    let r = f
        .post("/v1/runs", &start("sdk-client"), &token("full", 7))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(payload(r).await["run_id"], id);
    assert_eq!(f.backend.captured_inputs().len(), 1);
    client.cancel().await.unwrap();
    f.finish().await;
}

#[tokio::test]
async fn both_legacy_revisions_use_current_credentials_and_owned_sessions() {
    let f = Fixture::new(DummyScenario::Success).await;
    let full = token("full", 7);
    for version in ["2025-11-25", "2025-06-18"] {
        let session = f.legacy_init(version, &full).await;
        let body = rpc(
            "tools/call",
            json!({"name":"agents_start","arguments":start(version)}),
            false,
        );
        let r = f
            .post("/mcp", &body, &full)
            .header("Accept", "application/json, text/event-stream")
            .header("MCP-Protocol-Version", version)
            .header("Mcp-Session-Id", &session)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
        let receipt = payload(r).await;
        assert!(!receipt["result"].get("resultType").is_some());
        let id = receipt["result"]["structuredContent"]["run_id"]
            .as_str()
            .expect("current request principal must reach the handler");
        f.wait_terminal(id).await;
        for method in [reqwest::Method::POST, reqwest::Method::DELETE] {
            let mut r = f
                .client
                .request(method.clone(), format!("{}/mcp", f.url))
                .bearer_auth(token("other", 10))
                .header("MCP-Protocol-Version", version)
                .header("Mcp-Session-Id", &session)
                .header("Accept", "application/json, text/event-stream");
            if method == reqwest::Method::POST {
                r = r.json(&body);
            }
            assert_eq!(r.send().await.unwrap().status(), 404);
        }
        let r = f
            .client
            .post(format!("{}/mcp", f.url))
            .header("MCP-Protocol-Version", version)
            .header("Mcp-Session-Id", &session)
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 401);
        let r = f
            .client
            .delete(format!("{}/mcp", f.url))
            .bearer_auth(&full)
            .header("MCP-Protocol-Version", version)
            .header("Mcp-Session-Id", session)
            .send()
            .await
            .unwrap();
        assert!(r.status().is_success());
        let r = f
            .client
            .get(format!("{}/v1/runs/{id}", f.url))
            .bearer_auth(&full)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
    }
    assert_eq!(f.backend.captured_inputs().len(), 2);
    f.finish().await;
}

#[tokio::test]
async fn scopes_namespace_revocation_and_client_info_do_not_establish_identity() {
    let f = Fixture::new(DummyScenario::Success).await;
    let full = token("full", 7);
    let start_only = token("start", 8);
    let r = f
        .post("/v1/runs", &start("scopes"), &start_only)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 202);
    let receipt = payload(r).await;
    let id = receipt["run_id"].as_str().unwrap();
    f.wait_terminal(id).await;
    let replay = f.tool("agents_start", start("scopes"), &start_only).await;
    assert!(replay["result"]["structuredContent"].get("state").is_none());
    let mut resume = start("no-session-scope");
    resume["session"] = json!({"mode":"resume","session_id":receipt["session_id"]});
    assert_eq!(
        f.post("/v1/runs", &resume, &start_only)
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(
        f.modern(
            &rpc(
                "tools/call",
                json!({"name":"agents_start","arguments":resume}),
                true
            ),
            &start_only
        )
        .await
        .status(),
        403
    );
    let r = f
        .client
        .get(format!("{}/v1/runs/{id}", f.url))
        .bearer_auth(&start_only)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);
    let b = rpc(
        "tools/call",
        json!({"name":"runs_get","arguments":{"run_id":id}}),
        true,
    );
    assert_eq!(f.modern(&b, &start_only).await.status(), 403);
    let mut spoof = b.clone();
    spoof["params"]["_meta"]["io.modelcontextprotocol/clientInfo"] =
        json!({"name":"full","version":"1"});
    let r = f.modern(&spoof, &token("other", 10)).await;
    assert_eq!(r.status(), 200);
    assert_eq!(payload(r).await["result"]["isError"], true);
    let r = f
        .client
        .get(format!("{}/v1/runs/{id}", f.url))
        .bearer_auth(token("other", 10))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
    let read = token("read", 9);
    let r = f.modern(&rpc("tools/list", json!({}), true), &read).await;
    assert_eq!(
        payload(r).await["result"]["tools"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    assert_eq!(
        f.modern(
            &rpc(
                "tools/call",
                json!({"name":"agents_start","arguments":start("denied")}),
                true
            ),
            &read
        )
        .await
        .status(),
        403
    );
    f.access.revoke("full").unwrap();
    assert_eq!(
        f.modern(&rpc("server/discover", json!({}), true), &full)
            .await
            .status(),
        401
    );
    f.finish().await;
}

#[tokio::test]
async fn protocol_validation_and_strict_business_parsing_have_no_execution_side_effects() {
    let f = Fixture::new(DummyScenario::Success).await;
    let full = token("full", 7);
    let mut b = rpc("tools/list", json!({}), true);
    b["params"]["_meta"]["io.modelcontextprotocol/protocolVersion"] = json!("2025-11-25");
    let r = f.modern(&b, &full).await;
    assert_eq!(r.status(), 400);
    assert_eq!(payload(r).await["error"]["code"], -32020);
    let mut b = rpc("tools/list", json!({}), true);
    b["params"]["_meta"]
        .as_object_mut()
        .unwrap()
        .remove("io.modelcontextprotocol/clientCapabilities");
    let r = f.modern(&b, &full).await;
    assert_eq!(r.status(), 400);
    assert_eq!(payload(r).await["error"]["code"], -32602);
    let b = rpc(
        "tools/list",
        json!({"_meta":{"io.modelcontextprotocol/protocolVersion":"1900-01-01","io.modelcontextprotocol/clientCapabilities":{}}}),
        false,
    );
    let r = f
        .post("/mcp", &b, &full)
        .header("Accept", "application/json, text/event-stream")
        .header("MCP-Protocol-Version", "1900-01-01")
        .header("Mcp-Method", "tools/list")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    let p = payload(r).await;
    assert_eq!(p["error"]["code"], -32022);
    assert_eq!(
        p["error"]["data"]["supported"],
        json!([MODERN, "2025-11-25", "2025-06-18"])
    );
    let mut unknown = start("unknown");
    unknown["client_id"] = json!("other");
    assert_eq!(
        f.post("/v1/runs", &unknown, &full)
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    assert_eq!(
        f.tool("agents_start", unknown, &full).await["result"]["isError"],
        true
    );
    let mut oversized_input = start("unicode-input");
    oversized_input["input"]["request"] = json!("한".repeat(1366));
    assert_eq!(
        f.post("/v1/runs", &oversized_input, &full)
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    assert_eq!(
        f.tool("agents_start", oversized_input, &full).await["result"]["isError"],
        true
    );
    let mismatch = rpc(
        "tools/call",
        json!({"name":"agents_start","arguments":start("header-mismatch")}),
        true,
    );
    let r = f
        .post("/mcp", &mismatch, &full)
        .header("Accept", "application/json, text/event-stream")
        .header("MCP-Protocol-Version", MODERN)
        .header("Mcp-Method", "tools/call")
        .header("Mcp-Name", "runs_cancel")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    assert_eq!(payload(r).await["error"]["code"], -32020);
    let encoded = rpc(
        "tools/call",
        json!({"name":"runs_get","arguments":{"run_id":"not-found"}}),
        true,
    );
    let r = f
        .post("/mcp", &encoded, &full)
        .header("Accept", "application/json, text/event-stream")
        .header("MCP-Protocol-Version", MODERN)
        .header("Mcp-Method", "tools/call")
        .header("Mcp-Name", "=?base64?cnVuc19nZXQ=?=")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(payload(r).await["result"]["isError"], true);
    let unknown_method = rpc("unknown/method", json!({}), true);
    let r = f.modern(&unknown_method, &full).await;
    assert_eq!(r.status(), 404);
    assert_eq!(payload(r).await["error"]["code"], -32601);
    let malformed = f
        .client
        .post(format!("{}/mcp", f.url))
        .bearer_auth(&full)
        .header("Content-Type", "application/json")
        .body("{")
        .send()
        .await
        .unwrap();
    assert_eq!(malformed.status(), 400);
    assert_eq!(payload(malformed).await["error"]["code"], -32700);
    let duplicate = r#"{"profile_id":"profile","workspace_id":"workspace","input":{"request":"one","request":"two"},"idempotency_key":"duplicate"}"#;
    let r = f
        .client
        .post(format!("{}/v1/runs", f.url))
        .bearer_auth(&full)
        .header("Content-Type", "application/json")
        .body(duplicate)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    let duplicate_rpc = format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{{"name":"agents_start","arguments":{duplicate},"_meta":{{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{{}}}}}}}}"#
    );
    let r = f
        .client
        .post(format!("{}/mcp", f.url))
        .bearer_auth(&full)
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .header("MCP-Protocol-Version", MODERN)
        .header("Mcp-Method", "tools/call")
        .header("Mcp-Name", "agents_start")
        .body(duplicate_rpc)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    assert_eq!(
        f.post("/mcp", &json!([rpc("tools/list", json!({}), true)]), &full)
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    assert_eq!(
        f.post("/v1/runs", &json!({"padding":"x".repeat(20000)}), &full)
            .send()
            .await
            .unwrap()
            .status(),
        413
    );
    assert!(f.backend.captured_inputs().is_empty());
    f.finish().await;
}

#[tokio::test]
async fn pending_pagination_explicit_cancel_and_failed_run_are_normal_read_results() {
    let f = Fixture::new(DummyScenario::DelayedSuccess).await;
    let full = token("full", 7);
    let r = f
        .post("/v1/runs", &start("pending"), &full)
        .send()
        .await
        .unwrap();
    let receipt = payload(r).await;
    let id = receipt["run_id"].as_str().unwrap();
    let r = f.tool("runs_result", json!({"run_id":id}), &full).await;
    assert_eq!(r["result"]["structuredContent"]["availability"], "pending");
    assert_eq!(r["result"]["isError"], false);
    let r = f
        .client
        .get(format!("{}/v1/runs/{id}/events?limit=1", f.url))
        .bearer_auth(&full)
        .send()
        .await
        .unwrap();
    let page = payload(r).await;
    assert_eq!(page["events"].as_array().unwrap().len(), 1);
    assert!(page["events"][0].get("payload").is_none());
    let cursor = page["next_after"].as_i64().unwrap();
    let p = f
        .tool(
            "runs_events",
            json!({"run_id":id,"after":cursor,"limit":1}),
            &full,
        )
        .await;
    if let Some(e) = p["result"]["structuredContent"]["events"][0].get("sequence") {
        assert!(e.as_i64().unwrap() > cursor);
    }
    let r = f
        .post(&format!("/v1/runs/{id}/cancel"), &json!({}), &full)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(payload(r).await["outcome"], "cancel_requested");
    assert_eq!(f.wait_terminal(id).await["state"], "cancelled");
    let r = f.tool("runs_result", json!({"run_id":id}), &full).await;
    assert_eq!(r["result"]["isError"], false);
    assert_eq!(r["result"]["structuredContent"]["state"], "cancelled");
    for suffix in ["", "/result", "/events?limit=1"] {
        assert_eq!(
            f.client
                .get(format!("{}/v1/runs/unknown{suffix}", f.url))
                .bearer_auth(&full)
                .send()
                .await
                .unwrap()
                .status(),
            404
        );
    }
    assert_eq!(
        f.client
            .get(format!("{}/v1/runs/{id}/events?limit=0", f.url))
            .bearer_auth(&full)
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    f.finish().await;
    let f = Fixture::new(DummyScenario::Failure).await;
    let r = f.tool("agents_start", start("failure"), &full).await;
    let id = r["result"]["structuredContent"]["run_id"].as_str().unwrap();
    assert_eq!(f.wait_terminal(id).await["state"], "failed");
    assert_eq!(
        f.tool("runs_result", json!({"run_id":id}), &full).await["result"]["isError"],
        false
    );
    f.finish().await;
}

#[tokio::test]
async fn opaque_legacy_result_bytes_are_not_assumed_to_be_json() {
    let f = Fixture::new(DummyScenario::Success).await;
    let full = token("full", 7);
    let r = f
        .post("/v1/runs", &start("opaque"), &full)
        .send()
        .await
        .unwrap();
    let receipt = payload(r).await;
    let id = receipt["run_id"].as_str().unwrap();
    f.wait_terminal(id).await;
    let db = rusqlite::Connection::open(&f.path).unwrap();
    db.execute(
        "UPDATE results SET payload=?1 WHERE run_id=?2",
        rusqlite::params![vec![255u8, 0, 128], id],
    )
    .unwrap();
    let p = f.tool("runs_result", json!({"run_id":id}), &full).await;
    assert_eq!(p["result"]["structuredContent"]["encoding"], "base64");
    assert_eq!(p["result"]["structuredContent"]["data"], "/wCA");
    db.execute(
        "UPDATE results SET payload=?1 WHERE run_id=?2",
        rusqlite::params![vec![0u8; 4097], id],
    )
    .unwrap();
    let r = f
        .client
        .get(format!("{}/v1/runs/{id}/result", f.url))
        .bearer_auth(&full)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 500);
    assert_eq!(payload(r).await["error"]["code"], "stored_data_limit");
    db.execute(
        "UPDATE results SET payload=?1 WHERE run_id=?2",
        rusqlite::params![vec![1u8], id],
    )
    .unwrap();
    db.execute(
        "UPDATE runs SET scenario=?1 WHERE run_id=?2",
        rusqlite::params!["x".repeat(2049), id],
    )
    .unwrap();
    for suffix in ["", "/result", "/events?limit=1"] {
        let response = f
            .client
            .get(format!("{}/v1/runs/{id}{suffix}", f.url))
            .bearer_auth(&full)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 500);
        assert_eq!(
            payload(response).await["error"]["code"],
            "stored_data_limit"
        );
    }
    drop(db);
    f.finish().await;
}

#[tokio::test]
async fn lost_ack_and_disconnected_client_do_not_cancel_or_repeat_admitted_work() {
    let f = Fixture::new(DummyScenario::DelayedSuccess).await;
    let full = token("full", 7);
    let address = f.url.strip_prefix("http://").unwrap();
    let mut connection = tokio::net::TcpStream::connect(address).await.unwrap();
    let body = start("lost-ack").to_string();
    connection.write_all(format!("POST /v1/runs HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {full}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while f.backend.captured_inputs().is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    drop(connection);
    let r = f.tool("agents_start", start("lost-ack"), &full).await;
    assert_eq!(r["result"]["structuredContent"]["replay"], true);
    let id = r["result"]["structuredContent"]["run_id"].as_str().unwrap();
    assert_eq!(
        f.tool("runs_get", json!({"run_id":id}), &full).await["result"]["structuredContent"]["state"],
        "running"
    );
    assert_eq!(f.backend.captured_inputs().len(), 1);
    f.tool("runs_cancel", json!({"run_id":id}), &full).await;
    f.wait_terminal(id).await;
    f.finish().await;
}

#[tokio::test]
async fn handler_timeout_during_sqlite_wait_keeps_the_original_admission() {
    let f = Fixture::with_timeout(DummyScenario::Success, Duration::from_millis(80)).await;
    let full = token("full", 7);
    let db = rusqlite::Connection::open(&f.path).unwrap();
    db.execute_batch("BEGIN IMMEDIATE").unwrap();
    let r = f
        .post("/v1/runs", &start("timeout"), &full)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 504);
    assert_eq!(payload(r).await["error"]["code"], "request_timeout");
    db.execute_batch("COMMIT").unwrap();
    drop(db);
    tokio::time::sleep(Duration::from_millis(20)).await;
    let r = f
        .post("/v1/runs", &start("timeout"), &full)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let receipt = payload(r).await;
    assert_eq!(receipt["replay"], true);
    f.wait_terminal(receipt["run_id"].as_str().unwrap()).await;
    assert_eq!(f.backend.captured_inputs().len(), 1);
    f.finish().await;
}

#[tokio::test]
async fn host_origin_and_bearer_are_checked_before_any_dispatch() {
    let f = Fixture::new(DummyScenario::Success).await;
    let full = token("full", 7);
    assert_eq!(
        f.client
            .get(format!("{}/v1/agents", f.url))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        f.client
            .get(format!("{}/v1/agents", f.url))
            .bearer_auth("invalid")
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        f.post("/v1/runs", &start("origin"), &full)
            .header("Origin", "https://untrusted.example")
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(
        f.post("/v1/runs", &start("host"), &full)
            .header("Host", "evil.example")
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    {
        use tokio::io::AsyncReadExt;
        let address = f.url.strip_prefix("http://").unwrap();
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let body = start("absolute-authority").to_string();
        stream.write_all(format!("POST http://evil.example/v1/runs HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {full}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 400"), "{response}");
    }
    assert!(f.backend.captured_inputs().is_empty());
    f.finish().await;
}

#[tokio::test]
async fn only_the_exact_mcp_path_is_routed_and_unsupported_params_never_start_work() {
    let f = Fixture::new(DummyScenario::Success).await;
    let full = token("full", 7);
    let session = f.legacy_init("2025-11-25", &full).await;
    let body = rpc(
        "tools/call",
        json!({"name":"agents_start","arguments":start("bypass")}),
        false,
    );
    for path in ["/mcp/", "/mcp/child"] {
        let r = f
            .post(path, &body, &full)
            .header("Accept", "application/json, text/event-stream")
            .header("Mcp-Session-Id", &session)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 404);
    }
    let mut body = rpc(
        "tools/call",
        json!({"name":"agents_start","arguments":start("extra")}),
        true,
    );
    body["params"]["client_id"] = json!("other");
    let r = f.modern(&body, &full).await;
    assert_eq!(r.status(), 400);
    assert_eq!(payload(r).await["error"]["code"], -32602);
    let mut body = rpc("tools/list", json!({}), true);
    body["id"] = json!("x".repeat(12000));
    let r = f
        .post("/mcp", &body, &full)
        .header("Accept", "application/json, text/event-stream")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    assert!(r.bytes().await.unwrap().len() < 8192);
    assert!(f.backend.captured_inputs().is_empty());
    f.finish().await;
}

#[tokio::test]
async fn duplicate_legacy_request_ids_are_rejected_before_either_response_is_reassigned() {
    let f = Fixture::new(DummyScenario::Success).await;
    let full = token("full", 7);
    let session = f.legacy_init("2025-11-25", &full).await;
    let db = rusqlite::Connection::open(&f.path).unwrap();
    db.execute_batch("BEGIN IMMEDIATE").unwrap();
    let first = rpc(
        "tools/call",
        json!({"name":"agents_start","arguments":start("first-id")}),
        false,
    );
    let client = f.client.clone();
    let url = format!("{}/mcp", f.url);
    let auth = full.clone();
    let sid = session.clone();
    let running = tokio::spawn(async move {
        client
            .post(url)
            .bearer_auth(auth)
            .header("Accept", "application/json, text/event-stream")
            .header("MCP-Protocol-Version", "2025-11-25")
            .header("Mcp-Session-Id", sid)
            .json(&first)
            .send()
            .await
            .unwrap()
    });
    tokio::time::sleep(Duration::from_millis(40)).await;
    let second = rpc(
        "tools/call",
        json!({"name":"agents_start","arguments":start("second-id")}),
        false,
    );
    let r = f
        .post("/mcp", &second, &full)
        .header("Accept", "application/json, text/event-stream")
        .header("MCP-Protocol-Version", "2025-11-25")
        .header("Mcp-Session-Id", &session)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    assert_eq!(payload(r).await["error"]["code"], -32600);
    db.execute_batch("COMMIT").unwrap();
    drop(db);
    let r = running.await.unwrap();
    assert_eq!(r.status(), 200);
    let id = payload(r).await["result"]["structuredContent"]["run_id"]
        .as_str()
        .unwrap()
        .to_owned();
    f.wait_terminal(&id).await;
    let r = f
        .post("/mcp", &second, &full)
        .header("Accept", "application/json, text/event-stream")
        .header("MCP-Protocol-Version", "2025-11-25")
        .header("Mcp-Session-Id", &session)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let id = payload(r).await["result"]["structuredContent"]["run_id"]
        .as_str()
        .unwrap()
        .to_owned();
    f.wait_terminal(&id).await;
    assert_eq!(f.backend.captured_inputs().len(), 2);
    f.finish().await;
}

#[tokio::test]
async fn legacy_timeout_retires_transport_session_but_preserves_a_committed_run() {
    let f = Fixture::with_timeout(DummyScenario::Success, Duration::from_millis(100)).await;
    let full = token("full", 7);
    let session = f.legacy_init("2025-11-25", &full).await;
    let db = rusqlite::Connection::open(&f.path).unwrap();
    db.execute_batch("BEGIN IMMEDIATE").unwrap();
    let body = rpc(
        "tools/call",
        json!({"name":"agents_start","arguments":start("legacy-timeout")}),
        false,
    );
    let r = f
        .post("/mcp", &body, &full)
        .header("Accept", "application/json, text/event-stream")
        .header("MCP-Protocol-Version", "2025-11-25")
        .header("Mcp-Session-Id", &session)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 504);
    db.execute_batch("COMMIT").unwrap();
    drop(db);
    tokio::time::sleep(Duration::from_millis(30)).await;
    let r = f
        .post("/mcp", &body, &full)
        .header("Accept", "application/json, text/event-stream")
        .header("MCP-Protocol-Version", "2025-11-25")
        .header("Mcp-Session-Id", &session)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
    let replay = f
        .post("/v1/runs", &start("legacy-timeout"), &full)
        .send()
        .await
        .unwrap();
    assert_eq!(replay.status(), 200);
    let receipt = payload(replay).await;
    assert_eq!(receipt["replay"], true);
    f.wait_terminal(receipt["run_id"].as_str().unwrap()).await;
    assert_eq!(f.backend.captured_inputs().len(), 1);
    f.finish().await;
}

#[tokio::test]
async fn global_request_and_rate_limits_bound_unauthenticated_and_concurrent_requests() {
    let f = Fixture::with_transport(DummyScenario::Success, Duration::from_secs(3), |limits| {
        limits.max_inflight = 1
    })
    .await;
    let full = token("full", 7);
    let db = rusqlite::Connection::open(&f.path).unwrap();
    db.execute_batch("BEGIN IMMEDIATE").unwrap();
    let c = f.client.clone();
    let url = format!("{}/v1/runs", f.url);
    let auth = full.clone();
    let first = tokio::spawn(async move {
        c.post(url)
            .bearer_auth(auth)
            .json(&start("capacity"))
            .send()
            .await
            .unwrap()
    });
    tokio::time::sleep(Duration::from_millis(40)).await;
    let rejected = f
        .client
        .get(format!("{}/v1/agents", f.url))
        .bearer_auth(&full)
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), 429);
    db.execute_batch("COMMIT").unwrap();
    drop(db);
    let r = first.await.unwrap();
    assert_eq!(r.status(), 202);
    let receipt = payload(r).await;
    f.wait_terminal(receipt["run_id"].as_str().unwrap()).await;
    f.finish().await;
    let f = Fixture::with_transport(DummyScenario::Success, Duration::from_secs(3), |limits| {
        limits.requests_per_second = 1;
        limits.burst = 1;
    })
    .await;
    assert_eq!(
        f.client
            .get(format!("{}/v1/agents", f.url))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        f.client
            .get(format!("{}/v1/agents", f.url))
            .bearer_auth(full)
            .send()
            .await
            .unwrap()
            .status(),
        429
    );
    f.finish().await;
}

#[tokio::test]
async fn legacy_session_capacity_is_bounded_and_released_on_delete() {
    let f = Fixture::with_transport(DummyScenario::Success, Duration::from_secs(3), |limits| {
        limits.max_mcp_sessions = 1
    })
    .await;
    let full = token("full", 7);
    let session = f.legacy_init("2025-11-25", &full).await;
    let body = rpc(
        "initialize",
        json!({"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"fixture","version":"1"}}),
        false,
    );
    let r = f
        .post("/mcp", &body, &full)
        .header("Accept", "application/json, text/event-stream")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 429);
    let r = f
        .client
        .delete(format!("{}/mcp", f.url))
        .bearer_auth(&full)
        .header("MCP-Protocol-Version", "2025-11-25")
        .header("Mcp-Session-Id", session)
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success());
    f.legacy_init("2025-06-18", &full).await;
    f.finish().await;
}

#[tokio::test]
async fn idle_partial_headers_cannot_hold_unbounded_connection_capacity() {
    let f = Fixture::with_transport(
        DummyScenario::Success,
        Duration::from_millis(100),
        |limits| {
            limits.max_connections = 1;
            limits.connection_idle_timeout = Duration::from_millis(500);
        },
    )
    .await;
    let address = f.url.strip_prefix("http://").unwrap();
    let mut slow = tokio::net::TcpStream::connect(address).await.unwrap();
    slow.write_all(format!("GET /v1/agents HTTP/1.1\r\nHost: {address}\r\n").as_bytes())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let request = f
        .client
        .get(format!("{}/v1/agents", f.url))
        .bearer_auth(token("full", 7))
        .send();
    tokio::pin!(request);
    assert!(
        tokio::time::timeout(Duration::from_millis(80), &mut request)
            .await
            .is_err()
    );
    let response = tokio::time::timeout(Duration::from_secs(2), request)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), 200);
    drop(slow);
    f.finish().await;
}

#[tokio::test]
async fn cross_transport_replay_precedes_queue_saturation_and_rejects_conflicts() {
    let f = Fixture::with_limits(
        DummyScenario::DelayedSuccess,
        Duration::from_secs(3),
        |runs| runs.max_outstanding = 1,
        |_| {},
    )
    .await;
    let full = token("full", 7);
    let accepted = f
        .post("/v1/runs", &start("queue-full"), &full)
        .send()
        .await
        .unwrap();
    assert_eq!(accepted.status(), 202);
    let receipt = payload(accepted).await;
    let id = receipt["run_id"].as_str().unwrap();
    assert_eq!(
        f.post("/v1/runs", &start("other-key"), &full)
            .send()
            .await
            .unwrap()
            .status(),
        429
    );
    let replay = f.tool("agents_start", start("queue-full"), &full).await;
    assert_eq!(replay["result"]["structuredContent"]["run_id"], id);
    assert_eq!(replay["result"]["structuredContent"]["replay"], true);
    let mut conflict = start("queue-full");
    conflict["input"]["request"] = json!("different");
    assert_eq!(
        f.post("/v1/runs", &conflict, &full)
            .send()
            .await
            .unwrap()
            .status(),
        409
    );
    let result = f.tool("agents_start", conflict, &full).await;
    assert_eq!(result["result"]["isError"], true);
    assert_eq!(
        result["result"]["structuredContent"]["error"]["code"],
        "idempotency_conflict"
    );
    f.tool("runs_cancel", json!({"run_id":id}), &full).await;
    f.wait_terminal(id).await;
    f.finish().await;
}

#[tokio::test]
async fn legacy_rpc_cancellation_does_not_cancel_a_durably_admitted_run() {
    let f = Fixture::new(DummyScenario::Success).await;
    let full = token("full", 7);
    let session = f.legacy_init("2025-11-25", &full).await;
    let db = rusqlite::Connection::open(&f.path).unwrap();
    db.execute_batch("BEGIN IMMEDIATE").unwrap();
    let body = rpc(
        "tools/call",
        json!({"name":"agents_start","arguments":start("rpc-cancel")}),
        false,
    );
    let client = f.client.clone();
    let url = format!("{}/mcp", f.url);
    let auth = full.clone();
    let sid = session.clone();
    let request = tokio::spawn(async move {
        client
            .post(url)
            .bearer_auth(auth)
            .header("Accept", "application/json, text/event-stream")
            .header("MCP-Protocol-Version", "2025-11-25")
            .header("Mcp-Session-Id", sid)
            .json(&body)
            .send()
            .await
    });
    tokio::time::sleep(Duration::from_millis(40)).await;
    let notice = json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":1,"reason":"fixture"}});
    let r = f
        .post("/mcp", &notice, &full)
        .header("Accept", "application/json, text/event-stream")
        .header("MCP-Protocol-Version", "2025-11-25")
        .header("Mcp-Session-Id", &session)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 202);
    let _ = tokio::time::timeout(Duration::from_secs(2), request).await;
    let other = rpc(
        "tools/call",
        json!({"name":"agents_start","arguments":start("after-cancel-same-id")}),
        false,
    );
    let blocked = f
        .post("/mcp", &other, &full)
        .header("Accept", "application/json, text/event-stream")
        .header("MCP-Protocol-Version", "2025-11-25")
        .header("Mcp-Session-Id", &session)
        .send()
        .await
        .unwrap();
    assert_eq!(blocked.status(), 404);
    db.execute_batch("COMMIT").unwrap();
    let blocked_count: i64 = db
        .query_row(
            "SELECT count(*) FROM runs WHERE idempotency_key='after-cancel-same-id'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(blocked_count, 0);
    drop(db);
    tokio::time::sleep(Duration::from_millis(20)).await;
    let r = f
        .post("/v1/runs", &start("rpc-cancel"), &full)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let replay = payload(r).await;
    assert_eq!(replay["replay"], true);
    assert_eq!(
        f.wait_terminal(replay["run_id"].as_str().unwrap()).await["state"],
        "succeeded"
    );
    assert_eq!(f.backend.captured_inputs().len(), 1);
    f.finish().await;
}

#[tokio::test]
async fn replay_after_profile_revision_change_keeps_the_original_run_across_transports() {
    let f = Fixture::new(DummyScenario::Success).await;
    let full = token("full", 7);
    let first = f.tool("agents_start", start("profile-change"), &full).await;
    let run_id = first["result"]["structuredContent"]["run_id"]
        .as_str()
        .unwrap()
        .to_owned();
    f.wait_terminal(&run_id).await;
    let Fixture {
        _dir,
        path,
        client,
        stop,
        task,
        backend,
        access,
        run_limits,
        transport_limits,
        ..
    } = f;
    stop.cancel();
    task.await.unwrap().unwrap();
    let profiles = vec![
        BackendProfile {
            profile_id: "profile".into(),
            revision: "profile_v2".into(),
            state_realm: "dummy".into(),
            backend: backend.clone(),
        },
        BackendProfile {
            profile_id: "hidden".into(),
            revision: "hidden_v1".into(),
            state_realm: "dummy".into(),
            backend: backend.clone(),
        },
    ];
    let service = RunService::open(&path, access, profiles, run_limits)
        .await
        .unwrap();
    let server = Server::bind("127.0.0.1:0".parse().unwrap(), service, transport_limits)
        .await
        .unwrap();
    let url = format!("http://{}", server.local_addr().unwrap());
    let stop = CancellationToken::new();
    let task = tokio::spawn(server.run(stop.clone()));
    let r = client
        .post(format!("{url}/v1/runs"))
        .bearer_auth(&full)
        .json(&start("profile-change"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let replay = payload(r).await;
    assert_eq!(replay["run_id"], run_id);
    assert_eq!(replay["replay"], true);
    assert_eq!(backend.captured_inputs().len(), 1);
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    drop(_dir);
}

#[tokio::test]
async fn revocation_before_dispatch_blocks_queued_backend_execution() {
    let f = Fixture::new(DummyScenario::DelayedSuccess).await;
    let full = token("full", 7);
    let control = token("control", 11);
    let first = f
        .post("/v1/runs", &start("running-before-revocation"), &full)
        .send()
        .await
        .unwrap();
    let first_id = payload(first).await["run_id"].as_str().unwrap().to_owned();
    tokio::time::timeout(Duration::from_secs(2), async {
        while f.backend.captured_inputs().is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let second = f
        .tool("agents_start", start("queued-before-revocation"), &full)
        .await;
    let second_id = second["result"]["structuredContent"]["run_id"]
        .as_str()
        .unwrap()
        .to_owned();
    f.access.revoke("full").unwrap();
    assert_eq!(
        f.client
            .get(format!("{}/v1/runs/{first_id}", f.url))
            .bearer_auth(&control)
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap()["state"],
        "running"
    );
    let state = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let response = f
                .client
                .get(format!("{}/v1/runs/{second_id}", f.url))
                .bearer_auth(&control)
                .send()
                .await
                .unwrap();
            let value = payload(response).await;
            if value["state"] == "failed" {
                break value;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(state["state"], "failed");
    let first = f
        .client
        .get(format!("{}/v1/runs/{first_id}", f.url))
        .bearer_auth(&control)
        .send()
        .await
        .unwrap();
    assert_eq!(payload(first).await["state"], "succeeded");
    assert_eq!(f.backend.captured_inputs().len(), 1);
    f.finish().await;
}

#[tokio::test]
async fn modern_disconnect_does_not_cancel_an_admitted_run() {
    let f = Fixture::new(DummyScenario::DelayedSuccess).await;
    let full = token("full", 7);
    let address = f.url.strip_prefix("http://").unwrap();
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    let body = rpc(
        "tools/call",
        json!({"name":"agents_start","arguments":start("modern-disconnect")}),
        true,
    )
    .to_string();
    stream.write_all(format!("POST /mcp HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {full}\r\nContent-Type: application/json\r\nAccept: application/json, text/event-stream\r\nMCP-Protocol-Version: {MODERN}\r\nMcp-Method: tools/call\r\nMcp-Name: agents_start\r\nContent-Length: {}\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while f.backend.captured_inputs().is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    drop(stream);
    let r = f
        .post("/v1/runs", &start("modern-disconnect"), &full)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let receipt = payload(r).await;
    assert_eq!(receipt["replay"], true);
    let id = receipt["run_id"].as_str().unwrap();
    let status = f
        .client
        .get(format!("{}/v1/runs/{id}", f.url))
        .bearer_auth(&full)
        .send()
        .await
        .unwrap();
    assert_eq!(payload(status).await["state"], "running");
    f.tool("runs_cancel", json!({"run_id":id}), &full).await;
    f.wait_terminal(id).await;
    assert_eq!(f.backend.captured_inputs().len(), 1);
    f.finish().await;
}

#[tokio::test]
async fn oversized_catalog_response_is_rejected_without_truncating_json() {
    let dir = tempdir().unwrap();
    let namespace = Namespace {
        owner_id: "owner".into(),
        client_id: "client".into(),
    };
    let registration = ClientRegistration::new(
        "full",
        CredentialSecret::new([7; 32]),
        namespace,
        [Scope::RunStart],
        ["profile".into()],
    )
    .with_generation("generation");
    let workspaces = (0..2000)
        .map(|i| format!("workspace_{i:05}_{}", "x".repeat(40)))
        .collect::<Vec<_>>();
    let access = Arc::new(
        ClientAccessControl::new(
            [registration],
            [ExecutionProfile::dummy("profile", workspaces, 1024)],
            AccessLimits {
                max_request_bytes: 4096,
                max_skill_bytes: 1024,
                max_identifier_bytes: 128,
            },
        )
        .unwrap(),
    );
    let backend = Arc::new(DummyBackend::new(DummyScenario::Success, 0));
    let run = RunLimits {
        max_outstanding: 2,
        max_workers: 1,
        database_queue: 4,
        event_buffer: 4,
        max_events_per_run: 4,
        max_event_bytes: 256,
        max_result_bytes: 128,
        max_page_size: 1,
        run_timeout: Duration::from_secs(10),
        cleanup_grace: Duration::from_secs(1),
        poll_interval: Duration::from_millis(10),
    };
    let service = RunService::open(
        dir.path().join("runs.sqlite"),
        access,
        vec![BackendProfile {
            profile_id: "profile".into(),
            revision: "revision".into(),
            state_realm: "dummy".into(),
            backend,
        }],
        run,
    )
    .await
    .unwrap();
    let limits = ServerLimits {
        max_request_bytes: 8192,
        max_response_bytes: 16384,
        max_header_bytes: 8192,
        max_inflight: 4,
        max_connections: 4,
        connection_idle_timeout: Duration::from_secs(4),
        connection_max_lifetime: Duration::from_secs(30),
        requests_per_second: 100,
        burst: 100,
        request_timeout: Duration::from_secs(2),
        shutdown_grace: Duration::from_secs(2),
        max_mcp_sessions: 2,
        mcp_session_idle: Duration::from_secs(10),
    };
    let server = Server::bind("127.0.0.1:0".parse().unwrap(), service, limits)
        .await
        .unwrap();
    let url = format!("http://{}/v1/agents", server.local_addr().unwrap());
    let stop = CancellationToken::new();
    let task = tokio::spawn(server.run(stop.clone()));
    let r = Client::builder()
        .no_proxy()
        .build()
        .unwrap()
        .get(url)
        .bearer_auth(token("full", 7))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 500);
    let response = payload(r).await;
    assert_eq!(response["error"]["code"], "response_limit");
    stop.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn dropping_server_future_releases_the_listener() {
    let f = Fixture::new(DummyScenario::Success).await;
    assert_eq!(
        f.client
            .get(format!("{}/v1/agents", f.url))
            .bearer_auth(token("full", 7))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    let address = f
        .url
        .strip_prefix("http://")
        .unwrap()
        .parse::<std::net::SocketAddr>()
        .unwrap();
    f.task.abort();
    assert!(f.task.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match tokio::net::TcpListener::bind(address).await {
                Ok(listener) => {
                    drop(listener);
                    break;
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
    })
    .await
    .expect("listener remained active after server cancellation");
}

#[tokio::test]
async fn continuously_dripping_headers_cannot_hold_a_connection_forever() {
    let f = Fixture::with_transport(
        DummyScenario::Success,
        Duration::from_millis(100),
        |limits| {
            limits.max_connections = 1;
            limits.connection_idle_timeout = Duration::from_millis(300);
            limits.connection_max_lifetime = Duration::from_millis(800);
        },
    )
    .await;
    let address = f.url.strip_prefix("http://").unwrap();
    let mut slow = tokio::net::TcpStream::connect(address).await.unwrap();
    slow.write_all(format!("GET /v1/agents HTTP/1.1\r\nHost: {address}\r\nX-Drip: ").as_bytes())
        .await
        .unwrap();
    let dripper = tokio::spawn(async move {
        loop {
            if slow.write_all(b"x").await.is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    let request = f
        .client
        .get(format!("{}/v1/agents", f.url))
        .bearer_auth(token("full", 7))
        .send();
    tokio::pin!(request);
    assert!(
        tokio::time::timeout(Duration::from_millis(150), &mut request)
            .await
            .is_err()
    );
    let result = tokio::time::timeout(Duration::from_secs(2), request)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.status(), 200);
    dripper.abort();
    let _ = dripper.await;
    f.finish().await;
}

#[tokio::test]
async fn selected_result_schema_is_pinned_validated_and_shared_across_transports() {
    let f = Fixture::new(DummyScenario::StructuredSuccess).await;
    let full = token("full", 7);
    let mut unsupported = start("unsupported-schema");
    unsupported["result_schema"] = json!({"type":"object","$ref":"https://example.invalid"});
    assert_eq!(
        f.post("/v1/runs", &unsupported, &full)
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    assert!(f.backend.captured_inputs().is_empty());

    let mut request = start("valid-schema");
    request["result_schema"] = json!({"type":"object","required":["message"],"properties":{"message":{"type":"string"}},"additionalProperties":false});
    let response = f.post("/v1/runs", &request, &full).send().await.unwrap();
    assert_eq!(response.status(), 202);
    let receipt = payload(response).await;
    let id = receipt["run_id"].as_str().unwrap();
    assert_eq!(f.wait_terminal(id).await["state"], "succeeded");
    let replay = f.tool("agents_start", request.clone(), &full).await;
    assert_eq!(replay["result"]["structuredContent"]["run_id"], id);
    assert_eq!(replay["result"]["structuredContent"]["replay"], true);
    let result = f.tool("runs_result", json!({"run_id":id}), &full).await;
    let data: Value = serde_json::from_str(
        result["result"]["structuredContent"]["data"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(data["details"]["validation"]["status"], "passed");
    let mut changed = request.clone();
    changed["result_schema"]["required"] = json!(["other"]);
    assert_eq!(
        f.post("/v1/runs", &changed, &full)
            .send()
            .await
            .unwrap()
            .status(),
        409
    );

    changed["idempotency_key"] = json!("invalid-result-schema");
    let receipt = payload(f.post("/v1/runs", &changed, &full).send().await.unwrap()).await;
    let failed = receipt["run_id"].as_str().unwrap();
    assert_eq!(f.wait_terminal(failed).await["state"], "failed");
    let result = f.tool("runs_result", json!({"run_id":failed}), &full).await;
    let data: Value = serde_json::from_str(
        result["result"]["structuredContent"]["data"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(data["details"]["validation"]["reason"], "schema_mismatch");
    let db = rusqlite::Connection::open(&f.path).unwrap();
    let (disposition, reserved): (String, Option<String>) = db
        .query_row(
            "SELECT disposition,reserved_run_id FROM sessions WHERE session_id=?1",
            [receipt["session_id"].as_str().unwrap()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(disposition, "reusable");
    assert_eq!(reserved, None);
    drop(db);
    let mut resumed = start("resume-after-validation-failure");
    resumed["session"] = json!({"mode":"resume","session_id":receipt["session_id"]});
    let response = f.post("/v1/runs", &resumed, &full).send().await.unwrap();
    assert_eq!(response.status(), 202);
    let resumed = payload(response).await;
    assert_eq!(
        f.wait_terminal(resumed["run_id"].as_str().unwrap()).await["state"],
        "succeeded"
    );
    let response = f
        .post(
            "/v1/runs",
            &start("other-after-validation-failure"),
            &token("other", 10),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 202);
    tokio::time::timeout(Duration::from_secs(2), async {
        while f.backend.captured_inputs().len() < 4 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(f.backend.captured_inputs().len(), 4);
    f.finish().await;
}

#[tokio::test]
async fn registered_native_session_resume_is_canonical_and_namespace_scoped() {
    let f = Fixture::new(DummyScenario::Success).await;
    let full = token("full", 7);
    let first = payload(
        f.post("/v1/runs", &start("native-original"), &full)
            .send()
            .await
            .unwrap(),
    )
    .await;
    let run = first["run_id"].as_str().unwrap();
    assert_eq!(f.wait_terminal(run).await["state"], "succeeded");
    let session = first["session_id"].as_str().unwrap();
    let db = rusqlite::Connection::open(&f.path).unwrap();
    let native: String = db
        .query_row(
            "SELECT native_id FROM sessions WHERE session_id=?1",
            [session],
            |row| row.get(0),
        )
        .unwrap();
    drop(db);
    let mut request = start("native-resume");
    request["session"] = json!({"mode":"resume_native","native_id":native});
    assert_eq!(
        f.post("/v1/runs", &request, &token("start", 8))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(
        f.post("/v1/runs", &request, &token("other", 10))
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    for selection in [
        json!({"mode":"resume","session_id":session}),
        request["session"].clone(),
    ] {
        let mut incompatible = start("wrong-session-workspace");
        incompatible["workspace_id"] = json!("workspace-other");
        incompatible["session"] = selection;
        let response = f
            .post("/v1/runs", &incompatible, &full)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 409);
        assert_eq!(
            payload(response).await["error"]["code"],
            "session_incompatible"
        );
    }
    let result = f.tool("agents_start", request.clone(), &full).await;
    assert_eq!(result["result"]["structuredContent"]["session_id"], session);
    let resumed = result["result"]["structuredContent"]["run_id"]
        .as_str()
        .unwrap();
    assert_eq!(f.wait_terminal(resumed).await["state"], "succeeded");
    let replay = f.post("/v1/runs", &request, &full).send().await.unwrap();
    assert_eq!(replay.status(), 200);
    assert_eq!(payload(replay).await["run_id"], resumed);
    assert_eq!(f.backend.captured_inputs().len(), 2);
    f.finish().await;
}

#[tokio::test]
async fn busy_native_and_server_session_references_do_not_launch_another_run() {
    let f = Fixture::new(DummyScenario::DelayedSuccess).await;
    let full = token("full", 7);
    let first = payload(
        f.post("/v1/runs", &start("native-busy"), &full)
            .send()
            .await
            .unwrap(),
    )
    .await;
    let session = first["session_id"].as_str().unwrap();
    let native = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let db = rusqlite::Connection::open(&f.path).unwrap();
            let native: Option<String> = db
                .query_row(
                    "SELECT native_id FROM sessions WHERE session_id=?1",
                    [session],
                    |row| row.get(0),
                )
                .unwrap();
            if let Some(native) = native {
                break native;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    for selection in [
        json!({"mode":"resume","session_id":session}),
        json!({"mode":"resume_native","native_id":native}),
    ] {
        let mut request = start("busy-resume");
        request["session"] = selection;
        assert_eq!(
            f.post("/v1/runs", &request, &full)
                .send()
                .await
                .unwrap()
                .status(),
            409
        );
    }
    assert_eq!(f.backend.captured_inputs().len(), 1);
    f.finish().await;
}

#[tokio::test]
async fn synthetic_warnings_coverage_and_incomplete_output_remain_distinct() {
    for (scenario, expected_state, expected_coverage, expected_codes) in [
        (
            DummyScenario::MixedWarnings,
            "succeeded",
            "complete",
            vec!["permission_denied", "question_cancelled", "tool_error"],
        ),
        (
            DummyScenario::PartialWarnings,
            "succeeded",
            "partial",
            vec!["dummy_warning"],
        ),
        (DummyScenario::Incomplete, "failed", "complete", vec![]),
        (DummyScenario::Failure, "failed", "complete", vec![]),
    ] {
        let f = Fixture::new(scenario).await;
        let full = token("full", 7);
        let receipt = payload(
            f.post("/v1/runs", &start("warning-coverage"), &full)
                .send()
                .await
                .unwrap(),
        )
        .await;
        let run = receipt["run_id"].as_str().unwrap();
        let status = f.wait_terminal(run).await;
        assert_eq!(status["state"], expected_state);
        assert_eq!(status["metadata"]["synthetic"], true);
        let result = f.tool("runs_result", json!({"run_id":run}), &full).await;
        let data: Value = serde_json::from_str(
            result["result"]["structuredContent"]["data"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(data["details"]["warning_observation"], expected_coverage);
        let codes: Vec<&str> = data["details"]["warnings"]
            .as_array()
            .unwrap()
            .iter()
            .map(|warning| warning["code"].as_str().unwrap())
            .collect();
        assert_eq!(codes, expected_codes);
        if scenario == DummyScenario::Incomplete {
            assert!(
                data["details"]["outcome"]["error"]
                    .as_str()
                    .unwrap()
                    .contains("incomplete_result")
            );
        }
        if scenario == DummyScenario::Failure {
            assert!(
                data["details"]["outcome"]["error"]
                    .as_str()
                    .unwrap()
                    .contains("scripted_failure")
            );
        }
        let events = f
            .tool("runs_events", json!({"run_id":run,"limit":16}), &full)
            .await;
        let events = events["result"]["structuredContent"]["events"]
            .as_array()
            .unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| event["kind"] == "run.warning")
                .count(),
            expected_codes.len()
        );
        assert!(
            events
                .iter()
                .all(|event| event["metadata"]["synthetic"] == true)
        );
        f.finish().await;
    }
}

#[tokio::test]
async fn selected_schema_with_insufficient_result_budget_is_rejected_before_execution() {
    let f = Fixture::with_limits(
        DummyScenario::Success,
        Duration::from_secs(3),
        |limits| limits.max_result_bytes = 128,
        |_| {},
    )
    .await;
    let mut request = start("small-validation-budget");
    request["result_schema"] = json!({"type":"object"});
    let response = f
        .post("/v1/runs", &request, &token("full", 7))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
    assert!(f.backend.captured_inputs().is_empty());
    f.finish().await;
}

#[tokio::test]
async fn invalid_json_result_is_not_a_success_when_schema_is_selected() {
    let f = Fixture::new(DummyScenario::Success).await;
    let full = token("full", 7);
    let mut request = start("invalid-json");
    request["result_schema"] = json!({"type":"object"});
    let receipt = payload(f.post("/v1/runs", &request, &full).send().await.unwrap()).await;
    let id = receipt["run_id"].as_str().unwrap();
    assert_eq!(f.wait_terminal(id).await["state"], "failed");
    let result = f.tool("runs_result", json!({"run_id":id}), &full).await;
    let data: Value = serde_json::from_str(
        result["result"]["structuredContent"]["data"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        data["details"]["validation"]["reason"],
        "invalid_or_unsupported_json"
    );
    assert_eq!(
        data["details"]["outcome"]["output"],
        "Scripted dummy output"
    );
    f.finish().await;
}

#[tokio::test]
async fn retention_storage_failure_stops_the_server_without_losing_the_result() {
    let mut f = Fixture::with_retention(
        DummyScenario::Success,
        Duration::from_secs(3),
        |_| {},
        |_| {},
        Some(RetentionConfig {
            result_event_retention_ms: 60_000,
        }),
    )
    .await;
    let full = token("full", 7);
    let receipt = payload(
        f.post("/v1/runs", &start("retention-maintenance-failure"), &full)
            .send()
            .await
            .unwrap(),
    )
    .await;
    let run = receipt["run_id"].as_str().unwrap();
    assert_eq!(f.wait_terminal(run).await["state"], "succeeded");
    let db = rusqlite::Connection::open(&f.path).unwrap();
    db.execute_batch("BEGIN IMMEDIATE; CREATE TRIGGER fail_retention_delete BEFORE DELETE ON results BEGIN SELECT RAISE(ABORT, 'injected retention failure'); END;").unwrap();
    db.execute(
        "UPDATE run_retention SET expires_ms=1 WHERE run_id=?1",
        [run],
    )
    .unwrap();
    db.execute_batch("COMMIT").unwrap();
    drop(db);
    let stopped = tokio::time::timeout(Duration::from_secs(5), &mut f.task)
        .await
        .unwrap()
        .unwrap();
    assert!(stopped.is_err());
    let db = rusqlite::Connection::open(&f.path).unwrap();
    let (results,purged): (i64,bool) = db.query_row("SELECT (SELECT COUNT(*) FROM results WHERE run_id=?1),purged FROM run_retention WHERE run_id=?1", [run], |row| Ok((row.get(0)?,row.get(1)?))).unwrap();
    assert_eq!((results, purged), (1, false));
    assert!(
        f.client
            .post(format!("{}/v1/runs", f.url))
            .bearer_auth(&full)
            .json(&start("after-retention-failure"))
            .send()
            .await
            .is_err()
    );
}

#[tokio::test]
async fn expired_result_and_events_keep_cross_transport_replay_and_namespace_isolation() {
    let f = Fixture::with_retention(
        DummyScenario::Success,
        Duration::from_secs(3),
        |_| {},
        |_| {},
        Some(RetentionConfig {
            result_event_retention_ms: 60_000,
        }),
    )
    .await;
    let full = token("full", 7);
    let request = start("expiring-result");
    let response = f.post("/v1/runs", &request, &full).send().await.unwrap();
    assert_eq!(response.status(), 202);
    let receipt = payload(response).await;
    let id = receipt["run_id"].as_str().unwrap();
    assert_eq!(f.wait_terminal(id).await["state"], "succeeded");
    let db = rusqlite::Connection::open(&f.path).unwrap();
    let pinned: i64 = db
        .query_row(
            "SELECT retention_ms FROM run_retention WHERE run_id=?1",
            [id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(pinned, 60_000);
    db.execute(
        "UPDATE run_retention SET expires_ms=1 WHERE run_id=?1",
        [id],
    )
    .unwrap();
    drop(db);
    let result = f.tool("runs_result", json!({"run_id":id}), &full).await;
    assert_eq!(
        result["result"]["structuredContent"]["availability"],
        "expired"
    );
    assert_eq!(result["result"]["structuredContent"]["state"], "succeeded");
    assert_eq!(
        result["result"]["structuredContent"]["metadata"]["synthetic"],
        true
    );
    let rest_result = f
        .client
        .get(format!("{}/v1/runs/{id}/result", f.url))
        .bearer_auth(&full)
        .send()
        .await
        .unwrap();
    assert_eq!(rest_result.status(), 200);
    assert_eq!(payload(rest_result).await["availability"], "expired");
    let events = f
        .tool("runs_events", json!({"run_id":id,"limit":1}), &full)
        .await;
    assert_eq!(events["result"]["isError"], true);
    assert_eq!(
        events["result"]["structuredContent"]["error"]["code"],
        "expired"
    );
    assert_eq!(
        f.client
            .get(format!("{}/v1/runs/{id}/events?limit=1", f.url))
            .bearer_auth(&full)
            .send()
            .await
            .unwrap()
            .status(),
        410
    );
    let replay = f.tool("agents_start", request.clone(), &full).await;
    assert_eq!(replay["result"]["structuredContent"]["run_id"], id);
    assert_eq!(replay["result"]["structuredContent"]["replay"], true);
    let mut different = request;
    different["input"]["request"] = json!("different");
    assert_eq!(
        f.post("/v1/runs", &different, &full)
            .send()
            .await
            .unwrap()
            .status(),
        409
    );
    assert_eq!(
        f.client
            .get(format!("{}/v1/runs/{id}/result", f.url))
            .bearer_auth(token("other", 10))
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    assert_eq!(
        f.client
            .get(format!("{}/v1/runs/{id}/result", f.url))
            .bearer_auth(token("start", 8))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(f.backend.captured_inputs().len(), 1);
    let mut resume = start("resume-after-result-expiry");
    resume["session"] = json!({"mode":"resume","session_id":receipt["session_id"]});
    let response = f.post("/v1/runs", &resume, &full).send().await.unwrap();
    assert_eq!(response.status(), 202);
    let resumed = payload(response).await;
    assert_eq!(
        f.wait_terminal(resumed["run_id"].as_str().unwrap()).await["state"],
        "succeeded"
    );
    assert_eq!(f.backend.captured_inputs().len(), 2);
    f.finish().await;
}
