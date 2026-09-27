use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use agent_runner_server::{
    access::{
        AccessLimits, ClientAccessControl, ClientRegistration, CredentialSecret, ExecutionProfile,
        Scope,
    },
    application::{BackendProfile, RunLimits, RunService},
    core::CancellationToken,
    delivery::{DeliveryConfig, Destination, DestinationKind},
    runtimes::dummy::{DummyBackend, DummyScenario},
    server::{Server, ServerLimits},
    storage::Namespace,
};
use axum::{Router, body::Bytes, extract::State, http::HeaderMap, routing::post};
use reqwest::Client;
use serde_json::{Value, json};
use tempfile::{TempDir, tempdir};
use tokio::task::JoinHandle;

async fn rpc_body(response: reqwest::Response) -> Value {
    let text = response.text().await.unwrap();
    let body = if text.starts_with('{') {
        text.as_str()
    } else {
        text.lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .find(|line| line.starts_with('{'))
            .unwrap()
    };
    serde_json::from_str(body).unwrap()
}

async fn receive(State(received): State<Arc<Mutex<Vec<Value>>>>, body: Bytes) -> &'static str {
    received
        .lock()
        .unwrap()
        .push(serde_json::from_slice(&body).unwrap());
    "ok"
}

async fn receive_notion(
    State(received): State<Arc<Mutex<Vec<Value>>>>,
    headers: HeaderMap,
    body: Bytes,
) -> &'static str {
    let version = headers
        .get("notion-version")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    received
        .lock()
        .unwrap()
        .push(json!({"version":version,"notion":serde_json::from_slice::<Value>(&body).unwrap()}));
    "ok"
}

async fn receive_slow(State(received): State<Arc<Mutex<Vec<Value>>>>, body: Bytes) -> &'static str {
    received
        .lock()
        .unwrap()
        .push(serde_json::from_slice(&body).unwrap());
    tokio::time::sleep(Duration::from_millis(800)).await;
    "ok"
}

struct Fixture {
    _temp: TempDir,
    path: std::path::PathBuf,
    access: Arc<ClientAccessControl>,
    runs: RunLimits,
    transport: ServerLimits,
    url: String,
    client: Client,
    stop: CancellationToken,
    task: JoinHandle<Result<(), agent_runner_server::server::ServerError>>,
    receiver: JoinHandle<()>,
    received: Arc<Mutex<Vec<Value>>>,
}

impl Fixture {
    async fn new() -> Self {
        Self::with_settings(128, DummyScenario::Success).await
    }

    async fn with_identifier_limit(identifier_limit: usize) -> Self {
        Self::with_settings(identifier_limit, DummyScenario::Success).await
    }

    async fn with_settings(identifier_limit: usize, scenario: DummyScenario) -> Self {
        Self::with_budget(identifier_limit, scenario, false).await
    }

    async fn with_budget(identifier_limit: usize, scenario: DummyScenario, small: bool) -> Self {
        let received = Arc::new(Mutex::new(Vec::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let receiver_address = listener.local_addr().unwrap();
        let router = Router::new()
            .route("/hook", post(receive))
            .route("/slow", post(receive_slow))
            .route("/v1/pages", post(receive_notion))
            .with_state(Arc::clone(&received));
        let receiver = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let temp = tempdir().unwrap();
        let namespace = Namespace {
            owner_id: "owner".into(),
            client_id: "client".into(),
        };
        let full = [
            Scope::RunStart,
            Scope::RunRead,
            Scope::RunCancel,
            Scope::SessionRead,
            Scope::HookRead,
            Scope::HookWrite,
            Scope::InboxRead,
            Scope::InboxAck,
            Scope::DeliveryReconcile,
        ];
        let access = Arc::new(
            ClientAccessControl::new(
                [
                    ClientRegistration::new(
                        "full",
                        CredentialSecret::new([7; 32]),
                        namespace.clone(),
                        full,
                        ["profile".into()],
                    )
                    .with_generation("generation")
                    .with_destinations([
                        "inbox".into(),
                        "webhook".into(),
                        "notion".into(),
                        "slow".into(),
                    ])
                    .with_reconciliation_destinations(["slow".into()]),
                    ClientRegistration::new(
                        "control",
                        CredentialSecret::new([10; 32]),
                        namespace.clone(),
                        full,
                        ["profile".into()],
                    )
                    .with_generation("control")
                    .with_destinations([
                        "inbox".into(),
                        "webhook".into(),
                        "notion".into(),
                        "slow".into(),
                    ]),
                    ClientRegistration::new(
                        "start",
                        CredentialSecret::new([8; 32]),
                        namespace.clone(),
                        [Scope::RunStart],
                        ["profile".into()],
                    )
                    .with_generation("start"),
                    ClientRegistration::new(
                        "other",
                        CredentialSecret::new([9; 32]),
                        Namespace {
                            owner_id: "owner".into(),
                            client_id: "other".into(),
                        },
                        full,
                        ["profile".into()],
                    )
                    .with_generation("other")
                    .with_destinations([
                        "inbox".into(),
                        "webhook".into(),
                        "notion".into(),
                        "slow".into(),
                    ])
                    .with_reconciliation_destinations(["slow".into()]),
                ],
                [ExecutionProfile::dummy(
                    "profile",
                    ["workspace".into()],
                    4096,
                )],
                AccessLimits {
                    max_request_bytes: 8192,
                    max_skill_bytes: 1024,
                    max_identifier_bytes: identifier_limit,
                },
            )
            .unwrap(),
        );
        let backend = Arc::new(DummyBackend::new(scenario, 64));
        let profiles = vec![BackendProfile {
            profile_id: "profile".into(),
            revision: "v1".into(),
            state_realm: "dummy".into(),
            backend,
        }];
        let runs = RunLimits {
            max_outstanding: 8,
            max_workers: 1,
            database_queue: 16,
            event_buffer: 16,
            max_events_per_run: 32,
            max_event_bytes: if small { 256 } else { 2048 },
            max_result_bytes: if small { 1024 } else { 4096 },
            max_page_size: if small { 1 } else { 16 },
            run_timeout: Duration::from_secs(10),
            cleanup_grace: Duration::from_secs(1),
            poll_interval: Duration::from_millis(5),
        };
        let destinations = vec![
            Destination {
                id: "inbox".into(),
                revision: "v1".into(),
                kind: DestinationKind::Inbox,
                endpoint: None,
                notion_parent: None,
                deduplication: false,
                enabled: true,
            },
            Destination {
                id: "webhook".into(),
                revision: "v1".into(),
                kind: DestinationKind::WebhookTest,
                endpoint: Some(format!("http://{receiver_address}/hook")),
                notion_parent: None,
                deduplication: false,
                enabled: true,
            },
            Destination {
                id: "notion".into(),
                revision: "v1".into(),
                kind: DestinationKind::NotionMock,
                endpoint: Some(format!("http://{receiver_address}/v1/pages")),
                notion_parent: Some("test-parent".into()),
                deduplication: false,
                enabled: true,
            },
            Destination {
                id: "slow".into(),
                revision: "v1".into(),
                kind: DestinationKind::WebhookTest,
                endpoint: Some(format!("http://{receiver_address}/slow")),
                notion_parent: None,
                deduplication: false,
                enabled: true,
            },
        ];
        let delivery = DeliveryConfig {
            max_pending: 8,
            max_payload_bytes: if small { 11264 } else { 32768 },
            max_workers: 1,
            max_attempts: 2,
            delivery_ttl_ms: 10000,
            retention_ms: 10000,
            lease_ms: 1000,
            timeout_ms: 500,
            retry_ms: 20,
            poll_ms: 5,
            destinations,
        };
        let path = temp.path().join("runs.sqlite");
        let service = RunService::open_with_delivery(
            &path,
            Arc::clone(&access),
            profiles,
            runs.clone(),
            Some(delivery),
        )
        .await
        .unwrap();
        let limits = ServerLimits {
            max_request_bytes: 16384,
            max_response_bytes: if small { 188416 } else { 1048576 },
            max_header_bytes: 8192,
            max_inflight: 16,
            max_connections: 16,
            connection_idle_timeout: Duration::from_secs(6),
            connection_max_lifetime: Duration::from_secs(30),
            requests_per_second: 10000,
            burst: 1000,
            request_timeout: Duration::from_secs(3),
            shutdown_grace: Duration::from_secs(3),
            max_mcp_sessions: 16,
            mcp_session_idle: Duration::from_secs(30),
        };
        let server = Server::bind("127.0.0.1:0".parse().unwrap(), service, limits.clone())
            .await
            .unwrap();
        let url = format!("http://{}", server.local_addr().unwrap());
        let stop = CancellationToken::new();
        let task = tokio::spawn(server.run(stop.clone()));
        let client = Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(3))
            .build()
            .unwrap();
        Self {
            _temp: temp,
            path,
            access,
            runs,
            transport: limits,
            url,
            client,
            stop,
            task,
            receiver,
            received,
        }
    }

    async fn post(&self, path: &str, client: &str, secret: u8, body: Value) -> (u16, Value) {
        let response = self
            .client
            .post(format!("{}{path}", self.url))
            .bearer_auth(format!("{client}.{}", format!("{secret:02x}").repeat(32)))
            .json(&body)
            .send()
            .await
            .unwrap();
        let status = response.status().as_u16();
        (status, response.json().await.unwrap())
    }

    async fn mcp(
        &self,
        method: &str,
        name: Option<&str>,
        arguments: Value,
        client: &str,
        secret: u8,
    ) -> Value {
        let mut params = if let Some(name) = name {
            json!({"name":name,"arguments":arguments})
        } else {
            json!({})
        };
        params["_meta"] = json!({"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}});
        let mut request = self
            .client
            .post(format!("{}/mcp", self.url))
            .bearer_auth(format!("{client}.{}", format!("{secret:02x}").repeat(32)))
            .header("accept", "application/json, text/event-stream")
            .header("mcp-protocol-version", "2026-07-28")
            .header("mcp-method", method)
            .json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}));
        if let Some(name) = name {
            request = request.header("mcp-name", name);
        }
        let response = request.send().await.unwrap();
        assert_eq!(response.status(), 200);
        let text = response.text().await.unwrap();
        let json = if text.starts_with('{') {
            text.as_str()
        } else {
            text.lines()
                .filter_map(|line| line.strip_prefix("data: "))
                .find(|line| line.starts_with('{'))
                .unwrap()
        };
        serde_json::from_str(json).unwrap()
    }

    async fn restart_without_delivery(mut self) -> Self {
        self.stop.cancel();
        (&mut self.task).await.unwrap().unwrap();
        let namespace = Namespace {
            owner_id: "owner".into(),
            client_id: "client".into(),
        };
        self.access = Arc::new(
            ClientAccessControl::new(
                [ClientRegistration::new(
                    "full",
                    CredentialSecret::new([7; 32]),
                    namespace,
                    [Scope::RunRead, Scope::HookRead, Scope::DeliveryReconcile],
                    [],
                )
                .with_generation("reconcile-generation")
                .with_reconciliation_destinations(["slow".into()])],
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
        );
        let profiles = vec![BackendProfile {
            profile_id: "profile".into(),
            revision: "v1".into(),
            state_realm: "dummy".into(),
            backend: Arc::new(DummyBackend::new(DummyScenario::Success, 64)),
        }];
        let service = RunService::open(
            &self.path,
            Arc::clone(&self.access),
            profiles,
            self.runs.clone(),
        )
        .await
        .unwrap();
        let server = Server::bind(
            "127.0.0.1:0".parse().unwrap(),
            service,
            self.transport.clone(),
        )
        .await
        .unwrap();
        self.url = format!("http://{}", server.local_addr().unwrap());
        self.stop = CancellationToken::new();
        self.task = tokio::spawn(server.run(self.stop.clone()));
        self
    }

    async fn shutdown(self) {
        self.stop.cancel();
        self.task.await.unwrap().unwrap();
        self.receiver.abort();
    }
}

#[tokio::test]
async fn hooks_inbox_and_webhook_share_the_durable_completion() {
    let f = Fixture::new().await;
    for (hook, destination) in [("notify", "webhook"), ("inbox-hook", "inbox")] {
        let (status,body)=f.post("/v1/hooks","full",7,json!({"hook_id":hook,"version":"1","destination_id":destination,"events":["succeeded","cancelled"]})).await;
        assert_eq!(status, 200, "{body}");
    }
    let (_,body)=f.post("/v1/hook-defaults","full",7,json!({"hooks":[{"hook_id":"notify","version":"1"},{"hook_id":"inbox-hook","version":"1"}]})).await;
    assert_eq!(body["hooks"].as_array().unwrap().len(), 2);
    let request = json!({"profile_id":"profile","workspace_id":"workspace","input":{"request":"not included in notifications"},"idempotency_key":"job"});
    let (status, receipt) = f.post("/v1/runs", "full", 7, request.clone()).await;
    assert_eq!(status, 202, "{receipt}");
    let run = receipt["run_id"].as_str().unwrap();
    let _ = f
        .post("/v1/hook-defaults", "full", 7, json!({"hooks":[]}))
        .await;
    let (status, replay) = f.post("/v1/runs", "full", 7, request).await;
    assert_eq!(status, 200);
    assert_eq!(replay["run_id"], run);
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let (_, page) = f
                .post(
                    "/v1/deliveries/list",
                    "full",
                    7,
                    json!({"run_id":run,"limit":10}),
                )
                .await;
            if page["items"][0]["state"] == "succeeded" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let (status, inbox) = f
        .post("/v1/inbox/list", "full", 7, json!({"limit":10}))
        .await;
    assert_eq!(status, 200, "{inbox}");
    let item = &inbox["items"][0];
    assert_eq!(item["notification"]["run_id"], run);
    assert_eq!(item["notification"]["synthetic"], true);
    assert!(
        item.to_string()
            .find("not included in notifications")
            .is_none()
    );
    let inbox_id = item["inbox_id"].as_str().unwrap();
    for _ in 0..2 {
        let (status, ack) = f
            .post("/v1/inbox/ack", "full", 7, json!({"inbox_id":inbox_id}))
            .await;
        assert_eq!(status, 200, "{ack}");
        assert_eq!(ack, json!({"acknowledged":true}));
    }
    let (_, other) = f
        .post("/v1/inbox/list", "other", 9, json!({"limit":10}))
        .await;
    assert_eq!(other["items"].as_array().unwrap().len(), 0);
    let (status, _) = f
        .post("/v1/inbox/ack", "other", 9, json!({"inbox_id":inbox_id}))
        .await;
    assert_eq!(status, 404);
    let (status, _) = f
        .post("/v1/inbox/list", "start", 8, json!({"limit":10}))
        .await;
    assert_eq!(status, 403);
    let seen = f.received.lock().unwrap().clone();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0]["state"], "succeeded");
    assert!(
        seen[0]
            .to_string()
            .find("not included in notifications")
            .is_none()
    );
    f.shutdown().await;
}

#[tokio::test]
async fn hook_selection_rejects_ungranted_defaults_null_duplicates_and_urls() {
    let f = Fixture::new().await;
    let (status, body) = f
        .post(
            "/v1/hooks",
            "full",
            7,
            json!({"hook_id":"safe","version":"1","destination_id":"inbox","events":["succeeded"]}),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    let (status,_)=f.post("/v1/hooks","full",7,json!({"hook_id":"bad","version":"1","destination_id":"inbox","events":["succeeded"],"url":"http://example.invalid/"})).await;
    assert_eq!(status, 400);
    f.post(
        "/v1/hook-defaults",
        "full",
        7,
        json!({"hooks":[{"hook_id":"safe","version":"1"}]}),
    )
    .await;
    let mut body = json!({"profile_id":"profile","workspace_id":"workspace","input":{"request":"test"},"idempotency_key":"selection"});
    let (status, _) = f.post("/v1/runs", "start", 8, body.clone()).await;
    assert_eq!(status, 403);
    body["hooks"] = Value::Null;
    let (status, _) = f.post("/v1/runs", "full", 7, body.clone()).await;
    assert_eq!(status, 400);
    body["hooks"] = json!([{"hook_id":"safe","version":"1"},{"hook_id":"safe","version":"1"}]);
    let (status, _) = f.post("/v1/runs", "full", 7, body.clone()).await;
    assert_eq!(status, 400);
    body["hooks"] = json!([]);
    let (status, receipt) = f.post("/v1/runs", "start", 8, body).await;
    assert_eq!(status, 202, "{receipt}");
    f.shutdown().await;
}

#[tokio::test]
async fn mcp_hooks_and_rest_share_binding_and_scope_filtering() {
    let f = Fixture::new().await;
    let listing = f.mcp("tools/list", None, json!({}), "full", 7).await;
    let tools = listing["result"]["tools"].as_array().unwrap();
    assert!(tools.iter().any(|tool| tool["name"] == "hooks_register"));
    assert!(tools.iter().any(|tool| tool["name"] == "inbox_ack"));
    let restricted = f.mcp("tools/list", None, json!({}), "start", 8).await;
    assert!(
        !restricted["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .any(|tool| tool["name"] == "hooks_register" || tool["name"] == "inbox_list")
    );
    let created=f.mcp("tools/call",Some("hooks_register"),json!({"hook_id":"mcp-hook","version":"1","destination_id":"inbox","events":["succeeded"]}),"full",7).await;
    assert_eq!(created["result"]["structuredContent"]["created"], true);
    let request = json!({"profile_id":"profile","workspace_id":"workspace","input":{"request":"mcp"},"idempotency_key":"mcp-run","hooks":[{"hook_id":"mcp-hook","version":"1"}]});
    let (status, receipt) = f.post("/v1/runs", "full", 7, request.clone()).await;
    assert_eq!(status, 202);
    let replay = f
        .mcp("tools/call", Some("agents_start"), request, "full", 7)
        .await;
    assert_eq!(
        replay["result"]["structuredContent"]["run_id"],
        receipt["run_id"]
    );
    assert_eq!(replay["result"]["structuredContent"]["replay"], true);
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let inbox = f
                .mcp(
                    "tools/call",
                    Some("inbox_list"),
                    json!({"limit":5}),
                    "full",
                    7,
                )
                .await;
            if inbox["result"]["structuredContent"]["items"]
                .as_array()
                .is_some_and(|items| !items.is_empty())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    f.shutdown().await;
}

#[tokio::test]
async fn legacy_mcp_revisions_can_register_and_read_completion_hooks() {
    let f = Fixture::new().await;
    let token = format!("full.{}", "07".repeat(32));
    for version in ["2025-11-25", "2025-06-18"] {
        let response=f.client.post(format!("{}/mcp",f.url)).bearer_auth(&token).header("accept","application/json, text/event-stream")
            .json(&json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":version,"capabilities":{},"clientInfo":{"name":"fixture","version":"1"}}})).send().await.unwrap();
        assert_eq!(response.status(), 200);
        let session = response.headers()["mcp-session-id"]
            .to_str()
            .unwrap()
            .to_owned();
        let init = rpc_body(response).await;
        assert_eq!(init["result"]["protocolVersion"], version);
        let response = f
            .client
            .post(format!("{}/mcp", f.url))
            .bearer_auth(&token)
            .header("accept", "application/json, text/event-stream")
            .header("mcp-protocol-version", version)
            .header("mcp-session-id", &session)
            .json(&json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 202);
        let request = json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"hooks_register","arguments":{"hook_id":"legacy-hook","version":version,"destination_id":"inbox","events":["succeeded"]}}});
        let response = f
            .client
            .post(format!("{}/mcp", f.url))
            .bearer_auth(&token)
            .header("accept", "application/json, text/event-stream")
            .header("mcp-protocol-version", version)
            .header("mcp-session-id", &session)
            .json(&request)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let result = rpc_body(response).await;
        assert_eq!(result["result"]["structuredContent"]["created"], true);
        assert!(result["result"].get("resultType").is_none());
        let request = json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"hooks_get","arguments":{"hook_id":"legacy-hook","version":version}}});
        let response = f
            .client
            .post(format!("{}/mcp", f.url))
            .bearer_auth(&token)
            .header("accept", "application/json, text/event-stream")
            .header("mcp-protocol-version", version)
            .header("mcp-session-id", &session)
            .json(&request)
            .send()
            .await
            .unwrap();
        let result = rpc_body(response).await;
        assert_eq!(
            result["result"]["structuredContent"]["destination_id"],
            "inbox"
        );
    }
    f.shutdown().await;
}

#[tokio::test]
async fn smallest_delivery_response_budget_can_list_and_ack_one_inbox_item() {
    let f = Fixture::with_budget(128, DummyScenario::Success, true).await;
    let (status, body) = f
        .post(
            "/v1/hooks",
            "full",
            7,
            json!({"hook_id":"tiny","version":"1","destination_id":"inbox","events":["succeeded"]}),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    let (status,receipt)=f.post("/v1/runs","full",7,json!({"profile_id":"profile","workspace_id":"workspace","input":{"request":"test"},"idempotency_key":"tiny","hooks":[{"hook_id":"tiny","version":"1"}]})).await;
    assert_eq!(status, 202, "{receipt}");
    let inbox = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let (status, page) = f
                .post("/v1/inbox/list", "full", 7, json!({"limit":1}))
                .await;
            assert_eq!(status, 200, "{page}");
            if page["items"]
                .as_array()
                .is_some_and(|items| !items.is_empty())
            {
                break page;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let (status, body) = f
        .post(
            "/v1/inbox/ack",
            "full",
            7,
            json!({"inbox_id":inbox["items"][0]["inbox_id"]}),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    f.shutdown().await;
}

#[tokio::test]
async fn delivery_cursor_works_with_the_minimum_identifier_limit() {
    let f = Fixture::with_identifier_limit(40).await;
    for hook in ["a", "b"] {
        let (status,body)=f.post("/v1/hooks","full",7,json!({"hook_id":hook,"version":"1","destination_id":"webhook","events":["succeeded"]})).await;
        assert_eq!(status, 200, "{body}");
    }
    let (status,receipt)=f.post("/v1/runs","full",7,json!({"profile_id":"profile","workspace_id":"workspace","input":{"request":"test"},"idempotency_key":"cursor","hooks":[{"hook_id":"a","version":"1"},{"hook_id":"b","version":"1"}]})).await;
    assert_eq!(status, 202, "{receipt}");
    let run = receipt["run_id"].as_str().unwrap();
    let first = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let (_, page) = f
                .post(
                    "/v1/deliveries/list",
                    "full",
                    7,
                    json!({"run_id":run,"limit":1}),
                )
                .await;
            if page["items"]
                .as_array()
                .is_some_and(|items| !items.is_empty())
            {
                break page;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let cursor = first["next_after"].as_str().unwrap();
    assert!(cursor.len() <= 40);
    let (status, next) = f
        .post(
            "/v1/deliveries/list",
            "full",
            7,
            json!({"run_id":run,"limit":1,"after":cursor}),
        )
        .await;
    assert_eq!(status, 200, "{next}");
    assert_eq!(next["items"].as_array().unwrap().len(), 1);
    assert_ne!(
        first["items"][0]["delivery_id"],
        next["items"][0]["delivery_id"]
    );
    f.shutdown().await;
}

#[tokio::test]
async fn notion_mock_uses_create_page_mapping_without_live_credentials() {
    let f = Fixture::new().await;
    let (status,body)=f.post("/v1/hooks","full",7,json!({"hook_id":"notion-hook","version":"1","destination_id":"notion","events":["succeeded"]})).await;
    assert_eq!(status, 200, "{body}");
    let (status,receipt)=f.post("/v1/runs","full",7,json!({"profile_id":"profile","workspace_id":"workspace","input":{"request":"private prompt"},"idempotency_key":"notion-run","hooks":[{"hook_id":"notion-hook","version":"1"}]})).await;
    assert_eq!(status, 202, "{receipt}");
    let run = receipt["run_id"].as_str().unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let (_, page) = f
                .post(
                    "/v1/deliveries/list",
                    "full",
                    7,
                    json!({"run_id":run,"limit":5}),
                )
                .await;
            if page["items"][0]["state"] == "succeeded" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let seen = f.received.lock().unwrap().clone();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0]["version"], "2026-03-11");
    assert_eq!(seen[0]["notion"]["parent"]["page_id"], "test-parent");
    assert_eq!(
        seen[0]["notion"]["properties"]["title"]["title"][0]["text"]["content"],
        format!("Run {run}: succeeded")
    );
    assert!(!seen[0].to_string().contains("private prompt"));
    f.shutdown().await;
}

#[tokio::test]
async fn revoking_original_credential_blocks_pending_inbox_visibility() {
    let f = Fixture::with_settings(128, DummyScenario::DelayedSuccess).await;
    let (status,body)=f.post("/v1/hooks","full",7,json!({"hook_id":"revoke-hook","version":"1","destination_id":"inbox","events":["cancelled"]})).await;
    assert_eq!(status, 200, "{body}");
    let (status,receipt)=f.post("/v1/runs","full",7,json!({"profile_id":"profile","workspace_id":"workspace","input":{"request":"private"},"idempotency_key":"revoked-inbox","hooks":[{"hook_id":"revoke-hook","version":"1"}]})).await;
    assert_eq!(status, 202, "{receipt}");
    let run = receipt["run_id"].as_str().unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let state = f
                .client
                .get(format!("{}/v1/runs/{run}", f.url))
                .bearer_auth(format!("control.{}", "0a".repeat(32)))
                .send()
                .await
                .unwrap()
                .json::<Value>()
                .await
                .unwrap();
            if state["state"] == "running" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(f.access.revoke("full").unwrap());
    let (status, body) = f
        .post(&format!("/v1/runs/{run}/cancel"), "control", 10, json!({}))
        .await;
    assert_eq!(status, 200, "{body}");
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let conn = rusqlite::Connection::open_with_flags(
                &f.path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )
            .unwrap();
            let state: Result<(String,Option<String>), _> = conn.query_row(
                "SELECT status,reason FROM completion_inbox WHERE run_id=?1",
                [run],
                |r| Ok((r.get(0)?,r.get(1)?)),
            );
            if matches!(state,Ok((ref status,Some(ref reason))) if status=="skipped" && reason=="authorization_revoked") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let (_, inbox) = f
        .post("/v1/inbox/list", "control", 10, json!({"limit":10}))
        .await;
    assert_eq!(inbox["items"].as_array().unwrap().len(), 0);
    f.shutdown().await;
}

#[tokio::test]
async fn lost_webhook_response_is_uncertain_and_does_not_rerun_the_task() {
    let f = Fixture::new().await;
    let (status,body)=f.post("/v1/hooks","full",7,json!({"hook_id":"slow-hook","version":"1","destination_id":"slow","events":["succeeded"]})).await;
    assert_eq!(status, 200, "{body}");
    let (status,receipt)=f.post("/v1/runs","full",7,json!({"profile_id":"profile","workspace_id":"workspace","input":{"request":"private"},"idempotency_key":"slow-run","hooks":[{"hook_id":"slow-hook","version":"1"}]})).await;
    assert_eq!(status, 202, "{receipt}");
    let run = receipt["run_id"].as_str().unwrap();
    let delivery = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let (_, page) = f
                .post(
                    "/v1/deliveries/list",
                    "full",
                    7,
                    json!({"run_id":run,"limit":5}),
                )
                .await;
            if page["items"][0]["state"] == "uncertain" {
                break page["items"][0]["delivery_id"].as_str().unwrap().to_owned();
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(f.received.lock().unwrap().len(), 1);
    let response = f
        .client
        .get(format!("{}/v1/runs/{run}", f.url))
        .bearer_auth(format!("full.{}", "07".repeat(32)))
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(response["state"], "succeeded");
    let (status, _) = f
        .post(
            "/v1/deliveries/reconcile",
            "start",
            8,
            json!({"delivery_id":delivery,"outcome":"confirmed_delivered"}),
        )
        .await;
    assert_eq!(status, 403);
    let (status, _) = f
        .post(
            "/v1/deliveries/reconcile",
            "other",
            9,
            json!({"delivery_id":delivery,"outcome":"confirmed_delivered"}),
        )
        .await;
    assert_eq!(status, 404);
    let f = f.restart_without_delivery().await;
    let (status, body) = f
        .post(
            "/v1/deliveries/reconcile",
            "full",
            7,
            json!({"delivery_id":delivery,"outcome":"confirmed_delivered"}),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["reconciled"], true);
    let (status, _) = f
        .post(
            "/v1/deliveries/reconcile",
            "full",
            7,
            json!({"delivery_id":delivery,"outcome":"confirmed_not_delivered"}),
        )
        .await;
    assert_eq!(status, 409);
    f.shutdown().await;
}
