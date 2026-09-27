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
use axum::{
    Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::post,
};
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

async fn receive_deduplicated(
    State(received): State<Arc<Mutex<Vec<Value>>>>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    let id = headers
        .get("x-delivery-id")
        .and_then(|value| value.to_str().ok())
        .unwrap()
        .to_owned();
    let payload: Value = serde_json::from_slice(&body).unwrap();
    let mut received = received.lock().unwrap();
    let seen = received.iter().any(|item| item["dedup_id"] == id);
    received.push(json!({"dedup_id":id,"payload":payload,"applied":!seen}));
    if seen {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
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
    backend: Arc<DummyBackend>,
    delivery: DeliveryConfig,
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
            .route("/dedup", post(receive_deduplicated))
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
                        "dedup".into(),
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
            backend: backend.clone(),
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
                id: "dedup".into(),
                revision: "v1".into(),
                kind: DestinationKind::WebhookTest,
                endpoint: Some(format!("http://{receiver_address}/dedup")),
                notion_parent: None,
                deduplication: true,
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
            Some(delivery.clone()),
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
            backend,
            delivery,
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
            backend: self.backend.clone(),
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
    assert_eq!(f.backend.captured_inputs().len(), 1);
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
    assert_eq!(f.backend.captured_inputs().len(), 1);
    assert_eq!(f.received.lock().unwrap().len(), 1);
    f.shutdown().await;
}

#[tokio::test]
async fn schema_failure_commits_failed_result_and_matching_notification_without_leaking_output() {
    let f = Fixture::new().await;
    let (status, _) = f.post("/v1/hooks", "full", 7, json!({"hook_id":"validation-failed","version":"1","destination_id":"webhook","events":["failed"]})).await;
    assert_eq!(status, 200);
    let (status, receipt) = f.post("/v1/runs", "full", 7, json!({"profile_id":"profile","workspace_id":"workspace","input":{"request":"validation fixture"},"idempotency_key":"validation-notification","result_schema":{"type":"object"},"hooks":[{"hook_id":"validation-failed","version":"1"}]})).await;
    assert_eq!(status, 202);
    let run = receipt["run_id"].as_str().unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while f.received.lock().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let received = f.received.lock().unwrap().clone();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0]["state"], "failed");
    assert!(received[0].get("output").is_none());
    assert!(!received[0].to_string().contains("Scripted dummy output"));
    let result: Value = f
        .client
        .get(format!("{}/v1/runs/{run}/result", f.url))
        .bearer_auth(format!("full.{}", "07".repeat(32)))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let data: Value = serde_json::from_str(result["data"].as_str().unwrap()).unwrap();
    assert_eq!(
        data["details"]["validation"]["reason"],
        "invalid_or_unsupported_json"
    );
    assert_eq!(
        data["details"]["outcome"]["output"],
        "Scripted dummy output"
    );
    assert_eq!(f.backend.captured_inputs().len(), 1);
    f.shutdown().await;
}

#[tokio::test]
async fn hook_binding_stays_pinned_after_ack_and_default_change_before_terminal() {
    let f = Fixture::with_settings(128, DummyScenario::DelayedSuccess).await;
    let (status, body) = f.post("/v1/hooks", "full", 7, json!({"hook_id":"pinned","version":"1","destination_id":"webhook","events":["cancelled"]})).await;
    assert_eq!(status, 200, "{body}");
    let (status, _) = f
        .post(
            "/v1/hook-defaults",
            "full",
            7,
            json!({"hooks":[{"hook_id":"pinned","version":"1"}]}),
        )
        .await;
    assert_eq!(status, 200);
    let request = json!({"profile_id":"profile","workspace_id":"workspace","input":{"request":"pinned fixture"},"idempotency_key":"pinned-before-terminal"});
    let (status, receipt) = f.post("/v1/runs", "full", 7, request.clone()).await;
    assert_eq!(status, 202);
    let run = receipt["run_id"].as_str().unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while f.backend.captured_inputs().is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let (status, _) = f
        .post("/v1/hook-defaults", "full", 7, json!({"hooks":[]}))
        .await;
    assert_eq!(status, 200);
    let (status, _) = f.post("/v1/hooks", "full", 7, json!({"hook_id":"pinned","version":"2","destination_id":"notion","events":["cancelled"]})).await;
    assert_eq!(status, 200);
    let (status, replay) = f.post("/v1/runs", "full", 7, request).await;
    assert_eq!(status, 200);
    assert_eq!(replay["run_id"], run);
    assert_eq!(replay["replay"], true);
    let (status, _) = f
        .post(&format!("/v1/runs/{run}/cancel"), "full", 7, json!({}))
        .await;
    assert_eq!(status, 200);
    tokio::time::timeout(Duration::from_secs(3), async {
        while f.received.lock().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let received = f.received.lock().unwrap().clone();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0]["state"], "cancelled");
    assert_eq!(received[0]["synthetic"], true);
    assert!(received[0].get("notion").is_none());
    assert_eq!(f.backend.captured_inputs().len(), 1);
    f.shutdown().await;
}

#[tokio::test]
async fn lost_inbox_ack_response_can_be_retried_after_reconnection() {
    use tokio::io::AsyncWriteExt;
    let f = Fixture::new().await;
    let (status, body) = f.post("/v1/hooks", "full", 7, json!({"hook_id":"ack-hook","version":"1","destination_id":"inbox","events":["succeeded"]})).await;
    assert_eq!(status, 200, "{body}");
    let (status, _) = f.post("/v1/runs", "full", 7, json!({"profile_id":"profile","workspace_id":"workspace","input":{"request":"ack fixture"},"idempotency_key":"ack-run","hooks":[{"hook_id":"ack-hook","version":"1"}]})).await;
    assert_eq!(status, 202);
    let inbox_id = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let (_, page) = f
                .post("/v1/inbox/list", "full", 7, json!({"limit":5}))
                .await;
            if let Some(id) = page["items"][0]["inbox_id"].as_str() {
                break id.to_owned();
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let (_, page) = f
        .post("/v1/inbox/list", "full", 7, json!({"limit":5}))
        .await;
    assert_eq!(page["items"][0]["inbox_id"], inbox_id);
    let address = f.url.strip_prefix("http://").unwrap();
    let mut connection = tokio::net::TcpStream::connect(address).await.unwrap();
    let body = json!({"inbox_id":inbox_id}).to_string();
    let request = format!(
        "POST /v1/inbox/ack HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer full.{}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        "07".repeat(32),
        body.len()
    );
    connection.write_all(request.as_bytes()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let db = rusqlite::Connection::open(&f.path).unwrap();
            let acknowledged: bool = db
                .query_row(
                    "SELECT acknowledged_ms IS NOT NULL FROM completion_inbox WHERE inbox_id=?1",
                    [&inbox_id],
                    |row| row.get(0),
                )
                .unwrap_or(false);
            if acknowledged {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("ack was not committed before response loss");
    drop(connection);
    let result = f
        .mcp(
            "tools/call",
            Some("inbox_ack"),
            json!({"inbox_id":inbox_id}),
            "full",
            7,
        )
        .await;
    assert_eq!(result["result"]["structuredContent"]["acknowledged"], true);
    let (_, page) = f
        .post("/v1/inbox/list", "full", 7, json!({"limit":5}))
        .await;
    assert_eq!(page["items"][0]["acknowledged"], true);
    assert_eq!(f.backend.captured_inputs().len(), 1);
    f.shutdown().await;
}

#[tokio::test]
async fn changed_destination_after_ack_never_retargets_the_pinned_delivery() {
    let mut f = Fixture::with_settings(128, DummyScenario::DelayedSuccess).await;
    let (status, _) = f.post("/v1/hooks", "full", 7, json!({"hook_id":"pinned-destination","version":"1","destination_id":"webhook","events":["interrupted"]})).await;
    assert_eq!(status, 200);
    let db = rusqlite::Connection::open(&f.path).unwrap();
    db.execute_batch("CREATE TRIGGER block_pinned_dispatch BEFORE UPDATE OF dispatch_started ON completion_delivery_jobs WHEN NEW.dispatch_started=1 BEGIN SELECT RAISE(ABORT, 'dispatch barrier'); END;").unwrap();
    drop(db);
    let request = json!({"profile_id":"profile","workspace_id":"workspace","input":{"request":"destination snapshot"},"idempotency_key":"pinned-destination","hooks":[{"hook_id":"pinned-destination","version":"1"}]});
    let (status, receipt) = f.post("/v1/runs", "full", 7, request.clone()).await;
    assert_eq!(status, 202);
    let run = receipt["run_id"].as_str().unwrap().to_owned();
    tokio::time::timeout(Duration::from_secs(2), async {
        while f.backend.captured_inputs().is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    f.stop.cancel();
    tokio::time::timeout(Duration::from_secs(5), &mut f.task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let db = rusqlite::Connection::open(&f.path).unwrap();
    db.execute_batch("DROP TRIGGER block_pinned_dispatch")
        .unwrap();
    let snapshot: Vec<u8> = db
        .query_row(
            "SELECT destination_snapshot FROM completion_bindings WHERE run_id=?1",
            [&run],
            |row| row.get(0),
        )
        .unwrap();
    let pinned: Value = serde_json::from_slice(&snapshot).unwrap();
    let original = pinned["endpoint"].as_str().unwrap();
    assert!(original.ends_with("/hook"));
    drop(db);
    let destination = f
        .delivery
        .destinations
        .iter_mut()
        .find(|destination| destination.id == "webhook")
        .unwrap();
    destination.endpoint = Some(original.replace("/hook", "/dedup"));
    let profiles = vec![BackendProfile {
        profile_id: "profile".into(),
        revision: "v1".into(),
        state_realm: "dummy".into(),
        backend: f.backend.clone(),
    }];
    let service = RunService::open_with_delivery(
        &f.path,
        f.access.clone(),
        profiles,
        f.runs.clone(),
        Some(f.delivery.clone()),
    )
    .await
    .unwrap();
    let server = Server::bind("127.0.0.1:0".parse().unwrap(), service, f.transport.clone())
        .await
        .unwrap();
    f.url = format!("http://{}", server.local_addr().unwrap());
    f.stop = CancellationToken::new();
    f.task = tokio::spawn(server.run(f.stop.clone()));
    let (status, replay) = f.post("/v1/runs", "full", 7, request).await;
    assert_eq!(status, 200);
    assert_eq!(replay["run_id"], run);
    assert_eq!(replay["replay"], true);
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            let (_, page) = f
                .post(
                    "/v1/deliveries/list",
                    "full",
                    7,
                    json!({"run_id":run,"limit":5}),
                )
                .await;
            if page["items"][0]["state"] == "skipped" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(f.received.lock().unwrap().is_empty());
    assert_eq!(f.backend.captured_inputs().len(), 1);
    f.shutdown().await;
}

#[tokio::test]
async fn revocation_before_dispatch_blocks_the_actual_webhook_worker() {
    let f = Fixture::new().await;
    let (status, _) = f.post("/v1/hooks", "full", 7, json!({"hook_id":"revoked-before","version":"1","destination_id":"webhook","events":["succeeded"]})).await;
    assert_eq!(status, 200);
    let db = rusqlite::Connection::open(&f.path).unwrap();
    db.execute_batch("CREATE TRIGGER pause_authorized_dispatch BEFORE UPDATE OF dispatch_started ON completion_delivery_jobs WHEN NEW.dispatch_started=1 BEGIN SELECT RAISE(ABORT, 'dispatch barrier'); END;").unwrap();
    drop(db);
    let (status, receipt) = f.post("/v1/runs", "full", 7, json!({"profile_id":"profile","workspace_id":"workspace","input":{"request":"revocation fixture"},"idempotency_key":"revoked-before","hooks":[{"hook_id":"revoked-before","version":"1"}]})).await;
    assert_eq!(status, 202);
    let run = receipt["run_id"].as_str().unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let db = rusqlite::Connection::open(&f.path).unwrap();
            let state: Option<String> = db
                .query_row(
                    "SELECT state FROM delivery_intents WHERE run_id=?1",
                    [run],
                    |row| row.get(0),
                )
                .ok();
            if state.as_deref() == Some("leased") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(f.access.revoke("full").unwrap());
    let db = rusqlite::Connection::open(&f.path).unwrap();
    db.execute_batch("DROP TRIGGER pause_authorized_dispatch")
        .unwrap();
    drop(db);
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            let (_, page) = f
                .post(
                    "/v1/deliveries/list",
                    "control",
                    10,
                    json!({"run_id":run,"limit":5}),
                )
                .await;
            if page["items"][0]["state"] == "skipped" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(f.received.lock().unwrap().is_empty());
    assert_eq!(f.backend.captured_inputs().len(), 1);
    f.shutdown().await;
}

#[tokio::test]
async fn revocation_after_dispatch_preserves_uncertain_effect_and_payload() {
    let f = Fixture::new().await;
    let (status, _) = f.post("/v1/hooks", "full", 7, json!({"hook_id":"revoked-after","version":"1","destination_id":"slow","events":["succeeded"]})).await;
    assert_eq!(status, 200);
    let (status, receipt) = f.post("/v1/runs", "full", 7, json!({"profile_id":"profile","workspace_id":"workspace","input":{"request":"revocation fixture"},"idempotency_key":"revoked-after","hooks":[{"hook_id":"revoked-after","version":"1"}]})).await;
    assert_eq!(status, 202);
    let run = receipt["run_id"].as_str().unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while f.received.lock().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(f.access.revoke("full").unwrap());
    let delivery = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let (_, page) = f
                .post(
                    "/v1/deliveries/list",
                    "control",
                    10,
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
    let db = rusqlite::Connection::open(&f.path).unwrap();
    let bytes: i64 = db
        .query_row(
            "SELECT length(payload) FROM delivery_intents WHERE delivery_id=?1",
            [&delivery],
            |row| row.get(0),
        )
        .unwrap();
    assert!(bytes > 0);
    assert_eq!(f.received.lock().unwrap().len(), 1);
    assert_eq!(f.backend.captured_inputs().len(), 1);
    drop(db);
    f.shutdown().await;
}

#[tokio::test]
async fn deduplicating_receiver_observes_stable_retry_without_duplicate_effect_or_run() {
    let f = Fixture::new().await;
    let (status, body) = f.post("/v1/hooks", "full", 7, json!({"hook_id":"dedup-hook","version":"1","destination_id":"dedup","events":["succeeded"]})).await;
    assert_eq!(status, 200, "{body}");
    let (status, receipt) = f.post("/v1/runs", "full", 7, json!({"profile_id":"profile","workspace_id":"workspace","input":{"request":"dedup fixture"},"idempotency_key":"dedup-run","hooks":[{"hook_id":"dedup-hook","version":"1"}]})).await;
    assert_eq!(status, 202);
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
    let received = f.received.lock().unwrap().clone();
    assert_eq!(received.len(), 2);
    assert_eq!(received[0]["dedup_id"], received[1]["dedup_id"]);
    assert_eq!(received[0]["payload"], received[1]["payload"]);
    assert_eq!(received[0]["applied"], true);
    assert_eq!(received[1]["applied"], false);
    assert_eq!(f.backend.captured_inputs().len(), 1);
    f.shutdown().await;
}

#[tokio::test]
async fn committed_terminal_survives_delivery_worker_fault_and_restart() {
    let mut f = Fixture::new().await;
    let (status, body) = f.post("/v1/hooks", "full", 7, json!({"hook_id":"postcommit","version":"1","destination_id":"webhook","events":["succeeded"]})).await;
    assert_eq!(status, 200, "{body}");
    let db = rusqlite::Connection::open(&f.path).unwrap();
    db.execute_batch("CREATE TRIGGER block_dispatch BEFORE UPDATE OF dispatch_started ON completion_delivery_jobs WHEN NEW.dispatch_started=1 BEGIN SELECT RAISE(ABORT, 'injected dispatch fault'); END;").unwrap();
    drop(db);
    let request = json!({"profile_id":"profile","workspace_id":"workspace","input":{"request":"postcommit fixture"},"idempotency_key":"postcommit","hooks":[{"hook_id":"postcommit","version":"1"}]});
    let (status, receipt) = f.post("/v1/runs", "full", 7, request.clone()).await;
    assert_eq!(status, 202);
    let run = receipt["run_id"].as_str().unwrap().to_owned();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let db = rusqlite::Connection::open(&f.path).unwrap();
            let state: Option<String> = db
                .query_row(
                    "SELECT state FROM delivery_intents WHERE run_id=?1",
                    [&run],
                    |row| row.get(0),
                )
                .ok();
            if state.as_deref() == Some("leased") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("delivery lease not observed");
    let db = rusqlite::Connection::open(&f.path).unwrap();
    let (state, result, terminal): (String,i64,i64) = db.query_row("SELECT state,(SELECT COUNT(*) FROM results WHERE run_id=r.run_id),(SELECT COUNT(*) FROM events WHERE run_id=r.run_id AND kind='run.finished') FROM runs r WHERE run_id=?1", [&run], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?))).unwrap();
    assert_eq!((state.as_str(), result, terminal), ("succeeded", 1, 1));
    assert!(f.received.lock().unwrap().is_empty());
    drop(db);
    f.stop.cancel();
    tokio::time::timeout(Duration::from_secs(5), &mut f.task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let db = rusqlite::Connection::open(&f.path).unwrap();
    db.execute_batch("DROP TRIGGER block_dispatch").unwrap();
    drop(db);
    let profiles = vec![BackendProfile {
        profile_id: "profile".into(),
        revision: "v1".into(),
        state_realm: "dummy".into(),
        backend: f.backend.clone(),
    }];
    let service = RunService::open_with_delivery(
        &f.path,
        f.access.clone(),
        profiles,
        f.runs.clone(),
        Some(f.delivery.clone()),
    )
    .await
    .unwrap();
    let server = Server::bind("127.0.0.1:0".parse().unwrap(), service, f.transport.clone())
        .await
        .unwrap();
    f.url = format!("http://{}", server.local_addr().unwrap());
    f.stop = CancellationToken::new();
    f.task = tokio::spawn(server.run(f.stop.clone()));
    let (status, replay) = f.post("/v1/runs", "full", 7, request).await;
    assert_eq!(status, 200);
    assert_eq!(replay["run_id"], run);
    assert_eq!(replay["replay"], true);
    tokio::time::timeout(Duration::from_secs(4), async {
        while f.received.lock().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(f.received.lock().unwrap().len(), 1);
    assert_eq!(f.received.lock().unwrap()[0]["state"], "succeeded");
    assert_eq!(f.backend.captured_inputs().len(), 1);
    f.shutdown().await;
}

#[tokio::test]
async fn terminal_fault_rolls_back_the_real_service_and_recovery_does_not_rerun() {
    let mut f = Fixture::new().await;
    for (hook, destination) in [("fault-webhook", "webhook"), ("fault-inbox", "inbox")] {
        let (status, body) = f.post("/v1/hooks", "full", 7, json!({"hook_id":hook,"version":"1","destination_id":destination,"events":["succeeded","interrupted"]})).await;
        assert_eq!(status, 200, "{body}");
    }
    let db = rusqlite::Connection::open(&f.path).unwrap();
    db.execute_batch("CREATE TRIGGER fail_terminal_inbox BEFORE INSERT ON completion_inbox BEGIN SELECT RAISE(ABORT, 'injected failure'); END;").unwrap();
    drop(db);
    let request = json!({"profile_id":"profile","workspace_id":"workspace","input":{"request":"synthetic terminal fault"},"idempotency_key":"terminal-fault","hooks":[{"hook_id":"fault-webhook","version":"1"},{"hook_id":"fault-inbox","version":"1"}]});
    let response = f
        .client
        .post(format!("{}/v1/runs", f.url))
        .bearer_auth(format!("full.{}", "07".repeat(32)))
        .json(&request)
        .send()
        .await;
    if let Ok(response) = response {
        assert!([202, 503].contains(&response.status().as_u16()));
    }
    let stopped = tokio::time::timeout(Duration::from_secs(5), &mut f.task)
        .await
        .unwrap()
        .unwrap();
    assert!(stopped.is_err());
    assert_eq!(f.backend.captured_inputs().len(), 1);
    let db = rusqlite::Connection::open(&f.path).unwrap();
    let run: String = db
        .query_row(
            "SELECT run_id FROM runs WHERE idempotency_key='terminal-fault'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let (state, results, terminals, deliveries, inbox): (String,i64,i64,i64,i64) = db.query_row("SELECT state,(SELECT COUNT(*) FROM results WHERE run_id=r.run_id),(SELECT COUNT(*) FROM events WHERE run_id=r.run_id AND kind='run.finished'),(SELECT COUNT(*) FROM delivery_intents WHERE run_id=r.run_id),(SELECT COUNT(*) FROM inbox_items WHERE run_id=r.run_id) FROM runs r WHERE run_id=?1", [&run], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?))).unwrap();
    assert_eq!(
        (state.as_str(), results, terminals, deliveries, inbox),
        ("running", 0, 0, 0, 0)
    );
    assert!(f.received.lock().unwrap().is_empty());
    drop(db);
    let db = rusqlite::Connection::open(&f.path).unwrap();
    db.execute_batch("DROP TRIGGER fail_terminal_inbox")
        .unwrap();
    drop(db);
    let profiles = vec![BackendProfile {
        profile_id: "profile".into(),
        revision: "v1".into(),
        state_realm: "dummy".into(),
        backend: f.backend.clone(),
    }];
    let service = RunService::open_with_delivery(
        &f.path,
        f.access.clone(),
        profiles,
        f.runs.clone(),
        Some(f.delivery.clone()),
    )
    .await
    .unwrap();
    let server = Server::bind("127.0.0.1:0".parse().unwrap(), service, f.transport.clone())
        .await
        .unwrap();
    f.url = format!("http://{}", server.local_addr().unwrap());
    f.stop = CancellationToken::new();
    f.task = tokio::spawn(server.run(f.stop.clone()));
    let (status, replay) = f.post("/v1/runs", "full", 7, request).await;
    assert_eq!(status, 200);
    assert_eq!(replay["run_id"], run);
    assert_eq!(replay["replay"], true);
    tokio::time::timeout(Duration::from_secs(3), async {
        while f.received.lock().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(f.received.lock().unwrap()[0]["state"], "interrupted");
    assert_eq!(f.backend.captured_inputs().len(), 1);
    let db = rusqlite::Connection::open(&f.path).unwrap();
    let (state,results,terminals,deliveries,inbox): (String,i64,i64,i64,i64) = db.query_row("SELECT state,(SELECT COUNT(*) FROM results WHERE run_id=r.run_id),(SELECT COUNT(*) FROM events WHERE run_id=r.run_id AND kind='run.finished'),(SELECT COUNT(*) FROM delivery_intents WHERE run_id=r.run_id),(SELECT COUNT(*) FROM inbox_items WHERE run_id=r.run_id) FROM runs r WHERE run_id=?1", [&run], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?))).unwrap();
    assert_eq!(
        (state.as_str(), results, terminals, deliveries, inbox),
        ("interrupted", 1, 1, 1, 1)
    );
    drop(db);
    f.shutdown().await;
}
