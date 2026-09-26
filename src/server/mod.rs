//! Authenticated loopback REST and MCP adapters for the same durable application.

pub mod config;
mod listener;
mod mcp;
mod sessions;
mod wire;

use std::{
    fmt,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU16, Ordering},
    },
    time::{Duration, Instant},
};

use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{Request, State},
    http::{HeaderValue, Method, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use serde_json::{Value, json};
use tokio::{net::TcpListener, sync::Semaphore};
use tokio_util::sync::CancellationToken;
use zeroize::Zeroizing;

use crate::{
    access::{AuthenticatedClient, ClientAccessControl, Scope},
    application::{RunHandle, RunService, ServiceError},
};
use sessions::{BoundSessions, PRINCIPAL};
use wire::ApiError;

/// Explicit transport budgets, independent of queue-inclusive run deadlines.
#[derive(Clone, Debug)]
pub struct ServerLimits {
    pub max_request_bytes: usize,
    pub max_response_bytes: usize,
    pub max_header_bytes: usize,
    pub max_inflight: usize,
    pub max_connections: usize,
    pub connection_idle_timeout: Duration,
    pub connection_max_lifetime: Duration,
    pub requests_per_second: u32,
    pub burst: u32,
    pub request_timeout: Duration,
    pub shutdown_grace: Duration,
    pub max_mcp_sessions: usize,
    pub mcp_session_idle: Duration,
}
impl ServerLimits {
    fn validate(&self) -> bool {
        self.max_request_bytes >= 1024
            && self.max_response_bytes >= 8192
            && self.max_header_bytes >= 512
            && self.max_inflight > 0
            && self.max_connections > 0
            && self.connection_idle_timeout > self.request_timeout
            && self.connection_max_lifetime > self.connection_idle_timeout
            && self.requests_per_second > 0
            && self.burst > 0
            && self.max_mcp_sessions > 0
            && [
                self.request_timeout,
                self.connection_idle_timeout,
                self.connection_max_lifetime,
                self.shutdown_grace,
                self.mcp_session_idle,
            ]
            .iter()
            .all(|d| !d.is_zero() && Instant::now().checked_add(*d).is_some())
    }
}

#[derive(Debug)]
pub enum ServerError {
    InvalidConfiguration,
    Bind,
    Transport,
    ShutdownTimeout,
    Service(ServiceError),
}
impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidConfiguration => "invalid server configuration",
            Self::Bind => "cannot bind loopback listener",
            Self::Transport => "HTTP transport failed",
            Self::ShutdownTimeout => "server shutdown deadline exceeded",
            Self::Service(_) => "execution service failed",
        })
    }
}
impl std::error::Error for ServerError {}

struct TransportGuard {
    stop: CancellationToken,
    abort: tokio::task::AbortHandle,
    grace: Duration,
    armed: bool,
}
impl Drop for TransportGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.stop.cancel();
        let abort = self.abort.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let grace = self.grace;
            runtime.spawn(async move {
                tokio::time::sleep(grace).await;
                abort.abort();
            });
        } else {
            abort.abort();
        }
    }
}

#[derive(Clone)]
struct RequestPrincipal {
    client: AuthenticatedClient,
    http_status: Arc<AtomicU16>,
}
#[derive(Clone)]
struct ParsedBody(Value);
struct Bucket {
    tokens: f64,
    updated: Instant,
}
struct Shared {
    handle: RunHandle,
    access: Arc<ClientAccessControl>,
    limits: ServerLimits,
    authority: String,
    localhost_authority: String,
    intake: Arc<Semaphore>,
    bucket: Mutex<Bucket>,
    sessions: Arc<BoundSessions>,
}

/// Owns transport shutdown while the application retains exclusive execution ownership.
pub struct Server {
    service: RunService,
    listener: TcpListener,
    router: Router,
    shared: Arc<Shared>,
    transport_stop: CancellationToken,
}
impl Server {
    /// Port zero is available to integration tests; deployment configuration requires a port.
    pub async fn bind(
        address: SocketAddr,
        service: RunService,
        limits: ServerLimits,
    ) -> Result<Self, ServerError> {
        if !address.ip().is_loopback()
            || !limits.validate()
            || unsafe_sdk_logging_enabled()
            || service
                .minimum_response_bytes()
                .is_none_or(|minimum| limits.max_response_bytes < minimum)
        {
            let _ = service.shutdown().await;
            return Err(ServerError::InvalidConfiguration);
        }
        let listener = match TcpListener::bind(address).await {
            Ok(l) => l,
            Err(_) => {
                let _ = service.shutdown().await;
                return Err(ServerError::Bind);
            }
        };
        let address = listener.local_addr().map_err(|_| ServerError::Bind)?;
        let transport_stop = CancellationToken::new();
        let shared = Arc::new(Shared {
            handle: service.handle(),
            access: service.access_control(),
            authority: address.to_string(),
            localhost_authority: format!("localhost:{}", address.port()),
            intake: Arc::new(Semaphore::new(limits.max_inflight)),
            bucket: Mutex::new(Bucket {
                tokens: f64::from(limits.burst),
                updated: Instant::now(),
            }),
            sessions: Arc::new(BoundSessions::new(
                limits.max_mcp_sessions,
                limits.mcp_session_idle,
                limits.request_timeout,
            )),
            limits,
        });
        let state = Arc::clone(&shared);
        let sdk: StreamableHttpService<mcp::Handler, BoundSessions> = StreamableHttpService::new(
            move || {
                Ok(mcp::Handler {
                    shared: Arc::clone(&state),
                })
            },
            Arc::clone(&shared.sessions),
            StreamableHttpServerConfig::default()
                .with_legacy_session_mode(true)
                .with_json_response(true)
                .with_sse_keep_alive(None)
                .with_max_request_body_bytes(shared.limits.max_request_bytes)
                .with_allowed_hosts([shared.authority.clone(), shared.localhost_authority.clone()])
                .enforce_origin_validation()
                .with_cancellation_token(transport_stop.clone()),
        );
        let router = Router::new()
            .nest_service("/mcp", sdk)
            .fallback(rest)
            .layer(middleware::from_fn_with_state(Arc::clone(&shared), ingress))
            .with_state(Arc::clone(&shared));
        Ok(Self {
            service,
            listener,
            router,
            shared,
            transport_stop,
        })
    }
    pub fn local_addr(&self) -> Result<SocketAddr, ServerError> {
        self.listener.local_addr().map_err(|_| ServerError::Bind)
    }

    /// Stops new work, drains bounded request handlers and releases the service guard.
    pub async fn run(self, stop: CancellationToken) -> Result<(), ServerError> {
        let Self {
            service,
            listener,
            router,
            shared,
            transport_stop,
        } = self;
        let graceful = transport_stop.clone();
        let accept_failed = CancellationToken::new();
        let listener = listener::BoundedListener::new(
            listener,
            shared.limits.max_connections,
            shared.limits.connection_idle_timeout,
            shared.limits.connection_max_lifetime,
            accept_failed.clone(),
        );
        let mut task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(graceful.cancelled_owned())
                .await
        });
        let mut transport_guard = TransportGuard {
            stop: transport_stop.clone(),
            abort: task.abort_handle(),
            grace: shared.limits.shutdown_grace,
            armed: true,
        };
        let mut maintenance_failed = false;
        let transport_result = loop {
            tokio::select! {
                result = &mut task => break Some(result),
                _ = stop.cancelled() => break None,
                _ = accept_failed.cancelled() => { maintenance_failed = true; break None; },
                _ = tokio::time::sleep(shared.limits.mcp_session_idle.min(shared.limits.request_timeout).min(Duration::from_secs(5))) => {
                    if !matches!(tokio::time::timeout(shared.limits.request_timeout, shared.sessions.expire(false)).await, Ok(Ok(()))) {
                        maintenance_failed = true;
                        break None;
                    }
                }
            }
        };
        service.begin_shutdown();
        transport_stop.cancel();
        maintenance_failed |= accept_failed.is_cancelled();
        let grace = shared.limits.shutdown_grace;
        let transport_error = match transport_result {
            Some(_) => Some(ServerError::Transport),
            None => match tokio::time::timeout(grace, &mut task).await {
                Ok(Ok(Ok(()))) if !maintenance_failed => None,
                Ok(_) => Some(ServerError::Transport),
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                    Some(ServerError::ShutdownTimeout)
                }
            },
        };
        let sessions_ok = matches!(
            tokio::time::timeout(grace, shared.sessions.expire(true)).await,
            Ok(Ok(()))
        );
        let service_result = tokio::time::timeout(grace, service.shutdown()).await;
        transport_guard.armed = false;
        if let Some(error) = transport_error {
            return Err(error);
        }
        if !sessions_ok {
            return Err(ServerError::ShutdownTimeout);
        }
        match service_result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(ServerError::Service(e)),
            Err(_) => Err(ServerError::ShutdownTimeout),
        }
    }
}

fn response(error: ApiError) -> Response {
    json_response(error.status, &error.value())
}
fn json_response(status: StatusCode, value: &Value) -> Response {
    let bytes = serde_json::to_vec(value).expect("JSON values are serializable");
    (
        status,
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        bytes,
    )
        .into_response()
}
fn rpc_error(status: StatusCode, code: i32, message: &str, id: Option<&Value>) -> Response {
    let mut value = json!({"jsonrpc":"2.0","error":{"code":code,"message":message}});
    if let Some(id) = id {
        value["id"] = id.clone();
    }
    json_response(status, &value)
}

fn authenticate(shared: &Shared, request: &Request) -> Result<AuthenticatedClient, ApiError> {
    if request
        .headers()
        .get_all(header::AUTHORIZATION)
        .iter()
        .count()
        != 1
    {
        return Err(ApiError::new(StatusCode::UNAUTHORIZED, "unauthenticated"));
    }
    let token = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or(ApiError::new(StatusCode::UNAUTHORIZED, "unauthenticated"))?;
    let (id, secret) = token
        .split_once('.')
        .ok_or(ApiError::new(StatusCode::UNAUTHORIZED, "unauthenticated"))?;
    let proof =
        decode_secret(secret).ok_or(ApiError::new(StatusCode::UNAUTHORIZED, "unauthenticated"))?;
    shared
        .access
        .authenticate(id, proof.as_ref())
        .map_err(Into::into)
}
pub(crate) fn decode_secret(s: &str) -> Option<Zeroizing<[u8; 32]>> {
    if s.len() != 64 {
        return None;
    }
    let mut proof = Zeroizing::new([0; 32]);
    for (n, pair) in s.as_bytes().chunks_exact(2).enumerate() {
        let hex = |c: u8| match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            _ => None,
        };
        proof[n] = hex(pair[0])? * 16 + hex(pair[1])?;
    }
    Some(proof)
}
fn reserve_rate(shared: &Shared) -> bool {
    let Ok(mut b) = shared.bucket.lock() else {
        return false;
    };
    b.tokens = (b.tokens
        + b.updated.elapsed().as_secs_f64() * f64::from(shared.limits.requests_per_second))
    .min(f64::from(shared.limits.burst));
    b.updated = Instant::now();
    if b.tokens < 1.0 {
        false
    } else {
        b.tokens -= 1.0;
        true
    }
}

async fn ingress(State(shared): State<Arc<Shared>>, request: Request, next: Next) -> Response {
    let max = shared.limits.max_response_bytes;
    let result = ingress_inner(State(shared), request, next).await;
    let (mut parts, body) = result.into_parts();
    match to_bytes(body, max).await {
        Ok(bytes) => {
            parts.headers.remove(header::TRANSFER_ENCODING);
            parts
                .headers
                .insert(header::CONTENT_LENGTH, HeaderValue::from(bytes.len()));
            parts
                .headers
                .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
            Response::from_parts(parts, Body::from(bytes))
        }
        Err(_) => response(ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "response_limit",
        )),
    }
}

fn unsafe_sdk_logging_enabled() -> bool {
    macro_rules! any_level_enabled {
        ($target:literal) => {
            tracing::enabled!(target: $target, tracing::Level::ERROR)
                || tracing::enabled!(target: $target, tracing::Level::WARN)
                || tracing::enabled!(target: $target, tracing::Level::INFO)
                || tracing::enabled!(target: $target, tracing::Level::DEBUG)
                || tracing::enabled!(target: $target, tracing::Level::TRACE)
        };
    }
    any_level_enabled!("rmcp::service")
        || any_level_enabled!("rmcp::transport::streamable_http_server::tower")
        || any_level_enabled!("rmcp::transport::streamable_http_server::session::local")
        || any_level_enabled!("rmcp::transport::common::server_side_http")
        || any_level_enabled!("rmcp::transport::worker")
}

async fn ingress_inner(
    State(shared): State<Arc<Shared>>,
    mut request: Request,
    next: Next,
) -> Response {
    let permit = match Arc::clone(&shared.intake).try_acquire_owned() {
        Ok(p) => p,
        Err(_) => {
            return response(ApiError::new(
                StatusCode::TOO_MANY_REQUESTS,
                "request_capacity",
            ));
        }
    };
    let headers = request.headers();
    let header_bytes = headers.iter().fold(0usize, |size, (k, v)| {
        size.saturating_add(k.as_str().len())
            .saturating_add(v.as_bytes().len())
    });
    if header_bytes > shared.limits.max_header_bytes {
        return response(ApiError::new(
            StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
            "header_limit",
        ));
    }
    let host = headers.get(header::HOST).and_then(|h| h.to_str().ok());
    if headers.get_all(header::HOST).iter().count() != 1
        || !host.is_some_and(|h| {
            h.eq_ignore_ascii_case(&shared.authority)
                || h.eq_ignore_ascii_case(&shared.localhost_authority)
        })
        || headers.contains_key(header::ORIGIN)
    {
        return response(ApiError::new(
            StatusCode::FORBIDDEN,
            "invalid_origin_or_host",
        ));
    }
    if request.uri().scheme().is_some()
        || request.uri().authority().is_some()
        || request
            .headers()
            .get_all(header::CONTENT_TYPE)
            .iter()
            .count()
            > 1
    {
        return response(ApiError::invalid());
    }
    if !reserve_rate(&shared) {
        return response(ApiError::new(StatusCode::TOO_MANY_REQUESTS, "request_rate"));
    }
    let client = match authenticate(&shared, &request) {
        Ok(c) => c,
        Err(e) => {
            let mut r = response(e);
            if e.status == StatusCode::UNAUTHORIZED {
                r.headers_mut().insert(
                    header::WWW_AUTHENTICATE,
                    HeaderValue::from_static("Bearer realm=\"agent-runner-server\""),
                );
            }
            return r;
        }
    };
    let identity = match shared.access.identity(&client) {
        Ok(i) => i,
        Err(e) => return response(e.into()),
    };
    request.headers_mut().remove(header::AUTHORIZATION);
    let principal = RequestPrincipal {
        client,
        http_status: Arc::new(AtomicU16::new(0)),
    };
    request.extensions_mut().insert(principal.clone());
    let is_mcp = request.uri().path() == "/mcp";
    if request.uri().path().starts_with("/mcp/") {
        return response(ApiError::new(StatusCode::NOT_FOUND, "not_found"));
    }
    if is_mcp && unsafe_sdk_logging_enabled() {
        return response(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "unsafe_sdk_logging",
        ));
    }
    let operation = async {
        let mut lease = None;
        let mut cancellation_guard = None;
        if request.headers().contains_key(header::CONTENT_ENCODING) {
            return response(ApiError::new(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "content_encoding_unsupported",
            ));
        }
        if request.method() == Method::POST {
            if !request
                .headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v.split(';').next() == Some("application/json"))
            {
                return response(ApiError::new(
                    StatusCode::UNSUPPORTED_MEDIA_TYPE,
                    "json_required",
                ));
            }
            let (parts, body) = request.into_parts();
            let bytes = match to_bytes(body, shared.limits.max_request_bytes).await {
                Ok(b) => b,
                Err(_) => {
                    return response(ApiError::new(StatusCode::PAYLOAD_TOO_LARGE, "body_limit"));
                }
            };
            let value = match wire::parse_json(&bytes) {
                Ok(v) => v,
                Err(e) => {
                    return if is_mcp {
                        rpc_error(
                            StatusCode::BAD_REQUEST,
                            -32700,
                            "invalid JSON or duplicate key",
                            None,
                        )
                    } else {
                        response(e)
                    };
                }
            };
            request = Request::from_parts(parts, Body::from(bytes));
            if is_mcp {
                if let Some(r) = validate_mcp(&shared, &principal, &identity, &mut request, &value)
                {
                    return r;
                }
                if value.get("method").and_then(Value::as_str) == Some("notifications/cancelled")
                    && let (Some(id), Some(version)) = (
                        request
                            .headers()
                            .get("mcp-session-id")
                            .and_then(|v| v.to_str().ok()),
                        request
                            .headers()
                            .get("mcp-protocol-version")
                            .and_then(|v| v.to_str().ok()),
                    )
                    && mcp::VERSIONS[1..].contains(&version)
                {
                    let request_id = value.pointer("/params/requestId").map(Value::to_string);
                    match shared
                        .sessions
                        .begin_cancellation(&Arc::from(id), &identity, request_id)
                    {
                        Ok(guard) => cancellation_guard = Some(guard),
                        Err(_) => {
                            return response(ApiError::new(
                                StatusCode::NOT_FOUND,
                                "session_not_found",
                            ));
                        }
                    }
                }
                if let (Some(id), Some(version), Some(request_id)) = (
                    request
                        .headers()
                        .get("mcp-session-id")
                        .and_then(|v| v.to_str().ok()),
                    request
                        .headers()
                        .get("mcp-protocol-version")
                        .and_then(|v| v.to_str().ok()),
                    value.get("id"),
                ) && mcp::VERSIONS[1..].contains(&version)
                {
                    match shared.sessions.begin_request(
                        &Arc::from(id),
                        &identity,
                        request_id.to_string(),
                    ) {
                        Ok(guard) => lease = Some(guard),
                        Err(_) => {
                            return rpc_error(
                                StatusCode::BAD_REQUEST,
                                -32600,
                                "request ID already active",
                                Some(request_id),
                            );
                        }
                    }
                }
            }
            request.extensions_mut().insert(ParsedBody(value));
        } else if is_mcp {
            if request.method() != Method::DELETE {
                return response(ApiError::new(
                    StatusCode::METHOD_NOT_ALLOWED,
                    "method_not_allowed",
                ));
            }
            let id = request
                .headers()
                .get("mcp-session-id")
                .and_then(|v| v.to_str().ok());
            let version = request
                .headers()
                .get("mcp-protocol-version")
                .and_then(|v| v.to_str().ok());
            if version == Some(mcp::VERSIONS[0]) {
                return response(ApiError::new(
                    StatusCode::METHOD_NOT_ALLOWED,
                    "method_not_allowed",
                ));
            }
            if !id.is_some_and(|id| {
                shared
                    .sessions
                    .check_version(&Arc::from(id), &identity, version)
            }) {
                return response(ApiError::new(StatusCode::NOT_FOUND, "session_not_found"));
            }
        }
        let context = sessions::SessionContext {
            identity,
            http_status: Arc::clone(&principal.http_status),
        };
        let r = PRINCIPAL.scope(context, next.run(request)).await;
        drop(cancellation_guard.take());
        let (mut parts, body) = r.into_parts();
        let bytes = match to_bytes(body, shared.limits.max_response_bytes).await {
            Ok(b) => b,
            Err(_) => {
                return response(ApiError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "response_limit",
                ));
            }
        };
        if let Some(lease) = lease.take() {
            lease.finish(&bytes);
        }
        let status = principal.http_status.load(Ordering::Acquire);
        if status == 401 || status == 403 || status == 429 {
            return response(ApiError::new(
                StatusCode::from_u16(status).unwrap(),
                match status {
                    401 => "unauthenticated",
                    403 => "forbidden",
                    _ => "session_capacity",
                },
            ));
        }
        parts.headers.remove(header::TRANSFER_ENCODING);
        parts
            .headers
            .insert(header::CONTENT_LENGTH, HeaderValue::from(bytes.len()));
        parts
            .headers
            .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        Response::from_parts(parts, Body::from(bytes))
    };
    let result = match tokio::time::timeout(shared.limits.request_timeout, operation).await {
        Ok(r) => r,
        Err(_) => json_response(
            StatusCode::GATEWAY_TIMEOUT,
            &json!({"error":{"code":"request_timeout","recovery":"Retry the same request with the same idempotency_key; admission may have committed."}}),
        ),
    };
    drop(permit);
    result
}

fn validate_mcp(
    shared: &Shared,
    principal: &RequestPrincipal,
    identity: &crate::access::ClientIdentity,
    request: &mut Request,
    value: &Value,
) -> Option<Response> {
    let method = value.get("method").and_then(Value::as_str);
    if value
        .get("id")
        .and_then(Value::as_str)
        .is_some_and(|id| id.len() > 128)
    {
        return Some(rpc_error(
            StatusCode::BAD_REQUEST,
            -32600,
            "request ID too long",
            None,
        ));
    }
    for name in [
        "mcp-protocol-version",
        "mcp-method",
        "mcp-name",
        "mcp-session-id",
    ] {
        if request.headers().get_all(name).iter().count() > 1 {
            return Some(rpc_error(
                StatusCode::BAD_REQUEST,
                -32020,
                "duplicate protocol header",
                value.get("id"),
            ));
        }
    }
    if value.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        || method.is_none()
        || !value.is_object()
        || value
            .get("id")
            .is_some_and(|id| !(id.is_string() || id.is_i64() || id.is_u64()))
    {
        return Some(rpc_error(
            StatusCode::BAD_REQUEST,
            -32600,
            "invalid request envelope",
            None,
        ));
    }
    if request.uri().query().is_some() {
        return Some(response(ApiError::invalid()));
    }
    if method == Some("initialize") {
        if request.headers().contains_key("mcp-session-id") {
            return Some(response(ApiError::invalid()));
        }
    } else {
        let version = request
            .headers()
            .get("mcp-protocol-version")
            .and_then(|v| v.to_str().ok());
        let Some(version) = version else {
            return Some(rpc_error(
                StatusCode::BAD_REQUEST,
                -32020,
                "protocol header required",
                value.get("id"),
            ));
        };
        if !mcp::VERSIONS.contains(&version) {
            let mut error = json!({"jsonrpc":"2.0","error":{"code":-32022,"message":"Unsupported protocol version","data":{"supported":mcp::VERSIONS,"requested":version}}});
            if let Some(id) = value.get("id") {
                error["id"] = id.clone();
            }
            return Some(json_response(StatusCode::BAD_REQUEST, &error));
        }
        if version == mcp::VERSIONS[0] {
            request.headers_mut().remove("mcp-session-id");
            request.headers_mut().remove("last-event-id");
        } else if mcp::VERSIONS[1..].contains(&version) {
            let valid = request
                .headers()
                .get("mcp-session-id")
                .and_then(|v| v.to_str().ok())
                .is_some_and(|id| {
                    shared
                        .sessions
                        .check_version(&Arc::from(id), identity, Some(version))
                });
            if !valid {
                return Some(response(ApiError::new(
                    StatusCode::NOT_FOUND,
                    "session_not_found",
                )));
            }
        }
    }
    if method == Some("tools/call") {
        let params = value.get("params").and_then(Value::as_object);
        if params.is_none_or(|p| {
            p.keys()
                .any(|k| !matches!(k.as_str(), "name" | "arguments" | "_meta"))
        }) {
            return Some(rpc_error(
                StatusCode::BAD_REQUEST,
                -32602,
                "unsupported tool-call parameters",
                value.get("id"),
            ));
        }
        let name = value
            .pointer("/params/name")
            .and_then(Value::as_str)
            .unwrap_or("");
        if !wire::TOOLS.contains(&name) {
            if request
                .headers()
                .get("mcp-protocol-version")
                .and_then(|v| v.to_str().ok())
                == Some(mcp::VERSIONS[0])
            {
                use base64::Engine;
                let method_header = request
                    .headers()
                    .get("mcp-method")
                    .and_then(|v| v.to_str().ok());
                let name_header = request
                    .headers()
                    .get("mcp-name")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| {
                        if let Some(encoded) = v
                            .strip_prefix("=?base64?")
                            .and_then(|v| v.strip_suffix("?="))
                        {
                            base64::engine::general_purpose::STANDARD
                                .decode(encoded)
                                .ok()
                                .and_then(|v| String::from_utf8(v).ok())
                        } else {
                            Some(v.to_owned())
                        }
                    });
                if method_header != Some("tools/call") || name_header.as_deref() != Some(name) {
                    return Some(rpc_error(
                        StatusCode::BAD_REQUEST,
                        -32020,
                        "header mismatch",
                        value.get("id"),
                    ));
                }
            }
            return Some(rpc_error(
                StatusCode::BAD_REQUEST,
                -32602,
                "unknown tool",
                value.get("id"),
            ));
        }
        if let Some(scopes) = value
            .pointer("/params/name")
            .and_then(Value::as_str)
            .and_then(wire::required_scopes)
        {
            for scope in scopes {
                if let Err(e) = shared.access.check_scope(&principal.client, *scope) {
                    return Some(response(e.into()));
                }
            }
        }
        if value
            .pointer("/params/arguments/session/mode")
            .and_then(Value::as_str)
            == Some("resume")
            && let Err(e) = shared
                .access
                .check_scope(&principal.client, Scope::SessionRead)
        {
            return Some(response(e.into()));
        }
    }
    None
}

fn decode_uri_component(value: &str, query: bool) -> Result<String, ApiError> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut position = 0;
    while position < bytes.len() {
        match bytes[position] {
            b'%' => {
                let high = bytes
                    .get(position + 1)
                    .and_then(|v| (*v as char).to_digit(16))
                    .ok_or_else(ApiError::invalid)?;
                let low = bytes
                    .get(position + 2)
                    .and_then(|v| (*v as char).to_digit(16))
                    .ok_or_else(ApiError::invalid)?;
                decoded.push(((high << 4) | low) as u8);
                position += 3;
            }
            b'+' if query => {
                decoded.push(b' ');
                position += 1;
            }
            value => {
                decoded.push(value);
                position += 1;
            }
        }
    }
    String::from_utf8(decoded).map_err(|_| ApiError::invalid())
}

async fn rest(State(shared): State<Arc<Shared>>, request: Request) -> Response {
    let Some(principal) = request.extensions().get::<RequestPrincipal>() else {
        return response(ApiError::new(StatusCode::UNAUTHORIZED, "unauthenticated"));
    };
    let path: Vec<_> = request.uri().path().split('/').collect();
    let method = request.method();
    let body = request
        .extensions()
        .get::<ParsedBody>()
        .map(|b| b.0.clone())
        .unwrap_or_else(|| json!({}));
    macro_rules! decode {
        ($value:expr, $query:expr) => {
            match decode_uri_component($value, $query) {
                Ok(value) => value,
                Err(error) => return response(error),
            }
        };
    }
    let (name, args) = match (method, path.as_slice()) {
        (&Method::GET, ["", "v1", "agents"]) => ("agents_list", json!({})),
        (&Method::POST, ["", "v1", "skills"]) => ("skills_register", body),
        (&Method::POST, ["", "v1", "skills", "list"]) => ("skills_list", body),
        (&Method::POST, ["", "v1", "skills", "get"]) => ("skills_get", body),
        (&Method::POST, ["", "v1", "skills", "enabled"]) => ("skills_set_enabled", body),
        (&Method::GET, ["", "v1", "skills"]) => {
            let mut limit = None;
            let mut skill_id = None;
            let mut version = None;
            for pair in request.uri().query().unwrap_or("").split('&') {
                match pair.split_once('=') {
                    Some(("limit", v)) if limit.is_none() => {
                        limit = v.parse::<usize>().ok();
                        if limit.is_none() {
                            return response(ApiError::invalid());
                        }
                    }
                    Some(("after_skill_id", v)) if skill_id.is_none() => {
                        skill_id = Some(decode!(v, true))
                    }
                    Some(("after_version", v)) if version.is_none() => {
                        version = Some(decode!(v, true))
                    }
                    _ => return response(ApiError::invalid()),
                }
            }
            let Some(limit) = limit else {
                return response(ApiError::invalid());
            };
            let after = match (skill_id, version) {
                (None, None) => Value::Null,
                (Some(id), Some(version)) => json!({"skill_id":id,"version":version}),
                _ => return response(ApiError::invalid()),
            };
            ("skills_list", json!({"limit":limit,"after":after}))
        }
        (&Method::GET, ["", "v1", "skills", id, version]) => (
            "skills_get",
            json!({"skill_id":decode!(id, false),"version":decode!(version, false)}),
        ),
        (&Method::POST, ["", "v1", "skills", id, version, "enabled"]) => {
            let Some(enabled) = body.get("enabled").and_then(Value::as_bool) else {
                return response(ApiError::invalid());
            };
            if body.as_object().is_none_or(|object| object.len() != 1) {
                return response(ApiError::invalid());
            }
            (
                "skills_set_enabled",
                json!({"skill_id":decode!(id, false),"version":decode!(version, false),"enabled":enabled}),
            )
        }
        (&Method::GET, ["", "v1", "skill-defaults"]) => ("skills_defaults", json!({})),
        (&Method::POST, ["", "v1", "skill-defaults"]) => ("skills_set_defaults", body),
        (&Method::POST, ["", "v1", "runs"]) => ("agents_start", body),
        (&Method::GET, ["", "v1", "runs", id]) => ("runs_get", json!({"run_id":id})),
        (&Method::GET, ["", "v1", "runs", id, "result"]) => ("runs_result", json!({"run_id":id})),
        (&Method::GET, ["", "v1", "runs", id, "events"]) => {
            let mut after = None;
            let mut limit = None;
            for pair in request.uri().query().unwrap_or("").split('&') {
                match pair.split_once('=') {
                    Some(("after", v)) if after.is_none() => {
                        after = v.parse::<i64>().ok();
                        if after.is_none() {
                            return response(ApiError::invalid());
                        }
                    }
                    Some(("limit", v)) if limit.is_none() => {
                        limit = v.parse::<usize>().ok();
                        if limit.is_none() {
                            return response(ApiError::invalid());
                        }
                    }
                    _ => return response(ApiError::invalid()),
                }
            }
            let Some(limit) = limit else {
                return response(ApiError::invalid());
            };
            (
                "runs_events",
                json!({"run_id":id,"after":after,"limit":limit}),
            )
        }
        (&Method::POST, ["", "v1", "runs", id, "cancel"]) => {
            if body != json!({}) {
                return response(ApiError::invalid());
            }
            ("runs_cancel", json!({"run_id":id}))
        }
        _ => return response(ApiError::new(StatusCode::NOT_FOUND, "not_found")),
    };
    if name != "runs_events"
        && !(name == "skills_list" && method == Method::GET)
        && request.uri().query().is_some()
    {
        return response(ApiError::invalid());
    }
    match wire::execute(
        &shared.handle,
        &principal.client,
        name,
        args,
        shared.limits.max_response_bytes,
    )
    .await
    {
        Ok(value) => {
            let status = if name == "agents_start" && value["replay"] == false {
                StatusCode::ACCEPTED
            } else {
                StatusCode::OK
            };
            let mut r = json_response(status, &value);
            if name == "agents_start"
                && let Some(id) = value["run_id"].as_str()
                && let Ok(location) = HeaderValue::from_str(&format!("/v1/runs/{id}"))
            {
                r.headers_mut().insert(header::LOCATION, location);
            }
            r
        }
        Err(e) => response(e),
    }
}

#[cfg(test)]
mod tests {
    use tracing::{
        Event, Metadata, Subscriber,
        span::{Attributes, Id, Record},
    };
    struct TestSubscriber(bool, Option<tracing::Level>);
    impl Subscriber for TestSubscriber {
        fn enabled(&self, metadata: &Metadata<'_>) -> bool {
            self.0 && self.1.is_none_or(|level| *metadata.level() == level)
        }
        fn new_span(&self, _: &Attributes<'_>) -> Id {
            Id::from_u64(1)
        }
        fn record(&self, _: &Id, _: &Record<'_>) {}
        fn record_follows_from(&self, _: &Id, _: &Id) {}
        fn event(&self, _: &Event<'_>) {}
        fn enter(&self, _: &Id) {}
        fn exit(&self, _: &Id) {}
    }
    #[test]
    fn sdk_logging_guard_fails_closed_for_an_enabled_subscriber() {
        tracing::subscriber::with_default(TestSubscriber(true, None), || {
            assert!(super::unsafe_sdk_logging_enabled())
        });
        tracing::subscriber::with_default(
            TestSubscriber(true, Some(tracing::Level::DEBUG)),
            || assert!(super::unsafe_sdk_logging_enabled()),
        );
        tracing::subscriber::with_default(TestSubscriber(false, None), || {
            assert!(!super::unsafe_sdk_logging_enabled())
        });
    }
}
