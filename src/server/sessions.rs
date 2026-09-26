use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU16, Ordering},
    },
    time::{Duration, Instant},
};

use futures::Stream;
use rmcp::{
    model::{ClientJsonRpcMessage, ServerJsonRpcMessage},
    transport::streamable_http_server::session::{
        ServerSseMessage, SessionId, SessionManager,
        local::{LocalSessionManager, LocalSessionManagerError},
    },
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::access::ClientIdentity;

pub(super) struct SessionContext {
    pub identity: ClientIdentity,
    pub http_status: Arc<AtomicU16>,
}
tokio::task_local! {
    pub(super) static PRINCIPAL: SessionContext;
}

struct Binding {
    identity: ClientIdentity,
    version: Option<String>,
    last_used: Instant,
    requests: HashSet<String>,
    cancelled_requests: HashSet<String>,
    retiring: bool,
    _permit: OwnedSemaphorePermit,
}

/// Adds bounded ownership to SDK sessions without storing business execution state.
pub(super) struct BoundSessions {
    inner: Arc<LocalSessionManager>,
    bindings: Mutex<HashMap<SessionId, Binding>>,
    slots: Arc<Semaphore>,
    idle: Duration,
    init_timeout: Duration,
}

#[derive(Debug)]
pub(super) struct SessionError;
impl fmt::Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("MCP session unavailable")
    }
}
impl std::error::Error for SessionError {}
impl From<LocalSessionManagerError> for SessionError {
    fn from(_: LocalSessionManagerError) -> Self {
        Self
    }
}

impl BoundSessions {
    pub fn new(max_sessions: usize, idle: Duration, init_timeout: Duration) -> Self {
        let mut inner = LocalSessionManager::default();
        inner.session_config.keep_alive = Some(idle);
        inner.session_config.init_timeout = Some(init_timeout);
        inner.session_config.completed_cache_ttl = Duration::ZERO;
        inner.session_config.sse_retry = None;
        Self {
            inner: Arc::new(inner),
            bindings: Mutex::new(HashMap::new()),
            slots: Arc::new(Semaphore::new(max_sessions)),
            idle,
            init_timeout,
        }
    }
    fn authorize(&self, id: &SessionId) -> Result<(), SessionError> {
        let identity = PRINCIPAL
            .try_with(|context| context.identity.clone())
            .map_err(|_| SessionError)?;
        let mut bindings = self.bindings.lock().map_err(|_| SessionError)?;
        let binding = bindings.get_mut(id).ok_or(SessionError)?;
        if binding.identity != identity || binding.last_used.elapsed() >= self.idle {
            return Err(SessionError);
        }
        binding.last_used = Instant::now();
        Ok(())
    }
    pub fn check_version(
        &self,
        id: &SessionId,
        identity: &ClientIdentity,
        version: Option<&str>,
    ) -> bool {
        let Ok(bindings) = self.bindings.lock() else {
            return false;
        };
        bindings.get(id).is_some_and(|b| {
            b.identity == *identity
                && !b.retiring
                && b.last_used.elapsed() < self.idle
                && version.is_some()
                && b.version.as_deref() == version
        })
    }
    pub fn begin_request(
        self: &Arc<Self>,
        id: &SessionId,
        identity: &ClientIdentity,
        request_id: String,
    ) -> Result<RequestLease, SessionError> {
        let mut bindings = self.bindings.lock().map_err(|_| SessionError)?;
        let binding = bindings.get_mut(id).ok_or(SessionError)?;
        if binding.identity != *identity
            || binding.retiring
            || binding.last_used.elapsed() >= self.idle
            || !binding.requests.insert(request_id.clone())
        {
            return Err(SessionError);
        }
        Ok(RequestLease {
            sessions: Arc::clone(self),
            session_id: id.clone(),
            request_id,
            completed: false,
        })
    }

    pub fn begin_cancellation(
        self: &Arc<Self>,
        id: &SessionId,
        identity: &ClientIdentity,
        request_id: Option<String>,
    ) -> Result<SessionRetireGuard, SessionError> {
        let mut bindings = self.bindings.lock().map_err(|_| SessionError)?;
        let binding = bindings.get_mut(id).ok_or(SessionError)?;
        if binding.identity != *identity || binding.retiring {
            return Err(SessionError);
        }
        binding.retiring = true;
        if let Some(request_id) = request_id
            && binding.requests.contains(&request_id)
        {
            binding.cancelled_requests.insert(request_id);
        }
        Ok(SessionRetireGuard {
            sessions: Arc::clone(self),
            session_id: id.clone(),
        })
    }

    fn retire_after_drop(&self, id: &SessionId) {
        let binding = self
            .bindings
            .lock()
            .ok()
            .and_then(|mut entries| entries.remove(id));
        if let Some(binding) = binding {
            let inner = Arc::clone(&self.inner);
            let id = id.clone();
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn(async move {
                    let _ = inner.close_session(&id).await;
                    drop(binding);
                });
            }
        }
    }

    async fn retire(&self, id: &SessionId) -> Result<(), SessionError> {
        let binding = self.bindings.lock().map_err(|_| SessionError)?.remove(id);
        let inner = Arc::clone(&self.inner);
        let id = id.clone();
        tokio::spawn(async move {
            let result = inner.close_session(&id).await.map_err(SessionError::from);
            drop(binding);
            result
        })
        .await
        .map_err(|_| SessionError)?
    }

    pub async fn expire(&self, all: bool) -> Result<(), SessionError> {
        let ids = {
            let bindings = self.bindings.lock().map_err(|_| SessionError)?;
            bindings
                .iter()
                .filter(|(_, b)| {
                    all || b.last_used.elapsed() >= self.idle
                        || (b.version.is_none() && b.last_used.elapsed() >= self.init_timeout)
                })
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>()
        };
        for id in ids {
            self.retire(&id).await?;
        }
        Ok(())
    }
}

pub(super) struct RequestLease {
    sessions: Arc<BoundSessions>,
    session_id: SessionId,
    request_id: String,
    completed: bool,
}
impl RequestLease {
    pub fn finish(mut self, response: &[u8]) {
        let expected_id = serde_json::from_str::<serde_json::Value>(&self.request_id).ok();
        let matches_terminal = |candidate: &[u8]| {
            serde_json::from_slice::<serde_json::Value>(candidate)
                .ok()
                .is_some_and(|value| {
                    expected_id
                        .as_ref()
                        .is_some_and(|id| value.get("id") == Some(id))
                        && (value.get("result").is_some() || value.get("error").is_some())
                })
        };
        let terminal = matches_terminal(response)
            || std::str::from_utf8(response).ok().is_some_and(|text| {
                text.lines()
                    .filter_map(|line| line.strip_prefix("data:"))
                    .any(|data| matches_terminal(data.trim_start().as_bytes()))
            });
        let cancelled = self
            .sessions
            .bindings
            .lock()
            .ok()
            .and_then(|bindings| {
                bindings
                    .get(&self.session_id)
                    .map(|binding| binding.cancelled_requests.contains(&self.request_id))
            })
            .unwrap_or(true);
        self.completed = terminal && !cancelled;
    }
}
impl Drop for RequestLease {
    fn drop(&mut self) {
        if self.completed {
            if let Ok(mut bindings) = self.sessions.bindings.lock()
                && let Some(binding) = bindings.get_mut(&self.session_id)
            {
                binding.requests.remove(&self.request_id);
                binding.cancelled_requests.remove(&self.request_id);
            }
        } else {
            self.sessions.retire_after_drop(&self.session_id);
        }
    }
}

pub(super) struct SessionRetireGuard {
    sessions: Arc<BoundSessions>,
    session_id: SessionId,
}
impl Drop for SessionRetireGuard {
    fn drop(&mut self) {
        self.sessions.retire_after_drop(&self.session_id);
    }
}

impl SessionManager for BoundSessions {
    type Error = SessionError;
    type Transport = <LocalSessionManager as SessionManager>::Transport;
    async fn create_session(&self) -> Result<(SessionId, Self::Transport), Self::Error> {
        let identity = PRINCIPAL
            .try_with(|context| context.identity.clone())
            .map_err(|_| SessionError)?;
        let permit = Arc::clone(&self.slots).try_acquire_owned().map_err(|_| {
            let _ = PRINCIPAL.try_with(|context| context.http_status.store(429, Ordering::Release));
            SessionError
        })?;
        let (id, transport) = self.inner.create_session().await?;
        self.bindings.lock().map_err(|_| SessionError)?.insert(
            id.clone(),
            Binding {
                identity,
                version: None,
                last_used: Instant::now(),
                requests: HashSet::new(),
                cancelled_requests: HashSet::new(),
                retiring: false,
                _permit: permit,
            },
        );
        Ok((id, transport))
    }
    async fn initialize_session(
        &self,
        id: &SessionId,
        message: ClientJsonRpcMessage,
    ) -> Result<ServerJsonRpcMessage, Self::Error> {
        self.authorize(id)?;
        let response = self.inner.initialize_session(id, message).await?;
        let value = serde_json::to_value(&response).map_err(|_| SessionError)?;
        if let Some(binding) = self.bindings.lock().map_err(|_| SessionError)?.get_mut(id) {
            binding.version = value
                .pointer("/result/protocolVersion")
                .and_then(|v| v.as_str())
                .map(str::to_owned);
        }
        Ok(response)
    }
    async fn has_session(&self, id: &SessionId) -> Result<bool, Self::Error> {
        if self.authorize(id).is_err() {
            return Ok(false);
        }
        Ok(self.inner.has_session(id).await?)
    }
    async fn close_session(&self, id: &SessionId) -> Result<(), Self::Error> {
        if PRINCIPAL.try_with(|_| ()).is_ok() {
            self.authorize(id)?;
        }
        self.retire(id).await
    }
    async fn create_stream(
        &self,
        id: &SessionId,
        message: ClientJsonRpcMessage,
    ) -> Result<impl Stream<Item = ServerSseMessage> + Send + 'static, Self::Error> {
        self.authorize(id)?;
        Ok(self.inner.create_stream(id, message).await?)
    }
    async fn accept_message(
        &self,
        id: &SessionId,
        message: ClientJsonRpcMessage,
    ) -> Result<(), Self::Error> {
        self.authorize(id)?;
        Ok(self.inner.accept_message(id, message).await?)
    }
    async fn create_standalone_stream(
        &self,
        id: &SessionId,
    ) -> Result<impl Stream<Item = ServerSseMessage> + Send + 'static, Self::Error> {
        self.authorize(id)?;
        Ok(self.inner.create_standalone_stream(id).await?)
    }
    async fn resume(
        &self,
        id: &SessionId,
        last_event_id: String,
    ) -> Result<impl Stream<Item = ServerSseMessage> + Send + 'static, Self::Error> {
        self.authorize(id)?;
        Ok(self.inner.resume(id, last_event_id).await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Namespace;

    fn identity(id: &str) -> SessionContext {
        SessionContext {
            identity: ClientIdentity {
                credential_id: id.into(),
                generation: "generation".into(),
                namespace: Namespace {
                    owner_id: "owner".into(),
                    client_id: "client".into(),
                },
            },
            http_status: Arc::new(AtomicU16::new(0)),
        }
    }

    #[tokio::test]
    async fn internal_worker_cleanup_releases_the_slot_without_bypassing_external_ownership() {
        let sessions = Arc::new(BoundSessions::new(
            1,
            Duration::from_secs(10),
            Duration::from_secs(1),
        ));
        let (id, _transport) = PRINCIPAL
            .scope(identity("first"), sessions.create_session())
            .await
            .unwrap();
        assert!(
            PRINCIPAL
                .scope(identity("other"), sessions.close_session(&id))
                .await
                .is_err()
        );
        assert!(
            PRINCIPAL
                .scope(identity("other"), sessions.create_session())
                .await
                .is_err()
        );
        tokio::time::timeout(Duration::from_secs(1), sessions.close_session(&id))
            .await
            .unwrap()
            .unwrap();
        let (next, _transport) = PRINCIPAL
            .scope(identity("other"), sessions.create_session())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), sessions.close_session(&next))
            .await
            .unwrap()
            .unwrap();
    }
}
