//! Trusted JSON configuration and protected local credential supply.

use std::{
    fs::File,
    io::Read,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use serde::Deserialize;
use zeroize::Zeroizing;

use super::{Server, ServerError, ServerLimits, decode_secret, wire};
use crate::{
    access::{
        AccessLimits, ClientAccessControl, ClientRegistration, CredentialSecret, ExecutionProfile,
        Scope,
    },
    application::{BackendProfile, RetentionConfig, RunLimits, RunService},
    runtimes::dummy::{DummyBackend, DummyScenario},
    storage::Namespace,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Configuration {
    schema_version: u32,
    listen: SocketAddr,
    database: PathBuf,
    access: AccessSettings,
    run: RunSettings,
    transport: TransportSettings,
    clients: Vec<ClientSettings>,
    profiles: Vec<ProfileSettings>,
    #[serde(default)]
    delivery: Option<crate::delivery::DeliveryConfig>,
    #[serde(default)]
    retention: Option<RetentionConfig>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AccessSettings {
    max_request_bytes: usize,
    max_skill_bytes: usize,
    max_identifier_bytes: usize,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RunSettings {
    max_outstanding: usize,
    max_workers: usize,
    database_queue: usize,
    event_buffer: usize,
    max_events_per_run: usize,
    max_event_bytes: usize,
    max_result_bytes: usize,
    max_page_size: usize,
    run_timeout_ms: u64,
    cleanup_grace_ms: u64,
    poll_interval_ms: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TransportSettings {
    max_request_bytes: usize,
    max_response_bytes: usize,
    max_header_bytes: usize,
    max_inflight: usize,
    max_connections: usize,
    connection_idle_timeout_ms: u64,
    connection_max_lifetime_ms: u64,
    requests_per_second: u32,
    burst: u32,
    request_timeout_ms: u64,
    shutdown_grace_ms: u64,
    max_mcp_sessions: usize,
    mcp_session_idle_ms: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ClientSettings {
    credential_id: String,
    generation: String,
    secret_file: PathBuf,
    owner_id: String,
    client_id: String,
    scopes: Vec<Scope>,
    profiles: Vec<String>,
    #[serde(default)]
    destinations: Vec<String>,
    #[serde(default)]
    reconciliation_destinations: Vec<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProfileSettings {
    profile_id: String,
    revision: String,
    workspaces: Vec<String>,
    max_input_bytes: usize,
    scenario: Scenario,
}
#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
enum Scenario {
    Success,
    DelayedSuccess,
    StructuredSuccess,
    Warning,
    MixedWarnings,
    PartialWarnings,
    Incomplete,
    Failure,
}

/// Fully checked server assembly inputs; secret bytes are held only by access control.
pub struct PreparedServer {
    listen: SocketAddr,
    database: PathBuf,
    access: Arc<ClientAccessControl>,
    profiles: Vec<BackendProfile>,
    runs: RunLimits,
    transport: ServerLimits,
    delivery: Option<crate::delivery::DeliveryConfig>,
    retention: Option<RetentionConfig>,
}
impl PreparedServer {
    /// Loads paths relative to the configuration file, never relative to a client request.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ServerError> {
        let path = path.as_ref();
        let root = path.parent().unwrap_or(Path::new("."));
        let file = File::open(path).map_err(|_| ServerError::InvalidConfiguration)?;
        let mut bytes = Vec::new();
        file.take(1024 * 1024 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| ServerError::InvalidConfiguration)?;
        if bytes.len() > 1024 * 1024 {
            return Err(ServerError::InvalidConfiguration);
        }
        let value = wire::parse_json(&bytes).map_err(|_| ServerError::InvalidConfiguration)?;
        let config: Configuration =
            serde_json::from_value(value).map_err(|_| ServerError::InvalidConfiguration)?;
        if config.schema_version != 1
            || !config.listen.ip().is_loopback()
            || config.listen.port() == 0
            || config.database.as_os_str().is_empty()
            || config.clients.is_empty()
            || config.profiles.is_empty()
        {
            return Err(ServerError::InvalidConfiguration);
        }
        if config
            .retention
            .as_ref()
            .is_some_and(|retention| retention.milliseconds().is_err())
        {
            return Err(ServerError::InvalidConfiguration);
        }
        if config.delivery.as_ref().is_some_and(|delivery| {
            !delivery.validate(
                config.access.max_identifier_bytes,
                config.run.max_event_bytes,
            )
        }) || config.clients.iter().any(|client| {
            client.destinations.iter().any(|id| {
                config.delivery.as_ref().is_none_or(|delivery| {
                    !delivery
                        .destinations
                        .iter()
                        .any(|destination| destination.id == *id)
                })
            })
        }) {
            return Err(ServerError::InvalidConfiguration);
        }
        let r = config.run;
        let runs = RunLimits {
            max_outstanding: r.max_outstanding,
            max_workers: r.max_workers,
            database_queue: r.database_queue,
            event_buffer: r.event_buffer,
            max_events_per_run: r.max_events_per_run,
            max_event_bytes: r.max_event_bytes,
            max_result_bytes: r.max_result_bytes,
            max_page_size: r.max_page_size,
            run_timeout: Duration::from_millis(r.run_timeout_ms),
            cleanup_grace: Duration::from_millis(r.cleanup_grace_ms),
            poll_interval: Duration::from_millis(r.poll_interval_ms),
        };
        let t = config.transport;
        let transport = ServerLimits {
            max_request_bytes: t.max_request_bytes,
            max_response_bytes: t.max_response_bytes,
            max_header_bytes: t.max_header_bytes,
            max_inflight: t.max_inflight,
            max_connections: t.max_connections,
            connection_idle_timeout: Duration::from_millis(t.connection_idle_timeout_ms),
            connection_max_lifetime: Duration::from_millis(t.connection_max_lifetime_ms),
            requests_per_second: t.requests_per_second,
            burst: t.burst,
            request_timeout: Duration::from_millis(t.request_timeout_ms),
            shutdown_grace: Duration::from_millis(t.shutdown_grace_ms),
            max_mcp_sessions: t.max_mcp_sessions,
            mcp_session_idle: Duration::from_millis(t.mcp_session_idle_ms),
        };
        let response_min = r
            .max_event_bytes
            .checked_mul(r.max_page_size)
            .map(|n| n.max(r.max_result_bytes))
            .map(|n| {
                n.max(
                    config
                        .delivery
                        .as_ref()
                        .map_or(0, |delivery| delivery.max_payload_bytes),
                )
            })
            .and_then(|n| n.checked_mul(16))
            .and_then(|n| n.checked_add(8192))
            .ok_or(ServerError::InvalidConfiguration)?;
        if !transport.validate()
            || transport.max_request_bytes < config.access.max_request_bytes.saturating_add(4096)
            || transport.max_response_bytes < response_min
        {
            return Err(ServerError::InvalidConfiguration);
        }
        let mut profiles = Vec::new();
        let grants = config
            .profiles
            .into_iter()
            .map(|p| {
                let scenario = match p.scenario {
                    Scenario::Success => DummyScenario::Success,
                    Scenario::DelayedSuccess => DummyScenario::DelayedSuccess,
                    Scenario::StructuredSuccess => DummyScenario::StructuredSuccess,
                    Scenario::Warning => DummyScenario::Warning,
                    Scenario::MixedWarnings => DummyScenario::MixedWarnings,
                    Scenario::PartialWarnings => DummyScenario::PartialWarnings,
                    Scenario::Incomplete => DummyScenario::Incomplete,
                    Scenario::Failure => DummyScenario::Failure,
                };
                profiles.push(BackendProfile {
                    profile_id: p.profile_id.clone(),
                    revision: p.revision,
                    state_realm: "dummy".into(),
                    backend: Arc::new(DummyBackend::new(scenario, 0)),
                });
                ExecutionProfile::dummy(p.profile_id, p.workspaces, p.max_input_bytes)
            })
            .collect::<Vec<_>>();
        let registrations = config
            .clients
            .into_iter()
            .map(|c| {
                let secret = read_secret(&root.join(c.secret_file))?;
                Ok(ClientRegistration::new(
                    c.credential_id,
                    CredentialSecret::new(*secret),
                    Namespace {
                        owner_id: c.owner_id,
                        client_id: c.client_id,
                    },
                    c.scopes,
                    c.profiles,
                )
                .with_generation(c.generation)
                .with_destinations(c.destinations)
                .with_reconciliation_destinations(c.reconciliation_destinations))
            })
            .collect::<Result<Vec<_>, ServerError>>()?;
        let a = config.access;
        let access = Arc::new(
            ClientAccessControl::new(
                registrations,
                grants,
                AccessLimits {
                    max_request_bytes: a.max_request_bytes,
                    max_skill_bytes: a.max_skill_bytes,
                    max_identifier_bytes: a.max_identifier_bytes,
                },
            )
            .map_err(|_| ServerError::InvalidConfiguration)?,
        );
        Ok(Self {
            listen: config.listen,
            database: root.join(config.database),
            access,
            profiles,
            runs,
            transport,
            delivery: config.delivery,
            retention: config.retention,
        })
    }
    pub async fn start(self) -> Result<Server, ServerError> {
        let service = RunService::open_with_retention(
            self.database,
            self.access,
            self.profiles,
            self.runs,
            self.delivery,
            self.retention,
        )
        .await
        .map_err(ServerError::Service)?;
        Server::bind(self.listen, service, self.transport).await
    }
}
fn read_secret(path: &Path) -> Result<Zeroizing<[u8; 32]>, ServerError> {
    let meta = std::fs::symlink_metadata(path).map_err(|_| ServerError::InvalidConfiguration)?;
    if !meta.is_file() || meta.file_type().is_symlink() {
        return Err(ServerError::InvalidConfiguration);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if meta.permissions().mode() & 0o077 != 0 {
            return Err(ServerError::InvalidConfiguration);
        }
    }
    let mut bytes = Zeroizing::new(Vec::new());
    File::open(path)
        .map_err(|_| ServerError::InvalidConfiguration)?
        .take(67)
        .read_to_end(&mut bytes)
        .map_err(|_| ServerError::InvalidConfiguration)?;
    let text = std::str::from_utf8(&bytes).map_err(|_| ServerError::InvalidConfiguration)?;
    let text = text.strip_suffix('\n').unwrap_or(text);
    let text = text.strip_suffix('\r').unwrap_or(text);
    decode_secret(text).ok_or(ServerError::InvalidConfiguration)
}
