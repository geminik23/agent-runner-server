use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fmt,
    sync::{Arc, RwLock},
};

use subtle::ConstantTimeEq;
use zeroize::Zeroizing;

use crate::storage::{Namespace, StorageError};

use super::request::{AuthorizedStart, StartBody};

const CREDENTIAL_BYTES: usize = 32;

/// Holds an operator-provided credential without exposing a formatting interface.
pub struct CredentialSecret(Zeroizing<[u8; CREDENTIAL_BYTES]>);

impl CredentialSecret {
    /// Accepts secret bytes from a trusted source; generation is the operator's responsibility.
    pub fn new(bytes: [u8; CREDENTIAL_BYTES]) -> Self {
        Self(Zeroizing::new(bytes))
    }
}

/// Lists client operations that can be granted independently.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Scope {
    RunStart,
    RunRead,
    RunCancel,
    SessionRead,
    SkillRead,
    SkillWrite,
    HookRead,
    HookWrite,
    InboxRead,
    InboxAck,
}

/// Sets request bounds supplied by the trusted application configuration.
#[derive(Clone, Copy)]
pub struct AccessLimits {
    pub max_request_bytes: usize,
    pub max_skill_bytes: usize,
    pub max_identifier_bytes: usize,
}

/// Describes a server-defined synthetic profile with opaque workspace IDs.
pub struct ExecutionProfile {
    id: String,
    workspaces: HashSet<String>,
    max_input_bytes: usize,
}

impl ExecutionProfile {
    /// Creates a profile that permits only the dummy runtime.
    pub fn dummy(
        id: impl Into<String>,
        workspaces: impl IntoIterator<Item = String>,
        max_input_bytes: usize,
    ) -> Self {
        Self {
            id: id.into(),
            workspaces: workspaces.into_iter().collect(),
            max_input_bytes,
        }
    }
}

/// Associates an operator-provided credential with a namespace and its grants.
pub struct ClientRegistration {
    credential_id: String,
    secret: CredentialSecret,
    namespace: Namespace,
    scopes: HashSet<Scope>,
    profiles: HashSet<String>,
}

impl ClientRegistration {
    /// Builds a registration; it must come from trusted server configuration.
    pub fn new(
        credential_id: impl Into<String>,
        secret: CredentialSecret,
        namespace: Namespace,
        scopes: impl IntoIterator<Item = Scope>,
        profiles: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            credential_id: credential_id.into(),
            secret,
            namespace,
            scopes: scopes.into_iter().collect(),
            profiles: profiles.into_iter().collect(),
        }
    }
}

struct Registry {
    credentials: HashMap<String, ClientRegistration>,
    profiles: HashMap<String, ExecutionProfile>,
}

/// Authenticates registered clients and applies server-owned grants.
pub struct ClientAccessControl {
    authority: Arc<()>,
    registry: RwLock<Registry>,
    limits: AccessLimits,
}

/// Identifies an authenticated credential without retaining its secret.
#[derive(Clone)]
pub struct AuthenticatedClient {
    authority: Arc<()>,
    credential_id: String,
}

/// Reports access failures without including credentials or request content.
#[derive(Debug)]
pub enum AccessError {
    Unauthenticated,
    Forbidden,
    InvalidRequest,
    InvalidConfiguration,
    Unavailable,
    Storage(StorageError),
}

impl fmt::Display for AccessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unauthenticated => formatter.write_str("authentication failed"),
            Self::Forbidden => formatter.write_str("access denied"),
            Self::InvalidRequest => formatter.write_str("invalid request"),
            Self::InvalidConfiguration => formatter.write_str("invalid access configuration"),
            Self::Unavailable => formatter.write_str("access control unavailable"),
            Self::Storage(_) => formatter.write_str("storage operation failed"),
        }
    }
}

impl Error for AccessError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Storage(error) => Some(error),
            _ => None,
        }
    }
}

impl From<StorageError> for AccessError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

impl ClientAccessControl {
    /// Validates trusted registrations, profile grants, and caller-supplied limits.
    pub fn new(
        registrations: impl IntoIterator<Item = ClientRegistration>,
        profiles: impl IntoIterator<Item = ExecutionProfile>,
        limits: AccessLimits,
    ) -> Result<Self, AccessError> {
        if limits.max_request_bytes == 0
            || limits.max_skill_bytes == 0
            || limits.max_identifier_bytes == 0
        {
            return Err(AccessError::InvalidConfiguration);
        }
        let mut profile_map = HashMap::new();
        for profile in profiles {
            if !valid_identifier(&profile.id)
                || profile.id.len() > limits.max_identifier_bytes
                || profile.max_input_bytes == 0
                || profile.workspaces.is_empty()
                || profile
                    .workspaces
                    .iter()
                    .any(|id| !valid_identifier(id) || id.len() > limits.max_identifier_bytes)
            {
                return Err(AccessError::InvalidConfiguration);
            }
            if profile_map.insert(profile.id.clone(), profile).is_some() {
                return Err(AccessError::InvalidConfiguration);
            }
        }
        let mut credentials = HashMap::new();
        for registration in registrations {
            if !valid_identifier(&registration.credential_id)
                || registration.namespace.owner_id.is_empty()
                || registration.namespace.client_id.is_empty()
                || registration
                    .profiles
                    .iter()
                    .any(|id| !profile_map.contains_key(id))
            {
                return Err(AccessError::InvalidConfiguration);
            }
            if credentials
                .insert(registration.credential_id.clone(), registration)
                .is_some()
            {
                return Err(AccessError::InvalidConfiguration);
            }
        }
        Ok(Self {
            authority: Arc::new(()),
            registry: RwLock::new(Registry {
                credentials,
                profiles: profile_map,
            }),
            limits,
        })
    }

    /// Authenticates fixed-size secret bytes using a constant-time comparison.
    pub fn authenticate(
        &self,
        credential_id: &str,
        proof: &[u8],
    ) -> Result<AuthenticatedClient, AccessError> {
        let registry = self.registry.read().map_err(|_| AccessError::Unavailable)?;
        let registration = if valid_identifier(credential_id) {
            registry.credentials.get(credential_id)
        } else {
            None
        };
        let fallback = [0_u8; CREDENTIAL_BYTES];
        let expected = registration.map_or(&fallback[..], |entry| &entry.secret.0[..]);
        let candidate = if proof.len() == CREDENTIAL_BYTES {
            proof
        } else {
            &fallback[..]
        };
        let matches = bool::from(expected.ct_eq(candidate));
        if proof.len() != CREDENTIAL_BYTES || registration.is_none() || !matches {
            return Err(AccessError::Unauthenticated);
        }
        Ok(AuthenticatedClient {
            authority: Arc::clone(&self.authority),
            credential_id: credential_id.to_owned(),
        })
    }

    /// Removes a credential and invalidates future checks using existing contexts.
    pub fn revoke(&self, credential_id: &str) -> Result<bool, AccessError> {
        let mut registry = self
            .registry
            .write()
            .map_err(|_| AccessError::Unavailable)?;
        Ok(registry.credentials.remove(credential_id).is_some())
    }

    /// Checks an operation without performing the operation itself.
    pub fn check_scope(
        &self,
        client: &AuthenticatedClient,
        scope: Scope,
    ) -> Result<(), AccessError> {
        self.with_permissions(client, &[scope], |_, _| Ok(()))
    }

    /// Validates a new dummy request without writing to storage or starting a backend.
    pub fn authorize_start(
        &self,
        client: &AuthenticatedClient,
        body: &[u8],
    ) -> Result<AuthorizedStart, AccessError> {
        let registry = self.registry.read().map_err(|_| AccessError::Unavailable)?;
        let registration = self.registration(&registry, client, &[Scope::RunStart])?;
        if body.len() > self.limits.max_request_bytes {
            return Err(AccessError::InvalidRequest);
        }
        let request: StartBody =
            serde_json::from_slice(body).map_err(|_| AccessError::InvalidRequest)?;
        if !valid_identifier(&request.profile_id)
            || !valid_identifier(&request.workspace_id)
            || request.profile_id.len() > self.limits.max_identifier_bytes
            || request.workspace_id.len() > self.limits.max_identifier_bytes
            || request.idempotency_key.is_empty()
            || request.idempotency_key.len() > self.limits.max_identifier_bytes
        {
            return Err(AccessError::InvalidRequest);
        }
        let profile = registry
            .profiles
            .get(&request.profile_id)
            .ok_or(AccessError::Forbidden)?;
        if !registration.profiles.contains(&request.profile_id)
            || !profile.workspaces.contains(&request.workspace_id)
        {
            return Err(AccessError::Forbidden);
        }
        if request.input.request.len() > profile.max_input_bytes {
            return Err(AccessError::InvalidRequest);
        }
        Ok(AuthorizedStart::from_body(request))
    }

    pub(super) fn with_permissions<T>(
        &self,
        client: &AuthenticatedClient,
        required: &[Scope],
        operation: impl FnOnce(&Namespace, &AccessLimits) -> Result<T, AccessError>,
    ) -> Result<T, AccessError> {
        let registry = self.registry.read().map_err(|_| AccessError::Unavailable)?;
        let registration = self.registration(&registry, client, required)?;
        operation(&registration.namespace, &self.limits)
    }

    fn registration<'a>(
        &self,
        registry: &'a Registry,
        client: &AuthenticatedClient,
        required: &[Scope],
    ) -> Result<&'a ClientRegistration, AccessError> {
        if !Arc::ptr_eq(&self.authority, &client.authority) {
            return Err(AccessError::Unauthenticated);
        }
        let registration = registry
            .credentials
            .get(&client.credential_id)
            .ok_or(AccessError::Unauthenticated)?;
        if required
            .iter()
            .any(|scope| !registration.scopes.contains(scope))
        {
            return Err(AccessError::Forbidden);
        }
        Ok(registration)
    }
}

pub(super) fn validate_resource_id(value: &str, limits: &AccessLimits) -> Result<(), AccessError> {
    if value.is_empty() || value.len() > limits.max_identifier_bytes {
        Err(AccessError::InvalidRequest)
    } else {
        Ok(())
    }
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, TryLockError};

    use super::{
        AccessError, AccessLimits, ClientAccessControl, ClientRegistration, CredentialSecret, Scope,
    };
    use crate::storage::Namespace;

    fn controller() -> ClientAccessControl {
        ClientAccessControl::new(
            [ClientRegistration::new(
                "client",
                CredentialSecret::new([7; 32]),
                Namespace {
                    owner_id: "owner".into(),
                    client_id: "client".into(),
                },
                [Scope::RunRead],
                Vec::<String>::new(),
            )],
            [],
            AccessLimits {
                max_request_bytes: 256,
                max_skill_bytes: 128,
                max_identifier_bytes: 64,
            },
        )
        .unwrap()
    }

    #[test]
    fn authorization_holds_the_registry_read_lock_during_an_operation() {
        let control = controller();
        let client = control.authenticate("client", &[7; 32]).unwrap();
        control
            .with_permissions(&client, &[Scope::RunRead], |_, _| {
                assert!(matches!(
                    control.registry.try_write(),
                    Err(TryLockError::WouldBlock)
                ));
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn poisoned_registry_fails_closed() {
        let control = Arc::new(controller());
        let client = control.authenticate("client", &[7; 32]).unwrap();
        let other = Arc::clone(&control);
        let outcome = std::thread::spawn(move || {
            let _guard = other.registry.write().unwrap();
            panic!("poison registry for test");
        })
        .join();
        assert!(outcome.is_err());
        assert!(matches!(
            control.authenticate("client", &[7; 32]),
            Err(AccessError::Unavailable)
        ));
        assert!(matches!(
            control.check_scope(&client, Scope::RunRead),
            Err(AccessError::Unavailable)
        ));
        assert!(matches!(
            control.revoke("client"),
            Err(AccessError::Unavailable)
        ));
    }
}
