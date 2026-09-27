use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fmt,
    sync::{Arc, RwLock},
};

use subtle::ConstantTimeEq;
use zeroize::Zeroizing;

use crate::storage::{Namespace, StorageError};

use super::request::{AuthorizedStart, StartBody, StartSession};

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
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
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
    DeliveryReconcile,
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
    generation: Option<String>,
    secret: CredentialSecret,
    namespace: Namespace,
    scopes: HashSet<Scope>,
    profiles: HashSet<String>,
    destinations: HashSet<String>,
    reconciliation_destinations: HashSet<String>,
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
            generation: None,
            secret,
            namespace,
            scopes: scopes.into_iter().collect(),
            profiles: profiles.into_iter().collect(),
            destinations: HashSet::new(),
            reconciliation_destinations: HashSet::new(),
        }
    }

    /// Grants access to server-owned destination identifiers.
    pub fn with_destinations(mut self, destinations: impl IntoIterator<Item = String>) -> Self {
        self.destinations = destinations.into_iter().collect();
        self
    }

    /// Grants reconciliation without enabling a destination for new deliveries.
    pub fn with_reconciliation_destinations(
        mut self,
        destinations: impl IntoIterator<Item = String>,
    ) -> Self {
        self.reconciliation_destinations = destinations.into_iter().collect();
        self
    }

    /// Sets a stable, non-secret registration generation supplied by trusted configuration.
    /// A replacement or reissued credential must use a new generation.
    pub fn with_generation(mut self, generation: impl Into<String>) -> Self {
        self.generation = Some(generation.into());
        self
    }
}

pub(crate) struct LaunchAuthorization<'a> {
    pub credential_id: &'a str,
    pub generation: &'a str,
    pub namespace: &'a Namespace,
    pub profile_id: &'a str,
    pub workspace_id: &'a str,
    pub input_len: usize,
    pub resume: bool,
    pub requires_skill_read: bool,
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

/// Binds a transport session to a current registration, never to its secret.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientIdentity {
    pub credential_id: String,
    pub generation: String,
    pub namespace: Namespace,
}

/// Describes only the profiles and workspaces granted to the current caller.
#[derive(Clone, Debug)]
pub struct ProfileGrant {
    pub profile_id: String,
    pub workspaces: Vec<String>,
    pub max_input_bytes: usize,
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
                || registration
                    .generation
                    .as_deref()
                    .is_some_and(|value| !valid_identifier(value))
                || registration.namespace.owner_id.is_empty()
                || registration.namespace.client_id.is_empty()
                || registration
                    .profiles
                    .iter()
                    .any(|id| !profile_map.contains_key(id))
                || registration
                    .destinations
                    .iter()
                    .any(|id| id.is_empty() || id.len() > limits.max_identifier_bytes)
                || registration
                    .reconciliation_destinations
                    .iter()
                    .any(|id| id.is_empty() || id.len() > limits.max_identifier_bytes)
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

    /// Returns a non-secret binding after checking the current registration.
    pub fn identity(&self, client: &AuthenticatedClient) -> Result<ClientIdentity, AccessError> {
        let registry = self.registry.read().map_err(|_| AccessError::Unavailable)?;
        let registration = self.registration(&registry, client, &[])?;
        Ok(ClientIdentity {
            credential_id: registration.credential_id.clone(),
            generation: registration
                .generation
                .clone()
                .ok_or(AccessError::InvalidConfiguration)?,
            namespace: registration.namespace.clone(),
        })
    }

    /// Lists current start grants without exposing other profiles or credentials.
    pub fn allowed_profiles(
        &self,
        client: &AuthenticatedClient,
    ) -> Result<Vec<ProfileGrant>, AccessError> {
        let registry = self.registry.read().map_err(|_| AccessError::Unavailable)?;
        let registration = self.registration(&registry, client, &[Scope::RunStart])?;
        let mut result = Vec::new();
        for id in &registration.profiles {
            let profile = registry
                .profiles
                .get(id)
                .ok_or(AccessError::InvalidConfiguration)?;
            let mut workspaces: Vec<_> = profile.workspaces.iter().cloned().collect();
            workspaces.sort();
            result.push(ProfileGrant {
                profile_id: id.clone(),
                workspaces,
                max_input_bytes: profile.max_input_bytes,
            });
        }
        result.sort_by(|a, b| a.profile_id.cmp(&b.profile_id));
        Ok(result)
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
        self.with_start(client, body, |_, _, _, _, authorize| authorize())
    }

    pub(crate) fn with_start<T>(
        &self,
        client: &AuthenticatedClient,
        body: &[u8],
        operation: impl FnOnce(
            &Namespace,
            &str,
            Option<&str>,
            &StartBody,
            &dyn Fn() -> Result<AuthorizedStart, AccessError>,
        ) -> Result<T, AccessError>,
    ) -> Result<T, AccessError> {
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
        if let Some(skills) = &request.skills {
            crate::storage::skills::validate_refs(skills, self.limits.max_identifier_bytes)
                .map_err(|_| AccessError::InvalidRequest)?;
        }
        if let Some(hooks) = &request.hooks {
            crate::storage::hooks::validate_refs(hooks, self.limits.max_identifier_bytes)
                .map_err(|_| AccessError::InvalidRequest)?;
            if !hooks.is_empty()
                && (!registration.scopes.contains(&Scope::HookRead)
                    || !registration.scopes.contains(&Scope::RunRead))
            {
                return Err(AccessError::Forbidden);
            }
        }
        if let StartSession::Resume { session_id } = &request.session {
            validate_resource_id(session_id, &self.limits)?;
            if !registration.scopes.contains(&Scope::SessionRead) {
                return Err(AccessError::Forbidden);
            }
        }
        if request
            .skills
            .as_ref()
            .is_some_and(|skills| !skills.is_empty())
            && !registration.scopes.contains(&Scope::SkillRead)
        {
            return Err(AccessError::Forbidden);
        }
        let authorize = || {
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
            Ok(AuthorizedStart::from_body(
                request.clone(),
                registration.scopes.contains(&Scope::SkillRead),
                registration.scopes.contains(&Scope::HookRead)
                    && registration.scopes.contains(&Scope::RunRead),
                registration.destinations.clone(),
            ))
        };
        operation(
            &registration.namespace,
            &registration.credential_id,
            registration.generation.as_deref(),
            &request,
            &authorize,
        )
    }

    pub(crate) fn with_launch<T>(
        &self,
        authorization: LaunchAuthorization<'_>,
        operation: impl FnOnce() -> Result<T, AccessError>,
    ) -> Result<T, AccessError> {
        let registry = self.registry.read().map_err(|_| AccessError::Unavailable)?;
        let registration = registry
            .credentials
            .get(authorization.credential_id)
            .ok_or(AccessError::Unauthenticated)?;
        let profile = registry
            .profiles
            .get(authorization.profile_id)
            .ok_or(AccessError::Forbidden)?;
        if &registration.namespace != authorization.namespace
            || registration.generation.as_deref() != Some(authorization.generation)
            || !registration.scopes.contains(&Scope::RunStart)
            || (authorization.resume && !registration.scopes.contains(&Scope::SessionRead))
            || (authorization.requires_skill_read
                && !registration.scopes.contains(&Scope::SkillRead))
            || !registration.profiles.contains(authorization.profile_id)
            || !profile.workspaces.contains(authorization.workspace_id)
            || authorization.input_len > profile.max_input_bytes
        {
            return Err(AccessError::Forbidden);
        }
        operation()
    }

    pub(crate) fn request_limit(&self) -> usize {
        self.limits.max_request_bytes
    }

    pub(crate) fn skill_limit(&self) -> usize {
        self.limits.max_skill_bytes
    }

    pub(crate) fn identifier_limit(&self) -> usize {
        self.limits.max_identifier_bytes
    }

    pub(crate) fn supports_service_ids(&self) -> bool {
        self.limits.max_identifier_bytes >= 40
    }

    pub(crate) fn with_delivery_authorization<T>(
        &self,
        credential_id: &str,
        generation: &str,
        namespace: &Namespace,
        destination_id: &str,
        operation: impl FnOnce() -> Result<T, AccessError>,
    ) -> Result<T, AccessError> {
        let registry = self.registry.read().map_err(|_| AccessError::Unavailable)?;
        let registration = registry
            .credentials
            .get(credential_id)
            .ok_or(AccessError::Unauthenticated)?;
        if &registration.namespace != namespace
            || registration.generation.as_deref() != Some(generation)
            || !registration.scopes.contains(&Scope::HookRead)
            || !registration.scopes.contains(&Scope::RunRead)
            || !registration.destinations.contains(destination_id)
        {
            return Err(AccessError::Forbidden);
        }
        operation()
    }

    pub(crate) fn with_hook_permissions<T>(
        &self,
        client: &AuthenticatedClient,
        required: &[Scope],
        operation: impl FnOnce(
            &Namespace,
            &AccessLimits,
            &HashSet<String>,
            &HashSet<String>,
            &str,
            Option<&str>,
        ) -> Result<T, AccessError>,
    ) -> Result<T, AccessError> {
        let registry = self.registry.read().map_err(|_| AccessError::Unavailable)?;
        let registration = self.registration(&registry, client, required)?;
        operation(
            &registration.namespace,
            &self.limits,
            &registration.destinations,
            &registration.reconciliation_destinations,
            &registration.credential_id,
            registration.generation.as_deref(),
        )
    }

    pub(crate) fn with_permissions<T>(
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

pub(crate) fn validate_resource_id(value: &str, limits: &AccessLimits) -> Result<(), AccessError> {
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
        AccessError, AccessLimits, ClientAccessControl, ClientRegistration, CredentialSecret,
        ExecutionProfile, LaunchAuthorization, Scope,
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
    fn launch_rechecks_registration_generation_namespace_and_resume_scope() {
        let namespace = Namespace {
            owner_id: "owner".into(),
            client_id: "client".into(),
        };
        let create = |generation: &str, scopes: Vec<Scope>| {
            ClientAccessControl::new(
                [ClientRegistration::new(
                    "client",
                    CredentialSecret::new([7; 32]),
                    namespace.clone(),
                    scopes,
                    ["profile".into()],
                )
                .with_generation(generation)],
                [ExecutionProfile::dummy(
                    "profile",
                    ["workspace".into()],
                    100,
                )],
                AccessLimits {
                    max_request_bytes: 256,
                    max_skill_bytes: 128,
                    max_identifier_bytes: 64,
                },
            )
            .unwrap()
        };
        let current = create("generation_2", vec![Scope::RunStart]);
        let check = |generation, resume| LaunchAuthorization {
            requires_skill_read: false,
            credential_id: "client",
            generation,
            namespace: &namespace,
            profile_id: "profile",
            workspace_id: "workspace",
            input_len: 5,
            resume,
        };
        assert!(matches!(
            current.with_launch(check("generation_1", false), || Ok(())),
            Err(AccessError::Forbidden)
        ));
        assert!(
            current
                .with_launch(check("generation_2", false), || Ok(()))
                .is_ok()
        );
        assert!(matches!(
            current.with_launch(check("generation_2", true), || Ok(())),
            Err(AccessError::Forbidden)
        ));
        let permitted = create("generation_2", vec![Scope::RunStart, Scope::SessionRead]);
        assert!(
            permitted
                .with_launch(check("generation_2", true), || Ok(()))
                .is_ok()
        );
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
