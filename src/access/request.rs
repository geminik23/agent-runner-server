use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StartBody {
    pub profile_id: String,
    pub workspace_id: String,
    pub input: StartInput,
    pub idempotency_key: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StartInput {
    pub request: String,
}

/// Contains a validated dummy request, not a persistent authorization grant.
pub struct AuthorizedStart {
    profile_id: String,
    workspace_id: String,
    input: String,
    idempotency_key: String,
}

impl AuthorizedStart {
    pub(super) fn from_body(body: StartBody) -> Self {
        Self {
            profile_id: body.profile_id,
            workspace_id: body.workspace_id,
            input: body.input.request,
            idempotency_key: body.idempotency_key,
        }
    }

    /// Returns the server-approved profile identifier.
    pub fn profile_id(&self) -> &str {
        &self.profile_id
    }

    /// Returns the server-approved opaque workspace identifier.
    pub fn workspace_id(&self) -> &str {
        &self.workspace_id
    }

    /// Returns the literal input, which may contain sensitive user data.
    pub fn input(&self) -> &str {
        &self.input
    }

    /// Returns the caller's idempotency key.
    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }

    /// Only the synthetic runtime is available in this profile implementation.
    pub fn runtime(&self) -> &'static str {
        "dummy"
    }
}
