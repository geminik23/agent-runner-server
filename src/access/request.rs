use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StartBody {
    pub profile_id: String,
    pub workspace_id: String,
    #[serde(default)]
    pub session: StartSession,
    pub input: StartInput,
    pub idempotency_key: String,
}

#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum StartSession {
    #[default]
    New,
    Resume {
        session_id: String,
    },
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StartInput {
    pub request: String,
}

/// Contains a validated dummy request, not a persistent authorization grant.
pub struct AuthorizedStart {
    profile_id: String,
    workspace_id: String,
    input: String,
    idempotency_key: String,
    session: StartSession,
}

impl AuthorizedStart {
    pub(super) fn from_body(body: StartBody) -> Self {
        Self {
            profile_id: body.profile_id,
            workspace_id: body.workspace_id,
            input: body.input.request,
            idempotency_key: body.idempotency_key,
            session: body.session,
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

    /// Returns the validated new-session or resume selection.
    pub fn session(&self) -> &StartSession {
        &self.session
    }

    pub(crate) fn resume_session_id(&self) -> Option<&str> {
        match &self.session {
            StartSession::New => None,
            StartSession::Resume { session_id } => Some(session_id),
        }
    }

    /// Only the synthetic runtime is available in this profile implementation.
    pub fn runtime(&self) -> &'static str {
        "dummy"
    }
}
