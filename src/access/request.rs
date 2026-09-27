use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::storage::{HookRef, SkillRef};

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StartBody {
    pub profile_id: String,
    pub workspace_id: String,
    #[serde(default)]
    pub session: StartSession,
    pub input: StartInput,
    pub idempotency_key: String,
    #[serde(
        default,
        deserialize_with = "deserialize_skills",
        skip_serializing_if = "Option::is_none"
    )]
    pub skills: Option<Vec<SkillRef>>,
    #[serde(
        default,
        deserialize_with = "deserialize_hooks",
        skip_serializing_if = "Option::is_none"
    )]
    pub hooks: Option<Vec<HookRef>>,
    #[serde(
        default,
        deserialize_with = "deserialize_result_schema",
        skip_serializing_if = "Option::is_none"
    )]
    pub result_schema: Option<serde_json::Value>,
}

fn deserialize_result_schema<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<serde_json::Value>, D::Error> {
    let value = serde_json::Value::deserialize(deserializer)?;
    if value.is_null() {
        return Err(serde::de::Error::custom("result schema cannot be null"));
    }
    Ok(Some(value))
}

fn deserialize_skills<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Vec<SkillRef>>, D::Error> {
    Vec::<SkillRef>::deserialize(deserializer).map(Some)
}

fn deserialize_hooks<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Vec<HookRef>>, D::Error> {
    Vec::<HookRef>::deserialize(deserializer).map(Some)
}

#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum StartSession {
    #[default]
    New,
    Resume {
        session_id: String,
    },
    ResumeNative {
        native_id: String,
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
    may_use_skills: bool,
    may_use_hooks: bool,
    destinations: HashSet<String>,
}

impl AuthorizedStart {
    pub(super) fn from_body(
        body: StartBody,
        may_use_skills: bool,
        may_use_hooks: bool,
        destinations: HashSet<String>,
    ) -> Self {
        Self {
            profile_id: body.profile_id,
            workspace_id: body.workspace_id,
            input: body.input.request,
            idempotency_key: body.idempotency_key,
            session: body.session,
            may_use_skills,
            may_use_hooks,
            destinations,
        }
    }

    pub(crate) fn may_use_hooks(&self) -> bool {
        self.may_use_hooks
    }

    pub(crate) fn destinations(&self) -> &HashSet<String> {
        &self.destinations
    }

    pub(crate) fn may_use_skills(&self) -> bool {
        self.may_use_skills
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
            StartSession::New | StartSession::ResumeNative { .. } => None,
            StartSession::Resume { session_id } => Some(session_id),
        }
    }

    pub(crate) fn resume_native_id(&self) -> Option<&str> {
        match &self.session {
            StartSession::ResumeNative { native_id } => Some(native_id),
            _ => None,
        }
    }

    pub(crate) fn is_resume(&self) -> bool {
        !matches!(self.session, StartSession::New)
    }

    /// Only the synthetic runtime is available in this profile implementation.
    pub fn runtime(&self) -> &'static str {
        "dummy"
    }
}
