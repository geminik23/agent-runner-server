use crate::storage::{
    EventSummary, HookBinding, RunSummary, SkillRef, SkillVersion, SkillWrite, SqliteStore,
    StoredAttempt, StoredDelivery, StoredInboxItem, StoredResult, StoredSession,
};

use super::authority::{
    AccessError, AccessLimits, AuthenticatedClient, ClientAccessControl, Scope,
    validate_resource_id,
};

/// Exposes only client operations whose namespace comes from authenticated context.
pub struct AuthorizedStore<'a> {
    authority: &'a ClientAccessControl,
    client: &'a AuthenticatedClient,
    storage: &'a mut SqliteStore,
}

impl<'a> AuthorizedStore<'a> {
    /// Binds a trusted store to an authenticated context without exposing its namespace.
    pub fn new(
        authority: &'a ClientAccessControl,
        client: &'a AuthenticatedClient,
        storage: &'a mut SqliteStore,
    ) -> Self {
        Self {
            authority,
            client,
            storage,
        }
    }

    /// Reads a run summary without exposing its internal request or manifest.
    pub fn get_run(&self, run_id: &str) -> Result<Option<RunSummary>, AccessError> {
        self.authority
            .with_permissions(self.client, &[Scope::RunRead], |namespace, limits| {
                validate_resource_id(run_id, limits)?;
                self.storage
                    .get_run_summary(namespace, run_id)
                    .map_err(Into::into)
            })
    }

    /// Reads a run attempt when the client can read runs.
    pub fn get_attempt(
        &self,
        run_id: &str,
        attempt_id: &str,
    ) -> Result<Option<StoredAttempt>, AccessError> {
        self.authority
            .with_permissions(self.client, &[Scope::RunRead], |namespace, limits| {
                validate_resource_id(run_id, limits)?;
                validate_resource_id(attempt_id, limits)?;
                self.storage
                    .get_attempt(namespace, run_id, attempt_id)
                    .map_err(Into::into)
            })
    }

    /// Reads event summaries without exposing unclassified payloads.
    pub fn get_events(
        &self,
        run_id: &str,
        after: Option<i64>,
    ) -> Result<Vec<EventSummary>, AccessError> {
        self.authority
            .with_permissions(self.client, &[Scope::RunRead], |namespace, limits| {
                validate_resource_id(run_id, limits)?;
                self.storage
                    .get_event_summaries(namespace, run_id, after)
                    .map_err(Into::into)
            })
    }

    /// Reads a result when the client can read runs.
    pub fn get_result(&self, run_id: &str) -> Result<Option<StoredResult>, AccessError> {
        self.authority
            .with_permissions(self.client, &[Scope::RunRead], |namespace, limits| {
                validate_resource_id(run_id, limits)?;
                self.storage
                    .get_result(namespace, run_id)
                    .map_err(Into::into)
            })
    }

    /// Reads a session when the client can read sessions.
    pub fn get_session(&self, session_id: &str) -> Result<Option<StoredSession>, AccessError> {
        self.authority
            .with_permissions(self.client, &[Scope::SessionRead], |namespace, limits| {
                validate_resource_id(session_id, limits)?;
                self.storage
                    .get_session(namespace, session_id)
                    .map_err(Into::into)
            })
    }

    /// Reads a skill version when the client can read skills.
    pub fn get_skill_version(
        &self,
        reference: &SkillRef,
    ) -> Result<Option<SkillVersion>, AccessError> {
        self.authority
            .with_permissions(self.client, &[Scope::SkillRead], |namespace, limits| {
                validate_skill_reference(reference, limits)?;
                self.storage
                    .get_skill_version(namespace, reference)
                    .map_err(Into::into)
            })
    }

    /// Requires both skill scopes to avoid revealing whether guessed existing content matches.
    pub fn put_skill_version(
        &mut self,
        reference: &SkillRef,
        markdown: &str,
    ) -> Result<SkillWrite, AccessError> {
        let storage = &mut *self.storage;
        self.authority.with_permissions(
            self.client,
            &[Scope::SkillRead, Scope::SkillWrite],
            |namespace, limits| {
                validate_skill_reference(reference, limits)?;
                let total = reference
                    .skill_id
                    .len()
                    .checked_add(reference.version.len())
                    .and_then(|length| length.checked_add(markdown.len()))
                    .ok_or(AccessError::InvalidRequest)?;
                if markdown.len() > limits.max_skill_bytes || total > limits.max_request_bytes {
                    return Err(AccessError::InvalidRequest);
                }
                storage
                    .put_skill_version(namespace, reference, markdown)
                    .map_err(Into::into)
            },
        )
    }

    /// Reads run skill references only with both run and skill read permissions.
    pub fn get_run_skills(&self, run_id: &str) -> Result<Vec<SkillRef>, AccessError> {
        self.authority.with_permissions(
            self.client,
            &[Scope::RunRead, Scope::SkillRead],
            |namespace, limits| {
                validate_resource_id(run_id, limits)?;
                self.storage
                    .get_run_skills(namespace, run_id)
                    .map_err(Into::into)
            },
        )
    }

    /// Reads pinned hook bindings only with both run and hook read permissions.
    pub fn get_hook_bindings(&self, run_id: &str) -> Result<Vec<HookBinding>, AccessError> {
        self.authority.with_permissions(
            self.client,
            &[Scope::RunRead, Scope::HookRead],
            |namespace, limits| {
                validate_resource_id(run_id, limits)?;
                self.storage
                    .get_hook_bindings(namespace, run_id)
                    .map_err(Into::into)
            },
        )
    }

    /// Reads delivery payloads only with both run and hook read permissions.
    pub fn get_deliveries(&self, run_id: &str) -> Result<Vec<StoredDelivery>, AccessError> {
        self.authority.with_permissions(
            self.client,
            &[Scope::RunRead, Scope::HookRead],
            |namespace, limits| {
                validate_resource_id(run_id, limits)?;
                self.storage
                    .get_deliveries(namespace, run_id)
                    .map_err(Into::into)
            },
        )
    }

    /// Reads inbox items when the client has inbox read permission.
    pub fn get_inbox_items(&self, run_id: &str) -> Result<Vec<StoredInboxItem>, AccessError> {
        self.authority
            .with_permissions(self.client, &[Scope::InboxRead], |namespace, limits| {
                validate_resource_id(run_id, limits)?;
                self.storage
                    .get_inbox_items(namespace, run_id)
                    .map_err(Into::into)
            })
    }
}

fn validate_skill_reference(
    reference: &SkillRef,
    limits: &AccessLimits,
) -> Result<(), AccessError> {
    validate_resource_id(&reference.skill_id, limits)?;
    validate_resource_id(&reference.version, limits)
}
