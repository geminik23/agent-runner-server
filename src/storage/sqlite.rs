use std::{path::Path, time::Duration};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use crate::core::SessionRef;

use super::{
    queries::{load_session, load_skill, session_from_row},
    types::{
        Namespace, NewSession, SkillRef, SkillVersion, SkillWrite, StorageError, StoredSession,
    },
};

const SCHEMA_VERSION: i64 = 3;
const APPLICATION_ID: i64 = 0x41525353;
const INITIAL_MIGRATION: &str = include_str!("../../migrations/0001_initial.sql");
const RUN_SERVICE_MIGRATION: &str = include_str!("../../migrations/0002_run_service.sql");
const SKILL_REGISTRY_MIGRATION: &str = include_str!("../../migrations/0003_skill_registry.sql");

/// Owns a synchronous SQLite connection; async callers must avoid blocking their runtime.
pub struct SqliteStore {
    pub(super) connection: Connection,
}

impl SqliteStore {
    /// Opens a file-backed store and applies supported migrations atomically.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        Self::initialize(Connection::open(path)?)
    }

    /// Opens an isolated in-memory store.
    pub fn open_in_memory() -> Result<Self, StorageError> {
        Self::initialize(Connection::open_in_memory()?)
    }

    fn initialize(mut connection: Connection) -> Result<Self, StorageError> {
        connection.pragma_update(None, "foreign_keys", true)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let version: i64 =
            transaction.pragma_query_value(None, "user_version", |row| row.get(0))?;
        let application_id: i64 =
            transaction.pragma_query_value(None, "application_id", |row| row.get(0))?;
        match (application_id, version) {
            (0, 0) => {
                let occupied: bool = transaction.query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*')",
                    [],
                    |row| row.get(0),
                )?;
                if occupied {
                    return Err(StorageError::InvalidDatabase);
                }
                transaction.execute_batch(INITIAL_MIGRATION)?;
                transaction.execute_batch(RUN_SERVICE_MIGRATION)?;
                transaction.execute_batch(SKILL_REGISTRY_MIGRATION)?;
                transaction.pragma_update(None, "application_id", APPLICATION_ID)?;
                transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
            }
            (APPLICATION_ID, 1) => {
                transaction.execute_batch(RUN_SERVICE_MIGRATION)?;
                transaction.execute_batch(SKILL_REGISTRY_MIGRATION)?;
                transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
            }
            (APPLICATION_ID, 2) => {
                transaction.execute_batch(SKILL_REGISTRY_MIGRATION)?;
                transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
            }
            (APPLICATION_ID, SCHEMA_VERSION) => {}
            (APPLICATION_ID, other) => return Err(StorageError::UnsupportedSchema(other)),
            _ => return Err(StorageError::InvalidDatabase),
        }
        transaction.commit()?;
        Ok(Self { connection })
    }

    /// Creates a storage namespace without granting credentials or permissions.
    pub fn create_namespace(&self, namespace: &Namespace) -> Result<(), StorageError> {
        validate_namespace(namespace)?;
        self.connection.execute(
            "INSERT INTO namespaces (owner_id, client_id) VALUES (?1, ?2) ON CONFLICT DO NOTHING",
            params![namespace.owner_id, namespace.client_id],
        )?;
        Ok(())
    }

    /// Inserts an immutable skill version or accepts identical stored text.
    pub fn put_skill_version(
        &mut self,
        namespace: &Namespace,
        reference: &SkillRef,
        markdown: &str,
    ) -> Result<SkillWrite, StorageError> {
        validate_namespace(namespace)?;
        validate_skill(reference)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing) = load_skill(&transaction, namespace, reference)? {
            if existing.markdown.as_bytes() != markdown.as_bytes() {
                return Err(StorageError::SkillVersionConflict);
            }
            transaction.commit()?;
            return Ok(SkillWrite::Unchanged);
        }
        transaction.execute(
            "INSERT INTO skill_versions (owner_id, client_id, skill_id, version, markdown) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![namespace.owner_id, namespace.client_id, reference.skill_id, reference.version, markdown],
        )?;
        transaction.commit()?;
        Ok(SkillWrite::Inserted)
    }

    /// Retrieves a skill version only from the requested namespace.
    pub fn get_skill_version(
        &self,
        namespace: &Namespace,
        reference: &SkillRef,
    ) -> Result<Option<SkillVersion>, StorageError> {
        validate_namespace(namespace)?;
        validate_skill(reference)?;
        load_skill(&self.connection, namespace, reference)
    }

    /// Registers a session; native identities cannot belong to multiple sessions.
    pub fn create_session(
        &mut self,
        namespace: &Namespace,
        session: &NewSession,
    ) -> Result<(), StorageError> {
        validate_namespace(namespace)?;
        nonempty(&session.session_id, "session_id")?;
        nonempty(&session.runtime, "runtime")?;
        nonempty(&session.state_realm, "state_realm")?;
        if let Some(native_id) = &session.native_id {
            nonempty(native_id, "native_id")?;
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if session.native_id.is_some() {
            let blocked: bool = transaction.query_row(
                "SELECT EXISTS(SELECT 1 FROM sessions WHERE runtime = ?1 AND state_realm = ?2 AND native_id IS NULL AND reserved_run_id IS NOT NULL)",
                params![session.runtime, session.state_realm],
                |row| row.get(0),
            )?;
            if blocked {
                return Err(StorageError::SessionBusy);
            }
        }
        transaction.execute(
            "INSERT INTO sessions (owner_id, client_id, session_id, runtime, state_realm, native_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![namespace.owner_id, namespace.client_id, session.session_id, session.runtime, session.state_realm, session.native_id],
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Resolves a server session ID inside a namespace.
    pub fn get_session(
        &self,
        namespace: &Namespace,
        session_id: &str,
    ) -> Result<Option<StoredSession>, StorageError> {
        validate_namespace(namespace)?;
        nonempty(session_id, "session_id")?;
        load_session(&self.connection, namespace, session_id)
    }

    /// Resolves a canonical native identity without crossing namespaces.
    pub fn get_session_by_native(
        &self,
        namespace: &Namespace,
        native: &SessionRef,
    ) -> Result<Option<StoredSession>, StorageError> {
        validate_namespace(namespace)?;
        nonempty(&native.runtime, "runtime")?;
        nonempty(&native.state_realm, "state_realm")?;
        nonempty(&native.native_id, "native_id")?;
        self.connection
            .query_row(
                "SELECT session_id, runtime, state_realm, native_id, reserved_run_id, cleanup, disposition FROM sessions WHERE owner_id = ?1 AND client_id = ?2 AND runtime = ?3 AND state_realm = ?4 AND native_id = ?5",
                params![namespace.owner_id, namespace.client_id, native.runtime, native.state_realm, native.native_id],
                session_from_row,
            )
            .optional()
            .map_err(Into::into)
    }

    /// Binds an observed native identity without replacing an existing one.
    pub fn bind_native_session(
        &mut self,
        namespace: &Namespace,
        session_id: &str,
        native: &SessionRef,
    ) -> Result<(), StorageError> {
        validate_namespace(namespace)?;
        nonempty(session_id, "session_id")?;
        nonempty(&native.native_id, "native_id")?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current =
            load_session(&transaction, namespace, session_id)?.ok_or(StorageError::NotFound)?;
        if current.session.runtime != native.runtime
            || current.session.state_realm != native.state_realm
        {
            return Err(StorageError::InvalidInput("native session realm"));
        }
        match current.session.native_id {
            Some(existing) if existing != native.native_id => {
                return Err(StorageError::InvalidState);
            }
            Some(_) => {}
            None => {
                let blocked: bool = transaction.query_row(
                    "SELECT EXISTS(SELECT 1 FROM sessions s LEFT JOIN service_jobs j ON j.owner_id = s.owner_id AND j.client_id = s.client_id AND j.run_id = s.reserved_run_id LEFT JOIN attempts a ON a.owner_id = j.owner_id AND a.client_id = j.client_id AND a.run_id = j.run_id AND a.attempt_id = j.attempt_id WHERE s.runtime = ?1 AND s.state_realm = ?2 AND s.native_id IS NULL AND s.reserved_run_id IS NOT NULL AND NOT (s.owner_id = ?3 AND s.client_id = ?4 AND s.session_id = ?5) AND (j.queue_id IS NULL OR a.spawn_intent = 1))",
                    params![native.runtime, native.state_realm, namespace.owner_id, namespace.client_id, session_id],
                    |row| row.get(0),
                )?;
                if blocked {
                    return Err(StorageError::SessionBusy);
                }
                transaction.execute(
                    "UPDATE sessions SET native_id = ?1 WHERE owner_id = ?2 AND client_id = ?3 AND session_id = ?4 AND native_id IS NULL",
                    params![native.native_id, namespace.owner_id, namespace.client_id, session_id],
                )?;
            }
        }
        transaction.commit()?;
        Ok(())
    }
}

pub(super) fn nonempty(value: &str, field: &'static str) -> Result<(), StorageError> {
    if value.is_empty() {
        Err(StorageError::InvalidInput(field))
    } else {
        Ok(())
    }
}

pub(super) fn validate_namespace(namespace: &Namespace) -> Result<(), StorageError> {
    nonempty(&namespace.owner_id, "owner_id")?;
    nonempty(&namespace.client_id, "client_id")
}

pub(super) fn validate_skill(reference: &SkillRef) -> Result<(), StorageError> {
    nonempty(&reference.skill_id, "skill_id")?;
    nonempty(&reference.version, "skill version")
}
