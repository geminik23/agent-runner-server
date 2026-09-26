use rusqlite::{Connection, OptionalExtension, Row, params};

use crate::core::ExecutionMetadata;

use super::{
    sqlite::{SqliteStore, nonempty, validate_namespace},
    types::{
        EventSummary, HookBinding, Namespace, NewSession, RunSummary, SkillRef, SkillVersion,
        StorageError, StoredAttempt, StoredDelivery, StoredEvent, StoredInboxItem, StoredResult,
        StoredRun, StoredSession,
    },
};

impl SqliteStore {
    /// Reads status and provenance without loading the internal request or manifest.
    pub fn get_run_summary(
        &self,
        namespace: &Namespace,
        run_id: &str,
    ) -> Result<Option<RunSummary>, StorageError> {
        validate_namespace(namespace)?;
        nonempty(run_id, "run_id")?;
        self.connection.query_row(
            "SELECT run_id, state, runtime, synthetic, scenario FROM runs WHERE owner_id = ?1 AND client_id = ?2 AND run_id = ?3",
            params![namespace.owner_id, namespace.client_id, run_id],
            |row| Ok(RunSummary { run_id: row.get(0)?, state: row.get(1)?, metadata: metadata_from_row(row, 2)? }),
        ).optional().map_err(Into::into)
    }

    /// Reads a run by server ID within its namespace.
    pub fn get_run(
        &self,
        namespace: &Namespace,
        run_id: &str,
    ) -> Result<Option<StoredRun>, StorageError> {
        validate_namespace(namespace)?;
        nonempty(run_id, "run_id")?;
        load_run(&self.connection, namespace, run_id)
    }

    /// Reads an attempt and the execution metadata of its run.
    pub fn get_attempt(
        &self,
        namespace: &Namespace,
        run_id: &str,
        attempt_id: &str,
    ) -> Result<Option<StoredAttempt>, StorageError> {
        validate_namespace(namespace)?;
        nonempty(run_id, "run_id")?;
        nonempty(attempt_id, "attempt_id")?;
        self.connection
            .query_row(
                "SELECT a.attempt_id, a.state, a.spawn_intent, a.cleanup, a.disposition, r.runtime, r.synthetic, r.scenario FROM attempts a JOIN runs r USING (owner_id, client_id, run_id) WHERE a.owner_id = ?1 AND a.client_id = ?2 AND a.run_id = ?3 AND a.attempt_id = ?4",
                params![namespace.owner_id, namespace.client_id, run_id, attempt_id],
                |row| Ok(StoredAttempt { attempt_id: row.get(0)?, state: row.get(1)?, spawn_intent: row.get(2)?, cleanup: row.get(3)?, disposition: row.get(4)?, metadata: metadata_from_row(row, 5)? }),
            )
            .optional()
            .map_err(Into::into)
    }

    /// Reads the exact skill references fixed at admission.
    pub fn get_run_skills(
        &self,
        namespace: &Namespace,
        run_id: &str,
    ) -> Result<Vec<SkillRef>, StorageError> {
        validate_namespace(namespace)?;
        nonempty(run_id, "run_id")?;
        let mut statement = self.connection.prepare(
            "SELECT skill_id, version FROM run_skills WHERE owner_id = ?1 AND client_id = ?2 AND run_id = ?3 ORDER BY position, skill_id",
        )?;
        let rows = statement.query_map(
            params![namespace.owner_id, namespace.client_id, run_id],
            |row| {
                Ok(SkillRef {
                    skill_id: row.get(0)?,
                    version: row.get(1)?,
                })
            },
        )?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    /// Reads the hook bindings fixed at admission.
    pub fn get_hook_bindings(
        &self,
        namespace: &Namespace,
        run_id: &str,
    ) -> Result<Vec<HookBinding>, StorageError> {
        validate_namespace(namespace)?;
        nonempty(run_id, "run_id")?;
        let mut statement = self.connection.prepare(
            "SELECT binding_id, hook_revision, destination_revision, projection FROM hook_bindings WHERE owner_id = ?1 AND client_id = ?2 AND run_id = ?3 ORDER BY binding_id",
        )?;
        let rows = statement.query_map(
            params![namespace.owner_id, namespace.client_id, run_id],
            |row| {
                Ok(HookBinding {
                    binding_id: row.get(0)?,
                    hook_revision: row.get(1)?,
                    destination_revision: row.get(2)?,
                    projection: row.get(3)?,
                })
            },
        )?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    /// Reads ordered event metadata without loading unclassified payloads.
    pub fn get_event_summaries(
        &self,
        namespace: &Namespace,
        run_id: &str,
        after: Option<i64>,
    ) -> Result<Vec<EventSummary>, StorageError> {
        validate_namespace(namespace)?;
        nonempty(run_id, "run_id")?;
        let mut statement = self.connection.prepare(
            "SELECT e.event_id, e.sequence, e.kind, r.runtime, r.synthetic, r.scenario FROM events e JOIN runs r USING (owner_id, client_id, run_id) WHERE e.owner_id = ?1 AND e.client_id = ?2 AND e.run_id = ?3 AND e.sequence > ?4 ORDER BY e.sequence",
        )?;
        let rows = statement.query_map(
            params![
                namespace.owner_id,
                namespace.client_id,
                run_id,
                after.unwrap_or(-1)
            ],
            |row| {
                Ok(EventSummary {
                    event_id: row.get(0)?,
                    sequence: row.get(1)?,
                    kind: row.get(2)?,
                    metadata: metadata_from_row(row, 3)?,
                })
            },
        )?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    /// Reads ordered events after a sequence cursor and attaches run metadata.
    pub fn get_events(
        &self,
        namespace: &Namespace,
        run_id: &str,
        after: Option<i64>,
    ) -> Result<Vec<StoredEvent>, StorageError> {
        validate_namespace(namespace)?;
        nonempty(run_id, "run_id")?;
        let mut statement = self.connection.prepare(
            "SELECT e.event_id, e.sequence, e.kind, e.payload, r.runtime, r.synthetic, r.scenario FROM events e JOIN runs r USING (owner_id, client_id, run_id) WHERE e.owner_id = ?1 AND e.client_id = ?2 AND e.run_id = ?3 AND e.sequence > ?4 ORDER BY e.sequence",
        )?;
        let rows = statement.query_map(
            params![
                namespace.owner_id,
                namespace.client_id,
                run_id,
                after.unwrap_or(-1)
            ],
            |row| {
                Ok(StoredEvent {
                    event_id: row.get(0)?,
                    sequence: row.get(1)?,
                    kind: row.get(2)?,
                    payload: row.get(3)?,
                    metadata: metadata_from_row(row, 4)?,
                })
            },
        )?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    /// Reads a committed result within its namespace.
    pub fn get_result(
        &self,
        namespace: &Namespace,
        run_id: &str,
    ) -> Result<Option<StoredResult>, StorageError> {
        validate_namespace(namespace)?;
        nonempty(run_id, "run_id")?;
        self.connection
            .query_row(
                "SELECT x.revision, x.payload, r.runtime, r.synthetic, r.scenario FROM results x JOIN runs r USING (owner_id, client_id, run_id) WHERE x.owner_id = ?1 AND x.client_id = ?2 AND x.run_id = ?3",
                params![namespace.owner_id, namespace.client_id, run_id],
                |row| Ok(StoredResult { revision: row.get(0)?, payload: row.get(1)?, metadata: metadata_from_row(row, 2)? }),
            )
            .optional()
            .map_err(Into::into)
    }

    /// Reads delivery intents together with their pinned bindings.
    pub fn get_deliveries(
        &self,
        namespace: &Namespace,
        run_id: &str,
    ) -> Result<Vec<StoredDelivery>, StorageError> {
        validate_namespace(namespace)?;
        nonempty(run_id, "run_id")?;
        let mut statement = self.connection.prepare(
            "SELECT d.delivery_id, d.event_id, b.binding_id, b.hook_revision, b.destination_revision, b.projection, d.payload, d.state, r.runtime, r.synthetic, r.scenario FROM delivery_intents d JOIN hook_bindings b USING (owner_id, client_id, run_id, binding_id) JOIN runs r USING (owner_id, client_id, run_id) WHERE d.owner_id = ?1 AND d.client_id = ?2 AND d.run_id = ?3 ORDER BY d.delivery_id",
        )?;
        let rows = statement.query_map(
            params![namespace.owner_id, namespace.client_id, run_id],
            |row| {
                Ok(StoredDelivery {
                    delivery_id: row.get(0)?,
                    event_id: row.get(1)?,
                    binding: HookBinding {
                        binding_id: row.get(2)?,
                        hook_revision: row.get(3)?,
                        destination_revision: row.get(4)?,
                        projection: row.get(5)?,
                    },
                    payload: row.get(6)?,
                    state: row.get(7)?,
                    metadata: metadata_from_row(row, 8)?,
                })
            },
        )?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    /// Reads inbox items belonging to a run within its namespace.
    pub fn get_inbox_items(
        &self,
        namespace: &Namespace,
        run_id: &str,
    ) -> Result<Vec<StoredInboxItem>, StorageError> {
        validate_namespace(namespace)?;
        nonempty(run_id, "run_id")?;
        let mut statement = self.connection.prepare(
            "SELECT i.inbox_id, i.event_id, i.payload, r.runtime, r.synthetic, r.scenario FROM inbox_items i JOIN runs r USING (owner_id, client_id, run_id) WHERE i.owner_id = ?1 AND i.client_id = ?2 AND i.run_id = ?3 ORDER BY i.inbox_id",
        )?;
        let rows = statement.query_map(
            params![namespace.owner_id, namespace.client_id, run_id],
            |row| {
                Ok(StoredInboxItem {
                    inbox_id: row.get(0)?,
                    event_id: row.get(1)?,
                    payload: row.get(2)?,
                    metadata: metadata_from_row(row, 3)?,
                })
            },
        )?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }
}

pub(super) fn load_run(
    connection: &Connection,
    namespace: &Namespace,
    run_id: &str,
) -> Result<Option<StoredRun>, StorageError> {
    connection
        .query_row(
            "SELECT run_id, session_id, idempotency_key, normalized_request, manifest, runtime, synthetic, scenario, state FROM runs WHERE owner_id = ?1 AND client_id = ?2 AND run_id = ?3",
            params![namespace.owner_id, namespace.client_id, run_id],
            run_from_row,
        )
        .optional()
        .map_err(Into::into)
}

pub(super) fn load_run_by_key(
    connection: &Connection,
    namespace: &Namespace,
    key: &str,
) -> Result<Option<StoredRun>, StorageError> {
    connection
        .query_row(
            "SELECT run_id, session_id, idempotency_key, normalized_request, manifest, runtime, synthetic, scenario, state FROM runs WHERE owner_id = ?1 AND client_id = ?2 AND idempotency_key = ?3",
            params![namespace.owner_id, namespace.client_id, key],
            run_from_row,
        )
        .optional()
        .map_err(Into::into)
}

fn run_from_row(row: &Row<'_>) -> rusqlite::Result<StoredRun> {
    Ok(StoredRun {
        run_id: row.get(0)?,
        session_id: row.get(1)?,
        idempotency_key: row.get(2)?,
        normalized_request: row.get(3)?,
        manifest: row.get(4)?,
        metadata: metadata_from_row(row, 5)?,
        state: row.get(8)?,
    })
}

fn metadata_from_row(row: &Row<'_>, offset: usize) -> rusqlite::Result<ExecutionMetadata> {
    Ok(ExecutionMetadata {
        runtime: row.get(offset)?,
        synthetic: row.get(offset + 1)?,
        scenario: row.get(offset + 2)?,
    })
}

pub(super) fn load_skill(
    connection: &Connection,
    namespace: &Namespace,
    reference: &SkillRef,
) -> Result<Option<SkillVersion>, StorageError> {
    connection
        .query_row(
            "SELECT markdown FROM skill_versions WHERE owner_id = ?1 AND client_id = ?2 AND skill_id = ?3 AND version = ?4",
            params![namespace.owner_id, namespace.client_id, reference.skill_id, reference.version],
            |row| Ok(SkillVersion { reference: reference.clone(), markdown: row.get(0)? }),
        )
        .optional()
        .map_err(Into::into)
}

pub(super) fn load_session(
    connection: &Connection,
    namespace: &Namespace,
    session_id: &str,
) -> Result<Option<StoredSession>, StorageError> {
    connection
        .query_row(
            "SELECT session_id, runtime, state_realm, native_id, reserved_run_id, cleanup, disposition FROM sessions WHERE owner_id = ?1 AND client_id = ?2 AND session_id = ?3",
            params![namespace.owner_id, namespace.client_id, session_id],
            session_from_row,
        )
        .optional()
        .map_err(Into::into)
}

pub(super) fn session_from_row(row: &Row<'_>) -> rusqlite::Result<StoredSession> {
    Ok(StoredSession {
        session: NewSession {
            session_id: row.get(0)?,
            runtime: row.get(1)?,
            state_realm: row.get(2)?,
            native_id: row.get(3)?,
        },
        reserved_run_id: row.get(4)?,
        cleanup: row.get(5)?,
        disposition: row.get(6)?,
    })
}
