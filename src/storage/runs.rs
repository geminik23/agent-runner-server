use rusqlite::{Connection, TransactionBehavior, params};

use crate::core::{CleanupState, SessionDisposition};

use super::{
    queries::{load_run, load_run_by_key, load_session},
    sqlite::{SqliteStore, nonempty, validate_namespace, validate_skill},
    types::{Admission, AdmissionOutcome, Completion, Namespace, StorageError},
};

impl SqliteStore {
    /// Atomically stores a run, attempt, reservation, pinned inputs, and queued event.
    pub fn admit_run(
        &mut self,
        namespace: &Namespace,
        admission: &Admission,
    ) -> Result<AdmissionOutcome, StorageError> {
        validate_namespace(namespace)?;
        nonempty(&admission.idempotency_key, "idempotency_key")?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing) =
            load_run_by_key(&transaction, namespace, &admission.idempotency_key)?
        {
            if existing.normalized_request != admission.normalized_request {
                return Err(StorageError::IdempotencyConflict);
            }
            transaction.commit()?;
            return Ok(AdmissionOutcome::Existing(existing));
        }

        validate_admission(admission)?;
        if let Some(session_id) = &admission.session_id {
            let session =
                load_session(&transaction, namespace, session_id)?.ok_or(StorageError::NotFound)?;
            if session.session.runtime != admission.metadata.runtime {
                return Err(StorageError::InvalidInput("session runtime"));
            }
            if session.reserved_run_id.is_some() {
                return Err(StorageError::SessionBusy);
            }
            let blocked: bool = transaction.query_row(
                "SELECT EXISTS(SELECT 1 FROM sessions WHERE runtime = ?1 AND state_realm = ?2 AND reserved_run_id IS NOT NULL AND (native_id IS NULL OR ?3 IS NULL))",
                params![session.session.runtime, session.session.state_realm, session.session.native_id],
                |row| row.get(0),
            )?;
            if blocked {
                return Err(StorageError::SessionBusy);
            }
        }
        transaction.execute(
            "INSERT INTO runs (owner_id, client_id, run_id, session_id, idempotency_key, normalized_request, manifest, runtime, synthetic, scenario, state) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 'queued')",
            params![namespace.owner_id, namespace.client_id, admission.run_id, admission.session_id, admission.idempotency_key, admission.normalized_request, admission.manifest, admission.metadata.runtime, admission.metadata.synthetic, admission.metadata.scenario],
        )?;
        transaction.execute(
            "INSERT INTO attempts (owner_id, client_id, run_id, attempt_id, state) VALUES (?1, ?2, ?3, ?4, 'queued')",
            params![namespace.owner_id, namespace.client_id, admission.run_id, admission.attempt_id],
        )?;
        if let Some(session_id) = &admission.session_id {
            let updated = transaction.execute(
                "UPDATE sessions SET reserved_run_id = ?1, cleanup = NULL, disposition = NULL WHERE owner_id = ?2 AND client_id = ?3 AND session_id = ?4 AND reserved_run_id IS NULL",
                params![admission.run_id, namespace.owner_id, namespace.client_id, session_id],
            )?;
            if updated != 1 {
                return Err(StorageError::SessionBusy);
            }
        }
        transaction.execute(
            "INSERT INTO events (owner_id, client_id, run_id, event_id, sequence, kind, payload) VALUES (?1, ?2, ?3, ?4, 0, 'run.queued', ?5)",
            params![namespace.owner_id, namespace.client_id, admission.run_id, admission.queued_event_id, admission.queued_event_payload],
        )?;
        for skill in &admission.skills {
            transaction.execute(
                "INSERT INTO run_skills (owner_id, client_id, run_id, skill_id, version) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![namespace.owner_id, namespace.client_id, admission.run_id, skill.skill_id, skill.version],
            )?;
        }
        for hook in &admission.hooks {
            transaction.execute(
                "INSERT INTO hook_bindings (owner_id, client_id, run_id, binding_id, hook_revision, destination_revision, projection) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![namespace.owner_id, namespace.client_id, admission.run_id, hook.binding_id, hook.hook_revision, hook.destination_revision, hook.projection],
            )?;
        }
        let stored = load_run(&transaction, namespace, &admission.run_id)?
            .ok_or(StorageError::InvalidState)?;
        transaction.commit()?;
        Ok(AdmissionOutcome::Accepted(stored))
    }

    /// Commits launch intent before a backend can create or access native session state.
    pub fn record_spawn_intent(
        &mut self,
        namespace: &Namespace,
        run_id: &str,
        attempt_id: &str,
    ) -> Result<(), StorageError> {
        validate_namespace(namespace)?;
        nonempty(run_id, "run_id")?;
        nonempty(attempt_id, "attempt_id")?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let run = load_run(&transaction, namespace, run_id)?.ok_or(StorageError::NotFound)?;
        if is_terminal(&run.state) {
            return Err(StorageError::AlreadyTerminal);
        }
        let updated = transaction.execute(
            "UPDATE attempts SET spawn_intent = 1 WHERE owner_id = ?1 AND client_id = ?2 AND run_id = ?3 AND attempt_id = ?4 AND state IN ('queued', 'preparing', 'running', 'validating')",
            params![namespace.owner_id, namespace.client_id, run_id, attempt_id],
        )?;
        if updated != 1 {
            return Err(StorageError::NotFound);
        }
        transaction.commit()?;
        Ok(())
    }

    /// Appends an observation while a run is nonterminal.
    pub fn append_event(
        &mut self,
        namespace: &Namespace,
        run_id: &str,
        event_id: &str,
        kind: &str,
        payload: &[u8],
    ) -> Result<i64, StorageError> {
        validate_namespace(namespace)?;
        nonempty(run_id, "run_id")?;
        nonempty(event_id, "event_id")?;
        nonempty(kind, "kind")?;
        if kind == "run.queued" || kind == "run.finished" {
            return Err(StorageError::InvalidInput("reserved event kind"));
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let run = load_run(&transaction, namespace, run_id)?.ok_or(StorageError::NotFound)?;
        if is_terminal(&run.state) {
            return Err(StorageError::AlreadyTerminal);
        }
        let sequence = next_sequence(&transaction, namespace, run_id)?;
        transaction.execute(
            "INSERT INTO events (owner_id, client_id, run_id, event_id, sequence, kind, payload) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![namespace.owner_id, namespace.client_id, run_id, event_id, sequence, kind, payload],
        )?;
        transaction.commit()?;
        Ok(sequence)
    }

    /// Atomically commits a terminal result, event, delivery intents, and optional inbox item.
    pub fn finish_run(
        &mut self,
        namespace: &Namespace,
        completion: &Completion,
    ) -> Result<(), StorageError> {
        validate_namespace(namespace)?;
        validate_completion(completion)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let run =
            load_run(&transaction, namespace, &completion.run_id)?.ok_or(StorageError::NotFound)?;
        if is_terminal(&run.state) {
            return Err(StorageError::AlreadyTerminal);
        }
        let managed: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM service_jobs WHERE owner_id=?1 AND client_id=?2 AND run_id=?3)",
            params![namespace.owner_id, namespace.client_id, completion.run_id],
            |row| row.get(0),
        )?;
        if managed {
            return Err(StorageError::InvalidState);
        }
        if run.session_id.is_none() && completion.disposition != SessionDisposition::NotApplicable {
            return Err(StorageError::InvalidInput("session disposition"));
        }
        let attempt_exists: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM attempts WHERE owner_id = ?1 AND client_id = ?2 AND run_id = ?3 AND attempt_id = ?4 AND state IN ('queued', 'preparing', 'running', 'validating'))",
            params![namespace.owner_id, namespace.client_id, completion.run_id, completion.attempt_id],
            |row| row.get(0),
        )?;
        if !attempt_exists {
            return Err(StorageError::NotFound);
        }
        let cleanup = cleanup_str(completion.cleanup);
        let disposition = disposition_str(completion.disposition);
        transaction.execute(
            "UPDATE runs SET state = ?1 WHERE owner_id = ?2 AND client_id = ?3 AND run_id = ?4",
            params![
                completion.state.as_str(),
                namespace.owner_id,
                namespace.client_id,
                completion.run_id
            ],
        )?;
        transaction.execute(
            "UPDATE attempts SET state = ?1, cleanup = ?2, disposition = ?3 WHERE owner_id = ?4 AND client_id = ?5 AND run_id = ?6 AND attempt_id = ?7",
            params![completion.state.as_str(), cleanup, disposition, namespace.owner_id, namespace.client_id, completion.run_id, completion.attempt_id],
        )?;
        transaction.execute(
            "INSERT INTO results (owner_id, client_id, run_id, revision, payload) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![namespace.owner_id, namespace.client_id, completion.run_id, completion.result_revision, completion.result_payload],
        )?;
        let sequence = next_sequence(&transaction, namespace, &completion.run_id)?;
        transaction.execute(
            "INSERT INTO events (owner_id, client_id, run_id, event_id, sequence, kind, payload) VALUES (?1, ?2, ?3, ?4, ?5, 'run.finished', ?6)",
            params![namespace.owner_id, namespace.client_id, completion.run_id, completion.terminal_event_id, sequence, completion.terminal_event_payload],
        )?;
        for delivery in &completion.deliveries {
            transaction.execute(
                "INSERT INTO delivery_intents (owner_id, client_id, delivery_id, run_id, event_id, binding_id, payload, state) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'pending')",
                params![namespace.owner_id, namespace.client_id, delivery.delivery_id, completion.run_id, completion.terminal_event_id, delivery.binding_id, delivery.payload],
            )?;
        }
        if let Some(inbox) = &completion.inbox {
            transaction.execute(
                "INSERT INTO inbox_items (owner_id, client_id, inbox_id, run_id, event_id, payload) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![namespace.owner_id, namespace.client_id, inbox.inbox_id, completion.run_id, completion.terminal_event_id, inbox.payload],
            )?;
        }
        if let Some(session_id) = &run.session_id {
            let release = completion.cleanup == CleanupState::Settled
                && completion.disposition == SessionDisposition::Reusable;
            let session_disposition = if completion.disposition == SessionDisposition::NotApplicable
            {
                "uncertain"
            } else {
                disposition
            };
            let updated = transaction.execute(
                "UPDATE sessions SET cleanup = ?1, disposition = CASE WHEN native_id IS NULL AND ?2 = 'reusable' THEN 'uncertain' ELSE ?2 END, reserved_run_id = CASE WHEN ?3 AND native_id IS NOT NULL THEN NULL ELSE reserved_run_id END WHERE owner_id = ?4 AND client_id = ?5 AND session_id = ?6 AND reserved_run_id = ?7",
                params![cleanup, session_disposition, release, namespace.owner_id, namespace.client_id, session_id, completion.run_id],
            )?;
            if updated != 1 {
                return Err(StorageError::InvalidState);
            }
        }
        transaction.commit()?;
        Ok(())
    }

    /// Releases an unbound session only when a terminal attempt has no recorded launch intent.
    /// Callers must record launch intent before starting any backend that can access session state.
    pub fn release_unstarted_session(
        &mut self,
        namespace: &Namespace,
        run_id: &str,
    ) -> Result<bool, StorageError> {
        validate_namespace(namespace)?;
        nonempty(run_id, "run_id")?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let run = load_run(&transaction, namespace, run_id)?.ok_or(StorageError::NotFound)?;
        if !is_terminal(&run.state) {
            return Err(StorageError::InvalidState);
        }
        let session_id = run.session_id.ok_or(StorageError::InvalidState)?;
        let verified: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM attempts WHERE owner_id = ?1 AND client_id = ?2 AND run_id = ?3 AND cleanup = 'settled' AND disposition = 'not_applicable') AND NOT EXISTS(SELECT 1 FROM attempts WHERE owner_id = ?1 AND client_id = ?2 AND run_id = ?3 AND spawn_intent = 1)",
            params![namespace.owner_id, namespace.client_id, run_id],
            |row| row.get(0),
        )?;
        if !verified {
            transaction.commit()?;
            return Ok(false);
        }
        let updated = transaction.execute(
            "UPDATE sessions SET cleanup = 'settled', disposition = 'not_started', reserved_run_id = NULL WHERE owner_id = ?1 AND client_id = ?2 AND session_id = ?3 AND reserved_run_id = ?4 AND native_id IS NULL",
            params![namespace.owner_id, namespace.client_id, session_id, run_id],
        )?;
        if updated == 1 {
            transaction.execute("UPDATE run_workspaces SET held=0, uncertain=0 WHERE owner_id=?1 AND client_id=?2 AND run_id=?3", params![namespace.owner_id, namespace.client_id, run_id])?;
            transaction.execute("INSERT INTO workspace_settlements(owner_id,client_id,run_id) SELECT owner_id,client_id,run_id FROM run_workspaces WHERE owner_id=?1 AND client_id=?2 AND run_id=?3 ON CONFLICT(owner_id,client_id,run_id) DO NOTHING", params![namespace.owner_id, namespace.client_id, run_id])?;
        }
        transaction.commit()?;
        Ok(updated == 1)
    }

    /// Releases an unbound prelaunch workspace after the operator verifies that no work remains.
    /// The caller must verify filesystem activity and any possible external execution independently.
    pub fn settle_unstarted_workspace(
        &mut self,
        namespace: &Namespace,
        run_id: &str,
    ) -> Result<bool, StorageError> {
        validate_namespace(namespace)?;
        nonempty(run_id, "run_id")?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let run = load_run(&tx, namespace, run_id)?.ok_or(StorageError::NotFound)?;
        if !is_terminal(&run.state) {
            return Err(StorageError::InvalidState);
        }
        let session_id = run.session_id.ok_or(StorageError::InvalidState)?;
        let safe_shape: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM attempts WHERE owner_id=?1 AND client_id=?2 AND run_id=?3) AND NOT EXISTS(SELECT 1 FROM attempts WHERE owner_id=?1 AND client_id=?2 AND run_id=?3 AND spawn_intent=1) AND EXISTS(SELECT 1 FROM run_workspaces WHERE owner_id=?1 AND client_id=?2 AND run_id=?3 AND held=1 AND uncertain=1)",
            params![namespace.owner_id,namespace.client_id,run_id], |row| row.get(0),
        )?;
        if !safe_shape {
            tx.commit()?;
            return Ok(false);
        }
        let updated = tx.execute(
            "UPDATE sessions SET cleanup='settled', disposition='not_started', reserved_run_id=NULL WHERE owner_id=?1 AND client_id=?2 AND session_id=?3 AND reserved_run_id=?4 AND native_id IS NULL",
            params![namespace.owner_id,namespace.client_id,session_id,run_id],
        )?;
        if updated == 1 {
            tx.execute("UPDATE run_workspaces SET held=0,uncertain=0 WHERE owner_id=?1 AND client_id=?2 AND run_id=?3",params![namespace.owner_id,namespace.client_id,run_id])?;
            tx.execute("INSERT INTO workspace_settlements(owner_id,client_id,run_id) SELECT owner_id,client_id,run_id FROM run_workspaces WHERE owner_id=?1 AND client_id=?2 AND run_id=?3 ON CONFLICT(owner_id,client_id,run_id) DO NOTHING",params![namespace.owner_id,namespace.client_id,run_id])?;
        }
        tx.commit()?;
        Ok(updated == 1)
    }

    /// Records observed cleanup and releases a terminal run's protected session.
    /// The caller must verify that cleanup and reusable native state were actually observed.
    pub fn settle_session(
        &mut self,
        namespace: &Namespace,
        run_id: &str,
        cleanup: CleanupState,
        disposition: SessionDisposition,
    ) -> Result<bool, StorageError> {
        validate_namespace(namespace)?;
        nonempty(run_id, "run_id")?;
        if cleanup != CleanupState::Settled || disposition != SessionDisposition::Reusable {
            return Err(StorageError::InvalidInput("session settlement"));
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let run = load_run(&transaction, namespace, run_id)?.ok_or(StorageError::NotFound)?;
        if !is_terminal(&run.state) {
            return Err(StorageError::InvalidState);
        }
        let session_id = run.session_id.ok_or(StorageError::InvalidState)?;
        let updated = transaction.execute(
            "UPDATE sessions SET cleanup = 'settled', disposition = 'reusable', reserved_run_id = NULL WHERE owner_id = ?1 AND client_id = ?2 AND session_id = ?3 AND reserved_run_id = ?4 AND native_id IS NOT NULL",
            params![namespace.owner_id, namespace.client_id, session_id, run_id],
        )?;
        if updated == 1 {
            transaction.execute("UPDATE run_workspaces SET held=0, uncertain=0 WHERE owner_id=?1 AND client_id=?2 AND run_id=?3", params![namespace.owner_id, namespace.client_id, run_id])?;
            transaction.execute("INSERT INTO workspace_settlements(owner_id,client_id,run_id) SELECT owner_id,client_id,run_id FROM run_workspaces WHERE owner_id=?1 AND client_id=?2 AND run_id=?3 ON CONFLICT(owner_id,client_id,run_id) DO NOTHING", params![namespace.owner_id, namespace.client_id, run_id])?;
        }
        transaction.commit()?;
        Ok(updated == 1)
    }
}

fn validate_admission(admission: &Admission) -> Result<(), StorageError> {
    nonempty(&admission.run_id, "run_id")?;
    nonempty(&admission.attempt_id, "attempt_id")?;
    nonempty(&admission.queued_event_id, "queued_event_id")?;
    nonempty(&admission.metadata.runtime, "runtime")?;
    if let Some(session_id) = &admission.session_id {
        nonempty(session_id, "session_id")?;
    }
    if admission.metadata.runtime == "dummy" && !admission.metadata.synthetic {
        return Err(StorageError::InvalidInput("dummy metadata"));
    }
    if admission.metadata.synthetic
        && admission
            .metadata
            .scenario
            .as_deref()
            .is_none_or(str::is_empty)
    {
        return Err(StorageError::InvalidInput("synthetic scenario"));
    }
    for skill in &admission.skills {
        validate_skill(skill)?;
    }
    for hook in &admission.hooks {
        nonempty(&hook.binding_id, "binding_id")?;
        nonempty(&hook.hook_revision, "hook_revision")?;
        nonempty(&hook.destination_revision, "destination_revision")?;
    }
    Ok(())
}

fn validate_completion(completion: &Completion) -> Result<(), StorageError> {
    nonempty(&completion.run_id, "run_id")?;
    nonempty(&completion.attempt_id, "attempt_id")?;
    nonempty(&completion.result_revision, "result_revision")?;
    nonempty(&completion.terminal_event_id, "terminal_event_id")?;
    for delivery in &completion.deliveries {
        nonempty(&delivery.delivery_id, "delivery_id")?;
        nonempty(&delivery.binding_id, "binding_id")?;
    }
    if let Some(inbox) = &completion.inbox {
        nonempty(&inbox.inbox_id, "inbox_id")?;
    }
    Ok(())
}

fn next_sequence(
    connection: &Connection,
    namespace: &Namespace,
    run_id: &str,
) -> Result<i64, StorageError> {
    connection
        .query_row(
            "SELECT COALESCE(MAX(sequence), -1) + 1 FROM events WHERE owner_id = ?1 AND client_id = ?2 AND run_id = ?3",
            params![namespace.owner_id, namespace.client_id, run_id],
            |row| row.get(0),
        )
        .map_err(Into::into)
}

fn is_terminal(state: &str) -> bool {
    matches!(
        state,
        "succeeded" | "failed" | "cancelled" | "timed_out" | "interrupted"
    )
}

fn cleanup_str(cleanup: CleanupState) -> &'static str {
    match cleanup {
        CleanupState::Settled => "settled",
        CleanupState::Uncertain => "uncertain",
    }
}

fn disposition_str(disposition: SessionDisposition) -> &'static str {
    match disposition {
        SessionDisposition::NotApplicable => "not_applicable",
        SessionDisposition::Reusable => "reusable",
        SessionDisposition::Uncertain => "uncertain",
    }
}
