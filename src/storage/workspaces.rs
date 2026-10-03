use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};

use crate::workspace::{Directory, WorkspaceAccess, WorkspacePlan, journal::JournalKey};

use super::{
    service::ClaimedJob,
    sqlite::SqliteStore,
    types::{Namespace, StorageError},
};

pub(super) fn pin(
    tx: &Transaction<'_>,
    namespace: &Namespace,
    run_id: &str,
    session_id: &str,
    resume: bool,
    plan: Option<&WorkspacePlan>,
) -> Result<(), StorageError> {
    let existing: Option<Vec<u8>> = tx
        .query_row(
            "SELECT plan FROM session_workspaces WHERE owner_id=?1 AND client_id=?2 AND session_id=?3",
            params![namespace.owner_id, namespace.client_id, session_id],
            |row| row.get(0),
        )
        .optional()?;
    let Some(plan) = plan else {
        if existing.is_some() {
            return Err(StorageError::SessionIncompatible);
        }
        return Ok(());
    };
    let encoded = serde_json::to_vec(plan).map_err(|_| StorageError::InvalidState)?;
    if resume {
        if existing.as_deref() != Some(encoded.as_slice()) {
            return Err(StorageError::SessionIncompatible);
        }
    } else {
        if existing.is_some() {
            return Err(StorageError::InvalidState);
        }
        let managed_root = if let Some((root, limit)) = plan
            .managed_capacity_key()
            .map_err(|_| StorageError::InvalidState)?
        {
            let count: i64 = tx.query_row(
                "SELECT COUNT(*) FROM session_workspaces WHERE managed_root=?1",
                [&root],
                |row| row.get(0),
            )?;
            if usize::try_from(count).map_or(true, |count| count >= limit) {
                return Err(StorageError::QueueFull);
            }
            Some(root)
        } else {
            None
        };
        tx.execute(
            "INSERT INTO session_workspaces (owner_id,client_id,session_id,plan,managed_root) VALUES (?1,?2,?3,?4,?5)",
            params![namespace.owner_id, namespace.client_id, session_id, encoded, managed_root],
        )?;
    }
    let path = plan
        .resource_key(session_id)
        .map_err(|_| StorageError::InvalidState)?;
    let access = match plan.access {
        WorkspaceAccess::SharedRead => "shared_read",
        WorkspaceAccess::Exclusive => "exclusive",
    };
    tx.execute(
        "INSERT INTO run_workspaces (owner_id,client_id,run_id,resource_path,access) VALUES (?1,?2,?3,?4,?5)",
        params![namespace.owner_id, namespace.client_id, run_id, path, access],
    )?;
    let (ancestors, first_is_target) = plan.resource_ancestors();
    pin_ancestors(tx, namespace, run_id, ancestors, first_is_target)?;
    Ok(())
}

fn pin_ancestors(
    tx: &Transaction<'_>,
    namespace: &Namespace,
    run_id: &str,
    ancestors: &[(u64, u64)],
    first_is_target: bool,
) -> Result<(), StorageError> {
    for (index, (device, inode)) in ancestors.iter().enumerate() {
        tx.execute(
            "INSERT INTO workspace_ancestors(owner_id,client_id,run_id,device,inode,is_target) VALUES (?1,?2,?3,?4,?5,?6) ON CONFLICT(owner_id,client_id,run_id,device,inode) DO UPDATE SET is_target=MAX(workspace_ancestors.is_target,excluded.is_target)",
            params![namespace.owner_id, namespace.client_id, run_id, device.to_string(), inode.to_string(), first_is_target && index == 0],
        )?;
    }
    Ok(())
}

type BindingRow = (Vec<u8>, Option<Vec<u8>>, String, bool);

pub(crate) enum JournalStatus {
    Settled,
    Uncertain,
    NeedsQuarantine,
    Inconsistent,
}

impl SqliteStore {
    pub(crate) fn has_unsettled_workspace_jobs(&self) -> Result<bool, StorageError> {
        let present: bool = self.connection.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='run_workspaces')", [], |row| row.get(0))?;
        if !present {
            return Ok(false);
        }
        self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM run_workspaces w JOIN runs r USING(owner_id,client_id,run_id) WHERE w.held=1 OR w.uncertain=1 OR r.state IN ('queued','preparing','running','validating'))",
            [], |row| row.get(0),
        ).map_err(Into::into)
    }

    pub(crate) fn workspace_journal_epoch(
        &self,
        identity: &str,
    ) -> Result<Option<u64>, StorageError> {
        let present: bool = self.connection.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='workspace_journal_epochs')", [], |row| row.get(0))?;
        if !present {
            return Ok(None);
        }
        let epoch: Option<i64> = self
            .connection
            .query_row(
                "SELECT epoch FROM workspace_journal_epochs WHERE owner_identity=?1",
                [identity],
                |row| row.get(0),
            )
            .optional()?;
        Ok(Some(
            u64::try_from(epoch.unwrap_or(0)).map_err(|_| StorageError::InvalidState)?,
        ))
    }

    pub(crate) fn workspace_advance_journal_epoch(
        &mut self,
        identity: &str,
        current: u64,
    ) -> Result<u64, StorageError> {
        let next = current
            .checked_add(1)
            .and_then(|value| i64::try_from(value).ok())
            .ok_or(StorageError::InvalidState)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = if current == 0 {
            tx.execute("INSERT INTO workspace_journal_epochs(owner_identity,epoch) VALUES (?1,?2) ON CONFLICT(owner_identity) DO NOTHING", params![identity,next])?
        } else {
            let current = i64::try_from(current).map_err(|_| StorageError::InvalidState)?;
            tx.execute(
                "UPDATE workspace_journal_epochs SET epoch=?1 WHERE owner_identity=?2 AND epoch=?3",
                params![next, identity, current],
            )?
        };
        if changed != 1 {
            return Err(StorageError::InvalidState);
        }
        tx.commit()?;
        u64::try_from(next).map_err(|_| StorageError::InvalidState)
    }

    pub(crate) fn workspace_journal_status(
        &self,
        key: &JournalKey,
    ) -> Result<JournalStatus, StorageError> {
        type Row = (String, bool, Option<String>, bool, bool, bool);
        let row: Option<Row> = self.connection.query_row(
            "SELECT r.state,a.spawn_intent,s.reserved_run_id,w.held,w.uncertain,EXISTS(SELECT 1 FROM workspace_settlements z WHERE z.owner_id=r.owner_id AND z.client_id=r.client_id AND z.run_id=r.run_id) FROM runs r JOIN service_jobs j USING(owner_id,client_id,run_id) JOIN attempts a ON a.owner_id=j.owner_id AND a.client_id=j.client_id AND a.run_id=j.run_id AND a.attempt_id=j.attempt_id JOIN sessions s ON s.owner_id=r.owner_id AND s.client_id=r.client_id AND s.session_id=r.session_id JOIN run_workspaces w USING(owner_id,client_id,run_id) WHERE r.owner_id=?1 AND r.client_id=?2 AND r.run_id=?3 AND j.state_realm=?4",
            params![key.owner_id,key.client_id,key.run_id,key.state_realm],
            |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?)),
        ).optional()?;
        let Some((state, spawn, reserved, held, uncertain, settled)) = row else {
            return Ok(JournalStatus::Inconsistent);
        };
        let terminal = [
            "succeeded",
            "failed",
            "cancelled",
            "timed_out",
            "interrupted",
        ]
        .contains(&state.as_str());
        if terminal && settled && !held && !uncertain {
            Ok(JournalStatus::Settled)
        } else if held
            && reserved.as_deref() == Some(key.run_id.as_str())
            && ((spawn && !terminal) || (terminal && uncertain))
        {
            Ok(JournalStatus::Uncertain)
        } else if !spawn
            && !terminal
            && !settled
            && reserved.as_deref() == Some(key.run_id.as_str())
        {
            Ok(JournalStatus::NeedsQuarantine)
        } else {
            Ok(JournalStatus::Inconsistent)
        }
    }

    pub(crate) fn workspace_binding(
        &self,
        job: &ClaimedJob,
        plan: &WorkspacePlan,
    ) -> Result<(Option<Directory>, bool), StorageError> {
        let row: Option<BindingRow> = self.connection.query_row(
            "SELECT s.plan,s.directory,w.resource_path,s.materialization_intent FROM session_workspaces s JOIN run_workspaces w ON w.owner_id=s.owner_id AND w.client_id=s.client_id JOIN service_jobs j ON j.owner_id=w.owner_id AND j.client_id=w.client_id AND j.run_id=w.run_id JOIN runs r ON r.owner_id=j.owner_id AND r.client_id=j.client_id AND r.run_id=j.run_id AND r.session_id=s.session_id WHERE s.owner_id=?1 AND s.client_id=?2 AND s.session_id=?3 AND j.run_id=?4 AND j.claim_token=?5 AND w.held=1 AND r.state='preparing'",
            params![job.namespace.owner_id, job.namespace.client_id, job.session_id, job.run_id, job.claim_token],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        ).optional()?;
        let (encoded, directory, path, intent) = row.ok_or(StorageError::InvalidState)?;
        let stored: WorkspacePlan =
            serde_json::from_slice(&encoded).map_err(|_| StorageError::InvalidState)?;
        if stored != *plan
            || plan.resource_key(&job.session_id).ok().as_deref() != Some(path.as_str())
        {
            return Err(StorageError::InvalidState);
        }
        let directory = directory
            .map(|bytes| serde_json::from_slice(&bytes).map_err(|_| StorageError::InvalidState))
            .transpose()?;
        Ok((directory, intent))
    }

    pub(crate) fn workspace_mark_materialization(
        &mut self,
        job: &ClaimedJob,
    ) -> Result<(), StorageError> {
        let changed = self.connection.execute(
            "UPDATE session_workspaces SET materialization_intent=1 WHERE owner_id=?1 AND client_id=?2 AND session_id=?3 AND managed_root IS NOT NULL AND directory IS NULL AND EXISTS(SELECT 1 FROM service_jobs j JOIN runs r USING(owner_id,client_id,run_id) JOIN run_workspaces w USING(owner_id,client_id,run_id) WHERE j.owner_id=?1 AND j.client_id=?2 AND j.run_id=?4 AND j.claim_token=?5 AND r.session_id=?3 AND r.state='preparing' AND w.held=1)",
            params![job.namespace.owner_id, job.namespace.client_id, job.session_id, job.run_id, job.claim_token],
        )?;
        if changed != 1 {
            return Err(StorageError::InvalidState);
        }
        Ok(())
    }

    pub(crate) fn workspace_clear_uncreated(
        &mut self,
        job: &ClaimedJob,
    ) -> Result<(), StorageError> {
        let changed = self.connection.execute(
            "UPDATE session_workspaces SET materialization_intent=0 WHERE owner_id=?1 AND client_id=?2 AND session_id=?3 AND managed_root IS NOT NULL AND directory IS NULL AND materialization_intent=1 AND EXISTS(SELECT 1 FROM service_jobs j JOIN runs r USING(owner_id,client_id,run_id) WHERE j.owner_id=?1 AND j.client_id=?2 AND j.run_id=?4 AND j.claim_token=?5 AND r.session_id=?3 AND r.state='preparing')",
            params![job.namespace.owner_id, job.namespace.client_id, job.session_id, job.run_id, job.claim_token],
        )?;
        if changed != 1 {
            return Err(StorageError::InvalidState);
        }
        Ok(())
    }

    pub(crate) fn workspace_store_binding(
        &mut self,
        job: &ClaimedJob,
        plan: &WorkspacePlan,
        directory: &Directory,
    ) -> Result<(), StorageError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let encoded = serde_json::to_vec(plan).map_err(|_| StorageError::InvalidState)?;
        let binding = serde_json::to_vec(directory).map_err(|_| StorageError::InvalidState)?;
        let changed = tx.execute(
            "UPDATE session_workspaces SET directory=?1 WHERE owner_id=?2 AND client_id=?3 AND session_id=?4 AND plan=?5 AND (directory IS NULL OR directory=?1) AND EXISTS(SELECT 1 FROM service_jobs j JOIN runs r USING(owner_id,client_id,run_id) JOIN run_workspaces w USING(owner_id,client_id,run_id) WHERE j.owner_id=?2 AND j.client_id=?3 AND j.run_id=?6 AND j.claim_token=?7 AND r.session_id=?4 AND r.state='preparing' AND w.held=1)",
            params![binding, job.namespace.owner_id, job.namespace.client_id, job.session_id, encoded, job.run_id, job.claim_token],
        )?;
        if changed != 1 {
            return Err(StorageError::InvalidState);
        }
        pin_ancestors(&tx, &job.namespace, &job.run_id, &directory.ancestors, true)?;
        tx.commit()?;
        Ok(())
    }
}
