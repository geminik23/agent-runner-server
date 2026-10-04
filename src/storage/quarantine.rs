//! Read and release terminal runs that still protect a session or workspace.

use rusqlite::{TransactionBehavior, params};

use super::{
    queries::load_run,
    sqlite::{SqliteStore, nonempty, validate_namespace},
    types::{Namespace, StorageError},
};

/// Explains what a quarantined terminal run still blocks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QuarantineKind {
    /// A CLI started but its native session ID was never recorded; every admission in the state realm is refused.
    UnboundSession,
    /// The native session ID is known but cleanup was not confirmed; new sessions in the state realm and this session are refused.
    BoundSession,
    /// The workspace was left uncertain before any backend started; the workspace and session stay reserved.
    UnstartedWorkspace,
    /// The protection has a shape that no maintenance release handles; it stays quarantined.
    Unrecognized,
}

impl QuarantineKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UnboundSession => "unbound_session",
            Self::BoundSession => "bound_session",
            Self::UnstartedWorkspace => "unstarted_workspace",
            Self::Unrecognized => "unrecognized",
        }
    }
}

/// Describes one terminal run whose session reservation or workspace hold was not released.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuarantinedRun {
    pub namespace: Namespace,
    pub run_id: String,
    pub session_id: Option<String>,
    pub runtime: String,
    pub state_realm: Option<String>,
    pub state: String,
    pub stop_cause: Option<String>,
    pub native_id: Option<String>,
    pub launched: bool,
    /// Recorded session cleanup; `settled` means no process from the run remains and only this session stays reserved.
    pub cleanup: Option<String>,
    /// Configured workspace ID pinned at admission; maintenance resolves it to the visible directory.
    pub workspace_id: Option<String>,
    /// Stored workspace resource key (`<device>:<path below the mount root>`), not a path to open.
    pub workspace_key: Option<String>,
    pub kind: QuarantineKind,
}

const TERMINAL: &str = "('succeeded','failed','cancelled','timed_out','interrupted')";

impl SqliteStore {
    /// Lists terminal runs that still hold a session reservation or a workspace.
    /// Non-terminal runs are excluded because daemon recovery decides their outcome first.
    pub fn quarantined_runs(&self) -> Result<Vec<QuarantinedRun>, StorageError> {
        let sql = format!(
            "SELECT r.owner_id, r.client_id, r.run_id, r.session_id, r.runtime, r.state, j.stop_cause, \
             EXISTS(SELECT 1 FROM attempts a WHERE a.owner_id=r.owner_id AND a.client_id=r.client_id AND a.run_id=r.run_id AND a.spawn_intent=1), \
             s.state_realm, s.native_id, s.reserved_run_id=r.run_id, w.resource_path, COALESCE(w.held,0), COALESCE(w.uncertain,0), j.workspace_id, s.cleanup \
             FROM runs r \
             LEFT JOIN service_jobs j USING (owner_id, client_id, run_id) \
             LEFT JOIN sessions s ON s.owner_id=r.owner_id AND s.client_id=r.client_id AND s.session_id=r.session_id \
             LEFT JOIN run_workspaces w ON w.owner_id=r.owner_id AND w.client_id=r.client_id AND w.run_id=r.run_id \
             WHERE r.state IN {TERMINAL} AND (s.reserved_run_id=r.run_id OR w.held=1 OR w.uncertain=1) \
             ORDER BY r.owner_id, r.client_id, r.run_id"
        );
        let mut statement = self.connection.prepare(&sql)?;
        let rows = statement.query_map([], |row| {
            let launched: bool = row.get(7)?;
            let native_id: Option<String> = row.get(9)?;
            let reserved: Option<bool> = row.get(10)?;
            let held: bool = row.get(12)?;
            let uncertain: bool = row.get(13)?;
            let reserved = reserved.unwrap_or(false);
            let kind = match (reserved, launched, &native_id) {
                (true, true, None) => QuarantineKind::UnboundSession,
                (true, _, Some(_)) => QuarantineKind::BoundSession,
                (true, false, None) if held && uncertain => QuarantineKind::UnstartedWorkspace,
                _ => QuarantineKind::Unrecognized,
            };
            Ok(QuarantinedRun {
                namespace: Namespace {
                    owner_id: row.get(0)?,
                    client_id: row.get(1)?,
                },
                run_id: row.get(2)?,
                session_id: row.get(3)?,
                runtime: row.get(4)?,
                state: row.get(5)?,
                stop_cause: row.get(6)?,
                launched,
                state_realm: row.get(8)?,
                native_id,
                workspace_id: row.get(14)?,
                cleanup: row.get(15)?,
                workspace_key: row.get(11)?,
                kind,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    /// Gives up a launched session whose native ID was never recorded and releases its reservation and workspace.
    /// The session can never be resumed afterwards; any native transcript the CLI wrote stays untouched and unreferenced.
    /// The caller must hold the service locks and verify independently that no process from the run remains.
    pub fn abandon_unbound_session(
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
        if !matches!(
            run.state.as_str(),
            "succeeded" | "failed" | "cancelled" | "timed_out" | "interrupted"
        ) {
            return Err(StorageError::InvalidState);
        }
        let session_id = run.session_id.ok_or(StorageError::InvalidState)?;
        let launched: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM attempts WHERE owner_id=?1 AND client_id=?2 AND run_id=?3 AND spawn_intent=1)",
            params![namespace.owner_id, namespace.client_id, run_id],
            |row| row.get(0),
        )?;
        if !launched {
            tx.commit()?;
            return Ok(false);
        }
        // The disposition stays uncertain and the native ID stays absent, so no admission can resume this session.
        let updated = tx.execute(
            "UPDATE sessions SET cleanup='settled', disposition='uncertain', reserved_run_id=NULL WHERE owner_id=?1 AND client_id=?2 AND session_id=?3 AND reserved_run_id=?4 AND native_id IS NULL",
            params![namespace.owner_id, namespace.client_id, session_id, run_id],
        )?;
        if updated == 1 {
            tx.execute(
                "UPDATE run_workspaces SET held=0, uncertain=0 WHERE owner_id=?1 AND client_id=?2 AND run_id=?3",
                params![namespace.owner_id, namespace.client_id, run_id],
            )?;
            tx.execute(
                "INSERT INTO workspace_settlements(owner_id,client_id,run_id) SELECT owner_id,client_id,run_id FROM run_workspaces WHERE owner_id=?1 AND client_id=?2 AND run_id=?3 ON CONFLICT(owner_id,client_id,run_id) DO NOTHING",
                params![namespace.owner_id, namespace.client_id, run_id],
            )?;
        }
        tx.commit()?;
        Ok(updated == 1)
    }
}
