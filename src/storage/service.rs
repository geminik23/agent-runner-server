use std::{
    collections::HashSet,
    time::{SystemTime, UNIX_EPOCH},
};

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde_json::{Value, json};

use crate::core::{CleanupState, ExecutionMetadata, SessionDisposition, SessionRef};

use super::{
    queries::{load_run_by_key, load_session},
    sqlite::SqliteStore,
    types::{
        AdmissionOutcome, EventSummary, Namespace, SkillRef, StorageError, StoredResult, StoredRun,
        TerminalState,
    },
};

type StopLookup = (String, Option<String>, Option<String>, Option<i64>);
type LimitedResultRow = (
    Option<String>,
    Option<Vec<u8>>,
    Option<String>,
    bool,
    Option<String>,
    bool,
);
type LimitedRunRow = (
    Option<String>,
    Option<String>,
    Option<String>,
    bool,
    Option<String>,
    bool,
);

pub(crate) struct ServiceAdmission {
    pub namespace: Namespace,
    pub idempotency_key: String,
    pub normalized_request: Vec<u8>,
    pub manifest: Vec<u8>,
    pub metadata: ExecutionMetadata,
    pub credential_id: String,
    pub credential_generation: String,
    pub profile_id: String,
    pub profile_revision: String,
    pub workspace_id: String,
    pub state_realm: String,
    pub workspace: Option<crate::workspace::WorkspacePlan>,
    pub independent_new_sessions: bool,
    pub resume_session_id: Option<String>,
    pub resume_native_id: Option<String>,
    pub deadline_ms: i64,
    pub retention_ms: Option<i64>,
    pub max_outstanding: usize,
    pub skills: Option<Vec<SkillRef>>,
    pub may_use_skills: bool,
    pub skill_bounds: super::skills::SkillBounds,
    pub hooks: Option<Vec<super::HookRef>>,
    pub may_use_hooks: bool,
    pub destination_grants: HashSet<String>,
    pub delivery: Option<crate::delivery::DeliveryConfig>,
}

#[derive(Clone, Debug)]
pub(crate) struct ClaimedJob {
    pub namespace: Namespace,
    pub run_id: String,
    pub attempt_id: String,
    pub session_id: String,
    pub credential_id: String,
    pub credential_generation: String,
    pub profile_id: String,
    pub workspace_id: String,
    pub state_realm: String,
    pub deadline_ms: i64,
    pub claim_token: String,
    pub manifest: Vec<u8>,
    pub metadata: ExecutionMetadata,
    pub native_id: Option<String>,
}

#[derive(Clone, Copy)]
pub(crate) enum ServiceClock {
    Wall,
    Fixed(i64),
}
impl From<i64> for ServiceClock {
    fn from(value: i64) -> Self {
        Self::Fixed(value)
    }
}
impl ServiceClock {
    pub(super) fn read(self) -> Result<i64, StorageError> {
        match self {
            Self::Wall => wall_clock_ms(),
            Self::Fixed(value) => Ok(value),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StopCause {
    Cancelled,
    TimedOut,
    Interrupted,
}

impl StopCause {
    fn as_str(self) -> &'static str {
        match self {
            Self::Cancelled => "cancelled",
            Self::TimedOut => "timed_out",
            Self::Interrupted => "interrupted",
        }
    }

    fn from_str(value: &str) -> Result<Self, StorageError> {
        match value {
            "cancelled" => Ok(Self::Cancelled),
            "timed_out" => Ok(Self::TimedOut),
            "interrupted" => Ok(Self::Interrupted),
            _ => Err(StorageError::InvalidState),
        }
    }

    fn terminal(self) -> TerminalState {
        match self {
            Self::Cancelled => TerminalState::Cancelled,
            Self::TimedOut => TerminalState::TimedOut,
            Self::Interrupted => TerminalState::Interrupted,
        }
    }
}

struct FinishContext {
    stop: Option<StopCause>,
    max_result_bytes: usize,
    now_ms: i64,
}

pub(crate) struct Candidate {
    pub state: TerminalState,
    pub details: Value,
    pub cleanup: CleanupState,
    pub disposition: SessionDisposition,
}

impl Candidate {
    pub fn stopped() -> Self {
        Self {
            state: TerminalState::Interrupted,
            details: Value::Null,
            cleanup: CleanupState::Settled,
            disposition: SessionDisposition::NotApplicable,
        }
    }
}

impl SqliteStore {
    pub(crate) fn service_replay(
        &self,
        namespace: &Namespace,
        key: &str,
        normalized: &[u8],
    ) -> Result<Option<StoredRun>, StorageError> {
        match load_run_by_key(&self.connection, namespace, key)? {
            Some(run) if run.normalized_request != normalized => {
                Err(StorageError::IdempotencyConflict)
            }
            Some(run) => {
                let managed: bool = self.connection.query_row(
                    "SELECT EXISTS(SELECT 1 FROM service_jobs WHERE owner_id = ?1 AND client_id = ?2 AND run_id = ?3)",
                    params![namespace.owner_id, namespace.client_id, run.run_id], |row| row.get(0),
                )?;
                if managed {
                    Ok(Some(run))
                } else {
                    Err(StorageError::InvalidState)
                }
            }
            None => Ok(None),
        }
    }

    pub(crate) fn service_admit(
        &mut self,
        admission: &ServiceAdmission,
    ) -> Result<AdmissionOutcome, StorageError> {
        let ns = &admission.namespace;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing) = load_run_by_key(&tx, ns, &admission.idempotency_key)? {
            if existing.normalized_request != admission.normalized_request {
                return Err(StorageError::IdempotencyConflict);
            }
            let managed: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM service_jobs WHERE owner_id = ?1 AND client_id = ?2 AND run_id = ?3)",
                params![ns.owner_id, ns.client_id, existing.run_id], |row| row.get(0),
            )?;
            if !managed {
                return Err(StorageError::InvalidState);
            }
            tx.commit()?;
            return Ok(AdmissionOutcome::Existing(existing));
        }
        let selected_skills = super::skills::resolve(
            &tx,
            ns,
            admission.skills.as_deref(),
            admission.may_use_skills,
            admission.skill_bounds,
        )?;
        let selected_hooks = super::hooks::resolve(&tx, admission)?;
        let outstanding: i64 = tx.query_row(
            "SELECT COUNT(*) FROM service_jobs j JOIN runs r USING (owner_id, client_id, run_id) WHERE r.state IN ('queued', 'preparing', 'running', 'validating')",
            [],
            |row| row.get(0),
        )?;
        if usize::try_from(outstanding).map_or(true, |count| count >= admission.max_outstanding) {
            return Err(StorageError::QueueFull);
        }
        let resume_session_id = match (&admission.resume_session_id, &admission.resume_native_id) {
            (Some(_), Some(_)) => return Err(StorageError::InvalidInput("session")),
            (Some(id), None) => Some(id.clone()),
            (None, Some(native_id)) => Some(tx.query_row(
                "SELECT session_id FROM sessions WHERE owner_id=?1 AND client_id=?2 AND runtime=?3 AND state_realm=?4 AND native_id=?5",
                params![ns.owner_id, ns.client_id, admission.metadata.runtime, admission.state_realm, native_id],
                |row| row.get::<_, String>(0),
            ).optional()?.ok_or(StorageError::NotFound)?),
            (None, None) => None,
        };
        let quarantined: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM sessions s LEFT JOIN service_jobs j ON j.owner_id = s.owner_id AND j.client_id = s.client_id AND j.run_id = s.reserved_run_id LEFT JOIN attempts a ON a.owner_id = j.owner_id AND a.client_id = j.client_id AND a.run_id = j.run_id AND a.attempt_id = j.attempt_id LEFT JOIN runs r ON r.owner_id = s.owner_id AND r.client_id = s.client_id AND r.run_id = s.reserved_run_id WHERE s.runtime = ?1 AND s.state_realm = ?2 AND s.reserved_run_id IS NOT NULL AND ((s.native_id IS NULL AND (j.queue_id IS NULL OR a.spawn_intent = 1 OR (s.cleanup='uncertain' AND r.state IN ('succeeded','failed','cancelled','timed_out','interrupted'))) AND NOT (?4 AND COALESCE(j.independent_new_sessions,0)=1 AND r.state IN ('queued','preparing','running','validating') AND j.stop_cause IS NULL)) OR (?3 AND s.native_id IS NOT NULL AND COALESCE(s.cleanup,'uncertain')<>'settled' AND r.state IN ('succeeded', 'failed', 'cancelled', 'timed_out', 'interrupted'))))",
            params![admission.metadata.runtime, admission.state_realm, resume_session_id.is_none(), admission.independent_new_sessions],
            |row| row.get(0),
        )?;
        if quarantined {
            return Err(StorageError::SessionBusy);
        }
        tx.execute(
            "INSERT INTO namespaces (owner_id, client_id) VALUES (?1, ?2) ON CONFLICT DO NOTHING",
            params![ns.owner_id, ns.client_id],
        )?;
        let run_id = new_id(&tx, "run")?;
        let attempt_id = new_id(&tx, "attempt")?;
        let session_id = if let Some(id) = &resume_session_id {
            let session = load_session(&tx, ns, id)?.ok_or(StorageError::NotFound)?;
            if session.reserved_run_id.is_some() {
                return Err(StorageError::SessionBusy);
            }
            if session.session.native_id.is_none()
                || session.session.runtime != admission.metadata.runtime
                || session.session.state_realm != admission.state_realm
            {
                return Err(StorageError::SessionIncompatible);
            }
            let compatible: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM service_sessions WHERE owner_id = ?1 AND client_id = ?2 AND session_id = ?3 AND profile_id = ?4 AND profile_revision = ?5 AND workspace_id = ?6)",
                params![ns.owner_id, ns.client_id, id, admission.profile_id, admission.profile_revision, admission.workspace_id],
                |row| row.get(0),
            )?;
            if !compatible {
                return Err(StorageError::SessionIncompatible);
            }
            if super::skills::bundle(&tx, "session", ns, id, admission.skill_bounds)?
                != selected_skills
            {
                return Err(StorageError::SkillBundleConflict);
            }
            id.clone()
        } else {
            let id = new_id(&tx, "session")?;
            tx.execute(
                "INSERT INTO sessions (owner_id, client_id, session_id, runtime, state_realm) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![ns.owner_id, ns.client_id, id, admission.metadata.runtime, admission.state_realm],
            )?;
            tx.execute(
                "INSERT INTO service_sessions (owner_id, client_id, session_id, profile_id, profile_revision, workspace_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![ns.owner_id, ns.client_id, id, admission.profile_id, admission.profile_revision, admission.workspace_id],
            )?;
            for (position, skill) in selected_skills.iter().enumerate() {
                tx.execute(
                    "INSERT INTO session_skills (owner_id, client_id, session_id, position, skill_id, version) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![ns.owner_id, ns.client_id, id, position as i64, skill.skill_id, skill.version],
                )?;
            }
            id
        };
        tx.execute(
            "INSERT INTO runs (owner_id, client_id, run_id, session_id, idempotency_key, normalized_request, manifest, runtime, synthetic, scenario, state) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 'queued')",
            params![ns.owner_id, ns.client_id, run_id, session_id, admission.idempotency_key, admission.normalized_request, admission.manifest, admission.metadata.runtime, admission.metadata.synthetic, admission.metadata.scenario],
        )?;
        super::workspaces::pin(
            &tx,
            ns,
            &run_id,
            &session_id,
            resume_session_id.is_some(),
            admission.workspace.as_ref(),
        )?;
        if let Some(retention_ms) = admission.retention_ms {
            if retention_ms <= 0 {
                return Err(StorageError::InvalidInput("retention"));
            }
            if tx.execute(
                "INSERT INTO run_retention (owner_id,client_id,run_id,retention_ms) VALUES (?1,?2,?3,?4)",
                params![ns.owner_id, ns.client_id, run_id, retention_ms],
            )? != 1 {
                return Err(StorageError::InvalidState);
            }
        }
        if !selected_hooks.is_empty() {
            super::hooks::pin(
                &tx,
                ns,
                &run_id,
                &selected_hooks,
                admission
                    .delivery
                    .as_ref()
                    .ok_or(StorageError::InvalidState)?,
            )?;
        }
        for (position, skill) in selected_skills.iter().enumerate() {
            tx.execute(
                "INSERT INTO run_skills (owner_id, client_id, run_id, skill_id, version, position) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![ns.owner_id, ns.client_id, run_id, skill.skill_id, skill.version, position as i64],
            )?;
        }
        tx.execute(
            "INSERT INTO attempts (owner_id, client_id, run_id, attempt_id, state) VALUES (?1, ?2, ?3, ?4, 'queued')",
            params![ns.owner_id, ns.client_id, run_id, attempt_id],
        )?;
        if tx.execute(
            "UPDATE sessions SET reserved_run_id = ?1 WHERE owner_id = ?2 AND client_id = ?3 AND session_id = ?4 AND reserved_run_id IS NULL",
            params![run_id, ns.owner_id, ns.client_id, session_id],
        )? != 1 {
            return Err(StorageError::SessionBusy);
        }
        tx.execute(
            "INSERT INTO service_jobs (owner_id, client_id, run_id, attempt_id, credential_id, credential_generation, profile_id, workspace_id, state_realm, deadline_ms, independent_new_sessions) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![ns.owner_id, ns.client_id, run_id, attempt_id, admission.credential_id, admission.credential_generation, admission.profile_id, admission.workspace_id, admission.state_realm, admission.deadline_ms, admission.independent_new_sessions],
        )?;
        tx.execute(
            "INSERT INTO events (owner_id, client_id, run_id, event_id, sequence, kind, payload) VALUES (?1, ?2, ?3, ?4, 0, 'run.queued', ?5)",
            params![ns.owner_id, ns.client_id, run_id, format!("{run_id}:queued"), b"{}".as_slice()],
        )?;
        let stored =
            super::queries::load_run(&tx, ns, &run_id)?.ok_or(StorageError::InvalidState)?;
        tx.commit()?;
        Ok(AdmissionOutcome::Accepted(stored))
    }

    pub(crate) fn service_deadline_maintenance(
        &mut self,
        now: impl Into<ServiceClock>,
    ) -> Result<Option<i64>, StorageError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        expire_waiting(&tx, now.into().read()?)?;
        let next = tx.query_row(
            "SELECT MIN(j.deadline_ms) FROM service_jobs j JOIN runs r USING (owner_id, client_id, run_id) WHERE r.state = 'queued' AND j.claim_token IS NULL",
            [],
            |row| row.get(0),
        )?;
        tx.commit()?;
        Ok(next)
    }

    pub(crate) fn service_claim(
        &mut self,
        now: impl Into<ServiceClock>,
    ) -> Result<Option<ClaimedJob>, StorageError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now_ms = now.into().read()?;
        expire_waiting(&tx, now_ms)?;
        let candidate = tx.query_row(
            "SELECT j.owner_id, j.client_id, j.run_id, j.attempt_id, r.session_id, j.credential_id, j.profile_id, j.workspace_id, j.state_realm, j.deadline_ms, r.manifest, r.runtime, r.synthetic, r.scenario, s.native_id, j.credential_generation
             FROM service_jobs j JOIN runs r USING (owner_id, client_id, run_id)
             JOIN sessions s ON s.owner_id=r.owner_id AND s.client_id=r.client_id AND s.session_id=r.session_id
             LEFT JOIN run_workspaces wanted ON wanted.owner_id=j.owner_id AND wanted.client_id=j.client_id AND wanted.run_id=j.run_id
             WHERE r.state='queued' AND j.claim_token IS NULL AND j.stop_cause IS NULL
             AND NOT EXISTS (
                 SELECT 1 FROM sessions other
                 LEFT JOIN service_jobs oj ON oj.owner_id=other.owner_id AND oj.client_id=other.client_id AND oj.run_id=other.reserved_run_id
                 LEFT JOIN attempts oa ON oa.owner_id=oj.owner_id AND oa.client_id=oj.client_id AND oa.run_id=oj.run_id AND oa.attempt_id=oj.attempt_id
                 LEFT JOIN runs other_run ON other_run.owner_id=oj.owner_id AND other_run.client_id=oj.client_id AND other_run.run_id=oj.run_id
                 WHERE other.runtime=r.runtime AND other.state_realm=j.state_realm AND other.reserved_run_id IS NOT NULL
                 AND NOT (other.owner_id=j.owner_id AND other.client_id=j.client_id AND other.reserved_run_id=j.run_id)
                 AND ((other.native_id IS NULL AND (oj.queue_id IS NULL OR oj.claim_token IS NOT NULL OR oa.spawn_intent=1 OR (other.cleanup='uncertain' AND other_run.state IN ('succeeded','failed','cancelled','timed_out','interrupted')))) OR (s.native_id IS NULL AND other.native_id IS NOT NULL AND NOT (COALESCE(other.cleanup,'uncertain')='settled' AND other_run.state IN ('succeeded','failed','cancelled','timed_out','interrupted'))))
                 AND NOT (j.independent_new_sessions=1 AND COALESCE(oj.independent_new_sessions,0)=1 AND other_run.state IN ('queued','preparing','running','validating') AND oj.stop_cause IS NULL)
             )
             AND NOT EXISTS (
                 SELECT 1 FROM service_jobs blocked JOIN runs br USING (owner_id,client_id,run_id)
                 JOIN sessions bs ON bs.owner_id=br.owner_id AND bs.client_id=br.client_id AND bs.session_id=br.session_id
                 WHERE blocked.workspace_id=j.workspace_id AND bs.reserved_run_id=br.run_id AND bs.cleanup='uncertain'
                 AND br.state IN ('succeeded','failed','cancelled','timed_out','interrupted')
                 AND (wanted.resource_path IS NULL OR NOT EXISTS(SELECT 1 FROM run_workspaces bw WHERE bw.owner_id=blocked.owner_id AND bw.client_id=blocked.client_id AND bw.run_id=blocked.run_id))
             )
             AND NOT EXISTS (
                 SELECT 1 FROM run_workspaces held
                 WHERE wanted.resource_path IS NOT NULL AND held.held=1
                 AND (held.resource_path=wanted.resource_path OR substr(held.resource_path,1,length(wanted.resource_path)+1)=wanted.resource_path||'/' OR substr(wanted.resource_path,1,length(held.resource_path)+1)=held.resource_path||'/'
                   OR EXISTS(SELECT 1 FROM workspace_ancestors ha JOIN workspace_ancestors wa ON ha.device=wa.device AND ha.inode=wa.inode WHERE ha.owner_id=held.owner_id AND ha.client_id=held.client_id AND ha.run_id=held.run_id AND wa.owner_id=wanted.owner_id AND wa.client_id=wanted.client_id AND wa.run_id=wanted.run_id AND (ha.is_target=1 OR wa.is_target=1)))
                 AND (held.uncertain=1 OR held.access='exclusive' OR wanted.access='exclusive')
             )
             AND NOT EXISTS (
                 SELECT 1 FROM run_workspaces waiting
                 JOIN service_jobs older ON older.owner_id=waiting.owner_id AND older.client_id=waiting.client_id AND older.run_id=waiting.run_id
                 JOIN runs older_run ON older_run.owner_id=older.owner_id AND older_run.client_id=older.client_id AND older_run.run_id=older.run_id
                 WHERE wanted.resource_path IS NOT NULL AND older.queue_id<j.queue_id AND older.claim_token IS NULL AND older.stop_cause IS NULL AND older.deadline_ms>?1 AND older_run.state='queued'
                 AND (waiting.resource_path=wanted.resource_path OR substr(waiting.resource_path,1,length(wanted.resource_path)+1)=wanted.resource_path||'/' OR substr(wanted.resource_path,1,length(waiting.resource_path)+1)=waiting.resource_path||'/'
                   OR EXISTS(SELECT 1 FROM workspace_ancestors ha JOIN workspace_ancestors wa ON ha.device=wa.device AND ha.inode=wa.inode WHERE ha.owner_id=waiting.owner_id AND ha.client_id=waiting.client_id AND ha.run_id=waiting.run_id AND wa.owner_id=wanted.owner_id AND wa.client_id=wanted.client_id AND wa.run_id=wanted.run_id AND (ha.is_target=1 OR wa.is_target=1)))
                 AND (waiting.access='exclusive' OR wanted.access='exclusive')
             ) ORDER BY j.queue_id LIMIT 1",
            [now_ms],
            |row| Ok(ClaimedJob {
                namespace: Namespace { owner_id: row.get(0)?, client_id: row.get(1)? },
                run_id: row.get(2)?, attempt_id: row.get(3)?, session_id: row.get(4)?,
                credential_id: row.get(5)?, profile_id: row.get(6)?, workspace_id: row.get(7)?,
                state_realm: row.get(8)?, deadline_ms: row.get(9)?, claim_token: String::new(), manifest: row.get(10)?,
                metadata: ExecutionMetadata { runtime: row.get(11)?, synthetic: row.get(12)?, scenario: row.get(13)? },
                native_id: row.get(14)?, credential_generation: row.get(15)?,
            }),
        ).optional()?;
        let Some(mut job) = candidate else {
            tx.commit()?;
            return Ok(None);
        };
        job.claim_token = new_id(&tx, "claim")?;
        if tx.execute(
            "UPDATE service_jobs SET claim_token = ?1 WHERE owner_id = ?2 AND client_id = ?3 AND run_id = ?4 AND claim_token IS NULL",
            params![job.claim_token, job.namespace.owner_id, job.namespace.client_id, job.run_id],
        )? != 1 {
            return Err(StorageError::InvalidState);
        }
        tx.execute("UPDATE run_workspaces SET held=1 WHERE owner_id=?1 AND client_id=?2 AND run_id=?3 AND held=0", params![job.namespace.owner_id, job.namespace.client_id, job.run_id])?;
        tx.execute("UPDATE runs SET state = 'preparing' WHERE owner_id = ?1 AND client_id = ?2 AND run_id = ?3 AND state = 'queued'", params![job.namespace.owner_id, job.namespace.client_id, job.run_id])?;
        tx.execute("UPDATE attempts SET state = 'preparing' WHERE owner_id = ?1 AND client_id = ?2 AND run_id = ?3 AND attempt_id = ?4 AND state = 'queued'", params![job.namespace.owner_id, job.namespace.client_id, job.run_id, job.attempt_id])?;
        tx.commit()?;
        Ok(Some(job))
    }

    pub(crate) fn service_prepare_prompt(
        &mut self,
        job: &ClaimedJob,
        input: &str,
        max_bytes: usize,
        bounds: super::skills::SkillBounds,
        now: impl Into<ServiceClock>,
    ) -> Result<Option<(String, bool)>, StorageError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (stop, deadline, spawn) = job_status(&tx, job)?;
        if stop.is_some() || now.into().read()? >= deadline {
            tx.commit()?;
            return Ok(None);
        }
        if spawn {
            return Err(StorageError::InvalidState);
        }
        let has_skills =
            !super::skills::bundle(&tx, "run", &job.namespace, &job.run_id, bounds)?.is_empty();
        let existing: Option<(bool, Option<Vec<u8>>)> = tx.query_row(
            "SELECT compiler_revision = ?5 AND context = ?6, CASE WHEN length(prompt) <= ?7 THEN prompt ELSE NULL END FROM attempt_prompt_snapshots WHERE owner_id = ?1 AND client_id = ?2 AND run_id = ?3 AND attempt_id = ?4",
            params![job.namespace.owner_id, job.namespace.client_id, job.run_id, job.attempt_id, super::skills::COMPILER_REVISION, b"{\"source\":\"none\"}".as_slice(), i64::try_from(max_bytes).unwrap_or(i64::MAX)],
            |row| Ok((row.get(0)?, row.get(1)?)),
        ).optional()?;
        if let Some((compatible, bytes)) = existing {
            if !compatible {
                return Err(StorageError::InvalidState);
            }
            let bytes = bytes.ok_or(StorageError::InvalidInput("prompt limit"))?;
            let prompt = String::from_utf8(bytes).map_err(|_| StorageError::InvalidState)?;
            tx.commit()?;
            return Ok(Some((prompt, has_skills)));
        }
        let prompt =
            super::skills::compile(&tx, &job.namespace, &job.run_id, input, max_bytes, bounds)?;
        tx.execute(
            "INSERT INTO attempt_prompt_snapshots (owner_id, client_id, run_id, attempt_id, compiler_revision, context, prompt) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![job.namespace.owner_id, job.namespace.client_id, job.run_id, job.attempt_id, super::skills::COMPILER_REVISION, b"{\"source\":\"none\"}".as_slice(), prompt.as_bytes()],
        )?;
        tx.commit()?;
        Ok(Some((prompt, has_skills)))
    }

    pub(crate) fn service_launch(
        &mut self,
        job: &ClaimedJob,
        now: impl Into<ServiceClock>,
    ) -> Result<bool, StorageError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now_ms = now.into().read()?;
        let (stop, deadline, spawn) = job_status(&tx, job)?;
        if stop.is_some() || now_ms >= deadline {
            let cause = stop.unwrap_or(StopCause::TimedOut);
            set_stop(&tx, &job.namespace, &job.run_id, cause)?;
            finish_tx(
                &tx,
                &job.namespace,
                &job.run_id,
                &job.attempt_id,
                &Candidate::stopped(),
                FinishContext {
                    stop: Some(cause),
                    max_result_bytes: 4096,
                    now_ms,
                },
            )?;
            tx.commit()?;
            return Ok(false);
        }
        if spawn {
            return Err(StorageError::InvalidState);
        }
        let unavailable: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM run_skills r LEFT JOIN skills s USING (owner_id, client_id, skill_id) LEFT JOIN skill_policies p USING (owner_id, client_id, skill_id, version) WHERE r.owner_id = ?1 AND r.client_id = ?2 AND r.run_id = ?3 AND (COALESCE(s.enabled, 1) = 0 OR COALESCE(p.enabled, 1) = 0))",
            params![job.namespace.owner_id, job.namespace.client_id, job.run_id], |row| row.get(0),
        )?;
        if unavailable {
            return Err(StorageError::SkillUnavailable);
        }
        let requires_snapshot = serde_json::from_slice::<Value>(&job.manifest)
            .ok()
            .and_then(|v| v.get("version").and_then(Value::as_u64))
            == Some(2);
        if requires_snapshot {
            let prepared: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM attempt_prompt_snapshots WHERE owner_id = ?1 AND client_id = ?2 AND run_id = ?3 AND attempt_id = ?4 AND compiler_revision = ?5)",
                params![job.namespace.owner_id, job.namespace.client_id, job.run_id, job.attempt_id, super::skills::COMPILER_REVISION], |row| row.get(0),
            )?;
            if !prepared {
                return Err(StorageError::InvalidState);
            }
        }
        if tx.execute(
            "UPDATE attempts SET state = 'running', spawn_intent = 1 WHERE owner_id = ?1 AND client_id = ?2 AND run_id = ?3 AND attempt_id = ?4 AND state = 'preparing' AND spawn_intent = 0",
            params![job.namespace.owner_id, job.namespace.client_id, job.run_id, job.attempt_id],
        )? != 1 {
            return Err(StorageError::InvalidState);
        }
        tx.execute("UPDATE runs SET state = 'running' WHERE owner_id = ?1 AND client_id = ?2 AND run_id = ?3 AND state = 'preparing'", params![job.namespace.owner_id, job.namespace.client_id, job.run_id])?;
        tx.commit()?;
        Ok(true)
    }

    pub(crate) fn service_proven_unstarted(
        &mut self,
        job: &ClaimedJob,
        now: impl Into<ServiceClock>,
    ) -> Result<(), StorageError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now_ms = now.into().read()?;
        let (stop, deadline, spawn) = job_status(&tx, job)?;
        if !spawn {
            return Err(StorageError::InvalidState);
        }
        let has_observations: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM events WHERE owner_id = ?1 AND client_id = ?2 AND run_id = ?3 AND kind <> 'run.queued')",
            params![job.namespace.owner_id, job.namespace.client_id, job.run_id], |row| row.get(0),
        )?;
        if has_observations {
            return Err(StorageError::InvalidState);
        }
        if tx.execute(
            "UPDATE attempts SET spawn_intent = 0 WHERE owner_id = ?1 AND client_id = ?2 AND run_id = ?3 AND attempt_id = ?4 AND spawn_intent = 1",
            params![job.namespace.owner_id, job.namespace.client_id, job.run_id, job.attempt_id],
        )? != 1 { return Err(StorageError::InvalidState); }
        let cause = stop.unwrap_or(if now_ms >= deadline {
            StopCause::TimedOut
        } else {
            StopCause::Interrupted
        });
        set_stop(&tx, &job.namespace, &job.run_id, cause)?;
        finish_tx(
            &tx,
            &job.namespace,
            &job.run_id,
            &job.attempt_id,
            &Candidate::stopped(),
            FinishContext {
                stop: Some(cause),
                max_result_bytes: 4096,
                now_ms,
            },
        )?;
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn service_finish_proven_unstarted(
        &mut self,
        job: &ClaimedJob,
        candidate: &Candidate,
        now: impl Into<ServiceClock>,
        max_result_bytes: usize,
    ) -> Result<(), StorageError> {
        if candidate.state == TerminalState::Succeeded
            || candidate.cleanup != CleanupState::Settled
            || candidate.disposition != SessionDisposition::NotApplicable
        {
            return Err(StorageError::InvalidState);
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now_ms = now.into().read()?;
        let (mut stop, deadline, spawn) = job_status(&tx, job)?;
        if !spawn {
            return Err(StorageError::InvalidState);
        }
        let observations: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM events WHERE owner_id=?1 AND client_id=?2 AND run_id=?3 AND kind<>'run.queued')",
            params![job.namespace.owner_id,job.namespace.client_id,job.run_id], |row| row.get(0),
        )?;
        if observations {
            return Err(StorageError::InvalidState);
        }
        if tx.execute("UPDATE attempts SET spawn_intent=0 WHERE owner_id=?1 AND client_id=?2 AND run_id=?3 AND attempt_id=?4 AND spawn_intent=1", params![job.namespace.owner_id,job.namespace.client_id,job.run_id,job.attempt_id])? != 1 { return Err(StorageError::InvalidState); }
        if stop.is_none() && now_ms >= deadline {
            set_stop(&tx, &job.namespace, &job.run_id, StopCause::TimedOut)?;
            stop = Some(StopCause::TimedOut);
        }
        finish_tx(
            &tx,
            &job.namespace,
            &job.run_id,
            &job.attempt_id,
            candidate,
            FinishContext {
                stop,
                max_result_bytes,
                now_ms,
            },
        )?;
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn service_stop(
        &mut self,
        namespace: &Namespace,
        run_id: &str,
        cause: StopCause,
        now: impl Into<ServiceClock>,
    ) -> Result<Option<StopCause>, StorageError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now_ms = now.into().read()?;
        let row: Option<StopLookup> = tx.query_row(
            "SELECT r.state, j.claim_token, j.attempt_id, j.deadline_ms FROM runs r LEFT JOIN service_jobs j USING (owner_id, client_id, run_id) WHERE r.owner_id = ?1 AND r.client_id = ?2 AND r.run_id = ?3",
            params![namespace.owner_id, namespace.client_id, run_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        ).optional()?;
        let Some((state, claim, attempt_id, deadline)) = row else {
            return Err(StorageError::NotFound);
        };
        if is_terminal(&state) {
            tx.commit()?;
            return Ok(None);
        }
        let attempt_id = attempt_id.ok_or(StorageError::InvalidState)?;
        let deadline = deadline.ok_or(StorageError::InvalidState)?;
        if now_ms >= deadline {
            set_stop(&tx, namespace, run_id, StopCause::TimedOut)?;
        }
        set_stop(&tx, namespace, run_id, cause)?;
        let actual = load_stop(&tx, namespace, run_id)?.ok_or(StorageError::InvalidState)?;
        if state == "queued" && claim.is_none() {
            finish_tx(
                &tx,
                namespace,
                run_id,
                &attempt_id,
                &Candidate::stopped(),
                FinishContext {
                    stop: Some(actual),
                    max_result_bytes: 4096,
                    now_ms,
                },
            )?;
        }
        tx.commit()?;
        Ok(Some(actual))
    }

    pub(crate) fn service_status(
        &mut self,
        job: &ClaimedJob,
        now: impl Into<ServiceClock>,
    ) -> Result<Option<StopCause>, StorageError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now_ms = now.into().read()?;
        let (mut cause, deadline, _) = job_status(&tx, job)?;
        if cause.is_none() && now_ms >= deadline {
            set_stop(&tx, &job.namespace, &job.run_id, StopCause::TimedOut)?;
            cause = Some(StopCause::TimedOut);
        }
        tx.commit()?;
        Ok(cause)
    }

    pub(crate) fn service_event(
        &mut self,
        job: &ClaimedJob,
        kind: &str,
        payload: &[u8],
        session: Option<&SessionRef>,
        max_events: usize,
    ) -> Result<(), StorageError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (_, _, spawn) = job_status(&tx, job)?;
        if !spawn || kind == "run.queued" || kind == "run.finished" {
            return Err(StorageError::InvalidState);
        }
        let count: i64 = tx.query_row(
            "SELECT COUNT(*) FROM events WHERE owner_id = ?1 AND client_id = ?2 AND run_id = ?3",
            params![job.namespace.owner_id, job.namespace.client_id, job.run_id],
            |row| row.get(0),
        )?;
        if usize::try_from(count).map_or(true, |value| value >= max_events.saturating_sub(1)) {
            return Err(StorageError::InvalidInput("event limit"));
        }
        if let Some(native) = session {
            if native.runtime != job.metadata.runtime
                || native.state_realm != job.state_realm
                || native.native_id.is_empty()
            {
                return Err(StorageError::InvalidInput("session identity"));
            }
            let current = load_session(&tx, &job.namespace, &job.session_id)?
                .ok_or(StorageError::NotFound)?;
            if current.reserved_run_id.as_deref() != Some(&job.run_id)
                || current
                    .session
                    .native_id
                    .as_deref()
                    .is_some_and(|id| id != native.native_id)
            {
                return Err(StorageError::InvalidState);
            }
            tx.execute("UPDATE sessions SET native_id = ?1 WHERE owner_id = ?2 AND client_id = ?3 AND session_id = ?4 AND reserved_run_id = ?5", params![native.native_id, job.namespace.owner_id, job.namespace.client_id, job.session_id, job.run_id])?;
        }
        let sequence: i64 = tx.query_row("SELECT COALESCE(MAX(sequence), -1) + 1 FROM events WHERE owner_id = ?1 AND client_id = ?2 AND run_id = ?3", params![job.namespace.owner_id, job.namespace.client_id, job.run_id], |row| row.get(0))?;
        tx.execute("INSERT INTO events (owner_id, client_id, run_id, event_id, sequence, kind, payload) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)", params![job.namespace.owner_id, job.namespace.client_id, job.run_id, format!("{}:event:{sequence}", job.run_id), sequence, kind, payload])?;
        tx.commit()?;
        Ok(())
    }

    /// Records the started process on the launched attempt so that an operator release can check it still runs.
    /// The first record wins; a later different record for the same attempt is refused.
    pub(crate) fn service_record_process(
        &mut self,
        job: &ClaimedJob,
        record: &crate::core::ProcessRecord,
    ) -> Result<(), StorageError> {
        if record.pid <= 1 || record.start.is_empty() || record.start.len() > 128 {
            return Err(StorageError::InvalidInput("process identity"));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (_, _, spawn) = job_status(&tx, job)?;
        if !spawn {
            return Err(StorageError::InvalidState);
        }
        let updated = tx.execute(
            "UPDATE attempts SET process_pid = ?5, process_start = ?6 WHERE owner_id = ?1 AND client_id = ?2 AND run_id = ?3 AND attempt_id = ?4 AND spawn_intent = 1 AND process_pid IS NULL",
            params![job.namespace.owner_id, job.namespace.client_id, job.run_id, job.attempt_id, record.pid, record.start],
        )?;
        if updated != 1 {
            return Err(StorageError::InvalidState);
        }
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn service_finish(
        &mut self,
        job: &ClaimedJob,
        candidate: &Candidate,
        now: impl Into<ServiceClock>,
        max_result_bytes: usize,
    ) -> Result<(), StorageError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now_ms = now.into().read()?;
        let (mut stop, deadline, _) = job_status(&tx, job)?;
        if stop.is_none() && now_ms >= deadline {
            set_stop(&tx, &job.namespace, &job.run_id, StopCause::TimedOut)?;
            stop = Some(StopCause::TimedOut);
        }
        finish_tx(
            &tx,
            &job.namespace,
            &job.run_id,
            &job.attempt_id,
            candidate,
            FinishContext {
                stop,
                max_result_bytes,
                now_ms,
            },
        )?;
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn service_run_summary(
        &self,
        namespace: &Namespace,
        run_id: &str,
        max_bytes: usize,
    ) -> Result<Option<super::RunSummary>, StorageError> {
        let limit =
            i64::try_from(max_bytes).map_err(|_| StorageError::InvalidInput("event limit"))?;
        let row: Option<LimitedRunRow> = self.connection.query_row(
            "SELECT CASE WHEN allowed THEN run_id END, CASE WHEN allowed THEN state END, CASE WHEN allowed THEN runtime END, synthetic, CASE WHEN allowed THEN scenario END, allowed FROM (SELECT run_id,state,runtime,synthetic,scenario,(length(CAST(run_id AS BLOB))+length(CAST(state AS BLOB))+length(CAST(runtime AS BLOB))+COALESCE(length(CAST(scenario AS BLOB)),0)) <= ?4 AS allowed FROM runs WHERE owner_id=?1 AND client_id=?2 AND run_id=?3)",
            params![namespace.owner_id,namespace.client_id,run_id,limit],
            |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?)),
        ).optional()?;
        row.map(|(run_id, state, runtime, synthetic, scenario, allowed)| {
            if !allowed {
                return Err(StorageError::InvalidInput("event limit"));
            }
            Ok(super::RunSummary {
                run_id: run_id.ok_or(StorageError::InvalidState)?,
                state: state.ok_or(StorageError::InvalidState)?,
                metadata: ExecutionMetadata {
                    runtime: runtime.ok_or(StorageError::InvalidState)?,
                    synthetic,
                    scenario,
                },
            })
        })
        .transpose()
    }

    pub(crate) fn service_result(
        &self,
        namespace: &Namespace,
        run_id: &str,
        max_bytes: usize,
        max_metadata_bytes: usize,
    ) -> Result<Option<StoredResult>, StorageError> {
        let tx = Transaction::new_unchecked(&self.connection, TransactionBehavior::Immediate)?;
        if super::retention::expired(&tx, namespace, run_id, ServiceClock::Wall)? {
            tx.commit()?;
            return Err(StorageError::Expired);
        }
        let result =
            self.service_result_unchecked(namespace, run_id, max_bytes, max_metadata_bytes)?;
        tx.commit()?;
        Ok(result)
    }

    fn service_result_unchecked(
        &self,
        namespace: &Namespace,
        run_id: &str,
        max_bytes: usize,
        max_metadata_bytes: usize,
    ) -> Result<Option<StoredResult>, StorageError> {
        let limit =
            i64::try_from(max_bytes).map_err(|_| StorageError::InvalidInput("result limit"))?;
        let metadata_limit = i64::try_from(max_metadata_bytes)
            .map_err(|_| StorageError::InvalidInput("event limit"))?;
        let row: Option<LimitedResultRow> = self.connection.query_row(
            "SELECT CASE WHEN allowed THEN revision END, CASE WHEN length(payload)<=?4 THEN payload END, CASE WHEN allowed THEN runtime END, synthetic, CASE WHEN allowed THEN scenario END, allowed FROM (SELECT x.revision,x.payload,r.runtime,r.synthetic,r.scenario,(length(CAST(x.revision AS BLOB))+length(CAST(r.runtime AS BLOB))+COALESCE(length(CAST(r.scenario AS BLOB)),0)) <= ?5 AS allowed FROM results x JOIN runs r USING (owner_id,client_id,run_id) WHERE x.owner_id=?1 AND x.client_id=?2 AND x.run_id=?3)",
            params![namespace.owner_id,namespace.client_id,run_id,limit,metadata_limit],
            |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?)),
        ).optional()?;
        row.map(
            |(revision, payload, runtime, synthetic, scenario, allowed)| {
                if !allowed {
                    return Err(StorageError::InvalidInput("event limit"));
                }
                Ok(StoredResult {
                    revision: revision.ok_or(StorageError::InvalidState)?,
                    payload: payload.ok_or(StorageError::InvalidInput("result limit"))?,
                    metadata: ExecutionMetadata {
                        runtime: runtime.ok_or(StorageError::InvalidState)?,
                        synthetic,
                        scenario,
                    },
                })
            },
        )
        .transpose()
    }

    pub(crate) fn service_result_view(
        &self,
        namespace: &Namespace,
        run_id: &str,
        max_bytes: usize,
        max_metadata_bytes: usize,
    ) -> Result<super::ResultView, StorageError> {
        let tx = Transaction::new_unchecked(&self.connection, TransactionBehavior::Immediate)?;
        let view = match self.service_run_summary(namespace, run_id, max_metadata_bytes)? {
            None => super::ResultView::NotFound,
            Some(run) if super::retention::expired(&tx, namespace, run_id, ServiceClock::Wall)? => {
                super::ResultView::Expired(run)
            }
            Some(run) => {
                match self.service_result_unchecked(
                    namespace,
                    run_id,
                    max_bytes,
                    max_metadata_bytes,
                )? {
                    Some(result) if is_terminal(&run.state) => {
                        super::ResultView::Available { run, result }
                    }
                    None if !is_terminal(&run.state) => super::ResultView::Pending(run),
                    _ => return Err(StorageError::InvalidState),
                }
            }
        };
        tx.commit()?;
        Ok(view)
    }

    pub(crate) fn service_event_view(
        &self,
        namespace: &Namespace,
        run_id: &str,
        after: Option<i64>,
        limit: usize,
        max_bytes: usize,
    ) -> Result<Option<Vec<super::EventSummary>>, StorageError> {
        let tx = Transaction::new_unchecked(&self.connection, TransactionBehavior::Immediate)?;
        if super::retention::expired(&tx, namespace, run_id, ServiceClock::Wall)? {
            tx.commit()?;
            return Err(StorageError::Expired);
        }
        let page = if self
            .service_run_summary(namespace, run_id, max_bytes)?
            .is_some()
        {
            Some(self.service_events_page_unchecked(namespace, run_id, after, limit, max_bytes)?)
        } else {
            None
        };
        tx.commit()?;
        Ok(page)
    }

    pub(crate) fn service_events_page(
        &self,
        namespace: &Namespace,
        run_id: &str,
        after: Option<i64>,
        limit: usize,
        max_bytes: usize,
    ) -> Result<Vec<EventSummary>, StorageError> {
        let tx = Transaction::new_unchecked(&self.connection, TransactionBehavior::Immediate)?;
        if super::retention::expired(&tx, namespace, run_id, ServiceClock::Wall)? {
            tx.commit()?;
            return Err(StorageError::Expired);
        }
        let page =
            self.service_events_page_unchecked(namespace, run_id, after, limit, max_bytes)?;
        tx.commit()?;
        Ok(page)
    }

    fn service_events_page_unchecked(
        &self,
        namespace: &Namespace,
        run_id: &str,
        after: Option<i64>,
        limit: usize,
        max_bytes: usize,
    ) -> Result<Vec<EventSummary>, StorageError> {
        let mut statement = self.connection.prepare(
            "WITH page AS (SELECT e.event_id, e.sequence, e.kind, r.runtime, r.synthetic, r.scenario, (length(CAST(e.event_id AS BLOB)) + length(CAST(e.kind AS BLOB)) + length(CAST(r.runtime AS BLOB)) + COALESCE(length(CAST(r.scenario AS BLOB)), 0)) <= ?6 AS allowed FROM events e JOIN runs r USING (owner_id, client_id, run_id) WHERE e.owner_id = ?1 AND e.client_id = ?2 AND e.run_id = ?3 AND e.sequence > ?4 ORDER BY e.sequence LIMIT ?5) SELECT CASE WHEN allowed THEN event_id END, sequence, CASE WHEN allowed THEN kind END, CASE WHEN allowed THEN runtime END, synthetic, CASE WHEN allowed THEN scenario END, allowed FROM page ORDER BY sequence",
        )?;
        let mut rows = statement.query(params![
            namespace.owner_id,
            namespace.client_id,
            run_id,
            after.unwrap_or(-1),
            i64::try_from(limit).map_err(|_| StorageError::InvalidInput("page limit"))?,
            i64::try_from(max_bytes).map_err(|_| StorageError::InvalidInput("event limit"))?,
        ])?;
        let mut result = Vec::new();
        while let Some(row) = rows.next()? {
            if !row.get::<_, bool>(6)? {
                return Err(StorageError::InvalidInput("event limit"));
            }
            result.push(EventSummary {
                event_id: row.get(0)?,
                sequence: row.get(1)?,
                kind: row.get(2)?,
                metadata: ExecutionMetadata {
                    runtime: row.get(3)?,
                    synthetic: row.get(4)?,
                    scenario: row.get(5)?,
                },
            });
        }
        Ok(result)
    }

    pub(crate) fn workspace_quarantine_journal(
        &mut self,
        key: &crate::workspace::journal::JournalKey,
    ) -> Result<(), StorageError> {
        if !matches!(
            self.workspace_journal_status(key)?,
            super::workspaces::JournalStatus::NeedsQuarantine
        ) {
            return Err(StorageError::InvalidState);
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let namespace = Namespace {
            owner_id: key.owner_id.clone(),
            client_id: key.client_id.clone(),
        };
        let attempt_id: String = tx.query_row(
            "SELECT a.attempt_id FROM service_jobs j JOIN runs r USING(owner_id,client_id,run_id) JOIN attempts a ON a.owner_id=j.owner_id AND a.client_id=j.client_id AND a.run_id=j.run_id AND a.attempt_id=j.attempt_id WHERE j.owner_id=?1 AND j.client_id=?2 AND j.run_id=?3 AND j.state_realm=?4 AND a.spawn_intent=0 AND r.state IN ('queued','preparing')",
            params![key.owner_id,key.client_id,key.run_id,key.state_realm], |row| row.get(0),
        ).optional()?.ok_or(StorageError::InvalidState)?;
        set_stop(&tx, &namespace, &key.run_id, StopCause::Interrupted)?;
        let cause = load_stop(&tx, &namespace, &key.run_id)?.ok_or(StorageError::InvalidState)?;
        finish_tx(
            &tx,
            &namespace,
            &key.run_id,
            &attempt_id,
            &Candidate {
                state: TerminalState::Interrupted,
                details: json!({"error":"workspace_recovery_uncertain"}),
                cleanup: CleanupState::Uncertain,
                disposition: SessionDisposition::Uncertain,
            },
            FinishContext {
                stop: Some(cause),
                max_result_bytes: 4096,
                now_ms: wall_clock_ms()?,
            },
        )?;
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn service_recover(
        &mut self,
        now: impl Into<ServiceClock>,
    ) -> Result<(), StorageError> {
        let clock = now.into();
        let mut cursor = 0_i64;
        loop {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)?;
            if cursor == 0 {
                let unmanaged: bool = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM runs r LEFT JOIN service_jobs j USING (owner_id, client_id, run_id) WHERE j.queue_id IS NULL AND r.state IN ('queued', 'preparing', 'running', 'validating'))",
                    [], |row| row.get(0),
                )?;
                if unmanaged {
                    return Err(StorageError::InvalidState);
                }
            }
            let jobs = {
                let mut stmt = tx.prepare("SELECT j.queue_id, j.owner_id, j.client_id, j.run_id, j.attempt_id, j.stop_cause, j.deadline_ms, a.spawn_intent FROM service_jobs j JOIN runs r USING (owner_id, client_id, run_id) JOIN attempts a ON a.owner_id = j.owner_id AND a.client_id = j.client_id AND a.run_id = j.run_id AND a.attempt_id = j.attempt_id WHERE j.queue_id > ?1 AND r.state IN ('queued', 'preparing', 'running', 'validating') ORDER BY j.queue_id LIMIT 64")?;
                let rows = stmt.query_map([cursor], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        Namespace {
                            owner_id: row.get(1)?,
                            client_id: row.get(2)?,
                        },
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, i64>(6)?,
                        row.get::<_, bool>(7)?,
                    ))
                })?;
                rows.collect::<Result<Vec<_>, _>>()?
            };
            if jobs.is_empty() {
                tx.commit()?;
                return Ok(());
            }
            for (queue_id, namespace, run_id, attempt_id, stop, deadline, spawn) in jobs {
                cursor = queue_id;
                let now_ms = clock.read()?;
                let stop = stop.as_deref().map(StopCause::from_str).transpose()?;
                let cause = stop.or_else(|| (now_ms >= deadline).then_some(StopCause::TimedOut));
                if spawn || cause.is_some() {
                    let cause = cause.unwrap_or(StopCause::Interrupted);
                    set_stop(&tx, &namespace, &run_id, cause)?;
                    let mut candidate = Candidate::stopped();
                    if spawn {
                        candidate.cleanup = CleanupState::Uncertain;
                        candidate.disposition = SessionDisposition::Uncertain;
                    }
                    finish_tx(
                        &tx,
                        &namespace,
                        &run_id,
                        &attempt_id,
                        &candidate,
                        FinishContext {
                            stop: Some(cause),
                            max_result_bytes: 4096,
                            now_ms,
                        },
                    )?;
                } else {
                    tx.execute("UPDATE run_workspaces SET held=0, uncertain=0 WHERE owner_id=?1 AND client_id=?2 AND run_id=?3", params![namespace.owner_id, namespace.client_id, run_id])?;
                    tx.execute("UPDATE service_jobs SET claim_token = NULL WHERE owner_id = ?1 AND client_id = ?2 AND run_id = ?3", params![namespace.owner_id, namespace.client_id, run_id])?;
                    tx.execute("UPDATE runs SET state = 'queued' WHERE owner_id = ?1 AND client_id = ?2 AND run_id = ?3", params![namespace.owner_id, namespace.client_id, run_id])?;
                    tx.execute("UPDATE attempts SET state = 'queued' WHERE owner_id = ?1 AND client_id = ?2 AND run_id = ?3 AND attempt_id = ?4", params![namespace.owner_id, namespace.client_id, run_id, attempt_id])?;
                }
            }
            tx.commit()?;
        }
    }
}

pub(crate) fn wall_clock_ms() -> Result<i64, StorageError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| StorageError::InvalidState)?;
    i64::try_from(now.as_millis()).map_err(|_| StorageError::InvalidState)
}

fn new_id(connection: &Connection, prefix: &str) -> Result<String, StorageError> {
    let suffix: String =
        connection.query_row("SELECT lower(hex(randomblob(16)))", [], |row| row.get(0))?;
    Ok(format!("{prefix}_{suffix}"))
}

fn job_status(
    connection: &Connection,
    job: &ClaimedJob,
) -> Result<(Option<StopCause>, i64, bool), StorageError> {
    let (cause, deadline, spawn): (Option<String>, i64, bool) = connection.query_row(
        "SELECT j.stop_cause, j.deadline_ms, a.spawn_intent FROM service_jobs j JOIN runs r USING (owner_id, client_id, run_id) JOIN attempts a ON a.owner_id = j.owner_id AND a.client_id = j.client_id AND a.run_id = j.run_id AND a.attempt_id = j.attempt_id WHERE j.owner_id = ?1 AND j.client_id = ?2 AND j.run_id = ?3 AND j.attempt_id = ?4 AND j.claim_token = ?5 AND r.state IN ('preparing', 'running', 'validating')",
        params![job.namespace.owner_id, job.namespace.client_id, job.run_id, job.attempt_id, job.claim_token],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    ).optional()?.ok_or(StorageError::InvalidState)?;
    if deadline != job.deadline_ms {
        return Err(StorageError::InvalidState);
    }
    Ok((
        cause.as_deref().map(StopCause::from_str).transpose()?,
        deadline,
        spawn,
    ))
}

fn set_stop(
    tx: &Transaction<'_>,
    namespace: &Namespace,
    run_id: &str,
    cause: StopCause,
) -> Result<(), StorageError> {
    tx.execute("UPDATE service_jobs SET stop_cause = COALESCE(stop_cause, ?1) WHERE owner_id = ?2 AND client_id = ?3 AND run_id = ?4", params![cause.as_str(), namespace.owner_id, namespace.client_id, run_id])?;
    Ok(())
}

fn load_stop(
    tx: &Transaction<'_>,
    namespace: &Namespace,
    run_id: &str,
) -> Result<Option<StopCause>, StorageError> {
    let value: Option<String> = tx.query_row("SELECT stop_cause FROM service_jobs WHERE owner_id = ?1 AND client_id = ?2 AND run_id = ?3", params![namespace.owner_id, namespace.client_id, run_id], |row| row.get(0))?;
    value.as_deref().map(StopCause::from_str).transpose()
}

fn expire_waiting(tx: &Transaction<'_>, now_ms: i64) -> Result<(), StorageError> {
    let jobs = {
        let mut statement = tx.prepare("SELECT j.owner_id, j.client_id, j.run_id, j.attempt_id FROM service_jobs j JOIN runs r USING (owner_id, client_id, run_id) WHERE r.state = 'queued' AND j.claim_token IS NULL AND j.deadline_ms <= ?1 ORDER BY j.queue_id LIMIT 64")?;
        let rows = statement.query_map([now_ms], |row| {
            Ok((
                Namespace {
                    owner_id: row.get(0)?,
                    client_id: row.get(1)?,
                },
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;
        rows.collect::<Result<Vec<_>, _>>()?
    };
    for (namespace, run_id, attempt_id) in jobs {
        set_stop(tx, &namespace, &run_id, StopCause::TimedOut)?;
        let stop = load_stop(tx, &namespace, &run_id)?.ok_or(StorageError::InvalidState)?;
        finish_tx(
            tx,
            &namespace,
            &run_id,
            &attempt_id,
            &Candidate::stopped(),
            FinishContext {
                stop: Some(stop),
                max_result_bytes: 4096,
                now_ms,
            },
        )?;
    }
    Ok(())
}

fn finish_tx(
    tx: &Transaction<'_>,
    namespace: &Namespace,
    run_id: &str,
    attempt_id: &str,
    candidate: &Candidate,
    context: FinishContext,
) -> Result<(), StorageError> {
    let FinishContext {
        stop,
        max_result_bytes,
        now_ms,
    } = context;
    let (state, session_id, spawn): (String, String, bool) = tx.query_row(
        "SELECT r.state, r.session_id, a.spawn_intent FROM runs r JOIN attempts a USING (owner_id, client_id, run_id) WHERE r.owner_id = ?1 AND r.client_id = ?2 AND r.run_id = ?3 AND a.attempt_id = ?4",
        params![namespace.owner_id, namespace.client_id, run_id, attempt_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    ).optional()?.ok_or(StorageError::NotFound)?;
    if is_terminal(&state) {
        return Err(StorageError::AlreadyTerminal);
    }
    let mut terminal = stop.map_or(candidate.state, StopCause::terminal);
    let warnings = candidate.details.get("warnings").and_then(Value::as_array);
    let observation = candidate
        .details
        .get("warning_observation")
        .and_then(Value::as_str)
        .filter(|value| ["complete", "partial", "unavailable"].contains(value));
    let stopped_details = match (warnings, observation) {
        (Some(warnings), Some(observation)) => {
            json!({"warnings":warnings,"warning_observation":observation})
        }
        _ => Value::Null,
    };
    let mut payload = json!({"state": terminal.as_str(), "details": if stop.is_some() { stopped_details } else { candidate.details.clone() }}).to_string().into_bytes();
    if payload.len() > max_result_bytes {
        let omitted = warnings.is_none_or(|warnings| !warnings.is_empty());
        if stop.is_some() {
            payload = json!({"state":terminal.as_str(),"details":{"warning_observation":observation.unwrap_or("unavailable"),"warnings_omitted":omitted}}).to_string().into_bytes();
            if payload.len() > max_result_bytes {
                return Err(StorageError::InvalidInput("result limit"));
            }
        } else {
            terminal = TerminalState::Failed;
            let reason = candidate
                .details
                .get("validation")
                .filter(|validation| {
                    validation.get("status").and_then(Value::as_str) == Some("failed")
                })
                .and_then(|validation| validation.get("reason"))
                .and_then(Value::as_str)
                .filter(|reason| {
                    [
                        "invalid_or_unsupported_json",
                        "schema_mismatch",
                        "validation_limit_exceeded",
                    ]
                    .contains(reason)
                });
            payload = reason.map(|reason| json!({"state":"failed","details":{"validation":{"status":"failed","reason":reason},"warning_observation":observation.unwrap_or("unavailable"),"warnings_omitted":omitted,"output_omitted":true}}).to_string().into_bytes())
                .filter(|payload| payload.len() <= max_result_bytes)
                .unwrap_or_else(|| br#"{"state":"failed","error":"result_limit_exceeded"}"#.to_vec());
        }
    }
    let cleanup = match candidate.cleanup {
        CleanupState::Settled => "settled",
        CleanupState::Uncertain => "uncertain",
    };
    let disposition = match candidate.disposition {
        SessionDisposition::NotApplicable => "not_applicable",
        SessionDisposition::Reusable => "reusable",
        SessionDisposition::Uncertain => "uncertain",
    };
    if tx.execute(
        "UPDATE runs SET state = ?1 WHERE owner_id = ?2 AND client_id = ?3 AND run_id = ?4",
        params![
            terminal.as_str(),
            namespace.owner_id,
            namespace.client_id,
            run_id
        ],
    )? != 1
    {
        return Err(StorageError::InvalidState);
    }
    if tx.execute("UPDATE attempts SET state = ?1, cleanup = ?2, disposition = ?3 WHERE owner_id = ?4 AND client_id = ?5 AND run_id = ?6 AND attempt_id = ?7", params![terminal.as_str(), cleanup, disposition, namespace.owner_id, namespace.client_id, run_id, attempt_id])? != 1 {
        return Err(StorageError::InvalidState);
    }
    if tx.execute("INSERT INTO results (owner_id, client_id, run_id, revision, payload) VALUES (?1, ?2, ?3, '1', ?4)", params![namespace.owner_id, namespace.client_id, run_id, payload])? != 1 {
        return Err(StorageError::InvalidState);
    }
    let sequence: i64 = tx.query_row("SELECT COALESCE(MAX(sequence), -1) + 1 FROM events WHERE owner_id = ?1 AND client_id = ?2 AND run_id = ?3", params![namespace.owner_id, namespace.client_id, run_id], |row| row.get(0))?;
    let terminal_payload = json!({"state": terminal.as_str(), "cleanup": cleanup}).to_string();
    if tx.execute("INSERT INTO events (owner_id, client_id, run_id, event_id, sequence, kind, payload) VALUES (?1, ?2, ?3, ?4, ?5, 'run.finished', ?6)", params![namespace.owner_id, namespace.client_id, run_id, format!("{run_id}:finished"), sequence, terminal_payload.as_bytes()])? != 1 {
        return Err(StorageError::InvalidState);
    }
    if !spawn
        && candidate.disposition == SessionDisposition::NotApplicable
        && candidate.cleanup == CleanupState::Settled
    {
        if tx.execute("UPDATE sessions SET reserved_run_id = NULL, cleanup = CASE WHEN native_id IS NULL THEN 'settled' ELSE cleanup END, disposition = CASE WHEN native_id IS NULL THEN 'not_started' ELSE disposition END WHERE owner_id = ?1 AND client_id = ?2 AND session_id = ?3 AND reserved_run_id = ?4", params![namespace.owner_id, namespace.client_id, session_id, run_id])? != 1 {
            return Err(StorageError::InvalidState);
        }
    } else {
        let release = candidate.cleanup == CleanupState::Settled
            && candidate.disposition == SessionDisposition::Reusable;
        let updated = tx.execute("UPDATE sessions SET cleanup = ?1, disposition = CASE WHEN native_id IS NULL OR ?2 = 'not_applicable' THEN 'uncertain' ELSE ?2 END, reserved_run_id = CASE WHEN ?3 AND native_id IS NOT NULL THEN NULL ELSE reserved_run_id END WHERE owner_id = ?4 AND client_id = ?5 AND session_id = ?6 AND reserved_run_id = ?7", params![cleanup, disposition, release, namespace.owner_id, namespace.client_id, session_id, run_id])?;
        if updated != 1 {
            return Err(StorageError::InvalidState);
        }
    }
    if !spawn && candidate.cleanup == CleanupState::Settled {
        tx.execute(
            "DELETE FROM session_workspaces WHERE owner_id=?1 AND client_id=?2 AND session_id=?3 AND directory IS NULL AND materialization_intent=0 AND NOT EXISTS(SELECT 1 FROM sessions WHERE owner_id=?1 AND client_id=?2 AND session_id=?3 AND native_id IS NOT NULL)",
            params![namespace.owner_id, namespace.client_id, session_id],
        )?;
    }
    // A workspace stays held only while a process from the run may still write to it. An unproven session
    // disposition keeps the session reserved but does not hold the workspace once cleanup is settled.
    let workspace_uncertain = candidate.cleanup == CleanupState::Uncertain;
    tx.execute(
        "UPDATE run_workspaces SET held=?1, uncertain=?1 WHERE owner_id=?2 AND client_id=?3 AND run_id=?4",
        params![workspace_uncertain, namespace.owner_id, namespace.client_id, run_id],
    )?;
    if !workspace_uncertain {
        tx.execute(
            "INSERT INTO workspace_settlements(owner_id,client_id,run_id) SELECT owner_id,client_id,run_id FROM run_workspaces WHERE owner_id=?1 AND client_id=?2 AND run_id=?3 ON CONFLICT(owner_id,client_id,run_id) DO NOTHING",
            params![namespace.owner_id, namespace.client_id, run_id],
        )?;
    }
    super::completion::enqueue_terminal(tx, namespace, run_id, terminal.as_str(), cleanup, now_ms)?;
    let retention: Option<i64> = tx
        .query_row(
            "SELECT retention_ms FROM run_retention WHERE owner_id=?1 AND client_id=?2 AND run_id=?3",
            params![namespace.owner_id, namespace.client_id, run_id],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(retention) = retention {
        let expires = now_ms
            .checked_add(retention)
            .ok_or(StorageError::InvalidState)?;
        if tx.execute(
            "UPDATE run_retention SET expires_ms=?1 WHERE owner_id=?2 AND client_id=?3 AND run_id=?4 AND expires_ms IS NULL",
            params![expires, namespace.owner_id, namespace.client_id, run_id],
        )? != 1 {
            return Err(StorageError::InvalidState);
        }
    }
    Ok(())
}

fn is_terminal(state: &str) -> bool {
    matches!(
        state,
        "succeeded" | "failed" | "cancelled" | "timed_out" | "interrupted"
    )
}
