use rusqlite::{OptionalExtension, TransactionBehavior, params};

use crate::delivery::Destination;

use super::{Namespace, SqliteStore, StorageError, service::ServiceClock};

type DeliveryCandidateRow = (
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    bool,
    Vec<u8>,
    Vec<u8>,
    i64,
);

#[derive(Clone)]
pub(crate) struct ClaimedDelivery {
    pub namespace: Namespace,
    pub id: String,
    pub owner: String,
    pub generation: i64,
    pub credential_id: String,
    pub credential_generation: String,
    pub destination: Destination,
    pub synthetic: bool,
    pub payload: Vec<u8>,
}

#[derive(Clone, Copy)]
pub(crate) enum Observation {
    Delivered,
    NotDispatched,
    Ambiguous,
}

impl Observation {
    fn as_str(self) -> &'static str {
        match self {
            Self::Delivered => "delivered",
            Self::NotDispatched => "not_dispatched",
            Self::Ambiguous => "ambiguous",
        }
    }
}

impl SqliteStore {
    pub(crate) fn completion_disable(
        &mut self,
        now: impl Into<ServiceClock>,
    ) -> Result<(), StorageError> {
        let pending: bool = self.connection.query_row("SELECT EXISTS(SELECT 1 FROM completion_inbox WHERE status='pending' AND payload_purged=0) OR EXISTS(SELECT 1 FROM completion_delivery_jobs j JOIN delivery_intents d USING(owner_id,client_id,delivery_id) WHERE d.state IN ('pending','retry_pending','leased'))", [], |row| row.get(0))?;
        if !pending {
            return Ok(());
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now = now.into().read()?;
        tx.execute("INSERT INTO completion_delivery_observations (owner_id,client_id,delivery_id,lease_generation,observed_ms,observation) SELECT j.owner_id,j.client_id,j.delivery_id,j.lease_generation,?1,'destination_unavailable' FROM completion_delivery_jobs j JOIN delivery_intents d USING(owner_id,client_id,delivery_id) WHERE d.state='leased' AND j.dispatch_started=1",[now])?;
        tx.execute("UPDATE delivery_intents SET state=CASE WHEN (state='leased' AND EXISTS(SELECT 1 FROM completion_delivery_jobs j WHERE j.owner_id=delivery_intents.owner_id AND j.client_id=delivery_intents.client_id AND j.delivery_id=delivery_intents.delivery_id AND j.dispatch_started=1)) OR EXISTS(SELECT 1 FROM completion_delivery_observations o WHERE o.owner_id=delivery_intents.owner_id AND o.client_id=delivery_intents.client_id AND o.delivery_id=delivery_intents.delivery_id AND o.observation IN ('ambiguous','lease_expired')) THEN 'uncertain' ELSE 'skipped' END WHERE state IN ('pending','retry_pending','leased') AND EXISTS(SELECT 1 FROM completion_delivery_jobs j WHERE j.owner_id=delivery_intents.owner_id AND j.client_id=delivery_intents.client_id AND j.delivery_id=delivery_intents.delivery_id)",[])?;
        tx.execute("UPDATE completion_delivery_jobs SET lease_owner=NULL,lease_until_ms=NULL,dispatch_started=0,settled_ms=?1,last_reason='destination_unavailable' WHERE settled_ms IS NULL AND EXISTS(SELECT 1 FROM delivery_intents d WHERE d.owner_id=completion_delivery_jobs.owner_id AND d.client_id=completion_delivery_jobs.client_id AND d.delivery_id=completion_delivery_jobs.delivery_id AND d.state IN ('skipped','uncertain'))",[now])?;
        tx.execute(
            "UPDATE completion_inbox SET status='skipped',reason='destination_unavailable' WHERE status='pending'",
            [],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn completion_maintenance(
        &mut self,
        now: impl Into<ServiceClock>,
    ) -> Result<(), StorageError> {
        let charged: bool = self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM completion_bindings WHERE charged=1)",
            [],
            |row| row.get(0),
        )?;
        if !charged {
            return Ok(());
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now = now.into().read()?;
        let inbox = {
            let mut stmt = tx.prepare("SELECT c.owner_id,c.client_id,c.inbox_id,c.run_id,c.binding_id FROM completion_inbox c JOIN completion_bindings b ON b.owner_id=c.owner_id AND b.client_id=c.client_id AND b.run_id=c.run_id AND b.binding_id=c.binding_id WHERE c.payload_purged=0 AND (c.expires_ms<=?1 OR (c.acknowledged_ms IS NOT NULL AND c.acknowledged_ms<=?1-b.retention_ms) OR (c.status='skipped' AND c.created_ms<=?1-b.retention_ms)) LIMIT 64")?;
            let rows = stmt.query_map([now], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, String>(4)?,
                ))
            })?;
            rows.collect::<Result<Vec<_>, _>>()?
        };
        for (owner, client, id, run, binding) in inbox {
            tx.execute("UPDATE inbox_items SET payload=X'' WHERE owner_id=?1 AND client_id=?2 AND inbox_id=?3",params![owner,client,id])?;
            tx.execute("UPDATE completion_inbox SET payload_purged=1,reason=CASE WHEN status='pending' THEN 'expired' ELSE reason END,status=CASE WHEN status='pending' THEN 'skipped' ELSE status END WHERE owner_id=?1 AND client_id=?2 AND inbox_id=?3",params![owner,client,id])?;
            tx.execute("UPDATE completion_bindings SET charged=0 WHERE owner_id=?1 AND client_id=?2 AND run_id=?3 AND binding_id=?4",params![owner,client,run,binding])?;
        }
        let deliveries = {
            let mut stmt = tx.prepare("SELECT d.owner_id,d.client_id,d.delivery_id,d.run_id,d.binding_id FROM delivery_intents d JOIN completion_delivery_jobs j USING(owner_id,client_id,delivery_id) JOIN completion_bindings b ON b.owner_id=d.owner_id AND b.client_id=d.client_id AND b.run_id=d.run_id AND b.binding_id=d.binding_id WHERE j.payload_purged=0 AND j.settled_ms IS NOT NULL AND j.settled_ms<=?1-b.retention_ms AND (d.state IN ('succeeded','failed','skipped') OR (d.state='uncertain' AND j.reconciled_ms IS NOT NULL AND j.reconciled_ms<=?1-b.retention_ms)) LIMIT 64")?;
            let rows = stmt.query_map([now], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, String>(4)?,
                ))
            })?;
            rows.collect::<Result<Vec<_>, _>>()?
        };
        for (owner, client, id, run, binding) in deliveries {
            tx.execute("UPDATE delivery_intents SET payload=X'' WHERE owner_id=?1 AND client_id=?2 AND delivery_id=?3",params![owner,client,id])?;
            tx.execute("UPDATE completion_delivery_jobs SET payload_purged=1 WHERE owner_id=?1 AND client_id=?2 AND delivery_id=?3",params![owner,client,id])?;
            tx.execute("UPDATE completion_bindings SET charged=0 WHERE owner_id=?1 AND client_id=?2 AND run_id=?3 AND binding_id=?4",params![owner,client,run,binding])?;
        }
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn completion_claim(
        &mut self,
        now: impl Into<ServiceClock>,
        lease_ms: i64,
    ) -> Result<Option<ClaimedDelivery>, StorageError> {
        let clock = now.into();
        let probe = clock.read()?;
        let ready: bool = self.connection.query_row("SELECT EXISTS(SELECT 1 FROM completion_delivery_jobs j JOIN delivery_intents d USING(owner_id,client_id,delivery_id) WHERE (d.state='leased' AND j.lease_until_ms<=?1) OR (d.state IN ('pending','retry_pending') AND (j.next_attempt_ms<=?1 OR j.expires_ms<=?1)))",[probe],|row|row.get(0))?;
        if !ready {
            return Ok(None);
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now = clock.read()?;
        let expired = {
            let mut stmt = tx.prepare("SELECT j.owner_id,j.client_id,j.delivery_id,j.lease_generation,j.dispatch_started,j.attempts,j.expires_ms,b.max_attempts,b.destination_snapshot,d.state FROM completion_delivery_jobs j JOIN delivery_intents d USING(owner_id,client_id,delivery_id) JOIN completion_bindings b ON b.owner_id=d.owner_id AND b.client_id=d.client_id AND b.run_id=d.run_id AND b.binding_id=d.binding_id WHERE (d.state='leased' AND j.lease_until_ms<=?1) OR (d.state IN ('pending','retry_pending') AND j.expires_ms<=?1) LIMIT 64")?;
            let rows = stmt.query_map([now], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, i64>(3)?,
                    r.get::<_, bool>(4)?,
                    r.get::<_, i64>(5)?,
                    r.get::<_, i64>(6)?,
                    r.get::<_, i64>(7)?,
                    r.get::<_, Vec<u8>>(8)?,
                    r.get::<_, String>(9)?,
                ))
            })?;
            rows.collect::<Result<Vec<_>, _>>()?
        };
        for (
            owner,
            client,
            id,
            generation,
            started,
            attempts,
            expires,
            max_attempts,
            snapshot,
            state,
        ) in expired
        {
            let destination: Destination =
                serde_json::from_slice(&snapshot).map_err(|_| StorageError::InvalidState)?;
            let previous_ambiguous: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM completion_delivery_observations WHERE owner_id=?1 AND client_id=?2 AND delivery_id=?3 AND observation IN ('ambiguous','lease_expired'))",params![owner,client,id],|r|r.get(0))?;
            let next = if state == "leased" && started {
                tx.execute("INSERT INTO completion_delivery_observations (owner_id,client_id,delivery_id,lease_generation,observed_ms,observation) VALUES (?1,?2,?3,?4,?5,'lease_expired')",params![owner,client,id,generation,now])?;
                if destination.deduplication && attempts < max_attempts && now < expires {
                    "retry_pending"
                } else {
                    "uncertain"
                }
            } else if now < expires {
                "retry_pending"
            } else if previous_ambiguous {
                "uncertain"
            } else {
                "failed"
            };
            tx.execute("UPDATE delivery_intents SET state=?1 WHERE owner_id=?2 AND client_id=?3 AND delivery_id=?4",params![next,owner,client,id])?;
            tx.execute("UPDATE completion_delivery_jobs SET lease_owner=NULL,lease_until_ms=NULL,dispatch_started=0,next_attempt_ms=?1,settled_ms=CASE WHEN ?2 IN ('uncertain','failed') THEN ?1 ELSE NULL END,last_reason='lease_or_ttl_expired' WHERE owner_id=?3 AND client_id=?4 AND delivery_id=?5",params![now,next,owner,client,id])?;
        }
        let candidate: Option<DeliveryCandidateRow> = tx.query_row(
            "SELECT d.owner_id,d.client_id,d.delivery_id,s.credential_id,s.credential_generation,r.run_id,b.destination_id,r.synthetic,b.destination_snapshot,d.payload,j.lease_generation FROM delivery_intents d JOIN completion_delivery_jobs j USING(owner_id,client_id,delivery_id) JOIN completion_bindings b ON b.owner_id=d.owner_id AND b.client_id=d.client_id AND b.run_id=d.run_id AND b.binding_id=d.binding_id JOIN service_jobs s USING(owner_id,client_id,run_id) JOIN runs r USING(owner_id,client_id,run_id) WHERE d.state IN ('pending','retry_pending') AND j.next_attempt_ms<=?1 AND j.expires_ms>?1 ORDER BY j.next_attempt_ms,d.delivery_id LIMIT 1",
            [now], |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?,r.get(8)?,r.get(9)?,r.get(10)?)),
        ).optional()?;
        let Some((
            owner_id,
            client_id,
            id,
            credential_id,
            credential_generation,
            _run_id,
            destination_id,
            synthetic,
            snapshot,
            payload,
            generation,
        )) = candidate
        else {
            tx.commit()?;
            return Ok(None);
        };
        let destination: Destination =
            serde_json::from_slice(&snapshot).map_err(|_| StorageError::InvalidState)?;
        if destination.id != destination_id {
            return Err(StorageError::InvalidState);
        }
        let owner: String = tx.query_row("SELECT lower(hex(randomblob(16)))", [], |r| r.get(0))?;
        let generation = generation
            .checked_add(1)
            .ok_or(StorageError::InvalidState)?;
        let until = now
            .checked_add(lease_ms)
            .ok_or(StorageError::InvalidState)?;
        tx.execute("UPDATE delivery_intents SET state='leased' WHERE owner_id=?1 AND client_id=?2 AND delivery_id=?3",params![owner_id,client_id,id])?;
        tx.execute("UPDATE completion_delivery_jobs SET lease_owner=?1,lease_generation=?2,lease_until_ms=?3,dispatch_started=0 WHERE owner_id=?4 AND client_id=?5 AND delivery_id=?6",params![owner,generation,until,owner_id,client_id,id])?;
        tx.commit()?;
        Ok(Some(ClaimedDelivery {
            namespace: Namespace {
                owner_id,
                client_id,
            },
            id,
            owner,
            generation,
            credential_id,
            credential_generation,
            destination,
            synthetic,
            payload,
        }))
    }

    pub(crate) fn completion_begin_dispatch(
        &mut self,
        job: &ClaimedDelivery,
        now: impl Into<ServiceClock>,
    ) -> Result<bool, StorageError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now = now.into().read()?;
        let enabled: bool = tx.query_row("SELECT h.enabled FROM delivery_intents d JOIN completion_bindings b ON b.owner_id=d.owner_id AND b.client_id=d.client_id AND b.run_id=d.run_id AND b.binding_id=d.binding_id JOIN completion_hook_versions h ON h.owner_id=b.owner_id AND h.client_id=b.client_id AND h.hook_id=b.hook_id AND h.version=b.hook_version WHERE d.owner_id=?1 AND d.client_id=?2 AND d.delivery_id=?3",params![job.namespace.owner_id,job.namespace.client_id,job.id],|r|r.get(0)).optional()?.ok_or(StorageError::InvalidState)?;
        if !enabled {
            tx.commit()?;
            return Ok(false);
        }
        let updated = tx.execute("UPDATE completion_delivery_jobs SET dispatch_started=1,attempts=attempts+1 WHERE owner_id=?1 AND client_id=?2 AND delivery_id=?3 AND lease_owner=?4 AND lease_generation=?5 AND dispatch_started=0 AND lease_until_ms>?6 AND expires_ms>?6 AND EXISTS(SELECT 1 FROM delivery_intents d WHERE d.owner_id=?1 AND d.client_id=?2 AND d.delivery_id=?3 AND d.state='leased')",params![job.namespace.owner_id,job.namespace.client_id,job.id,job.owner,job.generation,now])?;
        if updated == 1 {
            tx.execute("INSERT INTO completion_delivery_attempts (owner_id,client_id,delivery_id,lease_generation,started_ms) VALUES (?1,?2,?3,?4,?5)",params![job.namespace.owner_id,job.namespace.client_id,job.id,job.generation,now])?;
        }
        tx.commit()?;
        Ok(updated == 1)
    }

    pub(crate) fn completion_skip(
        &mut self,
        job: &ClaimedDelivery,
        now: impl Into<ServiceClock>,
        reason: &str,
    ) -> Result<(), StorageError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now = now.into().read()?;
        let updated = tx.execute("UPDATE completion_delivery_jobs SET lease_owner=NULL,lease_until_ms=NULL,settled_ms=?1,last_reason=?2 WHERE owner_id=?3 AND client_id=?4 AND delivery_id=?5 AND lease_owner=?6 AND lease_generation=?7 AND dispatch_started=0 AND lease_until_ms>?1 AND expires_ms>?1",params![now,reason,job.namespace.owner_id,job.namespace.client_id,job.id,job.owner,job.generation])?;
        if updated == 1 {
            tx.execute("UPDATE delivery_intents SET state=CASE WHEN EXISTS(SELECT 1 FROM completion_delivery_observations o WHERE o.owner_id=delivery_intents.owner_id AND o.client_id=delivery_intents.client_id AND o.delivery_id=delivery_intents.delivery_id AND o.observation IN ('ambiguous','lease_expired')) THEN 'uncertain' ELSE 'skipped' END WHERE owner_id=?1 AND client_id=?2 AND delivery_id=?3 AND state='leased'",params![job.namespace.owner_id,job.namespace.client_id,job.id])?;
        }
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn completion_observe(
        &mut self,
        job: &ClaimedDelivery,
        now: impl Into<ServiceClock>,
        retry_ms: i64,
        observation: Observation,
    ) -> Result<(), StorageError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now = now.into().read()?;
        tx.execute("INSERT INTO completion_delivery_observations (owner_id,client_id,delivery_id,lease_generation,observed_ms,observation) VALUES (?1,?2,?3,?4,?5,?6)",params![job.namespace.owner_id,job.namespace.client_id,job.id,job.generation,now,observation.as_str()])?;
        tx.execute("UPDATE completion_delivery_attempts SET observed_ms=?1, observation=?2 WHERE owner_id=?3 AND client_id=?4 AND delivery_id=?5 AND lease_generation=?6",params![now,observation.as_str(),job.namespace.owner_id,job.namespace.client_id,job.id,job.generation])?;
        let current: Option<(i64,i64,i64)> = tx.query_row("SELECT j.attempts,j.expires_ms,b.max_attempts FROM completion_delivery_jobs j JOIN delivery_intents d USING(owner_id,client_id,delivery_id) JOIN completion_bindings b ON b.owner_id=d.owner_id AND b.client_id=d.client_id AND b.run_id=d.run_id AND b.binding_id=d.binding_id WHERE j.owner_id=?1 AND j.client_id=?2 AND j.delivery_id=?3 AND j.lease_owner=?4 AND j.lease_generation=?5 AND j.dispatch_started=1 AND d.state='leased'",params![job.namespace.owner_id,job.namespace.client_id,job.id,job.owner,job.generation],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
        if let Some((attempts, expires, max_attempts)) = current {
            let retry_at = now.saturating_add(retry_ms);
            let can_retry = attempts < max_attempts && retry_at < expires;
            let previous_ambiguous: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM completion_delivery_observations WHERE owner_id=?1 AND client_id=?2 AND delivery_id=?3 AND observation IN ('ambiguous','lease_expired'))",params![job.namespace.owner_id,job.namespace.client_id,job.id],|r|r.get(0))?;
            let state = match observation {
                Observation::Delivered => "succeeded",
                Observation::NotDispatched if can_retry => "retry_pending",
                Observation::NotDispatched if previous_ambiguous => "uncertain",
                Observation::NotDispatched => "failed",
                Observation::Ambiguous if job.destination.deduplication && can_retry => {
                    "retry_pending"
                }
                Observation::Ambiguous => "uncertain",
            };
            tx.execute("UPDATE delivery_intents SET state=?1 WHERE owner_id=?2 AND client_id=?3 AND delivery_id=?4",params![state,job.namespace.owner_id,job.namespace.client_id,job.id])?;
            tx.execute("UPDATE completion_delivery_jobs SET lease_owner=NULL,lease_until_ms=NULL,dispatch_started=0,next_attempt_ms=?1,settled_ms=CASE WHEN ?2='retry_pending' THEN NULL ELSE ?3 END,last_reason=?4 WHERE owner_id=?5 AND client_id=?6 AND delivery_id=?7",params![retry_at,state,now,observation.as_str(),job.namespace.owner_id,job.namespace.client_id,job.id])?;
        }
        tx.commit()?;
        Ok(())
    }
}
