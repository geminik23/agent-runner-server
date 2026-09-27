use rusqlite::{OptionalExtension, TransactionBehavior, params};
use serde_json::{Value, json};

use super::{Namespace, SqliteStore, StorageError, service::ServiceClock};
use crate::delivery::Destination;

pub(crate) struct DeliveryReconciliation<'a> {
    pub id: &'a str,
    pub actor: &'a str,
    pub generation: &'a str,
    pub outcome: &'a str,
    pub grants: &'a std::collections::HashSet<String>,
}

pub(crate) struct PendingInbox {
    pub namespace: Namespace,
    pub id: String,
    pub credential_id: String,
    pub generation: String,
    pub destination: Destination,
}

impl SqliteStore {
    pub(crate) fn pending_inbox(&self) -> Result<Option<PendingInbox>, StorageError> {
        let row: Option<(String,String,String,String,String,Vec<u8>)> = self.connection.query_row(
            "SELECT c.owner_id,c.client_id,c.inbox_id,s.credential_id,s.credential_generation,b.destination_snapshot FROM completion_inbox c JOIN completion_bindings b ON b.owner_id=c.owner_id AND b.client_id=c.client_id AND b.run_id=c.run_id AND b.binding_id=c.binding_id JOIN service_jobs s USING(owner_id,client_id,run_id) WHERE c.status='pending' AND c.payload_purged=0 ORDER BY c.sequence LIMIT 1",
            [], |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?)),
        ).optional()?;
        row.map(
            |(owner_id, client_id, id, credential_id, generation, snapshot)| {
                Ok(PendingInbox {
                    namespace: Namespace {
                        owner_id,
                        client_id,
                    },
                    id,
                    credential_id,
                    generation,
                    destination: serde_json::from_slice(&snapshot)
                        .map_err(|_| StorageError::InvalidState)?,
                })
            },
        )
        .transpose()
    }

    pub(crate) fn settle_pending_inbox(
        &mut self,
        item: &PendingInbox,
        denial_reason: Option<&str>,
        now: impl Into<ServiceClock>,
    ) -> Result<(), StorageError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now = now.into().read()?;
        let enabled: bool = tx.query_row("SELECT h.enabled FROM completion_inbox c JOIN completion_bindings b ON b.owner_id=c.owner_id AND b.client_id=c.client_id AND b.run_id=c.run_id AND b.binding_id=c.binding_id JOIN completion_hook_versions h ON h.owner_id=b.owner_id AND h.client_id=b.client_id AND h.hook_id=b.hook_id AND h.version=b.hook_version WHERE c.owner_id=?1 AND c.client_id=?2 AND c.inbox_id=?3",params![item.namespace.owner_id,item.namespace.client_id,item.id],|row|row.get(0)).optional()?.ok_or(StorageError::InvalidState)?;
        let reason = denial_reason.or_else(|| (!enabled).then_some("hook_revoked"));
        let status = if reason.is_some() {
            "skipped"
        } else {
            "available"
        };
        tx.execute("UPDATE completion_inbox SET status=?1,reason=?2 WHERE owner_id=?3 AND client_id=?4 AND inbox_id=?5 AND status='pending' AND expires_ms>?6",params![status,reason,item.namespace.owner_id,item.namespace.client_id,item.id,now])?;
        tx.commit()?;
        Ok(())
    }
    pub(crate) fn completion_inbox_page(
        &self,
        ns: &Namespace,
        after: i64,
        limit: usize,
        now: i64,
        max_bytes: usize,
    ) -> Result<Vec<Value>, StorageError> {
        let mut stmt = self.connection.prepare("SELECT c.sequence, c.inbox_id, c.run_id, c.acknowledged_ms IS NOT NULL, CASE WHEN length(i.payload) <= ?6 THEN i.payload END FROM completion_inbox c JOIN inbox_items i USING(owner_id,client_id,inbox_id) WHERE c.owner_id=?1 AND c.client_id=?2 AND c.sequence>?3 AND c.expires_ms>?4 AND c.payload_purged=0 AND c.status='available' ORDER BY c.sequence LIMIT ?5")?;
        let rows = stmt.query_map(
            params![
                ns.owner_id,
                ns.client_id,
                after,
                now,
                i64::try_from(limit).map_err(|_| StorageError::InvalidInput("page limit"))?,
                i64::try_from(max_bytes)
                    .map_err(|_| StorageError::InvalidInput("response limit"))?
            ],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, bool>(3)?,
                    row.get::<_, Option<Vec<u8>>>(4)?,
                ))
            },
        )?;
        let mut items = Vec::new();
        let mut total = 0usize;
        for row in rows {
            let (sequence, id, run_id, acknowledged, payload) = row?;
            let payload = payload.ok_or(StorageError::InvalidInput("response limit"))?;
            total = total
                .checked_add(payload.len())
                .ok_or(StorageError::InvalidInput("response limit"))?;
            if total > max_bytes {
                return Err(StorageError::InvalidInput("response limit"));
            }
            let payload: Value =
                serde_json::from_slice(&payload).map_err(|_| StorageError::InvalidState)?;
            items.push(json!({"sequence":sequence,"inbox_id":id,"run_id":run_id,"acknowledged":acknowledged,"notification":payload}));
        }
        Ok(items)
    }

    pub(crate) fn acknowledge_completion_inbox(
        &self,
        ns: &Namespace,
        id: &str,
        now: i64,
    ) -> Result<(), StorageError> {
        if self.connection.execute("UPDATE completion_inbox SET acknowledged_ms=COALESCE(acknowledged_ms,?1) WHERE owner_id=?2 AND client_id=?3 AND inbox_id=?4 AND expires_ms>?1 AND payload_purged=0 AND status='available'",params![now,ns.owner_id,ns.client_id,id])? != 1 { return Err(StorageError::NotFound); }
        Ok(())
    }

    pub(crate) fn reconcile_delivery(
        &mut self,
        ns: &Namespace,
        input: &DeliveryReconciliation<'_>,
        clock: impl Into<ServiceClock>,
    ) -> Result<(), StorageError> {
        if !matches!(
            input.outcome,
            "confirmed_delivered" | "confirmed_not_delivered"
        ) {
            return Err(StorageError::InvalidInput("reconciliation outcome"));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now = clock.into().read()?;
        let row: Option<(String,Option<String>)> = tx.query_row("SELECT b.destination_id,j.reconcile_outcome FROM completion_delivery_jobs j JOIN delivery_intents d USING(owner_id,client_id,delivery_id) JOIN completion_bindings b ON b.owner_id=d.owner_id AND b.client_id=d.client_id AND b.run_id=d.run_id AND b.binding_id=d.binding_id WHERE j.owner_id=?1 AND j.client_id=?2 AND j.delivery_id=?3 AND d.state='uncertain'",params![ns.owner_id,ns.client_id,input.id],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
        let (destination, old_outcome) = row.ok_or(StorageError::NotFound)?;
        if !input.grants.contains(&destination) {
            return Err(StorageError::HookUseForbidden);
        }
        if old_outcome
            .as_deref()
            .is_some_and(|old| old != input.outcome)
        {
            return Err(StorageError::IdempotencyConflict);
        }
        tx.execute("UPDATE completion_delivery_jobs SET reconciled_ms=COALESCE(reconciled_ms,?1),reconciled_by=COALESCE(reconciled_by,?2),reconciled_generation=COALESCE(reconciled_generation,?3),reconcile_outcome=COALESCE(reconcile_outcome,?4) WHERE owner_id=?5 AND client_id=?6 AND delivery_id=?7",params![now,input.actor,input.generation,input.outcome,ns.owner_id,ns.client_id,input.id])?;
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn legacy_inbox_count(&self, ns: &Namespace) -> Result<i64, StorageError> {
        self.connection.query_row("SELECT COUNT(*) FROM inbox_items i LEFT JOIN completion_inbox c USING(owner_id,client_id,inbox_id) WHERE i.owner_id=?1 AND i.client_id=?2 AND c.sequence IS NULL",params![ns.owner_id,ns.client_id],|r|r.get(0)).map_err(Into::into)
    }

    pub(crate) fn legacy_delivery_count(
        &self,
        ns: &Namespace,
        run_id: &str,
    ) -> Result<i64, StorageError> {
        self.connection.query_row("SELECT COUNT(*) FROM delivery_intents d LEFT JOIN completion_delivery_jobs j USING(owner_id,client_id,delivery_id) WHERE d.owner_id=?1 AND d.client_id=?2 AND d.run_id=?3 AND j.delivery_id IS NULL",params![ns.owner_id,ns.client_id,run_id],|r|r.get(0)).map_err(Into::into)
    }

    pub(crate) fn completion_deliveries(
        &self,
        ns: &Namespace,
        run_id: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Option<Vec<Value>>, StorageError> {
        let exists: bool = self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM runs WHERE owner_id=?1 AND client_id=?2 AND run_id=?3)",
            params![ns.owner_id, ns.client_id, run_id],
            |row| row.get(0),
        )?;
        if !exists {
            return Ok(None);
        }
        let mut stmt = self.connection.prepare("SELECT d.delivery_id,d.event_id,d.state,j.attempts,j.last_reason,j.reconciled_ms IS NOT NULL FROM delivery_intents d JOIN completion_delivery_jobs j USING(owner_id,client_id,delivery_id) WHERE d.owner_id=?1 AND d.client_id=?2 AND d.run_id=?3 AND (?4 IS NULL OR d.delivery_id>?4) ORDER BY d.delivery_id LIMIT ?5")?;
        let rows = stmt.query_map(params![ns.owner_id,ns.client_id,run_id,after,i64::try_from(limit).map_err(|_|StorageError::InvalidInput("page limit"))?],|row|Ok(json!({"delivery_id":row.get::<_,String>(0)?,"event_id":row.get::<_,String>(1)?,"state":row.get::<_,String>(2)?,"attempts":row.get::<_,u32>(3)?,"reason":row.get::<_,Option<String>>(4)?,"reconciled":row.get::<_,bool>(5)?})))?;
        Ok(Some(rows.collect::<Result<Vec<_>, _>>()?))
    }
}
