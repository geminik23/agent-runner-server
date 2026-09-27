use std::collections::HashSet;

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::Serialize;

use crate::delivery::{DeliveryConfig, Destination, DestinationKind};

use super::{HookRef, Namespace, SqliteStore, StorageError};

pub(crate) const MAX_HOOKS: usize = 32;
const STATES: [&str; 5] = [
    "succeeded",
    "failed",
    "cancelled",
    "timed_out",
    "interrupted",
];

#[derive(Serialize)]
pub(crate) struct HookRecord {
    pub hook_id: String,
    pub version: String,
    pub destination_id: String,
    pub destination_revision: String,
    #[serde(skip)]
    destination_snapshot: Vec<u8>,
    pub events: Vec<String>,
    pub projection: String,
    pub enabled: bool,
}

pub(crate) struct SelectedHook {
    reference: HookRef,
    destination: Destination,
    events: String,
}

pub(crate) fn validate_refs(refs: &[HookRef], identifier_limit: usize) -> Result<(), StorageError> {
    let mut ids = HashSet::new();
    if refs.len() > MAX_HOOKS
        || refs.iter().any(|r| {
            r.hook_id.is_empty()
                || r.hook_id.len() > identifier_limit
                || r.version.is_empty()
                || r.version.len() > identifier_limit
                || !ids.insert(&r.hook_id)
        })
    {
        return Err(StorageError::InvalidInput("hook reference"));
    }
    Ok(())
}

pub(crate) fn validate_events(events: &mut [String]) -> Result<(), StorageError> {
    events.sort();
    if events.is_empty()
        || events.len() > STATES.len()
        || events.iter().any(|event| !STATES.contains(&event.as_str()))
        || events.windows(2).any(|pair| pair[0] == pair[1])
    {
        return Err(StorageError::InvalidInput("hook events"));
    }
    Ok(())
}

fn record(
    conn: &Connection,
    ns: &Namespace,
    r: &HookRef,
    limit: usize,
) -> Result<Option<HookRecord>, StorageError> {
    let row: Option<(String, String, Vec<u8>, String, String, bool)> = conn.query_row(
        "SELECT destination_id, destination_revision, destination_snapshot, events, projection, enabled FROM completion_hook_versions WHERE owner_id = ?1 AND client_id = ?2 AND hook_id = ?3 AND version = ?4 AND length(CAST(destination_id AS BLOB)) <= ?5 AND length(CAST(destination_revision AS BLOB)) <= ?5 AND length(destination_snapshot) <= 8192 AND length(CAST(events AS BLOB)) <= 256",
        params![ns.owner_id, ns.client_id, r.hook_id, r.version, i64::try_from(limit).unwrap_or(i64::MAX)],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?)),
    ).optional()?;
    row.map(
        |(
            destination_id,
            destination_revision,
            destination_snapshot,
            events,
            projection,
            enabled,
        )| {
            let mut events: Vec<String> =
                serde_json::from_str(&events).map_err(|_| StorageError::InvalidState)?;
            validate_events(&mut events)?;
            if projection != "metadata" {
                return Err(StorageError::InvalidState);
            }
            Ok(HookRecord {
                hook_id: r.hook_id.clone(),
                version: r.version.clone(),
                destination_id,
                destination_revision,
                destination_snapshot,
                events,
                projection,
                enabled,
            })
        },
    )
    .transpose()
}

impl SqliteStore {
    pub(crate) fn register_hook(
        &mut self,
        ns: &Namespace,
        reference: &HookRef,
        dest: &Destination,
        events: &[String],
        limit: usize,
    ) -> Result<bool, StorageError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "INSERT INTO namespaces (owner_id, client_id) VALUES (?1, ?2) ON CONFLICT DO NOTHING",
            params![ns.owner_id, ns.client_id],
        )?;
        let snapshot = serde_json::to_vec(dest).map_err(|_| StorageError::InvalidState)?;
        if let Some(existing) = record(&tx, ns, reference, limit)? {
            if existing.destination_snapshot != snapshot
                || existing.destination_id != dest.id
                || existing.destination_revision != dest.revision
                || existing.events != events
            {
                return Err(StorageError::HookVersionConflict);
            }
            tx.commit()?;
            return Ok(false);
        }
        let events = serde_json::to_string(events).map_err(|_| StorageError::InvalidState)?;
        tx.execute("INSERT INTO completion_hook_versions (owner_id, client_id, hook_id, version, destination_id, destination_revision, destination_snapshot, events, projection) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'metadata')", params![ns.owner_id, ns.client_id, reference.hook_id, reference.version, dest.id, dest.revision, snapshot, events])?;
        tx.commit()?;
        Ok(true)
    }

    pub(crate) fn hook(
        &self,
        ns: &Namespace,
        reference: &HookRef,
        limit: usize,
    ) -> Result<Option<HookRecord>, StorageError> {
        record(&self.connection, ns, reference, limit)
    }

    pub(crate) fn list_hooks(
        &self,
        ns: &Namespace,
        after: Option<&HookRef>,
        limit: usize,
        identifier_limit: usize,
    ) -> Result<Vec<HookRecord>, StorageError> {
        let mut stmt = self.connection.prepare("SELECT hook_id, version FROM completion_hook_versions WHERE owner_id=?1 AND client_id=?2 AND (?3 IS NULL OR (hook_id,version)>(?3,?4)) ORDER BY hook_id,version LIMIT ?5")?;
        let rows = stmt.query_map(
            params![
                ns.owner_id,
                ns.client_id,
                after.map(|r| &r.hook_id),
                after.map(|r| &r.version),
                i64::try_from(limit).map_err(|_| StorageError::InvalidInput("page limit"))?
            ],
            |row| {
                Ok(HookRef {
                    hook_id: row.get(0)?,
                    version: row.get(1)?,
                })
            },
        )?;
        let mut records = Vec::new();
        for row in rows {
            let r = row?;
            validate_refs(std::slice::from_ref(&r), identifier_limit)?;
            records.push(
                record(&self.connection, ns, &r, identifier_limit)?
                    .ok_or(StorageError::InvalidState)?,
            );
        }
        Ok(records)
    }

    pub(crate) fn set_hook_enabled(
        &self,
        ns: &Namespace,
        r: &HookRef,
        enabled: bool,
    ) -> Result<(), StorageError> {
        if self.connection.execute("UPDATE completion_hook_versions SET enabled=?1 WHERE owner_id=?2 AND client_id=?3 AND hook_id=?4 AND version=?5", params![enabled,ns.owner_id,ns.client_id,r.hook_id,r.version])? != 1 { return Err(StorageError::NotFound); }
        Ok(())
    }

    pub(crate) fn hook_defaults(
        &self,
        ns: &Namespace,
        limit: usize,
    ) -> Result<Vec<HookRef>, StorageError> {
        defaults(&self.connection, ns, limit)
    }

    pub(crate) fn set_hook_defaults(
        &mut self,
        ns: &Namespace,
        refs: &[HookRef],
        limit: usize,
    ) -> Result<(), StorageError> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "INSERT INTO namespaces (owner_id,client_id) VALUES (?1,?2) ON CONFLICT DO NOTHING",
            params![ns.owner_id, ns.client_id],
        )?;
        for r in refs {
            if !record(&tx, ns, r, limit)?
                .ok_or(StorageError::HookUnavailable)?
                .enabled
            {
                return Err(StorageError::HookUnavailable);
            }
        }
        tx.execute(
            "DELETE FROM completion_hook_defaults WHERE owner_id=?1 AND client_id=?2",
            params![ns.owner_id, ns.client_id],
        )?;
        for (position, r) in refs.iter().enumerate() {
            tx.execute(
                "INSERT INTO completion_hook_defaults VALUES (?1,?2,?3,?4,?5)",
                params![
                    ns.owner_id,
                    ns.client_id,
                    position as i64,
                    r.hook_id,
                    r.version
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }
}

fn defaults(
    conn: &Connection,
    ns: &Namespace,
    identifier_limit: usize,
) -> Result<Vec<HookRef>, StorageError> {
    let mut stmt = conn.prepare("SELECT hook_id, version FROM completion_hook_defaults WHERE owner_id=?1 AND client_id=?2 ORDER BY position LIMIT 33")?;
    let rows = stmt.query_map(params![ns.owner_id, ns.client_id], |row| {
        Ok(HookRef {
            hook_id: row.get(0)?,
            version: row.get(1)?,
        })
    })?;
    let refs = rows.collect::<Result<Vec<_>, _>>()?;
    validate_refs(&refs, identifier_limit)?;
    Ok(refs)
}

pub(crate) fn resolve(
    tx: &Transaction<'_>,
    admission: &super::service::ServiceAdmission,
) -> Result<Vec<SelectedHook>, StorageError> {
    let ns = &admission.namespace;
    let requested = admission.hooks.as_deref();
    let may_use = admission.may_use_hooks;
    let grants = &admission.destination_grants;
    let config = admission.delivery.as_ref();
    let identifier_limit = admission.skill_bounds.identifier;
    let metadata = &admission.metadata;
    let refs = match requested {
        Some(refs) => refs.to_vec(),
        None => defaults(tx, ns, identifier_limit)?,
    };
    validate_refs(&refs, identifier_limit)?;
    if refs.is_empty() {
        return Ok(Vec::new());
    }
    if !may_use {
        return Err(StorageError::HookUseForbidden);
    }
    let config = config.ok_or(StorageError::HookUnavailable)?;
    let mut selected = Vec::new();
    let mut inboxes = 0;
    for r in refs {
        let hook = record(tx, ns, &r, identifier_limit)?.ok_or(StorageError::HookUnavailable)?;
        let dest = config
            .destination(&hook.destination_id)
            .ok_or(StorageError::HookUnavailable)?;
        if !hook.enabled
            || dest.revision != hook.destination_revision
            || hook.destination_snapshot
                != serde_json::to_vec(dest).map_err(|_| StorageError::InvalidState)?
            || !grants.contains(&dest.id)
            || (!metadata.synthetic && dest.kind != DestinationKind::Inbox)
        {
            return Err(StorageError::HookUnavailable);
        }
        if dest.kind == DestinationKind::Inbox {
            inboxes += 1;
        }
        if dest.kind == DestinationKind::NotionMock
            && serde_json::to_vec(
                &serde_json::json!({"runtime":metadata.runtime,"scenario":metadata.scenario}),
            )
            .map_err(|_| StorageError::InvalidState)?
            .len()
                > 1200
        {
            return Err(StorageError::InvalidInput("notion metadata limit"));
        }
        selected.push(SelectedHook {
            reference: r,
            destination: dest.clone(),
            events: serde_json::to_string(&hook.events).map_err(|_| StorageError::InvalidState)?,
        });
    }
    if inboxes > 1 {
        return Err(StorageError::InvalidInput("duplicate inbox"));
    }
    let (count, bytes): (i64,i64) = tx.query_row("SELECT COUNT(*), COALESCE(SUM(max_payload_bytes),0) FROM completion_bindings WHERE charged=1",[],|row|Ok((row.get(0)?,row.get(1)?)))?;
    let max_count = i64::try_from(config.max_pending).map_err(|_| StorageError::InvalidState)?;
    let max_payload =
        i64::try_from(config.max_payload_bytes).map_err(|_| StorageError::InvalidState)?;
    let selected_count = i64::try_from(selected.len()).map_err(|_| StorageError::InvalidState)?;
    if count
        .checked_add(selected_count)
        .is_none_or(|n| n > max_count)
        || bytes
            .checked_add(
                selected_count
                    .checked_mul(max_payload)
                    .ok_or(StorageError::InvalidState)?,
            )
            .is_none_or(|n| n > max_count.saturating_mul(max_payload))
    {
        return Err(StorageError::DeliveryQuota);
    }
    Ok(selected)
}

pub(crate) fn pin(
    tx: &Transaction<'_>,
    ns: &Namespace,
    run_id: &str,
    selected: &[SelectedHook],
    config: &DeliveryConfig,
) -> Result<(), StorageError> {
    for (position, item) in selected.iter().enumerate() {
        let binding_id = format!("binding_{position}");
        let snapshot =
            serde_json::to_vec(&item.destination).map_err(|_| StorageError::InvalidState)?;
        tx.execute("INSERT INTO hook_bindings (owner_id,client_id,run_id,binding_id,hook_revision,destination_revision,projection) VALUES (?1,?2,?3,?4,?5,?6,?7)", params![ns.owner_id,ns.client_id,run_id,binding_id,item.reference.version,item.destination.revision,b"metadata".as_slice()])?;
        tx.execute("INSERT INTO completion_bindings (owner_id,client_id,run_id,binding_id,hook_id,hook_version,destination_id,destination_revision,destination_snapshot,connector,events,projection,max_payload_bytes,max_attempts,delivery_ttl_ms,retention_ms) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,'metadata',?12,?13,?14,?15)", params![ns.owner_id,ns.client_id,run_id,binding_id,item.reference.hook_id,item.reference.version,item.destination.id,item.destination.revision,snapshot,item.destination.kind.as_str(),item.events,i64::try_from(config.max_payload_bytes).map_err(|_|StorageError::InvalidState)?,config.max_attempts,i64::try_from(config.delivery_ttl_ms).map_err(|_|StorageError::InvalidState)?,i64::try_from(config.retention_ms).map_err(|_|StorageError::InvalidState)?])?;
    }
    Ok(())
}
