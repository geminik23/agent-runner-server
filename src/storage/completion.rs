use rusqlite::{Connection, Transaction, params};
use serde_json::{Value, json};

use super::{Namespace, StorageError};

struct Binding {
    binding_id: String,
    connector: String,
    destination_snapshot: Vec<u8>,
    events: String,
    max_payload_bytes: i64,
    delivery_ttl_ms: i64,
}

fn new_id(connection: &Connection, prefix: &str) -> Result<String, StorageError> {
    let suffix: String =
        connection.query_row("SELECT lower(hex(randomblob(16)))", [], |row| row.get(0))?;
    Ok(format!("{prefix}_{suffix}"))
}

pub(super) fn enqueue_terminal(
    tx: &Transaction<'_>,
    namespace: &Namespace,
    run_id: &str,
    terminal: &str,
    cleanup: &str,
    now_ms: i64,
) -> Result<(), StorageError> {
    let (runtime, synthetic, scenario): (String, bool, Option<String>) = tx.query_row(
        "SELECT runtime, synthetic, scenario FROM runs WHERE owner_id = ?1 AND client_id = ?2 AND run_id = ?3",
        params![namespace.owner_id, namespace.client_id, run_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    let bindings = {
        let mut stmt = tx.prepare(
            "SELECT binding_id, connector, destination_snapshot, events, max_payload_bytes, delivery_ttl_ms FROM completion_bindings WHERE owner_id = ?1 AND client_id = ?2 AND run_id = ?3 ORDER BY binding_id",
        )?;
        let rows = stmt.query_map(
            params![namespace.owner_id, namespace.client_id, run_id],
            |row| {
                Ok(Binding {
                    binding_id: row.get(0)?,
                    connector: row.get(1)?,
                    destination_snapshot: row.get(2)?,
                    events: row.get(3)?,
                    max_payload_bytes: row.get(4)?,
                    delivery_ttl_ms: row.get(5)?,
                })
            },
        )?;
        rows.collect::<Result<Vec<_>, _>>()?
    };
    let event_id = format!("{run_id}:finished");
    for binding in bindings {
        let events: Vec<String> =
            serde_json::from_str(&binding.events).map_err(|_| StorageError::InvalidState)?;
        if !events.iter().any(|event| event == terminal) {
            tx.execute(
                "UPDATE completion_bindings SET charged = 0 WHERE owner_id = ?1 AND client_id = ?2 AND run_id = ?3 AND binding_id = ?4",
                params![namespace.owner_id, namespace.client_id, run_id, binding.binding_id],
            )?;
            continue;
        }
        let id = new_id(
            tx,
            if binding.connector == "inbox" {
                "inbox"
            } else {
                "del"
            },
        )?;
        let notification = json!({
            "event_id": event_id,
            "delivery_id": id,
            "run_id": run_id,
            "state": terminal,
            "result_revision": "1",
            "runtime": runtime,
            "synthetic": synthetic,
            "scenario": scenario,
            "cleanup": cleanup,
        });
        let payload = match binding.connector.as_str() {
            "inbox" | "webhook_test" => serde_json::to_vec(&notification),
            "notion_mock" => {
                let destination: Value = serde_json::from_slice(&binding.destination_snapshot)
                    .map_err(|_| StorageError::InvalidState)?;
                let parent = destination.get("notion_parent").and_then(Value::as_str)
                    .ok_or(StorageError::InvalidState)?;
                let text = notification.to_string();
                serde_json::to_vec(&json!({
                    "parent": {"page_id": parent},
                    "properties": {"title": {"title": [{"text": {"content": format!("Run {run_id}: {terminal}")}}]}},
                    "children": [{"object":"block","type":"paragraph","paragraph":{"rich_text":[{"type":"text","text":{"content":text}}]}}]
                }))
            }
            _ => return Err(StorageError::InvalidState),
        }.map_err(|_| StorageError::InvalidState)?;
        if i64::try_from(payload.len()).map_or(true, |len| len > binding.max_payload_bytes) {
            return Err(StorageError::InvalidState);
        }
        let expires_ms = now_ms
            .checked_add(binding.delivery_ttl_ms)
            .ok_or(StorageError::InvalidState)?;
        if binding.connector == "inbox" {
            tx.execute(
                "INSERT INTO inbox_items (owner_id, client_id, inbox_id, run_id, event_id, payload) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![namespace.owner_id, namespace.client_id, id, run_id, event_id, payload],
            )?;
            tx.execute(
                "INSERT INTO completion_inbox (owner_id, client_id, inbox_id, run_id, binding_id, created_ms, expires_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![namespace.owner_id, namespace.client_id, id, run_id, binding.binding_id, now_ms, expires_ms],
            )?;
        } else {
            tx.execute(
                "INSERT INTO delivery_intents (owner_id, client_id, delivery_id, run_id, event_id, binding_id, payload, state) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'pending')",
                params![namespace.owner_id, namespace.client_id, id, run_id, event_id, binding.binding_id, payload],
            )?;
            tx.execute(
                "INSERT INTO completion_delivery_jobs (owner_id, client_id, delivery_id, next_attempt_ms, expires_ms) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![namespace.owner_id, namespace.client_id, id, now_ms, expires_ms],
            )?;
        }
    }
    Ok(())
}
