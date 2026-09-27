use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};

use super::{Namespace, SqliteStore, StorageError, service::ServiceClock};

/// Marks an elapsed retention period only when its completion references are released.
pub(super) fn expired(
    connection: &Connection,
    namespace: &Namespace,
    run_id: &str,
    now: impl Into<ServiceClock>,
) -> Result<bool, StorageError> {
    let now = now.into().read()?;
    let row: Option<(Option<i64>, bool, bool, bool)> = connection
        .query_row(
            "SELECT expires_ms,expired,purged,EXISTS(SELECT 1 FROM completion_bindings b WHERE b.owner_id=r.owner_id AND b.client_id=r.client_id AND b.run_id=r.run_id AND b.charged=1) FROM run_retention r WHERE owner_id=?1 AND client_id=?2 AND run_id=?3",
            params![namespace.owner_id, namespace.client_id, run_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    match row {
        None | Some((None, false, _, _)) => Ok(false),
        Some((_, true, _, true)) => Err(StorageError::InvalidState),
        Some((_, true, true, false)) => Ok(true),
        Some((_, true, false, false)) => {
            verify_terminal_data(connection, namespace, run_id)?;
            Ok(true)
        }
        Some((Some(_), false, _, true)) => Ok(false),
        Some((Some(expires), false, _, false)) if expires > now => Ok(false),
        Some((Some(_), false, _, false)) => {
            verify_terminal_data(connection, namespace, run_id)?;
            let changed = connection.execute(
                "UPDATE run_retention SET expired=1 WHERE owner_id=?1 AND client_id=?2 AND run_id=?3 AND expired=0 AND expires_ms<=?4 AND NOT EXISTS (SELECT 1 FROM completion_bindings b WHERE b.owner_id=run_retention.owner_id AND b.client_id=run_retention.client_id AND b.run_id=run_retention.run_id AND b.charged=1)",
                params![namespace.owner_id, namespace.client_id, run_id, now],
            )?;
            if changed != 1 {
                return Err(StorageError::InvalidState);
            }
            Ok(true)
        }
    }
}

fn verify_terminal_data(
    connection: &Connection,
    namespace: &Namespace,
    run_id: &str,
) -> Result<(), StorageError> {
    let intact: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM runs WHERE owner_id=?1 AND client_id=?2 AND run_id=?3 AND state IN ('succeeded','failed','cancelled','timed_out','interrupted')) AND EXISTS(SELECT 1 FROM results WHERE owner_id=?1 AND client_id=?2 AND run_id=?3) AND EXISTS(SELECT 1 FROM events WHERE owner_id=?1 AND client_id=?2 AND run_id=?3 AND kind='run.finished')",
        params![namespace.owner_id, namespace.client_id, run_id],
        |row| row.get(0),
    )?;
    if intact {
        Ok(())
    } else {
        Err(StorageError::InvalidState)
    }
}

impl SqliteStore {
    pub(crate) fn visible_result(
        &self,
        namespace: &Namespace,
        run_id: &str,
    ) -> Result<Option<super::StoredResult>, StorageError> {
        let tx = Transaction::new_unchecked(&self.connection, TransactionBehavior::Immediate)?;
        if expired(&tx, namespace, run_id, ServiceClock::Wall)? {
            tx.commit()?;
            return Err(StorageError::Expired);
        }
        let result = self.get_result(namespace, run_id)?;
        tx.commit()?;
        Ok(result)
    }

    pub(crate) fn visible_event_summaries(
        &self,
        namespace: &Namespace,
        run_id: &str,
        after: Option<i64>,
    ) -> Result<Vec<super::EventSummary>, StorageError> {
        let tx = Transaction::new_unchecked(&self.connection, TransactionBehavior::Immediate)?;
        if expired(&tx, namespace, run_id, ServiceClock::Wall)? {
            tx.commit()?;
            return Err(StorageError::Expired);
        }
        let events = self.get_event_summaries(namespace, run_id, after)?;
        tx.commit()?;
        Ok(events)
    }

    pub(crate) fn run_retention_maintenance(
        &mut self,
        clock: impl Into<ServiceClock>,
    ) -> Result<(), StorageError> {
        let clock = clock.into();
        let before_lock = clock.read()?;
        let due: bool = self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM run_retention r WHERE purged=0 AND (expired=1 OR (expires_ms<=?1 AND NOT EXISTS (SELECT 1 FROM completion_bindings b WHERE b.owner_id=r.owner_id AND b.client_id=r.client_id AND b.run_id=r.run_id AND b.charged=1))))",
            [before_lock],
            |row| row.get(0),
        )?;
        if !due {
            return Ok(());
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now = clock.read()?;
        let due = {
            let mut statement = transaction.prepare(
                "SELECT owner_id, client_id, run_id FROM run_retention r WHERE purged=0 AND (expired=1 OR (expires_ms<=?1 AND NOT EXISTS (SELECT 1 FROM completion_bindings b WHERE b.owner_id=r.owner_id AND b.client_id=r.client_id AND b.run_id=r.run_id AND b.charged=1))) ORDER BY expires_ms LIMIT 64",
            )?;
            let rows = statement.query_map([now], |row| {
                Ok((
                    Namespace {
                        owner_id: row.get(0)?,
                        client_id: row.get(1)?,
                    },
                    row.get::<_, String>(2)?,
                ))
            })?;
            rows.collect::<Result<Vec<_>, _>>()?
        };
        for (namespace, run_id) in due {
            if !expired(&transaction, &namespace, &run_id, now)? {
                continue;
            }
            if transaction.execute(
                "DELETE FROM results WHERE owner_id=?1 AND client_id=?2 AND run_id=?3",
                params![namespace.owner_id, namespace.client_id, run_id],
            )? != 1
            {
                return Err(StorageError::InvalidState);
            }
            if transaction.execute(
                "UPDATE events SET payload=X'' WHERE owner_id=?1 AND client_id=?2 AND run_id=?3",
                params![namespace.owner_id, namespace.client_id, run_id],
            )? == 0
            {
                return Err(StorageError::InvalidState);
            }
            let uncleared: bool = transaction.query_row(
                "SELECT EXISTS(SELECT 1 FROM events WHERE owner_id=?1 AND client_id=?2 AND run_id=?3 AND length(payload)>0)",
                params![namespace.owner_id, namespace.client_id, run_id],
                |row| row.get(0),
            )?;
            if uncleared {
                return Err(StorageError::InvalidState);
            }
            if transaction.execute(
                "UPDATE run_retention SET purged=1 WHERE owner_id=?1 AND client_id=?2 AND run_id=?3 AND expired=1",
                params![namespace.owner_id, namespace.client_id, run_id],
            )? != 1 {
                return Err(StorageError::InvalidState);
            }
        }
        transaction.commit()?;
        Ok(())
    }
}
