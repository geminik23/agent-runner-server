//! Read-only checks of the retained database; the daemon stays its only writer.

use std::{path::Path, time::Duration};

use rusqlite::{Connection, OpenFlags, OptionalExtension};

/// Summary of the records that decide whether live work can safely start or stop.
#[derive(Debug)]
pub struct Health {
    pub runs_by_state: Vec<(String, i64)>,
    pub active_runs: i64,
    pub uncertain_sessions: i64,
    pub held_workspaces: i64,
}

impl Health {
    /// A terminal uncertain session blocks every new session of its CLI state realm until an operator settles it.
    pub fn is_safe(&self) -> bool {
        self.active_runs == 0 && self.uncertain_sessions == 0 && self.held_workspaces == 0
    }

    pub fn describe(&self) -> String {
        let runs: Vec<String> = self
            .runs_by_state
            .iter()
            .map(|(state, count)| format!("{state}={count}"))
            .collect();
        format!(
            "runs [{}], active runs {}, uncertain sessions {}, held workspaces {}",
            runs.join(", "),
            self.active_runs,
            self.uncertain_sessions,
            self.held_workspaces
        )
    }
}

/// Stored protection state of one session.
#[derive(Debug)]
pub struct SessionState {
    pub cleanup: String,
    pub disposition: String,
    pub reserved: bool,
}

fn open(database: &Path) -> Result<Connection, String> {
    let connection = Connection::open_with_flags(database, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|error| format!("open {} read-only: {error}", database.display()))?;
    connection
        .busy_timeout(Duration::from_secs(5))
        .map_err(|error| format!("busy timeout: {error}"))?;
    Ok(connection)
}

fn query_error(error: rusqlite::Error) -> String {
    format!("database query: {error}")
}

pub fn health(database: &Path) -> Result<Health, String> {
    let connection = open(database)?;
    let mut statement = connection
        .prepare("SELECT state, COUNT(*) FROM runs GROUP BY state ORDER BY state")
        .map_err(query_error)?;
    let runs_by_state = statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .map_err(query_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(query_error)?;
    let count = |sql: &str| -> Result<i64, String> {
        connection
            .query_row(sql, [], |row| row.get(0))
            .map_err(query_error)
    };
    Ok(Health {
        runs_by_state,
        active_runs: count(ACTIVE_RUNS)?,
        // An abandoned session keeps an uncertain disposition without a reservation, so only reserved ones still block work.
        uncertain_sessions: count(
            "SELECT COUNT(*) FROM sessions WHERE reserved_run_id IS NOT NULL AND (cleanup='uncertain' OR disposition='uncertain')",
        )?,
        held_workspaces: count("SELECT COUNT(*) FROM run_workspaces WHERE held=1 OR uncertain=1")?,
    })
}

const ACTIVE_RUNS: &str =
    "SELECT COUNT(*) FROM runs WHERE state IN ('queued','preparing','running','validating')";

pub fn active_runs(database: &Path) -> Result<i64, String> {
    open(database)?
        .query_row(ACTIVE_RUNS, [], |row| row.get(0))
        .map_err(query_error)
}

/// Returns the native CLI session ID recorded for a server session, which the public API does not expose.
pub fn native_id(database: &Path, session_id: &str) -> Result<Option<String>, String> {
    open(database)?
        .query_row(
            "SELECT native_id FROM sessions WHERE session_id=?1",
            [session_id],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()
        .map(Option::flatten)
        .map_err(query_error)
}

pub fn session_state(database: &Path, session_id: &str) -> Result<Option<SessionState>, String> {
    open(database)?
        .query_row(
            "SELECT cleanup, disposition, reserved_run_id IS NOT NULL FROM sessions WHERE session_id=?1",
            [session_id],
            |row| {
                Ok(SessionState {
                    cleanup: row.get(0)?,
                    disposition: row.get(1)?,
                    reserved: row.get(2)?,
                })
            },
        )
        .optional()
        .map_err(query_error)
}
