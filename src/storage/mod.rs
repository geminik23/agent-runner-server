//! SQLite persistence for execution snapshots and completion intents.

mod completion;
pub(crate) mod dispatch;
pub(crate) mod hooks;
pub(crate) mod inbox;
mod queries;
mod retention;
mod runs;
pub(crate) mod service;
pub(crate) mod skills;
mod sqlite;
mod types;
pub(crate) mod workspaces;

#[cfg(test)]
mod delivery_tests;
#[cfg(test)]
mod service_tests;

pub use sqlite::SqliteStore;
pub use types::*;
