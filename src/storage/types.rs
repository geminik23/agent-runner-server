use std::{error::Error, fmt};

use crate::core::{CleanupState, ExecutionMetadata, SessionDisposition};

/// Identifies a storage namespace; authentication is handled separately.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Namespace {
    pub owner_id: String,
    pub client_id: String,
}

/// Identifies an immutable skill version within a namespace.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkillRef {
    pub skill_id: String,
    pub version: String,
}

/// Contains the original text of an immutable skill version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkillVersion {
    pub reference: SkillRef,
    pub markdown: String,
}

/// Distinguishes creation from an identical repeated write.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SkillWrite {
    Inserted,
    Unchanged,
}

/// Describes a server session and its optional canonical native identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewSession {
    pub session_id: String,
    pub runtime: String,
    pub state_realm: String,
    pub native_id: Option<String>,
}

/// Holds the current reservation and last recorded cleanup disposition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredSession {
    pub session: NewSession,
    pub reserved_run_id: Option<String>,
    pub cleanup: Option<String>,
    pub disposition: Option<String>,
}

/// Pins a hook and destination revision and its payload projection for a run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HookBinding {
    pub binding_id: String,
    pub hook_revision: String,
    pub destination_revision: String,
    pub projection: Vec<u8>,
}

/// Supplies all values that must be fixed atomically on admission.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Admission {
    pub run_id: String,
    pub attempt_id: String,
    pub session_id: Option<String>,
    pub idempotency_key: String,
    pub normalized_request: Vec<u8>,
    pub manifest: Vec<u8>,
    pub metadata: ExecutionMetadata,
    pub queued_event_id: String,
    pub queued_event_payload: Vec<u8>,
    pub skills: Vec<SkillRef>,
    pub hooks: Vec<HookBinding>,
}

/// Represents the persisted admission snapshot, not authorization to access it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredRun {
    pub run_id: String,
    pub session_id: Option<String>,
    pub idempotency_key: String,
    pub normalized_request: Vec<u8>,
    pub manifest: Vec<u8>,
    pub metadata: ExecutionMetadata,
    pub state: String,
}

/// Exposes run status without the internal request, session, or manifest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunSummary {
    pub run_id: String,
    pub state: String,
    pub metadata: ExecutionMetadata,
}

/// Distinguishes a new run from an idempotent replay.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AdmissionOutcome {
    Accepted(StoredRun),
    Existing(StoredRun),
}

/// Represents an attempt and its persisted cleanup disposition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredAttempt {
    pub attempt_id: String,
    pub state: String,
    pub spawn_intent: bool,
    pub cleanup: Option<String>,
    pub disposition: Option<String>,
    pub metadata: ExecutionMetadata,
}

/// Represents an ordered event with its run's execution metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredEvent {
    pub event_id: String,
    pub sequence: i64,
    pub kind: String,
    pub payload: Vec<u8>,
    pub metadata: ExecutionMetadata,
}

/// Exposes event identity and kind without the unclassified payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventSummary {
    pub event_id: String,
    pub sequence: i64,
    pub kind: String,
    pub metadata: ExecutionMetadata,
}

/// Enumerates terminal states accepted by the completion transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TerminalState {
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
    Interrupted,
}

impl TerminalState {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::TimedOut => "timed_out",
            Self::Interrupted => "interrupted",
        }
    }
}

/// Pins the delivery payload to an admission-time hook binding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeliveryIntent {
    pub delivery_id: String,
    pub binding_id: String,
    pub payload: Vec<u8>,
}

/// Describes an optional durable inbox notification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewInboxItem {
    pub inbox_id: String,
    pub payload: Vec<u8>,
}

/// Supplies terminal values to be committed together.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Completion {
    pub run_id: String,
    pub attempt_id: String,
    pub state: TerminalState,
    pub result_revision: String,
    pub result_payload: Vec<u8>,
    pub terminal_event_id: String,
    pub terminal_event_payload: Vec<u8>,
    pub cleanup: CleanupState,
    pub disposition: SessionDisposition,
    pub deliveries: Vec<DeliveryIntent>,
    pub inbox: Option<NewInboxItem>,
}

/// Represents a committed result and its run's execution metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredResult {
    pub revision: String,
    pub payload: Vec<u8>,
    pub metadata: ExecutionMetadata,
}

/// Represents an intent and the exact binding captured at admission.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredDelivery {
    pub delivery_id: String,
    pub event_id: String,
    pub binding: HookBinding,
    pub payload: Vec<u8>,
    pub state: String,
    pub metadata: ExecutionMetadata,
}

/// Represents a committed inbox item and its run's execution metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredInboxItem {
    pub inbox_id: String,
    pub event_id: String,
    pub payload: Vec<u8>,
    pub metadata: ExecutionMetadata,
}

/// Reports storage conflicts without exposing another namespace's data.
#[derive(Debug)]
pub enum StorageError {
    Database(rusqlite::Error),
    InvalidInput(&'static str),
    NotFound,
    IdempotencyConflict,
    SkillVersionConflict,
    SessionBusy,
    QueueFull,
    AlreadyTerminal,
    InvalidState,
    InvalidDatabase,
    UnsupportedSchema(i64),
}

impl fmt::Display for StorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(error) => write!(formatter, "database error: {error}"),
            Self::InvalidInput(field) => write!(formatter, "invalid input: {field}"),
            Self::NotFound => formatter.write_str("record not found"),
            Self::IdempotencyConflict => {
                formatter.write_str("idempotency key conflicts with another request")
            }
            Self::SkillVersionConflict => {
                formatter.write_str("skill version conflicts with existing content")
            }
            Self::SessionBusy => formatter.write_str("session is reserved"),
            Self::QueueFull => formatter.write_str("execution queue is full"),
            Self::AlreadyTerminal => formatter.write_str("run is already terminal"),
            Self::InvalidState => {
                formatter.write_str("operation is not valid in the current state")
            }
            Self::InvalidDatabase => {
                formatter.write_str("database does not belong to this application")
            }
            Self::UnsupportedSchema(version) => {
                write!(formatter, "unsupported schema version: {version}")
            }
        }
    }
}

impl Error for StorageError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Database(error) => Some(error),
            _ => None,
        }
    }
}

impl From<rusqlite::Error> for StorageError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Database(error)
    }
}
