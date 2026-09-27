//! Types shared by execution backends, independent of a vendor CLI.

use std::{future::Future, pin::Pin, sync::Arc};

pub use tokio_util::sync::CancellationToken;

/// Identifies the runtime and, for synthetic work, the selected scenario.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionMetadata {
    pub runtime: String,
    pub synthetic: bool,
    pub scenario: Option<String>,
}

/// Identifies a run without implying that it has been persisted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Run {
    pub id: String,
    pub metadata: ExecutionMetadata,
}

/// Identifies an attempt belonging to a run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Attempt {
    pub id: String,
    pub run_id: String,
    pub metadata: ExecutionMetadata,
}

/// Identifies native session state within one runtime and state realm.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionRef {
    pub runtime: String,
    pub state_realm: String,
    pub native_id: String,
}

/// Requests a new session or refers to an existing runtime session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionRequest {
    New,
    Resume(SessionRef),
}

/// Supplies one attempt's input to a backend.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionRequest {
    pub run_id: String,
    pub attempt_id: String,
    /// The approved workspace identifier, resolved by the trusted backend profile.
    pub workspace_id: String,
    pub session: SessionRequest,
    pub prompt: String,
}

/// Describes a non-fatal backend observation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionWarning {
    pub code: String,
    pub message: String,
}

/// Describes a backend observation rather than a committed terminal event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecutionEventKind {
    Started,
    SessionObserved(SessionRef),
    Progress {
        message: String,
    },
    Warning(ExecutionWarning),
    CleanupObserved {
        cleanup: CleanupState,
        session: SessionDisposition,
    },
}

/// Associates an observation with its run, attempt, and provenance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionEvent {
    pub run_id: String,
    pub attempt_id: String,
    pub metadata: ExecutionMetadata,
    pub kind: ExecutionEventKind,
}

/// Describes a backend failure without treating it as a successful result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BackendError {
    InvalidRequest,
    IncompatibleSession,
    ExecutionFailed { code: String, message: String },
}

/// Describes how much of the backend's warning set was observable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WarningObservation {
    Complete,
    Partial,
    Unavailable,
}

/// Separates execution completion from process or resource cleanup.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CleanupState {
    Settled,
    Uncertain,
}

/// Describes backend-level reuse evidence; callers must still enforce access and reservations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionDisposition {
    NotApplicable,
    Reusable,
    Uncertain,
}

/// Captures the outcome of an attempt before application-level persistence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecutionOutcome {
    Succeeded { output: String },
    Failed(BackendError),
    Cancelled,
}

/// Keeps provenance on the run, attempt, and outcome returned by a backend.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionResult {
    pub run: Run,
    pub attempt: Attempt,
    pub metadata: ExecutionMetadata,
    pub session: Option<SessionRef>,
    pub warnings: Vec<ExecutionWarning>,
    pub warning_observation: WarningObservation,
    pub cleanup: CleanupState,
    pub session_disposition: SessionDisposition,
    pub outcome: ExecutionOutcome,
}

/// Reports that an observation could not be accepted by the event consumer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EventSinkError;

/// Accepts observations synchronously; consumers must avoid blocking or panicking.
/// A backend must stop producing observations after its execution future returns.
pub type EventSink = Arc<dyn Fn(ExecutionEvent) -> Result<(), EventSinkError> + Send + Sync>;

/// Supports runtime-selected backends without exposing vendor-specific types.
/// Implementations must not block executor threads and must cooperate with cancellation.
/// Constructing the execution future must not start work before its first poll.
/// A dropped future does not establish external process or session cleanup.
pub trait ExecutionBackend: Send + Sync {
    /// Returns the provenance that will be attached to this backend's execution.
    fn metadata(&self) -> ExecutionMetadata;

    /// Identifies the actual native state namespace used by this backend instance.
    fn state_realm(&self) -> &str;

    fn execute<'a>(
        &'a self,
        request: ExecutionRequest,
        events: EventSink,
        cancellation: CancellationToken,
    ) -> Pin<Box<dyn Future<Output = ExecutionResult> + Send + 'a>>;
}
