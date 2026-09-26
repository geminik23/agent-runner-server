//! A fixed-scenario backend for testing without an agent or credentials.

use std::{collections::VecDeque, future::Future, pin::Pin, sync::Mutex, time::Duration};

use crate::core::{
    Attempt, BackendError, CancellationToken, CleanupState, EventSink, ExecutionBackend,
    ExecutionEvent, ExecutionEventKind, ExecutionMetadata, ExecutionOutcome, ExecutionRequest,
    ExecutionResult, ExecutionWarning, Run, SessionDisposition, SessionRef, SessionRequest,
    WarningObservation,
};

/// The deterministic delay used by the delayed scenario.
pub const DELAYED_SCENARIO_DURATION: Duration = Duration::from_secs(5);

/// The only scenarios selectable for the synthetic backend.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DummyScenario {
    Success,
    DelayedSuccess,
    Warning,
    Failure,
}

impl DummyScenario {
    fn id(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::DelayedSuccess => "delayed-success",
            Self::Warning => "warning",
            Self::Failure => "failure",
        }
    }
}

/// Records exactly what the backend received, including its synthetic provenance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CapturedInput {
    pub request: ExecutionRequest,
    pub metadata: ExecutionMetadata,
}

#[derive(Default)]
struct CaptureBuffer {
    inputs: VecDeque<CapturedInput>,
    dropped: usize,
}

/// Runs a fixed scenario and optionally retains a bounded number of inputs.
pub struct DummyBackend {
    scenario: DummyScenario,
    capture_limit: usize,
    captured: Mutex<CaptureBuffer>,
}

impl DummyBackend {
    /// Creates a backend with a fixed scenario and a maximum number of captured inputs.
    /// A zero capture limit disables input retention.
    pub fn new(scenario: DummyScenario, capture_limit: usize) -> Self {
        Self {
            scenario,
            capture_limit,
            captured: Mutex::new(CaptureBuffer::default()),
        }
    }

    /// Returns a snapshot of retained inputs in their capture order.
    pub fn captured_inputs(&self) -> Vec<CapturedInput> {
        self.captured
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .inputs
            .iter()
            .cloned()
            .collect()
    }

    /// Removes and returns retained inputs without resetting the dropped count.
    pub fn take_captured_inputs(&self) -> Vec<CapturedInput> {
        self.captured
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .inputs
            .drain(..)
            .collect()
    }

    /// Reports how many older inputs were evicted because the capture limit was reached.
    pub fn dropped_capture_count(&self) -> usize {
        self.captured
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .dropped
    }

    async fn run(
        &self,
        request: ExecutionRequest,
        events: EventSink,
        cancellation: CancellationToken,
    ) -> ExecutionResult {
        let metadata = self.metadata();
        if self.capture_limit > 0 {
            let mut captured = self
                .captured
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if captured.inputs.len() == self.capture_limit {
                captured.inputs.pop_front();
                captured.dropped = captured.dropped.saturating_add(1);
            }
            captured.inputs.push_back(CapturedInput {
                request: request.clone(),
                metadata: metadata.clone(),
            });
        }

        let result = |outcome, session: Option<SessionRef>, warnings| ExecutionResult {
            run: Run {
                id: request.run_id.clone(),
                metadata: metadata.clone(),
            },
            attempt: Attempt {
                id: request.attempt_id.clone(),
                run_id: request.run_id.clone(),
                metadata: metadata.clone(),
            },
            metadata: metadata.clone(),
            session_disposition: match (&outcome, &session) {
                (_, None) => SessionDisposition::NotApplicable,
                (ExecutionOutcome::Succeeded { .. }, Some(_)) => SessionDisposition::Reusable,
                (_, Some(_)) => SessionDisposition::Uncertain,
            },
            session,
            warnings,
            warning_observation: WarningObservation::Complete,
            cleanup: CleanupState::Settled,
            outcome,
        };
        let emit = |kind| {
            events(ExecutionEvent {
                run_id: request.run_id.clone(),
                attempt_id: request.attempt_id.clone(),
                metadata: metadata.clone(),
                kind,
            });
        };

        if request.run_id.is_empty() || request.attempt_id.is_empty() {
            return result(
                ExecutionOutcome::Failed(BackendError::InvalidRequest),
                None,
                vec![],
            );
        }
        if cancellation.is_cancelled() {
            return result(ExecutionOutcome::Cancelled, None, vec![]);
        }

        let session = match &request.session {
            SessionRequest::New => SessionRef {
                runtime: "dummy".into(),
                state_realm: "dummy".into(),
                native_id: format!(
                    "dummy-{}-{}-{}",
                    request.run_id.len(),
                    request.run_id,
                    request.attempt_id
                ),
            },
            SessionRequest::Resume(session)
                if session.runtime == "dummy"
                    && session.state_realm == "dummy"
                    && !session.native_id.is_empty() =>
            {
                session.clone()
            }
            SessionRequest::Resume(_) => {
                return result(
                    ExecutionOutcome::Failed(BackendError::IncompatibleSession),
                    None,
                    vec![],
                );
            }
        };

        emit(ExecutionEventKind::Started);
        emit(ExecutionEventKind::SessionObserved(session.clone()));
        if self.scenario == DummyScenario::DelayedSuccess {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return result(ExecutionOutcome::Cancelled, Some(session), vec![]),
                _ = tokio::time::sleep(DELAYED_SCENARIO_DURATION) => {}
            }
        }
        if cancellation.is_cancelled() {
            return result(ExecutionOutcome::Cancelled, Some(session), vec![]);
        }

        emit(ExecutionEventKind::Progress {
            message: "Scripted dummy progress".into(),
        });
        if cancellation.is_cancelled() {
            return result(ExecutionOutcome::Cancelled, Some(session), vec![]);
        }

        let mut warnings = Vec::new();
        if self.scenario == DummyScenario::Warning {
            let warning = ExecutionWarning {
                code: "dummy_warning".into(),
                message: "Scripted dummy warning".into(),
            };
            emit(ExecutionEventKind::Warning(warning.clone()));
            warnings.push(warning);
        }
        if cancellation.is_cancelled() {
            return result(ExecutionOutcome::Cancelled, Some(session), warnings);
        }
        if self.scenario == DummyScenario::Failure {
            return result(
                ExecutionOutcome::Failed(BackendError::ExecutionFailed {
                    code: "scripted_failure".into(),
                    message: "Scripted dummy failure".into(),
                }),
                Some(session),
                warnings,
            );
        }

        result(
            ExecutionOutcome::Succeeded {
                output: "Scripted dummy output".into(),
            },
            Some(session),
            warnings,
        )
    }
}

impl ExecutionBackend for DummyBackend {
    fn metadata(&self) -> ExecutionMetadata {
        ExecutionMetadata {
            runtime: "dummy".into(),
            synthetic: true,
            scenario: Some(self.scenario.id().into()),
        }
    }

    fn execute<'a>(
        &'a self,
        request: ExecutionRequest,
        events: EventSink,
        cancellation: CancellationToken,
    ) -> Pin<Box<dyn Future<Output = ExecutionResult> + Send + 'a>> {
        Box::pin(self.run(request, events, cancellation))
    }
}
