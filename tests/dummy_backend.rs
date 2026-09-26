use std::sync::{Arc, Mutex};

use agent_runner_server::{
    core::{
        BackendError, CancellationToken, CleanupState, EventSink, ExecutionBackend, ExecutionEvent,
        ExecutionEventKind, ExecutionOutcome, ExecutionRequest, SessionDisposition, SessionRef,
        SessionRequest, WarningObservation,
    },
    runtimes::dummy::{DELAYED_SCENARIO_DURATION, DummyBackend, DummyScenario},
};

type EventLog = Arc<Mutex<Vec<ExecutionEvent>>>;

fn request() -> ExecutionRequest {
    ExecutionRequest {
        run_id: "run-1".into(),
        attempt_id: "attempt-1".into(),
        session: SessionRequest::New,
        prompt: "Literal input: {{not a template}}".into(),
    }
}

fn event_log() -> (EventSink, EventLog) {
    let log = Arc::new(Mutex::new(Vec::new()));
    let events = Arc::clone(&log);
    let sink: EventSink = Arc::new(move |event| events.lock().unwrap().push(event));
    (sink, log)
}

#[tokio::test]
async fn success_captures_exact_input_and_marks_all_observations() {
    let backend = DummyBackend::new(DummyScenario::Success, 8);
    let input = request();
    let (sink, log) = event_log();

    let result = backend
        .execute(input.clone(), sink, CancellationToken::new())
        .await;

    assert_eq!(backend.captured_inputs().len(), 1);
    let capture = &backend.captured_inputs()[0];
    assert_eq!(capture.request, input);
    assert_eq!(capture.metadata, result.metadata);
    assert_eq!(result.run.id, "run-1");
    assert_eq!(result.attempt.id, "attempt-1");
    assert_eq!(result.attempt.run_id, result.run.id);
    assert_eq!(result.run.metadata, result.metadata);
    assert_eq!(result.attempt.metadata, result.metadata);
    assert_eq!(result.metadata.runtime, "dummy");
    assert!(result.metadata.synthetic);
    assert_eq!(result.metadata.scenario.as_deref(), Some("success"));
    assert!(
        matches!(&result.outcome, ExecutionOutcome::Succeeded { output } if output == "Scripted dummy output")
    );
    assert!(result.warnings.is_empty());
    let session = result.session.as_ref().unwrap();
    assert_eq!(session.runtime, "dummy");
    assert_eq!(session.state_realm, "dummy");
    assert_eq!(session.native_id, "dummy-5-run-1-attempt-1");
    assert_eq!(result.warning_observation, WarningObservation::Complete);
    assert_eq!(result.cleanup, CleanupState::Settled);
    assert_eq!(result.session_disposition, SessionDisposition::Reusable);

    let events = log.lock().unwrap();
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].kind, ExecutionEventKind::Started);
    assert_eq!(
        events[1].kind,
        ExecutionEventKind::SessionObserved(session.clone())
    );
    assert!(matches!(
        events[2].kind,
        ExecutionEventKind::Progress { .. }
    ));
    for event in events.iter() {
        assert_eq!(event.run_id, result.run.id);
        assert_eq!(event.attempt_id, result.attempt.id);
        assert_eq!(event.metadata, result.metadata);
    }
}

#[tokio::test(start_paused = true)]
async fn delayed_scenario_emits_start_before_finishing() {
    let backend = Arc::new(DummyBackend::new(DummyScenario::DelayedSuccess, 8));
    let (sink, log) = event_log();
    let task = tokio::spawn(async move {
        backend
            .execute(request(), sink, CancellationToken::new())
            .await
    });

    tokio::task::yield_now().await;
    assert!(!task.is_finished());
    assert_eq!(log.lock().unwrap().len(), 2);
    assert_eq!(log.lock().unwrap()[0].kind, ExecutionEventKind::Started);
    assert!(matches!(
        log.lock().unwrap()[1].kind,
        ExecutionEventKind::SessionObserved(_)
    ));

    tokio::time::advance(DELAYED_SCENARIO_DURATION).await;
    let result = task.await.unwrap();
    assert!(matches!(result.outcome, ExecutionOutcome::Succeeded { .. }));
    assert_eq!(result.metadata.scenario.as_deref(), Some("delayed-success"));
    assert_eq!(log.lock().unwrap().len(), 3);
}

#[tokio::test(start_paused = true)]
async fn cancellation_during_delay_returns_cancelled_without_progress() {
    let backend = Arc::new(DummyBackend::new(DummyScenario::DelayedSuccess, 8));
    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();
    let (sink, log) = event_log();
    let task =
        tokio::spawn(async move { backend.execute(request(), sink, task_cancellation).await });

    tokio::task::yield_now().await;
    assert_eq!(log.lock().unwrap().len(), 2);
    cancellation.cancel();
    let result = task.await.unwrap();
    assert_eq!(result.outcome, ExecutionOutcome::Cancelled);
    assert!(result.session.is_some());
    assert_eq!(result.cleanup, CleanupState::Settled);
    assert_eq!(result.session_disposition, SessionDisposition::Uncertain);
    assert_eq!(log.lock().unwrap().len(), 2);
    assert!(result.metadata.synthetic);
    assert_eq!(result.metadata.scenario.as_deref(), Some("delayed-success"));
}

#[tokio::test]
async fn cancellation_before_start_still_captures_input_without_events() {
    let backend = DummyBackend::new(DummyScenario::Success, 8);
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let (sink, log) = event_log();

    let result = backend.execute(request(), sink, cancellation).await;

    assert_eq!(result.outcome, ExecutionOutcome::Cancelled);
    assert_eq!(backend.captured_inputs()[0].request, request());
    assert!(log.lock().unwrap().is_empty());
}

#[tokio::test]
async fn cancellation_from_event_observer_prevents_a_late_success() {
    let backend = DummyBackend::new(DummyScenario::Success, 8);
    let cancellation = CancellationToken::new();
    let observer_cancellation = cancellation.clone();
    let (record, log) = event_log();
    let sink: EventSink = Arc::new(move |event| {
        if event.kind == ExecutionEventKind::Started {
            observer_cancellation.cancel();
        }
        record(event);
    });

    let result = backend.execute(request(), sink, cancellation).await;
    assert_eq!(result.outcome, ExecutionOutcome::Cancelled);
    let session = result.session.unwrap();
    assert_eq!(result.session_disposition, SessionDisposition::Uncertain);
    let events = log.lock().unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].kind, ExecutionEventKind::Started);
    assert_eq!(events[1].kind, ExecutionEventKind::SessionObserved(session));
}

#[tokio::test]
async fn warning_is_preserved_in_both_event_and_successful_result() {
    let backend = DummyBackend::new(DummyScenario::Warning, 8);
    let (sink, log) = event_log();
    let result = backend
        .execute(request(), sink, CancellationToken::new())
        .await;

    assert_eq!(result.metadata.scenario.as_deref(), Some("warning"));
    assert!(matches!(result.outcome, ExecutionOutcome::Succeeded { .. }));
    let warnings = &result.warnings;
    assert_eq!(warnings.len(), 1);
    assert_eq!(warnings[0].code, "dummy_warning");
    let events = log.lock().unwrap();
    assert_eq!(events.len(), 4);
    assert_eq!(
        events[3].kind,
        ExecutionEventKind::Warning(warnings[0].clone())
    );
    assert!(events.iter().all(|event| event.metadata == result.metadata));
}

#[tokio::test]
async fn scripted_failure_is_not_reported_as_success() {
    let backend = DummyBackend::new(DummyScenario::Failure, 8);
    let (sink, log) = event_log();
    let result = backend
        .execute(request(), sink, CancellationToken::new())
        .await;

    assert_eq!(
        result.outcome,
        ExecutionOutcome::Failed(BackendError::ExecutionFailed {
            code: "scripted_failure".into(),
            message: "Scripted dummy failure".into(),
        })
    );
    assert!(result.session.is_some());
    assert_eq!(result.metadata.scenario.as_deref(), Some("failure"));
    assert!(
        log.lock()
            .unwrap()
            .iter()
            .all(|event| event.metadata == result.metadata)
    );
}

#[tokio::test]
async fn dummy_session_reference_is_preserved_on_resume() {
    let backend = DummyBackend::new(DummyScenario::Success, 8);
    let (sink, _) = event_log();
    let first = backend
        .execute(request(), sink, CancellationToken::new())
        .await;
    assert!(matches!(first.outcome, ExecutionOutcome::Succeeded { .. }));
    assert_eq!(first.session_disposition, SessionDisposition::Reusable);
    let session = first.session.unwrap();
    let mut next = request();
    next.run_id = "run-2".into();
    next.session = SessionRequest::Resume(session.clone());
    let (sink, _) = event_log();

    let resumed = backend
        .execute(next.clone(), sink, CancellationToken::new())
        .await;
    assert!(matches!(
        resumed.outcome,
        ExecutionOutcome::Succeeded { .. }
    ));
    assert_eq!(resumed.session, Some(session));
    assert_eq!(resumed.session_disposition, SessionDisposition::Reusable);
    assert_eq!(backend.captured_inputs()[1].request, next);
}

#[tokio::test]
async fn foreign_or_invalid_session_is_rejected_before_any_event() {
    let backend = DummyBackend::new(DummyScenario::Success, 8);
    for (runtime, state_realm, native_id) in [
        ("other-runtime", "dummy", "session-1"),
        ("dummy", "other-realm", "session-1"),
        ("dummy", "dummy", ""),
    ] {
        let mut input = request();
        input.session = SessionRequest::Resume(SessionRef {
            runtime: runtime.into(),
            state_realm: state_realm.into(),
            native_id: native_id.into(),
        });
        let (sink, log) = event_log();
        let result = backend.execute(input, sink, CancellationToken::new()).await;
        assert_eq!(
            result.outcome,
            ExecutionOutcome::Failed(BackendError::IncompatibleSession)
        );
        assert_eq!(
            result.session_disposition,
            SessionDisposition::NotApplicable
        );
        assert!(log.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn empty_run_or_attempt_ids_are_rejected() {
    let backend = DummyBackend::new(DummyScenario::Success, 8);
    for (run_id, attempt_id) in [("", "attempt-1"), ("run-1", "")] {
        let mut input = request();
        input.run_id = run_id.into();
        input.attempt_id = attempt_id.into();
        let (sink, log) = event_log();
        let result = backend.execute(input, sink, CancellationToken::new()).await;
        assert_eq!(
            result.outcome,
            ExecutionOutcome::Failed(BackendError::InvalidRequest)
        );
        assert!(log.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn backend_contract_supports_dynamic_dispatch() {
    let backend: Box<dyn ExecutionBackend> = Box::new(DummyBackend::new(DummyScenario::Success, 8));
    let metadata = backend.metadata();
    assert_eq!(metadata.runtime, "dummy");
    assert!(metadata.synthetic);
    assert_eq!(metadata.scenario.as_deref(), Some("success"));
    let (sink, _) = event_log();
    let result = backend
        .execute(request(), sink, CancellationToken::new())
        .await;
    assert_eq!(metadata, result.metadata);
    assert!(matches!(result.outcome, ExecutionOutcome::Succeeded { .. }));
}

#[tokio::test]
async fn new_sessions_are_distinct_for_distinct_run_attempt_pairs() {
    let backend = DummyBackend::new(DummyScenario::Success, 0);
    let mut sessions = Vec::new();
    for (run_id, attempt_id) in [("a", "b-c"), ("a-b", "c"), ("a", "other")] {
        let mut input = request();
        input.run_id = run_id.into();
        input.attempt_id = attempt_id.into();
        let (sink, _) = event_log();
        let result = backend.execute(input, sink, CancellationToken::new()).await;
        sessions.push(result.session.unwrap());
    }
    assert_ne!(sessions[0], sessions[1]);
    assert_ne!(sessions[0], sessions[2]);
}

#[tokio::test]
async fn capture_can_be_disabled_bounded_and_drained() {
    let backend = DummyBackend::new(DummyScenario::Success, 1);
    for prompt in ["first", "second", "third"] {
        let mut input = request();
        input.prompt = prompt.into();
        let (sink, _) = event_log();
        backend.execute(input, sink, CancellationToken::new()).await;
    }
    assert_eq!(backend.dropped_capture_count(), 2);
    assert_eq!(backend.captured_inputs()[0].request.prompt, "third");
    assert_eq!(backend.take_captured_inputs().len(), 1);
    assert!(backend.captured_inputs().is_empty());
    assert_eq!(backend.dropped_capture_count(), 2);

    let disabled = DummyBackend::new(DummyScenario::Success, 0);
    let (sink, _) = event_log();
    disabled
        .execute(request(), sink, CancellationToken::new())
        .await;
    assert!(disabled.captured_inputs().is_empty());
}

#[tokio::test]
async fn cancellation_after_warning_preserves_the_observed_warning() {
    let backend = DummyBackend::new(DummyScenario::Warning, 0);
    let cancellation = CancellationToken::new();
    let observer_cancellation = cancellation.clone();
    let sink: EventSink = Arc::new(move |event| {
        if matches!(event.kind, ExecutionEventKind::Warning(_)) {
            observer_cancellation.cancel();
        }
    });
    let result = backend.execute(request(), sink, cancellation).await;
    assert_eq!(result.outcome, ExecutionOutcome::Cancelled);
    assert_eq!(result.warnings.len(), 1);
    assert_eq!(result.warnings[0].code, "dummy_warning");
    assert_eq!(result.warning_observation, WarningObservation::Complete);
}
