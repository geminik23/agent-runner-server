#![cfg(target_os = "linux")]

use std::{
    ffi::OsString,
    future::Future,
    io::{Read, Write},
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use agent_runner_server::process::{
    InputCompletion, ObserverError, OutputCompletion, OutputStream, ProcessError, ProcessLimits,
    ProcessObserver, ProcessSpec, ProcessSupervisor, StopReason, StopSignal,
};
use agent_runner_server::{
    access::{
        AccessLimits, ClientAccessControl, ClientRegistration, CredentialSecret, ExecutionProfile,
        Scope,
    },
    application::{BackendProfile, CancelOutcome, RunLimits, RunService},
    core::{
        Attempt, BackendError, CleanupState, EventSink, ExecutionBackend, ExecutionEvent,
        ExecutionEventKind, ExecutionMetadata, ExecutionOutcome, ExecutionRequest, ExecutionResult,
        Run, SessionDisposition, SessionRef, WarningObservation,
    },
    storage::Namespace,
};
use tokio_util::sync::CancellationToken;

const SCENARIO: &str = "AGENT_RUNNER_FAKE_SCENARIO";
const READY: &str = "AGENT_RUNNER_FAKE_READY";

fn spec(scenario: &str) -> ProcessSpec {
    ProcessSpec {
        program: std::env::current_exe().unwrap(),
        args: vec![
            "--exact".into(),
            "fake_process_fixture".into(),
            "--nocapture".into(),
            "--test-threads=1".into(),
            "--skip".into(),
            "spaces 한글 $;*".into(),
        ],
        cwd: None,
        env: vec![(SCENARIO.into(), scenario.into())],
        stdin: Vec::new(),
        limits: ProcessLimits {
            max_args: 16,
            max_env: 8,
            max_spec_bytes: 16384,
            max_stdin_bytes: 512 * 1024,
            max_stdout_bytes: 512 * 1024,
            max_stderr_bytes: 512 * 1024,
            max_line_bytes: 512 * 1024,
            timeout: Duration::from_secs(4),
            signal_grace: Duration::from_millis(120),
            kill_grace: Duration::from_secs(1),
            pipe_grace: Duration::from_millis(250),
        },
        stop_signal: StopSignal::Terminate,
    }
}

fn ready_env(spec: &mut ProcessSpec, path: &std::path::Path) {
    spec.env.push((READY.into(), path.as_os_str().to_owned()));
}

async fn wait_for_file(path: &std::path::Path) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("fixture did not become ready");
}

#[test]
fn fake_process_fixture() {
    let Ok(scenario) = std::env::var(SCENARIO) else {
        return;
    };
    match scenario.as_str() {
        "echo" => {
            let mut input = Vec::new();
            std::io::stdin().read_to_end(&mut input).unwrap();
            let args: Vec<_> = std::env::args().collect();
            println!("FAKE_PID={}", std::process::id());
            println!(
                "FAKE_ARG={}",
                args.iter().any(|value| value == "spaces 한글 $;*")
            );
            println!("FAKE_ENV_CLEAR={}", std::env::var_os("PATH").is_none());
            println!("FAKE_INPUT={}", String::from_utf8(input).unwrap());
        }
        "supervisor_closed_stdio" => {
            let output = std::env::var_os(READY).unwrap();
            // SAFETY: This fixture runs in its own subprocess and intentionally closes its standard descriptors.
            unsafe {
                libc::close(0);
                libc::close(1);
                libc::close(2);
            }
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let result = runtime.block_on(ProcessSupervisor::run(
                spec("echo"),
                CancellationToken::new(),
            ));
            let ok = result.is_ok_and(|report| {
                report.reason == StopReason::Exited
                    && report.exit.is_some_and(|status| status.code == Some(0))
                    && report.direct_io_settled()
            });
            std::fs::write(output, if ok { "ok" } else { "failed" }).unwrap();
        }
        "stream_gate" => {
            println!("FAKE_STREAM_READY");
            std::io::stdout().flush().unwrap();
            let gate = std::path::PathBuf::from(std::env::var_os(READY).unwrap());
            let deadline = std::time::Instant::now() + Duration::from_secs(3);
            while !gate.exists() && std::time::Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(5));
            }
            assert!(gate.exists());
            println!("FAKE_STREAM_DONE");
        }
        "identity" => {
            let mut input = Vec::new();
            std::io::stdin().read_to_end(&mut input).unwrap();
            println!(
                "FAKE_SESSION={}",
                std::env::var("AGENT_RUNNER_FAKE_SESSION").unwrap()
            );
        }
        "duplex" => {
            std::thread::scope(|scope| {
                scope.spawn(|| {
                    let mut out = std::io::stdout().lock();
                    for _ in 0..32 {
                        out.write_all(&[b'O'; 4096]).unwrap();
                    }
                    out.flush().unwrap();
                });
                scope.spawn(|| {
                    let mut out = std::io::stderr().lock();
                    for _ in 0..32 {
                        out.write_all(&[b'E'; 4096]).unwrap();
                    }
                    out.flush().unwrap();
                });
                let mut input = Vec::new();
                std::io::stdin().read_to_end(&mut input).unwrap();
                assert_eq!(input.len(), 256 * 1024);
            });
        }
        "stdout_flood" => {
            let mut out = std::io::stdout().lock();
            for _ in 0..256 {
                if out.write_all(&[b'O'; 4096]).is_err() {
                    break;
                }
            }
        }
        "stderr_flood" => {
            let mut out = std::io::stderr().lock();
            for _ in 0..256 {
                if out.write_all(&[b'E'; 4096]).is_err() {
                    break;
                }
            }
        }
        "long_line" => {
            let _ = std::io::stdout().write_all(&[b'L'; 4096]);
        }
        "partial_utf8" => {
            std::io::stdout().write_all(&[0xe2, 0x82]).unwrap();
            std::io::stdout().flush().unwrap();
            std::process::exit(0);
        }
        "exit_23" => std::process::exit(23),
        "ignore_stdin" => std::process::exit(0),
        "signal_term" | "signal_int" | "ignore_term" | "tree_parent" | "tree_child"
        | "tree_parent_ignore" | "tree_child_ignore" => {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async {
                use tokio::signal::unix::{SignalKind,signal};
                let mut term=signal(SignalKind::terminate()).unwrap();
                let mut interrupt=signal(SignalKind::interrupt()).unwrap();
                if scenario=="tree_parent" || scenario=="tree_parent_ignore" {
                    let ready=std::env::var_os(READY).unwrap();
                    let child_ready=std::path::PathBuf::from(&ready).with_extension("child");
                    let mut child=std::process::Command::new(std::env::current_exe().unwrap());
                    let mut spawned=child.args(["--exact","fake_process_fixture","--nocapture","--test-threads=1"])
                        .env(SCENARIO,if scenario=="tree_parent" {"tree_child"} else {"tree_child_ignore"}).env(READY,&child_ready)
                        .stdout(std::process::Stdio::inherit()).stderr(std::process::Stdio::inherit()).spawn().unwrap();
                    std::thread::spawn(move || {let _=spawned.wait();});
                    tokio::time::timeout(Duration::from_secs(2),async {while !child_ready.exists(){tokio::time::sleep(Duration::from_millis(5)).await;}}).await.unwrap();
                }
                if let Ok(id)=std::env::var("AGENT_RUNNER_FAKE_SESSION") { println!("FAKE_SESSION={id}"); std::io::stdout().flush().unwrap(); }
                std::fs::write(std::env::var_os(READY).unwrap(),std::process::id().to_string()).unwrap();
                tokio::select! { _=term.recv()=>println!("FAKE_SIGNAL=TERM:{scenario}"), _=interrupt.recv()=>println!("FAKE_SIGNAL=INT:{scenario}") }
                std::io::stdout().flush().unwrap();
                if scenario=="ignore_term" || scenario=="tree_child_ignore" { tokio::time::sleep(Duration::from_secs(30)).await; }
            });
        }
        "pipe_parent" | "pipe_parent_cancel" => {
            let mut input = Vec::new();
            std::io::stdin().read_to_end(&mut input).unwrap();
            if let Some(ready) = std::env::var_os(READY) {
                std::fs::write(&ready, std::process::id().to_string()).unwrap();
            }
            std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "fake_process_fixture",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env(
                    SCENARIO,
                    if scenario == "pipe_parent" {
                        "pipe_child"
                    } else {
                        "pipe_child_wait"
                    },
                )
                .stdout(std::process::Stdio::inherit())
                .stderr(std::process::Stdio::inherit())
                .spawn()
                .unwrap();
            std::process::exit(0);
        }
        "stdin_parent" => {
            let ready = std::env::var_os(READY).unwrap();
            let child_ready = std::path::PathBuf::from(&ready).with_extension("child");
            std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "fake_process_fixture",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env(SCENARIO, "stdin_child")
                .env(READY, &child_ready)
                .stdin(std::process::Stdio::inherit())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(2);
            while !child_ready.exists() && std::time::Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(5));
            }
            assert!(child_ready.exists());
            std::process::exit(0);
        }
        "stdin_child" => {
            std::fs::write(
                std::env::var_os(READY).unwrap(),
                std::process::id().to_string(),
            )
            .unwrap();
            std::thread::sleep(Duration::from_secs(3));
        }
        "pipe_child" => std::thread::sleep(Duration::from_millis(550)),
        "pipe_child_wait" => {
            if let Some(ready) = std::env::var_os(READY) {
                std::fs::write(
                    std::path::PathBuf::from(ready).with_extension("child"),
                    std::process::id().to_string(),
                )
                .unwrap();
            }
            std::thread::sleep(Duration::from_millis(1000));
        }
        _ => panic!("unknown fake scenario"),
    }
}

#[tokio::test]
async fn closed_standard_descriptors_do_not_collide_with_the_spawn_handshake() {
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("result");
    let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "fake_process_fixture",
            "--nocapture",
            "--test-threads=1",
        ])
        .env_clear()
        .env(SCENARIO, "supervisor_closed_stdio")
        .env(READY, &output)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(status.success());
    assert_eq!(std::fs::read_to_string(output).unwrap(), "ok");
}

#[tokio::test]
async fn post_release_spawn_failures_are_conservatively_reported() {
    let temp = tempfile::tempdir().unwrap();
    let mut input = spec("echo");
    input.program = temp.path().join("missing-executable");
    input.limits.timeout = Duration::from_secs(4);
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        ProcessSupervisor::run(input, CancellationToken::new()),
    )
    .await
    .unwrap();
    assert!(matches!(
        result,
        Err(ProcessError::StartedUnsettled {
            kind: std::io::ErrorKind::NotFound,
            ..
        })
    ));
}

#[tokio::test]
async fn passes_exact_argv_stdin_eof_and_explicit_environment() {
    let mut input = spec("echo");
    input.stdin = "hello 한글".as_bytes().to_vec();
    let report = ProcessSupervisor::run(input, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(report.reason, StopReason::Exited);
    assert_eq!(report.exit.unwrap().code, Some(0));
    assert_eq!(report.input, InputCompletion::Complete);
    assert!(report.direct_io_settled());
    assert!(!format!("{report:?}").contains("hello 한글"));
    let output = String::from_utf8(report.stdout.bytes).unwrap();
    assert!(output.contains(&format!("FAKE_PID={}", report.identity.pid)));
    assert!(output.contains("FAKE_ARG=true"));
    assert!(output.contains("FAKE_ENV_CLEAR=true"));
    assert!(output.contains("FAKE_INPUT=hello 한글"));
}

#[tokio::test]
async fn observer_receives_ordered_bytes_before_the_process_exits() {
    let dir = tempfile::tempdir().unwrap();
    let gate = dir.path().join("gate");
    let mut input = spec("stream_gate");
    ready_env(&mut input, &gate);
    let captured = Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
    let bytes = Arc::clone(&captured);
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let sender = std::sync::Mutex::new(Some(sender));
    let observer: ProcessObserver = Arc::new(move |stream, chunk| {
        if stream == OutputStream::Stdout {
            let mut bytes = bytes.lock().unwrap();
            bytes.extend_from_slice(chunk);
            if bytes
                .windows(b"FAKE_STREAM_READY".len())
                .any(|window| window == b"FAKE_STREAM_READY")
                && let Some(sender) = sender.lock().unwrap().take()
            {
                let _ = sender.send(());
            }
        }
        Ok(())
    });
    let task = tokio::spawn(ProcessSupervisor::run_with_observer(
        input,
        CancellationToken::new(),
        Some(observer),
    ));
    tokio::time::timeout(Duration::from_secs(2), receiver)
        .await
        .unwrap()
        .unwrap();
    assert!(!task.is_finished());
    std::fs::write(&gate, "release").unwrap();
    let report = task.await.unwrap().unwrap();
    assert_eq!(report.reason, StopReason::Exited);
    assert_eq!(*captured.lock().unwrap(), report.stdout.bytes);
}

#[tokio::test]
async fn rejected_streaming_observation_stops_the_process_without_reporting_success() {
    let observer: ProcessObserver = Arc::new(|stream, _| {
        if stream == OutputStream::Stdout {
            Err(ObserverError)
        } else {
            Ok(())
        }
    });
    let mut input = spec("duplex");
    input.stdin = vec![b'I'; 256 * 1024];
    let report =
        ProcessSupervisor::run_with_observer(input, CancellationToken::new(), Some(observer))
            .await
            .unwrap();
    assert_eq!(
        report.reason,
        StopReason::ObserverFailure(OutputStream::Stdout)
    );
    assert_eq!(report.stdout.completion, OutputCompletion::ObserverFailure);
    assert!(report.signals.graceful_attempted);
}

#[tokio::test]
async fn concurrent_stdout_stderr_and_stdin_do_not_deadlock() {
    let mut input = spec("duplex");
    input.stdin = vec![b'I'; 256 * 1024];
    let report = ProcessSupervisor::run(input, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(report.reason, StopReason::Exited);
    assert_eq!(report.exit.unwrap().code, Some(0));
    assert_eq!(report.input, InputCompletion::Complete);
    assert!(report.direct_io_settled());
    assert!(report.stdout.bytes.iter().filter(|&&b| b == b'O').count() >= 128 * 1024);
    assert!(report.stderr.bytes.iter().filter(|&&b| b == b'E').count() >= 128 * 1024);
}

#[tokio::test]
async fn output_and_line_limits_never_report_truncated_success() {
    for (scenario, stream) in [
        ("stdout_flood", OutputStream::Stdout),
        ("stderr_flood", OutputStream::Stderr),
    ] {
        let mut input = spec(scenario);
        input.limits.max_stdout_bytes = 2048;
        input.limits.max_stderr_bytes = 2048;
        let report = ProcessSupervisor::run(input, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(report.reason, StopReason::OutputLimit(stream));
        let output = if stream == OutputStream::Stdout {
            report.stdout
        } else {
            report.stderr
        };
        assert_eq!(output.completion, OutputCompletion::OutputLimit);
        assert_eq!(output.bytes.len(), 2048);
    }
    let mut input = spec("long_line");
    input.limits.max_line_bytes = 80;
    let report = ProcessSupervisor::run(input, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(report.reason, StopReason::LineLimit(OutputStream::Stdout));
    assert_eq!(report.stdout.completion, OutputCompletion::LineLimit);
}

#[tokio::test]
async fn preserves_partial_bytes_and_nonzero_exit_without_interpreting_them() {
    let report = ProcessSupervisor::run(spec("partial_utf8"), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(report.reason, StopReason::Exited);
    assert!(report.stdout.bytes.ends_with(&[0xe2, 0x82]));
    assert!(report.stdout.trailing_line_bytes >= 2);
    assert_eq!(report.stdout.completion, OutputCompletion::Eof);
    let report = ProcessSupervisor::run(spec("exit_23"), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(report.exit.unwrap().code, Some(23));
    assert_eq!(report.reason, StopReason::Exited);
}

#[tokio::test]
async fn incomplete_stdin_is_not_silently_accepted() {
    let mut input = spec("ignore_stdin");
    input.stdin = vec![b'I'; 256 * 1024];
    let report = ProcessSupervisor::run(input, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(report.reason, StopReason::InputFailure);
    assert!(matches!(
        report.input,
        InputCompletion::Failed(_) | InputCompletion::TimedOut | InputCompletion::Interrupted
    ));
}

#[tokio::test]
async fn unfinished_stdin_after_parent_exit_triggers_group_cleanup() {
    let dir = tempfile::tempdir().unwrap();
    let ready = dir.path().join("ready");
    let mut input = spec("stdin_parent");
    ready_env(&mut input, &ready);
    input.stdin = vec![b'I'; 256 * 1024];
    let report = ProcessSupervisor::run(input, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(report.reason, StopReason::InputFailure);
    assert_ne!(report.input, InputCompletion::Complete);
    assert!(report.signals.graceful_attempted && report.signals.force_attempted);
    let pid: u32 = std::fs::read_to_string(ready.with_extension("child"))
        .unwrap()
        .parse()
        .unwrap();
    tokio::time::timeout(Duration::from_millis(800), async {
        loop {
            let status = std::fs::read_to_string(format!("/proc/{pid}/stat"));
            let stopped = match status {
                Err(_) => true,
                Ok(line) => line
                    .rsplit_once(')')
                    .is_some_and(|(_, rest)| rest.trim_start().starts_with('Z')),
            };
            if stopped {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("stdin-holding descendant remained running");
}

#[tokio::test]
async fn cancellation_sends_selected_signal_to_the_owned_group() {
    for (scenario, signal, marker) in [
        (
            "signal_term",
            StopSignal::Terminate,
            "FAKE_SIGNAL=TERM:signal_term",
        ),
        (
            "signal_int",
            StopSignal::Interrupt,
            "FAKE_SIGNAL=INT:signal_int",
        ),
        (
            "tree_parent",
            StopSignal::Terminate,
            "FAKE_SIGNAL=TERM:tree_child",
        ),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let ready = dir.path().join("ready");
        let mut input = spec(scenario);
        input.stop_signal = signal;
        ready_env(&mut input, &ready);
        let cancel = CancellationToken::new();
        let task = tokio::spawn(ProcessSupervisor::run(input, cancel.clone()));
        wait_for_file(&ready).await;
        cancel.cancel();
        let report = task.await.unwrap().unwrap();
        assert_eq!(report.reason, StopReason::Cancelled);
        assert!(report.signals.graceful_attempted);
        assert!(report.signals.force_attempted);
        assert!(report.direct_io_settled());
        assert!(String::from_utf8_lossy(&report.stdout.bytes).contains(marker));
    }
}

#[tokio::test]
async fn force_signal_targets_owned_descendant_after_parent_exits() {
    let dir = tempfile::tempdir().unwrap();
    let ready = dir.path().join("ready");
    let mut input = spec("tree_parent_ignore");
    ready_env(&mut input, &ready);
    let cancel = CancellationToken::new();
    let task = tokio::spawn(ProcessSupervisor::run(input, cancel.clone()));
    wait_for_file(&ready).await;
    cancel.cancel();
    let report = task.await.unwrap().unwrap();
    assert_eq!(report.reason, StopReason::Cancelled);
    assert!(report.signals.force_attempted);
    assert_eq!(report.signals.force_error, None);
    assert!(report.direct_io_settled(), "{report:?}");
    assert!(
        String::from_utf8_lossy(&report.stdout.bytes)
            .contains("FAKE_SIGNAL=TERM:tree_child_ignore")
    );
}

#[tokio::test]
async fn cancellation_after_parent_exit_still_stops_owned_descendant() {
    let dir = tempfile::tempdir().unwrap();
    let ready = dir.path().join("ready");
    let child_ready = ready.with_extension("child");
    let mut input = spec("pipe_parent_cancel");
    ready_env(&mut input, &ready);
    input.limits.pipe_grace = Duration::from_secs(2);
    let cancel = CancellationToken::new();
    let task = tokio::spawn(ProcessSupervisor::run(input, cancel.clone()));
    wait_for_file(&ready).await;
    wait_for_file(&child_ready).await;
    let parent_pid: i32 = std::fs::read_to_string(&ready).unwrap().parse().unwrap();
    let parent = rustix::process::Pid::from_raw(parent_pid).unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while rustix::process::test_kill_process(parent).is_ok() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let started = std::time::Instant::now();
    cancel.cancel();
    let report = task.await.unwrap().unwrap();
    assert_eq!(report.reason, StopReason::Cancelled);
    assert!(report.signals.graceful_attempted && report.signals.force_attempted);
    assert_eq!(report.signals.graceful_error, None);
    assert!(report.direct_io_settled(), "{report:?}");
    assert!(started.elapsed() < Duration::from_millis(800));
}

#[tokio::test]
async fn deadline_is_enforced_while_a_descendant_keeps_a_pipe_open() {
    let mut input = spec("pipe_parent_cancel");
    input.limits.timeout = Duration::from_millis(400);
    input.limits.pipe_grace = Duration::from_secs(2);
    let report = ProcessSupervisor::run(input, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(report.reason, StopReason::Deadline);
    assert!(report.signals.graceful_attempted && report.signals.force_attempted);
    assert!(report.direct_io_settled(), "{report:?}");
}

#[tokio::test]
async fn dropping_during_pipe_drain_signals_the_original_group() {
    let dir = tempfile::tempdir().unwrap();
    let ready = dir.path().join("ready");
    let child_ready = ready.with_extension("child");
    let mut input = spec("pipe_parent_cancel");
    ready_env(&mut input, &ready);
    input.limits.pipe_grace = Duration::from_secs(2);
    let task = tokio::spawn(ProcessSupervisor::run(input, CancellationToken::new()));
    wait_for_file(&ready).await;
    wait_for_file(&child_ready).await;
    let parent_pid: i32 = std::fs::read_to_string(&ready).unwrap().parse().unwrap();
    let parent = rustix::process::Pid::from_raw(parent_pid).unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while rustix::process::test_kill_process(parent).is_ok() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let descendant: u32 = std::fs::read_to_string(&child_ready)
        .unwrap()
        .parse()
        .unwrap();
    task.abort();
    let _ = task.await;
    tokio::time::timeout(Duration::from_millis(800), async {
        loop {
            let status = std::fs::read_to_string(format!("/proc/{descendant}/stat"));
            let stopped = match status {
                Err(_) => true,
                Ok(line) => line
                    .rsplit_once(')')
                    .is_some_and(|(_, rest)| rest.trim_start().starts_with('Z')),
            };
            if stopped {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("owned descendant remained running after supervisor drop");
}

#[tokio::test]
async fn cancelling_an_owned_group_does_not_signal_an_independent_process() {
    struct ChildGuard(std::process::Child);
    impl Drop for ChildGuard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let independent_ready = dir.path().join("independent");
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "fake_process_fixture",
            "--nocapture",
            "--test-threads=1",
        ])
        .env_clear()
        .env(SCENARIO, "pipe_child_wait")
        .env(READY, &independent_ready)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let mut independent = ChildGuard(child);
    wait_for_file(&independent_ready.with_extension("child")).await;
    let ready = dir.path().join("owned");
    let mut input = spec("ignore_term");
    ready_env(&mut input, &ready);
    let cancel = CancellationToken::new();
    let task = tokio::spawn(ProcessSupervisor::run(input, cancel.clone()));
    wait_for_file(&ready).await;
    cancel.cancel();
    let report = task.await.unwrap().unwrap();
    assert_eq!(report.reason, StopReason::Cancelled);
    assert!(independent.0.try_wait().unwrap().is_none());
}

#[tokio::test]
async fn deadline_escalates_when_the_child_ignores_the_graceful_signal() {
    let dir = tempfile::tempdir().unwrap();
    let ready = dir.path().join("ready");
    let mut input = spec("ignore_term");
    ready_env(&mut input, &ready);
    input.limits.timeout = Duration::from_millis(350);
    let task = tokio::spawn(ProcessSupervisor::run(input, CancellationToken::new()));
    wait_for_file(&ready).await;
    let report = task.await.unwrap().unwrap();
    assert_eq!(report.reason, StopReason::Deadline);
    assert!(report.signals.graceful_attempted && report.signals.force_attempted);
    assert_eq!(report.exit.unwrap().signal, Some(9));
    assert!(report.direct_io_settled());
}

#[tokio::test]
async fn child_inheriting_a_pipe_is_bounded_and_does_not_prove_cleanup() {
    let mut input = spec("pipe_parent");
    input.limits.pipe_grace = Duration::from_millis(100);
    let report = ProcessSupervisor::run(input, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(report.reason, StopReason::PipeTimeout);
    assert_eq!(report.exit.unwrap().code, Some(0));
    assert!(report.signals.graceful_attempted && report.signals.force_attempted);
    assert!(report.direct_io_settled(), "{report:?}");
}

#[tokio::test]
async fn dropped_future_still_requests_bounded_background_cleanup() {
    let dir = tempfile::tempdir().unwrap();
    let ready = dir.path().join("ready");
    let mut input = spec("ignore_term");
    ready_env(&mut input, &ready);
    let task = tokio::spawn(ProcessSupervisor::run(input, CancellationToken::new()));
    wait_for_file(&ready).await;
    let pid: i32 = std::fs::read_to_string(&ready).unwrap().parse().unwrap();
    task.abort();
    let _ = task.await;
    tokio::time::timeout(Duration::from_secs(3), async {
        let pid = rustix::process::Pid::from_raw(pid).unwrap();
        loop {
            if rustix::process::test_kill_process(pid).is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("direct child remained alive after cancellation");
}

#[tokio::test]
async fn invalid_specs_and_pre_cancel_are_rejected_without_spawning() {
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(matches!(
        ProcessSupervisor::run(spec("echo"), cancel).await,
        Err(ProcessError::CancelledBeforeSpawn)
    ));
    let mut input = spec("echo");
    input.program = std::path::PathBuf::from("relative");
    assert!(matches!(
        ProcessSupervisor::run(input, CancellationToken::new()).await,
        Err(ProcessError::InvalidSpec("absolute paths"))
    ));
    let mut input = spec("echo");
    input.env.push((OsString::from("A=B"), OsString::from("x")));
    assert!(matches!(
        ProcessSupervisor::run(input, CancellationToken::new()).await,
        Err(ProcessError::InvalidSpec("environment key"))
    ));
    let mut input = spec("echo");
    input.stdin = vec![0; input.limits.max_stdin_bytes + 1];
    assert!(matches!(
        ProcessSupervisor::run(input, CancellationToken::new()).await,
        Err(ProcessError::InvalidSpec("input limit"))
    ));
    let dir = tempfile::tempdir().unwrap();
    let ready = dir.path().join("never-started");
    let mut input = spec("signal_term");
    ready_env(&mut input, &ready);
    let future = ProcessSupervisor::run(input, CancellationToken::new());
    drop(future);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!ready.exists());
}

struct FixtureBackend {
    native_id: String,
    scenario: &'static str,
    ready: Option<std::path::PathBuf>,
}

impl ExecutionBackend for FixtureBackend {
    fn metadata(&self) -> ExecutionMetadata {
        ExecutionMetadata {
            runtime: "dummy".into(),
            synthetic: true,
            scenario: Some("fake-process".into()),
        }
    }
    fn state_realm(&self) -> &str {
        "dummy"
    }
    fn execute<'a>(
        &'a self,
        request: ExecutionRequest,
        events: EventSink,
        cancellation: CancellationToken,
    ) -> Pin<Box<dyn Future<Output = ExecutionResult> + Send + 'a>> {
        Box::pin(async move {
            let metadata = self.metadata();
            let mut process = spec(self.scenario);
            process.stdin = request.prompt.as_bytes().to_vec();
            process.env.push((
                "AGENT_RUNNER_FAKE_SESSION".into(),
                self.native_id.clone().into(),
            ));
            if let Some(ready) = &self.ready {
                ready_env(&mut process, ready);
            }
            let report = ProcessSupervisor::run(process, cancellation).await;
            let session = SessionRef {
                runtime: "dummy".into(),
                state_realm: "dummy".into(),
                native_id: self.native_id.clone(),
            };
            let observed = report.as_ref().is_ok_and(|report| {
                String::from_utf8_lossy(&report.stdout.bytes)
                    .contains(&format!("FAKE_SESSION={}", self.native_id))
            });
            let accepted = observed
                && events(ExecutionEvent {
                    run_id: request.run_id.clone(),
                    attempt_id: request.attempt_id.clone(),
                    metadata: metadata.clone(),
                    kind: ExecutionEventKind::SessionObserved(session.clone()),
                })
                .is_ok();
            let successful = accepted
                && report.as_ref().is_ok_and(|report| {
                    report.reason == StopReason::Exited
                        && report.exit.is_some_and(|exit| exit.code == Some(0))
                        && report.direct_io_settled()
                });
            let cancelled = report
                .as_ref()
                .is_ok_and(|report| report.reason == StopReason::Cancelled);
            ExecutionResult {
                run: Run {
                    id: request.run_id.clone(),
                    metadata: metadata.clone(),
                },
                attempt: Attempt {
                    id: request.attempt_id,
                    run_id: request.run_id,
                    metadata: metadata.clone(),
                },
                metadata,
                session: accepted.then_some(session),
                warnings: vec![],
                warning_observation: WarningObservation::Unavailable,
                cleanup: if successful {
                    CleanupState::Settled
                } else {
                    CleanupState::Uncertain
                },
                session_disposition: if successful {
                    SessionDisposition::Reusable
                } else {
                    SessionDisposition::Uncertain
                },
                outcome: if successful {
                    ExecutionOutcome::Succeeded {
                        output: "fake completion".into(),
                    }
                } else if cancelled {
                    ExecutionOutcome::Cancelled
                } else {
                    ExecutionOutcome::Failed(BackendError::ExecutionFailed {
                        code: "fake_process_failed".into(),
                        message: "fixture failed".into(),
                    })
                },
            }
        })
    }
}

async fn fixture_service(
    path: &std::path::Path,
    native_id: String,
    scenario: &'static str,
    ready: Option<std::path::PathBuf>,
) -> (RunService, Arc<ClientAccessControl>) {
    let namespace = Namespace {
        owner_id: "owner".into(),
        client_id: "client".into(),
    };
    let access = Arc::new(
        ClientAccessControl::new(
            [ClientRegistration::new(
                "test",
                CredentialSecret::new([7; 32]),
                namespace,
                [
                    Scope::RunStart,
                    Scope::RunRead,
                    Scope::RunCancel,
                    Scope::SessionRead,
                ],
                ["profile".into()],
            )
            .with_generation("generation")],
            [ExecutionProfile::dummy(
                "profile",
                ["workspace".into()],
                4096,
            )],
            AccessLimits {
                max_request_bytes: 8192,
                max_skill_bytes: 1024,
                max_identifier_bytes: 128,
            },
        )
        .unwrap(),
    );
    let backend = Arc::new(FixtureBackend {
        native_id,
        scenario,
        ready,
    });
    let profiles = vec![BackendProfile {
        profile_id: "profile".into(),
        revision: "v1".into(),
        state_realm: "dummy".into(),
        backend,
    }];
    let limits = RunLimits {
        max_outstanding: 4,
        max_workers: 1,
        database_queue: 16,
        event_buffer: 16,
        max_events_per_run: 16,
        max_event_bytes: 512,
        max_result_bytes: 4096,
        max_page_size: 8,
        run_timeout: Duration::from_secs(10),
        cleanup_grace: Duration::from_secs(2),
        poll_interval: Duration::from_millis(5),
    };
    let service = RunService::open(path, Arc::clone(&access), profiles, limits)
        .await
        .unwrap();
    (service, access)
}

async fn terminal_state(
    service: &RunService,
    client: &agent_runner_server::access::AuthenticatedClient,
    run_id: &str,
) -> String {
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            let run = service.get_run(client, run_id).await.unwrap().unwrap();
            if matches!(
                run.state.as_str(),
                "succeeded" | "failed" | "cancelled" | "timed_out" | "interrupted"
            ) {
                break run.state;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn fake_backend_checks_native_identity_limits_in_the_real_run_service() {
    for (id, expected) in [
        ("native-fixture".to_owned(), "succeeded"),
        ("x".repeat(2048), "failed"),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("runs.sqlite");
        let (service, access) = fixture_service(&path, id.clone(), "identity", None).await;
        let client = access.authenticate("test", &[7; 32]).unwrap();
        let body=br#"{"profile_id":"profile","workspace_id":"workspace","input":{"request":"fixture"},"idempotency_key":"identity"}"#;
        let receipt = service.start(&client, body).await.unwrap();
        assert_eq!(
            terminal_state(&service, &client, &receipt.run_id).await,
            expected
        );
        let db = rusqlite::Connection::open_with_flags(
            &path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        let (native,reserved):(Option<String>,Option<String>)=db.query_row("SELECT native_id,reserved_run_id FROM sessions WHERE owner_id='owner' AND client_id='client' AND session_id=?1",[receipt.session_id.unwrap()],|row|Ok((row.get(0)?,row.get(1)?))).unwrap();
        if expected == "succeeded" {
            assert_eq!(native.as_deref(), Some(id.as_str()));
            assert!(reserved.is_none());
        } else {
            assert!(native.is_none());
            assert_eq!(reserved.as_deref(), Some(receipt.run_id.as_str()));
        }
        service.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn fake_backend_cancellation_preserves_the_service_stop_and_session_quarantine() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("runs.sqlite");
    let ready = temp.path().join("ready");
    let (service, access) = fixture_service(
        &path,
        "native-cancel".into(),
        "signal_term",
        Some(ready.clone()),
    )
    .await;
    let client = access.authenticate("test", &[7; 32]).unwrap();
    let body=br#"{"profile_id":"profile","workspace_id":"workspace","input":{"request":"fixture"},"idempotency_key":"cancel"}"#;
    let receipt = service.start(&client, body).await.unwrap();
    wait_for_file(&ready).await;
    assert_eq!(
        service.cancel(&client, &receipt.run_id).await.unwrap(),
        CancelOutcome::CancelRequested
    );
    assert_eq!(
        terminal_state(&service, &client, &receipt.run_id).await,
        "cancelled"
    );
    let db =
        rusqlite::Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();
    let (cleanup,disposition,reserved):(Option<String>,Option<String>,Option<String>)=db.query_row("SELECT cleanup,disposition,reserved_run_id FROM sessions WHERE owner_id='owner' AND client_id='client' AND session_id=?1",[receipt.session_id.unwrap()],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?))).unwrap();
    assert_eq!(cleanup.as_deref(), Some("uncertain"));
    assert_eq!(disposition.as_deref(), Some("uncertain"));
    assert_eq!(reserved.as_deref(), Some(receipt.run_id.as_str()));
    service.shutdown().await.unwrap();
}
