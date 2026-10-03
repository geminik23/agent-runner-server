#![cfg(target_os = "linux")]

use agent_runner_server::{
    core::CancellationToken,
    process::{ProcessLimits, ProcessSpec, ProcessSupervisor, StopReason, StopSignal},
};
use std::{
    os::unix::fs::PermissionsExt,
    time::{Duration, Instant},
};
use tempfile::tempdir;

/// Serializes tests that assert a pipe is closed against every other test that spawns processes.
/// A process forked by another test holds copies of this process's descriptors until it executes, which can keep a closed pipe open.
static SPAWN_GATE: tokio::sync::RwLock<()> = tokio::sync::RwLock::const_new(());

#[tokio::test]
async fn helper_waits_for_a_detached_pipe_free_descendant_before_success() {
    let _gate = SPAWN_GATE.read().await;
    let temp = tempdir().unwrap();
    let script = temp.path().join("cli-fixture");
    let marker = temp.path().join("descendant-finished");
    let script_text = format!(
        "#!/bin/sh\n/usr/bin/setsid /bin/sh -c 'sleep 0.3; printf finished > \"{}\"' </dev/null >/dev/null 2>&1 &\nprintf 'cli result\\n'\nexit 0\n",
        marker.display()
    );
    std::fs::write(&script, script_text).unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();
    let spec = ProcessSpec {
        program: env!("CARGO_BIN_EXE_agent-runner-cli-helper").into(),
        args: vec!["--".into(), script.into_os_string(), "fixture".into()],
        cwd: Some(temp.path().to_path_buf()),
        env: vec![("PATH".into(), "/usr/bin:/bin".into())],
        stdin: vec![],
        limits: ProcessLimits {
            max_args: 8,
            max_env: 4,
            max_spec_bytes: 8192,
            max_stdin_bytes: 1024,
            max_stdout_bytes: 1024,
            max_stderr_bytes: 1024,
            max_line_bytes: 1024,
            timeout: Duration::from_secs(3),
            signal_grace: Duration::from_millis(200),
            kill_grace: Duration::from_millis(200),
            pipe_grace: Duration::from_millis(200),
        },
        stop_signal: StopSignal::Interrupt,
    };
    let before = Instant::now();
    let report = ProcessSupervisor::run(spec, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(report.reason, StopReason::Exited);
    assert_eq!(report.exit.unwrap().code, Some(0));
    assert!(before.elapsed() >= Duration::from_millis(200));
    assert_eq!(std::fs::read_to_string(marker).unwrap(), "finished");
    assert_eq!(report.stdout.bytes, b"cli result\n");
}

#[tokio::test]
async fn helper_does_not_keep_cli_stdin_open_after_the_cli_exits() {
    let _gate = SPAWN_GATE.write().await;
    use std::process::Stdio;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let temp = tempdir().unwrap();
    let script = temp.path().join("cli-fixture");
    std::fs::write(
        &script,
        "#!/bin/sh\n/usr/bin/setsid /bin/sh -c 'sleep 2' </dev/null >/dev/null 2>&1 &\nexit 0\n",
    )
    .unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut helper = tokio::process::Command::new(env!("CARGO_BIN_EXE_agent-runner-cli-helper"))
        .arg("--")
        .arg(&script)
        .arg("fixture")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(helper.stderr.take().unwrap()).lines();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let line = lines
                .next_line()
                .await
                .unwrap()
                .expect("helper exited early");
            if line == "\u{1e}agent-runner:child-exited" {
                break;
            }
        }
    })
    .await
    .expect("missing child exit marker");
    assert!(helper.try_wait().unwrap().is_none());
    let write = helper
        .stdin
        .take()
        .unwrap()
        .write_all(b"later fixture input")
        .await;
    let status = tokio::time::timeout(Duration::from_secs(4), helper.wait())
        .await
        .expect("helper did not reap its descendant")
        .unwrap();
    assert!(status.success());
    assert_eq!(write.unwrap_err().kind(), std::io::ErrorKind::BrokenPipe);
}

#[tokio::test]
async fn brokered_input_closes_before_child_exit_is_reported() {
    let _gate = SPAWN_GATE.write().await;
    use std::process::Stdio;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let temp = tempdir().unwrap();
    let script = temp.path().join("cli-fixture");
    let marker = temp.path().join("received");
    std::fs::write(
        &script,
        "#!/bin/sh\nexec 3<&0\n/usr/bin/setsid /bin/sh -c 'IFS= read -r line; if [ \"$line\" = fixture-input ]; then printf received > \"$1\"; fi' child \"$1\" <&3 3<&- >/dev/null 2>&1 &\nexec 3<&-\nexit 0\n",
    )
    .unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut helper = tokio::process::Command::new(env!("CARGO_BIN_EXE_agent-runner-cli-helper"))
        .arg("--broker-stdin")
        .arg("--")
        .arg(&script)
        .arg(&marker)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(helper.stderr.take().unwrap()).lines();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let line = lines
                .next_line()
                .await
                .unwrap()
                .expect("helper exited early");
            if line == "\u{1e}agent-runner:child-exited" {
                break;
            }
        }
    })
    .await
    .expect("missing child exit marker");
    let write = helper
        .stdin
        .take()
        .unwrap()
        .write_all(b"fixture-input\n")
        .await;
    let status = tokio::time::timeout(Duration::from_secs(4), helper.wait())
        .await
        .expect("helper did not reap its descendant")
        .unwrap();
    assert!(!status.success());
    assert_eq!(write.unwrap_err().kind(), std::io::ErrorKind::BrokenPipe);
    assert!(!marker.exists());
}

#[tokio::test]
async fn brokered_input_forwards_data_and_eof_to_a_live_child() {
    let _gate = SPAWN_GATE.read().await;
    use std::process::Stdio;
    use tokio::io::AsyncWriteExt;

    let mut helper = tokio::process::Command::new(env!("CARGO_BIN_EXE_agent-runner-cli-helper"))
        .args(["--broker-stdin", "--", "/bin/sh", "-c", "cat"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut stdin = helper.stdin.take().unwrap();
    stdin.write_all(b"fixture input\n").await.unwrap();
    stdin.shutdown().await.unwrap();
    drop(stdin);
    let output = tokio::time::timeout(Duration::from_secs(4), helper.wait_with_output())
        .await
        .expect("helper did not finish")
        .unwrap();
    assert!(output.status.success());
    assert_eq!(output.stdout, b"fixture input\n");
}

#[tokio::test]
async fn broker_does_not_change_the_callers_stdin_status_flags() {
    let _gate = SPAWN_GATE.read().await;
    use std::{
        io::Write,
        os::fd::{AsRawFd, FromRawFd, OwnedFd},
        process::Stdio,
    };
    use tokio::io::{AsyncBufReadExt, BufReader};

    let mut fds = [-1; 2];
    // SAFETY: fds is a valid array for the two returned pipe descriptors.
    assert_eq!(unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) }, 0);
    // SAFETY: pipe2 returned two new owned descriptors.
    let input = unsafe { std::fs::File::from(OwnedFd::from_raw_fd(fds[0])) };
    let mut output = unsafe { std::fs::File::from(OwnedFd::from_raw_fd(fds[1])) };
    let mut helper = tokio::process::Command::new(env!("CARGO_BIN_EXE_agent-runner-cli-helper"))
        .args([
            "--broker-stdin",
            "--",
            "/bin/sh",
            "-c",
            "IFS= read -r line; printf 'ready\\n'; IFS= read -r line || :",
        ])
        .stdin(Stdio::from(input.try_clone().unwrap()))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    output.write_all(b"first\n").unwrap();
    let mut lines = BufReader::new(helper.stdout.take().unwrap()).lines();
    let ready = tokio::time::timeout(Duration::from_secs(3), lines.next_line())
        .await
        .expect("child did not receive input")
        .unwrap();
    assert_eq!(ready.as_deref(), Some("ready"));
    // SAFETY: input remains open for the duration of this query.
    let flags = unsafe { libc::fcntl(input.as_raw_fd(), libc::F_GETFL) };
    drop(output);
    let status = tokio::time::timeout(Duration::from_secs(3), helper.wait())
        .await
        .expect("helper did not finish")
        .unwrap();
    assert!(status.success());
    assert!(flags >= 0 && flags & libc::O_NONBLOCK == 0);
}

#[tokio::test]
async fn supervisor_can_drive_brokered_input_and_explicit_eof() {
    let _gate = SPAWN_GATE.read().await;
    use agent_runner_server::process::{
        ControlledInput, InputCompletion, OutputStream, ProcessObserver,
    };
    use std::sync::{Arc, Mutex};

    let temp = tempdir().unwrap();
    let spec = ProcessSpec {
        program: env!("CARGO_BIN_EXE_agent-runner-cli-helper").into(),
        args: vec![
            "--broker-stdin".into(),
            "--".into(),
            "/bin/sh".into(),
            "-c".into(),
            "IFS= read -r init; printf 'ready\\n'; IFS= read -r input; printf 'received:%s\\n' \"$input\"; cat >/dev/null".into(),
        ],
        cwd: Some(temp.path().to_path_buf()),
        env: vec![("PATH".into(), "/usr/bin:/bin".into())],
        stdin: b"initialize\n".to_vec(),
        limits: ProcessLimits {
            max_args: 8,
            max_env: 4,
            max_spec_bytes: 8192,
            max_stdin_bytes: 1024,
            max_stdout_bytes: 1024,
            max_stderr_bytes: 1024,
            max_line_bytes: 1024,
            timeout: Duration::from_secs(3),
            signal_grace: Duration::from_millis(200),
            kill_grace: Duration::from_millis(200),
            pipe_grace: Duration::from_millis(200),
        },
        stop_signal: StopSignal::Interrupt,
    };
    let (ready_send, ready_recv) = tokio::sync::oneshot::channel();
    let state = Arc::new(Mutex::new((Vec::new(), Some(ready_send))));
    let observer: ProcessObserver = Arc::new(move |stream, bytes| {
        if stream == OutputStream::Stdout {
            let mut state = state
                .lock()
                .map_err(|_| agent_runner_server::process::ObserverError)?;
            state.0.extend_from_slice(bytes);
            if state
                .0
                .windows(b"ready\n".len())
                .any(|chunk| chunk == b"ready\n")
                && let Some(sender) = state.1.take()
            {
                let _ = sender.send(());
            }
        }
        Ok(())
    });
    let (send, receive) = tokio::sync::mpsc::channel(2);
    let task = tokio::spawn(ProcessSupervisor::run_interactive(
        spec,
        CancellationToken::new(),
        Some(observer),
        receive,
    ));
    tokio::time::timeout(Duration::from_secs(3), ready_recv)
        .await
        .expect("child did not become ready")
        .unwrap();
    let (written, receipt) = tokio::sync::oneshot::channel();
    send.send(ControlledInput::Confirmed {
        bytes: b"fixture\n".to_vec(),
        written,
    })
    .await
    .unwrap();
    receipt.await.unwrap();
    send.send(ControlledInput::Finish).await.unwrap();
    let report = task.await.unwrap().unwrap();
    assert_eq!(report.reason, StopReason::Exited);
    assert_eq!(report.input, InputCompletion::Complete);
    assert_eq!(report.exit.unwrap().code, Some(0));
    assert_eq!(report.stdout.bytes, b"ready\nreceived:fixture\n");
}

#[tokio::test]
async fn supervisor_cancels_brokered_input_without_reporting_completion() {
    let _gate = SPAWN_GATE.read().await;
    use agent_runner_server::process::{InputCompletion, OutputStream, ProcessObserver};
    use std::sync::{Arc, Mutex};

    let temp = tempdir().unwrap();
    let spec = ProcessSpec {
        program: env!("CARGO_BIN_EXE_agent-runner-cli-helper").into(),
        args: vec![
            "--broker-stdin".into(),
            "--".into(),
            "/bin/sh".into(),
            "-c".into(),
            "IFS= read -r init; printf 'ready\\n'; sleep 10".into(),
        ],
        cwd: Some(temp.path().to_path_buf()),
        env: vec![("PATH".into(), "/usr/bin:/bin".into())],
        stdin: b"initialize\n".to_vec(),
        limits: ProcessLimits {
            max_args: 8,
            max_env: 4,
            max_spec_bytes: 8192,
            max_stdin_bytes: 1024,
            max_stdout_bytes: 1024,
            max_stderr_bytes: 1024,
            max_line_bytes: 1024,
            timeout: Duration::from_secs(5),
            signal_grace: Duration::from_millis(100),
            kill_grace: Duration::from_millis(100),
            pipe_grace: Duration::from_millis(100),
        },
        stop_signal: StopSignal::Terminate,
    };
    let (ready_send, ready_recv) = tokio::sync::oneshot::channel();
    let state = Arc::new(Mutex::new((Vec::new(), Some(ready_send))));
    let observer: ProcessObserver = Arc::new(move |stream, bytes| {
        if stream == OutputStream::Stdout {
            let mut state = state
                .lock()
                .map_err(|_| agent_runner_server::process::ObserverError)?;
            state.0.extend_from_slice(bytes);
            if state
                .0
                .windows(b"ready\n".len())
                .any(|chunk| chunk == b"ready\n")
                && let Some(sender) = state.1.take()
            {
                let _ = sender.send(());
            }
        }
        Ok(())
    });
    let cancellation = CancellationToken::new();
    let (send, receive) = tokio::sync::mpsc::channel(2);
    let task = tokio::spawn(ProcessSupervisor::run_interactive(
        spec,
        cancellation.clone(),
        Some(observer),
        receive,
    ));
    tokio::time::timeout(Duration::from_secs(3), ready_recv)
        .await
        .expect("child did not become ready")
        .unwrap();
    cancellation.cancel();
    let report = tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .expect("brokered cancellation did not complete")
        .unwrap()
        .unwrap();
    drop(send);
    assert_eq!(report.reason, StopReason::Cancelled);
    assert_ne!(report.input, InputCompletion::Complete);
    assert!(report.signals.graceful_attempted);
}

#[tokio::test]
async fn broker_rejects_a_write_only_stdin_pipe() {
    let _gate = SPAWN_GATE.read().await;
    use std::{
        os::fd::{FromRawFd, OwnedFd},
        process::Stdio,
    };

    let mut fds = [-1; 2];
    // SAFETY: fds is a valid array for the two returned pipe descriptors.
    assert_eq!(unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) }, 0);
    // SAFETY: pipe2 returned two new owned descriptors.
    let read_end = unsafe { OwnedFd::from_raw_fd(fds[0]) };
    let write_end = unsafe { OwnedFd::from_raw_fd(fds[1]) };
    let mut helper = tokio::process::Command::new(env!("CARGO_BIN_EXE_agent-runner-cli-helper"))
        .args(["--broker-stdin", "--", "/bin/cat", "-"])
        .stdin(Stdio::from(std::fs::File::from(write_end)))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let status = tokio::time::timeout(Duration::from_secs(3), helper.wait())
        .await
        .expect("helper did not reject invalid stdin")
        .unwrap();
    drop(read_end);
    assert!(!status.success());
}

#[tokio::test]
async fn helper_kills_and_reaps_its_child_when_status_output_fails() {
    let _gate = SPAWN_GATE.read().await;
    use std::process::Stdio;

    let full = std::fs::OpenOptions::new()
        .write(true)
        .open("/dev/full")
        .unwrap();
    let mut helper = tokio::process::Command::new(env!("CARGO_BIN_EXE_agent-runner-cli-helper"))
        .args(["--broker-stdin", "--", "/bin/sleep", "10"])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::from(full))
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let status = tokio::time::timeout(Duration::from_secs(3), helper.wait())
        .await
        .expect("helper did not terminate after status failure")
        .unwrap();
    assert_eq!(status.code(), Some(2));
}

#[tokio::test]
async fn broker_failure_keeps_reaping_detached_descendants() {
    let _gate = SPAWN_GATE.read().await;
    use std::process::Stdio;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let temp = tempdir().unwrap();
    let script = temp.path().join("cli-fixture");
    let marker = temp.path().join("descendant-finished");
    let descendant_ready = temp.path().join("descendant-ready");
    std::fs::write(
        &script,
        "#!/bin/sh\n/usr/bin/setsid /bin/sh -c 'printf ready > \"$2\"; sleep 0.3; printf finished > \"$1\"' child \"$1\" \"$2\" </dev/null >/dev/null 2>&1 &\nexec 0<&-\nwhile [ ! -f \"$2\" ]; do sleep 0.01; done\nprintf 'ready\\n'\nsleep 1\n",
    )
    .unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut helper = tokio::process::Command::new(env!("CARGO_BIN_EXE_agent-runner-cli-helper"))
        .arg("--broker-stdin")
        .arg("--")
        .arg(&script)
        .arg(&marker)
        .arg(&descendant_ready)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(helper.stdout.take().unwrap()).lines();
    let ready = tokio::time::timeout(Duration::from_secs(3), lines.next_line())
        .await
        .expect("child did not close stdin")
        .unwrap();
    assert_eq!(ready.as_deref(), Some("ready"));
    let _ = helper
        .stdin
        .take()
        .unwrap()
        .write_all(b"fixture input\n")
        .await;
    let status = tokio::time::timeout(Duration::from_secs(4), helper.wait())
        .await
        .expect("helper did not reap descendants after broker failure")
        .unwrap();
    assert!(!status.success());
    assert_eq!(std::fs::read_to_string(marker).unwrap(), "finished");
}

#[tokio::test]
async fn brokered_input_forwards_large_stream_while_output_is_drained() {
    let _gate = SPAWN_GATE.read().await;
    use std::process::Stdio;
    use tokio::io::AsyncWriteExt;

    let mut helper = tokio::process::Command::new(env!("CARGO_BIN_EXE_agent-runner-cli-helper"))
        .args(["--broker-stdin", "--", "/bin/sh", "-c", "cat"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut stdin = helper.stdin.take().unwrap();
    let expected = vec![b'x'; 256 * 1024];
    let input = expected.clone();
    let write = async move {
        stdin.write_all(&input).await.unwrap();
        stdin.shutdown().await.unwrap();
    };
    let collect = async move { helper.wait_with_output().await.unwrap() };
    let (_, output) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(write, collect)
    })
    .await
    .expect("broker stalled while forwarding");
    assert!(output.status.success());
    assert_eq!(output.stdout, expected);
}

#[tokio::test]
async fn a_detached_descendant_outliving_deadline_never_reports_normal_completion() {
    let _gate = SPAWN_GATE.read().await;
    let temp = tempdir().unwrap();
    let script = temp.path().join("cli-fixture");
    let marker = temp.path().join("descendant-finished");
    let script_text = format!(
        "#!/bin/sh\n/usr/bin/setsid /bin/sh -c 'sleep 0.5; printf finished > \"{}\"' </dev/null >/dev/null 2>&1 &\nprintf 'cli result\\n'\nexit 0\n",
        marker.display()
    );
    std::fs::write(&script, script_text).unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();
    let spec = ProcessSpec {
        program: env!("CARGO_BIN_EXE_agent-runner-cli-helper").into(),
        args: vec!["--".into(), script.into_os_string(), "fixture".into()],
        cwd: Some(temp.path().to_path_buf()),
        env: vec![("PATH".into(), "/usr/bin:/bin".into())],
        stdin: vec![],
        limits: ProcessLimits {
            max_args: 8,
            max_env: 4,
            max_spec_bytes: 8192,
            max_stdin_bytes: 1024,
            max_stdout_bytes: 1024,
            max_stderr_bytes: 1024,
            max_line_bytes: 1024,
            timeout: Duration::from_millis(100),
            signal_grace: Duration::from_millis(50),
            kill_grace: Duration::from_millis(50),
            pipe_grace: Duration::from_millis(50),
        },
        stop_signal: StopSignal::Interrupt,
    };
    let report = ProcessSupervisor::run(spec, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(report.reason, StopReason::Deadline);
    tokio::time::timeout(Duration::from_secs(2), async {
        while !marker.exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
}
