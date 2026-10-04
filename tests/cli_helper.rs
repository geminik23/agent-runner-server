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
async fn helper_kills_and_reaps_its_child_when_status_output_fails() {
    let _gate = SPAWN_GATE.read().await;
    use std::process::Stdio;

    let full = std::fs::OpenOptions::new()
        .write(true)
        .open("/dev/full")
        .unwrap();
    let mut helper = tokio::process::Command::new(env!("CARGO_BIN_EXE_agent-runner-cli-helper"))
        .args(["--", "/bin/sleep", "10"])
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

#[tokio::test]
async fn helper_reports_reaped_descendants_only_after_a_detached_descendant_exits() {
    let _gate = SPAWN_GATE.read().await;
    let temp = tempdir().unwrap();
    let script = temp.path().join("cli-fixture");
    let marker = temp.path().join("descendant-finished");
    std::fs::write(
        &script,
        format!(
            "#!/bin/sh\n/usr/bin/setsid /bin/sh -c 'sleep 0.3; printf finished > \"{}\"' </dev/null >/dev/null 2>&1 &\nexit 4\n",
            marker.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_agent-runner-cli-helper"))
        .arg("--")
        .arg(&script)
        .arg("fixture")
        .stdin(std::process::Stdio::null())
        .output()
        .await
        .unwrap();
    // The CLI's own failure code is preserved, and the reaped marker follows the exit marker only after the descendant finished.
    assert_eq!(output.status.code(), Some(4));
    assert_eq!(
        output.stderr,
        b"\x1eagent-runner:child-started\n\x1eagent-runner:child-exited\n\x1eagent-runner:descendants-reaped\n"
    );
    assert_eq!(std::fs::read_to_string(marker).unwrap(), "finished");
}
