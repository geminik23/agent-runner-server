#![cfg(target_os = "linux")]

use std::{
    os::{
        fd::AsRawFd,
        unix::{net::UnixStream as StdUnixStream, process::CommandExt},
    },
    process::Stdio,
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
    process::{Child, Command},
};

const READY: u8 = 1;
const WRITTEN: u8 = 2;
const CLOSED: u8 = 3;
const CHILD_EXITED: u8 = 4;
const MAGIC: u64 = u64::from_be_bytes(*b"ARSBR001");

fn start(program: &str, args: &[&str]) -> (Child, UnixStream) {
    start_with_input(program, args, Stdio::piped())
}

fn start_with_input(program: &str, args: &[&str], input: Stdio) -> (Child, UnixStream) {
    let (parent, child) = StdUnixStream::pair().unwrap();
    let child_fd = rustix::io::fcntl_dupfd_cloexec(&child, 128).unwrap();
    let capacity: libc::c_int = 2048;
    // SAFETY: child_fd and the option value remain valid during setsockopt.
    assert_eq!(
        unsafe {
            libc::setsockopt(
                child_fd.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_SNDBUF,
                (&capacity as *const libc::c_int).cast(),
                std::mem::size_of_val(&capacity) as libc::socklen_t,
            )
        },
        0
    );
    drop(child);
    let raw = child_fd.as_raw_fd();
    let raw_text = raw.to_string();
    let mut command = Command::new(env!("CARGO_BIN_EXE_agent-runner-cli-helper"));
    command
        .arg("--framed-stdin")
        .arg(&raw_text)
        .arg("--")
        .arg(program);
    for arg in args {
        command.arg(if *arg == "CONTROL_FD" {
            raw_text.as_str()
        } else {
            arg
        });
    }
    command
        .stdin(input)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    // SAFETY: The callback only changes descriptor flags in the forked child before exec.
    unsafe {
        command.as_std_mut().pre_exec(move || {
            let flags = libc::fcntl(child_fd.as_raw_fd(), libc::F_GETFD);
            if flags < 0
                || libc::fcntl(
                    child_fd.as_raw_fd(),
                    libc::F_SETFD,
                    flags & !libc::FD_CLOEXEC,
                ) < 0
            {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let process = command.spawn().unwrap();
    drop(command);
    parent.set_nonblocking(true).unwrap();
    (process, UnixStream::from_std(parent).unwrap())
}

async fn status(control: &mut UnixStream) -> (u8, u64) {
    let mut bytes = [0_u8; 9];
    tokio::time::timeout(Duration::from_secs(4), control.read_exact(&mut bytes))
        .await
        .expect("status timeout")
        .expect("status channel closed");
    (bytes[0], u64::from_be_bytes(bytes[1..].try_into().unwrap()))
}

async fn frame(process: &mut Child, sequence: u64, data: &[u8]) {
    let mut header = [0_u8; 12];
    header[..4].copy_from_slice(&(data.len() as u32).to_be_bytes());
    header[4..].copy_from_slice(&sequence.to_be_bytes());
    let input = process.stdin.as_mut().unwrap();
    input.write_all(&header).await.unwrap();
    input.write_all(data).await.unwrap();
}

async fn finish(process: &mut Child, sequence: u64) {
    let mut header = [0_u8; 12];
    header[..4].copy_from_slice(&u32::MAX.to_be_bytes());
    header[4..].copy_from_slice(&sequence.to_be_bytes());
    process
        .stdin
        .as_mut()
        .unwrap()
        .write_all(&header)
        .await
        .unwrap();
}

#[tokio::test]
async fn oversized_initial_frame_is_rejected_before_helper_exec() {
    use agent_runner_server::{
        core::CancellationToken,
        process::{ProcessError, ProcessLimits, ProcessSpec, ProcessSupervisor, StopSignal},
    };
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let marker = dir.path().join("spawned");
    let helper = dir.path().join("helper");
    std::fs::write(
        &helper,
        format!("#!/bin/sh\nprintf spawned > '{}'\n", marker.display()),
    )
    .unwrap();
    std::fs::set_permissions(&helper, std::fs::Permissions::from_mode(0o700)).unwrap();
    let spec = ProcessSpec {
        program: helper,
        args: vec!["--".into(), "/bin/true".into(), "fixture".into()],
        cwd: Some(dir.path().to_path_buf()),
        env: vec![("PATH".into(), "/usr/bin:/bin".into())],
        stdin: vec![0; 16 * 1024 * 1024 + 1],
        limits: ProcessLimits {
            max_args: 8,
            max_env: 4,
            max_spec_bytes: 8192,
            max_stdin_bytes: 32 * 1024 * 1024,
            max_stdout_bytes: 1024,
            max_stderr_bytes: 1024,
            max_line_bytes: 1024,
            timeout: Duration::from_secs(4),
            signal_grace: Duration::from_millis(100),
            kill_grace: Duration::from_millis(100),
            pipe_grace: Duration::from_millis(200),
        },
        stop_signal: StopSignal::Terminate,
    };
    let (_sender, receiver) = agent_runner_server::process::brokered_input_channel(1, 1024);
    let result =
        ProcessSupervisor::run_interactive_brokered(spec, CancellationToken::new(), None, receiver)
            .await;
    assert!(matches!(
        result,
        Err(ProcessError::InvalidSpec("broker input limit"))
    ));
    assert!(!marker.exists());
}

#[tokio::test]
async fn invalid_control_descriptor_is_rejected_before_target_exec() {
    let dir = tempfile::tempdir().unwrap();
    let marker = dir.path().join("spawned");
    let file = std::fs::File::open("/dev/null").unwrap();
    let control = rustix::io::fcntl_dupfd_cloexec(&file, 128).unwrap();
    let number = control.as_raw_fd().to_string();
    let mut command = Command::new(env!("CARGO_BIN_EXE_agent-runner-cli-helper"));
    command.args([
        "--framed-stdin",
        &number,
        "--",
        "/bin/sh",
        "-c",
        "printf spawned > \"$1\"",
        "fixture",
        marker.to_str().unwrap(),
    ]);
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    // SAFETY: The callback changes only this descriptor's flag in the forked child.
    unsafe {
        command.as_std_mut().pre_exec(move || {
            let flags = libc::fcntl(control.as_raw_fd(), libc::F_GETFD);
            if flags < 0
                || libc::fcntl(
                    control.as_raw_fd(),
                    libc::F_SETFD,
                    flags & !libc::FD_CLOEXEC,
                ) < 0
            {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut process = command.spawn().unwrap();
    drop(command);
    let outcome = tokio::time::timeout(Duration::from_secs(3), process.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(!outcome.success());
    assert!(!marker.exists());
}

#[tokio::test]
async fn never_connected_named_fifo_is_rejected_before_target_exec() {
    use std::{
        ffi::CString,
        os::unix::{ffi::OsStrExt, fs::OpenOptionsExt},
    };

    let dir = tempfile::tempdir().unwrap();
    let marker = dir.path().join("spawned");
    let fifo = dir.path().join("input.fifo");
    let name = CString::new(fifo.as_os_str().as_bytes()).unwrap();
    // SAFETY: name is a valid NUL-terminated path in the private fixture directory.
    assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
    let input = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(&fifo)
        .unwrap();
    let (mut child, _control) = start_with_input(
        "/bin/sh",
        &[
            "-c",
            "printf spawned > \"$1\"",
            "fixture",
            marker.to_str().unwrap(),
        ],
        Stdio::from(input),
    );
    let outcome = tokio::time::timeout(Duration::from_secs(3), child.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(!outcome.success());
    assert!(!marker.exists());
}

#[tokio::test]
async fn already_closed_upstream_is_rejected_before_target_exec() {
    use std::os::fd::{FromRawFd, OwnedFd};

    let dir = tempfile::tempdir().unwrap();
    let marker = dir.path().join("spawned");
    let mut fds = [-1; 2];
    // SAFETY: fds can hold the two descriptors returned by pipe2.
    assert_eq!(unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) }, 0);
    // SAFETY: pipe2 created two new owned descriptors.
    let reader = unsafe { OwnedFd::from_raw_fd(fds[0]) };
    let writer = unsafe { OwnedFd::from_raw_fd(fds[1]) };
    drop(writer);
    let (mut child, _control) = start_with_input(
        "/bin/sh",
        &[
            "-c",
            "printf spawned > \"$1\"",
            "fixture",
            marker.to_str().unwrap(),
        ],
        Stdio::from(std::fs::File::from(reader)),
    );
    let outcome = tokio::time::timeout(Duration::from_secs(3), child.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(!outcome.success());
    assert!(!marker.exists());
}

#[tokio::test]
async fn invalid_broker_upstream_is_rejected_before_target_exec() {
    let dir = tempfile::tempdir().unwrap();
    let marker = dir.path().join("spawned");
    let script = "printf spawned > \"$1\"";
    let (mut child, _control) = start_with_input(
        "/bin/sh",
        &["-c", script, "fixture", marker.to_str().unwrap()],
        Stdio::null(),
    );
    let outcome = tokio::time::timeout(Duration::from_secs(3), child.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(!outcome.success());
    assert!(!marker.exists());
}

#[tokio::test]
async fn framed_status_confirms_forwarding_and_explicit_close() {
    let (mut child, mut control) = start(
        "/bin/sh",
        &[
            "-c",
            "IFS= read -r line; printf 'read:%s\\n' \"$line\"; cat >/dev/null",
        ],
    );
    assert_eq!(status(&mut control).await, (READY, MAGIC));
    frame(&mut child, 1, b"hello\n").await;
    assert_eq!(status(&mut control).await, (WRITTEN, 1));
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .as_deref(),
        Some("read:hello")
    );
    finish(&mut child, 2).await;
    assert_eq!(status(&mut control).await, (CLOSED, 2));
    assert_eq!(status(&mut control).await, (CHILD_EXITED, 0));
    assert!(
        tokio::time::timeout(Duration::from_secs(3), child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
}

#[tokio::test]
async fn framed_control_descriptor_is_not_inherited_by_the_cli() {
    let script = "if [ -e /proc/self/fd/\"$1\" ]; then printf 'inherited\\n'; else printf 'closed\\n'; fi; cat >/dev/null";
    let (mut child, mut control) = start("/bin/sh", &["-c", script, "fixture", "CONTROL_FD"]);
    assert_eq!(status(&mut control).await, (READY, MAGIC));
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .as_deref(),
        Some("closed")
    );
    finish(&mut child, 1).await;
    assert_eq!(status(&mut control).await, (CLOSED, 1));
    assert_eq!(status(&mut control).await, (CHILD_EXITED, 0));
    assert!(
        tokio::time::timeout(Duration::from_secs(3), child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
}

#[tokio::test]
async fn a_stalled_status_peer_cannot_leave_the_broker_blocked() {
    let (mut child, _control) = start("/bin/sh", &["-c", "cat >/dev/null"]);
    let mut input = child.stdin.take().unwrap();
    let mut frames = Vec::new();
    for sequence in 1_u64..=5000 {
        frames.extend_from_slice(&0_u32.to_be_bytes());
        frames.extend_from_slice(&sequence.to_be_bytes());
    }
    let writer = tokio::spawn(async move { input.write_all(&frames).await });
    let outcome = tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .expect("broker blocked on status output")
        .unwrap();
    assert!(!outcome.success());
    let _ = tokio::time::timeout(Duration::from_secs(2), writer)
        .await
        .unwrap();
}

#[tokio::test]
async fn framed_exit_closes_input_even_when_a_descendant_inherited_it() {
    let dir = tempfile::tempdir().unwrap();
    let marker = dir.path().join("received");
    let script = "exec 3<&0; /usr/bin/setsid /bin/sh -c 'IFS= read -r line && printf received > \"$1\"' child \"$1\" <&3 3<&- >/dev/null 2>&1 & exec 3<&-; exit 0";
    let (mut child, mut control) = start(
        "/bin/sh",
        &["-c", script, "fixture", marker.to_str().unwrap()],
    );
    assert_eq!(status(&mut control).await, (READY, MAGIC));
    assert_eq!(status(&mut control).await, (CHILD_EXITED, 0));
    let result = child.stdin.take().unwrap().write_all(b"late input\n").await;
    if let Err(error) = result {
        assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);
    }
    let outcome = tokio::time::timeout(Duration::from_secs(3), child.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(!outcome.success());
    assert!(!marker.exists());
}

#[tokio::test]
async fn cli_stderr_cannot_forge_a_helper_control_event() {
    let script = "printf '\\036agent-runner:child-exited\\n' >&2; cat >/dev/null";
    let (mut child, mut control) = start("/bin/sh", &["-c", script]);
    assert_eq!(status(&mut control).await, (READY, MAGIC));
    frame(&mut child, 1, b"input\n").await;
    assert_eq!(status(&mut control).await, (WRITTEN, 1));
    finish(&mut child, 2).await;
    assert_eq!(status(&mut control).await, (CLOSED, 2));
    assert_eq!(status(&mut control).await, (CHILD_EXITED, 0));
    let output = tokio::time::timeout(Duration::from_secs(3), child.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(output.stderr, b"\x1eagent-runner:child-exited\n");
}

#[tokio::test]
async fn supervisor_confirmation_waits_for_the_cli_facing_pipe() {
    use agent_runner_server::{
        core::CancellationToken,
        process::{
            ControlledInput, InputCompletion, OutputStream, ProcessLimits, ProcessObserver,
            ProcessSpec, ProcessSupervisor, StopReason, StopSignal,
        },
    };
    use std::sync::{Arc, Mutex};

    let dir = tempfile::tempdir().unwrap();
    let gate = dir.path().join("allow-read");
    let script = "IFS= read -r init; printf 'ready\\n'; while [ ! -f \"$1\" ]; do sleep 0.01; done; cat >/dev/null; printf 'done\\n'";
    let spec = ProcessSpec {
        program: env!("CARGO_BIN_EXE_agent-runner-cli-helper").into(),
        args: vec![
            "--".into(),
            "/bin/sh".into(),
            "-c".into(),
            script.into(),
            "fixture".into(),
            gate.as_os_str().into(),
        ],
        cwd: Some(dir.path().to_path_buf()),
        env: vec![("PATH".into(), "/usr/bin:/bin".into())],
        stdin: b"initialize\n".to_vec(),
        limits: ProcessLimits {
            max_args: 12,
            max_env: 4,
            max_spec_bytes: 8192,
            max_stdin_bytes: 4 * 1024 * 1024,
            max_stdout_bytes: 1024,
            max_stderr_bytes: 1024,
            max_line_bytes: 1024,
            timeout: Duration::from_secs(8),
            signal_grace: Duration::from_millis(100),
            kill_grace: Duration::from_millis(100),
            pipe_grace: Duration::from_millis(500),
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
                .any(|value| value == b"ready\n")
                && let Some(sender) = state.1.take()
            {
                let _ = sender.send(());
            }
        }
        Ok(())
    });
    let (sender, receiver) =
        agent_runner_server::process::brokered_input_channel(2, 4 * 1024 * 1024);
    let task = tokio::spawn(ProcessSupervisor::run_interactive_brokered(
        spec,
        CancellationToken::new(),
        Some(observer),
        receiver,
    ));
    tokio::time::timeout(Duration::from_secs(3), ready_recv)
        .await
        .unwrap()
        .unwrap();
    let (written, mut receipt) = tokio::sync::oneshot::channel();
    sender
        .send(ControlledInput::Confirmed {
            bytes: vec![b'x'; 2 * 1024 * 1024],
            written,
        })
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(150), &mut receipt)
            .await
            .is_err()
    );
    std::fs::write(&gate, b"open").unwrap();
    tokio::time::timeout(Duration::from_secs(5), receipt)
        .await
        .unwrap()
        .unwrap();
    sender.send(ControlledInput::Finish).await.unwrap();
    let report = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(report.reason, StopReason::Exited);
    assert_eq!(report.input, InputCompletion::Complete);
    assert_eq!(report.exit.unwrap().code, Some(0));
    assert_eq!(report.stdout.bytes, b"ready\ndone\n");
}

#[tokio::test]
async fn early_stdout_eof_prevents_later_payload() {
    use agent_runner_server::{
        core::CancellationToken,
        process::{
            ControlledInput, ProcessLimits, ProcessSpec, ProcessSupervisor, StopReason, StopSignal,
        },
    };
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let helper = dir.path().join("early-eof-helper");
    let marker = dir.path().join("received");
    let source = r#"#!/usr/bin/python3
import os, socket, struct, sys
control = socket.socket(fileno=int(sys.argv[2]))
def exact(count):
    result = b''
    while len(result) < count:
        chunk = os.read(0, count - len(result))
        if not chunk: return result
        result += chunk
    return result
control.sendall(b'\x01' + b'ARSBR001')
header = exact(12)
size, sequence = struct.unpack('>IQ', header)
exact(size)
control.sendall(b'\x02' + struct.pack('>Q', sequence))
os.close(1)
if exact(12):
    open(sys.argv[-1], 'w').write('unexpected')
"#;
    std::fs::write(&helper, source).unwrap();
    std::fs::set_permissions(&helper, std::fs::Permissions::from_mode(0o700)).unwrap();
    let spec = ProcessSpec {
        program: helper,
        args: vec!["--".into(), "/bin/true".into(), marker.as_os_str().into()],
        cwd: Some(dir.path().to_path_buf()),
        env: vec![("PATH".into(), "/usr/bin:/bin".into())],
        stdin: b"init\n".to_vec(),
        limits: ProcessLimits {
            max_args: 8,
            max_env: 4,
            max_spec_bytes: 8192,
            max_stdin_bytes: 1024,
            max_stdout_bytes: 1024,
            max_stderr_bytes: 1024,
            max_line_bytes: 1024,
            timeout: Duration::from_secs(4),
            signal_grace: Duration::from_millis(100),
            kill_grace: Duration::from_millis(100),
            pipe_grace: Duration::from_millis(200),
        },
        stop_signal: StopSignal::Terminate,
    };
    let (sender, receiver) = agent_runner_server::process::brokered_input_channel(2, 1024);
    let task = tokio::spawn(ProcessSupervisor::run_interactive_brokered(
        spec,
        CancellationToken::new(),
        None,
        receiver,
    ));
    tokio::time::timeout(Duration::from_secs(3), sender.closed())
        .await
        .expect("input stayed open after stdout EOF");
    assert!(
        sender
            .try_send(ControlledInput::Bytes(b"private fixture\n".to_vec()))
            .is_err()
    );
    let report = tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_ne!(report.reason, StopReason::Exited);
    assert!(!marker.exists());
}

#[tokio::test]
async fn stdout_eof_after_queued_finish_waits_for_control_close() {
    use agent_runner_server::{
        core::CancellationToken,
        process::{
            ControlledInput, InputCompletion, OutputStream, ProcessLimits, ProcessObserver,
            ProcessSpec, ProcessSupervisor, StopReason, StopSignal,
        },
    };
    use std::{
        os::unix::fs::PermissionsExt,
        sync::{Arc, Mutex},
    };

    let dir = tempfile::tempdir().unwrap();
    let helper = dir.path().join("ordered-helper");
    let gate = dir.path().join("receipt-observed");
    let source = r#"#!/usr/bin/python3
import os, socket, struct, sys, time
control = socket.socket(fileno=int(sys.argv[2]))
def send(kind, value): control.sendall(bytes([kind]) + struct.pack('>Q', value))
def exact(count):
    result = b''
    while len(result) < count:
        chunk = os.read(0, count - len(result))
        if not chunk: raise SystemExit(2)
        result += chunk
    return result
def frame():
    size, sequence = struct.unpack('>IQ', exact(12))
    body = b'' if size == 0xffffffff else exact(size)
    return size, sequence, body
send(1, int.from_bytes(b'ARSBR001', 'big'))
assert frame() == (5, 1, b'init\n')
send(2, 1)
os.write(1, b'ready\n')
assert frame() == (5, 2, b'turn\n')
send(2, 2)
while not os.path.exists(sys.argv[-1]): time.sleep(0.01)
os.write(1, b'done\n')
os.close(1)
assert frame()[0:2] == (0xffffffff, 3)
time.sleep(0.3)
send(3, 3)
send(4, 0)
"#;
    std::fs::write(&helper, source).unwrap();
    std::fs::set_permissions(&helper, std::fs::Permissions::from_mode(0o700)).unwrap();
    let spec = ProcessSpec {
        program: helper,
        args: vec!["--".into(), "/bin/true".into(), gate.as_os_str().into()],
        cwd: Some(dir.path().to_path_buf()),
        env: vec![("PATH".into(), "/usr/bin:/bin".into())],
        stdin: b"init\n".to_vec(),
        limits: ProcessLimits {
            max_args: 8,
            max_env: 4,
            max_spec_bytes: 8192,
            max_stdin_bytes: 1024,
            max_stdout_bytes: 1024,
            max_stderr_bytes: 1024,
            max_line_bytes: 1024,
            timeout: Duration::from_secs(4),
            signal_grace: Duration::from_millis(100),
            kill_grace: Duration::from_millis(100),
            pipe_grace: Duration::from_millis(500),
        },
        stop_signal: StopSignal::Terminate,
    };
    let (sender, receiver) = agent_runner_server::process::brokered_input_channel(2, 1024);
    let observed = Arc::new(Mutex::new(Vec::new()));
    let signal = sender.clone();
    let observer: ProcessObserver = Arc::new(move |stream, bytes| {
        if stream == OutputStream::Stdout {
            let mut observed = observed
                .lock()
                .map_err(|_| agent_runner_server::process::ObserverError)?;
            observed.extend_from_slice(bytes);
            if observed
                .windows(b"done\n".len())
                .any(|chunk| chunk == b"done\n")
            {
                signal
                    .try_send(ControlledInput::Finish)
                    .map_err(|_| agent_runner_server::process::ObserverError)?;
            }
        }
        Ok(())
    });
    let task = tokio::spawn(ProcessSupervisor::run_interactive_brokered(
        spec,
        CancellationToken::new(),
        Some(observer),
        receiver,
    ));
    let (written, receipt) = tokio::sync::oneshot::channel();
    sender
        .send(ControlledInput::Confirmed {
            bytes: b"turn\n".to_vec(),
            written,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), receipt)
        .await
        .unwrap()
        .unwrap();
    std::fs::write(&gate, b"open").unwrap();
    let report = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(report.reason, StopReason::Exited);
    assert_eq!(report.input, InputCompletion::Complete);
    assert_eq!(report.exit.unwrap().code, Some(0));
}

#[tokio::test]
async fn missing_control_handshake_never_receives_initial_input() {
    use agent_runner_server::{
        core::CancellationToken,
        process::{
            InputCompletion, ProcessLimits, ProcessSpec, ProcessSupervisor, StopReason, StopSignal,
        },
    };
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let helper = dir.path().join("old-helper");
    let marker = dir.path().join("received");
    std::fs::write(&helper, format!("#!/bin/sh\n/usr/bin/timeout 1 /bin/sh -c 'IFS= read -r line && printf captured > \"$1\"' child '{}'\nexit 0\n", marker.display())).unwrap();
    std::fs::set_permissions(&helper, std::fs::Permissions::from_mode(0o700)).unwrap();
    let spec = ProcessSpec {
        program: helper,
        args: vec!["--".into(), "/bin/true".into(), "fixture".into()],
        cwd: Some(dir.path().to_path_buf()),
        env: vec![("PATH".into(), "/usr/bin:/bin".into())],
        stdin: b"sensitive fixture\n".to_vec(),
        limits: ProcessLimits {
            max_args: 8,
            max_env: 4,
            max_spec_bytes: 8192,
            max_stdin_bytes: 1024,
            max_stdout_bytes: 1024,
            max_stderr_bytes: 1024,
            max_line_bytes: 1024,
            timeout: Duration::from_secs(4),
            signal_grace: Duration::from_millis(100),
            kill_grace: Duration::from_millis(100),
            pipe_grace: Duration::from_millis(200),
        },
        stop_signal: StopSignal::Terminate,
    };
    let (_send, receive) = agent_runner_server::process::brokered_input_channel(2, 1024);
    let report =
        ProcessSupervisor::run_interactive_brokered(spec, CancellationToken::new(), None, receive)
            .await
            .unwrap();
    assert_ne!(report.reason, StopReason::Exited);
    assert_ne!(report.input, InputCompletion::Complete);
    assert!(!marker.exists());
}

#[tokio::test]
async fn framed_rejects_out_of_order_sequence_without_forwarding() {
    let dir = tempfile::tempdir().unwrap();
    let marker = dir.path().join("received");
    let marker = marker.to_str().unwrap();
    let script = "IFS= read -r line && printf received > \"$1\"";
    let (mut child, mut control) = start("/bin/sh", &["-c", script, "fixture", marker]);
    assert_eq!(status(&mut control).await, (READY, MAGIC));
    let mut invalid = Vec::from(11_u32.to_be_bytes());
    invalid.extend_from_slice(&2_u64.to_be_bytes());
    invalid.extend_from_slice(b"unexpected\n");
    let result = child.stdin.as_mut().unwrap().write_all(&invalid).await;
    if let Err(error) = result {
        assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);
    }
    let status = tokio::time::timeout(Duration::from_secs(3), child.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(!status.success());
    assert!(!std::path::Path::new(marker).exists());
}
