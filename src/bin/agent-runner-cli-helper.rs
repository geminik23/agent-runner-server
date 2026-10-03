//! A single-invocation Linux child subreaper for supervised CLI execution.

#[cfg(target_os = "linux")]
#[path = "agent-runner-cli-helper/broker.rs"]
mod broker;
#[cfg(target_os = "linux")]
#[path = "../process/broker_protocol.rs"]
mod broker_protocol;
#[cfg(target_os = "linux")]
#[path = "agent-runner-cli-helper/framed.rs"]
mod framed;
#[cfg(target_os = "linux")]
#[path = "agent-runner-cli-helper/gate.rs"]
mod gate;

#[cfg(target_os = "linux")]
fn main() {
    use std::{
        ffi::OsString,
        fs::File,
        io::{self, Write},
        os::{fd::FromRawFd, unix::process::ExitStatusExt},
        path::PathBuf,
        process::{Command, Stdio},
    };

    let mut args = std::env::args_os().skip(1);
    let first = args.next();
    if first.as_deref() == Some(std::ffi::OsStr::new("--version")) && args.next().is_none() {
        println!("agent-runner-cli-helper {}", env!("CARGO_PKG_VERSION"));
        return;
    }
    let framed_stdin = first.as_deref() == Some(std::ffi::OsStr::new("--framed-stdin"));
    let broker_stdin =
        framed_stdin || first.as_deref() == Some(std::ffi::OsStr::new("--broker-stdin"));
    let control = if framed_stdin {
        let fd = args
            .next()
            .and_then(|value| value.to_str().and_then(|value| value.parse::<i32>().ok()))
            .filter(|fd| *fd > 2)
            .unwrap_or_else(|| std::process::exit(2));
        // SAFETY: fcntl reads and sets descriptor flags without retaining pointers.
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0 {
            std::process::exit(2);
        }
        // SAFETY: The trusted parent passed this dedicated descriptor to the helper.
        Some(unsafe { File::from_raw_fd(fd) })
    } else {
        None
    };
    let separator = if broker_stdin { args.next() } else { first };
    if separator.as_deref() != Some(std::ffi::OsStr::new("--")) {
        std::process::exit(2);
    }
    let Some(program) = args.next().map(PathBuf::from) else {
        std::process::exit(2)
    };
    let arguments: Vec<OsString> = args.collect();
    if !program.is_absolute() || arguments.is_empty() {
        std::process::exit(2);
    }
    // SAFETY: prctl changes only this single-threaded helper's child reparenting policy.
    if unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) } != 0 {
        std::process::exit(2);
    }
    // SAFETY: The zeroed action is initialized with a default handler and an empty signal mask.
    let mut action: libc::sigaction = unsafe { std::mem::zeroed() };
    action.sa_sigaction = libc::SIG_DFL;
    // SAFETY: action owns a valid signal-set structure for both calls.
    if unsafe { libc::sigemptyset(&mut action.sa_mask) } != 0
        || unsafe { libc::sigaction(libc::SIGCHLD, &action, std::ptr::null_mut()) } != 0
    {
        std::process::exit(2);
    }
    let mut command = Command::new(program);
    command
        .args(arguments)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    let mut prepared = None;
    let mut child = if let Some(control) = control {
        let (state, reader) = match framed::prepare(control) {
            Ok(value) => value,
            Err(_) => std::process::exit(2),
        };
        command.stdin(Stdio::from(reader));
        let (child, pidfd) = match gate::spawn(command, || state.still_ready()) {
            Ok(value) => value,
            Err(_) => std::process::exit(2),
        };
        prepared = Some((state, pidfd));
        child
    } else {
        command.stdin(if broker_stdin {
            Stdio::piped()
        } else {
            Stdio::inherit()
        });
        match command.spawn() {
            Ok(child) => child,
            Err(_) => std::process::exit(2),
        }
    };
    if !broker_stdin {
        // SAFETY: The CLI has inherited its own descriptor; the helper must not keep an unused stdin reader alive.
        unsafe { libc::close(libc::STDIN_FILENO) };
    }
    let started_marker = framed_stdin
        || io::stderr()
            .write_all(b"\x1eagent-runner:child-started\n")
            .is_ok();
    if !started_marker && broker_stdin {
        // SAFETY: The broker has not taken ownership of this descriptor on the marker failure path.
        unsafe { libc::close(libc::STDIN_FILENO) };
    }
    let result = if !started_marker {
        Err(io::ErrorKind::BrokenPipe.into())
    } else if let Some((state, pidfd)) = prepared {
        framed::forward(&mut child, pidfd, state)
    } else if broker_stdin {
        broker::forward(&mut child)
    } else {
        child.wait().map(|status| (status, true))
    };
    let (status, input_complete) = match result {
        Ok((status, complete)) => (Some(status), complete),
        Err(_) => {
            let _ = child.kill();
            (child.wait().ok(), false)
        }
    };
    let exited_marker = status.is_some()
        && (framed_stdin
            || io::stderr()
                .write_all(b"\x1eagent-runner:child-exited\n")
                .is_ok());
    loop {
        let mut child_status = 0;
        // SAFETY: waitpid owns the status pointer and waits only for this helper's child processes.
        let result = unsafe { libc::waitpid(-1, &mut child_status, 0) };
        if result > 0 {
            continue;
        }
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::EINTR) {
            continue;
        }
        if error.raw_os_error() != Some(libc::ECHILD) {
            std::process::exit(2);
        }
        break;
    }
    if !started_marker || !exited_marker || !input_complete {
        std::process::exit(2);
    }
    let Some(status) = status else {
        std::process::exit(2);
    };
    if !status.success() {
        std::process::exit(
            status
                .code()
                .unwrap_or_else(|| 128 + status.signal().unwrap_or(1))
                .clamp(1, 255),
        );
    }
}

#[cfg(not(target_os = "linux"))]
fn main() {
    std::process::exit(2);
}
