//! A single-invocation Linux child subreaper for supervised CLI execution.

#[cfg(target_os = "linux")]
fn main() {
    use std::{
        ffi::OsString,
        io::{self, Write},
        os::unix::process::ExitStatusExt,
        path::PathBuf,
        process::{Command, Stdio},
    };

    let mut args = std::env::args_os().skip(1);
    let first = args.next();
    if first.as_deref() == Some(std::ffi::OsStr::new("--version")) && args.next().is_none() {
        println!("agent-runner-cli-helper {}", env!("CARGO_PKG_VERSION"));
        return;
    }
    if first.as_deref() != Some(std::ffi::OsStr::new("--")) {
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
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(_) => std::process::exit(2),
    };
    // SAFETY: The CLI has inherited its own descriptor; the helper must not keep an unused stdin reader alive.
    unsafe { libc::close(libc::STDIN_FILENO) };
    let started_marker = io::stderr()
        .write_all(b"\x1eagent-runner:child-started\n")
        .is_ok();
    let result = if started_marker {
        child.wait()
    } else {
        Err(io::ErrorKind::BrokenPipe.into())
    };
    let status = match result {
        Ok(status) => Some(status),
        Err(_) => {
            let _ = child.kill();
            child.wait().ok()
        }
    };
    let exited_marker = status.is_some()
        && io::stderr()
            .write_all(b"\x1eagent-runner:child-exited\n")
            .is_ok();
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
    // Written only after waitpid reported no remaining child, so every descendant that stayed in this subreaper's tree has exited.
    // The backend treats it as cleanup evidence only together with this helper's own normal exit.
    if started_marker && exited_marker {
        let _ = io::stderr().write_all(b"\x1eagent-runner:descendants-reaped\n");
    }
    if !started_marker || !exited_marker {
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
