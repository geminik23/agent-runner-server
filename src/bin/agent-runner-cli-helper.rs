//! A single-invocation Linux child subreaper for supervised CLI execution.
//!
//! `agent-runner-cli-helper [--parent PID --stop-grace-ms MS] -- PROGRAM ARGS...`
//!
//! The helper runs one CLI, reaps every descendant that stays in its subreaper tree and exits only after
//! `waitpid` reports no remaining child. Interrupt and terminate signals do not stop the helper itself: the
//! supervisor signals the whole process group, so the CLI receives them directly while the helper keeps
//! reaping and still reports its markers. With `--parent`, the helper also stops its process group when that
//! parent exits, escalating to a kill after the grace period because no supervisor remains to do it.

#[cfg(target_os = "linux")]
fn main() {
    use std::{
        ffi::OsString,
        io::{self, Write},
        os::{
            fd::{AsRawFd, FromRawFd, OwnedFd},
            unix::process::{CommandExt, ExitStatusExt},
        },
        path::PathBuf,
        process::{Command, ExitStatus, Stdio},
        time::{Duration, Instant},
    };

    let mut args = std::env::args_os().skip(1).peekable();
    if args.peek().map(OsString::as_os_str) == Some(std::ffi::OsStr::new("--version")) {
        args.next();
        if args.next().is_none() {
            println!("agent-runner-cli-helper {}", env!("CARGO_PKG_VERSION"));
            return;
        }
        std::process::exit(2);
    }
    let mut parent = None;
    let mut stop_grace = None;
    loop {
        let Some(flag) = args.next() else {
            std::process::exit(2)
        };
        let mut number = || {
            args.next()
                .and_then(|value| value.to_str().and_then(|value| value.parse::<u64>().ok()))
                .unwrap_or_else(|| std::process::exit(2))
        };
        match flag.to_str() {
            Some("--") => break,
            Some("--parent") if parent.is_none() => {
                parent = Some(
                    i32::try_from(number())
                        .ok()
                        .filter(|pid| *pid > 1)
                        .unwrap_or_else(|| std::process::exit(2)),
                )
            }
            Some("--stop-grace-ms") if stop_grace.is_none() => {
                stop_grace = Some(Duration::from_millis(number()))
            }
            _ => std::process::exit(2),
        }
    }
    // A parent watch is useless without a bounded escalation, and a grace without a parent is meaningless.
    let (parent, stop_grace) = match (parent, stop_grace) {
        (Some(parent), Some(grace)) if !grace.is_zero() => (Some(parent), grace),
        (None, None) => (None, Duration::ZERO),
        _ => std::process::exit(2),
    };
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
    // An inherited ignored SIGCHLD would let the kernel reap children before waitpid can observe them.
    // SAFETY: The zeroed action is initialized with a default handler and an empty signal mask.
    let mut action: libc::sigaction = unsafe { std::mem::zeroed() };
    action.sa_sigaction = libc::SIG_DFL;
    // SAFETY: action owns a valid signal-set structure for both calls.
    if unsafe { libc::sigemptyset(&mut action.sa_mask) } != 0
        || unsafe { libc::sigaction(libc::SIGCHLD, &action, std::ptr::null_mut()) } != 0
    {
        std::process::exit(2);
    }
    // Signals arrive through a signalfd so that the helper decides how to react instead of being stopped.
    // SAFETY: The zeroed set is initialized by sigemptyset before use and owned by this frame.
    let mut handled: libc::sigset_t = unsafe { std::mem::zeroed() };
    // SAFETY: handled is a valid signal set for every call below.
    let blocked = unsafe {
        libc::sigemptyset(&mut handled) == 0
            && libc::sigaddset(&mut handled, libc::SIGCHLD) == 0
            && libc::sigaddset(&mut handled, libc::SIGINT) == 0
            && libc::sigaddset(&mut handled, libc::SIGTERM) == 0
            && libc::sigprocmask(libc::SIG_BLOCK, &handled, std::ptr::null_mut()) == 0
    };
    if !blocked {
        std::process::exit(2);
    }
    // SAFETY: signalfd creates a new descriptor for the blocked set; ownership moves into OwnedFd.
    let signals = unsafe { libc::signalfd(-1, &handled, libc::SFD_CLOEXEC | libc::SFD_NONBLOCK) };
    if signals < 0 {
        std::process::exit(2);
    }
    // SAFETY: signals is a fresh descriptor owned only here.
    let signals = unsafe { OwnedFd::from_raw_fd(signals) };
    // Opening the pidfd before checking the parent pins the process: if it is still our parent afterwards,
    // the descriptor refers to it and not to a process that reused its ID.
    let parent_fd = match parent {
        Some(pid) => {
            // SAFETY: pidfd_open takes a pid and flags and returns a new descriptor or -1.
            let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
            if fd < 0 {
                std::process::exit(2);
            }
            // SAFETY: fd is a fresh descriptor owned only here; pidfds are close-on-exec.
            let fd = unsafe { OwnedFd::from_raw_fd(fd as i32) };
            // SAFETY: getppid has no preconditions.
            if unsafe { libc::getppid() } != pid {
                std::process::exit(2);
            }
            // Stopping after the parent exits signals the helper's whole process group, which must therefore be
            // the group the supervisor created for this run and nothing else.
            // SAFETY: getpgrp and getpid have no preconditions.
            if unsafe { libc::getpgrp() != libc::getpid() } {
                std::process::exit(2);
            }
            Some(fd)
        }
        None => None,
    };
    let mut command = Command::new(program);
    command
        .args(arguments)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    // SAFETY: sigprocmask is async-signal-safe and only restores the default mask in the forked child.
    unsafe {
        command.pre_exec(|| {
            let mut empty: libc::sigset_t = std::mem::zeroed();
            if libc::sigemptyset(&mut empty) != 0
                || libc::sigprocmask(libc::SIG_SETMASK, &empty, std::ptr::null_mut()) != 0
            {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let child = match command.spawn() {
        Ok(child) => child,
        Err(_) => std::process::exit(2),
    };
    let cli = child.id() as i32;
    // The CLI owns its process now; reaping goes through waitpid so that every descendant is collected the same way.
    drop(child);
    // SAFETY: The CLI has inherited its own descriptor; the helper must not keep an unused stdin reader alive.
    unsafe { libc::close(libc::STDIN_FILENO) };
    let started_marker = io::stderr()
        .write_all(b"\x1eagent-runner:child-started\n")
        .is_ok();
    if !started_marker {
        // SAFETY: kill only signals the CLI that this helper started and has not reaped.
        unsafe { libc::kill(cli, libc::SIGKILL) };
    }
    let mut status: Option<ExitStatus> = None;
    let mut exited_marker = false;
    let mut kill_at: Option<Instant> = None;
    let mut parent_fd = parent_fd;
    loop {
        // Collect every exited child; ECHILD after the CLI was reaped means nothing from the tree remains.
        let mut finished = false;
        loop {
            let mut raw = 0;
            // SAFETY: waitpid owns the status pointer and waits only for this helper's child processes.
            let reaped = unsafe { libc::waitpid(-1, &mut raw, libc::WNOHANG) };
            if reaped > 0 {
                if reaped == cli {
                    status = Some(ExitStatus::from_raw(raw));
                    exited_marker = started_marker
                        && io::stderr()
                            .write_all(b"\x1eagent-runner:child-exited\n")
                            .is_ok();
                }
                continue;
            }
            if reaped == 0 {
                break;
            }
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            if error.raw_os_error() != Some(libc::ECHILD) || status.is_none() {
                std::process::exit(2);
            }
            finished = true;
            break;
        }
        if finished {
            break;
        }
        let timeout = match kill_at {
            Some(deadline) => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    // No supervisor is left to escalate; this also ends the helper, which is in the same group.
                    // SAFETY: kill(0, ...) signals this helper's own process group.
                    unsafe { libc::kill(0, libc::SIGKILL) };
                    std::process::exit(2);
                }
                i32::try_from(remaining.as_millis().max(1)).unwrap_or(i32::MAX)
            }
            None => -1,
        };
        let mut polled = [
            libc::pollfd {
                fd: signals.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: parent_fd.as_ref().map_or(-1, AsRawFd::as_raw_fd),
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        // SAFETY: polled is a valid array of two pollfd entries; a negative fd is ignored by poll.
        let ready = unsafe { libc::poll(polled.as_mut_ptr(), 2, timeout) };
        if ready < 0 {
            if io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            std::process::exit(2);
        }
        if polled[0].revents != 0 {
            // Drain the queued signals. SIGCHLD leads to the reaping loop above; interrupt and terminate are
            // already delivered to the CLI through the group, so the helper only keeps waiting for it.
            loop {
                // SAFETY: info is a correctly sized buffer for one signalfd record.
                let mut info: libc::signalfd_siginfo = unsafe { std::mem::zeroed() };
                // SAFETY: read writes at most size_of::<signalfd_siginfo>() bytes into info.
                let read = unsafe {
                    libc::read(
                        signals.as_raw_fd(),
                        (&mut info as *mut libc::signalfd_siginfo).cast(),
                        std::mem::size_of::<libc::signalfd_siginfo>(),
                    )
                };
                if read <= 0 {
                    break;
                }
            }
        }
        if polled[1].revents != 0 {
            // The daemon is gone: ask the whole group to stop once and kill it after the grace period.
            parent_fd = None;
            // SAFETY: kill(0, ...) signals this helper's own process group; the helper blocks SIGTERM itself.
            unsafe { libc::kill(0, libc::SIGTERM) };
            kill_at = Some(Instant::now() + stop_grace);
        }
    }
    // Written only after waitpid reported no remaining child, so every descendant that stayed in this subreaper's tree has exited.
    // The backend treats it as cleanup evidence only together with this helper's own exit.
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
