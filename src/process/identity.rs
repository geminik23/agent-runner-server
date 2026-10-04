//! Linux process identity and process-group signalling for owned children.

use std::{
    io,
    net::Shutdown,
    os::{
        fd::{AsFd, AsRawFd, OwnedFd},
        unix::{net::UnixStream as StdUnixStream, process::CommandExt},
    },
    sync::{Arc, Mutex, OnceLock, mpsc as std_mpsc},
    time::Duration,
};

use rustix::{
    process::{
        Pid, PidfdFlags, Signal, WaitId, WaitIdOptions, pidfd_open, pidfd_send_signal, waitid,
    },
    rand::{GetRandomFlags, getrandom},
};
use tokio::{
    net::UnixStream,
    process::{Child, Command},
    sync::{Semaphore, SemaphorePermit},
    task::JoinHandle,
    time::Instant,
};
use tokio_util::sync::CancellationToken;

use super::ProcessError;

const CHALLENGE: u8 = 0x71;
const GO: u8 = 0x36;
const PIDFD_SIGNAL_PROCESS_GROUP: libc::c_uint = 1 << 2;
const MAX_STARTING_PROCESSES: usize = 32;
const MAX_SUPERVISED_PROCESSES: usize = 32;
static START_SLOTS: Semaphore = Semaphore::const_new(MAX_STARTING_PROCESSES);
static CLEANUP_SLOTS: Semaphore = Semaphore::const_new(MAX_SUPERVISED_PROCESSES);
static REAPER: Mutex<Option<std_mpsc::SyncSender<ReapJob>>> = Mutex::new(None);

struct ReapJob {
    fd: OwnedFd,
    _permit: SemaphorePermit<'static>,
}

fn reaper() -> Result<std_mpsc::SyncSender<ReapJob>, io::ErrorKind> {
    get_or_start_reaper(&REAPER, || {
        let (sender, receiver) = std_mpsc::sync_channel(MAX_SUPERVISED_PROCESSES);
        std::thread::Builder::new()
            .name("agent-runner-reaper".into())
            .spawn(move || reap_loop(receiver))
            .map_err(|error| error.kind())?;
        Ok(sender)
    })
}

fn get_or_start_reaper(
    slot: &Mutex<Option<std_mpsc::SyncSender<ReapJob>>>,
    start: impl FnOnce() -> Result<std_mpsc::SyncSender<ReapJob>, io::ErrorKind>,
) -> Result<std_mpsc::SyncSender<ReapJob>, io::ErrorKind> {
    let mut current = slot.lock().unwrap_or_else(|error| error.into_inner());
    if let Some(sender) = current.as_ref() {
        return Ok(sender.clone());
    }
    let sender = start()?;
    *current = Some(sender.clone());
    Ok(sender)
}

fn reap_loop(receiver: std_mpsc::Receiver<ReapJob>) {
    let mut active = Vec::with_capacity(MAX_SUPERVISED_PROCESSES);
    loop {
        if active.is_empty() {
            match receiver.recv() {
                Ok(job) => active.push(job),
                Err(_) => return,
            }
        }
        while let Ok(job) = receiver.try_recv() {
            active.push(job);
        }
        let mut index = 0;
        while index < active.len() {
            if reap_ready(&active[index].fd) {
                active.swap_remove(index);
            } else {
                index += 1;
            }
        }
        if !active.is_empty() {
            match receiver.recv_timeout(Duration::from_millis(20)) {
                Ok(job) => active.push(job),
                Err(std_mpsc::RecvTimeoutError::Timeout) => {}
                Err(std_mpsc::RecvTimeoutError::Disconnected) => return,
            }
        }
    }
}

type QueuedLaunch = (Command, SemaphorePermit<'static>);

struct CancelQueued(Arc<Mutex<Option<QueuedLaunch>>>);
impl Drop for CancelQueued {
    fn drop(&mut self) {
        let mut queued = self.0.lock().unwrap_or_else(|error| error.into_inner());
        queued.take();
    }
}

pub(super) struct OwnedGroup {
    fd: Option<OwnedFd>,
    #[cfg(test)]
    pid: u32,
    cleanup_permit: Option<SemaphorePermit<'static>>,
    armed: bool,
}

impl OwnedGroup {
    fn open(pid: u32) -> Result<Self, io::ErrorKind> {
        let pid = i32::try_from(pid)
            .ok()
            .filter(|id| *id > 1)
            .and_then(Pid::from_raw)
            .ok_or(io::ErrorKind::InvalidInput)?;
        let fd = pidfd_open(pid, PidfdFlags::empty()).map_err(error_kind)?;
        let status = waitid(
            WaitId::PidFd(fd.as_fd()),
            WaitIdOptions::EXITED | WaitIdOptions::NOHANG | WaitIdOptions::NOWAIT,
        )
        .map_err(error_kind)?;
        if status.is_some() {
            return Err(io::ErrorKind::NotFound);
        }
        Ok(Self {
            fd: Some(fd),
            #[cfg(test)]
            pid: pid.as_raw_pid() as u32,
            cleanup_permit: None,
            armed: false,
        })
    }

    fn group_signal_raw(&self, signal: i32) -> Result<(), io::ErrorKind> {
        let fd = self.fd.as_ref().ok_or(io::ErrorKind::NotFound)?;
        // SAFETY: The pidfd stays owned for the syscall, the signal is a valid signal or zero, and Linux accepts a null siginfo pointer.
        let result = unsafe {
            libc::syscall(
                libc::SYS_pidfd_send_signal,
                fd.as_raw_fd(),
                signal,
                std::ptr::null::<libc::siginfo_t>(),
                PIDFD_SIGNAL_PROCESS_GROUP,
            )
        };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error().kind())
        }
    }

    fn probe(&self) -> Result<(), io::ErrorKind> {
        self.group_signal_raw(0)
    }

    pub(super) fn signal(&self, signal: Signal) -> Result<(), io::ErrorKind> {
        match self.group_signal_raw(signal.as_raw()) {
            Ok(()) => Ok(()),
            Err(group_error) => {
                if let Some(fd) = &self.fd {
                    let _ = pidfd_send_signal(fd, signal);
                }
                Err(group_error)
            }
        }
    }

    async fn reap_after_force(&self, grace: Duration) -> bool {
        let Some(fd) = &self.fd else { return false };
        tokio::time::timeout(grace, reap_owned(fd))
            .await
            .unwrap_or(false)
    }

    pub(super) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for OwnedGroup {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.signal(Signal::KILL);
            if let (Some(fd), Some(permit)) = (self.fd.take(), self.cleanup_permit.take()) {
                let job = ReapJob {
                    fd,
                    _permit: permit,
                };
                if let Ok(sender) = reaper() {
                    if let Err(error) = sender.try_send(job) {
                        tracing::error!("process reaper is unavailable; capacity remains reserved");
                        let job = match error {
                            std_mpsc::TrySendError::Full(job)
                            | std_mpsc::TrySendError::Disconnected(job) => job,
                        };
                        std::mem::forget(job);
                    }
                } else {
                    tracing::error!("process reaper could not start; capacity remains reserved");
                    std::mem::forget(job);
                }
            }
        }
    }
}

fn reap_ready(fd: &OwnedFd) -> bool {
    match waitid(
        WaitId::PidFd(fd.as_fd()),
        WaitIdOptions::EXITED | WaitIdOptions::NOHANG,
    ) {
        Ok(Some(_)) => true,
        Err(error) if error == rustix::io::Errno::CHILD => true,
        _ => false,
    }
}

async fn reap_owned(fd: &OwnedFd) -> bool {
    loop {
        if reap_ready(fd) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn error_kind(error: rustix::io::Errno) -> io::ErrorKind {
    io::Error::from_raw_os_error(error.raw_os_error()).kind()
}

fn child_write(fd: libc::c_int, bytes: &[u8]) -> io::Result<()> {
    let mut remaining = bytes;
    while !remaining.is_empty() {
        // SAFETY: The slice is valid for the duration of write and fd is inherited by the child.
        let count = unsafe { libc::write(fd, remaining.as_ptr().cast(), remaining.len()) };
        if count < 0 {
            return Err(io::Error::last_os_error());
        }
        if count == 0 {
            return Err(io::Error::from_raw_os_error(libc::EPIPE));
        }
        remaining = &remaining[count as usize..];
    }
    Ok(())
}

fn child_read(fd: libc::c_int, expected: u8) -> io::Result<()> {
    let mut byte = 0_u8;
    // SAFETY: byte is a valid one-byte output buffer and fd is inherited by the child.
    let count = unsafe { libc::read(fd, (&mut byte as *mut u8).cast(), 1) };
    if count == 1 {
        if byte == expected {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(libc::EINVAL))
        }
    } else if count == 0 {
        Err(io::Error::from_raw_os_error(libc::EPIPE))
    } else {
        Err(io::Error::last_os_error())
    }
}

async fn receive(stream: &UnixStream, mut bytes: &mut [u8]) -> io::Result<()> {
    while !bytes.is_empty() {
        stream.readable().await?;
        match stream.try_read(bytes) {
            Ok(0) => return Err(io::Error::from(io::ErrorKind::UnexpectedEof)),
            Ok(count) => bytes = &mut bytes[count..],
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

async fn send(stream: &UnixStream, mut bytes: &[u8]) -> io::Result<()> {
    while !bytes.is_empty() {
        stream.writable().await?;
        match stream.try_write(bytes) {
            Ok(0) => return Err(io::Error::from(io::ErrorKind::WriteZero)),
            Ok(count) => bytes = &bytes[count..],
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn abandon(parent: &StdUnixStream, actor: &UnixStream, task: JoinHandle<io::Result<Child>>) {
    task.abort();
    if parent.shutdown(Shutdown::Write).is_err() {
        let _ = actor.try_write(&[0]);
    }
    tokio::spawn(async move {
        let _ = task.await;
    });
}

/// Identifies one parent descriptor that the forked child should close before the handshake.
#[derive(Clone, Copy)]
struct InheritedDescriptor {
    fd: libc::c_int,
    device: u64,
    inode: u64,
}

/// Lists the parent's open descriptors above stdio except the handshake socket.
/// A forked child holds copies of all of them until exec, which can keep other runs' pipes open while the handshake waits.
/// An unreadable descriptor table yields an empty list, which leaves exec to close the copies as before.
fn inherited_descriptors(keep: libc::c_int) -> Box<[InheritedDescriptor]> {
    let Ok(entries) = std::fs::read_dir("/proc/self/fd") else {
        return Box::new([]);
    };
    entries
        .filter_map(|entry| {
            entry
                .ok()?
                .file_name()
                .to_str()?
                .parse::<libc::c_int>()
                .ok()
        })
        .filter(|fd| *fd > 2 && *fd != keep)
        .filter_map(|fd| {
            let mut status = std::mem::MaybeUninit::<libc::stat>::zeroed();
            // SAFETY: fstat writes only into the provided buffer and reports closed descriptors as errors.
            if unsafe { libc::fstat(fd, status.as_mut_ptr()) } != 0 {
                return None;
            }
            // SAFETY: A successful fstat initialized the buffer.
            let status = unsafe { status.assume_init() };
            Some(InheritedDescriptor {
                fd,
                device: status.st_dev,
                inode: status.st_ino,
            })
        })
        .collect()
}

/// Closes the recorded descriptors in the forked child before the handshake.
/// Only descriptors that still refer to the recorded file and are close-on-exec are closed, so exec would have closed them anyway.
/// Descriptors opened after the list was taken, and descriptors deliberately inherited across exec, stay open.
/// This runs between fork and exec and therefore uses only async-signal-safe calls without allocation.
fn close_inherited_copies(descriptors: &[InheritedDescriptor]) {
    for descriptor in descriptors {
        // SAFETY: fcntl only reads descriptor flags of this process.
        let flags = unsafe { libc::fcntl(descriptor.fd, libc::F_GETFD) };
        if flags < 0 || flags & libc::FD_CLOEXEC == 0 {
            continue;
        }
        let mut status = std::mem::MaybeUninit::<libc::stat>::zeroed();
        // SAFETY: fstat writes only into the stack buffer.
        if unsafe { libc::fstat(descriptor.fd, status.as_mut_ptr()) } != 0 {
            continue;
        }
        // SAFETY: A successful fstat initialized the buffer.
        let status = unsafe { status.assume_init() };
        if status.st_dev == descriptor.device && status.st_ino == descriptor.inode {
            // SAFETY: The descriptor is a close-on-exec copy owned by this forked child.
            unsafe { libc::close(descriptor.fd) };
        }
    }
}

fn outside_stdio(stream: StdUnixStream) -> Result<StdUnixStream, ProcessError> {
    if stream.as_raw_fd() > 2 {
        return Ok(stream);
    }
    let fd = rustix::io::fcntl_dupfd_cloexec(&stream, 3)
        .map_err(|error| ProcessError::Setup(error_kind(error)))?;
    Ok(StdUnixStream::from(fd))
}

fn nonce() -> Result<[u8; 32], ProcessError> {
    let mut value = [0_u8; 32];
    let mut filled = 0;
    while filled < value.len() {
        let count = getrandom(&mut value[filled..], GetRandomFlags::NONBLOCK)
            .map_err(|error| ProcessError::Setup(error_kind(error)))?;
        if count == 0 {
            return Err(ProcessError::Setup(io::ErrorKind::UnexpectedEof));
        }
        filled += count;
    }
    Ok(value)
}

#[derive(Clone, Copy)]
struct LaunchPolicy {
    probe: fn(&OwnedGroup) -> Result<(), io::ErrorKind>,
    spawn: fn(&mut Command) -> io::Result<Child>,
}

pub(super) async fn spawn(
    command: Command,
    cancellation: &CancellationToken,
    dropped: &CancellationToken,
    deadline: Instant,
    timeout: Duration,
    cleanup_grace: Duration,
) -> Result<(Child, OwnedGroup, u32), ProcessError> {
    spawn_checked(
        command,
        cancellation,
        dropped,
        deadline,
        timeout,
        cleanup_grace,
        LaunchPolicy {
            probe: OwnedGroup::probe,
            spawn: Command::spawn,
        },
    )
    .await
}

async fn spawn_checked(
    mut command: Command,
    cancellation: &CancellationToken,
    dropped: &CancellationToken,
    deadline: Instant,
    timeout: Duration,
    cleanup_grace: Duration,
    policy: LaunchPolicy,
) -> Result<(Child, OwnedGroup, u32), ProcessError> {
    if cancellation.is_cancelled() || dropped.is_cancelled() {
        return Err(ProcessError::CancelledBeforeSpawn);
    }
    if Instant::now() >= deadline {
        return Err(ProcessError::DeadlineBeforeSpawn);
    }
    let _ = reaper().map_err(ProcessError::Setup)?;
    let permit = tokio::select! {
        biased;
        _ = cancellation.cancelled() => return Err(ProcessError::CancelledBeforeSpawn),
        _ = dropped.cancelled() => return Err(ProcessError::CancelledBeforeSpawn),
        _ = tokio::time::sleep_until(deadline) => return Err(ProcessError::DeadlineBeforeSpawn),
        permit = START_SLOTS.acquire() => permit.map_err(|_|ProcessError::SupervisorFailed)?,
    };
    let cleanup_permit = tokio::select! {
        biased;
        _ = cancellation.cancelled() => return Err(ProcessError::CancelledBeforeSpawn),
        _ = dropped.cancelled() => return Err(ProcessError::CancelledBeforeSpawn),
        _ = tokio::time::sleep_until(deadline) => return Err(ProcessError::DeadlineBeforeSpawn),
        permit = CLEANUP_SLOTS.acquire() => permit.map_err(|_|ProcessError::SupervisorFailed)?,
    };
    let secret = nonce()?;
    let (parent, peer) = StdUnixStream::pair().map_err(|e| ProcessError::Setup(e.kind()))?;
    let parent = outside_stdio(parent)?;
    let peer = outside_stdio(peer)?;
    let handshake_timeout = Some(timeout.min(Duration::from_secs(30)));
    peer.set_read_timeout(handshake_timeout)
        .map_err(|e| ProcessError::Setup(e.kind()))?;
    peer.set_write_timeout(handshake_timeout)
        .map_err(|e| ProcessError::Setup(e.kind()))?;
    let actor = outside_stdio(
        parent
            .try_clone()
            .map_err(|e| ProcessError::Setup(e.kind()))?,
    )?;
    actor
        .set_nonblocking(true)
        .map_err(|e| ProcessError::Setup(e.kind()))?;
    let parent = Arc::new(parent);
    let actor = Arc::new(UnixStream::from_std(actor).map_err(|e| ProcessError::Setup(e.kind()))?);
    let peer = Arc::new(peer);
    let keep_parent = Arc::clone(&parent);
    let keep_actor = Arc::clone(&actor);
    let keep_peer = Arc::clone(&peer);
    let peer_fd = peer.as_raw_fd();
    let inherited = Arc::new(OnceLock::<Box<[InheritedDescriptor]>>::new());
    let close_inherited = Arc::clone(&inherited);
    // SAFETY: The child callback uses only async-signal-safe syscalls and stack data before exec.
    // It is registered after the caller's callbacks, so the pinned directory change has already happened.
    unsafe {
        command.as_std_mut().pre_exec(move || {
            let parent_fd = keep_parent.as_raw_fd();
            let actor_fd = keep_actor.as_raw_fd();
            let child_fd = keep_peer.as_raw_fd();
            libc::close(parent_fd);
            libc::close(actor_fd);
            if let Some(descriptors) = close_inherited.get() {
                close_inherited_copies(descriptors);
            }
            let pid = libc::getpid().to_ne_bytes();
            child_write(child_fd, &pid)?;
            child_write(child_fd, &secret)?;
            child_read(child_fd, CHALLENGE)?;
            child_write(child_fd, &secret)?;
            child_read(child_fd, GO)
        });
    }
    drop(peer);
    let queued = Arc::new(Mutex::new(Some((command, permit))));
    let _cancel_queued = CancelQueued(Arc::clone(&queued));
    let mut task = tokio::task::spawn_blocking(move || {
        let job = queued
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        let Some((mut command, _permit)) = job else {
            return Err(io::Error::from(io::ErrorKind::Interrupted));
        };
        // Recording on the spawning thread keeps descriptors that the standard library opens for this spawn out of the list.
        let _ = inherited.set(inherited_descriptors(peer_fd));
        (policy.spawn)(&mut command)
    });
    let mut initial = [0_u8; 36];
    let received = tokio::select! {
        biased;
        _ = cancellation.cancelled() => Err(ProcessError::CancelledBeforeSpawn),
        _ = dropped.cancelled() => Err(ProcessError::CancelledBeforeSpawn),
        _ = tokio::time::sleep_until(deadline) => Err(ProcessError::DeadlineBeforeSpawn),
        result = &mut task => match result {
            Ok(Err(error)) => return Err(ProcessError::Spawn(error.kind())),
            Ok(Ok(child)) => return Err(ProcessError::StartedUnsettled { pid:child.id().unwrap_or(0),kind:io::ErrorKind::InvalidData }),
            Err(_) => return Err(ProcessError::SupervisorFailed),
        },
        result = receive(&actor, &mut initial) => result.map_err(|e|ProcessError::Setup(e.kind())),
    };
    if let Err(error) = received {
        abandon(&parent, &actor, task);
        return Err(error);
    }
    let raw = i32::from_ne_bytes([initial[0], initial[1], initial[2], initial[3]]);
    let pid = match u32::try_from(raw) {
        Ok(pid) => pid,
        Err(_) => {
            abandon(&parent, &actor, task);
            return Err(ProcessError::IdentityUnavailable(
                io::ErrorKind::InvalidData,
            ));
        }
    };
    if initial[4..] != secret {
        abandon(&parent, &actor, task);
        return Err(ProcessError::IdentityUnavailable(
            io::ErrorKind::InvalidData,
        ));
    }
    let mut group = match OwnedGroup::open(pid) {
        Ok(group) => group,
        Err(kind) => {
            abandon(&parent, &actor, task);
            return Err(ProcessError::IdentityUnavailable(kind));
        }
    };
    group.cleanup_permit = Some(cleanup_permit);
    let mut ack = [0_u8; 32];
    let verified = tokio::select! {
        biased;
        _ = cancellation.cancelled() => Err(ProcessError::CancelledBeforeSpawn),
        _ = dropped.cancelled() => Err(ProcessError::CancelledBeforeSpawn),
        _ = tokio::time::sleep_until(deadline) => Err(ProcessError::DeadlineBeforeSpawn),
        result = &mut task => match result {
            Ok(Err(error)) => return Err(ProcessError::Spawn(error.kind())),
            Ok(Ok(child)) => return Err(ProcessError::StartedUnsettled{pid:child.id().unwrap_or(0),kind:io::ErrorKind::InvalidData}),
            Err(_) => return Err(ProcessError::SupervisorFailed),
        },
        result = async {send(&actor,&[CHALLENGE]).await?; receive(&actor,&mut ack).await} => result.map_err(|e|ProcessError::Setup(e.kind())),
    };
    if let Err(error) = verified {
        abandon(&parent, &actor, task);
        return Err(error);
    }
    if ack != secret {
        abandon(&parent, &actor, task);
        return Err(ProcessError::IdentityUnavailable(
            io::ErrorKind::InvalidData,
        ));
    }
    group.armed = true;
    if let Err(kind) = (policy.probe)(&group) {
        abandon(&parent, &actor, task);
        return Err(ProcessError::IdentityUnavailable(kind));
    }
    let released = tokio::select! {
        biased;
        _ = cancellation.cancelled() => Err(io::ErrorKind::Interrupted),
        _ = dropped.cancelled() => Err(io::ErrorKind::Interrupted),
        _ = tokio::time::sleep_until(deadline) => Err(io::ErrorKind::TimedOut),
        result = send(&actor,&[GO]) => result.map_err(|e|e.kind()),
    };
    if let Err(kind) = released {
        abandon(&parent, &actor, task);
        return Err(ProcessError::StartedUnsettled { pid, kind });
    }
    let result = tokio::select! {
        biased;
        result = &mut task => Ok(result),
        _ = cancellation.cancelled() => Err(io::ErrorKind::Interrupted),
        _ = dropped.cancelled() => Err(io::ErrorKind::Interrupted),
        _ = tokio::time::sleep_until(deadline) => Err(io::ErrorKind::TimedOut),
    };
    let result = match result {
        Ok(result) => result,
        Err(kind) => {
            let _ = group.signal(Signal::KILL);
            match tokio::time::timeout(cleanup_grace, &mut task).await {
                Ok(result) => result,
                Err(_) => {
                    abandon(&parent, &actor, task);
                    return Err(ProcessError::StartedUnsettled { pid, kind });
                }
            }
        }
    };
    let child = match result {
        Ok(Ok(child)) => child,
        Ok(Err(error)) => {
            let _ = group.signal(Signal::KILL);
            if group.reap_after_force(cleanup_grace).await {
                group.disarm();
            }
            return Err(ProcessError::StartedUnsettled {
                pid,
                kind: error.kind(),
            });
        }
        Err(_) => return Err(ProcessError::SupervisorFailed),
    };
    if child.id() != Some(pid) {
        return Err(ProcessError::StartedUnsettled {
            pid,
            kind: io::ErrorKind::InvalidData,
        });
    }
    Ok((child, group, pid))
}

#[cfg(test)]
mod tests {
    use super::{
        CHALLENGE, CLEANUP_SLOTS, LaunchPolicy, OwnedGroup, ReapJob, START_SLOTS, child_read,
        get_or_start_reaper, spawn_checked,
    };
    use crate::process::ProcessError;
    use std::{
        io,
        os::unix::process::CommandExt,
        process::Stdio,
        sync::atomic::{AtomicU32, Ordering},
        time::Duration,
    };
    use tokio::{process::Command, time::Instant};
    use tokio_util::sync::CancellationToken;

    static PROBE_PID: AtomicU32 = AtomicU32::new(0);
    static HANDSHAKE_DESCRIPTORS: std::sync::Mutex<Option<Vec<i32>>> = std::sync::Mutex::new(None);
    static FAILURE_PID: AtomicU32 = AtomicU32::new(0);
    static SLOT_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    #[test]
    fn marker_fixture() {
        if let Some(path) = std::env::var_os("AGENT_RUNNER_IDENTITY_MARKER") {
            std::fs::write(path, "executed").unwrap();
            if std::env::var_os("AGENT_RUNNER_IDENTITY_SLEEP").is_some() {
                std::thread::sleep(Duration::from_secs(30));
            }
        }
    }

    #[test]
    fn transient_reaper_creation_error_can_be_retried() {
        let slot = std::sync::Mutex::<Option<std::sync::mpsc::SyncSender<ReapJob>>>::new(None);
        assert!(matches!(
            get_or_start_reaper(&slot, || Err(io::ErrorKind::WouldBlock)),
            Err(io::ErrorKind::WouldBlock)
        ));
        let (sender, _receiver) = std::sync::mpsc::sync_channel(1);
        assert!(get_or_start_reaper(&slot, || Ok(sender)).is_ok());
        assert!(get_or_start_reaper(&slot, || panic!("must reuse existing reaper")).is_ok());
    }

    #[test]
    fn nonchild_identity_is_never_accepted_for_signalling() {
        assert!(OwnedGroup::open(0).is_err());
        assert!(OwnedGroup::open(std::process::id()).is_err());
    }

    #[tokio::test]
    async fn stalled_handshake_read_is_bounded_even_without_socket_shutdown() {
        use std::os::fd::AsRawFd;
        let (_parent, peer) = std::os::unix::net::UnixStream::pair().unwrap();
        peer.set_read_timeout(Some(Duration::from_millis(50)))
            .unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            tokio::task::spawn_blocking(move || child_read(peer.as_raw_fd(), CHALLENGE)),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            matches!(result,Err(error) if matches!(error.kind(),io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut))
        );
    }

    #[tokio::test]
    async fn preexec_child_drops_inherited_close_on_exec_copies_before_the_handshake() {
        use std::os::fd::AsRawFd;
        // The probe runs while the child still waits for GO, so it sees the descriptor table the handshake keeps.
        fn observe(group: &OwnedGroup) -> Result<(), io::ErrorKind> {
            let descriptors = std::fs::read_dir(format!("/proc/{}/fd", group.pid))
                .unwrap()
                .filter_map(|entry| entry.ok()?.file_name().to_str()?.parse().ok())
                .collect();
            *HANDSHAKE_DESCRIPTORS.lock().unwrap() = Some(descriptors);
            Err(io::ErrorKind::Unsupported)
        }
        let (held_read, held_write) = std::os::unix::net::UnixStream::pair().unwrap();
        let held = [held_read.as_raw_fd(), held_write.as_raw_fd()];
        // SAFETY: open receives a valid NUL-terminated path; omitting O_CLOEXEC makes the descriptor inheritable across exec.
        let inheritable = unsafe { libc::open(c"/dev/null".as_ptr(), libc::O_RDONLY) };
        assert!(inheritable > 2);
        let dir = tempfile::tempdir().unwrap();
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "process::identity::tests::marker_fixture",
                "--nocapture",
            ])
            .env_clear()
            .env("AGENT_RUNNER_IDENTITY_MARKER", dir.path().join("marker"))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        command.as_std_mut().process_group(0);
        let token = CancellationToken::new();
        let result = spawn_checked(
            command,
            &token,
            &token,
            Instant::now() + Duration::from_secs(3),
            Duration::from_secs(3),
            Duration::from_secs(1),
            LaunchPolicy {
                probe: observe,
                spawn: Command::spawn,
            },
        )
        .await;
        // SAFETY: The descriptor was opened above and is closed exactly once.
        unsafe { libc::close(inheritable) };
        assert!(matches!(
            result,
            Err(ProcessError::IdentityUnavailable(
                io::ErrorKind::Unsupported
            ))
        ));
        let observed = HANDSHAKE_DESCRIPTORS.lock().unwrap().take().unwrap();
        for fd in [0, 1, 2] {
            assert!(observed.contains(&fd), "stdio {fd} must stay open");
        }
        for fd in held {
            assert!(
                !observed.contains(&fd),
                "close-on-exec descriptor {fd} was still held during the handshake: {observed:?}"
            );
        }
        assert!(
            observed.contains(&inheritable),
            "a descriptor inherited across exec must stay open"
        );
        drop((held_read, held_write));
    }

    #[tokio::test]
    async fn failed_identity_probe_never_executes_and_reaps_the_preexec_child() {
        fn reject(group: &OwnedGroup) -> Result<(), io::ErrorKind> {
            PROBE_PID.store(group.pid, Ordering::SeqCst);
            Err(io::ErrorKind::Unsupported)
        }
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("marker");
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "process::identity::tests::marker_fixture",
                "--nocapture",
            ])
            .env_clear()
            .env("AGENT_RUNNER_IDENTITY_MARKER", &marker)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        command.as_std_mut().process_group(0);
        let token = CancellationToken::new();
        let deadline = Instant::now() + Duration::from_secs(3);
        let result = spawn_checked(
            command,
            &token,
            &token,
            deadline,
            Duration::from_secs(3),
            Duration::from_secs(1),
            LaunchPolicy {
                probe: reject,
                spawn: Command::spawn,
            },
        )
        .await;
        assert!(matches!(
            result,
            Err(ProcessError::IdentityUnavailable(
                io::ErrorKind::Unsupported
            ))
        ));
        let pid = PROBE_PID.load(Ordering::SeqCst);
        assert_ne!(pid, 0);
        tokio::time::timeout(Duration::from_secs(3), async {
            let pid = rustix::process::Pid::from_raw(pid as i32).unwrap();
            while rustix::process::test_kill_process(pid).is_ok() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("pre-exec child was not reaped");
        assert!(!marker.exists());
    }

    #[tokio::test]
    async fn queued_start_limit_obeys_deadline_before_allocating_a_child() {
        let _lock = SLOT_TEST_LOCK.lock().await;
        let _all = START_SLOTS.acquire_many(32).await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("marker");
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "process::identity::tests::marker_fixture",
                "--nocapture",
            ])
            .env_clear()
            .env("AGENT_RUNNER_IDENTITY_MARKER", &marker);
        command.as_std_mut().process_group(0);
        let token = CancellationToken::new();
        let result = spawn_checked(
            command,
            &token,
            &token,
            Instant::now() + Duration::from_millis(40),
            Duration::from_secs(1),
            Duration::from_secs(1),
            LaunchPolicy {
                probe: OwnedGroup::probe,
                spawn: Command::spawn,
            },
        )
        .await;
        assert!(matches!(result, Err(ProcessError::DeadlineBeforeSpawn)));
        assert!(!marker.exists());
    }

    #[tokio::test]
    async fn cancelled_queued_spawn_releases_its_slot_while_another_runtime_is_blocked() {
        let _lock = SLOT_TEST_LOCK.lock().await;
        let held = START_SLOTS.acquire_many(31).await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("queued-marker");
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let (queued_tx, queued_rx) = tokio::sync::oneshot::channel();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let cancel = CancellationToken::new();
        let thread_cancel = cancel.clone();
        let thread_marker = marker.clone();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .max_blocking_threads(1)
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                let (started_tx, started_rx) = tokio::sync::oneshot::channel();
                let blocker = tokio::task::spawn_blocking(move || {
                    let _ = started_tx.send(());
                    let _ = release_rx.recv();
                });
                started_rx.await.unwrap();
                let mut command = Command::new(std::env::current_exe().unwrap());
                command
                    .args([
                        "--exact",
                        "process::identity::tests::marker_fixture",
                        "--nocapture",
                    ])
                    .env_clear()
                    .env("AGENT_RUNNER_IDENTITY_MARKER", thread_marker);
                command.as_std_mut().process_group(0);
                let dropped = CancellationToken::new();
                let task = tokio::spawn(async move {
                    spawn_checked(
                        command,
                        &thread_cancel,
                        &dropped,
                        Instant::now() + Duration::from_secs(4),
                        Duration::from_secs(4),
                        Duration::from_secs(1),
                        LaunchPolicy {
                            probe: OwnedGroup::probe,
                            spawn: Command::spawn,
                        },
                    )
                    .await
                });
                while START_SLOTS.available_permits() != 0 {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
                let _ = queued_tx.send(());
                let result = task.await.unwrap();
                let _ = done_tx.send(matches!(result, Err(ProcessError::CancelledBeforeSpawn)));
                blocker.await.unwrap();
            });
        });
        tokio::time::timeout(Duration::from_secs(3), queued_rx)
            .await
            .unwrap()
            .unwrap();
        cancel.cancel();
        assert!(
            tokio::time::timeout(Duration::from_secs(3), done_rx)
                .await
                .unwrap()
                .unwrap()
        );
        let slot = tokio::time::timeout(Duration::from_millis(300), START_SLOTS.acquire())
            .await
            .expect("cancelled queued task retained a global slot")
            .unwrap();
        drop(slot);
        drop(held);
        release_tx.send(()).unwrap();
        thread.join().unwrap();
        assert!(!marker.exists());
    }

    #[tokio::test]
    async fn reaper_releases_capacity_when_origin_runtime_is_not_driven() {
        let held = CLEANUP_SLOTS.acquire_many(31).await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("reap-marker");
        let thread_marker = marker.clone();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                let mut command = Command::new(std::env::current_exe().unwrap());
                command
                    .args([
                        "--exact",
                        "process::identity::tests::marker_fixture",
                        "--nocapture",
                    ])
                    .env_clear()
                    .env("AGENT_RUNNER_IDENTITY_MARKER", &thread_marker)
                    .env("AGENT_RUNNER_IDENTITY_SLEEP", "1")
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null());
                command.as_std_mut().process_group(0);
                let token = CancellationToken::new();
                let (child, group, pid) = spawn_checked(
                    command,
                    &token,
                    &token,
                    Instant::now() + Duration::from_secs(3),
                    Duration::from_secs(3),
                    Duration::from_secs(1),
                    LaunchPolicy {
                        probe: OwnedGroup::probe,
                        spawn: Command::spawn,
                    },
                )
                .await
                .unwrap();
                while !thread_marker.exists() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                drop(child);
                drop(group);
                let _ = ready_tx.send(pid);
            });
            let _ = release_rx.recv();
        });
        let pid = tokio::time::timeout(Duration::from_secs(3), ready_rx)
            .await
            .unwrap()
            .unwrap();
        let permit = tokio::time::timeout(Duration::from_secs(3), CLEANUP_SLOTS.acquire())
            .await
            .expect("cleanup capacity remained tied to an idle runtime")
            .unwrap();
        let pid = rustix::process::Pid::from_raw(pid as i32).unwrap();
        assert!(rustix::process::test_kill_process(pid).is_err());
        drop(permit);
        drop(held);
        release_tx.send(()).unwrap();
        thread.join().unwrap();
        assert!(marker.exists());
    }

    #[tokio::test]
    async fn failure_after_target_exec_is_uncertain_and_requests_reap() {
        fn spawn_then_fail(command: &mut Command) -> io::Result<tokio::process::Child> {
            let child = command.spawn()?;
            FAILURE_PID.store(child.id().unwrap(), Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(100));
            drop(child);
            Err(io::Error::other("injected post-exec setup failure"))
        }
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("marker");
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "process::identity::tests::marker_fixture",
                "--nocapture",
            ])
            .env_clear()
            .env("AGENT_RUNNER_IDENTITY_MARKER", &marker)
            .env("AGENT_RUNNER_IDENTITY_SLEEP", "1")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        command.as_std_mut().process_group(0);
        let token = CancellationToken::new();
        let result = spawn_checked(
            command,
            &token,
            &token,
            Instant::now() + Duration::from_secs(3),
            Duration::from_secs(3),
            Duration::from_secs(1),
            LaunchPolicy {
                probe: OwnedGroup::probe,
                spawn: spawn_then_fail,
            },
        )
        .await;
        assert!(matches!(
            result,
            Err(ProcessError::StartedUnsettled {
                kind: io::ErrorKind::Other,
                ..
            })
        ));
        assert!(marker.exists(), "target did not reach the fixture body");
        let pid =
            rustix::process::Pid::from_raw(FAILURE_PID.load(Ordering::SeqCst) as i32).unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while rustix::process::test_kill_process(pid).is_ok() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("post-exec child remained alive");
    }
}
