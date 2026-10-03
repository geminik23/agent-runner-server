//! Holds a trusted target before exec until its pidfd has been verified.

use std::{
    io::{self, Read, Write},
    net::Shutdown,
    os::{
        fd::{AsFd, AsRawFd, OwnedFd},
        unix::{net::UnixStream, process::CommandExt},
    },
    process::{Child, Command},
    time::Duration,
};

use rustix::process::{Pid, PidfdFlags, WaitId, WaitIdOptions, pidfd_open, waitid};

fn outside_stdio(stream: UnixStream) -> io::Result<UnixStream> {
    if stream.as_raw_fd() > 2 {
        Ok(stream)
    } else {
        let fd = rustix::io::fcntl_dupfd_cloexec(&stream, 3).map_err(io::Error::from)?;
        Ok(UnixStream::from(fd))
    }
}

fn child_write(fd: i32, bytes: &[u8]) -> io::Result<()> {
    let mut offset = 0;
    while offset < bytes.len() {
        // SAFETY: bytes is readable and fd is the child's handshake socket.
        let count =
            unsafe { libc::write(fd, bytes[offset..].as_ptr().cast(), bytes.len() - offset) };
        if count < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        if count == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
        offset += count as usize;
    }
    Ok(())
}

fn child_read_go(fd: i32) -> io::Result<()> {
    let mut byte = 0_u8;
    loop {
        // SAFETY: byte is writable and fd is the child's handshake socket.
        let count = unsafe { libc::read(fd, (&mut byte as *mut u8).cast(), 1) };
        if count == 1 {
            return if byte == 0x37 {
                Ok(())
            } else {
                Err(io::ErrorKind::InvalidData.into())
            };
        }
        if count == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

fn verified_pidfd(pid: Pid) -> io::Result<OwnedFd> {
    let fd = pidfd_open(pid, PidfdFlags::empty()).map_err(io::Error::from)?;
    let ready = waitid(
        WaitId::PidFd(fd.as_fd()),
        WaitIdOptions::EXITED | WaitIdOptions::NOHANG | WaitIdOptions::NOWAIT,
    )
    .map_err(io::Error::from)?;
    if ready.is_some() {
        return Err(io::ErrorKind::NotFound.into());
    }
    Ok(fd)
}

pub(super) fn spawn(
    command: Command,
    ready: impl FnOnce() -> io::Result<()>,
) -> io::Result<(Child, OwnedFd)> {
    spawn_with(command, verified_pidfd, ready)
}

fn spawn_with(
    mut command: Command,
    acquire: impl FnOnce(Pid) -> io::Result<OwnedFd>,
    ready: impl FnOnce() -> io::Result<()>,
) -> io::Result<(Child, OwnedFd)> {
    let (parent, peer) = UnixStream::pair()?;
    let mut parent = outside_stdio(parent)?;
    let peer = outside_stdio(peer)?;
    let timeout = Some(Duration::from_secs(10));
    parent.set_read_timeout(timeout)?;
    parent.set_write_timeout(timeout)?;
    peer.set_read_timeout(timeout)?;
    peer.set_write_timeout(timeout)?;
    let parent_fd = parent.as_raw_fd();
    // SAFETY: The child callback uses only async-signal-safe syscalls and stack data.
    unsafe {
        command.pre_exec(move || {
            libc::close(parent_fd);
            let pid = libc::getpid().to_ne_bytes();
            child_write(peer.as_raw_fd(), &pid)?;
            child_read_go(peer.as_raw_fd())
        });
    }
    let task = std::thread::Builder::new()
        .name("agent-runner-cli-launch".into())
        .spawn(move || command.spawn())?;
    let prepare = (|| -> io::Result<(u32, OwnedFd)> {
        let mut bytes = [0_u8; 4];
        parent.read_exact(&mut bytes)?;
        let raw = i32::from_ne_bytes(bytes);
        let pid = Pid::from_raw(raw)
            .filter(|pid| pid.as_raw_pid() > 1)
            .ok_or(io::ErrorKind::InvalidData)?;
        let fd = acquire(pid)?;
        ready()?;
        parent.write_all(&[0x37])?;
        Ok((raw as u32, fd))
    })();
    if prepare.is_err() {
        let _ = parent.shutdown(Shutdown::Both);
    }
    drop(parent);
    let spawned = task.join().map_err(|_| io::ErrorKind::Other)?;
    let (expected, fd) = prepare?;
    let mut child = spawned?;
    if child.id() != expected {
        let _ = child.kill();
        let _ = child.wait();
        return Err(io::ErrorKind::InvalidData.into());
    }
    Ok((child, fd))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn command(marker: &std::path::Path) -> Command {
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "printf spawned > \"$1\"", "fixture"]);
        command.arg(marker);
        command
    }

    #[test]
    fn target_does_not_exec_if_pidfd_verification_fails() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("spawned");
        let result = spawn_with(
            command(&marker),
            |_| Err(io::ErrorKind::PermissionDenied.into()),
            || Ok(()),
        );
        assert!(result.is_err());
        assert!(!marker.exists());
    }

    #[test]
    fn target_does_not_exec_if_control_fails_before_release() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("spawned");
        let result = spawn_with(command(&marker), verified_pidfd, || {
            Err(io::ErrorKind::BrokenPipe.into())
        });
        assert!(result.is_err());
        assert!(!marker.exists());
    }

    #[test]
    fn target_execs_only_after_pidfd_verification() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("spawned");
        let (mut child, _pidfd) = spawn_with(
            command(&marker),
            |pid| {
                assert!(!marker.exists());
                verified_pidfd(pid)
            },
            || Ok(()),
        )
        .unwrap();
        assert!(child.wait().unwrap().success());
        assert_eq!(std::fs::read_to_string(marker).unwrap(), "spawned");
    }
}
