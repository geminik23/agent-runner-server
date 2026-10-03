//! Bounded, opt-in stdin forwarding for a single CLI child.

use std::{
    fs::{File, OpenOptions},
    io,
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
        unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt},
    },
    process::{Child, ExitStatus},
};

pub(super) fn nonblocking(fd: RawFd) -> io::Result<()> {
    // SAFETY: fcntl operates on a valid descriptor without retaining pointers.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: F_SETFL changes flags on this open file description only.
    if unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

pub(super) fn pidfd(child: &Child) -> io::Result<OwnedFd> {
    // SAFETY: pidfd_open takes the spawned child PID and scalar flags.
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, child.id(), 0_u32) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: pidfd_open returns a new descriptor owned by this function.
    Ok(unsafe { OwnedFd::from_raw_fd(fd as RawFd) })
}

pub(super) fn open_upstream() -> io::Result<File> {
    // SAFETY: F_GETFD checks whether the inherited stdin descriptor is open.
    if unsafe { libc::fcntl(libc::STDIN_FILENO, libc::F_GETFD) } < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: This function owns the helper's inherited stdin descriptor.
    let inherited = unsafe { File::from_raw_fd(libc::STDIN_FILENO) };
    let original = inherited.metadata()?;
    // SAFETY: inherited remains open for the duration of this query.
    let flags = unsafe { libc::fcntl(inherited.as_raw_fd(), libc::F_GETFL) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }
    if !original.file_type().is_fifo()
        || flags & libc::O_ACCMODE != libc::O_RDONLY
        || flags & libc::O_PATH != 0
    {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    let link = std::fs::read_link("/proc/self/fd/0")?;
    if link.as_os_str() != std::ffi::OsStr::new(&format!("pipe:[{}]", original.ino())) {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    let reopened = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK | libc::O_CLOEXEC)
        .open("/proc/self/fd/0")?;
    let metadata = reopened.metadata()?;
    if (metadata.dev(), metadata.ino()) != (original.dev(), original.ino()) {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    drop(inherited);
    Ok(reopened)
}

/// Reports whether EOF followed forwarding of all bytes read from the dedicated upstream pipe.
/// A caller must ensure that no other process consumes that pipe.
pub(super) fn forward(child: &mut Child) -> io::Result<(ExitStatus, bool)> {
    let cli_input = child.stdin.take().ok_or(io::ErrorKind::InvalidInput)?;
    let mut upstream = Some(open_upstream()?);
    let child_pidfd = pidfd(child)?;
    nonblocking(cli_input.as_raw_fd())?;
    let mut buffer = [0_u8; 8192];
    let mut begin = 0;
    let mut end = 0;
    loop {
        let mut fds = [
            libc::pollfd {
                fd: child_pidfd.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: if begin == end {
                    upstream.as_ref().map_or(-1, AsRawFd::as_raw_fd)
                } else {
                    -1
                },
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: if begin < end {
                    cli_input.as_raw_fd()
                } else {
                    -1
                },
                events: libc::POLLOUT,
                revents: 0,
            },
        ];
        // SAFETY: fds is a valid writable array for the duration of poll.
        let result = unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, -1) };
        if result < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        if fds[0].revents & libc::POLLNVAL != 0 {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        if fds[0].revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR) != 0 {
            drop(cli_input);
            drop(upstream);
            return child.wait().map(|status| (status, false));
        }
        if fds[2].revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) != 0 {
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        if fds[2].revents & libc::POLLOUT != 0 {
            // SAFETY: The remaining buffer is valid for reading and cli_input is an open pipe.
            let count = unsafe {
                libc::write(
                    cli_input.as_raw_fd(),
                    buffer[begin..end].as_ptr().cast(),
                    end - begin,
                )
            };
            if count < 0 {
                let error = io::Error::last_os_error();
                if matches!(
                    error.kind(),
                    io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
                ) {
                    continue;
                }
                return Err(error);
            }
            if count == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            begin += count as usize;
        }
        if fds[1].revents & libc::POLLNVAL != 0 {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        if fds[1].revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR) != 0 {
            let fd = upstream.as_ref().ok_or(io::ErrorKind::InvalidInput)?;
            // SAFETY: buffer is valid for writing and fd is the reopened upstream pipe.
            let count =
                unsafe { libc::read(fd.as_raw_fd(), buffer.as_mut_ptr().cast(), buffer.len()) };
            if count < 0 {
                let error = io::Error::last_os_error();
                if matches!(
                    error.kind(),
                    io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
                ) {
                    continue;
                }
                return Err(error);
            }
            if count == 0 {
                drop(cli_input);
                upstream.take();
                return child.wait().map(|status| (status, true));
            }
            begin = 0;
            end = count as usize;
        }
    }
}
