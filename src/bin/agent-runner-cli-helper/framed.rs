//! Private framed stdin forwarding with a separate helper status channel.

use std::{
    fs::File,
    io::{self, Write},
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    process::{Child, ExitStatus},
};

use super::{broker, broker_protocol as wire};

fn status(control: &mut File, kind: u8, sequence: u64) -> io::Result<()> {
    let mut bytes = [0; wire::STATUS_BYTES];
    bytes[0] = kind;
    bytes[1..].copy_from_slice(&sequence.to_be_bytes());
    control.write_all(&bytes)
}

fn read_nonblocking(fd: i32, target: &mut [u8]) -> io::Result<Option<usize>> {
    // SAFETY: target is writable for its length and fd is held open by the caller.
    let count = unsafe { libc::read(fd, target.as_mut_ptr().cast(), target.len()) };
    if count < 0 {
        let error = io::Error::last_os_error();
        if matches!(
            error.kind(),
            io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
        ) {
            return Ok(None);
        }
        return Err(error);
    }
    Ok(Some(count as usize))
}

pub(super) struct Prepared {
    control: File,
    upstream: File,
    cli_input: File,
}

impl Prepared {
    pub(super) fn still_ready(&self) -> io::Result<()> {
        let mut descriptors = [
            libc::pollfd {
                fd: self.control.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: self.upstream.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        // SAFETY: descriptors is a valid writable array and the timeout does not block.
        if unsafe {
            libc::poll(
                descriptors.as_mut_ptr(),
                descriptors.len() as libc::nfds_t,
                0,
            )
        } < 0
        {
            return Err(io::Error::last_os_error());
        }
        if descriptors[0].revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR | libc::POLLNVAL)
            != 0
            || descriptors[1].revents & (libc::POLLHUP | libc::POLLERR | libc::POLLNVAL) != 0
        {
            return Err(io::ErrorKind::Interrupted.into());
        }
        Ok(())
    }
}

fn outside_stdio(fd: OwnedFd) -> io::Result<OwnedFd> {
    if fd.as_raw_fd() > 2 {
        Ok(fd)
    } else {
        rustix::io::fcntl_dupfd_cloexec(&fd, 3).map_err(io::Error::from)
    }
}

/// Validates the private control socket and prepares both pipes before launching the target.
pub(super) fn prepare(control: File) -> io::Result<(Prepared, File)> {
    let mut socket_type: libc::c_int = 0;
    let mut length = std::mem::size_of_val(&socket_type) as libc::socklen_t;
    // SAFETY: socket_type and length are valid writable buffers for getsockopt.
    let result = unsafe {
        libc::getsockopt(
            control.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_TYPE,
            (&mut socket_type as *mut libc::c_int).cast(),
            &mut length,
        )
    };
    if result != 0 || socket_type != libc::SOCK_STREAM {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    let mut address = std::mem::MaybeUninit::<libc::sockaddr_storage>::zeroed();
    let mut address_len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    // SAFETY: address and address_len are valid writable buffers for getsockname.
    if unsafe {
        libc::getsockname(
            control.as_raw_fd(),
            address.as_mut_ptr().cast(),
            &mut address_len,
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    if address_len < std::mem::size_of::<libc::sa_family_t>() as libc::socklen_t {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    // SAFETY: getsockname initialized at least the address family.
    if unsafe { address.assume_init().ss_family as libc::c_int } != libc::AF_UNIX {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    let mut peer = std::mem::MaybeUninit::<libc::sockaddr_storage>::zeroed();
    let mut peer_len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    // SAFETY: peer and peer_len are valid writable buffers for getpeername.
    if unsafe { libc::getpeername(control.as_raw_fd(), peer.as_mut_ptr().cast(), &mut peer_len) }
        != 0
    {
        return Err(io::Error::last_os_error());
    }
    if peer_len < std::mem::size_of::<libc::sa_family_t>() as libc::socklen_t {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    // SAFETY: getpeername initialized at least the address family.
    if unsafe { peer.assume_init().ss_family as libc::c_int } != libc::AF_UNIX {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    broker::nonblocking(control.as_raw_fd())?;
    let upstream = broker::open_upstream()?;
    let mut fds = [-1; 2];
    // SAFETY: fds is a valid array for the two pipe descriptors.
    if unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: pipe2 returned two new, owned descriptors.
    let reader = unsafe { OwnedFd::from_raw_fd(fds[0]) };
    let writer = unsafe { OwnedFd::from_raw_fd(fds[1]) };
    let reader = File::from(outside_stdio(reader)?);
    let cli_input = File::from(outside_stdio(writer)?);
    broker::nonblocking(cli_input.as_raw_fd())?;
    let prepared = Prepared {
        control,
        upstream,
        cli_input,
    };
    prepared.still_ready()?;
    Ok((prepared, reader))
}

/// Forwards one framed input at a time and reports writes to the direct child's pipe.
pub(super) fn forward(
    child: &mut Child,
    child_pidfd: OwnedFd,
    prepared: Prepared,
) -> io::Result<(ExitStatus, bool)> {
    let Prepared {
        mut control,
        upstream,
        cli_input,
    } = prepared;
    let mut cli_input = Some(cli_input);
    let mut upstream = Some(upstream);
    status(&mut control, wire::READY, wire::MAGIC)?;
    let mut header = [0_u8; wire::HEADER_BYTES];
    let mut header_len = 0;
    let mut sequence = 0_u64;
    let mut last_sequence = 0_u64;
    let mut remaining: Option<usize> = None;
    let mut buffer = [0_u8; 8192];
    let mut begin = 0;
    let mut end = 0;
    let mut finishing = false;
    loop {
        let mut fds = [
            libc::pollfd {
                fd: child_pidfd.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: control.as_raw_fd(),
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
                    cli_input.as_ref().map_or(-1, AsRawFd::as_raw_fd)
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
            status(&mut control, wire::CHILD_EXITED, 0)?;
            return child.wait().map(|result| (result, finishing));
        }
        if fds[1].revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR | libc::POLLNVAL) != 0 {
            return Err(io::ErrorKind::Interrupted.into());
        }
        if fds[3].revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) != 0 {
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        if fds[3].revents & libc::POLLOUT != 0 {
            let input = cli_input.as_ref().ok_or(io::ErrorKind::InvalidInput)?;
            // SAFETY: The remaining buffer is valid for reading and input is an open pipe.
            let count = unsafe {
                libc::write(
                    input.as_raw_fd(),
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
            if begin == end && remaining == Some(0) {
                status(&mut control, wire::WRITTEN, sequence)?;
                remaining = None;
                header_len = 0;
            }
        }
        if fds[2].revents & libc::POLLNVAL != 0 {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        if fds[2].revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR) != 0 {
            let fd = upstream
                .as_ref()
                .ok_or(io::ErrorKind::InvalidInput)?
                .as_raw_fd();
            if let Some(left) = remaining {
                let length = left.min(buffer.len());
                let count = match read_nonblocking(fd, &mut buffer[..length])? {
                    Some(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
                    Some(count) => count,
                    None => continue,
                };
                remaining = Some(left - count);
                begin = 0;
                end = count;
            } else {
                let count = match read_nonblocking(fd, &mut header[header_len..])? {
                    Some(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
                    Some(count) => count,
                    None => continue,
                };
                header_len += count;
                if header_len != wire::HEADER_BYTES {
                    continue;
                }
                let length = u32::from_be_bytes(
                    header[..4]
                        .try_into()
                        .map_err(|_| io::ErrorKind::InvalidData)?,
                );
                sequence = u64::from_be_bytes(
                    header[4..]
                        .try_into()
                        .map_err(|_| io::ErrorKind::InvalidData)?,
                );
                if last_sequence.checked_add(1) != Some(sequence) {
                    return Err(io::ErrorKind::InvalidData.into());
                }
                last_sequence = sequence;
                if length == wire::FINISH {
                    drop(cli_input.take());
                    drop(upstream.take());
                    finishing = true;
                    status(&mut control, wire::CLOSED, sequence)?;
                } else if length > wire::MAX_FRAME {
                    return Err(io::ErrorKind::InvalidData.into());
                } else if length == 0 {
                    status(&mut control, wire::WRITTEN, sequence)?;
                    header_len = 0;
                } else {
                    remaining = Some(length as usize);
                }
            }
        }
    }
}
