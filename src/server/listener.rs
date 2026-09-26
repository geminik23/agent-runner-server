use std::{
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use axum::serve::Listener;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Sleep,
};
use tokio_util::sync::CancellationToken;

/// Bounds socket ownership independently from the number of active handlers.
pub(super) struct BoundedListener {
    inner: TcpListener,
    slots: Arc<Semaphore>,
    idle: Duration,
    max_lifetime: Duration,
    failed: CancellationToken,
}
impl BoundedListener {
    pub fn new(
        inner: TcpListener,
        max: usize,
        idle: Duration,
        max_lifetime: Duration,
        failed: CancellationToken,
    ) -> Self {
        Self {
            inner,
            slots: Arc::new(Semaphore::new(max)),
            idle,
            max_lifetime,
            failed,
        }
    }
}
impl Listener for BoundedListener {
    type Io = BoundedStream;
    type Addr = SocketAddr;
    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        let mut failures = 0u8;
        loop {
            let permit = Arc::clone(&self.slots)
                .acquire_owned()
                .await
                .expect("connection semaphore is never closed");
            match self.inner.accept().await {
                Ok((stream, address)) => {
                    return (
                        BoundedStream::new(stream, permit, self.idle, self.max_lifetime),
                        address,
                    );
                }
                Err(error) => {
                    drop(permit);
                    if !matches!(
                        error.kind(),
                        io::ErrorKind::Interrupted
                            | io::ErrorKind::ConnectionAborted
                            | io::ErrorKind::ConnectionReset
                            | io::ErrorKind::WouldBlock
                    ) {
                        failures += 1;
                        if failures >= 3 {
                            self.failed.cancel();
                            return std::future::pending().await;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }
    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.inner.local_addr()
    }
}

pub(super) struct BoundedStream {
    stream: TcpStream,
    _permit: OwnedSemaphorePermit,
    idle: Duration,
    deadline: Pin<Box<Sleep>>,
    lifetime: Pin<Box<Sleep>>,
}
impl BoundedStream {
    fn new(
        stream: TcpStream,
        permit: OwnedSemaphorePermit,
        idle: Duration,
        max_lifetime: Duration,
    ) -> Self {
        Self {
            stream,
            _permit: permit,
            idle,
            deadline: Box::pin(tokio::time::sleep(idle)),
            lifetime: Box::pin(tokio::time::sleep(max_lifetime)),
        }
    }
    fn reset(&mut self) {
        self.deadline
            .as_mut()
            .reset(tokio::time::Instant::now() + self.idle);
    }
    fn lifetime_expired(&mut self, cx: &mut Context<'_>) -> bool {
        self.lifetime.as_mut().poll(cx).is_ready()
    }
    fn idle(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.lifetime_expired(cx) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "connection lifetime limit",
            )));
        }
        match self.deadline.as_mut().poll(cx) {
            Poll::Ready(()) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "connection idle limit",
            ))),
            Poll::Pending => Poll::Pending,
        }
    }
}
impl AsyncRead for BoundedStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.lifetime_expired(cx) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "connection lifetime limit",
            )));
        }
        let before = buf.filled().len();
        match Pin::new(&mut self.stream).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                if buf.filled().len() > before {
                    self.reset();
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => self.idle(cx),
        }
    }
}
impl AsyncWrite for BoundedStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.lifetime_expired(cx) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "connection lifetime limit",
            )));
        }
        match Pin::new(&mut self.stream).poll_write(cx, buf) {
            Poll::Ready(Ok(n)) => {
                if n > 0 {
                    self.reset();
                }
                Poll::Ready(Ok(n))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => match self.idle(cx) {
                Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
                _ => Poll::Pending,
            },
        }
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.lifetime_expired(cx) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "connection lifetime limit",
            )));
        }
        match Pin::new(&mut self.stream).poll_flush(cx) {
            Poll::Pending => self.idle(cx),
            other => other,
        }
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.lifetime_expired(cx) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "connection lifetime limit",
            )));
        }
        match Pin::new(&mut self.stream).poll_shutdown(cx) {
            Poll::Pending => self.idle(cx),
            other => other,
        }
    }
}
