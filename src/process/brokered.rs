//! Framed stdin delivery and helper status observation for a trusted CLI helper.

use std::{
    cell::Cell,
    io,
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex},
};

use tokio::{
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::UnixStream,
    sync::mpsc,
    time::Instant,
};

use super::{
    BrokerInputState, BrokeredInputReceiver, ControlledInput, Fault, InputCancellation,
    InputCompletion, ProcessBrokerFaultObserver, broker_protocol as wire,
};

pub(super) struct BrokeredInput {
    pub control: UnixStream,
    pub initial: Vec<u8>,
    pub input: BrokeredInputReceiver,
    pub max_bytes: usize,
    pub on_fault: Option<ProcessBrokerFaultObserver>,
    pub state: Arc<Mutex<BrokerInputState>>,
}

async fn read_status(control: &mut UnixStream, pending: &mut Vec<u8>) -> io::Result<(u8, u64)> {
    while pending.len() < wire::STATUS_BYTES {
        let mut buf = [0_u8; wire::STATUS_BYTES];
        let length = wire::STATUS_BYTES - pending.len();
        let count = control.read(&mut buf[..length]).await?;
        if count == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        pending.extend_from_slice(&buf[..count]);
    }
    let kind = pending[0];
    let sequence = u64::from_be_bytes(
        pending[1..]
            .try_into()
            .map_err(|_| io::ErrorKind::InvalidData)?,
    );
    pending.clear();
    Ok((kind, sequence))
}

async fn next_status(
    control: &mut UnixStream,
    pending: &mut Vec<u8>,
    cancellation: &InputCancellation,
    abort_on_output_close: bool,
) -> Result<(u8, u64), Option<io::ErrorKind>> {
    tokio::select! {
        biased;
        _ = cancellation.caller.cancelled() => Err(None),
        _ = cancellation.dropped.cancelled() => Err(None),
        _ = cancellation.stop.cancelled() => Err(None),
        _ = tokio::time::sleep_until(cancellation.deadline) => Err(None),
        _ = cancellation.payload_stop.cancelled(), if abort_on_output_close => Err(Some(io::ErrorKind::UnexpectedEof)),
        result = read_status(control, pending) => result.map_err(|error| Some(error.kind())),
    }
}

fn failed(events: &FaultGuard, kind: io::ErrorKind) -> InputCompletion {
    events.notify();
    let _ = events.sender.try_send(Fault::Input);
    InputCompletion::Failed(kind)
}

struct FaultGuard {
    sender: mpsc::Sender<Fault>,
    observer: Option<ProcessBrokerFaultObserver>,
    armed: Cell<bool>,
}

impl FaultGuard {
    fn notify(&self) {
        if self.armed.replace(false)
            && let Some(observer) = &self.observer
        {
            let _ = std::panic::catch_unwind(AssertUnwindSafe(|| observer()));
        }
    }
}

impl Drop for FaultGuard {
    fn drop(&mut self) {
        self.notify();
    }
}

fn confirm_written(
    gate: &Mutex<BrokerInputState>,
    confirmation: Option<tokio::sync::oneshot::Sender<()>>,
) -> Result<(), io::ErrorKind> {
    let state = gate.lock().map_err(|_| io::ErrorKind::InvalidData)?;
    if state.output_closed || state.protocol_faulted {
        return Err(io::ErrorKind::UnexpectedEof);
    }
    // The gate check orders this receipt against EOF; a waker must run after the lock is released.
    drop(state);
    if let Some(confirmation) = confirmation {
        let _ = confirmation.send(());
    }
    Ok(())
}

async fn write_frame<W: AsyncWrite + Unpin>(
    input: &mut W,
    length: u32,
    sequence: u64,
    bytes: &[u8],
    finish: bool,
    gate: &Mutex<BrokerInputState>,
    cancellation: &InputCancellation,
) -> Result<(), Option<io::ErrorKind>> {
    if !finish {
        loop {
            let notify = {
                let state = gate.lock().map_err(|_| Some(io::ErrorKind::InvalidData))?;
                if state.output_closed || state.protocol_faulted {
                    return Err(Some(io::ErrorKind::UnexpectedEof));
                }
                if state.output_observations == 0
                    && !state.protocol_holds_input
                    && !state.protocol_transition
                {
                    // A later EOF or protocol fault cannot retract this admitted write.
                    break;
                }
                Arc::clone(&state.observation_done)
            };
            tokio::select! {
                biased;
                _ = cancellation.caller.cancelled() => return Err(None),
                _ = cancellation.dropped.cancelled() => return Err(None),
                _ = cancellation.stop.cancelled() => return Err(None),
                _ = cancellation.payload_stop.cancelled() => return Err(None),
                _ = tokio::time::sleep_until(cancellation.deadline) => return Err(None),
                _ = notify.notified() => {},
            }
        }
    }
    if (!finish && cancellation.payload_stop.is_cancelled())
        || cancellation.caller.is_cancelled()
        || cancellation.dropped.is_cancelled()
        || cancellation.stop.is_cancelled()
        || Instant::now() >= cancellation.deadline
    {
        return Err(None);
    }
    let mut header = [0_u8; wire::HEADER_BYTES];
    header[..4].copy_from_slice(&length.to_be_bytes());
    header[4..].copy_from_slice(&sequence.to_be_bytes());
    tokio::select! {
        biased;
        _ = cancellation.caller.cancelled() => Err(None),
        _ = cancellation.dropped.cancelled() => Err(None),
        _ = cancellation.stop.cancelled() => Err(None),
        _ = cancellation.payload_stop.cancelled(), if !finish => Err(None),
        _ = tokio::time::sleep_until(cancellation.deadline) => Err(None),
        result = async { input.write_all(&header).await?; input.write_all(bytes).await } => result.map_err(|error| Some(error.kind())),
    }
}

pub(super) async fn write_controlled_input<W: AsyncWrite + Unpin>(
    mut stdin: W,
    context: BrokeredInput,
    events: mpsc::Sender<Fault>,
    cancellation: InputCancellation,
) -> InputCompletion {
    let BrokeredInput {
        mut control,
        initial,
        mut input,
        max_bytes,
        on_fault,
        state,
    } = context;
    let events = FaultGuard {
        sender: events,
        observer: on_fault,
        armed: Cell::new(true),
    };
    let mut pending = Vec::with_capacity(wire::STATUS_BYTES);
    match next_status(&mut control, &mut pending, &cancellation, true).await {
        Ok((wire::READY, wire::MAGIC)) => {}
        Err(None) => return InputCompletion::Interrupted,
        Err(Some(kind)) => return failed(&events, kind),
        _ => return failed(&events, io::ErrorKind::InvalidData),
    }
    let mut sequence = 0_u64;
    let mut total = 0_usize;
    let mut next = Some((ControlledInput::Bytes(initial), None));
    loop {
        let command = if let Some(command) = next.take() {
            Some(command)
        } else {
            tokio::select! {
                biased;
                _ = cancellation.caller.cancelled() => return InputCompletion::Interrupted,
                _ = cancellation.dropped.cancelled() => return InputCompletion::Interrupted,
                _ = cancellation.stop.cancelled() => return InputCompletion::Interrupted,
                _ = cancellation.payload_stop.cancelled() => {
                    let finish_queued = state.lock().is_ok_and(|state| state.finish_queued);
                    if !finish_queued {
                        return failed(&events, io::ErrorKind::UnexpectedEof);
                    }
                    tokio::select! {
                        biased;
                        _ = cancellation.caller.cancelled() => return InputCompletion::Interrupted,
                        _ = cancellation.dropped.cancelled() => return InputCompletion::Interrupted,
                        _ = cancellation.stop.cancelled() => return InputCompletion::Interrupted,
                        _ = tokio::time::sleep_until(cancellation.deadline) => return InputCompletion::Interrupted,
                        command = input.recv() => match command {
                            Some(message) if matches!(&message.input, ControlledInput::Finish) => {
                                Some((message.input, Some(message.reservation)))
                            }
                            _ => return failed(&events, io::ErrorKind::UnexpectedEof),
                        },
                    }
                }
                _ = tokio::time::sleep_until(cancellation.deadline) => return InputCompletion::Interrupted,
                response = read_status(&mut control, &mut pending) => {
                    match response {
                        Ok((wire::CHILD_EXITED, 0)) => {
                            return failed(&events, io::ErrorKind::UnexpectedEof);
                        }
                        Ok(_) => return failed(&events, io::ErrorKind::InvalidData),
                        Err(error) => return failed(&events, error.kind()),
                    }
                }
                command = input.recv() => command.map(|message| (message.input, Some(message.reservation))),
            }
        };
        let Some((command, reservation)) = command else {
            return failed(&events, io::ErrorKind::UnexpectedEof);
        };
        let Some(next_sequence) = sequence.checked_add(1) else {
            return failed(&events, io::ErrorKind::InvalidData);
        };
        sequence = next_sequence;
        let (length, bytes, confirmation, finish) = match command {
            ControlledInput::Bytes(bytes) => (bytes.len(), bytes, None, false),
            ControlledInput::Confirmed { bytes, written } => {
                (bytes.len(), bytes, Some(written), false)
            }
            ControlledInput::Finish => (0, Vec::new(), None, true),
        };
        let Some(next_total) = total.checked_add(length).filter(|size| *size <= max_bytes) else {
            return failed(&events, io::ErrorKind::InvalidInput);
        };
        let Some(length) = u32::try_from(length)
            .ok()
            .filter(|length| *length <= wire::MAX_FRAME)
        else {
            return failed(&events, io::ErrorKind::InvalidInput);
        };
        let length = if finish { wire::FINISH } else { length };
        match write_frame(
            &mut stdin,
            length,
            sequence,
            &bytes,
            finish,
            &state,
            &cancellation,
        )
        .await
        {
            Ok(()) => {}
            Err(None) if !finish && cancellation.payload_stop.is_cancelled() => {
                return failed(&events, io::ErrorKind::UnexpectedEof);
            }
            Err(None) => return InputCompletion::Interrupted,
            Err(Some(kind)) => return failed(&events, kind),
        }
        let response = match next_status(&mut control, &mut pending, &cancellation, !finish).await {
            Ok(response) => response,
            Err(None) => return InputCompletion::Interrupted,
            Err(Some(kind)) => return failed(&events, kind),
        };
        if response == (wire::CHILD_EXITED, 0) {
            return failed(&events, io::ErrorKind::UnexpectedEof);
        }
        if response != (if finish { wire::CLOSED } else { wire::WRITTEN }, sequence) {
            return failed(&events, io::ErrorKind::InvalidData);
        }
        total = next_total;
        drop(reservation);
        if !finish && let Err(kind) = confirm_written(&state, confirmation) {
            return failed(&events, kind);
        }
        if finish {
            let shutdown = tokio::select! {
                biased;
                _ = cancellation.caller.cancelled() => return InputCompletion::Interrupted,
                _ = cancellation.dropped.cancelled() => return InputCompletion::Interrupted,
                _ = cancellation.stop.cancelled() => return InputCompletion::Interrupted,
                _ = tokio::time::sleep_until(cancellation.deadline) => return InputCompletion::Interrupted,
                result = stdin.shutdown() => result,
            };
            if let Err(error) = shutdown {
                return failed(&events, error.kind());
            }
            cancellation.finished.cancel();
            match next_status(&mut control, &mut pending, &cancellation, false).await {
                Ok((wire::CHILD_EXITED, 0)) => {
                    events.armed.set(false);
                    return InputCompletion::Complete;
                }
                Ok(_) => return failed(&events, io::ErrorKind::InvalidData),
                Err(None) => return InputCompletion::Interrupted,
                Err(Some(kind)) => return failed(&events, kind),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };
    use tokio_util::sync::CancellationToken;

    #[test]
    fn confirmation_waker_can_reenter_the_input_sender() {
        use std::{
            future::Future,
            pin::Pin,
            task::{Context, Poll, Wake, Waker},
        };

        struct Reenter {
            sender: super::super::BrokeredInputSender,
            called: Arc<AtomicBool>,
        }
        impl Wake for Reenter {
            fn wake(self: Arc<Self>) {
                self.sender.try_send(ControlledInput::Finish).unwrap();
                self.called.store(true, Ordering::Release);
            }
        }
        let (sender, mut receiver) = super::super::brokered_input_channel(2, 1024);
        let called = Arc::new(AtomicBool::new(false));
        let waker = Waker::from(Arc::new(Reenter {
            sender,
            called: Arc::clone(&called),
        }));
        let (written, mut receipt) = tokio::sync::oneshot::channel();
        let mut context = Context::from_waker(&waker);
        assert!(matches!(
            Pin::new(&mut receipt).poll(&mut context),
            Poll::Pending
        ));
        let state = Arc::clone(&receiver.state);
        let (done, result) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = done.send(confirm_written(&state, Some(written)));
        });
        result
            .recv_timeout(Duration::from_secs(2))
            .expect("confirmation deadlocked")
            .unwrap();
        assert!(called.load(Ordering::Acquire));
        assert!(matches!(
            receiver.input.try_recv(),
            Ok(super::super::BrokeredMessage {
                input: ControlledInput::Finish,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn partial_status_survives_cancellation_of_a_pending_read() {
        let (mut parent, mut child) = UnixStream::pair().unwrap();
        let mut pending = Vec::new();
        child.write_all(&[wire::WRITTEN, 0, 0]).await.unwrap();
        tokio::select! {
            result = read_status(&mut parent, &mut pending) => panic!("unexpected status: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(25)) => {}
        }
        assert_eq!(pending.len(), 3);
        child.write_all(&[0, 0, 0, 0, 0, 7]).await.unwrap();
        assert_eq!(
            read_status(&mut parent, &mut pending).await.unwrap(),
            (wire::WRITTEN, 7)
        );
        assert!(pending.is_empty());
    }

    #[tokio::test]
    async fn payload_is_not_started_after_gate_observes_eof() {
        let (mut writer, mut reader) = tokio::io::duplex(64);
        let gate = Mutex::new(BrokerInputState {
            output_closed: true,
            ..Default::default()
        });
        let cancellation = InputCancellation {
            stop: CancellationToken::new(),
            payload_stop: CancellationToken::new(),
            caller: CancellationToken::new(),
            dropped: CancellationToken::new(),
            deadline: Instant::now() + Duration::from_secs(2),
            finished: CancellationToken::new(),
        };
        assert_eq!(
            write_frame(&mut writer, 4, 1, b"data", false, &gate, &cancellation).await,
            Err(Some(io::ErrorKind::UnexpectedEof))
        );
        drop(writer);
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await.unwrap();
        assert!(bytes.is_empty());
    }

    #[tokio::test]
    async fn protocol_fault_prevents_a_queued_payload_from_starting() {
        let (sender, mut receiver) = super::super::brokered_input_channel(1, 1024);
        sender
            .try_send(ControlledInput::Bytes(b"data".to_vec()))
            .unwrap();
        let message = receiver.recv().await.unwrap();
        sender.stop_before_write();
        assert_eq!(
            sender.try_send(ControlledInput::Bytes(b"later".to_vec())),
            Err(super::super::BrokerInputError::Stopped)
        );
        let ControlledInput::Bytes(bytes) = message.input else {
            panic!("expected queued payload")
        };
        let (mut writer, mut reader) = tokio::io::duplex(64);
        let cancellation = InputCancellation {
            stop: CancellationToken::new(),
            payload_stop: CancellationToken::new(),
            caller: CancellationToken::new(),
            dropped: CancellationToken::new(),
            deadline: Instant::now() + Duration::from_secs(2),
            finished: CancellationToken::new(),
        };
        assert_eq!(
            write_frame(
                &mut writer,
                4,
                1,
                &bytes,
                false,
                &receiver.state,
                &cancellation
            )
            .await,
            Err(Some(io::ErrorKind::UnexpectedEof))
        );
        drop(writer);
        let mut observed = Vec::new();
        reader.read_to_end(&mut observed).await.unwrap();
        assert!(observed.is_empty());
    }

    #[tokio::test]
    async fn already_read_output_is_observed_before_another_payload_starts() {
        use std::{
            future::Future,
            task::{Context, Poll, Waker},
        };

        for valid in [false, true] {
            let gate = Arc::new(Mutex::new(BrokerInputState::default()));
            let (mut cli_stdout, mut stdout) = tokio::io::duplex(64);
            cli_stdout.write_all(b"output\n").await.unwrap();
            let mut observed = [0_u8; 64];
            assert_eq!(
                super::super::read_brokered_stream(
                    &mut stdout,
                    &mut observed,
                    &gate,
                    &CancellationToken::new(),
                    &CancellationToken::new(),
                    super::super::OutputStream::Stdout,
                )
                .await
                .unwrap(),
                7
            );
            let mut observation = super::super::BrokerObservation::new(Arc::clone(&gate)).unwrap();
            let (mut writer, mut reader) = tokio::io::duplex(64);
            let cancellation = InputCancellation {
                stop: CancellationToken::new(),
                payload_stop: CancellationToken::new(),
                caller: CancellationToken::new(),
                dropped: CancellationToken::new(),
                deadline: Instant::now() + Duration::from_secs(2),
                finished: CancellationToken::new(),
            };
            let mut write = Box::pin(write_frame(
                &mut writer,
                4,
                1,
                b"data",
                false,
                &gate,
                &cancellation,
            ));
            let mut context = Context::from_waker(Waker::noop());
            assert!(matches!(write.as_mut().poll(&mut context), Poll::Pending));
            observation.finish(valid);
            let result = write.await;
            if valid {
                assert_eq!(result, Ok(()));
            } else {
                assert_eq!(result, Err(Some(io::ErrorKind::UnexpectedEof)));
            }
            drop(writer);
            let mut sent = Vec::new();
            reader.read_to_end(&mut sent).await.unwrap();
            if valid {
                assert_eq!(&sent[12..], b"data");
            } else {
                assert!(sent.is_empty());
            }
        }
    }

    #[tokio::test]
    async fn incomplete_protocol_frame_holds_queued_writes_until_released() {
        use std::{
            future::Future,
            task::{Context, Poll, Waker},
        };

        let (sender, receiver) = super::super::brokered_input_channel(1, 1024);
        sender.hold_before_write(true);
        let (mut writer, mut reader) = tokio::io::duplex(64);
        let cancellation = InputCancellation {
            stop: CancellationToken::new(),
            payload_stop: CancellationToken::new(),
            caller: CancellationToken::new(),
            dropped: CancellationToken::new(),
            deadline: Instant::now() + Duration::from_secs(2),
            finished: CancellationToken::new(),
        };
        let mut write = Box::pin(write_frame(
            &mut writer,
            4,
            1,
            b"data",
            false,
            &receiver.state,
            &cancellation,
        ));
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(write.as_mut().poll(&mut context), Poll::Pending));
        sender.hold_before_write(false);
        assert_eq!(write.await, Ok(()));
        drop(writer);
        let mut sent = Vec::new();
        reader.read_to_end(&mut sent).await.unwrap();
        assert_eq!(&sent[12..], b"data");
    }

    #[tokio::test]
    async fn brokered_stream_read_error_latches_the_write_gate() {
        use std::{
            pin::Pin,
            task::{Context, Poll},
        };
        struct Broken;
        impl tokio::io::AsyncRead for Broken {
            fn poll_read(
                self: Pin<&mut Self>,
                _: &mut Context<'_>,
                _: &mut tokio::io::ReadBuf<'_>,
            ) -> Poll<io::Result<()>> {
                Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()))
            }
        }
        for stream in [
            super::super::OutputStream::Stdout,
            super::super::OutputStream::Stderr,
        ] {
            let gate = Mutex::new(BrokerInputState::default());
            assert!(
                super::super::read_brokered_stream(
                    &mut Broken,
                    &mut [0_u8; 16],
                    &gate,
                    &CancellationToken::new(),
                    &CancellationToken::new(),
                    stream,
                )
                .await
                .is_err()
            );
            assert!(gate.lock().unwrap().protocol_faulted);
        }
    }

    #[tokio::test]
    async fn output_eof_interrupts_payload_receipt_but_allows_finish_receipt() {
        let (mut peer, mut control) = UnixStream::pair().unwrap();
        let cancellation = InputCancellation {
            stop: CancellationToken::new(),
            payload_stop: CancellationToken::new(),
            caller: CancellationToken::new(),
            dropped: CancellationToken::new(),
            deadline: Instant::now() + Duration::from_secs(2),
            finished: CancellationToken::new(),
        };
        let mut written = [0_u8; wire::STATUS_BYTES];
        written[0] = wire::WRITTEN;
        written[1..].copy_from_slice(&2_u64.to_be_bytes());
        peer.write_all(&written).await.unwrap();
        cancellation.payload_stop.cancel();
        let mut pending = Vec::new();
        assert_eq!(
            next_status(&mut control, &mut pending, &cancellation, true).await,
            Err(Some(io::ErrorKind::UnexpectedEof))
        );
        assert_eq!(
            read_status(&mut control, &mut pending).await.unwrap(),
            (wire::WRITTEN, 2)
        );
        let mut closed = [0_u8; wire::STATUS_BYTES];
        closed[0] = wire::CLOSED;
        closed[1..].copy_from_slice(&3_u64.to_be_bytes());
        peer.write_all(&closed).await.unwrap();
        assert_eq!(
            next_status(&mut control, &mut pending, &cancellation, false).await,
            Ok((wire::CLOSED, 3))
        );
    }

    #[tokio::test]
    async fn failed_frame_latches_protocol_before_reporting_input_fault() {
        let (mut control_peer, control) = UnixStream::pair().unwrap();
        let mut ready = [0_u8; wire::STATUS_BYTES];
        ready[0] = wire::READY;
        ready[1..].copy_from_slice(&wire::MAGIC.to_be_bytes());
        control_peer.write_all(&ready).await.unwrap();
        let (stdin, reader) = tokio::io::duplex(16);
        drop(reader);
        let (_send, input) = super::super::brokered_input_channel(1, 1024);
        let (events, mut faults) = mpsc::channel(2);
        let latched = Arc::new(AtomicBool::new(false));
        let observed = Arc::clone(&latched);
        let on_fault: ProcessBrokerFaultObserver = Arc::new(move || {
            observed.store(true, Ordering::Release);
            Ok(())
        });
        let cancellation = InputCancellation {
            stop: CancellationToken::new(),
            payload_stop: CancellationToken::new(),
            caller: CancellationToken::new(),
            dropped: CancellationToken::new(),
            deadline: Instant::now() + Duration::from_secs(2),
            finished: CancellationToken::new(),
        };
        let result = write_controlled_input(
            stdin,
            BrokeredInput {
                control,
                initial: b"init".to_vec(),
                input,
                max_bytes: 1024,
                on_fault: Some(on_fault),
                state: Arc::new(Mutex::new(BrokerInputState::default())),
            },
            events,
            cancellation,
        )
        .await;
        assert!(matches!(result, InputCompletion::Failed(_)));
        assert!(matches!(faults.try_recv(), Ok(Fault::Input)));
        assert!(latched.load(Ordering::Acquire));
    }
}
