//! Bounded supervision of a trusted, directly owned child process.

#[cfg(target_os = "linux")]
mod broker_protocol;
#[cfg(target_os = "linux")]
mod brokered;
#[cfg(target_os = "linux")]
mod identity;

#[cfg(target_os = "linux")]
use std::process::Stdio;
use std::{
    collections::{BTreeMap, HashSet},
    ffi::OsString,
    fmt,
    fs::File,
    io,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::time::Instant;
#[cfg(target_os = "linux")]
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    process::Command,
    sync::mpsc,
    time::timeout,
};
use tokio_util::sync::CancellationToken;

/// A command assembled by trusted application code, never by an untrusted request.
/// The child receives only the explicitly supplied environment entries.
pub struct ProcessSpec {
    pub program: PathBuf,
    pub args: Vec<OsString>,
    pub cwd: Option<PathBuf>,
    pub env: Vec<(OsString, OsString)>,
    pub stdin: Vec<u8>,
    pub limits: ProcessLimits,
    pub stop_signal: StopSignal,
}

/// Explicit resource and timing limits for one child process.
#[derive(Clone, Copy, Debug)]
pub struct ProcessLimits {
    pub max_args: usize,
    pub max_env: usize,
    pub max_spec_bytes: usize,
    pub max_stdin_bytes: usize,
    pub max_stdout_bytes: usize,
    pub max_stderr_bytes: usize,
    pub max_line_bytes: usize,
    /// This bound starts when the supervisor is polled, not at Run admission.
    pub timeout: Duration,
    pub signal_grace: Duration,
    pub kill_grace: Duration,
    pub pipe_grace: Duration,
}

/// A graceful signal applied to the owned Unix process group before escalation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StopSignal {
    Interrupt,
    Terminate,
}

/// Identifies the spawned child for observation, not as authority to signal a PID later.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProcessIdentity {
    pub pid: u32,
}

/// Identifies a pipe without embedding its contents in errors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutputStream {
    Stdout,
    Stderr,
}

/// Describes why the supervisor stopped waiting for normal process exit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StopReason {
    Exited,
    Cancelled,
    Deadline,
    OutputLimit(OutputStream),
    LineLimit(OutputStream),
    InputFailure,
    OutputFailure(OutputStream),
    ObserverFailure(OutputStream),
    WaitFailure,
    PipeTimeout,
}

/// Records the direct child's observed exit, not a vendor operation outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExitObservation {
    pub code: Option<i32>,
    pub signal: Option<i32>,
}

/// Records what happened to the stdin writer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InputCompletion {
    Complete,
    Interrupted,
    Failed(io::ErrorKind),
    TimedOut,
}

/// Records why a pipe reader stopped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutputCompletion {
    Eof,
    OutputLimit,
    LineLimit,
    ObserverFailure,
    Failed(io::ErrorKind),
    Interrupted,
    TimedOut,
}

/// Reports that a streaming consumer could not accept an output chunk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObserverError;

/// Receives bounded raw chunks in stream order and must never block or panic.
/// The callback can be invoked concurrently for stdout and stderr.
pub type ProcessObserver =
    Arc<dyn Fn(OutputStream, &[u8]) -> Result<(), ObserverError> + Send + Sync>;

pub(crate) type ProcessStartObserver = Arc<dyn Fn(ProcessIdentity) + Send + Sync>;
#[cfg(target_os = "linux")]
pub(crate) type ProcessBrokerFaultObserver =
    Arc<dyn Fn() -> Result<(), ObserverError> + Send + Sync>;

#[cfg(target_os = "linux")]
struct BrokerEndpoints {
    parent: tokio::net::UnixStream,
    child: std::os::unix::net::UnixStream,
    on_fault: Option<ProcessBrokerFaultObserver>,
}

enum ProcessInput {
    Static,
    Controlled(tokio::sync::mpsc::Receiver<ControlledInput>),
    #[cfg(target_os = "linux")]
    Brokered {
        input: BrokeredInputReceiver,
        control: BrokerEndpoints,
    },
}

struct SupervisionIo {
    observer: Option<ProcessObserver>,
    on_start: Option<ProcessStartObserver>,
    pinned_directory: Option<File>,
    input: ProcessInput,
}

/// Supplies a bounded continuation of stdin to a trusted interactive child.
/// Dropping the channel without `Finish` interrupts input rather than reporting complete delivery.
pub enum ControlledInput {
    /// Writes one frame under the aggregate stdin limit.
    Bytes(Vec<u8>),
    /// Reports when all frame bytes have been written to the OS pipe, not when the child has consumed them.
    Confirmed {
        bytes: Vec<u8>,
        written: tokio::sync::oneshot::Sender<()>,
    },
    /// Closes stdin and marks controlled input complete.
    Finish,
}

const MAX_BROKERED_FRAME_BYTES: usize = 16 * 1024 * 1024;
#[cfg(target_os = "linux")]
const _: () = assert!(broker_protocol::MAX_FRAME as usize == MAX_BROKERED_FRAME_BYTES);

#[derive(Default)]
struct BrokerInputState {
    output_closed: bool,
    protocol_faulted: bool,
    output_observations: usize,
    protocol_holds_input: bool,
    protocol_transition: bool,
    observation_done: Arc<tokio::sync::Notify>,
    protocol_observation_done: Arc<tokio::sync::Notify>,
    transition_done: Arc<tokio::sync::Notify>,
    finish_queued: bool,
    next_order: u64,
    outstanding_bytes: usize,
    max_outstanding_bytes: usize,
}

struct BrokeredReservation {
    _slot: tokio::sync::OwnedSemaphorePermit,
    state: Arc<Mutex<BrokerInputState>>,
    stopped: CancellationToken,
    bytes: usize,
}

impl Drop for BrokeredReservation {
    fn drop(&mut self) {
        let valid = match self.state.lock() {
            Ok(mut state) => match state.outstanding_bytes.checked_sub(self.bytes) {
                Some(remaining) => {
                    state.outstanding_bytes = remaining;
                    true
                }
                None => false,
            },
            Err(_) => false,
        };
        if !valid {
            let _ =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.stopped.cancel()));
        }
    }
}

struct BrokeredMessage {
    order: u64,
    input: ControlledInput,
    reservation: BrokeredReservation,
}

struct BrokerFinishStop(CancellationToken);
impl Drop for BrokerFinishStop {
    fn drop(&mut self) {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.0.cancel()));
    }
}

#[cfg(test)]
pub(crate) enum BrokerTransitionAdmission {
    Ready(BrokerTransition),
    Wait,
}

#[cfg(test)]
pub(crate) struct BrokerTransition {
    state: Arc<Mutex<BrokerInputState>>,
    notify: Arc<tokio::sync::Notify>,
    writer_notify: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
impl Drop for BrokerTransition {
    fn drop(&mut self) {
        if let Ok(mut state) = self.state.lock() {
            state.protocol_transition = false;
        }
        self.notify.notify_waiters();
        self.writer_notify.notify_one();
    }
}

/// Reports that a broker input channel is full, closed, or no longer accepts frames.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BrokerInputError {
    Full,
    Closed,
    Stopped,
    InputLimit,
}

/// Sends brokered frames while ordering `Finish` against observed stdout EOF.
#[derive(Clone)]
pub struct BrokeredInputSender {
    input: tokio::sync::mpsc::Sender<BrokeredMessage>,
    state: Arc<Mutex<BrokerInputState>>,
    stopped: CancellationToken,
    slots: Arc<tokio::sync::Semaphore>,
}

/// Receives frames and the associated stdout EOF ordering state.
pub struct BrokeredInputReceiver {
    input: tokio::sync::mpsc::Receiver<BrokeredMessage>,
    state: Arc<Mutex<BrokerInputState>>,
    stopped: CancellationToken,
    expected: u64,
    pending: BTreeMap<u64, BrokeredMessage>,
    max_pending: usize,
}

/// Creates a broker channel bounded by outstanding frame count and payload lengths.
/// The initial process input and allocated `Vec` capacity are separate from this limit.
pub fn brokered_input_channel(
    capacity: usize,
    max_outstanding_bytes: usize,
) -> (BrokeredInputSender, BrokeredInputReceiver) {
    let (sender, receiver) = tokio::sync::mpsc::channel(capacity);
    let state = Arc::new(Mutex::new(BrokerInputState {
        max_outstanding_bytes,
        ..Default::default()
    }));
    let stopped = CancellationToken::new();
    let slots = Arc::new(tokio::sync::Semaphore::new(capacity));
    (
        BrokeredInputSender {
            input: sender,
            state: Arc::clone(&state),
            stopped: stopped.clone(),
            slots,
        },
        BrokeredInputReceiver {
            input: receiver,
            state,
            stopped,
            expected: 0,
            pending: BTreeMap::new(),
            max_pending: capacity,
        },
    )
}

impl BrokeredInputSender {
    fn ensure_open(&self) -> Result<(), BrokerInputError> {
        let state = self.state.lock().map_err(|_| BrokerInputError::Stopped)?;
        if state.output_closed || state.protocol_faulted || state.finish_queued {
            Err(BrokerInputError::Stopped)
        } else {
            Ok(())
        }
    }

    /// Prevents new broker writes after a trusted protocol observer detects a fault.
    // Protocol hooks have no production adapter caller; they stay test-only until one exists.
    #[cfg(test)]
    pub(crate) fn stop_before_write(&self) {
        let notify = if let Ok(mut state) = self.state.lock() {
            state.protocol_faulted = true;
            Some((
                Arc::clone(&state.observation_done),
                Arc::clone(&state.protocol_observation_done),
            ))
        } else {
            None
        };
        if let Some((writer, protocol)) = notify {
            writer.notify_one();
            protocol.notify_one();
        }
    }

    #[cfg(test)]
    pub(crate) fn observation_barrier(
        &self,
    ) -> Result<BrokerTransitionAdmission, BrokerInputError> {
        let mut state = self.state.lock().map_err(|_| BrokerInputError::Stopped)?;
        if state.output_closed || state.protocol_faulted || state.protocol_transition {
            return Err(BrokerInputError::Stopped);
        }
        if state.output_observations > 0 {
            return Ok(BrokerTransitionAdmission::Wait);
        }
        state.protocol_transition = true;
        Ok(BrokerTransitionAdmission::Ready(BrokerTransition {
            state: Arc::clone(&self.state),
            notify: Arc::clone(&state.transition_done),
            writer_notify: Arc::clone(&state.observation_done),
        }))
    }

    #[cfg(test)]
    pub(crate) fn hold_before_write(&self, hold: bool) {
        let notify = if let Ok(mut state) = self.state.lock() {
            state.protocol_holds_input = hold;
            (!hold).then(|| Arc::clone(&state.observation_done))
        } else {
            None
        };
        if let Some(notify) = notify {
            notify.notify_one();
        }
    }

    fn admit(&self, input: &ControlledInput) -> Result<(u64, bool, usize), BrokerInputError> {
        let mut state = self.state.lock().map_err(|_| BrokerInputError::Stopped)?;
        if state.output_closed || state.protocol_faulted || state.finish_queued {
            return Err(BrokerInputError::Stopped);
        }
        let bytes = match input {
            ControlledInput::Bytes(bytes) | ControlledInput::Confirmed { bytes, .. } => bytes.len(),
            ControlledInput::Finish => 0,
        };
        if bytes > MAX_BROKERED_FRAME_BYTES || bytes > state.max_outstanding_bytes {
            return Err(BrokerInputError::InputLimit);
        }
        let outstanding = state
            .outstanding_bytes
            .checked_add(bytes)
            .ok_or(BrokerInputError::InputLimit)?;
        if outstanding > state.max_outstanding_bytes {
            return Err(BrokerInputError::Full);
        }
        // Tickets preserve admission order even when senders are preempted before enqueueing.
        let order = state.next_order;
        state.next_order = state
            .next_order
            .checked_add(1)
            .ok_or(BrokerInputError::Stopped)?;
        let finish = matches!(input, ControlledInput::Finish);
        if finish {
            state.finish_queued = true;
        }
        state.outstanding_bytes = outstanding;
        Ok((order, finish, bytes))
    }

    fn commit(
        &self,
        permit: tokio::sync::mpsc::Permit<'_, BrokeredMessage>,
        slot: tokio::sync::OwnedSemaphorePermit,
        input: ControlledInput,
    ) -> Result<(), BrokerInputError> {
        let (order, finish, bytes) = self.admit(&input)?;
        let reservation = BrokeredReservation {
            _slot: slot,
            state: Arc::clone(&self.state),
            stopped: self.stopped.clone(),
            bytes,
        };
        let stop = finish.then(|| BrokerFinishStop(self.stopped.clone()));
        permit.send(BrokeredMessage {
            order,
            input,
            reservation,
        });
        drop(stop);
        Ok(())
    }

    /// Sends a frame only while output is open and `Finish` has not been queued.
    pub async fn send(&self, input: ControlledInput) -> Result<(), BrokerInputError> {
        self.ensure_open()?;
        let slot = tokio::select! {
            biased;
            _ = self.stopped.cancelled() => return Err(BrokerInputError::Stopped),
            slot = Arc::clone(&self.slots).acquire_owned() => slot.map_err(|_| BrokerInputError::Closed)?,
        };
        let permit = tokio::select! {
            biased;
            _ = self.stopped.cancelled() => return Err(BrokerInputError::Stopped),
            permit = self.input.reserve() => permit.map_err(|_| BrokerInputError::Closed)?,
        };
        self.commit(permit, slot, input)
    }

    /// Non-blocking form of `send` for a process output observer.
    pub fn try_send(&self, input: ControlledInput) -> Result<(), BrokerInputError> {
        self.ensure_open()?;
        let slot = Arc::clone(&self.slots)
            .try_acquire_owned()
            .map_err(|error| match error {
                tokio::sync::TryAcquireError::NoPermits => BrokerInputError::Full,
                tokio::sync::TryAcquireError::Closed => BrokerInputError::Closed,
            })?;
        let permit = self.input.try_reserve().map_err(|error| match error {
            tokio::sync::mpsc::error::TrySendError::Full(_) => BrokerInputError::Full,
            tokio::sync::mpsc::error::TrySendError::Closed(_) => BrokerInputError::Closed,
        })?;
        self.commit(permit, slot, input)
    }

    /// Waits until the receiver has stopped accepting frames.
    pub async fn closed(&self) {
        tokio::select! {
            _ = self.stopped.cancelled() => {},
            _ = self.input.closed() => {},
        }
    }
}

impl BrokeredInputReceiver {
    async fn recv(&mut self) -> Option<BrokeredMessage> {
        loop {
            if let Some(message) = self.pending.remove(&self.expected) {
                self.expected = self.expected.checked_add(1)?;
                return Some(message);
            }
            let message = self.input.recv().await?;
            if message.order < self.expected {
                return None;
            }
            if message.order == self.expected {
                self.expected = self.expected.checked_add(1)?;
                return Some(message);
            }
            if self.pending.len() >= self.max_pending {
                return None;
            }
            if self.pending.insert(message.order, message).is_some() {
                return None;
            }
        }
    }
}

/// A byte-preserving bounded stream capture and its completeness observation.
pub struct CapturedOutput {
    pub bytes: Vec<u8>,
    pub completion: OutputCompletion,
    /// The final line is unterminated when this count is nonzero at EOF.
    pub trailing_line_bytes: usize,
}

impl fmt::Debug for CapturedOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CapturedOutput")
            .field("bytes", &self.bytes.len())
            .field("completion", &self.completion)
            .field("trailing_line_bytes", &self.trailing_line_bytes)
            .finish()
    }
}

/// Describes signal attempts without claiming that descendants or vendor state are settled.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SignalObservation {
    pub graceful_attempted: bool,
    pub graceful_error: Option<io::ErrorKind>,
    pub force_attempted: bool,
    pub force_error: Option<io::ErrorKind>,
}

/// Contains observations from the owned process and its captured pipes.
#[derive(Debug)]
pub struct ProcessReport {
    pub identity: ProcessIdentity,
    pub reason: StopReason,
    pub exit: Option<ExitObservation>,
    pub input: InputCompletion,
    pub stdout: CapturedOutput,
    pub stderr: CapturedOutput,
    pub signals: SignalObservation,
}

impl ProcessReport {
    /// Only the direct child and captured pipes are covered by this observation.
    /// It does not prove that detached descendants or native session state are safe.
    pub fn direct_io_settled(&self) -> bool {
        self.exit.is_some()
            && self.stdout.completion == OutputCompletion::Eof
            && self.stderr.completion == OutputCompletion::Eof
            && self.input != InputCompletion::TimedOut
    }
}

/// Reports setup failures without including a command, environment, or input contents.
#[derive(Debug, PartialEq, Eq)]
pub enum ProcessError {
    InvalidSpec(&'static str),
    UnsupportedPlatform,
    CancelledBeforeSpawn,
    DeadlineBeforeSpawn,
    Setup(io::ErrorKind),
    IdentityUnavailable(io::ErrorKind),
    Spawn(io::ErrorKind),
    /// The process may have started, but its completion could not be observed.
    StartedUnsettled {
        pid: u32,
        kind: io::ErrorKind,
    },
    SupervisorFailed,
}

impl fmt::Display for ProcessError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSpec(field) => write!(f, "invalid process specification: {field}"),
            Self::UnsupportedPlatform => {
                f.write_str("process supervision is unavailable on this platform")
            }
            Self::CancelledBeforeSpawn => f.write_str("process was cancelled before spawn"),
            Self::DeadlineBeforeSpawn => f.write_str("process deadline elapsed before spawn"),
            Self::Setup(kind) => write!(f, "process setup failed: {kind:?}"),
            Self::IdentityUnavailable(kind) => {
                write!(f, "process identity is unavailable: {kind:?}")
            }
            Self::Spawn(kind) => write!(f, "process spawn failed: {kind:?}"),
            Self::StartedUnsettled { pid, kind } => {
                write!(
                    f,
                    "process {pid} may have started without an observed completion: {kind:?}"
                )
            }
            Self::SupervisorFailed => f.write_str("process supervisor failed"),
        }
    }
}

impl std::error::Error for ProcessError {}

impl ProcessSpec {
    fn validate(&self) -> Result<(), ProcessError> {
        let limits = self.limits;
        if !self.program.is_absolute() || self.cwd.as_ref().is_some_and(|path| !path.is_absolute())
        {
            return Err(ProcessError::InvalidSpec("absolute paths"));
        }
        if limits.max_args == 0
            || limits.max_env == 0
            || limits.max_spec_bytes == 0
            || limits.max_stdin_bytes == 0
            || limits.max_stdout_bytes == 0
            || limits.max_stderr_bytes == 0
            || limits.max_line_bytes == 0
            || limits.timeout.is_zero()
            || limits.signal_grace.is_zero()
            || limits.kill_grace.is_zero()
            || limits.pipe_grace.is_zero()
            || Instant::now().checked_add(limits.timeout).is_none()
            || Instant::now().checked_add(limits.signal_grace).is_none()
            || Instant::now().checked_add(limits.kill_grace).is_none()
            || Instant::now().checked_add(limits.pipe_grace).is_none()
            || limits
                .max_stdout_bytes
                .checked_add(limits.max_stderr_bytes)
                .is_none()
        {
            return Err(ProcessError::InvalidSpec("limits"));
        }
        if self.args.len() > limits.max_args
            || self.env.len() > limits.max_env
            || self.stdin.len() > limits.max_stdin_bytes
        {
            return Err(ProcessError::InvalidSpec("input limit"));
        }
        let mut bytes = self.program.as_os_str().as_encoded_bytes().len();
        let mut add = |value: &std::ffi::OsStr| -> Result<(), ProcessError> {
            let value = value.as_encoded_bytes();
            if value.contains(&0) {
                return Err(ProcessError::InvalidSpec("NUL byte"));
            }
            bytes = bytes
                .checked_add(value.len())
                .ok_or(ProcessError::InvalidSpec("size"))?;
            Ok(())
        };
        if self.program.as_os_str().as_encoded_bytes().contains(&0) {
            return Err(ProcessError::InvalidSpec("NUL byte"));
        }
        if let Some(cwd) = &self.cwd {
            add(cwd.as_os_str())?;
        }
        for arg in &self.args {
            add(arg)?;
        }
        let mut keys = HashSet::new();
        for (key, value) in &self.env {
            let raw = key.as_encoded_bytes();
            if raw.is_empty() || raw.contains(&b'=') || !keys.insert(key) {
                return Err(ProcessError::InvalidSpec("environment key"));
            }
            add(key)?;
            add(value)?;
        }
        if bytes > limits.max_spec_bytes {
            return Err(ProcessError::InvalidSpec("specification size"));
        }
        Ok(())
    }
}

/// Runs a trusted command without a shell and with bounded pipe collection.
pub struct ProcessSupervisor;

impl ProcessSupervisor {
    /// Cancellation or dropping this future requests cleanup in an owned background task.
    /// The runtime must remain alive for that background cleanup to make progress.
    pub async fn run(
        spec: ProcessSpec,
        cancellation: CancellationToken,
    ) -> Result<ProcessReport, ProcessError> {
        Self::run_with_observer(spec, cancellation, None).await
    }

    /// Streams accepted pipe bytes to a non-blocking observer while retaining bounded captures.
    pub async fn run_with_observer(
        spec: ProcessSpec,
        cancellation: CancellationToken,
        observer: Option<ProcessObserver>,
    ) -> Result<ProcessReport, ProcessError> {
        Self::run_with_start_observer(spec, cancellation, observer, None, None).await
    }

    pub(crate) async fn run_with_start_observer(
        spec: ProcessSpec,
        cancellation: CancellationToken,
        observer: Option<ProcessObserver>,
        on_start: Option<ProcessStartObserver>,
        pinned_directory: Option<File>,
    ) -> Result<ProcessReport, ProcessError> {
        Self::run_inner(
            spec,
            cancellation,
            observer,
            on_start,
            pinned_directory,
            ProcessInput::Static,
        )
        .await
    }

    /// Runs a trusted process with bounded, explicitly finished incremental stdin.
    /// The initial `ProcessSpec.stdin` and all subsequent frames share `max_stdin_bytes`.
    /// The caller must use a bounded channel and avoid sending sensitive frames before its protocol permits them.
    pub async fn run_interactive(
        spec: ProcessSpec,
        cancellation: CancellationToken,
        observer: Option<ProcessObserver>,
        input: tokio::sync::mpsc::Receiver<ControlledInput>,
    ) -> Result<ProcessReport, ProcessError> {
        Self::run_with_controlled_input(spec, cancellation, observer, None, None, input).await
    }

    pub(crate) async fn run_with_controlled_input(
        spec: ProcessSpec,
        cancellation: CancellationToken,
        observer: Option<ProcessObserver>,
        on_start: Option<ProcessStartObserver>,
        pinned_directory: Option<File>,
        input: tokio::sync::mpsc::Receiver<ControlledInput>,
    ) -> Result<ProcessReport, ProcessError> {
        Self::run_inner(
            spec,
            cancellation,
            observer,
            on_start,
            pinned_directory,
            ProcessInput::Controlled(input),
        )
        .await
    }

    /// Runs a trusted helper with a dedicated status channel and framed stdin forwarding.
    /// `Confirmed` acknowledges writes to the CLI-facing pipe, not CLI consumption.
    #[cfg(target_os = "linux")]
    pub async fn run_interactive_brokered(
        spec: ProcessSpec,
        cancellation: CancellationToken,
        observer: Option<ProcessObserver>,
        input: BrokeredInputReceiver,
    ) -> Result<ProcessReport, ProcessError> {
        Self::run_with_brokered_input(spec, cancellation, observer, None, None, input, None).await
    }

    #[cfg(target_os = "linux")]
    pub(crate) async fn run_with_brokered_input(
        mut spec: ProcessSpec,
        cancellation: CancellationToken,
        observer: Option<ProcessObserver>,
        on_start: Option<ProcessStartObserver>,
        pinned_directory: Option<File>,
        input: BrokeredInputReceiver,
        on_fault: Option<ProcessBrokerFaultObserver>,
    ) -> Result<ProcessReport, ProcessError> {
        use std::{os::fd::AsRawFd, os::unix::net::UnixStream as StdUnixStream};
        if spec.args.first().map(OsString::as_os_str) != Some(std::ffi::OsStr::new("--")) {
            return Err(ProcessError::InvalidSpec("broker helper arguments"));
        }
        if spec.stdin.len() > broker_protocol::MAX_FRAME as usize {
            return Err(ProcessError::InvalidSpec("broker input limit"));
        }
        let max_queued = input
            .state
            .lock()
            .map_err(|_| ProcessError::InvalidSpec("broker input state"))?
            .max_outstanding_bytes;
        if max_queued > spec.limits.max_stdin_bytes {
            return Err(ProcessError::InvalidSpec("broker queue limit"));
        }
        let (parent, child) =
            StdUnixStream::pair().map_err(|error| ProcessError::Setup(error.kind()))?;
        let outside_stdio = |stream: StdUnixStream| -> Result<StdUnixStream, ProcessError> {
            if stream.as_raw_fd() > 2 {
                Ok(stream)
            } else {
                let fd = rustix::io::fcntl_dupfd_cloexec(&stream, 3).map_err(|error| {
                    ProcessError::Setup(io::Error::from_raw_os_error(error.raw_os_error()).kind())
                })?;
                Ok(StdUnixStream::from(fd))
            }
        };
        let parent = outside_stdio(parent)?;
        let child = outside_stdio(child)?;
        parent
            .set_nonblocking(true)
            .map_err(|error| ProcessError::Setup(error.kind()))?;
        let parent = tokio::net::UnixStream::from_std(parent)
            .map_err(|error| ProcessError::Setup(error.kind()))?;
        spec.args.insert(0, child.as_raw_fd().to_string().into());
        spec.args.insert(0, "--framed-stdin".into());
        Self::run_inner(
            spec,
            cancellation,
            observer,
            on_start,
            pinned_directory,
            ProcessInput::Brokered {
                input,
                control: BrokerEndpoints {
                    parent,
                    child,
                    on_fault,
                },
            },
        )
        .await
    }

    async fn run_inner(
        spec: ProcessSpec,
        cancellation: CancellationToken,
        observer: Option<ProcessObserver>,
        on_start: Option<ProcessStartObserver>,
        pinned_directory: Option<File>,
        input: ProcessInput,
    ) -> Result<ProcessReport, ProcessError> {
        let started = Instant::now();
        spec.validate()?;
        if !cfg!(target_os = "linux") {
            return Err(ProcessError::UnsupportedPlatform);
        }
        if cancellation.is_cancelled() {
            return Err(ProcessError::CancelledBeforeSpawn);
        }
        let deadline = started
            .checked_add(spec.limits.timeout)
            .ok_or(ProcessError::InvalidSpec("timeout"))?;
        if Instant::now() >= deadline {
            return Err(ProcessError::DeadlineBeforeSpawn);
        }
        let stop_on_drop = CancellationToken::new();
        let guard = CancelOnDrop(stop_on_drop.clone());
        let task = tokio::spawn(supervise(
            spec,
            cancellation,
            stop_on_drop,
            deadline,
            SupervisionIo {
                observer,
                on_start,
                pinned_directory,
                input,
            },
        ));
        let result = task.await.map_err(|_| ProcessError::SupervisorFailed)?;
        drop(guard);
        result
    }
}

struct CancelOnDrop(CancellationToken);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

#[cfg(target_os = "linux")]
#[derive(Clone, Copy)]
enum Fault {
    Output(OutputStream, OutputCompletion),
    Input,
}

#[cfg(target_os = "linux")]
struct CaptureState {
    bytes: Vec<u8>,
    completion: Option<OutputCompletion>,
    trailing: usize,
}

#[cfg(target_os = "linux")]
struct ReaderContext {
    stream: OutputStream,
    max_bytes: usize,
    max_line: usize,
    capture: Arc<Mutex<CaptureState>>,
    events: mpsc::Sender<Fault>,
    liveness: CancellationToken,
    input_stop: CancellationToken,
    payload_stop: CancellationToken,
    input_finished: CancellationToken,
    atomic_observation: bool,
    brokered: bool,
    broker_state: Option<Arc<Mutex<BrokerInputState>>>,
    broker_stopped: Option<CancellationToken>,
    observer: Option<ProcessObserver>,
}

#[cfg(target_os = "linux")]
async fn read_brokered_stream<R: AsyncRead + Unpin>(
    pipe: &mut R,
    buffer: &mut [u8],
    gate: &Mutex<BrokerInputState>,
    stopped: &CancellationToken,
    payload_stop: &CancellationToken,
    stream: OutputStream,
) -> io::Result<usize> {
    use std::{pin::Pin, task::Poll};
    let transition_done = Arc::clone(
        &gate
            .lock()
            .map_err(|_| io::Error::other("broker input state"))?
            .transition_done,
    );
    loop {
        let notified = transition_done.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let result = std::future::poll_fn(|context| {
            let mut state = match gate.lock() {
                Ok(state) => state,
                Err(_) => return Poll::Ready(Err(io::Error::other("broker input state"))),
            };
            if state.protocol_transition {
                return Poll::Ready(Ok(None));
            }
            // Read admission and protocol transitions share the gate.
            let mut read = tokio::io::ReadBuf::new(&mut *buffer);
            match Pin::new(&mut *pipe).poll_read(context, &mut read) {
                Poll::Ready(Ok(())) => {
                    let count = read.filled().len();
                    if count == 0 && stream == OutputStream::Stdout {
                        state.output_closed = true;
                    } else if count > 0 {
                        let Some(observations) = state.output_observations.checked_add(1) else {
                            state.protocol_faulted = true;
                            return Poll::Ready(Err(io::Error::other("broker observation limit")));
                        };
                        state.output_observations = observations;
                    }
                    let protocol_notify = (count == 0 && stream == OutputStream::Stdout)
                        .then(|| Arc::clone(&state.protocol_observation_done));
                    drop(state);
                    if let Some(protocol_notify) = protocol_notify {
                        stopped.cancel();
                        payload_stop.cancel();
                        protocol_notify.notify_one();
                    }
                    Poll::Ready(Ok(Some(count)))
                }
                Poll::Ready(Err(error)) => {
                    state.protocol_faulted = true;
                    let writer = Arc::clone(&state.observation_done);
                    let protocol = Arc::clone(&state.protocol_observation_done);
                    drop(state);
                    writer.notify_one();
                    protocol.notify_one();
                    Poll::Ready(Err(error))
                }
                Poll::Pending => Poll::Pending,
            }
        })
        .await?;
        if let Some(count) = result {
            return Ok(count);
        }
        notified.await;
    }
}

#[cfg(target_os = "linux")]
struct BrokerObservation {
    state: Arc<Mutex<BrokerInputState>>,
    notify: Arc<tokio::sync::Notify>,
    protocol_notify: Arc<tokio::sync::Notify>,
    finished: bool,
}

#[cfg(target_os = "linux")]
impl BrokerObservation {
    fn new(state: Arc<Mutex<BrokerInputState>>) -> Result<Self, io::ErrorKind> {
        let (notify, protocol_notify) = {
            let locked = state.lock().map_err(|_| io::ErrorKind::InvalidData)?;
            (
                Arc::clone(&locked.observation_done),
                Arc::clone(&locked.protocol_observation_done),
            )
        };
        Ok(Self {
            state,
            notify,
            protocol_notify,
            finished: false,
        })
    }

    fn finish(&mut self, success: bool) {
        if self.finished {
            return;
        }
        if let Ok(mut state) = self.state.lock() {
            match state.output_observations.checked_sub(1) {
                Some(remaining) => state.output_observations = remaining,
                None => state.protocol_faulted = true,
            }
            state.protocol_faulted |= !success;
        }
        self.finished = true;
        self.notify.notify_one();
        self.protocol_notify.notify_one();
    }
}

#[cfg(target_os = "linux")]
impl Drop for BrokerObservation {
    fn drop(&mut self) {
        self.finish(false);
    }
}

#[cfg(target_os = "linux")]
async fn read_pipe<R: AsyncRead + Unpin>(mut pipe: R, context: ReaderContext) {
    let ReaderContext {
        stream,
        max_bytes,
        max_line,
        capture,
        events,
        liveness,
        input_stop,
        payload_stop,
        input_finished,
        atomic_observation,
        brokered,
        broker_state,
        broker_stopped,
        observer,
    } = context;
    let mut buf = [0_u8; 8192];
    loop {
        let read = tokio::select! {
            _ = liveness.cancelled() => {
                if let Ok(mut state) = capture.lock() { state.completion = Some(OutputCompletion::Interrupted); }
                return;
            }
            result = async {
                if brokered {
                    let state = broker_state.as_ref().ok_or(io::ErrorKind::InvalidInput)?;
                    let stopped = broker_stopped.as_ref().ok_or(io::ErrorKind::InvalidInput)?;
                    read_brokered_stream(&mut pipe, &mut buf, state, stopped, &payload_stop, stream).await
                } else {
                    pipe.read(&mut buf).await
                }
            } => result,
        };
        let mut observation = if brokered && matches!(read, Ok(count) if count > 0) {
            match broker_state
                .as_ref()
                .and_then(|state| BrokerObservation::new(Arc::clone(state)).ok())
            {
                Some(observation) => Some(observation),
                None => {
                    input_stop.cancel();
                    let _ =
                        events.try_send(Fault::Output(stream, OutputCompletion::ObserverFailure));
                    return;
                }
            }
        } else {
            None
        };
        let mut state = match capture.lock() {
            Ok(state) => state,
            Err(_) => return,
        };
        let count = match read {
            Ok(0) => {
                state.completion = Some(OutputCompletion::Eof);
                if atomic_observation
                    && stream == OutputStream::Stdout
                    && !input_finished.is_cancelled()
                    && !brokered
                {
                    input_stop.cancel();
                    let _ = events.try_send(Fault::Output(stream, OutputCompletion::Eof));
                }
                return;
            }
            Ok(n) => n,
            Err(error) => {
                let kind = OutputCompletion::Failed(error.kind());
                state.completion = Some(kind);
                input_stop.cancel();
                let _ = events.try_send(Fault::Output(stream, kind));
                return;
            }
        };
        let mut accepted = 0;
        let mut fault = None;
        for byte in &buf[..count] {
            fault = if state.bytes.len() == max_bytes {
                Some(OutputCompletion::OutputLimit)
            } else if *byte != b'\n' && state.trailing == max_line {
                Some(OutputCompletion::LineLimit)
            } else {
                None
            };
            if let Some(fault) = fault {
                state.completion = Some(fault);
                break;
            }
            state.bytes.push(*byte);
            accepted += 1;
            if *byte == b'\n' {
                state.trailing = 0;
            } else {
                state.trailing += 1;
            }
        }
        drop(state);
        if atomic_observation && let Some(fault) = fault {
            input_stop.cancel();
            let _ = events.try_send(Fault::Output(stream, fault));
            return;
        }
        if accepted > 0
            && let Some(observer) = &observer
        {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                observer(stream, &buf[..accepted])
            }));
            if !result.is_ok_and(|result| result.is_ok()) {
                if let Ok(mut state) = capture.lock() {
                    state.completion = Some(OutputCompletion::ObserverFailure);
                }
                input_stop.cancel();
                let _ = events.try_send(Fault::Output(stream, OutputCompletion::ObserverFailure));
                return;
            }
        }
        if let Some(fault) = fault {
            input_stop.cancel();
            let _ = events.try_send(Fault::Output(stream, fault));
            return;
        }
        if let Some(observation) = &mut observation {
            observation.finish(true);
        }
    }
}

#[cfg(target_os = "linux")]
struct InputCancellation {
    stop: CancellationToken,
    payload_stop: CancellationToken,
    caller: CancellationToken,
    dropped: CancellationToken,
    deadline: Instant,
    finished: CancellationToken,
}

#[cfg(target_os = "linux")]
async fn write_input<W: AsyncWrite + Unpin>(
    mut stdin: W,
    bytes: Vec<u8>,
    events: mpsc::Sender<Fault>,
    cancellation: InputCancellation,
) -> InputCompletion {
    if cancellation.caller.is_cancelled()
        || cancellation.dropped.is_cancelled()
        || cancellation.stop.is_cancelled()
        || Instant::now() >= cancellation.deadline
    {
        return InputCompletion::Interrupted;
    }
    let result = tokio::select! {
        biased;
        _ = cancellation.caller.cancelled() => return InputCompletion::Interrupted,
        _ = cancellation.dropped.cancelled() => return InputCompletion::Interrupted,
        _ = cancellation.stop.cancelled() => return InputCompletion::Interrupted,
        _ = tokio::time::sleep_until(cancellation.deadline) => return InputCompletion::Interrupted,
        result = async { stdin.write_all(&bytes).await?; stdin.shutdown().await } => result,
    };
    match result {
        Ok(()) => InputCompletion::Complete,
        Err(error) => {
            let _ = events.try_send(Fault::Input);
            InputCompletion::Failed(error.kind())
        }
    }
}

#[cfg(target_os = "linux")]
async fn write_controlled_input<W: AsyncWrite + Unpin>(
    mut stdin: W,
    initial: Vec<u8>,
    mut input: mpsc::Receiver<ControlledInput>,
    max_bytes: usize,
    events: mpsc::Sender<Fault>,
    cancellation: InputCancellation,
) -> InputCompletion {
    let mut next = Some(ControlledInput::Bytes(initial));
    let mut written = 0usize;
    loop {
        let command = if let Some(command) = next.take() {
            Some(command)
        } else {
            tokio::select! {
                biased;
                _ = cancellation.caller.cancelled() => return InputCompletion::Interrupted,
                _ = cancellation.dropped.cancelled() => return InputCompletion::Interrupted,
                _ = cancellation.stop.cancelled() => return InputCompletion::Interrupted,
                _ = tokio::time::sleep_until(cancellation.deadline) => return InputCompletion::Interrupted,
                command = input.recv() => command,
            }
        };
        let Some(command) = command else {
            let _ = events.try_send(Fault::Input);
            return InputCompletion::Interrupted;
        };
        if cancellation.caller.is_cancelled()
            || cancellation.dropped.is_cancelled()
            || cancellation.stop.is_cancelled()
            || Instant::now() >= cancellation.deadline
        {
            return InputCompletion::Interrupted;
        }
        let (bytes, confirmation) = match command {
            ControlledInput::Bytes(bytes) => (bytes, None),
            ControlledInput::Confirmed { bytes, written } => (bytes, Some(written)),
            ControlledInput::Finish => {
                let result = tokio::select! {
                    biased;
                    _ = cancellation.caller.cancelled() => return InputCompletion::Interrupted,
                    _ = cancellation.dropped.cancelled() => return InputCompletion::Interrupted,
                    _ = cancellation.stop.cancelled() => return InputCompletion::Interrupted,
                    _ = tokio::time::sleep_until(cancellation.deadline) => return InputCompletion::Interrupted,
                    result = stdin.shutdown() => result,
                };
                return match result {
                    Ok(()) => {
                        cancellation.finished.cancel();
                        InputCompletion::Complete
                    }
                    Err(error) => {
                        let _ = events.try_send(Fault::Input);
                        InputCompletion::Failed(error.kind())
                    }
                };
            }
        };
        let Some(total) = written
            .checked_add(bytes.len())
            .filter(|total| *total <= max_bytes)
        else {
            let _ = events.try_send(Fault::Input);
            return InputCompletion::Failed(io::ErrorKind::InvalidInput);
        };
        let result = tokio::select! {
            biased;
            _ = cancellation.caller.cancelled() => return InputCompletion::Interrupted,
            _ = cancellation.dropped.cancelled() => return InputCompletion::Interrupted,
            _ = cancellation.stop.cancelled() => return InputCompletion::Interrupted,
            _ = tokio::time::sleep_until(cancellation.deadline) => return InputCompletion::Interrupted,
            result = stdin.write_all(&bytes) => result,
        };
        if let Err(error) = result {
            let _ = events.try_send(Fault::Input);
            return InputCompletion::Failed(error.kind());
        }
        written = total;
        if let Some(confirmation) = confirmation {
            let _ = confirmation.send(());
        }
    }
}

#[cfg(target_os = "linux")]
fn capture_snapshot(
    state: &Arc<Mutex<CaptureState>>,
    fallback: OutputCompletion,
) -> CapturedOutput {
    match state.lock() {
        Ok(mut state) => CapturedOutput {
            bytes: std::mem::take(&mut state.bytes),
            completion: state.completion.unwrap_or(fallback),
            trailing_line_bytes: state.trailing,
        },
        Err(_) => CapturedOutput {
            bytes: Vec::new(),
            completion: OutputCompletion::Failed(io::ErrorKind::Other),
            trailing_line_bytes: 0,
        },
    }
}

#[cfg(target_os = "linux")]
fn reason_for_fault(fault: Fault) -> StopReason {
    match fault {
        Fault::Input => StopReason::InputFailure,
        Fault::Output(stream, OutputCompletion::OutputLimit) => StopReason::OutputLimit(stream),
        Fault::Output(stream, OutputCompletion::LineLimit) => StopReason::LineLimit(stream),
        Fault::Output(stream, OutputCompletion::ObserverFailure) => {
            StopReason::ObserverFailure(stream)
        }
        Fault::Output(stream, _) => StopReason::OutputFailure(stream),
    }
}

#[cfg(target_os = "linux")]
fn graceful_signal(signal: StopSignal) -> rustix::process::Signal {
    match signal {
        StopSignal::Interrupt => rustix::process::Signal::INT,
        StopSignal::Terminate => rustix::process::Signal::TERM,
    }
}
#[cfg(target_os = "linux")]
fn exit_observation(status: std::process::ExitStatus) -> ExitObservation {
    #[cfg(unix)]
    let signal = {
        use std::os::unix::process::ExitStatusExt;
        status.signal()
    };
    #[cfg(not(unix))]
    let signal = None;
    ExitObservation {
        code: status.code(),
        signal,
    }
}

#[cfg(target_os = "linux")]
async fn supervise(
    spec: ProcessSpec,
    cancellation: CancellationToken,
    dropped: CancellationToken,
    deadline: Instant,
    io: SupervisionIo,
) -> Result<ProcessReport, ProcessError> {
    let SupervisionIo {
        observer,
        on_start,
        pinned_directory,
        input,
    } = io;
    let (controlled_input, broker_input, broker_control, broker_state, broker_stopped) = match input
    {
        ProcessInput::Static => (None, None, None, None, None),
        ProcessInput::Controlled(input) => (Some(input), None, None, None, None),
        ProcessInput::Brokered { input, control } => {
            let state = Arc::clone(&input.state);
            let stopped = input.stopped.clone();
            (None, Some(input), Some(control), Some(state), Some(stopped))
        }
    };
    let (broker_parent, broker_child) = match broker_control {
        Some(BrokerEndpoints {
            parent,
            child,
            on_fault,
        }) => (Some((parent, on_fault)), Some(child)),
        None => (None, None),
    };
    let limits = spec.limits;
    if cancellation.is_cancelled() || dropped.is_cancelled() {
        return Err(ProcessError::CancelledBeforeSpawn);
    }
    if Instant::now() >= deadline {
        return Err(ProcessError::DeadlineBeforeSpawn);
    }
    let mut command = Command::new(&spec.program);
    command
        .args(&spec.args)
        .env_clear()
        .envs(spec.env.iter().cloned())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(false);
    if pinned_directory.is_none()
        && let Some(cwd) = &spec.cwd
    {
        command.current_dir(cwd);
    }
    #[cfg(unix)]
    {
        use std::os::{fd::AsRawFd, unix::process::CommandExt};
        command.as_std_mut().process_group(0);
        if let Some(control) = broker_child {
            // SAFETY: fcntl is async-signal-safe and only changes the forked child's descriptor flags.
            unsafe {
                command.as_std_mut().pre_exec(move || {
                    let fd = control.as_raw_fd();
                    let flags = libc::fcntl(fd, libc::F_GETFD);
                    if flags < 0 || libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) < 0 {
                        Err(io::Error::last_os_error())
                    } else {
                        Ok(())
                    }
                });
            }
        }
        if let Some(directory) = pinned_directory {
            // SAFETY: fchdir is async-signal-safe and the opened directory remains owned by the callback until exec.
            unsafe {
                command.as_std_mut().pre_exec(move || {
                    if libc::fchdir(directory.as_raw_fd()) == 0 {
                        Ok(())
                    } else {
                        Err(io::Error::last_os_error())
                    }
                });
            }
        }
    }
    if cancellation.is_cancelled() || dropped.is_cancelled() {
        return Err(ProcessError::CancelledBeforeSpawn);
    }
    if Instant::now() >= deadline {
        return Err(ProcessError::DeadlineBeforeSpawn);
    }
    let (mut child, mut group, pid) = identity::spawn(
        command,
        &cancellation,
        &dropped,
        deadline,
        limits.timeout,
        limits.kill_grace,
    )
    .await?;
    if let Some(on_start) = on_start
        && std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            on_start(ProcessIdentity { pid })
        }))
        .is_err()
    {
        cancellation.cancel();
    }
    let stdin = child.stdin.take().ok_or(ProcessError::SupervisorFailed)?;
    let stdout = child.stdout.take().ok_or(ProcessError::SupervisorFailed)?;
    let stderr = child.stderr.take().ok_or(ProcessError::SupervisorFailed)?;
    let stdout_state = Arc::new(Mutex::new(CaptureState {
        bytes: Vec::new(),
        completion: None,
        trailing: 0,
    }));
    let stderr_state = Arc::new(Mutex::new(CaptureState {
        bytes: Vec::new(),
        completion: None,
        trailing: 0,
    }));
    let liveness = CancellationToken::new();
    let _reader_guard = CancelOnDrop(liveness.clone());
    let input_stop = CancellationToken::new();
    let payload_stop = CancellationToken::new();
    let input_finished = CancellationToken::new();
    let atomic_observation = controlled_input.is_some() || broker_input.is_some();
    let brokered = broker_parent.is_some();
    let (events, mut faults) = mpsc::channel(3);
    let mut out_task = tokio::spawn(read_pipe(
        stdout,
        ReaderContext {
            stream: OutputStream::Stdout,
            max_bytes: limits.max_stdout_bytes,
            max_line: limits.max_line_bytes,
            capture: Arc::clone(&stdout_state),
            events: events.clone(),
            liveness: liveness.clone(),
            input_stop: input_stop.clone(),
            payload_stop: payload_stop.clone(),
            input_finished: input_finished.clone(),
            atomic_observation,
            brokered,
            broker_state: broker_state.clone(),
            broker_stopped: broker_stopped.clone(),
            observer: observer.clone(),
        },
    ));
    let mut err_task = tokio::spawn(read_pipe(
        stderr,
        ReaderContext {
            stream: OutputStream::Stderr,
            max_bytes: limits.max_stderr_bytes,
            max_line: limits.max_line_bytes,
            capture: Arc::clone(&stderr_state),
            events: events.clone(),
            liveness,
            input_stop: input_stop.clone(),
            payload_stop: payload_stop.clone(),
            input_finished: input_finished.clone(),
            atomic_observation,
            brokered,
            broker_state: broker_state.clone(),
            broker_stopped: broker_stopped.clone(),
            observer,
        },
    ));
    let stop_input_on_exit = atomic_observation || !spec.stdin.is_empty();
    let input_cancellation = InputCancellation {
        stop: input_stop.clone(),
        payload_stop: payload_stop.clone(),
        caller: cancellation.clone(),
        dropped: dropped.clone(),
        deadline,
        finished: input_finished.clone(),
    };
    let mut in_task = if let Some(input) = broker_input {
        let (control, on_fault) = broker_parent.ok_or(ProcessError::SupervisorFailed)?;
        tokio::spawn(brokered::write_controlled_input(
            stdin,
            brokered::BrokeredInput {
                control,
                initial: spec.stdin,
                input,
                max_bytes: limits.max_stdin_bytes,
                on_fault,
                state: broker_state.clone().ok_or(ProcessError::SupervisorFailed)?,
            },
            events,
            input_cancellation,
        ))
    } else if let Some(input) = controlled_input {
        tokio::spawn(write_controlled_input(
            stdin,
            spec.stdin,
            input,
            limits.max_stdin_bytes,
            events,
            input_cancellation,
        ))
    } else {
        tokio::spawn(write_input(stdin, spec.stdin, events, input_cancellation))
    };
    let mut exit = None;
    let mut signals = SignalObservation::default();
    let mut reason = tokio::select! {
        biased;
        _ = cancellation.cancelled() => StopReason::Cancelled,
        _ = dropped.cancelled() => StopReason::Cancelled,
        _ = tokio::time::sleep_until(deadline) => StopReason::Deadline,
        Some(fault) = faults.recv() => reason_for_fault(fault),
        result = child.wait() => match result {
            Ok(status) => { exit = Some(exit_observation(status)); StopReason::Exited },
            Err(_) => StopReason::WaitFailure,
        },
    };
    // Empty static input may finish closing after exit; pending data must not flow to descendants.
    if reason != StopReason::Exited {
        input_stop.cancel();
    } else if brokered {
        if let Some(state) = &broker_state {
            match state.lock() {
                Ok(mut state) => state.output_closed = true,
                Err(_) => input_stop.cancel(),
            }
        }
        if let Some(stopped) = &broker_stopped {
            stopped.cancel();
        }
        payload_stop.cancel();
    } else if stop_input_on_exit && !input_finished.is_cancelled() {
        input_stop.cancel();
    }
    let mut signalled = false;
    let mut until = Instant::now()
        .checked_add(limits.pipe_grace)
        .ok_or(ProcessError::InvalidSpec("pipe grace"))?;
    let mut out_done = None;
    let mut err_done = None;
    let mut input_done = None;
    loop {
        if reason == StopReason::Exited
            && let Ok(fault) = faults.try_recv()
        {
            reason = reason_for_fault(fault);
        }
        if reason != StopReason::Exited && !signalled {
            input_stop.cancel();
            signals.graceful_attempted = true;
            signals.graceful_error = group.signal(graceful_signal(spec.stop_signal)).err();
            tokio::time::sleep(limits.signal_grace).await;
            signals.force_attempted = true;
            signals.force_error = group.signal(rustix::process::Signal::KILL).err();
            if exit.is_none()
                && let Ok(Ok(status)) = timeout(limits.kill_grace, child.wait()).await
            {
                exit = Some(exit_observation(status));
            }
            signalled = true;
            until = Instant::now()
                .checked_add(limits.pipe_grace)
                .ok_or(ProcessError::InvalidSpec("pipe grace"))?;
        }
        if out_done.is_some() && err_done.is_some() && input_done.is_some() {
            break;
        }
        tokio::select! {
            biased;
            _ = cancellation.cancelled(), if reason == StopReason::Exited => { reason = StopReason::Cancelled; }
            _ = dropped.cancelled(), if reason == StopReason::Exited => { reason = StopReason::Cancelled; }
            _ = tokio::time::sleep_until(deadline), if reason == StopReason::Exited => { reason = StopReason::Deadline; }
            _ = tokio::time::sleep_until(until) => {
                if reason == StopReason::Exited {
                    reason = if input_done.is_none() { StopReason::InputFailure } else { StopReason::PipeTimeout };
                } else { break; }
            }
            Some(fault) = faults.recv(), if reason == StopReason::Exited => { reason = reason_for_fault(fault); }
            result = &mut out_task, if out_done.is_none() => {
                let fallback = if result.is_ok() { OutputCompletion::Interrupted } else { OutputCompletion::Failed(io::ErrorKind::Other) };
                if result.is_err() && reason == StopReason::Exited { reason = StopReason::OutputFailure(OutputStream::Stdout); }
                out_done = Some(fallback);
            }
            result = &mut err_task, if err_done.is_none() => {
                let fallback = if result.is_ok() { OutputCompletion::Interrupted } else { OutputCompletion::Failed(io::ErrorKind::Other) };
                if result.is_err() && reason == StopReason::Exited { reason = StopReason::OutputFailure(OutputStream::Stderr); }
                err_done = Some(fallback);
            }
            result = &mut in_task, if input_done.is_none() => {
                let result = result.unwrap_or(InputCompletion::Failed(io::ErrorKind::Other));
                if result != InputCompletion::Complete && reason == StopReason::Exited {
                    reason = StopReason::InputFailure;
                }
                input_done = Some(result);
            }
        }
    }
    let out_fallback = if let Some(result) = out_done {
        result
    } else {
        out_task.abort();
        let _ = out_task.await;
        OutputCompletion::TimedOut
    };
    let err_fallback = if let Some(result) = err_done {
        result
    } else {
        err_task.abort();
        let _ = err_task.await;
        OutputCompletion::TimedOut
    };
    let input = if let Some(result) = input_done {
        result
    } else {
        in_task.abort();
        let _ = in_task.await;
        InputCompletion::TimedOut
    };
    let stdout = capture_snapshot(&stdout_state, out_fallback);
    let stderr = capture_snapshot(&stderr_state, err_fallback);
    if reason == StopReason::Exited {
        if let Ok(fault) = faults.try_recv() {
            reason = reason_for_fault(fault);
        } else if stdout.completion == OutputCompletion::TimedOut
            || stderr.completion == OutputCompletion::TimedOut
        {
            reason = StopReason::PipeTimeout;
        } else if !matches!(input, InputCompletion::Complete) {
            reason = StopReason::InputFailure;
        }
    }
    if exit.is_some() && (!signals.force_attempted || signals.force_error.is_none()) {
        group.disarm();
    }
    Ok(ProcessReport {
        identity: ProcessIdentity { pid },
        reason,
        exit,
        input,
        stdout,
        stderr,
        signals,
    })
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::{
        ControlledInput, InputCancellation, InputCompletion, write_controlled_input, write_input,
    };
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_util::sync::CancellationToken;

    #[test]
    fn brokered_finish_is_ordered_before_or_after_observed_output_eof() {
        let (sender, mut receiver) = super::brokered_input_channel(2, 1024);
        sender.try_send(ControlledInput::Finish).unwrap();
        receiver.state.lock().unwrap().output_closed = true;
        assert!(matches!(
            receiver.input.try_recv(),
            Ok(super::BrokeredMessage {
                input: ControlledInput::Finish,
                ..
            })
        ));
        assert!(receiver.state.lock().unwrap().finish_queued);

        let (sender, receiver) = super::brokered_input_channel(2, 1024);
        receiver.state.lock().unwrap().output_closed = true;
        assert_eq!(
            sender.try_send(ControlledInput::Finish),
            Err(super::BrokerInputError::Stopped)
        );
    }

    #[test]
    fn brokered_finish_stops_other_senders_even_if_a_waker_panics() {
        use std::{
            panic::AssertUnwindSafe,
            task::{Context, Poll, Wake, Waker},
        };
        struct PanicWake;
        impl Wake for PanicWake {
            fn wake(self: Arc<Self>) {
                panic!("fixture waker failure");
            }
        }
        let (sender, mut receiver) = super::brokered_input_channel(1, 1024);
        let waker = Waker::from(Arc::new(PanicWake));
        let mut context = Context::from_waker(&waker);
        assert!(matches!(
            receiver.input.poll_recv(&mut context),
            Poll::Pending
        ));
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            sender.try_send(ControlledInput::Finish)
        }));
        assert!(result.is_err());
        assert!(sender.stopped.is_cancelled());
        assert_eq!(
            sender.try_send(ControlledInput::Bytes(vec![1])),
            Err(super::BrokerInputError::Stopped)
        );
    }

    #[test]
    fn broker_finish_guard_preserves_an_existing_panic_if_cancel_waker_panics() {
        use std::{
            future::Future,
            panic::AssertUnwindSafe,
            pin::Pin,
            task::{Context, Poll, Wake, Waker},
        };
        struct PanicWake;
        impl Wake for PanicWake {
            fn wake(self: Arc<Self>) {
                panic!("cancellation waker failure");
            }
        }
        let cancellation = CancellationToken::new();
        let mut waiter = Box::pin(cancellation.cancelled());
        let waker = Waker::from(Arc::new(PanicWake));
        assert!(matches!(
            Pin::new(&mut waiter).poll(&mut Context::from_waker(&waker)),
            Poll::Pending
        ));
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let _guard = super::BrokerFinishStop(cancellation.clone());
            panic!("original panic");
        }));
        let panic = result.expect_err("the original panic must propagate");
        assert_eq!(panic.downcast_ref::<&str>(), Some(&"original panic"));
        assert!(cancellation.is_cancelled());
    }

    #[tokio::test]
    async fn brokered_receiver_preserves_admission_order_when_enqueue_is_reversed() {
        let (sender, mut receiver) = super::brokered_input_channel(2, 1024);
        let first_slot = Arc::clone(&sender.slots).try_acquire_owned().unwrap();
        let first = sender.input.try_reserve().unwrap();
        let (first_order, _, first_bytes) = sender.admit(&ControlledInput::Bytes(vec![1])).unwrap();
        let second_slot = Arc::clone(&sender.slots).try_acquire_owned().unwrap();
        let second = sender.input.try_reserve().unwrap();
        let (second_order, _, second_bytes) = sender.admit(&ControlledInput::Finish).unwrap();
        let reservation = |slot, bytes| super::BrokeredReservation {
            _slot: slot,
            state: Arc::clone(&sender.state),
            stopped: sender.stopped.clone(),
            bytes,
        };
        second.send(super::BrokeredMessage {
            order: second_order,
            input: ControlledInput::Finish,
            reservation: reservation(second_slot, second_bytes),
        });
        first.send(super::BrokeredMessage {
            order: first_order,
            input: ControlledInput::Bytes(vec![1]),
            reservation: reservation(first_slot, first_bytes),
        });
        assert!(
            matches!(receiver.recv().await.map(|message| message.input), Some(ControlledInput::Bytes(bytes)) if bytes == vec![1])
        );
        assert!(matches!(
            receiver.recv().await.map(|message| message.input),
            Some(ControlledInput::Finish)
        ));
    }

    #[tokio::test]
    async fn brokered_out_of_order_buffer_keeps_its_capacity_reservation() {
        let (sender, mut receiver) = super::brokered_input_channel(2, 1024);
        let first_slot = Arc::clone(&sender.slots).try_acquire_owned().unwrap();
        let first = sender.input.try_reserve().unwrap();
        let (order, _, bytes) = sender.admit(&ControlledInput::Bytes(vec![1])).unwrap();
        sender.try_send(ControlledInput::Bytes(vec![2])).unwrap();
        let (ready, observed) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(30), receiver.recv())
                    .await
                    .is_err()
            );
            let _ = ready.send(());
            let first = receiver.recv().await.unwrap();
            let second = receiver.recv().await.unwrap();
            (first, second)
        });
        observed.await.unwrap();
        assert_eq!(
            sender.try_send(ControlledInput::Bytes(vec![3])),
            Err(super::BrokerInputError::Full)
        );
        first.send(super::BrokeredMessage {
            order,
            input: ControlledInput::Bytes(vec![1]),
            reservation: super::BrokeredReservation {
                _slot: first_slot,
                state: Arc::clone(&sender.state),
                stopped: sender.stopped.clone(),
                bytes,
            },
        });
        let (first, second) = task.await.unwrap();
        assert!(matches!(first.input, ControlledInput::Bytes(bytes) if bytes == vec![1]));
        assert!(matches!(second.input, ControlledInput::Bytes(bytes) if bytes == vec![2]));
    }

    #[tokio::test]
    async fn brokered_outstanding_bytes_remain_reserved_until_frame_is_released() {
        let (sender, mut receiver) = super::brokered_input_channel(2, 4);
        sender.try_send(ControlledInput::Bytes(vec![1; 3])).unwrap();
        assert_eq!(
            sender.try_send(ControlledInput::Bytes(vec![2; 2])),
            Err(super::BrokerInputError::Full)
        );
        let message = receiver.recv().await.unwrap();
        assert_eq!(
            sender.try_send(ControlledInput::Bytes(vec![2; 2])),
            Err(super::BrokerInputError::Full)
        );
        drop(message);
        sender.try_send(ControlledInput::Bytes(vec![2; 2])).unwrap();
        assert_eq!(
            sender.try_send(ControlledInput::Bytes(vec![3; 5])),
            Err(super::BrokerInputError::InputLimit)
        );
    }

    #[tokio::test]
    async fn brokered_stdout_eof_closes_gate_before_later_finish() {
        let (sender, receiver) = super::brokered_input_channel(1, 1024);
        let (writer, mut reader) = tokio::io::duplex(16);
        drop(writer);
        let payload_stop = CancellationToken::new();
        let mut buffer = [0_u8; 16];
        assert_eq!(
            super::read_brokered_stream(
                &mut reader,
                &mut buffer,
                &receiver.state,
                &receiver.stopped,
                &payload_stop,
                super::OutputStream::Stdout,
            )
            .await
            .unwrap(),
            0
        );
        assert!(payload_stop.is_cancelled());
        assert_eq!(
            sender.try_send(ControlledInput::Finish),
            Err(super::BrokerInputError::Stopped)
        );
    }

    #[tokio::test]
    async fn brokered_output_limit_faults_latch_before_the_observation_is_released() {
        for stream in [super::OutputStream::Stdout, super::OutputStream::Stderr] {
            let (mut peer, pipe) = tokio::io::duplex(16);
            peer.write_all(b"too long\n").await.unwrap();
            let (sender, receiver) = super::brokered_input_channel(1, 1024);
            let (events, _) = tokio::sync::mpsc::channel(3);
            let capture = Arc::new(std::sync::Mutex::new(super::CaptureState {
                bytes: Vec::new(),
                completion: None,
                trailing: 0,
            }));
            super::read_pipe(
                pipe,
                super::ReaderContext {
                    stream,
                    max_bytes: 4,
                    max_line: 16,
                    capture,
                    events,
                    liveness: CancellationToken::new(),
                    input_stop: CancellationToken::new(),
                    payload_stop: CancellationToken::new(),
                    input_finished: CancellationToken::new(),
                    atomic_observation: true,
                    brokered: true,
                    broker_state: Some(Arc::clone(&receiver.state)),
                    broker_stopped: Some(receiver.stopped.clone()),
                    observer: None,
                },
            )
            .await;
            assert!(receiver.state.lock().unwrap().protocol_faulted);
            assert_eq!(
                sender.try_send(ControlledInput::Bytes(vec![1])),
                Err(super::BrokerInputError::Stopped)
            );
        }
    }

    #[tokio::test]
    async fn receipt_transition_orders_and_wakes_both_output_readers() {
        use std::{
            future::Future,
            task::{Context, Poll, Waker},
        };

        let (sender, receiver) = super::brokered_input_channel(1, 1024);
        let super::BrokerTransitionAdmission::Ready(transition) =
            sender.observation_barrier().unwrap()
        else {
            panic!("expected transition admission")
        };
        let (mut stdout_writer, mut stdout) = tokio::io::duplex(16);
        let (mut stderr_writer, mut stderr) = tokio::io::duplex(16);
        stdout_writer.write_all(b"out").await.unwrap();
        stderr_writer.write_all(b"err").await.unwrap();
        let mut stdout_bytes = [0_u8; 16];
        let mut stderr_bytes = [0_u8; 16];
        let stopped = CancellationToken::new();
        let payload_stop = CancellationToken::new();
        let mut stdout_read = Box::pin(super::read_brokered_stream(
            &mut stdout,
            &mut stdout_bytes,
            &receiver.state,
            &stopped,
            &payload_stop,
            super::OutputStream::Stdout,
        ));
        let mut stderr_read = Box::pin(super::read_brokered_stream(
            &mut stderr,
            &mut stderr_bytes,
            &receiver.state,
            &stopped,
            &payload_stop,
            super::OutputStream::Stderr,
        ));
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(
            stdout_read.as_mut().poll(&mut context),
            Poll::Pending
        ));
        assert!(matches!(
            stderr_read.as_mut().poll(&mut context),
            Poll::Pending
        ));
        assert_eq!(receiver.state.lock().unwrap().output_observations, 0);
        drop(transition);
        let (out, err) = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            tokio::join!(stdout_read, stderr_read)
        })
        .await
        .expect("readers were not woken");
        assert_eq!(out.unwrap(), 3);
        assert_eq!(err.unwrap(), 3);
        assert_eq!(receiver.state.lock().unwrap().output_observations, 2);
        let mut stdout_observation =
            super::BrokerObservation::new(Arc::clone(&receiver.state)).unwrap();
        let mut stderr_observation =
            super::BrokerObservation::new(Arc::clone(&receiver.state)).unwrap();
        stdout_observation.finish(true);
        stderr_observation.finish(true);
    }

    #[tokio::test]
    async fn brokered_sender_does_not_wait_for_capacity_after_stop() {
        let (sender, receiver) = super::brokered_input_channel(1, 1024);
        sender.try_send(ControlledInput::Finish).unwrap();
        assert_eq!(
            sender.try_send(ControlledInput::Bytes(vec![1])),
            Err(super::BrokerInputError::Stopped)
        );
        assert_eq!(
            sender.send(ControlledInput::Bytes(vec![1])).await,
            Err(super::BrokerInputError::Stopped)
        );
        drop(receiver);

        let (sender, receiver) = super::brokered_input_channel(1, 1024);
        sender.try_send(ControlledInput::Bytes(vec![1])).unwrap();
        let waiting =
            tokio::spawn(async move { sender.send(ControlledInput::Bytes(vec![2])).await });
        tokio::task::yield_now().await;
        receiver.state.lock().unwrap().output_closed = true;
        receiver.stopped.cancel();
        assert_eq!(
            waiting.await.unwrap(),
            Err(super::BrokerInputError::Stopped)
        );
    }

    fn active_input(caller: CancellationToken) -> InputCancellation {
        InputCancellation {
            stop: CancellationToken::new(),
            payload_stop: CancellationToken::new(),
            caller,
            dropped: CancellationToken::new(),
            deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(2),
            finished: CancellationToken::new(),
        }
    }

    #[tokio::test]
    async fn controlled_observer_never_receives_a_chunk_with_output_overflow() {
        use std::sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        };
        let (mut writer, reader) = tokio::io::duplex(128);
        writer.write_all(b"ready\nexcess").await.unwrap();
        drop(writer);
        let called = Arc::new(AtomicBool::new(false));
        let marker = Arc::clone(&called);
        let observer: super::ProcessObserver = Arc::new(move |_, _| {
            marker.store(true, Ordering::Release);
            Ok(())
        });
        let capture = Arc::new(Mutex::new(super::CaptureState {
            bytes: Vec::new(),
            completion: None,
            trailing: 0,
        }));
        let (events, mut faults) = tokio::sync::mpsc::channel(2);
        let input_stop = CancellationToken::new();
        super::read_pipe(
            reader,
            super::ReaderContext {
                stream: super::OutputStream::Stdout,
                max_bytes: 6,
                max_line: 64,
                capture,
                events,
                liveness: CancellationToken::new(),
                input_stop: input_stop.clone(),
                payload_stop: CancellationToken::new(),
                input_finished: CancellationToken::new(),
                atomic_observation: true,
                brokered: false,
                broker_state: None,
                broker_stopped: None,
                observer: Some(observer),
            },
        )
        .await;
        assert!(!called.load(Ordering::Acquire));
        assert!(input_stop.is_cancelled());
        assert!(matches!(
            faults.try_recv(),
            Ok(super::Fault::Output(
                super::OutputStream::Stdout,
                super::OutputCompletion::OutputLimit
            ))
        ));
    }

    #[tokio::test]
    async fn controlled_writer_waits_for_frames_and_explicit_finish() {
        let (writer, mut reader) = tokio::io::duplex(128);
        let (events, _faults) = tokio::sync::mpsc::channel(2);
        let (sender, receiver) = tokio::sync::mpsc::channel(2);
        let task = tokio::spawn(write_controlled_input(
            writer,
            b"init\n".to_vec(),
            receiver,
            64,
            events,
            active_input(CancellationToken::new()),
        ));
        let mut initial = [0u8; 5];
        reader.read_exact(&mut initial).await.unwrap();
        assert_eq!(&initial, b"init\n");
        let mut buffer = [0u8; 16];
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(30),
                reader.read(&mut buffer)
            )
            .await
            .is_err()
        );
        sender
            .send(ControlledInput::Bytes(b"prompt\n".to_vec()))
            .await
            .unwrap();
        let count = reader.read(&mut buffer).await.unwrap();
        assert_eq!(&buffer[..count], b"prompt\n");
        sender.send(ControlledInput::Finish).await.unwrap();
        assert_eq!(task.await.unwrap(), InputCompletion::Complete);
        assert_eq!(reader.read(&mut buffer).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn confirmed_frame_reports_only_after_all_bytes_reach_the_pipe() {
        let (writer, mut reader) = tokio::io::duplex(2);
        let (events, _faults) = tokio::sync::mpsc::channel(2);
        let (sender, receiver) = tokio::sync::mpsc::channel(2);
        let task = tokio::spawn(write_controlled_input(
            writer,
            Vec::new(),
            receiver,
            64,
            events,
            active_input(CancellationToken::new()),
        ));
        let (written, mut receipt) = tokio::sync::oneshot::channel();
        sender
            .send(ControlledInput::Confirmed {
                bytes: b"abcd".to_vec(),
                written,
            })
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(30), &mut receipt)
                .await
                .is_err()
        );
        let mut bytes = [0u8; 4];
        reader.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"abcd");
        receipt.await.unwrap();
        sender.send(ControlledInput::Finish).await.unwrap();
        assert_eq!(task.await.unwrap(), InputCompletion::Complete);
    }

    #[tokio::test]
    async fn controlled_writer_pre_cancel_sends_no_initial_frame() {
        let (writer, mut reader) = tokio::io::duplex(128);
        let (events, _faults) = tokio::sync::mpsc::channel(2);
        let (_sender, receiver) = tokio::sync::mpsc::channel(2);
        let cancel = CancellationToken::new();
        cancel.cancel();
        let result = write_controlled_input(
            writer,
            b"init".to_vec(),
            receiver,
            64,
            events,
            active_input(cancel),
        )
        .await;
        assert_eq!(result, InputCompletion::Interrupted);
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await.unwrap();
        assert!(bytes.is_empty());
    }

    #[tokio::test]
    async fn controlled_writer_cancellation_never_releases_waiting_input() {
        let (writer, mut reader) = tokio::io::duplex(128);
        let (events, _faults) = tokio::sync::mpsc::channel(2);
        let (_sender, receiver) = tokio::sync::mpsc::channel(2);
        let cancel = CancellationToken::new();
        let task = tokio::spawn(write_controlled_input(
            writer,
            b"init".to_vec(),
            receiver,
            64,
            events,
            active_input(cancel.clone()),
        ));
        let mut initial = [0u8; 4];
        reader.read_exact(&mut initial).await.unwrap();
        cancel.cancel();
        assert_eq!(task.await.unwrap(), InputCompletion::Interrupted);
        let mut remaining = Vec::new();
        reader.read_to_end(&mut remaining).await.unwrap();
        assert!(remaining.is_empty());
    }

    #[tokio::test]
    async fn controlled_writer_requires_explicit_finish() {
        let (writer, mut reader) = tokio::io::duplex(128);
        let (events, mut faults) = tokio::sync::mpsc::channel(2);
        let (sender, receiver) = tokio::sync::mpsc::channel(2);
        let task = tokio::spawn(write_controlled_input(
            writer,
            b"init".to_vec(),
            receiver,
            64,
            events,
            active_input(CancellationToken::new()),
        ));
        let mut initial = [0u8; 4];
        reader.read_exact(&mut initial).await.unwrap();
        drop(sender);
        assert_eq!(task.await.unwrap(), InputCompletion::Interrupted);
        assert!(matches!(faults.try_recv(), Ok(super::Fault::Input)));
        let mut remaining = Vec::new();
        reader.read_to_end(&mut remaining).await.unwrap();
        assert!(remaining.is_empty());
    }

    #[tokio::test]
    async fn controlled_writer_enforces_aggregate_limit_before_writing_a_frame() {
        let (writer, mut reader) = tokio::io::duplex(128);
        let (events, _faults) = tokio::sync::mpsc::channel(2);
        let (sender, receiver) = tokio::sync::mpsc::channel(2);
        let task = tokio::spawn(write_controlled_input(
            writer,
            b"init".to_vec(),
            receiver,
            6,
            events,
            active_input(CancellationToken::new()),
        ));
        let mut initial = [0u8; 4];
        reader.read_exact(&mut initial).await.unwrap();
        sender
            .send(ControlledInput::Bytes(b"long".to_vec()))
            .await
            .unwrap();
        assert_eq!(
            task.await.unwrap(),
            InputCompletion::Failed(std::io::ErrorKind::InvalidInput)
        );
        let mut remaining = Vec::new();
        reader.read_to_end(&mut remaining).await.unwrap();
        assert!(remaining.is_empty());
    }

    #[tokio::test]
    async fn cancelled_writer_does_not_begin_a_new_write() {
        for cause in 0..4 {
            let (writer, mut reader) = tokio::io::duplex(16);
            let (events, _receiver) = tokio::sync::mpsc::channel(1);
            let stop = CancellationToken::new();
            let caller = CancellationToken::new();
            let dropped = CancellationToken::new();
            match cause {
                0 => stop.cancel(),
                1 => caller.cancel(),
                2 => dropped.cancel(),
                _ => {}
            }
            let deadline = tokio::time::Instant::now()
                + if cause == 3 {
                    std::time::Duration::ZERO
                } else {
                    std::time::Duration::from_secs(1)
                };
            let cancellation = InputCancellation {
                stop,
                payload_stop: CancellationToken::new(),
                caller,
                dropped,
                deadline,
                finished: CancellationToken::new(),
            };
            let result =
                write_input(writer, b"private request".to_vec(), events, cancellation).await;
            assert_eq!(result, InputCompletion::Interrupted);
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes).await.unwrap();
            assert!(bytes.is_empty());
        }
    }
}

#[cfg(not(target_os = "linux"))]
async fn supervise(
    _spec: ProcessSpec,
    _cancellation: CancellationToken,
    _dropped: CancellationToken,
    _deadline: Instant,
    _io: SupervisionIo,
) -> Result<ProcessReport, ProcessError> {
    Err(ProcessError::UnsupportedPlatform)
}
