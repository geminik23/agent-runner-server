//! Bounded supervision of a trusted, directly owned child process.

#[cfg(target_os = "linux")]
mod identity;

#[cfg(target_os = "linux")]
use std::process::Stdio;
use std::{
    collections::HashSet,
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
struct SupervisionIo {
    observer: Option<ProcessObserver>,
    on_start: Option<ProcessStartObserver>,
    pinned_directory: Option<File>,
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

/// Returns the start token of a running process: the boot ID and the kernel start time of `pid`.
/// Together with the PID it names one process instance even after the PID is reused or the host reboots.
pub fn process_start_token(pid: u32) -> Option<String> {
    let boot = std::fs::read_to_string("/proc/sys/kernel/random/boot_id").ok()?;
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // The command name may contain spaces and parentheses, so fields are counted after its last `)`.
    // The start time is field 22 of the whole line, which is the 20th field after the state.
    let (_, fields) = stat.rsplit_once(')')?;
    let mut fields = fields.split_whitespace();
    let state = fields.next()?;
    if state == "Z" || state == "X" {
        return None;
    }
    let start = fields.nth(18)?.parse::<u64>().ok()?;
    Some(format!("{}:{start}", boot.trim()))
}

/// Reports whether the process recorded as `(pid, start)` still runs; a different start token means the PID was reused.
pub fn process_still_running(pid: u32, start: &str) -> bool {
    process_start_token(pid).is_some_and(|current| current == start)
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
        Self::run_inner(spec, cancellation, observer, on_start, pinned_directory).await
    }

    async fn run_inner(
        spec: ProcessSpec,
        cancellation: CancellationToken,
        observer: Option<ProcessObserver>,
        on_start: Option<ProcessStartObserver>,
        pinned_directory: Option<File>,
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
    observer: Option<ProcessObserver>,
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
        observer,
    } = context;
    let mut buf = [0_u8; 8192];
    loop {
        let read = tokio::select! {
            _ = liveness.cancelled() => {
                if let Ok(mut state) = capture.lock() { state.completion = Some(OutputCompletion::Interrupted); }
                return;
            }
            result = pipe.read(&mut buf) => result,
        };
        let mut state = match capture.lock() {
            Ok(state) => state,
            Err(_) => return,
        };
        let count = match read {
            Ok(0) => {
                state.completion = Some(OutputCompletion::Eof);
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
    }
}

#[cfg(target_os = "linux")]
struct InputCancellation {
    stop: CancellationToken,
    caller: CancellationToken,
    dropped: CancellationToken,
    deadline: Instant,
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
    } = io;
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
            observer,
        },
    ));
    let stop_input_on_exit = !spec.stdin.is_empty();
    let input_cancellation = InputCancellation {
        stop: input_stop.clone(),
        caller: cancellation.clone(),
        dropped: dropped.clone(),
        deadline,
    };
    let mut in_task = tokio::spawn(write_input(stdin, spec.stdin, events, input_cancellation));
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
    if reason != StopReason::Exited || stop_input_on_exit {
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
    use super::{InputCancellation, InputCompletion, write_input};

    #[test]
    fn start_tokens_name_one_running_process_instance() {
        let own = std::process::id();
        let token = super::process_start_token(own).unwrap();
        assert!(super::process_still_running(own, &token));
        let (boot, ticks) = token.rsplit_once(':').unwrap();
        assert!(!boot.is_empty() && ticks.parse::<u64>().is_ok());
        // Another start time for the same PID is a different, reused process.
        assert!(!super::process_still_running(
            own,
            &format!("{boot}:{}", ticks.parse::<u64>().unwrap() + 1)
        ));
        let mut child = std::process::Command::new("/bin/true").spawn().unwrap();
        let pid = child.id();
        child.wait().unwrap();
        assert!(super::process_start_token(pid).is_none_or(|current| current != token));
    }
    use tokio::io::AsyncReadExt;
    use tokio_util::sync::CancellationToken;

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
                caller,
                dropped,
                deadline,
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
