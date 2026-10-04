//! Restricted, version-gated adapters for trusted local CLI installations.

use std::{
    ffi::OsString,
    future::Future,
    path::PathBuf,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use serde::Deserialize;
use serde_json::Value;

use crate::{
    core::{
        Attempt, BackendError, CancellationToken, CleanupState, DispatchStatus, EventSink,
        ExecutionBackend, ExecutionEvent, ExecutionEventKind, ExecutionMetadata, ExecutionOutcome,
        ExecutionRequest, ExecutionResult, ExecutionWarning, Run, SessionDisposition, SessionRef,
        SessionRequest, WarningObservation,
    },
    process::{
        InputCompletion, ObserverError, OutputStream, ProcessLimits, ProcessObserver, ProcessSpec,
        ProcessStartObserver, ProcessSupervisor, StopReason, StopSignal,
    },
};

/// Argument token replaced by the registered native session ID in `resume_args`.
pub const NATIVE_SESSION_TOKEN: &str = "{native_session_id}";

/// Upper bound for the combined program, argument, cwd and environment bytes of one spec.
const MAX_SPEC_BYTES: usize = 64 * 1024;

/// Exact `--version` output of the helper built from this crate.
const HELPER_VERSION: &str = concat!("agent-runner-cli-helper ", env!("CARGO_PKG_VERSION"));
/// Bounds the observed version copied into a run warning when the profile accepts any version.
const MAX_UNPINNED_VERSION_BYTES: usize = 128;

/// Selects the output protocol adapter; commands, versions and environment come from settings.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CliKind {
    /// Codex `exec --json` JSONL events with `thread.started` identity.
    Codex,
    /// Claude Code `--output-format json` single result object with `session_id`.
    Claude,
}

impl CliKind {
    /// Runtime identifier recorded on runs, sessions and profile grants.
    pub fn runtime(self) -> &'static str {
        match self {
            CliKind::Codex => "codex",
            CliKind::Claude => "claude",
        }
    }
}

/// Trusted CLI configuration; request bodies cannot modify executable, arguments or environment.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CliSettings {
    pub kind: CliKind,
    pub program: PathBuf,
    pub helper_program: PathBuf,
    /// Operator label of the reviewed compatibility record this profile follows.
    pub compatibility: String,
    /// Exact version probe that must succeed before any prompt is sent.
    pub version: CliVersionProbe,
    /// Arguments for a new session; the prompt is always written to stdin.
    pub new_args: Vec<String>,
    /// Arguments for an explicit resume; must contain `NATIVE_SESSION_TOKEN` exactly once.
    pub resume_args: Vec<String>,
    pub environment: CliEnvironment,
    #[serde(default)]
    pub platform: CliPlatform,
    /// Entry names that must not exist in the workspace or its ancestors, except the state directory itself.
    #[serde(default)]
    pub forbidden_workspace_entries: Vec<String>,
    /// Native transcript location used to reject a missing session before launch.
    #[serde(default)]
    pub session_store: Option<CliSessionStore>,
    pub max_stdin_bytes: usize,
    pub max_stdout_bytes: usize,
    pub max_stderr_bytes: usize,
    pub max_line_bytes: usize,
    pub timeout_ms: u64,
    pub preflight_timeout_ms: u64,
    pub signal_grace_ms: u64,
    pub kill_grace_ms: u64,
    pub pipe_grace_ms: u64,
}

/// Command and trimmed stdout that identify a reviewed CLI build.
/// Exactly one of `expected_stdout` and `accept_any` must be set.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CliVersionProbe {
    pub args: Vec<String>,
    /// Exact trimmed stdout of the reviewed build; every run is refused when the installed CLI differs.
    #[serde(default)]
    pub expected_stdout: Option<String>,
    /// Runs any build whose probe exits successfully and reports the observed version as a run warning.
    #[serde(default)]
    pub accept_any: bool,
}

/// Environment inheritance and native state selection for one CLI.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CliEnvironment {
    /// Variables copied from the daemon environment when present; `HOME` and `PATH` are required.
    pub inherit: Vec<String>,
    /// Variable that names the native state directory and is always passed as a canonical path.
    pub state_directory_variable: String,
    /// State directory relative to `HOME` when `state_directory_variable` is unset.
    pub default_state_directory: PathBuf,
    /// Daemon variables whose presence rejects the profile, such as provider credential overrides.
    #[serde(default)]
    pub forbidden_prefixes: Vec<String>,
    #[serde(default)]
    pub forbidden_names: Vec<String>,
}

/// Optional host requirements for the reviewed compatibility record.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CliPlatform {
    /// Required `std::env::consts::ARCH` value.
    #[serde(default)]
    pub architecture: Option<String>,
    /// Case-insensitive substrings that must all appear in the kernel release.
    #[serde(default)]
    pub kernel_release_contains: Vec<String>,
}

/// Bounded search for a native transcript whose file name ends with the session ID and suffix.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CliSessionStore {
    /// Directory relative to the native state directory.
    pub directory: PathBuf,
    pub file_suffix: String,
    /// Maximum path depth below `directory`, counting the transcript file itself.
    pub max_depth: usize,
    /// Maximum directory entries inspected; exceeding it rejects the resume as unverifiable.
    pub max_entries: usize,
}

/// Executes one managed CLI invocation without exposing credentials to callers.
pub struct CliBackend {
    kind: CliKind,
    program: PathBuf,
    program_identity: Option<ExecutableIdentity>,
    helper_program: Option<PathBuf>,
    helper_identity: Option<ExecutableIdentity>,
    state_realm: String,
    state_directory: Option<crate::workspace::Directory>,
    state_owner_root: Option<PathBuf>,
    environment: Vec<(OsString, OsString)>,
    new_args: Vec<OsString>,
    resume_args: Vec<OsString>,
    version_args: Vec<OsString>,
    /// `None` only when the validated profile set `accept_any`.
    expected_version: Option<String>,
    forbidden_workspace_entries: Vec<OsString>,
    session_store: Option<CliSessionStore>,
    preflight_timeout: Duration,
    limits: ProcessLimits,
}

impl CliBackend {
    /// Builds a profile from a trusted, operator-reviewed CLI definition.
    pub fn new(settings: CliSettings) -> Result<Self, &'static str> {
        if !cfg!(target_os = "linux") || !settings.platform.matches() {
            return Err("unsupported CLI environment");
        }
        validate_invocation(&settings)?;
        if settings.max_stdin_bytes == 0
            || settings.max_stdout_bytes == 0
            || settings.max_stderr_bytes == 0
            || settings.max_line_bytes == 0
            || settings.max_stdin_bytes > 16 * 1024 * 1024
            || settings.max_stdout_bytes > 16 * 1024 * 1024
            || settings.max_stderr_bytes > 16 * 1024 * 1024
            || settings.max_line_bytes > settings.max_stdout_bytes.max(settings.max_stderr_bytes)
            || [
                settings.timeout_ms,
                settings.preflight_timeout_ms,
                settings.signal_grace_ms,
                settings.kill_grace_ms,
                settings.pipe_grace_ms,
            ]
            .contains(&0)
            || [
                settings.timeout_ms,
                settings.preflight_timeout_ms,
                settings.signal_grace_ms,
                settings.kill_grace_ms,
                settings.pipe_grace_ms,
            ]
            .iter()
            .any(|value| *value > 3_600_000)
        {
            return Err("invalid CLI limits");
        }
        let program =
            std::fs::canonicalize(&settings.program).map_err(|_| "invalid CLI executable")?;
        let metadata = std::fs::metadata(&program).map_err(|_| "invalid CLI executable")?;
        if !program.is_absolute() || !metadata.is_file() {
            return Err("invalid CLI executable");
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if metadata.permissions().mode() & 0o111 == 0 {
                return Err("invalid CLI executable");
            }
        }
        let program_identity =
            ExecutableIdentity::inspect(&program).ok_or("invalid CLI executable")?;
        let helper_program =
            std::fs::canonicalize(&settings.helper_program).map_err(|_| "invalid CLI helper")?;
        let helper_identity =
            ExecutableIdentity::inspect(&helper_program).ok_or("invalid CLI helper")?;
        let env_settings = &settings.environment;
        let forbidden = std::env::vars_os().any(|(key, _)| forbidden_override(env_settings, &key));
        if forbidden {
            return Err("unsupported CLI authentication or provider override");
        }
        let state_key = env_settings.state_directory_variable.as_str();
        let mut names: Vec<&str> = env_settings.inherit.iter().map(String::as_str).collect();
        if !names.contains(&state_key) {
            names.push(state_key);
        }
        let mut environment: Vec<_> = names
            .into_iter()
            .filter_map(|name| std::env::var_os(name).map(|value| (OsString::from(name), value)))
            .collect();
        let value = |name: &str| {
            environment
                .iter()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value.clone())
        };
        let home = PathBuf::from(value("HOME").ok_or("HOME is unavailable")?);
        let path = value("PATH").ok_or("CLI environment is unavailable")?;
        if !home.is_absolute() || !absolute_search_path(&path) {
            return Err("CLI environment is unavailable");
        }
        let home = std::fs::canonicalize(home).map_err(|_| "HOME is unavailable")?;
        let state = value(state_key)
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(&env_settings.default_state_directory));
        let state =
            std::fs::canonicalize(state).map_err(|_| "CLI state directory is unavailable")?;
        if !state.is_dir() {
            return Err("CLI state directory is unavailable");
        }
        let state_directory = crate::workspace::Directory::inspect(&state)
            .map_err(|_| "CLI state directory is unavailable")?;
        if let Some((_, value)) = environment.iter_mut().find(|(key, _)| key == "HOME") {
            *value = home.into_os_string();
        }
        if let Some((_, value)) = environment.iter_mut().find(|(key, _)| key == state_key) {
            *value = state.clone().into_os_string();
        } else {
            environment.push((state_key.into(), state.clone().into_os_string()));
        }
        let state_realm = native_realm(settings.kind, &state_directory);
        let os_args = |args: &[String]| args.iter().map(OsString::from).collect::<Vec<_>>();
        // The helper wrapper adds a separator and the CLI program before the configured arguments.
        let max_args = 2 + settings
            .new_args
            .len()
            .max(settings.resume_args.len())
            .max(settings.version.args.len());
        let max_env = environment.len();
        Ok(Self {
            kind: settings.kind,
            program,
            program_identity: Some(program_identity),
            helper_program: Some(helper_program),
            helper_identity: Some(helper_identity),
            state_realm,
            state_directory: Some(state_directory),
            state_owner_root: Some(account_home().ok_or("account home is unavailable")?),
            environment,
            new_args: os_args(&settings.new_args),
            resume_args: os_args(&settings.resume_args),
            version_args: os_args(&settings.version.args),
            expected_version: settings.version.expected_stdout,
            forbidden_workspace_entries: settings
                .forbidden_workspace_entries
                .iter()
                .map(OsString::from)
                .collect(),
            session_store: settings.session_store,
            preflight_timeout: Duration::from_millis(settings.preflight_timeout_ms),
            limits: ProcessLimits {
                max_args,
                max_env,
                max_spec_bytes: MAX_SPEC_BYTES,
                max_stdin_bytes: settings.max_stdin_bytes,
                max_stdout_bytes: settings.max_stdout_bytes,
                max_stderr_bytes: settings.max_stderr_bytes,
                max_line_bytes: settings.max_line_bytes,
                timeout: Duration::from_millis(settings.timeout_ms),
                signal_grace: Duration::from_millis(settings.signal_grace_ms),
                kill_grace: Duration::from_millis(settings.kill_grace_ms),
                pipe_grace: Duration::from_millis(settings.pipe_grace_ms),
            },
        })
    }

    /// Returns the minimum time needed for bounded process cleanup.
    pub fn cleanup_budget(&self) -> Option<Duration> {
        self.limits
            .signal_grace
            .checked_add(self.limits.kill_grace)?
            .checked_add(self.limits.pipe_grace)
    }

    async fn program_unchanged(&self, cancellation: CancellationToken) -> bool {
        let target = self
            .program_identity
            .clone()
            .map(|identity| (self.program.clone(), identity));
        let helper = self
            .helper_program
            .clone()
            .zip(self.helper_identity.clone());
        tokio::select! {
            _ = cancellation.cancelled() => false,
            result = tokio::task::spawn_blocking(move || target.into_iter().chain(helper).all(|(path, identity)| ExecutableIdentity::inspect(&path).as_ref() == Some(&identity))) => result.unwrap_or(false),
        }
    }

    fn spec(
        &self,
        args: Vec<OsString>,
        cwd: Option<PathBuf>,
        stdin: Vec<u8>,
        mut limits: ProcessLimits,
    ) -> ProcessSpec {
        if cwd.is_none() {
            limits.timeout = limits.timeout.min(self.preflight_timeout);
        }
        let (program, args) = if let Some(helper) = &self.helper_program {
            let mut wrapped = vec![OsString::from("--"), self.program.as_os_str().to_owned()];
            wrapped.extend(args);
            (helper.clone(), wrapped)
        } else {
            (self.program.clone(), args)
        };
        ProcessSpec {
            program,
            args,
            cwd,
            env: self.environment.clone(),
            stdin,
            limits,
            stop_signal: StopSignal::Interrupt,
        }
    }

    fn args(&self, session: &SessionRequest) -> Result<Vec<OsString>, BackendError> {
        match session {
            SessionRequest::New => Ok(self.new_args.clone()),
            SessionRequest::Resume(session)
                if session.runtime == self.metadata().runtime
                    && session.state_realm == self.state_realm
                    && valid_uuid(&session.native_id) =>
            {
                Ok(self
                    .resume_args
                    .iter()
                    .map(|arg| {
                        if arg == NATIVE_SESSION_TOKEN {
                            OsString::from(&session.native_id)
                        } else {
                            arg.clone()
                        }
                    })
                    .collect())
            }
            SessionRequest::Resume(_) => Err(BackendError::IncompatibleSession),
        }
    }

    fn result(
        &self,
        request: &ExecutionRequest,
        outcome: ExecutionOutcome,
        session: Option<SessionRef>,
        warnings: Vec<ExecutionWarning>,
        cleanup: CleanupState,
        disposition: SessionDisposition,
    ) -> ExecutionResult {
        let metadata = self.metadata();
        let dispatch = if cleanup == CleanupState::Settled
            && disposition == SessionDisposition::NotApplicable
            && session.is_none()
            && !matches!(&outcome, ExecutionOutcome::Succeeded { .. })
        {
            DispatchStatus::ProvenUnstarted
        } else {
            DispatchStatus::StartedOrUnknown
        };
        ExecutionResult {
            run: Run {
                id: request.run_id.clone(),
                metadata: metadata.clone(),
            },
            attempt: Attempt {
                id: request.attempt_id.clone(),
                run_id: request.run_id.clone(),
                metadata: metadata.clone(),
            },
            metadata,
            session,
            warnings,
            warning_observation: WarningObservation::Partial,
            dispatch,
            cleanup,
            session_disposition: disposition,
            outcome,
        }
    }

    /// Checks the pinned state, programs and version before any launch.
    /// Returns the observed version only when the profile accepts any version.
    async fn probe(&self, cancellation: CancellationToken) -> Result<Option<String>, BackendError> {
        if let Some(state) = self.state_directory.clone() {
            let unchanged = tokio::select! {
                _ = cancellation.cancelled() => false,
                result = tokio::task::spawn_blocking(move || state.unchanged()) => result.unwrap_or(false),
            };
            if !unchanged {
                return Err(BackendError::InvalidRequest);
            }
        }
        if !self.program_unchanged(cancellation.clone()).await {
            return Err(BackendError::InvalidRequest);
        }
        if let Some(helper) = &self.helper_program {
            let mut spec = self.spec(Vec::new(), None, vec![], self.limits);
            spec.program = helper.clone();
            spec.args = vec!["--version".into()];
            let report = ProcessSupervisor::run(spec, cancellation.clone())
                .await
                .map_err(|_| BackendError::InvalidRequest)?;
            if report.reason != StopReason::Exited
                || !report.direct_io_settled()
                || report.input != InputCompletion::Complete
                || !report.exit.is_some_and(|exit| exit.code == Some(0))
                || std::str::from_utf8(&report.stdout.bytes)
                    .ok()
                    .map(str::trim)
                    != Some(HELPER_VERSION)
            {
                return Err(BackendError::InvalidRequest);
            }
        }
        let report = ProcessSupervisor::run(
            self.spec(self.version_args.clone(), None, vec![], self.limits),
            cancellation.clone(),
        )
        .await
        .map_err(|_| BackendError::InvalidRequest)?;
        let observed = std::str::from_utf8(&report.stdout.bytes)
            .ok()
            .map(str::trim)
            .filter(|version| !version.is_empty());
        let accepted = match (&self.expected_version, observed) {
            (Some(expected), Some(observed)) => observed == expected,
            (None, Some(observed)) => {
                observed.len() <= MAX_UNPINNED_VERSION_BYTES
                    && observed
                        .bytes()
                        .all(|byte| byte.is_ascii_graphic() || byte == b' ')
            }
            (_, None) => false,
        };
        if !self.program_unchanged(cancellation).await
            || report.reason != StopReason::Exited
            || !report.direct_io_settled()
            || report.input != InputCompletion::Complete
            || !report.exit.is_some_and(|exit| exit.code == Some(0))
            || !accepted
        {
            return Err(BackendError::InvalidRequest);
        }
        Ok(observed
            .filter(|_| self.expected_version.is_none())
            .map(str::to_owned))
    }

    async fn run(
        &self,
        request: ExecutionRequest,
        events: EventSink,
        cancellation: CancellationToken,
    ) -> ExecutionResult {
        let fail = |code: &str, cleanup, disposition, session| {
            self.result(
                &request,
                ExecutionOutcome::Failed(BackendError::ExecutionFailed {
                    code: code.into(),
                    message: "CLI execution did not produce a complete result".into(),
                }),
                session,
                vec![],
                cleanup,
                disposition,
            )
        };
        let Some(directory) = request
            .working_directory
            .clone()
            .filter(|directory| directory.path.is_absolute())
        else {
            return fail(
                "workspace_unavailable",
                CleanupState::Settled,
                SessionDisposition::NotApplicable,
                None,
            );
        };
        let args = match self.args(&request.session) {
            Ok(args) => args,
            Err(error) => {
                return self.result(
                    &request,
                    ExecutionOutcome::Failed(error),
                    None,
                    vec![],
                    CleanupState::Settled,
                    SessionDisposition::NotApplicable,
                );
            }
        };
        if cancellation.is_cancelled() {
            return self.result(
                &request,
                ExecutionOutcome::Cancelled,
                None,
                vec![],
                CleanupState::Settled,
                SessionDisposition::NotApplicable,
            );
        }
        if request.prompt.len() > self.limits.max_stdin_bytes {
            return fail(
                "cli_input_invalid",
                CleanupState::Settled,
                SessionDisposition::NotApplicable,
                None,
            );
        }
        let unpinned_version = match self.probe(cancellation.clone()).await {
            Ok(observed) => observed,
            Err(_) if cancellation.is_cancelled() => {
                return self.result(
                    &request,
                    ExecutionOutcome::Cancelled,
                    None,
                    vec![],
                    CleanupState::Settled,
                    SessionDisposition::NotApplicable,
                );
            }
            Err(_) => {
                return fail(
                    "backend_compatibility_unavailable",
                    CleanupState::Settled,
                    SessionDisposition::NotApplicable,
                    None,
                );
            }
        };
        let directory_path = directory.path.clone();
        let opened = tokio::select! {
            _ = cancellation.cancelled() => None,
            result = tokio::task::spawn_blocking(move || open_pinned_directory(&directory)) => result.ok().and_then(Result::ok),
        };
        let Some(opened) = opened else {
            return fail(
                "workspace_unavailable",
                CleanupState::Settled,
                SessionDisposition::NotApplicable,
                None,
            );
        };
        if !self.forbidden_workspace_entries.is_empty() {
            let path = directory_path.clone();
            let state = self
                .state_directory
                .as_ref()
                .map(|directory| directory.path.clone());
            let entries = self.forbidden_workspace_entries.clone();
            let safe = tokio::select! {
                _ = cancellation.cancelled() => false,
                result = tokio::task::spawn_blocking(move || no_forbidden_workspace_entries(&path, state.as_deref(), &entries)) => result.unwrap_or(false),
            };
            if !safe {
                return fail(
                    "unsupported_workspace_configuration",
                    CleanupState::Settled,
                    SessionDisposition::NotApplicable,
                    None,
                );
            }
        }
        if !self.program_unchanged(cancellation.clone()).await {
            return fail(
                "backend_compatibility_unavailable",
                CleanupState::Settled,
                SessionDisposition::NotApplicable,
                None,
            );
        }
        // A resume never becomes a new conversation: a missing transcript is rejected before launch.
        if let (SessionRequest::Resume(session), Some(store), Some(state)) = (
            &request.session,
            self.session_store.clone(),
            self.state_directory
                .as_ref()
                .map(|state| state.path.clone()),
        ) {
            let id = session.native_id.clone();
            let found = tokio::select! {
                _ = cancellation.cancelled() => None,
                result = tokio::task::spawn_blocking(move || native_session_present(&store, &state, &id)) => Some(result.unwrap_or(Err(()))),
            };
            match found {
                None => {
                    return self.result(
                        &request,
                        ExecutionOutcome::Cancelled,
                        None,
                        vec![],
                        CleanupState::Settled,
                        SessionDisposition::NotApplicable,
                    );
                }
                Some(Ok(true)) => {}
                Some(Ok(false)) => {
                    return fail(
                        "native_session_missing",
                        CleanupState::Settled,
                        SessionDisposition::NotApplicable,
                        None,
                    );
                }
                Some(Err(())) => {
                    return fail(
                        "native_session_unverifiable",
                        CleanupState::Settled,
                        SessionDisposition::NotApplicable,
                        None,
                    );
                }
            }
        }
        let metadata = self.metadata();
        let emit = |kind| {
            events(ExecutionEvent {
                run_id: request.run_id.clone(),
                attempt_id: request.attempt_id.clone(),
                metadata: metadata.clone(),
                kind,
            })
        };
        let known = match &request.session {
            SessionRequest::New => None,
            SessionRequest::Resume(session) => Some(session.clone()),
        };
        let start_observer: ProcessStartObserver = {
            let events = Arc::clone(&events);
            let metadata = metadata.clone();
            let run_id = request.run_id.clone();
            let attempt_id = request.attempt_id.clone();
            let known = known.clone();
            let cancellation = cancellation.clone();
            Arc::new(move |_| {
                let send = |kind| {
                    events(ExecutionEvent {
                        run_id: run_id.clone(),
                        attempt_id: attempt_id.clone(),
                        metadata: metadata.clone(),
                        kind,
                    })
                };
                if send(ExecutionEventKind::Started).is_err()
                    || known.as_ref().is_some_and(|session| {
                        send(ExecutionEventKind::SessionObserved(session.clone())).is_err()
                    })
                {
                    cancellation.cancel();
                }
            })
        };
        let stream = Arc::new(Mutex::new(StreamSession {
            bytes: Vec::new(),
            stderr_bytes: Vec::new(),
            child_started: false,
            child_exited: false,
            session: known.clone(),
            emitted: known.is_some(),
            thread_seen: false,
            turn_started: false,
            terminal_seen: false,
        }));
        let observer: Option<ProcessObserver> = {
            let stream = Arc::clone(&stream);
            let events = Arc::clone(&events);
            let metadata = metadata.clone();
            let run_id = request.run_id.clone();
            let attempt_id = request.attempt_id.clone();
            let realm = self.state_realm.clone();
            let runtime = metadata.runtime.clone();
            let cli_kind = self.kind;
            let has_helper = self.helper_program.is_some();
            Some(Arc::new(move |kind, bytes| {
                let mut state = stream.lock().map_err(|_| ObserverError)?;
                if kind == OutputStream::Stderr {
                    if !has_helper {
                        return Ok(());
                    }
                    state.stderr_bytes.extend_from_slice(bytes);
                    while let Some(end) = state.stderr_bytes.iter().position(|byte| *byte == b'\n')
                    {
                        let line: Vec<_> = state.stderr_bytes.drain(..=end).collect();
                        let message = if line == b"\x1eagent-runner:child-started\n"
                            && !state.child_started
                        {
                            state.child_started = true;
                            Some("cli_child_started")
                        } else if line == b"\x1eagent-runner:child-exited\n"
                            && state.child_started
                            && !state.child_exited
                        {
                            state.child_exited = true;
                            Some("cli_child_exited")
                        } else {
                            None
                        };
                        if let Some(message) = message {
                            events(ExecutionEvent {
                                run_id: run_id.clone(),
                                attempt_id: attempt_id.clone(),
                                metadata: metadata.clone(),
                                kind: ExecutionEventKind::Progress {
                                    message: message.into(),
                                },
                            })
                            .map_err(|_| ObserverError)?;
                        }
                    }
                    return Ok(());
                }
                if cli_kind != CliKind::Codex {
                    return Ok(());
                }
                state.bytes.extend_from_slice(bytes);
                while let Some(end) = state.bytes.iter().position(|byte| *byte == b'\n') {
                    let line: Vec<_> = state.bytes.drain(..=end).collect();
                    if line.iter().all(u8::is_ascii_whitespace) {
                        continue;
                    }
                    let event =
                        crate::validation::parse_unique_json(&line).map_err(|_| ObserverError)?;
                    let event_type = event
                        .get("type")
                        .and_then(Value::as_str)
                        .ok_or(ObserverError)?;
                    if state.terminal_seen {
                        return Err(ObserverError);
                    }
                    if event_type == "thread.started" {
                        if state.thread_seen || state.turn_started {
                            return Err(ObserverError);
                        }
                        state.thread_seen = true;
                        let id = event
                            .get("thread_id")
                            .and_then(Value::as_str)
                            .filter(|id| valid_uuid(id))
                            .ok_or(ObserverError)?;
                        if state
                            .session
                            .as_ref()
                            .is_some_and(|session| session.native_id != id)
                        {
                            return Err(ObserverError);
                        }
                        let session = SessionRef {
                            runtime: runtime.clone(),
                            state_realm: realm.clone(),
                            native_id: id.into(),
                        };
                        state.session = Some(session.clone());
                        if !state.emitted {
                            events(ExecutionEvent {
                                run_id: run_id.clone(),
                                attempt_id: attempt_id.clone(),
                                metadata: metadata.clone(),
                                kind: ExecutionEventKind::SessionObserved(session),
                            })
                            .map_err(|_| ObserverError)?;
                            state.emitted = true;
                        }
                    } else if event_type == "turn.started" {
                        if !state.thread_seen || state.turn_started {
                            return Err(ObserverError);
                        }
                        state.turn_started = true;
                    } else if event_type == "turn.completed" || event_type == "turn.failed" {
                        if !state.turn_started {
                            return Err(ObserverError);
                        }
                        state.terminal_seen = true;
                    }
                }
                Ok(())
            }))
        };
        let report = ProcessSupervisor::run_with_start_observer(
            self.spec(
                args,
                Some(directory_path),
                request.prompt.as_bytes().to_vec(),
                self.limits,
            ),
            cancellation.clone(),
            observer,
            Some(start_observer),
            Some(opened),
        )
        .await;
        let mut session = stream.lock().ok().and_then(|state| state.session.clone());
        let report = match report {
            Ok(report) => report,
            Err(_) => {
                return fail(
                    "cli_start_failed",
                    CleanupState::Uncertain,
                    SessionDisposition::Uncertain,
                    session,
                );
            }
        };
        let with_version = |mut warnings: Vec<ExecutionWarning>| {
            if let Some(version) = &unpinned_version {
                warnings.push(ExecutionWarning {
                    code: "cli_version_unpinned".into(),
                    message: format!("The profile accepts any CLI version; observed `{version}`"),
                });
            }
            warnings
        };
        let observed_warnings = with_version(observed_warnings(self.kind, &report.stdout.bytes));
        let fail_observed = |code: &str, session| {
            self.result(
                &request,
                ExecutionOutcome::Failed(BackendError::ExecutionFailed {
                    code: code.into(),
                    message: "CLI execution did not produce a complete result".into(),
                }),
                session,
                observed_warnings.clone(),
                CleanupState::Uncertain,
                SessionDisposition::Uncertain,
            )
        };
        if cancellation.is_cancelled() || report.reason == StopReason::Cancelled {
            return self.result(
                &request,
                ExecutionOutcome::Cancelled,
                session,
                observed_warnings,
                CleanupState::Uncertain,
                SessionDisposition::Uncertain,
            );
        }
        if report.reason != StopReason::Exited
            || !report.direct_io_settled()
            || report.input != InputCompletion::Complete
            || !report.exit.is_some_and(|exit| exit.code == Some(0))
        {
            return fail_observed("cli_execution_failed", session);
        }
        let decoded = match self.kind {
            CliKind::Codex => decode_codex(&report.stdout.bytes),
            CliKind::Claude => decode_claude(&report.stdout.bytes),
        };
        let decoded = match decoded {
            Ok(decoded) => decoded,
            Err(code) => {
                return fail_observed(code, session);
            }
        };
        if session
            .as_ref()
            .is_some_and(|existing| existing.native_id != decoded.native_id)
        {
            return fail_observed("session_identity_mismatch", session);
        }
        if session.is_none() {
            let observed = SessionRef {
                runtime: metadata.runtime.clone(),
                state_realm: self.state_realm.clone(),
                native_id: decoded.native_id,
            };
            if emit(ExecutionEventKind::SessionObserved(observed.clone())).is_err() {
                return fail_observed("event_sink_unavailable", None);
            }
            session = Some(observed);
        }
        self.result(
            &request,
            ExecutionOutcome::Succeeded {
                output: decoded.output,
            },
            session,
            with_version(decoded.warnings),
            CleanupState::Settled,
            SessionDisposition::Reusable,
        )
    }
}

impl ExecutionBackend for CliBackend {
    fn metadata(&self) -> ExecutionMetadata {
        ExecutionMetadata {
            runtime: self.kind.runtime().into(),
            synthetic: false,
            scenario: None,
        }
    }
    fn state_realm(&self) -> &str {
        &self.state_realm
    }
    fn state_directory(&self) -> Option<&std::path::Path> {
        self.state_directory
            .as_ref()
            .map(|state| state.path.as_path())
    }
    fn state_owner_root(&self) -> Option<&std::path::Path> {
        self.state_owner_root.as_deref()
    }
    fn independent_new_sessions(&self) -> bool {
        true
    }
    fn requires_workspace(&self) -> bool {
        true
    }
    fn minimum_events(&self) -> usize {
        6
    }
    fn cleanup_budget(&self) -> Option<Duration> {
        CliBackend::cleanup_budget(self)
    }
    fn preflight<'a>(
        &'a self,
        cancellation: CancellationToken,
    ) -> Pin<Box<dyn Future<Output = Result<(), BackendError>> + Send + 'a>> {
        Box::pin(async move { self.probe(cancellation).await.map(|_| ()) })
    }
    fn execute<'a>(
        &'a self,
        request: ExecutionRequest,
        events: EventSink,
        cancellation: CancellationToken,
    ) -> Pin<Box<dyn Future<Output = ExecutionResult> + Send + 'a>> {
        Box::pin(self.run(request, events, cancellation))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ExecutableIdentity {
    device: u64,
    inode: u64,
    size: u64,
    modified: i64,
    modified_nsec: i64,
    changed: i64,
    changed_nsec: i64,
}
impl ExecutableIdentity {
    fn inspect(path: &std::path::Path) -> Option<Self> {
        #[cfg(not(target_os = "linux"))]
        {
            let _ = path;
            None
        }
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::MetadataExt;
            let metadata = std::fs::metadata(path).ok()?;
            if !metadata.is_file() || metadata.mode() & 0o111 == 0 {
                return None;
            }
            Some(Self {
                device: metadata.dev(),
                inode: metadata.ino(),
                size: metadata.size(),
                modified: metadata.mtime(),
                modified_nsec: metadata.mtime_nsec(),
                changed: metadata.ctime(),
                changed_nsec: metadata.ctime_nsec(),
            })
        }
    }
}

struct StreamSession {
    bytes: Vec<u8>,
    stderr_bytes: Vec<u8>,
    child_started: bool,
    child_exited: bool,
    session: Option<SessionRef>,
    emitted: bool,
    thread_seen: bool,
    turn_started: bool,
    terminal_seen: bool,
}
struct Decoded {
    native_id: String,
    output: String,
    warnings: Vec<ExecutionWarning>,
}

fn native_realm(kind: CliKind, state: &crate::workspace::Directory) -> String {
    format!("{}:{:x}:{:x}", kind.runtime(), state.device, state.inode)
}

fn account_home() -> Option<PathBuf> {
    #[cfg(target_os = "linux")]
    {
        use std::{ffi::CStr, os::unix::ffi::OsStringExt};
        let mut entry = std::mem::MaybeUninit::<libc::passwd>::zeroed();
        let mut buffer = vec![0_u8; 16 * 1024];
        let mut result = std::ptr::null_mut();
        // SAFETY: The passwd record, result pointer, and backing buffer remain valid during getpwuid_r.
        let status = unsafe {
            libc::getpwuid_r(
                libc::geteuid(),
                entry.as_mut_ptr(),
                buffer.as_mut_ptr().cast(),
                buffer.len(),
                &mut result,
            )
        };
        if status != 0 || result.is_null() {
            return None;
        }
        // SAFETY: A successful getpwuid_r result points into the live record and backing buffer.
        let directory = unsafe { (*result).pw_dir };
        if directory.is_null() {
            return None;
        }
        // SAFETY: A non-null pw_dir returned by getpwuid_r is NUL-terminated in the backing buffer.
        let raw = unsafe { CStr::from_ptr(directory) }.to_bytes().to_vec();
        let home = PathBuf::from(OsString::from_vec(raw));
        if !home.is_absolute() {
            return None;
        }
        std::fs::canonicalize(home)
            .ok()
            .filter(|path| path.is_dir())
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

impl CliPlatform {
    fn matches(&self) -> bool {
        if self
            .architecture
            .as_deref()
            .is_some_and(|architecture| architecture != std::env::consts::ARCH)
        {
            return false;
        }
        if self.kernel_release_contains.is_empty() {
            return true;
        }
        std::fs::read_to_string("/proc/sys/kernel/osrelease").is_ok_and(|release| {
            let release = release.to_ascii_lowercase();
            self.kernel_release_contains
                .iter()
                .all(|needle| release.contains(&needle.to_ascii_lowercase()))
        })
    }
}

fn validate_invocation(settings: &CliSettings) -> Result<(), &'static str> {
    let valid_arg = |arg: &String| !arg.is_empty() && !arg.contains('\0');
    let token_count = |args: &[String]| {
        args.iter()
            .filter(|arg| *arg == NATIVE_SESSION_TOKEN)
            .count()
    };
    let env = &settings.environment;
    let valid_name =
        |name: &String| !name.is_empty() && !name.contains('=') && !name.contains('\0');
    let label = settings.compatibility.as_str();
    if label.is_empty()
        || label.len() > 128
        || !label.bytes().all(|byte| byte.is_ascii_graphic())
        || settings.version.args.is_empty()
        || match (
            &settings.version.expected_stdout,
            settings.version.accept_any,
        ) {
            (Some(expected), false) => expected.trim().is_empty(),
            (None, true) => false,
            _ => true,
        }
        || !settings.version.args.iter().all(valid_arg)
        || !settings.new_args.iter().all(valid_arg)
        || !settings.resume_args.iter().all(valid_arg)
        || token_count(&settings.new_args) != 0
        || token_count(&settings.version.args) != 0
        || token_count(&settings.resume_args) != 1
    {
        return Err("invalid CLI invocation");
    }
    if !env.inherit.iter().any(|name| name == "HOME")
        || !env.inherit.iter().any(|name| name == "PATH")
        || !env.inherit.iter().all(valid_name)
        || !valid_name(&env.state_directory_variable)
        || env.default_state_directory.as_os_str().is_empty()
        || !relative_without_parent(&env.default_state_directory)
        || !env.forbidden_prefixes.iter().all(valid_name)
        || !env.forbidden_names.iter().all(valid_name)
    {
        return Err("invalid CLI environment");
    }
    if !settings
        .forbidden_workspace_entries
        .iter()
        .all(|name| valid_name(name) && !name.contains('/') && name != "." && name != "..")
    {
        return Err("invalid CLI workspace guard");
    }
    if let Some(store) = &settings.session_store
        && (!relative_without_parent(&store.directory)
            || store.file_suffix.is_empty()
            || store.file_suffix.contains('/')
            || store.max_depth == 0
            || store.max_entries == 0)
    {
        return Err("invalid CLI session store");
    }
    Ok(())
}

fn relative_without_parent(path: &std::path::Path) -> bool {
    path.components().all(|component| {
        matches!(
            component,
            std::path::Component::Normal(_) | std::path::Component::CurDir
        )
    })
}

fn no_forbidden_workspace_entries(
    workspace: &std::path::Path,
    global_state: Option<&std::path::Path>,
    entries: &[OsString],
) -> bool {
    workspace.ancestors().all(|ancestor| {
        entries.iter().all(|name| {
            let candidate = ancestor.join(name);
            if global_state == Some(candidate.as_path()) {
                return true;
            }
            matches!(std::fs::symlink_metadata(candidate), Err(error) if error.kind() == std::io::ErrorKind::NotFound)
        })
    })
}

/// Searches the native store without following symlinks; `Err` means presence could not be decided.
fn native_session_present(
    store: &CliSessionStore,
    state: &std::path::Path,
    native_id: &str,
) -> Result<bool, ()> {
    let wanted = format!("{native_id}{}", store.file_suffix);
    let root = state.join(&store.directory);
    match std::fs::symlink_metadata(&root) {
        Ok(metadata) if metadata.is_dir() => {}
        Ok(_) => return Err(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(_) => return Err(()),
    }
    let mut pending = vec![(root, 1_usize)];
    let mut inspected = 0_usize;
    while let Some((directory, depth)) = pending.pop() {
        for entry in std::fs::read_dir(&directory).map_err(|_| ())? {
            inspected += 1;
            if inspected > store.max_entries {
                return Err(());
            }
            let entry = entry.map_err(|_| ())?;
            let kind = entry.file_type().map_err(|_| ())?;
            let name = entry.file_name();
            if kind.is_file() {
                let Some(name) = name.to_str() else { continue };
                // The ID must be the whole stem or follow a separator, never a longer identifier.
                if let Some(prefix) = name.strip_suffix(wanted.as_str())
                    && (prefix.is_empty() || prefix.ends_with(['-', '_', '.']))
                {
                    return Ok(true);
                }
            } else if kind.is_dir() && depth < store.max_depth {
                pending.push((entry.path(), depth + 1));
            }
        }
    }
    Ok(false)
}

fn absolute_search_path(path: &std::ffi::OsStr) -> bool {
    std::env::split_paths(path).all(|entry| entry.is_absolute())
}

fn forbidden_override(environment: &CliEnvironment, key: &std::ffi::OsStr) -> bool {
    let Some(name) = key.to_str() else {
        return true;
    };
    environment
        .forbidden_prefixes
        .iter()
        .any(|prefix| name.starts_with(prefix.as_str()))
        || environment
            .forbidden_names
            .iter()
            .any(|forbidden| forbidden == name)
}

fn open_pinned_directory(directory: &crate::core::WorkingDirectory) -> Result<std::fs::File, ()> {
    #[cfg(not(target_os = "linux"))]
    {
        let _ = directory;
        Err(())
    }
    #[cfg(target_os = "linux")]
    {
        use std::os::{
            fd::AsRawFd,
            unix::fs::{MetadataExt, OpenOptionsExt},
        };
        let file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&directory.path)
            .map_err(|_| ())?;
        let metadata = file.metadata().map_err(|_| ())?;
        if !metadata.is_dir()
            || metadata.dev() != directory.device
            || metadata.ino() != directory.inode
        {
            return Err(());
        }
        if file.as_raw_fd() <= 2 {
            let fd = rustix::io::fcntl_dupfd_cloexec(&file, 3).map_err(|_| ())?;
            Ok(std::fs::File::from(fd))
        } else {
            Ok(file)
        }
    }
}

fn valid_uuid(id: &str) -> bool {
    id.len() == 36
        && id.bytes().enumerate().all(|(index, byte)| {
            if [8, 13, 18, 23].contains(&index) {
                byte == b'-'
            } else {
                byte.is_ascii_hexdigit()
            }
        })
}

fn warning(code: &str) -> ExecutionWarning {
    ExecutionWarning {
        code: code.into(),
        message: "A non-fatal CLI warning was observed".into(),
    }
}

fn observed_warnings(kind: CliKind, bytes: &[u8]) -> Vec<ExecutionWarning> {
    let has_warning = match kind {
        CliKind::Codex => bytes
            .split_inclusive(|byte| *byte == b'\n')
            .filter(|line| line.ends_with(b"\n"))
            .filter_map(|line| crate::validation::parse_unique_json(line).ok())
            .filter(|value| value.get("type").and_then(Value::as_str) == Some("item.completed"))
            .filter_map(|value| value.get("item").cloned())
            .any(|item| {
                item.get("status").and_then(Value::as_str) == Some("failed")
                    || item
                        .get("exit_code")
                        .and_then(Value::as_i64)
                        .is_some_and(|code| code != 0)
            }),
        CliKind::Claude => crate::validation::parse_unique_json(bytes)
            .ok()
            .and_then(|value| {
                value
                    .get("permission_denials")
                    .and_then(Value::as_array)
                    .map(|denials| !denials.is_empty())
            })
            .unwrap_or(false),
    };
    if has_warning {
        vec![warning(match kind {
            CliKind::Codex => "tool_error",
            CliKind::Claude => "permission_denied",
        })]
    } else {
        vec![]
    }
}

fn decode_codex(bytes: &[u8]) -> Result<Decoded, &'static str> {
    let mut native = None;
    let mut started = false;
    let mut completed = false;
    let mut output = None;
    let mut tool_error = false;
    for line in bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.iter().all(u8::is_ascii_whitespace))
    {
        let event = crate::validation::parse_unique_json(line).map_err(|_| "invalid_cli_output")?;
        match event
            .get("type")
            .and_then(Value::as_str)
            .ok_or("invalid_cli_output")?
        {
            "thread.started" if !started && native.is_none() => {
                let id = event
                    .get("thread_id")
                    .and_then(Value::as_str)
                    .filter(|id| valid_uuid(id))
                    .ok_or("invalid_cli_output")?;
                native = Some(id.to_owned());
            }
            "turn.started" if native.is_some() && !started && !completed => started = true,
            "turn.completed" if started && !completed => completed = true,
            "turn.failed" => return Err("cli_turn_failed"),
            "item.completed" if started && !completed => {
                let item = event
                    .get("item")
                    .and_then(Value::as_object)
                    .ok_or("invalid_cli_output")?;
                if item.get("type").and_then(Value::as_str) == Some("agent_message") {
                    output = Some(
                        item.get("text")
                            .and_then(Value::as_str)
                            .ok_or("invalid_cli_output")?
                            .to_owned(),
                    );
                }
                if item.get("status").and_then(Value::as_str) == Some("failed")
                    || item
                        .get("exit_code")
                        .and_then(Value::as_i64)
                        .is_some_and(|code| code != 0)
                {
                    tool_error = true;
                }
            }
            "item.started" | "item.updated" if started && !completed => {}
            "error" => return Err("cli_reported_error"),
            _ => return Err("invalid_cli_output"),
        }
    }
    if !started || !completed {
        return Err("incomplete_result");
    }
    Ok(Decoded {
        native_id: native.ok_or("incomplete_result")?,
        output: output.ok_or("incomplete_result")?,
        warnings: if tool_error {
            vec![warning("tool_error")]
        } else {
            vec![]
        },
    })
}

fn decode_claude(bytes: &[u8]) -> Result<Decoded, &'static str> {
    let value = crate::validation::parse_unique_json(bytes).map_err(|_| "invalid_cli_output")?;
    if value.get("type").and_then(Value::as_str) != Some("result")
        || value.get("subtype").and_then(Value::as_str) != Some("success")
        || value.get("is_error").and_then(Value::as_bool) != Some(false)
    {
        return Err("cli_result_failed");
    }
    let native_id = value
        .get("session_id")
        .and_then(Value::as_str)
        .filter(|id| valid_uuid(id))
        .ok_or("incomplete_result")?
        .to_owned();
    let output = value
        .get("result")
        .and_then(Value::as_str)
        .ok_or("incomplete_result")?
        .to_owned();
    let warnings = if value
        .get("permission_denials")
        .and_then(Value::as_array)
        .is_some_and(|denials| !denials.is_empty())
    {
        vec![warning("permission_denied")]
    } else {
        vec![]
    };
    Ok(Decoded {
        native_id,
        output,
        warnings,
    })
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::{os::unix::fs::PermissionsExt, sync::Mutex};
    use tempfile::TempDir;

    const ID: &str = "12345678-1234-4234-8234-123456789abc";

    fn example(kind: CliKind) -> CliSettings {
        let text = match kind {
            CliKind::Codex => include_str!("../../examples/cli-profiles/codex.json"),
            CliKind::Claude => include_str!("../../examples/cli-profiles/claude.json"),
        };
        serde_json::from_str(text).unwrap()
    }

    fn fixture(kind: CliKind, version_ok: bool) -> (TempDir, CliBackend) {
        let temp = tempfile::tempdir().unwrap();
        let settings = example(kind);
        validate_invocation(&settings).unwrap();
        let program = temp.path().join("fixture");
        let version = if version_ok {
            settings.version.expected_stdout.clone().unwrap()
        } else {
            "unverified-version".into()
        };
        let output = match kind {
            CliKind::Codex => format!(
                "{{\"type\":\"thread.started\",\"thread_id\":\"{ID}\"}}\n{{\"type\":\"turn.started\"}}\n{{\"type\":\"item.completed\",\"item\":{{\"type\":\"agent_message\",\"text\":\"done\"}}}}\n{{\"type\":\"turn.completed\"}}"
            ),
            CliKind::Claude => format!(
                "{{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"session_id\":\"{ID}\",\"result\":\"done\",\"permission_denials\":[]}}"
            ),
        };
        let script = format!(
            "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then printf '%s\\n' '{version}'; exit 0; fi\nif [ -f '{0}/missing_session' ]; then printf x >> '{0}/missing_invocations'; cat > '{0}/missing_input'; printf '%s\\n' 'session not found' >&2; exit 1; fi\ncat > '{0}/input'\npwd > '{0}/cwd'\nprintf '%s\\n' \"$@\" > '{0}/args'\ncat <<'RESULT'\n{output}\nRESULT\n",
            temp.path().display()
        );
        std::fs::write(&program, script).unwrap();
        std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o700)).unwrap();
        let os_args = |args: &[String]| args.iter().map(OsString::from).collect::<Vec<_>>();
        let backend = CliBackend {
            kind,
            program,
            program_identity: None,
            helper_program: None,
            helper_identity: None,
            state_realm: format!("{}:fixture", kind.runtime()),
            state_directory: None,
            state_owner_root: None,
            environment: vec![
                ("PATH".into(), "/usr/bin:/bin".into()),
                ("HOME".into(), temp.path().into()),
            ],
            new_args: os_args(&settings.new_args),
            resume_args: os_args(&settings.resume_args),
            version_args: os_args(&settings.version.args),
            expected_version: settings.version.expected_stdout,
            forbidden_workspace_entries: vec![],
            session_store: None,
            preflight_timeout: Duration::from_secs(5),
            limits: ProcessLimits {
                max_args: 32,
                max_env: 8,
                max_spec_bytes: 8192,
                max_stdin_bytes: 4096,
                max_stdout_bytes: 8192,
                max_stderr_bytes: 8192,
                max_line_bytes: 4096,
                timeout: Duration::from_secs(5),
                signal_grace: Duration::from_millis(200),
                kill_grace: Duration::from_millis(200),
                pipe_grace: Duration::from_millis(200),
            },
        };
        (temp, backend)
    }

    fn request(directory: PathBuf) -> ExecutionRequest {
        use std::os::unix::fs::MetadataExt;
        let metadata = std::fs::metadata(&directory).unwrap();
        let directory = crate::core::WorkingDirectory {
            path: directory,
            device: metadata.dev(),
            inode: metadata.ino(),
        };
        ExecutionRequest {
            run_id: "run-test".into(),
            attempt_id: "attempt-test".into(),
            workspace_id: "workspace".into(),
            working_directory: Some(directory),
            session: SessionRequest::New,
            prompt: "literal prompt\n".into(),
        }
    }

    #[tokio::test]
    async fn cli_fixtures_preserve_input_cwd_identity_and_restricted_arguments() {
        for kind in [CliKind::Codex, CliKind::Claude] {
            let (temp, backend) = fixture(kind, true);
            let observed = Arc::new(Mutex::new(Vec::new()));
            let recorded = Arc::clone(&observed);
            let sink: EventSink = Arc::new(move |event| {
                recorded.lock().unwrap().push(event.kind);
                Ok(())
            });
            let mut input = request(temp.path().to_path_buf());
            let first = backend
                .execute(input.clone(), sink.clone(), CancellationToken::new())
                .await;
            assert!(
                matches!(&first.outcome, ExecutionOutcome::Succeeded { output } if output == "done")
            );
            assert_eq!(first.session.as_ref().unwrap().native_id, ID);
            assert_eq!(
                std::fs::read_to_string(temp.path().join("input")).unwrap(),
                "literal prompt\n"
            );
            assert_eq!(
                std::fs::read_to_string(temp.path().join("cwd"))
                    .unwrap()
                    .trim(),
                temp.path().to_str().unwrap()
            );
            let args = std::fs::read_to_string(temp.path().join("args")).unwrap();
            match kind {
                CliKind::Codex => assert!(
                    args.contains("--no-daemon\n")
                        && args.contains("--ignore-user-config\n")
                        && args.contains("read-only\n")
                        && !args.contains("--ephemeral\n")
                ),
                CliKind::Claude => assert!(
                    args.contains("--safe-mode\n")
                        && args.contains("--restricted\n")
                        && !args.contains("--bare\n")
                ),
            }
            input.session = SessionRequest::Resume(first.session.unwrap());
            let second = backend.execute(input, sink, CancellationToken::new()).await;
            assert!(matches!(second.outcome, ExecutionOutcome::Succeeded { .. }));
            let args = std::fs::read_to_string(temp.path().join("args")).unwrap();
            assert!(args.contains("resume\n") || args.contains("--resume\n"));
            assert!(observed.lock().unwrap().iter().any(|kind| matches!(kind, ExecutionEventKind::SessionObserved(session) if session.native_id == ID)));
        }
    }

    #[tokio::test]
    async fn oversized_prompt_is_proven_unstarted_for_new_and_resume() {
        for kind in [CliKind::Codex, CliKind::Claude] {
            let (temp, mut backend) = fixture(kind, true);
            backend.limits.max_stdin_bytes = 5;
            let sink: EventSink = Arc::new(|_| Ok(()));
            for session in [
                SessionRequest::New,
                SessionRequest::Resume(SessionRef {
                    runtime: backend.metadata().runtime,
                    state_realm: backend.state_realm().to_owned(),
                    native_id: ID.into(),
                }),
            ] {
                let mut input = request(temp.path().to_path_buf());
                input.session = session;
                let result = backend
                    .execute(input, sink.clone(), CancellationToken::new())
                    .await;
                assert!(matches!(result.outcome, ExecutionOutcome::Failed(_)));
                assert_eq!(result.dispatch, DispatchStatus::ProvenUnstarted);
                assert_eq!(result.cleanup, CleanupState::Settled);
                assert_eq!(
                    result.session_disposition,
                    SessionDisposition::NotApplicable
                );
                assert!(result.session.is_none());
                assert!(!temp.path().join("input").exists());
            }
        }
    }

    #[tokio::test]
    async fn mismatched_resumed_session_is_not_reported_as_success() {
        for kind in [CliKind::Codex, CliKind::Claude] {
            let (temp, backend) = fixture(kind, true);
            let mut input = request(temp.path().to_path_buf());
            input.session = SessionRequest::Resume(SessionRef {
                runtime: backend.metadata().runtime,
                state_realm: backend.state_realm().to_owned(),
                native_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa".into(),
            });
            let sink: EventSink = Arc::new(|_| Ok(()));
            let result = backend.execute(input, sink, CancellationToken::new()).await;
            assert!(matches!(result.outcome, ExecutionOutcome::Failed(_)));
            assert_eq!(
                result.dispatch,
                DispatchStatus::StartedOrUnknown,
                "{kind:?}: {:?}",
                result.outcome
            );
            assert_eq!(result.cleanup, CleanupState::Uncertain);
            assert_eq!(result.session_disposition, SessionDisposition::Uncertain);
            assert!(temp.path().join("input").exists());
        }
    }

    #[tokio::test]
    async fn unverified_version_never_receives_the_prompt() {
        for kind in [CliKind::Codex, CliKind::Claude] {
            let (temp, backend) = fixture(kind, false);
            let sink: EventSink = Arc::new(|_| Ok(()));
            let result = backend
                .execute(
                    request(temp.path().to_path_buf()),
                    sink,
                    CancellationToken::new(),
                )
                .await;
            assert!(matches!(result.outcome, ExecutionOutcome::Failed(_)));
            assert!(!temp.path().join("input").exists());
        }
    }

    #[tokio::test]
    async fn accept_any_runs_another_version_and_reports_it_as_a_warning() {
        for kind in [CliKind::Codex, CliKind::Claude] {
            let (temp, mut backend) = fixture(kind, false);
            backend.expected_version = None;
            let sink: EventSink = Arc::new(|_| Ok(()));
            let result = backend
                .execute(
                    request(temp.path().to_path_buf()),
                    sink,
                    CancellationToken::new(),
                )
                .await;
            assert!(
                matches!(result.outcome, ExecutionOutcome::Succeeded { .. }),
                "{kind:?}: {:?}",
                result.outcome
            );
            assert!(
                result
                    .warnings
                    .iter()
                    .any(|warning| warning.code == "cli_version_unpinned"
                        && warning.message.contains("unverified-version"))
            );
            assert!(temp.path().join("input").exists());
        }
    }

    #[tokio::test]
    async fn pinned_version_reports_no_version_warning() {
        let (temp, backend) = fixture(CliKind::Codex, true);
        let sink: EventSink = Arc::new(|_| Ok(()));
        let result = backend
            .execute(
                request(temp.path().to_path_buf()),
                sink,
                CancellationToken::new(),
            )
            .await;
        assert!(matches!(result.outcome, ExecutionOutcome::Succeeded { .. }));
        assert!(
            !result
                .warnings
                .iter()
                .any(|warning| warning.code == "cli_version_unpinned")
        );
    }

    #[test]
    fn version_policy_requires_exactly_one_of_a_pinned_version_and_accept_any() {
        let mut settings = example(CliKind::Codex);
        settings.version.accept_any = true;
        assert!(validate_invocation(&settings).is_err());
        settings.version.expected_stdout = None;
        assert!(validate_invocation(&settings).is_ok());
        settings.version.accept_any = false;
        assert!(validate_invocation(&settings).is_err());
        settings.version.expected_stdout = Some(" ".into());
        assert!(validate_invocation(&settings).is_err());
    }

    #[tokio::test]
    async fn replaced_executable_is_rejected_even_when_it_reports_the_same_version() {
        let (temp, mut backend) = fixture(CliKind::Codex, true);
        backend.program_identity = ExecutableIdentity::inspect(&backend.program);
        let bytes = std::fs::read(&backend.program).unwrap();
        std::fs::rename(&backend.program, temp.path().join("old-fixture")).unwrap();
        std::fs::write(&backend.program, bytes).unwrap();
        std::fs::set_permissions(&backend.program, std::fs::Permissions::from_mode(0o700)).unwrap();
        let sink: EventSink = Arc::new(|_| Ok(()));
        let result = backend
            .execute(
                request(temp.path().to_path_buf()),
                sink,
                CancellationToken::new(),
            )
            .await;
        assert!(matches!(result.outcome, ExecutionOutcome::Failed(_)));
        assert!(!temp.path().join("input").exists());
    }

    #[tokio::test]
    async fn both_cli_fixtures_run_and_resume_through_durable_service() {
        use crate::{
            access::{
                AccessLimits, ClientAccessControl, ClientRegistration, CredentialSecret,
                ExecutionProfile, Scope,
            },
            application::{BackendProfile, RunLimits, RunService},
            storage::Namespace,
            workspace::{WorkspaceAccess, WorkspaceConfig, WorkspaceEntry, WorkspaceLocation},
        };
        use serde_json::json;
        for kind in [CliKind::Codex, CliKind::Claude] {
            let (temp, mut backend) = fixture(kind, true);
            backend.limits.max_stdin_bytes = 64;
            for dir in ["owner", "work"] {
                let path = temp.path().join(dir);
                std::fs::create_dir(&path).unwrap();
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
            }
            let runtime = backend.metadata().runtime;
            let realm = backend.state_realm().to_owned();
            let access = Arc::new(
                ClientAccessControl::new(
                    [ClientRegistration::new(
                        "credential",
                        CredentialSecret::new([7; 32]),
                        Namespace {
                            owner_id: "owner".into(),
                            client_id: "client".into(),
                        },
                        [Scope::RunStart, Scope::RunRead, Scope::SessionRead],
                        ["profile".into()],
                    )
                    .with_generation("v1")],
                    [ExecutionProfile::for_runtime(
                        "profile",
                        runtime,
                        ["work".into()],
                        4096,
                    )],
                    AccessLimits {
                        max_request_bytes: 8192,
                        max_skill_bytes: 1024,
                        max_identifier_bytes: 128,
                    },
                )
                .unwrap(),
            );
            let client = access.authenticate("credential", &[7; 32]).unwrap();
            let limits = RunLimits {
                max_outstanding: 4,
                max_workers: 1,
                database_queue: 16,
                event_buffer: 16,
                max_events_per_run: 16,
                max_event_bytes: 2048,
                max_result_bytes: 4096,
                max_page_size: 8,
                run_timeout: Duration::from_secs(10),
                cleanup_grace: Duration::from_secs(2),
                poll_interval: Duration::from_millis(5),
            };
            let workspaces = WorkspaceConfig {
                ownership_directory: temp.path().join("owner"),
                entries: vec![WorkspaceEntry {
                    id: "work".into(),
                    revision: "v1".into(),
                    access: WorkspaceAccess::Exclusive,
                    location: WorkspaceLocation::Existing {
                        path: temp.path().join("work"),
                    },
                }],
            };
            let service = RunService::open_with_workspaces(
                temp.path().join("runs.sqlite"),
                access,
                vec![BackendProfile {
                    profile_id: "profile".into(),
                    revision: "v1".into(),
                    state_realm: realm,
                    backend: Arc::new(backend),
                }],
                limits,
                None,
                None,
                Some(workspaces),
            )
            .await
            .unwrap();
            let body = |key: &str, session: Value| {
                serde_json::to_vec(&json!({"profile_id":"profile","workspace_id":"work","idempotency_key":key,"session":session,"input":{"request":"literal prompt"}})).unwrap()
            };
            let oversized = |key: &str, session: Value| {
                serde_json::to_vec(&json!({"profile_id":"profile","workspace_id":"work","idempotency_key":key,"session":session,"input":{"request":"x".repeat(256)}})).unwrap()
            };
            let rejected = service
                .start(&client, &oversized("rejected-new", json!({"mode":"new"})))
                .await
                .unwrap();
            let await_terminal = |id: String| {
                let service = &service;
                let client = &client;
                async move {
                    tokio::time::timeout(Duration::from_secs(3), async {
                        loop {
                            let state = service.get_run(client, &id).await.unwrap().unwrap().state;
                            if [
                                "succeeded",
                                "failed",
                                "cancelled",
                                "timed_out",
                                "interrupted",
                            ]
                            .contains(&state.as_str())
                            {
                                break state;
                            }
                            tokio::time::sleep(Duration::from_millis(5)).await;
                        }
                    })
                    .await
                    .unwrap()
                }
            };
            assert_eq!(await_terminal(rejected.run_id).await, "failed");
            let first = service
                .start(&client, &body("first", json!({"mode":"new"})))
                .await
                .unwrap();
            let first_state = await_terminal(first.run_id.clone()).await;
            if first_state != "succeeded" {
                let result = service
                    .get_result(&client, &first.run_id)
                    .await
                    .unwrap()
                    .unwrap();
                let data: Value = serde_json::from_slice(&result.payload).unwrap();
                panic!(
                    "{kind:?}: first run {first_state}, outcome {:?}",
                    data["details"]["outcome"]
                );
            }
            let session_id = first.session_id.unwrap();
            let rejected_resume = service
                .start(
                    &client,
                    &oversized(
                        "rejected-resume",
                        json!({"mode":"resume","session_id":session_id}),
                    ),
                )
                .await
                .unwrap();
            assert_eq!(await_terminal(rejected_resume.run_id).await, "failed");
            let second = service
                .start(
                    &client,
                    &body("second", json!({"mode":"resume","session_id":session_id})),
                )
                .await
                .unwrap();
            assert_eq!(await_terminal(second.run_id.clone()).await, "succeeded");
            let result = service
                .get_result(&client, &second.run_id)
                .await
                .unwrap()
                .unwrap();
            let result: Value = serde_json::from_slice(&result.payload).unwrap();
            assert_eq!(result["details"]["outcome"]["output"], "done");

            std::fs::write(temp.path().join("missing_session"), b"missing").unwrap();
            let missing_request = body(
                "missing-resume",
                json!({"mode":"resume","session_id":session_id}),
            );
            let missing = service.start(&client, &missing_request).await.unwrap();
            assert_eq!(await_terminal(missing.run_id.clone()).await, "failed");
            let replay = service.start(&client, &missing_request).await.unwrap();
            assert_eq!(replay.run_id, missing.run_id);
            assert_eq!(
                std::fs::read(temp.path().join("missing_invocations")).unwrap(),
                b"x"
            );
            // The fake CLI observes stdin before reporting a missing native session.
            assert!(
                !std::fs::read(temp.path().join("missing_input"))
                    .unwrap()
                    .is_empty()
            );
            let database = rusqlite::Connection::open(temp.path().join("runs.sqlite")).unwrap();
            let (held, uncertain): (bool, bool) = database
                .query_row(
                    "SELECT held,uncertain FROM run_workspaces WHERE run_id=?1",
                    [&missing.run_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert!(held && uncertain);
            drop(database);
            assert!(matches!(
                service
                    .start(&client, &body("after-missing", json!({"mode":"new"})))
                    .await,
                Err(crate::application::ServiceError::Access(
                    crate::access::AccessError::Storage(crate::storage::StorageError::SessionBusy)
                ))
            ));
            service.shutdown().await.unwrap();
        }
    }

    #[tokio::test]
    async fn pinned_cwd_uses_the_verified_directory_after_a_path_replacement() {
        use std::os::unix::fs::MetadataExt;
        let temp = tempfile::tempdir().unwrap();
        let original = temp.path().join("original");
        let moved = temp.path().join("moved");
        std::fs::create_dir(&original).unwrap();
        let metadata = std::fs::metadata(&original).unwrap();
        let directory = crate::core::WorkingDirectory {
            path: original.clone(),
            device: metadata.dev(),
            inode: metadata.ino(),
        };
        let opened = open_pinned_directory(&directory).unwrap();
        std::fs::rename(&original, &moved).unwrap();
        std::fs::create_dir(&original).unwrap();
        assert!(open_pinned_directory(&directory).is_err());
        let (_fixture, backend) = fixture(CliKind::Codex, true);
        let spec = ProcessSpec {
            program: "/bin/pwd".into(),
            args: vec![],
            cwd: Some(original),
            env: vec![("PATH".into(), "/usr/bin:/bin".into())],
            stdin: vec![],
            limits: backend.limits,
            stop_signal: StopSignal::Interrupt,
        };
        let report = ProcessSupervisor::run_with_start_observer(
            spec,
            CancellationToken::new(),
            None,
            None,
            Some(opened),
        )
        .await
        .unwrap();
        assert_eq!(report.reason, StopReason::Exited);
        assert_eq!(
            std::str::from_utf8(&report.stdout.bytes).unwrap().trim(),
            moved.to_str().unwrap()
        );
    }

    #[test]
    fn forbidden_workspace_entries_are_rejected_without_reading_their_contents() {
        let temp = tempfile::tempdir().unwrap();
        let work = temp.path().join("work");
        let global = temp.path().join(".codex");
        let entries = example(CliKind::Codex).forbidden_workspace_entries;
        let entries: Vec<OsString> = entries.iter().map(OsString::from).collect();
        std::fs::create_dir(&work).unwrap();
        std::fs::create_dir(&global).unwrap();
        assert!(no_forbidden_workspace_entries(
            &work,
            Some(&global),
            &entries
        ));
        std::fs::create_dir(work.join(".codex")).unwrap();
        assert!(!no_forbidden_workspace_entries(
            &work,
            Some(&global),
            &entries
        ));
        assert!(no_forbidden_workspace_entries(&work, Some(&global), &[]));
    }

    #[test]
    fn native_realm_uses_physical_state_identity_not_its_path_alias() {
        let temp = tempfile::tempdir().unwrap();
        let original = temp.path().join("original");
        let parent = temp.path().join("other");
        std::fs::create_dir(&original).unwrap();
        std::fs::create_dir(&parent).unwrap();
        let first = crate::workspace::Directory::inspect(&original).unwrap();
        let moved = parent.join("moved");
        std::fs::rename(original, &moved).unwrap();
        let second = crate::workspace::Directory::inspect(&moved).unwrap();
        assert_eq!(
            native_realm(CliKind::Codex, &first),
            native_realm(CliKind::Codex, &second)
        );
        assert_ne!(
            native_realm(CliKind::Codex, &first),
            native_realm(CliKind::Claude, &second)
        );
    }

    #[test]
    fn relative_search_paths_and_auth_overrides_are_not_silently_inherited() {
        assert!(absolute_search_path(std::ffi::OsStr::new("/usr/bin:/bin")));
        assert!(!absolute_search_path(std::ffi::OsStr::new(
            "/usr/bin:.:/bin"
        )));
        assert!(!absolute_search_path(std::ffi::OsStr::new(
            "/usr/bin::/bin"
        )));
        let codex = example(CliKind::Codex).environment;
        let claude = example(CliKind::Claude).environment;
        assert!(forbidden_override(
            &codex,
            std::ffi::OsStr::new("OPENAI_API_KEY")
        ));
        assert!(forbidden_override(
            &codex,
            std::ffi::OsStr::new("CODEX_API_KEY")
        ));
        assert!(forbidden_override(
            &claude,
            std::ffi::OsStr::new("CLAUDE_CODE_USE_BEDROCK")
        ));
        assert!(!forbidden_override(&claude, std::ffi::OsStr::new("HOME")));
        assert!(!forbidden_override(
            &claude,
            std::ffi::OsStr::new("AWS_ACCESS_KEY")
        ));
    }

    #[test]
    fn decoders_reject_missing_terminals_and_preserve_observed_warnings() {
        let incomplete = format!(
            "{{\"type\":\"thread.started\",\"thread_id\":\"{ID}\"}}\n{{\"type\":\"turn.started\"}}\n{{\"type\":\"item.completed\",\"item\":{{\"type\":\"agent_message\",\"text\":\"done\"}}}}\n"
        );
        assert!(matches!(
            decode_codex(incomplete.as_bytes()),
            Err("incomplete_result")
        ));
        let mixed = format!(
            "{{\"type\":\"thread.started\",\"thread_id\":\"{ID}\"}}\n{{\"type\":\"turn.started\"}}\n{{\"type\":\"item.completed\",\"item\":{{\"type\":\"command_execution\",\"exit_code\":1}}}}\n{{\"type\":\"item.completed\",\"item\":{{\"type\":\"agent_message\",\"text\":\"done\"}}}}\n{{\"type\":\"turn.completed\"}}\n"
        );
        let decoded = decode_codex(mixed.as_bytes()).unwrap();
        assert_eq!(decoded.output, "done");
        assert_eq!(decoded.warnings[0].code, "tool_error");
        assert_eq!(
            observed_warnings(CliKind::Codex, mixed.as_bytes())[0].code,
            "tool_error"
        );
        let late_identity = format!(
            "{{\"type\":\"turn.started\"}}\n{{\"type\":\"item.completed\",\"item\":{{\"type\":\"agent_message\",\"text\":\"done\"}}}}\n{{\"type\":\"turn.completed\"}}\n{{\"type\":\"thread.started\",\"thread_id\":\"{ID}\"}}\n"
        );
        assert!(decode_codex(late_identity.as_bytes()).is_err());
        let top_level_error = format!(
            "{{\"type\":\"thread.started\",\"thread_id\":\"{ID}\"}}\n{{\"type\":\"turn.started\"}}\n{{\"type\":\"error\"}}\n{{\"type\":\"turn.completed\"}}\n"
        );
        assert!(decode_codex(top_level_error.as_bytes()).is_err());
        let claude = format!(
            "{{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"session_id\":\"{ID}\",\"result\":\"done\",\"permission_denials\":[{{\"tool_name\":\"Write\"}}]}}"
        );
        assert_eq!(
            decode_claude(claude.as_bytes()).unwrap().warnings[0].code,
            "permission_denied"
        );
        assert_eq!(
            observed_warnings(CliKind::Claude, claude.as_bytes())[0].code,
            "permission_denied"
        );
        assert!(decode_claude(br#"{"type":"result","type":"result"}"#).is_err());
        assert!(decode_claude(br#"{"type":"result","subtype":"error","is_error":true}"#).is_err());
    }

    #[test]
    fn native_session_store_matches_whole_ids_within_bounds_without_following_links() {
        let temp = tempfile::tempdir().unwrap();
        let codex = example(CliKind::Codex).session_store.unwrap();
        let claude = example(CliKind::Claude).session_store.unwrap();
        let state = temp.path();
        assert_eq!(native_session_present(&codex, state, ID), Ok(false));
        let nested = state.join("sessions/2026/11/07");
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::write(
            nested.join(format!("rollout-2026-11-07T00-00-00-x{ID}.jsonl")),
            b"",
        )
        .unwrap();
        assert_eq!(native_session_present(&codex, state, ID), Ok(false));
        std::fs::write(
            nested.join(format!("rollout-2026-11-07T00-00-00-{ID}.jsonl")),
            b"",
        )
        .unwrap();
        assert_eq!(native_session_present(&codex, state, ID), Ok(true));
        let shallow = CliSessionStore {
            max_depth: 3,
            ..codex.clone()
        };
        assert_eq!(native_session_present(&shallow, state, ID), Ok(false));
        let tiny = CliSessionStore {
            max_entries: 2,
            ..codex.clone()
        };
        assert_eq!(native_session_present(&tiny, state, ID), Err(()));
        let project = state.join("projects/-work");
        std::fs::create_dir_all(&project).unwrap();
        let elsewhere = temp.path().join("elsewhere");
        std::fs::create_dir(&elsewhere).unwrap();
        std::fs::write(elsewhere.join(format!("{ID}.jsonl")), b"").unwrap();
        std::os::unix::fs::symlink(&elsewhere, project.join("linked")).unwrap();
        std::os::unix::fs::symlink(
            elsewhere.join(format!("{ID}.jsonl")),
            project.join(format!("other-{ID}.jsonl")),
        )
        .unwrap();
        assert_eq!(native_session_present(&claude, state, ID), Ok(false));
        std::fs::write(project.join(format!("{ID}.jsonl")), b"").unwrap();
        assert_eq!(native_session_present(&claude, state, ID), Ok(true));
    }

    #[tokio::test]
    async fn resume_with_missing_native_transcript_is_rejected_before_launch() {
        for kind in [CliKind::Codex, CliKind::Claude] {
            let (temp, mut backend) = fixture(kind, true);
            let state = temp.path().join("state");
            std::fs::create_dir(&state).unwrap();
            let store = example(kind).session_store.unwrap();
            backend.state_directory = Some(crate::workspace::Directory::inspect(&state).unwrap());
            backend.session_store = Some(store.clone());
            let work = temp.path().join("work");
            std::fs::create_dir(&work).unwrap();
            let mut input = request(work);
            input.session = SessionRequest::Resume(SessionRef {
                runtime: backend.metadata().runtime,
                state_realm: backend.state_realm().to_owned(),
                native_id: ID.into(),
            });
            let sink: EventSink = Arc::new(|_| Ok(()));
            let missing = backend
                .execute(input.clone(), sink.clone(), CancellationToken::new())
                .await;
            assert!(
                matches!(&missing.outcome, ExecutionOutcome::Failed(BackendError::ExecutionFailed { code, .. }) if code == "native_session_missing"),
                "{kind:?}: {:?}",
                missing.outcome
            );
            assert_eq!(missing.dispatch, DispatchStatus::ProvenUnstarted);
            assert!(!temp.path().join("input").exists());
            let transcript = state.join(&store.directory).join("fixture");
            std::fs::create_dir_all(&transcript).unwrap();
            std::fs::write(transcript.join(format!("{ID}{}", store.file_suffix)), b"").unwrap();
            let resumed = backend.execute(input, sink, CancellationToken::new()).await;
            assert!(
                matches!(resumed.outcome, ExecutionOutcome::Succeeded { .. }),
                "{kind:?}"
            );
            assert!(temp.path().join("input").exists());
        }
    }

    #[test]
    fn invocation_settings_reject_ambiguous_session_tokens_and_unsafe_paths() {
        for kind in [CliKind::Codex, CliKind::Claude] {
            assert!(validate_invocation(&example(kind)).is_ok());
            let mut settings = example(kind);
            settings
                .resume_args
                .retain(|arg| arg != NATIVE_SESSION_TOKEN);
            assert!(validate_invocation(&settings).is_err());
            let mut settings = example(kind);
            settings.resume_args.push(NATIVE_SESSION_TOKEN.into());
            assert!(validate_invocation(&settings).is_err());
            let mut settings = example(kind);
            settings.new_args.push(NATIVE_SESSION_TOKEN.into());
            assert!(validate_invocation(&settings).is_err());
            let mut settings = example(kind);
            settings.environment.inherit.retain(|name| name != "HOME");
            assert!(validate_invocation(&settings).is_err());
            let mut settings = example(kind);
            settings.environment.default_state_directory = "/etc".into();
            assert!(validate_invocation(&settings).is_err());
            let mut settings = example(kind);
            settings.session_store.as_mut().unwrap().directory = "../escape".into();
            assert!(validate_invocation(&settings).is_err());
            let mut settings = example(kind);
            settings.forbidden_workspace_entries = vec!["a/b".into()];
            assert!(validate_invocation(&settings).is_err());
            let mut settings = example(kind);
            settings.compatibility = String::new();
            assert!(validate_invocation(&settings).is_err());
        }
    }
}
