#![cfg(target_os = "linux")]

use std::{
    collections::HashMap,
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use agent_runner_server::{
    access::{
        AccessLimits, ClientAccessControl, ClientRegistration, CredentialSecret, ExecutionProfile,
        Scope,
    },
    application::{BackendProfile, RunLimits, RunService},
    core::{
        BackendError, CancellationToken, DispatchStatus, EventSink, ExecutionBackend,
        ExecutionEventKind, ExecutionMetadata, ExecutionOutcome, ExecutionRequest, ExecutionResult,
        SessionRef, SessionRequest, WorkingDirectory,
    },
    runtimes::cli::{CliBackend, CliKind, CliSettings, NATIVE_SESSION_TOKEN},
    storage::Namespace,
    workspace::{WorkspaceAccess, WorkspaceConfig, WorkspaceEntry, WorkspaceLocation},
};
use serde_json::{Value, json};

// This test-only wrapper avoids creating durable ownership files in the user's real CLI state.
// Realm fencing is exercised separately with a temporary native state directory.
type Timings = Arc<Mutex<HashMap<String, (Option<Instant>, Option<Instant>)>>>;
struct LiveBackend(CliBackend, Timings);
impl ExecutionBackend for LiveBackend {
    fn metadata(&self) -> ExecutionMetadata {
        self.0.metadata()
    }
    fn state_realm(&self) -> &str {
        self.0.state_realm()
    }
    fn independent_new_sessions(&self) -> bool {
        self.0.independent_new_sessions()
    }
    fn requires_workspace(&self) -> bool {
        self.0.requires_workspace()
    }
    fn minimum_events(&self) -> usize {
        self.0.minimum_events()
    }
    fn cleanup_budget(&self) -> Option<Duration> {
        self.0.cleanup_budget()
    }
    fn preflight<'a>(
        &'a self,
        cancellation: CancellationToken,
    ) -> Pin<Box<dyn Future<Output = Result<(), BackendError>> + Send + 'a>> {
        self.0.preflight(cancellation)
    }
    fn execute<'a>(
        &'a self,
        request: ExecutionRequest,
        events: EventSink,
        cancellation: CancellationToken,
    ) -> Pin<Box<dyn Future<Output = ExecutionResult> + Send + 'a>> {
        let timings = Arc::clone(&self.1);
        let sink: EventSink = Arc::new(move |event| {
            if let ExecutionEventKind::Progress { message } = &event.kind {
                let mut timings = timings.lock().unwrap();
                let interval = timings.entry(event.run_id.clone()).or_default();
                if message == "cli_child_started" {
                    interval.0 = Some(Instant::now());
                }
                if message == "cli_child_exited" {
                    interval.1 = Some(Instant::now());
                }
            }
            events(event)
        });
        self.0.execute(request, sink, cancellation)
    }
}

fn program(name: &str) -> PathBuf {
    std::env::split_paths(&std::env::var_os("PATH").expect("PATH"))
        .map(|directory| directory.join(name))
        .find(|path| path.is_file())
        .expect("CLI executable in PATH")
}
/// Loads the shipped reviewed profile so live runs validate the same definition operators use.
fn example_settings(kind: CliKind, executable: PathBuf, timeout_ms: u64) -> CliSettings {
    let text = match kind {
        CliKind::Codex => include_str!("../examples/cli-profiles/codex.json"),
        CliKind::Claude => include_str!("../examples/cli-profiles/claude.json"),
    };
    let mut settings: CliSettings = serde_json::from_str(text).expect("example CLI profile");
    settings.program = executable;
    settings.helper_program = PathBuf::from(env!("CARGO_BIN_EXE_agent-runner-cli-helper"));
    settings.timeout_ms = timeout_ms;
    accept_installed_version(&mut settings.version, &settings.program);
    settings
}
/// Lets an operator explicitly run the live tests against the installed CLI without editing the shipped profile.
const ACCEPT_INSTALLED_VERSION: &str = "AGENT_RUNNER_LIVE_ACCEPT_INSTALLED_VERSION";
fn accept_installed_version(
    version: &mut agent_runner_server::runtimes::cli::CliVersionProbe,
    program: &Path,
) {
    if std::env::var_os(ACCEPT_INSTALLED_VERSION).is_none() {
        return;
    }
    let output = std::process::Command::new(program)
        .args(&version.args)
        .output()
        .expect("CLI version");
    assert!(
        output.status.success(),
        "installed CLI version probe failed"
    );
    version.expected_stdout = Some(
        String::from_utf8(output.stdout)
            .expect("UTF-8 CLI version")
            .trim()
            .to_owned(),
    );
}
fn make_dir(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::create_dir(path).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
}
fn body(workspace: &str, key: &str, session: Value, prompt: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({"profile_id":"profile","workspace_id":workspace,"idempotency_key":key,"session":session,"input":{"request":prompt}})).unwrap()
}
fn kind_name(kind: CliKind) -> &'static str {
    match kind {
        CliKind::Codex => "codex",
        CliKind::Claude => "claude",
    }
}
fn live_limits() -> RunLimits {
    RunLimits {
        max_outstanding: 4,
        max_workers: 2,
        database_queue: 32,
        event_buffer: 32,
        max_events_per_run: 32,
        max_event_bytes: 4096,
        max_result_bytes: 16384,
        max_page_size: 16,
        run_timeout: Duration::from_secs(75),
        cleanup_grace: Duration::from_secs(8),
        poll_interval: Duration::from_millis(10),
    }
}
/// Opens a RunService over the shipped profile with two exclusive workspaces under `root`.
async fn live_service(
    kind: CliKind,
    root: &Path,
    limits: RunLimits,
    timings: Timings,
) -> (RunService, agent_runner_server::access::AuthenticatedClient) {
    for dir in ["owner", "a", "b"] {
        make_dir(&root.join(dir));
    }
    let backend = Arc::new(LiveBackend(
        CliBackend::new(example_settings(kind, program(kind_name(kind)), 60000))
            .expect("trusted CLI profile"),
        timings,
    ));
    let runtime = backend.metadata().runtime;
    let realm = backend.state_realm().to_owned();
    let access = Arc::new(
        ClientAccessControl::new(
            [ClientRegistration::new(
                "client",
                CredentialSecret::new([7; 32]),
                Namespace {
                    owner_id: "owner".into(),
                    client_id: "client".into(),
                },
                [
                    Scope::RunStart,
                    Scope::RunRead,
                    Scope::RunCancel,
                    Scope::SessionRead,
                ],
                ["profile".into()],
            )
            .with_generation("v1")],
            [ExecutionProfile::for_runtime(
                "profile",
                runtime,
                ["a".into(), "b".into()],
                8192,
            )],
            AccessLimits {
                max_request_bytes: 16384,
                max_skill_bytes: 1024,
                max_identifier_bytes: 128,
            },
        )
        .unwrap(),
    );
    let client = access.authenticate("client", &[7; 32]).unwrap();
    let workspaces = WorkspaceConfig {
        ownership_directory: root.join("owner"),
        entries: ["a", "b"]
            .into_iter()
            .map(|id| WorkspaceEntry {
                id: id.into(),
                revision: "v1".into(),
                access: WorkspaceAccess::Exclusive,
                location: WorkspaceLocation::Existing {
                    path: root.join(id),
                },
            })
            .collect(),
    };
    let service = RunService::open_with_workspaces(
        root.join("runs.sqlite"),
        access,
        vec![BackendProfile {
            profile_id: "profile".into(),
            revision: "v1".into(),
            state_realm: realm,
            backend,
        }],
        limits,
        None,
        None,
        Some(workspaces),
    )
    .await
    .unwrap();
    (service, client)
}
/// Waits for a terminal Run state and returns it.
async fn wait(
    service: &RunService,
    client: &agent_runner_server::access::AuthenticatedClient,
    id: &str,
) -> String {
    tokio::time::timeout(Duration::from_secs(80), async {
        loop {
            let state = service.get_run(client, id).await.unwrap().unwrap().state;
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
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("live run terminal")
}
async fn run_live(kind: CliKind) {
    let (name, token) = match kind {
        CliKind::Codex => ("codex", "ORBIT642"),
        CliKind::Claude => ("claude", "MEADOW317"),
    };
    let temp = tempfile::tempdir().unwrap();
    let timings: Timings = Arc::new(Mutex::new(HashMap::new()));
    let (service, client) =
        live_service(kind, temp.path(), live_limits(), Arc::clone(&timings)).await;
    let first = service.start(&client, &body("a", "new-a", json!({"mode":"new"}), &format!("Remember token {token}. In about 80 words explain why rain falls. Do not use any tools."))).await.unwrap();
    let second = service
        .start(
            &client,
            &body(
                "b",
                "new-b",
                json!({"mode":"new"}),
                "In about 80 words explain why clouds form. Do not use any tools.",
            ),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(65), async {
        loop {
            let a = service
                .get_run(&client, &first.run_id)
                .await
                .unwrap()
                .unwrap();
            let b = service
                .get_run(&client, &second.run_id)
                .await
                .unwrap()
                .unwrap();
            let events_a = service
                .get_events(&client, &first.run_id, None, 16)
                .await
                .unwrap();
            let events_b = service
                .get_events(&client, &second.run_id, None, 16)
                .await
                .unwrap();
            if a.state == "running"
                && b.state == "running"
                && events_a.iter().any(|event| event.kind == "run.started")
                && events_b.iter().any(|event| event.kind == "run.started")
            {
                break;
            }
            if [
                "failed",
                "succeeded",
                "cancelled",
                "timed_out",
                "interrupted",
            ]
            .contains(&a.state.as_str())
                || [
                    "failed",
                    "succeeded",
                    "cancelled",
                    "timed_out",
                    "interrupted",
                ]
                .contains(&b.state.as_str())
            {
                let error_a = service
                    .get_result(&client, &first.run_id)
                    .await
                    .unwrap()
                    .and_then(|result| serde_json::from_slice::<Value>(&result.payload).ok())
                    .and_then(|value| {
                        value
                            .pointer("/details/outcome/error")
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                    });
                let error_b = service
                    .get_result(&client, &second.run_id)
                    .await
                    .unwrap()
                    .and_then(|result| serde_json::from_slice::<Value>(&result.payload).ok())
                    .and_then(|value| {
                        value
                            .pointer("/details/outcome/error")
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                    });
                panic!(
                    "{name}: two live started runs did not overlap: {} {:?} {:?}; {} {:?} {:?}",
                    a.state,
                    events_a.iter().map(|event| &event.kind).collect::<Vec<_>>(),
                    error_a,
                    b.state,
                    events_b.iter().map(|event| &event.kind).collect::<Vec<_>>(),
                    error_b,
                );
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("two live started runs");
    assert_eq!(
        wait(&service, &client, &first.run_id).await,
        "succeeded",
        "{name} first"
    );
    assert_eq!(
        wait(&service, &client, &second.run_id).await,
        "succeeded",
        "{name} second"
    );
    {
        let observed = timings.lock().unwrap();
        let (a_start, a_end) = observed
            .get(&first.run_id)
            .copied()
            .expect("first CLI lifecycle");
        let (b_start, b_end) = observed
            .get(&second.run_id)
            .copied()
            .expect("second CLI lifecycle");
        let (a_start, a_end, b_start, b_end) = (
            a_start.expect("first start"),
            a_end.expect("first exit"),
            b_start.expect("second start"),
            b_end.expect("second exit"),
        );
        assert!(
            a_start < b_end && b_start < a_end,
            "{name}: CLI child process intervals did not overlap"
        );
    }
    let continued = service
        .start(
            &client,
            &body(
                "a",
                "resume-a",
                json!({"mode":"resume","session_id":first.session_id.unwrap()}),
                "Reply with only the token I asked you to remember earlier. Do not use tools.",
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        wait(&service, &client, &continued.run_id).await,
        "succeeded",
        "{name} resume"
    );
    let result = service
        .get_result(&client, &continued.run_id)
        .await
        .unwrap()
        .unwrap();
    let result: Value = serde_json::from_slice(&result.payload).unwrap();
    assert!(
        result["details"]["outcome"]["output"]
            .as_str()
            .is_some_and(|output| output.contains(token)),
        "{name}: continuation did not recall token"
    );
    let cancelled = service
        .start(
            &client,
            &body(
                "a",
                "cancel-live",
                json!({"mode":"new"}),
                "Explain how rain forms in about 250 words. Do not use tools.",
            ),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            {
                let observed = timings.lock().unwrap();
                if let Some((Some(_), end)) = observed.get(&cancelled.run_id) {
                    assert!(end.is_none(), "{name}: CLI exited before cancellation test");
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("live CLI start for cancellation");
    service.cancel(&client, &cancelled.run_id).await.unwrap();
    assert_eq!(
        wait(&service, &client, &cancelled.run_id).await,
        "cancelled",
        "{name} cancellation"
    );
    let connection = rusqlite::Connection::open(temp.path().join("runs.sqlite")).unwrap();
    let (held, uncertain): (bool, bool) = connection
        .query_row(
            "SELECT held,uncertain FROM run_workspaces WHERE run_id=?1",
            [&cancelled.run_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert!(
        held && uncertain,
        "{name}: cancelled workspace must remain quarantined"
    );
    drop(connection);
    println!(
        "{name}: CLI child process intervals overlapped across separate workspaces; new, explicit resume, and cancellation quarantine succeeded"
    );
    service.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "runs real CLIs against an unknown session and could invoke a provider if a CLI falls back"]
async fn live_missing_native_uuid_does_not_start_a_new_turn() {
    use agent_runner_server::process::{
        ProcessLimits, ProcessSpec, ProcessSupervisor, StopReason, StopSignal,
    };
    use std::{io::Read, os::unix::fs::PermissionsExt};
    let base = PathBuf::from(
        std::env::var_os("AGENT_RUNNER_LIVE_TEST_DIR").expect("explicit persistent test directory"),
    )
    .canonicalize()
    .expect("existing test directory");
    let allowed = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("temp")
        .canonicalize()
        .unwrap();
    assert!(
        base.starts_with(allowed),
        "test directory must be inside the project temp directory"
    );
    let root = tempfile::Builder::new()
        .prefix("missing-native-")
        .tempdir_in(base)
        .unwrap()
        .keep();
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
    println!("retained negative-test directory: {}", root.display());
    let mut random = [0_u8; 16];
    std::fs::File::open("/dev/urandom")
        .unwrap()
        .read_exact(&mut random)
        .unwrap();
    let id = format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-4{:01x}{:02x}-8{:01x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        random[0],
        random[1],
        random[2],
        random[3],
        random[4],
        random[5],
        random[6] & 15,
        random[7],
        random[8] & 15,
        random[9],
        random[10],
        random[11],
        random[12],
        random[13],
        random[14],
        random[15]
    );
    for kind in [CliKind::Codex, CliKind::Claude] {
        let name = match kind {
            CliKind::Codex => "codex",
            CliKind::Claude => "claude",
        };
        let executable = program(name).canonicalize().unwrap();
        let version = std::process::Command::new(&executable)
            .arg("--version")
            .output()
            .expect("CLI version");
        let settings = example_settings(kind, executable.clone(), 30000);
        let expected = settings
            .version
            .expected_stdout
            .as_deref()
            .expect("pinned example profile");
        assert!(
            version.status.success()
                && std::str::from_utf8(&version.stdout).ok().map(str::trim) == Some(expected),
            "{name}: unverified version"
        );
        // The adapter must reject the unknown session before launching the CLI.
        let backend = CliBackend::new(settings.clone()).expect("trusted CLI profile");
        let metadata = std::fs::metadata(&root).unwrap();
        let rejected = backend
            .execute(
                ExecutionRequest {
                    run_id: format!("missing-{name}"),
                    attempt_id: format!("missing-{name}-attempt"),
                    workspace_id: "missing".into(),
                    working_directory: Some(WorkingDirectory {
                        path: root.clone(),
                        device: std::os::unix::fs::MetadataExt::dev(&metadata),
                        inode: std::os::unix::fs::MetadataExt::ino(&metadata),
                    }),
                    session: SessionRequest::Resume(SessionRef {
                        runtime: backend.metadata().runtime,
                        state_realm: backend.state_realm().to_owned(),
                        native_id: id.clone(),
                    }),
                    prompt: "Reply ONLY with the word TEST. Do not use tools.\n".into(),
                },
                Arc::new(|_| Ok(())),
                CancellationToken::new(),
            )
            .await;
        assert!(
            matches!(&rejected.outcome, ExecutionOutcome::Failed(BackendError::ExecutionFailed { code, .. }) if code == "native_session_missing"),
            "{name}: adapter did not reject the missing session before launch: {:?}",
            rejected.outcome
        );
        assert_eq!(rejected.dispatch, DispatchStatus::ProvenUnstarted);
        // Independently, the CLI itself must fail closed if a transcript disappears after the check.
        let args: Vec<String> = settings
            .resume_args
            .iter()
            .map(|arg| {
                if arg == NATIVE_SESSION_TOKEN {
                    id.clone()
                } else {
                    arg.clone()
                }
            })
            .collect();
        let mut names: Vec<&str> = settings
            .environment
            .inherit
            .iter()
            .map(String::as_str)
            .collect();
        names.push(settings.environment.state_directory_variable.as_str());
        let env = names
            .into_iter()
            .filter_map(|name| std::env::var_os(name).map(|value| (name.into(), value)))
            .collect();
        let report = ProcessSupervisor::run(
            ProcessSpec {
                program: executable,
                args: args.into_iter().map(Into::into).collect(),
                cwd: Some(root.clone()),
                env,
                stdin: b"Reply ONLY with the word TEST. Do not use tools.\n".to_vec(),
                limits: ProcessLimits {
                    max_args: 32,
                    max_env: 32,
                    max_spec_bytes: 65536,
                    max_stdin_bytes: 4096,
                    max_stdout_bytes: 1048576,
                    max_stderr_bytes: 262144,
                    max_line_bytes: 262144,
                    timeout: Duration::from_secs(30),
                    signal_grace: Duration::from_secs(1),
                    kill_grace: Duration::from_secs(1),
                    pipe_grace: Duration::from_secs(1),
                },
                stop_signal: StopSignal::Terminate,
            },
            CancellationToken::new(),
        )
        .await
        .expect("negative CLI invocation");
        assert_eq!(
            report.reason,
            StopReason::Exited,
            "{name}: missing-session invocation did not exit normally"
        );
        assert!(
            report
                .exit
                .is_some_and(|exit| exit.code.is_some_and(|code| code != 0)),
            "{name}: missing session was not rejected"
        );
        let diagnostic = format!(
            "{} {}",
            String::from_utf8_lossy(&report.stdout.bytes),
            String::from_utf8_lossy(&report.stderr.bytes)
        )
        .to_ascii_lowercase();
        let identifies_session = ["session", "conversation", "thread", "rollout"]
            .iter()
            .any(|term| diagnostic.contains(term));
        let identifies_missing = [
            "not found",
            "no conversation",
            "no session",
            "no rollout",
            "could not find",
            "does not exist",
            "unknown session",
        ]
        .iter()
        .any(|term| diagnostic.contains(term));
        assert!(
            identifies_session && identifies_missing,
            "{name}: error did not identify a missing session"
        );
        if kind == CliKind::Codex {
            let started = report
                .stdout
                .bytes
                .split(|byte| *byte == b'\n')
                .filter_map(|line| serde_json::from_slice::<Value>(line).ok())
                .any(|value| value["type"] == "thread.started" || value["type"] == "turn.started");
            assert!(!started, "codex: a new thread or turn was observed");
        } else if !report.stdout.bytes.is_empty() {
            let value: Value =
                serde_json::from_slice(&report.stdout.bytes).expect("Claude JSON error output");
            assert!(
                value["is_error"] == true || value["subtype"] != "success",
                "claude: missing session returned success"
            );
        }
        println!("{name}: unknown native UUID exited nonzero without an observed successful turn");
    }
}

#[tokio::test]
#[ignore = "performs approved real provider requests and creates native CLI sessions"]
async fn live_codex_workspace_parallel_and_resume() {
    run_live(CliKind::Codex).await;
}

#[tokio::test]
#[ignore = "performs approved real provider requests and creates native CLI sessions"]
async fn live_claude_workspace_parallel_and_resume() {
    run_live(CliKind::Claude).await;
}

/// Reads the error code recorded in a terminal Run result, if any.
async fn result_error(
    service: &RunService,
    client: &agent_runner_server::access::AuthenticatedClient,
    id: &str,
) -> Option<String> {
    service
        .get_result(client, id)
        .await
        .unwrap()
        .and_then(|result| serde_json::from_slice::<Value>(&result.payload).ok())
        .and_then(|value| {
            ["/details/error", "/details/outcome/error"]
                .iter()
                .find_map(|pointer| value.pointer(pointer).and_then(Value::as_str))
                .map(str::to_owned)
        })
}

/// Covers approval-required writes, result limits and run timeouts against a real CLI.
async fn run_live_boundaries(kind: CliKind) {
    let name = kind_name(kind);
    let temp = tempfile::tempdir().unwrap();
    let timings: Timings = Arc::new(Mutex::new(HashMap::new()));
    let limits = RunLimits {
        max_result_bytes: 2048,
        ..live_limits()
    };
    let (service, client) = live_service(kind, temp.path(), limits, Arc::clone(&timings)).await;

    // The read-only profile must not wait for a human or write into the workspace.
    let probe = temp.path().join("a").join("approval-probe.txt");
    let started = Instant::now();
    let approval = service
        .start(
            &client,
            &body(
                "a",
                "approval",
                json!({"mode":"new"}),
                "Create a file named approval-probe.txt in the current directory containing the word OK. If you cannot create it, reply with one short sentence explaining why.",
            ),
        )
        .await
        .unwrap();
    let approval_state = wait(&service, &client, &approval.run_id).await;
    assert!(
        !probe.exists(),
        "{name}: read-only profile created a workspace file"
    );
    println!(
        "{name}: approval-required write ended as {approval_state} in {:?} without creating the file (error: {:?})",
        started.elapsed(),
        result_error(&service, &client, &approval.run_id).await
    );

    // Output above the configured result limit must fail instead of being stored or truncated.
    let large = service
        .start(
            &client,
            &body(
                "b",
                "large-output",
                json!({"mode":"new"}),
                "Print the integers from 1 to 600, one per line, with no other text. Do not use any tools.",
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        wait(&service, &client, &large.run_id).await,
        "failed",
        "{name}: oversized output"
    );
    assert_eq!(
        result_error(&service, &client, &large.run_id)
            .await
            .as_deref(),
        Some("result_limit_exceeded"),
        "{name}: oversized output error"
    );
    println!("{name}: output above the result limit failed with result_limit_exceeded");
    service.shutdown().await.unwrap();

    // A run deadline that expires after launch must stop the CLI and keep the workspace isolated.
    let temp = tempfile::tempdir().unwrap();
    let limits = RunLimits {
        run_timeout: Duration::from_secs(6),
        ..live_limits()
    };
    let (service, client) = live_service(kind, temp.path(), limits, Arc::clone(&timings)).await;
    let slow = service
        .start(
            &client,
            &body(
                "a",
                "timeout",
                json!({"mode":"new"}),
                "Write a detailed essay of about 1500 words on the history of weather forecasting. Do not use any tools.",
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        wait(&service, &client, &slow.run_id).await,
        "timed_out",
        "{name}: run deadline"
    );
    let interval = timings.lock().unwrap().get(&slow.run_id).copied();
    assert!(
        interval.is_some_and(|(start, _)| start.is_some()),
        "{name}: the deadline expired before the CLI started"
    );
    let connection = rusqlite::Connection::open(temp.path().join("runs.sqlite")).unwrap();
    let (held, uncertain): (bool, bool) = connection
        .query_row(
            "SELECT held,uncertain FROM run_workspaces WHERE run_id=?1",
            [&slow.run_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert!(
        held && uncertain,
        "{name}: timed out workspace must remain quarantined"
    );
    drop(connection);
    println!(
        "{name}: run deadline after launch ended as timed_out and kept the workspace quarantined"
    );
    service.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "performs approved real provider requests and creates native CLI sessions"]
async fn live_codex_approval_result_limit_and_timeout() {
    run_live_boundaries(CliKind::Codex).await;
}

#[tokio::test]
#[ignore = "performs approved real provider requests and creates native CLI sessions"]
async fn live_claude_approval_result_limit_and_timeout() {
    run_live_boundaries(CliKind::Claude).await;
}
