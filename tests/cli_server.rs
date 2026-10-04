#![cfg(target_os = "linux")]

use serde_json::{Value, json};
use std::{
    os::unix::{
        ffi::OsStringExt,
        fs::{MetadataExt, PermissionsExt},
    },
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};
use tempfile::tempdir;

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}
fn account_home() -> PathBuf {
    let mut entry = std::mem::MaybeUninit::<libc::passwd>::zeroed();
    let mut buffer = vec![0_u8; 16 * 1024];
    let mut result = std::ptr::null_mut();
    // SAFETY: All pointers refer to live storage during getpwuid_r.
    let status = unsafe {
        libc::getpwuid_r(
            libc::geteuid(),
            entry.as_mut_ptr(),
            buffer.as_mut_ptr().cast(),
            buffer.len(),
            &mut result,
        )
    };
    assert_eq!(status, 0);
    assert!(!result.is_null());
    // SAFETY: A successful lookup returns a valid passwd record.
    let directory = unsafe { (*result).pw_dir };
    assert!(!directory.is_null());
    // SAFETY: pw_dir is NUL-terminated and backed by the live buffer.
    PathBuf::from(std::ffi::OsString::from_vec(
        unsafe { std::ffi::CStr::from_ptr(directory) }
            .to_bytes()
            .to_vec(),
    ))
    .canonicalize()
    .unwrap()
}

fn native_owner_record(state: &Path, owner_root: &Path) -> std::io::Result<PathBuf> {
    let metadata = std::fs::metadata(state)?;
    Ok(owner_root
        .join(".agent-runner-server-realms")
        .join(format!("i{:x}-{:x}", metadata.dev(), metadata.ino()))
        .join("owner.json"))
}

#[test]
fn native_owner_preflight_recognizes_existing_records_without_modifying_them() {
    let temp = tempdir().unwrap();
    private_dir(&temp.path().join("state"));
    let owner = native_owner_record(&temp.path().join("state"), temp.path()).unwrap();
    assert!(!owner.exists());
    std::fs::create_dir_all(owner.parent().unwrap()).unwrap();
    std::fs::write(&owner, b"existing owner").unwrap();
    assert!(
        native_owner_record(&temp.path().join("state"), temp.path())
            .unwrap()
            .exists()
    );
    assert_eq!(std::fs::read(&owner).unwrap(), b"existing owner");
}

struct RealmCleanup {
    base: PathBuf,
    base_created: bool,
    directories: Vec<(PathBuf, u64, u64)>,
    database: PathBuf,
}
impl RealmCleanup {
    fn new(root: &Path) -> Self {
        let base = account_home().join(".agent-runner-server-realms");
        let base_created = match std::fs::create_dir(&base) {
            Ok(()) => {
                std::fs::set_permissions(&base, std::fs::Permissions::from_mode(0o700)).unwrap();
                true
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => false,
            Err(error) => panic!("test realm root: {error}"),
        };
        let mut cleanup = Self {
            base,
            base_created,
            directories: Vec::new(),
            database: root.canonicalize().unwrap().join("runs.sqlite"),
        };
        for name in [".codex", ".claude"] {
            let state = std::fs::metadata(root.join("home").join(name)).unwrap();
            let directory = cleanup
                .base
                .join(format!("i{:x}-{:x}", state.dev(), state.ino()));
            std::fs::create_dir(&directory).expect("test realm anchor must not preexist");
            std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
            cleanup
                .directories
                .push((directory, state.dev(), state.ino()));
        }
        cleanup
    }
}
impl Drop for RealmCleanup {
    fn drop(&mut self) {
        for (directory, device, inode) in &self.directories {
            let owner = directory.join("owner.json");
            if !owner.exists() {
                let _ = std::fs::remove_dir(directory);
                continue;
            }
            let valid = std::fs::read(&owner)
                .ok()
                .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                .is_some_and(|value| {
                    value["state_device"].as_u64() == Some(*device)
                        && value["state_inode"].as_u64() == Some(*inode)
                        && value["database"]["path"]
                            .as_str()
                            .is_some_and(|path| Path::new(path) == self.database)
                });
            if !valid {
                continue;
            }
            let Ok(entries) = std::fs::read_dir(directory) else {
                continue;
            };
            let names: Vec<_> = entries
                .filter_map(Result::ok)
                .map(|entry| entry.file_name())
                .collect();
            let allowed = [
                "owner.json",
                "journal.jsonl",
                "journal.seq",
                "journal.epoch",
            ];
            if names
                .iter()
                .any(|name| !allowed.iter().any(|allowed| name == allowed))
            {
                continue;
            }
            for name in names {
                let _ = std::fs::remove_file(directory.join(name));
            }
            let _ = std::fs::remove_dir(directory);
        }
        if self.base_created {
            let _ = std::fs::remove_dir(&self.base);
        }
    }
}

/// Loads the shipped reviewed profile so server tests exercise the operator-facing definition.
fn example_profile(kind: &str) -> serde_json::Value {
    let text = match kind {
        "codex" => include_str!("../examples/cli-profiles/codex.json"),
        "claude" => include_str!("../examples/cli-profiles/claude.json"),
        _ => panic!("unknown example profile"),
    };
    serde_json::from_str(text).expect("example CLI profile")
}
fn private_dir(path: &Path) {
    std::fs::create_dir(path).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
}
/// Writes a fake CLI that records its native transcript at `transcript`, a shell path expression.
/// `${sid}` in `transcript` and `output` is the ID passed with `--session-id` or `--resume`, otherwise `default_id`.
fn cli_fixture(path: &Path, version: &str, default_id: &str, transcript: &str, output: &str) {
    let script = format!(
        "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then printf '%s\\n' '{version}'; exit 0; fi\nprintf x >> \"$0.calls\"\nsid={default_id}; prev=; for arg in \"$@\"; do if [ \"$prev\" = --session-id ] || [ \"$prev\" = --resume ]; then sid=\"$arg\"; fi; prev=\"$arg\"; done\ncat >/dev/null\nmkdir -p \"$(dirname \"{transcript}\")\" && : >> \"{transcript}\"\ncat <<RESULT\n{output}\nRESULT\n"
    );
    std::fs::write(path, script).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
}

#[tokio::test]
async fn configured_cli_profiles_use_real_server_and_owned_state_without_live_providers() {
    const ID: &str = "12345678-1234-4234-8234-123456789abc";
    let temp = tempdir().unwrap();
    for name in ["home", "owner", "a", "b"] {
        private_dir(&temp.path().join(name));
    }
    for name in [".codex", ".claude"] {
        private_dir(&temp.path().join("home").join(name));
    }
    let realm_cleanup = RealmCleanup::new(temp.path());
    let created_anchors: Vec<_> = realm_cleanup
        .directories
        .iter()
        .map(|(path, _, _)| path.clone())
        .collect();
    cli_fixture(
        &temp.path().join("fake-codex"),
        example_profile("codex")["version"]["expected_stdout"]
            .as_str()
            .unwrap(),
        ID,
        &format!("$CODEX_HOME/sessions/2026/11/07/rollout-fixture-{ID}.jsonl"),
        &format!(
            "{{\"type\":\"thread.started\",\"thread_id\":\"{ID}\"}}\n{{\"type\":\"turn.started\"}}\n{{\"type\":\"item.completed\",\"item\":{{\"type\":\"agent_message\",\"text\":\"codex fixture\"}}}}\n{{\"type\":\"turn.completed\"}}"
        ),
    );
    cli_fixture(
        &temp.path().join("fake-claude"),
        example_profile("claude")["version"]["expected_stdout"]
            .as_str()
            .unwrap(),
        ID,
        "$CLAUDE_CONFIG_DIR/projects/fixture/${sid}.jsonl",
        "{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"session_id\":\"${sid}\",\"result\":\"claude fixture\",\"permission_denials\":[]}",
    );
    let secret = temp.path().join("secret");
    std::fs::write(&secret, "07".repeat(32)).unwrap();
    std::fs::set_permissions(&secret, std::fs::Permissions::from_mode(0o600)).unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let cli = |kind: &str, program: &str| {
        let mut value = example_profile(kind);
        for (key, limit) in [
            ("program", json!(program)),
            (
                "helper_program",
                json!(env!("CARGO_BIN_EXE_agent-runner-cli-helper")),
            ),
            ("max_stdin_bytes", json!(8192)),
            ("max_stdout_bytes", json!(8192)),
            ("max_stderr_bytes", json!(8192)),
            ("max_line_bytes", json!(4096)),
            ("timeout_ms", json!(10000)),
            ("signal_grace_ms", json!(200)),
            ("kill_grace_ms", json!(200)),
            ("pipe_grace_ms", json!(200)),
        ] {
            value[key] = limit;
        }
        value
    };
    let value = json!({
        "schema_version":1,"listen":format!("127.0.0.1:{port}"),"database":"runs.sqlite",
        "access":{"max_request_bytes":8192,"max_skill_bytes":1024,"max_identifier_bytes":128},
        "run":{"max_outstanding":8,"max_workers":2,"database_queue":16,"event_buffer":16,"max_events_per_run":32,"max_event_bytes":2048,"max_result_bytes":4096,"max_page_size":16,"run_timeout_ms":10000,"cleanup_grace_ms":3000,"poll_interval_ms":5},
        "transport":{"max_request_bytes":16384,"max_response_bytes":1048576,"max_header_bytes":8192,"max_inflight":16,"max_connections":16,"connection_idle_timeout_ms":4000,"connection_max_lifetime_ms":30000,"requests_per_second":1000,"burst":1000,"request_timeout_ms":2000,"shutdown_grace_ms":4000,"max_mcp_sessions":4,"mcp_session_idle_ms":60000},
        "clients":[{"credential_id":"full","generation":"v1","secret_file":"secret","owner_id":"owner","client_id":"client","scopes":["run_start","run_read","run_cancel","session_read"],"profiles":["codex","claude"]}],
        "workspaces":{"ownership_directory":"owner","entries":[
            {"id":"a","revision":"v1","access":"exclusive","location":{"kind":"existing","path":"a"}},
            {"id":"b","revision":"v1","access":"exclusive","location":{"kind":"existing","path":"b"}}
        ]},
        "profiles":[
            {"profile_id":"codex","revision":"v1","workspaces":["a"],"max_input_bytes":4096,"cli":cli("codex","fake-codex")},
            {"profile_id":"claude","revision":"v1","workspaces":["b"],"max_input_bytes":4096,"cli":cli("claude","fake-claude")}
        ]
    });
    let config = temp.path().join("config.json");
    std::fs::write(&config, serde_json::to_vec(&value).unwrap()).unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_agent-runner-server"))
        .arg("--config")
        .arg(&config)
        .env_clear()
        .env("HOME", temp.path().join("home"))
        .env("PATH", "/usr/bin:/bin")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut child = ChildGuard(child);
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_millis(500))
        .build()
        .unwrap();
    let url = format!("http://127.0.0.1:{port}");
    let bearer = format!("full.{}", "07".repeat(32));
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if client
                .get(format!("{url}/v1/agents"))
                .bearer_auth(&bearer)
                .send()
                .await
                .is_ok_and(|response| response.status() == 200)
            {
                break;
            }
            if let Some(status) = child.0.try_wait().unwrap() {
                panic!("fixture server exited: {status}");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("fixture server readiness");
    for (profile, workspace) in [("codex", "a"), ("claude", "b")] {
        let mut session = json!({"mode":"new"});
        for turn in ["new", "resume"] {
            let request = json!({
                "profile_id":profile,"workspace_id":workspace,"session":session,"input":{"request":"fixture prompt"},"idempotency_key":format!("{profile}-{turn}")
            });
            let response = client
                .post(format!("{url}/v1/runs"))
                .bearer_auth(&bearer)
                .json(&request)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 202);
            let receipt: Value = response.json().await.unwrap();
            let run = receipt["run_id"].as_str().unwrap();
            let rpc = json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{
                "name":"agents_start","arguments":request,
                "_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}
            }});
            let response = client
                .post(format!("{url}/mcp"))
                .bearer_auth(&bearer)
                .header("Accept", "application/json, text/event-stream")
                .header("MCP-Protocol-Version", "2026-07-28")
                .header("Mcp-Method", "tools/call")
                .header("Mcp-Name", "agents_start")
                .json(&rpc)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 200);
            let text = response.text().await.unwrap();
            let payload = if text.starts_with('{') {
                text.as_str()
            } else {
                text.lines()
                    .filter_map(|line| line.strip_prefix("data: "))
                    .find(|line| line.starts_with('{'))
                    .expect("MCP JSON payload")
            };
            let replay: Value = serde_json::from_str(payload).unwrap();
            assert_eq!(replay["result"]["structuredContent"]["run_id"], run);
            assert_eq!(replay["result"]["structuredContent"]["replay"], true);
            let state = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let response: Value = client
                        .get(format!("{url}/v1/runs/{run}"))
                        .bearer_auth(&bearer)
                        .send()
                        .await
                        .unwrap()
                        .json()
                        .await
                        .unwrap();
                    let state = response["state"].as_str().unwrap();
                    if [
                        "succeeded",
                        "failed",
                        "cancelled",
                        "timed_out",
                        "interrupted",
                    ]
                    .contains(&state)
                    {
                        break state.to_owned();
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            assert_eq!(state, "succeeded", "{profile} {turn}");
            session = json!({"mode":"resume","session_id":receipt["session_id"]});
        }
        // A registered session whose native transcript vanished is rejected before the CLI starts,
        // and the session stays usable once the transcript is back.
        let (store, program) = match profile {
            "codex" => (temp.path().join("home/.codex/sessions"), "fake-codex"),
            _ => (temp.path().join("home/.claude/projects"), "fake-claude"),
        };
        let parked = temp.path().join(format!("parked-{profile}"));
        std::fs::rename(&store, &parked).unwrap();
        let calls = temp.path().join(format!("{program}.calls"));
        let before = std::fs::read(&calls).unwrap().len();
        for (turn, expected) in [("vanished", "failed"), ("restored", "succeeded")] {
            if turn == "restored" {
                std::fs::rename(&parked, &store).unwrap();
            }
            let request = json!({
                "profile_id":profile,"workspace_id":workspace,"session":session,"input":{"request":"fixture prompt"},"idempotency_key":format!("{profile}-{turn}")
            });
            let receipt: Value = client
                .post(format!("{url}/v1/runs"))
                .bearer_auth(&bearer)
                .json(&request)
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            let run = receipt["run_id"].as_str().unwrap().to_owned();
            let state = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let response: Value = client
                        .get(format!("{url}/v1/runs/{run}"))
                        .bearer_auth(&bearer)
                        .send()
                        .await
                        .unwrap()
                        .json()
                        .await
                        .unwrap();
                    let state = response["state"].as_str().unwrap().to_owned();
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
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            assert_eq!(state, expected, "{profile} {turn}: {receipt}");
            let calls_now = std::fs::read(&calls).unwrap().len();
            if turn == "vanished" {
                assert_eq!(
                    calls_now, before,
                    "{profile}: CLI started for a missing session"
                );
                let result = client
                    .get(format!("{url}/v1/runs/{run}/result"))
                    .bearer_auth(&bearer)
                    .send()
                    .await
                    .unwrap()
                    .text()
                    .await
                    .unwrap();
                assert!(
                    result.contains("native_session_missing"),
                    "{profile}: {result}"
                );
            } else {
                assert_eq!(
                    calls_now,
                    before + 1,
                    "{profile}: restored resume did not run once"
                );
            }
        }
    }
    // SAFETY: The PID belongs to the child spawned by this test and SIGTERM requests its graceful shutdown.
    assert_eq!(unsafe { libc::kill(child.0.id() as i32, libc::SIGTERM) }, 0);
    let status = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("fixture server shutdown");
    assert!(status.success());
    drop(child);
    drop(realm_cleanup);
    assert!(created_anchors.iter().all(|path| !path.exists()));
}

/// Calls one MCP tool and returns its structured content after rejecting transport and tool errors.
async fn mcp(client: &reqwest::Client, url: &str, bearer: &str, name: &str, args: Value) -> Value {
    let body = json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{
        "name":name,"arguments":args,
        "_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}
    }});
    let response = client
        .post(format!("{url}/mcp"))
        .bearer_auth(bearer)
        .header("Accept", "application/json, text/event-stream")
        .header("MCP-Protocol-Version", "2026-07-28")
        .header("Mcp-Method", "tools/call")
        .header("Mcp-Name", name)
        .json(&body)
        .send()
        .await
        .expect("MCP request");
    assert_eq!(response.status(), 200, "MCP transport");
    let text = response.text().await.unwrap();
    let payload = if text.starts_with('{') {
        text.as_str()
    } else {
        text.lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .find(|line| line.starts_with('{'))
            .expect("MCP JSON payload")
    };
    let value: Value = serde_json::from_str(payload).unwrap();
    assert_ne!(value["result"]["isError"], true, "MCP tool failure");
    value["result"]["structuredContent"].clone()
}

/// Admits one run through REST and checks the expected admission status.
async fn rest_start(
    client: &reqwest::Client,
    url: &str,
    bearer: &str,
    body: &Value,
    status: u16,
) -> Value {
    let response = client
        .post(format!("{url}/v1/runs"))
        .bearer_auth(bearer)
        .json(body)
        .send()
        .await
        .expect("REST admission");
    assert_eq!(response.status().as_u16(), status, "REST admission status");
    response.json().await.unwrap()
}

/// Waits for a live run to succeed and fails fast on any other terminal state.
async fn wait_success(client: &reqwest::Client, url: &str, bearer: &str, run: &str, label: &str) {
    tokio::time::timeout(Duration::from_secs(110), async {
        loop {
            let response = client
                .get(format!("{url}/v1/runs/{run}"))
                .bearer_auth(bearer)
                .send()
                .await
                .expect("run status");
            assert_eq!(response.status(), 200);
            let value: Value = response.json().await.unwrap();
            let state = value["state"].as_str().expect("run state");
            if state == "succeeded" {
                break;
            }
            assert!(
                !["failed", "cancelled", "timed_out", "interrupted"].contains(&state),
                "{label}: {state}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("live run timed out");
}

/// Builds the shipped profile for the executable found in PATH after checking its exact version.
fn live_cli_profile(kind: &str) -> Value {
    let executable = std::env::split_paths(&std::env::var_os("PATH").expect("PATH"))
        .map(|directory| directory.join(kind))
        .find(|path| path.is_file())
        .expect("CLI executable in PATH");
    let mut value = example_profile(kind);
    let version = Command::new(&executable)
        .arg("--version")
        .output()
        .expect("CLI version");
    // An operator may explicitly accept the installed CLI version for a candidate run; the shipped profile is unchanged.
    if std::env::var_os("AGENT_RUNNER_LIVE_ACCEPT_INSTALLED_VERSION").is_some() {
        value["version"]["expected_stdout"] = json!(
            std::str::from_utf8(&version.stdout)
                .expect("UTF-8 CLI version")
                .trim()
        );
    }
    let expected = value["version"]["expected_stdout"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(
        version.status.success()
            && std::str::from_utf8(&version.stdout).ok().map(str::trim) == Some(expected.as_str()),
        "{kind}: unverified version"
    );
    value["program"] = json!(executable);
    value["helper_program"] = json!(env!("CARGO_BIN_EXE_agent-runner-cli-helper"));
    value
}

/// Starts the daemon from `config` and waits until the authenticated agent list responds.
async fn start_live_daemon(
    config: &Path,
    log: std::fs::File,
    port: u16,
    secret: &str,
) -> (ChildGuard, reqwest::Client, String, String) {
    let child = Command::new(env!("CARGO_BIN_EXE_agent-runner-server"))
        .arg("--config")
        .arg(config)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::from(log))
        .spawn()
        .unwrap();
    let mut child = ChildGuard(child);
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let url = format!("http://127.0.0.1:{port}");
    let bearer = format!("live.{secret}");
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if client
                .get(format!("{url}/v1/agents"))
                .bearer_auth(&bearer)
                .send()
                .await
                .is_ok_and(|response| response.status() == 200)
            {
                break;
            }
            if let Some(status) = child.0.try_wait().unwrap() {
                panic!("daemon exited before readiness: {status}");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("daemon readiness");
    (child, client, url, bearer)
}

/// Stops the daemon with SIGTERM and requires a clean exit.
async fn stop_live_daemon(mut child: ChildGuard) {
    // SAFETY: The PID identifies the daemon spawned by this test.
    assert_eq!(unsafe { libc::kill(child.0.id() as i32, libc::SIGTERM) }, 0);
    let status = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("daemon shutdown");
    assert!(status.success());
    drop(child);
}

/// Starts the daemon from `config`, runs the approved REST and MCP new, replay and resume flow, and stops it cleanly.
/// `suffix` keeps idempotency keys unique when the database already holds earlier live runs.
async fn live_rest_mcp_flow(
    config: &Path,
    log: std::fs::File,
    port: u16,
    secret: &str,
    suffix: &str,
) {
    let (child, client, url, bearer) = start_live_daemon(config, log, port, secret).await;

    let new = |profile: &str, workspace: &str, key: &str, token: &str| {
        json!({
            "profile_id":profile,"workspace_id":workspace,"session":{"mode":"new"},
            "idempotency_key":key,"input":{"request":format!("Remember token {token}. Reply OK only. Do not use tools.")}
        })
    };
    let codex = new("codex", "a", &format!("codex-new{suffix}"), "ORBIT642");
    let claude = new("claude", "b", &format!("claude-new{suffix}"), "MEADOW317");
    let a = rest_start(&client, &url, &bearer, &codex, 202).await;
    let b = mcp(&client, &url, &bearer, "agents_start", claude.clone()).await;
    let a_replay = mcp(&client, &url, &bearer, "agents_start", codex).await;
    let b_replay = rest_start(&client, &url, &bearer, &claude, 200).await;
    assert_eq!(a_replay["run_id"], a["run_id"]);
    assert_eq!(b_replay["run_id"], b["run_id"]);
    assert_eq!(a_replay["replay"], true);
    assert_eq!(b_replay["replay"], true);
    let a_id = a["run_id"].as_str().unwrap();
    let b_id = b["run_id"].as_str().unwrap();
    wait_success(&client, &url, &bearer, a_id, "codex new").await;
    wait_success(&client, &url, &bearer, b_id, "claude new").await;
    println!("both new requests completed through the live daemon");
    let resume = |profile: &str, workspace: &str, key: &str, session: &Value| {
        json!({
            "profile_id":profile,"workspace_id":workspace,"session":{"mode":"resume","session_id":session},
            "idempotency_key":key,"input":{"request":"Reply with only the token I asked you to remember. Do not use tools."}
        })
    };
    let a_next = resume(
        "codex",
        "a",
        &format!("codex-resume{suffix}"),
        &a["session_id"],
    );
    let b_next = resume(
        "claude",
        "b",
        &format!("claude-resume{suffix}"),
        &b["session_id"],
    );
    let a_next = mcp(&client, &url, &bearer, "agents_start", a_next).await;
    let b_next = rest_start(&client, &url, &bearer, &b_next, 202).await;
    for (result, token, label) in [
        (&a_next, "ORBIT642", "codex resume"),
        (&b_next, "MEADOW317", "claude resume"),
    ] {
        let run = result["run_id"].as_str().unwrap();
        wait_success(&client, &url, &bearer, run, label).await;
        let view = mcp(&client, &url, &bearer, "runs_result", json!({"run_id":run})).await;
        assert_eq!(view["availability"], "available");
        let data: Value =
            serde_json::from_str(view["data"].as_str().expect("UTF-8 result")).unwrap();
        assert!(
            data["details"]["outcome"]["output"]
                .as_str()
                .is_some_and(|output| output.contains(token)),
            "{label}: context recall failed"
        );
    }
    println!("both explicit continuations recalled their tokens");
    stop_live_daemon(child).await;
}

/// Holds a temporary configuration copy and log for one daemon run over a retained database.
struct ExistingDaemon {
    config: tempfile::NamedTempFile,
    log: std::fs::File,
    port: u16,
    secret: String,
    suffix: String,
}

/// Copies the retained configuration with a fresh port and live CLI profiles, then applies `adjust`.
/// The original configuration and secret stay unchanged, and `suffix` keeps new identifiers unique.
fn prepare_existing_daemon(root: &Path, adjust: impl FnOnce(&mut Value)) -> ExistingDaemon {
    use std::{
        io::{Read, Write},
        os::unix::fs::OpenOptionsExt,
    };
    let mut config: Value =
        serde_json::from_slice(&std::fs::read(root.join("config.json")).unwrap()).unwrap();
    let secret = std::fs::read_to_string(root.join("secret")).unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    config["listen"] = json!(format!("127.0.0.1:{port}"));
    for profile in config["profiles"].as_array_mut().unwrap() {
        let Some(kind) = profile
            .get("cli")
            .and_then(|cli| cli["kind"].as_str())
            .map(str::to_owned)
        else {
            continue;
        };
        profile["cli"] = live_cli_profile(&kind);
    }
    adjust(&mut config);
    let mut bytes = [0_u8; 8];
    std::fs::File::open("/dev/urandom")
        .unwrap()
        .read_exact(&mut bytes)
        .unwrap();
    let suffix: String = std::iter::once("-".to_owned())
        .chain(bytes.iter().map(|byte| format!("{byte:02x}")))
        .collect();
    let mut copy = tempfile::NamedTempFile::new_in(root).unwrap();
    copy.write_all(&serde_json::to_vec(&config).unwrap())
        .unwrap();
    copy.flush().unwrap();
    let log = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(root.join(format!("server{suffix}.log")))
        .unwrap();
    ExistingDaemon {
        config: copy,
        log,
        port,
        secret,
        suffix,
    }
}

/// Counts runs and sessions with a recorded native ID in a retained database.
fn owned_counts(root: &Path) -> (i64, i64) {
    let db = rusqlite::Connection::open_with_flags(
        root.join("runs.sqlite"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let runs: i64 = db
        .query_row("SELECT COUNT(*) FROM runs", [], |row| row.get(0))
        .unwrap();
    let sessions: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM sessions WHERE native_id IS NOT NULL",
            [],
            |row| row.get(0),
        )
        .unwrap();
    (runs, sessions)
}

/// Returns the explicitly selected retained live directory after checking its location and private permissions.
fn existing_live_root() -> PathBuf {
    let root = PathBuf::from(
        std::env::var_os("AGENT_RUNNER_LIVE_EXISTING_DIR")
            .expect("explicit existing test directory"),
    )
    .canonicalize()
    .expect("existing test directory");
    let allowed = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("temp")
        .canonicalize()
        .unwrap();
    assert!(
        root.starts_with(allowed),
        "test directory must be inside the project temp directory"
    );
    assert_eq!(
        std::fs::metadata(&root).unwrap().permissions().mode() & 0o077,
        0
    );
    assert_eq!(
        std::fs::metadata(root.join("runs.sqlite"))
            .unwrap()
            .permissions()
            .mode()
            & 0o077,
        0
    );
    assert_eq!(
        std::fs::metadata(root.join("secret"))
            .unwrap()
            .permissions()
            .mode()
            & 0o077,
        0
    );
    root
}

#[tokio::test]
#[ignore = "uses real CLI authentication, sends provider requests, and retains the database and owner records"]
async fn approved_real_cli_daemon_rest_mcp_and_owner_integration() {
    use std::{io::Read, os::unix::fs::OpenOptionsExt};

    let base = PathBuf::from(
        std::env::var_os("AGENT_RUNNER_LIVE_TEST_DIR").expect("explicit persistent test directory"),
    );
    let base = base.canonicalize().expect("existing test directory");
    let allowed = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("temp")
        .canonicalize()
        .unwrap();
    assert!(
        base.starts_with(&allowed),
        "test directory must be inside the project temp directory"
    );
    let home = PathBuf::from(std::env::var_os("HOME").expect("HOME"));
    for (name, variable, default) in [
        ("codex", "CODEX_HOME", ".codex"),
        ("claude", "CLAUDE_CONFIG_DIR", ".claude"),
    ] {
        let state = std::env::var_os(variable)
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(default))
            .canonicalize()
            .expect("CLI state directory");
        let owner = native_owner_record(&state, &account_home()).expect("native owner path");
        assert!(
            !owner.exists(),
            "{name}: native state already has an owner; use its existing database instead"
        );
    }
    let root = tempfile::Builder::new()
        .prefix("real-daemon-")
        .tempdir_in(&base)
        .unwrap()
        .keep();
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
    println!("retained test directory: {}", root.display());
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(root.join("runs.sqlite"))
        .unwrap();
    for name in ["owner", "a", "b"] {
        private_dir(&root.join(name));
    }
    let mut bytes = [0_u8; 32];
    std::fs::File::open("/dev/urandom")
        .unwrap()
        .read_exact(&mut bytes)
        .unwrap();
    let secret: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    let secret_path = root.join("secret");
    use std::io::Write;
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&secret_path)
        .unwrap()
        .write_all(secret.as_bytes())
        .unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let value = json!({
        "schema_version":1,"listen":format!("127.0.0.1:{port}"),"database":"runs.sqlite",
        "access":{"max_request_bytes":16384,"max_skill_bytes":1024,"max_identifier_bytes":128},
        "run":{"max_outstanding":8,"max_workers":2,"database_queue":32,"event_buffer":32,"max_events_per_run":64,"max_event_bytes":4096,"max_result_bytes":16384,"max_page_size":16,"run_timeout_ms":100000,"cleanup_grace_ms":8000,"poll_interval_ms":10},
        "transport":{"max_request_bytes":32768,"max_response_bytes":2097152,"max_header_bytes":8192,"max_inflight":16,"max_connections":16,"connection_idle_timeout_ms":4000,"connection_max_lifetime_ms":30000,"requests_per_second":1000,"burst":1000,"request_timeout_ms":2000,"shutdown_grace_ms":10000,"max_mcp_sessions":4,"mcp_session_idle_ms":60000},
        "clients":[{"credential_id":"live","generation":"v1","secret_file":"secret","owner_id":"fixture","client_id":"fixture","scopes":["run_start","run_read","run_cancel","session_read"],"profiles":["codex","claude"]}],
        "workspaces":{"ownership_directory":"owner","entries":[
            {"id":"a","revision":"v1","access":"exclusive","location":{"kind":"existing","path":"a"}},
            {"id":"b","revision":"v1","access":"exclusive","location":{"kind":"existing","path":"b"}}
        ]},
        "profiles":[
            {"profile_id":"codex","revision":"v1","workspaces":["a"],"max_input_bytes":4096,"cli":live_cli_profile("codex")},
            {"profile_id":"claude","revision":"v1","workspaces":["b"],"max_input_bytes":4096,"cli":live_cli_profile("claude")}
        ]
    });
    let config = root.join("config.json");
    std::fs::write(&config, serde_json::to_vec(&value).unwrap()).unwrap();
    let log = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(root.join("server.log"))
        .unwrap();
    live_rest_mcp_flow(&config, log, port, &secret, "").await;
    let db = rusqlite::Connection::open_with_flags(
        root.join("runs.sqlite"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let runs: i64 = db
        .query_row("SELECT COUNT(*) FROM runs", [], |row| row.get(0))
        .unwrap();
    assert_eq!(runs, 4, "cross-transport replay must not create extra runs");
    let sessions: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM sessions WHERE native_id IS NOT NULL",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(sessions, 2);
    println!("live daemon, cross-transport replay, owned state and durable sessions passed");
}

#[tokio::test]
#[ignore = "reopens an explicitly selected live test database and real CLI owner records without provider requests"]
async fn real_owned_state_reopens_without_provider_requests() {
    use std::io::Write;
    let root = existing_live_root();
    let mut config: Value =
        serde_json::from_slice(&std::fs::read(root.join("config.json")).unwrap()).unwrap();
    let secret = std::fs::read_to_string(root.join("secret")).unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    config["listen"] = json!(format!("127.0.0.1:{port}"));
    // Retained databases may predate data-driven CLI profiles; rebuild each definition from the
    // shipped example while keeping the recorded executables, so the stored sessions stay bound.
    for profile in config["profiles"].as_array_mut().unwrap() {
        let Some(old) = profile.get("cli").cloned() else {
            continue;
        };
        let mut cli = example_profile(old["kind"].as_str().unwrap());
        cli["program"] = old["program"].clone();
        cli["helper_program"] = old["helper_program"].clone();
        profile["cli"] = cli;
    }
    let mut config_copy = tempfile::NamedTempFile::new_in(&root).unwrap();
    config_copy
        .write_all(&serde_json::to_vec(&config).unwrap())
        .unwrap();
    config_copy.flush().unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_agent-runner-server"))
        .arg("--config")
        .arg(config_copy.path())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut child = ChildGuard(child);
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let url = format!("http://127.0.0.1:{port}/v1/agents");
    let bearer = format!("live.{secret}");
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(response) = client.get(&url).bearer_auth(&bearer).send().await
                && response.status() == 200
            {
                break;
            }
            if let Some(status) = child.0.try_wait().unwrap() {
                panic!("daemon failed to reopen owned state: {status}");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("daemon readiness");
    // SAFETY: The PID identifies the daemon spawned by this test.
    assert_eq!(unsafe { libc::kill(child.0.id() as i32, libc::SIGTERM) }, 0);
    let status = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("daemon shutdown");
    assert!(status.success());
    println!(
        "existing private test database and both native owners reopened without a provider request"
    );
}

#[tokio::test]
#[ignore = "reopens an explicitly selected live test database, sends provider requests, and adds runs to its retained records"]
async fn approved_real_cli_daemon_rest_mcp_on_existing_owned_state() {
    // The native state already belongs to this database, so the live flow reuses it instead of creating a new owner.
    let root = existing_live_root();
    let daemon = prepare_existing_daemon(&root, |_| {});
    let counts = || owned_counts(&root);
    let before = counts();
    live_rest_mcp_flow(
        daemon.config.path(),
        daemon.log,
        daemon.port,
        &daemon.secret,
        &daemon.suffix,
    )
    .await;
    let after = counts();
    assert_eq!(
        after.0 - before.0,
        4,
        "cross-transport replay must not create extra runs"
    );
    assert_eq!(after.1 - before.1, 2);
    println!(
        "live daemon on existing owned state, cross-transport replay and durable sessions passed"
    );
}

#[tokio::test]
#[ignore = "reopens an explicitly selected live test database, sends provider requests, and adds runs, skills and hooks to its retained records"]
async fn approved_real_cli_daemon_skill_and_completion_hooks_on_existing_owned_state() {
    // Inbox delivery is the only destination allowed for real runs; the test connector stays closed to them.
    let root = existing_live_root();
    let daemon = prepare_existing_daemon(&root, |config| {
        config["delivery"] = json!({
            "max_pending":8,"max_payload_bytes":65536,"max_workers":1,"max_attempts":2,
            "delivery_ttl_ms":600000,"retention_ms":600000,"lease_ms":2000,"timeout_ms":1000,"retry_ms":50,"poll_ms":10,
            "destinations":[
                {"id":"inbox","revision":"v1","kind":"inbox"},
                {"id":"test-webhook","revision":"v1","kind":"webhook_test","endpoint":"http://127.0.0.1:9/hook"}
            ]
        });
        let client = &mut config["clients"][0];
        client["scopes"] = json!([
            "run_start",
            "run_read",
            "run_cancel",
            "session_read",
            "skill_read",
            "skill_write",
            "hook_read",
            "hook_write",
            "inbox_read",
            "inbox_ack"
        ]);
        client["destinations"] = json!(["inbox", "test-webhook"]);
    });
    let suffix = daemon.suffix.clone();
    let before = owned_counts(&root);
    let (child, client, url, bearer) = start_live_daemon(
        daemon.config.path(),
        daemon.log,
        daemon.port,
        &daemon.secret,
    )
    .await;
    let post = |path: &str, body: Value| {
        client
            .post(format!("{url}{path}"))
            .bearer_auth(&bearer)
            .json(&body)
            .send()
    };
    let skill = json!({"skill_id":"live-marker","version":format!("1{suffix}")});
    let response = post(
        "/v1/skills",
        json!({"skill_id":"live-marker","version":format!("1{suffix}"),"markdown":"End every reply with the exact word KESTREL on its own line.","required_capabilities":[]}),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), 200, "skill registration");
    for (hook, destination) in [("live-inbox", "inbox"), ("live-webhook", "test-webhook")] {
        let response = post(
            "/v1/hooks",
            json!({"hook_id":format!("{hook}{suffix}"),"version":"1","destination_id":destination,"events":["succeeded","failed"]}),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), 200, "{hook} registration");
    }
    let request = |profile: &str, workspace: &str, key: &str, hook: &str| {
        json!({
            "profile_id":profile,"workspace_id":workspace,"session":{"mode":"new"},
            "idempotency_key":format!("{key}{suffix}"),
            "skills":[skill.clone()],
            "hooks":[{"hook_id":format!("{hook}{suffix}"),"version":"1"}],
            "input":{"request":"Reply with one short sentence about the sky. Do not use tools."}
        })
    };
    // A real run must not bind a test connector, and the rejected admission must not create a run.
    let rejected = post(
        "/v1/runs",
        request("codex", "a", "codex-webhook", "live-webhook"),
    )
    .await
    .unwrap();
    assert_eq!(rejected.status(), 409, "test connector for a real run");
    let rejected: Value = rejected.json().await.unwrap();
    assert_eq!(rejected["error"]["code"], "hook_unavailable", "{rejected}");
    let codex = rest_start(
        &client,
        &url,
        &bearer,
        &request("codex", "a", "codex-skill", "live-inbox"),
        202,
    )
    .await;
    let claude = mcp(
        &client,
        &url,
        &bearer,
        "agents_start",
        request("claude", "b", "claude-skill", "live-inbox"),
    )
    .await;
    let mut runs = Vec::new();
    for (receipt, label) in [(&codex, "codex"), (&claude, "claude")] {
        let run = receipt["run_id"].as_str().unwrap().to_owned();
        wait_success(&client, &url, &bearer, &run, label).await;
        let view = mcp(&client, &url, &bearer, "runs_result", json!({"run_id":run})).await;
        let data: Value =
            serde_json::from_str(view["data"].as_str().expect("UTF-8 result")).unwrap();
        assert!(
            data["details"]["outcome"]["output"]
                .as_str()
                .is_some_and(|output| output.contains("KESTREL")),
            "{label}: the selected skill did not reach the CLI prompt"
        );
        runs.push(run);
    }
    println!("both real CLIs followed the selected skill");
    let items = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let page: Value = post("/v1/inbox/list", json!({"limit":16}))
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            let items: Vec<Value> = page["items"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|item| {
                    runs.iter()
                        .any(|run| item["notification"]["run_id"] == run.as_str())
                })
                .cloned()
                .collect();
            if items.len() == runs.len() {
                break items;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("inbox notifications for both real runs");
    for item in &items {
        assert_eq!(item["notification"]["synthetic"], false, "{item}");
        assert_eq!(item["notification"]["state"], "succeeded", "{item}");
        let response = post("/v1/inbox/ack", json!({"inbox_id":item["inbox_id"]}))
            .await
            .unwrap();
        assert_eq!(response.status(), 200, "inbox ack");
    }
    println!("both real runs delivered a non-synthetic inbox notification");
    stop_live_daemon(child).await;
    let after = owned_counts(&root);
    assert_eq!(
        after.0 - before.0,
        2,
        "the rejected admission created a run"
    );
    assert_eq!(after.1 - before.1, 2);
    println!(
        "live daemon skill, inbox hook and test connector boundary passed on existing owned state"
    );
}
