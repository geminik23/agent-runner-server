use std::{fs, path::Path, time::Duration};

use agent_runner_server::server::config::PreparedServer;
use serde_json::{Value, json};
use tempfile::tempdir;

fn settings(port: u16) -> Value {
    json!({
        "schema_version":1,"listen":format!("127.0.0.1:{port}"),"database":"runs.sqlite",
        "access":{"max_request_bytes":8192,"max_skill_bytes":1024,"max_identifier_bytes":128},
        "run":{"max_outstanding":8,"max_workers":1,"database_queue":16,"event_buffer":16,"max_events_per_run":32,"max_event_bytes":2048,"max_result_bytes":4096,"max_page_size":16,"run_timeout_ms":10000,"cleanup_grace_ms":1000,"poll_interval_ms":5},
        "transport":{"max_request_bytes":16384,"max_response_bytes":1048576,"max_header_bytes":8192,"max_inflight":16,"max_connections":16,"connection_idle_timeout_ms":4000,"connection_max_lifetime_ms":30000,"requests_per_second":1000,"burst":1000,"request_timeout_ms":2000,"shutdown_grace_ms":2000,"max_mcp_sessions":4,"mcp_session_idle_ms":60000},
        "clients":[{"credential_id":"full","generation":"test_generation","secret_file":"secret","owner_id":"owner","client_id":"client","scopes":["run_start","run_read","run_cancel","session_read"],"profiles":["profile"]}],
        "profiles":[{"profile_id":"profile","revision":"profile_v1","workspaces":["workspace"],"max_input_bytes":4096,"scenario":"success"}]
    })
}
fn write_config(path: &Path, value: &Value) {
    fs::write(path, serde_json::to_vec(value).unwrap()).unwrap();
}
fn secret(path: &Path) {
    fs::write(path, "07".repeat(32)).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
    }
}

#[test]
fn trusted_configuration_rejects_invalid_limits_unknown_fields_and_public_binding() {
    let temp = tempdir().unwrap();
    let config = temp.path().join("config.json");
    secret(&temp.path().join("secret"));
    let value = settings(48001);
    write_config(&config, &value);
    assert!(PreparedServer::load(&config).is_ok());
    assert!(!temp.path().join("runs.sqlite").exists());
    let mut retention = value.clone();
    retention["retention"] = json!({"result_event_retention_ms": 1000});
    write_config(&config, &retention);
    assert!(PreparedServer::load(&config).is_ok());
    retention["retention"]["result_event_retention_ms"] = json!(0);
    write_config(&config, &retention);
    assert!(PreparedServer::load(&config).is_err());
    retention["retention"]["result_event_retention_ms"] = json!(u64::MAX);
    write_config(&config, &retention);
    assert!(PreparedServer::load(&config).is_err());
    let mut invalid = value.clone();
    invalid["listen"] = json!("0.0.0.0:48001");
    write_config(&config, &invalid);
    assert!(PreparedServer::load(&config).is_err());
    let mut invalid = value.clone();
    invalid["listen"] = json!("127.0.0.1:0");
    write_config(&config, &invalid);
    assert!(PreparedServer::load(&config).is_err());
    let mut invalid = value.clone();
    invalid["clients"][0]["secret"] = json!("not allowed");
    write_config(&config, &invalid);
    assert!(PreparedServer::load(&config).is_err());
    let mut invalid = value.clone();
    invalid["transport"]["max_response_bytes"] = json!(8192);
    write_config(&config, &invalid);
    assert!(PreparedServer::load(&config).is_err());
    let mut invalid = value.clone();
    invalid["clients"][0]["generation"] = json!("");
    write_config(&config, &invalid);
    assert!(PreparedServer::load(&config).is_err());
    fs::write(&config, br#"{"schema_version":1,"schema_version":1}"#).unwrap();
    assert!(PreparedServer::load(&config).is_err());
}

#[test]
fn delivery_configuration_accepts_only_explicit_loopback_test_destinations() {
    let temp = tempdir().unwrap();
    let config = temp.path().join("config.json");
    secret(&temp.path().join("secret"));
    let mut value = settings(48001);
    value["clients"][0]["destinations"] = json!(["test", "inbox"]);
    value["delivery"] = json!({"max_pending":8,"max_payload_bytes":32768,"max_workers":1,"max_attempts":2,"delivery_ttl_ms":10000,"retention_ms":1000,"lease_ms":1000,"timeout_ms":500,"retry_ms":20,"poll_ms":5,"destinations":[
        {"id":"test","revision":"v1","kind":"webhook_test","endpoint":"http://127.0.0.1:8888/hook"},
        {"id":"inbox","revision":"v1","kind":"inbox"}
    ]});
    write_config(&config, &value);
    assert!(PreparedServer::load(&config).is_ok());
    for endpoint in [
        "http://example.com:8888/hook",
        "http://localhost:8888/hook",
        "https://127.0.0.1:8888/hook",
        "http://user@127.0.0.1:8888/hook",
        "http://127.0.0.1:8888/hook?target=evil",
    ] {
        let mut invalid = value.clone();
        invalid["delivery"]["destinations"][0]["endpoint"] = json!(endpoint);
        write_config(&config, &invalid);
        assert!(PreparedServer::load(&config).is_err(), "{endpoint}");
        assert!(!temp.path().join("runs.sqlite").exists());
    }
    let mut invalid = value.clone();
    invalid["transport"]["max_response_bytes"] = json!(12288);
    write_config(&config, &invalid);
    assert!(PreparedServer::load(&config).is_err());
    let mut invalid = value.clone();
    invalid["clients"][0]["destinations"] = json!(["missing"]);
    write_config(&config, &invalid);
    assert!(PreparedServer::load(&config).is_err());
    let mut without_delivery = settings(48001);
    without_delivery["clients"][0]["reconciliation_destinations"] = json!(["removed-destination"]);
    write_config(&config, &without_delivery);
    assert!(PreparedServer::load(&config).is_ok());
    let mut invalid = value.clone();
    invalid["delivery"]["delivery_ttl_ms"] = json!(i64::MAX);
    write_config(&config, &invalid);
    assert!(PreparedServer::load(&config).is_err());
    let mut invalid = value;
    invalid["delivery"]["max_payload_bytes"] = json!(1024);
    write_config(&config, &invalid);
    assert!(PreparedServer::load(&config).is_err());
    assert!(!temp.path().join("runs.sqlite").exists());
}

#[tokio::test]
async fn existing_small_response_configuration_remains_startable() {
    let temp = tempdir().unwrap();
    let config = temp.path().join("config.json");
    secret(&temp.path().join("secret"));
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let mut value = settings(port);
    value["run"]["max_event_bytes"] = json!(256);
    value["run"]["max_page_size"] = json!(1);
    value["transport"]["max_response_bytes"] = json!(73728);
    write_config(&config, &value);
    let prepared = PreparedServer::load(&config).unwrap();
    assert!(!temp.path().join("runs.sqlite").exists());
    let server = prepared.start().await.unwrap();
    let stop = agent_runner_server::core::CancellationToken::new();
    stop.cancel();
    server.run(stop).await.unwrap();
}

#[cfg(unix)]
#[test]
fn secret_files_must_be_private_regular_files_with_exact_encoding() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let temp = tempdir().unwrap();
    let config = temp.path().join("config.json");
    let path = temp.path().join("secret");
    write_config(&config, &settings(48001));
    secret(&path);
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(PreparedServer::load(&config).is_err());
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    fs::write(&path, "07".repeat(31)).unwrap();
    assert!(PreparedServer::load(&config).is_err());
    fs::write(&path, "07".repeat(32)).unwrap();
    let link = temp.path().join("secret-link");
    symlink(&path, &link).unwrap();
    let mut value = settings(48001);
    value["clients"][0]["secret_file"] = json!("secret-link");
    write_config(&config, &value);
    assert!(PreparedServer::load(&config).is_err());
}

#[cfg(unix)]
#[tokio::test]
async fn binary_starts_from_protected_config_accepts_a_dummy_run_and_stops_on_sigterm() {
    use std::{
        io::Read,
        process::{Child, Command, Stdio},
    };
    struct ChildGuard(Child);
    impl Drop for ChildGuard {
        fn drop(&mut self) {
            if self.0.try_wait().ok().flatten().is_none() {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
    }
    let temp = tempdir().unwrap();
    let config = temp.path().join("config.json");
    secret(&temp.path().join("secret"));
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    write_config(&config, &settings(port));
    let child = Command::new(env!("CARGO_BIN_EXE_agent-runner-server"))
        .arg("--config")
        .arg(&config)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
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
            match client
                .get(format!("{url}/v1/agents"))
                .bearer_auth(&bearer)
                .send()
                .await
            {
                Ok(r) if r.status() == 200 => break,
                _ => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
    })
    .await
    .expect("binary did not become ready");
    let body = json!({"profile_id":"profile","workspace_id":"workspace","input":{"request":"private fixture"},"idempotency_key":"binary-test"});
    let response = client
        .post(format!("{url}/v1/runs"))
        .bearer_auth(&bearer)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 202);
    let run = response.json::<Value>().await.unwrap();
    let run_id = run["run_id"].as_str().unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let value = client
                .get(format!("{url}/v1/runs/{run_id}"))
                .bearer_auth(&bearer)
                .send()
                .await
                .unwrap()
                .json::<Value>()
                .await
                .unwrap();
            if value["state"] == "succeeded" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        Command::new("kill")
            .arg("-TERM")
            .arg(child.0.id().to_string())
            .status()
            .unwrap()
            .success()
    );
    let status = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(s) = child.0.try_wait().unwrap() {
                break s;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("binary did not stop");
    assert!(status.success());
    let mut output = String::new();
    child
        .0
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut output)
        .unwrap();
    assert!(!output.contains(&bearer));
    assert!(!output.contains("private fixture"));
}

#[cfg(unix)]
#[tokio::test]
async fn forced_daemon_crash_does_not_repeat_a_started_dummy_run() {
    use std::process::{Child, Command, Stdio};
    struct ChildGuard(Child);
    impl Drop for ChildGuard {
        fn drop(&mut self) {
            if self.0.try_wait().ok().flatten().is_none() {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
    }
    fn spawn(config: &Path) -> ChildGuard {
        ChildGuard(
            Command::new(env!("CARGO_BIN_EXE_agent-runner-server"))
                .arg("--config")
                .arg(config)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        )
    }
    let temp = tempdir().unwrap();
    let config = temp.path().join("config.json");
    secret(&temp.path().join("secret"));
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let mut value = settings(port);
    value["profiles"][0]["scenario"] = json!("delayed-success");
    write_config(&config, &value);
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_millis(500))
        .build()
        .unwrap();
    let url = format!("http://127.0.0.1:{port}");
    let bearer = format!("full.{}", "07".repeat(32));
    let mut child = spawn(&config);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if client
                .get(format!("{url}/v1/agents"))
                .bearer_auth(&bearer)
                .send()
                .await
                .is_ok_and(|r| r.status() == 200)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("first daemon did not start");
    let request = json!({"profile_id":"profile","workspace_id":"workspace","input":{"request":"synthetic crash fixture"},"idempotency_key":"crash-test"});
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
    let session = receipt["session_id"].as_str().unwrap();
    let path = temp.path().join("runs.sqlite");
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let connection = rusqlite::Connection::open(&path).unwrap();
            let observed: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM attempts WHERE run_id=?1 AND spawn_intent=1) AND EXISTS(SELECT 1 FROM events WHERE run_id=?1 AND kind='run.started') AND EXISTS(SELECT 1 FROM events WHERE run_id=?1 AND kind='run.session')", [run], |row| row.get(0)).unwrap_or(false);
            if observed { break; }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }).await.expect("dummy launch was not recorded");
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    let mut restarted = spawn(&config);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if client
                .get(format!("{url}/v1/agents"))
                .bearer_auth(&bearer)
                .send()
                .await
                .is_ok_and(|r| r.status() == 200)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("restarted daemon did not start");
    let response = client
        .post(format!("{url}/v1/runs"))
        .bearer_auth(&bearer)
        .json(&request)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let replay: Value = response.json().await.unwrap();
    assert_eq!(replay["run_id"], run);
    assert_eq!(replay["replay"], true);
    let status: Value = client
        .get(format!("{url}/v1/runs/{run}"))
        .bearer_auth(&bearer)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(status["state"], "interrupted");
    let mut resume = request;
    resume["idempotency_key"] = json!("resume-after-crash");
    resume["session"] = json!({"mode":"resume","session_id":session});
    assert_eq!(
        client
            .post(format!("{url}/v1/runs"))
            .bearer_auth(&bearer)
            .json(&resume)
            .send()
            .await
            .unwrap()
            .status(),
        409
    );
    let db = rusqlite::Connection::open(&path).unwrap();
    let (starts, terminals): (i64, i64) = db
        .query_row(
            "SELECT SUM(kind='run.started'),SUM(kind='run.finished') FROM events WHERE run_id=?1",
            [run],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!((starts, terminals), (1, 1));
    let (cleanup, reserved, native): (String, String, String) = db
        .query_row(
            "SELECT cleanup,reserved_run_id,native_id FROM sessions WHERE session_id=?1",
            [session],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(cleanup, "uncertain");
    assert_eq!(reserved, run);
    drop(db);
    resume["idempotency_key"] = json!("native-after-crash");
    resume["session"] = json!({"mode":"resume_native","native_id":native});
    assert_eq!(
        client
            .post(format!("{url}/v1/runs"))
            .bearer_auth(&bearer)
            .json(&resume)
            .send()
            .await
            .unwrap()
            .status(),
        409
    );
    restarted.0.kill().unwrap();
    restarted.0.wait().unwrap();
}
