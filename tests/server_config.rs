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
