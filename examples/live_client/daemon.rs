//! Configuration copies over a retained owned database and supervision of one daemon process.

use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::{
        fs::{OpenOptionsExt, PermissionsExt},
        process::CommandExt,
    },
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use serde_json::{Value, json};

use crate::{api::Api, state};

/// How long a graceful stop waits for active runs before leaving the daemon running.
const IDLE_WAIT: Duration = Duration::from_secs(300);

/// A retained live directory whose database already owns the real CLI state directories.
pub struct Root {
    pub dir: PathBuf,
    pub config: Value,
    credential_id: String,
    secret: String,
}

/// One CLI profile of the retained configuration and the workspace it may use.
#[derive(Clone, Debug)]
pub struct Target {
    pub cli: String,
    pub profile_id: String,
    pub workspace_id: String,
}

impl Root {
    /// Opens `dir` after checking that it is inside the project temp directory and private.
    pub fn open(dir: &Path) -> Result<Self, String> {
        let dir = dir
            .canonicalize()
            .map_err(|error| format!("live directory {}: {error}", dir.display()))?;
        let temp = Path::new(env!("CARGO_MANIFEST_DIR")).join("temp");
        let temp = temp
            .canonicalize()
            .map_err(|error| format!("project temp directory: {error}"))?;
        if !dir.starts_with(&temp) {
            return Err(format!(
                "live directory must be inside {}; got {}",
                temp.display(),
                dir.display()
            ));
        }
        for path in [dir.clone(), dir.join("runs.sqlite"), dir.join("secret")] {
            let mode = fs::metadata(&path)
                .map_err(|error| format!("{}: {error}", path.display()))?
                .permissions()
                .mode();
            if mode & 0o077 != 0 {
                return Err(format!(
                    "{} must not be accessible to group or others",
                    path.display()
                ));
            }
        }
        let config: Value = serde_json::from_slice(
            &fs::read(dir.join("config.json")).map_err(|error| format!("config.json: {error}"))?,
        )
        .map_err(|error| format!("config.json: {error}"))?;
        let client = &config["clients"][0];
        let credential_id = client["credential_id"]
            .as_str()
            .ok_or("config.json has no client credential")?
            .to_owned();
        if client["secret_file"].as_str() != Some("secret") {
            return Err("the first client must use the `secret` file of the live directory".into());
        }
        let secret = fs::read_to_string(dir.join("secret"))
            .map_err(|error| format!("secret: {error}"))?
            .trim()
            .to_owned();
        Ok(Self {
            dir,
            config,
            credential_id,
            secret,
        })
    }

    pub fn database(&self) -> PathBuf {
        let name = self.config["database"].as_str().unwrap_or("runs.sqlite");
        self.dir.join(name)
    }

    pub fn credential_id(&self) -> &str {
        &self.credential_id
    }

    /// Builds the bearer token; callers must never print it.
    pub fn bearer(&self) -> String {
        format!("{}.{}", self.credential_id, self.secret)
    }

    pub fn page_size(&self) -> u64 {
        self.config["run"]["max_page_size"].as_u64().unwrap_or(16)
    }

    /// Lists CLI profiles in configuration order; each uses its first granted workspace.
    pub fn targets(&self) -> Result<Vec<Target>, String> {
        let mut targets = Vec::new();
        for profile in self.config["profiles"].as_array().into_iter().flatten() {
            let Some(cli) = profile["cli"]["kind"].as_str() else {
                continue;
            };
            let profile_id = profile["profile_id"]
                .as_str()
                .ok_or("profile without profile_id")?;
            let workspace_id = profile["workspaces"][0]
                .as_str()
                .ok_or_else(|| format!("profile {profile_id} has no workspace"))?;
            targets.push(Target {
                cli: cli.to_owned(),
                profile_id: profile_id.to_owned(),
                workspace_id: workspace_id.to_owned(),
            });
        }
        Ok(targets)
    }
}

/// Paths and switches used to turn the retained configuration into a runnable copy.
pub struct LaunchOptions {
    pub server: PathBuf,
    pub helper: PathBuf,
    pub profiles: PathBuf,
    pub overlay: PathBuf,
    pub programs: BTreeMap<String, PathBuf>,
    pub accept_installed_version: bool,
}

/// A private configuration copy and log in the live directory; the copy is removed on drop.
pub struct Prepared {
    pub config_path: PathBuf,
    pub log_path: PathBuf,
    config: Value,
}

impl Prepared {
    /// Copies the retained configuration with live CLI profiles and the overlay applied.
    pub fn create(root: &Root, options: &LaunchOptions, suffix: &str) -> Result<Self, String> {
        let mut config = root.config.clone();
        for profile in config["profiles"].as_array_mut().into_iter().flatten() {
            let Some(kind) = profile["cli"]["kind"].as_str().map(str::to_owned) else {
                continue;
            };
            profile["cli"] = cli_profile(&kind, options)?;
        }
        apply_overlay(&mut config, &options.overlay)?;
        let config_path = root.dir.join(format!("live-client-{suffix}.json"));
        let log_path = root.dir.join(format!("live-client-{suffix}.log"));
        let prepared = Self {
            config_path,
            log_path,
            config,
        };
        prepared.write()?;
        Ok(prepared)
    }

    /// Rewrites the copy so the next daemon start listens on a fresh loopback port.
    pub fn assign_port(&mut self) -> Result<u16, String> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")
            .map_err(|error| format!("free port: {error}"))?;
        let port = listener
            .local_addr()
            .map_err(|error| format!("free port: {error}"))?
            .port();
        drop(listener);
        self.config["listen"] = json!(format!("127.0.0.1:{port}"));
        self.write()?;
        Ok(port)
    }

    fn write(&self) -> Result<(), String> {
        let bytes = serde_json::to_vec_pretty(&self.config).map_err(|error| error.to_string())?;
        let temporary = self.config_path.with_extension("json.tmp");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)
            .map_err(|error| format!("{}: {error}", temporary.display()))?;
        file.write_all(&bytes)
            .and_then(|()| file.sync_all())
            .map_err(|error| format!("{}: {error}", temporary.display()))?;
        fs::rename(&temporary, &self.config_path)
            .map_err(|error| format!("{}: {error}", self.config_path.display()))
    }

    fn open_log(&self) -> Result<File, String> {
        OpenOptions::new()
            .append(true)
            .create(true)
            .mode(0o600)
            .open(&self.log_path)
            .map_err(|error| format!("{}: {error}", self.log_path.display()))
    }
}

impl Drop for Prepared {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.config_path);
    }
}

/// Loads the reviewed profile for `kind` and binds it to the local program and helper.
fn cli_profile(kind: &str, options: &LaunchOptions) -> Result<Value, String> {
    let path = options.profiles.join(format!("{kind}.json"));
    let mut profile: Value = serde_json::from_slice(
        &fs::read(&path).map_err(|error| format!("{}: {error}", path.display()))?,
    )
    .map_err(|error| format!("{}: {error}", path.display()))?;
    let program = match options.programs.get(kind) {
        Some(program) => program.clone(),
        None => find_in_path(kind)?,
    };
    refuse_forbidden_environment(kind, &profile)?;
    let output = Command::new(&program)
        .arg("--version")
        .stdin(Stdio::null())
        .output()
        .map_err(|error| format!("{} --version: {error}", program.display()))?;
    let installed = String::from_utf8(output.stdout)
        .map_err(|_| format!("{kind}: version output is not UTF-8"))?
        .trim()
        .to_owned();
    if !output.status.success() {
        return Err(format!("{kind}: version check failed"));
    }
    let expected = profile["version"]["expected_stdout"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    if installed != expected {
        if !options.accept_installed_version {
            return Err(format!(
                "{kind}: installed `{installed}` differs from the profile `{expected}`; pass --accept-installed-version to use the installed CLI for this run"
            ));
        }
        // Only the temporary copy accepts the installed version; the reviewed profile file stays unchanged.
        profile["version"]["expected_stdout"] = json!(installed);
    }
    profile["program"] = json!(program);
    profile["helper_program"] = json!(options.helper);
    Ok(profile)
}

fn find_in_path(name: &str) -> Result<PathBuf, String> {
    let path = std::env::var_os("PATH").ok_or("PATH is not set")?;
    std::env::split_paths(&path)
        .map(|directory| directory.join(name))
        .find(|candidate| candidate.is_file())
        .ok_or_else(|| format!("`{name}` was not found in PATH; pass --program {name}=PATH"))
}

/// Refuses to start when the environment holds a credential override that the profile forbids.
fn refuse_forbidden_environment(kind: &str, profile: &Value) -> Result<(), String> {
    let environment = &profile["environment"];
    let prefixes: Vec<&str> = environment["forbidden_prefixes"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect();
    let names: Vec<&str> = environment["forbidden_names"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect();
    let present: Vec<String> = std::env::vars_os()
        .filter_map(|(name, _)| name.into_string().ok())
        .filter(|name| {
            names.contains(&name.as_str()) || prefixes.iter().any(|prefix| name.starts_with(prefix))
        })
        .collect();
    if present.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "{kind}: unset {} before running; the profile forbids these credential overrides",
            present.join(", ")
        ))
    }
}

/// Applies the overlay: `config` keys are set only when absent, `client` lists are merged into every client.
fn apply_overlay(config: &mut Value, overlay: &Path) -> Result<(), String> {
    let overlay: Value = serde_json::from_slice(
        &fs::read(overlay).map_err(|error| format!("{}: {error}", overlay.display()))?,
    )
    .map_err(|error| format!("{}: {error}", overlay.display()))?;
    for (key, value) in overlay["config"].as_object().into_iter().flatten() {
        if config.get(key).is_none() {
            config[key] = value.clone();
        }
    }
    for client in config["clients"].as_array_mut().into_iter().flatten() {
        for (key, values) in overlay["client"].as_object().into_iter().flatten() {
            let list = client[key.as_str()].as_array().cloned().unwrap_or_default();
            let mut merged = list;
            for value in values.as_array().into_iter().flatten() {
                if !merged.contains(value) {
                    merged.push(value.clone());
                }
            }
            client[key.as_str()] = Value::Array(merged);
        }
    }
    Ok(())
}

/// A daemon started from a prepared copy in its own process group.
pub struct Daemon {
    child: Option<Child>,
    pub url: String,
}

impl Daemon {
    /// Starts the daemon and waits until the authenticated agent list answers.
    pub async fn start(
        server: &Path,
        prepared: &mut Prepared,
        bearer: &str,
    ) -> Result<(Self, Api), String> {
        let port = prepared.assign_port()?;
        let log = prepared.open_log()?;
        // A separate process group keeps a terminal Ctrl-C from stopping the daemon in the middle of a run.
        let child = Command::new(server)
            .arg("--config")
            .arg(&prepared.config_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::from(log))
            .process_group(0)
            .spawn()
            .map_err(|error| format!("start {}: {error}", server.display()))?;
        let url = format!("http://127.0.0.1:{port}");
        let mut daemon = Self {
            child: Some(child),
            url: url.clone(),
        };
        let api = Api::new(url, bearer.to_owned())?;
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Ok(reply) = api
                .call(crate::api::Via::Rest, "agents_list", &json!({}))
                .await
                && reply.status == 200
            {
                break;
            }
            if let Some(status) = daemon.try_wait()? {
                daemon.child = None;
                return Err(format!(
                    "daemon exited before readiness ({status}); see {}",
                    prepared.log_path.display()
                ));
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "daemon did not become ready; see {}",
                    prepared.log_path.display()
                ));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok((daemon, api))
    }

    pub fn pid(&self) -> Option<u32> {
        self.child.as_ref().map(Child::id)
    }

    fn try_wait(&mut self) -> Result<Option<std::process::ExitStatus>, String> {
        match self.child.as_mut() {
            Some(child) => child
                .try_wait()
                .map_err(|error| format!("daemon wait: {error}")),
            None => Ok(None),
        }
    }

    /// Waits until no run is active, then stops the daemon with SIGTERM.
    /// If runs stay active, the daemon is left running because stopping it would interrupt a launched CLI.
    pub async fn stop(mut self, database: &Path) -> Result<(), String> {
        let deadline = Instant::now() + IDLE_WAIT;
        let mut announced = false;
        loop {
            let active = state::active_runs(database)?;
            if active == 0 {
                break;
            }
            if !announced {
                eprintln!("waiting for {active} active run(s) before stopping the daemon");
                announced = true;
            }
            if Instant::now() > deadline {
                let pid = self.pid().unwrap_or_default();
                self.child = None;
                return Err(format!(
                    "runs are still active; the daemon (pid {pid}) was left running. Stop it with SIGTERM after they finish"
                ));
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        let Some(mut child) = self.child.take() else {
            return Ok(());
        };
        let pid = i32::try_from(child.id()).map_err(|_| "daemon pid out of range")?;
        // SAFETY: kill only sends SIGTERM to the daemon process this client spawned and still owns.
        if unsafe { libc::kill(pid, libc::SIGTERM) } != 0 {
            return Err("failed to signal the daemon".into());
        }
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(status) = child
                .try_wait()
                .map_err(|error| format!("daemon wait: {error}"))?
            {
                return if status.success() {
                    Ok(())
                } else {
                    Err(format!("daemon exited with {status}"))
                };
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "daemon (pid {pid}) did not exit within 30 seconds of SIGTERM"
                ));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        // Killing here could interrupt a launched CLI and isolate its session, so an unexpected drop only reports.
        if let Some(child) = &self.child {
            eprintln!(
                "warning: the daemon (pid {}) is still running; stop it with SIGTERM once no run is active",
                child.id()
            );
        }
    }
}

/// Returns 8 random hexadecimal characters for unique file names and identifiers.
pub fn random_hex() -> Result<String, String> {
    let mut bytes = [0_u8; 4];
    File::open("/dev/urandom")
        .and_then(|mut file| file.read_exact(&mut bytes))
        .map_err(|error| format!("random bytes: {error}"))?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

/// Creates a private file in the live directory that fails if it already exists.
pub fn create_private(path: &Path) -> Result<File, String> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|error| format!("{}: {error}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overlay_sets_missing_keys_and_merges_client_lists() {
        let directory =
            std::env::temp_dir().join(format!("live-client-overlay-{}", random_hex().unwrap()));
        fs::create_dir(&directory).unwrap();
        let overlay = directory.join("overlay.json");
        fs::write(
            &overlay,
            r#"{"config":{"delivery":{"x":1},"run":{"ignored":true}},"client":{"scopes":["run_read","hook_read"]}}"#,
        )
        .unwrap();
        let mut config =
            json!({"run":{"kept":true},"clients":[{"scopes":["run_start","run_read"]}]});
        apply_overlay(&mut config, &overlay).unwrap();
        fs::remove_dir_all(&directory).unwrap();
        assert_eq!(config["delivery"], json!({"x":1}));
        assert_eq!(config["run"], json!({"kept":true}));
        assert_eq!(
            config["clients"][0]["scopes"],
            json!(["run_start", "run_read", "hook_read"])
        );
    }
}
