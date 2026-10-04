//! Scenario files: ordered steps with variables and soft expectations, run against one CLI target at a time.

use std::{
    collections::HashMap,
    fs,
    io::Write,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use serde::Deserialize;
use serde_json::{Value, json};

use crate::{
    api::{Api, Via},
    daemon::{Daemon, Prepared, Root, Target},
    state,
};

/// Terminal run states reported by the service.
const TERMINAL: [&str; 5] = [
    "succeeded",
    "failed",
    "cancelled",
    "timed_out",
    "interrupted",
];

/// Default wait for one run; longer than the shipped run and CLI timeouts so the service decides the outcome.
const DEFAULT_WAIT_MS: u64 = 180_000;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    pub name: String,
    pub description: String,
    pub steps: Vec<Step>,
}

/// One scenario step. `${name}` and `${name.field}` in strings refer to built-in or saved values.
#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Step {
    /// Submits a run; the receipt and the idempotency key used are saved under `id`.
    Start {
        id: String,
        #[serde(default)]
        via: Via,
        request: Value,
        #[serde(default)]
        expect: Box<Expect>,
    },
    /// Polls a run until it is terminal without ever cancelling it, then reads its result and events.
    Wait {
        run: String,
        #[serde(default)]
        expect: Box<Expect>,
        timeout_ms: Option<u64>,
    },
    /// Cancels `run` only while it is queued behind a launched `blocker`, so no CLI is ever interrupted.
    CancelQueued { run: String, blocker: String },
    /// Calls any tool; the response body is saved under `save`.
    Call {
        tool: String,
        #[serde(default)]
        via: Via,
        #[serde(default)]
        args: Value,
        #[serde(default)]
        expect: Box<Expect>,
        save: Option<String>,
    },
    /// Reads the native CLI session ID of a server session from the database and saves it as a string.
    NativeId { session: String, save: String },
    /// Waits for inbox notifications of the given runs, checks them and acknowledges them.
    Inbox {
        runs: Vec<String>,
        #[serde(default)]
        expect: Box<Expect>,
        timeout_ms: Option<u64>,
    },
    /// Stops the idle daemon and starts it again from the same configuration.
    Restart {},
    /// Cancels `run` after it has been running for `delay_ms`; the CLI is interrupted and its session is quarantined.
    /// The scenario must release the run later, and running it needs `--allow-interruption`.
    CancelRunning {
        run: String,
        #[serde(default)]
        delay_ms: u64,
        #[serde(default)]
        expect: Box<Expect>,
    },
    /// Kills the daemon with SIGKILL after `run` has been running for `delay_ms`; launched CLIs may outlive it.
    /// The daemon stays down until the next `release` or `restart` step.
    Crash {
        run: String,
        #[serde(default)]
        delay_ms: u64,
    },
    /// Stops the daemon once idle, lists and releases `runs` with the server's offline quarantine command, then restarts it.
    /// A release refused because processes still work in the workspace is retried until they exit.
    /// `kinds`, when present, lists the expected quarantine kind per run.
    Release {
        runs: Vec<String>,
        kinds: Option<Vec<String>>,
    },
}

/// Soft expectations: a mismatch is reported and the scenario continues.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Expect {
    /// Expected error code; absent means the call must succeed.
    pub error: Option<String>,
    pub replay: Option<bool>,
    /// The receipt must carry the same run ID as the saved start step of this name.
    pub same_run_as: Option<String>,
    /// Expected terminal state; `wait` defaults to `succeeded`.
    pub state: Option<String>,
    pub output_contains: Option<String>,
    /// Expected result schema validation status: `passed` or `failed`.
    pub validation: Option<String>,
    pub synthetic: Option<bool>,
    /// A warning code that the run result must report.
    pub warning: Option<String>,
}

impl Scenario {
    pub fn load(path: &Path) -> Result<Self, String> {
        let text =
            fs::read_to_string(path).map_err(|error| format!("{}: {error}", path.display()))?;
        let scenario: Self =
            serde_json::from_str(&text).map_err(|error| format!("{}: {error}", path.display()))?;
        scenario
            .validate()
            .map_err(|error| format!("{}: {error}", path.display()))?;
        Ok(scenario)
    }

    fn validate(&self) -> Result<(), String> {
        if !plain(&self.name) {
            return Err(format!(
                "scenario name `{}` must be a plain identifier",
                self.name
            ));
        }
        let mut ids = Vec::new();
        let mut unreleased: Vec<&String> = Vec::new();
        for step in &self.steps {
            match step {
                Step::Start { id, .. } => {
                    if !plain(id) || ids.contains(id) {
                        return Err(format!("start id `{id}` must be a unique plain identifier"));
                    }
                    ids.push(id.clone());
                }
                Step::CancelRunning { run, .. } | Step::Crash { run, .. } => unreleased.push(run),
                Step::Release { runs, kinds } => {
                    if kinds
                        .as_ref()
                        .is_some_and(|kinds| kinds.len() != runs.len())
                    {
                        return Err("release kinds must list one kind per run".into());
                    }
                    unreleased.retain(|run| !runs.contains(run));
                }
                _ => {}
            }
        }
        // An interrupted run left quarantined would block later live work in the retained database.
        if let Some(run) = unreleased.first() {
            return Err(format!(
                "interrupted run `{run}` must be released by a later release step"
            ));
        }
        Ok(())
    }

    /// Whether the scenario interrupts a launched CLI and therefore needs explicit permission.
    pub fn interrupts(&self) -> bool {
        self.steps
            .iter()
            .any(|step| matches!(step, Step::CancelRunning { .. } | Step::Crash { .. }))
    }

    /// Upper bound of provider requests per target: every start expected to admit a new run.
    pub fn provider_requests(&self) -> usize {
        self.steps
            .iter()
            .filter(|step| {
                matches!(step, Step::Start { expect, .. } if expect.error.is_none() && expect.replay != Some(true))
            })
            .count()
    }
}

fn plain(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

/// Resolves scenario arguments: a path, a shipped scenario name, or `all`.
pub fn resolve_paths(arguments: &[String], directory: &Path) -> Result<Vec<PathBuf>, String> {
    let mut paths = Vec::new();
    for argument in arguments {
        if argument == "all" {
            let mut shipped: Vec<PathBuf> = fs::read_dir(directory)
                .map_err(|error| format!("{}: {error}", directory.display()))?
                .filter_map(|entry| entry.ok().map(|entry| entry.path()))
                .filter(|path| {
                    path.extension()
                        .is_some_and(|extension| extension == "json")
                })
                .collect();
            shipped.sort();
            paths.extend(shipped);
        } else if Path::new(argument).is_file() {
            paths.push(PathBuf::from(argument));
        } else if !argument.ends_with(".json") && !argument.starts_with('/') {
            // Shipped names may include a subdirectory, such as `recovery/cancel_new_session`.
            paths.push(directory.join(format!("{argument}.json")));
        } else {
            paths.push(PathBuf::from(argument));
        }
    }
    if paths.is_empty() {
        return Err("no scenario given; pass names, paths or `all`".into());
    }
    Ok(paths)
}

/// Values visible to `${...}` references.
#[derive(Default)]
pub struct Vars {
    values: HashMap<String, Value>,
}

impl Vars {
    pub fn set(&mut self, name: &str, value: Value) {
        self.values.insert(name.to_owned(), value);
    }

    fn lookup(&self, reference: &str) -> Result<Value, String> {
        let mut parts = reference.split('.');
        let head = parts.next().unwrap_or_default();
        let mut value = self
            .values
            .get(head)
            .ok_or_else(|| format!("unknown variable `{head}`"))?;
        for part in parts {
            value = value
                .get(part)
                .filter(|value| !value.is_null())
                .ok_or_else(|| format!("`{reference}` has no value"))?;
        }
        Ok(value.clone())
    }

    /// Replaces references in every string of `value`; a string that is one whole reference keeps the JSON type.
    pub fn resolve(&self, value: &Value) -> Result<Value, String> {
        Ok(match value {
            Value::String(text) => self.resolve_text(text)?,
            Value::Array(items) => Value::Array(
                items
                    .iter()
                    .map(|item| self.resolve(item))
                    .collect::<Result<_, _>>()?,
            ),
            Value::Object(map) => Value::Object(
                map.iter()
                    .map(|(key, item)| Ok((key.clone(), self.resolve(item)?)))
                    .collect::<Result<_, String>>()?,
            ),
            other => other.clone(),
        })
    }

    fn resolve_text(&self, text: &str) -> Result<Value, String> {
        if let Some(inner) = text
            .strip_prefix("${")
            .and_then(|rest| rest.strip_suffix('}'))
            && !inner.contains("${")
            && !inner.contains('}')
        {
            return self.lookup(inner);
        }
        let mut output = String::new();
        let mut rest = text;
        while let Some(start) = rest.find("${") {
            output.push_str(&rest[..start]);
            let after = &rest[start + 2..];
            let end = after
                .find('}')
                .ok_or_else(|| format!("unterminated reference in `{text}`"))?;
            match self.lookup(&after[..end])? {
                Value::String(value) => output.push_str(&value),
                other => output.push_str(&other.to_string()),
            }
            rest = &after[end + 1..];
        }
        output.push_str(rest);
        Ok(Value::String(output))
    }

    pub fn resolve_string(&self, text: &str) -> Result<String, String> {
        match self.resolve_text(text)? {
            Value::String(value) => Ok(value),
            other => Ok(other.to_string()),
        }
    }
}

/// Outcome of one step.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Ok,
    Fail,
    Skip,
    Error,
}

impl Outcome {
    fn label(self) -> &'static str {
        match self {
            Self::Ok => "OK",
            Self::Fail => "FAIL",
            Self::Skip => "SKIP",
            Self::Error => "ERROR",
        }
    }
}

/// One reported step.
pub struct Record {
    pub scenario: String,
    pub cli: String,
    pub step: String,
    pub outcome: Outcome,
    pub detail: String,
}

/// Executes scenarios against one daemon and keeps the running report.
pub struct Runner<'a> {
    pub root: &'a Root,
    pub prepared: &'a mut Prepared,
    pub server: &'a Path,
    pub daemon: Option<Daemon>,
    pub api: Api,
    pub records: Vec<Record>,
    pub log: fs::File,
    pub run_suffix: String,
}

impl Runner<'_> {
    /// Runs every step of `scenario` for `target`; an error stops the remaining steps of this pair.
    pub async fn run(&mut self, scenario: &Scenario, target: &Target) {
        let mut vars = Vars::default();
        let suffix = format!("{}-{}", self.run_suffix, target.cli);
        let token = format!("{}{}", target.cli.to_uppercase(), &self.run_suffix[..6]);
        vars.set("cli", json!(target.cli));
        vars.set("profile", json!(target.profile_id));
        vars.set("workspace", json!(target.workspace_id));
        vars.set("suffix", json!(suffix));
        vars.set("token", json!(token));
        vars.set("scenario", json!(scenario.name));
        println!(
            "\n== {} / {}: {}",
            scenario.name, target.cli, scenario.description
        );
        for (index, step) in scenario.steps.iter().enumerate() {
            let label = format!("{:02} {}", index + 1, step_label(step));
            let (outcome, detail) = match self.step(step, &mut vars, &suffix).await {
                Ok(result) => result,
                Err(error) => (Outcome::Error, error),
            };
            self.record(Record {
                scenario: scenario.name.clone(),
                cli: target.cli.clone(),
                step: label,
                outcome,
                detail,
            });
            if outcome == Outcome::Error {
                println!("   remaining steps of this scenario were skipped");
                break;
            }
        }
    }

    fn record(&mut self, record: Record) {
        println!(
            "   {:<5} {:<34} {}",
            record.outcome.label(),
            record.step,
            record.detail
        );
        let line = json!({
            "scenario": record.scenario, "cli": record.cli, "step": record.step,
            "outcome": record.outcome.label(), "detail": record.detail,
        });
        if writeln!(self.log, "{line}").is_err() {
            eprintln!("warning: could not append to the JSONL report");
        }
        self.records.push(record);
    }

    async fn step(
        &mut self,
        step: &Step,
        vars: &mut Vars,
        suffix: &str,
    ) -> Result<(Outcome, String), String> {
        match step {
            Step::Start {
                id,
                via,
                request,
                expect,
            } => {
                let mut request = vars.resolve(request)?;
                let object = request
                    .as_object_mut()
                    .ok_or("start request must be an object")?;
                if !object.contains_key("idempotency_key") {
                    let scenario = vars.resolve_string("${scenario}")?;
                    object.insert(
                        "idempotency_key".into(),
                        json!(format!("{scenario}-{id}-{suffix}")),
                    );
                }
                let key = object["idempotency_key"].clone();
                let reply = self.api.call(*via, "agents_start", &request).await?;
                let mut problems = check_error(expect, reply.error.as_deref());
                if reply.error.is_none() {
                    let mut saved = reply.body.clone();
                    saved["idempotency_key"] = key;
                    if let Some(replay) = expect.replay
                        && reply.body["replay"].as_bool().unwrap_or(false) != replay
                    {
                        problems.push(format!("replay was {}", reply.body["replay"]));
                    }
                    if let Some(other) = &expect.same_run_as {
                        let expected = vars.lookup(&format!("{other}.run_id"))?;
                        if reply.body["run_id"] != expected {
                            problems.push(format!("run_id differs from {other}"));
                        }
                    }
                    vars.set(id, saved);
                }
                let summary = match &reply.error {
                    Some(code) => format!("{} {} -> {code}", via.name(), reply.status),
                    None => format!(
                        "{} {} run {} session {}{}",
                        via.name(),
                        reply.status,
                        short(&reply.body["run_id"]),
                        short(&reply.body["session_id"]),
                        if reply.body["replay"] == true {
                            " (replay)"
                        } else {
                            ""
                        }
                    ),
                };
                Ok(verdict(problems, summary))
            }
            Step::Wait {
                run,
                expect,
                timeout_ms,
            } => {
                let run_id = vars.resolve_string(&format!("${{{run}.run_id}}"))?;
                let view = self
                    .wait_terminal(&run_id, timeout_ms.unwrap_or(DEFAULT_WAIT_MS))
                    .await?;
                let mut saved = vars.lookup(run)?;
                for (key, value) in view.as_object().into_iter().flatten() {
                    saved[key] = value.clone();
                }
                vars.set(run, saved);
                let mut problems = Vec::new();
                let state = view["state"].as_str().unwrap_or_default();
                let expected = expect.state.as_deref().unwrap_or("succeeded");
                if state != expected {
                    problems.push(format!("state {state}, expected {expected}"));
                }
                if let Some(needle) = &expect.output_contains {
                    let needle = vars.resolve_string(needle)?;
                    if !view["output"]
                        .as_str()
                        .is_some_and(|output| output.contains(&needle))
                    {
                        problems.push(format!("output lacks `{needle}`"));
                    }
                }
                if let Some(code) = &expect.warning
                    && !view["warnings"]
                        .as_array()
                        .is_some_and(|codes| codes.iter().any(|item| item == code.as_str()))
                {
                    problems.push(format!("warning {code} was not reported"));
                }
                if let Some(validation) = &expect.validation
                    && view["validation"].as_str() != Some(validation.as_str())
                {
                    problems.push(format!(
                        "validation {}, expected {validation}",
                        view["validation"]
                    ));
                }
                let mut summary =
                    format!("{state} in {}s, {} events", view["seconds"], view["events"]);
                if let Some(error) = view["error"].as_str() {
                    summary.push_str(&format!(", error {error}"));
                }
                if let Some(validation) = view["validation"].as_str() {
                    summary.push_str(&format!(", validation {validation}"));
                }
                if let Some(codes) = view["warnings"]
                    .as_array()
                    .filter(|codes| !codes.is_empty())
                {
                    let codes: Vec<&str> = codes.iter().filter_map(Value::as_str).collect();
                    summary.push_str(&format!(", warnings {}", codes.join(",")));
                }
                if let Some(output) = view["output"].as_str() {
                    summary.push_str(&format!(", output \"{}\"", excerpt(output)));
                }
                Ok(verdict(problems, summary))
            }
            Step::CancelQueued { run, blocker } => {
                let run_id = vars.resolve_string(&format!("${{{run}.run_id}}"))?;
                let blocker_id = vars.resolve_string(&format!("${{{blocker}.run_id}}"))?;
                let run_state = self.state_of(&run_id).await?;
                let blocker_state = self.state_of(&blocker_id).await?;
                if run_state != "queued"
                    || !["preparing", "running"].contains(&blocker_state.as_str())
                {
                    // Cancelling a run that may already be launching could isolate its session, so it is left to finish.
                    let view = self.wait_terminal(&run_id, DEFAULT_WAIT_MS).await?;
                    return Ok((
                        Outcome::Skip,
                        format!(
                            "not cancelled: run was {run_state}, blocker {blocker_state}; run ended {}",
                            view["state"]
                        ),
                    ));
                }
                let reply = self
                    .api
                    .call(Via::Rest, "runs_cancel", &json!({"run_id":run_id}))
                    .await?;
                if let Some(code) = reply.error {
                    return Ok((Outcome::Fail, format!("cancel rejected: {code}")));
                }
                let outcome = reply.body["outcome"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned();
                let view = self.wait_terminal(&run_id, DEFAULT_WAIT_MS).await?;
                let mut problems = Vec::new();
                if outcome != "cancel_requested" {
                    problems.push(format!("cancel outcome {outcome}"));
                }
                if view["state"] != "cancelled" {
                    problems.push(format!("state {}", view["state"]));
                }
                let session = vars.resolve_string(&format!("${{{run}.session_id}}"))?;
                let stored = state::session_state(&self.root.database(), &session)?;
                let session_text = match &stored {
                    Some(stored) => {
                        if stored.cleanup == "uncertain" || stored.disposition == "uncertain" {
                            problems.push("session became uncertain".into());
                        }
                        format!(
                            "session cleanup {}, disposition {}, reserved {}",
                            stored.cleanup, stored.disposition, stored.reserved
                        )
                    }
                    None => "session record not found".into(),
                };
                Ok(verdict(
                    problems,
                    format!("{outcome}, run {}, {session_text}", view["state"]),
                ))
            }
            Step::Call {
                tool,
                via,
                args,
                expect,
                save,
            } => {
                let args = vars.resolve(args)?;
                if tool == "runs_cancel" {
                    return Err(
                        "use cancel_queued; running runs are never cancelled by scenarios".into(),
                    );
                }
                let reply = self.api.call(*via, tool, &args).await?;
                let problems = check_error(expect, reply.error.as_deref());
                if let (Some(name), None) = (save, &reply.error) {
                    vars.set(name, reply.body.clone());
                }
                let summary = match &reply.error {
                    Some(code) => format!("{tool} over {} -> {code}", via.name()),
                    None => format!("{tool} over {} ok", via.name()),
                };
                Ok(verdict(problems, summary))
            }
            Step::NativeId { session, save } => {
                let session = vars.resolve_string(session)?;
                let native = state::native_id(&self.root.database(), &session)?
                    .ok_or_else(|| format!("session {session} has no native ID"))?;
                let summary = format!("native ID {}", short(&json!(native)));
                vars.set(save, json!(native));
                Ok((Outcome::Ok, summary))
            }
            Step::Inbox {
                runs,
                expect,
                timeout_ms,
            } => {
                let wanted: Vec<String> = runs
                    .iter()
                    .map(|run| vars.resolve_string(&format!("${{{run}.run_id}}")))
                    .collect::<Result<_, _>>()?;
                let items = self
                    .wait_inbox(&wanted, timeout_ms.unwrap_or(30_000))
                    .await?;
                let mut problems = Vec::new();
                for item in &items {
                    if let Some(synthetic) = expect.synthetic
                        && item["notification"]["synthetic"].as_bool() != Some(synthetic)
                    {
                        problems.push(format!(
                            "synthetic was {}",
                            item["notification"]["synthetic"]
                        ));
                    }
                    if let Some(state) = &expect.state
                        && item["notification"]["state"].as_str() != Some(state.as_str())
                    {
                        problems.push(format!(
                            "notification state {}",
                            item["notification"]["state"]
                        ));
                    }
                    let reply = self
                        .api
                        .call(
                            Via::Rest,
                            "inbox_ack",
                            &json!({"inbox_id":item["inbox_id"]}),
                        )
                        .await?;
                    if let Some(code) = reply.error {
                        problems.push(format!("ack rejected: {code}"));
                    }
                }
                Ok(verdict(
                    problems,
                    format!("{} notification(s) found and acknowledged", items.len()),
                ))
            }
            Step::Restart {} => {
                let database = self.root.database();
                if let Some(daemon) = self.daemon.take() {
                    daemon.stop(&database).await?;
                }
                let (daemon, api) =
                    Daemon::start(self.server, self.prepared, &self.root.bearer()).await?;
                let url = daemon.url.clone();
                self.daemon = Some(daemon);
                self.api = api;
                Ok((
                    Outcome::Ok,
                    format!("daemon stopped while idle and restarted at {url}"),
                ))
            }
            Step::CancelRunning {
                run,
                delay_ms,
                expect,
            } => {
                let run_id = vars.resolve_string(&format!("${{{run}.run_id}}"))?;
                if let Some(state) = self.running_for(&run_id, *delay_ms).await? {
                    let view = self.wait_terminal(&run_id, DEFAULT_WAIT_MS).await?;
                    return Ok((
                        Outcome::Skip,
                        format!("not cancelled: run was {state} and ended {}", view["state"]),
                    ));
                }
                let reply = self
                    .api
                    .call(Via::Rest, "runs_cancel", &json!({"run_id":run_id}))
                    .await?;
                if let Some(code) = reply.error {
                    return Ok((Outcome::Fail, format!("cancel rejected: {code}")));
                }
                let view = self.wait_terminal(&run_id, DEFAULT_WAIT_MS).await?;
                let mut problems = Vec::new();
                let expected = expect.state.as_deref().unwrap_or("cancelled");
                if view["state"] != expected {
                    problems.push(format!("state {}, expected {expected}", view["state"]));
                }
                let session = vars.resolve_string(&format!("${{{run}.session_id}}"))?;
                Ok(verdict(
                    problems,
                    format!(
                        "{} after {}ms, run {}, {}",
                        reply.body["outcome"].as_str().unwrap_or_default(),
                        delay_ms,
                        view["state"],
                        self.describe_session(&session)?
                    ),
                ))
            }
            Step::Crash { run, delay_ms } => {
                let run_id = vars.resolve_string(&format!("${{{run}.run_id}}"))?;
                if let Some(state) = self.running_for(&run_id, *delay_ms).await? {
                    let view = self.wait_terminal(&run_id, DEFAULT_WAIT_MS).await?;
                    return Ok((
                        Outcome::Skip,
                        format!("not crashed: run was {state} and ended {}", view["state"]),
                    ));
                }
                let daemon = self.daemon.take().ok_or("no daemon to crash")?;
                daemon.kill()?;
                Ok((
                    Outcome::Ok,
                    format!("daemon killed after {delay_ms}ms of running"),
                ))
            }
            Step::Release { runs, kinds } => {
                let database = self.root.database();
                if let Some(daemon) = self.daemon.take() {
                    daemon.stop(&database).await?;
                }
                let listed = self.quarantine(&["list"])?;
                let mut problems = Vec::new();
                let mut details = Vec::new();
                for (index, run) in runs.iter().enumerate() {
                    let run_id = vars.resolve_string(&format!("${{{run}.run_id}}"))?;
                    let Some(line) = listed.lines().find(|line| line.starts_with(&run_id)) else {
                        problems.push(format!("{run} is not quarantined"));
                        continue;
                    };
                    let fields: Vec<&str> = line.split_whitespace().collect();
                    let kind = fields.last().copied().unwrap_or_default();
                    let state = fields.get(2).copied().unwrap_or_default();
                    if let Some(expected) = kinds.as_ref().map(|kinds| kinds[index].as_str())
                        && kind != expected
                    {
                        problems.push(format!("{run} was {kind}, expected {expected}"));
                    }
                    let started = Instant::now();
                    let deadline = started + Duration::from_millis(DEFAULT_WAIT_MS);
                    let mut refusals = 0;
                    let result = loop {
                        match self.quarantine(&["release", &run_id, "--confirm-no-cli-process"]) {
                            Err(error)
                                if error.contains("processes still work")
                                    && Instant::now() < deadline =>
                            {
                                // A CLI that outlived a crashed daemon still works in the workspace; wait for it to exit.
                                refusals += 1;
                                tokio::time::sleep(Duration::from_secs(1)).await;
                            }
                            other => break other,
                        }
                    };
                    let waited = if refusals == 0 {
                        String::new()
                    } else {
                        format!(
                            " (refused {refusals} time(s) while processes remained, released after {}s)",
                            started.elapsed().as_secs()
                        )
                    };
                    match result {
                        Ok(message) => details.push(format!(
                            "{run} {state} {kind}: {}{waited}",
                            message
                                .trim()
                                .split_once(": ")
                                .map_or(message.trim(), |(_, text)| text)
                        )),
                        Err(error) => problems.push(format!("{run} release failed: {error}")),
                    }
                }
                let (daemon, api) =
                    Daemon::start(self.server, self.prepared, &self.root.bearer()).await?;
                self.daemon = Some(daemon);
                self.api = api;
                Ok(verdict(problems, details.join("; ")))
            }
        }
    }

    /// Waits until the run is running and `delay_ms` has passed; returns its state instead when it is no longer running.
    async fn running_for(&self, run_id: &str, delay_ms: u64) -> Result<Option<String>, String> {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let state = self.state_of(run_id).await?;
            if state == "running" {
                break;
            }
            if TERMINAL.contains(&state.as_str()) || Instant::now() > deadline {
                return Ok(Some(state));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        let state = self.state_of(run_id).await?;
        Ok((state != "running").then_some(state))
    }

    fn describe_session(&self, session: &str) -> Result<String, String> {
        let database = self.root.database();
        let native = state::native_id(&database, session)?.is_some();
        Ok(match state::session_state(&database, session)? {
            Some(stored) => format!(
                "session cleanup {}, disposition {}, reserved {}, native ID {}",
                stored.cleanup,
                stored.disposition,
                stored.reserved,
                if native { "recorded" } else { "absent" }
            ),
            None => "session record not found".into(),
        })
    }

    /// Runs the server's offline quarantine command against the private configuration copy.
    fn quarantine(&self, args: &[&str]) -> Result<String, String> {
        let output = std::process::Command::new(self.server)
            .arg("--config")
            .arg(&self.prepared.config_path)
            .arg("quarantine")
            .args(args)
            .stdin(std::process::Stdio::null())
            .output()
            .map_err(|error| format!("quarantine {}: {error}", args.join(" ")))?;
        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).into_owned())
        } else {
            Err(String::from_utf8_lossy(&output.stderr).trim().to_owned())
        }
    }

    async fn state_of(&self, run_id: &str) -> Result<String, String> {
        let reply = self
            .api
            .call(Via::Rest, "runs_get", &json!({"run_id":run_id}))
            .await?;
        if let Some(code) = reply.error {
            return Err(format!("runs_get {run_id}: {code}"));
        }
        Ok(reply.body["state"].as_str().unwrap_or_default().to_owned())
    }

    /// Polls until the run is terminal and returns its state, output, error, validation, event count and duration.
    async fn wait_terminal(&self, run_id: &str, timeout_ms: u64) -> Result<Value, String> {
        let started = Instant::now();
        let deadline = started + Duration::from_millis(timeout_ms);
        let state = loop {
            let state = self.state_of(run_id).await?;
            if TERMINAL.contains(&state.as_str()) {
                break state;
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "run {run_id} is still {state} after {}s; it was not cancelled",
                    timeout_ms / 1000
                ));
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        };
        let seconds = started.elapsed().as_secs();
        let reply = self
            .api
            .call(Via::Rest, "runs_result", &json!({"run_id":run_id}))
            .await?;
        let data: Value = reply.body["data"]
            .as_str()
            .and_then(|data| serde_json::from_str(data).ok())
            .unwrap_or(Value::Null);
        let outcome = &data["details"]["outcome"];
        let events = self.count_events(run_id).await?;
        Ok(json!({
            "state": state,
            "seconds": seconds,
            "events": events,
            "output": outcome["output"],
            "error": outcome["error"].as_str().map(str::to_owned).or_else(|| data["details"]["error"].as_str().map(str::to_owned)),
            "validation": data["details"]["validation"]["status"],
            "warnings": data["details"]["warnings"]
                .as_array()
                .map(|items| items.iter().map(|item| item["code"].clone()).collect::<Vec<_>>())
                .unwrap_or_default(),
        }))
    }

    async fn count_events(&self, run_id: &str) -> Result<usize, String> {
        let mut after: Option<i64> = None;
        let mut total = 0;
        loop {
            let mut args = json!({"run_id":run_id,"limit":self.root.page_size()});
            if let Some(after) = after {
                args["after"] = json!(after);
            }
            let reply = self.api.call(Via::Rest, "runs_events", &args).await?;
            if let Some(code) = reply.error {
                return Err(format!("runs_events {run_id}: {code}"));
            }
            let page = reply.body["events"].as_array().map_or(0, Vec::len);
            total += page;
            let next = reply.body["next_after"].as_i64();
            if page == 0 || next == after {
                return Ok(total);
            }
            after = next;
        }
    }

    async fn wait_inbox(&self, runs: &[String], timeout_ms: u64) -> Result<Vec<Value>, String> {
        let deadline = Instant::now() + Duration::from_millis(timeout_ms);
        loop {
            let reply = self
                .api
                .call(
                    Via::Rest,
                    "inbox_list",
                    &json!({"limit":self.root.page_size()}),
                )
                .await?;
            if let Some(code) = reply.error {
                return Err(format!("inbox_list: {code}"));
            }
            let items: Vec<Value> = reply.body["items"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|item| {
                    runs.iter()
                        .any(|run| item["notification"]["run_id"] == run.as_str())
                })
                .cloned()
                .collect();
            if items.len() >= runs.len() {
                return Ok(items);
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "found {} of {} inbox notification(s) before the timeout",
                    items.len(),
                    runs.len()
                ));
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
}

fn step_label(step: &Step) -> String {
    match step {
        Step::Start { id, .. } => format!("start {id}"),
        Step::Wait { run, .. } => format!("wait {run}"),
        Step::CancelQueued { run, .. } => format!("cancel_queued {run}"),
        Step::Call { tool, .. } => format!("call {tool}"),
        Step::NativeId { save, .. } => format!("native_id {save}"),
        Step::Inbox { .. } => "inbox".into(),
        Step::Restart {} => "restart".into(),
        Step::CancelRunning { run, .. } => format!("cancel_running {run}"),
        Step::Crash { run, .. } => format!("crash {run}"),
        Step::Release { .. } => "release".into(),
    }
}

fn check_error(expect: &Expect, actual: Option<&str>) -> Vec<String> {
    match (expect.error.as_deref(), actual) {
        (None, None) => Vec::new(),
        (Some(expected), Some(actual)) if expected == actual => Vec::new(),
        (Some(expected), actual) => vec![format!(
            "expected error {expected}, got {}",
            actual.unwrap_or("success")
        )],
        (None, Some(actual)) => vec![format!("unexpected error {actual}")],
    }
}

fn verdict(problems: Vec<String>, summary: String) -> (Outcome, String) {
    if problems.is_empty() {
        (Outcome::Ok, summary)
    } else {
        (
            Outcome::Fail,
            format!("{} | {summary}", problems.join("; ")),
        )
    }
}

fn short(value: &Value) -> String {
    let text = value.as_str().unwrap_or("-");
    text.chars().take(12).collect()
}

fn excerpt(text: &str) -> String {
    let flat: String = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if flat.chars().count() > 80 {
        format!("{}...", flat.chars().take(80).collect::<String>())
    } else {
        flat
    }
}

/// Prints the final table and returns whether every step passed or was skipped.
pub fn print_summary(records: &[Record]) -> bool {
    println!("\n{:<28} {:<8} {:<5} result", "scenario", "cli", "steps");
    let mut keys: Vec<(String, String)> = Vec::new();
    for record in records {
        let key = (record.scenario.clone(), record.cli.clone());
        if !keys.contains(&key) {
            keys.push(key);
        }
    }
    let mut clean = true;
    for (scenario, cli) in keys {
        let group: Vec<&Record> = records
            .iter()
            .filter(|record| record.scenario == scenario && record.cli == cli)
            .collect();
        let count = |outcome: Outcome| {
            group
                .iter()
                .filter(|record| record.outcome == outcome)
                .count()
        };
        let (fail, error, skip) = (
            count(Outcome::Fail),
            count(Outcome::Error),
            count(Outcome::Skip),
        );
        let result = if fail + error == 0 {
            if skip == 0 {
                "OK".to_owned()
            } else {
                format!("OK ({skip} skipped)")
            }
        } else {
            clean = false;
            format!("{fail} failed, {error} error(s), {skip} skipped")
        };
        println!("{scenario:<28} {cli:<8} {:<5} {result}", group.len());
    }
    clean
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shipped() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/live_client/scenarios")
    }

    #[test]
    fn shipped_scenarios_load_and_stay_within_the_request_budget() {
        let paths = resolve_paths(&["all".into()], &shipped()).unwrap();
        assert!(!paths.is_empty());
        let mut total = 0;
        for path in paths {
            let scenario = Scenario::load(&path).unwrap();
            assert!(!scenario.steps.is_empty(), "{}", path.display());
            total += scenario.provider_requests();
        }
        // The bound counts the queued run, which reaches a provider only when its cancellation is skipped.
        assert!(
            total <= 11,
            "one pass must stay within the approved provider request budget per CLI, got {total}"
        );
    }

    #[test]
    fn recovery_scenarios_release_every_interruption_within_their_budget() {
        let directory = shipped();
        let paths = resolve_paths(
            &[
                "recovery/cancel_new_session".into(),
                "recovery/cancel_and_crash_resume".into(),
            ],
            &directory,
        )
        .unwrap();
        let mut total = 0;
        for path in &paths {
            let scenario = Scenario::load(path).unwrap();
            assert!(scenario.interrupts(), "{}", path.display());
            total += scenario.provider_requests();
        }
        assert!(total <= 5, "recovery pass per CLI, got {total}");
        let all = resolve_paths(&["all".into()], &directory).unwrap();
        assert!(
            all.iter()
                .all(|path| !path.starts_with(directory.join("recovery")))
        );
    }

    #[test]
    fn an_interruption_without_a_later_release_is_rejected() {
        let text = r#"{"name":"x","description":"x","steps":[
            {"op":"start","id":"a","request":{}},
            {"op":"cancel_running","run":"a"}]}"#;
        let scenario: Scenario = serde_json::from_str(text).unwrap();
        assert!(scenario.validate().is_err());
        let text = r#"{"name":"x","description":"x","steps":[
            {"op":"start","id":"a","request":{}},
            {"op":"release","runs":["a"],"kinds":["bound_session","extra"]}]}"#;
        let scenario: Scenario = serde_json::from_str(text).unwrap();
        assert!(scenario.validate().is_err());
    }

    #[test]
    fn unknown_step_fields_are_rejected() {
        let text = r#"{"name":"x","description":"x","steps":[{"op":"wait","run":"a","expct":{}}]}"#;
        assert!(serde_json::from_str::<Scenario>(text).is_err());
    }

    #[test]
    fn references_keep_types_and_interpolate_strings() {
        let mut vars = Vars::default();
        vars.set("first", json!({"run_id":"r1","n":3}));
        vars.set("token", json!("T1"));
        let value = vars
            .resolve(&json!({"id":"${first.run_id}","n":"${first.n}","text":"say ${token} now","list":["${token}"]}))
            .unwrap();
        assert_eq!(
            value,
            json!({"id":"r1","n":3,"text":"say T1 now","list":["T1"]})
        );
        assert!(vars.resolve(&json!("${missing}")).is_err());
        assert!(vars.resolve(&json!("${first.absent}")).is_err());
    }
}
