//! Development client that drives a real daemon over REST and MCP from scenario files.
//!
//! It reuses a retained live directory whose database already owns the real CLI state, so it never creates a new owner.
//! Run `cargo build --bins` first, then `cargo run --example live_client -- help`.

#[cfg(target_os = "linux")]
mod api;
#[cfg(target_os = "linux")]
mod daemon;
#[cfg(target_os = "linux")]
mod scenario;
#[cfg(target_os = "linux")]
mod state;

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("live_client needs Linux because CLI profiles need pidfd and a subreaper");
    std::process::exit(2);
}

#[cfg(target_os = "linux")]
fn main() {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("error: tokio runtime: {error}");
            std::process::exit(2);
        }
    };
    match runtime.block_on(linux::main(arguments)) {
        Ok(code) => std::process::exit(code),
        Err(error) => {
            eprintln!("error: {error}");
            std::process::exit(2);
        }
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use std::{
        collections::BTreeMap,
        io::Write,
        path::{Path, PathBuf},
    };

    use serde_json::{Value, json};

    use crate::{
        api::{Api, Via},
        daemon::{self, Daemon, LaunchOptions, Prepared, Root},
        scenario::{self, Runner, Scenario},
        state,
    };

    const USAGE: &str = "\
usage: live_client <command> [options] [arguments]

commands:
  plan  [--cli LIST] SCENARIO...        show steps and the provider request upper bound; starts nothing
  doctor [--root DIR]                   read-only database check
  run   [--root DIR] [--cli LIST] [--yes] [--allow-interruption] [launch options] SCENARIO...
                                        start the daemon, run scenarios, stop it once no run is active
  serve [--root DIR] [launch options]   start the daemon in the foreground for manual calls until Ctrl-C
  call  [--root DIR] [--via rest|mcp] [--allow-running-cancel] TOOL [ARGS_JSON]
                                        call one tool on the daemon started by `serve`

SCENARIO is a shipped name (see examples/live_client/scenarios), a path, or `all`.
`all` excludes the recovery scenarios under scenarios/recovery, which interrupt launched CLIs and release the
resulting quarantine with the server's offline maintenance command; they need --allow-interruption.
--root defaults to AGENT_RUNNER_LIVE_EXISTING_DIR. --cli is a comma list of CLI kinds (default: every CLI profile).

launch options:
  --accept-installed-version   use the installed CLI version in the temporary configuration copy only
  --accept-any-version         set version.accept_any in the temporary configuration copy only
  --program KIND=PATH          CLI executable instead of the PATH lookup
  --server PATH, --helper PATH binaries (default: next to this example in the target directory)
  --profiles DIR               reviewed CLI profiles (default: examples/cli-profiles)
  --overlay PATH               configuration overlay (default: examples/live_client/overlay.json)
";

    /// Parsed command line.
    #[derive(Default)]
    struct Options {
        root: Option<PathBuf>,
        cli: Option<Vec<String>>,
        yes: bool,
        accept_installed_version: bool,
        accept_any_version: bool,
        allow_running_cancel: bool,
        allow_interruption: bool,
        programs: BTreeMap<String, PathBuf>,
        server: Option<PathBuf>,
        helper: Option<PathBuf>,
        profiles: Option<PathBuf>,
        overlay: Option<PathBuf>,
        via: Via,
        positional: Vec<String>,
    }

    fn parse(arguments: &[String]) -> Result<Options, String> {
        let mut options = Options::default();
        let mut iter = arguments.iter();
        while let Some(argument) = iter.next() {
            let mut value = |name: &str| {
                iter.next()
                    .cloned()
                    .ok_or_else(|| format!("{name} needs a value"))
            };
            match argument.as_str() {
                "--root" => options.root = Some(value("--root")?.into()),
                "--cli" => {
                    options.cli = Some(
                        value("--cli")?
                            .split(',')
                            .filter(|kind| !kind.is_empty())
                            .map(str::to_owned)
                            .collect(),
                    );
                }
                "--yes" => options.yes = true,
                "--accept-installed-version" => options.accept_installed_version = true,
                "--accept-any-version" => options.accept_any_version = true,
                "--allow-interruption" => options.allow_interruption = true,
                "--allow-running-cancel" => options.allow_running_cancel = true,
                "--program" => {
                    let pair = value("--program")?;
                    let (kind, path) = pair.split_once('=').ok_or("--program needs KIND=PATH")?;
                    options.programs.insert(kind.to_owned(), path.into());
                }
                "--server" => options.server = Some(value("--server")?.into()),
                "--helper" => options.helper = Some(value("--helper")?.into()),
                "--profiles" => options.profiles = Some(value("--profiles")?.into()),
                "--overlay" => options.overlay = Some(value("--overlay")?.into()),
                "--via" => options.via = Via::parse(&value("--via")?)?,
                flag if flag.starts_with("--") => return Err(format!("unknown option {flag}")),
                _ => options.positional.push(argument.clone()),
            }
        }
        Ok(options)
    }

    fn manifest() -> &'static Path {
        Path::new(env!("CARGO_MANIFEST_DIR"))
    }

    fn scenario_directory() -> PathBuf {
        manifest().join("examples/live_client/scenarios")
    }

    fn open_root(options: &Options) -> Result<Root, String> {
        let dir = match &options.root {
            Some(dir) => dir.clone(),
            None => std::env::var_os("AGENT_RUNNER_LIVE_EXISTING_DIR")
                .map(PathBuf::from)
                .ok_or("pass --root or set AGENT_RUNNER_LIVE_EXISTING_DIR")?,
        };
        Root::open(&dir)
    }

    /// Locates a sibling binary of the target directory that holds this example.
    fn sibling_binary(name: &str) -> Result<PathBuf, String> {
        let exe =
            std::env::current_exe().map_err(|error| format!("current executable: {error}"))?;
        let directory = exe
            .parent()
            .and_then(Path::parent)
            .ok_or("cannot locate the target directory")?;
        let path = directory.join(name);
        if path.is_file() {
            Ok(path)
        } else {
            Err(format!(
                "{} not found; run `cargo build --bins` first or pass --server/--helper",
                path.display()
            ))
        }
    }

    fn launch_options(options: &Options) -> Result<LaunchOptions, String> {
        Ok(LaunchOptions {
            server: match &options.server {
                Some(path) => path.clone(),
                None => sibling_binary("agent-runner-server")?,
            },
            helper: match &options.helper {
                Some(path) => path.clone(),
                None => sibling_binary("agent-runner-cli-helper")?,
            },
            profiles: options
                .profiles
                .clone()
                .unwrap_or_else(|| manifest().join("examples/cli-profiles")),
            overlay: options
                .overlay
                .clone()
                .unwrap_or_else(|| manifest().join("examples/live_client/overlay.json")),
            programs: options.programs.clone(),
            accept_installed_version: options.accept_installed_version,
            accept_any_version: options.accept_any_version,
        })
    }

    pub async fn main(arguments: Vec<String>) -> Result<i32, String> {
        let Some((command, rest)) = arguments.split_first() else {
            print!("{USAGE}");
            return Ok(2);
        };
        let options = parse(rest)?;
        match command.as_str() {
            "plan" => plan(&options),
            "doctor" => doctor(&options),
            "run" => run(&options).await,
            "serve" => serve(&options).await,
            "call" => call(&options).await,
            "help" | "--help" | "-h" => {
                print!("{USAGE}");
                Ok(0)
            }
            other => Err(format!("unknown command `{other}`\n\n{USAGE}")),
        }
    }

    fn load_scenarios(options: &Options) -> Result<Vec<Scenario>, String> {
        scenario::resolve_paths(&options.positional, &scenario_directory())?
            .iter()
            .map(|path| Scenario::load(path))
            .collect()
    }

    /// Prints scenarios and returns the provider request upper bound for `targets` CLIs.
    fn print_plan(scenarios: &[Scenario], targets: &[String]) -> usize {
        let mut total = 0;
        for scenario in scenarios {
            let requests = scenario.provider_requests();
            total += requests * targets.len();
            println!(
                "{:<28} {} step(s), up to {requests} provider request(s) per CLI - {}",
                scenario.name,
                scenario.steps.len(),
                scenario.description
            );
        }
        println!(
            "targets: {}; provider requests: up to {total} in total",
            targets.join(", ")
        );
        total
    }

    fn plan(options: &Options) -> Result<i32, String> {
        let scenarios = load_scenarios(options)?;
        let targets = match (&options.cli, open_root(options)) {
            (Some(list), _) => list.clone(),
            (None, Ok(root)) => root
                .targets()?
                .into_iter()
                .map(|target| target.cli)
                .collect(),
            (None, Err(_)) => vec!["<each CLI profile>".into()],
        };
        print_plan(&scenarios, &targets);
        Ok(0)
    }

    fn doctor(options: &Options) -> Result<i32, String> {
        let root = open_root(options)?;
        let health = state::health(&root.database())?;
        println!("{}: {}", root.database().display(), health.describe());
        if health.is_safe() {
            println!("safe: no active run, uncertain session or held workspace");
            Ok(0)
        } else {
            println!(
                "unsafe: live work must not start; an uncertain session blocks new sessions of its CLI until an operator settles it"
            );
            Ok(1)
        }
    }

    fn check_before_start(root: &Root) -> Result<(), String> {
        let health = state::health(&root.database())?;
        println!("before: {}", health.describe());
        if health.is_safe() {
            Ok(())
        } else {
            Err(
                "the database is not idle and clean; run `doctor` and resolve it before live work"
                    .into(),
            )
        }
    }

    async fn run(options: &Options) -> Result<i32, String> {
        let root = open_root(options)?;
        let scenarios = load_scenarios(options)?;
        let all = root.targets()?;
        let targets: Vec<_> = match &options.cli {
            Some(list) => {
                for kind in list {
                    if !all.iter().any(|target| &target.cli == kind) {
                        return Err(format!(
                            "no CLI profile of kind `{kind}` in the live configuration"
                        ));
                    }
                }
                all.into_iter()
                    .filter(|target| list.contains(&target.cli))
                    .collect()
            }
            None => all,
        };
        let names: Vec<String> = targets.iter().map(|target| target.cli.clone()).collect();
        let total = print_plan(&scenarios, &names);
        let interrupting: Vec<&str> = scenarios
            .iter()
            .filter(|scenario| scenario.interrupts())
            .map(|scenario| scenario.name.as_str())
            .collect();
        if !interrupting.is_empty() && !options.allow_interruption {
            return Err(format!(
                "{} interrupt launched CLIs and release the quarantine offline; pass --allow-interruption",
                interrupting.join(", ")
            ));
        }
        if !options.yes {
            println!("nothing was started; pass --yes to send up to {total} provider request(s)");
            return Ok(0);
        }
        check_before_start(&root)?;
        let launch = launch_options(options)?;
        let suffix = daemon::random_hex()?;
        let mut prepared = Prepared::create(&root, &launch, &suffix)?;
        let report_path = root.dir.join(format!("live-client-{suffix}.jsonl"));
        let log = daemon::create_private(&report_path)?;
        let (started, api) = Daemon::start(&launch.server, &mut prepared, &root.bearer()).await?;
        println!(
            "daemon pid {} at {}",
            started.pid().unwrap_or_default(),
            started.url
        );
        let mut runner = Runner {
            root: &root,
            prepared: &mut prepared,
            server: &launch.server,
            daemon: Some(started),
            api,
            records: Vec::new(),
            log,
            run_suffix: suffix.clone(),
        };
        let interrupted = tokio::select! {
            () = async {
                for scenario in &scenarios {
                    for target in &targets {
                        runner.run(scenario, target).await;
                    }
                }
            } => false,
            _ = tokio::signal::ctrl_c() => true,
        };
        if interrupted {
            println!("\ninterrupted; submitted runs are left to finish before the daemon stops");
        }
        let clean = scenario::print_summary(&runner.records);
        let daemon = runner.daemon.take();
        drop(runner);
        if let Some(daemon) = daemon {
            stop_with_ctrl_c(daemon, &root.database()).await?;
        }
        let health = state::health(&root.database())?;
        println!("after: {}", health.describe());
        println!(
            "report {}, daemon log {}",
            report_path.display(),
            prepared.log_path.display()
        );
        if !health.is_safe() {
            println!("warning: the database is no longer clean; see `doctor`");
            return Ok(1);
        }
        Ok(if clean && !interrupted { 0 } else { 1 })
    }

    /// Stops the daemon gracefully; a Ctrl-C while waiting leaves it running instead of interrupting a launched CLI.
    async fn stop_with_ctrl_c(daemon: Daemon, database: &Path) -> Result<(), String> {
        let pid = daemon.pid().unwrap_or_default();
        tokio::select! {
            result = daemon.stop(database) => result,
            _ = tokio::signal::ctrl_c() => Err(format!(
                "stop was interrupted; the daemon (pid {pid}) keeps running. Stop it with SIGTERM once no run is active"
            )),
        }
    }

    fn serve_state(root: &Root) -> PathBuf {
        root.dir.join("live-client-serve.json")
    }

    async fn serve(options: &Options) -> Result<i32, String> {
        let root = open_root(options)?;
        check_before_start(&root)?;
        let launch = launch_options(options)?;
        let suffix = daemon::random_hex()?;
        let mut prepared = Prepared::create(&root, &launch, &suffix)?;
        let state_path = serve_state(&root);
        let mut file = daemon::create_private(&state_path)
            .map_err(|error| format!("{error}; is another `serve` running?"))?;
        let started = match Daemon::start(&launch.server, &mut prepared, &root.bearer()).await {
            Ok((started, _)) => started,
            Err(error) => {
                let _ = std::fs::remove_file(&state_path);
                return Err(error);
            }
        };
        let record = json!({"url": started.url, "pid": started.pid()});
        if let Err(error) = writeln!(file, "{record}") {
            eprintln!("warning: could not write {}: {error}", state_path.display());
        }
        drop(file);
        println!(
            "daemon pid {} at {}",
            started.pid().unwrap_or_default(),
            started.url
        );
        println!(
            "token for curl: TOKEN=\"{}.$(cat {})\"",
            root.credential_id(),
            root.dir.join("secret").display()
        );
        println!(
            "use `live_client call TOOL ARGS_JSON` or curl; press Ctrl-C to stop once runs finish"
        );
        let _ = tokio::signal::ctrl_c().await;
        let _ = std::fs::remove_file(&state_path);
        println!("stopping; waiting for active runs first");
        let result = stop_with_ctrl_c(started, &root.database()).await;
        let health = state::health(&root.database())?;
        println!("after: {}", health.describe());
        result.map(|()| if health.is_safe() { 0 } else { 1 })
    }

    async fn call(options: &Options) -> Result<i32, String> {
        let root = open_root(options)?;
        let (tool, args) = match options.positional.as_slice() {
            [tool] => (tool.clone(), json!({})),
            [tool, args] => (
                tool.clone(),
                serde_json::from_str::<Value>(args)
                    .map_err(|error| format!("ARGS_JSON: {error}"))?,
            ),
            _ => return Err("call needs TOOL [ARGS_JSON]".into()),
        };
        let record: Value = serde_json::from_slice(
            &std::fs::read(serve_state(&root))
                .map_err(|_| "no daemon from `serve` is running for this live directory")?,
        )
        .map_err(|error| format!("serve state: {error}"))?;
        let url = record["url"]
            .as_str()
            .ok_or("serve state has no url")?
            .to_owned();
        let api = Api::new(url, root.bearer())?;
        if tool == "runs_cancel" && !options.allow_running_cancel {
            let reply = api.call(Via::Rest, "runs_get", &args).await?;
            if let Some(code) = reply.error {
                return Err(format!("cannot check the run before cancelling: {code}"));
            }
            let run_state = reply.body["state"].as_str().unwrap_or_default();
            if run_state != "queued" {
                return Err(format!(
                    "the run is {run_state}; cancelling a launched run isolates its session and blocks new sessions of that CLI in this database. Pass --allow-running-cancel only if that is intended"
                ));
            }
        }
        let reply = api.call(options.via, &tool, &args).await?;
        println!(
            "{} {} {}",
            options.via.name(),
            reply.status,
            reply.error.as_deref().unwrap_or("ok")
        );
        println!(
            "{}",
            serde_json::to_string_pretty(&reply.body).map_err(|error| error.to_string())?
        );
        Ok(if reply.error.is_some() { 1 } else { 0 })
    }
}
