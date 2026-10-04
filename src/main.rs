use std::{ffi::OsString, process::ExitCode};

use agent_runner_server::{
    application::ServiceError,
    maintenance::{Maintenance, Release},
    server::{ServerError, config::PreparedServer},
    storage::{Namespace, QuarantineKind, QuarantinedRun},
};
use tokio_util::sync::CancellationToken;

const USAGE: &str = "usage:
  agent-runner-server --config <path>
  agent-runner-server --config <path> quarantine list
  agent-runner-server --config <path> quarantine release <run_id> --confirm-no-cli-process [--owner <id> --client <id>]

quarantine commands need the daemon to be stopped; they take the same locks as the daemon.";

fn main() -> ExitCode {
    let args: Vec<OsString> = std::env::args_os().skip(1).collect();
    if args.first().map(OsString::as_os_str) != Some("--config".as_ref()) || args.len() < 2 {
        eprintln!("{USAGE}");
        return ExitCode::FAILURE;
    }
    let path = args[1].clone();
    let rest = &args[2..];
    if rest.is_empty() {
        let runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(error) => {
                eprintln!("{error}");
                return ExitCode::FAILURE;
            }
        };
        return match runtime.block_on(serve(path)) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("{}", describe(&error));
                ExitCode::FAILURE
            }
        };
    }
    let rest: Option<Vec<&str>> = rest.iter().map(|arg| arg.to_str()).collect();
    let Some(rest) = rest else {
        eprintln!("{USAGE}");
        return ExitCode::FAILURE;
    };
    match quarantine(path, &rest) {
        Ok(code) => code,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

async fn serve(path: OsString) -> Result<(), ServerError> {
    let prepared = PreparedServer::load(path)?;
    let server = prepared.start().await?;
    eprintln!("listening on {}", server.local_addr()?);
    let stop = CancellationToken::new();
    let mut signal_task = tokio::spawn(wait_for_signal());
    let mut server_task = tokio::spawn(server.run(stop.clone()));
    tokio::select! {
        result = &mut server_task => {
            signal_task.abort();
            result.map_err(|_| ServerError::Transport)?
        }
        signal = &mut signal_task => {
            stop.cancel();
            let result = server_task.await.map_err(|_| ServerError::Transport)?;
            signal.map_err(|_| ServerError::Transport)?.map_err(|_| ServerError::Transport)?;
            result
        }
    }
}

fn describe(error: &ServerError) -> String {
    match error {
        ServerError::Service(ServiceError::AlreadyRunning) => {
            "the database is in use; stop the daemon first".into()
        }
        ServerError::Service(inner) => format!("{error}: {inner}"),
        _ => error.to_string(),
    }
}

fn quarantine(path: OsString, args: &[&str]) -> Result<ExitCode, String> {
    match args {
        ["quarantine", "list"] => {
            let maintenance = open(path)?;
            let entries = maintenance
                .quarantined()
                .map_err(|error| error.to_string())?;
            if entries.is_empty() {
                println!("no quarantined runs");
            }
            for entry in &entries {
                print_entry(entry, maintenance.workspace_directory(entry));
            }
            Ok(ExitCode::SUCCESS)
        }
        ["quarantine", "release", run_id, options @ ..] => {
            let mut confirmed = false;
            let mut owner = None;
            let mut client = None;
            let mut options = options.iter();
            while let Some(option) = options.next() {
                match *option {
                    "--confirm-no-cli-process" => confirmed = true,
                    "--owner" => owner = options.next().copied(),
                    "--client" => client = options.next().copied(),
                    _ => return Err(USAGE.into()),
                }
            }
            if !confirmed {
                return Err("release needs --confirm-no-cli-process after you have checked that no CLI process from the run remains".into());
            }
            let mut maintenance = open(path)?;
            let entries = maintenance
                .quarantined()
                .map_err(|error| error.to_string())?;
            let matches: Vec<&QuarantinedRun> = entries
                .iter()
                .filter(|entry| {
                    entry.run_id == *run_id
                        && owner.is_none_or(|owner| entry.namespace.owner_id == owner)
                        && client.is_none_or(|client| entry.namespace.client_id == client)
                })
                .collect();
            let entry = match matches.as_slice() {
                [] => return Err(format!("{run_id} is not a quarantined run")),
                [entry] => *entry,
                _ => {
                    return Err(format!(
                        "{run_id} exists in several namespaces; pass --owner and --client"
                    ));
                }
            };
            let namespace: Namespace = entry.namespace.clone();
            let release = maintenance
                .release(&namespace, run_id)
                .map_err(|error| format!("{run_id}: {error}"))?;
            println!(
                "{run_id}: {}",
                match release {
                    Release::Resumable => "released; the session can be resumed again",
                    Release::Abandoned =>
                        "released; the session was abandoned and cannot be resumed",
                    Release::Unstarted =>
                        "released; the workspace and the unstarted session are free",
                }
            );
            Ok(ExitCode::SUCCESS)
        }
        _ => Err(USAGE.into()),
    }
}

fn open(path: OsString) -> Result<Maintenance, String> {
    PreparedServer::load(path)
        .and_then(PreparedServer::maintenance)
        .map_err(|error| describe(&error))
}

fn print_entry(entry: &QuarantinedRun, directory: Option<std::path::PathBuf>) {
    let (blocks, release) = match entry.kind {
        QuarantineKind::UnboundSession => (
            "every new run and resume in this state realm",
            "abandons the session; it can never be resumed",
        ),
        QuarantineKind::BoundSession => (
            "new sessions in this state realm and this session",
            "makes the session resumable again",
        ),
        QuarantineKind::UnstartedWorkspace => (
            "this workspace and this session",
            "frees the workspace and the unstarted session",
        ),
        QuarantineKind::Unrecognized => ("unknown", "not supported; it stays quarantined"),
    };
    println!(
        "{}  {}/{}  {}{}  {}",
        entry.run_id,
        entry.namespace.owner_id,
        entry.namespace.client_id,
        entry.state,
        entry
            .stop_cause
            .as_deref()
            .map(|cause| format!(" ({cause})"))
            .unwrap_or_default(),
        entry.kind.as_str()
    );
    println!(
        "  runtime {}, realm {}, session {}, native ID {}",
        entry.runtime,
        entry.state_realm.as_deref().unwrap_or("-"),
        entry.session_id.as_deref().unwrap_or("-"),
        entry.native_id.as_deref().unwrap_or("not recorded")
    );
    println!(
        "  workspace {}",
        match (&entry.workspace_key, directory) {
            (None, _) => "-".to_owned(),
            (Some(_), Some(directory)) => directory.display().to_string(),
            (Some(key), None) => format!("{key} (not in the current configuration)"),
        }
    );
    println!("  blocks: {blocks}");
    println!("  release: {release}");
}

async fn wait_for_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            event = term.recv() => event.map(|_| ()).ok_or_else(|| std::io::Error::other("termination signal stream ended")),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await
    }
}
