use std::process::ExitCode;

use agent_runner_server::server::{ServerError, config::PreparedServer};
use tokio_util::sync::CancellationToken;

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), ServerError> {
    let mut args = std::env::args_os().skip(1);
    if args.next().as_deref() != Some(std::ffi::OsStr::new("--config")) {
        return Err(ServerError::InvalidConfiguration);
    }
    let path = args.next().ok_or(ServerError::InvalidConfiguration)?;
    if args.next().is_some() {
        return Err(ServerError::InvalidConfiguration);
    }
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
