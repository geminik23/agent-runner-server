//! Offline operator maintenance for terminal runs that still quarantine sessions or workspaces.
//!
//! Maintenance takes the same database, workspace and native state owners as the daemon, so it refuses to open while the daemon runs.

use std::{
    fmt,
    fs::File,
    path::{Path, PathBuf},
};

use crate::{
    application::{BackendProfile, ServiceError, open_owned_store, owned_states},
    core::{CleanupState, SessionDisposition},
    storage::{
        Namespace, QuarantineKind, QuarantinedRun, SqliteStore, StorageError, service::ServiceClock,
    },
    workspace::{WorkspaceConfig, WorkspaceOwnership, WorkspaceRegistry},
};

/// Holds the daemon's exclusive locks while an operator inspects or releases quarantined runs.
pub struct Maintenance {
    store: SqliteStore,
    owner: Option<WorkspaceOwnership>,
    registry: Option<WorkspaceRegistry>,
    _guard: File,
}

/// Reports which release was applied to a quarantined run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Release {
    /// The bound session became resumable again.
    Resumable,
    /// The unbound session was given up and can never be resumed.
    Abandoned,
    /// The unstarted workspace and its session were released.
    Unstarted,
}

/// Explains why a release was refused; the run stays quarantined in every case.
#[derive(Debug)]
pub enum ReleaseError {
    /// The run is not a terminal run that still holds a session or workspace.
    NotQuarantined,
    /// The protection has a shape that maintenance does not release.
    Unsupported,
    /// The run's workspace is not in the current configuration, so remaining processes cannot be checked.
    WorkspaceUnknown,
    /// Processes still work inside the run's workspace, as `(pid, command name)`.
    ProcessesRemain(PathBuf, Vec<(u32, String)>),
    Service(ServiceError),
}

impl fmt::Display for ReleaseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotQuarantined => formatter.write_str("the run is not quarantined"),
            Self::Unsupported => {
                formatter.write_str("maintenance does not release this shape; it stays quarantined")
            }
            Self::WorkspaceUnknown => formatter.write_str(
                "the run's workspace is not configured, so remaining processes cannot be checked",
            ),
            Self::ProcessesRemain(directory, found) => {
                let list: Vec<String> = found
                    .iter()
                    .map(|(pid, name)| format!("{pid} ({name})"))
                    .collect();
                write!(
                    formatter,
                    "processes still work in {}: {}; stop them or wait until they exit",
                    directory.display(),
                    list.join(", ")
                )
            }
            Self::Service(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for ReleaseError {}

impl From<ServiceError> for ReleaseError {
    fn from(error: ServiceError) -> Self {
        Self::Service(error)
    }
}

impl From<StorageError> for ReleaseError {
    fn from(error: StorageError) -> Self {
        Self::Service(error.into())
    }
}

impl Maintenance {
    /// Opens the database exactly as the daemon would, including journal reconciliation and run recovery.
    /// Recovery records interrupted runs left by a stopped or crashed daemon so they appear as quarantined.
    pub fn open(
        database: &Path,
        profiles: &[BackendProfile],
        workspaces: Option<WorkspaceConfig>,
    ) -> Result<Self, ServiceError> {
        let registry = workspaces
            .map(|config| WorkspaceRegistry::prepare(config, Path::new(".")))
            .transpose()?;
        let states = owned_states(profiles)?;
        let (mut store, guard, owner) = open_owned_store(database, registry.as_ref(), &states)?;
        store.service_recover(ServiceClock::Wall)?;
        Ok(Self {
            store,
            owner,
            registry,
            _guard: guard,
        })
    }

    /// Lists terminal runs that still hold a session reservation or a workspace.
    pub fn quarantined(&self) -> Result<Vec<QuarantinedRun>, ServiceError> {
        self.store.quarantined_runs().map_err(Into::into)
    }

    /// Resolves the visible directory of a quarantined run's workspace from the current configuration.
    /// The stored workspace key names the resource below its mount root and is not a path to open.
    pub fn workspace_directory(&self, entry: &QuarantinedRun) -> Option<PathBuf> {
        let plan = self
            .registry
            .as_ref()?
            .get(entry.workspace_id.as_deref()?)?;
        plan.resource_path(entry.session_id.as_deref()?).ok()
    }

    /// Releases one quarantined run after the operator confirmed that no process from it remains.
    /// It refuses while any process works inside the run's workspace; a process that moved elsewhere is not found.
    /// The release matches the recorded shape; unrecognized shapes stay quarantined.
    pub fn release(
        &mut self,
        namespace: &Namespace,
        run_id: &str,
    ) -> Result<Release, ReleaseError> {
        let entry = self
            .quarantined()?
            .into_iter()
            .find(|entry| &entry.namespace == namespace && entry.run_id == run_id)
            .ok_or(ReleaseError::NotQuarantined)?;
        if entry.workspace_key.is_some() {
            let directory = self
                .workspace_directory(&entry)
                .ok_or(ReleaseError::WorkspaceUnknown)?;
            #[cfg(target_os = "linux")]
            {
                let found = processes_in(&directory).map_err(|_| ServiceError::Unavailable)?;
                if !found.is_empty() {
                    return Err(ReleaseError::ProcessesRemain(directory, found));
                }
            }
        }
        let (released, outcome) = match entry.kind {
            QuarantineKind::BoundSession => (
                self.store.settle_session(
                    namespace,
                    run_id,
                    CleanupState::Settled,
                    SessionDisposition::Reusable,
                )?,
                Release::Resumable,
            ),
            QuarantineKind::UnboundSession => (
                self.store.abandon_unbound_session(namespace, run_id)?,
                Release::Abandoned,
            ),
            QuarantineKind::UnstartedWorkspace => (
                self.store.settle_unstarted_workspace(namespace, run_id)?,
                Release::Unstarted,
            ),
            QuarantineKind::Unrecognized => return Err(ReleaseError::Unsupported),
        };
        if !released {
            return Err(ReleaseError::Unsupported);
        }
        // Settle the journal entries now instead of leaving them for the next daemon start.
        if let Some(owner) = &mut self.owner {
            owner.reconcile(&mut self.store)?;
        }
        Ok(outcome)
    }
}

/// Lists processes whose working directory is inside `path`, as `(pid, command name)`.
/// A process that changed directory or detached elsewhere is not found, so an empty list is not proof that none remain.
#[cfg(target_os = "linux")]
pub fn processes_in(path: &Path) -> std::io::Result<Vec<(u32, String)>> {
    // The kernel reports canonical working directories, so compare against the canonical workspace path.
    // A directory that does not exist, such as a managed one never created, cannot hold a working directory.
    let path = match std::fs::canonicalize(path) {
        Ok(path) => path,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut found = Vec::new();
    for entry in std::fs::read_dir("/proc")? {
        let Ok(entry) = entry else { continue };
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        // Processes of other users or ones that exit during the scan cannot be inspected and are skipped.
        let Ok(cwd) = std::fs::read_link(entry.path().join("cwd")) else {
            continue;
        };
        if cwd.starts_with(&path) {
            let name = std::fs::read_to_string(entry.path().join("comm"))
                .map(|name| name.trim().to_owned())
                .unwrap_or_default();
            found.push((pid, name));
        }
    }
    found.sort();
    Ok(found)
}
