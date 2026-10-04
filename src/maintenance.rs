//! Offline operator maintenance for terminal runs that still quarantine sessions or workspaces.
//!
//! Maintenance takes the same database, workspace and native state owners as the daemon, so it refuses to open while the daemon runs.

use std::{fs::File, path::Path};

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
            _guard: guard,
        })
    }

    /// Lists terminal runs that still hold a session reservation or a workspace.
    pub fn quarantined(&self) -> Result<Vec<QuarantinedRun>, ServiceError> {
        self.store.quarantined_runs().map_err(Into::into)
    }

    /// Releases one quarantined run after the operator confirmed that no process from it remains.
    /// The release matches the recorded shape; unrecognized shapes stay quarantined.
    pub fn release(
        &mut self,
        namespace: &Namespace,
        run_id: &str,
    ) -> Result<Release, ServiceError> {
        let entry = self
            .quarantined()?
            .into_iter()
            .find(|entry| &entry.namespace == namespace && entry.run_id == run_id)
            .ok_or(ServiceError::Storage(StorageError::NotFound))?;
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
            QuarantineKind::Unrecognized => {
                return Err(ServiceError::Storage(StorageError::InvalidState));
            }
        };
        if !released {
            return Err(ServiceError::Storage(StorageError::InvalidState));
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
        if cwd.starts_with(path) {
            let name = std::fs::read_to_string(entry.path().join("comm"))
                .map(|name| name.trim().to_owned())
                .unwrap_or_default();
            found.push((pid, name));
        }
    }
    found.sort();
    Ok(found)
}
