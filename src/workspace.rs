//! Trusted workspace selection, stable directory identities, and runner ownership.

pub(crate) mod journal;

use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::application::ServiceError;

/// Selects the scheduling policy for a trusted workspace.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceAccess {
    /// Shared execution is available only to a backend with verified read-only capability.
    SharedRead,
    /// Conflicting workspaces are executed exclusively.
    Exclusive,
}

/// Selects a trusted existing directory or a root for managed session directories.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkspaceLocation {
    Existing { path: PathBuf },
    Managed { root: PathBuf, max_sessions: usize },
}

/// Registers one workspace under an operator-controlled identifier and revision.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceEntry {
    pub id: String,
    pub revision: String,
    pub access: WorkspaceAccess,
    pub location: WorkspaceLocation,
}

/// Configures workspace registration and the common runner ownership directory.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceConfig {
    pub ownership_directory: PathBuf,
    pub entries: Vec<WorkspaceEntry>,
}

impl WorkspaceConfig {
    pub(crate) fn resolve_relative(&mut self, root: &Path) {
        self.ownership_directory = root.join(&self.ownership_directory);
        for entry in &mut self.entries {
            match &mut entry.location {
                WorkspaceLocation::Existing { path } => *path = root.join(&*path),
                WorkspaceLocation::Managed { root: managed, .. } => *managed = root.join(&*managed),
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct Directory {
    pub path: PathBuf,
    pub resource_path: PathBuf,
    pub device: u64,
    pub inode: u64,
    pub ancestors: Vec<(u64, u64)>,
}

impl Directory {
    pub(crate) fn inspect(path: &Path) -> Result<Self, ServiceError> {
        #[cfg(not(target_os = "linux"))]
        {
            let _ = path;
            return Err(ServiceError::InvalidConfiguration);
        }
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::MetadataExt;
            let canonical =
                fs::canonicalize(path).map_err(|_| ServiceError::InvalidConfiguration)?;
            if !canonical.is_absolute()
                || canonical == Path::new("/")
                || canonical.to_str().is_none_or(|text| text.len() > 1024)
            {
                return Err(ServiceError::InvalidConfiguration);
            }
            let metadata =
                fs::metadata(&canonical).map_err(|_| ServiceError::InvalidConfiguration)?;
            if !metadata.is_dir() {
                return Err(ServiceError::InvalidConfiguration);
            }
            let resource_path = linux_resource_path(&canonical)?;
            let mut ancestors = Vec::new();
            for path in canonical.ancestors() {
                if ancestors.len() >= 128 {
                    return Err(ServiceError::InvalidConfiguration);
                }
                let ancestor =
                    fs::metadata(path).map_err(|_| ServiceError::InvalidConfiguration)?;
                if !ancestor.is_dir() {
                    return Err(ServiceError::InvalidConfiguration);
                }
                ancestors.push((ancestor.dev(), ancestor.ino()));
            }
            let current =
                fs::metadata(&canonical).map_err(|_| ServiceError::InvalidConfiguration)?;
            if (current.dev(), current.ino()) != (metadata.dev(), metadata.ino()) {
                return Err(ServiceError::InvalidConfiguration);
            }
            Ok(Self {
                path: canonical,
                resource_path,
                device: metadata.dev(),
                inode: metadata.ino(),
                ancestors,
            })
        }
    }

    pub(crate) fn unchanged(&self) -> bool {
        Self::inspect(&self.path).is_ok_and(|current| current == *self)
    }
}

#[cfg(target_os = "linux")]
fn linux_resource_path(path: &Path) -> Result<PathBuf, ServiceError> {
    linux_resource_path_with_policy(path, true)
}

#[cfg(target_os = "linux")]
fn linux_resource_path_with_policy(
    path: &Path,
    reject_nested: bool,
) -> Result<PathBuf, ServiceError> {
    let mut bytes = Vec::new();
    File::open("/proc/self/mountinfo")
        .map_err(|_| ServiceError::InvalidConfiguration)?
        .take(4 * 1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| ServiceError::InvalidConfiguration)?;
    if bytes.len() > 4 * 1024 * 1024 {
        return Err(ServiceError::InvalidConfiguration);
    }
    mount_resource_path_with_policy(path, &bytes, reject_nested)
}

#[cfg(all(test, target_os = "linux"))]
fn mount_resource_path(path: &Path, bytes: &[u8]) -> Result<PathBuf, ServiceError> {
    mount_resource_path_with_policy(path, bytes, true)
}

#[cfg(target_os = "linux")]
fn mount_resource_path_with_policy(
    path: &Path,
    bytes: &[u8],
    reject_nested: bool,
) -> Result<PathBuf, ServiceError> {
    use std::os::unix::ffi::OsStringExt;
    let mut enclosing: Option<(PathBuf, PathBuf)> = None;
    let mut points = std::collections::HashSet::new();
    for line in bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let fields: Vec<_> = line
            .split(|byte| *byte == b' ')
            .filter(|field| !field.is_empty())
            .collect();
        if fields.len() < 6 {
            return Err(ServiceError::InvalidConfiguration);
        }
        let root = PathBuf::from(std::ffi::OsString::from_vec(decode_mount_field(fields[3])?));
        let point = PathBuf::from(std::ffi::OsString::from_vec(decode_mount_field(fields[4])?));
        if !root.is_absolute() || !point.is_absolute() {
            return Err(ServiceError::InvalidConfiguration);
        }
        if !points.insert(point.clone())
            && (path.starts_with(&point) || (reject_nested && point.starts_with(path)))
        {
            return Err(ServiceError::InvalidConfiguration);
        }
        if reject_nested && point != path && point.starts_with(path) {
            return Err(ServiceError::InvalidConfiguration);
        }
        if path.starts_with(&point)
            && enclosing.as_ref().is_none_or(|(_, current)| {
                point.components().count() > current.components().count()
            })
        {
            enclosing = Some((root, point));
        }
    }
    let (root, point) = enclosing.ok_or(ServiceError::InvalidConfiguration)?;
    let relative = path
        .strip_prefix(point)
        .map_err(|_| ServiceError::InvalidConfiguration)?;
    let resource = root.join(relative);
    if resource.to_str().is_none_or(|text| text.len() > 2048) {
        return Err(ServiceError::InvalidConfiguration);
    }
    Ok(resource)
}

#[cfg(target_os = "linux")]
fn decode_mount_field(field: &[u8]) -> Result<Vec<u8>, ServiceError> {
    let mut decoded = Vec::with_capacity(field.len());
    let mut index = 0;
    while index < field.len() {
        if field[index] == b'\\' {
            let digits = field
                .get(index + 1..index + 4)
                .ok_or(ServiceError::InvalidConfiguration)?;
            if digits.iter().any(|digit| !(b'0'..=b'7').contains(digit)) {
                return Err(ServiceError::InvalidConfiguration);
            }
            let value = u16::from(digits[0] - b'0') * 64
                + u16::from(digits[1] - b'0') * 8
                + u16::from(digits[2] - b'0');
            decoded.push(u8::try_from(value).map_err(|_| ServiceError::InvalidConfiguration)?);
            index += 4;
        } else {
            decoded.push(field[index]);
            index += 1;
        }
    }
    Ok(decoded)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Location {
    Existing {
        directory: Directory,
    },
    Managed {
        root: Directory,
        max_sessions: usize,
    },
}

/// A pinned logical workspace; this value never contains a client-supplied path.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkspacePlan {
    pub id: String,
    pub revision: String,
    pub access: WorkspaceAccess,
    pub location: Location,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MaterializeError {
    NotCreated,
    Uncertain,
}

impl WorkspacePlan {
    pub(crate) fn resource_path(&self, session_id: &str) -> Result<PathBuf, ServiceError> {
        match &self.location {
            Location::Existing { directory } => Ok(directory.path.clone()),
            Location::Managed { root, .. } => {
                if session_id.is_empty()
                    || !session_id
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
                {
                    return Err(ServiceError::InvalidConfiguration);
                }
                Ok(root.path.join(session_id))
            }
        }
    }

    pub(crate) fn resource_ancestors(&self) -> (&[(u64, u64)], bool) {
        match &self.location {
            Location::Existing { directory } => (&directory.ancestors, true),
            Location::Managed { root, .. } => (&root.ancestors, false),
        }
    }

    pub(crate) fn resource_key(&self, session_id: &str) -> Result<String, ServiceError> {
        let (directory, relative) = match &self.location {
            Location::Existing { directory } => (directory, None),
            Location::Managed { root, .. } => {
                self.resource_path(session_id)?;
                (root, Some(session_id))
            }
        };
        let path = relative.map_or_else(
            || directory.resource_path.clone(),
            |id| directory.resource_path.join(id),
        );
        let text = path.to_str().ok_or(ServiceError::InvalidConfiguration)?;
        let text = if text == "/" { "" } else { text };
        Ok(format!("{:x}:{text}", directory.device))
    }

    pub(crate) fn managed_root(&self) -> Option<(&Path, usize)> {
        match &self.location {
            Location::Existing { .. } => None,
            Location::Managed { root, max_sessions } => Some((&root.path, *max_sessions)),
        }
    }

    pub(crate) fn managed_capacity_key(&self) -> Result<Option<(String, usize)>, ServiceError> {
        match &self.location {
            Location::Existing { .. } => Ok(None),
            Location::Managed { root, max_sessions } => {
                let path = root
                    .resource_path
                    .to_str()
                    .ok_or(ServiceError::InvalidConfiguration)?;
                Ok(Some((
                    format!("{:x}:{}", root.device, if path == "/" { "" } else { path }),
                    *max_sessions,
                )))
            }
        }
    }

    /// Creates a new managed directory only before the first binding is recorded.
    pub(crate) fn materialize(
        &self,
        session_id: &str,
        bound: Option<Directory>,
    ) -> Result<Directory, MaterializeError> {
        match (&self.location, bound) {
            (Location::Existing { directory }, None) if directory.unchanged() => {
                Ok(directory.clone())
            }
            (Location::Existing { directory }, Some(bound))
                if *directory == bound && bound.unchanged() =>
            {
                Ok(bound)
            }
            (Location::Managed { root, .. }, Some(bound))
                if root.unchanged()
                    && self.resource_path(session_id).ok().as_ref() == Some(&bound.path)
                    && bound.unchanged() =>
            {
                Ok(bound)
            }
            (Location::Managed { root, .. }, None) if root.unchanged() => {
                let path = self
                    .resource_path(session_id)
                    .map_err(|_| MaterializeError::NotCreated)?;
                create_managed_directory(root, session_id, &path)
            }
            _ => Err(MaterializeError::NotCreated),
        }
    }
}

#[cfg(target_os = "linux")]
fn create_managed_directory(
    root: &Directory,
    session_id: &str,
    path: &Path,
) -> Result<Directory, MaterializeError> {
    use std::os::{
        fd::{AsRawFd, FromRawFd},
        unix::fs::{MetadataExt, OpenOptionsExt},
    };
    let parent = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&root.path)
        .map_err(|_| MaterializeError::NotCreated)?;
    let metadata = parent
        .metadata()
        .map_err(|_| MaterializeError::NotCreated)?;
    if !metadata.is_dir() || (metadata.dev(), metadata.ino()) != (root.device, root.inode) {
        return Err(MaterializeError::NotCreated);
    }
    let name = std::ffi::CString::new(session_id).map_err(|_| MaterializeError::NotCreated)?;
    // SAFETY: The directory descriptor and NUL-terminated validated session name remain valid for mkdirat.
    let created = unsafe { libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), 0o700) };
    if created != 0 {
        return Err(match std::io::Error::last_os_error().kind() {
            std::io::ErrorKind::PermissionDenied
            | std::io::ErrorKind::AlreadyExists
            | std::io::ErrorKind::NotFound => MaterializeError::NotCreated,
            _ => MaterializeError::Uncertain,
        });
    }
    parent.sync_all().map_err(|_| MaterializeError::Uncertain)?;
    // SAFETY: openat uses a valid parent descriptor and validated name; successful descriptors are immediately owned by File.
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(MaterializeError::Uncertain);
    }
    // SAFETY: fd is a newly returned, uniquely owned descriptor.
    let child = unsafe { File::from_raw_fd(fd) };
    let child_metadata = child.metadata().map_err(|_| MaterializeError::Uncertain)?;
    let directory = Directory::inspect(path).map_err(|_| MaterializeError::Uncertain)?;
    if directory.path != path
        || (directory.device, directory.inode) != (child_metadata.dev(), child_metadata.ino())
    {
        return Err(MaterializeError::Uncertain);
    }
    Ok(directory)
}

#[cfg(not(target_os = "linux"))]
fn create_managed_directory(
    _root: &Directory,
    _session_id: &str,
    _path: &Path,
) -> Result<Directory, MaterializeError> {
    Err(MaterializeError::NotCreated)
}

/// A prepared registry of canonical paths controlled by the service operator.
#[derive(Clone)]
pub(crate) struct WorkspaceRegistry {
    ownership_directory: Directory,
    entries: HashMap<String, WorkspacePlan>,
}

impl WorkspaceRegistry {
    pub(crate) fn prepare(config: WorkspaceConfig, root: &Path) -> Result<Self, ServiceError> {
        if config.entries.is_empty() {
            return Err(ServiceError::InvalidConfiguration);
        }
        let ownership_directory = Directory::inspect(&root.join(config.ownership_directory))?;
        private_directory(&ownership_directory.path)?;
        let mut entries = HashMap::new();
        for entry in config.entries {
            if !valid_id(&entry.id) || !valid_id(&entry.revision) {
                return Err(ServiceError::InvalidConfiguration);
            }
            let location = match entry.location {
                WorkspaceLocation::Existing { path } => Location::Existing {
                    directory: Directory::inspect(&root.join(path))?,
                },
                WorkspaceLocation::Managed {
                    root: managed,
                    max_sessions,
                } => {
                    if max_sessions == 0 || max_sessions > 1_000_000 {
                        return Err(ServiceError::InvalidConfiguration);
                    }
                    let directory = Directory::inspect(&root.join(managed))?;
                    private_directory(&directory.path)?;
                    Location::Managed {
                        root: directory,
                        max_sessions,
                    }
                }
            };
            let plan = WorkspacePlan {
                id: entry.id.clone(),
                revision: entry.revision,
                access: entry.access,
                location,
            };
            if entries.insert(entry.id, plan).is_some() {
                return Err(ServiceError::InvalidConfiguration);
            }
        }
        if entries.values().any(|entry| {
            let directory = match &entry.location {
                Location::Existing { directory } => directory,
                Location::Managed { root, .. } => root,
            };
            directory.device == ownership_directory.device
                && (directory
                    .resource_path
                    .starts_with(&ownership_directory.resource_path)
                    || ownership_directory
                        .resource_path
                        .starts_with(&directory.resource_path))
        }) {
            return Err(ServiceError::InvalidConfiguration);
        }
        Ok(Self {
            ownership_directory,
            entries,
        })
    }

    pub(crate) fn get(&self, id: &str) -> Option<&WorkspacePlan> {
        self.entries.get(id)
    }

    /// Acquires persistent database ownership for the registry and each native state realm.
    pub(crate) fn acquire_owner(
        &self,
        database: &Path,
        states: &[(String, PathBuf, PathBuf)],
        store: &crate::storage::SqliteStore,
    ) -> Result<WorkspaceOwnership, ServiceError> {
        if !self.ownership_directory.unchanged() {
            return Err(ServiceError::InvalidConfiguration);
        }
        let database = DirectoryFile::inspect(database)?;
        let (guard, fresh) = lock_owner_file(
            &self.ownership_directory.path.join("runner-owner.json"),
            &serde_json::to_value(&database).map_err(|_| ServiceError::Unavailable)?,
        )?;
        let mut guards = vec![guard];
        let mut journals = vec![journal::Journal::open(
            &self.ownership_directory.path.join("runner-journal.jsonl"),
            fresh,
            &guards[0],
            store,
        )?];
        let mut realms = HashMap::new();
        let mut seen = HashMap::new();
        for (realm, path, owner_root) in states {
            let state = Directory::inspect(path)?;
            let owner_root =
                fs::canonicalize(owner_root).map_err(|_| ServiceError::InvalidConfiguration)?;
            if let Some((index, old_root)) = seen.get(&(state.device, state.inode)) {
                if old_root != &owner_root {
                    return Err(ServiceError::InvalidConfiguration);
                }
                realms.insert(realm.clone(), *index);
                continue;
            }
            #[cfg(target_os = "linux")]
            let home_resource = {
                use std::os::unix::fs::MetadataExt;
                let metadata =
                    fs::metadata(&owner_root).map_err(|_| ServiceError::InvalidConfiguration)?;
                if !metadata.is_dir() {
                    return Err(ServiceError::InvalidConfiguration);
                }
                (
                    metadata.dev(),
                    linux_resource_path_with_policy(&owner_root, false)?
                        .join(".agent-runner-server-realms"),
                )
            };
            #[cfg(not(target_os = "linux"))]
            return Err(ServiceError::InvalidConfiguration);
            if self.entries.values().any(|entry| {
                let directory = match &entry.location {
                    Location::Existing { directory } => directory,
                    Location::Managed { root, .. } => root,
                };
                directory.device == home_resource.0
                    && (home_resource.1.starts_with(&directory.resource_path)
                        || directory.resource_path.starts_with(&home_resource.1))
            }) {
                return Err(ServiceError::InvalidConfiguration);
            }
            let state_parent = state
                .resource_path
                .parent()
                .ok_or(ServiceError::InvalidConfiguration)?;
            let owner_resource = state_parent.join(".agent-runner-server-realms");
            if self.entries.values().any(|entry| {
                let directory = match &entry.location {
                    Location::Existing { directory } => directory,
                    Location::Managed { root, .. } => root,
                };
                directory.device == state.device
                    && (owner_resource.starts_with(&directory.resource_path)
                        || directory.resource_path.starts_with(&owner_resource))
            }) {
                return Err(ServiceError::InvalidConfiguration);
            }
            let visible = visible_state_path(&state)?;
            let parent = visible.parent().ok_or(ServiceError::InvalidConfiguration)?;
            let home_base = owner_root.join(".agent-runner-server-realms");
            ensure_private_directory(&home_base)?;
            let inode_directory = home_base.join(format!("i{:x}-{:x}", state.device, state.inode));
            ensure_private_directory(&inode_directory)?;
            let inode_record = serde_json::json!({"database":database,"state_device":state.device,"state_inode":state.inode});
            let (inode_guard, inode_fresh) =
                lock_owner_file(&inode_directory.join("owner.json"), &inode_record)?;
            let inode_journal = journal::Journal::open(
                &inode_directory.join("journal.jsonl"),
                inode_fresh,
                &inode_guard,
                store,
            )?;
            guards.push(inode_guard);
            let mut directory = parent.join(".agent-runner-server-realms");
            ensure_private_directory(&directory)?;
            let name = state
                .resource_path
                .file_name()
                .ok_or(ServiceError::InvalidConfiguration)?;
            for chunk in name.as_encoded_bytes().chunks(80) {
                let mut encoded = String::from("r");
                for byte in chunk {
                    use std::fmt::Write as _;
                    write!(&mut encoded, "{byte:02x}").map_err(|_| ServiceError::Unavailable)?;
                }
                directory.push(encoded);
                ensure_private_directory(&directory)?;
            }
            let record = serde_json::json!({"database":database,"state_device":state.device,"state_inode":state.inode,"state_resource":state.resource_path});
            let (guard, _) = lock_owner_file(&directory.join("owner.json"), &record)?;
            guards.push(guard);
            let index = journals.len();
            journals.push(inode_journal);
            realms.insert(realm.clone(), index);
            seen.insert((state.device, state.inode), (index, owner_root));
        }
        Ok(WorkspaceOwnership {
            _guards: guards,
            journals,
            realms,
        })
    }
}

pub(crate) struct WorkspaceOwnership {
    _guards: Vec<File>,
    journals: Vec<journal::Journal>,
    realms: HashMap<String, usize>,
}

impl WorkspaceOwnership {
    pub(crate) fn start(
        &mut self,
        key: journal::JournalKey,
        store: &mut crate::storage::SqliteStore,
    ) -> Result<bool, ServiceError> {
        let index = self.realms.get(&key.state_realm).copied().unwrap_or(0);
        if !self.journals[0].ensure_capacity(&key, store)?
            || (index != 0 && !self.journals[index].ensure_capacity(&key, store)?)
        {
            return Ok(false);
        }
        self.journals[0].start(key.clone())?;
        if index != 0 {
            self.journals[index].start(key)?;
        }
        Ok(true)
    }
    pub(crate) fn settle(&mut self, key: &journal::JournalKey) -> Result<(), ServiceError> {
        let index = self.realms.get(&key.state_realm).copied().unwrap_or(0);
        if index != 0 {
            self.journals[index].settle(key)?;
        }
        self.journals[0].settle(key)
    }
    #[cfg(test)]
    pub(crate) fn set_journal_capacity(&mut self, capacity: u64) {
        for journal in &mut self.journals {
            journal.set_capacity(capacity);
        }
    }

    #[cfg(test)]
    pub(crate) fn set_checkpoint_fault(&mut self, stage: u8) {
        for journal in &mut self.journals {
            journal.set_checkpoint_fault(stage);
        }
    }

    pub(crate) fn verify(&self, store: &crate::storage::SqliteStore) -> Result<(), ServiceError> {
        for journal in &self.journals {
            journal.verify(store)?;
        }
        Ok(())
    }
    pub(crate) fn reconcile(
        &mut self,
        store: &mut crate::storage::SqliteStore,
    ) -> Result<(), ServiceError> {
        self.verify(store)?;
        for journal in &mut self.journals {
            journal.reconcile(store)?;
        }
        Ok(())
    }
}

fn lock_owner_file(path: &Path, record: &serde_json::Value) -> Result<(File, bool), ServiceError> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(true);
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    let (mut file, fresh) = match options.open(path) {
        Ok(file) => (file, true),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            options.create_new(false);
            (
                options.open(path).map_err(|_| ServiceError::Unavailable)?,
                false,
            )
        }
        Err(_) => return Err(ServiceError::Unavailable),
    };
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let metadata = file.metadata().map_err(|_| ServiceError::Unavailable)?;
        // SAFETY: geteuid reads the current process credential without modifying it.
        let uid = unsafe { libc::geteuid() };
        if !metadata.is_file()
            || metadata.nlink() != 1
            || metadata.uid() != uid
            || metadata.permissions().mode() & 0o077 != 0
        {
            return Err(ServiceError::InvalidConfiguration);
        }
    }
    file.try_lock().map_err(|_| ServiceError::AlreadyRunning)?;
    let mut bytes = Vec::new();
    (&mut file)
        .take(16_385)
        .read_to_end(&mut bytes)
        .map_err(|_| ServiceError::Unavailable)?;
    if bytes.len() > 16_384 {
        return Err(ServiceError::InvalidConfiguration);
    }
    if fresh && bytes.is_empty() {
        let encoded = serde_json::to_vec(record).map_err(|_| ServiceError::Unavailable)?;
        file.seek(SeekFrom::Start(0))
            .map_err(|_| ServiceError::Unavailable)?;
        file.write_all(&encoded)
            .map_err(|_| ServiceError::Unavailable)?;
        file.sync_all().map_err(|_| ServiceError::Unavailable)?;
        File::open(path.parent().ok_or(ServiceError::InvalidConfiguration)?)
            .and_then(|directory| directory.sync_all())
            .map_err(|_| ServiceError::Unavailable)?;
    } else if fresh || crate::validation::parse_unique_json(&bytes).ok().as_ref() != Some(record) {
        return Err(ServiceError::InvalidConfiguration);
    }
    Ok((file, fresh))
}

fn ensure_private_directory(path: &Path) -> Result<(), ServiceError> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::DirBuilderExt;
        let mut builder = fs::DirBuilder::new();
        builder.mode(0o700);
        match builder.create(path) {
            Ok(()) => {
                File::open(path.parent().ok_or(ServiceError::InvalidConfiguration)?)
                    .and_then(|directory| directory.sync_all())
                    .map_err(|_| ServiceError::Unavailable)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(_) => return Err(ServiceError::Unavailable),
        }
        let metadata = fs::symlink_metadata(path).map_err(|_| ServiceError::Unavailable)?;
        if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
            return Err(ServiceError::InvalidConfiguration);
        }
        private_directory(path)?;
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = path;
        Err(ServiceError::InvalidConfiguration)
    }
}

#[cfg(target_os = "linux")]
fn visible_state_path(state: &Directory) -> Result<PathBuf, ServiceError> {
    use std::os::unix::{ffi::OsStringExt, fs::MetadataExt};
    let mut bytes = Vec::new();
    File::open("/proc/self/mountinfo")
        .map_err(|_| ServiceError::InvalidConfiguration)?
        .take(4 * 1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| ServiceError::InvalidConfiguration)?;
    if bytes.len() > 4 * 1024 * 1024 {
        return Err(ServiceError::InvalidConfiguration);
    }
    let relative = state
        .resource_path
        .strip_prefix("/")
        .map_err(|_| ServiceError::InvalidConfiguration)?;
    let mut candidates = Vec::new();
    for line in bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let fields: Vec<_> = line
            .split(|byte| *byte == b' ')
            .filter(|field| !field.is_empty())
            .collect();
        if fields.len() < 6 {
            return Err(ServiceError::InvalidConfiguration);
        }
        let root = PathBuf::from(std::ffi::OsString::from_vec(decode_mount_field(fields[3])?));
        if root != Path::new("/") {
            continue;
        }
        let point = PathBuf::from(std::ffi::OsString::from_vec(decode_mount_field(fields[4])?));
        let candidate = point.join(relative);
        if fs::metadata(&candidate).is_ok_and(|metadata| {
            metadata.is_dir() && metadata.dev() == state.device && metadata.ino() == state.inode
        }) {
            candidates.push(candidate);
        }
    }
    candidates.sort();
    let candidate = candidates
        .into_iter()
        .next()
        .ok_or(ServiceError::InvalidConfiguration)?;
    fs::canonicalize(candidate).map_err(|_| ServiceError::InvalidConfiguration)
}

#[cfg(not(target_os = "linux"))]
fn visible_state_path(_state: &Directory) -> Result<PathBuf, ServiceError> {
    Err(ServiceError::InvalidConfiguration)
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct DirectoryFile {
    path: PathBuf,
    device: u64,
    inode: u64,
}

impl DirectoryFile {
    fn inspect(path: &Path) -> Result<Self, ServiceError> {
        #[cfg(not(target_os = "linux"))]
        {
            let _ = path;
            return Err(ServiceError::InvalidConfiguration);
        }
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::MetadataExt;
            let path = fs::canonicalize(path).map_err(|_| ServiceError::Unavailable)?;
            let meta = fs::metadata(&path).map_err(|_| ServiceError::Unavailable)?;
            if !meta.is_file() || meta.nlink() != 1 {
                return Err(ServiceError::InvalidConfiguration);
            }
            Ok(Self {
                path,
                device: meta.dev(),
                inode: meta.ino(),
            })
        }
    }
}

fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

fn private_directory(path: &Path) -> Result<(), ServiceError> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let metadata = fs::metadata(path).map_err(|_| ServiceError::InvalidConfiguration)?;
        // SAFETY: geteuid reads the current process credential without modifying it.
        let uid = unsafe { libc::geteuid() };
        if metadata.uid() != uid || metadata.permissions().mode() & 0o077 != 0 {
            return Err(ServiceError::InvalidConfiguration);
        }
    }
    Ok(())
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    #[test]
    fn mount_aliases_resolve_to_the_same_underlying_resource() {
        let mounts = b"10 1 8:1 / / rw - ext4 /dev/test rw\n11 10 8:1 /home/repo /alias rw - ext4 /dev/test rw\n";
        assert_eq!(
            mount_resource_path(Path::new("/home/repo/src"), mounts).unwrap(),
            Path::new("/home/repo/src")
        );
        assert_eq!(
            mount_resource_path(Path::new("/alias/src"), mounts).unwrap(),
            Path::new("/home/repo/src")
        );
    }

    #[test]
    fn nested_mounts_are_rejected_and_escaped_names_are_decoded() {
        let mounts = b"10 1 8:1 / / rw - ext4 /dev/test rw\n11 10 8:1 /other /home/repo/vendor rw - ext4 /dev/test rw\n";
        assert!(mount_resource_path(Path::new("/home/repo"), mounts).is_err());
        assert_eq!(
            decode_mount_field(br"path\040with\134slash").unwrap(),
            b"path with\\slash"
        );
        assert!(decode_mount_field(br"path\777").is_err());
    }

    #[test]
    fn stacked_mountpoints_are_rejected_instead_of_guessing_which_mount_is_visible() {
        let mounts = b"10 1 8:1 / / rw - ext4 /dev/test rw\n11 10 8:1 /old /alias rw - ext4 /dev/test rw\n12 10 8:1 /new /alias rw - ext4 /dev/test rw\n";
        assert!(mount_resource_path(Path::new("/alias/repo"), mounts).is_err());
    }
}
