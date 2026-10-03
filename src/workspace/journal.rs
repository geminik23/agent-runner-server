//! Fail-closed launch evidence stored outside the execution database.

use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use crate::{
    application::ServiceError,
    storage::{SqliteStore, workspaces::JournalStatus},
};

const MAX_JOURNAL_BYTES: u64 = 16 * 1024 * 1024;
const JOURNAL_HEADER: &[u8] = b"agent-runner-workspace-journal-v1\n";

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(deny_unknown_fields)]
pub(crate) struct JournalKey {
    pub owner_id: String,
    pub client_id: String,
    pub run_id: String,
    pub state_realm: String,
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Record {
    Started { key: JournalKey },
    Settled { key: JournalKey },
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CheckpointIntent {
    from: u64,
    to: u64,
    nonce: String,
}

pub(super) struct Journal {
    path: PathBuf,
    identity: String,
    file: File,
    sequence: File,
    epoch_file: File,
    epoch: u64,
    capacity: u64,
    #[cfg(test)]
    checkpoint_fault: Option<u8>,
    sequence_number: u64,
    anchor_lagging: bool,
    deferred_checkpoint_rollback: bool,
    pending: HashSet<JournalKey>,
    settled: HashSet<JournalKey>,
}

impl Journal {
    pub(super) fn open(
        path: &Path,
        fresh_owner: bool,
        owner: &File,
        store: &SqliteStore,
    ) -> Result<Self, ServiceError> {
        #[cfg(target_os = "linux")]
        let identity = {
            use std::os::unix::fs::MetadataExt;
            let metadata = owner.metadata().map_err(|_| ServiceError::Unavailable)?;
            format!("{:x}:{:x}", metadata.dev(), metadata.ino())
        };
        #[cfg(not(target_os = "linux"))]
        let identity = {
            let _ = owner;
            return Err(ServiceError::InvalidConfiguration);
        };
        let deferred_checkpoint_rollback = if fresh_owner {
            match std::fs::symlink_metadata(path.with_extension("intent")) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
                _ => return Err(ServiceError::InvalidConfiguration),
            }
        } else {
            recover_checkpoint(path, &identity, store)?
        };
        let mut file = open_private(path, fresh_owner)?;
        let mut sequence = open_private(&path.with_extension("seq"), fresh_owner)?;
        let mut epoch_file = open_private(&path.with_extension("epoch"), fresh_owner)?;
        if fresh_owner {
            epoch_file
                .write_all(b"00000000000000000000\n")
                .map_err(|_| ServiceError::Unavailable)?;
            epoch_file
                .sync_all()
                .map_err(|_| ServiceError::Unavailable)?;
        }
        let epoch = read_number(&mut epoch_file)?;
        if fresh_owner {
            sequence
                .write_all(b"00000000000000000000\n")
                .map_err(|_| ServiceError::Unavailable)?;
            sequence.sync_all().map_err(|_| ServiceError::Unavailable)?;
        }
        sequence
            .seek(SeekFrom::Start(0))
            .map_err(|_| ServiceError::Unavailable)?;
        let mut sequence_bytes = Vec::new();
        (&mut sequence)
            .take(22)
            .read_to_end(&mut sequence_bytes)
            .map_err(|_| ServiceError::Unavailable)?;
        if sequence_bytes.len() != 21
            || sequence_bytes[20] != b'\n'
            || !sequence_bytes[..20].iter().all(u8::is_ascii_digit)
        {
            return Err(ServiceError::InvalidConfiguration);
        }
        let anchored: u64 = std::str::from_utf8(&sequence_bytes[..20])
            .map_err(|_| ServiceError::InvalidConfiguration)?
            .parse()
            .map_err(|_| ServiceError::InvalidConfiguration)?;
        if fresh_owner {
            file.write_all(JOURNAL_HEADER)
                .map_err(|_| ServiceError::Unavailable)?;
            file.sync_all().map_err(|_| ServiceError::Unavailable)?;
            file.seek(SeekFrom::Start(0))
                .map_err(|_| ServiceError::Unavailable)?;
        }
        let mut bytes = Vec::new();
        (&mut file)
            .take(MAX_JOURNAL_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| ServiceError::Unavailable)?;
        if bytes.len() as u64 > MAX_JOURNAL_BYTES
            || !bytes.starts_with(JOURNAL_HEADER)
            || !bytes.ends_with(b"\n")
        {
            return Err(ServiceError::InvalidConfiguration);
        }
        let mut pending = HashSet::new();
        let mut settled = HashSet::new();
        let mut sequence_number = 0_u64;
        let records = &bytes[JOURNAL_HEADER.len()..];
        if records.first() == Some(&b'\n') || records.windows(2).any(|pair| pair == b"\n\n") {
            return Err(ServiceError::InvalidConfiguration);
        }
        for line in records
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
        {
            sequence_number = sequence_number
                .checked_add(1)
                .ok_or(ServiceError::InvalidConfiguration)?;
            let value = crate::validation::parse_unique_json(line)
                .map_err(|_| ServiceError::InvalidConfiguration)?;
            let record: Record =
                serde_json::from_value(value).map_err(|_| ServiceError::InvalidConfiguration)?;
            match record {
                Record::Started { key } => {
                    if settled.contains(&key) || !pending.insert(key) {
                        return Err(ServiceError::InvalidConfiguration);
                    }
                }
                Record::Settled { key } => {
                    if !pending.remove(&key) || !settled.insert(key) {
                        return Err(ServiceError::InvalidConfiguration);
                    }
                }
            }
        }
        if sequence_number < anchored || sequence_number.saturating_sub(anchored) > 1 {
            return Err(ServiceError::InvalidConfiguration);
        }
        File::open(path.parent().ok_or(ServiceError::InvalidConfiguration)?)
            .and_then(|directory| directory.sync_all())
            .map_err(|_| ServiceError::Unavailable)?;
        Ok(Self {
            path: path.to_path_buf(),
            identity,
            file,
            sequence,
            epoch_file,
            epoch,
            capacity: MAX_JOURNAL_BYTES,
            #[cfg(test)]
            checkpoint_fault: None,
            sequence_number,
            anchor_lagging: sequence_number != anchored,
            deferred_checkpoint_rollback,
            pending,
            settled,
        })
    }

    fn append(&mut self, record: &Record, reserve: u64) -> Result<(), ServiceError> {
        if self.anchor_lagging {
            return Err(ServiceError::InvalidConfiguration);
        }
        let mut encoded = serde_json::to_vec(record).map_err(|_| ServiceError::Unavailable)?;
        encoded.push(b'\n');
        let current = self
            .file
            .metadata()
            .map_err(|_| ServiceError::Unavailable)?
            .len();
        let needed = u64::try_from(encoded.len()).map_err(|_| ServiceError::Unavailable)?;

        if current.saturating_add(needed).saturating_add(reserve) > self.capacity {
            return Err(ServiceError::Unavailable);
        }
        self.file
            .seek(SeekFrom::End(0))
            .map_err(|_| ServiceError::Unavailable)?;
        self.file
            .write_all(&encoded)
            .map_err(|_| ServiceError::Unavailable)?;
        self.file
            .sync_data()
            .map_err(|_| ServiceError::Unavailable)?;
        self.sequence_number = self
            .sequence_number
            .checked_add(1)
            .ok_or(ServiceError::Unavailable)?;
        self.anchor_lagging = true;
        self.persist_sequence()?;
        Ok(())
    }

    fn persist_sequence(&mut self) -> Result<(), ServiceError> {
        self.sequence
            .seek(SeekFrom::Start(0))
            .map_err(|_| ServiceError::Unavailable)?;
        self.sequence
            .write_all(format!("{:020}\n", self.sequence_number).as_bytes())
            .map_err(|_| ServiceError::Unavailable)?;
        self.sequence
            .sync_data()
            .map_err(|_| ServiceError::Unavailable)?;
        self.anchor_lagging = false;
        Ok(())
    }

    fn start_reserve(&self, key: &JournalKey) -> Result<u64, ServiceError> {
        self.pending.iter().try_fold(
            record_size(&Record::Settled { key: key.clone() })?,
            |total, existing| {
                total
                    .checked_add(record_size(&Record::Settled {
                        key: existing.clone(),
                    })?)
                    .ok_or(ServiceError::Unavailable)
            },
        )
    }

    pub(super) fn can_start(&self, key: &JournalKey) -> Result<bool, ServiceError> {
        if self.anchor_lagging
            || self.deferred_checkpoint_rollback
            || self.pending.contains(key)
            || self.settled.contains(key)
        {
            return Err(ServiceError::InvalidConfiguration);
        }
        let current = self
            .file
            .metadata()
            .map_err(|_| ServiceError::Unavailable)?
            .len();
        let needed = record_size(&Record::Started { key: key.clone() })?;
        Ok(current
            .saturating_add(needed)
            .saturating_add(self.start_reserve(key)?)
            <= self.capacity)
    }

    pub(super) fn ensure_capacity(
        &mut self,
        key: &JournalKey,
        store: &mut SqliteStore,
    ) -> Result<bool, ServiceError> {
        if self.can_start(key)? {
            return Ok(true);
        }
        if !self.pending.is_empty() || self.settled.is_empty() {
            return Ok(false);
        }
        self.checkpoint(store)?;
        self.can_start(key)
    }

    fn checkpoint(&mut self, store: &mut SqliteStore) -> Result<(), ServiceError> {
        if !self.pending.is_empty() || self.anchor_lagging {
            return Err(ServiceError::InvalidConfiguration);
        }
        self.verify(store)?;
        let next = self.epoch.checked_add(1).ok_or(ServiceError::Unavailable)?;
        let nonce = checkpoint_nonce()?;
        let next_journal = self.path.with_extension(format!("journal-{nonce}.tmp"));
        let next_sequence = self.path.with_extension(format!("sequence-{nonce}.tmp"));
        let next_epoch = self.path.with_extension(format!("epoch-{nonce}.tmp"));
        for (path, contents) in [
            (&next_journal, JOURNAL_HEADER.to_vec()),
            (&next_sequence, b"00000000000000000000\n".to_vec()),
            (&next_epoch, format!("{next:020}\n").into_bytes()),
        ] {
            let mut file = open_private(path, true)?;
            file.write_all(&contents)
                .map_err(|_| ServiceError::Unavailable)?;
            file.sync_all().map_err(|_| ServiceError::Unavailable)?;
        }
        let parent = self
            .path
            .parent()
            .ok_or(ServiceError::InvalidConfiguration)?;
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|_| ServiceError::Unavailable)?;
        let intent_path = self.path.with_extension("intent");
        let intent = CheckpointIntent {
            from: self.epoch,
            to: next,
            nonce,
        };
        let mut intent_file = open_private(&intent_path, true)?;
        intent_file
            .write_all(&serde_json::to_vec(&intent).map_err(|_| ServiceError::Unavailable)?)
            .map_err(|_| ServiceError::Unavailable)?;
        intent_file
            .sync_all()
            .map_err(|_| ServiceError::Unavailable)?;
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|_| ServiceError::Unavailable)?;
        #[cfg(test)]
        if self.checkpoint_fault == Some(1) {
            return Err(ServiceError::Unavailable);
        }
        if store
            .workspace_advance_journal_epoch(&self.identity, self.epoch)
            .map_err(ServiceError::Storage)?
            != next
        {
            return Err(ServiceError::InvalidConfiguration);
        }
        #[cfg(test)]
        if self.checkpoint_fault == Some(2) {
            return Err(ServiceError::Unavailable);
        }
        std::fs::rename(&next_journal, &self.path).map_err(|_| ServiceError::Unavailable)?;
        #[cfg(test)]
        if self.checkpoint_fault == Some(3) {
            return Err(ServiceError::Unavailable);
        }
        std::fs::rename(&next_sequence, self.path.with_extension("seq"))
            .map_err(|_| ServiceError::Unavailable)?;
        #[cfg(test)]
        if self.checkpoint_fault == Some(4) {
            return Err(ServiceError::Unavailable);
        }
        std::fs::rename(&next_epoch, self.path.with_extension("epoch"))
            .map_err(|_| ServiceError::Unavailable)?;
        #[cfg(test)]
        if self.checkpoint_fault == Some(5) {
            return Err(ServiceError::Unavailable);
        }
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|_| ServiceError::Unavailable)?;
        std::fs::remove_file(&intent_path).map_err(|_| ServiceError::Unavailable)?;
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|_| ServiceError::Unavailable)?;
        self.file = open_private(&self.path, false)?;
        self.sequence = open_private(&self.path.with_extension("seq"), false)?;
        self.epoch_file = open_private(&self.path.with_extension("epoch"), false)?;
        self.epoch = next;
        self.sequence_number = 0;
        self.settled.clear();
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn set_capacity(&mut self, capacity: u64) {
        self.capacity = capacity;
    }

    #[cfg(test)]
    pub(super) fn set_checkpoint_fault(&mut self, stage: u8) {
        self.checkpoint_fault = Some(stage);
    }

    pub(super) fn start(&mut self, key: JournalKey) -> Result<(), ServiceError> {
        if !self.can_start(&key)? {
            return Err(ServiceError::Unavailable);
        }
        let reserve = self.start_reserve(&key)?;
        self.append(&Record::Started { key: key.clone() }, reserve)?;
        self.pending.insert(key);
        Ok(())
    }

    pub(super) fn settle(&mut self, key: &JournalKey) -> Result<(), ServiceError> {
        if !self.pending.contains(key) {
            return Err(ServiceError::InvalidConfiguration);
        }
        let reserve = self.pending.iter().filter(|other| *other != key).try_fold(
            0_u64,
            |total, existing| {
                total
                    .checked_add(record_size(&Record::Settled {
                        key: existing.clone(),
                    })?)
                    .ok_or(ServiceError::Unavailable)
            },
        )?;
        self.append(&Record::Settled { key: key.clone() }, reserve)?;
        self.pending.remove(key);
        self.settled.insert(key.clone());
        Ok(())
    }

    pub(super) fn verify(&self, store: &SqliteStore) -> Result<(), ServiceError> {
        match store
            .workspace_journal_epoch(&self.identity)
            .map_err(ServiceError::Storage)?
        {
            Some(epoch) if epoch == self.epoch => {}
            None if self.epoch == 0 && self.pending.is_empty() && self.settled.is_empty() => {}
            _ => return Err(ServiceError::InvalidConfiguration),
        }
        for key in &self.settled {
            if !matches!(
                store
                    .workspace_journal_status(key)
                    .map_err(ServiceError::Storage)?,
                JournalStatus::Settled
            ) {
                return Err(ServiceError::InvalidConfiguration);
            }
        }
        for key in &self.pending {
            if matches!(
                store
                    .workspace_journal_status(key)
                    .map_err(ServiceError::Storage)?,
                JournalStatus::Inconsistent
            ) {
                return Err(ServiceError::InvalidConfiguration);
            }
        }
        Ok(())
    }

    pub(super) fn reconcile(&mut self, store: &mut SqliteStore) -> Result<(), ServiceError> {
        self.verify(store)?;
        if self.deferred_checkpoint_rollback {
            std::fs::remove_file(self.path.with_extension("intent"))
                .map_err(|_| ServiceError::Unavailable)?;
            File::open(
                self.path
                    .parent()
                    .ok_or(ServiceError::InvalidConfiguration)?,
            )
            .and_then(|directory| directory.sync_all())
            .map_err(|_| ServiceError::Unavailable)?;
            self.deferred_checkpoint_rollback = false;
        }
        if self.anchor_lagging {
            self.persist_sequence()?;
        }
        for key in self.pending.clone() {
            match store
                .workspace_journal_status(&key)
                .map_err(ServiceError::Storage)?
            {
                JournalStatus::Settled => self.settle(&key)?,
                JournalStatus::Uncertain => {}
                JournalStatus::NeedsQuarantine => store
                    .workspace_quarantine_journal(&key)
                    .map_err(ServiceError::Storage)?,
                JournalStatus::Inconsistent => return Err(ServiceError::InvalidConfiguration),
            }
        }
        Ok(())
    }
}

fn recover_checkpoint(
    path: &Path,
    identity: &str,
    store: &SqliteStore,
) -> Result<bool, ServiceError> {
    let intent_path = path.with_extension("intent");
    match std::fs::symlink_metadata(&intent_path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(_) => return Err(ServiceError::InvalidConfiguration),
        Ok(_) => {}
    }
    let bytes = read_private(&intent_path, 4096)?;
    let value = crate::validation::parse_unique_json(&bytes)
        .map_err(|_| ServiceError::InvalidConfiguration)?;
    let intent: CheckpointIntent =
        serde_json::from_value(value).map_err(|_| ServiceError::InvalidConfiguration)?;
    if intent.to
        != intent
            .from
            .checked_add(1)
            .ok_or(ServiceError::InvalidConfiguration)?
        || intent.nonce.len() != 16
        || !intent.nonce.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(ServiceError::InvalidConfiguration);
    }
    let epoch = store
        .workspace_journal_epoch(identity)
        .map_err(ServiceError::Storage)?
        .ok_or(ServiceError::InvalidConfiguration)?;
    let mut epoch_file = open_private(&path.with_extension("epoch"), false)?;
    let disk_epoch = read_number(&mut epoch_file)?;
    let parent = path.parent().ok_or(ServiceError::InvalidConfiguration)?;
    if epoch == intent.from {
        let mut sequence = open_private(&path.with_extension("seq"), false)?;
        let old_sequence = read_number(&mut sequence)?;
        let journal = std::fs::metadata(path).map_err(|_| ServiceError::InvalidConfiguration)?;
        if disk_epoch != intent.from
            || old_sequence == 0
            || journal.len() <= JOURNAL_HEADER.len() as u64
        {
            return Err(ServiceError::InvalidConfiguration);
        }
        verify_checkpoint_source(path, old_sequence, store)?;
        return Ok(true);
    } else if epoch == intent.to {
        if disk_epoch != intent.from && disk_epoch != intent.to {
            return Err(ServiceError::InvalidConfiguration);
        }
        let next_journal = path.with_extension(format!("journal-{}.tmp", intent.nonce));
        let next_sequence = path.with_extension(format!("sequence-{}.tmp", intent.nonce));
        let next_epoch = path.with_extension(format!("epoch-{}.tmp", intent.nonce));
        for (temporary, current, expected) in [
            (next_journal, path.to_path_buf(), JOURNAL_HEADER.to_vec()),
            (
                next_sequence,
                path.with_extension("seq"),
                b"00000000000000000000\n".to_vec(),
            ),
            (
                next_epoch,
                path.with_extension("epoch"),
                format!("{:020}\n", intent.to).into_bytes(),
            ),
        ] {
            match std::fs::symlink_metadata(&temporary) {
                Ok(_) => {
                    if read_private(&temporary, expected.len())? != expected {
                        return Err(ServiceError::InvalidConfiguration);
                    }
                    std::fs::rename(&temporary, &current).map_err(|_| ServiceError::Unavailable)?;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    if read_private(&current, expected.len())? != expected {
                        return Err(ServiceError::InvalidConfiguration);
                    }
                }
                Err(_) => return Err(ServiceError::InvalidConfiguration),
            }
        }
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|_| ServiceError::Unavailable)?;
    } else {
        return Err(ServiceError::InvalidConfiguration);
    }
    std::fs::remove_file(intent_path).map_err(|_| ServiceError::Unavailable)?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| ServiceError::Unavailable)?;
    Ok(false)
}

fn verify_checkpoint_source(
    path: &Path,
    sequence: u64,
    store: &SqliteStore,
) -> Result<(), ServiceError> {
    let bytes = read_private(
        path,
        usize::try_from(MAX_JOURNAL_BYTES).map_err(|_| ServiceError::Unavailable)?,
    )?;
    if !bytes.starts_with(JOURNAL_HEADER) || !bytes.ends_with(b"\n") {
        return Err(ServiceError::InvalidConfiguration);
    }
    let records = &bytes[JOURNAL_HEADER.len()..];
    if records.first() == Some(&b'\n') || records.windows(2).any(|pair| pair == b"\n\n") {
        return Err(ServiceError::InvalidConfiguration);
    }
    let mut pending = HashSet::new();
    let mut settled = HashSet::new();
    let mut count = 0_u64;
    for line in records
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        count = count
            .checked_add(1)
            .ok_or(ServiceError::InvalidConfiguration)?;
        let value = crate::validation::parse_unique_json(line)
            .map_err(|_| ServiceError::InvalidConfiguration)?;
        let record: Record =
            serde_json::from_value(value).map_err(|_| ServiceError::InvalidConfiguration)?;
        match record {
            Record::Started { key } => {
                if settled.contains(&key) || !pending.insert(key) {
                    return Err(ServiceError::InvalidConfiguration);
                }
            }
            Record::Settled { key } => {
                if !pending.remove(&key) || !settled.insert(key) {
                    return Err(ServiceError::InvalidConfiguration);
                }
            }
        }
    }
    if count != sequence || !pending.is_empty() || settled.is_empty() {
        return Err(ServiceError::InvalidConfiguration);
    }
    for key in settled {
        if !matches!(
            store
                .workspace_journal_status(&key)
                .map_err(ServiceError::Storage)?,
            JournalStatus::Settled
        ) {
            return Err(ServiceError::InvalidConfiguration);
        }
    }
    Ok(())
}

fn read_private(path: &Path, limit: usize) -> Result<Vec<u8>, ServiceError> {
    let mut bytes = Vec::new();
    open_private(path, false)?
        .take(
            u64::try_from(limit)
                .map_err(|_| ServiceError::InvalidConfiguration)?
                .saturating_add(1),
        )
        .read_to_end(&mut bytes)
        .map_err(|_| ServiceError::Unavailable)?;
    if bytes.len() > limit {
        return Err(ServiceError::InvalidConfiguration);
    }
    Ok(bytes)
}

fn checkpoint_nonce() -> Result<String, ServiceError> {
    #[cfg(target_os = "linux")]
    {
        use rustix::rand::{GetRandomFlags, getrandom};
        let mut bytes = [0_u8; 8];
        let mut filled = 0;
        while filled < bytes.len() {
            let count = getrandom(&mut bytes[filled..], GetRandomFlags::NONBLOCK)
                .map_err(|_| ServiceError::Unavailable)?;
            if count == 0 {
                return Err(ServiceError::Unavailable);
            }
            filled += count;
        }
        let mut result = String::with_capacity(16);
        for byte in bytes {
            use std::fmt::Write as _;
            write!(&mut result, "{byte:02x}").map_err(|_| ServiceError::Unavailable)?;
        }
        Ok(result)
    }
    #[cfg(not(target_os = "linux"))]
    {
        Err(ServiceError::InvalidConfiguration)
    }
}

fn read_number(file: &mut File) -> Result<u64, ServiceError> {
    file.seek(SeekFrom::Start(0))
        .map_err(|_| ServiceError::Unavailable)?;
    let mut bytes = Vec::new();
    file.take(22)
        .read_to_end(&mut bytes)
        .map_err(|_| ServiceError::Unavailable)?;
    if bytes.len() != 21 || bytes[20] != b'\n' || !bytes[..20].iter().all(u8::is_ascii_digit) {
        return Err(ServiceError::InvalidConfiguration);
    }
    std::str::from_utf8(&bytes[..20])
        .map_err(|_| ServiceError::InvalidConfiguration)?
        .parse()
        .map_err(|_| ServiceError::InvalidConfiguration)
}

fn open_private(path: &Path, create: bool) -> Result<File, ServiceError> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(create);
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    let file = options.open(path).map_err(|_| ServiceError::Unavailable)?;
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
    Ok(file)
}

fn record_size(record: &Record) -> Result<u64, ServiceError> {
    let size = serde_json::to_vec(record)
        .map_err(|_| ServiceError::Unavailable)?
        .len();
    u64::try_from(size)
        .ok()
        .and_then(|value| value.checked_add(1))
        .ok_or(ServiceError::Unavailable)
}
