//! Control for atomic project publication.

use super::{
    Arc, Deserialize, Digest, File, GfError, HashMap, MAX_JOURNAL_BYTES, Mutex, OnceLock,
    OpenOptions, Path, PathBuf, ProjectErrorCode, ProjectGenerationRequest, Read,
    RevertJournalExtension, Serialize, Sha256, StagedParticipant, Uuid, Write, project_error,
    project_failpoint, publication_io,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum JournalPhase {
    Preparing,
    Staged,
    Validated,
    Durable,
    Published,
    Aborted,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct JournalRecord {
    pub(crate) format: String,
    pub(crate) format_version: u32,
    pub(crate) transaction_uuid: String,
    pub(crate) generation_uuid: String,
    pub(crate) parent_generation_uuid: Option<String>,
    pub(crate) phase: JournalPhase,
    pub(crate) request_fingerprint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) operation_fingerprint: Option<String>,
    pub(crate) participant_paths: Vec<String>,
    pub(crate) generation_manifest_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) revert: Option<RevertJournalExtension>,
}

impl JournalRecord {
    pub(super) fn new(
        request: &ProjectGenerationRequest,
        parent: Option<Uuid>,
        phase: JournalPhase,
        fingerprints: (String, String),
        participants: &[StagedParticipant],
        generation_manifest_sha256: Option<String>,
        revert: Option<RevertJournalExtension>,
    ) -> Self {
        let (request_fingerprint, operation_fingerprint) = fingerprints;
        Self {
            format: "graphforge-transaction".into(),
            format_version: 1,
            transaction_uuid: request.transaction_uuid.hyphenated().to_string(),
            generation_uuid: request.generation_uuid.hyphenated().to_string(),
            parent_generation_uuid: parent.map(|uuid| uuid.hyphenated().to_string()),
            phase,
            request_fingerprint,
            operation_fingerprint: Some(operation_fingerprint),
            participant_paths: participants
                .iter()
                .map(|participant| participant.relative_path.clone())
                .collect(),
            generation_manifest_sha256,
            revert,
        }
    }

    pub(crate) fn operation_fingerprint(&self) -> &str {
        self.operation_fingerprint
            .as_deref()
            .unwrap_or(&self.request_fingerprint)
    }
}

#[derive(Debug, Serialize)]
pub(super) struct CurrentRecord {
    pub(super) format: String,
    pub(super) format_version: u32,
    pub(super) generation_uuid: String,
    pub(super) generation_manifest_sha256: String,
}

#[derive(Debug, Serialize)]
pub(super) struct GenerationManifestRecord {
    pub(super) format: String,
    pub(super) format_version: u32,
    pub(super) generation_uuid: String,
    pub(super) parent_generation_uuid: Option<String>,
    pub(super) transaction_uuid: String,
    pub(super) capabilities: Vec<CapabilityRecord>,
    pub(super) participants: Vec<StagedParticipant>,
}

#[derive(Debug, Serialize)]
pub(super) struct CapabilityRecord {
    pub(super) capability_id: String,
    pub(super) capability_version: u32,
}

pub(super) fn write_new(
    path: &Path,
    bytes: &[u8],
    allocation: Option<&crate::StorageAllocationOperation>,
) -> Result<File, GfError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(publication_io)?;
    let written = file.write_all(bytes).map_err(publication_io);
    let observed = allocation.map_or(Ok(()), |allocation| allocation.replace_file_at(path, &file));
    written?;
    observed?;
    Ok(file)
}

pub(super) fn failpoint_as_io(
    name: &str,
    transaction_uuid: Uuid,
    generation_uuid: Uuid,
    phase: &str,
    committed: bool,
) -> std::io::Result<()> {
    project_failpoint::hit(
        name,
        Some(transaction_uuid),
        Some(generation_uuid),
        phase,
        committed,
    )
    .map_err(|error| std::io::Error::other(error.to_string()))
}

pub(super) fn verify_exact_file(path: &Path, expected: &[u8]) -> Result<(), GfError> {
    let mut actual = Vec::new();
    File::open(path)
        .and_then(|mut file| file.read_to_end(&mut actual))
        .map_err(publication_io)?;
    if actual != expected {
        return Err(project_error(
            ProjectErrorCode::PublicationFailed,
            "durable file reread did not match staged bytes",
        ));
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn write_journal(path: &Path, journal: &JournalRecord) -> Result<(), GfError> {
    write_journal_with_allocation(path, journal, None)
}

pub(crate) fn write_journal_with_allocation(
    path: &Path,
    journal: &JournalRecord,
    allocation: Option<&crate::StorageAllocationOperation>,
) -> Result<(), GfError> {
    let bytes = canonical_line(journal)?;
    publish_atomic_bytes_with_allocation(path, &bytes, || Ok(()), || Ok(()), || Ok(()), allocation)
        .map_err(publication_io)?;
    sync_directory(
        path.parent()
            .expect("transaction journal always has a parent"),
    )
}

#[derive(Debug)]
pub(crate) enum AtomicPublishError {
    Io(std::io::Error),
    Replacement(graphforge_filesystem::ReplaceFileError),
    CancelledBeforeReplace,
}

impl std::fmt::Display for AtomicPublishError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => error.fmt(formatter),
            Self::Replacement(error) => error.fmt(formatter),
            Self::CancelledBeforeReplace => formatter.write_str("cancelled before replacement"),
        }
    }
}

impl std::error::Error for AtomicPublishError {}

impl From<std::io::Error> for AtomicPublishError {
    fn from(error: std::io::Error) -> Self {
        if let Some(source) = error.get_ref()
            && source.is::<CancelledBeforeReplace>()
        {
            Self::CancelledBeforeReplace
        } else {
            Self::Io(error)
        }
    }
}

#[derive(Debug)]
pub(super) struct CancelledBeforeReplace;

impl std::fmt::Display for CancelledBeforeReplace {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("publication cancelled at project.before_current_replace")
    }
}

impl std::error::Error for CancelledBeforeReplace {}

pub(crate) fn publish_atomic_bytes(
    path: &Path,
    bytes: &[u8],
    after_write: impl FnOnce() -> std::io::Result<()>,
    after_sync: impl FnOnce() -> std::io::Result<()>,
    before_replace: impl FnOnce() -> std::io::Result<()>,
) -> Result<(), AtomicPublishError> {
    publish_atomic_bytes_with_allocation(path, bytes, after_write, after_sync, before_replace, None)
}

pub(crate) fn publish_atomic_bytes_with_allocation(
    path: &Path,
    bytes: &[u8],
    after_write: impl FnOnce() -> std::io::Result<()>,
    after_sync: impl FnOnce() -> std::io::Result<()>,
    before_replace: impl FnOnce() -> std::io::Result<()>,
    allocation: Option<&crate::StorageAllocationOperation>,
) -> Result<(), AtomicPublishError> {
    let parent = path
        .parent()
        .ok_or_else(|| std::io::Error::other("atomic publication target has no parent"))?;
    let target_name = path
        .file_name()
        .ok_or_else(|| std::io::Error::other("atomic publication target has no file name"))?;
    let target_text = target_name
        .to_str()
        .ok_or_else(|| std::io::Error::other("atomic publication target is not UTF-8"))?;
    // Hash the target name plus a per-attempt identity. Hashing only the
    // target made concurrent CURRENT publishers share one temp, so one
    // writer's `create_new` prep deleted the other's in-flight file and
    // `replace_file` failed with ENOENT ("file was not replaced").
    let temp_name = unique_atomic_temp_name(target_text);
    let temp_path = parent.join(&temp_name);

    let publish = || -> Result<(), AtomicPublishError> {
        let mut temp = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&temp_path)?;
        // Recovery may run in a separately loaded native addon while an
        // optimistic writer is still staging its journal.  A Rust static is
        // not an authority across those environments, so the temporary file
        // itself carries a kernel-visible lease until replacement completes.
        crate::file_lock::lock_exclusive(&temp)?;
        let written = temp.write_all(bytes);
        let observed = allocation.map_or(Ok(()), |allocation| {
            allocation.replace_file_at(&temp_path, &temp)
        });
        written?;
        observed.map_err(std::io::Error::other)?;
        after_write()?;
        temp.sync_all()?;
        if let Some(allocation) = allocation {
            allocation
                .replace_file_at(&temp_path, &temp)
                .map_err(std::io::Error::other)?;
        }
        after_sync()?;
        before_replace()?;

        // Concurrent same-process replace of one target must not overlap
        // `replace_file`: the loser's post-rename identity check sees the
        // winner's inode and returns StateUnknown. Unique temps already
        // prevent the shared-name ENOENT; this lock serializes the rename.
        let namespace_lock = lock_atomic_publish_target(path);
        let _namespace_guard = namespace_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let directory = crate::filesystem_admission::open_directory_handle(parent)?;
        // Existence is one snapshot; `replace_file` / `install_new_file` verify
        // regular single-link identity on the open handles.
        let result = match std::fs::symlink_metadata(path) {
            Ok(_) => graphforge_filesystem::replace_file(
                &directory,
                std::ffi::OsStr::new(&temp_name),
                target_name,
            )
            .map_err(AtomicPublishError::Replacement),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                graphforge_filesystem::install_new_file(
                    &directory,
                    std::ffi::OsStr::new(&temp_name),
                    target_name,
                )
                .map_err(AtomicPublishError::Io)
            }
            Err(error) => Err(AtomicPublishError::Io(error)),
        };
        if result.is_ok() {
            record_atomic_replacement(allocation, path, &temp_path, &temp)?;
        }
        let unlock = crate::file_lock::unlock(&temp);
        drop(temp);
        result.and_then(|()| unlock.map_err(AtomicPublishError::Io))
    };
    let result = publish();
    if result.is_err()
        && std::fs::remove_file(&temp_path).is_ok()
        && let Some(allocation) = allocation
    {
        // Preserve the original publication error, especially StateUnknown.
        // This path cannot turn a failed publication into accepted evidence.
        let _ = allocation.remove_file_at(&temp_path);
    }
    result
}

fn record_atomic_replacement(
    allocation: Option<&crate::StorageAllocationOperation>,
    destination: &Path,
    temporary_path: &Path,
    file: &File,
) -> Result<(), AtomicPublishError> {
    let Some(allocation) = allocation else {
        return Ok(());
    };
    let recorded = (|| {
        allocation.remove_file_at(destination)?;
        allocation.replace_file_at(destination, file)?;
        allocation.remove_file_at(temporary_path)
    })();
    // Namespace replacement has already occurred. An evidence failure must
    // preserve the publisher's post-replacement reconciliation semantics.
    recorded.map_err(|error| {
        AtomicPublishError::Replacement(graphforge_filesystem::ReplaceFileError::StateUnknown(
            std::io::Error::other(error),
        ))
    })
}

#[allow(clippy::too_many_arguments)] // Existing atomic interface plus optional diagnostic context.
pub(super) fn publish_atomic_bytes_in(
    directory: &graphforge_filesystem::StableDirectory,
    diagnostic_path: &Path,
    target_name: &std::ffi::OsStr,
    bytes: &[u8],
    after_write: impl FnOnce() -> std::io::Result<()>,
    after_sync: impl FnOnce() -> std::io::Result<()>,
    before_replace: impl FnOnce() -> std::io::Result<()>,
    allocation: Option<&crate::StorageAllocationOperation>,
) -> Result<(), AtomicPublishError> {
    let target_text = target_name
        .to_str()
        .ok_or_else(|| std::io::Error::other("atomic publication target is not UTF-8"))?;
    let temp_name = std::ffi::OsString::from(unique_atomic_temp_name(target_text));
    let mut temp = directory.create_replaceable_child_file(&temp_name)?;
    let temp_path = diagnostic_path.with_file_name(&temp_name);
    let temp_identity = graphforge_filesystem::file_identity(&temp)?;
    let publish = || -> Result<(), AtomicPublishError> {
        crate::file_lock::lock_exclusive(&temp)?;
        let written = temp.write_all(bytes);
        let observed = allocation.map_or(Ok(()), |allocation| {
            allocation.replace_file_at(&temp_path, &temp)
        });
        written?;
        observed.map_err(std::io::Error::other)?;
        after_write()?;
        temp.sync_all()?;
        if let Some(allocation) = allocation {
            allocation
                .replace_file_at(&temp_path, &temp)
                .map_err(std::io::Error::other)?;
        }
        after_sync()?;
        before_replace()?;
        let namespace_lock = lock_atomic_publish_target(diagnostic_path);
        let _namespace_guard = namespace_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        directory.replace_child(&temp_name, temp_identity, target_name)?;
        record_atomic_replacement(allocation, diagnostic_path, &temp_path, &temp)?;
        crate::file_lock::unlock(&temp)?;
        Ok(())
    };
    let result = publish();
    if result.is_err()
        && directory
            .unlink_child_if_identity(&temp_name, temp_identity)
            .is_ok()
        && let Some(allocation) = allocation
    {
        let _ = allocation.remove_file_at(&temp_path);
    }
    result
}

fn unique_atomic_temp_name(target_text: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(target_text.as_bytes());
    hasher.update(Uuid::now_v7().as_bytes());
    format!(
        ".graphforge-atomic-{}.tmp",
        hex_digest(hasher.finalize().into())
    )
}

fn lock_atomic_publish_target(path: &Path) -> Arc<Mutex<()>> {
    static LOCKS: OnceLock<Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>> = OnceLock::new();
    let mut locks = LOCKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    Arc::clone(
        locks
            .entry(path.to_path_buf())
            .or_insert_with(|| Arc::new(Mutex::new(()))),
    )
}

pub(crate) fn read_journal(path: &Path) -> Result<JournalRecord, GfError> {
    let metadata = std::fs::symlink_metadata(path).map_err(publication_io)?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() > MAX_JOURNAL_BYTES
    {
        return Err(project_error(
            ProjectErrorCode::ProjectCorrupt,
            "transaction journal is invalid",
        ));
    }
    let bytes = std::fs::read(path).map_err(publication_io)?;
    let journal: JournalRecord = serde_json::from_slice(&bytes).map_err(|_| {
        project_error(
            ProjectErrorCode::ProjectCorrupt,
            "transaction journal is not canonical JSON",
        )
    })?;
    if canonical_line(&journal)? != bytes
        || journal.format != "graphforge-transaction"
        || journal.format_version != 1
        || parse_digest(&journal.request_fingerprint).is_none()
        || journal
            .operation_fingerprint
            .as_deref()
            .is_some_and(|fingerprint| parse_digest(fingerprint).is_none())
    {
        return Err(project_error(
            ProjectErrorCode::ProjectCorrupt,
            "transaction journal is not canonical",
        ));
    }
    Ok(journal)
}

pub(crate) fn cleanup_atomicwrite_temp(path: &Path) -> Result<bool, GfError> {
    cleanup_atomicwrite_temp_with_allocation(path, None)
}

pub(crate) fn cleanup_atomicwrite_temp_with_allocation(
    path: &Path,
    allocation: Option<&crate::StorageAllocationOperation>,
) -> Result<bool, GfError> {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return Ok(false);
    };
    if let Some(digest) = name
        .strip_prefix(".graphforge-atomic-")
        .and_then(|name| name.strip_suffix(".tmp"))
        && digest.len() == 64
        && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        let path_metadata = std::fs::symlink_metadata(path).map_err(publication_io)?;
        if !path_metadata.is_file() || path_metadata.file_type().is_symlink() {
            return Ok(false);
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(publication_io)?;
        if !crate::file_lock::try_lock_exclusive(&file).map_err(publication_io)? {
            // A live publisher owns this exact temporary inode.  Recognize it
            // as protocol state, but never delete another environment's work.
            return Ok(true);
        }
        let metadata = file.metadata().map_err(publication_io)?;
        if !metadata.is_file()
            || graphforge_filesystem::file_link_count(&file).map_err(publication_io)? != 1
            || graphforge_filesystem::path_identity(path).map_err(publication_io)?
                != graphforge_filesystem::file_identity(&file).map_err(publication_io)?
        {
            let _ = crate::file_lock::unlock(&file);
            return Ok(false);
        }
        if let Some(allocation) = allocation {
            allocation.replace_file_at(path, &file)?;
        }
        std::fs::remove_file(path).map_err(publication_io)?;
        if let Some(allocation) = allocation {
            allocation.remove_file_at(path)?;
        }
        crate::file_lock::unlock(&file).map_err(publication_io)?;
        drop(file);
        sync_directory(
            path.parent()
                .expect("atomic-write temporary directory always has a parent"),
        )?;
        return Ok(true);
    }
    let Some(suffix) = name.strip_prefix(".atomicwrite") else {
        return Ok(false);
    };
    if suffix.len() != 6 || !suffix.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
        return Ok(false);
    }
    let metadata = std::fs::symlink_metadata(path).map_err(publication_io)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Ok(false);
    }
    let mut entries = std::fs::read_dir(path).map_err(publication_io)?;
    if let Some(entry) = entries.next().transpose().map_err(publication_io)? {
        if entries
            .next()
            .transpose()
            .map_err(publication_io)?
            .is_some()
            || entry.file_name() != "tmpfile.tmp"
        {
            return Ok(false);
        }
        let entry_metadata = std::fs::symlink_metadata(entry.path()).map_err(publication_io)?;
        if !entry_metadata.is_file() || entry_metadata.file_type().is_symlink() {
            return Ok(false);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if entry_metadata.nlink() != 1 {
                return Ok(false);
            }
        }
        if let Some(allocation) = allocation {
            let file = crate::project_portable::open_regular_nofollow(&entry.path())
                .map_err(publication_io)?;
            allocation.replace_file_at(&entry.path(), &file)?;
        }
        std::fs::remove_file(entry.path()).map_err(publication_io)?;
        if let Some(allocation) = allocation {
            allocation.remove_file_at(&entry.path())?;
        }
    }
    std::fs::remove_dir(path).map_err(publication_io)?;
    sync_directory(
        path.parent()
            .expect("atomic-write temporary directory always has a parent"),
    )?;
    Ok(true)
}

pub(super) fn canonical_line<T: Serialize>(value: &T) -> Result<Vec<u8>, GfError> {
    let mut bytes = serde_json::to_vec(value)
        .map_err(|error| GfError::Storage(format!("failed to encode project record: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

#[cfg(unix)]
pub(crate) fn sync_directory(path: &Path) -> Result<(), GfError> {
    let _wait = crate::concurrency_attribution::RegionScope::named("fsync");
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(publication_io)
}

#[cfg(windows)]
pub(crate) fn sync_directory(path: &Path) -> Result<(), GfError> {
    let _wait = crate::concurrency_attribution::RegionScope::named("fsync");
    use std::os::windows::fs::OpenOptionsExt;

    // FILE_FLAG_BACKUP_SEMANTICS permits opening a directory handle. The
    // resulting safe std::fs::File can then be flushed with FlushFileBuffers.
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    OpenOptions::new()
        // `File::sync_all` calls `FlushFileBuffers`, which requires a
        // write-capable directory handle on Windows.
        .write(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(publication_io)
}

#[cfg(all(not(unix), not(windows)))]
pub(crate) fn sync_directory(_path: &Path) -> Result<(), GfError> {
    Err(project_error(
        ProjectErrorCode::UnsupportedFilesystem,
        "directory durability is unsupported on this platform",
    ))
}

pub(super) fn hex_digest(bytes: [u8; 32]) -> String {
    let mut output = String::with_capacity(64);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

pub(super) fn parse_digest(value: &str) -> Option<[u8; 32]> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return None;
    }
    let mut digest = [0u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = hex_nibble(pair[0])?;
        let low = hex_nibble(pair[1])?;
        digest[index] = (high << 4) | low;
    }
    Some(digest)
}

const fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests;
