//! Control for atomic project publication.

use super::{
    Deserialize, File, GfError, MAX_JOURNAL_BYTES, OpenOptions, Path, ProjectErrorCode,
    ProjectGenerationRequest, Read, RevertJournalExtension, Serialize, StagedParticipant, Uuid,
    project_error, project_failpoint, publication_io,
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
        .map_err(publication_io)
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

#[cfg(test)]
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
    crate::durable_commit::publish_atomic(
        path,
        bytes,
        crate::durable_commit::AtomicHooks {
            after_write,
            after_seal: after_sync,
            before_visible: before_replace,
        },
        allocation,
    )
    .map_err(|error| AtomicPublishError::from(error.cause))
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)] // Existing compatibility hooks preserve project failpoint order.
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
    directory.revalidate_named()?;
    let retained = graphforge_filesystem::StableDirectory::open(directory.path())?;
    if retained.identity() != directory.identity()
        || diagnostic_path.parent() != Some(directory.path())
    {
        return Err(std::io::Error::other("atomic publication directory authority changed").into());
    }
    crate::durable_commit::publish_atomic_in(
        retained,
        target_name,
        bytes,
        crate::durable_commit::AtomicHooks {
            after_write,
            after_seal: after_sync,
            before_visible: before_replace,
        },
        allocation,
    )
    .and_then(|pending| pending.acknowledge(allocation))
    .map_err(|error| AtomicPublishError::from(error.cause))
}

impl From<crate::durable_commit::CommitCause> for AtomicPublishError {
    fn from(cause: crate::durable_commit::CommitCause) -> Self {
        match cause {
            crate::durable_commit::CommitCause::Io(error) => Self::from(error),
            crate::durable_commit::CommitCause::Replacement(error) => Self::Replacement(error),
        }
    }
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
        return cleanup_locked_atomic_temporary(path, allocation);
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
        let child = graphforge_filesystem::StableDirectory::open(path).map_err(publication_io)?;
        let mut retirement =
            crate::durable_commit::RetirementBatch::new(&child).map_err(publication_io)?;
        retirement
            .unlink(
                &entry.file_name(),
                graphforge_filesystem::path_identity(&entry.path()).map_err(publication_io)?,
            )
            .map_err(publication_io)?;
        // This private child directory is removed below; its parent is the
        // durable retirement boundary, so do not add an intermediate fence.
        drop(retirement);
        drop(child);
        if let Some(allocation) = allocation {
            allocation.remove_file_at(&entry.path())?;
        }
    }
    let parent = graphforge_filesystem::StableDirectory::open(
        path.parent()
            .expect("atomic-write temporary directory has a parent"),
    )
    .map_err(publication_io)?;
    crate::durable_commit::retire_directory(
        &parent,
        path.file_name()
            .expect("atomic-write temporary directory has a name"),
        graphforge_filesystem::path_identity(path).map_err(publication_io)?,
    )
    .map_err(publication_io)?;
    Ok(true)
}

fn cleanup_locked_atomic_temporary(
    path: &Path,
    allocation: Option<&crate::StorageAllocationOperation>,
) -> Result<bool, GfError> {
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
    let parent = graphforge_filesystem::StableDirectory::open(
        path.parent().expect("atomic temporary has a parent"),
    )
    .map_err(publication_io)?;
    let mut retirement =
        crate::durable_commit::RetirementBatch::new(&parent).map_err(publication_io)?;
    retirement
        .unlink(
            path.file_name().expect("atomic temporary has a name"),
            graphforge_filesystem::file_identity(&file).map_err(publication_io)?,
        )
        .map_err(publication_io)?;
    if let Some(allocation) = allocation {
        allocation.remove_file_at(path)?;
    }
    crate::file_lock::unlock(&file).map_err(publication_io)?;
    drop(file);
    retirement.acknowledge().map_err(publication_io)?;
    Ok(true)
}

pub(super) fn canonical_line<T: Serialize>(value: &T) -> Result<Vec<u8>, GfError> {
    let mut bytes = serde_json::to_vec(value)
        .map_err(|error| GfError::Storage(format!("failed to encode project record: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

pub(crate) fn sync_directory(path: &Path) -> Result<(), GfError> {
    crate::durable_commit::sync_directory(path).map_err(publication_io)
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
