//! Atomic publication of complete immutable project generations.
//!
//! This module owns only the transaction protocol. Participant schemas and
//! domain semantics remain in their owning crates and enter validation through
//! opaque callbacks.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use atomicwrites::{AllowOverwrite, AtomicFile};
use graphforge_core::{GfError, ProjectErrorCode};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::project_failpoint;
use crate::project_generation::{
    CURRENT_FILE, ResolvedProjectGeneration, resolve_project_generation,
};

pub(crate) const LOCKS_DIR: &str = "locks";
pub(crate) const WRITER_LOCK_FILE: &str = "writer.lock";
pub(crate) const TRANSACTION_LOCKS_DIR: &str = "transactions";
pub(crate) const TRANSACTIONS_DIR: &str = "transactions";
pub(crate) const ATTEMPTS_DIR: &str = "attempts";
pub(crate) const GENERATIONS_DIR: &str = "generations";
const PARTICIPANTS_DIR: &str = "participants";
const LEASE_FILE: &str = "lease.lock";
const MANIFEST_FILE: &str = "manifest.json";
const MAX_JOURNAL_BYTES: u64 = 1024 * 1024;

/// Persisted participant encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ProjectParticipantEncoding {
    /// Apache Parquet.
    Parquet,
    /// Arrow IPC file format.
    Arrow,
    /// Canonical JSON.
    Json,
}

impl ProjectParticipantEncoding {
    const fn extension(self) -> &'static str {
        match self {
            Self::Parquet => "parquet",
            Self::Arrow => "arrow",
            Self::Json => "json",
        }
    }
}

/// Immutable bytes and contract metadata for one generation participant.
#[derive(Debug, Clone)]
pub struct ProjectParticipant {
    /// Stable capability ID.
    pub capability_id: String,
    /// Capability contract version.
    pub capability_version: u32,
    /// Stable record-family ID.
    pub record_family_id: String,
    /// Record contract version.
    pub record_version: u32,
    /// Persisted encoding.
    pub encoding: ProjectParticipantEncoding,
    /// Canonical Arrow/schema fingerprint.
    pub schema_fingerprint: [u8; 32],
    /// Logical row count.
    pub row_count: u64,
    /// Exact persisted bytes.
    pub bytes: Vec<u8>,
}

/// One capability declaration for a complete replacement generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProjectCapability {
    /// Stable capability ID.
    pub capability_id: String,
    /// Positive capability contract version.
    pub capability_version: u32,
}

/// Complete immutable input to one publication attempt.
///
/// Callers must include every participant that the resulting generation will
/// expose, including unchanged participants copied from the parent. Omission
/// means absence; publication never merges an incomplete request with the
/// parent generation.
#[derive(Debug, Clone)]
pub struct ProjectGenerationRequest {
    /// Caller-stable idempotency identity.
    pub transaction_uuid: Uuid,
    /// UUID of the generation to publish.
    pub generation_uuid: Uuid,
    /// Complete manifest-declared capability set.
    pub capabilities: Vec<ProjectCapability>,
    /// Complete participant set.
    pub participants: Vec<ProjectParticipant>,
}

/// Safe participant metadata available to domain validators.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedParticipant {
    /// Stable capability ID.
    pub capability_id: String,
    /// Capability contract version.
    pub capability_version: u32,
    /// Stable record-family ID.
    pub record_family_id: String,
    /// Record contract version.
    pub record_version: u32,
    /// Machine-derived relative path.
    pub relative_path: String,
    /// Persisted encoding.
    pub encoding: String,
    /// Exact byte length.
    pub byte_length: u64,
    /// Logical row count.
    pub row_count: u64,
    /// Canonical schema fingerprint.
    pub schema_fingerprint: String,
    /// SHA-256 over exact persisted bytes.
    pub content_sha256: String,
}

/// Durable publication result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectPublicationReceipt {
    /// Transaction UUID.
    pub transaction_uuid: Uuid,
    /// Published generation UUID.
    pub generation_uuid: Uuid,
    /// Digest over exact generation-manifest bytes.
    pub generation_manifest_sha256: [u8; 32],
    /// Whether an already-published identical transaction was replayed.
    pub idempotent_replay: bool,
}

/// Result of the stage operation.
pub enum ProjectStageOutcome {
    /// New private generation staged under its required publication locks.
    Staged(Box<StagedProjectGeneration>),
    /// The transaction and identical immutable inputs were already published.
    AlreadyPublished(ProjectPublicationReceipt),
}

/// A staged generation that still requires domain and composite validation.
pub struct StagedProjectGeneration {
    root: PathBuf,
    publication_lock: PublicationLock,
    parent: ResolvedProjectGeneration,
    transaction_uuid: Uuid,
    generation_uuid: Uuid,
    generation_root: PathBuf,
    requires_promotion: bool,
    request_fingerprint: String,
    operation_fingerprint: String,
    capabilities: Vec<ProjectCapability>,
    participants: Vec<StagedParticipant>,
    revert: Option<RevertJournalExtension>,
}

enum PublicationLock {
    Exclusive(File),
    Optimistic(File),
}

/// Canonical revert metadata persisted in every ADR 0015 journal phase.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RevertJournalExtension {
    pub(crate) operation_uuid: String,
    pub(crate) request_sha256: String,
    pub(crate) checkpoint_uuid: String,
    pub(crate) checkpoint_name: String,
    pub(crate) source_generation_uuid: String,
    pub(crate) source_manifest_sha256: String,
    pub(crate) prior_current_generation_uuid: String,
    pub(crate) restored_at: i64,
    pub(crate) reason: String,
    pub(crate) actor_uuid: Option<String>,
    pub(crate) restoration_uuid: String,
    pub(crate) registry_revision: u64,
}

/// A generation whose participant bytes and domain contracts were validated.
pub struct ValidatedProjectGeneration(StagedProjectGeneration);

/// Stage every participant of one private immutable generation.
///
/// The default writer-lock acquisition is non-blocking. This function writes
/// no participant until it owns that lock and has resolved the complete parent
/// generation.
///
/// # Errors
/// Returns a stable project error for a busy writer, malformed participant,
/// conflicting transaction replay, corrupt parent, or I/O failure.
pub fn stage_project_generation(
    container_root: impl AsRef<Path>,
    request: &ProjectGenerationRequest,
) -> Result<ProjectStageOutcome, GfError> {
    stage_project_generation_inner(container_root.as_ref(), request)
        .map_err(|error| map_stage_error(request, error))
}

/// Stage a complete private generation while allowing other transaction
/// identities to stage against the same committed parent.
///
/// A transaction-scoped kernel lock prevents two live attempts for the same
/// logical operation. Publication later acquires the global writer lock and
/// compares the pinned parent with `CURRENT`. `operation_fingerprint` is stable
/// across rebase attempts even though carried-forward parent participant bytes
/// may change.
///
/// # Errors
/// Returns a stable busy, idempotency, validation, corruption, or storage error.
pub fn stage_project_generation_optimistic(
    container_root: impl AsRef<Path>,
    request: &ProjectGenerationRequest,
    operation_fingerprint: [u8; 32],
) -> Result<ProjectStageOutcome, GfError> {
    stage_project_generation_optimistic_inner(
        container_root.as_ref(),
        request,
        operation_fingerprint,
    )
    .map_err(|error| map_stage_error(request, error))
}

fn map_stage_error(request: &ProjectGenerationRequest, error: GfError) -> GfError {
    match error {
        GfError::Storage(message) => publication_error(request, "STAGE", false, &message),
        other => other,
    }
}

fn stage_project_generation_inner(
    container_root: &Path,
    request: &ProjectGenerationRequest,
) -> Result<ProjectStageOutcome, GfError> {
    let root = canonical_supported_root(container_root)?;
    let writer_lock = acquire_writer_lock(&root, request)?;
    project_failpoint::hit(
        "project.after_writer_lock",
        Some(request.transaction_uuid),
        Some(request.generation_uuid),
        "WRITER_LOCK",
        false,
    )?;
    let parent = resolve_project_generation(&root)?;
    stage_project_generation_with_lock(root, writer_lock, parent, request, None)
}

fn stage_project_generation_optimistic_inner(
    container_root: &Path,
    request: &ProjectGenerationRequest,
    operation_fingerprint: [u8; 32],
) -> Result<ProjectStageOutcome, GfError> {
    let root = canonical_supported_root(container_root)?;
    let transaction_lock = acquire_transaction_lock(&root, request)?;
    let parent = resolve_project_generation(&root)?;
    stage_project_generation_inner_with_locks(
        root,
        PublicationLock::Optimistic(transaction_lock),
        parent,
        request,
        None,
        Some(operation_fingerprint),
    )
}

/// Stage a generation using a writer lock and parent resolved by a composed
/// storage operation such as complete-workspace checkpoint revert.
pub(crate) fn stage_project_generation_with_lock(
    root: PathBuf,
    writer_lock: File,
    parent: ResolvedProjectGeneration,
    request: &ProjectGenerationRequest,
    revert: Option<RevertJournalExtension>,
) -> Result<ProjectStageOutcome, GfError> {
    stage_project_generation_inner_with_locks(
        root,
        PublicationLock::Exclusive(writer_lock),
        parent,
        request,
        revert,
        None,
    )
}

fn stage_project_generation_inner_with_locks(
    root: PathBuf,
    publication_lock: PublicationLock,
    parent: ResolvedProjectGeneration,
    request: &ProjectGenerationRequest,
    revert: Option<RevertJournalExtension>,
    operation_fingerprint: Option<[u8; 32]>,
) -> Result<ProjectStageOutcome, GfError> {
    validate_request(request)?;
    let (capabilities, participants, request_fingerprint) = request_metadata(request)?;
    let operation_fingerprint =
        operation_fingerprint.map_or_else(|| request_fingerprint.clone(), hex_digest);
    let transactions_dir = ensure_machine_directory(&root, Path::new(TRANSACTIONS_DIR))?;
    sync_directory(&root)?;
    let journal_path =
        transactions_dir.join(format!("{}.json", request.transaction_uuid.hyphenated()));
    if journal_path.exists()
        && let Some(outcome) = handle_existing_journal(
            &root,
            request,
            &request_fingerprint,
            &operation_fingerprint,
            revert.as_ref(),
            &journal_path,
        )?
    {
        return Ok(outcome);
    }

    let requires_promotion = matches!(publication_lock, PublicationLock::Optimistic(_));
    let generation_root =
        prepare_generation_directory(&root, request, &request_fingerprint, requires_promotion)?;
    write_journal(
        &journal_path,
        &JournalRecord::new(
            request,
            Some(parent.generation_uuid()),
            JournalPhase::Preparing,
            (request_fingerprint.clone(), operation_fingerprint.clone()),
            &participants,
            None,
            revert.clone(),
        ),
    )?;
    project_failpoint::hit(
        "project.after_journal_preparing",
        Some(request.transaction_uuid),
        Some(request.generation_uuid),
        "PREPARING",
        false,
    )?;

    stage_participant_files(request, &generation_root, &participants)?;
    sync_participant_directories(&generation_root.join(PARTICIPANTS_DIR), &participants)?;
    project_failpoint::hit(
        "project.after_participant_dir_fsync",
        Some(request.transaction_uuid),
        Some(request.generation_uuid),
        "STAGED",
        false,
    )?;
    write_journal(
        &journal_path,
        &JournalRecord::new(
            request,
            Some(parent.generation_uuid()),
            JournalPhase::Staged,
            (request_fingerprint.clone(), operation_fingerprint.clone()),
            &participants,
            None,
            revert.clone(),
        ),
    )?;
    project_failpoint::hit(
        "project.after_journal_staged",
        Some(request.transaction_uuid),
        Some(request.generation_uuid),
        "STAGED",
        false,
    )?;

    Ok(ProjectStageOutcome::Staged(Box::new(
        StagedProjectGeneration {
            root,
            publication_lock,
            parent,
            transaction_uuid: request.transaction_uuid,
            generation_uuid: request.generation_uuid,
            generation_root,
            requires_promotion,
            request_fingerprint,
            operation_fingerprint,
            capabilities,
            participants,
            revert,
        },
    )))
}

fn acquire_writer_lock(root: &Path, request: &ProjectGenerationRequest) -> Result<File, GfError> {
    acquire_writer_lock_for_parts(root, request.transaction_uuid, request.generation_uuid)
}

fn acquire_writer_lock_for_parts(
    root: &Path,
    transaction_uuid: Uuid,
    generation_uuid: Uuid,
) -> Result<File, GfError> {
    let lock_dir = ensure_machine_directory(root, Path::new(LOCKS_DIR))?;
    sync_directory(root)?;
    let writer_lock = open_regular_lock(&lock_dir.join(WRITER_LOCK_FILE))?;
    if !crate::file_lock::try_lock_exclusive(&writer_lock).map_err(publication_io)? {
        return Err(project_error(
            ProjectErrorCode::WriterBusy,
            format!(
                "transaction_uuid={} generation_uuid={} phase=WRITER_LOCK committed=false cause=busy",
                transaction_uuid.hyphenated(),
                generation_uuid.hyphenated()
            ),
        ));
    }
    Ok(writer_lock)
}

fn wait_for_writer_lock(root: &Path) -> Result<File, GfError> {
    let lock_dir = ensure_machine_directory(root, Path::new(LOCKS_DIR))?;
    sync_directory(root)?;
    let writer_lock = open_regular_lock(&lock_dir.join(WRITER_LOCK_FILE))?;
    crate::file_lock::lock_exclusive(&writer_lock).map_err(publication_io)?;
    Ok(writer_lock)
}

fn acquire_transaction_lock(
    root: &Path,
    request: &ProjectGenerationRequest,
) -> Result<File, GfError> {
    let lock = open_transaction_lock(root, request.transaction_uuid)?;
    if !crate::file_lock::try_lock_exclusive(&lock).map_err(publication_io)? {
        return Err(project_error(
            ProjectErrorCode::WriterBusy,
            format!(
                "transaction_uuid={} generation_uuid={} phase=TRANSACTION_LOCK committed=false cause=busy",
                request.transaction_uuid.hyphenated(),
                request.generation_uuid.hyphenated()
            ),
        ));
    }
    Ok(lock)
}

pub(crate) fn open_transaction_lock(root: &Path, transaction_uuid: Uuid) -> Result<File, GfError> {
    let lock_dir =
        ensure_machine_directory(root, &Path::new(LOCKS_DIR).join(TRANSACTION_LOCKS_DIR))?;
    open_regular_lock(&lock_dir.join(format!("{}.lock", transaction_uuid.hyphenated())))
}

fn handle_existing_journal(
    root: &Path,
    request: &ProjectGenerationRequest,
    request_fingerprint: &str,
    operation_fingerprint: &str,
    expected_revert: Option<&RevertJournalExtension>,
    journal_path: &Path,
) -> Result<Option<ProjectStageOutcome>, GfError> {
    let journal = read_journal(journal_path)?;
    if journal.operation_fingerprint() != operation_fingerprint
        || journal.generation_uuid != request.generation_uuid.hyphenated().to_string()
        || journal.revert.as_ref() != expected_revert
    {
        return Err(transaction_conflict(request));
    }
    if journal.phase == JournalPhase::Aborted {
        let generation_name = request.generation_uuid.hyphenated().to_string();
        if root.join(GENERATIONS_DIR).join(&generation_name).exists()
            || root.join("trash").join(&generation_name).exists()
        {
            return Err(publication_error(
                request,
                "ABORTED",
                false,
                "aborted transaction cleanup is incomplete; run recovery again",
            ));
        }
        cleanup_aborted_attempts(root, request.transaction_uuid)?;
        return Ok(None);
    }
    if journal.request_fingerprint != request_fingerprint
        && journal.phase != JournalPhase::Published
    {
        return Err(transaction_conflict(request));
    }
    if journal.phase != JournalPhase::Published {
        return Err(publication_error(
            request,
            "PREPARING",
            false,
            "an interrupted transaction requires recovery",
        ));
    }
    let digest = journal
        .generation_manifest_sha256
        .as_deref()
        .and_then(parse_digest)
        .ok_or_else(|| {
            project_error(
                ProjectErrorCode::ProjectCorrupt,
                "published transaction journal has no valid manifest digest",
            )
        })?;
    let manifest_path = root
        .join(GENERATIONS_DIR)
        .join(request.generation_uuid.hyphenated().to_string())
        .join(MANIFEST_FILE);
    let manifest_bytes = std::fs::read(&manifest_path).map_err(publication_io)?;
    let actual: [u8; 32] = Sha256::digest(&manifest_bytes).into();
    if actual != digest {
        return Err(project_error(
            ProjectErrorCode::ProjectCorrupt,
            "published transaction manifest does not match its journal",
        ));
    }
    Ok(Some(ProjectStageOutcome::AlreadyPublished(
        ProjectPublicationReceipt {
            transaction_uuid: request.transaction_uuid,
            generation_uuid: request.generation_uuid,
            generation_manifest_sha256: digest,
            idempotent_replay: true,
        },
    )))
}

fn cleanup_aborted_attempts(root: &Path, transaction_uuid: Uuid) -> Result<(), GfError> {
    let attempts_root = root.join(ATTEMPTS_DIR);
    let transaction_root = attempts_root.join(transaction_uuid.hyphenated().to_string());
    let metadata = match std::fs::symlink_metadata(&transaction_root) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(publication_io(error)),
    };
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(project_error(
            ProjectErrorCode::ProjectCorrupt,
            "aborted transaction attempt path is linked or not a directory",
        ));
    }
    std::fs::remove_dir_all(&transaction_root).map_err(publication_io)?;
    sync_directory(&attempts_root)
}

/// Load the canonical revert extension for direct idempotent replay lookup.
pub(crate) fn load_revert_journal_extension(
    root: &Path,
    transaction_uuid: Uuid,
) -> Result<Option<RevertJournalExtension>, GfError> {
    let path = root
        .join(TRANSACTIONS_DIR)
        .join(format!("{}.json", transaction_uuid.hyphenated()));
    if !path.exists() {
        return Ok(None);
    }
    Ok(read_journal(&path)?.revert)
}

/// Load a completed revert publication directly from its durable journal.
pub(crate) fn load_published_revert(
    root: &Path,
    transaction_uuid: Uuid,
) -> Result<Option<(RevertJournalExtension, ProjectPublicationReceipt)>, GfError> {
    let path = root
        .join(TRANSACTIONS_DIR)
        .join(format!("{}.json", transaction_uuid.hyphenated()));
    if !path.exists() {
        return Ok(None);
    }
    let journal = read_journal(&path)?;
    let Some(revert) = journal.revert else {
        return Ok(None);
    };
    if journal.phase != JournalPhase::Published {
        return Ok(None);
    }
    let generation_uuid = Uuid::parse_str(&journal.generation_uuid).map_err(|_| {
        project_error(
            ProjectErrorCode::ProjectCorrupt,
            "published journal has an invalid generation UUID",
        )
    })?;
    let digest = journal
        .generation_manifest_sha256
        .as_deref()
        .and_then(parse_digest)
        .ok_or_else(|| {
            project_error(
                ProjectErrorCode::ProjectCorrupt,
                "published journal has an invalid manifest digest",
            )
        })?;
    Ok(Some((
        revert,
        ProjectPublicationReceipt {
            transaction_uuid,
            generation_uuid,
            generation_manifest_sha256: digest,
            idempotent_replay: true,
        },
    )))
}

/// Load a completed project publication directly from its durable journal.
///
/// This is a read-only idempotency probe. It verifies the published manifest
/// bytes before returning a receipt and never acquires the writer lock.
pub fn published_project_transaction(
    root: &Path,
    transaction_uuid: Uuid,
) -> Result<Option<ProjectPublicationReceipt>, GfError> {
    let path = root
        .join(TRANSACTIONS_DIR)
        .join(format!("{}.json", transaction_uuid.hyphenated()));
    if !path.exists() {
        return Ok(None);
    }
    let journal = read_journal(&path)?;
    if journal.phase != JournalPhase::Published {
        return Ok(None);
    }
    let generation_uuid = Uuid::parse_str(&journal.generation_uuid).map_err(|_| {
        project_error(
            ProjectErrorCode::ProjectCorrupt,
            "published journal has an invalid generation UUID",
        )
    })?;
    let digest = journal
        .generation_manifest_sha256
        .as_deref()
        .and_then(parse_digest)
        .ok_or_else(|| {
            project_error(
                ProjectErrorCode::ProjectCorrupt,
                "published journal has an invalid manifest digest",
            )
        })?;
    let manifest_path = root
        .join(GENERATIONS_DIR)
        .join(generation_uuid.hyphenated().to_string())
        .join(MANIFEST_FILE);
    let actual: [u8; 32] =
        Sha256::digest(std::fs::read(manifest_path).map_err(publication_io)?).into();
    if actual != digest {
        return Err(project_error(
            ProjectErrorCode::ProjectCorrupt,
            "published transaction manifest does not match its journal",
        ));
    }
    Ok(Some(ProjectPublicationReceipt {
        transaction_uuid,
        generation_uuid,
        generation_manifest_sha256: digest,
        idempotent_replay: true,
    }))
}

fn prepare_generation_directory(
    root: &Path,
    request: &ProjectGenerationRequest,
    request_fingerprint: &str,
    requires_promotion: bool,
) -> Result<PathBuf, GfError> {
    let relative_root = if requires_promotion {
        Path::new(ATTEMPTS_DIR)
            .join(request.transaction_uuid.hyphenated().to_string())
            .join(request_fingerprint)
    } else {
        Path::new(GENERATIONS_DIR).join(request.generation_uuid.hyphenated().to_string())
    };
    let generation_root = root.join(&relative_root);
    if generation_root.exists() {
        return Err(transaction_conflict(request));
    }
    ensure_machine_directory(root, &relative_root.join(PARTICIPANTS_DIR))?;
    Ok(generation_root)
}

fn stage_participant_files(
    request: &ProjectGenerationRequest,
    generation_root: &Path,
    participants: &[StagedParticipant],
) -> Result<(), GfError> {
    for metadata in participants {
        let input = request
            .participants
            .iter()
            .find(|candidate| {
                candidate.capability_id == metadata.capability_id
                    && candidate.record_family_id == metadata.record_family_id
            })
            .expect("validated canonical metadata has one source participant");
        let destination = generation_root
            .join(PARTICIPANTS_DIR)
            .join(&metadata.relative_path);
        let parent_dir = destination
            .parent()
            .expect("machine-derived participant path has a parent");
        let relative_parent = parent_dir
            .strip_prefix(generation_root)
            .expect("machine-derived participant parent is contained");
        ensure_machine_directory(generation_root, relative_parent)?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&destination)
            .map_err(publication_io)?;
        file.write_all(&input.bytes).map_err(publication_io)?;
        project_failpoint::hit(
            "project.after_participant_write",
            Some(request.transaction_uuid),
            Some(request.generation_uuid),
            "STAGED",
            false,
        )?;
        file.sync_all().map_err(publication_io)?;
        project_failpoint::hit(
            "project.after_participant_fsync",
            Some(request.transaction_uuid),
            Some(request.generation_uuid),
            "STAGED",
            false,
        )?;
        verify_participant_file(&destination, metadata)?;
    }
    Ok(())
}

impl StagedProjectGeneration {
    /// Safe staged metadata in canonical manifest order.
    #[must_use]
    pub fn participants(&self) -> &[StagedParticipant] {
        &self.participants
    }

    /// Run domain-local and composite validation without rereading `CURRENT`.
    ///
    /// # Errors
    /// Returns the validator error without publishing the private generation.
    pub fn validate<D, C>(
        self,
        domain_validation: D,
        composite_validation: C,
    ) -> Result<ValidatedProjectGeneration, GfError>
    where
        D: FnOnce(&[StagedParticipant]) -> Result<(), GfError>,
        C: FnOnce(&ResolvedProjectGeneration, &[StagedParticipant]) -> Result<(), GfError>,
    {
        for participant in &self.participants {
            verify_participant_file(
                &self
                    .generation_root
                    .join(PARTICIPANTS_DIR)
                    .join(&participant.relative_path),
                participant,
            )?;
        }
        domain_validation(&self.participants)?;
        project_failpoint::hit(
            "project.after_domain_validation",
            Some(self.transaction_uuid),
            Some(self.generation_uuid),
            "VALIDATED",
            false,
        )?;
        if let Err(error) = composite_validation(&self.parent, &self.participants) {
            // An optimistic caller uses `GF_WRITE_CONFLICT` to request a rebase
            // after CURRENT changes between staging and composite validation.
            // Retaining that private attempt would make the retry collide with
            // its own non-published journal even though the logical operation
            // identity is unchanged. Abort only that transaction-owned attempt
            // before returning the stable conflict to the caller.
            if self.requires_promotion && error.code() == "GF_WRITE_CONFLICT" {
                abort_stale_generation(&self)?;
            }
            return Err(error);
        }
        project_failpoint::hit(
            "project.after_composite_validation",
            Some(self.transaction_uuid),
            Some(self.generation_uuid),
            "VALIDATED",
            false,
        )?;
        write_journal(
            &self.journal_path(),
            &self.journal(JournalPhase::Validated, None),
        )?;
        project_failpoint::hit(
            "project.after_journal_validated",
            Some(self.transaction_uuid),
            Some(self.generation_uuid),
            "VALIDATED",
            false,
        )?;
        Ok(ValidatedProjectGeneration(self))
    }

    fn journal_path(&self) -> PathBuf {
        self.root
            .join(TRANSACTIONS_DIR)
            .join(format!("{}.json", self.transaction_uuid.hyphenated()))
    }

    fn journal(&self, phase: JournalPhase, manifest_sha256: Option<String>) -> JournalRecord {
        JournalRecord {
            format: "graphforge-transaction".into(),
            format_version: 1,
            transaction_uuid: self.transaction_uuid.hyphenated().to_string(),
            generation_uuid: self.generation_uuid.hyphenated().to_string(),
            parent_generation_uuid: Some(self.parent.generation_uuid().hyphenated().to_string()),
            phase,
            request_fingerprint: self.request_fingerprint.clone(),
            operation_fingerprint: Some(self.operation_fingerprint.clone()),
            participant_paths: self
                .participants
                .iter()
                .map(|participant| participant.relative_path.clone())
                .collect(),
            generation_manifest_sha256: manifest_sha256,
            revert: self.revert.clone(),
        }
    }
}

impl ValidatedProjectGeneration {
    /// Durably install the generation, then atomically replace `CURRENT`.
    ///
    /// # Errors
    /// Returns a stable publication error whose diagnostic states whether the
    /// commit point was crossed.
    pub fn publish(self) -> Result<ProjectPublicationReceipt, GfError> {
        let commit_lock = self.prepare_commit_lock()?;
        let result = self.publish_inner().map_err(|error| {
            if matches!(error, GfError::Project { .. }) {
                error
            } else {
                publication_error_from_parts(
                    self.0.transaction_uuid,
                    self.0.generation_uuid,
                    "DURABLE",
                    false,
                    &error.to_string(),
                )
            }
        });
        drop(commit_lock);
        result
    }

    fn prepare_commit_lock(&self) -> Result<Option<File>, GfError> {
        if matches!(self.0.publication_lock, PublicationLock::Exclusive(_)) {
            return Ok(None);
        }
        let staged = &self.0;
        let writer_lock = wait_for_writer_lock(&staged.root)?;
        project_failpoint::hit(
            "project.after_optimistic_commit_lock",
            Some(staged.transaction_uuid),
            Some(staged.generation_uuid),
            "COMMIT_LOCK",
            false,
        )?;
        let current = resolve_project_generation(&staged.root)?;
        if current.generation_uuid() != staged.parent.generation_uuid() {
            abort_stale_generation(staged)?;
            return Err(project_error(
                ProjectErrorCode::WriteConflict,
                format!(
                    "transaction_uuid={} generation_uuid={} phase=COMMIT_LOCK committed=false cause=stale_parent expected_parent={} actual_parent={}",
                    staged.transaction_uuid.hyphenated(),
                    staged.generation_uuid.hyphenated(),
                    staged.parent.generation_uuid().hyphenated(),
                    current.generation_uuid().hyphenated()
                ),
            ));
        }
        Ok(Some(writer_lock))
    }

    fn publish_inner(&self) -> Result<ProjectPublicationReceipt, GfError> {
        let staged = &self.0;
        let manifest_sha256 = make_generation_durable(staged)?;
        replace_current(staged, manifest_sha256)?;
        finish_published_generation(staged, manifest_sha256)?;
        Ok(ProjectPublicationReceipt {
            transaction_uuid: staged.transaction_uuid,
            generation_uuid: staged.generation_uuid,
            generation_manifest_sha256: manifest_sha256,
            idempotent_replay: false,
        })
    }
}

fn abort_stale_generation(staged: &StagedProjectGeneration) -> Result<(), GfError> {
    write_journal(
        &staged.journal_path(),
        &staged.journal(JournalPhase::Aborted, None),
    )?;
    if staged.generation_root.exists() {
        std::fs::remove_dir_all(&staged.generation_root).map_err(publication_io)?;
        sync_directory(
            staged
                .generation_root
                .parent()
                .expect("machine attempt path has a parent"),
        )?;
    }
    Ok(())
}

fn make_generation_durable(staged: &StagedProjectGeneration) -> Result<[u8; 32], GfError> {
    let lease_path = staged.generation_root.join(LEASE_FILE);
    let lease = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&lease_path)
        .map_err(publication_io)?;
    lease.sync_all().map_err(publication_io)?;
    // Windows rejects a parent-directory rename while a descendant file handle
    // is still live. The transaction lock, not this newly created lease file,
    // owns the staged attempt, so release the handle after its durability sync.
    drop(lease);

    let manifest = GenerationManifestRecord {
        format: "graphforge-generation".into(),
        format_version: 1,
        generation_uuid: staged.generation_uuid.hyphenated().to_string(),
        parent_generation_uuid: Some(staged.parent.generation_uuid().hyphenated().to_string()),
        transaction_uuid: staged.transaction_uuid.hyphenated().to_string(),
        capabilities: staged
            .capabilities
            .iter()
            .map(|capability| CapabilityRecord {
                capability_id: capability.capability_id.clone(),
                capability_version: capability.capability_version,
            })
            .collect(),
        participants: staged.participants.clone(),
    };
    let manifest_bytes = canonical_line(&manifest)?;
    let manifest_path = staged.generation_root.join(MANIFEST_FILE);
    let manifest_file = write_new(&manifest_path, &manifest_bytes)?;
    project_failpoint::hit(
        "project.after_manifest_write",
        Some(staged.transaction_uuid),
        Some(staged.generation_uuid),
        "DURABLE",
        false,
    )?;
    manifest_file.sync_all().map_err(publication_io)?;
    // Optimistic publication promotes the complete staging directory below.
    // Close the manifest handle before that rename for Windows parity.
    drop(manifest_file);
    project_failpoint::hit(
        "project.after_manifest_fsync",
        Some(staged.transaction_uuid),
        Some(staged.generation_uuid),
        "DURABLE",
        false,
    )?;
    let manifest_sha256: [u8; 32] = Sha256::digest(&manifest_bytes).into();
    for participant in &staged.participants {
        verify_participant_file(
            &staged
                .generation_root
                .join(PARTICIPANTS_DIR)
                .join(&participant.relative_path),
            participant,
        )?;
    }
    verify_exact_file(&manifest_path, &manifest_bytes)?;
    sync_participant_directories(
        &staged.generation_root.join(PARTICIPANTS_DIR),
        &staged.participants,
    )?;
    sync_directory(&staged.generation_root)?;
    project_failpoint::hit(
        "project.after_generation_dir_fsync",
        Some(staged.transaction_uuid),
        Some(staged.generation_uuid),
        "DURABLE",
        false,
    )?;
    if staged.requires_promotion {
        promote_optimistic_generation(staged)?;
    } else {
        sync_directory(&staged.root.join(GENERATIONS_DIR))?;
    }
    write_journal(
        &staged.journal_path(),
        &staged.journal(JournalPhase::Durable, Some(hex_digest(manifest_sha256))),
    )?;
    project_failpoint::hit(
        "project.after_journal_durable",
        Some(staged.transaction_uuid),
        Some(staged.generation_uuid),
        "DURABLE",
        false,
    )?;
    Ok(manifest_sha256)
}

fn promote_optimistic_generation(staged: &StagedProjectGeneration) -> Result<(), GfError> {
    let generations_root = staged.root.join(GENERATIONS_DIR);
    let destination = generations_root.join(staged.generation_uuid.hyphenated().to_string());
    if destination.exists() {
        return Err(project_error(
            ProjectErrorCode::TransactionConflict,
            format!(
                "transaction_uuid={} generation_uuid={} phase=PROMOTE committed=false cause=generation_exists",
                staged.transaction_uuid.hyphenated(),
                staged.generation_uuid.hyphenated()
            ),
        ));
    }
    std::fs::rename(&staged.generation_root, &destination).map_err(publication_io)?;
    let transaction_attempt_root = staged
        .generation_root
        .parent()
        .expect("machine attempt path has a parent");
    sync_directory(transaction_attempt_root)?;
    std::fs::remove_dir(transaction_attempt_root).map_err(publication_io)?;
    sync_directory(&staged.root.join(ATTEMPTS_DIR))?;
    sync_directory(&generations_root)?;
    project_failpoint::hit(
        "project.after_optimistic_promotion",
        Some(staged.transaction_uuid),
        Some(staged.generation_uuid),
        "DURABLE",
        false,
    )
}

fn replace_current(
    staged: &StagedProjectGeneration,
    manifest_sha256: [u8; 32],
) -> Result<(), GfError> {
    let current = CurrentRecord {
        format: "graphforge-project".into(),
        format_version: 1,
        generation_uuid: staged.generation_uuid.hyphenated().to_string(),
        generation_manifest_sha256: hex_digest(manifest_sha256),
    };
    let current_bytes = canonical_line(&current)?;
    let current_path = staged.root.join(CURRENT_FILE);
    AtomicFile::new(&current_path, AllowOverwrite)
        .write(|file| {
            file.write_all(&current_bytes)?;
            failpoint_as_io(
                "project.after_current_temp_write",
                staged.transaction_uuid,
                staged.generation_uuid,
                "CURRENT",
                false,
            )?;
            file.sync_all()?;
            failpoint_as_io(
                "project.after_current_temp_fsync",
                staged.transaction_uuid,
                staged.generation_uuid,
                "CURRENT",
                false,
            )?;
            failpoint_as_io(
                "project.before_current_replace",
                staged.transaction_uuid,
                staged.generation_uuid,
                "CURRENT",
                false,
            )
        })
        .map_err(|error| {
            publication_error_from_parts(
                staged.transaction_uuid,
                staged.generation_uuid,
                "CURRENT",
                false,
                &error.to_string(),
            )
        })?;
    project_failpoint::hit(
        "project.after_current_replace",
        Some(staged.transaction_uuid),
        Some(staged.generation_uuid),
        "CURRENT",
        true,
    )
}

fn finish_published_generation(
    staged: &StagedProjectGeneration,
    manifest_sha256: [u8; 32],
) -> Result<(), GfError> {
    // Past the sole linearization point: any later failure reports
    // committed=true and never attempts rollback.
    sync_directory(&staged.root).map_err(|error| {
        publication_error_from_parts(
            staged.transaction_uuid,
            staged.generation_uuid,
            "CURRENT",
            true,
            &error.to_string(),
        )
    })?;
    project_failpoint::hit(
        "project.after_root_fsync",
        Some(staged.transaction_uuid),
        Some(staged.generation_uuid),
        "PUBLISHED",
        true,
    )?;
    write_journal(
        &staged.journal_path(),
        &staged.journal(JournalPhase::Published, Some(hex_digest(manifest_sha256))),
    )
    .map_err(|error| {
        publication_error_from_parts(
            staged.transaction_uuid,
            staged.generation_uuid,
            "PUBLISHED",
            true,
            &error.to_string(),
        )
    })?;
    project_failpoint::hit(
        "project.after_journal_published",
        Some(staged.transaction_uuid),
        Some(staged.generation_uuid),
        "PUBLISHED",
        true,
    )?;

    let resolved = resolve_project_generation(&staged.root).map_err(|error| {
        publication_error_from_parts(
            staged.transaction_uuid,
            staged.generation_uuid,
            "PUBLISHED",
            true,
            &error.to_string(),
        )
    })?;
    if resolved.generation_uuid() != staged.generation_uuid
        || resolved.manifest_sha256() != manifest_sha256
    {
        return Err(publication_error_from_parts(
            staged.transaction_uuid,
            staged.generation_uuid,
            "PUBLISHED",
            true,
            "published CURRENT did not resolve to exact generation bytes",
        ));
    }
    Ok(())
}

impl Drop for StagedProjectGeneration {
    fn drop(&mut self) {
        match &self.publication_lock {
            PublicationLock::Exclusive(lock) | PublicationLock::Optimistic(lock) => {
                let _ = crate::file_lock::unlock(lock);
            }
        }
    }
}

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
    fn new(
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
struct RequestFingerprint<'a> {
    format: &'static str,
    format_version: u32,
    transaction_uuid: String,
    generation_uuid: String,
    capabilities: &'a [ProjectCapability],
    participants: &'a [StagedParticipant],
}

#[derive(Debug, Serialize)]
struct CurrentRecord {
    format: String,
    format_version: u32,
    generation_uuid: String,
    generation_manifest_sha256: String,
}

#[derive(Debug, Serialize)]
struct GenerationManifestRecord {
    format: String,
    format_version: u32,
    generation_uuid: String,
    parent_generation_uuid: Option<String>,
    transaction_uuid: String,
    capabilities: Vec<CapabilityRecord>,
    participants: Vec<StagedParticipant>,
}

#[derive(Debug, Serialize)]
struct CapabilityRecord {
    capability_id: String,
    capability_version: u32,
}

impl Serialize for StagedParticipant {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        struct Ordered<'a> {
            capability_id: &'a str,
            capability_version: u32,
            record_family_id: &'a str,
            record_version: u32,
            relative_path: &'a str,
            encoding: &'a str,
            byte_length: u64,
            row_count: u64,
            schema_fingerprint: &'a str,
            content_sha256: &'a str,
        }
        Ordered {
            capability_id: &self.capability_id,
            capability_version: self.capability_version,
            record_family_id: &self.record_family_id,
            record_version: self.record_version,
            relative_path: &self.relative_path,
            encoding: &self.encoding,
            byte_length: self.byte_length,
            row_count: self.row_count,
            schema_fingerprint: &self.schema_fingerprint,
            content_sha256: &self.content_sha256,
        }
        .serialize(serializer)
    }
}

fn canonical_supported_root(root: &Path) -> Result<PathBuf, GfError> {
    // Resolution validates FORMAT, CURRENT, containment, and link policy.
    resolve_project_generation(root).map(|resolved| resolved.container_root().to_owned())
}

fn validate_request(request: &ProjectGenerationRequest) -> Result<(), GfError> {
    if request.capabilities.is_empty() {
        return Err(project_error(
            ProjectErrorCode::PublicationFailed,
            "a generation must declare at least one capability",
        ));
    }
    for capability in &request.capabilities {
        validate_machine_id(&capability.capability_id)?;
        if capability.capability_version == 0 {
            return Err(project_error(
                ProjectErrorCode::PublicationFailed,
                "capability contract versions must be positive",
            ));
        }
    }
    for participant in &request.participants {
        validate_machine_id(&participant.capability_id)?;
        validate_machine_id(&participant.record_family_id)?;
        if participant.capability_version == 0 || participant.record_version == 0 {
            return Err(project_error(
                ProjectErrorCode::PublicationFailed,
                "participant contract versions must be positive",
            ));
        }
    }
    Ok(())
}

fn request_metadata(
    request: &ProjectGenerationRequest,
) -> Result<(Vec<ProjectCapability>, Vec<StagedParticipant>, String), GfError> {
    let mut capabilities = request.capabilities.clone();
    capabilities.sort_by(|left, right| left.capability_id.cmp(&right.capability_id));
    if capabilities
        .windows(2)
        .any(|pair| pair[0].capability_id == pair[1].capability_id)
    {
        return Err(project_error(
            ProjectErrorCode::PublicationFailed,
            "duplicate capability identity",
        ));
    }
    if capabilities
        .binary_search_by(|entry| entry.capability_id.as_str().cmp("graph"))
        .ok()
        .map(|index| capabilities[index].capability_version)
        != Some(1)
    {
        return Err(project_error(
            ProjectErrorCode::PublicationFailed,
            "every generation must declare graph capability version 1",
        ));
    }
    let mut participants = Vec::with_capacity(request.participants.len());
    for participant in &request.participants {
        let content_sha256: [u8; 32] = Sha256::digest(&participant.bytes).into();
        participants.push(StagedParticipant {
            capability_id: participant.capability_id.clone(),
            capability_version: participant.capability_version,
            record_family_id: participant.record_family_id.clone(),
            record_version: participant.record_version,
            relative_path: format!(
                "{}/{}.{}",
                participant.capability_id,
                participant.record_family_id,
                participant.encoding.extension()
            ),
            encoding: participant.encoding.extension().into(),
            byte_length: u64::try_from(participant.bytes.len()).map_err(|_| {
                project_error(
                    ProjectErrorCode::PublicationFailed,
                    "participant byte length exceeds u64",
                )
            })?,
            row_count: participant.row_count,
            schema_fingerprint: hex_digest(participant.schema_fingerprint),
            content_sha256: hex_digest(content_sha256),
        });
    }
    participants.sort_by(|left, right| {
        (
            &left.capability_id,
            &left.record_family_id,
            &left.relative_path,
        )
            .cmp(&(
                &right.capability_id,
                &right.record_family_id,
                &right.relative_path,
            ))
    });
    if participants.windows(2).any(|pair| {
        pair[0].capability_id == pair[1].capability_id
            && pair[0].record_family_id == pair[1].record_family_id
    }) {
        return Err(project_error(
            ProjectErrorCode::PublicationFailed,
            "duplicate participant identity",
        ));
    }
    for participant in &participants {
        let capability = capabilities
            .binary_search_by(|entry| entry.capability_id.cmp(&participant.capability_id))
            .ok()
            .map(|index| &capabilities[index])
            .ok_or_else(|| {
                project_error(
                    ProjectErrorCode::PublicationFailed,
                    "participant capability is not declared",
                )
            })?;
        if capability.capability_version != participant.capability_version {
            return Err(project_error(
                ProjectErrorCode::PublicationFailed,
                "participant capability version conflicts with declaration",
            ));
        }
    }
    let fingerprint_input = RequestFingerprint {
        format: "graphforge-publication-request",
        format_version: 1,
        transaction_uuid: request.transaction_uuid.hyphenated().to_string(),
        generation_uuid: request.generation_uuid.hyphenated().to_string(),
        capabilities: &capabilities,
        participants: &participants,
    };
    let bytes = canonical_line(&fingerprint_input)?;
    let digest: [u8; 32] = Sha256::digest(bytes).into();
    Ok((capabilities, participants, hex_digest(digest)))
}

fn validate_machine_id(value: &str) -> Result<(), GfError> {
    if value.is_empty()
        || value.len() > 64
        || !value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
        })
    {
        return Err(project_error(
            ProjectErrorCode::PublicationFailed,
            "machine ID must be 1-64 lowercase ASCII letters, digits, hyphens, or underscores",
        ));
    }
    Ok(())
}

fn verify_participant_file(path: &Path, expected: &StagedParticipant) -> Result<(), GfError> {
    let metadata = std::fs::symlink_metadata(path).map_err(publication_io)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(project_error(
            ProjectErrorCode::PublicationFailed,
            "staged participant is not a regular non-link file",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(project_error(
                ProjectErrorCode::PublicationFailed,
                "staged participant is hard-linked",
            ));
        }
    }
    if metadata.len() != expected.byte_length {
        return Err(project_error(
            ProjectErrorCode::PublicationFailed,
            "staged participant byte length changed",
        ));
    }
    let mut file = File::open(path).map_err(publication_io)?;
    let mut hasher = Sha256::new();
    std::io::copy(&mut file, &mut hasher).map_err(publication_io)?;
    let actual: [u8; 32] = hasher.finalize().into();
    if hex_digest(actual) != expected.content_sha256 {
        return Err(project_error(
            ProjectErrorCode::PublicationFailed,
            "staged participant digest changed",
        ));
    }
    Ok(())
}

fn sync_participant_directories(
    participants_root: &Path,
    participants: &[StagedParticipant],
) -> Result<(), GfError> {
    let mut directories: Vec<PathBuf> = participants
        .iter()
        .filter_map(|participant| {
            participants_root
                .join(&participant.relative_path)
                .parent()
                .map(Path::to_owned)
        })
        .collect();
    directories.sort();
    directories.dedup();
    directories.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for directory in directories {
        sync_directory(&directory)?;
    }
    sync_directory(participants_root)
}

fn write_new(path: &Path, bytes: &[u8]) -> Result<File, GfError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(publication_io)?;
    file.write_all(bytes).map_err(publication_io)?;
    Ok(file)
}

fn failpoint_as_io(
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

pub(crate) fn ensure_machine_directory(root: &Path, relative: &Path) -> Result<PathBuf, GfError> {
    let mut current = root.to_owned();
    for component in relative.components() {
        let std::path::Component::Normal(component) = component else {
            return Err(project_error(
                ProjectErrorCode::ProjectCorrupt,
                "machine directory path is not normalized",
            ));
        };
        current.push(component);
        match std::fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
            Ok(_) => {
                return Err(project_error(
                    ProjectErrorCode::ProjectCorrupt,
                    "machine directory is linked or not a directory",
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match std::fs::create_dir(&current) {
                    Ok(()) => {
                        sync_directory(
                            current
                                .parent()
                                .expect("machine directory beneath project has a parent"),
                        )?;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                        let metadata =
                            std::fs::symlink_metadata(&current).map_err(publication_io)?;
                        if !metadata.is_dir() || metadata.file_type().is_symlink() {
                            return Err(project_error(
                                ProjectErrorCode::ProjectCorrupt,
                                "concurrently created machine path is linked or not a directory",
                            ));
                        }
                    }
                    Err(error) => return Err(publication_io(error)),
                }
            }
            Err(error) => return Err(publication_io(error)),
        }
    }
    Ok(current)
}

pub(crate) fn open_regular_lock(path: &Path) -> Result<File, GfError> {
    if let Ok(metadata) = std::fs::symlink_metadata(path)
        && (!metadata.is_file() || metadata.file_type().is_symlink())
    {
        return Err(project_error(
            ProjectErrorCode::ProjectCorrupt,
            "writer lock is linked or not a regular file",
        ));
    }
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .map_err(publication_io)?;
    let metadata = file.metadata().map_err(publication_io)?;
    if !metadata.is_file() {
        return Err(project_error(
            ProjectErrorCode::ProjectCorrupt,
            "writer lock is not a regular file",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(project_error(
                ProjectErrorCode::ProjectCorrupt,
                "writer lock is hard-linked",
            ));
        }
    }
    Ok(file)
}

fn verify_exact_file(path: &Path, expected: &[u8]) -> Result<(), GfError> {
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

pub(crate) fn write_journal(path: &Path, journal: &JournalRecord) -> Result<(), GfError> {
    let bytes = canonical_line(journal)?;
    AtomicFile::new(path, AllowOverwrite)
        .write(|file| file.write_all(&bytes))
        .map_err(|error| publication_io(std::io::Error::other(error.to_string())))?;
    sync_directory(
        path.parent()
            .expect("transaction journal always has a parent"),
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
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return Ok(false);
    };
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
        std::fs::remove_file(entry.path()).map_err(publication_io)?;
    }
    std::fs::remove_dir(path).map_err(publication_io)?;
    sync_directory(
        path.parent()
            .expect("atomic-write temporary directory always has a parent"),
    )?;
    Ok(true)
}

fn canonical_line<T: Serialize>(value: &T) -> Result<Vec<u8>, GfError> {
    let mut bytes = serde_json::to_vec(value)
        .map_err(|error| GfError::Storage(format!("failed to encode project record: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

#[cfg(unix)]
pub(crate) fn sync_directory(path: &Path) -> Result<(), GfError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(publication_io)
}

#[cfg(windows)]
pub(crate) fn sync_directory(path: &Path) -> Result<(), GfError> {
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

fn hex_digest(bytes: [u8; 32]) -> String {
    let mut output = String::with_capacity(64);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

fn parse_digest(value: &str) -> Option<[u8; 32]> {
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

fn project_error(code: ProjectErrorCode, message: impl Into<String>) -> GfError {
    GfError::Project {
        code,
        message: message.into(),
    }
}

fn publication_io(error: impl std::fmt::Display) -> GfError {
    GfError::Storage(error.to_string())
}

fn transaction_conflict(request: &ProjectGenerationRequest) -> GfError {
    project_error(
        ProjectErrorCode::TransactionConflict,
        format!(
            "transaction_uuid={} generation_uuid={} phase=PREPARING committed=false cause=identity_conflict",
            request.transaction_uuid.hyphenated(),
            request.generation_uuid.hyphenated()
        ),
    )
}

fn publication_error(
    request: &ProjectGenerationRequest,
    phase: &str,
    committed: bool,
    cause: &str,
) -> GfError {
    publication_error_from_parts(
        request.transaction_uuid,
        request.generation_uuid,
        phase,
        committed,
        cause,
    )
}

fn publication_error_from_parts(
    transaction_uuid: Uuid,
    generation_uuid: Uuid,
    phase: &str,
    committed: bool,
    cause: &str,
) -> GfError {
    project_error(
        ProjectErrorCode::PublicationFailed,
        format!(
            "transaction_uuid={} generation_uuid={} phase={phase} committed={committed} cause={}",
            transaction_uuid.hyphenated(),
            generation_uuid.hyphenated(),
            safe_cause(cause)
        ),
    )
}

fn safe_cause(cause: &str) -> String {
    cause
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || "_ -".contains(*character))
        .take(96)
        .collect()
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use crate::open_or_initialize_project;

    #[cfg(windows)]
    #[test]
    fn windows_directory_sync_uses_write_capable_handle() {
        let root = tempfile::tempdir().unwrap();

        sync_directory(root.path()).unwrap();
    }

    fn participant(capability: &str, family: &str, value: &[u8]) -> ProjectParticipant {
        ProjectParticipant {
            capability_id: capability.into(),
            capability_version: 1,
            record_family_id: family.into(),
            record_version: 1,
            encoding: ProjectParticipantEncoding::Parquet,
            schema_fingerprint: Sha256::digest(format!("{capability}/{family}")).into(),
            row_count: 1,
            bytes: value.to_vec(),
        }
    }

    fn request(participants: Vec<ProjectParticipant>) -> ProjectGenerationRequest {
        let mut capabilities = vec![ProjectCapability {
            capability_id: "graph".into(),
            capability_version: 1,
        }];
        for participant in &participants {
            if participant.capability_id != "graph"
                && !capabilities
                    .iter()
                    .any(|entry| entry.capability_id == participant.capability_id)
            {
                capabilities.push(ProjectCapability {
                    capability_id: participant.capability_id.clone(),
                    capability_version: participant.capability_version,
                });
            }
        }
        ProjectGenerationRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
            capabilities,
            participants,
        }
    }

    fn project() -> tempfile::TempDir {
        let root = tempfile::tempdir().unwrap();
        open_or_initialize_project(root.path()).unwrap();
        root
    }

    fn publish(root: &Path, request: ProjectGenerationRequest) -> ProjectPublicationReceipt {
        let ProjectStageOutcome::Staged(staged) = stage_project_generation(root, &request).unwrap()
        else {
            panic!("new request unexpectedly replayed");
        };
        staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .unwrap()
            .publish()
            .unwrap()
    }

    fn journal_path(root: &Path, transaction_uuid: Uuid) -> PathBuf {
        root.join(TRANSACTIONS_DIR)
            .join(format!("{}.json", transaction_uuid.hyphenated()))
    }

    #[test]
    fn publishes_graph_only_and_multi_domain_sets_atomically() {
        for participants in [
            vec![participant("graph", "nodes", b"graph")],
            vec![
                participant("graph", "nodes", b"graph"),
                participant("provenance", "events", b"provenance"),
            ],
            vec![
                participant("graph", "nodes", b"graph"),
                participant("provenance", "events", b"provenance"),
                participant("knowledge", "assertions", b"knowledge"),
            ],
        ] {
            let root = project();
            let request = request(participants);
            let expected = request.generation_uuid;
            publish(root.path(), request);
            let resolved = resolve_project_generation(root.path()).unwrap();
            assert_eq!(resolved.generation_uuid(), expected);
        }
    }

    #[test]
    fn validation_failure_leaves_parent_authoritative() {
        let root = project();
        let parent = resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid();
        let request = request(vec![participant("graph", "nodes", b"new")]);
        let ProjectStageOutcome::Staged(staged) =
            stage_project_generation(root.path(), &request).unwrap()
        else {
            panic!("new request unexpectedly replayed");
        };

        let error = staged
            .validate(
                |_| Err(GfError::Validation("domain rejected".into())),
                |_, _| Ok(()),
            )
            .err()
            .expect("validation must fail");

        assert!(matches!(error, GfError::Validation(_)));
        assert_eq!(
            resolve_project_generation(root.path())
                .unwrap()
                .generation_uuid(),
            parent
        );
    }

    #[test]
    fn durable_install_io_failure_is_wrapped_before_current_changes() {
        let root = project();
        let parent = resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid();
        let request = request(vec![participant("graph", "nodes", b"new")]);
        let ProjectStageOutcome::Staged(staged) =
            stage_project_generation(root.path(), &request).unwrap()
        else {
            panic!("new request unexpectedly replayed")
        };
        let validated = staged.validate(|_| Ok(()), |_, _| Ok(())).unwrap();
        fs::remove_file(
            validated
                .0
                .generation_root
                .join(PARTICIPANTS_DIR)
                .join(&validated.0.participants[0].relative_path),
        )
        .unwrap();
        let error = validated.publish().unwrap_err();
        assert_eq!(error.code(), "GF_PUBLICATION_FAILED");
        assert!(error.to_string().contains("phase=DURABLE committed=false"));
        assert_eq!(
            resolve_project_generation(root.path())
                .unwrap()
                .generation_uuid(),
            parent
        );
    }

    #[test]
    fn journal_records_each_deterministic_publication_phase() {
        let root = project();
        let request = request(vec![participant("graph", "nodes", b"new")]);
        let journal_path = journal_path(root.path(), request.transaction_uuid);
        let ProjectStageOutcome::Staged(staged) =
            stage_project_generation(root.path(), &request).unwrap()
        else {
            panic!("new request unexpectedly replayed");
        };
        assert_eq!(
            read_journal(&journal_path).unwrap().phase,
            JournalPhase::Staged
        );

        let validated = staged.validate(|_| Ok(()), |_, _| Ok(())).unwrap();
        assert_eq!(
            read_journal(&journal_path).unwrap().phase,
            JournalPhase::Validated
        );

        validated.publish().unwrap();
        let published = read_journal(&journal_path).unwrap();
        assert_eq!(published.phase, JournalPhase::Published);
        assert!(published.generation_manifest_sha256.is_some());
    }

    #[test]
    fn request_fingerprint_is_independent_of_participant_input_order() {
        let mut request = request(vec![
            participant("provenance", "events", b"provenance"),
            participant("graph", "nodes", b"graph"),
        ]);
        let (_, first_metadata, first_fingerprint) = request_metadata(&request).unwrap();
        request.participants.reverse();
        let (_, second_metadata, second_fingerprint) = request_metadata(&request).unwrap();

        assert_eq!(first_metadata, second_metadata);
        assert_eq!(first_fingerprint, second_fingerprint);
        assert_eq!(first_metadata[0].capability_id, "graph");
        assert_eq!(first_metadata[1].capability_id, "provenance");
    }

    #[test]
    fn machine_ids_match_the_committed_generation_reader_contract() {
        let root = project();
        let valid = request(vec![participant("graph", "node-properties", b"properties")]);
        let generation_uuid = valid.generation_uuid;
        publish(root.path(), valid);
        let resolved = resolve_project_generation(root.path()).unwrap();
        assert_eq!(resolved.generation_uuid(), generation_uuid);
        assert!(
            resolved
                .participant_path("graph", "node-properties")
                .unwrap()
                .is_file()
        );

        let underscore = request(vec![participant(
            "graph_data",
            "node_properties",
            b"properties",
        )]);
        publish(root.path(), underscore);
        let resolved = resolve_project_generation(root.path()).unwrap();
        assert!(
            resolved
                .participant_path("graph_data", "node_properties")
                .unwrap()
                .is_file()
        );

        let invalid = request(vec![participant("graph", "NodeProperties", b"properties")]);
        let error = stage_project_generation(root.path(), &invalid)
            .err()
            .expect("reader-incompatible machine ID must be rejected");
        assert_eq!(error.code(), "GF_PUBLICATION_FAILED");
    }

    #[test]
    fn tampered_staged_bytes_fail_before_publication() {
        let root = project();
        let parent = resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid();
        let initial_request = request(vec![participant("graph", "nodes", b"original")]);
        let ProjectStageOutcome::Staged(staged) =
            stage_project_generation(root.path(), &initial_request).unwrap()
        else {
            panic!("new request unexpectedly replayed");
        };
        std::fs::write(
            staged.generation_root.join(PARTICIPANTS_DIR).join(
                staged
                    .participants
                    .first()
                    .expect("participant")
                    .relative_path
                    .as_str(),
            ),
            b"tampered",
        )
        .unwrap();

        let error = staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .err()
            .expect("tampered bytes must fail validation");

        assert_eq!(error.code(), "GF_PUBLICATION_FAILED");
        assert_eq!(
            resolve_project_generation(root.path())
                .unwrap()
                .generation_uuid(),
            parent
        );

        let request = request(vec![participant("graph", "nodes", b"original")]);
        let ProjectStageOutcome::Staged(staged) =
            stage_project_generation(root.path(), &request).unwrap()
        else {
            panic!("new request unexpectedly replayed");
        };
        let path = staged
            .generation_root
            .join(PARTICIPANTS_DIR)
            .join(&staged.participants[0].relative_path);
        std::fs::write(path, b"short").unwrap();
        assert_eq!(
            staged
                .validate(|_| Ok(()), |_, _| Ok(()))
                .err()
                .expect("truncated staged bytes must fail")
                .code(),
            "GF_PUBLICATION_FAILED"
        );
        assert_eq!(
            resolve_project_generation(root.path())
                .unwrap()
                .generation_uuid(),
            parent
        );
    }

    #[cfg(unix)]
    #[test]
    fn staged_participant_hard_link_fails_before_current_mutation() {
        let root = project();
        let parent = resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid();
        let request = request(vec![participant("graph", "nodes", b"stable")]);
        let ProjectStageOutcome::Staged(staged) =
            stage_project_generation(root.path(), &request).unwrap()
        else {
            panic!("unexpected replay")
        };
        let path = staged
            .generation_root
            .join(PARTICIPANTS_DIR)
            .join(&staged.participants[0].relative_path);
        let external = root.path().join("external-participant");
        fs::rename(&path, &external).unwrap();
        fs::hard_link(&external, &path).unwrap();

        assert_eq!(
            staged
                .validate(|_| Ok(()), |_, _| Ok(()))
                .err()
                .expect("hard-linked staged bytes must fail")
                .code(),
            "GF_PUBLICATION_FAILED"
        );
        assert_eq!(
            resolve_project_generation(root.path())
                .unwrap()
                .generation_uuid(),
            parent
        );
    }

    #[test]
    fn identical_published_transaction_is_idempotent() {
        let root = project();
        let request = request(vec![participant("graph", "nodes", b"same")]);
        publish(root.path(), request.clone());

        let ProjectStageOutcome::AlreadyPublished(receipt) =
            stage_project_generation(root.path(), &request).unwrap()
        else {
            panic!("identical replay was not recognized");
        };
        assert!(receipt.idempotent_replay);
    }

    #[test]
    fn historical_published_transaction_remains_idempotent() {
        let root = project();
        let first = request(vec![participant("graph", "nodes", b"first")]);
        publish(root.path(), first.clone());
        publish(
            root.path(),
            request(vec![participant("graph", "nodes", b"second")]),
        );

        let ProjectStageOutcome::AlreadyPublished(receipt) =
            stage_project_generation(root.path(), &first).unwrap()
        else {
            panic!("historical identical replay was not recognized");
        };
        assert!(receipt.idempotent_replay);
    }

    #[test]
    fn changed_content_under_same_transaction_conflicts() {
        let root = project();
        let request = request(vec![participant("graph", "nodes", b"first")]);
        publish(root.path(), request.clone());
        let mut conflicting = request;
        conflicting.participants[0].bytes = b"different".to_vec();

        let error = stage_project_generation(root.path(), &conflicting)
            .err()
            .expect("conflicting replay must fail");

        assert_eq!(error.code(), "GF_IDEMPOTENCY_CONFLICT");
    }

    #[test]
    fn interrupted_stage_and_tampered_published_replay_are_exactly_classified() {
        let root = project();
        let staged_request = request(vec![participant("graph", "nodes", b"staged")]);
        let ProjectStageOutcome::Staged(staged) =
            stage_project_generation(root.path(), &staged_request).unwrap()
        else {
            panic!("fresh transaction unexpectedly replayed")
        };
        drop(staged);
        let interrupted = stage_project_generation(root.path(), &staged_request)
            .err()
            .expect("interrupted stage must fail");
        assert_eq!(interrupted.code(), "GF_PUBLICATION_FAILED");
        assert!(interrupted.to_string().contains("requires recovery"));

        let published_request = request(vec![participant("graph", "nodes", b"published")]);
        let receipt = publish(root.path(), published_request.clone());
        let manifest = root
            .path()
            .join(GENERATIONS_DIR)
            .join(receipt.generation_uuid.hyphenated().to_string())
            .join(MANIFEST_FILE);
        fs::write(&manifest, b"tampered\n").unwrap();
        assert_eq!(
            stage_project_generation(root.path(), &published_request)
                .err()
                .expect("tampered published replay must fail")
                .code(),
            "GF_PROJECT_CORRUPT"
        );
    }

    #[test]
    fn optimistic_promotion_refuses_an_existing_generation_destination() {
        let root = project();
        let request = request(vec![participant("graph", "nodes", b"optimistic")]);
        let operation: [u8; 32] = Sha256::digest(b"wave7-existing-destination").into();
        let ProjectStageOutcome::Staged(staged) =
            stage_project_generation_optimistic(root.path(), &request, operation).unwrap()
        else {
            panic!("fresh optimistic transaction unexpectedly replayed")
        };
        let destination = root
            .path()
            .join(GENERATIONS_DIR)
            .join(request.generation_uuid.hyphenated().to_string());
        fs::create_dir(&destination).unwrap();
        let error = promote_optimistic_generation(&staged).unwrap_err();
        assert_eq!(error.code(), "GF_IDEMPOTENCY_CONFLICT");
        assert!(error.to_string().contains("generation_exists"));
        assert!(staged.generation_root.is_dir());
    }

    #[test]
    fn reader_pinned_to_parent_survives_publication() {
        let root = project();
        let parent = resolve_project_generation(root.path()).unwrap();
        let request = request(vec![participant("graph", "nodes", b"new")]);
        let child = request.generation_uuid;

        publish(root.path(), request);

        assert_ne!(parent.generation_uuid(), child);
        assert_eq!(
            resolve_project_generation(root.path())
                .unwrap()
                .generation_uuid(),
            child
        );
        assert!(parent.generation_root().exists());
    }

    #[test]
    fn optimistic_attempts_stage_concurrently_and_compare_parent_at_commit() {
        let root = project();
        let first = request(vec![participant("graph", "nodes", b"first")]);
        let second = request(vec![participant("graph", "nodes", b"second")]);
        let first_operation: [u8; 32] = Sha256::digest(b"logical-first").into();
        let second_operation: [u8; 32] = Sha256::digest(b"logical-second").into();

        let ProjectStageOutcome::Staged(first_staged) =
            stage_project_generation_optimistic(root.path(), &first, first_operation).unwrap()
        else {
            panic!("first optimistic operation replayed unexpectedly");
        };
        let ProjectStageOutcome::Staged(second_staged) =
            stage_project_generation_optimistic(root.path(), &second, second_operation).unwrap()
        else {
            panic!("second optimistic operation replayed unexpectedly");
        };

        let first_validated = first_staged.validate(|_| Ok(()), |_, _| Ok(())).unwrap();
        let second_validated = second_staged.validate(|_| Ok(()), |_, _| Ok(())).unwrap();
        first_validated.publish().unwrap();
        let error = second_validated
            .publish()
            .expect_err("stale optimistic parent must not publish");
        assert_eq!(error.code(), "GF_WRITE_CONFLICT");
        assert!(
            !root
                .path()
                .join(GENERATIONS_DIR)
                .join(second.generation_uuid.hyphenated().to_string())
                .exists()
        );

        let mut rebased = second.clone();
        rebased.participants[0].bytes = b"second-rebased".to_vec();
        let ProjectStageOutcome::Staged(rebased) =
            stage_project_generation_optimistic(root.path(), &rebased, second_operation).unwrap()
        else {
            panic!("aborted optimistic operation did not permit a rebase attempt");
        };
        rebased
            .validate(|_| Ok(()), |_, _| Ok(()))
            .unwrap()
            .publish()
            .unwrap();
        assert_eq!(
            resolve_project_generation(root.path())
                .unwrap()
                .generation_uuid(),
            second.generation_uuid
        );
    }

    #[test]
    fn optimistic_validation_conflict_aborts_only_its_own_rebase_attempt() {
        let root = project();
        let mut stale_request = request(vec![participant("graph", "nodes", b"attempt")]);
        let operation: [u8; 32] = Sha256::digest(b"logical-validation-rebase").into();
        let ProjectStageOutcome::Staged(staged) =
            stage_project_generation_optimistic(root.path(), &stale_request, operation).unwrap()
        else {
            panic!("optimistic operation replayed unexpectedly");
        };

        publish(
            root.path(),
            request(vec![participant("graph", "nodes", b"concurrent")]),
        );
        let error = staged
            .validate(
                |_| Ok(()),
                |parent, _| {
                    let current = resolve_project_generation(root.path())?;
                    if current.generation_uuid() != parent.generation_uuid() {
                        return Err(project_error(
                            ProjectErrorCode::WriteConflict,
                            "project generation changed before composite validation",
                        ));
                    }
                    Ok(())
                },
            )
            .err()
            .expect("stale optimistic validation must request a rebase");
        assert_eq!(error.code(), "GF_WRITE_CONFLICT");
        assert_eq!(
            read_journal(&journal_path(root.path(), stale_request.transaction_uuid,))
                .unwrap()
                .phase,
            JournalPhase::Aborted
        );

        let different_operation: [u8; 32] = Sha256::digest(b"different-operation").into();
        let identity_error =
            stage_project_generation_optimistic(root.path(), &stale_request, different_operation)
                .err()
                .expect("different logical identity must not reuse the aborted transaction");
        assert_eq!(identity_error.code(), "GF_IDEMPOTENCY_CONFLICT");

        stale_request.participants[0].bytes = b"rebased-attempt".to_vec();
        let ProjectStageOutcome::Staged(rebased) =
            stage_project_generation_optimistic(root.path(), &stale_request, operation).unwrap()
        else {
            panic!("aborted optimistic operation did not permit a rebase attempt");
        };
        rebased
            .validate(|_| Ok(()), |_, _| Ok(()))
            .unwrap()
            .publish()
            .unwrap();
    }

    #[test]
    fn aborted_optimistic_replay_fails_closed_when_generation_cleanup_is_incomplete() {
        let root = project();
        let request = request(vec![participant("graph", "nodes", b"attempt")]);
        let operation: [u8; 32] = Sha256::digest(b"aborted-cleanup-contract").into();
        let ProjectStageOutcome::Staged(staged) =
            stage_project_generation_optimistic(root.path(), &request, operation).unwrap()
        else {
            panic!("optimistic operation replayed unexpectedly");
        };
        abort_stale_generation(&staged).unwrap();
        drop(staged);

        let leftover = root
            .path()
            .join(GENERATIONS_DIR)
            .join(request.generation_uuid.hyphenated().to_string());
        std::fs::create_dir(&leftover).unwrap();
        let error = stage_project_generation_optimistic(root.path(), &request, operation)
            .err()
            .expect("incomplete aborted cleanup must fail closed");
        assert_eq!(error.code(), "GF_PUBLICATION_FAILED");
        assert!(error.to_string().contains("cleanup is incomplete"));
        assert!(leftover.exists());
        assert_eq!(
            read_journal(&journal_path(root.path(), request.transaction_uuid))
                .unwrap()
                .phase,
            JournalPhase::Aborted
        );
    }

    #[test]
    fn optimistic_promotion_closes_staged_handles_before_directory_rename() {
        let root = project();
        let request = request(vec![participant("graph", "nodes", b"promoted")]);
        let generation_uuid = request.generation_uuid;
        let operation: [u8; 32] = Sha256::digest(b"windows-promotion-handles").into();

        let ProjectStageOutcome::Staged(staged) =
            stage_project_generation_optimistic(root.path(), &request, operation).unwrap()
        else {
            panic!("optimistic operation replayed unexpectedly");
        };
        staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .unwrap()
            .publish()
            .unwrap();

        assert_eq!(
            resolve_project_generation(root.path())
                .unwrap()
                .generation_uuid(),
            generation_uuid
        );
    }

    #[test]
    fn optimistic_transaction_identity_has_one_live_attempt() {
        let root = project();
        let request = request(vec![participant("graph", "nodes", b"attempt")]);
        let operation: [u8; 32] = Sha256::digest(b"logical-attempt").into();
        let first = stage_project_generation_optimistic(root.path(), &request, operation).unwrap();

        let error = stage_project_generation_optimistic(root.path(), &request, operation)
            .err()
            .expect("duplicate live attempt must be rejected");
        assert_eq!(error.code(), "GF_WRITER_BUSY");
        drop(first);
    }

    #[test]
    fn recovery_preserves_live_optimistic_attempt_then_cleans_it_after_release() {
        let root = project();
        let request = request(vec![participant("graph", "nodes", b"live")]);
        let operation: [u8; 32] = Sha256::digest(b"logical-live").into();
        let (_, _, request_fingerprint) = request_metadata(&request).unwrap();
        let generation_path = root
            .path()
            .join(ATTEMPTS_DIR)
            .join(request.transaction_uuid.hyphenated().to_string())
            .join(request_fingerprint);
        let staged = stage_project_generation_optimistic(root.path(), &request, operation).unwrap();

        let live_report = crate::recover_project_transactions(root.path()).unwrap();
        assert_eq!(live_report.aborted_journals, 0);
        assert_eq!(live_report.removed_generations, 0);
        assert!(generation_path.exists());

        drop(staged);
        let abandoned_report = crate::recover_project_transactions(root.path()).unwrap();
        assert_eq!(abandoned_report.aborted_journals, 1);
        assert_eq!(abandoned_report.removed_generations, 1);
        assert!(!generation_path.exists());
    }

    #[test]
    fn writer_lock_is_nonblocking_and_fail_closed() {
        let root = project();
        let lock_dir = root.path().join(LOCKS_DIR);
        std::fs::create_dir_all(&lock_dir).unwrap();
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_dir.join(WRITER_LOCK_FILE))
            .unwrap();
        crate::file_lock::lock_exclusive(&lock).unwrap();
        let parent = resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid();

        let error = stage_project_generation(
            root.path(),
            &request(vec![participant("graph", "nodes", b"new")]),
        )
        .err()
        .expect("busy writer must fail");

        assert_eq!(error.code(), "GF_WRITER_BUSY");
        assert_eq!(
            resolve_project_generation(root.path())
                .unwrap()
                .generation_uuid(),
            parent
        );
    }

    #[test]
    fn malformed_generation_contracts_fail_before_staging_or_current_change() {
        let root = project();
        let before = fs::read(root.path().join(CURRENT_FILE)).unwrap();

        let mut cases = Vec::new();
        let mut no_capability = request(vec![]);
        no_capability.capabilities.clear();
        cases.push((no_capability, "at least one capability"));

        let mut zero_capability = request(vec![]);
        zero_capability.capabilities[0].capability_version = 0;
        cases.push((zero_capability, "capability contract versions"));

        let mut missing_graph = request(vec![]);
        missing_graph.capabilities[0].capability_id = "knowledge".into();
        cases.push((missing_graph, "graph capability version 1"));

        let mut duplicate_capability = request(vec![]);
        duplicate_capability
            .capabilities
            .push(duplicate_capability.capabilities[0].clone());
        cases.push((duplicate_capability, "duplicate capability identity"));

        let mut undeclared = request(vec![participant("knowledge", "events", b"event")]);
        undeclared
            .capabilities
            .retain(|entry| entry.capability_id == "graph");
        cases.push((undeclared, "participant capability is not declared"));

        let mut version_mismatch = request(vec![participant("knowledge", "events", b"event")]);
        version_mismatch.participants[0].capability_version = 2;
        cases.push((version_mismatch, "version conflicts with declaration"));

        let duplicate = participant("graph", "nodes", b"same");
        cases.push((
            request(vec![duplicate.clone(), duplicate]),
            "duplicate participant identity",
        ));

        let mut zero_record = request(vec![participant("graph", "nodes", b"node")]);
        zero_record.participants[0].record_version = 0;
        cases.push((zero_record, "participant contract versions"));

        let mut invalid_id = request(vec![participant("graph", "nodes", b"node")]);
        invalid_id.participants[0].record_family_id = "../nodes".into();
        cases.push((invalid_id, "machine ID"));

        for (candidate, expected) in cases {
            let error = stage_project_generation(root.path(), &candidate)
                .err()
                .expect("malformed request must fail");
            assert_eq!(error.code(), "GF_PUBLICATION_FAILED");
            assert!(error.to_string().contains(expected), "{error}");
            assert_eq!(fs::read(root.path().join(CURRENT_FILE)).unwrap(), before);
            assert!(!journal_path(root.path(), candidate.transaction_uuid).exists());
        }
    }

    #[test]
    fn publication_error_redacts_unsafe_cause_and_digest_parser_is_canonical() {
        let transaction = Uuid::now_v7();
        let generation = Uuid::now_v7();
        let error = publication_error_from_parts(
            transaction,
            generation,
            "STAGED",
            false,
            "bad/path:\nsecret=<value>!",
        );
        let text = error.to_string();
        assert!(text.contains("phase=STAGED committed=false cause=badpathsecretvalue"));
        assert!(!text.contains('/') && !text.contains('<') && !text.contains('!'));

        let bytes = [0xabu8; 32];
        let canonical = hex_digest(bytes);
        assert_eq!(parse_digest(&canonical), Some(bytes));
        for malformed in ["", "ab", &"A".repeat(64), &"g".repeat(64)] {
            assert_eq!(parse_digest(malformed), None);
        }
    }

    #[test]
    fn published_transaction_probe_verifies_durable_manifest_on_reopen() {
        let root = project();
        let input = request(vec![
            participant("graph", "nodes", b"nodes"),
            participant("graph", "edges", b"edges"),
        ]);
        let receipt = publish(root.path(), input.clone());
        let probed = published_project_transaction(root.path(), input.transaction_uuid)
            .unwrap()
            .unwrap();
        assert_eq!(probed.transaction_uuid, receipt.transaction_uuid);
        assert_eq!(probed.generation_uuid, receipt.generation_uuid);
        assert_eq!(
            probed.generation_manifest_sha256,
            receipt.generation_manifest_sha256
        );
        assert!(probed.idempotent_replay);
        assert!(
            published_project_transaction(root.path(), Uuid::now_v7())
                .unwrap()
                .is_none()
        );

        let reopened = resolve_project_generation(root.path()).unwrap();
        assert_eq!(reopened.generation_uuid(), receipt.generation_uuid);
        reopened.validate_complete_participant_inventory().unwrap();
        drop(reopened);

        let manifest = root
            .path()
            .join(GENERATIONS_DIR)
            .join(receipt.generation_uuid.hyphenated().to_string())
            .join(MANIFEST_FILE);
        fs::write(&manifest, b"tampered\n").unwrap();
        let error = published_project_transaction(root.path(), input.transaction_uuid).unwrap_err();
        assert_eq!(error.code(), "GF_PROJECT_CORRUPT");
        assert!(error.to_string().contains("does not match its journal"));
    }

    #[test]
    fn staged_participant_file_kind_matrix_fails_before_current_mutation() {
        for kind in ["missing", "directory", "symlink"] {
            let root = project();
            let parent = resolve_project_generation(root.path())
                .unwrap()
                .generation_uuid();
            let request = request(vec![participant("graph", "nodes", b"stable")]);
            let ProjectStageOutcome::Staged(staged) =
                stage_project_generation(root.path(), &request).unwrap()
            else {
                panic!("unexpected replay")
            };
            let path = staged
                .generation_root
                .join(PARTICIPANTS_DIR)
                .join(&staged.participants.first().unwrap().relative_path);
            std::fs::remove_file(&path).unwrap();
            match kind {
                "missing" => {}
                "directory" => std::fs::create_dir(&path).unwrap(),
                "symlink" => {
                    #[cfg(unix)]
                    std::os::unix::fs::symlink(root.path().join(CURRENT_FILE), &path).unwrap();
                    #[cfg(not(unix))]
                    std::fs::create_dir(&path).unwrap();
                }
                _ => unreachable!(),
            }
            let error = match staged.validate(|_| Ok(()), |_, _| Ok(())) {
                Ok(_) => panic!("hostile staged participant must fail"),
                Err(error) => error,
            };
            let expected_code = if kind == "missing" {
                "GF_IO"
            } else {
                "GF_PUBLICATION_FAILED"
            };
            assert_eq!(error.code(), expected_code);
            assert_eq!(
                resolve_project_generation(root.path())
                    .unwrap()
                    .generation_uuid(),
                parent
            );
        }
    }

    #[test]
    fn journal_decode_and_atomic_temp_cleanup_matrix_is_fail_closed() {
        let root = project();
        let journal = root.path().join(TRANSACTIONS_DIR).join("malformed.json");
        std::fs::create_dir_all(journal.parent().unwrap()).unwrap();
        for bytes in [
            b"not-json".as_slice(),
            br#"{"journal_version":999}"#,
            br#"{"journal_version":1,"transaction_uuid":"bad"}"#,
        ] {
            std::fs::write(&journal, bytes).unwrap();
            assert_eq!(
                read_journal(&journal).unwrap_err().code(),
                "GF_PROJECT_CORRUPT"
            );
            assert_eq!(std::fs::read(&journal).unwrap(), bytes);
        }

        let unrelated = root.path().join("metadata.json");
        assert!(!cleanup_atomicwrite_temp(&unrelated).unwrap());
        let empty = root.path().join(".atomicwriteabc123");
        std::fs::create_dir(&empty).unwrap();
        assert!(cleanup_atomicwrite_temp(&empty).unwrap());
        assert!(!empty.exists());

        let populated = root.path().join(".atomicwritedef456");
        std::fs::create_dir(&populated).unwrap();
        std::fs::write(populated.join("tmpfile.tmp"), b"abandoned").unwrap();
        assert!(cleanup_atomicwrite_temp(&populated).unwrap());
        assert!(!populated.exists());

        let hostile = root.path().join(".atomicwriteghi789");
        std::fs::create_dir(&hostile).unwrap();
        std::fs::write(hostile.join("unexpected"), b"caller bytes").unwrap();
        assert!(!cleanup_atomicwrite_temp(&hostile).unwrap());
        assert_eq!(
            std::fs::read(hostile.join("unexpected")).unwrap(),
            b"caller bytes"
        );
    }

    #[test]
    fn wave9_journal_metadata_and_lock_aliases_fail_closed() {
        let root = project();
        let journal = root.path().join(TRANSACTIONS_DIR).join("hostile.json");
        std::fs::create_dir_all(journal.parent().unwrap()).unwrap();

        std::fs::create_dir(&journal).unwrap();
        assert_eq!(
            read_journal(&journal).unwrap_err().code(),
            "GF_PROJECT_CORRUPT"
        );
        std::fs::remove_dir(&journal).unwrap();
        std::fs::write(&journal, vec![b'x'; MAX_JOURNAL_BYTES as usize + 1]).unwrap();
        assert_eq!(
            read_journal(&journal).unwrap_err().code(),
            "GF_PROJECT_CORRUPT"
        );

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            std::fs::remove_file(&journal).unwrap();
            let target = root.path().join("caller-journal");
            std::fs::write(&target, b"caller bytes").unwrap();
            symlink(&target, &journal).unwrap();
            assert_eq!(
                read_journal(&journal).unwrap_err().code(),
                "GF_PROJECT_CORRUPT"
            );

            let owner = root.path().join("lock-owner");
            let alias = root.path().join("lock-alias");
            std::fs::write(&owner, b"").unwrap();
            std::fs::hard_link(&owner, &alias).unwrap();
            assert_eq!(
                open_regular_lock(&alias).unwrap_err().code(),
                "GF_PROJECT_CORRUPT"
            );
            assert!(owner.exists());
        }
    }

    #[test]
    fn atomic_temp_cleanup_rejects_near_misses_links_and_multiple_entries() {
        let root = project();
        for name in [
            ".atomicwrite",
            ".atomicwrite12345",
            ".atomicwrite1234567",
            ".atomicwrite12-456",
            "atomicwrite123456",
        ] {
            let path = root.path().join(name);
            std::fs::create_dir(&path).unwrap();
            assert!(!cleanup_atomicwrite_temp(&path).unwrap());
            assert!(path.exists());
        }

        let regular = root.path().join(".atomicwriteabc001");
        std::fs::write(&regular, b"caller").unwrap();
        assert!(!cleanup_atomicwrite_temp(&regular).unwrap());
        assert_eq!(std::fs::read(&regular).unwrap(), b"caller");

        let multiple = root.path().join(".atomicwriteabc002");
        std::fs::create_dir(&multiple).unwrap();
        std::fs::write(multiple.join("tmpfile.tmp"), b"temporary").unwrap();
        std::fs::write(multiple.join("second"), b"caller").unwrap();
        assert!(!cleanup_atomicwrite_temp(&multiple).unwrap());
        assert_eq!(std::fs::read(multiple.join("second")).unwrap(), b"caller");

        let wrong_entry = root.path().join(".atomicwriteabc003");
        std::fs::create_dir(&wrong_entry).unwrap();
        std::fs::write(wrong_entry.join("not-temp"), b"caller").unwrap();
        assert!(!cleanup_atomicwrite_temp(&wrong_entry).unwrap());
        assert_eq!(
            std::fs::read(wrong_entry.join("not-temp")).unwrap(),
            b"caller"
        );

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            let linked_dir = root.path().join(".atomicwriteabc004");
            symlink(root.path(), &linked_dir).unwrap();
            assert!(!cleanup_atomicwrite_temp(&linked_dir).unwrap());
            assert!(
                linked_dir
                    .symlink_metadata()
                    .unwrap()
                    .file_type()
                    .is_symlink()
            );

            let linked_entry = root.path().join(".atomicwriteabc005");
            std::fs::create_dir(&linked_entry).unwrap();
            symlink(
                root.path().join(CURRENT_FILE),
                linked_entry.join("tmpfile.tmp"),
            )
            .unwrap();
            assert!(!cleanup_atomicwrite_temp(&linked_entry).unwrap());
            assert!(
                linked_entry
                    .join("tmpfile.tmp")
                    .symlink_metadata()
                    .unwrap()
                    .file_type()
                    .is_symlink()
            );

            let hardlinked_entry = root.path().join(".atomicwriteabc006");
            std::fs::create_dir(&hardlinked_entry).unwrap();
            let owned = root.path().join("hardlink-owner");
            std::fs::write(&owned, b"caller").unwrap();
            std::fs::hard_link(&owned, hardlinked_entry.join("tmpfile.tmp")).unwrap();
            assert!(!cleanup_atomicwrite_temp(&hardlinked_entry).unwrap());
            assert_eq!(std::fs::read(&owned).unwrap(), b"caller");
        }
    }

    #[test]
    fn machine_directory_and_lock_reject_hostile_path_components_without_replacement() {
        let root = tempfile::tempdir().unwrap();
        for relative in [
            Path::new("../escape"),
            Path::new("/absolute"),
            Path::new("safe/../escape"),
        ] {
            assert_eq!(
                ensure_machine_directory(root.path(), relative)
                    .unwrap_err()
                    .code(),
                "GF_PROJECT_CORRUPT"
            );
        }
        let file = root.path().join("owned");
        std::fs::write(&file, b"caller bytes").unwrap();
        assert_eq!(
            ensure_machine_directory(root.path(), Path::new("owned/child"))
                .unwrap_err()
                .code(),
            "GF_PROJECT_CORRUPT"
        );
        assert_eq!(std::fs::read(&file).unwrap(), b"caller bytes");

        let lock = root.path().join("lock");
        std::fs::create_dir(&lock).unwrap();
        assert_eq!(
            open_regular_lock(&lock).unwrap_err().code(),
            "GF_PROJECT_CORRUPT"
        );
        assert!(lock.is_dir());
    }

    #[test]
    fn public_transaction_probe_distinguishes_absent_staged_and_durable_publication() {
        let root = project();
        let request = request(vec![participant("graph", "nodes", b"rows")]);

        assert!(
            published_project_transaction(root.path(), request.transaction_uuid)
                .unwrap()
                .is_none()
        );
        let ProjectStageOutcome::Staged(staged) =
            stage_project_generation(root.path(), &request).unwrap()
        else {
            panic!("fresh transaction unexpectedly replayed");
        };
        assert!(
            published_project_transaction(root.path(), request.transaction_uuid)
                .unwrap()
                .is_none()
        );
        let receipt = staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .unwrap()
            .publish()
            .unwrap();
        let reopened = published_project_transaction(root.path(), request.transaction_uuid)
            .unwrap()
            .unwrap();
        assert_eq!(reopened.generation_uuid, receipt.generation_uuid);
        assert_eq!(
            reopened.generation_manifest_sha256,
            receipt.generation_manifest_sha256
        );
        assert!(reopened.idempotent_replay);
    }
}
