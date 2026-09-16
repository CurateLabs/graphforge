//! Atomic publication of complete immutable project generations.
//!
//! This module owns only the transaction protocol. Participant schemas and
//! domain semantics remain in their owning crates and enter validation through
//! opaque callbacks.
//!
//! Participant validation/staging and journal/atomic-file controls have private
//! owners; commit sequencing, reconciliation, and mutation locks stay here.

mod participants;
use participants::{
    prepare_generation_directory, request_metadata_with_payloads, stage_optional_graph_tree,
    stage_participant_files, sync_participant_directories, validate_request,
    verify_optional_generation_graph_tree, verify_optional_graph_tree_with_lease,
    verify_participant_file,
};
mod control;
pub(crate) use control::{
    AtomicPublishError, JournalPhase, JournalRecord, cleanup_atomicwrite_temp,
    cleanup_atomicwrite_temp_with_allocation, publish_atomic_bytes,
    publish_atomic_bytes_with_allocation, read_journal, sync_directory,
    write_journal_with_allocation,
};
use control::{
    CancelledBeforeReplace, CapabilityRecord, CurrentRecord, GenerationManifestRecord,
    canonical_line, failpoint_as_io, hex_digest, parse_digest, publish_atomic_bytes_in,
    verify_exact_file, write_new,
};

#[cfg(test)]
pub(crate) use control::write_journal;
#[cfg(test)]
use participants::request_metadata;

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use graphforge_core::{ApiErrorCode, GfError, ProjectErrorCode};
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
const MAX_GRAPH_MANIFEST_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;

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

#[derive(Debug, Clone)]
pub(crate) struct ProjectFileParticipant {
    pub participant: ProjectParticipant,
    pub source: PathBuf,
    pub byte_length: u64,
    pub content_sha256: [u8; 32],
}

#[derive(Clone, Copy)]
enum ParticipantPayloads<'a> {
    Memory,
    Files(&'a [ProjectFileParticipant], Option<&'a AtomicBool>, usize),
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
    allocation: Option<crate::StorageAllocationOperation>,
    root: PathBuf,
    publication_lock: PublicationLock,
    admission: StagedAdmission,
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

struct CommitLock(File);

enum StagedAdmission {
    Exclusive(crate::filesystem_admission::ProjectRootIdentity),
    Optimistic(Option<crate::filesystem_admission::ProjectRootIdentity>),
}

impl StagedAdmission {
    fn revalidate_identity(&self) -> Result<(), GfError> {
        match self {
            Self::Exclusive(identity) | Self::Optimistic(Some(identity)) => {
                identity.revalidate_identity()
            }
            Self::Optimistic(None) => Err(project_error(
                ProjectErrorCode::PublicationFailed,
                "optimistic project identity was already consumed",
            )),
        }
    }

    fn readmit_for_publish(
        &mut self,
    ) -> Result<Option<crate::filesystem_admission::ProjectLifecycleAdmission>, GfError> {
        match self {
            Self::Exclusive(identity) => {
                identity.revalidate_identity()?;
                Ok(None)
            }
            Self::Optimistic(identity) => identity
                .take()
                .ok_or_else(|| {
                    project_error(
                        ProjectErrorCode::PublicationFailed,
                        "optimistic project identity was already consumed",
                    )
                })?
                .readmit()
                .map(Some),
        }
    }
}

impl Drop for PublicationLock {
    fn drop(&mut self) {
        // Every error path that abandons a held publication lock must release it.
        // Closing the fd alone is not enough on every supported lock backend.
        match self {
            Self::Exclusive(lock) | Self::Optimistic(lock) => {
                let _ = crate::file_lock::unlock(lock);
            }
        }
    }
}

impl Drop for CommitLock {
    fn drop(&mut self) {
        let _ = crate::file_lock::unlock(&self.0);
    }
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
/// When the request includes a `graph`/`files` inventory and no explicit tree
/// source is provided, the parent's generation-owned `graph/` directory is
/// carried forward after inventory verification.
///
/// # Errors
/// Returns a stable project error for a busy writer, malformed participant,
/// conflicting transaction replay, corrupt parent, or I/O failure.
pub fn stage_project_generation(
    container_root: impl AsRef<Path>,
    request: &ProjectGenerationRequest,
) -> Result<ProjectStageOutcome, GfError> {
    stage_project_generation_with_graph_tree(container_root, request, None)
}

/// Stage a generation while supplying an explicit file-backed graph tree source.
///
/// `graph_tree` is required when publishing a new or replaced `graph`/`files`
/// inventory that does not match the parent generation tree. Unchanged
/// carry-forward can omit it; the parent tree is verified and copied.
///
/// # Errors
/// Returns the same stable staging errors as [`stage_project_generation`].
pub fn stage_project_generation_with_graph_tree(
    container_root: impl AsRef<Path>,
    request: &ProjectGenerationRequest,
    graph_tree: Option<&Path>,
) -> Result<ProjectStageOutcome, GfError> {
    stage_project_generation_with_graph_tree_mode(
        container_root,
        request,
        graph_tree,
        crate::filesystem_admission::ProjectLifecycleMode::Durable,
    )
}

/// Stage a generation using the lifecycle mode established when the owning
/// facade opened the project.
///
/// Ephemeral mode is reserved for process-owned temporary projects. Durable
/// callers must use the default wrapper or pass `Durable` explicitly.
///
/// # Errors
/// Returns the same stable staging errors as
/// [`stage_project_generation_with_graph_tree`].
pub fn stage_project_generation_with_graph_tree_mode(
    container_root: impl AsRef<Path>,
    request: &ProjectGenerationRequest,
    graph_tree: Option<&Path>,
    mode: crate::filesystem_admission::ProjectLifecycleMode,
) -> Result<ProjectStageOutcome, GfError> {
    stage_project_generation_inner(container_root.as_ref(), request, graph_tree, mode)
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
    stage_project_generation_optimistic_with_graph_tree(
        container_root,
        request,
        operation_fingerprint,
        None,
    )
}

/// Optimistic staging with an explicit file-backed graph tree source.
///
/// # Errors
/// Returns the same stable staging errors as
/// [`stage_project_generation_optimistic`].
pub fn stage_project_generation_optimistic_with_graph_tree(
    container_root: impl AsRef<Path>,
    request: &ProjectGenerationRequest,
    operation_fingerprint: [u8; 32],
    graph_tree: Option<&Path>,
) -> Result<ProjectStageOutcome, GfError> {
    stage_project_generation_optimistic_with_graph_tree_mode(
        container_root,
        request,
        operation_fingerprint,
        graph_tree,
        crate::filesystem_admission::ProjectLifecycleMode::Durable,
    )
}

/// Optimistic staging using the lifecycle mode established when the owning
/// facade opened the project.
///
/// # Errors
/// Returns the same stable staging errors as
/// [`stage_project_generation_optimistic_with_graph_tree`].
pub fn stage_project_generation_optimistic_with_graph_tree_mode(
    container_root: impl AsRef<Path>,
    request: &ProjectGenerationRequest,
    operation_fingerprint: [u8; 32],
    graph_tree: Option<&Path>,
    mode: crate::filesystem_admission::ProjectLifecycleMode,
) -> Result<ProjectStageOutcome, GfError> {
    stage_project_generation_optimistic_inner(
        container_root.as_ref(),
        request,
        operation_fingerprint,
        graph_tree,
        mode,
    )
    .map_err(|error| map_stage_error(request, error))
}

/// Stage against one caller-prepared, lifetime-pinned CURRENT generation.
///
/// The caller passes the full admission that resolved `parent`. This path takes
/// the writer while admission is still held, verifies CURRENT still names the
/// exact prepared generation, and only then releases the lifecycle lock while
/// retaining root identity under the writer.
pub(crate) fn stage_project_generation_from_admitted_parent(
    admission: crate::filesystem_admission::ProjectLifecycleAdmission,
    parent: ResolvedProjectGeneration,
    request: &ProjectGenerationRequest,
    graph_tree: Option<&Path>,
    allocation: Option<&crate::StorageAllocationOperation>,
) -> Result<ProjectStageOutcome, GfError> {
    let result = (|| {
        validate_request(request)?;
        admission.revalidate_identity()?;
        let root = canonical_supported_root(admission.root())?;
        if parent.container_root() != root {
            return Err(project_error(
                ProjectErrorCode::PublicationFailed,
                "prepared generation does not belong to the admitted project root",
            ));
        }
        let writer_lock = acquire_writer_lock(&root, request)?;
        project_failpoint::hit(
            "project.after_writer_lock",
            Some(request.transaction_uuid),
            Some(request.generation_uuid),
            "WRITER_LOCK",
            false,
        )?;
        admission.revalidate_identity()?;
        let current = resolve_project_generation(&root)?;
        if current.generation_uuid() != parent.generation_uuid()
            || current.manifest_sha256() != parent.manifest_sha256()
        {
            return Err(project_error(
                ProjectErrorCode::WriteConflict,
                "prepared CURRENT changed before admitted publication acquired the writer",
            ));
        }
        let identity = admission.into_identity()?;
        stage_project_generation_inner_with_locks(
            StagedAdmission::Exclusive(identity),
            root,
            PublicationLock::Exclusive(writer_lock),
            parent,
            request,
            None,
            None,
            graph_tree,
            ParticipantPayloads::Memory,
            allocation,
        )
    })();
    result.map_err(|error| map_stage_error(request, error))
}

#[allow(clippy::too_many_arguments)] // Ordinary staging inputs plus explicit diagnostic context.
pub(crate) fn stage_project_generation_from_files_admitted(
    admission: crate::filesystem_admission::ProjectLifecycleAdmission,
    parent: ResolvedProjectGeneration,
    request: &ProjectGenerationRequest,
    files: &[ProjectFileParticipant],
    graph_tree: Option<&Path>,
    cancelled: Option<&AtomicBool>,
    copy_buffer_bytes: usize,
    allocation: Option<&crate::StorageAllocationOperation>,
) -> Result<ProjectStageOutcome, GfError> {
    let result = (|| {
        validate_request(request)?;
        admission.revalidate_identity()?;
        let root = canonical_supported_root(admission.root())?;
        let writer_lock = acquire_writer_lock(&root, request)?;
        project_failpoint::hit(
            "project.after_writer_lock",
            Some(request.transaction_uuid),
            Some(request.generation_uuid),
            "WRITER_LOCK",
            false,
        )?;
        let current = resolve_project_generation(&root)?;
        if current.generation_uuid() != parent.generation_uuid()
            || current.manifest_sha256() != parent.manifest_sha256()
        {
            return Err(project_error(
                ProjectErrorCode::WriteConflict,
                "prepared CURRENT changed before portable import publication",
            ));
        }
        let identity = admission.into_identity()?;
        stage_project_generation_inner_with_locks(
            StagedAdmission::Exclusive(identity),
            root,
            PublicationLock::Exclusive(writer_lock),
            parent,
            request,
            None,
            None,
            graph_tree,
            ParticipantPayloads::Files(files, cancelled, copy_buffer_bytes),
            allocation,
        )
    })();
    result.map_err(|error| map_stage_error(request, error))
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
    graph_tree: Option<&Path>,
    mode: crate::filesystem_admission::ProjectLifecycleMode,
) -> Result<ProjectStageOutcome, GfError> {
    // Reject malformed contracts before taking the writer lock so concurrent
    // readers/writers are never blocked by validation-only failures.
    validate_request(request)?;
    let admission = crate::filesystem_admission::admit_project_lifecycle(
        container_root,
        mode,
        crate::filesystem_admission::ProjectRootRequirement::Existing,
    )?;
    admission.revalidate_identity()?;
    let root = canonical_supported_root(admission.root())?;
    let writer_lock = acquire_writer_lock(&root, request)?;
    project_failpoint::hit(
        "project.after_writer_lock",
        Some(request.transaction_uuid),
        Some(request.generation_uuid),
        "WRITER_LOCK",
        false,
    )?;
    let parent = resolve_project_generation(&root)?;
    let identity = admission.into_identity()?;
    stage_project_generation_inner_with_locks(
        StagedAdmission::Exclusive(identity),
        root,
        PublicationLock::Exclusive(writer_lock),
        parent,
        request,
        None,
        None,
        graph_tree,
        ParticipantPayloads::Memory,
        None,
    )
}

fn stage_project_generation_optimistic_inner(
    container_root: &Path,
    request: &ProjectGenerationRequest,
    operation_fingerprint: [u8; 32],
    graph_tree: Option<&Path>,
    mode: crate::filesystem_admission::ProjectLifecycleMode,
) -> Result<ProjectStageOutcome, GfError> {
    validate_request(request)?;
    let admission = crate::filesystem_admission::admit_project_lifecycle(
        container_root,
        mode,
        crate::filesystem_admission::ProjectRootRequirement::Existing,
    )?;
    admission.revalidate_identity()?;
    let root = canonical_supported_root(admission.root())?;
    let transaction_lock = acquire_transaction_lock(&root, request)?;
    let parent = resolve_project_generation(&root)?;
    let identity = admission.into_identity()?;
    stage_project_generation_inner_with_locks(
        StagedAdmission::Optimistic(Some(identity)),
        root,
        PublicationLock::Optimistic(transaction_lock),
        parent,
        request,
        None,
        Some(operation_fingerprint),
        graph_tree,
        ParticipantPayloads::Memory,
        None,
    )
}

/// Stage a generation using a writer lock and parent resolved by a composed
/// storage operation such as complete-workspace checkpoint revert.
///
/// Pass `graph_tree` when the request's `graph`/`files` inventory must be
/// staged from a non-parent source (for example a pinned checkpoint generation).
pub(crate) fn stage_project_generation_with_lock(
    identity: crate::filesystem_admission::ProjectRootIdentity,
    root: PathBuf,
    writer_lock: File,
    parent: ResolvedProjectGeneration,
    request: &ProjectGenerationRequest,
    revert: Option<RevertJournalExtension>,
    graph_tree: Option<&Path>,
) -> Result<ProjectStageOutcome, GfError> {
    identity.revalidate_identity()?;
    stage_project_generation_inner_with_locks(
        StagedAdmission::Exclusive(identity),
        root,
        PublicationLock::Exclusive(writer_lock),
        parent,
        request,
        revert,
        None,
        graph_tree,
        ParticipantPayloads::Memory,
        None,
    )
}

fn after_preparing_journal(request: &ProjectGenerationRequest) -> Result<(), GfError> {
    project_failpoint::hit(
        "project.after_journal_preparing",
        Some(request.transaction_uuid),
        Some(request.generation_uuid),
        "PREPARING",
        false,
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "the shared staging kernel keeps admission, lock, parent, request, revert, optimistic identity, and graph-tree authority explicit"
)]
fn stage_project_generation_inner_with_locks(
    admission: StagedAdmission,
    root: PathBuf,
    publication_lock: PublicationLock,
    parent: ResolvedProjectGeneration,
    request: &ProjectGenerationRequest,
    revert: Option<RevertJournalExtension>,
    operation_fingerprint: Option<[u8; 32]>,
    graph_tree: Option<&Path>,
    payloads: ParticipantPayloads<'_>,
    allocation: Option<&crate::StorageAllocationOperation>,
) -> Result<ProjectStageOutcome, GfError> {
    validate_request(request)?;
    let (capabilities, participants, request_fingerprint) =
        request_metadata_with_payloads(request, payloads)?;
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
            allocation,
        )?
    {
        return Ok(outcome);
    }

    let requires_promotion = matches!(publication_lock, PublicationLock::Optimistic(_));
    let generation_root =
        prepare_generation_directory(&root, request, &request_fingerprint, requires_promotion)?;
    write_journal_with_allocation(
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
        allocation,
    )?;
    after_preparing_journal(request)?;

    stage_participant_files(
        request,
        &generation_root,
        &participants,
        payloads,
        allocation,
    )?;
    sync_participant_directories(&generation_root.join(PARTICIPANTS_DIR), &participants)?;
    stage_optional_graph_tree(
        &participants,
        &parent,
        &generation_root,
        graph_tree,
        allocation,
    )?;
    project_failpoint::hit(
        "project.after_participant_dir_fsync",
        Some(request.transaction_uuid),
        Some(request.generation_uuid),
        "STAGED",
        false,
    )?;
    write_journal_with_allocation(
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
        allocation,
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
            allocation: allocation.cloned(),
            admission,
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

pub(crate) fn wait_for_writer_lock(root: &Path) -> Result<File, GfError> {
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
    allocation: Option<&crate::StorageAllocationOperation>,
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
        cleanup_aborted_attempts(root, request.transaction_uuid, allocation)?;
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

fn cleanup_aborted_attempts(
    root: &Path,
    transaction_uuid: Uuid,
    allocation: Option<&crate::StorageAllocationOperation>,
) -> Result<(), GfError> {
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
    crate::project_recovery::remove_recovery_tree_with_allocation(&transaction_root, allocation)?;
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
        self.admission.revalidate_identity()?;
        for participant in &self.participants {
            verify_participant_file(
                &self
                    .generation_root
                    .join(PARTICIPANTS_DIR)
                    .join(&participant.relative_path),
                participant,
            )?;
        }
        verify_optional_generation_graph_tree(&self.generation_root, &self.participants)?;
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
        write_journal_with_allocation(
            &self.journal_path(),
            &self.journal(JournalPhase::Validated, None),
            self.allocation.as_ref(),
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

type ReaderPreparation<'a> = &'a mut dyn FnMut(&ResolvedProjectGeneration) -> Result<(), GfError>;

impl ValidatedProjectGeneration {
    /// Durably install the generation, then atomically replace `CURRENT`.
    ///
    /// # Errors
    /// Returns a stable publication error whose diagnostic states whether the
    /// commit point was crossed.
    pub fn publish(mut self) -> Result<ProjectPublicationReceipt, GfError> {
        self.publish_with_optional_graph_objects(None, None, None)
    }

    /// Publish a compact graph generation while holding its CAS lease through
    /// the final closure and named-root validation immediately before CURRENT.
    pub fn publish_with_graph_objects(
        mut self,
        lease: &crate::GraphObjectPublicationLease,
    ) -> Result<ProjectPublicationReceipt, GfError> {
        self.publish_with_optional_graph_objects(Some(lease), None, None)
    }

    /// Publish a compact graph while polling cancellation until the atomic
    /// `CURRENT` replacement commit point.
    pub(crate) fn publish_with_graph_objects_cancellable(
        mut self,
        lease: &crate::GraphObjectPublicationLease,
        cancelled: &mut dyn FnMut() -> bool,
    ) -> Result<ProjectPublicationReceipt, GfError> {
        self.publish_with_optional_graph_objects(Some(lease), Some(cancelled), None)
    }

    /// Prepare authenticated readers against the durable candidate before CURRENT.
    /// The callback runs under the normal publication lock and cannot select a generation.
    ///
    /// # Errors
    /// Callback failure leaves CURRENT unchanged; publication errors retain their commit status.
    pub fn publish_with_reader_preparation(
        mut self,
        lease: Option<&crate::GraphObjectPublicationLease>,
        prepare: &mut dyn FnMut(&ResolvedProjectGeneration) -> Result<(), GfError>,
    ) -> Result<ProjectPublicationReceipt, GfError> {
        self.publish_with_optional_graph_objects(lease, None, Some(prepare))
    }

    fn publish_with_optional_graph_objects(
        &mut self,
        graph_object_lease: Option<&crate::GraphObjectPublicationLease>,
        cancellation: Option<&mut dyn FnMut() -> bool>,
        preparation: Option<ReaderPreparation<'_>>,
    ) -> Result<ProjectPublicationReceipt, GfError> {
        self.0.admission.revalidate_identity()?;
        let lifecycle_admission = self.0.admission.readmit_for_publish()?;
        let commit_lock = self.prepare_commit_lock()?;
        if let Some(admission) = &lifecycle_admission {
            admission.revalidate_identity()?;
        } else {
            self.0.admission.revalidate_identity()?;
        }
        let result = self
            .publish_inner(
                graph_object_lease,
                lifecycle_admission.as_ref(),
                cancellation,
                preparation,
            )
            .map_err(|error| {
                if matches!(error, GfError::Project { .. } | GfError::Api { .. }) {
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
        drop(lifecycle_admission);
        result
    }

    fn prepare_commit_lock(&self) -> Result<Option<CommitLock>, GfError> {
        if matches!(self.0.publication_lock, PublicationLock::Exclusive(_)) {
            return Ok(None);
        }
        let staged = &self.0;
        let writer_lock = CommitLock(wait_for_writer_lock(&staged.root)?);
        #[cfg(test)]
        writer_lock_test_barrier(staged.transaction_uuid);
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

    fn publish_inner(
        &self,
        graph_object_lease: Option<&crate::GraphObjectPublicationLease>,
        lifecycle_admission: Option<&crate::filesystem_admission::ProjectLifecycleAdmission>,
        cancellation: Option<&mut dyn FnMut() -> bool>,
        preparation: Option<ReaderPreparation<'_>>,
    ) -> Result<ProjectPublicationReceipt, GfError> {
        let staged = &self.0;
        let compact_graph = has_compact_graph_participant(staged)?;
        if compact_graph && graph_object_lease.is_none() {
            return Err(project_error(
                ProjectErrorCode::PublicationFailed,
                "compact graph publication requires its graph object lease through CURRENT",
            ));
        }
        if let Some(lease) = graph_object_lease {
            lease.revalidate_for_root(staged.parent.container_root())?;
        }
        let manifest_sha256 = make_generation_durable(staged)?;
        if let Some(lease) = graph_object_lease {
            // Durability promotes optimistic attempts before the final CAS
            // closure check. Authenticate the installed generation, not its
            // former private attempt path.
            let durable_root = staged
                .root
                .join(GENERATIONS_DIR)
                .join(staged.generation_uuid.hyphenated().to_string());
            verify_optional_graph_tree_with_lease(&durable_root, &staged.participants, lease)?;
            lease.revalidate_for_root(staged.parent.container_root())?;
        }
        if let Some(prepare) = preparation {
            let candidate = crate::resolve_verified_generation(
                &staged.root,
                staged.generation_uuid,
                manifest_sha256,
            )?;
            prepare(&candidate)?;
        }
        replace_current(
            staged,
            manifest_sha256,
            graph_object_lease,
            lifecycle_admission,
            cancellation,
        )?;
        finish_published_generation(staged, manifest_sha256)?;
        Ok(ProjectPublicationReceipt {
            transaction_uuid: staged.transaction_uuid,
            generation_uuid: staged.generation_uuid,
            generation_manifest_sha256: manifest_sha256,
            idempotent_replay: false,
        })
    }
}

#[cfg(test)]
struct WriterLockTestBarrier {
    transaction: Uuid,
    acquired: std::sync::mpsc::SyncSender<()>,
    resume: std::sync::mpsc::Receiver<()>,
}

#[cfg(test)]
static WRITER_LOCK_TEST_BARRIER: std::sync::Mutex<Option<WriterLockTestBarrier>> =
    std::sync::Mutex::new(None);

#[cfg(test)]
pub(crate) fn install_writer_lock_test_barrier(
    transaction: Uuid,
    acquired: std::sync::mpsc::SyncSender<()>,
    resume: std::sync::mpsc::Receiver<()>,
) {
    *WRITER_LOCK_TEST_BARRIER
        .lock()
        .expect("writer-lock test barrier lock") = Some(WriterLockTestBarrier {
        transaction,
        acquired,
        resume,
    });
}

#[cfg(test)]
fn writer_lock_test_barrier(transaction: Uuid) {
    let barrier = WRITER_LOCK_TEST_BARRIER
        .lock()
        .expect("writer-lock test barrier lock")
        .take();
    let Some(barrier) = barrier.filter(|barrier| barrier.transaction == transaction) else {
        return;
    };
    barrier.acquired.send(()).expect("report writer lock held");
    barrier.resume.recv().expect("release writer lock barrier");
}

fn has_compact_graph_participant(staged: &StagedProjectGeneration) -> Result<bool, GfError> {
    let Some(files) = staged.participants.iter().find(|participant| {
        participant.capability_id == crate::GRAPH_CAPABILITY_ID
            && participant.record_family_id == crate::GRAPH_FILES_FAMILY
    }) else {
        return Ok(false);
    };
    let path = staged
        .generation_root
        .join(PARTICIPANTS_DIR)
        .join(&files.relative_path);
    let bytes = std::fs::read(&path).map_err(publication_io)?;
    if matches!(
        crate::decode_versioned_graph_files_participant(files.record_version, &bytes)?,
        crate::GraphFilesParticipant::V2(_)
    ) {
        Ok(true)
    } else {
        Ok(false)
    }
}

fn abort_stale_generation(staged: &StagedProjectGeneration) -> Result<(), GfError> {
    write_journal_with_allocation(
        &staged.journal_path(),
        &staged.journal(JournalPhase::Aborted, None),
        staged.allocation.as_ref(),
    )?;
    if staged.generation_root.exists() {
        crate::project_recovery::remove_recovery_tree_with_allocation(
            &staged.generation_root,
            staged.allocation.as_ref(),
        )?;
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
    if let Some(allocation) = &staged.allocation {
        allocation.replace_file_at(&lease_path, &lease)?;
    }
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
    let manifest_file = write_new(&manifest_path, &manifest_bytes, staged.allocation.as_ref())?;
    project_failpoint::hit(
        "project.after_manifest_write",
        Some(staged.transaction_uuid),
        Some(staged.generation_uuid),
        "DURABLE",
        false,
    )?;
    manifest_file.sync_all().map_err(publication_io)?;
    if let Some(allocation) = &staged.allocation {
        allocation.replace_file_at(&manifest_path, &manifest_file)?;
    }
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
    verify_optional_generation_graph_tree(&staged.generation_root, &staged.participants)?;
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
    write_journal_with_allocation(
        &staged.journal_path(),
        &staged.journal(JournalPhase::Durable, Some(hex_digest(manifest_sha256))),
        staged.allocation.as_ref(),
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
    graph_object_lease: Option<&crate::GraphObjectPublicationLease>,
    lifecycle_admission: Option<&crate::filesystem_admission::ProjectLifecycleAdmission>,
    mut cancellation: Option<&mut dyn FnMut() -> bool>,
) -> Result<(), GfError> {
    let current = CurrentRecord {
        format: "graphforge-project".into(),
        format_version: 1,
        generation_uuid: staged.generation_uuid.hyphenated().to_string(),
        generation_manifest_sha256: hex_digest(manifest_sha256),
    };
    let current_bytes = canonical_line(&current)?;
    let current_path = staged.root.join(CURRENT_FILE);
    let stable_root =
        graphforge_filesystem::StableDirectory::open(&staged.root).map_err(publication_io)?;
    let replace_result = publish_atomic_bytes_in(
        &stable_root,
        &current_path,
        std::ffi::OsStr::new(CURRENT_FILE),
        &current_bytes,
        || {
            failpoint_as_io(
                "project.after_current_temp_write",
                staged.transaction_uuid,
                staged.generation_uuid,
                "CURRENT",
                false,
            )
        },
        || {
            failpoint_as_io(
                "project.after_current_temp_fsync",
                staged.transaction_uuid,
                staged.generation_uuid,
                "CURRENT",
                false,
            )
        },
        || {
            failpoint_as_io(
                "project.before_current_replace",
                staged.transaction_uuid,
                staged.generation_uuid,
                "CURRENT",
                false,
            )?;
            if let Some(admission) = lifecycle_admission {
                admission
                    .revalidate_identity()
                    .map_err(std::io::Error::other)?;
            } else {
                staged
                    .admission
                    .revalidate_identity()
                    .map_err(std::io::Error::other)?;
            }
            if let Some(lease) = graph_object_lease {
                lease
                    .revalidate_for_root(staged.parent.container_root())
                    .map_err(std::io::Error::other)?;
            }
            // This is deliberately the final fallible predicate before the
            // single native replacement commit point. After replacement,
            // reconciliation owns the result and cancellation cannot undo it.
            if cancellation.as_mut().is_some_and(|cancelled| cancelled()) {
                return Err(std::io::Error::other(CancelledBeforeReplace));
            }
            Ok(())
        },
        staged.allocation.as_ref(),
    );
    if let Err(error) = replace_result {
        reconcile_current_replacement_error(
            &staged.root,
            staged.transaction_uuid,
            staged.generation_uuid,
            manifest_sha256,
            &error,
        )?;
    }
    project_failpoint::hit(
        "project.after_current_replace",
        Some(staged.transaction_uuid),
        Some(staged.generation_uuid),
        "CURRENT",
        true,
    )
}

fn reconcile_current_replacement_error(
    root: &Path,
    transaction_uuid: Uuid,
    generation_uuid: Uuid,
    manifest_sha256: [u8; 32],
    error: &AtomicPublishError,
) -> Result<(), GfError> {
    // `CancelledBeforeReplace` is emitted only by the final predicate inside
    // `before_replace`; the native namespace operation has not started, so
    // committed=false is proven without consulting recovery-visible journals.
    if matches!(error, AtomicPublishError::CancelledBeforeReplace) {
        return Err(GfError::Api {
            code: ApiErrorCode::Cancelled,
            message: format!(
                "transaction_uuid={} generation_uuid={} boundary=project.before_current_replace committed=false",
                transaction_uuid.hyphenated(),
                generation_uuid.hyphenated()
            ),
        });
    }
    // The native primitive distinguishes a proved no-op from an outcome whose
    // namespace state requires reconciliation. Re-read CURRENT under the
    // still-held writer lock for every error so callers never receive
    // committed=false after the child actually became authoritative.
    let resolved = resolve_project_generation(root).map_err(|authority_error| {
        project_error(
            ProjectErrorCode::ProjectCorrupt,
            format!(
                "CURRENT authority could not be reconciled after native replacement error: {}",
                safe_cause(&authority_error.to_string())
            ),
        )
    })?;
    if resolved.generation_uuid() == generation_uuid
        && resolved.manifest_sha256() == manifest_sha256
    {
        return Ok(());
    }
    Err(publication_error_from_parts(
        transaction_uuid,
        generation_uuid,
        "CURRENT",
        false,
        &error.to_string(),
    ))
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
    write_journal_with_allocation(
        &staged.journal_path(),
        &staged.journal(JournalPhase::Published, Some(hex_digest(manifest_sha256))),
        staged.allocation.as_ref(),
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

fn canonical_supported_root(root: &Path) -> Result<PathBuf, GfError> {
    // Resolution validates FORMAT, CURRENT, containment, and link policy.
    resolve_project_generation(root).map(|resolved| resolved.container_root().to_owned())
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
mod tests;
