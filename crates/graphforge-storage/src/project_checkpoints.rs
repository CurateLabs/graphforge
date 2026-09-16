//! Durable named references to verified immutable project generations.
//!
//! Registry authentication and pair recovery live in `registry`; revert participant
//! construction lives in `restoration`. Lifecycle and lock ownership stay here.

mod registry;
use registry::{
    commit_registry, decode_digest, hex, read_registry, read_registry_for_read, recover_pair,
    registry_corrupt,
};
mod restoration;
use restoration::{
    restoration_participant, restoration_uuid, restored_generation_uuid, revert_request_digest,
    revert_transaction_uuid, snapshot_to_participant,
};

use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{
    ArrayRef, FixedSizeBinaryBuilder, StringArray, TimestampMicrosecondArray, UInt32Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use graphforge_core::canonical::{CANONICAL_CONTRACT_VERSION, CanonicalDomain, fingerprint};
use graphforge_core::{GfError, ProjectErrorCode};
use parquet::arrow::ArrowWriter;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use unicode_normalization::UnicodeNormalization;
use uuid::Uuid;

use crate::project_failpoint;
use crate::project_generation::resolve_verified_generation;
use crate::project_publication::{
    LOCKS_DIR, ProjectCapability, ProjectGenerationRequest, ProjectParticipant,
    ProjectParticipantEncoding, ProjectStageOutcome, RevertJournalExtension, WRITER_LOCK_FILE,
    ensure_machine_directory, load_published_revert, load_revert_journal_extension,
    open_regular_lock, stage_project_generation_with_lock, sync_directory,
};
use crate::resolve_project_generation;

const CHECKPOINTS_DIR: &str = "checkpoints";
const REGISTRY_FILE: &str = "registry.json";
const CHECKSUM_FILE: &str = "registry.json.sha256";
const INTENT_FILE: &str = "registry.txn.json";
const CHECKPOINT_LOCK_FILE: &str = "checkpoints.lock";
const MAX_REGISTRY_BYTES: u64 = 8 * 1024 * 1024;
const MAX_ACTIVE: usize = 1_024;
const MAX_TOMBSTONES: usize = 4_096;
const MAX_NAME_BYTES: usize = 128;
const MAX_DESCRIPTION_BYTES: usize = 1_024;
const MAX_REASON_BYTES: usize = 1_024;
const RESTORATION_FAMILY: &str = "restoration_transition";
const RESTORATION_CONTRACT_VERSION: u32 = 1;

/// Input for an idempotent checkpoint creation.
#[derive(Debug, Clone)]
pub struct CheckpointCreateRequest {
    /// Canonical operation UUID.
    pub operation_uuid: Uuid,
    /// Human-facing checkpoint name (content, never a path).
    pub name: String,
    /// Optional bounded description.
    pub description: Option<String>,
    /// Optional actor identity.
    pub actor_uuid: Option<Uuid>,
}

/// Input for an idempotent checkpoint deletion.
#[derive(Debug, Clone)]
pub struct CheckpointDeleteRequest {
    /// Canonical operation UUID.
    pub operation_uuid: Uuid,
    /// Exact normalized checkpoint name.
    pub name: String,
    /// Optional actor identity.
    pub actor_uuid: Option<Uuid>,
}

/// Internal complete-workspace revert request after the API selects its clock.
#[derive(Debug, Clone)]
pub struct CheckpointRevertRequest {
    /// Caller-controlled idempotency UUID.
    pub operation_uuid: Uuid,
    /// Canonical checkpoint name.
    pub name: String,
    /// Bounded human restoration reason.
    pub reason: String,
    /// Optional actor identity.
    pub actor_uuid: Option<Uuid>,
}

/// One active durable checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointRecord {
    /// Stable deterministic identity.
    pub checkpoint_uuid: Uuid,
    /// Normalized display name.
    pub name: String,
    /// Exact pinned generation.
    pub generation_uuid: Uuid,
    /// Digest of that generation's canonical manifest.
    pub generation_manifest_sha256: String,
    /// Optional description.
    pub description: Option<String>,
    /// Engine-supplied UTC microseconds.
    pub created_at: i64,
    /// Optional actor identity.
    pub created_by: Option<Uuid>,
    /// Idempotency operation.
    pub create_operation_uuid: Uuid,
    /// Canonical request digest.
    pub create_request_sha256: String,
    /// Registry revision that originally committed this checkpoint.
    pub created_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckpointTombstone {
    checkpoint_uuid: Uuid,
    name: String,
    generation_uuid: Uuid,
    generation_manifest_sha256: String,
    description: Option<String>,
    created_at: i64,
    created_by: Option<Uuid>,
    create_operation_uuid: Uuid,
    create_request_sha256: String,
    created_revision: u64,
    deleted_at: i64,
    deleted_by: Option<Uuid>,
    delete_operation_uuid: Uuid,
    delete_request_sha256: String,
    deleted_revision: u64,
}

/// Stable mutation receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointReceipt {
    /// Operation name (`checkpoint`, `delete_checkpoint`, or `revert_to_checkpoint`).
    pub operation: &'static str,
    /// Idempotency UUID.
    pub operation_uuid: Uuid,
    /// Stable checkpoint UUID.
    pub checkpoint_uuid: Uuid,
    /// Checkpoint name.
    pub name: String,
    /// Pinned generation UUID.
    pub source_generation_uuid: Uuid,
    /// Generation that was current immediately before a revert; absent for registry-only operations.
    pub prior_current_generation_uuid: Option<Uuid>,
    /// Newly published generation for revert; absent for registry-only operations.
    pub result_generation_uuid: Option<Uuid>,
    /// Resulting registry revision.
    pub registry_revision: u64,
    /// Original commit time in UTC microseconds.
    pub committed_at: i64,
}

struct MutationLocks {
    writer: Option<File>,
    checkpoint: Option<File>,
}

struct CheckpointReadLock(File);

impl Drop for CheckpointReadLock {
    fn drop(&mut self) {
        let _ = crate::file_lock::unlock(&self.0);
    }
}

impl MutationLocks {
    fn transfer_writer_for_revert_publication(&mut self) -> File {
        self.writer
            .take()
            .expect("writer lock must be present until revert publication")
    }

    fn release_revert_replay(mut self) -> Result<(), GfError> {
        let checkpoint = self
            .checkpoint
            .take()
            .expect("checkpoint lock must be present");
        let writer = self.writer.take().expect("writer lock must be present");
        release_revert_replay_locks(&checkpoint, &writer)
    }
}

impl Drop for MutationLocks {
    fn drop(&mut self) {
        if let Some(checkpoint) = &self.checkpoint {
            let _ = crate::file_lock::unlock(checkpoint);
        }
        if let Some(writer) = &self.writer {
            let _ = crate::file_lock::unlock(writer);
        }
    }
}

/// Create a checkpoint pinned to the post-lock validated `CURRENT` generation.
pub fn create_checkpoint(
    container_root: impl AsRef<Path>,
    request: &CheckpointCreateRequest,
) -> Result<CheckpointReceipt, GfError> {
    create_checkpoint_with_mode(
        container_root,
        request,
        crate::filesystem_admission::ProjectLifecycleMode::Durable,
    )
}

/// Create a checkpoint using the lifecycle mode established by the owning facade.
///
/// # Errors
/// Returns the same errors as [`create_checkpoint`].
pub fn create_checkpoint_with_mode(
    container_root: impl AsRef<Path>,
    request: &CheckpointCreateRequest,
    mode: crate::filesystem_admission::ProjectLifecycleMode,
) -> Result<CheckpointReceipt, GfError> {
    let name = validate_name(&request.name)?;
    validate_description(request.description.as_deref())?;
    let admission = admit_existing_project(container_root.as_ref(), mode)?;
    let root = canonical_project_root(admission.root())?;
    let _locks = acquire_mutation_locks(&root)?;
    let checkpoint_root = checkpoint_root(&root)?;
    recover_pair(&checkpoint_root)?;
    let mut registry = read_registry(&checkpoint_root)?;
    let request_digest = create_request_digest(request, &name);
    let request_hex = hex(&request_digest);

    if let Some(row) = registry
        .active
        .iter()
        .find(|row| row.create_operation_uuid == request.operation_uuid)
    {
        if row.create_request_sha256 == request_hex {
            return Ok(create_receipt(row));
        }
        return Err(project_error(
            ProjectErrorCode::TransactionConflict,
            "checkpoint operation UUID was reused with different canonical request bytes",
        ));
    }
    if registry
        .tombstones
        .iter()
        .any(|row| row.delete_operation_uuid == request.operation_uuid)
    {
        return Err(project_error(
            ProjectErrorCode::TransactionConflict,
            "checkpoint operation UUID was already used by delete_checkpoint",
        ));
    }
    if let Some(row) = registry
        .tombstones
        .iter()
        .find(|row| row.create_operation_uuid == request.operation_uuid)
    {
        if row.create_request_sha256 == request_hex {
            return Ok(create_tombstone_receipt(row));
        }
        return Err(project_error(
            ProjectErrorCode::TransactionConflict,
            "checkpoint create operation UUID was reused with different canonical request bytes",
        ));
    }
    if registry.active.iter().any(|row| row.name == name) {
        return Err(project_error(
            ProjectErrorCode::CheckpointExists,
            "checkpoint name already exists",
        ));
    }
    if registry.active.len() >= MAX_ACTIVE {
        return Err(project_error(
            ProjectErrorCode::ResourceLimit,
            "active checkpoint limit is 1024",
        ));
    }

    let selected = resolve_project_generation(&root)?;
    let now = utc_micros()?;
    let checkpoint_uuid = checkpoint_uuid(request.operation_uuid, request_digest);
    let revision = registry.revision.checked_add(1).ok_or_else(|| {
        project_error(
            ProjectErrorCode::ResourceLimit,
            "checkpoint registry revision overflow",
        )
    })?;
    let row = CheckpointRecord {
        checkpoint_uuid,
        name,
        generation_uuid: selected.generation_uuid(),
        generation_manifest_sha256: hex(&selected.manifest_sha256()),
        description: request.description.clone(),
        created_at: now,
        created_by: request.actor_uuid,
        create_operation_uuid: request.operation_uuid,
        create_request_sha256: request_hex,
        created_revision: revision,
    };
    registry.revision = revision;
    registry.active.push(row.clone());
    registry.active.sort_by(|left, right| {
        (&left.name, left.checkpoint_uuid).cmp(&(&right.name, right.checkpoint_uuid))
    });
    commit_registry(&checkpoint_root, &registry, request.operation_uuid)?;
    Ok(create_receipt(&row))
}

/// Delete one active checkpoint while preserving any already-open generation lease.
pub fn delete_checkpoint(
    container_root: impl AsRef<Path>,
    request: &CheckpointDeleteRequest,
) -> Result<CheckpointReceipt, GfError> {
    delete_checkpoint_with_mode(
        container_root,
        request,
        crate::filesystem_admission::ProjectLifecycleMode::Durable,
    )
}

/// Delete a checkpoint using the lifecycle mode established by the owning facade.
///
/// # Errors
/// Returns the same errors as [`delete_checkpoint`].
pub fn delete_checkpoint_with_mode(
    container_root: impl AsRef<Path>,
    request: &CheckpointDeleteRequest,
    mode: crate::filesystem_admission::ProjectLifecycleMode,
) -> Result<CheckpointReceipt, GfError> {
    let name = validate_name(&request.name)?;
    let admission = admit_existing_project(container_root.as_ref(), mode)?;
    let root = canonical_project_root(admission.root())?;
    let _locks = acquire_mutation_locks(&root)?;
    let checkpoint_root = checkpoint_root(&root)?;
    recover_pair(&checkpoint_root)?;
    let mut registry = read_registry(&checkpoint_root)?;
    let digest = delete_request_digest(request, &name);
    let digest_hex = hex(&digest);
    if let Some(row) = registry
        .tombstones
        .iter()
        .find(|row| row.delete_operation_uuid == request.operation_uuid)
    {
        if row.delete_request_sha256 == digest_hex {
            return Ok(delete_receipt(row));
        }
        return Err(project_error(
            ProjectErrorCode::TransactionConflict,
            "checkpoint delete operation UUID was reused with different canonical request bytes",
        ));
    }
    if registry
        .active
        .iter()
        .any(|row| row.create_operation_uuid == request.operation_uuid)
        || registry
            .tombstones
            .iter()
            .any(|row| row.create_operation_uuid == request.operation_uuid)
    {
        return Err(project_error(
            ProjectErrorCode::TransactionConflict,
            "checkpoint operation UUID was already used by checkpoint",
        ));
    }
    let index = registry
        .active
        .iter()
        .position(|row| row.name == name)
        .ok_or_else(|| {
            project_error(
                ProjectErrorCode::CheckpointNotFound,
                "checkpoint name does not exist",
            )
        })?;
    let row = registry.active.remove(index);
    let now = utc_micros()?;
    registry.revision = registry.revision.checked_add(1).ok_or_else(|| {
        project_error(
            ProjectErrorCode::ResourceLimit,
            "checkpoint registry revision overflow",
        )
    })?;
    let tombstone = CheckpointTombstone {
        checkpoint_uuid: row.checkpoint_uuid,
        name: row.name,
        generation_uuid: row.generation_uuid,
        generation_manifest_sha256: row.generation_manifest_sha256,
        description: row.description,
        created_at: row.created_at,
        created_by: row.created_by,
        create_operation_uuid: row.create_operation_uuid,
        create_request_sha256: row.create_request_sha256,
        created_revision: row.created_revision,
        deleted_at: now,
        deleted_by: request.actor_uuid,
        delete_operation_uuid: request.operation_uuid,
        delete_request_sha256: digest_hex,
        deleted_revision: registry.revision,
    };
    registry.tombstones.push(tombstone.clone());
    registry
        .tombstones
        .sort_by_key(|row| (row.deleted_revision, row.checkpoint_uuid));
    if registry.tombstones.len() > MAX_TOMBSTONES {
        registry
            .tombstones
            .drain(..registry.tombstones.len() - MAX_TOMBSTONES);
    }
    commit_registry(&checkpoint_root, &registry, request.operation_uuid)?;
    Ok(delete_receipt(&tombstone))
}

/// Return active checkpoints in canonical `(name, checkpoint_uuid)` order.
pub fn list_checkpoints(
    container_root: impl AsRef<Path>,
) -> Result<Vec<CheckpointRecord>, GfError> {
    list_checkpoints_with_mode(
        container_root,
        crate::filesystem_admission::ProjectLifecycleMode::Durable,
    )
}

/// List checkpoints using the lifecycle mode established by the owning facade.
///
/// # Errors
/// Returns the same errors as [`list_checkpoints`].
pub fn list_checkpoints_with_mode(
    container_root: impl AsRef<Path>,
    mode: crate::filesystem_admission::ProjectLifecycleMode,
) -> Result<Vec<CheckpointRecord>, GfError> {
    let admission = admit_existing_project(container_root.as_ref(), mode)?;
    let root = canonical_project_root(admission.root())?;
    let checkpoint_root = checkpoint_root(&root)?;
    let (_checkpoint_lock, registry) = read_registry_for_read(&root, &checkpoint_root)?;
    Ok(registry.active)
}

/// Resolve and lifetime-pin the exact generation named by an active checkpoint.
pub fn open_checkpoint_generation(
    container_root: impl AsRef<Path>,
    name: &str,
) -> Result<(CheckpointRecord, crate::ResolvedProjectGeneration), GfError> {
    open_checkpoint_generation_with_mode(
        container_root,
        name,
        crate::filesystem_admission::ProjectLifecycleMode::Durable,
    )
}

/// Open a checkpoint using the lifecycle mode established by the owning facade.
///
/// # Errors
/// Returns the same errors as [`open_checkpoint_generation`].
pub fn open_checkpoint_generation_with_mode(
    container_root: impl AsRef<Path>,
    name: &str,
    mode: crate::filesystem_admission::ProjectLifecycleMode,
) -> Result<(CheckpointRecord, crate::ResolvedProjectGeneration), GfError> {
    let name = validate_name(name)?;
    let admission = admit_existing_project(container_root.as_ref(), mode)?;
    let root = canonical_project_root(admission.root())?;
    let checkpoint_root = checkpoint_root(&root)?;
    let (_checkpoint_lock, registry) = read_registry_for_read(&root, &checkpoint_root)?;
    let row = registry
        .active
        .iter()
        .find(|row| row.name == name)
        .cloned()
        .ok_or_else(|| {
            project_error(
                ProjectErrorCode::CheckpointNotFound,
                "checkpoint name does not exist",
            )
        })?;
    let generation = resolve_verified_generation(
        &root,
        row.generation_uuid,
        decode_digest(&row.generation_manifest_sha256)?,
    )?;
    let after = read_registry(&checkpoint_root)?;
    if after.revision != registry.revision
        || !after.active.iter().any(|candidate| candidate == &row)
    {
        return Err(project_error(
            ProjectErrorCode::CheckpointNotFound,
            "checkpoint changed while its generation was being pinned",
        ));
    }
    Ok((row, generation))
}

/// Publish a complete-workspace restoration as a new child generation.
pub fn revert_checkpoint<T, V>(
    container_root: impl AsRef<Path>,
    request: &CheckpointRevertRequest,
    select_timestamp: T,
    validate_source: V,
) -> Result<(CheckpointReceipt, crate::ResolvedProjectGeneration), GfError>
where
    T: FnOnce() -> Result<i64, GfError>,
    V: FnOnce(&crate::ResolvedProjectGeneration) -> Result<(), GfError>,
{
    revert_checkpoint_with_mode(
        container_root,
        request,
        select_timestamp,
        validate_source,
        crate::filesystem_admission::ProjectLifecycleMode::Durable,
    )
}

/// Revert a checkpoint using the lifecycle mode established by the owning facade.
///
/// # Errors
/// Returns the same errors as [`revert_checkpoint`].
#[expect(
    clippy::too_many_lines,
    reason = "the revert transaction is intentionally linear so lock ownership and publication order remain auditable"
)]
pub fn revert_checkpoint_with_mode<T, V>(
    container_root: impl AsRef<Path>,
    request: &CheckpointRevertRequest,
    select_timestamp: T,
    validate_source: V,
    mode: crate::filesystem_admission::ProjectLifecycleMode,
) -> Result<(CheckpointReceipt, crate::ResolvedProjectGeneration), GfError>
where
    T: FnOnce() -> Result<i64, GfError>,
    V: FnOnce(&crate::ResolvedProjectGeneration) -> Result<(), GfError>,
{
    let requested_name = validate_name(&request.name)?;
    let requested_reason = validate_reason(&request.reason)?;
    let admission = admit_existing_project(container_root.as_ref(), mode)?;
    let root = canonical_project_root(admission.root())?;
    let transaction_uuid = revert_transaction_uuid(request.operation_uuid);
    // Global lifecycle order is CAS -> writer -> checkpoint. Retain the
    // shared CAS publication guard before acquiring either mutation lock;
    // compact sources need it through CURRENT, while v1 holds it unused.
    let graph_object_lease = crate::begin_graph_object_publication(&root)?;
    let mut locks = acquire_mutation_locks(&root)?;
    let checkpoint_root = checkpoint_root(&root)?;
    recover_pair(&checkpoint_root)?;
    let registry = read_registry(&checkpoint_root)?;
    let prior_current = resolve_project_generation(&root)?;

    if let Some((extension, receipt)) = load_published_revert(&root, transaction_uuid)? {
        validate_revert_replay_request(request, &requested_name, &requested_reason, &extension)?;
        let resolved = resolve_verified_generation(
            &root,
            receipt.generation_uuid,
            receipt.generation_manifest_sha256,
        )?;
        validate_source(&resolved)?;
        let replay = revert_receipt(
            request,
            &requested_name,
            &extension,
            receipt.generation_uuid,
        )?;
        locks.release_revert_replay()?;
        return Ok((replay, resolved));
    }

    let prior_extension = load_revert_journal_extension(&root, transaction_uuid)?;
    let (checkpoint, source, restored_at, registry_revision) =
        if let Some(extension) = prior_extension.as_ref() {
            let checkpoint_uuid = parse_uuid(&extension.checkpoint_uuid)?;
            let source_uuid = parse_uuid(&extension.source_generation_uuid)?;
            let source_digest = decode_digest(&extension.source_manifest_sha256)?;
            let source = resolve_verified_generation(&root, source_uuid, source_digest)?;
            let row = CheckpointRecord {
                checkpoint_uuid,
                name: extension.checkpoint_name.clone(),
                generation_uuid: source_uuid,
                generation_manifest_sha256: extension.source_manifest_sha256.clone(),
                description: None,
                created_at: 0,
                created_by: None,
                create_operation_uuid: Uuid::nil(),
                create_request_sha256: "0".repeat(64),
                created_revision: extension.registry_revision,
            };
            (
                row,
                source,
                extension.restored_at,
                extension.registry_revision,
            )
        } else {
            let row = registry
                .active
                .iter()
                .find(|row| row.name == requested_name)
                .cloned()
                .ok_or_else(|| {
                    project_error(
                        ProjectErrorCode::CheckpointNotFound,
                        "checkpoint name does not exist",
                    )
                })?;
            let source = resolve_verified_generation(
                &root,
                row.generation_uuid,
                decode_digest(&row.generation_manifest_sha256)?,
            )?;
            (row, source, select_timestamp()?, registry.revision)
        };

    let request_digest = revert_request_digest(
        request.operation_uuid,
        &requested_name,
        checkpoint.checkpoint_uuid,
        source.generation_uuid(),
        source.manifest_sha256(),
        &requested_reason,
        request.actor_uuid,
    );
    let request_hex = hex(&request_digest);
    let restoration_uuid = restoration_uuid(request.operation_uuid, request_digest);
    let original_prior_uuid = prior_extension.as_ref().map_or_else(
        || Ok(prior_current.generation_uuid()),
        |value| parse_uuid(&value.prior_current_generation_uuid),
    )?;
    let generation_uuid = restored_generation_uuid(
        transaction_uuid,
        checkpoint.checkpoint_uuid,
        source.generation_uuid(),
        source.manifest_sha256(),
        original_prior_uuid,
        restored_at,
        request_digest,
    );
    let expected_extension = RevertJournalExtension {
        operation_uuid: request.operation_uuid.to_string(),
        request_sha256: request_hex,
        checkpoint_uuid: checkpoint.checkpoint_uuid.to_string(),
        checkpoint_name: requested_name.clone(),
        source_generation_uuid: source.generation_uuid().to_string(),
        source_manifest_sha256: hex(&source.manifest_sha256()),
        prior_current_generation_uuid: original_prior_uuid.to_string(),
        restored_at,
        reason: requested_reason.clone(),
        actor_uuid: request.actor_uuid.map(|value| value.to_string()),
        restoration_uuid: restoration_uuid.to_string(),
        registry_revision,
    };
    if prior_extension
        .as_ref()
        .is_some_and(|value| value != &expected_extension)
    {
        return Err(project_error(
            ProjectErrorCode::TransactionConflict,
            "revert operation UUID was reused with different canonical request bytes",
        ));
    }

    validate_source(&source)?;
    let mut participants = source
        .participant_snapshots()?
        .into_iter()
        .filter(|snapshot| {
            !(snapshot.capability_id == crate::WORKSPACE_CAPABILITY_ID
                && snapshot.record_family_id == RESTORATION_FAMILY)
        })
        .map(snapshot_to_participant)
        .collect::<Result<Vec<_>, GfError>>()?;
    participants.push(restoration_participant(
        restoration_uuid,
        checkpoint.checkpoint_uuid,
        source.generation_uuid(),
        source.manifest_sha256(),
        parse_uuid(&expected_extension.prior_current_generation_uuid)?,
        generation_uuid,
        request.operation_uuid,
        request.actor_uuid,
        &requested_reason,
        restored_at,
    )?);
    let capabilities = source
        .capabilities()
        .into_iter()
        .map(|value| ProjectCapability {
            capability_id: value.capability_id,
            capability_version: value.capability_version,
        })
        .collect();
    let publication = ProjectGenerationRequest {
        transaction_uuid,
        generation_uuid,
        capabilities,
        participants,
    };
    let expected_parent_uuid = prior_current.generation_uuid();
    let expected_participants = publication
        .participants
        .iter()
        .map(|row| {
            (
                row.capability_id.clone(),
                row.record_family_id.clone(),
                row.record_version,
                row.row_count,
            )
        })
        .collect::<BTreeSet<_>>();
    let writer = locks.transfer_writer_for_revert_publication();
    let identity = admission.into_identity()?;
    // Revert must stage graph bytes from the pinned source generation. Using
    // the parent's tree (CURRENT) would verify the restored inventory against
    // post-checkpoint mutations and fail closed with length/digest mismatch.
    let source_graph_tree = source.graph_tree_root();
    let source_graph_files = source.declared_graph_files_participant()?;
    let compact_graph_objects = matches!(
        source_graph_files,
        Some(crate::GraphFilesParticipant::V2(_))
    );
    let graph_tree = matches!(
        source_graph_files,
        Some(crate::GraphFilesParticipant::V1(_))
    )
    .then_some(source_graph_tree.as_path());
    let receipt = match stage_project_generation_with_lock(
        identity,
        root.clone(),
        writer,
        prior_current,
        &publication,
        Some(expected_extension),
        graph_tree,
    )? {
        ProjectStageOutcome::AlreadyPublished(receipt) => receipt,
        ProjectStageOutcome::Staged(staged) => {
            let validated = staged
                .validate(
                    |rows| {
                        let actual = rows
                            .iter()
                            .map(|row| {
                                (
                                    row.capability_id.clone(),
                                    row.record_family_id.clone(),
                                    row.record_version,
                                    row.row_count,
                                )
                            })
                            .collect::<BTreeSet<_>>();
                        if rows.len() != expected_participants.len()
                            || actual != expected_participants
                            || rows.iter().filter(|row| {
                                row.capability_id == crate::WORKSPACE_CAPABILITY_ID
                                    && row.record_family_id == RESTORATION_FAMILY
                                    && row.encoding == "parquet"
                                    && row.record_version == RESTORATION_CONTRACT_VERSION
                                    && row.row_count == 1
                            }).count() != 1
                        {
                            return Err(GfError::Validation(
                                "staged revert participant inventory differs from the validated complete snapshot"
                                    .into(),
                            ));
                        }
                        Ok(())
                    },
                    |parent, _| {
                        if parent.generation_uuid() != expected_parent_uuid {
                            return Err(GfError::Validation(
                                "staged revert parent changed after composite validation".into(),
                            ));
                        }
                        Ok(())
                    },
                )?;
            if compact_graph_objects {
                validated.publish_with_graph_objects(&graph_object_lease)?
            } else {
                validated.publish()?
            }
        }
    };
    let resolved = resolve_verified_generation(
        &root,
        receipt.generation_uuid,
        receipt.generation_manifest_sha256,
    )?;
    Ok((
        CheckpointReceipt {
            operation: "revert_to_checkpoint",
            operation_uuid: request.operation_uuid,
            checkpoint_uuid: checkpoint.checkpoint_uuid,
            name: requested_name,
            source_generation_uuid: source.generation_uuid(),
            prior_current_generation_uuid: Some(original_prior_uuid),
            result_generation_uuid: Some(receipt.generation_uuid),
            registry_revision,
            committed_at: restored_at,
        },
        resolved,
    ))
}

fn release_revert_replay_locks(checkpoint: &File, writer: &File) -> Result<(), GfError> {
    let checkpoint_unlock = crate::file_lock::unlock(checkpoint);
    let writer_unlock = crate::file_lock::unlock(writer);
    finish_revert_replay_lock_handoff(checkpoint_unlock, writer_unlock)
}

fn finish_revert_replay_lock_handoff(
    checkpoint_unlock: std::io::Result<()>,
    writer_unlock: std::io::Result<()>,
) -> Result<(), GfError> {
    checkpoint_unlock.map_err(|error| {
        GfError::Storage(format!(
            "checkpoint revert replay lock handoff failed at checkpoints.lock: {error}"
        ))
    })?;
    writer_unlock.map_err(|error| {
        GfError::Storage(format!(
            "checkpoint revert replay lock handoff failed at writer.lock: {error}"
        ))
    })
}

fn validate_revert_replay_request(
    request: &CheckpointRevertRequest,
    name: &str,
    reason: &str,
    extension: &RevertJournalExtension,
) -> Result<(), GfError> {
    if extension.operation_uuid != request.operation_uuid.to_string()
        || extension.checkpoint_name != name
        || extension.reason != reason
        || extension.actor_uuid != request.actor_uuid.map(|value| value.to_string())
    {
        return Err(project_error(
            ProjectErrorCode::TransactionConflict,
            "revert operation UUID was reused with different canonical request bytes",
        ));
    }
    Ok(())
}

fn revert_receipt(
    request: &CheckpointRevertRequest,
    name: &str,
    extension: &RevertJournalExtension,
    result_generation_uuid: Uuid,
) -> Result<CheckpointReceipt, GfError> {
    Ok(CheckpointReceipt {
        operation: "revert_to_checkpoint",
        operation_uuid: request.operation_uuid,
        checkpoint_uuid: parse_uuid(&extension.checkpoint_uuid)?,
        name: name.to_owned(),
        source_generation_uuid: parse_uuid(&extension.source_generation_uuid)?,
        prior_current_generation_uuid: Some(parse_uuid(&extension.prior_current_generation_uuid)?),
        result_generation_uuid: Some(result_generation_uuid),
        registry_revision: extension.registry_revision,
        committed_at: extension.restored_at,
    })
}

pub(crate) struct CheckpointRetentionRoots {
    checkpoint_lock: File,
    pub(crate) roots: Vec<(Uuid, [u8; 32])>,
}

impl Drop for CheckpointRetentionRoots {
    fn drop(&mut self) {
        let _ = crate::file_lock::unlock(&self.checkpoint_lock);
    }
}

pub(crate) fn checkpoint_retention_roots_after_writer_lock(
    root: &Path,
) -> Result<CheckpointRetentionRoots, GfError> {
    let lock_root = ensure_machine_directory(root, Path::new(LOCKS_DIR))?;
    let checkpoint_lock = open_regular_lock(&lock_root.join(CHECKPOINT_LOCK_FILE))?;
    if !crate::file_lock::try_lock_exclusive(&checkpoint_lock).map_err(storage_io)? {
        return Err(project_error(
            ProjectErrorCode::WriterBusy,
            "recovery could not acquire checkpoints.lock after writer.lock",
        ));
    }
    let checkpoint_root = checkpoint_root(root)?;
    recover_pair(&checkpoint_root)?;
    let registry = read_registry(&checkpoint_root)?;
    let roots = registry
        .active
        .into_iter()
        .map(|row| {
            let digest = decode_digest(&row.generation_manifest_sha256)?;
            Ok((row.generation_uuid, digest))
        })
        .collect::<Result<Vec<_>, GfError>>()?;
    Ok(CheckpointRetentionRoots {
        checkpoint_lock,
        roots,
    })
}

fn create_receipt(row: &CheckpointRecord) -> CheckpointReceipt {
    CheckpointReceipt {
        operation: "checkpoint",
        operation_uuid: row.create_operation_uuid,
        checkpoint_uuid: row.checkpoint_uuid,
        name: row.name.clone(),
        source_generation_uuid: row.generation_uuid,
        prior_current_generation_uuid: None,
        result_generation_uuid: None,
        registry_revision: row.created_revision,
        committed_at: row.created_at,
    }
}

fn create_tombstone_receipt(row: &CheckpointTombstone) -> CheckpointReceipt {
    CheckpointReceipt {
        operation: "checkpoint",
        operation_uuid: row.create_operation_uuid,
        checkpoint_uuid: row.checkpoint_uuid,
        name: row.name.clone(),
        source_generation_uuid: row.generation_uuid,
        prior_current_generation_uuid: None,
        result_generation_uuid: None,
        registry_revision: row.created_revision,
        committed_at: row.created_at,
    }
}

fn delete_receipt(row: &CheckpointTombstone) -> CheckpointReceipt {
    CheckpointReceipt {
        operation: "delete_checkpoint",
        operation_uuid: row.delete_operation_uuid,
        checkpoint_uuid: row.checkpoint_uuid,
        name: row.name.clone(),
        source_generation_uuid: row.generation_uuid,
        prior_current_generation_uuid: None,
        result_generation_uuid: None,
        registry_revision: row.deleted_revision,
        committed_at: row.deleted_at,
    }
}

fn canonical_project_root(path: &Path) -> Result<PathBuf, GfError> {
    let metadata = fs::symlink_metadata(path).map_err(storage_io)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(project_error(
            ProjectErrorCode::UnsupportedProjectFormat,
            "project root must be a real local directory, not a link",
        ));
    }
    std::fs::canonicalize(path).map_err(storage_io)
}

fn admit_existing_project(
    root: &Path,
    mode: crate::filesystem_admission::ProjectLifecycleMode,
) -> Result<crate::filesystem_admission::ProjectLifecycleAdmission, GfError> {
    let admission = crate::filesystem_admission::admit_project_lifecycle(
        root,
        mode,
        crate::filesystem_admission::ProjectRootRequirement::Existing,
    )?;
    admission.revalidate_identity()?;
    Ok(admission)
}

fn checkpoint_root(root: &Path) -> Result<PathBuf, GfError> {
    ensure_machine_directory(root, Path::new(CHECKPOINTS_DIR))
}

fn acquire_mutation_locks(root: &Path) -> Result<MutationLocks, GfError> {
    let lock_root = ensure_machine_directory(root, Path::new(LOCKS_DIR))?;
    sync_directory(root)?;
    let writer = open_regular_lock(&lock_root.join(WRITER_LOCK_FILE))?;
    if !crate::file_lock::try_lock_exclusive(&writer).map_err(storage_io)? {
        return Err(project_error(
            ProjectErrorCode::WriterBusy,
            "checkpoint mutation could not acquire writer.lock",
        ));
    }
    let checkpoint = open_regular_lock(&lock_root.join(CHECKPOINT_LOCK_FILE))?;
    if !crate::file_lock::try_lock_exclusive(&checkpoint).map_err(storage_io)? {
        return Err(project_error(
            ProjectErrorCode::WriterBusy,
            "checkpoint mutation could not acquire checkpoints.lock",
        ));
    }
    Ok(MutationLocks {
        writer: Some(writer),
        checkpoint: Some(checkpoint),
    })
}

fn acquire_checkpoint_read_lock(root: &Path) -> Result<CheckpointReadLock, GfError> {
    let lock_root = ensure_machine_directory(root, Path::new(LOCKS_DIR))?;
    let checkpoint = open_regular_lock(&lock_root.join(CHECKPOINT_LOCK_FILE))?;
    if !crate::file_lock::try_lock_shared(&checkpoint).map_err(storage_io)? {
        return Err(project_error(
            ProjectErrorCode::WriterBusy,
            "checkpoint read could not acquire checkpoints.lock",
        ));
    }
    Ok(CheckpointReadLock(checkpoint))
}

fn validate_name(value: &str) -> Result<String, GfError> {
    let normalized: String = value.nfc().collect();
    if normalized != value
        || value.is_empty()
        || value.len() > MAX_NAME_BYTES
        || value.trim() != value
        || value == "."
        || value == ".."
        || value.contains("  ")
        || !value
            .chars()
            .all(|ch| ch.is_alphanumeric() || matches!(ch, ' ' | '_' | '-' | '.'))
    {
        return Err(GfError::Validation(
            "checkpoint name is not canonical NFC content or violates the 1-128 byte grammar"
                .into(),
        ));
    }
    Ok(normalized)
}

fn validate_description(value: Option<&str>) -> Result<(), GfError> {
    if value.is_some_and(|value| {
        value.len() > MAX_DESCRIPTION_BYTES || value.chars().any(char::is_control)
    }) {
        return Err(GfError::Validation(
            "checkpoint description exceeds 1024 UTF-8 bytes or contains controls".into(),
        ));
    }
    Ok(())
}

fn create_request_digest(request: &CheckpointCreateRequest, name: &str) -> [u8; 32] {
    create_request_digest_values(
        request.operation_uuid,
        name,
        request.description.as_deref(),
        request.actor_uuid,
    )
}

fn create_request_digest_values(
    operation_uuid: Uuid,
    name: &str,
    description: Option<&str>,
    actor_uuid: Option<Uuid>,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge-checkpoint-create-request/1");
    hasher.update(operation_uuid.as_bytes());
    append_bytes(&mut hasher, name.as_bytes());
    match description {
        Some(value) => {
            hasher.update([1]);
            append_bytes(&mut hasher, value.as_bytes());
        }
        None => hasher.update([0]),
    }
    append_actor(&mut hasher, actor_uuid);
    hasher.finalize().into()
}

fn delete_request_digest(request: &CheckpointDeleteRequest, name: &str) -> [u8; 32] {
    delete_request_digest_values(request.operation_uuid, name, request.actor_uuid)
}

fn delete_request_digest_values(
    operation_uuid: Uuid,
    name: &str,
    actor_uuid: Option<Uuid>,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge-checkpoint-delete-request/1");
    hasher.update(operation_uuid.as_bytes());
    append_bytes(&mut hasher, name.as_bytes());
    append_actor(&mut hasher, actor_uuid);
    hasher.finalize().into()
}

fn validate_record_identity(
    checkpoint: Uuid,
    operation: Uuid,
    name: &str,
    description: Option<&str>,
    actor: Option<Uuid>,
    request_hex: &str,
) -> Result<(), GfError> {
    let request = create_request_digest_values(operation, name, description, actor);
    if request_hex != hex(&request) || checkpoint != checkpoint_uuid(operation, request) {
        return Err(registry_corrupt(
            "checkpoint deterministic identity or create request digest is inconsistent",
        ));
    }
    Ok(())
}

fn checkpoint_uuid(operation_uuid: Uuid, request_digest: [u8; 32]) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge-checkpoint-uuid/1");
    hasher.update(operation_uuid.as_bytes());
    hasher.update(request_digest);
    graphforge_core::canonical::uuid_v8(hasher.finalize().into())
}

fn append_bytes(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update(
        u32::try_from(bytes.len())
            .expect("validated checkpoint strings fit u32")
            .to_be_bytes(),
    );
    hasher.update(bytes);
}
fn append_actor(hasher: &mut Sha256, actor: Option<Uuid>) {
    match actor {
        Some(value) => {
            hasher.update([1]);
            hasher.update(value.as_bytes());
        }
        None => hasher.update([0]),
    }
}

fn parse_uuid(value: &str) -> Result<Uuid, GfError> {
    Uuid::parse_str(value).map_err(|_| registry_corrupt("revert journal UUID is invalid"))
}

fn validate_reason(value: &str) -> Result<String, GfError> {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed.len() > MAX_REASON_BYTES {
        return Err(GfError::Validation(
            "checkpoint revert reason must contain 1..=1024 UTF-8 bytes after trimming".into(),
        ));
    }
    Ok(trimmed.to_owned())
}

fn utc_micros() -> Result<i64, GfError> {
    let value = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| GfError::Storage("system clock is before Unix epoch".into()))?
        .as_micros();
    i64::try_from(value).map_err(|_| GfError::Storage("UTC microsecond timestamp overflow".into()))
}
fn project_error(code: ProjectErrorCode, message: impl Into<String>) -> GfError {
    GfError::Project {
        code,
        message: message.into(),
    }
}
fn storage_io(error: impl std::fmt::Display) -> GfError {
    GfError::Storage(format!("checkpoint registry I/O failed: {error}"))
}

#[cfg(test)]
mod tests;
