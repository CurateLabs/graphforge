//! Durable named references to verified immutable project generations.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{
    ArrayRef, FixedSizeBinaryBuilder, StringArray, TimestampMicrosecondArray, UInt32Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use fs4::fs_std::FileExt;
use graphforge_core::canonical::{CANONICAL_CONTRACT_VERSION, CanonicalDomain, fingerprint};
use graphforge_core::{GfError, ProjectErrorCode};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Registry {
    format: String,
    format_version: u32,
    revision: u64,
    active: Vec<CheckpointRecord>,
    tombstones: Vec<CheckpointTombstone>,
}

impl Registry {
    fn empty() -> Self {
        Self {
            format: "graphforge-checkpoints".into(),
            format_version: 1,
            revision: 0,
            active: Vec::new(),
            tombstones: Vec::new(),
        }
    }

    fn canonical_bytes(&self) -> Result<Vec<u8>, GfError> {
        validate_registry(self)?;
        let mut bytes = serde_json::to_vec(self).map_err(registry_serde)?;
        bytes.push(b'\n');
        if bytes.len() as u64 > MAX_REGISTRY_BYTES {
            return Err(project_error(
                ProjectErrorCode::ResourceLimit,
                "checkpoint registry exceeds 8 MiB",
            ));
        }
        Ok(bytes)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegistryIntent {
    transaction_uuid: Uuid,
    previous_revision: Option<u64>,
    previous_sha256: Option<String>,
    next_revision: u64,
    next_sha256: String,
    registry_temp: String,
    checksum_temp: String,
}

struct MutationLocks {
    writer: Option<File>,
    checkpoint: Option<File>,
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
            let _ = FileExt::unlock(checkpoint);
        }
        if let Some(writer) = &self.writer {
            let _ = FileExt::unlock(writer);
        }
    }
}

/// Create a checkpoint pinned to the post-lock validated `CURRENT` generation.
pub fn create_checkpoint(
    container_root: impl AsRef<Path>,
    request: &CheckpointCreateRequest,
) -> Result<CheckpointReceipt, GfError> {
    let name = validate_name(&request.name)?;
    validate_description(request.description.as_deref())?;
    let root = canonical_project_root(container_root.as_ref())?;
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
    let name = validate_name(&request.name)?;
    let root = canonical_project_root(container_root.as_ref())?;
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
    let root = canonical_project_root(container_root.as_ref())?;
    let checkpoint_root = checkpoint_root(&root)?;
    let (_checkpoint_lock, registry) = read_registry_for_read(&root, &checkpoint_root)?;
    Ok(registry.active)
}

/// Resolve and lifetime-pin the exact generation named by an active checkpoint.
pub fn open_checkpoint_generation(
    container_root: impl AsRef<Path>,
    name: &str,
) -> Result<(CheckpointRecord, crate::ResolvedProjectGeneration), GfError> {
    let name = validate_name(name)?;
    let root = canonical_project_root(container_root.as_ref())?;
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
#[expect(
    clippy::too_many_lines,
    reason = "the revert transaction is intentionally linear so lock ownership and publication order remain auditable"
)]
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
    let requested_name = validate_name(&request.name)?;
    let requested_reason = validate_reason(&request.reason)?;
    let root = canonical_project_root(container_root.as_ref())?;
    let transaction_uuid = revert_transaction_uuid(request.operation_uuid);
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
    let receipt = match stage_project_generation_with_lock(
        root.clone(),
        writer,
        prior_current,
        &publication,
        Some(expected_extension),
    )? {
        ProjectStageOutcome::AlreadyPublished(receipt) => receipt,
        ProjectStageOutcome::Staged(staged) => {
            staged
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
                )?
                .publish()?
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
    let checkpoint_unlock = FileExt::unlock(checkpoint);
    let writer_unlock = FileExt::unlock(writer);
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
    _checkpoint_lock: File,
    pub(crate) roots: Vec<(Uuid, [u8; 32])>,
}

pub(crate) fn checkpoint_retention_roots_after_writer_lock(
    root: &Path,
) -> Result<CheckpointRetentionRoots, GfError> {
    let lock_root = ensure_machine_directory(root, Path::new(LOCKS_DIR))?;
    let checkpoint_lock = open_regular_lock(&lock_root.join(CHECKPOINT_LOCK_FILE))?;
    if !FileExt::try_lock_exclusive(&checkpoint_lock).map_err(storage_io)? {
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
        _checkpoint_lock: checkpoint_lock,
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

fn checkpoint_root(root: &Path) -> Result<PathBuf, GfError> {
    ensure_machine_directory(root, Path::new(CHECKPOINTS_DIR))
}

fn acquire_mutation_locks(root: &Path) -> Result<MutationLocks, GfError> {
    let lock_root = ensure_machine_directory(root, Path::new(LOCKS_DIR))?;
    sync_directory(root)?;
    let writer = open_regular_lock(&lock_root.join(WRITER_LOCK_FILE))?;
    if !FileExt::try_lock_exclusive(&writer).map_err(storage_io)? {
        return Err(project_error(
            ProjectErrorCode::WriterBusy,
            "checkpoint mutation could not acquire writer.lock",
        ));
    }
    let checkpoint = open_regular_lock(&lock_root.join(CHECKPOINT_LOCK_FILE))?;
    if !FileExt::try_lock_exclusive(&checkpoint).map_err(storage_io)? {
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

fn acquire_checkpoint_read_lock(root: &Path) -> Result<File, GfError> {
    let lock_root = ensure_machine_directory(root, Path::new(LOCKS_DIR))?;
    let checkpoint = open_regular_lock(&lock_root.join(CHECKPOINT_LOCK_FILE))?;
    if !FileExt::try_lock_shared(&checkpoint).map_err(storage_io)? {
        return Err(project_error(
            ProjectErrorCode::WriterBusy,
            "checkpoint read could not acquire checkpoints.lock",
        ));
    }
    Ok(checkpoint)
}

fn read_registry_for_read(
    root: &Path,
    checkpoint_root: &Path,
) -> Result<(File, Registry), GfError> {
    let checkpoint = acquire_checkpoint_read_lock(root)?;
    if !checkpoint_root.join(INTENT_FILE).exists() {
        return read_registry(checkpoint_root).map(|registry| (checkpoint, registry));
    }
    drop(checkpoint);
    {
        let _locks = acquire_mutation_locks(root)?;
        recover_pair(checkpoint_root)?;
    }
    let checkpoint = acquire_checkpoint_read_lock(root)?;
    let registry = read_registry(checkpoint_root)?;
    Ok((checkpoint, registry))
}

fn read_registry(root: &Path) -> Result<Registry, GfError> {
    let registry_path = root.join(REGISTRY_FILE);
    let checksum_path = root.join(CHECKSUM_FILE);
    if !registry_path.exists() && !checksum_path.exists() {
        return Ok(Registry::empty());
    }
    let bytes = read_regular_bounded(&registry_path, MAX_REGISTRY_BYTES)?;
    let checksum = read_regular_bounded(&checksum_path, 128)?;
    let expected = format!("{}\n", hex(&Sha256::digest(&bytes).into()));
    if checksum != expected.as_bytes() {
        return Err(registry_corrupt(
            "registry checksum does not match exact bytes",
        ));
    }
    let registry: Registry = serde_json::from_slice(&bytes)
        .map_err(|_| registry_corrupt("registry JSON is malformed"))?;
    if registry.canonical_bytes()? != bytes {
        return Err(registry_corrupt("registry JSON is noncanonical"));
    }
    Ok(registry)
}

fn commit_registry(
    root: &Path,
    registry: &Registry,
    transaction_uuid: Uuid,
) -> Result<(), GfError> {
    let next = registry.canonical_bytes()?;
    let next_digest = hex(&Sha256::digest(&next).into());
    let previous = read_valid_pair(root)?;
    let registry_temp = format!(".registry.{transaction_uuid}.json.next");
    let checksum_temp = format!(".registry.{transaction_uuid}.sha256.next");
    prepare_temp_path(&root.join(&registry_temp))?;
    prepare_temp_path(&root.join(&checksum_temp))?;
    write_new_synced(&root.join(&registry_temp), &next)?;
    write_new_synced(
        &root.join(&checksum_temp),
        format!("{next_digest}\n").as_bytes(),
    )?;
    sync_directory(root)?;
    project_failpoint::hit(
        "checkpoint.registry.after_file_fsync",
        Some(transaction_uuid),
        None,
        "REGISTRY_STAGED",
        false,
    )?;
    let intent = RegistryIntent {
        transaction_uuid,
        previous_revision: previous.as_ref().map(|(registry, _)| registry.revision),
        previous_sha256: previous.as_ref().map(|(_, digest)| digest.clone()),
        next_revision: registry.revision,
        next_sha256: next_digest,
        registry_temp: registry_temp.clone(),
        checksum_temp: checksum_temp.clone(),
    };
    write_intent(root, &intent)?;
    project_failpoint::hit(
        "checkpoint.registry.before_replace",
        Some(transaction_uuid),
        None,
        "REGISTRY_INTENT_DURABLE",
        false,
    )?;
    fs::rename(root.join(&registry_temp), root.join(REGISTRY_FILE)).map_err(storage_io)?;
    project_failpoint::hit(
        "checkpoint.registry.after_replace",
        Some(transaction_uuid),
        None,
        "REGISTRY_REPLACED",
        true,
    )?;
    fs::rename(root.join(&checksum_temp), root.join(CHECKSUM_FILE)).map_err(storage_io)?;
    sync_directory(root)?;
    project_failpoint::hit(
        "checkpoint.registry.after_dir_fsync",
        Some(transaction_uuid),
        None,
        "REGISTRY_DURABLE",
        true,
    )?;
    fs::remove_file(root.join(INTENT_FILE)).map_err(storage_io)?;
    sync_directory(root)
}

fn recover_pair(root: &Path) -> Result<(), GfError> {
    let intent_path = root.join(INTENT_FILE);
    if !intent_path.exists() {
        read_registry(root)?;
        return Ok(());
    }
    let bytes = read_regular_bounded(&intent_path, 16 * 1024)?;
    let intent: RegistryIntent = serde_json::from_slice(&bytes)
        .map_err(|_| registry_corrupt("registry intent is malformed"))?;
    let mut canonical = serde_json::to_vec(&intent).map_err(registry_serde)?;
    canonical.push(b'\n');
    if canonical != bytes
        || !valid_private_name(&intent.registry_temp, intent.transaction_uuid, "json")
        || !valid_private_name(&intent.checksum_temp, intent.transaction_uuid, "sha256")
    {
        return Err(registry_corrupt(
            "registry intent is noncanonical or names unsafe files",
        ));
    }
    if let Ok(Some((current, digest))) = read_valid_pair(root) {
        if current.revision == intent.next_revision && digest == intent.next_sha256 {
            cleanup_intent(root, &intent)?;
            return Ok(());
        }
        if Some(current.revision) == intent.previous_revision
            && Some(digest) == intent.previous_sha256
        {
            validate_staged_pair(root, &intent)?;
            cleanup_intent(root, &intent)?;
            return Ok(());
        }
    }
    if intent.previous_revision.is_none()
        && !root.join(REGISTRY_FILE).exists()
        && !root.join(CHECKSUM_FILE).exists()
    {
        validate_staged_pair(root, &intent)?;
        cleanup_intent(root, &intent)?;
        return Ok(());
    }
    let registry_bytes = read_regular_bounded(&root.join(REGISTRY_FILE), MAX_REGISTRY_BYTES)?;
    if hex(&Sha256::digest(&registry_bytes).into()) == intent.next_sha256 {
        let checksum_bytes = read_regular_bounded(&root.join(&intent.checksum_temp), 128)?;
        if checksum_bytes == format!("{}\n", intent.next_sha256).as_bytes() {
            fs::rename(root.join(&intent.checksum_temp), root.join(CHECKSUM_FILE))
                .map_err(storage_io)?;
            sync_directory(root)?;
            cleanup_intent(root, &intent)?;
            read_registry(root)?;
            return Ok(());
        }
    }
    Err(registry_corrupt(
        "registry transaction is not a validated previous or staged next state",
    ))
}

fn validate_staged_pair(root: &Path, intent: &RegistryIntent) -> Result<(), GfError> {
    let registry_bytes =
        read_regular_bounded(&root.join(&intent.registry_temp), MAX_REGISTRY_BYTES)?;
    let checksum_bytes = read_regular_bounded(&root.join(&intent.checksum_temp), 128)?;
    let digest = hex(&Sha256::digest(&registry_bytes).into());
    if digest != intent.next_sha256
        || checksum_bytes != format!("{}\n", intent.next_sha256).as_bytes()
    {
        return Err(registry_corrupt(
            "registry staged pair does not match its durable intent",
        ));
    }
    let registry: Registry = serde_json::from_slice(&registry_bytes)
        .map_err(|_| registry_corrupt("staged checkpoint registry is malformed"))?;
    if registry.canonical_bytes()? != registry_bytes || registry.revision != intent.next_revision {
        return Err(registry_corrupt(
            "registry staged pair is noncanonical or has the wrong revision",
        ));
    }
    Ok(())
}

fn read_valid_pair(root: &Path) -> Result<Option<(Registry, String)>, GfError> {
    if !root.join(REGISTRY_FILE).exists() && !root.join(CHECKSUM_FILE).exists() {
        return Ok(None);
    }
    let registry = read_registry(root)?;
    let digest = hex(&Sha256::digest(registry.canonical_bytes()?).into());
    Ok(Some((registry, digest)))
}

fn cleanup_intent(root: &Path, intent: &RegistryIntent) -> Result<(), GfError> {
    for name in [&intent.registry_temp, &intent.checksum_temp] {
        let path = root.join(name);
        if path.exists() {
            validate_single_link_regular(&path, "registry transaction temporary file")?;
        }
        match fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(storage_io(error)),
        }
    }
    fs::remove_file(root.join(INTENT_FILE)).map_err(storage_io)?;
    sync_directory(root)
}

fn write_intent(root: &Path, intent: &RegistryIntent) -> Result<(), GfError> {
    let temp = root.join(format!(".registry.{}.txn.next", intent.transaction_uuid));
    let mut bytes = serde_json::to_vec(intent).map_err(registry_serde)?;
    bytes.push(b'\n');
    prepare_temp_path(&temp)?;
    write_new_synced(&temp, &bytes)?;
    project_failpoint::hit(
        "checkpoint.registry.after_intent_file_fsync",
        Some(intent.transaction_uuid),
        None,
        "REGISTRY_INTENT_STAGED",
        false,
    )?;
    fs::rename(temp, root.join(INTENT_FILE)).map_err(storage_io)?;
    sync_directory(root)
}

fn write_new_synced(path: &Path, bytes: &[u8]) -> Result<(), GfError> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path).map_err(storage_io)?;
    file.write_all(bytes).map_err(storage_io)?;
    file.sync_all().map_err(storage_io)
}

fn prepare_temp_path(path: &Path) -> Result<(), GfError> {
    if !path.exists() {
        return Ok(());
    }
    let metadata = fs::symlink_metadata(path).map_err(storage_io)?;
    if !metadata.file_type().is_file() {
        return Err(registry_corrupt(
            "checkpoint registry temporary path is linked or special",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(registry_corrupt(
                "checkpoint registry temporary path is hard-linked",
            ));
        }
    }
    fs::remove_file(path).map_err(storage_io)
}

fn read_regular_bounded(path: &Path, max: u64) -> Result<Vec<u8>, GfError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| registry_corrupt("checkpoint registry file is missing"))?;
    if !metadata.file_type().is_file() || metadata.len() > max {
        return Err(registry_corrupt(
            "checkpoint registry file is linked, special, or oversized",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(registry_corrupt("checkpoint registry file is hard-linked"));
        }
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options.open(path).map_err(|_| {
        registry_corrupt("checkpoint registry file could not be opened without following links")
    })?;
    let opened = file.metadata().map_err(storage_io)?;
    if !opened.is_file() || opened.len() != metadata.len() {
        return Err(registry_corrupt(
            "checkpoint registry file identity changed while opening",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if opened.dev() != metadata.dev() || opened.ino() != metadata.ino() || opened.nlink() != 1 {
            return Err(registry_corrupt(
                "checkpoint registry file identity changed while opening",
            ));
        }
    }
    let capacity = usize::try_from(metadata.len())
        .map_err(|_| registry_corrupt("checkpoint registry file length exceeds address space"))?;
    let mut bytes = Vec::with_capacity(capacity);
    file.take(max + 1)
        .read_to_end(&mut bytes)
        .map_err(storage_io)?;
    if bytes.len() as u64 > max {
        return Err(registry_corrupt(
            "checkpoint registry file exceeds its read bound",
        ));
    }
    Ok(bytes)
}

fn validate_registry(registry: &Registry) -> Result<(), GfError> {
    if registry.format != "graphforge-checkpoints"
        || registry.format_version != 1
        || registry.active.len() > MAX_ACTIVE
        || registry.tombstones.len() > MAX_TOMBSTONES
    {
        return Err(registry_corrupt(
            "checkpoint registry header or bounds are invalid",
        ));
    }
    if !registry.active.windows(2).all(|pair| {
        (&pair[0].name, pair[0].checkpoint_uuid) < (&pair[1].name, pair[1].checkpoint_uuid)
    }) {
        return Err(registry_corrupt(
            "active checkpoints are not strictly sorted",
        ));
    }
    if !registry.tombstones.windows(2).all(|pair| {
        (pair[0].deleted_revision, pair[0].checkpoint_uuid)
            < (pair[1].deleted_revision, pair[1].checkpoint_uuid)
    }) {
        return Err(registry_corrupt(
            "checkpoint tombstones are not strictly sorted",
        ));
    }
    let mut names = BTreeSet::new();
    let mut checkpoint_uuids = BTreeSet::new();
    let mut create_operations = BTreeSet::new();
    let mut delete_operations = BTreeSet::new();
    for row in &registry.active {
        validate_checkpoint_content(&CheckpointContentRef {
            label: "active checkpoint",
            checkpoint_uuid: row.checkpoint_uuid,
            create_operation_uuid: row.create_operation_uuid,
            name: &row.name,
            description: row.description.as_deref(),
            created_by: row.created_by,
            generation_manifest_sha256: &row.generation_manifest_sha256,
            create_request_sha256: &row.create_request_sha256,
        })?;
        if row.created_revision == 0
            || row.created_revision > registry.revision
            || !names.insert(row.name.as_str())
            || !checkpoint_uuids.insert(row.checkpoint_uuid)
            || !create_operations.insert(row.create_operation_uuid)
        {
            return Err(registry_corrupt(
                "active checkpoint identities or revision are inconsistent",
            ));
        }
    }
    for row in &registry.tombstones {
        // Preserve pre-consolidation order: content fields, then delete digest,
        // then deterministic create-request identity (error precedence).
        validate_name(&row.name)
            .map_err(|_| registry_corrupt("checkpoint tombstone name is invalid"))?;
        validate_description(row.description.as_deref())
            .map_err(|_| registry_corrupt("checkpoint tombstone description is invalid"))?;
        validate_digest(&row.generation_manifest_sha256)?;
        validate_digest(&row.create_request_sha256)?;
        validate_digest(&row.delete_request_sha256)?;
        validate_record_identity(
            row.checkpoint_uuid,
            row.create_operation_uuid,
            &row.name,
            row.description.as_deref(),
            row.created_by,
            &row.create_request_sha256,
        )?;
        let expected_delete =
            delete_request_digest_values(row.delete_operation_uuid, &row.name, row.deleted_by);
        if row.created_revision == 0
            || row.created_revision >= row.deleted_revision
            || row.deleted_revision > registry.revision
            || !checkpoint_uuids.insert(row.checkpoint_uuid)
            || !create_operations.insert(row.create_operation_uuid)
            || delete_operations.contains(&row.create_operation_uuid)
            || !delete_operations.insert(row.delete_operation_uuid)
            || create_operations.contains(&row.delete_operation_uuid)
            || row.delete_request_sha256 != hex(&expected_delete)
        {
            return Err(registry_corrupt(
                "checkpoint tombstone identities or revisions are inconsistent",
            ));
        }
    }
    Ok(())
}

struct CheckpointContentRef<'a> {
    label: &'static str,
    checkpoint_uuid: Uuid,
    create_operation_uuid: Uuid,
    name: &'a str,
    description: Option<&'a str>,
    created_by: Option<Uuid>,
    generation_manifest_sha256: &'a str,
    create_request_sha256: &'a str,
}

fn validate_checkpoint_content(content: &CheckpointContentRef<'_>) -> Result<(), GfError> {
    validate_name(content.name)
        .map_err(|_| registry_corrupt(format!("{} name is invalid", content.label)))?;
    validate_description(content.description)
        .map_err(|_| registry_corrupt(format!("{} description is invalid", content.label)))?;
    validate_digest(content.generation_manifest_sha256)?;
    validate_digest(content.create_request_sha256)?;
    validate_record_identity(
        content.checkpoint_uuid,
        content.create_operation_uuid,
        content.name,
        content.description,
        content.created_by,
        content.create_request_sha256,
    )
}

fn validate_single_link_regular(path: &Path, label: &str) -> Result<(), GfError> {
    let metadata = fs::symlink_metadata(path).map_err(storage_io)?;
    if !metadata.file_type().is_file() {
        return Err(registry_corrupt(format!("{label} is linked or special")));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(registry_corrupt(format!("{label} is hard-linked")));
        }
    }
    Ok(())
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

fn validate_digest(value: &str) -> Result<(), GfError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(registry_corrupt("checkpoint digest is noncanonical"));
    }
    Ok(())
}

fn decode_digest(value: &str) -> Result<[u8; 32], GfError> {
    validate_digest(value)?;
    let mut digest = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let text = std::str::from_utf8(pair)
            .map_err(|_| registry_corrupt("checkpoint digest is not UTF-8"))?;
        digest[index] = u8::from_str_radix(text, 16)
            .map_err(|_| registry_corrupt("checkpoint digest is not lowercase hex"))?;
    }
    Ok(digest)
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
fn valid_private_name(name: &str, uuid: Uuid, kind: &str) -> bool {
    name == format!(".registry.{uuid}.{kind}.next")
}
fn hex(bytes: &[u8; 32]) -> String {
    let mut output = String::with_capacity(64);
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("writing hexadecimal to String cannot fail");
    }
    output
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

fn revert_request_digest(
    operation_uuid: Uuid,
    name: &str,
    checkpoint_uuid: Uuid,
    source_generation_uuid: Uuid,
    source_manifest_sha256: [u8; 32],
    reason: &str,
    actor_uuid: Option<Uuid>,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge-checkpoint-revert-request/1");
    hasher.update(operation_uuid.as_bytes());
    append_bytes(&mut hasher, name.as_bytes());
    hasher.update(checkpoint_uuid.as_bytes());
    hasher.update(source_generation_uuid.as_bytes());
    hasher.update(source_manifest_sha256);
    append_bytes(&mut hasher, reason.as_bytes());
    append_actor(&mut hasher, actor_uuid);
    hasher.finalize().into()
}

fn revert_transaction_uuid(operation_uuid: Uuid) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge-checkpoint-revert-transaction/1");
    hasher.update(operation_uuid.as_bytes());
    graphforge_core::canonical::uuid_v8(hasher.finalize().into())
}

fn restoration_uuid(operation_uuid: Uuid, request_digest: [u8; 32]) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge-restoration-transition-uuid/1");
    hasher.update(operation_uuid.as_bytes());
    hasher.update(request_digest);
    graphforge_core::canonical::uuid_v8(hasher.finalize().into())
}

fn restored_generation_uuid(
    transaction_uuid: Uuid,
    checkpoint_uuid: Uuid,
    source_generation_uuid: Uuid,
    source_manifest_sha256: [u8; 32],
    prior_current_generation_uuid: Uuid,
    restored_at: i64,
    request_digest: [u8; 32],
) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge-checkpoint-restored-generation/1");
    hasher.update(transaction_uuid.as_bytes());
    hasher.update(checkpoint_uuid.as_bytes());
    hasher.update(source_generation_uuid.as_bytes());
    hasher.update(source_manifest_sha256);
    hasher.update(prior_current_generation_uuid.as_bytes());
    hasher.update(restored_at.to_be_bytes());
    hasher.update(request_digest);
    graphforge_core::canonical::uuid_v8(hasher.finalize().into())
}

fn snapshot_to_participant(
    snapshot: crate::ProjectParticipantSnapshot,
) -> Result<ProjectParticipant, GfError> {
    let encoding = match snapshot.encoding.as_str() {
        "parquet" => ProjectParticipantEncoding::Parquet,
        "arrow" => ProjectParticipantEncoding::Arrow,
        "json" => ProjectParticipantEncoding::Json,
        _ => {
            return Err(registry_corrupt(
                "checkpoint participant encoding is unsupported",
            ));
        }
    };
    Ok(ProjectParticipant {
        capability_id: snapshot.capability_id,
        capability_version: snapshot.capability_version,
        record_family_id: snapshot.record_family_id,
        record_version: snapshot.record_version,
        encoding,
        schema_fingerprint: snapshot.schema_fingerprint,
        row_count: snapshot.row_count,
        bytes: snapshot.bytes,
    })
}

#[allow(clippy::too_many_arguments)]
fn restoration_participant(
    restoration_uuid: Uuid,
    checkpoint_uuid: Uuid,
    source_generation_uuid: Uuid,
    source_manifest_sha256: [u8; 32],
    prior_current_generation_uuid: Uuid,
    restored_generation_uuid: Uuid,
    operation_uuid: Uuid,
    actor_uuid: Option<Uuid>,
    reason: &str,
    restored_at: i64,
) -> Result<ProjectParticipant, GfError> {
    let schema = Arc::new(Schema::new(vec![
        uuid_field("restoration_uuid", false),
        uuid_field("checkpoint_uuid", false),
        uuid_field("source_generation_uuid", false),
        Field::new(
            "source_manifest_sha256",
            DataType::FixedSizeBinary(32),
            false,
        ),
        uuid_field("prior_current_generation_uuid", false),
        uuid_field("restored_generation_uuid", false),
        uuid_field("operation_uuid", false),
        uuid_field("actor_uuid", true),
        Field::new("reason", DataType::Utf8, false),
        Field::new(
            "restored_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("contract_version", DataType::UInt32, false),
    ]));
    let mut columns = Vec::<ArrayRef>::new();
    for value in [
        Some(restoration_uuid),
        Some(checkpoint_uuid),
        Some(source_generation_uuid),
        Some(prior_current_generation_uuid),
        Some(restored_generation_uuid),
        Some(operation_uuid),
        actor_uuid,
    ] {
        let mut builder = FixedSizeBinaryBuilder::with_capacity(1, 16);
        match value {
            Some(uuid) => builder.append_value(uuid.as_bytes()).map_err(arrow_error)?,
            None => builder.append_null(),
        }
        columns.push(Arc::new(builder.finish()));
    }
    let mut source_digest = FixedSizeBinaryBuilder::with_capacity(1, 32);
    source_digest
        .append_value(source_manifest_sha256)
        .map_err(arrow_error)?;
    columns.insert(3, Arc::new(source_digest.finish()));
    columns.push(Arc::new(StringArray::from(vec![reason])));
    columns.push(Arc::new(
        TimestampMicrosecondArray::from(vec![restored_at]).with_timezone("UTC"),
    ));
    columns.push(Arc::new(UInt32Array::from(vec![
        RESTORATION_CONTRACT_VERSION,
    ])));
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).map_err(arrow_error)?;
    let properties = WriterProperties::builder()
        .set_created_by("graphforge-restoration-transition/1".into())
        .build();
    let mut writer =
        ArrowWriter::try_new(Vec::new(), schema, Some(properties)).map_err(parquet_error)?;
    writer.write(&batch).map_err(parquet_error)?;
    let bytes = writer.into_inner().map_err(parquet_error)?;
    let schema_fingerprint = fingerprint(
        CanonicalDomain::Schema,
        CANONICAL_CONTRACT_VERSION,
        b"restoration_transition/1|restoration_uuid:fixed[16]:required|checkpoint_uuid:fixed[16]:required|source_generation_uuid:fixed[16]:required|source_manifest_sha256:fixed[32]:required|prior_current_generation_uuid:fixed[16]:required|restored_generation_uuid:fixed[16]:required|operation_uuid:fixed[16]:required|actor_uuid:fixed[16]:optional|reason:utf8:required|restored_at:timestamp_us_utc:required|contract_version:u32:required",
    )
    .map_err(|error| GfError::Validation(error.to_string()))?;
    Ok(ProjectParticipant {
        capability_id: crate::WORKSPACE_CAPABILITY_ID.into(),
        capability_version: crate::WORKSPACE_CAPABILITY_VERSION,
        record_family_id: RESTORATION_FAMILY.into(),
        record_version: RESTORATION_CONTRACT_VERSION,
        encoding: ProjectParticipantEncoding::Parquet,
        schema_fingerprint,
        row_count: 1,
        bytes,
    })
}

fn uuid_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::FixedSizeBinary(16), nullable)
}

fn arrow_error(error: arrow::error::ArrowError) -> GfError {
    let message = format!("restoration Arrow encoding failed: {error}");
    drop(error);
    GfError::Storage(message)
}

fn parquet_error(error: parquet::errors::ParquetError) -> GfError {
    let message = format!("restoration Parquet encoding failed: {error}");
    drop(error);
    GfError::Storage(message)
}
fn utc_micros() -> Result<i64, GfError> {
    let value = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| GfError::Storage("system clock is before Unix epoch".into()))?
        .as_micros();
    i64::try_from(value).map_err(|_| GfError::Storage("UTC microsecond timestamp overflow".into()))
}
fn registry_serde(error: impl std::fmt::Display) -> GfError {
    GfError::Storage(format!("checkpoint registry encoding failed: {error}"))
}
fn registry_corrupt(message: impl Into<String>) -> GfError {
    project_error(ProjectErrorCode::CheckpointRegistryCorrupt, message)
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
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::io::{BufRead, BufReader};
    use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
    use std::process::{Command, Stdio};
    use std::sync::mpsc;
    use std::time::Duration;
    use tempfile::tempdir;
    use wait_timeout::ChildExt;

    const TEST_DEADLINE: Duration = Duration::from_secs(1);
    const CHILD_DEADLINE: Duration = Duration::from_secs(10);

    struct WriterLockHolder {
        release: Option<mpsc::SyncSender<()>>,
        worker: Option<std::thread::JoinHandle<()>>,
    }

    impl WriterLockHolder {
        fn finish(mut self) -> Result<(), String> {
            let release = self
                .release
                .take()
                .ok_or_else(|| "phase=main release sender missing".to_owned())?;
            let release_result = release
                .send(())
                .map_err(|error| format!("phase=main release holder error={error}"));
            let join_result = self
                .worker
                .take()
                .ok_or_else(|| "phase=main holder worker missing".to_owned())?
                .join()
                .map_err(|_| "phase=main holder worker panicked".to_owned());
            release_result.and(join_result)
        }
    }

    impl Drop for WriterLockHolder {
        fn drop(&mut self) {
            if let Some(release) = self.release.take() {
                let _ = release.send(());
            }
            if let Some(worker) = self.worker.take() {
                let _ = worker.join();
            }
        }
    }

    fn while_writer_lock_is_held<T>(root: &Path, action: impl FnOnce() -> T) -> T {
        let writer_path = root.join(LOCKS_DIR).join(WRITER_LOCK_FILE);
        let worker_path = writer_path.clone();
        let (ready_sender, ready_receiver) = mpsc::sync_channel(0);
        let (release_sender, release_receiver) = mpsc::sync_channel(0);
        let worker = std::thread::Builder::new()
            .name("checkpoint-writer-lock-holder".into())
            .spawn(move || {
                let writer =
                    open_regular_lock(&worker_path).expect("phase=holder open writer.lock");
                assert!(
                    FileExt::try_lock_exclusive(&writer).expect("phase=holder acquire writer.lock"),
                    "phase=holder writer.lock unexpectedly busy"
                );
                ready_sender.send(()).expect("phase=holder publish ready");
                release_receiver.recv().expect("phase=holder await release");
                FileExt::unlock(&writer).expect("phase=holder release writer.lock");
            })
            .expect("phase=holder spawn");
        let holder = WriterLockHolder {
            release: Some(release_sender),
            worker: Some(worker),
        };
        if let Err(error) = ready_receiver.recv_timeout(TEST_DEADLINE) {
            drop(ready_receiver);
            let cleanup = holder.finish();
            panic!("phase=main await held writer.lock error={error}; cleanup={cleanup:?}");
        }
        let result = catch_unwind(AssertUnwindSafe(action));
        let cleanup = holder.finish();
        match result {
            Ok(value) => {
                cleanup.unwrap_or_else(|error| panic!("phase=main holder cleanup error={error}"));
                value
            }
            Err(original) => {
                let _ = cleanup;
                resume_unwind(original);
            }
        }
    }

    struct BoundedChild {
        child: std::process::Child,
        reaped: bool,
    }

    impl BoundedChild {
        fn wait(mut self, phase: &str) -> std::process::ExitStatus {
            let mut failures = Vec::new();
            match self.child.wait_timeout(CHILD_DEADLINE) {
                Ok(Some(status)) => {
                    self.reaped = true;
                    return status;
                }
                Ok(None) => failures.push(format!("wait timeout={CHILD_DEADLINE:?}")),
                Err(error) => failures.push(format!("wait error={error}")),
            }
            if let Err(error) = self.child.kill() {
                failures.push(format!("kill error={error}"));
            }
            match self.child.wait_timeout(TEST_DEADLINE) {
                Ok(Some(status)) => {
                    self.reaped = true;
                    failures.push(format!("killed_status={status}"));
                }
                Ok(None) => failures.push(format!("reap timeout={TEST_DEADLINE:?}")),
                Err(error) => failures.push(format!("reap error={error}")),
            }
            panic!("phase={phase} child cleanup failures={failures:?}");
        }
    }

    impl Drop for BoundedChild {
        fn drop(&mut self) {
            if !self.reaped {
                let mut failures = Vec::new();
                if let Err(error) = self.child.kill() {
                    failures.push(format!("kill error={error}"));
                }
                match self.child.wait_timeout(TEST_DEADLINE) {
                    Ok(Some(_)) => self.reaped = true,
                    Ok(None) => failures.push(format!("reap timeout={TEST_DEADLINE:?}")),
                    Err(error) => failures.push(format!("reap error={error}")),
                }
                if !failures.is_empty() {
                    eprintln!("phase=drop child cleanup failures={failures:?}");
                }
            }
        }
    }

    fn recover_checkpoint_pair_after_lock_handoff(root: &Path, phase: &str) {
        checkpoint_lock_handoff(root, phase, true);
    }

    fn preserve_checkpoint_intent_after_lock_handoff(root: &Path, phase: &str) {
        checkpoint_lock_handoff(root, phase, false);
    }

    fn checkpoint_lock_handoff(root: &Path, phase: &str, recover_durable_intent: bool) {
        let lock_root = root.join(LOCKS_DIR);
        let writer_path = lock_root.join(WRITER_LOCK_FILE);
        let checkpoint_path = lock_root.join(CHECKPOINT_LOCK_FILE);
        let checkpoint_root = root.join(CHECKPOINTS_DIR);
        let worker_writer_path = writer_path.clone();
        let worker_checkpoint_path = checkpoint_path.clone();
        let (sender, receiver) = mpsc::sync_channel(0);
        std::thread::Builder::new()
            .name("checkpoint-lock-handoff-recovery".into())
            .spawn(move || {
                let result = (|| {
                    let writer = open_regular_lock(&worker_writer_path)
                        .map_err(|error| format!("open writer.lock failed: {error}"))?;
                    FileExt::lock_exclusive(&writer)
                        .map_err(|error| format!("acquire writer.lock failed: {error}"))?;

                    let checkpoint = match open_regular_lock(&worker_checkpoint_path) {
                        Ok(checkpoint) => checkpoint,
                        Err(error) => {
                            let writer_unlock = FileExt::unlock(&writer);
                            return Err(format!(
                                "open checkpoints.lock failed: {error}; \
                                 writer_unlock={writer_unlock:?}"
                            ));
                        }
                    };
                    if let Err(error) = FileExt::lock_exclusive(&checkpoint) {
                        let writer_unlock = FileExt::unlock(&writer);
                        return Err(format!(
                            "acquire checkpoints.lock failed: {error}; writer_unlock={writer_unlock:?}"
                        ));
                    }

                    let recovery = if recover_durable_intent
                        && checkpoint_root.join(INTENT_FILE).exists()
                    {
                        recover_pair(&checkpoint_root)
                            .map_err(|error| format!("recover durable checkpoint intent failed: {error}"))
                    } else {
                        Ok(())
                    };
                    let checkpoint_unlock = FileExt::unlock(&checkpoint)
                        .map_err(|error| format!("unlock checkpoints.lock failed: {error}"));
                    let writer_unlock = FileExt::unlock(&writer)
                        .map_err(|error| format!("unlock writer.lock failed: {error}"));

                    recovery?;
                    checkpoint_unlock?;
                    writer_unlock
                })();
                let _ = sender.send(result);
            })
            .unwrap();
        match receiver.recv_timeout(Duration::from_secs(1)) {
            Ok(Ok(())) => {}
            Ok(Err(error)) => panic!(
                "checkpoint lock handoff/recovery failed at {phase}; writer_path={}; \
                 checkpoint_path={}: {error}",
                writer_path.display(),
                checkpoint_path.display()
            ),
            Err(error) => panic!(
                "checkpoint lock handoff/recovery timed out at {phase}; writer_path={}; \
                 checkpoint_path={}; timeout=1s; channel={error}",
                writer_path.display(),
                checkpoint_path.display()
            ),
        }
    }

    fn publish_clone(root: &Path) -> Uuid {
        let selected = crate::resolve_project_generation(root).unwrap();
        let capabilities = selected
            .capabilities()
            .into_iter()
            .map(|entry| crate::ProjectCapability {
                capability_id: entry.capability_id,
                capability_version: entry.capability_version,
            })
            .collect();
        let participants = selected
            .participant_snapshots()
            .unwrap()
            .into_iter()
            .map(|entry| crate::ProjectParticipant {
                capability_id: entry.capability_id,
                capability_version: entry.capability_version,
                record_family_id: entry.record_family_id,
                record_version: entry.record_version,
                encoding: match entry.encoding.as_str() {
                    "arrow" => crate::ProjectParticipantEncoding::Arrow,
                    "json" => crate::ProjectParticipantEncoding::Json,
                    "parquet" => crate::ProjectParticipantEncoding::Parquet,
                    other => panic!("unexpected participant encoding {other}"),
                },
                schema_fingerprint: entry.schema_fingerprint,
                row_count: entry.row_count,
                bytes: entry.bytes,
            })
            .collect();
        let generation_uuid = Uuid::now_v7();
        let request = crate::ProjectGenerationRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid,
            capabilities,
            participants,
        };
        let crate::ProjectStageOutcome::Staged(staged) =
            crate::stage_project_generation(root, &request).unwrap()
        else {
            panic!("fresh publication unexpectedly replayed");
        };
        staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .unwrap()
            .publish()
            .unwrap();
        generation_uuid
    }

    fn create_request(operation_uuid: Uuid, name: &str) -> CheckpointCreateRequest {
        CheckpointCreateRequest {
            operation_uuid,
            name: name.into(),
            description: Some("release candidate".into()),
            actor_uuid: Some(Uuid::parse_str("018f0f4e-7b8c-7000-8000-0000000000aa").unwrap()),
        }
    }

    fn write_raw_registry(root: &Path, registry: &Registry) {
        let checkpoint_root = root.join(CHECKPOINTS_DIR);
        let mut bytes = serde_json::to_vec(registry).unwrap();
        bytes.push(b'\n');
        fs::write(checkpoint_root.join(REGISTRY_FILE), &bytes).unwrap();
        fs::write(
            checkpoint_root.join(CHECKSUM_FILE),
            format!("{}\n", hex(&Sha256::digest(&bytes).into())),
        )
        .unwrap();
    }

    fn install_registry_intent(
        checkpoint_root: &Path,
        previous: Option<&Registry>,
        next: &Registry,
    ) -> RegistryIntent {
        fs::create_dir_all(checkpoint_root).unwrap();
        let transaction_uuid = Uuid::now_v7();
        let next_bytes = next.canonical_bytes().unwrap();
        let next_sha256 = hex(&Sha256::digest(&next_bytes).into());
        let registry_temp = format!(".registry.{transaction_uuid}.json.next");
        let checksum_temp = format!(".registry.{transaction_uuid}.sha256.next");
        fs::write(checkpoint_root.join(&registry_temp), &next_bytes).unwrap();
        fs::write(
            checkpoint_root.join(&checksum_temp),
            format!("{next_sha256}\n"),
        )
        .unwrap();
        let intent = RegistryIntent {
            transaction_uuid,
            previous_revision: previous.map(|registry| registry.revision),
            previous_sha256: previous
                .map(|registry| hex(&Sha256::digest(registry.canonical_bytes().unwrap()).into())),
            next_revision: next.revision,
            next_sha256,
            registry_temp,
            checksum_temp,
        };
        let mut intent_bytes = serde_json::to_vec(&intent).unwrap();
        intent_bytes.push(b'\n');
        fs::write(checkpoint_root.join(INTENT_FILE), intent_bytes).unwrap();
        intent
    }

    #[test]
    fn revert_identity_matches_frozen_golden_vector() {
        let operation = Uuid::parse_str("018f0f4e-7b8c-7000-8000-000000000003").unwrap();
        let checkpoint = Uuid::parse_str("4084179c-38db-8b6b-9b6e-c0b0a855e002").unwrap();
        let source = Uuid::parse_str("018f0f4e-7b8c-7000-8000-0000000000b0").unwrap();
        let prior = Uuid::parse_str("018f0f4e-7b8c-7000-8000-0000000000d0").unwrap();
        let actor = Uuid::parse_str("018f0f4e-7b8c-7000-8000-0000000000aa").unwrap();
        let source_digest = [0x11; 32];
        let request_digest = revert_request_digest(
            operation,
            "Release 1.0",
            checkpoint,
            source,
            source_digest,
            "restore release candidate",
            Some(actor),
        );
        assert_eq!(
            hex(&request_digest),
            "dff3755629942d1189060b117cb70dc864428fcc3b28d5c2d22d3924c3690e93"
        );
        let transaction = revert_transaction_uuid(operation);
        assert_eq!(
            transaction.to_string(),
            "908d637b-d6e6-8508-919e-d4e708e037b2"
        );
        assert_eq!(
            restoration_uuid(operation, request_digest).to_string(),
            "9e1f160c-badb-80c9-beaa-ae580910bf8a"
        );
        assert_eq!(
            restored_generation_uuid(
                transaction,
                checkpoint,
                source,
                source_digest,
                prior,
                1_720_000_000_123_456,
                request_digest,
            )
            .to_string(),
            "5dc02888-2064-8892-a0e1-2c00968ba0cc"
        );
    }

    #[test]
    fn revert_publishes_child_preserves_registry_and_replays_after_delete() {
        let directory = tempdir().unwrap();
        crate::open_or_initialize_project(directory.path()).unwrap();
        let source = crate::resolve_project_generation(directory.path()).unwrap();
        let created = create_checkpoint(
            directory.path(),
            &create_request(Uuid::from_u128(40), "Before"),
        )
        .unwrap();
        let prior_current = publish_clone(directory.path());
        let request = CheckpointRevertRequest {
            operation_uuid: Uuid::from_u128(41),
            name: "Before".into(),
            reason: " restore known state ".into(),
            actor_uuid: None,
        };
        let (receipt, restored) = revert_checkpoint(
            directory.path(),
            &request,
            || Ok(1_720_000_000_123_456),
            |_| Ok(()),
        )
        .unwrap();
        assert_eq!(restored.parent_generation_uuid(), Some(prior_current));
        assert_eq!(receipt.source_generation_uuid, source.generation_uuid());
        assert_eq!(receipt.prior_current_generation_uuid, Some(prior_current));
        assert_eq!(receipt.registry_revision, created.registry_revision);
        assert_eq!(list_checkpoints(directory.path()).unwrap().len(), 1);
        let restoration_count = restored
            .participant_descriptors()
            .unwrap()
            .iter()
            .filter(|row| row.record_family_id == RESTORATION_FAMILY)
            .count();
        assert_eq!(restoration_count, 1);

        delete_checkpoint(
            directory.path(),
            &CheckpointDeleteRequest {
                operation_uuid: Uuid::from_u128(42),
                name: "Before".into(),
                actor_uuid: None,
            },
        )
        .unwrap();
        let (replay, replayed_generation) = revert_checkpoint(
            directory.path(),
            &request,
            || panic!("published replay sampled clock"),
            |_| Ok(()),
        )
        .unwrap();
        assert_eq!(replay, receipt);
        assert_eq!(replay.prior_current_generation_uuid, Some(prior_current));
        assert_eq!(
            replayed_generation.generation_uuid(),
            restored.generation_uuid()
        );
        checkpoint_lock_handoff(
            directory.path(),
            "action=revert published-replay return",
            false,
        );

        let mut conflict = request;
        conflict.reason = "different".into();
        let conflict_error =
            revert_checkpoint(directory.path(), &conflict, || Ok(0), |_| Ok(())).unwrap_err();
        assert_eq!(conflict_error.code(), "GF_IDEMPOTENCY_CONFLICT");
        checkpoint_lock_handoff(
            directory.path(),
            "action=revert published-replay conflict return",
            false,
        );
    }

    #[test]
    fn revert_replay_lock_handoff_fails_closed_with_stable_storage_errors() {
        let checkpoint_error = finish_revert_replay_lock_handoff(
            Err(std::io::Error::other("checkpoint unlock failed")),
            Ok(()),
        )
        .unwrap_err();
        assert_eq!(checkpoint_error.code(), "GF_IO");
        assert_eq!(
            checkpoint_error.to_string(),
            "storage error: checkpoint revert replay lock handoff failed at checkpoints.lock: checkpoint unlock failed"
        );

        let writer_error = finish_revert_replay_lock_handoff(
            Ok(()),
            Err(std::io::Error::other("writer unlock failed")),
        )
        .unwrap_err();
        assert_eq!(writer_error.code(), "GF_IO");
        assert_eq!(
            writer_error.to_string(),
            "storage error: checkpoint revert replay lock handoff failed at writer.lock: writer unlock failed"
        );
    }

    #[test]
    fn revert_validation_failure_preserves_prior_current() {
        let directory = tempdir().unwrap();
        crate::open_or_initialize_project(directory.path()).unwrap();
        create_checkpoint(
            directory.path(),
            &create_request(Uuid::from_u128(50), "Before"),
        )
        .unwrap();
        let prior = publish_clone(directory.path());
        let error = revert_checkpoint(
            directory.path(),
            &CheckpointRevertRequest {
                operation_uuid: Uuid::from_u128(51),
                name: "Before".into(),
                reason: "must fail closed".into(),
                actor_uuid: None,
            },
            || Ok(1_720_000_000_123_456),
            |_| Err(GfError::Validation("injected composite failure".into())),
        )
        .unwrap_err();
        assert_eq!(error.code(), "GF_VALIDATION");
        checkpoint_lock_handoff(
            directory.path(),
            "action=revert validation-error return",
            false,
        );
        assert_eq!(
            crate::resolve_project_generation(directory.path())
                .unwrap()
                .generation_uuid(),
            prior
        );
        assert_eq!(list_checkpoints(directory.path()).unwrap().len(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn mutation_lock_guard_unlocks_checkpoint_with_retained_duplicate_open() {
        let directory = tempdir().unwrap();
        crate::open_or_initialize_project(directory.path()).unwrap();
        let locks = acquire_mutation_locks(directory.path()).unwrap();
        let retained = locks.checkpoint.as_ref().unwrap().try_clone().unwrap();
        drop(locks);

        let checkpoint =
            open_regular_lock(&directory.path().join(LOCKS_DIR).join(CHECKPOINT_LOCK_FILE))
                .unwrap();
        assert!(FileExt::try_lock_exclusive(&checkpoint).unwrap());
        FileExt::unlock(&checkpoint).unwrap();
        drop(retained);
    }

    #[test]
    fn create_list_delete_and_replays_are_deterministic() {
        let directory = tempdir().unwrap();
        crate::open_or_initialize_project(directory.path()).unwrap();
        let operation = Uuid::parse_str("018f0f4e-7b8c-7000-8000-000000000001").unwrap();
        let request = create_request(operation, "Release 1.0");
        assert_eq!(
            hex(&create_request_digest(&request, "Release 1.0")),
            "01c7bf2f2c443d85d31ff80fef4a36484e31402213e8d145371867bdb2addbe8"
        );
        let created = create_checkpoint(directory.path(), &request).unwrap();
        assert_eq!(
            created.checkpoint_uuid,
            Uuid::parse_str("4084179c-38db-8b6b-9b6e-c0b0a855e002").unwrap()
        );
        let replayed = create_checkpoint(directory.path(), &request).unwrap();
        assert_eq!(created, replayed);
        assert_eq!(created.registry_revision, 1);

        let rows = list_checkpoints(directory.path()).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].checkpoint_uuid, created.checkpoint_uuid);
        assert_eq!(rows[0].generation_uuid, created.source_generation_uuid);

        let delete = CheckpointDeleteRequest {
            operation_uuid: Uuid::parse_str("018f0f4e-7b8c-7000-8000-000000000002").unwrap(),
            name: "Release 1.0".into(),
            actor_uuid: request.actor_uuid,
        };
        assert_eq!(
            hex(&delete_request_digest(&delete, "Release 1.0")),
            "9e6e15801f66ea4f7f58755c505fc1964e48ab87625459f388e2c23659135bb3"
        );
        let deleted = delete_checkpoint(directory.path(), &delete).unwrap();
        assert_eq!(
            deleted,
            delete_checkpoint(directory.path(), &delete).unwrap()
        );
        assert_eq!(deleted.registry_revision, 2);
        assert!(list_checkpoints(directory.path()).unwrap().is_empty());
        assert_eq!(
            created,
            create_checkpoint(directory.path(), &request).unwrap()
        );
        let changed_replay = create_request(operation, "Release 1.1");
        assert_eq!(
            create_checkpoint(directory.path(), &changed_replay)
                .unwrap_err()
                .code(),
            "GF_IDEMPOTENCY_CONFLICT"
        );
        assert!(list_checkpoints(directory.path()).unwrap().is_empty());
    }

    #[test]
    fn identity_is_stable_across_independent_projects() {
        let first = tempdir().unwrap();
        let second = tempdir().unwrap();
        crate::open_or_initialize_project(first.path()).unwrap();
        crate::open_or_initialize_project(second.path()).unwrap();
        let request = create_request(
            Uuid::parse_str("018f0f4e-7b8c-7000-8000-000000000010").unwrap(),
            "Stable",
        );
        let left = create_checkpoint(first.path(), &request).unwrap();
        let right = create_checkpoint(second.path(), &request).unwrap();
        assert_eq!(left.checkpoint_uuid, right.checkpoint_uuid);
    }

    #[test]
    fn opened_checkpoint_generation_remains_pinned_after_delete() {
        let directory = tempdir().unwrap();
        crate::open_or_initialize_project(directory.path()).unwrap();
        let created = create_checkpoint(
            directory.path(),
            &create_request(Uuid::now_v7(), "Pinned View"),
        )
        .unwrap();
        let (row, opened) = open_checkpoint_generation(directory.path(), "Pinned View").unwrap();
        assert_eq!(row.checkpoint_uuid, created.checkpoint_uuid);
        assert_eq!(opened.generation_uuid(), created.source_generation_uuid);
        delete_checkpoint(
            directory.path(),
            &CheckpointDeleteRequest {
                operation_uuid: Uuid::now_v7(),
                name: "Pinned View".into(),
                actor_uuid: None,
            },
        )
        .unwrap();
        assert_eq!(
            open_checkpoint_generation(directory.path(), "Pinned View")
                .unwrap_err()
                .code(),
            "GF_CHECKPOINT_NOT_FOUND"
        );
        assert_eq!(opened.generation_uuid(), created.source_generation_uuid);
        assert!(opened.participant_snapshots().is_ok());
    }

    #[test]
    fn conflicts_names_and_corruption_fail_closed() {
        let directory = tempdir().unwrap();
        crate::open_or_initialize_project(directory.path()).unwrap();
        let operation = Uuid::parse_str("018f0f4e-7b8c-7000-8000-000000000020").unwrap();
        create_checkpoint(directory.path(), &create_request(operation, "Safe.Name")).unwrap();

        let conflict =
            create_checkpoint(directory.path(), &create_request(operation, "Other")).unwrap_err();
        assert_eq!(conflict.code(), "GF_IDEMPOTENCY_CONFLICT");
        let exists = create_checkpoint(
            directory.path(),
            &create_request(
                Uuid::parse_str("018f0f4e-7b8c-7000-8000-000000000021").unwrap(),
                "Safe.Name",
            ),
        )
        .unwrap_err();
        assert_eq!(exists.code(), "GF_CHECKPOINT_EXISTS");
        for invalid in ["", "../escape", "two  spaces", " e", "e ", ".", ".."] {
            let error =
                create_checkpoint(directory.path(), &create_request(Uuid::now_v7(), invalid))
                    .unwrap_err();
            assert_eq!(error.code(), "GF_VALIDATION", "name={invalid:?}");
        }

        fs::write(
            directory.path().join(CHECKPOINTS_DIR).join(CHECKSUM_FILE),
            b"0000000000000000000000000000000000000000000000000000000000000000\n",
        )
        .unwrap();
        let error = list_checkpoints(directory.path()).unwrap_err();
        assert_eq!(error.code(), "GF_CHECKPOINT_REGISTRY_CORRUPT");
    }

    #[test]
    fn checksummed_but_impossible_registry_identities_fail_closed() {
        let directory = tempdir().unwrap();
        crate::open_or_initialize_project(directory.path()).unwrap();
        create_checkpoint(
            directory.path(),
            &create_request(Uuid::now_v7(), "Tampered"),
        )
        .unwrap();
        let checkpoint_root = directory.path().join(CHECKPOINTS_DIR);
        let mut registry = read_registry(&checkpoint_root).unwrap();
        registry.active[0].checkpoint_uuid = Uuid::now_v7();
        write_raw_registry(directory.path(), &registry);
        assert_eq!(
            list_checkpoints(directory.path()).unwrap_err().code(),
            "GF_CHECKPOINT_REGISTRY_CORRUPT"
        );

        let directory = tempdir().unwrap();
        crate::open_or_initialize_project(directory.path()).unwrap();
        create_checkpoint(directory.path(), &create_request(Uuid::now_v7(), "Overlap")).unwrap();
        delete_checkpoint(
            directory.path(),
            &CheckpointDeleteRequest {
                operation_uuid: Uuid::now_v7(),
                name: "Overlap".into(),
                actor_uuid: None,
            },
        )
        .unwrap();
        let checkpoint_root = directory.path().join(CHECKPOINTS_DIR);
        let mut registry = read_registry(&checkpoint_root).unwrap();
        let row = &mut registry.tombstones[0];
        row.delete_operation_uuid = row.create_operation_uuid;
        row.delete_request_sha256 = hex(&delete_request_digest_values(
            row.delete_operation_uuid,
            &row.name,
            row.deleted_by,
        ));
        write_raw_registry(directory.path(), &registry);
        assert_eq!(
            list_checkpoints(directory.path()).unwrap_err().code(),
            "GF_CHECKPOINT_REGISTRY_CORRUPT"
        );
    }

    #[test]
    fn exact_input_bounds_and_writer_lock_are_enforced() {
        assert!(validate_name(&"a".repeat(MAX_NAME_BYTES)).is_ok());
        assert_eq!(
            validate_name(&"a".repeat(MAX_NAME_BYTES + 1))
                .unwrap_err()
                .code(),
            "GF_VALIDATION"
        );
        assert!(validate_description(Some(&"d".repeat(MAX_DESCRIPTION_BYTES))).is_ok());
        assert_eq!(
            validate_description(Some(&"d".repeat(MAX_DESCRIPTION_BYTES + 1)))
                .unwrap_err()
                .code(),
            "GF_VALIDATION"
        );

        let directory = tempdir().unwrap();
        let selected = crate::open_or_initialize_project(directory.path()).unwrap();
        let root = selected.container_root().to_owned();
        let locks = acquire_mutation_locks(&root).unwrap();
        let error = create_checkpoint(directory.path(), &create_request(Uuid::now_v7(), "Busy"))
            .unwrap_err();
        assert_eq!(error.code(), "GF_WRITER_BUSY");
        drop(locks);
    }

    #[cfg(unix)]
    #[test]
    fn linked_registry_surfaces_fail_closed_without_following_targets() {
        use std::os::unix::fs::symlink;

        for hard in [false, true] {
            let directory = tempdir().unwrap();
            crate::open_or_initialize_project(directory.path()).unwrap();
            create_checkpoint(directory.path(), &create_request(Uuid::now_v7(), "Linked")).unwrap();
            let checkpoint_root = directory.path().join(CHECKPOINTS_DIR);
            let checksum = checkpoint_root.join(CHECKSUM_FILE);
            let external = directory.path().join("external-checksum");
            fs::rename(&checksum, &external).unwrap();
            if hard {
                fs::hard_link(&external, &checksum).unwrap();
            } else {
                symlink(&external, &checksum).unwrap();
            }
            let external_before = fs::read(&external).unwrap();
            let error = while_writer_lock_is_held(directory.path(), || {
                list_checkpoints(directory.path()).unwrap_err()
            });
            assert_eq!(error.code(), "GF_CHECKPOINT_REGISTRY_CORRUPT");
            assert_eq!(fs::read(&external).unwrap(), external_before);
        }
    }

    #[test]
    fn no_intent_registry_corruption_wins_over_writer_contention() {
        let directory = tempdir().unwrap();
        crate::open_or_initialize_project(directory.path()).unwrap();
        create_checkpoint(directory.path(), &create_request(Uuid::now_v7(), "Corrupt")).unwrap();
        let checkpoint_root = directory.path().join(CHECKPOINTS_DIR);
        fs::write(
            checkpoint_root.join(CHECKSUM_FILE),
            b"not-the-registry-digest\n",
        )
        .unwrap();

        let expected = read_registry(&checkpoint_root).unwrap_err();
        let error = while_writer_lock_is_held(directory.path(), || {
            list_checkpoints(directory.path()).unwrap_err()
        });
        assert_eq!(error.code(), "GF_CHECKPOINT_REGISTRY_CORRUPT");
        assert_eq!(error.to_string(), expected.to_string());
    }

    #[test]
    fn intent_recovery_contention_preserves_writer_busy_and_intent() {
        let directory = tempdir().unwrap();
        crate::open_or_initialize_project(directory.path()).unwrap();
        let checkpoint_root = directory.path().join(CHECKPOINTS_DIR);
        let child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "project_checkpoints::tests::checkpoint_failpoint_helper",
                "--ignored",
            ])
            .env(
                "GRAPHFORGE_PROJECT_FAILPOINTS",
                "graphforge-internal-subprocess-v1",
            )
            .env(
                "GRAPHFORGE_PROJECT_FAILPOINT",
                "checkpoint.registry.before_replace",
            )
            .env("GRAPHFORGE_CHECKPOINT_TEST_ROOT", directory.path())
            .spawn()
            .unwrap();
        let status = BoundedChild {
            child,
            reaped: false,
        }
        .wait("intent-recovery-contention failpoint=checkpoint.registry.before_replace");
        assert_eq!(status.code(), Some(crate::project_failpoint::exit_code()));
        let intent_path = checkpoint_root.join(INTENT_FILE);
        let intent = fs::read(&intent_path).unwrap();
        let staged = fs::read_dir(&checkpoint_root)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".registry.")
            })
            .map(|entry| (entry.file_name(), fs::read(entry.path()).unwrap()))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(staged.len(), 2);

        let error = while_writer_lock_is_held(directory.path(), || {
            list_checkpoints(directory.path()).unwrap_err()
        });
        assert_eq!(error.code(), "GF_WRITER_BUSY");
        assert_eq!(fs::read(intent_path).unwrap(), intent);
        let staged_after = fs::read_dir(&checkpoint_root)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".registry.")
            })
            .map(|entry| (entry.file_name(), fs::read(entry.path()).unwrap()))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(staged_after, staged);
    }

    #[cfg(unix)]
    #[test]
    fn linked_project_root_is_rejected_before_checkpoint_access() {
        use std::os::unix::fs::symlink;

        let directory = tempdir().unwrap();
        let project = directory.path().join("project");
        fs::create_dir(&project).unwrap();
        crate::open_or_initialize_project(&project).unwrap();
        let linked = directory.path().join("linked-project");
        symlink(&project, &linked).unwrap();
        assert_eq!(
            list_checkpoints(&linked).unwrap_err().code(),
            "GF_UNSUPPORTED_PROJECT_FORMAT"
        );
    }

    #[test]
    fn checkpoint_pin_and_open_lease_control_recovery_cleanup() {
        let directory = tempdir().unwrap();
        crate::open_or_initialize_project(directory.path()).unwrap();
        let pinned_generation = publish_clone(directory.path());
        let created =
            create_checkpoint(directory.path(), &create_request(Uuid::now_v7(), "Pinned")).unwrap();
        assert_eq!(created.source_generation_uuid, pinned_generation);
        for _ in 0..4 {
            publish_clone(directory.path());
        }
        crate::recover_project_transactions(directory.path()).unwrap();
        recover_checkpoint_pair_after_lock_handoff(
            directory.path(),
            "action=delete-pinned-checkpoint recovery-complete",
        );
        let generation_path = directory
            .path()
            .join(crate::project_publication::GENERATIONS_DIR)
            .join(pinned_generation.hyphenated().to_string());
        assert!(
            generation_path.exists(),
            "active checkpoint lost its generation"
        );

        let lease =
            crate::project_publication::open_regular_lock(&generation_path.join("lease.lock"))
                .unwrap();
        FileExt::lock_shared(&lease).unwrap();
        delete_checkpoint(
            directory.path(),
            &CheckpointDeleteRequest {
                operation_uuid: Uuid::now_v7(),
                name: "Pinned".into(),
                actor_uuid: None,
            },
        )
        .unwrap();
        crate::recover_project_transactions(directory.path()).unwrap();
        assert!(generation_path.exists(), "an open lease was invalidated");
        FileExt::unlock(&lease).unwrap();
        drop(lease);
        crate::recover_project_transactions(directory.path()).unwrap();
        assert!(
            !generation_path.exists(),
            "deleted pin did not permit later GC"
        );
    }

    #[test]
    fn checkpoint_pin_survives_process_restart() {
        let directory = tempdir().unwrap();
        crate::open_or_initialize_project(directory.path()).unwrap();
        let pinned_generation = publish_clone(directory.path());
        create_checkpoint(
            directory.path(),
            &create_request(Uuid::from_u128(70), "Restart Pin"),
        )
        .unwrap();
        for _ in 0..4 {
            publish_clone(directory.path());
        }

        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "project_checkpoints::tests::checkpoint_failpoint_helper",
                "--ignored",
                "--nocapture",
            ])
            .env("GRAPHFORGE_CHECKPOINT_TEST_ROOT", directory.path())
            .env("GRAPHFORGE_CHECKPOINT_TEST_ACTION", "hold-open")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut output = BufReader::new(child.stdout.take().unwrap());
        let mut ready = String::new();
        while ready != "ready\n" {
            ready.clear();
            assert_ne!(
                output.read_line(&mut ready).unwrap(),
                0,
                "child exited before ready"
            );
        }

        delete_checkpoint(
            directory.path(),
            &CheckpointDeleteRequest {
                operation_uuid: Uuid::from_u128(71),
                name: "Restart Pin".into(),
                actor_uuid: None,
            },
        )
        .unwrap();
        crate::recover_project_transactions(directory.path()).unwrap();
        let generation_path = directory
            .path()
            .join(crate::project_publication::GENERATIONS_DIR)
            .join(pinned_generation.hyphenated().to_string());
        assert!(
            generation_path.exists(),
            "subprocess lease was not retained"
        );

        child.stdin.take().unwrap().write_all(b"release\n").unwrap();
        assert!(child.wait().unwrap().success());
        crate::recover_project_transactions(directory.path()).unwrap();
        assert!(
            !generation_path.exists(),
            "generation survived after the restarted reader released its lease"
        );
    }

    #[test]
    fn checkpoint_cleanup_removes_all_transient_resources() {
        let directory = tempdir().unwrap();
        crate::open_or_initialize_project(directory.path()).unwrap();
        create_checkpoint(
            directory.path(),
            &create_request(Uuid::from_u128(80), "Cleanup"),
        )
        .unwrap();
        publish_clone(directory.path());
        delete_checkpoint(
            directory.path(),
            &CheckpointDeleteRequest {
                operation_uuid: Uuid::from_u128(81),
                name: "Cleanup".into(),
                actor_uuid: None,
            },
        )
        .unwrap();
        crate::recover_project_transactions(directory.path()).unwrap();

        let selected = crate::resolve_project_generation(directory.path()).unwrap();
        let root = selected.container_root().to_owned();
        drop(selected);
        let checkpoint_root = root.join(CHECKPOINTS_DIR);
        let checkpoint_entries = fs::read_dir(&checkpoint_root)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            checkpoint_entries,
            BTreeSet::from([REGISTRY_FILE.into(), CHECKSUM_FILE.into()]),
            "checkpoint transaction staging leaked"
        );
        let trash = root.join("trash");
        assert!(
            !trash.exists() || fs::read_dir(&trash).unwrap().next().is_none(),
            "recovery trash was not emptied"
        );
        assert!(
            !root.join("cache").exists(),
            "checkpoint lifecycle leaked process-cache state to disk"
        );
        for entry in fs::read_dir(root.join(crate::project_publication::GENERATIONS_DIR)).unwrap() {
            let path = entry.unwrap().path();
            let name = path.file_name().unwrap().to_str().unwrap();
            Uuid::parse_str(name).expect("generation staging entry leaked");
            let lease = open_regular_lock(&path.join("lease.lock")).unwrap();
            assert!(FileExt::try_lock_exclusive(&lease).unwrap());
            FileExt::unlock(&lease).unwrap();
        }
        let lock_root = root.join(LOCKS_DIR);
        for name in [WRITER_LOCK_FILE, CHECKPOINT_LOCK_FILE] {
            let lock = open_regular_lock(&lock_root.join(name)).unwrap();
            assert!(FileExt::try_lock_exclusive(&lock).unwrap(), "{name} leaked");
            FileExt::unlock(&lock).unwrap();
        }
    }

    #[test]
    #[ignore = "subprocess failpoint helper"]
    fn checkpoint_failpoint_helper() {
        let root = std::env::var("GRAPHFORGE_CHECKPOINT_TEST_ROOT").unwrap();
        let action = std::env::var("GRAPHFORGE_CHECKPOINT_TEST_ACTION");
        if action.as_deref() == Ok("hold-open") {
            let (_, opened) = open_checkpoint_generation(root, "Restart Pin").unwrap();
            println!("ready");
            std::io::stdout().flush().unwrap();
            let mut release = String::new();
            std::io::stdin().read_line(&mut release).unwrap();
            assert_eq!(release, "release\n");
            assert!(opened.participant_snapshots().is_ok());
        } else if action.as_deref() == Ok("revert") {
            revert_checkpoint(
                root,
                &CheckpointRevertRequest {
                    operation_uuid: Uuid::from_u128(61),
                    name: "Base".into(),
                    reason: "crash recovery".into(),
                    actor_uuid: None,
                },
                || Ok(1_720_000_000_123_456),
                |_| Ok(()),
            )
            .unwrap();
        } else if action.as_deref() == Ok("delete") {
            delete_checkpoint(
                root,
                &CheckpointDeleteRequest {
                    operation_uuid: Uuid::parse_str("018f0f4e-7b8c-7000-8000-000000000031")
                        .unwrap(),
                    name: "Base".into(),
                    actor_uuid: None,
                },
            )
            .unwrap();
        } else {
            let request = create_request(
                Uuid::parse_str("018f0f4e-7b8c-7000-8000-000000000030").unwrap(),
                "Crash",
            );
            create_checkpoint(root, &request).unwrap();
        }
    }

    #[test]
    fn revert_publication_failpoint_matrix() {
        let failpoints = [
            ("project.after_journal_preparing", false),
            ("project.after_participant_dir_fsync", false),
            ("project.after_journal_staged", false),
            ("project.after_domain_validation", false),
            ("project.after_composite_validation", false),
            ("project.after_journal_validated", false),
            ("project.after_manifest_write", false),
            ("project.after_manifest_fsync", false),
            ("project.after_generation_dir_fsync", false),
            ("project.after_journal_durable", false),
            ("project.after_current_temp_write", false),
            ("project.after_current_temp_fsync", false),
            ("project.before_current_replace", false),
            ("project.after_current_replace", true),
            ("project.after_root_fsync", true),
            ("project.after_journal_published", true),
        ];
        for (failpoint, committed) in failpoints {
            let directory = tempdir().unwrap();
            crate::open_or_initialize_project(directory.path()).unwrap();
            create_checkpoint(
                directory.path(),
                &create_request(Uuid::from_u128(60), "Base"),
            )
            .unwrap();
            let prior = publish_clone(directory.path());
            let status = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "project_checkpoints::tests::checkpoint_failpoint_helper",
                    "--ignored",
                ])
                .env(
                    "GRAPHFORGE_PROJECT_FAILPOINTS",
                    "graphforge-internal-subprocess-v1",
                )
                .env("GRAPHFORGE_PROJECT_FAILPOINT", failpoint)
                .env("GRAPHFORGE_CHECKPOINT_TEST_ROOT", directory.path())
                .env("GRAPHFORGE_CHECKPOINT_TEST_ACTION", "revert")
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(crate::project_failpoint::exit_code()));
            recover_checkpoint_pair_after_lock_handoff(
                directory.path(),
                &format!("action=revert failpoint={failpoint} committed={committed}"),
            );
            crate::recover_project_transactions(directory.path()).unwrap();
            let recovered = crate::resolve_project_generation(directory.path()).unwrap();
            assert_eq!(
                recovered.generation_uuid() != prior,
                committed,
                "{failpoint}"
            );
            recover_checkpoint_pair_after_lock_handoff(
                directory.path(),
                &format!(
                    "action=revert parent-recovery-complete failpoint={failpoint} \
                     committed={committed}"
                ),
            );

            let (receipt, replayed) = revert_checkpoint(
                directory.path(),
                &CheckpointRevertRequest {
                    operation_uuid: Uuid::from_u128(61),
                    name: "Base".into(),
                    reason: "crash recovery".into(),
                    actor_uuid: None,
                },
                || Ok(1_720_000_000_123_456),
                |_| Ok(()),
            )
            .unwrap();
            assert_eq!(
                receipt.result_generation_uuid,
                Some(replayed.generation_uuid())
            );
            recover_checkpoint_pair_after_lock_handoff(
                directory.path(),
                &format!(
                    "action=revert replay-complete failpoint={failpoint} committed={committed}"
                ),
            );
            assert_eq!(list_checkpoints(directory.path()).unwrap().len(), 1);
        }
    }

    #[test]
    fn registry_failpoints_recover_exact_previous_or_next_revision() {
        for (failpoint, committed) in [
            ("checkpoint.registry.after_intent_file_fsync", false),
            ("checkpoint.registry.after_file_fsync", false),
            ("checkpoint.registry.before_replace", false),
            ("checkpoint.registry.after_replace", true),
            ("checkpoint.registry.after_dir_fsync", true),
        ] {
            let directory = tempdir().unwrap();
            crate::open_or_initialize_project(directory.path()).unwrap();
            let status = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "project_checkpoints::tests::checkpoint_failpoint_helper",
                    "--ignored",
                ])
                .env(
                    "GRAPHFORGE_PROJECT_FAILPOINTS",
                    "graphforge-internal-subprocess-v1",
                )
                .env("GRAPHFORGE_PROJECT_FAILPOINT", failpoint)
                .env("GRAPHFORGE_CHECKPOINT_TEST_ROOT", directory.path())
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(crate::project_failpoint::exit_code()));
            recover_checkpoint_pair_after_lock_handoff(
                directory.path(),
                &format!("action=create-unseeded failpoint={failpoint} committed={committed}"),
            );
            let rows = list_checkpoints(directory.path()).unwrap();
            assert_eq!(rows.len(), usize::from(committed), "{failpoint}");
            if !committed {
                recover_checkpoint_pair_after_lock_handoff(
                    directory.path(),
                    &format!(
                        "action=create-unseeded parent-read-complete failpoint={failpoint} \
                         committed={committed}"
                    ),
                );
                let request = create_request(
                    Uuid::parse_str("018f0f4e-7b8c-7000-8000-000000000030").unwrap(),
                    "Crash",
                );
                create_checkpoint(directory.path(), &request).unwrap();
                recover_checkpoint_pair_after_lock_handoff(
                    directory.path(),
                    &format!(
                        "action=create-unseeded replay-complete failpoint={failpoint} \
                         committed={committed}"
                    ),
                );
                assert_eq!(list_checkpoints(directory.path()).unwrap().len(), 1);
            }
        }
    }

    #[test]
    fn seeded_create_and_delete_failpoints_recover_exact_previous_or_next_revision() {
        let failpoints = [
            ("checkpoint.registry.after_intent_file_fsync", false),
            ("checkpoint.registry.after_file_fsync", false),
            ("checkpoint.registry.before_replace", false),
            ("checkpoint.registry.after_replace", true),
            ("checkpoint.registry.after_dir_fsync", true),
        ];
        for action in ["create", "delete"] {
            for (failpoint, committed) in failpoints {
                let directory = tempdir().unwrap();
                crate::open_or_initialize_project(directory.path()).unwrap();
                create_checkpoint(
                    directory.path(),
                    &create_request(
                        Uuid::parse_str("018f0f4e-7b8c-7000-8000-000000000029").unwrap(),
                        "Base",
                    ),
                )
                .unwrap();
                let status = Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "project_checkpoints::tests::checkpoint_failpoint_helper",
                        "--ignored",
                    ])
                    .env(
                        "GRAPHFORGE_PROJECT_FAILPOINTS",
                        "graphforge-internal-subprocess-v1",
                    )
                    .env("GRAPHFORGE_PROJECT_FAILPOINT", failpoint)
                    .env("GRAPHFORGE_CHECKPOINT_TEST_ROOT", directory.path())
                    .env("GRAPHFORGE_CHECKPOINT_TEST_ACTION", action)
                    .status()
                    .unwrap();
                assert_eq!(status.code(), Some(crate::project_failpoint::exit_code()));
                recover_checkpoint_pair_after_lock_handoff(
                    directory.path(),
                    &format!("action={action} failpoint={failpoint} committed={committed}"),
                );
                let rows = list_checkpoints(directory.path()).unwrap();
                let expected = match (action, committed) {
                    ("create", true) => vec!["Base", "Crash"],
                    ("create", false) | ("delete", false) => vec!["Base"],
                    ("delete", true) => vec![],
                    _ => unreachable!(),
                };
                assert_eq!(
                    rows.iter().map(|row| row.name.as_str()).collect::<Vec<_>>(),
                    expected,
                    "{action} {failpoint}"
                );
                recover_checkpoint_pair_after_lock_handoff(
                    directory.path(),
                    &format!(
                        "action={action} parent-read-complete failpoint={failpoint} \
                         committed={committed}"
                    ),
                );

                if action == "create" {
                    let replay = create_checkpoint(
                        directory.path(),
                        &create_request(
                            Uuid::parse_str("018f0f4e-7b8c-7000-8000-000000000030").unwrap(),
                            "Crash",
                        ),
                    )
                    .unwrap();
                    assert_eq!(replay.registry_revision, 2);
                } else {
                    let replay = delete_checkpoint(
                        directory.path(),
                        &CheckpointDeleteRequest {
                            operation_uuid: Uuid::parse_str("018f0f4e-7b8c-7000-8000-000000000031")
                                .unwrap(),
                            name: "Base".into(),
                            actor_uuid: None,
                        },
                    )
                    .unwrap();
                    assert_eq!(replay.registry_revision, 2);
                }
                recover_checkpoint_pair_after_lock_handoff(
                    directory.path(),
                    &format!(
                        "action={action} replay-complete failpoint={failpoint} \
                         committed={committed}"
                    ),
                );
                let final_rows = list_checkpoints(directory.path()).unwrap();
                let final_names = final_rows
                    .iter()
                    .map(|row| row.name.as_str())
                    .collect::<Vec<_>>();
                if action == "create" {
                    assert_eq!(final_names, vec!["Base", "Crash"], "{failpoint}");
                } else {
                    assert!(final_names.is_empty(), "{failpoint}");
                }
            }
        }
    }

    #[test]
    fn wave9_durable_registry_intent_recovers_every_atomic_pair_boundary() {
        for boundary in [
            "first-staged",
            "previous-staged",
            "registry-replaced",
            "next-complete",
        ] {
            let directory = tempdir().unwrap();
            crate::open_or_initialize_project(directory.path()).unwrap();
            let checkpoint_root = directory.path().join(CHECKPOINTS_DIR);
            let previous = Registry::empty();
            let mut next = Registry::empty();
            next.revision = 1;

            let has_previous = boundary != "first-staged";
            fs::create_dir_all(&checkpoint_root).unwrap();
            if has_previous {
                write_raw_registry(directory.path(), &previous);
            }
            let intent =
                install_registry_intent(&checkpoint_root, has_previous.then_some(&previous), &next);
            match boundary {
                "first-staged" | "previous-staged" => {}
                "registry-replaced" => {
                    fs::rename(
                        checkpoint_root.join(&intent.registry_temp),
                        checkpoint_root.join(REGISTRY_FILE),
                    )
                    .unwrap();
                }
                "next-complete" => {
                    fs::rename(
                        checkpoint_root.join(&intent.registry_temp),
                        checkpoint_root.join(REGISTRY_FILE),
                    )
                    .unwrap();
                    fs::rename(
                        checkpoint_root.join(&intent.checksum_temp),
                        checkpoint_root.join(CHECKSUM_FILE),
                    )
                    .unwrap();
                }
                _ => unreachable!(),
            }

            recover_pair(&checkpoint_root).unwrap();

            assert!(!checkpoint_root.join(INTENT_FILE).exists(), "{boundary}");
            assert!(
                !checkpoint_root.join(&intent.registry_temp).exists(),
                "{boundary}"
            );
            assert!(
                !checkpoint_root.join(&intent.checksum_temp).exists(),
                "{boundary}"
            );
            let recovered = read_registry(&checkpoint_root).unwrap();
            let expected_revision =
                usize::from(matches!(boundary, "registry-replaced" | "next-complete"));
            assert_eq!(recovered.revision, expected_revision as u64, "{boundary}");
        }
    }

    #[test]
    fn wave9_registry_intent_rejects_unsafe_names_and_wrong_staged_revision() {
        let directory = tempdir().unwrap();
        crate::open_or_initialize_project(directory.path()).unwrap();
        let checkpoint_root = directory.path().join(CHECKPOINTS_DIR);
        let mut next = Registry::empty();
        next.revision = 1;
        let mut intent = install_registry_intent(&checkpoint_root, None, &next);
        intent.registry_temp = "../registry.json".into();
        let mut bytes = serde_json::to_vec(&intent).unwrap();
        bytes.push(b'\n');
        fs::write(checkpoint_root.join(INTENT_FILE), bytes).unwrap();
        assert_eq!(
            recover_pair(&checkpoint_root).unwrap_err().code(),
            "GF_CHECKPOINT_REGISTRY_CORRUPT"
        );

        fs::remove_dir_all(&checkpoint_root).unwrap();
        fs::create_dir(&checkpoint_root).unwrap();
        let mut intent = install_registry_intent(&checkpoint_root, None, &next);
        intent.next_revision = 2;
        let mut bytes = serde_json::to_vec(&intent).unwrap();
        bytes.push(b'\n');
        fs::write(checkpoint_root.join(INTENT_FILE), bytes).unwrap();
        assert_eq!(
            recover_pair(&checkpoint_root).unwrap_err().code(),
            "GF_CHECKPOINT_REGISTRY_CORRUPT"
        );
    }

    #[test]
    fn recovery_rejects_missing_or_tampered_staged_pair() {
        for tamper_checksum in [false, true] {
            let directory = tempdir().unwrap();
            crate::open_or_initialize_project(directory.path()).unwrap();
            create_checkpoint(directory.path(), &create_request(Uuid::now_v7(), "Base")).unwrap();
            let status = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "project_checkpoints::tests::checkpoint_failpoint_helper",
                    "--ignored",
                ])
                .env(
                    "GRAPHFORGE_PROJECT_FAILPOINTS",
                    "graphforge-internal-subprocess-v1",
                )
                .env(
                    "GRAPHFORGE_PROJECT_FAILPOINT",
                    "checkpoint.registry.before_replace",
                )
                .env("GRAPHFORGE_CHECKPOINT_TEST_ROOT", directory.path())
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(crate::project_failpoint::exit_code()));
            preserve_checkpoint_intent_after_lock_handoff(
                directory.path(),
                &format!(
                    "action=create-tamper failpoint=checkpoint.registry.before_replace \
                     committed=false tamper_checksum={tamper_checksum}"
                ),
            );
            let checkpoint_root = directory.path().join(CHECKPOINTS_DIR);
            let intent_bytes = fs::read(checkpoint_root.join(INTENT_FILE)).unwrap();
            let intent: RegistryIntent = serde_json::from_slice(&intent_bytes).unwrap();
            if tamper_checksum {
                fs::write(
                    checkpoint_root.join(intent.checksum_temp),
                    b"0000000000000000000000000000000000000000000000000000000000000000\n",
                )
                .unwrap();
            } else {
                fs::remove_file(checkpoint_root.join(intent.registry_temp)).unwrap();
            }
            assert_eq!(
                list_checkpoints(directory.path()).unwrap_err().code(),
                "GF_CHECKPOINT_REGISTRY_CORRUPT"
            );
        }
    }

    #[test]
    fn checkpoint_text_and_identity_boundaries_are_canonical() {
        for invalid in [
            "",
            " leading",
            "trailing ",
            ".",
            "..",
            "two  spaces",
            "bad/name",
        ] {
            assert_eq!(validate_name(invalid).unwrap_err().code(), "GF_VALIDATION");
        }
        assert_eq!(validate_name("Résumé.v1").unwrap(), "Résumé.v1");
        let combining_acute = char::from_u32(0x301).unwrap();
        let decomposed = format!("Re{combining_acute}sume{combining_acute}");
        assert_eq!(
            validate_name(&decomposed).unwrap_err().code(),
            "GF_VALIDATION"
        );
        assert!(validate_description(None).is_ok());
        assert!(validate_description(Some("bounded description")).is_ok());
        assert_eq!(
            validate_description(Some("contains\ncontrol"))
                .unwrap_err()
                .code(),
            "GF_VALIDATION"
        );
        assert_eq!(
            validate_description(Some(&"x".repeat(MAX_DESCRIPTION_BYTES + 1)))
                .unwrap_err()
                .code(),
            "GF_VALIDATION"
        );
        assert_eq!(
            validate_reason("  restored after audit  ").unwrap(),
            "restored after audit"
        );
        assert_eq!(validate_reason("   ").unwrap_err().code(), "GF_VALIDATION");

        let operation = Uuid::now_v7();
        let actor = Uuid::now_v7();
        let digest = create_request_digest_values(operation, "baseline", Some("desc"), Some(actor));
        let checkpoint = checkpoint_uuid(operation, digest);
        let encoded = hex(&digest);
        assert_eq!(decode_digest(&encoded).unwrap(), digest);
        assert!(
            validate_record_identity(
                checkpoint,
                operation,
                "baseline",
                Some("desc"),
                Some(actor),
                &encoded,
            )
            .is_ok()
        );
        assert_eq!(
            validate_record_identity(
                Uuid::nil(),
                operation,
                "baseline",
                Some("desc"),
                Some(actor),
                &encoded,
            )
            .unwrap_err()
            .code(),
            "GF_CHECKPOINT_REGISTRY_CORRUPT"
        );
        for malformed in ["0", &"A".repeat(64), &"g".repeat(64)] {
            assert_eq!(
                decode_digest(malformed).unwrap_err().code(),
                "GF_CHECKPOINT_REGISTRY_CORRUPT"
            );
        }
        assert!(valid_private_name(
            &format!(".registry.{operation}.json.next"),
            operation,
            "json"
        ));
        assert!(!valid_private_name(
            ".registry.other.json.next",
            operation,
            "json"
        ));
        assert_eq!(
            parse_uuid("not-a-uuid").unwrap_err().code(),
            "GF_CHECKPOINT_REGISTRY_CORRUPT"
        );
    }

    #[test]
    fn empty_checkpoint_registry_has_stable_canonical_bytes() {
        let registry = Registry::empty();
        let first = registry.canonical_bytes().unwrap();
        let second = registry.canonical_bytes().unwrap();
        assert_eq!(first, second);
        assert!(first.ends_with(b"\n"));
        let decoded: Registry = serde_json::from_slice(&first).unwrap();
        assert_eq!(decoded, registry);
    }

    #[test]
    fn checkpoint_operation_identities_remain_disjoint_across_tombstones() {
        let root = tempdir().unwrap();
        crate::open_or_initialize_project(root.path()).unwrap();
        let create_operation = Uuid::now_v7();
        let create = create_request(create_operation, "Baseline");
        let created = create_checkpoint(root.path(), &create).unwrap();
        let exact_create_replay = create_checkpoint(root.path(), &create).unwrap();
        assert_eq!(exact_create_replay, created);

        let changed_create = CheckpointCreateRequest {
            name: "Changed".into(),
            ..create.clone()
        };
        assert_eq!(
            create_checkpoint(root.path(), &changed_create)
                .unwrap_err()
                .code(),
            "GF_IDEMPOTENCY_CONFLICT"
        );
        assert_eq!(list_checkpoints(root.path()).unwrap().len(), 1);

        let delete_operation = Uuid::now_v7();
        let delete = CheckpointDeleteRequest {
            operation_uuid: delete_operation,
            name: "Baseline".into(),
            actor_uuid: create.actor_uuid,
        };
        let deleted = delete_checkpoint(root.path(), &delete).unwrap();
        let exact_delete_replay = delete_checkpoint(root.path(), &delete).unwrap();
        assert_eq!(exact_delete_replay, deleted);
        assert!(list_checkpoints(root.path()).unwrap().is_empty());

        let tombstone_create_replay = create_checkpoint(root.path(), &create).unwrap();
        assert_eq!(
            tombstone_create_replay.checkpoint_uuid,
            created.checkpoint_uuid
        );
        assert_eq!(tombstone_create_replay, created);
        assert!(list_checkpoints(root.path()).unwrap().is_empty());

        let changed_delete = CheckpointDeleteRequest {
            name: "Other".into(),
            ..delete.clone()
        };
        assert_eq!(
            delete_checkpoint(root.path(), &changed_delete)
                .unwrap_err()
                .code(),
            "GF_IDEMPOTENCY_CONFLICT"
        );
        assert_eq!(
            create_checkpoint(root.path(), &create_request(delete_operation, "Other"))
                .unwrap_err()
                .code(),
            "GF_IDEMPOTENCY_CONFLICT"
        );
        assert_eq!(
            delete_checkpoint(
                root.path(),
                &CheckpointDeleteRequest {
                    operation_uuid: create_operation,
                    name: "Missing".into(),
                    actor_uuid: None,
                },
            )
            .unwrap_err()
            .code(),
            "GF_IDEMPOTENCY_CONFLICT"
        );
        assert_eq!(
            delete_checkpoint(
                root.path(),
                &CheckpointDeleteRequest {
                    operation_uuid: Uuid::now_v7(),
                    name: "Missing".into(),
                    actor_uuid: None,
                },
            )
            .unwrap_err()
            .code(),
            "GF_CHECKPOINT_NOT_FOUND"
        );
        assert!(list_checkpoints(root.path()).unwrap().is_empty());
    }

    #[test]
    fn registry_header_sort_and_revision_validation_matrix_uses_durable_records() {
        let directory = tempdir().unwrap();
        crate::open_or_initialize_project(directory.path()).unwrap();
        create_checkpoint(
            directory.path(),
            &create_request(Uuid::from_u128(701), "Alpha"),
        )
        .unwrap();
        create_checkpoint(
            directory.path(),
            &create_request(Uuid::from_u128(702), "Beta"),
        )
        .unwrap();
        let root = directory.path().join(CHECKPOINTS_DIR);
        let stable = read_registry(&root).unwrap();
        assert!(validate_registry(&stable).is_ok());

        let mutations: Vec<Box<dyn Fn(&mut Registry)>> = vec![
            Box::new(|registry| registry.format = "future".into()),
            Box::new(|registry| registry.format_version = 2),
            Box::new(|registry| registry.active.reverse()),
            Box::new(|registry| registry.active[0].created_revision = 0),
            Box::new(|registry| registry.active[1].name = registry.active[0].name.clone()),
            Box::new(|registry| {
                registry.active[1].checkpoint_uuid = registry.active[0].checkpoint_uuid
            }),
            Box::new(|registry| {
                registry.active[1].create_operation_uuid = registry.active[0].create_operation_uuid
            }),
        ];
        for mutate in mutations {
            let mut candidate = stable.clone();
            mutate(&mut candidate);
            assert_eq!(
                validate_registry(&candidate).unwrap_err().code(),
                "GF_CHECKPOINT_REGISTRY_CORRUPT"
            );
        }
        assert_eq!(read_registry(&root).unwrap(), stable);
    }

    #[test]
    fn tombstone_identity_revision_and_operation_disjointness_matrix_is_total() {
        let directory = tempdir().unwrap();
        crate::open_or_initialize_project(directory.path()).unwrap();
        create_checkpoint(
            directory.path(),
            &create_request(Uuid::from_u128(801), "Deleted"),
        )
        .unwrap();
        delete_checkpoint(
            directory.path(),
            &CheckpointDeleteRequest {
                operation_uuid: Uuid::from_u128(802),
                name: "Deleted".into(),
                actor_uuid: None,
            },
        )
        .unwrap();
        let root = directory.path().join(CHECKPOINTS_DIR);
        let stable = read_registry(&root).unwrap();
        assert_eq!(stable.active.len(), 0);
        assert_eq!(stable.tombstones.len(), 1);
        assert!(validate_registry(&stable).is_ok());

        let mutations: Vec<Box<dyn Fn(&mut Registry)>> = vec![
            Box::new(|registry| registry.tombstones[0].name = " bad".into()),
            Box::new(|registry| registry.tombstones[0].description = Some("bad\nvalue".into())),
            Box::new(|registry| registry.tombstones[0].generation_manifest_sha256 = "bad".into()),
            Box::new(|registry| registry.tombstones[0].create_request_sha256 = "bad".into()),
            Box::new(|registry| registry.tombstones[0].delete_request_sha256 = "bad".into()),
            Box::new(|registry| registry.tombstones[0].checkpoint_uuid = Uuid::nil()),
            Box::new(|registry| registry.tombstones[0].created_revision = 0),
            Box::new(|registry| {
                registry.tombstones[0].deleted_revision = registry.tombstones[0].created_revision
            }),
            Box::new(|registry| registry.tombstones[0].deleted_revision = registry.revision + 1),
            Box::new(|registry| {
                registry.tombstones[0].delete_operation_uuid =
                    registry.tombstones[0].create_operation_uuid
            }),
            Box::new(|registry| registry.revision = 0),
        ];
        for mutate in mutations {
            let mut candidate = stable.clone();
            mutate(&mut candidate);
            assert_eq!(
                validate_registry(&candidate).unwrap_err().code(),
                "GF_CHECKPOINT_REGISTRY_CORRUPT"
            );
        }

        let mut duplicate = stable.clone();
        let mut second = duplicate.tombstones[0].clone();
        second.deleted_revision += 1;
        duplicate.revision = second.deleted_revision;
        duplicate.tombstones.push(second);
        assert_eq!(
            validate_registry(&duplicate).unwrap_err().code(),
            "GF_CHECKPOINT_REGISTRY_CORRUPT"
        );
        assert_eq!(read_registry(&root).unwrap(), stable);
    }

    #[test]
    fn public_checkpoint_operations_reject_cross_kind_uuid_reuse_after_reopen() {
        let root = tempdir().unwrap();
        crate::open_or_initialize_project(root.path()).unwrap();
        let operation = Uuid::now_v7();
        create_checkpoint(root.path(), &create_request(operation, "release")).unwrap();

        let delete = CheckpointDeleteRequest {
            operation_uuid: operation,
            name: "release".into(),
            actor_uuid: None,
        };
        assert_eq!(
            delete_checkpoint(root.path(), &delete).unwrap_err().code(),
            "GF_IDEMPOTENCY_CONFLICT"
        );

        let delete_operation = Uuid::now_v7();
        delete_checkpoint(
            root.path(),
            &CheckpointDeleteRequest {
                operation_uuid: delete_operation,
                name: "release".into(),
                actor_uuid: None,
            },
        )
        .unwrap();
        assert_eq!(
            create_checkpoint(
                root.path(),
                &create_request(delete_operation, "replacement")
            )
            .unwrap_err()
            .code(),
            "GF_IDEMPOTENCY_CONFLICT"
        );
        assert!(list_checkpoints(root.path()).unwrap().is_empty());
    }
}
