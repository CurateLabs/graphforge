//! Controls for graph construction.

use super::{
    CHECKPOINT, Checkpoint, ConstructionPublicationState, Deserialize, DetailCodec, File,
    FileIdentity, GfError, GraphConstructionBudgets, GraphConstructionState, MAX_CONTROL_BYTES,
    MAX_SHAPE_CONTROL_BYTES, OsStr, OsString, Read, SHAPE_INTENT, Serialize, ShapeIntent,
    StableDirectory, Uuid, Write, checked_evidence_sum, construction_failpoint, file_identity,
    file_link_count, is_control_temp, sha256, storage,
};

/// Current-format phase totals must be exact; omitted old-version fields are refused.
pub(super) fn validate_parent_phase_bytes(checkpoint: &Checkpoint) -> Result<(), GfError> {
    let evidence = &checkpoint.evidence;
    let expected_shape = checked_evidence_sum(
        "parent shape phase bytes",
        evidence.parent_catalog_read_bytes,
        &[
            evidence.shape_input_validation_read_bytes,
            evidence.merge_read_bytes,
            evidence.parquet_read_bytes,
            evidence.shaped_output_authentication_bytes,
            evidence.retained_probe_read_bytes,
        ],
    )?;
    if evidence.seal_application_read_bytes != evidence.authentication_read_bytes
        || evidence.shape_application_read_bytes != expected_shape
    {
        return Err(storage("construction parent phase bytes disagree"));
    }
    Ok(())
}

pub(super) fn control_sha256(value: &impl Serialize) -> Result<String, GfError> {
    Ok(sha256(&serde_json::to_vec(value).map_err(storage)?))
}

pub(super) fn validate_sha256(value: &str, label: &str) -> Result<(), GfError> {
    if !is_canonical_sha256(value) {
        return Err(storage(format!("{label} digest is invalid")));
    }
    Ok(())
}

pub(super) fn is_canonical_sha256(value: &str) -> bool {
    is_canonical_lower_hex(value, 64)
}

pub(super) fn is_canonical_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn validate_checkpoint(
    checkpoint: &Checkpoint,
    operation: Uuid,
    project: FileIdentity,
    session: FileIdentity,
    generation: u64,
    ontology_mode: graphforge_core::OntologyMode,
    lifecycle_mode: crate::filesystem_admission::ProjectLifecycleMode,
    semantic_authority_sha256: Option<&str>,
    budgets: GraphConstructionBudgets,
    parent_catalog_sha256: Option<&str>,
    parent_generation_uuid: Uuid,
    parent_generation_manifest_sha256: &str,
) -> Result<(), GfError> {
    let minimum_artifacts = checkpoint
        .next_sequence
        .checked_mul(3)
        .ok_or_else(|| storage("checkpoint artifact lower bound overflow"))?;
    let maximum_artifacts = checkpoint
        .next_sequence
        .checked_mul(4)
        .ok_or_else(|| storage("checkpoint artifact upper bound overflow"))?;
    let schema_groups = checkpoint
        .node_schema_sha256
        .len()
        .checked_add(checkpoint.edge_schema_sha256.len())
        .ok_or_else(|| storage("checkpoint schema-group count overflow"))?;
    if DetailCodec::from_version(checkpoint.format_version).is_err()
        || checkpoint.operation_uuid != operation
        || !checkpoint.project_identity.matches(project)
        || !checkpoint.session_identity.matches(session)
        || checkpoint.parent_topology_generation != generation
        || checkpoint.parent_generation_uuid != parent_generation_uuid
        || checkpoint.parent_generation_manifest_sha256 != parent_generation_manifest_sha256
        || checkpoint.parent_generation_uuid.is_nil()
        || validate_sha256(
            &checkpoint.parent_generation_manifest_sha256,
            "parent generation manifest",
        )
        .is_err()
        || checkpoint.ontology_mode != ontology_mode
        || checkpoint.lifecycle_mode != lifecycle_mode
        || checkpoint.semantic_authority_sha256.as_deref() != semantic_authority_sha256
        || checkpoint
            .semantic_authority_sha256
            .as_ref()
            .is_some_and(|digest| !is_canonical_sha256(digest))
        || checkpoint.session_now_micros <= 0
        || checkpoint.budgets != budgets
        || checkpoint.has_base_snapshot != (generation != 0)
        || checkpoint.parent_catalog_sha256.as_deref() != parent_catalog_sha256
        || checkpoint
            .parent_catalog_sha256
            .as_ref()
            .is_some_and(|digest| !is_canonical_sha256(digest))
        || checkpoint.next_sequence > budgets.max_chunks
        || checkpoint.last_receipt_sha256.is_some() != (checkpoint.next_sequence != 0)
        || checkpoint
            .last_receipt_sha256
            .as_ref()
            .is_some_and(|digest| !is_canonical_sha256(digest))
        || checkpoint
            .shape_authority_sha256
            .as_ref()
            .is_some_and(|digest| !is_canonical_sha256(digest))
        || checkpoint
            .encoding_inventory_sha256
            .as_ref()
            .is_some_and(|digest| !is_canonical_sha256(digest))
        || checkpoint.encoding_inventory_sha256.is_some()
            && checkpoint.shape_authority_sha256.is_none()
        || checkpoint.inputs_retired && (checkpoint.shape_authority_sha256.is_none())
        || checkpoint.shape_retired && (checkpoint.encoding_inventory_sha256.is_none())
        || match checkpoint.state {
            GraphConstructionState::Staging | GraphConstructionState::Aborted => {
                checkpoint.publication_state.is_some()
            }
            GraphConstructionState::Sealed => checkpoint.publication_state.is_none(),
        }
        || matches!(
            checkpoint.publication_state,
            Some(
                ConstructionPublicationState::Publishing | ConstructionPublicationState::Published
            )
        ) && checkpoint.encoding_inventory_sha256.is_none()
        || checkpoint.evidence.input_batches != checkpoint.next_sequence
        || checkpoint.evidence.parquet_shards != checkpoint.next_sequence
        || (checkpoint.evidence.immutable_artifacts < minimum_artifacts
            || checkpoint.evidence.immutable_artifacts > maximum_artifacts)
        || checkpoint.evidence.peak_batch_rows > budgets.max_batch_rows as u64
        || checkpoint.evidence.peak_batch_bytes > budgets.max_batch_bytes as u64
        || checkpoint.evidence.peak_run_records > budgets.max_run_records as u64
        || checkpoint.evidence.prior_topology_rows_decoded != 0
        || checkpoint.evidence.current_transitions != 0
        || checkpoint
            .node_schema_sha256
            .iter()
            .any(|digest| !is_canonical_sha256(digest))
        || checkpoint
            .edge_schema_sha256
            .iter()
            .any(|digest| !is_canonical_sha256(digest))
        || schema_groups > budgets.max_schema_groups
    {
        return Err(storage("checkpoint authority or resume parameters changed"));
    }
    Ok(())
}

/// A completed initial checkpoint temporary pins only the codec choice. It is
/// never promoted: the ordinary writer creates the initial checkpoint after
/// verifying the candidate against the current admitted parent and parameters.
pub(super) fn initial_checkpoint_format(
    root: &StableDirectory,
    project_identity: FileIdentity,
    initial: &Checkpoint,
) -> Result<u32, GfError> {
    let mut selected = None;
    for name in root.child_names().map_err(storage)? {
        let Some(text) = name.to_str() else { continue };
        if !text.starts_with(".checkpoint.json-") || !is_control_temp(text) {
            continue;
        }
        let mut file = root.open_child_file(&name).map_err(storage)?;
        let body = read_bounded_limit(&mut file, MAX_CONTROL_BYTES)?;
        let Ok(candidate) = serde_json::from_slice::<Checkpoint>(&body) else {
            continue;
        };
        if candidate.operation_uuid != initial.operation_uuid
            || candidate.project_identity != initial.project_identity
            || candidate.session_identity != initial.session_identity
        {
            continue;
        }
        if file_link_count(&file).map_err(storage)? != 1 {
            return Err(storage("initial checkpoint temporary has unexpected links"));
        }
        validate_checkpoint(
            &candidate,
            initial.operation_uuid,
            project_identity,
            root.identity(),
            initial.parent_topology_generation,
            initial.ontology_mode,
            initial.lifecycle_mode,
            initial.semantic_authority_sha256.as_deref(),
            initial.budgets,
            initial.parent_catalog_sha256.as_deref(),
            initial.parent_generation_uuid,
            &initial.parent_generation_manifest_sha256,
        )?;
        if candidate.state != GraphConstructionState::Staging
            || candidate.next_sequence != 0
            || candidate.saw_edge
            || candidate.last_receipt_sha256.is_some()
            || candidate.publication_state.is_some()
            || candidate.shape_authority_sha256.is_some()
            || candidate.encoding_inventory_sha256.is_some()
            || !candidate.node_schema_sha256.is_empty()
            || !candidate.edge_schema_sha256.is_empty()
        {
            return Err(storage("checkpoint temporary is not initial authority"));
        }
        if selected.is_some_and(|version| version != candidate.format_version) {
            return Err(storage("initial checkpoint temporary versions disagree"));
        }
        selected = Some(candidate.format_version);
    }
    Ok(selected.unwrap_or(initial.format_version))
}

pub(super) fn install_control<T: Serialize>(
    root: &StableDirectory,
    target: &str,
    value: &T,
) -> Result<(), GfError> {
    let body = encode_control(value, target)?;
    let temporary = control_temp(target);
    let mut file = root
        .create_replaceable_child_file(OsStr::new(&temporary))
        .map_err(storage)?;
    let identity = file_identity(&file).map_err(storage)?;
    write_control_body(&mut file, &body, target, "install")?;
    file.sync_all().map_err(storage)?;
    root.sync().map_err(storage)?;
    construction_failpoint(&format!("control.install.after_temp_fsync.{target}"));
    root.install_child(OsStr::new(&temporary), identity, OsStr::new(target))
        .map_err(storage)?;
    root.sync().map_err(storage)?;
    construction_failpoint(&format!("control.install.after_install.{target}"));
    Ok(())
}

/// Directory-durability batch for the spill seal path (#1452).
///
/// Sealing one spill used to issue three directory syncs on the partition
/// directory's inode — one after the artifact rename, and two inside the
/// receipt install — every one serialized against every other family and
/// partition doing the same work. The batch amortizes them: renames keep
/// their order and their refusals (`RENAME_NOREPLACE` plus the post-rename
/// identity reconciliation), receipt content stays fsynced before its name
/// is linked, and one flush at the batch boundary makes every name linked
/// by the batch durable together.
///
/// A crash before the flush can drop any subset of those names, which is
/// exactly the incomplete-shape state `recover_shape_intent` already cleans
/// and re-runs: receipt installs are idempotent, surviving payloads are
/// authenticated before removal, and a receipt whose artifact name was lost
/// is tolerated (`NotFound` skips its payload work). The completed-shape
/// state is unreachable before the flush, because the shape intent is marked
/// complete only after every seal batch of the shape has flushed and the
/// outputs have been published durably.
pub(super) struct SealDirectoryBatch<'a> {
    root: &'a StableDirectory,
    pending: bool,
}

impl<'a> SealDirectoryBatch<'a> {
    pub(super) fn new(root: &'a StableDirectory) -> Self {
        Self {
            root,
            pending: false,
        }
    }

    /// Record that a name was linked into the directory and must become
    /// durable at the next flush.
    pub(super) fn mark(&mut self) {
        self.pending = true;
    }

    /// Take over `other`'s pending names, so this batch's flush makes them
    /// durable and `other` no longer syncs on drop (#1448: a lane seals into
    /// its own batch, which the boundary's batch absorbs).
    pub(super) fn absorb(&mut self, other: &mut SealDirectoryBatch<'_>) {
        self.pending |= std::mem::take(&mut other.pending);
    }

    /// Make every name linked since the last flush durable. Idempotent; the
    /// barrier is counted only when a sync was actually required.
    pub(super) fn flush(
        &mut self,
        evidence: &mut super::GraphConstructionEvidence,
    ) -> Result<(), GfError> {
        if !self.pending {
            return Ok(());
        }
        self.root.sync().map_err(storage)?;
        self.pending = false;
        evidence.merge_directory_fsync_operations = evidence
            .merge_directory_fsync_operations
            .checked_add(1)
            .ok_or_else(|| storage("merge directory fsync operations overflows"))?;
        Ok(())
    }
}

impl Drop for SealDirectoryBatch<'_> {
    fn drop(&mut self) {
        // Durability before refusal: a failure between renames still leaves
        // every name the batch already linked as durable as it can be, so the
        // recovery contract sees no state the unbatched protocol could not
        // produce. The sync result cannot beat the primary error back to the
        // caller, and every real consumer re-establishes durability itself
        // (the same best-effort pattern as spill abandonment).
        if self.pending {
            let _ = self.root.sync();
        }
    }
}

/// Install a control whose containing-directory durability is provided by a
/// later [`SealDirectoryBatch`] flush instead of this call (#1452).
///
/// The control body is fsynced before its name is linked, exactly as in
/// [`install_control`]; only the two per-call directory syncs are deferred
/// into the batch. A crash in the window loses at most the control's name,
/// which recovery handles like a control that was never written.
pub(super) fn install_control_batched<T: Serialize>(
    root: &StableDirectory,
    target: &str,
    value: &T,
    batch: &mut SealDirectoryBatch,
) -> Result<(), GfError> {
    let body = encode_control(value, target)?;
    let temporary = control_temp(target);
    let mut file = root
        .create_replaceable_child_file(OsStr::new(&temporary))
        .map_err(storage)?;
    let identity = file_identity(&file).map_err(storage)?;
    write_control_body(&mut file, &body, target, "install")?;
    file.sync_all().map_err(storage)?;
    construction_failpoint(&format!("control.install.after_temp_fsync.{target}"));
    root.install_child(OsStr::new(&temporary), identity, OsStr::new(target))
        .map_err(storage)?;
    batch.mark();
    construction_failpoint(&format!("control.install.after_install.{target}"));
    Ok(())
}

pub(super) fn replace_control<T: Serialize>(
    root: &StableDirectory,
    target: &str,
    value: &T,
) -> Result<(), GfError> {
    let body = encode_control(value, target)?;
    let temporary = control_temp(target);
    let mut file = root
        .create_replaceable_child_file(OsStr::new(&temporary))
        .map_err(storage)?;
    let identity = file_identity(&file).map_err(storage)?;
    write_control_body(&mut file, &body, target, "replace")?;
    file.sync_all().map_err(storage)?;
    root.sync().map_err(storage)?;
    construction_failpoint(&format!("control.replace.after_temp_fsync.{target}"));
    root.replace_child(OsStr::new(&temporary), identity, OsStr::new(target))
        .map_err(storage)?;
    root.sync().map_err(storage)?;
    construction_failpoint(&format!("control.replace.after_replace.{target}"));
    Ok(())
}

/// Persist only resumable allocation state. Transition history is live
/// operation evidence; serializing it would make the fixed-size checkpoint
/// grow with every accepted chunk. The exact current union and numeric peak
/// remain durable.
pub(super) fn replace_checkpoint_control(
    root: &StableDirectory,
    checkpoint: &Checkpoint,
) -> Result<(), GfError> {
    let mut durable = checkpoint.clone();
    durable.evidence.storage_allocation_transitions.clear();
    replace_control(root, CHECKPOINT, &durable)
}

/// Persist a shape intent without transition history.
///
/// The same rule the checkpoint states above: transition history is live
/// operation evidence, and serializing it makes a fixed-purpose durable
/// control grow with the number of artifacts the shape installed. Nothing
/// reads it back as authority — recovery clears both sides before comparing
/// persisted evidence, adopts the intent's transitions only to reseed a
/// single-entry union, and `copy_post_shape_io` overwrites them from the
/// intent it is comparing against (#1526).
pub(super) fn install_shape_intent(
    root: &StableDirectory,
    intent: &mut ShapeIntent,
) -> Result<(), GfError> {
    drop_live_evidence(intent);
    install_control(root, SHAPE_INTENT, intent)
}

/// Replace a shape intent without transition history; see
/// [`install_shape_intent`].
pub(super) fn replace_shape_intent(
    root: &StableDirectory,
    intent: &mut ShapeIntent,
) -> Result<(), GfError> {
    drop_live_evidence(intent);
    replace_control(root, SHAPE_INTENT, intent)
}

/// Drop the transition history from an intent's evidence, in place, so the
/// record written and the record the writer holds agree.
fn drop_live_evidence(intent: &mut ShapeIntent) {
    intent.baseline_evidence.storage_allocation_transitions = Vec::new();
    if let Some(final_evidence) = intent.final_evidence.as_mut() {
        final_evidence.storage_allocation_transitions = Vec::new();
    }
}

fn control_limit(target: &str) -> u64 {
    if target == SHAPE_INTENT
        || target.starts_with(super::progress::SHAPE_PROGRESS_PREFIX)
        || target.starts_with(super::finish_stages::SHAPE_STAGE_PREFIX)
    {
        MAX_SHAPE_CONTROL_BYTES
    } else {
        MAX_CONTROL_BYTES
    }
}

struct BoundedControlWriter {
    body: Vec<u8>,
    limit: usize,
}

impl Write for BoundedControlWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let new_length = self
            .body
            .len()
            .checked_add(bytes.len())
            .ok_or_else(|| std::io::Error::other("control record length overflow"))?;
        if new_length > self.limit {
            return Err(std::io::Error::other("control record exceeds bound"));
        }
        self.body.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn encode_control<T: Serialize>(value: &T, target: &str) -> Result<Vec<u8>, GfError> {
    let limit = usize::try_from(control_limit(target)).map_err(storage)?;
    let mut writer = BoundedControlWriter {
        body: Vec::new(),
        limit,
    };
    serde_json::to_writer(&mut writer, value).map_err(storage)?;
    Ok(writer.body)
}

pub(super) fn decode_bounded<T: for<'de> Deserialize<'de>>(file: &mut File) -> Result<T, GfError> {
    serde_json::from_slice(&read_bounded(file)?).map_err(storage)
}

pub(super) fn decode_shape_intent(file: &mut File) -> Result<ShapeIntent, GfError> {
    serde_json::from_slice(&read_bounded_limit(file, MAX_SHAPE_CONTROL_BYTES)?).map_err(storage)
}

fn read_bounded(file: &mut File) -> Result<Vec<u8>, GfError> {
    read_bounded_limit(file, MAX_CONTROL_BYTES)
}

pub(super) fn read_bounded_limit(file: &mut File, limit: u64) -> Result<Vec<u8>, GfError> {
    if file.metadata().map_err(storage)?.len() > limit {
        return Err(storage("control record exceeds bound"));
    }
    let mut body = Vec::new();
    file.take(limit + 1)
        .read_to_end(&mut body)
        .map_err(storage)?;
    if body.len() as u64 > limit {
        return Err(storage("control record exceeds bound"));
    }
    Ok(body)
}

pub(super) fn control_temp(target: &str) -> OsString {
    OsString::from(format!(".{target}-{}.tmp", Uuid::new_v4().simple()))
}

pub(super) fn artifact_temp(target: &str) -> OsString {
    OsString::from(format!(
        ".artifact-{target}-{}.tmp",
        Uuid::new_v4().simple()
    ))
}

fn write_control_body(
    file: &mut File,
    body: &[u8],
    target: &str,
    operation: &str,
) -> Result<(), GfError> {
    #[cfg(test)]
    {
        let middle = body.len() / 2;
        file.write_all(&body[..middle]).map_err(storage)?;
        file.sync_all().map_err(storage)?;
        construction_failpoint(&format!("control.{operation}.after_partial.{target}"));
        file.write_all(&body[middle..]).map_err(storage)?;
    }
    #[cfg(not(test))]
    {
        let _ = (target, operation);
        file.write_all(body).map_err(storage)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
