//! Recovery for graph construction.

use super::{
    ArtifactReceipt, BASE_IDENTITY_WIDTH, BLOCK_BYTES, BufReader, BufWriter,
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, Checkpoint, ChunkIntent,
    ConstructionChunkKind, ConstructionChunkReceipt, ConstructionPublicationIntent,
    ConstructionPublicationReceipt, CountingChunkReader, DetailCodec, DetailValidator, Digest,
    EDGE_DETAIL_WIDTH, ENDPOINT_WIDTH, FileIdentity, GfError, GraphConstructionEvidence,
    GraphConstructionSession, HashingWriter, IDENTITY_SURROGATE_OFFSET, IDENTITY_WIDTH, INTENT,
    IoCounter, MAX_SHAPE_CONTROL_BYTES, NODE_DETAIL_WIDTH, OsStr, ParquetRecordBatchReaderBuilder,
    Read, ReceiptPointer, SHAPE_INTENT, Sha256, ShapeIntent, StableDirectory, Uuid, Write,
    account_cache_release, artifact_stem, authenticate_shaped_output,
    authenticate_shaped_output_identity, checked_category_remove, combine_cache_cleanup,
    combine_secondary_cleanup, construction_failpoint, copy_post_shape_io, decode_bounded,
    decode_shape_intent, file_identity, file_link_count, hex, install_control, is_canonical_sha256,
    is_shape_artifact_name, merge_cache_release_evidence, read_bounded_limit, receipt_from_intent,
    receipt_name, record_active_identity_remove, replace_checkpoint_control, sha256,
    shape_authority_sha256, shape_receipt_name, storage, supersession, validate_artifact_name,
    validate_intent, validate_receipt_artifacts, validate_receipt_semantics,
    validate_shape_binding, validate_sorted_run,
};

impl GraphConstructionSession {
    #[allow(clippy::too_many_lines)]
    pub(super) fn recover_intent(&mut self) -> Result<(), GfError> {
        let mut file = match self.root.open_child_file(OsStr::new(INTENT)) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(storage(error)),
        };
        let intent: ChunkIntent = decode_bounded(&mut file)?;
        validate_intent(&intent, &self.checkpoint)?;
        let receipt_name = receipt_name(intent.sequence);
        match self.root.open_child_file(OsStr::new(&receipt_name)) {
            Ok(mut receipt_file) => {
                let receipt: ConstructionChunkReceipt = decode_bounded(&mut receipt_file)?;
                validate_receipt_semantics(
                    &receipt,
                    intent.sequence,
                    self.checkpoint.budgets,
                    DetailCodec::from_version(self.checkpoint.format_version).map_err(storage)?,
                )?;
                if receipt != receipt_from_intent(&intent)? {
                    return Err(storage("recovered receipt differs from durable intent"));
                }
                let recovery_work = validate_receipt_artifacts(
                    &self.root,
                    &receipt,
                    DetailCodec::from_version(self.checkpoint.format_version).map_err(storage)?,
                )?;
                self.checkpoint.evidence.recovery_application_read_bytes = self
                    .checkpoint
                    .evidence
                    .recovery_application_read_bytes
                    .checked_add(recovery_work.bytes)
                    .ok_or_else(|| storage("recovery read byte count overflows"))?;
                self.checkpoint
                    .evidence
                    .recovery_application_read_operations = self
                    .checkpoint
                    .evidence
                    .recovery_application_read_operations
                    .checked_add(recovery_work.operations)
                    .ok_or_else(|| storage("recovery read operation count overflows"))?;
                let body = serde_json::to_vec(&receipt).map_err(storage)?;
                if intent.sequence < self.checkpoint.next_sequence
                    && self.checkpoint.last_receipt_sha256.as_deref()
                        != Some(sha256(&body).as_str())
                {
                    return Err(storage(
                        "completed intent receipt differs from checkpoint tail",
                    ));
                }
                let expected_pointer = ReceiptPointer {
                    operation_uuid: self.checkpoint.operation_uuid,
                    project_identity: self.checkpoint.project_identity.clone(),
                    session_identity: self.checkpoint.session_identity.clone(),
                    sequence: receipt.sequence,
                    receipt_sha256: sha256(&body),
                };
                match self.root.open_child_file(OsStr::new(&intent.chunk_key)) {
                    Ok(mut pointer_file) => {
                        let pointer: ReceiptPointer = decode_bounded(&mut pointer_file)?;
                        if pointer.operation_uuid != expected_pointer.operation_uuid
                            || pointer.project_identity != expected_pointer.project_identity
                            || pointer.session_identity != expected_pointer.session_identity
                            || pointer.sequence != expected_pointer.sequence
                            || pointer.receipt_sha256 != expected_pointer.receipt_sha256
                        {
                            return Err(storage("recovered receipt pointer is inconsistent"));
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        install_control(&self.root, &intent.chunk_key, &expected_pointer)?;
                    }
                    Err(error) => return Err(storage(error)),
                }
                self.advance_checkpoint(&receipt, &body)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                for artifact in [
                    intent.parquet.clone(),
                    intent.identities.clone(),
                    intent.endpoints.clone(),
                    intent.details.clone(),
                ]
                .into_iter()
                .flatten()
                {
                    let recovery_work = authenticate_artifact(
                        &self.root,
                        &artifact,
                        DetailCodec::from_version(self.checkpoint.format_version)
                            .map_err(storage)?,
                    )?;
                    account_cache_release(
                        recovery_work.cache_release,
                        &mut self.checkpoint.evidence,
                    )?;
                    self.checkpoint.evidence.recovery_application_read_bytes = self
                        .checkpoint
                        .evidence
                        .recovery_application_read_bytes
                        .checked_add(recovery_work.bytes)
                        .ok_or_else(|| storage("recovery read byte count overflows"))?;
                    self.checkpoint
                        .evidence
                        .recovery_application_read_operations = self
                        .checkpoint
                        .evidence
                        .recovery_application_read_operations
                        .checked_add(recovery_work.operations)
                        .ok_or_else(|| storage("recovery read operation count overflows"))?;
                    unlink_artifact(&self.root, &artifact)?;
                }
                let stem = artifact_stem(intent.sequence, intent.kind);
                if intent.parquet.is_none() {
                    remove_unrecorded_artifact(
                        &self.root,
                        &format!("{stem}.parquet"),
                        intent.kind,
                        intent.rows,
                        DetailCodec::from_version(self.checkpoint.format_version)
                            .map_err(storage)?,
                    )?;
                }
                if intent.identities.is_none() {
                    remove_unrecorded_artifact(
                        &self.root,
                        &format!("{stem}.identities.run"),
                        intent.kind,
                        intent.rows,
                        DetailCodec::from_version(self.checkpoint.format_version)
                            .map_err(storage)?,
                    )?;
                }
                if intent.kind == ConstructionChunkKind::Edge && intent.endpoints.is_none() {
                    remove_unrecorded_artifact(
                        &self.root,
                        &format!("{stem}.endpoints.run"),
                        intent.kind,
                        intent.rows,
                        DetailCodec::from_version(self.checkpoint.format_version)
                            .map_err(storage)?,
                    )?;
                }
                if intent.details.is_none() {
                    remove_unrecorded_artifact(
                        &self.root,
                        &format!(
                            "{stem}.{}-details.run",
                            if intent.kind == ConstructionChunkKind::Node {
                                "node"
                            } else {
                                "edge"
                            }
                        ),
                        intent.kind,
                        intent.rows,
                        DetailCodec::from_version(self.checkpoint.format_version)
                            .map_err(storage)?,
                    )?;
                }
            }
            Err(error) => return Err(storage(error)),
        }
        unlink_named(&self.root, INTENT)
    }
}

pub(super) fn remove_owned_directory_tree(
    parent: &StableDirectory,
    name: &OsStr,
    expected: FileIdentity,
    remaining: &mut u64,
) -> Result<(), GfError> {
    if *remaining == 0 {
        return Err(storage(
            "construction discard exceeded authenticated entry bound",
        ));
    }
    *remaining -= 1;
    let directory = parent.open_child_directory(name).map_err(storage)?;
    if directory.identity() != expected {
        return Err(storage("construction discard directory identity changed"));
    }
    let child_limit = usize::try_from(*remaining).unwrap_or(usize::MAX);
    let names = directory
        .child_names_bounded(child_limit)
        .map_err(storage)?;
    for child_name in names {
        if *remaining == 0 {
            return Err(storage(
                "construction discard exceeded authenticated entry bound",
            ));
        }
        *remaining -= 1;
        match directory.open_child_file(&child_name) {
            Ok(file) => {
                let identity = file_identity(&file).map_err(storage)?;
                drop(file);
                directory
                    .unlink_child_if_identity(&child_name, identity)
                    .map_err(storage)?;
            }
            Err(file_error) => {
                let child = directory
                    .open_child_directory(&child_name)
                    .map_err(|directory_error| {
                        storage(format!(
                            "construction discard child is neither an authenticated file nor directory: file={file_error}; directory={directory_error}"
                        ))
                    })?;
                let identity = child.identity();
                drop(child);
                remove_owned_directory_tree(&directory, &child_name, identity, remaining)?;
            }
        }
    }
    drop(directory);
    parent
        .remove_child_directory_if_identity(name, expected)
        .map_err(storage)
}

/// Every name reserved by the shaping path: the shaped domains, the staged
/// domains that feed surrogate assignment and endpoint resolution, and the
/// per-partition spills.
pub(super) fn is_shape_scoped_name(name: &str) -> bool {
    name.starts_with("shaped-") || name.starts_with("staged-") || name.starts_with("part-")
}

pub(super) fn reject_existing_merge_artifacts(root: &StableDirectory) -> Result<(), GfError> {
    for name in root.child_names().map_err(storage)? {
        let Some(name) = name.to_str() else { continue };
        if is_shape_scoped_name(name) {
            return Err(storage("unowned construction shaping artifact exists"));
        }
    }
    Ok(())
}

/// Returns the authentication work performed and whether the retained shape
/// output payloads were verified, so the session does not repeat that pass at
/// its next trust boundary (#1392).
pub(super) fn recover_shape_intent(
    root: &StableDirectory,
    checkpoint: &mut Checkpoint,
) -> Result<(ReadWork, bool), GfError> {
    let mut file = match root.open_child_file(OsStr::new(SHAPE_INTENT)) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok((ReadWork::default(), false));
        }
        Err(error) => return Err(storage(error)),
    };
    let intent: ShapeIntent = decode_shape_intent(&mut file)?;
    validate_shape_binding(&intent, checkpoint)?;
    if intent.complete {
        let shape = intent
            .shape
            .as_ref()
            .ok_or_else(|| storage("complete shape manifest lacks output"))?;
        if shape.ontology_mode != checkpoint.ontology_mode
            || shape.semantic_authority_sha256 != checkpoint.semantic_authority_sha256
            || shape.runtime_catalog_now_micros != checkpoint.session_now_micros
            || !is_canonical_sha256(&shape.runtime_catalog_inputs_sha256)
            || std::iter::once(&shape.identities)
                .chain(shape.node_details.iter())
                .chain(shape.edge_details.iter())
                .chain(shape.node_rows.iter())
                .chain(shape.edge_rows.iter())
                .chain(shape.edge_endpoints.iter())
                .chain(std::iter::once(&shape.runtime_catalog))
                .any(|name| !intent.outputs.iter().any(|item| item.name == *name))
        {
            return Err(storage("complete shape manifest inventory is incomplete"));
        }
        // Completed-shape replay is a trust boundary: the shape outputs are
        // about to be consumed by encoding, so their payloads are verified
        // here, not incidentally by a later retirement pass (#1392).
        let verified = checkpoint.encoding_inventory_sha256.is_none();
        let work = if verified {
            authenticate_completed_shape_outputs(root, &intent.outputs)?
        } else {
            ReadWork::default()
        };
        let expected_shape_authority = shape_authority_sha256(shape, &intent.outputs)?;
        if intent.shape_authority_sha256.as_deref() != Some(&expected_shape_authority) {
            return Err(storage(
                "complete shape authority digest differs from inventory",
            ));
        }
        let final_evidence = intent
            .final_evidence
            .as_ref()
            .ok_or_else(|| storage("complete shape manifest lacks final evidence"))?;
        recover_final_shape_evidence(
            root,
            checkpoint,
            &intent.baseline_evidence,
            final_evidence,
            expected_shape_authority,
        )?;
        // Charged after the completed shape's own evidence is restored, and
        // made durable by the supersession checkpoint the caller writes next,
        // exactly where this read work was charged before it moved to the
        // boundary. Returning it to the caller instead would cost an extra
        // checkpoint barrier on every reopen.
        checkpoint.evidence.recovery_application_read_bytes = checkpoint
            .evidence
            .recovery_application_read_bytes
            .checked_add(work.bytes)
            .ok_or_else(|| storage("shape recovery read bytes overflow"))?;
        checkpoint.evidence.recovery_application_read_operations = checkpoint
            .evidence
            .recovery_application_read_operations
            .checked_add(work.operations)
            .ok_or_else(|| storage("shape recovery read operations overflow"))?;
        account_cache_release(work.cache_release, &mut checkpoint.evidence)?;
        return Ok((ReadWork::default(), verified));
    }
    if intent.shape.is_some() || !intent.outputs.is_empty() {
        return Err(storage("incomplete shape intent claims completed output"));
    }
    if intent.final_evidence.is_some()
        || !persisted_evidence_equivalent(&checkpoint.evidence, &intent.baseline_evidence)
    {
        return Err(storage("incomplete shape changed committed evidence"));
    }
    let work = cleanup_incomplete_shape_capabilities(root)?;
    for child in root.child_names().map_err(storage)? {
        let Some(name) = child.to_str() else { continue };
        if !is_shape_scoped_name(name) {
            continue;
        }
        match root.open_child_file(OsStr::new(name)) {
            Ok(file) => {
                if file_link_count(&file).map_err(storage)? != 1 {
                    return Err(storage("construction shape artifact has extra links"));
                }
                drop(file);
                unlink_named(root, name)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(storage(error)),
        }
    }
    unlink_named(root, SHAPE_INTENT)?;
    Ok((work, false))
}

/// Stream and checksum every retained payload of a completed shape, returning
/// the read work performed. See [`authenticate_shaped_output`] for why this is
/// the boundary that owns the refusal (#1392).
fn authenticate_completed_shape_outputs(
    root: &StableDirectory,
    outputs: &[ArtifactReceipt],
) -> Result<ReadWork, GfError> {
    let mut work = ReadWork::default();
    for output in outputs {
        let observed = authenticate_shaped_output(root, output)?;
        work.bytes = work
            .bytes
            .checked_add(observed.bytes)
            .ok_or_else(|| storage("shape recovery authentication bytes overflow"))?;
        work.operations = work
            .operations
            .checked_add(observed.operations)
            .ok_or_else(|| storage("shape recovery authentication operations overflow"))?;
        merge_cache_release_evidence(&mut work.cache_release, observed.cache_release)?;
    }
    Ok(work)
}

fn recover_final_shape_evidence(
    root: &StableDirectory,
    checkpoint: &mut Checkpoint,
    baseline: &GraphConstructionEvidence,
    final_evidence: &GraphConstructionEvidence,
    expected_authority: String,
) -> Result<(), GfError> {
    if persisted_evidence_equivalent(&checkpoint.evidence, baseline)
        && checkpoint.shape_authority_sha256.is_none()
    {
        checkpoint.evidence = final_evidence.clone();
        checkpoint.shape_authority_sha256 = Some(expected_authority);
        return replace_checkpoint_control(root, checkpoint);
    }
    let mut observed = checkpoint.evidence.clone();
    copy_post_shape_io(&mut observed, final_evidence);
    if observed == *final_evidence
        && checkpoint.shape_authority_sha256.as_deref() == Some(&expected_authority)
    {
        return Ok(());
    }
    Err(storage("shape evidence authority differs from inventory"))
}

fn persisted_evidence_equivalent(
    left: &GraphConstructionEvidence,
    right: &GraphConstructionEvidence,
) -> bool {
    let mut left = left.clone();
    let mut right = right.clone();
    left.storage_allocation_transitions.clear();
    right.storage_allocation_transitions.clear();
    left == right
}

fn cleanup_incomplete_shape_capabilities(root: &StableDirectory) -> Result<ReadWork, GfError> {
    let mut work = ReadWork::default();
    // Authenticate every surviving derived payload before removing any recovery
    // authority. A writer receipt alone cannot detect in-place byte corruption.
    for child in root.child_names().map_err(storage)? {
        let Some(name) = child.to_str() else { continue };
        if !name.starts_with("shape-receipt-") {
            continue;
        }
        let mut file = root.open_child_file(OsStr::new(name)).map_err(storage)?;
        if file_link_count(&file).map_err(storage)? != 1 {
            return Err(storage("shaped writer capability has extra links"));
        }
        work.bytes = work
            .bytes
            .checked_add(file.metadata().map_err(storage)?.len())
            .ok_or_else(|| storage("shape recovery control bytes overflow"))?;
        work.operations = work
            .operations
            .checked_add(1)
            .ok_or_else(|| storage("shape recovery control reads overflow"))?;
        let receipt: ArtifactReceipt = decode_bounded(&mut file)?;
        if shape_receipt_name(&receipt.name) != name {
            return Err(storage("shaped writer capability name changed"));
        }
        if !is_shape_artifact_name(&receipt.name) {
            continue;
        }
        if !is_canonical_sha256(&receipt.sha256) {
            return Err(storage("shaped writer capability digest changed"));
        }
        match root.open_child_file(OsStr::new(&receipt.name)) {
            Ok(artifact) => {
                drop(artifact);
                let observed = supersession::authenticate_payload(root, &receipt, &mut || false)?;
                work.bytes = work
                    .bytes
                    .checked_add(observed.bytes)
                    .ok_or_else(|| storage("shape recovery read bytes overflow"))?;
                work.operations = work
                    .operations
                    .checked_add(observed.operations)
                    .ok_or_else(|| storage("shape recovery read operations overflow"))?;
                merge_cache_release_evidence(&mut work.cache_release, observed.cache_release)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(storage(error)),
        }
    }
    for child in root.child_names().map_err(storage)? {
        let Some(name) = child.to_str() else { continue };
        if !name.starts_with("shape-receipt-") {
            continue;
        }
        let mut file = root.open_child_file(OsStr::new(name)).map_err(storage)?;
        let identity = file_identity(&file).map_err(storage)?;
        if file_link_count(&file).map_err(storage)? != 1 {
            return Err(storage("shaped writer capability has extra links"));
        }
        work.bytes = work
            .bytes
            .checked_add(file.metadata().map_err(storage)?.len())
            .ok_or_else(|| storage("shape recovery control bytes overflow"))?;
        work.operations = work
            .operations
            .checked_add(1)
            .ok_or_else(|| storage("shape recovery control reads overflow"))?;
        let receipt: ArtifactReceipt = decode_bounded(&mut file)?;
        if shape_receipt_name(&receipt.name) != name {
            return Err(storage("shaped writer capability name changed"));
        }
        if !is_shape_artifact_name(&receipt.name) {
            continue;
        }
        drop(file);
        root.unlink_child_if_identity(OsStr::new(name), identity)
            .map_err(storage)?;
        root.sync().map_err(storage)?;
    }
    Ok(work)
}

pub(super) fn receipt_for_existing(
    root: &StableDirectory,
    name: &str,
) -> Result<ArtifactReceipt, GfError> {
    receipt_for_existing_with_work(root, name).map(|(receipt, _)| receipt)
}

pub(super) fn receipt_for_existing_with_work(
    root: &StableDirectory,
    name: &str,
) -> Result<(ArtifactReceipt, ReadWork), GfError> {
    let capability_name = shape_receipt_name(name);
    if let Ok(mut capability_file) = root.open_child_file(OsStr::new(&capability_name)) {
        let control_bytes = capability_file.metadata().map_err(storage)?.len();
        let receipt: ArtifactReceipt = decode_bounded(&mut capability_file)?;
        if receipt.name != name {
            return Err(storage("shaped writer capability names another artifact"));
        }
        let file = root.open_child_file(OsStr::new(name)).map_err(storage)?;
        if file_link_count(&file).map_err(storage)? != 1
            || !receipt
                .identity
                .matches(file_identity(&file).map_err(storage)?)
            || file.metadata().map_err(storage)?.len() != receipt.bytes
        {
            return Err(storage("shaped writer capability identity changed"));
        }
        return Ok((
            receipt,
            ReadWork {
                bytes: control_bytes,
                operations: 1,
                ..Default::default()
            },
        ));
    }
    let file = root.open_child_file(OsStr::new(name)).map_err(storage)?;
    if file_link_count(&file).map_err(storage)? != 1 {
        return Err(storage("shaped output has extra links"));
    }
    let identity = file_identity(&file).map_err(storage)?;
    let mut file = graphforge_filesystem::FileCacheReleasingReader::new(file).map_err(storage)?;
    let mut digest = Sha256::new();
    let mut checksum = crate::corruption_checksum::Checksum::new();
    let mut bytes = 0_u64;
    let mut operations = 0_u64;
    let mut block = vec![0_u8; BLOCK_BYTES];
    let authenticated = (|| -> Result<(ArtifactReceipt, ReadWork), GfError> {
        loop {
            let count = file.read(&mut block).map_err(storage)?;
            if count == 0 {
                break;
            }
            digest.update(&block[..count]);
            checksum.update(&block[..count]);
            bytes = bytes
                .checked_add(count as u64)
                .ok_or_else(|| storage("bytes overflows"))?;
            operations = operations
                .checked_add(1)
                .ok_or_else(|| storage("operations overflows"))?;
        }
        Ok((
            ArtifactReceipt {
                name: name.to_owned(),
                bytes,
                allocated_bytes: graphforge_filesystem::file_space_usage(file.file())
                    .map_err(storage)?
                    .allocated_bytes,
                sha256: hex(&digest.finalize()),
                xxh64: crate::corruption_checksum::hex(checksum.finish()),
                identity: identity.into(),
                write_operations: 0,
                fsync_operations: 0,
            },
            ReadWork {
                bytes,
                operations,
                ..Default::default()
            },
        ))
    })();
    let released = file.finish().map_err(storage);
    match (authenticated, released) {
        (Ok((receipt, mut work)), Ok(cache_release)) => {
            work.cache_release = cache_release;
            Ok((receipt, work))
        }
        (Ok(_), Err(release)) => Err(release),
        (Err(primary), Ok(_)) => Err(primary),
        (Err(primary), Err(release)) => Err(storage(format!(
            "{primary}; shaped artifact cache release also failed: {release}"
        ))),
    }
}

pub(super) fn unlink_writer_capability(
    root: &StableDirectory,
    name: &str,
    expected: Option<&ArtifactReceipt>,
) -> Result<(), GfError> {
    let capability_name = shape_receipt_name(name);
    let mut file = match root.open_child_file(OsStr::new(&capability_name)) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(storage(error)),
    };
    if file_link_count(&file).map_err(storage)? != 1 {
        return Err(storage("writer capability has extra links"));
    }
    let identity = file_identity(&file).map_err(storage)?;
    let receipt: ArtifactReceipt = decode_bounded(&mut file)?;
    if receipt.name != name
        || shape_receipt_name(&receipt.name) != capability_name
        || expected.is_some_and(|expected| expected != &receipt)
    {
        return Err(storage("writer capability differs from artifact authority"));
    }
    // This path removes the artifact; its bytes are never consumed again, so
    // the identity-only authority check is the right one here.
    authenticate_shaped_output_identity(root, &receipt)?;
    drop(file);
    root.unlink_child_if_identity(OsStr::new(&capability_name), identity)
        .map_err(storage)?;
    root.sync().map_err(storage)
}

pub(super) fn unlink_shape_artifact(
    root: &StableDirectory,
    name: &str,
    evidence: &mut GraphConstructionEvidence,
) -> Result<(), GfError> {
    let receipt = receipt_for_existing(root, name)?;
    unlink_artifact(root, &receipt)?;
    construction_failpoint("shape.after_derived_unlink");
    let identity_key = format!(
        "{:016x}:{}",
        receipt.identity.volume_serial, receipt.identity.file_id
    );
    let removed = record_active_identity_remove(evidence, &identity_key).map_err(|_| {
        storage(format!(
            "shape active identity ledger is absent for {name} ({identity_key})"
        ))
    })?;
    if removed != receipt.allocated_bytes {
        return Err(storage("shape active identity allocation changed"));
    }
    let category = crate::ArtifactCategory::ConstructionStaging;
    let reported = checked_category_remove(
        evidence
            .storage_current
            .get(&category)
            .ok_or_else(|| storage("shape allocation ledger is absent"))?,
        receipt.bytes,
        receipt.allocated_bytes,
    )?;
    let authority = checked_category_remove(
        evidence
            .storage_receipt_category_authorities
            .get(&category)
            .ok_or_else(|| storage("shape authority ledger is absent"))?,
        receipt.bytes,
        receipt.allocated_bytes,
    )?;
    evidence.storage_current.insert(category, reported);
    evidence
        .storage_receipt_category_authorities
        .insert(category, authority);
    evidence.current_merge_temporary_allocated_bytes = evidence
        .current_merge_temporary_allocated_bytes
        .checked_sub(receipt.allocated_bytes)
        .ok_or_else(|| storage("shape active-allocation ledger underflow"))?;
    Ok(())
}

pub(super) fn cleanup_failed_shape_output(
    mut writer: BufWriter<HashingWriter>,
    publication: &mut graphforge_filesystem::UnpublishedArtifactGuard,
    evidence: &mut GraphConstructionEvidence,
) -> Result<(), GfError> {
    let flushed = writer.flush().map_err(storage);
    let synchronized = writer
        .get_mut()
        .inner
        .sync_all_and_release()
        .map_err(storage);
    let cache_release = writer.get_ref().inner.evidence();
    account_cache_release(cache_release, evidence)?;
    let finalized =
        combine_secondary_cleanup(flushed, synchronized, "shape output synchronization");
    drop(writer);
    let guard_cleanup = cleanup_shape_publication(publication);
    combine_secondary_cleanup(finalized, guard_cleanup, "shape output removal")
}

pub(super) fn cleanup_shape_publication(
    publication: &mut graphforge_filesystem::UnpublishedArtifactGuard,
) -> Result<(), GfError> {
    publication
        .cleanup_checked(|| {
            take_shape_output_cleanup_failure()
                .map_err(|_| std::io::Error::other("injected shape output cleanup failure"))
        })
        .map_err(storage)
}

#[cfg(test)]
#[derive(Default)]
pub(super) struct ShapeCleanupFailures {
    pub(super) input_release: bool,
    output_cleanup: bool,
    publication_failure: Option<&'static str>,
}

#[cfg(test)]
thread_local! {
    pub(super) static SHAPE_CLEANUP_FAILURES: std::cell::RefCell<ShapeCleanupFailures> =
        std::cell::RefCell::new(ShapeCleanupFailures::default());
}

#[cfg(test)]
fn inject_shape_cleanup_failures(input_release: bool, output_cleanup: bool) {
    SHAPE_CLEANUP_FAILURES.with(|failures| {
        *failures.borrow_mut() = ShapeCleanupFailures {
            input_release,
            output_cleanup,
            publication_failure: None,
        };
    });
}

#[cfg(test)]
pub(super) fn inject_shape_publication_failure(point: &'static str) {
    SHAPE_CLEANUP_FAILURES.with(|failures| failures.borrow_mut().publication_failure = Some(point));
}

#[allow(clippy::unnecessary_wraps)]
pub(super) fn shape_publication_failure(point: &str) -> Result<(), GfError> {
    #[cfg(test)]
    SHAPE_CLEANUP_FAILURES.with(|failures| {
        if failures
            .borrow()
            .publication_failure
            .is_some_and(|configured| configured == point)
        {
            failures.borrow_mut().publication_failure.take();
            return Err(storage(format!(
                "injected shape publication failure at {point}"
            )));
        }
        Ok(())
    })?;
    #[cfg(not(test))]
    let _ = point;
    Ok(())
}

pub(super) fn shape_publication_io_failure(point: &str) -> std::io::Result<()> {
    shape_publication_failure(point).map_err(|_| {
        std::io::Error::other(format!("injected shape publication failure at {point}"))
    })
}

#[allow(clippy::unnecessary_wraps)]
fn take_shape_output_cleanup_failure() -> Result<(), GfError> {
    #[cfg(test)]
    if SHAPE_CLEANUP_FAILURES.with(|failures| {
        let output_cleanup = failures.borrow().output_cleanup;
        failures.borrow_mut().output_cleanup = false;
        output_cleanup
    }) {
        return Err(storage("injected shape output cleanup failure"));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct ReadWork {
    pub(super) detail_records: u64,
    pub(super) bytes: u64,
    pub(super) operations: u64,
    pub(super) cache_release: graphforge_filesystem::FileCacheReleaseEvidence,
}

pub(super) fn authenticate_artifact(
    root: &StableDirectory,
    receipt: &ArtifactReceipt,
    codec: DetailCodec,
) -> Result<ReadWork, GfError> {
    let _diagnostic_scope =
        crate::graph_construction::diagnostics::Scope::start("artifact_authentication");
    validate_artifact_name(receipt)?;
    authenticate_artifact_contents(root, receipt, codec)
}

/// Authenticate an internally generated, sealed row spill before IPC decoding.
/// These temporary spills are not published construction artifacts.
pub(super) fn authenticate_row_spill(
    root: &StableDirectory,
    receipt: &ArtifactReceipt,
) -> Result<ReadWork, GfError> {
    if !receipt.name.starts_with("part-rows-")
        || std::path::Path::new(&receipt.name).extension() != Some(OsStr::new("arrow"))
        || receipt.name.contains('/')
        || receipt.name.contains('\\')
        || !is_canonical_sha256(&receipt.sha256)
    {
        return Err(storage("invalid row partition spill receipt"));
    }
    authenticate_artifact_contents(root, receipt, DetailCodec::Compact)
}

#[allow(clippy::too_many_lines)] // One authentication pass validates format, digest, and cache cleanup together.
fn authenticate_artifact_contents(
    root: &StableDirectory,
    receipt: &ArtifactReceipt,
    codec: DetailCodec,
) -> Result<ReadWork, GfError> {
    let file = root
        .open_child_file(OsStr::new(&receipt.name))
        .map_err(storage)?;
    if !receipt
        .identity
        .matches(file_identity(&file).map_err(storage)?)
        || file_link_count(&file).map_err(storage)? != 1
    {
        return Err(storage("artifact identity or link count changed"));
    }
    let releasing = graphforge_filesystem::FileCacheReleasingReader::new(file).map_err(storage)?;
    let mut reader = BufReader::with_capacity(BLOCK_BYTES, releasing);
    let result = (|| -> Result<(u64, u64, u64), GfError> {
        let mut block = vec![0_u8; BLOCK_BYTES];
        let mut digest = Sha256::new();
        let mut bytes = 0_u64;
        let mut operations = 0_u64;
        let width = if receipt.name.ends_with(".identities.run") {
            Some(IDENTITY_WIDTH)
        } else if receipt.name.ends_with(".endpoints.run") {
            Some(ENDPOINT_WIDTH)
        } else if receipt.name.ends_with(".node-details.run") {
            Some(NODE_DETAIL_WIDTH)
        } else if receipt.name.ends_with(".edge-details.run") {
            Some(EDGE_DETAIL_WIDTH)
        } else {
            None
        };
        let mut detail = width
            .filter(|width| matches!(*width, NODE_DETAIL_WIDTH | EDGE_DETAIL_WIDTH))
            .map(|width| DetailValidator::new(codec, width))
            .transpose()
            .map_err(storage)?;
        let mut pending = Vec::new();
        let mut previous: Option<Vec<u8>> = None;
        loop {
            let count = reader.read(&mut block).map_err(storage)?;
            if count == 0 {
                break;
            }
            digest.update(&block[..count]);
            bytes = bytes
                .checked_add(count as u64)
                .ok_or_else(|| storage("bytes overflows"))?;
            operations = operations
                .checked_add(1)
                .ok_or_else(|| storage("operations overflows"))?;
            if let Some(detail) = detail.as_mut() {
                detail.consume(&block[..count]).map_err(storage)?;
            } else if let Some(width) = width {
                pending.extend_from_slice(&block[..count]);
                let complete = pending.len() / width * width;
                for record in pending[..complete].chunks_exact(width) {
                    if previous
                        .as_ref()
                        .is_some_and(|prior| prior.as_slice() >= record)
                    {
                        return Err(storage("fixed construction run is not strictly sorted"));
                    }
                    if width == ENDPOINT_WIDTH && (!matches!(record[32], 0 | 1)) {
                        return Err(storage("endpoint run record is malformed"));
                    }
                    if width == EDGE_DETAIL_WIDTH {
                        let route_len = record[48] as usize;
                        if route_len == 0 || record[49 + route_len..].iter().any(|byte| *byte != 0)
                        {
                            return Err(storage("edge detail run record is malformed"));
                        }
                    }
                    if width == NODE_DETAIL_WIDTH {
                        let label_len = record[16] as usize;
                        if label_len == 0 || record[17 + label_len..].iter().any(|byte| *byte != 0)
                        {
                            return Err(storage("node detail run record is malformed"));
                        }
                    }
                    if width == BASE_IDENTITY_WIDTH
                        && (!matches!(record[16], 0 | 1)
                            || record[17] != 1
                            || (record[16] == 0
                                && record[IDENTITY_SURROGATE_OFFSET..]
                                    .iter()
                                    .all(|byte| *byte == 0))
                            || (record[16] == 1
                                && record[IDENTITY_SURROGATE_OFFSET..]
                                    .iter()
                                    .any(|byte| *byte != 0)))
                    {
                        return Err(storage("base identity run record is malformed"));
                    }
                    previous = Some(record.to_vec());
                }
                pending.drain(..complete);
            }
        }
        if !pending.is_empty() {
            return Err(storage("fixed construction run has a truncated tail"));
        }
        if bytes != receipt.bytes || hex(&digest.finalize()) != receipt.sha256 {
            return Err(storage("artifact digest or size changed"));
        }
        let records = detail
            .as_ref()
            .map(DetailValidator::finish)
            .transpose()
            .map_err(storage)?
            .unwrap_or(0);
        Ok((bytes, operations, records))
    })();
    let release = reader.get_mut().finish().map_err(storage);
    let (bytes, operations, detail_records) = match (result, release) {
        (Ok(value), Ok(_)) => value,
        (Ok(_), Err(error)) => return Err(error),
        (Err(primary), Ok(_)) => return Err(primary),
        (Err(primary), Err(release)) => {
            return Err(storage(format!(
                "{primary}; cache release after failed authentication also failed: {release}"
            )));
        }
    };
    let cache_release = reader.get_ref().tracker().evidence();
    Ok(ReadWork {
        detail_records,
        bytes,
        operations,
        cache_release,
    })
}

pub(super) fn cleanup_authenticated_control_temps(
    root: &StableDirectory,
    operation: Uuid,
    project: FileIdentity,
    session: FileIdentity,
    format_version: u32,
) -> Result<(), GfError> {
    for name in root.child_names().map_err(storage)? {
        let Some(text) = name.to_str() else { continue };
        if !is_control_temp(text) {
            continue;
        }
        let mut file = root.open_child_file(&name).map_err(storage)?;
        let body = read_bounded_limit(&mut file, MAX_SHAPE_CONTROL_BYTES)?;
        let authenticated = serde_json::from_slice::<Checkpoint>(&body).is_ok_and(|value| {
            value.operation_uuid == operation
                && value.project_identity.matches(project)
                && value.session_identity.matches(session)
        }) || serde_json::from_slice::<ChunkIntent>(&body).is_ok_and(|value| {
            value.operation_uuid == operation
                && value.project_identity.matches(project)
                && value.session_identity.matches(session)
        }) || serde_json::from_slice::<ConstructionChunkReceipt>(&body)
            .is_ok_and(|value| {
                value.operation_uuid == operation
                    && value.project_identity.matches(project)
                    && value.session_identity.matches(session)
            })
            || serde_json::from_slice::<ReceiptPointer>(&body).is_ok_and(|value| {
                value.operation_uuid == operation
                    && value.project_identity.matches(project)
                    && value.session_identity.matches(session)
            })
            || serde_json::from_slice::<ShapeIntent>(&body).is_ok_and(|value| {
                value.operation_uuid == operation
                    && value.project_identity.matches(project)
                    && value.session_identity.matches(session)
            })
            || serde_json::from_slice::<ConstructionPublicationIntent>(&body).is_ok_and(|value| {
                value.operation_uuid == operation
                    && value.project_identity.matches(project)
                    && value.session_identity.matches(session)
            })
            || serde_json::from_slice::<ConstructionPublicationReceipt>(&body).is_ok_and(|value| {
                value.operation_uuid == operation
                    && value.project_identity.matches(project)
                    && value.session_identity.matches(session)
            });
        if authenticated {
            // A complete intent can also deserialize as a versionless receipt.
            // Pin its version before that structural overlap can authorize cleanup.
            let control: serde_json::Value = serde_json::from_slice(&body).map_err(storage)?;
            if let Some(version) = control.get("format_version")
                && version.as_u64() != Some(u64::from(format_version))
            {
                return Err(storage("temporary control version differs from session"));
            }
        }
        if authenticated && file_link_count(&file).map_err(storage)? == 1 {
            let identity = file_identity(&file).map_err(storage)?;
            drop(file);
            root.unlink_child_if_identity(&name, identity)
                .map_err(storage)?;
            root.sync().map_err(storage)?;
        }
    }
    Ok(())
}

pub(super) fn is_control_temp(name: &str) -> bool {
    let Some((prefix, suffix)) = name.rsplit_once('.') else {
        return false;
    };
    suffix == "tmp"
        && prefix.starts_with('.')
        && prefix.rsplit_once('-').is_some_and(|(_, random)| {
            random.len() == 32 && random.bytes().all(|b| b.is_ascii_hexdigit())
        })
}

pub(super) fn cleanup_owned_artifact_temps(root: &StableDirectory) -> Result<(), GfError> {
    for name in root.child_names().map_err(storage)? {
        let Some(text) = name.to_str() else { continue };
        if !is_owned_artifact_temp(text) {
            continue;
        }
        let file = root.open_child_file(&name).map_err(storage)?;
        if file_link_count(&file).map_err(storage)? != 1 {
            return Err(storage("incomplete artifact temp has unexpected links"));
        }
        let identity = file_identity(&file).map_err(storage)?;
        drop(file);
        root.unlink_child_if_identity(&name, identity)
            .map_err(storage)?;
        root.sync().map_err(storage)?;
    }
    Ok(())
}

pub(super) fn is_owned_artifact_temp(name: &str) -> bool {
    let Some(body) = name
        .strip_prefix(".artifact-")
        .and_then(|value| value.strip_suffix(".tmp"))
    else {
        return false;
    };
    let Some((target, random)) = body.rsplit_once('-') else {
        return false;
    };
    random.len() == 32
        && random.bytes().all(|byte| byte.is_ascii_hexdigit())
        && canonical_artifact_target(target)
}

pub(super) fn canonical_artifact_target(name: &str) -> bool {
    if is_shape_artifact_name(name) {
        return true;
    }
    let Some(body) = name.strip_prefix("chunk-") else {
        return false;
    };
    let Some((sequence, tail)) = body.split_once('-') else {
        return false;
    };
    sequence.len() == 20
        && sequence.bytes().all(|byte| byte.is_ascii_digit())
        && matches!(
            tail,
            "node.parquet"
                | "node.identities.run"
                | "edge.parquet"
                | "edge.identities.run"
                | "edge.endpoints.run"
                | "node.node-details.run"
                | "edge.edge-details.run"
        )
}

pub(super) fn unlink_named(root: &StableDirectory, name: &str) -> Result<(), GfError> {
    let file = root.open_child_file(OsStr::new(name)).map_err(storage)?;
    if file_link_count(&file).map_err(storage)? != 1 {
        return Err(storage("control record link count changed"));
    }
    let identity = file_identity(&file).map_err(storage)?;
    drop(file);
    root.unlink_child_if_identity(OsStr::new(name), identity)
        .map_err(storage)?;
    root.sync().map_err(storage)
}

fn unlink_artifact(root: &StableDirectory, receipt: &ArtifactReceipt) -> Result<(), GfError> {
    let file = root
        .open_child_file(OsStr::new(&receipt.name))
        .map_err(storage)?;
    let identity = file_identity(&file).map_err(storage)?;
    if !receipt.identity.matches(identity) || file_link_count(&file).map_err(storage)? != 1 {
        return Err(storage("orphan artifact identity changed"));
    }
    unlink_writer_capability(root, &receipt.name, Some(receipt))?;
    drop(file);
    root.unlink_child_if_identity(OsStr::new(&receipt.name), identity)
        .map_err(storage)?;
    root.sync().map_err(storage)
}

fn remove_unrecorded_artifact(
    root: &StableDirectory,
    name: &str,
    kind: ConstructionChunkKind,
    rows: u64,
    codec: DetailCodec,
) -> Result<(), GfError> {
    let file = match root.open_child_file(OsStr::new(name)) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(storage(error)),
    };
    let identity = file_identity(&file).map_err(storage)?;
    if file_link_count(&file).map_err(storage)? != 1 {
        return Err(storage("unrecorded artifact has unexpected links"));
    }
    if name.ends_with(".parquet") {
        let chunk_reader = CountingChunkReader::new(file, IoCounter::default());
        let cache_release = chunk_reader.cache_release_tracker();
        let validated = (|| -> Result<(), GfError> {
            let builder =
                ParquetRecordBatchReaderBuilder::try_new(chunk_reader).map_err(storage)?;
            let expected = match kind {
                ConstructionChunkKind::Node => &*CONSTRUCTION_NODE_SCHEMA,
                ConstructionChunkKind::Edge => &*CONSTRUCTION_EDGE_SCHEMA,
            };
            if builder.schema().fields().len() < expected.fields().len()
                || builder.schema().fields()[..expected.fields().len()] != expected.fields()[..]
                || builder.metadata().file_metadata().num_rows()
                    != i64::try_from(rows).map_err(|_| storage("artifact row count exceeds i64"))?
            {
                return Err(storage("unrecorded Parquet artifact is not session-owned"));
            }
            Ok(())
        })();
        combine_cache_cleanup(
            validated,
            cache_release.check_error().map_err(storage),
            "unrecorded Parquet artifact",
        )?;
    } else {
        let width = if name.ends_with(".identities.run") {
            IDENTITY_WIDTH
        } else if name.ends_with(".endpoints.run") {
            ENDPOINT_WIDTH
        } else if name.ends_with(".node-details.run") {
            NODE_DETAIL_WIDTH
        } else if name.ends_with(".edge-details.run") {
            EDGE_DETAIL_WIDTH
        } else {
            return Err(storage("unrecorded artifact name is not canonical"));
        };
        let expected_records = if width == ENDPOINT_WIDTH {
            rows.checked_mul(2)
                .ok_or_else(|| storage("unrecorded endpoint count overflow"))?
        } else {
            rows
        };
        let expected_bytes = expected_records
            .checked_mul(u64::try_from(width).map_err(storage)?)
            .ok_or_else(|| storage("unrecorded fixed run byte count overflow"))?;
        if !(codec == DetailCodec::Compact
            && matches!(width, NODE_DETAIL_WIDTH | EDGE_DETAIL_WIDTH))
            && file.metadata().map_err(storage)?.len() != expected_bytes
        {
            return Err(storage("unrecorded fixed run row count changed"));
        }
        validate_sorted_run(file, width, codec, expected_records)?;
    }
    unlink_writer_capability(root, name, None)?;
    root.unlink_child_if_identity(OsStr::new(name), identity)
        .map_err(storage)?;
    root.sync().map_err(storage)
}

#[cfg(test)]
mod tests;
