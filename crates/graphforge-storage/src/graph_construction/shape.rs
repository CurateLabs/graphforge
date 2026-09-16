//! Shape for graph construction.

use super::{
    ArtifactReceipt, AuthenticatedShapeSource, AuthenticatedUuidIndexSnapshot, BASE_IDENTITY_WIDTH,
    BLOCK_BYTES, BTreeMap, BufRead, BufReader, BufWriter, Checkpoint, ConstructionChunkKind,
    ConstructionPublicationState, ConstructionShape, DetailCodec, DetailValidator, Digest,
    EDGE_DETAIL_WIDTH, ENDPOINT_WIDTH, File, FixedMergeAccumulator, GfError,
    GraphConstructionEvidence, GraphConstructionSession, GraphConstructionState, HashingWriter,
    IDENTITY_SURROGATE_OFFSET, NODE_DETAIL_WIDTH, OsStr, RESOLVED_ENDPOINT_WIDTH,
    RESOLVED_SURROGATE_OFFSET, Read, RowMergeAccumulator, SHAPE_INTENT, Sha256, ShapeIntent,
    StableDirectory, Uuid, UuidIndexKind, Write, account_cache_release,
    account_fixed_read_operations, account_fixed_write_operations, account_merge_read,
    account_merge_write, account_probe_work, account_sequential_write, artifact_temp,
    authenticate_artifact, build_runtime_catalog, canonical_artifact_target, checked_evidence_sum,
    combine_cache_cleanup, compact_parent_surrogate_tails, construction_failpoint,
    convert_identity_run, copy_authenticated_run, copy_authenticated_run_with_codec,
    decode_bounded, decode_shape_intent, file_identity, file_link_count, hex, install_control,
    is_canonical_lower_hex, is_canonical_sha256, open_counted_fixed_reader, receipt_for_existing,
    receipt_for_existing_with_work, record_shape_artifact_install, reject_cancelled,
    reject_existing_merge_artifacts, release_counted_reader_cache, replace_checkpoint_control,
    replace_control, sha256, shape_authority_sha256, shape_publication_failure, storage,
    unlink_shape_artifact, validate_parquet_metadata, write_fixed_run,
};

impl GraphConstructionSession {
    /// Validate the sealed identity domains and produce deterministic,
    /// UUID-sorted canonical construction runs.  This is deliberately still
    /// private staging: the generation-last publisher owns Parquet and CURRENT.
    #[allow(clippy::too_many_lines)] // One authenticated external-shape lifecycle; ordering is the invariant.
    pub fn shape_canonical_with_cancellation(
        &mut self,
        cancelled: impl FnMut() -> bool,
    ) -> Result<ConstructionShape, GfError> {
        self.shape_canonical_inner(cancelled, true)
    }

    #[allow(clippy::too_many_lines)] // One authenticated external-shape lifecycle; ordering is the invariant.
    pub(super) fn shape_canonical_inner(
        &mut self,
        mut cancelled: impl FnMut() -> bool,
        authenticate_completed_outputs: bool,
    ) -> Result<ConstructionShape, GfError> {
        #[cfg(any(test, feature = "test-support"))]
        let _diagnostic_scope = crate::graph_construction::diagnostics::Scope::start("shaping");
        self.revalidate_authority()?;
        if self.checkpoint.state != GraphConstructionState::Sealed
            || self.checkpoint.publication_state != Some(ConstructionPublicationState::Sealed)
        {
            return Err(storage("only a sealed session can be shaped"));
        }
        reject_cancelled(&mut cancelled)?;
        if let Some(shape) = read_completed_shape(
            &self.root,
            &self.checkpoint,
            authenticate_completed_outputs && !self.has_encoding_successor(),
        )? {
            self.reclaim_superseded_payloads_cancellable(&mut cancelled)?;
            return Ok(shape);
        }
        reject_existing_merge_artifacts(&self.root)?;

        let fan_in = self.checkpoint.budgets.merge_fan_in;
        let mut unified = FixedMergeAccumulator::new("merge-identities", fan_in, true);
        let detail_codec =
            DetailCodec::from_version(self.checkpoint.format_version).map_err(storage)?;
        let mut node_details = FixedMergeAccumulator::new("merge-node-details", fan_in, true)
            .with_detail_codec(detail_codec);
        let mut edge_details = FixedMergeAccumulator::new("merge-edge-details", fan_in, true)
            .with_detail_codec(detail_codec);
        let mut endpoints = FixedMergeAccumulator::new("merge-endpoints", fan_in, false);
        let mut row_groups: BTreeMap<(u8, String), RowMergeAccumulator> = BTreeMap::new();
        let mut catalog_authority = Sha256::new();
        let shape_intent = ShapeIntent {
            format_version: self.checkpoint.format_version,
            operation_uuid: self.checkpoint.operation_uuid,
            project_identity: self.checkpoint.project_identity.clone(),
            session_identity: self.checkpoint.session_identity.clone(),
            parent_topology_generation: self.checkpoint.parent_topology_generation,
            ontology_mode: self.checkpoint.ontology_mode,
            semantic_authority_sha256: self.checkpoint.semantic_authority_sha256.clone(),
            budgets: self.checkpoint.budgets,
            last_receipt_sha256: self.checkpoint.last_receipt_sha256.clone(),
            baseline_evidence: self.checkpoint.evidence.clone(),
            final_evidence: None,
            complete: false,
            shape: None,
            outputs: Vec::new(),
            shape_authority_sha256: None,
        };
        install_control(&self.root, SHAPE_INTENT, &shape_intent)?;
        for sequence in 0..self.checkpoint.next_sequence {
            reject_cancelled(&mut cancelled)?;
            let receipt = self.read_receipt(sequence)?;
            // Fixed-width inputs authenticate their exact inode, length and
            // digest in the merge consumers below. Parquet's range-oriented
            // decoder cannot establish a whole-file digest, so retain exactly
            // one explicit whole-file authentication pass for that artifact.
            let mut work = authenticate_artifact(
                &self.root,
                &receipt.parquet,
                DetailCodec::from_version(self.checkpoint.format_version).map_err(storage)?,
            )?;
            account_cache_release(work.cache_release, &mut self.checkpoint.evidence)?;
            let metadata_work = validate_parquet_metadata(&self.root, &receipt)?;
            account_cache_release(metadata_work.cache_release, &mut self.checkpoint.evidence)?;
            work.bytes = work
                .bytes
                .checked_add(metadata_work.bytes)
                .ok_or_else(|| storage("bytes overflows"))?;
            work.operations = work
                .operations
                .checked_add(metadata_work.operations)
                .ok_or_else(|| storage("operations overflows"))?;
            self.checkpoint.evidence.shape_input_validation_read_bytes = self
                .checkpoint
                .evidence
                .shape_input_validation_read_bytes
                .checked_add(work.bytes)
                .ok_or_else(|| storage("shape input validation byte count overflows"))?;
            self.checkpoint
                .evidence
                .shape_input_validation_read_operations = self
                .checkpoint
                .evidence
                .shape_input_validation_read_operations
                .checked_add(work.operations)
                .ok_or_else(|| storage("shape input validation operation count overflows"))?;
            let kind = u8::from(receipt.kind == ConstructionChunkKind::Edge);
            catalog_authority.update([kind]);
            catalog_authority.update(receipt.schema_sha256.as_bytes());
            catalog_authority.update(receipt.parquet.sha256.as_bytes());
            row_groups
                .entry((kind, receipt.schema_sha256.clone()))
                .or_insert_with(|| {
                    RowMergeAccumulator::new(fan_in, &format!("{kind}-{}", receipt.schema_sha256))
                })
                .push(
                    &self.root,
                    receipt.parquet.name.clone(),
                    self.checkpoint.budgets.max_batch_rows,
                    self.checkpoint.budgets.max_batch_bytes,
                    &mut cancelled,
                    &mut self.checkpoint.evidence,
                )?;
            let name = format!("merge-unified-{sequence:020}.run");
            convert_identity_run(
                &self.root,
                &receipt,
                &name,
                &mut cancelled,
                &mut self.checkpoint.evidence,
            )?;
            unified.push::<BASE_IDENTITY_WIDTH>(
                &self.root,
                name,
                &mut cancelled,
                &mut self.checkpoint.evidence,
            )?;
            match receipt.kind {
                ConstructionChunkKind::Node => {
                    let name = format!("merge-node-source-{sequence:020}.run");
                    copy_authenticated_run_with_codec::<NODE_DETAIL_WIDTH>(
                        &self.root,
                        &receipt.details,
                        &name,
                        &mut cancelled,
                        &mut self.checkpoint.evidence,
                        Some(detail_codec),
                    )?;
                    node_details.push::<NODE_DETAIL_WIDTH>(
                        &self.root,
                        name,
                        &mut cancelled,
                        &mut self.checkpoint.evidence,
                    )?;
                }
                ConstructionChunkKind::Edge => {
                    let detail = format!("merge-edge-source-{sequence:020}.run");
                    copy_authenticated_run_with_codec::<EDGE_DETAIL_WIDTH>(
                        &self.root,
                        &receipt.details,
                        &detail,
                        &mut cancelled,
                        &mut self.checkpoint.evidence,
                        Some(detail_codec),
                    )?;
                    edge_details.push::<EDGE_DETAIL_WIDTH>(
                        &self.root,
                        detail,
                        &mut cancelled,
                        &mut self.checkpoint.evidence,
                    )?;
                    let endpoint = format!("merge-endpoint-source-{sequence:020}.run");
                    copy_authenticated_run::<ENDPOINT_WIDTH>(
                        &self.root,
                        receipt
                            .endpoints
                            .as_ref()
                            .ok_or_else(|| storage("edge receipt lacks endpoint run"))?,
                        &endpoint,
                        &mut cancelled,
                        &mut self.checkpoint.evidence,
                    )?;
                    endpoints.push::<ENDPOINT_WIDTH>(
                        &self.root,
                        endpoint,
                        &mut cancelled,
                        &mut self.checkpoint.evidence,
                    )?;
                }
            }
            let row_group_names = row_groups
                .values()
                .try_fold(0_usize, |count, group| {
                    count.checked_add(group.slot_count())
                })
                .ok_or_else(|| storage("row-group name-slot count overflow"))?;
            let retained_names = unified
                .slot_count()
                .checked_add(node_details.slot_count())
                .and_then(|count| count.checked_add(edge_details.slot_count()))
                .and_then(|count| count.checked_add(endpoints.slot_count()))
                .and_then(|count| count.checked_add(row_group_names))
                .ok_or_else(|| storage("merge name-slot count overflow"))?;
            self.checkpoint.evidence.peak_merge_name_slots = self
                .checkpoint
                .evidence
                .peak_merge_name_slots
                .max(u64::try_from(retained_names).map_err(storage)?);
        }
        let staged_identities = unified.finish_optional::<BASE_IDENTITY_WIDTH>(
            &self.root,
            &mut cancelled,
            &mut self.checkpoint.evidence,
        )?;
        let staged_identities =
            staged_identities.ok_or_else(|| storage("construction contains no identities"))?;
        let node_details = node_details.finish_optional::<NODE_DETAIL_WIDTH>(
            &self.root,
            &mut cancelled,
            &mut self.checkpoint.evidence,
        )?;
        let edge_details = edge_details.finish_optional::<EDGE_DETAIL_WIDTH>(
            &self.root,
            &mut cancelled,
            &mut self.checkpoint.evidence,
        )?;
        let endpoints = endpoints.finish_optional::<ENDPOINT_WIDTH>(
            &self.root,
            &mut cancelled,
            &mut self.checkpoint.evidence,
        )?;
        let (base_max_node, base_max_edge) = match self.checkpoint.parent_topology_generation {
            0 => (0, 0),
            _ => (if let Some(inventory) = &self.compact_parent {
                compact_parent_surrogate_tails(&self.project_path, inventory)?
            } else {
                None
            })
            .or(crate::writer::read_surrogate_tails(
                // The retained project directory is revalidated immediately above.
                &self.project_path,
            )?)
            .ok_or_else(|| storage("nonempty parent lacks surrogate tails"))?,
        };
        if base_max_node != self.checkpoint.base_work.max_node_surrogate {
            return Err(storage("UUID snapshot and surrogate tails disagree"));
        }
        let (new_nodes, new_edges) = validate_staged_details(
            &self.root,
            &staged_identities,
            node_details.as_deref(),
            edge_details.as_deref(),
            detail_codec,
            &mut cancelled,
            &mut self.checkpoint.evidence,
        )?;
        if let Some(base) = self.base_snapshot.as_mut() {
            reject_staged_base_conflicts(
                &self.root,
                &staged_identities,
                base,
                self.checkpoint.budgets.max_batch_rows,
                &mut cancelled,
                &mut self.checkpoint.evidence,
            )?;
        }
        let identities = assign_surrogates(
            &self.root,
            &staged_identities,
            base_max_node,
            base_max_edge,
            &mut cancelled,
            &mut self.checkpoint.evidence,
        )?;
        // Original chunks remain recovery authority until shaping completes. The
        // assigned identity successor now owns every later identity consumer.
        shape_publication_failure("shape.before_identity_retirement")?;
        unlink_shape_artifact(
            &self.root,
            &staged_identities,
            &mut self.checkpoint.evidence,
        )?;
        construction_failpoint("shape.after_identity_retirement");
        shape_publication_failure("shape.after_identity_retirement")?;
        reject_cancelled(&mut cancelled)?;
        let edge_endpoints = resolve_endpoint_surrogates(
            &self.root,
            &identities,
            endpoints.as_deref(),
            self.base_snapshot.as_mut(),
            self.checkpoint.budgets.max_batch_rows,
            self.checkpoint.budgets.merge_fan_in,
            &mut cancelled,
            &mut self.checkpoint.evidence,
        )?;
        let node_count = self
            .checkpoint
            .base_work
            .live_nodes
            .checked_add(new_nodes)
            .ok_or_else(|| storage("node count overflows"))?;
        let edge_count = self
            .checkpoint
            .base_work
            .live_edges
            .checked_add(new_edges)
            .ok_or_else(|| storage("edge count overflows"))?;
        let max_node_surrogate = base_max_node
            .checked_add(new_nodes)
            .ok_or_else(|| storage("node surrogate overflow"))?;
        let max_edge_surrogate = base_max_edge
            .checked_add(new_edges)
            .ok_or_else(|| storage("edge surrogate overflow"))?;
        let mut node_rows = Vec::new();
        let mut edge_rows = Vec::new();
        for ((kind, schema_digest), rows) in row_groups {
            reject_cancelled(&mut cancelled)?;
            let output = format!("shaped-rows-{kind}-{schema_digest}.parquet");
            let output = rows.finish(
                &self.root,
                &output,
                self.checkpoint.budgets.max_batch_rows,
                self.checkpoint.budgets.max_batch_bytes,
                self.checkpoint.budgets.merge_fan_in,
                &mut cancelled,
                &mut self.checkpoint.evidence,
            )?;
            if kind == 0 {
                node_rows.push(output);
            } else {
                edge_rows.push(output);
            }
        }
        let runtime_catalog = build_runtime_catalog(
            self.parent_catalog.clone(),
            &self.root,
            &node_rows,
            &edge_rows,
            self.checkpoint.session_now_micros,
            self.checkpoint.budgets,
            &mut cancelled,
            &mut self.checkpoint.evidence,
        )?;
        let shape = ConstructionShape {
            ontology_mode: self.checkpoint.ontology_mode,
            semantic_authority_sha256: self.checkpoint.semantic_authority_sha256.clone(),
            parent_topology_generation: self.checkpoint.parent_topology_generation,
            parent_uuid_manifest_sha256: self
                .base_snapshot
                .as_ref()
                .map(|snapshot| snapshot.manifest_sha256().to_owned()),
            identities,
            node_details,
            edge_details,
            node_rows,
            edge_rows,
            edge_endpoints,
            runtime_catalog_now_micros: self.checkpoint.session_now_micros,
            runtime_catalog_inputs_sha256: hex(&catalog_authority.finalize()),
            runtime_catalog,
            node_count,
            edge_count,
            max_node_surrogate,
            max_edge_surrogate,
        };
        let (identity_output, mut inventory_work) =
            receipt_for_existing_with_work(&self.root, &shape.identities)?;
        let mut outputs = vec![identity_output];
        for name in shape
            .node_details
            .iter()
            .chain(shape.edge_details.iter())
            .chain(shape.node_rows.iter())
            .chain(shape.edge_rows.iter())
            .chain(shape.edge_endpoints.iter())
            .chain(std::iter::once(&shape.runtime_catalog))
        {
            let (output, work) = receipt_for_existing_with_work(&self.root, name)?;
            inventory_work.bytes = inventory_work
                .bytes
                .checked_add(work.bytes)
                .ok_or_else(|| storage("shape output authentication bytes overflow"))?;
            inventory_work.operations = inventory_work
                .operations
                .checked_add(work.operations)
                .ok_or_else(|| storage("shape output authentication operations overflow"))?;
            outputs.push(output);
        }
        self.checkpoint.evidence.shaped_output_authentication_bytes = self
            .checkpoint
            .evidence
            .shaped_output_authentication_bytes
            .checked_add(inventory_work.bytes)
            .ok_or_else(|| storage("shaped output authentication bytes overflow"))?;
        self.checkpoint
            .evidence
            .shaped_output_authentication_operations = self
            .checkpoint
            .evidence
            .shaped_output_authentication_operations
            .checked_add(inventory_work.operations)
            .ok_or_else(|| storage("shaped output authentication operations overflow"))?;
        self.checkpoint.evidence.shape_application_read_bytes = checked_evidence_sum(
            "shape application read bytes",
            0,
            &[
                self.checkpoint.evidence.shape_input_validation_read_bytes,
                self.checkpoint.evidence.merge_read_bytes,
                self.checkpoint.evidence.parquet_read_bytes,
                self.checkpoint.evidence.shaped_output_authentication_bytes,
                self.checkpoint.evidence.parent_catalog_read_bytes,
                self.checkpoint.evidence.retained_probe_read_bytes,
            ],
        )?;
        let shape_authority_sha256 = shape_authority_sha256(&shape, &outputs)?;
        self.checkpoint.shape_authority_sha256 = Some(shape_authority_sha256.clone());
        replace_control(
            &self.root,
            SHAPE_INTENT,
            &ShapeIntent {
                format_version: self.checkpoint.format_version,
                operation_uuid: self.checkpoint.operation_uuid,
                project_identity: self.checkpoint.project_identity.clone(),
                session_identity: self.checkpoint.session_identity.clone(),
                parent_topology_generation: self.checkpoint.parent_topology_generation,
                ontology_mode: self.checkpoint.ontology_mode,
                semantic_authority_sha256: self.checkpoint.semantic_authority_sha256.clone(),
                budgets: self.checkpoint.budgets,
                last_receipt_sha256: self.checkpoint.last_receipt_sha256.clone(),
                baseline_evidence: shape_intent.baseline_evidence,
                final_evidence: Some(self.checkpoint.evidence.clone()),
                complete: true,
                shape: Some(shape.clone()),
                outputs,
                shape_authority_sha256: Some(shape_authority_sha256),
            },
        )?;
        construction_failpoint("shape.after_complete_inventory");
        replace_checkpoint_control(&self.root, &self.checkpoint)?;
        construction_failpoint("shape.after_evidence_checkpoint");
        self.reclaim_superseded_payloads_cancellable(&mut cancelled)?;
        Ok(shape)
    }
}

pub(super) fn validate_shape_binding(
    intent: &ShapeIntent,
    checkpoint: &Checkpoint,
) -> Result<(), GfError> {
    if intent.format_version != checkpoint.format_version
        || intent.operation_uuid != checkpoint.operation_uuid
        || intent.project_identity != checkpoint.project_identity
        || intent.session_identity != checkpoint.session_identity
        || intent.parent_topology_generation != checkpoint.parent_topology_generation
        || intent.ontology_mode != checkpoint.ontology_mode
        || intent.semantic_authority_sha256 != checkpoint.semantic_authority_sha256
        || intent.budgets != checkpoint.budgets
        || intent.last_receipt_sha256 != checkpoint.last_receipt_sha256
    {
        return Err(storage("construction shape manifest authority changed"));
    }
    Ok(())
}

pub(super) fn read_completed_shape(
    root: &StableDirectory,
    checkpoint: &Checkpoint,
    authenticate_outputs: bool,
) -> Result<Option<ConstructionShape>, GfError> {
    let mut file = match root.open_child_file(OsStr::new(SHAPE_INTENT)) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(storage(error)),
    };
    let manifest: ShapeIntent = decode_shape_intent(&mut file)?;
    validate_shape_binding(&manifest, checkpoint)?;
    if !manifest.complete {
        return Err(storage("incomplete construction shape was not recovered"));
    }
    if authenticate_outputs {
        for output in &manifest.outputs {
            authenticate_shaped_output(root, output)?;
        }
    }
    let shape = manifest
        .shape
        .ok_or_else(|| storage("complete shape manifest lacks output"))?;
    let authority = shape_authority_sha256(&shape, &manifest.outputs)?;
    if manifest.shape_authority_sha256.as_deref() != Some(&authority)
        || checkpoint.shape_authority_sha256.as_deref() != Some(&authority)
    {
        return Err(storage("completed shape authority digest changed"));
    }
    Ok(Some(shape))
}

pub(super) fn read_completed_shape_outputs(
    root: &StableDirectory,
    checkpoint: &Checkpoint,
) -> Result<Vec<ArtifactReceipt>, GfError> {
    let mut file = root
        .open_child_file(OsStr::new(SHAPE_INTENT))
        .map_err(storage)?;
    let manifest: ShapeIntent = decode_shape_intent(&mut file)?;
    validate_shape_binding(&manifest, checkpoint)?;
    if !manifest.complete || manifest.shape.is_none() {
        return Err(storage("complete construction shape inventory is absent"));
    }
    Ok(manifest.outputs)
}

pub(crate) fn shaped_output_sha256<'a>(
    outputs: &'a [ArtifactReceipt],
    name: &str,
) -> Result<&'a str, GfError> {
    outputs
        .iter()
        .find(|output| output.name == name)
        .map(|output| output.sha256.as_str())
        .ok_or_else(|| storage("shaped output receipt is absent"))
}

pub(crate) fn open_authenticated_shape_source(
    root: &StableDirectory,
    outputs: &[ArtifactReceipt],
    name: &str,
) -> Result<AuthenticatedShapeSource, GfError> {
    let expected = outputs
        .iter()
        .find(|output| output.name == name)
        .ok_or_else(|| storage("shaped output receipt is absent"))?;
    let file = root.open_child_file(OsStr::new(name)).map_err(storage)?;
    if file_link_count(&file).map_err(storage)? != 1 {
        return Err(storage("shaped output has extra links"));
    }
    let identity = file_identity(&file).map_err(storage)?;
    if !expected.identity.matches(identity) {
        return Err(storage("shaped output identity changed before consumption"));
    }
    Ok(AuthenticatedShapeSource {
        file,
        identity,
        bytes: expected.bytes,
        sha256: expected.sha256.clone(),
    })
}

pub(super) fn shape_receipt_name(name: &str) -> String {
    format!("shape-receipt-{}.json", &sha256(name.as_bytes())[..32])
}

pub(super) fn persist_shape_receipt(
    root: &StableDirectory,
    receipt: &ArtifactReceipt,
) -> Result<(), GfError> {
    if is_shape_artifact_name(&receipt.name) || canonical_artifact_target(&receipt.name) {
        let capability_name = shape_receipt_name(&receipt.name);
        match root.open_child_file(OsStr::new(&capability_name)) {
            Ok(mut file) => {
                if file_link_count(&file).map_err(storage)? != 1 {
                    return Err(storage("shaped writer capability has extra links"));
                }
                let existing: ArtifactReceipt = decode_bounded(&mut file)?;
                if existing != *receipt {
                    return Err(storage("shaped writer capability changed"));
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                install_control(root, &capability_name, receipt)?;
            }
            Err(error) => return Err(storage(error)),
        }
    }
    Ok(())
}

pub(super) fn authenticate_shaped_output(
    root: &StableDirectory,
    expected: &ArtifactReceipt,
) -> Result<(), GfError> {
    if !is_shape_artifact_name(&expected.name) && !canonical_artifact_target(&expected.name) {
        return Err(storage("shape manifest output name is not canonical"));
    }
    let actual = receipt_for_existing(root, &expected.name)?;
    if actual.bytes != expected.bytes
        || actual.sha256 != expected.sha256
        || actual.identity != expected.identity
    {
        return Err(storage("shape manifest output authentication changed"));
    }
    Ok(())
}

pub(super) fn is_shape_artifact_name(name: &str) -> bool {
    if name == "shaped-identities.run" || name == "shaped-runtime-catalog.parquet" {
        return true;
    }
    if let Some(body) = name
        .strip_prefix("shaped-rows-")
        .and_then(|body| body.strip_suffix(".parquet"))
    {
        return body.len() == 66
            && matches!(body.as_bytes().first(), Some(b'0' | b'1'))
            && body.as_bytes().get(1) == Some(&b'-')
            && is_canonical_sha256(&body[2..]);
    }
    if let Some(body) = name
        .strip_prefix("merge-rows-")
        .and_then(|body| body.strip_suffix(".parquet"))
    {
        let mut parts = body.split("-l");
        let namespace = parts.next().unwrap_or_default();
        let level_group = parts.next().unwrap_or_default();
        return parts.next().is_none()
            && namespace.len() == 16
            && is_canonical_lower_hex(namespace, 16)
            && level_group.split_once("-g").is_some_and(|(level, group)| {
                level.len() == 3
                    && level.bytes().all(|byte| byte.is_ascii_digit())
                    && group.len() == 20
                    && group.bytes().all(|byte| byte.is_ascii_digit())
            });
    }
    if let Some(sequence) = name
        .strip_prefix("merge-unified-")
        .and_then(|body| body.strip_suffix(".run"))
    {
        return sequence.len() == 20 && sequence.bytes().all(|byte| byte.is_ascii_digit());
    }
    for prefix in [
        "merge-node-source-",
        "merge-edge-source-",
        "merge-endpoint-source-",
        "merge-resolved-source-",
    ] {
        if let Some(sequence) = name
            .strip_prefix(prefix)
            .and_then(|body| body.strip_suffix(".run"))
        {
            return sequence.len() == 20 && sequence.bytes().all(|byte| byte.is_ascii_digit());
        }
    }
    [
        "merge-identities-l",
        "merge-node-details-l",
        "merge-edge-details-l",
        "merge-endpoints-l",
        "merge-resolved-l",
    ]
    .iter()
    .any(|prefix| {
        name.strip_prefix(prefix).is_some_and(|tail| {
            tail.len() == 17
                && tail.get(3..5) == Some("-g")
                && tail.get(13..) == Some(".run")
                && tail[..3].bytes().all(|byte| byte.is_ascii_digit())
                && tail[5..13].bytes().all(|byte| byte.is_ascii_digit())
        })
    })
}

pub(super) fn run_record_bytes<const N: usize>(
    record: &[u8; N],
    codec: Option<DetailCodec>,
) -> Result<&[u8], GfError> {
    match codec {
        Some(codec) => codec.bytes(record).map_err(storage),
        None => Ok(record),
    }
}

pub(super) fn read_fixed<const N: usize>(
    reader: &mut impl Read,
) -> Result<Option<[u8; N]>, GfError> {
    let mut record = [0_u8; N];
    let mut filled = 0;
    while filled < N {
        match reader.read(&mut record[filled..]).map_err(storage)? {
            0 if filled == 0 => return Ok(None),
            0 => return Err(storage("truncated fixed-width construction run")),
            count => filled += count,
        }
    }
    Ok(Some(record))
}

fn validate_staged_details(
    root: &StableDirectory,
    identities_name: &str,
    node_details_name: Option<&str>,
    edge_details_name: Option<&str>,
    codec: DetailCodec,
    cancelled: &mut impl FnMut() -> bool,
    evidence: &mut GraphConstructionEvidence,
) -> Result<(u64, u64), GfError> {
    let (mut identities, identities_counter) =
        open_counted_fixed_reader(root, identities_name, evidence)?;
    let mut nodes = 0_u64;
    let mut edges = 0_u64;
    while let Some(record) = read_fixed::<BASE_IDENTITY_WIDTH>(&mut identities)? {
        if record[17] != 0 || record[18..].iter().any(|byte| *byte != 0) {
            return Err(storage("staged identity record is not canonical"));
        }
        match record[16] {
            0 => {
                nodes = nodes
                    .checked_add(1)
                    .ok_or_else(|| storage("staged node count overflows"))?;
            }
            1 => {
                edges = edges
                    .checked_add(1)
                    .ok_or_else(|| storage("staged edge count overflows"))?;
            }
            _ => return Err(storage("invalid staged identity kind")),
        }
        account_merge_read::<BASE_IDENTITY_WIDTH>(evidence)?;
        if nodes
            .checked_add(edges)
            .ok_or_else(|| storage("staged identity count overflows"))?
            .is_multiple_of(4096)
        {
            reject_cancelled(cancelled)?;
        }
    }
    account_fixed_read_operations(&identities_counter, evidence)?;
    let actual_nodes = validate_detail_domain::<NODE_DETAIL_WIDTH>(
        root,
        identities_name,
        node_details_name,
        0,
        codec,
        cancelled,
        evidence,
    )?;
    let actual_edges = validate_detail_domain::<EDGE_DETAIL_WIDTH>(
        root,
        identities_name,
        edge_details_name,
        1,
        codec,
        cancelled,
        evidence,
    )?;
    if actual_nodes != nodes || actual_edges != edges {
        return Err(storage("staged identity and detail domains disagree"));
    }
    Ok((nodes, edges))
}

fn reject_staged_base_conflicts(
    root: &StableDirectory,
    identities_name: &str,
    base: &mut AuthenticatedUuidIndexSnapshot,
    window_rows: usize,
    cancelled: &mut impl FnMut() -> bool,
    evidence: &mut GraphConstructionEvidence,
) -> Result<(), GfError> {
    let (mut reader, reader_counter) = open_counted_fixed_reader(root, identities_name, evidence)?;
    loop {
        let mut requested = Vec::with_capacity(window_rows);
        for _ in 0..window_rows {
            let Some(record) = read_fixed::<BASE_IDENTITY_WIDTH>(&mut reader)? else {
                break;
            };
            account_merge_read::<BASE_IDENTITY_WIDTH>(evidence)?;
            requested.push(Uuid::from_bytes(
                record[..16].try_into().expect("fixed UUID"),
            ));
        }
        if requested.is_empty() {
            break;
        }
        let (nodes, node_work) = base.probe(UuidIndexKind::Node, &requested)?;
        let (edges, edge_work) = base.probe(UuidIndexKind::Edge, &requested)?;
        account_probe_work(&node_work, evidence)?;
        account_probe_work(&edge_work, evidence)?;
        if nodes
            .into_iter()
            .zip(edges)
            .any(|(node, edge)| node || edge)
        {
            return Err(storage("staged UUID conflicts with pinned base identity"));
        }
        reject_cancelled(cancelled)?;
    }
    account_fixed_read_operations(&reader_counter, evidence)?;
    release_counted_reader_cache(&mut reader, evidence)?;
    base.revalidate()?;
    Ok(())
}

fn validate_detail_domain<const N: usize>(
    root: &StableDirectory,
    identities_name: &str,
    details_name: Option<&str>,
    kind: u8,
    codec: DetailCodec,
    cancelled: &mut impl FnMut() -> bool,
    evidence: &mut GraphConstructionEvidence,
) -> Result<u64, GfError> {
    let Some(details_name) = details_name else {
        return Ok(0);
    };
    let (mut identities, identities_counter) =
        open_counted_fixed_reader(root, identities_name, evidence)?;
    let (mut details, details_counter) = open_counted_fixed_reader(root, details_name, evidence)?;
    let mut identity = read_fixed::<BASE_IDENTITY_WIDTH>(&mut identities)?;
    if identity.is_some() {
        account_merge_read::<BASE_IDENTITY_WIDTH>(evidence)?;
    }
    let mut count = 0_u64;
    while let Some(detail) = codec.read::<N>(&mut details).map_err(storage)? {
        while identity
            .as_ref()
            .is_some_and(|item| item[17] == 1 || item[16] != kind || item[..16] < detail[..16])
        {
            identity = read_fixed::<BASE_IDENTITY_WIDTH>(&mut identities)?;
            account_merge_read::<BASE_IDENTITY_WIDTH>(evidence)?;
        }
        if identity
            .as_ref()
            .is_none_or(|item| item[17] != 0 || item[16] != kind || item[..16] != detail[..16])
        {
            return Err(storage(
                "canonical detail UUID differs from identity domain",
            ));
        }
        account_merge_read::<N>(evidence)?;
        identity = read_fixed::<BASE_IDENTITY_WIDTH>(&mut identities)?;
        account_merge_read::<BASE_IDENTITY_WIDTH>(evidence)?;
        count += 1;
        if count.is_multiple_of(4096) {
            reject_cancelled(cancelled)?;
        }
    }
    while identity
        .as_ref()
        .is_some_and(|item| item[17] == 1 || item[16] != kind)
    {
        identity = read_fixed::<BASE_IDENTITY_WIDTH>(&mut identities)?;
    }
    if identity.is_some() {
        return Err(storage(
            "identity domain contains a row without canonical detail",
        ));
    }
    account_fixed_read_operations(&identities_counter, evidence)?;
    account_fixed_read_operations(&details_counter, evidence)?;
    release_counted_reader_cache(&mut identities, evidence)?;
    release_counted_reader_cache(&mut details, evidence)?;
    Ok(count)
}

#[cfg(test)]
#[allow(dead_code)]
fn validate_endpoints(
    root: &StableDirectory,
    identities_name: &str,
    endpoints_name: Option<&str>,
    new_edges: u64,
    cancelled: &mut impl FnMut() -> bool,
    evidence: &mut GraphConstructionEvidence,
) -> Result<(), GfError> {
    if new_edges == 0 {
        return if endpoints_name.is_none() {
            Ok(())
        } else {
            Err(storage("endpoint run exists without new edges"))
        };
    }
    let endpoints_name = endpoints_name.ok_or_else(|| storage("new edges lack endpoints"))?;
    let (mut identities, identities_counter) =
        open_counted_fixed_reader(root, identities_name, evidence)?;
    let (mut endpoints, endpoints_counter) =
        open_counted_fixed_reader(root, endpoints_name, evidence)?;
    let mut identity = read_fixed::<BASE_IDENTITY_WIDTH>(&mut identities)?;
    let mut endpoint_count = 0_u64;
    while let Some(endpoint) = read_fixed::<ENDPOINT_WIDTH>(&mut endpoints)? {
        while identity
            .as_ref()
            .is_some_and(|item| item[..16] < endpoint[..16])
        {
            identity = read_fixed::<BASE_IDENTITY_WIDTH>(&mut identities)?;
            account_merge_read::<BASE_IDENTITY_WIDTH>(evidence)?;
        }
        let Some(node) = identity.as_ref() else {
            return Err(storage("edge endpoint UUID does not exist"));
        };
        if node[..16] != endpoint[..16] || node[16] != 0 {
            return Err(storage("edge endpoint is not a node UUID"));
        }
        if endpoint[32] > 1 {
            return Err(storage("endpoint run record is not canonical"));
        }
        endpoint_count = endpoint_count
            .checked_add(1)
            .ok_or_else(|| storage("endpoint validation count overflow"))?;
        account_merge_read::<ENDPOINT_WIDTH>(evidence)?;
        if endpoint_count.is_multiple_of(4096) {
            reject_cancelled(cancelled)?;
        }
    }
    if endpoint_count
        != new_edges
            .checked_mul(2)
            .ok_or_else(|| storage("expected endpoint count overflow"))?
    {
        return Err(storage(
            "edge endpoint cardinality differs from edge domain",
        ));
    }
    account_fixed_read_operations(&identities_counter, evidence)?;
    account_fixed_read_operations(&endpoints_counter, evidence)?;
    release_counted_reader_cache(&mut identities, evidence)?;
    release_counted_reader_cache(&mut endpoints, evidence)?;
    Ok(())
}

fn assign_surrogates(
    root: &StableDirectory,
    input_name: &str,
    mut node_tail: u64,
    mut edge_tail: u64,
    cancelled: &mut impl FnMut() -> bool,
    evidence: &mut GraphConstructionEvidence,
) -> Result<String, GfError> {
    let output = "shaped-identities.run";
    let temporary = artifact_temp(output);
    let file = root
        .create_replaceable_child_file(OsStr::new(&temporary))
        .map_err(storage)?;
    let identity = file_identity(&file).map_err(storage)?;
    let (mut reader, reader_counter) = open_counted_fixed_reader(root, input_name, evidence)?;
    let hashing = HashingWriter::new(file)?;
    let mut writer = BufWriter::with_capacity(BLOCK_BYTES, hashing);
    let mut count = 0_u64;
    while let Some(mut record) = read_fixed::<BASE_IDENTITY_WIDTH>(&mut reader)? {
        if record[17] == 0 {
            let surrogate = match record[16] {
                0 => {
                    node_tail = node_tail
                        .checked_add(1)
                        .ok_or_else(|| storage("node surrogate overflow"))?;
                    node_tail
                }
                1 => {
                    edge_tail = edge_tail
                        .checked_add(1)
                        .ok_or_else(|| storage("edge surrogate overflow"))?;
                    edge_tail
                }
                _ => return Err(storage("invalid identity kind during shaping")),
            };
            record[IDENTITY_SURROGATE_OFFSET..BASE_IDENTITY_WIDTH]
                .copy_from_slice(&surrogate.to_be_bytes());
        } else if record[17] != 1 {
            return Err(storage("invalid retained identity marker during shaping"));
        }
        writer.write_all(&record).map_err(storage)?;
        account_merge_read::<BASE_IDENTITY_WIDTH>(evidence)?;
        account_merge_write::<BASE_IDENTITY_WIDTH>(evidence)?;
        count += 1;
        if count.is_multiple_of(4096) {
            reject_cancelled(cancelled)?;
        }
    }
    account_fixed_read_operations(&reader_counter, evidence)?;
    writer.flush().map_err(storage)?;
    writer
        .get_mut()
        .inner
        .sync_all_and_release()
        .map_err(storage)?;
    let cache_release = writer.get_ref().inner.evidence();
    account_cache_release(cache_release, evidence)?;
    account_sequential_write(writer.get_ref().bytes, evidence)?;
    let output_receipt = ArtifactReceipt {
        name: output.to_owned(),
        bytes: writer.get_ref().bytes,
        allocated_bytes: graphforge_filesystem::file_space_usage(writer.get_ref().inner.file())
            .map_err(storage)?
            .allocated_bytes,
        sha256: hex(&writer.get_ref().digest.clone().finalize()),
        identity: identity.into(),
        write_operations: writer.get_ref().operations,
        fsync_operations: cache_release
            .sync_operations
            .checked_add(1)
            .ok_or_else(|| storage("artifact synchronization count overflows"))?,
    };
    drop(writer);
    root.install_child(OsStr::new(&temporary), identity, OsStr::new(output))
        .map_err(storage)?;
    root.sync().map_err(storage)?;
    persist_shape_receipt(root, &output_receipt)?;
    record_shape_artifact_install(evidence, &output_receipt)?;
    account_fixed_write_operations(&output_receipt, evidence)?;
    evidence.merge_fsync_operations = evidence
        .merge_fsync_operations
        .checked_add(output_receipt.fsync_operations)
        .ok_or_else(|| storage("merge fsync operations overflows"))?;
    Ok(output.to_owned())
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(super) fn resolve_endpoint_surrogates(
    root: &StableDirectory,
    identities_name: &str,
    endpoints_name: Option<&str>,
    mut base: Option<&mut AuthenticatedUuidIndexSnapshot>,
    window_rows: usize,
    fan_in: usize,
    cancelled: &mut impl FnMut() -> bool,
    evidence: &mut GraphConstructionEvidence,
) -> Result<Option<String>, GfError> {
    let Some(endpoints_name) = endpoints_name else {
        return Ok(None);
    };
    let (mut identities, identities_counter) =
        open_counted_fixed_reader(root, identities_name, evidence)?;
    let (mut endpoints, endpoints_counter) =
        open_counted_fixed_reader(root, endpoints_name, evidence)?;
    let mut identity = read_fixed::<BASE_IDENTITY_WIDTH>(&mut identities)?;
    let mut window = Vec::<[u8; RESOLVED_ENDPOINT_WIDTH]>::with_capacity(window_rows);
    let mut resolved = FixedMergeAccumulator::new("merge-resolved", fan_in, false);
    let mut sequence = 0_u64;
    loop {
        let mut endpoint_window = Vec::with_capacity(window_rows);
        for _ in 0..window_rows {
            let Some(endpoint) = read_fixed::<ENDPOINT_WIDTH>(&mut endpoints)? else {
                break;
            };
            endpoint_window.push(endpoint);
        }
        if endpoint_window.is_empty() {
            break;
        }
        let mut surrogates = Vec::with_capacity(endpoint_window.len());
        let mut base_requests = Vec::new();
        let mut base_positions = Vec::new();
        for endpoint in &endpoint_window {
            while identity
                .as_ref()
                .is_some_and(|record| record[..16] < endpoint[..16])
            {
                identity = read_fixed::<BASE_IDENTITY_WIDTH>(&mut identities)?;
                account_merge_read::<BASE_IDENTITY_WIDTH>(evidence)?;
            }
            if let Some(node) = identity
                .as_ref()
                .filter(|record| record[..16] == endpoint[..16] && record[16] == 0)
            {
                surrogates.push(Some(u64::from_be_bytes(
                    node[IDENTITY_SURROGATE_OFFSET..BASE_IDENTITY_WIDTH]
                        .try_into()
                        .expect("fixed"),
                )));
            } else {
                base_positions.push(surrogates.len());
                base_requests.push(Uuid::from_bytes(endpoint[..16].try_into().expect("fixed")));
                surrogates.push(None);
            }
        }
        if !base_requests.is_empty() {
            let (resolved, probe_work) = base
                .as_deref_mut()
                .ok_or_else(|| storage("endpoint UUID lacks node surrogate"))?
                .lookup_node_surrogates(&base_requests)?;
            account_probe_work(&probe_work, evidence)?;
            for (position, surrogate) in base_positions.into_iter().zip(resolved) {
                surrogates[position] = surrogate;
            }
        }
        for (endpoint, surrogate) in endpoint_window.into_iter().zip(surrogates) {
            let surrogate = surrogate
                .filter(|value| *value != 0)
                .ok_or_else(|| storage("endpoint UUID lacks node surrogate"))?;
            let mut resolved = [0_u8; RESOLVED_ENDPOINT_WIDTH];
            resolved[..16].copy_from_slice(&endpoint[16..32]);
            resolved[16] = endpoint[32];
            resolved[RESOLVED_SURROGATE_OFFSET..RESOLVED_ENDPOINT_WIDTH]
                .copy_from_slice(&surrogate.to_be_bytes());
            window.push(resolved);
            account_merge_read::<ENDPOINT_WIDTH>(evidence)?;
        }
        // Defer the final insertion: push can itself trigger the largest carry
        // merge. The trailing block installs its durable run before retirement.
        if endpoints.fill_buf().map_err(storage)?.is_empty() {
            break;
        }
        if window.len() == window_rows {
            window.sort_unstable();
            let name = format!("merge-resolved-source-{sequence:020}.run");
            let receipt = write_fixed_run(root, &name, &window, evidence)?;
            record_shape_artifact_install(evidence, &receipt)?;
            evidence.merge_written_bytes = evidence
                .merge_written_bytes
                .checked_add(receipt.bytes)
                .ok_or_else(|| storage("merge written byte count overflows"))?;
            account_fixed_write_operations(&receipt, evidence)?;
            evidence.merge_written_records = evidence
                .merge_written_records
                .checked_add(window.len() as u64)
                .ok_or_else(|| storage("merge written record count overflows"))?;
            evidence.merge_fsync_operations = evidence
                .merge_fsync_operations
                .checked_add(receipt.fsync_operations)
                .ok_or_else(|| storage("merge fsync count overflows"))?;
            account_sequential_write(receipt.bytes, evidence)?;
            resolved.push::<RESOLVED_ENDPOINT_WIDTH>(root, name, cancelled, evidence)?;
            evidence.peak_resolved_endpoint_name_slots = evidence
                .peak_resolved_endpoint_name_slots
                .max(resolved.slot_count() as u64);
            window.clear();
            sequence = sequence
                .checked_add(1)
                .ok_or_else(|| storage("resolved endpoint sequence overflows"))?;
            reject_cancelled(cancelled)?;
        }
    }
    let pending = if window.is_empty() {
        None
    } else {
        window.sort_unstable();
        let name = format!("merge-resolved-source-{sequence:020}.run");
        let receipt = write_fixed_run(root, &name, &window, evidence)?;
        record_shape_artifact_install(evidence, &receipt)?;
        evidence.merge_written_bytes = evidence
            .merge_written_bytes
            .checked_add(receipt.bytes)
            .ok_or_else(|| storage("merge written byte count overflows"))?;
        account_fixed_write_operations(&receipt, evidence)?;
        evidence.merge_written_records = evidence
            .merge_written_records
            .checked_add(window.len() as u64)
            .ok_or_else(|| storage("merge written record count overflows"))?;
        evidence.merge_fsync_operations = evidence
            .merge_fsync_operations
            .checked_add(receipt.fsync_operations)
            .ok_or_else(|| storage("merge fsync count overflows"))?;
        account_sequential_write(receipt.bytes, evidence)?;
        Some(name)
    };
    account_fixed_read_operations(&identities_counter, evidence)?;
    account_fixed_read_operations(&endpoints_counter, evidence)?;
    // All resolved windows are durable; the final merge needs only those runs.
    drop(identities);
    drop(endpoints);
    shape_publication_failure("shape.before_endpoint_retirement")?;
    unlink_shape_artifact(root, endpoints_name, evidence)?;
    construction_failpoint("shape.after_endpoint_retirement");
    shape_publication_failure("shape.after_endpoint_retirement")?;
    reject_cancelled(cancelled)?;
    if let Some(name) = pending {
        resolved.push::<RESOLVED_ENDPOINT_WIDTH>(root, name, cancelled, evidence)?;
        evidence.peak_resolved_endpoint_name_slots = evidence
            .peak_resolved_endpoint_name_slots
            .max(resolved.slot_count() as u64);
    }
    resolved.finish_optional::<RESOLVED_ENDPOINT_WIDTH>(root, cancelled, evidence)
}

pub(super) fn validate_sorted_run(
    file: File,
    width: usize,
    codec: DetailCodec,
    expected_records: u64,
) -> Result<(), GfError> {
    let releasing = graphforge_filesystem::FileCacheReleasingReader::new(file).map_err(storage)?;
    let mut reader = BufReader::with_capacity(BLOCK_BYTES, releasing);
    let mut block = vec![0_u8; BLOCK_BYTES];
    let mut pending = Vec::new();
    let mut previous: Option<Vec<u8>> = None;
    let mut detail = if matches!(width, NODE_DETAIL_WIDTH | EDGE_DETAIL_WIDTH) {
        Some(DetailValidator::new(codec, width).map_err(storage)?)
    } else {
        None
    };
    let validated = (|| -> Result<(), GfError> {
        loop {
            let count = reader.read(&mut block).map_err(storage)?;
            if count == 0 {
                break;
            }
            if let Some(detail) = detail.as_mut() {
                detail.consume(&block[..count]).map_err(storage)?;
                continue;
            }
            pending.extend_from_slice(&block[..count]);
            let complete = pending.len() / width * width;
            for record in pending[..complete].chunks_exact(width) {
                if previous
                    .as_ref()
                    .is_some_and(|prior| prior.as_slice() >= record)
                    || (width == ENDPOINT_WIDTH && (!matches!(record[32], 0 | 1)))
                    || (width == EDGE_DETAIL_WIDTH
                        && (record[48] == 0
                            || record[49 + record[48] as usize..]
                                .iter()
                                .any(|byte| *byte != 0)))
                    || (width == NODE_DETAIL_WIDTH
                        && (record[16] == 0
                            || record[17 + record[16] as usize..]
                                .iter()
                                .any(|byte| *byte != 0)))
                    || (width == BASE_IDENTITY_WIDTH
                        && (!matches!(record[16], 0 | 1)
                            || record[17] != 1
                            || (record[16] == 0
                                && record[IDENTITY_SURROGATE_OFFSET..]
                                    .iter()
                                    .all(|byte| *byte == 0))
                            || (record[16] == 1
                                && record[IDENTITY_SURROGATE_OFFSET..]
                                    .iter()
                                    .any(|byte| *byte != 0))))
                {
                    return Err(storage("unrecorded fixed run is malformed"));
                }
                previous = Some(record.to_vec());
            }
            pending.drain(..complete);
        }
        if let Some(detail) = detail.as_ref()
            && detail.finish().map_err(storage)? != expected_records
        {
            return Err(storage("unrecorded detail row count changed"));
        }
        if !pending.is_empty() {
            return Err(storage("unrecorded fixed run has truncated tail"));
        }
        Ok(())
    })();
    let released = reader.get_mut().finish().map_err(storage);
    combine_cache_cleanup(validated, released.map(|_| ()), "unrecorded fixed run")
}

#[cfg(test)]
mod tests;
