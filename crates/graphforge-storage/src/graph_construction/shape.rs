//! Shape for graph construction.

use super::finish_stages::{ShapeStageKind, ShapeStages, StageResult};
use super::partition::PartitionBalance;
use super::partition::{IdentitySampler, PartitionPlan};
use super::partition_shaping::{
    FixedRangePartitioner, PartitionFamily, RowRangePartitioner, is_partition_artifact_name,
    parse_segment_name,
};
use super::progress::{
    LoadedShapeProgress, ShapeProgressPartition, authenticate_shape_segments,
    install_shape_progress,
};
use super::recovery::{
    is_shape_scoped_name, reconcile_shape_artifact_removal, unlink_shape_artifact_files,
};
use super::{
    ArtifactReceipt, AuthenticatedShapeSource, AuthenticatedUuidIndexSnapshot, BASE_IDENTITY_WIDTH,
    BLOCK_BYTES, BTreeMap, BufReader, BufWriter, CatalogSource, Checkpoint, ConstructionChunkKind,
    ConstructionChunkReceipt, ConstructionPublicationState, ConstructionShape, CountingRead,
    DetailCodec, DetailValidator, Digest, EDGE_DETAIL_WIDTH, ENDPOINT_WIDTH, File, GfError,
    GraphConstructionEvidence, GraphConstructionSession, GraphConstructionState, HashingWriter,
    IDENTITY_SURROGATE_OFFSET, IDENTITY_WIDTH, IoCounter, NODE_DETAIL_WIDTH, OsStr,
    RESOLVED_ENDPOINT_WIDTH, RESOLVED_SURROGATE_OFFSET, Read, ReadWork, SHAPE_INTENT,
    SealDirectoryBatch, Sha256, ShapeIntent, StableDirectory, Uuid, UuidIndexKind, Write,
    account_cache_release, account_fixed_read_operations, account_fixed_write_operations,
    account_merge_read, account_merge_write, account_probe_work, account_sequential_read,
    account_sequential_write, artifact_temp, authenticate_artifact, build_runtime_catalog,
    canonical_artifact_target, checked_evidence_sum, combine_cache_cleanup,
    compact_parent_surrogate_tails, construction_failpoint, decode_bounded, decode_shape_intent,
    discard_completed_shape_segments, file_identity, file_link_count, hex, install_control,
    install_control_batched, install_shape_intent, is_canonical_lower_hex, is_canonical_sha256,
    load_shape_progress_chain, merge_cache_release_evidence, open_counted_fixed_reader,
    property_free_schema_sha256, read_run_record, receipt_for_existing,
    receipt_for_existing_with_work, reconcile_retained_shape_segments,
    record_shape_artifact_install, reject_cancelled, reject_existing_merge_artifacts,
    release_counted_reader_cache, replace_checkpoint_control, replace_shape_intent,
    retained_shape_segments, retire_staged_payload, scan_shape_segments, sha256,
    shape_authority_sha256, shape_publication_failure, storage, unlink_reconciled_shape_segments,
    unlink_shape_artifact, validate_parquet_metadata,
};
use std::io::Seek;

/// Pre-surrogate identity domain, produced by concatenating sorted partitions.
pub(super) const STAGED_IDENTITIES: &str = "staged-identities.run";
/// Pre-resolution endpoint domain, produced by concatenating sorted partitions.
pub(super) const STAGED_ENDPOINTS: &str = "staged-endpoints.run";
/// Shaped node detail domain.
pub(super) const SHAPED_NODE_DETAILS: &str = "shaped-node-details.run";
/// Shaped edge detail domain.
pub(super) const SHAPED_EDGE_DETAILS: &str = "shaped-edge-details.run";
/// Shaped, surrogate-resolved edge endpoint domain.
pub(super) const SHAPED_EDGE_ENDPOINTS: &str = "shaped-edge-endpoints.run";
/// Surrogate-assigned identity domain.
pub(super) const SHAPED_IDENTITIES: &str = "shaped-identities.run";
/// Runtime catalog artifact.
pub(super) const SHAPED_RUNTIME_CATALOG: &str = "shaped-runtime-catalog.parquet";

/// Encode recorded splitters as canonical lower-hex, for the durable intent.
pub(super) fn encode_splitters(plan: &PartitionPlan) -> Vec<String> {
    plan.splitters().iter().map(|key| hex(key)).collect()
}

/// Decode recorded splitters from the durable intent.
///
/// # Errors
/// Returns an error when a recorded splitter is not canonical 32-character
/// lower-case hex.
pub(super) fn decode_splitters(recorded: &[String]) -> Result<Vec<[u8; 16]>, GfError> {
    recorded
        .iter()
        .map(|encoded| {
            if !is_canonical_lower_hex(encoded, 32) {
                return Err(storage("recorded partition splitter is not canonical"));
            }
            let mut key = [0_u8; 16];
            for (index, slot) in key.iter_mut().enumerate() {
                *slot = u8::from_str_radix(&encoded[index * 2..index * 2 + 2], 16)
                    .map_err(|_| storage("recorded partition splitter is not canonical"))?;
            }
            Ok(key)
        })
        .collect()
}

impl GraphConstructionSession {
    /// Barrier A0: choose the range-partition splitters for this shaping run.
    ///
    /// The stride is derived from the recorded chunk receipts, so the sampled
    /// positions are fixed before a byte is read and the pass seeks directly to
    /// them. Sampling therefore costs `O(partition_count)` reads rather than a
    /// scan of the identity domain, and the result is a pure function of the
    /// staged input and the recorded partition count.
    fn choose_partition_plan(
        &mut self,
        cancelled: &mut impl FnMut() -> bool,
    ) -> Result<PartitionPlan, GfError> {
        self.sample_partition_plan(None, cancelled)
    }

    /// Barrier A0'. Choose splitters over only the staged **node** identity
    /// domain (#1439).
    ///
    /// Endpoints, node details and node-kind rows are keyed by node UUID, but
    /// the joint splitters above are sampled from the combined node+edge
    /// domain. Graph500-shaped input is disjoint by kind in UUID-space
    /// (namespace bit) and overwhelmingly edges, so almost every node UUID
    /// falls inside a handful of the joint splitters' partitions: those
    /// families were routed with the wrong splitters, and it is the
    /// dominant term in ingest resident memory, not partition count.
    ///
    /// A second, node-only splitter set is a pure function of the same
    /// recorded chunk receipts as the joint one, so it costs R1 nothing: it
    /// is what actually matches the key distribution those families are
    /// routed by.
    fn choose_node_partition_plan(
        &mut self,
        cancelled: &mut impl FnMut() -> bool,
    ) -> Result<PartitionPlan, GfError> {
        self.sample_partition_plan(Some(ConstructionChunkKind::Node), cancelled)
    }

    /// Shared sampler behind [`Self::choose_partition_plan`] and
    /// [`Self::choose_node_partition_plan`]: identical logic, restricted to
    /// receipts of `kind_filter` when given.
    fn sample_partition_plan(
        &mut self,
        kind_filter: Option<ConstructionChunkKind>,
        cancelled: &mut impl FnMut() -> bool,
    ) -> Result<PartitionPlan, GfError> {
        let partition_count = self.checkpoint.budgets.partition_count;
        let mut extents = Vec::new();
        let mut total = 0_u64;
        for sequence in 0..self.checkpoint.next_sequence {
            reject_cancelled(cancelled)?;
            let receipt = self.read_receipt(sequence)?;
            if kind_filter.is_some_and(|kind| receipt.kind != kind) {
                continue;
            }
            if receipt.identities.bytes % IDENTITY_WIDTH as u64 != 0 {
                return Err(storage("staged identity run is not record aligned"));
            }
            let records = receipt.identities.bytes / IDENTITY_WIDTH as u64;
            extents.push((receipt.identities.name.clone(), total, records));
            total = total
                .checked_add(records)
                .ok_or_else(|| storage("staged identity record count overflows"))?;
        }
        // #1439 follow-up: sizing the node plan's cut from the *routed*
        // population (endpoints + node details, which outnumber node
        // identities by the graph's average degree) rather than this sample
        // domain was tried and measured worse -- it drove the count past the
        // empirically measured RSS-vs-buffer-overhead minimum into the
        // buffer-dominated side of that curve. Reverted: the cut is sized
        // from the sample domain here, same as the joint plan.
        let mut sampler = IdentitySampler::with_target(
            partition_count,
            total,
            self.checkpoint.budgets.target_partition_records,
        )?;
        let positions = sampler.positions().collect::<Vec<_>>();
        let mut cursor = 0_usize;
        for position in positions {
            while cursor < extents.len() && position >= extents[cursor].1 + extents[cursor].2 {
                cursor += 1;
            }
            let (name, base, _) = extents
                .get(cursor)
                .ok_or_else(|| storage("identity sample position is out of range"))?;
            let mut file = self
                .root
                .open_child_file(OsStr::new(name))
                .map_err(storage)?;
            file.seek(std::io::SeekFrom::Start(
                (position - base)
                    .checked_mul(IDENTITY_WIDTH as u64)
                    .ok_or_else(|| storage("identity sample offset overflows"))?,
            ))
            .map_err(storage)?;
            let mut key = [0_u8; IDENTITY_WIDTH];
            file.read_exact(&mut key).map_err(storage)?;
            account_sequential_read(IDENTITY_WIDTH as u64, &mut self.checkpoint.evidence)?;
            sampler.admit(key)?;
        }
        if kind_filter.is_none() {
            self.checkpoint.evidence.splitter_sample_records = sampler.sampled_records();
            self.checkpoint.evidence.splitter_sampled_source_records = sampler.source_records();
        } else {
            self.checkpoint.evidence.node_splitter_sample_records = sampler.sampled_records();
            self.checkpoint
                .evidence
                .node_splitter_sampled_source_records = sampler.source_records();
        }
        sampler.into_plan(partition_count)
    }

    /// Validate the sealed identity domains and produce deterministic,
    /// UUID-sorted canonical construction runs.  This is deliberately still
    /// private staging: the generation-last publisher owns Parquet and CURRENT.
    #[allow(clippy::too_many_lines)] // One authenticated external-shape lifecycle; ordering is the invariant.
    pub fn shape_canonical_with_cancellation(
        &mut self,
        cancelled: impl FnMut() -> bool,
    ) -> Result<ConstructionShape, GfError> {
        self.shape_canonical_inner(cancelled)
    }

    #[allow(clippy::too_many_lines)] // One authenticated external-shape lifecycle; ordering is the invariant.
    pub(super) fn shape_canonical_inner(
        &mut self,
        mut cancelled: impl FnMut() -> bool,
    ) -> Result<ConstructionShape, GfError> {
        let _diagnostic_scope = crate::graph_construction::diagnostics::Scope::start("shaping");
        self.revalidate_authority()?;
        if self.checkpoint.state != GraphConstructionState::Sealed
            || self.checkpoint.publication_state != Some(ConstructionPublicationState::Sealed)
        {
            return Err(storage("only a sealed session can be shaped"));
        }
        reject_cancelled(&mut cancelled)?;
        let authenticate_outputs = !self.has_encoding_successor() && !self.shape_outputs_verified;
        let installed_intent = read_installed_shape_intent(&self.root, &self.checkpoint)?;
        if installed_intent
            .as_ref()
            .is_some_and(|intent| intent.complete)
        {
            if let Some((shape, work)) =
                read_completed_shape(&self.root, &self.checkpoint, authenticate_outputs)?
            {
                self.shape_outputs_verified |= authenticate_outputs;
                // The completed-shape replay boundary re-verifies the retained
                // payloads (#1392). Charge that read where every other replay and
                // recovery authentication is charged; `shaped_output_authentication_bytes`
                // is bound to the shape-phase sum and is not a replay counter.
                self.checkpoint.evidence.recovery_application_read_bytes = self
                    .checkpoint
                    .evidence
                    .recovery_application_read_bytes
                    .checked_add(work.bytes)
                    .ok_or_else(|| storage("shape replay authentication bytes overflow"))?;
                self.checkpoint
                    .evidence
                    .recovery_application_read_operations = self
                    .checkpoint
                    .evidence
                    .recovery_application_read_operations
                    .checked_add(work.operations)
                    .ok_or_else(|| storage("shape replay authentication operations overflow"))?;
                account_cache_release(work.cache_release, &mut self.checkpoint.evidence)?;
                // A replay on the same session object converges exactly as a
                // reopen does: collect any segment the shape-end retirement
                // window left behind before supersession inspects it (#1526).
                discard_completed_shape_segments(&self.root, &self.checkpoint.evidence)?;
                self.reclaim_superseded_payloads_cancellable(&mut cancelled)?;
                return Ok(shape);
            }
            return Err(storage("incomplete construction shape was not recovered"));
        }
        if self.shape_finish_interrupted {
            return Err(storage("incomplete construction shape was not recovered"));
        }
        // An incomplete shape resumes: sealed segment groups survive behind the
        // progress chain, and the staged inputs behind those boundaries are
        // already retired (#1418).
        let resume = match self.shape_resume.take() {
            Some(resume) => Some(resume),
            None => self.begin_shape_resume(installed_intent, &mut cancelled)?,
        };
        let start_sequence = resume.as_ref().map_or(0, |state| state.retired_through);
        if resume.is_none() {
            reject_existing_merge_artifacts(&self.root)?;
        }

        // Snapshot the committed evidence before any shaping work, including the
        // splitter sampling pass: the shape intent's baseline must match what a
        // reopen reads back from the checkpoint on disk. A resumed shape keeps
        // the baseline its installed intent recorded.
        let baseline_evidence = resume.as_ref().map_or_else(
            || self.checkpoint.evidence.clone(),
            |state| state.baseline_evidence.clone(),
        );
        let detail_codec =
            DetailCodec::from_version(self.checkpoint.format_version).map_err(storage)?;
        // Barrier A0. Choose the range-partition splitters from a deterministic
        // sample of the staged identity domain, and record them before any
        // partition writes a byte. `super::partition` records why a formula
        // over the key's high bits is not an option for UUIDv7 identities.
        // A resumed shape replays the recorded splitters instead: the chunks
        // behind the resumed boundary are retired, so their sampling domain is
        // gone, and the recorded intent is the authority anyway.
        let planning = crate::concurrency_attribution::RegionScope::named("shape_planning");
        let (plan, node_plan) = if let Some(state) = &resume {
            (
                PartitionPlan::from_recorded(
                    self.checkpoint.budgets.partition_count,
                    state.splitters.clone(),
                )?,
                PartitionPlan::from_recorded(
                    self.checkpoint.budgets.partition_count,
                    state.node_splitters.clone(),
                )?,
            )
        } else {
            (
                self.choose_partition_plan(&mut cancelled)?,
                self.choose_node_partition_plan(&mut cancelled)?,
            )
        };
        drop(planning);
        let partitions = plan.partitions();
        self.checkpoint.evidence.shape_partitions = u64::try_from(partitions).map_err(storage)?;
        self.checkpoint.evidence.shape_partition_count = u64::from(plan.partition_count());
        // Barrier A0'. A second splitter set over the node-only domain
        // (#1439): endpoints, node details and node-kind rows are keyed by
        // node UUID, and routing them with the joint `plan` above skews them
        // badly once nodes and edges occupy disjoint UUID bands. `node_plan`
        // has at most as many partitions as `plan` (its domain is a subset),
        // so every family below is constructed with the partition count of
        // whichever plan actually routes it. This matters beyond bounds
        // safety: `PartitionBalance`'s mean is `total / partitions`, so
        // sizing a node-keyed family's spill/balance vectors by the joint
        // plan's (larger) partition count would silently dilute its mean
        // with slots that routing through `node_plan` can never reach,
        // making a perfectly balanced node-keyed family look skewed.
        let node_partitions = node_plan.partitions();
        self.checkpoint.evidence.shape_node_partitions =
            u64::try_from(node_partitions).map_err(storage)?;
        let mut identities = FixedRangePartitioner::<BASE_IDENTITY_WIDTH>::new(
            &self.root,
            PartitionFamily::Identities,
            partitions,
            None,
            true,
        )?
        .with_materialization_limit(self.checkpoint.budgets.max_partition_bytes)
        .with_external_partitions(self.checkpoint.budgets.max_external_partition_bytes)
        .with_cpu_admission(self.cpu_admission.clone());
        let mut node_details = FixedRangePartitioner::<NODE_DETAIL_WIDTH>::new(
            &self.root,
            PartitionFamily::NodeDetails,
            node_partitions,
            Some(detail_codec),
            false,
        )?
        .with_materialization_limit(self.checkpoint.budgets.max_partition_bytes)
        .with_external_partitions(self.checkpoint.budgets.max_external_partition_bytes)
        .with_cpu_admission(self.cpu_admission.clone());
        let mut edge_details = FixedRangePartitioner::<EDGE_DETAIL_WIDTH>::new(
            &self.root,
            PartitionFamily::EdgeDetails,
            partitions,
            Some(detail_codec),
            false,
        )?
        .with_materialization_limit(self.checkpoint.budgets.max_partition_bytes)
        .with_external_partitions(self.checkpoint.budgets.max_external_partition_bytes)
        .with_cpu_admission(self.cpu_admission.clone());
        let mut endpoints = FixedRangePartitioner::<ENDPOINT_WIDTH>::new(
            &self.root,
            PartitionFamily::Endpoints,
            node_partitions,
            None,
            false,
        )?
        .with_materialization_limit(self.checkpoint.budgets.max_partition_bytes)
        .with_external_partitions(self.checkpoint.budgets.max_external_partition_bytes)
        .with_cpu_admission(self.cpu_admission.clone());
        let mut row_groups: BTreeMap<(u8, String), RowRangePartitioner> = BTreeMap::new();
        // #1455. A kind whose every staged chunk carries the bare canonical
        // schema has no property columns, so its shaped rows would hold
        // exactly the identity plus label (or endpoints plus route) that its
        // details family already holds, in the same UUID order. The rows'
        // only consumers are the runtime-catalog scan and the encoder's
        // property projection, and the latter discards a property-free batch
        // on sight. Skip the row spills and the rows Parquet finish for such
        // a kind and derive its catalog observations from the details family
        // instead. A kind with any property-bearing chunk, including one
        // that mixes bare and property schemas, keeps the row path unchanged:
        // its rows are grouped by exact schema before UUID, and the catalog's
        // intern order must follow that grouping.
        let node_catalog_from_details = catalog_derives_from_details(
            &self.checkpoint.node_schema_sha256,
            ConstructionChunkKind::Node,
        );
        let edge_catalog_from_details = catalog_derives_from_details(
            &self.checkpoint.edge_schema_sha256,
            ConstructionChunkKind::Edge,
        );
        if let Some(state) = &resume {
            // Rebuild every family from its claimed sealed segments before the
            // loop resumes. Row groups for schemas whose chunks all retired
            // behind the boundary would otherwise never be constructed, and
            // their segments would be orphaned at finish.
            identities.restore(
                state.segments_for("identities"),
                &state.rows_for("identities", partitions),
            )?;
            node_details.restore(
                state.segments_for("node-details"),
                &state.rows_for("node-details", node_partitions),
            )?;
            edge_details.restore(
                state.segments_for("edge-details"),
                &state.rows_for("edge-details", partitions),
            )?;
            endpoints.restore(
                state.segments_for("endpoints"),
                &state.rows_for("endpoints", node_partitions),
            )?;
            for (kind, schemas, from_details, routing_plan, routing_partitions) in [
                (
                    0_u8,
                    &self.checkpoint.node_schema_sha256,
                    node_catalog_from_details,
                    &node_plan,
                    node_partitions,
                ),
                (
                    1_u8,
                    &self.checkpoint.edge_schema_sha256,
                    edge_catalog_from_details,
                    &plan,
                    partitions,
                ),
            ] {
                if from_details {
                    continue;
                }
                for digest in schemas {
                    let authority = format!("{kind}-{digest}");
                    let tag = format!("rows:{}", &sha256(authority.as_bytes())[..16].to_owned());
                    let claimed = state.segments_for(&tag);
                    if claimed.is_empty() {
                        continue;
                    }
                    let mut partitioner = RowRangePartitioner::new(
                        &self.root,
                        &authority,
                        routing_plan.partitions(),
                    )?
                    .with_materialization_limit(self.checkpoint.budgets.max_partition_bytes);
                    partitioner.restore(claimed, &state.rows_for(&tag, routing_partitions))?;
                    if let Some(schema) = state.row_schemas.get(&tag) {
                        partitioner.restore_schema(schema.clone());
                    }
                    row_groups.insert((kind, digest.clone()), partitioner);
                }
            }
        }
        let mut catalog_authority = Sha256::new();
        let mut shape_intent = ShapeIntent {
            format_version: self.checkpoint.format_version,
            operation_uuid: self.checkpoint.operation_uuid,
            project_identity: self.checkpoint.project_identity.clone(),
            session_identity: self.checkpoint.session_identity.clone(),
            parent_topology_generation: self.checkpoint.parent_topology_generation,
            ontology_mode: self.checkpoint.ontology_mode,
            semantic_authority_sha256: self.checkpoint.semantic_authority_sha256.clone(),
            budgets: self.checkpoint.budgets,
            last_receipt_sha256: self.checkpoint.last_receipt_sha256.clone(),
            baseline_evidence,
            final_evidence: None,
            complete: false,
            shape: None,
            outputs: Vec::new(),
            shape_authority_sha256: None,
            splitters: encode_splitters(&plan),
            node_splitters: encode_splitters(&node_plan),
            partition_identity_rows: Vec::new(),
        };
        if resume.is_none() {
            install_shape_intent(&self.root, &mut shape_intent)?;
        }
        // Sealing cadence (#1418): retired staged input is the point of the
        // boundary machinery, but every boundary also costs one fsync per
        // open spill. Space boundaries so each spill is fsynced once per
        // 255 KiB of newly routed input, the same bytes-per-fsync budget the
        // write path established in #1442.
        let mut routed_since_boundary = 0_u64;
        let mut sealed_through = start_sequence;
        let mut last_progress_sha256 = resume
            .as_ref()
            .and_then(|state| state.last_progress_sha256.clone());
        let mut previous_rows: BTreeMap<String, Vec<u64>> = resume
            .as_ref()
            .map(|state| state.segment_rows.clone())
            .unwrap_or_default();
        let routing = crate::concurrency_attribution::RegionScope::named("shape_routing");
        for sequence in start_sequence..self.checkpoint.next_sequence {
            reject_cancelled(&mut cancelled)?;
            let receipt = self.read_receipt(sequence)?;
            // Fixed-width inputs authenticate their exact inode, length and
            // digest in the partition routers below. Parquet's range-oriented
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
            let group = (kind, receipt.schema_sha256.clone());
            // Row groups are keyed by their own kind's UUID (#1439): a
            // node-kind schema group's rows are keyed by node UUID and must
            // be routed by `node_plan`, matching node details and endpoints
            // below -- sized to `node_partitions`, for the same balance-mean
            // reason those two are. Edge-kind groups stay on the joint
            // `plan`, which their key domain already matches well since
            // edges dominate it.
            let (row_plan, route_rows) = match receipt.kind {
                ConstructionChunkKind::Node => (&node_plan, !node_catalog_from_details),
                ConstructionChunkKind::Edge => (&plan, !edge_catalog_from_details),
            };
            if route_rows {
                if !row_groups.contains_key(&group) {
                    row_groups.insert(
                        group.clone(),
                        RowRangePartitioner::new(
                            &self.root,
                            &format!("{kind}-{}", receipt.schema_sha256),
                            row_plan.partitions(),
                        )?
                        .with_materialization_limit(self.checkpoint.budgets.max_partition_bytes),
                    );
                }
                row_groups
                    .get_mut(&group)
                    .ok_or_else(|| storage("row partitioner is absent"))?
                    .push(
                        row_plan,
                        &receipt.parquet.name,
                        self.checkpoint.budgets.max_batch_rows,
                        &mut cancelled,
                        &mut self.checkpoint.evidence,
                    )?;
            }
            route_identity_run(
                &self.root,
                &plan,
                &receipt,
                &mut identities,
                &mut cancelled,
                &mut self.checkpoint.evidence,
            )?;
            match receipt.kind {
                ConstructionChunkKind::Node => {
                    route_fixed_run::<NODE_DETAIL_WIDTH>(
                        &self.root,
                        &node_plan,
                        &receipt.details,
                        Some(detail_codec),
                        &mut node_details,
                        &mut cancelled,
                        &mut self.checkpoint.evidence,
                    )?;
                }
                ConstructionChunkKind::Edge => {
                    route_fixed_run::<EDGE_DETAIL_WIDTH>(
                        &self.root,
                        &plan,
                        &receipt.details,
                        Some(detail_codec),
                        &mut edge_details,
                        &mut cancelled,
                        &mut self.checkpoint.evidence,
                    )?;
                    // Endpoints are keyed by the referenced *node*'s UUID at
                    // this staged, pre-resolution stage (#1439) -- the later
                    // resolved endpoints, re-keyed by edge UUID in
                    // `resolve_endpoint_surrogates`, correctly keep using
                    // `plan`.
                    route_fixed_run::<ENDPOINT_WIDTH>(
                        &self.root,
                        &node_plan,
                        receipt
                            .endpoints
                            .as_ref()
                            .ok_or_else(|| storage("edge receipt lacks endpoint run"))?,
                        None,
                        &mut endpoints,
                        &mut cancelled,
                        &mut self.checkpoint.evidence,
                    )?;
                }
            }
            routed_since_boundary = routed_since_boundary
                .checked_add(staged_input_bytes(&receipt))
                .ok_or_else(|| storage("routed input byte count overflows"))?;
            let open_spills = identities.open_spill_count()
                + node_details.open_spill_count()
                + edge_details.open_spill_count()
                + endpoints.open_spill_count()
                + row_groups
                    .values()
                    .map(RowRangePartitioner::open_spill_count)
                    .sum::<usize>();
            // Once any boundary has retired staged input, the last chunk
            // always closes one (#1562): every routed row is then held by a
            // claimed segment before a finish stage may retire a family, so a
            // resumed shape never has to re-route into a family whose
            // segments are already gone.
            let completes_routing = sealed_through > 0
                && sequence.checked_add(1) == Some(self.checkpoint.next_sequence);
            if !completes_routing && routed_since_boundary < boundary_threshold(open_spills) {
                continue;
            }
            let boundary = sequence
                .checked_add(1)
                .ok_or_else(|| storage("shape boundary overflows"))?;
            let mut partition_rows = Vec::new();
            capture_balance_delta(
                super::progress::IDENTITY_TAG,
                identities.balance(),
                &mut previous_rows,
                &mut partition_rows,
            );
            capture_balance_delta(
                "node-details",
                node_details.balance(),
                &mut previous_rows,
                &mut partition_rows,
            );
            capture_balance_delta(
                "edge-details",
                edge_details.balance(),
                &mut previous_rows,
                &mut partition_rows,
            );
            capture_balance_delta(
                "endpoints",
                endpoints.balance(),
                &mut previous_rows,
                &mut partition_rows,
            );
            for ((kind, digest), partitioner) in &row_groups {
                let authority = format!("{kind}-{digest}");
                let tag = format!("rows:{}", &sha256(authority.as_bytes())[..16].to_owned());
                capture_balance_delta(
                    &tag,
                    partitioner.balance(),
                    &mut previous_rows,
                    &mut partition_rows,
                );
            }
            // One directory-durability batch covers the whole group's seal
            // (#1452); every spill payload fsynced before its name landed in
            // the batch, so the flush is what makes the boundary's segments
            // durable together.
            let mut batch = SealDirectoryBatch::new(&self.root);
            identities.seal_at_boundary(boundary, &mut self.checkpoint.evidence, &mut batch)?;
            node_details.seal_at_boundary(boundary, &mut self.checkpoint.evidence, &mut batch)?;
            edge_details.seal_at_boundary(boundary, &mut self.checkpoint.evidence, &mut batch)?;
            endpoints.seal_at_boundary(boundary, &mut self.checkpoint.evidence, &mut batch)?;
            for partitioner in row_groups.values_mut() {
                partitioner.seal_at_boundary(
                    boundary,
                    &mut self.checkpoint.evidence,
                    &mut batch,
                )?;
            }
            construction_failpoint("shape.partition_spill.before_flush");
            batch.flush(&mut self.checkpoint.evidence)?;
            // The boundary control lands before any unlink: from here on the
            // group's inputs have durable sealed successors, so an interrupted
            // retirement never strands routed data behind missing inputs.
            let progress = super::progress::ShapeProgress::new(
                &self.checkpoint,
                boundary,
                last_progress_sha256.clone(),
                partition_rows,
            );
            last_progress_sha256 = Some(install_shape_progress(&self.root, &progress)?);
            construction_failpoint("shape.after_group_seal");
            for retire_sequence in sealed_through..boundary {
                let receipt = self.read_receipt(retire_sequence)?;
                for artifact in [&receipt.parquet, &receipt.identities, &receipt.details]
                    .into_iter()
                    .chain(receipt.endpoints.iter())
                {
                    retire_staged_payload(
                        &self.root,
                        &mut self.checkpoint.evidence,
                        artifact,
                        false,
                        false,
                        &mut cancelled,
                    )?;
                }
            }
            sealed_through = boundary;
            routed_since_boundary = 0;
            self.shape_boundary_retired_through = boundary;
            construction_failpoint("shape.after_group_retire");
        }
        // Load-bearing, not a nicety: a collapsed one-partition run is
        // perfectly deterministic and passes every byte-equality test, so this
        // is the only check that can tell a working range partition from a
        // catastrophically skewed one.
        drop(routing);
        identities.balance().assert_balanced("staged identity")?;
        let partition_identity_rows = identities.balance().rows().to_vec();
        self.checkpoint.evidence.max_partition_identity_rows = identities.balance().max_rows();
        self.checkpoint.evidence.partitioned_identity_rows = identities.balance().total();
        let final_boundary = self.checkpoint.next_sequence;
        // Segments are the resume state only while staged inputs have been
        // retired behind a progress boundary; a boundary-less shape frees
        // them at finish exactly as the per-family finish always did (#1418).
        let retain_segments = sealed_through > 0;
        // Finish stages hand each family's authority from its segments to its
        // installed output as soon as that output exists (#1562). Only a
        // shape that retired staged input needs them; a boundary-less shape
        // frees its segments at each finish, as it always did.
        let mut stages = if retain_segments {
            let resumed = resume.map(|state| state.stages).unwrap_or_default();
            Some(if resumed.has_stages() {
                resumed
            } else {
                ShapeStages::after_progress(
                    last_progress_sha256
                        .clone()
                        .ok_or_else(|| storage("retained shape segments lack a progress head"))?,
                )
            })
        } else {
            None
        };
        self.shape_finish_interrupted = stages.is_some();
        let family_finish =
            crate::concurrency_attribution::RegionScope::named("shape_family_finish");
        let staged_identities = finish_family_stage(
            &self.root,
            &mut self.checkpoint,
            stages.as_mut(),
            ShapeStageKind::Identities,
            identities,
            STAGED_IDENTITIES,
            final_boundary,
            self.cpu_admission.as_ref(),
            &mut cancelled,
        )?
        .ok_or_else(|| storage("construction contains no identities"))?;
        let node_details = finish_family_stage(
            &self.root,
            &mut self.checkpoint,
            stages.as_mut(),
            ShapeStageKind::NodeDetails,
            node_details,
            SHAPED_NODE_DETAILS,
            final_boundary,
            self.cpu_admission.as_ref(),
            &mut cancelled,
        )?;
        let edge_details = finish_family_stage(
            &self.root,
            &mut self.checkpoint,
            stages.as_mut(),
            ShapeStageKind::EdgeDetails,
            edge_details,
            SHAPED_EDGE_DETAILS,
            final_boundary,
            self.cpu_admission.as_ref(),
            &mut cancelled,
        )?;
        let endpoints = finish_family_stage(
            &self.root,
            &mut self.checkpoint,
            stages.as_mut(),
            ShapeStageKind::Endpoints,
            endpoints,
            STAGED_ENDPOINTS,
            final_boundary,
            self.cpu_admission.as_ref(),
            &mut cancelled,
        )?;
        drop(family_finish);
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
        let assigned = stages
            .as_ref()
            .and_then(|stages| stages.completed(ShapeStageKind::Assigned))
            .map(|stage| (stage.new_nodes, stage.new_edges));
        let assignment = crate::concurrency_attribution::RegionScope::named("surrogate_assignment");
        let (identities, new_nodes, new_edges) = if let Some((new_nodes, new_edges)) = assigned {
            // Validation, base-conflict rejection and assignment all ran
            // before the interruption; the recorded successor carries them.
            (SHAPED_IDENTITIES.to_owned(), new_nodes, new_edges)
        } else {
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
            if let Some(stages) = stages.as_mut() {
                stages.record(
                    &self.root,
                    &self.checkpoint,
                    ShapeStageKind::Assigned,
                    StageResult {
                        outputs: vec![receipt_for_existing(&self.root, &identities)?],
                        new_nodes,
                        new_edges,
                        ..StageResult::default()
                    },
                )?;
                construction_failpoint("shape.stage.assigned.after_install");
            }
            // Original chunks remain recovery authority until shaping
            // completes. The assigned identity successor now owns every later
            // identity consumer.
            shape_publication_failure("shape.before_identity_retirement")?;
            unlink_shape_artifact(
                &self.root,
                &staged_identities,
                &mut self.checkpoint.evidence,
            )?;
            construction_failpoint("shape.after_identity_retirement");
            shape_publication_failure("shape.after_identity_retirement")?;
            (identities, new_nodes, new_edges)
        };
        drop(assignment);
        reject_cancelled(&mut cancelled)?;
        let resolution = crate::concurrency_attribution::RegionScope::named("endpoint_resolution");
        let edge_endpoints = resolve_endpoint_stages(
            &self.root,
            &mut self.checkpoint,
            stages.as_mut(),
            &plan,
            &identities,
            endpoints.as_deref(),
            self.base_snapshot.as_mut(),
            final_boundary,
            retain_segments,
            self.cpu_admission.as_ref(),
            &mut cancelled,
        )?;
        drop(resolution);
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
        let row_finish = crate::concurrency_attribution::RegionScope::named("shape_row_finish");
        let mut node_rows = Vec::new();
        let mut edge_rows = Vec::new();
        for ((kind, schema_digest), rows) in row_groups {
            reject_cancelled(&mut cancelled)?;
            let output = format!("shaped-rows-{kind}-{schema_digest}.parquet");
            let output = rows.finish(
                &output,
                final_boundary,
                retain_segments,
                self.checkpoint.budgets.max_batch_rows,
                self.checkpoint.budgets.max_batch_bytes,
                &mut cancelled,
                &mut self.checkpoint.evidence,
            )?;
            if kind == 0 {
                node_rows.push(output);
            } else {
                edge_rows.push(output);
            }
        }
        drop(row_finish);
        let catalog_region = crate::concurrency_attribution::RegionScope::named("runtime_catalog");
        let runtime_catalog = build_runtime_catalog(
            self.parent_catalog.clone(),
            &self.root,
            if node_catalog_from_details {
                CatalogSource::Details(node_details.as_deref())
            } else {
                CatalogSource::Rows(&node_rows)
            },
            if edge_catalog_from_details {
                CatalogSource::Details(edge_details.as_deref())
            } else {
                CatalogSource::Rows(&edge_rows)
            },
            detail_codec,
            self.checkpoint.budgets,
            &mut cancelled,
            &mut self.checkpoint.evidence,
        )?;
        drop(catalog_region);
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
        let _completion = crate::concurrency_attribution::RegionScope::named("shape_completion");
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
        // Boundary-retained segments (#1418) have no consumer left: every
        // family's output is installed and authenticated above. Charge their
        // removal here so the complete inventory and the shape-end checkpoint
        // both record the post-retirement ledger; a durable control whose
        // allocation map carried one entry per sealed segment grew with routed
        // input and exceeded its bound (#1526). The payloads follow the
        // complete inventory, so a crash in between leaves them for recovery
        // rather than stranding a resumable shape with no segments.
        let retained_segments = retained_shape_segments(&self.root, &mut cancelled)?;
        reconcile_retained_shape_segments(&mut self.checkpoint.evidence, &retained_segments)?;
        replace_shape_intent(
            &self.root,
            &mut ShapeIntent {
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
                splitters: shape_intent.splitters,
                node_splitters: shape_intent.node_splitters,
                partition_identity_rows,
            },
        )?;
        construction_failpoint("shape.after_complete_inventory");
        unlink_reconciled_shape_segments(&self.root, &retained_segments, &mut cancelled)?;
        construction_failpoint("shape.after_segment_discard");
        replace_checkpoint_control(&self.root, &self.checkpoint)?;
        construction_failpoint("shape.after_evidence_checkpoint");
        self.shape_finish_interrupted = false;
        self.reclaim_superseded_payloads_cancellable(&mut cancelled)?;
        crate::concurrency_attribution::RegionScope::record_work("nodes", shape.node_count);
        crate::concurrency_attribution::RegionScope::record_work("edges", shape.edge_count);
        Ok(shape)
    }

    /// Build the resume state for an interrupted shape, or `None` when no
    /// progress chain exists.
    ///
    /// The boundary chain must already be the recovery-cleaned state: sealed
    /// segments at or below the head boundary are claimed and authenticated
    /// here once per process, everything else was removed, and the installed
    /// incomplete intent supplies the recorded splitters the resumed routing
    /// replays.
    fn begin_shape_resume(
        &mut self,
        installed_intent: Option<ShapeIntent>,
        cancelled: &mut impl FnMut() -> bool,
    ) -> Result<Option<super::progress::ShapeResume>, GfError> {
        let chain = load_shape_progress_chain(&self.root, &self.checkpoint)?;
        let Some(head) = chain.last() else {
            if installed_intent.is_some() {
                // Recovery removes an incomplete intent that carries no
                // progress; finding one here means durable state this session
                // cannot reason about.
                return Err(storage("incomplete construction shape was not recovered"));
            }
            return Ok(None);
        };
        let Some(intent) = installed_intent else {
            return Err(storage(
                "construction shape progress exists without shape intent",
            ));
        };
        let retired_through = head.retired_through();
        let last_progress_sha256 = head.body_sha256.clone();
        self.shape_boundary_retired_through = retired_through;
        let stages = super::finish_stages::load_shape_stages(&self.root, &self.checkpoint, &chain)?;
        let mut segments = scan_shape_segments(
            &self.root,
            retired_through,
            &stages,
            &mut self.checkpoint.evidence,
            cancelled,
        )?;
        // Reconcile the allocation ledger for everything the boundary chain
        // retired. The durable checkpoint predates those removals (they were
        // in-flight when the process died), and leaving their inode entries
        // in the active map would let a reused inode clobber a stale entry
        // and read as corruption at supersession. This is the same
        // accounting `retire_payload` performs; the files are already gone
        // except for a marker-to-unlink crash window, where it unlinks them.
        for reconcile_sequence in 0..retired_through {
            let receipt = self.read_receipt(reconcile_sequence)?;
            for artifact in [&receipt.parquet, &receipt.identities, &receipt.details]
                .into_iter()
                .chain(receipt.endpoints.iter())
            {
                retire_staged_payload(
                    &self.root,
                    &mut self.checkpoint.evidence,
                    artifact,
                    false,
                    false,
                    cancelled,
                )?;
            }
        }
        let mut claimed_names: std::collections::BTreeSet<String> = segments
            .values()
            .flatten()
            .flat_map(|partition| partition.iter().map(|receipt| receipt.name.clone()))
            .collect();
        claimed_names.extend(stages.live_output_names());
        // A resumed shape tolerates exactly its claimed segments. Any other
        // shape-scoped artifact — a derived domain from an interrupted finish,
        // an unexpected spill — has no producer on the resumed path and would
        // collide with the re-run's deterministic output.
        for child in self.root.child_names().map_err(storage)? {
            let Some(name) = child.to_str() else { continue };
            if !is_shape_scoped_name(name) || claimed_names.contains(name) {
                continue;
            }
            return Err(storage("unowned construction shaping artifact exists"));
        }
        let segment_rows = LoadedShapeProgress::cumulative_rows(&chain)?;
        let row_schemas = authenticate_shape_segments(
            &self.root,
            &segments,
            &mut self.checkpoint.evidence,
            cancelled,
        )?;
        super::finish_stages::authenticate_stage_outputs(
            &self.root,
            &stages,
            &mut self.checkpoint.evidence,
            cancelled,
        )?;
        // Once authenticated and ledger-installed, the claimed receipts are
        // the restored state; drop the empties so restore sees only families
        // with real segments.
        segments.retain(|_, partitions| partitions.iter().any(|partition| !partition.is_empty()));
        Ok(Some(super::progress::ShapeResume {
            retired_through,
            baseline_evidence: intent.baseline_evidence,
            splitters: decode_splitters(&intent.splitters)?,
            node_splitters: decode_splitters(&intent.node_splitters)?,
            segments,
            segment_rows,
            row_schemas,
            last_progress_sha256: Some(last_progress_sha256),
            stages,
        }))
    }
}

/// Read the installed shape intent, whatever its completion state.
fn read_installed_shape_intent(
    root: &StableDirectory,
    checkpoint: &Checkpoint,
) -> Result<Option<ShapeIntent>, GfError> {
    let mut file = match root.open_child_file(OsStr::new(SHAPE_INTENT)) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(storage(error)),
    };
    let intent = decode_shape_intent(&mut file)?;
    validate_shape_binding(&intent, checkpoint)?;
    Ok(Some(intent))
}

/// Staged input bytes one chunk's routing consumes, the cadence counter's
/// unit.
fn staged_input_bytes(receipt: &ConstructionChunkReceipt) -> u64 {
    let mut total = receipt
        .parquet
        .bytes
        .saturating_add(receipt.identities.bytes)
        .saturating_add(receipt.details.bytes);
    if let Some(artifact) = &receipt.endpoints {
        total = total.saturating_add(artifact.bytes);
    }
    total
}

/// The routed-input byte count that justifies one sealing boundary at
/// `open_spills` open spills.
fn boundary_threshold(open_spills: usize) -> u64 {
    const SEAL_SPACING_BYTES: u64 = 255 * 1024;
    (open_spills as u64)
        .saturating_mul(SEAL_SPACING_BYTES)
        .max(1)
}

/// Record one partitioner's routing delta since the previous boundary.
fn capture_balance_delta(
    tag: &str,
    balance: &PartitionBalance,
    previous: &mut BTreeMap<String, Vec<u64>>,
    out: &mut Vec<ShapeProgressPartition>,
) {
    let current = balance.rows();
    let prior = previous
        .entry(tag.to_owned())
        .or_insert_with(|| vec![0; current.len()]);
    if prior.len() != current.len() {
        prior.resize(current.len(), 0);
    }
    let delta: Vec<u64> = current
        .iter()
        .zip(prior.iter())
        .map(|(count, prior)| count - prior)
        .collect();
    *prior = current.to_vec();
    if delta.iter().any(|rows| *rows != 0) {
        out.push(ShapeProgressPartition {
            tag: tag.to_owned(),
            rows: delta,
        });
    }
}

/// Whether every staged chunk of `kind` carried the bare canonical schema
/// (#1455). An empty set -- no chunk of that kind -- trivially qualifies and
/// has no rows to route either way.
fn catalog_derives_from_details(
    schema_sha256: &std::collections::BTreeSet<String>,
    kind: ConstructionChunkKind,
) -> bool {
    schema_sha256
        .iter()
        .all(|digest| digest == property_free_schema_sha256(kind))
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
    // The recorded splitters are the range-partition authority. Refuse an
    // intent whose splitters are not a well-formed monotone partition of the
    // key space: a partitioning that is not a range partition would make the
    // concatenated shape silently unordered.
    PartitionPlan::from_recorded(
        checkpoint.budgets.partition_count,
        decode_splitters(&intent.splitters)?,
    )?;
    // The node-only splitters (#1439) are the same kind of authority, over
    // the node-keyed families' own domain.
    PartitionPlan::from_recorded(
        checkpoint.budgets.partition_count,
        decode_splitters(&intent.node_splitters)?,
    )?;
    Ok(())
}

pub(super) fn read_completed_shape(
    root: &StableDirectory,
    checkpoint: &Checkpoint,
    authenticate_outputs: bool,
) -> Result<Option<(ConstructionShape, ReadWork)>, GfError> {
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
    let mut work = ReadWork::default();
    if authenticate_outputs {
        for output in &manifest.outputs {
            let observed = authenticate_shaped_output(root, output)?;
            work.bytes = work
                .bytes
                .checked_add(observed.bytes)
                .ok_or_else(|| storage("shaped output authentication bytes overflow"))?;
            work.operations = work
                .operations
                .checked_add(observed.operations)
                .ok_or_else(|| storage("shaped output authentication operations overflow"))?;
            merge_cache_release_evidence(&mut work.cache_release, observed.cache_release)?;
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
    Ok(Some((shape, work)))
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
    persist_shape_receipt_with(root, receipt, &mut |root, target, value| {
        install_control(root, target, value)
    })
}

/// [`persist_shape_receipt`] whose containing-directory durability is
/// provided by the seal batch's flush instead of two per-call directory
/// syncs (#1452). The receipt body is fsynced before its name is linked;
/// a crash before the flush loses at most the name, which the incomplete
/// shape cleanup already tolerates.
pub(super) fn persist_shape_receipt_in_batch(
    root: &StableDirectory,
    receipt: &ArtifactReceipt,
    batch: &mut SealDirectoryBatch,
) -> Result<(), GfError> {
    persist_shape_receipt_with(root, receipt, &mut |root, target, value| {
        install_control_batched(root, target, value, batch)
    })
}

fn persist_shape_receipt_with(
    root: &StableDirectory,
    receipt: &ArtifactReceipt,
    install: &mut dyn FnMut(&StableDirectory, &str, &ArtifactReceipt) -> Result<(), GfError>,
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
                install(root, &capability_name, receipt)?;
            }
            Err(error) => return Err(storage(error)),
        }
    }
    Ok(())
}

/// Identity-only authority check for a shaped output: name grammar, inode,
/// link count, length and the recorded digest.
///
/// This establishes that the manifest still names *this* file. It cannot see a
/// payload mutation that preserves inode and length, which is exactly the
/// defect #1269 recorded, so it is used only where the artifact is about to be
/// removed and its bytes are never consumed again. Every trust boundary that
/// consumes or replays a shaped output uses [`authenticate_shaped_output`].
pub(super) fn authenticate_shaped_output_identity(
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

/// Authenticate a completed shape output at a trust boundary: the identity
/// check above, **plus** the recorded payload checksum over the bytes that are
/// on disk right now.
///
/// The writer-receipt fast path compares stored metadata, so on its own it
/// accepts a same-inode, same-length payload mutation of a *completed* shape
/// output (#1392, the same defect class as #1269). The refusal used to arrive
/// incidentally, from the full SHA-256 that `retire_payload` performed just
/// before unlinking a payload; that pass is removed under #1384, so the
/// refusal is established here instead, deliberately and at the boundary.
///
/// The primitive is non-cryptographic on purpose. The threat is byte mutation,
/// not a forged digest; see [`crate::corruption_checksum`] for the two
/// assumptions that permits and what invalidates them.
pub(super) fn authenticate_shaped_output(
    root: &StableDirectory,
    expected: &ArtifactReceipt,
) -> Result<ReadWork, GfError> {
    authenticate_shaped_output_identity(root, expected)?;
    verify_payload_checksum(root, expected)
}

/// Stream a retained payload once and refuse it unless its length and
/// corruption checksum match the receipt the writer produced.
fn verify_payload_checksum(
    root: &StableDirectory,
    expected: &ArtifactReceipt,
) -> Result<ReadWork, GfError> {
    let file = root
        .open_child_file(OsStr::new(&expected.name))
        .map_err(storage)?;
    let mut reader = graphforge_filesystem::FileCacheReleasingReader::new(file).map_err(storage)?;
    let mut work = ReadWork::default();
    let mut checksum = crate::corruption_checksum::Checksum::new();
    let mut block = vec![0_u8; BLOCK_BYTES];
    let verified = (|| -> Result<(), GfError> {
        loop {
            let count = reader.read(&mut block).map_err(storage)?;
            if count == 0 {
                break;
            }
            checksum.update(&block[..count]);
            work.bytes = work
                .bytes
                .checked_add(count as u64)
                .ok_or_else(|| storage("shaped output size overflow"))?;
            work.operations = work
                .operations
                .checked_add(1)
                .ok_or_else(|| storage("shaped output read operation overflow"))?;
        }
        if work.bytes != expected.bytes
            || crate::corruption_checksum::hex(checksum.finish()) != expected.xxh64
        {
            return Err(storage("shape manifest output payload changed"));
        }
        Ok(())
    })();
    let released = reader.finish().map_err(storage);
    match (verified, released) {
        (Ok(()), Ok(released)) => {
            work.cache_release = released;
            Ok(work)
        }
        (Err(primary), Ok(_)) => Err(primary),
        (Ok(()), Err(error)) => Err(error),
        (Err(primary), Err(secondary)) => Err(storage(format!(
            "{primary}; shaped output cache release failed: {secondary}"
        ))),
    }
}

/// The durable shaping artifact grammar.
///
/// Range partitioning replaced the external merge tree, so the `merge-*`
/// families are gone: what remains is the shaped domain set, the two staged
/// domains that feed surrogate assignment and endpoint resolution, and the
/// per-partition spills.
pub(super) fn is_shape_artifact_name(name: &str) -> bool {
    if matches!(
        name,
        SHAPED_IDENTITIES
            | SHAPED_RUNTIME_CATALOG
            | SHAPED_NODE_DETAILS
            | SHAPED_EDGE_DETAILS
            | SHAPED_EDGE_ENDPOINTS
            | STAGED_IDENTITIES
            | STAGED_ENDPOINTS
    ) {
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
    is_partition_artifact_name(name)
}

pub(super) fn run_record_bytes<const N: usize>(
    record: &[u8; N],
    codec: Option<DetailCodec>,
) -> Result<&[u8], GfError> {
    match codec {
        Some(codec) => codec.wire(record).map_err(storage),
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

/// Stream one staged fixed-width run into its range partitions.
///
/// This replaces the copy-then-merge pair the shaping path used to run: the
/// staged run is authenticated against its writer receipt *while* its records
/// are routed, so the verification the copy used to perform survives without
/// the copy. No intermediate artifact is produced.
pub(super) fn route_fixed_run<const N: usize>(
    root: &StableDirectory,
    plan: &PartitionPlan,
    source: &ArtifactReceipt,
    codec: Option<DetailCodec>,
    target: &mut FixedRangePartitioner<N>,
    cancelled: &mut impl FnMut() -> bool,
    evidence: &mut GraphConstructionEvidence,
) -> Result<(), GfError> {
    let input = root
        .open_child_file(OsStr::new(&source.name))
        .map_err(storage)?;
    account_sequential_read(source.bytes, evidence)?;
    if !source
        .identity
        .matches(file_identity(&input).map_err(storage)?)
        || file_link_count(&input).map_err(storage)? != 1
    {
        return Err(storage("construction partition source authority changed"));
    }
    let counter = IoCounter::default();
    let cache_window =
        graphforge_filesystem::cache_release_window_for_streams(4).map_err(storage)?;
    let mut reader = BufReader::with_capacity(
        BLOCK_BYTES,
        CountingRead {
            inner: graphforge_filesystem::FileCacheReleasingReader::with_window_bytes(
                input,
                cache_window,
                graphforge_filesystem::FileCacheReleaseTracker::default(),
            )
            .map_err(storage)?,
            counter: counter.clone(),
        },
    );
    let mut digest = Sha256::new();
    let mut bytes = 0_u64;
    let routed = (|| -> Result<(), GfError> {
        let mut run = PartitionRun::new();
        while let Some(record) = read_run_record::<N>(&mut reader, codec)? {
            let wire = run_record_bytes(&record, codec)?;
            digest.update(wire);
            bytes = bytes
                .checked_add(wire.len() as u64)
                .ok_or_else(|| storage("partition source byte count overflows"))?;
            let key: [u8; 16] = record[..16].try_into().expect("fixed key prefix");
            let partition = plan.partition_of(&key);
            run.push(partition, wire, target, evidence)?;
            reject_cancelled(cancelled)?;
        }
        run.flush(target, evidence)?;
        if bytes != source.bytes || hex(&digest.clone().finalize()) != source.sha256 {
            return Err(storage("construction partition source content changed"));
        }
        account_fixed_read_operations(&counter, evidence)
    })();
    let released = release_counted_reader_cache(&mut reader, evidence);
    combine_cache_cleanup(routed, released, "construction partition source")
}

/// Maximum wire bytes retained by one routing accumulator.
const PARTITION_RUN_BYTES: usize = 64 * 1024;

/// Accumulates bounded same-partition slices of wire bytes, amortizing writes
/// without retaining a whole partition run (#1445).
///
/// The staged runs routed through [`route_fixed_run`] and
/// [`route_identity_run`] are UUID-sorted and the partition function is
/// monotone, so consecutive same-partition records are always contiguous
/// within one staged chunk. This is the same property
/// [`RowRangePartitioner::route_batch`] already exploits for Arrow rows;
/// this is its fixed-width-record equivalent.
struct PartitionRun {
    bound: usize,
    partition: Option<usize>,
    bytes: Vec<u8>,
    records: u64,
}

impl PartitionRun {
    fn new() -> Self {
        Self::with_bound(PARTITION_RUN_BYTES)
    }

    fn with_bound(bound: usize) -> Self {
        Self {
            bound,
            partition: None,
            bytes: Vec::new(),
            records: 0,
        }
    }

    fn push<const N: usize>(
        &mut self,
        partition: usize,
        wire: &[u8],
        target: &mut FixedRangePartitioner<N>,
        evidence: &mut GraphConstructionEvidence,
    ) -> Result<(), GfError> {
        if self.partition.is_some_and(|current| current != partition)
            || wire.len() > self.bound - self.bytes.len()
        {
            self.flush(target, evidence)?;
        }
        // Keep an oversized record whole without growing the accumulator.
        if wire.len() >= self.bound {
            return target.route_slice(partition, wire, 1, evidence);
        }
        if self.bytes.capacity() == 0 {
            self.bytes.reserve_exact(self.bound);
        }
        self.partition = Some(partition);
        self.bytes.extend_from_slice(wire);
        self.records = self
            .records
            .checked_add(1)
            .ok_or_else(|| storage("partition run record count overflows"))?;
        Ok(())
    }

    fn flush<const N: usize>(
        &mut self,
        target: &mut FixedRangePartitioner<N>,
        evidence: &mut GraphConstructionEvidence,
    ) -> Result<(), GfError> {
        if let Some(partition) = self.partition.take() {
            target.route_slice(partition, &self.bytes, self.records, evidence)?;
            self.bytes.clear();
            self.records = 0;
        }
        Ok(())
    }
}

/// Stream one staged identity run into its range partitions, widening each
/// UUID into the base identity record the shaped domain uses.
fn route_identity_run(
    root: &StableDirectory,
    plan: &PartitionPlan,
    receipt: &ConstructionChunkReceipt,
    target: &mut FixedRangePartitioner<BASE_IDENTITY_WIDTH>,
    cancelled: &mut impl FnMut() -> bool,
    evidence: &mut GraphConstructionEvidence,
) -> Result<(), GfError> {
    let source = &receipt.identities;
    let input = root
        .open_child_file(OsStr::new(&source.name))
        .map_err(storage)?;
    account_sequential_read(source.bytes, evidence)?;
    if !source
        .identity
        .matches(file_identity(&input).map_err(storage)?)
        || file_link_count(&input).map_err(storage)? != 1
    {
        return Err(storage(
            "identity source authority changed before partitioning",
        ));
    }
    let counter = IoCounter::default();
    let cache_window =
        graphforge_filesystem::cache_release_window_for_streams(4).map_err(storage)?;
    let mut reader = BufReader::with_capacity(
        BLOCK_BYTES,
        CountingRead {
            inner: graphforge_filesystem::FileCacheReleasingReader::with_window_bytes(
                input,
                cache_window,
                graphforge_filesystem::FileCacheReleaseTracker::default(),
            )
            .map_err(storage)?,
            counter: counter.clone(),
        },
    );
    let kind = u8::from(receipt.kind == ConstructionChunkKind::Edge);
    let mut digest = Sha256::new();
    let mut bytes = 0_u64;
    let routed = (|| -> Result<(), GfError> {
        let mut run = PartitionRun::new();
        while let Some(uuid) = read_fixed::<IDENTITY_WIDTH>(&mut reader)? {
            digest.update(uuid);
            bytes = bytes
                .checked_add(IDENTITY_WIDTH as u64)
                .ok_or_else(|| storage("identity source byte count overflows"))?;
            let mut record = [0_u8; BASE_IDENTITY_WIDTH];
            record[..16].copy_from_slice(&uuid);
            record[16] = kind;
            let wire = run_record_bytes(&record, None)?;
            let partition = plan.partition_of(&uuid);
            run.push(partition, wire, target, evidence)?;
            reject_cancelled(cancelled)?;
        }
        run.flush(target, evidence)?;
        if bytes != source.bytes || hex(&digest.clone().finalize()) != source.sha256 {
            return Err(storage(
                "identity source content changed before partitioning",
            ));
        }
        account_fixed_read_operations(&counter, evidence)
    })();
    let released = release_counted_reader_cache(&mut reader, evidence);
    combine_cache_cleanup(routed, released, "identity partition source")
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
    let output = SHAPED_IDENTITIES;
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
        xxh64: crate::corruption_checksum::hex(writer.get_ref().checksum.finish()),
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

/// Resolve every staged endpoint's node UUID to its surrogate and re-key the
/// result by edge UUID.
///
/// Resolution reads endpoints in node-UUID order but must emit them in
/// edge-UUID order, so the output is a genuine reordering. It is produced the
/// same way as every other shaped domain: route by the output key into range
/// partitions, sort each partition, concatenate. Nothing depends on the order
/// in which a result was produced, which is what makes this a deterministic
/// shuffle rather than an arrival-ordered one.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(super) fn resolve_endpoint_surrogates(
    root: &StableDirectory,
    plan: &PartitionPlan,
    identities_name: &str,
    endpoints_name: Option<&str>,
    base: Option<&mut AuthenticatedUuidIndexSnapshot>,
    window_rows: usize,
    max_partition_bytes: u64,
    max_external_partition_bytes: u64,
    boundary: u64,
    retain_segments: bool,
    cpu_admission: Option<&std::sync::Arc<super::cpu_admission::ConstructionCpuAdmission>>,
    cancelled: &mut impl FnMut() -> bool,
    evidence: &mut GraphConstructionEvidence,
) -> Result<Option<String>, GfError> {
    let Some(endpoints_name) = endpoints_name else {
        return Ok(None);
    };
    let mut resolved = FixedRangePartitioner::<RESOLVED_ENDPOINT_WIDTH>::new(
        root,
        PartitionFamily::Resolved,
        plan.partitions(),
        None,
        false,
    )?
    .with_materialization_limit(max_partition_bytes)
    .with_external_partitions(max_external_partition_bytes)
    .with_cpu_admission(cpu_admission.cloned());
    route_resolved_endpoints(
        root,
        plan,
        identities_name,
        endpoints_name,
        base,
        window_rows,
        &mut resolved,
        cancelled,
        evidence,
    )?;
    // Every routed record is durable before the endpoint domain is retired.
    resolved.seal(boundary, evidence)?;
    shape_publication_failure("shape.before_endpoint_retirement")?;
    unlink_shape_artifact(root, endpoints_name, evidence)?;
    construction_failpoint("shape.after_endpoint_retirement");
    shape_publication_failure("shape.after_endpoint_retirement")?;
    reject_cancelled(cancelled)?;
    resolved.finish_optional(
        SHAPED_EDGE_ENDPOINTS,
        boundary,
        retain_segments,
        cancelled,
        evidence,
    )
}

/// Route every staged endpoint, resolved to its node surrogate and re-keyed
/// by edge UUID, into `resolved`. Nothing is sealed here.
#[allow(clippy::too_many_arguments)]
fn route_resolved_endpoints(
    root: &StableDirectory,
    plan: &PartitionPlan,
    identities_name: &str,
    endpoints_name: &str,
    mut base: Option<&mut AuthenticatedUuidIndexSnapshot>,
    window_rows: usize,
    resolved: &mut FixedRangePartitioner<'_, RESOLVED_ENDPOINT_WIDTH>,
    cancelled: &mut impl FnMut() -> bool,
    evidence: &mut GraphConstructionEvidence,
) -> Result<(), GfError> {
    let (mut identities, identities_counter) =
        open_counted_fixed_reader(root, identities_name, evidence)?;
    let (mut endpoints, endpoints_counter) =
        open_counted_fixed_reader(root, endpoints_name, evidence)?;
    let mut identity = read_fixed::<BASE_IDENTITY_WIDTH>(&mut identities)?;
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
            let (found, probe_work) = base
                .as_deref_mut()
                .ok_or_else(|| storage("endpoint UUID lacks node surrogate"))?
                .lookup_node_surrogates(&base_requests)?;
            account_probe_work(&probe_work, evidence)?;
            for (position, surrogate) in base_positions.into_iter().zip(found) {
                surrogates[position] = surrogate;
            }
        }
        for (endpoint, surrogate) in endpoint_window.into_iter().zip(surrogates) {
            let surrogate = surrogate
                .filter(|value| *value != 0)
                .ok_or_else(|| storage("endpoint UUID lacks node surrogate"))?;
            let mut record = [0_u8; RESOLVED_ENDPOINT_WIDTH];
            record[..16].copy_from_slice(&endpoint[16..32]);
            record[16] = endpoint[32];
            record[RESOLVED_SURROGATE_OFFSET..RESOLVED_ENDPOINT_WIDTH]
                .copy_from_slice(&surrogate.to_be_bytes());
            let key: [u8; 16] = record[..16].try_into().expect("fixed key prefix");
            resolved.route(plan, &key, &record, evidence)?;
            account_merge_read::<ENDPOINT_WIDTH>(evidence)?;
        }
        reject_cancelled(cancelled)?;
    }
    account_fixed_read_operations(&identities_counter, evidence)?;
    account_fixed_read_operations(&endpoints_counter, evidence)?;
    release_counted_reader_cache(&mut identities, evidence)?;
    release_counted_reader_cache(&mut endpoints, evidence)?;
    Ok(())
}

/// Finish one fixed-width family. Under finish stages (#1562) the family's
/// segments are retired as soon as a stage records the installed output as
/// their successor; a family a resumed shape already finished is adopted
/// from its stage instead of re-derived.
#[allow(clippy::too_many_arguments)]
fn finish_family_stage<const N: usize>(
    root: &StableDirectory,
    checkpoint: &mut Checkpoint,
    stages: Option<&mut ShapeStages>,
    kind: ShapeStageKind,
    partitioner: FixedRangePartitioner<'_, N>,
    output: &str,
    boundary: u64,
    cpu_admission: Option<&std::sync::Arc<super::cpu_admission::ConstructionCpuAdmission>>,
    cancelled: &mut impl FnMut() -> bool,
) -> Result<Option<String>, GfError> {
    let Some(stages) = stages else {
        return partitioner.finish_optional(
            output,
            boundary,
            false,
            cancelled,
            &mut checkpoint.evidence,
        );
    };
    if let Some(stage) = stages.completed(kind) {
        return Ok(stage.outputs.first().map(|receipt| receipt.name.clone()));
    }
    let (installed, segments) =
        partitioner.finish_retaining(output, boundary, cancelled, &mut checkpoint.evidence)?;
    retire_behind_stage(
        root,
        checkpoint,
        stages,
        kind,
        StageResult {
            outputs: installed_outputs(root, installed.as_deref())?,
            ..StageResult::default()
        },
        &segments,
        cpu_admission,
    )?;
    Ok(installed)
}

/// The receipt of a finish's installed output, if it installed one.
fn installed_outputs(
    root: &StableDirectory,
    installed: Option<&str>,
) -> Result<Vec<ArtifactReceipt>, GfError> {
    installed
        .map(|name| receipt_for_existing(root, name))
        .transpose()
        .map(|receipt| receipt.into_iter().collect())
}

/// Install `kind`'s stage, then unlink the segments its successor replaces.
///
/// The order is the whole invariant: until the stage is durable the segments
/// are the only authority for their rows, and once it is durable they are
/// garbage that recovery discards if a crash strands them.
fn retire_behind_stage(
    root: &StableDirectory,
    checkpoint: &mut Checkpoint,
    stages: &mut ShapeStages,
    kind: ShapeStageKind,
    result: StageResult,
    segments: &[ArtifactReceipt],
    cpu_admission: Option<&std::sync::Arc<super::cpu_admission::ConstructionCpuAdmission>>,
) -> Result<(), GfError> {
    stages.record(root, checkpoint, kind, result)?;
    construction_failpoint(&format!("shape.stage.{}.after_install", kind.tag()));
    retire_segments(root, segments, cpu_admission, &mut checkpoint.evidence)?;
    construction_failpoint(&format!("shape.stage.{}.after_retire", kind.tag()));
    Ok(())
}

/// Most lanes one retirement leases (#1448). Each segment's retirement is a
/// few small metadata operations and two directory syncs, so lanes mostly
/// overlap sync waits; more than this measured no further gain.
const RETIRE_LANES: usize = 8;

/// Unlink every retired segment, in parallel lanes when the instance has CPU
/// admission to spare (#1448).
///
/// Each segment's retirement is independent once its stage is durable: any
/// subset left on disk by a crash is garbage recovery discards. Lanes do only
/// the filesystem work; the evidence is reconciled here, in segment order, so
/// it does not depend on the schedule. Every segment is attempted even after
/// one fails, so the set removed, and the evidence charged for it, is the
/// same for any lane count; the first error in segment order is returned.
fn retire_segments(
    root: &StableDirectory,
    segments: &[ArtifactReceipt],
    cpu_admission: Option<&std::sync::Arc<super::cpu_admission::ConstructionCpuAdmission>>,
    evidence: &mut GraphConstructionEvidence,
) -> Result<(), GfError> {
    let want = std::num::NonZeroUsize::new(RETIRE_LANES.min(segments.len()))
        .filter(|lanes| lanes.get() > 1);
    let lease =
        want.and_then(|want| cpu_admission.and_then(|admission| admission.try_acquire(want)));
    let lanes = lease.as_ref().map_or(1, |lease| lease.lanes().get());
    if lanes == 1 {
        let mut first_error = None;
        for segment in segments {
            if let Err(error) = unlink_shape_artifact(root, &segment.name, evidence) {
                first_error.get_or_insert(error);
            }
        }
        return first_error.map_or(Ok(()), Err);
    }
    let next = std::sync::atomic::AtomicUsize::new(0);
    let results = std::sync::Mutex::new(
        std::iter::repeat_with(|| None)
            .take(segments.len())
            .collect::<Vec<Option<Result<ArtifactReceipt, GfError>>>>(),
    );
    std::thread::scope(|scope| {
        for _ in 0..lanes {
            scope.spawn(|| {
                use std::sync::atomic::Ordering;
                loop {
                    let index = next.fetch_add(1, Ordering::AcqRel);
                    let Some(segment) = segments.get(index) else {
                        break;
                    };
                    let result = unlink_shape_artifact_files(root, &segment.name);
                    if let Ok(mut results) = results.lock() {
                        results[index] = Some(result);
                    }
                }
            });
        }
    });
    drop(lease);
    let results = results
        .into_inner()
        .map_err(|_| storage("segment retirement results poisoned"))?;
    let mut first_error = None;
    for result in results.into_iter().flatten() {
        match result {
            Ok(receipt) => reconcile_shape_artifact_removal(evidence, &receipt)?,
            Err(error) => {
                first_error.get_or_insert(error);
            }
        }
    }
    first_error.map_or(Ok(()), Err)
}

/// Resolve endpoints under finish stages (#1562): the resolved segments are
/// recorded before the staged endpoint domain is retired, and are themselves
/// retired once the sorted resolved output is recorded. Without stages this
/// is [`resolve_endpoint_surrogates`].
#[allow(clippy::too_many_arguments)]
fn resolve_endpoint_stages(
    root: &StableDirectory,
    checkpoint: &mut Checkpoint,
    stages: Option<&mut ShapeStages>,
    plan: &PartitionPlan,
    identities_name: &str,
    endpoints_name: Option<&str>,
    base: Option<&mut AuthenticatedUuidIndexSnapshot>,
    boundary: u64,
    retain_segments: bool,
    cpu_admission: Option<&std::sync::Arc<super::cpu_admission::ConstructionCpuAdmission>>,
    cancelled: &mut impl FnMut() -> bool,
) -> Result<Option<String>, GfError> {
    let budgets = checkpoint.budgets;
    let Some(stages) = stages else {
        return resolve_endpoint_surrogates(
            root,
            plan,
            identities_name,
            endpoints_name,
            base,
            budgets.max_batch_rows,
            budgets.max_partition_bytes,
            budgets.max_external_partition_bytes,
            boundary,
            retain_segments,
            cpu_admission,
            cancelled,
            &mut checkpoint.evidence,
        );
    };
    if let Some(stage) = stages.completed(ShapeStageKind::Resolved) {
        return Ok(stage.outputs.first().map(|receipt| receipt.name.clone()));
    }
    let mut resolved = FixedRangePartitioner::<RESOLVED_ENDPOINT_WIDTH>::new(
        root,
        PartitionFamily::Resolved,
        plan.partitions(),
        None,
        false,
    )?
    .with_materialization_limit(budgets.max_partition_bytes)
    .with_external_partitions(budgets.max_external_partition_bytes)
    .with_cpu_admission(cpu_admission.cloned());
    if let Some(stage) = stages.completed(ShapeStageKind::ResolvedRouted) {
        let mut segments = vec![Vec::new(); plan.partitions()];
        for receipt in &stage.outputs {
            let partition = parse_segment_name(&receipt.name)
                .map(|segment| segment.partition)
                .ok_or_else(|| storage("resolved stage segment name is invalid"))?;
            segments
                .get_mut(partition)
                .ok_or_else(|| storage("resolved stage segment is outside the plan"))?
                .push(receipt.clone());
        }
        resolved.restore(&segments, &stage.rows)?;
    } else {
        if let Some(endpoints_name) = endpoints_name {
            route_resolved_endpoints(
                root,
                plan,
                identities_name,
                endpoints_name,
                base,
                budgets.max_batch_rows,
                &mut resolved,
                cancelled,
                &mut checkpoint.evidence,
            )?;
            // Every routed record is durable before the endpoint domain is
            // retired.
            resolved.seal(boundary, &mut checkpoint.evidence)?;
        }
        stages.record(
            root,
            checkpoint,
            ShapeStageKind::ResolvedRouted,
            StageResult {
                outputs: resolved.sealed_segments(),
                rows: resolved.balance().rows().to_vec(),
                ..StageResult::default()
            },
        )?;
        construction_failpoint("shape.stage.resolved-routed.after_install");
        if let Some(endpoints_name) = endpoints_name {
            shape_publication_failure("shape.before_endpoint_retirement")?;
            unlink_shape_artifact(root, endpoints_name, &mut checkpoint.evidence)?;
            construction_failpoint("shape.after_endpoint_retirement");
            shape_publication_failure("shape.after_endpoint_retirement")?;
        }
        reject_cancelled(cancelled)?;
    }
    let (installed, segments) = resolved.finish_retaining(
        SHAPED_EDGE_ENDPOINTS,
        boundary,
        cancelled,
        &mut checkpoint.evidence,
    )?;
    retire_behind_stage(
        root,
        checkpoint,
        stages,
        ShapeStageKind::Resolved,
        StageResult {
            outputs: installed_outputs(root, installed.as_deref())?,
            ..StageResult::default()
        },
        &segments,
        cpu_admission,
    )?;
    Ok(installed)
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
