//! Shape for graph construction.

use super::partition::{IdentitySampler, PartitionPlan};
use super::partition_shaping::{
    FixedRangePartitioner, PartitionFamily, RowRangePartitioner, is_partition_artifact_name,
};
use super::{
    ArtifactReceipt, AuthenticatedShapeSource, AuthenticatedUuidIndexSnapshot, BASE_IDENTITY_WIDTH,
    BLOCK_BYTES, BTreeMap, BufReader, BufWriter, Checkpoint, ConstructionChunkKind,
    ConstructionChunkReceipt, ConstructionPublicationState, ConstructionShape, CountingRead,
    DetailCodec, DetailValidator, Digest, EDGE_DETAIL_WIDTH, ENDPOINT_WIDTH, File, GfError,
    GraphConstructionEvidence, GraphConstructionSession, GraphConstructionState, HashingWriter,
    IDENTITY_SURROGATE_OFFSET, IDENTITY_WIDTH, IoCounter, NODE_DETAIL_WIDTH, OsStr,
    RESOLVED_ENDPOINT_WIDTH, RESOLVED_SURROGATE_OFFSET, Read, ReadWork, SHAPE_INTENT, Sha256,
    ShapeIntent, StableDirectory, Uuid, UuidIndexKind, Write, account_cache_release,
    account_fixed_read_operations, account_fixed_write_operations, account_merge_read,
    account_merge_write, account_probe_work, account_sequential_read, account_sequential_write,
    artifact_temp, authenticate_artifact, build_runtime_catalog, canonical_artifact_target,
    checked_evidence_sum, combine_cache_cleanup, compact_parent_surrogate_tails,
    construction_failpoint, decode_bounded, decode_shape_intent, file_identity, file_link_count,
    hex, install_control, is_canonical_lower_hex, is_canonical_sha256,
    merge_cache_release_evidence, open_counted_fixed_reader, read_run_record, receipt_for_existing,
    receipt_for_existing_with_work, record_shape_artifact_install, reject_cancelled,
    reject_existing_merge_artifacts, release_counted_reader_cache, replace_checkpoint_control,
    replace_control, sha256, shape_authority_sha256, shape_publication_failure, storage,
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
        let mut sampler = IdentitySampler::new(partition_count, total)?;
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
        #[cfg(any(test, feature = "test-support"))]
        let _diagnostic_scope = crate::graph_construction::diagnostics::Scope::start("shaping");
        self.revalidate_authority()?;
        if self.checkpoint.state != GraphConstructionState::Sealed
            || self.checkpoint.publication_state != Some(ConstructionPublicationState::Sealed)
        {
            return Err(storage("only a sealed session can be shaped"));
        }
        reject_cancelled(&mut cancelled)?;
        let authenticate_outputs = !self.has_encoding_successor() && !self.shape_outputs_verified;
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
            self.reclaim_superseded_payloads_cancellable(&mut cancelled)?;
            return Ok(shape);
        }
        reject_existing_merge_artifacts(&self.root)?;

        // Snapshot the committed evidence before any shaping work, including the
        // splitter sampling pass: the shape intent's baseline must match what a
        // reopen reads back from the checkpoint on disk.
        let baseline_evidence = self.checkpoint.evidence.clone();
        let detail_codec =
            DetailCodec::from_version(self.checkpoint.format_version).map_err(storage)?;
        // Barrier A0. Choose the range-partition splitters from a deterministic
        // sample of the staged identity domain, and record them before any
        // partition writes a byte. `super::partition` records why a formula
        // over the key's high bits is not an option for UUIDv7 identities.
        let plan = self.choose_partition_plan(&mut cancelled)?;
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
        let node_plan = self.choose_node_partition_plan(&mut cancelled)?;
        let node_partitions = node_plan.partitions();
        self.checkpoint.evidence.shape_node_partitions =
            u64::try_from(node_partitions).map_err(storage)?;
        let mut identities = FixedRangePartitioner::<BASE_IDENTITY_WIDTH>::new(
            &self.root,
            PartitionFamily::Identities,
            partitions,
            None,
            true,
        )?;
        let mut node_details = FixedRangePartitioner::<NODE_DETAIL_WIDTH>::new(
            &self.root,
            PartitionFamily::NodeDetails,
            node_partitions,
            Some(detail_codec),
            false,
        )?;
        let mut edge_details = FixedRangePartitioner::<EDGE_DETAIL_WIDTH>::new(
            &self.root,
            PartitionFamily::EdgeDetails,
            partitions,
            Some(detail_codec),
            false,
        )?;
        let mut endpoints = FixedRangePartitioner::<ENDPOINT_WIDTH>::new(
            &self.root,
            PartitionFamily::Endpoints,
            node_partitions,
            None,
            false,
        )?;
        let mut row_groups: BTreeMap<(u8, String), RowRangePartitioner> = BTreeMap::new();
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
        install_control(&self.root, SHAPE_INTENT, &shape_intent)?;
        #[cfg(any(test, feature = "test-support"))]
        let chunk_scope = crate::graph_construction::diagnostics::Scope::start("shape.chunk_loop");
        for sequence in 0..self.checkpoint.next_sequence {
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
            let row_plan = match receipt.kind {
                ConstructionChunkKind::Node => &node_plan,
                ConstructionChunkKind::Edge => &plan,
            };
            if !row_groups.contains_key(&group) {
                row_groups.insert(
                    group.clone(),
                    RowRangePartitioner::new(
                        &self.root,
                        &format!("{kind}-{}", receipt.schema_sha256),
                        row_plan.partitions(),
                    )?,
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
        }
        #[cfg(any(test, feature = "test-support"))]
        drop(chunk_scope);
        // Load-bearing, not a nicety: a collapsed one-partition run is
        // perfectly deterministic and passes every byte-equality test, so this
        // is the only check that can tell a working range partition from a
        // catastrophically skewed one.
        #[cfg(any(test, feature = "test-support"))]
        let between_scope =
            crate::graph_construction::diagnostics::Scope::start("shape.loop_to_chain");
        identities.balance().assert_balanced("staged identity")?;
        let partition_identity_rows = identities.balance().rows().to_vec();
        self.checkpoint.evidence.max_partition_identity_rows = identities.balance().max_rows();
        self.checkpoint.evidence.partitioned_identity_rows = identities.balance().total();
        let staged_identities = identities
            .finish_optional(
                STAGED_IDENTITIES,
                &mut cancelled,
                &mut self.checkpoint.evidence,
            )?
            .ok_or_else(|| storage("construction contains no identities"))?;
        let node_details = node_details.finish_optional(
            SHAPED_NODE_DETAILS,
            &mut cancelled,
            &mut self.checkpoint.evidence,
        )?;
        let edge_details = edge_details.finish_optional(
            SHAPED_EDGE_DETAILS,
            &mut cancelled,
            &mut self.checkpoint.evidence,
        )?;
        let endpoints = endpoints.finish_optional(
            STAGED_ENDPOINTS,
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
        #[cfg(any(test, feature = "test-support"))]
        drop(between_scope);
        #[cfg(any(test, feature = "test-support"))]
        let validate_scope =
            crate::graph_construction::diagnostics::Scope::start("chain.validate_staged_details");
        let (new_nodes, new_edges) = validate_staged_details(
            &self.root,
            &staged_identities,
            node_details.as_deref(),
            edge_details.as_deref(),
            detail_codec,
            &mut cancelled,
            &mut self.checkpoint.evidence,
        )?;
        #[cfg(any(test, feature = "test-support"))]
        drop(validate_scope);
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
        #[cfg(any(test, feature = "test-support"))]
        let assign_scope =
            crate::graph_construction::diagnostics::Scope::start("chain.assign_surrogates");
        let identities = assign_surrogates(
            &self.root,
            &staged_identities,
            base_max_node,
            base_max_edge,
            &mut cancelled,
            &mut self.checkpoint.evidence,
        )?;
        #[cfg(any(test, feature = "test-support"))]
        drop(assign_scope);
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
        #[cfg(any(test, feature = "test-support"))]
        let resolve_scope = crate::graph_construction::diagnostics::Scope::start(
            "chain.resolve_endpoint_surrogates",
        );
        let edge_endpoints = resolve_endpoint_surrogates(
            &self.root,
            &plan,
            &identities,
            endpoints.as_deref(),
            self.base_snapshot.as_mut(),
            self.checkpoint.budgets.max_batch_rows,
            &mut cancelled,
            &mut self.checkpoint.evidence,
        )?;
        #[cfg(any(test, feature = "test-support"))]
        drop(resolve_scope);
        #[cfg(any(test, feature = "test-support"))]
        let tail_scope = crate::graph_construction::diagnostics::Scope::start("shape.after_chain");
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
                &output,
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
                splitters: shape_intent.splitters,
                node_splitters: shape_intent.node_splitters,
                partition_identity_rows,
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

/// Accumulates one contiguous same-partition run of wire bytes so the
/// caller writes it in a single call instead of once per record (#1439
/// follow-up).
///
/// The staged runs routed through [`route_fixed_run`] and
/// [`route_identity_run`] are UUID-sorted and the partition function is
/// monotone, so consecutive same-partition records are always contiguous
/// within one staged chunk. This is the same property
/// [`RowRangePartitioner::route_batch`] already exploits for Arrow rows;
/// this is its fixed-width-record equivalent.
struct PartitionRun {
    partition: Option<usize>,
    bytes: Vec<u8>,
    records: u64,
}

impl PartitionRun {
    fn new() -> Self {
        Self {
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
        if self.partition.is_some_and(|current| current != partition) {
            self.flush(target, evidence)?;
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
    mut base: Option<&mut AuthenticatedUuidIndexSnapshot>,
    window_rows: usize,
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
    let mut resolved = FixedRangePartitioner::<RESOLVED_ENDPOINT_WIDTH>::new(
        root,
        PartitionFamily::Resolved,
        plan.partitions(),
        None,
        false,
    )?;
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
    drop(identities);
    drop(endpoints);
    // Every routed record is durable before the endpoint domain is retired.
    resolved.seal(evidence)?;
    shape_publication_failure("shape.before_endpoint_retirement")?;
    unlink_shape_artifact(root, endpoints_name, evidence)?;
    construction_failpoint("shape.after_endpoint_retirement");
    shape_publication_failure("shape.after_endpoint_retirement")?;
    reject_cancelled(cancelled)?;
    resolved.finish_optional(SHAPED_EDGE_ENDPOINTS, cancelled, evidence)
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
