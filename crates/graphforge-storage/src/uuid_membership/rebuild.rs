//! UUID and ordinal rebuild, migration, and bounded record sorting.

use super::BULK_IO_BYTES;
use super::FORMAT_VERSION;
#[cfg(test)]
use super::FileRecord;
use super::IDENTITY_RECORD_BYTES;
use super::IDENTITY_RECORD_WIDTH;
use super::INDEX_DIR;
use super::MANIFEST;
use super::Manifest;
use super::NODE_LOOKUP_RECORD_BYTES;
use super::RunRecord;
use super::TOPOLOGY_RECEIPT;
use super::TopologyIndexReceipt;
use super::UuidIndexBuildLimits;
use super::UuidIndexBuildMetrics;
use super::V4_ORDINAL_MANIFEST;
use super::V4_ORDINAL_RECEIPT;
use super::V4OrdinalBuildMetrics;
use super::V4OrdinalRebuildDisposition;
use super::V4OrdinalRebuildEvidence;
#[cfg(test)]
use super::describe_blocks;
use super::describe_staged_data;
use super::identity_codec;
use super::maintenance::selected_generation_for_graph_root;
use super::ordinal_artifacts::V4AuthorityTransactionProof;
use super::ordinal_artifacts::V4ConstructionArtifactBundle;
use super::ordinal_artifacts::V4OrdinalConstructionWriter;
use super::ordinal_artifacts::commit_v4_publications;
use super::storage_err;
use super::topology_delta::hex_sha256;
use super::uuid_membership_index_is_fresh;
use super::validate_run_descriptors;
use arrow::array::Array;
use arrow::array::FixedSizeBinaryArray;
use arrow::array::UInt64Array;
use graphforge_core::GfError;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
#[cfg(test)]
use sha2::Digest;
#[cfg(test)]
use sha2::Sha256;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
#[cfg(test)]
use std::fmt::Write as _;
use std::fs;
use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct V4RebuildScratchAccounting {
    peak_bytes: u64,
    sorted_projection_bytes: u64,
    live_artifact_bytes: u64,
    retained_staged_bytes: u64,
    staged_control_bytes: u64,
    scratch_live: bool,
}

impl V4RebuildScratchAccounting {
    fn register_sorted_projection(&mut self, path: &Path) -> Result<(), GfError> {
        self.sorted_projection_bytes = path.metadata().map_err(storage_err)?.len();
        // A complete merge output coexists with the complete input round until
        // that round is retired. The sorter owns both sets, so account for the
        // overlap directly instead of inspecting the active scratch tree.
        self.observe(self.sorted_projection_bytes.checked_mul(2))
    }

    fn register_surrogate_projection(&mut self, path: &Path) -> Result<(), GfError> {
        let surrogate_bytes = path.metadata().map_err(storage_err)?.len();
        if surrogate_bytes != self.sorted_projection_bytes {
            return Err(storage_err(
                "v4 rebuild projections have inconsistent scratch sizes",
            ));
        }
        // UUID-sorted input remains live while a complete surrogate merge
        // output coexists with its complete input round.
        self.observe(
            self.sorted_projection_bytes.checked_add(
                surrogate_bytes
                    .checked_mul(2)
                    .ok_or_else(|| storage_err("v4 rebuild scratch byte count overflow"))?,
            ),
        )
    }

    fn register_artifacts(&mut self, artifact_peak: u64) -> Result<(), GfError> {
        // Both final sort projections remain open while the immutable forward
        // and ordinal artifacts are streamed.
        self.live_artifact_bytes = artifact_peak;
        self.scratch_live = true;
        self.observe_current()
    }

    fn register_staged_artifact(&mut self, bytes: u64) -> Result<(), GfError> {
        self.retained_staged_bytes = self
            .retained_staged_bytes
            .checked_add(bytes)
            .ok_or_else(|| storage_err("v4 rebuild staged artifact byte count overflow"))?;
        self.observe_current()
    }

    fn register_staged_control(&mut self, bytes: u64) -> Result<(), GfError> {
        self.staged_control_bytes = self
            .staged_control_bytes
            .checked_add(bytes)
            .ok_or_else(|| storage_err("v4 rebuild staged control byte count overflow"))?;
        self.observe_current()
    }

    fn release_scratch(&mut self) -> Result<(), GfError> {
        self.scratch_live = false;
        self.observe_current()
    }

    fn observe_current(&mut self) -> Result<(), GfError> {
        let scratch = if self.scratch_live {
            self.sorted_projection_bytes
                .checked_mul(2)
                .and_then(|bytes| bytes.checked_add(self.live_artifact_bytes))
        } else {
            Some(0)
        };
        self.observe(
            scratch
                .and_then(|bytes| bytes.checked_add(self.retained_staged_bytes))
                .and_then(|bytes| bytes.checked_add(self.staged_control_bytes)),
        )
    }

    fn observe(&mut self, bytes: Option<u64>) -> Result<(), GfError> {
        let bytes = bytes.ok_or_else(|| storage_err("v4 rebuild scratch byte count overflow"))?;
        self.peak_bytes = self.peak_bytes.max(bytes);
        Ok(())
    }
}

struct StagedV4OrdinalRebuild {
    generation: u64,
    build: UuidIndexBuildMetrics,
    artifacts: V4OrdinalBuildMetrics,
    scratch: V4RebuildScratchAccounting,
}

/// Explicit bounded rebuild/migration path. Immutable data files are completed
/// and synced first; `manifest.json` is atomically replaced last.
pub fn rebuild_uuid_membership_indexes(
    project_dir: &Path,
    limits: UuidIndexBuildLimits,
) -> Result<UuidIndexBuildMetrics, GfError> {
    migrate_uuid_membership_indexes(project_dir, limits, true)
}

/// Rebuild v4 ordinal identity from canonical topology, never v3 reverse state.
pub fn rebuild_v4_ordinal_identity(
    project_dir: &Path,
    limits: UuidIndexBuildLimits,
) -> Result<UuidIndexBuildMetrics, GfError> {
    rebuild_v4_ordinal_identity_with_evidence(project_dir, limits).map(|evidence| evidence.build)
}

/// Rebuild v4 ordinal identity and return its explicit sanitized disposition.
pub fn rebuild_v4_ordinal_identity_with_evidence(
    project_dir: &Path,
    limits: UuidIndexBuildLimits,
) -> Result<V4OrdinalRebuildEvidence, GfError> {
    let metrics = std::rc::Rc::new(std::cell::RefCell::new(None));
    let callback_metrics = std::rc::Rc::clone(&metrics);
    let root = project_dir.to_path_buf();
    let participant: crate::durable_rewrite::RewriteParticipantPreparer<'_> =
        Box::new(move |context, batch| {
            let mut staged = stage_v4_ordinal_rebuild_locked(
                context.project_root,
                context.prior.topology,
                limits,
                batch,
            )?;
            let manifest_path = context
                .project_root
                .join(INDEX_DIR)
                .join(V4_ORDINAL_MANIFEST);
            let manifest_bytes = fs::read(
                batch
                    .staged_temp(&manifest_path)
                    .ok_or_else(|| storage_err("v4 rebuild did not stage its manifest"))?,
            )
            .map_err(storage_err)?;
            let receipt = TopologyIndexReceipt {
                nonce: Uuid::new_v4().simple().to_string(),
                expected_generation: context.prior.topology,
                topology_delta_sha256: hex_sha256(b"uuid-membership-v4-canonical-rebuild"),
                manifest_sha256: hex_sha256(&manifest_bytes),
            };
            let receipt_bytes = serde_json::to_vec(&receipt).map_err(storage_err)?;
            batch.stage_bytes(
                &context
                    .project_root
                    .join(INDEX_DIR)
                    .join(V4_ORDINAL_RECEIPT),
                &receipt_bytes,
            )?;
            staged.scratch.register_staged_control(
                u64::try_from(receipt_bytes.len())
                    .map_err(|_| storage_err("v4 rebuild receipt length overflow"))?,
            )?;
            *callback_metrics.borrow_mut() = Some(v4_rebuild_evidence(
                staged.generation,
                staged.build,
                &staged.artifacts,
                staged.scratch,
            )?);
            // Artifacts, then receipt, then facet manifest. The enclosing
            // generation record remains the durable transaction's last switch.
            batch.move_staged_destination_to_end(&manifest_path);
            Ok(Some(crate::AuxiliaryReceipt {
                kind: "uuid-membership/v4".to_owned(),
                schema_version: crate::ORDINAL_IDENTITY_V4,
                path: format!("{INDEX_DIR}/{V4_ORDINAL_RECEIPT}"),
                digest: hex_sha256(&receipt_bytes),
                bytes: u64::try_from(receipt_bytes.len())
                    .map_err(|_| storage_err("receipt length overflow"))?,
            }))
        });
    crate::generation::commit_topology_aware_with_participant(
        crate::staging::RewriteBatch::new(),
        &root,
        participant,
    )?;
    metrics
        .borrow_mut()
        .take()
        .ok_or_else(|| storage_err("v4 ordinal rebuild produced no evidence"))
}

#[allow(clippy::too_many_lines)]
fn stage_v4_ordinal_rebuild_locked(
    project_dir: &Path,
    generation: u64,
    limits: UuidIndexBuildLimits,
    batch: &mut crate::staging::RewriteBatch,
) -> Result<StagedV4OrdinalRebuild, GfError> {
    if generation == 0 {
        return Err(storage_err(
            "v4 ordinal identity requires a nonzero topology generation",
        ));
    }
    let limits = limits.validate()?;
    let destination = project_dir.join(INDEX_DIR);
    fs::create_dir_all(&destination).map_err(storage_err)?;
    let staging = project_dir
        .parent()
        .ok_or_else(|| storage_err("project directory has no staging parent"))?;
    let scratch = tempfile::Builder::new()
        .prefix("uuid-membership-v4-build-")
        .tempdir_in(staging)
        .map_err(storage_err)?;
    let artifact_root = scratch.path().join("artifacts");
    fs::create_dir(&artifact_root).map_err(storage_err)?;
    let artifact_directory =
        graphforge_filesystem::StableDirectory::open(&artifact_root).map_err(storage_err)?;
    let mut metrics = UuidIndexBuildMetrics::default();
    let mut scratch_accounting = V4RebuildScratchAccounting::default();

    // Both bounded projections originate from this single canonical topology
    // scan. For an immutable project generation, authenticate its declared
    // graph inventory before opening any topology payload. Standalone graph
    // roots have no enclosing inventory authority and retain the admitted,
    // validated Parquet path used by the existing explicit v3 rebuild.
    let uuid_runs = if let Some(selected) = selected_generation_for_graph_root(project_dir)? {
        let mut pinned = selected
            .authenticated_graph_files_pinned_where(|entry| {
                canonical_node_topology_inventory_path(&entry.relative_path)
            })?
            .ok_or_else(|| {
                storage_err("selected project generation has no authenticated graph inventory")
            })?;
        pinned.sort_by(|left, right| left.entry.relative_path.cmp(&right.entry.relative_path));
        scan_pinned_entity_surrogate_runs(
            &pinned,
            "node_uuid",
            "node_id",
            "v4-node",
            scratch.path(),
            limits,
            &mut metrics,
        )?
    } else {
        // Standalone roots have no immutable project inventory authority.
        let node_paths = crate::mutator::node_parquet_files(project_dir).map_err(storage_err)?;
        scan_entity_surrogate_runs(
            &node_paths,
            "node_uuid",
            "node_id",
            "v4-node",
            scratch.path(),
            limits,
            &mut metrics,
        )?
    };
    // No v3 index file is opened or used as migration authority.
    let uuid_sorted =
        merge_node_surrogate_runs(uuid_runs, scratch.path(), limits.merge_fan_in, &mut metrics)?;
    scratch_accounting.register_sorted_projection(&uuid_sorted)?;
    let ordinal_sorted = build_surrogate_run(&uuid_sorted, scratch.path(), limits, &mut metrics)?;
    scratch_accounting.register_surrogate_projection(&ordinal_sorted)?;

    let mut writer = V4OrdinalConstructionWriter::start(generation, &artifact_directory)?;
    let mut cancelled = || false;
    let mut forward = BufReader::with_capacity(
        BULK_IO_BYTES,
        File::open(&uuid_sorted).map_err(storage_err)?,
    );
    while let Some((uuid, node_id)) = read_node_surrogate_record(&mut forward)? {
        writer.push_forward(Uuid::from_bytes(uuid), node_id, &mut cancelled)?;
    }
    let mut ordinal = BufReader::with_capacity(
        BULK_IO_BYTES,
        File::open(&ordinal_sorted).map_err(storage_err)?,
    );
    while let Some((node_id, uuid)) = read_surrogate_record(&mut ordinal)? {
        writer.push_ordinal(node_id, Uuid::from_bytes(uuid), &mut cancelled)?;
    }
    let V4ConstructionArtifactBundle {
        manifest,
        metrics: v4_metrics,
        publications,
    } = writer.finish()?;
    scratch_accounting.register_artifacts(v4_metrics.peak_temporary_bytes)?;
    metrics.node_count = v4_metrics.input_records;
    stage_v4_rebuild_artifacts(
        &manifest,
        &destination,
        &artifact_root,
        batch,
        &mut scratch_accounting,
    )?;
    let manifest_bytes = serde_json::to_vec(&manifest).map_err(storage_err)?;
    if manifest_bytes.len() as u64 > crate::ordinal_identity_v4::MAX_MANIFEST_BYTES {
        return Err(storage_err("v4 ordinal manifest exceeds bound"));
    }
    batch.stage_bytes(&destination.join(V4_ORDINAL_MANIFEST), &manifest_bytes)?;
    scratch_accounting.register_staged_control(
        u64::try_from(manifest_bytes.len())
            .map_err(|_| storage_err("v4 ordinal manifest length overflow"))?,
    )?;
    scratch_accounting.release_scratch()?;
    commit_v4_publications(publications, V4AuthorityTransactionProof)?;
    Ok(StagedV4OrdinalRebuild {
        generation,
        build: metrics,
        artifacts: v4_metrics,
        scratch: scratch_accounting,
    })
}

fn stage_v4_rebuild_artifacts(
    manifest: &crate::V4OrdinalIdentityManifest,
    destination: &Path,
    artifact_root: &Path,
    batch: &mut crate::staging::RewriteBatch,
    scratch: &mut V4RebuildScratchAccounting,
) -> Result<(), GfError> {
    for artifact in manifest
        .forward_identities
        .iter()
        .chain(manifest.ordinal_ranges.iter().map(|range| &range.artifact))
        .chain(manifest.tombstones.iter().map(|run| &run.artifact))
    {
        batch.stage_file(
            &destination.join(&artifact.name),
            &artifact_root.join(&artifact.name),
        )?;
        scratch.register_staged_artifact(artifact.bytes)?;
    }
    Ok(())
}

fn v4_rebuild_evidence(
    generation: u64,
    build: UuidIndexBuildMetrics,
    artifacts: &V4OrdinalBuildMetrics,
    scratch: V4RebuildScratchAccounting,
) -> Result<V4OrdinalRebuildEvidence, GfError> {
    Ok(V4OrdinalRebuildEvidence {
        disposition: V4OrdinalRebuildDisposition::CanonicalTopology,
        topology_generation: generation,
        input_identities: artifacts.input_records,
        ordinal_ranges: u64::try_from(artifacts.ranges).map_err(storage_err)?,
        artifact_bytes: artifacts.artifact_bytes,
        write_blocks: artifacts.write_blocks,
        peak_buffer_bytes: u64::try_from(artifacts.peak_buffer_bytes).map_err(storage_err)?,
        peak_temporary_bytes: scratch.peak_bytes,
        fsync_operations: artifacts.fsync_operations,
        build,
    })
}

/// Ensure the current topology generation has a v3 UUID index before a
/// topology mutation enters its sealed rewrite callback.
pub(crate) fn ensure_uuid_membership_migrated(project_dir: &Path) -> Result<(), GfError> {
    migrate_uuid_membership_indexes(project_dir, UuidIndexBuildLimits::default(), false).map(|_| ())
}

fn migrate_uuid_membership_indexes(
    project_dir: &Path,
    limits: UuidIndexBuildLimits,
    force: bool,
) -> Result<UuidIndexBuildMetrics, GfError> {
    if !force && uuid_membership_index_is_fresh(project_dir)? {
        return Ok(UuidIndexBuildMetrics::default());
    }
    let metrics = std::rc::Rc::new(std::cell::RefCell::new(None));
    let callback_metrics = std::rc::Rc::clone(&metrics);
    let root = project_dir.to_path_buf();
    let participant: crate::durable_rewrite::RewriteParticipantPreparer<'_> =
        Box::new(move |context, batch| {
            if !force && manifest_generation(context.project_root)? == Some(context.prior.topology)
            {
                return Ok(None);
            }
            let built = stage_uuid_membership_rebuild_locked(
                context.project_root,
                context.prior.topology,
                limits,
                batch,
            )?;
            *callback_metrics.borrow_mut() = Some(built);
            let manifest_destination = context.project_root.join(INDEX_DIR).join(MANIFEST);
            let manifest_temp = batch.staged_temp(&manifest_destination).ok_or_else(|| {
                storage_err("UUID migration did not stage its canonical manifest")
            })?;
            let manifest_bytes = fs::read(manifest_temp).map_err(storage_err)?;
            let receipt = TopologyIndexReceipt {
                nonce: Uuid::new_v4().simple().to_string(),
                expected_generation: context.prior.topology,
                topology_delta_sha256: hex_sha256(b"uuid-membership-migration"),
                manifest_sha256: hex_sha256(&manifest_bytes),
            };
            let receipt_bytes = serde_json::to_vec(&receipt).map_err(storage_err)?;
            batch.stage_bytes(
                &context.project_root.join(INDEX_DIR).join(TOPOLOGY_RECEIPT),
                &receipt_bytes,
            )?;
            Ok(Some(crate::AuxiliaryReceipt {
                kind: "uuid-membership/v5".to_owned(),
                schema_version: FORMAT_VERSION,
                path: format!("{INDEX_DIR}/{TOPOLOGY_RECEIPT}"),
                digest: hex_sha256(&receipt_bytes),
                bytes: u64::try_from(receipt_bytes.len())
                    .map_err(|_| storage_err("receipt length overflow"))?,
            }))
        });
    crate::generation::commit_topology_aware_with_participant(
        crate::staging::RewriteBatch::new(),
        &root,
        participant,
    )?;
    let result = metrics.borrow_mut().take().unwrap_or_default();
    Ok(result)
}

#[allow(clippy::too_many_lines)] // Sequential bounded rebuild pipeline with one authority output.
fn stage_uuid_membership_rebuild_locked(
    project_dir: &Path,
    generation: u64,
    limits: UuidIndexBuildLimits,
    batch: &mut crate::staging::RewriteBatch,
) -> Result<UuidIndexBuildMetrics, GfError> {
    let limits = limits.validate()?;
    let root = project_dir.join(INDEX_DIR);
    fs::create_dir_all(&root).map_err(storage_err)?;
    let staging = project_dir
        .parent()
        .ok_or_else(|| storage_err("project directory has no staging parent"))?;
    let scratch = tempfile::Builder::new()
        .prefix("uuid-membership-build-")
        .tempdir_in(staging)
        .map_err(storage_err)?;
    let mut metrics = UuidIndexBuildMetrics::default();
    let node_paths = crate::mutator::node_parquet_files(project_dir).map_err(storage_err)?;
    let node_runs = scan_to_runs(
        &node_paths,
        "node_uuid",
        scratch.path(),
        "node",
        limits,
        &mut metrics,
    )?;
    let node_surrogate_runs = scan_entity_surrogate_runs(
        &node_paths,
        "node_uuid",
        "node_id",
        "node",
        scratch.path(),
        limits,
        &mut metrics,
    )?;
    let node_surrogate_validation_runs =
        scan_node_surrogate_validation_runs(&node_paths, scratch.path(), limits, &mut metrics)?;
    let mut edge_paths = crate::mutator::edge_parquet_files(project_dir, None)
        .map_err(storage_err)?
        .into_iter()
        .map(|(_, path)| path)
        .collect::<Vec<_>>();
    edge_paths.sort();
    let edge_runs = scan_to_runs(
        &edge_paths,
        "edge_uuid",
        scratch.path(),
        "edge",
        limits,
        &mut metrics,
    )?;
    let node_tmp = merge_all(
        node_runs,
        scratch.path(),
        "nodes",
        limits.merge_fan_in,
        &mut metrics,
    )?;
    let node_surrogates_tmp = merge_node_surrogate_runs(
        node_surrogate_runs,
        scratch.path(),
        limits.merge_fan_in,
        &mut metrics,
    )?;
    let validated_surrogates = merge_node_surrogate_validation_runs(
        node_surrogate_validation_runs,
        scratch.path(),
        limits.merge_fan_in,
        &mut metrics,
    )?;
    fs::remove_file(validated_surrogates).map_err(storage_err)?;
    let edge_tmp = merge_all(
        edge_runs,
        scratch.path(),
        "edges",
        limits.merge_fan_in,
        &mut metrics,
    )?;
    reject_cross_kind_identities(&node_tmp, &edge_tmp)?;
    let identity_tmp = scratch.path().join("identities-v5.run");
    build_identity_run(&node_surrogates_tmp, &edge_tmp, &identity_tmp)?;
    let surrogate_tmp =
        build_surrogate_run(&node_surrogates_tmp, scratch.path(), limits, &mut metrics)?;
    let identities = describe_staged_data(
        &identity_tmp,
        "identities-v5",
        generation,
        IDENTITY_RECORD_BYTES,
    )?;
    let node_surrogates = describe_staged_data(
        &surrogate_tmp,
        "node-surrogates-v5",
        generation,
        NODE_LOOKUP_RECORD_BYTES,
    )?;
    metrics.node_count = node_surrogates.count;
    metrics.edge_count = identities.count.saturating_sub(metrics.node_count);
    let manifest = Manifest {
        format_version: FORMAT_VERSION,
        base_generation: generation,
        current_generation: generation,
        live_node_count: metrics.node_count,
        live_edge_count: metrics.edge_count,
        runs: vec![RunRecord {
            base: true,
            level: 0,
            first_generation: 0,
            last_generation: generation,
            identities,
            node_surrogates,
            node_count: metrics.node_count,
            edge_count: metrics.edge_count,
            deleted_node_count: 0,
            deleted_edge_count: 0,
        }],
    };
    batch.stage_file(&root.join(&manifest.runs[0].identities.name), &identity_tmp)?;
    batch.stage_file(
        &root.join(&manifest.runs[0].node_surrogates.name),
        &surrogate_tmp,
    )?;
    batch.stage_bytes(
        &root.join(MANIFEST),
        &serde_json::to_vec(&manifest).map_err(storage_err)?,
    )?;
    Ok(metrics)
}

pub(super) fn manifest_generation(project_dir: &Path) -> Result<Option<u64>, GfError> {
    let bytes = match fs::read(project_dir.join(INDEX_DIR).join(MANIFEST)) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(storage_err(error)),
    };
    let manifest: Manifest = serde_json::from_slice(&bytes).map_err(storage_err)?;
    validate_run_descriptors(&manifest)?;
    Ok((manifest.format_version == FORMAT_VERSION).then_some(manifest.current_generation))
}

fn scan_to_runs(
    paths: &[PathBuf],
    column: &str,
    scratch: &Path,
    prefix: &str,
    limits: UuidIndexBuildLimits,
    metrics: &mut UuidIndexBuildMetrics,
) -> Result<Vec<PathBuf>, GfError> {
    let mut buffer = Vec::<[u8; 16]>::with_capacity(limits.run_records);
    let mut runs = Vec::new();
    for path in paths {
        if !path.exists() {
            continue;
        }
        let file = File::open(path).map_err(storage_err)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(storage_err)?
            .with_batch_size(limits.scan_batch_rows)
            .build()
            .map_err(storage_err)?;
        for batch in reader {
            let batch = batch.map_err(storage_err)?;
            let array = batch
                .column_by_name(column)
                .ok_or_else(|| storage_err(format!("{} lacks {column}", path.display())))?
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| storage_err(format!("{column} is not FixedSizeBinary")))?;
            for row in 0..array.len() {
                if array.is_null(row) || array.value(row).len() != 16 {
                    return Err(storage_err(format!("invalid {column} at row {row}")));
                }
                buffer.push(array.value(row).try_into().expect("length checked"));
                metrics.peak_buffered_records = metrics.peak_buffered_records.max(buffer.len());
                if buffer.len() == limits.run_records {
                    flush_run(&mut buffer, scratch, prefix, &mut runs, metrics)?;
                }
            }
        }
    }
    if !buffer.is_empty() {
        flush_run(&mut buffer, scratch, prefix, &mut runs, metrics)?;
    }
    if runs.is_empty() {
        let path = scratch.join(format!("{prefix}-empty.run"));
        File::create(&path)
            .map_err(storage_err)?
            .sync_all()
            .map_err(storage_err)?;
        runs.push(path);
    }
    Ok(runs)
}

pub(super) fn build_identity_run(nodes: &Path, edges: &Path, output: &Path) -> Result<(), GfError> {
    let mut node_reader = BufReader::new(File::open(nodes).map_err(storage_err)?);
    let mut edge_reader = BufReader::new(File::open(edges).map_err(storage_err)?);
    let mut node = read_node_surrogate_record(&mut node_reader)?;
    let mut edge = read_record(&mut edge_reader)?;
    let mut out = File::create(output).map_err(storage_err)?;
    let mut block = Vec::with_capacity(BULK_IO_BYTES);
    while node.is_some() || edge.is_some() {
        let take_node = match (&node, &edge) {
            (Some((node_uuid, _)), Some(edge_uuid)) => {
                if node_uuid == edge_uuid {
                    return Err(storage_err("UUID occurs in both identity domains"));
                }
                node_uuid < edge_uuid
            }
            (Some(_), None) => true,
            _ => false,
        };
        let (uuid, surrogate, kind) = if take_node {
            let (uuid, surrogate) = node.take().expect("node present");
            node = read_node_surrogate_record(&mut node_reader)?;
            (uuid, surrogate, 0_u8)
        } else {
            let uuid = edge.take().expect("edge present");
            edge = read_record(&mut edge_reader)?;
            (uuid, 0, 1_u8)
        };
        let mut record = [0_u8; IDENTITY_RECORD_WIDTH];
        record[..16].copy_from_slice(&uuid);
        record[16] = kind;
        record[17..].copy_from_slice(&surrogate.to_be_bytes());
        if block.len() + IDENTITY_RECORD_WIDTH > BULK_IO_BYTES {
            out.write_all(&block).map_err(storage_err)?;
            block.clear();
        }
        block.extend_from_slice(identity_codec::encoded(&record)?);
    }
    if !block.is_empty() {
        out.write_all(&block).map_err(storage_err)?;
    }
    out.flush().map_err(storage_err)?;
    out.sync_all().map_err(storage_err)
}

pub(super) fn build_surrogate_run(
    nodes: &Path,
    scratch: &Path,
    limits: UuidIndexBuildLimits,
    metrics: &mut UuidIndexBuildMetrics,
) -> Result<PathBuf, GfError> {
    let mut reader = BufReader::new(File::open(nodes).map_err(storage_err)?);
    let mut buffer = Vec::with_capacity(limits.run_records);
    let mut runs = Vec::new();
    while let Some((uuid, surrogate)) = read_node_surrogate_record(&mut reader)? {
        buffer.push((surrogate, uuid));
        metrics.peak_buffered_records = metrics.peak_buffered_records.max(buffer.len());
        if buffer.len() == limits.run_records {
            flush_surrogate_run(&mut buffer, scratch, &mut runs, metrics)?;
        }
    }
    if !buffer.is_empty() {
        flush_surrogate_run(&mut buffer, scratch, &mut runs, metrics)?;
    }
    if runs.is_empty() {
        let path = scratch.join("surrogates-empty.run");
        File::create(&path)
            .and_then(|file| file.sync_all())
            .map_err(storage_err)?;
        runs.push(path);
    }
    let mut round = 0;
    while runs.len() > 1 {
        let mut next = Vec::new();
        for (group, inputs) in runs.chunks(limits.merge_fan_in).enumerate() {
            let output = scratch.join(format!("surrogates-merge-{round}-{group}.run"));
            merge_surrogate_runs(inputs, &output)?;
            next.push(output);
        }
        for run in runs {
            let _ = fs::remove_file(run);
        }
        runs = next;
        round += 1;
    }
    Ok(runs.pop().expect("surrogate run exists"))
}

fn flush_surrogate_run(
    buffer: &mut Vec<(u64, [u8; 16])>,
    scratch: &Path,
    runs: &mut Vec<PathBuf>,
    metrics: &mut UuidIndexBuildMetrics,
) -> Result<(), GfError> {
    buffer.sort_unstable();
    if buffer.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(storage_err("duplicate node surrogate"));
    }
    let path = scratch.join(format!("surrogates-{:08}.run", runs.len()));
    let mut bytes = Vec::with_capacity(buffer.len() * 24);
    for (surrogate, uuid) in buffer.iter() {
        bytes.extend_from_slice(&surrogate.to_be_bytes());
        bytes.extend_from_slice(uuid);
    }
    let mut file = File::create(&path).map_err(storage_err)?;
    file.write_all(&bytes).map_err(storage_err)?;
    file.sync_all().map_err(storage_err)?;
    buffer.clear();
    runs.push(path);
    metrics.temporary_runs += 1;
    Ok(())
}

pub(super) fn merge_surrogate_runs(inputs: &[PathBuf], output: &Path) -> Result<(), GfError> {
    let mut readers = inputs
        .iter()
        .map(|path| File::open(path).map(BufReader::new).map_err(storage_err))
        .collect::<Result<Vec<_>, _>>()?;
    let mut heap = BinaryHeap::<Reverse<((u64, [u8; 16]), usize)>>::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(record) = read_surrogate_record(reader)? {
            heap.push(Reverse((record, index)));
        }
    }
    let mut out = File::create(output).map_err(storage_err)?;
    let mut block = Vec::with_capacity(BULK_IO_BYTES);
    let mut previous = None;
    while let Some(Reverse(((surrogate, uuid), index))) = heap.pop() {
        if previous.is_some_and(|(prior, _)| prior == surrogate) {
            if previous.is_some_and(|(_, prior_uuid)| prior_uuid == uuid) {
                if let Some(record) = read_surrogate_record(&mut readers[index])? {
                    heap.push(Reverse((record, index)));
                }
                continue;
            }
            return Err(storage_err("duplicate node surrogate across runs"));
        }
        if block.len() + 24 > BULK_IO_BYTES {
            out.write_all(&block).map_err(storage_err)?;
            block.clear();
        }
        block.extend_from_slice(&surrogate.to_be_bytes());
        block.extend_from_slice(&uuid);
        previous = Some((surrogate, uuid));
        if let Some(record) = read_surrogate_record(&mut readers[index])? {
            heap.push(Reverse((record, index)));
        }
    }
    if !block.is_empty() {
        out.write_all(&block).map_err(storage_err)?;
    }
    out.flush().map_err(storage_err)?;
    out.sync_all().map_err(storage_err)
}

pub(super) fn read_surrogate_record(
    reader: &mut BufReader<File>,
) -> Result<Option<(u64, [u8; 16])>, GfError> {
    let Some(record) = read_exact_record::<24>(reader)? else {
        return Ok(None);
    };
    Ok(Some((
        u64::from_be_bytes(record[..8].try_into().expect("fixed")),
        record[8..].try_into().expect("fixed"),
    )))
}

pub(super) fn read_exact_record<const N: usize>(
    reader: &mut impl Read,
) -> Result<Option<[u8; N]>, GfError> {
    let mut record = [0_u8; N];
    let mut filled = 0;
    while filled < N {
        match reader.read(&mut record[filled..]).map_err(storage_err)? {
            0 if filled == 0 => return Ok(None),
            0 => return Err(storage_err("truncated fixed-width index record")),
            read => filled += read,
        }
    }
    Ok(Some(record))
}

fn flush_run(
    buffer: &mut Vec<[u8; 16]>,
    scratch: &Path,
    prefix: &str,
    runs: &mut Vec<PathBuf>,
    metrics: &mut UuidIndexBuildMetrics,
) -> Result<(), GfError> {
    buffer.sort_unstable();
    if buffer.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(storage_err(format!(
            "duplicate {prefix} UUID in canonical topology"
        )));
    }
    let path = scratch.join(format!("{prefix}-{:08}.run", runs.len()));
    let mut out = File::create(&path).map_err(storage_err)?;
    let mut block = Vec::with_capacity(buffer.len().min(BULK_IO_BYTES / 16) * 16);
    for value in buffer.iter() {
        block.extend_from_slice(value);
    }
    if !block.is_empty() {
        out.write_all(&block).map_err(storage_err)?;
    }
    out.sync_all().map_err(storage_err)?;
    buffer.clear();
    runs.push(path);
    metrics.temporary_runs += 1;
    Ok(())
}

fn merge_all(
    mut runs: Vec<PathBuf>,
    scratch: &Path,
    prefix: &str,
    fan_in: usize,
    metrics: &mut UuidIndexBuildMetrics,
) -> Result<PathBuf, GfError> {
    let mut round = 0;
    while runs.len() > 1 {
        let mut next = Vec::new();
        for (group, chunk) in runs.chunks(fan_in).enumerate() {
            let path = scratch.join(format!("{prefix}-merge-{round}-{group}.run"));
            merge_runs(chunk, &path)?;
            next.push(path);
            metrics.temporary_runs += 1;
        }
        for path in runs {
            let _ = fs::remove_file(path);
        }
        runs = next;
        round += 1;
    }
    Ok(runs.pop().expect("at least one run"))
}

fn merge_runs(inputs: &[PathBuf], output: &Path) -> Result<(), GfError> {
    let mut readers = inputs
        .iter()
        .map(|p| File::open(p).map(BufReader::new).map_err(storage_err))
        .collect::<Result<Vec<_>, _>>()?;
    let mut heap = BinaryHeap::<Reverse<([u8; 16], usize)>>::new();
    for (idx, reader) in readers.iter_mut().enumerate() {
        if let Some(value) = read_record(reader)? {
            heap.push(Reverse((value, idx)));
        }
    }
    let mut out = File::create(output).map_err(storage_err)?;
    let mut block = Vec::with_capacity(BULK_IO_BYTES);
    let mut previous = None;
    while let Some(Reverse((value, idx))) = heap.pop() {
        if previous == Some(value) {
            return Err(storage_err("duplicate UUID across external index runs"));
        }
        if block.len() + 16 > BULK_IO_BYTES {
            out.write_all(&block).map_err(storage_err)?;
            block.clear();
        }
        block.extend_from_slice(&value);
        previous = Some(value);
        if let Some(next) = read_record(&mut readers[idx])? {
            heap.push(Reverse((next, idx)));
        }
    }
    if !block.is_empty() {
        out.write_all(&block).map_err(storage_err)?;
    }
    out.sync_all().map_err(storage_err)?;
    Ok(())
}

fn scan_entity_surrogate_runs(
    paths: &[PathBuf],
    uuid_column: &str,
    surrogate_column: &str,
    prefix: &str,
    scratch: &Path,
    limits: UuidIndexBuildLimits,
    metrics: &mut UuidIndexBuildMetrics,
) -> Result<Vec<PathBuf>, GfError> {
    let mut buffer = Vec::<([u8; 16], u64)>::with_capacity(limits.run_records);
    let mut runs = Vec::new();
    for path in paths {
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(File::open(path).map_err(storage_err)?)
                .map_err(storage_err)?
                .with_batch_size(limits.scan_batch_rows)
                .build()
                .map_err(storage_err)?;
        for batch in reader {
            let batch = batch.map_err(storage_err)?;
            let uuids = batch
                .column_by_name(uuid_column)
                .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
                .ok_or_else(|| storage_err(format!("{} lacks {uuid_column}", path.display())))?;
            let surrogates = batch
                .column_by_name(surrogate_column)
                .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
                .ok_or_else(|| {
                    storage_err(format!("{} lacks {surrogate_column}", path.display()))
                })?;
            if uuids.len() != surrogates.len() {
                return Err(storage_err(
                    "identity UUID and surrogate columns differ in length",
                ));
            }
            for row in 0..uuids.len() {
                if uuids.is_null(row) || uuids.value(row).len() != 16 || surrogates.is_null(row) {
                    return Err(storage_err(format!("invalid entity identity at row {row}")));
                }
                buffer.push((
                    uuids.value(row).try_into().expect("length checked"),
                    surrogates.value(row),
                ));
                metrics.peak_buffered_records = metrics.peak_buffered_records.max(buffer.len());
                if buffer.len() == limits.run_records {
                    flush_entity_surrogate_run(&mut buffer, scratch, prefix, &mut runs, metrics)?;
                }
            }
        }
    }
    if !buffer.is_empty() {
        flush_entity_surrogate_run(&mut buffer, scratch, prefix, &mut runs, metrics)?;
    }
    if runs.is_empty() {
        let path = scratch.join(format!("{prefix}-surrogates-empty.run"));
        File::create(&path)
            .map_err(storage_err)?
            .sync_all()
            .map_err(storage_err)?;
        runs.push(path);
    }
    Ok(runs)
}

fn canonical_node_topology_inventory_path(relative: &str) -> bool {
    relative == "topology/nodes.parquet"
        || relative
            .strip_prefix("topology/nodes/")
            .is_some_and(|name| !name.contains('/') && name.ends_with(".parquet"))
}

#[allow(clippy::too_many_arguments)]
fn scan_pinned_entity_surrogate_runs(
    inputs: &[crate::project_generation::PinnedGraphFile],
    uuid_column: &str,
    surrogate_column: &str,
    prefix: &str,
    scratch: &Path,
    limits: UuidIndexBuildLimits,
    metrics: &mut UuidIndexBuildMetrics,
) -> Result<Vec<PathBuf>, GfError> {
    let mut buffer = Vec::<([u8; 16], u64)>::with_capacity(limits.run_records);
    let mut runs = Vec::new();
    for input in inputs {
        let identity = graphforge_filesystem::file_identity(&input.file).map_err(storage_err)?;
        if identity != input.identity
            || input.file.metadata().map_err(storage_err)?.len() != input.entry.byte_length
        {
            return Err(storage_err(
                "authenticated topology payload identity changed before scan",
            ));
        }
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(input.file.try_clone().map_err(storage_err)?)
                .map_err(storage_err)?
                .with_batch_size(limits.scan_batch_rows)
                .build()
                .map_err(storage_err)?;
        for batch in reader {
            let batch = batch.map_err(storage_err)?;
            let uuids = batch
                .column_by_name(uuid_column)
                .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
                .ok_or_else(|| {
                    storage_err(format!("{} lacks {uuid_column}", input.entry.relative_path))
                })?;
            let surrogates = batch
                .column_by_name(surrogate_column)
                .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
                .ok_or_else(|| {
                    storage_err(format!(
                        "{} lacks {surrogate_column}",
                        input.entry.relative_path
                    ))
                })?;
            if uuids.len() != surrogates.len() {
                return Err(storage_err(
                    "identity UUID and surrogate columns differ in length",
                ));
            }
            for row in 0..uuids.len() {
                if uuids.is_null(row) || uuids.value(row).len() != 16 || surrogates.is_null(row) {
                    return Err(storage_err(format!("invalid entity identity at row {row}")));
                }
                buffer.push((
                    uuids.value(row).try_into().expect("length checked"),
                    surrogates.value(row),
                ));
                metrics.peak_buffered_records = metrics.peak_buffered_records.max(buffer.len());
                if buffer.len() == limits.run_records {
                    flush_entity_surrogate_run(&mut buffer, scratch, prefix, &mut runs, metrics)?;
                }
            }
        }
        if graphforge_filesystem::file_identity(&input.file).map_err(storage_err)? != input.identity
        {
            return Err(storage_err(
                "authenticated topology payload identity changed during scan",
            ));
        }
    }
    if !buffer.is_empty() {
        flush_entity_surrogate_run(&mut buffer, scratch, prefix, &mut runs, metrics)?;
    }
    if runs.is_empty() {
        let path = scratch.join(format!("{prefix}-surrogates-empty.run"));
        File::create(&path)
            .map_err(storage_err)?
            .sync_all()
            .map_err(storage_err)?;
        runs.push(path);
    }
    Ok(runs)
}

fn scan_node_surrogate_validation_runs(
    paths: &[PathBuf],
    scratch: &Path,
    limits: UuidIndexBuildLimits,
    metrics: &mut UuidIndexBuildMetrics,
) -> Result<Vec<PathBuf>, GfError> {
    let mut buffer = Vec::<u64>::with_capacity(limits.run_records);
    let mut runs = Vec::new();
    for path in paths {
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(File::open(path).map_err(storage_err)?)
                .map_err(storage_err)?
                .with_batch_size(limits.scan_batch_rows)
                .build()
                .map_err(storage_err)?;
        for batch in reader {
            let batch = batch.map_err(storage_err)?;
            let surrogates = batch
                .column_by_name("node_id")
                .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
                .ok_or_else(|| storage_err(format!("{} lacks node_id", path.display())))?;
            for row in 0..surrogates.len() {
                if surrogates.is_null(row) || surrogates.value(row) == 0 {
                    return Err(storage_err(format!("invalid node surrogate at row {row}")));
                }
                buffer.push(surrogates.value(row));
                metrics.peak_buffered_records = metrics.peak_buffered_records.max(buffer.len());
                if buffer.len() == limits.run_records {
                    flush_node_surrogate_validation_run(&mut buffer, scratch, &mut runs, metrics)?;
                }
            }
        }
    }
    if !buffer.is_empty() {
        flush_node_surrogate_validation_run(&mut buffer, scratch, &mut runs, metrics)?;
    }
    if runs.is_empty() {
        let path = scratch.join("node-surrogate-validation-empty.run");
        File::create(&path)
            .map_err(storage_err)?
            .sync_all()
            .map_err(storage_err)?;
        runs.push(path);
    }
    Ok(runs)
}

fn flush_node_surrogate_validation_run(
    buffer: &mut Vec<u64>,
    scratch: &Path,
    runs: &mut Vec<PathBuf>,
    metrics: &mut UuidIndexBuildMetrics,
) -> Result<(), GfError> {
    buffer.sort_unstable();
    if buffer.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(storage_err(
            "duplicate node surrogate in canonical topology",
        ));
    }
    let path = scratch.join(format!("node-surrogate-validation-{:08}.run", runs.len()));
    let mut bytes = Vec::with_capacity(buffer.len() * 8);
    for surrogate in buffer.iter() {
        bytes.extend_from_slice(&surrogate.to_le_bytes());
    }
    let mut file = File::create(&path).map_err(storage_err)?;
    if !bytes.is_empty() {
        file.write_all(&bytes).map_err(storage_err)?;
    }
    file.sync_all().map_err(storage_err)?;
    buffer.clear();
    runs.push(path);
    metrics.temporary_runs += 1;
    Ok(())
}

fn merge_node_surrogate_validation_runs(
    mut runs: Vec<PathBuf>,
    scratch: &Path,
    fan_in: usize,
    metrics: &mut UuidIndexBuildMetrics,
) -> Result<PathBuf, GfError> {
    let mut round = 0;
    while runs.len() > 1 {
        let mut next = Vec::new();
        for (group, chunk) in runs.chunks(fan_in).enumerate() {
            let path = scratch.join(format!(
                "node-surrogate-validation-merge-{round}-{group}.run"
            ));
            merge_node_surrogate_validation_group(chunk, &path)?;
            next.push(path);
            metrics.temporary_runs += 1;
        }
        for path in runs {
            let _ = fs::remove_file(path);
        }
        runs = next;
        round += 1;
    }
    Ok(runs.pop().expect("surrogate validation run exists"))
}

fn merge_node_surrogate_validation_group(inputs: &[PathBuf], output: &Path) -> Result<(), GfError> {
    let mut readers = inputs
        .iter()
        .map(|path| File::open(path).map(BufReader::new).map_err(storage_err))
        .collect::<Result<Vec<_>, _>>()?;
    let mut heap = BinaryHeap::<Reverse<(u64, usize)>>::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(value) = read_validation_surrogate(reader)? {
            heap.push(Reverse((value, index)));
        }
    }
    let mut bytes = Vec::with_capacity(BULK_IO_BYTES);
    let mut out = File::create(output).map_err(storage_err)?;
    let mut previous = None;
    while let Some(Reverse((value, index))) = heap.pop() {
        if previous == Some(value) {
            return Err(storage_err(
                "duplicate node surrogate across external index runs",
            ));
        }
        if bytes.len() + 8 > BULK_IO_BYTES {
            out.write_all(&bytes).map_err(storage_err)?;
            bytes.clear();
        }
        bytes.extend_from_slice(&value.to_le_bytes());
        previous = Some(value);
        if let Some(next) = read_validation_surrogate(&mut readers[index])? {
            heap.push(Reverse((next, index)));
        }
    }
    if !bytes.is_empty() {
        out.write_all(&bytes).map_err(storage_err)?;
    }
    out.sync_all().map_err(storage_err)
}

fn read_validation_surrogate(reader: &mut impl Read) -> Result<Option<u64>, GfError> {
    Ok(read_exact_record::<8>(reader)?.map(u64::from_le_bytes))
}

pub(super) fn flush_entity_surrogate_run(
    buffer: &mut Vec<([u8; 16], u64)>,
    scratch: &Path,
    prefix: &str,
    runs: &mut Vec<PathBuf>,
    metrics: &mut UuidIndexBuildMetrics,
) -> Result<(), GfError> {
    buffer.sort_unstable_by_key(|record| record.0);
    if buffer.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(storage_err("duplicate UUID in canonical topology"));
    }
    let path = scratch.join(format!("{prefix}-surrogates-{:08}.run", runs.len()));
    let mut out = File::create(&path).map_err(storage_err)?;
    let mut block = Vec::with_capacity(buffer.len().min(BULK_IO_BYTES / 24) * 24);
    for (uuid, surrogate) in buffer.iter() {
        block.extend_from_slice(uuid);
        block.extend_from_slice(&surrogate.to_le_bytes());
    }
    if !block.is_empty() {
        out.write_all(&block).map_err(storage_err)?;
    }
    out.sync_all().map_err(storage_err)?;
    buffer.clear();
    runs.push(path);
    metrics.temporary_runs += 1;
    Ok(())
}

pub(super) fn merge_node_surrogate_runs(
    mut runs: Vec<PathBuf>,
    scratch: &Path,
    fan_in: usize,
    metrics: &mut UuidIndexBuildMetrics,
) -> Result<PathBuf, GfError> {
    let mut round = 0;
    while runs.len() > 1 {
        let mut next = Vec::new();
        for (group, chunk) in runs.chunks(fan_in).enumerate() {
            let path = scratch.join(format!("node-surrogates-merge-{round}-{group}.run"));
            merge_node_surrogate_group(chunk, &path)?;
            next.push(path);
            metrics.temporary_runs += 1;
        }
        for path in runs {
            let _ = fs::remove_file(path);
        }
        runs = next;
        round += 1;
    }
    Ok(runs.pop().expect("at least one node-surrogate run"))
}

fn merge_node_surrogate_group(inputs: &[PathBuf], output: &Path) -> Result<(), GfError> {
    let mut readers = inputs
        .iter()
        .map(|path| File::open(path).map(BufReader::new).map_err(storage_err))
        .collect::<Result<Vec<_>, _>>()?;
    let mut heap = BinaryHeap::<Reverse<(([u8; 16], u64), usize)>>::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(record) = read_node_surrogate_record(reader)? {
            heap.push(Reverse((record, index)));
        }
    }
    let mut out = File::create(output).map_err(storage_err)?;
    let mut block = Vec::with_capacity(BULK_IO_BYTES);
    let mut previous = None;
    while let Some(Reverse(((uuid, surrogate), index))) = heap.pop() {
        if previous == Some(uuid) {
            return Err(storage_err(
                "duplicate node UUID across external index runs",
            ));
        }
        if block.len() + 24 > BULK_IO_BYTES {
            out.write_all(&block).map_err(storage_err)?;
            block.clear();
        }
        block.extend_from_slice(&uuid);
        block.extend_from_slice(&surrogate.to_le_bytes());
        previous = Some(uuid);
        if let Some(record) = read_node_surrogate_record(&mut readers[index])? {
            heap.push(Reverse((record, index)));
        }
    }
    if !block.is_empty() {
        out.write_all(&block).map_err(storage_err)?;
    }
    out.sync_all().map_err(storage_err)?;
    Ok(())
}

pub(super) fn read_node_surrogate_record(
    reader: &mut BufReader<File>,
) -> Result<Option<([u8; 16], u64)>, GfError> {
    let Some(record) = read_exact_record::<24>(reader)? else {
        return Ok(None);
    };
    Ok(Some((
        record[..16].try_into().expect("fixed"),
        u64::from_le_bytes(record[16..].try_into().expect("fixed")),
    )))
}

fn reject_cross_kind_identities(nodes: &Path, edges: &Path) -> Result<(), GfError> {
    let mut node_reader = BufReader::new(File::open(nodes).map_err(storage_err)?);
    let mut edge_reader = BufReader::new(File::open(edges).map_err(storage_err)?);
    let mut node = read_record(&mut node_reader)?;
    let mut edge = read_record(&mut edge_reader)?;
    while let (Some(node_uuid), Some(edge_uuid)) = (node, edge) {
        match node_uuid.cmp(&edge_uuid) {
            std::cmp::Ordering::Less => node = read_record(&mut node_reader)?,
            std::cmp::Ordering::Greater => edge = read_record(&mut edge_reader)?,
            std::cmp::Ordering::Equal => {
                return Err(storage_err(
                    "UUID occurs in both node and edge identity domains",
                ));
            }
        }
    }
    Ok(())
}

fn read_record(reader: &mut BufReader<File>) -> Result<Option<[u8; 16]>, GfError> {
    read_exact_record::<16>(reader)
}

#[cfg(test)]
pub(super) fn publish_data(
    source: &Path,
    root: &Path,
    _staging: &Path,
    kind: &str,
    generation: u64,
    record_bytes: u64,
) -> Result<FileRecord, GfError> {
    let length = source.metadata().map_err(storage_err)?.len();
    if record_bytes != IDENTITY_RECORD_BYTES && length % record_bytes != 0 {
        return Err(storage_err("internal run has a partial index record"));
    }
    let mut input = File::open(source).map_err(storage_err)?;
    let (sha256, blocks, count) = describe_blocks(&mut input, record_bytes)?;
    let name = format!("{kind}-{generation}-{}.uuidx", &sha256[..16]);
    let directory = graphforge_filesystem::StableDirectory::open(root).map_err(storage_err)?;
    let target = std::ffi::OsStr::new(&name);
    if let Ok(mut existing) = directory.open_child_file(target) {
        if existing.metadata().map_err(storage_err)?.len() != length
            || sha256_reader(&mut existing)? != sha256
        {
            return Err(storage_err(
                "existing immutable run does not match its content name",
            ));
        }
    } else {
        let temp_name = std::ffi::OsString::from(format!(".run-{}.tmp", Uuid::new_v4()));
        let mut temp = directory
            .create_child_file(&temp_name)
            .map_err(storage_err)?;
        let temp_identity = graphforge_filesystem::file_identity(&temp).map_err(storage_err)?;
        let mut install = || -> Result<(), GfError> {
            let mut input = File::open(source).map_err(storage_err)?;
            std::io::copy(&mut input, &mut temp).map_err(storage_err)?;
            temp.sync_all().map_err(storage_err)?;
            match directory.link_child_into(&temp_name, &temp, temp_identity, &directory, target) {
                Ok(_) => Ok(()),
                Err(_) => {
                    let mut existing = directory.open_child_file(target).map_err(storage_err)?;
                    if existing.metadata().map_err(storage_err)?.len() != length
                        || sha256_reader(&mut existing)? != sha256
                    {
                        return Err(storage_err("concurrent immutable run mismatch"));
                    }
                    Ok(())
                }
            }
        };
        let result = install();
        let _ = directory.unlink_child_if_identity(&temp_name, temp_identity);
        result?;
        directory.sync().map_err(storage_err)?;
    }
    Ok(FileRecord {
        name,
        count,
        sha256,
        blocks,
    })
}

#[cfg(test)]
fn sha256_reader(reader: &mut impl Read) -> Result<String, GfError> {
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer).map_err(storage_err)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    let mut encoded = String::with_capacity(64);
    for byte in digest.finalize() {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    Ok(encoded)
}

#[cfg(test)]
mod tests;
