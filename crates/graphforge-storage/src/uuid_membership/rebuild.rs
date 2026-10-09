//! UUID and ordinal rebuild, migration, and bounded record sorting.
//! Sorting runs are process-owned scratch, not restart checkpoints. Flush/close
//! makes them available to readers; the final staged artifact owns durability.

use super::maintenance::selected_generation_for_graph_root;
use super::ordinal_artifacts::commit_v4_publications;
use super::ordinal_artifacts::V4AuthorityTransactionProof;
use super::ordinal_artifacts::V4ConstructionArtifactBundle;
use super::ordinal_artifacts::V4OrdinalConstructionWriter;
use super::storage_err;
use super::topology_delta::hex_sha256;
use super::TopologyIndexReceipt;
use super::UuidIndexBuildLimits;
use super::UuidIndexBuildMetrics;
use super::V4OrdinalBuildMetrics;
use super::V4OrdinalRebuildDisposition;
use super::V4OrdinalRebuildEvidence;
use super::BULK_IO_BYTES;
use super::INDEX_DIR;
use super::V4_ORDINAL_MANIFEST;
use super::V4_ORDINAL_RECEIPT;
use arrow::array::Array;
use arrow::array::FixedSizeBinaryArray;
use arrow::array::UInt64Array;
use graphforge_core::GfError;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
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
                kind: "uuid-membership/ordinal-v6".to_owned(),
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
        ..
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
        File::create(&path).map_err(storage_err)?;
        runs.push(path);
    }
    Ok(runs)
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
    out.flush().map_err(storage_err)?;
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
    out.flush().map_err(storage_err)?;
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
            ParquetRecordBatchReaderBuilder::try_new(crate::graph_admission::open_admitted(path)?)
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
        File::create(&path).map_err(storage_err)?;
        runs.push(path);
    }
    Ok(runs)
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
        File::create(&path).map_err(storage_err)?;
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
    file.flush().map_err(storage_err)?;
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
    out.flush().map_err(storage_err)
}

#[cfg(test)]
mod tests;
