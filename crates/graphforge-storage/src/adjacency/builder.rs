//! Bounded adjacency construction, spill scheduling, and manifest-last publication.

use super::{
    ALL_RELATIONS_STEM, AdjacencyManifestRow, BuildEntry, DEFAULT_ADJACENCY_BATCH_SIZE,
    DEFAULT_CSR_SHARD_EDGES, DEFAULT_CSR_SHARD_NODES, Direction, ShardedCsrWriter, adjacency_dir,
    csr_path, for_each_adjacency_edge_path, named_column, storage_err, string_column,
    uint64_column, usable_stem,
};
use graphforge_core::GfError;
use std::path::{Path, PathBuf};

#[cfg(test)]
mod tests;

/// Aggregate bounded-resource evidence for one adjacency build.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AdjacencyBuildMetrics {
    pub(crate) captured_artifacts: Vec<super::CapturedAdjacencyArtifact>,
    /// Projected source rows consumed from Parquet.
    pub source_rows: u64,
    /// Sorted spill runs written across relation and union accumulators.
    pub spill_runs: u64,
    /// Peak bytes charged to the spill session.
    pub spill_bytes: u64,
    /// Persisted CSR shards written across every relation/direction pair.
    pub csr_shards: u64,
    /// Largest number of entries retained by a CSR shard sink.
    pub peak_shard_edges: u64,
    /// Largest number of local CSR rows retained by a shard sink.
    pub peak_shard_nodes: u64,
}

/// Default in-memory entry budget before spilling a sorted run.
///
/// ~1M triples ≈ 24 MiB of raw entry storage before direction-keyed flush
/// copies. Peak working set remains a function of this budget (and the
/// configured memory/spill caps), not total edge count.
pub const DEFAULT_ADJACENCY_CHUNK_ROWS: usize = 1_048_576;
/// Maximum sorted runs opened concurrently by one merge pass.
pub const DEFAULT_ADJACENCY_MERGE_FAN_IN: usize = 64;

/// Spill subdirectory name under the artifact adjacency directory when no
/// explicit spill root is configured.
pub const ADJACENCY_SPILL_DIR_NAME: &str = ".spill";

const SPILL_RUN_MAGIC: &[u8; 8] = b"GFADJRUN";
const SPILL_RUN_VERSION: u32 = 1;
const BYTES_PER_KEYED_ENTRY: u64 = 24;
// One budget per merge, including compaction passes. A per-run MiB made
// query-time rebuild RSS grow as the number of spill runs approached fan-in.
const MERGE_READER_BUFFER_BYTES: usize = 1 << 20;

/// Bounded build policy for streamed adjacency construction (#336).
///
/// Peak memory is governed by [`chunk_rows`](Self::chunk_rows),
/// [`batch_size`](Self::batch_size), and optional
/// [`memory_budget_bytes`](Self::memory_budget_bytes) — not by total edge
/// count. Sorted runs spill under [`spill_dir`](Self::spill_dir) (or a
/// project-local `.spill` root) and are removed on success, failure, or
/// cancellation.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct AdjacencyBuildOptions {
    /// Maximum projected edge rows retained in memory per relation/union
    /// accumulator before flushing a sorted spill run.
    pub chunk_rows: usize,
    /// Parquet `RecordBatch` size for the projected streaming reader.
    pub batch_size: usize,
    /// Optional absolute spill directory. When `None`, spill files live under
    /// `indexes/adjacency/.spill/` inside the artifact project root.
    pub spill_dir: Option<PathBuf>,
    /// Optional upper bound on temporary spill bytes. Exceeding the cap fails
    /// closed with [`ApiErrorCode::ResourceLimit`].
    pub spill_max_bytes: Option<u64>,
    /// Soft memory budget used to shrink [`chunk_rows`](Self::chunk_rows) when
    /// set. Does not replace the hard spill-byte cap.
    pub memory_budget_bytes: Option<u64>,
    /// Hard upper bound on adjacency entries retained by one CSR shard sink.
    /// A single high-degree row is split across consecutive shards when needed.
    pub shard_max_edges: usize,
    /// Hard upper bound on local CSR rows (offset entries minus one) per shard.
    pub shard_max_nodes: usize,
    /// Maximum spill runs opened in one k-way merge pass (minimum 2).
    pub merge_fan_in: usize,
}

impl Default for AdjacencyBuildOptions {
    fn default() -> Self {
        Self {
            chunk_rows: DEFAULT_ADJACENCY_CHUNK_ROWS,
            batch_size: DEFAULT_ADJACENCY_BATCH_SIZE,
            spill_dir: None,
            spill_max_bytes: None,
            memory_budget_bytes: None,
            shard_max_edges: DEFAULT_CSR_SHARD_EDGES,
            shard_max_nodes: DEFAULT_CSR_SHARD_NODES,
            merge_fan_in: DEFAULT_ADJACENCY_MERGE_FAN_IN,
        }
    }
}

impl AdjacencyBuildOptions {
    /// Resolve effective chunk/batch sizes after applying the optional memory
    /// budget. Batch size and chunk rows are always at least 1.
    #[must_use]
    pub fn effective(&self) -> Self {
        let mut out = self.clone();
        out.batch_size = out.batch_size.max(1);
        out.shard_max_edges = out.shard_max_edges.max(1);
        out.shard_max_nodes = out.shard_max_nodes.max(1);
        out.merge_fan_in = out.merge_fan_in.max(2);
        let mut chunk = out.chunk_rows.max(1);
        if let Some(budget) = out.memory_budget_bytes.filter(|b| *b > 0) {
            // Leave headroom for out+in keyed copies (~2×) plus CSR/merge state.
            let entry_budget = budget / (BYTES_PER_KEYED_ENTRY * 4);
            if entry_budget > 0 {
                chunk = chunk.min(usize::try_from(entry_budget).unwrap_or(usize::MAX).max(1));
            }
        }
        out.chunk_rows = chunk;
        out
    }
}

fn resource_limit(message: impl Into<String>) -> GfError {
    GfError::Api {
        code: graphforge_core::ApiErrorCode::ResourceLimit,
        message: message.into(),
    }
}

/// Build the full adjacency index for the project under `indexes/adjacency/`:
/// one `{out, in}` CSR pair per relation type found in `topology/edges/`, plus
/// the [`ALL_RELATIONS_STEM`] union pair, then `index_manifest.parquet`
/// **last** (the build-ordering convention in the module docs).
///
/// Mode-agnostic: `_exploratory.parquet` rows are grouped by their
/// `rel_type_name` column; every other file is a typed edge table keyed by its
/// file stem. Relation names that are not usable as a file stem (path
/// separators, `..`, empty) or that collide with the reserved
/// [`ALL_RELATIONS_STEM`] are skipped — those relations are served by
/// scan-build forever, but their rows still flow into the union index.
///
/// The project `topology_generation` is read **before** any edge scan and
/// stamped into the manifest: a concurrent topology write mid-build bumps the
/// counter past the stamp, so a racing build can only produce an index that
/// reads as *stale*, never as falsely fresh.
///
/// Determinism (R-ADJ-2): `out` entries sort by `(src_id, edge_id)` and `in`
/// entries by `(dst_id, edge_id)`, so CSR bytes are reproducible from
/// `topology/` alone. Because edge files are ascending in `edge_id`, the
/// per-node entry order equals edge-file row order — the same order the
/// scan-build path produces.
///
/// Returns the manifest rows written. An empty project still writes the
/// (empty) union pair plus the manifest, so an explicit build always creates a
/// well-formed capability directory.
///
/// # Errors
/// Returns [`GfError::Storage`] on any read, build, or write failure; the
/// manifest is only written after every CSR file succeeded. Resource exhaustion
/// (spill cap) returns [`ApiErrorCode::ResourceLimit`].
pub fn build_adjacency_index(
    project_dir: &Path,
    built_at_micros: i64,
) -> Result<Vec<AdjacencyManifestRow>, GfError> {
    build_adjacency_index_with_checkpoint(project_dir, built_at_micros, || Ok(()))
}

/// Cancellation-aware variant of [`build_adjacency_index`].
pub fn build_adjacency_index_with_checkpoint(
    project_dir: &Path,
    built_at_micros: i64,
    mut checkpoint: impl FnMut() -> Result<(), GfError>,
) -> Result<Vec<AdjacencyManifestRow>, GfError> {
    build_adjacency_index_into(project_dir, project_dir, built_at_micros, &mut checkpoint)
}

/// Build from canonical topology in `source_project_dir` into a separate
/// private artifact project root. The caller publishes the completed directory.
pub fn build_adjacency_index_into(
    source_project_dir: &Path,
    artifact_project_dir: &Path,
    built_at_micros: i64,
    mut checkpoint: impl FnMut() -> Result<(), GfError>,
) -> Result<Vec<AdjacencyManifestRow>, GfError> {
    build_adjacency_index_into_with_options(
        source_project_dir,
        artifact_project_dir,
        built_at_micros,
        &AdjacencyBuildOptions::default(),
        &mut checkpoint,
    )
}

/// Bounded / streaming variant of [`build_adjacency_index_into`].
pub fn build_adjacency_index_into_with_options(
    source_project_dir: &Path,
    artifact_project_dir: &Path,
    built_at_micros: i64,
    options: &AdjacencyBuildOptions,
    mut checkpoint: impl FnMut() -> Result<(), GfError>,
) -> Result<Vec<AdjacencyManifestRow>, GfError> {
    build_adjacency_index_into_with_metrics(
        source_project_dir,
        artifact_project_dir,
        built_at_micros,
        options,
        &mut checkpoint,
    )
    .map(|(manifest, _)| manifest)
}

/// Bounded build returning explicit source/spill/shard resource counters.
pub fn build_adjacency_index_into_with_metrics(
    source_project_dir: &Path,
    artifact_project_dir: &Path,
    built_at_micros: i64,
    options: &AdjacencyBuildOptions,
    mut checkpoint: impl FnMut() -> Result<(), GfError>,
) -> Result<(Vec<AdjacencyManifestRow>, AdjacencyBuildMetrics), GfError> {
    let inventory = super::capture_adjacency_inventory(source_project_dir)?;
    build_adjacency_index_from_inventory(
        source_project_dir,
        artifact_project_dir,
        Some(&inventory),
        built_at_micros,
        options,
        &mut checkpoint,
    )
}

/// Build derived adjacency using already-admitted route authority, without rehashing the graph.
/// `None` retains the explicit legacy raw-layout contract.
pub fn build_adjacency_index_from_inventory(
    source_project_dir: &Path,
    artifact_project_dir: &Path,
    inventory: Option<&crate::AuthenticatedPropertyInventory>,
    built_at_micros: i64,
    options: &AdjacencyBuildOptions,
    mut checkpoint: impl FnMut() -> Result<(), GfError>,
) -> Result<(Vec<AdjacencyManifestRow>, AdjacencyBuildMetrics), GfError> {
    checkpoint()?;
    // Generation BEFORE the scan — see the race note in the doc comment.
    let generation = crate::generation::read_topology_generation(source_project_dir)?;
    let edge_files = super::resolve_adjacency_edge_files(source_project_dir, inventory)?;
    build_adjacency_index_for_edge_files(
        artifact_project_dir,
        &edge_files,
        generation,
        built_at_micros,
        options,
        checkpoint,
    )
}

/// Build the index from an explicit `(relation, path)` edge-table list already
/// admitted by the caller, stamping `topology_generation` into the manifest.
///
/// This is the publish-side entry (#1388): construction encoding and portable
/// import hand over the exact edge tables of the generation they are about to
/// publish, so the CSR ships inside that generation instead of being rebuilt
/// by every query process. The relation names are the semantic routes the
/// caller resolved; no inventory or counter file is consulted here.
pub(crate) fn build_adjacency_index_for_edge_files(
    artifact_project_dir: &Path,
    edge_files: &[(String, PathBuf)],
    topology_generation: u64,
    built_at_micros: i64,
    options: &AdjacencyBuildOptions,
    checkpoint: impl FnMut() -> Result<(), GfError>,
) -> Result<(Vec<AdjacencyManifestRow>, AdjacencyBuildMetrics), GfError> {
    build_adjacency_index_for_edge_files_on_lanes(
        artifact_project_dir,
        edge_files,
        topology_generation,
        built_at_micros,
        options,
        None,
        checkpoint,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_adjacency_index_for_edge_files_on_lanes(
    artifact_project_dir: &Path,
    edge_files: &[(String, PathBuf)],
    topology_generation: u64,
    built_at_micros: i64,
    options: &AdjacencyBuildOptions,
    admission: Option<
        &std::sync::Arc<crate::graph_construction::cpu_admission::ConstructionCpuAdmission>,
    >,
    checkpoint: impl FnMut() -> Result<(), GfError>,
) -> Result<(Vec<AdjacencyManifestRow>, AdjacencyBuildMetrics), GfError> {
    build_adjacency_index_for_edge_files_observed(
        artifact_project_dir,
        edge_files,
        topology_generation,
        built_at_micros,
        options,
        admission,
        None,
        checkpoint,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_adjacency_index_for_edge_files_observed(
    artifact_project_dir: &Path,
    edge_files: &[(String, PathBuf)],
    topology_generation: u64,
    built_at_micros: i64,
    options: &AdjacencyBuildOptions,
    admission: Option<
        &std::sync::Arc<crate::graph_construction::cpu_admission::ConstructionCpuAdmission>,
    >,
    allocation: Option<&crate::StorageAllocationOperation>,
    mut checkpoint: impl FnMut() -> Result<(), GfError>,
) -> Result<(Vec<AdjacencyManifestRow>, AdjacencyBuildMetrics), GfError> {
    let generation = topology_generation;
    let options = options.effective();

    let adjacency = adjacency_dir(artifact_project_dir);
    std::fs::create_dir_all(&adjacency).map_err(storage_err)?;

    let spill_root = {
        let base = options
            .spill_dir
            .clone()
            .unwrap_or_else(|| adjacency.join(ADJACENCY_SPILL_DIR_NAME));
        // Unique per-build subdirectory so a shared #337 spill root is never
        // wiped, and concurrent builders cannot collide.
        base.join(format!("build-{}", uuid::Uuid::new_v4().as_simple()))
    };
    let mut spill = SpillSession::create(&spill_root)?.with_max_bytes(options.spill_max_bytes);
    spill.admission = admission.cloned();
    spill.allocation = allocation.cloned();
    let mut metrics = AdjacencyBuildMetrics::default();

    let build_result = (|| {
        let grouping_region =
            crate::concurrency_attribution::RegionScope::named("adjacency_grouping");
        let mut groups = stream_build_groups(
            edge_files,
            &options,
            &mut spill,
            &mut metrics,
            &mut checkpoint,
        )?;
        checkpoint()?;

        drop(grouping_region);
        let csr_region = crate::concurrency_attribution::RegionScope::named("adjacency_csr");
        let manifest = finish_groups(
            artifact_project_dir,
            &mut groups,
            generation,
            built_at_micros,
            &options,
            &mut spill,
            &mut metrics,
            admission,
            &mut checkpoint,
        )?;
        checkpoint()?;

        drop(csr_region);
        // Manifest LAST: a crash before this point leaves the manifest absent or
        // old, so a torn build always reads as stale.
        super::write_manifest_observed(artifact_project_dir, &manifest, allocation)?;
        checkpoint()?;

        // Phase-1 compaction (#765): the rebuilt base subsumes every delta segment
        // at or below the generation it was stamped with, so prune them. Segments
        // written by a concurrent append DURING the build (generation > stamp)
        // survive, so the new base + those is immediately fresh. Manifest first,
        // prune after: a crash between leaves dead segments a later prune removes.
        crate::adjacency_delta::prune_delta_segments(artifact_project_dir, generation);
        metrics.spill_runs = spill.run_counter;
        metrics.spill_bytes = spill.peak_bytes;
        Ok((manifest, metrics.clone()))
    })();

    match build_result {
        Ok(result) => {
            spill.cleanup()?;
            Ok(result)
        }
        Err(error) => {
            let _ = spill.cleanup();
            Err(error)
        }
    }
}

/// RAII spill directory: always removed on drop / explicit cleanup so cancel
/// and failure cannot leave temporary runs behind as a published artifact.
struct SpillSession {
    allocation: Option<crate::StorageAllocationOperation>,
    recorded_paths: std::collections::BTreeSet<PathBuf>,
    admission:
        Option<std::sync::Arc<crate::graph_construction::cpu_admission::ConstructionCpuAdmission>>,
    root: PathBuf,
    bytes_current: u64,
    peak_bytes: u64,
    max_bytes: Option<u64>,
    run_counter: u64,
    cleaned: bool,
}

impl SpillSession {
    fn create(root: &Path) -> Result<Self, GfError> {
        // Only create the per-build directory; never delete a caller-supplied
        // parent spill root (it may be shared with DataFusion / other ops).
        std::fs::create_dir_all(root).map_err(storage_err)?;
        Ok(Self {
            allocation: None,
            recorded_paths: std::collections::BTreeSet::new(),
            admission: None,
            root: root.to_path_buf(),
            bytes_current: 0,
            peak_bytes: 0,
            max_bytes: None,
            run_counter: 0,
            cleaned: false,
        })
    }

    fn with_max_bytes(mut self, max_bytes: Option<u64>) -> Self {
        self.max_bytes = max_bytes;
        self
    }

    fn next_run_path(&mut self, label: &str, direction: Direction) -> PathBuf {
        let id = self.run_counter;
        self.run_counter += 1;
        self.root.join(format!(
            "{}.{}.{id}.run",
            crate::route_component::component(label),
            direction.as_str()
        ))
    }

    fn account_write(&mut self, bytes: u64) -> Result<(), GfError> {
        self.bytes_current = self.bytes_current.saturating_add(bytes);
        self.peak_bytes = self.peak_bytes.max(self.bytes_current);
        if let Some(max) = self.max_bytes
            && self.bytes_current > max
        {
            return Err(resource_limit(format!(
                "adjacency build spill exceeded max_bytes ({max})"
            )));
        }
        Ok(())
    }

    fn remove_run(&mut self, path: &Path) -> Result<(), GfError> {
        let bytes = std::fs::metadata(path).map_err(storage_err)?.len();
        std::fs::remove_file(path).map_err(storage_err)?;
        if let Some(allocation) = &self.allocation {
            allocation.remove_file_at(path)?;
        }
        self.recorded_paths.remove(path);
        self.bytes_current = self.bytes_current.saturating_sub(bytes);
        Ok(())
    }

    fn observe_run(&mut self, path: &Path, file: &std::fs::File) -> Result<(), GfError> {
        if let Some(allocation) = &self.allocation {
            allocation.replace_file_at(path, file)?;
            self.recorded_paths.insert(path.to_path_buf());
        }
        Ok(())
    }

    fn cleanup(&mut self) -> Result<(), GfError> {
        if self.cleaned {
            return Ok(());
        }
        if let Some(allocation) = &self.allocation {
            std::fs::remove_dir_all(&self.root).map_err(storage_err)?;
            for path in &self.recorded_paths {
                allocation.remove_file_at(path)?;
            }
        } else {
            let _ = std::fs::remove_dir_all(&self.root);
        }
        // Best-effort: remove an empty project-local `.spill` parent we created.
        // Never delete a shared policy spill root that may hold other files.
        if let Some(parent) = self.root.parent()
            && parent
                .file_name()
                .is_some_and(|name| name == ADJACENCY_SPILL_DIR_NAME)
        {
            let _ = std::fs::remove_dir(parent);
        }
        self.cleaned = true;
        Ok(())
    }
}

impl Drop for SpillSession {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}

/// Per-relation (or union) accumulator that flushes sorted direction-keyed
/// runs when the chunk budget is reached.
#[derive(Default)]
struct EntryGroup {
    buffer: Vec<BuildEntry>,
    out_runs: Vec<PathBuf>,
    in_runs: Vec<PathBuf>,
    label: String,
}

impl EntryGroup {
    fn with_label(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            ..Self::default()
        }
    }

    fn push(
        &mut self,
        entry: BuildEntry,
        chunk_rows: usize,
        spill: &mut SpillSession,
        checkpoint: &mut dyn FnMut() -> Result<(), GfError>,
    ) -> Result<(), GfError> {
        self.buffer.push(entry);
        if self.buffer.len() >= chunk_rows {
            self.flush(spill, checkpoint)?;
        }
        Ok(())
    }

    fn flush(
        &mut self,
        spill: &mut SpillSession,
        checkpoint: &mut dyn FnMut() -> Result<(), GfError>,
    ) -> Result<(), GfError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        checkpoint()?;
        let label = if self.label.is_empty() {
            "group"
        } else {
            self.label.as_str()
        };
        // Out: (src, edge, dst); In: (dst, edge, src).
        let (out_keyed, in_keyed) =
            sorted_directions(&self.buffer, spill.admission.as_ref(), checkpoint)?;
        let out_path = spill.next_run_path(label, Direction::Out);
        write_keyed_run(&out_path, &out_keyed, spill)?;
        self.out_runs.push(out_path);

        let in_path = spill.next_run_path(label, Direction::In);
        write_keyed_run(&in_path, &in_keyed, spill)?;
        self.in_runs.push(in_path);

        self.buffer.clear();
        // Keep capacity so subsequent chunks avoid reallocation; peak retained
        // heap stays O(chunk_rows), not O(total edges).
        Ok(())
    }

    fn finish_sharded_csr(
        &mut self,
        direction: Direction,
        path: &Path,
        options: &AdjacencyBuildOptions,
        spill: &mut SpillSession,
        checkpoint: &mut dyn FnMut() -> Result<(), GfError>,
    ) -> Result<ShardedWriteOutcome, GfError> {
        self.prepare_direction(direction, options, spill, checkpoint)?;
        self.write_sharded_csr(
            direction,
            path,
            options,
            spill.allocation.as_ref(),
            checkpoint,
        )
    }

    fn prepare_direction(
        &mut self,
        direction: Direction,
        options: &AdjacencyBuildOptions,
        spill: &mut SpillSession,
        checkpoint: &mut dyn FnMut() -> Result<(), GfError>,
    ) -> Result<(), GfError> {
        let had_runs = match direction {
            Direction::Out => !self.out_runs.is_empty(),
            Direction::In => !self.in_runs.is_empty(),
        };
        if had_runs && !self.buffer.is_empty() {
            self.flush(spill, checkpoint)?;
        }
        if had_runs {
            let runs = match direction {
                Direction::Out => &mut self.out_runs,
                Direction::In => &mut self.in_runs,
            };
            compact_keyed_runs(
                runs,
                options.merge_fan_in,
                &self.label,
                direction,
                spill,
                checkpoint,
            )?;
        }
        Ok(())
    }

    fn write_sharded_csr(
        &self,
        direction: Direction,
        path: &Path,
        options: &AdjacencyBuildOptions,
        allocation: Option<&crate::StorageAllocationOperation>,
        checkpoint: &mut dyn FnMut() -> Result<(), GfError>,
    ) -> Result<ShardedWriteOutcome, GfError> {
        let had_runs = match direction {
            Direction::Out => !self.out_runs.is_empty(),
            Direction::In => !self.in_runs.is_empty(),
        };
        let mut writer =
            ShardedCsrWriter::create(path, options.shard_max_edges, options.shard_max_nodes)?;
        writer.allocation = allocation.cloned();
        let mut max_key = None::<u64>;
        let mut emit = |entry: (u64, u64, u64)| {
            max_key = Some(max_key.map_or(entry.0, |prior| prior.max(entry.0)));
            writer.emit(entry)
        };
        if had_runs {
            let runs = match direction {
                Direction::Out => &self.out_runs,
                Direction::In => &self.in_runs,
            };
            merge_keyed_runs(runs, checkpoint, &mut emit)?;
        } else {
            // The no-spill fast path remains bounded by `chunk_rows`.
            let mut keyed: Vec<_> = match direction {
                Direction::Out => self.buffer.clone(),
                Direction::In => self
                    .buffer
                    .iter()
                    .map(|&(src, edge, dst)| (dst, edge, src))
                    .collect(),
            };
            keyed.sort_unstable_by_key(|&(key, edge, _)| (key, edge));
            for (index, entry) in keyed.into_iter().enumerate() {
                if index.is_multiple_of(4096) {
                    checkpoint()?;
                }
                emit(entry)?;
            }
        }
        let node_count = max_key.map_or(0, |key| key.saturating_add(1));
        let edge_count = writer.edge_count;
        let (shards, peak_shard_edges, peak_shard_nodes, captured_artifacts) =
            writer.finish(node_count)?;
        Ok(ShardedWriteOutcome {
            captured_artifacts,
            node_count,
            edge_count,
            shards,
            peak_shard_edges,
            peak_shard_nodes,
        })
    }
}

struct ShardedWriteOutcome {
    captured_artifacts: Vec<super::CapturedAdjacencyArtifact>,
    node_count: u64,
    edge_count: u64,
    shards: u64,
    peak_shard_edges: u64,
    peak_shard_nodes: u64,
}

fn write_keyed_run(
    path: &Path,
    entries: &[(u64, u64, u64)],
    spill: &mut SpillSession,
) -> Result<(), GfError> {
    use std::io::{BufWriter, Write};
    let file = std::fs::File::create(path).map_err(storage_err)?;
    let mut file = BufWriter::with_capacity(1 << 20, file);
    let header = 8 + 4 + 8;
    let body = entries.len() as u64 * BYTES_PER_KEYED_ENTRY;
    spill.account_write(header + body)?;
    crate::lifecycle_io::record_write(crate::StorageIoPhase::ReadPathScan, header + body, 1);
    file.write_all(SPILL_RUN_MAGIC).map_err(storage_err)?;
    file.write_all(&SPILL_RUN_VERSION.to_le_bytes())
        .map_err(storage_err)?;
    file.write_all(&(entries.len() as u64).to_le_bytes())
        .map_err(storage_err)?;
    for &(key, edge, neighbor) in entries {
        file.write_all(&key.to_le_bytes()).map_err(storage_err)?;
        file.write_all(&edge.to_le_bytes()).map_err(storage_err)?;
        file.write_all(&neighbor.to_le_bytes())
            .map_err(storage_err)?;
    }
    // Spill runs are ephemeral (SpillSession removes them on success/failure/
    // cancel and never publish). Avoid per-run sync_all — it dominated >200M
    // build wall time on agent hosts without improving published-index safety.
    file.flush().map_err(storage_err)?;
    spill.observe_run(path, file.get_ref())?;
    Ok(())
}

/// Spill-run file whose reads are attributed as they reach the file, beneath
/// any buffering: calls count refills, not the 24-byte records decoded from
/// them (#1449).
type SpillRunFile = crate::lifecycle_io::ReadPathRead<std::fs::File>;

struct RunCursor {
    file: std::io::BufReader<SpillRunFile>,
    remaining: u64,
    current: Option<(u64, u64, u64)>,
}

impl RunCursor {
    fn open(path: &Path, buffer_bytes: usize) -> Result<Self, GfError> {
        use std::io::Read;
        let file = SpillRunFile::new(std::fs::File::open(path).map_err(storage_err)?);
        let mut file = std::io::BufReader::with_capacity(buffer_bytes, file);
        let mut magic = [0u8; 8];
        file.read_exact(&mut magic).map_err(storage_err)?;
        if &magic != SPILL_RUN_MAGIC {
            return Err(GfError::Storage(format!(
                "adjacency spill run {} has invalid magic",
                path.display()
            )));
        }
        let mut version = [0u8; 4];
        file.read_exact(&mut version).map_err(storage_err)?;
        if u32::from_le_bytes(version) != SPILL_RUN_VERSION {
            return Err(GfError::Storage(format!(
                "adjacency spill run {} has unsupported version",
                path.display()
            )));
        }
        let mut count = [0u8; 8];
        file.read_exact(&mut count).map_err(storage_err)?;
        let mut cursor = Self {
            file,
            remaining: u64::from_le_bytes(count),
            current: None,
        };
        cursor.pull()?;
        Ok(cursor)
    }

    fn pull(&mut self) -> Result<(), GfError> {
        use std::io::Read;
        if self.remaining == 0 {
            self.current = None;
            return Ok(());
        }
        let mut buf = [0u8; 24];
        self.file.read_exact(&mut buf).map_err(storage_err)?;
        let key = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let edge = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        let neighbor = u64::from_le_bytes(buf[16..24].try_into().unwrap());
        self.remaining -= 1;
        self.current = Some((key, edge, neighbor));
        Ok(())
    }
}

fn open_run_cursors(runs: &[PathBuf]) -> Result<Vec<RunCursor>, GfError> {
    let buffer_bytes = MERGE_READER_BUFFER_BYTES / runs.len().max(1);
    // Do not round up: even an unusually large caller-selected fan-in must
    // stay within the budget. BufReader with zero capacity reads directly.
    runs.iter()
        .map(|path| RunCursor::open(path, buffer_bytes))
        .collect()
}

fn compact_keyed_runs(
    runs: &mut Vec<PathBuf>,
    fan_in: usize,
    label: &str,
    direction: Direction,
    spill: &mut SpillSession,
    checkpoint: &mut dyn FnMut() -> Result<(), GfError>,
) -> Result<(), GfError> {
    let fan_in = fan_in.max(2);
    while runs.len() > fan_in {
        let mut next = Vec::with_capacity(runs.len().div_ceil(fan_in));
        for chunk in runs.chunks(fan_in) {
            checkpoint()?;
            if chunk.len() == 1 {
                next.push(chunk[0].clone());
                continue;
            }
            let output = spill.next_run_path(label, direction);
            merge_keyed_runs_to_run(chunk, &output, spill, checkpoint)?;
            for input in chunk {
                spill.remove_run(input)?;
            }
            next.push(output);
        }
        *runs = next;
    }
    Ok(())
}

fn keyed_run_count(path: &Path) -> Result<u64, GfError> {
    use std::io::Read;
    // Unbuffered: only the header is needed, so read exactly its 20 bytes and
    // attribute the calls that returned them.
    let mut file = SpillRunFile::new(std::fs::File::open(path).map_err(storage_err)?);
    let mut header = [0_u8; 20];
    file.read_exact(&mut header).map_err(storage_err)?;
    if &header[..8] != SPILL_RUN_MAGIC
        || u32::from_le_bytes(header[8..12].try_into().expect("four bytes")) != SPILL_RUN_VERSION
    {
        return Err(GfError::Storage(format!(
            "adjacency spill run {} has invalid header",
            path.display()
        )));
    }
    Ok(u64::from_le_bytes(
        header[12..20].try_into().expect("eight bytes"),
    ))
}

fn merge_keyed_runs_to_run(
    inputs: &[PathBuf],
    output: &Path,
    spill: &mut SpillSession,
    checkpoint: &mut dyn FnMut() -> Result<(), GfError>,
) -> Result<(), GfError> {
    use std::io::{BufWriter, Write};
    let count = inputs.iter().try_fold(0_u64, |total, path| {
        keyed_run_count(path).map(|count| total.saturating_add(count))
    })?;
    let bytes = 20_u64.saturating_add(count.saturating_mul(BYTES_PER_KEYED_ENTRY));
    spill.account_write(bytes)?;
    let mut writer =
        BufWriter::with_capacity(1 << 20, std::fs::File::create(output).map_err(storage_err)?);
    writer.write_all(SPILL_RUN_MAGIC).map_err(storage_err)?;
    writer
        .write_all(&SPILL_RUN_VERSION.to_le_bytes())
        .map_err(storage_err)?;
    writer
        .write_all(&count.to_le_bytes())
        .map_err(storage_err)?;
    merge_keyed_runs(inputs, checkpoint, &mut |(key, edge, neighbor)| {
        writer.write_all(&key.to_le_bytes()).map_err(storage_err)?;
        writer.write_all(&edge.to_le_bytes()).map_err(storage_err)?;
        writer
            .write_all(&neighbor.to_le_bytes())
            .map_err(storage_err)
    })?;
    writer.flush().map_err(storage_err)?;
    spill.observe_run(output, writer.get_ref())?;
    crate::lifecycle_io::record_write(crate::StorageIoPhase::ReadPathScan, bytes, 1);
    Ok(())
}

fn merge_keyed_runs(
    runs: &[PathBuf],
    checkpoint: &mut dyn FnMut() -> Result<(), GfError>,
    emit: &mut dyn FnMut((u64, u64, u64)) -> Result<(), GfError>,
) -> Result<(), GfError> {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    if runs.is_empty() {
        return Ok(());
    }

    let mut cursors = open_run_cursors(runs)?;
    // Min-heap by (key, edge, neighbor, cursor_index).
    let mut heap: BinaryHeap<Reverse<(u64, u64, u64, usize)>> = BinaryHeap::new();
    for (idx, cursor) in cursors.iter().enumerate() {
        if let Some((key, edge, neighbor)) = cursor.current {
            heap.push(Reverse((key, edge, neighbor, idx)));
        }
    }

    let mut seen = 0u64;
    while let Some(Reverse((key, edge, neighbor, idx))) = heap.pop() {
        if seen.is_multiple_of(65_536) {
            checkpoint()?;
        }
        seen += 1;

        emit((key, edge, neighbor))?;

        cursors[idx].pull()?;
        if let Some((k, e, n)) = cursors[idx].current {
            heap.push(Reverse((k, e, n, idx)));
        }
    }
    Ok(())
}

fn stream_build_groups(
    edge_files: &[(String, PathBuf)],
    options: &AdjacencyBuildOptions,
    spill: &mut SpillSession,
    metrics: &mut AdjacencyBuildMetrics,
    checkpoint: &mut dyn FnMut() -> Result<(), GfError>,
) -> Result<std::collections::BTreeMap<String, EntryGroup>, GfError> {
    spill.max_bytes = options.spill_max_bytes;
    let mut groups: std::collections::BTreeMap<String, EntryGroup> =
        std::collections::BTreeMap::new();
    groups.insert(
        ALL_RELATIONS_STEM.to_owned(),
        EntryGroup::with_label(ALL_RELATIONS_STEM),
    );

    let admission = spill.admission.clone();
    for_each_admitted_edge_batch(
        edge_files,
        options.batch_size,
        admission.as_ref(),
        &mut |stem, exploratory, batch| {
            checkpoint()?;
            let edge_ids = uint64_column(named_column(batch, "edge_id")?, "edge_id")?;
            let src_ids = uint64_column(named_column(batch, "src_id")?, "src_id")?;
            let dst_ids = uint64_column(named_column(batch, "dst_id")?, "dst_id")?;
            let rel_names = if exploratory {
                Some(string_column(
                    named_column(batch, "rel_type_name")?,
                    "rel_type_name",
                )?)
            } else {
                None
            };
            for i in 0..batch.num_rows() {
                metrics.source_rows = metrics.source_rows.saturating_add(1);
                let entry = (src_ids.value(i), edge_ids.value(i), dst_ids.value(i));
                groups
                    .get_mut(ALL_RELATIONS_STEM)
                    .expect("union group")
                    .push(entry, options.chunk_rows, spill, checkpoint)?;
                let rel = rel_names.map_or(stem, |names| names.value(i));
                if usable_stem(rel) {
                    if !groups.contains_key(rel) {
                        groups.insert(rel.to_owned(), EntryGroup::with_label(rel));
                    }
                    groups.get_mut(rel).expect("rel group").push(
                        entry,
                        options.chunk_rows,
                        spill,
                        checkpoint,
                    )?;
                }
            }
            Ok(())
        },
    )?;
    Ok(groups)
}

#[allow(clippy::too_many_arguments)]
fn finish_groups(
    root: &Path,
    groups: &mut std::collections::BTreeMap<String, EntryGroup>,
    generation: u64,
    built_at_micros: i64,
    options: &AdjacencyBuildOptions,
    spill: &mut SpillSession,
    metrics: &mut AdjacencyBuildMetrics,
    admission: Option<
        &std::sync::Arc<crate::graph_construction::cpu_admission::ConstructionCpuAdmission>,
    >,
    checkpoint: &mut impl FnMut() -> Result<(), GfError>,
) -> Result<Vec<AdjacencyManifestRow>, GfError> {
    let union = groups.remove(ALL_RELATIONS_STEM).unwrap_or_default();
    let mut ordered = std::mem::take(groups).into_iter().collect::<Vec<_>>();
    ordered.push((ALL_RELATIONS_STEM.to_owned(), union));
    let want =
        std::num::NonZeroUsize::new((ordered.len() * 2).min(8)).expect("union has two directions");
    let lease = admission.and_then(|admission| admission.try_acquire(want));
    let lanes = lease.as_ref().map_or(1, |lease| lease.lanes().get());
    let mut outcomes = Vec::new();
    if lanes == 1 {
        for (stem, group) in &mut ordered {
            for direction in [Direction::Out, Direction::In] {
                checkpoint()?;
                outcomes.push(group.finish_sharded_csr(
                    direction,
                    &csr_path(root, stem, direction),
                    options,
                    spill,
                    checkpoint,
                )?);
            }
        }
    } else {
        // Spill names, compaction and accounting follow the original order.
        // Workers only read prepared runs and write disjoint CSR paths.
        for (_, group) in &mut ordered {
            for direction in [Direction::Out, Direction::In] {
                group.prepare_direction(direction, options, spill, checkpoint)?;
            }
        }
        outcomes = finish_groups_on_lanes(
            root,
            &ordered,
            options,
            lanes,
            spill.allocation.as_ref(),
            checkpoint,
        )?;
    }
    let mut manifest = Vec::with_capacity(outcomes.len());
    for (index, outcome) in outcomes.into_iter().enumerate() {
        metrics
            .captured_artifacts
            .extend(outcome.captured_artifacts);
        metrics.csr_shards = metrics.csr_shards.saturating_add(outcome.shards);
        metrics.peak_shard_edges = metrics.peak_shard_edges.max(outcome.peak_shard_edges);
        metrics.peak_shard_nodes = metrics.peak_shard_nodes.max(outcome.peak_shard_nodes);
        manifest.push(AdjacencyManifestRow {
            relation_type: ordered[index / 2].0.clone(),
            direction: if index.is_multiple_of(2) {
                Direction::Out
            } else {
                Direction::In
            },
            topology_generation: generation,
            built_at_micros,
            node_count: outcome.node_count,
            edge_count: outcome.edge_count,
        });
    }
    Ok(manifest)
}

fn finish_groups_on_lanes(
    root: &Path,
    ordered: &[(String, EntryGroup)],
    options: &AdjacencyBuildOptions,
    lanes: usize,
    allocation: Option<&crate::StorageAllocationOperation>,
    checkpoint: &mut impl FnMut() -> Result<(), GfError>,
) -> Result<Vec<ShardedWriteOutcome>, GfError> {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Mutex, mpsc};
    let next = AtomicUsize::new(0);
    let stop = AtomicBool::new(false);
    let count = ordered.len() * 2;
    let results = Mutex::new((0..count).map(|_| None).collect::<Vec<_>>());
    let reversed = crate::graph_construction::lane_jobs_reversed();
    let phase = crate::lifecycle_io::effective_phase(crate::StorageIoPhase::ReadPathScan);
    let mut cancellation = None;
    std::thread::scope(|scope| {
        let (sender, receiver) = mpsc::channel();
        for _ in 0..lanes {
            let sender = sender.clone();
            let ordered = &ordered;
            let next = &next;
            let stop = &stop;
            let results = &results;
            #[cfg(any(test, feature = "test-support"))]
            let digest_context = graphforge_core::hash_observation::operation::Context::capture();
            let lifecycle_context = crate::lifecycle_io::CaptureContext::current();
            scope.spawn(move || {
                #[cfg(any(test, feature = "test-support"))]
                let _digest_guard = digest_context.attach();
                let _lifecycle_capture = lifecycle_context.attach();
                let _phase = crate::lifecycle_io::PhaseScope::enter(phase);
                loop {
                    let index = crate::graph_construction::lane_job(
                        next.fetch_add(1, Ordering::Relaxed),
                        count,
                        reversed,
                    );
                    if index >= count {
                        break;
                    }
                    let result =
                        write_group_job(root, ordered, options, index, allocation, &mut || {
                            if stop.load(Ordering::Acquire) {
                                Err(storage_err("adjacency construction cancelled"))
                            } else {
                                Ok(())
                            }
                        });
                    results
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)[index] = Some(result);
                }
                let _ = sender.send(());
            });
        }
        drop(sender);
        // The coordinator is additional to the admitted background lanes.
        // Its checkpoint forwards cancellation to all workers.
        let index = crate::graph_construction::lane_job(
            next.fetch_add(1, Ordering::Relaxed),
            count,
            reversed,
        );
        if index < count {
            let result = write_group_job(root, ordered, options, index, allocation, &mut || {
                let result = checkpoint();
                if result.is_err() {
                    stop.store(true, Ordering::Release);
                }
                result
            });
            results
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)[index] = Some(result);
        }
        let mut completed = 0;
        while completed < lanes {
            if cancellation.is_none()
                && let Err(error) = checkpoint()
            {
                cancellation = Some(error);
                stop.store(true, Ordering::Release);
            }
            match receiver.recv_timeout(std::time::Duration::from_millis(5)) {
                Ok(()) => completed += 1,
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    cancellation.get_or_insert_with(|| storage_err("adjacency lane disconnected"));
                    break;
                }
            }
        }
    });
    if let Some(error) = cancellation {
        return Err(error);
    }
    checkpoint()?;
    let mut outcomes = Vec::with_capacity(count);
    for result in results
        .into_inner()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
    {
        outcomes.push(result.ok_or_else(|| storage_err("adjacency lane result missing"))??);
    }
    Ok(outcomes)
}

#[allow(clippy::type_complexity)]
fn sorted_directions(
    buffer: &[BuildEntry],
    admission: Option<
        &std::sync::Arc<crate::graph_construction::cpu_admission::ConstructionCpuAdmission>,
    >,
    checkpoint: &mut dyn FnMut() -> Result<(), GfError>,
) -> Result<(Vec<BuildEntry>, Vec<BuildEntry>), GfError> {
    let lease = admission
        .and_then(|admission| admission.try_acquire(std::num::NonZeroUsize::new(1).unwrap()));
    let mut out = buffer.to_vec();
    let incoming = || {
        let mut entries = buffer
            .iter()
            .map(|&(src, edge, dst)| (dst, edge, src))
            .collect::<Vec<_>>();
        entries.sort_unstable_by_key(|&(key, edge, _)| (key, edge));
        entries
    };
    let incoming = if lease.is_some() {
        std::thread::scope(|scope| {
            #[cfg(any(test, feature = "test-support"))]
            let digest_context = graphforge_core::hash_observation::operation::Context::capture();
            let lifecycle_context = crate::lifecycle_io::CaptureContext::current();
            let worker = scope.spawn(move || {
                #[cfg(any(test, feature = "test-support"))]
                let _digest_guard = digest_context.attach();
                let _lifecycle_capture = lifecycle_context.attach();
                incoming()
            });
            out.sort_unstable_by_key(|&(key, edge, _)| (key, edge));
            worker
                .join()
                .map_err(|_| storage_err("adjacency sorting lane panicked"))
        })?
    } else {
        out.sort_unstable_by_key(|&(key, edge, _)| (key, edge));
        incoming()
    };
    checkpoint()?;
    Ok((out, incoming))
}

fn write_group_job(
    root: &Path,
    ordered: &[(String, EntryGroup)],
    options: &AdjacencyBuildOptions,
    index: usize,
    allocation: Option<&crate::StorageAllocationOperation>,
    checkpoint: &mut dyn FnMut() -> Result<(), GfError>,
) -> Result<ShardedWriteOutcome, GfError> {
    let (stem, group) = &ordered[index / 2];
    let direction = if index.is_multiple_of(2) {
        Direction::Out
    } else {
        Direction::In
    };
    group.write_sharded_csr(
        direction,
        &csr_path(root, stem, direction),
        options,
        allocation,
        checkpoint,
    )
}

/// Decode on one admitted lane while the caller groups the preceding batch.
/// The bounded channel preserves file and row order and limits read-ahead.
fn for_each_admitted_edge_batch(
    edge_files: &[(String, PathBuf)],
    batch_size: usize,
    admission: Option<
        &std::sync::Arc<crate::graph_construction::cpu_admission::ConstructionCpuAdmission>,
    >,
    consume: &mut impl FnMut(&str, bool, &arrow::record_batch::RecordBatch) -> Result<(), GfError>,
) -> Result<(), GfError> {
    let lease = admission
        .and_then(|admission| admission.try_acquire(std::num::NonZeroUsize::new(1).unwrap()));
    let Some(_lease) = lease else {
        return for_each_adjacency_edge_path(edge_files, batch_size, consume);
    };
    let phase = crate::lifecycle_io::effective_phase(crate::StorageIoPhase::ReadPathScan);
    std::thread::scope(|scope| {
        let (sender, receiver) = std::sync::mpsc::sync_channel(2);
        #[cfg(any(test, feature = "test-support"))]
        let digest_context = graphforge_core::hash_observation::operation::Context::capture();
        let lifecycle_context = crate::lifecycle_io::CaptureContext::current();
        let worker = scope.spawn(move || {
            #[cfg(any(test, feature = "test-support"))]
            let _digest_guard = digest_context.attach();
            let _lifecycle_capture = lifecycle_context.attach();
            let _phase = crate::lifecycle_io::PhaseScope::enter(phase);
            for_each_adjacency_edge_path(edge_files, batch_size, &mut |stem, exploratory, batch| {
                sender
                    .send((stem.to_owned(), exploratory, batch.clone()))
                    .map_err(|_| storage_err("adjacency decode receiver closed"))
            })
        });
        let consumed = (|| {
            for (stem, exploratory, batch) in &receiver {
                consume(&stem, exploratory, &batch)?;
            }
            Ok(())
        })();
        // Dropping the receiver releases a producer blocked by backpressure,
        // including when the consumer failed or cancelled.
        drop(receiver);
        let decoded = worker
            .join()
            .map_err(|_| storage_err("adjacency decoding lane panicked"))?;
        consumed.and(decoded)
    })
}
