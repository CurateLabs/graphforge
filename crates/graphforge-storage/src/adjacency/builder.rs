//! Bounded adjacency construction, spill scheduling, and manifest-last publication.

use super::{
    ALL_RELATIONS_STEM, AdjacencyManifestRow, BuildEntry, DEFAULT_ADJACENCY_BATCH_SIZE,
    DEFAULT_CSR_SHARD_EDGES, DEFAULT_CSR_SHARD_NODES, Direction, ShardedCsrWriter, adjacency_dir,
    csr_path, for_each_adjacency_edge_file, named_column, storage_err, string_column,
    uint64_column, usable_stem, write_manifest,
};
use graphforge_core::GfError;
use std::path::{Path, PathBuf};

#[cfg(test)]
mod tests;

/// Aggregate bounded-resource evidence for one adjacency build.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AdjacencyBuildMetrics {
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
    checkpoint()?;
    // Generation BEFORE the scan — see the race note in the doc comment.
    let generation = crate::generation::read_topology_generation(source_project_dir)?;
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
    let mut metrics = AdjacencyBuildMetrics::default();

    let build_result = (|| {
        let mut groups = stream_build_groups(
            source_project_dir,
            &options,
            &mut spill,
            &mut metrics,
            &mut checkpoint,
        )?;
        checkpoint()?;

        let mut manifest = Vec::new();
        let mut write_pair = |stem: &str, group: &mut EntryGroup| -> Result<(), GfError> {
            for direction in [Direction::Out, Direction::In] {
                checkpoint()?;
                let outcome = group.finish_sharded_csr(
                    direction,
                    &csr_path(artifact_project_dir, stem, direction),
                    &options,
                    &mut spill,
                    &mut checkpoint,
                )?;
                metrics.csr_shards = metrics.csr_shards.saturating_add(outcome.shards);
                metrics.peak_shard_edges = metrics.peak_shard_edges.max(outcome.peak_shard_edges);
                metrics.peak_shard_nodes = metrics.peak_shard_nodes.max(outcome.peak_shard_nodes);
                manifest.push(AdjacencyManifestRow {
                    relation_type: stem.to_owned(),
                    direction,
                    topology_generation: generation,
                    built_at_micros,
                    node_count: outcome.node_count,
                    edge_count: outcome.edge_count,
                });
            }
            Ok(())
        };

        let mut union = groups
            .remove(ALL_RELATIONS_STEM)
            .unwrap_or_else(EntryGroup::default);
        // Stable stem order for deterministic manifest row ordering.
        let stems: Vec<String> = groups.keys().cloned().collect();
        for stem in stems {
            let mut group = groups.remove(&stem).expect("stem present");
            write_pair(&stem, &mut group)?;
        }
        write_pair(ALL_RELATIONS_STEM, &mut union)?;
        checkpoint()?;

        // Manifest LAST: a crash before this point leaves the manifest absent or
        // old, so a torn build always reads as stale.
        write_manifest(artifact_project_dir, &manifest)?;
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
            spill.cleanup();
            Ok(result)
        }
        Err(error) => {
            spill.cleanup();
            Err(error)
        }
    }
}

/// RAII spill directory: always removed on drop / explicit cleanup so cancel
/// and failure cannot leave temporary runs behind as a published artifact.
struct SpillSession {
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
        self.root
            .join(format!("{label}.{}.{id}.run", direction.as_str()))
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
        self.bytes_current = self.bytes_current.saturating_sub(bytes);
        Ok(())
    }

    fn cleanup(&mut self) {
        if self.cleaned {
            return;
        }
        self.cleaned = true;
        let _ = std::fs::remove_dir_all(&self.root);
        // Best-effort: remove an empty project-local `.spill` parent we created.
        // Never delete a shared policy spill root that may hold other files.
        if let Some(parent) = self.root.parent()
            && parent
                .file_name()
                .is_some_and(|name| name == ADJACENCY_SPILL_DIR_NAME)
        {
            let _ = std::fs::remove_dir(parent);
        }
    }
}

impl Drop for SpillSession {
    fn drop(&mut self) {
        self.cleanup();
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
        let mut out_keyed: Vec<(u64, u64, u64)> = self
            .buffer
            .iter()
            .map(|&(src, edge, dst)| (src, edge, dst))
            .collect();
        out_keyed.sort_unstable_by_key(|&(key, edge, _)| (key, edge));
        let out_path = spill.next_run_path(label, Direction::Out);
        write_keyed_run(&out_path, &out_keyed, spill)?;
        self.out_runs.push(out_path);

        let mut in_keyed: Vec<(u64, u64, u64)> = self
            .buffer
            .iter()
            .map(|&(src, edge, dst)| (dst, edge, src))
            .collect();
        in_keyed.sort_unstable_by_key(|&(key, edge, _)| (key, edge));
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
        let mut writer =
            ShardedCsrWriter::create(path, options.shard_max_edges, options.shard_max_nodes)?;
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
            for entry in keyed {
                emit(entry)?;
            }
        }
        let node_count = max_key.map_or(0, |key| key.saturating_add(1));
        let edge_count = writer.edge_count;
        let (shards, peak_shard_edges, peak_shard_nodes) = writer.finish(node_count)?;
        Ok(ShardedWriteOutcome {
            node_count,
            edge_count,
            shards,
            peak_shard_edges,
            peak_shard_nodes,
        })
    }
}

struct ShardedWriteOutcome {
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
    Ok(())
}

struct RunCursor {
    file: std::io::BufReader<std::fs::File>,
    remaining: u64,
    current: Option<(u64, u64, u64)>,
}

impl RunCursor {
    fn open(path: &Path) -> Result<Self, GfError> {
        use std::io::Read;
        let file = std::fs::File::open(path).map_err(storage_err)?;
        let mut file = std::io::BufReader::with_capacity(1 << 20, file);
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
    let mut file = std::io::BufReader::new(std::fs::File::open(path).map_err(storage_err)?);
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
    writer.flush().map_err(storage_err)
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

    let mut cursors: Vec<RunCursor> = runs
        .iter()
        .map(|p| RunCursor::open(p))
        .collect::<Result<_, _>>()?;
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
    project_dir: &Path,
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

    for_each_adjacency_edge_file(
        project_dir,
        options.batch_size,
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
