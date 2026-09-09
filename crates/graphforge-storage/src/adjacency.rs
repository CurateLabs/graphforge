//! On-disk format for the derived adjacency index (`indexes/adjacency/`, ADR 0005).
//!
//! The adjacency index is a derived, rebuildable CSR (compressed sparse row)
//! representation of the topology. Canonical Parquet under `topology/` remains
//! the sole source of truth: every file in `indexes/adjacency/` can be
//! reconstructed from the typed edge tables alone, and an absent index means
//! "build in memory on demand", never an error.
//!
//! ```text
//! indexes/adjacency/
//! ├── index_manifest.parquet    ADJACENCY_MANIFEST_SCHEMA (Parquet)
//! ├── WORKS_AT.out.csr.json     versioned/checksummed shard manifest
//! ├── WORKS_AT.out.csr.shards-<digest>.d/
//! ├── WORKS_AT.in.csr.json
//! └── _all.out.csr.json         union across relation types
//! ```
//!
//! # Build ordering convention
//!
//! Builders MUST write immutable shard files, publish each shard manifest
//! atomically, and write `index_manifest.parquet` **last**. A crash mid-build
//! then leaves the manifest absent or carrying the
//! old `topology_generation`, so the index reads as stale and the provider
//! falls back to scan-and-build — a torn build can cost a rebuild, never
//! correctness.
//!
//! # CSR encoding
//!
//! Each bounded shard `.csr` file is Arrow IPC with one column,
//! `adjacency: LargeList<Struct{edge_id, neighbor_id}>` and one row per
//! local surrogate range. A high-degree logical row may continue in the next
//! shard. The list offsets buffer is the CSR
//! offsets array; the struct child is the targets array. See
//! [`ADJACENCY_CSR_SCHEMA`] and `docs/book/architecture/storage.md` §Derived
//! Indexes. [`ShardedCsrIndex`] resolves only the shard(s) containing a requested
//! row. Legacy single-batch [`CsrIndex`] files remain readable until rebuild.

mod builder;
pub use builder::{
    ADJACENCY_SPILL_DIR_NAME, AdjacencyBuildMetrics, AdjacencyBuildOptions,
    DEFAULT_ADJACENCY_CHUNK_ROWS, DEFAULT_ADJACENCY_MERGE_FAN_IN, build_adjacency_index,
    build_adjacency_index_into, build_adjacency_index_into_with_metrics,
    build_adjacency_index_into_with_options, build_adjacency_index_with_checkpoint,
};

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    Array, LargeListArray, RecordBatch, StringArray, StructArray, TimestampMicrosecondArray,
    UInt64Array,
};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field};
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;

use graphforge_core::GfError;

use crate::schemas::{ADJACENCY_CSR_SCHEMA, ADJACENCY_MANIFEST_SCHEMA, adjacency_entry_fields};
use crate::staging::RewriteBatch;

/// Reserved relation-type stem for the union-across-relation-types index
/// (`_all.out.csr`). Underscore-prefixed names cannot collide with declared
/// relation types (matching the `_exploratory.parquet` convention).
pub const ALL_RELATIONS_STEM: &str = "_all";

/// File name of the adjacency index manifest within `indexes/adjacency/`.
pub const MANIFEST_FILE: &str = "index_manifest.parquet";

const SHARDED_CSR_VERSION: u32 = 1;
/// Default maximum adjacency entries materialized in one persisted CSR shard.
pub const DEFAULT_CSR_SHARD_EDGES: usize = 1_048_576;
/// Default maximum local CSR rows (offset entries minus one) per shard.
pub const DEFAULT_CSR_SHARD_NODES: usize = 1_048_576;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CsrShardRecord {
    first_node: u64,
    node_count: u64,
    edge_count: u64,
    file: String,
    sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CsrShardManifest {
    format: String,
    version: u32,
    node_count: u64,
    edge_count: u64,
    shard_dir: String,
    shards: Vec<CsrShardRecord>,
}

/// Bounded reader for a versioned sharded CSR. Opening validates the small
/// manifest; row access reads and authenticates only the containing shard.
#[derive(Clone, Debug)]
pub struct ShardedCsrIndex {
    root: PathBuf,
    manifest: CsrShardManifest,
    // One decoded shard is enough to make sequential row traversal O(shards)
    // while keeping reader memory bounded by the configured shard limit.
    cache: std::sync::Arc<std::sync::Mutex<Option<(String, CsrIndex)>>>,
}

impl ShardedCsrIndex {
    /// Open the shard manifest beside the legacy logical `.csr` path.
    pub fn open(path: &Path) -> Result<Self, GfError> {
        let manifest_path = path.with_extension("csr.json");
        let bytes = std::fs::read(&manifest_path).map_err(storage_err)?;
        let manifest: CsrShardManifest = serde_json::from_slice(&bytes).map_err(storage_err)?;
        if manifest.format != "graphforge.csr-shards" || manifest.version != SHARDED_CSR_VERSION {
            return Err(GfError::Storage(format!(
                "unsupported sharded CSR manifest {}",
                manifest_path.display()
            )));
        }
        let mut prior_first = None;
        let mut edges = 0_u64;
        if !is_normal_path_component(&manifest.shard_dir) {
            return Err(GfError::Storage("invalid CSR shard directory name".into()));
        }
        let root = path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(&manifest.shard_dir);
        for shard in &manifest.shards {
            if shard.node_count == 0
                || shard.first_node.saturating_add(shard.node_count) > manifest.node_count
                || prior_first.is_some_and(|prior| shard.first_node < prior)
            {
                return Err(GfError::Storage(
                    "invalid CSR shard boundary ordering".into(),
                ));
            }
            prior_first = Some(shard.first_node);
            edges = edges.saturating_add(shard.edge_count);
            if !is_normal_path_component(&shard.file) {
                return Err(GfError::Storage("invalid CSR shard file name".into()));
            }
            // Presence only. Checksum and structural authentication stay on the
            // first row touch so opening a multi-shard index cannot charge O(E)
            // process RSS (#1094).
            let shard_path = root.join(&shard.file);
            let metadata = std::fs::metadata(&shard_path).map_err(|error| {
                GfError::Storage(format!("missing CSR shard {}: {error}", shard.file))
            })?;
            if !metadata.is_file() {
                return Err(GfError::Storage(format!(
                    "missing CSR shard {}",
                    shard.file
                )));
            }
        }
        if edges != manifest.edge_count {
            return Err(GfError::Storage(
                "CSR shard manifest counts disagree".into(),
            ));
        }
        Ok(Self {
            root,
            manifest,
            cache: std::sync::Arc::new(std::sync::Mutex::new(None)),
        })
    }

    /// Total logical source rows across all shards.
    #[must_use]
    pub const fn node_count(&self) -> u64 {
        self.manifest.node_count
    }

    /// Total adjacency entries across all shards.
    #[must_use]
    pub const fn edge_count(&self) -> u64 {
        self.manifest.edge_count
    }

    /// Read one logical row without loading unrelated shards.
    pub fn row(&self, node_id: u64) -> Result<Vec<(u64, u64)>, GfError> {
        if node_id >= self.manifest.node_count {
            return Ok(Vec::new());
        }
        // A single high-degree row may span adjacent hard-capped shards. The
        // manifest is ordered by `first_node`, so stop once starts pass the key.
        let end = self
            .manifest
            .shards
            .partition_point(|record| record.first_node <= node_id);
        let mut start = end;
        while start > 0 {
            let prior = &self.manifest.shards[start - 1];
            if node_id >= prior.first_node.saturating_add(prior.node_count) {
                break;
            }
            start -= 1;
        }
        let mut output = Vec::new();
        for record in &self.manifest.shards[start..end] {
            output.extend(
                self.map_record_row(record, node_id, |row| row.iter().collect::<Vec<_>>())?,
            );
        }
        Ok(output)
    }

    /// Count a logical row without concatenating its neighbor payload. A hub can
    /// span many shards; only one authenticated bounded shard is retained.
    pub fn row_len(&self, node_id: u64) -> Result<u64, GfError> {
        if node_id >= self.manifest.node_count {
            return Ok(0);
        }
        let end = self
            .manifest
            .shards
            .partition_point(|record| record.first_node <= node_id);
        let mut total = 0_u64;
        for record in self.manifest.shards[..end].iter().rev() {
            if node_id >= record.first_node.saturating_add(record.node_count) {
                break;
            }
            let count = self.map_record_row(record, node_id, |row| row.len() as u64)?;
            total = total
                .checked_add(count)
                .ok_or_else(|| GfError::Storage("CSR degree exceeds u64".into()))?;
        }
        Ok(total)
    }

    /// Copy at most `limit` entries after a logical row offset. This preserves
    /// shard order and never concatenates a whole high-degree row.
    pub fn row_chunk(
        &self,
        node_id: u64,
        mut skip: usize,
        limit: usize,
    ) -> Result<Vec<(u64, u64)>, GfError> {
        if node_id >= self.manifest.node_count || limit == 0 {
            return Ok(Vec::new());
        }
        let end = self
            .manifest
            .shards
            .partition_point(|record| record.first_node <= node_id);
        let mut start = end;
        while start > 0
            && node_id
                < self.manifest.shards[start - 1]
                    .first_node
                    .saturating_add(self.manifest.shards[start - 1].node_count)
        {
            start -= 1;
        }
        let mut output = Vec::new();
        for record in &self.manifest.shards[start..end] {
            self.map_record_row(record, node_id, |row| {
                if skip >= row.len() {
                    skip -= row.len();
                    return;
                }
                output.extend(row.iter().skip(skip).take(limit - output.len()));
                skip = 0;
            })?;
            if output.len() == limit {
                break;
            }
        }
        Ok(output)
    }

    fn map_record_row<T>(
        &self,
        record: &CsrShardRecord,
        node_id: u64,
        map: impl FnOnce(CsrRow<'_>) -> T,
    ) -> Result<T, GfError> {
        let mut cache = self
            .cache
            .lock()
            .map_err(|_| GfError::Storage("CSR shard cache lock poisoned".into()))?;
        if let Some((file, csr)) = cache.as_ref()
            && file == &record.file
        {
            return Ok(map(csr.row(node_id - record.first_node)));
        }
        let path = self.root.join(&record.file);
        let bytes = std::fs::read(&path).map_err(|error| {
            GfError::Storage(format!("missing CSR shard {}: {error}", path.display()))
        })?;
        if sha256_hex(&bytes) != record.sha256 {
            return Err(GfError::Storage(format!(
                "CSR shard checksum mismatch: {}",
                record.file
            )));
        }
        let csr = read_csr_bytes(&bytes, &path)?;
        if csr.node_count() != record.node_count || csr.edge_count() != record.edge_count {
            return Err(GfError::Storage(format!(
                "CSR shard count mismatch: {}",
                record.file
            )));
        }
        let output = map(csr.row(node_id - record.first_node));
        *cache = Some((record.file.clone(), csr));
        Ok(output)
    }
}

/// Whether the versioned sharded representation is published for `path`.
#[must_use]
pub fn sharded_csr_exists(path: &Path) -> bool {
    path.with_extension("csr.json").is_file()
}

fn csr_artifact_exists(path: &Path) -> bool {
    path.is_file() || sharded_csr_exists(path)
}

fn is_normal_path_component(value: &str) -> bool {
    let mut components = Path::new(value).components();
    matches!(components.next(), Some(std::path::Component::Normal(_)))
        && components.next().is_none()
}

/// Write a versioned checksummed shard set and publish its manifest last.
/// This compatibility helper accepts an in-memory CSR; streaming builders use
/// the same shard writer while producing one bounded shard at a time.
pub fn write_sharded_csr(path: &Path, csr: &CsrIndex, max_edges: usize) -> Result<(), GfError> {
    csr.validate()?;
    let mut writer = ShardedCsrWriter::create(path, max_edges, DEFAULT_CSR_SHARD_NODES)?;
    for node in 0..csr.node_count() {
        for (edge, neighbor) in csr.row(node).iter() {
            writer.emit((node, edge, neighbor))?;
        }
    }
    writer.finish(csr.node_count()).map(|_| ())
}

/// Row-emitting bounded sink used directly by the external-run merge.
struct ShardedCsrWriter {
    path: PathBuf,
    root: PathBuf,
    shard_dir: String,
    max_edges: usize,
    max_nodes: usize,
    records: Vec<CsrShardRecord>,
    shard: CsrIndex,
    first_node: Option<u64>,
    last_key: Option<u64>,
    edge_count: u64,
    peak_shard_edges: u64,
    peak_shard_nodes: u64,
    owned_root: Option<PathBuf>,
    finished: bool,
}

impl ShardedCsrWriter {
    fn create(path: &Path, max_edges: usize, max_nodes: usize) -> Result<Self, GfError> {
        let parent = path
            .parent()
            .ok_or_else(|| GfError::Storage("CSR path has no parent".into()))?;
        std::fs::create_dir_all(parent).map_err(storage_err)?;
        let stem = path.file_name().and_then(|n| n.to_str()).unwrap_or("csr");
        let shard_dir = format!("{stem}.{}.d", uuid::Uuid::new_v4().as_simple());
        let root = parent.join(&shard_dir);
        std::fs::create_dir(&root).map_err(storage_err)?;
        Ok(Self {
            path: path.to_path_buf(),
            owned_root: Some(root.clone()),
            root,
            shard_dir,
            max_edges: max_edges.max(1),
            max_nodes: max_nodes.max(1),
            records: Vec::new(),
            shard: CsrIndex {
                offsets: vec![0],
                ..CsrIndex::default()
            },
            first_node: None,
            last_key: None,
            edge_count: 0,
            peak_shard_edges: 0,
            peak_shard_nodes: 0,
            finished: false,
        })
    }

    fn emit(&mut self, (key, edge, neighbor): (u64, u64, u64)) -> Result<(), GfError> {
        if self.shard.edge_ids.len() >= self.max_edges
            || self.first_node.is_some_and(|first| {
                key.saturating_sub(first) >= u64::try_from(self.max_nodes).unwrap_or(u64::MAX)
            })
        {
            self.flush()?;
        }
        let first = *self.first_node.get_or_insert(key);
        if key < self.last_key.unwrap_or(key) {
            return Err(GfError::Storage(
                "CSR shard sink received unsorted row keys".into(),
            ));
        }
        let local = key - first;
        while self.shard.node_count() <= local {
            self.shard.offsets.push(self.shard.edge_count());
        }
        self.shard.edge_ids.push(edge);
        self.shard.neighbor_ids.push(neighbor);
        *self.shard.offsets.last_mut().expect("offset exists") = self.shard.edge_count();
        self.last_key = Some(key);
        self.edge_count = self.edge_count.saturating_add(1);
        self.peak_shard_edges = self.peak_shard_edges.max(self.shard.edge_count());
        self.peak_shard_nodes = self.peak_shard_nodes.max(self.shard.node_count());
        Ok(())
    }

    fn flush(&mut self) -> Result<(), GfError> {
        let Some(first) = self.first_node else {
            return Ok(());
        };
        self.records.push(write_csr_shard(
            &self.root,
            first,
            &self.shard,
            self.records.len(),
        )?);
        self.shard = CsrIndex {
            offsets: vec![0],
            ..CsrIndex::default()
        };
        self.first_node = None;
        self.last_key = None;
        Ok(())
    }

    fn finish(mut self, node_count: u64) -> Result<(u64, u64, u64), GfError> {
        use std::io::Write as _;

        self.flush()?;
        let mut identity = Sha256::new();
        identity.update(b"graphforge/csr-shards/v1\0");
        identity.update(node_count.to_le_bytes());
        identity.update(self.edge_count.to_le_bytes());
        for record in &self.records {
            identity.update(record.first_node.to_le_bytes());
            identity.update(record.node_count.to_le_bytes());
            identity.update(record.edge_count.to_le_bytes());
            identity.update(record.sha256.as_bytes());
        }
        let digest = sha256_hex(&identity.finalize());
        let stem = self
            .path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("csr");
        let stable_dir = format!("{stem}.shards-{}.d", &digest[..24]);
        let stable_root = self
            .path
            .parent()
            .expect("validated parent")
            .join(&stable_dir);
        if stable_root.exists() && shard_set_matches(&stable_root, &self.records) {
            std::fs::remove_dir_all(&self.root).map_err(storage_err)?;
            self.owned_root = None;
        } else {
            if stable_root.exists() {
                std::fs::remove_dir_all(&stable_root).map_err(storage_err)?;
            }
            std::fs::rename(&self.root, &stable_root).map_err(storage_err)?;
            self.owned_root = Some(stable_root.clone());
        }
        self.root = stable_root;
        self.shard_dir = stable_dir;
        let manifest = CsrShardManifest {
            format: "graphforge.csr-shards".into(),
            version: SHARDED_CSR_VERSION,
            node_count,
            edge_count: self.edge_count,
            shard_dir: self.shard_dir.clone(),
            shards: std::mem::take(&mut self.records),
        };
        let bytes = serde_json::to_vec_pretty(&manifest).map_err(storage_err)?;
        let parent = self.path.parent().expect("validated parent");
        let mut temp = tempfile::Builder::new()
            .prefix(stem)
            .suffix(".json.tmp")
            .tempfile_in(parent)
            .map_err(storage_err)?;
        temp.write_all(&bytes).map_err(storage_err)?;
        temp.as_file().sync_all().map_err(storage_err)?;
        persist_temp(temp, &self.path.with_extension("csr.json"))?;
        self.finished = true;
        Ok((
            manifest.shards.len() as u64,
            self.peak_shard_edges,
            self.peak_shard_nodes,
        ))
    }
}

impl Drop for ShardedCsrWriter {
    fn drop(&mut self) {
        if !self.finished
            && let Some(root) = self.owned_root.as_ref()
        {
            let _ = std::fs::remove_dir_all(root);
        }
    }
}

fn shard_set_matches(root: &Path, records: &[CsrShardRecord]) -> bool {
    for record in records {
        let path = root.join(&record.file);
        let Ok(bytes) = std::fs::read(&path) else {
            return false;
        };
        if sha256_hex(&bytes) != record.sha256 {
            return false;
        }
        let Ok(csr) = read_csr_bytes(&bytes, &path) else {
            return false;
        };
        if csr.node_count() != record.node_count || csr.edge_count() != record.edge_count {
            return false;
        }
    }
    true
}

fn write_csr_shard(
    root: &Path,
    first_node: u64,
    shard: &CsrIndex,
    ordinal: usize,
) -> Result<CsrShardRecord, GfError> {
    let file = format!("{ordinal:020}.csr");
    let path = root.join(&file);
    write_csr(&path, shard)?;
    let bytes = std::fs::read(&path).map_err(storage_err)?;
    Ok(CsrShardRecord {
        first_node,
        node_count: shard.node_count(),
        edge_count: shard.edge_count(),
        file,
        sha256: sha256_hex(&bytes),
    })
}

fn storage_err(e: impl std::fmt::Display) -> GfError {
    GfError::Storage(e.to_string())
}

/// Edge direction a CSR file is keyed by.
///
/// `Out` means rows are keyed by `src_id` and neighbors are destinations;
/// `In` means rows are keyed by `dst_id` and neighbors are sources.
/// Undirected traversal is served by unioning the two — there is no
/// undirected file on disk.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Direction {
    /// Outgoing edges: keyed by `src_id`, neighbor is `dst_id`.
    Out,
    /// Incoming edges: keyed by `dst_id`, neighbor is `src_id`.
    In,
}

impl Direction {
    /// The on-disk token (`"out"` | `"in"`) used in file names and the
    /// manifest `direction` column.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Out => "out",
            Self::In => "in",
        }
    }

    /// Parse a manifest `direction` token.
    ///
    /// # Errors
    /// Returns [`GfError::Storage`] for anything other than `"out"` / `"in"`.
    pub fn parse(s: &str) -> Result<Self, GfError> {
        match s {
            "out" => Ok(Self::Out),
            "in" => Ok(Self::In),
            other => Err(GfError::Storage(format!(
                "invalid adjacency direction {other:?} (expected \"out\" or \"in\")"
            ))),
        }
    }
}

/// In-memory CSR adjacency structure, surrogate-keyed.
///
/// Neighbors of `node_id = i` are
/// `(edge_ids[j], neighbor_ids[j]) for j in offsets[i]..offsets[i + 1]`.
///
/// # Invariants (enforced on read and write)
///
/// - `offsets` is non-empty and `offsets[0] == 0` — the empty graph is
///   `offsets == [0]` with empty targets, never an empty `offsets`.
/// - `offsets` is monotonically non-decreasing; a node with no neighbors is
///   an empty range.
/// - `*offsets.last() == edge_ids.len() == neighbor_ids.len()`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CsrIndex {
    /// CSR offsets, length `node_count + 1`.
    pub offsets: Vec<u64>,
    /// Edge surrogate per adjacency entry, in CSR order.
    pub edge_ids: Vec<u64>,
    /// Neighbor node surrogate per adjacency entry, in CSR order.
    pub neighbor_ids: Vec<u64>,
}

impl CsrIndex {
    /// Number of source nodes covered (CSR row count).
    #[must_use]
    pub fn node_count(&self) -> u64 {
        (self.offsets.len().max(1) - 1) as u64
    }

    /// Number of `(edge, neighbor)` adjacency entries.
    #[must_use]
    pub fn edge_count(&self) -> u64 {
        self.edge_ids.len() as u64
    }

    /// Checked O(1) row lookup: parallel `(edge_id, neighbor_id)` slices for
    /// `node_id`, or empty slices when the id is out of range / isolated.
    ///
    /// Callers that loaded this CSR via [`read_csr`] already validated offsets,
    /// so the returned ranges are always in bounds.
    #[must_use]
    pub fn row(&self, node_id: u64) -> CsrRow<'_> {
        if node_id >= self.node_count() {
            return CsrRow {
                edge_ids: &[],
                neighbor_ids: &[],
            };
        }
        let i = usize::try_from(node_id).unwrap_or(usize::MAX);
        if i >= self.offsets.len().saturating_sub(1) {
            return CsrRow {
                edge_ids: &[],
                neighbor_ids: &[],
            };
        }
        let start = usize::try_from(self.offsets[i]).unwrap_or(0);
        let end = usize::try_from(self.offsets[i + 1]).unwrap_or(start);
        let end = end.min(self.edge_ids.len()).min(self.neighbor_ids.len());
        let start = start.min(end);
        CsrRow {
            edge_ids: &self.edge_ids[start..end],
            neighbor_ids: &self.neighbor_ids[start..end],
        }
    }

    /// Check the structural invariants listed on the type.
    fn validate(&self) -> Result<(), GfError> {
        if self.offsets.first() != Some(&0) {
            return Err(GfError::Storage(format!(
                "invalid CSR: offsets must start with 0 (got {:?})",
                self.offsets.first()
            )));
        }
        if self.offsets.windows(2).any(|w| w[0] > w[1]) {
            return Err(GfError::Storage(
                "invalid CSR: offsets must be monotonically non-decreasing".to_owned(),
            ));
        }
        let last = *self.offsets.last().unwrap_or(&0);
        if last != self.edge_count() || self.edge_ids.len() != self.neighbor_ids.len() {
            return Err(GfError::Storage(format!(
                "invalid CSR: final offset {last} must equal target lengths \
                 (edge_ids: {}, neighbor_ids: {})",
                self.edge_ids.len(),
                self.neighbor_ids.len()
            )));
        }
        Ok(())
    }
}

/// Borrowed CSR row: parallel edge-id and neighbor-id slices (same length).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CsrRow<'a> {
    /// Edge surrogates for this row, in CSR order.
    pub edge_ids: &'a [u64],
    /// Neighbor node surrogates aligned with [`Self::edge_ids`].
    pub neighbor_ids: &'a [u64],
}

impl<'a> CsrRow<'a> {
    /// Number of `(edge, neighbor)` entries in this row.
    #[must_use]
    pub fn len(&self) -> usize {
        self.edge_ids.len().min(self.neighbor_ids.len())
    }

    /// Whether the row has no adjacency entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Entry at `index`, if in range.
    #[must_use]
    pub fn get(&self, index: usize) -> Option<(u64, u64)> {
        if index < self.len() {
            Some((self.edge_ids[index], self.neighbor_ids[index]))
        } else {
            None
        }
    }

    /// Iterate `(edge_id, neighbor_id)` pairs without allocating.
    pub fn iter(self) -> impl Iterator<Item = (u64, u64)> + 'a {
        self.edge_ids
            .iter()
            .copied()
            .zip(self.neighbor_ids.iter().copied())
    }
}

/// One row of `index_manifest.parquet` — the build record for a single CSR
/// file. See [`ADJACENCY_MANIFEST_SCHEMA`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdjacencyManifestRow {
    /// Relation type name, or [`ALL_RELATIONS_STEM`] for the union index.
    pub relation_type: String,
    /// Direction the CSR file is keyed by.
    pub direction: Direction,
    /// Project topology generation the CSR was built from.
    pub topology_generation: u64,
    /// Build wall-clock time, microseconds since the Unix epoch (UTC).
    /// Caller-supplied; excluded from the determinism guarantee.
    pub built_at_micros: i64,
    /// Number of source nodes covered (CSR row count).
    pub node_count: u64,
    /// Number of `(edge, neighbor)` adjacency entries.
    pub edge_count: u64,
}

/// Bounded freshness state for the derived adjacency artifact.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdjacencyFreshnessState {
    /// The effective base plus delta chain exactly represents current topology.
    Current,
    /// No published adjacency manifest exists.
    Missing,
    /// A readable artifact exists but cannot be advanced to current topology.
    Stale,
    /// The artifact is torn, unreadable, or does not match canonical topology.
    Incompatible,
}

impl AdjacencyFreshnessState {
    /// Stable cross-binding token.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Current => "current",
            Self::Missing => "missing",
            Self::Stale => "stale",
            Self::Incompatible => "incompatible",
        }
    }
}

/// Bounded reason accompanying a non-current adjacency state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdjacencyFreshnessReason {
    /// No manifest has been published.
    NotBuilt,
    /// Manifest rows disagree about their base generation.
    MixedArtifactGeneration,
    /// The required bounded delta chain is absent, gapped, unreadable, or over limit.
    IncompleteDeltaChain,
    /// A manifest-referenced CSR is missing.
    MissingCsr,
    /// A manifest or CSR cannot be decoded.
    UnreadableArtifact,
    /// The effective CSR differs from canonical topology.
    ContentMismatch,
    /// The artifact claims a generation newer than its source topology.
    FutureArtifactGeneration,
}

impl AdjacencyFreshnessReason {
    /// Stable cross-binding token.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotBuilt => "not_built",
            Self::MixedArtifactGeneration => "mixed_artifact_generation",
            Self::IncompleteDeltaChain => "incomplete_delta_chain",
            Self::MissingCsr => "missing_csr",
            Self::UnreadableArtifact => "unreadable_artifact",
            Self::ContentMismatch => "content_mismatch",
            Self::FutureArtifactGeneration => "future_artifact_generation",
        }
    }
}

/// Rust-owned identity and freshness inspection for the adjacency artifact.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdjacencyInspection {
    /// Current canonical topology generation.
    pub source_generation: u64,
    /// SHA-256 of canonically sorted `(src_id, edge_id, dst_id)` tuples.
    pub source_fingerprint: String,
    /// Uniform manifest base generation, when readable.
    pub artifact_generation: Option<u64>,
    /// Effective source generation after applying a complete delta chain.
    pub artifact_effective_generation: Option<u64>,
    /// SHA-256 of the effective union CSR, when readable and delta-complete.
    pub artifact_fingerprint: Option<String>,
    /// Bounded freshness state.
    pub state: AdjacencyFreshnessState,
    /// Bounded reason for a non-current state.
    pub reason: Option<AdjacencyFreshnessReason>,
}

/// `indexes/adjacency/` within `project_dir`.
#[must_use]
pub fn adjacency_dir(project_dir: &Path) -> PathBuf {
    project_dir.join("indexes").join("adjacency")
}

/// Path of the CSR file for (`relation_type`, `direction`):
/// `indexes/adjacency/<REL_TYPE>.<dir>.csr`.
#[must_use]
pub fn csr_path(project_dir: &Path, relation_type: &str, direction: Direction) -> PathBuf {
    adjacency_dir(project_dir).join(format!("{relation_type}.{}.csr", direction.as_str()))
}

/// Path of `index_manifest.parquet` within `project_dir`.
#[must_use]
pub fn manifest_path(project_dir: &Path) -> PathBuf {
    adjacency_dir(project_dir).join(MANIFEST_FILE)
}

/// Write `csr` to `path` as a single-batch Arrow IPC file
/// ([`ADJACENCY_CSR_SCHEMA`]), atomically (sibling temp + rename, the same
/// pattern as [`RewriteBatch`]; IPC files cannot reuse it directly because it
/// encodes Parquet).
///
/// # Errors
/// Returns [`GfError::Storage`] if `csr` violates its invariants or on
/// I/O/encode failure; on failure `path` is untouched.
pub fn write_csr(path: &Path, csr: &CsrIndex) -> Result<(), GfError> {
    csr.validate()?;

    let offsets: Vec<i64> = csr
        .offsets
        .iter()
        .map(|&o| i64::try_from(o).map_err(storage_err))
        .collect::<Result<_, _>>()?;
    let entries = StructArray::new(
        adjacency_entry_fields(),
        vec![
            Arc::new(UInt64Array::from(csr.edge_ids.clone())),
            Arc::new(UInt64Array::from(csr.neighbor_ids.clone())),
        ],
        None,
    );
    let item_field = Arc::new(Field::new(
        "item",
        DataType::Struct(adjacency_entry_fields()),
        false,
    ));
    let adjacency = LargeListArray::new(
        item_field,
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        Arc::new(entries),
        None,
    );
    let batch = RecordBatch::try_new(Arc::clone(&ADJACENCY_CSR_SCHEMA), vec![Arc::new(adjacency)])
        .map_err(storage_err)?;

    let parent = path.parent().ok_or_else(|| {
        GfError::Storage(format!(
            "CSR path {} has no parent directory",
            path.display()
        ))
    })?;
    std::fs::create_dir_all(parent).map_err(storage_err)?;
    let file_name = path
        .file_name()
        .map_or_else(|| "csr".to_owned(), |n| n.to_string_lossy().into_owned());
    let tmp = tempfile::Builder::new()
        .prefix(&format!("{file_name}."))
        .suffix(".tmp")
        .tempfile_in(parent)
        .map_err(storage_err)?;

    let mut writer =
        FileWriter::try_new(tmp.as_file(), &ADJACENCY_CSR_SCHEMA).map_err(storage_err)?;
    writer.write(&batch).map_err(storage_err)?;
    writer.finish().map_err(storage_err)?;
    persist_temp(tmp, path)?;
    // Explicit legacy writes are a supported migration/testing seam. Make the
    // representation choice unambiguous so a stale sharded manifest cannot
    // shadow the newly published single-batch file.
    let sharded_manifest = path.with_extension("csr.json");
    if sharded_manifest.exists() {
        let shard_root = std::fs::read(&sharded_manifest)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<CsrShardManifest>(&bytes).ok())
            .filter(|manifest| is_normal_path_component(&manifest.shard_dir))
            .and_then(|manifest| path.parent().map(|parent| parent.join(manifest.shard_dir)));
        std::fs::remove_file(sharded_manifest).map_err(storage_err)?;
        if let Some(shard_root) = shard_root {
            let _ = std::fs::remove_dir_all(shard_root);
        }
    }
    Ok(())
}

/// Read a CSR file written by [`write_csr`] back into a [`CsrIndex`].
///
/// # Errors
/// Returns [`GfError::Storage`] if the file is missing, is not an Arrow IPC
/// file with [`ADJACENCY_CSR_SCHEMA`], or decodes to an invalid CSR.
pub fn read_csr(path: &Path) -> Result<CsrIndex, GfError> {
    if sharded_csr_exists(path) {
        let sharded = ShardedCsrIndex::open(path)?;
        let mut csr = CsrIndex {
            offsets: vec![0],
            ..CsrIndex::default()
        };
        for node in 0..sharded.node_count() {
            for (edge, neighbor) in sharded.row(node)? {
                csr.edge_ids.push(edge);
                csr.neighbor_ids.push(neighbor);
            }
            csr.offsets.push(csr.edge_count());
        }
        csr.validate()?;
        return Ok(csr);
    }
    let file = File::open(path)
        .map_err(|e| GfError::Storage(format!("cannot open CSR file {}: {e}", path.display())))?;
    let reader = FileReader::try_new(file, None)
        .map_err(|e| GfError::Storage(format!("invalid CSR file {}: {e}", path.display())))?;
    decode_csr(reader, path)
}

fn read_csr_bytes(bytes: &[u8], path: &Path) -> Result<CsrIndex, GfError> {
    let reader = FileReader::try_new(std::io::Cursor::new(bytes), None).map_err(|error| {
        GfError::Storage(format!("invalid CSR shard {}: {error}", path.display()))
    })?;
    decode_csr(reader, path)
}

fn decode_csr<R: std::io::Read + std::io::Seek>(
    reader: FileReader<R>,
    path: &Path,
) -> Result<CsrIndex, GfError> {
    if reader.schema().fields() != ADJACENCY_CSR_SCHEMA.fields() {
        return Err(GfError::Storage(format!(
            "CSR file {} has unexpected schema {:?}",
            path.display(),
            reader.schema()
        )));
    }

    let mut csr = CsrIndex {
        offsets: vec![0],
        ..CsrIndex::default()
    };
    for batch in reader {
        let batch = batch.map_err(storage_err)?;
        let adjacency = batch
            .column(0)
            .as_any()
            .downcast_ref::<LargeListArray>()
            .ok_or_else(|| {
                GfError::Storage(format!(
                    "CSR file {}: adjacency column is not a LargeList",
                    path.display()
                ))
            })?;
        let entries = adjacency
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                GfError::Storage(format!(
                    "CSR file {}: adjacency entries are not a Struct",
                    path.display()
                ))
            })?;
        let (edge_ids, neighbor_ids) = (
            uint64_column(entries.column(0), "edge_id")?,
            uint64_column(entries.column(1), "neighbor_id")?,
        );
        // Walk per-row through the list offsets rather than copying the child
        // arrays wholesale: this stays correct for multi-batch files and for
        // list arrays whose offsets do not start at zero (slices).
        let value_offsets = adjacency.value_offsets();
        for row in 0..adjacency.len() {
            let start = usize::try_from(value_offsets[row]).map_err(storage_err)?;
            let end = usize::try_from(value_offsets[row + 1]).map_err(storage_err)?;
            for entry in start..end {
                csr.edge_ids.push(edge_ids.value(entry));
                csr.neighbor_ids.push(neighbor_ids.value(entry));
            }
            csr.offsets.push(csr.edge_count());
        }
    }
    csr.validate()?;
    Ok(csr)
}

fn sha256_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    Sha256::digest(bytes)
        .iter()
        .fold(String::with_capacity(64), |mut output, byte| {
            let _ = write!(output, "{byte:02x}");
            output
        })
}

/// Replace `index_manifest.parquet` with `rows`, atomically.
///
/// Per the build ordering convention (module docs), call this **after** all
/// CSR files referenced by `rows` have been written.
///
/// # Errors
/// Returns [`GfError::Storage`] on I/O or Parquet-encode failure; on failure
/// any existing manifest is untouched.
pub fn write_manifest(project_dir: &Path, rows: &[AdjacencyManifestRow]) -> Result<(), GfError> {
    let relation_types: StringArray = rows
        .iter()
        .map(|r| Some(r.relation_type.as_str()))
        .collect();
    let directions: StringArray = rows.iter().map(|r| Some(r.direction.as_str())).collect();
    let generations: Vec<u64> = rows.iter().map(|r| r.topology_generation).collect();
    let built_ats: Vec<i64> = rows.iter().map(|r| r.built_at_micros).collect();
    let node_counts: Vec<u64> = rows.iter().map(|r| r.node_count).collect();
    let edge_counts: Vec<u64> = rows.iter().map(|r| r.edge_count).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&ADJACENCY_MANIFEST_SCHEMA),
        vec![
            Arc::new(relation_types),
            Arc::new(directions),
            Arc::new(UInt64Array::from(generations)),
            Arc::new(TimestampMicrosecondArray::from(built_ats).with_timezone("UTC")),
            Arc::new(UInt64Array::from(node_counts)),
            Arc::new(UInt64Array::from(edge_counts)),
        ],
    )
    .map_err(storage_err)?;

    let mut staged = RewriteBatch::new();
    staged.stage(
        &manifest_path(project_dir),
        Arc::clone(&ADJACENCY_MANIFEST_SCHEMA),
        &batch,
    )?;
    staged.commit_at(project_dir)
}

/// Read `index_manifest.parquet`. An absent manifest (or absent
/// `indexes/adjacency/` directory) returns `Ok(vec![])` — the index simply
/// has not been built, mirroring the absent-file semantics of the catalog
/// readers.
///
/// # Errors
/// Returns [`GfError::Storage`] if the file exists but is not a manifest with
/// [`ADJACENCY_MANIFEST_SCHEMA`].
pub fn read_manifest(project_dir: &Path) -> Result<Vec<AdjacencyManifestRow>, GfError> {
    let path = manifest_path(project_dir);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let file = File::open(&path).map_err(storage_err)?;
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(storage_err)?
        .build()
        .map_err(storage_err)?;

    let mut rows = Vec::new();
    for batch in reader {
        let batch = batch.map_err(storage_err)?;
        if batch.schema().fields() != ADJACENCY_MANIFEST_SCHEMA.fields() {
            return Err(GfError::Storage(format!(
                "adjacency manifest {} has unexpected schema {:?}",
                path.display(),
                batch.schema()
            )));
        }
        let relation_types = string_column(batch.column(0), "relation_type")?;
        let directions = string_column(batch.column(1), "direction")?;
        let generations = uint64_column(batch.column(2), "topology_generation")?;
        let built_ats = batch
            .column(3)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or_else(|| {
                GfError::Storage("adjacency manifest: built_at is not a timestamp".to_owned())
            })?;
        let node_counts = uint64_column(batch.column(4), "node_count")?;
        let edge_counts = uint64_column(batch.column(5), "edge_count")?;
        for i in 0..batch.num_rows() {
            rows.push(AdjacencyManifestRow {
                relation_type: relation_types.value(i).to_owned(),
                direction: Direction::parse(directions.value(i))?,
                topology_generation: generations.value(i),
                built_at_micros: built_ats.value(i),
                node_count: node_counts.value(i),
                edge_count: edge_counts.value(i),
            });
        }
    }
    Ok(rows)
}

// ---------------------------------------------------------------------------
// Index builder (#761 / #336)
// ---------------------------------------------------------------------------

/// One edge occurrence during the index build: `(src_id, edge_id, dst_id)`.
/// [`csr_from_entries`] re-keys per direction (`src` for `out`, `dst` for `in`).
pub(crate) type BuildEntry = (u64, u64, u64);

/// Default Parquet batch size for adjacency streaming reads.
pub const DEFAULT_ADJACENCY_BATCH_SIZE: usize = 8_192;

/// Stream projected adjacency edge batches for every file under
/// `topology/edges/`. UUID / FixedSizeBinary columns are never decoded.
///
/// Shared by the builder, validator, and inspector so none of them concatenate
/// a full edge file into one Arrow record batch (#336).
fn for_each_adjacency_edge_file(
    project_dir: &Path,
    batch_size: usize,
    on_batch: &mut dyn FnMut(&str, bool, &RecordBatch) -> Result<(), GfError>,
) -> Result<(), GfError> {
    for (stem, path) in crate::mutator::edge_parquet_files(project_dir, None)? {
        // An unreadable edge file must FAIL the build, not be skipped: a
        // manifest written without it would stamp the current generation and
        // make an index missing a relation's edges look fresh.
        let _schema = match crate::catalog::discover_parquet_schema_detailed(&path) {
            Ok(schema) => schema,
            Err(detail) => {
                return Err(GfError::Storage(format!(
                    "adjacency build: cannot read parquet schema for {}: {detail}",
                    path.display()
                )));
            }
        };
        let exploratory = stem == "_exploratory";
        let columns: &[&str] = if exploratory {
            &["edge_id", "src_id", "dst_id", "rel_type_name"]
        } else {
            &["edge_id", "src_id", "dst_id"]
        };
        stream_projected_parquet_batches(&path, columns, batch_size, &mut |batch| {
            on_batch(&stem, exploratory, &batch)
        })?;
    }
    Ok(())
}

/// Read `path` as projected Parquet batches without concatenating row groups.
///
/// Uses `with_batch_size` and a [`ProjectionMask`] so UUID FixedSizeBinary
/// columns are dropped at the reader when `column_names` names only id fields.
pub(crate) fn stream_projected_parquet_batches(
    path: &Path,
    column_names: &[&str],
    batch_size: usize,
    on_batch: &mut dyn FnMut(RecordBatch) -> Result<(), GfError>,
) -> Result<usize, GfError> {
    use parquet::arrow::ProjectionMask;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    if !path.exists() {
        return Ok(0);
    }
    let file = File::open(path).map_err(storage_err)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(storage_err)?;
    let mask = ProjectionMask::columns(builder.parquet_schema(), column_names.iter().copied());
    let batch_size = batch_size.max(1);
    let reader = builder
        .with_projection(mask)
        .with_batch_size(batch_size)
        .build()
        .map_err(storage_err)?;
    let mut batches = 0usize;
    for batch in reader {
        let batch = batch.map_err(storage_err)?;
        // Defense in depth: projected batches must not carry FixedSizeBinary
        // UUID columns that would recreate the Arrow concat ceiling.
        for field in batch.schema().fields() {
            if matches!(field.data_type(), DataType::FixedSizeBinary(_)) {
                return Err(GfError::Storage(format!(
                    "adjacency stream: projected batch unexpectedly contains FixedSizeBinary column {}",
                    field.name()
                )));
            }
        }
        on_batch(batch)?;
        batches += 1;
    }
    Ok(batches)
}

/// Scan `topology/edges/` and group every edge occurrence by relation type:
/// per-relation entries (stems unusable as file names are skipped, see
/// [`build_adjacency_index`]) plus the full union. Shared by the validator and
/// inspector. Uses the projected streaming reader so validation/inspection
/// cannot hit the full-file UUID concat ceiling (#336).
#[allow(clippy::type_complexity)]
fn collect_adjacency_groups(
    project_dir: &Path,
) -> Result<
    (
        std::collections::BTreeMap<String, Vec<BuildEntry>>,
        Vec<BuildEntry>,
    ),
    GfError,
> {
    collect_adjacency_groups_with_batch_size(project_dir, DEFAULT_ADJACENCY_BATCH_SIZE)
}

#[allow(clippy::type_complexity)]
fn collect_adjacency_groups_with_batch_size(
    project_dir: &Path,
    batch_size: usize,
) -> Result<
    (
        std::collections::BTreeMap<String, Vec<BuildEntry>>,
        Vec<BuildEntry>,
    ),
    GfError,
> {
    use std::collections::BTreeMap;

    let mut groups: BTreeMap<String, Vec<BuildEntry>> = BTreeMap::new();
    let mut union_out: Vec<BuildEntry> = Vec::new();
    for_each_adjacency_edge_file(project_dir, batch_size, &mut |stem, exploratory, batch| {
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
            let entry = (src_ids.value(i), edge_ids.value(i), dst_ids.value(i));
            union_out.push(entry);
            let rel = rel_names.map_or(stem, |names| names.value(i));
            if usable_stem(rel) {
                groups.entry(rel.to_owned()).or_default().push(entry);
            }
        }
        Ok(())
    })?;
    Ok((groups, union_out))
}
// ---------------------------------------------------------------------------
// Index validation (#766)
// ---------------------------------------------------------------------------

/// One problem found by [`validate_adjacency_index`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AdjacencyValidationIssue {
    /// The manifest was built at a different `topology_generation` than the
    /// project's current counter — the index is stale, not corrupt.
    StaleGeneration {
        /// Generation recorded in the manifest row(s).
        manifest: u64,
        /// The project's current counter.
        current: u64,
    },
    /// A manifest row's CSR file is missing on disk.
    MissingCsr {
        /// Relation type stem.
        rel: String,
        /// CSR direction.
        direction: Direction,
    },
    /// A manifest row's CSR file exists but cannot be read or fails its
    /// structural invariants.
    UnreadableCsr {
        /// Relation type stem.
        rel: String,
        /// CSR direction.
        direction: Direction,
        /// The underlying read error.
        error: String,
    },
    /// The CSR file's content differs from a fresh in-memory rebuild of the
    /// same relation/direction — index corruption.
    Mismatch {
        /// Relation type stem.
        rel: String,
        /// CSR direction.
        direction: Direction,
    },
}

/// Verify a persisted adjacency index against a fresh in-memory rebuild from
/// `topology/` (ADR 0005 maintenance op): for every manifest row, the CSR
/// file must exist, parse, and equal the expected CSR byte-for-byte (same
/// deterministic sort the builder uses). An **absent** index (no manifest)
/// is clean — there is nothing to validate, not an error.
///
/// Returns the list of issues found; an empty list means the index is valid.
/// A [`StaleGeneration`](AdjacencyValidationIssue::StaleGeneration) issue is
/// reported once and content checks still run against current topology, so a
/// stale-but-otherwise-intact index reports exactly one issue.
///
/// # Errors
/// Returns [`GfError::Storage`] when the project itself cannot be read (the
/// generation counter, the manifest, or a topology edge file) — problems with
/// the *index* are reported as issues, not errors.
pub fn validate_adjacency_index(
    project_dir: &Path,
) -> Result<Vec<AdjacencyValidationIssue>, GfError> {
    validate_adjacency_index_against(project_dir, project_dir)
}

/// Validate a privately staged artifact against canonical source topology.
pub fn validate_adjacency_index_against(
    source_project_dir: &Path,
    artifact_project_dir: &Path,
) -> Result<Vec<AdjacencyValidationIssue>, GfError> {
    let manifest = read_manifest(artifact_project_dir)?;
    if manifest.is_empty() {
        return Ok(Vec::new()); // no index ⇒ nothing to validate
    }

    let mut issues = Vec::new();
    let current = crate::generation::read_topology_generation(source_project_dir)?;

    // Delta-covered (#765): a uniform base older than the counter, with an
    // intact chain over (base, current], is effectively fresh — the overlay of
    // base CSR + chain equals a rebuild at `current`. Suppress StaleGeneration
    // and diff the *effective* CSR; an incomplete/absent chain keeps today's
    // behavior (StaleGeneration + a content check against current topology, so
    // a generation bump with unchanged content is exactly one issue).
    let base = manifest.first().map(|r| r.topology_generation);
    let uniform = base.is_some_and(|b| manifest.iter().all(|r| r.topology_generation == b));
    let chain = match base {
        Some(b) if uniform && b < current => {
            crate::adjacency_delta::read_delta_chain(artifact_project_dir, b, current)
        }
        _ => None,
    };
    let delta_covered = chain.is_some();
    let chain = chain.unwrap_or_default();

    if !delta_covered
        && let Some(stale) = manifest
            .iter()
            .find(|r| r.topology_generation != current)
            .map(|r| r.topology_generation)
    {
        issues.push(AdjacencyValidationIssue::StaleGeneration {
            manifest: stale,
            current,
        });
    }

    let (groups, union_out) = collect_adjacency_groups(source_project_dir)?;
    for row in &manifest {
        let expected_entries: &[BuildEntry] = if row.relation_type == ALL_RELATIONS_STEM {
            &union_out
        } else {
            groups
                .get(&row.relation_type)
                .map_or(&[][..], Vec::as_slice)
        };
        let expected = csr_from_entries(expected_entries, row.direction);
        let path = csr_path(artifact_project_dir, &row.relation_type, row.direction);
        if !csr_artifact_exists(&path) {
            issues.push(AdjacencyValidationIssue::MissingCsr {
                rel: row.relation_type.clone(),
                direction: row.direction,
            });
            continue;
        }
        match read_csr(&path) {
            // When delta-covered, validate the overlay (base CSR + chain), not
            // the bare base CSR, against the current-topology rebuild.
            Ok(base_csr) => {
                let actual = if delta_covered {
                    crate::adjacency_delta::apply_delta_segments(
                        &base_csr,
                        &row.relation_type,
                        row.direction,
                        &chain,
                    )
                } else {
                    base_csr
                };
                if actual != expected {
                    issues.push(AdjacencyValidationIssue::Mismatch {
                        rel: row.relation_type.clone(),
                        direction: row.direction,
                    });
                }
            }
            Err(e) => issues.push(AdjacencyValidationIssue::UnreadableCsr {
                rel: row.relation_type.clone(),
                direction: row.direction,
                error: e.to_string(),
            }),
        }
    }
    Ok(issues)
}

/// Inspect adjacency identity and freshness using the same complete-delta-chain
/// rule as validation and execution.
///
/// # Errors
/// Returns a storage error only when canonical topology itself cannot be read.
pub fn inspect_adjacency_index(project_dir: &Path) -> Result<AdjacencyInspection, GfError> {
    let source_generation = crate::generation::read_topology_generation(project_dir)?;
    let (_, mut source_entries) = collect_adjacency_groups(project_dir)?;
    let source_fingerprint = entries_fingerprint(&mut source_entries);
    let Ok(manifest) = read_manifest(project_dir) else {
        return Ok(inspection_without_artifact(
            source_generation,
            source_fingerprint,
            AdjacencyFreshnessState::Incompatible,
            AdjacencyFreshnessReason::UnreadableArtifact,
        ));
    };
    if manifest.is_empty() {
        let built = manifest_path(project_dir).exists();
        return Ok(inspection_without_artifact(
            source_generation,
            source_fingerprint,
            if built {
                AdjacencyFreshnessState::Incompatible
            } else {
                AdjacencyFreshnessState::Missing
            },
            if built {
                AdjacencyFreshnessReason::UnreadableArtifact
            } else {
                AdjacencyFreshnessReason::NotBuilt
            },
        ));
    }
    let base = manifest[0].topology_generation;
    if manifest.iter().any(|row| row.topology_generation != base) {
        return Ok(AdjacencyInspection {
            source_generation,
            source_fingerprint,
            artifact_generation: None,
            artifact_effective_generation: None,
            artifact_fingerprint: None,
            state: AdjacencyFreshnessState::Incompatible,
            reason: Some(AdjacencyFreshnessReason::MixedArtifactGeneration),
        });
    }
    if base > source_generation {
        return Ok(AdjacencyInspection {
            source_generation,
            source_fingerprint,
            artifact_generation: Some(base),
            artifact_effective_generation: None,
            artifact_fingerprint: None,
            state: AdjacencyFreshnessState::Incompatible,
            reason: Some(AdjacencyFreshnessReason::FutureArtifactGeneration),
        });
    }
    let chain = if base < source_generation {
        match crate::adjacency_delta::read_delta_chain(project_dir, base, source_generation) {
            Some(chain) => chain,
            None => {
                return Ok(AdjacencyInspection {
                    source_generation,
                    source_fingerprint,
                    artifact_generation: Some(base),
                    artifact_effective_generation: None,
                    artifact_fingerprint: None,
                    state: AdjacencyFreshnessState::Stale,
                    reason: Some(AdjacencyFreshnessReason::IncompleteDeltaChain),
                });
            }
        }
    } else {
        Vec::new()
    };
    let union_path = csr_path(project_dir, ALL_RELATIONS_STEM, Direction::Out);
    if !csr_artifact_exists(&union_path) {
        return Ok(AdjacencyInspection {
            source_generation,
            source_fingerprint,
            artifact_generation: Some(base),
            artifact_effective_generation: Some(source_generation),
            artifact_fingerprint: None,
            state: AdjacencyFreshnessState::Incompatible,
            reason: Some(AdjacencyFreshnessReason::MissingCsr),
        });
    }
    let Ok(base_csr) = read_csr(&union_path) else {
        return Ok(AdjacencyInspection {
            source_generation,
            source_fingerprint,
            artifact_generation: Some(base),
            artifact_effective_generation: Some(source_generation),
            artifact_fingerprint: None,
            state: AdjacencyFreshnessState::Incompatible,
            reason: Some(AdjacencyFreshnessReason::UnreadableArtifact),
        });
    };
    inspect_effective_artifact(
        project_dir,
        source_generation,
        source_fingerprint,
        base,
        &base_csr,
        &chain,
    )
}

fn inspect_effective_artifact(
    project_dir: &Path,
    source_generation: u64,
    source_fingerprint: String,
    base: u64,
    base_csr: &CsrIndex,
    chain: &[crate::adjacency_delta::DeltaSegment],
) -> Result<AdjacencyInspection, GfError> {
    let effective = crate::adjacency_delta::apply_delta_segments(
        base_csr,
        ALL_RELATIONS_STEM,
        Direction::Out,
        chain,
    );
    let Some(mut artifact_entries) = entries_from_out_csr(&effective) else {
        return Ok(AdjacencyInspection {
            source_generation,
            source_fingerprint,
            artifact_generation: Some(base),
            artifact_effective_generation: Some(source_generation),
            artifact_fingerprint: None,
            state: AdjacencyFreshnessState::Incompatible,
            reason: Some(AdjacencyFreshnessReason::UnreadableArtifact),
        });
    };
    let artifact_fingerprint = entries_fingerprint(&mut artifact_entries);
    let validation_issues = validate_adjacency_index(project_dir)?;
    let validation_reason = validation_issues.first().map(|issue| match issue {
        AdjacencyValidationIssue::StaleGeneration { .. } => {
            AdjacencyFreshnessReason::IncompleteDeltaChain
        }
        AdjacencyValidationIssue::MissingCsr { .. } => AdjacencyFreshnessReason::MissingCsr,
        AdjacencyValidationIssue::UnreadableCsr { .. } => {
            AdjacencyFreshnessReason::UnreadableArtifact
        }
        AdjacencyValidationIssue::Mismatch { .. } => AdjacencyFreshnessReason::ContentMismatch,
    });
    let (state, reason) =
        if artifact_fingerprint == source_fingerprint && validation_reason.is_none() {
            (AdjacencyFreshnessState::Current, None)
        } else {
            (
                AdjacencyFreshnessState::Incompatible,
                Some(validation_reason.unwrap_or(AdjacencyFreshnessReason::ContentMismatch)),
            )
        };
    Ok(AdjacencyInspection {
        source_generation,
        source_fingerprint,
        artifact_generation: Some(base),
        artifact_effective_generation: Some(source_generation),
        artifact_fingerprint: Some(artifact_fingerprint),
        state,
        reason,
    })
}

fn inspection_without_artifact(
    source_generation: u64,
    source_fingerprint: String,
    state: AdjacencyFreshnessState,
    reason: AdjacencyFreshnessReason,
) -> AdjacencyInspection {
    AdjacencyInspection {
        source_generation,
        source_fingerprint,
        artifact_generation: None,
        artifact_effective_generation: None,
        artifact_fingerprint: None,
        state,
        reason: Some(reason),
    }
}

fn entries_from_out_csr(csr: &CsrIndex) -> Option<Vec<BuildEntry>> {
    let mut entries = Vec::with_capacity(csr.edge_ids.len());
    for (src_index, offsets) in csr.offsets.windows(2).enumerate() {
        let src = u64::try_from(src_index).ok()?;
        let start = usize::try_from(offsets[0]).ok()?;
        let end = usize::try_from(offsets[1]).ok()?;
        if start > end || end > csr.edge_ids.len() || end > csr.neighbor_ids.len() {
            return None;
        }
        for index in start..end {
            entries.push((src, csr.edge_ids[index], csr.neighbor_ids[index]));
        }
    }
    Some(entries)
}

fn entries_fingerprint(entries: &mut [BuildEntry]) -> String {
    entries.sort_unstable();
    let mut digest = Sha256::new();
    digest.update(b"graphforge/adjacency-topology/v1\0");
    for &(src, edge, dst) in entries.iter() {
        digest.update(src.to_le_bytes());
        digest.update(edge.to_le_bytes());
        digest.update(dst.to_le_bytes());
    }
    {
        use std::fmt::Write as _;
        let hex = digest
            .finalize()
            .iter()
            .fold(String::with_capacity(64), |mut out, byte| {
                let _ = write!(out, "{byte:02x}");
                out
            });
        format!("sha256:{hex}")
    }
}

/// Build a dense [`CsrIndex`] from `(src_id, edge_id, dst_id)` entries for the
/// given direction: `out` is keyed by `src_id` with `dst_id` neighbors, sorted
/// by `(src_id, edge_id)`; `in` is keyed by `dst_id` with `src_id` neighbors,
/// sorted by `(dst_id, edge_id)`. Surrogate gaps (post-DELETE) are empty rows.
pub(crate) fn csr_from_entries(entries: &[BuildEntry], direction: Direction) -> CsrIndex {
    let mut keyed: Vec<(u64, u64, u64)> = entries
        .iter()
        .map(|&(src, edge, dst)| match direction {
            Direction::Out => (src, edge, dst),
            Direction::In => (dst, edge, src),
        })
        .collect();
    keyed.sort_unstable_by_key(|&(key, edge, _)| (key, edge));

    let node_count = keyed.last().map_or(0, |&(key, _, _)| key + 1);
    let mut csr = CsrIndex {
        offsets: Vec::with_capacity(usize::try_from(node_count).unwrap_or(0) + 1),
        edge_ids: Vec::with_capacity(keyed.len()),
        neighbor_ids: Vec::with_capacity(keyed.len()),
    };
    csr.offsets.push(0);
    let mut next = 0usize; // index into `keyed`
    for node in 0..node_count {
        while next < keyed.len() && keyed[next].0 == node {
            csr.edge_ids.push(keyed[next].1);
            csr.neighbor_ids.push(keyed[next].2);
            next += 1;
        }
        csr.offsets.push(csr.edge_count());
    }
    csr
}

/// Whether `rel` is usable as a CSR file stem: a single plain path component
/// (no separators, no `..`, non-empty — the same rule `read_edges` applies to
/// typed file names) and not the reserved [`ALL_RELATIONS_STEM`].
pub(crate) fn usable_stem(rel: &str) -> bool {
    if rel == ALL_RELATIONS_STEM {
        return false;
    }
    let mut comps = Path::new(rel).components();
    matches!(comps.next(), Some(std::path::Component::Normal(_))) && comps.next().is_none()
}

/// Borrow a column by name, erroring on absence.
fn named_column<'a>(
    batch: &'a arrow::record_batch::RecordBatch,
    name: &str,
) -> Result<&'a arrow::array::ArrayRef, GfError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| GfError::Storage(format!("adjacency build: missing column {name}")))
}

/// Atomically rename `tmp` into place at `path`.
fn persist_temp(tmp: NamedTempFile, path: &Path) -> Result<(), GfError> {
    tmp.persist(path)
        .map(|_| ())
        .map_err(|e| storage_err(e.error))
}

fn uint64_column<'a>(
    column: &'a arrow::array::ArrayRef,
    name: &str,
) -> Result<&'a UInt64Array, GfError> {
    column
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| GfError::Storage(format!("adjacency: {name} column is not UInt64")))
}

fn string_column<'a>(
    column: &'a arrow::array::ArrayRef,
    name: &str,
) -> Result<&'a StringArray, GfError> {
    column
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| GfError::Storage(format!("adjacency: {name} column is not Utf8")))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    /// 3 nodes / 4 entries: node 0 → two neighbors, node 1 → none, node 2 → two.
    fn sample_csr() -> CsrIndex {
        CsrIndex {
            offsets: vec![0, 2, 2, 4],
            edge_ids: vec![10, 11, 12, 13],
            neighbor_ids: vec![1, 2, 0, 1],
        }
    }

    #[test]
    fn sharded_csr_crosses_boundaries_and_rejects_missing_or_corrupt_shards() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("KNOWS.out.csr");
        let expected = sample_csr();
        write_sharded_csr(&path, &expected, 2).unwrap();
        let reader = ShardedCsrIndex::open(&path).unwrap();
        assert_eq!((reader.node_count(), reader.edge_count()), (3, 4));
        for node in 0..expected.node_count() {
            assert_eq!(
                reader.row(node).unwrap(),
                expected.row(node).iter().collect::<Vec<_>>()
            );
        }

        let first = reader.root.join(&reader.manifest.shards[0].file);
        let original = std::fs::read(&first).unwrap();
        std::fs::write(&first, b"corrupt").unwrap();
        assert!(reader.row(0).unwrap_err().to_string().contains("checksum"));
        assert!(
            reader
                .row_len(0)
                .unwrap_err()
                .to_string()
                .contains("checksum")
        );
        assert!(
            reader
                .row_chunk(0, 0, 1)
                .unwrap_err()
                .to_string()
                .contains("checksum")
        );
        std::fs::write(&first, original).unwrap();
        std::fs::remove_file(&first).unwrap();
        assert!(
            reader
                .row(0)
                .unwrap_err()
                .to_string()
                .contains("missing CSR shard")
        );
        assert!(
            reader
                .row_len(0)
                .unwrap_err()
                .to_string()
                .contains("missing CSR shard")
        );
        assert!(
            reader
                .row_chunk(0, 0, 1)
                .unwrap_err()
                .to_string()
                .contains("missing CSR shard")
        );
    }

    #[test]
    fn opening_sharded_csr_does_not_read_or_decode_shard_payloads() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("KNOWS.out.csr");
        let expected = sample_csr();
        write_sharded_csr(&path, &expected, 2).unwrap();
        let published = ShardedCsrIndex::open(&path).unwrap();
        assert_eq!(
            (published.node_count(), published.edge_count()),
            (expected.node_count(), expected.edge_count())
        );
        for record in &published.manifest.shards {
            std::fs::write(published.root.join(&record.file), b"corrupt-payload").unwrap();
        }
        let reader = ShardedCsrIndex::open(&path).expect("open must not decode shard payloads");
        assert_eq!(
            (reader.node_count(), reader.edge_count()),
            (expected.node_count(), expected.edge_count())
        );
        assert!(reader.row(0).unwrap_err().to_string().contains("checksum"));
    }

    #[test]
    fn sequential_rows_reuse_only_the_current_authenticated_shard() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("KNOWS.out.csr");
        let expected = sample_csr();
        write_sharded_csr(&path, &expected, 8).unwrap();
        let reader = ShardedCsrIndex::open(&path).unwrap();
        assert_eq!(reader.row(0).unwrap(), vec![(10, 1), (11, 2)]);
        std::fs::remove_file(reader.root.join(&reader.manifest.shards[0].file)).unwrap();
        assert_eq!(reader.row(2).unwrap(), vec![(12, 0), (13, 1)]);
    }

    #[test]
    fn deterministic_rebuild_repairs_a_corrupt_stable_shard_set() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("KNOWS.out.csr");
        let expected = sample_csr();
        write_sharded_csr(&path, &expected, 2).unwrap();
        let reader = ShardedCsrIndex::open(&path).unwrap();
        std::fs::write(
            reader.root.join(&reader.manifest.shards[0].file),
            b"corrupt",
        )
        .unwrap();

        write_sharded_csr(&path, &expected, 2).unwrap();
        assert_eq!(read_csr(&path).unwrap(), expected);
    }

    #[test]
    fn failed_manifest_republish_does_not_delete_reused_live_shards() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("KNOWS.out.csr");
        let manifest_path = path.with_extension("csr.json");
        let saved_manifest = directory.path().join("saved-manifest.json");
        let expected = sample_csr();
        write_sharded_csr(&path, &expected, 2).unwrap();
        std::fs::rename(&manifest_path, &saved_manifest).unwrap();
        std::fs::create_dir(&manifest_path).unwrap();

        assert!(write_sharded_csr(&path, &expected, 2).is_err());
        std::fs::remove_dir(&manifest_path).unwrap();
        std::fs::rename(&saved_manifest, &manifest_path).unwrap();
        assert_eq!(read_csr(&path).unwrap(), expected);
    }

    #[test]
    fn high_degree_row_spans_hard_capped_shards_in_order() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("KNOWS.out.csr");
        let expected = CsrIndex {
            offsets: vec![0, 7],
            edge_ids: (10..17).collect(),
            neighbor_ids: (20..27).collect(),
        };
        write_sharded_csr(&path, &expected, 2).unwrap();
        let reader = ShardedCsrIndex::open(&path).unwrap();
        assert_eq!(reader.manifest.shards.len(), 4);
        assert!(
            reader
                .manifest
                .shards
                .iter()
                .all(|shard| shard.edge_count <= 2)
        );
        assert!(
            reader
                .manifest
                .shards
                .iter()
                .all(|shard| shard.first_node == 0)
        );
        assert_eq!(
            reader.row(0).unwrap(),
            expected.row(0).iter().collect::<Vec<_>>()
        );
        assert_eq!(reader.row_len(0).unwrap(), 7);
        assert_eq!(reader.row_len(1).unwrap(), 0);
        let mut chunks = Vec::new();
        for offset in (0..7).step_by(3) {
            let chunk = reader.row_chunk(0, offset, 3).unwrap();
            assert!(chunk.len() <= 3);
            chunks.extend(chunk);
            let cache = reader.cache.lock().unwrap();
            assert!(cache.as_ref().unwrap().1.edge_count() <= 2);
        }
        assert_eq!(chunks, expected.row(0).iter().collect::<Vec<_>>());
        assert!(reader.row_chunk(0, 7, 3).unwrap().is_empty());
    }

    #[test]
    fn sparse_surrogate_gap_cannot_expand_one_shard_offsets() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("KNOWS.out.csr");
        let mut writer = ShardedCsrWriter::create(&path, 8, 2).unwrap();
        writer.emit((0, 1, 100)).unwrap();
        writer.emit((1_000_000, 2, 0)).unwrap();
        let (shards, _, peak_nodes) = writer.finish(1_000_001).unwrap();
        assert_eq!(shards, 2);
        assert!(peak_nodes <= 2);
        let reader = ShardedCsrIndex::open(&path).unwrap();
        assert_eq!(reader.row(0).unwrap(), vec![(1, 100)]);
        assert_eq!(reader.row(999_999).unwrap(), Vec::<(u64, u64)>::new());
        assert_eq!(reader.row(1_000_000).unwrap(), vec![(2, 0)]);
    }

    #[test]
    fn legacy_single_batch_csr_migrates_to_shards_on_rebuild() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("KNOWS.out.csr");
        let expected = sample_csr();

        write_csr(&path, &expected).unwrap();
        assert!(!sharded_csr_exists(&path));
        assert_eq!(read_csr(&path).unwrap(), expected);

        write_sharded_csr(&path, &expected, 2).unwrap();
        let shard_root = ShardedCsrIndex::open(&path).unwrap().root;
        assert!(sharded_csr_exists(&path));
        assert_eq!(read_csr(&path).unwrap(), expected);

        write_csr(&path, &expected).unwrap();
        assert!(!shard_root.exists());
        assert!(!sharded_csr_exists(&path));
        assert_eq!(read_csr(&path).unwrap(), expected);
    }

    #[test]
    fn legacy_cleanup_rejects_parent_directory_manifest_paths() {
        let directory = TempDir::new().unwrap();
        let adjacency = directory.path().join("adjacency");
        std::fs::create_dir(&adjacency).unwrap();
        let sentinel = directory.path().join("must-remain");
        std::fs::write(&sentinel, b"safe").unwrap();
        let path = adjacency.join("KNOWS.out.csr");
        let malicious = CsrShardManifest {
            format: "graphforge.csr-shards".into(),
            version: SHARDED_CSR_VERSION,
            node_count: 0,
            edge_count: 0,
            shard_dir: "..".into(),
            shards: Vec::new(),
        };
        std::fs::write(
            path.with_extension("csr.json"),
            serde_json::to_vec(&malicious).unwrap(),
        )
        .unwrap();

        write_csr(&path, &sample_csr()).unwrap();
        assert_eq!(std::fs::read(&sentinel).unwrap(), b"safe");
    }

    #[test]
    fn entries_from_out_csr_rejects_malformed_offset_bounds() {
        let past_targets = CsrIndex {
            offsets: vec![0, 2],
            edge_ids: vec![7],
            neighbor_ids: vec![9],
        };
        assert!(entries_from_out_csr(&past_targets).is_none());

        let descending = CsrIndex {
            offsets: vec![1, 0],
            edge_ids: vec![7],
            neighbor_ids: vec![9],
        };
        assert!(entries_from_out_csr(&descending).is_none());

        let mismatched_columns = CsrIndex {
            offsets: vec![0, 1],
            edge_ids: vec![7],
            neighbor_ids: vec![],
        };
        assert!(entries_from_out_csr(&mismatched_columns).is_none());

        let missing_offsets = CsrIndex {
            offsets: vec![],
            edge_ids: vec![],
            neighbor_ids: vec![],
        };
        assert_eq!(entries_from_out_csr(&missing_offsets), Some(Vec::new()));
    }

    #[test]
    fn wave10_effective_entry_decode_rejects_malformed_csr_bounds() {
        for malformed in [
            CsrIndex {
                offsets: vec![0, 2],
                edge_ids: vec![1],
                neighbor_ids: vec![2],
            },
            CsrIndex {
                offsets: vec![1, 0],
                edge_ids: vec![1],
                neighbor_ids: vec![2],
            },
            CsrIndex {
                offsets: vec![0, 1],
                edge_ids: vec![1],
                neighbor_ids: vec![],
            },
        ] {
            assert!(entries_from_out_csr(&malformed).is_none());
        }
    }

    #[test]
    fn public_csr_writer_rejects_every_inconsistent_topology_shape_without_file() {
        let dir = TempDir::new().unwrap();
        let path = csr_path(dir.path(), "BROKEN", Direction::Out);
        for malformed in [
            CsrIndex {
                offsets: vec![],
                edge_ids: vec![],
                neighbor_ids: vec![],
            },
            CsrIndex {
                offsets: vec![0, 2],
                edge_ids: vec![1],
                neighbor_ids: vec![2],
            },
            CsrIndex {
                offsets: vec![0, 1],
                edge_ids: vec![1],
                neighbor_ids: vec![],
            },
            CsrIndex {
                offsets: vec![1],
                edge_ids: vec![],
                neighbor_ids: vec![],
            },
        ] {
            assert_eq!(write_csr(&path, &malformed).unwrap_err().code(), "GF_IO");
            assert!(!path.exists());
        }
    }

    #[test]
    fn csr_round_trip_preserves_offsets_and_targets() {
        let dir = TempDir::new().unwrap();
        let path = csr_path(dir.path(), "KNOWS", Direction::Out);
        let csr = sample_csr();
        write_csr(&path, &csr).unwrap();
        assert_eq!(read_csr(&path).unwrap(), csr);
    }

    #[test]
    fn csr_row_lookup_is_o1_and_handles_empty_boundary_and_oor() {
        let csr = sample_csr();
        let row0 = csr.row(0);
        assert_eq!(row0.len(), 2);
        assert_eq!(row0.get(0), Some((csr.edge_ids[0], csr.neighbor_ids[0])));
        assert_eq!(row0.get(1), Some((csr.edge_ids[1], csr.neighbor_ids[1])));
        assert!(csr.row(1).is_empty(), "empty interior row");
        assert_eq!(csr.row(2).len(), 2);
        assert!(csr.row(3).is_empty(), "out of range");
        assert!(csr.row(u64::MAX).is_empty());
        let empty = CsrIndex {
            offsets: vec![0],
            ..CsrIndex::default()
        };
        assert!(empty.row(0).is_empty());
    }

    #[test]
    fn empty_graph_round_trips_as_offsets_zero() {
        let dir = TempDir::new().unwrap();
        let path = csr_path(dir.path(), "KNOWS", Direction::In);
        let csr = CsrIndex {
            offsets: vec![0],
            ..CsrIndex::default()
        };
        write_csr(&path, &csr).unwrap();
        let back = read_csr(&path).unwrap();
        assert_eq!(back, csr);
        assert_eq!(back.node_count(), 0);
        assert_eq!(back.edge_count(), 0);
    }

    #[test]
    fn node_with_no_neighbors_round_trips() {
        let dir = TempDir::new().unwrap();
        let path = csr_path(dir.path(), ALL_RELATIONS_STEM, Direction::Out);
        let csr = sample_csr(); // node 1 has an empty range
        write_csr(&path, &csr).unwrap();
        let back = read_csr(&path).unwrap();
        assert_eq!(back.offsets[1], back.offsets[2], "node 1 has no neighbors");
        assert_eq!(back.node_count(), 3);
        assert_eq!(back.edge_count(), 4);
    }

    #[test]
    fn write_csr_replaces_existing_file_atomically() {
        let dir = TempDir::new().unwrap();
        let path = csr_path(dir.path(), "KNOWS", Direction::Out);
        write_csr(&path, &sample_csr()).unwrap();

        let newer = CsrIndex {
            offsets: vec![0, 1],
            edge_ids: vec![99],
            neighbor_ids: vec![0],
        };
        write_csr(&path, &newer).unwrap();
        assert_eq!(read_csr(&path).unwrap(), newer, "second write wins");

        let temps = std::fs::read_dir(adjacency_dir(dir.path()))
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| e.path().extension().is_some_and(|x| x == "tmp"))
            .count();
        assert_eq!(temps, 0, "no temp residue");
    }

    #[test]
    fn read_csr_rejects_wrong_schema() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("bogus.csr");
        std::fs::write(&path, b"not an arrow ipc file").unwrap();
        assert!(matches!(read_csr(&path), Err(GfError::Storage(_))));
    }

    #[test]
    fn read_csr_missing_file_is_an_error() {
        let dir = TempDir::new().unwrap();
        let path = csr_path(dir.path(), "ABSENT", Direction::Out);
        assert!(matches!(read_csr(&path), Err(GfError::Storage(_))));
    }

    #[test]
    fn write_csr_rejects_invalid_offsets() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("bad.csr");

        // Non-monotone offsets.
        let non_monotone = CsrIndex {
            offsets: vec![0, 3, 1],
            edge_ids: vec![1],
            neighbor_ids: vec![1],
        };
        assert!(matches!(
            write_csr(&path, &non_monotone),
            Err(GfError::Storage(_))
        ));

        // Final offset disagrees with target lengths.
        let length_mismatch = CsrIndex {
            offsets: vec![0, 2],
            edge_ids: vec![1],
            neighbor_ids: vec![1],
        };
        assert!(matches!(
            write_csr(&path, &length_mismatch),
            Err(GfError::Storage(_))
        ));

        // Empty offsets (the empty graph must be [0], not []).
        let empty_offsets = CsrIndex::default();
        assert!(matches!(
            write_csr(&path, &empty_offsets),
            Err(GfError::Storage(_))
        ));

        // Targets of differing lengths.
        let ragged = CsrIndex {
            offsets: vec![0, 2],
            edge_ids: vec![1, 2],
            neighbor_ids: vec![1],
        };
        assert!(matches!(
            write_csr(&path, &ragged),
            Err(GfError::Storage(_))
        ));

        assert!(!path.exists(), "no file written for invalid CSR");
    }

    #[test]
    fn manifest_round_trip_multi_relation() {
        const TS: i64 = 1_700_000_000_000_000;
        let dir = TempDir::new().unwrap();
        let rows = vec![
            AdjacencyManifestRow {
                relation_type: "WORKS_AT".to_owned(),
                direction: Direction::Out,
                topology_generation: 7,
                built_at_micros: TS,
                node_count: 100,
                edge_count: 250,
            },
            AdjacencyManifestRow {
                relation_type: "WORKS_AT".to_owned(),
                direction: Direction::In,
                topology_generation: 7,
                built_at_micros: TS,
                node_count: 100,
                edge_count: 250,
            },
            AdjacencyManifestRow {
                relation_type: "OWNS".to_owned(),
                direction: Direction::Out,
                topology_generation: 7,
                built_at_micros: TS + 1,
                node_count: 40,
                edge_count: 41,
            },
            AdjacencyManifestRow {
                relation_type: ALL_RELATIONS_STEM.to_owned(),
                direction: Direction::Out,
                topology_generation: 7,
                built_at_micros: TS + 2,
                node_count: 100,
                edge_count: 291,
            },
        ];
        write_manifest(dir.path(), &rows).unwrap();
        assert_eq!(read_manifest(dir.path()).unwrap(), rows);
    }

    #[test]
    fn read_manifest_absent_returns_empty() {
        let dir = TempDir::new().unwrap();
        assert_eq!(read_manifest(dir.path()).unwrap(), Vec::new());
    }

    #[test]
    fn write_manifest_replaces_existing() {
        let dir = TempDir::new().unwrap();
        let first = vec![AdjacencyManifestRow {
            relation_type: "KNOWS".to_owned(),
            direction: Direction::Out,
            topology_generation: 1,
            built_at_micros: 0,
            node_count: 1,
            edge_count: 1,
        }];
        write_manifest(dir.path(), &first).unwrap();

        let second = vec![AdjacencyManifestRow {
            relation_type: "KNOWS".to_owned(),
            direction: Direction::Out,
            topology_generation: 2,
            built_at_micros: 1,
            node_count: 2,
            edge_count: 3,
        }];
        write_manifest(dir.path(), &second).unwrap();
        assert_eq!(read_manifest(dir.path()).unwrap(), second);
    }

    #[test]
    fn read_manifest_rejects_wrong_schema() {
        let dir = TempDir::new().unwrap();
        // Write a valid Parquet file with the wrong schema at the manifest path.
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            "v",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(arrow::array::Int64Array::from(vec![1]))],
        )
        .unwrap();
        let mut staged = RewriteBatch::new();
        staged
            .stage(&manifest_path(dir.path()), schema, &batch)
            .unwrap();
        staged.commit_at(dir.path()).unwrap();

        assert!(matches!(
            read_manifest(dir.path()),
            Err(GfError::Storage(_))
        ));
    }

    #[test]
    fn direction_round_trips_through_str() {
        for d in [Direction::Out, Direction::In] {
            assert_eq!(Direction::parse(d.as_str()).unwrap(), d);
        }
        assert!(matches!(
            Direction::parse("sideways"),
            Err(GfError::Storage(_))
        ));
    }

    #[test]
    fn csr_path_layout() {
        let p = csr_path(Path::new("/proj"), "WORKS_AT", Direction::In);
        assert_eq!(p, Path::new("/proj/indexes/adjacency/WORKS_AT.in.csr"));
        assert_eq!(
            manifest_path(Path::new("/proj")),
            Path::new("/proj/indexes/adjacency/index_manifest.parquet")
        );
    }

    // -----------------------------------------------------------------------
    // build_adjacency_index (#761)
    // -----------------------------------------------------------------------

    use crate::GraphWriter;
    use graphforge_core::OntologyMode;
    use graphforge_core::TypeId;
    use graphforge_core::uuid::{Uuid, new_v7};

    /// Fixed timestamp for deterministic fixtures and manifests.
    pub(super) const BUILD_TS: i64 = 1_700_000_000_000_000;

    /// Strict-mode diamond a->b, a->c, b->d, c->d plus a parallel a->b and a
    /// self-loop d->d, all KNOWS. Returns the surrogate node ids.
    pub(super) fn write_diamond(dir: &Path) -> [u64; 4] {
        let mut w = GraphWriter::open_at(dir, OntologyMode::Strict, BUILD_TS).unwrap();
        let uuids: Vec<Uuid> = (0..4).map(|_| new_v7()).collect();
        let ids: Vec<u64> = uuids
            .iter()
            .map(|u| {
                w.create_node(
                    *u,
                    graphforge_value::EntityTypeId::ontology(TypeId(0)).unwrap(),
                )
                .unwrap()
            })
            .collect();
        let (a, b, c, d) = (&uuids[0], &uuids[1], &uuids[2], &uuids[3]);
        for (src, dst) in [(a, b), (a, c), (b, d), (c, d), (a, b), (d, d)] {
            w.create_edge(new_v7(), "KNOWS", src, dst).unwrap();
        }
        w.flush().unwrap();
        [ids[0], ids[1], ids[2], ids[3]]
    }

    // -----------------------------------------------------------------------
    // validate_adjacency_index (#766)
    // -----------------------------------------------------------------------

    #[test]
    fn validate_reports_clean_on_valid_index() {
        let dir = TempDir::new().unwrap();
        write_diamond(dir.path());
        build_adjacency_index(dir.path(), BUILD_TS).unwrap();
        assert_eq!(validate_adjacency_index(dir.path()).unwrap(), Vec::new());
    }

    #[test]
    fn validate_reports_clean_on_absent_index() {
        let dir = TempDir::new().unwrap();
        write_diamond(dir.path());
        assert_eq!(validate_adjacency_index(dir.path()).unwrap(), Vec::new());
    }

    #[test]
    fn inspection_moves_from_missing_to_current_with_stable_identity() {
        let dir = TempDir::new().unwrap();
        write_diamond(dir.path());
        let missing = inspect_adjacency_index(dir.path()).unwrap();
        assert_eq!(missing.state, AdjacencyFreshnessState::Missing);
        assert_eq!(missing.reason, Some(AdjacencyFreshnessReason::NotBuilt));
        assert!(missing.source_fingerprint.starts_with("sha256:"));

        build_adjacency_index(dir.path(), BUILD_TS).unwrap();
        let current = inspect_adjacency_index(dir.path()).unwrap();
        assert_eq!(current.state, AdjacencyFreshnessState::Current);
        assert_eq!(current.reason, None);
        assert_eq!(current.artifact_generation, Some(current.source_generation));
        assert_eq!(
            current.artifact_fingerprint.as_deref(),
            Some(current.source_fingerprint.as_str())
        );
    }

    #[test]
    fn inspection_checks_every_manifest_referenced_csr() {
        let dir = TempDir::new().unwrap();
        write_diamond(dir.path());
        build_adjacency_index(dir.path(), BUILD_TS).unwrap();
        let path = csr_path(dir.path(), "KNOWS", Direction::In);
        let reader = ShardedCsrIndex::open(&path).unwrap();
        std::fs::write(
            reader.root.join(&reader.manifest.shards[0].file),
            b"garbage",
        )
        .unwrap();

        let inspection = inspect_adjacency_index(dir.path()).unwrap();
        assert_eq!(inspection.state, AdjacencyFreshnessState::Incompatible);
        assert_eq!(
            inspection.reason,
            Some(AdjacencyFreshnessReason::UnreadableArtifact)
        );
    }

    #[test]
    fn inspection_distinguishes_manifest_union_absence_and_union_corruption_after_reopen() {
        for case in ["manifest", "missing-union", "corrupt-union"] {
            let dir = TempDir::new().unwrap();
            write_diamond(dir.path());
            build_adjacency_index(dir.path(), BUILD_TS).unwrap();
            match case {
                "manifest" => std::fs::write(manifest_path(dir.path()), b"corrupt").unwrap(),
                "missing-union" => {
                    std::fs::remove_file(
                        csr_path(dir.path(), ALL_RELATIONS_STEM, Direction::Out)
                            .with_extension("csr.json"),
                    )
                    .unwrap();
                }
                "corrupt-union" => {
                    let path = csr_path(dir.path(), ALL_RELATIONS_STEM, Direction::Out);
                    let reader = ShardedCsrIndex::open(&path).unwrap();
                    std::fs::write(
                        reader.root.join(&reader.manifest.shards[0].file),
                        b"corrupt",
                    )
                    .unwrap();
                }
                _ => unreachable!(),
            }

            let inspection = inspect_adjacency_index(dir.path()).unwrap();
            assert_eq!(inspection.state, AdjacencyFreshnessState::Incompatible);
            assert_eq!(
                inspection.reason,
                Some(if case == "missing-union" {
                    AdjacencyFreshnessReason::MissingCsr
                } else {
                    AdjacencyFreshnessReason::UnreadableArtifact
                })
            );
            assert_eq!(inspection.artifact_fingerprint, None);
        }
    }

    #[test]
    fn inspection_accepts_only_a_complete_delta_chain_as_current() {
        let dir = TempDir::new().unwrap();
        write_diamond(dir.path());
        build_adjacency_index(dir.path(), BUILD_TS).unwrap();
        crate::generation::force_bump_topology_generation_for_test(dir.path()).unwrap();

        let inspection = inspect_adjacency_index(dir.path()).unwrap();
        assert_eq!(inspection.state, AdjacencyFreshnessState::Stale);
        assert_eq!(
            inspection.reason,
            Some(AdjacencyFreshnessReason::IncompleteDeltaChain)
        );
    }

    #[test]
    fn freshness_vocabulary_and_manifest_generation_failures_are_exact() {
        assert_eq!(AdjacencyFreshnessState::Current.as_str(), "current");
        assert_eq!(AdjacencyFreshnessState::Missing.as_str(), "missing");
        assert_eq!(AdjacencyFreshnessState::Stale.as_str(), "stale");
        assert_eq!(
            AdjacencyFreshnessState::Incompatible.as_str(),
            "incompatible"
        );
        for (reason, token) in [
            (AdjacencyFreshnessReason::NotBuilt, "not_built"),
            (
                AdjacencyFreshnessReason::MixedArtifactGeneration,
                "mixed_artifact_generation",
            ),
            (
                AdjacencyFreshnessReason::IncompleteDeltaChain,
                "incomplete_delta_chain",
            ),
            (AdjacencyFreshnessReason::MissingCsr, "missing_csr"),
            (
                AdjacencyFreshnessReason::UnreadableArtifact,
                "unreadable_artifact",
            ),
            (
                AdjacencyFreshnessReason::ContentMismatch,
                "content_mismatch",
            ),
            (
                AdjacencyFreshnessReason::FutureArtifactGeneration,
                "future_artifact_generation",
            ),
        ] {
            assert_eq!(reason.as_str(), token);
        }

        let mixed = TempDir::new().unwrap();
        write_diamond(mixed.path());
        build_adjacency_index(mixed.path(), BUILD_TS).unwrap();
        let mut manifest = read_manifest(mixed.path()).unwrap();
        manifest[0].topology_generation += 1;
        write_manifest(mixed.path(), &manifest).unwrap();
        let inspection = inspect_adjacency_index(mixed.path()).unwrap();
        assert_eq!(inspection.state, AdjacencyFreshnessState::Incompatible);
        assert_eq!(
            inspection.reason,
            Some(AdjacencyFreshnessReason::MixedArtifactGeneration)
        );
        assert_eq!(inspection.artifact_generation, None);

        let future = TempDir::new().unwrap();
        write_diamond(future.path());
        build_adjacency_index(future.path(), BUILD_TS).unwrap();
        let mut manifest = read_manifest(future.path()).unwrap();
        for row in &mut manifest {
            row.topology_generation += 1;
        }
        write_manifest(future.path(), &manifest).unwrap();
        let inspection = inspect_adjacency_index(future.path()).unwrap();
        assert_eq!(inspection.state, AdjacencyFreshnessState::Incompatible);
        assert_eq!(
            inspection.reason,
            Some(AdjacencyFreshnessReason::FutureArtifactGeneration)
        );
        assert_eq!(inspection.artifact_generation, Some(2));
        assert_eq!(inspection.artifact_fingerprint, None);
    }

    #[test]
    fn validate_detects_corrupted_csr() {
        let dir = TempDir::new().unwrap();
        write_diamond(dir.path());
        build_adjacency_index(dir.path(), BUILD_TS).unwrap();

        // Overwrite KNOWS.out.csr with a VALID but WRONG CSR (content swap).
        let bogus = CsrIndex {
            offsets: vec![0, 1],
            edge_ids: vec![99],
            neighbor_ids: vec![1],
        };
        write_csr(&csr_path(dir.path(), "KNOWS", Direction::Out), &bogus).unwrap();

        let issues = validate_adjacency_index(dir.path()).unwrap();
        assert_eq!(
            issues,
            vec![AdjacencyValidationIssue::Mismatch {
                rel: "KNOWS".to_owned(),
                direction: Direction::Out,
            }]
        );
    }

    #[test]
    fn validate_detects_unreadable_csr() {
        let dir = TempDir::new().unwrap();
        write_diamond(dir.path());
        build_adjacency_index(dir.path(), BUILD_TS).unwrap();
        let path = csr_path(dir.path(), "KNOWS", Direction::In);
        let reader = ShardedCsrIndex::open(&path).unwrap();
        std::fs::write(
            reader.root.join(&reader.manifest.shards[0].file),
            b"garbage",
        )
        .unwrap();

        let issues = validate_adjacency_index(dir.path()).unwrap();
        assert_eq!(issues.len(), 1);
        assert!(matches!(
            &issues[0],
            AdjacencyValidationIssue::UnreadableCsr { rel, direction: Direction::In, .. }
                if rel == "KNOWS"
        ));
    }

    #[test]
    fn validate_detects_missing_csr() {
        let dir = TempDir::new().unwrap();
        write_diamond(dir.path());
        build_adjacency_index(dir.path(), BUILD_TS).unwrap();
        std::fs::remove_file(
            csr_path(dir.path(), ALL_RELATIONS_STEM, Direction::Out).with_extension("csr.json"),
        )
        .unwrap();

        let issues = validate_adjacency_index(dir.path()).unwrap();
        assert_eq!(
            issues,
            vec![AdjacencyValidationIssue::MissingCsr {
                rel: ALL_RELATIONS_STEM.to_owned(),
                direction: Direction::Out,
            }]
        );
    }

    #[test]
    fn validate_detects_stale_generation_only_once() {
        let dir = TempDir::new().unwrap();
        write_diamond(dir.path());
        build_adjacency_index(dir.path(), BUILD_TS).unwrap();
        // Bump the counter without touching topology content: the index is
        // stale but its content still matches the (unchanged) edge files.
        crate::generation::force_bump_topology_generation_for_test(dir.path()).unwrap();

        let issues = validate_adjacency_index(dir.path()).unwrap();
        assert_eq!(
            issues,
            vec![AdjacencyValidationIssue::StaleGeneration {
                manifest: 1,
                current: 2,
            }]
        );
    }

    /// #765: a delta-covered index (stale by generation but an intact chain
    /// covers the gap) validates **clean** — no `StaleGeneration`, no
    /// `Mismatch` — because the base CSR + chain overlay equals a rebuild at
    /// the current generation.
    #[test]
    fn validate_clean_on_delta_covered_index() {
        let dir = TempDir::new().unwrap();
        write_diamond(dir.path());
        build_adjacency_index(dir.path(), BUILD_TS).unwrap();

        // A pure-append flush writes a delta segment (bumps the generation).
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Strict, BUILD_TS).unwrap();
        let (a, b) = (new_v7(), new_v7());
        w.create_node(
            a,
            graphforge_value::EntityTypeId::ontology(TypeId(0)).unwrap(),
        )
        .unwrap();
        w.create_node(
            b,
            graphforge_value::EntityTypeId::ontology(TypeId(0)).unwrap(),
        )
        .unwrap();
        w.create_edge(new_v7(), "KNOWS", &a, &b).unwrap();
        w.flush().unwrap();

        assert!(
            validate_adjacency_index(dir.path()).unwrap().is_empty(),
            "base + intact chain == current rebuild ⇒ no issues"
        );
        let inspection = inspect_adjacency_index(dir.path()).unwrap();
        assert_eq!(inspection.state, AdjacencyFreshnessState::Current);
        assert_eq!(
            inspection.artifact_effective_generation,
            Some(inspection.source_generation)
        );
        assert_eq!(
            inspection.artifact_fingerprint.as_deref(),
            Some(inspection.source_fingerprint.as_str())
        );
    }

    /// A corrupt base CSR under a delta chain is still caught: the overlay diff
    /// against the current rebuild reports a `Mismatch` (delta coverage does not
    /// mask corruption).
    #[test]
    fn validate_detects_mismatch_under_delta_chain() {
        let dir = TempDir::new().unwrap();
        write_diamond(dir.path());
        build_adjacency_index(dir.path(), BUILD_TS).unwrap();
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Strict, BUILD_TS).unwrap();
        let (a, b) = (new_v7(), new_v7());
        w.create_node(
            a,
            graphforge_value::EntityTypeId::ontology(TypeId(0)).unwrap(),
        )
        .unwrap();
        w.create_node(
            b,
            graphforge_value::EntityTypeId::ontology(TypeId(0)).unwrap(),
        )
        .unwrap();
        w.create_edge(new_v7(), "KNOWS", &a, &b).unwrap();
        w.flush().unwrap();

        // Corrupt the base KNOWS.out CSR by overwriting it with an empty index
        // (the diamond has only KNOWS, so the _all CSR is identical and would
        // not be a corruption).
        let knows_out = csr_path(dir.path(), "KNOWS", Direction::Out);
        write_csr(&knows_out, &csr_from_entries(&[], Direction::Out)).unwrap();

        let issues = validate_adjacency_index(dir.path()).unwrap();
        assert!(
            issues.contains(&AdjacencyValidationIssue::Mismatch {
                rel: "KNOWS".to_owned(),
                direction: Direction::Out,
            }),
            "corruption under a chain is still detected: {issues:?}"
        );
        let inspection = inspect_adjacency_index(dir.path()).unwrap();
        assert_eq!(inspection.state, AdjacencyFreshnessState::Incompatible);
        assert_eq!(
            inspection.artifact_effective_generation,
            Some(inspection.source_generation)
        );
        assert!(inspection.artifact_fingerprint.is_some());
    }

    #[test]
    fn wave13_csr_io_rejects_parentless_destination_and_wrong_arrow_schema() {
        let empty = CsrIndex {
            offsets: vec![0],
            edge_ids: vec![],
            neighbor_ids: vec![],
        };
        assert!(write_csr(Path::new("/"), &empty).is_err());

        let root = TempDir::new().unwrap();
        let path = root.path().join("wrong-schema.arrow");
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            "wrong",
            DataType::UInt64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(UInt64Array::from(vec![1]))],
        )
        .unwrap();
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = FileWriter::try_new(file, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        assert!(read_csr(&path).is_err());
    }

    // -----------------------------------------------------------------------
    // Streaming / spill build (#336)
    // -----------------------------------------------------------------------

    pub(super) fn write_multi_row_group_knows(dir: &Path, edges: &[(u64, u64, u64)]) {
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;

        // Bootstrap project layout + generation via the writer, then replace the
        // typed edge file with a multi-row-group Parquet that still carries UUID
        // FixedSizeBinary columns (the concat hazard #336 removes).
        let mut w = GraphWriter::open_at(dir, OntologyMode::Strict, BUILD_TS).unwrap();
        let max_node = edges.iter().map(|&(s, _, d)| s.max(d)).max().unwrap_or(0);
        let mut node_uuids = Vec::new();
        for _ in 0..=max_node {
            node_uuids.push(new_v7());
            w.create_node(
                *node_uuids.last().unwrap(),
                graphforge_value::EntityTypeId::ontology(TypeId(0)).unwrap(),
            )
            .unwrap();
        }
        // At least one edge so flush creates topology/edges/.
        w.create_edge(
            new_v7(),
            "KNOWS",
            &node_uuids[0],
            &node_uuids[usize::try_from(max_node.min(1)).unwrap()],
        )
        .unwrap();
        w.flush().unwrap();

        let edges_path = dir.join("topology/edges/KNOWS.parquet");
        let schema = crate::schemas::TYPED_EDGE_SCHEMA.clone();
        let n = edges.len();
        let edge_uuid = arrow::array::FixedSizeBinaryArray::try_from_iter((0..n).map(|i| {
            let mut bytes = [0u8; 16];
            bytes[12..].copy_from_slice(&(i as u32).to_be_bytes());
            bytes
        }))
        .unwrap();
        let src_uuid =
            arrow::array::FixedSizeBinaryArray::try_from_iter((0..n).map(|_| [0u8; 16])).unwrap();
        let dst_uuid =
            arrow::array::FixedSizeBinaryArray::try_from_iter((0..n).map(|_| [1u8; 16])).unwrap();
        let edge_id = UInt64Array::from(edges.iter().map(|e| e.1).collect::<Vec<_>>());
        let src_id = UInt64Array::from(edges.iter().map(|e| e.0).collect::<Vec<_>>());
        let dst_id = UInt64Array::from(edges.iter().map(|e| e.2).collect::<Vec<_>>());
        let created = TimestampMicrosecondArray::from(vec![BUILD_TS; n]).with_timezone("UTC");
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(edge_uuid),
                Arc::new(src_uuid),
                Arc::new(dst_uuid),
                Arc::new(edge_id),
                Arc::new(src_id),
                Arc::new(dst_id),
                Arc::new(created),
            ],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(1))
            .build();
        let file = std::fs::File::create(&edges_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[test]
    fn streaming_reader_emits_multiple_batches_without_uuid_columns() {
        let dir = TempDir::new().unwrap();
        // 5 edges → 5 row groups with max_row_group_row_count=1.
        let edges = [(0, 1, 1), (0, 2, 2), (1, 3, 2), (2, 4, 3), (3, 5, 0)];
        write_multi_row_group_knows(dir.path(), &edges);
        let path = dir.path().join("topology/edges/KNOWS.parquet");

        let mut batches = 0usize;
        let mut rows = 0usize;
        let count = stream_projected_parquet_batches(
            &path,
            &["edge_id", "src_id", "dst_id"],
            /* batch_size */ 1,
            &mut |batch| {
                batches += 1;
                rows += batch.num_rows();
                for field in batch.schema().fields() {
                    assert!(
                        !matches!(field.data_type(), DataType::FixedSizeBinary(_)),
                        "UUID column {} must not be projected",
                        field.name()
                    );
                }
                assert!(batch.column_by_name("edge_uuid").is_none());
                assert!(batch.column_by_name("src_uuid").is_none());
                assert!(batch.column_by_name("dst_uuid").is_none());
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(count, batches);
        assert!(
            batches >= 5,
            "expected one batch per tiny row-group, got {batches}"
        );
        assert_eq!(rows, 5);
    }

    /// Deterministic seam: projected streaming never materializes a single
    /// FixedSizeBinary(16) buffer covering every edge. CI uses a tiny fixture;
    /// the ignored companion below documents the >134M boundary simulation.
    #[test]
    fn arrow_uuid_concat_boundary_is_avoided_by_projection() {
        let dir = TempDir::new().unwrap();
        let edges: Vec<(u64, u64, u64)> = (0..64).map(|i| (i % 8, i + 1, (i + 1) % 8)).collect();
        write_multi_row_group_knows(dir.path(), &edges);
        let path = dir.path().join("topology/edges/KNOWS.parquet");

        // Full-schema eager path (what the old builder did) would concat UUID
        // columns to `edges.len()` values. The streaming path must not.
        let full =
            crate::catalog::read_parquet_or_empty(&path, crate::schemas::TYPED_EDGE_SCHEMA.clone())
                .unwrap();
        assert_eq!(full.len(), 1, "legacy helper still concats to one batch");
        let uuid = full[0]
            .column_by_name("edge_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(uuid.len(), edges.len());

        let mut projected_rows = 0usize;
        stream_projected_parquet_batches(
            &path,
            &["edge_id", "src_id", "dst_id"],
            8,
            &mut |batch| {
                projected_rows += batch.num_rows();
                assert!(batch.column_by_name("edge_uuid").is_none());
                // Each projected batch stays well below the Arrow 2GiB UUID
                // buffer ceiling (134,217,728 FixedSizeBinary(16) values).
                assert!(batch.num_rows() <= 8);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(projected_rows, edges.len());

        let options = AdjacencyBuildOptions {
            chunk_rows: 4,
            batch_size: 8,
            ..AdjacencyBuildOptions::default()
        };
        build_adjacency_index_into_with_options(
            dir.path(),
            dir.path(),
            BUILD_TS,
            &options,
            &mut || Ok(()),
        )
        .unwrap();
        let expected = csr_from_entries(
            &edges.iter().map(|&(s, e, d)| (s, e, d)).collect::<Vec<_>>(),
            Direction::Out,
        );
        assert_eq!(
            read_csr(&csr_path(dir.path(), "KNOWS", Direction::Out)).unwrap(),
            expected
        );
    }

    /// Optional large-boundary simulation: tiny flush threshold stands in for
    /// the 134,217,728 FixedSizeBinary concat ceiling without allocating it.
    #[test]
    #[ignore = "manual/scale: exercises many spill runs; run explicitly for #336 evidence"]
    fn ignored_arrow_boundary_simulation_via_tiny_flush_threshold() {
        let dir = TempDir::new().unwrap();
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Strict, BUILD_TS).unwrap();
        let nodes: Vec<_> = (0..256).map(|_| new_v7()).collect();
        for u in &nodes {
            w.create_node(
                *u,
                graphforge_value::EntityTypeId::ontology(TypeId(0)).unwrap(),
            )
            .unwrap();
        }
        for i in 0..4_096 {
            let src = &nodes[i % nodes.len()];
            let dst = &nodes[(i * 7) % nodes.len()];
            w.create_edge(new_v7(), "KNOWS", src, dst).unwrap();
        }
        w.flush().unwrap();
        let options = AdjacencyBuildOptions {
            chunk_rows: 17, // awkward prime to stress merge
            batch_size: 13,
            spill_max_bytes: Some(64 * 1024 * 1024),
            ..AdjacencyBuildOptions::default()
        };
        build_adjacency_index_into_with_options(
            dir.path(),
            dir.path(),
            BUILD_TS,
            &options,
            &mut || Ok(()),
        )
        .unwrap();
        assert!(validate_adjacency_index(dir.path()).unwrap().is_empty());
    }
}
