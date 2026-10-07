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
//! row. Only the current versioned shard representation is supported.

mod builder;
mod codec;
mod installation;
pub use builder::{
    ADJACENCY_SPILL_DIR_NAME, AdjacencyBuildMetrics, AdjacencyBuildOptions,
    DEFAULT_ADJACENCY_CHUNK_ROWS, DEFAULT_ADJACENCY_MERGE_FAN_IN, build_adjacency_index,
    build_adjacency_index_from_inventory, build_adjacency_index_into,
    build_adjacency_index_into_with_metrics, build_adjacency_index_into_with_options,
    build_adjacency_index_with_checkpoint,
};
pub(crate) use builder::{
    build_adjacency_index_for_edge_files_observed, build_adjacency_index_for_edge_files_on_lanes,
};
use codec::{corrupt_index, corrupt_index_from, corrupt_structure, shard_io_error};
use installation::{
    csr_temporary_parent, observe_csr_barriers, persist_temp_observed, promote_shards,
    write_csr_shard_bytes_observed,
};

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
use graphforge_core::hash_observation::ArtifactSha256 as Sha256;
use serde::{Deserialize, Serialize};
use sha2::Digest;

use graphforge_core::GfError;

use crate::schemas::{ADJACENCY_CSR_SCHEMA, ADJACENCY_MANIFEST_SCHEMA, adjacency_entry_fields};
use crate::staging::RewriteBatch;

/// Reserved relation-type stem for the union-across-relation-types index
/// (`_all.out.csr`). Underscore-prefixed names cannot collide with declared
/// relation types (matching the `_exploratory.parquet` convention).
pub const ALL_RELATIONS_STEM: &str = "_all";

/// File name of the adjacency index manifest within `indexes/adjacency/`.
pub const MANIFEST_FILE: &str = "index_manifest.parquet";

const SHARDED_CSR_VERSION: u32 = 3;
/// Default maximum adjacency entries materialized in one persisted CSR shard.
pub const DEFAULT_CSR_SHARD_EDGES: usize = 1_048_576;
/// Default maximum local CSR rows (offset entries minus one) per shard.
pub const DEFAULT_CSR_SHARD_NODES: usize = 1_048_576;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CapturedAdjacencyArtifact {
    pub(crate) path: PathBuf,
    pub(crate) bytes: u64,
    pub(crate) sha256: String,
    pub(crate) xxh64: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct CsrShardRecord {
    first_node: u64,
    node_count: u64,
    edge_count: u64,
    file: String,
    sha256: String,
    #[serde(with = "crate::corruption_checksum::wire_hex")]
    xxh64: u64,
    encoded_bytes: u64,
    decoded_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CsrShardManifest {
    format: String,
    version: u32,
    node_count: u64,
    edge_count: u64,
    shard_dir: String,
    shards: Vec<CsrShardRecord>,
}

/// Default byte budget for decoded shards retained by one [`ShardedCsrIndex`].
/// Mirrors the 1 GiB `DEFAULT_CACHE_RELEASE_WINDOW_BYTES` operation scale: a
/// hard-capped shard decodes to at most `8*(N+1) + 16*E + bitmaps` bytes with
/// `N, E <= 1_048_576` (under 24 MiB), so the budget always retains dozens of
/// shards while never scaling with the graph (`#1094`).
pub const DEFAULT_DECODED_SHARD_CACHE_BYTES: u64 = 1024 * 1024 * 1024;

/// One decoded shard retained by [`DecodedShardCache`].
#[derive(Debug)]
struct CachedShard {
    file: String,
    csr: CsrIndex,
    decoded_bytes: u64,
    last_used: u64,
}

/// Byte-budgeted least-recently-used cache of decoded shards. Random-access
/// frontiers (for example a two-hop `ExpandExec` frontier in neighbour order)
/// otherwise re-decode one shard per row; retaining several decoded shards
/// bounds the decodes by the shard count while keeping reader memory bounded
/// by the configured budget, not by the graph (#1518). Entries survive until a
/// replacement fully authenticates, so a failed read never evicts the usable
/// state.
#[derive(Debug, Default)]
struct DecodedShardCache {
    entries: Vec<CachedShard>,
    retained_bytes: u64,
    budget_bytes: u64,
    tick: u64,
    decodes: u64,
}

impl DecodedShardCache {
    fn new(budget_bytes: u64) -> Self {
        Self {
            budget_bytes: budget_bytes.max(1),
            ..Self::default()
        }
    }

    fn position(&self, file: &str) -> Option<usize> {
        self.entries.iter().position(|entry| entry.file == file)
    }

    fn note_hit(&mut self, index: usize) {
        self.tick += 1;
        self.entries[index].last_used = self.tick;
    }

    /// Insert a fully authenticated shard, evicting least-recently-used
    /// entries first. A shard larger than the whole budget (impossible under
    /// the hard shard caps) still caches alone instead of being dropped, so
    /// the retained total never exceeds `max(budget, one shard)`.
    fn insert(&mut self, file: String, csr: CsrIndex, decoded_bytes: u64) {
        self.tick += 1;
        while !self.entries.is_empty() && self.retained_bytes + decoded_bytes > self.budget_bytes {
            let victim = self
                .entries
                .iter()
                .enumerate()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(index, _)| index)
                .expect("non-empty entries");
            let removed = self.entries.remove(victim);
            self.retained_bytes -= removed.decoded_bytes;
        }
        self.retained_bytes += decoded_bytes;
        self.entries.push(CachedShard {
            file,
            csr,
            decoded_bytes,
            last_used: self.tick,
        });
    }
}

/// Bounded reader for a versioned sharded CSR. Opening validates the small
/// manifest; row access reads and authenticates only the containing shard.
#[derive(Clone, Debug)]
pub struct ShardedCsrIndex {
    root: PathBuf,
    manifest: CsrShardManifest,
    // Decoded shards are retained least-recently-used within a byte budget:
    // sequential traversal still pays O(shards) decodes, while random-access
    // frontiers stop paying one decode per row (#1518). Reader memory stays
    // bounded by the budget, never by the graph (#1094).
    cache: std::sync::Arc<std::sync::Mutex<DecodedShardCache>>,
}

impl ShardedCsrIndex {
    /// Open the current shard manifest beside the logical `.csr` path.
    pub fn open(path: &Path) -> Result<Self, GfError> {
        let manifest_path = path.with_extension("csr.json");
        let bytes = codec::read_admitted_manifest(&manifest_path)?;
        crate::lifecycle_io::record_read(
            crate::StorageIoPhase::ReadPathScan,
            bytes.len() as u64,
            1,
        );
        let manifest = codec::decode_manifest(&bytes, &manifest_path)?;
        let mut prior_first = None;
        let mut edges = 0_u64;
        if !is_normal_path_component(&manifest.shard_dir) {
            return Err(corrupt_index("invalid CSR shard directory name"));
        }
        let root = path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(&manifest.shard_dir);
        for shard in &manifest.shards {
            if shard.node_count == 0
                || shard
                    .first_node
                    .checked_add(shard.node_count)
                    .is_none_or(|end| end > manifest.node_count)
                || prior_first.is_some_and(|prior| shard.first_node < prior)
            {
                return Err(corrupt_index("invalid CSR shard boundary ordering"));
            }
            prior_first = Some(shard.first_node);
            edges = edges
                .checked_add(shard.edge_count)
                .ok_or_else(|| corrupt_index("CSR manifest edge count overflow"))?;
            if shard.decoded_bytes
                != codec::decoded_bytes(shard.node_count, shard.edge_count)
                    .map_err(corrupt_structure)?
                || shard.encoded_bytes
                    > codec::encoded_limit(shard.node_count, shard.edge_count)
                        .map_err(corrupt_structure)?
                || shard.sha256.len() != 64
                || !shard
                    .sha256
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            {
                return Err(corrupt_index("CSR shard admission metadata disagrees"));
            }
            if !is_normal_path_component(&shard.file) {
                return Err(corrupt_index("invalid CSR shard file name"));
            }
            // Presence and length only; checksums stay on the first row touch so
            // opening a multi-shard index cannot charge O(E) RSS (#1094).
            let metadata = std::fs::metadata(root.join(&shard.file))
                .map_err(|error| shard_io_error(&shard.file, &error))?;
            if !metadata.is_file() || metadata.len() != shard.encoded_bytes {
                return Err(corrupt_index(format!(
                    "CSR shard {} length does not match its manifest",
                    shard.file
                )));
            }
        }
        if edges != manifest.edge_count {
            return Err(corrupt_index("CSR shard manifest counts disagree"));
        }
        Ok(Self {
            root,
            manifest,
            cache: std::sync::Arc::new(std::sync::Mutex::new(DecodedShardCache::new(
                DEFAULT_DECODED_SHARD_CACHE_BYTES,
            ))),
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
        if let Some(index) = cache.position(&record.file) {
            cache.note_hit(index);
            return Ok(map(cache.entries[index]
                .csr
                .row(node_id - record.first_node)));
        }
        // The decode holds the lock so concurrent readers cannot duplicate the
        // read-authenticate-decode work; retained entries survive a failed
        // decode unchanged.
        let path = self.root.join(&record.file);
        let csr = read_authenticated_shard(&path, record)?;
        let output = map(csr.row(node_id - record.first_node));
        cache.decodes += 1;
        cache.insert(record.file.clone(), csr, record.decoded_bytes);
        Ok(output)
    }

    /// Number of shard reads this index authenticated and decoded. Diagnostics
    /// for the bounded-decode guarantee of `#1518`: random-access frontiers
    /// must not drive this toward the frontier length.
    #[must_use]
    pub fn shard_decode_count(&self) -> u64 {
        self.cache
            .lock()
            .map(|cache| cache.decodes)
            .unwrap_or_default()
    }

    /// Number of shards in the opened manifest.
    #[must_use]
    pub fn shard_count(&self) -> usize {
        self.manifest.shards.len()
    }

    /// Decoded shard bytes currently retained by the byte-budgeted cache.
    #[must_use]
    pub fn retained_decoded_bytes(&self) -> u64 {
        self.cache
            .lock()
            .map(|cache| cache.retained_bytes)
            .unwrap_or_default()
    }
}

/// Whether the versioned sharded representation is published for `path`.
#[must_use]
pub fn sharded_csr_exists(path: &Path) -> bool {
    path.with_extension("csr.json").is_file()
}

fn csr_artifact_exists(path: &Path) -> bool {
    sharded_csr_exists(path)
}

fn is_normal_path_component(value: &str) -> bool {
    let mut components = Path::new(value).components();
    matches!(components.next(), Some(std::path::Component::Normal(_)))
        && components.next().is_none()
}

/// Write a versioned checksummed shard set and publish its manifest last.
/// This writer accepts an in-memory CSR; streaming builders use
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
    allocation: Option<crate::StorageAllocationOperation>,
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
            allocation: None,
            path: path.to_path_buf(),
            owned_root: Some(root.clone()),
            root,
            shard_dir,
            max_edges: max_edges.clamp(1, DEFAULT_CSR_SHARD_EDGES),
            max_nodes: max_nodes.clamp(1, DEFAULT_CSR_SHARD_NODES),
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
            self.allocation.as_ref(),
        )?);
        self.shard = CsrIndex {
            offsets: vec![0],
            ..CsrIndex::default()
        };
        self.first_node = None;
        self.last_key = None;
        Ok(())
    }

    fn finish(
        mut self,
        node_count: u64,
    ) -> Result<(u64, u64, u64, Vec<CapturedAdjacencyArtifact>), GfError> {
        use std::io::Write as _;

        self.flush()?;
        let digest = codec::shard_set_identity(node_count, self.edge_count, &self.records);
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
            self.remove_scratch_tree(&self.root)?;
            self.owned_root = None;
        } else {
            if stable_root.exists() {
                self.remove_scratch_tree(&stable_root)?;
            }
            promote_shards(&self.root, &stable_root)?;
            if let Some(allocation) = &self.allocation {
                for record in &self.records {
                    let source = self.root.join(&record.file);
                    let destination = stable_root.join(&record.file);
                    let file = std::fs::File::open(&destination).map_err(storage_err)?;
                    allocation.remove_file_at(&source)?;
                    allocation.replace_file_at(&destination, &file)?;
                }
            }
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
            .tempfile_in(csr_temporary_parent(parent)?)
            .map_err(storage_err)?;
        temp.write_all(&bytes).map_err(storage_err)?;
        // The CSR manifest barrier is attributed to its shard phase (#1449).
        crate::lifecycle_io::record_write(
            crate::StorageIoPhase::ReadPathScan,
            bytes.len() as u64,
            1,
        );
        if let Some(allocation) = &self.allocation {
            let temporary = parent.join(temp.path().file_name().expect("named CSR temporary"));
            allocation.replace_file_at(&temporary, temp.as_file())?;
        }
        persist_temp_observed(
            temp,
            &self.path.with_extension("csr.json"),
            self.allocation.as_ref(),
        )?;
        self.finished = true;
        Ok((
            manifest.shards.len() as u64,
            self.peak_shard_edges,
            self.peak_shard_nodes,
            manifest
                .shards
                .iter()
                .map(|record| CapturedAdjacencyArtifact {
                    path: self.root.join(&record.file),
                    bytes: record.encoded_bytes,
                    sha256: record.sha256.clone(),
                    xxh64: record.xxh64,
                })
                .collect(),
        ))
    }

    fn remove_scratch_tree(&self, root: &Path) -> Result<(), GfError> {
        match &self.allocation {
            Some(allocation) => allocation.remove_owned_tree(root),
            None => std::fs::remove_dir_all(root).map_err(storage_err),
        }
    }
}

impl Drop for ShardedCsrWriter {
    fn drop(&mut self) {
        if !self.finished
            && let Some(root) = self.owned_root.as_ref()
        {
            let _ = self.remove_scratch_tree(root);
        }
    }
}

/// Outcome of [`write_sharded_csr_from_sorted`].
pub(crate) struct SortedCsrOutcome {
    /// CSR rows: the largest key plus one, or zero without entries.
    pub(crate) node_count: u64,
    /// Adjacency entries written.
    pub(crate) edge_count: u64,
    /// Shards written.
    pub(crate) shards: u64,
    /// Digests of every shard, computed while it was written.
    pub(crate) captured: Vec<CapturedAdjacencyArtifact>,
}

/// Write one sharded CSR from entries already ordered by `(key, edge)`.
///
/// Each entry is `key << 32 | edge_id`; its neighbor is `neighbors[edge_id - 1]`.
/// Shard boundaries are exactly those [`ShardedCsrWriter::emit`] produces for
/// the same sequence, so the published bytes equal a streamed build's. Shards
/// are independent once the boundaries are fixed and are encoded in parallel
/// on the current rayon pool.
pub(crate) fn write_sharded_csr_from_sorted(
    path: &Path,
    sorted: &[u64],
    neighbors: &[u32],
    max_edges: usize,
    max_nodes: usize,
    allocation: Option<&crate::StorageAllocationOperation>,
) -> Result<SortedCsrOutcome, GfError> {
    use rayon::prelude::*;

    let mut writer = ShardedCsrWriter::create(path, max_edges, max_nodes)?;
    writer.allocation = allocation.cloned();
    let (max_edges, max_nodes) = (writer.max_edges, writer.max_nodes as u64);
    let key = |entry: u64| entry >> 32;
    let mut bounds = Vec::new();
    let mut start = 0;
    while start < sorted.len() {
        let first = key(sorted[start]);
        let by_nodes =
            start + sorted[start..].partition_point(|entry| key(*entry) - first < max_nodes);
        let end = by_nodes.min(start.saturating_add(max_edges));
        bounds.push((start, end));
        start = end;
    }
    let root = writer.root.clone();
    let records = bounds
        .par_iter()
        .enumerate()
        .map(|(ordinal, &(from, to))| {
            let slice = &sorted[from..to];
            let first = key(slice[0]);
            let local =
                usize::try_from(key(slice[slice.len() - 1]) - first + 1).map_err(storage_err)?;
            let mut offsets = vec![0_u64; local + 1];
            for entry in slice {
                offsets[usize::try_from(key(*entry) - first).map_err(storage_err)? + 1] += 1;
            }
            for row in 0..local {
                offsets[row + 1] += offsets[row];
            }
            let shard = CsrIndex {
                offsets,
                edge_ids: slice.iter().map(|entry| entry & 0xffff_ffff).collect(),
                neighbor_ids: slice
                    .iter()
                    .map(|entry| u64::from(neighbors[(entry & 0xffff_ffff) as usize - 1]))
                    .collect(),
            };
            write_csr_shard(&root, first, &shard, ordinal, allocation)
        })
        .collect::<Result<Vec<_>, GfError>>()?;
    writer.records = records;
    writer.edge_count = sorted.len() as u64;
    let node_count = sorted.last().map_or(0, |entry| key(*entry) + 1);
    let edge_count = writer.edge_count;
    let (shards, _, _, captured) = writer.finish(node_count)?;
    Ok(SortedCsrOutcome {
        node_count,
        edge_count,
        shards,
        captured,
    })
}

fn shard_set_matches(root: &Path, records: &[CsrShardRecord]) -> bool {
    for record in records {
        let path = root.join(&record.file);
        if read_authenticated_shard(&path, record).is_err() {
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
    allocation: Option<&crate::StorageAllocationOperation>,
) -> Result<CsrShardRecord, GfError> {
    let file = format!("{ordinal:020}.csr");
    let path = root.join(&file);
    // Hash-on-write (#1384, accepted decision 1): encode once, then verify and
    // hash the exact bytes that land on disk before writing them once. No pass
    // reads a shard back in order to hash it. The recorded digest still names
    // the shard, so the consuming boundaries keep their refusals: CAS install
    // re-derives the digest while copying, and shard reads preflight against
    // the manifest before Arrow decodes anything.
    let bytes = encode_csr_shard_bytes(shard)?;
    codec::admit_encoded_len(bytes.len() as u64, shard.node_count(), shard.edge_count())?;
    codec::preflight(&bytes, shard.node_count(), shard.edge_count())?;
    let encoded_bytes = bytes.len() as u64;
    crate::lifecycle_io::record_write(crate::StorageIoPhase::ReadPathScan, encoded_bytes, 1);
    write_csr_shard_bytes_observed(&path, &bytes, allocation)?;
    Ok(CsrShardRecord {
        first_node,
        node_count: shard.node_count(),
        edge_count: shard.edge_count(),
        file,
        sha256: sha256_hex(&bytes),
        xxh64: crate::corruption_checksum::checksum(&bytes),
        encoded_bytes,
        decoded_bytes: codec::decoded_bytes(shard.node_count(), shard.edge_count())?,
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
    /// Caller-supplied; excluded from the determinism guarantee. Zero denotes
    /// an unknown observation time for deterministic portable reconstruction;
    /// freshness is determined solely by `topology_generation`.
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
/// `indexes/adjacency/<route-component>.<dir>.csr`.
/// The manifest retains the exact semantic relation name.
#[must_use]
pub fn csr_path(project_dir: &Path, relation_type: &str, direction: Direction) -> PathBuf {
    adjacency_dir(project_dir).join(format!(
        "{}.{}.csr",
        crate::route_component::component(relation_type),
        direction.as_str()
    ))
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
/// Encode one bounded shard into the exact Arrow IPC bytes a shard file
/// carries. Shards are admission-bounded, so the buffer is bounded too.
fn encode_csr_shard_bytes(csr: &CsrIndex) -> Result<Vec<u8>, GfError> {
    csr.validate()?;
    codec::decoded_bytes(csr.node_count(), csr.edge_count())?;

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

    let options = arrow::ipc::writer::IpcWriteOptions::default()
        .try_with_compression(Some(arrow::ipc::CompressionType::ZSTD))
        .map_err(storage_err)?;
    let mut writer = FileWriter::try_new_with_options(
        std::io::Cursor::new(Vec::<u8>::new()),
        &ADJACENCY_CSR_SCHEMA,
        options,
    )
    .map_err(storage_err)?;
    writer.write(&batch).map_err(storage_err)?;
    writer.finish().map_err(storage_err)?;
    Ok(writer.into_inner().map_err(storage_err)?.into_inner())
}

#[cfg(test)]
fn write_csr_shard_bytes(path: &Path, bytes: &[u8]) -> Result<(), GfError> {
    write_csr_shard_bytes_observed(path, bytes, None)
}

/// Materialize a current sharded CSR for explicit validation/inspection.
/// Query row access uses [`ShardedCsrIndex`] and does not assemble the index.
///
/// # Errors
/// Refuses missing, unsupported or corrupt shards.
pub fn read_csr(path: &Path) -> Result<CsrIndex, GfError> {
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
    Ok(csr)
}

/// Read, authenticate and decode one shard against its manifest record. Absent,
/// mis-sized, checksum-failing or undecodable shards are corrupt (`GF_VALIDATION`),
/// since a correct writer never produces checksummed bytes that do not decode;
/// only an I/O failure other than "not found" is a storage error.
fn read_authenticated_shard(path: &Path, record: &CsrShardRecord) -> Result<CsrIndex, GfError> {
    let decoded =
        codec::decoded_bytes(record.node_count, record.edge_count).map_err(corrupt_structure)?;
    let limit =
        codec::encoded_limit(record.node_count, record.edge_count).map_err(corrupt_structure)?;
    if record.decoded_bytes != decoded {
        return Err(corrupt_index("CSR decoded-byte admission mismatch"));
    }
    let bytes = codec::read(path, &record.file, record.encoded_bytes, limit)?;
    crate::lifecycle_io::record_read(crate::StorageIoPhase::ReadPathScan, record.encoded_bytes, 1);
    if crate::corruption_checksum::checksum(&bytes) != record.xxh64 {
        return Err(corrupt_index(format!(
            "CSR shard checksum mismatch: {}",
            record.file
        )));
    }
    codec::preflight(&bytes, record.node_count, record.edge_count).map_err(corrupt_structure)?;
    let reader = FileReader::try_new(std::io::Cursor::new(&bytes), None)
        .map_err(|error| corrupt_index(error.to_string()))?;
    let csr = decode_csr(reader, path, record.node_count, record.edge_count)
        .map_err(corrupt_structure)?;
    if csr.node_count() != record.node_count || csr.edge_count() != record.edge_count {
        return Err(corrupt_index("CSR shard count mismatch"));
    }
    Ok(csr)
}

fn decode_csr<R: std::io::Read + std::io::Seek>(
    reader: FileReader<R>,
    path: &Path,
    admitted_nodes: u64,
    admitted_edges: u64,
) -> Result<CsrIndex, GfError> {
    if reader.schema().fields() != ADJACENCY_CSR_SCHEMA.fields() {
        return Err(GfError::Storage(format!(
            "CSR file {} has unexpected schema {:?}",
            path.display(),
            reader.schema()
        )));
    }

    let mut csr = CsrIndex {
        offsets: Vec::with_capacity(usize::try_from(admitted_nodes + 1).map_err(storage_err)?),
        edge_ids: Vec::with_capacity(usize::try_from(admitted_edges).map_err(storage_err)?),
        neighbor_ids: Vec::with_capacity(usize::try_from(admitted_edges).map_err(storage_err)?),
    };
    csr.offsets.push(0);
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
        if adjacency.null_count() != 0
            || entries.null_count() != 0
            || edge_ids.null_count() != 0
            || neighbor_ids.null_count() != 0
            || value_offsets.first() != Some(&0)
            || value_offsets.last().copied() != i64::try_from(edge_ids.len()).ok()
            || value_offsets.windows(2).any(|pair| pair[0] > pair[1])
        {
            return Err(GfError::Storage(
                "invalid CSR offsets or null values".into(),
            ));
        }
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
    write_manifest_observed(project_dir, rows, None)
}

pub(crate) fn write_manifest_observed(
    project_dir: &Path,
    rows: &[AdjacencyManifestRow],
    allocation: Option<&crate::StorageAllocationOperation>,
) -> Result<(), GfError> {
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
    observe_csr_barriers(|| staged.commit_retained_at_observed(project_dir, allocation))?;
    crate::lifecycle_io::record_write(
        crate::StorageIoPhase::ReadPathScan,
        std::fs::metadata(manifest_path(project_dir))
            .map_err(storage_err)?
            .len(),
        1,
    );
    Ok(())
}

/// Read `index_manifest.parquet`. An absent manifest (or absent
/// `indexes/adjacency/` directory) returns `Ok(vec![])` — the index simply
/// has not been built, mirroring the absent-file semantics of the catalog
/// readers.
///
/// # Errors
/// [`GfError::Validation`] if it fails admission or does not decode;
/// [`GfError::Storage`] on I/O failure or a schema other than [`ADJACENCY_MANIFEST_SCHEMA`].
pub fn read_manifest(project_dir: &Path) -> Result<Vec<AdjacencyManifestRow>, GfError> {
    let path = manifest_path(project_dir);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let file = std::fs::File::open(&path).map_err(storage_err)?;
    // Admitted (a flipped byte is refused), then decoded: it is only ever
    // published whole, so one that does not decode is damaged.
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        crate::lifecycle_io::ReadPathFile::admitted(file)?,
    )
    .map_err(corrupt_index_from)?
    .build()
    .map_err(corrupt_index_from)?;

    let mut rows = Vec::new();
    for batch in reader {
        let batch = batch.map_err(corrupt_index_from)?;
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
                direction: Direction::parse(directions.value(i)).map_err(corrupt_structure)?,
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
fn capture_adjacency_inventory(
    root: &Path,
) -> Result<crate::AuthenticatedPropertyInventory, GfError> {
    crate::AuthenticatedPropertyInventory::capture(root)
}

/// The `(relation, path)` edge tables an adjacency build streams, resolved
/// through the admitted inventory when one is supplied, else the legacy raw
/// `topology/edges/` layout.
pub(crate) fn resolve_adjacency_edge_files(
    project_dir: &Path,
    inventory: Option<&crate::AuthenticatedPropertyInventory>,
) -> Result<Vec<(String, PathBuf)>, GfError> {
    match inventory {
        Some(inventory) => Ok(inventory.edge_files(None)),
        None => crate::mutator::edge_parquet_files(project_dir, None),
    }
}

fn for_each_adjacency_edge_file(
    project_dir: &Path,
    inventory: Option<&crate::AuthenticatedPropertyInventory>,
    batch_size: usize,
    on_batch: &mut dyn FnMut(&str, bool, &RecordBatch) -> Result<(), GfError>,
) -> Result<(), GfError> {
    let paths = resolve_adjacency_edge_files(project_dir, inventory)?;
    for_each_adjacency_edge_path(&paths, batch_size, on_batch)
}

/// Stream every `(relation, path)` edge table in `paths` as projected batches.
pub(crate) fn for_each_adjacency_edge_path(
    paths: &[(String, PathBuf)],
    batch_size: usize,
    on_batch: &mut dyn FnMut(&str, bool, &RecordBatch) -> Result<(), GfError>,
) -> Result<(), GfError> {
    for (stem, path) in paths {
        // An unreadable edge file must FAIL the build, not be skipped: a
        // manifest written without it would stamp the current generation and
        // make an index missing a relation's edges look fresh.
        let _schema = match crate::catalog::discover_parquet_schema_detailed(path) {
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
        stream_projected_parquet_batches(path, columns, batch_size, &mut |batch| {
            on_batch(stem, exploratory, &batch)
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
    let file = std::fs::File::open(path).map_err(storage_err)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(
        crate::lifecycle_io::ReadPathFile::admitted(file)?,
    )
    .map_err(storage_err)?;
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
    inventory: Option<&crate::AuthenticatedPropertyInventory>,
) -> Result<
    (
        std::collections::BTreeMap<String, Vec<BuildEntry>>,
        Vec<BuildEntry>,
    ),
    GfError,
> {
    collect_adjacency_groups_with_batch_size(project_dir, inventory, DEFAULT_ADJACENCY_BATCH_SIZE)
}

#[allow(clippy::type_complexity)]
fn collect_adjacency_groups_with_batch_size(
    project_dir: &Path,
    inventory: Option<&crate::AuthenticatedPropertyInventory>,
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
    for_each_adjacency_edge_file(
        project_dir,
        inventory,
        batch_size,
        &mut |stem, exploratory, batch| {
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
        },
    )?;
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
    let inventory = capture_adjacency_inventory(source_project_dir)?;
    validate_adjacency_index_from_inventory(
        source_project_dir,
        artifact_project_dir,
        Some(&inventory),
    )
}

/// Validate derived adjacency against explicitly admitted semantic routes.
pub fn validate_adjacency_index_from_inventory(
    source_project_dir: &Path,
    artifact_project_dir: &Path,
    inventory: Option<&crate::AuthenticatedPropertyInventory>,
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

    let (groups, union_out) = collect_adjacency_groups(source_project_dir, inventory)?;
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
    let inventory = capture_adjacency_inventory(project_dir)?;
    inspect_adjacency_index_from_inventory(project_dir, Some(&inventory))
}

/// Inspect derived adjacency against explicitly admitted semantic routes.
pub fn inspect_adjacency_index_from_inventory(
    project_dir: &Path,
    inventory: Option<&crate::AuthenticatedPropertyInventory>,
) -> Result<AdjacencyInspection, GfError> {
    let source_generation = crate::generation::read_topology_generation(project_dir)?;
    let (_, mut source_entries) = collect_adjacency_groups(project_dir, inventory)?;
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
    let mut digest = graphforge_core::hash_observation::ContractSha256::new();
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
    !rel.is_empty() && rel != "." && rel != ".." && !rel.contains('/')
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
mod capture_tests;
#[cfg(test)]
mod classification_tests;

#[cfg(test)]
mod tests;
