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
//! ├── WORKS_AT.out.csr          ADJACENCY_CSR_SCHEMA (Arrow IPC)
//! ├── WORKS_AT.in.csr
//! └── _all.out.csr              union across relation types
//! ```
//!
//! # Build ordering convention
//!
//! Builders MUST write all CSR files first and `index_manifest.parquet`
//! **last**. A crash mid-build then leaves the manifest absent or carrying the
//! old `topology_generation`, so the index reads as stale and the provider
//! falls back to scan-and-build — a torn build can cost a rebuild, never
//! correctness.
//!
//! # CSR encoding
//!
//! A `.csr` file is a single-batch Arrow IPC file with one column,
//! `adjacency: LargeList<Struct{edge_id, neighbor_id}>` and one row per
//! surrogate `node_id` in `0..node_count`. The list offsets buffer is the CSR
//! offsets array; the struct child is the targets array. See
//! [`ADJACENCY_CSR_SCHEMA`] and `docs/book/architecture/storage.md` §Derived
//! Indexes. The in-memory [`CsrIndex`] exposes the logical `offsets`/`targets`
//! model directly, so consumers never deal with the list encoding.

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
    persist_temp(tmp, path)
}

/// Read a CSR file written by [`write_csr`] back into a [`CsrIndex`].
///
/// # Errors
/// Returns [`GfError::Storage`] if the file is missing, is not an Arrow IPC
/// file with [`ADJACENCY_CSR_SCHEMA`], or decodes to an invalid CSR.
pub fn read_csr(path: &Path) -> Result<CsrIndex, GfError> {
    let file = File::open(path)
        .map_err(|e| GfError::Storage(format!("cannot open CSR file {}: {e}", path.display())))?;
    let reader = FileReader::try_new(file, None)
        .map_err(|e| GfError::Storage(format!("invalid CSR file {}: {e}", path.display())))?;
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
    staged.commit()
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
// Index builder (#761)
// ---------------------------------------------------------------------------

/// One edge occurrence during the index build: `(src_id, edge_id, dst_id)`.
/// [`csr_from_entries`] re-keys per direction (`src` for `out`, `dst` for `in`).
pub(crate) type BuildEntry = (u64, u64, u64);

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
/// manifest is only written after every CSR file succeeded.
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
    checkpoint()?;
    // Generation BEFORE the scan — see the race note in the doc comment.
    let generation = crate::generation::read_topology_generation(source_project_dir)?;
    let (groups, union_out) = collect_adjacency_groups(source_project_dir)?;

    let adjacency = adjacency_dir(artifact_project_dir);
    std::fs::create_dir_all(&adjacency).map_err(storage_err)?;

    let mut manifest = Vec::new();
    {
        let mut write_pair = |stem: &str, entries: &[BuildEntry]| -> Result<(), GfError> {
            for direction in [Direction::Out, Direction::In] {
                checkpoint()?;
                let csr = csr_from_entries(entries, direction);
                write_csr(&csr_path(artifact_project_dir, stem, direction), &csr)?;
                manifest.push(AdjacencyManifestRow {
                    relation_type: stem.to_owned(),
                    direction,
                    topology_generation: generation,
                    built_at_micros,
                    node_count: csr.node_count(),
                    edge_count: csr.edge_count(),
                });
            }
            Ok(())
        };
        for (stem, entries) in &groups {
            write_pair(stem, entries)?;
        }
        write_pair(ALL_RELATIONS_STEM, &union_out)?;
    }
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
    Ok(manifest)
}

/// Scan `topology/edges/` and group every edge occurrence by relation type:
/// per-relation entries (stems unusable as file names are skipped, see
/// [`build_adjacency_index`]) plus the full union. Shared by the builder and
/// the validator so "what the index SHOULD contain" has exactly one
/// definition.
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
    use std::collections::BTreeMap;

    let mut groups: BTreeMap<String, Vec<BuildEntry>> = BTreeMap::new();
    let mut union_out: Vec<BuildEntry> = Vec::new();
    for path in crate::mutator::parquet_files_in(project_dir, "topology/edges")? {
        let Some(stem) = path.file_stem().and_then(|s| s.to_str()).map(str::to_owned) else {
            continue;
        };
        // An unreadable edge file must FAIL the build, not be skipped: a
        // manifest written without it would stamp the current generation and
        // make an index missing a relation's edges look fresh.
        let schema = crate::catalog::discover_parquet_schema(&path).ok_or_else(|| {
            GfError::Storage(format!(
                "adjacency build: cannot read parquet schema for {}",
                path.display()
            ))
        })?;
        let batches = crate::catalog::read_parquet_or_empty(&path, schema).map_err(storage_err)?;
        let exploratory = stem == "_exploratory";
        for batch in &batches {
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
                let rel = rel_names.map_or(stem.as_str(), |names| names.value(i));
                if usable_stem(rel) {
                    groups.entry(rel.to_owned()).or_default().push(entry);
                }
            }
        }
    }
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
        if !path.exists() {
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
    if !union_path.exists() {
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
    format!("sha256:{:x}", digest.finalize())
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
        staged.commit().unwrap();

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
    use graphforge_core::uuid::{Uuid, new_v7, to_bytes};

    /// Fixed timestamp for deterministic fixtures and manifests.
    const BUILD_TS: i64 = 1_700_000_000_000_000;

    /// Strict-mode diamond a->b, a->c, b->d, c->d plus a parallel a->b and a
    /// self-loop d->d, all KNOWS. Returns the surrogate node ids.
    fn write_diamond(dir: &Path) -> [u64; 4] {
        let mut w = GraphWriter::open_at(dir, OntologyMode::Strict, BUILD_TS).unwrap();
        let uuids: Vec<Uuid> = (0..4).map(|_| new_v7()).collect();
        let ids: Vec<u64> = uuids
            .iter()
            .map(|u| w.create_node(*u, TypeId(0)).unwrap())
            .collect();
        let (a, b, c, d) = (&uuids[0], &uuids[1], &uuids[2], &uuids[3]);
        for (src, dst) in [(a, b), (a, c), (b, d), (c, d), (a, b), (d, d)] {
            w.create_edge(new_v7(), "KNOWS", src, dst).unwrap();
        }
        w.flush().unwrap();
        [ids[0], ids[1], ids[2], ids[3]]
    }

    #[test]
    fn build_is_deterministic_and_stamps_pre_scan_generation() {
        let dir = TempDir::new().unwrap();
        write_diamond(dir.path()); // flush -> generation 1

        let rows = build_adjacency_index(dir.path(), BUILD_TS).unwrap();
        assert!(rows.iter().all(|r| r.topology_generation == 1));
        // KNOWS out/in + _all out/in.
        assert_eq!(rows.len(), 4);

        let knows_out = std::fs::read(csr_path(dir.path(), "KNOWS", Direction::Out)).unwrap();
        let all_in =
            std::fs::read(csr_path(dir.path(), ALL_RELATIONS_STEM, Direction::In)).unwrap();

        // Rebuild: byte-identical CSR files (R-ADJ-2).
        build_adjacency_index(dir.path(), BUILD_TS).unwrap();
        assert_eq!(
            std::fs::read(csr_path(dir.path(), "KNOWS", Direction::Out)).unwrap(),
            knows_out
        );
        assert_eq!(
            std::fs::read(csr_path(dir.path(), ALL_RELATIONS_STEM, Direction::In)).unwrap(),
            all_in
        );

        // Per-node entries are (key, edge_id)-sorted == edge-file row order.
        let csr = read_csr(&csr_path(dir.path(), "KNOWS", Direction::Out)).unwrap();
        let knows_manifest = read_manifest(dir.path())
            .unwrap()
            .into_iter()
            .find(|r| r.relation_type == "KNOWS" && r.direction == Direction::Out)
            .unwrap();
        assert_eq!(knows_manifest.node_count, csr.node_count());
        assert_eq!(knows_manifest.edge_count, 6);
        let windows: Vec<&[u64]> = csr
            .offsets
            .windows(2)
            .map(|w| &csr.edge_ids[w[0] as usize..w[1] as usize])
            .collect();
        for per_node in windows {
            assert!(
                per_node.windows(2).all(|w| w[0] <= w[1]),
                "edge ids ascending per node"
            );
        }
    }

    #[test]
    fn private_staging_never_changes_the_reader_visible_artifact() {
        let dir = TempDir::new().unwrap();
        write_diamond(dir.path());
        build_adjacency_index(dir.path(), BUILD_TS).unwrap();
        let prior_manifest = std::fs::read(manifest_path(dir.path())).unwrap();
        let stage = TempDir::new_in(dir.path().parent().unwrap()).unwrap();
        let mut checkpoints = 0;

        build_adjacency_index_into(dir.path(), stage.path(), BUILD_TS + 1, || {
            checkpoints += 1;
            assert_eq!(
                std::fs::read(manifest_path(dir.path())).unwrap(),
                prior_manifest
            );
            assert_eq!(
                inspect_adjacency_index(dir.path()).unwrap().state,
                AdjacencyFreshnessState::Current
            );
            Ok(())
        })
        .unwrap();
        assert!(checkpoints >= 4);
        assert!(
            validate_adjacency_index_against(dir.path(), stage.path())
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn post_delete_sparse_ids_build_round_trips() {
        let dir = TempDir::new().unwrap();
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Strict, BUILD_TS).unwrap();
        let uuids: Vec<Uuid> = (0..4).map(|_| new_v7()).collect();
        let ids: Vec<u64> = uuids
            .iter()
            .map(|u| w.create_node(*u, TypeId(0)).unwrap())
            .collect();
        for pair in uuids.windows(2) {
            w.create_edge(new_v7(), "KNOWS", &pair[0], &pair[1])
                .unwrap();
        }
        w.flush().unwrap();

        // DETACH-DELETE the middle node n2: its id becomes a gap.
        let node_set: std::collections::HashSet<[u8; 16]> =
            std::iter::once(to_bytes(&uuids[1])).collect();
        let incident = crate::incident_edge_uuids(dir.path(), &node_set).unwrap();
        let edge_set: std::collections::HashSet<[u8; 16]> = incident.into_iter().collect();
        crate::delete_nodes_and_edges(dir.path(), &node_set, &edge_set).unwrap();

        build_adjacency_index(dir.path(), BUILD_TS).unwrap();
        let csr = read_csr(&csr_path(dir.path(), "KNOWS", Direction::Out)).unwrap();
        let gap = usize::try_from(ids[1]).unwrap();
        assert_eq!(
            csr.offsets[gap],
            csr.offsets[gap + 1],
            "deleted id is an empty range"
        );
        // Survivor n3 -> n4 intact.
        let n3 = usize::try_from(ids[2]).unwrap();
        let (s, e) = (csr.offsets[n3] as usize, csr.offsets[n3 + 1] as usize);
        assert_eq!(&csr.neighbor_ids[s..e], &[ids[3]]);
    }

    #[test]
    fn exploratory_rows_group_by_rel_type_and_union_covers_all() {
        let dir = TempDir::new().unwrap();
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, BUILD_TS).unwrap();
        let (a, b, c) = (new_v7(), new_v7(), new_v7());
        let ids: Vec<u64> = [a, b, c]
            .iter()
            .map(|u| w.create_node(*u, TypeId(0)).unwrap())
            .collect();
        w.create_edge(new_v7(), "KNOWS", &a, &b).unwrap();
        w.create_edge(new_v7(), "OWNS", &a, &c).unwrap();
        w.flush().unwrap();

        build_adjacency_index(dir.path(), BUILD_TS).unwrap();

        let knows = read_csr(&csr_path(dir.path(), "KNOWS", Direction::Out)).unwrap();
        assert_eq!(knows.edge_count(), 1, "decoy OWNS row excluded");
        let owns = read_csr(&csr_path(dir.path(), "OWNS", Direction::Out)).unwrap();
        assert_eq!(owns.edge_count(), 1);

        let all = read_csr(&csr_path(dir.path(), ALL_RELATIONS_STEM, Direction::Out)).unwrap();
        assert_eq!(all.edge_count(), 2, "union covers both rel types");
        let row = usize::try_from(ids[0]).unwrap();
        let (s, e) = (all.offsets[row] as usize, all.offsets[row + 1] as usize);
        assert_eq!(&all.edge_ids[s..e], &[1, 2], "union in edge_id order");
        assert_eq!(&all.neighbor_ids[s..e], &[ids[1], ids[2]]);
    }

    #[test]
    fn hostile_and_reserved_stems_are_skipped_but_counted_in_union() {
        let dir = TempDir::new().unwrap();
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, BUILD_TS).unwrap();
        let (a, b, c) = (new_v7(), new_v7(), new_v7());
        for u in [a, b, c] {
            w.create_node(u, TypeId(0)).unwrap();
        }
        w.create_edge(new_v7(), "a/b", &a, &b).unwrap();
        w.create_edge(new_v7(), ALL_RELATIONS_STEM, &a, &c).unwrap();
        w.flush().unwrap();

        let rows = build_adjacency_index(dir.path(), BUILD_TS).unwrap();
        // Only the union pair: both rel names are unusable as stems.
        assert!(rows.iter().all(|r| r.relation_type == ALL_RELATIONS_STEM));
        assert_eq!(rows.len(), 2);
        let all = read_csr(&csr_path(dir.path(), ALL_RELATIONS_STEM, Direction::Out)).unwrap();
        assert_eq!(
            all.edge_count(),
            2,
            "skipped rels still flow into the union"
        );
        assert!(
            !csr_path(dir.path(), "a/b", Direction::Out).exists(),
            "no nested path written for the separator-bearing rel name"
        );
        assert!(!csr_path(dir.path(), "a", Direction::Out).exists());
    }

    #[test]
    fn build_failure_leaves_no_manifest() {
        let dir = TempDir::new().unwrap();
        write_diamond(dir.path());
        // Pre-create indexes/adjacency as a READ-ONLY dir so write_csr fails.
        let adj = adjacency_dir(dir.path());
        std::fs::create_dir_all(&adj).unwrap();
        let mut perms = std::fs::metadata(&adj).unwrap().permissions();
        perms.set_readonly(true);
        std::fs::set_permissions(&adj, perms.clone()).unwrap();

        let result = build_adjacency_index(dir.path(), BUILD_TS);
        // Restore writability so TempDir cleanup succeeds before asserting.
        perms.set_readonly(false);
        std::fs::set_permissions(&adj, perms).unwrap();

        assert!(result.is_err());
        assert!(!manifest_path(dir.path()).exists(), "manifest written last");
        assert_eq!(read_manifest(dir.path()).unwrap(), Vec::new());
    }

    #[test]
    fn empty_project_builds_union_pair_and_manifest() {
        let dir = TempDir::new().unwrap();
        let rows = build_adjacency_index(dir.path(), BUILD_TS).unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|r| {
            r.relation_type == ALL_RELATIONS_STEM
                && r.topology_generation == 0
                && r.node_count == 0
                && r.edge_count == 0
        }));
        let all = read_csr(&csr_path(dir.path(), ALL_RELATIONS_STEM, Direction::Out)).unwrap();
        assert_eq!(all.offsets, vec![0]);
        assert_eq!(read_manifest(dir.path()).unwrap(), rows);
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
        std::fs::write(csr_path(dir.path(), "KNOWS", Direction::In), b"garbage").unwrap();

        let inspection = inspect_adjacency_index(dir.path()).unwrap();
        assert_eq!(inspection.state, AdjacencyFreshnessState::Incompatible);
        assert_eq!(
            inspection.reason,
            Some(AdjacencyFreshnessReason::UnreadableArtifact)
        );
    }

    #[test]
    fn inspection_accepts_only_a_complete_delta_chain_as_current() {
        let dir = TempDir::new().unwrap();
        write_diamond(dir.path());
        build_adjacency_index(dir.path(), BUILD_TS).unwrap();
        crate::generation::bump_topology_generation(dir.path()).unwrap();

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
        std::fs::write(csr_path(dir.path(), "KNOWS", Direction::In), b"garbage").unwrap();

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
        std::fs::remove_file(csr_path(dir.path(), ALL_RELATIONS_STEM, Direction::Out)).unwrap();

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
        crate::generation::bump_topology_generation(dir.path()).unwrap();

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
        w.create_node(a, TypeId(0)).unwrap();
        w.create_node(b, TypeId(0)).unwrap();
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
        w.create_node(a, TypeId(0)).unwrap();
        w.create_node(b, TypeId(0)).unwrap();
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
}
