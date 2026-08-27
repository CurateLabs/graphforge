//! The single adjacency abstraction (ADR 0005, R-ADJ-4) — `AdjacencyProvider`.
//!
//! Both adjacency consumers obtain their view through this trait: the Cypher
//! variable-length traversal (`VarLenExpandExec`, #762) and the analyst-verb
//! `export_adjacency` adapter (algorithm #610). After #762 there is exactly **one**
//! adjacency implementation in the codebase.
//!
//! Two implementations exist:
//!
//! - [`ScanBuildAdjacencyProvider`] — reads the typed edge tables and builds
//!   the view in memory on every call (the behavior of the retired private
//!   `build_adjacency`); retained as an explicit oracle/foreign-session provider.
//! - [`PersistentAdjacencyProvider`] (#761) — serves from the on-disk CSR
//!   index under `indexes/adjacency/` when it is fresh (manifest
//!   `topology_generation` matches the project counter), lazily rebuilds a
//!   stale, corrupt, incomplete, or missing index with the bounded external-sort
//!   builder, and fails closed if that reconstruction fails. The project facade
//!   never falls back to the O(E)-memory oracle. The adjacency-aware lowering
//!   rule is #763.
//!
//! Surrogate-only (R-ADJ-3): the view holds `node_id` / `edge_id` `u64`
//! surrogates exclusively; UUIDs are resolved at the API boundary, never here.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use arrow::record_batch::RecordBatch;
use graphforge_core::{GfError, OntologyMode};
use graphforge_ir::Direction;
use graphforge_storage::adjacency::{
    self as csr, ALL_RELATIONS_STEM, AdjacencyManifestRow, AdjacencySourceIdentity, CsrIndex,
    CsrRow, ShardedCsrIndex, adjacency_relation_key, is_adjacency_relation_key,
};
use graphforge_storage::adjacency_delta::{
    CsrDeltaOverlay, DeltaSegment, overlay_delta_segments, read_delta_chain,
};
use graphforge_storage::generation::read_topology_generation;

use crate::ValueAt;

/// How an adjacency request is (or would be) served — surfaced in explain
/// output (#762) and consulted by the lowering rule (#763).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdjacencyStatus {
    /// Served from a fresh on-disk CSR index.
    Hit,
    /// The index capability is present but could not serve this key fresh:
    /// stale or corrupt manifest/counter, a fresh index with no row for the
    /// relation, or a missing CSR file. The persistent provider rebuilds it.
    Miss,
    /// No index capability currently exists: `indexes/adjacency/` is absent,
    /// or this is an explicit scan-build-only provider. Persistent execution
    /// builds the capability on first use.
    Building,
}

impl AdjacencyStatus {
    /// The explain-output token: `"hit"` | `"miss"` | `"building"`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Miss => "miss",
            Self::Building => "building",
        }
    }
}

/// How a view is physically backed — used for structural assertions (#340).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdjacencyBacking {
    /// Scan-built `HashMap` of per-node vectors (fallback / oracle path).
    ScanHashMap,
    /// Directed CSR served with O(1) row lookup; no O(E) expansion.
    CsrNative,
    /// Directed CSR plus a bounded delta-key overlay; base CSR not copied.
    CsrOverlay,
    /// Out+in CSR pair merged per row on access (no full merged hash map).
    CsrUndirected,
}

/// Surrogate-keyed adjacency view consumed by traversal (#340).
///
/// Persisted-index hits keep the validated CSR (and optional bounded overlay)
/// without expanding into `HashMap<u64, Vec<_>>`. Scan-build fallback retains
/// the historical map representation for oracle parity.
#[derive(Clone, Debug, Default)]
pub struct Adjacency {
    inner: AdjacencyInner,
}

#[derive(Clone, Debug, Default)]
enum AdjacencyInner {
    #[default]
    Empty,
    Map(HashMap<u64, Vec<(u64, u64)>>),
    Csr(Arc<CsrIndex>),
    Sharded(Arc<ShardedCsrIndex>),
    ShardedOverlay {
        base: Arc<ShardedCsrIndex>,
        replaced: HashMap<u64, Vec<(u64, u64)>>,
        node_extent: u64,
    },
    Overlay(CsrDeltaOverlay),
    Undirected {
        out: Arc<AdjacencyInner>,
        inbound: Arc<AdjacencyInner>,
    },
}

type ShardedReplacementRows = HashMap<u64, Vec<(u64, u64)>>;

/// Borrowed or owned neighbor row for one node.
#[derive(Clone, Debug)]
pub struct NeighborRow<'a> {
    kind: NeighborRowKind<'a>,
}

#[derive(Clone, Debug)]
enum NeighborRowKind<'a> {
    Pairs(&'a [(u64, u64)]),
    Csr(CsrRow<'a>),
    Owned(Vec<(u64, u64)>),
}

impl<'a> NeighborRow<'a> {
    fn pairs(entries: &'a [(u64, u64)]) -> Self {
        Self {
            kind: NeighborRowKind::Pairs(entries),
        }
    }

    fn csr(row: CsrRow<'a>) -> Self {
        Self {
            kind: NeighborRowKind::Csr(row),
        }
    }

    fn owned(entries: Vec<(u64, u64)>) -> Self {
        Self {
            kind: NeighborRowKind::Owned(entries),
        }
    }

    /// Number of `(edge_id, neighbor_id)` entries.
    #[must_use]
    pub fn len(&self) -> usize {
        match &self.kind {
            NeighborRowKind::Pairs(entries) => entries.len(),
            NeighborRowKind::Csr(row) => row.len(),
            NeighborRowKind::Owned(entries) => entries.len(),
        }
    }

    /// Whether the row is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Entry at `index`, if present.
    #[must_use]
    pub fn get(&self, index: usize) -> Option<(u64, u64)> {
        match &self.kind {
            NeighborRowKind::Pairs(entries) => entries.get(index).copied(),
            NeighborRowKind::Csr(row) => row.get(index),
            NeighborRowKind::Owned(entries) => entries.get(index).copied(),
        }
    }

    /// Materialize entries (tests and rare owned-row consumers).
    #[must_use]
    pub fn to_vec(&self) -> Vec<(u64, u64)> {
        match &self.kind {
            NeighborRowKind::Pairs(entries) => entries.to_vec(),
            NeighborRowKind::Csr(row) => row.iter().collect(),
            NeighborRowKind::Owned(entries) => entries.clone(),
        }
    }

    /// Iterate `(edge_id, neighbor_id)` pairs.
    #[must_use]
    pub fn iter(&self) -> NeighborRowIter<'_> {
        NeighborRowIter {
            row: self,
            index: 0,
        }
    }
}

impl PartialEq<[(u64, u64)]> for NeighborRow<'_> {
    fn eq(&self, other: &[(u64, u64)]) -> bool {
        self.len() == other.len() && (0..self.len()).all(|i| self.get(i) == Some(other[i]))
    }
}

impl PartialEq<&[(u64, u64)]> for NeighborRow<'_> {
    fn eq(&self, other: &&[(u64, u64)]) -> bool {
        self == *other
    }
}

/// Iterator over [`NeighborRow`] entries.
pub struct NeighborRowIter<'a> {
    row: &'a NeighborRow<'a>,
    index: usize,
}

impl Iterator for NeighborRowIter<'_> {
    type Item = (u64, u64);

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.row.get(self.index)?;
        self.index += 1;
        Some(item)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.row.len().saturating_sub(self.index);
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for NeighborRowIter<'_> {}

impl<'a> IntoIterator for &'a NeighborRow<'a> {
    type Item = (u64, u64);
    type IntoIter = NeighborRowIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl Adjacency {
    /// The `(edge_id, neighbor_node_id)` entries for `node_id`, in edge-file
    /// row order (for `Undirected`, each edge row contributes its src-keyed
    /// and dst-keyed entries in that order). BFS emission order observably
    /// depends on this ordering.
    ///
    /// Unknown, isolated, or deleted ids yield an empty row — never a panic.
    #[must_use]
    pub fn neighbors(&self, node_id: u64) -> NeighborRow<'_> {
        self.inner.neighbors(node_id)
    }

    /// Physical backing representation (#340 structural counter surface).
    #[must_use]
    pub fn backing(&self) -> AdjacencyBacking {
        self.inner.backing()
    }

    /// Number of base-CSR adjacency entries that were expanded into a hash map
    /// or per-node heap vectors while constructing this view.
    ///
    /// Persisted CSR hits (directed, undirected, and bounded overlays) report
    /// `0`. Scan-build fallback reports the number of map entries inserted.
    #[must_use]
    pub fn base_csr_entries_expanded(&self) -> u64 {
        self.inner.base_csr_entries_expanded()
    }

    /// Overlay replacement-row count (0 unless [`AdjacencyBacking::CsrOverlay`]
    /// or an undirected pair that includes an overlay).
    #[must_use]
    pub fn overlay_row_count(&self) -> u64 {
        self.inner.overlay_row_count()
    }

    /// Whether the view contains no adjacency entries at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Visit every node that may have adjacency entries.
    ///
    /// CSR-native backings yield dense `0..node_extent` ids (empty rows
    /// included only when the callback needs them — callers that skip empty
    /// rows should check [`NeighborRow::is_empty`]). Map backings yield only
    /// keys present in the map.
    pub(crate) fn for_each_row(&self, mut visit: impl FnMut(u64, NeighborRow<'_>)) {
        self.inner.for_each_row(&mut visit);
    }
}

impl AdjacencyInner {
    fn is_empty(&self) -> bool {
        match self {
            Self::Empty => true,
            Self::Map(map) => map.is_empty(),
            Self::Csr(csr) => csr.edge_count() == 0,
            Self::Sharded(csr) => csr.edge_count() == 0,
            Self::ShardedOverlay { base, replaced, .. } => {
                base.edge_count() == 0 && replaced.values().all(Vec::is_empty)
            }
            Self::Overlay(overlay) => {
                overlay.base.edge_count() == 0 && overlay.replaced.values().all(Vec::is_empty)
            }
            Self::Undirected { out, inbound } => out.is_empty() && inbound.is_empty(),
        }
    }

    fn neighbors(&self, node_id: u64) -> NeighborRow<'_> {
        match self {
            Self::Empty => NeighborRow::pairs(&[]),
            Self::Map(map) => NeighborRow::pairs(map.get(&node_id).map_or(&[], Vec::as_slice)),
            Self::Csr(csr) => NeighborRow::csr(csr.row(node_id)),
            Self::Sharded(csr) => NeighborRow::owned(
                csr.row(node_id)
                    .expect("authenticated immutable CSR shard changed after open"),
            ),
            Self::ShardedOverlay { base, replaced, .. } => {
                NeighborRow::owned(replaced.get(&node_id).cloned().unwrap_or_else(|| {
                    base.row(node_id)
                        .expect("authenticated immutable CSR shard changed after open")
                }))
            }
            Self::Overlay(overlay) => match overlay.row(node_id) {
                graphforge_storage::adjacency_delta::OverlayRow::Base(row) => NeighborRow::csr(row),
                graphforge_storage::adjacency_delta::OverlayRow::Replaced(entries) => {
                    NeighborRow::pairs(entries)
                }
            },
            Self::Undirected { out, inbound } => {
                merge_undirected_row(&out.neighbors(node_id), &inbound.neighbors(node_id))
            }
        }
    }

    fn backing(&self) -> AdjacencyBacking {
        match self {
            Self::Empty | Self::Map(_) => AdjacencyBacking::ScanHashMap,
            Self::Csr(_) | Self::Sharded(_) => AdjacencyBacking::CsrNative,
            Self::Overlay(_) | Self::ShardedOverlay { .. } => AdjacencyBacking::CsrOverlay,
            Self::Undirected { .. } => AdjacencyBacking::CsrUndirected,
        }
    }

    fn base_csr_entries_expanded(&self) -> u64 {
        match self {
            Self::Empty
            | Self::Csr(_)
            | Self::Sharded(_)
            | Self::Overlay(_)
            | Self::ShardedOverlay { .. } => 0,
            Self::Map(map) => u64::try_from(map.values().map(Vec::len).sum::<usize>()).unwrap_or(0),
            Self::Undirected { out, inbound } => out
                .base_csr_entries_expanded()
                .saturating_add(inbound.base_csr_entries_expanded()),
        }
    }

    fn overlay_row_count(&self) -> u64 {
        match self {
            Self::Overlay(overlay) => overlay.overlay_row_count(),
            Self::ShardedOverlay { replaced, .. } => {
                u64::try_from(replaced.len()).unwrap_or(u64::MAX)
            }
            Self::Undirected { out, inbound } => out
                .overlay_row_count()
                .saturating_add(inbound.overlay_row_count()),
            _ => 0,
        }
    }

    fn node_extent(&self) -> u64 {
        match self {
            Self::Empty => 0,
            Self::Map(map) => map.keys().next().map_or(0, |_| {
                map.keys().copied().max().map_or(0, |m| m.saturating_add(1))
            }),
            Self::Csr(csr) => csr.node_count(),
            Self::Sharded(csr) => csr.node_count(),
            Self::Overlay(overlay) => overlay.node_extent,
            Self::ShardedOverlay { node_extent, .. } => *node_extent,
            Self::Undirected { out, inbound } => out.node_extent().max(inbound.node_extent()),
        }
    }

    fn for_each_row(&self, visit: &mut dyn FnMut(u64, NeighborRow<'_>)) {
        match self {
            Self::Empty => {}
            Self::Map(map) => {
                for (&node_id, entries) in map {
                    visit(node_id, NeighborRow::pairs(entries.as_slice()));
                }
            }
            Self::Csr(csr) => {
                for node_id in 0..csr.node_count() {
                    visit(node_id, NeighborRow::csr(csr.row(node_id)));
                }
            }
            Self::Sharded(csr) => {
                for node_id in 0..csr.node_count() {
                    visit(node_id, self.neighbors(node_id));
                }
            }
            Self::Overlay(overlay) => {
                for node_id in 0..overlay.node_extent {
                    visit(node_id, self.neighbors(node_id));
                }
            }
            Self::ShardedOverlay { node_extent, .. } => {
                for node_id in 0..*node_extent {
                    visit(node_id, self.neighbors(node_id));
                }
            }
            Self::Undirected { out, inbound } => {
                let extent = out.node_extent().max(inbound.node_extent());
                for node_id in 0..extent {
                    visit(node_id, self.neighbors(node_id));
                }
            }
        }
    }
}

impl PartialEq for Adjacency {
    fn eq(&self, other: &Self) -> bool {
        fn collect(view: &Adjacency) -> Vec<(u64, Vec<(u64, u64)>)> {
            let mut rows = Vec::new();
            view.for_each_row(|node_id, row| {
                if !row.is_empty() {
                    rows.push((node_id, row.to_vec()));
                }
            });
            rows.sort_unstable_by_key(|(node_id, _)| *node_id);
            rows
        }
        collect(self) == collect(other)
    }
}

impl Eq for Adjacency {}

/// Merge out and in rows with ascending `edge_id` and **out before in on ties**.
fn merge_undirected_row<'a>(out: &NeighborRow<'a>, inbound: &NeighborRow<'a>) -> NeighborRow<'a> {
    if out.is_empty() {
        return NeighborRow::owned(inbound.to_vec());
    }
    if inbound.is_empty() {
        return NeighborRow::owned(out.to_vec());
    }
    let mut entries = Vec::with_capacity(out.len() + inbound.len());
    let mut o = 0usize;
    let mut i = 0usize;
    while o < out.len() || i < inbound.len() {
        let take_out = i >= inbound.len()
            || (o < out.len() && out.get(o).map(|(e, _)| e) <= inbound.get(i).map(|(e, _)| e));
        if take_out {
            entries.push(out.get(o).expect("out index in range"));
            o += 1;
        } else {
            entries.push(inbound.get(i).expect("in index in range"));
            i += 1;
        }
    }
    NeighborRow::owned(entries)
}

/// Single adjacency abstraction (ADR 0005): implementations decide *how* a
/// view is produced (explicit scan oracle or persistent bounded CSR) —
/// consumers only see [`Adjacency`].
pub trait AdjacencyProvider: Send + Sync {
    /// The adjacency view for (`rel_type_name`, `direction`).
    ///
    /// `"*"` means all relation types (#823): the per-row `rel_type_name`
    /// filter is skipped and every relation's edges are unioned — served by the
    /// `_all` CSR union when the persistent index is fresh. Persistent execution
    /// bounded-builds that union when absent; only an explicitly selected scan
    /// provider uses `read_edges(dir, "*", mode)`.
    ///
    /// # Errors
    /// Returns [`GfError::Execution`] on storage or decode failure.
    fn adjacency(
        &self,
        rel_type_name: &str,
        direction: Direction,
    ) -> Result<Arc<Adjacency>, GfError>;

    /// How [`adjacency`](Self::adjacency) for the same key would be served.
    fn status(&self, rel_type_name: &str, direction: Direction) -> AdjacencyStatus;
}

/// Scan-build provider: `graphforge_storage::read_edges` + in-memory build on every
/// call, with behavior parity to the retired private `build_adjacency`. No
/// caching and no persistence — session-scoped reuse and the disk-backed
/// loader are #761.
pub struct ScanBuildAdjacencyProvider {
    dir: PathBuf,
    mode: OntologyMode,
}

impl ScanBuildAdjacencyProvider {
    /// A provider over the project at `dir` in ontology `mode`.
    #[must_use]
    pub fn new(dir: PathBuf, mode: OntologyMode) -> Self {
        Self { dir, mode }
    }
}

impl AdjacencyProvider for ScanBuildAdjacencyProvider {
    fn adjacency(
        &self,
        rel_type_name: &str,
        direction: Direction,
    ) -> Result<Arc<Adjacency>, GfError> {
        // Advisory adoption does not rewrite edges that were created while the
        // project was exploratory. Read the union for a named advisory
        // relationship so `build_from_edge_batches` can select both legacy
        // exploratory rows and newly typed rows by their canonical name.
        let read_name = if matches!(self.mode, OntologyMode::Advisory) && rel_type_name != "*" {
            "*"
        } else {
            rel_type_name
        };
        let batches = graphforge_storage::read_edges(&self.dir, read_name, self.mode)
            .map_err(|e| GfError::Execution(e.to_string()))?;
        build_from_edge_batches(rel_type_name, direction, &batches).map(Arc::new)
    }

    fn status(&self, _rel_type_name: &str, _direction: Direction) -> AdjacencyStatus {
        AdjacencyStatus::Building
    }
}

/// Build `node_id -> [(edge_id, neighbour_node_id)]` honouring direction and,
/// when the batch carries a `rel_type_name` column, the relation-type filter.
fn build_from_edge_batches(
    rel_type_name: &str,
    direction: Direction,
    edge_batches: &[RecordBatch],
) -> Result<Adjacency, GfError> {
    let mut adj: HashMap<u64, Vec<(u64, u64)>> = HashMap::new();
    for batch in edge_batches {
        let edge_ids = crate::u64_column(batch, 3)?; // edge_id
        let src_ids = crate::u64_column(batch, 4)?; // src_id
        let dst_ids = crate::u64_column(batch, 5)?; // dst_id
        // A batch with a `rel_type_name` column — an exploratory file, or the
        // typed `"*"` union read (#823) — is filtered to the requested relation;
        // the `"*"` wildcard keeps every row. A typed per-relation file has no
        // such column (already pre-filtered by its file name), so gate on schema
        // presence, not ontology mode.
        let has_rel_col = batch.schema().field_with_name("rel_type_name").is_ok();
        let rel_names = if has_rel_col && rel_type_name != "*" {
            Some(crate::string_column(batch, "rel_type_name")?)
        } else {
            None
        };

        for i in 0..batch.num_rows() {
            if let Some(names) = &rel_names
                && names.value(i) != rel_type_name
            {
                continue;
            }
            let (Some(edge_id), Some(src), Some(dst)) = (
                edge_ids.value_at(i),
                src_ids.value_at(i),
                dst_ids.value_at(i),
            ) else {
                continue;
            };
            match direction {
                Direction::Out => adj.entry(src).or_default().push((edge_id, dst)),
                Direction::In => adj.entry(dst).or_default().push((edge_id, src)),
                Direction::Undirected => {
                    adj.entry(src).or_default().push((edge_id, dst));
                    adj.entry(dst).or_default().push((edge_id, src));
                }
            }
        }
    }
    Ok(Adjacency {
        inner: if adj.is_empty() {
            AdjacencyInner::Empty
        } else {
            AdjacencyInner::Map(adj)
        },
    })
}

// ---------------------------------------------------------------------------
// Persistent provider (#761)
// ---------------------------------------------------------------------------

/// What the persistent provider found under `indexes/adjacency/`, read at most
/// once per provider (= per query) and shared by `status()` and `adjacency()`
/// so explain output and execution agree.
#[derive(Clone, Debug)]
enum IndexState {
    /// `indexes/adjacency/` does not exist — the capability is not enabled.
    Absent,
    /// The generation counter or the manifest exists but cannot be read.
    /// Treated as always-stale WITHOUT rebuild: stamping a rebuilt manifest
    /// needs a readable counter.
    Unreadable,
    /// Manifest read. `fresh` = serveable: either the manifest generation
    /// equals the current counter, or it is older and an intact delta chain
    /// (#765) covers the gap.
    Ready {
        /// Whether the index may be served (vs. lazily rebuilt).
        fresh: bool,
        /// The project `topology_generation` observed when this state was
        /// read — what [`PersistentAdjacencyProvider::revalidate`] compares
        /// against for cheap cross-query freshness (#832).
        generation: u64,
        /// Authenticated immutable source identity owning this private cache.
        /// `None` is limited to legacy mutable sessions.
        source_identity: Option<AdjacencySourceIdentity>,
        /// The manifest rows.
        rows: Vec<AdjacencyManifestRow>,
        /// The delta chain (#765) overlaid on the base CSRs to reach
        /// `generation`; empty when the manifest already matches the counter.
        /// Loaded eagerly so `status()` and `adjacency()` agree on one snapshot.
        deltas: Arc<Vec<DeltaSegment>>,
    },
}

/// Provider over the on-disk CSR index (#761): serves `Hit`s from
/// `indexes/adjacency/` when the manifest generation matches the project's
/// `topology_generation`, and lazily runs the bounded external-sort builder
/// whenever the index cannot serve a key. The persistent provider never uses
/// the O(E)-memory scan-build oracle: if bounded index construction cannot
/// complete, traversal fails with a typed execution error instead of risking
/// process OOM.
///
/// The facade shares one instance across execution sessions. Loaded views are
/// cached per `(stem, direction)`, and lazy external-sort publication is
/// single-flight: concurrent waiters re-read and serve the winner's files.
pub struct PersistentAdjacencyProvider {
    dir: PathBuf,
    source_identity: Option<AdjacencySourceIdentity>,
    cache_dir: PathBuf,
    artifact_dir: Mutex<PathBuf>,
    /// Serializes lazy publication. Waiters re-read the published state.
    rebuild: Mutex<()>,
    /// Lazily-read index state; refreshed after a successful lazy rebuild.
    state: Mutex<Option<IndexState>>,
    /// Loaded views per `(stem, direction)`.
    cache: Mutex<HashMap<(String, Direction), Arc<Adjacency>>>,
}

impl PersistentAdjacencyProvider {
    /// A provider over the project at `dir` in ontology `mode`.
    #[must_use]
    pub fn new(dir: PathBuf, _mode: OntologyMode) -> Self {
        Self::with_artifact_dir(dir.clone(), dir, None)
    }

    /// A provider whose lazy derived artifacts live outside the source graph tree.
    ///
    /// Each provider receives a private child directory because the bounded
    /// builder uses fixed staging names. Providers for projected graphs or
    /// alternate execution modes must therefore never race in a shared cache
    /// root.
    #[must_use]
    pub fn new_with_cache(dir: PathBuf, cache_root: &std::path::Path, _mode: OntologyMode) -> Self {
        static NEXT_CACHE_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let id = NEXT_CACHE_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let artifact_dir = cache_root.join(format!("provider-{id}"));
        Self::with_artifact_dir(dir, artifact_dir, None)
    }

    /// A provider for one authenticated immutable project generation.
    #[must_use]
    pub fn new_with_authenticated_cache(
        dir: PathBuf,
        cache_root: &std::path::Path,
        _mode: OntologyMode,
        source_identity: AdjacencySourceIdentity,
    ) -> Self {
        static NEXT_CACHE_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let id = NEXT_CACHE_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let digest = source_identity
            .generation_manifest_sha256
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let artifact_dir = cache_root.join(format!(
            "generation-{}-{digest}-provider-{id}",
            source_identity.generation_uuid.hyphenated()
        ));
        Self::with_artifact_dir(dir, artifact_dir, Some(source_identity))
    }

    fn with_artifact_dir(
        dir: PathBuf,
        artifact_dir: PathBuf,
        source_identity: Option<AdjacencySourceIdentity>,
    ) -> Self {
        let active_artifact = if csr::adjacency_dir(&dir).exists() {
            dir.clone()
        } else {
            artifact_dir.clone()
        };
        Self {
            dir,
            source_identity,
            cache_dir: artifact_dir,
            artifact_dir: Mutex::new(active_artifact),
            rebuild: Mutex::new(()),
            state: Mutex::new(None),
            cache: Mutex::new(HashMap::new()),
        }
    }

    /// The CSR file stem serving `rel_type_name`: the reserved
    /// [`ALL_RELATIONS_STEM`] union pair for an untyped `"*"` pattern — in
    /// **all** modes (#823), since the union index is built mode-agnostically —
    /// else the relation name itself.
    fn stem_for(rel_type_name: &str) -> String {
        if rel_type_name == "*" {
            ALL_RELATIONS_STEM.to_owned()
        } else {
            adjacency_relation_key(rel_type_name)
        }
    }

    /// The current index state, read once and memoized.
    fn state(&self) -> IndexState {
        let mut guard = self.state.lock().expect("adjacency state lock");
        let artifact_dir = self
            .artifact_dir
            .lock()
            .expect("adjacency artifact lock")
            .clone();
        guard
            .get_or_insert_with(|| Self::read_state(&self.dir, &artifact_dir, self.source_identity))
            .clone()
    }

    fn read_state(
        source_dir: &std::path::Path,
        artifact_dir: &std::path::Path,
        source_identity: Option<AdjacencySourceIdentity>,
    ) -> IndexState {
        if !csr::adjacency_dir(artifact_dir).exists() {
            return IndexState::Absent;
        }
        let Ok(generation) = read_topology_generation(source_dir) else {
            return IndexState::Unreadable;
        };
        let Ok(rows) = csr::read_manifest(artifact_dir) else {
            return IndexState::Unreadable;
        };
        // The base generation the CSRs were built at — uniform across rows on a
        // clean build. An empty manifest (torn build) or rows that disagree are
        // stale (rebuild repairs them), never served.
        let base = rows.first().map(|r| r.topology_generation);
        let uniform = base.is_some_and(|b| rows.iter().all(|r| r.topology_generation == b));
        // Raw-name manifests predate identity-bound keys and cannot distinguish
        // a literal `_all` relation from the wildcard union. Rebuild them;
        // never interpret absent encoded coverage as a proven empty relation.
        let identity_bound = rows.iter().all(|row| match row.relation_name.as_deref() {
            None => row.relation_type == ALL_RELATIONS_STEM,
            Some(name) => {
                is_adjacency_relation_key(&row.relation_type)
                    && row.relation_type == adjacency_relation_key(name)
            }
        });
        let (fresh, deltas) = match base {
            // base == counter: exact match, no overlay (the #761 fast path).
            Some(b) if uniform && identity_bound && b == generation => (true, Vec::new()),
            // base < counter: serveable iff an intact, bounded delta chain
            // (#765) covers (base, counter]; otherwise stale ⇒ rebuild.
            Some(b) if uniform && identity_bound && b < generation => {
                match read_delta_chain(source_dir, b, generation) {
                    Some(chain) => (true, chain),
                    None => (false, Vec::new()),
                }
            }
            // Empty / torn manifest, or an index newer than the counter
            // (anomalous, e.g. a counter reset): stale.
            _ => (false, Vec::new()),
        };
        IndexState::Ready {
            fresh,
            generation,
            source_identity,
            rows,
            deltas: Arc::new(deltas),
        }
    }

    /// Whether `rows` contain entries for every CSR file `direction` needs.
    fn rows_cover(rows: &[AdjacencyManifestRow], stem: &str, direction: Direction) -> bool {
        let has = |d: csr::Direction| {
            rows.iter()
                .any(|r| r.relation_type == stem && r.direction == d)
        };
        match direction {
            Direction::Out => has(csr::Direction::Out),
            Direction::In => has(csr::Direction::In),
            Direction::Undirected => has(csr::Direction::Out) && has(csr::Direction::In),
        }
    }

    fn rows_cover_name(
        rows: &[AdjacencyManifestRow],
        stem: &str,
        rel_type_name: &str,
        direction: Direction,
    ) -> bool {
        let expected = (rel_type_name != "*").then_some(rel_type_name);
        Self::rows_cover(rows, stem, direction)
            && rows
                .iter()
                .filter(|row| row.relation_type == stem)
                .all(|row| row.relation_name.as_deref() == expected)
    }

    /// Load the view for (`stem`, `direction`) from the CSR file(s), overlaying
    /// the delta chain (#765) when one is present (`deltas` non-empty).
    ///
    /// A fresh base CSR is retained as a CSR-native view (#340): no O(E)
    /// HashMap expansion. Non-empty delta chains attach a bounded overlay that
    /// replaces only touched keys.
    fn load(
        &self,
        stem: &str,
        direction: Direction,
        rows: &[AdjacencyManifestRow],
        deltas: &[DeltaSegment],
    ) -> Result<Adjacency, GfError> {
        // One immutable publication serves both halves of an undirected view.
        // Capture it once so concurrent invalidation cannot mix roots.
        let artifact_dir = self
            .artifact_dir
            .lock()
            .expect("adjacency artifact lock")
            .clone();
        let directed = |d: csr::Direction| -> Result<AdjacencyInner, GfError> {
            let path = csr::csr_path(&artifact_dir, stem, d);
            if csr::sharded_csr_exists(&path) {
                let base = Arc::new(ShardedCsrIndex::open(&path)?);
                if let Some(row) = rows
                    .iter()
                    .find(|r| r.relation_type == stem && r.direction == d)
                    && (base.node_count() != row.node_count || base.edge_count() != row.edge_count)
                {
                    return Err(GfError::Storage(
                        "adjacency sharded CSR disagrees with manifest counts (torn read)".into(),
                    ));
                }
                if deltas.is_empty() {
                    return Ok(AdjacencyInner::Sharded(base));
                }
                let (replaced, node_extent) = sharded_overlay_rows(&base, stem, d, deltas)?;
                if replaced.is_empty() {
                    return Ok(AdjacencyInner::Sharded(base));
                }
                return Ok(AdjacencyInner::ShardedOverlay {
                    base,
                    replaced,
                    node_extent,
                });
            }
            // Legacy single-batch CSR migration path. A successful rebuild
            // publishes sharded v1 files; old projects remain readable until
            // that rebuild occurs.
            let base = Arc::new(csr::read_csr(&path)?);
            if deltas.is_empty() {
                return Ok(AdjacencyInner::Csr(base));
            }
            // Torn-read count guard (#765): the base CSR must match the manifest
            // row it was loaded against. If a concurrent compaction rewrote the
            // CSR under this snapshot, the recorded counts differ and applying
            // the chain would double-count — bail to a rebuild instead.
            if let Some(row) = rows
                .iter()
                .find(|r| r.relation_type == stem && r.direction == d)
                && (base.node_count() != row.node_count || base.edge_count() != row.edge_count)
            {
                return Err(GfError::Storage(
                    "adjacency base CSR disagrees with manifest counts (torn read)".into(),
                ));
            }
            let overlay = overlay_delta_segments(base, stem, d, deltas);
            if overlay.replaced.is_empty() {
                Ok(AdjacencyInner::Csr(overlay.base))
            } else {
                Ok(AdjacencyInner::Overlay(overlay))
            }
        };
        match direction {
            Direction::Out => Ok(Adjacency {
                inner: directed(csr::Direction::Out)?,
            }),
            Direction::In => Ok(Adjacency {
                inner: directed(csr::Direction::In)?,
            }),
            Direction::Undirected => Ok(Adjacency {
                inner: AdjacencyInner::Undirected {
                    out: Arc::new(directed(csr::Direction::Out)?),
                    inbound: Arc::new(directed(csr::Direction::In)?),
                },
            }),
        }
    }

    /// Lazily rebuild the index, refresh the memoized state, and serve from
    /// the fresh files. This path deliberately fails closed rather than using
    /// the O(E)-memory scan-build oracle.
    fn rebuild_and_serve(
        &self,
        rel_type_name: &str,
        direction: Direction,
    ) -> Result<Arc<Adjacency>, GfError> {
        self.rebuild_and_serve_with_checkpoint(rel_type_name, direction, || Ok(()))
    }

    #[allow(clippy::too_many_lines)] // single-flight recheck, build, generation validation, and publication are one atomic path
    fn rebuild_and_serve_with_checkpoint(
        &self,
        rel_type_name: &str,
        direction: Direction,
        mut checkpoint: impl FnMut() -> Result<(), GfError>,
    ) -> Result<Arc<Adjacency>, GfError> {
        let stem = Self::stem_for(rel_type_name);
        let _rebuild = self.rebuild.lock().expect("adjacency rebuild lock");

        // Another query may have completed the bounded build while this caller
        // waited. Re-read from disk under the single-flight lock and serve its
        // publication instead of multiplying external-sort memory and racing
        // the builder's fixed staging paths.
        if let Some(view) = self
            .cache
            .lock()
            .expect("adjacency cache lock")
            .get(&(stem.clone(), direction))
            .cloned()
        {
            return Ok(view);
        }
        let active_artifact = self
            .artifact_dir
            .lock()
            .expect("adjacency artifact lock")
            .clone();
        if let IndexState::Ready {
            fresh: true,
            generation,
            source_identity,
            rows,
            deltas,
        } = Self::read_state(&self.dir, &active_artifact, self.source_identity)
        {
            let covered = Self::rows_cover_name(&rows, &stem, rel_type_name, direction);
            let loaded = if covered {
                self.load(&stem, direction, &rows, &deltas).map(Some)
            } else if deltas.is_empty() {
                Ok(None)
            } else {
                Err(GfError::Execution(
                    "fresh adjacency delta chain lacks requested relation coverage".into(),
                ))
            };
            if let Ok(view) = loaded {
                *self.state.lock().expect("adjacency state lock") = Some(IndexState::Ready {
                    fresh: true,
                    generation,
                    source_identity,
                    rows,
                    deltas,
                });
                return Ok(self.cache_view(&stem, direction, view.unwrap_or_default()));
            }
        }
        self.artifact_dir
            .lock()
            .expect("adjacency artifact lock")
            .clone_from(&self.cache_dir);
        for attempt in 0..2 {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| i64::try_from(d.as_micros()).unwrap_or(i64::MAX));
            let rows = graphforge_storage::adjacency::build_adjacency_index_into(
                &self.dir,
                &self.cache_dir,
                now,
                &mut checkpoint,
            )
            .map_err(|error| {
                GfError::Execution(format!("bounded adjacency index build failed: {error}"))
            })?;
            let base_generation = rows.first().map_or_else(
                || read_topology_generation(&self.dir).unwrap_or(0),
                |row| row.topology_generation,
            );
            let current_generation = read_topology_generation(&self.dir).map_err(|error| {
                GfError::Execution(format!(
                    "cannot validate bounded adjacency build generation: {error}"
                ))
            })?;
            let deltas = if current_generation == base_generation {
                Vec::new()
            } else if current_generation > base_generation {
                match read_delta_chain(&self.dir, base_generation, current_generation) {
                    Some(chain) => chain,
                    None if attempt == 0 => continue,
                    None => {
                        return Err(GfError::Execution(
                            "topology changed during bounded adjacency build without a complete delta chain"
                                .into(),
                        ));
                    }
                }
            } else if attempt == 0 {
                continue;
            } else {
                return Err(GfError::Execution(
                    "topology generation moved backwards during bounded adjacency build".into(),
                ));
            };
            let covered = Self::rows_cover_name(&rows, &stem, rel_type_name, direction);
            let view = covered
                .then(|| self.load(&stem, direction, &rows, &deltas))
                .transpose()?;
            *self.state.lock().expect("adjacency state lock") = Some(IndexState::Ready {
                fresh: true,
                generation: current_generation,
                source_identity: self.source_identity,
                rows,
                deltas: Arc::new(deltas),
            });
            if let Some(view) = view {
                return Ok(self.cache_view(&stem, direction, view));
            }
            return Ok(self.cache_view(&stem, direction, Adjacency::default()));
        }
        unreachable!("bounded adjacency rebuild retry loop returns")
    }

    /// Drop the memoized index state and every loaded view, forcing the next
    /// request to re-read the generation counter and manifest.
    ///
    /// [`ExecutionSession`](crate::ExecutionSession) calls this after every
    /// successful write: a session that read (caching a view), wrote (bumping
    /// the generation), and read again would otherwise serve the pre-write
    /// view from cache.
    pub fn invalidate(&self) {
        *self.state.lock().expect("adjacency state lock") = None;
        self.cache.lock().expect("adjacency cache lock").clear();
        *self.artifact_dir.lock().expect("adjacency artifact lock") =
            if csr::adjacency_dir(&self.dir).exists() {
                self.dir.clone()
            } else {
                self.cache_dir.clone()
            };
    }

    /// Cheap cross-query freshness check (#832): one `generation.json` read
    /// (plus one existence probe when the index was absent). Called once per
    /// [`ExecutionSession`](crate::ExecutionSession) construction by the
    /// shared facade provider; within-query memoization — the
    /// status/adjacency snapshot agreement — is untouched.
    ///
    /// Drops the memoized state and view cache when: the prior read was
    /// `Unreadable` (always retry — rare and cheap), the index was `Absent`
    /// (scan-built views are query-scoped), or the observed generation no
    /// longer matches the counter.
    pub fn revalidate(&self) {
        let mut state = self.state.lock().expect("adjacency state lock");
        let drop_state = match state.as_ref() {
            None => false,
            Some(IndexState::Ready {
                fresh: true,
                generation,
                source_identity,
                ..
            }) => source_identity.map_or_else(
                || !read_topology_generation(&self.dir).is_ok_and(|g| g == *generation),
                |observed| Some(observed) != self.source_identity,
            ),
            // Non-serving states — always retry (a cheap manifest re-read, not
            // a rebuild). For a stale `fresh: false` index this matters: it may
            // have been repaired in place at the *same* topology generation (an
            // explicit `forge.index()` rebuild or an external builder), and the
            // generation check above would otherwise pin the stale state
            // forever — single-hop would keep lowering to a join without ever
            // calling `adjacency()` to self-repair.
            Some(
                IndexState::Absent
                | IndexState::Unreadable
                | IndexState::Ready { fresh: false, .. },
            ) => true,
        };
        if drop_state {
            *state = None;
            self.cache.lock().expect("adjacency cache lock").clear();
        }
    }

    fn cache_shared_view(
        &self,
        stem: &str,
        direction: Direction,
        view: Arc<Adjacency>,
    ) -> Arc<Adjacency> {
        self.cache
            .lock()
            .expect("adjacency cache lock")
            .insert((stem.to_owned(), direction), Arc::clone(&view));
        view
    }

    fn cache_view(&self, stem: &str, direction: Direction, view: Adjacency) -> Arc<Adjacency> {
        self.cache_shared_view(stem, direction, Arc::new(view))
    }
}

fn sharded_overlay_rows(
    base: &ShardedCsrIndex,
    stem: &str,
    direction: csr::Direction,
    chain: &[DeltaSegment],
) -> Result<(ShardedReplacementRows, u64), GfError> {
    let take_all = stem == ALL_RELATIONS_STEM;
    let mut by_key: HashMap<u64, Vec<(u64, u64)>> = HashMap::new();
    let mut max_key = base.node_count().saturating_sub(1);
    for segment in chain {
        for edge in &segment.edges {
            if take_all || adjacency_relation_key(&edge.rel_type_name) == stem {
                let (key, neighbor) = match direction {
                    csr::Direction::Out => (edge.src_id, edge.dst_id),
                    csr::Direction::In => (edge.dst_id, edge.src_id),
                };
                by_key
                    .entry(key)
                    .or_default()
                    .push((edge.edge_id, neighbor));
                max_key = max_key.max(key);
            }
        }
    }
    for (key, delta) in &mut by_key {
        let mut combined = base.row(*key)?;
        combined.append(delta);
        combined.sort_unstable_by_key(|&(edge_id, _)| edge_id);
        *delta = combined;
    }
    let extent = if by_key.is_empty() {
        base.node_count()
    } else {
        base.node_count().max(max_key.saturating_add(1))
    };
    Ok((by_key, extent))
}

impl AdjacencyProvider for PersistentAdjacencyProvider {
    fn adjacency(
        &self,
        rel_type_name: &str,
        direction: Direction,
    ) -> Result<Arc<Adjacency>, GfError> {
        let stem = Self::stem_for(rel_type_name);
        if let Some(view) = self
            .cache
            .lock()
            .expect("adjacency cache lock")
            .get(&(stem.clone(), direction))
        {
            return Ok(Arc::clone(view));
        }
        match self.state() {
            // The scan fallback materializes every edge twice: first in Arrow
            // batches and then in a HashMap. That makes the first ordinary
            // fixed-hop query O(E) anonymous memory. The adjacency builder is
            // already an external-sort, bounded-memory operation; use it to
            // publish sharded CSR and serve requested rows from disk. This is
            // also the repair path for an unreadable index.
            IndexState::Ready {
                fresh: true,
                rows,
                deltas,
                ..
            } => {
                if !Self::rows_cover_name(&rows, &stem, rel_type_name, direction) {
                    // With no delta chain, a complete current manifest proves
                    // that this relation has no source edges. A relation born
                    // after the base index can exist only when deltas are
                    // present; rebuild that case so missing coverage never
                    // diverts into the O(E)-memory scan oracle.
                    return if deltas.is_empty() {
                        Ok(self.cache_view(&stem, direction, Adjacency::default()))
                    } else {
                        self.rebuild_and_serve(rel_type_name, direction)
                    };
                }
                match self.load(&stem, direction, &rows, &deltas) {
                    Ok(view) => Ok(self.cache_view(&stem, direction, view)),
                    // CSR missing/corrupt, or the torn-read count guard tripped:
                    // one lazy rebuild repairs the index.
                    Err(_) => self.rebuild_and_serve(rel_type_name, direction),
                }
            }
            IndexState::Absent
            | IndexState::Unreadable
            | IndexState::Ready { fresh: false, .. } => {
                self.rebuild_and_serve(rel_type_name, direction)
            }
        }
    }

    fn status(&self, rel_type_name: &str, direction: Direction) -> AdjacencyStatus {
        let stem = Self::stem_for(rel_type_name);
        match self.state() {
            IndexState::Absent => AdjacencyStatus::Building,
            IndexState::Unreadable | IndexState::Ready { fresh: false, .. } => {
                AdjacencyStatus::Miss
            }
            IndexState::Ready {
                fresh: true, rows, ..
            } => {
                let files_exist = |d: csr::Direction| {
                    let path = csr::csr_path(&self.dir, &stem, d);
                    path.exists() || csr::sharded_csr_exists(&path)
                };
                let present = Self::rows_cover_name(&rows, &stem, rel_type_name, direction)
                    && match direction {
                        Direction::Out => files_exist(csr::Direction::Out),
                        Direction::In => files_exist(csr::Direction::In),
                        Direction::Undirected => {
                            files_exist(csr::Direction::Out) && files_exist(csr::Direction::In)
                        }
                    };
                if present {
                    AdjacencyStatus::Hit
                } else {
                    AdjacencyStatus::Miss
                }
            }
        }
    }
}

/// Undirected CSR pair without materializing a merged hash map (#340).
#[cfg(test)]
fn merge_undirected(out: CsrIndex, inbound: CsrIndex) -> Adjacency {
    Adjacency {
        inner: AdjacencyInner::Undirected {
            out: Arc::new(AdjacencyInner::Csr(Arc::new(out))),
            inbound: Arc::new(AdjacencyInner::Csr(Arc::new(inbound))),
        },
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::path::Path;

    use graphforge_core::TypeId;
    use graphforge_core::uuid::{Uuid, new_v7, to_bytes};
    use tempfile::TempDir;

    use graphforge_storage::GraphWriter;

    use super::*;

    /// Fixed timestamp so written Parquet is deterministic.
    const TS: i64 = 1_700_000_000_000_000;

    /// Diamond a→b, a→c, b→d, c→d plus a parallel edge a→b and a self-loop
    /// d→d, all `KNOWS`, Strict mode. Returns the surrogate node ids.
    #[allow(clippy::many_single_char_names)]
    fn write_diamond(dir: &Path) -> [u64; 4] {
        let mut w = GraphWriter::open_at(dir, OntologyMode::Strict, TS).unwrap();
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
    fn out_in_undirected_strict() {
        let dir = TempDir::new().unwrap();
        let [a, b, _c, d] = write_diamond(dir.path());
        let provider =
            ScanBuildAdjacencyProvider::new(dir.path().to_path_buf(), OntologyMode::Strict);
        let out = provider.adjacency("KNOWS", Direction::Out).unwrap();
        // a has three outgoing entries: a→b, a→c, then the parallel a→b.
        assert_eq!(out.neighbors(a).len(), 3);
        // The self-loop is one Out entry under d...
        assert_eq!(out.neighbors(d).to_vec(), vec![(6, d)]);
        // ...and two Undirected entries under d (src-keyed + dst-keyed), after
        // the two incoming diamond edges b→d, c→d.
        let undirected = provider.adjacency("KNOWS", Direction::Undirected).unwrap();
        assert_eq!(undirected.neighbors(d).len(), 4);
        let in_view = provider.adjacency("KNOWS", Direction::In).unwrap();
        // b's incoming: a→b and the parallel a→b.
        assert_eq!(in_view.neighbors(b).to_vec(), vec![(1, a), (5, a)]);
    }

    /// `KNOWS` and `OWNS` rows share `_exploratory.parquet`; the rel filter
    /// must exclude the decoy type.
    #[test]
    fn exploratory_rel_filter_excludes_decoy() {
        let dir = TempDir::new().unwrap();
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
        let (a, b, c) = (new_v7(), new_v7(), new_v7());
        let ids: Vec<u64> = [a, b, c]
            .iter()
            .map(|u| w.create_node(*u, TypeId(0)).unwrap())
            .collect();
        w.create_edge(new_v7(), "KNOWS", &a, &b).unwrap();
        w.create_edge(new_v7(), "OWNS", &a, &c).unwrap();
        w.flush().unwrap();

        let provider =
            ScanBuildAdjacencyProvider::new(dir.path().to_path_buf(), OntologyMode::Exploratory);
        let view = provider.adjacency("KNOWS", Direction::Out).unwrap();
        assert_eq!(
            view.neighbors(ids[0]).to_vec(),
            vec![(1, ids[1])],
            "OWNS row excluded"
        );
    }

    #[test]
    fn exploratory_wildcard_includes_all_rel_types() {
        let dir = TempDir::new().unwrap();
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
        let (a, b, c) = (new_v7(), new_v7(), new_v7());
        let ids: Vec<u64> = [a, b, c]
            .iter()
            .map(|u| w.create_node(*u, TypeId(0)).unwrap())
            .collect();
        w.create_edge(new_v7(), "KNOWS", &a, &b).unwrap();
        w.create_edge(new_v7(), "OWNS", &a, &c).unwrap();
        w.flush().unwrap();

        let provider =
            ScanBuildAdjacencyProvider::new(dir.path().to_path_buf(), OntologyMode::Exploratory);
        let view = provider.adjacency("*", Direction::Out).unwrap();
        assert_eq!(
            view.neighbors(ids[0]).to_vec(),
            vec![(1, ids[1]), (2, ids[2])],
            "wildcard skips the rel filter"
        );
    }

    /// In Strict/Advisory `"*"` is an untyped pattern: scan-build reads the
    /// `read_edges(dir, "*")` union of every per-relation
    /// `topology/edges/*.parquet` (#823), so the wildcard view is the union of
    /// all relation types — the same view exploratory mode produces from the
    /// shared file. (Was `typed_mode_wildcard_is_empty`, the pre-#823 bug.)
    #[test]
    fn typed_mode_wildcard_unions_all_rel_types() {
        let dir = TempDir::new().unwrap();
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
        let (a, b, c) = (new_v7(), new_v7(), new_v7());
        let ids: Vec<u64> = [a, b, c]
            .iter()
            .map(|u| w.create_node(*u, TypeId(0)).unwrap())
            .collect();
        w.create_edge(new_v7(), "KNOWS", &a, &b).unwrap();
        w.create_edge(new_v7(), "OWNS", &a, &c).unwrap();
        w.flush().unwrap();

        let provider =
            ScanBuildAdjacencyProvider::new(dir.path().to_path_buf(), OntologyMode::Strict);
        let view = provider.adjacency("*", Direction::Out).unwrap();
        // Both relations' edges appear (KNOWS a→b, OWNS a→c), unioned across the
        // two per-relation files — no longer the pre-#823 empty view.
        let mut got = view.neighbors(ids[0]).to_vec();
        got.sort_unstable();
        assert_eq!(
            got,
            vec![(1, ids[1]), (2, ids[2])],
            "typed wildcard unions KNOWS+OWNS"
        );
    }

    /// DELETE rewrites edge files dropping rows, so surrogate ids are sparse
    /// afterwards: the deleted node's id must yield `&[]`, survivors stay
    /// correct.
    #[test]
    fn post_delete_sparse_ids() {
        let dir = TempDir::new().unwrap();
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
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

        // DETACH-DELETE the second node (n1..n4 chain: delete n2 + its edges).
        let node_set: std::collections::HashSet<[u8; 16]> =
            std::iter::once(to_bytes(&uuids[1])).collect();
        let incident = graphforge_storage::incident_edge_uuids(dir.path(), &node_set).unwrap();
        let edge_set: std::collections::HashSet<[u8; 16]> = incident.into_iter().collect();
        graphforge_storage::delete_nodes_and_edges(dir.path(), &node_set, &edge_set).unwrap();

        let provider =
            ScanBuildAdjacencyProvider::new(dir.path().to_path_buf(), OntologyMode::Strict);
        let view = provider.adjacency("KNOWS", Direction::Out).unwrap();
        assert!(view.neighbors(ids[1]).is_empty(), "deleted id yields empty");
        assert_eq!(
            view.neighbors(ids[2]).to_vec(),
            vec![(3, ids[3])],
            "survivor intact"
        );
    }

    #[test]
    fn absent_edges_dir_yields_empty_adjacency() {
        let dir = TempDir::new().unwrap();
        let provider =
            ScanBuildAdjacencyProvider::new(dir.path().to_path_buf(), OntologyMode::Strict);
        let view = provider.adjacency("KNOWS", Direction::Out).unwrap();
        assert!(view.is_empty());
        assert!(view.neighbors(1).is_empty());
    }

    /// Neighbor order is edge-file row order — BFS emission order depends on
    /// it, so it is part of the behavioral contract.
    #[test]
    fn neighbor_order_matches_edge_row_order() {
        let dir = TempDir::new().unwrap();
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
        let hub = new_v7();
        let hub_id = w.create_node(hub, TypeId(0)).unwrap();
        let spokes: Vec<Uuid> = (0..3).map(|_| new_v7()).collect();
        let spoke_ids: Vec<u64> = spokes
            .iter()
            .map(|u| w.create_node(*u, TypeId(0)).unwrap())
            .collect();
        for spoke in &spokes {
            w.create_edge(new_v7(), "KNOWS", &hub, spoke).unwrap();
        }
        w.flush().unwrap();

        let provider =
            ScanBuildAdjacencyProvider::new(dir.path().to_path_buf(), OntologyMode::Strict);
        let view = provider.adjacency("KNOWS", Direction::Out).unwrap();
        assert_eq!(
            view.neighbors(hub_id).to_vec(),
            vec![(1, spoke_ids[0]), (2, spoke_ids[1]), (3, spoke_ids[2])],
            "entries in edge-file row order"
        );
    }

    #[test]
    fn persisted_csr_hit_does_not_expand_base_into_hash_map() {
        let dir = TempDir::new().unwrap();
        let _ids = write_diamond(dir.path());
        graphforge_storage::adjacency::build_adjacency_index(dir.path(), TS).unwrap();
        let provider =
            PersistentAdjacencyProvider::new(dir.path().to_path_buf(), OntologyMode::Strict);
        assert_eq!(
            provider.status("KNOWS", Direction::Out),
            AdjacencyStatus::Hit
        );
        let out = provider.adjacency("KNOWS", Direction::Out).unwrap();
        assert_eq!(out.backing(), AdjacencyBacking::CsrNative);
        assert_eq!(out.base_csr_entries_expanded(), 0);
        assert_eq!(out.overlay_row_count(), 0);

        let undirected = provider.adjacency("KNOWS", Direction::Undirected).unwrap();
        assert_eq!(undirected.backing(), AdjacencyBacking::CsrUndirected);
        assert_eq!(undirected.base_csr_entries_expanded(), 0);

        let scanned =
            ScanBuildAdjacencyProvider::new(dir.path().to_path_buf(), OntologyMode::Strict)
                .adjacency("KNOWS", Direction::Out)
                .unwrap();
        assert_eq!(
            out.as_ref(),
            scanned.as_ref(),
            "CSR-native matches scan oracle"
        );
        assert!(scanned.base_csr_entries_expanded() > 0);
        assert_eq!(scanned.backing(), AdjacencyBacking::ScanHashMap);
    }

    /// The first ordinary fixed-hop query on an unindexed project must not
    /// construct an O(E) anonymous-memory HashMap. It builds the bounded,
    /// spillable persistent representation and immediately serves sharded CSR.
    #[test]
    fn absent_index_builds_and_serves_disk_backed_csr() {
        let dir = TempDir::new().unwrap();
        let [a, b, c, _d] = write_diamond(dir.path());
        let provider =
            PersistentAdjacencyProvider::new(dir.path().to_path_buf(), OntologyMode::Strict);

        assert_eq!(
            provider.status("KNOWS", Direction::Out),
            AdjacencyStatus::Building
        );
        let out = provider.adjacency("KNOWS", Direction::Out).unwrap();

        assert_eq!(out.backing(), AdjacencyBacking::CsrNative);
        assert_eq!(out.base_csr_entries_expanded(), 0);
        assert_eq!(out.neighbors(a).to_vec(), vec![(1, b), (2, c), (5, b)]);
        assert!(csr::adjacency_dir(dir.path()).exists());
        assert_eq!(
            provider.status("KNOWS", Direction::Out),
            AdjacencyStatus::Hit
        );
    }

    #[test]
    fn persistent_exact_relation_keys_do_not_collide_with_wildcard_or_paths() {
        let dir = TempDir::new().unwrap();
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
        let (a, b, c) = (new_v7(), new_v7(), new_v7());
        let ids: Vec<u64> = [a, b, c]
            .iter()
            .map(|uuid| w.create_node(*uuid, TypeId(0)).unwrap())
            .collect();
        w.create_edge(new_v7(), "a/b", &a, &b).unwrap();
        w.create_edge(new_v7(), "_all", &a, &c).unwrap();
        w.flush().unwrap();

        let provider =
            PersistentAdjacencyProvider::new(dir.path().to_path_buf(), OntologyMode::Exploratory);
        assert_eq!(
            provider
                .adjacency("a/b", Direction::Out)
                .unwrap()
                .neighbors(ids[0])
                .to_vec(),
            vec![(1, ids[1])]
        );
        assert_eq!(
            provider
                .adjacency("_all", Direction::Out)
                .unwrap()
                .neighbors(ids[0])
                .to_vec(),
            vec![(2, ids[2])]
        );
        assert_eq!(
            provider
                .adjacency("*", Direction::Out)
                .unwrap()
                .neighbors(ids[0])
                .to_vec(),
            vec![(1, ids[1]), (2, ids[2])]
        );
    }

    #[test]
    fn lazy_build_validates_and_overlays_a_concurrent_topology_commit() {
        let dir = TempDir::new().unwrap();
        let [a, _b, _c, _d] = write_diamond(dir.path());
        let provider =
            PersistentAdjacencyProvider::new(dir.path().to_path_buf(), OntologyMode::Strict);
        let mut checkpoints = 0;
        let mut appended = None;

        let view = provider
            .rebuild_and_serve_with_checkpoint("KNOWS", Direction::Out, || {
                checkpoints += 1;
                if checkpoints == 2 {
                    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
                    let src = new_v7();
                    let dst = new_v7();
                    let src_id = w.create_node(src, TypeId(0)).unwrap();
                    let dst_id = w.create_node(dst, TypeId(0)).unwrap();
                    w.create_edge(new_v7(), "KNOWS", &src, &dst).unwrap();
                    w.flush().unwrap();
                    appended = Some((src_id, dst_id));
                }
                Ok(())
            })
            .unwrap();

        let (src, dst) = appended.expect("checkpoint injected one topology commit");
        assert_eq!(view.neighbors(src).to_vec(), vec![(7, dst)]);
        assert_eq!(view.neighbors(a).len(), 3);
        assert_eq!(
            provider.status("KNOWS", Direction::Out),
            AdjacencyStatus::Hit,
            "the memoized state is stamped only after generation validation"
        );
    }

    #[test]
    fn private_lazy_build_never_mutates_the_authoritative_graph_tree() {
        let dir = TempDir::new().unwrap();
        let cache = TempDir::new().unwrap();
        let [a, ..] = write_diamond(dir.path());
        assert!(!csr::adjacency_dir(dir.path()).exists());

        let provider = PersistentAdjacencyProvider::new_with_cache(
            dir.path().to_path_buf(),
            cache.path(),
            OntologyMode::Strict,
        );
        let view = provider.adjacency("KNOWS", Direction::Out).unwrap();

        assert_eq!(view.neighbors(a).len(), 3);
        assert!(
            !csr::adjacency_dir(dir.path()).exists(),
            "query repair wrote derived files into authoritative graph content"
        );
        assert!(
            std::fs::read_dir(cache.path())
                .unwrap()
                .any(|entry| csr::adjacency_dir(&entry.unwrap().path()).exists()),
            "bounded CSR publication did not use its private cache namespace"
        );
    }

    #[test]
    fn concurrent_lazy_rebuild_is_single_flight_and_waiter_serves_publication() {
        use std::sync::mpsc;
        use std::time::Duration;

        let dir = TempDir::new().unwrap();
        write_diamond(dir.path());
        let provider = Arc::new(PersistentAdjacencyProvider::new(
            dir.path().to_path_buf(),
            OntologyMode::Strict,
        ));
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (waiter_started_tx, waiter_started_rx) = mpsc::channel();
        let (waiter_tx, waiter_rx) = mpsc::channel();

        std::thread::scope(|scope| {
            let builder_provider = Arc::clone(&provider);
            let builder = scope.spawn(move || {
                let mut first = true;
                builder_provider.rebuild_and_serve_with_checkpoint("KNOWS", Direction::Out, || {
                    if first {
                        first = false;
                        entered_tx.send(()).unwrap();
                        release_rx.recv().unwrap();
                    }
                    Ok(())
                })
            });
            entered_rx.recv().unwrap();

            let waiter_provider = Arc::clone(&provider);
            let waiter = scope.spawn(move || {
                waiter_started_tx.send(()).unwrap();
                let result = waiter_provider.adjacency("KNOWS", Direction::Out);
                waiter_tx.send(()).unwrap();
                result
            });
            waiter_started_rx.recv().unwrap();
            assert!(
                waiter_rx.recv_timeout(Duration::from_millis(50)).is_err(),
                "waiter must not enter a second external-sort build"
            );
            release_tx.send(()).unwrap();

            let built = builder.join().unwrap().unwrap();
            let waiter_view = waiter.join().unwrap().unwrap();
            assert!(Arc::ptr_eq(&built, &waiter_view));
        });
    }

    #[test]
    fn complete_index_missing_relation_returns_empty_without_scan_hash_map() {
        let dir = TempDir::new().unwrap();
        write_diamond(dir.path());
        graphforge_storage::adjacency::build_adjacency_index(dir.path(), TS).unwrap();
        let provider =
            PersistentAdjacencyProvider::new(dir.path().to_path_buf(), OntologyMode::Strict);

        let missing = provider.adjacency("MISSING", Direction::Out).unwrap();

        assert!(missing.is_empty());
        assert_eq!(missing.base_csr_entries_expanded(), 0);
        assert_eq!(
            provider.status("MISSING", Direction::Out),
            AdjacencyStatus::Miss,
            "the complete current manifest proves bounded empty coverage"
        );
    }

    #[test]
    fn bounded_build_failure_does_not_fall_back_to_full_scan() {
        let dir = TempDir::new().unwrap();
        write_diamond(dir.path());
        let adjacency_path = csr::adjacency_dir(dir.path());
        std::fs::create_dir_all(adjacency_path.parent().unwrap()).unwrap();
        std::fs::write(&adjacency_path, b"blocks adjacency directory creation").unwrap();
        let provider =
            PersistentAdjacencyProvider::new(dir.path().to_path_buf(), OntologyMode::Strict);

        let error = provider
            .adjacency("KNOWS", Direction::Out)
            .expect_err("bounded build failure must fail closed");

        assert!(
            error
                .to_string()
                .contains("bounded adjacency index build failed"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn undirected_csr_merge_preserves_out_before_in_ties() {
        let out = CsrIndex {
            offsets: vec![0, 1],
            edge_ids: vec![7],
            neighbor_ids: vec![1],
        };
        let inbound = CsrIndex {
            offsets: vec![0, 1],
            edge_ids: vec![7],
            neighbor_ids: vec![2],
        };
        let view = merge_undirected(out, inbound);
        assert_eq!(view.neighbors(0).to_vec(), vec![(7, 1), (7, 2)]);
        assert_eq!(view.base_csr_entries_expanded(), 0);
    }

    #[test]
    fn scan_build_status_is_building() {
        let dir = TempDir::new().unwrap();
        let provider =
            ScanBuildAdjacencyProvider::new(dir.path().to_path_buf(), OntologyMode::Strict);
        for direction in [Direction::Out, Direction::In, Direction::Undirected] {
            assert_eq!(
                provider.status("KNOWS", direction),
                AdjacencyStatus::Building
            );
            assert_eq!(provider.status("*", direction), AdjacencyStatus::Building);
        }
        assert_eq!(AdjacencyStatus::Building.as_str(), "building");
        assert_eq!(AdjacencyStatus::Hit.as_str(), "hit");
        assert_eq!(AdjacencyStatus::Miss.as_str(), "miss");
    }

    #[test]
    fn authenticated_private_cache_key_binds_generation_and_manifest() {
        let source = TempDir::new().unwrap();
        let cache = TempDir::new().unwrap();
        let first = AdjacencySourceIdentity {
            generation_uuid: Uuid::from_u128(1),
            generation_manifest_sha256: [0x11; 32],
        };
        let second = AdjacencySourceIdentity {
            generation_uuid: Uuid::from_u128(1),
            generation_manifest_sha256: [0x22; 32],
        };
        let first_provider = PersistentAdjacencyProvider::new_with_authenticated_cache(
            source.path().to_path_buf(),
            cache.path(),
            OntologyMode::Strict,
            first,
        );
        let second_provider = PersistentAdjacencyProvider::new_with_authenticated_cache(
            source.path().to_path_buf(),
            cache.path(),
            OntologyMode::Strict,
            second,
        );

        assert_eq!(first_provider.source_identity, Some(first));
        assert_eq!(second_provider.source_identity, Some(second));
        assert_ne!(first_provider.cache_dir, second_provider.cache_dir);
        let first_name = first_provider
            .cache_dir
            .file_name()
            .unwrap()
            .to_string_lossy();
        assert!(first_name.contains(&first.generation_uuid.hyphenated().to_string()));
        assert!(first_name.contains(&"11".repeat(32)));
    }
}
