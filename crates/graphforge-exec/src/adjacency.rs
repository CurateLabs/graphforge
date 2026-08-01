//! The single adjacency abstraction (ADR 0005, R-ADJ-4) — `AdjacencyProvider`.
//!
//! Both adjacency consumers obtain their view through this trait: the Cypher
//! variable-length traversal (`VarLenExpandExec`, #762) and the analyst-verb
//! `export_adjacency` adapter (M18 #610). After #762 there is exactly **one**
//! adjacency implementation in the codebase.
//!
//! Two implementations exist:
//!
//! - [`ScanBuildAdjacencyProvider`] — reads the typed edge tables and builds
//!   the view in memory on every call (the behavior of the retired private
//!   `build_adjacency`); the universal fallback.
//! - [`PersistentAdjacencyProvider`] (#761) — serves from the on-disk CSR
//!   index under `indexes/adjacency/` when it is fresh (manifest
//!   `topology_generation` matches the project counter), lazily rebuilds a
//!   stale index, and falls back to scan-build whenever the index cannot
//!   serve a key. A stale, corrupt, or missing index can only cost speed,
//!   never correctness. The adjacency-aware lowering rule is #763.
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
    self as csr, ALL_RELATIONS_STEM, AdjacencyManifestRow, CsrIndex, build_adjacency_index,
};
use graphforge_storage::adjacency_delta::{DeltaSegment, apply_delta_segments, read_delta_chain};
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
    /// relation, or a missing CSR file. The request scan-builds (and, when
    /// the whole index was stale, lazily rebuilds it).
    Miss,
    /// No index capability for this request: `indexes/adjacency/` absent, a
    /// scan-build-only provider, or the typed-mode `"*"` bypass.
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

/// Surrogate-keyed adjacency view consumed by traversal.
///
/// The representation is private. Today it is the scan-built map the retired
/// `build_adjacency` produced; #761 may back the same contract with a
/// disk-loaded CSR (offsets + interleaved entries) — both serve
/// [`neighbors`](Self::neighbors) as a contiguous slice, so consumers never
/// change.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Adjacency {
    map: HashMap<u64, Vec<(u64, u64)>>,
}

impl Adjacency {
    /// The `(edge_id, neighbor_node_id)` entries for `node_id`, in edge-file
    /// row order (for `Undirected`, each edge row contributes its src-keyed
    /// and dst-keyed entries in that order). BFS emission order observably
    /// depends on this ordering.
    ///
    /// Unknown, isolated, or deleted ids yield `&[]` — never a panic. (#761's
    /// dense CSR backing must preserve this for out-of-range ids.)
    #[must_use]
    pub fn neighbors(&self, node_id: u64) -> &[(u64, u64)] {
        self.map.get(&node_id).map_or(&[], Vec::as_slice)
    }

    /// Whether the view contains no entries at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Iterate the surrogate-keyed rows in unspecified map order.
    ///
    /// Consumers that expose deterministic behavior must sort the yielded
    /// node ids. The entries within each row retain provider order.
    pub(crate) fn rows(&self) -> impl Iterator<Item = (u64, &[(u64, u64)])> {
        self.map
            .iter()
            .map(|(&node_id, entries)| (node_id, entries.as_slice()))
    }
}

/// Single adjacency abstraction (ADR 0005): implementations decide *how* a
/// view is produced (scan-build now; disk-loaded CSR with scan fallback in
/// #761) — consumers only see [`Adjacency`].
pub trait AdjacencyProvider: Send + Sync {
    /// The adjacency view for (`rel_type_name`, `direction`).
    ///
    /// `"*"` means all relation types (#823): the per-row `rel_type_name`
    /// filter is skipped and every relation's edges are unioned — served by the
    /// `_all` CSR union (a `Hit`) when the index is fresh, otherwise by a union
    /// scan-build over `read_edges(dir, "*", mode)` (which itself unions every
    /// `topology/edges/*.parquet` in Strict/Advisory).
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
    Ok(Adjacency { map: adj })
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
/// `topology_generation`, lazily rebuilds a stale index, and falls back to
/// scan-build whenever the index cannot serve a key — a stale, corrupt, or
/// missing index only ever costs speed, never correctness.
///
/// One instance lives per [`ExecutionSession`](crate::ExecutionSession)
/// (= per query); loaded views are cached per `(stem, direction)` so a
/// multi-expand query loads each CSR once. Facade-level cross-query caching
/// is a planned later lift (the `Arc<Adjacency>` return type already
/// supports it).
pub struct PersistentAdjacencyProvider {
    dir: PathBuf,
    /// Scan-build fallback, fed the ORIGINAL relation name so per-row relation
    /// filtering still applies (the union read serves the typed `"*"` wildcard).
    scan: ScanBuildAdjacencyProvider,
    /// Lazily-read index state; refreshed after a successful lazy rebuild.
    state: Mutex<Option<IndexState>>,
    /// Loaded views per `(stem, direction)`.
    cache: Mutex<HashMap<(String, Direction), Arc<Adjacency>>>,
}

impl PersistentAdjacencyProvider {
    /// A provider over the project at `dir` in ontology `mode`.
    #[must_use]
    pub fn new(dir: PathBuf, mode: OntologyMode) -> Self {
        Self {
            scan: ScanBuildAdjacencyProvider::new(dir.clone(), mode),
            dir,
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
            rel_type_name.to_owned()
        }
    }

    /// The current index state, read once and memoized.
    fn state(&self) -> IndexState {
        let mut guard = self.state.lock().expect("adjacency state lock");
        guard
            .get_or_insert_with(|| Self::read_state(&self.dir))
            .clone()
    }

    fn read_state(dir: &std::path::Path) -> IndexState {
        if !csr::adjacency_dir(dir).exists() {
            return IndexState::Absent;
        }
        let Ok(generation) = read_topology_generation(dir) else {
            return IndexState::Unreadable;
        };
        let Ok(rows) = csr::read_manifest(dir) else {
            return IndexState::Unreadable;
        };
        // The base generation the CSRs were built at — uniform across rows on a
        // clean build. An empty manifest (torn build) or rows that disagree are
        // stale (rebuild repairs them), never served.
        let base = rows.first().map(|r| r.topology_generation);
        let uniform = base.is_some_and(|b| rows.iter().all(|r| r.topology_generation == b));
        let (fresh, deltas) = match base {
            // base == counter: exact match, no overlay (the #761 fast path).
            Some(b) if uniform && b == generation => (true, Vec::new()),
            // base < counter: serveable iff an intact, bounded delta chain
            // (#765) covers (base, counter]; otherwise stale ⇒ rebuild.
            Some(b) if uniform && b < generation => match read_delta_chain(dir, b, generation) {
                Some(chain) => (true, chain),
                None => (false, Vec::new()),
            },
            // Empty / torn manifest, or an index newer than the counter
            // (anomalous, e.g. a counter reset): stale.
            _ => (false, Vec::new()),
        };
        IndexState::Ready {
            fresh,
            generation,
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

    /// Load the view for (`stem`, `direction`) from the CSR file(s), overlaying
    /// the delta chain (#765) when one is present (`deltas` non-empty).
    fn load(
        &self,
        stem: &str,
        direction: Direction,
        rows: &[AdjacencyManifestRow],
        deltas: &[DeltaSegment],
    ) -> Result<Adjacency, GfError> {
        let overlaid = |d: csr::Direction| -> Result<CsrIndex, GfError> {
            let base = csr::read_csr(&csr::csr_path(&self.dir, stem, d))?;
            if deltas.is_empty() {
                return Ok(base);
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
            Ok(apply_delta_segments(&base, stem, d, deltas))
        };
        match direction {
            Direction::Out => Ok(adjacency_from_csr(&overlaid(csr::Direction::Out)?)),
            Direction::In => Ok(adjacency_from_csr(&overlaid(csr::Direction::In)?)),
            Direction::Undirected => Ok(merge_undirected(
                &overlaid(csr::Direction::Out)?,
                &overlaid(csr::Direction::In)?,
            )),
        }
    }

    /// Lazily rebuild the index, refresh the memoized state, and serve from
    /// the fresh files; any failure falls back to scan-build (a build problem
    /// must never fail the query).
    fn rebuild_and_serve(
        &self,
        rel_type_name: &str,
        stem: &str,
        direction: Direction,
    ) -> Result<Arc<Adjacency>, GfError> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| i64::try_from(d.as_micros()).unwrap_or(i64::MAX));
        match build_adjacency_index(&self.dir, now) {
            Ok(rows) => {
                let covered = Self::rows_cover(&rows, stem, direction);
                // Index writes don't bump the topology counter, so re-read it
                // for the stamp — a missed stamp would make the next
                // revalidate() spuriously drop this fresh rebuild.
                let generation = rows.first().map_or_else(
                    || read_topology_generation(&self.dir).unwrap_or(0),
                    |r| r.topology_generation,
                );
                // A fresh rebuild has no overlay: the new base IS `generation`
                // and the builder pruned the consumed segments (#765).
                let view = if covered {
                    self.load(stem, direction, &rows, &[]).ok()
                } else {
                    None
                };
                *self.state.lock().expect("adjacency state lock") = Some(IndexState::Ready {
                    fresh: true,
                    generation,
                    rows,
                    deltas: Arc::new(Vec::new()),
                });
                if let Some(view) = view {
                    return Ok(self.cache_view(stem, direction, view));
                }
                self.scan.adjacency(rel_type_name, direction)
            }
            Err(_) => self.scan.adjacency(rel_type_name, direction),
        }
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
                ..
            }) => !read_topology_generation(&self.dir).is_ok_and(|g| g == *generation),
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
            IndexState::Absent | IndexState::Unreadable => {
                // A streaming ExpandExec may request the same view once per
                // input batch. Scan-build exactly once per session/query and
                // cache it just like a CSR view; writes/revalidation clear the
                // cache before a changed topology can be observed (#1248).
                let view = self.scan.adjacency(rel_type_name, direction)?;
                Ok(self.cache_shared_view(&stem, direction, view))
            }
            IndexState::Ready {
                fresh: true,
                rows,
                deltas,
                ..
            } => {
                if !Self::rows_cover(&rows, &stem, direction) {
                    // A FRESH index with no row for this relation: a relation
                    // born only in the delta chain has no base CSR to overlay,
                    // and rebuilding cannot add an unknown/unusable stem either,
                    // so scan-build without rebuild (the union `_all` still
                    // carries those rows for exploratory `*`). Correct, slower.
                    return self.scan.adjacency(rel_type_name, direction);
                }
                match self.load(&stem, direction, &rows, &deltas) {
                    Ok(view) => Ok(self.cache_view(&stem, direction, view)),
                    // CSR missing/corrupt, or the torn-read count guard tripped:
                    // one lazy rebuild repairs the index.
                    Err(_) => self.rebuild_and_serve(rel_type_name, &stem, direction),
                }
            }
            IndexState::Ready { fresh: false, .. } => {
                self.rebuild_and_serve(rel_type_name, &stem, direction)
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
                let files_exist = |d: csr::Direction| csr::csr_path(&self.dir, &stem, d).exists();
                let present = Self::rows_cover(&rows, &stem, direction)
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

/// Convert a directed CSR into the traversal view. Empty rows are skipped so
/// the result is `PartialEq`-identical to a scan-built view (scan-build never
/// inserts empty entry vectors).
///
/// Bounds safety: callers only pass [`CsrIndex`]es produced by
/// `graphforge_storage::adjacency::read_csr`, which validates the invariants
/// (offsets `[0]`-rooted, monotone, final offset == target lengths) before
/// returning — so every slice below is in bounds and the `usize` conversions
/// cannot fail (offsets never exceed `Vec::len`, which is a `usize`). A
/// malformed file errors inside `read_csr`, taking the rebuild/fallback path
/// in the provider, never a panic here.
fn adjacency_from_csr(index: &CsrIndex) -> Adjacency {
    let mut map: HashMap<u64, Vec<(u64, u64)>> = HashMap::new();
    for (node, window) in index.offsets.windows(2).enumerate() {
        let (start, end) = (
            usize::try_from(window[0]).expect("CSR offset exceeds usize"),
            usize::try_from(window[1]).expect("CSR offset exceeds usize"),
        );
        if start == end {
            continue;
        }
        map.insert(
            node as u64,
            index.edge_ids[start..end]
                .iter()
                .copied()
                .zip(index.neighbor_ids[start..end].iter().copied())
                .collect(),
        );
    }
    Adjacency { map }
}

/// Merge a node's out and in CSR rows into the undirected view, interleaving
/// ascending by `edge_id` with **out before in on ties** — the exact order
/// scan-build produces (edge files are edge_id-ascending; each row pushes its
/// src-keyed entry, then for a self-loop its dst-keyed entry).
///
/// Bounds safety: same `read_csr`-validated invariants as
/// [`adjacency_from_csr`]; `row()` additionally guards `node` against the
/// CSR's own `node_count`.
fn merge_undirected(out: &CsrIndex, inbound: &CsrIndex) -> Adjacency {
    let row = |index: &CsrIndex, node: u64| -> (usize, usize) {
        if node >= index.node_count() {
            return (0, 0);
        }
        let i = usize::try_from(node).expect("node id exceeds usize");
        (
            usize::try_from(index.offsets[i]).expect("CSR offset exceeds usize"),
            usize::try_from(index.offsets[i + 1]).expect("CSR offset exceeds usize"),
        )
    };
    let mut map: HashMap<u64, Vec<(u64, u64)>> = HashMap::new();
    for node in 0..out.node_count().max(inbound.node_count()) {
        let (mut o, o_end) = row(out, node);
        let (mut i, i_end) = row(inbound, node);
        if o == o_end && i == i_end {
            continue;
        }
        let mut entries = Vec::with_capacity((o_end - o) + (i_end - i));
        while o < o_end || i < i_end {
            let take_out = i >= i_end || (o < o_end && out.edge_ids[o] <= inbound.edge_ids[i]);
            if take_out {
                entries.push((out.edge_ids[o], out.neighbor_ids[o]));
                o += 1;
            } else {
                entries.push((inbound.edge_ids[i], inbound.neighbor_ids[i]));
                i += 1;
            }
        }
        map.insert(node, entries);
    }
    Adjacency { map }
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
        assert_eq!(out.neighbors(d), &[(6, d)]);
        // ...and two Undirected entries under d (src-keyed + dst-keyed), after
        // the two incoming diamond edges b→d, c→d.
        let undirected = provider.adjacency("KNOWS", Direction::Undirected).unwrap();
        assert_eq!(undirected.neighbors(d).len(), 4);
        let in_view = provider.adjacency("KNOWS", Direction::In).unwrap();
        // b's incoming: a→b and the parallel a→b.
        assert_eq!(in_view.neighbors(b), &[(1, a), (5, a)]);
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
        assert_eq!(view.neighbors(ids[0]), &[(1, ids[1])], "OWNS row excluded");
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
            view.neighbors(ids[0]),
            &[(1, ids[1]), (2, ids[2])],
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
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
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
        assert_eq!(view.neighbors(ids[1]), &[], "deleted id yields empty");
        assert_eq!(view.neighbors(ids[2]), &[(3, ids[3])], "survivor intact");
    }

    #[test]
    fn absent_edges_dir_yields_empty_adjacency() {
        let dir = TempDir::new().unwrap();
        let provider =
            ScanBuildAdjacencyProvider::new(dir.path().to_path_buf(), OntologyMode::Strict);
        let view = provider.adjacency("KNOWS", Direction::Out).unwrap();
        assert!(view.is_empty());
        assert_eq!(view.neighbors(1), &[]);
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
            view.neighbors(hub_id),
            &[(1, spoke_ids[0]), (2, spoke_ids[1]), (3, spoke_ids[2])],
            "entries in edge-file row order"
        );
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
}
