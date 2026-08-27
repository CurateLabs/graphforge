//! Incremental adjacency: per-commit **delta segments** (#765).
//!
//! Each pure-append topology commit writes one tiny Parquet file
//! `indexes/adjacency/deltas/<generation>.parquet` holding exactly the edges it
//! created (possibly zero rows, for a node-only flush). The adjacency provider
//! serves a fresh view as *base CSR ⊎ a contiguous chain of segments* covering
//! `(G_base, G_cur]`, so newly-created edges are visible without a full rebuild.
//! Anything that breaks the chain — a DELETE, a crash between commit and segment
//! write, an unreadable segment, or a chain longer than [`MAX_DELTA_CHAIN`] —
//! reads as stale and falls back to the existing full-rebuild path.
//!
//! The merge ([`apply_delta_segments`]) reconstructs the base entries from the
//! loaded CSR, concatenates the (filtered) delta entries, and re-runs the
//! builder's [`csr_from_entries`](crate::adjacency::csr_from_entries). Because
//! `edge_id` is a unique surrogate, the result is byte-identical to a full
//! rebuild from `topology/` for the same edges — that equivalence is the
//! correctness contract (acceptance criterion 1), pinned by the tests below.
//!
//! Execution (#340) prefers [`overlay_delta_segments`]: a borrowable base CSR
//! plus a **bounded** replacement map over only the keys touched by the delta
//! chain. That path never copies the complete valid base CSR.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{RecordBatch, StringArray, UInt64Array};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use graphforge_core::GfError;

use crate::adjacency::{
    ALL_RELATIONS_STEM, BuildEntry, CsrIndex, CsrRow, Direction, adjacency_dir,
    adjacency_relation_key, csr_from_entries, is_adjacency_relation_key,
};

fn normalized_relation_stem(stem: &str) -> std::borrow::Cow<'_, str> {
    if stem == ALL_RELATIONS_STEM || is_adjacency_relation_key(stem) {
        std::borrow::Cow::Borrowed(stem)
    } else {
        std::borrow::Cow::Owned(adjacency_relation_key(stem))
    }
}
use crate::schemas::ADJACENCY_DELTA_SCHEMA;
use crate::staging::RewriteBatch;

/// Longest delta chain served before the index reads stale and is rebuilt.
/// Bounds both per-view merge cost and `deltas/` accumulation between rebuilds.
pub const MAX_DELTA_CHAIN: u64 = 64;

fn storage_err(e: impl std::fmt::Display) -> GfError {
    GfError::Storage(e.to_string())
}

/// One created edge in a delta segment, in creation (ascending `edge_id`) order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeltaEdge {
    /// Typed file stem, or the exploratory row's `rel_type_name`.
    pub rel_type_name: String,
    /// Edge surrogate (globally ascending across an intact chain).
    pub edge_id: u64,
    /// Source node surrogate.
    pub src_id: u64,
    /// Destination node surrogate.
    pub dst_id: u64,
}

/// The edges created by the commit that bumped the counter to `generation`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeltaSegment {
    /// Post-bump topology generation this segment is tagged with.
    pub generation: u64,
    /// Created edges, in creation order. Empty for a node-only commit.
    pub edges: Vec<DeltaEdge>,
}

/// `indexes/adjacency/deltas/` within `project_dir`.
#[must_use]
pub fn delta_dir(project_dir: &Path) -> PathBuf {
    adjacency_dir(project_dir).join("deltas")
}

/// Path of the delta segment for `generation`.
#[must_use]
pub fn delta_path(project_dir: &Path, generation: u64) -> PathBuf {
    delta_dir(project_dir).join(format!("{generation}.parquet"))
}

/// Write the segment for `generation` (creating `deltas/` if needed). A
/// zero-edge segment is still written so a node-only commit keeps the chain
/// contiguous. Atomic via temp-file + rename ([`RewriteBatch`]).
///
/// # Errors
/// [`GfError::Storage`] on any directory-create, encode, or rename failure.
pub fn write_delta_segment(
    project_dir: &Path,
    generation: u64,
    edges: &[DeltaEdge],
) -> Result<(), GfError> {
    std::fs::create_dir_all(delta_dir(project_dir)).map_err(storage_err)?;
    let rel_types: StringArray = edges
        .iter()
        .map(|e| Some(e.rel_type_name.as_str()))
        .collect();
    let edge_ids: UInt64Array = edges.iter().map(|e| e.edge_id).collect();
    let src_ids: UInt64Array = edges.iter().map(|e| e.src_id).collect();
    let dst_ids: UInt64Array = edges.iter().map(|e| e.dst_id).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&ADJACENCY_DELTA_SCHEMA),
        vec![
            Arc::new(rel_types),
            Arc::new(edge_ids),
            Arc::new(src_ids),
            Arc::new(dst_ids),
        ],
    )
    .map_err(storage_err)?;

    let mut staged = RewriteBatch::new();
    staged.stage(
        &delta_path(project_dir, generation),
        Arc::clone(&ADJACENCY_DELTA_SCHEMA),
        &batch,
    )?;
    staged.commit_at(project_dir)
}

/// Read one delta segment from disk. A missing file is an error here — callers
/// that tolerate absence ([`read_delta_chain`]) check existence first.
///
/// # Errors
/// [`GfError::Storage`] if the file is missing, unreadable, or has the wrong
/// schema.
pub fn read_delta_segment(project_dir: &Path, generation: u64) -> Result<DeltaSegment, GfError> {
    let path = delta_path(project_dir, generation);
    let file = std::fs::File::open(&path).map_err(storage_err)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(storage_err)?
        .build()
        .map_err(storage_err)?;
    let mut edges = Vec::new();
    for batch in reader {
        let batch = batch.map_err(storage_err)?;
        if batch.schema().fields() != ADJACENCY_DELTA_SCHEMA.fields() {
            return Err(GfError::Storage(format!(
                "adjacency delta {} has unexpected schema",
                path.display()
            )));
        }
        let rel_types = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| GfError::Storage("delta: rel_type_name not Utf8".to_owned()))?;
        let cols: Vec<&UInt64Array> = (1..=3)
            .map(|i| {
                batch
                    .column(i)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| GfError::Storage("delta: id column not UInt64".to_owned()))
            })
            .collect::<Result<_, _>>()?;
        for i in 0..batch.num_rows() {
            edges.push(DeltaEdge {
                rel_type_name: rel_types.value(i).to_owned(),
                edge_id: cols[0].value(i),
                src_id: cols[1].value(i),
                dst_id: cols[2].value(i),
            });
        }
    }
    Ok(DeltaSegment { generation, edges })
}

/// Read the contiguous chain of segments covering `(base, current]`.
///
/// Returns `Some(chain)` only when the chain is *intact and bounded*: every
/// generation `base+1 ..= current` has a readable segment and the span is at
/// most [`MAX_DELTA_CHAIN`]. Returns `Some(vec![])` when `current <= base`
/// (nothing to apply). Returns `None` on any gap, unreadable segment, or
/// over-cap span — all of which the provider treats as stale (full rebuild).
#[must_use]
pub fn read_delta_chain(project_dir: &Path, base: u64, current: u64) -> Option<Vec<DeltaSegment>> {
    if current <= base {
        return Some(Vec::new());
    }
    if current - base > MAX_DELTA_CHAIN {
        return None;
    }
    let mut chain = Vec::with_capacity(usize::try_from(current - base).unwrap_or(0));
    for g in (base + 1)..=current {
        if !delta_path(project_dir, g).exists() {
            return None; // gap ⇒ chain broken
        }
        match read_delta_segment(project_dir, g) {
            Ok(seg) => chain.push(seg),
            Err(_) => return None, // unreadable ⇒ chain broken
        }
    }
    Some(chain)
}

/// Best-effort removal of the segment at `generation` — for a commit that
/// bumps the counter but writes **no** segment (any DELETE / non-pure-append
/// statement). This makes "no segment here" an affirmative invariant: the chain
/// can never contain a file the bumping commit did not author, even across a
/// counter reset that left a stale file at that generation. A failure is
/// harmless — an unexpected file at `generation` only breaks the chain there,
/// forcing a (correct) rebuild.
pub fn discard_segment(project_dir: &Path, generation: u64) {
    let _ = std::fs::remove_file(delta_path(project_dir, generation));
}

/// Remove every delta segment with generation `<= up_to` (consumed by a rebuild
/// or compaction at `up_to`). Best-effort: a failed unlink leaves dead weight a
/// later prune removes, never incorrect data. Segments `> up_to` (written by a
/// concurrent append during the build) survive, so the new base + those is
/// immediately fresh.
pub fn prune_delta_segments(project_dir: &Path, up_to: u64) {
    let dir = delta_dir(project_dir);
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("parquet") {
            continue;
        }
        let parsed = path
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.parse::<u64>().ok());
        if let Some(g) = parsed
            && g <= up_to
        {
            let _ = std::fs::remove_file(&path);
        }
    }
}

/// Merge a contiguous delta `chain` onto `base` (a loaded CSR for `direction`)
/// and return the overlaid CSR — equal to a full rebuild over the base's edges
/// plus the chain's, for `stem`.
///
/// `stem == _all` takes every delta edge; a per-relation `stem` takes edges
/// whose exact relation key matches `stem`. Raw names are normalized for
/// compatibility; callers address the literal `_all` relation through its
/// encoded key because `_all` itself remains the wildcard selector.
///
/// Implementation: reconstruct the base `(src, edge, dst)` entries from the CSR,
/// concatenate the filtered delta entries, and re-run
/// [`csr_from_entries`](crate::adjacency::csr_from_entries). Since `edge_id` is
/// unique, the sorted result is identical to building from the union directly —
/// independent of input order, so it is robust even if a segment's edge_ids do
/// not strictly exceed the base's.
#[must_use]
pub fn apply_delta_segments(
    base: &CsrIndex,
    stem: &str,
    direction: Direction,
    chain: &[DeltaSegment],
) -> CsrIndex {
    let mut entries: Vec<BuildEntry> = base_entries(base, direction);
    let stem = normalized_relation_stem(stem);
    let take_all = stem == ALL_RELATIONS_STEM;
    for seg in chain {
        for e in &seg.edges {
            if take_all || adjacency_relation_key(&e.rel_type_name) == stem {
                entries.push((e.src_id, e.edge_id, e.dst_id));
            }
        }
    }
    csr_from_entries(&entries, direction)
}

/// Bounded delta overlay over a borrowed base CSR (#340).
///
/// Untouched rows are served directly from `base` (no copy). Only keys that
/// appear in the filtered delta chain allocate replacement rows — size is
/// proportional to overlay cardinality, never to the complete base edge count.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CsrDeltaOverlay {
    /// Validated base CSR; retained by reference-counted ownership.
    pub base: Arc<CsrIndex>,
    /// Complete replacement rows for keys touched by the delta chain.
    pub replaced: HashMap<u64, Vec<(u64, u64)>>,
    /// Exclusive upper bound on node ids that may have entries
    /// (`max(base.node_count, max_replaced_key + 1)`).
    pub node_extent: u64,
}

impl CsrDeltaOverlay {
    /// Checked row lookup: overlay replacement when present, else base CSR row.
    #[must_use]
    pub fn row(&self, node_id: u64) -> OverlayRow<'_> {
        if let Some(entries) = self.replaced.get(&node_id) {
            return OverlayRow::Replaced(entries.as_slice());
        }
        OverlayRow::Base(self.base.row(node_id))
    }

    /// Number of keys with an allocated replacement row.
    #[must_use]
    pub fn overlay_row_count(&self) -> u64 {
        u64::try_from(self.replaced.len()).unwrap_or(u64::MAX)
    }
}

/// A row resolved through [`CsrDeltaOverlay::row`].
#[derive(Clone, Copy, Debug)]
pub enum OverlayRow<'a> {
    /// Untouched base CSR row.
    Base(CsrRow<'a>),
    /// Delta-touched replacement (base row ∪ filtered delta entries).
    Replaced(&'a [(u64, u64)]),
}

impl OverlayRow<'_> {
    /// Number of `(edge, neighbor)` entries.
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::Base(row) => row.len(),
            Self::Replaced(entries) => entries.len(),
        }
    }

    /// Whether the row is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Entry at `index`, if in range.
    #[must_use]
    pub fn get(&self, index: usize) -> Option<(u64, u64)> {
        match self {
            Self::Base(row) => row.get(index),
            Self::Replaced(entries) => entries.get(index).copied(),
        }
    }
}

/// Build a bounded overlay that preserves the base CSR without copying it.
///
/// Semantic parity with [`apply_delta_segments`]: every key's row matches the
/// fully rebuilt CSR for that key. Empty chains return an overlay with no
/// replacements (base served as-is).
#[must_use]
pub fn overlay_delta_segments(
    base: Arc<CsrIndex>,
    stem: &str,
    direction: Direction,
    chain: &[DeltaSegment],
) -> CsrDeltaOverlay {
    let stem = normalized_relation_stem(stem);
    let take_all = stem == ALL_RELATIONS_STEM;
    let mut delta_by_key: HashMap<u64, Vec<(u64, u64)>> = HashMap::new();
    let mut max_key = 0_u64;
    let mut saw_key = false;
    for seg in chain {
        for e in &seg.edges {
            if take_all || adjacency_relation_key(&e.rel_type_name) == stem {
                let (key, neighbor) = match direction {
                    Direction::Out => (e.src_id, e.dst_id),
                    Direction::In => (e.dst_id, e.src_id),
                };
                delta_by_key
                    .entry(key)
                    .or_default()
                    .push((e.edge_id, neighbor));
                max_key = if saw_key { max_key.max(key) } else { key };
                saw_key = true;
            }
        }
    }

    let mut replaced = HashMap::with_capacity(delta_by_key.len());
    for (key, mut delta_entries) in delta_by_key {
        let mut row = Vec::new();
        if key < base.node_count() {
            let base_row = base.row(key);
            row.reserve(base_row.len() + delta_entries.len());
            row.extend(base_row.iter());
        } else {
            row.reserve(delta_entries.len());
        }
        row.append(&mut delta_entries);
        row.sort_unstable_by_key(|&(edge_id, _)| edge_id);
        replaced.insert(key, row);
    }

    let node_extent = if saw_key {
        max_key.saturating_add(1).max(base.node_count())
    } else {
        base.node_count()
    };

    CsrDeltaOverlay {
        base,
        replaced,
        node_extent,
    }
}

/// Reconstruct the `(src, edge, dst)` build entries a CSR for `direction` was
/// built from: an `out` CSR is keyed by `src` with `dst` neighbors, an `in` CSR
/// by `dst` with `src` neighbors.
fn base_entries(base: &CsrIndex, direction: Direction) -> Vec<BuildEntry> {
    let mut entries = Vec::with_capacity(base.edge_ids.len());
    for key in 0..base.node_count() {
        let lo = base.offsets[usize::try_from(key).unwrap_or(0)];
        let hi = base.offsets[usize::try_from(key + 1).unwrap_or(0)];
        for j in lo..hi {
            let j = usize::try_from(j).unwrap_or(0);
            let (edge, neighbor) = (base.edge_ids[j], base.neighbor_ids[j]);
            entries.push(match direction {
                Direction::Out => (key, edge, neighbor), // key=src, neighbor=dst
                Direction::In => (neighbor, edge, key),  // key=dst, neighbor=src
            });
        }
    }
    entries
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn seg(generation: u64, edges: &[(&str, u64, u64, u64)]) -> DeltaSegment {
        DeltaSegment {
            generation,
            edges: edges
                .iter()
                .map(|&(rel, edge_id, src_id, dst_id)| DeltaEdge {
                    rel_type_name: rel.to_owned(),
                    edge_id,
                    src_id,
                    dst_id,
                })
                .collect(),
        }
    }

    /// The merge equals a full rebuild for every (stem, direction) — the #765
    /// acceptance-1 contract, over a fixture with parallel edges, a self-loop,
    /// a new node beyond the base, and an unusable relation name.
    #[test]
    fn apply_equals_full_rebuild() {
        // Base edges (src, edge, dst), edge_ids 1..=5.
        let base_edges: Vec<BuildEntry> = vec![
            (0, 1, 1),
            (0, 2, 1), // parallel edge 0->1
            (1, 3, 2),
            (2, 4, 0),
            (2, 5, 2), // self-loop 2->2
        ];
        // Delta edges, edge_ids 6..=9 (strictly after the base), incl. a new
        // node 3 and an unusable relation name.
        let chain = vec![
            seg(8, &[("KNOWS", 6, 1, 3), ("KNOWS", 7, 3, 0)]),
            seg(9, &[("OWNS", 8, 0, 2), ("../evil", 9, 3, 1)]),
        ];
        let delta_entries: Vec<BuildEntry> = vec![(1, 6, 3), (3, 7, 0), (0, 8, 2), (3, 9, 1)];

        for direction in [Direction::Out, Direction::In] {
            // _all overlay: every delta edge participates.
            let base_all = csr_from_entries(&base_edges, direction);
            let mut all = base_edges.clone();
            all.extend_from_slice(&delta_entries);
            let expected_all = csr_from_entries(&all, direction);
            assert_eq!(
                apply_delta_segments(&base_all, ALL_RELATIONS_STEM, direction, &chain),
                expected_all,
                "_all {direction:?}"
            );

            // Per-relation KNOWS overlay: only KNOWS delta edges (6, 7); the
            // unusable "../evil" name never reaches a per-rel stem.
            let base_knows: Vec<BuildEntry> = vec![(0, 1, 1), (0, 2, 1), (1, 3, 2)];
            let base_knows_csr = csr_from_entries(&base_knows, direction);
            let mut knows = base_knows.clone();
            knows.push((1, 6, 3));
            knows.push((3, 7, 0));
            let expected_knows = csr_from_entries(&knows, direction);
            assert_eq!(
                apply_delta_segments(&base_knows_csr, "KNOWS", direction, &chain),
                expected_knows,
                "KNOWS {direction:?}"
            );
        }
    }

    #[test]
    fn empty_chain_returns_the_base_unchanged() {
        let base = csr_from_entries(&[(0, 1, 1), (1, 2, 0)], Direction::Out);
        assert_eq!(
            apply_delta_segments(&base, ALL_RELATIONS_STEM, Direction::Out, &[]),
            base
        );
        let overlay = overlay_delta_segments(
            Arc::new(base.clone()),
            ALL_RELATIONS_STEM,
            Direction::Out,
            &[],
        );
        assert!(overlay.replaced.is_empty());
        assert_eq!(overlay.node_extent, base.node_count());
        assert_eq!(overlay.base.as_ref(), &base);
    }

    #[test]
    fn overlay_matches_full_rebuild_without_copying_untouched_base_rows() {
        let base_edges: Vec<BuildEntry> =
            vec![(0, 1, 1), (0, 2, 1), (1, 3, 2), (2, 4, 0), (2, 5, 2)];
        let chain = vec![
            seg(8, &[("KNOWS", 6, 1, 3), ("KNOWS", 7, 3, 0)]),
            seg(9, &[("OWNS", 8, 0, 2), ("../evil", 9, 3, 1)]),
        ];
        for direction in [Direction::Out, Direction::In] {
            let base = Arc::new(csr_from_entries(&base_edges, direction));
            let rebuilt = apply_delta_segments(&base, ALL_RELATIONS_STEM, direction, &chain);
            let overlay =
                overlay_delta_segments(Arc::clone(&base), ALL_RELATIONS_STEM, direction, &chain);
            // Overlay keys are only those touched by filtered deltas — never |E|.
            assert!(overlay.overlay_row_count() < base.edge_count());
            assert!(
                Arc::ptr_eq(&overlay.base, &base),
                "overlay must retain the original base CSR allocation"
            );
            let extent = rebuilt.node_count().max(overlay.node_extent);
            for node in 0..extent {
                let expected = rebuilt.row(node);
                let got = overlay.row(node);
                assert_eq!(got.len(), expected.len(), "node {node} {direction:?}");
                for i in 0..expected.len() {
                    assert_eq!(got.get(i), expected.get(i), "node {node} idx {i}");
                }
            }
            // Untouched base key 2 (self-loop + reverse) keeps a Base borrow when
            // no delta edge touches key 2 under this direction.
            let key_two_touched = overlay.replaced.contains_key(&2);
            if !key_two_touched {
                assert!(matches!(overlay.row(2), OverlayRow::Base(_)));
            }
        }
    }

    #[test]
    fn segment_round_trips_including_empty() {
        let dir = TempDir::new().unwrap();
        let edges = vec![
            DeltaEdge {
                rel_type_name: "KNOWS".into(),
                edge_id: 6,
                src_id: 1,
                dst_id: 3,
            },
            DeltaEdge {
                rel_type_name: "OWNS".into(),
                edge_id: 7,
                src_id: 0,
                dst_id: 2,
            },
        ];
        write_delta_segment(dir.path(), 6, &edges).unwrap();
        assert_eq!(read_delta_segment(dir.path(), 6).unwrap().edges, edges);

        // Node-only commit: an empty segment keeps the chain contiguous.
        write_delta_segment(dir.path(), 7, &[]).unwrap();
        assert!(read_delta_segment(dir.path(), 7).unwrap().edges.is_empty());
    }

    #[test]
    fn chain_is_some_only_when_contiguous_and_bounded() {
        let dir = TempDir::new().unwrap();
        // Generations 6 and 7 present.
        write_delta_segment(dir.path(), 6, &[]).unwrap();
        write_delta_segment(dir.path(), 7, &[]).unwrap();

        assert_eq!(read_delta_chain(dir.path(), 5, 5).unwrap().len(), 0); // none needed
        assert_eq!(read_delta_chain(dir.path(), 5, 7).unwrap().len(), 2); // contiguous
        assert!(read_delta_chain(dir.path(), 5, 8).is_none()); // gap at 8
        assert!(read_delta_chain(dir.path(), 4, 7).is_none()); // gap at 5
        assert!(read_delta_chain(dir.path(), 0, MAX_DELTA_CHAIN + 1).is_none()); // over cap
    }

    #[test]
    fn prune_removes_only_consumed_segments() {
        let dir = TempDir::new().unwrap();
        for g in 6..=9 {
            write_delta_segment(dir.path(), g, &[]).unwrap();
        }
        prune_delta_segments(dir.path(), 7);
        assert!(!delta_path(dir.path(), 6).exists());
        assert!(!delta_path(dir.path(), 7).exists());
        assert!(delta_path(dir.path(), 8).exists()); // written after the build stamp
        assert!(delta_path(dir.path(), 9).exists());
        // Idempotent.
        prune_delta_segments(dir.path(), 7);
        assert!(delta_path(dir.path(), 8).exists());
    }
}
