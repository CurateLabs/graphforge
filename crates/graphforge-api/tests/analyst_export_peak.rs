//! Peak live heap of an analyst verb over a persisted multi-shard CSR (#1974),
//! through the public facade.
//!
//! A counting global allocator measures every live byte the process holds while
//! one `rank` (or `paths`) call runs. The call exports the graph, runs a kernel
//! over the export and shapes the result, so the bound states each of those
//! terms (see [`Shape::bound`]). Nothing here reaches into the engine: the
//! export is exercised only by calling the verb.
//!
//! Cases that must fail on a regressed export:
//! - a tiny label subset of a large view (an export that reserves the whole
//!   view's entry count before filtering holds `16 * view entries` it keeps no
//!   use for);
//! - a hub row that spans many shards, under an undirected merge;
//! - a decoded-shard cache that keeps what the walk read.
//!
//! The weighted case is a scaling test, because the property stream's window is
//! a fixed constant of the storage reader that no formula here should restate:
//! two graphs of the same shape with `E` four times apart may differ in peak by
//! no more than the per-entry cost of the export.
#![allow(
    unsafe_code,
    reason = "a counting global allocator wraps System and only observes sizes"
)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use arrow::array::{ArrayRef, FixedSizeBinaryBuilder, Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use graphforge_api::{
    BulkInputKind, ExecutionResourcePolicy, GraphForge, GraphForgeOptions, ImportSessionLimits,
    NodeSelector, OperationId, ResourcePolicyMode,
};
use graphforge_core::algorithms::{PathAlgorithm, RankAlgorithm};
use graphforge_core::{PathsOptions, RankOptions};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use uuid::Uuid;

struct Counting;
static CURRENT: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

fn grow(bytes: usize) {
    let now = CURRENT.fetch_add(bytes, Ordering::Relaxed) + bytes;
    PEAK.fetch_max(now, Ordering::Relaxed);
}

// SAFETY: every method defers to `System`, which upholds the `GlobalAlloc`
// contract; the counters only observe sizes.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            grow(layout.size());
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            grow(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        CURRENT.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new: usize) -> *mut u8 {
        let moved = unsafe { System.realloc(pointer, layout, new) };
        if !moved.is_null() {
            // A reallocation may copy, so the old and the new block are both
            // live at once; count it that way instead of assuming it is in place.
            grow(new);
            CURRENT.fetch_sub(layout.size(), Ordering::Relaxed);
        }
        moved
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// The counters are process-wide, so the tests here do not overlap.
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Run `operation`; return its result and the most bytes it held live at once
/// above where it started.
fn peak_live_bytes<T>(operation: impl FnOnce() -> T) -> (T, usize) {
    let baseline = CURRENT.load(Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);
    let result = operation();
    (
        result,
        PEAK.load(Ordering::Relaxed).saturating_sub(baseline),
    )
}

fn node_uuid(index: usize) -> Uuid {
    v7(index as u128 + 1)
}

fn edge_uuid(index: usize) -> Uuid {
    v7(0x1_0000_0000 + index as u128)
}

fn v7(value: u128) -> Uuid {
    Uuid::from_u128((value << 80) | (0x7 << 76) | (0x2 << 62) | value)
}

/// A generated graph: `nodes` nodes of which every `rare_every`th is labeled
/// `Rare` and the rest `Entity`; `degree` out-edges per node; and `hub_degree`
/// extra out-edges from node 0.
#[derive(Clone, Copy)]
struct Spec {
    nodes: usize,
    degree: usize,
    rare_every: usize,
    hub_degree: usize,
    weighted: bool,
    /// Node `i` points at `i + 1 .. i + degree`, so shortest paths are long.
    ring: bool,
}

impl Spec {
    fn rare(&self, node: usize) -> bool {
        self.rare_every > 0 && node.is_multiple_of(self.rare_every)
    }

    fn edges(&self) -> Vec<(usize, usize)> {
        let mut edges = Vec::new();
        for source in 0..self.nodes {
            for step in 0..self.degree {
                edges.push((
                    source,
                    if self.ring {
                        (source + step + 1) % self.nodes
                    } else {
                        (source * 7 + step * 131 + 1) % self.nodes
                    },
                ));
            }
        }
        for step in 0..self.hub_degree {
            edges.push((0, (step * 32 + 32) % self.nodes));
        }
        edges
    }
}

fn uuids(values: impl Iterator<Item = Uuid>, rows: usize) -> ArrayRef {
    let mut builder = FixedSizeBinaryBuilder::with_capacity(rows, 16);
    for value in values {
        builder.append_value(value.as_bytes()).unwrap();
    }
    Arc::new(builder.finish())
}

fn write_parquet(path: &Path, batches: &[RecordBatch]) {
    let mut out =
        ArrowWriter::try_new(File::create(path).unwrap(), batches[0].schema(), None).unwrap();
    for batch in batches {
        out.write(batch).unwrap();
    }
    out.close().unwrap();
}

const WRITE_ROWS: usize = 32 * 1024;

/// Import `spec` into a new durable project and return it, closed.
fn build_project(spec: Spec) -> (tempfile::TempDir, PathBuf) {
    let directory = tempfile::tempdir().unwrap();
    let sources = directory.path().join("sources");
    fs::create_dir(&sources).unwrap();
    let node_schema = Arc::new(Schema::new(vec![
        Field::new("node_uuid", DataType::FixedSizeBinary(16), true),
        Field::new("label", DataType::Utf8, false),
    ]));
    let node_batches: Vec<RecordBatch> = (0..spec.nodes)
        .step_by(WRITE_ROWS)
        .map(|start| {
            let end = (start + WRITE_ROWS).min(spec.nodes);
            let labels: Vec<&str> = (start..end)
                .map(|node| if spec.rare(node) { "Rare" } else { "Entity" })
                .collect();
            RecordBatch::try_new(
                Arc::clone(&node_schema),
                vec![
                    uuids((start..end).map(node_uuid), end - start),
                    Arc::new(StringArray::from(labels)),
                ],
            )
            .unwrap()
        })
        .collect();
    write_parquet(&sources.join("nodes.parquet"), &node_batches);

    let mut fields = vec![
        Field::new("edge_uuid", DataType::FixedSizeBinary(16), true),
        Field::new("rel_type", DataType::Utf8, false),
        Field::new("source_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("target_uuid", DataType::FixedSizeBinary(16), false),
    ];
    if spec.weighted {
        fields.push(Field::new("cost", DataType::Float64, true));
    }
    let edge_schema = Arc::new(Schema::new(fields));
    let edges = spec.edges();
    let edge_batches: Vec<RecordBatch> = (0..edges.len())
        .step_by(WRITE_ROWS)
        .map(|start| {
            let end = (start + WRITE_ROWS).min(edges.len());
            let rows = end - start;
            let mut columns: Vec<ArrayRef> = vec![
                uuids((start..end).map(edge_uuid), rows),
                Arc::new(StringArray::from(vec!["LINK"; rows])),
                uuids(edges[start..end].iter().map(|&(s, _)| node_uuid(s)), rows),
                uuids(edges[start..end].iter().map(|&(_, t)| node_uuid(t)), rows),
            ];
            if spec.weighted {
                // The edge from node 0 to node 1 is the cheapest by far, so a
                // shortest path to node 1 ends at the first pop.
                columns.push(Arc::new(Float64Array::from(
                    (start..end)
                        .map(|edge| {
                            if edge == 0 {
                                1.0
                            } else {
                                2.0 + (edge % 15) as f64
                            }
                        })
                        .collect::<Vec<_>>(),
                )));
            }
            RecordBatch::try_new(Arc::clone(&edge_schema), columns).unwrap()
        })
        .collect();
    write_parquet(&sources.join("edges.parquet"), &edge_batches);

    let project = directory.path().join("project");
    fs::create_dir(&project).unwrap();
    let graph = GraphForge::new(project.to_str()).unwrap();
    let mut session = graph
        .begin_import_session(
            OperationId(v7(7)),
            ImportSessionLimits {
                batch_rows: WRITE_ROWS,
                ..ImportSessionLimits::default()
            },
        )
        .unwrap();
    session
        .register_parquet(BulkInputKind::Node, &sources.join("nodes.parquet"))
        .unwrap();
    session
        .register_parquet(BulkInputKind::Edge, &sources.join("edges.parquet"))
        .unwrap();
    session.validate(&graph).unwrap();
    session.commit(&graph, None).unwrap();
    drop(graph);
    fs::remove_dir_all(&sources).unwrap();
    (directory, project)
}

/// The facade derives a CSR shard's cap from the memory budget, whose floor is
/// 16 MiB: `budget / 192` entries.
const BUDGET: u64 = 16 * 1024 * 1024;
const SHARD_EDGES: usize = (BUDGET / 192) as usize;

/// Open `project` under [`BUDGET`] and build its multi-shard adjacency index.
fn open_indexed(project: &Path) -> GraphForge {
    let forge = GraphForge::new_with_options(
        project.to_str(),
        GraphForgeOptions {
            resource: ExecutionResourcePolicy {
                mode: ResourcePolicyMode::Explicit,
                memory_budget_bytes: Some(BUDGET),
                tokio_worker_threads: Some(1),
                compute_threads: Some(1),
                io_concurrency: Some(1),
                ..ExecutionResourcePolicy::default()
            },
            ..GraphForgeOptions::default()
        },
    )
    .unwrap();
    forge.index_adjacency().unwrap();
    let inspection = forge.inspect_adjacency().unwrap();
    assert_eq!(
        inspection.state,
        graphforge_storage::adjacency::AdjacencyFreshnessState::Current
    );
    forge
}

/// Bytes the named columns of one row group hold decoded, at most, plus one
/// decoded batch of `batch_row_bytes` per row: what a streaming projection of
/// those columns holds beyond what it hands on.
fn stream_window(project: &Path, kind: &str, columns: &[&str], batch_row_bytes: usize) -> usize {
    let mut window = 0;
    let mut pending = vec![project.to_path_buf()];
    while let Some(path) = pending.pop() {
        for entry in fs::read_dir(&path).into_iter().flatten() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|ext| ext == "parquet")
                && path
                    .to_string_lossy()
                    .contains("graphforge-graph-workspace")
                && path
                    .to_string_lossy()
                    .contains(&format!("/topology/{kind}/"))
            {
                let builder =
                    ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap()).unwrap();
                for group in builder.metadata().row_groups() {
                    let chunks: i64 = group
                        .columns()
                        .iter()
                        .filter(|column| {
                            columns.contains(&column.column_path().parts()[0].as_str())
                        })
                        .map(|column| column.uncompressed_size())
                        .sum();
                    let rows = usize::try_from(group.num_rows()).unwrap();
                    window = window.max(
                        usize::try_from(chunks).unwrap()
                            + batch_row_bytes
                                * rows.min(graphforge_storage::EXPORT_STREAM_BATCH_ROWS),
                    );
                }
            }
        }
    }
    window
}

/// What one analyst call over a persisted view is expected to hold.
struct Shape {
    /// Nodes in the whole graph (the CSR's extent).
    all_nodes: usize,
    /// Nodes the label selects.
    selected: usize,
    /// Adjacency entries the export retains, both orientations if undirected.
    entries: usize,
    undirected: bool,
    /// Whether the selected ids have gaps, so a rank table is kept.
    sparse: bool,
    /// Longest CSR row of the view.
    widest_row: usize,
    /// Out-entries per node, at least, for sizing a shard's rows.
    min_degree: usize,
    node_window: usize,
    edge_window: usize,
}

/// Allocator bookkeeping, the facade, and small structures that do not grow
/// with the graph.
const SLACK: usize = 256 * 1024;

impl Shape {
    /// Bytes one cursor holds: a shard's encoded file and decoded arrays. A
    /// shard has at most `SHARD_EDGES` entries over at most
    /// `SHARD_EDGES / min_degree + 2` rows.
    fn shard_bytes(&self) -> usize {
        let rows = SHARD_EDGES / self.min_degree.max(1) + 2;
        2 * (rows.div_ceil(8) + 8 * (rows + 1) + 3 * SHARD_EDGES.div_ceil(8) + 16 * SHARD_EDGES)
    }

    /// The most live heap the call may hold, in bytes: the larger of two phases.
    ///
    /// Export phase, at its widest while edge UUIDs are resolved:
    /// - offsets `4(n+1)` and node UUIDs `16n`;
    /// - for a rank table: ids `8n` and `4 * all_nodes`;
    /// - neighbors `4E`, edge ids `8E`, edge UUIDs `16E`, the edge-id-sorted
    ///   permutation `4E` and its `E/8` filled bitmap;
    /// - one cursor per direction, each holding a shard and a spanning hub row
    ///   as two `u64` columns (`16 * widest_row`), and the undirected merge's
    ///   row-sized pair (`16 * widest_row`);
    /// - the larger of the node and edge Parquet stream windows.
    ///
    /// Kernel phase, with the permutation gone and the identity kept:
    /// - offsets, UUIDs and rank terms as above, neighbors `4E` and identity `24E`;
    /// - the node-id slice a kernel asks for `8n`;
    /// - the result: builders, the coalesced batch, the property-materialized
    ///   batch and the returned batch, each `24n`.
    fn bound(&self) -> usize {
        let n = self.selected;
        let e = self.entries;
        let cursors = if self.undirected { 2 } else { 1 };
        let rank = if self.sparse {
            8 * n + 4 * self.all_nodes
        } else {
            0
        };
        let rows = 4 * (n + 1) + 16 * n + rank;
        let hub = 16 * self.widest_row * cursors
            + if self.undirected {
                16 * self.widest_row
            } else {
                0
            };
        let export = rows
            + 32 * e
            + e / 8
            + cursors * self.shard_bytes()
            + hub
            + self.node_window.max(self.edge_window);
        let kernel = rows + 28 * e + 8 * n + 4 * 24 * n;
        export.max(kernel) + SLACK
    }
}

fn degree_rank(directed: bool) -> RankOptions {
    RankOptions {
        by: RankAlgorithm::Degree,
        via: Some("LINK".to_owned()),
        directed,
        ..RankOptions::default()
    }
}

/// Run one `rank` call, check what it computed against the generator, and
/// return its peak. The call is the first on the project, so what it opens is
/// part of what it holds, and what it leaves behind once its result is dropped
/// (a decoded-shard cache, say) must be nothing that grows with the graph.
fn measured_rank(forge: &GraphForge, label: &str, directed: bool, expect: &Shape) -> usize {
    let before = CURRENT.load(Ordering::Relaxed);
    let (batch, peak) = peak_live_bytes(|| forge.rank(label, degree_rank(directed)).unwrap());
    assert_eq!(batch.num_rows(), expect.selected, "{label} nodes");
    // Scores are entries / max(nodes - 1, 1): their sum recovers the entry count.
    let scores = batch
        .column_by_name("score")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let total: f64 = scores.iter().flatten().sum();
    let recovered = total * (expect.selected.saturating_sub(1).max(1)) as f64;
    drop(batch);
    let kept = CURRENT.load(Ordering::Relaxed).saturating_sub(before);
    assert!(kept <= SLACK, "{label}: the call left {kept} bytes behind");
    assert!(
        (recovered - expect.entries as f64).abs() < 1e-3,
        "{label}: export kept {recovered} entries, the generator says {}",
        expect.entries
    );
    peak
}

/// Entries between selected nodes under `spec`, and the longest CSR row of the
/// view they are drawn from.
fn expected(spec: Spec, label_rare: Option<bool>, undirected: bool, project: &Path) -> Shape {
    let selected = |node: usize| label_rare.is_none_or(|rare| spec.rare(node) == rare);
    let edges = spec.edges();
    let kept = edges
        .iter()
        .filter(|&&(s, t)| selected(s) && selected(t))
        .count();
    let mut out = vec![0_usize; spec.nodes];
    let mut inbound = vec![0_usize; spec.nodes];
    for &(s, t) in &edges {
        out[s] += 1;
        inbound[t] += 1;
    }
    let widest = if undirected {
        // Out and in rows are separate CSR rows; the merge holds their sum.
        out.iter().chain(&inbound).copied().max().unwrap_or(0).max(
            (0..spec.nodes)
                .map(|n| out[n] + inbound[n])
                .max()
                .unwrap_or(0),
        )
    } else {
        out.iter().copied().max().unwrap_or(0)
    };
    Shape {
        all_nodes: spec.nodes,
        selected: (0..spec.nodes).filter(|&n| selected(n)).count(),
        entries: if undirected { 2 * kept } else { kept },
        undirected,
        sparse: label_rare.is_some_and(|rare| rare)
            || (label_rare == Some(false) && spec.rare_every > 0),
        widest_row: widest,
        min_degree: spec.degree,
        node_window: stream_window(
            project,
            "nodes",
            &["node_id", "node_uuid", "type_ids"],
            8 + 16 + 8,
        ),
        edge_window: stream_window(project, "edges", &["edge_id", "edge_uuid"], 8 + 16),
    }
}

#[test]
fn rank_over_a_whole_label_stays_within_the_export_terms() {
    let _guard = serial();
    let spec = Spec {
        nodes: 50_000,
        degree: 12,
        rare_every: 0,
        hub_degree: 0,
        weighted: false,
        ring: false,
    };
    let (_directory, project) = build_project(spec);
    let forge = open_indexed(&project);
    for undirected in [false] {
        let shape = expected(spec, None, undirected, &project);
        let peak = measured_rank(&forge, "Entity", !undirected, &shape);
        let bound = shape.bound();
        eprintln!("whole label: held {peak} of {bound}");
        assert!(
            peak <= bound,
            "undirected={undirected}: held {peak} bytes live; the bound is {bound}"
        );
    }
}

#[test]
fn rank_over_a_tiny_subset_of_a_large_view_pays_for_the_subset() {
    let _guard = serial();
    let spec = Spec {
        nodes: 50_000,
        degree: 12,
        rare_every: 32,
        hub_degree: 150_000,
        weighted: false,
        ring: false,
    };
    let (_directory, project) = build_project(spec);
    let forge = open_indexed(&project);
    // `Rare` is every 32nd node, hub included: a few hundred nodes and fewer
    // than a thousand entries of a view with more than two hundred thousand.
    for (label, rare, undirected) in [
        ("Rare", true, false),
        ("Rare", true, true),
        // Everything but the hub's neighborhood: a subset that is nearly the whole.
        ("Entity", false, true),
    ] {
        let shape = expected(spec, Some(rare), undirected, &project);
        let peak = measured_rank(&forge, label, !undirected, &shape);
        let bound = shape.bound();
        eprintln!("{label} undirected={undirected}: held {peak} of {bound}");
        assert!(
            peak <= bound,
            "{label} undirected={undirected}: held {peak} bytes live; the bound is {bound} \
             ({} nodes, {} entries kept)",
            shape.selected,
            shape.entries
        );
    }
}

/// Peak of a shortest-path call from node 0 to its cheapest neighbor, node 1:
/// the search ends at the first pop, so the call is an export, a node-id slice
/// and a one-row result.
fn measured_dijkstra(spec: Spec) -> (usize, usize, usize, usize) {
    let (_directory, project) = build_project(spec);
    let forge = open_indexed(&project);
    let source = NodeSelector::Uuid(node_uuid(0));
    let target = NodeSelector::Uuid(node_uuid(1));
    let options = PathsOptions {
        by: PathAlgorithm::Dijkstra,
        // Every relation: a bulk-imported project keeps its edge properties in
        // one shared route, which a named relation does not select.
        via: None,
        directed: true,
        weight: Some("cost".to_owned()),
        ..PathsOptions::default()
    };
    let before = CURRENT.load(Ordering::Relaxed);
    let (batch, peak) = peak_live_bytes(|| {
        forge
            .paths(&source, Some(&target), options.clone())
            .unwrap()
    });
    assert_eq!(batch.num_rows(), 1);
    drop(batch);
    let kept = CURRENT.load(Ordering::Relaxed).saturating_sub(before);
    let windows = stream_window(&project, "nodes", &["node_id", "node_uuid", "type_ids"], 32)
        + stream_window(&project, "edges", &["edge_id", "edge_uuid"], 24);
    (peak, spec.edges().len(), windows, kept)
}

#[test]
fn weighted_paths_peak_grows_with_the_entries_by_no_more_than_the_export_costs() {
    let _guard = serial();
    let small = Spec {
        nodes: 4_000,
        degree: 40,
        rare_every: 0,
        hub_degree: 0,
        weighted: true,
        ring: false,
    };
    let large = Spec {
        nodes: 16_000,
        ..small
    };
    let (small_peak, small_edges, small_windows, small_kept) = measured_dijkstra(small);
    let (large_peak, large_edges, large_windows, large_kept) = measured_dijkstra(large);
    // What the first call leaves behind (opened indexes, resolved selectors) may
    // grow with the nodes, by a few words each, and not with the entries: a
    // decoded-shard cache would add megabytes.
    assert!(
        large_kept <= small_kept + 32 * (large.nodes - small.nodes),
        "the call left {small_kept} bytes behind on the small graph and {large_kept} on the large"
    );
    assert_eq!(large_edges, 4 * small_edges);

    // Per entry, at the widest point of the export, while weights are matched:
    // neighbors 4, edge ids 8, edge UUIDs 16, weights 8 and the UUID-sorted
    // permutation 4. Per node: offsets 4, UUIDs 16, the node-id slice 8 and the
    // search's per-node state, at most 64. The Parquet windows of the two
    // graphs are added from their row-group metadata. The property stream holds
    // a window fixed by its own limits, which is the same for both graphs, so
    // it does not appear: growth in it would show as growth here.
    let entry_bytes = 40;
    let node_bytes = 4 + 16 + 8 + 64;
    let allowed = entry_bytes * (large_edges - small_edges)
        + node_bytes * (large.nodes - small.nodes)
        + large_windows.saturating_sub(small_windows)
        + SLACK;
    let growth = large_peak.saturating_sub(small_peak);
    eprintln!("weighted: grew {growth} of {allowed} ({small_peak} to {large_peak})");
    assert!(
        growth <= allowed,
        "peak grew by {growth} bytes from {small_edges} to {large_edges} entries \
         ({small_peak} to {large_peak}); the export costs at most {allowed}"
    );
}

/// One single-source call from node 0 over every relation, and what it held.
struct SingleSource {
    rows: Vec<([u8; 16], u64)>,
    /// Entries in the `path` column, zero when the verb returns none.
    path_entries: usize,
    peak: usize,
    kept: usize,
}

fn measured_single_source(project: &Path, by: PathAlgorithm, weight: Option<&str>) -> SingleSource {
    let forge = open_indexed(project);
    let source = NodeSelector::Uuid(node_uuid(0));
    let options = PathsOptions {
        by,
        via: None,
        directed: true,
        weight: weight.map(str::to_owned),
        ..PathsOptions::default()
    };
    let before = CURRENT.load(Ordering::Relaxed);
    let (batch, peak) = peak_live_bytes(|| forge.paths(&source, None, options.clone()).unwrap());
    let targets = batch
        .column_by_name("target_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
        .unwrap();
    let costs = batch
        .column_by_name("cost")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let rows: Vec<([u8; 16], u64)> = (0..batch.num_rows())
        .map(|row| {
            (
                targets.value(row).try_into().unwrap(),
                costs.value(row).to_bits(),
            )
        })
        .collect();
    let path_entries = batch.column_by_name("path").map_or(0, |paths| {
        let paths = paths
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();
        paths.values().len()
    });
    drop(batch);
    // The rows copied out above are the test's own, not the call's.
    let own = rows.capacity() * std::mem::size_of::<([u8; 16], u64)>();
    let kept = CURRENT.load(Ordering::Relaxed).saturating_sub(before + own);
    SingleSource {
        rows,
        path_entries,
        peak,
        kept,
    }
}

/// `bfs_depth` and `dijkstra_cost` keep no path and no edge identity. Both
/// graphs hold more entries than one CSR shard, so the cursor's shard is full
/// in both and cancels.
///
/// Per adjacency entry the export holds at most 40 bytes while weights are
/// matched (neighbors 4, edge ids 8, edge UUIDs 16, weights 8, the UUID-sorted
/// permutation 4) and the search holds at most 28 (neighbors 4, weights 8, and
/// a heap entry of 16 per relaxation); `bfs_depth` holds 4 at either point.
/// Per node: offsets 4, UUIDs 16, the search state (BFS: depth 4 and visit
/// order 4; Dijkstra: cost 8 and a settled bit), and the result at 40 bytes a
/// row (two UUIDs and a cost). The Parquet windows are added from row-group
/// metadata; the property stream's own window is fixed, so it cancels.
///
/// `bfs` retains edge ids and UUIDs, 24 bytes per entry, which it is asserted
/// to hold above `bfs_depth`.
#[test]
fn cost_only_paths_hold_no_per_vertex_path_and_grow_by_the_export_costs() {
    let _guard = serial();
    let small = Spec {
        nodes: 3_000,
        degree: 32,
        rare_every: 0,
        hub_degree: 0,
        weighted: true,
        ring: false,
    };
    let large = Spec {
        nodes: 12_000,
        ..small
    };
    let (small_edges, large_edges) = (small.edges().len(), large.edges().len());
    assert_eq!(large_edges, 4 * small_edges);
    let windows = |project: &Path| {
        stream_window(project, "nodes", &["node_id", "node_uuid", "type_ids"], 32)
            + stream_window(project, "edges", &["edge_id", "edge_uuid"], 24)
    };
    let (_small_directory, small_project) = build_project(small);
    let (_large_directory, large_project) = build_project(large);
    let window_growth = windows(&large_project).saturating_sub(windows(&small_project));

    for (cost_by, paths_by, weight, entry_bytes, node_bytes) in [
        (
            PathAlgorithm::BfsDepth,
            PathAlgorithm::Bfs,
            None,
            4,
            4 + 16 + 4 + 4 + 40,
        ),
        (
            PathAlgorithm::DijkstraCost,
            PathAlgorithm::Dijkstra,
            Some("cost"),
            40,
            4 + 16 + 9 + 40,
        ),
    ] {
        let cost_small = measured_single_source(&small_project, cost_by, weight);
        let cost_large = measured_single_source(&large_project, cost_by, weight);
        let paths_large = measured_single_source(&large_project, paths_by, weight);

        // The same rows and cost bits as the verb that builds paths.
        assert_eq!(cost_large.rows, paths_large.rows, "{cost_by}");
        assert!(cost_large.rows.len() > large.nodes / 2, "{cost_by}");
        assert_eq!(cost_large.path_entries, 0);
        assert!(
            cost_large.kept <= cost_small.kept + 32 * (large.nodes - small.nodes),
            "{cost_by} left {} bytes behind on the small graph and {} on the large",
            cost_small.kept,
            cost_large.kept
        );

        let allowed = entry_bytes * (large_edges - small_edges)
            + node_bytes * (large.nodes - small.nodes)
            + window_growth
            + SLACK;
        let growth = cost_large.peak.saturating_sub(cost_small.peak);
        eprintln!(
            "{cost_by}: grew {growth} of {allowed} ({} to {}); {paths_by} held {}",
            cost_small.peak, cost_large.peak, paths_large.peak
        );
        assert!(
            growth <= allowed,
            "{cost_by}: peak grew by {growth} bytes from {small_edges} to {large_edges} entries; \
             the export and search cost at most {allowed}"
        );

        // `bfs` retains edge ids and UUIDs (24 bytes an entry) while it runs.
        // `dijkstra`'s export, which weights make identical, is the peak of
        // both, so only its per-path cost (the test below) can tell them apart.
        let extra = if paths_by == PathAlgorithm::Bfs {
            24 * large_edges
        } else {
            0
        };
        assert!(
            paths_large.peak >= cost_large.peak + extra,
            "{paths_by} held {} bytes; {cost_by} held {} and the paths variant must hold \
             {extra} more",
            paths_large.peak,
            cost_large.peak
        );
    }
}

/// A ring and a pseudo-random graph with the same nodes, entries and weights
/// differ only in path length: the ring's shortest paths are `n / degree` hops
/// long, the random graph's a handful. The paths verbs hold a path for every
/// vertex, `P` entries and at least `16P` bytes (UUIDs) in the result alone, so
/// the ring costs them `16 * (P_ring - P_random)` more. The cost-only verbs
/// hold none, so the ring costs them nothing beyond the heap, which this graph
/// shape keeps below the slack.
#[test]
fn cost_only_paths_do_not_pay_for_path_length() {
    let _guard = serial();
    let random = Spec {
        nodes: 3_000,
        degree: 2,
        rare_every: 0,
        hub_degree: 0,
        weighted: true,
        ring: false,
    };
    let ring = Spec {
        ring: true,
        ..random
    };
    assert_eq!(random.edges().len(), ring.edges().len());
    let (_random_directory, random_project) = build_project(random);
    let (_ring_directory, ring_project) = build_project(ring);
    for (cost_by, paths_by, weight) in [
        (PathAlgorithm::BfsDepth, PathAlgorithm::Bfs, None),
        (
            PathAlgorithm::DijkstraCost,
            PathAlgorithm::Dijkstra,
            Some("cost"),
        ),
    ] {
        let cost_random = measured_single_source(&random_project, cost_by, weight);
        let cost_ring = measured_single_source(&ring_project, cost_by, weight);
        let paths_random = measured_single_source(&random_project, paths_by, weight);
        let paths_ring = measured_single_source(&ring_project, paths_by, weight);
        assert_eq!(cost_ring.rows, paths_ring.rows, "{cost_by}");
        assert_eq!(cost_ring.rows.len(), ring.nodes);
        let longer = paths_ring.path_entries - paths_random.path_entries;
        assert!(
            longer >= ring.nodes * ring.nodes / (2 * ring.degree) - paths_random.path_entries,
            "{paths_by}: {} path entries on the ring",
            paths_ring.path_entries
        );
        eprintln!(
            "ring {cost_by}: {} (random graph {}); {paths_by}: {} (random graph {}) over \
             {longer} more path entries",
            cost_ring.peak, cost_random.peak, paths_ring.peak, paths_random.peak
        );
        assert!(
            cost_ring.peak <= cost_random.peak + SLACK,
            "{cost_by}: the ring held {} bytes and the random graph {}",
            cost_ring.peak,
            cost_random.peak
        );
        assert!(
            paths_ring.peak >= paths_random.peak + 16 * longer,
            "{paths_by}: the ring held {} bytes and the random graph {}; {longer} more path \
             entries cost 16 bytes each",
            paths_ring.peak,
            paths_random.peak
        );
    }
}
