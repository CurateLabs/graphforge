//! Peak live heap of the analyst export over a persisted multi-shard CSR
//! (#1974).
//!
//! The export holds the ordinal topology and little else. Its peak live heap is
//! bounded by the formula in [`expected_peak_bound`], measured by a counting
//! allocator over the real export (`export_analyst_topology`), for an out, an
//! in and an undirected export and for a weighted one.
#![allow(
    unsafe_code,
    reason = "a counting global allocator wraps System and only observes sizes"
)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::path::Path;

use graphforge_core::OntologyMode;
use graphforge_core::uuid::Uuid;
use graphforge_exec::{AdjacencyProvider, PersistentAdjacencyProvider, export_analyst_topology};
use graphforge_ir::{Direction, IrLiteral};
use graphforge_storage::GraphWriter;
use graphforge_storage::adjacency::{
    AdjacencyBuildOptions, build_adjacency_index_into_with_options,
};
use graphforge_value::{EntityTypeId, EntityTypeSelection};
use tempfile::TempDir;

/// The writer buffers a bounded window of topology rows between flushes.
const FLUSH_EVERY: usize = 4_096;
const TS: i64 = 1_700_000_000_000_000;

/// Counts the bytes this thread holds live while it is measuring. Other test
/// threads allocate freely and are not counted.
struct PeakCounter;

thread_local! {
    static MEASURING: Cell<bool> = const { Cell::new(false) };
    static LIVE: Cell<i64> = const { Cell::new(0) };
    static PEAK: Cell<i64> = const { Cell::new(0) };
}

fn grow(bytes: usize) {
    if MEASURING.try_with(Cell::get).unwrap_or(false) {
        let _ = LIVE.try_with(|live| {
            live.set(live.get() + bytes as i64);
            let _ = PEAK.try_with(|peak| peak.set(peak.get().max(live.get())));
        });
    }
}

fn shrink(bytes: usize) {
    if MEASURING.try_with(Cell::get).unwrap_or(false) {
        let _ = LIVE.try_with(|live| live.set(live.get() - bytes as i64));
    }
}

// SAFETY: every method defers to `System`, which upholds the `GlobalAlloc`
// contract; the counters only observe sizes.
unsafe impl GlobalAlloc for PeakCounter {
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
        shrink(layout.size());
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new: usize) -> *mut u8 {
        let moved = unsafe { System.realloc(pointer, layout, new) };
        if !moved.is_null() {
            if new >= layout.size() {
                grow(new - layout.size());
            } else {
                shrink(layout.size() - new);
            }
        }
        moved
    }
}

#[global_allocator]
static ALLOCATOR: PeakCounter = PeakCounter;

/// Run `operation`, returning its result and the most bytes it held live at
/// once above where it started (counting its own result).
fn peak_live_bytes<T>(operation: impl FnOnce() -> T) -> (T, usize) {
    struct Stop;
    impl Drop for Stop {
        fn drop(&mut self) {
            MEASURING.with(|measuring| measuring.set(false));
        }
    }
    LIVE.with(|live| live.set(0));
    PEAK.with(|peak| peak.set(0));
    MEASURING.with(|measuring| measuring.set(true));
    let stop = Stop;
    let result = operation();
    let peak = PEAK.with(Cell::get);
    drop(stop);
    (result, usize::try_from(peak).unwrap_or(0))
}

/// Nodes `0..nodes`, each with `degree` out-edges to scattered targets, plus a
/// self-loop and a parallel pair per fifth node.
struct Generated {
    dir: TempDir,
    nodes: usize,
    edges: usize,
}

fn node_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x2000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

fn edge_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x2100_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

fn generate(nodes: usize, degree: usize, weighted: bool) -> Generated {
    let dir = TempDir::new().unwrap();
    let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
    let label = EntityTypeId::decode(0).unwrap();
    for index in 0..nodes {
        writer.create_node(node_uuid(index), label).unwrap();
        if index % FLUSH_EVERY == FLUSH_EVERY - 1 {
            writer.flush().unwrap();
        }
    }
    let mut edges = 0;
    let mut add = |writer: &mut GraphWriter, source: usize, target: usize| {
        if edges % FLUSH_EVERY == FLUSH_EVERY - 1 {
            writer.flush().unwrap();
        }
        let uuid = edge_uuid(edges);
        writer
            .create_edge(uuid, "LINK", &node_uuid(source), &node_uuid(target))
            .unwrap();
        if weighted {
            writer
                .set_edge_properties(
                    &uuid,
                    Some("LINK"),
                    std::collections::HashMap::from([(
                        "cost".to_owned(),
                        IrLiteral::Float(f64::from(u32::try_from(edges % 97).unwrap()) - 20.0),
                    )]),
                )
                .unwrap();
        }
        edges += 1;
    };
    for source in 0..nodes {
        for step in 0..degree {
            add(&mut writer, source, (source * 7 + step * 131 + 1) % nodes);
        }
        if source % 5 == 0 {
            add(&mut writer, source, source);
            add(&mut writer, source, (source + 1) % nodes);
            add(&mut writer, source, (source + 1) % nodes);
        }
    }
    writer.flush().unwrap();
    Generated { dir, nodes, edges }
}

/// Persist a CSR whose shards hold at most `shard_edges` entries.
fn index_with_shards(dir: &Path, shard_edges: usize) {
    let mut options = AdjacencyBuildOptions::default();
    options.shard_max_edges = shard_edges;
    build_adjacency_index_into_with_options(dir, dir, TS, &options, || Ok(())).unwrap();
}

fn persistent(dir: &Path) -> PersistentAdjacencyProvider {
    let inventory =
        graphforge_storage::GraphCatalog::open(dir, None, &graphforge_ir::RuntimeCatalog::new())
            .unwrap()
            .admitted_inventory()
            .unwrap();
    PersistentAdjacencyProvider::new(dir.to_path_buf(), OntologyMode::Strict)
        .with_inventory(inventory)
}

/// Decoded bytes of one shard of `nodes` rows and `edges` entries in the CSR
/// format: validity bitmaps, 64-bit offsets and two 64-bit id columns.
fn decoded_shard_bytes(nodes: usize, edges: usize) -> usize {
    nodes.div_ceil(8) + 8 * (nodes + 1) + 3 * edges.div_ceil(8) + 16 * edges
}

/// Bytes a Parquet projection of `columns` (fixed bytes per row) holds while it
/// is read: the projected column chunks of the largest row group, plus one
/// decoded batch.
fn parquet_window(path: &Path, row_bytes: usize) -> usize {
    let file = std::fs::File::open(path).unwrap();
    let builder =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let rows = builder
        .metadata()
        .row_groups()
        .iter()
        .map(|group| usize::try_from(group.num_rows()).unwrap())
        .max()
        .unwrap_or(0);
    row_bytes * rows + row_bytes * rows.min(64 * 1024)
}

/// Parquet files under `topology/` or `properties/` whose path contains `part`.
fn parquet_files(dir: &Path, root: &str, part: &str) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    let mut pending = vec![dir.join(root)];
    while let Some(path) = pending.pop() {
        for entry in std::fs::read_dir(&path).into_iter().flatten() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|ext| ext == "parquet")
                && path.to_string_lossy().contains(part)
            {
                found.push(path);
            }
        }
    }
    found
}

/// The upper bound on an export's peak live heap, in bytes.
///
/// `4(N+1)` offsets, `4E'` neighbors and `16N` node UUIDs are always live.
/// Edge identity (`edge_ids` 8, `edge_uuids` 16 per slot) is live while it is
/// resolved and kept only on request. A requested weight adds `8E'` and, while
/// the weights are matched by UUID, a `4E'` UUID-sorted permutation; resolving
/// edge UUIDs holds a `4E'` edge-id-sorted permutation plus a `E'/8` bitmap.
/// Two shards' worth of bytes (encoded and decoded, an out and an in cursor)
/// and the Parquet windows the projections read through are added to that.
fn expected_peak_bound(
    nodes: usize,
    entries: usize,
    resolves_identity: bool,
    weighted: bool,
    shard_bytes: usize,
    parquet: usize,
) -> usize {
    let mut bound = 4 * (nodes + 1) + 4 * entries + 16 * nodes;
    if resolves_identity {
        bound += 24 * entries + 4 * entries + entries / 8;
    }
    if weighted {
        bound += 8 * entries + 4 * entries;
    }
    bound + 2 * shard_bytes + parquet + SLACK
}

/// Allocator bookkeeping and small per-export structures that do not grow with
/// the graph: the manifest, the provider view, closures and scratch rows.
const SLACK: usize = 128 * 1024;

const NODES: usize = 6_000;
const DEGREE: usize = 12;
const SHARD_EDGES: usize = 2_048;

/// Largest decoded-plus-encoded shard of the generated CSRs.
fn shard_bytes(shard_edges: usize) -> usize {
    // A shard holds at most `shard_edges` entries over the rows they cover;
    // rows average (DEGREE + 0.6) entries, so about `shard_edges / DEGREE` rows
    // plus the boundary row. The encoded IPC file is no larger than the decoded.
    2 * decoded_shard_bytes(shard_edges / DEGREE + 2, shard_edges)
}

#[test]
fn unweighted_export_peak_is_bounded_by_the_topology_plus_two_shards() {
    let graph = generate(NODES, DEGREE, false);
    index_with_shards(graph.dir.path(), SHARD_EDGES);
    let shard = shard_bytes(SHARD_EDGES);
    let node_window = parquet_files(graph.dir.path(), "topology", "node")
        .iter()
        .map(|path| parquet_window(path, 8 + 16))
        .max()
        .unwrap();

    for (direction, entries) in [
        (Direction::Out, graph.edges),
        (Direction::In, graph.edges),
        (Direction::Undirected, 2 * graph.edges),
    ] {
        let provider = persistent(graph.dir.path());
        // Opens the index and caches the view outside the measurement.
        let view = provider.adjacency("LINK", direction).unwrap();
        assert!(view.edge_entry_count().unwrap() > 0);
        drop(view);
        let (shape, peak) = peak_live_bytes(|| {
            export_analyst_topology(
                &provider,
                graph.dir.path(),
                OntologyMode::Strict,
                EntityTypeSelection::All,
                "LINK",
                direction,
                None,
                false,
            )
            .unwrap()
        });
        let bound = expected_peak_bound(graph.nodes, entries, false, false, shard, node_window);
        assert_eq!(shape.entries, entries, "{direction:?}");
        assert!(!shape.weighted && !shape.identity);
        assert_eq!(shape.edge_rows_read, 0);
        assert!(
            peak <= bound,
            "{direction:?} export held {peak} bytes live; the bound is {bound}"
        );
    }
}

#[test]
fn weighted_export_peak_is_bounded_by_topology_weights_and_transient_identity() {
    let graph = generate(12_000, 6, true);
    let shard_edges = 4_096;
    index_with_shards(graph.dir.path(), shard_edges);
    let shard = 2 * decoded_shard_bytes(shard_edges / 6 + 2, shard_edges);
    let node_window = parquet_files(graph.dir.path(), "topology", "node")
        .iter()
        .map(|path| parquet_window(path, 8 + 16))
        .max()
        .unwrap();

    // The weight is matched against streams the storage layer reads through its
    // own windows. Their peaks are measured on their own, here, and added to the
    // bound as constants of the reader rather than guessed.
    let inventory = graphforge_storage::GraphCatalog::open(
        graph.dir.path(),
        None,
        &graphforge_ir::RuntimeCatalog::new(),
    )
    .unwrap()
    .admitted_inventory()
    .unwrap();
    let files = graphforge_storage::TopologyFiles::from_inventory(&inventory).unwrap();
    let ((), edge_stream) = peak_live_bytes(|| {
        graphforge_storage::visit_edge_identities(&files, "LINK", OntologyMode::Strict, |_, _| {
            Ok(())
        })
        .unwrap();
    });
    let ((), property_stream) = peak_live_bytes(|| {
        graphforge_storage::visit_edge_property_batches(
            graph.dir.path(),
            Some(&inventory),
            "LINK",
            &["cost".to_owned()],
            |_| Ok(()),
        )
        .unwrap();
    });

    let provider = persistent(graph.dir.path());
    drop(provider.adjacency("LINK", Direction::Out).unwrap());
    let (shape, peak) = peak_live_bytes(|| {
        export_analyst_topology(
            &provider,
            graph.dir.path(),
            OntologyMode::Strict,
            EntityTypeSelection::All,
            "LINK",
            Direction::Out,
            Some("cost"),
            false,
        )
        .unwrap()
    });
    let bound = expected_peak_bound(
        graph.nodes,
        graph.edges,
        true,
        true,
        shard,
        node_window.max(edge_stream).max(property_stream),
    );
    assert!(shape.weighted && !shape.identity);
    assert_eq!(shape.entries, graph.edges);
    assert!(
        peak <= bound,
        "weighted export held {peak} bytes live; the bound is {bound}"
    );
}
