//! Peak live heap of synchronous label propagation (CDLP) with initial labels
//! read from a node property, through the public facade (#1974).
//!
//! A counting global allocator measures every live byte the process holds while
//! one `cluster` call runs. Initial labels are read from a node property for
//! every selected node. They must be held as a dense array by node ordinal, one
//! `i64` per node, and never as a map keyed by node whose values are decoded
//! property rows: that held about 770 bytes per node on cit-Patents, 2.89 GB of
//! hash tables on 3.77 million nodes.
//!
//! The same graph is clustered twice, from the node ordinals and from the
//! property, each in a fresh `GraphForge` over the same durable project, so the
//! two calls pay the same export, kernel and result. What the property route
//! adds is the difference between the two peaks (see [`labels_bound`]).
#![allow(
    unsafe_code,
    reason = "a counting global allocator wraps System and only observes sizes"
)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use arrow::array::{Array, ArrayRef, FixedSizeBinaryBuilder, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use graphforge_api::{
    BulkInputKind, ClusterAlgorithm, ClusterOptions, ExecutionResourcePolicy, GraphForge,
    GraphForgeOptions, ImportSessionLimits, OperationId, ResourcePolicyMode,
    SynchronousLabelPropagationOptions,
};
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

fn v7(value: u128) -> Uuid {
    Uuid::from_u128((value << 80) | (0x7 << 76) | (0x2 << 62) | value)
}

fn node_uuid(index: usize) -> Uuid {
    v7(index as u128 + 1)
}

fn edge_uuid(index: usize) -> Uuid {
    v7(0x1_0000_0000 + index as u128)
}

/// The property the labels are read from. Distinct, non-sequential values, so a
/// label cannot be mistaken for the node ordinal it replaces.
fn original_id(node: usize) -> i64 {
    1_000_000 + (node as i64 * 7_919) % 999_983
}

const PROPERTY: &str = "original_id";
const WRITE_ROWS: usize = 32 * 1024;
/// Rounds of the synchronous kernel; the peak does not depend on it.
const ROUNDS: u32 = 2;

/// Out-arcs per node: node `i` points at `i + 1`, `7i + 3` and `13i + 5`.
fn arcs(nodes: usize) -> impl Iterator<Item = (usize, usize)> {
    (0..nodes).flat_map(move |node| {
        [
            (node, (node + 1) % nodes),
            (node, (node * 7 + 3) % nodes),
            (node, (node * 13 + 5) % nodes),
        ]
    })
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

/// Import `nodes` nodes, each carrying an `Int64` [`PROPERTY`], and the
/// [`arcs`] between them into a new durable project, and return it closed.
fn build_project(nodes: usize) -> (tempfile::TempDir, PathBuf) {
    let directory = tempfile::tempdir().unwrap();
    let sources = directory.path().join("sources");
    fs::create_dir(&sources).unwrap();

    let node_schema = Arc::new(Schema::new(vec![
        Field::new("node_uuid", DataType::FixedSizeBinary(16), true),
        Field::new("label", DataType::Utf8, false),
        Field::new(PROPERTY, DataType::Int64, true),
    ]));
    let node_batches: Vec<RecordBatch> = (0..nodes)
        .step_by(WRITE_ROWS)
        .map(|start| {
            let end = (start + WRITE_ROWS).min(nodes);
            RecordBatch::try_new(
                Arc::clone(&node_schema),
                vec![
                    uuids((start..end).map(node_uuid), end - start),
                    Arc::new(StringArray::from(vec!["Vertex"; end - start])),
                    Arc::new(Int64Array::from(
                        (start..end).map(original_id).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap()
        })
        .collect();
    write_parquet(&sources.join("nodes.parquet"), &node_batches);

    let edge_schema = Arc::new(Schema::new(vec![
        Field::new("edge_uuid", DataType::FixedSizeBinary(16), true),
        Field::new("rel_type", DataType::Utf8, false),
        Field::new("source_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("target_uuid", DataType::FixedSizeBinary(16), false),
    ]));
    let edges: Vec<(usize, usize)> = arcs(nodes).collect();
    let edge_batches: Vec<RecordBatch> = (0..edges.len())
        .step_by(WRITE_ROWS)
        .map(|start| {
            let end = (start + WRITE_ROWS).min(edges.len());
            let rows = end - start;
            RecordBatch::try_new(
                Arc::clone(&edge_schema),
                vec![
                    uuids((start..end).map(edge_uuid), rows),
                    Arc::new(StringArray::from(vec!["LINK"; rows])),
                    uuids(edges[start..end].iter().map(|&(s, _)| node_uuid(s)), rows),
                    uuids(edges[start..end].iter().map(|&(_, t)| node_uuid(t)), rows),
                ],
            )
            .unwrap()
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
/// 16 MiB.
const BUDGET: u64 = 16 * 1024 * 1024;

/// Open `project` under [`BUDGET`] with its adjacency index, as a loaded
/// project is served.
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
    forge
}

fn cdlp_options(initial_label_property: Option<&str>) -> ClusterOptions {
    ClusterOptions {
        by: ClusterAlgorithm::LabelPropagation,
        directed: true,
        via: Some("LINK".to_owned()),
        synchronous_label_propagation: Some(SynchronousLabelPropagationOptions {
            iterations: ROUNDS,
            initial_label_property: initial_label_property.map(str::to_owned),
        }),
        ..ClusterOptions::default()
    }
}

/// One `cluster` call in a fresh `GraphForge` over `project`: its labels by node
/// UUID and the most bytes it held live.
fn measured_cluster(
    project: &Path,
    initial_label_property: Option<&str>,
) -> (BTreeMap<[u8; 16], i64>, usize) {
    let forge = open_indexed(project);
    let (batch, peak) = peak_live_bytes(|| {
        forge
            .cluster("Vertex", cdlp_options(initial_label_property))
            .unwrap()
    });
    let ids = batch
        .column_by_name("node_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
        .unwrap();
    let communities = batch
        .column_by_name("community_id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let labels = (0..batch.num_rows())
        .map(|row| (ids.value(row).try_into().unwrap(), communities.value(row)))
        .collect();
    (labels, peak)
}

/// Bytes a streaming projection of `columns` of the property files holds
/// decoded, at most: the largest row group's column chunks.
fn property_window(project: &Path, columns: &[&str]) -> usize {
    let mut window = 0;
    let mut pending = vec![project.to_path_buf()];
    while let Some(path) = pending.pop() {
        for entry in fs::read_dir(&path).into_iter().flatten() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|ext| ext == "parquet")
                && path.to_string_lossy().contains("propert")
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
                    window = window.max(usize::try_from(chunks).unwrap());
                }
            }
        }
    }
    window
}

/// Synchronous label propagation from its definition, sharing no code with the
/// engine: each round every node takes the most frequent label among its
/// distinct out-neighbors and its distinct in-neighbors (a reciprocal pair votes
/// twice), the smallest label on a tie, and keeps its label when it has no vote.
/// Self-loops and parallel arcs collapse.
fn expected_labels(nodes: usize, initial: impl Fn(usize) -> i64) -> Vec<i64> {
    let mut outbound = vec![BTreeSet::new(); nodes];
    let mut inbound = vec![BTreeSet::new(); nodes];
    for (source, target) in arcs(nodes) {
        if source != target {
            outbound[source].insert(target);
            inbound[target].insert(source);
        }
    }
    let mut labels: Vec<i64> = (0..nodes).map(initial).collect();
    for _ in 0..ROUNDS {
        labels = (0..nodes)
            .map(|node| {
                let mut counts = BTreeMap::<i64, usize>::new();
                for &voter in outbound[node].iter().chain(&inbound[node]) {
                    *counts.entry(labels[voter]).or_default() += 1;
                }
                counts.values().copied().max().map_or(labels[node], |most| {
                    *counts.iter().find(|(_, count)| **count == most).unwrap().0
                })
            })
            .collect();
    }
    labels
}

/// What reading the labels from a property may add to the peak of the same call
/// that starts from node ordinals, for a graph of `nodes` nodes:
/// - the labels, one `i64` per node, held for the kernel to start from (`8n`);
/// - the UUID-sorted permutation that places each property row on its node
///   (`4n`) and one presence flag per node (`n`);
/// - the property stream: the largest decoded row group of the node UUID and
///   the label columns of the property files, read from their metadata.
///
/// Reading each node's row into a map keyed by node held about 770 bytes per
/// node, 2.89 GB over 3.77 million nodes.
fn labels_bound(nodes: usize, window: usize) -> usize {
    (8 + 4 + 1) * nodes + window + SLACK
}

/// Allocator bookkeeping and small structures that do not grow with the graph.
const SLACK: usize = 256 * 1024;

#[test]
fn initial_labels_are_one_dense_array_and_not_a_row_per_node() {
    let _guard = serial();
    const NODES: usize = 40_000;
    let (_directory, project) = build_project(NODES);

    let (from_ordinals, plain_peak) = measured_cluster(&project, None);
    let (from_property, labeled_peak) = measured_cluster(&project, Some(PROPERTY));

    // The labels are right, node by node: the property's values are neither the
    // ordinals nor in ordinal order, so a misplaced label shows.
    let expect = |labels: Vec<i64>| -> BTreeMap<[u8; 16], i64> {
        labels
            .into_iter()
            .enumerate()
            .map(|(node, label)| (*node_uuid(node).as_bytes(), label))
            .collect()
    };
    assert_eq!(
        from_ordinals,
        expect(expected_labels(NODES, |node| node as i64))
    );
    assert_eq!(from_property, expect(expected_labels(NODES, original_id)));

    let window = property_window(&project, &["node_uuid", PROPERTY]);
    let bound = labels_bound(NODES, window);
    let added = labeled_peak.saturating_sub(plain_peak);
    eprintln!(
        "initial labels added {added} of {bound} bytes ({plain_peak} to {labeled_peak}, \
         {window} stream window)"
    );
    assert!(
        added <= bound,
        "reading initial labels added {added} bytes to the peak of {plain_peak}; the labels, the \
         placement of rows and the property stream cost at most {bound} for {NODES} nodes"
    );
}
