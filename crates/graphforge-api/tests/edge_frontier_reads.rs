//! A one-hop expansion from a fixed frontier reads the edge-file bytes of the
//! frontier, not of the relation (#1984).
//!
//! The edge route is several canonical files wide, and the frontier is a hub
//! whose edges are spread over the first two files. Before edges were selected
//! by row ordinal, the reader compared the size of the whole request with the
//! size of one file and read every file of the route in full once the request
//! passed half a file, so the bytes read grew with the relation although the
//! frontier did not. The gate is deterministic and counts bytes, never time.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryBuilder, Int64Array, Int64Builder, RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use graphforge_api::{
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, ExecutionResourcePolicy,
    GraphConstructionBudgets, GraphForge, GraphForgeOptions, LifecycleIoCapture, StorageIoPhase,
    lifecycle_io_snapshot,
};
use graphforge_core::uuid::Uuid;
use graphforge_ir::IrLiteral;
use graphforge_storage::io_stats;

/// Edges per canonical edge file: the construction session's batch window.
const WINDOW: usize = 4_096;
/// Out-degree of the hub node, whose edges are every other edge of the first
/// `2 * HUB` edge identities: 1,500 of the 4,096 rows of each of the first two
/// files, so the request is more than half of one file but less than half of
/// any file it touches.
const HUB: usize = 3_000;
const SMALL_NODES: usize = 12_288;
const LARGE_NODES: usize = 2 * SMALL_NODES;

fn node_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x1000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

/// Edge UUIDs rise with the edge's index, so an edge's `edge_id` is its index
/// plus one at every size, and the hub's identities do not move as the route
/// grows.
fn edge_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x2000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

/// `(source, target)` node indexes of edge `index` in a graph of `nodes`.
fn endpoints(index: usize, nodes: usize) -> (usize, usize) {
    if index < 2 * HUB && index % 2 == 0 {
        (0, 1 + index / 2)
    } else {
        let source = 1 + index % (nodes - 1);
        (source, (source + 1) % nodes)
    }
}

fn construct(dir: &Path, nodes: usize) {
    let mut fields = CONSTRUCTION_NODE_SCHEMA.fields().to_vec();
    fields.push(Arc::new(Field::new("ident", DataType::Int64, true)));
    let node_schema = Arc::new(Schema::new(fields));
    let forge = GraphForge::new(Some(dir.to_str().expect("utf-8 path"))).unwrap();
    let mut session = forge
        .begin_graph_construction(GraphConstructionBudgets {
            max_batch_rows: WINDOW,
            max_run_records: 4 * WINDOW,
            ..GraphConstructionBudgets::default()
        })
        .unwrap();
    for start in (0..nodes).step_by(WINDOW) {
        let end = (start + WINDOW).min(nodes);
        let mut ids = FixedSizeBinaryBuilder::with_capacity(end - start, 16);
        let mut idents = Int64Builder::with_capacity(end - start);
        for node in start..end {
            ids.append_value(node_uuid(node).as_bytes()).unwrap();
            idents.append_value(node as i64);
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&node_schema),
            vec![
                Arc::new(ids.finish()) as ArrayRef,
                Arc::new(StringArray::from(vec!["Entity"; end - start])),
                Arc::new(idents.finish()),
            ],
        )
        .unwrap();
        session
            .append_nodes(&format!("nodes-{start}"), &batch)
            .unwrap();
    }
    for start in (0..nodes).step_by(WINDOW) {
        let end = (start + WINDOW).min(nodes);
        let rows = end - start;
        let mut ids = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut sources = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut targets = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        for edge in start..end {
            let (source, target) = endpoints(edge, nodes);
            ids.append_value(edge_uuid(edge).as_bytes()).unwrap();
            sources.append_value(node_uuid(source).as_bytes()).unwrap();
            targets.append_value(node_uuid(target).as_bytes()).unwrap();
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&CONSTRUCTION_EDGE_SCHEMA),
            vec![
                Arc::new(ids.finish()) as ArrayRef,
                Arc::new(StringArray::from(vec!["LINK"; rows])),
                Arc::new(sources.finish()),
                Arc::new(targets.finish()),
            ],
        )
        .unwrap();
        session
            .append_edges(&format!("edges-{start}"), &batch)
            .unwrap();
    }
    session.seal_and_publish().unwrap();
}

/// One target partition keeps the whole statement on the calling thread, where
/// the lifecycle capture is installed.
fn open(project: &Path, target_partitions: usize) -> GraphForge {
    GraphForge::new_with_options(
        Some(project.to_str().expect("utf-8 path")),
        GraphForgeOptions {
            resource: ExecutionResourcePolicy {
                target_partitions: Some(target_partitions),
                ..ExecutionResourcePolicy::default()
            },
            ..GraphForgeOptions::default()
        },
    )
    .unwrap()
}

static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[derive(Debug)]
struct Measured {
    count: i64,
    read_bytes: u64,
    edge: io_stats::IoSnapshot,
}

fn measured(forge: &GraphForge, query: &str, ident: i64) -> Measured {
    let _capture = LifecycleIoCapture::install();
    io_stats::reset();
    let before = lifecycle_io_snapshot().expect("requested observation");
    let result = forge
        .execute_with_params(
            query,
            &HashMap::from([("ident".to_owned(), IrLiteral::Int(ident))]),
        )
        .expect("statement executes");
    let delta = lifecycle_io_snapshot()
        .expect("requested observation")
        .since(&before)
        .expect("attribution");
    let batches = result.batches;
    assert_eq!(batches.len(), 1);
    let count = batches[0]
        .column_by_name("n")
        .expect("n column")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("n is Int64")
        .value(0);
    Measured {
        count,
        read_bytes: delta.phases[&StorageIoPhase::ReadPathScan].read_bytes,
        edge: io_stats::snapshot().expect("requested I/O statistics"),
    }
}

const HUB_COUNT: &str = "MATCH (a:Entity {ident: $ident})-[e:LINK]->(b) RETURN count(e) AS n";

/// The counters of one measured hub expansion at one relation size.
struct Size {
    nodes: usize,
    edge_files: u64,
    measured: Measured,
}

fn edge_files(nodes: usize) -> u64 {
    nodes.div_ceil(WINDOW) as u64
}

/// A hub expansion decodes the rows of the hub's edges and no others, however
/// many edge files the relation spans, and reads bytes that differ between
/// relation sizes only by the metadata of the files it did not need.
#[test]
fn a_fixed_hub_frontier_reads_its_own_edges_not_the_relation() {
    let _serial = serial();
    let mut sizes = Vec::new();
    for nodes in [SMALL_NODES, LARGE_NODES] {
        let dir = tempfile::tempdir().unwrap();
        construct(dir.path(), nodes);
        let forge = open(dir.path(), 1);
        let plan = forge.explain(&HUB_COUNT.replace("$ident", "0")).unwrap();
        assert!(plan.contains("ExpandExec"), "{plan}");
        assert!(plan.contains("adjacency=hit"), "{plan}");
        // The first statement admits the footers, the adjacency index and the
        // node topology once per session.
        let warm = measured(&forge, HUB_COUNT, 0);
        assert_eq!(warm.count, HUB as i64);
        let hub = measured(&forge, HUB_COUNT, 0);
        assert_eq!(hub.count, HUB as i64, "{nodes} nodes");
        eprintln!("nodes={nodes} edge files={} {hub:?}", edge_files(nodes));
        sizes.push(Size {
            nodes,
            edge_files: edge_files(nodes),
            measured: hub,
        });
    }
    for size in &sizes {
        let edge = &size.measured.edge;
        assert_eq!(
            edge.edge_full_reads, 0,
            "{} nodes: an edge file was read in full: {edge:?}",
            size.nodes
        );
        assert_eq!(
            edge.edge_filtered_rows, HUB as u64,
            "{} nodes: the hub's rows and no others were returned: {edge:?}",
            size.nodes
        );
        assert_eq!(
            edge.edge_scanned_rows, HUB as u64,
            "{} nodes: the hub's rows and no others were decoded: {edge:?}",
            size.nodes
        );
    }
    let (small, large) = (&sizes[0], &sizes[1]);
    assert!(large.edge_files >= 2 * small.edge_files);
    // Each added edge file costs the footer and page index of its opens (a few
    // KiB) and its node file's. Reading an added file in full costs ~300 KiB;
    // the allowance is a tenth of that per file plus a fixed margin for the
    // node topology and adjacency metadata that grow with the nodes.
    let added = large.edge_files - small.edge_files;
    let allowance = added * 32 * 1024 + 96 * 1024;
    assert!(
        large.measured.read_bytes <= small.measured.read_bytes + allowance,
        "reads grew with the relation: {} -> {} bytes for {added} added edge files",
        small.measured.read_bytes,
        large.measured.read_bytes
    );
}

/// The answer does not depend on how many partitions run the plan.
#[test]
fn the_hub_expansion_answers_the_same_at_every_partition_count() {
    let _serial = serial();
    for nodes in [SMALL_NODES, LARGE_NODES] {
        let dir = tempfile::tempdir().unwrap();
        construct(dir.path(), nodes);
        for partitions in [1, 2, 4] {
            let forge = open(dir.path(), partitions);
            for ident in [0, 5, 4_000] {
                let expected = (0..nodes)
                    .filter(|edge| endpoints(*edge, nodes).0 == ident as usize)
                    .count() as i64;
                assert_eq!(
                    measured(&forge, HUB_COUNT, ident).count,
                    expected,
                    "{nodes} nodes, {partitions} partitions, ident {ident}"
                );
            }
        }
    }
}
