//! A hop that returns edge properties and only the destination identity reads
//! no node file for that destination (#1984).
//!
//! The destination `node_id` is the ordinal the CSR already yields, and
//! `node_uuid` comes from the v4 ordinal identity authority, so neither needs a
//! node file whether or not the hop also materialises edge columns. The
//! counters are exact and deterministic; the gate never measures time.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryArray, FixedSizeBinaryBuilder, Int64Array, Int64Builder,
    RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use graphforge_api::{
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, ExecutionResourcePolicy,
    GraphConstructionBudgets, GraphForge, GraphForgeOptions, LifecycleIoCapture,
};
use graphforge_core::uuid::Uuid;
use graphforge_exec::demand;
use graphforge_ir::IrLiteral;
use graphforge_storage::io_stats;

/// Rows per canonical file: the construction session's batch window.
const WINDOW: usize = 1_024;
/// Two node files, so the destinations span more than one.
const NODES: usize = 2 * WINDOW;
/// Every node starts this many edges.
const FANOUT: usize = 3;

fn node_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x1000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

fn edge_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x2000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

/// The node an edge `k` of node `i` ends at: two parallel edges to `i + 1` and
/// one to `i + 2`, so a node has three edges and two distinct neighbours.
fn neighbour(node: usize, k: usize) -> usize {
    (node + 1 + k / 2) % NODES
}

/// Unique across the graph, so a row identifies the edge it came from.
fn weight(node: usize, k: usize) -> i64 {
    (node * 10 + k) as i64
}

fn construct(dir: &Path) {
    let mut fields = CONSTRUCTION_NODE_SCHEMA.fields().to_vec();
    fields.push(Arc::new(Field::new("ident", DataType::Int64, true)));
    let node_schema = Arc::new(Schema::new(fields));
    let mut edge_fields = CONSTRUCTION_EDGE_SCHEMA.fields().to_vec();
    edge_fields.push(Arc::new(Field::new("weight", DataType::Int64, true)));
    let edge_schema = Arc::new(Schema::new(edge_fields));
    let forge = GraphForge::new(Some(dir.to_str().expect("utf-8 path"))).unwrap();
    let mut session = forge
        .begin_graph_construction(GraphConstructionBudgets {
            max_batch_rows: WINDOW,
            max_run_records: 4 * WINDOW,
            ..GraphConstructionBudgets::default()
        })
        .unwrap();
    for start in (0..NODES).step_by(WINDOW) {
        let end = (start + WINDOW).min(NODES);
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
    let edges = NODES * FANOUT;
    for start in (0..edges).step_by(WINDOW) {
        let end = (start + WINDOW).min(edges);
        let rows = end - start;
        let mut ids = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut sources = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut targets = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut weights = Int64Builder::with_capacity(rows);
        for edge in start..end {
            let (node, k) = (edge / FANOUT, edge % FANOUT);
            ids.append_value(edge_uuid(edge).as_bytes()).unwrap();
            sources.append_value(node_uuid(node).as_bytes()).unwrap();
            targets
                .append_value(node_uuid(neighbour(node, k)).as_bytes())
                .unwrap();
            weights.append_value(weight(node, k));
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&edge_schema),
            vec![
                Arc::new(ids.finish()) as ArrayRef,
                Arc::new(StringArray::from(vec!["LINK"; rows])),
                Arc::new(sources.finish()),
                Arc::new(targets.finish()),
                Arc::new(weights.finish()),
            ],
        )
        .unwrap();
        session
            .append_edges(&format!("edges-{start}"), &batch)
            .unwrap();
    }
    session.seal_and_publish().unwrap();
}

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

fn params(ident: i64) -> HashMap<String, IrLiteral> {
    HashMap::from([("ident".to_owned(), IrLiteral::Int(ident))])
}

/// The edge property and the destination identity, nothing else of the
/// destination.
const IDENTITY_ONLY: &str =
    "MATCH (a:Entity {ident: $ident})-[e:LINK]->(b) RETURN e.weight AS w, b.node_uuid AS id";

struct Counted {
    batches: Vec<RecordBatch>,
    io: io_stats::IoSnapshot,
    hop: demand::HopSnapshot,
}

fn run(forge: &GraphForge, query: &str, ident: i64) -> Counted {
    let _capture = LifecycleIoCapture::install();
    io_stats::reset();
    let (result, snapshot) = demand::capture(|| forge.execute_with_params(query, &params(ident)));
    let result = result.expect("statement executes");
    let io = io_stats::snapshot().expect("requested I/O statistics");
    let hop = snapshot.hops.values().next().expect("one hop").clone();
    Counted {
        batches: result.batches,
        io,
        hop,
    }
}

/// Rows of the edge weight and the destination UUID, sorted by weight.
fn identity_rows(batches: &[RecordBatch]) -> Vec<(i64, Vec<u8>)> {
    let mut rows = Vec::new();
    for batch in batches {
        let weights = batch
            .column_by_name("w")
            .expect("w column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("w is Int64");
        let ids = batch
            .column_by_name("id")
            .expect("id column")
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("id is a UUID");
        rows.extend((0..batch.num_rows()).map(|row| (weights.value(row), ids.value(row).to_vec())));
    }
    rows.sort();
    rows
}

fn expected_identity_rows(node: usize) -> Vec<(i64, Vec<u8>)> {
    let mut rows = (0..FANOUT)
        .map(|k| {
            (
                weight(node, k),
                node_uuid(neighbour(node, k)).as_bytes().to_vec(),
            )
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

/// A hop that returns edge properties and the destination UUID takes the UUID
/// from the ordinal identity authority: no node file is read, the lookup is
/// attributed to the hop, and every returned row carries its own edge's
/// property and its own neighbour's UUID.
#[test]
fn a_hop_returning_edge_properties_and_destination_uuids_reads_no_node_file() {
    let _serial = serial();
    let dir = tempfile::tempdir().unwrap();
    construct(dir.path());
    for partitions in [1, 4] {
        let forge = open(dir.path(), partitions);
        let plan = forge
            .explain(&IDENTITY_ONLY.replace("$ident", "1"))
            .unwrap();
        assert!(plan.contains("ExpandExec"), "{plan}");
        // Node 7 reaches node 8 over two parallel edges and node 9 over one; node
        // 1_500 sits in the second node file.
        for source in [7, 1_500, NODES - 1] {
            let counted = run(&forge, IDENTITY_ONLY, source as i64);
            assert_eq!(
                identity_rows(&counted.batches),
                expected_identity_rows(source),
                "{partitions} partitions, source {source}"
            );
            // The hop's own demand counters are query-scoped, so they hold at
            // every partition count: no node file was started, and the identity
            // authority answered the destination UUIDs.
            let hop = &counted.hop;
            assert_eq!(hop.node_reads_started, 0, "{partitions}: {hop:#?}");
            assert_eq!(hop.node_label_index_rows, 0, "{partitions}: {hop:#?}");
            assert!(hop.identity_ranges_selected > 0, "{partitions}: {hop:#?}");
            assert!(hop.identity_peak_buffer_bytes > 0, "{partitions}: {hop:#?}");
            assert_eq!(hop.identity_per_record_seeks, 0, "{partitions}: {hop:#?}");
            // The process-global counters are attributable only when the
            // statement runs on this thread, which the single-hop path does at
            // one partition.
            if partitions == 1 {
                let io = &counted.io;
                assert_eq!(io.node_filtered_reads, 0, "{source}: {io:?}");
                assert_eq!(io.node_full_reads, 0, "{source}: {io:?}");
                assert_eq!(io.node_label_index_lookups, 0, "{source}: {io:?}");
            }
        }
    }
}

/// The control: a project built by Cypher has no ordinal identity authority,
/// so the same hop falls back to the node files and still answers correctly.
/// This shows the demand counters see a node-file read when there is one.
#[test]
fn a_hop_without_an_identity_authority_still_reads_node_files() {
    let _serial = serial();
    let dir = tempfile::tempdir().unwrap();
    let forge = GraphForge::new(Some(dir.path().to_str().expect("utf-8 path"))).unwrap();
    for statement in [
        "UNWIND range(0, 20) AS i CREATE (:Entity {ident: i})",
        "MATCH (a:Entity), (b:Entity) WHERE b.ident = a.ident + 1 \
         CREATE (a)-[:LINK {weight: a.ident * 10}]->(b)",
        "MATCH (a:Entity), (b:Entity) WHERE b.ident = a.ident + 1 \
         CREATE (a)-[:LINK {weight: a.ident * 10 + 1}]->(b)",
    ] {
        forge.execute(statement).expect("fixture statement");
    }
    let counted = run(&forge, IDENTITY_ONLY, 7);
    let destination = forge
        .execute("MATCH (n:Entity {ident: 8}) RETURN n.node_uuid AS id")
        .expect("statement executes");
    let uuid_of_8 = destination.batches[0]
        .column_by_name("id")
        .expect("id column")
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("id is a UUID")
        .value(0)
        .to_vec();
    assert_eq!(
        identity_rows(&counted.batches),
        [(70, uuid_of_8.clone()), (71, uuid_of_8)]
    );
    assert!(counted.hop.node_reads_started > 0, "{:#?}", counted.hop);
    assert_eq!(counted.hop.identity_ranges_selected, 0);
}

/// A write that deletes a neighbour leaves the hop answering from the identity
/// authority: the deleted node and its edges are gone, the others still carry
/// their own UUIDs, and no node file is read.
#[test]
fn a_hop_after_a_neighbour_is_deleted_returns_only_live_destinations() {
    let _serial = serial();
    let dir = tempfile::tempdir().unwrap();
    construct(dir.path());
    let forge = open(dir.path(), 1);
    // Node 7 reaches 8 over two edges and 9 over one.
    assert_eq!(
        identity_rows(&run(&forge, IDENTITY_ONLY, 7).batches),
        expected_identity_rows(7)
    );
    forge
        .execute("MATCH (n:Entity {ident: 9}) DETACH DELETE n")
        .expect("the delete executes");
    let counted = run(&forge, IDENTITY_ONLY, 7);
    let mut live = expected_identity_rows(7);
    live.retain(|(_, uuid)| *uuid != node_uuid(9).as_bytes().to_vec());
    assert_eq!(identity_rows(&counted.batches), live);
    let io = &counted.io;
    assert_eq!(io.node_filtered_reads, 0, "{io:?}");
    assert_eq!(io.node_full_reads, 0, "{io:?}");
    assert_eq!(counted.hop.node_reads_started, 0, "{:#?}", counted.hop);
}
