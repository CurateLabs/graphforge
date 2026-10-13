//! Point-access reads decompress the pages of the rows they want, and a page is
//! small (#1984).
//!
//! A one-hop expansion from a fixed frontier selects its edge rows by dense
//! ordinal. The selection is exact, so what a read decompresses is decided by
//! the writer's page size: the 20,480-row pages of the original layout made
//! every wanted ordinal cost twenty thousand decoded rows. A destination wanted
//! for nothing beyond its identity and labels is answered from the node label
//! index and reads no node page. A destination whose hop demands anything else
//! still selects its node row by dense ordinal, and that read decodes one page.
//! These tests build projects with the construction writer, whose edge, node
//! and property files hold 2,048-row pages, double the relation and the node
//! table, and count the rows held by the pages the readers decoded. The
//! counters are exact and deterministic; the gate never measures time.

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

/// Rows per canonical file: the construction session's batch window. It spans
/// one 20,480-row page of the original layout and sixteen of the current one.
const WINDOW: usize = 32_768;
/// Rows held by one page of a point-access file.
const PAGE_ROWS: u64 = 2_048;
const SMALL_NODES: usize = 2 * WINDOW;
const LARGE_NODES: usize = 4 * WINDOW;
/// Source nodes of the measured expansions. Each has exactly one out-edge, to
/// the next node, and all lie inside the small graph.
const FRONTIERS: [i64; 3] = [1_000, 40_000, 65_000];

fn node_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x1000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

/// Edge UUIDs rise with the edge's index, so an edge's `edge_id` is its index
/// plus one at every size.
fn edge_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x2000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
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
            ids.append_value(edge_uuid(edge).as_bytes()).unwrap();
            sources.append_value(node_uuid(edge).as_bytes()).unwrap();
            targets
                .append_value(node_uuid((edge + 1) % nodes).as_bytes())
                .unwrap();
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
    value: i64,
    read_bytes: u64,
    io: io_stats::IoSnapshot,
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
    let value = batches[0]
        .column_by_name("n")
        .expect("n column")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("n is Int64")
        .value(0);
    Measured {
        value,
        read_bytes: delta.phases[&StorageIoPhase::ReadPathScan].read_bytes,
        io: io_stats::snapshot().expect("requested I/O statistics"),
    }
}

/// Counts the frontier's one edge; the destination node is never projected.
const EDGE_COUNT: &str =
    "MATCH (a:Entity {ident: $ident})-[e:LINK]->(b:Entity) RETURN count(e) AS n";
/// Reads a property of the frontier's one destination node; the edge variable
/// is unused, so the expansion reads no edge row.
const DESTINATION_PROPERTY: &str =
    "MATCH (a:Entity {ident: $ident})-[e:LINK]->(b:Entity) RETURN sum(b.ident) AS n";
/// Counts the frontier's one edge and reads a node column of its destination
/// that neither the label index nor the destination's identity holds, so the
/// edge row and the node row are both read by dense ordinal.
const EDGE_AND_DESTINATION_CREATED_AT: &str = "MATCH (a:Entity {ident: $ident})-[e:LINK]->(b) \
     RETURN count(e) AS n, count(b.created_at) AS created";

/// The project at `nodes` and the statement run twice: the first admits the
/// footers, adjacency index and key walks once per session, the second is the
/// measured read.
fn run(nodes: usize, query: &str, ident: i64, partitions: usize) -> Measured {
    let dir = tempfile::tempdir().unwrap();
    construct(dir.path(), nodes);
    let forge = open(dir.path(), partitions);
    let plan = forge.explain(&query.replace("$ident", "0")).unwrap();
    assert!(plan.contains("ExpandExec"), "{plan}");
    let _warm = measured(&forge, query, ident);
    measured(&forge, query, ident)
}

fn summary(nodes: usize, ident: i64, hop: &Measured) -> String {
    let io = &hop.io;
    format!(
        "nodes={nodes} ident={ident} value={} read_bytes={} | edge: dense_reads={} \
         pages {}/{} page_rows={} exact={} | node: dense_reads={} pages {}/{} page_rows={} \
         exact={} | uuid: selections={} pages {}/{} rows={} fallbacks={}",
        hop.value,
        hop.read_bytes,
        io.edge_dense_row_selection_reads,
        io.edge_pages_selected,
        io.edge_pages_considered,
        io.edge_page_rows_selected,
        io.edge_exact_rows_selected,
        io.node_dense_row_selection_reads,
        io.node_pages_selected,
        io.node_pages_considered,
        io.node_page_rows_selected,
        io.node_exact_rows_selected,
        io.property_uuid_selections,
        io.property_uuid_pages_selected,
        io.property_uuid_pages_considered,
        io.property_uuid_rows_decoded,
        io.property_uuid_index_fallbacks,
    )
}

/// What one frontier's reads decoded, independent of how large the project is.
#[derive(Debug, PartialEq, Eq)]
struct Decoded {
    edge_page_rows: u64,
    node_page_rows: u64,
    uuid_rows: u64,
}

/// Whether the measured statement demands its edge variable, so that the
/// expansion reads the edge row.
#[derive(Clone, Copy)]
enum EdgeRow {
    /// The statement uses the edge: one edge row is read by dense ordinal.
    Read,
    /// The edge variable is unused: its adjacency comes from the CSR and the
    /// edge file is not read at all.
    Unread,
}

/// Whether the measured statement reads its destination's node row.
#[derive(Clone, Copy)]
enum NodeRow {
    /// The destination is wanted for identity and labels only: the label index
    /// answers it and no node file is read.
    Unread,
    /// The destination demands a column the label index cannot supply: one node
    /// row is read by dense ordinal.
    Read,
}

/// The reads were exact dense-ordinal selections that never fell back, and each
/// wanted row cost at most the rows of one page of the layout.
fn decoded(nodes: usize, ident: i64, hop: &Measured, edge: EdgeRow, node: NodeRow) -> Decoded {
    let io = &hop.io;
    let context = summary(nodes, ident, hop);
    assert_eq!(io.edge_full_reads, 0, "{context}");
    assert_eq!(io.node_full_reads, 0, "{context}");
    assert_eq!(io.edge_dense_layout_declined, 0, "{context}");
    assert_eq!(io.edge_dense_half_file_declined, 0, "{context}");
    assert_eq!(io.edge_validation_fallbacks, 0, "{context}");
    assert_eq!(io.node_validation_fallbacks, 0, "{context}");
    assert_eq!(io.property_uuid_index_fallbacks, 0, "{context}");
    match node {
        NodeRow::Read => {
            assert!(io.node_dense_row_selection_reads > 0, "{context}");
            assert_eq!(io.node_exact_rows_selected, 1, "{context}");
            assert!(
                io.node_page_rows_selected <= PAGE_ROWS,
                "one node row decoded {} rows of pages: {context}",
                io.node_page_rows_selected
            );
        }
        NodeRow::Unread => {
            assert_eq!(io.node_dense_row_selection_reads, 0, "{context}");
            assert_eq!(io.node_exact_rows_selected, 0, "{context}");
            assert_eq!(io.node_page_rows_selected, 0, "{context}");
        }
    }
    match edge {
        EdgeRow::Read => {
            assert!(io.edge_dense_row_selection_reads > 0, "{context}");
            assert_eq!(io.edge_exact_rows_selected, 1, "{context}");
        }
        EdgeRow::Unread => {
            assert_eq!(io.edge_dense_row_selection_reads, 0, "{context}");
            assert_eq!(io.edge_exact_rows_selected, 0, "{context}");
            assert_eq!(io.edge_page_rows_selected, 0, "{context}");
        }
    }
    assert!(
        io.edge_page_rows_selected <= PAGE_ROWS,
        "one edge row decoded {} rows of pages: {context}",
        io.edge_page_rows_selected
    );
    assert!(
        io.property_uuid_pages_selected <= io.property_uuid_selections,
        "a one-row selection nominated more than one UUID page: {context}"
    );
    assert!(
        io.property_uuid_rows_decoded <= io.property_uuid_selections * PAGE_ROWS,
        "{} UUID rows decoded for {} one-row selections: {context}",
        io.property_uuid_rows_decoded,
        io.property_uuid_selections
    );
    Decoded {
        edge_page_rows: io.edge_page_rows_selected,
        node_page_rows: io.node_page_rows_selected,
        uuid_rows: io.property_uuid_rows_decoded,
    }
}

/// Doubling the relation and the node table adds files, so the readers consider
/// more pages; the pages they decode, and the rows those pages hold, stay those
/// of the wanted rows. The page index lets a read skip every other page.
#[test]
fn a_fixed_frontier_decodes_the_pages_of_its_rows_as_the_relation_doubles() {
    let _serial = serial();
    for ident in FRONTIERS {
        let mut seen = Vec::new();
        for nodes in [SMALL_NODES, LARGE_NODES] {
            let hop = run(nodes, EDGE_COUNT, ident, 1);
            eprintln!("{}", summary(nodes, ident, &hop));
            assert_eq!(hop.value, 1, "{nodes} nodes");
            seen.push(decoded(nodes, ident, &hop, EdgeRow::Read, NodeRow::Unread));
        }
        assert_eq!(seen[0], seen[1], "ident {ident}");
    }
}

/// A destination demanded for a column beyond its identity and labels still
/// reads its node row by dense ordinal, and that read decodes the one page of
/// the wanted row however many node files the table holds.
#[test]
fn a_destination_node_read_decodes_one_page_as_the_node_table_doubles() {
    let _serial = serial();
    for ident in FRONTIERS {
        let mut seen = Vec::new();
        for nodes in [SMALL_NODES, LARGE_NODES] {
            let hop = run(nodes, EDGE_AND_DESTINATION_CREATED_AT, ident, 1);
            eprintln!("{}", summary(nodes, ident, &hop));
            assert_eq!(hop.value, 1, "{nodes} nodes");
            seen.push(decoded(nodes, ident, &hop, EdgeRow::Read, NodeRow::Read));
        }
        assert_eq!(seen[0], seen[1], "ident {ident}");
    }
}

/// A frontier property read decodes the pages of the wanted rows in the UUID
/// column of the fragment that owns them, whether the node table holds one,
/// two or four fragments.
#[test]
fn a_frontier_property_read_decodes_uuid_pages_as_the_route_grows() {
    let _serial = serial();
    for ident in [1_000, 20_000] {
        let mut seen = Vec::new();
        for nodes in [WINDOW, SMALL_NODES, LARGE_NODES] {
            let hop = run(nodes, DESTINATION_PROPERTY, ident, 1);
            eprintln!("{}", summary(nodes, ident, &hop));
            assert_eq!(hop.value, ident + 1, "{nodes} nodes");
            assert!(hop.io.property_uuid_selections > 0, "{hop:?}");
            seen.push(decoded(
                nodes,
                ident,
                &hop,
                EdgeRow::Unread,
                NodeRow::Unread,
            ));
        }
        assert_eq!(seen[0], seen[1], "ident {ident}");
        assert_eq!(seen[1], seen[2], "ident {ident}");
    }
}
