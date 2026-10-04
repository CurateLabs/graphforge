//! A single-edge DELETE on a project with edge properties reads the edge
//! property route a bounded number of times, whatever the size of the
//! expansion that finds the edge (#1388).
//!
//! The DELETE has no identity seek: it expands every edge and filters on
//! endpoint identity. Its read prefix previously demanded every column of `r`,
//! the unused `note` property included. The expansion attached properties to
//! each output chunk, and a
//! targeted property read authenticates and decodes its whole route whatever
//! its target count. Reading the route once per chunk therefore cost chunks x
//! route bytes, quadratic in the edges: one DELETE took 374 s at 524,288 edges
//! against 7.6 s at 32,768 (debug build).
//!
//! The gate is deterministic: lifecycle-attributed read-path bytes, never wall
//! time. The edge-property work is the difference between two projects with
//! the same topology, one of which carries a `note` on every edge, and its
//! bound comes from the manifest and the fragments' own footers:
//! [`ROUTE_READS`] reads of the declared route, each making a fixed number of
//! passes over every fragment object ([`passes_per_read`]). Edges grow 16x
//! between the two sizes and the chunk size is fixed, so the number of chunks
//! grows 16x too.

use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, FixedSizeBinaryBuilder, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use graphforge_api::{
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, ExecutionResourcePolicy,
    GraphConstructionBudgets, GraphForge, GraphForgeOptions, LifecycleIoCapture, StorageIoPhase,
    lifecycle_io_snapshot,
};
use graphforge_core::uuid::Uuid;
use graphforge_ir::IrLiteral;
use parquet::file::reader::{FileReader, SerializedFileReader};

const SMALL_NODES: usize = 128;
const LARGE_NODES: usize = 16 * SMALL_NODES;
const FAN_OUT: usize = 16;
const WRITE_WINDOW: usize = 32 * 1024;
/// Rows per execution chunk: 2 chunks at the small size, 32 at the large one.
const CHUNK_ROWS: usize = 1024;
/// The edge removed: from node 9 to node 12.
const SOURCE: usize = 9;
const TARGET: usize = SOURCE + 3;

/// Route-sized reads a DELETE may make outside expansion: planning captures
/// the property schema, the write phase counts removed properties, and the
/// tombstone staging reads the deleted edge's properties. Expansion itself
/// needs only the target identity and reads no unused property value.
const ROUTE_READS: u64 = 3;

/// Passes one route read makes over a fragment object, each reading at most the
/// object: footer reads by the builders for its admission, its row-group
/// selection, each row group's validation and the selected decode, then the
/// validation decode of every row group and the decode of the selected ones.
fn passes_per_read(row_groups: u64) -> u64 {
    let footers = 3 + row_groups;
    let decodes = 2;
    footers + decodes
}

fn node_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x1000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

fn edge_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x2000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

fn edge_note(index: usize) -> String {
    format!("link-{index}")
}

fn with_property(schema: &Schema, name: &str) -> Arc<Schema> {
    let mut fields = schema.fields().to_vec();
    fields.push(Arc::new(Field::new(name, DataType::Utf8, true)));
    Arc::new(Schema::new(fields))
}

/// A graph where node `s` links to the next [`FAN_OUT`] nodes; every node
/// carries a `name` and, with `notes`, every edge a `note`.
fn construct(dir: &Path, nodes: usize, notes: bool) {
    let node_schema = with_property(&CONSTRUCTION_NODE_SCHEMA, "name");
    let edge_schema = if notes {
        with_property(&CONSTRUCTION_EDGE_SCHEMA, "note")
    } else {
        Arc::clone(&CONSTRUCTION_EDGE_SCHEMA)
    };
    let forge = GraphForge::new(Some(dir.to_str().expect("utf-8 path"))).unwrap();
    let mut session = forge
        .begin_graph_construction(GraphConstructionBudgets {
            max_batch_rows: WRITE_WINDOW,
            max_run_records: 4 * WRITE_WINDOW,
            ..GraphConstructionBudgets::default()
        })
        .unwrap();
    for start in (0..nodes).step_by(WRITE_WINDOW) {
        let end = (start + WRITE_WINDOW).min(nodes);
        let mut ids = FixedSizeBinaryBuilder::with_capacity(end - start, 16);
        for node in start..end {
            ids.append_value(node_uuid(node).as_bytes()).unwrap();
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&node_schema),
            vec![
                Arc::new(ids.finish()) as ArrayRef,
                Arc::new(StringArray::from(vec!["Entity"; end - start])),
                Arc::new(StringArray::from_iter_values(
                    (start..end).map(|node| format!("entity-{node}")),
                )),
            ],
        )
        .unwrap();
        session
            .append_nodes(&format!("nodes-{start}"), &batch)
            .unwrap();
    }
    let edges = nodes * FAN_OUT;
    for start in (0..edges).step_by(WRITE_WINDOW) {
        let end = (start + WRITE_WINDOW).min(edges);
        let rows = end - start;
        let mut ids = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut sources = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut targets = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        for edge in start..end {
            let source = edge / FAN_OUT;
            ids.append_value(edge_uuid(edge).as_bytes()).unwrap();
            sources.append_value(node_uuid(source).as_bytes()).unwrap();
            targets
                .append_value(node_uuid((source + edge % FAN_OUT + 1) % nodes).as_bytes())
                .unwrap();
        }
        let mut columns = vec![
            Arc::new(ids.finish()) as ArrayRef,
            Arc::new(StringArray::from(vec!["LINK"; rows])),
            Arc::new(sources.finish()),
            Arc::new(targets.finish()),
        ];
        if notes {
            columns.push(Arc::new(StringArray::from_iter_values(
                (start..end).map(edge_note),
            )));
        }
        let batch = RecordBatch::try_new(Arc::clone(&edge_schema), columns).unwrap();
        session
            .append_edges(&format!("edges-{start}"), &batch)
            .unwrap();
    }
    session.seal_and_publish().unwrap();
}

/// What [`ROUTE_READS`] reads of the declared edge-property route may read:
/// every fragment's declared bytes times its passes, the row groups taken from
/// the fragment's own footer, read before the measured statement.
fn route_read_bound(project: &Path) -> (u64, u64) {
    let inventory = graphforge_storage::resolve_project_generation(project)
        .expect("project resolves")
        .unadmitted_graph_files_inventory()
        .expect("inventory reads")
        .expect("a constructed generation declares an inventory");
    let mut route_bytes = 0;
    let mut per_read = 0;
    for file in inventory
        .files
        .iter()
        .filter(|file| file.relative_path.starts_with("edge_properties/"))
    {
        let object = graphforge_storage::graph_object_path(project, &file.content_sha256)
            .expect("declared object path");
        let row_groups = SerializedFileReader::new(std::fs::File::open(object).expect("object"))
            .expect("fragment footer")
            .metadata()
            .num_row_groups() as u64;
        route_bytes += file.byte_length;
        per_read += passes_per_read(row_groups) * file.byte_length;
    }
    (route_bytes, ROUTE_READS * per_read)
}

/// One target partition keeps the whole statement on the calling thread, where
/// the lifecycle capture is installed, so it observes every read-path byte.
fn open(project: &Path) -> GraphForge {
    GraphForge::new_with_options(
        Some(project.to_str().expect("utf-8 path")),
        GraphForgeOptions {
            resource: ExecutionResourcePolicy {
                target_partitions: Some(1),
                batch_size: Some(CHUNK_ROWS),
                ..ExecutionResourcePolicy::default()
            },
            ..GraphForgeOptions::default()
        },
    )
    .unwrap()
}

fn uuid_param(index: usize) -> IrLiteral {
    IrLiteral::Uuid(*node_uuid(index).as_bytes())
}

/// Read-path bytes of the single-edge DELETE.
fn delete_one_edge(forge: &GraphForge) -> u64 {
    let _capture = LifecycleIoCapture::install();
    let before = lifecycle_io_snapshot().expect("requested observation");
    let result = forge
        .execute_with_params(
            "MATCH (a:Entity)-[r:LINK]->(b:Entity) \
             WHERE a.node_uuid = $source AND b.node_uuid = $target DELETE r",
            &HashMap::from([
                ("source".to_owned(), uuid_param(SOURCE)),
                ("target".to_owned(), uuid_param(TARGET)),
            ]),
        )
        .expect("DELETE of one edge");
    let deleted = result.batches[0]
        .column_by_name("edges_deleted")
        .expect("edges_deleted counter")
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("counter is UInt64")
        .value(0);
    assert_eq!(deleted, 1, "the DELETE removes exactly one edge");
    let delete = lifecycle_io_snapshot()
        .expect("requested observation")
        .since(&before)
        .expect("delete attribution");
    delete
        .validate_for_qualification()
        .expect("delete reconciles");
    delete.phases[&StorageIoPhase::ReadPathScan].read_bytes
}

/// Every remaining edge's `note`, read through the same multi-chunk expansion.
fn remaining_notes(forge: &GraphForge) -> BTreeSet<String> {
    let result = forge
        .execute("MATCH (:Entity)-[r:LINK]->(:Entity) RETURN r.note AS note")
        .expect("note read");
    let mut notes = BTreeSet::new();
    for batch in &result.batches {
        let column = batch
            .column_by_name("note")
            .expect("note column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("note is Utf8");
        for row in 0..column.len() {
            assert!(column.is_valid(row), "every remaining edge keeps its note");
            assert!(
                notes.insert(column.value(row).to_owned()),
                "notes are unique"
            );
        }
    }
    notes
}

struct Measured {
    edges: usize,
    route_bytes: u64,
    bound: u64,
    property_read_bytes: u64,
}

fn measure(nodes: usize) -> Measured {
    let root = tempfile::tempdir().expect("project directory");
    let with_notes = root.path().join("notes");
    let without_notes = root.path().join("bare");
    construct(&with_notes, nodes, true);
    construct(&without_notes, nodes, false);
    let (route_bytes, bound) = route_read_bound(&with_notes);
    assert!(
        route_bytes > 0,
        "the project declares an edge-property route"
    );

    let forge = open(&with_notes);
    let with_properties = delete_one_edge(&forge);
    let edges = nodes * FAN_OUT;
    let deleted = SOURCE * FAN_OUT + (TARGET - SOURCE - 1);
    let expected = (0..edges)
        .filter(|edge| *edge != deleted)
        .map(edge_note)
        .collect::<BTreeSet<_>>();
    assert_eq!(remaining_notes(&forge), expected);
    drop(forge);
    let reopened = GraphForge::new(with_notes.to_str()).expect("reopen after DELETE");
    assert_eq!(remaining_notes(&reopened), expected);
    drop(reopened);

    let without_properties = delete_one_edge(&open(&without_notes));
    let property_read_bytes = with_properties.saturating_sub(without_properties);
    eprintln!(
        "edges={edges} route_bytes={route_bytes} bound={bound} \
         read_path_bytes={with_properties} without_properties={without_properties} \
         property_read_bytes={property_read_bytes}"
    );
    Measured {
        edges,
        route_bytes,
        bound,
        property_read_bytes,
    }
}

#[test]
fn terminal_delete_keeps_property_predicates_and_repeated_target_semantics() {
    let root = tempfile::tempdir().expect("project directory");
    construct(root.path(), SMALL_NODES, true);
    let deleted = SOURCE * FAN_OUT + (TARGET - SOURCE - 1);
    let forge = open(root.path());
    let result = forge
        .execute_with_params(
            "MATCH (:Entity)-[r:LINK]->(:Entity) WHERE r.note = $note DELETE r DELETE r",
            &HashMap::from([("note".to_owned(), IrLiteral::Str(edge_note(deleted)))]),
        )
        .expect("a property predicate selects the deletion target");
    for (column, expected) in [("edges_deleted", 1), ("properties_removed", 1)] {
        let count = result.batches[0]
            .column_by_name(column)
            .expect("mutation counter")
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("counter is UInt64")
            .value(0);
        assert_eq!(count, expected, "{column} counts the target once");
    }
    drop(forge);
    let reopened = GraphForge::new(root.path().to_str()).expect("reopen after property DELETE");
    assert_eq!(
        remaining_notes(&reopened),
        (0..SMALL_NODES * FAN_OUT)
            .filter(|edge| *edge != deleted)
            .map(edge_note)
            .collect()
    );
}

#[test]
fn single_edge_delete_reads_the_edge_property_route_a_bounded_number_of_times() {
    for measured in [measure(SMALL_NODES), measure(LARGE_NODES)] {
        assert!(
            measured.property_read_bytes <= measured.bound,
            "deleting one of {} edges read {} edge-property bytes, above {ROUTE_READS} \
             reads of the {}-byte route ({} bytes)",
            measured.edges,
            measured.property_read_bytes,
            measured.route_bytes,
            measured.bound,
        );
    }
}
