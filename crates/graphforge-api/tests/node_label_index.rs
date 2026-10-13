//! A destination label test is answered from the per-ordinal label index, not
//! from the node files (#1984).
//!
//! A destination label test lowers to `array_has(type_ids, id)`, so a hop whose
//! destination is demanded for nothing beyond identity and labels used to read
//! the `type_ids` column of every node file chunk by chunk. The provider now
//! holds one label set per node ordinal for the topology generation and answers
//! those hops from it. The counters are exact and deterministic; the gate never
//! measures time.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryBuilder, Int64Array, Int64Builder, ListArray, RecordBatch,
    StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use graphforge_api::{
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, ExecutionResourcePolicy,
    GraphConstructionBudgets, GraphForge, GraphForgeOptions, LifecycleIoCapture,
};
use graphforge_core::uuid::Uuid;
use graphforge_ir::IrLiteral;
use graphforge_storage::io_stats;

/// Rows per canonical file: the construction session's batch window.
const WINDOW: usize = 32_768;
/// Two node files, so the index spans more than one.
const NODES: usize = 2 * WINDOW;
/// Every fourth node carries the destination label.
const TARGET_STRIDE: usize = 4;

fn node_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x1000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

fn edge_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x2000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

fn label(index: usize) -> &'static str {
    if index.is_multiple_of(TARGET_STRIDE) {
        "Target"
    } else {
        "Entity"
    }
}

/// Node `i` has a single label and one `LINK` edge to node `i + 1`.
fn construct(dir: &Path) {
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
    for start in (0..NODES).step_by(WINDOW) {
        let end = (start + WINDOW).min(NODES);
        let mut ids = FixedSizeBinaryBuilder::with_capacity(end - start, 16);
        let mut idents = Int64Builder::with_capacity(end - start);
        let mut labels = Vec::with_capacity(end - start);
        for node in start..end {
            ids.append_value(node_uuid(node).as_bytes()).unwrap();
            idents.append_value(node as i64);
            labels.push(label(node));
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&node_schema),
            vec![
                Arc::new(ids.finish()) as ArrayRef,
                Arc::new(StringArray::from(labels)),
                Arc::new(idents.finish()),
            ],
        )
        .unwrap();
        session
            .append_nodes(&format!("nodes-{start}"), &batch)
            .unwrap();
    }
    for start in (0..NODES).step_by(WINDOW) {
        let end = (start + WINDOW).min(NODES);
        let rows = end - start;
        let mut ids = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut sources = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut targets = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        for edge in start..end {
            ids.append_value(edge_uuid(edge).as_bytes()).unwrap();
            sources.append_value(node_uuid(edge).as_bytes()).unwrap();
            targets
                .append_value(node_uuid((edge + 1) % NODES).as_bytes())
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

/// Counts the `Target` destinations of the `Entity` at `$ident`.
const LABELLED_DESTINATIONS: &str =
    "MATCH (a:Entity {ident: $ident})-[e:LINK]->(b:Target) RETURN count(b) AS n";

fn count(forge: &GraphForge, query: &str, ident: i64) -> i64 {
    let result = forge
        .execute_with_params(
            query,
            &HashMap::from([("ident".to_owned(), IrLiteral::Int(ident))]),
        )
        .expect("statement executes");
    let batches = result.batches;
    assert_eq!(batches.len(), 1);
    batches[0]
        .column_by_name("n")
        .expect("n column")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("n is Int64")
        .value(0)
}

/// The statements run in order under one capture; the counters are read after
/// each.
fn run_counted(forge: &GraphForge, queries: &[(&str, i64)]) -> Vec<(i64, io_stats::IoSnapshot)> {
    let _capture = LifecycleIoCapture::install();
    io_stats::reset();
    queries
        .iter()
        .map(|(query, ident)| {
            let value = count(forge, query, *ident);
            (
                value,
                io_stats::snapshot().expect("requested I/O statistics"),
            )
        })
        .collect()
}

/// A label-filtered hop reads no node file: the first statement of a session
/// builds the index, and every statement after it answers from the same one.
#[test]
fn a_label_filtered_hop_reads_no_node_file_and_builds_one_index() {
    let _serial = serial();
    let dir = tempfile::tempdir().unwrap();
    construct(dir.path());
    for partitions in [1, 4] {
        let forge = open(dir.path(), partitions);
        let plan = forge
            .explain(&LABELLED_DESTINATIONS.replace("$ident", "1"))
            .unwrap();
        assert!(plan.contains("ExpandExec"), "{plan}");
        // Node 3 links to the `Target` 4, node 5 to the `Entity` 6, and node
        // 40_003 to the `Target` 40_004 of the second node file.
        let counted = run_counted(
            &forge,
            &[
                (LABELLED_DESTINATIONS, 3),
                (LABELLED_DESTINATIONS, 5),
                (LABELLED_DESTINATIONS, 40_003),
            ],
        );
        let values = counted.iter().map(|(value, _)| *value).collect::<Vec<_>>();
        assert_eq!(values, [1, 0, 1], "{partitions} partitions");
        // The single-hop path attaches no capture to its worker threads, so the
        // counters are observable only when the statement runs on this thread.
        if partitions == 1 {
            let first = &counted[0].1;
            assert_eq!(first.node_filtered_reads, 0, "{first:?}");
            assert_eq!(first.node_full_reads, 0, "{first:?}");
            assert_eq!(first.node_label_index_builds, 1, "{first:?}");
            assert_eq!(first.node_label_index_lookups, 1, "{first:?}");
            assert!(first.node_label_index_bytes >= NODES as u64, "{first:?}");
            let last = &counted[2].1;
            assert_eq!(last.node_filtered_reads, 0, "{last:?}");
            assert_eq!(last.node_full_reads, 0, "{last:?}");
            assert_eq!(
                last.node_label_index_builds, 1,
                "a later statement rebuilt the index: {last:?}"
            );
            assert_eq!(
                last.node_label_index_lookups, 3,
                "one lookup per statement's chunk: {last:?}"
            );
        }
    }
}

const DELETED: usize = 40_004;

/// A write bumps the topology generation, so the next statement builds a fresh
/// index; the one it replaces never answers for the new generation.
#[test]
fn a_write_rebuilds_the_index_once_for_the_new_generation() {
    let _serial = serial();
    // The counters are observable only when the statement runs on this thread.
    let dir = tempfile::tempdir().unwrap();
    construct(dir.path());
    let forge = open(dir.path(), 1);
    let _capture = LifecycleIoCapture::install();
    io_stats::reset();
    assert_eq!(count(&forge, LABELLED_DESTINATIONS, 40_003), 1);
    let first = io_stats::snapshot().expect("requested I/O statistics");
    assert_eq!(first.node_label_index_builds, 1, "{first:?}");

    forge
        .execute(&format!(
            "MATCH (n:Target {{ident: {DELETED}}}) DETACH DELETE n"
        ))
        .expect("the delete executes");
    let written = io_stats::snapshot().expect("requested I/O statistics");

    // The deleted destination is gone, so node 40_003 no longer has an
    // out-edge to expand; node 3 still reaches its `Target`. Both answer
    // without a node file read.
    assert_eq!(count(&forge, LABELLED_DESTINATIONS, 40_003), 0);
    assert_eq!(count(&forge, LABELLED_DESTINATIONS, 3), 1);
    let after = io_stats::snapshot().expect("requested I/O statistics");
    assert_eq!(after.node_label_index_builds, 2, "{written:?} -> {after:?}");
    assert_eq!(
        after.node_label_index_lookups - written.node_label_index_lookups,
        1,
        "only the statement with an out-edge looks a destination up: {written:?} -> {after:?}"
    );
    assert_eq!(
        after.node_filtered_reads, written.node_filtered_reads,
        "{written:?} -> {after:?}"
    );
    assert_eq!(
        after.node_full_reads, written.node_full_reads,
        "{written:?} -> {after:?}"
    );
}

fn statements(forge: &GraphForge, statements: &[&str]) {
    for statement in statements {
        forge.execute(statement).expect("fixture statement");
    }
}

/// Every `labels(b)` of the query's rows, each sorted, then the rows sorted.
fn label_lists(forge: &GraphForge, query: &str) -> Vec<Vec<String>> {
    let result = forge.execute(query).expect("statement executes");
    let mut rows = Vec::new();
    for batch in &result.batches {
        let lists = batch
            .column_by_name("l")
            .expect("l column")
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("l is a list");
        for row in 0..lists.len() {
            let values = lists.value(row);
            let names = values
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("labels are strings");
            let mut labels = (0..names.len())
                .map(|index| names.value(index).to_owned())
                .collect::<Vec<_>>();
            labels.sort();
            rows.push(labels);
        }
    }
    rows.sort();
    rows
}

fn counted_rows<T>(run: impl FnOnce() -> T) -> (T, io_stats::IoSnapshot) {
    let _capture = LifecycleIoCapture::install();
    io_stats::reset();
    let value = run();
    (
        value,
        io_stats::snapshot().expect("requested I/O statistics"),
    )
}

/// A node with several labels keeps its whole label set: `labels()` and every
/// label test answer from the index as they would from the node files.
#[test]
fn multi_label_destinations_keep_their_whole_label_set() {
    let _serial = serial();
    for partitions in [1, 4] {
        let dir = tempfile::tempdir().unwrap();
        construct(dir.path());
        let forge = open(dir.path(), partitions);
        // Node 10 is an `Entity` that gains `Target`; node 12 is a `Target` that
        // gains `Entity`. Their sources are 9 and 11; nodes 5 -> 6 (`Entity`)
        // and 3 -> 4 (`Target`) stay single-labelled as controls.
        statements(
            &forge,
            &[
                "MATCH (n:Entity {ident: 10}) SET n:Target",
                "MATCH (n:Target {ident: 12}) SET n:Entity",
            ],
        );
        let both = || vec!["Entity".to_owned(), "Target".to_owned()];
        let entity = || vec!["Entity".to_owned()];
        let target = || vec!["Target".to_owned()];
        let labels = |ident: i64| {
            format!("MATCH (:Entity {{ident: {ident}}})-[:LINK]->(b) RETURN labels(b) AS l")
        };
        let tested = |ident: i64, label: &str| {
            let query = format!(
                "MATCH (a:Entity {{ident: $ident}})-[:LINK]->(b:{label}) RETURN count(b) AS n"
            );
            counted_rows(|| count(&forge, &query, ident))
        };

        let mut lookups = 0;
        for (ident, expected) in [
            (9, vec![both()]),
            (11, vec![both()]),
            (5, vec![entity()]),
            (3, vec![target()]),
        ] {
            let (rows, counters) = counted_rows(|| label_lists(&forge, &labels(ident)));
            assert_eq!(rows, expected, "{partitions} partitions, source {ident}");
            if partitions == 1 {
                assert_eq!(counters.node_filtered_reads, 0, "{counters:?}");
                assert_eq!(counters.node_full_reads, 0, "{counters:?}");
                assert_eq!(counters.node_label_index_lookups, 1, "{counters:?}");
                lookups += counters.node_label_index_lookups;
            }
        }
        for (ident, label, expected) in [
            (9, "Target", 1),
            (9, "Entity", 1),
            (11, "Target", 1),
            (11, "Entity", 1),
            (5, "Target", 0),
            (5, "Entity", 1),
            (3, "Target", 1),
            (3, "Entity", 0),
        ] {
            let (value, counters) = tested(ident, label);
            assert_eq!(
                value, expected,
                "{partitions} partitions, source {ident} -> :{label}"
            );
            if partitions == 1 {
                assert_eq!(counters.node_filtered_reads, 0, "{counters:?}");
                assert_eq!(counters.node_full_reads, 0, "{counters:?}");
                assert_eq!(counters.node_label_index_lookups, 1, "{counters:?}");
                lookups += counters.node_label_index_lookups;
            }
        }
        if partitions == 1 {
            assert_eq!(lookups, 12);
        }
    }
}
