//! A node anchor `(:L {id: $x})` answered by a persisted equality index reads
//! the matching node's topology row, not the node topology (#1983).
//!
//! The index answers the property side in milliseconds, but the labelled
//! anchor still joined the whole node topology to the one index result: every
//! fragment of the graph was decoded to find one row. With a current index the
//! topology scan now reads only the rows the index names, by ordinal, and the
//! label is checked on that row.
//!
//! The growth gate counts bytes, never time, and counts them two ways because
//! the lifecycle read-path counters belong to the calling thread: with one
//! target partition the whole statement runs there, with more the plan's
//! exchanges run on worker threads the counters do not see. `rchar` in
//! `/proc/self/io` counts every byte any thread of the process asked the
//! kernel to read, so it holds at every partition count. UUIDs are random, so
//! the topology's identity column cannot compress away, and every node carries
//! incompressible text, so the property route spans several fragments.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryBuilder, Int64Builder, StringArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use graphforge_api::{
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, ExecutionResourcePolicy, ExplainStage,
    GraphConstructionBudgets, GraphForge, GraphForgeOptions, LifecycleIoCapture, StorageIoPhase,
    lifecycle_io_snapshot,
};
use graphforge_core::uuid::Uuid;
use graphforge_ir::IrLiteral;

const WRITE_WINDOW: usize = 4 * 1024;
/// Bytes of incompressible text on every node, so that a property fragment
/// fills its byte cap with a few thousand rows and the route spans fragments.
const PADDING_BYTES: usize = 1024;
const SMALL_NODES: usize = 12_288;
const LARGE_NODES: usize = 2 * SMALL_NODES;
const ANCHOR: &str = "MATCH (n:Entity {id: $ident}) RETURN n.id AS id, n.name AS name";
const ANCHORED: &str = "anchor=property_equality(id)";
const INDEXED: &str = "index=property_equality(id)";

fn mix(mut state: u64) -> u64 {
    state = (state ^ (state >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    state = (state ^ (state >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    state ^ (state >> 31)
}

/// A random-looking UUID per node, so identities are neither compressible nor
/// ordered like the node index.
fn node_uuid(index: usize) -> Uuid {
    let high = mix(index as u64 + 1);
    let low = mix(high ^ 0x9e37_79b9_7f4a_7c15);
    Uuid::from_u128((u128::from(high) << 64) | u128::from(low))
}

fn edge_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x2000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

fn padding(index: usize) -> String {
    let mut text = String::with_capacity(PADDING_BYTES);
    let mut state = index as u64 + 1;
    while text.len() < PADDING_BYTES {
        state = mix(state);
        text.push_str(&format!("{state:016x}"));
    }
    text.truncate(PADDING_BYTES);
    text
}

/// A permutation of `0..nodes` that scatters every value across all fragments.
fn scattered(index: usize, nodes: usize) -> i64 {
    ((index * 7919 + 13) % nodes) as i64
}

/// Every fourth node is an `Other`; the rest are `Entity`.
fn label(index: usize) -> &'static str {
    if index % 4 == 3 { "Other" } else { "Entity" }
}

fn construct(project: &Path, nodes: usize, fan_out: usize) {
    let forge = GraphForge::new(Some(project.to_str().expect("utf-8 path"))).unwrap();
    construct_into(&forge, nodes, fan_out);
}

fn construct_into(forge: &GraphForge, nodes: usize, fan_out: usize) {
    let mut fields = CONSTRUCTION_NODE_SCHEMA.fields().to_vec();
    fields.push(Arc::new(Field::new("id", DataType::Int64, true)));
    fields.push(Arc::new(Field::new("name", DataType::Utf8, true)));
    fields.push(Arc::new(Field::new("padding", DataType::Utf8, true)));
    let schema = Arc::new(Schema::new(fields));
    let mut session = forge
        .begin_graph_construction(GraphConstructionBudgets {
            max_batch_rows: WRITE_WINDOW,
            max_run_records: 4 * WRITE_WINDOW,
            ..GraphConstructionBudgets::default()
        })
        .unwrap();
    for start in (0..nodes).step_by(WRITE_WINDOW) {
        let end = (start + WRITE_WINDOW).min(nodes);
        let mut uuids = FixedSizeBinaryBuilder::with_capacity(end - start, 16);
        let mut labels = StringBuilder::new();
        let mut ids = Int64Builder::with_capacity(end - start);
        let mut names = StringBuilder::new();
        for node in start..end {
            uuids.append_value(node_uuid(node).as_bytes()).unwrap();
            labels.append_value(label(node));
            ids.append_value(scattered(node, nodes));
            names.append_value(format!("n{node}"));
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(uuids.finish()) as ArrayRef,
                Arc::new(labels.finish()),
                Arc::new(ids.finish()),
                Arc::new(names.finish()),
                Arc::new(StringArray::from_iter_values((start..end).map(padding))),
            ],
        )
        .unwrap();
        session
            .append_nodes(&format!("nodes-{start}"), &batch)
            .unwrap();
    }
    let edges = nodes * fan_out;
    for start in (0..edges).step_by(WRITE_WINDOW) {
        let end = (start + WRITE_WINDOW).min(edges);
        let rows = end - start;
        let mut ids = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut sources = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut targets = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        for edge in start..end {
            let source = edge / fan_out;
            ids.append_value(edge_uuid(edge).as_bytes()).unwrap();
            sources.append_value(node_uuid(source).as_bytes()).unwrap();
            targets
                .append_value(node_uuid((source + edge % fan_out + 1) % nodes).as_bytes())
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

fn params(ident: i64) -> HashMap<String, IrLiteral> {
    HashMap::from([("ident".to_owned(), IrLiteral::Int(ident))])
}

fn plan(forge: &GraphForge, query: &str) -> String {
    forge
        .explain_stage(query, ExplainStage::PhysicalPlan)
        .expect("plan")
}

/// Bytes the process asked the kernel to read, on any thread; `None` off Linux.
#[cfg(target_os = "linux")]
fn process_read_bytes() -> Option<u64> {
    let io = std::fs::read_to_string("/proc/self/io")
        .expect("task I/O accounting unavailable: cannot read /proc/self/io");
    Some(
        io.lines()
            .find_map(|line| line.strip_prefix("rchar: ")?.trim().parse().ok())
            .expect("task I/O accounting unavailable: /proc/self/io has no valid rchar line"),
    )
}

#[cfg(not(target_os = "linux"))]
fn process_read_bytes() -> Option<u64> {
    None
}

/// What one statement read.
#[derive(Debug, Clone, Copy)]
struct Reads {
    /// Read-path bytes the calling thread observed.
    observed: u64,
    /// Bytes any thread of the process read.
    process: Option<u64>,
    /// Node topology rows decoded by ordinal selection.
    selected_rows: u64,
    /// Node topology rows read by any other path the counters see.
    full_rows: u64,
}

fn measured(forge: &GraphForge, query: &str, ident: i64) -> (Vec<RecordBatch>, Reads) {
    let _capture = LifecycleIoCapture::install();
    let before = lifecycle_io_snapshot().expect("requested observation");
    graphforge_storage::io_stats::reset();
    let process_before = process_read_bytes();
    let result = forge
        .execute_with_params(query, &params(ident))
        .expect("statement executes");
    let process_after = process_read_bytes();
    let delta = lifecycle_io_snapshot()
        .expect("requested observation")
        .since(&before)
        .expect("attribution");
    delta.validate_for_qualification().expect("reconciles");
    let stats = graphforge_storage::io_stats::snapshot().expect("requested I/O statistics");
    (
        result.batches,
        Reads {
            observed: delta.phases[&StorageIoPhase::ReadPathScan].read_bytes,
            process: process_before.zip(process_after).map(|(b, a)| a - b),
            selected_rows: stats.node_exact_rows_selected,
            full_rows: stats.node_full_rows,
        },
    )
}

fn rows_of(batches: &[RecordBatch]) -> Vec<Vec<Option<String>>> {
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| {
                        (!column.is_null(row)).then(|| {
                            arrow::util::display::array_value_to_string(column, row)
                                .expect("value formats")
                        })
                    })
                    .collect(),
            );
        }
    }
    rows.sort();
    rows
}

fn answer(forge: &GraphForge, query: &str) -> Vec<Vec<Option<String>>> {
    rows_of(
        &forge
            .execute(query)
            .unwrap_or_else(|error| panic!("{query}: {error}"))
            .batches,
    )
}

fn row(id: i64, name: Option<&str>) -> Vec<Option<String>> {
    vec![Some(id.to_string()), name.map(str::to_owned)]
}

/// Storage-layer accounting is process-wide, so measured tests run one at a time.
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct Built {
    _root: tempfile::TempDir,
    project: PathBuf,
    nodes: usize,
}

fn build(nodes: usize, fan_out: usize) -> Built {
    let root = tempfile::tempdir().expect("project directory");
    let project = root.path().join("project");
    construct(&project, nodes, fan_out);
    Built {
        _root: root,
        project,
        nodes,
    }
}

/// The nodes the growth gate looks up: `Entity` nodes in different fragments.
const LOOKUPS: [usize; 3] = [5, 4_100, 11_000];

/// A fixed amount of the node topology answers an anchor however large the
/// graph is, at every partition count.
#[test]
fn an_anchor_reads_the_match_not_the_node_topology() {
    let _serial = serial();
    let small = build(SMALL_NODES, 0);
    let large = build(LARGE_NODES, 0);
    let mut growth = Vec::new();
    for partitions in [1, 2, 4] {
        let mut by_size = Vec::new();
        for built in [&small, &large] {
            let forge = open(&built.project, partitions);
            let physical = plan(&forge, &ANCHOR.replace("$ident", "5"));
            assert!(
                physical.contains(ANCHORED) && physical.contains(INDEXED),
                "partitions={partitions}: {physical}"
            );
            // The first statement admits the topology and property footers
            // once per session.
            let (warm, _) = measured(&forge, ANCHOR, scattered(0, built.nodes));
            assert_eq!(
                rows_of(&warm),
                [row(scattered(0, built.nodes), Some("n0"))],
                "partitions={partitions}"
            );
            let mut worst = (0_u64, Some(0_u64));
            for index in LOOKUPS {
                let ident = scattered(index, built.nodes);
                let (batches, reads) = measured(&forge, ANCHOR, ident);
                assert_eq!(
                    rows_of(&batches),
                    [row(ident, Some(&format!("n{index}")))],
                    "partitions={partitions}, {} nodes",
                    built.nodes
                );
                // The topology row is read by ordinal, exactly one row, and
                // no path reads a node file whole. Only one partition runs
                // on the thread these counters belong to.
                if partitions == 1 {
                    assert_eq!(
                        (reads.selected_rows, reads.full_rows),
                        (1, 0),
                        "{} nodes: {reads:?}",
                        built.nodes
                    );
                }
                worst = (
                    worst.0.max(reads.observed),
                    worst.1.zip(reads.process).map(|(a, b)| a.max(b)),
                );
            }
            by_size.push(worst);
        }
        growth.push((partitions, by_size));
    }
    for (partitions, by_size) in growth {
        let (small_reads, large_reads) = (by_size[0], by_size[1]);
        // One identity column is 16 bytes a node and does not compress, so a
        // scan of the topology alone grows by far more than this.
        let allowance = 32 << 10;
        if partitions == 1 {
            assert!(
                large_reads.0 <= small_reads.0 + allowance,
                "reads grew with the node topology: {small_reads:?} -> {large_reads:?}"
            );
        }
        if let (Some(small), Some(large)) = (small_reads.1, large_reads.1) {
            assert!(
                large <= small + allowance,
                "partitions={partitions}: process reads grew with the node topology: \
                 {small} -> {large}"
            );
        }
    }
}

/// The label is checked on the row the index named: a node that carries the
/// value under another label does not match, and the answer is the same
/// whichever way the equality is written.
#[test]
fn a_label_mismatch_returns_nothing_and_a_match_returns_its_row() {
    let _serial = serial();
    let built = build(SMALL_NODES, 0);
    let forge = open(&built.project, 1);
    let entity = scattered(5, built.nodes);
    let other = scattered(3, built.nodes);
    assert_eq!((label(5), label(3)), ("Entity", "Other"));
    for (query, expected) in [
        (
            format!("MATCH (n:Entity {{id: {entity}}}) RETURN n.id AS id, n.name AS name"),
            vec![row(entity, Some("n5"))],
        ),
        (
            format!("MATCH (n:Other {{id: {other}}}) RETURN n.id AS id, n.name AS name"),
            vec![row(other, Some("n3"))],
        ),
        // The value exists, under the other label.
        (
            format!("MATCH (n:Other {{id: {entity}}}) RETURN n.id AS id, n.name AS name"),
            vec![],
        ),
        (
            format!("MATCH (n:Entity {{id: {other}}}) RETURN n.id AS id, n.name AS name"),
            vec![],
        ),
        // No node holds the value.
        (
            "MATCH (n:Entity {id: 99999999}) RETURN n.id AS id, n.name AS name".to_owned(),
            vec![],
        ),
        (
            format!("MATCH (n:Entity) WHERE n.id = {entity} RETURN n.id AS id, n.name AS name"),
            vec![row(entity, Some("n5"))],
        ),
        (
            format!("MATCH (n:Other) WHERE n.id = {entity} RETURN n.id AS id, n.name AS name"),
            vec![],
        ),
        // Without a label, the node is found whatever its label is.
        (
            format!("MATCH (n {{id: {other}}}) RETURN n.id AS id, n.name AS name"),
            vec![row(other, Some("n3"))],
        ),
    ] {
        let physical = plan(&forge, &query);
        assert!(physical.contains(ANCHORED), "{query}\n{physical}");
        assert_eq!(answer(&forge, &query), expected, "{query}");
    }
}

/// An index that does not describe the route is not used: the topology scan
/// reads every node again, the answers do not change, and rebuilding the index
/// restores the anchor.
#[test]
fn a_stale_index_falls_back_to_the_scan_and_stays_correct() {
    let _serial = serial();
    let built = build(SMALL_NODES, 0);
    let forge = open(&built.project, 1);
    let entity = scattered(5, built.nodes);
    let query = format!("MATCH (n:Entity {{id: {entity}}}) RETURN n.id AS id, n.name AS name");
    let other = format!("MATCH (n:Other {{id: {entity}}}) RETURN n.id AS id, n.name AS name");
    let physical = plan(&forge, &query);
    assert!(physical.contains(ANCHORED), "{physical}");
    assert_eq!(answer(&forge, &query), [row(entity, Some("n5"))]);

    // A write changes the route, so the index no longer describes it.
    forge
        .execute("CREATE (:Entity {id: 1000000, name: 'late'})")
        .unwrap();
    let late = "MATCH (n:Entity {id: 1000000}) RETURN n.id AS id, n.name AS name";
    for statement in [query.as_str(), late] {
        let physical = plan(&forge, statement);
        assert!(
            !physical.contains("anchor=") && !physical.contains("index="),
            "{statement}\n{physical}"
        );
    }
    assert_eq!(answer(&forge, &query), [row(entity, Some("n5"))]);
    assert_eq!(answer(&forge, &other), Vec::<Vec<Option<String>>>::new());
    assert_eq!(answer(&forge, late), [row(1_000_000, Some("late"))]);
    // The unanchored statement reads the node topology whole; the anchored
    // one did not.
    let (_, stale) = measured(&forge, ANCHOR, entity);
    assert_eq!(stale.selected_rows, 0, "{stale:?}");

    forge.rebuild_property_indexes(None).unwrap();
    for statement in [query.as_str(), late] {
        let physical = plan(&forge, statement);
        assert!(physical.contains(ANCHORED), "{statement}\n{physical}");
    }
    assert_eq!(answer(&forge, &query), [row(entity, Some("n5"))]);
    assert_eq!(answer(&forge, &other), Vec::<Vec<Option<String>>>::new());
    assert_eq!(answer(&forge, late), [row(1_000_000, Some("late"))]);
}

/// Nodes with several labels, nodes that share a value, and deleted nodes
/// answer the same through the anchor as through the scan.
#[test]
fn multi_label_shared_and_deleted_nodes_answer_as_the_scan_does() {
    let _serial = serial();
    let root = tempfile::tempdir().unwrap();
    let project = root.path().join("project");
    let forge = GraphForge::new(Some(project.to_str().unwrap())).unwrap();
    for statement in [
        "CREATE (:A:B {id: 1, name: 'ab'})",
        "CREATE (:A {id: 2})",
        "CREATE (:B {id: 2, name: 'b2'})",
        "CREATE (:C {id: 3, name: 'c3'})",
        "CREATE (:A {id: 4, name: 'a4'})",
    ] {
        forge.execute(statement).unwrap();
    }
    forge.rebuild_property_indexes(None).unwrap();
    let ret = "RETURN n.id AS id, n.name AS name";
    let cases: Vec<(String, Vec<Vec<Option<String>>>)> = vec![
        (
            format!("MATCH (n:A {{id: 1}}) {ret}"),
            vec![row(1, Some("ab"))],
        ),
        (
            format!("MATCH (n:B {{id: 1}}) {ret}"),
            vec![row(1, Some("ab"))],
        ),
        (
            format!("MATCH (n:A:B {{id: 1}}) {ret}"),
            vec![row(1, Some("ab"))],
        ),
        (format!("MATCH (n:C {{id: 1}}) {ret}"), vec![]),
        (format!("MATCH (n:A:B {{id: 2}}) {ret}"), vec![]),
        (format!("MATCH (n:A {{id: 2}}) {ret}"), vec![row(2, None)]),
        (
            format!("MATCH (n:B {{id: 2}}) {ret}"),
            vec![row(2, Some("b2"))],
        ),
        (
            format!("MATCH (n {{id: 2}}) {ret}"),
            vec![row(2, None), row(2, Some("b2"))],
        ),
        (
            format!("MATCH (n:A) WHERE n.id = 4 {ret}"),
            vec![row(4, Some("a4"))],
        ),
        (format!("MATCH (n:A {{id: 3}}) {ret}"), vec![]),
    ];
    for (query, expected) in &cases {
        let physical = plan(&forge, query);
        assert!(physical.contains(ANCHORED), "{query}\n{physical}");
        assert_eq!(&answer(&forge, query), expected, "anchored: {query}");
    }

    // Delete one of the two nodes that hold `id: 2`, then index again.
    forge
        .execute("MATCH (n:A {id: 2}) DETACH DELETE n")
        .unwrap();
    forge.rebuild_property_indexes(None).unwrap();
    for (query, expected) in [
        (format!("MATCH (n:A {{id: 2}}) {ret}"), vec![]),
        (
            format!("MATCH (n:B {{id: 2}}) {ret}"),
            vec![row(2, Some("b2"))],
        ),
        (
            format!("MATCH (n {{id: 2}}) {ret}"),
            vec![row(2, Some("b2"))],
        ),
    ] {
        let physical = plan(&forge, &query);
        assert!(physical.contains(ANCHORED), "{query}\n{physical}");
        assert_eq!(answer(&forge, &query), expected, "after delete: {query}");
    }

    // The scan gives the same answers once the index is stale.
    forge.execute("CREATE (:Z {id: 99})").unwrap();
    for (query, expected) in &cases {
        let physical = plan(&forge, query);
        assert!(!physical.contains("anchor="), "{query}\n{physical}");
        let expected = if query.contains("id: 2") {
            // The deleted node is gone from the scan's answer too.
            expected
                .iter()
                .filter(|row| row[1].is_some())
                .cloned()
                .collect()
        } else {
            expected.clone()
        };
        assert_eq!(answer(&forge, query), expected, "scanned: {query}");
    }
}

/// An anchored seed still feeds an expansion, and the hop reads what it read
/// before.
#[test]
fn an_anchored_seed_feeds_an_expansion() {
    let _serial = serial();
    let built = build(SMALL_NODES, 2);
    for partitions in [1, 2, 4] {
        let forge = open(&built.project, partitions);
        let seed = 5;
        let ident = scattered(seed, built.nodes);
        let query = format!(
            "MATCH (a:Entity {{id: {ident}}})-[:LINK]->(b) RETURN a.id AS anchor, b.name AS name"
        );
        let physical = plan(&forge, &query);
        assert!(physical.contains(ANCHORED), "{physical}");
        assert!(physical.contains("ExpandExec"), "{physical}");
        let expected = [seed + 1, seed + 2]
            .map(|node| {
                vec![
                    Some(ident.to_string()),
                    Some(format!("n{}", node % built.nodes)),
                ]
            })
            .to_vec();
        let mut expected = expected;
        expected.sort();
        assert_eq!(answer(&forge, &query), expected, "partitions={partitions}");
    }
}

/// A project that lives in memory builds the index and anchors the same way.
#[test]
fn an_in_memory_project_anchors_too() {
    let _serial = serial();
    let forge = GraphForge::new(None).unwrap();
    construct_into(&forge, 1_000, 0);
    let ident = scattered(5, 1_000);
    let query = format!("MATCH (n:Entity {{id: {ident}}}) RETURN n.id AS id, n.name AS name");
    let physical = plan(&forge, &query);
    assert!(
        physical.contains(ANCHORED) && physical.contains(INDEXED),
        "{physical}"
    );
    assert_eq!(answer(&forge, &query), [row(ident, Some("n5"))]);
    let other = format!("MATCH (n:Other {{id: {ident}}}) RETURN n.id AS id, n.name AS name");
    assert_eq!(answer(&forge, &other), Vec::<Vec<Option<String>>>::new());
}
