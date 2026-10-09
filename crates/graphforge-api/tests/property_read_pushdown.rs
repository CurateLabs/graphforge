//! A property-equality anchor on a durable project reads the compared column
//! of the fragments that can hold the value, and a read writes nothing (#1931).
//!
//! The route is several property fragments wide. Before the fix every query
//! authenticated each fragment into a scratch copy, decoded all of its
//! columns, spooled the decoded rows as JSON runs and merged them, so a lookup
//! cost the whole route in reads and a multiple of it in writes. The gate is
//! deterministic and counts bytes, never time:
//!
//! - the lifecycle read-path counters, which count the application's reads and
//!   writes through the storage layer, and
//! - on Linux, the process's own `wchar`, which counts every byte handed to a
//!   `write` system call and so also catches a write the counters do not see.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryBuilder, Int64Array, Int64Builder, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use graphforge_api::{
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, ExecutionResourcePolicy,
    GraphConstructionBudgets, GraphForge, GraphForgeOptions, LifecycleIoCapture, StorageIoPhase,
    lifecycle_io_snapshot,
};
use graphforge_core::uuid::Uuid;
use graphforge_ir::IrLiteral;

const WRITE_WINDOW: usize = 4 * 1024;
const FAN_OUT: usize = 2;
/// Bytes of incompressible text on every node, so that a fragment fills its
/// byte cap with a few thousand rows and the route spans several fragments.
const PADDING_BYTES: usize = 1024;
const SMALL_NODES: usize = 12_288;
const LARGE_NODES: usize = 2 * SMALL_NODES;

fn node_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x1000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

fn edge_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x2000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

fn mix(mut state: u64) -> u64 {
    state = (state ^ (state >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    state = (state ^ (state >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    state ^ (state >> 31)
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

/// `ident` is the node's index, so it rises with the UUID and each fragment
/// holds a disjoint range of it. `shuffled` is a permutation of the indices
/// that scatters every value across all fragments.
fn shuffled(index: usize, nodes: usize) -> i64 {
    ((index * 7919 + 13) % nodes) as i64
}

fn construct_with_fanout(dir: &Path, nodes: usize, fan_out: usize) {
    let mut fields = CONSTRUCTION_NODE_SCHEMA.fields().to_vec();
    fields.push(Arc::new(Field::new("ident", DataType::Int64, true)));
    fields.push(Arc::new(Field::new("shuffled", DataType::Int64, true)));
    fields.push(Arc::new(Field::new("padding", DataType::Utf8, true)));
    let node_schema = Arc::new(Schema::new(fields));
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
        let mut idents = Int64Builder::with_capacity(end - start);
        let mut scattered = Int64Builder::with_capacity(end - start);
        for node in start..end {
            ids.append_value(node_uuid(node).as_bytes()).unwrap();
            idents.append_value(node as i64);
            scattered.append_value(shuffled(node, nodes));
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&node_schema),
            vec![
                Arc::new(ids.finish()) as ArrayRef,
                Arc::new(StringArray::from(vec!["Entity"; end - start])),
                Arc::new(idents.finish()),
                Arc::new(scattered.finish()),
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

/// One target partition keeps the whole statement on the calling thread, where
/// the lifecycle capture is installed, so it observes every read-path byte.
fn open(project: &Path) -> GraphForge {
    open_with_partitions(project, 1)
}

fn open_with_partitions(project: &Path, target_partitions: usize) -> GraphForge {
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

/// What one statement read and wrote.
#[derive(Debug, Clone, Copy, Default)]
struct Io {
    read_bytes: u64,
    write_bytes: u64,
    write_calls: u64,
    /// Bytes this process handed to `write` system calls; `None` off Linux.
    wchar: Option<u64>,
}

#[cfg(target_os = "linux")]
fn wchar() -> Option<u64> {
    std::fs::read_to_string("/proc/self/io")
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix("wchar: ")?.trim().parse().ok())
}

#[cfg(not(target_os = "linux"))]
fn wchar() -> Option<u64> {
    None
}

fn measured(forge: &GraphForge, query: &str, ident: i64) -> (Vec<RecordBatch>, Io) {
    let _capture = LifecycleIoCapture::install();
    let before = lifecycle_io_snapshot().expect("requested observation");
    let wchar_before = wchar();
    let result = forge
        .execute_with_params(
            query,
            &HashMap::from([("ident".to_owned(), IrLiteral::Int(ident))]),
        )
        .expect("statement executes");
    let wchar_after = wchar();
    let delta = lifecycle_io_snapshot()
        .expect("requested observation")
        .since(&before)
        .expect("attribution");
    delta.validate_for_qualification().expect("reconciles");
    let io = Io {
        read_bytes: delta.phases[&StorageIoPhase::ReadPathScan].read_bytes,
        write_bytes: delta.totals.write_bytes,
        write_calls: delta.totals.write_calls,
        wchar: wchar_before.zip(wchar_after).map(|(b, a)| a - b),
    };
    (result.batches, io)
}

fn count(batches: &[RecordBatch]) -> i64 {
    assert_eq!(batches.len(), 1);
    batches[0]
        .column_by_name("n")
        .expect("n column")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("n is Int64")
        .value(0)
}

/// The `wchar` gate counts the whole process, so the tests of this binary,
/// whose fixtures write, run one at a time.
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

const ANCHORED_LOOKUP: &str = "MATCH (a:Entity {ident: $ident}) RETURN count(*) AS n";
const ANCHORED_HOP: &str = "MATCH (a:Entity {ident: $ident})-[:LINK]->(b) RETURN count(*) AS n";
/// The same anchor on a column whose values are scattered over every fragment,
/// so no fragment can be excluded by its statistics.
const SCATTERED_HOP: &str = "MATCH (a:Entity {shuffled: $ident})-[:LINK]->(b) RETURN count(*) AS n";
/// The anchor written so that no equality can be pushed to the property scan.
const UNPUSHED_HOP: &str =
    "MATCH (a:Entity)-[:LINK]->(b) WHERE a.ident + 0 = $ident RETURN count(*) AS n";

/// The property fragments of a project: how many, and the largest object.
fn property_fragments(project: &Path) -> (usize, u64) {
    let inventory = graphforge_storage::resolve_project_generation(project)
        .expect("project resolves")
        .unadmitted_graph_files_inventory()
        .expect("inventory reads")
        .expect("a constructed generation declares an inventory");
    let sizes = inventory
        .files
        .iter()
        .filter(|file| file.relative_path.starts_with("properties/"))
        .map(|file| file.byte_length)
        .collect::<Vec<_>>();
    (sizes.len(), sizes.into_iter().max().unwrap_or(0))
}

struct Built {
    _root: tempfile::TempDir,
    project: std::path::PathBuf,
    fragments: usize,
    largest: u64,
}

fn build(nodes: usize) -> Built {
    build_with_fanout(nodes, FAN_OUT)
}

fn build_with_fanout(nodes: usize, fan_out: usize) -> Built {
    let root = tempfile::tempdir().expect("project directory");
    let project = root.path().join("project");
    construct_with_fanout(&project, nodes, fan_out);
    let (fragments, largest) = property_fragments(&project);
    Built {
        _root: root,
        project,
        fragments,
        largest,
    }
}

#[test]
fn the_equality_is_planned_into_the_property_scan() {
    let _serial = serial();
    let built = build(SMALL_NODES);
    let forge = open(&built.project);
    let plan = forge
        .explain_stage(
            "MATCH (a:Entity {ident: 5})-[:LINK]->(b) RETURN count(*) AS n",
            graphforge_api::ExplainStage::PhysicalPlan,
        )
        .expect("plan");
    assert!(
        plan.contains("PropertyOverlayExec: route=_untyped, equality=ident=Int(5)"),
        "{plan}"
    );
    // The destination's route is still joined by key: a statement that reads
    // no property of it still authenticates it (permanent_storage_budgets).
    assert_eq!(plan.matches("PropertyOverlayExec").count(), 2, "{plan}");
}

/// An id-anchored lookup reads the fragment that holds the id, not the route:
/// its reads do not grow when the route does.
#[test]
fn an_anchored_lookup_reads_what_the_match_costs_not_what_the_route_costs() {
    let _serial = serial();
    let small = build(SMALL_NODES);
    let large = build(LARGE_NODES);
    assert!(small.fragments >= 3, "the route spans fragments");
    assert!(large.fragments >= 2 * small.fragments - 1);
    let mut reads = Vec::new();
    for built in [&small, &large] {
        let forge = open(&built.project);
        // The first statement admits the route's footers once per session.
        let (warm, _) = measured(&forge, ANCHORED_LOOKUP, 0);
        assert_eq!(count(&warm), 1);
        let (batches, io) = measured(&forge, ANCHORED_LOOKUP, 5);
        assert_eq!(count(&batches), 1);
        // The anchor's fragment is authenticated to find it and again to read
        // it, plus the footers and topology; never the other fragments.
        let bound = 2 * built.largest + (1 << 20);
        assert!(
            io.read_bytes <= bound,
            "{} fragments, largest {}: read {} bytes, bound {bound}",
            built.fragments,
            built.largest,
            io.read_bytes
        );
        reads.push(io.read_bytes);
    }
    // The large route holds twice the fragments and bytes.
    assert!(
        reads[1] <= reads[0] + (256 << 10),
        "reads grew with the route: {reads:?}"
    );
}

/// The destination's route is joined by key, which authenticates every
/// fragment of it once per statement: the one-hop adds one pass over the
/// route's key columns to the lookup, and no decode of its values.
#[test]
fn an_anchored_one_hop_adds_one_authentication_pass_of_the_destination_route() {
    let _serial = serial();
    let built = build(SMALL_NODES);
    let route_bytes = {
        let inventory = graphforge_storage::resolve_project_generation(&built.project)
            .expect("project resolves")
            .unadmitted_graph_files_inventory()
            .expect("inventory reads")
            .expect("a constructed generation declares an inventory");
        inventory
            .files
            .iter()
            .filter(|file| file.relative_path.starts_with("properties/"))
            .map(|file| file.byte_length)
            .sum::<u64>()
    };
    let forge = open(&built.project);
    let (_, _) = measured(&forge, ANCHORED_HOP, 0);
    let (_, lookup) = measured(&forge, ANCHORED_LOOKUP, 5);
    let (hop, one_hop) = measured(&forge, ANCHORED_HOP, 5);
    assert_eq!(count(&hop), FAN_OUT as i64);
    assert!(
        one_hop.read_bytes <= lookup.read_bytes + route_bytes + (1 << 20),
        "one-hop read {} against lookup {} + route {route_bytes}",
        one_hop.read_bytes,
        lookup.read_bytes
    );
}

/// An anchored expansion reads destination properties for the matching UUIDs.
/// Doubling unrelated rows must not double those authentication reads.
#[test]
fn anchored_destination_property_reads_do_not_grow_with_unrelated_rows() {
    let _serial = serial();
    let small = build(SMALL_NODES);
    let large = build(LARGE_NODES);
    assert!(large.fragments >= 2 * small.fragments - 1);
    let query = "MATCH (a:Entity {ident: $ident})-[:LINK]->(b) RETURN count(b.ident) AS n";
    for target_partitions in [1, 2, 4] {
        let mut reads = Vec::new();
        for built in [&small, &large] {
            let forge = open_with_partitions(&built.project, target_partitions);
            if target_partitions == 1 && reads.is_empty() {
                let plan = forge
                    .explain_stage(
                        &query.replace("$ident", "0"),
                        graphforge_api::ExplainStage::PhysicalPlan,
                    )
                    .expect("physical plan");
                assert!(plan.contains("UuidBuildKeyTapExec"), "{plan}");
            }
            let (warm, _) = measured(&forge, query, 0);
            assert_eq!(count(&warm), FAN_OUT as i64);
            let (batches, io) = measured(&forge, query, 5);
            assert_eq!(count(&batches), FAN_OUT as i64);
            assert_eq!(io.write_bytes, 0);
            assert_eq!(io.write_calls, 0);
            reads.push(io.read_bytes);
        }
        assert!(
            reads[1] <= reads[0] + (256 << 10),
            "target_partitions={target_partitions}: destination reads grew with unrelated rows: {reads:?}"
        );
    }
}

/// A partitioned hash join switches to its map representation for a larger
/// UUID frontier. The producer tap must still finish and preserve the exact
/// result when that frontier contains more than 150 destinations.
#[test]
fn large_partitioned_uuid_frontier_preserves_destination_results() {
    let _serial = serial();
    const MAP_FAN_OUT: usize = 160;
    let built = build_with_fanout(512, MAP_FAN_OUT);
    let forge = open_with_partitions(&built.project, 4);
    let query = "MATCH (a:Entity {ident: $ident})-[:LINK]->(b) RETURN count(b.ident) AS n";

    for ident in [0_i64, 5] {
        let (batches, io) = measured(&forge, query, ident);
        assert_eq!(count(&batches), MAP_FAN_OUT as i64);
        assert_eq!(io.write_bytes, 0);
        assert_eq!(io.write_calls, 0);
    }
}

/// Statistics cannot exclude a fragment from a scattered column, so its lookup
/// reads that column of every fragment; it returns the same rows.
#[test]
fn a_scattered_column_still_answers_exactly() {
    let _serial = serial();
    let built = build(SMALL_NODES);
    let forge = open(&built.project);
    for ident in [0_i64, 7, (SMALL_NODES / 2) as i64, (SMALL_NODES - 1) as i64] {
        let (pushed, _) = measured(&forge, SCATTERED_HOP, ident);
        assert_eq!(count(&pushed), FAN_OUT as i64, "shuffled = {ident}");
    }
    let (absent, _) = measured(&forge, SCATTERED_HOP, -1);
    assert_eq!(count(&absent), 0);
}

/// The pushed-down plan and a plan that cannot push return the same answers.
#[test]
fn pushing_the_equality_does_not_change_an_answer() {
    let _serial = serial();
    let built = build(SMALL_NODES);
    let forge = open(&built.project);
    for ident in [0_i64, 1, 4095, 4096, 8191, 12_287, 12_288, -3] {
        let (pushed, _) = measured(&forge, ANCHORED_HOP, ident);
        let (full, _) = measured(&forge, UNPUSHED_HOP, ident);
        assert_eq!(count(&pushed), count(&full), "ident = {ident}");
    }
}

/// A newer snapshot decides which value a UUID holds, whichever fragment the
/// equality finds it in, and a reopened project agrees.
#[test]
fn a_newer_snapshot_shadows_an_older_match() {
    let _serial = serial();
    let built = build(SMALL_NODES);
    let forge = open(&built.project);
    forge
        .execute("MATCH (a:Entity {ident: 7}) SET a.ident = 70000")
        .expect("update");
    drop(forge);
    for _ in 0..2 {
        let forge = open(&built.project);
        let (old, _) = measured(&forge, ANCHORED_HOP, 7);
        assert_eq!(count(&old), 0, "7 was overwritten");
        let (new, _) = measured(&forge, ANCHORED_HOP, 70_000);
        assert_eq!(count(&new), FAN_OUT as i64);
        let (untouched, _) = measured(&forge, ANCHORED_HOP, 8);
        assert_eq!(count(&untouched), FAN_OUT as i64);
    }
}

/// A read statement writes nothing: no scratch copy of a fragment and no
/// spooled run, in the application counters or in the bytes handed to `write`.
#[test]
fn a_read_statement_writes_nothing() {
    let _serial = serial();
    let built = build(SMALL_NODES);
    let forge = open(&built.project);
    for query in [ANCHORED_HOP, SCATTERED_HOP, UNPUSHED_HOP] {
        let (batches, io) = measured(&forge, query, 5);
        assert_eq!(count(&batches), FAN_OUT as i64);
        assert_eq!(io.write_bytes, 0, "{query}: {io:?}");
        assert_eq!(io.write_calls, 0, "{query}: {io:?}");
        if let Some(written) = io.wchar {
            assert_eq!(written, 0, "{query}: {io:?}");
        }
    }
    // A statement that scans the whole route is the harshest case.
    let (batches, io) = measured(&forge, "MATCH (a:Entity) RETURN count(a.padding) AS n", 0);
    assert_eq!(count(&batches), SMALL_NODES as i64);
    assert_eq!(io.write_bytes, 0, "{io:?}");
    assert_eq!(io.wchar.unwrap_or(0), 0, "{io:?}");
}
