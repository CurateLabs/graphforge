//! A node variable's properties are read for the rows that reach it, not for
//! the route that holds them, whether or not an equality anchors the frontier
//! and whether or not the projection keeps the node's UUID (#1984).
//!
//! The fixture is a ring: node `i` links forward to `i + 1` and `i + 2`, for
//! the first few thousand nodes (the rest only add route). Three of those
//! nodes carry the label `Seed`, so a label scan reaches a handful of nodes
//! that no equality anchors and no limit bounds, and one `Wide` hub links to
//! a hundred thousand others. The
//! route holds more rows than DataFusion collects into one partition, so every
//! enrichment plans as a partitioned hash join, as it does on any real graph,
//! and spans several property fragments, so any read that scales with the
//! route shows up as bytes. The gate is deterministic and counts bytes, never
//! time: the lifecycle read-path counters count the application's reads and
//! writes through the storage layer.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryBuilder, Int64Array, Int64Builder, StringArray,
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
const PADDING_BYTES: usize = 64;
/// Above the 131,072 rows below which DataFusion collects a join side.
const SMALL_NODES: usize = 140_000;
const LARGE_NODES: usize = 2 * SMALL_NODES;
/// Nodes with outgoing edges; the others are the unrelated route.
const LINKED_NODES: usize = 4 * 1024;
const ANCHOR: i64 = 5;
/// Nodes whose index is this modulo 1,000 have no `ident`, so an enrichment
/// that loses a preserved row shows as a missing value.
const MISSING_IDENT: usize = 9;
/// The nodes labelled `Seed`; every ring node but these is an `Entity`.
const SEEDS: [usize; 3] = [1007, 2007, 3007];
/// The node labelled `Wide`, which links to the `WIDE_FANOUT` nodes after it.
const HUB: usize = LINKED_NODES;
/// More than half of the small route and less than half of the large one.
const WIDE_FANOUT: usize = 100_000;

fn label(index: usize) -> &'static str {
    if SEEDS.contains(&index) {
        "Seed"
    } else if index == HUB {
        "Wide"
    } else {
        "Entity"
    }
}

/// The `ident` of a node, which some nodes lack.
fn ident(index: usize) -> Option<i64> {
    (index % 1000 != MISSING_IDENT).then_some(index as i64)
}

/// The nodes a ring node links to.
fn ring_neighbours(index: usize) -> [usize; 2] {
    [index + 1, index + 2]
}

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

fn construct(dir: &Path, nodes: usize) {
    let mut fields = CONSTRUCTION_NODE_SCHEMA.fields().to_vec();
    fields.push(Arc::new(Field::new("ident", DataType::Int64, true)));
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
        for node in start..end {
            ids.append_value(node_uuid(node).as_bytes()).unwrap();
            if node % 1000 == MISSING_IDENT {
                idents.append_null();
            } else {
                idents.append_value(node as i64);
            }
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&node_schema),
            vec![
                Arc::new(ids.finish()) as ArrayRef,
                Arc::new(StringArray::from_iter_values((start..end).map(label))),
                Arc::new(idents.finish()),
                Arc::new(StringArray::from_iter_values((start..end).map(padding))),
            ],
        )
        .unwrap();
        session
            .append_nodes(&format!("nodes-{start}"), &batch)
            .unwrap();
    }
    let ring = LINKED_NODES * 2;
    let edges = ring + WIDE_FANOUT;
    for start in (0..edges).step_by(WRITE_WINDOW) {
        let end = (start + WRITE_WINDOW).min(edges);
        let rows = end - start;
        let mut ids = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut sources = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut targets = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        for edge in start..end {
            let (source, target) = if edge < ring {
                let source = edge / 2;
                (source, (source + edge % 2 + 1) % nodes)
            } else {
                (HUB, HUB + 1 + (edge - ring))
            };
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

/// What one statement read and wrote.
#[derive(Debug, Clone, Copy)]
struct Io {
    read_bytes: u64,
    write_bytes: u64,
    write_calls: u64,
}

fn parameters(ident: i64) -> HashMap<String, IrLiteral> {
    HashMap::from([("ident".to_owned(), IrLiteral::Int(ident))])
}

fn measured(forge: &GraphForge, query: &str, parameters: &HashMap<String, IrLiteral>) -> (i64, Io) {
    let _capture = LifecycleIoCapture::install();
    let before = lifecycle_io_snapshot().expect("requested observation");
    let result = forge
        .execute_with_params(query, parameters)
        .expect("statement executes");
    let delta = lifecycle_io_snapshot()
        .expect("requested observation")
        .since(&before)
        .expect("attribution");
    delta.validate_for_qualification().expect("reconciles");
    assert_eq!(result.batches.len(), 1);
    let value = result.batches[0]
        .column_by_name("n")
        .expect("n column")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("n is Int64")
        .value(0);
    (
        value,
        Io {
            read_bytes: delta.phases[&StorageIoPhase::ReadPathScan].read_bytes,
            write_bytes: delta.totals.write_bytes,
            write_calls: delta.totals.write_calls,
        },
    )
}

/// Storage-layer write accounting is process-wide and the fixtures of this
/// binary write files, so measured tests run one at a time.
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct Built {
    _root: tempfile::TempDir,
    project: std::path::PathBuf,
}

fn build(nodes: usize) -> Built {
    let root = tempfile::tempdir().expect("project directory");
    let project = root.path().join("project");
    construct(&project, nodes);
    Built {
        _root: root,
        project,
    }
}

/// The bytes of the property fragments a project's route spans.
fn property_bytes(project: &Path) -> u64 {
    graphforge_storage::resolve_project_generation(project)
        .expect("project resolves")
        .unadmitted_graph_files_inventory()
        .expect("inventory reads")
        .expect("a constructed generation declares an inventory")
        .files
        .iter()
        .filter(|file| file.relative_path.starts_with("properties/"))
        .map(|file| file.byte_length)
        .sum()
}

const AFTER_TOP_K: &str = "MATCH (a:Entity {ident: $ident})-[:LINK]->(m:Entity) \
     WITH m ORDER BY m.ident DESC LIMIT 1 \
     MATCH (m)-[:LINK]->(c:Entity) ";
const AFTER_TOP_K_UNLABELLED: &str = "MATCH (a:Entity {ident: $ident})-[:LINK]->(m) \
     WITH m ORDER BY m.ident DESC LIMIT 1 \
     MATCH (m)-[:LINK]->(c) ";
const AFTER_SECOND_HOP: &str =
    "MATCH (a:Entity {ident: $ident})-[:LINK]->(b:Entity)-[:LINK]->(c:Entity) ";

/// Each statement enriches a node variable whose projection drops its UUID,
/// from a frontier that no equality seeds directly, with the value it must
/// return for `ANCHOR`.
fn statements() -> Vec<(&'static str, String, i64)> {
    vec![
        (
            "enrichment after top-k",
            format!("{AFTER_TOP_K}RETURN sum(c.ident) AS n"),
            // m = 7, whose neighbours are 8 and 9; 9 has no ident.
            8,
        ),
        (
            "enrichment after top-k, unlabelled",
            format!("{AFTER_TOP_K_UNLABELLED}RETURN sum(c.ident) AS n"),
            8,
        ),
        (
            "enrichment after a second hop",
            format!("{AFTER_SECOND_HOP}RETURN sum(c.ident) AS n"),
            // 5 -> {6, 7} -> {7, 8} and {8, 9}.
            7 + 8 + 8,
        ),
        (
            "a node that only the second hop names",
            format!("{AFTER_SECOND_HOP}RETURN count(*) AS n"),
            4,
        ),
    ]
}

fn idents(batches: &[RecordBatch]) -> Vec<Option<i64>> {
    let mut values = Vec::new();
    for batch in batches {
        let column = batch
            .column_by_name("n")
            .expect("n column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("n is Int64");
        values
            .extend((0..column.len()).map(|row| (!column.is_null(row)).then(|| column.value(row))));
    }
    values.sort_unstable();
    values
}

/// The property scans a statement plans: all of them, and those among them
/// that resolve the ids of the nodes a scan of the topology finds (a label
/// scan's own nodes: the seed of a traversal, not a frontier that reached
/// them), which plan no tap.
#[derive(Clone, Copy)]
struct PlannedScans {
    all: usize,
    own: usize,
}

/// Every property scan that enriches a frontier waits for the UUIDs of the
/// frontier that reaches it, whatever the partition count. `planned` is, for
/// each statement in order, the scans it plans: a node variable whose
/// properties the statement never reads plans none, so the count is exact.
fn un_nominated_scans(
    forge: &GraphForge,
    partitions: usize,
    statements: &[(&'static str, String, i64)],
    planned: &[PlannedScans],
) -> Vec<String> {
    assert_eq!(statements.len(), planned.len(), "one count per statement");
    let mut found = Vec::new();
    for ((name, query, _), planned) in statements.iter().zip(planned) {
        let plan = forge
            .explain_stage(
                &query.replace("$ident", &ANCHOR.to_string()),
                ExplainStage::PhysicalPlan,
            )
            .expect("physical plan");
        let scans = plan.matches("PropertyOverlayExec").count();
        let taps = plan.matches("UuidBuildKeyTapExec").count();
        assert_eq!(
            scans, planned.all,
            "{name}, partitions {partitions}: property scans planned:\n{plan}"
        );
        if taps + planned.own != scans {
            found.push(format!(
                "{name}, partitions {partitions}: {scans} property scans, {taps} taps, \
                 {} not enriching a frontier:\n{plan}",
                planned.own
            ));
        }
    }
    found
}

/// A preserved row whose node holds no value survives the pruned scan as a
/// missing value, and a node that is not on the frontier contributes nothing.
fn assert_preserved_rows_survive(forge: &GraphForge, partitions: usize) {
    for (name, text, expected) in [
        ("after top-k", AFTER_TOP_K, vec![None, Some(8)]),
        (
            "after a second hop",
            AFTER_SECOND_HOP,
            vec![None, Some(7), Some(8), Some(8)],
        ),
    ] {
        let result = forge
            .execute_with_params(&format!("{text}RETURN c.ident AS n"), &parameters(ANCHOR))
            .expect("statement executes");
        assert_eq!(
            idents(&result.batches),
            expected,
            "{name}, partitions {partitions}"
        );
    }
}

/// The anchor alone: what the lookup of `a` costs before any enrichment.
const ANCHOR_ONLY: &str = "MATCH (a:Entity {ident: $ident}) RETURN count(*) AS n";

/// Run `query` on a warm session and return its value and I/O. A statement
/// that names `$ident` runs once for another node first; one that does not
/// runs twice as written.
fn steady(forge: &GraphForge, name: &str, query: &str) -> (i64, Io) {
    // The first statement admits the route's footers and walks its keys once
    // per session; the measured one is the steady state.
    let anchored = query.contains("$ident");
    let (warm, _) = measured(
        forge,
        query,
        &if anchored {
            parameters(0)
        } else {
            HashMap::new()
        },
    );
    assert!(warm > 0, "{name}: warm-up returned {warm}");
    let (value, io) = measured(
        forge,
        query,
        &if anchored {
            parameters(ANCHOR)
        } else {
            HashMap::new()
        },
    );
    assert_eq!(io.write_bytes, 0, "{name}: {io:?}");
    assert_eq!(io.write_calls, 0, "{name}: {io:?}");
    (value, io)
}

/// Doubling the nodes that are not on the frontier leaves every statement's
/// reads where they were, beyond what finding the anchor costs: the frontier
/// is fixed, so the bytes are too.
#[test]
fn enrichment_is_bounded_by_its_frontier() {
    let _serial = serial();
    let small = build(SMALL_NODES);
    let large = build(LARGE_NODES);
    // The `Seed` and `Wide` labels add a few small fragments that do not grow,
    // so the route nearly doubles rather than exactly.
    let (small_bytes, large_bytes) = (
        property_bytes(&small.project),
        property_bytes(&large.project),
    );
    assert!(
        large_bytes * 10 >= small_bytes * 19,
        "the route doubles with the nodes: {small_bytes} property bytes for {SMALL_NODES} \
         nodes, {large_bytes} for {LARGE_NODES}",
    );
    let mut grew = Vec::new();
    let mut un_nominated = Vec::new();
    for target_partitions in [1, 2, 4] {
        let small_forge = open(&small.project, target_partitions);
        let large_forge = open(&large.project, target_partitions);
        un_nominated.extend(un_nominated_scans(
            &small_forge,
            target_partitions,
            &statements(),
            &[
                // The anchor, the node ordered by and the node summed.
                PlannedScans { all: 3, own: 0 },
                PlannedScans { all: 3, own: 0 },
                // The anchor and the value: the middle node reads nothing.
                PlannedScans { all: 2, own: 0 },
                // Only the anchor reads a property.
                PlannedScans { all: 1, own: 0 },
            ],
        ));
        assert_preserved_rows_survive(&small_forge, target_partitions);
        grew.extend(reads_that_grew(
            &small_forge,
            &large_forge,
            target_partitions,
            ANCHOR_ONLY,
            &statements(),
        ));
    }
    assert!(
        grew.is_empty() && un_nominated.is_empty(),
        "reads grew with unrelated nodes: {grew:#?}\nscans without a frontier: {}",
        un_nominated.join("\n")
    );
}

/// The statements whose reads grew, when the route doubled, by more than
/// finding their first node did. Each returns its value on both routes.
fn reads_that_grew(
    small: &GraphForge,
    large: &GraphForge,
    partitions: usize,
    start: &str,
    statements: &[(&'static str, String, i64)],
) -> Vec<String> {
    // Locating the first node reads the topology or the labels, which also
    // double.
    let (_, start_small) = steady(small, "start", start);
    let (_, start_large) = steady(large, "start", start);
    let start_growth = start_large
        .read_bytes
        .saturating_sub(start_small.read_bytes);
    let mut grew = Vec::new();
    for (name, query, expected) in statements {
        let (value, small_io) = steady(small, name, query);
        assert_eq!(value, *expected, "{name}, partitions {partitions}");
        let (value, large_io) = steady(large, name, query);
        assert_eq!(value, *expected, "{name}, partitions {partitions}");
        let reads = [small_io.read_bytes, large_io.read_bytes];
        if reads[1] > reads[0] + start_growth + (256 << 10) {
            grew.push(format!(
                "{name}, partitions {partitions}: {reads:?}, start grew by {start_growth}"
            ));
        }
    }
    grew
}

/// The label scan alone: what finding the seeds costs before any enrichment.
const SEEDS_ONLY: &str = "MATCH (a:Seed) RETURN count(*) AS n";
const SEED_FIRST_HOP: &str = "MATCH (a:Seed)-[:LINK]->(b:Entity) ";
const SEED_SECOND_HOP: &str = "MATCH (a:Seed)-[:LINK]->(b:Entity)-[:LINK]->(c:Entity) ";

/// What the seeds' links reach, from the fixture's definition.
fn seed_first_hop() -> Vec<usize> {
    SEEDS
        .iter()
        .flat_map(|&seed| ring_neighbours(seed))
        .collect()
}

fn seed_second_hop() -> Vec<usize> {
    seed_first_hop()
        .into_iter()
        .flat_map(ring_neighbours)
        .collect()
}

fn sum_of(nodes: impl IntoIterator<Item = usize>) -> i64 {
    nodes.into_iter().filter_map(ident).sum()
}

/// Each statement enriches a node variable from a frontier that only a label
/// scan of three nodes produces: no equality anchors it and no limit bounds it.
fn seed_statements() -> Vec<(&'static str, String, i64)> {
    vec![
        (
            "enrichment after a label scan",
            format!("{SEED_FIRST_HOP}RETURN sum(b.ident) AS n"),
            sum_of(seed_first_hop()),
        ),
        (
            "enrichment after a label scan and a second hop",
            format!("{SEED_SECOND_HOP}RETURN sum(c.ident) AS n"),
            sum_of(seed_second_hop()),
        ),
        (
            "a node that only the second hop names, after a label scan",
            format!("{SEED_SECOND_HOP}RETURN count(*) AS n"),
            seed_second_hop().len() as i64,
        ),
    ]
}

/// A preserved row whose node holds no value survives the pruned scan as a
/// missing value.
fn assert_seed_rows_survive(forge: &GraphForge, partitions: usize) {
    for (name, text, variable, nodes) in [
        ("after a label scan", SEED_FIRST_HOP, "b", seed_first_hop()),
        (
            "after a label scan and a second hop",
            SEED_SECOND_HOP,
            "c",
            seed_second_hop(),
        ),
    ] {
        let result = forge
            .execute(&format!("{text}RETURN {variable}.ident AS n"))
            .expect("statement executes");
        let mut expected: Vec<Option<i64>> = nodes.into_iter().map(ident).collect();
        expected.sort_unstable();
        assert!(
            expected.contains(&None),
            "{name}: a preserved row has no value"
        );
        assert_eq!(
            idents(&result.batches),
            expected,
            "{name}, partitions {partitions}"
        );
    }
}

/// A frontier that no equality anchors and no limit bounds is still enriched
/// for its own rows: three seeds reach a handful of nodes whatever the size of
/// the route behind them.
#[test]
fn enrichment_of_an_unanchored_frontier_is_bounded_by_it() {
    let _serial = serial();
    let small = build(SMALL_NODES);
    let large = build(LARGE_NODES);
    let mut grew = Vec::new();
    let mut un_nominated = Vec::new();
    for target_partitions in [1, 2, 4] {
        let small_forge = open(&small.project, target_partitions);
        let large_forge = open(&large.project, target_partitions);
        un_nominated.extend(un_nominated_scans(
            &small_forge,
            target_partitions,
            &seed_statements(),
            &[
                // The label scan's nodes read nothing, so only the value
                // they reach is enriched.
                PlannedScans { all: 1, own: 0 },
                PlannedScans { all: 1, own: 0 },
                // No node reads a property, so the plan keeps the key of
                // every route (the label scan's own and the two it reaches).
                PlannedScans { all: 3, own: 1 },
            ],
        ));
        assert_seed_rows_survive(&small_forge, target_partitions);
        grew.extend(reads_that_grew(
            &small_forge,
            &large_forge,
            target_partitions,
            SEEDS_ONLY,
            &seed_statements(),
        ));
    }
    assert!(
        grew.is_empty() && un_nominated.is_empty(),
        "reads grew with unrelated nodes: {grew:#?}\nscans without a frontier: {}",
        un_nominated.join("\n")
    );
}

const WIDE_FIRST_HOP: &str = "MATCH (a:Wide)-[:LINK]->(b:Entity) ";

/// A frontier that names more than half of the route is not worth pruning to:
/// one scan of the route reads less than that many point reads. The statement
/// still returns exactly what it returns with the route pruned, and it still
/// plans the same shaped enrichment; the scan abandons the nomination only
/// once the frontier proves too large.
#[test]
fn a_frontier_past_half_the_route_keeps_the_whole_scan_and_every_row() {
    let _serial = serial();
    let small = build(SMALL_NODES);
    let targets: Vec<usize> = (HUB + 1..=HUB + WIDE_FANOUT).collect();
    assert!(
        targets.len() > SMALL_NODES / 2,
        "the hub names more than half of the small route"
    );
    let sum = (
        "enrichment of a frontier past half the route",
        format!("{WIDE_FIRST_HOP}RETURN sum(b.ident) AS n"),
        sum_of(targets.iter().copied()),
    );
    let mut un_nominated = Vec::new();
    for target_partitions in [1, 2, 4] {
        let forge = open(&small.project, target_partitions);
        un_nominated.extend(un_nominated_scans(
            &forge,
            target_partitions,
            std::slice::from_ref(&sum),
            &[PlannedScans { all: 1, own: 0 }],
        ));
        let (value, _) = steady(&forge, sum.0, &sum.1);
        assert_eq!(value, sum.2, "partitions {target_partitions}");

        // Every preserved row survives, including those without a value.
        let result = forge
            .execute(&format!("{WIDE_FIRST_HOP}RETURN b.ident AS n"))
            .expect("statement executes");
        let mut expected: Vec<Option<i64>> = targets.iter().copied().map(ident).collect();
        expected.sort_unstable();
        assert_eq!(
            idents(&result.batches),
            expected,
            "partitions {target_partitions}"
        );
    }
    assert!(
        un_nominated.is_empty(),
        "scans without a frontier: {}",
        un_nominated.join("\n")
    );
}
