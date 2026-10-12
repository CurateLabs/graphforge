//! A node variable's properties are read for the rows that reach it, not for
//! the route that holds them, whether or not an equality anchors the frontier
//! and whether or not the projection keeps the node's UUID (#1984).
//!
//! The fixture is a ring: node `i` links forward to `i + 1` and `i + 2`, for
//! the first few thousand nodes (the rest only add route). The
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
                Arc::new(StringArray::from(vec!["Entity"; end - start])),
                Arc::new(idents.finish()),
                Arc::new(StringArray::from_iter_values((start..end).map(padding))),
            ],
        )
        .unwrap();
        session
            .append_nodes(&format!("nodes-{start}"), &batch)
            .unwrap();
    }
    let edges = LINKED_NODES * 2;
    for start in (0..edges).step_by(WRITE_WINDOW) {
        let end = (start + WRITE_WINDOW).min(edges);
        let rows = end - start;
        let mut ids = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut sources = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut targets = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        for edge in start..end {
            let source = edge / 2;
            ids.append_value(edge_uuid(edge).as_bytes()).unwrap();
            sources.append_value(node_uuid(source).as_bytes()).unwrap();
            targets
                .append_value(node_uuid((source + edge % 2 + 1) % nodes).as_bytes())
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

fn measured(forge: &GraphForge, query: &str, ident: i64) -> (i64, Io) {
    let _capture = LifecycleIoCapture::install();
    let before = lifecycle_io_snapshot().expect("requested observation");
    let result = forge
        .execute_with_params(query, &parameters(ident))
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

/// How many property fragments a project's route spans.
fn property_fragments(project: &Path) -> usize {
    graphforge_storage::resolve_project_generation(project)
        .expect("project resolves")
        .unadmitted_graph_files_inventory()
        .expect("inventory reads")
        .expect("a constructed generation declares an inventory")
        .files
        .iter()
        .filter(|file| file.relative_path.starts_with("properties/"))
        .count()
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

/// Every property scan waits for the UUIDs of the frontier that reaches it,
/// whatever the partition count.
fn un_nominated_scans(forge: &GraphForge, partitions: usize) -> Vec<String> {
    let mut found = Vec::new();
    for (name, query, _) in statements() {
        let plan = forge
            .explain_stage(
                &query.replace("$ident", &ANCHOR.to_string()),
                ExplainStage::PhysicalPlan,
            )
            .expect("physical plan");
        let scans = plan.matches("PropertyOverlayExec").count();
        let taps = plan.matches("UuidBuildKeyTapExec").count();
        assert!(
            scans >= 3,
            "{name}: the anchor and two enrichments:\n{plan}"
        );
        if taps != scans {
            found.push(format!(
                "{name}, partitions {partitions}: {scans} property scans, {taps} taps:\n{plan}"
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

/// Run `query` on a warm session and return its value and I/O.
fn steady(forge: &GraphForge, name: &str, query: &str) -> (i64, Io) {
    // The first statement admits the route's footers and walks its keys once
    // per session; the measured one is the steady state.
    let (warm, _) = measured(forge, query, 0);
    assert!(warm > 0, "{name}: warm-up returned {warm}");
    let (value, io) = measured(forge, query, ANCHOR);
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
    assert!(
        property_fragments(&large.project) >= 2 * property_fragments(&small.project) - 1,
        "the route doubles with the nodes"
    );
    let mut grew = Vec::new();
    let mut un_nominated = Vec::new();
    for target_partitions in [1, 2, 4] {
        let small_forge = open(&small.project, target_partitions);
        let large_forge = open(&large.project, target_partitions);
        un_nominated.extend(un_nominated_scans(&small_forge, target_partitions));
        assert_preserved_rows_survive(&small_forge, target_partitions);
        // Locating the anchor reads the topology, which also doubles.
        let (_, anchor_small) = steady(&small_forge, "anchor", ANCHOR_ONLY);
        let (_, anchor_large) = steady(&large_forge, "anchor", ANCHOR_ONLY);
        let anchor_growth = anchor_large
            .read_bytes
            .saturating_sub(anchor_small.read_bytes);
        for (name, query, expected) in statements() {
            let (value, small_io) = steady(&small_forge, name, &query);
            assert_eq!(value, expected, "{name}, partitions {target_partitions}");
            let (value, large_io) = steady(&large_forge, name, &query);
            assert_eq!(value, expected, "{name}, partitions {target_partitions}");
            let reads = [small_io.read_bytes, large_io.read_bytes];
            if reads[1] > reads[0] + anchor_growth + (256 << 10) {
                grew.push(format!(
                    "{name}, partitions {target_partitions}: {reads:?}, anchor grew by {anchor_growth}"
                ));
            }
        }
    }
    assert!(
        grew.is_empty() && un_nominated.is_empty(),
        "reads grew with unrelated nodes: {grew:#?}\nscans without a frontier: {}",
        un_nominated.join("\n")
    );
}
