//! An anchored variable-length expansion reads what its frontier costs, not
//! what the graph costs, and a read writes nothing (#1984).
//!
//! The fixture is a ring: node `i` links forward to `i + 1` and `i + 2`, so an
//! anchored `*1..3` expansion reaches the same 14 paths however many nodes the
//! graph has. The route is several property fragments wide, with incompressible
//! text on every node, so any read that scales with the route shows up as
//! bytes. The gate is deterministic and counts bytes, never time: the lifecycle
//! read-path counters count the application's reads and writes through the
//! storage layer.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use arrow::array::{ArrayRef, FixedSizeBinaryBuilder, Int64Array, Int64Builder, StringArray};
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
const PADDING_BYTES: usize = 1024;
const SMALL_NODES: usize = 12_288;
const LARGE_NODES: usize = 2 * SMALL_NODES;
/// Paths of one to three forward hops from a node: 2 + 4 + 8.
const PATHS: i64 = 14;
const ANCHOR: i64 = 5;

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
    let mut edge_fields = CONSTRUCTION_EDGE_SCHEMA.fields().to_vec();
    edge_fields.push(Arc::new(Field::new("weight", DataType::Int64, true)));
    let edge_schema = Arc::new(Schema::new(edge_fields));
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
            idents.append_value(node as i64);
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
    let edges = nodes * 2;
    for start in (0..edges).step_by(WRITE_WINDOW) {
        let end = (start + WRITE_WINDOW).min(edges);
        let rows = end - start;
        let mut ids = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut sources = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut targets = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut weights = Int64Builder::with_capacity(rows);
        for edge in start..end {
            let source = edge / 2;
            ids.append_value(edge_uuid(edge).as_bytes()).unwrap();
            sources.append_value(node_uuid(source).as_bytes()).unwrap();
            targets
                .append_value(node_uuid((source + edge % 2 + 1) % nodes).as_bytes())
                .unwrap();
            weights.append_value(edge as i64);
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

/// What one statement read and wrote.
#[derive(Debug, Clone, Copy)]
struct Io {
    read_bytes: u64,
    /// Every phase: edge records, nodes and edge properties are not all read in
    /// the read-path phase.
    total_read_bytes: u64,
    total_read_calls: u64,
    write_bytes: u64,
    write_calls: u64,
}

fn measured(forge: &GraphForge, query: &str, ident: i64) -> (i64, Io) {
    let _capture = LifecycleIoCapture::install();
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
            total_read_bytes: delta.totals.read_bytes,
            total_read_calls: delta.totals.read_calls,
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

/// Node offsets reached by every forward path of one to three hops.
fn reached_offsets() -> Vec<i64> {
    let mut frontier = vec![0_i64];
    let mut reached = Vec::new();
    for _ in 0..3 {
        frontier = frontier
            .iter()
            .flat_map(|offset| [offset + 1, offset + 2])
            .collect();
        reached.extend(frontier.iter().copied());
    }
    reached
}

/// Each anchored statement with the value it must return for `ANCHOR`.
fn statements() -> Vec<(&'static str, &'static str, i64)> {
    let offsets = reached_offsets();
    assert_eq!(offsets.len() as i64, PATHS);
    let sum_idents = offsets.iter().map(|offset| ANCHOR + offset).sum::<i64>();
    vec![
        (
            "labelled one hop",
            "MATCH (a:Entity {ident: $ident})-[:LINK]->(b:Entity) RETURN count(*) AS n",
            2,
        ),
        (
            "paths",
            "MATCH (a:Entity {ident: $ident})-[:LINK*1..3]->(b:Entity) RETURN count(*) AS n",
            PATHS,
        ),
        (
            "destination property",
            "MATCH (a:Entity {ident: $ident})-[:LINK*1..3]->(b:Entity) RETURN sum(b.ident) AS n",
            sum_idents,
        ),
        (
            "relationship list length",
            "MATCH (a:Entity {ident: $ident})-[r:LINK*1..3]->(b:Entity) RETURN sum(size(r)) AS n",
            2 + 4 * 2 + 8 * 3,
        ),
        (
            "relationship property",
            "MATCH (a:Entity {ident: $ident})-[r:LINK*1..3]->(b:Entity) \
             RETURN count(r[0].weight) AS n",
            PATHS,
        ),
    ]
}

/// Doubling the nodes that are not on the frontier leaves every statement's
/// reads where they were: the frontier is fixed, so the bytes are too.
#[test]
fn anchored_variable_length_reads_do_not_grow_with_unrelated_nodes() {
    let _serial = serial();
    let small = build(SMALL_NODES);
    let large = build(LARGE_NODES);
    let mut grew = Vec::new();
    for target_partitions in [1, 4] {
        for (name, query, expected) in statements() {
            let mut reads = Vec::new();
            for built in [&small, &large] {
                let forge = open(&built.project, target_partitions);
                // The first statement admits each route's footers and walks its
                // keys once per session; the measured one is the steady state.
                let (warm, _) = measured(&forge, query, 0);
                assert!(warm > 0, "{name}: warm-up returned {warm}");
                let (value, io) = measured(&forge, query, ANCHOR);
                assert_eq!(value, expected, "{name}, partitions {target_partitions}");
                assert_eq!(io.write_bytes, 0, "{name}: {io:?}");
                assert_eq!(io.write_calls, 0, "{name}: {io:?}");
                reads.push(io.read_bytes);
            }
            if reads[1] > reads[0] + (256 << 10) {
                grew.push(format!("{name}, partitions {target_partitions}: {reads:?}"));
            }
        }
    }
    assert!(
        grew.is_empty(),
        "reads grew with unrelated nodes: {grew:#?}"
    );
}

/// A seed whose paths fill several output batches reads what a seed that fits
/// in one batch reads: the edge records, destination nodes and edge properties
/// are read once for the seed and sliced, not read again for every batch.
///
/// Forward paths of one to `h` hops from a node number 2^(h+1) - 2, and an
/// output batch holds 65,536 of them, so `*1..15` fits in one batch (65,534
/// paths) and `*1..16` needs two (131,070).
#[test]
fn a_seed_spanning_several_output_batches_reads_what_one_batch_reads() {
    let _serial = serial();
    let built = build(SMALL_NODES);
    let query = |hops: u32| {
        format!(
            "MATCH (a:Entity {{ident: $ident}})-[r:LINK*1..{hops}]->(b:Entity) \
             RETURN count(r[0].weight) AS n"
        )
    };
    for target_partitions in [1, 4] {
        let mut reads = Vec::new();
        for hops in [15_u32, 16] {
            let forge = open(&built.project, target_partitions);
            let (warm, _) = measured(&forge, &query(hops), 0);
            assert!(warm > 0);
            let (paths, io) = measured(&forge, &query(hops), ANCHOR);
            assert_eq!(paths, (1_i64 << (hops + 1)) - 2, "{hops} hops");
            assert_eq!(io.write_bytes, 0, "{io:?}");
            assert_eq!(io.write_calls, 0, "{io:?}");
            reads.push((io.total_read_bytes, io.total_read_calls));
        }
        // The second statement reaches one more hop level of the ring, which
        // adds a handful of edges; a second pass over the route would add its
        // whole key column.
        assert!(
            reads[1].0 <= reads[0].0 + (64 << 10) && reads[1].1 <= reads[0].1 + 16,
            "partitions {target_partitions}: a two-batch seed read more than a one-batch seed \
             (bytes, calls): {reads:?}"
        );
    }
}
