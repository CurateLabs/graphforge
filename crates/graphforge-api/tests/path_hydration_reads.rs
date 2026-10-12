//! Hydrating the nodes of a path reads what the path costs, not what the
//! property route costs, and a read writes nothing (#1984).
//!
//! `[n IN nodes(p) | n.ident]` resolves the properties of every node a path
//! visits. It used to visit every batch of the whole property route and keep the
//! rows of the path's UUIDs, so the cost of a three-node path grew with the
//! graph. The fixture is a ring: node `i` links forward to `i + 1` and `i + 2`,
//! so an anchored `*1..3` expansion reaches the same 14 paths however many
//! nodes the graph has. The route is several property fragments wide, with
//! incompressible text on every node, so any read that scales with the route
//! shows up as bytes. The gate counts bytes, never time: the lifecycle
//! read-path counters count the application's reads and writes through the
//! storage layer.
//!
//! The expansion that produces the paths has its own reads. The hydration's
//! share is what the hydrating statement reads beyond the same expansion
//! without it, taken on the same project in the same session.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryBuilder, Int64Array, Int64Builder, ListArray, StringArray,
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
const PADDING_BYTES: usize = 1024;
const SMALL_NODES: usize = 12_288;
const LARGE_NODES: usize = 2 * SMALL_NODES;
/// Offset from the last node of the graph to the anchor. The path nodes sit at
/// the end of the property route, so a read that scans the route from its start
/// has to cross all of it, whatever the graph's size.
const ANCHOR_FROM_END: i64 = 40;
/// The anchor of the mutation test: near the start, where its mutated nodes are.
const MUTATED_ANCHOR: i64 = 5;

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

fn measured(forge: &GraphForge, query: &str, ident: i64) -> (Vec<RecordBatch>, Io) {
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
    (
        result.batches,
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

fn anchor(nodes: usize) -> i64 {
    nodes as i64 - ANCHOR_FROM_END
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

/// Every path of one to three forward hops, as the offsets its nodes sit at
/// from the anchor.
fn path_offsets() -> Vec<Vec<i64>> {
    let mut paths = Vec::new();
    let mut frontier = vec![vec![0_i64]];
    for _ in 0..3 {
        frontier = frontier
            .iter()
            .flat_map(|path| {
                let last = *path.last().expect("a path has a node");
                [1, 2].map(|step| {
                    let mut next = path.clone();
                    next.push(last + step);
                    next
                })
            })
            .collect();
        paths.extend(frontier.iter().cloned());
    }
    assert_eq!(paths.len(), 14);
    paths
}

/// The same paths, with the same expansion and its relationship list, so that
/// the difference between the two statements is the hydration of their nodes.
const HYDRATED: &str = "MATCH p = (a:Entity {ident: $ident})-[r:LINK*1..3]->(b:Entity) \
                        RETURN [n IN nodes(p) | n.ident] AS ids";
const EXPANDED: &str = "MATCH p = (a:Entity {ident: $ident})-[r:LINK*1..3]->(b:Entity) \
                        RETURN sum(size(r)) AS n";

fn id_lists(batches: &[RecordBatch]) -> Vec<Vec<Option<i64>>> {
    let mut lists = Vec::new();
    for batch in batches {
        let ids = batch
            .column_by_name("ids")
            .expect("ids column")
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("ids is a list");
        for row in 0..batch.num_rows() {
            let values = ids.value(row);
            let values = values
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("ident is Int64");
            lists.push(
                (0..values.len())
                    .map(|index| (!values.is_null(index)).then(|| values.value(index)))
                    .collect(),
            );
        }
    }
    lists.sort();
    lists
}

fn expected_lists(anchor: i64, ident_of: impl Fn(i64) -> Option<i64>) -> Vec<Vec<Option<i64>>> {
    let mut lists = path_offsets()
        .into_iter()
        .map(|path| {
            path.into_iter()
                .map(|offset| ident_of(anchor + offset))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    lists.sort();
    lists
}

/// Doubling the nodes that are not on the paths leaves what hydrating them reads
/// where it was, at every partition count, and the values are the paths'.
#[test]
fn hydrating_path_nodes_reads_do_not_grow_with_unrelated_nodes() {
    let _serial = serial();
    let small = build(SMALL_NODES);
    let large = build(LARGE_NODES);
    let mut grew = Vec::new();
    for target_partitions in [1, 4] {
        let mut hydration = Vec::new();
        for (built, nodes) in [(&small, SMALL_NODES), (&large, LARGE_NODES)] {
            let anchor = anchor(nodes);
            let forge = open(&built.project, target_partitions);
            // The first statements admit each route's footers once per session;
            // the measured ones are the steady state.
            for query in [HYDRATED, EXPANDED] {
                let (warm, _) = measured(&forge, query, 0);
                assert!(!warm.is_empty(), "warm-up returned no rows");
            }
            let (hydrated, hydrated_io) = measured(&forge, HYDRATED, anchor);
            let (_, expanded_io) = measured(&forge, EXPANDED, anchor);
            assert_eq!(
                id_lists(&hydrated),
                expected_lists(anchor, Some),
                "partitions {target_partitions}"
            );
            for io in [hydrated_io, expanded_io] {
                assert_eq!(io.write_bytes, 0, "{io:?}");
                assert_eq!(io.write_calls, 0, "{io:?}");
            }
            hydration.push(
                hydrated_io
                    .read_bytes
                    .saturating_sub(expanded_io.read_bytes),
            );
        }
        eprintln!(
            "partitions {target_partitions}: hydration reads at {SMALL_NODES} and {LARGE_NODES} \
             nodes: {hydration:?}"
        );
        if hydration[1] > hydration[0] + (256 << 10) {
            grew.push(format!(
                "partitions {target_partitions}: hydration reads {hydration:?}"
            ));
        }
    }
    assert!(
        grew.is_empty(),
        "hydrating path nodes read more as unrelated nodes doubled: {grew:#?}"
    );
}

/// A newer snapshot decides the value a path node hydrates to, whichever
/// fragment holds it, a removed property hydrates to null, and a reopened
/// project agrees.
#[test]
fn hydrated_path_nodes_resolve_the_newest_snapshot_of_each_node() {
    let _serial = serial();
    let built = build(SMALL_NODES);
    let forge = open(&built.project, 1);
    forge
        .execute("MATCH (a:Entity {ident: 7}) SET a.ident = 70000")
        .expect("update");
    forge
        .execute("MATCH (a:Entity {ident: 8}) REMOVE a.ident")
        .expect("removal");
    drop(forge);
    let ident_of = |index: i64| match index {
        7 => Some(70_000),
        8 => None,
        other => Some(other),
    };
    for partitions in [1, 4, 1] {
        let forge = open(&built.project, partitions);
        let (hydrated, io) = measured(&forge, HYDRATED, MUTATED_ANCHOR);
        assert_eq!(
            id_lists(&hydrated),
            expected_lists(MUTATED_ANCHOR, ident_of)
        );
        assert_eq!(io.write_bytes, 0, "{io:?}");
        assert_eq!(io.write_calls, 0, "{io:?}");
    }
}
