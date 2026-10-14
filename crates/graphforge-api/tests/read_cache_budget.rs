//! #1987: the read caches of a traversal are bounded by one share of the query
//! memory budget, and they account for one another.
//!
//! A traversal that reads an edge property over an indexed graph retains three
//! things for the next query: the decoded CSR shards of the adjacency index,
//! the node label index, and the edge-id map of the edge-property route. The
//! provider derives their one budget from the query memory budget `B` with
//! `graphforge_storage::read_cache_budget`: the topology share `P = B / 4`. The
//! label index and the map build under `P` less what the others already hold,
//! and are declined when they would not fit; the shards keep the rest.
//!
//! Construction stores every relationship type's properties in one route, so
//! the map is 16 B per edge of the whole graph. The fixture is indexed under the
//! smallest explicit budget, 16 MiB, which sizes a CSR shard at 87,381 edges and
//! gives the tight share `P = 4 MiB`. The invariants asserted are properties of
//! the formula, not of the observed byte counts: the total never exceeds `P`
//! while the unconstrained run holds more than `P`, so the tight run really is
//! under pressure.

#![cfg(target_os = "linux")]

use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryBuilder, Int64Array, Int64Builder, RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use graphforge_api::{
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, ExecutionResourcePolicy,
    GraphConstructionBudgets, GraphForge, GraphForgeOptions, LifecycleIoCapture,
    ResourcePolicyMode, RetainedTopologyBytes,
};
use graphforge_core::uuid::Uuid;
use graphforge_storage::io_stats;
use graphforge_storage::read_cache_budget::ReadCacheBudget;

const WINDOW: usize = 4_096;
const NODES: usize = 20_000;
/// The graph whose map (16 B per edge, about 1.9 MB) fits the tight share beside
/// the label index and some shards: 120,000 edges, two shards per direction.
const FITTING_EDGES: usize = 120_000;
/// The graph whose map (about 4.8 MB) is larger than the tight share of 4.19 MB:
/// 300,000 edges, four shards per direction.
const OVERSIZED_EDGES: usize = 300_000;
/// The facade's smallest explicit query memory budget.
const TIGHT_BUDGET: u64 = 16 * 1024 * 1024;
/// Large enough that nothing is evicted or declined.
const ROOMY_BUDGET: u64 = 512 * 1024 * 1024;

const LINK_OUT: &str = "MATCH (a:Entity)-[e:LINK]->(b:Entity) RETURN sum(e.timestamp) AS n";
const LINK_IN: &str = "MATCH (b:Entity)<-[e:LINK]-(a:Entity) RETURN sum(e.timestamp) AS n";

/// `sum(e.timestamp)` over the edges: each stores its own number.
fn link_sum(edges: usize) -> i64 {
    (edges * (edges - 1) / 2) as i64
}

fn node_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x1000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

fn edge_uuid(slot: usize) -> Uuid {
    Uuid::from_u128(0x2000_0000_0000_0000_0000_0000_0000_0000 | (slot as u128 + 1))
}

fn construct(dir: &Path, edges: usize) {
    let mut node_fields = CONSTRUCTION_NODE_SCHEMA.fields().to_vec();
    node_fields.push(Arc::new(Field::new("ident", DataType::Int64, true)));
    let node_schema = Arc::new(Schema::new(node_fields));
    let mut edge_fields = CONSTRUCTION_EDGE_SCHEMA.fields().to_vec();
    edge_fields.push(Arc::new(Field::new("timestamp", DataType::Int64, true)));
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
    for start in (0..edges).step_by(WINDOW) {
        let end = (start + WINDOW).min(edges);
        let rows = end - start;
        let mut ids = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut sources = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut targets = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut timestamps = Int64Builder::with_capacity(rows);
        for edge in start..end {
            ids.append_value(edge_uuid(edge).as_bytes()).unwrap();
            sources
                .append_value(node_uuid(edge % NODES).as_bytes())
                .unwrap();
            targets
                .append_value(node_uuid((edge * 7 + 1) % NODES).as_bytes())
                .unwrap();
            timestamps.append_value(edge as i64);
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&edge_schema),
            vec![
                Arc::new(ids.finish()) as ArrayRef,
                Arc::new(StringArray::from(vec!["LINK"; rows])),
                Arc::new(sources.finish()),
                Arc::new(targets.finish()),
                Arc::new(timestamps.finish()),
            ],
        )
        .unwrap();
        session
            .append_edges(&format!("LINK-{start}"), &batch)
            .unwrap();
    }
    session.seal_and_publish().unwrap();
}

fn open(project: &Path, budget: u64) -> GraphForge {
    GraphForge::new_with_options(
        Some(project.to_str().expect("utf-8 path")),
        GraphForgeOptions {
            resource: ExecutionResourcePolicy {
                mode: ResourcePolicyMode::Explicit,
                tokio_worker_threads: Some(2),
                // One partition, so the I/O counters are recorded where the
                // statement runs.
                target_partitions: Some(1),
                io_concurrency: Some(2),
                compute_threads: Some(1),
                memory_budget_bytes: Some(budget),
                ..ExecutionResourcePolicy::default()
            },
            ..GraphForgeOptions::default()
        },
    )
    .unwrap()
}

/// The sum a statement returns, asserting it read the CSR index.
fn sum(forge: &GraphForge, query: &str) -> i64 {
    let plan = forge.explain(query).unwrap();
    assert!(
        plan.lines()
            .any(|line| line.contains("ExpandExec") && line.contains("adjacency=hit")),
        "{query} must read the CSR index:\n{plan}"
    );
    let batches = forge.execute(query).unwrap().batches;
    assert_eq!(batches.len(), 1, "{query}");
    let column = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("sum is Int64");
    assert_eq!(column.len(), 1, "{query}");
    column.value(0)
}

fn retained(forge: &GraphForge) -> RetainedTopologyBytes {
    forge.resource_diagnostics().retained_topology_bytes
}

static SERIAL: Mutex<()> = Mutex::new(());

/// The I/O counters are process-global, so the tests that read them take turns.
fn serial() -> MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct Built {
    _root: tempfile::TempDir,
    project: std::path::PathBuf,
    edges: usize,
}

/// A graph of `edges` relationships with an adjacency index of 87,381-edge
/// shards, the size the smallest explicit budget gives.
fn build(edges: usize) -> Built {
    let root = tempfile::tempdir().expect("project directory");
    let project = root.path().join("project");
    construct(&project, edges);
    open(&project, TIGHT_BUDGET).index_adjacency().unwrap();
    Built {
        _root: root,
        project,
        edges,
    }
}

fn tight_share() -> u64 {
    ReadCacheBudget::from_query_budget(TIGHT_BUDGET).topology_bytes
}

/// The shards, the label index and the map together stay within the topology
/// share `P = B / 4` however much the unconstrained run would hold, and a
/// write releases the label index and the map.
#[test]
fn the_topology_caches_together_stay_within_their_share_of_the_budget() {
    let _serial = serial();
    let built = build(FITTING_EDGES);
    let expected = link_sum(built.edges);

    // Unconstrained: the share is 128 MiB, so nothing is evicted or declined.
    let roomy = open(&built.project, ROOMY_BUDGET);
    for query in [LINK_OUT, LINK_IN] {
        assert_eq!(sum(&roomy, query), expected);
    }
    let all = retained(&roomy);
    assert_eq!(
        all.budget,
        ReadCacheBudget::from_query_budget(ROOMY_BUDGET).topology_bytes
    );
    assert!(
        all.shards > 0 && all.label_index > 0 && all.edge_id_maps > 0,
        "every consumer retains something: {all:?}"
    );
    assert!(all.total() <= all.budget, "{all:?}");
    assert!(
        all.total() > tight_share(),
        "the unconstrained working set must exceed the tight share, or the tight run \
         below is not under pressure: {all:?} against {}",
        tight_share()
    );
    drop(roomy);

    // Tight: the share is 4 MiB; the map and the label index hold about half.
    let tight = open(&built.project, TIGHT_BUDGET);
    for _ in 0..2 {
        for query in [LINK_OUT, LINK_IN] {
            assert_eq!(sum(&tight, query), expected);
        }
        let held = retained(&tight);
        assert_eq!(held.budget, tight_share(), "{held:?}");
        assert!(
            tight
                .resource_diagnostics()
                .retained_inventory_metadata_bytes as u64
                <= ReadCacheBudget::from_query_budget(TIGHT_BUDGET).inventory_bytes
        );
        assert_eq!(held.label_index, all.label_index, "{held:?}");
        assert_eq!(held.edge_id_maps, all.edge_id_maps, "{held:?}");
        // The shard cache retains at most its capacity, `P` less the label
        // index and the map, or the one shard being decoded when that alone is
        // larger. The capacity here exceeds any shard, so the total stays in `P`.
        assert!(
            held.total() <= held.budget,
            "topology caches hold {} bytes against a {} byte share: {held:?}",
            held.total(),
            held.budget
        );
        assert!(
            held.shards > 0 && held.shards < all.shards,
            "the tight run must evict shards ({} of {} bytes retained): {held:?}",
            held.shards,
            all.shards
        );
    }

    // A write drops every view the provider holds, and with them the bytes the
    // label index and the map were charged.
    tight
        .execute("CREATE (:Entity {ident: -1})")
        .expect("the write executes");
    let dropped = retained(&tight);
    assert_eq!(dropped.label_index, 0, "{dropped:?}");
    assert_eq!(dropped.edge_id_maps, 0, "{dropped:?}");
    assert_eq!(sum(&tight, LINK_OUT), expected);
    let rebuilt = retained(&tight);
    assert_eq!(rebuilt.edge_id_maps, all.edge_id_maps, "{rebuilt:?}");
    assert!(rebuilt.total() <= rebuilt.budget, "{rebuilt:?}");
}

/// A map that does not fit under the share is declined once and remembered; the
/// statement still returns the stored values, the shards take the whole share,
/// and an unconstrained budget builds the map.
#[test]
fn a_map_larger_than_the_share_is_declined_and_the_shards_take_what_it_would_have_held() {
    let _serial = serial();
    let built = build(OVERSIZED_EDGES);
    let expected = link_sum(built.edges);
    let _capture = LifecycleIoCapture::install();

    let tight = open(&built.project, TIGHT_BUDGET);
    io_stats::reset();
    for _ in 0..2 {
        for query in [LINK_OUT, LINK_IN] {
            assert_eq!(sum(&tight, query), expected);
        }
    }
    let io = io_stats::snapshot().expect("requested I/O statistics");
    assert_eq!(io.edge_property_id_map_builds, 0, "{io:?}");
    assert_eq!(
        io.edge_property_id_map_declines, 1,
        "the decline is remembered, not retried by each statement: {io:?}"
    );
    let held = retained(&tight);
    assert_eq!(held.edge_id_maps, 0, "{held:?}");
    assert!(held.label_index > 0 && held.shards > 0, "{held:?}");
    assert!(held.total() <= held.budget, "{held:?}");
    drop(tight);

    // Unconstrained, the same map fits and is built; it is larger than the
    // tight share, so a tight run could not have retained it.
    let roomy = open(&built.project, ROOMY_BUDGET);
    io_stats::reset();
    for query in [LINK_OUT, LINK_IN] {
        assert_eq!(sum(&roomy, query), expected);
    }
    let io = io_stats::snapshot().expect("requested I/O statistics");
    assert_eq!(io.edge_property_id_map_builds, 1, "{io:?}");
    assert_eq!(io.edge_property_id_map_declines, 0, "{io:?}");
    let all = retained(&roomy);
    assert!(all.edge_id_maps > tight_share(), "{all:?}");
    assert!(all.total() <= all.budget, "{all:?}");
}

/// A small pool refuses the decoder before page allocation, even when the
/// requested rows themselves would fit. The same graph and query complete
/// with a roomy budget, so refusal is resource admission rather than invalid
/// topology or a binding-only check.
#[test]
fn edge_record_decode_is_admitted_before_reading_parquet_pages() {
    let _serial = serial();
    let built = build(FITTING_EDGES);
    let query = "MATCH (a:Entity)-[e:LINK]->(b:Entity) RETURN e";
    let tight = open(&built.project, TIGHT_BUDGET);
    let message = tight
        .execute(query)
        .expect_err("the full edge decode cannot fit the tight pool")
        .to_string();
    assert!(
        message.contains("Resources exhausted") && message.contains("ExpandExec hop read"),
        "{message}"
    );
    drop(tight);
    let roomy = open(&built.project, ROOMY_BUDGET);
    let result = roomy
        .execute(query)
        .expect("the same decoder fits a roomy budget");
    assert_eq!(
        result
            .batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        built.edges
    );
}
