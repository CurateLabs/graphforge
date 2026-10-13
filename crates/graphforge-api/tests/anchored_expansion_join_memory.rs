//! A traversal anchored on one node is planned for the rows it reaches from
//! that node, not for the graph (#1985).
//!
//! A join whose build side is estimated beyond half the query memory pool is
//! planned as a spilling sort-merge join, which reads the whole property route
//! of an enrichment instead of the rows of the frontier that reaches it. An
//! `{id: $x}` anchor reads at most the rows of the index shards that can hold
//! the value, so the seed of the traversal is a handful of rows and so is the
//! build side that follows it. The estimate must say so: the gate runs under
//! the smallest pool the facade admits, where the route's row count alone is
//! above the join's share of the pool, and counts bytes through the lifecycle
//! read-path counters, never time.
//!
//! The fixture is a ring: node `i` links forward to `i + 1` and `i + 2`, so a
//! node starts 14 trails of one to three hops, and the route of the graph is
//! above the smallest pool's share for a join. `id` is a permutation of the
//! node index, so no fragment's footer statistics exclude a value and only the
//! persisted equality index finds one row.

use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryBuilder, Int64Array, Int64Builder, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use graphforge_api::{
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, ExecutionResourcePolicy, ExplainStage,
    GraphConstructionBudgets, GraphForge, GraphForgeOptions, LifecycleIoCapture,
    ResourcePolicyMode, StorageIoPhase, lifecycle_io_snapshot,
};
use graphforge_core::uuid::Uuid;
use graphforge_ir::IrLiteral;

const WRITE_WINDOW: usize = 4 * 1024;
const PADDING_BYTES: usize = 64;
/// Above the 131,072 rows below which DataFusion collects a join side.
const SMALL_NODES: usize = 140_000;
const LARGE_NODES: usize = 2 * SMALL_NODES;
/// The hops of the anchored statements and of the unanchored one.
const ANCHORED_HOPS: usize = 3;
const UNANCHORED_HOPS: usize = 3;
/// The ring node the anchored statements start from.
const ANCHOR_NODE: usize = 5;
/// Nodes whose index is this modulo 1,000 have no `ident`, so an enrichment
/// that loses a preserved row shows as a missing value.
const MISSING_IDENT: usize = 9;
const MIB: u64 = 1024 * 1024;
/// The smallest pool the facade admits: half of it bounds a join's build side.
const SMALL_POOL: u64 = 16 * MIB;
const LARGE_POOL: u64 = 2048 * MIB;
const INDEX_IN_PLAN: &str = "index=property_equality(id)";

/// The traversal reaches the nodes one to three hops from the anchor, and the
/// enrichment reads their `ident`.
const ANCHORED: &str =
    "MATCH (m:Entity {id: $x})-[:LINK*1..3]->(p:Entity) RETURN sum(p.ident) AS n";
/// The same traversal and enrichment behind a type test, the shape of a
/// message that may be a post or a comment.
const ANCHORED_BEHIND_A_TYPE_TEST: &str = "MATCH (m)-[:LINK*1..3]->(p:Entity) \
     WHERE (m:Entity OR m:Seed) AND m.id = $x RETURN sum(p.ident) AS n";
/// The seeds are every node, so the traversal reaches the graph's trails (14
/// from each node), and the enrichment reads two columns of the route, which
/// alone is above the smallest pool's share for a join.
const UNANCHORED: &str = "MATCH (m:Entity)-[:LINK*1..3]->(p:Entity) \
     RETURN sum(p.ident) AS n, count(p.padding) AS pads";

fn label(index: usize) -> &'static str {
    if index % 1000 == 1 { "Seed" } else { "Entity" }
}

fn ident(index: usize) -> Option<i64> {
    (index % 1000 != MISSING_IDENT).then_some(index as i64)
}

/// A permutation of `0..nodes` that scatters every value across all fragments.
fn scattered(index: usize, nodes: usize) -> i64 {
    ((index * 7919 + 13) % nodes) as i64
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

/// The nodes a node links to.
fn neighbours(index: usize, nodes: usize) -> [usize; 2] {
    [(index + 1) % nodes, (index + 2) % nodes]
}

/// A ring of `nodes` nodes, each linking forward to the next two.
fn construct(dir: &Path, nodes: usize) {
    let mut fields = CONSTRUCTION_NODE_SCHEMA.fields().to_vec();
    fields.push(Arc::new(Field::new("id", DataType::Int64, true)));
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
        let mut uuids = FixedSizeBinaryBuilder::with_capacity(end - start, 16);
        let mut ids = Int64Builder::with_capacity(end - start);
        let mut idents = Int64Builder::with_capacity(end - start);
        for node in start..end {
            uuids.append_value(node_uuid(node).as_bytes()).unwrap();
            ids.append_value(scattered(node, nodes));
            idents.append_option(ident(node));
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&node_schema),
            vec![
                Arc::new(uuids.finish()) as ArrayRef,
                Arc::new(StringArray::from_iter_values((start..end).map(label))),
                Arc::new(ids.finish()),
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
            let target = neighbours(source, nodes)[edge % 2];
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

/// The nodes one to `hops` hops from `start`, one per trail.
fn trails(start: usize, nodes: usize, hops: usize) -> Vec<usize> {
    let mut reached = Vec::new();
    let mut frontier = vec![start];
    for _ in 0..hops {
        frontier = frontier
            .iter()
            .flat_map(|node| neighbours(*node, nodes))
            .collect();
        reached.extend(
            frontier
                .iter()
                .copied()
                .filter(|node| label(*node) == "Entity"),
        );
    }
    reached
}

/// What `sum(p.ident)` returns for the trails of `starts`.
fn expected_sum(starts: impl IntoIterator<Item = usize>, nodes: usize, hops: usize) -> i64 {
    starts
        .into_iter()
        .flat_map(|start| trails(start, nodes, hops))
        .filter_map(ident)
        .sum()
}

/// A session under an explicit pool of `memory_budget_bytes`.
fn open(project: &Path, memory_budget_bytes: u64) -> GraphForge {
    GraphForge::new_with_options(
        Some(project.to_str().expect("utf-8 path")),
        GraphForgeOptions {
            resource: ExecutionResourcePolicy {
                mode: ResourcePolicyMode::Explicit,
                tokio_worker_threads: Some(2),
                target_partitions: Some(2),
                io_concurrency: Some(2),
                compute_threads: Some(1),
                memory_budget_bytes: Some(memory_budget_bytes),
                ..ExecutionResourcePolicy::default()
            },
            ..GraphForgeOptions::default()
        },
    )
    .unwrap()
}

fn parameters(x: i64) -> std::collections::HashMap<String, IrLiteral> {
    std::collections::HashMap::from([("x".to_owned(), IrLiteral::Int(x))])
}

/// The plan of `query` with its anchor given as a literal.
fn plan(forge: &GraphForge, query: &str, anchor: i64) -> String {
    forge
        .explain_stage(
            &query.replace("$x", &anchor.to_string()),
            ExplainStage::PhysicalPlan,
        )
        .expect("physical plan")
}

/// The read-path bytes one statement read, and the value it returned.
fn measured(forge: &GraphForge, query: &str, anchor: i64) -> (i64, u64) {
    let _capture = LifecycleIoCapture::install();
    let before = lifecycle_io_snapshot().expect("requested observation");
    let result = forge
        .execute_with_params(query, &parameters(anchor))
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
        delta.phases[&StorageIoPhase::ReadPathScan].read_bytes,
    )
}

/// Run `query` on a warm session: the first statement admits the route's
/// footers and walks its keys once per session, and the measured one is the
/// steady state.
fn steady(forge: &GraphForge, query: &str, warm_anchor: i64, anchor: i64) -> (i64, u64) {
    measured(forge, query, warm_anchor);
    measured(forge, query, anchor)
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

/// The join operator directly above the traversal in the physical plan: the
/// nearest line above `ExpandExec` that is indented less and names a join,
/// skipping the exchanges, sorts and projections between them.
fn join_above_traversal(plan: &str) -> Option<&str> {
    let lines: Vec<&str> = plan.lines().collect();
    let indent = |line: &str| line.len() - line.trim_start().len();
    let expand = lines.iter().position(|line| line.contains("ExpandExec"))?;
    let mut depth = indent(lines[expand]);
    for line in lines[..expand].iter().rev() {
        if indent(line) < depth {
            if line.contains("JoinExec") {
                return Some(line.trim());
            }
            depth = indent(line);
        }
    }
    None
}

/// What the plan of an anchored statement must show: the equality index finds
/// the seed, the join above the traversal is a hash join, no join of the plan
/// is a sort-merge join, and every property scan but the anchor's own waits
/// for the ids of the frontier that reaches it.
fn assert_anchored_plan_keeps_the_hash_join(name: &str, plan: &str) {
    assert!(plan.contains(INDEX_IN_PLAN), "{name}: seed index:\n{plan}");
    assert!(
        join_above_traversal(plan).is_some_and(|join| join.starts_with("HashJoinExec")),
        "{name}: a build side of the rows one anchor reaches fits the pool:\n{plan}"
    );
    assert!(
        !plan.contains("SortMergeJoinExec"),
        "{name}: no join of an anchored traversal spills:\n{plan}"
    );
    let scans = plan.matches("PropertyOverlayExec").count();
    let taps = plan.matches("UuidBuildKeyTapExec").count();
    assert!(taps >= 1, "{name}: an enrichment scan is pruned:\n{plan}");
    assert_eq!(
        taps + 1,
        scans,
        "{name}: every scan but the anchor's own waits for a frontier:\n{plan}"
    );
}

/// An anchored traversal feeding an enrichment under the smallest pool keeps
/// its hash join and the pruned scan of the property route: the reads of the
/// statement are those of the frontier, so doubling the nodes that are not on
/// it leaves them where they were. Without a bound on the seed the route's
/// row count stands for the seed, the join is planned as a sort-merge join,
/// and the whole route is read for every statement.
#[test]
fn an_anchored_traversal_keeps_its_hash_join_and_its_pruned_enrichment() {
    let _serial = serial();
    let small = build(SMALL_NODES);
    let large = build(LARGE_NODES);
    for (name, query) in [
        ("anchored", ANCHORED),
        ("behind a type test", ANCHORED_BEHIND_A_TYPE_TEST),
    ] {
        let mut reads = Vec::new();
        for (built, nodes) in [(&small, SMALL_NODES), (&large, LARGE_NODES)] {
            let anchor = scattered(ANCHOR_NODE, nodes);
            let forge = open(&built.project, SMALL_POOL);
            assert_anchored_plan_keeps_the_hash_join(name, &plan(&forge, query, anchor));
            let (value, bytes) = steady(&forge, query, scattered(ANCHOR_NODE + 1, nodes), anchor);
            assert_eq!(
                value,
                expected_sum([ANCHOR_NODE], nodes, ANCHORED_HOPS),
                "{name}, {nodes} nodes"
            );
            reads.push(bytes);
        }
        // Finding the anchor reads the index, a fixed amount, so the reads of
        // the two sizes differ by what the route's growth costs the topology.
        assert!(
            reads[1] <= reads[0] + (256 << 10),
            "{name}: reads grew with unrelated nodes: {reads:?}"
        );
    }
}

/// A value no index shard can hold is a bound of zero rows, which the plan
/// must not take for an empty route: the statement keeps its plan and answers
/// with no trail.
#[test]
fn an_anchor_beyond_every_index_shard_keeps_the_plan_and_finds_nothing() {
    let _serial = serial();
    let built = build(SMALL_NODES);
    let forge = open(&built.project, SMALL_POOL);
    let missing = SMALL_NODES as i64 + 7;
    assert_anchored_plan_keeps_the_hash_join("missing anchor", &plan(&forge, ANCHORED, missing));
    let (value, _) = measured(&forge, ANCHORED, missing);
    assert_eq!(value, 0, "no node holds the id");
}

/// The estimate is the seeds' rows times the rows a seed reaches, not a
/// constant: a traversal from every node, whose trails and route cannot fit
/// the smallest pool, is still planned as a spilling sort-merge join, and
/// keeps the hash join where they fit.
#[test]
fn a_traversal_from_every_node_that_exceeds_the_pool_is_planned_as_a_sort_merge_join() {
    let _serial = serial();
    let built = build(SMALL_NODES);

    // The smallest pool the facade admits cannot run the sorts of any
    // sort-merge join (each reserves 10 MiB to merge its spills), so the plan
    // is checked there and the answer under the pool that holds the hash join.
    let small = open(&built.project, SMALL_POOL);
    let small_plan = plan(&small, UNANCHORED, 0);
    assert!(
        join_above_traversal(&small_plan).is_some_and(|join| join.starts_with("SortMergeJoinExec")),
        "{SMALL_NODES} seeds with 14 trails each cannot fit {SMALL_POOL} bytes of pool:\n{small_plan}"
    );

    let large = open(&built.project, LARGE_POOL);
    let large_plan = plan(&large, UNANCHORED, 0);
    assert!(
        join_above_traversal(&large_plan).is_some_and(|join| join.starts_with("HashJoinExec")),
        "a build side that fits keeps the hash join:\n{large_plan}"
    );
    let (value, _) = measured(&large, UNANCHORED, 0);
    assert_eq!(
        value,
        expected_sum(
            (0..SMALL_NODES).filter(|node| label(*node) == "Entity"),
            SMALL_NODES,
            UNANCHORED_HOPS
        ),
        "the answer under the pool that holds the hash join"
    );
}
