//! End-to-end fixed-hop `LIMIT` scale gate and release benchmark (#1248).
//!
//! The CI test asserts structural I/O bounds through the public `GraphForge`
//! facade. The ignored release test reports 1M/10M-edge wall time without a
//! brittle timing threshold.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use arrow::array::{FixedSizeBinaryArray, Int64Array, UInt64Array};
use graphforge_api::GraphForge;
use graphforge_core::uuid::{Uuid, new_v7};
use graphforge_core::{OntologyMode, TypeId};
use graphforge_exec::demand::DemandSnapshot;
use graphforge_ir::IrLiteral;
use graphforge_storage::adjacency::build_adjacency_index;
use graphforge_storage::{GraphWriter, io_stats};
use tempfile::TempDir;

#[path = "support/project_fixture.rs"]
mod project_fixture;

const TS: i64 = 1_700_000_000_000_000;
const NODE_TYPE: TypeId = TypeId(0);
const FAN_OUT: usize = 8;
const LIMIT: usize = 1_000;
const MAX_BATCH_ROWS: u64 = 8_192;

/// Serializes the process-global storage counters used by the assertions.
static IO_GUARD: Mutex<()> = Mutex::new(());

const ONE_HOP: &str = "MATCH (a)-[r]->(b) RETURN b.node_uuid AS id LIMIT 1000";
const TWO_HOP: &str = "MATCH (a)-[r1]->(b)-[r2]->(c) \
                       RETURN c.node_uuid AS id LIMIT 1000";

/// Deterministic ring: each node points to its next `fan_out` successors.
fn generate_graph(dir: &Path, nodes: usize, fan_out: usize) {
    assert!(nodes > fan_out);
    let workspace = TempDir::new().unwrap();
    let mut writer = GraphWriter::open_at(workspace.path(), OntologyMode::Exploratory, TS).unwrap();
    let uuids: Vec<Uuid> = (0..nodes).map(|_| new_v7()).collect();
    for uuid in &uuids {
        writer.create_node(*uuid, NODE_TYPE).unwrap();
    }
    for src in 0..nodes {
        for offset in 1..=fan_out {
            writer
                .create_edge(
                    new_v7(),
                    "LINK",
                    &uuids[src],
                    &uuids[(src + offset) % nodes],
                )
                .unwrap();
        }
    }
    writer.flush().unwrap();
    build_adjacency_index(workspace.path(), TS).unwrap();
    project_fixture::publish_graph_workspace(dir, workspace.path());
}

fn open_forge(dir: &Path) -> GraphForge {
    GraphForge::new(Some(dir.to_str().expect("temp path is UTF-8"))).unwrap()
}

fn uint64_values(result: &graphforge_api::ExecutionResult, column: &str) -> Vec<u64> {
    let mut values = Vec::new();
    for batch in &result.batches {
        let array = batch
            .column_by_name(column)
            .unwrap_or_else(|| panic!("missing {column} column"))
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap_or_else(|| panic!("{column} must be UInt64"));
        values.extend(array.values().iter().copied());
    }
    values
}

fn fixed_binary_values(result: &graphforge_api::ExecutionResult, column: &str) -> Vec<Vec<u8>> {
    let mut values = Vec::new();
    for batch in &result.batches {
        let array = batch
            .column_by_name(column)
            .unwrap_or_else(|| panic!("missing {column} column"))
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap_or_else(|| panic!("{column} must be FixedSizeBinary"));
        values.extend((0..batch.num_rows()).map(|row| array.value(row).to_vec()));
    }
    values
}

fn stable_fixture_uuid(kind: u8, ordinal: usize) -> Uuid {
    let mut bytes = [0u8; 16];
    bytes[0] = kind;
    bytes[8..].copy_from_slice(&(ordinal as u64).to_be_bytes());
    Uuid::from_bytes(bytes)
}

/// Build a graph whose first productive edge ids are localized but whose
/// destination node ids are evenly scattered through the node table. Target
/// UUIDs are stable across scales so the public results are directly
/// comparable even though their physical node ids move.
fn generate_scattered_destinations(
    dir: &Path,
    nodes: usize,
    fan_out: usize,
    targets: usize,
) -> usize {
    assert!(nodes > targets + fan_out);
    let workspace = TempDir::new().unwrap();
    let mut writer = GraphWriter::open_at(workspace.path(), OntologyMode::Exploratory, TS).unwrap();
    let source = stable_fixture_uuid(1, 0);
    let target_uuids: Vec<Uuid> = (0..targets)
        .map(|ordinal| stable_fixture_uuid(2, ordinal + 1))
        .collect();
    let target_positions: Vec<usize> = (0..targets)
        .map(|ordinal| 1 + ordinal * (nodes - 1) / targets)
        .collect();
    let mut uuids = Vec::with_capacity(nodes);
    uuids.push(source);
    let mut target_cursor = 0usize;
    for position in 1..nodes {
        let uuid = if target_cursor < targets && target_positions[target_cursor] == position {
            let uuid = target_uuids[target_cursor];
            target_cursor += 1;
            uuid
        } else {
            new_v7()
        };
        uuids.push(uuid);
    }
    assert_eq!(target_cursor, targets);
    for uuid in &uuids {
        writer.create_node(*uuid, NODE_TYPE).unwrap();
    }
    for target in &target_uuids {
        writer
            .create_edge(new_v7(), "LINK", &source, target)
            .unwrap();
    }
    for src in 1..nodes {
        for offset in 1..=fan_out {
            writer
                .create_edge(
                    new_v7(),
                    "LINK",
                    &uuids[src],
                    &uuids[(src + offset) % nodes],
                )
                .unwrap();
        }
    }
    writer.flush().unwrap();
    build_adjacency_index(workspace.path(), TS).unwrap();
    project_fixture::publish_graph_workspace(dir, workspace.path());
    targets + (nodes - 1) * fan_out
}

fn run_measured(
    forge: &GraphForge,
    query: &str,
) -> (Duration, io_stats::IoSnapshot, DemandSnapshot) {
    io_stats::reset();
    let started = Instant::now();
    let observed = forge.execute_observed(query);
    let result = observed.result.unwrap();
    let elapsed = started.elapsed();
    assert_eq!(result.stats.rows_produced, LIMIT as u64, "{query}");
    (elapsed, io_stats::snapshot(), observed.evidence)
}

#[derive(Debug)]
struct ScaleResult {
    nodes: usize,
    edges: usize,
    one_hop: Duration,
    two_hop: Duration,
    one_hop_io: io_stats::IoSnapshot,
    two_hop_io: io_stats::IoSnapshot,
    one_hop_demand: DemandSnapshot,
    two_hop_demand: DemandSnapshot,
}

fn run_scale(nodes: usize, fan_out: usize) -> ScaleResult {
    let dir = TempDir::new().unwrap();
    generate_graph(dir.path(), nodes, fan_out);
    let forge = open_forge(dir.path());

    let one_plan = forge.explain(ONE_HOP).unwrap();
    assert!(one_plan.contains("ExpandExec"), "{one_plan}");
    assert!(one_plan.contains("adjacency=hit"), "{one_plan}");
    assert!(one_plan.contains("fetch=1000"), "{one_plan}");
    let two_plan = forge.explain(TWO_HOP).unwrap();
    assert_eq!(two_plan.matches("ExpandExec").count(), 2, "{two_plan}");
    assert!(two_plan.contains("fetch=1000"), "{two_plan}");
    assert!(two_plan.contains("demand_batch=1000"), "{two_plan}");
    assert!(two_plan.contains("cancel=guarded"), "{two_plan}");
    assert!(!two_plan.contains("RoundRobinBatch"), "{two_plan}");
    drop(forge);

    // Fresh facades keep the process-global compatibility counters attributable
    // to one query. Query-scoped demand counters additionally prove that all
    // started reads quiesce before result collection returns.
    let one_forge = open_forge(dir.path());
    let (one_hop, one_hop_io, one_hop_demand) = run_measured(&one_forge, ONE_HOP);
    drop(one_forge);
    let two_forge = open_forge(dir.path());
    let (two_hop, two_hop_io, two_hop_demand) = run_measured(&two_forge, TWO_HOP);
    drop(two_forge);

    ScaleResult {
        nodes,
        edges: nodes * fan_out,
        one_hop,
        two_hop,
        one_hop_io,
        two_hop_io,
        one_hop_demand,
        two_hop_demand,
    }
}

fn assert_indexed_limit_io(io: &io_stats::IoSnapshot) {
    assert_eq!(io.edge_full_reads, 0, "{io:?}");
    assert_eq!(io.edge_full_rows, 0, "{io:?}");
    assert!(io.edge_filtered_reads >= 1, "{io:?}");
    assert!(io.edge_filtered_rows > 0, "{io:?}");
    assert_eq!(io.node_full_reads, 0, "{io:?}");
    assert!(io.node_filtered_reads >= 1, "{io:?}");
}

fn assert_bounded_demand(snapshot: &DemandSnapshot, expected_hops: usize, required: u64) {
    assert_eq!(snapshot.hops.len(), expected_hops, "{snapshot:#?}");
    assert!(snapshot.cancellations >= 1, "{snapshot:#?}");
    assert!(snapshot.max_in_flight_reads <= 1, "{snapshot:#?}");
    for hop in snapshot.hops.values() {
        assert_eq!(hop.reads_after_cancel, 0, "{snapshot:#?}");
        assert_eq!(
            hop.edge_reads_started, hop.edge_reads_completed,
            "{snapshot:#?}"
        );
        assert_eq!(
            hop.node_reads_started, hop.node_reads_completed,
            "{snapshot:#?}"
        );
        assert_eq!(hop.edge_reads_failed, 0, "{snapshot:#?}");
        assert_eq!(hop.node_reads_failed, 0, "{snapshot:#?}");
        assert_eq!(hop.edge_full_reads, 0, "{snapshot:#?}");
        assert_eq!(hop.node_full_reads, 0, "{snapshot:#?}");
        assert!(
            hop.candidates_generated <= required + MAX_BATCH_ROWS,
            "required={required}, snapshot={snapshot:#?}"
        );
    }
}

#[test]
fn terminal_limit_keeps_fixed_hop_io_bounded_as_graph_grows() {
    let _guard = IO_GUARD.lock().unwrap();
    let small = run_scale(32_768, FAN_OUT);
    let large = run_scale(327_680, FAN_OUT);
    println!("fixed-hop LIMIT structural smoke: small={small:?}, large={large:?}");

    for scale in [&small, &large] {
        assert_indexed_limit_io(&scale.one_hop_io);
        assert_indexed_limit_io(&scale.two_hop_io);
        assert_bounded_demand(&scale.one_hop_demand, 1, LIMIT as u64);
        assert_bounded_demand(&scale.two_hop_demand, 2, LIMIT as u64);
    }

    // A 10x graph may move Parquet page boundaries, but must not create a
    // graph-proportional increase in rows materialized for the same LIMIT.
    assert!(
        large.one_hop_io.edge_filtered_rows <= small.one_hop_io.edge_filtered_rows * 3,
        "small={small:?}, large={large:?}"
    );
    assert!(
        large.two_hop_io.edge_filtered_rows <= small.two_hop_io.edge_filtered_rows * 3,
        "small={small:?}, large={large:?}"
    );
    assert!(
        large.one_hop_io.node_filtered_rows <= small.one_hop_io.node_filtered_rows * 3,
        "small={small:?}, large={large:?}"
    );
    assert!(
        large.two_hop_io.node_filtered_rows <= small.two_hop_io.node_filtered_rows * 3,
        "small={small:?}, large={large:?}"
    );
}

fn run_scattered_destination_scale(
    nodes: usize,
) -> (Vec<Vec<u8>>, io_stats::IoSnapshot, DemandSnapshot, usize) {
    let dir = TempDir::new().unwrap();
    let edges = generate_scattered_destinations(dir.path(), nodes, 4, 1_500);
    let forge = open_forge(dir.path());
    io_stats::reset();
    let observed = forge.execute_observed(ONE_HOP);
    let result = observed.result.unwrap();
    assert_eq!(result.stats.rows_produced, LIMIT as u64);
    let mut values = fixed_binary_values(&result, "id");
    values.sort_unstable();
    (values, io_stats::snapshot(), observed.evidence, edges)
}

#[test]
fn scattered_node_hydration_is_neighborhood_proportional() {
    let _guard = IO_GUARD.lock().unwrap();
    let (small_values, small_io, small_demand, small_edges) =
        run_scattered_destination_scale(16_384);
    let (large_values, large_io, large_demand, large_edges) =
        run_scattered_destination_scale(163_840);

    assert_eq!(small_values, large_values);
    assert!(
        large_edges >= small_edges * 9,
        "{small_edges} vs {large_edges}"
    );
    for (io, demand) in [(&small_io, &small_demand), (&large_io, &large_demand)] {
        assert_indexed_limit_io(io);
        assert_eq!(io.node_filtered_reads, 1, "{io:#?}");
        assert_eq!(io.node_dense_row_selection_reads, 1, "{io:#?}");
        assert_eq!(io.node_row_group_predicate_reads, 0, "{io:#?}");
        assert_eq!(io.node_metadata_fallbacks, 0, "{io:#?}");
        assert_eq!(io.node_validation_fallbacks, 0, "{io:#?}");
        assert_eq!(io.node_scanned_rows, io.node_exact_rows_selected, "{io:#?}");
        assert_bounded_demand(demand, 1, LIMIT as u64);
        let hop = demand.hops.values().next().unwrap();
        assert_eq!(hop.node_dense_row_selection_reads, 1, "{demand:#?}");
        assert_eq!(hop.node_row_group_predicate_reads, 0, "{demand:#?}");
        assert_eq!(hop.reads_after_cancel, 0, "{demand:#?}");
    }
    assert!(
        large_io.node_scanned_rows <= 3 * small_io.node_scanned_rows.max(1),
        "scattered node work must stay bounded across 10x graph growth: \
         small={small_io:#?}, large={large_io:#?}"
    );
}

#[test]
fn limits_sweep_bounded_multi_hop_work_and_repartition() {
    let _guard = IO_GUARD.lock().unwrap();
    let dir = TempDir::new().unwrap();
    generate_graph(dir.path(), 4_096, FAN_OUT);
    let forge = open_forge(dir.path());

    for limit in [10_u64, 100, 1_000] {
        let query = format!("MATCH (a)-[r1]->(b)-[r2]->(c) RETURN c.node_uuid AS id LIMIT {limit}");
        let plan = forge.explain(&query).unwrap();
        assert_eq!(plan.matches("ExpandExec").count(), 2, "{plan}");
        assert!(
            plan.matches(&format!("demand_batch={limit}")).count() >= 2,
            "{plan}"
        );
        assert!(!plan.contains("RoundRobinBatch"), "{plan}");

        io_stats::reset();
        let observed = forge.execute_observed(&query);
        let result = observed.result.unwrap();
        assert_eq!(result.stats.rows_produced, limit);
        let io = io_stats::snapshot();
        assert_indexed_limit_io(&io);
        assert_bounded_demand(&observed.evidence, 2, limit);
    }
}

#[test]
fn selective_filter_tops_up_without_crossing_blockers() {
    let _guard = IO_GUARD.lock().unwrap();
    let dir = TempDir::new().unwrap();
    generate_graph(dir.path(), 64, 4);
    let forge = open_forge(dir.path());

    let selective = "MATCH (a)-[r1]->(b)-[r2]->(c) \
                     WHERE c.node_id = 64 RETURN c.node_id AS id LIMIT 10";
    let observed = forge.execute_observed(selective);
    let result = observed.result.unwrap();
    assert_eq!(result.stats.rows_produced, 10);
    let snapshot = observed.evidence;
    assert_bounded_demand(&snapshot, 2, 10);
    assert!(
        snapshot
            .filters
            .values()
            .any(|filter| filter.input_rows > filter.output_rows),
        "selective filter did not reject any candidates: {snapshot:#?}"
    );
    assert!(
        snapshot
            .hops
            .values()
            .any(|hop| hop.candidates_generated > 10),
        "selective query did not top up: {snapshot:#?}"
    );

    for query in [
        "MATCH ()-[r]->(b) RETURN b.node_id AS id ORDER BY id DESC LIMIT 5",
        "MATCH ()-[r]->(b) RETURN DISTINCT b.node_id AS id LIMIT 5",
        "MATCH ()-[r]->() RETURN count(r) AS total LIMIT 1",
    ] {
        let plan = forge.explain(query).unwrap();
        assert!(plan.contains("demand_batch=all, cancel=none"), "{plan}");
        assert!(!plan.contains("DemandGuardExec"), "{plan}");
    }

    let unlimited = forge
        .explain("MATCH ()-[r1]->()-[r2]->() RETURN r1, r2")
        .unwrap();
    assert!(!unlimited.contains("DemandGuardExec"), "{unlimited}");
    assert!(
        unlimited.contains("demand_batch=all, cancel=none"),
        "{unlimited}"
    );
}

#[test]
fn high_degree_source_resumes_without_losing_neighbors() {
    let _guard = IO_GUARD.lock().unwrap();
    let dir = TempDir::new().unwrap();
    let workspace = TempDir::new().unwrap();
    let mut writer = GraphWriter::open_at(workspace.path(), OntologyMode::Exploratory, TS).unwrap();
    let src = new_v7();
    let dst = new_v7();
    writer.create_node(src, NODE_TYPE).unwrap();
    writer.create_node(dst, NODE_TYPE).unwrap();
    for _ in 0..10_000 {
        writer.create_edge(new_v7(), "LINK", &src, &dst).unwrap();
    }
    writer.flush().unwrap();
    build_adjacency_index(workspace.path(), TS).unwrap();
    project_fixture::publish_graph_workspace(dir.path(), workspace.path());

    let result = open_forge(dir.path())
        .execute("MATCH ()-[r]->() RETURN r.edge_uuid AS id")
        .unwrap();
    assert_eq!(result.stats.rows_produced, 10_000);
    assert!(
        result.batches.iter().all(|batch| batch.num_rows() <= 8_192),
        "ExpandExec must honor the DataFusion output batch size"
    );
}

#[test]
fn fixed_hop_limit_preserves_skip_parameters_filters_and_blockers() {
    let _guard = IO_GUARD.lock().unwrap();
    let dir = TempDir::new().unwrap();
    generate_graph(dir.path(), 64, 4);
    let forge = open_forge(dir.path());

    io_stats::reset();
    assert_eq!(
        forge
            .execute("MATCH ()-[r]->() RETURN r LIMIT 0")
            .unwrap()
            .stats
            .rows_produced,
        0
    );
    let zero = io_stats::snapshot();
    assert_eq!(
        zero.edge_full_reads + zero.edge_filtered_reads,
        0,
        "{zero:?}"
    );

    assert_eq!(
        forge
            .execute("MATCH ()-[r]->() RETURN r SKIP 5 LIMIT 7")
            .unwrap()
            .stats
            .rows_produced,
        7
    );
    let params = HashMap::from([("n".to_owned(), IrLiteral::Int(9))]);
    assert_eq!(
        forge
            .execute_with_params("MATCH ()-[r]->() RETURN r LIMIT $n", &params)
            .unwrap()
            .stats
            .rows_produced,
        9
    );
    let filtered = forge
        .execute(
            "MATCH ()-[r]->(b) WHERE b.node_id >= 61 \
             RETURN b.node_id AS id LIMIT 11",
        )
        .unwrap();
    let filtered_ids = uint64_values(&filtered, "id");
    assert_eq!(filtered_ids.len(), 11);
    assert!(
        filtered_ids.iter().all(|id| (61..=64).contains(id)),
        "post-filter LIMIT returned unexpected IDs: {filtered_ids:?}"
    );

    let ordered_plan = forge
        .explain("MATCH ()-[r]->(b) RETURN b.node_id AS id ORDER BY id DESC LIMIT 5")
        .unwrap();
    assert!(
        ordered_plan.contains("SortExec: TopK(fetch=5)"),
        "ordered LIMIT must physically select bounded TopK: {ordered_plan}"
    );
    let observed = forge.execute_observed(
        "MATCH ()-[r]->(b) RETURN b.node_id AS id \
             ORDER BY id DESC LIMIT 5",
    );
    let ordered = observed.result.unwrap();
    assert_eq!(uint64_values(&ordered, "id"), [64, 64, 64, 64, 63]);
    let ordered_metrics = observed.evidence;
    assert_eq!(ordered_metrics.sorts.len(), 1, "{ordered_metrics:#?}");
    let sort = &ordered_metrics.sorts[0];
    assert_eq!(sort.fetch, Some(5), "{ordered_metrics:#?}");
    assert_eq!(sort.output_rows, 5, "{ordered_metrics:#?}");
    assert_eq!(
        sort.spill_count, 0,
        "TopK does not spill: {ordered_metrics:#?}"
    );
    assert_eq!(sort.spilled_bytes, 0, "{ordered_metrics:#?}");
    assert_eq!(sort.memory_used_after, 0, "{ordered_metrics:#?}");
    assert_eq!(
        ordered_metrics.memory_reserved_after, ordered_metrics.memory_reserved_before,
        "query memory reservations must quiesce: {ordered_metrics:#?}"
    );

    let distinct = forge
        .execute(
            "MATCH ()-[r]->(b) RETURN DISTINCT b.node_id AS id \
             ORDER BY id ASC LIMIT 5",
        )
        .unwrap();
    assert_eq!(uint64_values(&distinct, "id"), [1, 2, 3, 4, 5]);

    let aggregate = forge
        .execute("MATCH ()-[r]->() RETURN count(r) AS total LIMIT 1")
        .unwrap();
    let total = aggregate.batches[0]
        .column_by_name("total")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(total, 256, "aggregation must consume the complete hop");
}

#[test]
fn ordered_limit_topk_state_is_cardinality_independent_and_released() {
    let _guard = IO_GUARD.lock().unwrap();
    let mut snapshots = Vec::new();
    for nodes in [4_096, 40_960] {
        let dir = TempDir::new().unwrap();
        generate_graph(dir.path(), nodes, FAN_OUT);
        let forge = open_forge(dir.path());
        let observed = forge.execute_observed(
            "MATCH ()-[r]->(b) RETURN b.node_id AS id ORDER BY id DESC LIMIT 100",
        );
        let result = observed.result.unwrap();
        assert_eq!(result.stats.rows_produced, 100);
        snapshots.push(observed.evidence);
    }

    for snapshot in &snapshots {
        assert_eq!(snapshot.sorts.len(), 1, "{snapshot:#?}");
        let sort = &snapshot.sorts[0];
        assert_eq!(sort.fetch, Some(100), "{snapshot:#?}");
        assert_eq!(sort.output_rows, 100, "{snapshot:#?}");
        assert_eq!(sort.spill_count, 0, "{snapshot:#?}");
        assert_eq!(sort.spilled_bytes, 0, "{snapshot:#?}");
        assert_eq!(sort.memory_used_after, 0, "{snapshot:#?}");
        assert_eq!(
            snapshot.memory_reserved_after, snapshot.memory_reserved_before,
            "{snapshot:#?}"
        );
    }
}

fn env_usize(key: &str, default: usize) -> usize {
    match std::env::var(key) {
        Ok(value) => value
            .parse()
            .unwrap_or_else(|e| panic!("invalid {key}={value:?}: {e}")),
        Err(std::env::VarError::NotPresent) => default,
        Err(std::env::VarError::NotUnicode(_)) => panic!("{key} is not valid Unicode"),
    }
}

fn median(mut samples: Vec<Duration>) -> Duration {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

#[derive(Debug)]
struct LiveJournalSample {
    elapsed: Duration,
    io: io_stats::IoSnapshot,
    demand: DemandSnapshot,
}

fn livejournal_query(hops: usize, limit: usize) -> String {
    match hops {
        1 => format!("MATCH ()-[]->() RETURN 1 AS x LIMIT {limit}"),
        2 => format!("MATCH ()-[]->()-[]->() RETURN 1 AS x LIMIT {limit}"),
        _ => unreachable!("benchmark covers one and two hops"),
    }
}

fn physical_plan_only(explain: &str) -> &str {
    explain
        .split_once("PhysicalPlan\n------------\n")
        .map_or(explain, |(_, physical)| physical)
}

fn livejournal_sample(forge: &GraphForge, query: &str, limit: usize) -> LiveJournalSample {
    io_stats::reset();
    let started = Instant::now();
    let observed = forge.execute_observed(query);
    let result = observed
        .result
        .unwrap_or_else(|error| panic!("LiveJournal traversal execution failed: {error}"));
    let elapsed = started.elapsed();
    assert_eq!(result.stats.rows_produced, limit as u64);
    LiveJournalSample {
        elapsed,
        io: io_stats::snapshot(),
        demand: observed.evidence,
    }
}

#[test]
#[ignore = "release-only cached LiveJournal benchmark"]
fn release_livejournal_fixed_hop_limits() {
    assert!(
        !cfg!(debug_assertions),
        "run with cargo test --release; debug timings are not useful"
    );
    let _guard = IO_GUARD.lock().unwrap();
    let project = std::env::var_os("GF_LIVEJOURNAL_PROJECT")
        .expect("GF_LIVEJOURNAL_PROJECT must name the cached canonical fixture");
    let project = Path::new(&project);
    let project_str = project
        .to_str()
        .expect("GF_LIVEJOURNAL_PROJECT must be valid UTF-8");
    let forge = GraphForge::new(Some(project_str))
        .unwrap_or_else(|error| panic!("failed to open cached LiveJournal fixture: {error}"));

    for hops in [1, 2] {
        for limit in [10, 100, 1_000] {
            let query = livejournal_query(hops, limit);
            let explain = forge
                .explain(&query)
                .unwrap_or_else(|error| panic!("failed to plan LiveJournal traversal: {error}"));
            let plan = physical_plan_only(&explain);
            assert_eq!(plan.matches("ExpandExec").count(), hops, "{plan}");
            assert!(!plan.contains("RoundRobinBatch"), "{plan}");
            let cold = livejournal_sample(&forge, &query, limit);
            // One untimed warm-up precedes five independently captured samples.
            let warm = forge
                .execute(&query)
                .unwrap_or_else(|error| panic!("LiveJournal traversal warm-up failed: {error}"));
            assert_eq!(warm.stats.rows_produced, limit as u64);
            let mut samples: Vec<LiveJournalSample> = (0..5)
                .map(|_| livejournal_sample(&forge, &query, limit))
                .collect();
            samples.sort_by_key(|sample| sample.elapsed);
            let median = &samples[2];
            for sample in &samples {
                assert_indexed_limit_io(&sample.io);
                assert_bounded_demand(&sample.demand, hops, limit as u64);
                if hops == 1 && limit == 1_000 {
                    assert_eq!(sample.io.node_filtered_reads, 1, "{sample:#?}");
                    assert_eq!(sample.io.node_dense_row_selection_reads, 1, "{sample:#?}");
                    assert!(sample.io.node_scanned_rows <= 1_048_576, "{sample:#?}");
                }
                if hops == 2 && limit == 1_000 {
                    assert!(sample.io.edge_scanned_rows <= 2_949_120, "{sample:#?}");
                    assert!(sample.io.node_scanned_rows <= 11_612_672, "{sample:#?}");
                }
            }
            println!(
                "LIVEJOURNAL hops={hops} limit={limit} cold_ms={:.3} count=5 median_ms={:.3} range_ms={:.3}..{:.3} io={:?} demand={:?}\nPLAN_BEGIN\n{plan}\nPLAN_END",
                cold.elapsed.as_secs_f64() * 1_000.0,
                median.elapsed.as_secs_f64() * 1_000.0,
                samples[0].elapsed.as_secs_f64() * 1_000.0,
                samples[4].elapsed.as_secs_f64() * 1_000.0,
                median.io,
                median.demand,
            );
        }
    }
}

fn release_scale(nodes: usize, fan_out: usize) -> ScaleResult {
    let dir = TempDir::new().unwrap();
    generate_graph(dir.path(), nodes, fan_out);
    let warm = open_forge(dir.path());
    warm.execute(ONE_HOP).unwrap();
    warm.execute(TWO_HOP).unwrap();
    drop(warm);

    let mut one_samples = Vec::with_capacity(5);
    let mut two_samples = Vec::with_capacity(5);
    let mut one_hop_io = io_stats::IoSnapshot::default();
    let mut two_hop_io = io_stats::IoSnapshot::default();
    for _ in 0..5 {
        let forge = open_forge(dir.path());
        let (elapsed, io, _) = run_measured(&forge, ONE_HOP);
        one_samples.push(elapsed);
        one_hop_io = io;
        drop(forge);
    }
    for _ in 0..5 {
        let forge = open_forge(dir.path());
        let (elapsed, io, _) = run_measured(&forge, TWO_HOP);
        two_samples.push(elapsed);
        two_hop_io = io;
        drop(forge);
    }
    ScaleResult {
        nodes,
        edges: nodes * fan_out,
        one_hop: median(one_samples),
        two_hop: median(two_samples),
        one_hop_io,
        two_hop_io,
        one_hop_demand: DemandSnapshot::default(),
        two_hop_demand: DemandSnapshot::default(),
    }
}

#[test]
#[ignore = "release-only 1M/10M-edge benchmark"]
fn release_fixed_hop_limit_1m_10m() {
    assert!(
        !cfg!(debug_assertions),
        "run with cargo test --release; debug timings are not useful"
    );
    let _guard = IO_GUARD.lock().unwrap();
    let fan_out = env_usize("GF_FIXED_HOP_BENCH_FANOUT", 16);
    let small = release_scale(env_usize("GF_FIXED_HOP_BENCH_N1", 62_500), fan_out);
    let large = release_scale(env_usize("GF_FIXED_HOP_BENCH_N2", 625_000), fan_out);

    println!(
        "| scale | nodes | edges | one-hop ms | two-hop ms | one-hop edge rows | two-hop edge rows |"
    );
    println!("|---|---:|---:|---:|---:|---:|---:|");
    for (label, result) in [("S", small), ("M", large)] {
        println!(
            "| {label} | {} | {} | {:.2} | {:.2} | {} | {} |",
            result.nodes,
            result.edges,
            result.one_hop.as_secs_f64() * 1_000.0,
            result.two_hop.as_secs_f64() * 1_000.0,
            result.one_hop_io.edge_filtered_rows,
            result.two_hop_io.edge_filtered_rows,
        );
    }
}
