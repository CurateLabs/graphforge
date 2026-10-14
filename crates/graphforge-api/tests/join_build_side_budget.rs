//! #1985: a join whose build side outgrows the query memory budget completes
//! with the result it has under a large budget, and the query's peak
//! reservation stays below the budget.
//!
//! DataFusion's hash join keeps its whole build side in a reservation that
//! cannot spill, so a join that builds more than the pool holds fails with
//! `Resources exhausted ... HashJoinInput`. The query below is a pattern
//! comprehension over a variable-length hop: the hop publishes no row
//! estimate, so nothing at plan time says how large the join's build side is.
//! The join must therefore be one that can spill. The pool's peak reservation
//! comes from [`ExecutionResult::stats`].

use arrow::array::Int64Array;
use graphforge_api::{
    ExecutionResourcePolicy, ExecutionResult, GraphForge, GraphForgeOptions, ResourcePolicyMode,
};
use tempfile::TempDir;

#[path = "support/bulk_fixture.rs"]
mod bulk_fixture;

use bulk_fixture::generate_bulk_graph_with_properties;

const NODES: usize = 60_000;
const FAN_OUT: usize = 2;
const MIB: u64 = 1024 * 1024;
/// Far below what the query's join builds, and above what a sort-merge join
/// of the same inputs needs to merge its spills.
const SMALL_POOL: u64 = 128 * MIB;
const LARGE_POOL: u64 = 4096 * MIB;

/// Every node with the variable-length neighbours of its neighbours: rows of
/// four long strings, so the main stream is the join's build side, left joined
/// to the neighbours the comprehension collects.
const QUERY: &str = "MATCH (a) OPTIONAL MATCH (a)-[:LINK*1..2]->(b) \
     WITH a.name + '|' + b.name AS x1, a.name + '/' + b.name AS x2, \
          a.name + ':' + b.name AS x3, a.name + ',' + b.name AS x4, \
          [(b)-[:LINK*1..2]->(c) | c.name] AS cs \
     RETURN count(*) AS rows, \
            sum(size(x1) + size(x2) + size(x3) + size(x4)) AS chars, \
            sum(size(cs)) AS total";

fn open(dir: &TempDir, memory_budget_bytes: u64) -> GraphForge {
    GraphForge::new_with_options(
        Some(dir.path().to_str().expect("temp path is UTF-8")),
        GraphForgeOptions {
            resource: ExecutionResourcePolicy {
                mode: ResourcePolicyMode::Explicit,
                tokio_worker_threads: Some(2),
                // One partition makes the join a single `CollectLeft` build.
                target_partitions: Some(1),
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

fn only_int64(result: &ExecutionResult, column: &str) -> i64 {
    let values = result
        .batches
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name(column)
                .unwrap_or_else(|| panic!("missing {column} column"))
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap_or_else(|| panic!("{column} must be Int64"))
                .values()
                .iter()
                .copied()
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(values.len(), 1, "{column}: {values:?}");
    values[0]
}

fn answer(result: &ExecutionResult) -> [i64; 3] {
    ["rows", "chars", "total"].map(|column| only_int64(result, column))
}

#[test]
fn a_join_whose_build_side_outgrows_the_budget_returns_its_large_budget_result_within_the_budget() {
    let dir = TempDir::new().unwrap();
    generate_bulk_graph_with_properties(dir.path(), NODES, FAN_OUT, false);
    GraphForge::new(Some(dir.path().to_str().unwrap()))
        .unwrap()
        .index_adjacency()
        .unwrap();

    let large = open(&dir, LARGE_POOL).execute(QUERY).unwrap();
    let expected = answer(&large);
    assert!(
        expected[0] > i64::try_from(NODES).unwrap(),
        "the query joins more rows than there are nodes: {expected:?}"
    );
    assert!(
        large.stats.peak_memory_bytes > SMALL_POOL,
        "the query holds {} bytes under a pool that does not constrain it, so the {SMALL_POOL} \
         byte pool is one its join cannot fit as a hash join",
        large.stats.peak_memory_bytes
    );

    let small = open(&dir, SMALL_POOL).execute(QUERY).expect(
        "a join whose build side outgrows the budget must spill or re-side, not exhaust the pool",
    );
    assert_eq!(answer(&small), expected);
    assert!(
        small.stats.peak_memory_bytes < SMALL_POOL,
        "the pool's peak reservation {} must stay below its budget {SMALL_POOL}",
        small.stats.peak_memory_bytes
    );
}
