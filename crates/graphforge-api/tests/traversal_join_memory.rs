//! #1985: a join above a traversal is planned for the bytes the traversal is
//! estimated to emit. `ExpandExec` publishes its input rows times the degree of
//! the relation times the width of an output row, so a join whose build side is
//! a traversal's output that cannot fit the query memory pool becomes a
//! spilling sort-merge join, and one that fits stays a hash join. Without the
//! estimate the join has no size to compare with the pool and stays a hash join
//! whatever the traversal emits.

use arrow::array::Int64Array;
use graphforge_api::{
    ExecutionResourcePolicy, ExecutionResult, GraphForge, GraphForgeOptions, ResourcePolicyMode,
};
use tempfile::TempDir;

#[path = "support/bulk_fixture.rs"]
mod bulk_fixture;

use bulk_fixture::generate_bulk_graph_with_properties;

const NODES: usize = 450_000;
const FAN_OUT: usize = 2;
const SEEDS: usize = 200_000;
const MIB: u64 = 1024 * 1024;
/// The smallest pool the facade admits: half of it bounds a join's build side.
const SMALL_POOL: u64 = 16 * MIB;
const LARGE_POOL: u64 = 2048 * MIB;

/// A traversal from [`SEEDS`] nodes whose destinations are enriched with their
/// `name` property: a join above the traversal. The traversal emits
/// [`SEEDS`] x [`FAN_OUT`] rows, fewer than the [`NODES`] rows it is joined
/// with, so the join builds on the traversal's output. The query has a second
/// join, below the traversal, that enriches the seeds; the test reads the join
/// directly above the traversal, which is the one the traversal's estimate
/// decides.
fn enriched_destinations() -> String {
    format!(
        "MATCH (a) WITH a LIMIT {SEEDS} MATCH (a)-[:LINK]->(b) \
         RETURN count(*) AS rows, count(b.name) AS names"
    )
}

fn open(dir: &TempDir, memory_budget_bytes: u64) -> GraphForge {
    GraphForge::new_with_options(
        Some(dir.path().to_str().expect("temp path is UTF-8")),
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

#[test]
fn a_join_above_a_traversal_that_exceeds_the_pool_is_planned_as_a_sort_merge_join() {
    let dir = TempDir::new().unwrap();
    generate_bulk_graph_with_properties(dir.path(), NODES, FAN_OUT, false);
    GraphForge::new(Some(dir.path().to_str().unwrap()))
        .unwrap()
        .index_adjacency()
        .unwrap();
    let query = enriched_destinations();

    // The smallest pool the facade admits cannot run the sorts of any
    // sort-merge join (each reserves 10 MiB to merge its spills), so the plan
    // is checked there and the answer under the pool that holds the hash join;
    // `session::query_memory_tests` runs a sort-merge join to completion.
    let small = open(&dir, SMALL_POOL);
    let small_plan = small.explain(&query).unwrap();
    assert!(
        small_plan.contains("adjacency=hit"),
        "the traversal reads the persisted index:\n{small_plan}"
    );
    assert!(
        join_above_traversal(&small_plan).is_some_and(|join| join.starts_with("SortMergeJoinExec")),
        "a build side of {} traversal rows cannot fit {SMALL_POOL} bytes of pool:\n{small_plan}",
        SEEDS * FAN_OUT
    );

    let large = open(&dir, LARGE_POOL);
    let large_plan = large.explain(&query).unwrap();
    assert!(
        join_above_traversal(&large_plan).is_some_and(|join| join.starts_with("HashJoinExec")),
        "a build side that fits keeps the hash join:\n{large_plan}"
    );
    let result = large.execute(&query).unwrap();
    let edges = i64::try_from(SEEDS * FAN_OUT).unwrap();
    assert_eq!(only_int64(&result, "rows"), edges);
    assert_eq!(only_int64(&result, "names"), edges);
}
