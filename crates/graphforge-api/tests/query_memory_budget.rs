//! #1985: an in-memory instance spills an aggregate that exceeds its query
//! memory budget instead of failing, and the budget it ran under is reported.

use arrow::array::Int64Array;
use graphforge_api::{
    ExecutionResourcePolicy, GraphForge, GraphForgeOptions, MemoryBudgetSource, ResourcePolicyMode,
    SpillPolicy,
};

const GROUPS: i64 = 1_500_000;
const BUDGET: u64 = 24 * 1024 * 1024;

const QUERY: &str = "UNWIND range(1, 1500000) AS x \
                     WITH toString(x) AS k, count(*) AS c \
                     RETURN count(*) AS groups, sum(c) AS total";

fn instance(budget: u64, spill: SpillPolicy) -> GraphForge {
    GraphForge::new_with_options(
        None,
        GraphForgeOptions {
            resource: ExecutionResourcePolicy {
                mode: ResourcePolicyMode::Explicit,
                tokio_worker_threads: Some(2),
                target_partitions: Some(2),
                memory_budget_bytes: Some(budget),
                spill,
                ..ExecutionResourcePolicy::default()
            },
            ..GraphForgeOptions::default()
        },
    )
    .unwrap()
}

fn counts(graph: &GraphForge) -> Result<(i64, i64), graphforge_api::GfError> {
    let result = graph.execute(QUERY)?;
    let batch = &result.batches[0];
    let column = |index: usize| {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("integer column")
            .value(0)
    };
    Ok((column(0), column(1)))
}

#[test]
fn an_in_memory_aggregate_over_the_budget_spills_and_matches_the_large_budget() {
    let small = instance(BUDGET, SpillPolicy::default());
    assert_eq!(counts(&small).unwrap(), (GROUPS, GROUPS));
    let large = instance(4 * 1024 * 1024 * 1024, SpillPolicy::default());
    assert_eq!(counts(&large).unwrap(), (GROUPS, GROUPS));
}

#[test]
fn the_budget_binds_when_spill_is_disabled() {
    let refused = instance(
        BUDGET,
        SpillPolicy {
            enabled: false,
            ..SpillPolicy::default()
        },
    );
    let error = counts(&refused).unwrap_err().to_string();
    assert!(error.contains("Resources exhausted"), "{error}");
}

#[test]
fn the_default_budget_is_derived_from_the_process_limit_and_reported() {
    let graph = GraphForge::new(None).unwrap();
    let diagnostics = graph.resource_diagnostics();
    assert!(diagnostics.memory_budget_bytes >= 512 * 1024 * 1024);
    let (expected, source) =
        graphforge_api::derive_query_memory_budget(match diagnostics.memory_budget_source {
            MemoryBudgetSource::DerivedFromHostLimit { host_limit_bytes } => Some(host_limit_bytes),
            _ => None,
        });
    assert_eq!(diagnostics.memory_budget_bytes, expected);
    assert_eq!(diagnostics.memory_budget_source, source);
    assert_ne!(
        diagnostics.memory_budget_source,
        MemoryBudgetSource::Explicit
    );

    let stated = instance(BUDGET, SpillPolicy::default());
    let diagnostics = stated.resource_diagnostics();
    assert_eq!(diagnostics.memory_budget_bytes, BUDGET);
    assert_eq!(
        diagnostics.memory_budget_source,
        MemoryBudgetSource::Explicit
    );
}
