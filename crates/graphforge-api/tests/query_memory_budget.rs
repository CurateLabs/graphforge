//! #1985: the query memory budget an instance runs under is derived from the
//! process memory limit unless the caller states one, and is reported.

use graphforge_api::{
    ExecutionResourcePolicy, GraphForge, GraphForgeOptions, MemoryBudgetSource, ResourcePolicyMode,
    SpillPolicy, derive_query_memory_budget,
};

const STATED: u64 = 48 * 1024 * 1024;

#[test]
fn the_default_budget_is_derived_from_the_process_limit_and_reported() {
    let graph = GraphForge::new(None).unwrap();
    let diagnostics = graph.resource_diagnostics();
    assert!(diagnostics.memory_budget_bytes >= 512 * 1024 * 1024);
    let host_limit = match diagnostics.memory_budget_source {
        MemoryBudgetSource::DerivedFromHostLimit { host_limit_bytes } => Some(host_limit_bytes),
        MemoryBudgetSource::DerivedWithoutHostLimit => None,
        other => panic!("the default budget must be derived, not {other:?}"),
    };
    assert_eq!(
        (
            diagnostics.memory_budget_bytes,
            diagnostics.memory_budget_source
        ),
        derive_query_memory_budget(host_limit)
    );
}

#[test]
fn a_stated_budget_is_used_as_given_and_reported_as_stated() {
    let graph = GraphForge::new_with_options(
        None,
        GraphForgeOptions {
            resource: ExecutionResourcePolicy {
                mode: ResourcePolicyMode::Automatic,
                memory_budget_bytes: Some(STATED),
                spill: SpillPolicy::default(),
                ..ExecutionResourcePolicy::default()
            },
            ..GraphForgeOptions::default()
        },
    )
    .unwrap();
    let diagnostics = graph.resource_diagnostics();
    assert_eq!(diagnostics.memory_budget_bytes, STATED);
    assert_eq!(
        diagnostics.memory_budget_source,
        MemoryBudgetSource::Explicit
    );
}
