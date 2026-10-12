//! #1985: the query memory budget an instance runs under is derived from the
//! process memory limit unless the caller states one, and is reported.

use graphforge_api::{
    ExecutionResourcePolicy, GraphForge, GraphForgeOptions, MemoryBudgetSource, ResourcePolicyMode,
    SpillPolicy,
};

const STATED: u64 = 48 * 1024 * 1024;
const FLOOR: u64 = 512 * 1024 * 1024;
const CAP: u64 = 32 * 1024 * 1024 * 1024;

#[test]
fn the_default_budget_is_derived_from_the_process_limit_and_reported() {
    let graph = GraphForge::new(None).unwrap();
    let diagnostics = graph.resource_diagnostics();
    assert!(diagnostics.memory_budget_bytes >= 512 * 1024 * 1024);
    // The documented rule: half the process's hard memory limit, within a
    // 512 MiB floor and a 32 GiB cap; the floor when no limit is readable.
    let expected = match diagnostics.memory_budget_source {
        MemoryBudgetSource::DerivedFromHostLimit { host_limit_bytes } => {
            (host_limit_bytes / 2).clamp(FLOOR, CAP)
        }
        MemoryBudgetSource::DerivedWithoutHostLimit => FLOOR,
        other => panic!("the default budget must be derived, not {other:?}"),
    };
    assert_eq!(diagnostics.memory_budget_bytes, expected);
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
