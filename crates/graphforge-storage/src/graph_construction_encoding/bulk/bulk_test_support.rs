//! Test-only access to production sizing and pre-existing routing controls.

pub(crate) use super::budget::test_support::{
    ForcedPartitions, derived_concurrency, scratch_minimum_bytes,
};
pub(crate) use super::property_rows::test_support::ForcedPropertyFrames;

pub(crate) fn property_fan_in(
    plan: &super::plan::BulkBuildPlan<'_>,
    budget: u64,
    workers: usize,
    budgets: super::super::GraphConstructionBudgets,
) -> usize {
    super::budget::ScratchPlan::derive_with_budgets(plan, budget, workers, budgets)
        .property
        .fan_in
}

pub(crate) fn decode_pool(
    plan: &super::plan::BulkBuildPlan<'_>,
    budget: u64,
    workers: usize,
    budgets: super::super::GraphConstructionBudgets,
) -> u64 {
    super::budget::ScratchPlan::derive_with_budgets(plan, budget, workers, budgets).decode_bytes
}

pub(crate) fn task_decode_bytes_bounds(plan: &super::plan::BulkBuildPlan<'_>) -> (u64, u64) {
    let mut requests = plan
        .nodes
        .iter()
        .chain(&plan.edges)
        .map(|source| source.task_decode_bytes(0));
    let first = requests.next().unwrap_or(0);
    requests.fold((first, first), |(min, max), request| {
        (min.min(request), max.max(request))
    })
}
