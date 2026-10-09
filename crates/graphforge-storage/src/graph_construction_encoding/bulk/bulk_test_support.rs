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

pub(crate) fn max_task_decode_bytes(plan: &super::plan::BulkBuildPlan<'_>) -> u64 {
    plan.nodes
        .iter()
        .chain(&plan.edges)
        .map(|source| source.task_decode_bytes(0))
        .max()
        .unwrap_or(0)
}
