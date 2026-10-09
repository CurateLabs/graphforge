//! Small-fixture controls and assertions for the scratch budget tests.

use super::*;

thread_local! {
    /// Forces the partition counts of a test build (edge, CSR).
    pub(super) static FORCED_PARTITIONS: std::cell::Cell<Option<(usize, usize)>> =
        const { std::cell::Cell::new(None) };
    pub(super) static FORCED_GATE: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
}

/// Forces the partition counts of the builds the current test thread runs,
/// until dropped, so a small graph spans several partitions.
pub(crate) struct ForcedPartitions;

impl ForcedPartitions {
    pub(crate) fn with_gate(bytes: u64) -> Self {
        FORCED_GATE.with(|forced| forced.set(Some(bytes)));
        Self
    }

    pub(crate) fn set(edge: usize, csr: usize) -> Self {
        FORCED_PARTITIONS.with(|forced| forced.set(Some((edge, csr))));
        Self
    }
}

impl Drop for ForcedPartitions {
    fn drop(&mut self) {
        FORCED_PARTITIONS.with(|forced| forced.set(None));
        FORCED_GATE.with(|forced| forced.set(None));
    }
}

/// The fewest resident bytes a scratch build of `plan` can run in.
pub(crate) fn scratch_minimum_bytes(
    plan: &BulkBuildPlan<'_>,
    budgets: super::GraphConstructionBudgets,
) -> u64 {
    let properties = plan
        .nodes
        .iter()
        .chain(&plan.edges)
        .any(|source| !source.property_free);
    plan.node_tables_resident_bytes()
        .saturating_add(if properties {
            property_extra_workspace(plan, budgets)
        } else {
            0
        })
}

/// The concurrency a scratch build of `plan` derives under `budget` with
/// `workers` available, for tests that choose a budget by the concurrency it
/// admits.
pub(crate) fn derived_concurrency(
    plan: &BulkBuildPlan<'_>,
    budget: u64,
    workers: usize,
    budgets: super::GraphConstructionBudgets,
) -> usize {
    ScratchPlan::derive_with_budgets(plan, budget, workers, budgets).concurrency
}
