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
pub(crate) struct ForcedPartitions {
    previous_partitions: Option<(usize, usize)>,
    previous_gate: Option<u64>,
}

impl ForcedPartitions {
    pub(crate) fn with_gate(bytes: u64) -> Self {
        let previous = Self::snapshot();
        FORCED_GATE.with(|forced| forced.set(Some(bytes)));
        previous
    }

    pub(crate) fn set(edge: usize, csr: usize) -> Self {
        let previous = Self::snapshot();
        FORCED_PARTITIONS.with(|forced| forced.set(Some((edge, csr))));
        previous
    }

    fn snapshot() -> Self {
        Self {
            previous_partitions: FORCED_PARTITIONS.with(std::cell::Cell::get),
            previous_gate: FORCED_GATE.with(std::cell::Cell::get),
        }
    }
}

impl Drop for ForcedPartitions {
    fn drop(&mut self) {
        FORCED_PARTITIONS.with(|forced| forced.set(self.previous_partitions));
        FORCED_GATE.with(|forced| forced.set(self.previous_gate));
    }
}

#[cfg(test)]
#[test]
fn nested_scratch_budget_controls_restore_the_outer_values() {
    let _baseline = ForcedPartitions::set(2, 3);
    let _outer = ForcedPartitions::with_gate(32 << 10);
    let _inner = ForcedPartitions::set(7, 5);
    assert_eq!(FORCED_PARTITIONS.with(std::cell::Cell::get), Some((7, 5)));
    assert_eq!(FORCED_GATE.with(std::cell::Cell::get), Some(32 << 10));
    drop(_inner);
    assert_eq!(FORCED_PARTITIONS.with(std::cell::Cell::get), Some((2, 3)));
    assert_eq!(FORCED_GATE.with(std::cell::Cell::get), Some(32 << 10));
    drop(_outer);
    assert_eq!(FORCED_PARTITIONS.with(std::cell::Cell::get), Some((2, 3)));
    assert_eq!(FORCED_GATE.with(std::cell::Cell::get), None);
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

impl ScratchPlan {
    /// A plan with resident node tables and exactly these sizes.
    pub(crate) fn sized(
        concurrency: usize,
        edge_partitions: usize,
        csr_partitions: usize,
        gate_bytes: u64,
        staging_bytes: usize,
    ) -> Self {
        Self {
            concurrency,
            edge_partitions,
            csr_partitions,
            node_tables_on_scratch: false,
            node_partitions: 0,
            gate_bytes,
            staging_bytes,
            staging_total: staging_bytes as u64 * 64,
            edge_row_bytes: EDGE_PARTITION_BYTES,
            node_row_bytes: 0,
            property: PropertySizing::SERIAL,
            decode_bytes: 0,
        }
    }
}

impl BulkBuildPlan<'_> {
    pub(crate) fn property_floor_bytes(&self, budgets: super::GraphConstructionBudgets) -> u64 {
        property_extra_if_any(self, budgets)
    }
}
