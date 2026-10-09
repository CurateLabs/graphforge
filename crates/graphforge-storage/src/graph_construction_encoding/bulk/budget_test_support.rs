//! Small-fixture controls and assertions for the scratch budget tests.

use super::*;

thread_local! {
    /// Forces the partition counts of a test build (edge, CSR).
    pub(super) static FORCED_PARTITIONS: std::cell::Cell<Option<(usize, usize)>> =
        const { std::cell::Cell::new(None) };
    pub(super) static FORCED_GATE: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
    pub(super) static FORCED_CONCURRENCY: std::cell::Cell<Option<usize>> =
        const { std::cell::Cell::new(None) };
    pub(super) static FORCED_STAGGER: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
    pub(super) static FORCED_NODE_PARTITIONS: std::cell::Cell<Option<usize>> =
        const { std::cell::Cell::new(None) };
}

/// Forces the partition counts of the builds the current test thread runs,
/// until dropped, so a small graph spans several partitions.
pub(crate) struct ForcedPartitions;

impl ForcedPartitions {
    pub(crate) fn with_gate(bytes: u64) -> Self {
        FORCED_GATE.with(|forced| forced.set(Some(bytes)));
        Self
    }

    /// Makes earlier partitions slower than later ones, so a pass that must
    /// hand its turns over in order is tested against the worst schedule.
    pub(crate) fn with_stagger(millis: u64) -> Self {
        FORCED_STAGGER.with(|forced| forced.set(Some(millis)));
        Self
    }

    /// Partitions in flight, whatever the budget would allow.
    pub(crate) fn with_concurrency(workers: usize) -> Self {
        FORCED_CONCURRENCY.with(|forced| forced.set(Some(workers)));
        Self
    }

    pub(crate) fn set(edge: usize, csr: usize) -> Self {
        FORCED_PARTITIONS.with(|forced| forced.set(Some((edge, csr))));
        Self
    }

    /// Also forces the node-UUID partitions of a build with scratch node tables.
    pub(crate) fn with_nodes(self, nodes: usize) -> Self {
        FORCED_NODE_PARTITIONS.with(|forced| forced.set(Some(nodes)));
        self
    }
}

impl Drop for ForcedPartitions {
    fn drop(&mut self) {
        FORCED_PARTITIONS.with(|forced| forced.set(None));
        FORCED_GATE.with(|forced| forced.set(None));
        FORCED_CONCURRENCY.with(|forced| forced.set(None));
        FORCED_STAGGER.with(|forced| forced.set(None));
        FORCED_NODE_PARTITIONS.with(|forced| forced.set(None));
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
            stagger_millis: 0,
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
