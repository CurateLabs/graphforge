//! Plan-time routing and sizing of the bulk builder (#1900).
//!
//! The footers fix the answer to two questions before a byte is read:
//!
//! - which route builds the generation: in memory, on scratch files, or on the
//!   staged path (a typed reason says why not);
//! - for the scratch route, how many partitions and how many partitions in
//!   flight fit the normalized builder workspace inside the budget. Registered
//!   source decoding and normalization require the separate bound in #1918.
//!
//! The constants are fitted to measured runs (see ADR 0058) and rounded up.

use serde::{Deserialize, Serialize};

use super::plan::BulkBuildPlan;

/// Allocator, thread stacks, and the Parquet and Arrow runtime.
const RUNTIME_BYTES: u64 = 192 << 20;
/// One canonical CSR shard, its carried records and Arrow IPC encoder. The
/// published format caps both nodes and entries at 1,048,576 per shard. Only
/// one encoder and carry are live on the scratch adjacency path, regardless
/// of the number of relation types.
const CSR_WORKSPACE_BYTES: u64 = 256 << 20;
/// Minimum reusable scatter/sort working set. It is included in the route's
/// fixed footprint, rather than manufactured after the budget is exhausted.
const MIN_WORKING_BYTES: u64 = 64 << 20;
const FIXED_BYTES: u64 = RUNTIME_BYTES + CSR_WORKSPACE_BYTES + MIN_WORKING_BYTES;
/// Peak bytes per node while the node tables exist: the sorted UUIDs (16), the
/// label ids (4), and the larger of the sort's working set (order array plus
/// gathered copy, 20) and the endpoint index (8 to 16).
// Exact out/in degrees add 8 B/node beside the endpoint index during scatter.
// After dropping the endpoint index, resident tables (20), degrees (8),
// partition maps (16), and at most two heavy counters (8) total 52 B/node.
const NODE_TABLE_BYTES: u64 = 56;
/// Bytes one decoding task holds in flight: a row group of input plus its
/// decoded batches.
const DECODE_WINDOW_BYTES: u64 = 48 << 20;
/// Bytes per edge a partition holds while it is built: the 28-byte record, its
/// column copies for one window, and the CSR entries it stages.
const EDGE_PARTITION_BYTES: u64 = 44;
/// Bytes per adjacency entry a CSR partition holds: the sorted records, the
/// per-relation view, and its share of the shard encoder.
const CSR_PARTITION_BYTES: u64 = 40;
/// Smallest and largest staging buffer per partition per worker.
const MIN_STAGING_BYTES: u64 = 8 << 10;
const MAX_STAGING_BYTES: u64 = 256 << 10;
/// Partitions per set. Files open per block, so this bounds only bookkeeping.
const MAX_PARTITIONS: u64 = 4096;

/// Why an initial build cannot run on the bulk builder and stages instead.
///
/// Chosen once, at plan time, from the footers and the memory budget.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BulkStagedReason {
    /// The node tables (sorted UUIDs, labels, endpoint index, and any retained
    /// cached source metadata) do not fit the budget. External node handling is not
    /// implemented (#1881).
    NodeTablesExceedBudget,
    /// Historical reason retained for manifest decoding. New property-bearing
    /// plans use bounded property scratch instead of selecting this reason.
    EdgePropertiesExceedBudget,
}

/// How the bulk builder will build a plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BulkRoute {
    /// Everything resident: the estimate fits the budget.
    Memory,
    /// Edge records and adjacency entries go through scratch files.
    Scratch,
    /// The staged path builds it.
    Staged(BulkStagedReason),
}

impl BulkBuildPlan<'_> {
    fn node_rows(&self) -> u64 {
        self.nodes.iter().map(|source| source.rows).sum()
    }

    fn edge_rows(&self) -> u64 {
        self.edges.iter().map(|source| source.rows).sum()
    }

    pub(super) fn max_source_schema_bytes(&self) -> u64 {
        self.nodes
            .iter()
            .chain(&self.edges)
            .map(|source| source.reader.schema_resident_bytes())
            .max()
            .unwrap_or(0)
    }

    pub(super) fn source_decoder_bytes(&self) -> u64 {
        self.nodes
            .iter()
            .chain(&self.edges)
            .map(|source| source.reader.decoded_workspace_bytes())
            .max()
            .unwrap_or(0)
    }

    /// The node tables and minimum fixed builder workspace. In particular,
    /// this includes one bounded canonical CSR encoder, not one per relation.
    #[must_use]
    pub fn node_tables_resident_bytes(&self) -> u64 {
        FIXED_BYTES
            .saturating_add(self.node_rows().saturating_mul(NODE_TABLE_BYTES))
            .saturating_add(
                self.nodes
                    .iter()
                    .chain(&self.edges)
                    .map(|source| source.reader.retained_metadata_bytes())
                    .fold(0_u64, u64::saturating_add),
            )
    }

    /// Route the plan for `budget` resident bytes.
    #[must_use]
    pub fn route_for(&self, budget: u64) -> BulkRoute {
        if self.estimated_resident_bytes() <= budget {
            BulkRoute::Memory
        } else if self.node_tables_resident_bytes() > budget {
            BulkRoute::Staged(BulkStagedReason::NodeTablesExceedBudget)
        } else {
            BulkRoute::Scratch
        }
    }

    /// The route under the plan's own budget; without one, in memory.
    #[must_use]
    pub fn route(&self) -> BulkRoute {
        self.memory_budget
            .map_or(BulkRoute::Memory, |budget| self.route_for(budget))
    }
}

/// Decoded payload, IPC and encoder transients plus the logical window's
/// owner bitsets and identifier inventory. The CSR workspace is reusable.
pub(super) fn property_workspace(budgets: super::GraphConstructionBudgets) -> u64 {
    (budgets.max_batch_bytes as u64)
        .saturating_mul(8)
        .saturating_add(budgets.max_catalog_identifier_bytes as u64)
        .saturating_add(
            (budgets.max_batch_rows as u64).saturating_mul(
                (budgets.max_property_columns.div_ceil(64) as u64)
                    .saturating_mul(8)
                    .saturating_add(128),
            ),
        )
        .saturating_add(32 << 20)
}

pub(super) fn property_extra_workspace(
    plan: &BulkBuildPlan<'_>,
    budgets: super::GraphConstructionBudgets,
) -> u64 {
    property_workspace(budgets)
        .saturating_add(plan.max_source_schema_bytes().saturating_mul(8))
        .saturating_add(plan.source_decoder_bytes())
        .saturating_sub(CSR_WORKSPACE_BYTES)
}

/// Sizes of one scratch build.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ScratchPlan {
    /// Partitions in flight, and the threads that run them.
    pub(super) concurrency: usize,
    /// Edge-UUID range partitions.
    pub(super) edge_partitions: usize,
    /// Node-range partitions per CSR direction.
    pub(super) csr_partitions: usize,
    /// Bytes the partitions in flight may reserve in total.
    pub(super) gate_bytes: u64,
    /// Staging buffer per partition per worker.
    pub(super) staging_bytes: usize,
}

#[cfg(test)]
thread_local! {
    /// Forces the partition counts of a test build (edge, CSR).
    static FORCED_PARTITIONS: std::cell::Cell<Option<(usize, usize)>> =
        const { std::cell::Cell::new(None) };
    static FORCED_GATE: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
}

/// Forces the partition counts of the builds the current test thread runs,
/// until dropped, so a small graph spans several partitions.
#[cfg(test)]
pub(crate) struct ForcedPartitions;

#[cfg(test)]
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

#[cfg(test)]
impl Drop for ForcedPartitions {
    fn drop(&mut self) {
        FORCED_PARTITIONS.with(|forced| forced.set(None));
        FORCED_GATE.with(|forced| forced.set(None));
    }
}

impl ScratchPlan {
    #[cfg(test)]
    pub(super) fn derive(plan: &BulkBuildPlan<'_>, budget: u64, workers: usize) -> Self {
        Self::derive_with_budgets(
            plan,
            budget,
            workers,
            super::GraphConstructionBudgets::default(),
        )
    }

    pub(super) fn derive_with_budgets(
        plan: &BulkBuildPlan<'_>,
        budget: u64,
        workers: usize,
        budgets: super::GraphConstructionBudgets,
    ) -> Self {
        let edges = plan.edge_rows();
        let properties = plan
            .nodes
            .iter()
            .chain(&plan.edges)
            .any(|source| !source.property_free);
        let fixed = plan
            .node_tables_resident_bytes()
            .saturating_add(if properties {
                property_extra_workspace(plan, budgets)
            } else {
                0
            });
        let working = budget
            .saturating_sub(fixed)
            .saturating_add(MIN_WORKING_BYTES);
        // Three quarters of the working set hold partitions; the rest stages
        // scatter buffers and decoded input.
        let gate_bytes = working / 4 * 3;
        #[cfg(test)]
        let gate_bytes = FORCED_GATE.with(std::cell::Cell::get).unwrap_or(gate_bytes);
        let staging_total = working / 8;
        // A task's decode window is the larger of the planning constant and what
        // the sources need to open one row group (#1918).
        let decode_window = DECODE_WINDOW_BYTES.max(plan.source_decoder_bytes());
        let ceil = |bytes: u64, per: u64| bytes.div_ceil(per.max(1));
        let mut best = None;
        for concurrency in (1..=if properties { 1 } else { workers.max(1) as u64 }).rev() {
            // Decoding tasks in flight must fit beside the staging buffers.
            if concurrency > 1 && concurrency * decode_window > working / 4 {
                continue;
            }
            let per_partition = gate_bytes / (2 * concurrency);
            let edge_partitions = ceil(edges.saturating_mul(EDGE_PARTITION_BYTES), per_partition)
                .clamp(1, MAX_PARTITIONS);
            let csr_partitions = ceil(edges.saturating_mul(CSR_PARTITION_BYTES), per_partition)
                .clamp(1, MAX_PARTITIONS);
            let widest = edge_partitions.max(2 * csr_partitions);
            let staging = (staging_total / (concurrency * widest)).min(MAX_STAGING_BYTES);
            if staging >= MIN_STAGING_BYTES {
                best = Some((concurrency, edge_partitions, csr_partitions, staging));
                break;
            }
        }
        // If even one worker cannot stage at the preferred minimum, smaller
        // blocks preserve the same total buffer reservation. Radix refinement
        // will bound sorting independently of the initial partition cap.
        let (concurrency, edge_partitions, csr_partitions, staging) = best.unwrap_or_else(|| {
            let per_partition = gate_bytes / 2;
            (
                1,
                ceil(edges.saturating_mul(EDGE_PARTITION_BYTES), per_partition)
                    .clamp(1, MAX_PARTITIONS),
                ceil(edges.saturating_mul(CSR_PARTITION_BYTES), per_partition)
                    .clamp(1, MAX_PARTITIONS),
                (staging_total / (2 * MAX_PARTITIONS)).max(32),
            )
        });
        #[cfg(test)]
        let (edge_partitions, csr_partitions) = FORCED_PARTITIONS
            .with(std::cell::Cell::get)
            .map_or((edge_partitions, csr_partitions), |(edge, csr)| {
                (edge as u64, csr as u64)
            });
        Self {
            concurrency: usize::try_from(concurrency).unwrap_or(1),
            edge_partitions: usize::try_from(edge_partitions).unwrap_or(1),
            csr_partitions: usize::try_from(csr_partitions).unwrap_or(1),
            gate_bytes,
            staging_bytes: usize::try_from(staging).unwrap_or(8 << 10),
        }
    }

    /// Bytes the registered-source readers' tasks may reserve at once (#1918).
    ///
    /// With properties one task runs at a time and shares the property workspace,
    /// already reserved in the fixed footprint, with the pages its sources hold.
    /// Without, the decoding quarter of the working set (see `derive`).
    pub(super) fn source_pool_bytes(
        plan: &BulkBuildPlan<'_>,
        budget: u64,
        budgets: super::GraphConstructionBudgets,
    ) -> u64 {
        let decoder = plan.source_decoder_bytes();
        if plan
            .nodes
            .iter()
            .chain(&plan.edges)
            .any(|source| !source.property_free)
        {
            property_workspace(budgets).saturating_add(decoder)
        } else {
            let working = budget
                .saturating_sub(plan.node_tables_resident_bytes())
                .saturating_add(MIN_WORKING_BYTES);
            (working / 4).max(decoder)
        }
    }

    /// Bytes a partition of `rows` edges reserves while it is built.
    pub(super) fn edge_cost(rows: u64) -> u64 {
        rows.saturating_mul(EDGE_PARTITION_BYTES)
    }

    /// Bytes a CSR partition of `entries` adjacency entries reserves.
    pub(super) fn csr_cost(entries: u64) -> u64 {
        entries.saturating_mul(CSR_PARTITION_BYTES)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::record_batch::RecordBatch;
    use graphforge_core::GfError;

    use super::*;
    use crate::graph_construction_encoding::{BulkBatchReader, BulkSource};

    struct Never;

    impl BulkBatchReader for Never {
        fn task_rows(&self, _: usize) -> usize {
            unreachable!("planning only")
        }

        fn read_task(
            &self,
            _: usize,
            _: &mut dyn FnMut(RecordBatch) -> Result<(), GfError>,
        ) -> Result<(), GfError> {
            unreachable!("planning only")
        }
    }

    fn rung(scale: u32) -> BulkBuildPlan<'static> {
        let source = |rows: u64| BulkSource {
            reader: Arc::new(Never),
            tasks: 1,
            rows,
            property_free: true,
            decoded_bytes: 0,
        };
        BulkBuildPlan {
            nodes: vec![source(1 << scale)],
            edges: vec![source(16 << scale)],
            memory_budget: None,
        }
    }

    const GIB: u64 = 1 << 30;

    #[test]
    fn partition_sizes_and_concurrency_follow_the_budget() {
        for scale in [22, 24, 26] {
            let plan = rung(scale);
            let mut previous = None::<ScratchPlan>;
            for budget in [8 * GIB, 4 * GIB, 2 * GIB, GIB + GIB / 2] {
                if budget < plan.node_tables_resident_bytes() {
                    continue;
                }
                let sized = ScratchPlan::derive(&plan, budget, 16);
                assert!((1..=16).contains(&sized.concurrency), "{sized:?}");
                assert!(sized.staging_bytes >= 32, "{sized:?}");
                let working = budget - plan.node_tables_resident_bytes() + MIN_WORKING_BYTES;
                let buffers = sized.concurrency as u64
                    * (sized.edge_partitions.max(2 * sized.csr_partitions) as u64)
                    * sized.staging_bytes as u64;
                assert!(buffers <= working / 8, "{sized:?}");
                assert!(
                    plan.node_tables_resident_bytes() - MIN_WORKING_BYTES
                        + sized.gate_bytes
                        + working / 4
                        <= budget,
                    "{sized:?}"
                );
                // The partitions in flight reserve at most half the gate, so a
                // partition twice its share still fits.
                let edges = 16_u64 << scale;
                let in_flight =
                    ScratchPlan::edge_cost(edges.div_ceil(sized.edge_partitions as u64))
                        * sized.concurrency as u64;
                assert!(
                    sized.edge_partitions as u64 == MAX_PARTITIONS
                        || in_flight <= sized.gate_bytes / 2,
                    "S{scale} budget {budget}: {sized:?} reserves {in_flight}"
                );
                // A smaller budget reserves no more in flight.
                if let Some(larger) = previous {
                    assert!(
                        sized.gate_bytes <= larger.gate_bytes,
                        "{sized:?} {larger:?}"
                    );
                }
                previous = Some(sized);
            }
        }
    }

    #[test]
    fn a_tiny_input_is_one_partition_and_the_largest_is_bounded() {
        let tiny = ScratchPlan::derive(&rung(4), GIB, 16);
        assert_eq!((tiny.edge_partitions, tiny.csr_partitions), (1, 1));
        let huge = ScratchPlan::derive(&rung(30), 4 * GIB, 16);
        assert!(huge.edge_partitions as u64 <= MAX_PARTITIONS);
        assert!(huge.csr_partitions as u64 <= MAX_PARTITIONS);
    }
}
