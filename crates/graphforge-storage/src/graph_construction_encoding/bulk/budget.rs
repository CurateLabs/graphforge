//! Plan-time routing and sizing of the bulk builder (#1900).
//!
//! The footers fix the answer to two questions before a byte is read:
//!
//! - which route builds the generation: in memory, on scratch files with
//!   resident node tables, or on scratch files with the node tables on
//!   scratch too (the staged path remains only for appends and sessions an
//!   earlier binary began);
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
/// The same when node tables are on scratch: the partition also holds the two
/// 37-byte resolved endpoint records of each edge until they are joined, then
/// the two endpoint UUIDs.
const EDGE_PARTITION_BYTES_NODE_SCRATCH: u64 = 144;
/// Bytes per node a node partition holds while endpoints resolve: the 20-byte
/// record, the out and in degrees, and slack for the sort.
const NODE_PARTITION_BYTES: u64 = 40;
/// Bytes per adjacency entry a CSR partition holds: the sorted records, the
/// per-relation view, and its share of the shard encoder.
const CSR_PARTITION_BYTES: u64 = 40;
/// Smallest and largest staging buffer per partition per worker.
const MIN_STAGING_BYTES: u64 = 8 << 10;
const MAX_STAGING_BYTES: u64 = 256 << 10;
/// Partitions per set. Files open per block, so this bounds only bookkeeping.
const MAX_PARTITIONS: u64 = 4096;

/// Why an initial build staged instead of running on the bulk builder.
///
/// Both reasons are historical: they stay readable in manifests an earlier
/// binary wrote, and no new plan selects either.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BulkStagedReason {
    /// Historical reason retained for manifest decoding. New plans keep the node
    /// tables on scratch (`BulkRoute::ScratchNodes`) when they do not fit the
    /// budget (#1929).
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
    /// The node tables go through scratch files as well: node identities,
    /// endpoint resolution, degrees and CSR key ranges are built per node-UUID
    /// range partition (#1929).
    ScratchNodes,
    /// The staged path builds it. No plan selects this route; it is kept so a
    /// historical receipt still names a route.
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

    /// The workspace property-bearing sources add; zero for property-free input.
    #[cfg(test)]
    pub(crate) fn property_floor_bytes(&self, budgets: super::GraphConstructionBudgets) -> u64 {
        property_extra_if_any(self, budgets)
    }

    /// The fixed workspace and cached source metadata, without any node
    /// tables: the least a build on scratch needs resident.
    #[must_use]
    pub(crate) fn scratch_floor_bytes(&self) -> u64 {
        self.node_tables_resident_bytes()
            .saturating_sub(self.node_rows().saturating_mul(NODE_TABLE_BYTES))
    }

    /// Route the plan for `budget` resident bytes.
    #[must_use]
    pub fn route_for(&self, budget: u64) -> BulkRoute {
        if self.estimated_resident_bytes() <= budget {
            BulkRoute::Memory
        } else if self.node_tables_resident_bytes() > budget {
            BulkRoute::ScratchNodes
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

/// Whether the node tables go to scratch under `budget`: the resident node
/// tables and, for property-bearing input, the property workspace do not fit
/// beside the fixed workspace. [`BulkBuildPlan::route_for`] sees only the node
/// tables; the property workspace depends on the construction budgets.
pub(super) fn node_tables_on_scratch(
    plan: &BulkBuildPlan<'_>,
    budget: u64,
    budgets: super::GraphConstructionBudgets,
) -> bool {
    // A test that forces node partitions forces the route, so a build with no
    // nodes at all, which no budget can push over, still exercises it.
    #[cfg(test)]
    if FORCED_NODE_PARTITIONS.with(std::cell::Cell::get).is_some() {
        return true;
    }
    plan.node_tables_resident_bytes()
        .saturating_add(property_extra_if_any(plan, budgets))
        > budget
}

/// The property workspace of a plan that has property-bearing sources.
pub(super) fn property_extra_if_any(
    plan: &BulkBuildPlan<'_>,
    budgets: super::GraphConstructionBudgets,
) -> u64 {
    if plan
        .nodes
        .iter()
        .chain(&plan.edges)
        .any(|source| !source.property_free)
    {
        property_extra_workspace(plan, budgets)
    } else {
        0
    }
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
    /// Whether the node tables are on scratch.
    pub(super) node_tables_on_scratch: bool,
    /// Node-UUID range partitions when the node tables are on scratch.
    pub(super) node_partitions: usize,
    /// Bytes the partitions in flight may reserve in total.
    pub(super) gate_bytes: u64,
    /// Staging buffer per partition per worker.
    pub(super) staging_bytes: usize,
    /// Total staging allowance, shared by every open partition buffer.
    pub(super) staging_total: u64,
    /// Test builds only: milliseconds each partition sleeps per partition after
    /// it, before it takes its turn, so later partitions finish first.
    #[cfg(test)]
    pub(super) stagger_millis: u64,
    /// Bytes an edge partition reserves per row.
    pub(super) edge_row_bytes: u64,
    /// Bytes a node partition reserves per row; zero when nodes are resident.
    pub(super) node_row_bytes: u64,
}

#[cfg(test)]
thread_local! {
    /// Forces the partition counts of a test build (edge, CSR).
    static FORCED_PARTITIONS: std::cell::Cell<Option<(usize, usize)>> =
        const { std::cell::Cell::new(None) };
    static FORCED_GATE: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
    static FORCED_CONCURRENCY: std::cell::Cell<Option<usize>> =
        const { std::cell::Cell::new(None) };
    static FORCED_STAGGER: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
    static FORCED_NODE_PARTITIONS: std::cell::Cell<Option<usize>> =
        const { std::cell::Cell::new(None) };
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

#[cfg(test)]
impl Drop for ForcedPartitions {
    fn drop(&mut self) {
        FORCED_PARTITIONS.with(|forced| forced.set(None));
        FORCED_GATE.with(|forced| forced.set(None));
        FORCED_CONCURRENCY.with(|forced| forced.set(None));
        FORCED_STAGGER.with(|forced| forced.set(None));
        FORCED_NODE_PARTITIONS.with(|forced| forced.set(None));
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

    #[allow(clippy::too_many_lines)]
    pub(super) fn derive_with_budgets(
        plan: &BulkBuildPlan<'_>,
        budget: u64,
        workers: usize,
        budgets: super::GraphConstructionBudgets,
    ) -> Self {
        let edges = plan.edge_rows();
        let nodes = plan.node_rows();
        // With node tables on scratch no byte per node is resident; the node
        // partitions reserve their share from the gate instead.
        let node_scratch = node_tables_on_scratch(plan, budget, budgets);
        let (edge_row_bytes, node_row_bytes) = if node_scratch {
            (EDGE_PARTITION_BYTES_NODE_SCRATCH, NODE_PARTITION_BYTES)
        } else {
            (EDGE_PARTITION_BYTES, 0)
        };
        let properties = plan
            .nodes
            .iter()
            .chain(&plan.edges)
            .any(|source| !source.property_free);
        let fixed = if node_scratch {
            plan.scratch_floor_bytes()
        } else {
            plan.node_tables_resident_bytes()
        }
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
        let ceil = |bytes: u64, per: u64| bytes.div_ceil(per.max(1));
        let mut best = None;
        for concurrency in (1..=if properties { 1 } else { workers.max(1) as u64 }).rev() {
            // Decoding tasks in flight must fit beside the staging buffers.
            if concurrency > 1 && concurrency * DECODE_WINDOW_BYTES > working / 4 {
                continue;
            }
            let per_partition = gate_bytes / (2 * concurrency);
            let edge_partitions =
                ceil(edges.saturating_mul(edge_row_bytes), per_partition).clamp(1, MAX_PARTITIONS);
            let csr_partitions = ceil(edges.saturating_mul(CSR_PARTITION_BYTES), per_partition)
                .clamp(1, MAX_PARTITIONS);
            let node_partitions = if node_scratch {
                ceil(nodes.saturating_mul(node_row_bytes), per_partition).clamp(1, MAX_PARTITIONS)
            } else {
                0
            };
            let widest = (edge_partitions + node_partitions).max(2 * csr_partitions);
            let staging = (staging_total / (concurrency * widest)).min(MAX_STAGING_BYTES);
            if staging >= MIN_STAGING_BYTES {
                best = Some((
                    concurrency,
                    edge_partitions,
                    csr_partitions,
                    node_partitions,
                    staging,
                ));
                break;
            }
        }
        // If even one worker cannot stage at the preferred minimum, smaller
        // blocks preserve the same total buffer reservation. Radix refinement
        // will bound sorting independently of the initial partition cap.
        let (concurrency, edge_partitions, csr_partitions, node_partitions, staging) = best
            .unwrap_or_else(|| {
                let per_partition = gate_bytes / 2;
                (
                    1,
                    ceil(edges.saturating_mul(edge_row_bytes), per_partition)
                        .clamp(1, MAX_PARTITIONS),
                    ceil(edges.saturating_mul(CSR_PARTITION_BYTES), per_partition)
                        .clamp(1, MAX_PARTITIONS),
                    if node_scratch {
                        ceil(nodes.saturating_mul(node_row_bytes), per_partition)
                            .clamp(1, MAX_PARTITIONS)
                    } else {
                        0
                    },
                    (staging_total / (2 * MAX_PARTITIONS)).max(32),
                )
            });
        #[cfg(test)]
        let concurrency = FORCED_CONCURRENCY
            .with(std::cell::Cell::get)
            .map_or(concurrency, |forced| forced as u64);
        #[cfg(test)]
        let node_partitions =
            FORCED_NODE_PARTITIONS
                .with(std::cell::Cell::get)
                .map_or(node_partitions, |forced| {
                    if node_scratch { forced as u64 } else { 0 }
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
            node_tables_on_scratch: node_scratch,
            node_partitions: usize::try_from(node_partitions).unwrap_or(1),
            gate_bytes,
            staging_bytes: usize::try_from(staging).unwrap_or(8 << 10),
            staging_total,
            #[cfg(test)]
            stagger_millis: FORCED_STAGGER.with(std::cell::Cell::get).unwrap_or(0),
            edge_row_bytes,
            node_row_bytes,
        }
    }

    /// A plan with resident node tables and exactly these sizes.
    #[cfg(test)]
    pub(super) fn sized(
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
        }
    }

    /// Staging buffer per partition per worker when `fanout` partitions are
    /// open at once, from the plan's total staging allowance.
    pub(super) fn staging_for(&self, fanout: usize) -> usize {
        let share = self.staging_total / (self.concurrency.max(1) as u64 * fanout.max(1) as u64);
        usize::try_from(share.clamp(64, MAX_STAGING_BYTES)).unwrap_or(64)
    }

    /// Bytes a partition of `rows` edges reserves while it is built.
    pub(super) fn edge_cost(&self, rows: u64) -> u64 {
        rows.saturating_mul(self.edge_row_bytes)
    }

    /// Bytes a node partition of `rows` nodes reserves while it is resolved.
    pub(super) fn node_cost(&self, rows: u64) -> u64 {
        rows.saturating_mul(self.node_row_bytes)
    }

    /// The most adjacency entries one CSR partition may hold.
    pub(super) fn csr_entry_limit(&self) -> u64 {
        self.gate_bytes / (2 * self.concurrency.max(1) as u64) / CSR_PARTITION_BYTES
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
                let in_flight = sized.edge_cost(edges.div_ceil(sized.edge_partitions as u64))
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
