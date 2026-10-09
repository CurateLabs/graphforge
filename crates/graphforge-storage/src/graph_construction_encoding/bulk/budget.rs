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
use super::property_rows::PropertySizing;

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
/// Smallest and largest bytes one worker retains to form a property run. A
/// smaller run gives more runs to merge, a larger one more retained bytes.
const MIN_RUN_BYTES: u64 = 8 << 20;
const MAX_RUN_BYTES: u64 = 64 << 20;
/// Most runs one property merge holds open, and the frame sizes it may use.
const MAX_FAN_IN: u64 = 64;
const TARGET_FAN_IN: u64 = 32;
const MIN_FRAME_BYTES: u64 = 64 << 10;
const MAX_FRAME_BYTES: u64 = 1 << 20;
/// Bytes a merge holds per input and per frame byte: the IPC payload, the
/// decoded batch, the batch gathered into the chunk, and a share of the
/// output frame and its encoding.
const MERGE_FRAMES_PER_INPUT: u64 = 4;
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

/// Run formation and merging of property rows within `pool` bytes, shared by
/// `concurrency` workers. Formation retains whole runs; the merge, which runs
/// after it, holds one frame per input, so both draw on the same pool.
fn property_sizing(pool: u64, concurrency: u64, schema_bytes: u64) -> PropertySizing {
    let run_bytes = (pool / concurrency).clamp(1 << 20, MAX_RUN_BYTES);
    let frame_bytes = (pool / (concurrency * TARGET_FAN_IN * MERGE_FRAMES_PER_INPUT))
        .clamp(MIN_FRAME_BYTES, MAX_FRAME_BYTES);
    let per_input = MERGE_FRAMES_PER_INPUT
        .saturating_mul(frame_bytes)
        .saturating_add(schema_bytes);
    PropertySizing {
        run_bytes: usize::try_from(run_bytes).unwrap_or(usize::MAX),
        retained_bytes: pool.max(run_bytes),
        fan_in: usize::try_from((pool / (concurrency * per_input)).clamp(2, MAX_FAN_IN))
            .unwrap_or(2),
        frame_bytes: usize::try_from(frame_bytes).unwrap_or(usize::MAX),
    }
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
    /// Run formation and merging of property rows.
    pub(super) property: PropertySizing,
    /// Bytes the tasks decoding at once may reserve in total.
    pub(super) decode_bytes: u64,
}

#[cfg(test)]
thread_local! {
    /// Forces the partition counts of a test build (edge, CSR).
    static FORCED_PARTITIONS: std::cell::Cell<Option<(usize, usize)>> =
        const { std::cell::Cell::new(None) };
    static FORCED_GATE: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
    static FORCED_DECODE: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
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

    /// Forces the bytes the decoding tasks may reserve in total.
    pub(crate) fn with_decode_pool(bytes: u64) -> Self {
        FORCED_DECODE.with(|forced| forced.set(Some(bytes)));
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
        FORCED_DECODE.with(|forced| forced.set(None));
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
        // Held whatever the concurrency: the node tables, the fixed workspace,
        // and (with properties) the overlay writer's workspace.
        let shared = plan
            .node_tables_resident_bytes()
            .saturating_add(if properties {
                property_extra_workspace(plan, budgets)
            } else {
                0
            });
        // What the workers share beyond that: staging, the bytes the tasks in
        // flight decode, the bytes the property sort retains, and the partitions
        // in flight. Which tasks decode at once is decided by the decode pool at
        // run time, so the worker count is not charged one decoder each.
        let working = budget
            .saturating_sub(shared)
            .saturating_add(MIN_WORKING_BYTES);
        let decoder = DECODE_WINDOW_BYTES;
        let ceil = |bytes: u64, per: u64| bytes.div_ceil(per.max(1));
        let mut best = None;
        for concurrency in (1..=workers.max(1) as u64).rev() {
            // Decoding tasks in flight must fit beside the staging buffers.
            if !properties && concurrency > 1 && concurrency * decoder > working / 4 {
                continue;
            }
            // Each worker's property run fits the retained pool.
            if properties && concurrency > 1 && working / 8 * 3 / concurrency < MIN_RUN_BYTES {
                continue;
            }
            // Three quarters of the working set hold partitions; the rest stages
            // scatter buffers and decoded input.
            let gate_bytes = working / 4 * 3;
            #[cfg(test)]
            let gate_bytes = FORCED_GATE.with(std::cell::Cell::get).unwrap_or(gate_bytes);
            let per_partition = gate_bytes / (2 * concurrency);
            let edge_partitions = ceil(edges.saturating_mul(EDGE_PARTITION_BYTES), per_partition)
                .clamp(1, MAX_PARTITIONS);
            let csr_partitions = ceil(edges.saturating_mul(CSR_PARTITION_BYTES), per_partition)
                .clamp(1, MAX_PARTITIONS);
            let widest = edge_partitions.max(2 * csr_partitions);
            let staging = (working / 8 / (concurrency * widest)).min(MAX_STAGING_BYTES);
            if staging >= MIN_STAGING_BYTES {
                best = Some((
                    concurrency,
                    working,
                    gate_bytes,
                    edge_partitions,
                    csr_partitions,
                    staging,
                ));
                break;
            }
        }
        // If even one worker cannot stage at the preferred minimum, smaller
        // blocks preserve the same total buffer reservation. Radix refinement
        // will bound sorting independently of the initial partition cap.
        let (concurrency, working, gate_bytes, edge_partitions, csr_partitions, staging) = best
            .unwrap_or_else(|| {
                let gate_bytes = working / 4 * 3;
                #[cfg(test)]
                let gate_bytes = FORCED_GATE.with(std::cell::Cell::get).unwrap_or(gate_bytes);
                let per_partition = gate_bytes / 2;
                (
                    1,
                    working,
                    gate_bytes,
                    ceil(edges.saturating_mul(EDGE_PARTITION_BYTES), per_partition)
                        .clamp(1, MAX_PARTITIONS),
                    ceil(edges.saturating_mul(CSR_PARTITION_BYTES), per_partition)
                        .clamp(1, MAX_PARTITIONS),
                    (working / 8 / (2 * MAX_PARTITIONS)).max(32),
                )
            });
        #[cfg(test)]
        let (edge_partitions, csr_partitions) = FORCED_PARTITIONS
            .with(std::cell::Cell::get)
            .map_or((edge_partitions, csr_partitions), |(edge, csr)| {
                (edge as u64, csr as u64)
            });
        let decode_bytes = working / 8 * 3;
        #[cfg(test)]
        let decode_bytes = FORCED_DECODE
            .with(std::cell::Cell::get)
            .unwrap_or(decode_bytes);
        Self {
            concurrency: usize::try_from(concurrency).unwrap_or(1),
            edge_partitions: usize::try_from(edge_partitions).unwrap_or(1),
            csr_partitions: usize::try_from(csr_partitions).unwrap_or(1),
            gate_bytes,
            staging_bytes: usize::try_from(staging).unwrap_or(8 << 10),
            property: property_sizing(working / 8 * 3, concurrency, plan.max_source_schema_bytes()),
            decode_bytes,
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

/// The fewest resident bytes a scratch build of `plan` can run in.
#[cfg(test)]
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
#[cfg(test)]
pub(crate) fn derived_concurrency(
    plan: &BulkBuildPlan<'_>,
    budget: u64,
    workers: usize,
    budgets: super::GraphConstructionBudgets,
) -> usize {
    ScratchPlan::derive_with_budgets(plan, budget, workers, budgets).concurrency
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

    /// SNB BI SF1's shape: 3.0M nodes and 17.2M edges, both with properties,
    /// from 1.19 GB of Parquet.
    fn property_plan() -> BulkBuildPlan<'static> {
        let source = |rows: u64| BulkSource {
            reader: Arc::new(Never),
            tasks: 4,
            rows,
            property_free: false,
            decoded_bytes: 600 << 20,
        };
        BulkBuildPlan {
            nodes: vec![source(3_000_000)],
            edges: vec![source(17_200_000)],
            memory_budget: None,
        }
    }

    #[test]
    fn property_builds_derive_concurrency_and_sizes_from_the_budget() {
        let plan = property_plan();
        let budgets = crate::graph_construction::GraphConstructionBudgets::default();
        let floor = plan.node_tables_resident_bytes() + property_extra_workspace(&plan, budgets);
        let mut previous = 0;
        let mut distinct = std::collections::BTreeSet::new();
        for budget in [
            floor,
            floor + (64 << 20),
            floor + (128 << 20),
            floor + (256 << 20),
            floor + (512 << 20),
            2 * GIB,
            4 * GIB,
            16 * GIB,
        ] {
            if budget < floor {
                continue;
            }
            let sized = ScratchPlan::derive_with_budgets(&plan, budget, 16, budgets);
            let workers = sized.concurrency as u64;
            assert!((1..=16).contains(&workers), "{sized:?}");
            assert!(
                workers >= previous,
                "a larger budget lost workers: {sized:?}"
            );
            previous = workers;
            distinct.insert(workers);
            // The node tables, the overlay writer's workspace and the shared
            // run pool fit the budget, and the decode pool takes no more than
            // the working set leaves.
            let shared = floor - MIN_WORKING_BYTES;
            let property = sized.property;
            assert!(
                shared + property.retained_bytes + sized.decode_bytes <= budget + MIN_WORKING_BYTES
                    || property.retained_bytes <= 1 << 20,
                "budget {budget}: {sized:?}"
            );
            // One merge holds a frame set per input, for every worker at once.
            let per_input = MERGE_FRAMES_PER_INPUT * property.frame_bytes as u64
                + plan.max_source_schema_bytes();
            assert!(property.fan_in >= 2 && property.fan_in as u64 <= MAX_FAN_IN);
            assert!(
                workers * property.fan_in as u64 * per_input <= property.retained_bytes
                    || property.fan_in == 2,
                "budget {budget}: {sized:?}"
            );
            assert!(
                property.run_bytes as u64 <= MAX_RUN_BYTES && property.run_bytes >= 1 << 20,
                "{sized:?}"
            );
        }
        assert!(
            distinct.len() >= 3,
            "concurrency never grew with the budget: {distinct:?}"
        );
        // A property-free plan of the same size is not charged for property runs.
        let free = rung(21);
        assert!(
            ScratchPlan::derive(&free, 4 * GIB, 16).concurrency >= 1,
            "property-free plans keep deriving their own concurrency"
        );
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
