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

use super::GraphConstructionBudgets;
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
/// Resident bytes a property-bearing worker adds beyond the pools it draws on.
const WORKER_BYTES: u64 = 128 << 20;
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
    /// Node tables, edge records and adjacency entries use scratch files.
    ScratchNodes,
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

    /// Fixed workspace and cached source metadata without resident node tables.
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
    budgets: GraphConstructionBudgets,
) -> u64 {
    property_workspace(budgets)
        .saturating_add(plan.max_source_schema_bytes().saturating_mul(8))
        .saturating_add(plan.source_decoder_bytes())
        .saturating_sub(CSR_WORKSPACE_BYTES)
}

/// Bytes available to all property merge jobs after intake has finished.
/// The schema and decoder terms are shared with the source phase, while both
/// idle property pools are reusable because node and edge finishes are
/// sequential.
pub(super) fn property_merge_capacity(
    budgets: super::GraphConstructionBudgets,
    source_schema_bytes: u64,
    source_decoder_bytes: u64,
    property_retained_bytes: u64,
    decode_bytes: u64,
) -> u64 {
    (budgets.max_batch_bytes as u64)
        .saturating_mul(8)
        .saturating_add(source_schema_bytes.saturating_mul(8))
        .saturating_add(source_decoder_bytes)
        .saturating_add(property_retained_bytes)
        .saturating_add(decode_bytes)
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

const EDGE_PARTITION_BYTES_NODE_SCRATCH: u64 = 144;

const NODE_PARTITION_BYTES: u64 = 40;

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
    pub(super) node_tables_on_scratch: bool,
    pub(super) node_partitions: usize,
    pub(super) staging_total: u64,
    pub(super) edge_row_bytes: u64,
    pub(super) node_row_bytes: u64,
    #[cfg(test)]
    pub(super) stagger_millis: u64,
    /// Run formation and merging of property rows.
    pub(super) property: PropertySizing,
    /// Bytes the tasks decoding at once may reserve in total.
    pub(super) decode_bytes: u64,
}

#[cfg(test)]
#[path = "budget_test_support.rs"]
pub(crate) mod test_support;
#[cfg(test)]
use test_support::{
    FORCED_CONCURRENCY, FORCED_GATE, FORCED_NODE_PARTITIONS, FORCED_PARTITIONS, FORCED_STAGGER,
};
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
        let nodes = plan.node_rows();
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
        // Held whatever the concurrency: the node tables, the fixed workspace,
        // and (with properties) the overlay writer's workspace.
        let shared = if node_scratch {
            plan.scratch_floor_bytes()
        } else {
            plan.node_tables_resident_bytes()
        }
        .saturating_add(if properties {
            property_extra_workspace(plan, budgets)
        } else {
            0
        });
        // What the workers share beyond that: staging, the bytes the tasks in
        // flight decode, the bytes the property sort retains, and the partitions
        // in flight. Which tasks decode at once is decided by the decode pool at
        // run time, so the worker count is not charged one decoder each.
        let available = budget
            .saturating_sub(shared)
            .saturating_add(MIN_WORKING_BYTES);
        // Each further property-bearing worker also keeps what its thread
        // allocates and frees: measured on SNB BI SF1, peak resident memory grew by about
        // 120 MB per worker beyond the pools (1.27 GB at no workers, 2.10 GB at
        // 7, 3.05 GB at 15). Those bytes come off the working set, and the
        // workers may take at most five eighths of it.
        let overhead = |concurrency: u64| {
            if properties {
                // The first worker is part of the fixed footprint.
                (concurrency - 1).saturating_mul(WORKER_BYTES)
            } else {
                0
            }
        };
        let decoder = DECODE_WINDOW_BYTES;
        let ceil = |bytes: u64, per: u64| bytes.div_ceil(per.max(1));
        let mut best = None;
        for concurrency in (1..=workers.max(1) as u64).rev() {
            if overhead(concurrency) > available / 8 * 5 {
                continue;
            }
            let working = available.saturating_sub(overhead(concurrency));
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
            let staging = (working / 8 / (concurrency * widest)).min(MAX_STAGING_BYTES);
            if staging >= MIN_STAGING_BYTES {
                best = Some((
                    concurrency,
                    working,
                    gate_bytes,
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
        let (
            concurrency,
            working,
            gate_bytes,
            edge_partitions,
            csr_partitions,
            node_partitions,
            staging,
        ) = best.unwrap_or_else(|| {
            let working = available;
            let gate_bytes = working / 4 * 3;
            #[cfg(test)]
            let gate_bytes = FORCED_GATE.with(std::cell::Cell::get).unwrap_or(gate_bytes);
            let per_partition = gate_bytes / 2;
            (
                1,
                working,
                gate_bytes,
                ceil(edges.saturating_mul(edge_row_bytes), per_partition).clamp(1, MAX_PARTITIONS),
                ceil(edges.saturating_mul(CSR_PARTITION_BYTES), per_partition)
                    .clamp(1, MAX_PARTITIONS),
                if node_scratch {
                    ceil(nodes.saturating_mul(node_row_bytes), per_partition)
                        .clamp(1, MAX_PARTITIONS)
                } else {
                    0
                },
                (working / 8 / (2 * MAX_PARTITIONS)).max(32),
            )
        });
        #[cfg(test)]
        let (edge_partitions, csr_partitions) = FORCED_PARTITIONS
            .with(std::cell::Cell::get)
            .map_or((edge_partitions, csr_partitions), |(edge, csr)| {
                (edge as u64, csr as u64)
            });
        #[cfg(test)]
        let concurrency = FORCED_CONCURRENCY
            .with(std::cell::Cell::get)
            .map_or(concurrency, |n| n as u64);
        #[cfg(test)]
        let node_partitions = FORCED_NODE_PARTITIONS
            .with(std::cell::Cell::get)
            .map_or(node_partitions, |n| if node_scratch { n as u64 } else { 0 });
        let decode_bytes = working / 8 * 3;
        Self {
            concurrency: usize::try_from(concurrency).unwrap_or(1),
            edge_partitions: usize::try_from(edge_partitions).unwrap_or(1),
            csr_partitions: usize::try_from(csr_partitions).unwrap_or(1),
            node_tables_on_scratch: node_scratch,
            node_partitions: usize::try_from(node_partitions).unwrap_or(1),
            staging_total: working / 8,
            edge_row_bytes,
            node_row_bytes,
            #[cfg(test)]
            stagger_millis: FORCED_STAGGER.with(std::cell::Cell::get).unwrap_or(0),
            gate_bytes,
            staging_bytes: usize::try_from(staging).unwrap_or(8 << 10),
            property: property_sizing(working / 8 * 3, concurrency, plan.max_source_schema_bytes()),
            decode_bytes,
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
#[path = "budget_tests.rs"]
mod tests;
