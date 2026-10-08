//! Plan-time routing and sizing of the bulk builder (#1900).
//!
//! The footers fix the answer to two questions before a byte is read:
//!
//! - which route builds the generation: in memory, on scratch files, or on the
//!   staged path (a typed reason says why not);
//! - for the scratch route, how many partitions and how many partitions in
//!   flight keep the process inside the budget.
//!
//! The constants are fitted to measured runs (see ADR 0058) and rounded up.

use serde::{Deserialize, Serialize};

use super::plan::BulkBuildPlan;

/// Resident bytes outside the data the scratch passes hold: allocator, thread
/// stacks, the Parquet and Arrow runtime.
const FIXED_BYTES: u64 = 512 << 20;
/// Peak bytes per node while the node tables exist: the sorted UUIDs (16), the
/// label ids (4), and the larger of the sort's working set (order array plus
/// gathered copy, 20) and the endpoint index (8 to 16).
const NODE_TABLE_BYTES: u64 = 40;
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
    /// node properties) do not fit the budget. External node handling is not
    /// implemented (#1881).
    NodeTablesExceedBudget,
    /// The edge kind carries properties, which the builder retains in memory.
    /// Edge properties on scratch are not implemented (#1881).
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

    /// Decoded bytes of the node kind when it retains properties, else zero.
    fn retained_node_bytes(&self) -> u64 {
        if self.nodes.iter().all(|source| source.property_free) {
            0
        } else {
            self.nodes
                .iter()
                .map(|source| source.decoded_bytes)
                .sum::<u64>()
                .saturating_mul(super::plan::RETAINED_FACTOR)
        }
    }

    fn edges_retain_properties(&self) -> bool {
        self.edges.iter().any(|source| !source.property_free)
    }

    /// The bytes the node tables need whatever route builds the edges.
    #[must_use]
    pub fn node_tables_resident_bytes(&self) -> u64 {
        FIXED_BYTES
            .saturating_add(self.node_rows().saturating_mul(NODE_TABLE_BYTES))
            .saturating_add(self.retained_node_bytes())
    }

    /// Route the plan for `budget` resident bytes.
    #[must_use]
    pub fn route_for(&self, budget: u64) -> BulkRoute {
        if self.estimated_resident_bytes() <= budget {
            BulkRoute::Memory
        } else if self.node_tables_resident_bytes() > budget {
            BulkRoute::Staged(BulkStagedReason::NodeTablesExceedBudget)
        } else if self.edges_retain_properties() {
            BulkRoute::Staged(BulkStagedReason::EdgePropertiesExceedBudget)
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
}

/// Forces the partition counts of the builds the current test thread runs,
/// until dropped, so a small graph spans several partitions.
#[cfg(test)]
pub(crate) struct ForcedPartitions;

#[cfg(test)]
impl ForcedPartitions {
    pub(crate) fn set(edge: usize, csr: usize) -> Self {
        FORCED_PARTITIONS.with(|forced| forced.set(Some((edge, csr))));
        Self
    }
}

#[cfg(test)]
impl Drop for ForcedPartitions {
    fn drop(&mut self) {
        FORCED_PARTITIONS.with(|forced| forced.set(None));
    }
}

impl ScratchPlan {
    pub(super) fn derive(plan: &BulkBuildPlan<'_>, budget: u64, workers: usize) -> Self {
        let edges = plan.edge_rows();
        let working = budget
            .saturating_sub(plan.node_tables_resident_bytes())
            .max(64 << 20);
        // Three quarters of the working set hold partitions; the rest stages
        // scatter buffers and decoded input.
        let gate_bytes = working / 4 * 3;
        let staging_total = working / 8;
        let ceil = |bytes: u64, per: u64| bytes.div_ceil(per.max(1));
        let mut best = None;
        for concurrency in (1..=workers.max(1) as u64).rev() {
            // Decoding tasks in flight must fit beside the staging buffers.
            if concurrency > 1 && concurrency * DECODE_WINDOW_BYTES > working / 4 {
                continue;
            }
            let per_partition = gate_bytes / (2 * concurrency);
            let edge_partitions =
                ceil(edges.saturating_mul(EDGE_PARTITION_BYTES), per_partition).clamp(1, MAX_PARTITIONS);
            let csr_partitions =
                ceil(edges.saturating_mul(CSR_PARTITION_BYTES), per_partition).clamp(1, MAX_PARTITIONS);
            let widest = edge_partitions.max(2 * csr_partitions);
            let staging = (staging_total / (concurrency * widest)).min(MAX_STAGING_BYTES);
            if staging >= MIN_STAGING_BYTES {
                best = Some((concurrency, edge_partitions, csr_partitions, staging));
                break;
            }
        }
        // Even one worker cannot stage at the minimum: take the smallest
        // buffers and let the memory gate refuse a partition that cannot fit.
        let (concurrency, edge_partitions, csr_partitions, staging) = best.unwrap_or_else(|| {
            let per_partition = gate_bytes / 2;
            (
                1,
                ceil(edges.saturating_mul(EDGE_PARTITION_BYTES), per_partition).clamp(1, MAX_PARTITIONS),
                ceil(edges.saturating_mul(CSR_PARTITION_BYTES), per_partition).clamp(1, MAX_PARTITIONS),
                MIN_STAGING_BYTES,
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

    /// Bytes a partition of `rows` edges reserves while it is built.
    pub(super) fn edge_cost(rows: u64) -> u64 {
        rows.saturating_mul(EDGE_PARTITION_BYTES)
    }

    /// Bytes a CSR partition of `entries` adjacency entries reserves.
    pub(super) fn csr_cost(entries: u64) -> u64 {
        entries.saturating_mul(CSR_PARTITION_BYTES)
    }
}
