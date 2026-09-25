//! Size the sorted runs of a spilling `ORDER BY` (#1591).
//!
//! DataFusion's external sort reserves twice each buffered input batch, then,
//! when the pool is full, sorts every buffered batch and merges them into one
//! spill run. The merge holds one output-sized chunk of every buffered batch
//! plus that chunk's row-format sort keys. When the input batches are exactly
//! the output batch size, each buffered batch *is* one chunk, so the merge must
//! hold all of the buffered data at once plus its row encoding. For two
//! `FixedSizeBinary(16)` keys that is 74 bytes a row against the 64 reserved,
//! and the merge exhausts a pool the sorter has already filled. The 10 MiB
//! `sort_spill_reservation_bytes` headroom covers the excess only while the pool
//! is below about 70 MiB, which is why small budgets never failed.
//!
//! Coalescing the sort's input into runs several chunks long makes each run's
//! reservation cover its merge chunk and row keys, whatever the pool size or
//! partition count. It changes batch boundaries only: rows, their order within
//! a partition, and partitioning are preserved, and sort results are identical.

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::{Stream, StreamExt};

/// Smallest run target. Below this a run is not reliably larger than one
/// merge chunk plus its row keys for the default 8192-row batch.
const MIN_RUN_BYTES: usize = 1024 * 1024;
/// Largest run target. Bounds the concatenation copy and keeps any one
/// variable-width column far below Arrow's 2 GiB `i32` offset limit.
const MAX_RUN_BYTES: usize = 64 * 1024 * 1024;
/// Fraction of each partition's pool share one run may occupy. The sorter
/// reserves twice a run, so it still buffers several runs before spilling.
const RUNS_PER_PARTITION_SHARE: usize = 16;

/// Run target for a query memory budget shared by `target_partitions` sorts.
#[must_use]
pub(crate) fn sort_run_bytes(memory_budget: usize, target_partitions: usize) -> usize {
    (memory_budget / RUNS_PER_PARTITION_SHARE.saturating_mul(target_partitions.max(1)))
        .clamp(MIN_RUN_BYTES, MAX_RUN_BYTES)
}

/// Physical rule placing a [`SortRunCoalesceExec`] under every full sort.
///
/// Top-k sorts (`fetch` set) keep a bounded heap, never spill, and are left
/// alone, as are the ordered fast paths that replace them.
#[derive(Debug)]
pub(crate) struct SortRunCoalesceRule {
    run_bytes: usize,
}

impl SortRunCoalesceRule {
    pub(crate) fn new(run_bytes: usize) -> Self {
        Self { run_bytes }
    }
}

impl PhysicalOptimizerRule for SortRunCoalesceRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| {
            let Some(sort) = node.downcast_ref::<SortExec>() else {
                return Ok(Transformed::no(node));
            };
            let input = Arc::clone(sort.input());
            if sort.fetch().is_some() || input.downcast_ref::<SortRunCoalesceExec>().is_some() {
                return Ok(Transformed::no(node));
            }
            let coalesced: Arc<dyn ExecutionPlan> =
                Arc::new(SortRunCoalesceExec::new(input, self.run_bytes));
            Ok(Transformed::yes(node.with_new_children(vec![coalesced])?))
        })
        .map(|transformed| transformed.data)
    }

    fn name(&self) -> &str {
        "graphforge_sort_run_coalesce"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Concatenates consecutive input batches of one partition into runs of about
/// `run_bytes`, holding the buffered bytes in the query memory pool.
#[derive(Debug)]
pub(crate) struct SortRunCoalesceExec {
    input: Arc<dyn ExecutionPlan>,
    run_bytes: usize,
    props: Arc<PlanProperties>,
}

impl SortRunCoalesceExec {
    fn new(input: Arc<dyn ExecutionPlan>, run_bytes: usize) -> Self {
        let props = Arc::clone(input.properties());
        Self {
            input,
            run_bytes,
            props,
        }
    }
}

impl DisplayAs for SortRunCoalesceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SortRunCoalesceExec: run_bytes={}", self.run_bytes)
    }
}

impl ExecutionPlan for SortRunCoalesceExec {
    fn name(&self) -> &str {
        "SortRunCoalesceExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match (children.pop(), children.is_empty()) {
            (Some(input), true) => Ok(Arc::new(Self::new(input, self.run_bytes))),
            _ => Err(DataFusionError::Internal(
                "SortRunCoalesceExec requires exactly one child".into(),
            )),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, Arc::clone(&context))?;
        let reservation = MemoryConsumer::new(format!("SortRunCoalesce[{partition}]"))
            .register(context.memory_pool());
        Ok(Box::pin(SortRunCoalesceStream {
            schema: self.input.schema(),
            input: Some(input),
            run_bytes: self.run_bytes,
            buffered: Vec::new(),
            reservation,
        }))
    }
}

struct SortRunCoalesceStream {
    schema: SchemaRef,
    input: Option<SendableRecordBatchStream>,
    run_bytes: usize,
    buffered: Vec<RecordBatch>,
    reservation: MemoryReservation,
}

impl SortRunCoalesceStream {
    fn flush(&mut self) -> Option<Result<RecordBatch>> {
        let run = match self.buffered.len() {
            0 => return None,
            1 => Ok(self.buffered.pop().expect("one buffered batch")),
            _ => concat_batches(&self.schema, &self.buffered)
                .map_err(|error| DataFusionError::ArrowError(Box::new(error), None)),
        };
        self.buffered.clear();
        // The sorter reserves the run itself once it receives it.
        self.reservation.free();
        Some(run)
    }
}

impl Stream for SortRunCoalesceStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let Some(input) = self.input.as_mut() else {
                return Poll::Ready(None);
            };
            match input.poll_next_unpin(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Err(error))) => {
                    self.input = None;
                    self.buffered.clear();
                    self.reservation.free();
                    return Poll::Ready(Some(Err(error)));
                }
                Poll::Ready(Some(Ok(batch))) => {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    let size = batch.get_array_memory_size();
                    // A full pool ends the run early instead of failing: the
                    // sorter downstream is the operator that can spill.
                    let accounted = self.reservation.try_grow(size).is_ok();
                    self.buffered.push(batch);
                    if !accounted || self.reservation.size() >= self.run_bytes {
                        return Poll::Ready(self.flush());
                    }
                }
                Poll::Ready(None) => {
                    self.input = None;
                    return Poll::Ready(self.flush());
                }
            }
        }
    }
}

impl RecordBatchStream for SortRunCoalesceStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
