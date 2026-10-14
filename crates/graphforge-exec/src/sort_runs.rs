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

use arrow::array::{Array, AsArray, make_array};
use arrow::compute::concat_batches;
use arrow::datatypes::DataType;
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
    force_sort: bool,
    props: Arc<PlanProperties>,
}

impl SortRunCoalesceExec {
    pub(crate) fn new(input: Arc<dyn ExecutionPlan>, run_bytes: usize) -> Self {
        Self::with_force_sort(input, run_bytes, false)
    }

    /// Hide input ordering when the following sort must drain its input before
    /// yielding. SortExec otherwise bypasses materialization at execution time.
    pub(crate) fn with_force_sort(
        input: Arc<dyn ExecutionPlan>,
        run_bytes: usize,
        force_sort: bool,
    ) -> Self {
        let props = if force_sort {
            // Constants and equivalence proofs can also satisfy an ordering.
            // Publish no ordering proof when runtime materialization is required.
            let equivalence = datafusion::physical_expr::EquivalenceProperties::new(input.schema());
            Arc::new(
                input
                    .properties()
                    .as_ref()
                    .clone()
                    .with_eq_properties(equivalence),
            )
        } else {
            Arc::clone(input.properties())
        };
        Self {
            input,
            run_bytes,
            force_sort,
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
            (Some(input), true) => Ok(Arc::new(Self::with_force_sort(
                input,
                self.run_bytes,
                self.force_sort,
            ))),
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
            buffered_bytes: 0,
            pending: None,
            reservation,
        }))
    }
}

struct SortRunCoalesceStream {
    schema: SchemaRef,
    input: Option<SendableRecordBatchStream>,
    run_bytes: usize,
    buffered: Vec<RecordBatch>,
    buffered_bytes: usize,
    // Slices share the source allocation. Keep and charge that allocation until
    // the last compact window has been emitted; never give those slices to a sort.
    pending: Option<(RecordBatch, usize)>,
    reservation: MemoryReservation,
}

impl SortRunCoalesceStream {
    fn fail(&mut self, error: DataFusionError) -> Poll<Option<Result<RecordBatch>>> {
        self.input = None;
        self.pending = None;
        self.buffered.clear();
        self.buffered_bytes = 0;
        self.reservation.free();
        Poll::Ready(Some(Err(error)))
    }

    fn flush(&mut self) -> Result<RecordBatch> {
        let copied = if self.buffered.len() > 1 {
            // The source batches coexist with their concatenation. Admit the
            // extra copy before allocating it, including allocation rounding.
            let copied = self
                .buffered_bytes
                .saturating_add(self.schema.fields().len() * 512);
            self.reservation.try_grow(copied)?;
            copied
        } else {
            0
        };
        let run = match self.buffered.len() {
            1 => Ok(self.buffered.pop().expect("one buffered batch")),
            _ => concat_batches(&self.schema, &self.buffered)
                .map_err(|error| DataFusionError::ArrowError(Box::new(error), None)),
        };
        self.buffered.clear();
        self.reservation.shrink(self.buffered_bytes + copied);
        self.buffered_bytes = 0;
        run
    }

    fn compact_window(&mut self) -> Result<RecordBatch> {
        let (source, offset) = self.pending.as_ref().expect("pending source");
        let remaining = source.num_rows() - offset;
        let mut low = 1;
        let mut high = remaining;
        // The copy allowance covers buffer rounding and array metadata. A
        // single wide row is indivisible and is admitted against the pool.
        while low < high {
            let middle = low + (high - low).div_ceil(2);
            if compact_charge(&source.slice(*offset, middle)) <= self.run_bytes {
                low = middle;
            } else {
                high = middle - 1;
            }
        }
        let window = source.slice(*offset, low);
        let admitted = compact_charge(&window);
        self.reservation.try_grow(admitted)?;
        // Arrow concatenation with one batch returns a sharing slice. The empty
        // second batch forces an actual copy with only the selected values.
        let empty = RecordBatch::new_empty(Arc::clone(&self.schema));
        let copied = concat_batches(&self.schema, [&window, &empty])
            .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
        let columns = copied
            .columns()
            .iter()
            .map(|array| compact_views(array.to_data()).map(make_array))
            .collect::<Result<Vec<_>>>()?;
        let compact = RecordBatch::try_new(Arc::clone(&self.schema), columns)?;
        drop(copied);
        drop(window);
        let actual = compact.get_array_memory_size();
        if actual > admitted {
            self.reservation.try_grow(actual - admitted)?;
        } else {
            self.reservation.shrink(admitted - actual);
        }
        if low == remaining {
            let (source, _) = self.pending.take().expect("pending source");
            let held = source.get_array_memory_size();
            drop(source);
            self.reservation.shrink(held);
        } else {
            self.pending.as_mut().expect("pending source").1 += low;
        }
        // Ownership transfers to the downstream sorter, which reserves this run.
        self.reservation.shrink(actual);
        Ok(compact)
    }
}

/// Conservative compact-copy admission. Nested/dictionary children can retain
/// more than a row window, so Arrow's slice estimate deliberately overcounts
/// them; the retained size is reconciled after allocation. This bounds copied
/// variable-width values rather than counting their original shared buffers.
fn compact_charge(batch: &RecordBatch) -> usize {
    batch.columns().iter().fold(0usize, |bytes, array| {
        let data = array.to_data();
        let metadata = data
            .get_array_memory_size()
            .saturating_sub(data.get_buffer_memory_size());
        let selected = match array.data_type() {
            DataType::Utf8View => array
                .as_string_view()
                .iter()
                .map(|value| value.map_or(0, str::len))
                .sum::<usize>()
                .saturating_add(array.len() * 17),
            DataType::BinaryView => array
                .as_binary_view()
                .iter()
                .map(|value| value.map_or(0, <[u8]>::len))
                .sum::<usize>()
                .saturating_add(array.len() * 17),
            _ => data
                .get_slice_memory_size()
                .unwrap_or_else(|_| data.get_buffer_memory_size()),
        };
        bytes
            .saturating_add(selected.saturating_mul(2))
            .saturating_add(metadata)
            .saturating_add(512)
    })
}

// Byte-view concatenation shares its backing buffers. Collect only the views
// this window references, including when they are nested inside another array.
fn compact_views(data: arrow::array::ArrayData) -> Result<arrow::array::ArrayData> {
    match data.data_type() {
        DataType::Utf8View => Ok(make_array(data).as_string_view().gc().to_data()),
        DataType::BinaryView => Ok(make_array(data).as_binary_view().gc().to_data()),
        _ if data.child_data().is_empty() => Ok(data),
        _ => {
            let children = data
                .child_data()
                .iter()
                .cloned()
                .map(compact_views)
                .collect::<Result<Vec<_>>>()?;
            Ok(data.into_builder().child_data(children).build()?)
        }
    }
}

impl Stream for SortRunCoalesceStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if self.pending.is_some() {
                return match self.compact_window() {
                    Ok(run) => Poll::Ready(Some(Ok(run))),
                    Err(error) => self.fail(error),
                };
            }
            let Some(input) = self.input.as_mut() else {
                return Poll::Ready(None);
            };
            match input.poll_next_unpin(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Err(error))) => return self.fail(error),
                Poll::Ready(Some(Ok(batch))) => {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    let size = batch.get_array_memory_size();
                    if let Err(error) = self.reservation.try_grow(size) {
                        return self.fail(error);
                    }
                    if size > self.run_bytes && batch.num_rows() > 1 {
                        self.pending = Some((batch, 0));
                        if !self.buffered.is_empty() {
                            return match self.flush() {
                                Ok(run) => Poll::Ready(Some(Ok(run))),
                                Err(error) => self.fail(error),
                            };
                        }
                        continue;
                    }
                    self.buffered_bytes += size;
                    self.buffered.push(batch);
                    if self.buffered_bytes >= self.run_bytes {
                        return match self.flush() {
                            Ok(run) => Poll::Ready(Some(Ok(run))),
                            Err(error) => self.fail(error),
                        };
                    }
                }
                Poll::Ready(None) => {
                    self.input = None;
                    return if self.buffered.is_empty() {
                        Poll::Ready(None)
                    } else {
                        match self.flush() {
                            Ok(run) => Poll::Ready(Some(Ok(run))),
                            Err(error) => self.fail(error),
                        }
                    };
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

#[cfg(test)]
mod tests;
