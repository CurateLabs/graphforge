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
        // The copy allowance covers buffer rounding and array metadata. A
        // single wide row is indivisible and is admitted against the pool.
        let low = compact_window_rows(remaining, self.run_bytes, |rows| {
            compact_charge(&source.slice(*offset, rows))
        });
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

fn compact_window_rows(
    remaining: usize,
    run_bytes: usize,
    mut charge: impl FnMut(usize) -> usize,
) -> usize {
    let mut low = 1;
    let mut high = 1;
    // View sizing walks every selected value. Grow a local bound so sizing a
    // small run never starts by scanning half of the entire remaining batch.
    while high < remaining && charge(high) <= run_bytes {
        low = high;
        high = high.saturating_mul(2).min(remaining);
    }
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        if charge(middle) <= run_bytes {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    low
}

/// Bound the existing two-input concat plus nested view GC, while the entire
/// source remains independently admitted. Overflow cannot establish a bound,
/// so request the maximum charge and let the pool refuse before copying.
fn compact_charge(batch: &RecordBatch) -> usize {
    batch
        .columns()
        .iter()
        .try_fold(0usize, |bytes, array| {
            bytes.checked_add(compact_array_charge(&array.to_data(), false)?)
        })
        // Concat and GC outputs coexist. Retain the existing double-copy
        // allowance, now with rounding and metadata at every recursive node.
        .and_then(|bytes| bytes.checked_mul(2))
        .unwrap_or(usize::MAX)
}

fn compact_allocation(bytes: usize) -> Option<usize> {
    bytes.checked_add(63).map(|bytes| bytes / 64 * 64)
}

fn compact_offset_span(data: &arrow::array::ArrayData) -> Option<(usize, usize)> {
    let last = data.offset().checked_add(data.len())?;
    let (first, last) = match data.data_type() {
        DataType::Utf8 | DataType::Binary | DataType::List(_) => {
            let offsets = data.buffers()[0].typed_data::<i32>();
            (i64::from(offsets[data.offset()]), i64::from(offsets[last]))
        }
        _ => {
            let offsets = data.buffers()[0].typed_data::<i64>();
            (offsets[data.offset()], offsets[last])
        }
    };
    let first = usize::try_from(first).ok()?;
    let last = usize::try_from(last).ok()?;
    Some((first, last.checked_sub(first)?))
}

// View concat carries backing handles; GC copies every raw referenced span.
fn compact_view_charge(data: &arrow::array::ArrayData) -> Option<usize> {
    let rows = data.len();
    let views = data.buffers()[0].typed_data::<u128>();
    let mut payload = 0usize;
    let mut blocks = 0usize;
    for view in &views[data.offset()..data.offset().checked_add(rows)?] {
        let length = usize::try_from(arrow::array::ByteView::from(*view).length).ok()?;
        // GC visits raw views, including payload on logically NULL rows.
        if length > 12 {
            // Rounding each span also bounds GC splitting its payload
            // across multiple separately allocated blocks.
            payload = payload.checked_add(compact_allocation(length)?)?;
            blocks = blocks.checked_add(1)?;
        }
    }
    let mut bytes = compact_allocation(rows.checked_mul(16)?)?;
    bytes = bytes.checked_add(compact_allocation(payload)?)?;
    // append_array carries ALL backing handles, even unused ones. Bound
    // Vec growth/conversion and GC's handles/groups (at most one per raw
    // noninline view). Payload backings already belong to source admission.
    let handles = data.buffers().len().checked_sub(1)?.max(4);
    bytes = bytes.checked_add(compact_allocation(
        handles
            .checked_add(blocks)?
            .checked_mul(4 * size_of::<arrow_buffer::Buffer>())?,
    )?)?;
    Some(bytes)
}

fn compact_array_charge(data: &arrow::array::ArrayData, retained: bool) -> Option<usize> {
    let rows = data.len();
    // Apply the existing metadata allowance recursively, including empty
    // children. Each buffer/bitmap allocation is rounded separately to 64 bytes.
    let mut validity = rows.checked_add(7)? / 8;
    if retained && let Some(nulls) = data.nulls() {
        validity = validity.max(nulls.buffer().capacity());
    }
    let mut bytes = 512usize.checked_add(compact_allocation(validity)?)?;
    let mut children_retained = retained;
    if matches!(data.data_type(), DataType::Utf8View | DataType::BinaryView) {
        return bytes.checked_add(compact_view_charge(data)?);
    }
    if !retained && let Some(width) = data.data_type().primitive_width() {
        return bytes.checked_add(compact_allocation(rows.checked_mul(width)?)?);
    }
    match data.data_type() {
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::List(_)
        | DataType::LargeList(_)
            if !retained =>
        {
            let (first, length) = compact_offset_span(data)?;
            let width = if matches!(
                data.data_type(),
                DataType::LargeUtf8 | DataType::LargeBinary | DataType::LargeList(_)
            ) {
                8
            } else {
                4
            };
            bytes = bytes.checked_add(compact_allocation(
                rows.checked_add(1)?.checked_mul(width)?,
            )?)?;
            if matches!(data.data_type(), DataType::List(_) | DataType::LargeList(_)) {
                // concat_lists slices [first,last], including populated spans
                // beneath NULL lists. Struct's ArrayData already has sliced
                // fields; do not suppress children beneath NULL Struct rows.
                return bytes.checked_add(compact_array_charge(
                    &data.child_data()[0].slice(first, length),
                    false,
                )?);
            }
            return bytes.checked_add(compact_allocation(length)?);
        }
        DataType::FixedSizeList(_, width) if !retained => {
            let width = usize::try_from(*width).ok()?;
            return bytes.checked_add(compact_array_charge(
                &data.child_data()[0]
                    .slice(data.offset().checked_mul(width)?, rows.checked_mul(width)?),
                false,
            )?);
        }
        DataType::Dictionary(keys, _) if !retained => {
            bytes = bytes.checked_add(compact_allocation(
                rows.checked_mul(keys.primitive_width()?)?,
            )?)?;
            children_retained = true;
        }
        DataType::ListView(_) | DataType::LargeListView(_) if !retained => {
            let width = if matches!(data.data_type(), DataType::LargeListView(_)) {
                8
            } else {
                4
            };
            bytes =
                bytes.checked_add(compact_allocation(rows.checked_mul(width)?)?.checked_mul(2)?)?;
            children_retained = true;
        }
        DataType::Map(_, _) if !retained => {
            bytes = bytes.checked_add(compact_allocation(rows.checked_add(1)?.checked_mul(4)?)?)?;
            children_retained = true;
        }
        DataType::Union(_, mode) if !retained => {
            bytes = bytes.checked_add(compact_allocation(rows)?)?;
            if mode == &arrow::datatypes::UnionMode::Dense {
                bytes = bytes.checked_add(compact_allocation(rows.checked_mul(4)?)?)?;
            }
            children_retained = true;
        }
        DataType::Struct(_) | DataType::Null if !retained => {}
        DataType::Boolean if !retained => {
            bytes = bytes.checked_add(compact_allocation(rows.checked_add(7)? / 8)?)?;
        }
        DataType::FixedSizeBinary(width) if !retained => {
            bytes = bytes.checked_add(compact_allocation(
                rows.checked_mul(usize::try_from(*width).ok()?)?,
            )?)?;
        }
        _ => {
            // Dictionary concat can process the whole dictionary; ListView
            // concat copies its entire values(), unlike ordinary List. Keep
            // their full retained-child bound, also for the other concat paths.
            for buffer in data.buffers() {
                bytes = bytes.checked_add(compact_allocation(buffer.capacity())?)?;
            }
            children_retained = true;
        }
    }
    for child in data.child_data() {
        bytes = bytes.checked_add(compact_array_charge(child, children_retained)?)?;
    }
    Some(bytes)
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
