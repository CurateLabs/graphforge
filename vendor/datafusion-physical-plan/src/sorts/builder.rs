// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::copy_admission::{
    CopyAdmission, CopyCapability, Phase, Resident, SourceOwners,
};
use crate::spill::admission::{add, mul};
use crate::spill::get_record_batch_memory_size;
use crate::terminal_ownership as od;
use arrow::array::ArrayRef;
use arrow::compute::interleave;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::memory_pool::MemoryReservation;
use log::warn;
use std::sync::Arc;

#[derive(Debug, Copy, Clone, Default)]
struct BatchCursor {
    /// The index into BatchBuilder::batches
    batch_idx: usize,
    /// The row index within the given batch
    row_idx: usize,
}

#[derive(Debug)]
#[allow(dead_code)]
pub(super) struct ChargedBufferedBatch {
    stream_idx: usize,
    pub(super) batch: RecordBatch,
    // Cache the existing admission measurement. Dropping a consumed input must
    // not allocate ArrayData trees or a HashSet just to measure it again.
    memory_size: usize,
    // Reused while compacting Copy inputs; covered by BufferedBatch capacity.
    copy_remap: usize,
}

#[derive(Debug)]
pub(super) struct BufferedBatch {
    stream_idx: usize,
    pub(super) batch: RecordBatch,
    // Cache the existing admission measurement. Dropping a consumed input must
    // not allocate ArrayData trees or a HashSet just to measure it again.
    memory_size: usize,
    // Reused while compacting Copy inputs; covered by BufferedBatch capacity.
    copy_remap: usize,
    diagnostic: od::Life,
}

/// Provides an API to incrementally build a [`RecordBatch`] from partitioned [`RecordBatch`]
#[derive(Debug)]
#[allow(dead_code)]
pub(super) struct ChargedBatchBuilder {
    /// The schema of the RecordBatches yielded by this stream
    schema: SchemaRef,

    /// Maintain a list of [`RecordBatch`] and their corresponding stream
    batches: Vec<ChargedBufferedBatch>,

    /// Accounts for memory used by buffered batches.
    ///
    /// May include pre-reserved bytes (from `sort_spill_reservation_bytes`)
    /// that were transferred via [`MemoryReservation::take()`] to prevent
    /// starvation when concurrent sort partitions compete for pool memory.
    reservation: MemoryReservation,

    /// Tracks the actual memory used by buffered batches (not including
    /// pre-reserved bytes). This allows [`Self::push_batch`] to skip pool
    /// allocation requests when the pre-reserved bytes cover the batch.
    batches_mem_used: usize,

    /// The initial reservation size at construction time. When the reservation
    /// is pre-loaded with `sort_spill_reservation_bytes` (via `take()`), this
    /// records that amount so we never shrink below it, maintaining the
    /// anti-starvation guarantee throughout the merge.
    initial_reservation: usize,

    /// The current [`BatchCursor`] for each stream
    cursors: Vec<BatchCursor>,

    /// The accumulated stream indexes from which to pull rows
    /// Consists of a tuple of `(batch_idx, row_idx)`
    indices: Vec<(usize, usize)>,
    copy: Option<Arc<CopyAdmission>>,
    copy_resident: Option<Arc<Resident>>,
    failed_phase: Option<super::copy_admission::ChargedPhase>,
}

#[derive(Debug)]
pub struct BatchBuilder {
    /// The schema of the RecordBatches yielded by this stream
    schema: SchemaRef,

    /// Maintain a list of [`RecordBatch`] and their corresponding stream
    batches: Vec<BufferedBatch>,

    /// Accounts for memory used by buffered batches.
    ///
    /// May include pre-reserved bytes (from `sort_spill_reservation_bytes`)
    /// that were transferred via [`MemoryReservation::take()`] to prevent
    /// starvation when concurrent sort partitions compete for pool memory.
    reservation: MemoryReservation,

    /// Tracks the actual memory used by buffered batches (not including
    /// pre-reserved bytes). This allows [`Self::push_batch`] to skip pool
    /// allocation requests when the pre-reserved bytes cover the batch.
    batches_mem_used: usize,

    /// The initial reservation size at construction time. When the reservation
    /// is pre-loaded with `sort_spill_reservation_bytes` (via `take()`), this
    /// records that amount so we never shrink below it, maintaining the
    /// anti-starvation guarantee throughout the merge.
    initial_reservation: usize,

    /// The current [`BatchCursor`] for each stream
    cursors: Vec<BatchCursor>,

    /// The accumulated stream indexes from which to pull rows
    /// Consists of a tuple of `(batch_idx, row_idx)`
    indices: Vec<(usize, usize)>,
    copy: Option<Arc<CopyAdmission>>,
    copy_resident: Option<Arc<Resident>>,
    failed_phase: Option<Phase>,
    diagnostic_piece: od::Piece,
    diagnostic: od::Life,
}

impl BatchBuilder {
    pub(super) fn constructor_bound(streams: usize, batch_size: usize) -> Result<usize> {
        add(
            add(size_of::<ChargedBatchBuilder>(), 512)?,
            add(
                mul(mul(streams, 2)?, size_of::<ChargedBufferedBatch>())?,
                add(
                    mul(streams, size_of::<BatchCursor>())?,
                    mul(batch_size, size_of::<(usize, usize)>())?,
                )?,
            )?,
        )
    }
    /// Create a new [`BatchBuilder`] with the provided `stream_count` and `batch_size`
    pub fn new(
        schema: SchemaRef,
        stream_count: usize,
        batch_size: usize,
        reservation: MemoryReservation,
    ) -> Self {
        let initial_reservation = reservation.size();
        let diagnostic = od::Life::inherited(od::Kind::Builder);
        let diagnostic_piece = od::Piece::new(&reservation, diagnostic.id());
        diagnostic.update(|o| {
            o.piece = diagnostic_piece.id();
            o.held = initial_reservation as u64;
            o.flags = 4;
        });
        Self {
            diagnostic,
            diagnostic_piece,
            schema,
            batches: Vec::with_capacity(stream_count * 2),
            cursors: vec![BatchCursor::default(); stream_count],
            indices: Vec::with_capacity(batch_size),
            reservation,
            batches_mem_used: 0,
            initial_reservation,
            copy: None,
            copy_resident: None,
            failed_phase: None,
        }
    }

    /// The merge has built its last output.
    pub(super) fn finish(&self) {
        if let Some(copy) = &self.copy {
            copy.release_working_set();
        }
    }

    pub(super) fn attach_copy(
        &mut self,
        copy: Arc<CopyAdmission>,
        phase: &Phase,
    ) -> Result<()> {
        self.copy_resident = Some(phase.resident(self.metadata()?)?);
        self.diagnostic.edge(
            self.copy_resident.as_ref().unwrap().diagnostic_id(),
            2,
            self.metadata()?,
        );
        self.observe();
        self.copy = Some(copy);
        Ok(())
    }

    fn metadata(&self) -> Result<usize> {
        add(
            size_of::<ChargedBatchBuilder>(),
            add(
                mul(self.batches.capacity(), size_of::<ChargedBufferedBatch>())?,
                add(
                    mul(self.cursors.capacity(), size_of::<BatchCursor>())?,
                    mul(self.indices.capacity(), size_of::<(usize, usize)>())?,
                )?,
            )?,
        )
    }

    pub(super) fn observe(&self) {
        self.diagnostic_piece
            .refresh(&self.reservation, self.diagnostic.id());
        self.diagnostic.update(|o| {
            o.held = self.reservation.size() as u64;
            o.bytes = self.metadata().unwrap_or(usize::MAX) as u64;
            o.a = self.batches.len() as u64;
            o.b = self.batches.capacity() as u64;
            o.c = self.indices.len() as u64;
            o.d = self.indices.capacity() as u64;
        });
    }
    /// Append a new batch in `stream_idx`
    pub fn push_batch(&mut self, stream_idx: usize, batch: RecordBatch) -> Result<()> {
        let size = get_record_batch_memory_size(&batch);
        self.batches_mem_used += size;
        // Only request additional memory from the pool when actual batch
        // usage exceeds the current reservation (which may include
        // pre-reserved bytes from sort_spill_reservation_bytes).
        try_grow_reservation_to_at_least(&mut self.reservation, self.batches_mem_used)?;
        let batch_idx = self.batches.len();
        let growing = batch_idx == self.batches.capacity();
        let mut phase = if growing {
            self.copy
                .as_ref()
                .map(|copy| {
                    // Old capacity remains resident while the complete new Vec
                    // allocation can coexist with it during realloc.
                    let capacity = mul(self.batches.capacity(), 2)?.max(4);
                    copy.begin_operation(Some(mul(
                        capacity,
                        size_of::<ChargedBufferedBatch>(),
                    )?))
                })
                .transpose()?
        } else {
            None
        };
        let handed = od::take_handoff();
        let diagnostic = if handed.id() != 0 {
            handed
        } else {
            od::Life::new(
                od::Kind::Batch,
                self.diagnostic.id(),
                self.diagnostic_piece.id(),
            )
        };
        diagnostic.update(|o| {
            o.owner_kind = od::Kind::Batch as u64;
            o.parent = self.diagnostic.id();
            o.piece = self.diagnostic_piece.id();
            o.c = 0;
            o.d = 0;
        });
        diagnostic.batch(&batch);
        diagnostic.update(|o| {
            o.a = batch.num_rows() as u64;
            o.b = batch.num_columns() as u64;
            o.bytes = size as u64;
            o.held = self.reservation.size() as u64;
        });
        self.diagnostic_piece
            .refresh(&self.reservation, self.diagnostic.id());
        self.batches.push(BufferedBatch {
            diagnostic,
            stream_idx,
            batch,
            memory_size: size,
            copy_remap: usize::MAX,
        });
        if let Some(active) = &phase {
            let result = self.metadata().and_then(|metadata| {
                active.resize(self.copy_resident.as_ref().unwrap(), metadata)
            });
            if let Err(error) = result {
                self.failed_phase = phase.take();
                return Err(error);
            }
        }
        self.cursors[stream_idx] = BatchCursor {
            batch_idx,
            row_idx: 0,
        };
        self.observe();
        Ok(())
    }

    /// Append the next row from `stream_idx`
    pub fn push_row(&mut self, stream_idx: usize) {
        let cursor = &mut self.cursors[stream_idx];
        let row_idx = cursor.row_idx;
        cursor.row_idx += 1;
        self.indices.push((cursor.batch_idx, row_idx));
        self.observe();
    }

    /// Returns the number of in-progress rows in this [`BatchBuilder`]
    pub fn len(&self) -> usize {
        self.indices.len()
    }

    /// Returns `true` if this [`BatchBuilder`] contains no in-progress rows
    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    /// Whether a batch of `stream_idx` earlier than its current one is still
    /// held: one is kept only while rows pending in the output refer to it,
    /// and every batch is pruned to the stream's current one when the output
    /// is built, so this is whether the stream was refilled since then.
    pub(super) fn holds_an_earlier_batch(&self, stream_idx: usize) -> bool {
        self.batches
            .iter()
            .filter(|input| input.stream_idx == stream_idx)
            .count()
            >= 2
    }

    /// Whether any batch of `stream_idx` is still held.
    pub(super) fn holds_a_batch_of(&self, stream_idx: usize) -> bool {
        self.batches
            .iter()
            .any(|input| input.stream_idx == stream_idx)
    }

    /// Returns the schema of this [`BatchBuilder`]
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub(super) fn uses_row_copy(&self) -> bool {
        self.copy
            .as_ref()
            .is_some_and(|copy| matches!(copy.capability(), CopyCapability::Row { .. }))
    }

    /// Release only sealed Copy inputs with no future rows or pending indices.
    /// Mark pending inputs, calculate their destinations, then remap indices
    /// before compacting. Each pass is linear in batches or pending indices;
    /// the per-batch scratch slot is already covered by admitted Vec capacity.
    /// No ledger or Arrow metadata is constructed during cleanup.
    pub(super) fn release_consumed_inputs(&mut self) {
        if self.copy.is_none() {
            return;
        }
        for input in &mut self.batches {
            input.copy_remap = usize::MAX;
        }
        for &(batch, _) in &self.indices {
            self.batches[batch].copy_remap = 0;
        }
        let mut new_idx = 0;
        for (old_idx, input) in self.batches.iter_mut().enumerate() {
            let cursor = &mut self.cursors[input.stream_idx];
            let current = cursor.batch_idx == old_idx;
            let future = current && cursor.row_idx < input.batch.num_rows();
            let pending = input.copy_remap != usize::MAX;
            let retain = future || pending;
            if retain {
                input.copy_remap = new_idx;
                if current {
                    cursor.batch_idx = new_idx;
                }
                new_idx += 1;
            } else {
                self.batches_mem_used -= input.memory_size;
                if current {
                    // No future row can use this cursor. push_batch replaces
                    // it before push_row can select this stream again.
                    cursor.batch_idx = usize::MAX;
                }
            }
        }
        // Old slots still exist here, including those whose data will drop.
        // Every pending index marked its source, so its destination is live.
        for (batch, _) in &mut self.indices {
            *batch = self.batches[*batch].copy_remap;
        }
        self.batches.retain(|input| input.copy_remap != usize::MAX);
    }

    /// Try to interleave all columns using the given index slice.
    fn try_interleave_columns(
        &self,
        indices: &[(usize, usize)],
    ) -> Result<Vec<ArrayRef>> {
        let plan = if self
            .copy
            .as_ref()
            .is_some_and(|copy| copy.has_output_bounds())
        {
            super::copy_bounds::serial_interleave(&self.batches, indices)?
                .map(|payload| SourceOwners::plan(&self.batches, &self.schema, &payload))
                .transpose()?
                .flatten()
        } else {
            None
        };
        let phase = self
            .copy
            .as_ref()
            .map(|copy| {
                let workspace = if let Some(plan) = &plan {
                    Some(plan.workspace()?)
                } else if copy.has_output_bounds() {
                    super::copy_bounds::interleave(&self.batches, indices)?
                } else {
                    None
                };
                copy.begin_interleave(workspace)
            })
            .transpose()?;
        let source = phase
            .as_ref()
            .map(|phase| {
                if let Some(plan) = &plan {
                    SourceOwners::new_planned(&self.batches, plan)
                } else {
                    SourceOwners::new(
                        phase,
                        self.batches.iter().map(|input| &input.batch),
                    )
                }
            })
            .transpose()?;
        let columns = (0..self.schema.fields.len())
            .map(|column_idx| {
                let arrays: Vec<_> = self
                    .batches
                    .iter()
                    .map(|input| input.batch.column(column_idx).as_ref())
                    .collect();
                // Arrow 58.1.0+ returns OffsetOverflowError directly from
                // interleave, allowing retry_interleave to shrink the batch.
                interleave(&arrays, indices).map_err(Into::into)
            })
            .collect::<Result<Vec<_>>>()?;
        if let (Some(phase), Some(source)) = (&phase, &source) {
            if let Some(plan) = &plan {
                source.retain_planned(phase, &columns, plan)?;
            } else {
                source.retain_fresh(phase, &self.schema, &columns)?;
            }
        }
        Ok(columns)
    }

    /// Builds a record batch from the first `rows_to_emit` buffered rows.
    fn finish_record_batch(
        &mut self,
        rows_to_emit: usize,
        columns: Vec<ArrayRef>,
    ) -> Result<RecordBatch> {
        // Remove consumed indices, keeping any remaining for the next call.
        self.indices.drain(..rows_to_emit);

        if self.copy.is_some() {
            // Output claims are installed before indices are drained. Actual
            // source aliases keep their own tokens after this reference drops.
            self.release_consumed_inputs();
        } else if self.indices.is_empty() {
            // Legacy inputs retain their original whole-batch accounting and
            // prune only once no pending index can reference an earlier batch.
            // New cursors are only created once the previous cursor for the stream
            // is finished. This means all remaining rows from all but the last batch
            // for each stream have been yielded to the newly created record batch
            //
            // We can therefore drop all but the last batch for each stream
            let mut batch_idx = 0;
            let mut retained = 0;
            self.batches.retain(|input| {
                let stream_cursor = &mut self.cursors[input.stream_idx];
                let retain = stream_cursor.batch_idx == batch_idx;
                batch_idx += 1;

                if retain {
                    stream_cursor.batch_idx = retained;
                    retained += 1;
                } else {
                    self.batches_mem_used -= input.memory_size;
                }
                retain
            });
        }

        // Release excess memory back to the pool, but never shrink below
        // initial_reservation to maintain the anti-starvation guarantee
        // for the merge phase.
        let target = self.batches_mem_used.max(self.initial_reservation);
        if self.reservation.size() > target {
            self.reservation.shrink(self.reservation.size() - target);
        }

        self.observe();
        RecordBatch::try_new(Arc::clone(&self.schema), columns).map_err(Into::into)
    }

    /// Drains the in_progress row indexes, and builds a new RecordBatch from them
    ///
    /// Will then drop any batches for which all rows have been yielded to the output.
    /// If an offset overflow occurs (e.g. string/list offsets exceed i32::MAX),
    /// retries with progressively fewer rows until it succeeds.
    ///
    /// Returns `None` if no pending rows
    pub fn build_record_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.is_empty() {
            return Ok(None);
        }

        let (rows_to_emit, columns) =
            retry_interleave(self.indices.len(), self.indices.len(), |rows_to_emit| {
                self.try_interleave_columns(&self.indices[..rows_to_emit])
            })?;

        Ok(Some(self.finish_record_batch(rows_to_emit, columns)?))
    }
}

/// Try to grow `reservation` so it covers at least `needed` bytes.
///
/// When a reservation has been pre-loaded with bytes (e.g. via
/// [`MemoryReservation::take()`]), this avoids redundant pool
/// allocations: if the reservation already covers `needed`, this is
/// a no-op; otherwise only the deficit is requested from the pool.
pub(crate) fn try_grow_reservation_to_at_least(
    reservation: &mut MemoryReservation,
    needed: usize,
) -> Result<()> {
    if needed > reservation.size() {
        reservation.try_grow(needed - reservation.size())?;
    }
    Ok(())
}

/// Returns true if the error is an Arrow offset overflow.
fn is_offset_overflow(e: &DataFusionError) -> bool {
    matches!(
        e,
        DataFusionError::ArrowError(boxed, _)
            if matches!(boxed.as_ref(), ArrowError::OffsetOverflowError(_))
    )
}

#[cfg(test)]
fn offset_overflow_error() -> DataFusionError {
    DataFusionError::ArrowError(Box::new(ArrowError::OffsetOverflowError(0)), None)
}

fn retry_interleave<T, F>(
    mut rows_to_emit: usize,
    total_rows: usize,
    mut interleave: F,
) -> Result<(usize, T)>
where
    F: FnMut(usize) -> Result<T>,
{
    loop {
        match interleave(rows_to_emit) {
            Ok(value) => return Ok((rows_to_emit, value)),
            // Only offset overflow is recoverable by emitting fewer rows.
            Err(e) if is_offset_overflow(&e) => {
                rows_to_emit /= 2;
                if rows_to_emit == 0 {
                    return Err(e);
                }
                warn!(
                    "Interleave offset overflow with {total_rows} rows, retrying with {rows_to_emit}"
                );
            }
            Err(e) => return Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, ArrayDataBuilder, Int32Array, ListArray};
    use arrow::buffer::Buffer;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_execution::memory_pool::{
        MemoryConsumer, MemoryPool, UnboundedMemoryPool,
    };

    fn overflow_list_batch() -> RecordBatch {
        let values_field = Arc::new(Field::new_list_field(DataType::Int32, true));
        // SAFETY: This intentionally constructs an invalid child length so
        // Arrow's interleave hits offset overflow before touching child data.
        let list = ListArray::from(unsafe {
            ArrayDataBuilder::new(DataType::List(Arc::clone(&values_field)))
                .len(1)
                .add_buffer(Buffer::from_slice_ref([0_i32, i32::MAX]))
                .add_child_data(Int32Array::from(Vec::<i32>::new()).to_data())
                .build_unchecked()
        });
        let schema = Arc::new(Schema::new(vec![Field::new(
            "list_col",
            DataType::List(values_field),
            true,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(list)]).unwrap()
    }

    #[test]
    fn test_retry_interleave_halves_rows_until_success() {
        let mut attempts = Vec::new();

        let (rows_to_emit, result) = retry_interleave(4, 4, |rows_to_emit| {
            attempts.push(rows_to_emit);
            if rows_to_emit > 1 {
                Err(offset_overflow_error())
            } else {
                Ok("ok")
            }
        })
        .unwrap();

        assert_eq!(rows_to_emit, 1);
        assert_eq!(result, "ok");
        assert_eq!(attempts, vec![4, 2, 1]);
    }

    #[test]
    fn test_is_offset_overflow_matches_arrow_error() {
        assert!(is_offset_overflow(&offset_overflow_error()));
    }

    #[test]
    fn test_retry_interleave_does_not_retry_non_offset_errors() {
        let mut attempts = Vec::new();

        let error = retry_interleave(4, 4, |rows_to_emit| {
            attempts.push(rows_to_emit);
            Err::<(), _>(DataFusionError::Execution("boom".into()))
        })
        .unwrap_err();

        assert_eq!(attempts, vec![4]);
        assert!(matches!(error, DataFusionError::Execution(msg) if msg == "boom"));
    }

    #[test]
    fn test_try_interleave_columns_surfaces_arrow_offset_overflow() {
        let batch = overflow_list_batch();
        let schema = batch.schema();
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let reservation = MemoryConsumer::new("test").register(&pool);
        let mut builder = BatchBuilder::new(schema, 1, 2, reservation);
        builder.push_batch(0, batch).unwrap();

        let error = builder
            .try_interleave_columns(&[(0, 0), (0, 0)])
            .unwrap_err();

        assert!(is_offset_overflow(&error));
    }
}

impl Drop for BatchBuilder {
    fn drop(&mut self) {
        self.diagnostic.begin_drop();
    }
}
impl Drop for BufferedBatch {
    fn drop(&mut self) {
        self.diagnostic.begin_drop();
    }
}
