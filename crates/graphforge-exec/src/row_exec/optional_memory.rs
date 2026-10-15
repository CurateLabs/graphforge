//! One OPTIONAL MATCH execution owns a single nonspill reservation.
//!
//! Collections, copies, owned keys/hash scratch, take indexes and output share
//! its ceiling. All envelopes are admitted before retention/construction. The
//! producer already owns an incoming Arrow batch; this node charges its full
//! retained buffers before retaining that batch, not its visible slice length.

mod arrays;
mod join;
pub(super) use join::join;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::physical_plan::{ExecutionPlan, execute_stream};
use futures::TryStreamExt;
use std::sync::Arc;

pub(super) fn refused(message: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::ResourcesExhausted(format!("OptionalMatch: {message}"))
}
pub(super) fn add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b)
        .ok_or_else(|| refused("allocation size overflow"))
}
pub(super) fn mul(a: usize, b: usize) -> Result<usize> {
    a.checked_mul(b)
        .ok_or_else(|| refused("allocation size overflow"))
}

pub(super) struct Budget(MemoryReservation);

pub(super) struct Collected {
    pub(super) batches: Vec<RecordBatch>,
    bytes: usize,
}

impl Budget {
    pub(super) fn new(context: &TaskContext) -> Self {
        Self(MemoryConsumer::new("OptionalMatch").register(context.memory_pool()))
    }
    pub(super) fn admit(&self, bytes: usize) -> Result<()> {
        self.0.try_grow(bytes)
    }
    pub(super) fn release(&self, bytes: usize) {
        self.0.shrink(bytes);
    }
    pub(super) fn output(&self, batch: &RecordBatch) -> Result<()> {
        // All other owners must already be dropped; this only shrinks an
        // existing envelope and never justifies an allocation after the fact.
        let actual = batch_bytes(batch)?;
        if actual > self.0.size() {
            return Err(refused("output exceeded its admitted envelope"));
        }
        self.0.shrink(self.0.size() - actual);
        Ok(())
    }
    pub(super) async fn collect(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
    ) -> Result<Collected> {
        for field in plan.schema().fields() {
            arrays::validate(field.data_type())?;
        }
        let mut stream = execute_stream(plan, context)?;
        let mut batches = Vec::new();
        let mut bytes = 0;
        while let Some(batch) = stream.try_next().await? {
            for field in batch.schema().fields() {
                arrays::validate(field.data_type())?;
            }
            let held = batch_bytes(&batch)?;
            self.admit(held)
                .map_err(|error| error.context("OptionalMatch collection"))?;
            bytes = add(bytes, held)?;
            if batches.len() == batches.capacity() {
                let capacity = batches
                    .capacity()
                    .max(2)
                    .checked_mul(2)
                    .ok_or_else(|| refused("batch capacity overflow"))?;
                let allocated = mul(capacity, std::mem::size_of::<RecordBatch>())?;
                // The old vector remains charged during the allocation/copy.
                self.admit(allocated)
                    .map_err(|error| error.context("OptionalMatch collection"))?;
                let old = mul(batches.capacity(), std::mem::size_of::<RecordBatch>())?;
                batches
                    .try_reserve_exact(capacity - batches.len())
                    .map_err(|_| refused("batch vector allocation failed"))?;
                self.release(old);
                bytes = add(bytes - old, allocated)?;
            }
            batches.push(batch);
        }
        Ok(Collected { batches, bytes })
    }
    pub(super) fn seed(&self, batches: &[RecordBatch]) -> Result<()> {
        // Binding copies the batch/column handles, not the Arrow buffers.
        self.admit(handle_bytes(batches)?)
    }
    pub(super) fn concat(&self, collected: Collected, schema: &SchemaRef) -> Result<RecordBatch> {
        let Collected { mut batches, bytes } = collected;
        if batches.len() == 1 {
            let batch = batches.pop().expect("one collected batch");
            drop(batches);
            self.release(bytes - batch_bytes(&batch)?);
            return Ok(batch);
        }
        // Canonical concat copies at most the complete input buffers. Four
        // copies cover buffer growth/old allocations; metadata scales with the
        // existing array trees and batch handles, covered by the same envelope.
        let envelope = add(mul(bytes, 4)?, schema_bound(schema, 0)?)?;
        self.admit(envelope)
            .map_err(|error| error.context("OptionalMatch concat"))?;
        // Concat sums source spans; a take bound would falsely multiply one
        // large value by the lengths of unrelated, empty-valued batches.
        for column in 0..schema.fields().len() {
            let arrays: Vec<_> = batches
                .iter()
                .map(|batch| batch.column(column).clone())
                .collect();
            arrays::concat(&arrays)?;
        }
        let batch = arrow::compute::concat_batches(schema, &batches)?;
        let actual = batch_bytes(&batch)?;
        if actual > envelope {
            return Err(refused("concat exceeded its admitted envelope"));
        }
        drop(batches);
        self.release(add(bytes, envelope - actual)?);
        Ok(batch)
    }
    pub(super) fn indexes(&self, rows: usize) -> Result<()> {
        // Both Vecs plus nullable UInt32 conversion coexist. The plain u32 Vec
        // becomes Arrow's backing buffer without copying. Include metadata and
        // bitmap allocation rounding, even for zero rows.
        self.admit(add(
            mul(
                rows,
                std::mem::size_of::<u32>() + std::mem::size_of::<Option<u32>>() + 4,
            )?,
            add(rows.div_ceil(8), 2048)?,
        )?)
    }
    pub(super) fn take(
        &self,
        batch: &RecordBatch,
        indices: &arrow::array::UInt32Array,
        columns: impl Iterator<Item = usize>,
    ) -> Result<()> {
        for index in columns {
            self.admit(arrays::selected(batch.column(index).as_ref(), indices)?)
                .map_err(|error| error.context("OptionalMatch output"))?;
        }
        self.admit(mul(batch.num_columns(), 128)?)
    }
    pub(super) fn index(
        &self,
        inner: &RecordBatch,
        outer: &RecordBatch,
        keys: &[(usize, usize)],
        null_safe: bool,
    ) -> Result<usize> {
        use super::OptionalJoinKey;
        if keys.is_empty() {
            return Ok(0);
        }
        let rows = inner.num_rows();
        let table = table_bytes(
            rows,
            std::mem::size_of::<(Vec<OptionalJoinKey>, Vec<usize>)>(),
        )?;
        // At most one owned key per inner row plus a temporary insertion/probe
        // key. Row-list capacity <=4*rows, with its old allocation during growth.
        let vectors = add(
            mul(keys.len(), 2 * std::mem::size_of::<usize>())?,
            add(
                mul(
                    mul(add(rows, 2)?, keys.len())?,
                    std::mem::size_of::<OptionalJoinKey>(),
                )?,
                mul(rows, 8 * std::mem::size_of::<usize>())?,
            )?,
        )?;
        let mut owned = 0;
        let mut scratch = 0;
        for &(outer_index, inner_index) in keys {
            let left = outer.column(outer_index).as_ref();
            let right = inner.column(inner_index).as_ref();
            if null_safe {
                owned = add(
                    owned,
                    add(
                        mul(add(rows, 1)?, arrays::scalar(right)?)?,
                        arrays::scalar(left)?,
                    )?,
                )?;
                scratch = add(
                    scratch,
                    add(arrays::hash_scratch(left)?, arrays::hash_scratch(right)?)?,
                )?;
            }
        }
        let envelope = add(add(table, vectors)?, add(owned, scratch)?)?;
        self.admit(envelope)
            .map_err(|error| error.context("OptionalMatch index"))?;
        Ok(envelope)
    }
}

fn batch_bytes(batch: &RecordBatch) -> Result<usize> {
    add(
        batch.get_array_memory_size(),
        add(
            std::mem::size_of::<RecordBatch>(),
            mul(batch.num_columns(), 128)?,
        )?,
    )
}
fn schema_bound(schema: &SchemaRef, rows: usize) -> Result<usize> {
    // Typed empty arrays have no data-scaled payload, but their recursive
    // metadata still needs an admission before RecordBatch::new_empty/concat.
    fn nodes(data_type: &arrow::datatypes::DataType) -> Result<usize> {
        use arrow::datatypes::DataType;
        match data_type {
            DataType::Struct(fields) => fields
                .iter()
                .try_fold(1, |n, field| add(n, nodes(field.data_type())?)),
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::FixedSizeList(field, _) => add(1, nodes(field.data_type())?),
            _ => Ok(1),
        }
    }
    schema
        .fields()
        .iter()
        .try_fold(add(rows, arrays::NODE_BYTES)?, |bytes, field| {
            add(bytes, mul(nodes(field.data_type())?, arrays::NODE_BYTES)?)
        })
}
fn table_bytes(entries: usize, width: usize) -> Result<usize> {
    if entries == 0 {
        return Ok(0);
    }
    let buckets = (add(mul(entries, 8)?, 6)? / 7)
        .max(4)
        .checked_next_power_of_two()
        .ok_or_else(|| refused("hash capacity overflow"))?;
    add(mul(buckets, add(width, 1)?)?, 32)
}

/// Vec<RecordBatch> plus every cloned column-handle Vec. Arrow data is shared;
/// the owning outer collection already protects its retained buffers.
pub(super) fn handle_bytes(batches: &[RecordBatch]) -> Result<usize> {
    batches.iter().try_fold(0, |bytes, batch| {
        add(
            bytes,
            add(
                std::mem::size_of::<RecordBatch>(),
                mul(
                    batch.num_columns(),
                    std::mem::size_of::<arrow::array::ArrayRef>(),
                )?,
            )?,
        )
    })
}
