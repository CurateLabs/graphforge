//! One OPTIONAL MATCH execution owns a single nonspill reservation.
//!
//! Collections, copies, owned keys/hash scratch, take indexes and output share
//! its ceiling. All envelopes are admitted before retention/construction. The
//! producer already owns an incoming Arrow batch; this node charges its full
//! retained buffers before retaining that batch, not its visible slice length.

mod arrays;
mod join;
mod mapped_keys;
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
    mapped: Option<Vec<Option<u32>>>,
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
    #[cfg(test)]
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
        self.collect_with(plan, context, None).await
    }
    async fn collect_with(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
        mut projection: Option<&mut mapped_keys::Mapper<'_>>,
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
            let projected = projection.is_some();
            let batch = match projection.as_deref_mut() {
                Some(mapper) => mapper.project(batch, self)?,
                None => batch,
            };
            let held = batch_bytes(&batch)?;
            if !projected {
                self.admit(held)
                    .map_err(|error| error.context("OptionalMatch collection"))?;
            }
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
        Ok(Collected {
            batches,
            bytes,
            mapped: None,
        })
    }
    pub(super) fn compact(&self, rows: Collected, schema: &SchemaRef) -> Result<Collected> {
        let batch = self.concat(rows, schema)?;
        let handles = std::mem::size_of::<RecordBatch>();
        self.admit(handles)?;
        let bytes = add(batch_bytes(&batch)?, handles)?;
        Ok(Collected {
            batches: vec![batch],
            bytes,
            mapped: None,
        })
    }
    pub(super) fn seed(&self, batches: &[RecordBatch]) -> Result<()> {
        // Binding copies the batch/column handles, not the Arrow buffers.
        self.admit(handle_bytes(batches)?)
    }
    pub(super) fn concat(&self, collected: Collected, schema: &SchemaRef) -> Result<RecordBatch> {
        let Collected {
            mut batches,
            bytes,
            mapped,
        } = collected;
        debug_assert!(
            mapped.is_none(),
            "mapped ownership must reach join before concat"
        );
        if batches.len() == 1 {
            let batch = batches.pop().expect("one collected batch");
            drop(batches);
            self.release(bytes - batch_bytes(&batch)?);
            return Ok(batch);
        }
        if batches.is_empty() {
            let envelope = schema_bound(schema, 0)?;
            self.admit(envelope)
                .map_err(|error| error.context("OptionalMatch concat"))?;
            let batch = RecordBatch::new_empty(schema.clone());
            let actual = batch_bytes(&batch)?;
            if actual > envelope {
                return Err(refused("empty concat exceeded its admitted envelope"));
            }
            self.release(add(bytes, envelope - actual)?);
            return Ok(batch);
        }
        let rows = batches
            .iter()
            .try_fold(0, |rows, batch| add(rows, batch.num_rows()))?;
        // concat_batches already copies one column at a time. Move each source
        // column into that same kernel, then drop it before releasing its full
        // retained capacity. Completed outputs and all unconsumed columns stay
        // charged; source handles/recursive slice wrappers coexist in scratch.
        let scratch = mul(batches.len(), schema_bound(schema, 0)?)?;
        let output_handles = add(
            std::mem::size_of::<RecordBatch>(),
            mul(schema.fields().len(), 128)?,
        )?;
        self.admit(add(scratch, output_handles)?)
            .map_err(|error| error.context("OptionalMatch concat"))?;
        let mut inputs: Vec<_> = batches
            .into_iter()
            .map(|batch| batch.into_parts().1.into_iter())
            .collect();
        let mut remaining = bytes;
        let mut columns = Vec::with_capacity(schema.fields().len());
        for _ in schema.fields() {
            let arrays: Vec<_> = inputs
                .iter_mut()
                .map(|input| input.next().expect("validated batch column count"))
                .collect();
            let source = arrays
                .iter()
                .try_fold(0, |bytes, array| add(bytes, array.get_array_memory_size()))?;
            let envelope = arrays::concat(&arrays)?;
            self.admit(envelope)
                .map_err(|error| error.context("OptionalMatch concat"))?;
            let refs: Vec<_> = arrays.iter().map(AsRef::as_ref).collect();
            let column = arrow::compute::concat(&refs)?;
            let actual = column.get_array_memory_size();
            if actual > envelope {
                return Err(refused("concat exceeded its admitted envelope"));
            }
            drop(refs);
            drop(arrays);
            self.release(add(source, envelope - actual)?);
            remaining = remaining
                .checked_sub(source)
                .ok_or_else(|| refused("concat source accounting underflow"))?;
            columns.push(column);
        }
        drop(inputs);
        let batch = RecordBatch::try_new_with_options(
            schema.clone(),
            columns,
            &arrow::array::RecordBatchOptions::new().with_row_count(Some(rows)),
        )?;
        self.release(add(remaining, scratch)?);
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
    pub(super) fn index(
        &self,
        inner: &RecordBatch,
        outer: &RecordBatch,
        keys: &[(usize, usize)],
        null_safe: bool,
    ) -> Result<usize> {
        self.index_work(inner, outer, keys, null_safe, false)
    }
    pub(super) fn mapped_index(
        &self,
        outer: &RecordBatch,
        keys: &[(usize, usize)],
    ) -> Result<usize> {
        self.index_work(outer, outer, keys, true, true)
    }
    fn index_work(
        &self,
        inner: &RecordBatch,
        outer: &RecordBatch,
        keys: &[(usize, usize)],
        null_safe: bool,
        mapped: bool,
    ) -> Result<usize> {
        use super::OptionalJoinKey;
        if keys.is_empty() {
            return Ok(0);
        }
        // The index retains source row ordinals, not cloned ScalarValues.
        // Both a probe and one collision candidate can coexist. Their exact
        // canonical keys and recursive hash scratch are admitted before either
        // conversion; the original Arrow inputs remain fully charged.
        let mut owned = 0;
        let mut scratch = 0;
        for &(outer_index, inner_index) in keys {
            let left = outer.column(outer_index).as_ref();
            let right = inner
                .column(if mapped { outer_index } else { inner_index })
                .as_ref();
            if null_safe {
                owned = add(owned, add(arrays::scalar(left)?, arrays::scalar(right)?)?)?;
                scratch = add(
                    scratch,
                    add(arrays::hash_scratch(left)?, arrays::hash_scratch(right)?)?,
                )?;
            }
        }
        let vectors = mul(
            keys.len(),
            2 * (std::mem::size_of::<OptionalJoinKey>() + std::mem::size_of::<usize>()),
        )?;
        let envelope = add(vectors, add(owned, scratch)?)?;
        self.admit(envelope)
            .map_err(|error| error.context("OptionalMatch index"))?;
        Ok(envelope)
    }
}

pub(super) fn batch_bytes(batch: &RecordBatch) -> Result<usize> {
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
pub(super) fn table_bytes(entries: usize, width: usize) -> Result<usize> {
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
