//! Checked take windows retain their source authority through the last poll.
use arrow::array::{ArrayRef, RecordBatch, UInt32Array};
use arrow::datatypes::SchemaRef;
use datafusion::common::Result;

use super::super::super::OptionalConfig;
use super::super::{Budget, add, arrays, mul, refused};

#[derive(Debug)]
pub(in crate::row_exec) struct Output {
    outer: RecordBatch,
    inner: Vec<Option<ArrayRef>>,
    keep: Vec<usize>,
    schema: SchemaRef,
    outer_take: UInt32Array,
    inner_take: UInt32Array,
    offset: usize,
    empty_emitted: bool,
}

impl Output {
    pub(super) fn new(
        cfg: &OptionalConfig,
        outer: RecordBatch,
        inner: RecordBatch,
        outer_take: UInt32Array,
        inner_take: UInt32Array,
        budget: &Budget,
    ) -> Result<Self> {
        // Moved input handles and cloned keep indexes coexist with the original
        // batch metadata. No uncharged queue of complete output arrays exists.
        budget.admit(add(
            mul(inner.num_columns(), 128)?,
            mul(cfg.inner_keep_idx.len(), std::mem::size_of::<usize>())?,
        )?)?;
        let mut inner: Vec<_> = inner.into_parts().1.into_iter().map(Some).collect();
        for (index, source) in inner.iter_mut().enumerate() {
            if !cfg.inner_keep_idx.contains(&index) {
                let source = source.take().expect("unconsumed inner column");
                let bytes = source.get_array_memory_size();
                drop(source);
                budget.release(bytes);
            }
        }
        Ok(Self {
            outer,
            inner,
            keep: cfg.inner_keep_idx.clone(),
            schema: cfg.out_schema.clone(),
            outer_take,
            inner_take,
            offset: 0,
            empty_emitted: false,
        })
    }

    pub(in crate::row_exec) fn next(
        &mut self,
        budget: &Budget,
        rows: usize,
    ) -> Result<Option<RecordBatch>> {
        if self.offset == self.outer_take.len() && (self.offset != 0 || self.empty_emitted) {
            return Ok(None);
        }
        let rows = rows.max(1).min(self.outer_take.len() - self.offset);
        let outer = self.outer_take.slice(self.offset, rows);
        let inner = self.inner_take.slice(self.offset, rows);
        let metadata = add(
            std::mem::size_of::<RecordBatch>(),
            mul(self.schema.fields().len(), 128)?,
        )?;
        budget
            .admit(metadata)
            .map_err(|error| error.context("OptionalMatch output"))?;
        let mut columns = Vec::with_capacity(self.schema.fields().len());
        for source in self.outer.columns() {
            columns.push(column(source, &outer, budget)?);
        }
        for &index in &self.keep {
            columns.push(column(
                self.inner[index].as_ref().expect("selected inner column"),
                &inner,
                budget,
            )?);
        }
        let result = RecordBatch::try_new_with_options(
            self.schema.clone(),
            columns,
            &arrow::array::RecordBatchOptions::new().with_row_count(Some(rows)),
        )?;
        // Each column retained its actual charge after its kernel. This exact
        // batch charge is released only at the consumer's next poll or drop;
        // source arrays and complete take-index capacities stay charged too.
        self.offset = add(self.offset, rows)?;
        self.empty_emitted = true;
        Ok(Some(result))
    }
}

fn column(source: &ArrayRef, indices: &UInt32Array, budget: &Budget) -> Result<ArrayRef> {
    let envelope = arrays::selected(source.as_ref(), indices)?;
    budget
        .admit(envelope)
        .map_err(|error| error.context("OptionalMatch output"))?;
    let result = arrow::compute::take(source.as_ref(), indices, None)?;
    let actual = result.get_array_memory_size();
    if actual > envelope {
        return Err(refused("take exceeded its admitted envelope"));
    }
    budget.release(envelope - actual);
    Ok(result)
}
