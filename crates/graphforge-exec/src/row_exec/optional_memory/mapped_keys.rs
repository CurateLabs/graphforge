//! Compound correlated keys borrow their exact values from retained outer rows.
//! The unchanged producer runs first; only OPTIONAL's retained representation changes.
use std::collections::hash_map::RandomState;

use super::join::{KeySource, index::Index};
use super::{
    Arc, Budget, Collected, ExecutionPlan, RecordBatch, Result, SchemaRef, TaskContext, add, mul,
    refused, schema_bound,
};

pub(super) struct Mapper<'a> {
    outer: &'a RecordBatch,
    outer_keys: Vec<usize>,
    inner_keys: Vec<usize>,
    keep: &'a [usize],
    keys: &'a [(usize, usize)],
    schema: SchemaRef,
    index: Index<RandomState>,
    rows: Vec<Option<u32>>,
}

impl Mapper<'_> {
    pub(super) fn project(&mut self, batch: RecordBatch, budget: &Budget) -> Result<RecordBatch> {
        let work = budget.index(&batch, self.outer, self.keys, true)?;
        let required = add(self.rows.len(), batch.num_rows())?;
        u32::try_from(required.saturating_sub(1)).map_err(|_| refused("row index exceeds u32"))?;
        if required > self.rows.capacity() {
            let next = required.max(mul(self.rows.capacity(), 2)?);
            let bytes = mul(next, std::mem::size_of::<Option<u32>>())?;
            budget.admit(bytes)?;
            let old = mul(self.rows.capacity(), std::mem::size_of::<Option<u32>>())?;
            self.rows
                .try_reserve_exact(next - self.rows.len())
                .map_err(|_| refused("mapped row allocation failed"))?;
            budget.release(old);
        }
        let source = KeySource {
            batch: self.outer,
            columns: &self.outer_keys,
            mapped: None,
        };
        let probe = KeySource {
            batch: &batch,
            columns: &self.inner_keys,
            mapped: None,
        };
        for row in 0..batch.num_rows() {
            self.rows.push(
                self.index
                    .first(probe, row, source, true)?
                    .map(u32::try_from)
                    .transpose()
                    .map_err(|_| refused("row index exceeds u32"))?,
            );
        }
        let held = self.keep.iter().try_fold(
            add(
                std::mem::size_of::<RecordBatch>(),
                mul(self.keep.len(), 128)?,
            )?,
            |bytes, &index| add(bytes, batch.column(index).get_array_memory_size()),
        )?;
        budget
            .admit(held)
            .map_err(|error| error.context("OptionalMatch collection"))?;
        let projected = RecordBatch::try_new_with_options(
            self.schema.clone(),
            self.keep
                .iter()
                .map(|&index| Arc::clone(batch.column(index)))
                .collect(),
            &arrow::array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )?;
        drop(batch);
        budget.release(work);
        Ok(projected)
    }
}

impl Budget {
    pub(in crate::row_exec) async fn collect_mapped(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
        cfg: &mut super::super::OptionalConfig,
        outer: &RecordBatch,
    ) -> Result<Collected> {
        u32::try_from(outer.num_rows().saturating_sub(1))
            .map_err(|_| refused("row index exceeds u32"))?;
        // Column positions and key conversion scratch coexist with the source
        // buffers. Hash buckets retain ordinals only and check exact equality.
        let handles = mul(cfg.join_keys.len(), 2 * std::mem::size_of::<usize>())?;
        self.admit(handles)?;
        let outer_keys: Vec<_> = cfg.join_keys.iter().map(|&(outer, _)| outer).collect();
        let inner_keys: Vec<_> = cfg.join_keys.iter().map(|&(_, inner)| inner).collect();
        let work = self.mapped_index(outer, &cfg.join_keys)?;
        let index = Index::representatives(
            KeySource {
                batch: outer,
                columns: &outer_keys,
                mapped: None,
            },
            true,
            self,
            RandomState::new(),
        )?;
        self.release(work);
        self.admit(schema_bound(&cfg.inner_schema, 0)?)?;
        let schema = Arc::new(cfg.inner_schema.project(&cfg.inner_keep_idx)?);
        let mut mapper = Mapper {
            outer,
            outer_keys,
            inner_keys,
            keep: &cfg.inner_keep_idx,
            keys: &cfg.join_keys,
            schema: schema.clone(),
            index,
            rows: Vec::new(),
        };
        let mut collected = self.collect_with(plan, context, Some(&mut mapper)).await?;
        let Mapper {
            index,
            rows,
            outer_keys,
            inner_keys,
            ..
        } = mapper;
        index.release(self);
        drop(outer_keys);
        drop(inner_keys);
        self.release(handles);
        collected.mapped = Some(rows);
        cfg.inner_schema = schema;
        for (index, kept) in cfg.inner_keep_idx.iter_mut().enumerate() {
            *kept = index;
        }
        Ok(collected)
    }
}
