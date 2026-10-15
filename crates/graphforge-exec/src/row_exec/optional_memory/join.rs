//! The existing materialized left join, with one admitted ownership envelope.
use std::collections::hash_map::RandomState;

pub(super) mod index;
mod output;
use index::Index;

use arrow::array::{Array, FixedSizeBinaryArray, UInt32Array, UInt64Array};
use datafusion::common::{DataFusionError, Result};

use super::super::{OptionalConfig, OptionalJoinKey};
use super::{Budget, Collected, add, refused};
use arrow::array::RecordBatch;

fn key(
    batch: &RecordBatch,
    columns: &[usize],
    row: usize,
    null_safe: bool,
) -> Result<Option<Vec<OptionalJoinKey>>> {
    // Result/Option's collecting iterator has no exact lower size hint and
    // would grow a one-column key to Vec's minimum capacity. Request the
    // admitted exact key width instead, including temporary insertion/probes.
    let mut values = Vec::with_capacity(columns.len());
    for &index in columns {
        let column = batch.column(index);
        let value = if null_safe {
            OptionalJoinKey::Value(datafusion::scalar::ScalarValue::try_from_array(
                column, row,
            )?)
        } else if column.is_null(row)
            || matches!(column.data_type(), arrow::datatypes::DataType::Null)
        {
            return Ok(None);
        } else if let Some(values) = column.as_any().downcast_ref::<UInt64Array>() {
            OptionalJoinKey::U64(values.value(row))
        } else if let Some(values) = column.as_any().downcast_ref::<FixedSizeBinaryArray>() {
            OptionalJoinKey::Uuid(values.value(row).try_into().map_err(|_| {
                DataFusionError::Execution(format!(
                    "optional join key at index {index} is not a 16-byte UUID"
                ))
            })?)
        } else {
            return Err(DataFusionError::Execution(format!(
                "optional join key at index {index} has unsupported type {:?}",
                column.data_type()
            )));
        };
        values.push(value);
    }
    Ok(Some(values))
}

/// A compact inner row may name its original key in the retained outer batch.
#[derive(Clone, Copy)]
pub(super) struct KeySource<'a> {
    pub(super) batch: &'a RecordBatch,
    pub(super) columns: &'a [usize],
    pub(super) mapped: Option<&'a [Option<u32>]>,
}
impl KeySource<'_> {
    pub(super) fn len(self) -> usize {
        self.mapped.map_or(self.batch.num_rows(), <[_]>::len)
    }
    pub(super) fn key(self, row: usize, null_safe: bool) -> Result<Option<Vec<OptionalJoinKey>>> {
        let row = match self.mapped {
            Some(rows) => match rows[row] {
                Some(row) => row as usize,
                None => return Ok(None),
            },
            None => row,
        };
        key(self.batch, self.columns, row, null_safe)
    }
}

fn row_index(value: usize) -> Result<u32> {
    u32::try_from(value).map_err(|_| refused("row index exceeds u32"))
}

pub(in crate::row_exec) fn join(
    cfg: &OptionalConfig,
    outer: Collected,
    mut inner: Collected,
    budget: &Budget,
) -> Result<output::Output> {
    let outer = budget.concat(outer, &cfg.outer_schema)?;
    let mapped = inner.mapped.take();
    let payload = inner.payload.take();
    let inner = budget.concat(inner, &cfg.inner_schema)?;
    for batch in [&outer, &inner] {
        if u32::try_from(batch.num_rows().saturating_sub(1)).is_err() {
            return Err(refused("row index exceeds u32"));
        }
    }
    // Retain the source authority while digest buckets refer to inner rows.
    // Only two temporary canonical keys coexist; collisions use exact equality.
    let index_bytes = if mapped.is_some() {
        budget.mapped_index(&outer, &cfg.join_keys)?
    } else {
        budget.index(&inner, &outer, &cfg.join_keys, cfg.null_safe_keys)?
    };
    let outer_keys: Vec<_> = cfg.join_keys.iter().map(|&(outer, _)| outer).collect();
    let inner_keys: Vec<_> = cfg.join_keys.iter().map(|&(_, inner)| inner).collect();
    let outer_source = KeySource {
        batch: &outer,
        columns: &outer_keys,
        mapped: None,
    };
    let inner_source = KeySource {
        batch: if mapped.is_some() { &outer } else { &inner },
        columns: if mapped.is_some() {
            &outer_keys
        } else {
            &inner_keys
        },
        mapped: mapped.as_deref(),
    };
    let index = Index::build(inner_source, cfg.null_safe_keys, budget, RandomState::new())?;
    let mut rows = 0;
    for row in 0..outer.num_rows() {
        let count = if cfg.join_keys.is_empty() {
            inner.num_rows()
        } else {
            let mut count = 0;
            index.visit(outer_source, row, inner_source, cfg.null_safe_keys, |_| {
                count = add(count, 1)?;
                Ok(())
            })?;
            count
        };
        rows = add(rows, count.max(1))?;
    }
    // Count first, without materializing a match list. Both take vectors and
    // nullable Arrow conversion coexist with the complete index and inputs.
    budget.indexes(rows)?;
    let mut outer_take = Vec::with_capacity(rows);
    let mut inner_take = Vec::with_capacity(rows);
    for row in 0..outer.num_rows() {
        let before = outer_take.len();
        if cfg.join_keys.is_empty() {
            for inner_row in 0..inner.num_rows() {
                outer_take.push(row_index(row)?);
                inner_take.push(Some(row_index(inner_row)?));
            }
        } else {
            index.visit(
                outer_source,
                row,
                inner_source,
                cfg.null_safe_keys,
                |inner_row| {
                    outer_take.push(row_index(row)?);
                    inner_take.push(Some(match &payload {
                        Some(rows) => rows[inner_row],
                        None => row_index(inner_row)?,
                    }));
                    Ok(())
                },
            )?;
        }
        if before == outer_take.len() {
            outer_take.push(row_index(row)?);
            inner_take.push(None);
        }
    }
    index.release(budget);
    drop(outer_keys);
    drop(inner_keys);
    budget.release(index_bytes);
    if let Some(mapped) = mapped {
        let bytes = super::mul(mapped.capacity(), std::mem::size_of::<Option<u32>>())?;
        drop(mapped);
        budget.release(bytes);
    }
    if let Some(payload) = payload {
        let bytes = super::mul(payload.capacity(), std::mem::size_of::<u32>())?;
        drop(payload);
        budget.release(bytes);
    }
    let outer_take = UInt32Array::from(outer_take);
    let inner_take = UInt32Array::from(inner_take);
    for &column in &cfg.inner_keep_idx {
        if column >= inner.num_columns() {
            return Err(DataFusionError::Execution(format!(
                "OptionalMatch inner_keep_idx {column} out of range ({} inner columns)",
                inner.num_columns()
            )));
        }
    }
    output::Output::new(cfg, outer, inner, outer_take, inner_take, budget)
}
