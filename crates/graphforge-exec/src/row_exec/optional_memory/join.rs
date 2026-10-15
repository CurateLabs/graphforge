//! The existing materialized left join, with one admitted ownership envelope.
use std::collections::hash_map::RandomState;

mod index;
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

fn row_index(value: usize) -> Result<u32> {
    u32::try_from(value).map_err(|_| refused("row index exceeds u32"))
}

pub(in crate::row_exec) fn join(
    cfg: &OptionalConfig,
    outer: Collected,
    inner: Collected,
    budget: &Budget,
) -> Result<output::Output> {
    let outer = budget.concat(outer, &cfg.outer_schema)?;
    let inner = budget.concat(inner, &cfg.inner_schema)?;
    for batch in [&outer, &inner] {
        if u32::try_from(batch.num_rows().saturating_sub(1)).is_err() {
            return Err(refused("row index exceeds u32"));
        }
    }
    // Retain the source authority while digest buckets refer to inner rows.
    // Only two temporary canonical keys coexist; collisions use exact equality.
    let index_bytes = budget.index(&inner, &outer, &cfg.join_keys, cfg.null_safe_keys)?;
    let outer_keys: Vec<_> = cfg.join_keys.iter().map(|&(outer, _)| outer).collect();
    let inner_keys: Vec<_> = cfg.join_keys.iter().map(|&(_, inner)| inner).collect();
    let index = Index::build(
        &inner,
        &inner_keys,
        cfg.null_safe_keys,
        budget,
        RandomState::new(),
    )?;
    let mut rows = 0;
    for row in 0..outer.num_rows() {
        let count = if cfg.join_keys.is_empty() {
            inner.num_rows()
        } else {
            let mut count = 0;
            index.visit(
                (&outer, &outer_keys, row),
                (&inner, &inner_keys),
                cfg.null_safe_keys,
                |_| {
                    count = add(count, 1)?;
                    Ok(())
                },
            )?;
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
                (&outer, &outer_keys, row),
                (&inner, &inner_keys),
                cfg.null_safe_keys,
                |inner_row| {
                    outer_take.push(row_index(row)?);
                    inner_take.push(Some(row_index(inner_row)?));
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
