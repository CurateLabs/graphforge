//! The existing materialized left join, with one admitted ownership envelope.
use std::collections::HashMap;

use arrow::array::{Array, FixedSizeBinaryArray, UInt32Array, UInt64Array};
use arrow::compute::take;
use datafusion::common::{DataFusionError, Result};

use super::super::{OptionalConfig, OptionalJoinKey};
use super::{Budget, Collected, add, refused};
use arrow::array::RecordBatch;

type Index = HashMap<Vec<OptionalJoinKey>, Vec<usize>>;

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

fn build_index(inner: &RecordBatch, columns: &[usize], null_safe: bool) -> Result<Index> {
    if columns.is_empty() {
        return Ok(HashMap::new());
    }
    let mut index: Index = HashMap::with_capacity(inner.num_rows());
    for row in 0..inner.num_rows() {
        if let Some(key) = key(inner, columns, row, null_safe)? {
            index.entry(key).or_default().push(row);
        }
    }
    Ok(index)
}

fn matches<'a>(
    index: &'a Index,
    outer: &RecordBatch,
    columns: &[usize],
    row: usize,
    null_safe: bool,
) -> Result<&'a [usize]> {
    Ok(key(outer, columns, row, null_safe)?
        .and_then(|key| index.get(&key))
        .map_or(&[], Vec::as_slice))
}

pub(in crate::row_exec) fn join(
    cfg: &OptionalConfig,
    outer: Collected,
    inner: Collected,
    budget: &Budget,
) -> Result<RecordBatch> {
    let outer = budget.concat(outer, &cfg.outer_schema)?;
    let inner = budget.concat(inner, &cfg.inner_schema)?;
    for batch in [&outer, &inner] {
        if u32::try_from(batch.num_rows().saturating_sub(1)).is_err() {
            return Err(refused("row index exceeds u32"));
        }
    }
    // Input buffers and concat copies are still charged while indexing. The
    // pre-sized table cannot rehash; owned key values, probe keys, row-list
    // growth and recursive scalar hashing all draw on this admitted envelope.
    let index_bytes = budget.index(&inner, &outer, &cfg.join_keys, cfg.null_safe_keys)?;
    let outer_keys: Vec<_> = cfg.join_keys.iter().map(|&(outer, _)| outer).collect();
    let inner_keys: Vec<_> = cfg.join_keys.iter().map(|&(_, inner)| inner).collect();
    let index = build_index(&inner, &inner_keys, cfg.null_safe_keys)?;
    let mut rows = 0;
    for row in 0..outer.num_rows() {
        let count = if cfg.join_keys.is_empty() {
            inner.num_rows()
        } else {
            matches(&index, &outer, &outer_keys, row, cfg.null_safe_keys)?.len()
        };
        rows = add(rows, count.max(1))?;
    }
    // Count first, without materializing a match list. Both take vectors and
    // nullable Arrow conversion coexist with the complete index and inputs.
    budget.indexes(rows)?;
    let mut outer_take = Vec::with_capacity(rows);
    let mut inner_take = Vec::with_capacity(rows);
    for row in 0..outer.num_rows() {
        let matched = matches(&index, &outer, &outer_keys, row, cfg.null_safe_keys)?;
        if cfg.join_keys.is_empty() && inner.num_rows() != 0 {
            for inner_row in 0..inner.num_rows() {
                outer_take.push(row_index(row)?);
                inner_take.push(Some(row_index(inner_row)?));
            }
        } else if matched.is_empty() {
            outer_take.push(row_index(row)?);
            inner_take.push(None);
        } else {
            for &inner_row in matched {
                outer_take.push(row_index(row)?);
                inner_take.push(Some(row_index(inner_row)?));
            }
        }
    }
    drop(index);
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
    // Admit every completed column plus the active kernel, before the first
    // take. The source buffers and both take arrays remain live throughout.
    budget.take(&outer, &outer_take, 0..outer.num_columns())?;
    budget.take(&inner, &inner_take, cfg.inner_keep_idx.iter().copied())?;
    let mut columns = Vec::with_capacity(cfg.out_schema.fields().len());
    for column in outer.columns() {
        columns.push(take(column, &outer_take, None)?);
    }
    for &column in &cfg.inner_keep_idx {
        columns.push(take(inner.column(column), &inner_take, None)?);
    }
    Ok(RecordBatch::try_new(cfg.out_schema.clone(), columns)?)
}
