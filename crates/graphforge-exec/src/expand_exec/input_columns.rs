//! Which input columns a variable-length expansion copies into its emitted rows.
//!
//! Every emitted path repeats its source row, so a wide input column nobody
//! reads would be copied once per path (#1984). A column outside the mask is
//! emitted as nulls instead.

use super::ExpandConfig;
use arrow::array::ArrayRef;
use arrow::array::RecordBatch;
use arrow::array::UInt32Array;
use arrow::array::new_null_array;
use arrow::array::{
    FixedSizeBinaryArray, ListBuilder, StringArray, TimestampMicrosecondArray, UInt32Builder,
    UInt64Array,
};
use arrow::datatypes::{DataType, Field, SchemaRef, TimeUnit};
use graphforge_core::GfError;
use std::collections::BTreeSet;
use std::sync::Arc;

/// The copy mask for an expansion that emits `schema` with `input_width` input
/// columns in front, when only the `required` output columns are read. `None`
/// means every input column is copied.
///
/// A non-nullable column is always copied: a null array cannot stand in for it.
pub(super) fn copy_mask(
    schema: &SchemaRef,
    input_width: usize,
    required: &BTreeSet<usize>,
) -> Option<Arc<[bool]>> {
    let mask: Vec<bool> = (0..input_width)
        .map(|index| required.contains(&index) || !schema.field(index).is_nullable())
        .collect();
    mask.iter().any(|copied| !copied).then(|| Arc::from(mask))
}

/// Append the input columns of the rows `src_take` selects, one value per
/// emitted row. A column the mask skips is appended as nulls instead of being
/// copied once per path.
pub(super) fn push_input_columns(
    cfg: &ExpandConfig,
    input: &RecordBatch,
    src_take: &UInt32Array,
    columns: &mut Vec<ArrayRef>,
) -> Result<(), GfError> {
    for (index, column) in input.columns().iter().enumerate() {
        let skipped = cfg
            .copy_input
            .as_ref()
            .is_some_and(|copied| copied.get(index) == Some(&false));
        columns.push(if skipped {
            new_null_array(cfg.out_schema.field(index).data_type(), src_take.len())
        } else {
            arrow::compute::take(column, src_take, None)
                .map_err(|error| GfError::Execution(error.to_string()))?
        });
    }
    Ok(())
}

/// Build a schema-valid value column for an output that is provably unused by
/// every operator above this Expand. Nullable fields use Arrow nulls; required
/// physical fields receive inert values so the unchanged logical schema stays
/// valid without forcing their backing Parquet columns to be read.
pub(super) fn unused_expand_column(field: &Field, rows: usize) -> Result<ArrayRef, GfError> {
    if field.is_nullable() {
        return Ok(new_null_array(field.data_type(), rows));
    }
    let column: ArrayRef = match field.data_type() {
        DataType::FixedSizeBinary(width) => Arc::new(
            FixedSizeBinaryArray::try_from_iter(
                (0..rows).map(|_| vec![0_u8; usize::try_from(*width).unwrap_or(0)]),
            )
            .map_err(GfError::from_execution_error)?,
        ),
        DataType::UInt64 => Arc::new(UInt64Array::from(vec![0_u64; rows])),
        DataType::UInt32 => Arc::new(UInt32Array::from(vec![0_u32; rows])),
        DataType::Utf8 => Arc::new(StringArray::from(vec![""; rows])),
        DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
            let values = TimestampMicrosecondArray::from(vec![0_i64; rows]);
            Arc::new(if let Some(timezone) = timezone {
                values.with_timezone(Arc::clone(timezone))
            } else {
                values
            })
        }
        DataType::List(item) if item.data_type() == &DataType::UInt32 => {
            let mut builder = ListBuilder::new(UInt32Builder::new()).with_field(Arc::clone(item));
            for _ in 0..rows {
                builder.append(true);
            }
            Arc::new(builder.finish())
        }
        data_type => {
            return Err(GfError::Execution(format!(
                "Expand cannot synthesize unused non-nullable output '{}' with type {data_type}",
                field.name()
            )));
        }
    };
    Ok(column)
}
