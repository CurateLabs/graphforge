//! Preallocated Arrow 58.4 concat buffers, separate from retained source capacity.
use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::DataType;
use datafusion::common::Result;

use super::{NODE_BYTES, add, mul, offset, offset_limit, offset_span, refused};

/// concat_primitives/boolean/bytes preallocate the complete logical length and
/// bytes_capacity (sum of source offset spans), then freeze buffers without a
/// payload copy. Struct/list retain all completed children. Only list offsets
/// use a growing Vec: OffsetBuffer::from_lengths sees a flat_map lower hint0;
/// 4x its final slots covers <=2x capacity and its old allocation during growth.
/// NODE_BYTES covers headers and alignment, not unused source data capacity.
/// The caller separately admits source-wrapper/ArrayData/iterator metadata
/// before this recursive walk creates any temporary handles or slice wrappers.
pub(in crate::row_exec::optional_memory) fn bytes(arrays: &[ArrayRef]) -> Result<usize> {
    let Some(first) = arrays.first() else {
        return Ok(0);
    };
    let rows = arrays
        .iter()
        .try_fold(0, |rows, array| add(rows, array.len()))?;
    let values = match first.data_type() {
        DataType::Null => 0,
        DataType::Boolean => rows.div_ceil(8),
        DataType::Utf8 | DataType::Binary => byte_values(arrays, rows, 4)?,
        DataType::LargeUtf8 | DataType::LargeBinary => byte_values(arrays, rows, 8)?,
        DataType::List(_) => lists(arrays, rows, 4)?,
        DataType::LargeList(_) => lists(arrays, rows, 8)?,
        DataType::Struct(fields) => {
            let mut total = 0;
            for column in 0..fields.len() {
                let children: Vec<_> = arrays
                    .iter()
                    .map(|array| array.as_struct().column(column).clone())
                    .collect();
                total = add(total, bytes(&children)?)?;
            }
            total
        }
        DataType::FixedSizeList(_, width) => {
            mul(
                rows,
                usize::try_from(*width).map_err(|_| refused("negative list width"))?,
            )?;
            // Its admitted child type is flat. The fallback's capacities use
            // parent_rows*width or get_capacity over these complete children
            // (bytes fields), so both stored initial capacities are covered.
            let children: Vec<_> = arrays
                .iter()
                .map(|array| array.as_fixed_size_list().values().clone())
                .collect();
            bytes(&children)?
        }
        DataType::FixedSizeBinary(width) => mul(
            rows,
            usize::try_from(*width).map_err(|_| refused("negative binary width"))?,
        )?,
        data_type => mul(
            rows,
            data_type
                .primitive_width()
                .ok_or_else(|| refused(format!("cannot bound Arrow concat for {data_type}")))?,
        )?,
    };
    add(add(values, rows.div_ceil(8))?, NODE_BYTES)
}

fn byte_values(arrays: &[ArrayRef], rows: usize, width: usize) -> Result<usize> {
    let payload = arrays.iter().try_fold(0, |total, array| {
        let span = match array.data_type() {
            DataType::Utf8 => offset_span(array.as_string::<i32>().value_offsets())?,
            DataType::LargeUtf8 => offset_span(array.as_string::<i64>().value_offsets())?,
            DataType::Binary => offset_span(array.as_binary::<i32>().value_offsets())?,
            DataType::LargeBinary => offset_span(array.as_binary::<i64>().value_offsets())?,
            _ => return Err(refused("cannot bound non-byte concat payload")),
        };
        add(total, span)
    })?;
    offset_limit(payload, width)?;
    add(mul(add(rows, 1)?, width)?, payload)
}

fn lists(arrays: &[ArrayRef], rows: usize, width: usize) -> Result<usize> {
    let mut total = 0;
    let mut children = Vec::with_capacity(arrays.len());
    for array in arrays {
        let (start, end, child) = if width == 4 {
            let list = array.as_list::<i32>();
            let o = list.value_offsets();
            (offset(o[0])?, offset(o[o.len() - 1])?, list.values())
        } else {
            let list = array.as_list::<i64>();
            let o = list.value_offsets();
            (offset(o[0])?, offset(o[o.len() - 1])?, list.values())
        };
        let length = end
            .checked_sub(start)
            .ok_or_else(|| refused("invalid list span"))?;
        total = add(total, length)?;
        // concat_lists copies just this span, including null parent slots.
        children.push(child.slice(start, length));
    }
    offset_limit(total, width)?;
    add(mul(mul(add(rows, 1)?, width)?, 4)?, bytes(&children)?)
}
