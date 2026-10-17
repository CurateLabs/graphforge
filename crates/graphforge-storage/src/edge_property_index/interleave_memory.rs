//! Extra workspace of the pinned Arrow 58.4 interleave kernels.
//!
//! Callers separately admit stored payloads and final output slots. Every
//! selected source row occurs at most once, so complete source child lengths
//! bound the recursive index vectors without multiplying payload by hit count.

use std::mem::size_of;

use arrow::array::{Array, ArrayData, ArrayRef, AsArray, MutableArrayData};
use arrow::buffer::Buffer;
use arrow::datatypes::DataType;
use graphforge_core::GfError;

use super::{query_add, query_mul, query_size_error};

/// Additional scratch, summed over actual sources before interleave starts.
pub(super) fn workspace(array: &dyn Array) -> Result<usize, GfError> {
    // Native interleave keeps one source ArrayRef/reference-vector slot, one
    // &dyn Array input slot, and one typed downcast pointer per source. Views
    // additionally allocate an offsets Vec with sources + 1 usize entries:
    // two entries per source bound that final sentinel even for one source.
    // Recursive calls pay their own child-reference slots. Output arrays,
    // offsets and validity already have the caller's payload/NULL admission.
    let bytes = size_of::<ArrayRef>() + size_of::<&dyn Array>() + 3 * size_of::<usize>();
    let extra = match array.data_type() {
        DataType::List(_) => list_workspace(array.as_list::<i32>().values().as_ref())?,
        DataType::LargeList(_) => list_workspace(array.as_list::<i64>().values().as_ref())?,
        DataType::Struct(_) => array
            .as_struct()
            .columns()
            .iter()
            .try_fold(0, |bytes, child| {
                query_add(bytes, workspace(child.as_ref())?)
            })?,
        DataType::Utf8View => view_workspace(array.as_string_view().data_buffers().len())?,
        DataType::BinaryView => view_workspace(array.as_binary_view().data_buffers().len())?,
        // FixedSizeList uses interleave_fallback, whose MutableArrayData
        // propagates initial capacities even into empty nested children.
        DataType::FixedSizeList(_, _)
        | DataType::Null
        | DataType::Boolean
        | DataType::FixedSizeBinary(_) => {
            return fallback_workspace(array, array.len());
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary => 0,
        data_type if data_type.primitive_width().is_some() => 0,
        _ => return Err(query_size_error()),
    };
    query_add(bytes, extra)
}

fn list_workspace(values: &dyn Array) -> Result<usize, GfError> {
    // interleave_list allocates one (source array, child row) pair per selected
    // child element, then keeps that Vec alive during recursive interleave.
    query_add(
        query_mul(values.len(), size_of::<(usize, usize)>())?,
        workspace(values)?,
    )
}

fn view_workspace(buffers: usize) -> Result<usize, GfError> {
    // interleave_views maps EVERY source buffer, including unused/empty ones.
    // The output Buffer Vec can grow to twice its length with old/new overlap;
    // Vec -> Arc<[Buffer]> conversion also overlaps its final allocation.
    query_mul(
        buffers,
        query_add(size_of::<Option<u32>>(), query_mul(4, size_of::<Buffer>())?)?,
    )
}

fn metadata(array: &dyn Array) -> Result<usize, GfError> {
    // Per recursive source: input/downcast/reference Vec entries and view
    // offsets (including the final offset), plus fallback's ArrayData copies,
    // MutableArrayData child slots and two boxed extend closures. Four copies
    // cover source, mutable, frozen and concrete output metadata; four fat
    // slices cover both closures' captured source ranges and their Vec slots.
    query_add(
        query_mul(
            4,
            query_add(
                size_of::<ArrayData>(),
                query_add(size_of::<MutableArrayData<'_>>(), array.data_type().size())?,
            )?,
        )?,
        query_mul(16, size_of::<usize>())?,
    )
}

fn fallback_workspace(array: &dyn Array, capacity: usize) -> Result<usize, GfError> {
    let mut bytes = query_add(
        metadata(array)?,
        initial_buffers(array.data_type(), capacity)?,
    )?;
    match array.data_type() {
        DataType::List(_) => {
            bytes = query_add(
                bytes,
                fallback_workspace(array.as_list::<i32>().values().as_ref(), capacity)?,
            )?;
        }
        DataType::LargeList(_) => {
            bytes = query_add(
                bytes,
                fallback_workspace(array.as_list::<i64>().values().as_ref(), capacity)?,
            )?;
        }
        DataType::FixedSizeList(_, width) => {
            let width = usize::try_from(*width).map_err(|_| query_size_error())?;
            bytes = query_add(
                bytes,
                fallback_workspace(
                    array.as_fixed_size_list().values().as_ref(),
                    query_mul(capacity, width)?,
                )?,
            )?;
        }
        DataType::Struct(_) => {
            for child in array.as_struct().columns() {
                bytes = query_add(bytes, fallback_workspace(child.as_ref(), capacity)?)?;
            }
        }
        DataType::Utf8View => {
            bytes = query_add(
                bytes,
                view_workspace(array.as_string_view().data_buffers().len())?,
            )?;
        }
        DataType::BinaryView => {
            bytes = query_add(
                bytes,
                view_workspace(array.as_binary_view().data_buffers().len())?,
            )?;
        }
        _ => {}
    }
    Ok(bytes)
}

/// MutableArrayData::new_buffers uses the inherited row capacity for every
/// descendant, rather than that descendant's actual length. Pay those initial
/// buffers explicitly; existing payload admission covers later value growth.
fn initial_buffers(data_type: &DataType, capacity: usize) -> Result<usize, GfError> {
    let payload = match data_type {
        DataType::Null | DataType::Struct(_) | DataType::FixedSizeList(_, _) => 0,
        DataType::Boolean => capacity.div_ceil(8),
        DataType::Utf8 | DataType::Binary => {
            query_add(query_mul(query_add(capacity, 1)?, 4)?, capacity)?
        }
        DataType::LargeUtf8 | DataType::LargeBinary => {
            query_add(query_mul(query_add(capacity, 1)?, 8)?, capacity)?
        }
        DataType::List(_) => query_mul(query_add(capacity, 1)?, 4)?,
        DataType::LargeList(_) => query_mul(query_add(capacity, 1)?, 8)?,
        DataType::Utf8View | DataType::BinaryView => query_mul(capacity, 16)?,
        DataType::FixedSizeBinary(width) => query_mul(
            capacity,
            usize::try_from(*width).map_err(|_| query_size_error())?,
        )?,
        data_type if data_type.primitive_width().is_some() => {
            query_mul(capacity, data_type.primitive_width().unwrap())?
        }
        _ => return Err(query_size_error()),
    };
    // Old/new overlap, validity, and the three buffers' alignment tails.
    query_add(
        query_mul(2, query_add(payload, capacity.div_ceil(8))?)?,
        3 * 64,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{BooleanArray, ListArray, StringViewArray};
    use arrow::buffer::{OffsetBuffer, ScalarBuffer};
    use arrow::datatypes::Field;

    #[test]
    fn boolean_list_workspace_tracks_child_indices_instead_of_packed_payload() {
        let make = |length| {
            ListArray::new(
                Arc::new(Field::new("item", DataType::Boolean, true)),
                OffsetBuffer::from_lengths([length]),
                Arc::new(BooleanArray::from(vec![true; length])),
                None,
            )
        };
        let small = make(8);
        let large = make(8192);
        let growth = workspace(&large).unwrap() - workspace(&small).unwrap();
        let children = 8192 - 8;
        assert!(growth >= children * size_of::<(usize, usize)>());
        // Boolean's fallback bitmap workspace adds less than one byte per
        // child; the dominant term must remain the actual index-pair vector.
        assert!(growth < children * (size_of::<(usize, usize)>() + 1));
        assert!(workspace(&large).unwrap() > large.get_buffer_memory_size() * 16);
    }

    #[test]
    fn views_admit_unused_zero_capacity_buffer_descriptors() {
        let make = |buffers| {
            StringViewArray::new(
                ScalarBuffer::from(Vec::<u128>::new()),
                vec![Buffer::from_vec(Vec::<u8>::new()); buffers],
                None,
            )
        };
        let small = make(0);
        let large = make(1024);
        assert_eq!(large.get_buffer_memory_size(), 0);
        assert_eq!(
            workspace(&large).unwrap() - workspace(&small).unwrap(),
            1024 * (size_of::<Option<u32>>() + 4 * size_of::<Buffer>())
        );
    }

    #[test]
    fn fixed_list_fallback_admits_initial_capacity_beneath_empty_lists() {
        use arrow::array::FixedSizeListArray;
        let make = |rows| {
            let lists: ArrayRef = Arc::new(ListArray::new(
                Arc::new(Field::new("item", DataType::Int64, true)),
                OffsetBuffer::from_lengths(std::iter::repeat_n(0, rows)),
                Arc::new(arrow::array::Int64Array::from(Vec::<i64>::new())),
                None,
            ));
            FixedSizeListArray::new(
                Arc::new(Field::new("item", lists.data_type().clone(), true)),
                1,
                lists,
                None,
            )
        };
        let small = make(1);
        let large = make(65_536);
        // The leaf has zero values, but fallback preallocates Int64 slots
        // according to its inherited parent capacity.
        assert!(workspace(&large).unwrap() - workspace(&small).unwrap() >= 65_535 * 8);
    }
}
