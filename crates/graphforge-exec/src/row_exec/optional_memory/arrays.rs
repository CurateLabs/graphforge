//! Allocation envelopes for the pinned Arrow kernels used by OPTIONAL MATCH.
//!
//! Only canonical graph arrays enter these kernels. Dictionary/view/run/union
//! kernels have different retained children and scratch; refuse them before
//! construction rather than treating them as primitive slots. FixedSizeList
//! collection children also refuse: its concat fallback propagates an initial
//! capacity through nested children independently of their stored lengths.

use arrow::array::{Array, AsArray};
use arrow::datatypes::DataType;
use datafusion::common::Result;

use super::{add, mul, refused};

// Arrow 58.4 canonical nodes have at most three data/validity buffers. Their
// growing old/new buffers and FixedSizeList's index scratch need at most eight
// 64-byte alignment tails. Four ArrayData-sized headers cover the concrete
// array wrapper, ArrayData, mutable builder and temporary sliced wrapper; child
// vectors are covered once per recursive child. No payload is hidden here.
pub(super) const NODE_BYTES: usize = 4 * std::mem::size_of::<arrow::array::ArrayData>() + 8 * 64;

pub(super) fn validate(data_type: &DataType) -> Result<()> {
    match data_type {
        DataType::FixedSizeBinary(width) | DataType::FixedSizeList(_, width) if *width < 0 => {
            Err(refused("negative fixed-size Arrow width"))
        }
        DataType::Null
        | DataType::Boolean
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::FixedSizeBinary(_) => Ok(()),
        DataType::FixedSizeList(field, _)
            if matches!(
                field.data_type(),
                DataType::List(_)
                    | DataType::LargeList(_)
                    | DataType::Struct(_)
                    | DataType::FixedSizeList(_, _)
            ) =>
        {
            Err(refused("cannot bound nested FixedSizeList kernel"))
        }
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            validate(field.data_type())
        }
        DataType::Struct(fields) => fields
            .iter()
            .try_for_each(|field| validate(field.data_type())),
        data_type if data_type.primitive_width().is_some() => Ok(()),
        data_type => Err(refused(format!(
            "cannot bound Arrow kernel for {data_type}"
        ))),
    }
}

/// A complete copy/growth envelope, not an average bytes-per-row estimate.
/// Variable values use the largest source value times output multiplicity.
/// Four copies cover <=2x buffer growth plus the previous allocation during
/// reallocation; NODE_BYTES covers the pinned headers and aligned small buffers.
/// Parent envelopes sum their children, so all completed columns stay charged.
/// List construction also preallocates by full child_len / parent_len, which
/// can exceed selected spans for sliced sources or all-null take indexes.
pub(super) fn take(array: &dyn Array, rows: usize) -> Result<usize> {
    mul(raw_take(array, rows)?, 4)
}

/// Top-level bytes kernels precompute their exact selected payload. Use the
/// already-admitted indexes, including source/index nulls, so a single large
/// source value does not invent an offset overflow for many small output rows.
pub(super) fn selected(array: &dyn Array, indices: &arrow::array::UInt32Array) -> Result<usize> {
    let offset = match array.data_type() {
        DataType::Utf8 | DataType::Binary => 4,
        DataType::LargeUtf8 | DataType::LargeBinary => 8,
        _ => return take(array, indices.len()),
    };
    let payload = indices.iter().flatten().try_fold(0, |bytes, index| {
        let index = index as usize;
        let length = if array.is_null(index) {
            0
        } else {
            match array.data_type() {
                DataType::Utf8 => array.as_string::<i32>().value(index).len(),
                DataType::LargeUtf8 => array.as_string::<i64>().value(index).len(),
                DataType::Binary => array.as_binary::<i32>().value(index).len(),
                DataType::LargeBinary => array.as_binary::<i64>().value(index).len(),
                _ => unreachable!("byte type checked above"),
            }
        };
        add(bytes, length)
    })?;
    offset_limit(payload, offset)?;
    mul(
        add(
            add(
                mul(add(indices.len(), 1)?, offset)?,
                payload.max(indices.len()),
            )?,
            add(indices.len().div_ceil(8), NODE_BYTES)?,
        )?,
        4,
    )
}

fn raw_take(array: &dyn Array, rows: usize) -> Result<usize> {
    let validity = rows.div_ceil(8);
    let bytes = match array.data_type() {
        DataType::Null => 0,
        DataType::Boolean => validity,
        DataType::FixedSizeBinary(width) => mul(
            rows,
            usize::try_from(*width).map_err(|_| refused("negative binary width"))?,
        )?,
        DataType::Utf8 => bytes_bound(
            rows,
            array
                .as_string::<i32>()
                .iter()
                .flatten()
                .map(str::len)
                .max()
                .unwrap_or(0),
            4,
        )?,
        DataType::LargeUtf8 => bytes_bound(
            rows,
            array
                .as_string::<i64>()
                .iter()
                .flatten()
                .map(str::len)
                .max()
                .unwrap_or(0),
            8,
        )?,
        DataType::Binary => bytes_bound(
            rows,
            array
                .as_binary::<i32>()
                .iter()
                .flatten()
                .map(<[u8]>::len)
                .max()
                .unwrap_or(0),
            4,
        )?,
        DataType::LargeBinary => bytes_bound(
            rows,
            array
                .as_binary::<i64>()
                .iter()
                .flatten()
                .map(<[u8]>::len)
                .max()
                .unwrap_or(0),
            8,
        )?,
        DataType::List(_) => {
            let list = array.as_list::<i32>();
            let largest = largest_span(list.value_offsets())?;
            list_bound(list.values().as_ref(), array.len(), rows, largest, 4)?
        }
        DataType::LargeList(_) => {
            let list = array.as_list::<i64>();
            let largest = largest_span(list.value_offsets())?;
            list_bound(list.values().as_ref(), array.len(), rows, largest, 8)?
        }
        DataType::FixedSizeList(_, width) => {
            let list = array.as_fixed_size_list();
            if u32::try_from(list.values().len()).is_err() {
                return Err(refused("Arrow child index capacity exceeded"));
            }
            let child_rows = mul(
                rows,
                usize::try_from(*width).map_err(|_| refused("negative list width"))?,
            )?;
            // Arrow builds this nullable UInt32 array before taking children.
            add(
                add(mul(child_rows, 4)?, child_rows.div_ceil(8))?,
                raw_take(array.as_fixed_size_list().values().as_ref(), child_rows)?,
            )?
        }
        DataType::Struct(_) => array
            .as_struct()
            .columns()
            .iter()
            .try_fold(0, |bytes, child| {
                add(bytes, raw_take(child.as_ref(), rows)?)
            })?,
        data_type => match data_type.primitive_width() {
            Some(width) => mul(rows, width)?,
            None => return Err(refused(format!("cannot bound Arrow take for {data_type}"))),
        },
    };
    add(add(bytes, validity)?, NODE_BYTES)
}

fn bytes_bound(rows: usize, largest: usize, offset: usize) -> Result<usize> {
    let payload = mul(rows, largest)?;
    offset_limit(payload, offset)?;
    // MutableArrayData also initializes a byte buffer with capacity=rows even
    // for empty strings when nested inside list/struct copying.
    add(mul(add(rows, 1)?, offset)?, payload.max(rows))
}

fn list_bound(
    child: &dyn Array,
    source_rows: usize,
    rows: usize,
    largest: usize,
    offset: usize,
) -> Result<usize> {
    let actual = mul(rows, largest)?;
    offset_limit(actual, offset)?;
    let hint = mul(child.len().checked_div(source_rows).unwrap_or(0), rows)?;
    add(
        mul(add(rows, 1)?, offset)?,
        mutable_copy(child, hint, actual)?,
    )
}

/// take_list uses MutableArrayData for children, rather than recursively calling
/// take_list. Its initial slot hint is inherited unchanged through List and
/// Struct, and multiplied through FixedSizeList. In particular, empty nested
/// lists can still allocate a large grandchild buffer from that inherited hint.
fn mutable_copy(array: &dyn Array, initial: usize, rows: usize) -> Result<usize> {
    let slots = initial.max(rows);
    let bytes = match array.data_type() {
        DataType::List(_) => {
            let list = array.as_list::<i32>();
            let largest = largest_span(list.value_offsets())?;
            let children = mul(rows, largest)?;
            offset_limit(children, 4)?;
            add(
                mul(add(slots, 1)?, 4)?,
                mutable_copy(list.values().as_ref(), initial, children)?,
            )?
        }
        DataType::LargeList(_) => {
            let list = array.as_list::<i64>();
            let largest = largest_span(list.value_offsets())?;
            let children = mul(rows, largest)?;
            offset_limit(children, 8)?;
            add(
                mul(add(slots, 1)?, 8)?,
                mutable_copy(list.values().as_ref(), initial, children)?,
            )?
        }
        DataType::FixedSizeList(_, width) => {
            let width = usize::try_from(*width).map_err(|_| refused("negative list width"))?;
            mutable_copy(
                array.as_fixed_size_list().values().as_ref(),
                mul(initial, width)?,
                mul(rows, width)?,
            )?
        }
        DataType::Struct(_) => array
            .as_struct()
            .columns()
            .iter()
            .try_fold(0, |bytes, child| {
                add(bytes, mutable_copy(child.as_ref(), initial, rows)?)
            })?,
        // For scalar children, raw_take bounds both the inherited initial
        // capacity and copied values; it does not recurse into another kernel.
        _ => return raw_take(array, slots),
    };
    add(add(bytes, slots.div_ceil(8))?, NODE_BYTES)
}

fn offset<O: arrow::array::OffsetSizeTrait>(value: O) -> Result<usize> {
    value
        .to_usize()
        .ok_or_else(|| refused("invalid Arrow offset"))
}
fn offset_span<O: arrow::array::OffsetSizeTrait>(offsets: &[O]) -> Result<usize> {
    offset(offsets[offsets.len() - 1])?
        .checked_sub(offset(offsets[0])?)
        .ok_or_else(|| refused("invalid Arrow offset span"))
}
fn largest_span<O: arrow::array::OffsetSizeTrait>(offsets: &[O]) -> Result<usize> {
    offsets
        .windows(2)
        .try_fold(0, |largest, pair| Ok(largest.max(offset_span(pair)?)))
}

fn offset_limit(values: usize, offset: usize) -> Result<()> {
    let fits = if offset == 4 {
        i32::try_from(values).is_ok()
    } else {
        i64::try_from(values).is_ok()
    };
    if fits {
        Ok(())
    } else {
        Err(refused("Arrow offset capacity exceeded"))
    }
}

/// Upper bound for one nested ScalarValue conversion/hash invocation. Nested
/// hash kernels allocate u64 vectors over child rows, not encoded byte sizes.
/// Summing every child length covers simultaneously live recursive hash buffers,
/// including Null/Boolean children whose physical bytes can be much smaller.
pub(super) fn hash_scratch(array: &dyn Array) -> Result<usize> {
    if matches!(
        array.data_type(),
        DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Struct(_)
    ) {
        // DF 54.1 hash_nested_array owns one u64 vector; recursive struct
        // owns one per parent row, list/fixed-list one per child row. At most
        // two vectors per node coexist. Sum full child lengths (even slices)
        // and two u64s/slot, plus wrapper metadata, before invoking Hash.
        add(
            mul(hash_slots(array)?, 2 * std::mem::size_of::<u64>())?,
            take(array, 1)?,
        )
    } else {
        Ok(0)
    }
}
fn hash_slots(array: &dyn Array) -> Result<usize> {
    let children = match array.data_type() {
        DataType::List(_) => hash_slots(array.as_list::<i32>().values().as_ref())?,
        DataType::LargeList(_) => hash_slots(array.as_list::<i64>().values().as_ref())?,
        DataType::FixedSizeList(_, _) => hash_slots(array.as_fixed_size_list().values().as_ref())?,
        DataType::Struct(_) => array
            .as_struct()
            .columns()
            .iter()
            .try_fold(0, |slots, child| add(slots, hash_slots(child.as_ref())?))?,
        _ => 0,
    };
    add(array.len(), children)
}

/// Primitive ScalarValues are inline. Bytes are cloned; nested scalars create
/// array wrappers/slices (and null constructors), bounded by a one-row copy.
/// Shared source buffers remain protected by the collected-input charge.
pub(super) fn scalar(array: &dyn Array) -> Result<usize> {
    match array.data_type() {
        DataType::Utf8 => Ok(array
            .as_string::<i32>()
            .iter()
            .flatten()
            .map(str::len)
            .max()
            .unwrap_or(0)),
        DataType::LargeUtf8 => Ok(array
            .as_string::<i64>()
            .iter()
            .flatten()
            .map(str::len)
            .max()
            .unwrap_or(0)),
        DataType::Binary => Ok(array
            .as_binary::<i32>()
            .iter()
            .flatten()
            .map(<[u8]>::len)
            .max()
            .unwrap_or(0)),
        DataType::LargeBinary => Ok(array
            .as_binary::<i64>()
            .iter()
            .flatten()
            .map(<[u8]>::len)
            .max()
            .unwrap_or(0)),
        DataType::FixedSizeBinary(width) => {
            usize::try_from(*width).map_err(|_| refused("negative binary width"))
        }
        DataType::Struct(_)
        | DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(_, _) => take(array, 1),
        _ => Ok(0),
    }
}

/// Arrow 58.4 concat_lists slices each child to its actual offset span before
/// recursively concatenating it. Check those sums, including nested offsets,
/// before any concat kernel's infallible offset arithmetic. Temporary handles
/// and sliced wrappers are inside the already-admitted concat copy envelope.
pub(super) fn concat(arrays: &[arrow::array::ArrayRef]) -> Result<()> {
    let Some(first) = arrays.first() else {
        return Ok(());
    };
    let data_type = first.data_type();
    let rows = arrays
        .iter()
        .try_fold(0, |rows, array| add(rows, array.len()))?;
    let span = |array: &arrow::array::ArrayRef| -> Result<usize> {
        match data_type {
            DataType::Utf8 => {
                let o = array.as_string::<i32>().value_offsets();
                offset_span(o)
            }
            DataType::Binary => {
                let o = array.as_binary::<i32>().value_offsets();
                offset_span(o)
            }
            DataType::LargeUtf8 => {
                let o = array.as_string::<i64>().value_offsets();
                offset_span(o)
            }
            DataType::LargeBinary => {
                let o = array.as_binary::<i64>().value_offsets();
                offset_span(o)
            }
            _ => Ok(0),
        }
    };
    match data_type {
        DataType::Utf8 | DataType::Binary => offset_limit(
            arrays
                .iter()
                .try_fold(0, |sum, array| add(sum, span(array)?))?,
            4,
        )?,
        DataType::LargeUtf8 | DataType::LargeBinary => offset_limit(
            arrays
                .iter()
                .try_fold(0, |sum, array| add(sum, span(array)?))?,
            8,
        )?,
        DataType::List(_) | DataType::LargeList(_) => {
            let mut total = 0;
            let mut children = Vec::with_capacity(arrays.len());
            for array in arrays {
                let (start, end, child) = if matches!(data_type, DataType::List(_)) {
                    let list = array.as_list::<i32>();
                    let o = list.value_offsets();
                    (offset(o[0])?, offset(o[o.len() - 1])?, list.values())
                } else {
                    let list = array.as_list::<i64>();
                    let o = list.value_offsets();
                    (offset(o[0])?, offset(o[o.len() - 1])?, list.values())
                };
                total = add(total, end - start)?;
                children.push(child.slice(start, end - start));
            }
            offset_limit(
                total,
                if matches!(data_type, DataType::List(_)) {
                    4
                } else {
                    8
                },
            )?;
            concat(&children)?;
        }
        DataType::Struct(fields) => {
            for column in 0..fields.len() {
                let children: Vec<_> = arrays
                    .iter()
                    .map(|array| array.as_struct().column(column).clone())
                    .collect();
                concat(&children)?;
            }
        }
        DataType::FixedSizeList(_, width) => {
            mul(
                rows,
                usize::try_from(*width).map_err(|_| refused("negative list width"))?,
            )?;
            let children: Vec<_> = arrays
                .iter()
                .map(|array| array.as_fixed_size_list().values().clone())
                .collect();
            concat(&children)?;
        }
        DataType::FixedSizeBinary(width) => {
            mul(
                rows,
                usize::try_from(*width).map_err(|_| refused("negative binary width"))?,
            )?;
        }
        data_type => {
            if let Some(width) = data_type.primitive_width() {
                mul(rows, width)?;
            }
        }
    }
    Ok(())
}
