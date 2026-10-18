// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Borrowed layout observations; addresses are observations, never owner IDs.
//!
//! No Arrow owners are retained and no values, names, or schema strings are
//! emitted. Callers must discard incomplete traversals, and may preflight with
//! a callback returning true before reserving a complete group's output space.

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::buffer::Buffer;
use arrow::datatypes::*;

// Exactly one role bit is set. BITS changes offset/length units to bits;
// capacity always remains bytes. Array offsets/lengths are logical elements.
pub(super) const ARRAY: u64 = 1;
pub(super) const NULLS: u64 = 2;
pub(super) const VALUES: u64 = 4;
pub(super) const OFFSETS: u64 = 8;
pub(super) const VIEWS: u64 = 16;
pub(super) const BITS: u64 = 32;

/// One array node or one borrowed backing descriptor, with no value payload.
///
/// Nodes are one-based within this batch; parent zero denotes a root. Child
/// order is descriptor traversal order, and column is always the root ordinal.
/// `owner` is supplied by the caller, never derived from addresses. `object`
/// identifies the currently borrowed Array or Buffer object, not its lifetime.
/// For buffers, base/capacity describe the allocation and offset/length the
/// visible span from that base (in bytes, or bits with BITS). Null buffers use
/// their BooleanBuffer's bit span. Empty slices still report retained capacity.
/// Array records have base=capacity=0, logical offset/length and null count.
/// Their children/buffers are independently computed expected immediate child
/// and own buffer descriptor counts, including validity and dynamic view data.
/// Buffer records have children=buffers=0.
///
/// Canonical `data_type` low 16-bit IDs:
/// 1 Null, 2 Boolean; 3..6 Int8/16/32/64; 7..10 UInt8/16/32/64;
/// 11..13 Float16/32/64; 14 Date32, 15 Date64; 16 Time32, 17 Time64;
/// 18 Timestamp without timezone, 19 Duration, 20 Interval;
/// 21 FixedSizeBinary; 22 Utf8, 23 LargeUtf8, 24 Binary, 25 LargeBinary;
/// 26 Utf8View, 27 BinaryView, 28 List, 29 LargeList, 30 Struct;
/// 31..34 Decimal32/64/128/256.
/// For 16..19 bits 16..31 encode Seconds=1, Milliseconds=2,
/// Microseconds=3, Nanoseconds=4. For 20 the same bits encode
/// YearMonth=1, DayTime=2, MonthDayNano=3. For 21 bits 16..47 encode
/// nonnegative byte width. For decimals bits 16..23 encode precision and
/// bits 24..31 encode the scale's two's-complement i8 representation.
/// Timezone strings have no encoding and are explicitly unsupported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Backing {
    pub(super) owner: u64,
    pub(super) node: u64,
    pub(super) parent_node: u64,
    pub(super) column: u64,
    pub(super) data_type: u64,
    pub(super) object: u64,
    pub(super) base: u64,
    pub(super) capacity: u64,
    pub(super) offset: u64,
    pub(super) length: u64,
    pub(super) nulls: u64,
    pub(super) flags: u64,
    pub(super) children: u64,
    pub(super) buffers: u64,
}

/// Per-batch traversal limits. Depth is the number of allowed levels, including
/// the root. Columns bounds both root columns and immediate struct children.
#[derive(Debug, Clone, Copy)]
pub(super) struct Envelope {
    pub(super) nodes: usize,
    pub(super) depth: usize,
    pub(super) buffers: usize,
    pub(super) columns: usize,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct Counts {
    pub(super) nodes: usize,
    pub(super) buffers: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Error {
    Unsupported,
    Quota,
    Callback,
    ForeignBuffer,
    InvalidLayout,
}

/// Traverse concrete borrowed accessors only. Errors invalidate the entire
/// group, including descriptors already passed to the callback.
pub(super) fn visit_batch(
    batch: &RecordBatch,
    owner: u64,
    envelope: Envelope,
    emit: impl FnMut(Backing) -> bool,
) -> Result<Counts, Error> {
    if batch.num_columns() > envelope.columns {
        return Err(Error::Quota);
    }
    let mut visitor = Visitor {
        owner,
        envelope,
        counts: Counts::default(),
        emit,
    };
    for (column, array) in batch.columns().iter().enumerate() {
        visitor.array(array.as_ref(), 0, 0, column)?;
    }
    Ok(visitor.counts)
}

/// Traverse one evaluated key without constructing or cloning a RecordBatch.
pub(super) fn visit_array(
    array: &dyn Array,
    owner: u64,
    envelope: Envelope,
    emit: impl FnMut(Backing) -> bool,
) -> Result<Counts, Error> {
    let mut visitor = Visitor {
        owner,
        envelope,
        counts: Counts::default(),
        emit,
    };
    visitor.array(array, 0, 0, 0)?;
    Ok(visitor.counts)
}

fn unit(unit: &TimeUnit) -> u64 {
    match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 2,
        TimeUnit::Microsecond => 3,
        TimeUnit::Nanosecond => 4,
    }
}

fn layout(data_type: &DataType) -> Result<u64, Error> {
    Ok(match data_type {
        DataType::Null => 1,
        DataType::Boolean => 2,
        DataType::Int8 => 3,
        DataType::Int16 => 4,
        DataType::Int32 => 5,
        DataType::Int64 => 6,
        DataType::UInt8 => 7,
        DataType::UInt16 => 8,
        DataType::UInt32 => 9,
        DataType::UInt64 => 10,
        DataType::Float16 => 11,
        DataType::Float32 => 12,
        DataType::Float64 => 13,
        DataType::Date32 => 14,
        DataType::Date64 => 15,
        DataType::Time32(u @ (TimeUnit::Second | TimeUnit::Millisecond)) => {
            16 | (unit(u) << 16)
        }
        DataType::Time64(u @ (TimeUnit::Microsecond | TimeUnit::Nanosecond)) => {
            17 | (unit(u) << 16)
        }
        DataType::Timestamp(u, None) => 18 | (unit(u) << 16),
        DataType::Duration(u) => 19 | (unit(u) << 16),
        DataType::Interval(u) => {
            let id = match u {
                IntervalUnit::YearMonth => 1,
                IntervalUnit::DayTime => 2,
                IntervalUnit::MonthDayNano => 3,
            };
            20 | (id << 16)
        }
        DataType::FixedSizeBinary(width) if *width >= 0 => 21 | ((*width as u64) << 16),
        DataType::Utf8 => 22,
        DataType::LargeUtf8 => 23,
        DataType::Binary => 24,
        DataType::LargeBinary => 25,
        DataType::Utf8View => 26,
        DataType::BinaryView => 27,
        DataType::List(_) => 28,
        DataType::LargeList(_) => 29,
        DataType::Struct(_) => 30,
        DataType::Decimal32(p, s) => {
            31 | (u64::from(*p) << 16) | (u64::from(*s as u8) << 24)
        }
        DataType::Decimal64(p, s) => {
            32 | (u64::from(*p) << 16) | (u64::from(*s as u8) << 24)
        }
        DataType::Decimal128(p, s) => {
            33 | (u64::from(*p) << 16) | (u64::from(*s as u8) << 24)
        }
        DataType::Decimal256(p, s) => {
            34 | (u64::from(*p) << 16) | (u64::from(*s as u8) << 24)
        }
        _ => return Err(Error::Unsupported),
    })
}

struct Visitor<F> {
    owner: u64,
    envelope: Envelope,
    counts: Counts,
    emit: F,
}

impl<F: FnMut(Backing) -> bool> Visitor<F> {
    fn send(&mut self, descriptor: Backing) -> Result<(), Error> {
        if (self.emit)(descriptor) {
            Ok(())
        } else {
            Err(Error::Callback)
        }
    }

    fn buffer(
        &mut self,
        array: Backing,
        value: &Buffer,
        role: u64,
        bits: Option<(usize, usize)>,
    ) -> Result<(), Error> {
        if self.counts.buffers >= self.envelope.buffers {
            return Err(Error::Quota);
        }
        let base = value.data_ptr().as_ptr() as usize;
        let offset = (value.as_ptr() as usize)
            .checked_sub(base)
            .ok_or(Error::InvalidLayout)?;
        if !value.is_empty() && value.capacity() == 0 {
            return Err(Error::ForeignBuffer);
        }
        let end = offset
            .checked_add(value.len())
            .ok_or(Error::InvalidLayout)?;
        if value.capacity() != 0 && end > value.capacity() {
            return Err(Error::InvalidLayout);
        }
        let (offset, length, flags) = match bits {
            Some((bit_offset, bit_len)) => {
                if bit_offset
                    .checked_add(bit_len)
                    .ok_or(Error::InvalidLayout)?
                    > value.len().checked_mul(8).ok_or(Error::InvalidLayout)?
                {
                    return Err(Error::InvalidLayout);
                }
                (
                    offset
                        .checked_mul(8)
                        .and_then(|v| v.checked_add(bit_offset))
                        .ok_or(Error::InvalidLayout)?,
                    bit_len,
                    role | BITS,
                )
            }
            None => (offset, value.len(), role),
        };
        self.counts.buffers += 1;
        self.send(Backing {
            object: value as *const Buffer as usize as u64,
            base: base as u64,
            capacity: value.capacity() as u64,
            offset: offset as u64,
            length: length as u64,
            flags,
            children: 0,
            buffers: 0,
            ..array
        })
    }

    fn array(
        &mut self,
        array: &dyn Array,
        depth: usize,
        parent_node: u64,
        column: usize,
    ) -> Result<(), Error> {
        if depth >= self.envelope.depth || self.counts.nodes >= self.envelope.nodes {
            return Err(Error::Quota);
        }
        let data_type = layout(array.data_type())?;
        // Compute expectations directly from the borrowed concrete structure,
        // before emitting or recursively visiting any descriptor. These counts
        // must not be derived from how much output the callback accepted.
        let (children, own_buffers) = match array.data_type() {
            DataType::Null => (0, 0),
            DataType::List(_) | DataType::LargeList(_) => (1, 1),
            DataType::Struct(_) => (
                array
                    .as_struct_opt()
                    .ok_or(Error::InvalidLayout)?
                    .num_columns(),
                0,
            ),
            DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary => (0, 2),
            DataType::Utf8View => (
                0,
                array
                    .as_string_view_opt()
                    .ok_or(Error::InvalidLayout)?
                    .data_buffers()
                    .len()
                    .checked_add(1)
                    .ok_or(Error::InvalidLayout)?,
            ),
            DataType::BinaryView => (
                0,
                array
                    .as_binary_view_opt()
                    .ok_or(Error::InvalidLayout)?
                    .data_buffers()
                    .len()
                    .checked_add(1)
                    .ok_or(Error::InvalidLayout)?,
            ),
            _ => (0, 1),
        };
        let buffers = own_buffers
            .checked_add(usize::from(array.nulls().is_some()))
            .ok_or(Error::InvalidLayout)?;
        self.counts.nodes += 1;
        let node = self.counts.nodes as u64;
        let descriptor = Backing {
            owner: self.owner,
            node,
            parent_node,
            column: column as u64,
            data_type,
            object: array as *const dyn Array as *const () as usize as u64,
            base: 0,
            capacity: 0,
            offset: array.offset() as u64,
            length: array.len() as u64,
            nulls: array.null_count() as u64,
            flags: ARRAY,
            children: children as u64,
            buffers: buffers as u64,
        };
        self.send(descriptor)?;
        if let Some(nulls) = array.nulls() {
            self.buffer(
                descriptor,
                nulls.inner().inner(),
                NULLS,
                Some((nulls.offset(), nulls.len())),
            )?;
        }
        macro_rules! primitive {
            ($ty:ty) => {
                self.buffer(
                    descriptor,
                    array
                        .as_primitive_opt::<$ty>()
                        .ok_or(Error::InvalidLayout)?
                        .values()
                        .inner(),
                    VALUES,
                    None,
                )
            };
        }
        macro_rules! bytes {
            ($accessor:ident, $offset:ty) => {{
                let value = array.$accessor::<$offset>().ok_or(Error::InvalidLayout)?;
                self.buffer(descriptor, value.offsets().inner().inner(), OFFSETS, None)?;
                self.buffer(descriptor, value.values(), VALUES, None)
            }};
        }
        match array.data_type() {
            DataType::Null => Ok(()),
            DataType::Boolean => {
                let values = array.as_boolean_opt().ok_or(Error::InvalidLayout)?.values();
                self.buffer(
                    descriptor,
                    values.inner(),
                    VALUES,
                    Some((values.offset(), values.len())),
                )
            }
            DataType::Int8 => primitive!(Int8Type),
            DataType::Int16 => primitive!(Int16Type),
            DataType::Int32 => primitive!(Int32Type),
            DataType::Int64 => primitive!(Int64Type),
            DataType::UInt8 => primitive!(UInt8Type),
            DataType::UInt16 => primitive!(UInt16Type),
            DataType::UInt32 => primitive!(UInt32Type),
            DataType::UInt64 => primitive!(UInt64Type),
            DataType::Float16 => primitive!(Float16Type),
            DataType::Float32 => primitive!(Float32Type),
            DataType::Float64 => primitive!(Float64Type),
            DataType::Date32 => primitive!(Date32Type),
            DataType::Date64 => primitive!(Date64Type),
            DataType::Time32(TimeUnit::Second) => primitive!(Time32SecondType),
            DataType::Time32(TimeUnit::Millisecond) => {
                primitive!(Time32MillisecondType)
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                primitive!(Time64MicrosecondType)
            }
            DataType::Time64(TimeUnit::Nanosecond) => primitive!(Time64NanosecondType),
            DataType::Timestamp(TimeUnit::Second, None) => {
                primitive!(TimestampSecondType)
            }
            DataType::Timestamp(TimeUnit::Millisecond, None) => {
                primitive!(TimestampMillisecondType)
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                primitive!(TimestampMicrosecondType)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                primitive!(TimestampNanosecondType)
            }
            DataType::Duration(TimeUnit::Second) => primitive!(DurationSecondType),
            DataType::Duration(TimeUnit::Millisecond) => {
                primitive!(DurationMillisecondType)
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                primitive!(DurationMicrosecondType)
            }
            DataType::Duration(TimeUnit::Nanosecond) => {
                primitive!(DurationNanosecondType)
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                primitive!(IntervalYearMonthType)
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                primitive!(IntervalDayTimeType)
            }
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                primitive!(IntervalMonthDayNanoType)
            }
            DataType::Decimal32(_, _) => primitive!(Decimal32Type),
            DataType::Decimal64(_, _) => primitive!(Decimal64Type),
            DataType::Decimal128(_, _) => primitive!(Decimal128Type),
            DataType::Decimal256(_, _) => primitive!(Decimal256Type),
            DataType::FixedSizeBinary(_) => self.buffer(
                descriptor,
                array
                    .as_fixed_size_binary_opt()
                    .ok_or(Error::InvalidLayout)?
                    .values(),
                VALUES,
                None,
            ),
            DataType::Utf8 => bytes!(as_string_opt, i32),
            DataType::LargeUtf8 => bytes!(as_string_opt, i64),
            DataType::Binary => bytes!(as_binary_opt, i32),
            DataType::LargeBinary => bytes!(as_binary_opt, i64),
            DataType::Utf8View => {
                let value = array.as_string_view_opt().ok_or(Error::InvalidLayout)?;
                self.buffer(descriptor, value.views().inner(), VIEWS, None)?;
                for buffer in value.data_buffers() {
                    self.buffer(descriptor, buffer, VALUES, None)?;
                }
                Ok(())
            }
            DataType::BinaryView => {
                let value = array.as_binary_view_opt().ok_or(Error::InvalidLayout)?;
                self.buffer(descriptor, value.views().inner(), VIEWS, None)?;
                for buffer in value.data_buffers() {
                    self.buffer(descriptor, buffer, VALUES, None)?;
                }
                Ok(())
            }
            DataType::List(_) => {
                let value = array.as_list_opt::<i32>().ok_or(Error::InvalidLayout)?;
                self.buffer(descriptor, value.offsets().inner().inner(), OFFSETS, None)?;
                self.array(value.values().as_ref(), depth + 1, node, column)
            }
            DataType::LargeList(_) => {
                let value = array.as_list_opt::<i64>().ok_or(Error::InvalidLayout)?;
                self.buffer(descriptor, value.offsets().inner().inner(), OFFSETS, None)?;
                self.array(value.values().as_ref(), depth + 1, node, column)
            }
            DataType::Struct(_) => {
                let value = array.as_struct_opt().ok_or(Error::InvalidLayout)?;
                if value.num_columns() > self.envelope.columns {
                    return Err(Error::Quota);
                }
                for child in value.columns() {
                    self.array(child.as_ref(), depth + 1, node, column)?;
                }
                Ok(())
            }
            _ => Err(Error::Unsupported),
        }
    }
}

#[cfg(any(test, feature = "test_utils"))]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, BooleanArray, DictionaryArray, FixedSizeBinaryArray, Int8Array,
        Int32Array, Int64Array, ListArray, StringArray, StringViewArray, StructArray,
        TimestampMicrosecondArray,
    };
    use std::sync::Arc;

    fn envelope() -> Envelope {
        Envelope {
            nodes: 64,
            depth: 8,
            buffers: 128,
            columns: 16,
        }
    }

    fn collect(batch: &RecordBatch) -> Vec<Backing> {
        let mut descriptors = Vec::new();
        let counts = visit_batch(batch, 47, envelope(), |descriptor| {
            descriptors.push(descriptor);
            true
        })
        .unwrap();
        assert_eq!(descriptors.len(), counts.nodes + counts.buffers);
        assert!(descriptors.iter().all(|d| d.owner == 47));
        descriptors
    }

    #[cfg_attr(test, test)]
    fn shared_slice_keeps_allocation_identity_and_visible_span() {
        let source = Int64Array::from(vec![1, 2, 3, 4]);
        let values = Arc::new(source.slice(0, 2));
        let sliced = Arc::new(source.slice(1, 2));
        let batch = RecordBatch::try_from_iter([
            ("whole", values.clone() as ArrayRef),
            ("slice", sliced.clone() as ArrayRef),
        ])
        .unwrap();
        let owners_before = (Arc::strong_count(&values), Arc::strong_count(&sliced));
        let descriptors = collect(&batch);
        assert_eq!(
            owners_before,
            (Arc::strong_count(&values), Arc::strong_count(&sliced))
        );
        let buffers: Vec<_> = descriptors.iter().filter(|d| d.flags == VALUES).collect();
        assert_eq!(buffers.len(), 2);
        assert_eq!(buffers[0].base, buffers[1].base);
        assert_eq!(buffers[0].capacity, buffers[1].capacity);
        assert_eq!(buffers[1].offset - buffers[0].offset, 8);
        assert_eq!(buffers[1].length, 16);
        assert_ne!(buffers[0].node, buffers[1].node);
    }

    #[cfg_attr(test, test)]
    fn all_null_temporal_still_reports_values_and_validity_storage() {
        let batch = RecordBatch::try_from_iter([(
            "time",
            Arc::new(TimestampMicrosecondArray::new_null(3)) as ArrayRef,
        )])
        .unwrap();
        let descriptors = collect(&batch);
        let array = descriptors.iter().find(|d| d.flags == ARRAY).unwrap();
        assert_eq!(array.data_type, 18 | (3 << 16));
        assert_eq!((array.length, array.nulls), (3, 3));
        assert_eq!((array.children, array.buffers), (0, 2));
        let values = descriptors.iter().find(|d| d.flags == VALUES).unwrap();
        assert_eq!(values.length, 24);
        assert!(values.capacity >= 24);
        let nulls = descriptors
            .iter()
            .find(|d| d.flags == NULLS | BITS)
            .unwrap();
        assert_eq!(nulls.length, 3);
    }

    #[cfg_attr(test, test)]
    fn boolean_slice_preserves_bit_span() {
        let values = BooleanArray::from(vec![true, false, true, false, true]);
        let batch = RecordBatch::try_from_iter([(
            "flag",
            Arc::new(values.slice(1, 3)) as ArrayRef,
        )])
        .unwrap();
        let descriptors = collect(&batch);
        let values = descriptors
            .iter()
            .find(|d| d.flags == VALUES | BITS)
            .unwrap();
        assert_eq!((values.offset, values.length), (1, 3));
        assert!(values.capacity >= 1);
    }

    #[cfg_attr(test, test)]
    fn nested_list_and_struct_keep_parent_links() {
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(1), None]),
            None,
        ]);
        let structure = StructArray::from(vec![(
            Arc::new(Field::new("items", list.data_type().clone(), true)),
            Arc::new(list) as ArrayRef,
        )]);
        let batch =
            RecordBatch::try_from_iter([("root", Arc::new(structure) as ArrayRef)])
                .unwrap();
        let descriptors = collect(&batch);
        let arrays: Vec<_> = descriptors.iter().filter(|d| d.flags == ARRAY).collect();
        assert_eq!(arrays.len(), 3);
        assert_eq!((arrays[0].node, arrays[0].parent_node), (1, 0));
        assert_eq!((arrays[1].node, arrays[1].parent_node), (2, 1));
        assert_eq!((arrays[2].node, arrays[2].parent_node), (3, 2));
        assert!(arrays.iter().all(|d| d.column == 0));
        assert_eq!((arrays[0].children, arrays[0].buffers), (1, 0));
        assert_eq!((arrays[1].children, arrays[1].buffers), (1, 2));
        assert_eq!((arrays[2].children, arrays[2].buffers), (0, 2));
        assert!(
            descriptors
                .iter()
                .any(|d| d.node == 2 && d.flags == OFFSETS)
        );
    }

    #[cfg_attr(test, test)]
    fn views_report_all_dynamic_backings() {
        let array =
            StringViewArray::from(vec!["a long string outside the view", "short"]);
        let expected_buffers = array.data_buffers().len();
        assert!(expected_buffers > 0);
        let batch =
            RecordBatch::try_from_iter([("text", Arc::new(array) as ArrayRef)]).unwrap();
        let descriptors = collect(&batch);
        let node = descriptors.iter().find(|d| d.flags == ARRAY).unwrap();
        assert_eq!(
            (node.children, node.buffers),
            (0, expected_buffers as u64 + 1)
        );
        assert!(
            descriptors
                .iter()
                .filter(|d| d.flags != ARRAY)
                .all(|d| d.children == 0 && d.buffers == 0)
        );
        assert_eq!(descriptors.iter().filter(|d| d.flags == VIEWS).count(), 1);
        assert_eq!(
            descriptors.iter().filter(|d| d.flags == VALUES).count(),
            expected_buffers
        );
    }

    #[cfg_attr(test, test)]
    fn unsupported_types_and_timezone_strings_fail_explicitly() {
        let dictionary = DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![0]),
            Arc::new(StringArray::from(vec!["value"])),
        )
        .unwrap();
        let batch = RecordBatch::try_from_iter([(
            "dictionary",
            Arc::new(dictionary) as ArrayRef,
        )])
        .unwrap();
        let mut called = false;
        assert_eq!(
            visit_batch(&batch, 1, envelope(), |_| {
                called = true;
                true
            }),
            Err(Error::Unsupported)
        );
        assert!(!called);
        let batch = RecordBatch::try_from_iter([(
            "zoned",
            Arc::new(TimestampMicrosecondArray::new_null(1).with_timezone("UTC"))
                as ArrayRef,
        )])
        .unwrap();
        assert_eq!(
            visit_batch(&batch, 1, envelope(), |_| true),
            Err(Error::Unsupported)
        );
        assert_eq!(layout(&DataType::FixedSizeBinary(32)), Ok(21 | (32 << 16)));
    }

    #[cfg_attr(test, test)]
    fn zero_width_binary_keeps_row_and_validity_descriptors() {
        let batch = RecordBatch::try_from_iter([(
            "empty_bytes",
            Arc::new(FixedSizeBinaryArray::new_null(0, 3)) as ArrayRef,
        )])
        .unwrap();
        let descriptors = collect(&batch);
        assert!(descriptors.iter().all(|d| d.data_type == 21));
        let node = descriptors.iter().find(|d| d.flags == ARRAY).unwrap();
        assert_eq!((node.length, node.nulls, node.buffers), (3, 3, 2));
        let values = descriptors.iter().find(|d| d.flags == VALUES).unwrap();
        assert_eq!((values.capacity, values.length), (0, 0));
        let validity = descriptors
            .iter()
            .find(|d| d.flags == NULLS | BITS)
            .unwrap();
        assert_eq!(validity.length, 3);
        assert!(validity.capacity > 0);
    }

    #[cfg_attr(test, test)]
    fn quotas_and_callback_failure_never_report_success() {
        let batch = RecordBatch::try_from_iter([(
            "number",
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
        )])
        .unwrap();
        for limits in [
            Envelope {
                nodes: 0,
                ..envelope()
            },
            Envelope {
                depth: 0,
                ..envelope()
            },
            Envelope {
                buffers: 0,
                ..envelope()
            },
            Envelope {
                columns: 0,
                ..envelope()
            },
        ] {
            assert_eq!(visit_batch(&batch, 1, limits, |_| true), Err(Error::Quota));
        }
        assert_eq!(
            visit_batch(&batch, 1, envelope(), |_| false),
            Err(Error::Callback)
        );
    }
    pub(super) fn run_controls() {
        shared_slice_keeps_allocation_identity_and_visible_span();
        all_null_temporal_still_reports_values_and_validity_storage();
        boolean_slice_preserves_bit_span();
        nested_list_and_struct_keep_parent_links();
        views_report_all_dynamic_backings();
        unsupported_types_and_timezone_strings_fail_explicitly();
        zero_width_binary_keeps_row_and_validity_descriptors();
        quotas_and_callback_failure_never_report_success();
    }
}

/// Run borrowed layout controls through the workspace product test lane.
#[cfg(feature = "test_utils")]
#[doc(hidden)]
pub fn verify_controls() {
    tests::run_controls();
}
