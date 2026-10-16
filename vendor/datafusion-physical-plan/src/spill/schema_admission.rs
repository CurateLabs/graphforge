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

//! Resident ownership of schemas freshly parsed by the private IPC reader.
//!
//! Construction remains covered by admission::schema_bound. This traversal is
//! used only after unprojected StreamReader::try_new has finished conversion;
//! arbitrary externally supplied schemas do not have fresh-map provenance.
use std::alloc::Layout;
use std::collections::HashMap;
use std::mem::{align_of, size_of};
use std::sync::atomic::AtomicUsize;

use arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema};

use super::admission::SharedSchema;

fn add(a: usize, b: usize) -> Option<usize> {
    a.checked_add(b)
}

/// Rust 1.96 ArcInner is the two atomic counters followed by aligned data.
/// Its allocator extends this header by the value Layout and pads to alignment.
fn arc_bytes(value: Layout) -> Option<usize> {
    Some(
        Layout::new::<[AtomicUsize; 2]>()
            .extend(value)
            .ok()?
            .0
            .pad_to_align()
            .size(),
    )
}

fn arc_slice_bytes<T>(len: usize) -> Option<usize> {
    arc_bytes(Layout::array::<T>(len).ok()?)
}

/// Pinned IPC fb_to_schema makes these maps with default + insert, never
/// remove. Therefore capacity is the full usable bucket capacity. This would
/// not hold for arbitrary maps: tombstones can hide allocated buckets.
fn fresh_metadata_heap(map: &HashMap<String, String>) -> Option<usize> {
    if !matches!(usize::BITS, 32 | 64) {
        return None;
    }
    let capacity = map.capacity();
    let mut total = if capacity == 0 {
        if !map.is_empty() {
            return None;
        }
        0
    } else {
        // Rust 1.96 std uses hashbrown 0.16.1. Small tables have one empty
        // bucket, larger tables reserve 1/8 of buckets for empty controls.
        let buckets = match capacity {
            3 => 4,
            7 => 8,
            n if n >= 14 && n % 7 == 0 => (n / 7).checked_mul(8)?,
            _ => return None,
        };
        if !buckets.is_power_of_two() || buckets < 4 {
            return None;
        }
        let usable = if buckets <= 8 {
            buckets - 1
        } else {
            buckets / 8 * 7
        };
        if usable != capacity || map.len() > capacity {
            return None;
        }
        // All pinned hashbrown groups on supported 32/64-bit targets have
        // width <=16. Include bucket padding, every control byte, and the
        // trailing group; String headers reside in these actual buckets.
        let align = align_of::<(String, String)>().max(16);
        let payload = size_of::<(String, String)>().checked_mul(buckets)?;
        let control_offset = payload.checked_add(align - 1)? & !(align - 1);
        let bytes = control_offset.checked_add(buckets)?.checked_add(16)?;
        Layout::from_size_align(bytes, align).ok()?.size()
    };
    for (key, value) in map {
        total = add(total, add(key.capacity(), value.capacity())?)?;
    }
    Some(total)
}

fn field_owner(field: &Field, depth: usize) -> Option<usize> {
    let inline = arc_bytes(Layout::new::<Field>())?;
    let heaps = add(
        field.name().capacity(),
        fresh_metadata_heap(field.metadata())?,
    )?;
    add(
        add(inline, heaps)?,
        datatype_heap(field.data_type(), depth)?,
    )
}

fn fields_heap(fields: &Fields, depth: usize) -> Option<usize> {
    // Fields is Arc<[FieldRef]>. Even an empty Arc slice has its control header.
    let mut total = arc_slice_bytes::<FieldRef>(fields.len())?;
    for field in fields {
        total = add(total, field_owner(field, depth)?)?;
    }
    Some(total)
}

fn datatype_heap(data_type: &DataType, depth: usize) -> Option<usize> {
    // Proof eligibility only. Deeper/other representations keep the complete
    // construction grant; neither decoder acceptance nor fallback changes.
    if depth >= 64 {
        return None;
    }
    let next = depth + 1;
    match data_type {
        // IPC converts the borrowed timezone to a fresh exact-length Arc<str>.
        DataType::Timestamp(_, Some(zone)) => arc_slice_bytes::<u8>(zone.len()),
        DataType::Timestamp(_, None) => Some(0),
        DataType::List(field) | DataType::LargeList(field) => field_owner(field, next),
        DataType::Struct(fields) => fields_heap(fields, next),
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Interval(_)
        | DataType::Binary
        | DataType::FixedSizeBinary(_)
        | DataType::LargeBinary
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => Some(0),
        _ => None,
    }
}

/// Includes the fresh parsed Schema/Fields/Field trees and SharedSchema Arc.
/// The private caller must preserve fresh IPC provenance, not just type equality.
/// Counting each FieldRef occurrence is conservative if ownership is shared.
pub(super) fn fresh_resident_schema_bound(schema: &Schema) -> Option<usize> {
    let root = arc_bytes(Layout::new::<Schema>())?;
    let tree = fields_heap(schema.fields(), 0)?;
    let metadata = fresh_metadata_heap(schema.metadata())?;
    let owner = arc_bytes(Layout::new::<SharedSchema>())?;
    // The split Budget can be the last owner of this reader registration and
    // its literal-name String after the persistent reader grant is dropped.
    let registration =
        super::resident_metadata::literal_registration_bytes("SpillReaderMetadata")?;
    add(add(root, tree)?, add(metadata, add(owner, registration)?)?)
}
