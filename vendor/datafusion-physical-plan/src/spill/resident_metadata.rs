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

//! Resident concrete-array metadata after private IPC or interleave construction.
//!
//! Callers must establish constructor provenance. In particular an arbitrary
//! StructArray can contain an unobservable over-capacity child Vec and cannot
//! use this bound. Schema/datatype heaps and buffer/control grants are separate.
use std::alloc::Layout;
use std::mem::size_of;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};

use arrow::array::{
    Array, ArrayRef, LargeListArray, ListArray, RecordBatch, StructArray,
};
use arrow::datatypes::DataType;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryPool};

#[derive(Default)]
pub(crate) struct ResidentMetadata {
    pub(crate) headers: usize,
    pub(crate) buffers: usize,
}

/// Pinned Rust ArcInner: two atomic counters, aligned data, final padding.
pub(crate) fn arc_bytes(value: Layout) -> Option<usize> {
    Some(
        Layout::new::<[AtomicUsize; 2]>()
            .extend(value)
            .ok()?
            .0
            .pad_to_align()
            .size(),
    )
}

/// Extra heap of the pinned std mutex implementation. Apple LP64
/// pthread_mutex_t is a long signature followed by 56 opaque bytes; std's
/// pthread adapter owns it through one OnceBox. Other unproved targets keep
/// their existing complete metadata/control fees.
pub(crate) fn std_mutex_heap_bound() -> Option<usize> {
    if cfg!(any(
        target_os = "linux",
        all(target_os = "windows", not(target_vendor = "win7")),
    )) {
        Some(0)
    } else if cfg!(all(target_os = "macos", target_pointer_width = "64")) {
        Some(
            Layout::new::<std::ffi::c_long>()
                .extend(Layout::array::<u8>(56).ok()?)
                .ok()?
                .0
                .pad_to_align()
                .size(),
        )
    } else {
        None
    }
}

/// Pinned Arrow Bytes owns pointer + len + Deallocation (three usize words)
/// and this exact reservation mutex. Include its Arc and any known mutex heap.
/// Private IPC/fresh interleave claims initialize it on one thread BEFORE
/// publication; concurrent first-lock race allocations do not enter this proof.
/// These allocations have no external deallocator side owner.
pub(crate) fn raw_bytes_arc_bound() -> Option<usize> {
    if !matches!(usize::BITS, 32 | 64) {
        return None;
    }
    let mutex_heap = std_mutex_heap_bound()?;
    let bytes = Layout::array::<usize>(5)
        .ok()?
        .extend(Layout::new::<
            Mutex<Option<Box<dyn arrow_buffer::MemoryReservation>>>,
        >())
        .ok()?
        .0
        .pad_to_align();
    arc_bytes(bytes)?.checked_add(mutex_heap)
}

/// Pinned SharedRegistration is an Arc containing the pool Arc and the
/// public MemoryConsumer value. A freshly constructed literal-name consumer
/// owns an exact-length String; arbitrary existing consumer names do not
/// establish that capacity provenance. Splits share this registration.
pub(crate) fn literal_registration_bytes(name: &str) -> Option<usize> {
    let registration = Layout::new::<Arc<dyn MemoryPool>>()
        .extend(Layout::new::<MemoryConsumer>())
        .ok()?
        .0
        .pad_to_align();
    arc_bytes(registration)?.checked_add(name.len())
}

fn pushed_vec_bytes<T>(len: usize) -> Option<usize> {
    let capacity = if len == 0 {
        0
    } else {
        len.checked_mul(2)?.max(4)
    };
    Some(Layout::array::<T>(capacity).ok()?.size())
}

fn visit(array: &dyn Array, metadata: &mut ResidentMetadata, depth: usize) -> Option<()> {
    if depth >= 64 {
        return None;
    }
    let next = depth + 1;
    let data_buffers = match array.data_type() {
        DataType::List(_) => {
            let list = array.as_any().downcast_ref::<ListArray>()?;
            visit(list.values().as_ref(), metadata, next)?;
            1
        }
        DataType::LargeList(_) => {
            let list = array.as_any().downcast_ref::<LargeListArray>()?;
            visit(list.values().as_ref(), metadata, next)?;
            1
        }
        DataType::Struct(_) => {
            let structure = array.as_any().downcast_ref::<StructArray>()?;
            // IPC reader pushes children from vec![]; interleave collects a
            // fallible iterator. Both have capacity <=max(2*n,4), not just n.
            metadata.headers =
                metadata.headers.checked_add(pushed_vec_bytes::<ArrayRef>(
                    structure.num_columns(),
                )?)?;
            for child in structure.columns() {
                visit(child.as_ref(), metadata, next)?;
            }
            0
        }
        DataType::Null => 0,
        DataType::Binary
        | DataType::LargeBinary
        | DataType::Utf8
        | DataType::LargeUtf8 => 2,
        DataType::Boolean
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
        | DataType::Timestamp(_, _)
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Interval(_)
        | DataType::FixedSizeBinary(_)
        | DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => 1,
        // Keep all existing fees for dictionary, view and other unproved
        // representations, including an unsupported descendant.
        _ => return None,
    };
    metadata.headers = metadata
        .headers
        .checked_add(arc_bytes(Layout::for_value(array))?)?;
    metadata.buffers = metadata
        .buffers
        .checked_add(data_buffers)?
        .checked_add(usize::from(array.nulls().is_some()))?;
    Some(())
}

/// Includes the future RecordBatch value/root Vec as well as its array tree.
/// Only private IPC or this engine's interleave output constructors qualify.
/// This walks borrowed accessors; no ArrayData or other allocation is created.
pub(crate) fn columns(columns: &[ArrayRef]) -> Option<ResidentMetadata> {
    let mut metadata = ResidentMetadata {
        headers: size_of::<RecordBatch>()
            .checked_add(pushed_vec_bytes::<ArrayRef>(columns.len())?)?,
        buffers: 0,
    };
    for array in columns {
        visit(array.as_ref(), &mut metadata, 0)?;
    }
    Some(metadata)
}

pub(crate) fn batch(batch: &RecordBatch) -> Option<ResidentMetadata> {
    columns(batch.columns())
}
