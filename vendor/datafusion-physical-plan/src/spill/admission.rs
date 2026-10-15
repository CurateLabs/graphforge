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

//! Fallible IPC admission and the lifetime hook for immutable decoded buffers.
//!
//! Arrow's pool interface is infallible. It is used ONLY to attach a previously
//! admitted, fixed reservation to the original shared Bytes, never to admit an
//! allocation. Cloning/slicing an array consequently cannot shorten authority.
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use arrow::array::{Array, AsArray, BinaryViewArray, RecordBatch, StringViewArray};
use arrow::array::{downcast_dictionary_array, downcast_run_array};
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow_data::ArrayData;
use arrow_ipc::{CompressionType, MessageHeader};
use datafusion_common::{DataFusionError, Result, config::SpillCompression};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};

pub(crate) fn add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b).ok_or_else(overflow)
}
pub(crate) fn mul(a: usize, b: usize) -> Result<usize> {
    a.checked_mul(b).ok_or_else(overflow)
}
fn overflow() -> DataFusionError {
    DataFusionError::ResourcesExhausted("Spill IPC admission bound overflow".into())
}
fn nonnegative(value: i64) -> Result<usize> {
    usize::try_from(value).map_err(|_| overflow())
}
pub(crate) use super::credit::{Credit, ReaderPermit};
#[derive(Debug)]
pub(super) struct Budget {
    fresh: MemoryReservation,
    credit: Option<Arc<Credit>>,
    borrowed: AtomicUsize,
}
impl Budget {
    pub(super) fn size(&self) -> usize {
        self.fresh.size() + self.borrowed.load(Ordering::Relaxed)
    }
    pub(super) fn try_grow(&self, bytes: usize) -> Result<()> {
        add(self.size(), bytes)?;
        let borrowed = self.credit.as_ref().map_or(0, |credit| credit.take(bytes));
        if let Err(error) = self.fresh.try_grow(bytes - borrowed) {
            if let Some(credit) = &self.credit {
                credit.give(borrowed);
            }
            return Err(error);
        }
        self.borrowed.fetch_add(borrowed, Ordering::Relaxed);
        Ok(())
    }
    pub(super) fn split(&mut self, bytes: usize) -> Self {
        let borrowed = self.borrowed.load(Ordering::Relaxed).min(bytes);
        self.borrowed.fetch_sub(borrowed, Ordering::Relaxed);
        Self {
            fresh: self.fresh.split(bytes - borrowed),
            credit: self.credit.clone(),
            borrowed: AtomicUsize::new(borrowed),
        }
    }
    pub(super) fn try_resize(&self, bytes: usize) -> Result<()> {
        let size = self.size();
        if bytes >= size {
            return self.try_grow(bytes - size);
        }
        let shrink = size - bytes;
        let fresh = self.fresh.size().min(shrink);
        self.fresh.shrink(fresh);
        let borrowed = shrink - fresh;
        self.borrowed.fetch_sub(borrowed, Ordering::Relaxed);
        if let Some(credit) = &self.credit {
            credit.give(borrowed);
        }
        Ok(())
    }
}
impl Drop for Budget {
    fn drop(&mut self) {
        if let Some(credit) = &self.credit {
            credit.give(self.borrowed.load(Ordering::Relaxed));
        }
    }
}
pub(super) fn reserve(
    pool: &Arc<dyn MemoryPool>,
    bytes: usize,
    name: &str,
) -> Result<Budget> {
    reserve_with_credit(pool, bytes, name, None)
}
pub(super) fn reserve_with_credit(
    pool: &Arc<dyn MemoryPool>,
    bytes: usize,
    name: &str,
    credit: Option<Arc<Credit>>,
) -> Result<Budget> {
    let fresh = MemoryConsumer::new(name).register(pool);
    let borrowed = credit.as_ref().map_or(0, |credit| credit.take(bytes));
    let budget = Budget {
        fresh,
        borrowed: AtomicUsize::new(borrowed),
        credit,
    };
    budget.fresh.try_grow(bytes - borrowed)?;
    Ok(budget)
}

/// A FlatBuffer table/string/vector has less than 128 bytes of structural
/// overhead per corresponding Arrow schema object. Eight times Field::size
/// also covers offsets, padding, builder growth and decoded schema ownership;
/// the fixed 4096 covers the root tables and the initial builder allocation.
/// Schema metadata strings are included explicitly (Field::size includes field
/// metadata). This is admitted before to_data or IPC schema construction.
pub(crate) fn schema_bound(schema: &Schema) -> Result<usize> {
    let mut size = 4096;
    for field in schema.fields() {
        size = add(size, mul(field.size(), 8)?)?;
    }
    for (key, value) in schema.metadata() {
        size = add(size, mul(add(add(key.len(), value.len())?, 128)?, 8)?)?;
    }
    Ok(size)
}

pub(super) fn codec_bound(codec: SpillCompression, writing: bool) -> Result<usize> {
    match codec {
        SpillCompression::Uncompressed => Ok(0),
        // lz4_flex 0.13 FrameEncoder/Decoder: two 4 MiB block vectors,
        // compressBound overhead (n/255+16), hash table and frame structures.
        SpillCompression::Lz4Frame => {
            Ok(2 * 4 * 1024 * 1024 + 4 * 1024 * 1024 / 255 + 16 + 256 * 1024)
        }
        SpillCompression::Zstd => {
            // Arrow uses zstd's default level 3, single-threaded one-shot bulk
            // API. These upstream estimators are explicitly preallocation
            // bounds for that API and do not allocate a codec context.
            let size = unsafe {
                if writing {
                    zstd_sys::ZSTD_estimateCCtxSize(3)
                } else {
                    zstd_sys::ZSTD_estimateDCtxSize()
                }
            };
            if unsafe { zstd_sys::ZSTD_isError(size) } != 0 {
                return Err(overflow());
            }
            Ok(size)
        }
    }
}

#[derive(Default)]
struct LayoutBound {
    raw: usize,
    gc: usize,
    buffers: usize,
    headers: usize,
}
impl LayoutBound {
    fn visit(&mut self, data: &ArrayData, gc: bool) -> Result<()> {
        // ArrayData, concrete array, child/buffer Vecs, Arc/Bytes and claim
        // token boxes. 512 bytes/node and 256/buffer cover these concrete
        // Arrow 58.4 structures, including simultaneous to_data copies.
        self.headers = add(self.headers, 512)?;
        let mut buffer_memory = 0;
        for buffer in data.buffers() {
            buffer_memory = add(buffer_memory, buffer.capacity())?;
            self.raw = add(self.raw, buffer.capacity())?;
            self.buffers = add(self.buffers, 1)?;
        }
        let array_memory = add(
            buffer_memory,
            data.nulls().map_or(0, |nulls| nulls.buffer().capacity()),
        )?;
        if let Some(nulls) = data.nulls() {
            self.raw = add(self.raw, nulls.buffer().capacity())?;
            self.buffers = add(self.buffers, 1)?;
        } else if !matches!(
            data.data_type(),
            DataType::Null | DataType::Union(_, _) | DataType::RunEndEncoded(_, _)
        ) {
            // IPC V5 writer::has_validity_bitmap synthesizes all-valid bits,
            // even for zero-payload Struct<> / FixedSizeList<Null, 0>.
            let validity = add(data.len() / 8, usize::from(data.len() % 8 != 0))?;
            self.raw = add(self.raw, add(validity, 64)?)?;
            self.buffers = add(self.buffers, 1)?;
        }
        if gc && matches!(data.data_type(), DataType::Utf8View | DataType::BinaryView) {
            // gc() copies each referenced non-inline value, including repeated
            // views of the same bytes. Full source capacity alone is not a bound.
            let views = data.buffers()[0].typed_data::<u128>();
            let mut referenced = 0;
            for view in &views[data.offset()..add(data.offset(), data.len())?] {
                let len = (*view & 0xffff_ffff) as usize;
                if len > 12 {
                    referenced = add(referenced, len)?;
                }
            }
            let views_bytes = mul(data.len(), 16)?;
            let copied = add(referenced, views_bytes)?;
            self.gc = add(self.gc, copied)?;
            // Arrow's single-block GC branch needs at most these referenced
            // bytes and views. IPC sees those new buffers, not the original
            // allocator capacity. Original backing remains separately owned;
            // GC construction is still fully included above. Keep the old
            // bound for the multi-block branch and for arrays GC will skip.
            if referenced < i32::MAX as usize
                && super::should_gc_view_memory(
                    data.buffers().len() > 1,
                    array_memory,
                    views_bytes,
                )
            {
                self.raw = add(
                    self.raw.checked_sub(buffer_memory).ok_or_else(overflow)?,
                    copied,
                )?;
            }
            // The very-large gc path stores one group and buffer per value.
            self.headers = add(self.headers, mul(data.len(), 96)?)?;
        }
        for child in data.child_data() {
            self.visit(child, gc)?;
        }
        self.headers = add(self.headers, mul(data.buffers().len(), 256)?)?;
        Ok(())
    }
}

/// Bound temporary to_data metadata without first constructing ArrayData.
/// In particular a view's arbitrarily many data buffers are observable through
/// its concrete public accessor, even if all those buffers are empty.
pub(crate) fn source_metadata(batch: &RecordBatch) -> Result<usize> {
    batch
        .columns()
        .iter()
        .try_fold(schema_bound(&batch.schema())?, |size, array| {
            add(size, array_metadata(array.as_ref())?)
        })
}
pub(crate) fn array_metadata(array: &dyn Array) -> Result<usize> {
    let mut size = 2048;
    let child = match array.data_type() {
        DataType::Utf8View => {
            let buffers = array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .expect("validated Utf8View")
                .data_buffers();
            return add(size, mul(buffers.len(), 256)?);
        }
        DataType::BinaryView => {
            let buffers = array
                .as_any()
                .downcast_ref::<BinaryViewArray>()
                .expect("validated BinaryView")
                .data_buffers();
            return add(size, mul(buffers.len(), 256)?);
        }
        DataType::List(_) => Some(array.as_list::<i32>().values().as_ref()),
        DataType::LargeList(_) => Some(array.as_list::<i64>().values().as_ref()),
        DataType::ListView(_) => Some(array.as_list_view::<i32>().values().as_ref()),
        DataType::LargeListView(_) => Some(array.as_list_view::<i64>().values().as_ref()),
        DataType::FixedSizeList(_, _) => {
            Some(array.as_fixed_size_list().values().as_ref())
        }
        DataType::Map(_, _) => Some(array.as_map().entries() as &dyn Array),
        DataType::Dictionary(_, _) => {
            return downcast_dictionary_array!(array => {
            add(size, array_metadata(array.values().as_ref())?)
        }, _ => unreachable!("validated dictionary key"));
        }
        DataType::RunEndEncoded(_, _) => {
            return downcast_run_array!(array => {
            add(size, array_metadata(array.values().as_ref())?)
        }, _ => unreachable!("validated run-end type"));
        }
        DataType::Struct(_) => {
            for column in array.as_struct().columns() {
                size = add(size, array_metadata(column.as_ref())?)?;
            }
            return Ok(size);
        }
        DataType::Union(fields, _) => {
            for (id, _) in fields.iter() {
                size = add(size, array_metadata(array.as_union().child(id).as_ref())?)?;
            }
            return Ok(size);
        }
        _ => None,
    };
    if let Some(child) = child {
        size = add(size, array_metadata(child)?)?;
    }
    Ok(size)
}

/// The pinned uncompressed writer appends each payload and padding separately
/// to one initially empty Vec<u8>. Account reallocating old/new owners together
/// and temporary validity/rebased-offset buffers while those buffers are live.
#[derive(Default)]
struct WriteSequence {
    len: usize,
    capacity: usize,
    peak: usize,
}
impl WriteSequence {
    fn append(&mut self, bytes: usize, scratch: usize) -> Result<()> {
        self.peak = self.peak.max(add(self.capacity, scratch)?);
        let required = add(self.len, bytes)?;
        if required > self.capacity {
            let next = mul(self.capacity, 2)?.max(required).max(8);
            std::alloc::Layout::array::<u8>(next).map_err(|_| overflow())?;
            self.peak = self.peak.max(add(add(self.capacity, next)?, scratch)?);
            self.capacity = next;
        }
        self.len = required;
        Ok(())
    }
    fn buffer(&mut self, bytes: usize, scratch: usize) -> Result<()> {
        self.append(bytes, scratch)?;
        self.append((8 - bytes % 8) % 8, scratch)
    }
}
fn round_64(bytes: usize) -> Result<usize> {
    let capacity = mul(add(bytes, 63)? / 64, 64)?;
    std::alloc::Layout::array::<u8>(capacity).map_err(|_| overflow())?;
    Ok(capacity)
}
fn flat_write_peak(
    batch: &RecordBatch,
    declared: &Schema,
    codec: SpillCompression,
) -> Result<Option<usize>> {
    // Alignment is chosen from the declared writer schema, not from the
    // incoming batch schema (which may differ in field nullability).
    if codec != SpillCompression::Uncompressed
        || declared.fields().is_empty()
        || declared.fields().len() != batch.num_columns()
        || !declared
            .fields()
            .iter()
            .zip(batch.columns())
            .all(|(field, array)| {
                matches!(field.data_type(), DataType::Int64 | DataType::Utf8)
                    && field.data_type() == array.data_type()
            })
    {
        return Ok(None);
    }
    let rows = batch.num_rows();
    let validity = add(rows / 8, usize::from(rows % 8 != 0))?;
    let mut sequence = WriteSequence::default();
    for array in batch.columns() {
        // V5 writes validity even for all-valid fields. MutableBuffer's
        // alignment-rounded scratch also covers a shifted source bitmap.
        sequence.buffer(validity, round_64(validity)?)?;
        match array.data_type() {
            DataType::Int64 => sequence.buffer(mul(rows, 8)?, 0)?,
            DataType::Utf8 => {
                let strings = array.as_string::<i32>();
                let offsets = strings.value_offsets();
                let start = nonnegative(i64::from(offsets[0]))?;
                let end = nonnegative(i64::from(offsets[rows]))?;
                let values = end.checked_sub(start).ok_or_else(overflow)?;
                let offsets_bytes = mul(add(rows, 1)?, 4)?;
                let scratch = if rows == 0 {
                    64
                } else if start != 0 {
                    round_64(offsets_bytes)?
                } else {
                    0
                };
                sequence.buffer(offsets_bytes, scratch)?;
                // get_byte_array_buffers' rebased offsets are dropped at the
                // end of the offsets iteration, before the values append.
                sequence.buffer(values, 0)?;
            }
            _ => unreachable!(),
        }
    }
    sequence.append((8 - sequence.len % 8) % 8, 0)?;
    Ok(Some(sequence.peak))
}

pub(super) fn write_bound(
    batch: &RecordBatch,
    schema: usize,
    codec: SpillCompression,
    declared: &Schema,
) -> Result<usize> {
    let mut layout = LayoutBound::default();
    for column in batch.columns() {
        layout.visit(&column.to_data(), true)?;
    }
    if let Some(peak) = flat_write_peak(batch, declared, codec)? {
        return add(add(peak, layout.headers)?, schema);
    }
    let raw = add(layout.raw, layout.gc)?;
    let padding = mul(add(layout.buffers, 1)?, 128)?;
    // Encoded dictionaries and record body coexist. The encoder's append-only
    // Vec grows geometrically (<2x); compression has a temporary compressBound
    // output (<2x raw) before IPC's uncompressed fallback. GC is simultaneous.
    let encoded = mul(add(raw, padding)?, 4)?;
    add(add(add(encoded, layout.gc)?, layout.headers)?, schema)
}

#[derive(Debug)]
struct ClaimedReservation {
    size: usize,
    _authority: Arc<Budget>,
}
impl arrow_buffer::MemoryReservation for ClaimedReservation {
    fn size(&self) -> usize {
        self.size
    }
    fn resize(&mut self, new_size: usize) {
        // This hook tracks already-admitted immutable IPC bytes. Ownership
        // conversion does not expand the fixed authority or pretend to admit
        // a future mutable allocation; that allocation needs its operator's
        // normal fallible admission. In particular there is no resize panic.
        self.size = new_size;
    }
}
#[derive(Debug)]
struct Claims(Arc<Budget>);
impl arrow_buffer::MemoryPool for Claims {
    fn reserve(&self, size: usize) -> Box<dyn arrow_buffer::MemoryReservation> {
        Box::new(ClaimedReservation {
            size,
            _authority: Arc::clone(&self.0),
        })
    }
    fn available(&self) -> isize {
        0
    }
    fn used(&self) -> usize {
        self.0.size()
    }
    fn capacity(&self) -> usize {
        self.0.size()
    }
}

/// Reader-local roots pin only dictionary VALUES through the next decode and
/// reconciliation. Neither a ticket nor Credit can own Arrow data (no cycle).
#[derive(Default)]
pub(super) struct DictionarySources {
    values: Vec<ArrayData>,
    _metadata: Option<Budget>,
}
/// A private reader parses one schema and clones that exact Arc into each
/// unprojected batch. Keep its authority through the reader and last body token.
/// This owner never contains buffers, arrays, or producer permits.
#[derive(Debug)]
pub(super) struct SharedSchema {
    schema: SchemaRef,
    _authority: Budget,
}
impl SharedSchema {
    pub(super) fn bound(schema: &Schema) -> Result<usize> {
        add(
            schema_bound(schema)?,
            add(size_of::<Self>(), 2 * size_of::<usize>())?,
        )
    }
    pub(super) fn new(schema: SchemaRef, authority: Budget) -> Arc<Self> {
        Arc::new(Self {
            schema,
            _authority: authority,
        })
    }
    fn matches(&self, batch: &RecordBatch) -> bool {
        Arc::ptr_eq(&self.schema, batch.schema_ref())
    }
}
#[derive(Debug)]
struct BatchMetadata {
    _authority: Budget,
    _schema: Option<Arc<SharedSchema>>,
}
#[derive(Debug)]
struct OwnerClaims {
    payload: Arc<Budget>,
    metadata: Arc<BatchMetadata>,
}
#[derive(Debug)]
struct OwnerReservation {
    size: usize,
    _payload: Arc<Budget>,
    _metadata: Arc<BatchMetadata>,
}
impl arrow_buffer::MemoryReservation for OwnerReservation {
    fn size(&self) -> usize {
        self.size
    }
    fn resize(&mut self, size: usize) {
        self.size = size;
    }
}
impl arrow_buffer::MemoryPool for OwnerClaims {
    fn reserve(&self, size: usize) -> Box<dyn arrow_buffer::MemoryReservation> {
        Box::new(OwnerReservation {
            size,
            _payload: Arc::clone(&self.payload),
            _metadata: Arc::clone(&self.metadata),
        })
    }
    fn available(&self) -> isize {
        0
    }
    fn used(&self) -> usize {
        self.payload.size()
    }
    fn capacity(&self) -> usize {
        self.payload.size()
    }
}
fn table_bound(buffers: usize) -> Result<usize> {
    let buckets = if buffers == 0 {
        0
    } else {
        (add(mul(buffers, 8)?, 6)? / 7)
            .max(4)
            .checked_next_power_of_two()
            .ok_or_else(overflow)?
    };
    add(
        size_of::<HashMap<NonNull<u8>, usize>>(),
        add(
            mul(buckets, add(size_of::<(NonNull<u8>, usize)>(), 1)?)?,
            16,
        )?,
    )
}
fn dictionary_roots(data: &ArrayData) -> Result<usize> {
    if matches!(data.data_type(), DataType::Dictionary(_, _)) {
        // The VALUE subtree already contains every nested dictionary. Clone
        // each node at most once so snapshot metadata stays linear.
        return Ok(1);
    }
    let mut count = 0;
    for child in data.child_data() {
        count = add(count, dictionary_roots(child)?)?;
    }
    Ok(count)
}
fn pin_values(data: &ArrayData, values: &mut Vec<ArrayData>) {
    if matches!(data.data_type(), DataType::Dictionary(_, _)) {
        values.push(data.child_data()[0].clone());
        return;
    }
    for child in data.child_data() {
        pin_values(child, values);
    }
}
fn claim_owners(
    data: &ArrayData,
    owners: &mut HashMap<NonNull<u8>, usize>,
    work: &mut Budget,
    metadata: &Arc<BatchMetadata>,
) {
    let mut claim = |buffer: &arrow_buffer::Buffer| {
        if let Some(bytes) = owners.remove(&buffer.data_ptr()) {
            let payload = Arc::new(work.split(bytes));
            buffer.claim(&OwnerClaims {
                payload,
                metadata: Arc::clone(metadata),
            });
        }
    };
    for buffer in data.buffers() {
        claim(buffer);
    }
    if let Some(nulls) = data.nulls() {
        claim(nulls.buffer());
    }
    for child in data.child_data() {
        claim_owners(child, owners, work, metadata);
    }
}
/// Transfer each distinct full physical owner exactly once. Prior dictionary
/// value roots remain alive throughout this function, so matching bases cannot
/// be freed/reused, and their existing tickets must never be overwritten.
/// Ledgers, replacement roots, Arc/Box tokens and to_data metadata are covered
/// before allocation by the decoder's preadmitted metadata/source envelope.
pub(super) fn retain_batch(
    batch: &RecordBatch,
    work: &mut Budget,
    previous: &DictionarySources,
    schema: &Arc<SharedSchema>,
) -> Result<DictionarySources> {
    let mut layout = LayoutBound::default();
    let mut roots = 0;
    for column in batch.columns() {
        let data = column.to_data();
        layout.visit(&data, false)?;
        roots = add(roots, dictionary_roots(&data)?)?;
    }
    let mut prior = LayoutBound::default();
    for data in &previous.values {
        prior.visit(data, false)?;
    }
    let shared = schema.matches(batch);
    let metadata = add(
        layout.headers,
        add(size_of::<BatchMetadata>(), 2 * size_of::<usize>())?,
    )?;
    let metadata = if shared {
        metadata
    } else {
        add(metadata, schema_bound(&batch.schema())?)?
    };
    let snapshot = if roots == 0 {
        0
    } else {
        add(
            layout.headers,
            add(
                size_of::<DictionarySources>(),
                mul(roots, size_of::<ArrayData>())?,
            )?,
        )?
    };
    let scratch = add(
        add(metadata, snapshot)?,
        add(table_bound(layout.buffers)?, table_bound(prior.buffers)?)?,
    )?;
    if scratch > work.size() {
        return Err(overflow());
    }
    let mut owners = HashMap::new();
    let mut existing = HashMap::new();
    for (map, count) in [
        (&mut owners, layout.buffers),
        (&mut existing, prior.buffers),
    ] {
        map.try_reserve(count).map_err(|error| {
            DataFusionError::ResourcesExhausted(format!(
                "Spill owner ledger allocation failed: {error}"
            ))
        })?;
    }
    for data in &previous.values {
        record_owners(data, &mut existing);
    }
    for column in batch.columns() {
        record_owners(&column.to_data(), &mut owners);
    }
    for (base, capacity) in &owners {
        if existing.get(base).is_some_and(|old| old < capacity) {
            return Err(DataFusionError::Execution(
                "Immutable spill dictionary owner changed capacity".into(),
            ));
        }
    }
    owners.retain(|base, capacity| {
        *capacity != 0 && !existing.get(base).is_some_and(|old| old >= capacity)
    });
    let physical = owners.values().try_fold(0, |sum, bytes| add(sum, *bytes))?;
    if add(scratch, physical)? > work.size() {
        return Err(DataFusionError::Execution(
            "Spill owner transfer exceeds its admitted source bound".into(),
        ));
    }
    let mut next = DictionarySources::default();
    if roots != 0 {
        next._metadata = Some(work.split(snapshot));
        next.values.try_reserve_exact(roots).map_err(|error| {
            DataFusionError::ResourcesExhausted(format!(
                "Spill dictionary roots allocation failed: {error}"
            ))
        })?;
        for column in batch.columns() {
            pin_values(&column.to_data(), &mut next.values);
        }
    }
    let metadata = Arc::new(BatchMetadata {
        _authority: work.split(metadata),
        _schema: shared.then(|| Arc::clone(schema)),
    });
    for column in batch.columns() {
        claim_owners(&column.to_data(), &mut owners, work, &metadata);
    }
    debug_assert!(owners.is_empty());
    Ok(next)
}

/// Finite physical-owner shapes qualified for the first nonspilling bank.
/// Other schemas keep the existing full credit lifetime, without refusal.
pub(crate) fn ticket_schema(schema: &Schema) -> bool {
    fn supported(data_type: &DataType) -> bool {
        if data_type.primitive_width().is_some() {
            return true;
        }
        match data_type {
            DataType::Null
            | DataType::Boolean
            | DataType::FixedSizeBinary(_)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Utf8View
            | DataType::BinaryView => true,
            DataType::List(field) | DataType::LargeList(field) => {
                supported(field.data_type())
            }
            DataType::Struct(fields) => {
                fields.iter().all(|field| supported(field.data_type()))
            }
            DataType::Dictionary(key, value) => key.is_integer() && supported(value),
            _ => false,
        }
    }
    schema
        .fields()
        .iter()
        .all(|field| supported(field.data_type()))
}

/// Scan framing without changing the decoder's position. Each metadata Vec is
/// admitted before allocation. Body/decompression bounds are admitted before
/// Arrow allocates them. The ordinary Arrow decoder remains the sole codec.
/// Internally written IPC is the input contract, as for upstream skip_validation.
#[derive(Debug, Default)]
pub(super) struct FrameStats {
    wire_bytes: usize,
    metadata_bytes: usize,
    decoded_bytes: usize,
    buffers: usize,
}

/// Arrow58.4 MessageReader::maybe_next allocates exactly bodyLength bytes
/// with MutableBuffer::from_len_zeroed (Arrow ALIGNMENT), and uncompressed
/// read_buffer returns slices. For this checked flat layout, align_buffers
/// cannot allocate another body: Int64 requires 8, Utf8 offsets 4, bytes 1.
/// This is an allocation proof for the same private frame subsequently decoded,
/// not validation of arbitrary IPC values. Other layouts keep the old bound.
fn flat_body_matches(
    schema: &Schema,
    message: &arrow_ipc::Message<'_>,
    body: usize,
) -> Result<bool> {
    if message.version() != arrow_ipc::MetadataVersion::V5
        || message.header_type() != MessageHeader::RecordBatch
        || schema.fields().is_empty()
        || !schema
            .fields()
            .iter()
            .all(|field| matches!(field.data_type(), DataType::Int64 | DataType::Utf8))
    {
        return Ok(false);
    }
    let Some(batch) = message.header_as_record_batch() else {
        return Ok(false);
    };
    if batch.compression().is_some() || batch.variadicBufferCounts().is_some() {
        return Ok(false);
    }
    let (Some(nodes), Some(buffers)) = (batch.nodes(), batch.buffers()) else {
        return Ok(false);
    };
    let expected = schema.fields().iter().try_fold(0, |total, field| {
        add(
            total,
            if field.data_type() == &DataType::Int64 {
                2
            } else {
                3
            },
        )
    })?;
    if nodes.len() != schema.fields().len() || buffers.len() != expected {
        return Ok(false);
    }
    // from_len_zeroed unwraps this exact Layout before allocating.
    std::alloc::Layout::from_size_align(body, arrow_buffer::alloc::ALIGNMENT)
        .map_err(|_| overflow())?;
    let Ok(rows) = usize::try_from(batch.length()) else {
        return Ok(false);
    };
    let descriptor = |index: usize, minimum: usize| -> bool {
        let buffer = buffers.get(index);
        let (Ok(offset), Ok(len)) = (
            usize::try_from(buffer.offset()),
            usize::try_from(buffer.length()),
        ) else {
            return false;
        };
        offset % 8 == 0
            && len >= minimum
            && offset.checked_add(len).is_some_and(|end| end <= body)
    };
    let mut index = 0;
    for (field, node) in schema.fields().iter().zip(nodes) {
        let Ok(nulls) = usize::try_from(node.null_count()) else {
            return Ok(false);
        };
        if node.length() != batch.length() || nulls > rows {
            return Ok(false);
        }
        let bitmap = if nulls == 0 { 0 } else { add(rows, 7)? / 8 };
        if !descriptor(index, bitmap) {
            return Ok(false);
        }
        index += 1;
        match field.data_type() {
            DataType::Int64 => {
                if !descriptor(index, mul(rows, 8)?) {
                    return Ok(false);
                }
                index += 1;
            }
            DataType::Utf8 => {
                if !descriptor(index, mul(add(rows, 1)?, 4)?) || !descriptor(index + 1, 0)
                {
                    return Ok(false);
                }
                index += 2;
            }
            _ => unreachable!(),
        }
    }
    Ok(true)
}

pub(super) fn preflight(
    input: &mut BufReader<File>,
    workspace: &Budget,
    persistent: &Budget,
    cancelled: &AtomicBool,
    schema: &Schema,
    persistent_floor: usize,
) -> Result<FrameStats> {
    let position = input.stream_position()?;
    let result = scan(
        input,
        workspace,
        persistent,
        cancelled,
        schema,
        persistent_floor,
    );
    input.seek(SeekFrom::Start(position))?;
    result
}
fn scan(
    input: &mut BufReader<File>,
    work: &Budget,
    persistent: &Budget,
    cancelled: &AtomicBool,
    schema: &Schema,
    persistent_floor: usize,
) -> Result<FrameStats> {
    let mut stats = FrameStats::default();
    let mut ordinary = true;
    loop {
        if cancelled.load(Ordering::Acquire) {
            return Err(DataFusionError::Execution("Spill framing cancelled".into()));
        }
        let mut prefix = [0; 4];
        if let Err(error) = input.read_exact(&mut prefix) {
            if error.kind() == std::io::ErrorKind::UnexpectedEof {
                return Ok(stats);
            }
            return Err(error.into());
        }
        if prefix == [255; 4] {
            input.read_exact(&mut prefix)?;
        }
        let len = nonnegative(i64::from(i32::from_le_bytes(prefix)))?;
        if len == 0 {
            return Ok(stats);
        }
        // StreamReader metadata Vec may double its old high-water capacity.
        // This separate reservation follows the reader, including worker drop.
        let metadata = mul(len, 2)?;
        // Schema/BufReader authority cannot also pay this separately retained
        // MessageReader metadata Vec. Preserve its maximum through reader drop.
        persistent.try_resize(persistent.size().max(add(persistent_floor, metadata)?))?;
        work.try_grow(metadata)?;
        let mut bytes = vec![0; len];
        input.read_exact(&mut bytes)?;
        let message = arrow_ipc::root_as_message(&bytes).map_err(|error| {
            DataFusionError::Execution(format!("Invalid spill IPC metadata: {error}"))
        })?;
        let body = nonnegative(message.bodyLength())?;
        ordinary &= message.header_type() != MessageHeader::DictionaryBatch;
        stats.wire_bytes = add(stats.wire_bytes, body)?;
        stats.metadata_bytes = add(stats.metadata_bytes, len)?;
        let body_start = input.stream_position()?;
        let batch = match message.header_type() {
            MessageHeader::RecordBatch => message.header_as_record_batch(),
            MessageHeader::DictionaryBatch => message
                .header_as_dictionary_batch()
                .and_then(|batch| batch.data()),
            _ => None,
        };
        let mut decoded = 0;
        let mut headers = 4096;
        let mut codec = 0;
        if let Some(batch) = batch {
            if let Some(nodes) = batch.nodes() {
                headers = add(headers, mul(nodes.len(), 512)?)?;
            }
            if let Some(buffers) = batch.buffers() {
                stats.buffers = add(stats.buffers, buffers.len())?;
                headers = add(headers, mul(buffers.len(), 256)?)?;
                if let Some(compression) = batch.compression() {
                    let kind = match compression.codec() {
                        CompressionType::ZSTD => SpillCompression::Zstd,
                        CompressionType::LZ4_FRAME => SpillCompression::Lz4Frame,
                        _ => {
                            return Err(DataFusionError::Execution(
                                "Unknown spill IPC codec".into(),
                            ));
                        }
                    };
                    codec = codec_bound(kind, false)?;
                    for buffer in buffers {
                        let offset = nonnegative(buffer.offset())?;
                        let length = nonnegative(buffer.length())?;
                        if add(offset, length)? > body {
                            return Err(overflow());
                        }
                        if length == 0 {
                            continue;
                        }
                        if length < 8 {
                            return Err(overflow());
                        }
                        input.seek(SeekFrom::Start(
                            body_start.checked_add(offset as u64).ok_or_else(overflow)?,
                        ))?;
                        let mut size = [0; 8];
                        input.read_exact(&mut size)?;
                        let size = i64::from_le_bytes(size);
                        if size != -1 {
                            decoded = add(decoded, nonnegative(size)?)?;
                        }
                    }
                }
            }
        }
        // Wire body, decompressed buffers (Vec growth at most 2x on valid
        // frames), plus an aligned copy of all buffers when Arrow requires it.
        stats.decoded_bytes = add(stats.decoded_bytes, decoded)?;
        let payload = if ordinary && flat_body_matches(schema, &message, body)? {
            body
        } else {
            mul(add(body, mul(decoded, 2)?)?, 2)?
        };
        work.try_grow(add(add(payload, headers)?, codec)?)?;
        input.seek(SeekFrom::Start(
            body_start.checked_add(body as u64).ok_or_else(overflow)?,
        ))?;
        if message.header_type() != MessageHeader::DictionaryBatch {
            return Ok(stats);
        }
    }
}

pub(super) fn retained_bound(
    batch: &RecordBatch,
    admitted: usize,
    schema: &Arc<SharedSchema>,
) -> Result<usize> {
    let mut layout = LayoutBound::default();
    for column in batch.columns() {
        layout.visit(&column.to_data(), false)?;
    }
    let metadata = add(
        layout.headers,
        add(size_of::<BatchMetadata>(), 2 * size_of::<usize>())?,
    )?;
    let metadata = if schema.matches(batch) {
        metadata
    } else {
        add(metadata, schema_bound(&batch.schema())?)?
    };
    // HashMap's 7/8 load factor, power-of-two buckets, minimum four
    // buckets, and sixteen trailing control bytes. Admission covers the table
    // before allocation, including arbitrarily many view buffers. This is a
    // temporary batch-local ledger, never a runtime/global ownership cache.
    let buckets = if layout.buffers == 0 {
        0
    } else {
        (add(mul(layout.buffers, 8)?, 6)? / 7)
            .max(4)
            .checked_next_power_of_two()
            .ok_or_else(overflow)?
    };
    let table = add(
        size_of::<HashMap<NonNull<u8>, usize>>(),
        add(
            mul(buckets, add(size_of::<(NonNull<u8>, usize)>(), 1)?)?,
            16,
        )?,
    )?;
    if add(metadata, table)? > admitted {
        return Err(DataFusionError::ResourcesExhausted(
            "Spill owner ledger exceeds preadmitted metadata bound".into(),
        ));
    }
    let mut owners = HashMap::new();
    owners.try_reserve(layout.buffers).map_err(|error| {
        DataFusionError::ResourcesExhausted(format!(
            "Spill owner ledger allocation failed: {error}"
        ))
    })?;
    for column in batch.columns() {
        record_owners(&column.to_data(), &mut owners);
    }
    let physical = owners
        .values()
        .try_fold(0_usize, |total, capacity| add(total, *capacity))?;
    // Buffer::capacity is the complete owner capacity even for a one-row slice.
    // All buffers sliced from one IPC message share data_ptr and are charged
    // once. Different owners remain separate; inconsistent aliases use max.
    add(physical, metadata)
}
fn record_owners(data: &ArrayData, owners: &mut HashMap<NonNull<u8>, usize>) {
    let mut record = |buffer: &arrow_buffer::Buffer| {
        owners
            .entry(buffer.data_ptr())
            .and_modify(|capacity| {
                *capacity = (*capacity).max(buffer.capacity());
            })
            .or_insert(buffer.capacity());
    };
    for buffer in data.buffers() {
        record(buffer);
    }
    if let Some(nulls) = data.nulls() {
        record(nulls.buffer());
    }
    for child in data.child_data() {
        record_owners(child, owners);
    }
}

fn contains_dictionary(data: &ArrayData) -> bool {
    matches!(data.data_type(), DataType::Dictionary(_, _))
        || data.child_data().iter().any(contains_dictionary)
}
/// IPC DictionaryTracker::written retains the complete dictionary column's
/// ArrayData (including keys), not just dictionary values. Claim those exact
/// shared Bytes before passing them to the writer. No retained array is stored
/// in the authority itself, which would create a Bytes -> guard -> Bytes cycle.
pub(super) fn retain_dictionaries(
    batch: &RecordBatch,
    pool: &Arc<dyn MemoryPool>,
) -> Result<()> {
    let mut size = 0;
    for column in batch.columns() {
        if contains_dictionary(&column.to_data()) {
            size = add(size, column.get_array_memory_size())?;
        }
    }
    if size == 0 {
        return Ok(());
    }
    let reservation = reserve(
        pool,
        add(size, source_metadata(batch)?)?,
        "SpillWriterDictionaries",
    )?;
    let claims = Claims(Arc::new(reservation));
    for column in batch.columns() {
        if contains_dictionary(&column.to_data()) {
            column.claim(&claims);
        }
    }
    Ok(())
}

/// Full backing of any column containing dictionary values. This intentionally
/// includes its keys: it is a conservative source bound usable before the next
/// decode, whose reader may reuse dictionary values without a new IPC message.
pub(super) fn dictionary_bound(batch: &RecordBatch) -> Result<usize> {
    let mut size = 0;
    for column in batch.columns() {
        if contains_dictionary(&column.to_data()) {
            size = add(size, column.get_array_memory_size())?;
            size = add(size, array_metadata(column.as_ref())?)?;
        }
    }
    Ok(size)
}
