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

use crate::terminal_ownership as od;
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
// Exact pre-observation layout: reservation arithmetic must never charge
// numeric diagnostic fields, including when this owner is embedded in metadata.
#[allow(dead_code)]
struct ChargedBudget {
    fresh: MemoryReservation,
    credit: Option<Arc<Credit>>,
    borrowed: AtomicUsize,
}
#[derive(Debug)]
pub(super) struct Budget {
    fresh: MemoryReservation,
    credit: Option<Arc<Credit>>,
    borrowed: AtomicUsize,
    diagnostic_piece: od::Piece,
    diagnostic: od::Life,
}
impl Budget {
    pub(super) fn diagnostic_id(&self) -> u64 {
        self.diagnostic.id()
    }
    fn observe(&self) {
        let fresh = self.fresh.size();
        let borrowed = self.borrowed.load(Ordering::Relaxed);
        let credit = self.credit.as_ref().map_or(0, |c| c.diagnostic_id());
        let Some(held) = fresh.checked_add(borrowed) else {
            od::gap(od::GAP_UNKNOWN_SPONSOR);
            return;
        };
        self.diagnostic_piece
            .refresh(&self.fresh, self.diagnostic.id());
        self.diagnostic.update(|o| {
            o.piece = self.diagnostic_piece.id();
            o.held = held as u64;
            o.a = fresh as u64;
            o.b = borrowed as u64;
            o.c = credit;
        });
        if credit != 0 {
            self.diagnostic.edge(credit, 8, borrowed);
        }
    }
    pub(super) fn size(&self) -> usize {
        self.fresh.size() + self.borrowed.load(Ordering::Relaxed)
    }
    pub(super) fn try_grow(&self, bytes: usize) -> Result<()> {
        let _transition = od::Transition::begin();
        add(self.size(), bytes)?;
        let (borrowed, may_ask) = self
            .credit
            .as_ref()
            .map_or((0, true), |credit| credit.take(bytes));
        let result = if borrowed < bytes && !may_ask {
            Err(self.beyond_credit(bytes - borrowed))
        } else {
            self.fresh.try_grow(bytes - borrowed)
        };
        if let Err(error) = result {
            if let Some(credit) = &self.credit {
                credit.give(borrowed);
            }
            self.observe();
            return Err(error);
        }
        self.borrowed.fetch_add(borrowed, Ordering::Relaxed);
        self.observe();
        Ok(())
    }
    fn beyond_credit(&self, short: usize) -> DataFusionError {
        beyond_credit(self.fresh.consumer().name(), short)
    }
    pub(super) fn split(&mut self, bytes: usize) -> Self {
        let _transition = od::Transition::begin();
        let borrowed = self.borrowed.load(Ordering::Relaxed).min(bytes);
        self.borrowed.fetch_sub(borrowed, Ordering::Relaxed);
        let mut next = Self {
            fresh: self.fresh.split(bytes - borrowed),
            credit: self.credit.clone(),
            borrowed: AtomicUsize::new(borrowed),
            diagnostic_piece: od::Piece::default(),
            diagnostic: od::Life::inherited(od::Kind::Budget),
        };
        next.diagnostic_piece = self.diagnostic_piece.child(
            &next.fresh,
            next.diagnostic.id(),
            bytes - borrowed,
        );
        self.observe();
        next.observe();
        next
    }
    pub(super) fn try_resize(&self, bytes: usize) -> Result<()> {
        let size = self.size();
        if bytes >= size {
            return self.try_grow(bytes - size);
        }
        self.shrink_to(bytes);
        Ok(())
    }
    /// Move a fresh nonspilling workspace into its next owned phase without
    /// releasing bytes or changing consumer registration/sponsor eligibility.
    fn owned_authority_eligible(&self) -> Result<()> {
        if self.credit.is_some()
            || self.borrowed.load(Ordering::Relaxed) != 0
            || self.fresh.consumer().can_spill()
        {
            return Err(DataFusionError::Internal(
                "Owned spill authority requires a fresh nonspilling workspace".into(),
            ));
        }
        Ok(())
    }
    #[allow(dead_code)]
    pub(super) fn take_owned_authority(&mut self) -> Result<MemoryReservation> {
        let _transition = od::Transition::begin();
        self.owned_authority_eligible()?;
        let authority = self.fresh.take();
        self.observe();
        // A bare return cannot carry the numeric piece through an arbitrary
        // future caller. Qualified callers use the synchronous wrapper below.
        od::gap(od::GAP_UNKNOWN_SPONSOR);
        Ok(authority)
    }
    pub(super) fn with_owned_authority<R>(
        &mut self,
        next: impl FnOnce(MemoryReservation) -> Result<R>,
    ) -> Result<R> {
        let _transition = od::Transition::begin();
        self.owned_authority_eligible()?;
        let authority = self.fresh.take();
        let moving = std::mem::take(&mut self.diagnostic_piece);
        self.diagnostic_piece = od::scoped(self.diagnostic.id(), moving.id(), || {
            od::Piece::new(&self.fresh, self.diagnostic.id())
        });
        self.observe();
        moving.transfer(self.diagnostic.id(), || next(authority))
    }
    /// Return only excess existing authority; never allocate or readmit.
    fn shrink_to(&self, bytes: usize) {
        let _transition = od::Transition::begin();
        let size = self.size();
        if bytes >= size {
            return;
        }
        let shrink = size - bytes;
        let fresh = self.fresh.size().min(shrink);
        self.fresh.shrink(fresh);
        let borrowed = shrink - fresh;
        self.borrowed.fetch_sub(borrowed, Ordering::Relaxed);
        if let Some(credit) = &self.credit {
            credit.give(borrowed);
        }
        self.observe();
    }
}
impl Drop for Budget {
    fn drop(&mut self) {
        self.diagnostic.begin_drop();
        if let Some(credit) = &self.credit {
            credit.give(self.borrowed.load(Ordering::Relaxed));
        }
    }
}
/// A borrower of an exact bank needed `short` bytes more than the bank held.
fn beyond_credit(name: &str, short: usize) -> DataFusionError {
    let kind = if name.starts_with("SpillWriter") {
        "writer"
    } else {
        "reader"
    };
    DataFusionError::ResourcesExhausted(format!(
        "spill {kind} exceeded its admitted credit: {name} needed {short} more bytes"
    ))
}
/// A registered consumer holding nothing, which draws on `credit` when it grows.
pub(super) fn register(
    pool: &Arc<dyn MemoryPool>,
    name: &str,
    credit: Option<Arc<Credit>>,
) -> Budget {
    let _transition = od::Transition::begin();
    let fresh = MemoryConsumer::new(name).register(pool);
    let diagnostic = od::Life::inherited(od::Kind::Budget);
    let diagnostic_piece = od::Piece::new(&fresh, diagnostic.id());
    let budget = Budget {
        fresh,
        borrowed: AtomicUsize::new(0),
        credit,
        diagnostic_piece,
        diagnostic,
    };
    budget.observe();
    budget
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
    let _transition = od::Transition::begin();
    let fresh = MemoryConsumer::new(name).register(pool);
    let (borrowed, may_ask) = credit
        .as_ref()
        .map_or((0, true), |credit| credit.take(bytes));
    let diagnostic = od::Life::inherited(od::Kind::Budget);
    let diagnostic_piece = od::Piece::new(&fresh, diagnostic.id());
    let budget = Budget {
        fresh,
        borrowed: AtomicUsize::new(borrowed),
        credit,
        diagnostic_piece,
        diagnostic,
    };
    budget.observe();
    let result = if borrowed < bytes && !may_ask {
        Err(budget.beyond_credit(bytes - borrowed))
    } else {
        budget.fresh.try_grow(bytes - borrowed)
    };
    budget.observe();
    result?;
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

/// The IPC nodes and buffers one record batch of `schema` carries, when that
/// count does not depend on the data: flat fixed-width and binary columns.
/// Nested columns (the value count of a child is not derivable from the bytes
/// of the batch), views (any number of data buffers), dictionaries (separate
/// dictionary messages), unions, run-end and the other layouts have no such
/// count, so a merge of them keeps its original, unbounded reader admission.
fn ipc_shape(data_type: &DataType) -> Option<(usize, usize)> {
    match data_type {
        DataType::Null => Some((1, 0)),
        DataType::Boolean | DataType::FixedSizeBinary(_) => Some((1, 2)),
        data_type if data_type.primitive_width().is_some() => Some((1, 2)),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary => {
            Some((1, 3))
        }
        _ => None,
    }
}
fn schema_shape(schema: &Schema) -> Option<(usize, usize)> {
    schema.fields().iter().try_fold((0, 0), |(nodes, buffers), field| {
        let (n, b) = ipc_shape(field.data_type())?;
        Some((nodes + n, buffers + b))
    })
}
/// Layouts whose uncompressed IPC body the decoder aliases without copying
/// (see `aligned_body_matches`): its payload admission is the body alone.
fn aliased_layout(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int64
            | DataType::UInt64
            | DataType::UInt32
            | DataType::FixedSizeBinary(16)
            | DataType::Timestamp(_, _)
            | DataType::Utf8
    )
}
/// Whether a spill file of `schema` has reader and writer bounds derivable
/// from its largest batch alone. A merge of any other schema is not admitted
/// to an exact bank (see [`Credit::new`]).
pub(crate) fn bounded_schema(schema: &Schema) -> bool {
    !schema.fields().is_empty() && schema_shape(schema).is_some()
}
/// The IPC body of a batch whose sliced memory is `max` and whose columns hold
/// at most `rows` values: each buffer is padded to its 64-byte alignment and a
/// column without nulls still writes a validity bitmap (V5).
fn body_bound(schema: &Schema, max: usize, rows: usize) -> Result<usize> {
    let (nodes, buffers) = schema_shape(schema).ok_or_else(overflow)?;
    add(
        max,
        add(mul(buffers, 64)?, mul(nodes, add(rows / 8, 64)?)?)?,
    )
}
/// Bytes the decoder of a spill file admits for its largest batch, from the
/// arithmetic of `scan` and the decode that follows it: the fixed schema
/// grant, the message-metadata Vec (at most doubling a metadata length that
/// the headers allowance bounds), the body or its decompressed copies, the
/// header allowance and the codec.
fn reader_batch_bound(
    schema: &Schema,
    max: usize,
    rows: usize,
    codec: SpillCompression,
) -> Result<usize> {
    let (nodes, buffers) = schema_shape(schema).ok_or_else(overflow)?;
    let body = body_bound(schema, max, rows)?;
    let headers = add(add(4096, mul(nodes, 512)?)?, mul(buffers, 256)?)?;
    let payload = if codec != SpillCompression::Uncompressed {
        // (body + 2 * decoded) * 2, with the compressed body at most the
        // decoded bytes and a length word per buffer.
        mul(add(mul(body, 3)?, mul(buffers, 24)?)?, 2)?
    } else if schema.fields().iter().all(|f| aliased_layout(f.data_type())) {
        body
    } else {
        mul(body, 2)?
    };
    let codec_bytes = if codec == SpillCompression::Uncompressed {
        0
    } else {
        codec_bound(codec, false)?
    };
    add(
        add(schema_bound(schema)?, mul(headers, 2)?)?,
        add(add(payload, headers)?, codec_bytes)?,
    )
}
/// Bytes a reader of a spill file keeps for as long as it lives: the shared
/// schema owner, the `BufReader`, and the message-metadata Vec of its longest
/// message.
fn reader_fixed_bound(schema: &Schema) -> Result<usize> {
    let (nodes, buffers) = schema_shape(schema).ok_or_else(overflow)?;
    let headers = add(add(4096, mul(nodes, 512)?)?, mul(buffers, 256)?)?;
    add(
        SharedSchema::bound(schema)?,
        add(8192, mul(headers.max(schema_bound(schema)?), 2)?)?,
    )
}
/// Every byte one reader of a spill file whose largest batch is `max` can hold
/// alive at once, with `buffer_len` batches buffered. One reader keeps
/// `buffer_len` batches in its channel, one in flight in its producer (which
/// polls the next decode only after sending), and the merge keeps its current
/// batch and one earlier batch whose rows are still pending in the output.
/// `None` for a schema without a derivable bound.
pub fn reader_pass_bound(
    schema: &Schema,
    max: usize,
    rows: usize,
    codec: SpillCompression,
    buffer_len: usize,
) -> Result<Option<usize>> {
    if !bounded_schema(schema) {
        return Ok(None);
    }
    add(
        reader_fixed_bound(schema)?,
        mul(add(buffer_len, 3)?, reader_batch_bound(schema, max, rows, codec)?)?,
    )
    .map(Some)
}
/// Every byte the writer of an intermediate spill file takes for an output
/// batch that is no larger than `max`: the source metadata, the encoding
/// workspace, and the codec. The writer holds the larger of the metadata and
/// the workspace, then the codec beside it. `None` for a schema without a
/// derivable bound.
pub fn writer_pass_bound(
    schema: &Schema,
    max: usize,
    rows: usize,
    codec: SpillCompression,
) -> Result<Option<usize>> {
    let Some((nodes, buffers)) = schema_shape(schema) else {
        return Ok(None);
    };
    if schema.fields().is_empty() {
        return Ok(None);
    }
    let body = body_bound(schema, max, rows)?;
    let headers = add(add(4096, mul(nodes, 512)?)?, mul(buffers, 256)?)?;
    let schema_bytes = schema_bound(schema)?;
    let flat = codec == SpillCompression::Uncompressed
        && schema
            .fields()
            .iter()
            .all(|f| matches!(f.data_type(), DataType::Int64 | DataType::Utf8));
    let workspace = if flat {
        // `flat_write_peak`: the Vec grows to `max(2 * capacity, required)`
        // and holds its old and new capacity while it does, so its peak is
        // under three times the final length (a length just past a power of
        // two, or a last padding byte after a jump to the required length),
        // beside a rebased-offsets or validity scratch.
        let scratch = round_64(add(mul(add(rows, 1)?, 4)?, 64)?)?;
        add(add(mul(body, 3)?, scratch)?, add(headers, schema_bytes)?)?
    } else {
        // `write_bound`: the append-only encoder Vec grows geometrically, a
        // compressed body has a temporary output of up to twice its raw size.
        let padding = mul(add(buffers, 1)?, 128)?;
        add(
            add(mul(add(body, padding)?, 4)?, headers)?,
            schema_bytes,
        )?
    };
    let metadata = add(schema_bytes, mul(nodes, 2048)?)?;
    add(workspace.max(metadata), codec_bound(codec, true)?).map(Some)
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

#[allow(dead_code)]
struct ChargedClaimedReservation {
    size: usize,
    _authority: Arc<Budget>,
}
#[derive(Debug)]
struct ClaimedReservation {
    size: usize,
    _authority: Arc<Budget>,
    diagnostic: od::Life,
}
impl Drop for ClaimedReservation {
    fn drop(&mut self) {
        self.diagnostic.begin_drop();
    }
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
        if self.size != new_size {
            od::gap(od::GAP_UNSUPPORTED);
        }
        self.size = new_size;
        self.diagnostic.update(|o| o.bytes = new_size as u64);
    }
}
#[derive(Debug)]
struct Claims(Arc<Budget>);
impl arrow_buffer::MemoryPool for Claims {
    fn reserve(&self, size: usize) -> Box<dyn arrow_buffer::MemoryReservation> {
        let _transition = od::Transition::begin();
        let diagnostic = od::Life::new(od::Kind::Claim, self.0.diagnostic_id(), 0);
        diagnostic.update(|o| o.bytes = size as u64);
        diagnostic.edge(self.0.diagnostic_id(), 10, size);
        // Arrow's generic callback has no borrowed buffer identity. Never
        // manufacture a concrete owner certificate from its scalar size.
        od::gap(od::GAP_UNSUPPORTED);
        Box::new(ClaimedReservation {
            size,
            _authority: Arc::clone(&self.0),
            diagnostic,
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
#[allow(dead_code)]
struct ChargedDictionarySources {
    values: Vec<ArrayData>,
    metadata: Option<ChargedBudget>,
}
#[derive(Default)]
pub(super) struct DictionarySources {
    values: Vec<ArrayData>,
    metadata: Option<Budget>,
}
/// A private reader parses one schema and clones that exact Arc into each
/// unprojected batch. Keep its authority through the reader and last body token.
/// This owner never contains buffers, arrays, or producer permits.
#[allow(dead_code)]
struct ChargedSharedSchema {
    schema: SchemaRef,
    authority: ChargedBudget,
}
#[derive(Debug)]
pub(super) struct SharedSchema {
    schema: SchemaRef,
    authority: Budget,
    diagnostic: od::Life,
}
impl Drop for SharedSchema {
    fn drop(&mut self) {
        self.diagnostic.begin_drop();
    }
}
impl SharedSchema {
    pub(super) fn charged_layout() -> std::alloc::Layout {
        std::alloc::Layout::new::<ChargedSharedSchema>()
    }
    pub(super) fn bound(schema: &Schema) -> Result<usize> {
        add(
            schema_bound(schema)?,
            add(size_of::<ChargedSharedSchema>(), 2 * size_of::<usize>())?,
        )
    }
    /// Called only for the fresh schema returned by our private unprojected
    /// StreamReader. Conversion temporaries have ended; MessageReader's buffer
    /// and BufReader remain separately funded by the reader's persistent grant.
    pub(super) fn new(schema: SchemaRef, authority: Budget) -> Arc<Self> {
        let _transition = od::Transition::begin();
        let resident = super::schema_admission::fresh_resident_schema_bound(&schema);
        // Allocate this owner while complete construction authority is live.
        // Its schema drops before authority, including on the final body token.
        let diagnostic =
            od::Life::new(od::Kind::BatchMetadata, authority.diagnostic_id(), 0);
        diagnostic.update(|o| {
            o.a = 2;
            o.flags = 4;
        });
        let owner = Arc::new(Self {
            schema,
            authority,
            diagnostic,
        });
        if let Some(bytes) = resident {
            // Unknown/overflowing representations keep the old grant. A bound
            // above the construction grant cannot trigger post-parse admission.
            owner.authority.shrink_to(bytes);
        }
        let bytes = owner.authority.size();
        owner.diagnostic.update(|o| o.bytes = bytes as u64);
        owner
            .diagnostic
            .edge(owner.authority.diagnostic_id(), 10, bytes);
        owner
    }
    fn matches(&self, batch: &RecordBatch) -> bool {
        Arc::ptr_eq(&self.schema, batch.schema_ref())
    }
}
#[allow(dead_code)]
struct ChargedBatchMetadata {
    _authority: ChargedBudget,
    _schema: Option<Arc<SharedSchema>>,
}
#[derive(Debug)]
struct BatchMetadata {
    _authority: Budget,
    _schema: Option<Arc<SharedSchema>>,
    diagnostic: od::Life,
}
impl Drop for BatchMetadata {
    fn drop(&mut self) {
        self.diagnostic.begin_drop();
    }
}
#[derive(Debug)]
struct OwnerClaims {
    payload: Arc<Budget>,
    metadata: Arc<BatchMetadata>,
    base: usize,
    capacity: usize,
}
#[allow(dead_code)]
struct ChargedOwnerReservation {
    size: usize,
    _payload: Arc<Budget>,
    _metadata: Arc<BatchMetadata>,
}
#[derive(Debug)]
struct OwnerReservation {
    size: usize,
    _payload: Arc<Budget>,
    _metadata: Arc<BatchMetadata>,
    diagnostic: od::Life,
}
impl Drop for OwnerReservation {
    fn drop(&mut self) {
        self.diagnostic.begin_drop();
    }
}
impl arrow_buffer::MemoryReservation for OwnerReservation {
    fn size(&self) -> usize {
        self.size
    }
    fn resize(&mut self, size: usize) {
        if self.size != size {
            od::gap(od::GAP_UNSUPPORTED);
        }
        self.size = size;
        self.diagnostic.update(|o| o.bytes = size as u64);
    }
}
impl arrow_buffer::MemoryPool for OwnerClaims {
    fn reserve(&self, size: usize) -> Box<dyn arrow_buffer::MemoryReservation> {
        let _transition = od::Transition::begin();
        let diagnostic = od::Life::new(od::Kind::Claim, self.payload.diagnostic_id(), 0);
        diagnostic.update(|o| {
            o.bytes = size as u64;
            o.a = self.capacity as u64;
        });
        diagnostic.raw(35, self.base, self.capacity, self.capacity);
        diagnostic.edge(self.payload.diagnostic_id(), 10, self.payload.size());
        diagnostic.edge(self.metadata.diagnostic.id(), 7, 0);
        if size != self.capacity || self.payload.size() != self.capacity {
            od::gap(od::GAP_UNKNOWN_SPONSOR);
        }
        Box::new(OwnerReservation {
            size,
            _payload: Arc::clone(&self.payload),
            _metadata: Arc::clone(&self.metadata),
            diagnostic,
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
                base: buffer.data_ptr().as_ptr() as usize,
                capacity: buffer.capacity(),
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
/// Keep the existing 256-byte buffer/control allowance. Pinned Bytes owns
/// ptr + len + Deallocation (three usize words), and the actual reservation
/// mutex. Include its Arc, our payload Budget Arc and the claim token Box.
/// Private IPC allocations have no external custom-deallocator side owner.
fn resident_controls_fit() -> Option<()> {
    use std::alloc::Layout;
    let bytes = super::resident_metadata::raw_bytes_arc_bound()?;
    let payload = super::resident_metadata::arc_bytes(Layout::new::<ChargedBudget>())?;
    let token = size_of::<ChargedOwnerReservation>();
    // Each split also retains the shared registration and its fixed consumer
    // name. Counting it per occurrence conservatively covers the final split.
    let registration =
        super::resident_metadata::literal_registration_bytes("SpillReaderDecode")?;
    (bytes
        .checked_add(payload)?
        .checked_add(token)?
        .checked_add(registration)?
        <= 256)
        .then_some(())
}

fn resident_batch_metadata(
    batch: &RecordBatch,
    shared: bool,
    old_metadata: usize,
) -> Option<usize> {
    if !shared {
        return None;
    }
    resident_controls_fit()?;
    let resident = super::resident_metadata::batch(batch)?;
    let owner = super::resident_metadata::arc_bytes(std::alloc::Layout::new::<
        ChargedBatchMetadata,
    >())?;
    let bytes = resident
        .headers
        .checked_add(resident.buffers.checked_mul(256)?)?
        .checked_add(owner)?;
    // The complete old preparation fee still protects all following ArrayData
    // walks and owner tables. A larger/unknown candidate keeps that fee.
    (bytes <= old_metadata).then_some(bytes)
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
    let _transition = od::Transition::begin();
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
        add(size_of::<ChargedBatchMetadata>(), 2 * size_of::<usize>())?,
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
                size_of::<ChargedDictionarySources>(),
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
        next.metadata = Some(work.split(snapshot));
        next.values.try_reserve_exact(roots).map_err(|error| {
            DataFusionError::ResourcesExhausted(format!(
                "Spill dictionary roots allocation failed: {error}"
            ))
        })?;
        for column in batch.columns() {
            pin_values(&column.to_data(), &mut next.values);
        }
    }
    let resident = resident_batch_metadata(batch, shared, metadata).unwrap_or(metadata);
    let authority = work.split(resident);
    let diagnostic = od::Life::new(od::Kind::BatchMetadata, authority.diagnostic_id(), 0);
    diagnostic.update(|o| {
        o.bytes = resident as u64;
        o.a = 1;
        o.flags = 4;
    });
    diagnostic.edge(authority.diagnostic_id(), 10, resident);
    if shared {
        diagnostic.edge(schema.diagnostic.id(), 7, 0);
    }
    let metadata = Arc::new(BatchMetadata {
        _authority: authority,
        _schema: shared.then(|| Arc::clone(schema)),
        diagnostic,
    });
    // Only the resident metadata moves into tokens. The remaining complete
    // preparation grant stays in work through final to_data and table drops.
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

/// Arrow 58.4 allocates one ALIGNMENT-aligned body in MessageReader::maybe_next.
/// Uncompressed read_buffer returns body slices. This finite recursive layout
/// checks every descriptor's range, minimum length and eight-byte alignment,
/// so align_buffers cannot allocate another payload for its supported types.
/// Exact DFS node/buffer counts, parent lengths and null counts are checked.
/// This proves allocation for the same private frame subsequently decoded;
/// it does not validate arbitrary IPC values. Other layouts keep the old bound.
fn aligned_body_matches(
    schema: &Schema,
    message: &arrow_ipc::Message<'_>,
    body: usize,
) -> Result<bool> {
    // Finite primitive/container domain: each accepted typed buffer requires
    // at most 8-byte alignment. Arrow 58.4 read_buffer returns body slices;
    // align_buffers therefore makes no aligned payload copies in this domain.
    fn descriptor(buffer: &arrow_ipc::Buffer, minimum: usize, body: usize) -> bool {
        let (Ok(offset), Ok(length)) = (
            usize::try_from(buffer.offset()),
            usize::try_from(buffer.length()),
        ) else {
            return false;
        };
        offset % 8 == 0
            && length >= minimum
            && offset.checked_add(length).is_some_and(|end| end <= body)
    }
    fn array<'a>(
        data_type: &DataType,
        expected_length: Option<usize>,
        depth: usize,
        nodes: &mut impl Iterator<Item = &'a arrow_ipc::FieldNode>,
        buffers: &mut impl Iterator<Item = &'a arrow_ipc::Buffer>,
        body: usize,
    ) -> Result<bool> {
        if depth >= 64
            || !matches!(
                data_type,
                DataType::Int64
                    | DataType::UInt64
                    | DataType::UInt32
                    | DataType::FixedSizeBinary(16)
                    | DataType::Timestamp(_, _)
                    | DataType::Utf8
                    | DataType::List(_)
                    | DataType::Struct(_)
            )
        {
            return Ok(false);
        }
        let Some(node) = nodes.next() else {
            return Ok(false);
        };
        let (Ok(length), Ok(nulls)) = (
            usize::try_from(node.length()),
            usize::try_from(node.null_count()),
        ) else {
            return Ok(false);
        };
        if expected_length.is_some_and(|expected| length != expected) || nulls > length {
            return Ok(false);
        }
        let bitmap = if nulls == 0 { 0 } else { add(length, 7)? / 8 };
        if !buffers
            .next()
            .is_some_and(|buffer| descriptor(buffer, bitmap, body))
        {
            return Ok(false);
        }
        let mut next = |minimum| {
            buffers
                .next()
                .is_some_and(|buffer| descriptor(buffer, minimum, body))
        };
        match data_type {
            DataType::Int64 | DataType::UInt64 | DataType::Timestamp(_, _) => {
                Ok(next(mul(length, 8)?))
            }
            DataType::UInt32 => Ok(next(mul(length, 4)?)),
            DataType::FixedSizeBinary(16) => Ok(next(mul(length, 16)?)),
            DataType::Utf8 => Ok(next(mul(add(length, 1)?, 4)?) && next(0)),
            DataType::List(child) => {
                if !next(mul(add(length, 1)?, 4)?) {
                    return Ok(false);
                }
                // A List's child count comes from its OWN FieldNode. It is
                // not the parent row count, including for NULL parent spans.
                array(child.data_type(), None, depth + 1, nodes, buffers, body)
            }
            DataType::Struct(children) => {
                // Struct children must each carry exactly the parent row count.
                for child in children {
                    if !array(
                        child.data_type(),
                        Some(length),
                        depth + 1,
                        nodes,
                        buffers,
                        body,
                    )? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            _ => unreachable!(),
        }
    }
    if message.version() != arrow_ipc::MetadataVersion::V5
        || message.header_type() != MessageHeader::RecordBatch
        || schema.fields().is_empty()
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
    let Ok(rows) = usize::try_from(batch.length()) else {
        return Ok(false);
    };
    // Same Layout that Arrow MutableBuffer::from_len_zeroed unwraps.
    std::alloc::Layout::from_size_align(body, arrow_buffer::alloc::ALIGNMENT)
        .map_err(|_| overflow())?;
    let mut nodes = nodes.iter();
    let mut buffers = buffers.iter();
    for field in schema.fields() {
        if !array(
            field.data_type(),
            Some(rows),
            0,
            &mut nodes,
            &mut buffers,
            body,
        )? {
            return Ok(false);
        }
    }
    // Exact DFS counts; no omitted or unused field/buffer descriptors qualify.
    Ok(nodes.next().is_none() && buffers.next().is_none())
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
        let payload = if ordinary && aligned_body_matches(schema, &message, body)? {
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
        add(size_of::<ChargedBatchMetadata>(), 2 * size_of::<usize>())?,
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

#[cfg(feature = "test_utils")]
pub(crate) fn verify_ownership_controls() -> Result<()> {
    super::credit::verify_ownership_controls()?;
    ownership_tests::ipc_buffer_ticket_retains_payload_and_metadata_through_final_slice(
    )?;
    ownership_tests::queued_ipc_claims_capture_after_actual_producer_retirement()?;
    ownership_tests::original_inline_budget_and_claim_layouts_exclude_observer_fields();
    Ok(())
}

#[cfg(any(test, feature = "test_utils"))]
mod ownership_tests {
    use super::*;
    use datafusion_execution::memory_pool::GreedyMemoryPool;

    #[cfg_attr(test, test)]
    pub(super) fn ipc_buffer_ticket_retains_payload_and_metadata_through_final_slice()
    -> Result<()> {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(4096));
        let mut work = reserve(&pool, 4096, "SpillReaderDecode")?;
        let buffer = arrow_buffer::Buffer::from_vec(vec![7_u64; 8]);
        let capacity = buffer.capacity();
        let metadata = Arc::new(BatchMetadata {
            _authority: work.split(128),
            _schema: None,
            diagnostic: od::Life::default(),
        });
        let claims = OwnerClaims {
            payload: Arc::new(work.split(capacity)),
            metadata,
            base: buffer.data_ptr().as_ptr() as usize,
            capacity,
        };
        buffer.claim(&claims);
        drop(claims);
        let slice = buffer.slice_with_length(8, 8);
        drop(buffer);
        drop(work);
        assert_eq!(pool.reserved(), capacity + 128);
        assert_eq!(slice.as_slice(), &7_u64.to_ne_bytes());
        drop(slice);
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    #[cfg(feature = "test_utils")]
    #[cfg_attr(test, test)]
    pub(super) fn queued_ipc_claims_capture_after_actual_producer_retirement()
    -> Result<()> {
        use crate::terminal_ownership::{Envelope, Kind, Session};
        use arrow::array::{ArrayRef, Int64Array};
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?;
        let _entered = runtime.enter();
        let path = std::env::temp_dir().join(format!(
            "graphforge-queued-ipc-{}.jsonl",
            std::process::id()
        ));
        let capture = Session::create(&path, Envelope::default())?;
        for _ in 0..2 {
            capture.begin_call()?;
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(4096));
            let mut work = reserve(&pool, 4096, "SpillReaderDecode")?;
            let buffer = arrow_buffer::Buffer::from_vec(vec![7_i64; 8]);
            let capacity = buffer.capacity();
            let authority = work.split(128);
            let diagnostic =
                od::Life::new(Kind::BatchMetadata, authority.diagnostic_id(), 0);
            diagnostic.update(|o| {
                o.bytes = 128;
                o.a = 1;
                o.flags = 4;
            });
            diagnostic.edge(authority.diagnostic_id(), 10, 128);
            let metadata = Arc::new(BatchMetadata {
                _authority: authority,
                _schema: None,
                diagnostic,
            });
            buffer.claim(&OwnerClaims {
                payload: Arc::new(work.split(capacity)),
                metadata,
                base: buffer.data_ptr().as_ptr() as usize,
                capacity,
            });
            let array = Int64Array::new(arrow_buffer::ScalarBuffer::from(buffer), None);
            let batch =
                RecordBatch::try_from_iter([("value", Arc::new(array) as ArrayRef)])?;
            let input = Box::pin(crate::stream::RecordBatchStreamAdapter::new(
                batch.schema(),
                futures::stream::iter([Ok(batch)]),
            ));
            // Two slots let the actual producer reach EOF without the consumer
            // polling. Retirement is the barrier; no sleep or polling ahead.
            let queued = crate::common::spawn_buffered(input, 2);
            drop(work);
            capture.wait_for_retirement(Kind::Producer)?;
            assert_eq!(pool.reserved(), capacity + 128);
            capture.snapshot(0);
            drop(queued);
            assert_eq!(pool.reserved(), 0);
            capture.snapshot(0);
            capture.end_call(0);
        }
        capture.finish()?;
        let content = std::fs::read_to_string(&path)?;
        assert_eq!(
            content
                .lines()
                .filter(|l| l.contains("\"relation\":3"))
                .count(),
            2
        );
        assert_eq!(
            content
                .lines()
                .filter(|l| l.contains("\"kind\":\"snapshot_end\"")
                    && l.contains("\"complete\":true"))
                .count(),
            4
        );
        if let Some(out) = std::env::var_os("GRAPHFORGE_OWNERSHIP_EVIDENCE_DIR") {
            std::fs::create_dir_all(&out)?;
            std::fs::copy(
                &path,
                std::path::PathBuf::from(out).join("queued-ipc-two-call.jsonl"),
            )?;
        }
        std::fs::remove_file(path)?;
        Ok(())
    }

    #[cfg_attr(test, test)]
    pub(super) fn original_inline_budget_and_claim_layouts_exclude_observer_fields() {
        assert_eq!(
            size_of::<ChargedBudget>(),
            size_of::<MemoryReservation>()
                + size_of::<Option<Arc<Credit>>>()
                + size_of::<AtomicUsize>()
        );
        assert_eq!(
            size_of::<ChargedOwnerReservation>(),
            size_of::<usize>()
                + size_of::<Arc<Budget>>()
                + size_of::<Arc<BatchMetadata>>()
        );
        assert_eq!(
            size_of::<ChargedClaimedReservation>(),
            size_of::<usize>() + size_of::<Arc<Budget>>()
        );
    }
}
