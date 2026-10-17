//! Paged column blobs of the edge property index, and the pack reads that
//! decode only the pages a lookup needs.
//!
//! A shard column used to be one Zstd Arrow IPC batch, so reading three values
//! of it decoded all 4096. A paged blob is the same column cut into batches of
//! `PAGE_ROWS` rows, followed by a directory:
//!
//! ```text
//! [page 0][page 1] ... [page n-1][schema][entry 0] ... [entry n-1][schema length]
//! ```
//!
//! A page is one IPC record batch message. The directory holds the IPC schema
//! message once, and for each page its byte length, its XXH64 and, for the
//! UUID and id columns, the least and greatest key it holds. The blob's seal
//! covers the directory, so the directory authenticates every page it names;
//! a page is checked against its own entry when it is read.
//!
//! Page offsets are not stored: they are the running sum of the lengths, and
//! the directory must account for every byte of the blob. The number of pages
//! is fixed by the shard's row count and the manifest's `page_rows`, so a
//! reader knows how long the directory entries are before it has read them.
//!
//! Every manifest names `page_rows`. An index built before paging has none, so
//! its manifest does not decode and is refused like any other manifest the
//! reader cannot use; the index is derived data and `rebuild_property_indexes`
//! replaces it.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, FixedSizeBinaryArray, UInt64Array};
use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamDecoder;
use arrow::record_batch::RecordBatch;
use graphforge_core::GfError;

use super::{
    BlobSeal, ID_COLUMN, PACK_BYTES, PACK_EXTENSION, UUID_COLUMN, query_add, query_mul,
    query_size_error, storage, validation,
};
use crate::read_memory::{ReadAdmission, ReadGrant};

/// Rows per page. A lookup decodes the pages that hold its rows, so a page is
/// the unit of decode work; the directory costs `12 + 2 * key width` bytes per
/// page, which a smaller page multiplies.
pub(super) const PAGE_ROWS: usize = 128;

/// Bytes of one directory entry before its key bounds: length and XXH64.
const ENTRY_BYTES: usize = 4 + 8;
/// Bytes of the schema length that ends a directory.
const TRAILER_BYTES: usize = 4;
/// How much schema the first read of a directory expects beyond its entries. A
/// schema of one field is under 200 bytes; a longer one costs one more read.
const SCHEMA_ALLOWANCE: usize = 256;

/// What the values of a column are used to find rows by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Keys {
    /// `edge_uuid`: 16 bytes, strictly ascending across the whole shard.
    Uuid,
    /// `edge_id`: `u64`, in UUID order, so ascending only by convention.
    Id,
    /// A property column, which is only ever read by row.
    Values,
}

impl Keys {
    const fn width(self) -> usize {
        match self {
            Self::Uuid => 16,
            Self::Id => 8,
            Self::Values => 0,
        }
    }

    const fn entry_bytes(self) -> usize {
        ENTRY_BYTES + 2 * self.width()
    }
}

fn uuid_key(bytes: &[u8]) -> u128 {
    u128::from_be_bytes(bytes.try_into().expect("16-byte UUID"))
}

/// The least and greatest key of one page, `(0, 0)` for a value column.
fn page_bounds(keys: Keys, page: &ArrayRef) -> Result<(u128, u128), GfError> {
    match keys {
        Keys::Values => Ok((0, 0)),
        Keys::Uuid => {
            let uuids = page
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| validation("shard UUID column has the wrong type"))?;
            Ok((
                uuid_key(uuids.value(0)),
                uuid_key(uuids.value(uuids.len() - 1)),
            ))
        }
        Keys::Id => {
            let ids = page
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| validation("shard id column has the wrong type"))?;
            let min = ids.values().iter().copied().min().expect("non-empty page");
            let max = ids.values().iter().copied().max().expect("non-empty page");
            Ok((u128::from(min), u128::from(max)))
        }
    }
}

/// One column as a paged blob: Zstd-compressed Arrow IPC batches of
/// `page_rows` rows each, then the directory that names them.
///
/// The column must hold no dictionary: a page is decoded without its
/// neighbours, and property columns are never dictionary-typed.
///
/// Returns the blob and the XXH64 its seal carries, which is that of the
/// directory.
pub(super) fn encode_paged(
    name: &str,
    data_type: &DataType,
    array: &ArrayRef,
    keys: Keys,
    page_rows: usize,
) -> Result<(Vec<u8>, u64), GfError> {
    // Field metadata is a hash map: leaving it out keeps the bytes a function
    // of the content. The UUID and id columns are the only non-null ones.
    let nullable = name != UUID_COLUMN && name != ID_COLUMN;
    let schema = Arc::new(Schema::new(vec![Field::new(
        name,
        data_type.clone(),
        nullable,
    )]));
    // Zstd, as the CSR shards: the index must not cost more than the
    // compressed fragments it indexes.
    let options = arrow::ipc::writer::IpcWriteOptions::default()
        .try_with_compression(Some(arrow::ipc::CompressionType::ZSTD))
        .map_err(|e| validation(e.to_string()))?;
    let mut writer =
        arrow::ipc::writer::StreamWriter::try_new_with_options(Vec::new(), &schema, options)
            .map_err(|e| validation(e.to_string()))?;
    let schema_end = writer.get_ref().len();
    let mut ends = Vec::with_capacity(array.len().div_ceil(page_rows));
    let mut bounds = Vec::with_capacity(ends.capacity());
    for start in (0..array.len()).step_by(page_rows) {
        let page = array.slice(start, page_rows.min(array.len() - start));
        bounds.push(page_bounds(keys, &page)?);
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![page])
            .map_err(|e| validation(e.to_string()))?;
        writer
            .write(&batch)
            .map_err(|e| validation(e.to_string()))?;
        ends.push(writer.get_ref().len());
    }
    writer.finish().map_err(|e| validation(e.to_string()))?;
    let mut stream = writer.into_inner().map_err(|e| validation(e.to_string()))?;
    // The end-of-stream marker is not stored: a reader appends none, because it
    // decodes one message at a time.
    stream.truncate(ends.last().copied().unwrap_or(schema_end));
    let mut blob = stream.split_off(schema_end);
    let schema_bytes = stream;
    let mut directory = schema_bytes.clone();
    let mut begin = 0;
    for (end, (lo, hi)) in ends.iter().zip(&bounds) {
        let end = end - schema_end;
        let length = u32::try_from(end - begin).map_err(|_| validation("page exceeds u32"))?;
        directory.extend_from_slice(&length.to_le_bytes());
        directory.extend_from_slice(
            &crate::corruption_checksum::checksum(&blob[begin..end]).to_le_bytes(),
        );
        let width = keys.width();
        directory.extend_from_slice(&lo.to_be_bytes()[16 - width..]);
        directory.extend_from_slice(&hi.to_be_bytes()[16 - width..]);
        begin = end;
    }
    let schema_length =
        u32::try_from(schema_bytes.len()).map_err(|_| validation("schema too long"))?;
    directory.extend_from_slice(&schema_length.to_le_bytes());
    let seal_checksum = crate::corruption_checksum::checksum(&directory);
    blob.extend_from_slice(&directory);
    Ok((blob, seal_checksum))
}

/// One page of a paged blob.
#[derive(Debug, Clone, Copy)]
struct Page {
    /// Where the page starts, from the start of the blob.
    offset: u64,
    length: u64,
    xxh64: u64,
    lo: u128,
    hi: u128,
}

/// A column blob whose directory has been read and checked against its seal.
#[derive(Debug)]
pub(super) struct OpenBlob {
    seal: BlobSeal,
    keys: Keys,
    rows: u64,
    page_rows: u64,
    /// The IPC schema message every page is decoded against.
    schema: Vec<u8>,
    pages: Vec<Page>,
}

impl OpenBlob {
    /// Rows of each page but the last.
    pub(super) fn page_rows(&self) -> usize {
        usize::try_from(self.page_rows).expect("page rows fit usize")
    }

    /// Pages in the blob.
    pub(super) fn page_count(&self) -> usize {
        self.pages.len()
    }

    /// Rows the page holds.
    fn rows_in(&self, page: usize) -> usize {
        let start = page as u64 * self.page_rows;
        usize::try_from(self.page_rows.min(self.rows - start)).expect("page rows fit usize")
    }

    fn bounds(&self, page: usize) -> (u128, u128) {
        (self.pages[page].lo, self.pages[page].hi)
    }

    /// The pages that can hold `targets`, each with the targets that fall in
    /// it. UUID pages are disjoint and ascending, so a target belongs to at
    /// most one, and `targets` must ascend.
    pub(super) fn uuid_pages(&self, targets: &[[u8; 16]]) -> Vec<(usize, Range<usize>)> {
        let pages = &self.pages;
        let mut found: Vec<(usize, Range<usize>)> = Vec::new();
        for (index, target) in targets.iter().enumerate() {
            let key = uuid_key(target);
            let page = pages.partition_point(|page| page.hi < key);
            if page == pages.len() || pages[page].lo > key {
                continue;
            }
            match found.last_mut() {
                Some((last, range)) if *last == page => range.end = index + 1,
                _ => found.push((page, index..index + 1)),
            }
        }
        found
    }

    /// The pages whose id range holds a target, ascending. `targets` must
    /// ascend. Ids are in UUID order, so page ranges may overlap and one id
    /// can only be ruled out by its page's bounds, not its neighbours'.
    pub(super) fn id_pages(&self, targets: &[u64]) -> Vec<usize> {
        let pages = &self.pages;
        (0..pages.len())
            .filter(|&index| {
                let (lo, hi) = (pages[index].lo, pages[index].hi);
                let first = targets.partition_point(|target| u128::from(*target) < lo);
                first < targets.len() && u128::from(targets[first]) <= hi
            })
            .collect()
    }

    /// The ids of the blob lie in `min..=max`, as the shard's manifest entry says.
    pub(super) fn check_id_range(&self, min: u64, max: u64) -> Result<(), GfError> {
        if self
            .pages
            .iter()
            .any(|page| page.lo < u128::from(min) || page.hi > u128::from(max))
        {
            return Err(validation("shard id lies outside its manifest range"));
        }
        Ok(())
    }

    /// The pages that hold `rows`, ascending, and for each row the slot of its
    /// page among them and its offset in that page.
    pub(super) fn row_pages(&self, rows: &[usize]) -> (Vec<usize>, Vec<(usize, usize)>) {
        let page_rows = self.page_rows();
        let mut wanted: Vec<usize> = rows.iter().map(|row| row / page_rows).collect();
        wanted.sort_unstable();
        wanted.dedup();
        let positions = rows
            .iter()
            .map(|row| {
                let slot = wanted
                    .binary_search(&(row / page_rows))
                    .expect("page was collected");
                (slot, row % page_rows)
            })
            .collect();
        (wanted, positions)
    }
}

/// Check one decoded page against what the manifest and directory say of it.
fn check_page(
    keys: Keys,
    array: &ArrayRef,
    rows: usize,
    bounds: (u128, u128),
) -> Result<(), GfError> {
    if array.len() != rows {
        return Err(validation("blob shape does not match its manifest"));
    }
    match keys {
        Keys::Values => {}
        Keys::Uuid => {
            let wrong_type = || validation("shard UUID column has the wrong type");
            if array.data_type() != &DataType::FixedSizeBinary(16) || array.null_count() != 0 {
                return Err(wrong_type());
            }
            let uuids = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(wrong_type)?;
            if (1..uuids.len()).any(|row| uuids.value(row - 1) >= uuids.value(row)) {
                return Err(validation("shard UUIDs are not strictly ascending"));
            }
            if rows > 0 && bounds != (uuid_key(uuids.value(0)), uuid_key(uuids.value(rows - 1))) {
                return Err(validation("page does not match its directory entry"));
            }
        }
        Keys::Id => {
            if array.data_type() != &DataType::UInt64 || array.null_count() != 0 {
                return Err(validation("shard id column has the wrong type"));
            }
            if rows > 0 && page_bounds(keys, array)? != bounds {
                return Err(validation("page does not match its directory entry"));
            }
        }
    }
    Ok(())
}

/// The packs of one route, opened when a lookup first reads from them and kept
/// open for the rest of the lookup.
pub(super) struct PackHandles<'a> {
    directory: PathBuf,
    /// Rows per page of the route's blobs, from its manifest.
    page_rows: u32,
    open: BTreeMap<u32, (PathBuf, File)>,
    admission: Option<&'a dyn ReadAdmission>,
    memory: Option<ReadGrant>,
    /// Open pack paths and map entries outlive every scoped decode.
    handles_memory: Option<ReadGrant>,
}

impl<'a> PackHandles<'a> {
    pub(super) fn new(directory: PathBuf, page_rows: u32) -> Self {
        Self {
            directory,
            page_rows,
            open: BTreeMap::new(),
            admission: None,
            memory: None,
            handles_memory: None,
        }
    }

    pub(super) fn with_admission(
        mut self,
        admission: Option<&'a dyn ReadAdmission>,
    ) -> Result<Self, GfError> {
        self.admission = admission;
        self.handles_memory = super::query_reserve(admission, 4096)?;
        self.memory = super::query_reserve(admission, 0)?;
        Ok(self)
    }

    pub(super) fn charge(&mut self, bytes: usize) -> Result<(), GfError> {
        if let (Some(admission), Some(memory)) = (self.admission, self.memory.as_mut()) {
            memory
                .grow(admission, bytes)
                .map_err(GfError::from_execution_error)?;
        }
        Ok(())
    }

    pub(super) fn is_admitted(&self) -> bool {
        self.admission.is_some()
    }

    /// Existing owners stay funded while a constructor borrows new authority.
    pub(super) fn checkpoint(&self) -> usize {
        self.memory.as_ref().map_or(0, ReadGrant::size)
    }

    /// Call only after the scoped constructor's temporary allocations drop.
    /// This transfers already admitted authority; it can never grow the pool.
    pub(super) fn retain_since(
        &mut self,
        checkpoint: usize,
        retained: usize,
    ) -> Result<(), GfError> {
        if let Some(memory) = self.memory.as_mut() {
            memory
                .shrink_to(query_add(checkpoint, retained)?)
                .map_err(GfError::from_execution_error)?;
        }
        Ok(())
    }

    pub(super) fn take_memory(&mut self) -> Option<ReadGrant> {
        self.memory.take()
    }

    /// Interleave uniquely selected source rows after admitting recursive
    /// kernel workspace. Payload and output-slot bounds belong to the caller.
    pub(super) fn interleave(
        &mut self,
        arrays: &[ArrayRef],
        indices: &[(usize, usize)],
    ) -> Result<ArrayRef, GfError> {
        if self.admission.is_some() {
            let workspace = arrays.iter().try_fold(0, |bytes, array| {
                query_add(bytes, super::interleave_memory::workspace(array.as_ref())?)
            })?;
            self.charge(workspace)?;
        }
        let references = arrays.iter().map(AsRef::as_ref).collect::<Vec<_>>();
        arrow::compute::interleave(&references, indices).map_err(|e| validation(e.to_string()))
    }

    fn corrupt(&self, pack: u32) -> GfError {
        let path = self.open.get(&pack).map_or_else(
            || Path::new("<unopened>").to_path_buf(),
            |(path, _)| path.clone(),
        );
        validation(format!("pack {} failed its checksum", path.display()))
    }

    /// `length` bytes of `pack` from `offset`, which must lie inside it.
    fn read_range(&mut self, pack: u32, offset: u64, length: u64) -> Result<Vec<u8>, GfError> {
        if length > PACK_BYTES as u64
            || offset
                .checked_add(length)
                .is_none_or(|end| end > PACK_BYTES as u64)
        {
            return Err(self.corrupt(pack));
        }
        // Fund the returned Vec and read_to_end's capacity/probe slack before
        // opening/reading a pack. Container and path growth are included.
        self.charge(query_mul(
            usize::try_from(length).map_err(|_| query_size_error())?,
            2,
        )?)?;
        if let std::collections::btree_map::Entry::Vacant(slot) = self.open.entry(pack) {
            if let (Some(admission), Some(memory)) = (self.admission, self.handles_memory.as_mut())
            {
                memory
                    .grow(
                        admission,
                        query_add(query_mul(self.directory.as_os_str().len(), 4)?, 4096)?,
                    )
                    .map_err(GfError::from_execution_error)?;
            }
            let path = self.directory.join(format!("{pack:08}.{PACK_EXTENSION}"));
            let file = File::open(&path).map_err(|e| storage("open pack", &e))?;
            slot.insert((path, file));
        }
        let (_, file) = self.open.get_mut(&pack).expect("pack was just opened");
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| storage("seek pack", &e))?;
        // Sized to the range so it is one `read`, not the doubling probe an empty
        // buffer makes; `length` is bounded by `PACK_BYTES` above.
        let mut bytes = Vec::with_capacity(usize::try_from(length).expect("bounded by PACK_BYTES"));
        file.take(length)
            .read_to_end(&mut bytes)
            .map_err(|e| storage("read pack", &e))?;
        crate::lifecycle_io::record_read(
            crate::StorageIoPhase::ReadPathScan,
            bytes.len() as u64,
            1,
        );
        if bytes.len() as u64 != length {
            return Err(self.corrupt(pack));
        }
        Ok(bytes)
    }

    /// Open the blob `seal` names: its directory, read from the end of the blob
    /// and checked against the seal.
    pub(super) fn open_blob(
        &mut self,
        seal: BlobSeal,
        rows: u64,
        keys: Keys,
    ) -> Result<OpenBlob, GfError> {
        let checkpoint = self.checkpoint();
        let page_rows = u64::from(self.page_rows);
        let count =
            usize::try_from(rows.div_ceil(page_rows)).map_err(|_| self.corrupt(seal.pack))?;
        let fixed = query_add(query_mul(count, keys.entry_bytes())?, TRAILER_BYTES)?;
        self.charge(query_add(
            query_mul(count, std::mem::size_of::<Page>())?,
            std::mem::size_of::<OpenBlob>(),
        )?)?;
        if seal.byte_length > PACK_BYTES as u64 || seal.byte_length < fixed as u64 {
            self.read_range(seal.pack, 0, 0)?;
            return Err(self.corrupt(seal.pack));
        }
        let blob_end = seal
            .offset
            .checked_add(seal.byte_length)
            .filter(|end| *end <= PACK_BYTES as u64)
            .ok_or_else(|| self.corrupt(seal.pack))?;
        let window = (fixed + SCHEMA_ALLOWANCE)
            .min(usize::try_from(seal.byte_length).expect("bounded by PACK_BYTES"));
        let mut tail = self.read_range(seal.pack, blob_end - window as u64, window as u64)?;
        let schema_length = u32::from_le_bytes(
            tail[tail.len() - TRAILER_BYTES..]
                .try_into()
                .expect("four bytes"),
        ) as usize;
        let directory_length = fixed + schema_length;
        if directory_length as u64 > seal.byte_length {
            return Err(self.corrupt(seal.pack));
        }
        if directory_length > tail.len() {
            tail = self.read_range(
                seal.pack,
                blob_end - directory_length as u64,
                directory_length as u64,
            )?;
        }
        let directory = &tail[tail.len() - directory_length..];
        if crate::corruption_checksum::checksum(directory) != seal.xxh64 {
            return Err(self.corrupt(seal.pack));
        }
        let (schema, entries) = directory.split_at(schema_length);
        let mut pages = Vec::with_capacity(count);
        let mut offset = 0_u64;
        let width = keys.width();
        for entry in entries[..count * keys.entry_bytes()].chunks_exact(keys.entry_bytes()) {
            let length = u64::from(u32::from_le_bytes(entry[..4].try_into().expect("length")));
            let xxh64 = u64::from_le_bytes(entry[4..12].try_into().expect("checksum"));
            let key = |bytes: &[u8]| {
                let mut full = [0_u8; 16];
                full[16 - width..].copy_from_slice(bytes);
                u128::from_be_bytes(full)
            };
            pages.push(Page {
                offset,
                length,
                xxh64,
                lo: key(&entry[12..12 + width]),
                hi: key(&entry[12 + width..]),
            });
            offset += length;
        }
        if offset + directory_length as u64 != seal.byte_length
            || pages.iter().any(|page| page.lo > page.hi)
            || (keys == Keys::Uuid && pages.windows(2).any(|pair| pair[0].hi >= pair[1].lo))
        {
            return Err(self.corrupt(seal.pack));
        }
        self.charge(schema.len())?;
        let blob = OpenBlob {
            seal,
            keys,
            rows,
            page_rows,
            schema: schema.to_vec(),
            pages,
        };
        drop(tail);
        self.retain_since(
            checkpoint,
            query_add(
                query_add(
                    blob.schema.capacity(),
                    query_mul(blob.pages.capacity(), std::mem::size_of::<Page>())?,
                )?,
                std::mem::size_of::<OpenBlob>(),
            )?,
        )?;
        Ok(blob)
    }

    /// The pages `wanted` of an open blob, decoded in the same order. `wanted`
    /// ascends without repeats. Pages that follow one another are read with
    /// one read, and each is checked against its directory entry.
    pub(super) fn read_pages(
        &mut self,
        blob: &OpenBlob,
        wanted: &[usize],
    ) -> Result<Vec<ArrayRef>, GfError> {
        debug_assert!(wanted.windows(2).all(|pair| pair[0] < pair[1]));
        let checkpoint = self.checkpoint();
        self.charge(query_add(
            query_mul(blob.schema.len(), 512)?,
            query_add(query_mul(wanted.len(), 256)?, 64 * 1024)?,
        )?)?;
        let mut decoder = StreamDecoder::new();
        let mut header = Buffer::from_slice_ref(&blob.schema);
        if decoder
            .decode(&mut header)
            .map_err(|e| validation(e.to_string()))?
            .is_some()
            || !header.is_empty()
        {
            return Err(validation("blob schema is malformed"));
        }
        // A schema message has no body. StreamDecoder may only process that
        // body on the next nonempty decode, so schema() can still be None here.
        let mut arrays = Vec::with_capacity(wanted.len());
        let mut retained = query_mul(wanted.len(), std::mem::size_of::<ArrayRef>())?;
        let mut next = 0;
        while next < wanted.len() {
            let mut end = next + 1;
            while end < wanted.len() && wanted[end] == wanted[end - 1] + 1 {
                end += 1;
            }
            retained = query_add(
                retained,
                self.read_page_run(blob, &wanted[next..end], &mut decoder, &mut arrays)?,
            )?;
            next = end;
        }
        if self.is_admitted() {
            // Every page shares the decoder's nested schema allocations. Their
            // original construction grant stayed live across all page scopes;
            // transfer its retained type owner once, without pool growth.
            if let Some(schema) = decoder.schema() {
                for field in schema.fields() {
                    retained = query_add(retained, query_mul(field.data_type().size(), 4)?)?;
                }
            }
        }
        drop(header);
        drop(decoder);
        self.retain_since(checkpoint, retained)?;
        Ok(arrays)
    }

    /// Decode one contiguous read, retaining shared raw backing at most once.
    fn read_page_run(
        &mut self,
        blob: &OpenBlob,
        wanted: &[usize],
        decoder: &mut StreamDecoder,
        arrays: &mut Vec<ArrayRef>,
    ) -> Result<usize, GfError> {
        let checkpoint = self.checkpoint();
        let mut retained = 0;
        let mut aliases_run = false;
        let first = &blob.pages[wanted[0]];
        let last = &blob.pages[*wanted.last().expect("nonempty page run")];
        let bytes = self.read_range(
            blob.seal.pack,
            blob.seal.offset + first.offset,
            last.offset + last.length - first.offset,
        )?;
        let run = Buffer::from_vec(bytes);
        for &index in wanted {
            let page = &blob.pages[index];
            let start = usize::try_from(page.offset - first.offset).expect("run fits memory");
            let mut message = run.slice_with_length(
                start,
                usize::try_from(page.length).expect("page fits memory"),
            );
            if crate::corruption_checksum::checksum(message.as_slice()) != page.xxh64 {
                return Err(self.corrupt(blob.seal.pack));
            }
            let page_checkpoint = self.checkpoint();
            if self.is_admitted() {
                self.charge(ipc_page_bytes(message.as_slice())?)?;
            }
            let batch = decoder
                .decode(&mut message)
                .map_err(|e| validation(e.to_string()))?
                .filter(|_| message.is_empty())
                .ok_or_else(|| validation("blob page holds no single batch"))?;
            if batch.num_columns() != 1 {
                return Err(validation("blob shape does not match its manifest"));
            }
            let array = Arc::clone(batch.column(0));
            check_page(blob.keys, &array, blob.rows_in(index), blob.bounds(index))?;
            if self.is_admitted() {
                // The enclosing read_pages schema grant owns nested DataTypes.
                // Raw IPC buffers instead borrow this run's read grant until
                // the run ends; transfer that physical backing only once.
                let mut bytes = retained_array_bytes(array.as_ref())?
                    .checked_sub(query_mul(array.data_type().size(), 4)?)
                    .ok_or_else(query_size_error)?;
                crate::read_memory::visit_buffers(array.as_ref(), &mut |buffer| {
                    if buffer.capacity() != 0
                        && buffer.data_ptr() == run.data_ptr()
                        && buffer.capacity() == run.capacity()
                    {
                        bytes = bytes
                            .checked_sub(buffer.capacity())
                            .ok_or_else(crate::read_memory::exceeded)?;
                        aliases_run = true;
                    }
                    Ok(())
                })
                .map_err(GfError::from_execution_error)?;
                drop(batch);
                drop(message);
                self.retain_since(page_checkpoint, bytes)?;
                retained = query_add(retained, bytes)?;
            }
            arrays.push(array);
        }
        if self.is_admitted() {
            if aliases_run {
                retained = query_add(retained, run.capacity())?;
            }
            drop(run);
            self.retain_since(checkpoint, retained)?;
        }
        Ok(retained)
    }
}

/// Concrete retained Arrow allocations, including nested/view containers and
/// full aliased backing capacities. This only measures after construction;
/// callers must already hold its preallocation authority before calling it.
pub(super) fn retained_array_bytes(array: &dyn Array) -> Result<usize, GfError> {
    let mut bytes = query_add(
        array.get_array_memory_size(),
        crate::read_memory::container_bytes(array).map_err(GfError::from_execution_error)?,
    )?;
    bytes = query_add(
        bytes,
        query_add(query_mul(array.data_type().size(), 4)?, 256)?,
    )?;
    crate::read_memory::visit_buffers(array, &mut |buffer| {
        if buffer.capacity() == 0 && !buffer.is_empty() {
            return Err(crate::read_memory::exceeded());
        }
        if buffer.capacity() != 0 {
            // Future prepaid buffer token plus the buffer's shared header.
            bytes = crate::read_memory::add(bytes, crate::read_memory::token_bytes() + 64)?;
        }
        Ok(())
    })
    .map_err(GfError::from_execution_error)?;
    Ok(bytes)
}

/// Inspect authenticated IPC wire descriptors before StreamDecoder can allocate
/// decompressed buffers or Arrow wrappers. Compressed buffer prefixes are the
/// actual allocation lengths used by pinned Arrow, including raw (-1) blocks.
fn ipc_page_bytes(bytes: &[u8]) -> Result<usize, GfError> {
    let invalid = || validation("malformed IPC page allocation descriptors");
    let prefix = bytes.get(..4).ok_or_else(invalid)?;
    let continuation = prefix == [255; 4];
    let base = if continuation { 8 } else { 4 };
    let metadata = u32::from_le_bytes(
        bytes
            .get(base - 4..base)
            .ok_or_else(invalid)?
            .try_into()
            .unwrap(),
    ) as usize;
    let message = arrow::ipc::root_as_message(
        bytes
            .get(base..query_add(base, metadata)?)
            .ok_or_else(invalid)?,
    )
    .map_err(|_| invalid())?;
    let record = message.header_as_record_batch().ok_or_else(invalid)?;
    let body = bytes
        .get(query_add(base, metadata)?..)
        .ok_or_else(invalid)?;
    let mut size = query_add(query_mul(metadata, 512)?, 64 * 1024)?;
    let buffers = record.buffers().ok_or_else(invalid)?;
    for buffer in buffers {
        let offset = usize::try_from(buffer.offset()).map_err(|_| invalid())?;
        let length = usize::try_from(buffer.length()).map_err(|_| invalid())?;
        let stored = body
            .get(offset..query_add(offset, length)?)
            .ok_or_else(invalid)?;
        let decoded = if record.compression().is_some() && !stored.is_empty() {
            let prefix =
                i64::from_le_bytes(stored.get(..8).ok_or_else(invalid)?.try_into().unwrap());
            if prefix == -1 {
                stored.len() - 8
            } else {
                usize::try_from(prefix).map_err(|_| invalid())?
            }
        } else {
            stored.len()
        };
        size = query_add(size, query_add(query_mul(decoded, 2)?, 512)?)?;
    }
    if let Some(nodes) = record.nodes() {
        size = query_add(size, query_mul(nodes.len(), 4096)?)?;
    }
    Ok(size)
}

#[cfg(test)]
mod admission_tests {
    use super::*;
    use datafusion::execution::memory_pool::{
        GreedyMemoryPool, MemoryConsumer, MemoryPool, MemoryReservation,
    };
    use std::sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    };

    struct Admission {
        memory: Mutex<MemoryReservation>,
        cancelled: AtomicBool,
    }
    impl ReadAdmission for Admission {
        fn reserve(&self, bytes: usize) -> datafusion::error::Result<ReadGrant> {
            self.check_cancelled()?;
            let memory = self.memory.lock().unwrap();
            memory.try_grow(bytes)?;
            Ok(ReadGrant::from_reserved(memory.split(bytes)))
        }
        fn check_cancelled(&self) -> datafusion::error::Result<()> {
            if self.cancelled.load(Ordering::Acquire) {
                return Err(datafusion::error::DataFusionError::Execution(
                    "property read cancelled".into(),
                ));
            }
            Ok(())
        }
    }

    #[test]
    fn decoded_pages_retain_backing_without_accumulating_decoder_scratch() {
        let root = tempfile::tempdir().unwrap();
        let rows = 16_384;
        let values: ArrayRef = Arc::new(UInt64Array::from_iter_values(0..rows as u64));
        let (encoded, checksum) = encode_paged(
            "value",
            values.data_type(),
            &values,
            Keys::Values,
            PAGE_ROWS,
        )
        .unwrap();
        std::fs::write(root.path().join("00000000.epk"), &encoded).unwrap();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(2 * 1024 * 1024));
        let admission = Admission {
            memory: Mutex::new(MemoryConsumer::new("paged decode lifetime").register(&pool)),
            cancelled: AtomicBool::new(false),
        };
        let mut packs = PackHandles::new(root.path().to_owned(), PAGE_ROWS as u32)
            .with_admission(Some(&admission))
            .unwrap();
        let blob = packs
            .open_blob(
                BlobSeal {
                    pack: 0,
                    offset: 0,
                    byte_length: encoded.len() as u64,
                    xxh64: checksum,
                },
                rows as u64,
                Keys::Values,
            )
            .unwrap();
        let before = pool.reserved();
        let wanted = (0..blob.page_count()).collect::<Vec<_>>();
        let arrays = packs.read_pages(&blob, &wanted).unwrap();
        let retained = arrays
            .iter()
            .map(|array| retained_array_bytes(array.as_ref()).unwrap())
            .sum::<usize>()
            + arrays.capacity() * std::mem::size_of::<ArrayRef>();
        assert!(pool.reserved() <= before + retained);
        for (page, array) in arrays.iter().enumerate() {
            let values = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            for (offset, value) in values.values().iter().enumerate() {
                assert_eq!(*value, (page * PAGE_ROWS + offset) as u64);
            }
        }
        // The scoped grant funds every returned page until its caller releases
        // it; closing pack handles releases their independent metadata grant.
        assert!(pool.reserved() >= rows * std::mem::size_of::<u64>());
        drop(arrays);
        drop(blob);
        drop(packs);
        drop(admission);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn decoded_list_pages_pre_admit_retained_nested_field_metadata() {
        let root = tempfile::tempdir().unwrap();
        let child = Arc::new(Field::new("n".repeat(1024 * 1024), DataType::UInt64, false));
        let values: ArrayRef = Arc::new(
            arrow::array::ListArray::try_new(
                child,
                arrow::buffer::OffsetBuffer::new(vec![0_i32, 1].into()),
                Arc::new(UInt64Array::from(vec![7])),
                None,
            )
            .unwrap(),
        );
        let (encoded, checksum) = encode_paged(
            "value",
            values.data_type(),
            &values,
            Keys::Values,
            PAGE_ROWS,
        )
        .unwrap();
        std::fs::write(root.path().join("00000000.epk"), &encoded).unwrap();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(2 * 1024 * 1024 * 1024));
        let admission = Admission {
            memory: Mutex::new(MemoryConsumer::new("nested page metadata").register(&pool)),
            cancelled: AtomicBool::new(false),
        };
        let mut packs = PackHandles::new(root.path().to_owned(), PAGE_ROWS as u32)
            .with_admission(Some(&admission))
            .unwrap();
        let blob = packs
            .open_blob(
                BlobSeal {
                    pack: 0,
                    offset: 0,
                    byte_length: encoded.len() as u64,
                    xxh64: checksum,
                },
                1,
                Keys::Values,
            )
            .unwrap();
        let before = pool.reserved();
        assert!(packs.read_pages(&blob, &[]).unwrap().is_empty());
        assert_eq!(pool.reserved(), before);
        let arrays = packs.read_pages(&blob, &[0]).unwrap();
        let list = arrays[0]
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list.value_offsets(), &[0, 1]);
        assert_eq!(list.data_type(), values.data_type());
        assert_eq!(
            list.values()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[7],
        );
        drop(arrays);
        drop(blob);
        drop(packs);
        drop(admission);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn raw_ipc_pages_share_one_admitted_read_run() {
        let root = tempfile::tempdir().unwrap();
        // SplitMix64 gives each page incompressible values, exercising IPC's
        // raw (-1) blocks whose output slices retain the entire read run.
        let expected = (0_u64..4096)
            .map(|i| {
                let mut z = i.wrapping_add(0x9e37_79b9_7f4a_7c15);
                z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
                z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
                z ^ (z >> 31)
            })
            .collect::<Vec<_>>();
        let values: ArrayRef = Arc::new(UInt64Array::from(expected.clone()));
        let (encoded, checksum) = encode_paged(
            "value",
            values.data_type(),
            &values,
            Keys::Values,
            PAGE_ROWS,
        )
        .unwrap();
        std::fs::write(root.path().join("00000000.epk"), &encoded).unwrap();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(512 * 1024));
        let admission = Admission {
            memory: Mutex::new(MemoryConsumer::new("raw paged lifetime").register(&pool)),
            cancelled: AtomicBool::new(false),
        };
        let mut packs = PackHandles::new(root.path().to_owned(), PAGE_ROWS as u32)
            .with_admission(Some(&admission))
            .unwrap();
        let blob = packs
            .open_blob(
                BlobSeal {
                    pack: 0,
                    offset: 0,
                    byte_length: encoded.len() as u64,
                    xxh64: checksum,
                },
                expected.len() as u64,
                Keys::Values,
            )
            .unwrap();
        let wanted = (0..blob.page_count()).collect::<Vec<_>>();
        let arrays = packs.read_pages(&blob, &wanted).unwrap();
        assert!(arrays[0].get_buffer_memory_size() > PAGE_ROWS * 8);
        let actual = arrays
            .iter()
            .flat_map(|array| {
                array
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
        drop(arrays);
        drop(blob);
        drop(packs);
        drop(admission);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn authenticated_pack_bytes_and_ipc_decode_refuse_before_the_protected_boundary() {
        let root = tempfile::tempdir().unwrap();
        let text = "authenticated property page ".repeat(16_384);
        let values: ArrayRef = Arc::new(arrow::array::StringArray::from(vec![text.as_str()]));
        let (encoded, checksum) = encode_paged(
            "payload",
            values.data_type(),
            &values,
            Keys::Values,
            PAGE_ROWS,
        )
        .unwrap();
        let seal = BlobSeal {
            pack: 0,
            offset: 0,
            byte_length: encoded.len() as u64,
            xxh64: checksum,
        };
        std::fs::write(root.path().join("00000000.epk"), &encoded).unwrap();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(8 * 1024 * 1024));
        let admission = Admission {
            memory: Mutex::new(
                MemoryConsumer::new("authenticated edge-property page").register(&pool),
            ),
            cancelled: AtomicBool::new(false),
        };
        let mut packs = PackHandles::new(root.path().to_owned(), PAGE_ROWS as u32)
            .with_admission(Some(&admission))
            .unwrap();
        let blob = packs.open_blob(seal, 1, Keys::Values).unwrap();
        let filler = MemoryConsumer::new("page boundary fixture").register(&pool);
        filler
            .try_grow(8 * 1024 * 1024 - pool.reserved() - 1024)
            .unwrap();
        let _capture = crate::lifecycle_io::CaptureScope::install();
        let error = packs.read_range(0, 0, encoded.len() as u64).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("authenticated edge-property page"),
            "{error}"
        );
        assert_eq!(
            crate::lifecycle_io::snapshot().unwrap().totals.read_bytes,
            0
        );
        let error = packs.read_pages(&blob, &[0]).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("authenticated edge-property page"),
            "{error}"
        );
        assert_eq!(
            crate::lifecycle_io::snapshot().unwrap().totals.read_bytes,
            0
        );
        drop(filler);
        let page = packs.read_pages(&blob, &[0]).unwrap().remove(0);
        assert_eq!(
            page.as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap()
                .value(0),
            text
        );
        drop(page);
        crate::lifecycle_io::reset();
        admission.cancelled.store(true, Ordering::Release);
        assert!(
            packs
                .read_pages(&blob, &[0])
                .unwrap_err()
                .to_string()
                .contains("cancelled")
        );
        assert_eq!(
            crate::lifecycle_io::snapshot().unwrap().totals.read_bytes,
            0
        );
        admission.cancelled.store(false, Ordering::Release);
        let mut damaged = encoded.clone();
        damaged[0] ^= 1;
        std::fs::write(root.path().join("00000000.epk"), damaged).unwrap();
        assert!(
            packs
                .read_pages(&blob, &[0])
                .unwrap_err()
                .to_string()
                .contains("checksum")
        );
        drop(packs);
        let mut missing = PackHandles::new(root.path().to_owned(), PAGE_ROWS as u32)
            .with_admission(Some(&admission))
            .unwrap();
        assert!(
            missing
                .read_range(99, 0, 1)
                .unwrap_err()
                .to_string()
                .contains("open pack")
        );
        drop(missing);
        drop(admission);
        assert_eq!(pool.reserved(), 0);
    }
}
