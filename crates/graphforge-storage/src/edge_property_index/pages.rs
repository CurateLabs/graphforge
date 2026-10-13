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
//! A manifest without `page_rows` names the layout this replaced: each blob is
//! one batch covered by one checksum. [`PackHandles`] still reads it, as a blob
//! of a single page, so an index built before paging answers until it is
//! rebuilt.

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

use super::{BlobSeal, ID_COLUMN, PACK_BYTES, PACK_EXTENSION, UUID_COLUMN, storage, validation};

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

#[derive(Debug)]
enum Layout {
    /// One batch covered by the blob's checksum, as before paging.
    Whole,
    Paged {
        schema: Vec<u8>,
        pages: Vec<Page>,
    },
}

/// A column blob whose directory has been read and checked against its seal.
#[derive(Debug)]
pub(super) struct OpenBlob {
    seal: BlobSeal,
    keys: Keys,
    rows: u64,
    page_rows: u64,
    layout: Layout,
}

impl OpenBlob {
    /// Rows of each page but the last. A whole blob is a single page.
    pub(super) fn page_rows(&self) -> usize {
        usize::try_from(self.page_rows).expect("page rows fit usize")
    }

    /// Rows the page holds.
    fn rows_in(&self, page: usize) -> usize {
        let start = page as u64 * self.page_rows;
        usize::try_from(self.page_rows.min(self.rows - start)).expect("page rows fit usize")
    }

    fn bounds(&self, page: usize) -> Option<(u128, u128)> {
        match &self.layout {
            Layout::Whole => None,
            Layout::Paged { pages, .. } => Some((pages[page].lo, pages[page].hi)),
        }
    }

    /// The pages that can hold `targets`, each with the targets that fall in
    /// it. UUID pages are disjoint and ascending, so a target belongs to at
    /// most one, and `targets` must ascend.
    pub(super) fn uuid_pages(&self, targets: &[[u8; 16]]) -> Vec<(usize, Range<usize>)> {
        let Layout::Paged { pages, .. } = &self.layout else {
            return vec![(0, 0..targets.len())];
        };
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
        let Layout::Paged { pages, .. } = &self.layout else {
            return vec![0];
        };
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
        if let Layout::Paged { pages, .. } = &self.layout
            && pages
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
    bounds: Option<(u128, u128)>,
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
            if rows > 0
                && bounds.is_some_and(|bounds| {
                    bounds != (uuid_key(uuids.value(0)), uuid_key(uuids.value(rows - 1)))
                })
            {
                return Err(validation("page does not match its directory entry"));
            }
        }
        Keys::Id => {
            if array.data_type() != &DataType::UInt64 || array.null_count() != 0 {
                return Err(validation("shard id column has the wrong type"));
            }
            if rows > 0 && bounds.is_some() && Some(page_bounds(keys, array)?) != bounds {
                return Err(validation("page does not match its directory entry"));
            }
        }
    }
    Ok(())
}

/// The packs of one route, opened when a lookup first reads from them and kept
/// open for the rest of the lookup.
pub(super) struct PackHandles {
    directory: PathBuf,
    /// Rows per page of the route's blobs; `None` for the layout before paging.
    page_rows: Option<u32>,
    open: BTreeMap<u32, (PathBuf, File)>,
}

impl PackHandles {
    pub(super) fn new(directory: PathBuf, page_rows: Option<u32>) -> Self {
        Self {
            directory,
            page_rows,
            open: BTreeMap::new(),
        }
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
        if let std::collections::btree_map::Entry::Vacant(slot) = self.open.entry(pack) {
            let path = self.directory.join(format!("{pack:08}.{PACK_EXTENSION}"));
            let file = File::open(&path).map_err(|e| storage("open pack", &e))?;
            slot.insert((path, file));
        }
        if length > PACK_BYTES as u64 || offset.saturating_add(length) > PACK_BYTES as u64 {
            return Err(self.corrupt(pack));
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

    /// Open the blob `seal` names: its directory when paged, read from the end
    /// of the blob and checked against the seal.
    pub(super) fn open_blob(
        &mut self,
        seal: BlobSeal,
        rows: u64,
        keys: Keys,
    ) -> Result<OpenBlob, GfError> {
        let Some(page_rows) = self.page_rows else {
            return Ok(OpenBlob {
                seal,
                keys,
                rows,
                page_rows: rows.max(1),
                layout: Layout::Whole,
            });
        };
        let page_rows = u64::from(page_rows);
        let count =
            usize::try_from(rows.div_ceil(page_rows)).map_err(|_| self.corrupt(seal.pack))?;
        let fixed = count * keys.entry_bytes() + TRAILER_BYTES;
        if seal.byte_length > PACK_BYTES as u64 || seal.byte_length < fixed as u64 {
            self.read_range(seal.pack, 0, 0)?;
            return Err(self.corrupt(seal.pack));
        }
        let blob_end = seal.offset + seal.byte_length;
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
        Ok(OpenBlob {
            seal,
            keys,
            rows,
            page_rows,
            layout: Layout::Paged {
                schema: schema.to_vec(),
                pages,
            },
        })
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
        let Layout::Paged { schema, pages } = &blob.layout else {
            let array = self.read_whole(blob)?;
            check_page(blob.keys, &array, blob.rows_in(0), None)?;
            return Ok(vec![array]);
        };
        let pack = blob.seal.pack;
        let mut decoder = StreamDecoder::new();
        let mut header = Buffer::from_slice_ref(schema);
        if decoder
            .decode(&mut header)
            .map_err(|e| validation(e.to_string()))?
            .is_some()
            || !header.is_empty()
        {
            return Err(validation("blob schema is malformed"));
        }
        let mut arrays = Vec::with_capacity(wanted.len());
        let mut next = 0;
        while next < wanted.len() {
            let mut end = next + 1;
            while end < wanted.len() && wanted[end] == wanted[end - 1] + 1 {
                end += 1;
            }
            let (first, last) = (&pages[wanted[next]], &pages[wanted[end - 1]]);
            let run_end = last.offset + last.length;
            let bytes = self.read_range(
                pack,
                blob.seal.offset + first.offset,
                run_end - first.offset,
            )?;
            let run = Buffer::from_vec(bytes);
            for &index in &wanted[next..end] {
                let page = &pages[index];
                let start = usize::try_from(page.offset - first.offset).expect("run fits memory");
                let mut message = run.slice_with_length(
                    start,
                    usize::try_from(page.length).expect("page fits memory"),
                );
                if crate::corruption_checksum::checksum(message.as_slice()) != page.xxh64 {
                    return Err(self.corrupt(pack));
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
                arrays.push(array);
            }
            next = end;
        }
        Ok(arrays)
    }

    /// The whole column of a blob written before paging, checked against its
    /// seal.
    fn read_whole(&mut self, blob: &OpenBlob) -> Result<ArrayRef, GfError> {
        let seal = blob.seal;
        let bytes = self.read_range(seal.pack, seal.offset, seal.byte_length)?;
        if crate::corruption_checksum::checksum(&bytes) != seal.xxh64 {
            return Err(self.corrupt(seal.pack));
        }
        let mut reader =
            arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(&bytes), None)
                .map_err(|e| validation(e.to_string()))?;
        let batch = reader
            .next()
            .ok_or_else(|| validation("blob holds no batch"))?
            .map_err(|e| validation(e.to_string()))?;
        if reader.next().is_some() || batch.num_columns() != 1 {
            return Err(validation("blob shape does not match its manifest"));
        }
        Ok(Arc::clone(batch.column(0)))
    }
}
