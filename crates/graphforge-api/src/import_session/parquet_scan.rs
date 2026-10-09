//! Page-level facts of a registered Parquet source (#1918).
//!
//! The footer states each column chunk's compressed and uncompressed totals, not
//! the size of the pages a decoder holds while it works, and says nothing about
//! what an encoded page expands to. Page headers do: every page starts with a
//! small Thrift header naming its encoding, value count and both sizes. Reading
//! only those headers (never a page body) tells the builder what a decode will
//! hold before it allocates anything: the largest page of every column, which
//! the Arrow reader keeps decompressed while it advances, the dictionary each
//! dictionary-encoded column decodes once, and the row each page starts at, so a
//! batch's rows map to the pages that hold them.
//!
//! Every length is validated against the bytes that remain in its column chunk
//! before it is used, so a corrupt header is refused rather than allocated.

use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};

use graphforge_core::GfError;
use parquet::file::metadata::{ColumnChunkMetaData, ParquetMetaData};
use parquet::schema::types::ColumnDescriptor;

use super::{limit, storage};

/// A page header larger than this is not a page header: statistics are the only
/// variable part, and a writer truncates them far below it.
const MAX_HEADER_BYTES: usize = 1 << 20;
/// Thrift structs nest a few levels; this bounds recursion on corrupt input.
const MAX_DEPTH: usize = 12;

const DATA_PAGE: i32 = 0;
const INDEX_PAGE: i32 = 1;
const DICTIONARY_PAGE: i32 = 2;
const DATA_PAGE_V2: i32 = 3;

/// Parquet `Encoding` values this module distinguishes.
pub(super) mod encoding {
    pub(in crate::import_session) const PLAIN_DICTIONARY: i32 = 2;
    pub(in crate::import_session) const DELTA_BYTE_ARRAY: i32 = 7;
    pub(in crate::import_session) const RLE_DICTIONARY: i32 = 8;
}

/// What a page holds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PageKind {
    Dictionary,
    Data,
}

/// One page of a column chunk, from its header alone.
#[derive(Clone, Copy, Debug)]
pub(super) struct PageFact {
    pub(super) kind: PageKind,
    /// Compressed bytes of the body, as the header states them.
    pub(super) compressed: u32,
    /// Bytes of the body once decompressed.
    pub(super) uncompressed: u32,
    /// Levels in a data page (every row, null and repeated child included), or
    /// entries in a dictionary page.
    pub(super) values: u32,
    /// Rows a data page holds, when its header or the column's shape says so:
    /// a v2 header states them, and a column with no repetition levels has one
    /// level per row.
    pub(super) rows: Option<u32>,
    pub(super) encoding: i32,
}

/// An enumeration value, which a header states in an `i32`.
fn small(value: i64) -> Result<i32, GfError> {
    i32::try_from(value).map_err(|_| out_of_range("enumeration"))
}

fn out_of_range(what: &str) -> GfError {
    storage(format!("Parquet page header {what} is out of range"))
}

/// A Thrift compact-protocol reader that never reads past `limit` bytes.
struct Compact<'a, R: Read> {
    reader: &'a mut R,
    consumed: usize,
}

impl<R: Read> Compact<'_, R> {
    fn byte(&mut self) -> Result<u8, GfError> {
        if self.consumed >= MAX_HEADER_BYTES {
            return Err(storage("Parquet page header exceeds its size limit"));
        }
        let mut byte = [0_u8; 1];
        self.reader.read_exact(&mut byte).map_err(|error| {
            if error.kind() == std::io::ErrorKind::UnexpectedEof {
                storage("Parquet page header is truncated")
            } else {
                storage(error)
            }
        })?;
        self.consumed += 1;
        Ok(byte[0])
    }

    fn varint(&mut self) -> Result<u64, GfError> {
        let mut value = 0_u64;
        for shift in (0..70).step_by(7) {
            let byte = self.byte()?;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(storage("Parquet page header has an overlong integer"))
    }

    #[allow(clippy::cast_possible_wrap)] // zigzag decoding
    fn zigzag(&mut self) -> Result<i64, GfError> {
        let value = self.varint()?;
        Ok(((value >> 1) as i64) ^ -((value & 1) as i64))
    }

    /// Skip `count` bytes without allocating for them.
    fn skip_bytes(&mut self, count: u64) -> Result<(), GfError> {
        if count > (MAX_HEADER_BYTES - self.consumed) as u64 {
            return Err(storage("Parquet page header exceeds its size limit"));
        }
        let mut remaining = count;
        let mut scratch = [0_u8; 256];
        while remaining > 0 {
            let step =
                usize::try_from(remaining.min(scratch.len() as u64)).unwrap_or(scratch.len());
            self.reader
                .read_exact(&mut scratch[..step])
                .map_err(storage)?;
            self.consumed += step;
            remaining -= step as u64;
        }
        Ok(())
    }

    fn skip(&mut self, kind: u8, depth: usize) -> Result<(), GfError> {
        if depth > MAX_DEPTH {
            return Err(storage("Parquet page header nests too deeply"));
        }
        match kind {
            // Booleans in a struct field are carried by the field type itself.
            1 | 2 => Ok(()),
            3 => self.byte().map(|_| ()),
            4..=6 => self.varint().map(|_| ()),
            7 => self.skip_bytes(8),
            8 => {
                let length = self.varint()?;
                self.skip_bytes(length)
            }
            9 | 10 => {
                let header = self.byte()?;
                let mut size = u64::from(header >> 4);
                if size == 15 {
                    size = self.varint()?;
                }
                for _ in 0..size {
                    // Booleans in a list take a byte each.
                    match header & 0x0f {
                        1 | 2 => {
                            self.byte()?;
                        }
                        element => self.skip(element, depth + 1)?,
                    }
                }
                Ok(())
            }
            11 => {
                let size = self.varint()?;
                if size > 0 {
                    let types = self.byte()?;
                    for _ in 0..size {
                        for element in [types >> 4, types & 0x0f] {
                            match element {
                                1 | 2 => {
                                    self.byte()?;
                                }
                                element => self.skip(element, depth + 1)?,
                            }
                        }
                    }
                }
                Ok(())
            }
            12 => self.skip_struct(depth + 1),
            _ => Err(storage("Parquet page header has an unknown field type")),
        }
    }

    fn skip_struct(&mut self, depth: usize) -> Result<(), GfError> {
        while let Some((_, kind)) = self.field(0)? {
            self.skip(kind, depth)?;
        }
        Ok(())
    }

    /// The next field's `(id, type)`, or `None` at the struct's end.
    #[allow(clippy::cast_possible_truncation)] // field ids are i16 on the wire
    fn field(&mut self, previous: i16) -> Result<Option<(i16, u8)>, GfError> {
        let header = self.byte()?;
        if header == 0 {
            return Ok(None);
        }
        let kind = header & 0x0f;
        let delta = header >> 4;
        let id = if delta == 0 {
            self.zigzag()? as i16
        } else {
            previous.wrapping_add(i16::from(delta))
        };
        Ok(Some((id, kind)))
    }

    fn int(&mut self, kind: u8) -> Result<i64, GfError> {
        match kind {
            3 => Ok(i64::from(i8::from_ne_bytes([self.byte()?]))),
            4..=6 => self.zigzag(),
            _ => Err(storage("Parquet page header integer has the wrong type")),
        }
    }
}

/// Fields of `PageHeader` and of its three page-specific structs.
#[derive(Default)]
pub(super) struct RawHeader {
    pub(super) kind: Option<i32>,
    pub(super) uncompressed: Option<i64>,
    pub(super) compressed: Option<i64>,
    pub(super) values: Option<i64>,
    pub(super) encoding: Option<i32>,
    pub(super) rows: Option<i64>,
    pub(super) nulls: Option<i64>,
    pub(super) definition_encoding: Option<i32>,
    pub(super) repetition_encoding: Option<i32>,
    pub(super) definition_bytes: Option<i64>,
    pub(super) repetition_bytes: Option<i64>,
    pub(super) compressed_values: Option<bool>,
    pub(super) sorted_dictionary: Option<bool>,
    pub(super) crc: Option<i64>,
}

fn read_data_page_header<R: Read>(
    input: &mut Compact<'_, R>,
    raw: &mut RawHeader,
    v2: bool,
    depth: usize,
) -> Result<(), GfError> {
    let mut previous = 0;
    while let Some((id, kind)) = input.field(previous)? {
        previous = id;
        match (v2, id) {
            (_, 1) => raw.values = Some(input.int(kind)?),
            (false, 2) | (true, 4) => raw.encoding = Some(small(input.int(kind)?)?),
            (true, 2) => raw.nulls = Some(input.int(kind)?),
            (true, 3) => raw.rows = Some(input.int(kind)?),
            (false, 3) => raw.definition_encoding = Some(small(input.int(kind)?)?),
            (false, 4) => raw.repetition_encoding = Some(small(input.int(kind)?)?),
            (true, 5) => raw.definition_bytes = Some(input.int(kind)?),
            (true, 6) => raw.repetition_bytes = Some(input.int(kind)?),
            (true, 7) if kind == 1 || kind == 2 => raw.compressed_values = Some(kind == 1),
            _ => input.skip(kind, depth)?,
        }
    }
    Ok(())
}

fn read_dictionary_page_header<R: Read>(
    input: &mut Compact<'_, R>,
    raw: &mut RawHeader,
    depth: usize,
) -> Result<(), GfError> {
    let mut previous = 0;
    while let Some((id, kind)) = input.field(previous)? {
        previous = id;
        match id {
            1 => raw.values = Some(input.int(kind)?),
            2 => raw.encoding = Some(small(input.int(kind)?)?),
            3 if kind == 1 || kind == 2 => raw.sorted_dictionary = Some(kind == 1),
            _ => input.skip(kind, depth)?,
        }
    }
    Ok(())
}

/// Parse one page header from `reader`; returns its length in bytes.
pub(super) fn read_header<R: Read>(reader: &mut R) -> Result<(usize, RawHeader), GfError> {
    let mut input = Compact {
        reader,
        consumed: 0,
    };
    let mut raw = RawHeader::default();
    let mut previous = 0;
    while let Some((id, kind)) = input.field(previous)? {
        previous = id;
        match id {
            1 => raw.kind = Some(small(input.int(kind)?)?),
            2 => raw.uncompressed = Some(input.int(kind)?),
            3 => raw.compressed = Some(input.int(kind)?),
            4 => raw.crc = Some(input.int(kind)?),
            5 if kind == 12 => read_data_page_header(&mut input, &mut raw, false, 1)?,
            7 if kind == 12 => read_dictionary_page_header(&mut input, &mut raw, 1)?,
            8 if kind == 12 => read_data_page_header(&mut input, &mut raw, true, 1)?,
            _ => input.skip(kind, 1)?,
        }
    }
    Ok((input.consumed, raw))
}

/// The pages of one column chunk, in file order.
///
/// Reads each page's header (never its body) and checks it against the bytes
/// that remain in the chunk. `file` is a plain handle, not an observed one, so
/// the scan does not feed the source digest out of order.
pub(super) fn scan_chunk(
    file: &mut File,
    chunk: &ColumnChunkMetaData,
) -> Result<Vec<PageFact>, GfError> {
    let (start, length) = chunk.byte_range();
    let end = start
        .checked_add(length)
        .ok_or_else(|| storage("Parquet column chunk range overflows"))?;
    let flat = chunk.column_descr().max_rep_level() == 0;
    let mut pages = Vec::new();
    let mut offset = start;
    let mut data_values = 0_i64;
    while offset < end {
        file.seek(SeekFrom::Start(offset)).map_err(storage)?;
        let mut reader = BufReader::with_capacity(4096, &mut *file);
        let (header_len, raw) = read_header(&mut reader)?;
        let compressed = u64::try_from(raw.compressed.ok_or_else(|| out_of_range("size"))?)
            .map_err(|_| out_of_range("compressed size"))?;
        let uncompressed = u32::try_from(raw.uncompressed.ok_or_else(|| out_of_range("size"))?)
            .map_err(|_| out_of_range("uncompressed size"))?;
        let body = offset + header_len as u64;
        if body > end || compressed > end - body {
            return Err(storage("Parquet page extends beyond its column chunk"));
        }
        let kind = match raw.kind.ok_or_else(|| out_of_range("type"))? {
            DICTIONARY_PAGE => PageKind::Dictionary,
            DATA_PAGE | DATA_PAGE_V2 => PageKind::Data,
            // An index page carries no rows; the decoder skips it.
            INDEX_PAGE => {
                offset = body + compressed;
                continue;
            }
            _ => return Err(storage("Parquet page header has an unknown page type")),
        };
        let values = u32::try_from(raw.values.ok_or_else(|| out_of_range("value count"))?)
            .map_err(|_| out_of_range("value count"))?;
        let rows = match (raw.rows, kind) {
            (Some(rows), _) => Some(u32::try_from(rows).map_err(|_| out_of_range("row count"))?),
            (None, PageKind::Data) if flat => Some(values),
            _ => None,
        };
        if kind == PageKind::Data {
            data_values += i64::from(values);
        }
        pages.push(PageFact {
            kind,
            compressed: u32::try_from(compressed).map_err(|_| out_of_range("compressed size"))?,
            uncompressed,
            values,
            rows,
            encoding: raw.encoding.ok_or_else(|| out_of_range("encoding"))?,
        });
        offset = body + compressed;
    }
    // The footer states the chunk's uncompressed total. Writers differ in what
    // they count (headers, the dictionary) and some leave it zero, so only a claim
    // far past a stated total is a corrupt length.
    let claimed = pages
        .iter()
        .map(|page| u64::from(page.uncompressed))
        .sum::<u64>();
    let stated = u64::try_from(chunk.uncompressed_size()).unwrap_or(0);
    if stated > 0 && claimed > stated.saturating_mul(2).saturating_add(1 << 20) {
        return Err(storage(
            "Parquet pages claim more uncompressed bytes than the footer states",
        ));
    }
    if data_values != chunk.num_values() {
        return Err(storage(
            "Parquet column chunk pages disagree with the footer's value count",
        ));
    }
    Ok(pages)
}

/// Decoded form of a column's leaf values, as Arrow holds them.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Leaf {
    /// Every value takes this many bytes.
    Fixed(u64),
    /// Byte-array values of any length.
    Variable,
}

/// The widest Arrow value a leaf of this Parquet column decodes to.
pub(super) fn leaf_of(descriptor: &ColumnDescriptor) -> Leaf {
    use parquet::basic::{ConvertedType, LogicalType, Type};
    let decimal = descriptor.converted_type() == ConvertedType::DECIMAL
        || matches!(
            descriptor.logical_type_ref(),
            Some(LogicalType::Decimal { .. })
        );
    match descriptor.physical_type() {
        Type::BOOLEAN => Leaf::Fixed(1),
        Type::INT32 | Type::INT64 if decimal => Leaf::Fixed(16),
        Type::INT32 | Type::FLOAT => Leaf::Fixed(4),
        Type::INT64 | Type::DOUBLE => Leaf::Fixed(8),
        Type::INT96 => Leaf::Fixed(12),
        Type::FIXED_LEN_BYTE_ARRAY => {
            let length = u64::try_from(descriptor.type_length()).unwrap_or(0);
            Leaf::Fixed(if decimal {
                length.max(16).next_multiple_of(16)
            } else {
                length
            })
        }
        Type::BYTE_ARRAY => Leaf::Variable,
    }
}

/// What a decode of one column chunk holds besides the batch it produces.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct ChunkSummary {
    /// The largest decompressed data page.
    pub(super) data_page: u64,
    /// The largest compressed page, held beside its decompressed form while the
    /// decoder decompresses it.
    pub(super) compressed_page: u64,
    /// The dictionary page, decompressed.
    pub(super) dictionary_page: u64,
    /// Entries in the dictionary, which decode into an offset each.
    pub(super) dictionary_entries: u64,
    /// Some data page holds dictionary indices, which expand per row.
    pub(super) dictionary_encoded: bool,
    /// Some data page stores each value as a prefix shared with the one before,
    /// so its decoded size is not bounded by its bytes.
    pub(super) delta_byte_array: bool,
}

impl ChunkSummary {
    pub(super) fn of(pages: &[PageFact]) -> Self {
        let mut summary = Self::default();
        for page in pages {
            summary.compressed_page = summary.compressed_page.max(u64::from(page.compressed));
            match page.kind {
                PageKind::Dictionary => {
                    summary.dictionary_page += u64::from(page.uncompressed);
                    summary.dictionary_entries += u64::from(page.values);
                }
                PageKind::Data => {
                    summary.data_page = summary.data_page.max(u64::from(page.uncompressed));
                    summary.dictionary_encoded |= matches!(
                        page.encoding,
                        encoding::PLAIN_DICTIONARY | encoding::RLE_DICTIONARY
                    );
                    summary.delta_byte_array |= page.encoding == encoding::DELTA_BYTE_ARRAY;
                }
            }
        }
        summary
    }

    /// Bytes a decoder holds for this column while it advances through the
    /// chunk: the decompressed page it is reading and the dictionary it
    /// decoded (pages plus an offset per entry).
    pub(super) fn resident(&self) -> u64 {
        self.data_page
            .saturating_add(self.dictionary_page)
            .saturating_add(self.dictionary_entries.saturating_mul(4))
    }
}

/// A row group's page facts for every leaf column.
pub(super) struct GroupScan {
    pub(super) leaves: Vec<LeafScan>,
}

/// One leaf column of a row group.
pub(super) struct LeafScan {
    pub(super) leaf: Leaf,
    pub(super) nested: bool,
    /// The pages' decompressed bytes bound a batch of this column too coarsely to
    /// admit by, so its batches are sized from the values.
    pub(super) exact: bool,
    pub(super) summary: ChunkSummary,
    /// Kept only for columns a batch's size depends on the pages of: byte arrays
    /// and nested values. Fixed-width flat columns are sized by arithmetic.
    pub(super) pages: Option<Vec<PageFact>>,
}

impl GroupScan {
    /// Bytes a decoder holds opening this row group before it reads a row: every
    /// column's decompressed page and dictionary, and the one compressed page it
    /// is decompressing.
    pub(super) fn pages_resident(&self) -> u64 {
        let resident = self
            .leaves
            .iter()
            .map(|leaf| leaf.summary.resident())
            .fold(0_u64, u64::saturating_add);
        let transient = self
            .leaves
            .iter()
            .map(|leaf| leaf.summary.compressed_page)
            .max()
            .unwrap_or(0);
        resident.saturating_add(transient)
    }
}

/// Scan every leaf column of row group `group`.
pub(super) fn scan_group(
    file: &mut File,
    metadata: &ParquetMetaData,
    group: usize,
) -> Result<GroupScan, GfError> {
    let mut leaves = Vec::new();
    for chunk in metadata.row_group(group).columns() {
        let descriptor = chunk.column_descr();
        let leaf = leaf_of(descriptor);
        let nested = descriptor.max_rep_level() > 0;
        let pages = scan_chunk(file, chunk)?;
        let summary = ChunkSummary::of(&pages);
        leaves.push(LeafScan {
            leaf,
            nested,
            exact: false,
            summary,
            pages: (leaf == Leaf::Variable || nested).then_some(pages),
        });
    }
    Ok(GroupScan { leaves })
}

/// Refuse a page that no workspace of `capacity` bytes could hold, before any
/// reservation is attempted for it.
pub(super) fn require_page_fits(summary: &ChunkSummary, capacity: u64) -> Result<(), GfError> {
    let largest = summary
        .data_page
        .max(summary.compressed_page)
        .max(summary.dictionary_page);
    if largest > capacity {
        return Err(limit(format!(
            "a Parquet page of {largest} bytes exceeds the source workspace of {capacity} bytes"
        )));
    }
    Ok(())
}
