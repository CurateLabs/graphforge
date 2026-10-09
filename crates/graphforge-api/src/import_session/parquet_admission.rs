//! What a registered Parquet source decodes to, known before it is decoded
//! (#1918).
//!
//! The footer cannot say how large a batch of rows becomes in Arrow: a
//! dictionary-encoded string column stores each distinct value once and expands
//! it for every row that uses it, a delta-encoded one stores each value as a
//! suffix of its predecessor, and a repeated column holds as many values per row
//! as the row has. A decoder that is only told a row count would allocate
//! whatever those turn out to be. This module sizes every logical batch first,
//! from page headers and, for the encodings whose expansion the headers do not
//! state, from the dictionary indices or lengths themselves (read through the
//! typed column readers, which materialize no Arrow array), so the reader can
//! refuse a batch before its allocation and reserve exactly what it will hold.
//!
//! The sizes are Arrow value bytes (values, offsets and validity), the number
//! the builder's per-batch window is stated in.

use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::sync::Arc;

use bytes::Bytes;
use graphforge_core::GfError;
use parquet::column::reader::{ColumnReader, ColumnReaderImpl};
use parquet::data_type::DataType;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::properties::ReaderProperties;
use parquet::file::reader::{ChunkReader, Length, RowGroupReader};
use parquet::file::serialized_reader::SerializedRowGroupReader;

use super::inventory_budget::{InventoryBudget, reserve};
use super::parquet_scan::{GroupScan, Leaf, LeafScan, PageKind, encoding, scan_group};
use super::{limit, storage};
use crate::CancellationToken;

/// Records the typed readers return per call when sizing a column.
const RECORD_BLOCK: usize = 1_024;
/// Bytes the values of one block of a delta-encoded column may take.
const ORACLE_BLOCK_BYTES: u64 = 16 << 20;
/// Bytes per level entry (two levels and a value slot) an oracle block holds.
const LEVEL_ENTRY_BYTES: u64 = 40;
/// Bytes sizing a column's values may hold at once, however large the budget: the
/// levels and values of the records of one block.
const ORACLE_WORKSPACE_BYTES: u64 = 256 << 20;

/// A plain file as a `ChunkReader`, for the page scan and the sizing readers.
///
/// It does not feed the source digest: these reads come ahead of the decode's
/// own, and the decode's reads are the ones the digest is folded from.
pub(super) struct ScanFile {
    file: File,
    length: u64,
}

impl ScanFile {
    pub(super) fn new(file: File) -> Result<Self, GfError> {
        let length = file.metadata().map_err(storage)?.len();
        Ok(Self { file, length })
    }
}

impl Length for ScanFile {
    fn len(&self) -> u64 {
        self.length
    }
}

impl ChunkReader for ScanFile {
    type T = BufReader<File>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        let mut file = self.file.try_clone()?;
        file.seek(SeekFrom::Start(start))?;
        Ok(BufReader::with_capacity(1 << 10, file))
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        // A length the file cannot hold is refused before it is allocated.
        if start
            .checked_add(length as u64)
            .is_none_or(|end| end > self.length)
        {
            return Err(parquet::errors::ParquetError::EOF(
                "a read extends beyond the end of the source".into(),
            ));
        }
        let mut bytes = vec![0_u8; length];
        let mut file = self.file.try_clone()?;
        file.seek(SeekFrom::Start(start))?;
        file.read_exact(&mut bytes)?;
        Ok(Bytes::from(bytes))
    }
}

/// Whether a column's batch size has to be read from its values rather than
/// summed from its page headers.
fn needs_values(leaf: &LeafScan) -> bool {
    leaf.nested
        || leaf.exact
        || (leaf.leaf == Leaf::Variable
            && (leaf.summary.dictionary_encoded || leaf.summary.delta_byte_array))
}

/// Arrow bytes a value of a leaf occupies besides its payload: an offset, and a
/// validity bit per slot (added per record, rounded up).
const OFFSET_BYTES: u64 = 4;
/// What each child of a repeated value costs beyond its own bytes while it moves
/// through the build: the pair of 16-bit levels the reader holds for every slot of
/// a batch (4 bytes), and the 32-bit index the sink's sort takes it by (4 bytes).
/// Both scale with the number of children, not with their bytes, so a cell of
/// booleans costs far more than the bits Arrow stores for it.
const REPEATED_CHILD_BYTES: u64 = 8;

/// Everything known about how a source decodes, from its page headers and the
/// values the headers cannot size.
pub(super) struct SourceScan {
    groups: Vec<GroupScan>,
    /// First row of each row group.
    group_start: Vec<u64>,
    batch_rows: u64,
    rows: u64,
    /// Exact Arrow bytes per logical batch of the columns `needs_values`.
    value_bytes: Vec<u64>,
    /// Bytes of this inventory, resident for the life of the plan.
    resident_bytes: u64,
}

impl SourceScan {
    /// Scan the page headers of every row group and size the columns whose
    /// expansion only their values state.
    ///
    /// `capacity` bounds what the sizing reads may hold; a page larger than it
    /// is refused here, before any reader would allocate it. It also bounds the
    /// inventory the scan itself keeps — page facts, leaf scans, row-group
    /// starts, per-batch sizes — charged as it is built, so an inventory the
    /// workspace cannot hold is refused before it is allocated. `window` is the
    /// size past which a batch is refused: a page-bounded size that exceeds it
    /// is replaced by the exact one, so a coarse bound never refuses a batch
    /// that fits.
    pub(super) fn build(
        file: File,
        metadata: &ParquetMetaData,
        batch_rows: u64,
        capacity: u64,
        window: u64,
        cancellation: Option<&CancellationToken>,
    ) -> Result<Self, GfError> {
        let mut scanner = file.try_clone().map_err(storage)?;
        let mut budget = InventoryBudget::new(capacity);
        let mut groups = Vec::new();
        let mut group_start = Vec::new();
        let mut start = 0_u64;
        for index in 0..metadata.num_row_groups() {
            check(cancellation)?;
            reserve(&mut group_start, 1, &mut budget, "the row-group starts")?;
            reserve(&mut groups, 1, &mut budget, "the row-group page inventories")?;
            group_start.push(start);
            start += u64::try_from(metadata.row_group(index).num_rows()).unwrap_or(0);
            let group = scan_group(&mut scanner, metadata, index, &mut budget)?;
            for leaf in &group.leaves {
                super::parquet_scan::require_page_fits(&leaf.summary, capacity)?;
            }
            groups.push(group);
        }
        // Every later index is by the footer's row count; the groups must add up
        // to it.
        if i64::try_from(start).ok() != Some(metadata.file_metadata().num_rows()) {
            return Err(storage(
                "Parquet row groups disagree with the footer's row count",
            ));
        }
        let batches = usize::try_from(start.div_ceil(batch_rows.max(1))).map_err(storage)?;
        let mut value_bytes = Vec::new();
        reserve(&mut value_bytes, batches, &mut budget, "the per-batch sizes")?;
        value_bytes.resize(batches, 0);
        let mut scan = Self {
            groups,
            group_start,
            batch_rows: batch_rows.max(1),
            rows: start,
            value_bytes,
            resident_bytes: 0,
        };
        scan.size_values(
            file.try_clone().map_err(storage)?,
            metadata,
            capacity,
            cancellation,
        )?;
        // A column stored plain is bounded by the pages a batch touches, which
        // overstates a batch smaller than a page. Where that bound alone would
        // refuse a batch, size those columns from their values as well.
        if (0..scan.value_bytes.len() as u64).any(|batch| scan.batch_bytes(batch) > window) {
            for group in &mut scan.groups {
                for leaf in &mut group.leaves {
                    leaf.exact |= leaf.leaf == Leaf::Variable;
                }
            }
            scan.value_bytes.iter_mut().for_each(|bytes| *bytes = 0);
            scan.size_values(file, metadata, capacity, cancellation)?;
        }
        scan.resident_bytes = scan
            .groups
            .iter()
            .map(|group| {
                (std::mem::size_of::<GroupScan>() as u64)
                    + group
                        .leaves
                        .iter()
                        .map(|leaf| {
                            (std::mem::size_of::<LeafScan>() as u64).saturating_add(
                                leaf.pages.as_ref().map_or(0, |pages| {
                                    (pages.capacity()
                                        * std::mem::size_of::<super::parquet_scan::PageFact>())
                                        as u64
                                }),
                            )
                        })
                        .sum::<u64>()
            })
            .sum::<u64>()
            .saturating_add((scan.value_bytes.capacity() * 8) as u64)
            .saturating_add((scan.group_start.capacity() * 8) as u64);
        Ok(scan)
    }

    /// Bytes this inventory keeps resident.
    pub(super) fn resident_bytes(&self) -> u64 {
        self.resident_bytes
    }

    /// Bytes a decoder holds opening the most demanding row group before it
    /// reads a row.
    pub(super) fn pages_resident_max(&self) -> u64 {
        self.groups
            .iter()
            .map(GroupScan::pages_resident)
            .max()
            .unwrap_or(0)
    }

    /// The most a task's decoder holds beside its batches: the largest of the
    /// row groups it reads.
    pub(super) fn pages_resident(&self, first_row: u64, last_row: u64) -> u64 {
        self.overlapping(first_row, last_row)
            .map(|(_, _, _, group)| group.pages_resident())
            .max()
            .unwrap_or(0)
    }

    /// Row groups overlapping `[first, last)`, with the rows of each that fall
    /// in it: `(index, low, high, group)` where `low..high` are rows within the group.
    fn overlapping(
        &self,
        first: u64,
        last: u64,
    ) -> impl Iterator<Item = (usize, u64, u64, &GroupScan)> {
        self.groups
            .iter()
            .enumerate()
            .filter_map(move |(index, group)| {
                let start = self.group_start[index];
                let end = self
                    .group_start
                    .get(index + 1)
                    .copied()
                    .unwrap_or(self.rows);
                if first >= end || last <= start || end <= start {
                    return None;
                }
                Some((
                    index,
                    first.max(start) - start,
                    last.min(end) - start,
                    group,
                ))
            })
    }

    /// Decoded Arrow bytes of every column of a total of `rows`, summed over the
    /// whole source: what a decode that retains its batches holds.
    ///
    /// A page a batch only touches counts for the share of its rows the batch
    /// holds, since this is a total and not a bound on any one batch.
    pub(super) fn decoded_bytes(&self) -> u64 {
        let batches = self.value_bytes.len() as u64;
        (0..batches)
            .map(|batch| self.sum_batch(batch, true))
            .fold(0_u64, u64::saturating_add)
    }

    /// Arrow bytes of logical batch `batch`, an upper bound for the columns the
    /// page headers size (a page the batch touches counts in full) and exact
    /// for the rest.
    pub(super) fn batch_bytes(&self, batch: u64) -> u64 {
        self.sum_batch(batch, false)
    }

    fn sum_batch(&self, batch: u64, apportion: bool) -> u64 {
        let first = batch * self.batch_rows;
        let last = ((batch + 1) * self.batch_rows).min(self.rows);
        let mut bytes = self.value_bytes[usize::try_from(batch).unwrap_or(usize::MAX)];
        for (_, low, high, group) in self.overlapping(first, last) {
            for leaf in &group.leaves {
                if needs_values(leaf) {
                    continue;
                }
                bytes = bytes.saturating_add(window_bytes(leaf, low, high, apportion));
            }
        }
        bytes
    }

    /// Read every column whose batch size the headers cannot state, one row
    /// group at a time, and add its exact Arrow bytes to each batch it touches.
    fn size_values(
        &mut self,
        file: File,
        metadata: &ParquetMetaData,
        capacity: u64,
        cancellation: Option<&CancellationToken>,
    ) -> Result<(), GfError> {
        let reader = Arc::new(ScanFile::new(file)?);
        let properties = Arc::new(ReaderProperties::builder().build());
        for (index, group) in self.groups.iter().enumerate() {
            if !group.leaves.iter().any(needs_values) {
                continue;
            }
            let rows = u64::try_from(metadata.row_group(index).num_rows()).unwrap_or(0);
            let first_row = self.group_start[index];
            let row_group = SerializedRowGroupReader::new(
                Arc::clone(&reader),
                metadata.row_group(index),
                None,
                Arc::clone(&properties),
            )
            .map_err(storage)?;
            for (column, leaf) in group.leaves.iter().enumerate() {
                if !needs_values(leaf) {
                    continue;
                }
                let mut add = |row: u64, bytes: u64| {
                    let batch = (first_row + row) / self.batch_rows;
                    if let Some(slot) = self
                        .value_bytes
                        .get_mut(usize::try_from(batch).unwrap_or(usize::MAX))
                    {
                        *slot = slot.saturating_add(bytes);
                    }
                };
                // Every entry of the dictionary one length: a row's size is that
                // length wherever the indices point, and no index is read.
                if let Some(width) = uniform_dictionary_value(
                    &reader,
                    metadata.row_group(index).column(column),
                    leaf,
                    rows,
                )? {
                    let per_row = width + OFFSET_BYTES;
                    let mut row = 0_u64;
                    while row < rows {
                        let batch_end =
                            (first_row + row) / self.batch_rows * self.batch_rows + self.batch_rows;
                        let step = (batch_end - first_row - row).min(rows - row);
                        add(
                            row,
                            step.saturating_mul(per_row)
                                .saturating_add(step.div_ceil(8)),
                        );
                        row += step;
                    }
                    continue;
                }
                let values = leaf.pages.as_ref().map_or(0, |pages| {
                    pages
                        .iter()
                        .map(|page| u64::from(page.values))
                        .max()
                        .unwrap_or(0)
                });
                // A record is at most as long as the page it sits in, and sizing it
                // holds its levels and values: refuse a page whose records could
                // not be sized inside the workspace before reading any of them.
                let workspace = capacity.min(ORACLE_WORKSPACE_BYTES);
                if values.saturating_mul(LEVEL_ENTRY_BYTES) > workspace {
                    return Err(limit(format!(
                        "a Parquet page of {values} values would need more than the \
                         {workspace}-byte workspace that sizes its records"
                    )));
                }
                // Prefix-shared values are rebuilt one by one, each as long as the
                // page that holds it may be: keep a block of them to a bounded
                // number of bytes however large the pages are.
                let block = if leaf.summary.delta_byte_array {
                    usize::try_from(
                        (ORACLE_BLOCK_BYTES / leaf.summary.data_page.max(1))
                            .clamp(1, RECORD_BLOCK as u64),
                    )
                    .unwrap_or(1)
                } else {
                    RECORD_BLOCK
                };
                let descriptor = metadata.row_group(index).column(column).column_descr_ptr();
                let column_reader = row_group.get_column_reader(column).map_err(storage)?;
                let shape = Shape {
                    max_def: descriptor.max_def_level(),
                    max_rep: descriptor.max_rep_level(),
                    rows,
                    block,
                };
                measure_column(column_reader, &shape, cancellation, &mut add)?;
            }
        }
        Ok(())
    }
}

/// The width every value of a dictionary-encoded byte-array column has, when its
/// dictionary holds entries of one length only and no row is null.
fn uniform_dictionary_value(
    reader: &Arc<ScanFile>,
    chunk: &parquet::file::metadata::ColumnChunkMetaData,
    leaf: &LeafScan,
    rows: u64,
) -> Result<Option<u64>, GfError> {
    use parquet::column::page::{Page, PageReader};
    let Some(pages) = &leaf.pages else {
        return Ok(None);
    };
    let dictionary_only = leaf.leaf == Leaf::Variable
        && !leaf.nested
        && !leaf.summary.delta_byte_array
        && pages
            .iter()
            .filter(|page| page.kind == PageKind::Data)
            .all(|page| {
                matches!(
                    page.encoding,
                    encoding::PLAIN_DICTIONARY | encoding::RLE_DICTIONARY
                )
            });
    let descriptor = chunk.column_descr();
    let no_nulls = descriptor.max_def_level() == 0
        || chunk
            .statistics()
            .is_some_and(|statistics| statistics.null_count_opt() == Some(0));
    if !dictionary_only || !no_nulls || leaf.summary.dictionary_entries == 0 {
        return Ok(None);
    }
    let mut dictionary = parquet::file::serialized_reader::SerializedPageReader::new(
        Arc::clone(reader),
        chunk,
        usize::try_from(rows).map_err(storage)?,
        None,
    )
    .map_err(storage)?;
    let Some(Page::DictionaryPage {
        buf, num_values, ..
    }) = dictionary.get_next_page().map_err(storage)?
    else {
        return Ok(None);
    };
    // Plain byte arrays: a little-endian length, then that many bytes.
    let (mut offset, mut width, mut entries) = (0_usize, None::<u32>, 0_u32);
    while offset < buf.len() {
        let Some(prefix) = buf.get(offset..offset + 4) else {
            return Ok(None);
        };
        let length = u32::from_le_bytes(prefix.try_into().expect("four bytes"));
        if *width.get_or_insert(length) != length {
            return Ok(None);
        }
        offset = offset.saturating_add(4).saturating_add(length as usize);
        entries += 1;
    }
    Ok((offset == buf.len() && entries == num_values)
        .then_some(width)
        .flatten()
        .map(u64::from))
}

/// Size the records of one column through its typed reader.
fn measure_column(
    reader: ColumnReader,
    shape: &Shape,
    cancellation: Option<&CancellationToken>,
    add: &mut impl FnMut(u64, u64),
) -> Result<(), GfError> {
    match reader {
        ColumnReader::BoolColumnReader(mut r) => {
            measure(&mut r, shape, cancellation, |_| 1, add)?;
        }
        ColumnReader::Int32ColumnReader(mut r) => {
            measure(&mut r, shape, cancellation, |_| 4, add)?;
        }
        ColumnReader::Int64ColumnReader(mut r) => {
            measure(&mut r, shape, cancellation, |_| 8, add)?;
        }
        ColumnReader::Int96ColumnReader(mut r) => {
            measure(&mut r, shape, cancellation, |_| 12, add)?;
        }
        ColumnReader::FloatColumnReader(mut r) => {
            measure(&mut r, shape, cancellation, |_| 4, add)?;
        }
        ColumnReader::DoubleColumnReader(mut r) => {
            measure(&mut r, shape, cancellation, |_| 8, add)?;
        }
        ColumnReader::ByteArrayColumnReader(mut r) => {
            // A flat column's record offset is its value's offset; a
            // repeated one has an offset for the list and one per child.
            let offset = if shape.max_rep > 0 { OFFSET_BYTES } else { 0 };
            measure(
                &mut r,
                shape,
                cancellation,
                |value: &parquet::data_type::ByteArray| value.len() as u64 + offset,
                add,
            )?;
        }
        ColumnReader::FixedLenByteArrayColumnReader(mut r) => {
            measure(
                &mut r,
                shape,
                cancellation,
                |value: &parquet::data_type::FixedLenByteArray| {
                    parquet::data_type::ByteArray::from(value.clone()).len() as u64
                },
                add,
            )?;
        }
    }
    Ok(())
}

fn check(cancellation: Option<&CancellationToken>) -> Result<(), GfError> {
    if cancellation.is_some_and(CancellationToken::is_cancelled) {
        return Err(super::cancelled());
    }
    Ok(())
}

/// Arrow bytes rows `[low, high)` of a column the page headers size.
fn window_bytes(leaf: &LeafScan, low: u64, high: u64, apportion: bool) -> u64 {
    let rows = high - low;
    let validity = rows.div_ceil(8);
    match leaf.leaf {
        Leaf::Fixed(width) => rows.saturating_mul(width).saturating_add(validity),
        Leaf::Variable => {
            // Plain pages hold their values back to back, so a page's
            // decompressed bytes bound what any of its rows decode to. Count
            // every page the window touches in full.
            let mut touched = 0_u64;
            let mut position = 0_u64;
            if let Some(pages) = &leaf.pages {
                for page in pages.iter().filter(|page| page.kind == PageKind::Data) {
                    let page_rows = u64::from(page.rows.unwrap_or(page.values));
                    let inside = high
                        .min(position + page_rows)
                        .saturating_sub(low.max(position));
                    if inside > 0 {
                        let bytes = u64::from(page.uncompressed);
                        touched = touched.saturating_add(if apportion {
                            bytes.saturating_mul(inside) / page_rows.max(1)
                        } else {
                            bytes
                        });
                    }
                    position += page_rows;
                }
            }
            touched
                .saturating_add(rows.saturating_mul(OFFSET_BYTES))
                .saturating_add(validity)
        }
    }
}

struct Shape {
    max_def: i16,
    max_rep: i16,
    rows: u64,
    block: usize,
}

/// Read a column's records and report `(row within the group, Arrow bytes)` for
/// each, from the values themselves.
fn measure<T: DataType>(
    reader: &mut ColumnReaderImpl<T>,
    shape: &Shape,
    cancellation: Option<&CancellationToken>,
    value_bytes: impl Fn(&T::T) -> u64,
    add: &mut impl FnMut(u64, u64),
) -> Result<(), GfError> {
    let mut definition = Vec::<i16>::new();
    let mut repetition = Vec::<i16>::new();
    let mut values = Vec::<T::T>::new();
    let mut row = 0_u64;
    while row < shape.rows {
        check(cancellation)?;
        definition.clear();
        repetition.clear();
        values.clear();
        let (records, _, levels) = reader
            .read_records(
                shape.block,
                (shape.max_def > 0).then_some(&mut definition),
                (shape.max_rep > 0).then_some(&mut repetition),
                &mut values,
            )
            .map_err(storage)?;
        if records == 0 {
            return Err(storage("a Parquet column ended before its row count"));
        }
        let mut value = 0_usize;
        let mut record_bytes = 0_u64;
        let mut record_slots = 0_u64;
        let mut open = false;
        let mut finished = 0_usize;
        let per_child = if shape.max_rep > 0 {
            REPEATED_CHILD_BYTES
        } else {
            0
        };
        for level in 0..levels {
            if shape.max_rep == 0 || repetition[level] == 0 {
                if open {
                    add(
                        row,
                        record_bytes
                            + OFFSET_BYTES
                            + record_slots.div_ceil(8)
                            + record_slots * per_child,
                    );
                    row += 1;
                    finished += 1;
                }
                open = true;
                record_bytes = 0;
                record_slots = 0;
            }
            record_slots += 1;
            if shape.max_def == 0 || definition[level] == shape.max_def {
                record_bytes += value_bytes(&values[value]);
                value += 1;
            }
        }
        if open {
            add(
                row,
                record_bytes + OFFSET_BYTES + record_slots.div_ceil(8) + record_slots * per_child,
            );
            row += 1;
            finished += 1;
        }
        if finished != records {
            return Err(storage("a Parquet column returned a partial record"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod inventory_tests;
#[cfg(test)]
mod tests;
