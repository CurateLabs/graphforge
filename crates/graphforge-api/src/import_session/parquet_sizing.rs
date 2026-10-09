//! Bounded value sizing from the same owned pages admitted by the runtime reader.
//!
//! This pass retains only row scalars and fixed 1024-element work blocks. It
//! validates the exact level/value streams before adding the existing Arrow
//! sizing formula to a row's logical batch.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

use graphforge_core::GfError;
use parquet::basic::{Encoding, Type};
use parquet::column::page::Page;
use parquet::file::metadata::ColumnChunkMetaData;

use crate::CancellationToken;

use super::inventory_budget::InventoryBudget;
use super::parquet_delta;
use super::parquet_events::PageEvents;
use super::parquet_levels::{
    DictionaryIndices, HybridLevels, IndexSource, LevelSource, MAX_BLOCK_EVENTS,
};
use super::parquet_page_decode::DecodedPage;
use super::parquet_reader::{OwnedPageReader, PageFailures, PagePreflight};
use super::parquet_values::{DictionaryByteFacts, DictionaryExpanded, LengthSource, ValueLengths};
use super::{cancelled, limit, storage};

const OFFSET_BYTES: u64 = 4;
const REPEATED_CHILD_BYTES: u64 = 8;
const WORKSPACE_RESERVE: u64 = (MAX_BLOCK_EVENTS as u64) * (2 * 2 + 8 + 4) + 1024;

fn check_cancel(cancellation: Option<&CancellationToken>) -> Result<(), GfError> {
    if cancellation.is_some_and(CancellationToken::is_cancelled) {
        Err(cancelled())
    } else {
        Ok(())
    }
}

enum DictionaryFacts {
    Bytes(DictionaryByteFacts),
    Fixed { entries: usize },
}

impl DictionaryFacts {
    fn entries(&self) -> usize {
        match self {
            Self::Bytes(facts) => facts.entries(),
            Self::Fixed { entries } => *entries,
        }
    }
}

enum ValueCursor<'a, 'f> {
    Lengths(ValueLengths<'a>),
    DictionaryBytes(DictionaryExpanded<'a, 'f>),
    DictionaryFixed(DictionaryIndices<'a>, u64, usize, usize),
    Fixed(u64, usize, usize),
    DeltaFixedLength(ValueLengths<'a>, u64),
    DeltaFixed(u64, parquet_delta::DeltaFacts, usize),
    BooleanRle(HybridLevels<'a>, usize),
}

impl ValueCursor<'_, '_> {
    fn next_length(&mut self) -> Result<Option<u64>, GfError> {
        match self {
            Self::Lengths(source) => source.next_length(),
            Self::DictionaryBytes(source) => source.next_length(),
            Self::DeltaFixedLength(source, width) => match source.next_length()? {
                Some(length) if length == *width => Ok(Some(*width)),
                Some(_) => Err(storage(
                    "Parquet fixed-length delta value differs from its descriptor width",
                )),
                None => Ok(None),
            },
            Self::DictionaryFixed(indices, width, expected, emitted) => {
                if *emitted == *expected {
                    return Ok(None);
                }
                indices
                    .next_index()?
                    .ok_or_else(|| storage("Parquet dictionary index stream is incomplete"))?;
                *emitted = emitted
                    .checked_add(1)
                    .ok_or_else(|| storage("Parquet dictionary event count overflows"))?;
                Ok(Some(*width))
            }
            Self::Fixed(width, expected, emitted) => {
                if *emitted == *expected {
                    return Ok(None);
                }
                *emitted = emitted
                    .checked_add(1)
                    .ok_or_else(|| storage("Parquet fixed value count overflows"))?;
                Ok(Some(*width))
            }
            Self::DeltaFixed(width, facts, emitted) => {
                if *emitted == facts.values {
                    return Ok(None);
                }
                *emitted = emitted
                    .checked_add(1)
                    .ok_or_else(|| storage("Parquet delta value count overflows"))?;
                Ok(Some(*width))
            }
            Self::BooleanRle(levels, expected) => {
                if levels.emitted() == *expected {
                    return Ok(None);
                }
                let value = levels
                    .next_level()?
                    .ok_or_else(|| storage("Parquet RLE boolean stream is incomplete"))?;
                let _ = value;
                Ok(Some(1))
            }
        }
    }

    fn emitted(&self) -> usize {
        match self {
            Self::Lengths(source) => source.emitted(),
            Self::DictionaryBytes(source) => source.emitted(),
            Self::DeltaFixedLength(source, _) => source.emitted(),
            Self::DictionaryFixed(_, _, _, emitted) | Self::Fixed(_, _, emitted) => *emitted,
            Self::DeltaFixed(_, facts, emitted) => (*emitted).min(facts.values),
            Self::BooleanRle(levels, _) => levels.emitted(),
        }
    }
}

struct SizingPreflight<'a, 'b, F> {
    physical: Type,
    fixed_width: u64,
    max_rep: i16,
    max_def: i16,
    rows: u64,
    row_base: u64,
    capacity: u64,
    budget: &'a mut InventoryBudget,
    cancellation: Option<CancellationToken>,
    dictionary: Option<DictionaryFacts>,
    dictionary_charge: u64,
    row_slots: u64,
    row_payload: u64,
    row_open: bool,
    finished_rows: u64,
    add_row: &'b mut F,
}

fn open_page_values<'a, 'f>(
    physical: Type,
    fixed_width: u64,
    dictionary: Option<&'f DictionaryFacts>,
    encoding: Encoding,
    suffix: &'a [u8],
    nonnull: usize,
    cancellation: Option<&'a CancellationToken>,
) -> Result<ValueCursor<'a, 'f>, GfError> {
    match encoding {
        Encoding::PLAIN => match physical {
            Type::BYTE_ARRAY => Ok(ValueCursor::Lengths(ValueLengths::new(
                encoding,
                suffix,
                nonnull,
                cancellation,
            )?)),
            Type::BOOLEAN => {
                let needed = nonnull.div_ceil(8);
                if suffix.len() < needed {
                    return Err(storage("Parquet plain boolean values are truncated"));
                }
                Ok(ValueCursor::Fixed(1, nonnull, 0))
            }
            _ => {
                require_prefix(suffix, nonnull, fixed_width)?;
                Ok(ValueCursor::Fixed(fixed_width, nonnull, 0))
            }
        },
        Encoding::DELTA_LENGTH_BYTE_ARRAY if physical == Type::BYTE_ARRAY => Ok(
            ValueCursor::Lengths(ValueLengths::new(encoding, suffix, nonnull, cancellation)?),
        ),
        Encoding::DELTA_BYTE_ARRAY if physical == Type::BYTE_ARRAY => Ok(ValueCursor::Lengths(
            ValueLengths::new(encoding, suffix, nonnull, cancellation)?,
        )),
        Encoding::DELTA_BYTE_ARRAY if physical == Type::FIXED_LEN_BYTE_ARRAY => {
            Ok(ValueCursor::DeltaFixedLength(
                ValueLengths::new(encoding, suffix, nonnull, cancellation)?,
                fixed_width,
            ))
        }
        Encoding::PLAIN_DICTIONARY | Encoding::RLE_DICTIONARY => {
            let dictionary = dictionary.ok_or_else(|| {
                storage("Parquet dictionary page is missing before dictionary indices")
            })?;
            if nonnull == 0 {
                return Ok(ValueCursor::Fixed(fixed_width, 0, 0));
            }
            match dictionary {
                DictionaryFacts::Bytes(facts) => Ok(ValueCursor::DictionaryBytes(
                    DictionaryExpanded::new(Some(facts), suffix, nonnull, cancellation)?,
                )),
                DictionaryFacts::Fixed { entries } => Ok(ValueCursor::DictionaryFixed(
                    DictionaryIndices::new(suffix, *entries, nonnull)?,
                    fixed_width,
                    nonnull,
                    0,
                )),
            }
        }
        Encoding::BYTE_STREAM_SPLIT => {
            if !matches!(
                physical,
                Type::INT32 | Type::INT64 | Type::FLOAT | Type::DOUBLE | Type::FIXED_LEN_BYTE_ARRAY
            ) {
                return Err(storage(
                    "BYTE_STREAM_SPLIT is invalid for this Parquet type",
                ));
            }
            require_prefix(suffix, nonnull, fixed_width)?;
            Ok(ValueCursor::Fixed(fixed_width, nonnull, 0))
        }
        Encoding::DELTA_BINARY_PACKED if matches!(physical, Type::INT32 | Type::INT64) => {
            let width = if physical == Type::INT32 { 32 } else { 64 };
            let facts = parquet_delta::validate(encoding, suffix, nonnull, width)?
                .ok_or_else(|| storage("Parquet delta integer facts are missing"))?;
            if facts.values != nonnull {
                return Err(storage(
                    "Parquet delta integer count differs from nonnull values",
                ));
            }
            Ok(ValueCursor::DeltaFixed(fixed_width, facts, 0))
        }
        Encoding::RLE if physical == Type::BOOLEAN => {
            let values = length_prefixed(suffix)?;
            let values = HybridLevels::new(values, 1, nonnull)?;
            Ok(ValueCursor::BooleanRle(values, nonnull))
        }
        _ => Err(storage("unsupported Parquet sizing value encoding")),
    }
}

fn finish_row<F>(
    row_base: u64,
    finished_rows: u64,
    payload: u64,
    slots: u64,
    max_rep: i16,
    add_row: &mut F,
) -> Result<(), GfError>
where
    F: FnMut(u64, u64) -> Result<(), GfError>,
{
    let bytes = payload
        .checked_add(OFFSET_BYTES)
        .and_then(|value| value.checked_add(slots.div_ceil(8)))
        .and_then(|value| {
            if max_rep > 0 {
                slots.checked_mul(REPEATED_CHILD_BYTES)?.checked_add(value)
            } else {
                Some(value)
            }
        })
        .ok_or_else(|| storage("Parquet row Arrow size overflows"))?;
    let row = row_base
        .checked_add(finished_rows)
        .ok_or_else(|| storage("Parquet row index overflows"))?;
    add_row(row, bytes)
}

impl<F> SizingPreflight<'_, '_, F>
where
    F: FnMut(u64, u64) -> Result<(), GfError> + Send,
{
    fn check_cancel(&self) -> Result<(), GfError> {
        check_cancel(self.cancellation.as_ref())
    }

    fn process_data_page(&mut self, decoded: &DecodedPage) -> Result<(), GfError> {
        self.check_cancel()?;
        let (encoding, page) = match &decoded.page {
            Page::DataPage { encoding, .. } | Page::DataPageV2 { encoding, .. } => {
                (*encoding, &decoded.page)
            }
            _ => return Ok(()),
        };
        let mut events = PageEvents::new(page, self.max_rep, self.max_def)?;
        let summary = events.validated_summary(self.cancellation.as_ref())?;
        let nonnull = usize::try_from(summary.nonnull).map_err(storage)?;
        let suffix = events.value_suffix();
        let physical = self.physical;
        let fixed_width = self.fixed_width;
        let max_rep = self.max_rep;
        let max_def = self.max_def;
        let dictionary = self.dictionary.as_ref();
        let cancellation = self.cancellation.as_ref();
        let mut values = open_page_values(
            physical,
            fixed_width,
            dictionary,
            encoding,
            suffix,
            nonnull,
            cancellation,
        )?;
        let mut repetition = [0_i16; MAX_BLOCK_EVENTS];
        let mut definition = [0_i16; MAX_BLOCK_EVENTS];
        let mut seen_nonnull = 0_usize;
        let mut row_slots = self.row_slots;
        let mut row_payload = self.row_payload;
        let mut row_open = self.row_open;
        let mut finished_rows = self.finished_rows;
        let row_base = self.row_base;
        let rows = self.rows;
        let add_row = &mut *self.add_row;
        loop {
            check_cancel(cancellation)?;
            let count = events.next_block(&mut repetition, &mut definition, cancellation)?;
            for index in 0..count {
                let rep = repetition[index];
                if rep == 0 {
                    if row_open {
                        finish_row(
                            row_base,
                            finished_rows,
                            row_payload,
                            row_slots,
                            max_rep,
                            add_row,
                        )?;
                        finished_rows = finished_rows
                            .checked_add(1)
                            .ok_or_else(|| storage("Parquet completed row count overflows"))?;
                    }
                    if finished_rows >= rows {
                        return Err(storage("Parquet values exceed the row-group footer count"));
                    }
                    row_open = true;
                    row_slots = 0;
                    row_payload = 0;
                } else if !row_open {
                    return Err(storage(
                        "Parquet repeated value continues before a row starts",
                    ));
                }
                row_slots = row_slots
                    .checked_add(1)
                    .ok_or_else(|| storage("Parquet row value count overflows"))?;
                if definition[index] == max_def {
                    let length = values.next_length()?.ok_or_else(|| {
                        storage("Parquet value stream ends before nonnull levels")
                    })?;
                    let length = if physical == Type::BYTE_ARRAY && max_rep > 0 {
                        length
                            .checked_add(OFFSET_BYTES)
                            .ok_or_else(|| storage("Parquet repeated value size overflows"))?
                    } else {
                        length
                    };
                    row_payload = row_payload
                        .checked_add(length)
                        .ok_or_else(|| storage("Parquet row payload size overflows"))?;
                    seen_nonnull = seen_nonnull
                        .checked_add(1)
                        .ok_or_else(|| storage("Parquet nonnull count overflows"))?;
                }
            }
            if count == 0 {
                break;
            }
        }
        if seen_nonnull != nonnull || values.emitted() != nonnull {
            return Err(storage(
                "Parquet value lengths disagree with validated nonnull levels",
            ));
        }
        self.row_slots = row_slots;
        self.row_payload = row_payload;
        self.row_open = row_open;
        self.finished_rows = finished_rows;
        Ok(())
    }

    fn dictionary_page(&mut self, decoded: &DecodedPage) -> Result<(), GfError> {
        if self.dictionary.is_some() {
            return Err(storage("Parquet column has more than one dictionary page"));
        }
        let Page::DictionaryPage {
            buf,
            num_values,
            encoding,
            ..
        } = &decoded.page
        else {
            return Ok(());
        };
        if !matches!(encoding, Encoding::PLAIN | Encoding::PLAIN_DICTIONARY) {
            return Err(storage(
                "Parquet dictionary page does not use PLAIN encoding",
            ));
        }
        let entries = usize::try_from(*num_values).map_err(storage)?;
        if self.physical == Type::BYTE_ARRAY {
            let live = self.budget.live_bytes();
            let available = self
                .capacity
                .checked_sub(live)
                .ok_or_else(|| limit("Parquet sizing inventory exceeds its source workspace"))?;
            let body = u64::try_from(decoded.body_capacity).map_err(storage)?;
            let credit = available.checked_sub(body).ok_or_else(|| {
                limit("Parquet dictionary facts do not fit beside the owned page")
            })?;
            let credit_usize = usize::try_from(credit).map_err(storage)?;
            self.budget
                .admit(credit, "sizing dictionary value lengths")?;
            let facts = match DictionaryByteFacts::new(
                buf,
                entries,
                credit_usize,
                self.cancellation.as_ref(),
            ) {
                Ok(facts) => facts,
                Err(error) => {
                    self.budget.release(credit);
                    return Err(error);
                }
            };
            let actual = facts.inventory_bytes()?;
            self.budget
                .release(credit.checked_sub(actual).ok_or_else(|| {
                    limit("Parquet dictionary facts exceed their admitted workspace")
                })?);
            self.dictionary_charge = actual;
            self.dictionary = Some(DictionaryFacts::Bytes(facts));
        } else {
            if self.physical == Type::BOOLEAN {
                if buf.len() < entries.div_ceil(8) {
                    return Err(storage("Parquet dictionary boolean values are truncated"));
                }
            } else {
                require_prefix(buf, entries, self.fixed_width)?;
            }
            self.dictionary = Some(DictionaryFacts::Fixed { entries });
        }
        Ok(())
    }
}

impl<F> PagePreflight for SizingPreflight<'_, '_, F>
where
    F: FnMut(u64, u64) -> Result<(), GfError> + Send,
{
    fn remaining_workspace(&self) -> Result<usize, GfError> {
        let available = self
            .capacity
            .checked_sub(self.budget.live_bytes())
            .ok_or_else(|| limit("Parquet sizing inventory exceeds its source workspace"))?;
        usize::try_from(available).map_err(storage)
    }

    fn validate(&mut self, page: &DecodedPage) -> Result<(), GfError> {
        self.check_cancel()?;
        match &page.page {
            Page::DictionaryPage { .. } => self.dictionary_page(page),
            Page::DataPage { .. } | Page::DataPageV2 { .. } => self.process_data_page(page),
            _ => Ok(()),
        }
    }

    fn finish(&mut self) -> Result<(), GfError> {
        self.check_cancel()?;
        if self.row_open {
            finish_row(
                self.row_base,
                self.finished_rows,
                self.row_payload,
                self.row_slots,
                self.max_rep,
                self.add_row,
            )?;
            self.finished_rows = self
                .finished_rows
                .checked_add(1)
                .ok_or_else(|| storage("Parquet completed row count overflows"))?;
            self.row_open = false;
        }
        if self.finished_rows != self.rows {
            return Err(storage(
                "Parquet values disagree with the row-group footer count",
            ));
        }
        // The retained length allocation must be dead before its credit can
        // become available to the next physical column's sizing pass.
        self.dictionary = None;
        if self.dictionary_charge > 0 {
            self.budget.release(self.dictionary_charge);
            self.dictionary_charge = 0;
        }
        Ok(())
    }
}

/// Size one physical leaf by validating its owned page stream.
pub(super) fn size_column<F>(
    file: &File,
    column: &ColumnChunkMetaData,
    rows: u64,
    row_base: u64,
    capacity: u64,
    budget: &mut InventoryBudget,
    cancellation: Option<&CancellationToken>,
    add_row: &mut F,
) -> Result<(), GfError>
where
    F: FnMut(u64, u64) -> Result<(), GfError> + Send,
{
    let descriptor = column.column_descr();
    let physical = descriptor.physical_type();
    let fixed_width = match physical {
        Type::BOOLEAN => 1,
        Type::INT32 | Type::FLOAT => 4,
        Type::INT64 | Type::DOUBLE => 8,
        Type::INT96 => 12,
        Type::FIXED_LEN_BYTE_ARRAY => u64::try_from(descriptor.type_length()).map_err(storage)?,
        Type::BYTE_ARRAY => 0,
    };
    let start = column
        .dictionary_page_offset()
        .unwrap_or_else(|| column.data_page_offset());
    let start = u64::try_from(start).map_err(storage)?;
    let chunk_bytes = u64::try_from(column.compressed_size()).map_err(storage)?;
    let end = start
        .checked_add(chunk_bytes)
        .ok_or_else(|| storage("Parquet sizing column range overflows"))?;
    if end > file.metadata().map_err(storage)?.len() {
        return Err(storage("Parquet sizing column range exceeds the source"));
    }
    let mut reader = file.try_clone().map_err(storage)?;
    reader.seek(SeekFrom::Start(start)).map_err(storage)?;
    let chunk_reader = reader.take(chunk_bytes);
    let expected_data_events = u64::try_from(column.num_values()).map_err(storage)?;
    let failures = PageFailures::new();
    let cancellation = cancellation.cloned();
    let mut preflight = SizingPreflight {
        physical,
        fixed_width,
        max_rep: descriptor.max_rep_level(),
        max_def: descriptor.max_def_level(),
        rows,
        row_base,
        capacity,
        budget,
        cancellation: cancellation.clone(),
        dictionary: None,
        dictionary_charge: 0,
        row_slots: 0,
        row_payload: 0,
        row_open: false,
        finished_rows: 0,
        add_row,
    };
    preflight
        .budget
        .admit(WORKSPACE_RESERVE, "bounded Parquet sizing cursors")?;
    let page_reader = OwnedPageReader::new(
        chunk_reader,
        chunk_bytes,
        column.compression(),
        expected_data_events,
        cancellation,
        failures.clone(),
        preflight,
    );
    let mut page_reader = page_reader;
    while let Some(result) = page_reader.next() {
        if let Err(error) = result {
            return Err(failures.take().unwrap_or_else(|| storage(error)));
        }
    }
    drop(page_reader);
    if let Some(error) = failures.take() {
        return Err(error);
    }
    budget.release(WORKSPACE_RESERVE);
    Ok(())
}

fn require_prefix(body: &[u8], count: usize, width: u64) -> Result<(), GfError> {
    let bytes = u64::try_from(count)
        .map_err(storage)?
        .checked_mul(width)
        .ok_or_else(|| limit("Parquet fixed value byte count overflows"))?;
    if u64::try_from(body.len()).map_err(storage)? < bytes {
        return Err(storage("Parquet fixed-width value body is truncated"));
    }
    Ok(())
}

fn length_prefixed(body: &[u8]) -> Result<&[u8], GfError> {
    let prefix = body
        .get(..4)
        .ok_or_else(|| storage("Parquet RLE boolean length prefix is truncated"))?;
    let mut bytes = [0_u8; 4];
    bytes.copy_from_slice(prefix);
    let length = i32::from_le_bytes(bytes);
    let length =
        usize::try_from(length).map_err(|_| storage("Parquet RLE boolean length is negative"))?;
    let end = 4_usize
        .checked_add(length)
        .ok_or_else(|| storage("Parquet RLE boolean length overflows"))?;
    Ok(body
        .get(4..end)
        .ok_or_else(|| storage("Parquet RLE boolean values are truncated"))?)
}

#[cfg(test)]
#[path = "parquet_sizing/tests.rs"]
mod tests;
