//! Borrowing, bounded cursors for the definition and repetition levels in an
//! already-owned Parquet data page.
//!
//! This module only inspects level streams and reports their shape. It does
//! not read source files, decode values, or copy page data. Both the level
//! streams and value suffix remain borrowed from the caller's `Page`.

use graphforge_core::GfError;
use parquet::basic::Encoding;
use parquet::column::page::Page;

use crate::CancellationToken;

use super::parquet_levels::{
    HybridLevels, ImplicitZero, LevelSource, MAX_BLOCK_EVENTS, PackedLevels, split_v1,
};
use super::{cancelled, storage};

enum Levels<'a> {
    Implicit(ImplicitZero),
    Packed(PackedLevels<'a>),
    Hybrid(HybridLevels<'a>),
}

impl Levels<'_> {
    fn next(&mut self) -> Result<Option<i16>, GfError> {
        match self {
            Self::Implicit(levels) => levels.next_level(),
            Self::Packed(levels) => levels.next_level(),
            Self::Hybrid(levels) => levels.next_level(),
        }
    }
}

#[derive(Clone, Copy)]
enum PageVersion {
    V1,
    V2 { nulls: u32, rows: u32 },
}

/// Complete facts derived from the actual logical level events in a page.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct EventSummary {
    /// Number of logical repetition/definition events.
    pub(super) events: u64,
    /// Events whose definition level reaches the leaf maximum.
    pub(super) nonnull: u64,
    /// Events that start a row (repetition level zero).
    pub(super) row_starts: u64,
    /// First repetition level, if the page has at least one event.
    pub(super) first_repetition: Option<i16>,
}

/// A page-borrowing pair of level cursors and its borrowed value suffix.
///
/// The cursor owns only scalar progress and parser state. Output is supplied
/// by the caller in equal-sized arrays; each call writes at most 1024 events
/// and leaves any unused buffer tail untouched.
pub(super) struct PageEvents<'a> {
    page: &'a Page,
    max_repetition: i16,
    max_definition: i16,
    expected: usize,
    repetition: Levels<'a>,
    definition: Levels<'a>,
    values: &'a [u8],
    version: PageVersion,
    emitted: u64,
    nonnull: u64,
    row_starts: u64,
    first_repetition: Option<i16>,
}

impl<'a> PageEvents<'a> {
    /// Construct cursors over an already-owned public Parquet page.
    ///
    /// Descriptor maxima are the exact path's maxima. Dictionary pages are
    /// rejected because they do not contain data-page level events.
    pub(super) fn new(
        page: &'a Page,
        max_repetition: i16,
        max_definition: i16,
    ) -> Result<Self, GfError> {
        if max_repetition < 0 || max_definition < 0 {
            return Err(storage("Parquet level descriptor has a negative maximum"));
        }
        let expected = usize::try_from(page.num_values()).map_err(storage)?;
        let (version, repetition, definition, values) = match page {
            Page::DataPage {
                buf,
                num_values,
                def_level_encoding,
                rep_level_encoding,
                ..
            } => {
                let sections = split_v1(
                    buf,
                    max_repetition,
                    max_definition,
                    *num_values,
                    *rep_level_encoding,
                    *def_level_encoding,
                )?;
                (
                    PageVersion::V1,
                    make_level(
                        sections.repetition,
                        max_repetition,
                        expected,
                        *rep_level_encoding,
                    )?,
                    make_level(
                        sections.definition,
                        max_definition,
                        expected,
                        *def_level_encoding,
                    )?,
                    sections.values,
                )
            }
            Page::DataPageV2 {
                buf,
                num_nulls,
                num_rows,
                def_levels_byte_len,
                rep_levels_byte_len,
                ..
            } => {
                if *num_nulls > page.num_values() || *num_rows > page.num_values() {
                    return Err(storage("Parquet V2 row/null counts exceed its value count"));
                }
                let rep_len = usize::try_from(*rep_levels_byte_len).map_err(storage)?;
                let def_len = usize::try_from(*def_levels_byte_len).map_err(storage)?;
                let prefix_len = rep_len
                    .checked_add(def_len)
                    .ok_or_else(|| storage("Parquet V2 level lengths overflow"))?;
                let body = buf.as_ref();
                if prefix_len > body.len() {
                    return Err(storage("Parquet V2 level sections exceed the page body"));
                }
                // V2 declares the spans even when a descriptor maximum is
                // zero. They still locate the value suffix; the pinned reader
                // supplies implicit zeroes for that descriptor.
                let repetition_bytes = body
                    .get(..rep_len)
                    .ok_or_else(|| storage("Parquet V2 repetition levels exceed the page body"))?;
                let definition_bytes = body
                    .get(rep_len..prefix_len)
                    .ok_or_else(|| storage("Parquet V2 definition levels exceed the page body"))?;
                let values = body
                    .get(prefix_len..)
                    .ok_or_else(|| storage("Parquet V2 value suffix exceeds the page body"))?;
                (
                    PageVersion::V2 {
                        nulls: *num_nulls,
                        rows: *num_rows,
                    },
                    make_level(
                        Some(repetition_bytes),
                        max_repetition,
                        expected,
                        Encoding::RLE,
                    )?,
                    make_level(
                        Some(definition_bytes),
                        max_definition,
                        expected,
                        Encoding::RLE,
                    )?,
                    values,
                )
            }
            Page::DictionaryPage { .. } => {
                return Err(storage("Parquet dictionary page has no level events"));
            }
        };

        Ok(Self {
            page,
            max_repetition,
            max_definition,
            expected,
            repetition,
            definition,
            values,
            version,
            emitted: 0,
            nonnull: 0,
            row_starts: 0,
            first_repetition: None,
        })
    }

    /// Borrow the value bytes after V1/V2 level sections.
    ///
    /// Call [`Self::validated_summary`] first when the suffix is to be passed
    /// to a value decoder: the summary validates every level event and V2's
    /// declared counts against those events.
    pub(super) fn value_suffix(&self) -> &'a [u8] {
        self.values
    }

    /// Fill caller-owned repetition and definition blocks with validated
    /// levels. Both buffers must have the same length. At most
    /// [`MAX_BLOCK_EVENTS`] entries are written, so larger buffers are safe
    /// and retain their unused tails. A length mismatch is rejected before
    /// either buffer is written.
    pub(super) fn next_block(
        &mut self,
        repetition: &mut [i16],
        definition: &mut [i16],
        cancellation: Option<&CancellationToken>,
    ) -> Result<usize, GfError> {
        if repetition.len() != definition.len() {
            return Err(storage(
                "Parquet repetition and definition level blocks must have matching lengths",
            ));
        }
        if cancellation.is_some_and(CancellationToken::is_cancelled) {
            return Err(cancelled());
        }

        let count = repetition
            .len()
            .min(MAX_BLOCK_EVENTS)
            .min(self.expected.saturating_sub(self.emitted as usize));
        for index in 0..count {
            let rep = self
                .repetition
                .next()?
                .ok_or_else(|| storage("Parquet repetition levels end before page value count"))?;
            let def = self
                .definition
                .next()?
                .ok_or_else(|| storage("Parquet definition levels end before page value count"))?;
            let emitted = self
                .emitted
                .checked_add(1)
                .ok_or_else(|| storage("Parquet level event count overflows"))?;
            let nonnull = self
                .nonnull
                .checked_add(if def == self.max_definition { 1 } else { 0 })
                .ok_or_else(|| storage("Parquet non-null event count overflows"))?;
            let row_starts = self
                .row_starts
                .checked_add(if rep == 0 { 1 } else { 0 })
                .ok_or_else(|| storage("Parquet row-start count overflows"))?;
            self.first_repetition.get_or_insert(rep);
            self.emitted = emitted;
            self.nonnull = nonnull;
            self.row_starts = row_starts;
            repetition[index] = rep;
            definition[index] = def;
        }

        if self.emitted == self.expected as u64 {
            self.validate_v2_counts()?;
        }
        Ok(count)
    }

    /// Restart over the same borrowed page and validate all level events.
    /// This lets callers obtain trustworthy facts before constructing a value
    /// decoder, while preserving this cursor for the runtime pass.
    pub(super) fn validated_summary(
        &self,
        cancellation: Option<&CancellationToken>,
    ) -> Result<EventSummary, GfError> {
        if cancellation.is_some_and(CancellationToken::is_cancelled) {
            return Err(cancelled());
        }
        let mut scan = Self::new(self.page, self.max_repetition, self.max_definition)?;
        let mut repetition = [0_i16; MAX_BLOCK_EVENTS];
        let mut definition = [0_i16; MAX_BLOCK_EVENTS];
        while scan.emitted < scan.expected as u64 {
            scan.next_block(&mut repetition, &mut definition, cancellation)?;
        }
        // Empty pages still need their header counts checked.
        scan.validate_v2_counts()?;
        Ok(EventSummary {
            events: scan.emitted,
            nonnull: scan.nonnull,
            row_starts: scan.row_starts,
            first_repetition: scan.first_repetition,
        })
    }

    fn validate_v2_counts(&self) -> Result<(), GfError> {
        let PageVersion::V2 { nulls, rows } = self.version else {
            return Ok(());
        };
        if self.emitted != self.expected as u64 {
            return Ok(());
        }
        let expected_nulls = u64::from(nulls);
        let actual_nulls = self
            .emitted
            .checked_sub(self.nonnull)
            .ok_or_else(|| storage("Parquet non-null count exceeds level events"))?;
        if actual_nulls != expected_nulls {
            return Err(storage(
                "Parquet V2 declared null count disagrees with definition levels",
            ));
        }
        // The pinned parquet 58.4 reader treats V2 num_rows as the number of
        // repetition-zero events on that page. A V2 page must therefore begin
        // at a row boundary; V1 pages may continue an earlier row.
        if self.expected > 0 && self.max_repetition > 0 && self.first_repetition != Some(0) {
            return Err(storage(
                "Parquet V2 page begins with a continuation repetition level",
            ));
        }
        if self.row_starts != u64::from(rows) {
            return Err(storage(
                "Parquet V2 declared row count disagrees with repetition levels",
            ));
        }
        Ok(())
    }
}

fn make_level<'a>(
    section: Option<&'a [u8]>,
    max_level: i16,
    expected: usize,
    encoding: Encoding,
) -> Result<Levels<'a>, GfError> {
    if max_level == 0 {
        return Ok(Levels::Implicit(ImplicitZero::new(expected)));
    }
    let section = section.ok_or_else(|| storage("Parquet level section is missing"))?;
    match encoding {
        Encoding::RLE => Ok(Levels::Hybrid(HybridLevels::new(
            section, max_level, expected,
        )?)),
        #[allow(deprecated)]
        Encoding::BIT_PACKED => Ok(Levels::Packed(PackedLevels::new(
            section, max_level, expected,
        )?)),
        _ => Err(storage("Unsupported Parquet level encoding")),
    }
}

#[cfg(test)]
#[path = "parquet_events/tests.rs"]
mod tests;
