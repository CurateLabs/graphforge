//! UUID-column page selection for nominated property reads.
//!
//! A property fragment is strictly UUID-sorted, so the UUID column's page
//! index (the minimum and maximum UUID of every data page) bounds which pages
//! can hold a wanted UUID. A nominated read decodes only those candidate pages
//! instead of the whole column, and hands the page-indexed metadata back so the
//! data columns skip the same pages through the offset index.
//!
//! The page index is optional metadata. A fragment without one, with one that
//! cannot be read, or whose bounds disagree with the decoded rows (the sort
//! proof) takes a full decode of the selected row groups instead. That decode
//! raises the corruption error if the fragment is really corrupt, and the
//! fallback is counted so a regression to it is visible.

use std::sync::Arc;

use arrow::array::{Array, FixedSizeBinaryArray, RecordBatch};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReader,
    ParquetRecordBatchReaderBuilder, RowSelection, RowSelector,
};
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use parquet::file::page_index::column_index::ColumnIndexMetaData;

use super::selective_reads::column_leaf;
use super::{CountingChunkReader, GfError, authenticated_arrow_error, corrupt, parquet_error};
use crate::permanent_parquet::POINT_ACCESS_PAGE_ROWS;
use crate::uuid_set::UuidMembership;

/// Selections smaller than this many rows decode the UUID column outright: the
/// page index read costs more than the column.
const PAGED_SELECTION_MIN_ROWS: usize = 2 * POINT_ACCESS_PAGE_ROWS;

/// The rows of a fragment's selected row groups whose UUID is wanted.
pub(super) struct WantedRows {
    /// Relative to the concatenation of the selected row groups, in order.
    pub(super) selection: RowSelection,
    /// UUID-column rows actually decoded to build `selection`.
    pub(super) rows_decoded: u64,
    /// The footer plus the page index, present only when the page plan
    /// validated and produced `selection`. Readers built from it skip the
    /// pages `selection` skips without decompressing them.
    pub(super) paged: Option<ArrowReaderMetadata>,
}

/// The rows of `groups` whose UUID is wanted, as a selection relative to the
/// concatenation of those row groups. Only the UUID column is decoded, and
/// only its candidate pages when the page index proves the order. Order is
/// validated exactly as the row cursor validates it for decoded rows.
pub(super) fn wanted_row_selection(
    source: &CountingChunkReader,
    footer: &ArrowReaderMetadata,
    uuid_field: &str,
    groups: &[usize],
    wanted: &dyn UuidMembership,
    batch_rows: usize,
) -> Result<WantedRows, GfError> {
    let leaf = column_leaf(footer.metadata(), uuid_field)
        .ok_or_else(|| corrupt("property fragment lacks its UUID column"))?;
    let total_rows = selected_rows(footer.metadata(), groups)
        .ok_or_else(|| corrupt("property fragment row counts are invalid"))?;
    if total_rows >= PAGED_SELECTION_MIN_ROWS {
        match paged_selection(source, footer, leaf, groups, wanted, batch_rows, total_rows)? {
            Some(rows) => return Ok(rows),
            None => crate::io_stats::record_property_uuid_index_fallback(),
        }
    }
    full_selection(source, footer, leaf, groups, wanted, batch_rows)
}

fn selected_rows(metadata: &ParquetMetaData, groups: &[usize]) -> Option<usize> {
    groups.iter().try_fold(0_usize, |total, group| {
        let rows = usize::try_from(metadata.row_groups().get(*group)?.num_rows()).ok()?;
        total.checked_add(rows)
    })
}

/// A data page that may hold a wanted UUID.
struct Candidate {
    /// First row, relative to the concatenation of the selected row groups.
    start: usize,
    rows: usize,
    min: [u8; 16],
    max: [u8; 16],
}

struct PagePlan {
    candidates: Vec<Candidate>,
    pages: u64,
}

/// Load the page index beside an already-parsed footer. `None` when the file
/// carries no page index or it cannot be read.
fn load_page_index(
    source: &CountingChunkReader,
    footer: &ArrowReaderMetadata,
) -> Option<ArrowReaderMetadata> {
    let mut reader = ParquetMetaDataReader::new_with_metadata(footer.metadata().as_ref().clone())
        .with_page_index_policy(PageIndexPolicy::Optional);
    reader.read_page_indexes(source).ok()?;
    let metadata = reader.finish().ok()?;
    metadata.column_index()?;
    metadata.offset_index()?;
    ArrowReaderMetadata::try_new(Arc::new(metadata), ArrowReaderOptions::new()).ok()
}

/// Validate the UUID column's page bounds and choose the candidate pages.
/// `None` when the index is incomplete, malformed or contradicts the order the
/// fragment must have; the caller then decodes the whole column.
fn plan_pages(
    metadata: &ParquetMetaData,
    leaf: usize,
    groups: &[usize],
    wanted: &dyn UuidMembership,
) -> Option<PagePlan> {
    let columns = metadata.column_index()?;
    let offsets = metadata.offset_index()?;
    let mut candidates = Vec::new();
    let mut pages = 0_u64;
    let mut group_start = 0_usize;
    let mut prior_max: Option<[u8; 16]> = None;
    for &group in groups {
        let rows = usize::try_from(metadata.row_groups().get(group)?.num_rows()).ok()?;
        let ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(index) = columns.get(group)?.get(leaf)?
        else {
            return None;
        };
        let locations = offsets.get(group)?.get(leaf)?.page_locations();
        if locations.is_empty() || usize::try_from(index.num_pages()).ok()? != locations.len() {
            return None;
        }
        for (page, location) in locations.iter().enumerate() {
            let first = usize::try_from(location.first_row_index).ok()?;
            let end = match locations.get(page + 1) {
                Some(next) => usize::try_from(next.first_row_index).ok()?,
                None => rows,
            };
            if (page == 0 && first != 0) || first >= end || end > rows || index.is_null_page(page) {
                return None;
            }
            let min: [u8; 16] = index.min_value(page)?.try_into().ok()?;
            let max: [u8; 16] = index.max_value(page)?.try_into().ok()?;
            // Pages of one fragment are ascending and disjoint.
            if min > max || prior_max.is_some_and(|before| before >= min) {
                return None;
            }
            prior_max = Some(max);
            pages += 1;
            if wanted.may_contain_in_range(&min, &max) {
                candidates.push(Candidate {
                    start: group_start.checked_add(first)?,
                    rows: end - first,
                    min,
                    max,
                });
            }
        }
        group_start = group_start.checked_add(rows)?;
    }
    Some(PagePlan { candidates, pages })
}

/// Run of selectors merged as rows are pushed; zero-length runs are dropped.
#[derive(Default)]
struct Selectors {
    runs: Vec<RowSelector>,
    covered: usize,
}

impl Selectors {
    fn push(&mut self, select: bool, count: usize) {
        if count == 0 {
            return;
        }
        self.covered += count;
        match self.runs.last_mut() {
            Some(last) if last.skip != select => last.row_count += count,
            _ => self.runs.push(if select {
                RowSelector::select(count)
            } else {
                RowSelector::skip(count)
            }),
        }
    }

    /// Rows the pushed runs span so far.
    fn covered(&self) -> usize {
        self.covered
    }

    fn finish(self) -> RowSelection {
        RowSelection::from(self.runs)
    }
}

/// The decoded UUIDs of one batch of the UUID-only projection.
fn batch_uuids(batch: &RecordBatch) -> Result<&FixedSizeBinaryArray, GfError> {
    let column = batch
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .filter(|column| column.value_length() == 16)
        .ok_or_else(|| corrupt("property UUID column is not FixedSizeBinary(16)"))?;
    if column.null_count() != 0 {
        return Err(corrupt("property UUID column contains null slots"));
    }
    Ok(column)
}

fn uuid_reader(
    source: &CountingChunkReader,
    metadata: &ArrowReaderMetadata,
    leaf: usize,
    groups: &[usize],
    selection: Option<RowSelection>,
    batch_rows: usize,
) -> Result<ParquetRecordBatchReader, GfError> {
    let mask = ProjectionMask::leaves(metadata.parquet_schema(), [leaf]);
    let builder =
        ParquetRecordBatchReaderBuilder::new_with_metadata(source.clone(), metadata.clone())
            .with_projection(mask)
            .with_row_groups(groups.to_vec());
    let builder = match selection {
        Some(selection) => builder.with_row_selection(selection),
        None => builder,
    };
    builder
        .with_batch_size(batch_rows)
        .build()
        .map_err(parquet_error)
}

/// Decode only the candidate pages. `Ok(None)` when the page index is absent
/// or any plan or stream check fails, so the caller falls back to a full
/// decode; decode and corruption errors that do not depend on the index
/// propagate.
fn paged_selection(
    source: &CountingChunkReader,
    footer: &ArrowReaderMetadata,
    leaf: usize,
    groups: &[usize],
    wanted: &dyn UuidMembership,
    batch_rows: usize,
    total_rows: usize,
) -> Result<Option<WantedRows>, GfError> {
    let Some(paged) = load_page_index(source, footer) else {
        return Ok(None);
    };
    let Some(plan) = plan_pages(paged.metadata(), leaf, groups, wanted) else {
        return Ok(None);
    };
    let mut selectors = Selectors::default();
    let mut rows_decoded = 0_u64;
    if !plan.candidates.is_empty() {
        let mut candidate_rows = Selectors::default();
        let mut cursor = 0_usize;
        for candidate in &plan.candidates {
            let Some(gap) = candidate.start.checked_sub(cursor) else {
                return Ok(None);
            };
            candidate_rows.push(false, gap);
            candidate_rows.push(true, candidate.rows);
            cursor = candidate.start + candidate.rows;
        }
        let Some(tail) = total_rows.checked_sub(cursor) else {
            return Ok(None);
        };
        candidate_rows.push(false, tail);
        let reader = uuid_reader(
            source,
            &paged,
            leaf,
            groups,
            Some(candidate_rows.finish()),
            batch_rows,
        )?;
        let mut cursor = 0_usize;
        let mut next = 0_usize;
        let mut left = 0_usize;
        let mut prior: Option<[u8; 16]> = None;
        for batch in reader {
            let batch = batch.map_err(authenticated_arrow_error)?;
            let column = batch_uuids(&batch)?;
            for index in 0..column.len() {
                if left == 0 {
                    let Some(page) = plan.candidates.get(next) else {
                        return Ok(None);
                    };
                    let Some(gap) = page.start.checked_sub(cursor) else {
                        return Ok(None);
                    };
                    selectors.push(false, gap);
                    cursor = page.start;
                    left = page.rows;
                    next += 1;
                }
                let page = &plan.candidates[next - 1];
                let mut uuid = [0_u8; 16];
                uuid.copy_from_slice(column.value(index));
                if uuid < page.min || uuid > page.max || prior.is_some_and(|before| before >= uuid)
                {
                    return Ok(None);
                }
                prior = Some(uuid);
                selectors.push(wanted.contains_uuid(&uuid), 1);
                cursor += 1;
                left -= 1;
                rows_decoded += 1;
            }
        }
        if left != 0 || next != plan.candidates.len() {
            return Ok(None);
        }
    }
    let Some(tail) = total_rows.checked_sub(selectors.covered()) else {
        return Ok(None);
    };
    selectors.push(false, tail);
    crate::io_stats::record_property_uuid_selection(rows_decoded);
    crate::io_stats::record_property_uuid_pages(plan.pages, plan.candidates.len() as u64);
    Ok(Some(WantedRows {
        selection: selectors.finish(),
        rows_decoded,
        paged: Some(paged),
    }))
}

/// Decode the whole UUID column of the selected row groups.
fn full_selection(
    source: &CountingChunkReader,
    footer: &ArrowReaderMetadata,
    leaf: usize,
    groups: &[usize],
    wanted: &dyn UuidMembership,
    batch_rows: usize,
) -> Result<WantedRows, GfError> {
    let reader = uuid_reader(source, footer, leaf, groups, None, batch_rows)?;
    let mut selectors = Selectors::default();
    let mut prior: Option<[u8; 16]> = None;
    let mut rows_decoded = 0_u64;
    for batch in reader {
        let batch = batch.map_err(authenticated_arrow_error)?;
        let column = batch_uuids(&batch)?;
        for index in 0..column.len() {
            let mut uuid = [0_u8; 16];
            uuid.copy_from_slice(column.value(index));
            // Rows of different row groups of one fragment stay ordered.
            if prior.is_some_and(|before| before >= uuid) {
                return Err(corrupt("property fragment UUIDs are duplicate or unsorted"));
            }
            prior = Some(uuid);
            rows_decoded += 1;
            selectors.push(wanted.contains_uuid(&uuid), 1);
        }
    }
    crate::io_stats::record_property_uuid_selection(rows_decoded);
    Ok(WantedRows {
        selection: selectors.finish(),
        rows_decoded,
        paged: None,
    })
}

#[cfg(test)]
mod tests;
