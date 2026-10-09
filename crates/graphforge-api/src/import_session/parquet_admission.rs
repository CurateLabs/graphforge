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
//! bounded owned-page cursors, which materialize no Arrow array), so the reader can
//! refuse a batch before its allocation and reserve exactly what it will hold.
//!
//! The sizes are Arrow value bytes (values, offsets and validity), the number
//! the builder's per-batch window is stated in.

use graphforge_core::GfError;
use parquet::file::metadata::ParquetMetaData;
use std::fs::File;

use super::inventory_budget::{InventoryBudget, reserve};
use super::parquet_scan::{GroupScan, Leaf, LeafScan, PageKind, scan_group};
use super::storage;
use crate::CancellationToken;

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
            reserve(
                &mut groups,
                1,
                &mut budget,
                "the row-group page inventories",
            )?;
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
        reserve(
            &mut value_bytes,
            batches,
            &mut budget,
            "the per-batch sizes",
        )?;
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
            &mut budget,
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
            scan.size_values(file, metadata, &mut budget, capacity, cancellation)?;
        }
        // The budget charged actual capacities, including unused geometric
        // slots in groups/leaves. A sum of lengths would understate retained
        // inventory after a three-element vector grows to four slots.
        scan.resident_bytes = budget.live_bytes();
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
        budget: &mut InventoryBudget,
        capacity: u64,
        cancellation: Option<&CancellationToken>,
    ) -> Result<(), GfError> {
        for column in 0..metadata.schema_descr().num_columns() {
            for (index, group) in self.groups.iter().enumerate() {
                check(cancellation)?;
                let leaf = group
                    .leaves
                    .get(column)
                    .ok_or_else(|| storage("Parquet row groups disagree on physical columns"))?;
                if !needs_values(leaf) {
                    continue;
                }
                let rows = u64::try_from(metadata.row_group(index).num_rows()).unwrap_or(0);
                let first_row = self.group_start[index];
                let descriptor = metadata.row_group(index).column(column).column_descr();
                let flat = descriptor.max_rep_level() == 0;
                let mut add = |row: u64, bytes: u64| {
                    let global_row = first_row.checked_add(row).ok_or_else(|| {
                        storage("Parquet source row index overflows while sizing values")
                    })?;
                    let batch = global_row / self.batch_rows;
                    let slot = self
                        .value_bytes
                        .get_mut(usize::try_from(batch).unwrap_or(usize::MAX))
                        .ok_or_else(|| {
                            storage("Parquet sized row is outside the batch inventory")
                        })?;
                    let bytes = if flat {
                        bytes
                            .checked_sub(1)
                            .ok_or_else(|| storage("flat row size omitted its validity bit"))?
                    } else {
                        bytes
                    };
                    *slot = slot
                        .checked_add(bytes)
                        .ok_or_else(|| storage("Parquet per-batch value size overflows"))?;
                    Ok(())
                };
                super::parquet_sizing::size_column(
                    &file,
                    metadata.row_group(index).column(column),
                    rows,
                    0,
                    capacity,
                    budget,
                    cancellation,
                    &mut add,
                )?;
                drop(add);
                if flat && rows > 0 {
                    // Preserve the existing flat validity size while packing
                    // row-group segments against their global batch bit offset.
                    let end_row = first_row
                        .checked_add(rows)
                        .ok_or_else(|| storage("Parquet source row range overflows"))?;
                    let mut batch = first_row / self.batch_rows;
                    while batch
                        .checked_mul(self.batch_rows)
                        .is_some_and(|start| start < end_row)
                    {
                        let batch_start = batch
                            .checked_mul(self.batch_rows)
                            .ok_or_else(|| storage("Parquet batch row range overflows"))?;
                        let batch_end = batch_start
                            .checked_add(self.batch_rows)
                            .ok_or_else(|| storage("Parquet batch row range overflows"))?;
                        let lo = first_row.max(batch_start);
                        let hi = end_row.min(batch_end);
                        let before = lo
                            .checked_sub(batch_start)
                            .ok_or_else(|| storage("Parquet batch start is out of range"))?;
                        let after = hi
                            .checked_sub(batch_start)
                            .ok_or_else(|| storage("Parquet batch end is out of range"))?;
                        let bitmap_before = before.div_ceil(8);
                        let bitmap_after = after.div_ceil(8);
                        let bitmap_delta = bitmap_after
                            .checked_sub(bitmap_before)
                            .ok_or_else(|| storage("Parquet validity range is out of order"))?;
                        let slot = self
                            .value_bytes
                            .get_mut(usize::try_from(batch).unwrap_or(usize::MAX))
                            .ok_or_else(|| {
                                storage("Parquet batch is outside the size inventory")
                            })?;
                        *slot = slot
                            .checked_add(bitmap_delta)
                            .ok_or_else(|| storage("Parquet validity size overflows"))?;
                        batch = batch
                            .checked_add(1)
                            .ok_or_else(|| storage("Parquet batch index overflows"))?;
                    }
                }
            }
        }
        Ok(())
    }
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

#[cfg(test)]
mod inventory_tests;
#[cfg(test)]
mod tests;
