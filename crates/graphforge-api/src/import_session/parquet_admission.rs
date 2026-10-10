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

use arrow::datatypes::DataType;
use graphforge_core::GfError;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::file::metadata::ParquetMetaData;
use std::fs::File;

use super::inventory_budget::{InventoryBudget, reserve};
use super::parquet_alloc::mutable_envelope;
use super::parquet_scan::{GroupScan, Leaf, LeafScan, PageKind, scan_group};
use super::parquet_shape::SchemaShape;
use super::{limit, storage};
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
/// How far a decoded byte-array column's buffers can outgrow its values. The
/// native reader appends each value (each child of a nested value) to a
/// vector that grows by doubling, after reserving at most the page's average
/// value per row it reads, which is no more than the piece's widest rows; so
/// a piece's value and offset buffers hold under twice their byte-array bound
/// in capacity, one-row pieces included, and the builder charges a batch what
/// its buffers hold. Fixed-width columns are read into buffers reserved for
/// the piece's rows, exactly.
const VARIABLE_BUFFER_GROWTH: u64 = 2;

/// Everything known about how a source decodes, from its page headers and the
/// values the headers cannot size.
pub(super) struct SourceScan {
    /// Arrow-visible schema and its mapping to original physical leaf ordinals.
    shape: SchemaShape,
    groups: Vec<GroupScan>,
    /// First row of each row group.
    group_start: Vec<u64>,
    batch_rows: u64,
    rows: u64,
    /// Exact Arrow bytes per logical batch of the columns `needs_values`.
    value_bytes: Vec<u64>,
    /// Largest exact per-row Arrow contribution across all visible leaves.
    /// Only this scalar is retained; row costs are scanned through one bounded
    /// logical-batch window at a time.
    max_row_value_bytes: u64,
    /// The widest row's bytes in byte-array leaves alone: the share of a piece
    /// whose buffers grow past their payload.
    max_row_variable_bytes: u64,
    /// Exact max row contribution by logical batch. Used when an individual
    /// source row exceeds the window and pieces must be one row each.
    batch_max_row_value_bytes: Vec<u64>,
    /// First global row in each logical batch whose one-row request exceeds
    /// the predecode admission threshold; `u64::MAX` means none.
    first_oversized_row: Vec<u64>,
    flat_leaf_count: u64,
    offset_boundary_bytes: u64,
    /// Arrow's MutableBuffer allocation floor for the visible output arrays.
    /// Wide schemas with tiny batches can be dominated by one 64-byte buffer
    /// per values/offsets/validity buffer, even when their payload estimate is
    /// only a few bytes.
    arrow_buffer_floor_bytes: u64,
    physical_batch_rows: u64,
    /// Bytes of this inventory, resident for the life of the plan.
    resident_bytes: u64,
}

impl SourceScan {
    /// Scan the page headers of every row group and size the columns whose
    /// expansion only their values state.
    ///
    /// `capacity` bounds what the sizing reads may hold; a page larger than it
    /// is refused here, before any reader would allocate it. It also bounds the
    /// inventory the scan itself keeps — the Arrow schema shape, page facts,
    /// visible leaf scans, row-group starts, and per-batch sizes — charged as it
    /// is built, so an inventory the workspace cannot hold is refused before it
    /// is allocated. `window` is the
    /// size past which a batch is refused: a page-bounded size that exceeds it
    /// is replaced by the exact one, so a coarse bound never refuses a batch
    /// that fits.
    // One pass constructs the bounded facts from the same footer and schema;
    // splitting it would duplicate ordering state across admission phases.
    #[allow(clippy::too_many_lines)]
    pub(super) fn build(
        file: &File,
        metadata: &ArrowReaderMetadata,
        batch_rows: u64,
        capacity: u64,
        window: u64,
        cancellation: Option<&CancellationToken>,
    ) -> Result<Self, GfError> {
        let mut scanner = file.try_clone().map_err(storage)?;
        let mut budget = InventoryBudget::new(capacity);
        let shape = SchemaShape::build(metadata, &mut budget, cancellation)?;
        let offset_boundary_bytes = shape.nodes.iter().fold(0_u64, |bytes, node| {
            let width = match node.field.data_type() {
                DataType::Utf8
                | DataType::Binary
                | DataType::List(_)
                | DataType::Map(_, _)
                | DataType::ListView(_) => 4,
                DataType::LargeUtf8
                | DataType::LargeBinary
                | DataType::LargeList(_)
                | DataType::LargeListView(_) => 8,
                _ => 0,
            };
            bytes.saturating_add(width)
        });
        let mutable_floor = mutable_envelope(0, 1)?.peak_bytes;
        let arrow_buffer_floor_bytes = shape.nodes.iter().try_fold(0_u64, |bytes, node| {
            bytes
                .checked_add(
                    mutable_floor
                        .checked_mul(output_buffer_count(
                            node.kind,
                            node.field.data_type(),
                            node.nullable,
                        ))
                        .ok_or_else(|| limit("Parquet Arrow buffer floor overflows"))?,
                )
                .ok_or_else(|| limit("Parquet Arrow buffer floor overflows"))
        })?;
        let metadata = metadata.metadata();
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
            let group = scan_group(&mut scanner, metadata, index, &shape.leaves, &mut budget)?;
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
        let mut batch_max_row_value_bytes = Vec::new();
        reserve(
            &mut batch_max_row_value_bytes,
            batches,
            &mut budget,
            "the per-batch maximum row sizes",
        )?;
        batch_max_row_value_bytes.resize(batches, 0);
        let mut first_oversized_row = Vec::new();
        reserve(
            &mut first_oversized_row,
            batches,
            &mut budget,
            "the per-batch oversized row markers",
        )?;
        first_oversized_row.resize(batches, u64::MAX);
        let mut scan = Self {
            shape,
            groups,
            group_start,
            batch_rows: batch_rows.max(1),
            rows: start,
            value_bytes,
            max_row_value_bytes: 0,
            max_row_variable_bytes: 0,
            batch_max_row_value_bytes,
            first_oversized_row,
            flat_leaf_count: 0,
            offset_boundary_bytes,
            arrow_buffer_floor_bytes,
            physical_batch_rows: batch_rows.max(1),
            resident_bytes: 0,
        };
        scan.size_values(file, metadata, &mut budget, capacity, cancellation)?;
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
        scan.measure_max_row_cost(file, metadata, &mut budget, capacity, window, cancellation)?;
        scan.physical_batch_rows =
            choose_physical_rows(scan.batch_rows, window, |rows| scan.piece_bound(rows));
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

    /// Scratch floor for a sizing pass over one leaf while retaining one
    /// bounded row-cost vector. Reserve this before choosing the vector size so
    /// its allocation cannot starve the page reader and dictionary validator.
    fn sizing_floor(&self) -> u64 {
        self.groups
            .iter()
            .flat_map(|group| group.leaves.iter())
            .map(|leaf| leaf.summary.sizing_floor())
            .max()
            .unwrap_or(super::parquet_sizing::WORKSPACE_RESERVE)
    }

    /// The most a task's decoder holds beside its batches: the largest of the
    /// row groups it reads.
    pub(super) fn pages_resident(&self, first_row: u64, last_row: u64) -> u64 {
        self.overlapping(first_row, last_row)
            .map(|(_, _, _, group)| group.pages_resident())
            .max()
            .unwrap_or(0)
    }

    /// Maximum native decoder scratch in any selected row group. These are
    /// retained alongside each group's owned pages, unlike validator vectors
    /// which are included in the separate page-credit envelope.
    pub(super) fn native_decoder_auxiliary(&self, first_row: u64, last_row: u64) -> u64 {
        let group_peak = self
            .overlapping(first_row, last_row)
            .map(|(_, _, _, group)| group.native_decoder_auxiliary())
            .max()
            .unwrap_or(0);
        // DELTA_BYTE_ARRAY reconstruction may keep both the previous and the
        // current full value live. The exact largest reconstructed row is
        // established by the bounded values pass; use its source-wide maximum
        // because the decoder may materialize values from a touched page while
        // applying row selection.
        let has_delta = self
            .overlapping(first_row, last_row)
            .any(|(_, _, _, group)| {
                group
                    .leaves
                    .iter()
                    .any(|leaf| leaf.summary.delta_byte_array)
            });
        group_peak.saturating_add(if has_delta {
            self.max_row_value_bytes.saturating_mul(2)
        } else {
            0
        })
    }

    /// Validator dictionary vectors and bounded level/value cursor blocks for
    /// one selected row group. The current row-group validators retain their
    /// dictionaries together, so sum those actual header entry counts.
    pub(super) fn validator_workspace(&self, first_row: u64, last_row: u64) -> u64 {
        self.overlapping(first_row, last_row)
            .map(|(_, _, _, group)| {
                group
                    .validator_dictionary_bytes()
                    .saturating_add(super::parquet_sizing::WORKSPACE_RESERVE)
            })
            .max()
            .unwrap_or(super::parquet_sizing::WORKSPACE_RESERVE)
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

    /// Conservative Arrow output bound for one physical decoder piece.
    pub(super) fn physical_batch_bytes(&self, batch: u64) -> u64 {
        if self.physical_batch_rows == 1 {
            let row = batch;
            let logical = row / self.batch_rows;
            // The batch's widest row, the growth of its byte-array buffers
            // included.
            let max_row = self
                .batch_max_row_value_bytes
                .get(usize::try_from(logical).unwrap_or(usize::MAX))
                .copied()
                .unwrap_or_else(|| {
                    self.max_row_value_bytes.saturating_add(
                        self.max_row_variable_bytes
                            .saturating_mul(VARIABLE_BUFFER_GROWTH - 1),
                    )
                });
            return max_row
                .saturating_add(self.flat_leaf_count)
                .saturating_add(self.offset_boundary_bytes)
                .saturating_add(self.arrow_buffer_floor_bytes);
        }
        let first = batch.saturating_mul(self.physical_batch_rows);
        let count = self
            .physical_batch_rows
            .min(self.rows.saturating_sub(first));
        self.piece_bound(count)
    }

    /// Conservative bound on what a piece of `rows` rows occupies once decoded:
    /// the widest row's bytes per row, the growth of its byte-array buffers, a
    /// validity bit per flat leaf and row, and the fixed offsets and buffer
    /// floors.
    fn piece_bound(&self, rows: u64) -> u64 {
        let growth = rows
            .saturating_mul(self.max_row_variable_bytes)
            .saturating_mul(VARIABLE_BUFFER_GROWTH - 1);
        rows.saturating_mul(self.max_row_value_bytes)
            .saturating_add(growth)
            .saturating_add(self.flat_leaf_count.saturating_mul(rows.div_ceil(8)))
            .saturating_add(self.offset_boundary_bytes)
            .saturating_add(self.arrow_buffer_floor_bytes)
    }

    /// The widest bound over a task's pieces. Pieces that straddle different
    /// pages differ, so the task's admission walks all of them.
    pub(super) fn physical_batch_max_bytes(&self, first: u64, count: u64) -> u64 {
        if self.physical_batch_rows == 1 {
            return (first..first.saturating_add(count))
                .map(|row| self.physical_batch_bytes(row))
                .max()
                .unwrap_or(0);
        }
        (first..first.saturating_add(count))
            .map(|batch| self.physical_batch_bytes(batch))
            .max()
            .unwrap_or(0)
    }

    pub(super) fn physical_batch_exceeds(&self, batch: u64, limit: u64) -> bool {
        if self.physical_batch_rows != 1 {
            return self.physical_batch_bytes(batch) > limit;
        }
        let logical = batch / self.batch_rows;
        self.first_oversized_row
            .get(usize::try_from(logical).unwrap_or(usize::MAX))
            .is_some_and(|row| *row == batch)
    }

    pub(super) fn physical_batch_rows(&self) -> u64 {
        self.physical_batch_rows
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

    /// Read every visible column whose batch size the headers cannot state, one
    /// row group at a time, and add its exact Arrow bytes to each batch it
    /// touches.
    fn size_values(
        &mut self,
        file: &File,
        metadata: &ParquetMetaData,
        budget: &mut InventoryBudget,
        capacity: u64,
        cancellation: Option<&CancellationToken>,
    ) -> Result<(), GfError> {
        for (visible_index, shape_leaf) in self.shape.leaves.iter().enumerate() {
            let column = shape_leaf.column_index;
            for (index, group) in self.groups.iter().enumerate() {
                check(cancellation)?;
                let leaf = group
                    .leaves
                    .get(visible_index)
                    .ok_or_else(|| storage("Parquet row groups disagree on physical columns"))?;
                if leaf.physical_column_index != column {
                    return Err(storage("Parquet row-group visible column order differs"));
                }
                if !needs_values(leaf) {
                    continue;
                }
                let rows = u64::try_from(metadata.row_group(index).num_rows()).unwrap_or(0);
                let first_row = self.group_start[index];
                let descriptor = metadata.row_group(index).column(column).column_descr();
                let flat = descriptor.max_rep_level() == 0;
                {
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
                        super::parquet_sizing::SizeColumnInput {
                            file,
                            column: metadata.row_group(index).column(column),
                            rows,
                            row_base: 0,
                            capacity,
                            budget,
                            cancellation,
                        },
                        &mut add,
                    )?;
                }
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

    /// Measure all projected columns together, keeping only one logical
    /// batch's row costs live. This yields an exact maximum row contribution
    /// for a conservative physical chunk size without a source-wide row map.
    // The scan repeatedly reduces its per-window scratch before admitting it;
    // keep that retry/charge sequence together to preserve the budget proof.
    #[allow(clippy::too_many_lines)]
    fn measure_max_row_cost(
        &mut self,
        file: &File,
        metadata: &ParquetMetaData,
        budget: &mut InventoryBudget,
        capacity: u64,
        single_row_limit: u64,
        cancellation: Option<&CancellationToken>,
    ) -> Result<(), GfError> {
        let logical_batches = self.rows.div_ceil(self.batch_rows);
        let sizing_floor = self.sizing_floor();
        // The leaf's descriptor comes from the schema, which a file of no row
        // groups still carries.
        let schema = metadata.file_metadata().schema_descr();
        for shape_leaf in &self.shape.leaves {
            let descriptor = schema
                .columns()
                .get(shape_leaf.column_index)
                .ok_or_else(|| storage("Parquet schema has no visible physical column"))?;
            if descriptor.max_rep_level() == 0 {
                self.flat_leaf_count = self
                    .flat_leaf_count
                    .checked_add(1)
                    .ok_or_else(|| storage("Parquet flat leaf count overflows"))?;
            }
        }
        for batch in 0..logical_batches {
            check(cancellation)?;
            let batch_first = batch * self.batch_rows;
            let batch_end = (batch_first + self.batch_rows).min(self.rows);
            let mut first = batch_first;
            while first < batch_end {
                let mut count = usize::try_from(batch_end - first)
                    .map_err(storage)?
                    .min(usize::try_from(self.batch_rows).unwrap_or(usize::MAX));
                loop {
                    let rounded = count
                        .checked_next_power_of_two()
                        .ok_or_else(|| limit("Parquet row sizing window overflows"))?;
                    // Two vectors: every leaf's bytes per row, and the byte-array
                    // leaves' bytes per row.
                    let request = u64::try_from(rounded)
                        .map_err(storage)?
                        .checked_mul(
                            2 * u64::try_from(std::mem::size_of::<u64>()).map_err(storage)?,
                        )
                        .ok_or_else(|| limit("Parquet row sizing window overflows"))?;
                    let available = budget.remaining().saturating_sub(sizing_floor);
                    if request <= available {
                        break;
                    }
                    if count == 1 {
                        return Err(limit(
                            "Parquet row sizing and its required page workspace do not fit the inventory budget",
                        ));
                    }
                    count = (count / 2).max(1);
                }
                let end = first
                    .checked_add(u64::try_from(count).map_err(storage)?)
                    .ok_or_else(|| storage("Parquet row sizing range overflows"))?
                    .min(batch_end);
                count = usize::try_from(end - first).map_err(storage)?;
                let mut row_costs = Vec::new();
                reserve(
                    &mut row_costs,
                    count,
                    budget,
                    "the bounded per-row Parquet sizing window",
                )?;
                row_costs.resize(count, 0_u64);
                let mut row_variable = Vec::new();
                reserve(
                    &mut row_variable,
                    count,
                    budget,
                    "the bounded per-row Parquet sizing window",
                )?;
                row_variable.resize(count, 0_u64);
                for (visible_index, shape_leaf) in self.shape.leaves.iter().enumerate() {
                    let column = shape_leaf.column_index;
                    let descriptor = schema
                        .columns()
                        .get(column)
                        .ok_or_else(|| storage("Parquet schema has no visible physical column"))?;
                    let flat = descriptor.max_rep_level() == 0;
                    for (group_index, group) in self.groups.iter().enumerate() {
                        let group_first = self.group_start[group_index];
                        let group_rows =
                            u64::try_from(metadata.row_group(group_index).num_rows()).unwrap_or(0);
                        let group_end = group_first.saturating_add(group_rows);
                        if group_end <= first || group_first >= end {
                            continue;
                        }
                        check(cancellation)?;
                        let leaf = group.leaves.get(visible_index).ok_or_else(|| {
                            storage("Parquet row groups disagree on physical columns")
                        })?;
                        if leaf.physical_column_index != column {
                            return Err(storage("Parquet row-group visible column order differs"));
                        }
                        super::parquet_sizing::size_column(
                            super::parquet_sizing::SizeColumnInput {
                                file,
                                column: metadata.row_group(group_index).column(column),
                                rows: group_rows,
                                row_base: 0,
                                capacity,
                                budget,
                                cancellation,
                            },
                            &mut |row, bytes| {
                                let global_row = group_first.checked_add(row).ok_or_else(|| {
                                    storage("Parquet source row index overflows while sizing")
                                })?;
                                if global_row < first || global_row >= end {
                                    return Ok(());
                                }
                                let local = usize::try_from(global_row - first).map_err(storage)?;
                                let slot = row_costs.get_mut(local).ok_or_else(|| {
                                    storage("Parquet row cost maps outside its bounded window")
                                })?;
                                let bytes = if flat {
                                    bytes.checked_sub(1).ok_or_else(|| {
                                        storage("flat row size omitted its validity bit")
                                    })?
                                } else {
                                    bytes
                                };
                                *slot = slot.checked_add(bytes).ok_or_else(|| {
                                    storage("Parquet per-row output size overflows")
                                })?;
                                if leaf.leaf == Leaf::Variable {
                                    let slot = row_variable.get_mut(local).ok_or_else(|| {
                                        storage("Parquet row cost maps outside its bounded window")
                                    })?;
                                    *slot = slot.checked_add(bytes).ok_or_else(|| {
                                        storage("Parquet per-row output size overflows")
                                    })?;
                                }
                                Ok(())
                            },
                        )?;
                    }
                }
                self.max_row_value_bytes = self
                    .max_row_value_bytes
                    .max(row_costs.iter().copied().max().unwrap_or(0));
                self.max_row_variable_bytes = self
                    .max_row_variable_bytes
                    .max(row_variable.iter().copied().max().unwrap_or(0));
                let logical = usize::try_from(batch).map_err(storage)?;
                let flat_bytes = self.flat_leaf_count;
                let one_row_boundary = self.offset_boundary_bytes;
                // A row's bytes once decoded, the growth of its byte-array
                // buffers included: what a piece of that row alone holds.
                let grown = |row: usize| {
                    row_costs[row].saturating_add(
                        row_variable[row].saturating_mul(VARIABLE_BUFFER_GROWTH - 1),
                    )
                };
                let maximum = (0..row_costs.len()).map(grown).max().unwrap_or(0);
                let batch_max = self
                    .batch_max_row_value_bytes
                    .get_mut(logical)
                    .ok_or_else(|| storage("Parquet logical batch exceeds row maxima"))?;
                *batch_max = (*batch_max).max(maximum);
                for row in 0..row_costs.len() {
                    let requested = grown(row)
                        .saturating_add(flat_bytes)
                        .saturating_add(one_row_boundary)
                        .saturating_add(self.arrow_buffer_floor_bytes);
                    if requested > single_row_limit {
                        let global = first
                            .checked_add(u64::try_from(row).map_err(storage)?)
                            .ok_or_else(|| storage("Parquet oversized row index overflows"))?;
                        let marker = self
                            .first_oversized_row
                            .get_mut(logical)
                            .ok_or_else(|| storage("Parquet batch exceeds refusal inventory"))?;
                        if *marker == u64::MAX {
                            *marker = global;
                        }
                        break;
                    }
                }
                let charged = u64::try_from(row_costs.capacity())
                    .unwrap_or(u64::MAX)
                    .saturating_add(u64::try_from(row_variable.capacity()).unwrap_or(u64::MAX))
                    .saturating_mul(u64::try_from(std::mem::size_of::<u64>()).unwrap_or(u64::MAX));
                drop(row_costs);
                drop(row_variable);
                budget.release(charged);
                first = end;
            }
        }
        Ok(())
    }
}

/// The most rows per piece, dividing `logical_rows`, whose `bound` fits the
/// window; one row when none does.
fn choose_physical_rows(logical_rows: u64, window: u64, bound: impl Fn(u64) -> u64) -> u64 {
    let fits = |rows: u64| bound(rows) <= window;
    let logical_rows = logical_rows.max(1);
    let mut low = 1_u64;
    let mut high = logical_rows;
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        if fits(middle) {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    let cap = low;
    let mut largest = 1_u64;
    let mut divisor = 1_u64;
    while divisor <= logical_rows / divisor {
        if logical_rows.is_multiple_of(divisor) {
            let paired = logical_rows / divisor;
            if divisor <= cap {
                largest = largest.max(divisor);
            }
            if paired <= cap {
                largest = largest.max(paired);
            }
        }
        divisor += 1;
    }
    largest
}

/// Mutable Arrow output buffers created for one visible schema node. Child
/// nodes are counted independently, so a nested field's children are not
/// hidden in this node's estimate.
fn output_buffer_count(
    kind: super::parquet_shape::NodeKind,
    data_type: &DataType,
    nullable: bool,
) -> u64 {
    use super::parquet_shape::NodeKind;

    let validity = u64::from(nullable);
    match kind {
        NodeKind::Primitive => {
            let offsets_or_views = matches!(
                data_type,
                DataType::Utf8
                    | DataType::Binary
                    | DataType::LargeUtf8
                    | DataType::LargeBinary
                    | DataType::Utf8View
                    | DataType::BinaryView
            );
            if matches!(data_type, DataType::Null) {
                0
            } else if matches!(data_type, DataType::Utf8View | DataType::BinaryView) {
                // View descriptors and their optional backing data buffers.
                2 + validity
            } else {
                1 + u64::from(offsets_or_views) + validity
            }
        }
        NodeKind::List | NodeKind::LargeList | NodeKind::Map => 1 + validity,
        NodeKind::Struct | NodeKind::FixedSizeList => validity,
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
