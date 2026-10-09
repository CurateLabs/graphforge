//! Shared row progress and byte accounting for Parquet decode windows.
//!
//! The schema model supplies already-checked event byte increments and one
//! combined floor per output window. This module keeps row identity across
//! page boundaries and charges every projected leaf to the same batch totals.

use graphforge_core::GfError;

use crate::CancellationToken;

use super::inventory_budget::{InventoryBudget, reserve};
use super::{cancelled, limit, storage};

const MAX_PROGRESS_EVENTS: usize = 1024;

#[derive(Debug, Default)]
struct WindowTotal {
    bytes: u64,
    floor_applied: bool,
}

/// Task-shared actual byte totals, indexed by global batch number.
///
/// New ledgers always start at zero. Planned sizing values are not accepted as
/// constructor input, so runtime facts cannot accidentally count planned bytes
/// twice. The per-window vector is admitted before allocation.
#[derive(Debug)]
pub(super) struct WindowLedger {
    first_batch: u64,
    totals: Vec<WindowTotal>,
    window_limit: u64,
}

impl WindowLedger {
    /// Create zeroed totals for all global windows touched by the complete
    /// task/source row range. Keep this ledger alive while visiting every row
    /// group so groups on opposite sides of a batch boundary share the entry.
    pub(super) fn new(
        task_start_row: u64,
        task_rows: u64,
        batch_rows: u64,
        window_limit: u64,
        budget: &mut InventoryBudget,
        cancellation: Option<&CancellationToken>,
    ) -> Result<Self, GfError> {
        check_cancel(cancellation)?;
        if batch_rows == 0 {
            return Err(storage("Parquet batch row count must be positive"));
        }
        let task_end = task_start_row
            .checked_add(task_rows)
            .ok_or_else(|| storage("Parquet task row range overflows"))?;
        let first_batch = task_start_row / batch_rows;
        let end_batch = div_ceil(task_end, batch_rows);
        let count = if task_rows == 0 {
            0
        } else {
            usize::try_from(end_batch - first_batch).map_err(storage)?
        };
        let mut totals = Vec::new();
        reserve(
            &mut totals,
            count,
            budget,
            "the Parquet shared window ledger",
        )?;
        let reserved_bytes =
            checked_inventory_bytes(totals.capacity(), std::mem::size_of::<WindowTotal>())?;
        let mut initialized = 0;
        while initialized < count {
            if let Err(error) = check_cancel(cancellation) {
                budget.release(reserved_bytes);
                return Err(error);
            }
            let step = (count - initialized).min(MAX_PROGRESS_EVENTS);
            let Some(next) = initialized.checked_add(step) else {
                budget.release(reserved_bytes);
                return Err(limit("Parquet window initialization count overflows"));
            };
            totals.resize_with(next, WindowTotal::default);
            initialized = next;
        }
        if let Err(error) = check_cancel(cancellation) {
            budget.release(reserved_bytes);
            return Err(error);
        }
        Ok(Self {
            first_batch,
            totals,
            window_limit,
        })
    }

    /// Apply the schema-derived combined floor exactly once to a window.
    pub(super) fn apply_window_floor(
        &mut self,
        batch: u64,
        bytes: u64,
        cancellation: Option<&CancellationToken>,
    ) -> Result<(), GfError> {
        check_cancel(cancellation)?;
        let index = self.index(batch)?;
        let window = &mut self.totals[index];
        if window.floor_applied {
            return Err(storage("Parquet window floor was applied more than once"));
        }
        let next = checked_charge(window.bytes, bytes)?;
        ensure_fits(next, self.window_limit)?;
        window.bytes = next;
        window.floor_applied = true;
        Ok(())
    }

    /// Add one leaf's event bytes to the shared global window before the
    /// corresponding event is committed by its leaf progress cursor.
    fn add_event(&mut self, batch: u64, bytes: u64) -> Result<(), GfError> {
        let index = self.index(batch)?;
        let next = checked_charge(self.totals[index].bytes, bytes)?;
        ensure_fits(next, self.window_limit)?;
        self.totals[index].bytes = next;
        Ok(())
    }

    /// Current bytes charged across all touched windows.
    pub(super) fn current_bytes(&self) -> Result<u64, GfError> {
        self.totals.iter().try_fold(0_u64, |sum, window| {
            sum.checked_add(window.bytes)
                .ok_or_else(|| limit("Parquet window byte total exceeds a countable size"))
        })
    }

    /// Actual charged bytes in a global batch window.
    pub(super) fn batch_bytes(&self, batch: u64) -> Result<u64, GfError> {
        Ok(self.totals[self.index(batch)?].bytes)
    }

    /// Final actual totals, paired with their global batch numbers.
    pub(super) fn batch_totals(&self) -> impl Iterator<Item = (u64, u64)> + '_ {
        let first = self.first_batch;
        self.totals
            .iter()
            .enumerate()
            .map(move |(index, window)| (first + index as u64, window.bytes))
    }

    /// Resident vector bytes charged through the inventory budget.
    pub(super) fn inventory_bytes(&self) -> Result<u64, GfError> {
        checked_inventory_bytes(self.totals.capacity(), std::mem::size_of::<WindowTotal>())
    }

    fn index(&self, batch: u64) -> Result<usize, GfError> {
        let index = batch
            .checked_sub(self.first_batch)
            .and_then(|value| usize::try_from(value).ok())
            .filter(|index| *index < self.totals.len())
            .ok_or_else(|| storage("Parquet event maps outside its task windows"))?;
        Ok(index)
    }
}

/// Scalar progress for one projected Parquet leaf.
///
/// `process_block` accepts level values already validated by `PageEvents` and
/// checked byte increments supplied by the schema shape accountant. A page
/// boundary does not close the current row.
#[derive(Debug)]
pub(super) struct LeafProgress {
    group_start_row: u64,
    group_rows: u64,
    batch_rows: u64,
    expected_events: u64,
    events_seen: u64,
    rows_started: u64,
    row_open: bool,
}

impl LeafProgress {
    pub(super) fn new(
        group_start_row: u64,
        group_rows: u64,
        batch_rows: u64,
        expected_events: u64,
        cancellation: Option<&CancellationToken>,
    ) -> Result<Self, GfError> {
        check_cancel(cancellation)?;
        if batch_rows == 0 {
            return Err(storage("Parquet batch row count must be positive"));
        }
        group_start_row
            .checked_add(group_rows)
            .ok_or_else(|| storage("Parquet row-group range overflows"))?;
        Ok(Self {
            group_start_row,
            group_rows,
            batch_rows,
            expected_events,
            events_seen: 0,
            rows_started: 0,
            row_open: false,
        })
    }

    /// Process at most 1,024 events, returning the number consumed. If the
    /// input is longer, the caller resumes with its unconsumed suffix.
    pub(super) fn process_block(
        &mut self,
        ledger: &mut WindowLedger,
        repetitions: &[i16],
        byte_increments: &[u64],
        cancellation: Option<&CancellationToken>,
    ) -> Result<usize, GfError> {
        check_cancel(cancellation)?;
        if repetitions.len() != byte_increments.len() {
            return Err(storage(
                "Parquet repetition levels and event byte increments have different lengths",
            ));
        }
        let count = repetitions.len().min(MAX_PROGRESS_EVENTS);
        for index in 0..count {
            check_cancel(cancellation)?;
            if self.events_seen >= self.expected_events {
                return Err(storage("Parquet leaf exceeds its footer event count"));
            }

            let (row_index, next_rows_started, next_row_open) = if repetitions[index] == 0 {
                if self.rows_started >= self.group_rows {
                    return Err(storage("Parquet leaf exceeds its footer row count"));
                }
                (
                    self.rows_started,
                    self.rows_started
                        .checked_add(1)
                        .ok_or_else(|| storage("Parquet row count overflows"))?,
                    true,
                )
            } else {
                if !self.row_open {
                    return Err(storage(
                        "Parquet leaf begins with a continuation before any row starts",
                    ));
                }
                (self.rows_started - 1, self.rows_started, true)
            };
            let global_row = self
                .group_start_row
                .checked_add(row_index)
                .ok_or_else(|| storage("Parquet global row index overflows"))?;
            let batch = global_row / self.batch_rows;
            let next_events = self
                .events_seen
                .checked_add(1)
                .ok_or_else(|| storage("Parquet leaf event count overflows"))?;

            // The shared limit is checked before changing this leaf's progress.
            ledger.add_event(batch, byte_increments[index])?;
            self.events_seen = next_events;
            self.rows_started = next_rows_started;
            self.row_open = next_row_open;
        }
        Ok(count)
    }

    /// Verify the complete leaf against footer row and event totals.
    pub(super) fn finish(&self, cancellation: Option<&CancellationToken>) -> Result<(), GfError> {
        check_cancel(cancellation)?;
        if self.events_seen != self.expected_events || self.rows_started != self.group_rows {
            return Err(storage(
                "Parquet leaf progress disagrees with footer row or event totals",
            ));
        }
        Ok(())
    }

    pub(super) fn events_seen(&self) -> u64 {
        self.events_seen
    }

    pub(super) fn rows_started(&self) -> u64 {
        self.rows_started
    }
}

/// Budgeted inventory for per-leaf scalar cursors when a task has many leaves.
#[derive(Debug)]
pub(super) struct LeafProgressInventory {
    leaves: Vec<LeafProgress>,
}

impl LeafProgressInventory {
    pub(super) fn new() -> Self {
        Self { leaves: Vec::new() }
    }

    pub(super) fn push(
        &mut self,
        progress: LeafProgress,
        budget: &mut InventoryBudget,
    ) -> Result<(), GfError> {
        reserve(
            &mut self.leaves,
            1,
            budget,
            "the Parquet leaf progress inventory",
        )?;
        self.leaves.push(progress);
        Ok(())
    }

    pub(super) fn get_mut(&mut self, index: usize) -> Option<&mut LeafProgress> {
        self.leaves.get_mut(index)
    }

    pub(super) fn len(&self) -> usize {
        self.leaves.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.leaves.is_empty()
    }

    pub(super) fn inventory_bytes(&self) -> Result<u64, GfError> {
        checked_inventory_bytes(self.leaves.capacity(), std::mem::size_of::<LeafProgress>())
    }
}

fn check_cancel(cancellation: Option<&CancellationToken>) -> Result<(), GfError> {
    if cancellation.is_some_and(CancellationToken::is_cancelled) {
        return Err(cancelled());
    }
    Ok(())
}

fn checked_charge(current: u64, increment: u64) -> Result<u64, GfError> {
    current
        .checked_add(increment)
        .ok_or_else(|| limit("Parquet window byte total exceeds a countable size"))
}

fn ensure_fits(bytes: u64, limit_bytes: u64) -> Result<(), GfError> {
    if bytes > limit_bytes {
        return Err(limit(format!(
            "Parquet batch window requires {bytes} bytes, exceeding its {limit_bytes}-byte limit"
        )));
    }
    Ok(())
}

fn div_ceil(value: u64, divisor: u64) -> u64 {
    value / divisor + u64::from(value % divisor != 0)
}

fn checked_inventory_bytes(capacity: usize, element_bytes: usize) -> Result<u64, GfError> {
    let capacity = u64::try_from(capacity)
        .map_err(|_| limit("Parquet inventory capacity exceeds a countable size"))?;
    let element_bytes = u64::try_from(element_bytes)
        .map_err(|_| limit("Parquet inventory element size exceeds a countable size"))?;
    capacity
        .checked_mul(element_bytes)
        .ok_or_else(|| limit("Parquet inventory byte size exceeds a countable size"))
}

#[path = "parquet_windows/tests.rs"]
#[cfg(test)]
mod tests;
