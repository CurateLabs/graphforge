//! Public Arrow `RowGroups` backed by the owned, preflighting page reader.

use std::io::Read;
use std::sync::Arc;

use graphforge_core::GfError;
use parquet::arrow::array_reader::RowGroups;
use parquet::column::page::{PageIterator, PageReader};
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use parquet::file::reader::ChunkReader;

use crate::CancellationToken;

use super::inventory_budget::{self, InventoryBudget};
use super::parquet_reader::{OwnedPageReader, PageFailures, PagePreflight};
use super::{cancelled, limit, storage};

const ROW_GROUP_INIT_BLOCK: usize = 1024;

/// Runtime row groups whose every column page passes the same owned-byte
/// preflight before Arrow can decode it.
pub(super) struct OwnedRowGroups<T, F, C> {
    input: Arc<T>,
    metadata: Arc<ParquetMetaData>,
    selected: Arc<Vec<usize>>,
    num_rows: usize,
    factory: Arc<F>,
    cancellation: Option<CancellationToken>,
    failures: PageFailures,
    _callback: std::marker::PhantomData<fn() -> C>,
}

impl<T, F, C> OwnedRowGroups<T, F, C>
where
    T: ChunkReader + 'static,
    T::T: Read + Send + 'static,
    F: Fn(usize, usize) -> Result<C, GfError> + Send + Sync + 'static,
    C: PagePreflight + 'static,
{
    /// Build a selection, charging its retained row-group indices before the
    /// vector allocation. Selection must be strictly increasing so row order
    /// and checked aggregate row counts have one unambiguous authority.
    pub(super) fn new(
        input: Arc<T>,
        metadata: Arc<ParquetMetaData>,
        selected_row_groups: &[usize],
        budget: &mut InventoryBudget,
        factory: F,
        cancellation: Option<CancellationToken>,
        failures: PageFailures,
    ) -> Result<Self, GfError> {
        check_cancelled(cancellation.as_ref())?;

        let file_len = input.len();
        check_cancelled(cancellation.as_ref())?;
        let mut num_rows = 0_usize;
        let mut previous = None;
        for &group_index in selected_row_groups {
            check_cancelled(cancellation.as_ref())?;
            if group_index >= metadata.num_row_groups()
                || previous.is_some_and(|previous| previous >= group_index)
            {
                return Err(storage(
                    "selected Parquet row groups must be valid, unique, and ascending",
                ));
            }
            previous = Some(group_index);
            let group = metadata.row_group(group_index);
            let rows = usize::try_from(group.num_rows())
                .map_err(|_| storage("Parquet row-group row count is negative or too large"))?;
            num_rows = num_rows
                .checked_add(rows)
                .ok_or_else(|| limit("selected Parquet row count exceeds a countable size"))?;
            validate_group_columns(group, file_len, cancellation.as_ref())?;
        }
        check_cancelled(cancellation.as_ref())?;

        let mut selected = Vec::new();
        let charged_before = budget.live_bytes();
        if let Err(error) = inventory_budget::reserve(
            &mut selected,
            selected_row_groups.len(),
            budget,
            "selected row-group indices",
        ) {
            drop(selected);
            release_selection_charge(budget, charged_before)?;
            return Err(error);
        }
        let mut copied = 0_usize;
        while copied < selected_row_groups.len() {
            if let Err(error) = check_cancelled(cancellation.as_ref()) {
                drop(selected);
                release_selection_charge(budget, charged_before)?;
                return Err(error);
            }
            let count = (selected_row_groups.len() - copied).min(ROW_GROUP_INIT_BLOCK);
            let Some(end) = copied.checked_add(count) else {
                drop(selected);
                release_selection_charge(budget, charged_before)?;
                return Err(limit("selected Parquet row-group count overflows"));
            };
            selected.extend_from_slice(&selected_row_groups[copied..end]);
            copied = end;
        }
        if let Err(error) = check_cancelled(cancellation.as_ref()) {
            drop(selected);
            release_selection_charge(budget, charged_before)?;
            return Err(error);
        }

        Ok(Self {
            input,
            metadata,
            selected: Arc::new(selected),
            num_rows,
            factory: Arc::new(factory),
            cancellation,
            failures,
            _callback: std::marker::PhantomData,
        })
    }

    fn check_active(&self) -> ParquetResult<()> {
        if self.failures.failed() {
            return Err(as_parquet_error(storage("Parquet task has already failed")));
        }
        if let Err(error) = check_cancelled(self.cancellation.as_ref()) {
            self.failures.record(error.clone());
            return Err(as_parquet_error(error));
        }
        Ok(())
    }
}

impl<T, F, C> RowGroups for OwnedRowGroups<T, F, C>
where
    T: ChunkReader + 'static,
    T::T: Read + Send + 'static,
    F: Fn(usize, usize) -> Result<C, GfError> + Send + Sync + 'static,
    C: PagePreflight + 'static,
{
    fn num_rows(&self) -> usize {
        self.num_rows
    }

    fn column_chunks(&self, column_index: usize) -> ParquetResult<Box<dyn PageIterator>> {
        self.check_active()?;
        for &group_index in self.selected.iter() {
            self.check_active()?;
            if column_index >= self.metadata.row_group(group_index).num_columns() {
                let error =
                    storage("Parquet physical column index is outside the selected row group");
                self.failures.record(error.clone());
                return Err(as_parquet_error(error));
            }
        }
        self.check_active()?;
        Ok(Box::new(OwnedColumnPageIterator {
            input: Arc::clone(&self.input),
            metadata: Arc::clone(&self.metadata),
            selected: Arc::clone(&self.selected),
            factory: Arc::clone(&self.factory),
            cancellation: self.cancellation.clone(),
            failures: self.failures.clone(),
            column_index,
            next_group: 0,
            terminal: false,
            _callback: std::marker::PhantomData,
        }))
    }

    fn row_groups(&self) -> Box<dyn Iterator<Item = &RowGroupMetaData> + '_> {
        Box::new(
            self.selected
                .iter()
                .map(|&index| self.metadata.row_group(index)),
        )
    }

    fn metadata(&self) -> &ParquetMetaData {
        &self.metadata
    }
}

struct OwnedColumnPageIterator<T, F, C> {
    input: Arc<T>,
    metadata: Arc<ParquetMetaData>,
    selected: Arc<Vec<usize>>,
    factory: Arc<F>,
    cancellation: Option<CancellationToken>,
    failures: PageFailures,
    column_index: usize,
    next_group: usize,
    terminal: bool,
    _callback: std::marker::PhantomData<fn() -> C>,
}

impl<T, F, C> Iterator for OwnedColumnPageIterator<T, F, C>
where
    T: ChunkReader + 'static,
    T::T: Read + Send + 'static,
    F: Fn(usize, usize) -> Result<C, GfError> + Send + Sync + 'static,
    C: PagePreflight + 'static,
{
    type Item = ParquetResult<Box<dyn PageReader>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.terminal || self.failures.failed() {
            return None;
        }
        if let Err(error) = check_cancelled(self.cancellation.as_ref()) {
            return Some(self.fail(error));
        }
        let group_index = *self.selected.get(self.next_group)?;
        self.next_group += 1;
        let group = self.metadata.row_group(group_index);
        let column = group.column(self.column_index);
        let (start, length) = match checked_column_range(column, self.input.len()) {
            Ok(range) => range,
            Err(error) => {
                return Some(self.fail(error));
            }
        };
        let event_count = match u64::try_from(column.num_values()) {
            Ok(events) => events,
            Err(_) => {
                return Some(self.fail(storage(
                    "Parquet column event count is negative or too large",
                )));
            }
        };
        let reader = match self.input.get_read(start) {
            Ok(reader) => reader,
            Err(error) => {
                return Some(self.fail(storage(error)));
            }
        };
        let preflight = match (self.factory)(group_index, self.column_index) {
            Ok(preflight) => preflight,
            Err(error) => {
                return Some(self.fail(error));
            }
        };
        let page_reader = OwnedPageReader::new(
            reader,
            length,
            column.compression(),
            event_count,
            self.cancellation.clone(),
            self.failures.clone(),
            preflight,
        );
        Some(Ok(Box::new(page_reader)))
    }
}

impl<T, F, C> OwnedColumnPageIterator<T, F, C> {
    fn fail(&mut self, error: GfError) -> ParquetResult<Box<dyn PageReader>> {
        self.terminal = true;
        self.failures.record(error.clone());
        Err(as_parquet_error(error))
    }
}

impl<T, F, C> PageIterator for OwnedColumnPageIterator<T, F, C>
where
    T: ChunkReader + 'static,
    T::T: Read + Send + 'static,
    F: Fn(usize, usize) -> Result<C, GfError> + Send + Sync + 'static,
    C: PagePreflight + 'static,
{
}

fn validate_group_columns(
    group: &RowGroupMetaData,
    file_len: u64,
    cancellation: Option<&CancellationToken>,
) -> Result<(), GfError> {
    for column in group.columns() {
        check_cancelled(cancellation)?;
        checked_column_range(column, file_len)?;
        u64::try_from(column.num_values())
            .map_err(|_| storage("Parquet column event count is negative or too large"))?;
    }
    Ok(())
}

fn release_selection_charge(
    budget: &mut InventoryBudget,
    charged_before: u64,
) -> Result<(), GfError> {
    let charge = budget
        .live_bytes()
        .checked_sub(charged_before)
        .ok_or_else(|| storage("row-group selection budget decreased during construction"))?;
    budget.release(charge);
    Ok(())
}

fn checked_column_range(
    column: &parquet::file::metadata::ColumnChunkMetaData,
    file_len: u64,
) -> Result<(u64, u64), GfError> {
    let start = column
        .dictionary_page_offset()
        .unwrap_or_else(|| column.data_page_offset());
    let start = u64::try_from(start)
        .map_err(|_| storage("Parquet column start offset is negative or too large"))?;
    let length = u64::try_from(column.compressed_size())
        .map_err(|_| storage("Parquet column compressed length is negative or too large"))?;
    let end = start
        .checked_add(length)
        .ok_or_else(|| storage("Parquet column byte range overflows"))?;
    if end > file_len {
        return Err(storage("Parquet column byte range extends past its input"));
    }
    Ok((start, length))
}

fn check_cancelled(cancellation: Option<&CancellationToken>) -> Result<(), GfError> {
    if cancellation.is_some_and(CancellationToken::is_cancelled) {
        Err(cancelled())
    } else {
        Ok(())
    }
}

fn as_parquet_error(error: GfError) -> ParquetError {
    ParquetError::External(Box::new(error))
}

#[cfg(test)]
#[path = "parquet_row_groups/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "parquet_row_groups/empty_pages_tests.rs"]
mod empty_pages_tests;
