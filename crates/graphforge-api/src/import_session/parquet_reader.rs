//! A public Parquet `PageReader` that validates the owned bytes it returns.
//!
//! Header peeks are cached without owning page bodies. Data and dictionary
//! bodies are read once, decoded by the bounded owned-page path, passed to the
//! caller's preflight, then returned to Arrow from that same allocation.

use std::io::Read;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use graphforge_core::GfError;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::basic::Compression;
use parquet::column::page::{Page, PageMetadata, PageReader};
use parquet::errors::ParquetError;

use crate::CancellationToken;

use super::parquet_page;
use super::parquet_page_decode::{self, DecodedPage};
use super::parquet_scan::RawHeader;
use super::{cancelled, storage};

const INDEX_READ_BLOCK: usize = 8 << 10;
const INDEX_PAGE: i32 = 1;
const DATA_PAGE: i32 = 0;
const DICTIONARY_PAGE: i32 = 2;
const DATA_PAGE_V2: i32 = 3;

/// Callback which owns the task-shared shape, window, and decoder-cache
/// authority. The adapter asks for workspace immediately before reading each
/// body, then validates the decoded page before exposing it to Arrow.
pub(super) trait PagePreflight: Send {
    fn remaining_workspace(&self) -> Result<usize, GfError>;
    fn validate(&mut self, page: &DecodedPage) -> Result<(), GfError>;
    fn finish(&mut self) -> Result<(), GfError>;
}

/// First typed page-pipeline failure shared with the source-reader boundary.
/// Arrow's Parquet-to-Arrow error conversion stringifies `ParquetError`, so
/// callers retain this slot to recover the original GraphForge error.
#[derive(Clone, Default)]
pub(super) struct PageFailures(Arc<parking_lot::Mutex<PageFailureState>>);

#[derive(Default)]
struct PageFailureState {
    failed: bool,
    first: Option<GfError>,
}

impl PageFailures {
    pub(super) fn new() -> Self {
        Self::default()
    }

    pub(super) fn record(&self, error: GfError) {
        let mut state = self.0.lock();
        if !state.failed {
            state.failed = true;
            state.first = Some(error);
        }
    }

    pub(super) fn take(&self) -> Option<GfError> {
        self.0.lock().first.take()
    }

    pub(super) fn failed(&self) -> bool {
        self.0.lock().failed
    }
}

/// Source-boundary iterator which preserves typed preflight errors and drops
/// all native readers after the first failure or EOF. Arrow's own iterator
/// can continue to poll other columns after returning an error.
pub(super) struct OwnedBatchReader {
    reader: Option<ParquetRecordBatchReader>,
    failures: PageFailures,
}

impl OwnedBatchReader {
    pub(super) fn new(reader: ParquetRecordBatchReader, failures: PageFailures) -> Self {
        Self {
            reader: Some(reader),
            failures,
        }
    }
}

impl Iterator for OwnedBatchReader {
    type Item = Result<RecordBatch, GfError>;

    fn next(&mut self) -> Option<Self::Item> {
        let reader = self.reader.as_mut()?;
        if self.failures.failed() {
            self.reader = None;
            return Some(Err(self.failures.take().unwrap_or_else(|| {
                storage("Parquet task stopped after its page failure was consumed")
            })));
        }
        let result = reader.next();
        if self.failures.failed() {
            self.reader = None;
            return Some(Err(self.failures.take().unwrap_or_else(|| {
                storage("Parquet task stopped after its page failure was consumed")
            })));
        }
        match result {
            Some(Ok(batch)) => Some(Ok(batch)),
            Some(Err(error)) => {
                self.reader = None;
                let error = storage(error);
                self.failures.record(error.clone());
                Some(Err(self.failures.take().unwrap_or(error)))
            }
            None => {
                self.reader = None;
                None
            }
        }
    }
}

impl std::iter::FusedIterator for OwnedBatchReader {}

impl<T: PagePreflight + ?Sized> PagePreflight for Box<T> {
    fn remaining_workspace(&self) -> Result<usize, GfError> {
        (**self).remaining_workspace()
    }

    fn validate(&mut self, page: &DecodedPage) -> Result<(), GfError> {
        (**self).validate(page)
    }

    fn finish(&mut self) -> Result<(), GfError> {
        (**self).finish()
    }
}

struct PendingHeader {
    header_bytes: usize,
    header: RawHeader,
    compressed_bytes: usize,
    metadata: PageMetadata,
}

/// Owned-byte adapter implementing the public Parquet page-reader contract.
pub(super) struct OwnedPageReader<R, C> {
    reader: R,
    chunk_remaining: u64,
    compression: Compression,
    expected_data_events: u64,
    data_events: u64,
    cancellation: Option<CancellationToken>,
    failures: PageFailures,
    preflight: C,
    pending: Option<PendingHeader>,
    finish_called: bool,
    terminal: bool,
}

impl<R, C> OwnedPageReader<R, C>
where
    R: Read + Send,
    C: PagePreflight + Send,
{
    pub(super) fn new(
        reader: R,
        chunk_bytes: u64,
        compression: Compression,
        expected_data_events: u64,
        cancellation: Option<CancellationToken>,
        failures: PageFailures,
        preflight: C,
    ) -> Self {
        Self {
            reader,
            chunk_remaining: chunk_bytes,
            compression,
            expected_data_events,
            data_events: 0,
            cancellation,
            failures,
            preflight,
            pending: None,
            finish_called: false,
            terminal: false,
        }
    }

    fn next_page(&mut self) -> Result<Option<Page>, GfError> {
        if self.terminal || self.failures.failed() {
            return Ok(None);
        }
        self.check_cancel()?;
        if self.pending.is_none() && !self.load_next_header()? {
            return Ok(None);
        }
        let pending = self
            .pending
            .take()
            .ok_or_else(|| storage("Parquet page header cache is unexpectedly empty"))?;
        let workspace = self.preflight.remaining_workspace()?;
        let compressed = parquet_page::read_body(
            &mut self.reader,
            pending.header_bytes,
            pending.header,
            self.chunk_remaining,
            workspace,
            self.cancellation.as_ref(),
        )?;
        self.chunk_remaining = self
            .chunk_remaining
            .checked_sub(u64::try_from(pending.compressed_bytes).map_err(storage)?)
            .ok_or_else(|| storage("Parquet chunk byte count underflows"))?;

        let decoded = parquet_page_decode::decode(
            compressed,
            self.compression,
            workspace,
            self.cancellation.as_ref(),
        )?;
        let is_data = matches!(
            &decoded.page,
            Page::DataPage { .. } | Page::DataPageV2 { .. }
        );
        let next_events = if is_data {
            let next = self
                .data_events
                .checked_add(u64::from(decoded.page.num_values()))
                .ok_or_else(|| storage("Parquet data event total overflows"))?;
            if next > self.expected_data_events {
                return Err(storage("Parquet data pages exceed the footer event count"));
            }
            Some(next)
        } else {
            None
        };

        self.preflight.validate(&decoded)?;
        if let Some(events) = next_events {
            self.data_events = events;
        }
        Ok(Some(decoded.page))
    }

    fn load_next_header(&mut self) -> Result<bool, GfError> {
        loop {
            self.check_cancel()?;
            if self.chunk_remaining == 0 {
                self.finish_at_eof()?;
                return Ok(false);
            }

            let (header_bytes, header) = parquet_page::read_header(
                &mut self.reader,
                self.chunk_remaining,
                self.cancellation.as_ref(),
            )?;
            let header_length = u64::try_from(header_bytes).map_err(storage)?;
            self.chunk_remaining = self
                .chunk_remaining
                .checked_sub(header_length)
                .ok_or_else(|| storage("Parquet header extends beyond its column chunk"))?;
            let (compressed_bytes, _) = parquet_page_decode::body_lengths(&header)?;
            if u64::try_from(compressed_bytes).map_err(storage)? > self.chunk_remaining {
                return Err(storage("Parquet page extends beyond its column chunk"));
            }

            if header.kind == Some(INDEX_PAGE) {
                self.skip_index_body(&header, compressed_bytes)?;
                continue;
            }

            let metadata = parquet_page_decode::page_metadata(&header)?;
            if !matches!(
                header.kind,
                Some(DATA_PAGE | DICTIONARY_PAGE | DATA_PAGE_V2)
            ) {
                return Err(storage("Unsupported Parquet page type"));
            }
            self.pending = Some(PendingHeader {
                header_bytes,
                header,
                compressed_bytes,
                metadata,
            });
            return Ok(true);
        }
    }

    fn skip_index_body(
        &mut self,
        header: &RawHeader,
        compressed_bytes: usize,
    ) -> Result<(), GfError> {
        let mut remaining = compressed_bytes;
        let mut buffer = [0_u8; INDEX_READ_BLOCK];
        let mut checksum = crc32fast::Hasher::new();
        while remaining > 0 {
            self.check_cancel()?;
            let count = remaining.min(buffer.len());
            self.reader
                .read_exact(&mut buffer[..count])
                .map_err(storage)?;
            checksum.update(&buffer[..count]);
            remaining -= count;
        }
        if let Some(expected) = header.crc {
            let expected = i32::try_from(expected).map_err(storage)?;
            if checksum.finalize() != u32::from_ne_bytes(expected.to_ne_bytes()) {
                return Err(storage("Parquet page checksum mismatch"));
            }
        }
        self.chunk_remaining = self
            .chunk_remaining
            .checked_sub(u64::try_from(compressed_bytes).map_err(storage)?)
            .ok_or_else(|| storage("Parquet chunk byte count underflows"))?;
        Ok(())
    }

    fn finish_at_eof(&mut self) -> Result<(), GfError> {
        self.check_cancel()?;
        if self.data_events != self.expected_data_events {
            return Err(storage(
                "Parquet data pages disagree with the footer event count",
            ));
        }
        if !self.finish_called {
            self.finish_called = true;
            self.preflight.finish()?;
        }
        Ok(())
    }

    fn check_cancel(&self) -> Result<(), GfError> {
        if self
            .cancellation
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            return Err(cancelled());
        }
        Ok(())
    }

    fn typed_error(&mut self, error: GfError) -> ParquetError {
        self.failures.record(error.clone());
        self.pending = None;
        self.terminal = true;
        ParquetError::External(Box::new(error))
    }
}

impl<R, C> Iterator for OwnedPageReader<R, C>
where
    R: Read + Send,
    C: PagePreflight + Send,
{
    type Item = parquet::errors::Result<Page>;

    fn next(&mut self) -> Option<Self::Item> {
        match PageReader::get_next_page(self) {
            Ok(Some(page)) => Some(Ok(page)),
            Ok(None) => None,
            Err(error) => Some(Err(error)),
        }
    }
}

impl<R, C> PageReader for OwnedPageReader<R, C>
where
    R: Read + Send,
    C: PagePreflight + Send,
{
    fn get_next_page(&mut self) -> parquet::errors::Result<Option<Page>> {
        match self.next_page() {
            Ok(page) => Ok(page),
            Err(error) => Err(self.typed_error(error)),
        }
    }

    fn peek_next_page(&mut self) -> parquet::errors::Result<Option<PageMetadata>> {
        if self.terminal || self.failures.failed() {
            return Ok(None);
        }
        if let Err(error) = self.check_cancel() {
            return Err(self.typed_error(error));
        }
        if self.pending.is_none() {
            match self.load_next_header() {
                Ok(false) => return Ok(None),
                Ok(true) => {}
                Err(error) => return Err(self.typed_error(error)),
            }
        }
        Ok(self
            .pending
            .as_ref()
            .map(|pending| pending.metadata.clone()))
    }

    fn skip_next_page(&mut self) -> parquet::errors::Result<()> {
        self.get_next_page().map(|_| ())
    }
}

#[cfg(test)]
#[path = "parquet_reader/tests.rs"]
mod tests;
