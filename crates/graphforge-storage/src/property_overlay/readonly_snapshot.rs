//! Bounded, write-free reads of oversized legacy property Parquet files.

use super::{GfError, corrupt};
use crate::corruption_checksum::Checksum;
use graphforge_filesystem::FileIdentity;
use std::fs::File;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

pub(super) const BLOCK_BYTES: usize = 4 * 1024 * 1024;
const IO_BYTES: usize = 64 * 1024;

#[derive(Debug)]
pub(super) struct ReadonlyIndex {
    length: u64,
    checksums: Vec<u64>,
}

#[derive(Debug, Default)]
struct Counters {
    authentication_bytes: AtomicU64,
    authentication_blocks: AtomicU64,
    authentication_calls: AtomicU64,
    physical_bytes: AtomicU64,
    physical_calls: AtomicU64,
}

#[derive(Debug)]
struct CachedBlock {
    number: usize,
    bytes: Box<[u8]>,
}

/// One decoder's immutable authority and single verified block cache.
#[derive(Debug)]
pub(super) struct ReadonlyFile {
    file: Arc<File>,
    index: Arc<ReadonlyIndex>,
    cached: Mutex<Option<CachedBlock>>,
    counters: Counters,
    opening_authentication: (u64, u64, u64),
    reservation_bytes: u64,
}

pub(super) fn open(
    source: &File,
    expected_identity: FileIdentity,
    entry: &crate::GraphReadFileEntry,
    max_buffered_bytes: u64,
    cached_index: &OnceLock<Result<Arc<ReadonlyIndex>, GfError>>,
) -> Result<ReadonlyFile, GfError> {
    let reservation_bytes = reservation_bytes(entry.byte_length)?;
    if reservation_bytes > max_buffered_bytes {
        return Err(resource_limit(format!(
            "legacy property read needs {reservation_bytes} buffered bytes; limit is {max_buffered_bytes}"
        )));
    }
    let metadata = source.metadata().map_err(super::io_error)?;
    if !metadata.is_file() || metadata.len() != entry.byte_length {
        return Err(corrupt(
            "property handle length or kind conflicts with inventory",
        ));
    }
    if graphforge_filesystem::file_identity(source).map_err(super::io_error)? != expected_identity {
        return Err(corrupt(
            "property fragment identity changed after admission",
        ));
    }

    let counters = Counters::default();
    let index = cached_index
        .get_or_init(|| {
            build_index(
                source,
                expected_identity,
                entry.byte_length,
                entry.content_xxh64,
                &counters,
            )
            .map(Arc::new)
        })
        .clone()?;
    if index.length != entry.byte_length {
        return Err(corrupt(
            "property block index length conflicts with inventory",
        ));
    }
    let opening_authentication = (
        counters.authentication_bytes.load(Ordering::Relaxed),
        counters.authentication_blocks.load(Ordering::Relaxed),
        counters.authentication_calls.load(Ordering::Relaxed),
    );
    Ok(ReadonlyFile {
        file: Arc::new(source.try_clone().map_err(super::io_error)?),
        index,
        cached: Mutex::new(None),
        counters,
        opening_authentication,
        reservation_bytes,
    })
}

#[cfg(test)]
mod tests;

pub(super) fn reservation_bytes(length: u64) -> Result<u64, GfError> {
    let block_count = length.div_ceil(BLOCK_BYTES as u64);
    let index_bytes = block_count
        .checked_mul(std::mem::size_of::<u64>() as u64)
        .ok_or_else(|| resource_limit("legacy property block index size overflow"))?;
    index_bytes
        .checked_add(2 * BLOCK_BYTES as u64)
        .ok_or_else(|| resource_limit("legacy property read reservation overflow"))
}

impl ReadonlyFile {
    pub(super) fn length(&self) -> u64 {
        self.index.length
    }

    pub(super) fn reservation_bytes(&self) -> u64 {
        self.reservation_bytes
    }

    pub(super) fn authentication(&self) -> (u64, u64, u64) {
        (
            self.counters
                .authentication_bytes
                .load(Ordering::Relaxed)
                .saturating_sub(self.opening_authentication.0),
            self.counters
                .authentication_blocks
                .load(Ordering::Relaxed)
                .saturating_sub(self.opening_authentication.1),
            self.counters
                .authentication_calls
                .load(Ordering::Relaxed)
                .saturating_sub(self.opening_authentication.2),
        )
    }

    pub(super) fn opening_authentication(&self) -> (u64, u64, u64) {
        self.opening_authentication
    }

    pub(super) fn physical_reads(&self) -> (u64, u64) {
        (
            self.counters.physical_bytes.load(Ordering::Relaxed),
            self.counters.physical_calls.load(Ordering::Relaxed),
        )
    }

    pub(super) fn read_at(&self, output: &mut [u8], offset: u64) -> io::Result<usize> {
        let Ok(start) = usize::try_from(offset) else {
            return Ok(0);
        };
        if start >= usize::try_from(self.index.length).unwrap_or(usize::MAX) || output.is_empty() {
            return Ok(0);
        }
        let available = usize::try_from(self.index.length)
            .unwrap_or(usize::MAX)
            .saturating_sub(start);
        let wanted = output.len().min(available);
        let mut copied = 0;
        while copied < wanted {
            let position = start + copied;
            let block_number = position / BLOCK_BYTES;
            let block_offset = position % BLOCK_BYTES;
            let cached = self.verified_block(block_number)?;
            let block = cached.as_ref().expect("verified block is cached");
            let count = (wanted - copied).min(block.bytes.len() - block_offset);
            output[copied..copied + count]
                .copy_from_slice(&block.bytes[block_offset..block_offset + count]);
            copied += count;
        }
        Ok(copied)
    }

    fn verified_block(
        &self,
        number: usize,
    ) -> io::Result<std::sync::MutexGuard<'_, Option<CachedBlock>>> {
        let mut cached = self
            .cached
            .lock()
            .map_err(|_| io::Error::other("property block cache lock poisoned"))?;
        if cached.as_ref().is_none_or(|block| block.number != number) {
            let expected = *self.index.checksums.get(number).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "property block index is incomplete",
                )
            })?;
            let start = u64::try_from(number)
                .ok()
                .and_then(|number| number.checked_mul(BLOCK_BYTES as u64))
                .ok_or_else(|| io::Error::other("property block offset overflow"))?;
            let length = usize::try_from((self.index.length - start).min(BLOCK_BYTES as u64))
                .map_err(|_| io::Error::other("property block length overflow"))?;
            let mut bytes = vec![0_u8; length].into_boxed_slice();
            let mut read_calls = 0_u64;
            let mut consumed = 0;
            while consumed < length {
                let read = super::retained_read_at(
                    &self.file,
                    &mut bytes[consumed..],
                    start + consumed as u64,
                )?;
                if read == 0 {
                    self.record_block_read(consumed as u64, read_calls);
                    return Err(corrupt_io(
                        "property file was truncated during a block read",
                    ));
                }
                consumed += read;
                read_calls = read_calls.saturating_add(1);
            }
            let bytes_read = u64::try_from(length).unwrap_or(u64::MAX);
            self.record_block_read(bytes_read, read_calls);
            if bytes.len() != length || checksum(&bytes) != expected {
                return Err(corrupt_io(
                    "property block checksum conflicts with inventory",
                ));
            }
            *cached = Some(CachedBlock { number, bytes });
        }
        Ok(cached)
    }

    fn record_block_read(&self, bytes: u64, calls: u64) {
        let blocks = bytes.div_ceil(IO_BYTES as u64);
        self.counters
            .authentication_bytes
            .fetch_add(bytes, Ordering::Relaxed);
        self.counters
            .authentication_blocks
            .fetch_add(blocks, Ordering::Relaxed);
        self.counters
            .authentication_calls
            .fetch_add(calls, Ordering::Relaxed);
        self.counters
            .physical_bytes
            .fetch_add(bytes, Ordering::Relaxed);
        self.counters
            .physical_calls
            .fetch_add(calls, Ordering::Relaxed);
        crate::lifecycle_io::record_read(crate::StorageIoPhase::ReadPathScan, bytes, calls);
        crate::lifecycle_io::record_blocks(crate::StorageIoPhase::ReadPathScan, blocks);
    }
}

fn build_index(
    source: &File,
    expected_identity: FileIdentity,
    length: u64,
    expected_xxh64: u64,
    counters: &Counters,
) -> Result<ReadonlyIndex, GfError> {
    let count = usize::try_from(length.div_ceil(BLOCK_BYTES as u64))
        .map_err(|_| resource_limit("legacy property block index is too large"))?;
    let mut checksums = Vec::new();
    checksums
        .try_reserve_exact(count)
        .map_err(|_| resource_limit("legacy property block index allocation exceeds memory"))?;
    let mut block = vec![0_u8; BLOCK_BYTES];
    let mut whole = Checksum::new();
    let mut bytes = 0_u64;
    let mut calls = 0_u64;
    for _ in 0..count {
        let expected_length = usize::try_from((length - bytes).min(BLOCK_BYTES as u64))
            .map_err(|_| resource_limit("legacy property block length is not representable"))?;
        let mut consumed = 0;
        while consumed < expected_length {
            let end = (consumed + IO_BYTES).min(expected_length);
            let read =
                super::retained_read_at(source, &mut block[consumed..end], bytes + consumed as u64)
                    .map_err(super::io_error)?;
            if read == 0 {
                record_index_read(counters, bytes + consumed as u64, calls);
                return Err(corrupt(
                    "property file was truncated during checksum indexing",
                ));
            }
            consumed += read;
            calls = calls.saturating_add(1);
        }
        whole.update(&block[..expected_length]);
        checksums.push(checksum(&block[..expected_length]));
        bytes = bytes
            .checked_add(expected_length as u64)
            .ok_or_else(|| corrupt("property authentication byte overflow"))?;
    }
    record_index_read(counters, bytes, calls);
    if bytes != length || whole.finish() != expected_xxh64 {
        return Err(corrupt("property checksum digest conflicts with inventory"));
    }
    if graphforge_filesystem::file_identity(source).map_err(super::io_error)? != expected_identity
        || source.metadata().map_err(super::io_error)?.len() != length
    {
        return Err(corrupt(
            "property fragment identity changed during checksum indexing",
        ));
    }
    Ok(ReadonlyIndex { length, checksums })
}

fn record_index_read(counters: &Counters, bytes: u64, calls: u64) {
    let blocks = bytes.div_ceil(IO_BYTES as u64);
    counters
        .authentication_bytes
        .fetch_add(bytes, Ordering::Relaxed);
    counters
        .authentication_blocks
        .fetch_add(blocks, Ordering::Relaxed);
    counters
        .authentication_calls
        .fetch_add(calls, Ordering::Relaxed);
    counters.physical_bytes.fetch_add(bytes, Ordering::Relaxed);
    counters.physical_calls.fetch_add(calls, Ordering::Relaxed);
    crate::lifecycle_io::record_read(crate::StorageIoPhase::ReadPathScan, bytes, calls);
    crate::lifecycle_io::record_blocks(crate::StorageIoPhase::ReadPathScan, blocks);
}

fn checksum(bytes: &[u8]) -> u64 {
    let mut checksum = Checksum::new();
    checksum.update(bytes);
    checksum.finish()
}

fn corrupt_io(message: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("property overlay corruption: {message}"),
    )
}

fn resource_limit(message: impl Into<String>) -> GfError {
    GfError::Project {
        code: graphforge_core::ProjectErrorCode::ResourceLimit,
        message: format!("property overlay: {}", message.into()),
    }
}
