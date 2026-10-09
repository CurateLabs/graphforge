//! Read a page's current header and body into one admitted owned allocation.
//!
//! The same owned compressed bytes must supply checksum/decompression and all
//! later preflight. A prior header inventory never authorizes a reread of an
//! unvalidated changed body through SerializedPageReader.

use std::io::Read;

use bytes::Bytes;
use graphforge_core::GfError;

use crate::CancellationToken;

use super::parquet_scan::{RawHeader, read_header};
use super::{cancelled, limit, storage};

const READ_BLOCK: usize = 8 << 10;

pub(super) struct CompressedPage {
    pub(super) header: RawHeader,
    pub(super) body: Bytes,
    pub(super) physical_bytes: u64,
    /// The allocation remains owned even when a later consumer takes a slice.
    pub(super) body_capacity: usize,
}

fn check(cancellation: Option<&CancellationToken>) -> Result<(), GfError> {
    if cancellation.is_some_and(CancellationToken::is_cancelled) {
        return Err(cancelled());
    }
    Ok(())
}

/// Caller read-ahead and retained decoder bodies have separate live credits.
/// `capacity` is the remaining credit for this newly owned compressed body.
pub(super) fn read<R: Read>(
    reader: &mut R,
    remaining_chunk_bytes: u64,
    capacity: usize,
    cancellation: Option<&CancellationToken>,
) -> Result<CompressedPage, GfError> {
    check(cancellation)?;
    let mut bounded = reader.take(remaining_chunk_bytes);
    let (header_bytes, header) = read_header(&mut bounded)?;
    let compressed = usize::try_from(
        header
            .compressed
            .ok_or_else(|| storage("Missing compressed page size"))?,
    )
    .map_err(storage)?;
    let physical_bytes = u64::try_from(header_bytes)
        .map_err(storage)?
        .checked_add(u64::try_from(compressed).map_err(storage)?)
        .ok_or_else(|| storage("Parquet page byte range overflows"))?;
    if physical_bytes > remaining_chunk_bytes {
        return Err(storage("Parquet page extends beyond its column chunk"));
    }
    if compressed > capacity {
        return Err(limit(
            "Parquet compressed page exceeds its admitted workspace",
        ));
    }
    // Exactly requested capacity is admitted before the allocation request.
    let mut body = Vec::new();
    body.try_reserve_exact(compressed)
        .map_err(|_| limit("Cannot allocate admitted Parquet compressed page"))?;
    if body.capacity() > capacity {
        return Err(limit(
            "Parquet compressed page capacity exceeds its admitted workspace",
        ));
    }
    body.resize(compressed, 0);
    let mut checksum = crc32fast::Hasher::new();
    for block in body.chunks_mut(READ_BLOCK) {
        check(cancellation)?;
        bounded.read_exact(block).map_err(storage)?;
        checksum.update(block);
    }
    if let Some(expected) = header.crc {
        let expected = i32::try_from(expected).map_err(storage)?;
        if checksum.finalize() != u32::from_ne_bytes(expected.to_ne_bytes()) {
            return Err(storage("Parquet page checksum mismatch"));
        }
    }
    let body_capacity = body.capacity();
    Ok(CompressedPage {
        header,
        body: Bytes::from(body),
        physical_bytes,
        body_capacity,
    })
}

#[cfg(test)]
#[path = "parquet_page/tests.rs"]
mod tests;
