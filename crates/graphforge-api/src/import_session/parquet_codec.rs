//! Page codecs write into the caller's admitted destination, never a growing Vec.
//!
//! Gzip metadata is borrowed, including arbitrarily long names/comments. The
//! fixed miniz state is charged before construction and reused across members.

use graphforge_core::GfError;
use miniz_oxide::inflate::stream::{InflateState, inflate};
use miniz_oxide::{DataFormat, MZFlush, MZStatus};

use crate::CancellationToken;

use super::{cancelled, limit, storage};

const OUTPUT_BLOCK: usize = 8 << 10;

fn check(cancellation: Option<&CancellationToken>) -> Result<(), GfError> {
    if cancellation.is_some_and(CancellationToken::is_cancelled) {
        return Err(cancelled());
    }
    Ok(())
}

fn checksum(input: &[u8], cancellation: Option<&CancellationToken>) -> Result<u32, GfError> {
    let mut hasher = crc32fast::Hasher::new();
    for block in input.chunks(OUTPUT_BLOCK) {
        check(cancellation)?;
        hasher.update(block);
    }
    Ok(hasher.finalize())
}

pub(super) fn gzip_workspace() -> usize {
    std::mem::size_of::<InflateState>()
}

fn u32_at(input: &[u8], offset: usize) -> Result<u32, GfError> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| storage("Gzip offset overflow"))?;
    let bytes = input
        .get(offset..end)
        .ok_or_else(|| storage("Truncated gzip trailer"))?;
    Ok(u32::from_le_bytes(bytes.try_into().map_err(storage)?))
}

fn gzip_header(input: &[u8], cancellation: Option<&CancellationToken>) -> Result<usize, GfError> {
    check(cancellation)?;
    let header = input
        .get(..10)
        .ok_or_else(|| storage("Truncated gzip header"))?;
    if header[..3] != [0x1f, 0x8b, 8] || header[3] & 0xe0 != 0 {
        return Err(storage("Invalid gzip header"));
    }
    let flags = header[3];
    let mut offset = 10;
    if flags & 4 != 0 {
        let length = input
            .get(offset..offset + 2)
            .ok_or_else(|| storage("Truncated gzip extra length"))?;
        offset += 2;
        offset += usize::from(u16::from_le_bytes([length[0], length[1]]));
        if offset > input.len() {
            return Err(storage("Truncated gzip extra field"));
        }
    }
    for flag in [8, 16] {
        if flags & flag != 0 {
            let suffix = input
                .get(offset..)
                .ok_or_else(|| storage("Truncated gzip metadata"))?;
            let mut length = None;
            for (index, block) in suffix.chunks(OUTPUT_BLOCK).enumerate() {
                check(cancellation)?;
                if let Some(within) = block.iter().position(|byte| *byte == 0) {
                    length = Some(index * OUTPUT_BLOCK + within);
                    break;
                }
            }
            let length = length.ok_or_else(|| storage("Unterminated gzip metadata"))?;
            offset = offset
                .checked_add(length + 1)
                .ok_or_else(|| storage("Gzip metadata length overflow"))?;
        }
    }
    if flags & 2 != 0 {
        let checksum_bytes = input
            .get(offset..offset + 2)
            .ok_or_else(|| storage("Truncated gzip header checksum"))?;
        let expected = u16::from_le_bytes([checksum_bytes[0], checksum_bytes[1]]);
        if checksum(&input[..offset], cancellation)? as u16 != expected {
            return Err(storage("Gzip header checksum mismatch"));
        }
        offset += 2;
    }
    Ok(offset)
}

/// Multi-member GZIP, matching Parquet's ordinary MultiGzDecoder consumer.
/// Input/output are already charged; workspace covers only the fixed state.
pub(super) fn gzip(input: &[u8], output: &mut [u8], workspace: usize) -> Result<(), GfError> {
    gzip_cancellable(input, output, workspace, None)
}

pub(super) fn gzip_cancellable(
    input: &[u8],
    output: &mut [u8],
    workspace: usize,
    cancellation: Option<&CancellationToken>,
) -> Result<(), GfError> {
    check(cancellation)?;
    if workspace < gzip_workspace() {
        return Err(limit("Gzip decoder state exceeds its admitted workspace"));
    }
    let mut state = InflateState::new_boxed(DataFormat::Raw);
    let mut source = 0;
    let mut written = 0;
    while source < input.len() {
        check(cancellation)?;
        source += gzip_header(&input[source..], cancellation)?;
        state.reset(DataFormat::Raw);
        let member_start = written;
        loop {
            check(cancellation)?;
            let mut probe = [0_u8; 1];
            let checking_end = written == output.len();
            let destination = if checking_end {
                &mut probe[..]
            } else {
                let end = written.saturating_add(OUTPUT_BLOCK).min(output.len());
                &mut output[written..end]
            };
            let result = inflate(&mut state, &input[source..], destination, MZFlush::None);
            source += result.bytes_consumed;
            if checking_end && result.bytes_written != 0 {
                return Err(storage("Gzip page output exceeds its stated size"));
            }
            written += result.bytes_written;
            match result.status {
                Ok(MZStatus::StreamEnd) => break,
                Ok(_) if result.bytes_consumed != 0 || result.bytes_written != 0 => {}
                _ => return Err(storage("Invalid or truncated gzip deflate stream")),
            }
        }
        let expected_checksum = u32_at(input, source)?;
        let expected_length = u32_at(
            input,
            source
                .checked_add(4)
                .ok_or_else(|| storage("Gzip trailer offset overflow"))?,
        )?;
        if checksum(&output[member_start..written], cancellation)? != expected_checksum
            || (written - member_start) as u32 != expected_length
        {
            return Err(storage("Gzip member checksum or length mismatch"));
        }
        source += 8;
    }
    if written != output.len() {
        return Err(storage("Gzip page output is shorter than its stated size"));
    }
    Ok(())
}

/// Snappy's body declares a separate decoded length; inspect it before a
/// decoder can use it. The supplied destination never grows from that count.
pub(super) fn snappy(input: &[u8], output: &mut [u8]) -> Result<(), GfError> {
    let claimed = snap::raw::decompress_len(input).map_err(storage)?;
    if claimed != output.len() {
        return Err(storage(
            "Snappy body length disagrees with the admitted page size",
        ));
    }
    let decoded = snap::raw::Decoder::new()
        .decompress(input, output)
        .map_err(storage)?;
    if decoded != output.len() {
        return Err(storage(
            "Snappy output disagrees with the admitted page size",
        ));
    }
    Ok(())
}

/// Fixed modern one-shot DCtx envelope from pinned zstd 1.5.7:
/// tables, history pointers and maximum literal scratch total <165,070 bytes.
/// No stream, dictionary or legacy decoder is initialized on this route.
pub(super) const ZSTD_WORKSPACE: usize = 256 << 10;

pub(super) fn zstd(input: &[u8], output: &mut [u8], workspace: usize) -> Result<(), GfError> {
    if workspace < ZSTD_WORKSPACE {
        return Err(limit("Zstd decoder state exceeds its admitted workspace"));
    }
    let mut context = zstd_safe::DCtx::try_create()
        .ok_or_else(|| limit("Cannot allocate admitted Zstd decoder state"))?;
    if context.sizeof() > ZSTD_WORKSPACE {
        return Err(limit(
            "Zstd decoder state exceeds the pinned allocation envelope",
        ));
    }
    let decoded = context
        .decompress(output, input)
        .map_err(|error| storage(zstd_safe::get_error_name(error)))?;
    if context.sizeof() > ZSTD_WORKSPACE {
        return Err(limit(
            "Zstd decoder state exceeds the pinned allocation envelope",
        ));
    }
    if decoded != output.len() {
        return Err(storage("Zstd output disagrees with the admitted page size"));
    }
    Ok(())
}

#[cfg(test)]
#[path = "parquet_codec/tests.rs"]
mod tests;
