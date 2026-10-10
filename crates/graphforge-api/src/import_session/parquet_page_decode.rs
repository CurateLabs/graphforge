//! Decompress the currently owned page into an admitted, fixed destination.
//!
//! This transport does not authorize Arrow's value/level allocations: the
//! shared event/shape preflight must run on `page.buffer()` before dispatch.

use std::io::Read;

use bytes::Bytes;
use graphforge_core::GfError;
use parquet::basic::{Compression, Encoding};
use parquet::column::page::Page;

use crate::CancellationToken;

use super::parquet_page::CompressedPage;
use super::parquet_scan::RawHeader;
use super::{cancelled, limit, storage};

const BLOCK_BYTES: usize = 8 << 10;

pub(super) struct DecodedPage {
    pub(super) page: Page,
    /// Charge the full allocation, even when later readers retain a sub-slice.
    pub(super) body_capacity: usize,
    pub(super) physical_bytes: u64,
}

fn check(cancellation: Option<&CancellationToken>) -> Result<(), GfError> {
    if cancellation.is_some_and(CancellationToken::is_cancelled) {
        return Err(cancelled());
    }
    Ok(())
}

fn natural(value: Option<i64>, name: &str) -> Result<u32, GfError> {
    let value = value.ok_or_else(|| storage(format!("Missing Parquet {name}")))?;
    let value = i32::try_from(value).map_err(storage)?;
    u32::try_from(value).map_err(storage)
}

#[allow(deprecated)] // Decode the legacy BIT_PACKED encoding used by V1 files.
fn encoding(value: Option<i32>) -> Result<Encoding, GfError> {
    match value {
        Some(0) => Ok(Encoding::PLAIN),
        Some(2) => Ok(Encoding::PLAIN_DICTIONARY),
        Some(3) => Ok(Encoding::RLE),
        Some(4) => Ok(Encoding::BIT_PACKED),
        Some(5) => Ok(Encoding::DELTA_BINARY_PACKED),
        Some(6) => Ok(Encoding::DELTA_LENGTH_BYTE_ARRAY),
        Some(7) => Ok(Encoding::DELTA_BYTE_ARRAY),
        Some(8) => Ok(Encoding::RLE_DICTIONARY),
        Some(9) => Ok(Encoding::BYTE_STREAM_SPLIT),
        _ => Err(storage("Invalid or missing Parquet page encoding")),
    }
}

struct Shape {
    kind: i32,
    uncompressed: usize,
    values: u32,
    encoding: Encoding,
    definition_encoding: Option<Encoding>,
    repetition_encoding: Option<Encoding>,
    definition_bytes: u32,
    repetition_bytes: u32,
    nulls: u32,
    rows: u32,
    compressed_values: bool,
}

impl Shape {
    fn new(header: &RawHeader, compressed_length: usize) -> Result<Self, GfError> {
        let kind = header
            .kind
            .ok_or_else(|| storage("Missing Parquet page type"))?;
        let uncompressed = natural(header.uncompressed, "uncompressed page size")? as usize;
        let mut shape = Self {
            kind,
            uncompressed,
            values: natural(header.values, "page value count")?,
            encoding: encoding(header.encoding)?,
            definition_encoding: None,
            repetition_encoding: None,
            definition_bytes: 0,
            repetition_bytes: 0,
            nulls: 0,
            rows: 0,
            compressed_values: true,
        };
        match kind {
            0 => {
                shape.definition_encoding = Some(encoding(header.definition_encoding)?);
                shape.repetition_encoding = Some(encoding(header.repetition_encoding)?);
            }
            2 => {}
            3 => {
                shape.definition_bytes =
                    natural(header.definition_bytes, "definition level length")?;
                shape.repetition_bytes =
                    natural(header.repetition_bytes, "repetition level length")?;
                shape.nulls = natural(header.nulls, "null count")?;
                shape.rows = natural(header.rows, "row count")?;
                shape.compressed_values = header.compressed_values.unwrap_or(true);
                let prefix = shape.level_bytes()?;
                if prefix > compressed_length || prefix > uncompressed {
                    return Err(storage("Parquet V2 level sections exceed the page body"));
                }
                if shape.nulls > shape.values || shape.rows > shape.values {
                    return Err(storage("Parquet V2 row/null counts exceed its value count"));
                }
            }
            _ => return Err(storage("Unsupported Parquet page type")),
        }
        Ok(shape)
    }

    fn level_bytes(&self) -> Result<usize, GfError> {
        (self.definition_bytes as usize)
            .checked_add(self.repetition_bytes as usize)
            .ok_or_else(|| storage("Parquet V2 level lengths overflow"))
    }

    fn metadata(&self) -> Result<parquet::column::page::PageMetadata, GfError> {
        let values = usize::try_from(self.values).map_err(storage)?;
        match self.kind {
            0 => Ok(parquet::column::page::PageMetadata {
                num_rows: None,
                num_levels: Some(values),
                is_dict: false,
            }),
            2 => Ok(parquet::column::page::PageMetadata {
                num_rows: None,
                num_levels: None,
                is_dict: true,
            }),
            3 => Ok(parquet::column::page::PageMetadata {
                num_rows: Some(usize::try_from(self.rows).map_err(storage)?),
                num_levels: Some(values),
                is_dict: false,
            }),
            _ => Err(storage("Unsupported Parquet page type")),
        }
    }

    fn page(self, body: Bytes, header: &RawHeader) -> Result<Page, GfError> {
        match self.kind {
            0 => Ok(Page::DataPage {
                buf: body,
                num_values: self.values,
                encoding: self.encoding,
                def_level_encoding: self
                    .definition_encoding
                    .ok_or_else(|| storage("Missing definition encoding"))?,
                rep_level_encoding: self
                    .repetition_encoding
                    .ok_or_else(|| storage("Missing repetition encoding"))?,
                statistics: None,
            }),
            2 => Ok(Page::DictionaryPage {
                buf: body,
                num_values: self.values,
                encoding: self.encoding,
                is_sorted: header.sorted_dictionary.unwrap_or(false),
            }),
            3 => Ok(Page::DataPageV2 {
                buf: body,
                num_values: self.values,
                encoding: self.encoding,
                num_nulls: self.nulls,
                num_rows: self.rows,
                def_levels_byte_len: self.definition_bytes,
                rep_levels_byte_len: self.repetition_bytes,
                is_compressed: false,
                statistics: None,
            }),
            _ => Err(storage("Unsupported Parquet page type")),
        }
    }
}

/// Validate the two declared body lengths without allocating either body.
pub(super) fn body_lengths(header: &RawHeader) -> Result<(usize, usize), GfError> {
    let compressed =
        usize::try_from(natural(header.compressed, "compressed page size")?).map_err(storage)?;
    let uncompressed = usize::try_from(natural(header.uncompressed, "uncompressed page size")?)
        .map_err(storage)?;
    Ok((compressed, uncompressed))
}

/// Validate the header geometry/counts and expose the public reader metadata.
pub(super) fn page_metadata(
    header: &RawHeader,
) -> Result<parquet::column::page::PageMetadata, GfError> {
    let (compressed, _) = body_lengths(header)?;
    Shape::new(header, compressed)?.metadata()
}

fn lz4_length(
    input: &[u8],
    cursor: &mut usize,
    initial: usize,
    cancellation: Option<&CancellationToken>,
) -> Result<usize, GfError> {
    let mut length = initial;
    if initial != 15 {
        return Ok(length);
    }
    loop {
        check(cancellation)?;
        let byte = *input
            .get(*cursor)
            .ok_or_else(|| storage("Truncated LZ4 length"))?;
        *cursor += 1;
        length = length
            .checked_add(usize::from(byte))
            .ok_or_else(|| storage("LZ4 length overflows"))?;
        if byte != 255 {
            return Ok(length);
        }
    }
}

/// Decode raw tokens into the fixed destination. Both long literal copies and
/// overlapping matches check cancellation at most 8KiB of output apart; the
/// public block decoder has no callback inside a single enormous raw block.
fn raw_lz4(
    input: &[u8],
    output: &mut [u8],
    cancellation: Option<&CancellationToken>,
) -> Result<(), GfError> {
    let (mut cursor, mut written) = (0_usize, 0_usize);
    while cursor < input.len() {
        check(cancellation)?;
        let token = input[cursor];
        cursor += 1;
        let literals = lz4_length(input, &mut cursor, usize::from(token >> 4), cancellation)?;
        let source_end = cursor
            .checked_add(literals)
            .ok_or_else(|| storage("LZ4 literal range overflows"))?;
        let destination_end = written
            .checked_add(literals)
            .ok_or_else(|| storage("LZ4 output range overflows"))?;
        let source = input
            .get(cursor..source_end)
            .ok_or_else(|| storage("Truncated LZ4 literal bytes"))?;
        let destination = output
            .get_mut(written..destination_end)
            .ok_or_else(|| storage("LZ4 literals exceed admitted page output"))?;
        for (source, destination) in source
            .chunks(BLOCK_BYTES)
            .zip(destination.chunks_mut(BLOCK_BYTES))
        {
            check(cancellation)?;
            destination.copy_from_slice(source);
        }
        cursor = source_end;
        written = destination_end;
        if cursor == input.len() {
            break;
        }
        let offset_end = cursor
            .checked_add(2)
            .ok_or_else(|| storage("LZ4 offset overflows"))?;
        let raw = input
            .get(cursor..offset_end)
            .ok_or_else(|| storage("Truncated LZ4 match offset"))?;
        let offset = usize::from(u16::from_le_bytes([raw[0], raw[1]]));
        cursor = offset_end;
        if offset == 0 || offset > written {
            return Err(storage("Invalid LZ4 backwards match offset"));
        }
        let length = lz4_length(input, &mut cursor, usize::from(token & 15), cancellation)?
            .checked_add(4)
            .ok_or_else(|| storage("LZ4 match length overflows"))?;
        let end = written
            .checked_add(length)
            .ok_or_else(|| storage("LZ4 match range overflows"))?;
        if end > output.len() {
            return Err(storage("LZ4 match exceeds admitted page output"));
        }
        let mut distance = offset;
        while written < end {
            check(cancellation)?;
            let step = distance.min(BLOCK_BYTES).min(end - written);
            output.copy_within(written - distance..written - distance + step, written);
            written += step;
            // Grow a fully reconstructed, period-aligned history prefix for
            // tiny offsets instead of copying a one-byte match in a huge loop.
            if distance < BLOCK_BYTES && step == distance {
                distance *= 2;
            }
        }
        // The pinned block grammar requires a final literal token after a
        // match, even when its expanded bytes already fill the destination.
        if cursor == input.len() {
            return Err(storage("LZ4 match is missing its final literal token"));
        }
    }
    check(cancellation)?;
    if written != output.len() {
        return Err(storage("LZ4 page output disagrees with its stated size"));
    }
    Ok(())
}

/// Hadoop framing uses independent raw blocks and no decoder heap workspace.
fn hadoop_lz4(
    mut input: &[u8],
    mut output: &mut [u8],
    cancellation: Option<&CancellationToken>,
) -> Result<(), GfError> {
    while !input.is_empty() {
        check(cancellation)?;
        let prefix = input
            .get(..8)
            .ok_or_else(|| storage("Truncated Hadoop LZ4 frame"))?;
        let expanded = u32::from_be_bytes(prefix[..4].try_into().map_err(storage)?) as usize;
        let compressed = u32::from_be_bytes(prefix[4..].try_into().map_err(storage)?) as usize;
        input = &input[8..];
        let source = input
            .get(..compressed)
            .ok_or_else(|| storage("Truncated Hadoop LZ4 block"))?;
        let destination = output
            .get_mut(..expanded)
            .ok_or_else(|| storage("Hadoop LZ4 block exceeds admitted output"))?;
        raw_lz4(source, destination, cancellation)?;
        input = &input[compressed..];
        output = &mut output[expanded..];
    }
    if !output.is_empty() {
        return Err(storage("Hadoop LZ4 page is shorter than its stated size"));
    }
    Ok(())
}

fn framed_lz4(
    input: &[u8],
    output: &mut [u8],
    workspace: usize,
    cancellation: Option<&CancellationToken>,
) -> Result<(), GfError> {
    let needed = super::bounded_ipc::lz4_frame_workspace(input)?;
    if needed > workspace {
        return Err(limit("LZ4 frame buffers exceed admitted codec workspace"));
    }
    let mut decoder = lz4_flex::frame::FrameDecoder::new(input);
    let mut written = 0;
    while written < output.len() {
        check(cancellation)?;
        let end = written.saturating_add(BLOCK_BYTES).min(output.len());
        let count = decoder.read(&mut output[written..end]).map_err(storage)?;
        if count == 0 {
            return Err(storage("LZ4 frame is shorter than its stated page size"));
        }
        written += count;
    }
    check(cancellation)?;
    let mut probe = [0_u8; 1];
    if decoder.read(&mut probe).map_err(storage)? != 0 {
        return Err(storage("LZ4 frame exceeds its stated page size"));
    }
    Ok(())
}

fn decompress(
    codec: Compression,
    input: &[u8],
    output: &mut [u8],
    workspace: usize,
    cancellation: Option<&CancellationToken>,
) -> Result<(), GfError> {
    check(cancellation)?;
    match codec {
        Compression::SNAPPY => super::parquet_codec::snappy(input, output)?,
        Compression::GZIP(_) => {
            super::parquet_codec::gzip_cancellable(input, output, workspace, cancellation)?;
        }
        Compression::BROTLI(_) => {
            super::parquet_brotli::decode_cancellable(input, output, workspace, cancellation)?;
        }
        Compression::ZSTD(_) => super::parquet_codec::zstd(input, output, workspace)?,
        Compression::LZ4_RAW => raw_lz4(input, output, cancellation)?,
        Compression::LZ4 => {
            // Preserve the pinned codec's historical formats on the same
            // immutable owned bytes. A denied frame allocation is never
            // mistaken for a format error or bypassed by the raw path.
            if hadoop_lz4(input, output, cancellation).is_err() {
                check(cancellation)?;
                let magic = input
                    .get(..4)
                    .map(|bytes| u32::from_le_bytes(bytes.try_into().expect("four bytes")));
                if matches!(magic, Some(0x184D_2204 | 0x184C_2102)) {
                    framed_lz4(input, output, workspace, cancellation)?;
                } else {
                    raw_lz4(input, output, cancellation)?;
                }
            }
        }
        Compression::UNCOMPRESSED => return Err(storage("Unexpected uncompressed codec dispatch")),
        Compression::LZO => return Err(storage("Unsupported Parquet compression codec")),
    }
    check(cancellation)
}

/// `capacity` covers the currently owned compressed body PLUS new decoded
/// output PLUS codec state. Retained dictionaries/decoder bodies are separate
/// live credits held by the caller and must already have been subtracted.
pub(super) fn decode(
    compressed: CompressedPage,
    codec: Compression,
    capacity: usize,
    cancellation: Option<&CancellationToken>,
) -> Result<DecodedPage, GfError> {
    check(cancellation)?;
    let shape = Shape::new(&compressed.header, compressed.body.len())?;
    if compressed.body_capacity > capacity {
        return Err(limit("Owned Parquet body exceeds remaining page workspace"));
    }
    let prefix = shape.level_bytes()?;
    let is_compressed = codec != Compression::UNCOMPRESSED && shape.compressed_values;
    let (body, body_capacity) = if is_compressed {
        let available = capacity - compressed.body_capacity;
        if shape.uncompressed > available {
            return Err(limit("Parquet decoded page exceeds remaining workspace"));
        }
        let mut output = Vec::new();
        output
            .try_reserve_exact(shape.uncompressed)
            .map_err(|_| limit("Cannot allocate admitted decoded Parquet page"))?;
        let actual = output.capacity();
        if actual > available {
            return Err(limit(
                "Decoded Parquet page capacity exceeds remaining workspace",
            ));
        }
        while output.len() < shape.uncompressed {
            check(cancellation)?;
            let end = output
                .len()
                .saturating_add(BLOCK_BYTES)
                .min(shape.uncompressed);
            output.resize(end, 0);
        }
        for (source, destination) in compressed.body[..prefix]
            .chunks(BLOCK_BYTES)
            .zip(output[..prefix].chunks_mut(BLOCK_BYTES))
        {
            check(cancellation)?;
            destination.copy_from_slice(source);
        }
        // V2 pages containing only level/null events have no encoded value
        // suffix. The ordinary reader does not initialize a codec for them.
        if shape.uncompressed > prefix {
            decompress(
                codec,
                &compressed.body[prefix..],
                &mut output[prefix..],
                available - actual,
                cancellation,
            )?;
        }
        (Bytes::from(output), actual)
    } else {
        if compressed.body.len() != shape.uncompressed {
            return Err(storage(
                "Uncompressed Parquet page size disagrees with its header",
            ));
        }
        (compressed.body, compressed.body_capacity)
    };
    check(cancellation)?;
    let physical_bytes = compressed.physical_bytes;
    let page = shape.page(body, &compressed.header)?;
    Ok(DecodedPage {
        page,
        body_capacity,
        physical_bytes,
    })
}

#[cfg(test)]
#[path = "parquet_page_decode/tests.rs"]
mod tests;
