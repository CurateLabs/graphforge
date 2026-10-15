//! Source-backed bounds for the synchronous destination-node reader.

use super::{add, column_bytes, mul, nonnegative, retained_column};
use arrow::datatypes::DataType;
use datafusion::error::DataFusionError;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::basic::Type;
use parquet::file::metadata::ParquetStatisticsPolicy;

mod metadata;

fn refused() -> DataFusionError {
    DataFusionError::ResourcesExhausted(
        "ExpandExec node Parquet decode: cannot bound decoder allocation".into(),
    )
}

/// Admit the actual selected topology authority before its node reader runs.
///
/// Includes full physical schemas: the majority-id fallback decodes and
/// concatenates every physical column before projection. All groups are kept
/// in this envelope, including a dense selection's conservative validation
/// retry. The same authority must subsequently be passed to the reader.
/// `admit_metadata` reserves each checked delta before its corresponding
/// allocation. The returned tuple is (decoder envelope, metadata envelope,
/// retained schemas). Keep both envelopes through the actual read, then shrink
/// metadata to retained schemas after the reader and retry workspace drops.
///
/// # Errors
/// Returns a typed refusal for unsupported shapes, codecs or size overflow,
/// and propagates authenticated source/metadata failures.
#[allow(clippy::implicit_hasher, clippy::too_many_lines)]
pub fn node_decode_admission_bytes(
    files: &crate::TopologyFiles,
    ids: &std::collections::HashSet<u64>,
    admit_metadata: impl FnMut(usize) -> Result<(), DataFusionError>,
) -> Result<(usize, usize, usize), DataFusionError> {
    node_bytes(files, ids, admit_metadata).map_err(|error| match error {
        DataFusionError::ResourcesExhausted(message)
            if message.starts_with("ExpandExec edge Parquet decode") =>
        {
            refused()
        }
        other => other,
    })
}

#[allow(clippy::implicit_hasher, clippy::too_many_lines)]
fn node_bytes(
    files: &crate::TopologyFiles,
    ids: &std::collections::HashSet<u64>,
    mut admit_metadata: impl FnMut(usize) -> Result<(), DataFusionError>,
) -> Result<(usize, usize, usize), DataFusionError> {
    if ids.is_empty() {
        return Ok((0, 0, 0));
    }
    let mut metadata = metadata::Admission::default();
    let mut retained = 0usize;
    let mut active = 0usize;
    for (path, _) in files.node_fragments() {
        let (file_metadata, footer_start) = metadata.file(path, &mut admit_metadata)?;
        let options =
            ArrowReaderOptions::new().with_size_stats_policy(ParquetStatisticsPolicy::KeepAll);
        let reader = crate::catalog::admitted_parquet_with_options(path, options)?;
        metadata.indexes(
            path,
            reader.metadata(),
            file_metadata,
            footer_start,
            &mut admit_metadata,
        )?;
        let rows = nonnegative(reader.metadata().file_metadata().num_rows())?;
        let group_rows = reader
            .metadata()
            .row_groups()
            .iter()
            .try_fold(0, |n, group| add(n, nonnegative(group.num_rows())?))?;
        if rows != group_rows {
            return Err(refused());
        }
        validate_pages(path, reader.metadata())?;
        let batch = rows.clamp(1, 1024);
        let batches = rows.div_ceil(batch);
        let slots = mul(batches, batch)?;
        let mut file_retained = 0usize;
        let mut scratch = 0usize;
        let mut transient = 0usize;
        let mut leaf = 0usize;
        for field in reader.schema().fields() {
            let descriptor = reader
                .parquet_schema()
                .columns()
                .get(leaf)
                .ok_or_else(refused)?;
            if descriptor.path().parts().first() != Some(field.name()) {
                return Err(refused());
            }
            let mut values = 0usize;
            let mut payload = 0usize;
            let mut column_scratch = 0usize;
            for group in reader.metadata().row_groups() {
                let column = group.column(leaf);
                let count = nonnegative(column.num_values())?;
                if descriptor.max_rep_level() == 0 && count != nonnegative(group.num_rows())? {
                    return Err(refused());
                }
                values = add(values, count)?;
                // Do not trust optional byte size statistics as a physical
                // allocation ceiling. Dictionary expansion is at most one
                // complete uncompressed chunk per logical value.
                let bytes = if column.column_type() == Type::BYTE_ARRAY {
                    mul(count, nonnegative(column.uncompressed_size())?)?
                } else {
                    0
                };
                payload = add(payload, bytes)?;
                column_scratch = column_scratch.max(column_bytes(column, batch)?);
            }
            let bytes = match field.data_type() {
                DataType::List(child)
                    if field.name() == "type_ids"
                        && child.data_type() == &DataType::UInt32
                        && descriptor.physical_type() == Type::INT32
                        && descriptor.max_rep_level() == 1 =>
                {
                    if values > i32::MAX as usize {
                        return Err(refused());
                    }
                    // parquet 58.4 ListArrayReader constructs offsets from
                    // leaf length, then copies initialized parent offsets.
                    // Primitive values can grow to twice their length and
                    // retain the initial batch-sized allocation. Four bitmaps
                    // and per-batch aligned headers remain conservative.
                    transient = add(
                        transient,
                        add(
                            mul(values, 16)?,
                            add(mul(values.div_ceil(8), 6)?, mul(batches, 1024)?)?,
                        )?,
                    )?;
                    add(
                        add(mul(add(values, slots)?, 8)?, mul(add(rows, batches)?, 4)?)?,
                        add(mul(values.div_ceil(8), 4)?, mul(batches, 2048)?)?,
                    )?
                }
                kind if descriptor.max_rep_level() == 0 => {
                    retained_column(kind, slots, batches, payload)?
                }
                _ => return Err(refused()),
            };
            file_retained = add(file_retained, bytes)?;
            scratch = add(scratch, column_scratch)?;
            leaf += 1;
        }
        if leaf != reader.parquet_schema().num_columns() {
            return Err(refused());
        }
        if reader.schema().index_of("type_ids").is_err() {
            // Legacy normalization creates offsets and a singleton UInt32
            // child from type_id. PrimitiveBuilder starts at 1024 entries.
            file_retained = add(
                file_retained,
                add(
                    mul(add(rows, batches)?, 4)?,
                    add(
                        mul(mul(batches, 1024)?.max(mul(rows, 2)?), 4)?,
                        mul(batches, 1024)?,
                    )?,
                )?,
            )?;
        }
        retained = add(retained, file_retained)?;
        // Old dense output may survive a conservative retry. Its decoder,
        // accumulated batches and concat/filter copy overlap. Normalization
        // and all previous files remain live as well.
        active = active.max(add(add(scratch, transient)?, mul(file_retained, 2)?)?);
    }
    // Predicate membership clones, sorted IDs and dense validation's expected
    // set coexist with the caller's reached set. A conservative eight-times
    // fixed slot bound includes bucket controls, alignment and growth copies.
    Ok((
        add(add(retained, active)?, add(mul(ids.len(), 8 * 8)?, 1024)?)?,
        metadata.held,
        metadata.retained,
    ))
}

/// Raw header counts and byte sizes are allocation authorities in Parquet 58.4.
/// Authentication fixes the file identity; it does not prove their agreement
/// with the footer. Inspect headers with bounded transport before decoder use.
#[allow(clippy::too_many_lines)]
fn validate_pages(
    path: &std::path::Path,
    metadata: &parquet::file::metadata::ParquetMetaData,
) -> Result<(), DataFusionError> {
    use parquet::basic::Compression;

    use std::io::{Read, Seek, SeekFrom};
    // A small fixed window avoids a syscall for every compact scalar while
    // limiting payload read-ahead to at most 255 bytes per audited header.
    // The already-admitted metadata workspace covers this stack allocation.
    struct Header<'a> {
        file: &'a mut std::fs::File,
        consumed: usize,
        start: u64,
        limit: usize,
        window: [u8; 256],
        window_start: usize,
        window_len: usize,
    }
    impl Seek for Header<'_> {
        fn seek(&mut self, from: SeekFrom) -> std::io::Result<u64> {
            let position = match from {
                SeekFrom::Start(position) => Some(position),
                SeekFrom::Current(delta) => self
                    .start
                    .checked_add(self.consumed as u64)
                    .and_then(|position| position.checked_add_signed(delta)),
                SeekFrom::End(delta) => self.file.metadata()?.len().checked_add_signed(delta),
            }
            .ok_or_else(|| std::io::Error::other("bounded header seek overflow"))?;
            let consumed = position
                .checked_sub(self.start)
                .and_then(|n| usize::try_from(n).ok())
                .filter(|n| *n <= self.limit)
                .ok_or_else(|| std::io::Error::other("bounded header seek escaped its range"))?;
            // Protocol peeks seek within this window, without moving the file.
            self.consumed = consumed;
            Ok(position)
        }
    }
    impl Read for Header<'_> {
        fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
            let available = self.limit.saturating_sub(self.consumed).min(output.len());
            if available == 0 {
                return Ok(0);
            }
            if self.consumed < self.window_start
                || self.consumed >= self.window_start + self.window_len
            {
                self.file
                    .seek(SeekFrom::Start(self.start + self.consumed as u64))?;
                self.window_start = self.consumed;
                let request = self.window.len().min(self.limit - self.consumed);
                self.window_len = self.file.read(&mut self.window[..request])?;
                // Attribute actual physical reads, including bounded read-ahead,
                // rather than the parser's one-byte logical read requests.
                crate::lifecycle_io::record_read(
                    crate::StorageIoPhase::ReadPathScan,
                    self.window_len as u64,
                    u64::from(self.window_len != 0),
                );
                if self.window_len == 0 {
                    return Ok(0);
                }
            }
            let offset = self.consumed - self.window_start;
            let count = available.min(self.window_len - offset);
            output[..count].copy_from_slice(&self.window[offset..offset + count]);
            self.consumed += count;
            Ok(count)
        }
    }
    let invalid =
        |message: &str| DataFusionError::Execution(format!("invalid node Parquet page: {message}"));
    let mut file = std::fs::File::open(path).map_err(DataFusionError::IoError)?;
    let length = file.metadata().map_err(DataFusionError::IoError)?.len();
    for group in metadata.row_groups() {
        for column in group.columns() {
            let offset = |value| u64::try_from(value).map_err(|_| invalid("negative chunk size"));
            let start = offset(column.data_page_offset())?.min(
                column
                    .dictionary_page_offset()
                    .map(offset)
                    .transpose()?
                    .unwrap_or(u64::MAX),
            );
            let end = start
                .checked_add(offset(column.compressed_size())?)
                .filter(|end| *end <= length)
                .ok_or_else(|| invalid("chunk range"))?;
            let mut position = start;
            let mut bytes = 0u64;
            let mut values = 0u64;
            while position < end {
                file.seek(SeekFrom::Start(position))
                    .map_err(DataFusionError::IoError)?;
                let mut transport = Header {
                    file: &mut file,
                    consumed: 0,
                    start: position,
                    limit: usize::try_from((end - position).min(64 * 1024))
                        .map_err(|_| invalid("header range"))?,
                    window: [0; 256],
                    window_start: 0,
                    window_len: 0,
                };
                let header = page_header(&mut thrift::protocol::TCompactInputProtocol::new(
                    &mut transport,
                ))
                .map_err(|_| invalid("malformed or oversized header"))?;
                let header_bytes = transport.consumed as u64;
                let compressed = offset(i64::from(header.compressed))?;
                let uncompressed = offset(i64::from(header.uncompressed))?;
                let uncompressed_body = column.compression() == Compression::UNCOMPRESSED
                    || (header.kind == 3 && header.compressed_v2 == Some(false));
                if uncompressed_body && compressed != uncompressed {
                    return Err(invalid("uncompressed header/body size disagreement"));
                }
                bytes = bytes
                    .checked_add(header_bytes)
                    .and_then(|v| v.checked_add(uncompressed))
                    .filter(|v| *v <= offset(column.uncompressed_size()).unwrap_or(0))
                    .ok_or_else(|| invalid("page sizes exceed column footer"))?;
                if header.kind == 2 {
                    let count = offset(i64::from(
                        header
                            .values
                            .ok_or_else(|| invalid("missing dictionary header"))?,
                    ))?;
                    let minimum = match column.column_type() {
                        Type::BOOLEAN => Some(count.div_ceil(8)),
                        Type::INT32 | Type::FLOAT | Type::BYTE_ARRAY => count.checked_mul(4),
                        Type::INT64 | Type::DOUBLE => count.checked_mul(8),
                        Type::INT96 => count.checked_mul(12),
                        Type::FIXED_LEN_BYTE_ARRAY => count
                            .checked_mul(offset(i64::from(column.column_descr().type_length()))?),
                    };
                    if minimum.is_none_or(|v| v > uncompressed) {
                        return Err(invalid("dictionary count exceeds page bytes"));
                    }
                } else if matches!(header.kind, 0 | 3) {
                    let count = offset(i64::from(
                        header
                            .values
                            .ok_or_else(|| invalid("missing data header"))?,
                    ))?;
                    values = values
                        .checked_add(count)
                        .filter(|v| *v <= offset(column.num_values()).unwrap_or(0))
                        .ok_or_else(|| invalid("page values exceed column footer"))?;
                } else {
                    return Err(refused());
                }
                position = position
                    .checked_add(header_bytes)
                    .and_then(|v| v.checked_add(compressed))
                    .filter(|v| *v <= end)
                    .ok_or_else(|| invalid("page escapes chunk"))?;
            }
            if values != offset(column.num_values())? {
                return Err(invalid("page values disagree with column footer"));
            }
        }
    }
    Ok(())
}

struct PageSizes {
    kind: i32,
    compressed: i32,
    uncompressed: i32,
    values: Option<i32>,
    compressed_v2: Option<bool>,
}

fn header_error() -> thrift::Error {
    thrift::Error::Protocol(thrift::ProtocolError::new(
        thrift::ProtocolErrorKind::InvalidData,
        "invalid bounded node page header",
    ))
}

/// Read only scalar allocation authorities. Unknown fields are skipped without
/// constructing their byte strings/collections: thrift's generated reader
/// otherwise allocates a binary length before the bounded transport sees EOF.
fn checked_field(
    p: &mut (impl thrift::protocol::TInputProtocol + std::io::Seek),
    last: &mut i16,
) -> thrift::Result<thrift::protocol::TFieldIdentifier> {
    let position = p.stream_position()?;
    let byte = p.read_i8()?.cast_unsigned();
    p.seek(std::io::SeekFrom::Start(position))?;
    if byte >> 4 != 0 && last.checked_add(i16::from(byte >> 4)).is_none() {
        return Err(header_error());
    }
    let field = p.read_field_begin()?;
    if let Some(id) = field.id {
        *last = id;
    }
    Ok(field)
}

fn page_header(
    p: &mut (impl thrift::protocol::TInputProtocol + std::io::Seek),
) -> thrift::Result<PageSizes> {
    use thrift::protocol::TType;
    let mut kind = None;
    let mut compressed = None;
    let mut uncompressed = None;
    let mut data_values = None;
    let mut dictionary_values = None;
    let mut v2_values = None;
    let mut compressed_v2 = None;
    p.read_struct_begin()?;
    let mut last = 0;
    loop {
        let field = checked_field(p, &mut last)?;
        if field.field_type == TType::Stop {
            break;
        }
        match (field.id, field.field_type) {
            (Some(1), TType::I32) => kind = Some(p.read_i32()?),
            (Some(2), TType::I32) => uncompressed = Some(p.read_i32()?),
            (Some(3), TType::I32) => compressed = Some(p.read_i32()?),
            (Some(5 | 7 | 8), TType::Struct) => {
                p.read_struct_begin()?;
                let mut nested_last = 0;
                loop {
                    let nested = checked_field(p, &mut nested_last)?;
                    if nested.field_type == TType::Stop {
                        break;
                    }
                    match (nested.id, nested.field_type) {
                        (Some(1), TType::I32) => {
                            let value = Some(p.read_i32()?);
                            match field.id {
                                Some(5) => data_values = value,
                                Some(7) => dictionary_values = value,
                                Some(8) => v2_values = value,
                                _ => unreachable!(),
                            }
                        }
                        (Some(7), TType::Bool) if field.id == Some(8) => {
                            compressed_v2 = Some(p.read_bool()?);
                        }
                        _ => skip_field(p, nested.field_type, 0)?,
                    }
                    p.read_field_end()?;
                }
                p.read_struct_end()?;
            }
            _ => skip_field(p, field.field_type, 0)?,
        }
        p.read_field_end()?;
    }
    p.read_struct_end()?;
    let kind = kind.ok_or_else(header_error)?;
    Ok(PageSizes {
        kind,
        compressed: compressed.ok_or_else(header_error)?,
        uncompressed: uncompressed.ok_or_else(header_error)?,
        values: match kind {
            0 => data_values,
            2 => dictionary_values,
            3 => v2_values,
            _ => None,
        },
        compressed_v2,
    })
}

fn skip_field(
    p: &mut (impl thrift::protocol::TInputProtocol + std::io::Seek),
    kind: thrift::protocol::TType,
    depth: usize,
) -> thrift::Result<()> {
    use thrift::protocol::TType;
    if depth >= 64 {
        return Err(header_error());
    }
    match kind {
        TType::Bool => {
            p.read_bool()?;
        }
        TType::I08 => {
            p.read_i8()?;
        }
        TType::I16 => {
            p.read_i16()?;
        }
        TType::I32 => {
            p.read_i32()?;
        }
        TType::I64 => {
            p.read_i64()?;
        }
        TType::Double => {
            p.read_double()?;
        }
        TType::String => {
            // Compact binary lengths are unsigned varints, not zig-zag I32s.
            let mut length = 0u32;
            let mut complete = false;
            for shift in (0..35).step_by(7) {
                let byte = p.read_i8()?.cast_unsigned();
                if shift == 28 && byte > 15 {
                    return Err(header_error());
                }
                length |= u32::from(byte & 127) << shift;
                if byte & 128 == 0 {
                    complete = true;
                    break;
                }
            }
            if !complete || length > 64 * 1024 {
                return Err(header_error());
            }
            for _ in 0..length {
                p.read_i8()?;
            }
        }
        TType::Struct => {
            p.read_struct_begin()?;
            let mut last = 0;
            loop {
                let field = checked_field(p, &mut last)?;
                if field.field_type == TType::Stop {
                    break;
                }
                skip_field(p, field.field_type, depth + 1)?;
                p.read_field_end()?;
            }
            p.read_struct_end()?;
        }
        TType::List | TType::Set => {
            let (element, count) = if kind == TType::List {
                let list = p.read_list_begin()?;
                (list.element_type, list.size)
            } else {
                let set = p.read_set_begin()?;
                (set.element_type, set.size)
            };
            if !(0..=64 * 1024).contains(&count) {
                return Err(header_error());
            }
            for _ in 0..count {
                skip_field(p, element, depth + 1)?;
            }
            if kind == TType::List {
                p.read_list_end()?;
            } else {
                p.read_set_end()?;
            }
        }
        TType::Map => {
            let map = p.read_map_begin()?;
            if !(0..=64 * 1024).contains(&map.size) {
                return Err(header_error());
            }
            for _ in 0..map.size {
                skip_field(p, map.key_type.ok_or_else(header_error)?, depth + 1)?;
                skip_field(p, map.value_type.ok_or_else(header_error)?, depth + 1)?;
            }
            p.read_map_end()?;
        }
        _ => return Err(header_error()),
    }
    Ok(())
}

#[cfg(test)]
mod tests;
