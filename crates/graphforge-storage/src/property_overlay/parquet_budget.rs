//! Parquet budget for authenticated property overlays.

use super::{
    Arc, AtomicU64, AuthenticatedPropertyFragment, BTreeSet, BufReader, Bytes, ChunkReader, File,
    GfError, Length, OpenPropertyFragment, Ordering, PROPERTY_GENERATION_KEY, PROPERTY_KIND_KEY,
    PROPERTY_ORDINAL_KEY, PROPERTY_OVERLAY_FORMAT, PROPERTY_OVERLAY_FORMAT_KEY, PROPERTY_ROUTE_KEY,
    PROPERTY_TOMBSTONE_FIELD, ParquetRecordBatchReaderBuilder, PropertyFragmentId,
    PropertyFragmentLayout, PropertyOverlayLimits, PropertyOverlayMetrics, PropertyRouteKind, Read,
    RecordBatch, TSerializable, corrupt, io_error, parquet_error,
};

#[derive(Debug, Default)]
pub(super) struct ReadCounts {
    pub(super) bytes: AtomicU64,
    pub(super) blocks: AtomicU64,
    pub(super) range_seeks: AtomicU64,
}

#[derive(Debug)]
pub(super) struct CountingChunkReader {
    pub(super) file: Arc<File>,
    pub(super) length: u64,
    pub(super) counts: Arc<ReadCounts>,
}

pub(super) struct CountingRead<R> {
    inner: R,
    counts: Arc<ReadCounts>,
}

struct HeaderRead {
    file: Arc<File>,
    position: u64,
    remaining: usize,
    consumed: Arc<AtomicU64>,
    counts: Arc<ReadCounts>,
}

impl Read for HeaderRead {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let limit = buffer.len().min(self.remaining);
        if limit == 0 {
            return Ok(0);
        }
        let read = retained_read_at(&self.file, &mut buffer[..limit], self.position)?;
        self.position = self
            .position
            .saturating_add(u64::try_from(read).unwrap_or(u64::MAX));
        self.remaining -= read;
        if read != 0 {
            let read = u64::try_from(read).unwrap_or(u64::MAX);
            self.consumed.fetch_add(read, Ordering::Relaxed);
            self.counts.bytes.fetch_add(read, Ordering::Relaxed);
            self.counts.blocks.fetch_add(1, Ordering::Relaxed);
        }
        Ok(read)
    }
}

pub(super) struct PositionedRead {
    file: Arc<File>,
    position: u64,
}

impl Read for PositionedRead {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let read = retained_read_at(&self.file, buffer, self.position)?;
        self.position = self
            .position
            .checked_add(u64::try_from(read).unwrap_or(u64::MAX))
            .ok_or_else(|| std::io::Error::other("retained read offset overflow"))?;
        Ok(read)
    }
}

#[cfg(unix)]
pub(super) fn retained_read_at(
    file: &File,
    buffer: &mut [u8],
    offset: u64,
) -> std::io::Result<usize> {
    std::os::unix::fs::FileExt::read_at(file, buffer, offset)
}

#[cfg(windows)]
pub(super) fn retained_read_at(
    file: &File,
    buffer: &mut [u8],
    offset: u64,
) -> std::io::Result<usize> {
    std::os::windows::fs::FileExt::seek_read(file, buffer, offset)
}

impl<R: std::io::Read> std::io::Read for CountingRead<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        const BLOCK_BYTES: usize = 64 * 1024;
        let limit = buffer.len().min(BLOCK_BYTES);
        let read = self.inner.read(&mut buffer[..limit])?;
        if read != 0 {
            self.counts
                .bytes
                .fetch_add(u64::try_from(read).unwrap_or(u64::MAX), Ordering::Relaxed);
            self.counts.blocks.fetch_add(1, Ordering::Relaxed);
        }
        Ok(read)
    }
}

impl Length for CountingChunkReader {
    fn len(&self) -> u64 {
        self.length
    }
}

impl ChunkReader for CountingChunkReader {
    type T = CountingRead<BufReader<PositionedRead>>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        self.counts.range_seeks.fetch_add(1, Ordering::Relaxed);
        Ok(CountingRead {
            inner: BufReader::new(PositionedRead {
                file: Arc::clone(&self.file),
                position: start,
            }),
            counts: Arc::clone(&self.counts),
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        let mut reader = self.get_read(start)?;
        let mut buffer = vec![0; length];
        std::io::Read::read_exact(&mut reader, &mut buffer)?;
        Ok(Bytes::from(buffer))
    }
}

#[derive(Clone, Copy)]
pub(super) struct TargetReadAdmission {
    pub(super) limits: PropertyOverlayLimits,
    pub(super) page_reservation_bytes: u64,
    pub(super) replay: bool,
}

impl TargetReadAdmission {
    pub(super) fn error(self, message: &str) -> GfError {
        if self.replay {
            replay_decoder_limit(message)
        } else {
            corrupt(message)
        }
    }

    pub(super) fn check(self, arrow_bytes: u64, retained_bytes: u64) -> Result<u64, GfError> {
        // Existing non-replay targeted callers retain their original budget.
        let retained_bytes = if self.replay { retained_bytes } else { 0 };
        let bytes = self
            .page_reservation_bytes
            .checked_add(arrow_bytes)
            .and_then(|bytes| bytes.checked_add(retained_bytes))
            .ok_or_else(|| self.error("targeted property decode memory overflow"))?;
        if bytes > self.limits.max_buffered_bytes {
            return Err(self.error("targeted property decode exceeds live-byte budget"));
        }
        Ok(bytes)
    }
}

pub(super) fn charge_target_batch(
    metrics: &mut PropertyOverlayMetrics,
    batch: &RecordBatch,
    admission: TargetReadAdmission,
    retained_bytes: u64,
) -> Result<(), GfError> {
    let arrow_bytes = u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX);
    let decoded_reservation = admission
        .limits
        .max_row_bytes
        .saturating_mul(u64::try_from(batch.num_rows()).unwrap_or(u64::MAX));
    let bytes = admission.check(
        arrow_bytes.saturating_add(decoded_reservation),
        retained_bytes,
    )?;
    metrics.emitted_batches = metrics.emitted_batches.saturating_add(1);
    metrics.decoder_peak_rows = metrics.decoder_peak_rows.max(batch.num_rows() as u64);
    metrics.decoder_peak_bytes = metrics.decoder_peak_bytes.max(arrow_bytes);
    metrics.peak_buffered_bytes = metrics.peak_buffered_bytes.max(bytes);
    Ok(())
}

pub(super) fn admit_target_footer(file: &File, length: u64, budget: usize) -> Result<(), GfError> {
    if length < 8 {
        return Err(corrupt("property Parquet footer is truncated"));
    }
    let mut footer = [0_u8; 8];
    if retained_read_at(file, &mut footer, length - 8).map_err(io_error)? != footer.len()
        || &footer[4..] != b"PAR1"
    {
        return Err(corrupt("property Parquet footer is invalid"));
    }
    let encoded =
        u32::from_le_bytes(footer[..4].try_into().expect("four-byte footer length")) as usize;
    if encoded > 16 * 1024 * 1024 || encoded.saturating_mul(4).saturating_add(64 * 1024) > budget {
        return Err(replay_decoder_limit(
            "property Parquet footer exceeds replay budget",
        ));
    }
    Ok(())
}

pub(super) fn open_counted_retained_property_builder(
    fragment: &AuthenticatedPropertyFragment,
    opened: &OpenPropertyFragment,
    counts: Arc<ReadCounts>,
) -> Result<ParquetRecordBatchReaderBuilder<CountingChunkReader>, GfError> {
    ParquetRecordBatchReaderBuilder::try_new(CountingChunkReader {
        file: Arc::clone(&opened.file),
        length: fragment.entry.byte_length,
        counts,
    })
    .map_err(parquet_error)
}

pub(super) fn validate_fragment_schema(
    schema: &arrow::datatypes::Schema,
    id: PropertyFragmentId,
    layout: PropertyFragmentLayout,
    kind: PropertyRouteKind,
    route: &str,
) -> Result<(), GfError> {
    let uuid = schema
        .field_with_name(kind.uuid_field())
        .map_err(|_| corrupt("property fragment lacks its UUID field"))?;
    if uuid.is_nullable() || uuid.data_type() != &arrow::datatypes::DataType::FixedSizeBinary(16) {
        return Err(corrupt(
            "property UUID field is nullable or not fixed binary(16)",
        ));
    }
    if schema.fields().first().map(|field| field.name().as_str()) != Some(kind.uuid_field()) {
        return Err(corrupt("property UUID field is not canonical first field"));
    }
    if layout == PropertyFragmentLayout::LegacyFlat {
        return Ok(());
    }
    let expected = [
        (
            PROPERTY_OVERLAY_FORMAT_KEY,
            PROPERTY_OVERLAY_FORMAT.to_owned(),
        ),
        (PROPERTY_ROUTE_KEY, route.to_owned()),
        (PROPERTY_KIND_KEY, kind.metadata_value().to_owned()),
        (PROPERTY_GENERATION_KEY, id.generation.to_string()),
        (PROPERTY_ORDINAL_KEY, id.ordinal.to_string()),
    ];
    for (key, value) in expected {
        if schema.metadata().get(key) != Some(&value) {
            return Err(corrupt(
                "property fragment metadata conflicts with its identity",
            ));
        }
    }
    let tombstone = schema
        .field_with_name(PROPERTY_TOMBSTONE_FIELD)
        .map_err(|_| corrupt("property snapshot fragment lacks tombstone field"))?;
    if tombstone.is_nullable() || tombstone.data_type() != &arrow::datatypes::DataType::Boolean {
        return Err(corrupt(
            "property tombstone field is nullable or not boolean",
        ));
    }
    if schema.fields().get(1).map(|field| field.name().as_str()) != Some(PROPERTY_TOMBSTONE_FIELD) {
        return Err(corrupt("property tombstone is not canonical second field"));
    }
    Ok(())
}

#[allow(
    deprecated,
    reason = "Parquet 58 exposes raw compact-Thrift page type and sizes only here"
)]
#[allow(
    clippy::too_many_lines,
    reason = "raw page parsing and aggregate pre-decode admission form one proof"
)]
pub(super) fn validate_parquet_resource_admission(
    metadata: &parquet::file::metadata::ParquetMetaData,
    limits: PropertyOverlayLimits,
    file: &File,
    counts: &Arc<ReadCounts>,
    projected_columns: Option<&BTreeSet<usize>>,
) -> Result<u64, GfError> {
    Ok(parquet_resource_admission(
        metadata,
        limits,
        file,
        counts,
        projected_columns,
        admitted_batch_rows(limits),
        false,
        corrupt,
    )?
    .decoded_bytes)
}

pub(super) struct ParquetDecoderMemory {
    decoded_bytes: u64,
    pub(super) with_codec_bytes: u64,
}

pub(super) fn replay_decoder_limit(message: &str) -> GfError {
    GfError::Project {
        code: graphforge_core::ProjectErrorCode::ResourceLimit,
        message: message.into(),
    }
}

/// Reserve authenticated page, codec and batch exposure before replay decoding.
/// Reuses the same bounded raw-page parser as property admission.
pub(crate) fn replay_parquet_reader_reservation(
    metadata: &parquet::file::metadata::ParquetMetaData,
    file: &File,
    max_memory_bytes: usize,
    batch_rows: usize,
) -> Result<usize, GfError> {
    let limits = PropertyOverlayLimits {
        max_buffered_bytes: max_memory_bytes as u64,
        ..PropertyOverlayLimits::default()
    };
    let admission = parquet_resource_admission(
        metadata,
        limits,
        file,
        &Arc::new(ReadCounts::default()),
        None,
        batch_rows,
        true,
        replay_decoder_limit,
    )?;
    let bytes = usize::try_from(admission.with_codec_bytes)
        .map_err(|_| replay_decoder_limit("replay decoder reservation overflows"))?;
    if bytes > max_memory_bytes {
        return Err(replay_decoder_limit(
            "replay decoder pages and codecs exceed memory budget",
        ));
    }
    Ok(bytes)
}

#[allow(
    deprecated,
    reason = "Parquet 58 raw page sizes are exposed through compact Thrift headers"
)]
#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "one authenticated page scan computes legacy and replay resource envelopes"
)]
pub(super) fn parquet_resource_admission(
    metadata: &parquet::file::metadata::ParquetMetaData,
    limits: PropertyOverlayLimits,
    file: &File,
    counts: &Arc<ReadCounts>,
    projected_columns: Option<&BTreeSet<usize>>,
    batch_rows: usize,
    include_codec: bool,
    limit_error: fn(&str) -> GfError,
) -> Result<ParquetDecoderMemory, GfError> {
    const MAX_PAGE_HEADER_BYTES: usize = 64 * 1024;
    let max_page_bytes = limits.max_buffered_bytes / 4;
    if max_page_bytes == 0 {
        return Err(limit_error("property page byte budget is too small"));
    }
    let mut largest_group_exposure = 0_u64;
    let mut column_memory = if include_codec {
        vec![0_u64; metadata.file_metadata().schema_descr().num_columns()]
    } else {
        Vec::new()
    };
    let mut group_values = Vec::with_capacity(if include_codec {
        metadata.num_row_groups()
    } else {
        0
    });
    for group in metadata.row_groups() {
        let mut group_value_bytes = 0_u64;
        let mut group_exposure = 0_u64;
        for (column_index, column) in group.columns().iter().enumerate() {
            let selected = projected_columns.is_none_or(|columns| columns.contains(&column_index));
            let mut dictionary_exposure = 0_u64;
            let mut data_exposure = 0_u64;
            let mut compressed_dictionary = 0_u64;
            let mut compressed_data = 0_u64;
            let uncompressed = u64::try_from(column.uncompressed_size())
                .map_err(|_| corrupt("property column chunk has negative uncompressed size"))?;
            let compressed = u64::try_from(column.compressed_size())
                .map_err(|_| corrupt("property column chunk has negative compressed size"))?;
            let data_offset = u64::try_from(column.data_page_offset())
                .map_err(|_| corrupt("property column chunk has negative data-page offset"))?;
            let start = column
                .dictionary_page_offset()
                .map(|offset| {
                    u64::try_from(offset)
                        .map_err(|_| corrupt("property dictionary page has negative offset"))
                })
                .transpose()?
                .map_or(data_offset, |offset| offset.min(data_offset));
            let end = start
                .checked_add(compressed)
                .ok_or_else(|| corrupt("property column chunk range overflows"))?;
            if end > file.metadata().map_err(io_error)?.len() {
                return Err(corrupt("property column chunk escapes authenticated file"));
            }
            let mut position = start;
            while position < end {
                let consumed = Arc::new(AtomicU64::new(0));
                let transport = HeaderRead {
                    file: Arc::new(file.try_clone().map_err(io_error)?),
                    position,
                    remaining: MAX_PAGE_HEADER_BYTES,
                    consumed: Arc::clone(&consumed),
                    counts: Arc::clone(counts),
                };
                let mut protocol = thrift::protocol::TCompactInputProtocol::new(transport);
                #[allow(deprecated, reason = "Parquet 58 exposes raw page headers only here")]
                let header = parquet::format::PageHeader::read_from_in_protocol(&mut protocol)
                    .map_err(|_| corrupt("property page header is malformed or oversized"))?;
                let header_bytes = consumed.load(Ordering::Relaxed);
                #[allow(deprecated, reason = "Parquet 58 page admission requires raw sizes")]
                let compressed_page = u64::try_from(header.compressed_page_size)
                    .map_err(|_| corrupt("property page has negative compressed size"))?;
                #[allow(deprecated, reason = "Parquet 58 page admission requires raw sizes")]
                let uncompressed_page = u64::try_from(header.uncompressed_page_size)
                    .map_err(|_| corrupt("property page has negative uncompressed size"))?;
                if selected && uncompressed_page > max_page_bytes {
                    return Err(limit_error(
                        "property page exceeds pre-decode byte admission",
                    ));
                }
                if header_bytes == 0 || compressed_page > compressed {
                    return Err(corrupt("property page exceeds pre-decode byte admission"));
                }
                if selected {
                    if header.type_ == parquet::format::PageType::DICTIONARY_PAGE {
                        dictionary_exposure = dictionary_exposure.max(uncompressed_page);
                        compressed_dictionary = compressed_dictionary.max(compressed_page);
                    } else {
                        data_exposure = data_exposure.max(uncompressed_page);
                        compressed_data = compressed_data.max(compressed_page);
                    }
                }
                position = position
                    .checked_add(header_bytes)
                    .and_then(|offset| offset.checked_add(compressed_page))
                    .ok_or_else(|| corrupt("property page range overflows"))?;
                if position > end {
                    return Err(corrupt("property page escapes its column chunk"));
                }
            }
            if position != end {
                return Err(corrupt(
                    "property page sequence does not cover its column chunk",
                ));
            }
            if selected {
                if include_codec {
                    let native = match column.compression() {
                        parquet::basic::Compression::UNCOMPRESSED => 0,
                        parquet::basic::Compression::ZSTD(_) => {
                            crate::permanent_parquet::ZSTD_DECODER_WORKSPACE as u64
                        }
                        // Ordinary property callers use only decoded_bytes. Replay admission
                        // refuses a codec without a justified native-memory bound.
                        _ => u64::MAX,
                    };
                    // A returned Arrow batch may span multiple pages. Fixed-width
                    // leaves have a value-count bound; variable and repeated leaves
                    // conservatively reserve the entire contributing row group.
                    let descriptor = column.column_descr();
                    let values = u64::try_from(column.num_values())
                        .map_err(|_| corrupt("negative Parquet value count"))?;
                    let values = if descriptor.max_rep_level() == 0 {
                        values.min(batch_rows as u64)
                    } else {
                        values
                    };
                    let width = match descriptor.physical_type() {
                        parquet::basic::Type::BOOLEAN => Some(1_u64),
                        parquet::basic::Type::INT32 | parquet::basic::Type::FLOAT => Some(4),
                        parquet::basic::Type::INT64 | parquet::basic::Type::DOUBLE => Some(8),
                        parquet::basic::Type::INT96 => Some(12),
                        parquet::basic::Type::FIXED_LEN_BYTE_ARRAY => Some(
                            u64::try_from(descriptor.type_length())
                                .map_err(|_| corrupt("negative fixed binary width"))?,
                        ),
                        parquet::basic::Type::BYTE_ARRAY => None,
                    };
                    let decoded = width
                        .map_or_else(
                            || {
                                uncompressed
                                    .saturating_add(dictionary_exposure.saturating_mul(values))
                            },
                            |width| values.saturating_mul(width),
                        )
                        .saturating_add(
                            values.saturating_mul(16).saturating_mul(
                                1 + u64::try_from(descriptor.max_def_level())
                                    .map_err(|_| corrupt("negative definition level"))?
                                    + u64::try_from(descriptor.max_rep_level())
                                        .map_err(|_| corrupt("negative repetition level"))?,
                            ),
                        );
                    group_value_bytes = group_value_bytes.saturating_add(decoded);
                    let memory = data_exposure
                        .saturating_mul(2)
                        .saturating_add(dictionary_exposure.saturating_mul(2))
                        .saturating_add(compressed_data)
                        .saturating_add(compressed_dictionary)
                        .saturating_add(native)
                        .saturating_add(8 * 1024);
                    let column_max = column_memory.get_mut(column_index).ok_or_else(|| {
                        corrupt("Parquet row-group column count disagrees with schema")
                    })?;
                    *column_max = (*column_max).max(memory);
                }
                group_exposure = group_exposure
                    .checked_add(data_exposure)
                    .and_then(|bytes| {
                        dictionary_exposure
                            .checked_mul(u64::try_from(batch_rows).ok()?)
                            .and_then(|decoded_dictionary| bytes.checked_add(decoded_dictionary))
                    })
                    .and_then(|bytes| {
                        // Validity, offsets, and values buffers are live together.
                        // Sixteen bytes/value/column deliberately over-reserves the
                        // fixed Arrow bookkeeping before the builder allocates it.
                        u64::try_from(batch_rows)
                            .ok()?
                            .checked_mul(16)
                            .and_then(|overhead| bytes.checked_add(overhead))
                    })
                    .ok_or_else(|| corrupt("property projected page exposure overflows"))?;
            }
            if !include_codec && group_exposure > max_page_bytes {
                return Err(limit_error(
                    "property projected pages exceed pre-decode live-byte admission",
                ));
            }
        }
        largest_group_exposure = largest_group_exposure.max(group_exposure);
        if include_codec {
            group_values.push((
                u64::try_from(group.num_rows()).map_err(|_| corrupt("negative row-group rows"))?,
                group_value_bytes,
            ));
        }
    }
    Ok(ParquetDecoderMemory {
        decoded_bytes: largest_group_exposure,
        // The raw-header audit ends before decoding. Its bounded temporary
        // memory does not overlap decoder states or returned Arrow values.
        with_codec_bytes: column_memory
            .into_iter()
            .fold(0, u64::saturating_add)
            .saturating_add(contributing_row_group_bytes(&group_values, batch_rows))
            .max(64 * 1024),
    })
}

/// A batch can start at the final row of any group, then consume up to B-1
/// rows from following groups. Reserve every touched group's value envelope.
/// The sliding window is linear in the authenticated footer's group count.
fn contributing_row_group_bytes(groups: &[(u64, u64)], batch_rows: usize) -> u64 {
    let mut maximum = 0_u64;
    let mut end = 0;
    let mut rows = 0_u64;
    let mut bytes = 0_u64;
    for start in 0..groups.len() {
        if end == start {
            rows = groups[start].0;
            bytes = groups[start].1;
            end += 1;
        }
        while end < groups.len()
            && rows.saturating_sub(groups[start].0) < batch_rows.saturating_sub(1) as u64
        {
            rows = rows.saturating_add(groups[end].0);
            bytes = bytes.saturating_add(groups[end].1);
            end += 1;
        }
        maximum = maximum.max(bytes);
        // Overflow is a resource rejection, never a wrapped smaller bound.
        if rows == u64::MAX || bytes == u64::MAX {
            return u64::MAX;
        }
        rows -= groups[start].0;
        bytes -= groups[start].1;
    }
    maximum
}

pub(super) fn admitted_batch_rows(limits: PropertyOverlayLimits) -> usize {
    let row_budget = limits.max_buffered_bytes / 4;
    let byte_limited = usize::try_from(row_budget / limits.max_row_bytes)
        .unwrap_or(usize::MAX)
        .max(1);
    limits.max_buffered_rows.clamp(1, 4096).min(byte_limited)
}

#[cfg(test)]
mod tests;
