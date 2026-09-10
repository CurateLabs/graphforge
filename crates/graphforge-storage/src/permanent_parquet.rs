//! Encoding policy for Parquet that becomes a published project payload.
//!
//! Lifecycle owners keep their durability, streaming, row-group and dictionary
//! choices. Private construction streams and external query exports are not
//! permanent project payloads. Resource envelopes here target the pinned
//! Parquet 58 / Zstd 1.5.7 implementation, not process RSS or allocator overhead.

use arrow::datatypes::Schema;
use graphforge_core::{GfError, ProjectErrorCode};
use parquet::{
    arrow::ArrowSchemaConverter,
    basic::{Compression, ZstdLevel},
    file::properties::{
        EnabledStatistics, WriterProperties, WriterPropertiesBuilder, WriterVersion,
    },
};

pub(crate) const PAGE_BYTES: usize = 1024 * 1024;
pub(crate) const PAGE_ROWS: usize = 20_000;

/// Shared permanent-payload encoding defaults.
///
/// Callers retain measured path-specific dictionary and row-group settings,
/// and domain metadata such as restoration's `created_by` marker. The builder
/// does not own files, publication leases, recovery or cancellation.
#[must_use]
pub fn writer_properties() -> WriterPropertiesBuilder {
    WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_1_0)
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(1).expect("Zstd level 1 is valid"),
        ))
        .set_dictionary_enabled(true)
        .set_max_row_group_row_count(Some(1_048_576))
        .set_data_page_size_limit(PAGE_BYTES)
        .set_dictionary_page_size_limit(PAGE_BYTES)
        .set_data_page_row_count_limit(PAGE_ROWS)
        .set_write_batch_size(1024)
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_write_page_header_statistics(false)
        .set_column_index_truncate_length(Some(64))
        .set_statistics_truncate_length(Some(64))
        .set_offset_index_disabled(false)
        .set_coerce_types(false)
}

fn overflow() -> GfError {
    GfError::Project {
        code: ProjectErrorCode::ResourceLimit,
        message: "permanent Parquet memory reservation overflow".into(),
    }
}

pub(crate) fn physical_columns(schema: &Schema) -> Result<usize, GfError> {
    ArrowSchemaConverter::new()
        .convert(schema)
        .map(|schema| schema.num_columns())
        .map_err(|error| GfError::Storage(error.to_string()))
}

/// One encoder's native CCtx plus Parquet's dormant DCtx. Fast strategy, no
/// trained dictionary, no workers, and the maximum source within this row group.
/// Fixed space includes the pinned structs, block states, scratch, alignment
/// and default ASAN arena redzones. Runtime sizing regression pins this claim.
pub(crate) fn zstd_encoder_workspace(maximum_source: usize) -> usize {
    let block = maximum_source.min(128 * 1024);
    let hash = maximum_source.clamp(64, 16_384).next_power_of_two() * 8;
    128 * 1024 + hash + block + 11 * (block / 4)
}

/// Active DCtx plus the codec's dormant CCtx, independently of page buffers.
pub(crate) const ZSTD_DECODER_WORKSPACE: usize = 104 * 1024;
const ZSTD_DORMANT_CODEC_WORKSPACE: usize = 104 * 1024;

/// Conservative additional encoder reservation for dictionaries-off replay.
/// Includes every physical leaf's native state, encoded row-group chunks kept
/// until flush, page headers, and source/compressed temporary copies. Existing
/// row/snapshot and metadata reservations remain separate. `active_source_bytes`
/// is the aggregate charged payload of the active row group, not a per-row size.
pub(crate) fn replay_encoder_buffers(
    schema: &Schema,
    active_source_bytes: usize,
    active_rows: usize,
) -> Result<usize, GfError> {
    if active_rows == 0 {
        return Ok(0); // ArrowWriter creates column codecs lazily on first write.
    }
    let columns = physical_columns(schema)?;
    let levels = active_rows
        .checked_mul(columns)
        .and_then(|n| n.checked_mul(16))
        .ok_or_else(overflow)?;
    let source = active_source_bytes
        .checked_mul(3)
        .and_then(|n| n.checked_add(levels))
        .ok_or_else(overflow)?;
    let pages = columns
        .checked_mul(1 + active_rows / PAGE_ROWS)
        .and_then(|n| n.checked_add(source / PAGE_BYTES))
        .ok_or_else(overflow)?;
    // compressBound <= source + ceil(source/256) + 64 per page. Current
    // truncated page statistics keep each encoded page header below 512 bytes.
    let copies = source
        .checked_mul(4)
        .and_then(|n| n.checked_add(source.div_ceil(256).checked_mul(3)?))
        .and_then(|n| n.checked_add(pages.checked_mul(512 + 3 * 64)?))
        .ok_or_else(overflow)?;
    let parquet_schema = ArrowSchemaConverter::new()
        .convert(schema)
        .map_err(|error| GfError::Storage(error.to_string()))?;
    let mut maximum_leaf_source = 0;
    for leaf in parquet_schema.columns() {
        let width = match leaf.physical_type() {
            parquet::basic::Type::BOOLEAN => Some(1_usize),
            parquet::basic::Type::INT32 | parquet::basic::Type::FLOAT => Some(4),
            parquet::basic::Type::INT64 | parquet::basic::Type::DOUBLE => Some(8),
            parquet::basic::Type::INT96 => Some(12),
            parquet::basic::Type::FIXED_LEN_BYTE_ARRAY => usize::try_from(leaf.type_length()).ok(),
            parquet::basic::Type::BYTE_ARRAY => None,
        };
        let leaf_source = if leaf.max_rep_level() == 0 {
            width
                .map(|width| {
                    width
                        .checked_add(16)
                        .and_then(|bytes| bytes.checked_mul(active_rows))
                        .ok_or_else(overflow)
                })
                .transpose()?
                .unwrap_or(source)
                .min(source)
        } else {
            source
        };
        maximum_leaf_source = maximum_leaf_source.max(leaf_source);
    }
    let active_codec = zstd_encoder_workspace(maximum_leaf_source);
    let native = if source < PAGE_BYTES && active_rows < PAGE_ROWS {
        // No data page can flush during writes below both page thresholds.
        // Row-group close compresses and drops leaf writers sequentially, so
        // only one CCtx is active alongside the other dormant codec pairs.
        columns
            .checked_mul(ZSTD_DORMANT_CODEC_WORKSPACE)
            .and_then(|n| n.checked_add(active_codec.saturating_sub(ZSTD_DORMANT_CODEC_WORKSPACE)))
    } else {
        columns.checked_mul(active_codec)
    }
    .ok_or_else(overflow)?;
    native.checked_add(copies).ok_or_else(overflow)
}

/// Writer structures and both Arrow/Parquet schema ownership. Payload buffers,
/// native codecs and retained row-group metadata are charged separately.
pub(crate) fn replay_schema_bytes(schema: &Schema) -> Result<usize, GfError> {
    let parquet_schema = ArrowSchemaConverter::new()
        .convert(schema)
        .map_err(|error| GfError::Storage(error.to_string()))?;
    let parquet_metadata = parquet::file::metadata::ParquetMetaData::new(
        parquet::file::metadata::FileMetaData::new(
            1,
            0,
            None,
            None,
            std::sync::Arc::new(parquet_schema),
            None,
        ),
        Vec::new(),
    );
    let arrow_schema = schema
        .fields()
        .iter()
        .fold(0_usize, |bytes, field| bytes.saturating_add(field.size()))
        .saturating_add(
            schema
                .metadata()
                .iter()
                .fold(0_usize, |bytes, (key, value)| {
                    bytes
                        .saturating_add(64)
                        .saturating_add(key.capacity())
                        .saturating_add(value.capacity())
                }),
        );
    arrow_schema
        .checked_mul(4)
        .and_then(|bytes| bytes.checked_add(parquet_metadata.memory_size()))
        .ok_or_else(overflow)
}

pub(crate) fn replay_writer_structure_bytes(schema: &Schema) -> Result<usize, GfError> {
    // Pinned ArrowColumnWriter is 1464 bytes, including its 1416-byte column
    // writer. The per-leaf allowance also covers allocation containers and
    // one closing-column index conversion; variable schemas are separate.
    let schema_bytes = replay_schema_bytes(schema)?;
    physical_columns(schema)?
        .checked_mul(8 * 1024)
        .and_then(|bytes| bytes.checked_add(64 * 1024))
        .and_then(|bytes| bytes.checked_add(schema_bytes))
        .ok_or_else(overflow)
}

fn capacity_bound(len: usize, minimum: usize) -> usize {
    if len == 0 {
        0
    } else {
        len.saturating_mul(2).max(minimum)
    }
}

/// Retained chunk statistics, page indexes and container capacities for a
/// dictionaries-off replay file. Unlike ArrowWriter::memory_size(), this
/// includes already flushed row groups. All arithmetic fails closed.
pub(crate) fn replay_metadata_bytes(
    schema: &Schema,
    groups: usize,
    rows_per_group: usize,
    maximum_row_bytes: usize,
) -> Result<usize, GfError> {
    use parquet::file::{
        metadata::{ColumnChunkMetaData, RowGroupMetaData},
        page_index::{column_index::ColumnIndexMetaData, offset_index::OffsetIndexMetaData},
    };
    let parquet_schema = ArrowSchemaConverter::new()
        .convert(schema)
        .map_err(|error| GfError::Storage(error.to_string()))?;
    let mut per_group = 0_usize;
    for leaf in parquet_schema.columns() {
        let width = match leaf.physical_type() {
            parquet::basic::Type::BOOLEAN => Some(1_usize),
            parquet::basic::Type::INT32 | parquet::basic::Type::FLOAT => Some(4),
            parquet::basic::Type::INT64 | parquet::basic::Type::DOUBLE => Some(8),
            parquet::basic::Type::INT96 => Some(12),
            parquet::basic::Type::FIXED_LEN_BYTE_ARRAY => usize::try_from(leaf.type_length()).ok(),
            parquet::basic::Type::BYTE_ARRAY => None,
        };
        let levels = [leaf.max_def_level(), leaf.max_rep_level()]
            .into_iter()
            .filter(|level| *level > 0)
            .map(|level| usize::from(level.unsigned_abs()) + 1)
            .sum::<usize>();
        let source = if leaf.max_rep_level() == 0 {
            width
                .unwrap_or(maximum_row_bytes)
                .saturating_add(16)
                .saturating_mul(rows_per_group)
        } else {
            maximum_row_bytes
                .saturating_mul(3)
                .saturating_add(16)
                .saturating_mul(rows_per_group)
        };
        let pages = 1_usize
            .saturating_add(rows_per_group.saturating_sub(1) / PAGE_ROWS)
            .saturating_add(source / PAGE_BYTES);
        let binary = matches!(
            leaf.physical_type(),
            parquet::basic::Type::BYTE_ARRAY | parquet::basic::Type::FIXED_LEN_BYTE_ARRAY
        );
        // Truncating a maximum string can fail when its prefix cannot be
        // incremented. Reserve the original value bound, not an assumed 64B.
        let extrema = width.unwrap_or(maximum_row_bytes);
        let min_max = pages
            .saturating_mul(extrema)
            .saturating_mul(2)
            .saturating_add(if binary {
                pages
                    .saturating_add(1)
                    .saturating_mul(2 * size_of::<usize>())
            } else {
                0
            });
        let fixed = size_of::<ColumnChunkMetaData>()
            + size_of::<Option<ColumnIndexMetaData>>()
            + size_of::<Option<OffsetIndexMetaData>>()
            + size_of::<Option<parquet::bloom_filter::Sbbf>>();
        let statistics = if binary {
            extrema.saturating_mul(2).saturating_add(64)
        } else {
            0
        };
        let retained = fixed
            .saturating_add(statistics)
            .saturating_add(levels.saturating_mul(8))
            .saturating_add(capacity_bound(pages, 8)) // null flags
            .saturating_add(capacity_bound(pages, 4).saturating_mul(8)) // null counts
            .saturating_add(capacity_bound(pages.saturating_mul(levels), 4).saturating_mul(8))
            .saturating_add(min_max)
            .saturating_add(pages.saturating_mul(24)) // exact-sized final PageLocation vector
            .saturating_add(
                if leaf.physical_type() == parquet::basic::Type::BYTE_ARRAY {
                    capacity_bound(pages, 4).saturating_mul(8)
                } else {
                    0
                },
            );
        per_group = per_group.saturating_add(retained);
    }
    let outer = capacity_bound(groups, 4)
        .saturating_mul(size_of::<RowGroupMetaData>() + 3 * size_of::<Vec<usize>>());
    per_group
        .checked_mul(groups)
        .and_then(|bytes| bytes.checked_add(outer))
        .ok_or_else(overflow)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_writer_structural_memory_assessment() {
        use parquet::{
            arrow::arrow_writer::{ArrowColumnWriter, ArrowRowGroupWriterFactory},
            column::writer::ColumnWriter,
            file::{
                metadata::{ColumnChunkMetaData, RowGroupMetaData},
                page_index::{
                    column_index::ColumnIndexMetaData, offset_index::OffsetIndexMetaData,
                },
                writer::SerializedFileWriter,
            },
        };
        let measurements = serde_json::json!({
            "arrow_column_writer": size_of::<ArrowColumnWriter>(),
            "column_writer": size_of::<ColumnWriter<'static>>(),
            "row_group_factory": size_of::<ArrowRowGroupWriterFactory>(),
            "file_writer": size_of::<SerializedFileWriter<std::fs::File>>(),
            "column_metadata": size_of::<ColumnChunkMetaData>(),
            "row_group_metadata": size_of::<RowGroupMetaData>(),
            "column_index": size_of::<ColumnIndexMetaData>(),
            "offset_index": size_of::<OffsetIndexMetaData>(),
            "properties": size_of::<WriterProperties>(),
        });
        println!("REPLAY_WRITER_STRUCTURES {measurements}");
        assert!(size_of::<ArrowColumnWriter>() + size_of::<ColumnWriter<'static>>() < 8 * 1024);
    }
}
