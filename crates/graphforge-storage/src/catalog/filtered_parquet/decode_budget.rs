//! Metadata-derived admission before edge Parquet decoding allocates pages.
//!
//! The estimate covers the complete selected row groups, including the rows a
//! predicate later discards. It deliberately does not equate returned Arrow
//! bytes with peak decoder memory. Readers hold compressed pages, decompressed
//! pages, dictionaries, decoder scratch and Arrow output concurrently.

use std::collections::HashSet;
use std::path::Path;

use arrow::datatypes::DataType;
use datafusion::error::DataFusionError;
use graphforge_core::OntologyMode;
use parquet::basic::{Compression, Type};
use parquet::file::metadata::{ColumnChunkMetaData, ParquetStatisticsPolicy};
use parquet::file::statistics::Statistics;

use crate::AuthenticatedPropertyInventory;

/// Conservative transient admission for an admitted edge-record read.
///
/// Reserve active decoder scratch plus all retained output before reading.
/// Files and row groups decode sequentially; accumulated Arrow batches remain
/// live. Each column can advance groups independently, so scratch uses the sum
/// of column maxima. The envelope also covers a file's concat/filter copies.
/// Flat fixed arrays retain the reader's complete batch capacity, including
/// the final partial batch. Variable payloads retain conservative size bounds.
/// Missing or invalid metadata fails typed before decode.
///
/// # Errors
/// Returns storage errors or a named resource exhaustion when sizes cannot be
/// represented. This is a decoder admission bound, not a bound on process RSS,
/// allocator retention, catalog inventory, or later operator allocations.
#[allow(clippy::implicit_hasher)]
pub fn edge_decode_admission_bytes(
    inventory: &AuthenticatedPropertyInventory,
    rel_name: &str,
    mode: OntologyMode,
    edge_ids: &HashSet<u64>,
    projection: &[usize],
) -> Result<usize, DataFusionError> {
    if edge_ids.is_empty() {
        return Ok(0);
    }
    let union = rel_name == "*" && matches!(mode, OntologyMode::Advisory | OntologyMode::Strict);
    let route = match mode {
        OntologyMode::Exploratory => "_exploratory",
        OntologyMode::Advisory | OntologyMode::Strict => rel_name,
    };
    let mut retained = 0usize;
    let mut active = 0usize;
    for (name, path) in inventory.edge_files(if union { None } else { Some(route) }) {
        let file = file_bytes(
            &path,
            edge_ids,
            if union { None } else { Some(projection) },
            union.then_some(name.as_str()),
        )?;
        retained = add(retained, file.retained)?;
        active = active.max(file.scratch.max(mul(file.retained, 2)?));
    }
    add(retained, active)
}

#[derive(Debug)]
struct DecodeBytes {
    retained: usize,
    scratch: usize,
}

fn add(a: usize, b: usize) -> Result<usize, DataFusionError> {
    a.checked_add(b).ok_or_else(exhausted)
}

fn mul(a: usize, b: usize) -> Result<usize, DataFusionError> {
    a.checked_mul(b).ok_or_else(exhausted)
}

fn exhausted() -> DataFusionError {
    DataFusionError::ResourcesExhausted(
        "ExpandExec edge Parquet decode: metadata cannot bound decoder allocation".into(),
    )
}

fn nonnegative(value: i64) -> Result<usize, DataFusionError> {
    usize::try_from(value).map_err(|_| exhausted())
}

fn file_bytes(
    path: &Path,
    ids: &HashSet<u64>,
    projection: Option<&[usize]>,
    union_name: Option<&str>,
) -> Result<DecodeBytes, DataFusionError> {
    use parquet::arrow::arrow_reader::ArrowReaderOptions;
    let options =
        ArrowReaderOptions::new().with_size_stats_policy(ParquetStatisticsPolicy::KeepAll);
    // The common reader preflights the encoded footer's 16 MiB ceiling before
    // decoding metadata, then validates row-group and column byte ceilings.
    let reader = crate::catalog::admitted_parquet_with_options(path, options)?;
    let key = reader
        .schema()
        .index_of("edge_id")
        .map_err(|_| exhausted())?;
    let mut columns = projection.map_or_else(
        || (0..reader.schema().fields().len()).collect::<Vec<_>>(),
        <[usize]>::to_vec,
    );
    columns.push(key);
    columns.sort_unstable();
    columns.dedup();
    if columns
        .iter()
        .any(|&index| index >= reader.schema().fields().len())
    {
        return Err(DataFusionError::Execution(
            "edge decode projection index is out of range".into(),
        ));
    }
    // Canonical edge topology consists of one flat primitive leaf per field.
    // Refuse a noncanonical nested field rather than underestimate its buffers.
    if reader.parquet_schema().num_columns() != reader.schema().fields().len() {
        return Err(exhausted());
    }
    let file_rows = nonnegative(reader.metadata().file_metadata().num_rows())?;
    let batch_size = file_rows.clamp(1, 1024);
    let full_read = ids.len().saturating_mul(2) > file_rows;
    let mut rows = 0usize;
    let mut scratch = vec![0; columns.len()];
    let mut payloads = vec![0; columns.len()];
    for group in reader.metadata().row_groups() {
        if !full_read && !group_may_be_read(group.column(key).statistics(), ids) {
            continue;
        }
        rows = add(rows, nonnegative(group.num_rows())?)?;
        for (slot, &column) in columns.iter().enumerate() {
            let metadata = group.column(column);
            if metadata.column_descr().max_rep_level() != 0 {
                return Err(exhausted());
            }
            scratch[slot] = scratch[slot].max(column_bytes(metadata, batch_size)?);
            payloads[slot] = add(payloads[slot], variable_payload(metadata)?)?;
        }
    }
    let batches = rows.div_ceil(batch_size);
    let slots = mul(batches, batch_size)?;
    let mut retained = 0;
    for (slot, &column) in columns.iter().enumerate() {
        retained = add(
            retained,
            retained_column(
                reader.schema().field(column).data_type(),
                slots,
                batches,
                payloads[slot],
            )?,
        )?;
    }
    if let Some(name) = union_name
        && reader.schema().index_of("rel_type_name").is_err()
    {
        retained = add(
            retained,
            retained_column(&DataType::Utf8, slots, batches, mul(rows, name.len())?)?,
        )?;
    }
    Ok(DecodeBytes {
        retained,
        scratch: scratch.into_iter().try_fold(0, add)?,
    })
}

fn group_may_be_read(statistics: Option<&Statistics>, ids: &HashSet<u64>) -> bool {
    if let Some(Statistics::Int64(statistics)) = statistics
        && let (Some(&low), Some(&high)) = (statistics.min_opt(), statistics.max_opt())
    {
        // Match the conservative reader exactly. Negative INT64 metadata does
        // not prove an unsigned range, so it must not prune decoder admission.
        let low = u64::try_from(low).unwrap_or(0);
        let high = u64::try_from(high).unwrap_or(u64::MAX);
        return ids.iter().any(|id| *id >= low && *id <= high);
    }
    true
}

fn variable_payload(column: &ColumnChunkMetaData) -> Result<usize, DataFusionError> {
    if column.column_type() != Type::BYTE_ARRAY {
        return Ok(0);
    }
    match column.unencoded_byte_array_data_bytes() {
        Some(bytes) => nonnegative(bytes),
        None => mul(
            nonnegative(column.num_values())?,
            nonnegative(column.uncompressed_size())?,
        ),
    }
}

fn retained_column(
    data_type: &DataType,
    slots: usize,
    batches: usize,
    payload: usize,
) -> Result<usize, DataFusionError> {
    // Canonical topology has flat scalar fields. Refuse nested or dictionary
    // Arrow metadata rather than treating its children as fixed scalar slots.
    let values = match data_type {
        DataType::Boolean => slots.div_ceil(8),
        DataType::Int8 | DataType::UInt8 => slots,
        DataType::Int16 | DataType::UInt16 | DataType::Float16 => mul(slots, 2)?,
        DataType::Int32 | DataType::UInt32 | DataType::Float32 | DataType::Date32 => mul(slots, 4)?,
        DataType::Int64
        | DataType::UInt64
        | DataType::Float64
        | DataType::Date64
        | DataType::Timestamp(_, _)
        | DataType::Time64(_)
        | DataType::Duration(_) => mul(slots, 8)?,
        DataType::FixedSizeBinary(width) => mul(slots, nonnegative(i64::from(*width))?)?,
        DataType::Utf8 | DataType::Binary => add(mul(payload, 2)?, mul(add(slots, batches)?, 4)?)?,
        DataType::LargeUtf8 | DataType::LargeBinary => {
            add(mul(payload, 2)?, mul(add(slots, batches)?, 8)?)?
        }
        _ => return Err(exhausted()),
    };
    // Each batch/array owns headers and aligned validity buffers even when a
    // final partial batch has very few live rows. Capacity, not length, counts.
    add(values, add(slots.div_ceil(8), mul(batches, 512)?)?)
}

fn column_bytes(column: &ColumnChunkMetaData, batch_size: usize) -> Result<usize, DataFusionError> {
    let values = nonnegative(column.num_values())?;
    let compressed = nonnegative(column.compressed_size())?;
    let uncompressed = nonnegative(column.uncompressed_size())?;
    let codec = match column.compression() {
        Compression::UNCOMPRESSED => 0,
        // Creating the next page reader precedes dropping the previous reader.
        Compression::ZSTD(_) => 2 * crate::permanent_parquet::ZSTD_DECODER_WORKSPACE,
        _ => return Err(exhausted()),
    };
    let active_values = batch_size;
    let value_bytes = match column.column_type() {
        Type::BOOLEAN => active_values,
        Type::INT32 | Type::FLOAT => mul(active_values, 4)?,
        Type::INT64 | Type::DOUBLE => mul(active_values, 8)?,
        Type::INT96 => mul(active_values, 16)?,
        Type::FIXED_LEN_BYTE_ARRAY => mul(
            active_values,
            nonnegative(i64::from(column.column_descr().type_length()))?,
        )?,
        Type::BYTE_ARRAY => add(variable_payload(column)?, mul(values, 32)?)?,
    };
    // Keep conservative complete chunk/page/dictionary and level allowances.
    // Only fixed output construction uses the bounded Selectors batch window.
    add(
        mul(
            add(
                add(compressed, uncompressed)?,
                add(value_bytes, mul(values, 16)?)?,
            )?,
            4,
        )?,
        add(64 * 1024, codec)?,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::file::metadata::ColumnChunkMetaData;
    use parquet::schema::types::{ColumnDescriptor, ColumnPath, Type as SchemaType};
    use std::sync::Arc;

    fn dictionary_column(unencoded: Option<i64>) -> ColumnChunkMetaData {
        let field = SchemaType::primitive_type_builder("value", Type::BYTE_ARRAY)
            .build()
            .unwrap();
        let descriptor = Arc::new(ColumnDescriptor::new(
            Arc::new(field),
            0,
            0,
            ColumnPath::from("value"),
        ));
        ColumnChunkMetaData::builder(descriptor)
            .set_num_values(10_000)
            .set_total_compressed_size(2048)
            .set_total_uncompressed_size(4096)
            .set_unencoded_byte_array_data_bytes(unencoded)
            .build()
            .unwrap()
    }

    #[test]
    fn dictionary_repetition_is_bounded_by_unencoded_values_not_encoded_pages() {
        // One 1 KiB dictionary value repeated 10,000 times: encoded pages
        // occupy a few KiB but the Arrow payload occupies about 10 MiB.
        let payload = 1024 * 10_000;
        let bytes = column_bytes(&dictionary_column(Some(payload)), 1024).unwrap();
        assert!(bytes > payload as usize * 4);
        let old_footer = column_bytes(&dictionary_column(None), 1024).unwrap();
        assert!(
            old_footer > bytes,
            "missing size statistics must remain conservative"
        );
    }

    #[test]
    fn invalid_decode_sizes_fail_typed() {
        let error = column_bytes(&dictionary_column(Some(-1)), 1024).unwrap_err();
        assert!(matches!(error, DataFusionError::ResourcesExhausted(_)));
        assert!(error.to_string().contains("edge Parquet decode"));
    }
    fn fixed_file(path: &Path, rows: usize, row_group: usize) {
        use arrow::array::{FixedSizeBinaryArray, RecordBatch, UInt64Array};
        use arrow::datatypes::{Field, Schema};
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        let schema = Arc::new(Schema::new(vec![
            Field::new("edge_id", DataType::UInt64, false),
            Field::new("edge_uuid", DataType::FixedSizeBinary(16), false),
        ]));
        let ids = UInt64Array::from_iter_values(0..rows as u64);
        let uuid = [0u8; 16];
        let uuids =
            FixedSizeBinaryArray::try_from_iter((0..rows).map(|_| uuid.as_slice())).unwrap();
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(uuids)]).unwrap();
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(row_group))
            .build();
        let mut writer = ArrowWriter::try_new(
            std::fs::File::create(path).unwrap(),
            schema,
            Some(properties),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[test]
    fn fixed_output_accounts_for_the_full_capacity_of_a_partial_batch() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("edge.parquet");
        fixed_file(&path, 1025, 1025);
        let ids = (0..1025).collect();
        let bound = file_bytes(&path, &ids, None, None).unwrap();
        let reader = crate::catalog::admitted_parquet(&path)
            .unwrap()
            .build()
            .unwrap();
        let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(
            batches
                .iter()
                .map(|batch| batch.num_rows())
                .collect::<Vec<_>>(),
            [1024, 1]
        );
        let actual: usize = batches
            .iter()
            .map(arrow::array::RecordBatch::get_array_memory_size)
            .sum();
        assert!(
            actual > 1025 * 24,
            "partial capacity must be observed: {actual}"
        );
        assert!(bound.retained >= actual, "{} < {actual}", bound.retained);
    }

    #[test]
    fn sequential_row_groups_retain_output_but_do_not_sum_active_decoders() {
        let dir = tempfile::TempDir::new().unwrap();
        let one = dir.path().join("one.parquet");
        let many = dir.path().join("many.parquet");
        fixed_file(&one, 1024, 1024);
        fixed_file(&many, 20 * 1024, 1024);
        let ids = (0..20 * 1024).collect();
        let one = file_bytes(&one, &ids, None, None).unwrap();
        let many = file_bytes(&many, &ids, None, None).unwrap();
        assert!(many.retained >= 20 * one.retained);
        assert!(
            many.scratch < 2 * one.scratch,
            "{:?} versus {:?}",
            many,
            one
        );
    }

    #[test]
    fn noncanonical_nested_arrow_output_is_refused_before_decode() {
        let field = Arc::new(arrow::datatypes::Field::new(
            "item",
            DataType::UInt64,
            false,
        ));
        assert!(matches!(
            retained_column(&DataType::List(field), 1024, 1, 0),
            Err(DataFusionError::ResourcesExhausted(_))
        ));
    }
    #[test]
    fn ambiguous_unsigned_statistics_keep_the_conservative_reader_groups() {
        let ids = HashSet::from([1]);
        let statistics = Statistics::int64(Some(-10), Some(-1), None, Some(0), false);
        assert!(group_may_be_read(Some(&statistics), &ids));
        let statistics = Statistics::int64(Some(10), Some(20), None, Some(0), false);
        assert!(!group_may_be_read(Some(&statistics), &ids));
    }

    #[test]
    fn codecs_without_an_admitted_workspace_fail_before_decode() {
        let column = dictionary_column(Some(1024));
        let column = ColumnChunkMetaData::builder(column.column_descr_ptr())
            .set_num_values(10)
            .set_total_compressed_size(1024)
            .set_total_uncompressed_size(2048)
            .set_compression(Compression::BROTLI(Default::default()))
            .build()
            .unwrap();
        assert!(matches!(
            column_bytes(&column, 1024),
            Err(DataFusionError::ResourcesExhausted(_))
        ));
    }
}
