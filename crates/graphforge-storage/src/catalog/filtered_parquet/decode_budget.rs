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
use parquet::basic::{Compression, Encoding, Type};
use parquet::file::metadata::{ColumnChunkMetaData, ParquetStatisticsPolicy};
use parquet::file::statistics::Statistics;

use crate::AuthenticatedPropertyInventory;

pub(super) mod nodes;
pub use nodes::node_decode_admission_bytes;

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
pub(super) struct DecodeBytes {
    pub(super) retained: usize,
    pub(super) scratch: usize,
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
    metadata_bytes(
        reader.metadata(),
        reader.schema(),
        ids,
        projection,
        union_name,
    )
}

pub(super) fn metadata_bytes(
    metadata: &parquet::file::metadata::ParquetMetaData,
    schema: &arrow::datatypes::SchemaRef,
    ids: &HashSet<u64>,
    projection: Option<&[usize]>,
    union_name: Option<&str>,
) -> Result<DecodeBytes, DataFusionError> {
    let key = schema.index_of("edge_id").map_err(|_| exhausted())?;
    let mut columns = projection.map_or_else(
        || (0..schema.fields().len()).collect::<Vec<_>>(),
        <[usize]>::to_vec,
    );
    columns.push(key);
    columns.sort_unstable();
    columns.dedup();
    if columns.iter().any(|&index| index >= schema.fields().len()) {
        return Err(DataFusionError::Execution(
            "edge decode projection index is out of range".into(),
        ));
    }
    // Canonical edge topology consists of one flat primitive leaf per field.
    // Refuse a noncanonical nested field rather than underestimate its buffers.
    if metadata.file_metadata().schema_descr().num_columns() != schema.fields().len() {
        return Err(exhausted());
    }
    let file_rows = nonnegative(metadata.file_metadata().num_rows())?;
    let batch_size = file_rows.clamp(1, 1024);
    let full_read = ids.len().saturating_mul(2) > file_rows;
    let mut rows = 0usize;
    let mut scratch = vec![0; columns.len()];
    let mut payloads = vec![0; columns.len()];
    for group in metadata.row_groups() {
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
                schema.field(column).data_type(),
                slots,
                batches,
                payloads[slot],
            )?,
        )?;
    }
    if let Some(name) = union_name
        && schema.index_of("rel_type_name").is_err()
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

fn construction_bytes(
    column: &ColumnChunkMetaData,
    values: usize,
    batch_size: usize,
) -> Result<usize, DataFusionError> {
    let physical = column.column_type();
    let descriptor = column.column_descr();
    let whole = || mul(values, 16);
    if physical == Type::BYTE_ARRAY
        || descriptor.max_rep_level() != 0
        || !matches!(descriptor.max_def_level(), 0 | 1)
    {
        return whole();
    }
    let mut has_data_encoding = false;
    for encoding in column.encodings() {
        has_data_encoding |= match encoding {
            Encoding::PLAIN | Encoding::PLAIN_DICTIONARY | Encoding::RLE_DICTIONARY => true,
            Encoding::RLE => physical == Type::BOOLEAN,
            Encoding::DELTA_BINARY_PACKED if matches!(physical, Type::INT32 | Type::INT64) => true,
            Encoding::BYTE_STREAM_SPLIT
                if matches!(
                    physical,
                    Type::INT32
                        | Type::INT64
                        | Type::FLOAT
                        | Type::DOUBLE
                        | Type::FIXED_LEN_BYTE_ARRAY
                ) =>
            {
                true
            }
            // Arrow's fixed DELTA_BYTE_ARRAY decoder eagerly allocates both
            // full-page prefix/suffix Vec<i32>s, independently of batch size.
            // Empty or unsupported encoding metadata keeps the same envelope.
            _ => return whole(),
        };
    }
    if !has_data_encoding {
        return whole();
    }
    // parquet 58.4 RecordReader creates no levels for required flat columns;
    // nullable flat levels and fixed output advance within the reader batch.
    // Complete pages/dictionaries and retained output are charged separately.
    if descriptor.max_def_level() == 0 {
        Ok(0)
    } else {
        mul(batch_size, 16)
    }
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
    // Keep complete chunk/page/dictionary and codec allowances. Construction
    // uses a batch bound only for positively identified flat decoders.
    add(
        mul(
            add(
                add(compressed, uncompressed)?,
                add(value_bytes, construction_bytes(column, values, batch_size)?)?,
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

    fn fixed_column(
        physical: Type,
        definition_level: i16,
        encodings: Vec<Encoding>,
        values: i64,
    ) -> ColumnChunkMetaData {
        let mut field = SchemaType::primitive_type_builder("value", physical);
        if physical == Type::FIXED_LEN_BYTE_ARRAY {
            field = field.with_length(16);
        }
        let descriptor = Arc::new(ColumnDescriptor::new(
            Arc::new(field.build().unwrap()),
            definition_level,
            0,
            ColumnPath::from("value"),
        ));
        ColumnChunkMetaData::builder(descriptor)
            .set_num_values(values)
            .set_total_compressed_size(2048)
            .set_total_uncompressed_size(4096)
            .set_encodings(encodings)
            .build()
            .unwrap()
    }

    #[test]
    fn flat_primitive_construction_uses_the_batch_not_the_row_group() {
        for physical in [Type::INT64, Type::FIXED_LEN_BYTE_ARRAY] {
            let encodings = vec![Encoding::PLAIN, Encoding::RLE, Encoding::RLE_DICTIONARY];
            let required = fixed_column(physical, 0, encodings.clone(), 1_000_000);
            let nullable = fixed_column(physical, 1, encodings.clone(), 1_000_000);
            let small = fixed_column(physical, 1, encodings, 1024);
            let required = column_bytes(&required, 1024).unwrap();
            let nullable = column_bytes(&nullable, 1024).unwrap();
            assert!(required < 1024 * 1024, "required: {required}");
            assert_eq!(nullable, column_bytes(&small, 1024).unwrap());
            assert_eq!(nullable - required, 4 * 16 * 1024);
        }
    }

    #[test]
    fn whole_page_and_ambiguous_construction_remain_conservative() {
        let values = 1_000_000;
        for (physical, level, encodings) in [
            (
                Type::FIXED_LEN_BYTE_ARRAY,
                0,
                vec![Encoding::DELTA_BYTE_ARRAY],
            ),
            (Type::INT64, 0, vec![]),
            (Type::INT64, 0, vec![Encoding::RLE]),
            (Type::INT64, 0, vec![Encoding::DELTA_BYTE_ARRAY]),
            (Type::INT64, 2, vec![Encoding::PLAIN]),
        ] {
            let column = fixed_column(physical, level, encodings, values);
            assert!(column_bytes(&column, 1024).unwrap() >= values as usize * 16 * 4);
        }
        let huge = fixed_column(Type::FIXED_LEN_BYTE_ARRAY, 0, vec![], i64::MAX);
        assert!(matches!(
            column_bytes(&huge, 1024),
            Err(DataFusionError::ResourcesExhausted(_))
        ));
    }

    fn sparse_compressed_read_fits(nullable: bool, encoding: Encoding) {
        use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, RecordBatch, UInt64Array};
        use arrow::datatypes::{Field, Schema};
        use parquet::arrow::ArrowWriter;
        use parquet::column::page::Page;
        use parquet::file::properties::{WriterProperties, WriterVersion};
        use parquet::file::reader::FileReader;
        use parquet::file::serialized_reader::SerializedFileReader;

        const ROWS: usize = 240_001;
        const BUDGET: usize = 16 * 1024 * 1024;
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("edge.parquet");
        let boolean = encoding == Encoding::RLE;
        let value_type = if boolean {
            DataType::Boolean
        } else {
            DataType::Int64
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("edge_id", DataType::UInt64, false),
            Field::new("value", value_type, nullable),
        ]));
        let expected_value = |row: usize| {
            (!nullable || row % 3 != 0).then_some(if boolean {
                i64::from(row % 2 == 0)
            } else {
                row as i64
            })
        };
        let values: ArrayRef = if boolean {
            Arc::new(BooleanArray::from_iter(
                (0..ROWS).map(|row| expected_value(row).map(|value| value != 0)),
            ))
        } else {
            Arc::new(Int64Array::from_iter((0..ROWS).map(expected_value)))
        };
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from_iter_values(0..ROWS as u64)),
                values,
            ],
        )
        .unwrap();
        let properties = WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_dictionary_enabled(false)
            .set_encoding(Encoding::DELTA_BINARY_PACKED)
            .set_column_encoding(ColumnPath::from("value"), encoding)
            .set_compression(Compression::ZSTD(Default::default()))
            .set_max_row_group_row_count(Some(ROWS))
            .build();
        let mut writer = ArrowWriter::try_new(
            std::fs::File::create(&path).unwrap(),
            schema.clone(),
            Some(properties),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let reader = crate::catalog::admitted_parquet(&path).unwrap();
        assert_eq!(reader.metadata().num_row_groups(), 1);
        for (column, expected_encoding) in reader
            .metadata()
            .row_group(0)
            .columns()
            .iter()
            .zip([Encoding::DELTA_BINARY_PACKED, encoding])
        {
            assert!(
                column
                    .encodings()
                    .any(|encoding| encoding == expected_encoding)
            );
            assert!(column.compressed_size() < ROWS as i64);
        }
        // Inspect actual data pages: RLE in the footer can describe nullable
        // definition levels even when the Boolean values use another encoding.
        let file = SerializedFileReader::new(std::fs::File::open(&path).unwrap()).unwrap();
        let group = file.get_row_group(0).unwrap();
        let mut pages = group.get_column_page_reader(1).unwrap();
        let mut data_pages = 0;
        while let Some(page) = pages.get_next_page().unwrap() {
            match page {
                Page::DataPage {
                    encoding: actual, ..
                }
                | Page::DataPageV2 {
                    encoding: actual, ..
                } => {
                    assert_eq!(actual, encoding);
                    data_pages += 1;
                }
                Page::DictionaryPage { .. } => panic!("fixture disables dictionary encoding"),
            }
        }
        assert!(data_pages > 0, "fixture must contain encoded values");
        let selected = [0, 1024, 1025, ROWS - 1];
        let ids = selected.map(|row| row as u64).into_iter().collect();
        let bound = file_bytes(&path, &ids, None, None).unwrap();
        let total = add(
            bound.retained,
            bound.scratch.max(mul(bound.retained, 2).unwrap()),
        )
        .unwrap();
        assert!(
            total <= BUDGET,
            "sparse read must fit {BUDGET}: {bound:?}, total {total}"
        );
        let request = super::super::EdgeIdRequest::new(&ids);
        let batches =
            super::super::read_required_edge_filtered(&path, schema, &request, None, None).unwrap();
        let mut actual = Vec::new();
        for batch in batches {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                let value = if boolean {
                    let values = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .unwrap();
                    (!values.is_null(row)).then(|| i64::from(values.value(row)))
                } else {
                    let values = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    (!values.is_null(row)).then(|| values.value(row))
                };
                actual.push((ids.value(row), value));
            }
        }
        assert_eq!(
            actual,
            selected.map(|row| (row as u64, expected_value(row)))
        );
    }

    #[test]
    fn large_required_compressed_row_group_fits_a_sparse_read() {
        sparse_compressed_read_fits(false, Encoding::DELTA_BINARY_PACKED);
    }

    #[test]
    fn large_nullable_compressed_row_group_fits_a_sparse_read() {
        sparse_compressed_read_fits(true, Encoding::DELTA_BINARY_PACKED);
    }

    #[test]
    fn required_byte_stream_split_values_fit_a_sparse_read() {
        sparse_compressed_read_fits(false, Encoding::BYTE_STREAM_SPLIT);
    }

    #[test]
    fn nullable_byte_stream_split_values_fit_a_sparse_read() {
        sparse_compressed_read_fits(true, Encoding::BYTE_STREAM_SPLIT);
    }

    #[test]
    fn required_boolean_rle_values_fit_a_sparse_read() {
        sparse_compressed_read_fits(false, Encoding::RLE);
    }

    #[test]
    fn nullable_boolean_rle_values_fit_a_sparse_read() {
        sparse_compressed_read_fits(true, Encoding::RLE);
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
