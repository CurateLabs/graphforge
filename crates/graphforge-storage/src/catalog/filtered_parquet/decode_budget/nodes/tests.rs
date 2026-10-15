use super::*;
use arrow::array::{
    Array, FixedSizeBinaryArray, ListArray, RecordBatch, TimestampMicrosecondArray, UInt32Array,
    UInt64Array,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{Field, Schema};
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};
use parquet::arrow::ArrowWriter;
use std::sync::{Arc, Mutex};

fn node_batch(ids: &[u64], members: usize, legacy: bool) -> RecordBatch {
    let uuids = FixedSizeBinaryArray::try_from_iter(ids.iter().map(|id| {
        let mut uuid = [0u8; 16];
        uuid[8..].copy_from_slice(&id.to_be_bytes());
        uuid
    }))
    .unwrap();
    let timestamps = TimestampMicrosecondArray::from_iter_values(ids.iter().map(|id| *id as i64))
        .with_timezone("UTC");
    let memberships = ListArray::new(
        Arc::new(Field::new("item", DataType::UInt32, false)),
        OffsetBuffer::from_lengths(std::iter::repeat_n(members, ids.len())),
        Arc::new(UInt32Array::from_iter_values(
            (0..ids.len()).flat_map(|_| 1..=members as u32),
        )),
        None,
    );
    let canonical = RecordBatch::try_new(
        crate::TOPOLOGY_NODES_SCHEMA.clone(),
        vec![
            Arc::new(uuids),
            Arc::new(UInt64Array::from(ids.to_vec())),
            Arc::new(UInt32Array::from(vec![1; ids.len()])),
            Arc::new(memberships),
            Arc::new(timestamps.clone()),
            Arc::new(timestamps),
        ],
    )
    .unwrap();
    if legacy {
        canonical.project(&[0, 1, 2, 4, 5]).unwrap()
    } else {
        canonical
    }
}

fn node_file(path: &std::path::Path, batch: &RecordBatch, group: usize) -> crate::TopologyFiles {
    let properties = crate::permanent_parquet::writer_properties()
        .set_dictionary_enabled(false)
        .set_max_row_group_row_count(Some(group))
        .build();
    let mut writer = ArrowWriter::try_new(
        std::fs::File::create(path).unwrap(),
        batch.schema(),
        Some(properties),
    )
    .unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
    crate::TopologyFiles {
        nodes: vec![(path.to_owned(), "nodes.parquet".into())],
        edges: vec![],
    }
}

#[derive(Default)]
struct Observe(Mutex<Vec<crate::io_stats::FilteredReadPruning>>);
impl crate::io_stats::FilteredReadObserver for Observe {
    fn read_started(&self, _: crate::io_stats::FilteredReadTable) {}
    fn rows_scanned(&self, _: crate::io_stats::FilteredReadTable, _: u64) {}
    fn read_completed(&self, _: crate::io_stats::FilteredReadTable, _: u64, _: bool) {}
    fn read_failed(&self, _: crate::io_stats::FilteredReadTable) {}
    fn pruning(
        &self,
        _: crate::io_stats::FilteredReadTable,
        report: crate::io_stats::FilteredReadPruning,
    ) {
        self.0.lock().unwrap().push(report);
    }
}

#[test]
fn node_admission_covers_repeated_memberships_and_majority_full_schema_decode() {
    let dir = tempfile::TempDir::new().unwrap();
    let source = node_batch(&(1..=7).collect::<Vec<_>>(), 4097, false);
    for group in [1, 4, 7] {
        let files = node_file(
            &dir.path().join(format!("nodes-{group}.parquet")),
            &source,
            group,
        );
        let ids = (1..=6).collect();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(16 * 1024 * 1024));
        let reservation = MemoryConsumer::new("node decoder control").register(&pool);
        let (bound, metadata, _) =
            node_decode_admission_bytes(&files, &ids, |bytes| reservation.try_grow(bytes)).unwrap();
        assert!(metadata > 0);
        reservation.try_grow(bound).unwrap();
        let observed = Arc::new(Observe::default());
        let observer: Arc<dyn crate::io_stats::FilteredReadObserver> = observed.clone();
        let batches = crate::read_nodes_filtered_projected_observed_from_files(
            &files,
            &ids,
            &[3, 4],
            Some(&observer),
        )
        .unwrap();
        let actual = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
        let expected = source.slice(0, 6).project(&[1, 3, 4]).unwrap();
        assert_eq!(actual, expected);
        assert!(bound > source.get_array_memory_size() * 3);
        assert_eq!(
            observed.0.lock().unwrap()[0].strategy,
            crate::io_stats::FilteredReadStrategy::FullFallback
        );
        drop(actual);
        drop(batches);
        assert!(pool.reserved() >= bound);
        drop(reservation);
        assert_eq!(pool.reserved(), 0);
    }
}

#[test]
fn node_admission_covers_legacy_normalization_before_projection() {
    let dir = tempfile::TempDir::new().unwrap();
    let ids_vec = (1..=7).collect::<Vec<_>>();
    let source = node_batch(&ids_vec, 1, true);
    let files = node_file(&dir.path().join("legacy.parquet"), &source, 3);
    let ids = (1..=6).collect();
    let bound = node_decode_admission_bytes(&files, &ids, |_| Ok(()))
        .unwrap()
        .0;
    let batches =
        crate::read_nodes_filtered_projected_observed_from_files(&files, &ids, &[3, 4], None)
            .unwrap();
    let actual = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
    let expected = node_batch(&ids_vec, 1, false)
        .slice(0, 6)
        .project(&[1, 3, 4])
        .unwrap();
    assert_eq!(actual, expected);
    assert!(bound > source.get_array_memory_size() + actual.get_array_memory_size());
}

#[test]
fn node_admission_covers_a_genuine_dense_validation_retry() {
    let dir = tempfile::TempDir::new().unwrap();
    // Real values, genuine writer statistics: min/max and one page cover a
    // dense range, but two interior rows are permuted. No footer is invented.
    let source = node_batch(&[1, 3, 2, 4, 5, 6, 7], 257, false);
    let files = node_file(&dir.path().join("permuted.parquet"), &source, 7);
    let ids = std::collections::HashSet::from([2]);
    let bound = node_decode_admission_bytes(&files, &ids, |_| Ok(()))
        .unwrap()
        .0;
    let observed = Arc::new(Observe::default());
    let observer: Arc<dyn crate::io_stats::FilteredReadObserver> = observed.clone();
    let batches = crate::read_nodes_filtered_projected_observed_from_files(
        &files,
        &ids,
        &[3, 4],
        Some(&observer),
    )
    .unwrap();
    let actual = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
    assert_eq!(actual, source.slice(2, 1).project(&[1, 3, 4]).unwrap());
    let reports = observed.0.lock().unwrap();
    assert_eq!(
        reports.len(),
        2,
        "the first dense attempt must really retry"
    );
    assert_eq!(reports[0].validation_fallbacks, 1);
    assert_eq!(
        reports[1].strategy,
        crate::io_stats::FilteredReadStrategy::RowGroupPredicate
    );
    assert!(bound > actual.get_array_memory_size() * 3);
}

#[test]
fn node_admission_refuses_a_real_unsupported_nested_physical_root() {
    let dir = tempfile::TempDir::new().unwrap();
    let canonical = node_batch(&[1, 2, 3], 3, false);
    let nested = arrow::array::StructArray::from(vec![(
        Arc::new(Field::new("child", DataType::UInt32, false)),
        Arc::new(UInt32Array::from(vec![1, 2, 3])) as arrow::array::ArrayRef,
    )]);
    let mut fields = canonical.schema().fields().to_vec();
    fields.push(Arc::new(Field::new(
        "extra",
        nested.data_type().clone(),
        false,
    )));
    let mut columns = canonical.columns().to_vec();
    columns.push(Arc::new(nested));
    let source = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let files = node_file(&dir.path().join("nested.parquet"), &source, 3);
    let error =
        node_decode_admission_bytes(&files, &std::collections::HashSet::from([1]), |_| Ok(()))
            .unwrap_err();
    assert!(
        matches!(error, DataFusionError::ResourcesExhausted(_)),
        "{error}"
    );
}

#[allow(
    deprecated,
    reason = "hostile genuine-page controls mutate the raw Thrift allocation authority"
)]
fn rewrite_first_header(
    path: &std::path::Path,
    metadata: &parquet::file::metadata::ParquetMetaData,
    mutate: impl FnOnce(&mut parquet::format::PageHeader),
) {
    use parquet::thrift::TSerializable;
    let mut bytes = std::fs::read(path).unwrap();
    let start = metadata.row_group(0).column(0).data_page_offset() as usize;
    let mut input = std::io::Cursor::new(&bytes[start..]);
    let mut header = parquet::format::PageHeader::read_from_in_protocol(
        &mut thrift::protocol::TCompactInputProtocol::new(&mut input),
    )
    .unwrap();
    let length = input.position() as usize;
    mutate(&mut header);
    let mut encoded = Vec::new();
    header
        .write_to_out_protocol(&mut thrift::protocol::TCompactOutputProtocol::new(
            &mut encoded,
        ))
        .unwrap();
    assert_eq!(encoded.len(), length, "body offsets must remain unchanged");
    bytes[start..start + length].copy_from_slice(&encoded);
    std::fs::write(path, bytes).unwrap();
}

#[test]
#[allow(
    deprecated,
    reason = "mutates one real page's allocation-driving count"
)]
fn node_raw_admission_rejects_a_page_value_count_above_its_footer_before_decode() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("hostile-count.parquet");
    let source = node_batch(&(1..=7).collect::<Vec<_>>(), 3, false);
    node_file(&path, &source, 7);
    let builder = crate::catalog::admitted_parquet(&path).unwrap();
    let metadata = builder.metadata().clone();
    validate_pages(&path, &metadata).unwrap();
    rewrite_first_header(&path, &metadata, |header| {
        assert_eq!(header.data_page_header.as_ref().unwrap().num_values, 7);
        header.data_page_header.as_mut().unwrap().num_values = 8;
    });
    let error = validate_pages(&path, &metadata).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("page values exceed column footer"),
        "{error}"
    );
}

#[test]
#[allow(
    deprecated,
    reason = "mutates the size header of an actual uncompressed page"
)]
fn node_raw_admission_rejects_uncompressed_header_body_disagreement() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("hostile-size.parquet");
    let source = node_batch(&(1..=7).collect::<Vec<_>>(), 3, false);
    let properties = crate::permanent_parquet::writer_properties()
        .set_compression(parquet::basic::Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .build();
    let mut writer = ArrowWriter::try_new(
        std::fs::File::create(&path).unwrap(),
        source.schema(),
        Some(properties),
    )
    .unwrap();
    writer.write(&source).unwrap();
    writer.close().unwrap();
    let builder = crate::catalog::admitted_parquet(&path).unwrap();
    let metadata = builder.metadata().clone();
    validate_pages(&path, &metadata).unwrap();
    rewrite_first_header(&path, &metadata, |header| {
        assert_eq!(header.compressed_page_size, header.uncompressed_page_size);
        header.uncompressed_page_size -= 1;
    });
    let error = validate_pages(&path, &metadata).unwrap_err();
    assert!(
        error.to_string().contains("header/body size disagreement"),
        "{error}"
    );
}

#[test]
fn node_raw_admission_rejects_cumulative_multi_page_bytes_above_footer() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("multi-page.parquet");
    let source = node_batch(&(1..=7).collect::<Vec<_>>(), 3, false);
    let properties = crate::permanent_parquet::writer_properties()
        .set_dictionary_enabled(false)
        .set_write_batch_size(1)
        .set_data_page_row_count_limit(1)
        .build();
    let mut writer = ArrowWriter::try_new(
        std::fs::File::create(&path).unwrap(),
        source.schema(),
        Some(properties),
    )
    .unwrap();
    writer.write(&source).unwrap();
    writer.close().unwrap();
    let builder = crate::catalog::admitted_parquet(&path).unwrap();
    let metadata = builder.metadata().clone();
    validate_pages(&path, &metadata).unwrap();
    // Actual body/page sequence stays intact. Only the in-memory audit footer
    // understates the cumulative uncompressed column by one byte.
    let mut groups = metadata.row_groups().to_vec();
    let mut columns = groups[0].columns().to_vec();
    let understated = columns[0].uncompressed_size() - 1;
    columns[0] = columns[0]
        .clone()
        .into_builder()
        .set_total_uncompressed_size(understated)
        .build()
        .unwrap();
    groups[0] = groups[0]
        .clone()
        .into_builder()
        .set_column_metadata(columns)
        .build()
        .unwrap();
    let hostile = metadata
        .as_ref()
        .clone()
        .into_builder()
        .set_row_groups(groups)
        .build();
    let error = validate_pages(&path, &hostile).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("page sizes exceed column footer"),
        "{error}"
    );
}

#[test]
fn node_page_header_rejects_hostile_binary_length_without_materializing_it() {
    use thrift::protocol::{
        TCompactOutputProtocol, TFieldIdentifier, TOutputProtocol, TStructIdentifier, TType,
    };
    let mut bytes = Vec::new();
    {
        let mut output = TCompactOutputProtocol::new(&mut bytes);
        output
            .write_struct_begin(&TStructIdentifier::new("PageHeader"))
            .unwrap();
        for (id, value) in [(1, 0), (2, 8), (3, 8)] {
            output
                .write_field_begin(&TFieldIdentifier::new::<_, String, _>(
                    None::<String>,
                    TType::I32,
                    id,
                ))
                .unwrap();
            output.write_i32(value).unwrap();
            output.write_field_end().unwrap();
        }
        output
            .write_field_begin(&TFieldIdentifier::new::<_, String, _>(
                None::<String>,
                TType::Struct,
                5,
            ))
            .unwrap();
        output
            .write_struct_begin(&TStructIdentifier::new("DataPageHeader"))
            .unwrap();
        output
            .write_field_begin(&TFieldIdentifier::new::<_, String, _>(
                None::<String>,
                TType::I32,
                1,
            ))
            .unwrap();
        output.write_i32(1).unwrap();
        output.write_field_end().unwrap();
        output
            .write_field_begin(&TFieldIdentifier::new::<_, String, _>(
                None::<String>,
                TType::Struct,
                5,
            ))
            .unwrap();
        output
            .write_struct_begin(&TStructIdentifier::new("Statistics"))
            .unwrap();
        output
            .write_field_begin(&TFieldIdentifier::new::<_, String, _>(
                None::<String>,
                TType::String,
                1,
            ))
            .unwrap();
        // Compact binary length = 2 GiB; there is no payload to materialize.
        for byte in [0x80u8, 0x80, 0x80, 0x80, 0x08] {
            output.write_i8(byte as i8).unwrap();
        }
    }
    assert!(bytes.len() < 64);
    let mut cursor = std::io::Cursor::new(bytes);
    let result = page_header(&mut thrift::protocol::TCompactInputProtocol::new(
        &mut cursor,
    ));
    assert!(result.is_err());
    assert_eq!(cursor.position() as usize, cursor.get_ref().len());
}

/// Keep every genuine page byte and offset, replacing only the footer and its
/// trailer. A decoder can reach these allocation-driving fields before it has
/// rejected the malformed tree/list contents.
fn replace_node_footer(path: &std::path::Path, rewrite: impl FnOnce(&[u8]) -> Vec<u8>) {
    let mut bytes = std::fs::read(path).unwrap();
    let trailer = bytes.len() - 8;
    let length = u32::from_le_bytes(bytes[trailer..trailer + 4].try_into().unwrap()) as usize;
    let start = trailer - length;
    let footer = rewrite(&bytes[start..trailer]);
    assert!(
        footer.len() < 4096,
        "the hostile wire input must remain bounded"
    );
    bytes.truncate(start);
    bytes.extend_from_slice(&footer);
    bytes.extend_from_slice(&u32::try_from(footer.len()).unwrap().to_le_bytes());
    bytes.extend_from_slice(b"PAR1");
    std::fs::write(path, bytes).unwrap();
}

#[test]
#[allow(
    deprecated,
    reason = "mutates a genuine raw footer's allocation authority"
)]
fn node_metadata_admission_rejects_schema_children_before_tree_allocation() {
    use parquet::thrift::TSerializable;
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("schema-children.parquet");
    let source = node_batch(&[1, 2], 2, false);
    let files = node_file(&path, &source, 2);
    node_decode_admission_bytes(&files, &std::collections::HashSet::from([1]), |_| Ok(())).unwrap();
    replace_node_footer(&path, |footer| {
        let mut metadata = parquet::format::FileMetaData::read_from_in_protocol(
            &mut thrift::protocol::TCompactInputProtocol::new(footer),
        )
        .unwrap();
        assert_eq!(metadata.schema[0].num_children, Some(6));
        metadata.schema[0].num_children = Some(4096);
        let mut encoded = Vec::new();
        metadata
            .write_to_out_protocol(&mut thrift::protocol::TCompactOutputProtocol::new(
                &mut encoded,
            ))
            .unwrap();
        encoded
    });
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
    let reservation = MemoryConsumer::new("metadata tree control").register(&pool);
    let mut admissions = 0;
    let error =
        node_decode_admission_bytes(&files, &std::collections::HashSet::from([1]), |bytes| {
            admissions += 1;
            reservation.try_grow(bytes)
        })
        .unwrap_err();
    assert!(
        matches!(error, DataFusionError::ResourcesExhausted(_)),
        "{error}"
    );
    assert_eq!(
        admissions, 1,
        "refuse before admitting or constructing the decoded tree"
    );
    assert!(crate::catalog::admitted_parquet(&path).is_err());
    drop(reservation);
    assert_eq!(pool.reserved(), 0);
}

#[test]
fn node_metadata_admission_rejects_declared_footer_collection_before_allocation() {
    use std::io::{Seek, SeekFrom};
    use thrift::protocol::{
        TCompactInputProtocol, TCompactOutputProtocol, TInputProtocol, TListIdentifier,
        TOutputProtocol, TType,
    };
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("footer-collection.parquet");
    let source = node_batch(&[1, 2], 2, false);
    let files = node_file(&path, &source, 2);
    node_decode_admission_bytes(&files, &std::collections::HashSet::from([1]), |_| Ok(())).unwrap();
    replace_node_footer(&path, |footer| {
        let mut input = TCompactInputProtocol::new(std::io::Cursor::new(footer));
        input.read_struct_begin().unwrap();
        let start = loop {
            let field = input.read_field_begin().unwrap();
            assert_ne!(field.field_type, TType::Stop);
            if field.id == Some(4) {
                assert_eq!(field.field_type, TType::List);
                break input.stream_position().unwrap() as usize;
            }
            input.skip(field.field_type).unwrap();
            input.read_field_end().unwrap();
        };
        let list = input.read_list_begin().unwrap();
        assert_eq!(list.element_type, TType::Struct);
        assert_eq!(list.size, 1);
        let end = input.seek(SeekFrom::Current(0)).unwrap() as usize;
        let mut replacement = Vec::new();
        TCompactOutputProtocol::new(&mut replacement)
            .write_list_begin(&TListIdentifier::new(TType::Struct, 4096))
            .unwrap();
        let mut encoded = footer[..start].to_vec();
        encoded.extend_from_slice(&replacement);
        encoded.extend_from_slice(&footer[end..]);
        encoded
    });
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
    let reservation = MemoryConsumer::new("metadata collection control").register(&pool);
    let mut admissions = 0;
    let error =
        node_decode_admission_bytes(&files, &std::collections::HashSet::from([1]), |bytes| {
            admissions += 1;
            reservation.try_grow(bytes)
        })
        .unwrap_err();
    assert!(
        matches!(error, DataFusionError::ResourcesExhausted(_)),
        "{error}"
    );
    assert_eq!(
        admissions, 1,
        "wire count must fail before decoded collection allocation"
    );
    assert!(crate::catalog::admitted_parquet(&path).is_err());
    drop(reservation);
    assert_eq!(pool.reserved(), 0);
}

#[test]
#[allow(
    deprecated,
    reason = "encodes a genuine footer's valid empty metadata list"
)]
fn node_metadata_admission_accepts_a_zero_header_empty_key_value_list() {
    use parquet::thrift::TSerializable;
    use std::io::Seek;
    use thrift::protocol::{TCompactInputProtocol, TCompactOutputProtocol, TInputProtocol, TType};
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("empty-metadata.parquet");
    let source = node_batch(&[1, 2], 2, false);
    let files = node_file(&path, &source, 2);
    replace_node_footer(&path, |footer| {
        let mut metadata = parquet::format::FileMetaData::read_from_in_protocol(
            &mut TCompactInputProtocol::new(footer),
        )
        .unwrap();
        // Preserve physical schema, real pages and every index location; this
        // deliberately removes the optional embedded Arrow schema as well.
        metadata.key_value_metadata = Some(Vec::new());
        let mut encoded = Vec::new();
        metadata
            .write_to_out_protocol(&mut TCompactOutputProtocol::new(&mut encoded))
            .unwrap();
        let mut input = TCompactInputProtocol::new(std::io::Cursor::new(encoded.as_slice()));
        input.read_struct_begin().unwrap();
        let position = loop {
            let field = input.read_field_begin().unwrap();
            assert_ne!(field.field_type, TType::Stop);
            if field.id == Some(5) {
                assert_eq!(field.field_type, TType::List);
                break input.stream_position().unwrap() as usize;
            }
            input.skip(field.field_type).unwrap();
            input.read_field_end().unwrap();
        };
        let list = input.read_list_begin().unwrap();
        assert_eq!(list.size, 0);
        assert_eq!(input.stream_position().unwrap() as usize, position + 1);
        drop(input);
        assert_eq!(encoded[position], 12, "ordinary empty Struct-list header");
        encoded[position] = 0;
        encoded
    });
    // The pinned ordinary reader accepts this physical schema and encoding.
    let ordinary = crate::catalog::admitted_parquet(&path).unwrap();
    assert!(
        ordinary
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .unwrap()
            .is_empty()
    );
    let ordinary_batches = ordinary
        .build()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let expected = arrow::compute::concat_batches(&ordinary_batches[0].schema(), &ordinary_batches)
        .unwrap()
        .slice(0, 1)
        .project(&[1, 4])
        .unwrap();
    drop(ordinary_batches);
    let ids = std::collections::HashSet::from([1]);
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(16 * 1024 * 1024));
    let reservation = MemoryConsumer::new("empty metadata control").register(&pool);
    let (decoder, metadata, _) =
        node_decode_admission_bytes(&files, &ids, |bytes| reservation.try_grow(bytes)).unwrap();
    assert!(metadata > 0);
    reservation.try_grow(decoder).unwrap();
    let batches =
        crate::read_nodes_filtered_projected_observed_from_files(&files, &ids, &[4], None).unwrap();
    let actual = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(
        actual
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[1]
    );
    assert_eq!(
        actual
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .values()
            .as_ref(),
        &[1]
    );
    drop(actual);
    drop(batches);
    drop(reservation);
    assert_eq!(pool.reserved(), 0);
}

#[test]
fn node_page_header_audit_uses_small_physical_reads_for_real_pages() {
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("buffered-headers.parquet");
    let source = node_batch(&(1..=7).collect::<Vec<_>>(), 257, false);
    node_file(&path, &source, 1);
    let ordinary = SerializedFileReader::new(std::fs::File::open(&path).unwrap()).unwrap();
    let mut pages = 0u64;
    for group in 0..ordinary.num_row_groups() {
        let group = ordinary.get_row_group(group).unwrap();
        for column in 0..group.num_columns() {
            let mut reader = group.get_column_page_reader(column).unwrap();
            while reader.get_next_page().unwrap().is_some() {
                pages += 1;
            }
        }
    }
    assert!(
        pages >= 42,
        "exercise real headers across all groups and roots"
    );
    let metadata = ordinary.metadata().clone();
    drop(ordinary);
    let _capture = crate::lifecycle_io::CaptureScope::install();
    validate_pages(&path, &metadata).unwrap();
    let observed = crate::lifecycle_io::snapshot().unwrap();
    let reads = &observed.phases[&crate::StorageIoPhase::ReadPathScan];
    assert_eq!(
        reads.read_calls, pages,
        "one physical window reads each small real header"
    );
    assert!(reads.read_bytes >= reads.read_calls);
    assert!(
        reads.read_bytes <= pages * 256,
        "no large per-page payload prefetch: {reads:?}"
    );
}
