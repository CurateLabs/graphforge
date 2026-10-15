use super::super::*;
use super::*;
use arrow::array::{ArrayRef, BooleanArray, FixedSizeBinaryArray, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use std::collections::{BTreeSet, HashMap};
use tempfile::TempDir;

#[test]
fn flat_plain_bytes_release_transition_page_admission_only_while_idle() {
    use arrow::array::{ListBuilder, StringBuilder};
    use parquet::basic::{Compression, Encoding};
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;

    const ROWS: usize = 3;
    const BATCH_ROWS: usize = 2;
    let values = (0..ROWS)
        .map(|row| format!("{row}{}", "x".repeat(32 * 1024)))
        .collect::<Vec<_>>();
    for (encoding, repeated) in [
        (Encoding::PLAIN, false),
        (Encoding::DELTA_LENGTH_BYTE_ARRAY, false),
        (Encoding::PLAIN, true),
    ] {
        let values: ArrayRef = if repeated {
            let mut lists = ListBuilder::new(StringBuilder::new());
            for value in &values {
                lists.values().append_value(value);
                lists.append(true);
            }
            Arc::new(lists.finish())
        } else {
            Arc::new(StringArray::from_iter_values(&values))
        };
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            values.data_type().clone(),
            false,
        )]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![values]).unwrap();
        let file = tempfile::tempfile().unwrap();
        let properties = crate::permanent_parquet::writer_properties()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_encoding(encoding)
            .set_write_batch_size(ROWS)
            .set_max_row_group_row_count(Some(ROWS))
            .build();
        let mut writer = ArrowWriter::try_new(
            file.try_clone().unwrap(),
            Arc::clone(&schema),
            Some(properties),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file.try_clone().unwrap()).unwrap();
        let admission = parquet_resource_admission(
            builder.metadata(),
            PropertyOverlayLimits::default(),
            &file,
            &ReadCounts::new(false),
            None,
            BATCH_ROWS,
            true,
            corrupt,
        )
        .unwrap();
        if encoding == Encoding::PLAIN && !repeated {
            // The actual page fixes both the unchanged transition envelope and
            // the one-page idle state; footer totals also include its header.
            let physical = SerializedFileReader::new(file.try_clone().unwrap()).unwrap();
            let group = physical.get_row_group(0).unwrap();
            let mut pages = group.get_column_page_reader(0).unwrap();
            let page = pages.get_next_page().unwrap().unwrap();
            let page_bytes = u64::try_from(page.buffer().len()).unwrap();
            assert!(pages.get_next_page().unwrap().is_none());
            let chunk_bytes = u64::try_from(
                builder
                    .metadata()
                    .row_group(0)
                    .column(0)
                    .uncompressed_size(),
            )
            .unwrap();
            let bookkeeping = u64::try_from(BATCH_ROWS).unwrap() * 16;
            assert_eq!(
                admission.with_codec,
                3 * page_bytes + 8 * 1024 + chunk_bytes + bookkeeping
            );
            assert_eq!(
                admission.memory_with_codec,
                2 * page_bytes + 8 * 1024 + chunk_bytes + bookkeeping
            );
            assert_eq!(admission.retained, 2 * page_bytes + 8 * 1024 + bookkeeping);
            assert_eq!(
                admission.memory_retained,
                page_bytes + 8 * 1024 + bookkeeping
            );
        } else {
            assert_eq!(admission.retained, admission.with_codec);
            assert_eq!(admission.memory_retained, admission.memory_with_codec);
        }
        let decoded = builder
            .with_batch_size(BATCH_ROWS)
            .build()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            arrow::compute::concat_batches(&schema, &decoded).unwrap(),
            batch
        );
    }
}

#[test]
fn replay_decoder_admission_covers_pages_groups_and_repeated_values() {
    use arrow::array::{ListBuilder, UInt64Builder};
    let strings = (0..7)
        .map(|index| format!("{index}{}", "x".repeat(100 * 1024)))
        .collect::<Vec<_>>();
    let mut lists = ListBuilder::new(UInt64Builder::new());
    for row in 0..7 {
        for value in 0..4097 {
            lists.values().append_value(u64::MAX - value - row);
        }
        lists.append(true);
    }
    let nested = lists.finish();
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Utf8, false),
        Field::new("repeated", nested.data_type().clone(), false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(strings)), Arc::new(nested)],
    )
    .unwrap();
    for group_rows in [1, 2, 7] {
        let file = tempfile::tempfile().unwrap();
        let properties = crate::permanent_parquet::writer_properties()
            .set_dictionary_enabled(false)
            .set_write_batch_size(1)
            .set_data_page_row_count_limit(1)
            .set_max_row_group_row_count(Some(group_rows))
            .build();
        let mut writer =
            ArrowWriter::try_new(file.try_clone().unwrap(), schema.clone(), Some(properties))
                .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file.try_clone().unwrap()).unwrap();
        let required =
            replay_parquet_reader_reservation(builder.metadata(), &file, 64 * 1024 * 1024, 7)
                .unwrap();
        assert!(
            required
                > batch.get_array_memory_size()
                    + 2 * crate::permanent_parquet::ZSTD_DECODER_WORKSPACE
        );
        assert_eq!(
            replay_parquet_reader_reservation(builder.metadata(), &file, required, 7).unwrap(),
            required
        );
        assert_eq!(
            replay_parquet_reader_reservation(builder.metadata(), &file, required - 1, 7)
                .unwrap_err()
                .code(),
            "GF_RESOURCE_LIMIT"
        );
        let decoded = builder
            .with_batch_size(7)
            .build()
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(decoded, batch);
    }
}

#[test]
fn replay_decoder_group_window_covers_every_batch_start() {
    let groups = [(3, 100), (1, 700), (4, 200), (2, 900), (1, 300)];
    let rows = groups
        .iter()
        .enumerate()
        .flat_map(|(index, (rows, _))| std::iter::repeat_n(index, *rows as usize))
        .collect::<Vec<_>>();
    for batch_rows in 1..=rows.len() + 1 {
        let actual = (0..rows.len())
            .map(|start| {
                rows[start..rows.len().min(start + batch_rows)]
                    .iter()
                    .copied()
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .map(|index| groups[index].1)
                    .sum::<u64>()
            })
            .max()
            .unwrap();
        assert_eq!(contributing_row_group_bytes(&groups, batch_rows), actual);
    }
}

#[test]
fn retained_reader_rejects_wide_projected_pages_before_arrow_allocation() {
    let dir = TempDir::new().unwrap();
    let scratch = TempDir::new().unwrap();
    let route_dir = dir.path().join("properties/Wide");
    fs::create_dir_all(&route_dir).unwrap();
    let id = PropertyFragmentId {
        generation: 1,
        ordinal: 0,
    };
    let mut fields = vec![
        Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
        Field::new(PROPERTY_TOMBSTONE_FIELD, DataType::Boolean, false),
    ];
    let mut columns = vec![
        Arc::new(FixedSizeBinaryArray::try_from_iter(vec![vec![7; 16]].into_iter()).unwrap())
            as ArrayRef,
        Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
    ];
    let value = "x".repeat(256);
    for index in 0..32 {
        fields.push(Field::new(
            format!("value_{index:02}"),
            DataType::Utf8,
            true,
        ));
        columns.push(Arc::new(StringArray::from(vec![Some(value.as_str())])) as ArrayRef);
    }
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        HashMap::from([
            (
                PROPERTY_OVERLAY_FORMAT_KEY.into(),
                PROPERTY_OVERLAY_FORMAT.into(),
            ),
            (PROPERTY_ROUTE_KEY.into(), "Wide".into()),
            (PROPERTY_KIND_KEY.into(), "node".into()),
            (PROPERTY_GENERATION_KEY.into(), "1".into()),
            (PROPERTY_ORDINAL_KEY.into(), "0".into()),
        ]),
    ));
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
    let path = route_dir.join(id.file_name());
    let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let error = visit_authenticated_property_snapshots(
        dir.path(),
        PropertyRouteKind::Node,
        "Wide",
        scratch.path(),
        PropertyOverlayLimits {
            max_buffered_rows: 8,
            max_open_runs: 2,
            max_buffered_bytes: 16 * 1024,
            max_row_bytes: 12 * 1024,
        },
        |_| Ok(()),
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("projected pages exceed"),
        "{error}"
    );
}

#[test]
#[allow(
    deprecated,
    reason = "hostile raw PageHeader regression for Parquet 58"
)]
fn retained_reader_rejects_footer_small_page_header_large_before_decode() {
    use std::io::{Cursor, Seek, SeekFrom};

    let dir = TempDir::new().unwrap();
    let route_dir = dir.path().join("properties/Person");
    fs::create_dir_all(&route_dir).unwrap();
    let id = PropertyFragmentId {
        generation: 1,
        ordinal: 0,
    };
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
            Field::new(PROPERTY_TOMBSTONE_FIELD, DataType::Boolean, false),
            Field::new("name", DataType::Utf8, true),
        ],
        HashMap::from([
            (
                PROPERTY_OVERLAY_FORMAT_KEY.into(),
                PROPERTY_OVERLAY_FORMAT.into(),
            ),
            (PROPERTY_ROUTE_KEY.into(), "Person".into()),
            (PROPERTY_KIND_KEY.into(), "node".into()),
            (PROPERTY_GENERATION_KEY.into(), "1".into()),
            (PROPERTY_ORDINAL_KEY.into(), "0".into()),
        ]),
    ));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(FixedSizeBinaryArray::try_from_iter(vec![vec![7; 16]].into_iter()).unwrap()),
            Arc::new(BooleanArray::from(vec![false])),
            Arc::new(StringArray::from(vec![Some("small")])),
        ],
    )
    .unwrap();
    let path = route_dir.join(id.file_name());
    let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap()).unwrap();
    let column = &builder.metadata().row_group(0).columns()[0];
    let offset = u64::try_from(column.data_page_offset()).unwrap();
    let footer_uncompressed = column.uncompressed_size();
    let mut file = File::open(&path).unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    let mut raw = vec![0_u8; 64 * 1024];
    let read = file.read(&mut raw).unwrap();
    raw.truncate(read);
    let mut cursor = Cursor::new(raw.as_slice());
    let mut protocol = thrift::protocol::TCompactInputProtocol::new(&mut cursor);
    let original = parquet::format::PageHeader::read_from_in_protocol(&mut protocol).unwrap();
    drop(protocol);
    let header_len = usize::try_from(cursor.position()).unwrap();
    let mut hostile = original.clone();
    let mut encoded = Vec::new();
    for candidate in (footer_uncompressed + 1)..=(footer_uncompressed + 4096) {
        hostile.uncompressed_page_size = i32::try_from(candidate).unwrap();
        encoded.clear();
        let mut output = thrift::protocol::TCompactOutputProtocol::new(&mut encoded);
        hostile.write_to_out_protocol(&mut output).unwrap();
        if encoded.len() == header_len {
            break;
        }
    }
    assert_eq!(encoded.len(), header_len);
    let mut file = fs::OpenOptions::new().write(true).open(&path).unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&encoded).unwrap();
    file.flush().unwrap();

    let error = visit_authenticated_property_snapshots(
        dir.path(),
        PropertyRouteKind::Node,
        "Person",
        dir.path(),
        PropertyOverlayLimits {
            max_buffered_rows: 8,
            max_open_runs: 2,
            max_buffered_bytes: u64::try_from(footer_uncompressed).unwrap() * 3,
            max_row_bytes: 32,
        },
        |_| Ok(()),
    )
    .unwrap_err();
    assert!(error.to_string().contains("page exceeds"), "{error}");
}

#[test]
#[allow(
    deprecated,
    reason = "hostile raw dictionary PageHeader regression for Parquet 58"
)]
fn admitted_reader_rejects_oversized_dictionary_count_before_offset_allocation() {
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    use std::cell::Cell;
    use std::io::{Cursor, Seek, SeekFrom, Write};

    const ROWS: usize = 128;
    let root = TempDir::new().unwrap();
    let id = PropertyFragmentId {
        generation: 0,
        ordinal: 0,
    };
    let relative = format!("properties/Person/{}", id.file_name());
    let path = root.path().join(&relative);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
            Field::new(PROPERTY_TOMBSTONE_FIELD, DataType::Boolean, false),
            Field::new("name", DataType::Utf8, true),
        ],
        HashMap::from([
            (
                PROPERTY_OVERLAY_FORMAT_KEY.into(),
                PROPERTY_OVERLAY_FORMAT.into(),
            ),
            (PROPERTY_ROUTE_KEY.into(), "Person".into()),
            (PROPERTY_KIND_KEY.into(), "node".into()),
            (PROPERTY_GENERATION_KEY.into(), "0".into()),
            (PROPERTY_ORDINAL_KEY.into(), "0".into()),
        ]),
    ));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(
                FixedSizeBinaryArray::try_from_iter(
                    (0..ROWS).map(|row| (row as u128).to_be_bytes()),
                )
                .unwrap(),
            ),
            Arc::new(BooleanArray::from(vec![false; ROWS])),
            Arc::new(StringArray::from_iter_values((0..ROWS).map(|row| {
                if row == 0 {
                    String::new()
                } else {
                    format!("v{row:03}")
                }
            }))),
        ],
    )
    .unwrap();
    let properties = crate::permanent_parquet::writer_properties()
        .set_dictionary_enabled(true)
        .build();
    let mut writer = ArrowWriter::try_new(
        File::create(&path).unwrap(),
        Arc::clone(&schema),
        Some(properties),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap()).unwrap();
    // Empty strings still have a four-byte dictionary length. The valid
    // empty/short entries must pass the same audit as the hostile control.
    parquet_resource_admission(
        builder.metadata(),
        PropertyOverlayLimits::default(),
        &File::open(&path).unwrap(),
        &ReadCounts::new(false),
        None,
        ROWS,
        true,
        corrupt,
    )
    .unwrap();
    let offset = u64::try_from(
        builder
            .metadata()
            .row_group(0)
            .column(2)
            .dictionary_page_offset()
            .unwrap(),
    )
    .unwrap();
    let valid = builder
        .build()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        arrow::compute::concat_batches(&schema, &valid).unwrap(),
        batch
    );

    let mut file = File::open(&path).unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    let mut raw = vec![0; 64 * 1024];
    let read = file.read(&mut raw).unwrap();
    raw.truncate(read);
    let mut cursor = Cursor::new(raw.as_slice());
    let mut protocol = thrift::protocol::TCompactInputProtocol::new(&mut cursor);
    let mut header = parquet::format::PageHeader::read_from_in_protocol(&mut protocol).unwrap();
    drop(protocol);
    let header_len = usize::try_from(cursor.position()).unwrap();
    let dictionary = header.dictionary_page_header.as_mut().unwrap();
    assert_eq!(dictionary.num_values, i32::try_from(ROWS).unwrap());
    dictionary.num_values = 1024;
    assert!(dictionary.num_values * 4 > header.uncompressed_page_size);
    let mut encoded = Vec::new();
    let mut output = thrift::protocol::TCompactOutputProtocol::new(&mut encoded);
    header.write_to_out_protocol(&mut output).unwrap();
    drop(output);
    assert_eq!(
        encoded.len(),
        header_len,
        "keep body and footer offsets intact"
    );
    let mut file = fs::OpenOptions::new().write(true).open(&path).unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&encoded).unwrap();
    file.flush().unwrap();

    // Authenticate the hostile bytes themselves. A checksum mismatch must not
    // mask the preallocation check being exercised at the raw-page audit.
    let bytes = fs::read(&path).unwrap();
    let inventory = AuthenticatedPropertyInventory::from_entries_at_root(
        root.path(),
        vec![crate::GraphFileEntry {
            content_xxh64: crate::corruption_checksum::checksum(&bytes),
            relative_path: relative,
            byte_length: u64::try_from(bytes.len()).unwrap(),
            content_sha256: digest_hex(&Sha256::digest(&bytes)),
            role: crate::GraphFileRole::Properties,
        }],
    )
    .unwrap();
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(128 * 1024 * 1024));
    let execution = crate::property_scan_memory::Budget::new(&pool);
    let emitted = Cell::new(0);
    let error = inventory
        .visit_route_streaming_admitted(
            &super::super::streaming_merge::RouteRead {
                kind: PropertyRouteKind::Node,
                route: "Person",
                selected_properties: None,
                uuids: None,
                limits: PropertyOverlayLimits::default(),
                collect: false,
            },
            Some(execution),
            |_| {
                emitted.set(emitted.get() + 1);
                Ok(true)
            },
        )
        .unwrap_err();
    assert_eq!(error.code(), "GF_PROJECT_CORRUPT");
    assert!(
        error
            .to_string()
            .contains("dictionary value count exceeds its page bytes"),
        "{error}"
    );
    assert_eq!(emitted.get(), 0);
    assert_eq!(pool.reserved(), 0);
}

#[test]
#[allow(
    deprecated,
    reason = "hostile raw PageHeader regression for Parquet 58"
)]
fn page_audit_rejects_size_mismatch_when_parquet_skips_decompression() {
    use parquet::basic::{Compression, Encoding};
    use parquet::file::properties::WriterVersion;
    use std::io::{Cursor, Seek, SeekFrom, Write};

    for (compression, version) in [
        (Compression::UNCOMPRESSED, WriterVersion::PARQUET_1_0),
        (
            Compression::ZSTD(Default::default()),
            WriterVersion::PARQUET_2_0,
        ),
    ] {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec!["hello"]))],
        )
        .unwrap();
        let file = tempfile::tempfile().unwrap();
        let properties = crate::permanent_parquet::writer_properties()
            .set_compression(compression)
            .set_writer_version(version)
            .set_encoding(Encoding::PLAIN)
            .set_dictionary_enabled(false)
            .build();
        let mut writer = ArrowWriter::try_new(
            file.try_clone().unwrap(),
            Arc::clone(&schema),
            Some(properties),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file.try_clone().unwrap()).unwrap();
        let metadata = Arc::clone(builder.metadata());
        parquet_resource_admission(
            &metadata,
            PropertyOverlayLimits::default(),
            &file,
            &ReadCounts::new(false),
            None,
            1,
            true,
            corrupt,
        )
        .unwrap();
        assert_eq!(builder.build().unwrap().next().unwrap().unwrap(), batch);
        let offset = u64::try_from(metadata.row_group(0).column(0).data_page_offset()).unwrap();
        let mut raw_file = file.try_clone().unwrap();
        raw_file.seek(SeekFrom::Start(offset)).unwrap();
        let mut raw = Vec::new();
        raw_file.read_to_end(&mut raw).unwrap();
        let mut cursor = Cursor::new(raw);
        let mut protocol = thrift::protocol::TCompactInputProtocol::new(&mut cursor);
        let mut header = parquet::format::PageHeader::read_from_in_protocol(&mut protocol).unwrap();
        drop(protocol);
        let header_len = usize::try_from(cursor.position()).unwrap();
        if version == WriterVersion::PARQUET_2_0 {
            assert_eq!(header.type_, parquet::format::PageType::DATA_PAGE_V2);
            assert_eq!(
                header.data_page_header_v2.as_ref().unwrap().is_compressed,
                Some(false)
            );
        }
        assert_eq!(header.compressed_page_size, header.uncompressed_page_size);
        header.uncompressed_page_size -= 1;
        let mut encoded = Vec::new();
        let mut output = thrift::protocol::TCompactOutputProtocol::new(&mut encoded);
        header.write_to_out_protocol(&mut output).unwrap();
        drop(output);
        assert_eq!(encoded.len(), header_len);
        raw_file.seek(SeekFrom::Start(offset)).unwrap();
        raw_file.write_all(&encoded).unwrap();
        raw_file.flush().unwrap();

        // Audit the genuine body using its smaller hostile size claim, without
        // driving an Arrow decoder that trusts the no-decompression header.
        let error = parquet_resource_admission(
            &metadata,
            PropertyOverlayLimits::default(),
            &file,
            &ReadCounts::new(false),
            None,
            1,
            true,
            corrupt,
        )
        .err()
        .expect("the no-decompression body must match its size claim");
        assert_eq!(error.code(), "GF_PROJECT_CORRUPT");
        assert!(
            error.to_string().contains("body and header sizes disagree"),
            "{error}"
        );
    }
}

#[test]
fn page_audit_rejects_cumulative_pages_larger_than_column_footer() {
    use parquet::basic::{Compression, Encoding};
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;

    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(vec![
            "first", "second", "third",
        ]))],
    )
    .unwrap();
    let file = tempfile::tempfile().unwrap();
    let properties = crate::permanent_parquet::writer_properties()
        .set_compression(Compression::UNCOMPRESSED)
        .set_encoding(Encoding::PLAIN)
        .set_dictionary_enabled(false)
        .set_write_batch_size(1)
        .set_data_page_row_count_limit(1)
        .build();
    let mut writer = ArrowWriter::try_new(
        file.try_clone().unwrap(),
        Arc::clone(&schema),
        Some(properties),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file.try_clone().unwrap()).unwrap();
    let metadata = builder.metadata().as_ref().clone();
    parquet_resource_admission(
        &metadata,
        PropertyOverlayLimits::default(),
        &file,
        &ReadCounts::new(false),
        None,
        2,
        true,
        corrupt,
    )
    .unwrap();
    assert_eq!(builder.build().unwrap().next().unwrap().unwrap(), batch);
    let physical = SerializedFileReader::new(file.try_clone().unwrap()).unwrap();
    let group = physical.get_row_group(0).unwrap();
    let mut pages = group.get_column_page_reader(0).unwrap();
    let mut page_count = 0;
    while pages.get_next_page().unwrap().is_some() {
        page_count += 1;
    }
    assert_eq!(
        page_count, 3,
        "the footer claim must cover multiple real pages"
    );
    let original = metadata.row_group(0).column(0);
    let understated = original
        .clone()
        .into_builder()
        .set_total_uncompressed_size(original.uncompressed_size() - 1)
        .build()
        .unwrap();
    let row_group = metadata
        .row_group(0)
        .clone()
        .into_builder()
        .set_column_metadata(vec![understated])
        .build()
        .unwrap();
    let metadata = metadata
        .into_builder()
        .set_row_groups(vec![row_group])
        .build();
    let error = parquet_resource_admission(
        &metadata,
        PropertyOverlayLimits::default(),
        &file,
        &ReadCounts::new(false),
        None,
        2,
        true,
        corrupt,
    )
    .err()
    .expect("all page sizes must fit the column footer claim");
    assert_eq!(error.code(), "GF_PROJECT_CORRUPT");
    assert!(
        error
            .to_string()
            .contains("page bytes exceed column uncompressed size"),
        "{error}"
    );
}
