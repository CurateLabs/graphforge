use super::super::*;
use super::*;
use arrow::array::{ArrayRef, BooleanArray, FixedSizeBinaryArray, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use std::collections::{BTreeSet, HashMap};
use tempfile::TempDir;

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
