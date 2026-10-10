use super::super::inventory::MAX_IN_MEMORY_SNAPSHOT_BYTES;
use super::super::*;
use super::*;
use arrow::array::{FixedSizeBinaryArray, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use std::io::{Seek, SeekFrom, Write as _};
use tempfile::TempDir;

fn random_ascii(length: usize) -> String {
    let mut state = 0x9e37_79b9_u32;
    (0..length)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            char::from(b'a' + u8::try_from(state % 26).unwrap())
        })
        .collect()
}

fn legacy_inventory(root: &std::path::Path, value: &str) -> AuthenticatedPropertyInventory {
    let path = root.join("properties/Person.parquet");
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(FixedSizeBinaryArray::try_from_iter([vec![17_u8; 16]].into_iter()).unwrap()),
            Arc::new(StringArray::from(vec![value])),
        ],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(
        File::create(&path).unwrap(),
        schema,
        Some(
            WriterProperties::builder()
                .set_compression(parquet::basic::Compression::UNCOMPRESSED)
                .set_dictionary_enabled(false)
                .set_statistics_enabled(EnabledStatistics::None)
                .build(),
        ),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let bytes = fs::read(&path).unwrap();
    let entry = crate::GraphFileEntry {
        relative_path: "properties/Person.parquet".into(),
        byte_length: u64::try_from(bytes.len()).unwrap(),
        content_xxh64: crate::corruption_checksum::checksum(&bytes),
        content_sha256: super::super::digest_hex(&Sha256::digest(&bytes)),
        role: crate::GraphFileRole::Properties,
    };
    AuthenticatedPropertyInventory::from_entries_at_root(root, vec![entry]).unwrap()
}

#[test]
fn oversized_legacy_route_read_streams_without_writes_or_scratch() {
    let root = TempDir::new().unwrap();
    let payload = random_ascii(5 * 1024 * 1024);
    let inventory = legacy_inventory(root.path(), &payload);
    assert!(
        fs::metadata(root.path().join("properties/Person.parquet"))
            .unwrap()
            .len()
            > MAX_IN_MEMORY_SNAPSHOT_BYTES
    );
    let selected = BTreeSet::from(["payload".to_owned()]);
    let _capture = crate::lifecycle_io::CaptureScope::install();
    let mut rows = Vec::new();
    let metrics = inventory
        .visit_route_streaming(
            &RouteRead {
                kind: PropertyRouteKind::Node,
                route: "Person",
                selected_properties: Some(&selected),
                uuids: None,
                limits: PropertyOverlayLimits::default(),
                collect: true,
            },
            |row| {
                rows.push(row);
                Ok(true)
            },
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].uuid, [17; 16]);
    assert_eq!(rows[0].values["payload"], IrLiteral::Str(payload));
    assert!(
        metrics.authentication_bytes
            >= fs::metadata(root.path().join("properties/Person.parquet"))
                .unwrap()
                .len()
    );
    let region = crate::lifecycle_io::snapshot().expect("requested lifecycle measurement");
    let reads = &region.phases[&crate::StorageIoPhase::ReadPathScan];
    assert_eq!(reads.write_bytes, 0);
    assert_eq!(reads.write_calls, 0);
    assert_eq!(reads.object_count, 0);
    assert!(
        fs::read_dir(root.path())
            .unwrap()
            .filter_map(Result::ok)
            .all(|entry| !entry
                .file_name()
                .to_string_lossy()
                .starts_with(".gf-property-scratch-"))
    );
}

#[test]
fn oversized_legacy_property_table_sql_reads_without_writes_or_scratch() {
    let root = TempDir::new().unwrap();
    let payload = random_ascii(5 * 1024 * 1024);
    let inventory = Arc::new(legacy_inventory(root.path(), &payload));
    assert!(
        fs::metadata(root.path().join("properties/Person.parquet"))
            .unwrap()
            .len()
            > MAX_IN_MEMORY_SNAPSHOT_BYTES
    );

    let context = datafusion::prelude::SessionContext::new();
    context
        .register_table(
            "props",
            Arc::new(
                crate::catalog::PropertyTable::open_authenticated(root.path(), "Person", inventory)
                    .unwrap(),
            ),
        )
        .unwrap();
    let _capture = crate::lifecycle_io::CaptureScope::install();
    let batches = tokio::runtime::Runtime::new().unwrap().block_on(async {
        context
            .sql("SELECT payload, node_uuid FROM props")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
    });
    let result = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(
        result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        payload
    );
    assert_eq!(
        result
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(0),
        &[17; 16]
    );
    let region = crate::lifecycle_io::snapshot().expect("requested lifecycle measurement");
    let reads = &region.phases[&crate::StorageIoPhase::ReadPathScan];
    assert_eq!(reads.write_bytes, 0);
    assert_eq!(reads.write_calls, 0);
    assert_eq!(reads.object_count, 0);
    assert!(
        fs::read_dir(root.path())
            .unwrap()
            .filter_map(Result::ok)
            .all(|entry| !entry
                .file_name()
                .to_string_lossy()
                .starts_with(".gf-property-scratch-"))
    );
}

#[test]
fn uncached_mutated_block_fails_after_whole_file_index_is_cached() {
    let root = TempDir::new().unwrap();
    let payload = random_ascii(5 * 1024 * 1024);
    let inventory = legacy_inventory(root.path(), &payload);
    let fragment = &inventory.routes[&(PropertyRouteKind::Node, "Person".to_owned())][0];
    let scratch = inventory.lazy_snapshot_scratch().unwrap();
    let opened = inventory
        .open_fragment(fragment, SnapshotScratch::Lazy(&scratch))
        .unwrap();
    assert!(fragment.readonly_index.get().is_some());

    let counts = ReadCounts::new(false);
    let source = CountingChunkReader {
        file: Arc::clone(&opened.file),
        length: opened.logical_length,
        counts,
    };
    let builder = ParquetRecordBatchReaderBuilder::try_new(source).unwrap();
    let first_page = builder.metadata().row_group(0).column(1).data_page_offset();
    let mut reader = builder.with_batch_size(4096).build().unwrap();

    let file_path = root.path().join("properties/Person.parquet");
    let mut writer = File::options()
        .read(true)
        .write(true)
        .open(file_path)
        .unwrap();
    writer
        .seek(SeekFrom::Start(u64::try_from(first_page).unwrap()))
        .unwrap();
    writer.write_all(&[b'z']).unwrap();
    let error = reader.next().unwrap().unwrap_err();
    assert!(matches!(
        super::super::authenticated_arrow_error(error),
        GfError::Project {
            code: graphforge_core::ProjectErrorCode::ProjectCorrupt,
            ..
        }
    ));
}

#[test]
fn block_index_reservation_refuses_before_index_allocation() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let file = File::open(temp.path()).unwrap();
    let entry = crate::GraphReadFileEntry {
        relative_path: "properties/large.parquet".into(),
        byte_length: 40 * 1024 * 1024 * 1024 * 1024,
        content_xxh64: 0,
        role: crate::GraphFileRole::Properties,
    };
    let error = open(
        &file,
        graphforge_filesystem::file_identity(&file).unwrap(),
        &entry,
        PropertyOverlayLimits::default().max_buffered_bytes,
        &OnceLock::new(),
    )
    .unwrap_err();
    assert!(matches!(
        error,
        GfError::Project {
            code: graphforge_core::ProjectErrorCode::ResourceLimit,
            ..
        }
    ));
}
