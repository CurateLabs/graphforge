//! Registered-source facade regressions for bounded bulk intake (#1918).

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryArray, FixedSizeBinaryBuilder, Int64Array, ListBuilder,
    StringArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::{FileWriter as IpcFileWriter, IpcWriteOptions};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

use super::bulk_source;
use super::test_fixtures::fixture;
use super::*;

const BULK_BUDGET: u64 = 4 << 30;
const CLOCK: i64 = 1_789_000_000_000_000;
const LARGE_BATCH_WINDOW: usize = 64 << 20;

#[derive(Clone, Copy)]
enum SourceFormat {
    Parquet { row_group_rows: usize },
    CompressedIpc,
}

struct Published {
    inventory: BTreeMap<String, (u64, String)>,
    query: String,
    catalog_ids: Vec<(String, String, u32)>,
    rows: usize,
}

fn v7(value: u128) -> Uuid {
    Uuid::from_u128((value << 80) | (0x7 << 76) | (0x2 << 62) | value)
}

fn node_batch(ids: &[Option<Uuid>], properties: Vec<(String, DataType, ArrayRef)>) -> RecordBatch {
    let mut uuid = FixedSizeBinaryBuilder::with_capacity(ids.len(), 16);
    for id in ids {
        match id {
            Some(id) => uuid.append_value(id.as_bytes()).unwrap(),
            None => uuid.append_null(),
        }
    }
    let mut fields = vec![
        Field::new("node_uuid", DataType::FixedSizeBinary(16), true),
        Field::new("label", DataType::Utf8, false),
    ];
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(uuid.finish()),
        Arc::new(StringArray::from(vec!["Person"; ids.len()])),
    ];
    for (name, data_type, array) in properties {
        fields.push(Field::new(name, data_type, true));
        columns.push(array);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

fn nullable_ids(rows: usize) -> Vec<Option<Uuid>> {
    (0..rows)
        .map(|row| (row % 5 == 0).then(|| v7(row as u128 + 10_000)))
        .collect()
}

fn write_source(batch: &RecordBatch, path: &Path, format: SourceFormat, batch_rows: usize) {
    match format {
        SourceFormat::Parquet { row_group_rows } => {
            let properties = WriterProperties::builder()
                .set_max_row_group_row_count(Some(row_group_rows))
                .set_dictionary_page_size_limit(32 << 20)
                .build();
            let mut writer = ArrowWriter::try_new(
                File::create(path).unwrap(),
                batch.schema(),
                Some(properties),
            )
            .unwrap();
            writer.write(batch).unwrap();
            writer.close().unwrap();
        }
        SourceFormat::CompressedIpc => {
            let properties = batch.schema().fields()[2..]
                .iter()
                .map(|field| field.as_ref().clone())
                .collect();
            let schema = crate::bulk_node_input_schema(properties).unwrap();
            let canonical_batch =
                RecordBatch::try_new(schema.clone(), batch.columns().to_vec()).unwrap();
            let options = IpcWriteOptions::default()
                .try_with_compression(Some(arrow::ipc::CompressionType::LZ4_FRAME))
                .unwrap();
            let mut writer =
                IpcFileWriter::try_new_with_options(File::create(path).unwrap(), &schema, options)
                    .unwrap();
            for first in (0..batch.num_rows()).step_by(batch_rows) {
                writer
                    .write(&canonical_batch.slice(first, batch_rows.min(batch.num_rows() - first)))
                    .unwrap();
            }
            writer.finish().unwrap();
        }
    }
}

fn register_compressed_ipc(session: &mut GraphImportSession, source: &Path, rows: u64) {
    let sequence = session.next_sequence().unwrap();
    let name = format!("{sequence:020}.arrow");
    let sources = session.root.join("sources");
    let destination = sources.join(&name);
    let temporary = sources.join(format!(".{name}.tmp"));
    let file = File::create(&temporary).unwrap();
    let mut cache_writer = graphforge_filesystem::DurableFileCacheWriter::new(file).unwrap();
    io::copy(&mut File::open(source).unwrap(), &mut cache_writer).unwrap();
    cache_writer.flush().unwrap();
    let seal =
        graphforge_storage::durable_commit::seal_cache_writer_witness(&mut cache_writer).unwrap();
    drop(cache_writer);
    session
        .publish_source(&temporary, &destination, seal)
        .unwrap();
    let bytes = destination.metadata().unwrap().len();
    session
        .register_record(ImportSourceKind::ArrowNodes, name, bytes, rows, None)
        .unwrap();
}

fn parquet_physical_rows(path: &Path, rows: usize, window: usize) -> u64 {
    let metadata = parquet::arrow::arrow_reader::ArrowReaderMetadata::load(
        &File::open(path).unwrap(),
        parquet::arrow::arrow_reader::ArrowReaderOptions::new(),
    )
    .unwrap();
    let scan = super::parquet_admission::SourceScan::build(
        &File::open(path).unwrap(),
        &metadata,
        u64::try_from(rows).unwrap(),
        BULK_BUDGET,
        u64::try_from(window).unwrap(),
        None,
    )
    .unwrap();
    scan.physical_batch_rows()
}

fn pin_clock(root: &Path) {
    let path = root.join("checkpoint.json");
    let mut checkpoint: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    checkpoint["session_now_micros"] = serde_json::json!(CLOCK);
    std::fs::write(path, serde_json::to_vec(&checkpoint).unwrap()).unwrap();
}

fn construction_root(graph: &GraphForge, session_uuid: Uuid) -> PathBuf {
    graph
        .resolved_generation
        .container_root()
        .join(".graphforge-construction")
        .join(session_uuid.simple().to_string())
}

fn inventory(root: &Path) -> BTreeMap<String, (u64, String)> {
    let contents: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("encoded-v1/inventory.json")).unwrap())
            .unwrap();
    contents["artifacts"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|artifact| artifact["path"] != "topology/uuid-membership/ordinal-v4-receipt.json")
        .map(|artifact| {
            (
                artifact["path"].as_str().unwrap().to_owned(),
                (
                    artifact["bytes"].as_u64().unwrap(),
                    artifact["sha256"].as_str().unwrap().to_owned(),
                ),
            )
        })
        .collect()
}

fn catalog_ids(graph: &GraphForge) -> Vec<(String, String, u32)> {
    let catalog = graph.runtime_catalog();
    let catalog = catalog.lock().unwrap();
    let mut ids = catalog
        .entity_type_names_with_ids()
        .map(|(id, name)| ("entity".to_owned(), name.to_owned(), id.get()))
        .chain(
            catalog
                .relation_type_names_with_ids()
                .map(|(id, name)| ("relation".to_owned(), name.to_owned(), id.get())),
        )
        .chain(
            catalog
                .property_names()
                .map(|(id, name)| ("property".to_owned(), name.to_owned(), id.get())),
        )
        .collect::<Vec<_>>();
    ids.sort();
    ids
}

fn query_snapshot(graph: &GraphForge, property: &str) -> String {
    let identities = graph
        .execute("MATCH (n:Person) RETURN n.node_uuid AS id ORDER BY id")
        .unwrap();
    let sample_ids = graph
        .execute("MATCH (n:Person) RETURN n.node_uuid AS id ORDER BY id LIMIT 3")
        .unwrap();
    let ids = sample_ids
        .batches
        .iter()
        .flat_map(|batch| {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .expect("node UUID query returns fixed-size binary");
            (0..ids.len()).map(move |row| {
                ids.value(row)
                    .try_into()
                    .expect("node UUID has sixteen bytes")
            })
        })
        .collect::<Vec<_>>();
    let samples = ids
        .into_iter()
        .map(|id| {
            let params = std::collections::HashMap::from([(
                "id".to_owned(),
                crate::IrLiteral::Uuid(id),
            )]);
            graph
                .execute_with_params(
                    &format!(
                        "MATCH (n:Person) WHERE n.node_uuid = $id RETURN n.node_uuid AS id, n.{property} AS value"
                    ),
                    &params,
                )
                .map(|result| pretty_format_batches(&result.batches).unwrap().to_string())
        })
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    format!(
        "{}\n{}",
        pretty_format_batches(&identities.batches).unwrap(),
        samples.join("\n")
    )
}

/// The published graph answers with the input's values: for every seventh row
/// that has an identity, the property read through a query equals the source's.
fn assert_values_match_input(graph: &GraphForge, batch: &RecordBatch, property: &str) {
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let values = batch.column_by_name(property).unwrap();
    let mut checked = 0;
    for row in (0..batch.num_rows())
        .filter(|row| ids.is_valid(*row))
        .step_by(7)
    {
        let id: [u8; 16] = ids.value(row).try_into().unwrap();
        let params =
            std::collections::HashMap::from([("id".to_owned(), crate::IrLiteral::Uuid(id))]);
        let result = graph
            .execute_with_params(
                &format!("MATCH (n:Person) WHERE n.node_uuid = $id RETURN n.{property} AS value"),
                &params,
            )
            .unwrap();
        assert_eq!(
            result
                .batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            1
        );
        let answered = &result.batches[0].column(0);
        let render = |array: &ArrayRef, row: usize| {
            array
                .is_valid(row)
                .then(|| arrow::util::display::array_value_to_string(array, row).unwrap())
        };
        assert_eq!(render(answered, 0), render(values, row), "row {row}");
        checked += 1;
    }
    assert!(checked > 0, "no row was compared");
}

fn publish(
    batch: &RecordBatch,
    format: SourceFormat,
    batch_rows: usize,
    property: &str,
) -> Published {
    let (_directory, project, graph) = fixture();
    let source_directory = tempfile::tempdir().unwrap();
    let source = source_directory.path().join(match format {
        SourceFormat::Parquet { .. } => "registered.parquet",
        SourceFormat::CompressedIpc => "registered.arrow",
    });
    write_source(batch, &source, format, batch_rows);
    let operation = OperationId(v7(1918));
    let mut session = graph
        .begin_import_session(
            operation,
            ImportSessionLimits {
                batch_rows,
                ..ImportSessionLimits::default()
            },
        )
        .unwrap();
    match format {
        SourceFormat::Parquet { .. } => session
            .register_parquet(BulkInputKind::Node, &source)
            .unwrap(),
        SourceFormat::CompressedIpc => {
            register_compressed_ipc(&mut session, &source, batch.num_rows() as u64);
        }
    }
    let construction = session.open_construction(&graph).unwrap();
    let root = construction_root(&graph, construction.session_uuid());
    drop(construction);
    pin_clock(&root);
    bulk_source::TEST_BUDGET.with(|cell| cell.set(Some(BULK_BUDGET)));
    let progress = session.validate(&graph);
    bulk_source::TEST_BUDGET.with(|cell| cell.set(None));
    let progress = progress.unwrap();
    let construction = progress.construction.unwrap();
    assert_eq!(progress.rows_accepted, batch.num_rows() as u64);
    assert!(construction.bulk_build.is_some());
    assert_eq!(construction.accepted_chunks, 0);
    session.commit(&graph, None).unwrap();
    assert_eq!(graph.node_count("Person").unwrap(), batch.num_rows() as u64);
    let current_query = query_snapshot(&graph, property);
    let current_catalog = catalog_ids(&graph);
    drop(session);
    drop(graph);

    let reopened = GraphForge::new(project.to_str()).unwrap();
    assert_eq!(
        reopened.node_count("Person").unwrap(),
        batch.num_rows() as u64
    );
    let reopened_query = query_snapshot(&reopened, property);
    assert_eq!(current_query, reopened_query);
    assert_values_match_input(&reopened, batch, property);
    let reopened_catalog = catalog_ids(&reopened);
    assert_eq!(current_catalog, reopened_catalog);
    assert!(
        current_catalog
            .iter()
            .any(|entry| entry.0 == "entity" && entry.1 == "Person")
    );
    assert!(
        current_catalog
            .iter()
            .any(|entry| entry.0 == "property" && entry.1 == property)
    );
    Published {
        inventory: inventory(&root),
        query: reopened_query,
        catalog_ids: reopened_catalog,
        rows: batch.num_rows(),
    }
}

/// Publish the source twice into fresh projects: each reads back as the input
/// (`publish`), and the two publish the same artifacts, answers and catalog ids.
fn publish_and_read_back(
    batch: &RecordBatch,
    format: SourceFormat,
    batch_rows: usize,
    property: &str,
) {
    let first = publish(batch, format, batch_rows, property);
    let second = publish(batch, format, batch_rows, property);
    assert_eq!(first.rows, second.rows);
    assert_eq!(first.inventory, second.inventory);
    assert_eq!(first.query, second.query);
    assert_eq!(first.catalog_ids, second.catalog_ids);
}

#[test]
fn large_unused_dictionary_values_publish_and_read_back_exactly() {
    let rows = 5_000;
    let properties = (0..16)
        .map(|column| {
            let large = "z".repeat(64 << 10);
            let values = (0..rows)
                .map(|row| {
                    if row == rows - 1 {
                        large.as_str()
                    } else {
                        ["a", "b", "c", "d", "e", "f", "g"][row % 7]
                    }
                })
                .collect::<Vec<_>>();
            (
                format!("p{column:02}"),
                DataType::Utf8,
                Arc::new(StringArray::from(values)) as ArrayRef,
            )
        })
        .collect();
    let batch = node_batch(&nullable_ids(rows), properties);
    assert!(batch.slice(0, 4_096).get_array_memory_size() < 8 << 20);
    // Keep the rare value in the same row-group dictionary while the first
    // task reads only the small values.
    publish_and_read_back(
        &batch,
        SourceFormat::Parquet {
            row_group_rows: rows,
        },
        256,
        "p00",
    );
}

#[test]
fn repeated_and_nested_properties_publish_and_read_back_exactly() {
    let rows = 512;
    let mut repeated = ListBuilder::new(StringBuilder::new());
    let value = "x".repeat(256);
    for _ in 0..rows {
        for _ in 0..512 {
            repeated.values().append_value(&value);
        }
        repeated.append(true);
    }
    let mut nested = ListBuilder::new(ListBuilder::new(arrow::array::Int64Builder::new()));
    for row in 0..rows {
        for part in 0..2 {
            nested
                .values()
                .values()
                .append_value((row * 2 + part) as i64);
            nested.values().append(true);
        }
        nested.append(true);
    }
    let repeated = Arc::new(repeated.finish()) as ArrayRef;
    let nested = Arc::new(nested.finish()) as ArrayRef;
    let batch = node_batch(
        &nullable_ids(rows),
        vec![
            (
                "children".to_owned(),
                repeated.data_type().clone(),
                repeated,
            ),
            ("nested".to_owned(), nested.data_type().clone(), nested),
        ],
    );
    assert!(batch.get_array_memory_size() > LARGE_BATCH_WINDOW);
    publish_and_read_back(
        &batch,
        SourceFormat::Parquet {
            row_group_rows: 113,
        },
        16,
        "children",
    );
}

#[test]
fn wide_small_properties_publish_and_read_back_exactly_when_split() {
    let rows = 4_096;
    let properties = (0..2_400)
        .map(|column| {
            (
                format!("p{column:04}"),
                DataType::Int64,
                Arc::new(Int64Array::from(
                    (0..rows)
                        .map(|row| (row + column) as i64)
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
            )
        })
        .collect();
    let batch = node_batch(&nullable_ids(rows), properties);
    assert!(batch.get_array_memory_size() > LARGE_BATCH_WINDOW + LARGE_BATCH_WINDOW / 8);
    let source_directory = tempfile::tempdir().unwrap();
    let source = source_directory.path().join("wide.parquet");
    write_source(
        &batch,
        &source,
        SourceFormat::Parquet {
            row_group_rows: rows,
        },
        rows,
    );
    let physical_rows = parquet_physical_rows(&source, rows, LARGE_BATCH_WINDOW);
    assert!(physical_rows < rows as u64);
    assert_eq!(u64::try_from(rows).unwrap() % physical_rows, 0);
    assert!(rows as u64 / physical_rows > 1);
    publish_and_read_back(
        &batch,
        SourceFormat::Parquet {
            row_group_rows: rows,
        },
        rows,
        "p0000",
    );
}

#[test]
fn compressed_ipc_publishes_and_reads_back_exactly_after_reopen() {
    let rows = 1_024;
    let values = (0..rows)
        .map(|row| format!("payload-{}", row % 11).repeat(32))
        .collect::<Vec<_>>();
    let values = Arc::new(StringArray::from(values)) as ArrayRef;
    let batch = node_batch(
        &nullable_ids(rows),
        vec![("payload".to_owned(), DataType::Utf8, values)],
    );
    publish_and_read_back(&batch, SourceFormat::CompressedIpc, 128, "payload");
}

#[test]
fn corrupt_compressed_ipc_length_is_refused_by_bounded_preflight_before_decode() {
    let source_directory = tempfile::tempdir().unwrap();
    let path = source_directory.path().join("corrupt.arrow");
    let rows = 1_000;
    let large = "x".repeat(4_096);
    let property = Arc::new(StringArray::from(vec![large.as_str(); rows])) as ArrayRef;
    let batch = node_batch(
        &nullable_ids(rows),
        vec![("payload".to_owned(), DataType::Utf8, property)],
    );
    write_source(&batch, &path, SourceFormat::CompressedIpc, rows);
    let mut bytes = std::fs::read(&path).unwrap();
    let expected = (rows * 4_096) as u64;
    let at = bytes
        .windows(12)
        .position(|window| {
            window[..8] == expected.to_le_bytes() && window[8..] == [0x04, 0x22, 0x4D, 0x18]
        })
        .expect("compressed values buffer advertises its decoded byte length");
    bytes[at..at + 8].copy_from_slice(&(8_u64 << 30).to_le_bytes());
    std::fs::write(&path, bytes).unwrap();

    let (_directory, _project, graph) = fixture();
    let mut session = graph
        .begin_import_session(
            OperationId(v7(1918)),
            ImportSessionLimits {
                batch_rows: rows,
                ..ImportSessionLimits::default()
            },
        )
        .unwrap();
    register_compressed_ipc(&mut session, &path, rows as u64);
    bulk_source::TEST_BUDGET.with(|cell| cell.set(Some(BULK_BUDGET)));
    let error = session.validate(&graph).unwrap_err();
    bulk_source::TEST_BUDGET.with(|cell| cell.set(None));
    assert!(matches!(
        error,
        GfError::Project {
            code: graphforge_core::ProjectErrorCode::ResourceLimit,
            ..
        }
    ));
}
