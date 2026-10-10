use std::fs::File;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::file::metadata::{KeyValue, ParquetMetaData, ParquetMetaDataReader};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;

use super::{estimate_inferred_requests, preflight};
use crate::CancellationToken;
use crate::import_session::parquet_schema_envelope::SchemaTopologyFacts;

fn topology() -> SchemaTopologyFacts {
    SchemaTopologyFacts {
        nodes: 8,
        physical_leaves: 3,
        name_bytes: 48,
        group_child_slots: 5,
        path_string_slots: 12,
        path_name_bytes: 96,
        max_visited_depth: 3,
        max_definition_level: 2,
        max_repetition_level: 1,
        ..SchemaTopologyFacts::default()
    }
}

fn topology_for(metadata: &ParquetMetaData) -> SchemaTopologyFacts {
    fn visit(node: &parquet::schema::types::Type, depth: u64, facts: &mut SchemaTopologyFacts) {
        facts.nodes += 1;
        facts.name_bytes += u64::try_from(node.name().len()).unwrap();
        facts.max_visited_depth = facts.max_visited_depth.max(depth);
        if node.is_primitive() {
            facts.physical_leaves += 1;
            return;
        }
        let children = node.get_fields();
        facts.group_child_slots += u64::try_from(children.len()).unwrap();
        for child in children {
            visit(child, depth + 1, facts);
        }
    }

    let mut facts = SchemaTopologyFacts::default();
    visit(
        metadata.file_metadata().schema_descr().root_schema(),
        0,
        &mut facts,
    );
    facts
}

fn metadata_with_arrow_writer(schema: Schema) -> (tempfile::NamedTempFile, ParquetMetaData) {
    let file = tempfile::NamedTempFile::new().unwrap();
    let properties = WriterProperties::builder()
        .set_key_value_metadata(Some(vec![KeyValue::new(
            "custom-footer-key".to_owned(),
            "custom-footer-value".to_owned(),
        )]))
        .build();
    let writer =
        ArrowWriter::try_new(file.reopen().unwrap(), Arc::new(schema), Some(properties)).unwrap();
    writer.close().unwrap();
    let reader = ParquetMetaDataReader::new();
    let metadata = reader
        .parse_and_finish(&File::open(file.path()).unwrap())
        .unwrap();
    (file, metadata)
}

fn metadata_with_bad_hint(value: &str) -> (tempfile::NamedTempFile, ParquetMetaData) {
    let file = tempfile::NamedTempFile::new().unwrap();
    let schema = Arc::new(parse_message_type("message schema { required int64 value; }").unwrap());
    let properties = Arc::new(
        WriterProperties::builder()
            .set_key_value_metadata(Some(vec![KeyValue::new(
                "ARROW:schema".to_owned(),
                value.to_owned(),
            )]))
            .build(),
    );
    let writer = SerializedFileWriter::new(file.reopen().unwrap(), schema, properties).unwrap();
    writer.close().unwrap();
    let reader = ParquetMetaDataReader::new();
    let metadata = reader
        .parse_and_finish(&File::open(file.path()).unwrap())
        .unwrap();
    (file, metadata)
}

fn metadata_without_hint(message: &str) -> (tempfile::NamedTempFile, ParquetMetaData) {
    let file = tempfile::NamedTempFile::new().unwrap();
    let schema = Arc::new(parse_message_type(message).unwrap());
    let writer = SerializedFileWriter::new(
        file.reopen().unwrap(),
        schema,
        Arc::new(WriterProperties::builder().build()),
    )
    .unwrap();
    writer.close().unwrap();
    let reader = ParquetMetaDataReader::new();
    let metadata = reader
        .parse_and_finish(&File::open(file.path()).unwrap())
        .unwrap();
    (file, metadata)
}

#[test]
fn preflights_plain_and_nested_inferred_arrow_schemas() {
    let (_plain_file, plain_metadata) =
        metadata_without_hint("message schema { required int64 value; }");
    let plain_topology = topology_for(&plain_metadata);
    let plain_facts = preflight(&plain_metadata, plain_topology, 1 << 30, None).unwrap();
    assert_eq!(plain_facts.hint_decoded, 0);
    assert!(plain_facts.peak_request_bytes >= plain_facts.retained_request_bytes);
    ArrowReaderMetadata::try_new(Arc::new(plain_metadata.clone()), ArrowReaderOptions::new())
        .unwrap();

    let (_nested_file, nested_metadata) = metadata_without_hint(
        "message schema {
            optional group records (LIST) {
                repeated group list {
                    optional group record {
                        optional binary name (UTF8);
                        required double score;
                    }
                }
            }
        }",
    );
    let nested_topology = topology_for(&nested_metadata);
    assert!(nested_topology.nodes > plain_topology.nodes);
    assert!(nested_topology.group_child_slots > plain_topology.group_child_slots);
    let nested_facts = preflight(&nested_metadata, nested_topology, 1 << 30, None).unwrap();
    assert_eq!(nested_facts.hint_decoded, 0);
    assert!(nested_facts.peak_request_bytes >= nested_facts.retained_request_bytes);
    assert!(nested_facts.retained_request_bytes > plain_facts.retained_request_bytes);
    ArrowReaderMetadata::try_new(Arc::new(nested_metadata.clone()), ArrowReaderOptions::new())
        .unwrap();
}

#[test]
fn hint_names_and_arbitrary_metadata_are_admitted() {
    let long_name = "field_name".repeat(256);
    let field_metadata = [
        ("custom-field-key".to_owned(), "field-value".repeat(256)),
        ("another-key".to_owned(), "x".repeat(512)),
    ]
    .into_iter()
    .collect();
    let schema_metadata = [
        ("custom-schema-key".to_owned(), "schema-value".repeat(512)),
        ("owner".to_owned(), "metadata-owner".to_owned()),
    ]
    .into_iter()
    .collect();
    let schema = Schema::new_with_metadata(
        vec![Field::new(long_name.clone(), DataType::Utf8, true).with_metadata(field_metadata)],
        schema_metadata,
    );
    let (_file, metadata) = metadata_with_arrow_writer(schema);
    let facts = preflight(&metadata, topology(), 1 << 30, None).unwrap();
    assert!(facts.hint_decoded > 0);
    assert!(facts.retained_request_bytes >= facts.hint_decoded);
    ArrowReaderMetadata::try_new(Arc::new(metadata.clone()), ArrowReaderOptions::new()).unwrap();
}

#[test]
fn malformed_and_truncated_schema_hints_refuse_before_arrow_conversion() {
    for hint in ["abc", "AAAA", "AAAA===="] {
        let (_file, metadata) = metadata_with_bad_hint(hint);
        assert!(preflight(&metadata, topology(), 1 << 30, None).is_err());
    }
}

#[test]
fn peak_budget_is_checked_at_and_below_the_boundary() {
    let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
    let (_file, metadata) = metadata_with_arrow_writer(schema);
    let expected = preflight(&metadata, topology(), 1 << 30, None).unwrap();
    assert!(preflight(&metadata, topology(), expected.peak_request_bytes, None,).is_ok());
    assert!(expected.peak_request_bytes > 0);
    assert!(preflight(&metadata, topology(), expected.peak_request_bytes - 1, None,).is_err());
}

#[test]
fn cancellation_is_observed_before_schema_work() {
    let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
    let (_file, metadata) = metadata_with_arrow_writer(schema);
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    assert!(preflight(&metadata, topology(), 1 << 30, Some(&cancellation)).is_err());
}

#[test]
fn crs_annotation_payload_is_charged_proportionally() {
    let small = topology();
    let mut large = small;
    large.primitive_crs_clone_bytes = 32_768;
    large.group_crs_clone_bytes = 16_384;
    let small_bytes = estimate_inferred_requests(small, 0, 0, 0).unwrap();
    let large_bytes = estimate_inferred_requests(large, 0, 0, 0).unwrap();
    assert!(large_bytes >= small_bytes + 2 * (32_768 + 16_384));
}
