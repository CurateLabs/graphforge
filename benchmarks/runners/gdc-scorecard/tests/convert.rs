use std::fs;
use std::path::{Path, PathBuf};

use arrow::array::{Array, FixedSizeBinaryArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use gdc_scorecard::{Cause, convert, node_uuid};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::Value;

fn fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/gdc/load-fixture")
}

fn read(path: &Path) -> (arrow::datatypes::SchemaRef, Vec<RecordBatch>) {
    let builder = ParquetRecordBatchReaderBuilder::try_new(fs::File::open(path).unwrap()).unwrap();
    let schema = builder.schema().clone();
    let batches = builder.build().unwrap().map(Result::unwrap).collect();
    (schema, batches)
}

fn convert_fixture(out: &Path) -> Value {
    let mapping = fs::read(fixture().join("mapping.json")).unwrap();
    convert(&mapping, &fixture(), out).unwrap().manifest
}

fn rows_of(manifest: &Value, table: &str) -> u64 {
    manifest["outputs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|output| output["table"] == table)
        .unwrap()["rows"]
        .as_u64()
        .unwrap()
}

#[test]
fn fixture_converts_to_the_register_parquet_layout() {
    let scratch = tempfile::tempdir().unwrap();
    let out = scratch.path().join("out");
    let manifest = convert_fixture(&out);
    for (table, rows) in [
        ("vertex", 5),
        ("person", 3),
        ("place", 2),
        ("organisation", 1),
        ("link", 4),
        ("knows", 3),
    ] {
        assert_eq!(rows_of(&manifest, table), rows, "{table}");
    }
    assert_eq!(manifest["inputs"].as_array().unwrap().len(), 6);

    let (schema, batches) = read(&out.join("nodes/person.parquet"));
    let names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        names,
        [
            "node_uuid",
            "label",
            "active",
            "age",
            "first_name",
            "id",
            "score"
        ]
    );
    assert_eq!(schema.field(0).data_type(), &DataType::FixedSizeBinary(16));
    assert!(!schema.field(1).is_nullable());
    let batch = &batches[0];
    let uuids = batch
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(uuids.value(0), node_uuid("Person", 1));
    let labels = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!((0..3).all(|row| labels.value(row) == "Person"));
    let ages = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ages.value(0), 30);
    assert!(ages.is_null(2), "empty field is null");
    let scores = batch
        .column(6)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(scores.value(1), 2.5);

    let (schema, batches) = read(&out.join("edges/knows.parquet"));
    let names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        names,
        [
            "edge_uuid",
            "rel_type",
            "source_uuid",
            "target_uuid",
            "creation_date",
            "strength"
        ]
    );
    let batch = &batches[0];
    let sources = batch
        .column(2)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let targets = batch
        .column(3)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(sources.value(2), node_uuid("Person", 1));
    assert_eq!(targets.value(2), node_uuid("Person", 3));
    let strength = batch
        .column(5)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(strength.value(1), 7);
    assert!(strength.is_null(2));

    // The Graphalytics isolated vertex is a node with no edge.
    let (_, batches) = read(&out.join("edges/link.parquet"));
    let sources = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let isolated = node_uuid("Vertex", 5);
    assert!((0..sources.len()).all(|row| sources.value(row) != isolated));
    assert!(out.join("conversion-manifest.json").is_file());
    assert!(fs::read_dir(out.join("nodes")).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with(".partial")
    }));
}

#[test]
fn conversion_is_byte_deterministic() {
    let scratch = tempfile::tempdir().unwrap();
    let first = convert_fixture(&scratch.path().join("a"));
    let second = convert_fixture(&scratch.path().join("b"));
    assert_eq!(first, second, "manifest digests must not depend on the run");
}

struct Workspace {
    _dir: tempfile::TempDir,
    input: PathBuf,
    out: PathBuf,
}

fn workspace(files: &[(&str, &str)]) -> Workspace {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in");
    fs::create_dir_all(&input).unwrap();
    for (name, body) in files {
        fs::write(input.join(name), body).unwrap();
    }
    let out = dir.path().join("out");
    Workspace {
        _dir: dir,
        input,
        out,
    }
}

const PERSON_MAPPING: &str = r#"{
  "schema": "graphforge-gdc-load-mapping/1",
  "node_tables": [{"id": "person", "format": "ldbc-csv", "files": ["person.csv"],
    "label": "Person", "id_column": "id",
    "properties": [{"column": "age", "type": "int64"}]}],
  "edge_tables": [{"id": "knows", "format": "ldbc-csv", "files": ["knows.csv"],
    "rel_type": "KNOWS", "source": {"label": "Person", "column": "a"},
    "target": {"label": "Person", "column": "b"}}]
}"#;

fn run(files: &[(&str, &str)], mapping: &str) -> Result<(), gdc_scorecard::ConvertError> {
    let ws = workspace(files);
    convert(mapping.as_bytes(), &ws.input, &ws.out).map(|_| ())
}

#[test]
fn duplicate_label_and_id_fails_typed_even_across_files() {
    let error = run(
        &[
            ("person.csv", "id|age\n1|10\n2|20\n1|30\n"),
            ("knows.csv", "a|b\n"),
        ],
        PERSON_MAPPING,
    )
    .unwrap_err();
    assert_eq!(error.cause(), Cause::DuplicateNodeIdentity);
    assert!(error.message().contains("(Person, 1)"), "{error}");
    assert!(error.message().contains("row 3"), "{error}");

    let mapping = PERSON_MAPPING.replace(
        r#""files": ["person.csv"]"#,
        r#""files": ["person.csv", "person2.csv"]"#,
    );
    let error = run(
        &[
            ("person.csv", "id|age\n1|10\n"),
            ("person2.csv", "id|age\n1|11\n"),
            ("knows.csv", "a|b\n"),
        ],
        &mapping,
    )
    .unwrap_err();
    assert_eq!(error.cause(), Cause::DuplicateNodeIdentity);
}

#[test]
fn same_numeric_id_under_different_labels_is_not_a_duplicate() {
    let mapping = r#"{
      "schema": "graphforge-gdc-load-mapping/1",
      "node_tables": [
        {"id": "person", "format": "ldbc-csv", "files": ["person.csv"], "label": "Person", "id_column": "id"},
        {"id": "place", "format": "ldbc-csv", "files": ["place.csv"], "label": "Place", "id_column": "id"}
      ]}"#;
    run(
        &[("person.csv", "id\n1\n"), ("place.csv", "id\n1\n")],
        mapping,
    )
    .unwrap();
}

#[test]
fn dangling_endpoint_invalid_value_and_missing_column_fail_typed() {
    let nodes = ("person.csv", "id|age\n1|10\n2|20\n");
    let error = run(&[nodes, ("knows.csv", "a|b\n1|9\n")], PERSON_MAPPING).unwrap_err();
    assert_eq!(error.cause(), Cause::DanglingEndpoint);
    let error = run(
        &[("person.csv", "id|age\n1|old\n"), ("knows.csv", "a|b\n")],
        PERSON_MAPPING,
    )
    .unwrap_err();
    assert_eq!(error.cause(), Cause::InvalidValue);
    let error = run(
        &[("person.csv", "id\n1\n"), ("knows.csv", "a|b\n")],
        PERSON_MAPPING,
    )
    .unwrap_err();
    assert_eq!(error.cause(), Cause::MissingColumn);
    let error = run(&[("person.csv", "id|age\n|5\n")], PERSON_MAPPING).unwrap_err();
    assert_eq!(error.cause(), Cause::InvalidValue);
    let error = run(&[("knows.csv", "a|b\n")], PERSON_MAPPING).unwrap_err();
    assert_eq!(error.cause(), Cause::InputMissing);
}

#[test]
fn graphalytics_edges_keep_one_row_per_listed_pair_and_vertices_may_be_isolated() {
    let mapping = r#"{
      "schema": "graphforge-gdc-load-mapping/1",
      "node_tables": [{"id": "v", "format": "graphalytics-vertices", "files": ["g.v"], "label": "V", "id_column": "id"}],
      "edge_tables": [{"id": "e", "format": "graphalytics-edges", "files": ["g.e"], "rel_type": "E",
        "source": {"label": "V", "column": "source"}, "target": {"label": "V", "column": "target"}}]}"#;
    let ws = workspace(&[("g.v", "1\n2\n3\n"), ("g.e", "1 2\n2   1\n1\t2\n")]);
    let manifest = convert(mapping.as_bytes(), &ws.input, &ws.out)
        .unwrap()
        .manifest;
    assert_eq!(rows_of(&manifest, "v"), 3);
    assert_eq!(
        rows_of(&manifest, "e"),
        3,
        "parallel and reverse pairs are all kept"
    );

    let ws = workspace(&[("g.v", "1\n2\n"), ("g.e", "1 2 0.5\n2 1\n")]);
    let error = convert(mapping.as_bytes(), &ws.input, &ws.out).unwrap_err();
    assert_eq!(error.cause(), Cause::MalformedInput);
}

#[test]
fn invalid_mappings_and_existing_output_are_refused() {
    let error = run(&[], r#"{"schema": "x", "node_tables": []}"#).unwrap_err();
    assert_eq!(error.cause(), Cause::InvalidMapping);
    for bad in [
        PERSON_MAPPING.replace(
            r#""files": ["person.csv"]"#,
            r#""files": ["../person.csv"]"#,
        ),
        PERSON_MAPPING.replace(
            r#""label": "Person", "column""#,
            r#""label": "Ghost", "column""#,
        ),
        PERSON_MAPPING.replace(
            r#"{"column": "age", "type": "int64"}"#,
            r#"{"column": "age", "name": "label", "type": "int64"}"#,
        ),
        PERSON_MAPPING.replace(r#""id": "knows""#, r#""id": "person""#),
    ] {
        let error = run(&[], &bad).unwrap_err();
        assert_eq!(error.cause(), Cause::InvalidMapping, "{bad}");
    }
    let ws = workspace(&[("person.csv", "id|age\n1|1\n"), ("knows.csv", "a|b\n")]);
    fs::create_dir_all(&ws.out).unwrap();
    fs::write(ws.out.join("stale"), b"x").unwrap();
    let error = convert(PERSON_MAPPING.as_bytes(), &ws.input, &ws.out).unwrap_err();
    assert_eq!(error.cause(), Cause::OutputExists);
}
