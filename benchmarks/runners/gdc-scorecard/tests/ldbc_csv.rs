//! LDBC CSV suite inputs: gzip-compressed Spark part files named by wildcard,
//! repeated header names, date/datetime/list properties in each LDBC
//! encoding, and a stored label taken from a column.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use arrow::array::{
    Array, BooleanArray, FixedSizeBinaryArray, Int64Array, ListArray, StringArray, StructArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, TimeUnit};
use arrow::record_batch::RecordBatch;
use gdc_scorecard::{Cause, ConvertError, convert, node_uuid};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{Value, json};

fn fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/gdc/ldbc-csv-fixture")
}

fn read(path: &Path) -> RecordBatch {
    let reader = ParquetRecordBatchReaderBuilder::try_new(fs::File::open(path).unwrap())
        .unwrap()
        .build()
        .unwrap();
    let batches: Vec<RecordBatch> = reader.map(Result::unwrap).collect();
    arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap()
}

fn column<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> &'a T {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("column {name}"))
        .as_any()
        .downcast_ref::<T>()
        .unwrap_or_else(|| panic!("column {name} type"))
}

fn output<'a>(manifest: &'a Value, table: &str) -> &'a Value {
    manifest["outputs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["table"] == table)
        .unwrap()
}

fn strings(list: &ListArray, row: usize) -> Option<Vec<String>> {
    (!list.is_null(row)).then(|| {
        let values = list.value(row);
        let values = values.as_any().downcast_ref::<StringArray>().unwrap();
        (0..values.len())
            .map(|index| values.value(index).to_owned())
            .collect()
    })
}

const BI: &str = "bi-sf0-composite-projected-fk/graphs/csv/bi/composite-projected-fk/initial_snapshot";

#[test]
fn fixture_reads_gzip_parts_by_wildcard_in_sorted_order_and_skips_side_files() {
    let scratch = tempfile::tempdir().unwrap();
    let mapping = fs::read(fixture().join("mapping.json")).unwrap();
    let manifest = convert(&mapping, &fixture(), &scratch.path().join("out"))
        .unwrap()
        .manifest;
    let person_inputs: Vec<&str> = manifest["inputs"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|input| input["table"] == "person")
        .map(|input| input["path"].as_str().unwrap())
        .collect();
    assert_eq!(
        person_inputs,
        [
            format!("{BI}/dynamic/Person/part-00000-fixture-c000.csv.gz"),
            format!("{BI}/dynamic/Person/part-00001-fixture-c000.csv.gz"),
            format!("{BI}/dynamic/Person/part-00002-fixture-c000.csv.gz"),
        ],
        "_SUCCESS and the hidden .crc side file are not inputs"
    );
    assert_eq!(manifest["inputs"].as_array().unwrap().len(), 10);
    for (table, rows) in [
        ("place", 4),
        ("person", 3),
        ("owner", 2),
        ("place_ispartof_place", 3),
        ("person_islocatedin_city", 3),
        ("person_knows_person", 2),
        ("person_knows_person_long_date", 1),
    ] {
        assert_eq!(output(&manifest, table)["rows"], rows, "{table}");
    }
    assert_eq!(
        output(&manifest, "place")["labels"],
        json!({"City": 2, "Continent": 1, "Country": 1})
    );
    assert_eq!(output(&manifest, "place")["label"], "Place");
    assert_eq!(output(&manifest, "person")["labels"], json!({"Person": 3}));
    assert!(output(&manifest, "person_knows_person").get("labels").is_none());
}

#[test]
fn stored_label_comes_from_the_column_and_identity_from_the_table_label() {
    let scratch = tempfile::tempdir().unwrap();
    let out = scratch.path().join("out");
    let mapping = fs::read(fixture().join("mapping.json")).unwrap();
    convert(&mapping, &fixture(), &out).unwrap();

    let places = read(&out.join("nodes/place.parquet"));
    let labels = column::<StringArray>(&places, "label");
    let ids = column::<Int64Array>(&places, "id");
    let uuids = column::<FixedSizeBinaryArray>(&places, "node_uuid");
    let by_id: Vec<(i64, &str)> = (0..places.num_rows())
        .map(|row| (ids.value(row), labels.value(row)))
        .collect();
    assert_eq!(
        by_id,
        [(0, "Country"), (1, "Continent"), (2, "City"), (3, "City")]
    );
    for row in 0..places.num_rows() {
        assert_eq!(uuids.value(row), node_uuid("Place", ids.value(row)));
    }

    // An endpoint names the identity label, so it reaches the City node.
    let located = read(&out.join("edges/person_islocatedin_city.parquet"));
    let targets = column::<FixedSizeBinaryArray>(&located, "target_uuid");
    assert_eq!(targets.value(0), node_uuid("Place", 2));
}

#[test]
fn temporal_and_list_properties_use_the_canonical_graphforge_types() {
    let scratch = tempfile::tempdir().unwrap();
    let out = scratch.path().join("out");
    let mapping = fs::read(fixture().join("mapping.json")).unwrap();
    convert(&mapping, &fixture(), &out).unwrap();

    let person = read(&out.join("nodes/person.parquet"));
    let schema = person.schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        names,
        [
            "node_uuid",
            "label",
            "birthday",
            "creationDate",
            "email",
            "firstName",
            "id",
            "speaks"
        ]
    );
    assert_eq!(
        schema.field_with_name("birthday").unwrap().data_type(),
        &DataType::Struct(vec![Field::new("epoch_day", DataType::Int64, true)].into())
    );
    assert_eq!(
        schema.field_with_name("creationDate").unwrap().data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    assert_eq!(
        schema.field_with_name("speaks").unwrap().data_type(),
        &DataType::List(Field::new("item", DataType::Utf8, true).into())
    );

    let ids = column::<Int64Array>(&person, "id");
    assert_eq!(
        (0..3).map(|row| ids.value(row)).collect::<Vec<_>>(),
        [14, 15, 16]
    );
    let birthday = column::<StructArray>(&person, "birthday");
    let days = birthday
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    // 1984-03-11, 1815-12-10 and 1906-12-09 as days since 1970-01-01.
    assert_eq!(
        (0..3).map(|row| days.value(row)).collect::<Vec<_>>(),
        [5_183, -56_270, -23_034]
    );
    let created = column::<TimestampMicrosecondArray>(&person, "creationDate");
    assert_eq!(created.value(0), 1_262_531_431_499_000);
    let speaks = column::<ListArray>(&person, "speaks");
    let email = column::<ListArray>(&person, "email");
    assert_eq!(
        strings(speaks, 0),
        Some(vec!["fa".into(), "ku".into(), "en".into()])
    );
    assert_eq!(
        strings(email, 1),
        Some(vec!["ada@example.org".into(), "ada@example.net".into()])
    );
    assert_eq!(strings(speaks, 2), None, "an empty list field is null");
    assert_eq!(strings(email, 2), None);

    // FinBench: naive UTC datetimes with a short fraction, a midnight date.
    let owner = read(&out.join("nodes/owner.parquet"));
    let created = column::<TimestampMicrosecondArray>(&owner, "createTime");
    assert_eq!(created.value(0), 1_577_837_161_273_000);
    assert_eq!(created.value(1), 1_588_713_409_460_000);
    let birthday = column::<StructArray>(&owner, "birthday");
    let days = birthday
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(days.value(0), 6_864);
    let blocked = column::<BooleanArray>(&owner, "isBlocked");
    assert!(!blocked.value(0) && blocked.value(1));

    // Interactive v1: `Person.id|Person.id` reads as Person.id, Person.id.1.
    let knows = read(&out.join("edges/person_knows_person_long_date.parquet"));
    assert_eq!(
        column::<FixedSizeBinaryArray>(&knows, "source_uuid").value(0),
        node_uuid("Person", 16)
    );
    assert_eq!(
        column::<FixedSizeBinaryArray>(&knows, "target_uuid").value(0),
        node_uuid("Person", 14)
    );
    assert_eq!(
        column::<TimestampMicrosecondArray>(&knows, "creationDate").value(0),
        1_268_465_841_718_000
    );
}

struct Workspace {
    _dir: tempfile::TempDir,
    input: PathBuf,
    out: PathBuf,
}

fn gzip(text: &str) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(text.as_bytes()).unwrap();
    encoder.finish().unwrap()
}

fn workspace(files: &[(&str, Vec<u8>)]) -> Workspace {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in");
    for (name, body) in files {
        let path = input.join(name);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, body).unwrap();
    }
    let out = dir.path().join("out");
    Workspace {
        _dir: dir,
        input,
        out,
    }
}

fn run(files: &[(&str, Vec<u8>)], mapping: &str) -> Result<Value, ConvertError> {
    let ws = workspace(files);
    convert(mapping.as_bytes(), &ws.input, &ws.out).map(|conversion| conversion.manifest)
}

const PLACE: &str = r#"{
  "schema": "graphforge-gdc-load-mapping/1",
  "node_tables": [{"id": "place", "format": "ldbc-csv", "files": ["Place/*.csv.gz"],
    "label": "Place", "id_column": "id", "label_column": "type",
    "label_values": {"city": "City", "country": "Country"},
    "properties": [{"column": "founded", "type": "date"},
                   {"column": "seen", "type": "datetime"},
                   {"column": "tags", "type": "list", "separator": ";"}]}]
}"#;

fn place(rows: &str) -> Vec<(&'static str, Vec<u8>)> {
    vec![(
        "Place/part-00000.csv.gz",
        gzip(&format!("id|type|founded|seen|tags\n{rows}")),
    )]
}

#[test]
fn gzip_input_converts_to_the_same_bytes_as_the_plain_file() {
    let rows = "1|city|1900-01-01|2010-01-03T15:10:41.499+00:00|a;b\n2|country||2010-01-03T15:10:41Z|\n";
    let plain = PLACE.replace("Place/*.csv.gz", "Place/*.csv");
    assert_ne!(plain, PLACE);
    let from_gzip = run(&place(rows), PLACE).unwrap();
    let from_plain = run(
        &[(
            "Place/part-00000.csv",
            format!("id|type|founded|seen|tags\n{rows}").into_bytes(),
        )],
        &plain,
    )
    .unwrap();
    assert_eq!(from_gzip["outputs"], from_plain["outputs"]);
    assert_eq!(output(&from_gzip, "place")["rows"], 2);
}

#[test]
fn values_outside_the_declared_encoding_fail_typed() {
    for (rows, why) in [
        ("1|planet|||\n", "label value outside label_values"),
        ("1||||\n", "empty label value"),
        ("1|city|1900-02-30||\n", "date that does not exist"),
        ("1|city|1900-01-01 00:00:00||\n", "date in another format"),
        ("1|city||2010-01-03T15:10:41.499|\n", "datetime without offset"),
        ("1|city||2010-01-03 15:10:41.499+00:00|\n", "datetime without T"),
        ("1|city|||a;;b\n", "empty list item"),
    ] {
        let error = run(&place(rows), PLACE).unwrap_err();
        assert_eq!(error.cause(), Cause::InvalidValue, "{why}: {error}");
        assert!(error.message().contains("row 1"), "{why}: {error}");
    }
}

#[test]
fn undecodable_gzip_and_ambiguous_headers_are_malformed_input() {
    let corrupt = vec![("Place/part-00000.csv.gz", b"not gzip".to_vec())];
    let error = run(&corrupt, PLACE).unwrap_err();
    assert_eq!(error.cause(), Cause::MalformedInput, "{error}");

    let mut truncated = gzip("id|type|founded|seen|tags\n1|city|||\n");
    truncated.truncate(truncated.len() - 6);
    let error = run(&[("Place/part-00000.csv.gz", truncated)], PLACE).unwrap_err();
    assert_eq!(error.cause(), Cause::MalformedInput, "{error}");

    let mapping = r#"{"schema": "graphforge-gdc-load-mapping/1",
      "node_tables": [{"id": "p", "format": "ldbc-csv", "files": ["p.csv"],
        "label": "P", "id_column": "a"}]}"#;
    let error = run(&[("p.csv", b"a|a|a.1\n1|2|3\n".to_vec())], mapping).unwrap_err();
    assert_eq!(error.cause(), Cause::MalformedInput, "{error}");
    run(&[("p.csv", b"a|a|a\n1|2|3\n".to_vec())], mapping).unwrap();
}

#[test]
fn patterns_that_match_nothing_or_overlap_are_refused() {
    let error = run(&[("Other/part-00000.csv.gz", gzip("id\n"))], PLACE).unwrap_err();
    assert_eq!(error.cause(), Cause::InputMissing, "{error}");

    let overlapping = PLACE.replace(
        r#""files": ["Place/*.csv.gz"]"#,
        r#""files": ["Place/*.csv.gz", "Place/part-*"]"#,
    );
    assert_ne!(overlapping, PLACE);
    let error = run(&place("1|city|||\n"), &overlapping).unwrap_err();
    assert_eq!(error.cause(), Cause::InvalidMapping, "{error}");
}

#[test]
fn mapping_options_must_fit_their_property_type() {
    for (bad, why) in [
        (
            PLACE.replace(r#""label_values": {"city": "City", "country": "Country"},"#, ""),
            "label_column without label_values",
        ),
        (
            PLACE.replace(r#""label_values": {"city": "City", "country": "Country"}"#, r#""label_values": {}"#),
            "empty label_values",
        ),
        (
            PLACE.replace(r#""type": "list", "separator": ";""#, r#""type": "list""#),
            "list without separator",
        ),
        (
            PLACE.replace(r#""separator": ";""#, r#""separator": "|""#),
            "pipe separator",
        ),
        (
            PLACE.replace(r#""separator": ";""#, r#""separator": ";;""#),
            "two-character separator",
        ),
        (
            PLACE.replace(r#""type": "date""#, r#""type": "date", "separator": ";""#),
            "separator on a date",
        ),
        (
            PLACE.replace(r#""type": "list", "separator": ";""#, r#""type": "string", "format": "iso8601""#),
            "format on a string",
        ),
        (
            PLACE.replace(r#""type": "datetime""#, r#""type": "datetime", "format": "rfc2822""#),
            "unknown format",
        ),
    ] {
        assert_ne!(bad, PLACE, "{why}: the replacement must change the mapping");
        let error = run(&place("1|city|||\n"), &bad).unwrap_err();
        assert_eq!(error.cause(), Cause::InvalidMapping, "{why}: {error}");
    }
}
