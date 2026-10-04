//! Bit-exact Float64 properties through the public engine and interchange paths.

use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryBuilder, Float64Array, Int64Array, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use graphforge_api::{
    BulkInputKind, GraphForge, ImportSessionLimits, IrLiteral, OperationId, ResultSinkOptions,
};
#[cfg(feature = "portable")]
use graphforge_api::{
    PortableSelection, PortableV2ExportRequest, PortableV2ImportRequest, PortableV2Limits,
    PortableV2Output, PortableV2SelectionProfile,
};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

const PROJECTION: &str = "MATCH (n:Q) RETURN n.index AS index, n.x AS x ORDER BY index";

fn finite_values() -> Vec<f64> {
    let mut values = vec![
        f64::from(f32::MAX),
        0.0,
        -0.0,
        f64::from_bits(1),
        -f64::from_bits(1),
        f64::from_bits(f64::MIN_POSITIVE.to_bits() - 1),
        f64::MIN_POSITIVE,
        f64::MAX,
        -f64::MAX,
        0.1,
        1.0 / 3.0,
    ];
    let mut state = 0x7f10_a7b1_75ee_d123_u64;
    for _ in 0..64 {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let value = f64::from_bits(state);
        if value.is_finite() {
            values.push(value);
        }
    }
    values
}

fn write_values(forge: &GraphForge, values: &[f64]) {
    let rows = values
        .iter()
        .enumerate()
        .map(|(index, &value)| {
            IrLiteral::Map(vec![
                (
                    "index".into(),
                    IrLiteral::Int(i64::try_from(index).unwrap()),
                ),
                ("x".into(), IrLiteral::Float(value)),
            ])
        })
        .collect();
    forge
        .execute_with_params(
            "UNWIND $rows AS row CREATE (:Q {index: row.index, x: row.x})",
            &HashMap::from([("rows".into(), IrLiteral::List(rows))]),
        )
        .unwrap();
    forge
        .execute_with_params(
            "CREATE ()-[:R {x: $x}]->()",
            &HashMap::from([("x".into(), IrLiteral::Float(values[0]))]),
        )
        .unwrap();
}

fn rows(batches: &[RecordBatch]) -> Vec<(i64, u64)> {
    batches
        .iter()
        .flat_map(|batch| {
            let indices = batch
                .column_by_name("index")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let floats = batch
                .column_by_name("x")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            (0..batch.num_rows())
                .map(|row| {
                    assert!(!floats.is_null(row));
                    (indices.value(row), floats.value(row).to_bits())
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

fn expected(values: &[f64]) -> Vec<(i64, u64)> {
    values
        .iter()
        .enumerate()
        .map(|(index, value)| (i64::try_from(index).unwrap(), value.to_bits()))
        .collect()
}

fn verify_reads(forge: &GraphForge, values: &[f64], outputs: &Path) {
    assert_eq!(
        rows(&forge.execute(PROJECTION).unwrap().batches),
        expected(values)
    );
    let mut sorted = values.iter().enumerate().collect::<Vec<_>>();
    sorted.sort_by(|(left_index, left), (right_index, right)| {
        left.partial_cmp(right)
            .unwrap()
            .then(left_index.cmp(right_index))
    });
    let sorted = sorted
        .into_iter()
        .map(|(index, value)| (i64::try_from(index).unwrap(), value.to_bits()))
        .collect::<Vec<_>>();
    assert_eq!(
        rows(
            &forge
                .execute("MATCH (n:Q) RETURN n.index AS index, n.x AS x ORDER BY x, index")
                .unwrap()
                .batches
        ),
        sorted
    );
    for query in [
        "MATCH (n:Q) WHERE n.x = $x RETURN count(n) AS count",
        "MATCH (n:Q) WHERE n.x >= $x AND n.x <= $x RETURN count(n) AS count",
        "MATCH (n:Q) WHERE n.x = 3.4028234663852886e38 RETURN count(n) AS count",
        "MATCH ()-[r:R]->() WHERE r.x = $x RETURN count(r) AS count",
    ] {
        let result = forge
            .execute_with_params(
                query,
                &HashMap::from([("x".into(), IrLiteral::Float(values[0]))]),
            )
            .unwrap();
        assert_eq!(
            result.batches[0]
                .column_by_name("count")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            1,
            "{query}"
        );
    }
    let edges = forge.execute("MATCH ()-[r:R]->() RETURN r.x AS x").unwrap();
    assert_eq!(
        edges.batches[0]
            .column_by_name("x")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
            .to_bits(),
        values[0].to_bits()
    );

    let parquet = outputs.join("result.parquet");
    forge
        .execute_to_parquet_with_params(PROJECTION, &HashMap::new(), parquet.to_str().unwrap())
        .unwrap();
    let batches = ParquetRecordBatchReaderBuilder::try_new(File::open(parquet).unwrap())
        .unwrap()
        .build()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows(&batches), expected(values));
    let ipc = outputs.join("result.arrow");
    forge
        .execute_to_arrow_ipc_stream_with_params(
            PROJECTION,
            &HashMap::new(),
            ipc.to_str().unwrap(),
            &ResultSinkOptions::default(),
            None,
        )
        .unwrap();
    let batches = StreamReader::try_new(File::open(ipc).unwrap(), None)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows(&batches), expected(values));
}

#[cfg(feature = "portable")]
fn verify_portable_roundtrip(forge: &GraphForge, values: &[f64], root: &Path) {
    let package = root.join("graph.gfpb");
    forge
        .export_portable_v2(
            &PortableV2ExportRequest {
                selection: PortableSelection::Current,
                output_path: package.clone(),
                representation: PortableV2Output::Bundle,
                profile: PortableV2SelectionProfile::Complete,
                subset: None,
                limits: PortableV2Limits::default(),
            },
            None,
            |_| {},
        )
        .unwrap();
    let imported = root.join("imported");
    GraphForge::import_portable_v2(
        &imported,
        &PortableV2ImportRequest {
            input: package,
            operation_id: OperationId(uuid::Uuid::now_v7()),
            limits: PortableV2Limits::default(),
        },
        None,
    )
    .unwrap();
    let reopened = GraphForge::new(imported.to_str()).unwrap();
    verify_reads(&reopened, values, root);
}

#[test]
fn in_memory_float_properties_are_bit_exact() {
    let root = tempfile::tempdir().unwrap();
    let values = finite_values();
    let forge = GraphForge::new(None).unwrap();
    write_values(&forge, &values);
    verify_reads(&forge, &values, root.path());
    #[cfg(feature = "portable")]
    verify_portable_roundtrip(&forge, &values, root.path());
}

#[test]
fn durable_float_properties_are_bit_exact_after_reopen() {
    let root = tempfile::tempdir().unwrap();
    let project = root.path().join("project");
    let values = finite_values();
    let forge = GraphForge::new(project.to_str()).unwrap();
    write_values(&forge, &values);
    verify_reads(&forge, &values, root.path());
    drop(forge);
    let reopened = GraphForge::new(project.to_str()).unwrap();
    verify_reads(&reopened, &values, root.path());
    #[cfg(feature = "portable")]
    verify_portable_roundtrip(&reopened, &values, root.path());
}

#[test]
fn parquet_import_session_preserves_float_bits() {
    let root = tempfile::tempdir().unwrap();
    let values = finite_values();
    let mut identities = FixedSizeBinaryBuilder::with_capacity(values.len(), 16);
    for index in 0..values.len() {
        identities
            .append_value(uuid::Uuid::from_u128(u128::try_from(index).unwrap() + 1).as_bytes())
            .unwrap();
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("label", DataType::Utf8, false),
        Field::new("index", DataType::Int64, false),
        Field::new("x", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(identities.finish()) as ArrayRef,
            Arc::new(StringArray::from(vec!["Q"; values.len()])),
            Arc::new(Int64Array::from(
                (0..values.len())
                    .map(|index| i64::try_from(index).unwrap())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(values.clone())),
        ],
    )
    .unwrap();
    let input = root.path().join("input.parquet");
    let mut writer = ArrowWriter::try_new(File::create(&input).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let project = root.path().join("project");
    let forge = GraphForge::new(project.to_str()).unwrap();
    let mut session = forge
        .begin_import_session(
            OperationId(uuid::Uuid::now_v7()),
            ImportSessionLimits::default(),
        )
        .unwrap();
    session
        .register_parquet(BulkInputKind::Node, &input)
        .unwrap();
    session.validate(&forge).unwrap();
    session.commit(&forge, None).unwrap();
    drop(forge);
    let reopened = GraphForge::new(project.to_str()).unwrap();
    assert_eq!(
        rows(&reopened.execute(PROJECTION).unwrap().batches),
        expected(&values)
    );
    let result = reopened
        .execute("MATCH (n:Q) WHERE n.x = 3.4028234663852886e38 RETURN count(n) AS count")
        .unwrap();
    assert_eq!(
        result.batches[0]
            .column_by_name("count")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );
}
