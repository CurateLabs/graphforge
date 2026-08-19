//! Regenerate producer-neutral temporal IPC and Parquet conformance files.

use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use graphforge_api::{GraphForge, PropValue, TemporalValue};
use parquet::arrow::ArrowWriter;

fn main() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let contract: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(root.join("tests/contracts/temporal-interchange-v1.json"))
            .expect("read contract"),
    )
    .expect("parse contract");
    let cases = contract["cases"].as_array().expect("cases");
    let properties = cases
        .iter()
        .map(|case| {
            let value: TemporalValue =
                serde_json::from_value(case["value"].clone()).expect("temporal case");
            value.validate().expect("valid temporal case");
            (
                case["name"].as_str().expect("case name").to_owned(),
                PropValue::Temporal(value),
            )
        })
        .collect::<HashMap<_, _>>();
    let graph = GraphForge::new(None).expect("graph");
    graph.add_node("Temporal", &properties).expect("data row");
    graph
        .add_node("Temporal", &HashMap::new())
        .expect("null row");
    let projection = cases
        .iter()
        .map(|case| {
            let name = case["name"].as_str().expect("case name");
            format!("n.{name} AS {name}")
        })
        .collect::<Vec<_>>()
        .join(", ");
    let result = graph
        .execute(&format!("MATCH (n:Temporal) RETURN {projection}"))
        .expect("query fixture");
    let mut schema_metadata = result.schema.metadata().clone();
    schema_metadata.remove("graphforge.query_id");
    let schema = Arc::new(Schema::new_with_metadata(
        result.schema.fields().clone(),
        schema_metadata,
    ));
    let batches = result
        .batches
        .iter()
        .map(|batch| RecordBatch::try_new(schema.clone(), batch.columns().to_vec()).unwrap())
        .collect::<Vec<_>>();
    let fixtures = root.join("tests/fixtures/temporal-v1");
    std::fs::create_dir_all(&fixtures).expect("fixture directory");

    let ipc = File::create(fixtures.join("canonical.arrow")).expect("IPC file");
    let mut ipc = StreamWriter::try_new(ipc, &schema).expect("IPC writer");
    for batch in &batches {
        ipc.write(batch).expect("IPC batch");
    }
    ipc.finish().expect("finish IPC");

    let parquet = File::create(fixtures.join("canonical.parquet")).expect("Parquet file");
    let mut parquet = ArrowWriter::try_new(parquet, schema, None).expect("Parquet writer");
    for batch in &batches {
        parquet.write(batch).expect("Parquet batch");
    }
    parquet.close().expect("finish Parquet");
}
