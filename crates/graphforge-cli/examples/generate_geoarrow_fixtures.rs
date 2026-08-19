//! Regenerate the producer-neutral GeoArrow IPC and Parquet conformance files.

use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;

use arrow::ipc::writer::StreamWriter;
use graphforge_api::{GraphForge, PropValue, SpatialValue};
use parquet::arrow::ArrowWriter;

fn main() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let contract: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(root.join("tests/contracts/geoarrow-interchange-v1.json"))
            .expect("read contract"),
    )
    .expect("parse contract");
    let cases = contract["cases"].as_array().expect("cases");
    let properties = cases
        .iter()
        .map(|case| {
            let preserved = case
                .get("preservedOnly")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
            let value: SpatialValue = serde_json::from_value(serde_json::json!({
                "spatial_type": {"geometry": case["geometry"], "crs": case["crs"]},
                "coordinates": case["coordinates"],
                "extension_name": preserved.then(|| case["extensionName"].clone()),
                "extension_metadata": preserved.then(|| case["extensionMetadata"].clone()),
            }))
            .expect("spatial case");
            (
                case["name"].as_str().expect("case name").to_owned(),
                PropValue::Spatial(value),
            )
        })
        .collect::<HashMap<_, _>>();
    let graph = GraphForge::new(None).expect("graph");
    graph.add_node("Geometry", &properties).expect("data row");
    graph
        .add_node("Geometry", &HashMap::new())
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
        .execute(&format!("MATCH (n:Geometry) RETURN {projection}"))
        .expect("query fixture");
    let fixtures = root.join("tests/fixtures/geoarrow-v1");
    std::fs::create_dir_all(&fixtures).expect("fixture directory");

    let ipc = File::create(fixtures.join("canonical.arrow")).expect("IPC file");
    let mut ipc = StreamWriter::try_new(ipc, &result.schema).expect("IPC writer");
    for batch in &result.batches {
        ipc.write(batch).expect("IPC batch");
    }
    ipc.finish().expect("finish IPC");

    let parquet = File::create(fixtures.join("canonical.parquet")).expect("Parquet file");
    let mut parquet = ArrowWriter::try_new(parquet, result.schema, None).expect("Parquet writer");
    for batch in &result.batches {
        parquet.write(batch).expect("Parquet batch");
    }
    parquet.close().expect("finish Parquet");
}
