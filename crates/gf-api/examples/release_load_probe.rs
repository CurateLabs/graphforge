//! Repository-owned load probe for the public Rust facade.
//!
//! L/XL dense fixtures exceed the project recovery publication bound when built
//! through scalar `add_node` / `add_edge`. Construction therefore uses the
//! public atomic bulk APIs (`publish_bulk_nodes` / `publish_bulk_edges`) so each
//! language probe creates two generations regardless of fixture size.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, FixedSizeBinaryArray, FixedSizeBinaryBuilder, Float64Array, Int64Array,
    StringArray,
};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use gf_api::{
    FindOptions, GraphForge, OperationId, RankAlgorithm, RankOptions, SearchIndexOptions,
    bulk_edge_input_schema, bulk_node_input_schema,
};
use gf_core::uuid::Uuid;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

/// Same fixed bulk operation UUIDs as the Python/Node load probes so generated
/// entity UUIDs (and therefore m19 top-k find tie-breaks) are cross-language stable.
const NODE_OPERATION: &str = "018f0f4e-7b8c-7000-8000-00000000b001";
const EDGE_OPERATION: &str = "018f0f4e-7b8c-7000-8000-00000000b002";

#[derive(Deserialize)]
struct Request {
    manifest: Manifest,
    fixture: PathBuf,
    workload: Workload,
}

#[derive(Deserialize)]
struct Manifest {
    content_sha256: String,
}

#[derive(Deserialize)]
struct Workload {
    id: String,
}

#[derive(Deserialize)]
struct Fixture {
    nodes: Vec<Node>,
    edges: Vec<Edge>,
}

#[derive(Deserialize)]
struct Node {
    ordinal: i64,
    label: String,
    name: String,
    group: i64,
    salience: f64,
    active: bool,
    nullable: Option<String>,
}

#[derive(Deserialize)]
struct Edge {
    source: usize,
    target: usize,
    #[serde(rename = "type")]
    rel_type: String,
    weight: f64,
}

#[derive(Serialize)]
struct ProbeReport {
    schema: &'static str,
    language: &'static str,
    dataset_sha256: String,
    workload: String,
    observed: Observed,
    persisted_bytes: u64,
    temporary_bytes: u64,
    cleanup: &'static str,
    reopen_equivalent: bool,
}

#[derive(Serialize)]
struct Observed {
    node_rows: usize,
    edge_rows: usize,
    rank_rows: usize,
    find_rows: usize,
    reopen_node_rows: usize,
    schema_sha256: String,
    ordering_sha256: String,
    node_result_sha256: String,
    rank_result_sha256: String,
    find_result_sha256: String,
}

fn rows(result: &gf_api::ExecutionResult) -> usize {
    result
        .batches
        .iter()
        .map(arrow::record_batch::RecordBatch::num_rows)
        .sum()
}

fn directory_bytes(path: &Path) -> std::io::Result<u64> {
    let mut total = 0;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        total += if metadata.is_dir() {
            directory_bytes(&entry.path())?
        } else {
            metadata.len()
        };
    }
    Ok(total)
}

fn fingerprint<T: Serialize>(value: &T) -> Result<String, serde_json::Error> {
    let digest = Sha256::digest(serde_json::to_vec(value)?);
    Ok(digest.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn argument(name: &str) -> Result<PathBuf, String> {
    let args = std::env::args().collect::<Vec<_>>();
    let index = args
        .iter()
        .position(|value| value == name)
        .ok_or_else(|| format!("missing {name}"))?;
    args.get(index + 1)
        .map(PathBuf::from)
        .ok_or_else(|| format!("missing value for {name}"))
}

fn null_uuid_column(rows: usize) -> FixedSizeBinaryArray {
    let mut builder = FixedSizeBinaryBuilder::with_capacity(rows, 16);
    for _ in 0..rows {
        builder.append_null();
    }
    builder.finish()
}

fn load_fixture(graph: &GraphForge, fixture: &Fixture) -> Result<(), Box<dyn std::error::Error>> {
    let node_schema = bulk_node_input_schema(vec![
        Field::new("active", DataType::Boolean, false),
        Field::new("group", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("nullable", DataType::Utf8, true),
        Field::new("ordinal", DataType::Int64, false),
        Field::new("salience", DataType::Float64, false),
    ])
    .map_err(|error| error.to_string())?;
    let node_batch = RecordBatch::try_new(
        node_schema,
        vec![
            Arc::new(null_uuid_column(fixture.nodes.len())),
            Arc::new(StringArray::from_iter_values(
                fixture.nodes.iter().map(|node| node.label.as_str()),
            )),
            Arc::new(BooleanArray::from_iter(
                fixture.nodes.iter().map(|node| Some(node.active)),
            )),
            Arc::new(Int64Array::from_iter_values(
                fixture.nodes.iter().map(|node| node.group),
            )),
            Arc::new(StringArray::from_iter_values(
                fixture.nodes.iter().map(|node| node.name.as_str()),
            )),
            Arc::new(StringArray::from_iter(
                fixture.nodes.iter().map(|node| node.nullable.as_deref()),
            )),
            Arc::new(Int64Array::from_iter_values(
                fixture.nodes.iter().map(|node| node.ordinal),
            )),
            Arc::new(Float64Array::from_iter_values(
                fixture.nodes.iter().map(|node| node.salience),
            )),
        ],
    )?;
    let node_receipt = graph
        .publish_bulk_nodes(OperationId(Uuid::parse_str(NODE_OPERATION)?), &[node_batch])
        .map_err(|error| error.to_string())?;
    let node_ids = node_receipt
        .column_by_name("entity_uuid")
        .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
        .ok_or("bulk node receipt missing entity_uuid")?;
    if node_ids.len() != fixture.nodes.len() {
        return Err("bulk node receipt row count drifted from fixture".into());
    }

    let edge_schema = bulk_edge_input_schema(vec![Field::new("weight", DataType::Float64, false)])
        .map_err(|error| error.to_string())?;
    let mut edge_ids = FixedSizeBinaryBuilder::with_capacity(fixture.edges.len(), 16);
    let mut sources = FixedSizeBinaryBuilder::with_capacity(fixture.edges.len(), 16);
    let mut targets = FixedSizeBinaryBuilder::with_capacity(fixture.edges.len(), 16);
    for edge in &fixture.edges {
        edge_ids.append_null();
        let source = node_ids.value(edge.source);
        let target = node_ids.value(edge.target);
        sources
            .append_value(source)
            .map_err(|error| format!("source_uuid builder failed: {error}"))?;
        targets
            .append_value(target)
            .map_err(|error| format!("target_uuid builder failed: {error}"))?;
    }
    let edge_batch = RecordBatch::try_new(
        edge_schema,
        vec![
            Arc::new(edge_ids.finish()),
            Arc::new(StringArray::from_iter_values(
                fixture.edges.iter().map(|edge| edge.rel_type.as_str()),
            )),
            Arc::new(sources.finish()),
            Arc::new(targets.finish()),
            Arc::new(Float64Array::from_iter_values(
                fixture.edges.iter().map(|edge| edge.weight),
            )),
        ],
    )?;
    graph
        .publish_bulk_edges(OperationId(Uuid::parse_str(EDGE_OPERATION)?), &[edge_batch])
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let request_path = argument("--request")?;
    let output_path = argument("--output")?;
    let request: Request = serde_json::from_slice(&fs::read(request_path)?)?;
    let fixture: Fixture = serde_json::from_slice(&fs::read(&request.fixture)?)?;
    let directory = TempDir::new()?;
    let project = directory
        .path()
        .to_str()
        .ok_or("project path is not UTF-8")?;
    let graph = GraphForge::new(Some(project))?;
    load_fixture(&graph, &fixture)?;
    let node_result = graph.execute("MATCH (n) RETURN n.name AS name ORDER BY name")?;
    let node_rows = rows(&node_result);
    let schema_sha256 = fingerprint(
        &node_result
            .schema
            .fields()
            .iter()
            .map(|field| {
                (
                    field.name().to_owned(),
                    format!("{:?}", field.data_type()).to_lowercase(),
                )
            })
            .collect::<Vec<_>>(),
    )?;
    let mut names = Vec::with_capacity(node_rows);
    for batch in &node_result.batches {
        let values = batch
            .column_by_name("name")
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .ok_or("node query did not return UTF-8 names")?;
        names.extend((0..values.len()).map(|index| values.value(index).to_owned()));
    }
    let node_result_sha256 = fingerprint(&names)?;
    let edge_rows = rows(&graph.execute("MATCH ()-[r]->() RETURN r")?);
    let mut rank_rows = 0;
    let mut find_rows = 0;
    let mut rank_result_sha256 = fingerprint(&Vec::<(String, f64)>::new())?;
    let mut find_result_sha256 = fingerprint(&Vec::<(String, String)>::new())?;
    if request.workload.id.starts_with("m18-") {
        let rank = graph.rank(
            "Entity",
            RankOptions {
                by: RankAlgorithm::Degree,
                via: Some("LINK".into()),
                ..RankOptions::default()
            },
        )?;
        rank_rows = rank.num_rows();
        let rank_names = rank
            .column_by_name("name")
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .ok_or("rank did not return UTF-8 names")?;
        let scores = rank
            .column_by_name("score")
            .and_then(|column| column.as_any().downcast_ref::<Float64Array>())
            .ok_or("rank did not return Float64 scores")?;
        let mut values = (0..rank_rows)
            .map(|index| (rank_names.value(index).to_owned(), scores.value(index)))
            .collect::<Vec<_>>();
        values.sort_by(|left, right| left.0.cmp(&right.0));
        rank_result_sha256 = fingerprint(&values)?;
    }
    if request.workload.id.starts_with("m19-") {
        graph.index_search(
            "Entity",
            SearchIndexOptions::Text {
                properties: Some(vec!["name".into()]),
                rebuild: false,
            },
        )?;
        let found = graph.find(FindOptions {
            query: Some("n-00000001".into()),
            label: Some("Entity".into()),
            ..FindOptions::default()
        })?;
        find_rows = found.num_rows();
        let found_names = found
            .column_by_name("name")
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .ok_or("find did not return UTF-8 names")?;
        let matched = found
            .column_by_name("matched_on")
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .ok_or("find did not return UTF-8 matched_on")?;
        let mut values = (0..find_rows)
            .map(|index| {
                (
                    found_names.value(index).to_owned(),
                    matched.value(index).to_owned(),
                )
            })
            .collect::<Vec<_>>();
        values.sort();
        find_result_sha256 = fingerprint(&values)?;
    }
    drop(graph);
    let persisted_bytes = directory_bytes(directory.path())?;
    let reopened = GraphForge::new(Some(project))?;
    let reopened_nodes = reopened.execute("MATCH (n) RETURN n.name AS name ORDER BY name")?;
    let reopen_node_rows = rows(&reopened_nodes);
    let mut reopened_names = Vec::with_capacity(reopen_node_rows);
    for batch in &reopened_nodes.batches {
        let values = batch
            .column_by_name("name")
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .ok_or("reopened query did not return UTF-8 names")?;
        reopened_names.extend((0..values.len()).map(|index| values.value(index).to_owned()));
    }
    let reopen_node_result_sha256 = fingerprint(&reopened_names)?;
    if request.workload.id.starts_with("m18-") {
        let repeated = reopened
            .rank(
                "Entity",
                RankOptions {
                    by: RankAlgorithm::Degree,
                    via: Some("LINK".into()),
                    ..RankOptions::default()
                },
            )?
            .num_rows();
        if repeated != rank_rows {
            return Err("rank result changed after reopen".into());
        }
    }
    if request.workload.id.starts_with("m19-") {
        let repeated = reopened
            .find(FindOptions {
                query: Some("n-00000001".into()),
                label: Some("Entity".into()),
                ..FindOptions::default()
            })?
            .num_rows();
        if repeated != find_rows {
            return Err("find result changed after reopen".into());
        }
    }
    let temporary_bytes = directory_bytes(directory.path())?.saturating_sub(persisted_bytes);
    drop(reopened);
    directory.close()?;
    let reopen_equivalent =
        reopen_node_rows == node_rows && reopen_node_result_sha256 == node_result_sha256;
    let report = ProbeReport {
        schema: "graphforge-load-native-probe/1",
        language: "rust",
        dataset_sha256: request.manifest.content_sha256,
        workload: request.workload.id,
        observed: Observed {
            node_rows,
            edge_rows,
            rank_rows,
            find_rows,
            reopen_node_rows,
            schema_sha256,
            ordering_sha256: node_result_sha256.clone(),
            node_result_sha256,
            rank_result_sha256,
            find_result_sha256,
        },
        persisted_bytes,
        temporary_bytes,
        cleanup: "complete",
        reopen_equivalent,
    };
    fs::write(output_path, serde_json::to_vec(&report)?)?;
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("release load Rust probe failed: {error}");
        std::process::exit(1);
    }
}
