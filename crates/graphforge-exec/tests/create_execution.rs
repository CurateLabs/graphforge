//! End-to-end CREATE execution tests (#700): parse → bind → lower → physical
//! plan → write, verifying the summary batch and the Parquet files the
//! [`GraphWriter`] produced.
//!
//! The `RETURN n.name` read round-trip is intentionally NOT tested — the read
//! path cannot project property columns yet (deferred). We assert the write
//! summary and inspect the written files directly.

use std::fs::File;
use std::path::Path;
use std::sync::{Arc, Mutex};

use arrow::array::UInt64Array;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tempfile::TempDir;

use graphforge_exec::ExecutionSession;
use graphforge_ir::{Binder, GraphPlan, OntologyMode, RuntimeCatalog};
use graphforge_ontology::{OntologyCompiler, OntologyHandle, OntologyLoader};
use graphforge_storage::GraphCatalog;

fn hr_handle() -> OntologyHandle {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("graphforge-ontology/tests/fixtures/hr.yaml");
    let doc = OntologyLoader::load_file(&fixture).expect("load hr.yaml");
    let runtime = OntologyCompiler::compile(&doc).expect("compile HR ontology");
    OntologyHandle::new(runtime)
}

/// Bind a query with an optional ontology handle in the given mode.
fn bind(query: &str, ontology: Option<OntologyHandle>, mode: OntologyMode) -> GraphPlan {
    let catalog = Arc::new(Mutex::new(RuntimeCatalog::new()));
    let binder = Binder::new(ontology, catalog, mode);
    let ast = graphforge_cypher::parse(query).expect("parse");
    binder.bind(&ast).expect("bind")
}

/// Read the two summary counters out of an execution result's single batch.
fn summary(result: &graphforge_exec::ExecutionResult) -> (u64, u64) {
    let batch = &result.batches[0];
    let nodes = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .value(0);
    let edges = batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .value(0);
    (nodes, edges)
}

fn parquet_columns(path: &Path) -> Vec<String> {
    let file = File::open(path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    builder
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect()
}

fn parquet_row_count(path: &Path) -> usize {
    let file = File::open(path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let mut reader = builder.build().unwrap();
    reader.next().unwrap().unwrap().num_rows()
}

#[tokio::test]
async fn create_single_node_with_properties_strict() {
    let dir = TempDir::new().unwrap();
    let handle = hr_handle();
    let plan = bind(
        "CREATE (:Person {name: 'Alice'})",
        Some(handle.clone()),
        OntologyMode::Strict,
    );

    let catalog = GraphCatalog::open(dir.path(), Some(&handle), &RuntimeCatalog::new()).unwrap();
    let session = ExecutionSession::new_with_target(
        catalog,
        Some(handle),
        dir.path().to_path_buf(),
        OntologyMode::Strict,
    )
    .unwrap();

    let result = session.execute_create(&plan).await.expect("execute_create");
    assert_eq!(summary(&result), (1, 0));

    // Topology written.
    let nodes = dir.path().join("topology/nodes.parquet");
    assert!(nodes.exists());
    assert_eq!(parquet_row_count(&nodes), 1);

    // Properties written to the typed Person file with a `name` column.
    let props = dir.path().join("properties/Person.parquet");
    assert!(props.exists(), "Person property file should exist");
    let cols = parquet_columns(&props);
    assert!(cols.contains(&"node_uuid".to_owned()));
    assert!(cols.contains(&"name".to_owned()), "got {cols:?}");
}

#[tokio::test]
async fn create_edge_between_two_nodes_strict() {
    let dir = TempDir::new().unwrap();
    let handle = hr_handle();
    // IS_FRIEND_OF is a Person→Person relation in the HR ontology.
    let plan = bind(
        "CREATE (a:Person)-[:IS_FRIEND_OF]->(b:Person)",
        Some(handle.clone()),
        OntologyMode::Strict,
    );

    let catalog = GraphCatalog::open(dir.path(), Some(&handle), &RuntimeCatalog::new()).unwrap();
    let session = ExecutionSession::new_with_target(
        catalog,
        Some(handle),
        dir.path().to_path_buf(),
        OntologyMode::Strict,
    )
    .unwrap();

    let result = session.execute_create(&plan).await.expect("execute_create");
    assert_eq!(summary(&result), (2, 1));

    assert_eq!(
        parquet_row_count(&dir.path().join("topology/nodes.parquet")),
        2
    );
    let edges = dir.path().join("topology/edges/IS_FRIEND_OF.parquet");
    assert!(edges.exists(), "typed edge file should exist");
    assert_eq!(parquet_row_count(&edges), 1);
}

#[tokio::test]
async fn create_edge_with_properties_persists_edge_property_file() {
    // #784: CREATE edge properties are no longer rejected — they persist to
    // edge_properties/<REL>.parquet keyed by edge_uuid.
    let dir = TempDir::new().unwrap();
    let handle = hr_handle();
    let plan = bind(
        "CREATE (a:Person)-[:IS_FRIEND_OF {since: 2020}]->(b:Person)",
        Some(handle.clone()),
        OntologyMode::Strict,
    );

    let catalog = GraphCatalog::open(dir.path(), Some(&handle), &RuntimeCatalog::new()).unwrap();
    let session = ExecutionSession::new_with_target(
        catalog,
        Some(handle),
        dir.path().to_path_buf(),
        OntologyMode::Strict,
    )
    .unwrap();

    // Previously this errored with "CREATE edge properties are not yet supported".
    let result = session.execute_create(&plan).await.expect("execute_create");
    assert_eq!(summary(&result), (2, 1));

    let edge_props = dir.path().join("edge_properties/IS_FRIEND_OF.parquet");
    assert!(
        edge_props.exists(),
        "edge property file should exist at edge_properties/IS_FRIEND_OF.parquet"
    );
    assert_eq!(parquet_row_count(&edge_props), 1);
    let cols = parquet_columns(&edge_props);
    assert!(cols.contains(&"edge_uuid".to_owned()), "got {cols:?}");
    assert!(cols.contains(&"since".to_owned()), "got {cols:?}");
}

#[test]
fn create_untyped_edge_is_rejected_at_bind() {
    // #784 / #956: creating a relationship without a type is invalid
    // (openCypher NoSingleRelationshipType). The binder now rejects it at
    // compile time — earlier and more correct than the former execution-layer
    // orphaning guard — so a typeless edge never reaches the write path.
    let catalog = Arc::new(Mutex::new(RuntimeCatalog::new()));
    let binder = Binder::new(None, catalog, OntologyMode::Exploratory);
    let ast = graphforge_cypher::parse("CREATE (a)-[r {since: 2020}]->(b)").expect("parse");
    let errs = binder
        .bind(&ast)
        .expect_err("a typeless created relationship must be rejected");
    assert!(
        errs.iter().any(|e| e.message.contains("exactly one type")),
        "expected a NoSingleRelationshipType bind error, got: {errs:?}"
    );
}

#[tokio::test]
async fn create_unknown_node_exploratory() {
    let dir = TempDir::new().unwrap();
    // No ontology — exploratory mode.
    let plan = bind(
        "CREATE (:Unknown {name: 'X'})",
        None,
        OntologyMode::Exploratory,
    );

    let catalog = GraphCatalog::open(dir.path(), None, &RuntimeCatalog::new()).unwrap();
    let session = ExecutionSession::new_with_target(
        catalog,
        None,
        dir.path().to_path_buf(),
        OntologyMode::Exploratory,
    )
    .unwrap();

    let result = session.execute_create(&plan).await.expect("execute_create");
    assert_eq!(summary(&result), (1, 0));

    // Properties land in the untyped catch-all (no ontology → label_name None).
    let props = dir.path().join("properties/_untyped.parquet");
    assert!(props.exists(), "untyped property file should exist");
    let cols = parquet_columns(&props);
    assert!(cols.contains(&"name".to_owned()), "got {cols:?}");
}

#[tokio::test]
async fn execute_create_without_target_errors() {
    let dir = TempDir::new().unwrap();
    let catalog = GraphCatalog::open(dir.path(), None, &RuntimeCatalog::new()).unwrap();
    // Read-only session (no write target).
    let session = ExecutionSession::new(catalog, None).unwrap();
    let plan = bind("CREATE (:Unknown)", None, OntologyMode::Exploratory);
    assert!(session.execute_create(&plan).await.is_err());
}
