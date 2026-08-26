//! End-to-end CREATE execution tests (#700): parse → bind → lower → physical
//! plan → write, verifying the summary batch, topology, and authenticated
//! immutable property authority the [`GraphWriter`] produced.
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
use graphforge_ir::{Binder, GraphPlan, IrLiteral, OntologyMode, RuntimeCatalog};
use graphforge_ontology::{OntologyCompiler, OntologyHandle, OntologyLoader};
use graphforge_storage::{
    EdgePropertyTable, GraphCatalog, PropertyOverlayLimits, PropertyRouteKind, PropertyTable,
    enumerate_property_fragments, visit_authenticated_property_snapshots,
};

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

fn parquet_row_count(path: &Path) -> usize {
    let file = File::open(path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let mut reader = builder.build().unwrap();
    reader.next().unwrap().unwrap().num_rows()
}

fn property_rows(
    dir: &Path,
    kind: PropertyRouteKind,
    route: &str,
) -> Vec<graphforge_storage::PropertySnapshotRow> {
    let scratch = TempDir::new().unwrap();
    let mut rows = Vec::new();
    visit_authenticated_property_snapshots(
        dir,
        kind,
        route,
        scratch.path(),
        PropertyOverlayLimits::default(),
        |row| {
            assert!(!row.tombstone);
            rows.push(row);
            Ok(())
        },
    )
    .unwrap();
    rows
}

fn assert_canonical_fragment(dir: &Path, kind: PropertyRouteKind, route: &str) {
    let fragments = enumerate_property_fragments(dir, kind, route).unwrap();
    assert!(!fragments.is_empty(), "missing immutable property fragment");
    for fragment in fragments {
        assert_ne!(fragment.id.generation, 0, "new writes are not legacy files");
        assert_eq!(
            fragment.path.file_name().unwrap().to_str().unwrap(),
            fragment.id.file_name()
        );
    }
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

    assert_canonical_fragment(dir.path(), PropertyRouteKind::Node, "Person");
    let schema = PropertyTable::open_discovered(dir.path(), "Person").schema_ref();
    assert!(schema.field_with_name("node_uuid").is_ok());
    assert!(schema.field_with_name("name").is_ok());
    let rows = property_rows(dir.path(), PropertyRouteKind::Node, "Person");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["name"], IrLiteral::Str("Alice".into()));
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

    assert_canonical_fragment(dir.path(), PropertyRouteKind::Edge, "IS_FRIEND_OF");
    let schema = EdgePropertyTable::open_discovered(dir.path(), "IS_FRIEND_OF").schema_ref();
    assert!(schema.field_with_name("edge_uuid").is_ok());
    assert!(schema.field_with_name("since").is_ok());
    let rows = property_rows(dir.path(), PropertyRouteKind::Edge, "IS_FRIEND_OF");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["since"], IrLiteral::Int(2020));
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

    // Properties land in the authenticated untyped route.
    assert_canonical_fragment(dir.path(), PropertyRouteKind::Node, "_untyped");
    let rows = property_rows(dir.path(), PropertyRouteKind::Node, "_untyped");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["name"], IrLiteral::Str("X".into()));
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
