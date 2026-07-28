//! End-to-end read-path tests (#717): CREATE then MATCH executes against the
//! real Parquet-backed catalog tables via `execute_plan`.
//!
//! These prove the scan→catalog wiring: a `MATCH (n:Label)` scan now reads the
//! rows a prior `CREATE` wrote, rather than a schema-only empty source.

use std::sync::{Arc, Mutex};

use gf_exec::ExecutionSession;
use gf_ir::{Binder, GraphPlan, OntologyMode, RuntimeCatalog};
use gf_storage::GraphCatalog;
use tempfile::TempDir;

/// Bind a query in exploratory mode (no ontology) sharing one RuntimeCatalog.
fn bind(query: &str, catalog: Arc<Mutex<RuntimeCatalog>>) -> GraphPlan {
    let binder = Binder::new(None, catalog, OntologyMode::Exploratory);
    let ast = gf_cypher::parse(query).expect("parse");
    binder.bind(&ast).expect("bind")
}

/// Open a write+read session against `dir` in exploratory mode.
fn session(dir: &std::path::Path) -> ExecutionSession {
    let catalog = GraphCatalog::open(dir, None, &RuntimeCatalog::new()).unwrap();
    ExecutionSession::new_with_target(catalog, None, dir.to_path_buf(), OntologyMode::Exploratory)
        .unwrap()
}

/// Open a session whose catalog is built from `rc` — so its `PropId → name` map
/// reflects the properties the binder has already interned. The catalog
/// snapshots the runtime catalog at open time, so callers must bind (intern
/// properties) **before** opening the session that reads them back.
fn session_with(dir: &std::path::Path, rc: &RuntimeCatalog) -> ExecutionSession {
    let catalog = GraphCatalog::open(dir, None, rc).unwrap();
    ExecutionSession::new_with_target(catalog, None, dir.to_path_buf(), OntologyMode::Exploratory)
        .unwrap()
}

#[tokio::test]
async fn match_reads_created_nodes() {
    let dir = TempDir::new().unwrap();
    let rc = Arc::new(Mutex::new(RuntimeCatalog::new()));
    let session = session(dir.path());

    // CREATE three Person nodes in a single statement. (The writer now appends
    // across flushes — #733 — but one statement keeps this scan test focused.)
    let create = bind(
        "CREATE (:Person {name: 'Alice'}), (:Person {name: 'Bob'}), (:Person {name: 'Carol'})",
        rc.clone(),
    );
    session.execute_create(&create).await.expect("create");

    // MATCH (n:Person) RETURN 1 — the scan must read the 3 written rows.
    let plan = bind("MATCH (n:Person) RETURN 1 AS one", rc.clone());
    let result = session.execute_plan(&plan).await.expect("execute_plan");
    assert_eq!(
        result.stats.rows_produced, 3,
        "MATCH should read the 3 created Person nodes"
    );
}

#[tokio::test]
async fn match_on_empty_graph_yields_no_rows() {
    let dir = TempDir::new().unwrap();
    let rc = Arc::new(Mutex::new(RuntimeCatalog::new()));
    let session = session(dir.path());
    // No CREATE: the topology file does not exist; the scan reads zero rows.
    let plan = bind("MATCH (n:Person) RETURN 1 AS one", rc);
    let result = session.execute_plan(&plan).await.expect("execute_plan");
    assert_eq!(result.stats.rows_produced, 0);
}

#[tokio::test]
async fn read_only_session_scan_errors_instead_of_binding_to_cwd() {
    // A session built via `ExecutionSession::new` has an empty project dir, so
    // it cannot read persisted data. `execute_plan` must reject a scan plan with
    // a clear error rather than binding `TopologyNodeTable` to a CWD-relative
    // `topology/nodes.parquet` (which would silently read the wrong file).
    let dir = TempDir::new().unwrap();
    let rc = Arc::new(Mutex::new(RuntimeCatalog::new()));
    let catalog = GraphCatalog::open(dir.path(), None, &RuntimeCatalog::new()).unwrap();
    let session = ExecutionSession::new(catalog, None).unwrap();

    let plan = bind("MATCH (n:Person) RETURN 1 AS one", rc);
    let err = session
        .execute_plan(&plan)
        .await
        .expect_err("scan on a read-only session must error");
    assert!(
        err.to_string().contains("project directory"),
        "error should explain the missing project directory, got: {err}"
    );
}

#[tokio::test]
async fn read_only_session_computed_plan_still_runs() {
    // A scan-free plan (`RETURN 1`) reads no persisted data, so the empty-dir
    // guard must NOT fire: a read-only session executes it schema-only. (The
    // row-count semantics of a bare `RETURN` are a separate concern; here we
    // only assert that the plan lowers and executes without error.)
    let dir = TempDir::new().unwrap();
    let rc = Arc::new(Mutex::new(RuntimeCatalog::new()));
    let catalog = GraphCatalog::open(dir.path(), None, &RuntimeCatalog::new()).unwrap();
    let session = ExecutionSession::new(catalog, None).unwrap();

    let plan = bind("RETURN 1 AS one", rc);
    session
        .execute_plan(&plan)
        .await
        .expect("scan-free plan should run on a read-only session");
}

#[tokio::test]
async fn match_returns_property_value() {
    // #704: CREATE a node with a property, then `RETURN n.name` must read the
    // real value via the property-table JOIN (exploratory `_untyped` path).
    use arrow::array::StringArray;

    let dir = TempDir::new().unwrap();
    let rc = Arc::new(Mutex::new(RuntimeCatalog::new()));

    // Bind both statements FIRST so the shared runtime catalog interns the
    // `name` property before the read session's catalog snapshots it.
    let create = bind("CREATE (:Person {name: 'Alice'})", rc.clone());
    let read = bind("MATCH (n:Person) RETURN n.name AS name", rc.clone());

    // Write with a target session, then open a read session whose catalog
    // reflects the now-populated runtime catalog.
    let write = session(dir.path());
    write.execute_create(&create).await.expect("create");

    let read_session = session_with(dir.path(), &rc.lock().unwrap());
    let result = read_session
        .execute_plan(&read)
        .await
        .expect("execute_plan");

    assert_eq!(result.stats.rows_produced, 1, "one Person row");
    let batch = &result.batches[0];
    let names = batch
        .column_by_name("name")
        .expect("RETURN aliased the column `name`")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name is a Utf8 column");
    assert_eq!(names.value(0), "Alice");
}
