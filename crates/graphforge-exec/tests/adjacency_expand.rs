//! End-to-end tests for provider-backed single-hop lowering (#763, #1248):
//! `ExpandExec` is stable across a fresh index (`adjacency=hit`) and a missing
//! one (`adjacency=building`), and both produce identical rows.
//!
//! Sessions run in **Advisory** mode and need no ontology file.

use std::path::Path;
use std::sync::{Arc, Mutex};

use tempfile::TempDir;

use graphforge_exec::{ExecutionResult, ExecutionSession};
use graphforge_ir::{Binder, GraphPlan, OntologyMode, RuntimeCatalog};
use graphforge_storage::GraphCatalog;
use graphforge_storage::adjacency::build_adjacency_index;

const TS: i64 = 1_700_000_000_000_000;

fn bind(query: &str, rc: &Arc<Mutex<RuntimeCatalog>>) -> GraphPlan {
    let binder = Binder::new(None, Arc::clone(rc), OntologyMode::Advisory);
    let ast = graphforge_cypher::parse(query).expect("parse");
    binder.bind(&ast).expect("bind")
}

fn session(dir: &Path, rc: &Arc<Mutex<RuntimeCatalog>>) -> ExecutionSession {
    let catalog = GraphCatalog::open(dir, None, &rc.lock().unwrap()).unwrap();
    ExecutionSession::new_with_target(catalog, None, dir.to_path_buf(), OntologyMode::Advisory)
        .unwrap()
}

/// Seed: Alice→Bob→Carol KNOWS chain with `since` edge props, a parallel
/// Alice→Bob edge WITHOUT props (LEFT-join null parity), and a self-loop on
/// Carol (undirected dedup case).
async fn seed(dir: &Path) -> Arc<Mutex<RuntimeCatalog>> {
    let rc = Arc::new(Mutex::new(RuntimeCatalog::new()));
    let create = bind("CREATE (:Person {name: 'Alice'})", &rc);
    session(dir, &rc).execute_create(&create).await.unwrap();
    for stmt in [
        // Chain (new nodes created inline — the driver's supported shape).
        "MATCH (a:Person {name: 'Alice'}) CREATE (a)-[:KNOWS {since: 2020}]->(b:Person {name: 'Bob'})",
        "MATCH (b:Person {name: 'Bob'}) CREATE (b)-[:KNOWS {since: 2021}]->(c:Person {name: 'Carol'})",
        // Parallel Alice->Bob edge WITHOUT props: bind both endpoints by
        // matching the existing edge, then create a second one.
        "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
        // Self-loop on Carol.
        "MATCH (c:Person {name: 'Carol'}) CREATE (c)-[:KNOWS]->(c)",
    ] {
        let plan = bind(stmt, &rc);
        session(dir, &rc)
            .execute_write_statement(&plan)
            .await
            .unwrap_or_else(|e| panic!("seed {stmt:?}: {e}"));
    }
    rc
}

/// Canonical multiset of a result's rows for comparison: each row rendered as
/// a sorted `name=value` list, rows sorted.
fn canonical_rows(result: &ExecutionResult) -> Vec<String> {
    let mut rows = Vec::new();
    for batch in &result.batches {
        let display_cols: Vec<(String, arrow::array::ArrayRef)> = {
            let mut cols: Vec<(String, arrow::array::ArrayRef)> = batch
                .schema()
                .fields()
                .iter()
                .enumerate()
                .map(|(i, f)| (f.name().clone(), batch.column(i).clone()))
                .collect();
            cols.sort_by(|a, b| a.0.cmp(&b.0));
            cols
        };
        for row in 0..batch.num_rows() {
            let mut parts = Vec::new();
            for (name, col) in &display_cols {
                let value = arrow::util::display::array_value_to_string(col, row)
                    .unwrap_or_else(|_| "<err>".to_owned());
                let is_null = col.is_null(row);
                parts.push(format!("{name}={}", if is_null { "NULL" } else { &value }));
            }
            rows.push(parts.join(","));
        }
    }
    rows.sort();
    rows
}

/// Execute `query` with the index built and with it removed; assert the rows
/// agree and return the canonicalized rows.
async fn both_ways(dir: &Path, rc: &Arc<Mutex<RuntimeCatalog>>, query: &str) -> Vec<String> {
    let plan = bind(query, rc);

    build_adjacency_index(dir, TS).unwrap();
    let with_index = session(dir, rc).execute_plan(&plan).await.unwrap();

    std::fs::remove_dir_all(dir.join("indexes")).unwrap();
    let without_index = session(dir, rc).execute_plan(&plan).await.unwrap();

    let a = canonical_rows(&with_index);
    let b = canonical_rows(&without_index);
    assert_eq!(a, b, "indexed vs scan-build rows differ for {query}");
    a
}

use arrow::array::Array;

#[tokio::test]
async fn explain_shows_stable_expand_exec_across_index_states() {
    let dir = TempDir::new().unwrap();
    let rc = seed(dir.path()).await;
    let plan = bind(
        "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN b.name AS bn",
        &rc,
    );

    let without = session(dir.path(), &rc)
        .explain_physical(&plan)
        .await
        .unwrap();
    assert!(
        without.contains("ExpandExec") && without.contains("adjacency=hit"),
        "no index: bounded lazy build must be reflected as a hit, got:\n{without}"
    );

    build_adjacency_index(dir.path(), TS).unwrap();
    let with_index = session(dir.path(), &rc)
        .explain_physical(&plan)
        .await
        .unwrap();
    assert!(
        with_index.contains("ExpandExec") && with_index.contains("adjacency=hit"),
        "index built: adjacency-backed plan expected, got:\n{with_index}"
    );
}

#[tokio::test]
async fn out_results_identical_including_edge_props_and_nulls() {
    let dir = TempDir::new().unwrap();
    let rc = seed(dir.path()).await;
    // The parallel Alice→Bob edge has no property row: r.since must be NULL on
    // that row in BOTH paths (LEFT-join parity).
    let rows = both_ways(
        dir.path(),
        &rc,
        "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name AS an, b.name AS bn, r.since AS since",
    )
    .await;
    // Alice→Bob (2020), Alice→Bob (null), Bob→Carol (2021), Carol→Carol (null).
    assert_eq!(rows.len(), 4, "rows: {rows:?}");
    assert!(rows.iter().any(|r| r.contains("since=NULL")), "{rows:?}");
}

#[tokio::test]
async fn in_results_identical() {
    let dir = TempDir::new().unwrap();
    let rc = seed(dir.path()).await;
    let rows = both_ways(
        dir.path(),
        &rc,
        "MATCH (b:Person)<-[r:KNOWS]-(a:Person) RETURN a.name AS an, b.name AS bn",
    )
    .await;
    assert_eq!(rows.len(), 4, "rows: {rows:?}");
}

#[tokio::test]
async fn undirected_adjacency_path_works_and_dedups_self_loop() {
    // Undirected single-hop with a qualified projection now works on BOTH paths
    // (#825 fixed): the join path re-qualifies its union output and drops
    // self-loops from the In leg instead of using Distinct, so it matches the
    // adjacency path row-for-row (rather than erroring on lost qualifiers).
    let dir = TempDir::new().unwrap();
    let rc = seed(dir.path()).await;
    let rows = both_ways(
        dir.path(),
        &rc,
        "MATCH (a:Person {name: 'Carol'})-[r:KNOWS]-(b) RETURN b.node_uuid",
    )
    .await;
    // Carol: incoming Bob→Carol (b=Bob) and the self-loop exactly once (the
    // In-leg self-loop filter collapses the merged view's double entry).
    assert_eq!(rows.len(), 2, "self-loop must appear once: {rows:?}");
}

#[tokio::test]
async fn chained_two_hop_results_identical() {
    let dir = TempDir::new().unwrap();
    let rc = seed(dir.path()).await;
    let rows = both_ways(
        dir.path(),
        &rc,
        // c is unlabeled, so only topology columns resolve (no trailing
        // property join) — same as the join path.
        "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN c.node_uuid",
    )
    .await;
    // Two Alice→Bob edges × (Bob→Carol) = 2 rows of Carol.
    assert_eq!(rows.len(), 2, "rows: {rows:?}");
}

#[tokio::test]
async fn stale_between_plan_and_execute_is_correct() {
    let dir = TempDir::new().unwrap();
    let rc = seed(dir.path()).await;
    build_adjacency_index(dir.path(), TS).unwrap();

    // A write through another session bumps the generation. Verify a query in
    // a new session over the now-stale index still returns correct post-write
    // rows via the provider's stale fallback inside ExpandExec.
    let extend = bind(
        "MATCH (c:Person {name: 'Carol'}) CREATE (c)-[:KNOWS {since: 2024}]->(d:Person {name: 'Dave'})",
        &rc,
    );
    session(dir.path(), &rc)
        .execute_write_statement(&extend)
        .await
        .unwrap();
    // Create a generation gap with no delta segment so the provider reports a
    // genuine Miss rather than serving the write's valid overlay chain.
    graphforge_storage::generation::bump_topology_generation(dir.path()).unwrap();

    let plan = bind(
        "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN b.name AS bn",
        &rc,
    );
    let post_write = session(dir.path(), &rc);
    let physical = post_write.explain_physical(&plan).await.unwrap();
    assert!(
        physical.contains("ExpandExec") && physical.contains("adjacency=miss"),
        "stale index must retain the provider-backed plan: {physical}"
    );
    let result = post_write.execute_plan(&plan).await.unwrap();
    let rows = canonical_rows(&result);
    assert_eq!(rows.len(), 5, "post-write edge visible: {rows:?}");
    assert!(rows.iter().any(|r| r.contains("bn=Dave")), "{rows:?}");
}
