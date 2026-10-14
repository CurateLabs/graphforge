//! EXPLAIN ANALYZE shows what the expansion operators did (#1982).
//!
//! The fixture is four people and the KNOWS edges Alice->Bob, Alice->Carol,
//! Bob->Carol and Carol->Dave. Every assertion compares an operator's metric
//! with a row count the test derives from the query's own result or from the
//! fixture, so a metric that stays at zero or counts twice fails.
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use tempfile::TempDir;

use graphforge_exec::ExecutionSession;
use graphforge_ir::{Binder, GraphPlan, OntologyMode, RuntimeCatalog};
use graphforge_storage::GraphCatalog;

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

async fn seed(dir: &Path) -> Arc<Mutex<RuntimeCatalog>> {
    let rc = Arc::new(Mutex::new(RuntimeCatalog::new()));
    let create = bind("CREATE (:Person {name: 'Alice'})", &rc);
    session(dir, &rc).execute_create(&create).await.unwrap();
    for stmt in [
        "MATCH (a:Person {name: 'Alice'}) CREATE (a)-[:KNOWS]->(b:Person {name: 'Bob'})",
        "MATCH (a:Person {name: 'Alice'}) CREATE (a)-[:KNOWS]->(c:Person {name: 'Carol'})",
        "MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'}) CREATE (b)-[:KNOWS]->(c)",
        "MATCH (c:Person {name: 'Carol'}) CREATE (c)-[:KNOWS]->(d:Person {name: 'Dave'})",
    ] {
        let plan = bind(stmt, &rc);
        session(dir, &rc)
            .execute_write_statement(&plan)
            .await
            .unwrap_or_else(|e| panic!("seed {stmt:?}: {e}"));
    }
    rc
}

/// The `name=value` pairs of every operator line that starts with `operator`.
fn operators(text: &str, operator: &str) -> Vec<HashMap<String, String>> {
    text.lines()
        .filter(|line| line.trim_start().starts_with(&format!("{operator}:")))
        .map(|line| {
            let start = line.rfind("metrics=[").expect("operator line has metrics") + 9;
            let inner = line[start..].trim_end().trim_end_matches(']');
            inner
                .split(", ")
                .filter_map(|pair| pair.split_once('='))
                .map(|(name, value)| (name.to_owned(), value.to_owned()))
                .collect()
        })
        .collect()
}

fn count(metrics: &HashMap<String, String>, name: &str) -> usize {
    metrics
        .get(name)
        .unwrap_or_else(|| panic!("no {name} in {metrics:?}"))
        .parse()
        .unwrap_or_else(|_| panic!("{name} is not a count in {metrics:?}"))
}

fn assert_compute_time_recorded(metrics: &HashMap<String, String>) {
    let elapsed = metrics
        .get("elapsed_compute")
        .unwrap_or_else(|| panic!("no elapsed_compute in {metrics:?}"));
    assert!(
        elapsed != "0ns",
        "elapsed_compute stayed at zero in {metrics:?}"
    );
}

/// EXPLAIN ANALYZE text and the number of rows the same query returns.
async fn analyze(query: &str) -> (String, usize) {
    let dir = TempDir::new().unwrap();
    let rc = seed(dir.path()).await;
    let plan = bind(query, &rc);
    let text = session(dir.path(), &rc)
        .explain_analyze_with_params(&plan, &HashMap::new())
        .await
        .unwrap();
    let result = session(dir.path(), &rc)
        .execute_plan_with_params(&plan, &HashMap::new())
        .await
        .unwrap();
    let rows = result.batches.iter().map(|batch| batch.num_rows()).sum();
    (text, rows)
}

#[tokio::test]
async fn single_hop_expand_reports_the_rows_it_read_and_emitted() {
    let (text, rows) = analyze("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN b.name AS bn").await;
    assert_eq!(rows, 4, "the query returns one row per KNOWS edge");

    let expands = operators(&text, "ExpandExec");
    assert_eq!(expands.len(), 1, "{text}");
    let expand = &expands[0];
    assert_eq!(count(expand, "output_rows"), rows, "{text}");
    assert_eq!(count(expand, "input_rows"), 4, "four Person rows enter");
    assert_eq!(count(expand, "adjacency_entries_examined"), 4, "{text}");
    // Bob, Carol and Dave are the distinct destinations of the four edges.
    assert_eq!(count(expand, "destination_rows_read"), 3, "{text}");
    assert_compute_time_recorded(expand);
}

#[tokio::test]
async fn variable_length_expand_reports_the_trails_it_emitted() {
    let (text, rows) =
        analyze("MATCH (a:Person {name: 'Alice'})-[r:KNOWS*1..2]->(b:Person) RETURN b.name AS bn")
            .await;
    // Alice->Bob, Alice->Carol, Alice->Bob->Carol and Alice->Carol->Dave.
    assert_eq!(rows, 4);

    let expands = operators(&text, "VarLenExpandExec");
    assert_eq!(expands.len(), 1, "{text}");
    let expand = &expands[0];
    assert!(!text.contains("mode=reachability"), "{text}");
    assert_eq!(count(expand, "output_rows"), rows, "{text}");
    assert_eq!(count(expand, "input_rows"), 1, "only Alice is a start node");
    assert_eq!(count(expand, "adjacency_entries_examined"), 4, "{text}");
    assert_compute_time_recorded(expand);
}

#[tokio::test]
async fn reachability_expand_reports_the_nodes_it_reached() {
    let (text, rows) = analyze(
        "MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..2]->(b:Person) \
         RETURN DISTINCT b.name AS bn",
    )
    .await;
    // Bob, Carol and Dave, each once however many trails lead to it.
    assert_eq!(rows, 3);

    let expands = operators(&text, "VarLenExpandExec");
    assert_eq!(expands.len(), 1, "{text}");
    assert!(text.contains("mode=reachability"), "{text}");
    let expand = &expands[0];
    assert_eq!(count(expand, "output_rows"), rows, "{text}");
    assert_eq!(count(expand, "input_rows"), 1, "only Alice is a start node");
    assert!(count(expand, "adjacency_entries_examined") >= 3, "{text}");
    assert_compute_time_recorded(expand);
}

#[tokio::test]
async fn optional_match_shows_the_metrics_of_the_plan_it_executed() {
    let (text, rows) = analyze(
        "MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(b:Person) \
         RETURN a.name AS an, b.name AS bn",
    )
    .await;
    // Four edges match; Dave has none and keeps one null-extended row.
    assert_eq!(rows, 5);

    let optional = &operators(&text, "OptionalMatchExec")[0];
    assert_eq!(count(optional, "output_rows"), rows, "{text}");
    assert_eq!(count(optional, "outer_rows"), 4, "{text}");
    assert_eq!(count(optional, "inner_rows"), 4, "{text}");
    assert_compute_time_recorded(optional);

    // The Expand under the OPTIONAL is the copy bound to the outer rows and
    // run, so it reports what it did rather than the zeros of the planned copy.
    let expands = operators(&text, "ExpandExec");
    assert_eq!(expands.len(), 1, "{text}");
    assert_eq!(count(&expands[0], "output_rows"), 4, "{text}");
    assert_eq!(
        count(&expands[0], "adjacency_entries_examined"),
        4,
        "{text}"
    );
    assert_compute_time_recorded(&expands[0]);

    // DataFusion operators of the inner plan are rebuilt when it is bound to
    // the outer rows, so they report their rows only if the executed copy is
    // the one displayed. The outer scan's filter and the inner's both pass the
    // four rows they were given.
    let filters = operators(&text, "FilterExec");
    assert_eq!(filters.len(), 2, "{text}");
    for filter in &filters {
        assert_eq!(count(filter, "output_rows"), 4, "{text}");
    }
}

#[tokio::test]
async fn optional_match_inner_operators_keep_their_metrics_below_a_correlated_seed() {
    let (text, rows) = analyze(
        "MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) \
         WHERE c.name <> 'Zed' RETURN a.name AS an, c.name AS cn",
    )
    .await;
    // Alice->Bob->Carol, Alice->Carol->Dave and Bob->Carol->Dave match; Carol
    // and Dave have no two-hop path and each keep a null-extended row.
    assert_eq!(rows, 5);

    let optional = &operators(&text, "OptionalMatchExec")[0];
    assert_eq!(count(optional, "outer_rows"), 4, "{text}");
    assert_eq!(count(optional, "inner_rows"), 3, "{text}");
    assert_eq!(count(optional, "output_rows"), rows, "{text}");

    let expands = operators(&text, "ExpandExec");
    let emitted: Vec<usize> = expands.iter().map(|e| count(e, "output_rows")).collect();
    // The plan prints the consumer first: the second hop, then the first.
    assert_eq!(emitted, vec![3, 4], "{text}");
    for expand in &expands {
        assert_compute_time_recorded(expand);
    }

    // The residual filter ran inside the executed copy; a rebuilt DataFusion
    // operator would have shown no rows.
    let filters = operators(&text, "FilterExec");
    assert!(
        filters
            .iter()
            .any(|f| f.get("selectivity").is_some_and(|s| s.contains("(3/3)"))),
        "{text}"
    );
}
