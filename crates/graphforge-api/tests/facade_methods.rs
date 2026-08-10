//! Phase-0 facade methods wired for the binding-surface bindings (#850): `explain`,
//! `load_ontology`, and `execute_to_parquet`. These back the Python (#589) and
//! Node (#593) binding surfaces.

use arrow::array::{Array, StringArray};
use std::collections::HashMap;

use graphforge_api::{GfError, GraphForge, OntologyMode, PropValue};
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// explain()
// ---------------------------------------------------------------------------

#[test]
fn explain_contains_all_pipeline_stages() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    let s = gf
        .explain("MATCH (n:Person) RETURN n.node_uuid AS id")
        .expect("explain");
    for marker in ["AST", "GraphIR", "LogicalPlan", "PhysicalPlan"] {
        assert!(s.contains(marker), "missing stage {marker}:\n{s}");
    }
    // The GraphIR stage names the operators.
    assert!(s.contains("NodeScan"), "GraphIR should name NodeScan:\n{s}");
}

#[test]
fn explain_is_side_effect_free_on_the_shared_catalog() {
    // EXPLAIN binds against a snapshot, so a label seen only by explain must NOT
    // leak into the instance's shared runtime catalog (unlike execute()).
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.explain("MATCH (z:Zebra) RETURN z.node_uuid AS id")
        .expect("explain");
    let rc = gf.runtime_catalog();
    let guard = rc.lock().expect("catalog lock");
    assert!(
        !guard.contains_entity_type("Zebra"),
        "explain must not intern labels into the shared catalog"
    );
}

#[test]
fn explain_syntax_error_is_parse_variant() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    let err = gf
        .explain("MATCH (n) RETURN n WHERE")
        .expect_err("syntax error");
    assert!(
        matches!(err, GfError::Parse { .. }),
        "expected Parse, got {err:?}"
    );
}

/// A comment-only query strips to zero clauses: its raw text is non-blank (so it
/// slips past the `trim().is_empty()` guard) but it is still empty. It must be a
/// clean `Validation` error, not a panic in the result shaper (#603, found by
/// the `fuzz_exec` target).
#[test]
fn comment_only_query_is_empty_validation() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    for q in ["// just a comment", "/* block only */", "//x\n// y"] {
        let err = gf.execute(q).expect_err("comment-only query must error");
        assert!(
            matches!(err, GfError::Validation(_)),
            "expected Validation for {q:?}, got {err:?}"
        );
        // The streaming path shapes results too — it must also reject, not panic.
        // (`SendableRecordBatchStream` is not `Debug`, so match on the Result.)
        assert!(
            matches!(gf.execute_stream(q), Err(GfError::Validation(_))),
            "expected Validation (stream) for {q:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// load_ontology()
// ---------------------------------------------------------------------------

const MINIMAL_ONTOLOGY: &str = "\
ontology_id: binding_surface_test
version: \"v1\"
entity_types:
  - name: Person
    abstract: false
relation_types: []
properties:
  - owner: Person
    name: name
    type: utf8
    nullable: false
constraints: []
migrations: []
";

#[test]
fn load_ontology_applies_and_promotes_mode() {
    let dir = TempDir::new().unwrap();
    let onto = dir.path().join("test.yaml");
    std::fs::write(&onto, MINIMAL_ONTOLOGY).unwrap();

    let mut gf = GraphForge::new(None).expect("in-memory instance");
    assert!(matches!(gf.ontology_mode(), OntologyMode::Exploratory));

    gf.load_ontology(onto.to_str().unwrap())
        .expect("load ontology");

    // An ontology implies typed binding: exploratory is promoted to advisory.
    assert!(matches!(gf.ontology_mode(), OntologyMode::Advisory));
    // The declared label is now queryable (empty graph → zero rows, no error).
    let r = gf
        .execute("MATCH (n:Person) RETURN n.node_uuid AS id")
        .expect("query declared label");
    assert_eq!(r.stats.rows_produced, 0);
    assert_eq!(
        r.batches.len(),
        1,
        "zero-row MATCH must still surface one schema-bearing batch (#467)"
    );
    assert_eq!(r.batches[0].num_rows(), 0);
}

// Person + a KNOWS relation, so a typed traversal binds after load.
const FRIEND_ONTOLOGY: &str = "\
ontology_id: friends
version: \"v1\"
entity_types:
  - name: Person
    abstract: false
relation_types:
  - name: KNOWS
    src: Person
    dst: Person
    semantic:
      transitive: false
      symmetric: false
properties:
  - owner: Person
    name: name
    type: utf8
    nullable: false
constraints: []
migrations: []
";

#[test]
fn load_ontology_then_typed_var_len_traversal_uses_correct_layout() {
    // Regression guard: promoting Exploratory → Advisory must also rebuild the
    // adjacency provider, whose cached mode drives the edge-file layout. A
    // VAR-LEN expand always routes through the provider (a single hop would use
    // the join path with the session's correct mode and mask the bug), so a
    // stale Exploratory provider would scan the absent `_exploratory.parquet`
    // and return zero rows here.
    let dir = TempDir::new().unwrap();
    let onto = dir.path().join("friends.yaml");
    std::fs::write(&onto, FRIEND_ONTOLOGY).unwrap();

    let mut gf = GraphForge::new(None).expect("in-memory instance");
    gf.load_ontology(onto.to_str().unwrap())
        .expect("load ontology");
    assert!(matches!(gf.ontology_mode(), OntologyMode::Advisory));

    // Written in advisory mode → typed `topology/edges/KNOWS.parquet` layout.
    gf.execute("CREATE (a:Person {name: 'A'})-[:KNOWS]->(b:Person {name: 'B'})")
        .expect("typed create");

    let r = gf
        .execute("MATCH (a:Person {name: 'A'})-[r:KNOWS*1..2]->(b:Person) RETURN b.node_uuid AS id")
        .expect("typed var-len traversal");
    assert_eq!(r.stats.rows_produced, 1, "A reaches B via one KNOWS hop");
}

#[test]
fn advisory_unknown_relation_does_not_collide_with_ontology_type_id() {
    let dir = TempDir::new().unwrap();
    let onto = dir.path().join("friends.yaml");
    std::fs::write(&onto, FRIEND_ONTOLOGY).unwrap();
    let project = dir.path().join("project");
    std::fs::create_dir(&project).unwrap();

    let mut gf = GraphForge::new(Some(project.to_str().unwrap())).expect("persistent instance");
    gf.load_ontology(onto.to_str().unwrap())
        .expect("load ontology");
    let a = gf
        .add_node(
            "Person",
            &HashMap::from([("name".into(), PropValue::Str("A".into()))]),
        )
        .unwrap();
    let b = gf
        .add_node(
            "Person",
            &HashMap::from([("name".into(), PropValue::Str("B".into()))]),
        )
        .unwrap();
    let c = gf
        .add_node(
            "Person",
            &HashMap::from([("name".into(), PropValue::Str("C".into()))]),
        )
        .unwrap();
    gf.add_edge(&a, "KNOWS", &b, &HashMap::new()).unwrap();
    gf.add_edge(&b, "KNOWS", &c, &HashMap::new()).unwrap();
    gf.add_edge(
        &a,
        "OBSERVED_WITH",
        &c,
        &HashMap::from([("note".into(), PropValue::Str("synthetic".into()))]),
    )
    .unwrap();

    let assert_reads = |graph: &GraphForge| {
        let unknown = graph
            .execute("MATCH (a:Person)-[r:OBSERVED_WITH]->(b:Person) RETURN a.name AS source, b.name AS target, r.note AS note")
            .expect("first advisory relation read");
        assert_eq!(unknown.stats.rows_produced, 1);
        for (column, expected) in [("source", "A"), ("target", "C"), ("note", "synthetic")] {
            let values = unknown.batches[0]
                .column_by_name(column)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(values.value(0), expected);
        }
        assert_eq!(
            graph
                .execute("MATCH ()-[r:KNOWS]->() RETURN r.edge_uuid AS edge_uuid")
                .expect("known ontology relation read")
                .stats
                .rows_produced,
            2
        );
    };
    assert_reads(&gf);
    drop(gf);

    let mut reopened = GraphForge::new(Some(project.to_str().unwrap())).expect("reopen project");
    reopened
        .load_ontology(onto.to_str().unwrap())
        .expect("reload session ontology");
    assert_reads(&reopened);
}

#[test]
fn load_ontology_missing_file_is_ontology_error() {
    let mut gf = GraphForge::new(None).expect("in-memory instance");
    let err = gf
        .load_ontology("/nonexistent/path/ontology.yaml")
        .expect_err("missing file");
    assert!(
        matches!(err, GfError::Ontology(_)),
        "expected Ontology, got {err:?}"
    );
}

// ---------------------------------------------------------------------------
// execute_to_parquet()
// ---------------------------------------------------------------------------

fn read_parquet(path: &std::path::Path) -> Vec<arrow::record_batch::RecordBatch> {
    let file = std::fs::File::open(path).expect("open parquet");
    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("reader builder")
        .build()
        .expect("reader")
        .map(|b| b.expect("batch"))
        .collect()
}

#[test]
fn execute_to_parquet_roundtrips() {
    let dir = TempDir::new().unwrap();
    let out = dir.path().join("out.parquet");
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (:Person {name: 'Alice'})")
        .expect("create");

    gf.execute_to_parquet(
        "MATCH (p:Person) RETURN p.name AS name",
        out.to_str().unwrap(),
    )
    .expect("sink to parquet");

    let batches = read_parquet(&out);
    let total: usize = batches
        .iter()
        .map(arrow::record_batch::RecordBatch::num_rows)
        .sum();
    assert_eq!(total, 1, "one row written");
    let name = batches[0]
        .column_by_name("name")
        .expect("name column")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Utf8")
        .value(0);
    assert_eq!(name, "Alice");
}

#[test]
fn execute_to_parquet_zero_rows_writes_schema_only() {
    let dir = TempDir::new().unwrap();
    let out = dir.path().join("empty.parquet");
    let gf = GraphForge::new(None).expect("in-memory instance");

    gf.execute_to_parquet(
        "MATCH (p:Person) RETURN p.node_uuid AS id",
        out.to_str().unwrap(),
    )
    .expect("sink empty result");

    let batches = read_parquet(&out);
    let total: usize = batches
        .iter()
        .map(arrow::record_batch::RecordBatch::num_rows)
        .sum();
    assert_eq!(total, 0, "zero rows");
    // Schema is still present and carries the projected column.
    let file = std::fs::File::open(&out).unwrap();
    let builder =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    assert!(builder.schema().field_with_name("id").is_ok());
}
