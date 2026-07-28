//! Isolation regressions for reusable in-memory GraphForge fixtures.

use gf_api::{GfError, GraphForge, ProcedureDefinition};

fn row_count(result: &gf_api::ExecutionResult) -> usize {
    result
        .batches
        .iter()
        .map(arrow::record_batch::RecordBatch::num_rows)
        .sum()
}

#[test]
fn clear_resets_graph_catalog_and_procedures_for_reuse() {
    let forge = GraphForge::new(None).expect("in-memory forge");
    forge
        .execute("CREATE (:Person {name: 'Alice'})")
        .expect("seed first fixture");
    forge
        .register_procedure(ProcedureDefinition {
            name: "test.fixture".into(),
            inputs: vec![],
            outputs: vec![],
            rows: vec![vec![]],
        })
        .expect("register fixture procedure");
    forge
        .execute("CALL test.fixture()")
        .expect("fixture procedure is visible before clear");

    forge.clear().expect("clear in-memory fixture");

    let empty = forge
        .execute("MATCH (n) RETURN n")
        .expect("read cleared graph");
    assert_eq!(row_count(&empty), 0);
    assert!(
        forge.execute("CALL test.fixture()").is_err(),
        "fixture procedures must not leak between pooled scenarios"
    );

    forge
        .execute("CREATE (:Book {title: 'Graph Databases'})")
        .expect("reuse cleared fixture with a new catalog");
    let reused = forge
        .execute("MATCH (b:Book) RETURN b.title")
        .expect("read reused fixture");
    assert_eq!(row_count(&reused), 1);

    forge.clear().expect("clear remains idempotent");
    forge.clear().expect("clear empty fixture again");
}

#[test]
fn clear_rejects_persistent_projects_without_mutating_them() {
    let project = tempfile::TempDir::new().expect("project tempdir");
    let path = project.path().to_str().expect("utf-8 project path");
    let forge = GraphForge::new(Some(path)).expect("persistent forge");
    forge
        .execute("CREATE (:Person {name: 'Alice'})")
        .expect("seed persistent project");

    let error = forge
        .clear()
        .expect_err("persistent clear must be rejected");
    assert!(matches!(error, GfError::Storage(_)));

    let preserved = forge
        .execute("MATCH (n:Person) RETURN n.name")
        .expect("persistent data remains readable");
    assert_eq!(row_count(&preserved), 1);
}
