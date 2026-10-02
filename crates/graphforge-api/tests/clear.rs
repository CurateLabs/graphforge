//! Isolation regressions for reusable in-memory GraphForge fixtures.

use graphforge_api::{GfError, GraphForge, ProcedureDefinition};

fn row_count(result: &graphforge_api::ExecutionResult) -> usize {
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

/// `clear()` wipes the workspace, but the session's read authority still
/// declared the previous generation's node files, content-store objects the
/// wipe does not remove, and every later catalog listed node files from it
/// (#1388). The count after `clear()` must be zero, and a later write must
/// be the only thing visible.
#[test]
fn clear_drops_the_previous_generations_node_files_from_the_read_authority() {
    let forge = GraphForge::new(None).expect("in-memory forge");
    forge
        .execute("CREATE (:A {k: 1}), (:A {k: 2})")
        .expect("seed two nodes");
    let seeded = forge
        .execute("MATCH (n) RETURN count(n) AS total")
        .expect("count seeded");
    assert_eq!(count_total(&seeded), 2);

    forge.clear().expect("clear in-memory fixture");
    let cleared = forge
        .execute("MATCH (n) RETURN count(n) AS total")
        .expect("count cleared graph");
    assert_eq!(
        cleared_rows(&cleared),
        0,
        "nodes of the cleared generation leaked"
    );
    assert_eq!(
        count_total(&cleared),
        0,
        "nodes of the cleared generation leaked"
    );

    forge
        .execute("CREATE (:B {k: 3})")
        .expect("write after clear");
    let after = forge
        .execute("MATCH (n) RETURN count(n) AS total")
        .expect("count after clear and write");
    assert_eq!(count_total(&after), 1);
}

fn count_total(result: &graphforge_api::ExecutionResult) -> i64 {
    result
        .batches
        .iter()
        .filter(|batch| batch.num_rows() > 0)
        .map(|batch| {
            batch
                .column_by_name("total")
                .expect("total column")
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("count is Int64")
                .value(0)
        })
        .sum()
}

/// `MATCH (n) RETURN count(n)` on an empty graph yields one row of zero; the
/// leak showed as a count, so the row count is checked separately as `<= 1`.
fn cleared_rows(result: &graphforge_api::ExecutionResult) -> usize {
    row_count(result).saturating_sub(1)
}
