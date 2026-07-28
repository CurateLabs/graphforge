//! IR JSON golden tests — AST→IR corpus (issue #569).
//!
//! Each test runs `parse → bind → serialise GraphPlan` and compares the result
//! against a committed JSON fixture in `tests/ir_goldens/`.
//!
//! **Updating fixtures:**
//! ```
//! INSTA_UPDATE=always cargo test -p gf-ir --test golden
//! ```

use std::path::Path;
use std::sync::{Arc, Mutex};

use gf_ir::{Binder, OntologyMode, RuntimeCatalog};
use gf_ontology::{OntologyCompiler, OntologyHandle, OntologyLoader};

// ---------------------------------------------------------------------------
// Setup helpers
// ---------------------------------------------------------------------------

/// Path to the shared HR ontology fixture.
fn hr_fixture() -> std::path::PathBuf {
    // The HR fixture lives in gf-ontology's test fixtures directory.
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent() // crates/
        .unwrap()
        .join("gf-ontology")
        .join("tests")
        .join("fixtures")
        .join("hr.yaml")
}

/// Build a Binder loaded with the HR ontology in Advisory mode.
///
/// Advisory mode: formal ontology is present so label/relation-type IDs are
/// stable integers (deterministic JSON); property resolution falls back to the
/// RuntimeCatalog for properties whose owner TypeId is not yet resolved at bind
/// time (deferred to M13 type-inference).  This gives the best determinism
/// currently achievable without M13.
fn hr_binder() -> Binder {
    let doc = OntologyLoader::load_file(&hr_fixture()).expect("failed to load hr.yaml");
    let runtime = OntologyCompiler::compile(&doc).expect("failed to compile HR ontology");
    let handle = OntologyHandle::new(runtime);
    let catalog = Arc::new(Mutex::new(RuntimeCatalog::new()));
    Binder::new(Some(handle), catalog, OntologyMode::Advisory)
}

/// Bind `query` with the HR ontology and return the resulting `GraphPlan`.
///
/// Panics if parse or bind fails (golden tests assert the happy path).
fn bind_query(query: &str) -> gf_ir::GraphPlan {
    let ast =
        gf_cypher::parse(query).unwrap_or_else(|e| panic!("parse failed for query {query:?}: {e}"));
    hr_binder().bind(&ast).unwrap_or_else(|errs| {
        let msgs: Vec<_> = errs.iter().map(|e| e.message.as_str()).collect();
        panic!("bind failed for query {query:?}: {msgs:?}");
    })
}

/// Run all golden tests with the snapshot path set to `tests/ir_goldens/`.
///
/// Must be called at the start of every `#[test]` in this file.
fn golden_settings() -> insta::Settings {
    let mut settings = insta::Settings::clone_current();
    settings.set_snapshot_path(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("ir_goldens"),
    );
    // Omit the module path prefix from snapshot names for cleaner file names.
    settings.set_omit_expression(true);
    settings
}

// ---------------------------------------------------------------------------
// Golden scenarios
// ---------------------------------------------------------------------------

#[test]
fn simple_node_scan() {
    let plan = bind_query("MATCH (n:Person) RETURN n.name");
    golden_settings().bind(|| {
        insta::assert_json_snapshot!("simple_node_scan", plan);
    });
}

#[test]
fn filtered_scan() {
    let plan = bind_query("MATCH (n:Person) WHERE n.age > 30 RETURN n.name");
    golden_settings().bind(|| {
        insta::assert_json_snapshot!("filtered_scan", plan);
    });
}

#[test]
fn one_hop_expand() {
    let plan = bind_query("MATCH (a:Person)-[:MANAGES]->(b:Department) RETURN a.name, b.dept_name");
    golden_settings().bind(|| {
        insta::assert_json_snapshot!("one_hop_expand", plan);
    });
}

#[test]
fn two_hop_expand() {
    let plan = bind_query(
        "MATCH (a:Employee)-[:REPORTS_TO]->(b:Manager)-[:MANAGES]->(c:Department) \
         RETURN c.dept_name",
    );
    golden_settings().bind(|| {
        insta::assert_json_snapshot!("two_hop_expand", plan);
    });
}

#[test]
fn variable_length_expand() {
    let plan = bind_query("MATCH (a:Person)-[:IS_FRIEND_OF*1..3]->(b:Person) RETURN b.name");
    golden_settings().bind(|| {
        insta::assert_json_snapshot!("variable_length_expand", plan);
    });
}

#[test]
fn named_path_fixed_segment_and_return_p() {
    // #754 second slice: a fixed single hop composes from scalar columns
    // (_path_fixed_length / _node_struct_list / _rel_struct_list) and bare
    // `RETURN p` rewrites to _path_struct(<nodes>, <relationships>).
    let plan = bind_query(
        "MATCH p = (a:Person)-[:IS_FRIEND_OF]->(b:Person) \
         RETURN length(p) AS hops, nodes(p) AS ns, relationships(p) AS rels, p",
    );
    golden_settings().bind(|| {
        insta::assert_json_snapshot!("named_path_fixed_segment_and_return_p", plan);
    });
}

#[test]
fn named_path_variable_functions() {
    // #754: nodes(p)/relationships(p)/length(p) are bind-time rewrites onto
    // the path's constituent vars — the snapshot shows `relationships(p)` as
    // the edge VarRef, `length(p)` as length(<edge>), and `nodes(p)` as the
    // internal `_path_nodes(<start>, <edge>)` call. `p` itself never appears.
    let plan = bind_query(
        "MATCH p = (a:Person)-[:IS_FRIEND_OF*1..2]->(b:Person) \
         RETURN length(p) AS hops, relationships(p) AS rels, nodes(p) AS ns",
    );
    golden_settings().bind(|| {
        insta::assert_json_snapshot!("named_path_variable_functions", plan);
    });
}

#[test]
fn optional_match() {
    let plan = bind_query(
        "MATCH (a:Employee) \
         OPTIONAL MATCH (a)-[:REPORTS_TO]->(b:Manager) \
         RETURN a.employee_id, b.title",
    );
    golden_settings().bind(|| {
        insta::assert_json_snapshot!("optional_match", plan);
    });
}

#[test]
fn aggregation() {
    let plan = bind_query("MATCH (n:Person) RETURN count(n) AS total");
    golden_settings().bind(|| {
        insta::assert_json_snapshot!("aggregation", plan);
    });
}

#[test]
fn order_by_limit() {
    let plan = bind_query("MATCH (n:Person) RETURN n.name ORDER BY n.name DESC LIMIT 10");
    golden_settings().bind(|| {
        insta::assert_json_snapshot!("order_by_limit", plan);
    });
}

#[test]
fn with_pipeline() {
    // WITH projects/renames mid-pipeline and resets the scope to its aliases
    // (#814): `nm` is introduced (its `out_var`) and the WHERE + RETURN resolve
    // against it. Carrying a whole node (`WITH n`) is deferred — #814 follow-up.
    let plan = bind_query("MATCH (n:Person) WITH n.name AS nm WHERE nm = 'Alice' RETURN nm");
    golden_settings().bind(|| {
        insta::assert_json_snapshot!("with_pipeline", plan);
    });
}

#[test]
fn unwind() {
    let plan = bind_query("UNWIND [1,2,3] AS x RETURN x");
    golden_settings().bind(|| {
        insta::assert_json_snapshot!("unwind", plan);
    });
}

#[test]
fn parameter() {
    let plan = bind_query("MATCH (n:Employee) WHERE n.employee_id = $eid RETURN n.name");
    golden_settings().bind(|| {
        insta::assert_json_snapshot!("parameter", plan);
    });
}
