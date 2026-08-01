//! Logical-plan golden tests — DataFusion `LogicalPlan` corpus (issue #578).
//!
//! Each test runs `parse → bind → lower → optimise → render` and compares the
//! indented plan text (with per-node schemas) against a committed insta
//! snapshot in `tests/logical_plan_goldens/`.  This mirrors the IR golden
//! harness in `crates/graphforge-ir/tests/golden.rs`.
//!
//! **Updating fixtures:**
//! ```
//! INSTA_UPDATE=always cargo test -p graphforge-rel --test logical_plan_golden
//! ```
//!
//! ## Why these queries
//!
//! Milestone 12 lowers a deliberately small subset.  Several constructs from
//! the original issue corpus cannot lower yet and are intentionally avoided:
//!
//! - **Property / variable projection** (`RETURN n.name`, `RETURN n`) — node
//!   property tables are a later milestone, so the only columns present are the
//!   topology columns (`node_id`, `type_id`, …).  Scenarios therefore project a
//!   literal (`RETURN 1 AS one`); the node scan still produces the `TableScan`
//!   the test is about.
//! - **`count(...)` aggregation** — `count` is not yet a recognised built-in in
//!   the expression lowerer, so aggregation scenarios are deferred.
//! - **List literals in `UNWIND`** — list-literal lowering is deferred, so the
//!   UNWIND scenario iterates a `$param` instead.
//!
//! Binding uses the HR ontology in **Advisory mode** so that label/relation
//! type IDs are stable integers (deterministic snapshots) and typed edge scans
//! / variable-length expands can resolve their relation-type names.
//!
//! Snapshots are rendered with [`graphforge_rel::explain_logical_with`], which runs the
//! analyzer + optimizer and falls back to the pre-optimisation plan when the
//! optimizer rejects it (graph-native `Extension` stubs and `$param`
//! placeholders both fall back — see the helper docs).

use std::path::Path;
use std::sync::{Arc, Mutex};

use graphforge_ir::{Binder, OntologyMode, RuntimeCatalog};
use graphforge_ontology::{OntologyCompiler, OntologyHandle, OntologyLoader};

// ---------------------------------------------------------------------------
// Setup helpers (mirrors crates/graphforge-ir/tests/golden.rs)
// ---------------------------------------------------------------------------

/// Path to the shared HR ontology fixture.
fn hr_fixture() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent() // crates/
        .unwrap()
        .join("graphforge-ontology")
        .join("tests")
        .join("fixtures")
        .join("hr.yaml")
}

/// Compile the HR ontology into a reusable [`OntologyHandle`].
fn hr_handle() -> OntologyHandle {
    let doc = OntologyLoader::load_file(&hr_fixture()).expect("failed to load hr.yaml");
    let runtime = OntologyCompiler::compile(&doc).expect("failed to compile HR ontology");
    OntologyHandle::new(runtime)
}

/// Build a Binder loaded with the HR ontology in Advisory mode.
fn hr_binder() -> Binder {
    let catalog = Arc::new(Mutex::new(RuntimeCatalog::new()));
    Binder::new(Some(hr_handle()), catalog, OntologyMode::Advisory)
}

/// Bind `query` with the HR ontology and return the resulting `GraphPlan`.
fn bind_query(query: &str) -> graphforge_ir::GraphPlan {
    let ast = graphforge_cypher::parse(query)
        .unwrap_or_else(|e| panic!("parse failed for query {query:?}: {e}"));
    hr_binder().bind(&ast).unwrap_or_else(|errs| {
        let msgs: Vec<_> = errs.iter().map(|e| e.message.as_str()).collect();
        panic!("bind failed for query {query:?}: {msgs:?}");
    })
}

/// Lower + optimise `query` (ontology-aware) and render the plan as text.
fn render(query: &str) -> String {
    let plan = bind_query(query);
    let handle = hr_handle();
    graphforge_rel::explain_logical_with(&plan, Some(&handle))
        .unwrap_or_else(|e| panic!("lowering failed for query {query:?}: {e}"))
}

/// Lower `query` (ontology-aware) WITHOUT optimisation and render as text.
///
/// Used for the graph-native `Extension` stub scenarios: the optimizer's
/// column-pruning eliminates an Extension whose output columns are unreferenced
/// (these queries project a literal), so the stub only survives in the
/// pre-optimisation plan — which is the plan shape this milestone is testing.
fn render_lowered(query: &str) -> String {
    let plan = bind_query(query);
    let handle = hr_handle();
    // A project directory is supplied (via `new_with_dir`) so variable-length
    // `Expand` can bake its edge-read path; the directory is not rendered in
    // the explain output, so a throwaway temp dir keeps snapshots deterministic.
    let dir = tempfile::tempdir().expect("tempdir");
    let lowered = graphforge_rel::GraphPlanLowerer::new_with_dir(
        None,
        Some(&handle),
        dir.path(),
        OntologyMode::Advisory,
    )
    .lower_plan(&plan)
    .unwrap_or_else(|e| panic!("lowering failed for query {query:?}: {e}"));
    lowered.display_indent_schema().to_string()
}

/// Lower a property-reading query against a real (Parquet-backed) catalog,
/// rendered pre-optimisation so the property JOIN shape is visible.
///
/// Binds in exploratory mode sharing one runtime catalog (so `name` is
/// interned), writes a `Person {name}` node so `properties/_untyped.parquet`
/// gains a `name` column, then lowers with that catalog + project dir. The
/// directory is not part of the rendered plan, so snapshots stay deterministic.
fn render_property_read(query: &str) -> String {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use graphforge_core::TypeId;
    use graphforge_core::uuid::new_v7;
    use graphforge_ir::{Binder, IrLiteral, RuntimeCatalog};
    use graphforge_storage::{GraphCatalog, GraphWriter};

    let rc = Arc::new(Mutex::new(RuntimeCatalog::new()));
    let binder = Binder::new(None, rc.clone(), OntologyMode::Exploratory);
    let ast = graphforge_cypher::parse(query).unwrap_or_else(|e| panic!("parse {query:?}: {e}"));
    let plan = binder
        .bind(&ast)
        .unwrap_or_else(|_| panic!("bind {query:?}"));

    // Write one node with a `name` property so `properties/_untyped.parquet`
    // exists with a `name` column for the JOIN to discover.
    let dir = tempfile::tempdir().expect("tempdir");
    let mut writer = GraphWriter::open(dir.path(), OntologyMode::Exploratory).expect("writer");
    let uuid = new_v7();
    writer.create_node(uuid, TypeId(0)).expect("create_node");
    let mut props = HashMap::new();
    props.insert("name".to_owned(), IrLiteral::Str("Alice".to_owned()));
    writer
        .set_properties(&uuid, None, props)
        .expect("set_properties");
    writer.flush().expect("flush");

    let catalog = GraphCatalog::open(dir.path(), None, &rc.lock().unwrap()).expect("catalog");
    let lowered = graphforge_rel::GraphPlanLowerer::new_with_dir(
        Some(&catalog),
        None,
        dir.path(),
        OntologyMode::Exploratory,
    )
    .lower_plan(&plan)
    .unwrap_or_else(|e| panic!("lowering failed for query {query:?}: {e}"));
    lowered.display_indent_schema().to_string()
}

/// insta settings with the snapshot path set to `tests/logical_plan_goldens/`.
fn golden_settings() -> insta::Settings {
    let mut settings = insta::Settings::clone_current();
    settings.set_snapshot_path(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("logical_plan_goldens"),
    );
    settings.set_omit_expression(true);
    settings
}

// ---------------------------------------------------------------------------
// Golden scenarios — relational (optimised)
// ---------------------------------------------------------------------------

#[test]
fn simple_node_scan() {
    let plan = render("MATCH (n:Person) RETURN 1 AS one");
    golden_settings().bind(|| insta::assert_snapshot!("simple_node_scan", plan));
}

#[test]
fn unlabeled_scan() {
    let plan = render("MATCH (n) RETURN 1 AS one");
    golden_settings().bind(|| insta::assert_snapshot!("unlabeled_scan", plan));
}

#[test]
fn one_hop_expand() {
    let plan = render("MATCH (a:Employee)-[:WORKS_IN]->(b:Department) RETURN 1 AS one");
    golden_settings().bind(|| insta::assert_snapshot!("one_hop_expand", plan));
}

#[test]
fn property_read_joins_property_table() {
    // #704: `RETURN n.name` lowers to a LEFT join of the property table onto the
    // node scan (on node_uuid), projecting the real `name` column re-qualified
    // under the node's `var_N`. Rendered pre-optimisation against a real catalog.
    let plan = render_property_read("MATCH (n:Person) RETURN n.name AS name");
    assert!(
        plan.contains("Left Join") && plan.contains("node_uuid"),
        "expected a property-table LEFT join on node_uuid:\n{plan}"
    );
    golden_settings().bind(|| insta::assert_snapshot!("property_read", plan));
}

#[test]
fn undirected_expand() {
    let plan = render("MATCH (a:Person)-[:IS_FRIEND_OF]-(b:Person) RETURN 1 AS one");
    golden_settings().bind(|| insta::assert_snapshot!("undirected_expand", plan));
}

#[test]
fn order_by_limit() {
    let plan = render("MATCH (n:Person) RETURN 1 AS one ORDER BY 1 LIMIT 10");
    golden_settings().bind(|| insta::assert_snapshot!("order_by_limit", plan));
}

// ---------------------------------------------------------------------------
// Golden scenarios — filtered / parameterised (optimizer falls back)
// ---------------------------------------------------------------------------

#[test]
fn filtered_scan() {
    // Predicate over a property column; the optimizer's TypeCoercion rejects
    // the unresolved property reference, so this renders pre-optimisation.
    let plan = render("MATCH (n:Person) WHERE n.age > 30 RETURN 1 AS one");
    golden_settings().bind(|| insta::assert_snapshot!("filtered_scan", plan));
}

#[test]
fn parameter() {
    // `$eid` lowers to a typeless Placeholder; TypeCoercion cannot resolve it
    // pre-execution, so this renders pre-optimisation.
    let plan = render("MATCH (n:Employee) WHERE n.employee_id = $eid RETURN 1 AS one");
    golden_settings().bind(|| insta::assert_snapshot!("parameter", plan));
}

// ---------------------------------------------------------------------------
// Golden scenarios — graph-native Extension stubs (graphforge-plan, physical exec M13)
// ---------------------------------------------------------------------------

#[test]
fn variable_length_expand() {
    // Plan contains the VarLenExpand Extension stub. Rendered pre-optimisation
    // (the optimizer prunes the unreferenced Extension).
    let plan = render_lowered("MATCH (a:Person)-[:IS_FRIEND_OF*1..3]->(b:Person) RETURN 1 AS one");
    assert!(
        plan.contains("VarLenExpand"),
        "expected VarLenExpand:\n{plan}"
    );
    golden_settings().bind(|| insta::assert_snapshot!("variable_length_expand", plan));
}

#[test]
fn variable_length_edge_var_length_lowers() {
    // #709: the edge var `r` binds to the relationship list, so `length(r)`
    // lowers to `array_length` over the `var_<edge>.rels` List column, and the
    // VarLenExpand Extension is retained (it is referenced by the projection).
    let plan = render_lowered(
        "MATCH (a:Person)-[r:IS_FRIEND_OF*1..3]->(b:Person) RETURN length(r) AS hops",
    );
    assert!(
        plan.contains("VarLenExpand"),
        "VarLenExpand must be retained when the edge var is referenced:\n{plan}"
    );
    assert!(
        plan.contains("array_length"),
        "length(r) must lower to array_length:\n{plan}"
    );
    // Snapshot the full plan shape so regressions outside those two tokens are
    // also caught.
    golden_settings().bind(|| insta::assert_snapshot!("variable_length_edge_var_length", plan));
}

#[test]
fn named_path_variable_functions_lower() {
    // #754: nodes(p) lowers to cypher_path_nodes over the start node's uuid
    // column and the relationship list; relationships(p) is the list column
    // itself; length(p) is array_length over it.
    let plan = render_lowered(
        "MATCH p = (a:Person)-[:IS_FRIEND_OF*1..2]->(b:Person) \
         RETURN length(p) AS hops, relationships(p) AS rels, nodes(p) AS ns",
    );
    assert!(
        plan.contains("cypher_path_nodes"),
        "nodes(p) must lower to the cypher_path_nodes UDF:\n{plan}"
    );
    assert!(
        plan.contains("node_uuid"),
        "the walk must be seeded with the start node's uuid column:\n{plan}"
    );
    assert!(
        plan.contains("array_length"),
        "length(p) must lower to array_length over the rels list:\n{plan}"
    );
    golden_settings().bind(|| insta::assert_snapshot!("named_path_variable_functions", plan));
}

#[test]
fn named_path_fixed_segment_and_return_p_lower() {
    // #754 second slice: a fixed hop's path functions compose from the join's
    // scalar columns — named_struct over edge_uuid/src_uuid/dst_uuid with the
    // bind-time rel_type literal, make_array for the lists, a UInt64 constant
    // for length(p) — and bare `RETURN p` is named_struct(nodes, relationships).
    let plan = render_lowered(
        "MATCH p = (a:Person)-[:IS_FRIEND_OF]->(b:Person) \
         RETURN length(p) AS hops, nodes(p) AS ns, relationships(p) AS rels, p AS p",
    );
    assert!(
        plan.contains("named_struct"),
        "fixed-segment structs and RETURN p must use named_struct:\n{plan}"
    );
    assert!(
        plan.contains("IS_FRIEND_OF"),
        "rel_type must carry the bind-time relation name:\n{plan}"
    );
    golden_settings().bind(|| {
        insta::assert_snapshot!("named_path_fixed_segment_and_return_p", plan);
    });
}

#[test]
fn optional_match() {
    // Plan contains the OptionalMatch Extension stub. Rendered pre-optimisation.
    let plan = render_lowered(
        "MATCH (a:Employee) \
         OPTIONAL MATCH (a)-[:REPORTS_TO]->(b:Manager) \
         RETURN 1 AS one",
    );
    assert!(
        plan.contains("OptionalMatch"),
        "expected OptionalMatch:\n{plan}"
    );
    golden_settings().bind(|| insta::assert_snapshot!("optional_match", plan));
}

#[test]
fn unwind() {
    // Plan contains the Unwind Extension stub. Iterates a `$param`. Rendered
    // pre-optimisation.
    let plan = render_lowered("UNWIND $items AS x RETURN 1 AS one");
    assert!(plan.contains("Unwind"), "expected Unwind:\n{plan}");
    golden_settings().bind(|| insta::assert_snapshot!("unwind", plan));
}

#[test]
fn unwind_list_literal() {
    // #714: `UNWIND [1, 2, 3]` now lowers — the list literal folds to a
    // ScalarValue::List the Unwind node iterates. Rendered pre-optimisation.
    let plan = render_lowered("UNWIND [1, 2, 3] AS x RETURN 1 AS one");
    assert!(plan.contains("Unwind"), "expected Unwind:\n{plan}");
    golden_settings().bind(|| insta::assert_snapshot!("unwind_list_literal", plan));
}
