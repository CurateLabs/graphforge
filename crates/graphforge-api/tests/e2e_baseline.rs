//! End-to-end execution baseline (#584) — the full pipeline from openCypher to
//! Arrow, driven through the public [`GraphForge::execute`] facade.
//!
//! Each test builds a small fixture graph and asserts on the returned
//! [`ExecutionResult`]'s row counts and Arrow schema. This is the Milestone 13
//! exit gate: parse → bind → lower → execute → Arrow, against a real
//! (temporary) Parquet-backed graph.
//!
//! ## Fixture
//!
//! 5 `Person` nodes (Alice 30, Bob 25, Carol 35, Dave 28, Eve 22), 4 `KNOWS`
//! edges (Alice→Bob, Bob→Carol, Carol→Dave, Alice→Carol) and 1 `LIKES` edge
//! (Dave→Eve), created in one `CREATE`. (Separate `CREATE`s also accumulate now
//! that the writer appends on flush (#733) — see `incremental_create_accumulates`.)
//!
//! All #584 scenarios are now covered: scans, filters, `count()`, traversal
//! (single/two-hop, variable-length), `OPTIONAL MATCH`, `UNWIND`, `ORDER BY`/
//! `LIMIT`, incremental `CREATE`, query parameters, the Arrow result contract,
//! and (via #725) `execute_stream` plus cross-session RuntimeCatalog persistence.

use std::collections::HashMap;

use arrow::array::{
    Array, BooleanArray, FixedSizeBinaryArray, Float64Array, Int64Array, ListArray, StringArray,
    StructArray, UInt64Array,
};
use arrow::datatypes::DataType;
use graphforge_api::{ExecutionResult, GraphForge};
use graphforge_ir::IrLiteral;

/// Build the shared fixture graph in a single `CREATE`. (Incremental `CREATE`s
/// also accumulate now — see `incremental_create_accumulates`; one statement
/// just keeps the fixture compact.)
fn forge() -> GraphForge {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute(
        "CREATE \
         (alice:Person {name:'Alice', age:30}), \
         (bob:Person {name:'Bob', age:25}), \
         (carol:Person {name:'Carol', age:35}), \
         (dave:Person {name:'Dave', age:28}), \
         (eve:Person {name:'Eve', age:22}), \
         (alice)-[:KNOWS]->(bob), \
         (bob)-[:KNOWS]->(carol), \
         (carol)-[:KNOWS]->(dave), \
         (alice)-[:KNOWS]->(carol), \
         (dave)-[:LIKES]->(eve)",
    )
    .expect("create fixture");
    gf
}

fn rows(gf: &GraphForge, cypher: &str) -> ExecutionResult {
    gf.execute(cypher)
        .unwrap_or_else(|e| panic!("execute {cypher:?} failed: {e}"))
}

fn utf8_list_cell(list: &ListArray, row: usize) -> Vec<String> {
    let values = list.value(row);
    let strings = values
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("list values are Utf8");
    (0..strings.len())
        .filter(|&i| !strings.is_null(i))
        .map(|i| strings.value(i).to_owned())
        .collect()
}

fn bool_cell(result: &ExecutionResult, column: &str, row: usize) -> Option<bool> {
    let array = result.batches[0]
        .column_by_name(column)
        .unwrap_or_else(|| panic!("missing column {column}"))
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap_or_else(|| panic!("{column} is not Boolean"));
    (!array.is_null(row)).then(|| array.value(row))
}

fn string_column_values(result: &ExecutionResult, column: &str) -> Vec<String> {
    let mut values = Vec::new();
    for batch in &result.batches {
        let array = batch
            .column_by_name(column)
            .unwrap_or_else(|| panic!("missing column {column}"))
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("{column} is not Utf8"));
        for row in 0..array.len() {
            if !array.is_null(row) {
                values.push(array.value(row).to_owned());
            }
        }
    }
    values
}

// ---------------------------------------------------------------------------
// Read — simple scans
// ---------------------------------------------------------------------------

#[test]
fn scan_all_persons() {
    let gf = forge();
    let r = rows(&gf, "MATCH (n:Person) RETURN n.node_uuid AS node_uuid");
    assert_eq!(r.stats.rows_produced, 5);
    // node_uuid is FixedSizeBinary(16) — the UUID identity, never an integer.
    let f = r
        .schema
        .field_with_name("node_uuid")
        .expect("node_uuid column");
    assert_eq!(f.data_type(), &DataType::FixedSizeBinary(16));
}

#[test]
fn filtered_scan_by_age() {
    let gf = forge();
    // age > 28: Alice (30) and Carol (35). (Dave is 28, not > 28.)
    let r = rows(&gf, "MATCH (n:Person) WHERE n.age > 28 RETURN n.node_uuid");
    assert_eq!(r.stats.rows_produced, 2);
}

#[test]
fn inline_node_property_filters_match() {
    // #748: an inline property map filters like the equivalent WHERE clause.
    let gf = forge();
    // Exactly one Alice.
    let alice = rows(&gf, "MATCH (n:Person {name:'Alice'}) RETURN n.node_uuid");
    assert_eq!(alice.stats.rows_produced, 1, "only Alice matches");
    // No match → zero rows (not "all Persons").
    let none = rows(&gf, "MATCH (n:Person {name:'Nobody'}) RETURN n.node_uuid");
    assert_eq!(none.stats.rows_produced, 0, "no Person named Nobody");
}

#[test]
fn match_returns_whole_node_value() {
    // #785: a bare `RETURN n` materializes a whole node value — identity +
    // labels + readable properties — not just a uuid column.
    let gf = forge();
    let r = rows(&gf, "MATCH (n:Person {name:'Alice'}) RETURN n");
    assert_eq!(r.stats.rows_produced, 1, "only Alice matches");
    let batch = r
        .batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .expect("a non-empty result batch");
    let col = batch.column_by_name("n").expect("result column `n`");
    let node = col
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap_or_else(|| {
            panic!(
                "`n` should be a node-value Struct, got {:?}",
                col.data_type()
            )
        });

    // Identity present.
    assert!(
        node.column_by_name("node_uuid").is_some(),
        "carries node_uuid"
    );

    // Label present and correct.
    let labels = node.column_by_name("labels").expect("labels field");
    let list = labels
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("labels is a List");
    let label_vals = list.value(0);
    let label_strs = label_vals
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("labels are Utf8");
    assert_eq!(label_strs.value(0), "Person");

    // A property is readable off the node value.
    let name = node
        .column_by_name("name")
        .expect("name property in node value");
    let name_str = name
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name is Utf8");
    assert_eq!(name_str.value(0), "Alice");
}

#[test]
fn unlabelled_match_returns_node_with_resolved_label_and_props() {
    // #889: `MATCH (n) RETURN n` with NO label in the pattern resolves the
    // node's label name from its stored type_id (via the catalog reverse-map +
    // a runtime CASE) and carries its properties — not just a uuid. #785
    // required the label to be written in the pattern; this lifts that.
    let gf = forge();
    let r = rows(&gf, "MATCH (n) WHERE n.name = 'Alice' RETURN n");
    assert_eq!(r.stats.rows_produced, 1, "one Alice");
    let batch = r
        .batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .expect("a non-empty result batch");
    let node = batch
        .column_by_name("n")
        .expect("result column `n`")
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("node-value Struct");
    // Label recovered from type_id even though the pattern was unlabelled.
    let labels = node.column_by_name("labels").expect("labels field");
    let list = labels
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("labels is a List");
    let label_vals = list.value(0);
    let label_strs = label_vals
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("labels are Utf8");
    assert_eq!(
        label_strs.value(0),
        "Person",
        "unlabelled match resolves the label from type_id"
    );
    // Property carried through (joined from `_untyped` for the unlabelled scan).
    let name = node
        .column_by_name("name")
        .expect("name property in node value")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name is Utf8");
    assert_eq!(name.value(0), "Alice");
}

#[test]
fn start_end_node_return_whole_node() {
    // #753: startNode(r) / endNode(r) over a matched relationship return the
    // endpoint node's whole value (identity + labels + properties), reusing the
    // #785 materialization — not the raw UUID.
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'}) RETURN startNode(r)",
    );
    assert_eq!(r.stats.rows_produced, 1, "Alice KNOWS Bob is one edge");
    let batch = r
        .batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .expect("a non-empty result batch");
    // The column name is the expression display string (no friendly alias for a
    // function call), so address it positionally.
    let col = batch.column(0);
    let node = col
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap_or_else(|| {
            panic!(
                "startNode(r) should be a node-value Struct, got {:?}",
                col.data_type()
            )
        });
    // The start node is Alice — label + property both readable off the value.
    let labels = node.column_by_name("labels").expect("labels field");
    let list = labels
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("labels is a List");
    let label_vals = list.value(0);
    let label_strs = label_vals
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("labels are Utf8");
    assert_eq!(label_strs.value(0), "Person");
    let name = node.column_by_name("name").expect("name property");
    let name_str = name
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name is Utf8");
    assert_eq!(name_str.value(0), "Alice", "startNode(r) is the start node");
}

#[test]
fn start_end_node_property_access() {
    // #753: property access on startNode(r) / endNode(r) resolves against the
    // src / dst node's columns — `startNode(r).name` == the start node's name.
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'}) \
         RETURN startNode(r).name AS s, endNode(r).name AS e",
    );
    assert_eq!(r.stats.rows_produced, 1);
    let batch = r
        .batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .expect("a non-empty result batch");
    let s = batch
        .column_by_name("s")
        .expect("s")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Utf8");
    let e = batch
        .column_by_name("e")
        .expect("e")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Utf8");
    assert_eq!(s.value(0), "Alice", "startNode(r).name");
    assert_eq!(e.value(0), "Bob", "endNode(r).name");
}

#[test]
fn start_end_node_respects_incoming_direction() {
    // #753: the relationship's start/end follow the edge *direction*, not the
    // pattern's left/right order. For `(Bob)<-[r:KNOWS]-(b)`, the only match is
    // Alice KNOWS Bob, so startNode(r) is Alice (the edge source) and endNode(r)
    // is Bob — even though Bob is the pattern's left-hand node.
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (a:Person {name:'Bob'})<-[r:KNOWS]-(b:Person) \
         RETURN startNode(r).name AS s, endNode(r).name AS e",
    );
    assert_eq!(r.stats.rows_produced, 1, "only Alice KNOWS Bob");
    let batch = r
        .batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .expect("a non-empty result batch");
    let s = batch
        .column_by_name("s")
        .expect("s")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Utf8");
    let e = batch
        .column_by_name("e")
        .expect("e")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Utf8");
    assert_eq!(
        s.value(0),
        "Alice",
        "startNode = edge source, not pattern-left"
    );
    assert_eq!(e.value(0), "Bob", "endNode = edge target");
}

#[test]
fn start_node_over_optional_edge_is_deferred() {
    // #753 deferral: over an OPTIONAL-matched relationship, `r` is null on an
    // unmatched row and Cypher requires `startNode(r)` to be null. Honoring that
    // needs an edge-uuid null gate on endpoint materialization (#889), so the
    // rewrite is suppressed for optional edges. The suppression surfaces as a
    // bind-time `UnknownFunction` (data-independent), but the query uses Eve —
    // who has no outgoing edges — so the represented scenario is a genuinely
    // unmatched optional, not one papered over by a row that happens to match.
    let gf = forge();
    let res = gf.execute(
        "MATCH (a:Person {name:'Eve'}) OPTIONAL MATCH (a)-[r:KNOWS]->(b) RETURN startNode(r)",
    );
    assert!(
        res.is_err(),
        "optional-edge startNode is deferred; expected an error, got {res:?}"
    );
}

#[test]
fn count_aggregate() {
    let gf = forge();
    let r = rows(&gf, "MATCH (n:Person) RETURN count(n) AS total");
    assert_eq!(r.stats.rows_produced, 1);
    let total = r.batches[0]
        .column_by_name("total")
        .expect("total")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64")
        .value(0);
    assert_eq!(total, 5);
}

#[test]
fn builtins_reverse_tail_coalesce_sign_append() {
    // #955: a batch of list/string/math built-ins, including the polymorphic
    // `reverse` (list vs string) and `list + element` append.
    let gf = forge();
    let r = rows(&gf, "RETURN reverse([1,2,3]) AS l, reverse('abc') AS s");
    assert_eq!(r.stats.rows_produced, 1);
    let r = rows(
        &gf,
        "RETURN tail([1,2,3]) AS t, coalesce(null, 7) AS c, sign(-4) AS sg, [1,2] + 3 AS app",
    );
    assert_eq!(r.stats.rows_produced, 1);
}

#[test]
fn leading_with_then_match_cross_joins() {
    // #920: a leading `WITH <const> AS x` produces one row; a following MATCH
    // must cross-join with it (one row per matched node), not collapse to empty.
    // Previously the source-op base was zero-row, so the projection emitted no
    // rows and the cross-join was empty.
    let gf = forge();
    let r = rows(&gf, "WITH 1 AS x MATCH (n:Person) RETURN x, n.name AS name");
    assert_eq!(
        r.stats.rows_produced, 5,
        "WITH-then-MATCH cross-joins the unit row with all 5 Persons"
    );
}

#[test]
fn multi_pattern_match_is_a_cross_product() {
    // Comma-separated patterns (`MATCH (a), (b)`) are a Cartesian product of the
    // two matches — 5 Persons × 5 Persons = 25 rows. Previously the second scan
    // replaced the first (only `b` survived); now they cross-join.
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (a:Person), (b:Person) RETURN a.node_uuid, b.node_uuid",
    );
    assert_eq!(r.stats.rows_produced, 25, "5 × 5 cross product");
}

#[test]
fn multi_pattern_match_with_where_joins_disconnected_vars() {
    // Both disconnected pattern variables resolve after the cross product, so a
    // WHERE referencing both filters the product (here to a single pair).
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (a:Person), (b:Person) WHERE a.name = 'Alice' AND b.name = 'Bob' \
         RETURN a.name AS an, b.name AS bn",
    );
    assert_eq!(r.stats.rows_produced, 1);
    let batch = r
        .batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .expect("a non-empty result batch");
    let an = batch
        .column_by_name("an")
        .expect("an")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Utf8");
    let bn = batch
        .column_by_name("bn")
        .expect("bn")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Utf8");
    assert_eq!(an.value(0), "Alice");
    assert_eq!(bn.value(0), "Bob");
}

#[test]
fn node_equality_compares_identity() {
    // #598: `a = b` / `a <> b` over node variables compares node identity
    // (node_uuid), not the bare (multi-column) var qualifier.
    let gf = forge();
    let eq = rows(
        &gf,
        "MATCH (a:Person), (b:Person) WHERE a = b RETURN a.name AS n",
    );
    assert_eq!(
        eq.stats.rows_produced, 5,
        "each of 5 Persons equals only itself"
    );
    let ne = rows(
        &gf,
        "MATCH (a:Person), (b:Person) WHERE a <> b RETURN a.name AS n",
    );
    assert_eq!(
        ne.stats.rows_produced, 20,
        "5×5 cross product minus 5 self-pairs"
    );
}

#[test]
fn unlabelled_bound_dst_property_resolves() {
    // #598: an unlabelled bound endpoint (`x` in `(n)-[r]->(x)`) gets its
    // properties joined, so `WHERE`/`RETURN` on `x.<prop>` resolves.
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (n:Person)-[:KNOWS]->(x) WHERE x.name = 'Bob' RETURN n.name AS who",
    );
    assert_eq!(r.stats.rows_produced, 1, "only Alice KNOWS Bob");
    let who = r.batches[0]
        .column_by_name("who")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(who.value(0), "Alice");
}

#[test]
fn connected_anonymous_pattern_counts_relationships_not_nodes() {
    // Regression (root cause of the multi-pattern work): an anonymous connected
    // pattern `(:Person)-[:KNOWS]->(:Person)` must count the 4 KNOWS edges — not
    // collapse to a Person scan (5) or a cross product. The trailing anonymous
    // node now binds to the Expand's dst rather than minting a fresh, dangling var.
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (:Person)-[:KNOWS]->(:Person) RETURN count(*) AS c",
    );
    let c = r.batches[0]
        .column_by_name("c")
        .expect("c")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64")
        .value(0);
    assert_eq!(c, 4, "four KNOWS edges in the fixture");
}

#[test]
fn with_projects_and_renames() {
    // #814: WITH is a mid-pipeline projection; the alias carries to RETURN.
    let gf = forge();
    let r = rows(&gf, "MATCH (n:Person) WITH n.name AS nm RETURN nm");
    assert_eq!(
        r.stats.rows_produced, 5,
        "all five names project through WITH"
    );
}

#[test]
fn with_where_filters_post_projection() {
    // #814: a WITH ... WHERE filters on the projected alias.
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (n:Person) WITH n.name AS nm WHERE nm = 'Alice' RETURN nm",
    );
    assert_eq!(
        r.stats.rows_produced, 1,
        "only Alice survives the WITH WHERE"
    );
}

#[test]
fn with_where_sees_alias_and_pre_projection_vars() {
    // #1028: WITH ... WHERE is resolved against the projection aliases and the
    // incoming scope. `name` is projected; `r` is not, but remains visible to
    // the attached WHERE predicate.
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (n:Person)-[r:KNOWS]->() \
         WITH n.name AS name WHERE name = 'Alice' AND r IS NOT NULL \
         RETURN name ORDER BY name",
    );
    assert_eq!(r.stats.rows_produced, 2, "Alice has two KNOWS edges");
}

#[test]
fn with_where_supports_fixed_pattern_predicate() {
    // #1067: WITH ... WHERE uses the same fixed single-hop pattern predicate
    // semantics as MATCH WHERE, filtering by existence without multiplying rows.
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (n:Person) \
         WITH n WHERE (n)-[:KNOWS]->() \
         RETURN n.name AS name ORDER BY name",
    );
    assert_eq!(
        string_column_values(&r, "name"),
        vec!["Alice", "Bob", "Carol"]
    );
}

#[test]
fn with_star_expands_all_in_scope_vars() {
    // #1028: WITH * carries the current named scope forward. The relationship
    // var is deliberately kept live too, so the downstream WHERE can see it.
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (a:Person)-[r:KNOWS]->(b:Person) \
         WITH * WHERE a.name = 'Alice' AND r IS NOT NULL \
         RETURN b.name AS name ORDER BY name",
    );
    assert_eq!(r.stats.rows_produced, 2, "Alice knows Bob and Carol");
    let batch = &r.batches[0];
    let names = batch
        .column_by_name("name")
        .expect("name column")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Utf8 names");
    assert_eq!(names.value(0), "Bob");
    assert_eq!(names.value(1), "Carol");
}

#[test]
fn with_distinct_deduplicates_projected_rows() {
    // #1028: WITH DISTINCT deduplicates after projection, before the downstream
    // RETURN. Two Alice KNOWS rows collapse to one projected name.
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (n:Person)-[:KNOWS]->() WITH DISTINCT n.name AS name RETURN name",
    );
    assert_eq!(r.stats.rows_produced, 3, "Alice, Bob, and Carol");
}

#[test]
fn return_order_by_sees_projection_alias() {
    // #1028: RETURN ORDER BY can sort by an alias introduced by the RETURN
    // projection, while the existing non-projected ORDER BY behavior remains.
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (n:Person) RETURN n.name AS name ORDER BY name DESC LIMIT 1",
    );
    assert_eq!(r.stats.rows_produced, 1);
    let name = r.batches[0]
        .column_by_name("name")
        .expect("name")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Utf8")
        .value(0);
    assert_eq!(name, "Eve");
}

#[test]
fn return_order_by_hidden_source_key_does_not_leak_column() {
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (n:Person) RETURN n.name AS name ORDER BY n.age LIMIT 2",
    );
    assert_eq!(string_column_values(&r, "name"), vec!["Eve", "Bob"]);
    assert_eq!(r.schema.fields().len(), 1, "ORDER BY key stays hidden");
}

#[test]
fn with_order_by_hidden_source_key_does_not_leak_column() {
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (n:Person) WITH n.name AS name ORDER BY n.age LIMIT 2 RETURN name",
    );
    assert_eq!(string_column_values(&r, "name"), vec!["Eve", "Bob"]);
    assert_eq!(r.schema.fields().len(), 1, "ORDER BY key stays hidden");
}

#[test]
fn return_distinct_entity_orders_by_projected_property() {
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (n:Person)-[:KNOWS]->() RETURN DISTINCT n ORDER BY n.name",
    );
    assert_eq!(r.stats.rows_produced, 3);
    assert_eq!(r.schema.fields().len(), 1);
    let nodes = r.batches[0]
        .column_by_name("n")
        .expect("n")
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("node struct");
    let names = nodes
        .column_by_name("name")
        .expect("node name")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Utf8 names");
    assert_eq!(
        (0..names.len())
            .map(|row| names.value(row))
            .collect::<Vec<_>>(),
        vec!["Alice", "Bob", "Carol"]
    );
}

#[test]
fn return_order_by_aggregate_expression_uses_aggregate_output() {
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (n:Person)-[:KNOWS]->() \
         RETURN n.name AS name, count(*) AS c \
         ORDER BY count(*) DESC, n.name",
    );
    assert_eq!(
        string_column_values(&r, "name"),
        vec!["Alice", "Bob", "Carol"]
    );
    let counts = r.batches[0]
        .column_by_name("c")
        .expect("c")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 counts");
    assert_eq!(
        (0..counts.len())
            .map(|row| counts.value(row))
            .collect::<Vec<_>>(),
        vec![2, 1, 1]
    );
}

#[test]
fn with_where_preserves_hidden_order_key() {
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (n:Person) \
         WITH n.name AS name ORDER BY n.age LIMIT 2 WHERE n.age > 20 \
         RETURN name",
    );
    assert_eq!(string_column_values(&r, "name"), vec!["Eve", "Bob"]);
}

#[test]
fn return_rejects_order_by_only_aggregate() {
    let gf = forge();
    let result = gf.execute(
        "MATCH (n:Person)-[:KNOWS]->() \
         RETURN n.name AS name, count(*) AS c \
         ORDER BY max(n.age) DESC",
    );
    assert!(result.is_err(), "ORDER BY-only aggregate must be rejected");
}

#[test]
fn return_order_by_rewrites_alias_inside_case() {
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (n:Person) RETURN n.age AS age \
         ORDER BY CASE WHEN age IS NULL THEN 0 ELSE age END LIMIT 1",
    );
    let ages = r.batches[0]
        .column_by_name("age")
        .expect("age")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 ages");
    assert_eq!(ages.value(0), 22);
}

#[test]
fn return_distinct_matches_expression_with_literal_argument() {
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (n:Person) \
         RETURN DISTINCT coalesce(n.age, 0) AS age \
         ORDER BY coalesce(n.age, 0) LIMIT 1",
    );
    let ages = r.batches[0]
        .column_by_name("age")
        .expect("age")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 ages");
    assert_eq!(ages.value(0), 22);
}

#[test]
fn with_order_by_disambiguates_scalar_alias_from_forwarded_property() {
    // #1051: forwarding `n` keeps `var_0.name`, while the scalar alias is a
    // separate value. Re-projecting both must not create an ambiguous schema.
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (n:Person) \
         WITH n, n.name AS name \
         WITH n, name ORDER BY name LIMIT 1 \
         RETURN n.name AS original, name",
    );
    assert_eq!(r.stats.rows_produced, 1);
    let batch = &r.batches[0];
    let original = batch
        .column_by_name("original")
        .expect("original")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Utf8 original")
        .value(0);
    let name = batch
        .column_by_name("name")
        .expect("name")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Utf8 name")
        .value(0);
    assert_eq!(original, "Alice");
    assert_eq!(name, original);
}

#[test]
fn with_resets_scope_dropping_unprojected_vars() {
    // #814: WITH resets the scope to exactly its projected aliases — a variable
    // not carried forward is out of scope afterwards, so referencing it is an
    // error (not a silently-wrong result). `b` is dropped by `WITH a.name AS x`.
    let gf = forge();
    let res = gf.execute("MATCH (a:Person)-[:KNOWS]->(b:Person) WITH a.name AS x RETURN b.name");
    assert!(
        res.is_err(),
        "`b` is out of scope after WITH a.name AS x; expected an error, got {res:?}"
    );
}

#[test]
fn with_forwards_whole_node_then_property() {
    // #814: a whole node is carried through WITH (`WITH n`) — all its columns
    // forward, so a later WHERE / property access still resolves. Alice (30) and
    // Carol (35) are > 28.
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (n:Person) WITH n WHERE n.age > 28 RETURN n.name",
    );
    assert_eq!(r.stats.rows_produced, 2, "Alice (30) and Carol (35)");
}

#[test]
fn with_forwards_whole_node_then_node_value() {
    // #814 + #785: a node forwarded through WITH still materializes as a whole
    // node value in a terminal RETURN (label + properties).
    let gf = forge();
    let r = rows(&gf, "MATCH (n:Person {name:'Alice'}) WITH n RETURN n");
    assert_eq!(r.stats.rows_produced, 1);
    let batch = r
        .batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .expect("a non-empty result batch");
    let node = batch
        .column_by_name("n")
        .expect("result column `n`")
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("node-value Struct after WITH");
    let name = node
        .column_by_name("name")
        .expect("name property")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Utf8");
    assert_eq!(name.value(0), "Alice");
}

#[test]
fn with_renames_whole_node_then_property_and_node_value() {
    // #814: `WITH n AS m` forwards the same whole node under a new binder-side
    // alias, so both property access and whole-node materialization work.
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (n:Person {name:'Alice'}) WITH n AS m RETURN m.name AS name, m",
    );
    assert_eq!(r.stats.rows_produced, 1);
    let batch = r
        .batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .expect("a non-empty result batch");
    let name = batch
        .column_by_name("name")
        .expect("name")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Utf8 name")
        .value(0);
    assert_eq!(name, "Alice");
    assert!(
        batch
            .column_by_name("m")
            .expect("m")
            .as_any()
            .downcast_ref::<StructArray>()
            .is_some(),
        "`m` materializes as a whole node"
    );
}

#[test]
fn with_nested_aggregate_projects_post_aggregate_expression() {
    // #958: a nested aggregate expression lowers through Aggregate -> Project.
    let gf = forge();
    let r = rows(&gf, "MATCH (n:Person) WITH count(n) + 1 AS c RETURN c");
    let c = r.batches[0]
        .column_by_name("c")
        .expect("c")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64");
    assert_eq!(c.value(0), 6);
}

#[test]
fn with_aggregate_no_group_key() {
    // #958: `WITH count(*) AS c` collapses to one row, visible downstream.
    let gf = forge();
    let r = rows(&gf, "MATCH (n:Person) WITH count(*) AS c RETURN c");
    assert_eq!(r.stats.rows_produced, 1);
    let c = r.batches[0]
        .column_by_name("c")
        .expect("c")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64")
        .value(0);
    assert_eq!(c, 5);
}

#[test]
fn with_aggregate_scalar_group_key() {
    // #958: implicit grouping on a scalar key — one row per distinct age.
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (n:Person) WITH n.age AS age, count(*) AS c RETURN age, c",
    );
    // Five distinct ages (30, 25, 35, 28, 22) → five groups.
    assert_eq!(r.stats.rows_produced, 5);
}

#[test]
fn with_aggregate_where_on_alias() {
    // #958: a WHERE on the post-aggregate alias becomes a Filter. Alice KNOWS
    // both Bob and Carol (2), so she is the only person with relCount > 1.
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) \
         WITH a.name AS name, count(*) AS relCount WHERE relCount > 1 RETURN name",
    );
    assert_eq!(r.stats.rows_produced, 1, "only Alice has >1 KNOWS edge");
}

#[test]
fn with_collect_aggregate() {
    // #958: collect rides the shared aggregate path — one row, a list of names.
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (n:Person) WITH collect(n.name) AS names RETURN names",
    );
    assert_eq!(r.stats.rows_produced, 1);
}

#[test]
fn with_aggregate_nested_in_arg_is_rejected() {
    // #958: an aggregate nested inside another aggregate's argument
    // (`sum(count(*))`) is malformed Cypher; it must be rejected, not routed to
    // the aggregate path as an ordinary inner function call.
    let gf = forge();
    let res = gf.execute("MATCH (n:Person) WITH sum(count(*)) AS c RETURN c");
    assert!(
        res.is_err(),
        "a nested aggregate inside an aggregate arg should be rejected, got {res:?}"
    );
}

#[test]
fn with_aggregate_node_endpoint_group_key() {
    // #958: a node-valued endpoint call remains a whole node through aggregation.
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH ()-[r:KNOWS]->() WITH startNode(r) AS n, count(*) AS c RETURN n, c ORDER BY c DESC",
    );
    assert_eq!(r.stats.rows_produced, 3);
    let batch = &r.batches[0];
    assert!(
        batch
            .column_by_name("n")
            .expect("n")
            .as_any()
            .downcast_ref::<StructArray>()
            .is_some(),
        "`n` materializes as a whole node"
    );
    let counts = batch
        .column_by_name("c")
        .expect("c")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64");
    assert_eq!(counts.values(), &[2, 1, 1]);
}

#[test]
fn return_star_projects_all_in_scope_vars() {
    // #598: `RETURN *` projects every in-scope named variable as its own column.
    // `(Alice)-[:KNOWS]->(b)` matches Alice→Bob and Alice→Carol → 2 rows with
    // columns `a` and `b`, each a whole node value.
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (a:Person {name:'Alice'})-[:KNOWS]->(b:Person) RETURN *",
    );
    assert_eq!(r.stats.rows_produced, 2, "Alice KNOWS Bob and Carol");
    let batch = r
        .batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .expect("a non-empty result batch");
    let a = batch
        .column_by_name("a")
        .expect("column `a` from RETURN *")
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("a is a node value");
    assert!(
        a.column_by_name("node_uuid").is_some(),
        "a carries node_uuid"
    );
    assert!(
        batch.column_by_name("b").is_some(),
        "RETURN * also projects `b`"
    );
}

#[test]
fn return_star_after_with_forwarded_node() {
    // #598 + #814: the With1 shape — forward a node through WITH, MATCH again
    // using it, then `RETURN *`. Alice is forwarded, then `(a)-[:KNOWS]->(b)`
    // re-matches from her (Bob, Carol) → 2 rows, columns `a` and `b`.
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (a:Person {name:'Alice'}) WITH a MATCH (a)-[:KNOWS]->(b:Person) RETURN *",
    );
    assert_eq!(r.stats.rows_produced, 2);
    let batch = r
        .batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .expect("a non-empty result batch");
    assert!(
        batch.column_by_name("a").is_some(),
        "`a` forwarded + returned"
    );
    assert!(
        batch.column_by_name("b").is_some(),
        "`b` matched + returned"
    );
}

// ---------------------------------------------------------------------------
// Read — traversal
// ---------------------------------------------------------------------------

#[test]
fn single_hop_knows() {
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.node_uuid, b.node_uuid",
    );
    assert_eq!(r.stats.rows_produced, 4, "4 KNOWS edges");
}

#[test]
fn two_hop_knows() {
    let gf = forge();
    // Alice→Bob→Carol, Bob→Carol→Dave, Alice→Carol→Dave.
    let r = rows(
        &gf,
        "MATCH (a:Person)-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN c.node_uuid",
    );
    assert_eq!(r.stats.rows_produced, 3);
}

// ---------------------------------------------------------------------------
// Read — properties on a fixed-expand DESTINATION node (#789)
// ---------------------------------------------------------------------------
// Fixture KNOWS edges: Alice→Bob, Bob→Carol, Carol→Dave, Alice→Carol.
// Before #789, ANY property reference on the destination `b` (RETURN/WHERE/inline)
// failed to plan ("No field named var_N.<prop>") because the trailing
// NodeScan{dst} never joined the property table.

#[test]
fn inline_property_on_destination_node_filters() {
    // Inline `{name:'Carol'}` on the destination filters the traversal.
    // Edges into Carol: Bob→Carol and Alice→Carol → 2 rows.
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (a:Person)-[:KNOWS]->(b:Person {name:'Carol'}) RETURN a.node_uuid",
    );
    assert_eq!(r.stats.rows_produced, 2, "Bob→Carol and Alice→Carol");
}

#[test]
fn where_on_destination_property_filters() {
    use arrow::array::StringArray;
    // WHERE on a destination property. Destinations with age > 28: only Carol (35)
    // is a KNOWS destination over 28 (Dave is 28, not > 28); Carol is reached from
    // Bob and Alice → 2 rows, both with b.name = 'Carol'.
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE b.age > 28 RETURN b.name AS bn",
    );
    assert_eq!(r.stats.rows_produced, 2);
    for batch in &r.batches {
        let col = batch
            .column_by_name("bn")
            .expect("bn column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("b.name is Utf8");
        for i in 0..col.len() {
            assert_eq!(col.value(i), "Carol", "only Carol is a dst with age > 28");
        }
    }
}

#[test]
fn return_destination_property_resolves() {
    use arrow::array::StringArray;
    // RETURN a plain destination property — one value per KNOWS edge (4 edges).
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.name AS bn",
    );
    assert_eq!(r.stats.rows_produced, 4, "one row per KNOWS edge");
    let mut names: Vec<String> = Vec::new();
    for batch in &r.batches {
        let col = batch
            .column_by_name("bn")
            .expect("bn column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("b.name is Utf8");
        for i in 0..col.len() {
            names.push(col.value(i).to_owned());
        }
    }
    names.sort();
    // Destinations: Bob (←Alice), Carol (←Bob), Dave (←Carol), Carol (←Alice).
    assert_eq!(names, vec!["Bob", "Carol", "Carol", "Dave"]);
}

#[test]
fn traversal_returning_only_uuids_still_works_after_dst_property_join() {
    // Regression: the append-preserving join must keep the source + edge columns,
    // so a traversal that returns only topology UUIDs is unaffected.
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.node_uuid, b.node_uuid",
    );
    assert_eq!(r.stats.rows_produced, 4, "still 4 KNOWS edges");
}

#[test]
fn edge_property_round_trips_through_match() {
    // #784: a property set on a CREATE edge persists and reads back via
    // `MATCH (a)-[r:KNOWS]->(b) RETURN r.<prop>`. Previously CREATE rejected
    // edge properties outright; now the value round-trips end-to-end.
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (a:Person {name:'Alice'})-[:KNOWS {since:2020}]->(b:Person {name:'Bob'})")
        .expect("create edge with property");

    let r = rows(
        &gf,
        "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r.since AS since",
    );
    assert_eq!(r.stats.rows_produced, 1, "one KNOWS edge");
    let since = r.batches[0]
        .column_by_name("since")
        .expect("since column")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("since is Int64");
    assert_eq!(since.value(0), 2020);
}

#[test]
fn edge_property_named_like_topology_column_does_not_break_match() {
    // #784: an edge property whose name collides with an edge-topology column
    // (`created_at`) must not build a duplicate var_<edge>.created_at field and
    // break the MATCH plan — the colliding property is dropped, the topology
    // column stays authoritative, and a non-colliding property still reads back.
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute(
        "CREATE (a:Person {name:'Alice'})-[:KNOWS {created_at: 999, weight: 7}]->(b:Person {name:'Bob'})",
    )
    .expect("create edge with a topology-colliding property name");

    // The traversal must still plan and execute (previously a duplicate
    // qualified column would fail plan building).
    let r = rows(
        &gf,
        "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r.weight AS weight",
    );
    assert_eq!(r.stats.rows_produced, 1, "the KNOWS edge still matches");
    let weight = r.batches[0]
        .column_by_name("weight")
        .expect("weight column")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("weight is Int64");
    assert_eq!(weight.value(0), 7, "non-colliding property reads back");
}

#[test]
fn inline_relationship_property_filters_match() {
    // #750: an inline relationship-property map filters the traversal like the
    // equivalent WHERE clause. Two KNOWS edges with different `since` values;
    // `{since:2020}` must match only the 2020 edge. (Previously the inline map
    // was silently dropped and BOTH edges matched.)
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute(
        "CREATE (a:Person {name:'Alice'})-[:KNOWS {since:2020}]->(b:Person {name:'Bob'}), \
         (a)-[:KNOWS {since:1999}]->(c:Person {name:'Carol'})",
    )
    .expect("create two KNOWS edges with different since values");

    // Inline filter selects only the since=2020 edge → 1 row.
    let matched = rows(
        &gf,
        "MATCH (a:Person)-[r:KNOWS {since:2020}]->(b:Person) RETURN b.node_uuid",
    );
    assert_eq!(
        matched.stats.rows_produced, 1,
        "only the since=2020 edge matches"
    );

    // A non-matching value → zero rows (not "all KNOWS edges").
    let none = rows(
        &gf,
        "MATCH (a:Person)-[r:KNOWS {since:1234}]->(b:Person) RETURN b.node_uuid",
    );
    assert_eq!(none.stats.rows_produced, 0, "no KNOWS edge has since=1234");

    // Sanity: without the inline filter, both edges match.
    let all = rows(
        &gf,
        "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN b.node_uuid",
    );
    assert_eq!(
        all.stats.rows_produced, 2,
        "both KNOWS edges without a filter"
    );
}

#[test]
fn variable_length_knows() {
    let gf = forge();
    // 1..2 hops from any Person, distinct destinations.
    let r = rows(
        &gf,
        "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) RETURN DISTINCT b.node_uuid",
    );
    assert!(
        r.stats.rows_produced >= 1,
        "reachable set is non-empty, got {}",
        r.stats.rows_produced
    );
}

// ---------------------------------------------------------------------------
// Variable-length edge-list binding (#709)
// ---------------------------------------------------------------------------

#[test]
fn variable_length_length_function_returns_hop_count() {
    // #709: the edge var `r` binds to the relationship list, so `length(r)`
    // returns the hop count per matched path. The fixture has KNOWS edges
    // Alice→Bob, Bob→Carol, Carol→Dave, Alice→Carol, so 1..2-hop paths exist
    // at hop counts 1 and 2.
    use arrow::array::UInt64Array;

    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (a:Person)-[r:KNOWS*1..2]->(b:Person) RETURN length(r) AS hops",
    );
    assert!(r.stats.rows_produced >= 1, "at least one path");
    for batch in &r.batches {
        let col = batch
            .column_by_name("hops")
            .expect("hops column")
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("array_length yields UInt64");
        for i in 0..col.len() {
            let hops = col.value(i);
            assert!(
                hops == 1 || hops == 2,
                "every 1..2-hop path has length 1 or 2, got {hops}"
            );
        }
    }
}

#[test]
fn variable_length_return_r_is_list_of_uuid_structs() {
    // `RETURN r` yields the relationship list: a List<Struct> column whose
    // struct carries the edge's UUID identity (FixedSizeBinary(16)) — and no
    // surrogate id columns leak (the UUID-only output contract).
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (a:Person)-[r:KNOWS*1..2]->(b:Person) RETURN r AS rels",
    );
    assert!(r.stats.rows_produced >= 1);

    // No top-level surrogate columns surface.
    for surrogate in ["node_id", "edge_id", "src_id", "dst_id"] {
        assert!(
            r.schema.column_with_name(surrogate).is_none(),
            "surrogate {surrogate} must not surface"
        );
    }

    // `rels` is List<Struct<… edge_uuid: FixedSizeBinary(16) …>>.
    let field = r.schema.field_with_name("rels").expect("rels column");
    let DataType::List(item) = field.data_type() else {
        panic!("rels must be a List, got {:?}", field.data_type());
    };
    let DataType::Struct(struct_fields) = item.data_type() else {
        panic!("rels item must be a Struct, got {:?}", item.data_type());
    };
    let edge_uuid = struct_fields
        .iter()
        .find(|f| f.name() == "edge_uuid")
        .expect("edge_uuid struct field");
    assert_eq!(edge_uuid.data_type(), &DataType::FixedSizeBinary(16));
}

#[test]
fn fixed_hop_return_r_materializes_relationship_struct() {
    // #889: a fixed-hop bare relationship variable is a relationship value, not
    // just its edge UUID. It carries UUID topology, rel_type, and edge props.
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (a:Person {name:'Alice'})-[:KNOWS {since:2020}]->(b:Person {name:'Bob'})")
        .expect("create relationship with property");

    let r = rows(
        &gf,
        "MATCH (:Person {name:'Alice'})-[r:KNOWS]->(:Person {name:'Bob'}) RETURN r",
    );
    assert_eq!(r.stats.rows_produced, 1);
    let batch = &r.batches[0];
    let rel = batch
        .column_by_name("r")
        .expect("r column")
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("r is a relationship Struct");
    for field in ["edge_uuid", "src_uuid", "dst_uuid", "rel_type", "since"] {
        assert!(rel.column_by_name(field).is_some(), "missing {field}");
    }
    for surrogate in ["edge_id", "src_id", "dst_id"] {
        assert!(
            rel.column_by_name(surrogate).is_none(),
            "{surrogate} must stay internal"
        );
    }
    let rel_type = rel
        .column_by_name("rel_type")
        .expect("rel_type")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("rel_type is Utf8");
    assert_eq!(rel_type.value(0), "KNOWS");
    let since = rel
        .column_by_name("since")
        .expect("since")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("since is Int64");
    assert_eq!(since.value(0), 2020);
}

#[test]
fn fixed_hop_explicit_one_to_one_materializes_relationship_list() {
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (:Person {name:'Alice'})-[r:KNOWS*1..1]->(:Person {name:'Bob'}) RETURN r",
    );
    assert_eq!(r.stats.rows_produced, 1);
    let list = r.batches[0]
        .column_by_name("r")
        .expect("r column")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("explicit variable-length relationship is a list");
    assert_eq!(list.value_length(0), 1);
    assert!(
        list.value(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("relationship list elements are structs")
            .column_by_name("edge_uuid")
            .is_some()
    );
}

#[test]
fn fixed_hop_return_r_null_propagates_for_optional_miss() {
    // #889: an unmatched optional relationship is Cypher null as a whole value,
    // not a non-null struct with null fields.
    let gf = GraphForge::new(None).expect("in-memory instance");
    let r = rows(&gf, "OPTIONAL MATCH ()-[r:DOES_NOT_EXIST]->() RETURN r");
    assert_eq!(r.stats.rows_produced, 1);
    assert!(
        r.batches[0]
            .column_by_name("r")
            .expect("r column")
            .is_null(0),
        "unmatched optional relationship should be null"
    );
}

#[test]
fn fixed_hop_return_r_survives_with_projection() {
    // #889/#1028: WITH forwards a whole relationship alias so a later RETURN
    // still materializes it as a relationship value.
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE ()-[:KNOWS {since:2020}]->()")
        .expect("create relationship");

    let r = rows(&gf, "MATCH ()-[r:KNOWS]->() WITH r RETURN r");
    assert_eq!(r.stats.rows_produced, 1);
    let rel = r.batches[0]
        .column_by_name("r")
        .expect("r column")
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("r is a relationship Struct");
    assert!(rel.column_by_name("edge_uuid").is_some());
    assert!(rel.column_by_name("since").is_some());
}

#[test]
fn fixed_hop_relationship_reuse_after_with_filters_existing_edge() {
    // #889: forwarding `WITH r` keeps relationship value metadata, but a later
    // MATCH that reuses `r` must filter the existing edge instead of scanning a
    // duplicate `var_r` edge relation.
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (:A)-[:T]->(:B)")
        .expect("create relationship");

    let same = rows(
        &gf,
        "MATCH (a1)-[r:T]->() WITH r, a1 MATCH (a1)-[r:T]->(b2) RETURN r, b2",
    );
    assert_eq!(same.stats.rows_produced, 1);
    assert!(
        same.batches[0]
            .column_by_name("r")
            .expect("r column")
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("r is a relationship Struct")
            .column_by_name("edge_uuid")
            .is_some()
    );

    let conflicting = rows(
        &gf,
        "MATCH (a1)-[r:T]->() WITH r, a1 MATCH (a1)-[r:Y]->(b2) RETURN a1, r, b2",
    );
    assert_eq!(conflicting.stats.rows_produced, 0);
}

#[test]
fn fixed_hop_type_of_relationship_var_reads_rel_type() {
    // #889: type(r) over a fixed-hop relationship variable uses the materialized
    // relationship value's rel_type field.
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (:Person {name:'Alice'})-[r:KNOWS]->(:Person {name:'Bob'}) RETURN type(r) AS t",
    );
    assert_eq!(r.stats.rows_produced, 1);
    let t = r.batches[0]
        .column_by_name("t")
        .expect("t column")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("type(r) is Utf8");
    assert_eq!(t.value(0), "KNOWS");
}

#[test]
fn untyped_fixed_hop_type_reads_runtime_relationship_type() {
    // #889: an untyped fixed hop has no bind-time type literal, so type(r) must
    // fall back to the row's stored relationship type.
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (:Person {name:'Alice'})-[r]->(:Person {name:'Bob'}) RETURN type(r) AS t",
    );
    assert_eq!(r.stats.rows_produced, 1);
    let t = r.batches[0]
        .column_by_name("t")
        .expect("t column")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("type(r) is Utf8");
    assert_eq!(t.value(0), "KNOWS");
}

#[test]
fn relationship_type_predicate_projects_and_filters() {
    let gf = forge();

    let knows = rows(
        &gf,
        "MATCH (:Person {name:'Alice'})-[r:KNOWS]->(:Person {name:'Bob'}) RETURN r:KNOWS AS ok",
    );
    assert_eq!(knows.stats.rows_produced, 1);
    assert_eq!(bool_cell(&knows, "ok", 0), Some(true));

    let likes = rows(
        &gf,
        "MATCH (:Person {name:'Dave'})-[r:LIKES]->(:Person {name:'Eve'}) RETURN r:KNOWS AS ok",
    );
    assert_eq!(likes.stats.rows_produced, 1);
    assert_eq!(bool_cell(&likes, "ok", 0), Some(false));

    let filtered = rows(&gf, "MATCH ()-[r]->() WHERE r:LIKES RETURN r");
    assert_eq!(filtered.stats.rows_produced, 1);
}

#[test]
fn fixed_hop_relationship_literal_collection_contains_structs() {
    // #889: relationship variables inside projection collections materialize as
    // relationship structs, not scalar edge UUIDs.
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (:Person {name:'Alice'})-[r:KNOWS]->(:Person {name:'Bob'}) RETURN [r] AS rels",
    );
    assert_eq!(r.stats.rows_produced, 1);
    let rels = r.batches[0]
        .column_by_name("rels")
        .expect("rels")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("[r] is a List");
    let items = rels
        .value(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("[r] items are relationship structs")
        .clone();
    assert_eq!(items.len(), 1);
    assert!(items.column_by_name("edge_uuid").is_some());
    let rel_type = items
        .column_by_name("rel_type")
        .expect("rel_type")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("rel_type is Utf8");
    assert_eq!(rel_type.value(0), "KNOWS");
}

#[test]
fn labels_function_reads_node_labels_and_empty_label_sets() {
    // #889: labels(node) returns the node label list directly, and an unlabelled
    // node has an empty label list rather than `[null]`.
    let gf = GraphForge::new(None).expect("in-memory instance");

    let labelled = rows(&gf, "CREATE (node:Person) RETURN labels(node) AS labels");
    let labels = labelled.batches[0]
        .column_by_name("labels")
        .expect("labels")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("labels() is a List");
    assert_eq!(utf8_list_cell(labels, 0), vec!["Person"]);

    let unlabelled = rows(&gf, "CREATE (node) RETURN labels(node) AS labels");
    let labels = unlabelled.batches[0]
        .column_by_name("labels")
        .expect("labels")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("labels() is a List");
    assert!(utf8_list_cell(labels, 0).is_empty());
}

#[test]
fn multi_label_nodes_round_trip_and_match_by_membership() {
    let gf = GraphForge::new(None).expect("in-memory instance");

    let created = rows(
        &gf,
        "CREATE (node:Person:Employee {name:'Alice'}) RETURN labels(node) AS labels",
    );
    let labels = created.batches[0]
        .column_by_name("labels")
        .expect("labels")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("labels() is a List");
    assert_eq!(utf8_list_cell(labels, 0), vec!["Person", "Employee"]);

    for pattern in [":Person", ":Employee", ":Person:Employee"] {
        let result = rows(
            &gf,
            &format!("MATCH (node{pattern}) RETURN labels(node) AS labels"),
        );
        assert_eq!(result.stats.rows_produced, 1, "pattern {pattern}");
        let labels = result.batches[0]
            .column_by_name("labels")
            .expect("labels")
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("labels() is a List");
        assert_eq!(
            utf8_list_cell(labels, 0),
            vec!["Person", "Employee"],
            "pattern {pattern} returns the complete set"
        );
    }

    let missing = rows(&gf, "MATCH (node:Person:Contractor) RETURN node");
    assert_eq!(missing.stats.rows_produced, 0);
}

#[test]
fn labels_function_null_propagates_for_optional_and_literal_null() {
    // #889: openCypher labels(null) and labels(unmatched optional node) are null.
    let gf = GraphForge::new(None).expect("in-memory instance");
    let r = rows(
        &gf,
        "OPTIONAL MATCH (n:DoesNotExist) RETURN labels(n) AS ln, labels(null) AS lnull",
    );
    assert_eq!(r.stats.rows_produced, 1);
    let batch = &r.batches[0];
    assert!(
        batch.column_by_name("ln").expect("ln").is_null(0),
        "labels(unmatched optional node) is null"
    );
    assert!(
        batch.column_by_name("lnull").expect("lnull").is_null(0),
        "labels(null) is null"
    );
}

#[test]
fn node_label_predicate_projects_filters_and_null_propagates() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (:Person {name:'Alice'}), (:Dog {name:'Rex'})")
        .expect("create fixture");

    let person = rows(
        &gf,
        "MATCH (n:Person) RETURN n:Person AS is_person, n:Dog AS is_dog",
    );
    assert_eq!(person.stats.rows_produced, 1);
    assert_eq!(bool_cell(&person, "is_person", 0), Some(true));
    assert_eq!(bool_cell(&person, "is_dog", 0), Some(false));

    let filtered = rows(&gf, "MATCH (n) WHERE n:Dog RETURN n.name AS name");
    assert_eq!(filtered.stats.rows_produced, 1);
    let name = filtered.batches[0]
        .column_by_name("name")
        .expect("name")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name is Utf8");
    assert_eq!(name.value(0), "Rex");

    let missing = rows(
        &gf,
        "OPTIONAL MATCH (n:Missing) RETURN n:Person AS is_person",
    );
    assert_eq!(missing.stats.rows_produced, 1);
    assert_eq!(bool_cell(&missing, "is_person", 0), None);
}

#[test]
fn fixed_pattern_predicates_filter_without_multiplying_rows() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute(
        "CREATE \
         (a:A {name:'A'})-[:REL1]->(b:B {name:'B'}), \
         (b)-[:REL2]->(a), \
         (a)-[:REL3]->(c:C {name:'C'}), \
         (a)-[:REL1]->(d:D {name:'D'})",
    )
    .expect("create pattern predicate fixture");

    let outgoing = rows(
        &gf,
        "MATCH (n) WHERE (n)-[:REL1]->() RETURN n.name AS name ORDER BY name",
    );
    assert_eq!(string_column_values(&outgoing, "name"), vec!["A"]);

    let undirected = rows(
        &gf,
        "MATCH (n) WHERE (n)-[:REL1]-() RETURN n.name AS name ORDER BY name",
    );
    assert_eq!(
        string_column_values(&undirected, "name"),
        vec!["A", "B", "D"]
    );

    let negated = rows(
        &gf,
        "MATCH (n) WHERE NOT (n)-[:REL2]-() RETURN n.name AS name ORDER BY name",
    );
    assert_eq!(string_column_values(&negated, "name"), vec!["C", "D"]);

    let conjunction = rows(
        &gf,
        "MATCH (n) WHERE (n)-[:REL1]-() AND (n)-[:REL3]-() \
         RETURN n.name AS name ORDER BY name",
    );
    assert_eq!(string_column_values(&conjunction, "name"), vec!["A"]);

    let bound_pair = rows(
        &gf,
        "MATCH (n), (m) WHERE (n)-[:REL1]->(m) \
         RETURN n.name AS src, m.name AS dst ORDER BY src, dst",
    );
    assert_eq!(string_column_values(&bound_pair, "src"), vec!["A", "A"]);
    assert_eq!(string_column_values(&bound_pair, "dst"), vec!["B", "D"]);
}

#[test]
fn variable_length_pattern_predicates_filter_without_multiplying_rows() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute(
        "CREATE \
         (a:A {name:'A'})-[:REL1]->(b:B {name:'B'}), \
         (b)-[:REL2]->(a), \
         (a)-[:REL3]->(c:C {name:'C'}), \
         (a)-[:REL1]->(d:D {name:'D'})",
    )
    .expect("create variable-length pattern predicate fixture");

    let outgoing = rows(
        &gf,
        "MATCH (n) WHERE (n)-[:REL1*]->() RETURN n.name AS name ORDER BY name",
    );
    assert_eq!(string_column_values(&outgoing, "name"), vec!["A"]);

    let undirected = rows(
        &gf,
        "MATCH (n) WHERE (n)-[:REL1*]-() RETURN n.name AS name ORDER BY name",
    );
    assert_eq!(
        string_column_values(&undirected, "name"),
        vec!["A", "B", "D"]
    );

    let incoming = rows(
        &gf,
        "MATCH (n) WHERE (n)<-[:REL1*]-() RETURN n.name AS name ORDER BY name",
    );
    assert_eq!(string_column_values(&incoming, "name"), vec!["B", "D"]);

    let exact_two = rows(
        &gf,
        "MATCH (n) WHERE (n)-[:REL1*2]-() RETURN n.name AS name ORDER BY name",
    );
    assert_eq!(string_column_values(&exact_two, "name"), vec!["B", "D"]);

    let bound_pair = rows(
        &gf,
        "MATCH (n), (m) WHERE (n)-[:REL1*]-(m) \
         RETURN n.name AS src, m.name AS dst ORDER BY src, dst",
    );
    assert_eq!(
        string_column_values(&bound_pair, "src"),
        vec!["A", "A", "B", "B", "D", "D"]
    );
    assert_eq!(
        string_column_values(&bound_pair, "dst"),
        vec!["B", "D", "A", "D", "A", "B"]
    );
}

#[test]
fn disjunctive_pattern_predicates_filter_without_multiplying_rows() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute(
        "CREATE \
         (a:A {name:'A'})-[:REL1]->(b:B {name:'B'}), \
         (b)-[:REL2]->(a), \
         (a)-[:REL3]->(c:C {name:'C'}), \
         (a)-[:REL1]->(d:D {name:'D'})",
    )
    .expect("create disjunctive pattern predicate fixture");

    let multi_type = rows(
        &gf,
        "MATCH (n), (m) WHERE (n)-[:REL1|REL2|REL3|REL4]-(m) \
         RETURN n.name AS src, m.name AS dst ORDER BY src, dst",
    );
    assert_eq!(multi_type.stats.rows_produced, 6);
    assert_eq!(
        string_column_values(&multi_type, "src"),
        vec!["A", "A", "A", "B", "C", "D"]
    );
    assert_eq!(
        string_column_values(&multi_type, "dst"),
        vec!["B", "C", "D", "A", "A", "A"]
    );

    let disjunction = rows(
        &gf,
        "MATCH (n) WHERE (n)-[:REL1]-() OR (n)-[:REL2]-() \
         RETURN n.name AS name ORDER BY name",
    );
    assert_eq!(
        string_column_values(&disjunction, "name"),
        vec!["A", "B", "D"]
    );

    let outer_duplicates = rows(
        &gf,
        "MATCH (n)-[:REL1]->() \
         WHERE (n)-[:REL1]-() OR (n)-[:REL2]-() \
         RETURN n.name AS name ORDER BY name",
    );
    assert_eq!(
        string_column_values(&outer_duplicates, "name"),
        vec!["A", "A"]
    );

    let no_match = rows(
        &gf,
        "MATCH (n) WHERE (n)-[:MISSING1]-() OR (n)-[:MISSING2]-() \
         RETURN n.name AS name",
    );
    assert_eq!(no_match.stats.rows_produced, 0);
}

#[test]
fn relationship_keys_return_non_null_relationship_properties() {
    // #889: keys(r) works for fixed-hop relationship variables and excludes
    // null-valued properties, matching the node-key behavior.
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE ()-[:KNOWS {status:'bad', year:'2015', missing:null}]->()")
        .expect("create relationship with properties");

    let r = rows(
        &gf,
        "MATCH ()-[r:KNOWS]->() UNWIND keys(r) AS k RETURN k ORDER BY k",
    );
    assert_eq!(r.stats.rows_produced, 2);
    let mut keys = Vec::new();
    for batch in &r.batches {
        let col = batch
            .column_by_name("k")
            .expect("k")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("keys are Utf8");
        for i in 0..col.len() {
            keys.push(col.value(i).to_owned());
        }
    }
    assert_eq!(keys, vec!["status", "year"]);
}

#[test]
fn relationship_keys_empty_relationship_unwinds_to_zero_rows() {
    // #889: keys(r) on a relationship with no properties is an empty list, so an
    // UNWIND over it emits no rows (including through OPTIONAL MATCH).
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE ()-[:KNOWS]->()")
        .expect("create relationship without properties");

    let direct = rows(&gf, "MATCH ()-[r:KNOWS]->() UNWIND keys(r) AS k RETURN k");
    assert_eq!(direct.stats.rows_produced, 0);

    let optional = rows(
        &gf,
        "OPTIONAL MATCH ()-[r:KNOWS]-() UNWIND keys(r) AS k RETURN k",
    );
    assert_eq!(optional.stats.rows_produced, 0);
}

#[test]
fn relationship_keys_null_propagates_for_optional_and_literal_null() {
    // #889: keys(null) and keys(unmatched optional relationship) are null.
    let gf = GraphForge::new(None).expect("in-memory instance");
    let r = rows(
        &gf,
        "OPTIONAL MATCH ()-[r:DOES_NOT_EXIST]->() RETURN keys(r) AS kr, keys(null) AS knull",
    );
    assert_eq!(r.stats.rows_produced, 1);
    let batch = &r.batches[0];
    assert!(
        batch.column_by_name("kr").expect("kr").is_null(0),
        "keys(unmatched optional relationship) is null"
    );
    assert!(
        batch.column_by_name("knull").expect("knull").is_null(0),
        "keys(null) is null"
    );
}

// ---------------------------------------------------------------------------
// Relationship-list access (#743): indexing, type(), size()
// ---------------------------------------------------------------------------

#[test]
fn rel_list_type_of_first_element() {
    // type(r[0]) reads the rel_type of the first relationship on each path.
    // Every edge in the fixture is KNOWS.
    use arrow::array::StringArray;

    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (a:Person)-[r:KNOWS*1..2]->(b:Person) RETURN type(r[0]) AS t",
    );
    assert!(r.stats.rows_produced >= 1, "at least one path");
    for batch in &r.batches {
        let col = batch
            .column_by_name("t")
            .expect("t column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("type() yields Utf8");
        for i in 0..col.len() {
            assert_eq!(col.value(i), "KNOWS", "every edge is KNOWS");
        }
    }
}

#[test]
fn rel_list_index_returns_struct_element() {
    // r[0] is the first relationship struct; its edge_uuid is FixedSizeBinary(16).
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (a:Person)-[r:KNOWS*1..2]->(b:Person) RETURN r[0] AS first",
    );
    assert!(r.stats.rows_produced >= 1);
    let field = r.schema.field_with_name("first").expect("first column");
    let DataType::Struct(struct_fields) = field.data_type() else {
        panic!("r[0] must be a Struct, got {:?}", field.data_type());
    };
    let edge_uuid = struct_fields
        .iter()
        .find(|f| f.name() == "edge_uuid")
        .expect("edge_uuid struct field");
    assert_eq!(edge_uuid.data_type(), &DataType::FixedSizeBinary(16));
}

#[test]
fn rel_list_size_equals_length() {
    // openCypher `size(list)` == element count == `length(r)` (hop count) here.
    use arrow::array::{Int64Array, UInt64Array};

    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (a:Person)-[r:KNOWS*1..2]->(b:Person) RETURN size(r) AS s, length(r) AS l",
    );
    assert!(r.stats.rows_produced >= 1);
    for batch in &r.batches {
        let s = batch
            .column_by_name("s")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("size() yields Int64");
        let l = batch
            .column_by_name("l")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("length() yields UInt64");
        for i in 0..s.len() {
            assert_eq!(
                s.value(i),
                l.value(i) as i64,
                "size(r) must equal length(r)"
            );
        }
    }
}

#[test]
fn rel_list_element_property_resolves() {
    // #755: edge PROPERTIES on a variable-length relationship-list element.
    // `r[0].since` reads the `since` property of the first edge on each path.
    use arrow::array::Int64Array;

    let gf = GraphForge::new(None).expect("in-memory instance");
    // A 1-hop and a 2-hop reach from Alice, each first edge carrying since=2020.
    gf.execute(
        "CREATE (a:Person {name:'Alice'})-[:KNOWS {since:2020}]->(b:Person {name:'Bob'}), \
         (b)-[:KNOWS {since:2021}]->(c:Person {name:'Carol'})",
    )
    .expect("create a KNOWS chain with `since` on each edge");

    let r = rows(
        &gf,
        "MATCH (a:Person {name:'Alice'})-[r:KNOWS*1..2]->(b:Person) RETURN r[0].since AS since",
    );
    assert!(r.stats.rows_produced >= 1, "at least one path from Alice");
    for batch in &r.batches {
        let since = batch
            .column_by_name("since")
            .expect("since column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("r[0].since is Int64");
        for i in 0..since.len() {
            // Every path starts with the Alice→Bob edge (since=2020).
            assert_eq!(since.value(i), 2020, "r[0] is always the since=2020 edge");
        }
    }
}

#[test]
fn rel_list_element_property_is_null_when_absent() {
    // An edge with no `since` property yields NULL for its r[i].since (LEFT-join
    // semantics), not an error. Use a genuine variable-length pattern (*1..2 over
    // a 2-hop chain) whose SECOND edge lacks `since`, then read r[1].since.
    use arrow::array::Int64Array;

    let gf = GraphForge::new(None).expect("in-memory instance");
    // Alice→Bob carries since=2020; Bob→Carol has no `since`.
    gf.execute(
        "CREATE (a:Person {name:'Alice'})-[:KNOWS {since:2020}]->(b:Person {name:'Bob'}), \
         (b)-[:KNOWS]->(c:Person {name:'Carol'})",
    )
    .expect("create a 2-hop KNOWS chain; only the first edge has `since`");

    // The 2-hop path Alice→Bob→Carol: r[0].since = 2020, r[1].since = NULL.
    let r = rows(
        &gf,
        "MATCH (a:Person {name:'Alice'})-[r:KNOWS*2..2]->(c:Person) \
         RETURN r[0].since AS first, r[1].since AS second",
    );
    assert_eq!(r.stats.rows_produced, 1, "one 2-hop path");
    let col = |b: &arrow::record_batch::RecordBatch, name: &str| -> Option<i64> {
        let a = b
            .column_by_name(name)
            .expect("column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("since is Int64");
        if a.is_null(0) { None } else { Some(a.value(0)) }
    };
    let b = &r.batches[0];
    assert_eq!(col(b, "first"), Some(2020), "r[0] is the Alice→Bob edge");
    assert_eq!(
        col(b, "second"),
        None,
        "r[1] (Bob→Carol) has no `since` → NULL"
    );
}

// ---------------------------------------------------------------------------
// Named path variables (#754): nodes(p), relationships(p), length(p)
// ---------------------------------------------------------------------------

/// The fixture uuid of the `Person` named `name`.
fn person_uuid(gf: &GraphForge, name: &str) -> [u8; 16] {
    let r = rows(
        gf,
        &format!("MATCH (n:Person {{name:'{name}'}}) RETURN n.node_uuid AS node_uuid"),
    );
    let col = r.batches[0]
        .column_by_name("node_uuid")
        .expect("node_uuid")
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("uuid column");
    col.value(0).try_into().expect("16-byte uuid")
}

/// Row `row` of the `List<Struct{node_uuid}>` column `col_name` as uuids, or
/// `None` for a null path.
fn node_uuid_seq(
    batch: &arrow::record_batch::RecordBatch,
    col_name: &str,
    row: usize,
) -> Option<Vec<[u8; 16]>> {
    use arrow::array::{ListArray, StructArray};
    let list = batch
        .column_by_name(col_name)
        .expect("column")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("List column");
    if list.is_null(row) {
        return None;
    }
    let items = list.value(row);
    let items = items.as_any().downcast_ref::<StructArray>().expect("items");
    let uuids = items
        .column_by_name("node_uuid")
        .expect("node_uuid field")
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("uuid field")
        .clone();
    Some(
        (0..uuids.len())
            .map(|i| uuids.value(i).try_into().expect("16-byte uuid"))
            .collect(),
    )
}

#[test]
fn path_functions_agree_on_every_path() {
    // nodes(p) has length(p) + 1 entries, starts at the bound start node, and
    // relationships(p) has the same shape as `RETURN r` (#709).
    use arrow::array::UInt64Array;

    let gf = forge();
    let alice = person_uuid(&gf, "Alice");
    let r = rows(
        &gf,
        "MATCH p = (a:Person {name:'Alice'})-[:KNOWS*1..2]->(b:Person) \
         RETURN nodes(p) AS ns, relationships(p) AS rs, length(p) AS l",
    );
    // Alice→Bob, Alice→Carol (1 hop); Alice→Bob→Carol, Alice→Carol→Dave (2 hops).
    assert_eq!(r.stats.rows_produced, 4, "paths from Alice at 1..2 hops");

    // rs keeps the #709 List<Struct{edge_uuid: FixedSizeBinary(16), …}> shape.
    let rs_field = r.schema.field_with_name("rs").expect("rs column");
    let DataType::List(item) = rs_field.data_type() else {
        panic!("rs must be a List, got {:?}", rs_field.data_type());
    };
    let DataType::Struct(fields) = item.data_type() else {
        panic!("rs item must be a Struct, got {:?}", item.data_type());
    };
    assert!(fields.iter().any(|f| f.name() == "edge_uuid"));

    for batch in &r.batches {
        let hops = batch
            .column_by_name("l")
            .expect("l")
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("length(p) is the array_length UInt64");
        for row in 0..batch.num_rows() {
            let ns = node_uuid_seq(batch, "ns", row).expect("matched path is non-null");
            let l = usize::try_from(hops.value(row)).unwrap();
            assert_eq!(ns.len(), l + 1, "nodes(p) is one longer than length(p)");
            assert_eq!(ns[0], alice, "the walk starts at the bound start node");
        }
    }
}

#[test]
fn path_nodes_respect_traversal_direction() {
    // `(carol)<-[:KNOWS*1..2]-(x)` traverses *against* storage orientation
    // (edges are stored Alice→Carol, Bob→Carol, Alice→Bob), so every walk
    // starts at Carol and the 2-hop walk is [Carol, Bob, Alice] — the
    // traversal order, never the stored src/dst order (which would put Carol
    // second).
    use arrow::array::UInt64Array;

    let gf = forge();
    let alice = person_uuid(&gf, "Alice");
    let bob = person_uuid(&gf, "Bob");
    let carol = person_uuid(&gf, "Carol");
    let r = rows(
        &gf,
        "MATCH p = (c:Person {name:'Carol'})<-[:KNOWS*1..2]-(x:Person) \
         RETURN nodes(p) AS ns, length(p) AS l",
    );
    // Carol←Alice, Carol←Bob (1 hop); Carol←Bob←Alice (2 hops).
    assert_eq!(r.stats.rows_produced, 3, "incoming paths to Carol");

    let mut one_hop_tails = Vec::new();
    for batch in &r.batches {
        let hops = batch
            .column_by_name("l")
            .expect("l")
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("UInt64 hops");
        for row in 0..batch.num_rows() {
            let ns = node_uuid_seq(batch, "ns", row).expect("non-null path");
            assert_eq!(ns[0], carol, "every walk starts at the bound node");
            match hops.value(row) {
                1 => one_hop_tails.push(ns[1]),
                2 => assert_eq!(ns, vec![carol, bob, alice], "2-hop walk order"),
                other => panic!("unexpected hop count {other}"),
            }
        }
    }
    one_hop_tails.sort_unstable();
    let mut expected = vec![alice, bob];
    expected.sort_unstable();
    assert_eq!(one_hop_tails, expected, "1-hop neighbours of Carol");
}

#[test]
fn path_zero_hop_is_single_node() {
    // A `*0..1` match includes the 0-hop self-path: length(p) = 0 and
    // nodes(p) = [start] (the empty relationship list contributes no hops).
    use arrow::array::UInt64Array;

    let gf = forge();
    let eve = person_uuid(&gf, "Eve");
    // Eve has no outgoing KNOWS edges, so only her 0-hop path matches.
    let r = rows(
        &gf,
        "MATCH p = (a:Person {name:'Eve'})-[:KNOWS*0..1]->(b) \
         RETURN nodes(p) AS ns, length(p) AS l",
    );
    assert_eq!(r.stats.rows_produced, 1, "just the 0-hop self path");
    let batch = &r.batches[0];
    let hops = batch
        .column_by_name("l")
        .expect("l")
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("UInt64 hops");
    assert_eq!(hops.value(0), 0);
    assert_eq!(node_uuid_seq(batch, "ns", 0), Some(vec![eve]));
}

#[test]
fn bare_path_var_returns_struct_of_nodes_and_relationships() {
    // `RETURN p` yields one Arrow Struct{nodes, relationships} per path —
    // the same values nodes(p)/relationships(p) produce, in one column.
    use arrow::array::{ListArray, StructArray};

    let gf = forge();
    let alice = person_uuid(&gf, "Alice");
    let r = rows(
        &gf,
        "MATCH p = (a:Person {name:'Alice'})-[:KNOWS*1..2]->(b:Person) RETURN p",
    );
    assert_eq!(r.stats.rows_produced, 4, "paths from Alice at 1..2 hops");

    // Exactly one output column: the path struct with the two list fields.
    assert_eq!(r.schema.fields().len(), 1, "RETURN p is one column");
    let field = r.schema.field(0);
    let DataType::Struct(fields) = field.data_type() else {
        panic!("p must be a Struct, got {:?}", field.data_type());
    };
    let names: Vec<&str> = fields.iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, ["nodes", "relationships"]);

    for batch in &r.batches {
        let paths = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("struct column");
        let nodes = paths
            .column_by_name("nodes")
            .expect("nodes field")
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("nodes is a List")
            .clone();
        let rels = paths
            .column_by_name("relationships")
            .expect("relationships field")
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("relationships is a List")
            .clone();
        for row in 0..paths.len() {
            let node_items = nodes.value(row);
            let node_items = node_items
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("node items");
            let uuids = node_items
                .column_by_name("node_uuid")
                .expect("node_uuid")
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .expect("uuid field")
                .clone();
            assert_eq!(
                node_items.len(),
                rels.value(row).len() + 1,
                "nodes is one longer than relationships"
            );
            let first: [u8; 16] = uuids.value(0).try_into().unwrap();
            assert_eq!(first, alice, "every path starts at Alice");
        }
    }
}

#[test]
fn fixed_hop_path_functions_compose_from_scalar_columns() {
    // A fixed single hop has no relationship-list column — nodes(p) /
    // relationships(p) / length(p) compose from the join's scalar columns.
    use arrow::array::{ListArray, StringArray, StructArray, UInt64Array};

    let gf = forge();
    let alice = person_uuid(&gf, "Alice");
    let bob = person_uuid(&gf, "Bob");
    let r = rows(
        &gf,
        "MATCH p = (a:Person {name:'Alice'})-[:KNOWS]->(b:Person {name:'Bob'}) \
         RETURN nodes(p) AS ns, relationships(p) AS rs, length(p) AS l",
    );
    assert_eq!(r.stats.rows_produced, 1, "exactly the Alice→Bob edge");
    let batch = &r.batches[0];

    // length(p) is 1, UInt64 like the var-length form.
    let hops = batch
        .column_by_name("l")
        .expect("l")
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("fixed length(p) is UInt64 too");
    assert_eq!(hops.value(0), 1);

    // nodes(p) is [alice, bob] in traversal order.
    assert_eq!(node_uuid_seq(batch, "ns", 0), Some(vec![alice, bob]));

    // relationships(p) is a one-element list whose struct carries the
    // topology fields with rel_type = 'KNOWS'.
    let rs = batch
        .column_by_name("rs")
        .expect("rs")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("rs is a List");
    let items = rs.value(0);
    let items = items.as_any().downcast_ref::<StructArray>().expect("items");
    assert_eq!(items.len(), 1, "one relationship on a single hop");
    for topology in ["edge_uuid", "src_uuid", "dst_uuid"] {
        assert!(
            items.column_by_name(topology).is_some(),
            "missing {topology} field"
        );
    }
    let rel_type = items
        .column_by_name("rel_type")
        .expect("rel_type")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("rel_type is Utf8");
    assert_eq!(rel_type.value(0), "KNOWS");
}

#[test]
fn fixed_hop_nodes_preserve_endpoint_labels_and_properties() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (:Person {name:'Alice'})-[:WORKS_AT]->(:Company {title:'Acme'})")
        .expect("create path fixture");

    let r = rows(
        &gf,
        "MATCH p = (:Person)-[:WORKS_AT]->(:Company) RETURN nodes(p) AS ns",
    );
    assert_eq!(r.stats.rows_produced, 1);
    let nodes = r.batches[0]
        .column_by_name("ns")
        .expect("ns")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("nodes(p) is a List")
        .value(0);
    let nodes = nodes
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("nodes(p) elements are node Structs");
    assert_eq!(nodes.len(), 2);

    let labels = nodes
        .column_by_name("labels")
        .expect("labels")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("labels is a List");
    assert_eq!(utf8_list_cell(labels, 0), vec!["Person"]);
    assert_eq!(utf8_list_cell(labels, 1), vec!["Company"]);

    let names = nodes
        .column_by_name("name")
        .expect("unioned name property")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name is Utf8");
    assert_eq!(names.value(0), "Alice");
    assert!(names.is_null(1));

    let titles = nodes
        .column_by_name("title")
        .expect("unioned title property")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("title is Utf8");
    assert!(titles.is_null(0));
    assert_eq!(titles.value(1), "Acme");
}

#[test]
fn fixed_hop_return_p_carries_relationship_properties() {
    // #889: fixed-hop `RETURN p` uses the same relationship value shape as
    // fixed-hop relationships(p), including relationship properties.
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (a:Person {name:'Alice'})-[:KNOWS {since:2020}]->(b:Person {name:'Bob'})")
        .expect("create path fixture");

    let r = rows(
        &gf,
        "MATCH p = (:Person {name:'Alice'})-[:KNOWS]->(:Person {name:'Bob'}) RETURN p",
    );
    assert_eq!(r.stats.rows_produced, 1);
    let path = r.batches[0]
        .column_by_name("p")
        .expect("p")
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("p is a path Struct");
    let rels = path
        .column_by_name("relationships")
        .expect("relationships")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("relationships is a List");
    let items = rels
        .value(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("relationship items")
        .clone();
    let since = items
        .column_by_name("since")
        .expect("since")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("since is Int64");
    assert_eq!(since.value(0), 2020);
}

#[test]
fn unmatched_optional_fixed_hop_path_is_null() {
    // An unmatched OPTIONAL MATCH row's path is Cypher null — every path
    // function (and bare p) must be NULL, not a composed value over null
    // columns (length 1, a list of null-field structs, a non-null struct).
    use arrow::array::UInt64Array;

    let gf = forge();
    // Eve has no outgoing KNOWS edge → one row, unmatched optional.
    let r = rows(
        &gf,
        "MATCH (a:Person {name:'Eve'}) \
         OPTIONAL MATCH p = (a)-[:KNOWS]->(b:Person) \
         RETURN p, nodes(p) AS ns, relationships(p) AS rs, length(p) AS l",
    );
    assert_eq!(r.stats.rows_produced, 1, "Eve row survives the optional");
    let batch = &r.batches[0];
    assert!(
        batch.column_by_name("p").expect("p").is_null(0),
        "unmatched path value must be NULL"
    );
    assert_eq!(node_uuid_seq(batch, "ns", 0), None, "nodes(p) must be NULL");
    assert!(
        batch.column_by_name("rs").expect("rs").is_null(0),
        "relationships(p) must be NULL"
    );
    let hops = batch
        .column_by_name("l")
        .expect("l")
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("UInt64 hops");
    assert!(hops.is_null(0), "length(p) must be NULL, not 1");
}

#[test]
fn unmatched_optional_var_len_path_is_null() {
    // The var-length forms null-propagate through the list column; bare p
    // must be NULL too (not a non-null struct with null fields).
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (a:Person {name:'Eve'}) \
         OPTIONAL MATCH p = (a)-[:KNOWS*1..2]->(b:Person) \
         RETURN p, nodes(p) AS ns, length(p) AS l",
    );
    assert_eq!(r.stats.rows_produced, 1);
    let batch = &r.batches[0];
    assert!(batch.column_by_name("p").expect("p").is_null(0));
    assert_eq!(node_uuid_seq(batch, "ns", 0), None);
    assert!(batch.column_by_name("l").expect("l").is_null(0));
}

#[test]
fn fixed_hop_path_nodes_respect_left_arrow_direction() {
    // `(b)<-[:KNOWS]-(a)` puts Bob first in the pattern, so nodes(p) is
    // [bob, alice] — the binder's traversal order, not storage order.
    let gf = forge();
    let alice = person_uuid(&gf, "Alice");
    let bob = person_uuid(&gf, "Bob");
    let r = rows(
        &gf,
        "MATCH p = (b:Person {name:'Bob'})<-[:KNOWS]-(a:Person {name:'Alice'}) \
         RETURN nodes(p) AS ns",
    );
    assert_eq!(r.stats.rows_produced, 1);
    assert_eq!(
        node_uuid_seq(&r.batches[0], "ns", 0),
        Some(vec![bob, alice])
    );
}

#[test]
fn list_indexing_matches_opencypher_semantics() {
    // openCypher list indexing over a literal (controllable values): 0-based,
    // negative-from-end, null on out-of-range.
    use arrow::array::{Array, Int64Array};

    let gf = forge();
    let cases = [
        ("RETURN [10,20,30,40,50][0] AS x", Some(10)),
        ("RETURN [10,20,30,40,50][2] AS x", Some(30)),
        ("RETURN [10,20,30,40,50][-1] AS x", Some(50)),
        ("RETURN [10,20,30,40,50][-2] AS x", Some(40)),
        ("RETURN [10,20,30][10] AS x", None), // out of range → null
        ("RETURN [10,20,30][-10] AS x", None),
    ];
    for (q, expect) in cases {
        let r = rows(&gf, q);
        let col = r.batches[0]
            .column_by_name("x")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64");
        match expect {
            Some(v) => assert_eq!(col.value(0), v, "{q}"),
            None => assert!(col.is_null(0), "{q} should be null (out of range)"),
        }
    }
}

#[test]
fn list_slicing_matches_opencypher_semantics() {
    // openCypher slicing: 0-based, start-inclusive, end-EXCLUSIVE, null-unbounded
    // bounds, negative-from-end. The translation to DataFusion's 1-based
    // inclusive `array_slice` is the bug-prone part — assert exact values.
    use arrow::array::{Int64Array, ListArray};

    let gf = forge();
    let cases: [(&str, Vec<i64>); 6] = [
        ("RETURN [10,20,30,40,50][1..3] AS s", vec![20, 30]),
        ("RETURN [10,20,30,40,50][..2] AS s", vec![10, 20]),
        ("RETURN [10,20,30,40,50][3..] AS s", vec![40, 50]),
        ("RETURN [10,20,30,40,50][-2..] AS s", vec![40, 50]),
        ("RETURN [10,20,30,40,50][1..] AS s", vec![20, 30, 40, 50]),
        ("RETURN [10,20,30][1..1] AS s", vec![]), // start==end → empty
    ];
    for (q, expect) in cases {
        let r = rows(&gf, q);
        let list = r.batches[0]
            .column_by_name("s")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("List");
        let inner = list.value(0);
        let ints = inner.as_any().downcast_ref::<Int64Array>().expect("Int64");
        let got: Vec<i64> = (0..ints.len()).map(|i| ints.value(i)).collect();
        assert_eq!(got, expect, "{q}");
    }
}

#[test]
fn size_of_string_is_char_count_not_array_length() {
    // size() is polymorphic: on a string it returns the character count (proving
    // the cypher_size UDF dispatches on type, not a blind array_length).
    use arrow::array::Int64Array;

    let gf = forge();
    let r = rows(&gf, "RETURN size('hello') AS n");
    let n = r.batches[0]
        .column_by_name("n")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("size() yields Int64")
        .value(0);
    assert_eq!(n, 5, "size('hello') is the 5-char count");
}

// ---------------------------------------------------------------------------
// Read — optional match
// ---------------------------------------------------------------------------

#[test]
fn optional_match_likes() {
    let gf = forge();
    // Every Person is kept; m.node_uuid is null for the 4 without a LIKES edge.
    let r = rows(
        &gf,
        "MATCH (n:Person) OPTIONAL MATCH (n)-[:LIKES]->(m) RETURN n.node_uuid, m.node_uuid",
    );
    assert_eq!(r.stats.rows_produced, 5, "one row per Person");
}

// ---------------------------------------------------------------------------
// Read — UNWIND
// ---------------------------------------------------------------------------

#[test]
fn unwind_list_literal() {
    let gf = forge();
    let r = rows(&gf, "UNWIND [1, 2, 3] AS x RETURN x");
    assert_eq!(r.stats.rows_produced, 3);
}

#[test]
fn wildcard_relationship_type_names_are_available() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (x:X), (x)-[:T]->(), (x)-[:T]->(), (x)-[:T]->(), (x)-[:OTHER]->()")
        .expect("fixture create");

    let result = rows(&gf, "MATCH (x:X)-[r]->() RETURN type(r) AS rel_type");
    let values = result.batches[0]
        .column_by_name("rel_type")
        .expect("rel_type column")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("type() yields Utf8");
    let mut names = values.iter().flatten().collect::<Vec<_>>();
    names.sort_unstable();
    assert_eq!(names, ["OTHER", "T", "T", "T"]);
}

#[test]
fn multi_type_pattern_comprehension_includes_every_type() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (x:X), (x)-[:T]->(), (x)-[:T]->(), (x)-[:T]->(), (x)-[:OTHER]->()")
        .expect("fixture create");

    let filtered = rows(
        &gf,
        "MATCH (a:X)-[r]->() WHERE type(r) = 'T' OR type(r) = 'OTHER' RETURN r",
    );
    assert_eq!(filtered.stats.rows_produced, 4);

    let result = rows(
        &gf,
        "MATCH (a:X) RETURN size([(a)-[:T|OTHER]->() | 1]) AS length",
    );
    let length = result.batches[0]
        .column_by_name("length")
        .expect("length column")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("size() yields Int64");
    assert_eq!(length.value(0), 4);
}

#[test]
fn unwound_collected_node_remains_matchable() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (s:S), (n), (e:E), (s)-[:X]->(e), (s)-[:Y]->(e), (n)-[:Y]->(e)")
        .expect("fixture create");
    let query = "MATCH (a:S)-[:X]->(b1) \
                 WITH a, collect(b1) AS bees \
                 UNWIND bees AS b2 \
                 MATCH (a)-[:Y]->(b2) \
                 RETURN a, b2";
    let result = rows(&gf, query);
    assert_eq!(result.stats.rows_produced, 1);
}

// ---------------------------------------------------------------------------
// Read — ORDER BY + LIMIT
// ---------------------------------------------------------------------------

#[test]
fn order_by_age_limit() {
    let gf = forge();
    let r = rows(
        &gf,
        "MATCH (n:Person) RETURN n.node_uuid ORDER BY n.age DESC LIMIT 3",
    );
    assert_eq!(r.stats.rows_produced, 3);
}

#[test]
fn order_by_zoned_temporals_uses_absolute_instants() {
    let gf = forge();
    for (query, expected) in [
        (
            "UNWIND [time('10:35-08:00'), time('12:35:15+05:00')] AS value \
             WITH value ORDER BY value LIMIT 1 \
             RETURN toString(value) AS rendered",
            "12:35:15+05:00",
        ),
        (
            "UNWIND [datetime('1984-10-11T12:30:14+00:15'), \
                     datetime('1984-10-11T12:31:14+00:17')] AS value \
             WITH value ORDER BY value LIMIT 1 \
             RETURN toString(value) AS rendered",
            "1984-10-11T12:31:14+00:17",
        ),
    ] {
        let r = rows(&gf, query);
        let rendered = r.batches[0]
            .column_by_name("rendered")
            .expect("rendered")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Utf8 rendered")
            .value(0);
        assert_eq!(rendered, expected);
    }
}

#[test]
fn collect_preserves_explicit_input_order() {
    let gf = forge();
    let r = rows(
        &gf,
        "WITH [true, false] AS values \
         WITH values, size(values) AS numOfValues \
         UNWIND values AS value \
         WITH size([x IN values WHERE x < value]) AS x, value, numOfValues \
           ORDER BY value \
         WITH numOfValues, collect(x) AS orderedX \
         RETURN orderedX",
    );
    let list = r.batches[0]
        .column_by_name("orderedX")
        .expect("orderedX")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("List orderedX");
    let values = list
        .value(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 orderedX")
        .iter()
        .collect::<Vec<_>>();
    assert_eq!(values, vec![Some(0), Some(1)]);
}

// ---------------------------------------------------------------------------
// Arrow result contract
// ---------------------------------------------------------------------------

#[test]
fn result_schema_carries_query_metadata() {
    let gf = forge();
    let r = rows(&gf, "MATCH (n:Person) RETURN n.node_uuid AS node_uuid");
    let meta = r.schema.metadata();
    assert!(meta.contains_key("graphforge.query_id"), "query_id present");
    assert!(
        meta.contains_key("graphforge.ir_version"),
        "ir_version present"
    );
    assert_eq!(
        meta.get("graphforge.ontology_mode").map(String::as_str),
        Some("exploratory"),
    );
    // Exploratory mode has no ontology → no ontology_version.
    assert!(
        !meta.contains_key("graphforge.ontology_version"),
        "ontology_version omitted in exploratory mode"
    );
}

#[test]
fn surrogate_id_columns_never_surface() {
    let gf = forge();
    let r = rows(&gf, "MATCH (n:Person) RETURN n.node_uuid AS node_uuid");
    assert!(
        r.schema.column_with_name("node_id").is_none(),
        "node_id surrogate must not appear"
    );
    assert!(
        r.schema.column_with_name("edge_id").is_none(),
        "edge_id surrogate must not appear"
    );
}

#[test]
fn node_uuid_is_fixed_size_binary_16() {
    let gf = forge();
    let r = rows(&gf, "MATCH (n:Person) RETURN n.node_uuid AS node_uuid");
    let col = r.batches[0]
        .column_by_name("node_uuid")
        .expect("node_uuid column");
    let arr = col
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("node_uuid is FixedSizeBinary");
    assert_eq!(arr.value_length(), 16);
}

#[test]
fn query_id_is_unique_across_calls() {
    let gf = forge();
    let a = rows(&gf, "MATCH (n:Person) RETURN n.node_uuid AS node_uuid");
    let b = rows(&gf, "MATCH (n:Person) RETURN n.node_uuid AS node_uuid");
    assert_ne!(
        a.schema.metadata().get("graphforge.query_id"),
        b.schema.metadata().get("graphforge.query_id"),
        "each execute() stamps a fresh query_id"
    );
}

// ---------------------------------------------------------------------------
// Write — incremental CREATE accumulates (#733)
// ---------------------------------------------------------------------------

#[test]
fn incremental_create_accumulates() {
    use arrow::array::StringArray;

    let gf = GraphForge::new(None).expect("in-memory instance");
    // Two Persons, then a THIRD in a separate execute() — the second CREATE
    // must append, not replace the first (#733).
    gf.execute("CREATE (:Person {name:'A'}), (:Person {name:'B'})")
        .expect("create A,B");
    gf.execute("CREATE (:Person {name:'C'})").expect("create C");

    let total = rows(&gf, "MATCH (n:Person) RETURN count(n) AS total");
    let n = total.batches[0]
        .column_by_name("total")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n, 3, "all three Persons across two CREATEs");

    // All three names are readable.
    let names_result = rows(&gf, "MATCH (n:Person) RETURN n.name AS name");
    assert_eq!(names_result.stats.rows_produced, 3);
    let col = names_result.batches[0]
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let mut names: Vec<&str> = (0..col.len()).map(|i| col.value(i)).collect();
    names.sort_unstable();
    assert_eq!(names, vec!["A", "B", "C"]);
}

#[test]
fn create_anonymous_path_binds_edge_endpoints() {
    // #598/#601: `CREATE (:A)-[:REL]->(:B)` — a path whose nodes are anonymous —
    // must bind the edge's dst to the created `(:B)` node, not a fresh unbound
    // var. Before the fix this errored "CREATE edge references unbound dst var".
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (:A)-[:REL]->(:B)")
        .expect("create anonymous path");
    let r = rows(&gf, "MATCH (:A)-[:REL]->(:B) RETURN count(*) AS c");
    let c = r.batches[0]
        .column_by_name("c")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(c, 1, "one (:A)-[:REL]->(:B) path created");
}

// ---------------------------------------------------------------------------
// Write — DELETE / DETACH DELETE (#740)
// ---------------------------------------------------------------------------

/// Count `MATCH (n:Person) RETURN count(n)`.
fn person_count(gf: &GraphForge) -> i64 {
    let r = rows(gf, "MATCH (n:Person) RETURN count(n) AS total");
    r.batches[0]
        .column_by_name("total")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

#[test]
fn delete_isolated_node_removes_it() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (:Person {name:'Alice'}), (:Person {name:'Bob'})")
        .expect("create two isolated Persons");
    assert_eq!(person_count(&gf), 2);

    // Alice has no relationships → a plain DELETE succeeds.
    gf.execute("MATCH (p:Person {name:'Alice'}) DELETE p")
        .expect("delete isolated Alice");
    assert_eq!(person_count(&gf), 1, "Alice removed, Bob remains");

    let remaining = rows(&gf, "MATCH (n:Person) RETURN n.name AS name");
    let name = remaining.batches[0]
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap()
        .value(0);
    assert_eq!(name, "Bob");
}

#[test]
fn detach_delete_named_path_removes_every_entity() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (:A)-[:R]->(:B)-[:R]->(:C)")
        .expect("create path");
    let result = gf
        .execute("MATCH p = (:A)-->()-->() DETACH DELETE p")
        .expect("delete named path");
    let effects = result.side_effects.expect("side effects");

    assert_eq!(effects.nodes_deleted, 3);
    assert_eq!(effects.relationships_deleted, 2);
    assert_eq!(rows(&gf, "MATCH (n) RETURN n").stats.rows_produced, 0);
}

#[test]
fn delete_extracts_node_from_nested_map_and_list() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (a:User), (b:User), (a)-[:R]->(b), (b)-[:R]->(a)")
        .expect("create users");
    let result = gf
        .execute(
            "MATCH (u:User) WITH {key: collect(u)} AS nodeMap \
             DETACH DELETE nodeMap.key[0]",
        )
        .expect("delete nested node value");
    let effects = result.side_effects.expect("side effects");

    assert_eq!(effects.nodes_deleted, 1);
    assert_eq!(effects.relationships_deleted, 2);
    assert_eq!(rows(&gf, "MATCH (n:User) RETURN n").stats.rows_produced, 1);
}

#[test]
fn delete_connected_node_without_detach_errors() {
    // openCypher: deleting a node that still has relationships without DETACH
    // is an ExecutionError.
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (a:Person {name:'Alice'})-[:KNOWS]->(b:Person {name:'Bob'})")
        .expect("create a connected pair");

    let err = gf
        .execute("MATCH (p:Person {name:'Alice'}) DELETE p")
        .expect_err("deleting a connected node without DETACH must error");
    let msg = err.to_string();
    assert!(
        msg.contains("still has relationships"),
        "expected the no-DETACH relationship error, got: {msg}"
    );
    // Nothing was deleted.
    assert_eq!(person_count(&gf), 2, "the failed DELETE left both nodes");
}

#[test]
fn detach_delete_removes_node_and_incident_edges() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (a:Person {name:'Alice'})-[:KNOWS]->(b:Person {name:'Bob'})")
        .expect("create a connected pair");
    assert_eq!(person_count(&gf), 2);

    // DETACH DELETE removes Alice and her KNOWS edge.
    gf.execute("MATCH (p:Person {name:'Alice'}) DETACH DELETE p")
        .expect("DETACH DELETE the connected node");
    assert_eq!(person_count(&gf), 1, "Alice removed");

    // The KNOWS edge is gone: Bob has no incoming KNOWS now.
    let edges = rows(
        &gf,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.node_uuid",
    );
    assert_eq!(edges.stats.rows_produced, 0, "the incident edge is removed");
}

#[test]
fn plain_delete_of_node_and_its_relationship_in_one_statement_succeeds() {
    // openCypher: a non-DETACH DELETE may remove a node together with all its
    // relationships in the same statement — `DELETE r, a` is legal when r is a's
    // only edge, because no relationship survives. (Previously this wrongly
    // errored: the incident-edge check ignored edges deleted in the same query.)
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (a:Person {name:'Alice'})-[:KNOWS]->(b:Person {name:'Bob'})")
        .expect("create a connected pair");

    gf.execute("MATCH (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person) DELETE r, a")
        .expect("deleting a node with its only relationship in one statement is legal");

    assert_eq!(person_count(&gf), 1, "Alice removed, Bob remains");
    let edges = rows(
        &gf,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.node_uuid",
    );
    assert_eq!(edges.stats.rows_produced, 0, "the relationship is gone");
}

#[test]
fn delete_node_still_errors_when_an_untargeted_relationship_survives() {
    // The same-statement exemption is narrow: if a node keeps a relationship that
    // is NOT being deleted, a non-DETACH DELETE must still error. Alice has a
    // KNOWS edge and a LIKES edge; deleting only the KNOWS edge plus Alice leaves
    // the LIKES edge. (Two distinct rel types so the deleted edge is selected by
    // type — without relying on destination-node property filtering, which a
    // fixed single-hop expand does not yet apply.)
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute(
        "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), (c:Person {name:'Carol'}), \
         (a)-[:KNOWS]->(b), (a)-[:LIKES]->(c)",
    )
    .expect("create Alice with a KNOWS and a LIKES edge");

    // Delete Alice + only her KNOWS edge; the LIKES edge survives → error.
    let err = gf
        .execute("MATCH (a:Person {name:'Alice'})-[r:KNOWS]->(b) DELETE r, a")
        .expect_err("a surviving untargeted relationship must still block the node delete");
    assert!(
        err.to_string().contains("still has relationships"),
        "expected the no-DETACH error, got: {err}"
    );
    // Nothing deleted (the failed DELETE is all-or-nothing before the rewrite).
    assert_eq!(person_count(&gf), 3);
}

#[test]
fn optional_match_delete_of_unmatched_row_is_noop() {
    // openCypher: DELETE of a NULL is a no-op. An OPTIONAL MATCH that finds no
    // edge yields a null edge identity — DELETE r must succeed and delete
    // nothing, not error on the null. (The Person row still matches.)
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (:Person {name:'Alice'})")
        .expect("create a lone Alice with no relationships");

    gf.execute("MATCH (p:Person {name:'Alice'}) OPTIONAL MATCH (p)-[r:KNOWS]->(x) DELETE r")
        .expect("DELETE of a null (unmatched) relationship is a no-op");

    // Alice is untouched.
    assert_eq!(person_count(&gf), 1);
}

// ---------------------------------------------------------------------------
// Mixed buffered-append + rewrite writes in one statement (#792, Step 1)
// ---------------------------------------------------------------------------
// CREATE/MERGE (buffered append) + DELETE (in-place rewrite) have no defined
// ordering yet; mixing them in one statement is rejected loudly rather than
// silently running only one side. Proper sequencing is #792 Step 2.

#[test]
fn create_and_delete_in_one_statement_sequences_in_clause_order() {
    // #792 Step 2: mixed CREATE+DELETE now executes clause-ordered (the
    // Step-1 guard used to reject it) — Bob is created, then Alice deleted,
    // with the six-counter summary reporting both.
    use arrow::array::UInt64Array;

    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (:Person {name:'Alice'})")
        .expect("seed Alice");

    let r = gf
        .execute("MATCH (a:Person {name:'Alice'}) CREATE (b:Person {name:'Bob'}) DELETE a")
        .expect("mixed CREATE+DELETE sequences");
    let count = |name: &str| {
        r.batches[0]
            .column_by_name(name)
            .unwrap_or_else(|| panic!("{name} column"))
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("UInt64 counter")
            .value(0)
    };
    assert_eq!(count("nodes_created"), 1);
    assert_eq!(count("nodes_deleted"), 1);

    // Net effect: Alice replaced by Bob.
    assert_eq!(person_count(&gf), 1);
    let bob = rows(&gf, "MATCH (p:Person {name:'Bob'}) RETURN p.node_uuid");
    assert_eq!(bob.stats.rows_produced, 1, "Bob persisted");
    let alice = rows(&gf, "MATCH (p:Person {name:'Alice'}) RETURN p.node_uuid");
    assert_eq!(alice.stats.rows_produced, 0, "Alice deleted");
}

#[test]
fn merge_and_delete_run_in_clause_order() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (:Person {name:'Alice'})")
        .expect("seed Alice");

    gf.execute("MATCH (a:Person {name:'Alice'}) MERGE (b:Person {name:'Bob'}) DELETE a")
        .expect("MERGE and DELETE share the statement driver");
    assert_eq!(
        rows(
            &gf,
            "MATCH (:Person {name:'Alice'}) RETURN count(*) AS count"
        )
        .batches[0]
            .column_by_name("count")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        0
    );
    assert_eq!(
        rows(&gf, "MATCH (:Person {name:'Bob'}) RETURN count(*) AS count").batches[0]
            .column_by_name("count")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );
}

#[test]
fn unmixed_writes_still_run_after_the_guard() {
    // Regression: the guard must not affect CREATE-only or DELETE-only statements.
    let gf = GraphForge::new(None).expect("in-memory instance");
    // CREATE-only succeeds.
    gf.execute("CREATE (:Person {name:'Alice'}), (:Person {name:'Bob'})")
        .expect("CREATE-only is unaffected");
    assert_eq!(person_count(&gf), 2);
    // DELETE-only succeeds.
    gf.execute("MATCH (p:Person {name:'Alice'}) DELETE p")
        .expect("DELETE-only is unaffected");
    assert_eq!(person_count(&gf), 1);
}

// ---------------------------------------------------------------------------
// Parameters
// ---------------------------------------------------------------------------

#[test]
fn parameterized_filter() {
    // `WHERE n.age > $min` with `{min: 28}` → Alice (30) and Carol (35) pass
    // `> 28` (Dave is 28, not `> 28`).
    let gf = forge();
    let params = HashMap::from([("min".to_owned(), IrLiteral::Int(28))]);
    let r = gf
        .execute_with_params(
            "MATCH (n:Person) WHERE n.age > $min RETURN n.node_uuid",
            &params,
        )
        .expect("parameterized query");
    assert_eq!(r.stats.rows_produced, 2);
}

#[test]
fn parameterized_single_writes_use_the_unified_driver() {
    use arrow::array::UInt64Array;

    let gf = GraphForge::new(None).expect("in-memory instance");
    let counter = |result: &graphforge_api::ExecutionResult, name: &str| {
        assert_eq!(result.schema.fields().len(), 6, "unified write summary");
        result.batches[0]
            .column_by_name(name)
            .unwrap_or_else(|| panic!("{name} counter"))
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("UInt64 counter")
            .value(0)
    };

    let create_params = HashMap::from([("name".to_owned(), IrLiteral::Str("Alice".into()))]);
    let created = gf
        .execute_with_params("CREATE (:Person {name: $name})", &create_params)
        .expect("parameterized CREATE");
    assert_eq!(counter(&created, "nodes_created"), 1);
    assert_eq!(counter(&created, "properties_set"), 1);

    let set_params = HashMap::from([
        ("name".to_owned(), IrLiteral::Str("Alice".into())),
        ("age".to_owned(), IrLiteral::Int(42)),
    ]);
    let set = gf
        .execute_with_params(
            "MATCH (n:Person) WHERE n.name = $name SET n.age = $age",
            &set_params,
        )
        .expect("parameterized SET");
    assert_eq!(counter(&set, "properties_set"), 1);

    let deleted = gf
        .execute_with_params(
            "MATCH (n:Person) WHERE n.age = $age DELETE n",
            &HashMap::from([("age".to_owned(), IrLiteral::Int(42))]),
        )
        .expect("parameterized DELETE");
    assert_eq!(counter(&deleted, "nodes_deleted"), 1);
    assert_eq!(person_count(&gf), 0);
}

#[test]
fn create_with_filter_shapes_results_without_losing_side_effects() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    let result = gf
        .execute(
            "UNWIND [1, 2, 3, 4, 5] AS x \
             CREATE (n:N {num: x}) \
             WITH n WHERE n.num % 2 = 0 \
             RETURN n.num AS num",
        )
        .expect("CREATE followed by filtering WITH");

    let values = result.batches[0]
        .column_by_name("num")
        .expect("num")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 num")
        .values();
    assert_eq!(values, &[2, 4]);
    assert_eq!(result.side_effects.expect("side effects").nodes_created, 5);
}

#[test]
fn create_with_limit_zero_keeps_schema_side_effects_and_persistence() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    let result = gf
        .execute("CREATE (n:N {num: 1}) RETURN n.num AS num LIMIT 0")
        .expect("terminal LIMIT 0");

    assert_eq!(result.stats.rows_produced, 0);
    assert!(result.schema.field_with_name("num").is_ok());
    assert_eq!(result.side_effects.expect("side effects").nodes_created, 1);
    assert_eq!(rows(&gf, "MATCH (n:N) RETURN n").stats.rows_produced, 1);
}

#[test]
fn created_relationship_return_retains_computed_property() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    let result = gf
        .execute("WITH 2020 AS year CREATE ()-[r:KNOWS {since: year}]->() RETURN r")
        .expect("created relationship return");
    let relationship = result.batches[0]
        .column_by_name("r")
        .expect("r")
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("r is a relationship Struct");
    let since = relationship
        .column_by_name("since")
        .expect("since")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("since is Int64");

    assert_eq!(since.value(0), 2020);
}

#[test]
fn delete_counts_labels_added_earlier_in_the_statement() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (:A)").expect("seed node");
    let result = gf
        .execute("MATCH (n:A) SET n:B DELETE n")
        .expect("label mutation followed by DELETE");
    let effects = result.side_effects.expect("side effects");

    assert_eq!(effects.labels_added, 1);
    assert_eq!(effects.labels_removed, 2);
}

// ---------------------------------------------------------------------------
// Streaming + cross-session persistence (#725)
// ---------------------------------------------------------------------------

#[test]
fn execute_stream_yields_shaped_batches() {
    use futures::StreamExt;

    let gf = forge();
    let stream = gf
        .execute_stream("MATCH (n:Person) RETURN n.node_uuid AS u")
        .expect("stream");

    // Schema is available before consuming (the RecordBatchReader contract the
    // bindings rely on, #587) and is already shaped: no surrogate, UUID typed,
    // metadata stamped.
    assert!(stream.schema().column_with_name("node_id").is_none());
    assert_eq!(
        stream.schema().field_with_name("u").unwrap().data_type(),
        &DataType::FixedSizeBinary(16),
    );
    assert!(
        stream
            .schema()
            .metadata()
            .contains_key("graphforge.query_id")
    );

    // Consume lazily on a runtime; rows match the collected path (5 Person).
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let rows: usize = rt.block_on(async {
        let mut s = stream;
        let mut n = 0;
        while let Some(batch) = s.next().await {
            n += batch.expect("stream batch").num_rows();
        }
        n
    });
    assert_eq!(rows, 5);
}

#[test]
fn execute_stream_owned_guard_outlives_dropped_forge() {
    use futures::StreamExt;

    let gf = forge();
    let (mut stream, schema, guard) = gf
        .execute_stream_owned("MATCH (n:Person) RETURN n.node_uuid AS u", &HashMap::new())
        .expect("owned stream");

    // The schema is advertised up front and is independent of the stream.
    assert_eq!(
        schema.field_with_name("u").unwrap().data_type(),
        &DataType::FixedSizeBinary(16),
    );

    // Drop the GraphForge before consuming: the guard must keep the Tokio
    // runtime and on-disk workspace alive so the detached stream still drives
    // to completion — the lazy-reader lifetime contract the Python binding
    // relies on (#587), including streaming Parquet fragment opens (#339).
    drop(gf);

    let mut rows = 0;
    while let Some(batch) = guard.block_on(stream.next()) {
        rows += batch.expect("stream batch").num_rows();
    }
    assert_eq!(rows, 5);
}

#[test]
fn execute_stream_rejects_writes() {
    let gf = forge();
    // `SendableRecordBatchStream` isn't `Debug`, so match the Result directly
    // rather than using `expect_err`.
    match gf.execute_stream("CREATE (:Person {name:'X'})") {
        Err(graphforge_api::GfError::Validation(_)) => {}
        Ok(_) => panic!("writes must not be streamable"),
        Err(other) => panic!("expected Validation error, got: {other:?}"),
    }
}

#[test]
fn runtime_catalog_persists_across_sessions() {
    // A Parquet-backed instance flushes its RuntimeCatalog after a write, so a
    // fresh `new(path)` reloads the observed types (#725).
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap();

    {
        let gf = GraphForge::new(Some(path)).expect("open");
        gf.execute("CREATE (:Person {name:'Alice'})")
            .expect("create");
        assert!(
            gf.runtime_catalog()
                .lock()
                .unwrap()
                .contains_entity_type("Person")
        );
    }
    // Reopen proves the catalog was included in the committed graph snapshot,
    // without reaching into generation-private participant bytes.
    let gf2 = GraphForge::new(Some(path)).expect("reopen");
    assert!(
        gf2.runtime_catalog()
            .lock()
            .unwrap()
            .contains_entity_type("Person"),
        "reopened instance reloaded the persisted catalog"
    );
    let r = gf2
        .execute("MATCH (n:Person) RETURN n.name AS name")
        .expect("read after reopen");
    assert_eq!(r.stats.rows_produced, 1);
}

// ---------------------------------------------------------------------------
// Mixed MATCH + CREATE (#703) — the CREATE runs once per matched row
// ---------------------------------------------------------------------------

/// `count(n)` over a label, as a `u64`.
fn count(gf: &GraphForge, label: &str) -> u64 {
    let r = rows(gf, &format!("MATCH (n:{label}) RETURN count(n) AS c"));
    r.batches[0]
        .column_by_name("c")
        .expect("count column")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count is Int64")
        .value(0) as u64
}

/// `count(*)` over a single-hop KNOWS pattern.
fn knows_count(gf: &GraphForge) -> i64 {
    let r = rows(
        gf,
        "MATCH (:Person)-[:KNOWS]->(:Person) RETURN count(*) AS c",
    );
    r.batches[0]
        .column_by_name("c")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

#[test]
fn mixed_match_create_runs_once_per_matched_row() {
    // #703: `MATCH (a:Person) CREATE (a)-[:KNOWS]->(b:Person)` creates one new
    // `b` + one KNOWS edge PER matched `a`. With 5 matched Persons that adds 5
    // Persons and 5 KNOWS edges.
    let gf = forge();
    let persons_before = count(&gf, "Person");
    let knows_before = knows_count(&gf);
    assert_eq!(persons_before, 5, "fixture has 5 Persons");

    gf.execute("MATCH (a:Person) CREATE (a)-[:KNOWS]->(b:Person)")
        .expect("mixed MATCH+CREATE");

    // One new b + one KNOWS edge per matched a (5 each).
    assert_eq!(
        count(&gf, "Person"),
        persons_before + 5,
        "5 original + 5 newly-created b"
    );
    assert_eq!(
        knows_count(&gf),
        knows_before + 5,
        "5 new KNOWS edges, one per matched Person"
    );
}

#[test]
fn mixed_match_create_empty_match_creates_nothing() {
    // A MATCH that binds no rows drives zero creates. The inline property
    // predicate now filters (#748), so no Person matches → nothing created.
    let gf = forge();
    let before = count(&gf, "Person");
    gf.execute("MATCH (a:Person {name:'Nobody'}) CREATE (b:Person)")
        .expect("mixed create over empty match");
    assert_eq!(count(&gf, "Person"), before, "empty match creates nothing");
}

#[test]
fn mixed_match_with_create_returns_reference_and_created_node() {
    // #814: a matched node can be renamed through WITH, feed a CREATE pattern,
    // and still be projected alongside properties from the created row.
    let gf = forge();
    let knows_before = knows_count(&gf);
    let r = rows(
        &gf,
        "MATCH (n:Person {name:'Alice'}) \
         WITH n AS a \
         CREATE (a)-[:KNOWS]->(m:Person {w: 1}) \
         RETURN a.name AS name, m.w AS w",
    );

    assert_eq!(r.stats.rows_produced, 1);
    let effects = r.side_effects.as_ref().expect("write side effects");
    assert_eq!(effects.nodes_created, 1);
    assert_eq!(effects.relationships_created, 1);

    let name = r.batches[0]
        .column_by_name("name")
        .expect("name")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name is Utf8")
        .value(0);
    assert_eq!(name, "Alice");

    let w = r.batches[0]
        .column_by_name("w")
        .expect("w")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("w is Int64")
        .value(0);
    assert_eq!(w, 1);

    assert_eq!(
        knows_count(&gf),
        knows_before + 1,
        "the created relationship persisted"
    );
}

#[test]
fn standalone_create_still_creates_once() {
    // Regression: a standalone CREATE (no MATCH) still creates exactly once via
    // the unit-row input path.
    let gf = forge();
    let before = count(&gf, "Person");
    gf.execute("CREATE (:Person {name:'Frank'})")
        .expect("standalone create");
    assert_eq!(
        count(&gf, "Person"),
        before + 1,
        "standalone CREATE adds one"
    );
}

#[test]
fn standalone_create_return_projects_created_node_and_property() {
    // Single CREATE read suffixes use the statement driver's frontier; the
    // created node and its properties remain available to RETURN.
    let gf = GraphForge::new(None).expect("in-memory instance");
    let r = rows(
        &gf,
        "CREATE (m:Person {name:'Zed', w: 1}) RETURN m, m.w AS w",
    );

    assert_eq!(r.stats.rows_produced, 1);
    assert!(
        r.batches[0]
            .column_by_name("m")
            .expect("m")
            .as_any()
            .downcast_ref::<StructArray>()
            .is_some(),
        "`m` materializes as a whole node"
    );
    let w = r.batches[0]
        .column_by_name("w")
        .expect("w")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("w is Int64")
        .value(0);
    assert_eq!(w, 1);
}

#[test]
fn create_return_rejects_reserved_node_identity_property() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    let err = gf
        .execute("CREATE (m:Person {node_uuid:'shadow'}) RETURN m")
        .expect_err("reserved node identity property must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("reserved node topology field"),
        "unexpected error: {msg}"
    );
}

// ---------------------------------------------------------------------------
// Write — SET / REMOVE (#791)
// ---------------------------------------------------------------------------

/// Read a single-row `Int64` result column, asserting exactly one row.
fn read_int(gf: &GraphForge, cypher: &str) -> Option<i64> {
    let r = rows(gf, cypher);
    assert_eq!(r.stats.rows_produced, 1, "expected one row from {cypher:?}");
    let col = r.batches[0].column(0);
    if col.data_type() == &DataType::Null || col.is_null(0) {
        return None;
    }
    Some(
        col.as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 result")
            .value(0),
    )
}

#[test]
fn set_node_property_persists_and_reads_back() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (:Person {name:'Alice', age:30})")
        .expect("create Alice");

    gf.execute("MATCH (p:Person {name:'Alice'}) SET p.age = 42")
        .expect("set age");
    assert_eq!(
        read_int(&gf, "MATCH (p:Person {name:'Alice'}) RETURN p.age AS age"),
        Some(42),
        "SET overwrote the existing property"
    );
}

#[test]
fn set_new_property_on_propertyless_node() {
    // SET a property a node never had → a fresh property row is minted.
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (:Person {name:'Bob'})")
        .expect("create Bob without age");

    gf.execute("MATCH (p:Person {name:'Bob'}) SET p.age = 25")
        .expect("set age on a node with no prior age");
    assert_eq!(
        read_int(&gf, "MATCH (p:Person {name:'Bob'}) RETURN p.age AS age"),
        Some(25)
    );
}

#[test]
fn set_runtime_expression_value() {
    // The value is a runtime expression over the matched row, not a literal.
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (:Person {name:'Alice', age:30})")
        .expect("create Alice");

    gf.execute("MATCH (p:Person {name:'Alice'}) SET p.age = p.age + 1")
        .expect("set age to a runtime expression");
    assert_eq!(
        read_int(&gf, "MATCH (p:Person {name:'Alice'}) RETURN p.age AS age"),
        Some(31),
        "SET evaluated `p.age + 1` per row"
    );
}

#[test]
fn set_cross_variable_value() {
    // `SET a.age = b.age` copies a connected var's property to the target. Uses a
    // connected pattern (the dst-node properties are joined since #789) — disjoint
    // comma-separated `MATCH (a), (b)` is a separate, unrelated limitation.
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute(
        "CREATE (a:Person {name:'Alice', age:30})-[:KNOWS]->(b:Person {name:'Bob', age:25})",
    )
    .expect("create Alice → Bob");

    gf.execute("MATCH (a:Person)-[:KNOWS]->(b:Person) SET a.age = b.age")
        .expect("set a.age from b.age");
    assert_eq!(
        read_int(&gf, "MATCH (p:Person {name:'Alice'}) RETURN p.age AS age"),
        Some(25),
        "Alice's age now matches Bob's"
    );
}

#[test]
fn remove_node_property_yields_null() {
    // A second Person keeps `age`, so the `age` column survives the rewrite and
    // the removed property reads back as NULL (rather than the column vanishing —
    // a separate dynamic-schema limitation when *no* row keeps the property).
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (:Person {name:'Alice', age:30}), (:Person {name:'Bob', age:25})")
        .expect("create Alice and Bob, both with age");

    gf.execute("MATCH (p:Person {name:'Alice'}) REMOVE p.age")
        .expect("remove Alice's age");
    assert_eq!(
        read_int(&gf, "MATCH (p:Person {name:'Alice'}) RETURN p.age AS age"),
        None,
        "Alice's removed property reads back as NULL"
    );
    assert_eq!(
        read_int(&gf, "MATCH (p:Person {name:'Bob'}) RETURN p.age AS age"),
        Some(25),
        "Bob's age is untouched"
    );
}

#[test]
fn remove_missing_node_property_preserves_keys_over_empty_shapes() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (), (), ()")
        .expect("create propertyless nodes");

    let r = rows(
        &gf,
        "MATCH (n) REMOVE n.num RETURN sum(size(keys(n))) AS totalNumberOfProps",
    );

    assert_eq!(r.stats.rows_produced, 1);
    let total = r.batches[0]
        .column_by_name("totalNumberOfProps")
        .expect("totalNumberOfProps")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("sum(size(keys(n))) is Int64");
    assert_eq!(total.value(0), 0);
}

#[test]
fn set_edge_property_persists_and_reads_back() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (a:Person {name:'Alice'})-[:KNOWS]->(b:Person {name:'Bob'})")
        .expect("create a KNOWS edge");

    gf.execute("MATCH (a:Person)-[r:KNOWS]->(b:Person) SET r.since = 2020")
        .expect("set edge property");
    assert_eq!(
        read_int(
            &gf,
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r.since AS since"
        ),
        Some(2020)
    );
}

#[test]
fn remove_edge_property_yields_null() {
    // Two KNOWS edges both carry `since`; removing it from one (selected by its
    // source node) leaves the column alive on the other, so the removed edge's
    // property reads back as NULL.
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute(
        "CREATE (a:Person {name:'Alice'})-[:KNOWS {since:2020}]->(b:Person {name:'Bob'}), \
         (c:Person {name:'Carol'})-[:KNOWS {since:1999}]->(d:Person {name:'Dave'})",
    )
    .expect("create two KNOWS edges with `since`");

    gf.execute("MATCH (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person) REMOVE r.since")
        .expect("remove since from Alice's edge");

    assert_eq!(
        read_int(
            &gf,
            "MATCH (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person) RETURN r.since AS since"
        ),
        None,
        "Alice's edge `since` reads back as NULL"
    );
    assert_eq!(
        read_int(
            &gf,
            "MATCH (a:Person {name:'Carol'})-[r:KNOWS]->(b:Person) RETURN r.since AS since"
        ),
        Some(1999),
        "Carol's edge `since` is untouched"
    );
}

#[test]
fn set_property_maps_merge_and_replace_nodes() {
    let dir = tempfile::TempDir::new().unwrap();
    let gf = GraphForge::new(dir.path().to_str()).expect("persistent instance");
    gf.execute("CREATE (:Person {name:'Alice', age:29, stale:true})")
        .expect("create Alice");
    let same_statement = rows(
        &gf,
        "MATCH (p:Person) SET p += {age: 30, city:'Denver'} RETURN p",
    );
    let node = same_statement.batches[0]
        .column_by_name("p")
        .expect("p")
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("node struct");
    assert_eq!(
        node.column_by_name("city")
            .expect("same-statement city")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Utf8 city")
            .value(0),
        "Denver"
    );
    let merged = rows(
        &gf,
        "MATCH (p:Person) RETURN p.name AS name, p.age AS age, p.city AS city",
    );
    assert_eq!(merged.stats.rows_produced, 1);
    assert_eq!(
        merged.batches[0]
            .column_by_name("age")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        30
    );

    gf.execute("MATCH (p:Person) SET p = {name:p.name, active:true}")
        .expect("replace map with runtime value");
    let replaced = rows(
        &gf,
        "MATCH (p:Person) RETURN p.name AS name, p.age AS age, p.city AS city, p.stale AS stale, p.active AS active",
    );
    let batch = &replaced.batches[0];
    assert_eq!(
        batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "Alice"
    );
    assert_eq!(
        batch.column_by_name("age").unwrap().data_type(),
        &DataType::Null
    );
    assert_eq!(
        batch.column_by_name("city").unwrap().data_type(),
        &DataType::Null
    );
    assert_eq!(
        batch.column_by_name("stale").unwrap().data_type(),
        &DataType::Null
    );
}

#[test]
fn set_property_maps_support_parameters_and_relationships() {
    let dir = tempfile::TempDir::new().unwrap();
    let gf = GraphForge::new(dir.path().to_str()).expect("persistent instance");
    gf.execute(
        "CREATE (:Person {name:'Alice'})-[:KNOWS {since:2020, stale:true}]->(:Person {name:'Bob'})",
    )
    .expect("seed relationship");

    let params = HashMap::from([(
        "updates".to_owned(),
        IrLiteral::Map(vec![
            ("since".to_owned(), IrLiteral::Int(2024)),
            ("weight".to_owned(), IrLiteral::Float(0.75)),
        ]),
    )]);
    gf.execute_with_params("MATCH ()-[r:KNOWS]->() SET r += $updates", &params)
        .expect("merge parameter map into relationship");
    let merged = rows(
        &gf,
        "MATCH ()-[r:KNOWS]->() RETURN r.since AS since, r.weight AS weight, r.stale AS stale",
    );
    assert_eq!(
        merged.batches[0]
            .column_by_name("since")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2024
    );

    gf.execute("MATCH ()-[r:KNOWS]->() SET r = {weight:r.weight}")
        .expect("replace relationship map with runtime value");
    let replaced = rows(
        &gf,
        "MATCH ()-[r:KNOWS]->() RETURN r.since AS since, r.weight AS weight, r.stale AS stale",
    );
    assert_eq!(
        replaced.batches[0]
            .column_by_name("since")
            .unwrap()
            .data_type(),
        &DataType::Null
    );
    assert_eq!(
        replaced.batches[0]
            .column_by_name("stale")
            .unwrap()
            .data_type(),
        &DataType::Null
    );
}

#[test]
fn set_and_remove_route_untyped_relationships_by_runtime_type() {
    let dir = tempfile::TempDir::new().unwrap();
    let gf = GraphForge::new(dir.path().to_str()).expect("persistent instance");
    gf.execute("CREATE (:N)-[:KNOWS {old:1}]->(:N), (:N)-[:LIKES {old:2}]->(:N)")
        .expect("seed two relationship types");

    gf.execute("MATCH ()-[r]->() SET r.mark = 7 REMOVE r.old")
        .expect("untyped relationship writes");
    let result = rows(
        &gf,
        "MATCH ()-[r]->() RETURN type(r) AS kind, r.mark AS mark, r.old AS old ORDER BY kind",
    );
    assert_eq!(result.stats.rows_produced, 2);
    for batch in &result.batches {
        let marks = batch
            .column_by_name("mark")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert_eq!(marks.value(row), 7);
        }
        assert_eq!(
            batch.column_by_name("old").unwrap().data_type(),
            &DataType::Null
        );
    }

    gf.execute("MATCH ()-[r]->() SET r.mark = 8 REMOVE r.mark")
        .expect("SET then REMOVE same property");
    let removed = rows(&gf, "MATCH ()-[r]->() RETURN r.mark AS mark");
    for batch in &removed.batches {
        assert_eq!(
            batch.column_by_name("mark").unwrap().data_type(),
            &DataType::Null
        );
    }

    gf.execute("MATCH ()-[r]->() REMOVE r.mark SET r.mark = 9")
        .expect("REMOVE then SET same property");
    let restored = rows(&gf, "MATCH ()-[r]->() RETURN r.mark AS mark");
    for batch in &restored.batches {
        let marks = batch
            .column_by_name("mark")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert_eq!(marks.value(row), 9);
        }
    }
}

#[test]
fn set_list_property_round_trips_and_supports_later_list_expression() {
    let dir = tempfile::TempDir::new().unwrap();
    let gf = GraphForge::new(dir.path().to_str()).expect("persistent instance");
    gf.execute("CREATE (:N {name:'x'})").expect("seed node");
    gf.execute("MATCH (n:N) SET n.numbers = [1, 2, 3]")
        .expect("set list property");
    gf.execute("MATCH (n:N) SET n.numbers = n.numbers + [4, 5]")
        .expect("extend stored list property");

    let result = rows(&gf, "MATCH (n:N) RETURN n.numbers AS numbers");
    let numbers = result.batches[0]
        .column_by_name("numbers")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("list property");
    let values = numbers
        .value(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec();
    assert_eq!(values, vec![1, 2, 3, 4, 5]);
}

#[test]
fn merge_node_is_idempotent_by_label_and_properties() {
    let dir = tempfile::TempDir::new().unwrap();
    let gf = GraphForge::new(dir.path().to_str()).expect("persistent instance");

    gf.execute("MERGE (:Person {name:'Alice'})")
        .expect("first MERGE creates Alice");
    gf.execute("MERGE (:Person {name:'Alice'})")
        .expect("second MERGE matches Alice");

    let result = rows(
        &gf,
        "MATCH (p:Person {name:'Alice'}) RETURN count(*) AS count",
    );
    assert_eq!(
        result.batches[0]
            .column_by_name("count")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );
}

#[test]
fn merge_all_node_matches_feed_every_optional_relationship_match() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (a:A), (b:B) CREATE (a)-[:T1]->(b), (b)-[:T2]->(a)")
        .expect("fixture");

    let merged = rows(&gf, "MATCH (a) MERGE (b) WITH * RETURN count(*) AS count");
    assert_eq!(
        merged.batches[0]
            .column_by_name("count")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        4
    );

    let relationships = rows(
        &gf,
        "MATCH (a) MERGE (b) WITH * OPTIONAL MATCH (a)-[r]-(b) RETURN type(r) AS rel",
    );
    let rels = relationships.batches[0]
        .column_by_name("rel")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(rels.iter().filter(|value| *value == Some("T1")).count(), 2);
    assert_eq!(rels.iter().filter(|value| *value == Some("T2")).count(), 2);
    assert_eq!(rels.iter().filter(Option::is_none).count(), 2);

    let optional = rows(
        &gf,
        "MATCH (a) MERGE (b) WITH * OPTIONAL MATCH (a)--(b) RETURN count(*) AS count",
    );
    assert_eq!(
        optional.batches[0]
            .column_by_name("count")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        6
    );
}

#[test]
fn merge_node_runs_only_the_selected_on_action() {
    let dir = tempfile::TempDir::new().unwrap();
    let gf = GraphForge::new(dir.path().to_str()).expect("persistent instance");
    let query = "MERGE (p:Person {name:'Alice'}) \
                 ON CREATE SET p.branch = 1 \
                 ON MATCH SET p.branch = 2";

    gf.execute(query).expect("create branch");
    assert_eq!(
        read_int(
            &gf,
            "MATCH (p:Person {name:'Alice'}) RETURN p.branch AS value"
        ),
        Some(1)
    );

    gf.execute(query).expect("match branch");
    assert_eq!(
        read_int(
            &gf,
            "MATCH (p:Person {name:'Alice'}) RETURN p.branch AS value"
        ),
        Some(2)
    );
}

#[test]
fn merge_relationship_combines_pending_and_committed_matches() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (a:A), (b:B), (a)-[:TYPE]->(b)")
        .expect("seed committed relationship");

    assert_eq!(
        read_int(
            &gf,
            "MATCH (a:A), (b:B) \
             CREATE (a)-[:TYPE]->(b) \
             MERGE (a)-[r:TYPE]->(b) \
             RETURN count(r) AS value"
        ),
        Some(2)
    );
    assert_eq!(
        read_int(&gf, "MATCH ()-[r:TYPE]->() RETURN count(r) AS value"),
        Some(2)
    );
}

#[test]
fn merge_relationship_is_idempotent_and_runs_selected_action() {
    let dir = tempfile::TempDir::new().unwrap();
    let gf = GraphForge::new(dir.path().to_str()).expect("persistent instance");
    gf.execute("CREATE (:Person {name:'Alice'}), (:Person {name:'Bob'})")
        .expect("seed endpoints");
    let query = "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}) \
                 MERGE (a)-[r:KNOWS {since:2020}]->(b) \
                 ON CREATE SET r.branch = 1 \
                 ON MATCH SET r.branch = 2";

    gf.execute(query).expect("create relationship branch");
    assert_eq!(
        read_int(&gf, "MATCH ()-[r:KNOWS]->() RETURN r.branch AS value"),
        Some(1)
    );
    gf.execute(query).expect("match relationship branch");
    let result = rows(
        &gf,
        "MATCH (:Person {name:'Alice'})-[r:KNOWS]->(:Person {name:'Bob'}) \
         RETURN count(*) AS count",
    );
    let batch = &result.batches[0];
    assert_eq!(
        batch
            .column_by_name("count")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );
    assert_eq!(
        read_int(&gf, "MATCH ()-[r:KNOWS]->() RETURN r.branch AS value"),
        Some(2)
    );
}

#[test]
fn merge_relationship_preserves_all_existing_matches() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (a:A), (b:B), (a)-[:TYPE]->(b), (a)-[:TYPE]->(b)")
        .expect("seed parallel relationships");

    assert_eq!(
        read_int(
            &gf,
            "MATCH (a:A), (b:B) MERGE (a)-[r:TYPE]->(b) RETURN count(r) AS value"
        ),
        Some(2)
    );
    assert_eq!(
        read_int(&gf, "MATCH ()-[r:TYPE]->() RETURN count(r) AS value"),
        Some(2)
    );
}

#[test]
fn merge_relationship_resolves_row_properties_after_entity_aliasing() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    let result = rows(
        &gf,
        "CREATE (a:Foo {id:0}), (b:Bar) \
         WITH a AS source, b AS target \
         UNWIND [['admin'], ['reader']] AS roles \
         MERGE (source)-[r:ACCESS {roles:roles}]->(target) \
         RETURN count(r) AS count",
    );
    assert_eq!(
        result.batches[0]
            .column_by_name("count")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2
    );
    assert_eq!(
        read_int(&gf, "MATCH ()-[r:ACCESS]->() RETURN count(r) AS value"),
        Some(2)
    );
    assert_eq!(
        read_int(
            &gf,
            "MATCH ()-[r:ACCESS]->() WHERE r.roles = ['admin'] RETURN count(r) AS value"
        ),
        Some(1)
    );
    assert_eq!(
        read_int(
            &gf,
            "MATCH ()-[r:ACCESS]->() WHERE r.roles = ['reader'] RETURN count(r) AS value"
        ),
        Some(1)
    );
}

#[test]
fn with_where_preserves_forwarded_entity_alias_qualifiers() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (:Person {name:'Alice'}), (:Person {name:'Bob'})")
        .expect("seed nodes");

    let result = rows(
        &gf,
        "MATCH (n:Person) \
         WITH n AS person WHERE person.name = 'Alice' \
         RETURN person.name AS name",
    );
    assert_eq!(result.stats.rows_produced, 1);
    assert_eq!(
        result.batches[0]
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "Alice"
    );
}

#[test]
fn mixed_create_and_set_applies_to_the_created_node() {
    // #792 Step 2: SET on an entity created earlier in the same statement
    // lands in the writer's buffer and flushes with it.
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (p:Person {name:'X'}) SET p.age = 1")
        .expect("mixed CREATE + SET sequences");

    let r = rows(&gf, "MATCH (p:Person {name:'X'}) RETURN p.age AS age");
    assert_eq!(r.stats.rows_produced, 1);
    let age = r.batches[0]
        .column_by_name("age")
        .expect("age")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 age")
        .value(0);
    assert_eq!(age, 1, "the SET applied to the created node");
}

#[test]
fn later_set_reads_property_written_earlier_in_statement() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (:N {name:'x'})").expect("seed");
    gf.execute("MATCH (n:N) SET n.a = 1 SET n.b = n.a + 1")
        .expect("later SET sees n.a");

    let result = rows(&gf, "MATCH (n:N) RETURN n.a AS a, n.b AS b");
    for (name, expected) in [("a", 1), ("b", 2)] {
        let value = result.batches[0]
            .column_by_name(name)
            .expect(name)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64")
            .value(0);
        assert_eq!(value, expected);
    }
}

#[test]
fn pending_created_node_set_reads_statement_overlay() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (n:N) SET n.a = 4 SET n.b = n.a + 3")
        .expect("pending-created node uses overlay");

    let result = rows(&gf, "MATCH (n:N) RETURN n.b AS b");
    let b = result.batches[0]
        .column_by_name("b")
        .expect("b")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64")
        .value(0);
    assert_eq!(b, 7);
}

#[test]
fn later_set_observes_property_removed_in_statement() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (:N {a: 1})").expect("seed");
    gf.execute("MATCH (n:N) REMOVE n.a SET n.b = CASE WHEN n.a IS NULL THEN 9 ELSE n.a END")
        .expect("later SET sees removed property as null");

    let result = rows(&gf, "MATCH (n:N) RETURN n.b AS b");
    let b = result.batches[0]
        .column_by_name("b")
        .expect("b")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64")
        .value(0);
    assert_eq!(b, 9);
}

#[test]
fn terminal_return_after_write_phases_returns_rows_and_side_effects() {
    // #814: terminal RETURN projections can be evaluated from the final writer
    // frontier instead of replacing rows with a write summary.
    let gf = GraphForge::new(None).expect("in-memory instance");
    let r = rows(&gf, "CREATE (n:Person) DELETE n RETURN 1 AS ok");
    assert_eq!(r.stats.rows_produced, 1);
    let effects = r.side_effects.as_ref().expect("write side effects");
    assert_eq!(effects.nodes_created, 1);
    assert_eq!(effects.nodes_deleted, 1);
    let ok = r.batches[0]
        .column_by_name("ok")
        .expect("ok")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("ok is Int64")
        .value(0);
    assert_eq!(ok, 1);
}

#[test]
fn label_mutations_are_idempotent_counted_and_persistent() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap();
    let gf = graphforge_api::GraphForge::new(Some(path)).unwrap();
    gf.execute("CREATE (:Base {name: 'A'})").unwrap();

    let added = gf
        .execute("MATCH (n:Base) SET n:Extra:Extra RETURN n.name AS name")
        .unwrap();
    let effects = added.side_effects.expect("SET label effects");
    assert_eq!(effects.labels_added, 1);
    assert_eq!(
        gf.execute("MATCH (n:Extra) RETURN n")
            .unwrap()
            .stats
            .rows_produced,
        1
    );

    let idempotent = gf.execute("MATCH (n:Base) SET n:Extra RETURN n").unwrap();
    assert_eq!(
        idempotent
            .side_effects
            .expect("idempotent effects")
            .labels_added,
        0
    );

    let removed = gf
        .execute("MATCH (n:Base) REMOVE n:Base:Extra RETURN n.name AS name")
        .unwrap();
    assert_eq!(
        removed
            .side_effects
            .expect("REMOVE label effects")
            .labels_removed,
        2
    );
    drop(gf);

    let reopened = graphforge_api::GraphForge::new(Some(path)).unwrap();
    assert_eq!(
        reopened
            .execute("MATCH (n:Base) RETURN n")
            .unwrap()
            .stats
            .rows_produced,
        0
    );
    assert_eq!(
        reopened
            .execute("MATCH (n:Extra) RETURN n")
            .unwrap()
            .stats
            .rows_produced,
        0
    );
    let unlabeled = reopened
        .execute("MATCH (n) WHERE n.name = 'A' RETURN labels(n) AS labels")
        .unwrap();
    assert_eq!(unlabeled.stats.rows_produced, 1, "node must still exist");
    let labels = unlabeled.batches[0]
        .column_by_name("labels")
        .expect("labels column")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("labels list");
    let values = labels.value(0);
    let values = values
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("label strings");
    assert!(values.is_empty(), "removed node labels must stay empty");
}

#[test]
fn graph_read_after_write_sees_pending_node() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    let result = gf
        .execute("CREATE (n:Person) WITH n MATCH (m:Person) RETURN m")
        .expect("graph read after write should see the pending node");
    assert_eq!(result.stats.rows_produced, 1);
    assert_eq!(result.side_effects.unwrap().nodes_created, 1);

    let persisted = gf
        .execute("MATCH (m:Person) RETURN count(m) AS count")
        .expect("statement should commit after the pending read");
    let count = persisted.batches[0]
        .column_by_name("count")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 1);
}

#[test]
fn mixed_delete_then_set_on_deleted_errors_and_aborts() {
    // Write-after-delete is an error, and the abort leaves the pre-statement
    // state intact (nothing commits before the phase loop finishes).
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (:Person {name:'Y'})").expect("seed");

    let err = gf
        .execute("MATCH (a:Person {name:'Y'}) DELETE a SET a.x = 1")
        .expect_err("SET on a deleted entity must error");
    assert!(
        err.to_string().contains("deleted in this statement"),
        "got: {err}"
    );
    assert_eq!(person_count(&gf), 1, "abort left Y intact");
}

#[test]
fn execute_stream_rejects_set() {
    let gf = forge();
    // The Ok stream is not `Debug`, so match rather than `expect_err`.
    match gf.execute_stream("MATCH (p:Person) SET p.age = 1") {
        Err(graphforge_api::GfError::Validation(_)) => {}
        Err(other) => panic!("expected a Validation error, got: {other}"),
        Ok(_) => panic!("execute_stream must reject SET"),
    }
}

#[test]
fn repeated_node_in_one_hop_filters_against_the_existing_binding() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (a:A)-[:LOOP]->(a), (:A)-[:T]->(:A)")
        .expect("seed");

    let r = rows(&gf, "MATCH (n)-[r]->(n) RETURN count(r) AS total");
    let total = r.batches[0]
        .column_by_name("total")
        .expect("total")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 total")
        .value(0);
    assert_eq!(total, 1);
}

#[test]
fn relationships_are_unique_within_but_not_across_path_patterns() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (:A)-[:T]->(:B)").expect("seed");

    let one_path = rows(&gf, "MATCH ()-[r1]->()<-[r2]-() RETURN count(*) AS total");
    let one_path_total = one_path.batches[0]
        .column_by_name("total")
        .expect("total")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 total")
        .value(0);
    assert_eq!(one_path_total, 0, "one edge cannot fill both path hops");

    let separate = rows(
        &gf,
        "MATCH (a)-[r1]->(b), (x)-[r2]->(y) RETURN count(*) AS total",
    );
    let separate_total = separate.batches[0]
        .column_by_name("total")
        .expect("total")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 total")
        .value(0);
    assert_eq!(separate_total, 1, "separate patterns may reuse an edge");
}

#[test]
fn multi_segment_and_zero_hop_named_paths_compose() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (:N {i: 1})-[:T]->(:N {i: 2})-[:T]->(:N {i: 3})-[:T]->(:N {i: 4})")
        .expect("seed");

    let fixed = rows(
        &gf,
        "MATCH p = (:N {i: 1})-[:T]->()-[:T]->(:N {i: 3}) \
         RETURN length(p) AS l, size(nodes(p)) AS n, size(relationships(p)) AS r",
    );
    for (name, expected) in [("l", 2), ("n", 3), ("r", 2)] {
        let column = fixed.batches[0].column_by_name(name).expect(name);
        let value = if let Some(values) = column.as_any().downcast_ref::<Int64Array>() {
            values.value(0)
        } else {
            i64::try_from(
                column
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .expect("integer result")
                    .value(0),
            )
            .expect("small path length")
        };
        assert_eq!(value, expected, "{name}");
    }

    let zero = rows(
        &gf,
        "MATCH p = (n:N {i: 1}) \
         RETURN length(p) AS l, size(nodes(p)) AS n, size(relationships(p)) AS r",
    );
    for (name, expected) in [("l", 0), ("n", 1), ("r", 0)] {
        let column = zero.batches[0].column_by_name(name).expect(name);
        let value = if let Some(values) = column.as_any().downcast_ref::<Int64Array>() {
            values.value(0)
        } else {
            i64::try_from(
                column
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .expect("integer result")
                    .value(0),
            )
            .expect("small path length")
        };
        assert_eq!(value, expected, "{name}");
    }
}

#[test]
fn mixed_variable_and_fixed_named_path_composes() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (:N {i: 1})-[:T]->(:N {i: 2})-[:T]->(:N {i: 3})-[:T]->(:N {i: 4})")
        .expect("seed");

    let result = rows(
        &gf,
        "MATCH p = (:N {i: 1})-[:T*2]->(:N {i: 3})-[:T]->(:N {i: 4}) \
         RETURN length(p) AS l, size(nodes(p)) AS n, size(relationships(p)) AS r",
    );
    for (name, expected) in [("l", 3), ("n", 4), ("r", 3)] {
        let column = result.batches[0].column_by_name(name).expect(name);
        let value = if let Some(values) = column.as_any().downcast_ref::<Int64Array>() {
            values.value(0)
        } else {
            i64::try_from(
                column
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .expect("integer result")
                    .value(0),
            )
            .expect("small path length")
        };
        assert_eq!(value, expected, "{name}");
    }
}

#[test]
fn compound_aggregate_preserves_renamed_path_and_functions() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (:N)-[:T]->(:N)").expect("seed");

    let result = rows(
        &gf,
        "MATCH p = ()-[:T]->() \
         WITH p AS q, count(*) + 1 AS c \
         RETURN q, c, size(nodes(q)) AS n, size(relationships(q)) AS r, length(q) AS l",
    );
    assert_eq!(result.stats.rows_produced, 1);
    assert!(
        result.batches[0]
            .column_by_name("q")
            .expect("q")
            .as_any()
            .downcast_ref::<StructArray>()
            .is_some(),
        "renamed path remains a path struct"
    );
    for (name, expected) in [("c", 2), ("n", 2), ("r", 1), ("l", 1)] {
        let column = result.batches[0].column_by_name(name).expect(name);
        let value = if let Some(values) = column.as_any().downcast_ref::<Int64Array>() {
            values.value(0)
        } else {
            i64::try_from(
                column
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .expect("integer result")
                    .value(0),
            )
            .expect("small value")
        };
        assert_eq!(value, expected, "{name}");
    }
}

#[test]
fn return_nested_aggregates_use_projected_implicit_group_keys() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (a:A), (b:B {num: 42})").expect("seed");

    let grouped = rows(&gf, "MATCH (a) RETURN a, count(a) + 3 AS total");
    assert_eq!(grouped.stats.rows_produced, 2);
    assert!(grouped.batches[0].column_by_name("a").is_some());
    let totals = grouped.batches[0]
        .column_by_name("total")
        .expect("total")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("integer total");
    assert!((0..totals.len()).all(|row| totals.value(row) == 4));

    let nested_map = rows(
        &gf,
        "MATCH (a:A), (b:B) \
         RETURN coalesce(a.num, b.num) AS foo, b.num AS bar, {name: count(b)} AS baz",
    );
    assert_eq!(nested_map.stats.rows_produced, 1);
    let baz = nested_map.batches[0]
        .column_by_name("baz")
        .expect("baz")
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("map result");
    assert!(!baz.is_null(0));
    let names = baz
        .column_by_name("name")
        .expect("name")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("integer count");
    assert!(!names.is_null(0));
    assert_eq!(names.value(0), 1);

    let params = HashMap::from([("age".to_owned(), IrLiteral::Int(2_000))]);
    let parameterized = gf
        .execute_with_params(
            "MATCH (person) RETURN $age + avg(person.num) - 1000 AS value",
            &params,
        )
        .expect("parameterized nested aggregate");
    let values = parameterized.batches[0]
        .column_by_name("value")
        .expect("value")
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("floating-point aggregate expression");
    assert!(!values.is_null(0));
    assert_eq!(values.value(0), 1_042.0);
}

#[test]
fn with_forwards_named_path_values() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE ()").expect("seed");

    let result = rows(&gf, "MATCH p = (a) WITH p RETURN p");
    assert_eq!(result.stats.rows_produced, 1);
    assert!(
        result.batches[0]
            .column_by_name("p")
            .expect("p")
            .as_any()
            .downcast_ref::<StructArray>()
            .is_some()
    );

    let wildcard = rows(&gf, "MATCH p = (a) WITH p, a RETURN *");
    assert!(wildcard.batches[0].column_by_name("a").is_some());
    assert!(wildcard.batches[0].column_by_name("p").is_some());

    let functions = rows(
        &gf,
        "MATCH p = (a) WITH p \
         RETURN length(p) AS hops, size(nodes(p)) AS nodes, \
                size(relationships(p)) AS relationships",
    );
    for (name, expected) in [("hops", 0), ("nodes", 1), ("relationships", 0)] {
        let values = functions.batches[0]
            .column_by_name(name)
            .expect(name)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("integer path function");
        assert!(!values.is_null(0), "{name}");
        assert_eq!(values.value(0), expected, "{name}");
    }
}

#[test]
fn deleted_entities_cannot_be_projected_after_delete() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (:A {num: 0})").expect("seed node");
    let error = gf
        .execute("MATCH (n) DELETE n RETURN n.num")
        .expect_err("deleted node property access must fail");
    assert!(
        matches!(error, graphforge_api::GfError::Execution(message) if message.contains("DeletedEntityAccess"))
    );

    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE ()-[:T {num: 0}]->()")
        .expect("seed relationship");
    let error = gf
        .execute("MATCH ()-[r]->() DELETE r RETURN r.num")
        .expect_err("deleted relationship property access must fail");
    assert!(
        matches!(error, graphforge_api::GfError::Execution(message) if message.contains("DeletedEntityAccess"))
    );
}

#[test]
fn create_can_reference_properties_of_earlier_nodes_in_same_clause() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute(
        "CREATE (a:End {num: 42, id: 0}), \
         (:End {num: 3}), \
         (:Begin {num: a.id})",
    )
    .expect("sequential CREATE property reference");

    let result = rows(&gf, "MATCH (n:Begin) RETURN n.num AS num");
    assert_eq!(result.stats.rows_produced, 1);
    let nums = result.batches[0]
        .column_by_name("num")
        .expect("num")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("integer num");
    assert!(!nums.is_null(0));
    assert_eq!(nums.value(0), 0);
}

#[test]
fn with_alias_can_be_rebound_through_nested_maps() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    let result = rows(
        &gf,
        "CREATE (m {id: 0}) \
         WITH {first: m.id} AS m \
         WITH {second: m.first} AS m \
         RETURN m.second AS value",
    );
    assert_eq!(result.stats.rows_produced, 1);
    let values = result.batches[0]
        .column_by_name("value")
        .expect("value")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("integer value");
    assert!(!values.is_null(0));
    assert_eq!(values.value(0), 0);
}

#[test]
fn with_alias_property_access_after_head_collect() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute(
        "CREATE (a:Person), (b:Person), \
                (m1:Message {id: 10}), (m2:Message {id: 5}) \
         CREATE (a)-[:LIKE {creationDate: 20160614}]->(m1)-[:POSTED_BY]->(b), \
                (a)-[:LIKE {creationDate: 20160613}]->(m2)-[:POSTED_BY]->(b)",
    )
    .expect("seed");

    let result = rows(
        &gf,
        "MATCH (person:Person)<--(message)<-[like]-(:Person) \
         WITH like.creationDate AS likeTime, person AS person \
           ORDER BY likeTime, message.id \
         WITH head(collect({likeTime: likeTime})) AS latestLike, person AS person \
         WITH latestLike.likeTime AS likeTime ORDER BY likeTime \
         RETURN likeTime",
    );
    assert_eq!(result.stats.rows_produced, 1);
    assert_eq!(
        result.batches[0]
            .column_by_name("likeTime")
            .expect("likeTime")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("integer likeTime")
            .value(0),
        20_160_613
    );
}

#[test]
fn skip_and_limit_accept_variable_independent_expressions() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("UNWIND range(1, 10) AS i CREATE ({nr: i})")
        .expect("seed");

    let skipped = rows(
        &gf,
        "MATCH (n) WITH n SKIP toInteger(ceil(1.7)) RETURN count(*) AS count",
    );
    let count = skipped.batches[0]
        .column_by_name("count")
        .expect("count")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("integer count");
    assert!(!count.is_null(0));
    assert_eq!(count.value(0), 8);

    let limited = rows(
        &gf,
        "MATCH (n) WITH n LIMIT toInteger(ceil(1.7)) RETURN count(*) AS count",
    );
    let count = limited.batches[0]
        .column_by_name("count")
        .expect("count")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("integer count");
    assert_eq!(count.value(0), 2);
}

#[test]
fn dynamic_subscript_parameter_type_errors_remain_runtime_errors() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    let params = HashMap::from([
        ("expr".to_owned(), IrLiteral::Int(100)),
        ("idx".to_owned(), IrLiteral::Int(0)),
    ]);
    let error = gf
        .execute_with_params("WITH $expr AS expr, $idx AS idx RETURN expr[idx]", &params)
        .expect_err("scalar subscript must fail");
    assert!(matches!(error, graphforge_api::GfError::Execution(_)));
}
