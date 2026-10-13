//! Path-node hydration reads only what the expression over `nodes(p)` observes
//! (#1984).
//!
//! `[n IN nodes(p) | n.id]` needs one property column, not every label and
//! property of every node on the path. Each case below runs the same
//! expression twice against a durable project. The first query takes
//! `nodes(p)` directly, which the lowerer narrows to the observed parts. The
//! second routes the list through `WITH ... nodes(p) AS ns`, which the
//! analysis does not see through, so it hydrates the whole node. The results
//! must be identical, and the `hydration_*` counters show how much each one
//! read.

use arrow::util::pretty::pretty_format_batches;
use graphforge_api::GraphForge;
use graphforge_exec::demand;
use std::collections::BTreeMap;

/// A chain `1 -> 2 -> 3 -> 4 -> 5` with a branch `2 -> 6`.
///
/// Node 2 has two labels, node 4 three, node 5 stores a property named
/// `labels` that the label field shadows, node 6 sets `age` to NULL, and
/// only nodes 1, 4 and 6 carry `age`.
const SETUP: &[&str] = &[
    "CREATE (:Person {id: 1, name: 'ann', age: 30})",
    "CREATE (:Person:Employee {id: 2, name: 'bob'})",
    "CREATE (:Account {id: 3, balance: 7.5})",
    "CREATE (:Person:Employee:Manager {id: 4, name: 'dee', age: 41})",
    "CREATE (:Tagged {id: 5, labels: 'stored'})",
    "CREATE (:Person {id: 6, name: 'fay', age: null})",
    "MATCH (a {id: 1}), (b {id: 2}) CREATE (a)-[:T {amount: 10}]->(b)",
    "MATCH (a {id: 2}), (b {id: 3}) CREATE (a)-[:T {amount: 20}]->(b)",
    "MATCH (a {id: 3}), (b {id: 4}) CREATE (a)-[:T {amount: 30}]->(b)",
    "MATCH (a {id: 4}), (b {id: 5}) CREATE (a)-[:T {amount: 40}]->(b)",
    "MATCH (a {id: 2}), (b {id: 6}) CREATE (a)-[:T {amount: 60}]->(b)",
];

/// Distinct stored property columns of the fixture that whole hydration reads.
/// `labels` on node 5 is shadowed by the `labels` field and is not a column.
const WHOLE_COLUMNS: u64 = 4;

type Counters = BTreeMap<String, u64>;

fn fixture() -> (tempfile::TempDir, GraphForge) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("project");
    let forge = GraphForge::new(Some(path.to_str().expect("utf-8 path"))).expect("open project");
    for statement in SETUP {
        forge
            .execute(statement)
            .unwrap_or_else(|e| panic!("{statement}: {e}"));
    }
    (dir, forge)
}

fn run(forge: &GraphForge, query: &str) -> (Result<String, String>, Counters) {
    let (result, snapshot) = demand::capture(|| forge.execute(query));
    let rendered = result
        .map(|r| {
            pretty_format_batches(&r.batches)
                .expect("format batches")
                .to_string()
        })
        .map_err(|e| e.to_string());
    (rendered, snapshot.hydration)
}

fn count(counters: &Counters, name: &str) -> u64 {
    counters
        .get(&format!("hydration_{name}"))
        .copied()
        .unwrap_or(0)
}

/// Where the path starts and how its end is named.
struct Shape {
    pattern: &'static str,
    key: &'static str,
}

const FORWARD: Shape = Shape {
    pattern: "p = (a {id: 1})-[:T*1..4]->(z)",
    key: "z.id",
};
const BACKWARD: Shape = Shape {
    pattern: "p = (z {id: 5})<-[:T*1..4]-(a)",
    key: "a.id",
};

/// The narrowed query and the whole-hydration query for `expr`, where `{L}`
/// stands for the list of path nodes (or relationships).
fn queries(shape: &Shape, source: &str, expr: &str) -> (String, String) {
    let narrow = format!(
        "MATCH {} RETURN {} AS k, {} AS x ORDER BY k",
        shape.pattern,
        shape.key,
        expr.replace("{L}", source),
    );
    let whole = format!(
        "MATCH {} WITH p, a, z, {source} AS ns RETURN {} AS k, {} AS x ORDER BY k",
        shape.pattern,
        shape.key,
        expr.replace("{L}", "ns"),
    );
    (narrow, whole)
}

/// What the narrowed query is expected to read.
#[derive(Debug)]
enum Expect {
    /// No label scan; this many property columns.
    Columns(u64),
    /// The label scan; this many property columns.
    LabelsAndColumns(u64),
    /// The node escapes: the same reads as whole hydration.
    Whole,
}

/// Run `expr` narrowed and whole over path nodes, require identical results,
/// require the baseline to really be whole hydration, and check the narrowed
/// reads against `expect`. Returns the rendered result.
fn check(forge: &GraphForge, shape: &Shape, expr: &str, expect: &Expect) -> String {
    let (narrow_query, whole_query) = queries(shape, "nodes(p)", expr);
    let (narrow, narrow_counters) = run(forge, &narrow_query);
    let (whole, whole_counters) = run(forge, &whole_query);
    let narrow = narrow.unwrap_or_else(|e| panic!("{narrow_query}: {e}"));
    let whole = whole.unwrap_or_else(|e| panic!("{whole_query}: {e}"));
    assert_eq!(
        narrow, whole,
        "{expr}: narrowed result differs from whole hydration"
    );
    assert!(
        count(&whole_counters, "node_batches") > 0
            && count(&whole_counters, "property_columns") == WHOLE_COLUMNS,
        "{expr}: the baseline did not hydrate whole nodes: {whole_counters:?}"
    );
    match expect {
        Expect::Columns(columns) => {
            assert_eq!(
                count(&narrow_counters, "node_batches"),
                0,
                "{expr}: label scan ran: {narrow_counters:?}"
            );
            assert_eq!(
                count(&narrow_counters, "property_columns"),
                *columns,
                "{expr}: {narrow_counters:?}"
            );
        }
        Expect::LabelsAndColumns(columns) => {
            assert!(
                count(&narrow_counters, "node_batches") > 0,
                "{expr}: labels were observed but not scanned: {narrow_counters:?}"
            );
            assert_eq!(
                count(&narrow_counters, "property_columns"),
                *columns,
                "{expr}: {narrow_counters:?}"
            );
        }
        Expect::Whole => assert_eq!(
            narrow_counters, whole_counters,
            "{expr}: an escaping node must hydrate whole"
        ),
    }
    narrow
}

#[test]
fn one_property_read_hydrates_one_column_and_skips_the_label_scan() {
    let (_dir, forge) = fixture();
    let rendered = check(&forge, &FORWARD, "[n IN {L} | n.id]", &Expect::Columns(1));
    assert_eq!(
        normalise(&rendered),
        rows(&[
            ("2", "[1, 2]"),
            ("3", "[1, 2, 3]"),
            ("4", "[1, 2, 3, 4]"),
            ("5", "[1, 2, 3, 4, 5]"),
            ("6", "[1, 2, 6]"),
        ])
    );
}

/// The data rows of a rendered table, without the padding or the header.
fn normalise(table: &str) -> Vec<(String, String)> {
    table
        .lines()
        .filter(|line| line.starts_with('|'))
        .skip(1)
        .map(|line| {
            let cells: Vec<&str> = line.trim_matches('|').split('|').map(str::trim).collect();
            (cells[0].to_string(), cells[1].to_string())
        })
        .collect()
}

fn rows(expected: &[(&str, &str)]) -> Vec<(String, String)> {
    expected
        .iter()
        .map(|(k, x)| ((*k).to_string(), (*x).to_string()))
        .collect()
}

#[test]
fn observed_properties_are_the_only_columns_read() {
    let (_dir, forge) = fixture();
    for (expr, expect) in [
        ("[n IN {L} | n.id + coalesce(n.age, 0)]", Expect::Columns(2)),
        ("[n IN {L} | {i: n.id, a: n.age}]", Expect::Columns(2)),
        (
            "[n IN {L} WHERE n.age IS NOT NULL | n.id]",
            Expect::Columns(2),
        ),
        ("all(n IN {L} WHERE n.id > 0)", Expect::Columns(1)),
        ("any(n IN {L} WHERE n.balance > 5.0)", Expect::Columns(1)),
    ] {
        check(&forge, &FORWARD, expr, &expect);
    }
}

#[test]
fn null_and_missing_properties_match_whole_hydration() {
    let (_dir, forge) = fixture();
    // `age` is absent on most nodes and NULL on node 6.
    let ages = check(&forge, &FORWARD, "[n IN {L} | n.age]", &Expect::Columns(1));
    assert!(ages.contains("30") && ages.contains("41"), "{ages}");
    // `nosuch` is not a property of any node.
    check(
        &forge,
        &FORWARD,
        "[n IN {L} | n.nosuch]",
        &Expect::Columns(0),
    );
    check(
        &forge,
        &FORWARD,
        "[n IN {L} | coalesce(n.nosuch, n.name)]",
        &Expect::Columns(1),
    );
}

#[test]
fn labels_are_scanned_only_when_observed() {
    let (_dir, forge) = fixture();
    for expr in [
        "[n IN {L} | labels(n)]",
        "[n IN {L} | size(labels(n))]",
        "[n IN {L} | n.labels]",
        "any(n IN {L} WHERE 'Manager' IN labels(n))",
    ] {
        // Multi-label nodes: node 2 has two labels, node 4 three. Node 5
        // stores a property named `labels`, which the label field shadows.
        check(&forge, &FORWARD, expr, &Expect::LabelsAndColumns(0));
    }
    check(
        &forge,
        &FORWARD,
        "[n IN {L} WHERE size(labels(n)) > 2 | n.id]",
        &Expect::LabelsAndColumns(1),
    );
    check(
        &forge,
        &FORWARD,
        "[n IN {L} | {id: n.id, labels: labels(n)}]",
        &Expect::LabelsAndColumns(1),
    );
    let labels = check(
        &forge,
        &FORWARD,
        "[n IN {L} | labels(n)]",
        &Expect::LabelsAndColumns(0),
    );
    assert!(labels.contains("Person, Employee, Manager"), "{labels}");
}

#[test]
fn reversed_paths_match_whole_hydration() {
    let (_dir, forge) = fixture();
    let ids = check(&forge, &BACKWARD, "[n IN {L} | n.id]", &Expect::Columns(1));
    assert_eq!(
        normalise(&ids),
        rows(&[
            ("1", "[5, 4, 3, 2, 1]"),
            ("2", "[5, 4, 3, 2]"),
            ("3", "[5, 4, 3]"),
            ("4", "[5, 4]"),
        ])
    );
    check(
        &forge,
        &BACKWARD,
        "[n IN {L} | labels(n)]",
        &Expect::LabelsAndColumns(0),
    );
    check(&forge, &BACKWARD, "[n IN {L} | n.age]", &Expect::Columns(1));
}

#[test]
fn escaping_nodes_hydrate_whole() {
    let (_dir, forge) = fixture();
    for expr in [
        // The path nodes are returned.
        "{L}",
        // The loop variable is the projection, or is kept by a filter.
        "[n IN {L} | n]",
        "[n IN {L} WHERE n.age > 0]",
        // The loop variable is nested inside a larger value.
        "[n IN {L} | {id: n.id, node: n}]",
        "[n IN {L} | [n, n.id]]",
        "[n IN {L} | coalesce(n, {}).id]",
        // A property read beside an escape still needs every column.
        "[n IN {L} | head([n]).age]",
    ] {
        let rendered = check(&forge, &FORWARD, expr, &Expect::Whole);
        assert!(!rendered.is_empty(), "{expr}");
    }
}

#[test]
fn whole_node_value_carries_every_label_and_property() {
    let (_dir, forge) = fixture();
    let rendered = check(&forge, &FORWARD, "{L}", &Expect::Whole);
    for cell in ["Manager", "Account", "balance: 7.5", "name: dee", "age: 41"] {
        assert!(rendered.contains(cell), "{cell} missing from {rendered}");
    }
}

#[test]
fn property_bag_of_a_path_node_is_rejected_the_same_way() {
    // `properties(n)` over a hydrated path node is not a supported form
    // today. Narrowing must not change that: a bare use of the loop variable
    // keeps the whole node, so the outcome is the one whole hydration gives.
    let (_dir, forge) = fixture();
    let (narrow_query, whole_query) = queries(&FORWARD, "nodes(p)", "[n IN {L} | properties(n)]");
    let (narrow, _) = run(&forge, &narrow_query);
    let (whole, _) = run(&forge, &whole_query);
    assert_eq!(narrow, whole);
}

#[test]
fn relationship_lists_match_whole_hydration() {
    let (_dir, forge) = fixture();
    for expr in [
        "[r IN {L} | r.amount]",
        "[r IN {L} WHERE r.amount > 15 | r.amount]",
        "[r IN {L} | type(r)]",
        "{L}",
    ] {
        let (narrow_query, whole_query) = queries(&FORWARD, "relationships(p)", expr);
        let (narrow, _) = run(&forge, &narrow_query);
        let (whole, _) = run(&forge, &whole_query);
        let narrow = narrow.unwrap_or_else(|e| panic!("{narrow_query}: {e}"));
        let whole = whole.unwrap_or_else(|e| panic!("{whole_query}: {e}"));
        assert_eq!(narrow, whole, "{expr}");
    }
    let amounts = run(
        &forge,
        "MATCH p = (a {id: 1})-[:T*1..4]->(z) RETURN z.id AS k, [r IN relationships(p) | r.amount] AS x ORDER BY k",
    )
    .0
    .expect("amounts");
    assert_eq!(
        normalise(&amounts),
        rows(&[
            ("2", "[10]"),
            ("3", "[10, 20]"),
            ("4", "[10, 20, 30]"),
            ("5", "[10, 20, 30, 40]"),
            ("6", "[10, 60]"),
        ])
    );
}
