//! An OPTIONAL MATCH that mentions a node the query already binds expands from
//! that node instead of scanning the pattern's first node and crossing it with
//! every outer row (#1984, the LDBC SNB Interactive IC5 shape).
//!
//! Each result test pairs the pattern as the user wrote it with the same
//! pattern written from the bound node, which the lowering never reorders, and
//! asserts both against the rows the graph defines: a path read right to left
//! matches the same paths with the same multiplicity, an outer row with no
//! match still yields one NULL row, and the clause's WHERE stays part of the
//! match rather than filtering outer rows.

use arrow::array::Array;
use arrow::util::display::array_value_to_string;
use graphforge_api::{ExplainStage, GraphForge};

fn graph(statements: &[&str]) -> GraphForge {
    let graph = GraphForge::new(None).expect("in-memory instance");
    for statement in statements {
        graph
            .execute(statement)
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
    }
    graph
}

fn rows(graph: &GraphForge, query: &str) -> Vec<Vec<String>> {
    let result = graph
        .execute(query)
        .unwrap_or_else(|error| panic!("{query}: {error}"));
    let mut rows = Vec::new();
    for batch in &result.batches {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| {
                        if column
                            .logical_nulls()
                            .is_some_and(|nulls| nulls.is_null(row))
                        {
                            "null".to_owned()
                        } else {
                            array_value_to_string(column, row).expect("render")
                        }
                    })
                    .collect(),
            );
        }
    }
    rows
}

fn table(rows: &[&[&str]]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| row.iter().map(|cell| (*cell).to_owned()).collect())
        .collect()
}

/// Assert that `written` (a pattern the lowering anchors on its bound node)
/// and `from_bound` (the same pattern written from that node) both return
/// `expected`.
fn assert_same_rows(graph: &GraphForge, written: &str, from_bound: &str, expected: &[&[&str]]) {
    let expected = table(expected);
    assert_eq!(rows(graph, written), expected, "as written: {written}");
    assert_eq!(
        rows(graph, from_bound),
        expected,
        "from the bound node: {from_bound}"
    );
}

/// `A` nodes reach `B` nodes over `R`, `B` nodes reach `C` nodes over `S`.
///
/// ```text
/// A1 -R-> B1 -S-> C1        X -R-> B1
/// A1 -R-> B2                A2 -R-> B3 -S-> C1
///                                  B3 -S-> C2        C3 is unreached
/// ```
fn chain() -> GraphForge {
    graph(&[
        "CREATE (:A {id: 1}), (:A {id: 2}), (:X {id: 50}),
                (:B {id: 10}), (:B {id: 11}), (:B {id: 12}),
                (:C {id: 100}), (:C {id: 101}), (:C {id: 102})",
        "MATCH (a:A {id: 1}), (b:B {id: 10}) CREATE (a)-[:R]->(b)",
        "MATCH (a:A {id: 1}), (b:B {id: 11}) CREATE (a)-[:R]->(b)",
        "MATCH (a:A {id: 2}), (b:B {id: 12}) CREATE (a)-[:R]->(b)",
        "MATCH (x:X), (b:B {id: 10}) CREATE (x)-[:R]->(b)",
        "MATCH (b:B {id: 10}), (c:C {id: 100}) CREATE (b)-[:S]->(c)",
        "MATCH (b:B {id: 12}), (c:C {id: 100}) CREATE (b)-[:S]->(c)",
        "MATCH (b:B {id: 12}), (c:C {id: 101}) CREATE (b)-[:S]->(c)",
    ])
}

#[test]
fn a_bound_last_node_keeps_unmatched_rows_labels_and_every_match() {
    let graph = chain();
    // C3 reaches nothing, so it keeps one NULL row. `A` excludes the `X` node
    // that also reaches B1 over R.
    assert_same_rows(
        &graph,
        "MATCH (c:C) OPTIONAL MATCH (a:A)-[:R]->(b)-[:S]->(c)
         RETURN c.id, a.id, b.id ORDER BY c.id, a.id, b.id",
        "MATCH (c:C) OPTIONAL MATCH (c)<-[:S]-(b)<-[:R]-(a:A)
         RETURN c.id, a.id, b.id ORDER BY c.id, a.id, b.id",
        &[
            &["100", "1", "10"],
            &["100", "2", "12"],
            &["101", "2", "12"],
            &["102", "null", "null"],
        ],
    );
}

#[test]
fn an_aggregate_over_an_unmatched_bound_node_counts_zero() {
    let graph = chain();
    assert_same_rows(
        &graph,
        "MATCH (c:C) OPTIONAL MATCH (a:A)-[:R]->(b)-[:S]->(c)
         RETURN c.id, count(a) AS n ORDER BY c.id",
        "MATCH (c:C) OPTIONAL MATCH (c)<-[:S]-(b)<-[:R]-(a:A)
         RETURN c.id, count(a) AS n ORDER BY c.id",
        &[&["100", "2"], &["101", "1"], &["102", "0"]],
    );
}

#[test]
fn parallel_edges_multiply_the_matches() {
    let graph = graph(&[
        "CREATE (:A {id: 1}), (:B {id: 10}), (:C {id: 100})",
        "MATCH (a:A), (b:B) CREATE (a)-[:R]->(b), (a)-[:R]->(b)",
        "MATCH (b:B), (c:C) CREATE (b)-[:S]->(c), (b)-[:S]->(c), (b)-[:S]->(c)",
    ]);
    assert_same_rows(
        &graph,
        "MATCH (c:C) OPTIONAL MATCH (a:A)-[:R]->(b)-[:S]->(c) RETURN c.id, a.id",
        "MATCH (c:C) OPTIONAL MATCH (c)<-[:S]-(b)<-[:R]-(a:A) RETURN c.id, a.id",
        &[
            &["100", "1"],
            &["100", "1"],
            &["100", "1"],
            &["100", "1"],
            &["100", "1"],
            &["100", "1"],
        ],
    );
}

#[test]
fn the_where_clause_belongs_to_the_optional_match() {
    let graph = chain();
    // Matches that fail the WHERE leave a NULL row instead of dropping the
    // outer row: C 100 keeps only the path through A2 (A1's fails the WHERE),
    // C 101's only path passes, and C 102 never matched.
    assert_same_rows(
        &graph,
        "MATCH (c:C) OPTIONAL MATCH (a:A)-[:R]->(b)-[:S]->(c) WHERE a.id > 1 AND b.id < 13
         RETURN c.id, a.id, b.id ORDER BY c.id",
        "MATCH (c:C) OPTIONAL MATCH (c)<-[:S]-(b)<-[:R]-(a:A) WHERE a.id > 1 AND b.id < 13
         RETURN c.id, a.id, b.id ORDER BY c.id",
        &[
            &["100", "2", "12"],
            &["101", "2", "12"],
            &["102", "null", "null"],
        ],
    );
    // A WHERE that no path satisfies still yields every outer row.
    assert_same_rows(
        &graph,
        "MATCH (c:C) OPTIONAL MATCH (a:A)-[:R]->(b)-[:S]->(c) WHERE a.id > 5
         RETURN c.id, a.id ORDER BY c.id",
        "MATCH (c:C) OPTIONAL MATCH (c)<-[:S]-(b)<-[:R]-(a:A) WHERE a.id > 5
         RETURN c.id, a.id ORDER BY c.id",
        &[&["100", "null"], &["101", "null"], &["102", "null"]],
    );
}

#[test]
fn directed_hops_reverse_and_undirected_hops_stay_undirected() {
    let graph = chain();
    // Mixed directions: the first hop points at `b` and the second away from
    // it, so reversing flips both hops. The unlabeled bound node also receives
    // the NULL row, because 102 has no edges.
    assert_same_rows(
        &graph,
        "MATCH (c) WHERE c.id IN [50, 102] OPTIONAL MATCH (a:A)-[:R]->(b)<-[:R]-(c)
         RETURN c.id, a.id, b.id ORDER BY c.id",
        "MATCH (c) WHERE c.id IN [50, 102] OPTIONAL MATCH (c)-[:R]->(b)<-[:R]-(a:A)
         RETURN c.id, a.id, b.id ORDER BY c.id",
        &[&["50", "1", "10"], &["102", "null", "null"]],
    );
    // Undirected hops match either way round.
    assert_same_rows(
        &graph,
        "MATCH (c:C) OPTIONAL MATCH (a)-[:R]-(b)-[:S]-(c)
         RETURN c.id, a.id, b.id ORDER BY c.id, a.id, b.id",
        "MATCH (c:C) OPTIONAL MATCH (c)-[:S]-(b)-[:R]-(a)
         RETURN c.id, a.id, b.id ORDER BY c.id, a.id, b.id",
        &[
            &["100", "1", "10"],
            &["100", "2", "12"],
            &["100", "50", "10"],
            &["101", "2", "12"],
            &["102", "null", "null"],
        ],
    );
    // A single hop that ends at the bound node reverses to start there; the
    // same hop written from the bound node is the unchanged path.
    assert_same_rows(
        &graph,
        "MATCH (a:A) OPTIONAL MATCH (b)<-[:R]-(a) RETURN a.id, b.id ORDER BY a.id, b.id",
        "MATCH (a:A) OPTIONAL MATCH (a)-[:R]->(b) RETURN a.id, b.id ORDER BY a.id, b.id",
        &[&["1", "10"], &["1", "11"], &["2", "12"]],
    );
}

#[test]
fn a_bound_middle_node_joins_both_sides_or_neither() {
    let graph = chain();
    // B2 has an incoming R but no outgoing S: the head matches alone, yet the
    // clause fails as a whole, so B2 keeps one all-NULL row and not a half
    // match. B3 pairs its one R source with both of its S targets. The label on
    // the bound `b` filters, so non-B nodes produce NULLs.
    assert_same_rows(
        &graph,
        "MATCH (b) WHERE b.id >= 10 AND b.id <= 12 OR b.id = 100
         OPTIONAL MATCH (a:A)-[:R]->(b:B)-[:S]->(c:C)
         RETURN b.id, a.id, c.id ORDER BY b.id, a.id, c.id",
        "MATCH (b) WHERE b.id >= 10 AND b.id <= 12 OR b.id = 100
         OPTIONAL MATCH (b:B)<-[:R]-(a:A), (b)-[:S]->(c:C)
         RETURN b.id, a.id, c.id ORDER BY b.id, a.id, c.id",
        &[
            &["10", "1", "100"],
            &["11", "null", "null"],
            &["12", "2", "100"],
            &["12", "2", "101"],
            &["100", "null", "null"],
        ],
    );
}

#[test]
fn two_bound_nodes_constrain_both_ends() {
    let graph = chain();
    assert_same_rows(
        &graph,
        "MATCH (b:B), (c:C) OPTIONAL MATCH (a)-[:R]->(b)-[:S]->(c)
         RETURN b.id, c.id, a.id ORDER BY b.id, c.id, a.id",
        "MATCH (b:B), (c:C) OPTIONAL MATCH (b)<-[:R]-(a), (b)-[:S]->(c)
         RETURN b.id, c.id, a.id ORDER BY b.id, c.id, a.id",
        &[
            &["10", "100", "1"],
            &["10", "100", "50"],
            &["10", "101", "null"],
            &["10", "102", "null"],
            &["11", "100", "null"],
            &["11", "101", "null"],
            &["11", "102", "null"],
            &["12", "100", "2"],
            &["12", "101", "2"],
            &["12", "102", "null"],
        ],
    );
}

#[test]
fn a_variable_length_hop_expands_from_the_bound_node() {
    // S1 reaches E directly and over S1 -> M1 -> M2 -> E, and a stray node
    // reaches nothing.
    let graph = graph(&[
        "CREATE (:Start {id: 1}), (:Mid {id: 2}), (:Mid {id: 3}), (:End {id: 4}), (:End {id: 5})",
        "MATCH (a:Start), (b:Mid {id: 2}) CREATE (a)-[:NEXT]->(b)",
        "MATCH (a:Mid {id: 2}), (b:Mid {id: 3}) CREATE (a)-[:NEXT]->(b)",
        "MATCH (a:Mid {id: 3}), (b:End {id: 4}) CREATE (a)-[:NEXT]->(b)",
        "MATCH (a:Start), (b:End {id: 4}) CREATE (a)-[:NEXT]->(b)",
    ]);
    assert_same_rows(
        &graph,
        "MATCH (z:End) OPTIONAL MATCH (s:Start)-[:NEXT*1..3]->(z)
         RETURN z.id, s.id ORDER BY z.id",
        "MATCH (z:End) OPTIONAL MATCH (z)<-[:NEXT*1..3]-(s:Start)
         RETURN z.id, s.id ORDER BY z.id",
        &[&["4", "1"], &["4", "1"], &["5", "null"]],
    );
    // A var-len hop in the middle of a pattern with a fixed hop either side.
    assert_same_rows(
        &graph,
        "MATCH (z:End {id: 4}) OPTIONAL MATCH (s:Start)-[:NEXT]->(m)-[:NEXT*1..2]->(z)
         RETURN s.id, m.id",
        "MATCH (z:End {id: 4}) OPTIONAL MATCH (z)<-[:NEXT*1..2]-(m)<-[:NEXT]-(s:Start)
         RETURN s.id, m.id",
        &[&["1", "2"]],
    );
}

#[test]
fn a_relationship_is_not_reused_within_the_optional_pattern() {
    let graph = graph(&["CREATE (:P {id: 1})-[:E]->(:Q {id: 2})"]);
    // x - y - z needs two different edges; the only edge cannot serve both.
    assert_same_rows(
        &graph,
        "MATCH (z:P) OPTIONAL MATCH (x)-[]-(y)-[]-(z) RETURN z.id, x.id, y.id",
        "MATCH (z:P) OPTIONAL MATCH (z)-[]-(y)-[]-(x) RETURN z.id, x.id, y.id",
        &[&["1", "null", "null"]],
    );
}

#[test]
fn a_self_loop_matches_once_from_either_end() {
    let graph = graph(&[
        "CREATE (:L {id: 1}), (:L {id: 2})",
        "MATCH (l:L {id: 1}) CREATE (l)-[:LOOP]->(l)",
        "MATCH (l:L {id: 1}), (m:L {id: 2}) CREATE (l)-[:LOOP]->(m)",
    ]);
    // Undirected: the loop matches once, plus the edge to L2 seen from L2.
    let undirected = rows(
        &graph,
        "MATCH (l:L) OPTIONAL MATCH (x)-[:LOOP]-(l) RETURN l.id, x.id ORDER BY l.id, x.id",
    );
    assert_eq!(
        undirected,
        rows(
            &graph,
            "MATCH (l:L) OPTIONAL MATCH (l)-[:LOOP]-(x) RETURN l.id, x.id ORDER BY l.id, x.id",
        ),
    );
    assert_eq!(undirected, table(&[&["1", "1"], &["1", "2"], &["2", "1"]]));
    // Directed: incoming loop edges at each node.
    assert_same_rows(
        &graph,
        "MATCH (l:L) OPTIONAL MATCH (x)-[:LOOP]->(l) RETURN l.id, x.id ORDER BY l.id, x.id",
        "MATCH (l:L) OPTIONAL MATCH (l)<-[:LOOP]-(x) RETURN l.id, x.id ORDER BY l.id, x.id",
        &[&["1", "1"], &["2", "1"]],
    );
}

#[test]
fn a_node_carried_through_with_and_through_an_aggregate_anchors_the_same_way() {
    let graph = chain();
    assert_same_rows(
        &graph,
        "MATCH (c:C) WITH c OPTIONAL MATCH (a:A)-[:R]->(b)-[:S]->(c)
         RETURN c.id, a.id ORDER BY c.id, a.id",
        "MATCH (c:C) WITH c OPTIONAL MATCH (c)<-[:S]-(b)<-[:R]-(a:A)
         RETURN c.id, a.id ORDER BY c.id, a.id",
        &[
            &["100", "1"],
            &["100", "2"],
            &["101", "2"],
            &["102", "null"],
        ],
    );
    // The IC5 shape: a grouping key and a collected list, then a membership
    // test against that list inside the OPTIONAL MATCH.
    assert_same_rows(
        &graph,
        "MATCH (c:C)<-[:S]-(b) WITH c, collect(b) AS bs
         OPTIONAL MATCH (a)-[:R]->(b)-[:S]->(c) WHERE b IN bs
         RETURN c.id, count(a) AS n ORDER BY c.id",
        "MATCH (c:C)<-[:S]-(b) WITH c, collect(b) AS bs
         OPTIONAL MATCH (c)<-[:S]-(b)<-[:R]-(a) WHERE b IN bs
         RETURN c.id, count(a) AS n ORDER BY c.id",
        &[&["100", "3"], &["101", "1"]],
    );
}

#[test]
fn a_named_path_or_named_variable_length_relationship_keeps_its_order() {
    let graph = graph(&[
        "CREATE (:M {id: 1}), (:M {id: 2}), (:M {id: 3})",
        "MATCH (a:M {id: 1}), (b:M {id: 2}) CREATE (a)-[:N {w: 1}]->(b)",
        "MATCH (a:M {id: 2}), (b:M {id: 3}) CREATE (a)-[:N {w: 2}]->(b)",
    ]);
    // The list a named variable-length relationship binds, and the nodes of a
    // named path, are reported in the order the pattern was written. Lowering
    // them from the bound end would reverse both.
    assert_eq!(
        rows(
            &graph,
            "MATCH (z:M {id: 3}) OPTIONAL MATCH (x:M {id: 1})-[r:N*2]->(z)
             RETURN [e IN r | e.w] AS weights",
        ),
        table(&[&["[1, 2]"]]),
    );
    assert_eq!(
        rows(
            &graph,
            "MATCH (z:M {id: 3}) OPTIONAL MATCH p = (x:M)-[:N]->(y)-[:N]->(z)
             RETURN [n IN nodes(p) | n.id] AS ids",
        ),
        table(&[&["[1, 2, 3]"]]),
    );
}

/// The physical plan of `query`, outermost operator first.
fn physical_plan(graph: &GraphForge, query: &str) -> String {
    graph
        .explain_stage(query, ExplainStage::PhysicalPlan)
        .unwrap_or_else(|error| panic!("{query}: {error}"))
}

#[test]
fn the_pattern_expands_from_the_bound_node_instead_of_crossing_a_node_scan() {
    let graph = chain();
    // `c` is the only bound node and ends the pattern. Starting at `a` would
    // scan every `A` node and cross it with the outer rows before expanding.
    let plan = physical_plan(
        &graph,
        "MATCH (c:C) WITH c OPTIONAL MATCH (a:A)-[:R]->(b)-[:S]->(c) RETURN c.id, a.id",
    );
    for crossing in ["CrossJoinExec", "NestedLoopJoinExec"] {
        assert!(
            !plan.contains(crossing),
            "the pattern must not be crossed with a scan ({crossing}):\n{plan}",
        );
    }
    assert_eq!(
        plan.matches("GraphForgeParquetExec").count(),
        1,
        "only the outer MATCH reads a node table:\n{plan}",
    );
    // The plan prints the last operator to run first, so the hop that leaves
    // `c` (S) sits below the hop it feeds (R).
    let (r, s) = (
        plan.find("ExpandExec: rel=R").expect("R expansion"),
        plan.find("ExpandExec: rel=S").expect("S expansion"),
    );
    assert!(
        r < s,
        "S expands from the bound node and R follows:\n{plan}"
    );
}
