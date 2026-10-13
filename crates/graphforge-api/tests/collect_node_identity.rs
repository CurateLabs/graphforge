//! `collect(<node>)` collects only each node's identity when nothing reads more
//! than identity from the list (#1984, the LDBC SNB Interactive IC5
//! `collect(friend)` shape).
//!
//! Every result test runs the query twice: once as written, where the binder
//! narrows the collect, and once with an extra `size([m IN ps | m.id]) >= 0`
//! guard that is always true but reads the elements, which keeps the full node
//! value. The two must return the same rows, and both must return the rows the
//! graph defines. The plan assertions check that the first really collects the
//! bare `node_uuid` and the second the whole node, so neither run is vacuous.

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

/// ```text
/// Persons 1..4 (ann, bob, cat, dan)        Forums 10 alpha, 11 beta, 12 empty
/// KNOWS: 1 -> 2, 2 -> 3, 1 -> 3
/// HAS_MEMBER: 10 -> 1 (5), 10 -> 2 (5), 10 -> 2 (6), 10 -> 3 (5), 11 -> 3 (7)
/// Posts: 100 in forum 10 by 2, 101 in forum 10 by 2, 102 in forum 11 by 3,
///        103 in forum 10 by 4
/// ```
///
/// Bob is a member of alpha twice, so collecting alpha's members repeats him.
fn forums() -> GraphForge {
    graph(&[
        "CREATE (:Person {id: 1, name: 'ann'}), (:Person {id: 2, name: 'bob'}),
                (:Person {id: 3, name: 'cat'}), (:Person {id: 4, name: 'dan'}),
                (:Forum {id: 10, title: 'alpha'}), (:Forum {id: 11, title: 'beta'}),
                (:Forum {id: 12, title: 'empty'}),
                (:Post {id: 100}), (:Post {id: 101}), (:Post {id: 102}), (:Post {id: 103})",
        "MATCH (a:Person {id: 1}), (b:Person {id: 2}) CREATE (a)-[:KNOWS]->(b)",
        "MATCH (a:Person {id: 2}), (b:Person {id: 3}) CREATE (a)-[:KNOWS]->(b)",
        "MATCH (a:Person {id: 1}), (b:Person {id: 3}) CREATE (a)-[:KNOWS]->(b)",
        "MATCH (f:Forum {id: 10}), (p:Person {id: 1}) CREATE (f)-[:HAS_MEMBER {joinDate: 5}]->(p)",
        "MATCH (f:Forum {id: 10}), (p:Person {id: 2}) CREATE (f)-[:HAS_MEMBER {joinDate: 5}]->(p)",
        "MATCH (f:Forum {id: 10}), (p:Person {id: 2}) CREATE (f)-[:HAS_MEMBER {joinDate: 6}]->(p)",
        "MATCH (f:Forum {id: 10}), (p:Person {id: 3}) CREATE (f)-[:HAS_MEMBER {joinDate: 5}]->(p)",
        "MATCH (f:Forum {id: 11}), (p:Person {id: 3}) CREATE (f)-[:HAS_MEMBER {joinDate: 7}]->(p)",
        "MATCH (f:Forum {id: 10}), (x:Post {id: 100}), (p:Person {id: 2})
         CREATE (f)-[:CONTAINER_OF]->(x), (x)-[:HAS_CREATOR]->(p)",
        "MATCH (f:Forum {id: 10}), (x:Post {id: 101}), (p:Person {id: 2})
         CREATE (f)-[:CONTAINER_OF]->(x), (x)-[:HAS_CREATOR]->(p)",
        "MATCH (f:Forum {id: 11}), (x:Post {id: 102}), (p:Person {id: 3})
         CREATE (f)-[:CONTAINER_OF]->(x), (x)-[:HAS_CREATOR]->(p)",
        "MATCH (f:Forum {id: 10}), (x:Post {id: 103}), (p:Person {id: 4})
         CREATE (f)-[:CONTAINER_OF]->(x), (x)-[:HAS_CREATOR]->(p)",
    ])
}

/// A guard that is always true but reads the list's elements, which keeps the
/// collect whole.
const READS_ELEMENTS: &str = "size([m IN ps | m.id]) >= 0";

const MEMBERS: &str = "MATCH (f:Forum)-[:HAS_MEMBER]->(p:Person) WITH f, collect(p) AS ps";

/// The `cypher_collect` aggregate lines of a logical plan.
fn collect_lines(graph: &GraphForge, query: &str) -> Vec<String> {
    graph
        .explain_stage(query, ExplainStage::LogicalPlan)
        .unwrap_or_else(|error| panic!("{query}: {error}"))
        .lines()
        .filter(|line| line.contains("cypher_collect("))
        .map(str::to_owned)
        .collect()
}

/// The logical plan collects bare node identities: no `named_struct`, and the
/// list element type is `FixedSizeBinary(16)` itself, not a struct.
#[track_caller]
fn assert_identity_collect(graph: &GraphForge, query: &str) {
    let lines = collect_lines(graph, query);
    assert!(!lines.is_empty(), "no collect in the plan of {query}");
    for line in &lines {
        assert!(
            !line.contains("named_struct"),
            "collect should hold identities only: {line}\nquery: {query}"
        );
        assert!(
            line.contains("List(FixedSizeBinary(16))"),
            "collect should produce a list of uuids: {line}\nquery: {query}"
        );
        assert!(
            !line.contains("Struct("),
            "no struct in the list type: {line}\nquery: {query}"
        );
    }
}

/// The logical plan collects whole nodes: a struct that keeps the labels next
/// to the `node_uuid`, and never the bare uuid list.
#[track_caller]
fn assert_whole_collect(graph: &GraphForge, query: &str) {
    let lines = collect_lines(graph, query);
    assert!(!lines.is_empty(), "no collect in the plan of {query}");
    for line in &lines {
        assert!(
            line.contains("named_struct"),
            "collect should hold whole nodes: {line}\nquery: {query}"
        );
        assert!(
            line.contains(r#"Struct("node_uuid": FixedSizeBinary(16), "labels": List(Utf8)"#),
            "collect should produce the node struct: {line}\nquery: {query}"
        );
        assert!(
            !line.contains("ps:List(FixedSizeBinary(16))")
                && !line.contains("friends:List(FixedSizeBinary(16))"),
            "collect should not produce a bare uuid list: {line}\nquery: {query}"
        );
    }
}

/// Run `narrowed` (which the binder narrows) and `whole` (the same query kept
/// whole by an element read); both must return `expected`.
#[track_caller]
fn assert_narrowing_is_invisible(
    graph: &GraphForge,
    narrowed: &str,
    whole: &str,
    expected: &[&[&str]],
) {
    assert_identity_collect(graph, narrowed);
    assert_whole_collect(graph, whole);
    let expected = table(expected);
    assert_eq!(rows(graph, narrowed), expected, "narrowed: {narrowed}");
    assert_eq!(rows(graph, whole), expected, "whole: {whole}");
}

/// `rest` follows `WITH f, collect(p) AS ps` over each forum's members.
#[track_caller]
fn assert_members_pair(graph: &GraphForge, rest: &str, expected: &[&[&str]]) {
    assert_narrowing_is_invisible(
        graph,
        &format!("{MEMBERS} {rest}"),
        &format!("{MEMBERS} WHERE {READS_ELEMENTS} {rest}"),
        expected,
    );
}

#[test]
fn the_collected_list_holds_uuids_not_node_structs() {
    let graph = forums();
    let narrowed = format!("{MEMBERS} RETURN f.id AS f, size(ps) AS n");
    let whole = format!("{MEMBERS} RETURN f.id AS f, size(ps) AS n, ps");
    assert_identity_collect(&graph, &narrowed);
    assert_whole_collect(&graph, &whole);

    // The element type is the `node_uuid` column's type, with no struct
    // around it: no labels and no property columns per element.
    let plan = collect_lines(&graph, &narrowed).join("\n");
    assert!(plan.contains("ps:List(FixedSizeBinary(16))"), "{plan}");
    let plan = collect_lines(&graph, &whole).join("\n");
    assert!(
        plan.contains(r#""node_uuid": FixedSizeBinary(16)"#),
        "{plan}"
    );
    assert!(plan.contains(r#""labels": List(Utf8)"#), "{plan}");
}

#[test]
fn size_counts_every_member_including_repeats() {
    let graph = forums();
    // Bob is alpha's member twice, so alpha has four entries over three people.
    assert_members_pair(
        &graph,
        "RETURN f.id AS f, size(ps) AS n ORDER BY f",
        &[&["10", "4"], &["11", "1"]],
    );
}

#[test]
fn count_of_the_list_counts_lists() {
    let graph = forums();
    assert_members_pair(&graph, "RETURN count(ps) AS lists", &[&["2"]]);
}

#[test]
fn membership_finds_every_member_and_only_members() {
    let graph = forums();
    assert_members_pair(
        &graph,
        "MATCH (q:Person) WHERE q IN ps RETURN f.id AS f, q.id AS q ORDER BY f, q",
        &[&["10", "1"], &["10", "2"], &["10", "3"], &["11", "3"]],
    );
}

#[test]
fn negated_membership_finds_every_non_member() {
    let graph = forums();
    assert_members_pair(
        &graph,
        "MATCH (q:Person) WHERE NOT q IN ps RETURN f.id AS f, q.id AS q ORDER BY f, q",
        &[&["10", "4"], &["11", "1"], &["11", "2"], &["11", "4"]],
    );
}

#[test]
fn membership_and_size_share_one_narrowed_list() {
    let graph = forums();
    assert_members_pair(
        &graph,
        "MATCH (q:Person) WHERE q IN ps AND size(ps) > 1 \
         RETURN f.id AS f, q.id AS q, size(ps) AS n ORDER BY f, q",
        &[&["10", "1", "4"], &["10", "2", "4"], &["10", "3", "4"]],
    );
}

#[test]
fn membership_of_an_expand_endpoint_compares_by_identity() {
    let graph = forums();
    // Dan is nobody's friend and a member of nothing; Cat is in both forums.
    assert_members_pair(
        &graph,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE b IN ps \
         RETURN f.id AS f, a.id AS a, b.id AS b ORDER BY f, a, b",
        &[
            &["10", "1", "2"],
            &["10", "1", "3"],
            &["10", "2", "3"],
            &["11", "1", "3"],
            &["11", "2", "3"],
        ],
    );
}

#[test]
fn a_null_node_neither_enters_the_list_nor_matches_it() {
    let graph = forums();
    // The empty forum's OPTIONAL MATCH yields a NULL member: it is skipped by
    // the collect (size 0, not 1), and a membership test against it is NULL.
    let prefix = "MATCH (f:Forum) OPTIONAL MATCH (f)-[:HAS_MEMBER]->(p:Person) \
                  WITH f, collect(p) AS ps";
    let narrowed = format!("{prefix} RETURN f.id AS f, size(ps) AS n ORDER BY f");
    let whole =
        format!("{prefix} WHERE {READS_ELEMENTS} RETURN f.id AS f, size(ps) AS n ORDER BY f");
    assert_narrowing_is_invisible(
        &graph,
        &narrowed,
        &whole,
        &[&["10", "4"], &["11", "1"], &["12", "0"]],
    );

    let narrowed = format!(
        "{prefix} OPTIONAL MATCH (q:Person {{id: 99}}) \
         RETURN f.id AS f, q IN ps AS hit ORDER BY f"
    );
    let whole = format!(
        "{prefix} WHERE {READS_ELEMENTS} OPTIONAL MATCH (q:Person {{id: 99}}) \
         RETURN f.id AS f, q IN ps AS hit ORDER BY f"
    );
    // A NULL left operand makes a membership test over a non-empty list NULL,
    // and over the empty list false (`null IN []` is false in Cypher).
    assert_narrowing_is_invisible(
        &graph,
        &narrowed,
        &whole,
        &[&["10", "null"], &["11", "null"], &["12", "false"]],
    );

    let narrowed = format!(
        "{prefix} MATCH (q:Person) \
         RETURN f.id AS f, q.id AS q, q IN ps AS hit ORDER BY f, q"
    );
    let whole = format!(
        "{prefix} WHERE {READS_ELEMENTS} MATCH (q:Person) \
         RETURN f.id AS f, q.id AS q, q IN ps AS hit ORDER BY f, q"
    );
    // Every person against every forum's list: alpha holds Ann, Bob and Cat
    // (Bob twice), beta holds Cat, and the empty forum's list is empty, so
    // every test against it is false.
    assert_narrowing_is_invisible(
        &graph,
        &narrowed,
        &whole,
        &[
            &["10", "1", "true"],
            &["10", "2", "true"],
            &["10", "3", "true"],
            &["10", "4", "false"],
            &["11", "1", "false"],
            &["11", "2", "false"],
            &["11", "3", "true"],
            &["11", "4", "false"],
            &["12", "1", "false"],
            &["12", "2", "false"],
            &["12", "3", "false"],
            &["12", "4", "false"],
        ],
    );
}

#[test]
fn a_node_renamed_through_with_before_the_collect_still_narrows() {
    let graph = forums();
    let prefix = "MATCH (f:Forum)-[:HAS_MEMBER]->(p0:Person) WITH f, p0 AS p \
                  WITH f, collect(p) AS ps";
    assert_narrowing_is_invisible(
        &graph,
        &format!(
            "{prefix} MATCH (q:Person) WHERE q IN ps \
             RETURN f.id AS f, q.id AS q ORDER BY f, q"
        ),
        &format!(
            "{prefix} WHERE {READS_ELEMENTS} MATCH (q:Person) WHERE q IN ps \
             RETURN f.id AS f, q.id AS q ORDER BY f, q"
        ),
        &[&["10", "1"], &["10", "2"], &["10", "3"], &["11", "3"]],
    );
}

/// The IC5 text, with the list's only uses in the OPTIONAL MATCH's WHERE
/// (identity) plus `extra` (an element read when it is not empty).
fn ic5(extra: &str) -> String {
    format!(
        "MATCH (person:Person {{id: 1}})-[:KNOWS*1..2]-(friend)
         WHERE NOT person = friend
         WITH DISTINCT friend
         MATCH (friend)<-[membership:HAS_MEMBER]-(forum)
         WHERE membership.joinDate > 4
         WITH forum, collect(friend) AS friends
         OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post)<-[:CONTAINER_OF]-(forum)
         WHERE friend IN friends{extra}
         WITH forum, count(post) AS postCount
         RETURN forum.title AS forumName, postCount
         ORDER BY postCount DESC, forum.id ASC
         LIMIT 20"
    )
}

/// The `cypher_collect` lines of the physical plan, for queries whose logical
/// plan needs a dataset snapshot (variable-length expands).
fn physical_collect_lines(graph: &GraphForge, query: &str) -> Vec<String> {
    graph
        .explain_stage(query, ExplainStage::PhysicalPlan)
        .unwrap_or_else(|error| panic!("{query}: {error}"))
        .lines()
        .filter(|line| line.contains("cypher_collect("))
        .map(str::to_owned)
        .collect()
}

#[test]
fn the_interactive_ic5_query_collects_identities() {
    let graph = forums();
    // Ann's friends within two hops are Bob and Cat. Alpha holds Bob's two
    // posts (Dan's post is not a friend's); Beta holds Cat's one.
    let narrowed = ic5("");
    let whole = ic5(" AND size([m IN friends | m.id]) >= 0");

    let lines = physical_collect_lines(&graph, &narrowed);
    assert!(!lines.is_empty(), "no collect in the physical plan");
    for line in &lines {
        assert!(!line.contains("named_struct"), "IC5 narrows: {line}");
    }
    let lines = physical_collect_lines(&graph, &whole);
    assert!(!lines.is_empty(), "no collect in the physical plan");
    for line in &lines {
        assert!(
            line.contains("named_struct"),
            "the element read keeps it whole: {line}"
        );
    }

    let expected = table(&[&["alpha", "2"], &["beta", "1"]]);
    assert_eq!(rows(&graph, &narrowed), expected, "narrowed IC5");
    assert_eq!(rows(&graph, &whole), expected, "whole IC5");
}

#[test]
fn union_branches_narrow_independently() {
    let graph = forums();
    let query = format!(
        "{MEMBERS} RETURN f.id AS f, size(ps) AS n \
         UNION ALL {MEMBERS} UNWIND ps AS m RETURN f.id AS f, m.id AS n"
    );
    let lines = collect_lines(&graph, &query);
    assert_eq!(lines.len(), 2, "one collect per branch: {lines:?}");
    assert!(
        !lines[0].contains("named_struct"),
        "the size branch narrows: {}",
        lines[0]
    );
    assert!(
        lines[1].contains("named_struct"),
        "the UNWIND branch stays whole: {}",
        lines[1]
    );

    let mut got = rows(&graph, &query);
    got.sort();
    // The first branch contributes (10, 4) and (11, 1); the second each member
    // id (bob twice): (10, 1), (10, 2), (10, 2), (10, 3), (11, 3). UNION ALL
    // keeps the duplicate (10, 2).
    let mut expected = table(&[
        &["10", "4"],
        &["11", "1"],
        &["10", "1"],
        &["10", "2"],
        &["10", "2"],
        &["10", "3"],
        &["11", "3"],
    ]);
    expected.sort();
    assert_eq!(got, expected);
}

// ---------------------------------------------------------------------------
// Uses that read more than identity keep the whole node, and still work.
// ---------------------------------------------------------------------------

/// `rest` reads the list beyond identity next to an identity use, so only
/// `rest` can keep the collect whole.
#[track_caller]
fn assert_whole_with(graph: &GraphForge, rest: &str, expected: &[&[&str]]) {
    let query = format!("{MEMBERS} WHERE size(ps) > 0 {rest}");
    assert_whole_collect(graph, &query);
    let mut got = rows(graph, &query);
    got.sort();
    let mut expected = table(expected);
    expected.sort();
    assert_eq!(got, expected, "query: {query}");
}

#[test]
fn unwinding_the_list_keeps_node_properties() {
    let graph = forums();
    assert_whole_with(
        &graph,
        "UNWIND ps AS m RETURN f.id AS f, m.name AS name",
        &[
            &["10", "ann"],
            &["10", "bob"],
            &["10", "bob"],
            &["10", "cat"],
            &["11", "cat"],
        ],
    );
}

#[test]
fn a_list_comprehension_keeps_node_properties() {
    let graph = forums();
    assert_whole_with(
        &graph,
        "RETURN f.id AS f, [m IN ps WHERE m.id > 1 | m.name] AS names",
        &[&["10", "[bob, bob, cat]"], &["11", "[cat]"]],
    );
}

#[test]
fn a_quantifier_keeps_node_properties() {
    let graph = forums();
    // The logical plan of a quantifier over node structs cannot be explained
    // without a dataset snapshot, so the collect is checked in the physical
    // plan; the executed rows still prove the properties are read.
    let query = format!(
        "{MEMBERS} WHERE size(ps) > 0 \
         RETURN f.id AS f, any(m IN ps WHERE m.name = 'ann') AS has_ann"
    );
    let lines = physical_collect_lines(&graph, &query);
    assert!(!lines.is_empty(), "no collect in the physical plan");
    for line in &lines {
        assert!(
            line.contains("named_struct"),
            "the quantifier keeps the whole nodes: {line}"
        );
    }
    let mut got = rows(&graph, &query);
    got.sort();
    assert_eq!(got, table(&[&["10", "true"], &["11", "false"]]));
}

#[test]
fn indexing_the_list_keeps_node_properties() {
    let graph = forums();
    assert_whole_with(
        &graph,
        "RETURN f.id AS f, ps[0].name AS first",
        &[&["10", "ann"], &["11", "cat"]],
    );
}

#[test]
fn returning_the_list_keeps_the_whole_nodes() {
    let graph = forums();
    let query = format!("{MEMBERS} WHERE size(ps) > 0 RETURN f.id AS f, ps");
    assert_whole_collect(&graph, &query);
    let mut got = rows(&graph, &query);
    got.sort();
    assert_eq!(got.len(), 2);
    assert_eq!(got[0][0], "10");
    assert_eq!(got[1][0], "11");
    // The rendered nodes carry their labels, which an identity list could not.
    assert!(got[0][1].contains("Person"), "{}", got[0][1]);
    assert!(got[1][1].contains("Person"), "{}", got[1][1]);
}

#[test]
fn forwarding_the_list_through_with_keeps_the_whole_nodes() {
    let graph = forums();
    assert_whole_with(
        &graph,
        "WITH f, ps AS qs RETURN f.id AS f, size(qs) AS n",
        &[&["10", "4"], &["11", "1"]],
    );
}

#[test]
fn a_list_forwarded_next_to_a_renamed_node_keeps_the_whole_nodes() {
    let graph = forums();
    // `ps` crosses the WITH, which is a read of the list: the membership test
    // after it cannot be proved to be the only use, so the nodes stay whole.
    assert_whole_with(
        &graph,
        "MATCH (q:Person) WITH f, ps, q AS x WHERE x IN ps \
         RETURN f.id AS f, x.id AS x",
        &[&["10", "1"], &["10", "2"], &["10", "3"], &["11", "3"]],
    );
}

#[test]
fn a_membership_test_of_a_property_keeps_the_whole_nodes() {
    let graph = forums();
    // `q.id IN ps` compares an integer with node elements: not a node
    // identity test, so the list stays whole and the test is never true.
    assert_whole_with(
        &graph,
        "MATCH (q:Person) WHERE q.id IN ps RETURN f.id AS f, q.id AS q",
        &[],
    );
}

#[test]
fn a_list_used_by_a_write_keeps_the_whole_nodes() {
    let graph = forums();
    let query =
        format!("{MEMBERS} MATCH (q:Person) WHERE q IN ps AND size(ps) > 1 SET q.seen = true");
    assert_whole_collect(&graph, &query);
    graph.execute(&query).expect("write");
    assert_eq!(
        rows(
            &graph,
            "MATCH (q:Person) WHERE q.seen = true RETURN q.id AS id ORDER BY id"
        ),
        table(&[&["1"], &["2"], &["3"]]),
    );
}
