//! A MATCH path whose unbound node is pinned by an equality with a value the
//! query already has is planned from that node: the node is found by its id
//! once per outer row, and the hop back to the bound node is only checked
//! (#1984, the third MATCH of LDBC SNB Interactive IC7).
//!
//! ```text
//! MATCH (person)<-[:HAS_CREATOR]-(message)
//! WHERE (message:Post OR message:Comment) AND message.id = latestMessageId
//! ```
//!
//! Written that way, the lowering scans the creator's messages and keeps the
//! one with the id; planned from the message it joins the outer rows to the
//! message ids and checks the creator of each match.
//!
//! Each result test pairs the clause as the user wrote it with the same clause
//! written as two MATCH clauses, which the planner leaves in textual order,
//! and asserts both against the rows the graph defines. The rewrite must keep
//! the matches and their multiplicity, the label and the other conjuncts of
//! the WHERE, relationship uniqueness across the whole pattern, the NULL
//! semantics of the equality, and the NULL row of an OPTIONAL MATCH, which the
//! rewrite leaves alone.

use arrow::array::Array;
use arrow::util::display::array_value_to_string;
use graphforge_api::{ExplainStage, GraphForge};

/// One relationship to create: `(from label, from id, type, to label, to id)`.
/// Listing a link twice creates two parallel relationships.
type Link<'a> = (&'a str, i64, &'a str, &'a str, i64);

fn graph(create: &str, links: &[Link]) -> GraphForge {
    let graph = GraphForge::new(None).expect("in-memory instance");
    graph
        .execute(create)
        .unwrap_or_else(|error| panic!("{create}: {error}"));
    for (from, from_id, rel, to, to_id) in links {
        let statement = format!(
            "MATCH (x:{from} {{id: {from_id}}}), (y:{to} {{id: {to_id}}}) \
             CREATE (x)-[:{rel}]->(y)"
        );
        graph
            .execute(&statement)
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

/// Assert that `written` (a clause the planner reads from its pinned node) and
/// `separate` (the same clause as two MATCH clauses that stay in textual
/// order) both return `expected`.
fn assert_same_rows(graph: &GraphForge, written: &str, separate: &str, expected: &[&[&str]]) {
    let expected = table(expected);
    assert_eq!(rows(graph, written), expected, "as written: {written}");
    assert_eq!(
        rows(graph, separate),
        expected,
        "as separate clauses: {separate}"
    );
}

/// LDBC SNB Interactive IC7 in miniature.
///
/// ```text
/// person 1 created post 10, post 11, post 12 (twice: two parallel
///   relationships), comment 20, and is its own creator (a self loop)
/// person 10 created person 1 (an odd graph, but one whose node shares an id
///   with post 10)
/// person 2 created comment 21, person 3 created post 30
/// person 2 has newest message 10 and person 3 newest message 20
/// person 1 knows person 2, and person 3 knows person 1
/// ```
fn messages() -> GraphForge {
    graph(
        "CREATE (:Person {id: 1}), (:Person {id: 2, newest: 10}), (:Person {id: 3, newest: 20}),
                (:Person {id: 10}),
                (:Post {id: 10, length: 12}), (:Post {id: 11, length: 3}),
                (:Post {id: 12, length: 8}), (:Comment {id: 20, length: 7}),
                (:Comment {id: 21, length: 5}), (:Post {id: 30, length: 9})",
        &[
            ("Post", 10, "HAS_CREATOR", "Person", 1),
            ("Post", 11, "HAS_CREATOR", "Person", 1),
            ("Post", 12, "HAS_CREATOR", "Person", 1),
            ("Post", 12, "HAS_CREATOR", "Person", 1),
            ("Comment", 20, "HAS_CREATOR", "Person", 1),
            ("Comment", 21, "HAS_CREATOR", "Person", 2),
            ("Post", 30, "HAS_CREATOR", "Person", 3),
            ("Person", 1, "HAS_CREATOR", "Person", 1),
            ("Person", 10, "HAS_CREATOR", "Person", 1),
            ("Person", 1, "KNOWS", "Person", 2),
            ("Person", 3, "KNOWS", "Person", 1),
        ],
    )
}

const PERSON: &str = "MATCH (person:Person {id: 1})";

/// The clause under test and its separate-clause twin, for a WHERE whose
/// first conjunct pins `message` and whose `rest` follows. `label` is the
/// node's inline label (`""` or `":Post"`).
fn clauses(label: &str, filter: &str) -> (String, String) {
    let written = format!(
        "{PERSON} UNWIND $latest AS latest \
         MATCH (person)<-[:HAS_CREATOR]-(message{label}) WHERE {filter} \
         RETURN latest, labels(message)[0] AS label ORDER BY latest, label"
    );
    let separate = format!(
        "{PERSON} UNWIND $latest AS latest \
         MATCH (message{label}) WHERE {filter} \
         MATCH (person)<-[:HAS_CREATOR]-(message) \
         RETURN latest, labels(message)[0] AS label ORDER BY latest, label"
    );
    (written, separate)
}

/// The same pair with the list of ids spelled out.
fn with_ids(clauses: &(String, String), ids: &str) -> (String, String) {
    (
        clauses.0.replace("$latest", ids),
        clauses.1.replace("$latest", ids),
    )
}

#[test]
fn each_outer_row_finds_the_message_with_its_id_created_by_the_person() {
    let graph = messages();
    let (written, separate) = with_ids(
        &clauses("", "message.id = latest"),
        "[10, 11, 12, 20, 21, 30, 99]",
    );
    // Message 10 is a post and, because the clause names no label, also the
    // person who shares its id. Message 12 has two parallel relationships.
    // Messages 21 and 30 have other creators and 99 does not exist.
    assert_same_rows(
        &graph,
        &written,
        &separate,
        &[
            &["10", "Person"],
            &["10", "Post"],
            &["11", "Post"],
            &["12", "Post"],
            &["12", "Post"],
            &["20", "Comment"],
        ],
    );
}

#[test]
fn the_operands_of_the_equality_may_be_either_way_round() {
    let graph = messages();
    let (written, separate) = with_ids(&clauses(":Post", "latest = message.id"), "[11, 20]");
    assert_same_rows(&graph, &written, &separate, &[&["11", "Post"]]);
}

#[test]
fn an_inline_label_filters_the_pinned_node() {
    let graph = messages();
    let (written, separate) = with_ids(
        &clauses(":Post", "message.id = latest"),
        "[10, 11, 12, 20, 21, 30, 99]",
    );
    assert_same_rows(
        &graph,
        &written,
        &separate,
        &[
            &["10", "Post"],
            &["11", "Post"],
            &["12", "Post"],
            &["12", "Post"],
        ],
    );
}

#[test]
fn a_label_predicate_in_the_where_filters_the_pinned_node() {
    let graph = messages();
    let (written, separate) = with_ids(
        &clauses(
            "",
            "(message:Post OR message:Comment) AND message.id = latest",
        ),
        "[10, 11, 12, 20, 21, 30, 99]",
    );
    assert_same_rows(
        &graph,
        &written,
        &separate,
        &[
            &["10", "Post"],
            &["11", "Post"],
            &["12", "Post"],
            &["12", "Post"],
            &["20", "Comment"],
        ],
    );
}

#[test]
fn repeated_and_null_ids_keep_one_result_per_outer_row() {
    let graph = messages();
    // The equality with NULL is NULL, so that row matches nothing; the id 10
    // twice finds the post twice.
    let (written, separate) = with_ids(
        &clauses(":Post", "message.id = latest"),
        "[10, 10, null, 11]",
    );
    assert_same_rows(
        &graph,
        &written,
        &separate,
        &[&["10", "Post"], &["10", "Post"], &["11", "Post"]],
    );
}

#[test]
fn the_other_conjuncts_still_filter_the_pinned_node_wherever_they_stand() {
    let graph = messages();
    let (written, separate) = with_ids(
        &clauses(
            ":Post",
            "message.length >= 8 AND message.id = latest AND person.id <> message.id",
        ),
        "[10, 11, 12, 30]",
    );
    // Post 11 is too short, post 30 has another creator.
    assert_same_rows(
        &graph,
        &written,
        &separate,
        &[&["10", "Post"], &["12", "Post"], &["12", "Post"]],
    );
}

#[test]
fn a_pinned_node_that_is_the_bound_node_is_found_through_its_own_relationship() {
    let graph = messages();
    // Person 1 is its own creator, so the node with id 1 reaches the bound
    // person through the self loop; as a Post it does not exist.
    let (written, separate) = with_ids(&clauses("", "message.id = latest"), "[1]");
    assert_same_rows(&graph, &written, &separate, &[&["1", "Person"]]);
    let (written, separate) = with_ids(&clauses(":Post", "message.id = latest"), "[1]");
    assert_same_rows(&graph, &written, &separate, &[]);
}

#[test]
fn a_property_of_a_node_bound_before_the_clause_pins_the_node() {
    let graph = messages();
    let written = "MATCH (person:Person {id: 1})-[:KNOWS]-(friend:Person) \
         MATCH (person)<-[:HAS_CREATOR]-(message) WHERE message.id = friend.newest \
         RETURN friend.id AS friend, message.id AS message, labels(message)[0] AS label \
         ORDER BY friend, message, label";
    let separate = "MATCH (person:Person {id: 1})-[:KNOWS]-(friend:Person) \
         MATCH (message) WHERE message.id = friend.newest \
         MATCH (person)<-[:HAS_CREATOR]-(message) \
         RETURN friend.id AS friend, message.id AS message, labels(message)[0] AS label \
         ORDER BY friend, message, label";
    // Friend 2's newest message is 10 (the post and the person), friend 3's
    // is the comment 20.
    assert_same_rows(
        &graph,
        written,
        separate,
        &[
            &["2", "10", "Person"],
            &["2", "10", "Post"],
            &["3", "20", "Comment"],
        ],
    );
}

#[test]
fn the_node_keeps_its_label_for_the_clauses_after_the_pattern() {
    let graph = messages();
    let written = "MATCH (person:Person {id: 1}) UNWIND [10, 20] AS latest \
         MATCH (person)<-[:HAS_CREATOR]-(message:Post) WHERE message.id = latest \
         WITH latest, message MATCH (message)-[:HAS_CREATOR]->(creator) \
         RETURN latest, labels(message)[0] AS label, creator.id AS creator";
    assert_eq!(rows(&graph, written), table(&[&["10", "Post", "1"]]));
}

/// Two relationships of one type in one pattern: the pinned node is the one in
/// the middle, so the check that they are different relationships spans the
/// hop before it and the hop after it.
fn acquaintances(extra: &[Link]) -> GraphForge {
    let mut links = vec![
        ("Person", 1, "KNOWS", "Person", 2),
        ("Person", 2, "KNOWS", "Person", 3),
    ];
    links.extend_from_slice(extra);
    graph(
        "CREATE (:Person {id: 1}), (:Person {id: 2}), (:Person {id: 3})",
        &links,
    )
}

const ACQUAINTED: &str = "MATCH (person:Person {id: 1}) UNWIND [1, 2, 3] AS latest";

fn acquaintance_rows(graph: &GraphForge, expected: &[&[&str]]) {
    let order = "RETURN latest, x.id AS x ORDER BY latest, x";
    let written = format!(
        "{ACQUAINTED} MATCH (person)-[:KNOWS]-(m)-[:KNOWS]-(x) WHERE m.id = latest {order}"
    );
    let separate = format!(
        "{ACQUAINTED} MATCH (m) WHERE m.id = latest \
         MATCH (person)-[:KNOWS]-(m)-[:KNOWS]-(x) {order}"
    );
    assert_same_rows(graph, &written, &separate, expected);
}

#[test]
fn relationship_uniqueness_spans_both_halves_of_the_pattern() {
    // Person 1 reaches person 2 over their only relationship, which the hop
    // out of person 2 must not use again to return to person 1.
    acquaintance_rows(&acquaintances(&[]), &[&["2", "3"]]);
}

#[test]
fn parallel_relationships_are_different_relationships() {
    // With a second relationship between persons 1 and 2, either one can be
    // the first hop and the other the second.
    acquaintance_rows(
        &acquaintances(&[("Person", 1, "KNOWS", "Person", 2)]),
        &[&["2", "1"], &["2", "1"], &["2", "3"], &["2", "3"]],
    );
}

#[test]
fn an_optional_match_keeps_its_null_row() {
    let graph = messages();
    let query = "MATCH (person:Person {id: 1}) UNWIND [11, 21, 99] AS latest \
         OPTIONAL MATCH (person)<-[:HAS_CREATOR]-(message) WHERE message.id = latest \
         RETURN latest, message.id AS message ORDER BY latest";
    assert_eq!(
        rows(&graph, query),
        table(&[&["11", "11"], &["21", "null"], &["99", "null"]])
    );
}

fn physical_plan(graph: &GraphForge, query: &str) -> String {
    graph
        .explain_stage(query, ExplainStage::PhysicalPlan)
        .unwrap_or_else(|error| panic!("{query}: {error}"))
}

/// The clause is planned from the pinned node: the messages are joined to the
/// outer rows by id, the creator is looked up from each message along the
/// relationship, and the person is only compared with it. Read as written the
/// lowering expands from the person to all of its messages and filters them by
/// id, with no join.
#[test]
fn the_pinned_node_is_joined_by_id_and_its_creator_is_looked_up_from_it() {
    let graph = messages();
    let (written, _) = with_ids(
        &clauses(
            "",
            "(message:Post OR message:Comment) AND message.id = latest",
        ),
        "[10, 11]",
    );
    let plan = physical_plan(&graph, &written);
    assert!(
        plan.lines().any(|line| line.contains("HashJoinExec")
            && line.contains("join_type=Inner")
            && line.contains("on=[(id@")),
        "the messages are joined to the outer rows by id: {plan}"
    );
    assert!(
        plan.contains("ExpandExec: rel=HAS_CREATOR, dir=->"),
        "the creator is looked up from the message: {plan}"
    );
    assert!(
        !plan.contains("dir=<-"),
        "the messages of the person are not expanded: {plan}"
    );
}
