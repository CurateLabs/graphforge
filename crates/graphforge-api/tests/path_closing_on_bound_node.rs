//! A MATCH or OPTIONAL MATCH path that ends on a node the query already binds
//! is planned from that node as well as from the path's start, so the hop into
//! the bound node no longer leaves every neighbour of the node before it and
//! then keeps the rows that reach the bound one (#1984, the LDBC SNB
//! Interactive IC10 shape).
//!
//! Each result test pairs the path as the user wrote it with the same path
//! written as separate patterns that the planner leaves in textual order, and
//! asserts both against the rows the graph defines. The rewrite must keep the
//! matches and their multiplicity, the one NULL row of an OPTIONAL MATCH that
//! matches nothing, the clause's WHERE as part of the match, and relationship
//! uniqueness across the whole clause.

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

/// `row` repeated `times` times, for results that multiply one match.
fn repeat(row: &[&str], times: usize) -> Vec<Vec<String>> {
    vec![row.iter().map(|cell| (*cell).to_owned()).collect(); times]
}

/// Assert that `written` (a path the planner closes from its bound end) and
/// `separate` (the same path as patterns that stay in textual order) both
/// return `expected`.
fn assert_same_rows(graph: &GraphForge, written: &str, separate: &str, expected: &[&[&str]]) {
    let expected = table(expected);
    assert_eq!(rows(graph, written), expected, "as written: {written}");
    assert_eq!(
        rows(graph, separate),
        expected,
        "as separate patterns: {separate}"
    );
}

/// LDBC SNB Interactive IC10 in miniature: friends' posts that carry a tag the
/// start person is interested in.
///
/// ```text
/// person 1 is interested in tags 100 and 101
/// friend 2: post 10 (tags 100, 101), post 11 (tag 100), post 12 (tag 102)
/// friend 3: post 13 (tag 102)
/// friend 4: post 14 (tag 100)
/// persons 2, 3 and 5 are interested in tags too, which the bound person's
/// path must not pick up
/// ```
fn interests() -> GraphForge {
    graph(
        "CREATE (:Person {id: 1}), (:Person {id: 2}), (:Person {id: 3}), (:Person {id: 4}),
                (:Person {id: 5}), (:Tag {id: 100}), (:Tag {id: 101}), (:Tag {id: 102}),
                (:Post {id: 10}), (:Post {id: 11}), (:Post {id: 12}), (:Post {id: 13}),
                (:Post {id: 14})",
        &[
            ("Post", 10, "HAS_CREATOR", "Person", 2),
            ("Post", 11, "HAS_CREATOR", "Person", 2),
            ("Post", 12, "HAS_CREATOR", "Person", 2),
            ("Post", 13, "HAS_CREATOR", "Person", 3),
            ("Post", 14, "HAS_CREATOR", "Person", 4),
            ("Post", 10, "HAS_TAG", "Tag", 100),
            ("Post", 10, "HAS_TAG", "Tag", 101),
            ("Post", 11, "HAS_TAG", "Tag", 100),
            ("Post", 12, "HAS_TAG", "Tag", 102),
            ("Post", 13, "HAS_TAG", "Tag", 102),
            ("Post", 14, "HAS_TAG", "Tag", 100),
            ("Person", 1, "HAS_INTEREST", "Tag", 100),
            ("Person", 1, "HAS_INTEREST", "Tag", 101),
            ("Person", 2, "HAS_INTEREST", "Tag", 100),
            ("Person", 3, "HAS_INTEREST", "Tag", 102),
            ("Person", 5, "HAS_INTEREST", "Tag", 100),
            ("Person", 5, "HAS_INTEREST", "Tag", 102),
        ],
    )
}

const FRIENDS: &str = "MATCH (person:Person {id: 1})
     MATCH (friend:Person) WHERE friend.id IN [2, 3, 4]";
const INTEREST_PATH: &str = "(friend)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(:Tag)\
     <-[:HAS_INTEREST]-(person)";
const INTEREST_SEPARATE: &str = "(friend)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(t:Tag), \
     (person)-[:HAS_INTEREST]->(t)";

#[test]
fn an_optional_path_closing_on_the_bound_person_keeps_every_match() {
    let graph = interests();
    // Post 10 reaches person 1 through two tags, so it appears twice; friend 3
    // shares no tag with person 1 and keeps one NULL row.
    assert_same_rows(
        &graph,
        &format!(
            "{FRIENDS} OPTIONAL MATCH {INTEREST_PATH}
             RETURN friend.id, post.id ORDER BY friend.id, post.id"
        ),
        &format!(
            "{FRIENDS} OPTIONAL MATCH {INTEREST_SEPARATE}
             RETURN friend.id, post.id ORDER BY friend.id, post.id"
        ),
        &[
            &["2", "10"],
            &["2", "10"],
            &["2", "11"],
            &["3", "null"],
            &["4", "14"],
        ],
    );
}

#[test]
fn the_common_post_counts_match_the_written_order() {
    let graph = interests();
    // The IC10 aggregate: DISTINCT collapses the doubled post 10, count(post)
    // keeps its multiplicity and skips the NULL row.
    assert_same_rows(
        &graph,
        &format!(
            "{FRIENDS} OPTIONAL MATCH {INTEREST_PATH}
             WITH friend, count(DISTINCT post.id) AS distinct_posts, count(post) AS paths
             RETURN friend.id, distinct_posts, paths ORDER BY friend.id"
        ),
        &format!(
            "{FRIENDS} OPTIONAL MATCH {INTEREST_SEPARATE}
             WITH friend, count(DISTINCT post.id) AS distinct_posts, count(post) AS paths
             RETURN friend.id, distinct_posts, paths ORDER BY friend.id"
        ),
        &[&["2", "2", "3"], &["3", "0", "0"], &["4", "1", "1"]],
    );
}

#[test]
fn a_plain_match_closing_on_the_bound_person_drops_friends_without_a_match() {
    let graph = interests();
    assert_same_rows(
        &graph,
        &format!(
            "{FRIENDS} MATCH {INTEREST_PATH}
             RETURN friend.id, post.id ORDER BY friend.id, post.id"
        ),
        &format!(
            "{FRIENDS} MATCH {INTEREST_SEPARATE}
             RETURN friend.id, post.id ORDER BY friend.id, post.id"
        ),
        &[&["2", "10"], &["2", "10"], &["2", "11"], &["4", "14"]],
    );
}

#[test]
fn the_where_of_an_optional_match_is_part_of_the_match() {
    let graph = interests();
    // Friend 2 still has matches after `post.id > 10` (post 11), friend 4 has
    // post 14, and a friend whose matches all fail the WHERE gets one NULL
    // row instead of disappearing.
    assert_same_rows(
        &graph,
        &format!(
            "{FRIENDS} OPTIONAL MATCH {INTEREST_PATH} WHERE post.id > 10
             RETURN friend.id, post.id ORDER BY friend.id, post.id"
        ),
        &format!(
            "{FRIENDS} OPTIONAL MATCH {INTEREST_SEPARATE} WHERE post.id > 10
             RETURN friend.id, post.id ORDER BY friend.id, post.id"
        ),
        &[&["2", "11"], &["3", "null"], &["4", "14"]],
    );
    assert_same_rows(
        &graph,
        &format!(
            "{FRIENDS} OPTIONAL MATCH {INTEREST_PATH} WHERE post.id > 13
             RETURN friend.id, post.id ORDER BY friend.id, post.id"
        ),
        &format!(
            "{FRIENDS} OPTIONAL MATCH {INTEREST_SEPARATE} WHERE post.id > 13
             RETURN friend.id, post.id ORDER BY friend.id, post.id"
        ),
        &[&["2", "null"], &["3", "null"], &["4", "14"]],
    );
}

#[test]
fn an_outer_row_with_no_match_yields_one_null_row() {
    let graph = interests();
    assert_same_rows(
        &graph,
        "MATCH (person:Person {id: 1}) MATCH (friend:Person {id: 3})
         OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(:Tag)
                        <-[:HAS_INTEREST]-(person)
         RETURN friend.id, post.id",
        "MATCH (person:Person {id: 1}) MATCH (friend:Person {id: 3})
         OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(t:Tag),
                        (person)-[:HAS_INTEREST]->(t)
         RETURN friend.id, post.id",
        &[&["3", "null"]],
    );
    // The same outer row under MATCH has no match at all.
    assert_same_rows(
        &graph,
        "MATCH (person:Person {id: 1}) MATCH (friend:Person {id: 3})
         MATCH (friend)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(:Tag)
               <-[:HAS_INTEREST]-(person)
         RETURN friend.id, post.id",
        "MATCH (person:Person {id: 1}) MATCH (friend:Person {id: 3})
         MATCH (friend)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(t:Tag),
               (person)-[:HAS_INTEREST]->(t)
         RETURN friend.id, post.id",
        &[],
    );
}

/// `a` reaches `m` over `R`, `m` reaches `n` over `Q` and `n` reaches `b` over
/// `S`, so the path to `b` has three hops and closes on `b` at the meeting
/// node `n`:
///
/// ```text
/// m1: two R relationships from a, one Q to n1, three S from n1 to b   (6 paths)
/// m2: one R, one Q to n2, one S                                       (1 path)
/// m3: one R, one Q to n3, no S                                        (0 paths)
/// ```
///
/// `A` node 2 has no relationships at all.
fn multi_edges() -> GraphForge {
    let links = [
        ("A", 1, "R", "M", 1),
        ("A", 1, "R", "M", 1),
        ("M", 1, "Q", "N", 1),
        ("N", 1, "S", "B", 1),
        ("N", 1, "S", "B", 1),
        ("N", 1, "S", "B", 1),
        ("A", 1, "R", "M", 2),
        ("M", 2, "Q", "N", 2),
        ("N", 2, "S", "B", 1),
        ("A", 1, "R", "M", 3),
        ("M", 3, "Q", "N", 3),
    ];
    graph(
        "CREATE (:A {id: 1}), (:A {id: 2}), (:B {id: 1}),
                (:M {id: 1}), (:M {id: 2}), (:M {id: 3}),
                (:N {id: 1}), (:N {id: 2}), (:N {id: 3})",
        &links,
    )
}

/// The three-hop path of [`multi_edges`] as written, and the same path as the
/// separate patterns that the planner leaves in textual order.
const MULTI_PATH: &str = "(a)-[:R]->(m)-[:Q]->(n)-[:S]->(b)";
const MULTI_SEPARATE: &str = "(a)-[:R]->(m)-[:Q]->(n), (n)-[:S]->(b)";

#[test]
fn parallel_relationships_multiply_the_matches_of_the_closing_path() {
    let graph = multi_edges();
    let six_and_one: &[&[&str]] = &[&["1"], &["1"], &["1"], &["1"], &["1"], &["1"], &["2"]];
    let outer = "MATCH (a:A {id: 1}), (b:B)";
    for clause in ["MATCH", "OPTIONAL MATCH"] {
        assert_same_rows(
            &graph,
            &format!("{outer} {clause} {MULTI_PATH} RETURN m.id ORDER BY m.id"),
            &format!("{outer} {clause} {MULTI_SEPARATE} RETURN m.id ORDER BY m.id"),
            six_and_one,
        );
    }
    // The path read from the other end closes on `a` instead.
    assert_same_rows(
        &graph,
        &format!("{outer} MATCH (b)<-[:S]-(n)<-[:Q]-(m)<-[:R]-(a) RETURN count(*)"),
        &format!("{outer} MATCH (b)<-[:S]-(n)<-[:Q]-(m), (m)<-[:R]-(a) RETURN count(*)"),
        &[&["7"]],
    );
}

#[test]
fn every_outer_row_keeps_its_own_matches_or_one_null_row() {
    let graph = multi_edges();
    assert_same_rows(
        &graph,
        &format!(
            "MATCH (a:A), (b:B) OPTIONAL MATCH {MULTI_PATH}
             RETURN a.id, count(m) AS paths ORDER BY a.id"
        ),
        &format!(
            "MATCH (a:A), (b:B) OPTIONAL MATCH {MULTI_SEPARATE}
             RETURN a.id, count(m) AS paths ORDER BY a.id"
        ),
        &[&["1", "7"], &["2", "0"]],
    );
    assert_same_rows(
        &graph,
        &format!(
            "MATCH (a:A), (b:B) OPTIONAL MATCH {MULTI_PATH}
             WITH a, m WHERE m IS NULL RETURN a.id"
        ),
        &format!(
            "MATCH (a:A), (b:B) OPTIONAL MATCH {MULTI_SEPARATE}
             WITH a, m WHERE m IS NULL RETURN a.id"
        ),
        &[&["2"]],
    );
}

/// `a` and `b` are joined through three routes `m -Q-> n` whose first and last
/// relationships point in different directions:
///
/// ```text
/// route 1: a -R-> m1 -Q-> n1 -S-> b
/// route 2: a <-R- m2 -Q-> n2 <-S- b
/// route 3: a <-R- m3 -Q-> n3 -S-> b
/// ```
fn directions() -> GraphForge {
    graph(
        "CREATE (:A {id: 1}), (:B {id: 1}), (:M {id: 1}), (:M {id: 2}), (:M {id: 3}),
                (:N {id: 1}), (:N {id: 2}), (:N {id: 3})",
        &[
            ("A", 1, "R", "M", 1),
            ("M", 1, "Q", "N", 1),
            ("N", 1, "S", "B", 1),
            ("M", 2, "R", "A", 1),
            ("M", 2, "Q", "N", 2),
            ("B", 1, "S", "N", 2),
            ("M", 3, "R", "A", 1),
            ("M", 3, "Q", "N", 3),
            ("N", 3, "S", "B", 1),
        ],
    )
}

/// Assert that the path `(a){first}(m)-[:Q]->(n){second}(b)` returns `expected`
/// first-half nodes under MATCH, and the same rows (or one NULL row) under
/// OPTIONAL MATCH, both as written and as two separate patterns.
fn assert_directed_path(graph: &GraphForge, first: &str, second: &str, expected: &[&[&str]]) {
    let outer = "MATCH (a:A {id: 1}), (b:B {id: 1})";
    let written = format!("(a){first}(m)-[:Q]->(n){second}(b)");
    let separate = format!("(a){first}(m)-[:Q]->(n), (n){second}(b)");
    assert_same_rows(
        graph,
        &format!("{outer} MATCH {written} RETURN m.id ORDER BY m.id"),
        &format!("{outer} MATCH {separate} RETURN m.id ORDER BY m.id"),
        expected,
    );
    let optional: &[&[&str]] = if expected.is_empty() {
        &[&["null"]]
    } else {
        expected
    };
    assert_same_rows(
        graph,
        &format!("{outer} OPTIONAL MATCH {written} RETURN m.id ORDER BY m.id"),
        &format!("{outer} OPTIONAL MATCH {separate} RETURN m.id ORDER BY m.id"),
        optional,
    );
}

#[test]
fn directed_closing_paths_follow_each_relationship_direction() {
    let graph = directions();
    assert_directed_path(&graph, "-[:R]->", "-[:S]->", &[&["1"]]);
    assert_directed_path(&graph, "<-[:R]-", "<-[:S]-", &[&["2"]]);
    assert_directed_path(&graph, "<-[:R]-", "-[:S]->", &[&["3"]]);
    assert_directed_path(&graph, "-[:R]->", "<-[:S]-", &[]);
}

#[test]
fn an_undirected_closing_path_matches_every_orientation_once() {
    let graph = directions();
    assert_directed_path(&graph, "-[:R]-", "-[:S]-", &[&["1"], &["2"], &["3"]]);
    assert_directed_path(&graph, "-[:R]-", "-[:T]-", &[]);
}

/// Nodes whose relationships return to them:
///
/// ```text
/// 1 -> 2 -> 3 -> 1   a triangle
/// 4                  three self-loops
/// 5                  two self-loops
/// 6 -> 7             no way back
/// ```
fn cycles() -> GraphForge {
    graph(
        "CREATE (:Loop {id: 1}), (:Loop {id: 2}), (:Loop {id: 3}), (:Loop {id: 4}),
                (:Loop {id: 5}), (:Loop {id: 6}), (:Loop {id: 7})",
        &[
            ("Loop", 1, "R", "Loop", 2),
            ("Loop", 2, "R", "Loop", 3),
            ("Loop", 3, "R", "Loop", 1),
            ("Loop", 4, "R", "Loop", 4),
            ("Loop", 4, "R", "Loop", 4),
            ("Loop", 4, "R", "Loop", 4),
            ("Loop", 5, "R", "Loop", 5),
            ("Loop", 5, "R", "Loop", 5),
            ("Loop", 6, "R", "Loop", 7),
        ],
    )
}

const WALK: &str = "(a)-[:R]->(m)-[:R]->(n)-[:R]->(a)";
const WALK_SEPARATE: &str = "(a)-[:R]->(m)-[:R]->(n), (n)-[:R]->(a)";

#[test]
fn a_path_may_close_on_its_own_start_node() {
    let graph = cycles();
    // The triangle returns to each of its nodes along one trail. The three
    // self-loops of node 4 are walked in every order, and the two of node 5
    // cannot make three distinct hops.
    assert_same_rows(
        &graph,
        &format!(
            "MATCH (a:Loop) OPTIONAL MATCH {WALK}
             RETURN a.id, count(m) AS trails, count(*) AS result_rows ORDER BY a.id"
        ),
        &format!(
            "MATCH (a:Loop) OPTIONAL MATCH {WALK_SEPARATE}
             RETURN a.id, count(m) AS trails, count(*) AS result_rows ORDER BY a.id"
        ),
        &[
            &["1", "1", "1"],
            &["2", "1", "1"],
            &["3", "1", "1"],
            &["4", "6", "6"],
            &["5", "0", "1"],
            &["6", "0", "1"],
            &["7", "0", "1"],
        ],
    );
    assert_same_rows(
        &graph,
        &format!("MATCH (a:Loop) MATCH {WALK} RETURN a.id, count(*) AS trails ORDER BY a.id"),
        &format!(
            "MATCH (a:Loop) MATCH {WALK_SEPARATE} RETURN a.id, count(*) AS trails ORDER BY a.id"
        ),
        &[&["1", "1"], &["2", "1"], &["3", "1"], &["4", "6"]],
    );
}

#[test]
fn two_variables_bound_to_one_node_close_a_path_like_one_variable() {
    let graph = cycles();
    for (id, expected) in [
        (1, table(&[&["2", "3"]])),
        (4, repeat(&["4", "4"], 6)),
        (5, table(&[&["null", "null"]])),
    ] {
        let written = format!(
            "MATCH (a:Loop {{id: {id}}}), (b:Loop {{id: {id}}})
             OPTIONAL MATCH (a)-[:R]->(m)-[:R]->(n)-[:R]->(b) RETURN m.id, n.id"
        );
        let separate = format!(
            "MATCH (a:Loop {{id: {id}}}), (b:Loop {{id: {id}}})
             OPTIONAL MATCH (a)-[:R]->(m)-[:R]->(n), (n)-[:R]->(b) RETURN m.id, n.id"
        );
        assert_eq!(rows(&graph, &written), expected, "{written}");
        assert_eq!(rows(&graph, &separate), expected, "{separate}");
    }
}

#[test]
fn a_directed_closing_hop_cannot_walk_the_relationship_of_the_hop_before_it() {
    // Node 1 reaches node 2 over one relationship, and node 3 reaches node 2
    // over `parallel` relationships. The path 1 -> 2 <- 3 -> 2 walks two of
    // the relationships from 3 to 2, the second hop in the first half and the
    // closing hop in the other, so one relationship matches nothing.
    let create = "CREATE (:N {id: 1}), (:N {id: 2}), (:N {id: 3})";
    let path = "(a)-[:E]->(m)<-[:E]-(n)-[:E]->(b)";
    let separate = "(a)-[:E]->(m)<-[:E]-(n), (n)-[:E]->(b)";
    let outer = "MATCH (a:N {id: 1}), (b:N {id: 2})";
    for (parallel, optional, plain) in [
        (1, table(&[&["null", "null"]]), table(&[])),
        (2, repeat(&["2", "3"], 2), repeat(&["2", "3"], 2)),
    ] {
        let mut links = vec![("N", 1, "E", "N", 2)];
        links.extend(vec![("N", 3, "E", "N", 2); parallel]);
        let database = graph(create, &links);
        for pattern in [path, separate] {
            let query = format!("{outer} OPTIONAL MATCH {pattern} RETURN m.id, n.id");
            assert_eq!(
                rows(&database, &query),
                optional,
                "{parallel} parallel: {query}"
            );
            let query = format!("{outer} MATCH {pattern} RETURN m.id, n.id");
            assert_eq!(
                rows(&database, &query),
                plain,
                "{parallel} parallel: {query}"
            );
        }
    }
}

#[test]
fn a_relationship_of_the_first_half_is_not_reused_by_the_closing_hop_of_a_longer_path() {
    // Trails of three relationships from node 1 to node 2. With one
    // relationship between 2 and 3 the only walks reuse a relationship; with
    // a second, parallel one it can be walked out and back on the other.
    let links = |parallel: usize| {
        let mut links = vec![("N", 1, "E", "N", 2)];
        links.push(("N", 2, "E", "N", 3));
        links.extend(vec![("N", 3, "E", "N", 2); parallel]);
        links
    };
    let query = |path: &str| {
        format!("MATCH (a:N {{id: 1}}), (b:N {{id: 2}}) OPTIONAL MATCH {path} RETURN m.id, n.id")
    };
    let create = "CREATE (:N {id: 1}), (:N {id: 2}), (:N {id: 3})";
    let without = graph(create, &links(0));
    let with = graph(create, &links(1));
    let path = "(a)-[:E]-(m)-[:E]-(n)-[:E]-(b)";
    let separate = "(a)-[:E]-(m)-[:E]-(n), (n)-[:E]-(b)";
    let no_trail = table(&[&["null", "null"]]);
    let two_trails = table(&[&["2", "3"], &["2", "3"]]);
    assert_eq!(rows(&without, &query(path)), no_trail);
    assert_eq!(rows(&without, &query(separate)), no_trail);
    assert_eq!(rows(&with, &query(path)), two_trails);
    assert_eq!(rows(&with, &query(separate)), two_trails);
}

#[test]
fn the_closing_hop_is_unique_against_the_first_half_where_their_types_overlap() {
    // Relationships of different declared types can never be the same stored
    // relationship, so only hops whose types overlap compete. The closing hop
    // (`E`) skips the middle hop (`F`) and still competes with the first hop
    // (`E`), which sits in the other half of the split path.
    let create = "CREATE (:N {id: 1}), (:N {id: 2})";
    let one_e = graph(create, &[("N", 1, "E", "N", 2), ("N", 2, "F", "N", 1)]);
    let two_e = graph(
        create,
        &[
            ("N", 1, "E", "N", 2),
            ("N", 1, "E", "N", 2),
            ("N", 2, "F", "N", 1),
        ],
    );
    let query = |path: &str| {
        format!("MATCH (a:N {{id: 1}}), (b:N {{id: 2}}) OPTIONAL MATCH {path} RETURN m.id, n.id")
    };
    let path = "(a)-[:E]-(m)-[:F]-(n)-[:E]-(b)";
    let separate = "(a)-[:E]-(m)-[:F]-(n), (n)-[:E]-(b)";
    // One `E` cannot be both the first and the last hop; two can, in either
    // order.
    let no_trail = table(&[&["null", "null"]]);
    let two_trails = table(&[&["2", "1"], &["2", "1"]]);
    assert_eq!(rows(&one_e, &query(path)), no_trail);
    assert_eq!(rows(&one_e, &query(separate)), no_trail);
    assert_eq!(rows(&two_e, &query(path)), two_trails);
    assert_eq!(rows(&two_e, &query(separate)), two_trails);
    let plain = |path: &str| {
        format!("MATCH (a:N {{id: 1}}), (b:N {{id: 2}}) MATCH {path} RETURN m.id, n.id")
    };
    assert!(rows(&one_e, &plain(path)).is_empty());
    assert_eq!(rows(&two_e, &plain(path)), two_trails);
}

#[test]
fn hops_of_disjoint_types_may_walk_the_same_pair_of_nodes_in_both_halves() {
    // One `E` and two `F` relationships join nodes 1 and 2. Disjoint types
    // never share a relationship, so `E` then `F` then `F` takes the two `F`
    // relationships in either order. A closing hop that allows `E` competes
    // with the first `E` hop, so it can only take the `F` the middle hop left.
    // With every type allowed the three relationships fill the hops in any
    // order.
    let create = "CREATE (:N {id: 1}), (:N {id: 2})";
    let database = graph(
        create,
        &[
            ("N", 1, "E", "N", 2),
            ("N", 1, "F", "N", 2),
            ("N", 1, "F", "N", 2),
        ],
    );
    let outer = "MATCH (a:N {id: 1}), (b:N {id: 2}) OPTIONAL MATCH";
    for (path, separate, expected) in [
        (
            "(a)-[:E]-(m)-[:F]-(n)-[:F]-(b)",
            "(a)-[:E]-(m)-[:F]-(n), (n)-[:F]-(b)",
            repeat(&["2", "1"], 2),
        ),
        (
            "(a)-[:E]-(m)-[:F]-(n)-[:E|F]-(b)",
            "(a)-[:E]-(m)-[:F]-(n), (n)-[:E|F]-(b)",
            repeat(&["2", "1"], 2),
        ),
        (
            "(a)-[:E|F]-(m)-[:E|F]-(n)-[:E|F]-(b)",
            "(a)-[:E|F]-(m)-[:E|F]-(n), (n)-[:E|F]-(b)",
            repeat(&["2", "1"], 6),
        ),
    ] {
        assert_eq!(
            rows(&database, &format!("{outer} {path} RETURN m.id, n.id")),
            expected,
            "as written: {path}"
        );
        assert_eq!(
            rows(&database, &format!("{outer} {separate} RETURN m.id, n.id")),
            expected,
            "as separate patterns: {separate}"
        );
    }
}

/// `a` reaches `b` over three-hop routes: two `R`-`S`-`V` routes through
/// distinct nodes and one `T`-`W`-`U` route.
fn two_routes() -> GraphForge {
    graph(
        "CREATE (:A {id: 1}), (:B {id: 1}), (:M {id: 1}), (:M {id: 2}), (:M {id: 7}),
                (:N {id: 1}), (:N {id: 2}), (:N {id: 7})",
        &[
            ("A", 1, "R", "M", 1),
            ("M", 1, "S", "N", 1),
            ("N", 1, "V", "B", 1),
            ("A", 1, "R", "M", 2),
            ("M", 2, "S", "N", 2),
            ("N", 2, "V", "B", 1),
            ("A", 1, "T", "M", 7),
            ("M", 7, "W", "N", 7),
            ("N", 7, "U", "B", 1),
        ],
    )
}

#[test]
fn two_patterns_of_one_clause_close_on_distinct_anonymous_meeting_nodes() {
    let graph = two_routes();
    let outer = "MATCH (a:A), (b:B) OPTIONAL MATCH";
    // The routes multiply: two R-S-V paths times one T-W-U path.
    assert_same_rows(
        &graph,
        &format!(
            "{outer} (a)-[:R]->(m)-[:S]->()-[:V]->(b), (a)-[:T]->(n)-[:W]->()-[:U]->(b)
             RETURN m.id, n.id ORDER BY m.id"
        ),
        &format!(
            "{outer} (a)-[:R]->(m)-[:S]->(p), (p)-[:V]->(b), (a)-[:T]->(n)-[:W]->(q), (q)-[:U]->(b)
             RETURN m.id, n.id ORDER BY m.id"
        ),
        &[&["1", "7"], &["2", "7"]],
    );
    assert_same_rows(
        &graph,
        &format!(
            "{outer} (a)-[:R]->()-[:S]->()-[:V]->(b), (a)-[:T]->()-[:W]->()-[:U]->(b)
             RETURN count(*)"
        ),
        &format!(
            "{outer} (a)-[:R]->(m)-[:S]->(p), (p)-[:V]->(b), (a)-[:T]->(n)-[:W]->(q), (q)-[:U]->(b)
             RETURN count(*)"
        ),
        &[&["2"]],
    );
    // Repeating a route in one clause cannot reuse its relationships: the two
    // R-S-V paths pair up in both orders and never with themselves.
    assert_same_rows(
        &graph,
        &format!(
            "{outer} (a)-[:R]->()-[:S]->()-[:V]->(b), (a)-[:R]->()-[:S]->()-[:V]->(b)
             RETURN count(*)"
        ),
        &format!(
            "{outer} (a)-[:R]->(m)-[:S]->(p), (p)-[:V]->(b), (a)-[:R]->(n)-[:S]->(q), (q)-[:V]->(b)
             RETURN count(*)"
        ),
        &[&["2"]],
    );
}

#[test]
fn return_star_does_not_report_an_anonymous_meeting_node() {
    let graph = two_routes();
    let result = graph
        .execute("MATCH (a:A), (b:B) MATCH (a)-[:R]->()-[:S]->()-[:V]->(b) RETURN *")
        .expect("return star");
    let mut columns: Vec<String> = result.batches[0]
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    columns.sort();
    assert_eq!(columns, ["a", "b"]);
}

/// `a` reaches `b` through `x` and then through three meeting nodes that
/// differ in what they carry:
///
/// ```text
/// m1: label Mid, ok = true
/// m2: label Mid, ok = false
/// m3: label Other, ok = true
/// ```
///
/// `b` has `tag = 'x'` and two outgoing `T` relationships.
fn labelled_meeting_nodes() -> GraphForge {
    graph(
        "CREATE (:A {id: 1}), (:X {id: 1}), (:B {id: 1, tag: 'x'}), (:C {id: 1}), (:C {id: 2}),
                (:Mid {id: 1, ok: true}), (:Mid {id: 2, ok: false}), (:Other {id: 3, ok: true})",
        &[
            ("A", 1, "R", "X", 1),
            ("X", 1, "S", "Mid", 1),
            ("X", 1, "S", "Mid", 2),
            ("X", 1, "S", "Other", 3),
            ("Mid", 1, "V", "B", 1),
            ("Mid", 2, "V", "B", 1),
            ("Other", 3, "V", "B", 1),
            ("B", 1, "T", "C", 1),
            ("B", 1, "T", "C", 2),
        ],
    )
}

#[test]
fn the_meeting_node_keeps_its_labels_and_properties() {
    let graph = labelled_meeting_nodes();
    let outer = "MATCH (a:A), (b:B)";
    assert_same_rows(
        &graph,
        &format!("{outer} MATCH (a)-[:R]->()-[:S]->(m:Mid {{ok: true}})-[:V]->(b) RETURN m.id"),
        &format!(
            "{outer} MATCH (a)-[:R]->()-[:S]->(m:Mid {{ok: true}}), (m)-[:V]->(b) RETURN m.id"
        ),
        &[&["1"]],
    );
    assert_same_rows(
        &graph,
        &format!("{outer} MATCH (a)-[:R]->()-[:S]->(:Mid {{ok: true}})-[:V]->(b) RETURN count(*)"),
        &format!(
            "{outer} MATCH (a)-[:R]->()-[:S]->(m:Mid {{ok: true}}), (m)-[:V]->(b) RETURN count(*)"
        ),
        &[&["1"]],
    );
    assert_same_rows(
        &graph,
        &format!("{outer} OPTIONAL MATCH (a)-[:R]->()-[:S]->(m:Missing)-[:V]->(b) RETURN m.id"),
        &format!(
            "{outer} OPTIONAL MATCH (a)-[:R]->()-[:S]->(m:Missing), (m)-[:V]->(b) RETURN m.id"
        ),
        &[&["null"]],
    );
}

#[test]
fn the_closing_node_keeps_its_properties_and_what_follows_it() {
    let graph = labelled_meeting_nodes();
    let outer = "MATCH (a:A) MATCH (b:B)";
    // The closing node's own property filters the path, and the relationships
    // after it continue from the node the path closed on.
    assert_same_rows(
        &graph,
        &format!(
            "{outer} OPTIONAL MATCH (a)-[:R]->()-[:S]->(:Mid)-[:V]->(b {{tag: 'x'}})-[:T]->(c)
             RETURN c.id ORDER BY c.id"
        ),
        &format!(
            "{outer} OPTIONAL MATCH (a)-[:R]->()-[:S]->(m:Mid), (m)-[:V]->(b {{tag: 'x'}}), (b)-[:T]->(c)
             RETURN c.id ORDER BY c.id"
        ),
        &[&["1"], &["1"], &["2"], &["2"]],
    );
    assert_same_rows(
        &graph,
        &format!(
            "{outer} OPTIONAL MATCH (a)-[:R]->()-[:S]->(:Mid)-[:V]->(b {{tag: 'y'}})-[:T]->(c)
             RETURN c.id ORDER BY c.id"
        ),
        &format!(
            "{outer} OPTIONAL MATCH (a)-[:R]->()-[:S]->(m:Mid), (m)-[:V]->(b {{tag: 'y'}}), (b)-[:T]->(c)
             RETURN c.id ORDER BY c.id"
        ),
        &[&["null"]],
    );
}

#[test]
fn a_null_bound_node_never_closes_a_path() {
    let graph = labelled_meeting_nodes();
    assert_same_rows(
        &graph,
        "MATCH (a:A) OPTIONAL MATCH (b:Nothing)
         OPTIONAL MATCH (a)-[:R]->()-[:S]->(m)-[:V]->(b) RETURN a.id, b.id, m.id",
        "MATCH (a:A) OPTIONAL MATCH (b:Nothing)
         OPTIONAL MATCH (a)-[:R]->()-[:S]->(m), (m)-[:V]->(b) RETURN a.id, b.id, m.id",
        &[&["1", "null", "null"]],
    );
}

/// The physical plan of `query`, outermost operator first.
fn physical_plan(graph: &GraphForge, query: &str) -> String {
    graph
        .explain_stage(query, ExplainStage::PhysicalPlan)
        .unwrap_or_else(|error| panic!("{query}: {error}"))
}

/// The `ExpandExec` lines of `plan` for one relationship type.
fn expansions<'a>(plan: &'a str, rel: &str) -> Vec<&'a str> {
    let marker = format!("ExpandExec: rel={rel},");
    plan.lines()
        .map(str::trim)
        .filter(|line| line.starts_with(&marker))
        .collect()
}

#[test]
fn the_closing_hop_expands_from_the_bound_person_instead_of_from_every_tag() {
    let graph = interests();
    let suffix = "WITH friend, count(DISTINCT post.id) AS c RETURN friend.id, c";
    let written = physical_plan(
        &graph,
        &format!("{FRIENDS} OPTIONAL MATCH {INTEREST_PATH} {suffix}"),
    );
    let separate = physical_plan(
        &graph,
        &format!("{FRIENDS} OPTIONAL MATCH {INTEREST_SEPARATE} {suffix}"),
    );

    // `(tag)<-[:HAS_INTEREST]-(person)` read as written leaves a tag for every
    // interested person (`dir=<-`) and filters them to the bound person. The
    // closing hop is instead expanded once, from the bound person (`dir=->`),
    // and joined to the tag the first half reached.
    assert_eq!(
        expansions(&written, "HAS_INTEREST").len(),
        1,
        "one HAS_INTEREST expansion\n{written}"
    );
    assert!(
        expansions(&written, "HAS_INTEREST")[0]
            .starts_with("ExpandExec: rel=HAS_INTEREST, dir=->,"),
        "the closing hop starts at the bound person\n{written}"
    );
    assert!(
        !written.contains("NestedLoopJoinExec"),
        "the halves meet on the tag's identity, not through a nested loop\n{written}"
    );

    // The path as written plans like the two patterns written out by hand.
    assert_eq!(
        expansions(&written, "HAS_INTEREST"),
        expansions(&separate, "HAS_INTEREST")
    );
    assert_eq!(
        expansions(&written, "HAS_CREATOR"),
        expansions(&separate, "HAS_CREATOR")
    );
    assert_eq!(
        expansions(&written, "HAS_TAG"),
        expansions(&separate, "HAS_TAG")
    );
}

/// LDBC SNB Interactive IC7 in miniature: who liked the messages of person 1.
///
/// ```text
/// messages 10 and 11 are by person 1, message 12 is by person 4
/// person 2 likes message 10 twice (at 100 and at 200) and message 11 (at 100)
/// person 3 likes message 12; person 4 likes nothing
/// ```
fn liked_messages() -> GraphForge {
    let database = graph(
        "CREATE (:Person {id: 1}), (:Person {id: 2}), (:Person {id: 3}), (:Person {id: 4}),
                (:Message {id: 10}), (:Message {id: 11}), (:Message {id: 12})",
        &[
            ("Message", 10, "HAS_CREATOR", "Person", 1),
            ("Message", 11, "HAS_CREATOR", "Person", 1),
            ("Message", 12, "HAS_CREATOR", "Person", 4),
        ],
    );
    for (liker, message, time) in [(2, 10, 100), (2, 10, 200), (2, 11, 100), (3, 12, 100)] {
        let statement = format!(
            "MATCH (l:Person {{id: {liker}}}), (m:Message {{id: {message}}}) \
             CREATE (l)-[:LIKES {{creationDate: {time}}}]->(m)"
        );
        database
            .execute(&statement)
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
    }
    database
}

const LIKERS: &str = "MATCH (person:Person {id: 1}), (liker:Person) WHERE liker.id IN [2, 3, 4]";
const LIKES_OF: &str = "(person)<-[:HAS_CREATOR]-(message)<-[like:LIKES]-(liker)";
const LIKE_ROWS: &str = "RETURN liker.id, message.id, like.creationDate
     ORDER BY liker.id, message.id, like.creationDate";

#[test]
fn a_path_that_closes_after_one_unbound_node_matches_every_like() {
    let graph = liked_messages();
    // Both likes of message 10 are separate matches, and the named
    // relationship is available to the clause's WHERE and to the result.
    assert_eq!(
        rows(&graph, &format!("{LIKERS} MATCH {LIKES_OF} {LIKE_ROWS}")),
        table(&[
            &["2", "10", "100"],
            &["2", "10", "200"],
            &["2", "11", "100"]
        ])
    );
    assert_eq!(
        rows(
            &graph,
            &format!("{LIKERS} OPTIONAL MATCH {LIKES_OF} {LIKE_ROWS}")
        ),
        table(&[
            &["2", "10", "100"],
            &["2", "10", "200"],
            &["2", "11", "100"],
            &["3", "null", "null"],
            &["4", "null", "null"],
        ])
    );
    // The WHERE belongs to the OPTIONAL MATCH: a liker with no like at 200
    // keeps its row with NULLs.
    assert_eq!(
        rows(
            &graph,
            &format!(
                "{LIKERS} OPTIONAL MATCH {LIKES_OF} WHERE like.creationDate = 200 {LIKE_ROWS}"
            )
        ),
        table(&[
            &["2", "10", "200"],
            &["3", "null", "null"],
            &["4", "null", "null"],
        ])
    );
    // Each relationship has distinct endpoints, so the undirected path
    // matches the same rows.
    assert_eq!(
        rows(
            &graph,
            &format!(
                "{LIKERS} MATCH (person)-[:HAS_CREATOR]-(message)-[like:LIKES]-(liker) {LIKE_ROWS}"
            )
        ),
        table(&[
            &["2", "10", "100"],
            &["2", "10", "200"],
            &["2", "11", "100"]
        ])
    );
}

#[test]
fn a_path_that_closes_after_one_unbound_node_keeps_its_written_direction() {
    let graph = liked_messages();
    // With a single unbound node between the two bound ones, which end to
    // expand from depends on the graph's degrees, which the planner does not
    // see; the hop into `liker` stays as written (`dir=<-`) and plans like the
    // same path written as separate patterns.
    let written = physical_plan(
        &graph,
        &format!("{LIKERS} MATCH {LIKES_OF} RETURN message.id"),
    );
    let separate = physical_plan(
        &graph,
        &format!(
            "{LIKERS} MATCH (person)<-[:HAS_CREATOR]-(message), (message)<-[like:LIKES]-(liker)
             RETURN message.id"
        ),
    );
    let likes = expansions(&written, "LIKES");
    assert_eq!(likes.len(), 1, "one LIKES expansion\n{written}");
    assert!(
        likes[0].starts_with("ExpandExec: rel=LIKES, dir=<-,"),
        "the LIKES hop expands from the message\n{written}"
    );
    assert_eq!(likes, expansions(&separate, "LIKES"));
    assert_eq!(
        expansions(&written, "HAS_CREATOR"),
        expansions(&separate, "HAS_CREATOR")
    );
}
