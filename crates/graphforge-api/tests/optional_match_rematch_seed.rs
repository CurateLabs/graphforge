//! An OPTIONAL MATCH that matches an already-bound node again is seeded with,
//! and joined back on, only the outer variables its pattern names (#1984, the
//! LDBC SNB Interactive IC10 shape `OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post:Post)`).
//!
//! A correlated OPTIONAL MATCH executes its pattern once, over a seed of the
//! outer rows, and left-joins the matches back. Seeding it with the whole outer
//! row made the join key every column of that row, however few the pattern
//! read: IC10 carried 26 key pairs to match two node identities. The pattern
//! depends on the nodes it names and on the outer values its WHERE reads, so the
//! seed is those columns and rows that share them share their matches.
//!
//! Every test asserts the rows first and the join keys second, at one and at
//! four partitions, on a narrow graph and on one whose nodes carry twelve
//! properties no query reads. The expected rows are computed by hand from the
//! fixture, so they are also what the whole-row seed returned: a lowering that
//! changed an answer fails on the rows, and one that stopped narrowing the seed
//! fails on the keys. The patterns that must keep the whole-row seed assert the
//! rows and that the keys stayed wide.

use arrow::array::Array;
use arrow::util::display::array_value_to_string;
use graphforge_api::{ExecutionResourcePolicy, ExplainStage, GraphForge, GraphForgeOptions};

/// Four persons, three posts and two tags.
///
/// ```text
/// person 1 (score 5) KNOWS persons 2, 3 and 4, and is interested in tag 100
/// post 10 and post 11 are created by person 2; post 12 by person 3
/// post 10 and post 12 have tag 100; post 11 has tag 101
/// ```
///
/// Person 4 created nothing. `extra` further properties `x0`, `x1`, .. that no
/// query filters on widen every person.
fn social(partitions: usize, extra: usize) -> GraphForge {
    let wide: String = (0..extra).map(|i| format!(", x{i}: {i}")).collect();
    let forge = GraphForge::new_with_options(
        None,
        GraphForgeOptions {
            resource: ExecutionResourcePolicy {
                target_partitions: Some(partitions),
                ..ExecutionResourcePolicy::default()
            },
            ..GraphForgeOptions::default()
        },
    )
    .expect("in-memory forge");
    forge
        .execute(&format!(
            "CREATE (p1:Person {{id: 1, score: 5{wide}}}), (p2:Person {{id: 2, score: 7{wide}}}), \
             (p3:Person {{id: 3, score: 9{wide}}}), (p4:Person {{id: 4, score: 2{wide}}}), \
             (t1:Tag {{id: 100}}), (t2:Tag {{id: 101}}), \
             (a:Post {{id: 10}}), (b:Post {{id: 11}}), (c:Post {{id: 12}}), \
             (p1)-[:KNOWS]->(p2), (p1)-[:KNOWS]->(p3), (p1)-[:KNOWS]->(p4), \
             (a)-[:HAS_CREATOR]->(p2), (b)-[:HAS_CREATOR]->(p2), (c)-[:HAS_CREATOR]->(p3), \
             (a)-[:HAS_TAG]->(t1), (b)-[:HAS_TAG]->(t2), (c)-[:HAS_TAG]->(t1), \
             (p1)-[:HAS_INTEREST]->(t1)"
        ))
        .expect("fixture creates");
    forge
}

/// Every cell of the result rendered, nulls as `NULL`, one entry per row,
/// columns joined by `,`, sorted: no query here orders its result.
fn rows(forge: &GraphForge, query: &str) -> Vec<String> {
    let result = forge
        .execute(query)
        .unwrap_or_else(|error| panic!("{query}: {error}"));
    let mut out = Vec::new();
    for batch in &result.batches {
        for row in 0..batch.num_rows() {
            let cells: Vec<String> = (0..batch.num_columns())
                .map(|column| {
                    let array = batch.column(column);
                    if array.is_null(row) {
                        "NULL".to_owned()
                    } else {
                        array_value_to_string(array, row).expect("cell renders")
                    }
                })
                .collect();
            out.push(cells.join(","));
        }
    }
    out.sort();
    out
}

/// The join keys of every OPTIONAL MATCH in the physical plan, outermost first.
/// A pattern predicate lowers to the same operator, so it counts too.
fn join_keys(forge: &GraphForge, query: &str) -> Vec<usize> {
    let plan = forge
        .explain_stage(query, ExplainStage::PhysicalPlan)
        .unwrap_or_else(|error| panic!("{query}: {error}"));
    plan.lines()
        .filter_map(|line| {
            line.split_once("OptionalMatchExec: keys=")
                .map(|(_, tail)| tail)
        })
        .map(|tail| {
            tail.chars()
                .take_while(char::is_ascii_digit)
                .collect::<String>()
                .parse()
                .unwrap_or_else(|_| panic!("no number after `keys=` in `{tail}`"))
        })
        .collect()
}

/// `query` returns `expected` and its OPTIONAL MATCHes join on `keys`, on every
/// combination of one and four partitions, narrow and wide nodes.
fn assert_rows_and_keys(query: &str, expected: &[&str], keys: &[usize]) {
    let expected: Vec<String> = expected.iter().map(|row| (*row).to_owned()).collect();
    for partitions in [1, 4] {
        for extra in [0, 12] {
            let forge = social(partitions, extra);
            let context = format!("{partitions} partitions, {extra} extra properties: {query}");
            assert_eq!(rows(&forge, query), expected, "rows, {context}");
            assert_eq!(join_keys(&forge, query), keys, "join keys, {context}");
        }
    }
}

/// As [`assert_rows_and_keys`] for a pattern that must keep the whole-row seed:
/// the keys are the outer row's columns, far more than any narrowed seed.
fn assert_rows_and_whole_row_keys(query: &str, expected: &[&str], whole_row_joins: usize) {
    let expected: Vec<String> = expected.iter().map(|row| (*row).to_owned()).collect();
    for partitions in [1, 4] {
        for extra in [0, 12] {
            let forge = social(partitions, extra);
            let context = format!("{partitions} partitions, {extra} extra properties: {query}");
            assert_eq!(rows(&forge, query), expected, "rows, {context}");
            let keys = join_keys(&forge, query);
            assert_eq!(
                keys.iter().filter(|keys| **keys > NARROWED_LIMIT).count(),
                whole_row_joins,
                "joins keyed on the whole outer row, {context}: keys {keys:?}"
            );
        }
    }
}

/// No narrowed seed in this file joins on more keys than this: a node identity
/// per bound node, plus a scalar the pattern's WHERE reads.
const NARROWED_LIMIT: usize = 3;

// ---------------------------------------------------------------------------
// The IC10 shape
// ---------------------------------------------------------------------------

#[test]
fn rematching_a_bound_node_joins_on_its_identity_alone() {
    // Person 4 created nothing and keeps one NULL row; person 2 keeps two posts.
    assert_rows_and_keys(
        "MATCH (person:Person {id: 1})-[:KNOWS]->(friend:Person) \
         OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post:Post) \
         RETURN friend.id AS fid, post.id AS pid",
        &["2,10", "2,11", "3,12", "4,NULL"],
        &[1],
    );
}

#[test]
fn two_rematching_optional_matches_join_on_the_nodes_each_names() {
    // The second OPTIONAL MATCH names `friend` and `person` and closes on the
    // bound `person`. Friend 2 has two posts and one carries person 1's
    // interest; friend 3 has one and it does; friend 4 has none.
    assert_rows_and_keys(
        "MATCH (person:Person {id: 1})-[:KNOWS]->(friend:Person) \
         OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post:Post) \
         WITH person, friend, count(post) AS postCount \
         OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(:Tag)<-[:HAS_INTEREST]-(person) \
         RETURN friend.id AS fid, postCount, count(DISTINCT post.id) AS common",
        &["2,2,1", "3,1,1", "4,0,0"],
        &[2, 1],
    );
}

#[test]
fn join_keys_do_not_grow_with_the_properties_the_parent_reads() {
    // The parent reads six properties of two outer nodes; the OPTIONAL MATCH
    // reads neither, so its keys stay the one identity however wide the rows
    // are.
    let query = "MATCH (p:Person {id: 1})-[:KNOWS]->(f:Person) \
                 OPTIONAL MATCH (f)<-[:HAS_CREATOR]-(post:Post) \
                 RETURN f.x0 AS a, f.x1 AS b, f.x2 AS c, p.x3 AS d, p.x4 AS e, p.x5 AS g, \
                 post.id AS pid";
    let expected = [
        "0,1,2,3,4,5,10",
        "0,1,2,3,4,5,11",
        "0,1,2,3,4,5,12",
        "0,1,2,3,4,5,NULL",
    ];
    for partitions in [1, 4] {
        let forge = social(partitions, 12);
        assert_eq!(
            rows(&forge, query),
            expected,
            "rows, {partitions} partitions"
        );
        assert_eq!(
            join_keys(&forge, query),
            [1],
            "join keys, {partitions} partitions"
        );
    }
}

// ---------------------------------------------------------------------------
// Semantics the narrowed seed must keep
// ---------------------------------------------------------------------------

#[test]
fn outer_rows_that_share_a_node_each_get_its_matches() {
    // Person 2 is bound by two outer rows, and they differ only in `fid`'s
    // position in the list; both keep both posts. Person 4 keeps its NULL row.
    assert_rows_and_keys(
        "UNWIND [2, 2, 3, 4] AS fid MATCH (f:Person {id: fid}) \
         OPTIONAL MATCH (f)<-[:HAS_CREATOR]-(post:Post) RETURN fid, post.id AS pid",
        &["2,10", "2,10", "2,11", "2,11", "3,12", "4,NULL"],
        &[1],
    );
}

#[test]
fn outer_rows_that_share_a_node_and_differ_in_a_scalar_the_pattern_ignores_stay_distinct() {
    assert_rows_and_keys(
        "UNWIND [1, 2, 3] AS n MATCH (f:Person {id: 2}) \
         OPTIONAL MATCH (f)<-[:HAS_CREATOR]-(post:Post) RETURN n, post.id AS pid",
        &["1,10", "1,11", "2,10", "2,11", "3,10", "3,11"],
        &[1],
    );
}

#[test]
fn where_reading_an_outer_scalar_adds_that_column_to_the_keys() {
    // The WHERE keeps the posts whose id is at least `lim`: both for 10, one
    // for 11, none for 99.
    assert_rows_and_keys(
        "UNWIND [10, 11, 99] AS lim MATCH (f:Person {id: 2}) \
         OPTIONAL MATCH (f)<-[:HAS_CREATOR]-(post:Post) WHERE post.id >= lim \
         RETURN lim, post.id AS pid",
        &["10,10", "10,11", "11,11", "99,NULL"],
        &[2],
    );
}

#[test]
fn where_reading_a_property_of_an_outer_node_joins_on_that_node() {
    // Person 1's score is 5: the WHERE keeps the posts with an id above 10, so
    // person 2 keeps post 11, person 3 post 12 and person 4 none.
    assert_rows_and_keys(
        "MATCH (p:Person {id: 1})-[:KNOWS]->(f:Person) \
         OPTIONAL MATCH (f)<-[:HAS_CREATOR]-(post:Post) WHERE post.id > p.score + 5 \
         RETURN f.id AS fid, post.id AS pid",
        &["2,11", "3,12", "4,NULL"],
        &[2],
    );
}

#[test]
fn a_nested_optional_match_over_a_nullable_node_keeps_the_null() {
    // Person 4 has no post, so the second OPTIONAL MATCH re-matches a NULL
    // `post` and must return a NULL tag, not drop the row.
    assert_rows_and_keys(
        "MATCH (p:Person {id: 1})-[:KNOWS]->(f:Person) \
         OPTIONAL MATCH (f)<-[:HAS_CREATOR]-(post:Post) \
         OPTIONAL MATCH (post)-[:HAS_TAG]->(t:Tag) \
         RETURN f.id AS fid, post.id AS pid, t.id AS tid",
        &["2,10,100", "2,11,101", "3,12,100", "4,NULL,NULL"],
        &[1, 1],
    );
}

#[test]
fn an_optional_match_may_traverse_the_relationship_the_outer_match_bound() {
    // Separate clauses do not constrain each other: person 1 reaches each
    // friend over the KNOWS relationship the outer MATCH just traversed.
    assert_rows_and_keys(
        "MATCH (p:Person {id: 1})-[r:KNOWS]->(b:Person) \
         OPTIONAL MATCH (b)<-[:KNOWS]-(c:Person) RETURN b.id AS bid, c.id AS cid",
        &["2,1", "3,1", "4,1"],
        &[1],
    );
}

#[test]
fn relationships_of_one_optional_pattern_stay_distinct() {
    // Two comma patterns of one clause cannot bind the same HAS_CREATOR: person
    // 2 pairs its two posts both ways, person 3 has one post and so no pair.
    assert_rows_and_keys(
        "MATCH (p:Person {id: 1})-[:KNOWS]->(f:Person) \
         OPTIONAL MATCH (f)<-[:HAS_CREATOR]-(p1:Post), (f)<-[:HAS_CREATOR]-(p2:Post) \
         RETURN f.id AS fid, p1.id AS a, p2.id AS b",
        &["2,10,11", "2,11,10", "3,NULL,NULL", "4,NULL,NULL"],
        &[1],
    );
}

#[test]
fn a_label_the_rematched_node_lacks_matches_nothing() {
    assert_rows_and_keys(
        "MATCH (p:Person {id: 1})-[:KNOWS]->(f:Person) \
         OPTIONAL MATCH (f:Tag)<-[:HAS_CREATOR]-(post:Post) RETURN f.id AS fid, post.id AS pid",
        &["2,NULL", "3,NULL", "4,NULL"],
        &[1],
    );
}

#[test]
fn a_property_map_on_the_rematched_node_filters_its_matches() {
    // Only person 2 has score 7.
    assert_rows_and_keys(
        "MATCH (p:Person {id: 1})-[:KNOWS]->(f:Person) \
         OPTIONAL MATCH (f {score: 7})<-[:HAS_CREATOR]-(post:Post) \
         RETURN f.id AS fid, post.id AS pid",
        &["2,10", "2,11", "3,NULL", "4,NULL"],
        &[1],
    );
}

#[test]
fn an_optional_match_after_a_not_pattern_predicate_joins_on_its_identity_alone() {
    // The predicate keeps only person 4, who created no post. Both the
    // predicate's join and the OPTIONAL MATCH's are keyed on one identity.
    assert_rows_and_keys(
        "MATCH (p:Person {id: 1})-[:KNOWS]->(f:Person) WHERE NOT (f)<-[:HAS_CREATOR]-(:Post) \
         OPTIONAL MATCH (f)<-[:KNOWS]-(g:Person) RETURN f.id AS fid, g.id AS gid",
        &["4,1"],
        &[1, 1],
    );
}

// ---------------------------------------------------------------------------
// Patterns that keep the whole-row seed
// ---------------------------------------------------------------------------

#[test]
fn an_optional_match_that_rematches_an_outer_relationship_keeps_the_whole_row() {
    // `r` is bound by the outer MATCH and traversed again, backwards.
    assert_rows_and_whole_row_keys(
        "MATCH (p:Person {id: 1})-[r:KNOWS]->(b:Person) \
         OPTIONAL MATCH (b)<-[r]-(c:Person) RETURN b.id AS bid, c.id AS cid",
        &["2,1", "3,1", "4,1"],
        1,
    );
}

#[test]
fn an_optional_match_whose_where_holds_a_not_pattern_predicate_keeps_the_whole_row() {
    // The WHERE drops the posts tagged 100: person 2 keeps post 11, person 3
    // loses its only post, and person 4 had none.
    assert_rows_and_whole_row_keys(
        "MATCH (p:Person {id: 1})-[:KNOWS]->(f:Person) \
         OPTIONAL MATCH (f)<-[:HAS_CREATOR]-(post:Post) \
         WHERE NOT (post)-[:HAS_TAG]->(:Tag {id: 100}) \
         RETURN f.id AS fid, post.id AS pid",
        &["2,11", "3,NULL", "4,NULL"],
        1,
    );
}
