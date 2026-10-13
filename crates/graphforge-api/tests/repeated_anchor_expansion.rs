//! A MATCH that restarts at a node the input already bound is expanded once per
//! distinct anchor, not once per input row (#1984, the LDBC SNB Interactive IC7
//! shape).
//!
//! `WITH person, liker, max(like.creationDate) AS t MATCH (person)<-[..]-(m)<-[..]-(liker)`
//! used to expand `person`'s whole relationship set for every aggregated row. The
//! planner now expands it once per distinct `person` and hash-joins the result
//! back to the rows.
//!
//! Every result test asserts that the plan really contains the anchored
//! expansion, then compares the rewrite with the same query written so the
//! planner keeps the per-row form (an `ORDER BY` after the barrier), and with
//! the rows the graph defines. Both forms run at one and at four partitions, on
//! tiny batches so the collected input spans several of them. The rewrite must
//! keep each input row's multiplicity, match no row for a NULL anchor or a NULL
//! key, and return nothing for an empty input.

use arrow::array::Array;
use arrow::util::display::array_value_to_string;
use graphforge_api::{ExecutionResourcePolicy, ExplainStage, GraphForge, GraphForgeOptions};
use graphforge_exec::demand;

const PARTITIONS: [usize; 2] = [1, 4];

/// The graph every test reads.
///
/// ```text
/// persons 1, 2, 3 create messages; persons 10, 11, 12 like them; 13 does nothing
/// post 100, 101 and comment 200: created by 1     post 102: created by 2
/// comment 201: created by 3                       photo 300: created by 1
///
/// likes (liker -> message @ creationDate)
///   10 -> 100 @ 5, 101 @ 7, 200 @ 7, 102 @ 2
///   11 -> 100 @ 3, 102 @ 4, 300 @ 6
///   12 -> 100 @ NULL (no creationDate), 201 @ 9
/// ```
fn forge(target_partitions: usize) -> GraphForge {
    let forge = GraphForge::new_with_options(
        None,
        GraphForgeOptions {
            resource: ExecutionResourcePolicy {
                target_partitions: Some(target_partitions),
                batch_size: Some(2),
                ..ExecutionResourcePolicy::default()
            },
            ..GraphForgeOptions::default()
        },
    )
    .expect("in-memory instance");
    let statements = [
        "CREATE (:Person {id: 1}), (:Person {id: 2}), (:Person {id: 3}), (:Person {id: 10}),
                (:Person {id: 11}), (:Person {id: 12}), (:Person {id: 13}),
                (:Post {id: 100}), (:Post {id: 101}), (:Post {id: 102}),
                (:Comment {id: 200}), (:Comment {id: 201}), (:Photo {id: 300})",
        "MATCH (m:Post {id: 100}), (p:Person {id: 1}) CREATE (m)-[:HAS_CREATOR]->(p)",
        "MATCH (m:Post {id: 101}), (p:Person {id: 1}) CREATE (m)-[:HAS_CREATOR]->(p)",
        "MATCH (m:Post {id: 102}), (p:Person {id: 2}) CREATE (m)-[:HAS_CREATOR]->(p)",
        "MATCH (m:Comment {id: 200}), (p:Person {id: 1}) CREATE (m)-[:HAS_CREATOR]->(p)",
        "MATCH (m:Comment {id: 201}), (p:Person {id: 3}) CREATE (m)-[:HAS_CREATOR]->(p)",
        "MATCH (m:Photo {id: 300}), (p:Person {id: 1}) CREATE (m)-[:HAS_CREATOR]->(p)",
    ];
    for statement in statements {
        forge
            .execute(statement)
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
    }
    let likes: [(i64, &str, i64, Option<i64>); 9] = [
        (10, "Post", 100, Some(5)),
        (10, "Post", 101, Some(7)),
        (10, "Comment", 200, Some(7)),
        (10, "Post", 102, Some(2)),
        (11, "Post", 100, Some(3)),
        (11, "Post", 102, Some(4)),
        (11, "Photo", 300, Some(6)),
        (12, "Post", 100, None),
        (12, "Comment", 201, Some(9)),
    ];
    for (liker, label, message, created) in likes {
        let properties = created.map_or(String::new(), |at| format!(" {{creationDate: {at}}}"));
        let statement = format!(
            "MATCH (l:Person {{id: {liker}}}), (m:{label} {{id: {message}}}) \
             CREATE (l)-[:LIKES{properties}]->(m)"
        );
        forge
            .execute(&statement)
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
    }
    forge
}

fn rows(forge: &GraphForge, query: &str) -> Vec<Vec<String>> {
    let result = forge
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
    // The rewrite does not preserve row order; no query here orders its result.
    rows.sort();
    rows
}

fn table(rows: &[&[&str]]) -> Vec<Vec<String>> {
    let mut rows: Vec<Vec<String>> = rows
        .iter()
        .map(|row| row.iter().map(|cell| (*cell).to_owned()).collect())
        .collect();
    rows.sort();
    rows
}

fn physical_plan(forge: &GraphForge, query: &str) -> String {
    forge
        .explain_stage(query, ExplainStage::PhysicalPlan)
        .unwrap_or_else(|error| panic!("{query}: {error}"))
}

fn anchored_expansions(plan: &str) -> usize {
    plan.lines()
        .filter(|line| line.trim_start().starts_with("AnchoredExpansionExec"))
        .count()
}

/// Assert that `rewritten` plans `anchored` anchored expansions, that
/// `per_row` plans none, and that both return `expected`, at every partition
/// count.
fn assert_same_rows(rewritten: &str, per_row: &str, anchored: usize, expected: &[&[&str]]) {
    let expected = table(expected);
    for partitions in PARTITIONS {
        let forge = forge(partitions);
        let plan = physical_plan(&forge, rewritten);
        assert_eq!(
            anchored_expansions(&plan),
            anchored,
            "{partitions} partitions: the rewritten form's plan\n{plan}"
        );
        let plan = physical_plan(&forge, per_row);
        assert_eq!(
            anchored_expansions(&plan),
            0,
            "{partitions} partitions: the per-row form's plan\n{plan}"
        );
        assert_eq!(
            rows(&forge, rewritten),
            expected,
            "{partitions} partitions, rewritten: {rewritten}"
        );
        assert_eq!(
            rows(&forge, per_row),
            expected,
            "{partitions} partitions, per row: {per_row}"
        );
    }
}

/// The three stages of LDBC SNB Interactive IC7 over every person, not one.
/// `barrier` follows each aggregating WITH: empty for the rewritten form, an
/// ORDER BY for the form the planner keeps per row.
fn latest_like_query(first_stage_filter: &str, barrier: [&str; 2]) -> String {
    format!(
        "MATCH (person:Person)<-[:HAS_CREATOR]-(message)<-[like:LIKES]-(liker:Person)
         WHERE {first_stage_filter}(message:Post OR message:Comment)
         WITH person, liker, max(like.creationDate) AS likeTime {first}
         MATCH (person)<-[:HAS_CREATOR]-(message)<-[like:LIKES]-(liker)
         WHERE (message:Post OR message:Comment) AND like.creationDate = likeTime
         WITH person, liker, likeTime, min(message.id) AS latestMessageId {second}
         MATCH (person)<-[:HAS_CREATOR]-(message)
         WHERE (message:Post OR message:Comment) AND message.id = latestMessageId
         RETURN person.id AS personId, liker.id AS likerId, likeTime, message.id AS messageId",
        first = barrier[0],
        second = barrier[1],
    )
}

/// The latest like of each (creator, liker) pair, then the message it liked.
///
/// ```text
/// (1, 10): likes 101 and 200 at 7, the lower id wins    (1, 11): 100 at 3
/// (1, 12): its only like of person 1 has no creationDate, so it matches nothing
/// (2, 10): 102 at 2    (2, 11): 102 at 4    (3, 12): 201 at 9
/// ```
///
/// Person 1 anchors three groups, so its relationships are the same expansion
/// three times in the per-row form. Liker 12's NULL key must match no row.
#[test]
fn latest_like_per_liker_matches_the_per_row_form() {
    assert_same_rows(
        &latest_like_query("", ["", ""]),
        &latest_like_query(
            "",
            [
                "ORDER BY person.id, liker.id",
                "ORDER BY person.id, liker.id",
            ],
        ),
        2,
        &[
            &["1", "10", "7", "101"],
            &["1", "11", "3", "100"],
            &["2", "10", "2", "102"],
            &["2", "11", "4", "102"],
            &["3", "12", "9", "201"],
        ],
    );
}

/// A creator reached through an OPTIONAL MATCH is NULL for a person who likes
/// nothing, and a person who likes several messages of one creator yields one
/// group per creator and liker. Projecting the liker away leaves the creator
/// once per group: the input has duplicate rows, and every one of them must
/// reach every post of its creator.
fn creator_posts_query(barrier: &str) -> String {
    format!(
        "MATCH (liker:Person)
         OPTIONAL MATCH (liker)-[:LIKES]->(message)-[:HAS_CREATOR]->(creator:Person)
         WITH creator, liker, count(*) AS likes {barrier}
         WITH creator
         MATCH (creator)<-[:HAS_CREATOR]-(post:Post)
         RETURN creator.id AS creatorId, post.id AS postId"
    )
}

/// Creator 1 is reached from likers 10, 11 and 12, creator 2 from 10 and 11,
/// creator 3 from 12 and no creator from the four people who like nothing.
#[test]
fn duplicate_input_rows_and_null_anchors_keep_their_multiplicity() {
    let rewritten = creator_posts_query("");
    let per_row = creator_posts_query("ORDER BY liker.id");
    let expected: &[&[&str]] = &[
        &["1", "100"],
        &["1", "100"],
        &["1", "100"],
        &["1", "101"],
        &["1", "101"],
        &["1", "101"],
        &["2", "102"],
        &["2", "102"],
    ];
    assert_same_rows(&rewritten, &per_row, 1, expected);
}

/// An empty input expands nothing and returns nothing.
#[test]
fn empty_input_returns_no_rows() {
    assert_same_rows(
        &latest_like_query("person.id = 999 AND ", ["", ""]),
        &latest_like_query(
            "person.id = 999 AND ",
            [
                "ORDER BY person.id, liker.id",
                "ORDER BY person.id, liker.id",
            ],
        ),
        2,
        &[],
    );
}

/// Every person paired with every person, then each person once per pairing:
/// seven people, forty-nine input rows, and a single expansion in the query.
fn paired_posts_query(barrier: &str, filter: &str) -> String {
    format!(
        "MATCH (person:Person), (other:Person)
         WITH person, other, count(*) AS pairs {barrier}
         WITH person, other
         MATCH (person)<-[:HAS_CREATOR]-(post:Post)
         {filter}
         RETURN person.id AS personId, post.id AS postId, other.id AS otherId"
    )
}

fn paired_posts_rows(rows: &[(&str, &str)], others: &[&str]) -> Vec<Vec<String>> {
    let mut all = Vec::new();
    for (person, post) in rows {
        for other in others {
            all.push(vec![
                (*person).to_owned(),
                (*post).to_owned(),
                (*other).to_owned(),
            ]);
        }
    }
    all.sort();
    all
}

const PEOPLE: [&str; 7] = ["1", "2", "3", "10", "11", "12", "13"];

/// The relationship set of each anchor is read once, however many rows carry
/// the anchor: the expansion's input is the seven distinct people, where the
/// per-row form feeds it all forty-nine rows.
#[test]
fn an_anchor_is_expanded_once_however_many_rows_carry_it() {
    let rewritten = paired_posts_query("", "");
    let per_row = paired_posts_query("ORDER BY person.id, other.id", "");
    let expected = paired_posts_rows(&[("1", "100"), ("1", "101"), ("2", "102")], &PEOPLE);
    for partitions in PARTITIONS {
        let forge = forge(partitions);
        assert_eq!(
            anchored_expansions(&physical_plan(&forge, &rewritten)),
            1,
            "{partitions} partitions"
        );
        let (result, snapshot) = demand::capture(|| forge.execute(&rewritten));
        let result = result.expect("rewritten form");
        assert_eq!(
            result.batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            expected.len()
        );
        assert_eq!(
            snapshot.hops.len(),
            1,
            "{partitions} partitions: the query has one expansion: {:?}",
            snapshot.hops
        );
        let anchors = snapshot.hops.values().next().expect("one hop").input_rows;
        assert_eq!(
            anchors, 7,
            "{partitions} partitions: the expansion reads each distinct person once"
        );

        let (result, snapshot) = demand::capture(|| forge.execute(&per_row));
        result.expect("per-row form");
        let rows_in = snapshot.hops.values().next().expect("one hop").input_rows;
        assert_eq!(
            rows_in, 49,
            "{partitions} partitions: the per-row form reads the anchor once per row"
        );

        assert_eq!(
            rows(&forge, &rewritten),
            expected,
            "{partitions} partitions"
        );
        assert_eq!(rows(&forge, &per_row), expected, "{partitions} partitions");
    }
}

/// A predicate that reads a column of the input as well as the pattern cannot
/// run inside the expansion; it applies to the joined rows, and the pairs it
/// rejects are not returned.
#[test]
fn a_predicate_over_the_input_applies_after_the_join() {
    let filter = "WHERE post.id + other.id > 110";
    let rewritten = paired_posts_query("", filter);
    let per_row = paired_posts_query("ORDER BY person.id, other.id", filter);
    // post 100 needs other >= 11, post 101 needs other >= 10, post 102 needs
    // other >= 9.
    let mut expected = paired_posts_rows(&[("1", "100")], &["11", "12", "13"]);
    expected.extend(paired_posts_rows(
        &[("1", "101"), ("2", "102")],
        &["10", "11", "12", "13"],
    ));
    expected.sort();
    for partitions in PARTITIONS {
        let forge = forge(partitions);
        assert_eq!(
            anchored_expansions(&physical_plan(&forge, &rewritten)),
            1,
            "{partitions} partitions"
        );
        assert_eq!(
            rows(&forge, &rewritten),
            expected,
            "{partitions} partitions"
        );
        assert_eq!(rows(&forge, &per_row), expected, "{partitions} partitions");
    }
}

/// An input no barrier has materialized streams: expanding it per distinct
/// anchor would hold it in memory and cost its early termination, so the
/// expansion stays per row.
#[test]
fn a_streaming_input_is_not_decorrelated() {
    let query = "MATCH (person:Person) MATCH (person)<-[:HAS_CREATOR]-(post:Post)
                 RETURN person.id AS personId, post.id AS postId";
    let expected = table(&[&["1", "100"], &["1", "101"], &["2", "102"]]);
    for partitions in PARTITIONS {
        let forge = forge(partitions);
        assert_eq!(
            anchored_expansions(&physical_plan(&forge, query)),
            0,
            "{partitions} partitions"
        );
        assert_eq!(rows(&forge, query), expected, "{partitions} partitions");
    }
}
