//! A second MATCH that restates the first one and keeps only the rows holding
//! the extremum the first stage computed is answered from the first stage's
//! rows, not by expanding the pattern again (#1982, the LDBC SNB Interactive
//! IC7 shape).
//!
//! ```cypher
//! MATCH P WHERE W  WITH g, max(e) AS t
//! MATCH P WHERE W AND e = t  WITH g, t, min(id) AS latest
//! ```
//!
//! Every result test asserts the plan shape (one pair of expansions when the
//! pattern is restated exactly, two pairs when it is not) and the rows the
//! graph defines, at one and at four partitions, on tiny batches so the
//! collected rows span several of them. The graph has ties for the extremum, a
//! NULL value inside a group that also holds non-NULL values, a group whose
//! values are all NULL, duplicate relationships, and several groups.
//!
//! The negative tests change one thing in the second stage (an extra
//! conjunct, a stricter pattern, a flipped direction, the other extremum) and
//! assert both that the pass does not fire and that the rows are the ones the
//! graph defines for that different query.

use arrow::array::Array;
use arrow::util::display::array_value_to_string;
use graphforge_api::{ExecutionResourcePolicy, ExplainStage, GraphForge, GraphForgeOptions};

const PARTITIONS: [usize; 2] = [1, 4];

/// Messages created by persons 1 to 3, and who liked them when.
///
/// | liker | likes (message: creationDate)                                  |
/// |-------|----------------------------------------------------------------|
/// | 10    | Post 100: 5, Post 101: 7, Comment 200: 7 (tie), Post 102: 2    |
/// | 11    | Post 100: 3, Post 102: 4, Photo 300: 6 (not a Post or Comment) |
/// | 12    | Post 100: NULL, Comment 201: 9                                 |
/// | 13    | Post 101: NULL, Post 100: 4                                    |
/// | 14    | Post 100: 6 twice, Post 101: 6 twice (duplicate likes, tie)    |
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
                (:Person {id: 11}), (:Person {id: 12}), (:Person {id: 13}), (:Person {id: 14}),
                (:Person {id: 15}), (:Person {id: 16}),
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
    let likes: [(i64, &str, i64, Option<i64>); 15] = [
        (10, "Post", 100, Some(5)),
        (10, "Post", 101, Some(7)),
        (10, "Comment", 200, Some(7)),
        (10, "Post", 102, Some(2)),
        (11, "Post", 100, Some(3)),
        (11, "Post", 102, Some(4)),
        (11, "Photo", 300, Some(6)),
        (12, "Post", 100, None),
        (12, "Comment", 201, Some(9)),
        (13, "Post", 101, None),
        (13, "Post", 100, Some(4)),
        (14, "Post", 100, Some(6)),
        (14, "Post", 100, Some(6)),
        (14, "Post", 101, Some(6)),
        (14, "Post", 101, Some(6)),
    ];
    for (liker, label, message, created) in likes {
        like(
            &forge,
            liker,
            label,
            message,
            created.map(|at| at.to_string()),
        );
    }
    forge
}

fn like(forge: &GraphForge, liker: i64, label: &str, message: i64, created: Option<String>) {
    let properties = created.map_or(String::new(), |at| format!(" {{creationDate: {at}}}"));
    let statement = format!(
        "MATCH (l:Person {{id: {liker}}}), (m:{label} {{id: {message}}}) \
         CREATE (l)-[:LIKES{properties}]->(m)"
    );
    forge
        .execute(&statement)
        .unwrap_or_else(|error| panic!("{statement}: {error}"));
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
    // No query here orders its result.
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

/// Relationship expansions in a physical plan, per-row or anchored.
fn expansions(plan: &str) -> usize {
    plan.lines()
        .map(str::trim_start)
        .filter(|line| line.starts_with("ExpandExec") || line.starts_with("AnchoredExpansionExec"))
        .count()
}

/// Whether the plan reads the rows the first stage collected.
fn rematches(plan: &str) -> bool {
    plan.contains("UnwindExec: list=_extremum_rematch_")
}

const BOUND_SECOND_PATTERN: &str = "MATCH (person)<-[:HAS_CREATOR]-(message)<-[like:LIKES]-(liker)";

/// The IC7 shape: `first` and `second` pick the extremum direction of each
/// stage, `first_stage_anchor` restricts `person` in the first stage only,
/// `second_pattern` restates the pattern, and `second_extra` adds a conjunct.
fn latest_like_query(
    first: &str,
    first_stage_anchor: &str,
    second_pattern: &str,
    second_extra: &str,
    second: &str,
) -> String {
    format!(
        "MATCH (person:Person{first_stage_anchor})<-[:HAS_CREATOR]-(message)<-[like:LIKES]-(liker:Person)
         WHERE (message:Post OR message:Comment)
         WITH person, liker, {first}(like.creationDate) AS likeTime
         {second_pattern}
         WHERE (message:Post OR message:Comment) AND like.creationDate = likeTime{second_extra}
         WITH person, liker, likeTime, {second}(message.id) AS latestMessageId
         RETURN person.id AS personId, liker.id AS likerId, likeTime, latestMessageId"
    )
}

fn ic7(second_extra: &str) -> String {
    latest_like_query("max", "", BOUND_SECOND_PATTERN, second_extra, "min")
}

/// One query, whether the plan reads the first stage's rows again, and the
/// rows it returns.
type Case<'a> = (String, bool, &'a [&'a [&'a str]]);

/// Run each case at every partition count against one graph: assert the plan
/// has one pair of relationship expansions when it re-reads the first stage's
/// rows and more when the second stage expands again, and that the rows are
/// the ones the graph defines.
fn assert_cases(cases: &[Case<'_>]) {
    for partitions in PARTITIONS {
        let forge = forge(partitions);
        for (query, rematched, expected) in cases {
            let plan = physical_plan(&forge, query);
            assert_eq!(
                rematches(&plan),
                *rematched,
                "rematch at {partitions} partitions:\n{plan}"
            );
            if *rematched {
                assert_eq!(expansions(&plan), 2, "one pair of expansions:\n{plan}");
            } else {
                assert!(
                    expansions(&plan) > 2,
                    "the second stage expands again:\n{plan}"
                );
            }
            assert_eq!(
                rows(&forge, query),
                table(expected),
                "{partitions} partitions: {query}"
            );
        }
    }
}

fn assert_rows(query: &str, rematched: bool, expected: &[&[&str]]) {
    assert_cases(&[(query.to_owned(), rematched, expected)]);
}

/// Person, liker, the latest like time, and the smallest message id with it.
const LATEST: [&[&str]; 7] = [
    // Tie at 7 between Post 101 and Comment 200: the smaller id wins.
    &["1", "10", "7", "101"],
    // The Photo like (6) is not a Post or a Comment.
    &["1", "11", "3", "100"],
    // A NULL like beside a 4: the NULL is ignored by max and by `=`.
    &["1", "13", "4", "100"],
    // Two duplicate likes of each of two messages, all at 6.
    &["1", "14", "6", "100"],
    &["2", "10", "2", "102"],
    &["2", "11", "4", "102"],
    // Liker 12's only like of person 1's messages is NULL: no group.
    &["3", "12", "9", "201"],
];

#[test]
fn the_second_match_reads_the_first_stages_rows() {
    assert_rows(&ic7(""), true, &LATEST);
}

#[test]
fn the_rows_equal_those_of_the_stage_that_expands_again() {
    // `IS NOT NULL` is implied by `=`, so the rows are the same, but the extra
    // conjunct is not the extremum match and the pass must not fire.
    let rewritten = ic7("");
    let again = ic7(" AND like.creationDate IS NOT NULL");
    for partitions in PARTITIONS {
        let forge = forge(partitions);
        assert!(rematches(&physical_plan(&forge, &rewritten)));
        assert!(!rematches(&physical_plan(&forge, &again)));
        assert_eq!(
            rows(&forge, &rewritten),
            rows(&forge, &again),
            "{partitions} partitions"
        );
    }
}

#[test]
fn the_first_stage_may_anchor_the_group_the_second_restates() {
    // IC7 anchors `person` by id in the first stage; the second stage reads it
    // from the carried row.
    let query = latest_like_query("max", " {id: 1}", BOUND_SECOND_PATTERN, "", "min");
    assert_rows(
        &query,
        true,
        &[
            &["1", "10", "7", "101"],
            &["1", "11", "3", "100"],
            &["1", "13", "4", "100"],
            &["1", "14", "6", "100"],
        ],
    );
}

#[test]
fn restated_variables_may_be_renamed() {
    let query = "MATCH (person:Person)<-[:HAS_CREATOR]-(message)<-[like:LIKES]-(liker:Person)
         WHERE (message:Post OR message:Comment)
         WITH person, liker, max(like.creationDate) AS likeTime
         MATCH (person)<-[:HAS_CREATOR]-(post)<-[l:LIKES]-(liker)
         WHERE (post:Post OR post:Comment) AND l.creationDate = likeTime
         WITH person, liker, likeTime, min(post.id) AS latestMessageId
         RETURN person.id AS personId, liker.id AS likerId, likeTime, latestMessageId";
    assert_rows(query, true, &LATEST);
}

#[test]
fn an_extra_second_stage_conjunct_is_not_rewritten() {
    // Dropping Post 101 changes the tie at 7 for liker 10 to Comment 200 and
    // the tie at 6 for liker 14 stays at 100.
    assert_rows(
        &ic7(" AND message.id <> 101"),
        false,
        &[
            &["1", "10", "7", "200"],
            &["1", "11", "3", "100"],
            &["1", "13", "4", "100"],
            &["1", "14", "6", "100"],
            &["2", "10", "2", "102"],
            &["2", "11", "4", "102"],
            &["3", "12", "9", "201"],
        ],
    );
}

#[test]
fn a_stricter_second_pattern_is_not_rewritten() {
    // Restricting `message` to Post drops person 3's liker 12, whose maximum
    // is a Comment, and liker 10's tie goes to the Post.
    let query = latest_like_query(
        "max",
        "",
        "MATCH (person)<-[:HAS_CREATOR]-(message:Post)<-[like:LIKES]-(liker)",
        "",
        "min",
    );
    assert_rows(
        &query,
        false,
        &[
            &["1", "10", "7", "101"],
            &["1", "11", "3", "100"],
            &["1", "13", "4", "100"],
            &["1", "14", "6", "100"],
            &["2", "10", "2", "102"],
            &["2", "11", "4", "102"],
        ],
    );
}

#[test]
fn a_flipped_second_direction_is_not_rewritten() {
    // No message is created by a message: nothing matches.
    let query = latest_like_query(
        "max",
        "",
        "MATCH (person)-[:HAS_CREATOR]->(message)<-[like:LIKES]-(liker)",
        "",
        "min",
    );
    assert_rows(&query, false, &[]);
}

#[test]
fn a_different_extremum_direction_is_not_rewritten() {
    let pick = |first, second| latest_like_query(first, "", BOUND_SECOND_PATTERN, "", second);
    assert_cases(&[
        // Earliest like, smallest message.
        (
            pick("min", "min"),
            false,
            &[
                &["1", "10", "5", "100"],
                &["1", "11", "3", "100"],
                &["1", "13", "4", "100"],
                &["1", "14", "6", "100"],
                &["2", "10", "2", "102"],
                &["2", "11", "4", "102"],
                &["3", "12", "9", "201"],
            ],
        ),
        // Earliest like, largest message.
        (
            pick("min", "max"),
            false,
            &[
                &["1", "10", "5", "100"],
                &["1", "11", "3", "100"],
                &["1", "13", "4", "100"],
                &["1", "14", "6", "101"],
                &["2", "10", "2", "102"],
                &["2", "11", "4", "102"],
                &["3", "12", "9", "201"],
            ],
        ),
        // Latest like, largest message.
        (
            pick("max", "max"),
            false,
            &[
                &["1", "10", "7", "200"],
                &["1", "11", "3", "100"],
                &["1", "13", "4", "100"],
                &["1", "14", "6", "101"],
                &["2", "10", "2", "102"],
                &["2", "11", "4", "102"],
                &["3", "12", "9", "201"],
            ],
        ),
    ]);
}

#[test]
fn a_barrier_between_the_stages_is_not_rewritten() {
    let query = "MATCH (person:Person)<-[:HAS_CREATOR]-(message)<-[like:LIKES]-(liker:Person)
         WHERE (message:Post OR message:Comment)
         WITH person, liker, max(like.creationDate) AS likeTime
         ORDER BY person.id, liker.id
         MATCH (person)<-[:HAS_CREATOR]-(message)<-[like:LIKES]-(liker)
         WHERE (message:Post OR message:Comment) AND like.creationDate = likeTime
         WITH person, liker, likeTime, min(message.id) AS latestMessageId
         RETURN person.id AS personId, liker.id AS likerId, likeTime, latestMessageId";
    assert_rows(query, false, &LATEST);
}

/// Values the planner may not treat as plain integers: an integer and a float
/// that compare equal, a float NaN (equal to nothing, itself included), and a
/// group whose likes are all NULL beside one with a single value. The rematch
/// must return what the second expansion returns.
#[test]
fn numeric_edge_cases_match_the_stage_that_expands_again() {
    let rewritten = ic7("");
    let again = ic7(" AND like.creationDate IS NOT NULL");
    for partitions in PARTITIONS {
        let forge = forge(partitions);
        like(&forge, 15, "Post", 100, Some("8".to_owned()));
        like(&forge, 15, "Post", 101, Some("8.0".to_owned()));
        like(&forge, 16, "Post", 100, Some("0.0 / 0.0".to_owned()));
        like(&forge, 16, "Post", 101, Some("2.0".to_owned()));
        assert!(rematches(&physical_plan(&forge, &rewritten)));
        assert!(!rematches(&physical_plan(&forge, &again)));
        let got = rows(&forge, &rewritten);
        assert_eq!(got, rows(&forge, &again), "{partitions} partitions");
        assert!(
            got.iter().any(|row| row[1] == "15" && row[3] == "100"),
            "8 and 8.0 are one value, so Post 100 and Post 101 tie: {got:?}"
        );
    }
}
