//! A subquery that uses a variable bound outside the pipeline it runs in
//! correlates with that variable (#1887 D15): an `EXISTS { … }` filter on an
//! `OPTIONAL MATCH` that refers to a variable bound before the OPTIONAL MATCH
//! (the LDBC SNB Interactive IC10 shape), and a simple `EXISTS` whose WHERE
//! refers to an outer variable its pattern does not bind.

use arrow::array::Array;
use arrow::util::display::array_value_to_string;
use graphforge_api::GraphForge;

fn rows(gf: &GraphForge, query: &str) -> Vec<Vec<Option<String>>> {
    let result = gf
        .execute(query)
        .unwrap_or_else(|error| panic!("{query}: {error}"));
    let mut rows = Vec::new();
    for batch in &result.batches {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|array| {
                        (!array.is_null(row))
                            .then(|| array_value_to_string(array, row).expect("render"))
                    })
                    .collect(),
            );
        }
    }
    rows
}

fn strings(values: &[&str]) -> Vec<Option<String>> {
    values.iter().map(|v| Some((*v).to_owned())).collect()
}

/// Friend 3 created posts 21..25. Post 21 has tags 11 and 12, post 22 tag 12,
/// posts 23..25 tag 13. Person 1 is interested in tags 11 and 12; person 2 in
/// tag 13 — so a filter that loses its correlation with person 1 admits all
/// five posts instead of 21 and 22.
fn social() -> GraphForge {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute(
        "CREATE (person:Person {id: 1}), (other:Person {id: 2}), (friend:Person {id: 3}),
                (f1:Person {id: 4}),
                (t1:Tag {id: 11}), (t2:Tag {id: 12}), (t3:Tag {id: 13}),
                (p1:Post {id: 21}), (p2:Post {id: 22}), (p3:Post {id: 23}),
                (p4:Post {id: 24}), (p5:Post {id: 25}),
                (person)-[:KNOWS]->(f1), (f1)-[:KNOWS]->(friend),
                (p1)-[:HAS_CREATOR]->(friend), (p2)-[:HAS_CREATOR]->(friend),
                (p3)-[:HAS_CREATOR]->(friend), (p4)-[:HAS_CREATOR]->(friend),
                (p5)-[:HAS_CREATOR]->(friend),
                (p1)-[:HAS_TAG]->(t1), (p1)-[:HAS_TAG]->(t2), (p2)-[:HAS_TAG]->(t2),
                (p3)-[:HAS_TAG]->(t3), (p4)-[:HAS_TAG]->(t3), (p5)-[:HAS_TAG]->(t3),
                (person)-[:HAS_INTEREST]->(t1), (person)-[:HAS_INTEREST]->(t2),
                (other)-[:HAS_INTEREST]->(t3)",
    )
    .expect("create graph");
    gf
}

const INTEREST: &str = "EXISTS { (post)-[:HAS_TAG]->(:Tag)<-[:HAS_INTEREST]-(person) }";

#[test]
fn optional_match_exists_filter_correlates_with_an_earlier_variable() {
    let gf = social();
    let optional = format!(
        "MATCH (person:Person {{id: 1}}), (friend:Person {{id: 3}})
         OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post:Post) WHERE {INTEREST}
         RETURN count(post)"
    );
    let required = optional.replace("OPTIONAL MATCH", "MATCH");
    assert_eq!(rows(&gf, &required), vec![strings(&["2"])]);
    assert_eq!(rows(&gf, &optional), vec![strings(&["2"])]);
    assert_eq!(
        rows(
            &gf,
            &format!(
                "MATCH (person:Person {{id: 1}}) MATCH (friend:Person {{id: 3}})
                 OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post:Post) WHERE {INTEREST}
                 RETURN post.id ORDER BY post.id"
            )
        ),
        vec![strings(&["21"]), strings(&["22"])]
    );
}

#[test]
fn optional_match_exists_filter_in_the_ic10_pipeline() {
    let gf = social();
    assert_eq!(
        rows(
            &gf,
            &format!(
                "MATCH (person:Person {{id: 1}})-[:KNOWS*2..2]-(friend)
                 WITH DISTINCT person, friend
                 OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post:Post)
                 WITH person, friend, count(post) AS postCount
                 OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post:Post) WHERE {INTEREST}
                 RETURN friend.id, postCount, count(post) AS commonPostCount"
            )
        ),
        vec![strings(&["3", "5", "2"])]
    );
}

#[test]
fn optional_match_exists_filter_that_admits_nothing_leaves_a_null_row() {
    let gf = social();
    assert_eq!(
        rows(
            &gf,
            "MATCH (person:Person {id: 2}), (friend:Person {id: 3})
             OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post:Post)
             WHERE EXISTS { (post)-[:HAS_TAG]->(:Tag {id: 11})<-[:HAS_INTEREST]-(person) }
             RETURN friend.id, post.id"
        ),
        vec![vec![Some("3".to_owned()), None]]
    );
}

#[test]
fn simple_exists_where_correlates_with_an_outer_variable() {
    let gf = social();
    for filter in [
        "EXISTS { (post)-[:HAS_TAG]->(t:Tag) WHERE (person)-[:HAS_INTEREST]->(t) }",
        "EXISTS { (post)-[:HAS_TAG]->(t:Tag) WHERE EXISTS { (person)-[:HAS_INTEREST]->(t) } }",
        "EXISTS { MATCH (post)-[:HAS_TAG]->(t:Tag) WHERE (person)-[:HAS_INTEREST]->(t) RETURN t }",
    ] {
        assert_eq!(
            rows(
                &gf,
                &format!(
                    "MATCH (person:Person {{id: 1}}), (post:Post) WHERE {filter} \
                     RETURN count(post)"
                )
            ),
            vec![strings(&["2"])],
            "{filter}"
        );
    }
    assert_eq!(
        rows(
            &gf,
            "MATCH (person:Person {id: 1}), (post:Post) \
             WHERE EXISTS { (post)-[:HAS_TAG]->(t:Tag) WHERE t.id = person.id + 10 } \
             RETURN post.id"
        ),
        vec![strings(&["21"])]
    );
}

/// `(1)-[:K]->(2)-[:K]->(3)`.
fn chain() -> GraphForge {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (:P {id: 1})-[:K]->(:P {id: 2})-[:K]->(:P {id: 3})")
        .expect("create chain");
    gf
}

#[test]
fn a_null_outer_variable_is_evaluated_not_dropped() {
    let gf = chain();
    let prefix = "OPTIONAL MATCH (p:Nope) WITH p MATCH (f:P {id: 1})";
    assert_eq!(
        rows(
            &gf,
            &format!("{prefix} WHERE EXISTS {{ (f)-[:K]->(q) WHERE p IS NULL }} RETURN f.id")
        ),
        vec![strings(&["1"])]
    );
    assert_eq!(
        rows(
            &gf,
            &format!("{prefix} OPTIONAL MATCH (f)-[:K]->(q) WHERE p IS NULL RETURN q.id")
        ),
        vec![strings(&["2"])]
    );
    assert_eq!(
        rows(
            &gf,
            &format!(
                "{prefix} OPTIONAL MATCH (f)-[:K]->(q) \
                 WHERE EXISTS {{ (q)-[:K]->(r) WHERE p IS NULL }} RETURN q.id"
            )
        ),
        vec![strings(&["2"])]
    );
}

#[test]
fn correlated_subqueries_over_many_outer_rows() {
    let gf = chain();
    // p is node 2 on one outer row and null on the other.
    let outer = "UNWIND [2, 99] AS pid OPTIONAL MATCH (p:P {id: pid}) WITH pid, p MATCH (f:P)";
    let filter = "p IS NULL OR q <> p";
    assert_eq!(
        rows(
            &gf,
            &format!(
                "{outer} OPTIONAL MATCH (f)-[:K]->(q) WHERE {filter} \
                 RETURN pid, f.id, q.id ORDER BY pid, f.id"
            )
        ),
        vec![
            vec![Some("2".into()), Some("1".into()), None],
            vec![Some("2".into()), Some("2".into()), Some("3".into())],
            vec![Some("2".into()), Some("3".into()), None],
            vec![Some("99".into()), Some("1".into()), Some("2".into())],
            vec![Some("99".into()), Some("2".into()), Some("3".into())],
            vec![Some("99".into()), Some("3".into()), None],
        ]
    );
    assert_eq!(
        rows(
            &gf,
            &format!(
                "{outer} WHERE EXISTS {{ (f)-[:K]->(q) WHERE {filter} }} \
                 RETURN pid, f.id ORDER BY pid, f.id"
            )
        ),
        vec![
            strings(&["2", "2"]),
            strings(&["99", "1"]),
            strings(&["99", "2"]),
        ]
    );
}

#[test]
fn duplicate_outer_rows_each_keep_their_own_matches() {
    let gf = chain();
    assert_eq!(
        rows(
            &gf,
            "UNWIND [1, 1] AS d OPTIONAL MATCH (p:Nope) WITH d, p MATCH (f:P {id: 1}) \
             OPTIONAL MATCH (f)-[:K]->(q) WHERE p IS NULL RETURN count(*), count(q)"
        ),
        vec![strings(&["2", "2"])]
    );
    assert_eq!(
        rows(
            &gf,
            "UNWIND [1, 1] AS d OPTIONAL MATCH (p:Nope) WITH d, p MATCH (f:P {id: 1}) \
             WHERE EXISTS { (f)-[:K]->(q) WHERE p IS NULL } RETURN count(*)"
        ),
        vec![strings(&["2"])]
    );
}
