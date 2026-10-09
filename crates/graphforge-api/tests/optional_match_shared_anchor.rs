//! Shared-node OPTIONAL patterns are seeded from the actual outer rows.

use arrow::array::Array;
use arrow::util::display::array_value_to_string;
use graphforge_api::GraphForge;

fn graph() -> GraphForge {
    let graph = GraphForge::new(None).unwrap();
    for query in [
        "CREATE (:Person {id:1}), (:Person {id:2}), (:Person {id:3}), (:Other {id:4})",
        "MATCH (a:Person {id:1}), (b:Person {id:2}) CREATE (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b)",
        "CREATE (:University {id:10, name:'U'}), (:Company {id:20})",
        "MATCH (p:Person {id:1}), (u:University) CREATE (p)-[:STUDY]->(u)",
        "MATCH (p:Person {id:1}), (c:Company) CREATE (p)-[:WORK]->(c)",
    ] {
        graph
            .execute(query)
            .unwrap_or_else(|error| panic!("{query}: {error}"));
    }
    graph
}

fn rows(graph: &GraphForge, query: &str) -> Vec<Vec<String>> {
    let result = graph
        .execute(query)
        .unwrap_or_else(|error| panic!("{query}: {error}"));
    let mut rows = Vec::new();
    for batch in result.batches {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| {
                        if column.is_null(row) {
                            "null".to_owned()
                        } else {
                            array_value_to_string(column, row).unwrap()
                        }
                    })
                    .collect(),
            );
        }
    }
    rows
}

#[test]
fn shared_optional_keeps_duplicate_rows_relationships_and_unmatched_nodes() {
    let graph = graph();
    assert_eq!(
        rows(
            &graph,
            "UNWIND [1,1,3] AS pid MATCH (p:Person {id:pid})
         OPTIONAL MATCH (p)-[:KNOWS]->(q)
         RETURN pid, p.id, q.id ORDER BY pid, q.id"
        ),
        vec![
            vec!["1", "1", "2"],
            vec!["1", "1", "2"],
            vec!["1", "1", "2"],
            vec!["1", "1", "2"],
            vec!["3", "3", "null"]
        ]
    );
}

#[test]
fn shared_optional_keeps_a_null_node_from_an_earlier_optional() {
    let graph = graph();
    assert_eq!(
        rows(
            &graph,
            "UNWIND [1,99] AS pid OPTIONAL MATCH (p:Person {id:pid})
         WITH pid, p OPTIONAL MATCH (p)-[:WORK]->(c)
         RETURN pid, p.id, c.id ORDER BY pid"
        ),
        vec![vec!["1", "1", "20"], vec!["99", "null", "null"]]
    );
}

#[test]
fn shared_optional_preserves_the_rematched_nodes_label_constraint() {
    let graph = graph();
    assert_eq!(
        rows(
            &graph,
            "MATCH (p:Person {id:1}) OPTIONAL MATCH (p:Other)-[:KNOWS]->(q)
         RETURN p.id, q.id"
        ),
        vec![vec!["1", "null"]]
    );
}

#[test]
fn shared_optional_joins_back_on_an_outer_list_of_structs_with_null_fields() {
    let graph = graph();
    assert_eq!(
        rows(
            &graph,
            "MATCH (p:Person) WITH p ORDER BY p.id LIMIT 3
         OPTIONAL MATCH (p)-[:STUDY]->(u)
         WITH p, collect({id:u.id, name:u.name}) AS universities
         OPTIONAL MATCH (p)-[:WORK]->(c)
         RETURN p.id, size(universities), c.id ORDER BY p.id"
        ),
        vec![
            vec!["1", "1", "20"],
            vec!["2", "1", "null"],
            vec!["3", "1", "null"]
        ]
    );
}
