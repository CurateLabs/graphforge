//! Grouping and `DISTINCT` over a node variable compare node identity (#1985).
//!
//! A node variable spans its `node_uuid`, topology columns and every property
//! column, all determined by the identity. Two nodes with equal properties are
//! still two nodes, and the plan groups by the identity plus only the columns
//! the rest of the query reads, so a variable with many unread properties does
//! not become a wide group key that every operator above carries.

use arrow::util::display::array_value_to_string;
use graphforge_api::GraphForge;

fn graph() -> GraphForge {
    let graph = GraphForge::new(None).expect("in-memory instance");
    // Two distinct people with equal properties, and one more, who all know
    // someone. `city` and `born` are never read by the queries below.
    graph
        .execute(
            "CREATE (ann1:Person {name: 'Ann', city: 'Oslo', born: 1980}), \
                    (ann2:Person {name: 'Ann', city: 'Oslo', born: 1980}), \
                    (bob:Person {name: 'Bob', city: 'Rome', born: 1990}), \
                    (x:Person {name: 'X'}), (y:Person {name: 'Y'}), \
                    (ann1)-[:KNOWS]->(x), (ann1)-[:KNOWS]->(y), \
                    (ann2)-[:KNOWS]->(x), (bob)-[:KNOWS]->(y)",
        )
        .expect("create the people");
    graph
}

fn rows(graph: &GraphForge, query: &str) -> Vec<String> {
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
                    .map(|column| array_value_to_string(column, row).expect("render"))
                    .collect::<Vec<_>>()
                    .join(","),
            );
        }
    }
    rows.sort();
    rows
}

#[test]
fn equal_nodes_stay_separate_groups() {
    let graph = graph();
    // Ann knows two people, the other Ann one: three groups would be two if
    // the grouping merged nodes by their property values.
    assert_eq!(
        rows(
            &graph,
            "MATCH (p:Person)-[:KNOWS]->(q) WITH p, count(q) AS known \
             RETURN p.name AS name, known",
        ),
        ["Ann,1", "Ann,2", "Bob,1"]
    );
    assert_eq!(
        rows(
            &graph,
            "MATCH (p:Person) WITH DISTINCT p RETURN p.name AS name",
        ),
        ["Ann", "Ann", "Bob", "X", "Y"]
    );
    assert_eq!(
        rows(
            &graph,
            "MATCH (p:Person)-[:KNOWS]->(q) WITH p, q, count(*) AS paths \
             RETURN p.name AS name, q.name AS other, paths",
        ),
        ["Ann,X,1", "Ann,X,1", "Ann,Y,1", "Bob,Y,1"]
    );
}

#[test]
fn a_grouped_node_variable_is_keyed_by_identity_and_the_columns_read_above() {
    let graph = graph();
    let plan = graph
        .explain(
            "MATCH (p:Person)-[:KNOWS]->(q) WITH p, count(q) AS known \
             RETURN p.name AS name, known",
        )
        .unwrap();
    let aggregates = plan
        .lines()
        .filter(|line| line.trim_start().starts_with("AggregateExec"))
        .collect::<Vec<_>>();
    assert!(!aggregates.is_empty(), "{plan}");
    for aggregate in aggregates {
        assert!(aggregate.contains("node_uuid"), "{aggregate}");
        // Unread properties are neither group keys nor carried at all.
        for unread in ["city", "born"] {
            assert!(!aggregate.contains(unread), "{unread} in {aggregate}");
        }
    }
}
