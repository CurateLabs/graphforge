use arrow::util::display::array_value_to_string;
use graphforge_api::{ExplainStage, GraphForge};

fn graph(statements: &[&str]) -> GraphForge {
    let graph = GraphForge::new(None).expect("in-memory graph");
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
    result
        .batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(move |row| {
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
                            array_value_to_string(column, row).expect("render value")
                        }
                    })
                    .collect()
            })
        })
        .collect()
}

#[test]
fn relationship_uniqueness_is_preserved_and_disjoint_types_skip_the_check() {
    let reuse_only = graph(&[
        "CREATE (:N {id: 'a'}), (:N {id: 'b'})",
        "MATCH (a:N {id: 'a'}), (b:N {id: 'b'}) CREATE (a)-[:K]->(b)",
    ]);
    assert!(
        rows(
            &reuse_only,
            "MATCH (a:N {id: 'a'})-[:K]->(b)-[:K]-(c) RETURN a.id, b.id, c.id"
        )
        .is_empty()
    );
    assert!(
        rows(
            &reuse_only,
            "MATCH (a:N)-[:K]-(b)-[:K]-(c) RETURN a.id, b.id, c.id"
        )
        .is_empty()
    );

    let mixed = graph(&[
        "CREATE (:Person {id: 'friend'}), (:Post {id: 'post'}), (:Forum {id: 'forum'})",
        "MATCH (friend:Person), (post:Post) CREATE (friend)<-[:HAS_CREATOR]-(post)",
        "MATCH (post:Post), (forum:Forum) CREATE (post)<-[:CONTAINER_OF]-(forum)",
        "CREATE (:Person {id: 'without-post'})",
    ]);
    assert_eq!(
        rows(
            &mixed,
            "MATCH (friend:Person) OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post)<-[:CONTAINER_OF]-(forum) RETURN friend.id, post.id, forum.id ORDER BY friend.id"
        ),
        vec![
            vec!["friend".to_owned(), "post".to_owned(), "forum".to_owned()],
            vec![
                "without-post".to_owned(),
                "null".to_owned(),
                "null".to_owned()
            ],
        ]
    );
    let physical = mixed
        .explain_stage(
            "MATCH (friend:Person) OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post)<-[:CONTAINER_OF]-(forum) RETURN friend.id, post.id, forum.id",
            ExplainStage::PhysicalPlan,
        )
        .expect("physical plan");
    assert!(
        !physical.contains("cypher_relationship_disjoint"),
        "disjoint HAS_CREATOR and CONTAINER_OF must not demand edge IDs:\n{physical}"
    );

    let same_type = graph(&[
        "CREATE (:N {id: 'a'}), (:N {id: 'b'}), (:N {id: 'c'})",
        "MATCH (a:N {id: 'a'}), (b:N {id: 'b'}) CREATE (a)-[:K]->(b)",
        "MATCH (b:N {id: 'b'}), (c:N {id: 'c'}) CREATE (b)-[:K]->(c)",
    ]);
    assert_eq!(
        rows(
            &same_type,
            "MATCH (a:N {id: 'a'})-[:K]->(b)-[:K]->(c) RETURN a.id, b.id, c.id"
        ),
        vec![vec!["a".to_owned(), "b".to_owned(), "c".to_owned()]]
    );
    let same_type_physical = same_type
        .explain_stage(
            "MATCH (a:N {id: 'a'}) OPTIONAL MATCH (a)-[:K]->(b)-[:K]->(c) RETURN a.id, b.id, c.id",
            ExplainStage::PhysicalPlan,
        )
        .expect("same-type physical plan");
    assert!(
        same_type_physical.contains("cypher_relationship_disjoint"),
        "same-type hops must retain relationship uniqueness:\n{same_type_physical}"
    );
    assert_eq!(
        rows(
            &same_type,
            "MATCH (a:N) OPTIONAL MATCH (a)-[:K]->(b)-[:K]->(c) RETURN a.id, b.id, c.id ORDER BY a.id"
        ),
        vec![
            vec!["a".to_owned(), "b".to_owned(), "c".to_owned()],
            vec!["b".to_owned(), "c".to_owned(), "null".to_owned()],
            vec!["c".to_owned(), "null".to_owned(), "null".to_owned()],
        ]
    );
}
