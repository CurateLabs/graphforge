//! openCypher relationship isomorphism across the comma-separated patterns of
//! one MATCH clause (#1887 D6): no relationship binds two pattern positions of
//! the same clause, while separate clauses stay independent.

use arrow::array::{Array, Int64Array};
use graphforge_api::GraphForge;

fn count(gf: &GraphForge, query: &str) -> i64 {
    let result = gf
        .execute(query)
        .unwrap_or_else(|error| panic!("{query}: {error}"));
    let column = result.batches[0].column(0);
    let values = column
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap_or_else(|| panic!("{query}: count is {:?}", column.data_type()));
    assert_eq!(values.len(), 1, "{query}");
    values.value(0)
}

/// `(1)-[:K]->(2)-[:K]->(3)`: two relationships.
fn chain() -> GraphForge {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (a:P {id: 1})-[:K]->(b:P {id: 2}), (b)-[:K]->(c:P {id: 3})")
        .expect("create chain");
    gf
}

#[test]
fn disjoint_comma_patterns_never_bind_one_relationship_twice() {
    let gf = chain();
    // 2 x 2 pairs minus the 2 that reuse a relationship.
    assert_eq!(
        count(
            &gf,
            "MATCH (x)-[r1:K]->(y), (u)-[r2:K]->(v) RETURN count(*)"
        ),
        2
    );
    assert_eq!(
        count(
            &gf,
            "MATCH (x)-[r1:K]->(y), (u)-[r2:K]->(v) WHERE r1 = r2 RETURN count(*)"
        ),
        0
    );
    // Anonymous relationships are constrained too.
    assert_eq!(
        count(&gf, "MATCH ()-[:K]->(), ()-[:K]->() RETURN count(*)"),
        2
    );
    // Three patterns over two relationships cannot all be distinct.
    assert_eq!(
        count(
            &gf,
            "MATCH ()-[a:K]->(), ()-[b:K]->(), ()-[c:K]->() RETURN count(*)"
        ),
        0
    );
}

#[test]
fn comma_patterns_sharing_nodes_keep_relationships_distinct() {
    let gf = chain();
    // Only 1->2->3 continues; an undirected second pattern from the shared
    // middle node may not walk back over the first relationship.
    assert_eq!(
        count(
            &gf,
            "MATCH (x)-[r1:K]->(y), (y)-[r2:K]->(z) RETURN count(*)"
        ),
        1
    );
    assert_eq!(
        count(
            &gf,
            "MATCH (x:P {id: 1})-[r1:K]->(y), (y)-[r2:K]-(z) RETURN count(*)"
        ),
        1
    );
    // Undirected patterns: each relationship matches in both directions.
    assert_eq!(
        count(&gf, "MATCH (x)-[r1:K]-(y), (u)-[r2:K]-(v) RETURN count(*)"),
        8
    );
}

#[test]
fn variable_length_patterns_exclude_relationships_bound_by_other_patterns() {
    let gf = chain();
    // Paths of 1..2 hops: [1->2], [2->3], [1->2->3]; with r2 outside each path
    // only the two single-hop paths survive, one r2 each.
    assert_eq!(
        count(
            &gf,
            "MATCH (x)-[rs:K*1..2]->(y), (u)-[r2:K]->(v) RETURN count(*)"
        ),
        2
    );
}

#[test]
fn separate_match_clauses_do_not_constrain_each_other() {
    let gf = chain();
    assert_eq!(
        count(
            &gf,
            "MATCH (x)-[r1:K]->(y) MATCH (u)-[r2:K]->(v) RETURN count(*)"
        ),
        4
    );
}

#[test]
fn optional_match_comma_patterns_are_isomorphic_among_themselves() {
    let gf = chain();
    assert_eq!(
        count(
            &gf,
            "MATCH (s:P {id: 1}) OPTIONAL MATCH (x)-[r1:K]->(y), (u)-[r2:K]->(v) \
             RETURN count(r2)"
        ),
        2
    );
}

#[test]
fn reusing_a_relationship_variable_across_patterns_of_one_match_is_refused() {
    let gf = chain();
    let error = gf
        .execute("MATCH (a)-[r]->(b), (b)-[r]->(c) RETURN a")
        .expect_err("one relationship variable cannot bind two pattern positions");
    assert!(
        error
            .to_string()
            .contains("RelationshipUniquenessViolation"),
        "{error}"
    );
}
