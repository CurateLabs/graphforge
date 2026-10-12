use std::sync::{Arc, Mutex};

use super::*;
use crate::{Binder, GraphPlan, OntologyMode, RuntimeCatalog};

fn bind(query: &str) -> GraphPlan {
    let catalog = Arc::new(Mutex::new(RuntimeCatalog::new()));
    let binder = Binder::new(None, catalog, OntologyMode::Exploratory);
    let ast = graphforge_cypher::parse(query)
        .unwrap_or_else(|error| panic!("failed to parse {query:?}: {error}"));
    binder
        .bind(&ast)
        .unwrap_or_else(|error| panic!("failed to bind {query:?}: {error:?}"))
}

/// The analysis of the first variable-length `Expand` of `query`.
fn analysis(query: &str) -> Option<ReachabilityExpansion> {
    let plan = bind(query);
    let reachability = reachability_expansions(&plan.ops, &plan.exprs);
    let expand = plan
        .ops
        .iter()
        .position(|op| matches!(op, GraphOp::Expand { .. }))
        .expect("the query has an expansion");
    reachability.expansion_at(expand).cloned()
}

fn eligible(query: &str) -> bool {
    analysis(query).is_some()
}

#[test]
fn shortest_distance_to_a_distinct_group_is_reachability() {
    // IC1: the path is read only through `length()` inside `min()`.
    assert!(eligible(
        "MATCH path = (p:Person {id: $id})-[:KNOWS*1..3]-(friend:Person {firstName: $name}) \
         WHERE NOT p = friend \
         WITH friend, min(length(path)) AS distance \
         RETURN friend.id, distance"
    ));
    assert!(eligible(
        "MATCH (a:N)-[r:T*1..4]->(b:N) RETURN b.id AS id, min(size(r)) AS d"
    ));
    assert!(eligible(
        "MATCH (a:N)-[r:T*1..4]->(b:N) RETURN min(size(r)) AS d"
    ));
}

#[test]
fn distinct_destinations_are_reachability_without_the_edge_variable() {
    assert!(eligible(
        "MATCH (a:N)-[:T*1..2]->(b:N) RETURN DISTINCT b.id AS id"
    ));
    assert!(eligible(
        "MATCH (a:N)-[:T*1..2]->(b:N) WITH DISTINCT b RETURN b.id AS id"
    ));
    // IC5/IC11: `*1..2` over an undirected relationship.
    assert!(eligible(
        "MATCH (p:N {id: $id})-[:T*1..2]-(f) WHERE NOT p = f WITH DISTINCT f RETURN f.id AS id"
    ));
    // A zero-length start, over any relationship type (TCR8/TCR11).
    assert!(eligible(
        "MATCH (s:N)-[*0..2]->(v:N) WITH DISTINCT s, v RETURN v.id AS id"
    ));
    assert!(eligible(
        "MATCH (s:N)-[:T*0..]->(v:N) WITH DISTINCT s, v RETURN v.id AS id"
    ));
}

#[test]
fn multiplicity_insensitive_aggregates_are_reachability() {
    assert!(eligible(
        "MATCH (a:N)-[:T*1..2]->(b:N) RETURN a.id AS a, count(DISTINCT b) AS c"
    ));
    assert!(eligible(
        "MATCH (a:N)-[:T*1..2]->(b:N) RETURN a.id AS a, collect(DISTINCT b.id) AS ids"
    ));
    assert!(eligible(
        "MATCH (a:N)-[:T*1..2]->(b:N) RETURN a.id AS a, max(b.id) AS m, min(b.id) AS n"
    ));
}

#[test]
fn a_later_expansion_of_the_destination_keeps_the_proof() {
    // IC3: the destination is expanded again before it is made distinct. The
    // second relationship has a different relation type, so the uniqueness
    // constraint against the first is vacuous.
    let expansion = analysis(
        "MATCH (person:N {id: $id})-[:KNOWS*1..2]-(friend)-[:LOCATED_IN]->(city) \
         WHERE NOT person = friend AND NOT city.id IN $cities \
         WITH DISTINCT friend RETURN friend.id AS id",
    )
    .expect("IC3 is reachability");
    assert_eq!(expansion.vacuous_uniqueness.len(), 1);
}

#[test]
fn the_relationship_list_is_never_reachability() {
    assert!(!eligible("MATCH (a:N)-[r:T*1..2]->(b:N) RETURN DISTINCT r"));
    assert!(!eligible(
        "MATCH (a:N)-[r:T*1..2]->(b:N) RETURN DISTINCT b.id AS id, size(r) AS d"
    ));
    assert!(!eligible(
        "MATCH (a:N)-[r:T*1..2]->(b:N) WITH b, r RETURN DISTINCT b.id AS id"
    ));
    assert!(!eligible(
        "MATCH (a:N)-[r:T*1..2]->(b:N) WHERE size(r) > 1 RETURN DISTINCT b.id AS id"
    ));
    assert!(!eligible(
        "MATCH (a:N)-[r:T*1..2]->(b:N) RETURN b.id AS id, max(length(r)) AS d"
    ));
    assert!(!eligible(
        "MATCH (a:N)-[r:T*1..2]->(b:N) RETURN b.id AS id, collect(DISTINCT length(r)) AS d"
    ));
    assert!(!eligible(
        "MATCH (a:N)-[r:T*1..2]->(b:N) RETURN b.id AS id, min(size(r) + 1) AS d"
    ));
    assert!(!eligible(
        "MATCH p = (a:N)-[:T*1..2]->(b:N) RETURN DISTINCT [n IN nodes(p) | n.id] AS ids"
    ));
    assert!(!eligible(
        "MATCH p = (a:N)-[:T*1..2]->(b:N) RETURN DISTINCT p"
    ));
}

#[test]
fn per_hop_predicates_stay_on_trails() {
    // TCR3: `ALL(e IN r WHERE …)` reads the relationship list. Pushing it into
    // the search needs the edge properties of a frontier edge by CSR position.
    assert!(!eligible(
        "MATCH (a:N {id: $a})-[r:T*1..]->(b:N {id: $b}) \
         WHERE ALL(e IN r WHERE $from < e.timestamp AND e.timestamp < $to) \
         RETURN coalesce(min(size(r)), -1) AS d"
    ));
    assert!(!eligible(
        "MATCH (a:N)-[r:T*1..2 {since: 2020}]->(b:N) RETURN DISTINCT b.id AS id"
    ));
}

#[test]
fn row_multiplicity_must_be_erased_before_it_is_observed() {
    assert!(!eligible("MATCH (a:N)-[:T*1..2]->(b:N) RETURN b.id AS id"));
    assert!(!eligible(
        "MATCH (a:N)-[:T*1..2]->(b:N) RETURN count(*) AS c"
    ));
    assert!(!eligible(
        "MATCH (a:N)-[:T*1..2]->(b:N) RETURN count(b) AS c"
    ));
    assert!(!eligible(
        "MATCH (a:N)-[:T*1..2]->(b:N) RETURN sum(b.id) AS c"
    ));
    assert!(!eligible(
        "MATCH (a:N)-[:T*1..2]->(b:N) RETURN collect(b.id) AS c"
    ));
    assert!(!eligible(
        "MATCH (a:N)-[:T*1..2]->(b:N) RETURN a.id AS a, count(DISTINCT b) AS c, count(*) AS rows"
    ));
    // LIMIT picks rows by position, so which row it keeps depends on multiplicity.
    assert!(!eligible(
        "MATCH (a:N)-[:T*1..2]->(b:N) WITH b LIMIT 5 RETURN DISTINCT b.id AS id"
    ));
    assert!(!eligible(
        "MATCH (a:N)-[:T*1..2]->(b:N) OPTIONAL MATCH (b)-[:U]->(c) RETURN DISTINCT b.id AS id"
    ));
}

#[test]
fn only_hops_the_shortest_path_decides_are_reachability() {
    // `min_hops >= 2`: the shortest trail of at least two relationships is not a
    // shortest path.
    assert!(!eligible(
        "MATCH (a:N)-[:T*2..3]->(b:N) RETURN DISTINCT b.id AS id"
    ));
    // A fixed hop is not a variable-length expansion.
    let plan = bind("MATCH (a:N)-[:T]->(b:N) RETURN DISTINCT b.id AS id");
    assert!(reachability_expansions(&plan.ops, &plan.exprs).is_empty());
}

#[test]
fn a_shared_relation_type_keeps_relationship_uniqueness() {
    // The second hop could reuse a relationship of the first: not vacuous.
    assert!(!eligible(
        "MATCH (a:N)-[:T*1..2]->(b:N)-[:T]->(c:N) RETURN DISTINCT c.id AS id"
    ));
    assert!(!eligible(
        "MATCH (a:N)-[:T*1..2]->(b:N)-[]->(c:N) RETURN DISTINCT c.id AS id"
    ));
}

#[test]
fn an_unbounded_search_for_a_pinned_destination_hoists_the_destination_scan() {
    // TCR3 without the per-hop predicate.
    let query = "MATCH (a:N {id: $a})-[r:T*1..]->(b:N {id: $b}) \
                 RETURN coalesce(min(size(r)), -1) AS d";
    let plan = bind(query);
    let reachability = reachability_expansions(&plan.ops, &plan.exprs);
    let expand = plan
        .ops
        .iter()
        .position(|op| matches!(op, GraphOp::Expand { .. }))
        .unwrap();
    let expansion = reachability.expansion_at(expand).expect("reachability");
    // The destination's `NodeScan` and its `{id: $b}` filter.
    assert_eq!(expansion.hoisted_destination_ops, 2);
    let order = reachability.lowering_order(plan.ops.len());
    assert_eq!(
        &order[expand..=expand + 2],
        &[expand + 1, expand + 2, expand]
    );
    assert!(matches!(plan.ops[order[expand]], GraphOp::NodeScan { .. }));
    assert!(matches!(
        plan.ops[order[expand + 1]],
        GraphOp::Filter { .. }
    ));
    assert!(matches!(
        plan.ops[order[expand + 2]],
        GraphOp::Expand { .. }
    ));
}

#[test]
fn a_bounded_search_or_an_unpinned_destination_keeps_the_plan_order() {
    for query in [
        "MATCH (a:N {id: $a})-[r:T*1..4]->(b:N {id: $b}) RETURN min(size(r)) AS d",
        "MATCH (a:N {id: $a})-[r:T*1..]->(b:N) RETURN min(size(r)) AS d",
        "MATCH (a:N {id: $a})-[r:T*1..]->(b:N) WHERE b.id > 3 RETURN min(size(r)) AS d",
    ] {
        let expansion = analysis(query).unwrap_or_else(|| panic!("{query} is reachability"));
        assert_eq!(expansion.hoisted_destination_ops, 0, "{query}");
    }
}

#[test]
fn lowering_order_is_plan_order_without_a_hoist() {
    let plan = bind("MATCH (a:N)-[:T*1..2]->(b:N) RETURN DISTINCT b.id AS id");
    let reachability = reachability_expansions(&plan.ops, &plan.exprs);
    assert_eq!(
        reachability.lowering_order(plan.ops.len()),
        (0..plan.ops.len()).collect::<Vec<_>>()
    );
}
