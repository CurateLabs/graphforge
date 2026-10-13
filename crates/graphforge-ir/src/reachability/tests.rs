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
    // second relationship has a different relation type, so it can never be
    // one of the trail's relationships: the binder emits no uniqueness
    // constraint between them (#1984), and nothing reads the list.
    let query = "MATCH (person:N {id: $id})-[:KNOWS*1..2]-(friend)-[:LOCATED_IN]->(city) \
                 WHERE NOT person = friend AND NOT city.id IN $cities \
                 WITH DISTINCT friend RETURN friend.id AS id";
    let plan = bind(query);
    assert!(
        !plan
            .ops
            .iter()
            .any(|op| matches!(op, GraphOp::RelationshipUnique { .. })),
        "no uniqueness constraint between relationships of different types"
    );
    let expansion = analysis(query).expect("IC3 is reachability");
    assert!(expansion.vacuous_uniqueness.is_empty());
}

#[test]
fn a_vacuous_uniqueness_constraint_keeps_the_proof() {
    // The binder no longer emits `RelationshipUnique` between relationships of
    // known, different types, so this rule only guards plans built some other
    // way. It must still recognise the constraint as vacuous.
    let mut plan = bind(
        "MATCH (person:N {id: $id})-[:KNOWS*1..2]-(friend)-[:LOCATED_IN]->(city) \
         WITH DISTINCT friend RETURN friend.id AS id",
    );
    let edges: Vec<VarId> = plan
        .ops
        .iter()
        .filter_map(|op| match op {
            GraphOp::Expand { edge, .. } => Some(*edge),
            _ => None,
        })
        .collect();
    let [trail, located] = edges[..] else {
        panic!("two expansions: {edges:?}");
    };
    let after = plan
        .ops
        .iter()
        .rposition(|op| matches!(op, GraphOp::Expand { .. }))
        .expect("the second expansion")
        + 1;
    plan.ops.insert(
        after,
        GraphOp::RelationshipUnique {
            edge: located,
            prior_edges: vec![trail],
        },
    );
    let reachability = reachability_expansions(&plan.ops, &plan.exprs);
    let expand = plan
        .ops
        .iter()
        .position(|op| matches!(op, GraphOp::Expand { .. }))
        .expect("the trail expansion");
    let expansion = reachability
        .expansion_at(expand)
        .expect("a vacuous constraint keeps the proof");
    assert_eq!(expansion.vacuous_uniqueness, vec![after]);
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

const TCR3_SHAPE: &str = "MATCH (a:N {id: $a})-[r:T*1..]->(b:N {id: $b}) \
     WHERE ALL(e IN r WHERE $from < e.timestamp AND e.timestamp < $to) \
     RETURN coalesce(min(size(r)), -1) AS d";

/// The per-relationship predicates of the first variable-length `Expand`.
fn lifted(query: &str) -> Vec<EdgePredicate> {
    analysis(query)
        .unwrap_or_else(|| panic!("{query} is reachability"))
        .edge_predicates
}

#[test]
fn per_hop_predicates_over_the_relationship_alone_are_pushed() {
    // TCR3: the window reads only the timestamp of each relationship.
    let predicates = lifted(TCR3_SHAPE);
    assert_eq!(predicates.len(), 1);
    assert_eq!(predicates[0].properties.len(), 1);

    // An inline property map on the hop binds one `ALL` per key.
    let predicates =
        lifted("MATCH (a:N)-[r:T*1..2 {since: 2020, w: 3}]->(b:N) RETURN DISTINCT b.id AS id");
    assert_eq!(predicates.len(), 2);

    // Parameters, literals, and deterministic functions are allowed.
    assert_eq!(
        lifted(
            "MATCH (a:N)-[r:T*1..3]->(b:N) \
             WHERE ALL(x IN r WHERE abs(x.w) > $m AND coalesce(x.v, 0) IN [1, 2, $k]) \
             RETURN DISTINCT b.id AS id"
        )
        .len(),
        1
    );
    // A property read twice is one property; two properties are two.
    let predicates = lifted(
        "MATCH (a:N)-[r:T*1..3]->(b:N) \
         WHERE ALL(x IN r WHERE x.w > 1 AND x.w < 9 AND x.v = 2) \
         RETURN DISTINCT b.id AS id",
    );
    assert_eq!(predicates.len(), 1);
    assert_eq!(predicates[0].properties.len(), 2);
}

#[test]
fn every_top_level_conjunct_is_lifted_on_its_own_and_the_other_filters_stay() {
    // The binder emits one filter per top-level conjunct of a `WHERE`.
    let query = "MATCH (a:N)-[r:T*1..3]->(b:N) \
                 WHERE b.id > 1 AND ALL(x IN r WHERE x.w > $m) AND ALL(y IN r WHERE y.v < $n) \
                 RETURN DISTINCT b.id AS id";
    let plan = bind(query);
    let reachability = reachability_expansions(&plan.ops, &plan.exprs);
    let expand = plan
        .ops
        .iter()
        .position(|op| matches!(op, GraphOp::Expand { .. }))
        .unwrap();
    let expansion = reachability.expansion_at(expand).expect("reachability");
    assert_eq!(expansion.edge_predicates.len(), 2);
    let mut lifted_filters = 0;
    let mut kept_filters = 0;
    for (index, op) in plan.ops.iter().enumerate() {
        let GraphOp::Filter { predicate } = op else {
            continue;
        };
        match reachability.residual_conjuncts(&plan.exprs, index, *predicate) {
            Some(residual) => {
                assert!(residual.is_empty(), "a lifted filter keeps nothing");
                lifted_filters += 1;
            }
            None => kept_filters += 1,
        }
    }
    assert_eq!((lifted_filters, kept_filters), (2, 1));
}

#[test]
fn a_filter_that_mixes_lifted_and_other_conjuncts_keeps_the_others_in_order() {
    let mut exprs = ExprArena::new();
    let lit = |exprs: &mut ExprArena, value: i64| {
        exprs.push(IrExpr::Literal(crate::IrLiteral::Int(value)))
    };
    let first = lit(&mut exprs, 1);
    let lifted = lit(&mut exprs, 2);
    let last = lit(&mut exprs, 3);
    let and = |exprs: &mut ExprArena, left, right| {
        exprs.push(IrExpr::BinaryOp {
            op: BinaryOpKind::And,
            left,
            right,
        })
    };
    let head = and(&mut exprs, first, lifted);
    let whole = and(&mut exprs, head, last);
    let reachability = ReachabilityPlan {
        expansions: BTreeMap::from([(
            0,
            ReachabilityExpansion {
                hoisted_destination_ops: 0,
                vacuous_uniqueness: Vec::new(),
                edge_predicates: vec![EdgePredicate {
                    filter_op: 5,
                    conjunct: lifted,
                    loop_var: VarId(9),
                    predicate: lifted,
                    properties: Vec::new(),
                }],
            },
        )]),
    };
    assert_eq!(conjuncts(&exprs, whole), vec![first, lifted, last]);
    assert_eq!(
        reachability.residual_conjuncts(&exprs, 5, whole),
        Some(vec![first, last])
    );
    // The same conjunct in another filter is not the lifted one.
    assert_eq!(reachability.residual_conjuncts(&exprs, 6, whole), None);
}

#[test]
fn a_filter_of_only_lifted_conjuncts_has_no_residual() {
    let plan = bind(TCR3_SHAPE);
    let reachability = reachability_expansions(&plan.ops, &plan.exprs);
    let (index, predicate) = plan
        .ops
        .iter()
        .enumerate()
        .filter_map(|(index, op)| match op {
            GraphOp::Filter { predicate } => Some((index, *predicate)),
            _ => None,
        })
        .find(|(index, predicate)| {
            reachability
                .residual_conjuncts(&plan.exprs, *index, *predicate)
                .is_some()
        })
        .expect("the window filter holds the lifted conjunct");
    assert_eq!(
        reachability.residual_conjuncts(&plan.exprs, index, predicate),
        Some(Vec::new())
    );
    // The filters that held no lifted conjunct lower unchanged.
    for (other, op) in plan.ops.iter().enumerate() {
        if let GraphOp::Filter { predicate } = op
            && other != index
        {
            assert_eq!(
                reachability.residual_conjuncts(&plan.exprs, other, *predicate),
                None
            );
        }
    }
}

#[test]
fn withdrawn_edge_predicates_are_lowered_by_their_filters_and_the_hoist_stays() {
    let plan = bind(TCR3_SHAPE);
    let mut reachability = reachability_expansions(&plan.ops, &plan.exprs);
    let expand = plan
        .ops
        .iter()
        .position(|op| matches!(op, GraphOp::Expand { .. }))
        .unwrap();
    let before = reachability.expansion_at(expand).cloned().unwrap();
    assert!(!before.edge_predicates.is_empty());

    reachability.withdraw_edge_predicates(expand);

    for (index, op) in plan.ops.iter().enumerate() {
        if let GraphOp::Filter { predicate } = op {
            assert_eq!(
                reachability.residual_conjuncts(&plan.exprs, index, *predicate),
                None,
                "filter {index} must lower unchanged once its conjunct is withdrawn"
            );
        }
    }
    let after = reachability.expansion_at(expand).unwrap();
    assert!(after.edge_predicates.is_empty());
    assert_eq!(
        after.hoisted_destination_ops,
        before.hoisted_destination_ops
    );
    assert_eq!(after.vacuous_uniqueness, before.vacuous_uniqueness);

    // Withdrawing where nothing was recorded changes nothing.
    let snapshot = reachability.clone();
    reachability.withdraw_edge_predicates(expand + 100);
    assert_eq!(reachability, snapshot);
}

#[test]
fn a_pinned_destination_is_still_hoisted_when_the_window_filter_follows_it() {
    let plan = bind(TCR3_SHAPE);
    let reachability = reachability_expansions(&plan.ops, &plan.exprs);
    let expand = plan
        .ops
        .iter()
        .position(|op| matches!(op, GraphOp::Expand { .. }))
        .unwrap();
    let expansion = reachability.expansion_at(expand).expect("reachability");
    assert_eq!(expansion.hoisted_destination_ops, 2);
}

#[test]
fn per_hop_predicates_that_read_more_than_one_relationship_stay_on_trails() {
    for query in [
        // ANY / NONE / SINGLE are not "every relationship satisfies".
        "MATCH (a:N)-[r:T*1..3]->(b:N) WHERE ANY(e IN r WHERE e.w > 1) RETURN DISTINCT b.id AS id",
        "MATCH (a:N)-[r:T*1..3]->(b:N) WHERE NONE(e IN r WHERE e.w > 1) RETURN DISTINCT b.id AS id",
        "MATCH (a:N)-[r:T*1..3]->(b:N) WHERE SINGLE(e IN r WHERE e.w > 1) RETURN DISTINCT b.id AS id",
        // A predicate on another variable.
        "MATCH (a:N)-[r:T*1..3]->(b:N) WHERE ALL(e IN r WHERE e.w > b.id) RETURN DISTINCT b.id AS id",
        "MATCH (a:N)-[r:T*1..3]->(b:N) WHERE ALL(e IN r WHERE e.w > a.id) RETURN DISTINCT b.id AS id",
        // ALL mixed with OR is not a top-level conjunct.
        "MATCH (a:N)-[r:T*1..3]->(b:N) WHERE ALL(e IN r WHERE e.w > 1) OR b.id = 2 RETURN DISTINCT b.id AS id",
        "MATCH (a:N)-[r:T*1..3]->(b:N) WHERE NOT ALL(e IN r WHERE e.w > 1) RETURN DISTINCT b.id AS id",
        // ALL over the nodes of the path.
        "MATCH p = (a:N)-[:T*1..3]->(b:N) WHERE ALL(n IN nodes(p) WHERE n.id > 1) RETURN DISTINCT b.id AS id",
        // Position-dependent predicates.
        "MATCH (a:N)-[r:T*1..3]->(b:N) WHERE ALL(e IN r WHERE e.w > size(r)) RETURN DISTINCT b.id AS id",
        "MATCH (a:N)-[r:T*1..3]->(b:N) WHERE ALL(e IN r WHERE e.w > length(r)) RETURN DISTINCT b.id AS id",
        // The relationship as a whole, a whole-entity function, or a clock.
        "MATCH (a:N)-[r:T*1..3]->(b:N) WHERE ALL(e IN r WHERE type(e) = 'T') RETURN DISTINCT b.id AS id",
        "MATCH (a:N)-[r:T*1..3]->(b:N) WHERE ALL(e IN r WHERE e.w > rand()) RETURN DISTINCT b.id AS id",
        "MATCH (a:N)-[r:T*1..3]->(b:N) WHERE ALL(e IN r WHERE e.w < timestamp()) RETURN DISTINCT b.id AS id",
        // No property of the relationship at all.
        "MATCH (a:N)-[r:T*1..3]->(b:N) WHERE ALL(e IN r WHERE $k > 1) RETURN DISTINCT b.id AS id",
        // Nested iteration.
        "MATCH (a:N)-[r:T*1..3]->(b:N) WHERE ALL(e IN r WHERE ALL(k IN [1] WHERE e.w > k)) RETURN DISTINCT b.id AS id",
        // A lifted conjunct does not excuse another mention of the list.
        "MATCH (a:N)-[r:T*1..3]->(b:N) WHERE ALL(e IN r WHERE e.w > 1) AND size(r) > 1 RETURN DISTINCT b.id AS id",
        "MATCH (a:N)-[r:T*1..3]->(b:N) WHERE ALL(e IN r WHERE e.w > 1) RETURN DISTINCT r",
    ] {
        assert!(!eligible(query), "{query} must stay on trails");
    }
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
