//! Binder-level coverage for the extremum re-match rewrite.
//!
//! Each test binds a two-stage query and asks whether the pass fired, which it
//! shows by the synthetic `collect` it adds to the first aggregate. The
//! negative tests are the contract: every way the second stage can differ from
//! "the first stage's rows with `e = t`" must leave the plan untouched.

use super::super::tests::make_binder;
use crate::expr::IrExpr;
use crate::plan::{GraphOp, GraphPlan, OntologyMode};
use crate::{AggFunc, ExprId};
use graphforge_cypher::parse;

const FIRST: &str = "MATCH (a:P)<-[:OWNS]-(m)<-[r:LIKES]-(b:Q) \
                     WHERE a.id = $a AND (m:X OR m:Y) \
                     WITH a, b, max(r.w) AS t ";
const TAIL: &str = "WITH a, b, t, min(m.id) AS latest RETURN a.id, b.id, t, latest";

fn bind(query: &str) -> GraphPlan {
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    binder
        .bind(&parse(query).unwrap_or_else(|e| panic!("parse `{query}`: {e:?}")))
        .unwrap_or_else(|e| panic!("bind `{query}`: {e:?}"))
}

fn rematch_collects(plan: &GraphPlan) -> usize {
    plan.ops
        .iter()
        .map(|op| match op {
            GraphOp::Aggregate { aggs, .. } => aggs
                .iter()
                .filter(|agg| {
                    agg.func == AggFunc::Collect && agg.alias.starts_with("_extremum_rematch_")
                })
                .count(),
            _ => 0,
        })
        .sum()
}

fn expands(plan: &GraphPlan) -> usize {
    plan.ops
        .iter()
        .filter(|op| matches!(op, GraphOp::Expand { .. }))
        .count()
}

fn two_stage(second: &str) -> GraphPlan {
    bind(&format!("{FIRST}{second} {TAIL}"))
}

fn assert_rewritten(second: &str) {
    let plan = two_stage(second);
    assert_eq!(rematch_collects(&plan), 1, "expected a rewrite: {second}");
    assert_eq!(expands(&plan), 2, "one expansion per hop: {second}");
}

fn assert_untouched(second: &str) {
    let plan = two_stage(second);
    assert_eq!(rematch_collects(&plan), 0, "unexpected rewrite: {second}");
    assert!(
        expands(&plan) >= 4,
        "second stage must still expand: {second}"
    );
}

#[test]
fn the_same_pattern_with_the_extremum_conjunct_is_rewritten() {
    assert_rewritten("MATCH (a)<-[:OWNS]-(m)<-[r:LIKES]-(b) WHERE (m:X OR m:Y) AND r.w = t");
}

#[test]
fn the_extremum_may_be_written_on_either_side() {
    assert_rewritten("MATCH (a)<-[:OWNS]-(m)<-[r:LIKES]-(b) WHERE t = r.w AND (m:X OR m:Y)");
}

#[test]
fn second_stage_variables_may_be_renamed() {
    let plan = bind(&format!(
        "{FIRST}MATCH (a)<-[:OWNS]-(msg)<-[lk:LIKES]-(b) \
         WHERE (msg:X OR msg:Y) AND lk.w = t \
         WITH a, b, t, min(msg.id) AS latest RETURN a.id, b.id, t, latest"
    ));
    assert_eq!(rematch_collects(&plan), 1);
    assert_eq!(expands(&plan), 2);
}

#[test]
fn a_carried_variable_may_drop_its_label_but_not_gain_one() {
    // `P` and `Q` were proven by the first stage.
    assert_rewritten("MATCH (a:P)<-[:OWNS]-(m)<-[r:LIKES]-(b:Q) WHERE (m:X OR m:Y) AND r.w = t");
    assert_untouched("MATCH (a:Other)<-[:OWNS]-(m)<-[r:LIKES]-(b) WHERE (m:X OR m:Y) AND r.w = t");
}

#[test]
fn the_rewrite_keeps_one_pass_and_reads_the_collected_rows() {
    let plan = two_stage("MATCH (a)<-[:OWNS]-(m)<-[r:LIKES]-(b) WHERE (m:X OR m:Y) AND r.w = t");
    let kinds: Vec<&str> = plan
        .ops
        .iter()
        .skip_while(|op| !matches!(op, GraphOp::Aggregate { .. }))
        .map(|op| match op {
            GraphOp::Aggregate { .. } => "Aggregate",
            GraphOp::Unwind { .. } => "Unwind",
            GraphOp::Filter { .. } => "Filter",
            GraphOp::Project { .. } => "Project",
            _ => "other",
        })
        .collect();
    assert_eq!(
        kinds,
        ["Aggregate", "Unwind", "Filter", "Aggregate", "Project"]
    );
    let GraphOp::Aggregate { aggs, .. } = plan
        .ops
        .iter()
        .find(|op| matches!(op, GraphOp::Aggregate { .. }))
        .expect("first aggregate")
    else {
        unreachable!()
    };
    assert_eq!(aggs.len(), 2);
    assert_eq!(aggs[0].func, AggFunc::Max);
    let collected = aggs[1].arg.expect("collect argument");
    let IrExpr::MapLiteral(fields) = plan.exprs.get(collected) else {
        panic!("collect a map of the extremum and the tie-break");
    };
    let keys: Vec<&str> = fields.iter().map(|(key, _)| key.as_str()).collect();
    assert_eq!(keys, ["w", "id"]);
    // The max argument is shared, not recomputed.
    assert_eq!(aggs[0].arg, Some(fields[0].1));
    let _: ExprId = fields[1].1;
}

#[test]
fn an_extra_second_stage_conjunct_is_not_rewritten() {
    assert_untouched(
        "MATCH (a)<-[:OWNS]-(m)<-[r:LIKES]-(b) WHERE (m:X OR m:Y) AND r.w = t AND m.id > 3",
    );
}

#[test]
fn a_missing_second_stage_conjunct_is_not_rewritten() {
    // Without `m:X OR m:Y` the second stage reads rows the first excluded.
    assert_untouched("MATCH (a)<-[:OWNS]-(m)<-[r:LIKES]-(b) WHERE r.w = t");
}

#[test]
fn a_missing_extremum_conjunct_is_not_rewritten() {
    assert_untouched("MATCH (a)<-[:OWNS]-(m)<-[r:LIKES]-(b) WHERE (m:X OR m:Y)");
}

#[test]
fn an_extremum_over_another_property_is_not_rewritten() {
    assert_untouched("MATCH (a)<-[:OWNS]-(m)<-[r:LIKES]-(b) WHERE (m:X OR m:Y) AND r.other = t");
}

#[test]
fn a_different_relationship_type_is_not_rewritten() {
    assert_untouched("MATCH (a)<-[:OWNS]-(m)<-[r:SAVES]-(b) WHERE (m:X OR m:Y) AND r.w = t");
}

#[test]
fn a_different_direction_is_not_rewritten() {
    assert_untouched("MATCH (a)<-[:OWNS]-(m)-[r:LIKES]->(b) WHERE (m:X OR m:Y) AND r.w = t");
}

#[test]
fn a_different_pattern_shape_is_not_rewritten() {
    assert_untouched(
        "MATCH (a)<-[:OWNS]-(m)<-[r:LIKES]-(b)<-[:KNOWS]-(c) \
         WHERE (m:X OR m:Y) AND r.w = t",
    );
}

#[test]
fn a_different_label_on_an_interior_node_is_not_rewritten() {
    assert_untouched("MATCH (a)<-[:OWNS]-(m:X)<-[r:LIKES]-(b) WHERE (m:X OR m:Y) AND r.w = t");
}

#[test]
fn a_first_stage_filter_on_an_interior_node_is_not_dropped() {
    // The second stage lacks `m.flag = 1`, so it reads more rows.
    let plan = bind(
        "MATCH (a:P)<-[:OWNS]-(m)<-[r:LIKES]-(b:Q) WHERE m.flag = 1 \
         WITH a, b, max(r.w) AS t \
         MATCH (a)<-[:OWNS]-(m)<-[r:LIKES]-(b) WHERE r.w = t \
         WITH a, b, t, min(m.id) AS latest RETURN latest",
    );
    assert_eq!(rematch_collects(&plan), 0);
}

#[test]
fn a_volatile_function_in_both_stages_is_not_rewritten() {
    let plan = bind(
        "MATCH (a:P)<-[:OWNS]-(m)<-[r:LIKES]-(b:Q) WHERE rand() < 2 \
         WITH a, b, max(r.w) AS t \
         MATCH (a)<-[:OWNS]-(m)<-[r:LIKES]-(b) WHERE rand() < 2 AND r.w = t \
         WITH a, b, t, min(m.id) AS latest RETURN latest",
    );
    assert_eq!(rematch_collects(&plan), 0);
}

#[test]
fn the_opposite_extremum_direction_is_not_rewritten() {
    // min then min, max then max, and min then max all match different rows.
    for (first, second) in [
        ("min(r.w)", "min(m.id)"),
        ("max(r.w)", "max(m.id)"),
        ("min(r.w)", "max(m.id)"),
    ] {
        let plan = bind(&format!(
            "MATCH (a:P)<-[:OWNS]-(m)<-[r:LIKES]-(b:Q) WITH a, b, {first} AS t \
             MATCH (a)<-[:OWNS]-(m)<-[r:LIKES]-(b) WHERE r.w = t \
             WITH a, b, t, {second} AS latest RETURN latest"
        ));
        assert_eq!(rematch_collects(&plan), 0, "{first} then {second}");
    }
}

#[test]
fn a_barrier_between_the_stages_is_not_rewritten() {
    for barrier in ["ORDER BY t LIMIT 5", "ORDER BY t", "WHERE t > 1"] {
        let plan = bind(&format!(
            "MATCH (a:P)<-[:OWNS]-(m)<-[r:LIKES]-(b:Q) WITH a, b, max(r.w) AS t {barrier} \
             MATCH (a)<-[:OWNS]-(m)<-[r:LIKES]-(b) WHERE r.w = t \
             WITH a, b, t, min(m.id) AS latest RETURN latest"
        ));
        assert_eq!(rematch_collects(&plan), 0, "{barrier}");
    }
}

#[test]
fn a_computed_grouping_key_is_not_rewritten() {
    let plan = bind(
        "MATCH (a:P)<-[:OWNS]-(m)<-[r:LIKES]-(b:Q) WITH a, b.name AS who, max(r.w) AS t \
         MATCH (a)<-[:OWNS]-(m)<-[r:LIKES]-(b) WHERE r.w = t \
         WITH a, t, min(m.id) AS latest RETURN latest",
    );
    assert_eq!(rematch_collects(&plan), 0);
}

#[test]
fn a_first_stage_with_other_aggregates_is_not_rewritten() {
    let plan = bind(
        "MATCH (a:P)<-[:OWNS]-(m)<-[r:LIKES]-(b:Q) WITH a, b, max(r.w) AS t, count(*) AS n \
         MATCH (a)<-[:OWNS]-(m)<-[r:LIKES]-(b) WHERE r.w = t \
         WITH a, b, t, min(m.id) AS latest RETURN latest",
    );
    assert_eq!(rematch_collects(&plan), 0);
}

#[test]
fn a_property_name_shared_by_the_extremum_and_the_tie_break_is_not_rewritten() {
    // Both would be map keys `w`; the pass refuses rather than collide.
    let plan = bind(
        "MATCH (a:P)<-[:OWNS]-(m)<-[r:LIKES]-(b:Q) WITH a, b, max(r.w) AS t \
         MATCH (a)<-[:OWNS]-(m)<-[r:LIKES]-(b) WHERE r.w = t \
         WITH a, b, t, min(m.w) AS latest RETURN latest",
    );
    assert_eq!(rematch_collects(&plan), 0);
}

#[test]
fn a_query_with_one_stage_is_untouched() {
    let plan = bind("MATCH (a:P)<-[:OWNS]-(m) WITH a, max(m.w) AS t RETURN a.id, t");
    assert_eq!(rematch_collects(&plan), 0);
}

#[test]
fn a_user_extremum_alias_cannot_shadow_the_collected_rows() {
    let query = "MATCH (a:P)-[r:R]->(m) WITH a, max(r.w) AS t \
        MATCH (a)-[r:R]->(m) WHERE r.w = t \
        WITH a, min(m.id) AS latest RETURN latest";
    let baseline = bind(query);
    let collect_alias = baseline
        .ops
        .iter()
        .find_map(|op| {
            let GraphOp::Aggregate { aggs, .. } = op else {
                return None;
            };
            aggs.iter()
                .find(|agg| agg.func == AggFunc::Collect)
                .map(|agg| agg.alias.clone())
        })
        .expect("the baseline rewrites");
    let collision = query
        .replace(" AS t", &format!(" AS {collect_alias}"))
        .replace("r.w = t", &format!("r.w = {collect_alias}"));
    let plan = bind(&collision);
    assert_eq!(
        rematch_collects(&plan),
        1,
        "the exact same query still rewrites"
    );
    let GraphOp::Aggregate { aggs, .. } = plan
        .ops
        .iter()
        .find(|op| matches!(op, GraphOp::Aggregate { .. }))
        .unwrap()
    else {
        unreachable!()
    };
    assert_eq!(aggs[0].alias, collect_alias);
    assert_ne!(
        aggs[0].alias, aggs[1].alias,
        "physical aggregate columns must be distinct"
    );
}
