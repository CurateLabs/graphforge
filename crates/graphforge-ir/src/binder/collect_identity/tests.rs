//! Binder-level coverage for the identity-only `collect` narrowing.
//!
//! Each test binds a query and reports the shape of every top-level
//! `collect(...)` argument: `Identity` (a bare node variable, which lowers to
//! the node's `node_uuid`), `Whole` (the full `_node_struct` node value), or
//! `Other` (not a node collect at all). The disqualifying-use tests are the
//! contract: any use that can read more than a node's identity must leave the
//! collect whole.

use super::super::tests::make_binder;
use super::NODE_STRUCT_CALL;
use crate::expr::IrExpr;
use crate::plan::{GraphOp, GraphPlan, OntologyMode};
use crate::{
    AggExpr, AggFunc, BinaryOpKind, Direction, ExprId, GraphPlanBuilder, IrLiteral,
    ProcedureDefinition, ProcedureField, ProcedureYield, ProjectItem, QuantifierKind,
    RelationTypeId, TypeId, VarId,
};
use graphforge_cypher::parse;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Collected {
    Identity,
    Whole,
    Other,
}
use Collected::{Identity, Other, Whole};

fn bind(query: &str) -> GraphPlan {
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    binder
        .bind(&parse(query).unwrap_or_else(|e| panic!("parse `{query}`: {e:?}")))
        .unwrap_or_else(|e| panic!("bind `{query}`: {e:?}"))
}

fn shape(plan: &GraphPlan, agg: &AggExpr) -> Collected {
    let arg = agg.arg.expect("collect has an argument");
    match plan.exprs.get(arg) {
        IrExpr::VarRef(_) => Identity,
        IrExpr::FunctionCall { name, .. } if name == NODE_STRUCT_CALL => Whole,
        _ => Other,
    }
}

fn walk(plan: &GraphPlan, out: &mut Vec<Collected>) {
    for op in &plan.ops {
        match op {
            GraphOp::Aggregate { aggs, .. } => {
                for agg in aggs {
                    if matches!(agg.func, AggFunc::Collect | AggFunc::CollectDistinct) {
                        out.push(shape(plan, agg));
                    }
                }
            }
            GraphOp::Union { inputs, .. } => {
                for input in inputs {
                    walk(input, out);
                }
            }
            _ => {}
        }
    }
}

/// The shape of every top-level `collect` in plan order (UNION branches in
/// branch order).
fn collected(query: &str) -> Vec<Collected> {
    let plan = bind(query);
    let mut out = Vec::new();
    walk(&plan, &mut out);
    out
}

/// A forum with its members collected into `ps`, followed by `rest`.
fn over_members(rest: &str) -> String {
    format!("MATCH (f:Forum)-[:HAS_MEMBER]->(p:Person) WITH f, collect(p) AS ps {rest}")
}

#[track_caller]
fn assert_collected(query: &str, expected: &[Collected]) {
    assert_eq!(collected(query), expected, "query: {query}");
}

#[track_caller]
fn assert_members(rest: &str, expected: &[Collected]) {
    assert_collected(&over_members(rest), expected);
}

/// `rest` follows an identity use of `ps` (`witness IN ps`, handled by the
/// membership branch and independent of how `rest` is walked), so the collect
/// can only stay whole because of `rest` itself. Without the identity use a
/// "stays whole" assertion would hold even if the pass ignored `rest`.
#[track_caller]
fn assert_whole(rest: &str) {
    assert_members(
        &format!("MATCH (witness:Person) WHERE witness IN ps {rest}"),
        &[Whole],
    );
}

/// `predicate` is the `WHERE` of a filter and the only identity use of `ps` is
/// a later `size(ps)`, so the collect can only stay whole because the filter
/// predicate is walked.
#[track_caller]
fn assert_whole_in_filter(predicate: &str) {
    assert_members(
        &format!("MATCH (q:Person) WHERE {predicate} RETURN size(ps) AS n"),
        &[Whole],
    );
}

// ---------------------------------------------------------------------------
// Identity-only uses narrow the collect.
// ---------------------------------------------------------------------------

#[test]
fn size_of_the_list_is_identity_only() {
    assert_members("RETURN f.id AS f, size(ps) AS n", &[Identity]);
}

#[test]
fn size_is_case_insensitive() {
    assert_members("RETURN f.id AS f, SIZE(ps) AS n", &[Identity]);
}

#[test]
fn node_in_list_is_identity_only() {
    assert_members(
        "MATCH (q:Person) WHERE q IN ps RETURN f.id AS f, q.id AS q",
        &[Identity],
    );
}

#[test]
fn node_not_in_list_is_identity_only() {
    assert_members(
        "MATCH (q:Person) WHERE NOT q IN ps RETURN f.id AS f, q.id AS q",
        &[Identity],
    );
}

#[test]
fn count_of_the_list_is_identity_only() {
    assert_members("RETURN count(ps) AS n", &[Identity]);
}

#[test]
fn optional_match_membership_is_identity_only() {
    // The shape of the SNB Interactive IC5 tail: `friend` is bound inside the
    // OPTIONAL MATCH child plan and tested against the collected list.
    assert_members(
        "OPTIONAL MATCH (q:Person)<-[:HAS_CREATOR]-(post:Post)<-[:CONTAINER_OF]-(f) \
         WHERE q IN ps \
         WITH f, count(post) AS c RETURN f.title AS title, c ORDER BY c DESC LIMIT 20",
        &[Identity],
    );
}

#[test]
fn node_reached_through_a_projection_is_still_a_node() {
    assert_collected(
        "MATCH (q:Person) WITH q AS x \
         MATCH (f:Forum)-[:HAS_MEMBER]->(p:Person) \
         WITH x, f, collect(p) AS ps WHERE x IN ps RETURN x.id AS id",
        &[Identity],
    );
}

#[test]
fn expand_endpoint_is_a_node() {
    assert_members(
        "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE b IN ps RETURN a.id AS a",
        &[Identity],
    );
}

#[test]
fn several_identity_uses_of_one_list_narrow_it() {
    assert_members(
        "MATCH (q:Person) WHERE q IN ps AND size(ps) > 1 RETURN f.id AS f, size(ps) AS n",
        &[Identity],
    );
}

#[test]
fn each_collect_is_judged_by_its_own_uses() {
    assert_collected(
        "MATCH (f:Forum)-[:HAS_MEMBER]->(p:Person) \
         WITH f, collect(p) AS a, collect(p) AS b RETURN size(a) AS n, b",
        &[Identity, Whole],
    );
}

#[test]
fn nested_collect_inside_size_narrows_while_the_returned_one_stays_whole() {
    assert_collected(
        "MATCH (p:Person) RETURN size(collect(p)) AS b, collect(p) AS ps",
        &[Identity, Whole],
    );
}

#[test]
fn union_branches_are_judged_independently() {
    let narrow = "MATCH (f:Forum)-[:HAS_MEMBER]->(p:Person) WITH f, collect(p) AS ps \
                  RETURN f.id AS k, size(ps) AS v";
    let whole = "MATCH (f:Forum)-[:HAS_MEMBER]->(p:Person) WITH f, collect(p) AS ps \
                 RETURN f.id AS k, ps AS v";
    assert_collected(&format!("{narrow} UNION ALL {whole}"), &[Identity, Whole]);
    assert_collected(&format!("{whole} UNION ALL {narrow}"), &[Whole, Identity]);
}

// ---------------------------------------------------------------------------
// Every use that can read more than identity keeps the full node value.
// ---------------------------------------------------------------------------

#[test]
fn unwind_keeps_the_whole_node() {
    assert_whole("UNWIND ps AS m RETURN m.name AS name");
}

#[test]
fn unwind_keeps_the_whole_node_even_beside_an_identity_use() {
    assert_whole("UNWIND ps AS m RETURN m.name AS name, size(ps) AS n");
}

#[test]
fn unwind_of_the_node_itself_keeps_the_whole_node() {
    assert_whole("UNWIND ps AS m RETURN m");
}

#[test]
fn list_comprehension_keeps_the_whole_node() {
    assert_whole("RETURN [x IN ps | x.name] AS names");
    assert_whole("RETURN [x IN ps WHERE x.id > 1 | x.id] AS ids");
}

#[test]
fn quantifier_keeps_the_whole_node() {
    assert_whole("RETURN any(x IN ps WHERE x.id = 1) AS hit");
    assert_whole("RETURN all(x IN ps WHERE x.id = 1) AS hit");
}

#[test]
fn returning_the_list_keeps_the_whole_node() {
    assert_whole("RETURN f.id AS f, ps");
}

#[test]
fn returning_the_list_beside_an_identity_use_keeps_the_whole_node() {
    assert_whole("RETURN size(ps) AS n, ps");
}

#[test]
fn returning_a_wrapping_of_the_list_keeps_the_whole_node() {
    assert_whole("RETURN [ps] AS nested");
    assert_whole("RETURN {members: ps} AS m");
    assert_whole("RETURN ps + ps AS both");
}

#[test]
fn forwarding_the_list_through_with_keeps_the_whole_node() {
    assert_whole("WITH f, ps RETURN f.id AS f, size(ps) AS n");
    assert_whole("WITH f, ps AS qs RETURN f.id AS f, size(qs) AS n");
    assert_whole("WITH f, ps AS qs WITH f, qs AS rs RETURN f.id AS f, size(rs) AS n");
}

#[test]
fn star_projection_keeps_the_whole_node() {
    assert_whole("RETURN *");
    assert_whole("WITH * RETURN f.id AS f, size(ps) AS n");
}

#[test]
fn indexing_and_property_access_keep_the_whole_node() {
    assert_whole("RETURN ps[0] AS first");
    assert_whole("RETURN ps[0].name AS name");
    assert_whole("RETURN ps[1..] AS rest");
    assert_whole("RETURN head(ps) AS first");
    assert_whole("RETURN head(ps).name AS name");
    assert_whole("RETURN last(ps) AS final");
}

#[test]
fn comparing_the_list_keeps_the_whole_node() {
    assert_whole("RETURN ps = [] AS none");
    assert_whole("RETURN ps = ps AS same");
}

#[test]
fn other_list_functions_keep_the_whole_node() {
    assert_whole("RETURN reverse(ps) AS r");
    assert_whole("RETURN coalesce(ps, []) AS r");
    assert_whole("RETURN tail(ps) AS r");
}

#[test]
fn case_returning_the_list_keeps_the_whole_node() {
    assert_whole("RETURN CASE WHEN size(ps) > 0 THEN ps ELSE [] END AS r");
}

#[test]
fn list_as_the_left_operand_of_in_keeps_the_whole_node() {
    assert_whole("RETURN ps IN [ps] AS hit");
}

#[test]
fn list_inside_the_right_operand_of_in_keeps_the_whole_node() {
    assert_whole("MATCH (q:Person) WHERE q IN ps + ps RETURN q.id AS q");
    assert_whole("MATCH (q:Person) WHERE q IN head([ps]) RETURN q.id AS q");
}

#[test]
fn membership_of_a_non_node_keeps_the_whole_node() {
    // An UNWIND alias, an edge, a property and a literal are not provably
    // nodes, so their comparison against the list cannot be done on the
    // identity-only representation.
    assert_whole("UNWIND [1, 2] AS x RETURN x IN ps AS hit");
    assert_whole("MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r IN ps RETURN a.id AS a");
    assert_whole("MATCH (q:Person) WHERE q.id IN ps RETURN q.id AS q");
    assert_whole("RETURN 1 IN ps AS hit");
}

#[test]
fn membership_of_a_projected_non_node_keeps_the_whole_node() {
    assert_collected(
        "MATCH (q:Person) WITH q.id AS x \
         MATCH (f:Forum)-[:HAS_MEMBER]->(p:Person) \
         WITH x, f, collect(p) AS ps WHERE x IN ps AND size(ps) > 0 RETURN x",
        &[Whole],
    );
}

#[test]
fn collect_distinct_is_never_narrowed() {
    assert_collected(
        "MATCH (f:Forum)-[:HAS_MEMBER]->(p:Person) \
         WITH f, collect(DISTINCT p) AS ps RETURN size(ps) AS n",
        &[Whole],
    );
}

#[test]
fn count_distinct_of_the_list_keeps_the_whole_node() {
    assert_whole("RETURN count(DISTINCT ps) AS n");
}

#[test]
fn aggregating_the_list_keeps_the_whole_node() {
    // `collect(ps)` is itself a collect, over a list value rather than a
    // node, so only the first (the member list) is judged here.
    let query = over_members("WHERE size(ps) > 0 RETURN collect(ps) AS lists");
    assert_eq!(collected(&query)[0], Whole, "query: {query}");
}

#[test]
fn a_plan_with_a_write_keeps_the_whole_node() {
    assert_whole("MATCH (q:Person) WHERE q IN ps SET q.seen = true");
    assert_whole("MATCH (q:Person) WHERE size(ps) > 1 CREATE (q)-[:SEEN]->(:Mark)");
}

#[test]
fn a_list_read_by_a_pattern_comprehension_child_keeps_the_whole_node() {
    assert_whole(
        "MATCH (q:Person) RETURN q.id AS q, \
         [(q)-[:KNOWS]->(r) WHERE r IN ps | r.id] AS reached",
    );
}

#[test]
fn a_list_read_by_an_exists_child_keeps_the_whole_node() {
    assert_whole(
        "MATCH (q:Person) WHERE EXISTS { MATCH (q)-[:KNOWS]->(r) WHERE r IN ps RETURN r } \
         RETURN q.id AS q",
    );
}

#[test]
fn a_terminal_collect_keeps_the_whole_node() {
    assert_collected("MATCH (p:Person) RETURN collect(p) AS ps", &[Whole]);
}

#[test]
fn a_collect_of_a_non_node_is_left_alone() {
    assert_members_of_names("RETURN size(ps) AS n", &[Other]);
}

fn assert_members_of_names(rest: &str, expected: &[Collected]) {
    assert_collected(
        &format!("MATCH (f:Forum)-[:HAS_MEMBER]->(p:Person) WITH f, collect(p.name) AS ps {rest}"),
        expected,
    );
}

#[test]
fn end_node_of_a_matched_relationship_narrows_like_the_node() {
    // The binder resolves `endNode(r)` over a fixed-hop match to the bound
    // `b`, so the collect argument is the same `_node_struct(b)` and narrows
    // under the same rules.
    assert_collected(
        "MATCH (a:Person)-[r:KNOWS]->(b:Person) \
         WITH a, collect(endNode(r)) AS es RETURN a.id AS a, size(es) AS n",
        &[Identity],
    );
    assert_collected(
        "MATCH (a:Person)-[r:KNOWS]->(b:Person) \
         WITH a, collect(endNode(r)) AS es RETURN a.id AS a, es",
        &[Whole],
    );
}

// ---------------------------------------------------------------------------
// Every expression position that can hold the list is walked.
// ---------------------------------------------------------------------------

#[test]
fn list_comprehension_projection_and_filter_are_walked() {
    assert_whole("RETURN [x IN [1] | ps] AS r");
    assert_whole("RETURN [x IN [1] WHERE ps = [] | x] AS r");
    assert_whole("RETURN any(x IN [1] WHERE ps = []) AS r");
}

#[test]
fn list_of_an_element_pattern_comprehension_is_walked() {
    assert_whole("RETURN [x IN ps | [(x)-->() | x]] AS nested");
}

#[test]
fn case_operand_and_both_branches_are_walked() {
    assert_whole("RETURN CASE WHEN size(ps) > 0 THEN 1 ELSE ps END AS r");
    assert_whole("RETURN CASE WHEN ps = [] THEN 1 ELSE 2 END AS r");
    assert_whole("RETURN CASE ps WHEN [] THEN 1 ELSE 2 END AS r");
    assert_whole("RETURN CASE 1 WHEN 1 THEN 1 ELSE ps END AS r");
}

#[test]
fn both_operands_of_a_binary_operator_are_walked() {
    assert_whole("RETURN ps = [] AS none");
    assert_whole("RETURN [] = ps AS none");
    assert_whole("RETURN size(ps) > 0 AND ps = [] AS r");
}

#[test]
fn unary_operand_is_walked() {
    assert_whole("RETURN NOT (ps = []) AS none");
    assert_whole("RETURN NOT (ps IS NULL) AS some");
}

#[test]
fn sort_keys_are_walked() {
    assert_whole("RETURN f.id AS id ORDER BY ps");
}

#[test]
fn a_group_key_is_walked() {
    assert_whole("RETURN ps AS k, count(*) AS c");
    assert_whole("RETURN [ps] AS k, count(*) AS c");
}

#[test]
fn an_aggregate_argument_is_walked() {
    assert_whole("RETURN max(head(ps).id) AS m");
    // `collect([ps])` is a second collect, over a list value; only the first
    // (the member list) is judged here.
    let query =
        over_members("MATCH (witness:Person) WHERE witness IN ps RETURN collect([ps]) AS m");
    assert_eq!(collected(&query)[0], Whole, "query: {query}");
}

#[test]
fn a_percentile_argument_is_walked() {
    assert_whole("RETURN percentileCont(f.id, size(head(ps))) AS p");
}

#[test]
fn a_filter_predicate_is_walked() {
    assert_whole_in_filter("ps = []");
    assert_whole_in_filter("[] = ps");
    assert_whole_in_filter("NOT (ps = [])");
}

#[test]
fn a_list_comprehension_filter_in_a_filter_is_walked() {
    assert_whole_in_filter("size([x IN [1] WHERE ps = [] | x]) > 0");
}

#[test]
fn a_membership_between_two_collected_lists_reads_both() {
    // Neither side is a node, so `ps IN qs` is not an identity use of either.
    assert_collected(
        "MATCH (f:Forum)-[:HAS_MEMBER]->(p:Person) \
         WITH f, collect(p) AS ps, collect(p) AS qs \
         RETURN ps IN qs AS hit, size(ps) AS a, size(qs) AS b",
        &[Whole, Whole],
    );
}

// ---------------------------------------------------------------------------
// Hand-built plans.
//
// The binder never emits some shapes the pass must still get right: a
// variable defined twice, a `WITH` predicate, a `LIMIT` over a list, a node
// variable that is also an edge. The guards for them are exercised on plans
// assembled directly, so a regression in one cannot hide behind the binder.
// Every plan below starts from `base()`: scan a node and a witness node, and
// `collect(node) AS list`.
// ---------------------------------------------------------------------------

const NODE: VarId = VarId(1);
const WITNESS: VarId = VarId(2);
const LIST: VarId = VarId(3);
const EDGE: VarId = VarId(4);
const OTHER: VarId = VarId(5);
const SPARE: VarId = VarId(6);
const SPARE_TOO: VarId = VarId(7);

struct Hand {
    builder: GraphPlanBuilder,
}

impl Hand {
    fn new() -> Self {
        Self {
            builder: GraphPlan::builder("openCypher"),
        }
    }

    fn expr(&mut self, expr: IrExpr) -> ExprId {
        self.builder.push_expr(expr)
    }

    fn var(&mut self, var: VarId) -> ExprId {
        self.expr(IrExpr::VarRef(var))
    }

    fn int(&mut self, value: i64) -> ExprId {
        self.expr(IrExpr::Literal(IrLiteral::Int(value)))
    }

    fn op(&mut self, op: GraphOp) {
        self.builder.push_op_mut(op);
    }

    fn node_scan(&mut self, var: VarId) {
        self.op(GraphOp::NodeScan { var, ty: None });
    }

    /// `collect(_node_struct(node)) AS list`.
    fn collect(&mut self, node: VarId, list: VarId) {
        let node = self.var(node);
        let arg = self.expr(IrExpr::FunctionCall {
            name: NODE_STRUCT_CALL.to_string(),
            args: vec![node],
        });
        self.op(GraphOp::Aggregate {
            group_by: vec![],
            group_aliases: vec![],
            group_vars: vec![],
            aggs: vec![AggExpr {
                func: AggFunc::Collect,
                arg: Some(arg),
                percentile: None,
                alias: "ps".to_string(),
                out_var: Some(list),
            }],
        });
    }

    /// `WITNESS IN LIST`.
    fn membership(&mut self) -> ExprId {
        let left = self.var(WITNESS);
        let right = self.var(LIST);
        self.expr(IrExpr::BinaryOp {
            op: BinaryOpKind::In,
            left,
            right,
        })
    }

    /// A top-level identity use of the list, independent of whatever the
    /// test adds, so the collect can only stay whole because of that.
    fn witness_use(&mut self) {
        let predicate = self.membership();
        self.op(GraphOp::Filter { predicate });
    }

    /// A projection item that defines `out` from `expr`.
    fn define(&mut self, out: VarId, expr: ExprId) {
        self.op(GraphOp::Project {
            items: vec![ProjectItem {
                expr,
                alias: None,
                out_var: Some(out),
            }],
            distinct: false,
        });
    }

    /// `WITH 1 AS out` is not a node, whatever else binds `out`.
    fn define_from_literal(&mut self, out: VarId) {
        let one = self.int(1);
        self.define(out, one);
    }

    fn filter(&mut self, predicate: ExprId) {
        self.op(GraphOp::Filter { predicate });
    }
}

fn base() -> Hand {
    let mut hand = Hand::new();
    hand.node_scan(NODE);
    hand.node_scan(WITNESS);
    hand.collect(NODE, LIST);
    hand
}

/// A plan of its own (its own expression arena) that tests `WITNESS IN LIST`.
fn membership_plan() -> GraphPlan {
    let mut hand = Hand::new();
    hand.witness_use();
    hand.builder.build()
}

fn empty_plan() -> GraphPlan {
    GraphPlan::builder("openCypher").build()
}

fn narrowed(hand: Hand) -> Vec<Collected> {
    let mut plan = hand.builder.build();
    super::narrow_node_collects(&mut plan);
    let mut out = Vec::new();
    walk(&plan, &mut out);
    out
}

/// `base()` plus an identity use plus `extra`; the collect must stay whole.
#[track_caller]
fn stays_whole(label: &str, extra: impl FnOnce(&mut Hand)) {
    let mut hand = base();
    hand.witness_use();
    extra(&mut hand);
    assert_eq!(narrowed(hand), [Whole], "{label}");
}

#[test]
fn hand_built_baseline_narrows_only_with_an_identity_use() {
    let mut hand = base();
    hand.witness_use();
    assert_eq!(narrowed(hand), [Identity]);
    // No use at all is not an identity use.
    assert_eq!(narrowed(base()), [Whole]);
}

#[test]
fn a_list_variable_claimed_by_another_operator_stays_whole() {
    stays_whole("node scan", |h| h.node_scan(LIST));
    stays_whole("unwind alias", |h| {
        let list = h.expr(IrExpr::ListLiteral(vec![]));
        h.op(GraphOp::Unwind {
            list_expr: list,
            alias: LIST,
        });
    });
    stays_whole("projection output", |h| h.define_from_literal(LIST));
    stays_whole("pass-through projection output", |h| {
        let node = h.var(NODE);
        h.define(LIST, node);
    });
    stays_whole("group output", |h| {
        let key = h.int(1);
        h.op(GraphOp::Aggregate {
            group_by: vec![key],
            group_aliases: vec![None],
            group_vars: vec![Some(LIST)],
            aggs: vec![],
        });
    });
    stays_whole("pass-through group output", |h| {
        let key = h.var(NODE);
        h.op(GraphOp::Aggregate {
            group_by: vec![key],
            group_aliases: vec![None],
            group_vars: vec![Some(LIST)],
            aggs: vec![],
        });
    });
    for (label, src, dst) in [
        ("expand source", LIST, OTHER),
        ("expand target", OTHER, LIST),
    ] {
        stays_whole(label, |h| {
            h.op(GraphOp::Expand {
                src,
                edge: EDGE,
                dst,
                rel_ty: None,
                dir: Direction::Out,
                min_hops: 1,
                max_hops: Some(1),
            });
        });
    }
    stays_whole("edge scan", |h| {
        h.op(GraphOp::EdgeScan {
            var: LIST,
            ty: None,
        });
    });
}

#[test]
fn a_list_variable_defined_by_two_aggregates_stays_whole() {
    let mut hand = base();
    hand.witness_use();
    hand.collect(NODE, LIST);
    assert_eq!(narrowed(hand), [Whole, Whole]);
}

#[test]
fn a_collect_over_a_variable_never_bound_as_a_node_stays_whole() {
    let mut hand = Hand::new();
    hand.node_scan(WITNESS);
    hand.collect(OTHER, LIST);
    hand.witness_use();
    assert_eq!(narrowed(hand), [Whole]);
}

#[test]
fn a_node_variable_that_is_also_something_else_stays_whole() {
    // `NODE` is scanned as a node and also defined by each of these, so the
    // plan cannot prove `NODE` is always a node.
    stays_whole("edge scan", |h| {
        h.op(GraphOp::EdgeScan {
            var: NODE,
            ty: None,
        });
    });
    stays_whole("typed edge scan", |h| {
        h.op(GraphOp::TypedEdgeScan {
            var: NODE,
            rel_ty: RelationTypeId::ontology(TypeId(1)).unwrap(),
        });
    });
    stays_whole("expand edge", |h| {
        h.op(GraphOp::Expand {
            src: WITNESS,
            edge: NODE,
            dst: OTHER,
            rel_ty: None,
            dir: Direction::Out,
            min_hops: 1,
            max_hops: Some(1),
        });
    });
    stays_whole("relationship uniqueness edge", |h| {
        h.op(GraphOp::RelationshipUnique {
            edge: NODE,
            prior_edges: vec![],
        });
    });
    stays_whole("relationship uniqueness prior edge", |h| {
        h.op(GraphOp::RelationshipUnique {
            edge: EDGE,
            prior_edges: vec![NODE],
        });
    });
    stays_whole("unwind alias", |h| {
        let list = h.expr(IrExpr::ListLiteral(vec![]));
        h.op(GraphOp::Unwind {
            list_expr: list,
            alias: NODE,
        });
    });
    stays_whole("projection of a value", |h| h.define_from_literal(NODE));
    stays_whole("group output of a value", |h| {
        let key = h.int(1);
        h.op(GraphOp::Aggregate {
            group_by: vec![key],
            group_aliases: vec![None],
            group_vars: vec![Some(NODE)],
            aggs: vec![],
        });
    });
    stays_whole("aggregate output", |h| {
        h.op(GraphOp::Aggregate {
            group_by: vec![],
            group_aliases: vec![],
            group_vars: vec![],
            aggs: vec![AggExpr {
                func: AggFunc::Count,
                arg: None,
                percentile: None,
                alias: "c".to_string(),
                out_var: Some(NODE),
            }],
        });
    });
    stays_whole("procedure yield", |h| {
        h.op(GraphOp::Call {
            procedure: ProcedureDefinition {
                name: "test.proc".to_string(),
                inputs: vec![],
                outputs: vec![ProcedureField {
                    name: "out".to_string(),
                    type_name: "INTEGER".to_string(),
                    nullable: false,
                }],
                rows: vec![],
            },
            args: vec![],
            yields: vec![ProcedureYield {
                field: "out".to_string(),
                alias: "out".to_string(),
                var: NODE,
            }],
        });
    });
    stays_whole("pattern comprehension output", |h| {
        h.op(GraphOp::PatternComprehension {
            child: Box::new(empty_plan()),
            output: NODE,
        });
    });
    for (label, loop_var, pattern_output, output) in [
        (
            "element comprehension loop variable",
            NODE,
            SPARE,
            SPARE_TOO,
        ),
        (
            "element comprehension pattern output",
            SPARE,
            NODE,
            SPARE_TOO,
        ),
        ("element comprehension output", SPARE, SPARE_TOO, NODE),
    ] {
        stays_whole(label, |h| {
            let list_expr = h.expr(IrExpr::ListLiteral(vec![]));
            h.op(GraphOp::ListElementPatternComprehension {
                list_expr,
                loop_var,
                child: Box::new(empty_plan()),
                pattern_output,
                filter: None,
                projection: None,
                output,
            });
        });
    }
    stays_whole("quantifier loop variable", |h| {
        let list = h.expr(IrExpr::ListLiteral(vec![]));
        let predicate = h.expr(IrExpr::Literal(IrLiteral::Bool(true)));
        let quantifier = h.expr(IrExpr::Quantifier {
            kind: QuantifierKind::All,
            loop_var: NODE,
            list,
            predicate,
        });
        h.filter(quantifier);
    });
    stays_whole("list comprehension loop variable", |h| {
        let list = h.expr(IrExpr::ListLiteral(vec![]));
        let comprehension = h.expr(IrExpr::ListComprehension {
            loop_var: NODE,
            list,
            filter: None,
            projection: None,
        });
        h.filter(comprehension);
    });
}

#[test]
fn a_node_variable_re_projected_from_a_non_node_stays_whole() {
    // Directly: EDGE is not a node, NODE re-projects it.
    stays_whole("one hop", |h| {
        h.op(GraphOp::EdgeScan {
            var: EDGE,
            ty: None,
        });
        let edge = h.var(EDGE);
        h.define(NODE, edge);
    });
    // Through a chain recorded in the opposite order, which needs a second
    // pass of the fixpoint: OTHER <- NODE is listed before EDGE -> OTHER.
    stays_whole("two hops, listed backwards", |h| {
        h.op(GraphOp::EdgeScan {
            var: EDGE,
            ty: None,
        });
        let other = h.var(OTHER);
        h.define(NODE, other);
        let edge = h.var(EDGE);
        h.define(OTHER, edge);
    });
}

#[test]
fn a_node_variable_re_projected_from_a_node_still_narrows() {
    // The collect is over NODE_COPY, a bare re-projection of NODE, in a chain
    // recorded backwards, so the propagation needs its second pass.
    let mut hand = Hand::new();
    hand.node_scan(WITNESS);
    hand.node_scan(NODE);
    let copy = hand.var(OTHER);
    hand.define(SPARE, copy);
    let node = hand.var(NODE);
    hand.define(OTHER, node);
    hand.collect(SPARE, LIST);
    hand.witness_use();
    assert_eq!(narrowed(hand), [Identity]);
}

#[test]
fn a_with_predicate_reading_the_list_is_walked() {
    stays_whole("with predicate", |h| {
        let predicate = h.var(LIST);
        h.op(GraphOp::With {
            items: vec![],
            distinct: false,
            where_predicate: Some(predicate),
        });
    });
}

#[test]
fn limit_and_skip_expressions_are_walked() {
    // The binder only produces these for expressions that read no variable,
    // so a list cannot reach them from Cypher; the walk still covers them.
    stays_whole("limit", |h| {
        let expr = h.var(LIST);
        h.op(GraphOp::LimitExpr { expr });
    });
    stays_whole("skip", |h| {
        let expr = h.var(LIST);
        h.op(GraphOp::SkipExpr { expr });
    });
}

#[test]
fn a_call_argument_reading_the_list_is_walked() {
    stays_whole("procedure argument", |h| {
        let arg = h.var(LIST);
        h.op(GraphOp::Call {
            procedure: ProcedureDefinition {
                name: "test.proc".to_string(),
                inputs: vec![ProcedureField {
                    name: "in".to_string(),
                    type_name: "LIST".to_string(),
                    nullable: false,
                }],
                outputs: vec![],
                rows: vec![],
            },
            args: vec![arg],
            yields: vec![],
        });
    });
}

#[test]
fn a_child_plan_use_never_counts_as_an_identity_use() {
    // `WITNESS IN LIST` is an identity use at the top level, but inside a
    // child plan the list is correlated through another path, so it is a
    // plain read there and the collect stays whole.
    stays_whole("exists child", |h| {
        h.op(GraphOp::Exists {
            child: Box::new(membership_plan()),
            negated: false,
        });
    });
    stays_whole("pattern comprehension child", |h| {
        h.op(GraphOp::PatternComprehension {
            child: Box::new(membership_plan()),
            output: SPARE,
        });
    });
    stays_whole("element comprehension child", |h| {
        let list_expr = h.expr(IrExpr::ListLiteral(vec![]));
        h.op(GraphOp::ListElementPatternComprehension {
            list_expr,
            loop_var: SPARE,
            child: Box::new(membership_plan()),
            pattern_output: SPARE_TOO,
            filter: None,
            projection: None,
            output: EDGE,
        });
    });
    stays_whole("union input", |h| {
        h.op(GraphOp::Union {
            all: true,
            inputs: vec![membership_plan()],
        });
    });
}

#[test]
fn an_optional_child_inherits_the_identity_use() {
    // `OPTIONAL MATCH ... WHERE witness IN ps` correlates through the outer
    // row, so a use there is the same identity use as at the top level.
    let mut hand = base();
    hand.op(GraphOp::Optional {
        child: Box::new(membership_plan()),
    });
    assert_eq!(narrowed(hand), [Identity]);
}

#[test]
fn an_element_pattern_comprehension_reading_the_list_is_walked() {
    let element = |list_expr: Option<VarId>, filter: Option<VarId>, projection: Option<VarId>| {
        move |h: &mut Hand| {
            let list_expr = match list_expr {
                Some(var) => h.var(var),
                None => h.expr(IrExpr::ListLiteral(vec![])),
            };
            let filter = filter.map(|var| h.var(var));
            let projection = projection.map(|var| h.var(var));
            h.op(GraphOp::ListElementPatternComprehension {
                list_expr,
                loop_var: SPARE,
                child: Box::new(empty_plan()),
                pattern_output: SPARE_TOO,
                filter,
                projection,
                output: EDGE,
            });
        }
    };
    stays_whole("source list", element(Some(LIST), None, None));
    stays_whole("filter", element(None, Some(LIST), None));
    stays_whole("projection", element(None, None, Some(LIST)));
}

#[test]
fn a_node_bound_only_by_an_expand_endpoint_is_a_node() {
    for (label, src, dst) in [("destination", OTHER, NODE), ("source", NODE, OTHER)] {
        let mut hand = Hand::new();
        hand.node_scan(WITNESS);
        hand.op(GraphOp::Expand {
            src,
            edge: EDGE,
            dst,
            rel_ty: None,
            dir: Direction::Out,
            min_hops: 1,
            max_hops: Some(1),
        });
        hand.collect(NODE, LIST);
        hand.witness_use();
        assert_eq!(narrowed(hand), [Identity], "{label}");
    }
}
