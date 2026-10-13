//! Which variable-length expansions can run as a reachability search.
//!
//! openCypher binds `(a)-[r:T*min..max]->(b)` to every **trail** from `a`: a
//! walk that uses no relationship twice. A query over a dense graph has far more
//! trails than reached nodes (a layered graph of `L` layers of width `W` has
//! `W^(L-1)` trails to its last layer and `W * L` nodes), so enumerating trails
//! costs time and memory that the answer does not need whenever the answer only
//! depends on *which nodes are reached* and *how soon*. This module proves, from
//! the operators after an `Expand`, that it does; the lowerer then asks the
//! executor for one row per reached node with its shortest hop count instead of
//! one row per trail (`VarLenMode::Reachability`).
//!
//! # Proof conditions
//!
//! Let `R` be the rows an expansion emits under trail enumeration (one per
//! `(input row i, trail t)`, ending at node `b(t)`), and `R'` the rows under
//! reachability (one per `(i, b)` with `b` reached, carrying `hops(i, b)`). The
//! expansion is eligible when all of these hold, which together make every
//! result the query can observe the same under `R` and `R'`:
//!
//! 1. **Shape.** `min_hops <= 1` and the hop is not the fixed single hop. For
//!    `min_hops <= 1` the shortest trail from `a` to any `b != a` is a shortest
//!    walk (a simple path, hence a trail) of length `dist(a, b) >= 1`, so `b` is
//!    reached by a trail of length in `[min, max]` iff `dist(a, b) <= max`, and
//!    its shortest such trail has length `dist(a, b)`. (`b == a` is handled by the
//!    executor: it is reached at 0 hops when `min_hops = 0`, else by the shortest
//!    closed trail through `a`.) With `min_hops >= 2` the shortest trail of at
//!    least `min_hops` relationships is not a shortest path, so the mode is not
//!    selected.
//! 2. **The relationship list is never read.** The edge variable (and any named
//!    path, which binds to it) appears only as the argument of `length()` or
//!    `size()` that is itself the argument of `min()`, or as the list of a
//!    per-hop predicate that is pushed into the search (see below). Every other
//!    mention — `RETURN r`, `[e IN r | …]`, `ANY(e IN r WHERE …)`, `nodes(p)`,
//!    `r[0]`, `length(r) > 2` in a filter, `max(length(r))` — reads something
//!    reachability does not produce, so the expansion stays on trails. A
//!    relationship-uniqueness constraint against another relationship
//!    (`RelationshipUnique`) reads the list too; it is vacuous, and dropped, only
//!    when both relationships have known, different relation types, because then
//!    they cannot share a relationship.
//! 3. **Row multiplicity is erased before it is observed.** Between the
//!    expansion and the first operator that ignores how many times a row occurs,
//!    every operator is a function of one row at a time (filters, projections,
//!    unwinds, node and edge scans, further expansions, sorts) — so a row of `R'`
//!    stands in for all the rows of `R` it represents. That first operator is a
//!    `DISTINCT` projection or an aggregate whose every function ignores
//!    multiplicity: `min`, `max`, and the `DISTINCT` forms of `count`, `collect`,
//!    `sum` and `avg`. `count(*)`, `count(x)`, `sum`, `avg`, `collect` and the
//!    percentiles see multiplicity; `LIMIT`/`SKIP` pick rows by position;
//!    subqueries, writes and procedure calls are not modelled. Reaching the end
//!    of the pipeline without such an operator leaves the multiplicity visible.
//! 4. **`min(length(r))` is the shortest trail.** `min` over the trails of a
//!    group equals `min` over their shortest lengths, which is `hops`.
//!
//! # Required destination
//!
//! `MATCH (a {id: $x})-[r*1..]->(b {id: $y}) RETURN min(size(r))` binds `b` only
//! after the expansion, as a filter on every node the expansion reaches. When the
//! expansion is unbounded and `b` is pinned by an equality against a parameter or
//! literal, [`ReachabilityExpansion::hoisted_destination_ops`] asks the lowerer
//! to scan `b` first, so the executor receives the required destination as an
//! input column and stops its search at the level that reaches it. A bounded
//! expansion keeps the join after it: its search is bounded by the hop limit.
//!
//! # Per-hop predicates
//!
//! A top-level conjunct `ALL(e IN r WHERE p)` of a filter after the expansion
//! (`TCR3`, or the inline `-[r*1..3 {since: 2020}]->`, which the binder emits as
//! one such conjunct per key) is *pushed into the search* when `p` reads only
//! the properties of `e` itself, parameters and literals. It must not read `r`,
//! a position or index, another element of the list, `e` as a whole (`type(e)`,
//! `properties(e)`, `e = other`), any other variable, or a function that is not
//! deterministic and clock-free; `p` must read at least one property of `e`.
//! Only top-level `AND` conjuncts are lifted, and every other conjunct stays in
//! the filter. A conjunct that is not liftable but mentions `r` — `ANY`, `NONE`,
//! `SINGLE`, `ALL` over `nodes(p)`, `ALL` OR-mixed with another condition, a
//! predicate that mentions another variable — keeps the expansion on trails.
//!
//! **Exactness.** `ALL(e IN r WHERE p)` is true exactly when `p` is true for
//! every relationship of the trail `r`: one false makes it false, and one `NULL`
//! with no false makes it `NULL`, and a filter drops both. So a trail passes the
//! filter exactly when every one of its relationships is in
//! `S = { e | p(e) is true }`. The trails from `a` whose every relationship is in
//! `S` are precisely the trails of the subgraph that has only the relationships
//! of `S`, because a trail is a walk that uses no relationship twice and
//! whether it uses relationship `e` is decided by `e` alone. Conditions 1, 3 and
//! 4 of the proof above hold for every graph, so they hold for that subgraph:
//! the nodes reached, and the shortest trail to each (hence `min(size(r))`), are
//! exactly what a breadth-first search over `S` finds. A relationship whose `p`
//! is false or `NULL` is outside `S`, as `ALL` over a list containing it is
//! false or `NULL`. For `min_hops = 0` the zero-hop trail has the empty list,
//! `ALL` over `[]` is true, and the seed keeps its zero-hop row without
//! consulting `p`. The relationships of `S` are decided one frontier at a time
//! by the executor; a relationship is tested at most once per search level.
//!
//! The filter that held the conjunct is lowered without it. With a pinned
//! destination and an unbounded expansion the destination is still hoisted
//! (below) when the `ALL` filter follows the destination's own filters, as it
//! does in `TCR3`; an inline `{…}` map sits between the `Expand` and the
//! destination scan and keeps the plan order.

use std::collections::{BTreeMap, HashMap};

use crate::{
    AggFunc, BinaryOpKind, ExprArena, ExprId, GraphOp, IrExpr, PropertyId, QuantifierKind,
    RelationTypeId, VarId,
};

/// How the lowerer is to treat one provably reachability-only `Expand`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReachabilityExpansion {
    /// How many operators directly after the `Expand` — a `NodeScan` of its
    /// destination followed by filters on that node alone, at least one of which
    /// pins a property to a parameter or literal — move before it, so the
    /// executor receives the required destination as an input column. `0` keeps
    /// the plan order.
    pub hoisted_destination_ops: usize,
    /// Indices of `RelationshipUnique` operators that are vacuous because the
    /// relationship they constrain has a relation type no other relationship in
    /// the constraint has.
    pub vacuous_uniqueness: Vec<usize>,
    /// The per-relationship predicates the search applies itself, one per lifted
    /// `ALL(e IN r WHERE …)` conjunct, in plan order. A `Filter` that held one is
    /// lowered without it (see [`ReachabilityPlan::residual_conjuncts`]).
    pub edge_predicates: Vec<EdgePredicate>,
}

/// One `ALL(e IN r WHERE predicate)` conjunct that the search evaluates per
/// relationship (see the module docs for the conditions and the proof).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgePredicate {
    /// The index of the `Filter` operator that held the conjunct.
    pub filter_op: usize,
    /// The conjunct, the whole `ALL(…)` expression.
    pub conjunct: ExprId,
    /// The variable that stands for one relationship inside `predicate`.
    pub loop_var: VarId,
    /// The predicate, over `loop_var`'s properties, parameters and literals.
    pub predicate: ExprId,
    /// The distinct properties of `loop_var` that `predicate` reads. Never empty.
    pub properties: Vec<PropertyId>,
}

/// The expansions of one operator pipeline that can run as reachability
/// searches, keyed by the index of their `Expand` operator.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReachabilityPlan {
    expansions: BTreeMap<usize, ReachabilityExpansion>,
}

impl ReachabilityPlan {
    /// The proof for the `Expand` at `op_index`, if there is one.
    #[must_use]
    pub fn expansion_at(&self, op_index: usize) -> Option<&ReachabilityExpansion> {
        self.expansions.get(&op_index)
    }

    /// Whether the `RelationshipUnique` at `op_index` is vacuous and may be
    /// skipped.
    #[must_use]
    pub fn is_vacuous_uniqueness(&self, op_index: usize) -> bool {
        self.expansions
            .values()
            .any(|expansion| expansion.vacuous_uniqueness.contains(&op_index))
    }

    /// The order in which to lower the `op_count` operators: plan order, except
    /// that each hoisted destination scan precedes its `Expand`.
    #[must_use]
    pub fn lowering_order(&self, op_count: usize) -> Vec<usize> {
        let mut order: Vec<usize> = (0..op_count).collect();
        for (&expand, expansion) in &self.expansions {
            let hoisted = expansion.hoisted_destination_ops;
            if hoisted == 0 {
                continue;
            }
            // `expand` moves behind the `hoisted` operators that follow it.
            order[expand..=expand + hoisted].rotate_left(1);
        }
        order
    }

    /// Whether no expansion is eligible.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.expansions.is_empty()
    }

    /// Stop treating the per-relationship predicates of the `Expand` at
    /// `op_index` as the search's own, because the lowerer keeps that expansion on
    /// trails. Their `Filter`s then lower unchanged, so no predicate is dropped.
    /// The destination hoist and the vacuous-uniqueness skips do not depend on the
    /// search mode and stay.
    pub fn withdraw_edge_predicates(&mut self, op_index: usize) {
        if let Some(expansion) = self.expansions.get_mut(&op_index) {
            expansion.edge_predicates.clear();
        }
    }

    /// The conjuncts of the `Filter` at `op_index` (whose predicate is
    /// `predicate`) that remain after the per-relationship predicates the search
    /// applies itself are removed, in plan order. `None` when no conjunct of this
    /// filter was lifted, so it lowers unchanged; an empty list means that the
    /// whole filter was lifted and is skipped.
    #[must_use]
    pub fn residual_conjuncts(
        &self,
        exprs: &ExprArena,
        op_index: usize,
        predicate: ExprId,
    ) -> Option<Vec<ExprId>> {
        let lifted = |conjunct: ExprId| {
            self.expansions
                .values()
                .flat_map(|expansion| &expansion.edge_predicates)
                .any(|edge| edge.filter_op == op_index && edge.conjunct == conjunct)
        };
        let conjuncts = conjuncts(exprs, predicate);
        if !conjuncts.iter().any(|conjunct| lifted(*conjunct)) {
            return None;
        }
        Some(
            conjuncts
                .into_iter()
                .filter(|conjunct| !lifted(*conjunct))
                .collect(),
        )
    }
}

/// The top-level `AND` conjuncts of `id`, left to right. An expression that is
/// not an `AND` is its own single conjunct.
#[must_use]
pub fn conjuncts(exprs: &ExprArena, id: ExprId) -> Vec<ExprId> {
    let mut out = Vec::new();
    let mut stack = vec![id];
    while let Some(next) = stack.pop() {
        match exprs.get(next) {
            IrExpr::BinaryOp {
                op: BinaryOpKind::And,
                left,
                right,
            } => {
                stack.push(*right);
                stack.push(*left);
            }
            _ => out.push(next),
        }
    }
    out
}

/// Find the expansions of `ops` that can run as reachability searches.
///
/// The analysis is conservative: anything it does not model keeps trail
/// enumeration.
#[must_use]
pub fn reachability_expansions(ops: &[GraphOp], exprs: &ExprArena) -> ReachabilityPlan {
    let relation_types = relation_types(ops);
    let mut plan = ReachabilityPlan::default();
    for index in 0..ops.len() {
        if let Some(expansion) = analyse(ops, exprs, &relation_types, index) {
            plan.expansions.insert(index, expansion);
        }
    }
    plan
}

/// The relation type each relationship variable of the pipeline is matched
/// against, `None` for a wildcard or alternation.
fn relation_types(ops: &[GraphOp]) -> HashMap<VarId, Option<RelationTypeId>> {
    let mut types = HashMap::new();
    for op in ops {
        match op {
            GraphOp::Expand { edge, rel_ty, .. } => {
                types.insert(*edge, *rel_ty);
            }
            GraphOp::TypedEdgeScan { var, rel_ty } => {
                types.insert(*var, Some(*rel_ty));
            }
            GraphOp::EdgeScan { var, ty } => {
                types.insert(*var, *ty);
            }
            _ => {}
        }
    }
    types
}

/// Whether `op` introduces `var`.
fn binds(op: &GraphOp, var: VarId) -> bool {
    match op {
        GraphOp::NodeScan { var: bound, .. }
        | GraphOp::EdgeScan { var: bound, .. }
        | GraphOp::TypedEdgeScan { var: bound, .. } => *bound == var,
        GraphOp::Expand { src, edge, dst, .. } => *src == var || *edge == var || *dst == var,
        _ => false,
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "one forward scan with one arm per operator kind keeps the proof conditions in one place"
)]
fn analyse(
    ops: &[GraphOp],
    exprs: &ExprArena,
    relation_types: &HashMap<VarId, Option<RelationTypeId>>,
    index: usize,
) -> Option<ReachabilityExpansion> {
    let GraphOp::Expand {
        edge,
        rel_ty,
        min_hops,
        max_hops,
        ..
    } = &ops[index]
    else {
        return None;
    };
    let edge = *edge;
    // Condition 1: a variable-length hop that may start at 0 or 1.
    if *min_hops > 1 || (*min_hops == 1 && *max_hops == Some(1)) {
        return None;
    }
    // The relationship list must be this expansion's own: an edge variable
    // bound earlier makes the expansion a filter on a list it did not produce.
    if ops[..index].iter().any(|op| binds(op, edge)) {
        return None;
    }

    let mentions = |id: ExprId| mentions_var(exprs, id, edge);
    let mut vacuous_uniqueness = Vec::new();
    let mut edge_predicates = Vec::new();
    for (offset, op) in ops[index + 1..].iter().enumerate() {
        let position = index + 1 + offset;
        match op {
            // Row-at-a-time operators that do not read the relationship list.
            GraphOp::NodeScan { .. }
            | GraphOp::EdgeScan { .. }
            | GraphOp::TypedEdgeScan { .. }
            | GraphOp::Expand { .. } => {
                if binds(op, edge) {
                    return None;
                }
            }
            GraphOp::RelationshipUnique {
                edge: new,
                prior_edges,
            } => {
                if *new != edge && !prior_edges.contains(&edge) {
                    continue;
                }
                let others: Vec<VarId> = if *new == edge {
                    prior_edges.clone()
                } else {
                    vec![*new]
                };
                let disjoint = |other: &VarId| {
                    matches!(
                        (rel_ty, relation_types.get(other)),
                        (Some(own), Some(Some(theirs))) if own != theirs
                    )
                };
                if !others.iter().all(disjoint) {
                    return None;
                }
                vacuous_uniqueness.push(position);
            }
            GraphOp::Filter { predicate } => {
                // A conjunct that mentions the list is either a per-hop predicate
                // the search can apply itself, or a read of something
                // reachability does not produce.
                for conjunct in conjuncts(exprs, *predicate) {
                    if !mentions(conjunct) {
                        continue;
                    }
                    let lifted = edge_predicate(exprs, conjunct, edge, position)?;
                    edge_predicates.push(lifted);
                }
            }
            GraphOp::Unwind { list_expr, .. } => {
                if mentions(*list_expr) {
                    return None;
                }
            }
            GraphOp::Sort { keys } => {
                if keys.iter().any(|key| mentions(key.expr)) {
                    return None;
                }
            }
            GraphOp::Project { items, distinct } => {
                if items.iter().any(|item| mentions(item.expr)) {
                    return None;
                }
                if *distinct {
                    return Some(expansion(
                        ops,
                        exprs,
                        index,
                        vacuous_uniqueness,
                        edge_predicates,
                    ));
                }
            }
            GraphOp::With {
                items,
                distinct,
                where_predicate,
            } => {
                if items.iter().any(|item| mentions(item.expr))
                    || where_predicate.is_some_and(mentions)
                {
                    return None;
                }
                if *distinct {
                    return Some(expansion(
                        ops,
                        exprs,
                        index,
                        vacuous_uniqueness,
                        edge_predicates,
                    ));
                }
            }
            GraphOp::Aggregate { group_by, aggs, .. } => {
                if group_by.iter().any(|key| mentions(*key)) {
                    return None;
                }
                let ignores_multiplicity = aggs.iter().all(|agg| {
                    if agg.percentile.is_some_and(mentions) {
                        return false;
                    }
                    match agg.func {
                        // `min` over a group's trails is `min` over their shortest
                        // lengths (condition 4), so it may read `length(r)`.
                        AggFunc::Min => agg
                            .arg
                            .is_none_or(|arg| !mentions(arg) || is_length_of(exprs, arg, edge)),
                        AggFunc::Max
                        | AggFunc::CountDistinct
                        | AggFunc::SumDistinct
                        | AggFunc::AvgDistinct
                        | AggFunc::CollectDistinct => agg.arg.is_none_or(|arg| !mentions(arg)),
                        AggFunc::Count
                        | AggFunc::Sum
                        | AggFunc::Avg
                        | AggFunc::Collect
                        | AggFunc::PercentileDisc
                        | AggFunc::PercentileCont => false,
                    }
                });
                if !ignores_multiplicity {
                    return None;
                }
                return Some(expansion(
                    ops,
                    exprs,
                    index,
                    vacuous_uniqueness,
                    edge_predicates,
                ));
            }
            // LIMIT/SKIP select rows by position; subqueries, unions, procedure
            // calls and writes are not modelled.
            _ => return None,
        }
    }
    // The pipeline ends with the multiplicity still observable.
    None
}

/// Build the proof for the `Expand` at `index`, deciding whether its
/// destination can be hoisted.
fn expansion(
    ops: &[GraphOp],
    exprs: &ExprArena,
    index: usize,
    vacuous_uniqueness: Vec<usize>,
    edge_predicates: Vec<EdgePredicate>,
) -> ReachabilityExpansion {
    ReachabilityExpansion {
        hoisted_destination_ops: hoistable_destination_ops(ops, exprs, index),
        vacuous_uniqueness,
        edge_predicates,
    }
}

/// The per-relationship predicate that `conjunct` is, if it is
/// `ALL(e IN edge WHERE p)` with `p` pushable (see the module docs).
fn edge_predicate(
    exprs: &ExprArena,
    conjunct: ExprId,
    edge: VarId,
    filter_op: usize,
) -> Option<EdgePredicate> {
    let IrExpr::Quantifier {
        kind: QuantifierKind::All,
        loop_var,
        list,
        predicate,
    } = exprs.get(conjunct)
    else {
        return None;
    };
    if !matches!(exprs.get(*list), IrExpr::VarRef(v) if *v == edge) {
        return None;
    }
    let mut properties = Vec::new();
    if !pushable(exprs, *predicate, *loop_var, &mut properties) || properties.is_empty() {
        return None;
    }
    Some(EdgePredicate {
        filter_op,
        conjunct,
        loop_var: *loop_var,
        predicate: *predicate,
        properties,
    })
}

/// Whether `id` is a function of the properties of `loop_var` alone, parameters
/// and literals, built from deterministic operators. Records the properties of
/// `loop_var` it reads in `properties`.
///
/// This is an allow-list: a bare `loop_var` (which would let `type(e)`,
/// `properties(e)` or an equality with another relationship in), any other
/// variable, a nested quantifier or comprehension, a map, and any function not
/// listed are rejected.
fn pushable(
    exprs: &ExprArena,
    id: ExprId,
    loop_var: VarId,
    properties: &mut Vec<PropertyId>,
) -> bool {
    match exprs.get(id) {
        IrExpr::Literal(_) | IrExpr::Parameter(_) => true,
        IrExpr::PropertyAccess { base, prop } => {
            if !matches!(exprs.get(*base), IrExpr::VarRef(v) if *v == loop_var) {
                return false;
            }
            if !properties.contains(prop) {
                properties.push(*prop);
            }
            true
        }
        IrExpr::BinaryOp { left, right, .. } => {
            pushable(exprs, *left, loop_var, properties)
                && pushable(exprs, *right, loop_var, properties)
        }
        IrExpr::UnaryOp { expr, .. } => pushable(exprs, *expr, loop_var, properties),
        IrExpr::FunctionCall { name, args } => {
            DETERMINISTIC_FUNCTIONS
                .iter()
                .any(|allowed| name.eq_ignore_ascii_case(allowed))
                && args
                    .iter()
                    .all(|arg| pushable(exprs, *arg, loop_var, properties))
        }
        IrExpr::Case {
            operand,
            arms,
            else_expr,
        } => {
            operand
                .iter()
                .chain(else_expr)
                .all(|child| pushable(exprs, *child, loop_var, properties))
                && arms.iter().all(|arm| {
                    pushable(exprs, arm.when, loop_var, properties)
                        && pushable(exprs, arm.then, loop_var, properties)
                })
        }
        IrExpr::ListLiteral(items) => items
            .iter()
            .all(|item| pushable(exprs, *item, loop_var, properties)),
        IrExpr::VarRef(_)
        | IrExpr::MapLiteral(_)
        | IrExpr::Quantifier { .. }
        | IrExpr::ListComprehension { .. } => false,
    }
}

/// Functions a pushed predicate may call: pure functions of their arguments, with
/// no clock, randomness or access to a whole entity.
const DETERMINISTIC_FUNCTIONS: &[&str] = &[
    "abs",
    "ceil",
    "floor",
    "round",
    "sqrt",
    "sign",
    "exp",
    "log",
    "power",
    "coalesce",
    "size",
    "tolower",
    "lower",
    "toupper",
    "upper",
    "trim",
    "ltrim",
    "rtrim",
    "tostring",
    "tointeger",
    "tofloat",
    "toboolean",
];

/// The number of operators after the `Expand` at `index` to move before it
/// (see [`ReachabilityExpansion::hoisted_destination_ops`]), or `0`.
fn hoistable_destination_ops(ops: &[GraphOp], exprs: &ExprArena, index: usize) -> usize {
    let GraphOp::Expand {
        src, dst, max_hops, ..
    } = &ops[index]
    else {
        return 0;
    };
    // A bounded search is bounded by the hop limit; only an unbounded one gains
    // from stopping at the target.
    let before = &ops[..index];
    if max_hops.is_some()
        || src == dst
        || before.iter().any(|op| binds(op, *dst))
        || !before.iter().any(|op| binds(op, *src))
    {
        return 0;
    }
    let Some(GraphOp::NodeScan { var, .. }) = ops.get(index + 1) else {
        return 0;
    };
    if var != dst {
        return 0;
    }
    let mut count = 1;
    let mut pinned = false;
    for op in &ops[index + 2..] {
        let GraphOp::Filter { predicate } = op else {
            break;
        };
        if !only_mentions(exprs, *predicate, *dst) {
            break;
        }
        pinned |= pins_property(exprs, *predicate, *dst);
        count += 1;
    }
    if pinned { count } else { 0 }
}

/// Whether `id` mentions `var`, anywhere in its tree.
fn mentions_var(exprs: &ExprArena, id: ExprId, var: VarId) -> bool {
    let mut found = false;
    visit(exprs, id, &mut |expr| {
        if matches!(expr, IrExpr::VarRef(v) if *v == var) {
            found = true;
        }
    });
    found
}

/// Whether the only variable `id` mentions is `var`.
fn only_mentions(exprs: &ExprArena, id: ExprId, var: VarId) -> bool {
    let mut only = true;
    visit(exprs, id, &mut |expr| {
        if matches!(expr, IrExpr::VarRef(v) if *v != var) {
            only = false;
        }
    });
    only
}

/// Whether `id` is `length(var)` or `size(var)`.
fn is_length_of(exprs: &ExprArena, id: ExprId, var: VarId) -> bool {
    match exprs.get(id) {
        IrExpr::FunctionCall { name, args } => {
            (name.eq_ignore_ascii_case("length") || name.eq_ignore_ascii_case("size"))
                && matches!(args.as_slice(), [arg] if matches!(exprs.get(*arg), IrExpr::VarRef(v) if *v == var))
        }
        _ => false,
    }
}

/// Whether one conjunct of `id` equates a property of `var` with a parameter or
/// literal.
fn pins_property(exprs: &ExprArena, id: ExprId, var: VarId) -> bool {
    match exprs.get(id) {
        IrExpr::BinaryOp {
            op: BinaryOpKind::And,
            left,
            right,
        } => pins_property(exprs, *left, var) || pins_property(exprs, *right, var),
        IrExpr::BinaryOp {
            op: BinaryOpKind::Eq,
            left,
            right,
        } => {
            let is_property = |id: &ExprId| {
                matches!(
                    exprs.get(*id),
                    IrExpr::PropertyAccess { base, .. }
                        if matches!(exprs.get(*base), IrExpr::VarRef(v) if *v == var)
                )
            };
            let is_constant =
                |id: &ExprId| matches!(exprs.get(*id), IrExpr::Literal(_) | IrExpr::Parameter(_));
            (is_property(left) && is_constant(right)) || (is_property(right) && is_constant(left))
        }
        _ => false,
    }
}

/// Call `visit_node` on `id` and every expression below it.
fn visit(exprs: &ExprArena, id: ExprId, visit_node: &mut impl FnMut(&IrExpr)) {
    let expr = exprs.get(id);
    visit_node(expr);
    match expr {
        IrExpr::Literal(_) | IrExpr::VarRef(_) | IrExpr::Parameter(_) => {}
        IrExpr::PropertyAccess { base, .. } => visit(exprs, *base, visit_node),
        IrExpr::BinaryOp { left, right, .. } => {
            visit(exprs, *left, visit_node);
            visit(exprs, *right, visit_node);
        }
        IrExpr::UnaryOp { expr, .. } => visit(exprs, *expr, visit_node),
        IrExpr::FunctionCall { args, .. } => {
            for arg in args {
                visit(exprs, *arg, visit_node);
            }
        }
        IrExpr::Case {
            operand,
            arms,
            else_expr,
        } => {
            for child in operand.iter().chain(else_expr) {
                visit(exprs, *child, visit_node);
            }
            for arm in arms {
                visit(exprs, arm.when, visit_node);
                visit(exprs, arm.then, visit_node);
            }
        }
        IrExpr::ListLiteral(items) => {
            for item in items {
                visit(exprs, *item, visit_node);
            }
        }
        IrExpr::MapLiteral(entries) => {
            for (_, value) in entries {
                visit(exprs, *value, visit_node);
            }
        }
        IrExpr::Quantifier {
            list, predicate, ..
        } => {
            visit(exprs, *list, visit_node);
            visit(exprs, *predicate, visit_node);
        }
        IrExpr::ListComprehension {
            list,
            filter,
            projection,
            ..
        } => {
            visit(exprs, *list, visit_node);
            for child in filter.iter().chain(projection) {
                visit(exprs, *child, visit_node);
            }
        }
    }
}

#[cfg(test)]
mod tests;
