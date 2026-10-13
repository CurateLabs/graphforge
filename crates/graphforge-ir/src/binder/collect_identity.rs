//! Narrow `collect(<node>)` to node identity when nothing reads more (#1984).
//!
//! The binder lowers `collect(n)` over a bound node variable to
//! `collect(_node_struct(n))`, so every collected element is a whole node
//! value: the `node_uuid`, the `labels`, and a field for every property
//! column of the node's scan (19 fields on SNB). The `cypher_collect`
//! aggregate buffers one `ScalarValue::Struct` per input row and has no spill
//! path, so a collect over a large node set (the SNB Interactive IC5
//! `collect(friend)`) dominates memory and time.
//!
//! When every use of the collected list is identity-only, no consumer can
//! observe anything but each element's `node_uuid`. This pass then rewrites
//! the collect argument to the bare node variable, which lowers to the node's
//! `node_uuid` column, so the list becomes `List<FixedSizeBinary(16)>`.
//!
//! # Identity-only uses
//!
//! * `<node> IN list`, `NOT <node> IN list`, where `<node>` is a variable the
//!   plan proves is always a node;
//! * `size(list)`;
//! * `count(list)`.
//!
//! Every other reference to the list variable (UNWIND, list comprehension,
//! quantifier, indexing, any function other than `size`, projection,
//! property access, comparison, a pass-through `WITH list`, ...) keeps the
//! full collect. So does a list referenced from an `EXISTS`/pattern
//! comprehension child plan, and any plan containing a write operator.
//!
//! # Why the rewrite does not change a result
//!
//! * The collected elements. `lower_node_struct` wraps the struct in
//!   `null_unless(<node>.node_uuid IS NOT NULL, ...)`, and a bare node
//!   variable lowers to `<node>.node_uuid`. Both are NULL for the same rows
//!   and `cypher_collect` skips NULL inputs, so the two lists have the same
//!   length and the same element order.
//! * `<node> IN list`. The lowerer emits `cypher_in(lhs, list)`, which loops
//!   `cypher_in_elems` -> `cypher_value_eq` per element. A NULL operand or
//!   element makes the comparison NULL in both forms, before any type
//!   dispatch. For two non-NULL nodes every combination compares the same
//!   16 `node_uuid` bytes. A bare node variable on the left lowers to its
//!   `node_uuid` column, so the struct form takes the dedicated
//!   `(Struct(entity), FixedSizeBinary)` arm and the identity form compares
//!   two `FixedSizeBinary(16)` scalars. A whole-node struct on the left
//!   (a node that arrives as a struct column) takes `entity_identity_eq`
//!   against a struct element, and the same dedicated arm against an
//!   identity element. The membership test therefore needs the left operand
//!   to be a node (an edge uuid or a scalar is not comparable the same way),
//!   which is what the "always a node" analysis below guarantees.
//! * `size`/`count`. `CypherSize` dispatches on the list type, not the
//!   element type, and `count` reads only the list's own nullness.
//!
//! # Why the analysis is complete
//!
//! The walk destructures every [`GraphOp`] variant, and matches every
//! [`IrExpr`] variant, without a wildcard or `..` (write operators are the
//! one deliberate exception: any write anywhere skips the whole plan). A new
//! operator field or expression kind therefore fails to compile here until
//! this pass accounts for it, so a reference to the list cannot hide in an
//! expression slot the walk does not visit.

use super::patterns::NODE_STRUCT_CALL;
use crate::{
    AggExpr, AggFunc, BinaryOpKind, ExprArena, ExprId, GraphOp, GraphPlan, IrExpr, ProjectItem,
    VarId,
};
use std::collections::{HashMap, HashSet};

/// Rewrite every qualifying top-level `collect(<node>)` in `plan` to collect
/// the node's identity only. See the module documentation for the contract.
pub(super) fn narrow_node_collects(plan: &mut GraphPlan) {
    let mut census = Census::default();
    census.walk_plan(plan, true);
    if census.has_write {
        return;
    }
    census.settle();

    let mut rewrites: Vec<(usize, usize, VarId)> = Vec::new();
    for (op_index, op) in plan.ops.iter().enumerate() {
        let GraphOp::Aggregate { aggs, .. } = op else {
            continue;
        };
        for (agg_index, agg) in aggs.iter().enumerate() {
            if agg.func != AggFunc::Collect {
                continue;
            }
            let (Some(list_var), Some(arg)) = (agg.out_var, agg.arg) else {
                continue;
            };
            let Some(node_var) = node_struct_operand(&plan.exprs, arg) else {
                continue;
            };
            if census.qualifies(list_var, node_var) {
                rewrites.push((op_index, agg_index, node_var));
            }
        }
    }

    for (op_index, agg_index, node_var) in rewrites {
        let narrowed = plan.exprs.push(IrExpr::VarRef(node_var));
        if let GraphOp::Aggregate { aggs, .. } = &mut plan.ops[op_index] {
            aggs[agg_index].arg = Some(narrowed);
        }
    }
}

/// `_node_struct(VarRef(v), ...)` -> `v`.
fn node_struct_operand(arena: &ExprArena, id: ExprId) -> Option<VarId> {
    let IrExpr::FunctionCall { name, args } = arena.get(id) else {
        return None;
    };
    if name != NODE_STRUCT_CALL {
        return None;
    }
    match arena.get(*args.first()?) {
        IrExpr::VarRef(v) => Some(*v),
        _ => None,
    }
}

/// Facts gathered over the whole plan tree. VarIds are unique across one
/// bind, so the tree shares one census.
#[derive(Default)]
struct Census {
    /// A write operator exists somewhere in the tree.
    has_write: bool,
    /// Variables bound as nodes by `NodeScan` or as an `Expand` endpoint.
    node_bound: HashSet<VarId>,
    /// Variables that are, or may also be, something other than a node.
    not_node: HashSet<VarId>,
    /// `from -> to`: `to` is a bare re-projection of `from`.
    pass_through: Vec<(VarId, VarId)>,
    /// Variables named in a non-expression operator field or defined by a
    /// projection; such a variable is never a collect candidate.
    claimed: HashSet<VarId>,
    /// Variables referenced by an expression in a way that reads the value.
    read: HashSet<VarId>,
    /// `size(v)` / `count(v)` uses of `v`, and settled `x IN v` uses.
    identity_uses: HashMap<VarId, u32>,
    /// `(x, v)` for each `x IN v` seen where uses count; settled later.
    memberships: Vec<(VarId, VarId)>,
    /// How many aggregates in the tree define each output variable.
    aggregate_defs: HashMap<VarId, u32>,
    /// The nodes after the pass-through fixpoint.
    node_only: HashSet<VarId>,
}

impl Census {
    /// `counting`: identity-only uses of a list count toward qualifying it.
    /// It is false inside child plans (EXISTS, pattern comprehensions,
    /// UNION), which lower through their own correlation paths, so any
    /// reference from there keeps the full collect.
    fn walk_plan(&mut self, plan: &GraphPlan, counting: bool) {
        for op in &plan.ops {
            self.walk_op(&plan.exprs, op, counting);
        }
    }

    fn walk_op(&mut self, arena: &ExprArena, op: &GraphOp, counting: bool) {
        match op {
            GraphOp::NodeScan { .. }
            | GraphOp::EdgeScan { .. }
            | GraphOp::TypedEdgeScan { .. }
            | GraphOp::Expand { .. }
            | GraphOp::RelationshipUnique { .. } => self.walk_binding_op(op),
            GraphOp::Filter { predicate } => self.walk_expr(arena, *predicate, counting),
            GraphOp::Project { items, distinct: _ } => {
                self.walk_items(arena, items, counting);
            }
            GraphOp::With {
                items,
                distinct: _,
                where_predicate,
            } => {
                self.walk_items(arena, items, counting);
                if let Some(predicate) = where_predicate {
                    self.walk_expr(arena, *predicate, counting);
                }
            }
            GraphOp::Aggregate {
                group_by,
                group_aliases: _,
                group_vars,
                aggs,
            } => self.walk_aggregate(arena, group_by, group_vars, aggs, counting),
            GraphOp::Sort { keys } => {
                for key in keys {
                    self.walk_expr(arena, key.expr, counting);
                }
            }
            GraphOp::Limit { count: _ }
            | GraphOp::LimitParam { name: _ }
            | GraphOp::Skip { count: _ }
            | GraphOp::SkipParam { name: _ } => {}
            GraphOp::LimitExpr { expr } | GraphOp::SkipExpr { expr } => {
                self.walk_expr(arena, *expr, counting);
            }
            GraphOp::Optional { child } => self.walk_plan(child, counting),
            GraphOp::Exists { child, negated: _ } => self.walk_plan(child, false),
            GraphOp::PatternComprehension { child, output } => {
                self.define_non_node(*output);
                self.walk_plan(child, false);
            }
            GraphOp::ListElementPatternComprehension {
                list_expr,
                loop_var,
                child,
                pattern_output,
                filter,
                projection,
                output,
            } => {
                self.walk_expr(arena, *list_expr, false);
                for expr in [filter, projection].into_iter().flatten() {
                    self.walk_expr(arena, *expr, false);
                }
                for var in [loop_var, pattern_output, output] {
                    self.define_non_node(*var);
                }
                self.walk_plan(child, false);
            }
            GraphOp::Union { all: _, inputs } => {
                for input in inputs {
                    self.walk_plan(input, false);
                }
            }
            GraphOp::Unwind { list_expr, alias } => {
                self.walk_expr(arena, *list_expr, counting);
                self.define_non_node(*alias);
            }
            GraphOp::Call {
                procedure: _,
                args,
                yields,
            } => {
                for arg in args {
                    self.walk_expr(arena, *arg, counting);
                }
                for yielded in yields {
                    self.define_non_node(yielded.var);
                }
            }
            // A write plan evaluates expressions through the write
            // lowering, which this pass does not model.
            GraphOp::Create { .. }
            | GraphOp::Merge { .. }
            | GraphOp::Delete { .. }
            | GraphOp::Set { .. }
            | GraphOp::Remove { .. } => self.has_write = true,
        }
    }

    /// Operators that bind node or edge variables.
    fn walk_binding_op(&mut self, op: &GraphOp) {
        match op {
            GraphOp::NodeScan { var, ty: _ } => {
                self.node_bound.insert(*var);
                self.claimed.insert(*var);
            }
            GraphOp::EdgeScan { var, ty: _ } | GraphOp::TypedEdgeScan { var, rel_ty: _ } => {
                self.define_non_node(*var);
            }
            GraphOp::Expand {
                src,
                edge,
                dst,
                rel_ty: _,
                dir: _,
                min_hops: _,
                max_hops: _,
            } => {
                self.node_bound.insert(*src);
                self.node_bound.insert(*dst);
                self.claimed.insert(*src);
                self.claimed.insert(*dst);
                self.define_non_node(*edge);
            }
            GraphOp::RelationshipUnique { edge, prior_edges } => {
                self.define_non_node(*edge);
                for prior in prior_edges {
                    self.define_non_node(*prior);
                }
            }
            _ => {}
        }
    }

    fn walk_aggregate(
        &mut self,
        arena: &ExprArena,
        group_by: &[ExprId],
        group_vars: &[Option<VarId>],
        aggs: &[AggExpr],
        counting: bool,
    ) {
        for (index, key) in group_by.iter().enumerate() {
            self.walk_expr(arena, *key, counting);
            let Some(Some(out)) = group_vars.get(index) else {
                continue;
            };
            self.claimed.insert(*out);
            match arena.get(*key) {
                IrExpr::VarRef(from) => self.pass_through.push((*from, *out)),
                _ => {
                    self.not_node.insert(*out);
                }
            }
        }
        for agg in aggs {
            if let Some(out) = agg.out_var {
                *self.aggregate_defs.entry(out).or_insert(0) += 1;
                self.not_node.insert(out);
            }
            if let (AggFunc::Count, Some(arg)) = (agg.func, agg.arg)
                && counting
                && let IrExpr::VarRef(v) = arena.get(arg)
            {
                *self.identity_uses.entry(*v).or_insert(0) += 1;
            } else if let Some(arg) = agg.arg {
                self.walk_expr(arena, arg, counting);
            }
            if let Some(percentile) = agg.percentile {
                self.walk_expr(arena, percentile, counting);
            }
        }
    }

    fn define_non_node(&mut self, var: VarId) {
        self.not_node.insert(var);
        self.claimed.insert(var);
    }

    fn walk_items(&mut self, arena: &ExprArena, items: &[ProjectItem], counting: bool) {
        for item in items {
            self.walk_expr(arena, item.expr, counting);
            let Some(out) = item.out_var else {
                continue;
            };
            self.claimed.insert(out);
            match arena.get(item.expr) {
                IrExpr::VarRef(from) => self.pass_through.push((*from, out)),
                _ => {
                    self.not_node.insert(out);
                }
            }
        }
    }

    fn walk_expr(&mut self, arena: &ExprArena, id: ExprId, counting: bool) {
        match arena.get(id) {
            IrExpr::Literal(_) | IrExpr::Parameter(_) => {}
            IrExpr::VarRef(v) => {
                self.read.insert(*v);
            }
            IrExpr::PropertyAccess { base, prop: _ } => self.walk_expr(arena, *base, counting),
            IrExpr::BinaryOp { op, left, right } => {
                if counting
                    && *op == BinaryOpKind::In
                    && let (IrExpr::VarRef(member), IrExpr::VarRef(list)) =
                        (arena.get(*left), arena.get(*right))
                {
                    self.memberships.push((*member, *list));
                } else {
                    self.walk_expr(arena, *left, counting);
                    self.walk_expr(arena, *right, counting);
                }
            }
            IrExpr::UnaryOp { op: _, expr } => self.walk_expr(arena, *expr, counting),
            IrExpr::FunctionCall { name, args } => {
                if counting
                    && name.eq_ignore_ascii_case("size")
                    && let [only] = args.as_slice()
                    && let IrExpr::VarRef(v) = arena.get(*only)
                {
                    *self.identity_uses.entry(*v).or_insert(0) += 1;
                } else {
                    for arg in args {
                        self.walk_expr(arena, *arg, counting);
                    }
                }
            }
            IrExpr::Case {
                operand,
                arms,
                else_expr,
            } => {
                for expr in operand.iter().chain(else_expr) {
                    self.walk_expr(arena, *expr, counting);
                }
                for arm in arms {
                    self.walk_expr(arena, arm.when, counting);
                    self.walk_expr(arena, arm.then, counting);
                }
            }
            IrExpr::ListLiteral(items) => {
                for item in items {
                    self.walk_expr(arena, *item, counting);
                }
            }
            IrExpr::MapLiteral(entries) => {
                for (_, value) in entries {
                    self.walk_expr(arena, *value, counting);
                }
            }
            IrExpr::Quantifier {
                kind: _,
                loop_var,
                list,
                predicate,
            } => {
                self.define_non_node(*loop_var);
                self.walk_expr(arena, *list, counting);
                self.walk_expr(arena, *predicate, counting);
            }
            IrExpr::ListComprehension {
                loop_var,
                list,
                filter,
                projection,
            } => {
                self.define_non_node(*loop_var);
                self.walk_expr(arena, *list, counting);
                for expr in [filter, projection].into_iter().flatten() {
                    self.walk_expr(arena, *expr, counting);
                }
            }
        }
    }

    /// Resolve the pass-through chains and the recorded `x IN list` uses.
    fn settle(&mut self) {
        let mut node = self.node_bound.clone();
        let mut not_node = self.not_node.clone();
        loop {
            let mut changed = false;
            for (from, to) in &self.pass_through {
                if node.contains(from) {
                    changed |= node.insert(*to);
                }
                if not_node.contains(from) {
                    changed |= not_node.insert(*to);
                }
            }
            if !changed {
                break;
            }
        }
        self.node_only = node.difference(&not_node).copied().collect();

        for (member, list) in std::mem::take(&mut self.memberships) {
            if self.node_only.contains(&member) {
                *self.identity_uses.entry(list).or_insert(0) += 1;
            } else {
                // The left operand is not provably a node, so the identity
                // list could compare differently from the full one. The
                // operand is itself read here: it may be another collected
                // list (`ps IN qs`), which then needs its full value too.
                self.read.insert(list);
                self.read.insert(member);
            }
        }
    }

    fn qualifies(&self, list_var: VarId, node_var: VarId) -> bool {
        self.aggregate_defs.get(&list_var) == Some(&1)
            && !self.claimed.contains(&list_var)
            && !self.read.contains(&list_var)
            && self.identity_uses.get(&list_var).is_some_and(|n| *n > 0)
            && self.node_only.contains(&node_var)
    }
}

#[cfg(test)]
mod tests;
