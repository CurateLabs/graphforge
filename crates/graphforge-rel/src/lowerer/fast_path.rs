//! Choose the adjacency fast paths from the Graph IR (ADR 0050).
//!
//! The decision is taken from the binder's operator list, which DataFusion never
//! rewrites, so partitioning and transport operators cannot remove a fast path
//! (#1513). Only whole statements with exactly these shapes qualify:
//!
//! - `MATCH ()-[r]->() RETURN count(r)` (also `count(*)`, `count(1)`, a renaming
//!   `WITH`, and a positive `LIMIT` on the single result row)
//! - `MATCH (a)-[r]->(b) RETURN b.node_uuid AS id ORDER BY id LIMIT k`
//! - `MATCH (a)-[r1]->(b)-[r2]->(c) RETURN c.node_uuid AS id ORDER BY id LIMIT k`
//!
//! Every node scan must be unlabelled and unfiltered, so the first scan is a
//! complete frontier by construction; every hop is a single outgoing hop.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::logical_expr::{Extension, LogicalPlan};
use graphforge_ir::{
    AggFunc, Direction, ExprArena, ExprId, GraphOp, GraphPlan, IrExpr, IrLiteral, PropertyId,
    SortOrder, VarId,
};
use graphforge_plan::fast_path::{FastPathKind, FastPathNode};

use super::GraphPlanLowerer;

impl GraphPlanLowerer {
    /// Wrap `lowered` in a [`FastPathNode`] when `plan` has a fast-path shape.
    pub(super) fn wrap_structural_fast_path(
        &self,
        plan: &GraphPlan,
        lowered: LogicalPlan,
    ) -> LogicalPlan {
        if self.read_snapshot.is_none() || self.relational_reference() {
            return lowered;
        }
        let Some(kind) = detect(&plan.ops, &plan.exprs, &self.prop_names()) else {
            return lowered;
        };
        LogicalPlan::Extension(Extension {
            node: Arc::new(FastPathNode::new(Arc::new(lowered), kind)),
        })
    }
}

/// One outgoing single hop `src -[edge]-> dst` of relation type `rel_ty`.
struct Hop {
    src: VarId,
    edge: VarId,
    dst: VarId,
    rel_ty: Option<graphforge_ir::RelationTypeId>,
}

fn detect(
    ops: &[GraphOp],
    exprs: &ExprArena,
    prop_names: &HashMap<PropertyId, String>,
) -> Option<FastPathKind> {
    edge_count(ops, exprs)
        .or_else(|| ordered_one_hop(ops, exprs, prop_names))
        .or_else(|| ordered_two_hop(ops, exprs, prop_names))
}

/// `count` of every outgoing match: `count(*)`, `count` of a pattern variable,
/// or `count` of a non-null literal, optionally after a `WITH` that only
/// renames pattern variables, optionally under a positive `LIMIT`.
fn edge_count(ops: &[GraphOp], exprs: &ExprArena) -> Option<FastPathKind> {
    let [scan, expand, dst_scan, rest @ ..] = ops else {
        return None;
    };
    let hop = first_hop(scan, expand, dst_scan)?;
    let mut counted = vec![hop.src, hop.edge, hop.dst];
    let mut rest = rest;
    if let [
        GraphOp::With {
            items,
            distinct: false,
            where_predicate: None,
            ..
        },
        tail @ ..,
    ] = rest
    {
        let mut renamed = Vec::with_capacity(items.len());
        for item in items {
            match exprs.get(item.expr) {
                IrExpr::VarRef(var) if counted.contains(var) => renamed.push(item.out_var?),
                _ => return None,
            }
        }
        counted = renamed;
        rest = tail;
    }
    let (GraphOp::Aggregate { group_by, aggs, .. }, tail) = rest.split_first()? else {
        return None;
    };
    match tail {
        [] => {}
        [GraphOp::Limit { count }] if *count > 0 => {}
        _ => return None,
    }
    let [agg] = aggs.as_slice() else {
        return None;
    };
    let counts_matches = matches!(agg.func, AggFunc::Count)
        && agg.percentile.is_none()
        && agg.arg.is_none_or(|arg| match exprs.get(arg) {
            IrExpr::VarRef(var) => counted.contains(var),
            IrExpr::Literal(literal) => !matches!(literal, IrLiteral::Null),
            _ => false,
        });
    (group_by.is_empty() && counts_matches).then_some(FastPathKind::EdgeCount)
}

fn ordered_one_hop(
    ops: &[GraphOp],
    exprs: &ExprArena,
    prop_names: &HashMap<PropertyId, String>,
) -> Option<FastPathKind> {
    let [scan, expand, dst_scan, sort, project, limit] = ops else {
        return None;
    };
    let hop = first_hop(scan, expand, dst_scan)?;
    let fetch = ordered_uuid_tail(sort, project, limit, hop.dst, exprs, prop_names)?;
    Some(FastPathKind::OrderedOneHop { fetch })
}

fn ordered_two_hop(
    ops: &[GraphOp],
    exprs: &ExprArena,
    prop_names: &HashMap<PropertyId, String>,
) -> Option<FastPathKind> {
    let [scan, expand1, mid_scan, expand2, rest @ ..] = ops else {
        return None;
    };
    let first = first_hop(scan, expand1, mid_scan)?;
    let (unique, dst_scan, sort, project, limit) = match rest {
        [unique, dst_scan, sort, project, limit] => (Some(unique), dst_scan, sort, project, limit),
        [dst_scan, sort, project, limit] => (None, dst_scan, sort, project, limit),
        _ => return None,
    };
    let second = single_out_hop(expand2)?;
    if second.src != first.dst
        || second.rel_ty != first.rel_ty
        || second.edge == first.edge
        || [first.src, first.dst].contains(&second.dst)
        || !is_complete_scan(dst_scan, second.dst)
    {
        return None;
    }
    let require_edge_disjoint = match unique {
        None => false,
        Some(GraphOp::RelationshipUnique { edge, prior_edges })
            if *edge == second.edge && prior_edges.as_slice() == [first.edge] =>
        {
            true
        }
        Some(_) => return None,
    };
    let fetch = ordered_uuid_tail(sort, project, limit, second.dst, exprs, prop_names)?;
    Some(FastPathKind::OrderedTwoHop {
        fetch,
        require_edge_disjoint,
    })
}

/// `NodeScan(src) Expand(src -> dst) NodeScan(dst)` with both scans complete.
fn first_hop(scan: &GraphOp, expand: &GraphOp, dst_scan: &GraphOp) -> Option<Hop> {
    let hop = single_out_hop(expand)?;
    (hop.src != hop.dst && is_complete_scan(scan, hop.src) && is_complete_scan(dst_scan, hop.dst))
        .then_some(hop)
}

fn single_out_hop(op: &GraphOp) -> Option<Hop> {
    match op {
        GraphOp::Expand {
            src,
            edge,
            dst,
            rel_ty,
            dir: Direction::Out,
            min_hops: 1,
            max_hops: Some(1),
        } => Some(Hop {
            src: *src,
            edge: *edge,
            dst: *dst,
            rel_ty: *rel_ty,
        }),
        _ => None,
    }
}

/// An unlabelled scan of `var`: every node, no filter.
fn is_complete_scan(op: &GraphOp, var: VarId) -> bool {
    matches!(op, GraphOp::NodeScan { var: v, ty: None } if *v == var)
}

/// `Sort[dst.node_uuid ASC] Project[dst.node_uuid] Limit k`, returning `k`.
fn ordered_uuid_tail(
    sort: &GraphOp,
    project: &GraphOp,
    limit: &GraphOp,
    dst: VarId,
    exprs: &ExprArena,
    prop_names: &HashMap<PropertyId, String>,
) -> Option<usize> {
    let (
        GraphOp::Sort { keys },
        GraphOp::Project {
            items,
            distinct: false,
        },
        GraphOp::Limit { count },
    ) = (sort, project, limit)
    else {
        return None;
    };
    let ([key], [item]) = (keys.as_slice(), items.as_slice()) else {
        return None;
    };
    let fetch = usize::try_from(*count).ok().filter(|fetch| *fetch > 0)?;
    (matches!(key.order, SortOrder::Asc)
        && is_node_uuid(key.expr, dst, exprs, prop_names)
        && is_node_uuid(item.expr, dst, exprs, prop_names))
    .then_some(fetch)
}

fn is_node_uuid(
    expr: ExprId,
    var: VarId,
    exprs: &ExprArena,
    prop_names: &HashMap<PropertyId, String>,
) -> bool {
    let IrExpr::PropertyAccess { base, prop } = exprs.get(expr) else {
        return false;
    };
    matches!(exprs.get(*base), IrExpr::VarRef(v) if *v == var)
        && prop_names.get(prop).is_some_and(|name| name == "node_uuid")
}
