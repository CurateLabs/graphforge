//! Reuse a grouped extremum instead of re-matching the pattern for it (#1982).
//!
//! The "latest row per group, ties broken by id" idiom matches one pattern
//! twice. The first `MATCH ... WITH g, max(e) AS t` finds each group's
//! extremum; the second `MATCH <same pattern> WHERE ... AND e = t
//! WITH g, t, min(id)` walks the same pattern again to find which rows hit it.
//! SNB Interactive IC7 is the canonical instance: the second expansion of
//! `(person)<-[:HAS_CREATOR]-(message)<-[:LIKES]-(liker)` re-reads every like
//! of every message of the person, which is the dominant cost of the query.
//!
//! This pass proves the two stages match the same rows and replaces the second
//! match with a pass over rows the first stage already produced:
//!
//! ```text
//! A1: group(g) -> max(e) AS t,  collect({e: e, id: id}) AS rows       (one expansion)
//! UNWIND rows AS p
//! WHERE p.e = t
//! A2: group(g, t) -> min(p.id)
//! ```
//!
//! # Where the rewrite lives
//!
//! The binder, after the plan is built, next to `narrow_node_collects`. The
//! binder is the one place the whole pipeline is visible as a flat operator
//! list before it is split: `graphforge-plan` holds only DataFusion extension
//! nodes and sees no IR; the relational lowerer turns each operator into a
//! DataFusion node immediately, so by the optimizer the two stages are a join
//! tree whose equality would have to be re-derived from plan shapes; and the
//! IR has no window or arg-max operator to express the one-pass form directly,
//! so the rewrite is an exact IR-to-IR reshaping into operators that already
//! exist (`Aggregate`, `Unwind`, `Filter`).
//!
//! # The exact precondition
//!
//! All of the following are checked on the bound IR; any miss leaves the plan
//! untouched.
//!
//! * The plan begins with a pattern region (`NodeScan`, `Expand`,
//!   `RelationshipUnique`, `Filter` only), then an `Aggregate` A1, then a
//!   second pattern region, then an `Aggregate` A2.
//! * A1 groups only by bare node variables that it passes through (`g`) and
//!   has the single aggregate `max(e)` (`t`). A2 groups only by `g` or `t`
//!   and has the single aggregate `min(<m>.<id>)`.
//! * The two regions have the same operators in the same order (types,
//!   directions, single-hop expansions) under a renaming of variables that
//!   fixes `g`. A second-stage scan of a `g` variable may drop its label (the
//!   first stage already proved it) but never add one.
//! * The second WHERE is the first WHERE, plus exactly one conjunct,
//!   `e' = t`, where `e'` is `e` under the renaming. Conjuncts present only in
//!   the first stage must read nothing but `g` (the IC7 `person.id = $id`)
//!   and be deterministic.
//! * Every function either WHERE calls is in a fixed deterministic allowlist
//!   (no `rand()`, no `timestamp()`, no `id()`), and neither WHERE contains a
//!   quantifier or list comprehension.
//! * `e` is a property read and `<m>.<id>` is a property read of a variable
//!   the first stage binds, with distinct, catalog-resolvable property names.
//!
//! # Why the result is identical
//!
//! * Same rows. Let `R(g)` be the first stage's rows for group `g`: the
//!   pattern's matches with `g` fixed that pass the first WHERE. The second
//!   stage's rows for `g` are the pattern's matches with `g` fixed that pass
//!   the second WHERE. The two WHEREs differ by `e = t` and by conjuncts that
//!   are constant on `g` and already true because the group exists, so the
//!   second stage's rows are `{ r in R(g) : e(r) = t(g) }` as a multiset.
//!   Duplicate matches (two `LIKES` edges) stay duplicates.
//! * NULL. The collected element is a map, never NULL, so a row whose `e` is
//!   NULL is kept in the list and dropped by `p.e = t` exactly as `e = t` is
//!   NULL in the original. A group whose maximum is NULL has `t` NULL, so no
//!   row survives either form. `max` already ignores NULL `e`.
//! * Ties. All rows of `g` with `e = t` reach the same group of A2, so
//!   `min(id)` ranges over the same set.
//! * Errors. Every conjunct the second stage evaluated is evaluated by the
//!   first stage on the same rows, so no error can be introduced or removed.
//!   NaN and mixed numeric types compare through the same `=` on the same
//!   values, because the struct field carries the value unchanged.
//!
//! # Cost
//!
//! The collected list holds one two-field map per first-stage row of the
//! group, where the original streams `max` in constant space. That is the
//! price of a single expansion; `collect` has no spill path. Retained nested
//! values are compacted to their own buffers, and the aggregate reports their
//! buffers and collection capacities to DataFusion's query memory reservation.
//! A group that cannot fit the query pool fails with a resource-exhaustion
//! error instead of retaining the complete group. Memory checks occur after
//! each input batch, so a batch's transient allocations can exceed the pool;
//! the pool is not a hard cap on the process's resident memory. A query that
//! the original streaming `max` answered in a small pool can therefore fail.

use crate::catalog::RuntimeCatalog;
use crate::reachability::{conjuncts, visit};
use crate::{
    AggExpr, AggFunc, BinaryOpKind, ExprArena, ExprId, GraphOp, GraphPlan, IrExpr, PropertyId,
    VarId, referenced_vars,
};
use graphforge_value::Literal;
use std::collections::{HashMap, HashSet};

/// Functions whose result depends only on their arguments. Anything else may
/// be volatile or read hidden state, so a plan using it is never rewritten.
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
    "labels",
    "type",
];

/// Rewrite the first qualifying extremum re-match in `plan`. See the module
/// documentation for the contract.
pub(super) fn rematch_extremum(plan: &mut GraphPlan, catalog: &RuntimeCatalog, next_var: &mut u32) {
    if let Some(found) = detect(plan, catalog) {
        apply(plan, &found, next_var);
    }
}

/// What `detect` proved and `apply` needs.
struct Found {
    /// Index of the first aggregate.
    first_agg: usize,
    /// Index of the second aggregate.
    second_agg: usize,
    /// The first aggregate's argument `e`.
    extremum_arg: ExprId,
    /// The property `e` reads, and its catalog name.
    extremum_prop: PropertyId,
    extremum_name: String,
    /// The first-stage variable and property the tie-break reads, with the
    /// property's catalog name.
    id_var: VarId,
    id_prop: PropertyId,
    id_name: String,
    /// The extremum variable `t`.
    extremum_var: VarId,
    /// The second WHERE wrote `t = e'` rather than `e' = t`.
    extremum_on_left: bool,
}

fn is_pattern_op(op: &GraphOp) -> bool {
    matches!(
        op,
        GraphOp::NodeScan { .. }
            | GraphOp::Expand { .. }
            | GraphOp::RelationshipUnique { .. }
            | GraphOp::Filter { .. }
    )
}

/// Shape operators of a region (everything but filters) and its flattened
/// conjuncts.
struct Region<'a> {
    shape: Vec<&'a GraphOp>,
    conjuncts: Vec<ExprId>,
}

fn region<'a>(exprs: &ExprArena, ops: &'a [GraphOp]) -> Region<'a> {
    let mut shape = Vec::new();
    let mut conj = Vec::new();
    for op in ops {
        match op {
            GraphOp::Filter { predicate } => conj.extend(conjuncts(exprs, *predicate)),
            other => shape.push(other),
        }
    }
    Region {
        shape,
        conjuncts: conj,
    }
}

fn detect(plan: &GraphPlan, catalog: &RuntimeCatalog) -> Option<Found> {
    let ops = &plan.ops;
    let exprs = &plan.exprs;

    let first_agg = ops.iter().position(|op| !is_pattern_op(op))?;
    let second_len = ops[first_agg + 1..]
        .iter()
        .position(|op| !is_pattern_op(op))?;
    if first_agg == 0 || second_len == 0 {
        return None;
    }
    let second_agg = first_agg + 1 + second_len;

    let GraphOp::Aggregate {
        group_by: second_by,
        aggs: second_aggs,
        ..
    } = &ops[second_agg]
    else {
        return None;
    };
    let (carried, extremum_arg, extremum_var) = first_stage(exprs, &ops[first_agg])?;

    // A2: group only by `g` or `t`; one `min`.
    for by in second_by {
        match exprs.get(*by) {
            IrExpr::VarRef(v) if carried.contains(v) || *v == extremum_var => {}
            _ => return None,
        }
    }
    let [min_agg] = second_aggs.as_slice() else {
        return None;
    };
    if min_agg.func != AggFunc::Min || min_agg.percentile.is_some() {
        return None;
    }
    let IrExpr::PropertyAccess {
        base: min_base,
        prop: id_prop,
    } = exprs.get(min_agg.arg?)
    else {
        return None;
    };
    let IrExpr::VarRef(min_var) = exprs.get(*min_base) else {
        return None;
    };

    // The two pattern regions.
    let first = region(exprs, &ops[..first_agg]);
    let second = region(exprs, &ops[first_agg + 1..second_agg]);
    let mut rename = Rename::new(&first, &carried, extremum_var)?;
    if first.shape.len() != second.shape.len() {
        return None;
    }
    for (one, two) in first.shape.iter().zip(&second.shape) {
        if !rename.same_shape(one, two) {
            return None;
        }
    }

    // Conjuncts: the second WHERE is the first plus exactly `e' = t`.
    let extra = extra_conjunct(exprs, &first, &second, &rename, &carried)?;
    let IrExpr::BinaryOp {
        op: BinaryOpKind::Eq,
        left,
        right,
    } = exprs.get(extra)
    else {
        return None;
    };
    let is_extremum = |id: ExprId| matches!(exprs.get(id), IrExpr::VarRef(v) if *v == extremum_var);
    let (renamed_extremum, extremum_on_left) = match (is_extremum(*left), is_extremum(*right)) {
        (false, true) => (*left, false),
        (true, false) => (*right, true),
        _ => return None,
    };
    if !same_expr(exprs, extremum_arg, renamed_extremum, &rename) {
        return None;
    }

    // The extremum and the tie-break must be properties with distinct names
    // the lowerer can read back out of a plain-map column.
    let IrExpr::PropertyAccess {
        prop: extremum_prop,
        ..
    } = exprs.get(extremum_arg)
    else {
        return None;
    };
    let extremum_name = runtime_property_name(catalog, *extremum_prop)?;
    let id_name = runtime_property_name(catalog, *id_prop)?;
    if extremum_name == id_name {
        return None;
    }
    let id_var = rename.to_first(*min_var)?;

    Some(Found {
        first_agg,
        second_agg,
        extremum_arg,
        extremum_prop: *extremum_prop,
        extremum_name,
        id_var,
        id_prop: *id_prop,
        id_name,
        extremum_var,
        extremum_on_left,
    })
}

/// The first aggregate's shape: group only by passed-through bare variables
/// and compute one `max`. Returns the carried variables, the `max` argument and
/// the variable the `max` is bound to.
fn first_stage(exprs: &ExprArena, op: &GraphOp) -> Option<(HashSet<VarId>, ExprId, VarId)> {
    let GraphOp::Aggregate {
        group_by,
        group_aliases,
        group_vars,
        aggs,
    } = op
    else {
        return None;
    };
    if group_by.is_empty()
        || group_by.len() != group_aliases.len()
        || group_by.len() != group_vars.len()
    {
        return None;
    }
    let mut carried: HashSet<VarId> = HashSet::new();
    for ((by, alias), var) in group_by.iter().zip(group_aliases).zip(group_vars) {
        let IrExpr::VarRef(v) = exprs.get(*by) else {
            return None;
        };
        if alias.is_some() || *var != Some(*v) {
            return None;
        }
        carried.insert(*v);
    }
    let [max_agg] = aggs.as_slice() else {
        return None;
    };
    if max_agg.func != AggFunc::Max || max_agg.percentile.is_some() {
        return None;
    }
    Some((carried, max_agg.arg?, max_agg.out_var?))
}

/// The one second-stage conjunct the first stage lacks, provided every other
/// second-stage conjunct matches a distinct first-stage conjunct under the
/// renaming and every first-stage conjunct left over only reads carried
/// variables deterministically (so the second stage's rows are a superset).
fn extra_conjunct(
    exprs: &ExprArena,
    first: &Region<'_>,
    second: &Region<'_>,
    rename: &Rename<'_>,
    carried: &HashSet<VarId>,
) -> Option<ExprId> {
    let mut claimed = vec![false; first.conjuncts.len()];
    let mut extra = Vec::new();
    for &conjunct in &second.conjuncts {
        let hit = (0..first.conjuncts.len())
            .find(|&i| !claimed[i] && same_expr(exprs, first.conjuncts[i], conjunct, rename));
        match hit {
            Some(i) => claimed[i] = true,
            None => extra.push(conjunct),
        }
    }
    let [extra] = extra.as_slice() else {
        return None;
    };
    for (i, &conjunct) in first.conjuncts.iter().enumerate() {
        let carried_only = deterministic(exprs, conjunct)
            && referenced_vars(exprs, conjunct)
                .iter()
                .all(|v| carried.contains(v));
        if !claimed[i] && !carried_only {
            return None;
        }
    }
    Some(*extra)
}

fn runtime_property_name(catalog: &RuntimeCatalog, prop: PropertyId) -> Option<String> {
    catalog.property_name(prop.runtime_id()?).map(str::to_owned)
}

/// A bijection from the second region's variables to the first region's, with
/// the carried variables fixed.
struct Rename<'a> {
    carried: &'a HashSet<VarId>,
    /// Variables the second region may not reuse as its own.
    reserved: HashSet<VarId>,
    second_to_first: HashMap<VarId, VarId>,
    first_to_second: HashMap<VarId, VarId>,
}

impl<'a> Rename<'a> {
    /// Collect the first region's variables and check the carried ones are
    /// nodes there.
    fn new(first: &Region<'_>, carried: &'a HashSet<VarId>, extremum: VarId) -> Option<Self> {
        let mut reserved: HashSet<VarId> = HashSet::from([extremum]);
        let mut nodes: HashSet<VarId> = HashSet::new();
        let mut edges: HashSet<VarId> = HashSet::new();
        for op in &first.shape {
            match op {
                GraphOp::NodeScan { var, .. } => {
                    nodes.insert(*var);
                }
                GraphOp::Expand { src, edge, dst, .. } => {
                    nodes.extend([*src, *dst]);
                    edges.insert(*edge);
                }
                GraphOp::RelationshipUnique { edge, prior_edges } => {
                    edges.insert(*edge);
                    edges.extend(prior_edges);
                }
                _ => return None,
            }
        }
        if !carried
            .iter()
            .all(|v| nodes.contains(v) && !edges.contains(v))
        {
            return None;
        }
        reserved.extend(nodes);
        reserved.extend(edges);
        Some(Self {
            carried,
            reserved,
            second_to_first: HashMap::new(),
            first_to_second: HashMap::new(),
        })
    }

    /// Record `second -> first`; false when it contradicts the bijection.
    fn bind(&mut self, second: VarId, first: VarId) -> bool {
        if self.carried.contains(&second) || self.carried.contains(&first) {
            return second == first;
        }
        if self.reserved.contains(&second) {
            return false;
        }
        if let Some(known) = self.second_to_first.get(&second) {
            return *known == first;
        }
        if self.first_to_second.contains_key(&first) {
            return false;
        }
        self.second_to_first.insert(second, first);
        self.first_to_second.insert(first, second);
        true
    }

    /// The first-stage variable a second-stage variable stands for.
    fn to_first(&self, second: VarId) -> Option<VarId> {
        if self.carried.contains(&second) {
            Some(second)
        } else {
            self.second_to_first.get(&second).copied()
        }
    }

    fn same_shape(&mut self, first: &GraphOp, second: &GraphOp) -> bool {
        match (first, second) {
            (GraphOp::NodeScan { var: v1, ty: t1 }, GraphOp::NodeScan { var: v2, ty: t2 }) => {
                let label_ok = if self.carried.contains(v1) {
                    t2.is_none() || t2 == t1
                } else {
                    t2 == t1
                };
                label_ok && self.bind(*v2, *v1)
            }
            (
                GraphOp::Expand {
                    src: s1,
                    edge: e1,
                    dst: d1,
                    rel_ty: r1,
                    dir: dir1,
                    min_hops: lo1,
                    max_hops: hi1,
                },
                GraphOp::Expand {
                    src: s2,
                    edge: e2,
                    dst: d2,
                    rel_ty: r2,
                    dir: dir2,
                    min_hops: lo2,
                    max_hops: hi2,
                },
            ) => {
                r1 == r2
                    && dir1 == dir2
                    && (*lo1, *hi1) == (1, Some(1))
                    && (*lo2, *hi2) == (1, Some(1))
                    && self.bind(*s2, *s1)
                    && self.bind(*e2, *e1)
                    && self.bind(*d2, *d1)
            }
            (
                GraphOp::RelationshipUnique {
                    edge: e1,
                    prior_edges: p1,
                },
                GraphOp::RelationshipUnique {
                    edge: e2,
                    prior_edges: p2,
                },
            ) => {
                p1.len() == p2.len()
                    && self.bind(*e2, *e1)
                    && p1.iter().zip(p2).all(|(a, b)| self.bind(*b, *a))
            }
            _ => false,
        }
    }
}

fn deterministic_name(name: &str) -> bool {
    DETERMINISTIC_FUNCTIONS
        .iter()
        .any(|allowed| name.eq_ignore_ascii_case(allowed))
}

/// True when `id` calls only deterministic functions and binds no loop
/// variable.
fn deterministic(exprs: &ExprArena, id: ExprId) -> bool {
    let mut ok = true;
    visit(exprs, id, &mut |expr| match expr {
        IrExpr::FunctionCall { name, .. } => ok &= deterministic_name(name),
        IrExpr::Quantifier { .. } | IrExpr::ListComprehension { .. } => ok = false,
        _ => {}
    });
    ok
}

fn same_literal(first: &Literal, second: &Literal) -> bool {
    match (first, second) {
        (Literal::Float(a), Literal::Float(b)) => a.to_bits() == b.to_bits(),
        _ => first == second,
    }
}

fn same_all(exprs: &ExprArena, first: &[ExprId], second: &[ExprId], rename: &Rename<'_>) -> bool {
    first.len() == second.len()
        && first
            .iter()
            .zip(second)
            .all(|(a, b)| same_expr(exprs, *a, *b, rename))
}

fn same_opt(
    exprs: &ExprArena,
    first: Option<ExprId>,
    second: Option<ExprId>,
    rename: &Rename<'_>,
) -> bool {
    match (first, second) {
        (None, None) => true,
        (Some(a), Some(b)) => same_expr(exprs, a, b, rename),
        _ => false,
    }
}

/// Whether `second` (a second-stage expression) is `first` (a first-stage
/// expression) once its variables are renamed. Deterministic and loop-free
/// expressions only; anything else is "not the same".
fn same_expr(exprs: &ExprArena, first: ExprId, second: ExprId, rename: &Rename<'_>) -> bool {
    match (exprs.get(first), exprs.get(second)) {
        (IrExpr::Literal(a), IrExpr::Literal(b)) => same_literal(a, b),
        (IrExpr::VarRef(a), IrExpr::VarRef(b)) => rename.to_first(*b) == Some(*a),
        (
            IrExpr::PropertyAccess { base: ba, prop: pa },
            IrExpr::PropertyAccess { base: bb, prop: pb },
        ) => pa == pb && same_expr(exprs, *ba, *bb, rename),
        (
            IrExpr::BinaryOp {
                op: oa,
                left: la,
                right: ra,
            },
            IrExpr::BinaryOp {
                op: ob,
                left: lb,
                right: rb,
            },
        ) => oa == ob && same_expr(exprs, *la, *lb, rename) && same_expr(exprs, *ra, *rb, rename),
        (IrExpr::UnaryOp { op: oa, expr: ea }, IrExpr::UnaryOp { op: ob, expr: eb }) => {
            oa == ob && same_expr(exprs, *ea, *eb, rename)
        }
        (
            IrExpr::FunctionCall { name: na, args: aa },
            IrExpr::FunctionCall { name: nb, args: ab },
        ) => {
            na.eq_ignore_ascii_case(nb) && deterministic_name(na) && same_all(exprs, aa, ab, rename)
        }
        (IrExpr::Parameter(a), IrExpr::Parameter(b)) => a == b,
        (
            IrExpr::Case {
                operand: oa,
                arms: arms_a,
                else_expr: ea,
            },
            IrExpr::Case {
                operand: ob,
                arms: arms_b,
                else_expr: eb,
            },
        ) => {
            same_opt(exprs, *oa, *ob, rename)
                && same_opt(exprs, *ea, *eb, rename)
                && arms_a.len() == arms_b.len()
                && arms_a.iter().zip(arms_b).all(|(a, b)| {
                    same_expr(exprs, a.when, b.when, rename)
                        && same_expr(exprs, a.then, b.then, rename)
                })
        }
        (IrExpr::ListLiteral(a), IrExpr::ListLiteral(b)) => same_all(exprs, a, b, rename),
        (IrExpr::MapLiteral(a), IrExpr::MapLiteral(b)) => {
            a.len() == b.len()
                && a.iter()
                    .zip(b)
                    .all(|((ka, va), (kb, vb))| ka == kb && same_expr(exprs, *va, *vb, rename))
        }
        // A loop variable has a distinct id in each stage, so two copies are
        // never "the same" under the renaming; refuse rather than reason. The
        // remaining arms are every other pairing of unlike expression kinds.
        (
            IrExpr::Quantifier { .. }
            | IrExpr::ListComprehension { .. }
            | IrExpr::Literal(_)
            | IrExpr::VarRef(_)
            | IrExpr::PropertyAccess { .. }
            | IrExpr::BinaryOp { .. }
            | IrExpr::UnaryOp { .. }
            | IrExpr::FunctionCall { .. }
            | IrExpr::Parameter(_)
            | IrExpr::Case { .. }
            | IrExpr::ListLiteral(_)
            | IrExpr::MapLiteral(_),
            _,
        ) => false,
    }
}

fn apply(plan: &mut GraphPlan, found: &Found, next_var: &mut u32) {
    let list_var = VarId(*next_var);
    let item_var = VarId(*next_var + 1);
    *next_var += 2;

    // Aggregate aliases are physical column names, unlike VarIds. A user can
    // name `max(e)` exactly like our generated list, so select an unused name
    // in the aggregate that introduces it before adding the new expression.
    let GraphOp::Aggregate {
        aggs,
        group_aliases,
        ..
    } = &plan.ops[found.first_agg]
    else {
        unreachable!("detect proved the first aggregate");
    };
    let stem = format!("_extremum_rematch_{}", list_var.0);
    let mut list_alias = stem.clone();
    let mut suffix = 0;
    while aggs.iter().any(|agg| agg.alias == list_alias)
        || group_aliases
            .iter()
            .flatten()
            .any(|alias| *alias == list_alias)
    {
        suffix += 1;
        list_alias = format!("{stem}_{suffix}");
    }

    let exprs = &mut plan.exprs;
    let id_base = exprs.push(IrExpr::VarRef(found.id_var));
    let id_access = exprs.push(IrExpr::PropertyAccess {
        base: id_base,
        prop: found.id_prop,
    });
    let row = exprs.push(IrExpr::MapLiteral(vec![
        (found.extremum_name.clone(), found.extremum_arg),
        (found.id_name.clone(), id_access),
    ]));
    let rows = exprs.push(IrExpr::VarRef(list_var));
    let row_ref = exprs.push(IrExpr::VarRef(item_var));
    let row_extremum = exprs.push(IrExpr::PropertyAccess {
        base: row_ref,
        prop: found.extremum_prop,
    });
    let extremum = exprs.push(IrExpr::VarRef(found.extremum_var));
    let (left, right) = if found.extremum_on_left {
        (extremum, row_extremum)
    } else {
        (row_extremum, extremum)
    };
    let hit = exprs.push(IrExpr::BinaryOp {
        op: BinaryOpKind::Eq,
        left,
        right,
    });
    let row_ref_id = exprs.push(IrExpr::VarRef(item_var));
    let row_id = exprs.push(IrExpr::PropertyAccess {
        base: row_ref_id,
        prop: found.id_prop,
    });

    let mut ops = std::mem::take(&mut plan.ops);
    let tail = ops.split_off(found.second_agg + 1);
    let mut second = ops.pop().expect("second aggregate");
    ops.truncate(found.first_agg + 1);
    let first = ops.last_mut().expect("first aggregate");

    if let GraphOp::Aggregate { aggs, .. } = first {
        aggs.push(AggExpr {
            func: AggFunc::Collect,
            arg: Some(row),
            percentile: None,
            alias: list_alias,
            out_var: Some(list_var),
        });
    }
    if let GraphOp::Aggregate { aggs, .. } = &mut second {
        aggs[0].arg = Some(row_id);
    }
    ops.push(GraphOp::Unwind {
        list_expr: rows,
        alias: item_var,
    });
    ops.push(GraphOp::Filter { predicate: hit });
    ops.push(second);
    ops.extend(tail);
    plan.ops = ops;
}

#[cfg(test)]
mod tests;
