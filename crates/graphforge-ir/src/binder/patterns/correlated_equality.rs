//! Planning a MATCH path from a node that a correlated equality pins.
//!
//! The binder emits a pattern's operations in textual order, so
//! `MATCH (person)<-[:HAS_CREATOR]-(message) WHERE message.id = latestMessageId`
//! with `person` and `latestMessageId` bound by earlier clauses expands every
//! message `person` ever created and only then keeps the one whose `id` is the
//! row's `latestMessageId`. The equality names exactly one message per row, so
//! the path is cheaper read from that message: pin it first, then check that it
//! reaches `person`.
//!
//! The rewrite lowers the pinned node `f` ahead of the rest of its pattern,
//! with the pinning conjunct directly after it, so the relational layer sees an
//! equality between the incoming rows and a scan of `f` and can join them on it
//! instead of expanding first. The rest of the pattern is then read outwards
//! from `f`, and the nodes the query already bound are checked by identity:
//!
//! ```text
//! MATCH (f)                              // f's labels and properties
//! WHERE f.key = e                        // the pinning conjunct
//! MATCH (f)-[..]->(..)-[..]->(head)      // the head read from f, bare f
//! MATCH (f)-[..]->(..)                   // the tail, bare f (omitted if empty)
//! WHERE <every other conjunct>           // in textual order, after all patterns
//! ```
//!
//! Exactness. A path pattern read right to left matches exactly the same paths
//! as the same pattern read left to right, so the pieces produce the same
//! matches with the same multiplicity. Every piece lowers with the clause's
//! shared edge list, so relationship uniqueness still spans the whole clause,
//! computed the same way as for any other pattern (only between edges whose
//! declared types can overlap). The pinning conjunct is the equality
//! `cypher_eq(f.key, e)`, which is total: it yields true, false or null and
//! never fails, and a null drops the row exactly as the original WHERE did.
//! A total filter on `f` and on variables bound before the clause commutes with
//! every expansion, so applying it earlier keeps exactly the rows the original
//! kept. Hoisting it can only change which rows a later conjunct that can fail
//! sees, and openCypher does not define the evaluation order of the conjuncts of
//! AND.
//!
//! Where it applies. Only to a MATCH (not OPTIONAL MATCH) whose WHERE has no
//! aggregate, and only to a path pattern that is not a named path, alternates
//! nodes and relationships, has at least one relationship, contains a node the
//! query already binds, and has only reorderable elements (see
//! [`reorderable`]). The pinned node is a named node after the first one,
//! mentioned nowhere else in the clause, pinned by the first top-level
//! conjunct `f.key = e` or `e = f.key` whose other side is a variable bound
//! before the clause or a property of a node or relationship bound before the
//! clause. Literals, parameters and arbitrary expressions are not accepted,
//! because arithmetic can fail and so is not safe to move.
//!
//! Cost. The rule is static and not "never worse": no index or statistics are
//! reachable while the binder runs. The pinned node is found by scanning `f`'s
//! key column and joining it to the rows on equality, which reads every `f` once
//! per query. Expanding from the bound node reads only that node's own
//! relationships, so for a small outer relation and a bound node of small degree
//! the original order is cheaper; for a bound node of large degree, such as a
//! prolific author, the rewrite is far cheaper. Neither side is chosen from
//! data. The join is also found only where the equality lowers to a native `=`,
//! which needs both sides to have the same non-floating, non-nested type; any
//! other pair keeps the null-aware equality function, which the relational layer
//! cannot use as a join key, and the scan is crossed with the rows before it
//! filters. Choosing the side from statistics, or probing the key per row, is
//! the physical layer's job.

use super::anchor::{derived, reorderable, reversed};
use super::{expr_contains_aggregate, strip_parens};
use crate::VarId;
use crate::binder::{Binder, BinderState, VarKind};
use graphforge_ast::{
    BinaryOp, BinaryOpKind as AstBinOp, Expr, MatchClause, NodePattern, PathElement, PathPattern,
    WhereClause,
};
use std::collections::HashMap;

/// A pattern to split and the conjunct that pins its node.
struct Split<'a> {
    /// Index in the clause of the pattern to split.
    pattern: usize,
    /// Element index of the pinned node `f`; always an even index after the
    /// first element.
    node: usize,
    /// Index in `conjuncts` of the pinning conjunct.
    pin: usize,
    /// The top-level conjuncts of the WHERE predicate in textual order.
    conjuncts: Vec<&'a Expr>,
}

impl Binder {
    /// Lower a non-OPTIONAL MATCH clause: every path pattern in order, then the
    /// WHERE predicate. Relationship isomorphism spans every pattern of one
    /// MATCH. A pattern whose node a correlated equality pins is planned from
    /// that node instead (see the module documentation, #1984).
    pub(super) fn lower_plain_match(&self, m: &MatchClause, s: &mut BinderState) {
        let mut match_edges = Vec::new();
        let split = m
            .where_clause
            .as_ref()
            .and_then(|w| find_pinned_node(m, w, s).map(|split| (w, split)));
        let Some((w, split)) = split else {
            for pat in &m.patterns {
                self.lower_match_path_pattern(pat, &mut match_edges, s);
            }
            if let Some(w) = &m.where_clause {
                self.lower_where(w, s);
            }
            return;
        };

        for (index, pat) in m.patterns.iter().enumerate() {
            if index == split.pattern {
                self.lower_pinned_pattern(pat, w, &split, &mut match_edges, s);
            } else {
                self.lower_match_path_pattern(pat, &mut match_edges, s);
            }
        }
        for (index, conjunct) in split.conjuncts.iter().enumerate() {
            if index != split.pin {
                self.lower_where_predicate(conjunct, w.span, s);
            }
        }
    }

    /// Lower `pat` from its pinned node: the node, the pinning conjunct, then
    /// the part before the node read backwards from it and the part after it
    /// read forwards from it.
    fn lower_pinned_pattern(
        &self,
        pat: &PathPattern,
        w: &WhereClause,
        split: &Split<'_>,
        match_edges: &mut Vec<(VarId, Option<graphforge_value::RelationTypeId>)>,
        s: &mut BinderState,
    ) {
        let PathElement::Node(node) = &pat.elements[split.node] else {
            // `find_pinned_node` only selects node positions.
            return;
        };
        self.lower_match_path_pattern(
            &derived(pat, vec![PathElement::Node(node.clone())]),
            match_edges,
            s,
        );
        self.lower_where_predicate(split.conjuncts[split.pin], w.span, s);

        let mut head = reversed(&pat.elements[..=split.node]);
        bare_first_node(&mut head);
        self.lower_match_path_pattern(&derived(pat, head), match_edges, s);
        if split.node + 1 < pat.elements.len() {
            let mut tail = pat.elements[split.node..].to_vec();
            bare_first_node(&mut tail);
            self.lower_match_path_pattern(&derived(pat, tail), match_edges, s);
        }

        // Each mention of a node records its first label, and the bare copies
        // above recorded none while the pieces ran in a different order than
        // the pattern is written. Record the mentions in textual order so the
        // entries are the ones the pattern as written leaves.
        for element in &pat.elements {
            let PathElement::Node(mention) = element else {
                continue;
            };
            if let Some(id) = mention.var.as_deref().and_then(|name| s.vars.get(name)) {
                s.node_vars.insert(*id, mention.labels.first().cloned());
            }
        }
    }
}

/// Find the pattern, the node and the conjunct to split on, or `None` to lower
/// the clause as written.
fn find_pinned_node<'a>(
    m: &'a MatchClause,
    w: &'a WhereClause,
    s: &BinderState,
) -> Option<Split<'a>> {
    if expr_contains_aggregate(&w.predicate) {
        return None;
    }
    let mentions = mention_counts(m);
    // Nodes that may be pinned, by name: named nodes after the first of a
    // pattern that is planned from a bound node, mentioned exactly once in the
    // clause and not bound before it.
    let mut candidates: HashMap<&str, (usize, usize)> = HashMap::new();
    for (pattern, pat) in m.patterns.iter().enumerate() {
        if !reads_from_a_bound_node(pat, s) {
            continue;
        }
        for (node, element) in pat.elements.iter().enumerate().skip(2).step_by(2) {
            let PathElement::Node(NodePattern {
                var: Some(name), ..
            }) = element
            else {
                continue;
            };
            if mentions.get(name.as_str()) == Some(&1)
                && !s.vars.contains_key(name)
                && !s.path_vars.contains_key(name)
            {
                candidates.insert(name, (pattern, node));
            }
        }
    }
    if candidates.is_empty() {
        return None;
    }

    let mut conjuncts = Vec::new();
    collect_conjuncts(&w.predicate, &mut conjuncts);
    for (pin, conjunct) in conjuncts.iter().enumerate() {
        let Expr::BinaryOp(BinaryOp {
            op: AstBinOp::Eq,
            left,
            right,
            ..
        }) = conjunct
        else {
            continue;
        };
        for (property, other) in [(left, right), (right, left)] {
            let Some(name) = property_owner(property) else {
                continue;
            };
            let Some(&(pattern, node)) = candidates.get(name) else {
                continue;
            };
            if is_value_bound_before(other, &mentions, s) {
                return Some(Split {
                    pattern,
                    node,
                    pin,
                    conjuncts,
                });
            }
        }
    }
    None
}

/// The top-level conjuncts of `expr` in the order the binder lowers them:
/// through parentheses and `AND`, left to right.
fn collect_conjuncts<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expr {
        Expr::Parenthesized { inner, .. } => collect_conjuncts(inner, out),
        Expr::BinaryOp(BinaryOp {
            op: AstBinOp::And,
            left,
            right,
            ..
        }) => {
            collect_conjuncts(left, out);
            collect_conjuncts(right, out);
        }
        other => out.push(other),
    }
}

/// The variable `v` of a property access `v.key`, ignoring parentheses.
fn property_owner(expr: &Expr) -> Option<&str> {
    let Expr::Property(access) = strip_parens(expr) else {
        return None;
    };
    match strip_parens(&access.object) {
        Expr::Var(var) => Some(var.name.as_str()),
        _ => None,
    }
}

/// Whether `expr` reads only a variable the query binds before the clause: the
/// variable itself when it is a plain value, or one property of a node or a
/// single relationship it binds. Those evaluations never fail. Anything else
/// (literals, parameters, arithmetic, function calls) is left in place.
fn is_value_bound_before(expr: &Expr, mentions: &HashMap<&str, usize>, s: &BinderState) -> bool {
    match strip_parens(expr) {
        Expr::Var(var) => bound_before(&var.name, mentions, s).is_some_and(|id| {
            !(matches!(
                s.var_kinds.get(&id),
                Some(VarKind::Node | VarKind::Relationship)
            ) || s.node_vars.contains_key(&id)
                || s.edge_rel_names.contains_key(&id)
                || s.node_lists.contains_key(&id))
        }),
        Expr::Property(access) => {
            let Expr::Var(var) = strip_parens(&access.object) else {
                return false;
            };
            bound_before(&var.name, mentions, s).is_some_and(|id| {
                s.node_vars.contains_key(&id)
                    || (s.edge_rel_names.contains_key(&id) && !s.scalar_list_edges.contains(&id))
            })
        }
        _ => false,
    }
}

/// The variable `name` denotes when it was bound before the clause and no
/// pattern of the clause mentions it.
fn bound_before(name: &str, mentions: &HashMap<&str, usize>, s: &BinderState) -> Option<VarId> {
    if mentions.contains_key(name) || s.path_vars.contains_key(name) {
        return None;
    }
    s.vars.get(name).copied()
}

/// How often each name is introduced by the clause's patterns: as a node, a
/// relationship or a path variable.
fn mention_counts(m: &MatchClause) -> HashMap<&str, usize> {
    let mut counts = HashMap::new();
    for pat in &m.patterns {
        let names = std::iter::once(pat.var.as_deref()).chain(pat.elements.iter().map(|element| {
            match element {
                PathElement::Node(node) => node.var.as_deref(),
                PathElement::Rel(rel) => rel.var.as_deref(),
            }
        }));
        for name in names.flatten() {
            *counts.entry(name).or_insert(0) += 1;
        }
    }
    counts
}

/// Whether the pattern may be split: not a named path, `node (rel node)+`, every
/// element reorderable, and at least one node the query already binds.
fn reads_from_a_bound_node(pat: &PathPattern, s: &BinderState) -> bool {
    pat.var.is_none()
        && pat.elements.len() >= 3
        && alternates_nodes_and_relationships(&pat.elements)
        && pat.elements.iter().all(reorderable)
        && pat.elements.iter().any(|element| match element {
            PathElement::Node(node) => node
                .var
                .as_deref()
                .and_then(|name| s.vars.get(name))
                .is_some_and(|id| s.node_vars.contains_key(id)),
            PathElement::Rel(_) => false,
        })
}

/// Whether the elements are `node (rel node)*`.
fn alternates_nodes_and_relationships(elements: &[PathElement]) -> bool {
    elements
        .iter()
        .enumerate()
        .all(|(index, element)| match element {
            PathElement::Node(_) => index % 2 == 0,
            PathElement::Rel(_) => index % 2 == 1,
        })
}

/// Drop the labels and properties of the first node, which the pinned node's own
/// pattern already enforces.
fn bare_first_node(elements: &mut [PathElement]) {
    if let Some(PathElement::Node(first)) = elements.first_mut() {
        first.labels.clear();
        first.properties = None;
    }
}

#[cfg(test)]
mod tests;
