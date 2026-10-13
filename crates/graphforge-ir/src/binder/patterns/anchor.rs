//! Anchoring an OPTIONAL MATCH path pattern on a node the query already binds.
//!
//! The binder emits a pattern's operations in textual order, so
//! `OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post)<-[:CONTAINER_OF]-(forum)`
//! begins with a scan of the fresh `friend` even when only `forum` is bound by
//! earlier clauses. That scan is crossed with every outer row. Starting the
//! pattern at the bound node instead makes each hop a join from rows the query
//! already has.
//!
//! A path pattern read right to left matches exactly the same paths as the same
//! pattern read left to right, so the rewrite changes where the lowering
//! starts and nothing else: the matched variables, the rows and their
//! multiplicity are the same, and relationship uniqueness still spans the whole
//! clause because every produced pattern lowers with the clause's shared edge
//! list.

use graphforge_ast::{Direction, Expr, NodePattern, PathElement, PathPattern, RelPattern};

/// Rewrite `pattern` so that it starts at its first node already bound by the
/// outer query, or return `None` to keep the pattern as written.
///
/// `is_bound_node` answers whether a variable name denotes a node bound before
/// the pattern. The result is one or two patterns to lower in order:
///
/// * a bound node in the last position yields the single reversed pattern;
/// * a bound node in the middle yields the reversed head, starting at a bare
///   copy of the anchor, followed by the forward tail starting at the anchor
///   with its own labels and properties.
///
/// The pattern is kept as written when it is a named path (its node and
/// segment order is observable), when it already starts at a bound node, when
/// no node is bound, when a relationship is variable-length and named (its
/// edge list would be reported in the opposite order), or when an inline
/// property value is not a literal or parameter (it could read a variable that
/// the new order binds later).
pub(super) fn anchor_on_bound_node(
    pattern: &PathPattern,
    is_bound_node: impl Fn(&str) -> bool,
) -> Option<Vec<PathPattern>> {
    if pattern.var.is_some() || pattern.elements.len() < 3 {
        return None;
    }
    let anchor = pattern.elements.iter().position(|element| match element {
        PathElement::Node(node) => node.var.as_deref().is_some_and(&is_bound_node),
        PathElement::Rel(_) => false,
    })?;
    if anchor == 0 || !pattern.elements.iter().all(reorderable) {
        return None;
    }

    let head = reversed(&pattern.elements[..=anchor]);
    if anchor + 1 == pattern.elements.len() {
        return Some(vec![derived(pattern, head)]);
    }
    let mut head = head;
    if let Some(PathElement::Node(first)) = head.first_mut() {
        first.labels.clear();
        first.properties = None;
    }
    let tail = pattern.elements[anchor..].to_vec();
    Some(vec![derived(pattern, head), derived(pattern, tail)])
}

pub(super) fn derived(pattern: &PathPattern, elements: Vec<PathElement>) -> PathPattern {
    PathPattern {
        var: None,
        elements,
        span: pattern.span,
    }
}

/// `elements` read from the last element to the first, with each relationship
/// pointing the other way.
pub(super) fn reversed(elements: &[PathElement]) -> Vec<PathElement> {
    elements
        .iter()
        .rev()
        .map(|element| match element {
            PathElement::Node(node) => PathElement::Node(node.clone()),
            PathElement::Rel(rel) => PathElement::Rel(RelPattern {
                direction: match rel.direction {
                    Direction::Out => Direction::In,
                    Direction::In => Direction::Out,
                    Direction::Undirected => Direction::Undirected,
                },
                ..rel.clone()
            }),
        })
        .collect()
}

pub(super) fn reorderable(element: &PathElement) -> bool {
    match element {
        PathElement::Node(NodePattern { properties, .. }) => {
            constant_properties(properties.as_ref())
        }
        PathElement::Rel(rel) => {
            let variable_length = rel.min_hops.is_some() || rel.max_hops.is_some();
            !(variable_length && rel.var.is_some()) && constant_properties(rel.properties.as_ref())
        }
    }
}

/// Whether an inline property map reads no variable. Only literals and
/// parameters qualify; anything else is left in textual order.
pub(super) fn constant_properties(properties: Option<&Expr>) -> bool {
    match properties {
        None => true,
        Some(Expr::Map(map)) => map
            .entries
            .values()
            .all(|value| matches!(value, Expr::Literal(_) | Expr::Param(_))),
        Some(_) => false,
    }
}
