//! Planning a MATCH or OPTIONAL MATCH path pattern from the nodes the query
//! already binds.
//!
//! The binder emits a pattern's operations in textual order, so
//! `OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post)<-[:CONTAINER_OF]-(forum)`
//! begins with a scan of the fresh `friend` even when only `forum` is bound by
//! earlier clauses. That scan is crossed with every outer row. Starting the
//! pattern at the bound node instead makes each hop a join from rows the query
//! already has.
//!
//! The same textual order makes a path that ends on a second bound node expand
//! blindly into it. In
//! `(friend)<-[:HAS_CREATOR]-(post)-[:HAS_TAG]->(tag)<-[:HAS_INTEREST]-(person)`
//! with `friend` and `person` bound, the last hop leaves every `tag` for every
//! interested person and then keeps the rows whose person is the bound one. The
//! rewrite plans the closing hop from the bound node instead: it reads the hop
//! backwards, `(person)-[:HAS_INTEREST]->(tag)`, which expands only the bound
//! person's own relationships and keeps the rows that reach the same `tag`. It
//! applies only when the forward half has at least two hops; see the cost note
//! on [`start_from_bound_nodes`].
//!
//! A path pattern read right to left matches exactly the same paths as the same
//! pattern read left to right, so the rewrite changes where the lowering
//! starts and nothing else: the matched variables, the rows and their
//! multiplicity are the same, and relationship uniqueness still spans the whole
//! clause because every produced pattern lowers with the clause's shared edge
//! list.

use crate::VarId;
use graphforge_ast::{Direction, Expr, NodePattern, PathElement, PathPattern, RelPattern};
use std::collections::HashMap;

/// Prefix of the variable names given to anonymous meeting nodes. The name
/// contains a NUL, which no query can spell without quoting it deliberately,
/// and [`forget_meeting_names`] removes every such name once the clause's
/// patterns are lowered.
const MEETING_PREFIX: &str = "\u{0}meeting";

/// The least element distance between two bound nodes for the later one to
/// close a path: node, hop, node, hop, node, hop, node. Closer pairs, such as
/// the node-hop-node-hop-node diamond, are left as written; see the cost note
/// on [`start_from_bound_nodes`].
const MIN_CLOSING_DISTANCE: usize = 6;

/// How much of a pattern the rewrite may reorder.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Rewrite {
    /// Split only where a later bound node closes a path (MATCH keeps the
    /// pattern's first node as the start of the lowering).
    ClosingHops,
    /// Also start the lowering at the first bound node when the pattern begins
    /// with unbound nodes (OPTIONAL MATCH).
    HeadAndClosingHops,
}

/// Rewrite `pattern` so that it is planned from the nodes already bound by the
/// outer query, or return `None` to keep the pattern as written.
///
/// `is_bound_node` answers whether a variable name denotes a node bound before
/// the pattern; `ordinal` numbers the pattern within its clause so that the
/// names given to anonymous meeting nodes are unique. The result is the
/// patterns to lower in order:
///
/// * Head. With [`Rewrite::HeadAndClosingHops`] and unbound nodes before the
///   first bound node, the head `(n0)..(bound)` is read backwards so that the
///   lowering starts at the bound node. The head begins with a bare copy of
///   the bound node when its pattern continues, and that continuation keeps
///   the node's own labels and properties.
/// * Closing hops. A bound node `q` that follows another bound node `p` with
///   at least two unbound nodes between them (three hops or more) is closed
///   from its own side. The meeting node is the node before the last hop into
///   `q`. The path up to and including the meeting node stays as written and
///   keeps the meeting node's labels and properties; the last hop is then read
///   backwards from `q`: `(q)<-[r]-(meeting)` becomes `(q)-[r]->(meeting)` on a
///   bare copy of the meeting node. Bound nodes that follow each other
///   directly, and bound nodes with a single unbound node between them, are
///   left as written. Whatever follows `q` continues from a bare copy of `q`.
///
/// The pattern is kept as written when it is a named path (its node and
/// segment order is observable), when no node is bound, when neither a head
/// nor a closing hop applies, when a relationship is variable-length and named
/// (its edge list would be reported in the opposite order), or when an inline
/// property value is not a literal or parameter (it could read a variable that
/// the new order binds later).
///
/// Cost. The rule is static: no degree statistics are reachable while the
/// binder runs, and the rewrite is not "never worse". Writing the closing hop
/// as `(m)->(q)` expands every meeting node `m` once and keeps the rows that
/// reach the bound `q`; reading it as `(q)->(m)` expands `q` once per row and
/// keeps the rows that reach `m`. The first costs the meeting node's degree,
/// the second the bound node's degree, and which is smaller depends on the
/// label populations and degree skew of the graph. Two measured shapes on the
/// SNB scale-factor-1 graph pull in opposite directions:
///
/// * `(friend)<-[:HAS_CREATOR]-(post)-[:HAS_TAG]->(tag)<-[:HAS_INTEREST]-(person)`
///   (three hops to `person`). The meeting node is a tag, a hub that many
///   persons follow, while a person has few interests. Expanding the person is
///   far cheaper, and the rewrite turned a query that did not finish into one
///   that did.
/// * `(person)<-[:HAS_CREATOR]-(message)<-[:LIKES]-(liker)` (two hops to
///   `liker`). The meeting node is a message, which a few persons like, while
///   the bound person has authored thousands of messages and a liker has liked
///   hundreds. Expanding the bound `liker` for every row, each time from its
///   hundreds of liked messages, is slower than expanding the few likers of
///   each message, so the rewrite regressed a 100-liker cut of this shape from
///   24 s to over 200 s. The better plan there is the opposite one,
///   `(liker)-[:LIKES]->(message)-[:HAS_CREATOR]->(person)`, because
///   `HAS_CREATOR` leaves each message for exactly one person.
///
/// A meeting node reached by following an edge is biased towards high degree,
/// the effect behind the friendship paradox, so a single-hop closing hop from a
/// meeting node that is adjacent to the previous bound node has no safe default
/// direction. The rewrite therefore applies only when the forward half has at
/// least two hops, which is where the first shape sits and the second does not.
/// This is a conservative restriction fitted to measured shapes, not a proof.
/// Choosing the side from relationship statistics, or probing the bound
/// destination of a single-hop expand physically so that the check costs one
/// lookup in either direction, belongs to the physical layer.
pub(super) fn start_from_bound_nodes(
    pattern: &PathPattern,
    ordinal: usize,
    rewrite: Rewrite,
    is_bound_node: impl Fn(&str) -> bool,
) -> Option<Vec<PathPattern>> {
    let elements = &pattern.elements;
    if pattern.var.is_some()
        || elements.len() < 3
        || !alternates_nodes_and_relationships(elements)
        || !elements.iter().all(reorderable)
    {
        return None;
    }
    let bound: Vec<usize> = elements
        .iter()
        .enumerate()
        .filter_map(|(index, element)| match element {
            PathElement::Node(node) if node.var.as_deref().is_some_and(&is_bound_node) => {
                Some(index)
            }
            _ => None,
        })
        .collect();
    let first = *bound.first()?;
    // Element `q` closes a path when at least two unbound nodes separate it
    // from the previous bound node, so the forward half keeps two hops or more
    // and the meeting node is not adjacent to the previous bound node.
    let closing: Vec<usize> = bound
        .windows(2)
        .filter(|pair| pair[1] - pair[0] >= MIN_CLOSING_DISTANCE)
        .map(|pair| pair[1])
        .collect();
    let head = rewrite == Rewrite::HeadAndClosingHops && first > 0;
    if !head && closing.is_empty() {
        return None;
    }

    let mut patterns = Vec::new();
    let mut start = 0;
    // Whether the node at `start` already had its labels and properties
    // enforced by an earlier piece.
    let mut start_enforced = false;
    if head {
        let mut backwards = reversed(&elements[..=first]);
        if first + 1 < elements.len() {
            bare_first_node(&mut backwards);
        }
        patterns.push(derived(pattern, backwards));
        start = first;
    }
    for (index, &closed) in closing.iter().enumerate() {
        let PathElement::Node(meeting) = &elements[closed - 2] else {
            return None;
        };
        let meeting_name = meeting
            .var
            .clone()
            .unwrap_or_else(|| format!("{MEETING_PREFIX}{ordinal}_{index}"));

        let mut forwards = elements[start..closed - 1].to_vec();
        if start_enforced {
            bare_first_node(&mut forwards);
        }
        if let Some(PathElement::Node(last)) = forwards.last_mut() {
            last.var = Some(meeting_name.clone());
        }
        patterns.push(derived(pattern, forwards));

        let mut closing_hop = reversed(&elements[closed - 2..=closed]);
        if let Some(PathElement::Node(last)) = closing_hop.last_mut() {
            last.var = Some(meeting_name);
            last.labels.clear();
            last.properties = None;
        }
        patterns.push(derived(pattern, closing_hop));
        start = closed;
        start_enforced = true;
    }
    if start + 1 < elements.len() {
        let mut rest = elements[start..].to_vec();
        if start_enforced {
            bare_first_node(&mut rest);
        }
        patterns.push(derived(pattern, rest));
    }
    Some(patterns)
}

/// Remove the names [`start_from_bound_nodes`] gave to anonymous meeting nodes
/// so that they are not visible to `RETURN *`, to later clauses or to nested
/// subqueries.
pub(super) fn forget_meeting_names(vars: &mut HashMap<String, VarId>) {
    vars.retain(|name, _| !name.starts_with(MEETING_PREFIX));
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

/// Drop the labels and properties of the first node, which an earlier piece
/// of the same pattern already enforces.
fn bare_first_node(elements: &mut [PathElement]) {
    if let Some(PathElement::Node(first)) = elements.first_mut() {
        first.labels.clear();
        first.properties = None;
    }
}

fn derived(pattern: &PathPattern, elements: Vec<PathElement>) -> PathPattern {
    PathPattern {
        var: None,
        elements,
        span: pattern.span,
    }
}

/// `elements` read from the last element to the first, with each relationship
/// pointing the other way.
fn reversed(elements: &[PathElement]) -> Vec<PathElement> {
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

fn reorderable(element: &PathElement) -> bool {
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
fn constant_properties(properties: Option<&Expr>) -> bool {
    match properties {
        None => true,
        Some(Expr::Map(map)) => map
            .entries
            .values()
            .all(|value| matches!(value, Expr::Literal(_) | Expr::Param(_))),
        Some(_) => false,
    }
}
