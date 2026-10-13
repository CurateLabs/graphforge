use super::super::super::tests::make_binder;
use crate::expr::{BinaryOpKind, IrExpr, IrLiteral};
use crate::plan::{GraphOp, GraphPlan, OntologyMode};
use crate::{Direction, ExprId, VarId};
use graphforge_cypher::parse;

/// The clause before the MATCH under test: it binds `p` (a node) and `latest`
/// (a plain value) and leaves the MATCH's own operations after the last WITH.
const PREFIX: &str = "MATCH (p:Person {id: 1})-[:KNOWS]-(l:Person) WITH p, l, l.id AS latest ";

fn bind(query: &str) -> Result<GraphPlan, Vec<crate::binder::BindError>> {
    let (binder, _) = make_binder(OntologyMode::Exploratory);
    binder.bind(&parse(query).unwrap())
}

/// The operations of `clause`, bound after `PREFIX`, together with the
/// variables the prefix carried.
struct Bound {
    plan: GraphPlan,
    /// Index in `plan.ops` of the first operation after the prefix's WITH.
    start: usize,
    /// `p`, `l` and `latest`.
    carried: Vec<VarId>,
}

fn bound(clause: &str) -> Bound {
    let plan = bind(&format!("{PREFIX}{clause} RETURN p")).expect("query binds");
    let with = plan
        .ops
        .iter()
        .position(|op| matches!(op, GraphOp::With { .. }))
        .expect("the prefix has a WITH");
    let GraphOp::With { items, .. } = &plan.ops[with] else {
        unreachable!();
    };
    let carried = items.iter().filter_map(|item| item.out_var).collect();
    Bound {
        plan,
        start: with + 1,
        carried,
    }
}

impl Bound {
    /// The operations of the clause under test, up to the final projection.
    fn ops(&self) -> &[GraphOp] {
        let end = self
            .plan
            .ops
            .iter()
            .rposition(|op| matches!(op, GraphOp::Project { .. }))
            .unwrap_or(self.plan.ops.len());
        &self.plan.ops[self.start..end]
    }

    fn shape(&self) -> Vec<&'static str> {
        shape(self.ops())
    }

    fn p(&self) -> VarId {
        self.carried[0]
    }

    fn latest(&self) -> VarId {
        self.carried[2]
    }

    fn expr(&self, id: ExprId) -> &IrExpr {
        self.plan.exprs.get(id)
    }

    /// Whether the filter is `var.<key> = other`, either way round.
    fn pins(&self, op: &GraphOp, var: VarId, other: VarId) -> bool {
        let GraphOp::Filter { predicate } = op else {
            return false;
        };
        let IrExpr::BinaryOp {
            op: BinaryOpKind::Eq,
            left,
            right,
        } = self.expr(*predicate)
        else {
            return false;
        };
        let (property, value) = if matches!(self.expr(*left), IrExpr::PropertyAccess { .. }) {
            (left, right)
        } else {
            (right, left)
        };
        let IrExpr::PropertyAccess { base, .. } = self.expr(*property) else {
            return false;
        };
        matches!(self.expr(*base), IrExpr::VarRef(v) if *v == var)
            && matches!(self.expr(*value), IrExpr::VarRef(v) if *v == other)
    }

    /// The comparison operator of a filter, when it is a binary comparison.
    fn comparison(&self, op: &GraphOp) -> Option<BinaryOpKind> {
        let GraphOp::Filter { predicate } = op else {
            return None;
        };
        match self.expr(*predicate) {
            IrExpr::BinaryOp { op, .. } => Some(*op),
            _ => None,
        }
    }
}

fn shape(ops: &[GraphOp]) -> Vec<&'static str> {
    ops.iter()
        .map(|op| match op {
            GraphOp::NodeScan { .. } => "scan",
            GraphOp::Filter { .. } => "filter",
            GraphOp::RelationshipUnique { .. } => "unique",
            GraphOp::Expand { dir, .. } => match dir {
                Direction::Out => "out",
                Direction::In => "in",
                Direction::Undirected => "both",
            },
            GraphOp::Optional { .. } => "optional",
            _ => "other",
        })
        .collect()
}

/// The scan variable at `index` in the clause's operations.
fn scan_var(ops: &[GraphOp], index: usize) -> VarId {
    match &ops[index] {
        GraphOp::NodeScan { var, .. } => *var,
        other => panic!("operation {index} is not a scan: {other:?}"),
    }
}

fn expand_ends(ops: &[GraphOp], index: usize) -> (VarId, VarId) {
    match &ops[index] {
        GraphOp::Expand { src, dst, .. } => (*src, *dst),
        other => panic!("operation {index} is not an expand: {other:?}"),
    }
}

/// The clause as written: scan the bound node, expand to the other end, scan it,
/// filter.
const AS_WRITTEN: [&str; 4] = ["scan", "in", "scan", "filter"];

// -----------------------------------------------------------------------
// The rewrite applies
// -----------------------------------------------------------------------

#[test]
fn a_correlated_equality_pins_the_node_and_the_path_is_read_from_it() {
    let b = bound("MATCH (p)<-[:HAS_CREATOR]-(m) WHERE m.id = latest");
    let ops = b.ops();
    // `(m)`, the pinning equality, then `(m)-[:HAS_CREATOR]->(p)` on a bare `m`.
    assert_eq!(b.shape(), ["scan", "filter", "scan", "out", "scan"]);
    let m = scan_var(ops, 0);
    assert!(
        b.pins(&ops[1], m, b.latest()),
        "the first filter pins the node: {ops:?}"
    );
    assert_eq!(scan_var(ops, 2), m);
    assert_eq!(expand_ends(ops, 3), (m, b.p()));
    assert_eq!(scan_var(ops, 4), b.p());
}

#[test]
fn operands_of_the_equality_may_be_either_way_round() {
    let b = bound("MATCH (p)<-[:HAS_CREATOR]-(m) WHERE latest = m.id");
    let ops = b.ops();
    assert_eq!(b.shape(), ["scan", "filter", "scan", "out", "scan"]);
    assert!(b.pins(&ops[1], scan_var(ops, 0), b.latest()), "{ops:?}");
}

#[test]
fn the_pinned_node_keeps_its_labels_only_on_its_first_scan() {
    let b = bound("MATCH (p)<-[:HAS_CREATOR]-(m:Post) WHERE m.id = latest");
    let ops = b.ops();
    assert_eq!(b.shape(), ["scan", "filter", "scan", "out", "scan"]);
    assert!(
        matches!(&ops[0], GraphOp::NodeScan { ty: Some(_), .. }),
        "the first scan keeps the label: {ops:?}"
    );
    assert!(
        matches!(&ops[2], GraphOp::NodeScan { ty: None, .. }),
        "the repeated scan is bare: {ops:?}"
    );
}

#[test]
fn the_part_after_the_pinned_node_is_read_forwards_from_it() {
    let b = bound("MATCH (p)<-[:HAS_CREATOR]-(m)-[:HAS_TAG]->(t) WHERE m.id = latest");
    let ops = b.ops();
    assert_eq!(
        b.shape(),
        [
            "scan", "filter", "scan", "out", "scan", "scan", "out", "scan"
        ]
    );
    let m = scan_var(ops, 0);
    assert_eq!(expand_ends(ops, 3), (m, b.p()));
    assert_eq!(scan_var(ops, 5), m);
    assert_eq!(expand_ends(ops, 6).0, m);
}

#[test]
fn the_tail_does_not_repeat_the_labels_of_the_pinned_node() {
    let b = bound("MATCH (p)<-[:HAS_CREATOR]-(m:Post)-[:HAS_TAG]->(t) WHERE m.id = latest");
    let ops = b.ops();
    assert_eq!(
        b.shape(),
        [
            "scan", "filter", "scan", "out", "scan", "scan", "out", "scan"
        ]
    );
    assert!(
        matches!(&ops[0], GraphOp::NodeScan { ty: Some(_), .. }),
        "{ops:?}"
    );
    assert!(
        matches!(&ops[2], GraphOp::NodeScan { ty: None, .. }),
        "{ops:?}"
    );
    assert!(
        matches!(&ops[5], GraphOp::NodeScan { ty: None, .. }),
        "{ops:?}"
    );
}

#[test]
fn the_other_conjuncts_follow_the_patterns_in_textual_order() {
    let b = bound(
        "MATCH (p)<-[:HAS_CREATOR]-(m) \
         WHERE m.len > 0 AND (m.id = latest) AND m.x <> 3",
    );
    let ops = b.ops();
    assert_eq!(
        b.shape(),
        ["scan", "filter", "scan", "out", "scan", "filter", "filter"]
    );
    assert!(b.pins(&ops[1], scan_var(ops, 0), b.latest()), "{ops:?}");
    assert_eq!(b.comparison(&ops[5]), Some(BinaryOpKind::Gt));
    assert_eq!(b.comparison(&ops[6]), Some(BinaryOpKind::Neq));
}

#[test]
fn only_the_pinned_pattern_is_split() {
    let b = bound("MATCH (p)-[:KNOWS]-(x), (p)<-[:HAS_CREATOR]-(m) WHERE m.id = latest");
    let ops = b.ops();
    // The first pattern is lowered as written.
    assert_eq!(
        b.shape(),
        [
            "scan", "both", "scan", "scan", "filter", "scan", "out", "scan"
        ]
    );
    assert!(b.pins(&ops[4], scan_var(ops, 3), b.latest()), "{ops:?}");
}

#[test]
fn a_property_of_a_node_bound_before_the_clause_pins_the_node() {
    let b = bound("MATCH (p)<-[:HAS_CREATOR]-(m) WHERE m.id = l.id");
    assert_eq!(b.shape(), ["scan", "filter", "scan", "out", "scan"]);
}

#[test]
fn an_unwound_value_pins_the_node() {
    // An UNWIND alias is a runtime value of unknown kind, like a WITH alias.
    let plan = bind(
        "MATCH (p:Person {id: 1}) UNWIND [10, 11] AS latest \
         MATCH (p)<-[:HAS_CREATOR]-(m) WHERE m.id = latest RETURN p",
    )
    .expect("query binds");
    let unwind = plan
        .ops
        .iter()
        .position(|op| matches!(op, GraphOp::Unwind { .. }))
        .expect("the query has an UNWIND");
    let end = plan
        .ops
        .iter()
        .rposition(|op| matches!(op, GraphOp::Project { .. }))
        .expect("the query returns");
    assert_eq!(
        shape(&plan.ops[unwind + 1..end]),
        ["scan", "filter", "scan", "out", "scan"]
    );
}

#[test]
fn relationship_uniqueness_spans_both_halves() {
    // The two `HAS_CREATOR` hops share a declared type, so the second is
    // checked against the first even though the rewrite lowers them as separate
    // pieces.
    let b = bound("MATCH (p)<-[:HAS_CREATOR]-(m)-[:HAS_CREATOR]->(q) WHERE m.id = latest");
    let ops = b.ops();
    assert_eq!(
        b.shape(),
        [
            "scan", "filter", "scan", "out", "scan", "scan", "out", "unique", "scan"
        ]
    );
    let GraphOp::Expand { edge: first, .. } = &ops[3] else {
        panic!("{ops:?}");
    };
    let GraphOp::Expand { edge: second, .. } = &ops[6] else {
        panic!("{ops:?}");
    };
    assert_eq!(
        ops[7],
        GraphOp::RelationshipUnique {
            edge: *second,
            prior_edges: vec![*first],
        }
    );
}

#[test]
fn relationships_of_different_types_are_not_checked_for_uniqueness() {
    let b = bound("MATCH (p)<-[:HAS_CREATOR]-(m)-[:HAS_TAG]->(t) WHERE m.id = latest");
    assert!(!b.shape().contains(&"unique"), "{:?}", b.shape());
}

#[test]
fn the_node_keeps_its_label_for_later_clauses() {
    // `RETURN m` materialises the node with the label the pattern gave it. The
    // rewrite lowers bare copies of `m` last, which must not erase it.
    let plan = bind(&format!(
        "{PREFIX}MATCH (p)<-[:HAS_CREATOR]-(m:Post) WHERE m.id = latest RETURN m"
    ))
    .expect("query binds");
    let labelled = (0..plan.exprs.len()).any(|index| {
        let IrExpr::FunctionCall { name, args } = plan
            .exprs
            .get(ExprId(u32::try_from(index).expect("small plan")))
        else {
            return false;
        };
        name == super::super::NODE_STRUCT_CALL
            && args.len() == 2
            && matches!(
                plan.exprs.get(args[1]),
                IrExpr::Literal(IrLiteral::Str(label)) if label == "Post"
            )
    });
    assert!(labelled, "the node struct carries the Post label");
}

// -----------------------------------------------------------------------
// The clause is kept as written
// -----------------------------------------------------------------------

fn assert_as_written(clause: &str) {
    let b = bound(clause);
    assert_eq!(b.shape(), AS_WRITTEN, "{clause}: {:?}", b.ops());
    assert_eq!(scan_var(b.ops(), 0), b.p(), "{clause}");
}

#[test]
fn an_equality_with_a_literal_is_left_in_place() {
    assert_as_written("MATCH (p)<-[:HAS_CREATOR]-(m) WHERE m.id = 5");
}

#[test]
fn an_equality_with_a_parameter_is_left_in_place() {
    assert_as_written("MATCH (p)<-[:HAS_CREATOR]-(m) WHERE m.id = $x");
}

#[test]
fn an_equality_with_an_expression_is_left_in_place() {
    // Arithmetic can fail, so it is not moved ahead of the expansion.
    assert_as_written("MATCH (p)<-[:HAS_CREATOR]-(m) WHERE m.id = latest + 1");
}

#[test]
fn an_equality_with_a_node_is_left_in_place() {
    // `l` is a node carried through the prefix's WITH, not a plain value.
    assert_as_written("MATCH (p)<-[:HAS_CREATOR]-(m) WHERE m.id = l");
}

#[test]
fn an_equality_with_an_aliased_node_is_left_in_place() {
    let plan = bind(
        "MATCH (p:Person {id: 1})-[:KNOWS]-(l:Person) WITH p, l AS q \
         MATCH (p)<-[:HAS_CREATOR]-(m) WHERE m.id = q RETURN p",
    )
    .expect("query binds");
    let with = plan
        .ops
        .iter()
        .position(|op| matches!(op, GraphOp::With { .. }))
        .expect("the prefix has a WITH");
    let end = plan
        .ops
        .iter()
        .rposition(|op| matches!(op, GraphOp::Project { .. }))
        .unwrap();
    assert_eq!(
        shape(&plan.ops[with + 1..end]),
        AS_WRITTEN,
        "{:?}",
        plan.ops
    );
}

/// The shape of the operations `clause` binds to after `prefix`, which is a
/// sequence of complete clauses.
fn shape_after(prefix: &str, clause: &str) -> (Vec<&'static str>, GraphPlan) {
    // The prefix alone ends in the one Project that `RETURN p` adds.
    let start = bind(&format!("{prefix}RETURN p"))
        .expect("prefix binds")
        .ops
        .len()
        - 1;
    let plan = bind(&format!("{prefix}{clause} RETURN p")).expect("query binds");
    let end = plan
        .ops
        .iter()
        .rposition(|op| matches!(op, GraphOp::Project { .. }))
        .expect("the query returns");
    (shape(&plan.ops[start..end]), plan)
}

fn assert_as_written_after(prefix: &str, clause: &str) {
    let (written, plan) = shape_after(prefix, clause);
    assert_eq!(written, AS_WRITTEN, "{prefix}{clause}: {:?}", plan.ops);
}

fn assert_pinned_after(prefix: &str, clause: &str) {
    let (written, plan) = shape_after(prefix, clause);
    assert_eq!(
        written,
        ["scan", "filter", "scan", "out", "scan"],
        "{prefix}{clause}: {:?}",
        plan.ops
    );
}

const CREATOR: &str = "MATCH (p)<-[:HAS_CREATOR]-(m) WHERE ";

/// A relationship carried through the WITH under another name.
const RELATIONSHIP: &str = "MATCH (p:Person {id: 1})-[r:KNOWS]-(l:Person) WITH p, r AS q ";

/// A `*1..1` relationship variable, bound in the preceding clause.
const RELATIONSHIP_LIST: &str = "MATCH (p:Person {id: 1})-[r:KNOWS*1..1]-(l:Person) ";

/// A list of nodes collected by the prefix.
const NODE_LIST: &str = "MATCH (p:Person {id: 1})-[:KNOWS]-(l:Person) WITH p, collect(l) AS ls ";

#[test]
fn an_equality_with_a_relationship_is_left_in_place() {
    assert_as_written_after(RELATIONSHIP, &format!("{CREATOR}m.id = q"));
}

#[test]
fn an_equality_with_a_list_of_relationships_is_left_in_place() {
    assert_as_written_after(RELATIONSHIP_LIST, &format!("{CREATOR}m.id = r"));
}

#[test]
fn an_equality_with_a_list_of_nodes_is_left_in_place() {
    assert_as_written_after(NODE_LIST, &format!("{CREATOR}m.id = ls"));
}

#[test]
fn a_property_of_a_list_of_nodes_is_left_in_place() {
    // A list has no properties; reading one fails, so it is not moved ahead of
    // the expansion.
    assert_as_written_after(NODE_LIST, &format!("{CREATOR}m.id = ls.id"));
}

#[test]
fn a_property_of_a_list_of_relationships_is_left_in_place() {
    assert_as_written_after(RELATIONSHIP_LIST, &format!("{CREATOR}m.id = r.id"));
}

#[test]
fn a_property_of_a_plain_value_is_left_in_place() {
    // `latest` is an integer carried by the prefix, not a node or relationship.
    assert_as_written("MATCH (p)<-[:HAS_CREATOR]-(m) WHERE m.id = latest.x");
}

#[test]
fn a_property_of_a_relationship_bound_before_the_clause_pins_the_node() {
    assert_pinned_after(RELATIONSHIP, &format!("{CREATOR}m.id = q.creationDate"));
}

#[test]
fn an_equality_under_or_is_left_in_place() {
    assert_as_written("MATCH (p)<-[:HAS_CREATOR]-(m) WHERE m.id = latest OR m.x = 1");
}

#[test]
fn an_equality_on_a_property_of_the_bound_node_is_left_in_place() {
    // `p` is mentioned by the pattern, so the equality is not a constraint that
    // the rows already satisfy before the pattern runs.
    assert_as_written("MATCH (p)<-[:HAS_CREATOR]-(m) WHERE m.id = p.id");
}

#[test]
fn an_optional_match_is_left_in_place() {
    let b = bound("OPTIONAL MATCH (p)<-[:HAS_CREATOR]-(m) WHERE m.id = latest");
    let ops = b.ops();
    let [GraphOp::Optional { child }] = ops else {
        panic!("{ops:?}");
    };
    assert_eq!(shape(&child.ops), AS_WRITTEN, "{:?}", child.ops);
}

#[test]
fn a_named_path_is_left_in_place() {
    let b = bound("MATCH x = (p)<-[:HAS_CREATOR]-(m) WHERE m.id = latest");
    assert_eq!(b.shape(), AS_WRITTEN, "{:?}", b.ops());
}

#[test]
fn the_first_node_of_a_pattern_is_not_pinned() {
    let b = bound("MATCH (m)-[:HAS_CREATOR]->(p) WHERE m.id = latest");
    assert_eq!(
        b.shape(),
        ["scan", "out", "scan", "filter"],
        "{:?}",
        b.ops()
    );
    assert_eq!(scan_var(b.ops(), 0), expand_ends(b.ops(), 1).0);
}

#[test]
fn a_pattern_without_a_bound_node_is_left_in_place() {
    let b = bound("MATCH (a)-[:HAS_CREATOR]->(m) WHERE m.id = latest");
    assert_eq!(
        b.shape(),
        ["scan", "out", "scan", "filter"],
        "{:?}",
        b.ops()
    );
}

#[test]
fn a_node_bound_before_the_clause_is_not_pinned() {
    // `l` is carried from the prefix; its rows already hold the node.
    let b = bound("MATCH (p)<-[:KNOWS]-(l) WHERE l.id = latest");
    assert_eq!(b.shape(), AS_WRITTEN, "{:?}", b.ops());
}

#[test]
fn a_node_mentioned_twice_is_left_in_place() {
    let b =
        bound("MATCH (p)<-[:HAS_CREATOR]-(m)-[:HAS_TAG]->(t)<-[:HAS_TAG]-(m) WHERE m.id = latest");
    assert_eq!(scan_var(b.ops(), 0), b.p(), "{:?}", b.ops());
}

#[test]
fn a_value_bound_by_the_same_clause_is_left_in_place() {
    let b = bound("MATCH (p)<-[:HAS_CREATOR]-(m)-[:HAS_TAG]->(t) WHERE m.id = t.id");
    assert_eq!(scan_var(b.ops(), 0), b.p(), "{:?}", b.ops());
}

#[test]
fn an_inline_property_that_reads_a_variable_is_left_in_place() {
    let b = bound("MATCH (p)<-[:HAS_CREATOR]-(m)-[:HAS_TAG]->(t {id: latest}) WHERE m.id = latest");
    assert_eq!(scan_var(b.ops(), 0), b.p(), "{:?}", b.ops());
}

#[test]
fn an_aggregate_in_the_predicate_is_still_an_error() {
    let errors = bind(&format!(
        "{PREFIX}MATCH (p)<-[:HAS_CREATOR]-(m) WHERE m.id = latest AND count(*) > 0 RETURN p"
    ))
    .expect_err("an aggregate may not be used in WHERE");
    assert!(
        errors.iter().any(|error| error
            .message
            .contains("aggregate function may not be used in WHERE")),
        "{errors:?}"
    );
}
