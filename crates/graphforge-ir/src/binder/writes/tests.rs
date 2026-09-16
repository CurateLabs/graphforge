use super::super::BindErrorKind;
use super::super::tests::{expect_bind_error, make_binder};
use crate::Direction;
use crate::expr::IrExpr;
use crate::plan::{GraphOp, OntologyMode};

use graphforge_cypher::parse;

#[test]
fn create_single_node_populates_pattern() {
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    let ast = parse("CREATE (:Person {name: 'Alice', age: 30})").unwrap();
    let plan = binder.bind(&ast).expect("create bind should succeed");

    let create = plan
        .ops
        .iter()
        .find_map(|op| match op {
            GraphOp::Create { pattern } => Some(pattern),
            _ => None,
        })
        .expect("expected a Create op");
    assert_eq!(create.nodes.len(), 1);
    assert!(create.edges.is_empty());
    let node = &create.nodes[0];
    assert_eq!(node.labels.len(), 1, "Person label should resolve");
    let props = node.properties.expect("node should have a property map");
    // The property expr should be a MapLiteral in the arena.
    assert!(matches!(plan.exprs.get(props), IrExpr::MapLiteral(_)));
}

#[test]
fn create_edge_threads_src_and_dst_vars() {
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    let ast = parse("CREATE (a:Person)-[:KNOWS]->(b:Person)").unwrap();
    let plan = binder.bind(&ast).expect("create bind should succeed");

    let create = plan
        .ops
        .iter()
        .find_map(|op| match op {
            GraphOp::Create { pattern } => Some(pattern),
            _ => None,
        })
        .expect("expected a Create op");
    assert_eq!(create.nodes.len(), 2);
    assert_eq!(create.edges.len(), 1);
    let edge = &create.edges[0];
    // The edge's src/dst must match the two node vars, in order.
    assert_eq!(edge.src, create.nodes[0].var);
    assert_eq!(edge.dst, create.nodes[1].var);
    assert_eq!(edge.direction, Direction::Out);
    assert!(edge.rel_type.is_some());
}

#[test]
fn standalone_create_node_is_not_a_reference() {
    // No preceding clause → the CREATE introduces the var → mint, not ref.
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    let ast = parse("CREATE (a:Person)").unwrap();
    let plan = binder.bind(&ast).expect("bind");
    let create = plan
        .ops
        .iter()
        .find_map(|op| match op {
            GraphOp::Create { pattern } => Some(pattern),
            _ => None,
        })
        .expect("Create op");
    assert_eq!(create.nodes.len(), 1);
    assert!(
        !create.nodes[0].is_reference,
        "a CREATE-introduced var must be a mint, not a reference"
    );
}

#[test]
fn create_pattern_validation() {
    // A created relationship must have exactly one type, be fixed-length,
    // and be directed (#956).
    for q in [
        "CREATE ()-->()",         // no type
        "CREATE ()-[:FOO*2]->()", // variable-length
        "CREATE (a)-[:FOO]-(b)",  // undirected
    ] {
        expect_bind_error(q, BindErrorKind::InvalidArgument);
    }
}

#[test]
fn rebinding_a_bound_variable_in_create_is_rejected() {
    for q in [
        "MATCH (a) CREATE (a)",                          // bare re-create
        "MATCH (a) CREATE (a {name: 'x'})",              // re-declared with props
        "CREATE (n:Foo) CREATE (n:Bar)-[:OWNS]->(:Dog)", // re-declared with a label
        "MATCH ()-[r]->() CREATE ()-[r]->()",            // reused relationship var
    ] {
        expect_bind_error(q, BindErrorKind::VariableAlreadyBound);
    }
}

#[test]
fn create_reference_without_new_shape_is_accepted() {
    // Guardrail: a bound node referenced as an edge endpoint (no new labels
    // or properties) is a valid reference, not a rebind.
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    let ast = parse("MATCH (a) CREATE (a)-[:R]->(b)").expect("parse");
    binder.bind(&ast).expect("valid reference must bind");
}

#[test]
fn matched_var_in_create_is_a_reference_not_a_duplicate_mint() {
    // #703: `MATCH (a) CREATE (a)-[:KNOWS]->(b)` — `a` was bound by the
    // MATCH, so its CREATE node spec must be a REFERENCE (resolve the matched
    // node), and there must be exactly ONE spec for `a` (not a second mint).
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    let ast = parse("MATCH (a:Person) CREATE (a)-[:KNOWS]->(b:Person)").expect("parse");
    let plan = binder.bind(&ast).expect("bind");
    let create = plan
        .ops
        .iter()
        .find_map(|op| match op {
            GraphOp::Create { pattern } => Some(pattern),
            _ => None,
        })
        .expect("Create op");

    // The matched `a` is the edge src; the new `b` is the dst.
    let a_var = create.edges[0].src;
    let b_var = create.edges[0].dst;
    let a_specs: Vec<_> = create.nodes.iter().filter(|n| n.var == a_var).collect();
    assert_eq!(a_specs.len(), 1, "exactly one spec for the matched var `a`");
    assert!(a_specs[0].is_reference, "matched `a` must be a reference");
    let b_spec = create
        .nodes
        .iter()
        .find(|n| n.var == b_var)
        .expect("spec for new `b`");
    assert!(!b_spec.is_reference, "CREATE-introduced `b` must be a mint");
}

#[test]
fn delete_clause_lowers_to_delete_op() {
    // #740: DELETE now lowers to GraphOp::Delete (no longer rejected).
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    let ast = parse("MATCH (p:Person) DELETE p").unwrap();
    let plan = binder.bind(&ast).expect("DELETE binds");
    let delete = plan
        .ops
        .iter()
        .find_map(|op| match op {
            GraphOp::Delete { vars, detach, .. } => Some((vars.clone(), *detach)),
            _ => None,
        })
        .expect("a GraphOp::Delete op");
    assert_eq!(delete.0.len(), 1, "one target var");
    assert!(!delete.1, "plain DELETE is not DETACH");
}

#[test]
fn detach_delete_sets_detach_flag() {
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    let ast = parse("MATCH (p:Person) DETACH DELETE p").unwrap();
    let plan = binder.bind(&ast).expect("DETACH DELETE binds");
    assert!(
        plan.ops
            .iter()
            .any(|op| matches!(op, GraphOp::Delete { detach: true, .. })),
        "DETACH DELETE must set detach=true, got {:?}",
        plan.ops
    );
}

#[test]
fn delete_property_target_lowers_to_runtime_expression() {
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    let ast = parse("MATCH (p:Person) DELETE p.name").unwrap();
    let plan = binder
        .bind(&ast)
        .expect("property value binds for runtime typing");
    assert!(matches!(
        plan.ops.last(),
        Some(GraphOp::Delete { vars, exprs, .. }) if vars.is_empty() && exprs.len() == 1
    ));
}

#[test]
fn delete_scalar_expression_is_rejected() {
    expect_bind_error("MATCH () DELETE 1 + 1", BindErrorKind::InvalidDeleteTarget);
}

#[test]
fn delete_undeclared_variable_is_rejected() {
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    let ast = parse("MATCH (p:Person) DELETE q").unwrap();
    let errors = binder
        .bind(&ast)
        .expect_err("DELETE of an unbound var must be rejected");
    assert!(
        errors
            .iter()
            .any(|e| e.kind == BindErrorKind::UndeclaredVariable),
        "expected UndeclaredVariable, got {errors:?}"
    );
}

#[test]
fn set_property_clause_lowers_to_set_op() {
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    let ast = parse("MATCH (p:Person) SET p.age = 30").unwrap();
    let plan = binder.bind(&ast).expect("SET p.age = 30 must lower");
    let set = plan
        .ops
        .iter()
        .find_map(|op| match op {
            GraphOp::Set { items, .. } => Some(items),
            _ => None,
        })
        .expect("expected a GraphOp::Set");
    assert_eq!(set.len(), 1);
    assert_eq!(set[0].prop_name, "age");
}

#[test]
fn set_runtime_expr_value_lowers_to_non_literal() {
    // The value `p.age + 1` is a runtime expression, not a literal — it must
    // survive lowering as a compound `IrExpr`, not be collapsed to a literal.
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    let ast = parse("MATCH (p:Person) SET p.age = p.age + 1").unwrap();
    let plan = binder.bind(&ast).expect("SET p.age = p.age + 1 must lower");
    let items = plan
        .ops
        .iter()
        .find_map(|op| match op {
            GraphOp::Set { items, .. } => Some(items),
            _ => None,
        })
        .expect("expected a GraphOp::Set");
    let value = plan.exprs.get(items[0].value);
    assert!(
        matches!(value, IrExpr::BinaryOp { .. }),
        "expected a BinaryOp value expr, got {value:?}"
    );
}

#[test]
fn set_multiple_items_lower_to_one_op() {
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    let ast = parse("MATCH (p:Person) SET p.age = 30, p.name = 'Al'").unwrap();
    let plan = binder.bind(&ast).expect("multi-item SET must lower");
    let count = plan
        .ops
        .iter()
        .filter(|op| matches!(op, GraphOp::Set { .. }))
        .count();
    assert_eq!(count, 1, "expected exactly one GraphOp::Set");
}

#[test]
fn set_labels_lower_to_resolved_label_item() {
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    let ast = parse("MATCH (p:Person) SET p:Admin:Staff").unwrap();
    let plan = binder.bind(&ast).expect("SET labels must lower");
    let labels = plan.ops.iter().find_map(|op| match op {
        GraphOp::Set { label_items, .. } => Some(label_items),
        _ => None,
    });
    let labels = labels.expect("SET label items");
    assert_eq!(labels.len(), 1);
    assert_eq!(labels[0].labels.len(), 2);
}

#[test]
fn set_property_merge_lowers_to_map_item() {
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    let ast = parse("MATCH (p:Person) SET p += {age: 30}").unwrap();
    let plan = binder.bind(&ast).expect("SET += must lower");
    let map_items = plan
        .ops
        .iter()
        .find_map(|op| match op {
            GraphOp::Set { map_items, .. } => Some(map_items),
            _ => None,
        })
        .expect("expected SET op");
    assert_eq!(map_items.len(), 1);
    assert!(!map_items[0].replace);
}

#[test]
fn merge_lowers_real_pattern_specs() {
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    let ast = parse(
        "MERGE (p:Person {name:'Alice'}) \
             ON CREATE SET p.created = 1, p:New \
             ON MATCH SET p += {seen:true}",
    )
    .unwrap();
    let plan = binder.bind(&ast).expect("MERGE binds");
    let (pattern, on_create, on_match) = plan
        .ops
        .iter()
        .find_map(|op| match op {
            GraphOp::Merge {
                pattern,
                on_create,
                on_match,
            } => Some((pattern, on_create, on_match)),
            _ => None,
        })
        .expect("MERGE op");
    assert_eq!(pattern.nodes.len(), 1);
    assert!(pattern.nodes[0].properties.is_some());
    assert_eq!(on_create.len(), 2);
    assert_eq!(on_match.len(), 1);
}

#[test]
fn set_undeclared_variable_rejected() {
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    let ast = parse("MATCH (p:Person) SET q.age = 30").unwrap();
    let errors = binder.bind(&ast).expect_err("SET on unbound var must fail");
    assert!(
        errors
            .iter()
            .any(|e| e.kind == BindErrorKind::UndeclaredVariable),
        "expected UndeclaredVariable, got {errors:?}"
    );
}

#[test]
fn remove_property_clause_lowers_to_remove_op() {
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    let ast = parse("MATCH (p:Person) REMOVE p.age").unwrap();
    let plan = binder.bind(&ast).expect("REMOVE p.age must lower");
    let items = plan
        .ops
        .iter()
        .find_map(|op| match op {
            GraphOp::Remove { items, .. } => Some(items),
            _ => None,
        })
        .expect("expected a GraphOp::Remove");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].prop_name, "age");
}

#[test]
fn remove_labels_lower_to_resolved_label_item() {
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    let ast = parse("MATCH (p:Person) REMOVE p:Admin:Staff").unwrap();
    let plan = binder.bind(&ast).expect("REMOVE labels must lower");
    let labels = plan.ops.iter().find_map(|op| match op {
        GraphOp::Remove { label_items, .. } => Some(label_items),
        _ => None,
    });
    let labels = labels.expect("REMOVE label items");
    assert_eq!(labels.len(), 1);
    assert_eq!(labels[0].labels.len(), 2);
}
