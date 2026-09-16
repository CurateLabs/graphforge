use super::super::tests::{empty_state, expect_bind_error, make_binder};
use super::super::{BindError, BindErrorKind, Binder, BinderState, VarKind};
use crate::catalog::RuntimeCatalog;
use crate::expr::{BinaryOpKind, IrExpr, IrLiteral};
use crate::plan::{GraphOp, GraphPlan, OntologyMode, PATTERN_COMPREHENSION_VALUE_ALIAS};
use crate::{ExprId, VarId};
use graphforge_ast::{AstClause, BinaryOpKind as AstBinOp, Expr, Literal};
use graphforge_core::TypeId;
use graphforge_cypher::parse;
use graphforge_ontology::{OntologyCompiler, OntologyHandle, OntologyLoader};
use graphforge_value::{EntityTypeId, RuntimeEntityId};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

#[test]
fn advisory_runtime_entity_label_is_tagged_away_from_ontology_zero() {
    let doc = OntologyLoader::load_yaml(std::io::Cursor::new(
        b"\
ontology_id: people
version: \"v1\"
entity_types:
  - name: Person
    abstract: false
relation_types: []
properties: []
constraints: []
migrations: []
",
    ))
    .unwrap();
    let ontology = OntologyHandle::new(OntologyCompiler::compile(&doc).unwrap());
    assert_eq!(ontology.entity_type_id("Person"), Some(TypeId(0)));

    let catalog = Arc::new(Mutex::new(RuntimeCatalog::new()));
    let binder = Binder::new(
        Some(ontology.clone()),
        Arc::clone(&catalog),
        OntologyMode::Advisory,
    );
    let plan = binder
        .bind(&parse("MATCH (n:Ghost) RETURN n").unwrap())
        .expect("advisory Ghost bind");
    let ghost_ty = plan
        .ops
        .iter()
        .find_map(|op| match op {
            GraphOp::NodeScan { ty: Some(ty), .. } => Some(*ty),
            _ => None,
        })
        .expect("NodeScan");
    assert_eq!(
        ghost_ty,
        EntityTypeId::runtime(RuntimeEntityId::new(0).unwrap())
    );
    assert_ne!(ghost_ty, EntityTypeId::ontology(TypeId(0)).unwrap());
    assert_eq!(
        catalog.lock().unwrap().intern_label("Ghost").unwrap().get(),
        0
    );
}

#[test]
fn exploratory_unknown_label_succeeds() {
    let (binder, catalog) = make_binder(OntologyMode::Exploratory);
    let ast = parse("MATCH (a:UnknownLabel)-[:UNKNOWN_REL]->(b) RETURN a").unwrap();
    let plan = binder.bind(&ast).expect("exploratory bind should succeed");

    assert!(
        plan.ops
            .iter()
            .any(|op| matches!(op, GraphOp::NodeScan { .. }))
    );
    let cat = catalog.lock().unwrap();
    assert!(cat.contains_entity_type("UnknownLabel"));
    assert!(cat.relation_types().contains(&"UNKNOWN_REL"));
}

#[test]
fn fixed_pattern_predicate_lowers_to_exists() {
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    let ast = parse("MATCH (n) WHERE (n)-[:REL]->() RETURN n").unwrap();
    let plan = binder.bind(&ast).expect("pattern predicate binds");

    let exists = plan
        .ops
        .iter()
        .find_map(|op| match op {
            GraphOp::Exists { child, negated } => Some((child, negated)),
            _ => None,
        })
        .expect("pattern predicate should lower to Exists");
    assert!(!*exists.1);
    assert!(
        exists
            .0
            .ops
            .iter()
            .any(|op| matches!(op, GraphOp::Expand { .. })),
        "child plan must match the relationship pattern"
    );
}

#[test]
fn relationship_uniqueness_is_scoped_to_each_path_pattern() {
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    let ast = parse("MATCH (a)-[r1]->(b)-[r2]->(c), (x)-[r3]->(y) RETURN a").unwrap();
    let plan = binder.bind(&ast).expect("pattern binds");

    let constraints: Vec<_> = plan
        .ops
        .iter()
        .filter_map(|op| match op {
            GraphOp::RelationshipUnique { edge, prior_edges } => Some((*edge, prior_edges.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(constraints.len(), 1);
    assert_eq!(constraints[0].1.len(), 1);
}

#[test]
fn simple_existential_subquery_allows_child_local_variables() {
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    let ast =
        parse("MATCH (n) WHERE exists { (n)-[r]->(m) WHERE type(r) = 'REL' } RETURN n").unwrap();
    let plan = binder.bind(&ast).expect("existential subquery binds");

    let child = plan
        .ops
        .iter()
        .find_map(|op| match op {
            GraphOp::Exists { child, negated } => {
                assert!(!negated);
                Some(child)
            }
            _ => None,
        })
        .expect("expected Exists op");
    assert!(
        child
            .ops
            .iter()
            .any(|op| matches!(op, GraphOp::Expand { .. }))
    );
    assert!(
        child
            .ops
            .iter()
            .any(|op| matches!(op, GraphOp::Filter { .. }))
    );
}

#[test]
fn full_existential_correlation_is_scope_aware() {
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    let correlated =
        parse("MATCH (n) WHERE exists { MATCH (m) WHERE m.prop = n.prop RETURN true } RETURN n")
            .unwrap();
    binder
        .bind(&correlated)
        .expect("an outer variable used only in a child expression must bind");

    let shadowed = parse("MATCH (n) WHERE exists { WITH 1 AS n RETURN n } RETURN n").unwrap();
    let errors = binder
        .bind(&shadowed)
        .expect_err("a child-local alias must not count as outer correlation");
    assert!(
        errors.iter().any(|error| {
            error.kind == BindErrorKind::UndeclaredVariable
                && error
                    .message
                    .contains("must reference at least one outer variable")
        }),
        "expected uncorrelated-subquery error, got {errors:?}"
    );
}

#[test]
fn negated_pattern_predicate_lowers_to_anti_exists() {
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    let ast = parse("MATCH (n) WHERE NOT (n)-[:REL]-() RETURN n").unwrap();
    let plan = binder.bind(&ast).expect("negated pattern predicate binds");

    assert!(
        plan.ops
            .iter()
            .any(|op| { matches!(op, GraphOp::Exists { negated: true, .. }) })
    );
}

#[test]
fn multi_type_pattern_predicate_lowers_to_union_exists() {
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    let ast = parse("MATCH (n), (m) WHERE (n)-[:REL1|REL2]-(m) RETURN n").unwrap();
    let plan = binder.bind(&ast).expect("multi-type predicate binds");

    let inputs = plan
        .ops
        .iter()
        .find_map(|op| match op {
            GraphOp::Exists { child, .. } => match child.ops.as_slice() {
                [GraphOp::Union { inputs, .. }] => Some(inputs),
                _ => None,
            },
            _ => None,
        })
        .expect("multi-type predicate should lower to union-backed Exists");
    assert_eq!(inputs.len(), 2);
}

#[test]
fn or_pattern_predicate_lowers_to_union_exists() {
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    let ast = parse("MATCH (n) WHERE (n)-[:REL1]-() OR (n)-[:REL2]-() RETURN n").unwrap();
    let plan = binder.bind(&ast).expect("OR predicate binds");

    let inputs = plan
        .ops
        .iter()
        .find_map(|op| match op {
            GraphOp::Exists { child, .. } => match child.ops.as_slice() {
                [GraphOp::Union { inputs, .. }] => Some(inputs),
                _ => None,
            },
            _ => None,
        })
        .expect("OR predicate should lower to union-backed Exists");
    assert_eq!(inputs.len(), 2);
}

#[test]
fn pattern_predicate_rejects_new_named_variables() {
    expect_bind_error(
        "MATCH (n) WHERE (n)-[r]->() RETURN n",
        BindErrorKind::UndeclaredVariable,
    );
    expect_bind_error(
        "MATCH (n) WHERE (n)-->(m) RETURN n",
        BindErrorKind::UndeclaredVariable,
    );
}

#[test]
fn pattern_predicate_rejects_named_path_binding() {
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    let mut ast = parse("MATCH (n) WHERE (n)-[:REL]->() RETURN n").unwrap();
    let AstClause::Match(m) = &mut ast.clauses[0] else {
        panic!("expected MATCH clause");
    };
    let where_clause = m.where_clause.as_mut().expect("expected WHERE");
    let Expr::PatternPredicate(pp) = &mut where_clause.predicate else {
        panic!("expected pattern predicate");
    };
    pp.pattern.var = Some("p".into());

    let errs = binder
        .bind(&ast)
        .expect_err("predicate-local path binding should fail");
    assert!(
        errs.iter()
            .any(|err| err.kind == BindErrorKind::UndeclaredVariable),
        "expected undeclared path binding error, got {errs:?}"
    );
}

#[test]
fn var_length_pattern_predicate_rejects_relationship_properties() {
    expect_bind_error(
        "MATCH (n) WHERE (n)-[:REL* {k: 1}]->() RETURN n",
        BindErrorKind::InvalidArgument,
    );
}

#[test]
fn pattern_predicate_rejects_uncorrelated_patterns() {
    expect_bind_error(
        "MATCH (n) WHERE ()-[:REL]->() RETURN n",
        BindErrorKind::UndeclaredVariable,
    );
}

#[test]
fn named_pattern_comprehension_binds_correlated_child_projection() {
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    let ast = parse("MATCH (n) RETURN [p = (n)-[:REL]->() | p] AS paths").unwrap();
    let plan = binder.bind(&ast).expect("pattern comprehension binds");

    let (child, output) = plan
        .ops
        .iter()
        .find_map(|op| match op {
            GraphOp::PatternComprehension { child, output } => Some((child, output)),
            _ => None,
        })
        .expect("expected a PatternComprehension op");
    assert!(
        child
            .ops
            .iter()
            .any(|op| matches!(op, GraphOp::Expand { .. }))
    );
    let projection = child
        .ops
        .last()
        .and_then(|op| match op {
            GraphOp::Project { items, .. } => items.first(),
            _ => None,
        })
        .expect("child must end with one value projection");
    assert_eq!(
        projection.alias.as_deref(),
        Some(PATTERN_COMPREHENSION_VALUE_ALIAS)
    );
    assert!(matches!(
        child.exprs.get(projection.expr),
        IrExpr::FunctionCall { name, .. } if name == "_path_struct"
    ));
    let outer_projection = plan
        .ops
        .iter()
        .rev()
        .find_map(|op| match op {
            GraphOp::Project { items, .. } => items.first(),
            _ => None,
        })
        .expect("outer RETURN must project the collected result");
    assert!(matches!(
        plan.exprs.get(outer_projection.expr),
        IrExpr::VarRef(var) if var == output
    ));
}

#[test]
fn pattern_comprehension_binds_local_node_and_relationship_properties() {
    for query in [
        "MATCH (n) RETURN [(n)-[:REL]->(b) | b.name] AS names",
        "MATCH (n) RETURN [(n)-[r:REL]->() | r.name] AS names",
    ] {
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        let ast = parse(query).unwrap();
        let plan = binder.bind(&ast).expect("local projection binds");
        let child = plan
            .ops
            .iter()
            .find_map(|op| match op {
                GraphOp::PatternComprehension { child, .. } => Some(child),
                _ => None,
            })
            .expect("expected a PatternComprehension op");
        let projection = child
            .ops
            .last()
            .and_then(|op| match op {
                GraphOp::Project { items, .. } => items.first(),
                _ => None,
            })
            .expect("child must end with a projection");
        assert!(matches!(
            child.exprs.get(projection.expr),
            IrExpr::PropertyAccess { .. }
        ));
    }
}

#[test]
fn pattern_comprehension_binds_filter_and_variable_length_match() {
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    let ast =
        parse("MATCH (n) RETURN [(n)-[r:REL*]->(b) WHERE b.ok = true | b] AS matches").unwrap();
    let plan = binder
        .bind(&ast)
        .expect("filtered var-length comprehension binds");
    let child = plan
        .ops
        .iter()
        .find_map(|op| match op {
            GraphOp::PatternComprehension { child, .. } => Some(child),
            _ => None,
        })
        .expect("expected a PatternComprehension op");

    assert!(child.ops.iter().any(|op| {
        matches!(
            op,
            GraphOp::Expand {
                min_hops: 1,
                max_hops: None,
                ..
            }
        )
    }));
    assert!(
        child
            .ops
            .iter()
            .any(|op| matches!(op, GraphOp::Filter { .. }))
    );
    assert!(matches!(child.ops.last(), Some(GraphOp::Project { .. })));
}

#[test]
fn pattern_comprehension_local_variables_do_not_leak() {
    expect_bind_error(
        "MATCH (n) RETURN [(n)-->(b) | b] AS matches, b",
        BindErrorKind::UndeclaredVariable,
    );
}

#[test]
fn pattern_comprehension_in_list_element_scope_lifts_to_graph_op() {
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    let ast = parse("MATCH (n) RETURN [x IN [n] | [(x)-->() | x]] AS nested").unwrap();
    let plan = binder
        .bind(&ast)
        .expect("bind nested pattern comprehension");
    let lifted = plan.ops.iter().find_map(|op| match op {
        GraphOp::ListElementPatternComprehension { child, .. } => Some(child),
        _ => None,
    });
    let child = lifted.expect("list-element graph operation");
    assert!(matches!(child.ops.last(), Some(GraphOp::Project { .. })));
}

#[test]
fn bare_graph_value_where_predicate_is_rejected() {
    expect_bind_error(
        "MATCH (n) WHERE (n) RETURN n",
        BindErrorKind::InvalidArgument,
    );
}

#[test]
fn bare_path_value_where_predicate_is_rejected() {
    expect_bind_error(
        "MATCH p = (n)-[:REL]->() WHERE p RETURN p",
        BindErrorKind::InvalidArgument,
    );
}

#[test]
fn with_where_pattern_predicate_lowers_after_with() {
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    let ast = parse("MATCH (n) WITH n WHERE (n)-[:REL]->() RETURN n").unwrap();
    let plan = binder.bind(&ast).expect("WITH pattern predicate binds");

    let with_idx = plan
        .ops
        .iter()
        .position(|op| matches!(op, GraphOp::With { .. }))
        .expect("WITH should lower to a With op");
    let exists_idx = plan
        .ops
        .iter()
        .position(|op| matches!(op, GraphOp::Exists { .. }))
        .expect("WITH WHERE pattern predicate should lower to Exists");
    assert!(
        with_idx < exists_idx,
        "WITH projection must run before its pattern predicate"
    );
}

// -----------------------------------------------------------------------
// Variable-kind conflict validation (#956, VariableTypeConflict)
// -----------------------------------------------------------------------

/// Bind and assert the query is REJECTED with a `VariableKindConflict`.
fn expect_kind_conflict(query: &str) {
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    let ast = parse(query).expect("parse");
    let errs = binder
        .bind(&ast)
        .expect_err(&format!("expected a kind conflict for: {query}"));
    assert!(
        errs.iter()
            .any(|e| e.kind == BindErrorKind::VariableKindConflict),
        "expected VariableKindConflict for {query}, got {errs:?}"
    );
}

#[test]
fn relationship_var_reused_as_node_conflicts() {
    // A relationship variable used as a node pattern is a VariableTypeConflict.
    for q in [
        "MATCH ()-[r]-() MATCH (r) RETURN r",
        "MATCH ()-[r]->() MATCH (r) RETURN r",
        "MATCH (), ()-[r]-() MATCH (r) RETURN r",
    ] {
        expect_kind_conflict(q);
    }
}

#[test]
fn path_var_reused_as_node_conflicts() {
    // A (single-segment) path variable reused as a node pattern conflicts.
    expect_kind_conflict("MATCH r = ()-[]-() MATCH (r) RETURN r");
}

#[test]
fn scalar_aliases_reused_as_pattern_entities_conflict() {
    expect_kind_conflict("WITH 42 AS n MATCH (n) RETURN n");
    expect_kind_conflict("WITH true AS r MATCH ()-[r]->() RETURN r");
    expect_kind_conflict("MATCH (n) WITH collect(n) AS users MATCH (users)-[:R]->() RETURN users");
}

#[test]
fn runtime_polymorphic_pattern_values_remain_bindable() {
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    for query in [
        "WITH null AS a OPTIONAL MATCH p = (a)-[r]->() RETURN relationships(p)",
        "MATCH (a) WITH collect(a) AS nodes UNWIND nodes AS n MATCH (n) RETURN n",
    ] {
        let ast = parse(query).expect("parse");
        binder
            .bind(&ast)
            .unwrap_or_else(|errors| panic!("expected clean bind for {query}: {errors:?}"));
    }
}

#[test]
fn compatible_variable_reuse_is_accepted() {
    // Guardrail: re-using a variable with the SAME kind, distinct variables,
    // and WITH-rescoping must all still bind cleanly (no false conflict).
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    for q in [
        "MATCH (a) MATCH (a) RETURN a",              // node reused as node
        "MATCH (a), (b) RETURN a, b",                // distinct node vars
        "MATCH (a)-[r]->(b), (b)-[s]->(c) RETURN a", // b: dst then src, both nodes
        "MATCH (a) WITH a MATCH (b) RETURN a, b",    // WITH-rescoped
        "MATCH (a) CREATE (a)-[:R]->(b)",            // matched node referenced in CREATE
    ] {
        let ast = parse(q).expect("parse");
        binder
            .bind(&ast)
            .unwrap_or_else(|e| panic!("expected clean bind for {q}, got {e:?}"));
    }
}

#[test]
fn relationship_var_reused_with_different_type_adds_false_filter() {
    // A relationship variable forwarded through WITH keeps its known type.
    // Reusing it with a different known type is a valid match that returns no
    // rows; encode that independently of whether the edge scan schema carries
    // a `rel_type_name` column.
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    let ast = parse("MATCH ()-[r:T]->() WITH r MATCH ()-[r:Y]->() RETURN r").expect("parse");
    let plan = binder.bind(&ast).expect("bind");

    let has_false_filter = plan.ops.iter().any(|op| match op {
        GraphOp::Filter { predicate } => {
            matches!(
                plan.exprs.get(*predicate),
                IrExpr::Literal(IrLiteral::Bool(false))
            )
        }
        _ => false,
    });

    assert!(
        has_false_filter,
        "reusing a known relationship variable with a different type should filter to no rows"
    );
}

#[test]
fn advisory_unknown_label_produces_warnings() {
    let catalog = Arc::new(Mutex::new(RuntimeCatalog::new()));
    let binder = Binder::new(None, Arc::clone(&catalog), OntologyMode::Advisory);
    let ast = parse("MATCH (a:UnknownLabel) RETURN a").unwrap();

    let mut state = BinderState {
        vars: HashMap::new(),
        path_vars: HashMap::new(),
        node_vars: HashMap::new(),
        edge_vars: HashMap::new(),
        edge_rel_names: HashMap::new(),
        scalar_list_edges: HashSet::new(),
        var_kinds: HashMap::new(),
        next_var: 0,
        builder: GraphPlan::builder("openCypher").ontology_mode(OntologyMode::Advisory),
        errors: Vec::new(),
        warnings: Vec::new(),
        captured_pattern_comprehensions: None,
        existential_depth: 0,
        standalone_call: false,
    };
    for clause in &ast.clauses {
        binder.lower_clause(clause, &mut state);
    }

    assert!(
        !state.warnings.is_empty(),
        "advisory mode should produce warnings"
    );
    assert!(
        state.errors.is_empty(),
        "advisory mode should not produce errors"
    );
    assert!(
        state
            .warnings
            .iter()
            .any(|w| w.kind == BindErrorKind::UnknownLabel)
    );
}

#[test]
fn strict_unknown_label_produces_error() {
    let (binder, _) = make_binder(OntologyMode::Strict);
    let ast = parse("MATCH (a:UnknownLabel) RETURN a").unwrap();
    let result = binder.bind(&ast);
    assert!(result.is_err());
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.kind == BindErrorKind::UnknownLabel));
}

#[test]
fn omitted_restrictions_bind_but_failed_names_never_produce_wildcard_plans() {
    let (binder, catalog) = make_binder(OntologyMode::Strict);
    let plan = binder
        .bind(&parse("MATCH (a)-[r]->(b) RETURN a").unwrap())
        .unwrap();
    assert!(
        plan.ops
            .iter()
            .any(|op| matches!(op, GraphOp::NodeScan { ty: None, .. }))
    );
    assert!(
        plan.ops
            .iter()
            .any(|op| matches!(op, GraphOp::Expand { rel_ty: None, .. }))
    );
    let before = catalog.lock().unwrap().to_record_batch();
    for (query, kind) in [
        (
            "MATCH (a:MissingLabel) RETURN a",
            BindErrorKind::UnknownLabel,
        ),
        (
            "MATCH (a)-[:MISSING_RELATION]->(b) RETURN a",
            BindErrorKind::UnknownRelationType,
        ),
    ] {
        let errors = binder
            .bind(&parse(query).unwrap())
            .expect_err("failed resolution cannot become unrestricted syntax");
        assert!(errors.iter().any(|error| error.kind == kind));
        assert_eq!(catalog.lock().unwrap().to_record_batch(), before);
    }
}

#[test]
fn fixed_hop_with_known_relation_emits_expand() {
    use graphforge_ontology::{
        EntityTypeDef, OntologyCompiler, OntologyDoc, OntologyHandle, RelationTypeDef,
        SemanticFlags,
    };

    let doc = OntologyDoc {
        ontology_id: "test".into(),
        version: "1.0".into(),
        entity_types: vec![
            EntityTypeDef {
                name: "Person".into(),
                r#abstract: false,
                parent: None,
            },
            EntityTypeDef {
                name: "Organization".into(),
                r#abstract: false,
                parent: None,
            },
        ],
        relation_types: vec![RelationTypeDef {
            name: "WORKS_AT".into(),
            src: "Person".into(),
            dst: "Organization".into(),
            inverse: None,
            semantic: SemanticFlags::default(),
        }],
        properties: vec![],
        constraints: vec![],
        migrations: vec![],
    };
    let runtime = OntologyCompiler::compile(&doc).unwrap();
    let handle = OntologyHandle::new(runtime);
    let catalog = Arc::new(Mutex::new(RuntimeCatalog::new()));
    let binder = Binder::new(Some(handle), catalog, OntologyMode::Strict);

    let ast = parse("MATCH (a:Person)-[:WORKS_AT]->(b:Organization) RETURN a").unwrap();
    let plan = binder.bind(&ast).expect("known ontology should succeed");

    // A fixed single hop lowers to a single `Expand` carrying both node vars
    // and the resolved relation type — never a bare edge scan (#718).
    let expand = plan.ops.iter().find_map(|op| match op {
        GraphOp::Expand {
            rel_ty,
            min_hops,
            max_hops,
            ..
        } => Some((rel_ty, *min_hops, *max_hops)),
        _ => None,
    });
    let (rel_ty, min_hops, max_hops) = expand.expect("fixed hop should emit Expand");
    assert!(
        rel_ty.is_some(),
        "WORKS_AT should resolve to a relation type"
    );
    assert_eq!((min_hops, max_hops), (1, Some(1)), "fixed hop is 1..1");
    assert!(
        !plan
            .ops
            .iter()
            .any(|op| matches!(op, GraphOp::TypedEdgeScan { .. } | GraphOp::EdgeScan { .. }))
    );
}

#[test]
fn wildcard_fixed_hop_emits_untyped_expand() {
    let (binder, _) = make_binder(OntologyMode::Exploratory);
    let ast = parse("MATCH (a)-[r]->(b) RETURN r").unwrap();
    let plan = binder.bind(&ast).expect("wildcard bind should succeed");

    // An untyped fixed hop is still an `Expand` (rel_ty None), not an
    // EdgeScan, so the source/destination nodes stay connected (#718).
    let expand = plan.ops.iter().find_map(|op| match op {
        GraphOp::Expand {
            rel_ty,
            min_hops,
            max_hops,
            ..
        } => Some((rel_ty, *min_hops, *max_hops)),
        _ => None,
    });
    let (rel_ty, min_hops, max_hops) = expand.expect("wildcard hop should emit Expand");
    assert!(rel_ty.is_none(), "wildcard hop has no relation type");
    assert_eq!((min_hops, max_hops), (1, Some(1)));
    assert!(
        !plan
            .ops
            .iter()
            .any(|op| matches!(op, GraphOp::TypedEdgeScan { .. } | GraphOp::EdgeScan { .. }))
    );
}

#[test]
fn inline_node_property_emits_filter() {
    // #748: an inline property map becomes a Filter over the scanned node.
    let (binder, _) = make_binder(OntologyMode::Exploratory);
    let ast = parse("MATCH (a:Person {name:'Alice'}) RETURN a.name").unwrap();
    let plan = binder.bind(&ast).unwrap();
    assert!(
        plan.ops
            .iter()
            .any(|op| matches!(op, GraphOp::NodeScan { .. })),
        "scan present"
    );
    assert!(
        plan.ops
            .iter()
            .any(|op| matches!(op, GraphOp::Filter { .. })),
        "inline property must emit a Filter"
    );
}

#[test]
fn inline_multi_property_emits_single_filter() {
    // Multiple inline properties AND-combine into one Filter op.
    let (binder, _) = make_binder(OntologyMode::Exploratory);
    let ast = parse("MATCH (a:Person {name:'Alice', age:30}) RETURN a.name").unwrap();
    let plan = binder.bind(&ast).unwrap();
    let filters = plan
        .ops
        .iter()
        .filter(|op| matches!(op, GraphOp::Filter { .. }))
        .count();
    assert_eq!(filters, 1, "multi-property map → one AND-ed Filter");
}

#[test]
fn node_without_inline_properties_emits_no_filter() {
    // Regression: a property-free node pattern must not add a spurious Filter.
    let (binder, _) = make_binder(OntologyMode::Exploratory);
    let ast = parse("MATCH (a:Person) RETURN a.name").unwrap();
    let plan = binder.bind(&ast).unwrap();
    assert!(
        !plan
            .ops
            .iter()
            .any(|op| matches!(op, GraphOp::Filter { .. })),
        "no inline properties → no Filter"
    );
}

#[test]
fn inline_rel_property_emits_filter() {
    // #750: an inline relationship-property map becomes a Filter over the
    // just-expanded edge, mirroring the node case (#748).
    let (binder, _) = make_binder(OntologyMode::Exploratory);
    let ast = parse("MATCH (a:Person)-[r:KNOWS {since:2020}]->(b:Person) RETURN b.name").unwrap();
    let plan = binder.bind(&ast).unwrap();
    assert!(
        plan.ops
            .iter()
            .any(|op| matches!(op, GraphOp::Expand { .. })),
        "expand present"
    );
    assert!(
        plan.ops
            .iter()
            .any(|op| matches!(op, GraphOp::Filter { .. })),
        "inline relationship property must emit a Filter"
    );
}

#[test]
fn inline_multi_rel_property_emits_single_filter() {
    // Multiple inline rel properties AND-combine into one Filter op.
    let (binder, _) = make_binder(OntologyMode::Exploratory);
    let ast = parse("MATCH (a:Person)-[r:KNOWS {since:2020, weight:5}]->(b:Person) RETURN b.name")
        .unwrap();
    let plan = binder.bind(&ast).unwrap();
    let filters = plan
        .ops
        .iter()
        .filter(|op| matches!(op, GraphOp::Filter { .. }))
        .count();
    assert_eq!(filters, 1, "multi-property rel map → one AND-ed Filter");
}

#[test]
fn rel_without_inline_properties_emits_no_filter() {
    // Regression: a property-free relationship must not add a spurious Filter.
    let (binder, _) = make_binder(OntologyMode::Exploratory);
    let ast = parse("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN b.name").unwrap();
    let plan = binder.bind(&ast).unwrap();
    assert!(
        !plan
            .ops
            .iter()
            .any(|op| matches!(op, GraphOp::Filter { .. })),
        "no inline rel properties → no Filter"
    );
}

#[test]
fn inline_property_on_var_length_rel_emits_all_filter() {
    let (binder, _) = make_binder(OntologyMode::Exploratory);
    let ast =
        parse("MATCH (a:Person)-[r:KNOWS*1..2 {since:2020}]->(b:Person) RETURN b.name").unwrap();
    let plan = binder.bind(&ast).unwrap();
    let predicate = plan.ops.iter().find_map(|op| match op {
        GraphOp::Filter { predicate } => Some(*predicate),
        _ => None,
    });
    assert!(
        predicate.is_some_and(|predicate| matches!(
            plan.exprs.get(predicate),
            IrExpr::Quantifier {
                kind: graphforge_ast::QuantifierKind::All,
                ..
            }
        )),
        "variable-length relationship properties require an all() filter"
    );
}

// -----------------------------------------------------------------------
// Named path variables (#754)
// -----------------------------------------------------------------------

/// Bind `query` and return the errors (panics if the bind succeeds).
fn bind_errors(query: &str) -> Vec<BindError> {
    let (binder, _) = make_binder(OntologyMode::Exploratory);
    let ast = parse(query).unwrap();
    binder
        .bind(&ast)
        .expect_err("bind should fail for this query")
}

#[test]
fn path_var_functions_bind_with_anonymous_edge() {
    // The binder allocates an anon VarId for `[*1..2]`, so the rewrites
    // have an edge var to target even without `[r:...]`.
    let (binder, _) = make_binder(OntologyMode::Exploratory);
    let ast = parse(
        "MATCH p = (a:Person)-[*1..2]->(b) \
             RETURN nodes(p) AS ns, relationships(p) AS rs, length(p) AS l",
    )
    .unwrap();
    binder.bind(&ast).expect("path functions should bind");
}

#[test]
fn path_function_on_non_path_falls_through() {
    // `length(r)` on a var-length edge var keeps its generic lowering and
    // `nodes(a)` on a node var stays a generic function call — neither is
    // intercepted by the path rewrite.
    let (binder, _) = make_binder(OntologyMode::Exploratory);
    let ast = parse("MATCH (a)-[r:KNOWS*1..2]->(b) RETURN length(r) AS l, nodes(a) AS ns").unwrap();
    let plan = binder.bind(&ast).expect("bind succeeds");
    let names: Vec<&str> = (0..plan.exprs.len())
        .filter_map(
            |i| match plan.exprs.get(ExprId(u32::try_from(i).unwrap())) {
                IrExpr::FunctionCall { name, .. } => Some(name.as_str()),
                _ => None,
            },
        )
        .collect();
    assert!(names.contains(&"length"), "generic length kept: {names:?}");
    assert!(names.contains(&"nodes"), "generic nodes kept: {names:?}");
    assert!(
        !names.contains(&"_path_nodes"),
        "no path rewrite without a path var: {names:?}"
    );
}

#[test]
fn bare_path_var_binds_to_path_struct() {
    // `RETURN p` rewrites to `_path_struct(<nodes>, <relationships>)`.
    let (binder, _) = make_binder(OntologyMode::Exploratory);
    let ast = parse("MATCH p = (a)-[:KNOWS*1..2]->(b) RETURN p").unwrap();
    let plan = binder.bind(&ast).expect("bare path value binds");
    let has_struct = (0..plan.exprs.len()).any(|i| {
        matches!(
            plan.exprs.get(ExprId(u32::try_from(i).unwrap())),
            IrExpr::FunctionCall { name, .. } if name == "_path_struct"
        )
    });
    assert!(has_struct, "RETURN p must rewrite to _path_struct");
}

#[test]
fn multi_segment_path_var_composes_path_functions() {
    let (binder, _) = make_binder(OntologyMode::Exploratory);
    let ast = parse(
        "MATCH p = (a)-[:KNOWS]->(b)-[:KNOWS]->(c) \
             RETURN length(p), nodes(p), relationships(p)",
    )
    .unwrap();
    let plan = binder.bind(&ast).expect("multi-segment path binds");
    let add_count = (0..plan.exprs.len())
        .map(|index| plan.exprs.get(ExprId(index as u32)))
        .filter(|expr| {
            matches!(
                expr,
                IrExpr::BinaryOp {
                    op: BinaryOpKind::Add,
                    ..
                }
            )
        })
        .count();
    assert!(add_count >= 3, "each path function composes its segments");
}

#[test]
fn fixed_segment_path_functions_bind() {
    // A fixed single hop composes from scalar edge/node columns:
    // length → _path_fixed_length, nodes → _node_struct_list,
    // relationships → _rel_struct_list.
    let (binder, _) = make_binder(OntologyMode::Exploratory);
    let ast = parse(
        "MATCH p = (a)-[:KNOWS]->(b) \
             RETURN length(p) AS l, nodes(p) AS ns, relationships(p) AS rs",
    )
    .unwrap();
    let plan = binder.bind(&ast).expect("fixed path functions bind");
    let names: Vec<&str> = (0..plan.exprs.len())
        .filter_map(
            |i| match plan.exprs.get(ExprId(u32::try_from(i).unwrap())) {
                IrExpr::FunctionCall { name, .. } => Some(name.as_str()),
                _ => None,
            },
        )
        .collect();
    for expected in [
        "_path_fixed_length",
        "_node_struct_list",
        "_rel_struct_list",
    ] {
        assert!(names.contains(&expected), "missing {expected}: {names:?}");
    }
}

#[test]
fn explicit_one_hop_routes_as_fixed_segment() {
    // `*1..1` goes to the relational join (no list column), so the path
    // rewrite must treat it as a fixed segment too.
    let (binder, _) = make_binder(OntologyMode::Exploratory);
    let ast = parse("MATCH p = (a)-[:KNOWS*1..1]->(b) RETURN length(p) AS l").unwrap();
    let plan = binder.bind(&ast).expect("explicit 1..1 binds as fixed");
    let has_fixed = (0..plan.exprs.len()).any(|i| {
        matches!(
            plan.exprs.get(ExprId(u32::try_from(i).unwrap())),
            IrExpr::FunctionCall { name, .. } if name == "_path_fixed_length"
        )
    });
    assert!(
        has_fixed,
        "explicit *1..1 must use the fixed-segment rewrite"
    );
}

#[test]
fn path_var_name_conflict_is_rejected() {
    let errors = bind_errors("MATCH p = (p)-[:KNOWS*1..2]->(b) RETURN length(p)");
    assert!(
        errors
            .iter()
            .any(|e| matches!(e.kind, BindErrorKind::DuplicateVariable)),
        "expected DuplicateVariable, got {errors:?}"
    );
}

#[test]
fn optional_match_path_var_functions_bind() {
    // path_vars introduced inside OPTIONAL MATCH must propagate to the
    // outer scope so a later RETURN can rewrite against them.
    let (binder, _) = make_binder(OntologyMode::Exploratory);
    let ast = parse(
        "MATCH (a:Person) \
             OPTIONAL MATCH p = (a)-[:KNOWS*1..2]->(b) \
             RETURN nodes(p) AS ns, length(p) AS l",
    )
    .unwrap();
    binder.bind(&ast).expect("optional path functions bind");
}

#[test]
fn exact_zero_embedded_pattern_predicate_shape_is_rejected() {
    let ast = parse("MATCH (n) WHERE (n)-->(m) RETURN n").unwrap();
    let AstClause::Match(match_clause) = &ast.clauses[0] else {
        panic!("expected MATCH clause")
    };
    let where_clause = match_clause.where_clause.as_ref().expect("inline WHERE");
    let predicate = Expr::BinaryOp(graphforge_ast::BinaryOp {
        op: AstBinOp::Eq,
        left: Box::new(where_clause.predicate.clone()),
        right: Box::new(Expr::Literal(Literal::Bool(true, where_clause.span))),
        span: where_clause.span,
    });
    let (binder, _) = make_binder(OntologyMode::Exploratory);
    let mut state = empty_state(OntologyMode::Exploratory);
    state.vars.insert("n".into(), VarId(0));
    state.node_vars.insert(VarId(0), None);
    state.var_kinds.insert(VarId(0), VarKind::Node);
    binder.lower_where_predicate(&predicate, predicate.span(), &mut state);
    assert!(state.errors.iter().any(|error| {
        error.kind == BindErrorKind::InvalidArgument
            && error
                .message
                .contains("supported only as single-relationship")
    }));
}

#[test]
fn exact_zero_nested_pattern_comprehension_cardinality_is_rejected() {
    let query = "MATCH (a) RETURN [x IN nodes([(a)-->(b) | a]) | [(x)-->(c) | c] + [(x)-->(d) | d]] AS values";
    let (binder, _) = make_binder(OntologyMode::Exploratory);
    let errors = binder
        .bind(&parse(query).unwrap())
        .expect_err("one list comprehension cannot capture multiple child patterns");
    assert!(
        errors.iter().any(|error| {
            error.kind == BindErrorKind::InvalidArgument
                && error
                    .message
                    .contains("exactly one nested pattern comprehension")
        }),
        "errors={errors:?}"
    );
}
