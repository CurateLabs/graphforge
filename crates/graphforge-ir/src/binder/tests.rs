use super::{
    BindErrorKind, Binder, BinderState, PathBinding, VarKind, procedure_argument_type_matches,
};
use crate::catalog::RuntimeCatalog;
use crate::expr::{IrExpr, IrLiteral};
use crate::plan::{GraphOp, GraphPlan, OntologyMode};
use crate::{ProcedureDefinition, ProcedureField, ProcedureRegistry, VarId};
use arrow::array::{StringArray, UInt32Array, UInt64Array};
use graphforge_ast::{AstClause, AstQuery, Expr, Literal, PropertyAccess};
use graphforge_core::Span;
use graphforge_cypher::parse;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

pub(super) fn make_binder(mode: OntologyMode) -> (Binder, Arc<Mutex<RuntimeCatalog>>) {
    let catalog = Arc::new(Mutex::new(RuntimeCatalog::new()));
    let binder = Binder::new(None, Arc::clone(&catalog), mode);
    (binder, catalog)
}

pub(super) fn empty_state(mode: OntologyMode) -> BinderState {
    BinderState {
        vars: HashMap::new(),
        path_vars: HashMap::new(),
        node_vars: HashMap::new(),
        edge_vars: HashMap::new(),
        edge_rel_names: HashMap::new(),
        scalar_list_edges: HashSet::new(),
        var_kinds: HashMap::new(),
        next_var: 0,
        builder: GraphPlan::builder("openCypher").ontology_mode(mode),
        errors: Vec::new(),
        warnings: Vec::new(),
        captured_pattern_comprehensions: None,
        existential_depth: 0,
        standalone_call: false,
    }
}

pub(super) fn parsed_return_expr(source: &str) -> Expr {
    let ast = parse(source).unwrap_or_else(|error| panic!("failed to parse {source:?}: {error}"));
    let Some(AstClause::Return(clause)) = ast.clauses.last() else {
        panic!("expected RETURN clause for {source:?}");
    };
    clause.items[0].expr.clone()
}

fn catalog_entry(catalog: &RuntimeCatalog, kind: &str, name: &str) -> (u32, u64) {
    let batch = catalog.to_record_batch();
    let kinds = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let names = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let ids = batch
        .column(2)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();
    let counts = batch
        .column(3)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let row = (0..batch.num_rows())
        .find(|&row| kinds.value(row) == kind && names.value(row) == name)
        .unwrap_or_else(|| panic!("missing catalog entry {kind}:{name}"));
    (ids.value(row), counts.value(row))
}

#[test]
fn bind_catalog_mutations_commit_only_after_success() {
    let (binder, catalog) = make_binder(OntologyMode::Exploratory);
    {
        let mut catalog = catalog.lock().unwrap();
        assert_eq!(catalog.intern_label("Seed").unwrap().get(), 0);
        assert_eq!(catalog.intern_relation_type("SEED_REL").unwrap().get(), 1);
        assert_eq!(catalog.intern_property("seed", None).unwrap().get(), 0);
    }
    let before = catalog.lock().unwrap().to_record_batch();

    let errors = binder
        .bind(
            &parse(
                "MATCH (n:Rejected)-[:REJECTED_REL]->() \
                     RETURN n.rejected, missing",
            )
            .unwrap(),
        )
        .expect_err("a later semantic error must reject every staged observation");
    assert!(
        errors
            .iter()
            .any(|error| error.kind == BindErrorKind::UndeclaredVariable)
    );
    assert_eq!(
        catalog.lock().unwrap().to_record_batch(),
        before,
        "failed binding must leave entries, observations, timestamps, and IDs unchanged"
    );

    binder
        .bind(
            &parse(
                "MATCH (n:Accepted)-[:ACCEPTED_REL]->() \
                     RETURN n.accepted",
            )
            .unwrap(),
        )
        .expect("successful binding must publish the staged catalog");
    let catalog = catalog.lock().unwrap();
    assert_eq!(catalog_entry(&catalog, "entity_type", "Accepted"), (2, 1));
    assert_eq!(
        catalog_entry(&catalog, "relation_type", "ACCEPTED_REL"),
        (3, 1)
    );
    assert_eq!(catalog_entry(&catalog, "property", "accepted"), (1, 1));
}

// -----------------------------------------------------------------------
// Validator tail (#956): CREATE-pattern, rebind, aggregate-in-WHERE, alias
// -----------------------------------------------------------------------

/// Bind and assert the query is REJECTED with any bind error of `kind`.
pub(super) fn expect_bind_error(query: &str, kind: BindErrorKind) {
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    let ast = parse(query).expect("parse");
    let errs = binder
        .bind(&ast)
        .expect_err(&format!("expected a bind error for: {query}"));
    assert!(
        errs.iter().any(|e| e.kind == kind),
        "expected {kind:?} for {query}, got {errs:?}"
    );
}

#[test]
fn multi_error_collection_in_strict_mode() {
    let (binder, _) = make_binder(OntologyMode::Strict);
    let ast = parse("MATCH (a:LabelA)-[:REL_B]->(b:LabelC) RETURN a").unwrap();
    let result = binder.bind(&ast);
    assert!(result.is_err());
    let errors = result.unwrap_err();
    // LabelA, REL_B, LabelC → at least 3 errors
    assert!(
        errors.len() >= 3,
        "expected ≥3 errors, got {}",
        errors.len()
    );
}

#[test]
fn where_clause_emits_filter_op() {
    let (binder, _) = make_binder(OntologyMode::Exploratory);
    let ast = parse("MATCH (a:Person) WHERE a.age > 30 RETURN a").unwrap();
    let plan = binder.bind(&ast).unwrap();
    assert!(
        plan.ops
            .iter()
            .any(|op| matches!(op, GraphOp::Filter { .. }))
    );
}

pub(super) fn procedure_binder() -> Binder {
    let (binder, _) = make_binder(OntologyMode::Exploratory);
    let procedure = ProcedureDefinition {
        name: "test.proc".into(),
        inputs: vec![ProcedureField {
            name: "in".into(),
            type_name: "INTEGER".into(),
            nullable: true,
        }],
        outputs: vec![ProcedureField {
            name: "out".into(),
            type_name: "INTEGER".into(),
            nullable: true,
        }],
        rows: vec![vec![IrLiteral::Int(1), IrLiteral::Int(2)]],
    };
    binder.with_procedures(Arc::new(ProcedureRegistry::from([(
        procedure.name.clone(),
        procedure,
    )])))
}

#[test]
fn call_binds_explicit_args_and_yield_alias() {
    let plan = procedure_binder()
        .bind(&parse("CALL test.proc(1) YIELD out AS value RETURN value").unwrap())
        .expect("CALL should bind");
    let GraphOp::Call { args, yields, .. } = &plan.ops[0] else {
        panic!("expected CALL op")
    };
    assert_eq!(args.len(), 1);
    assert_eq!(yields[0].field, "out");
    assert_eq!(yields[0].alias, "value");
}

#[test]
fn call_without_parentheses_uses_implicit_parameters() {
    let plan = procedure_binder()
        .bind(&parse("CALL test.proc YIELD out").unwrap())
        .expect("implicit CALL should bind");
    let GraphOp::Call { args, .. } = &plan.ops[0] else {
        panic!("expected CALL op")
    };
    assert!(matches!(plan.exprs.get(args[0]), IrExpr::Parameter(name) if name == "in"));
}

#[test]
fn call_rejects_unknown_procedure_argument_count_and_yield() {
    for (query, message) in [
        ("CALL missing.proc()", "ProcedureNotFound"),
        ("CALL test.proc()", "InvalidNumberOfArguments"),
        ("CALL test.proc(1) YIELD missing", "ProcedureOutputNotFound"),
    ] {
        let errors = procedure_binder()
            .bind(&parse(query).unwrap())
            .expect_err("CALL should fail");
        assert!(
            errors.iter().any(|error| error.message.contains(message)),
            "expected {message}, got {errors:?}"
        );
    }
}

#[test]
fn union_binds_branch_plans_and_mode() {
    let (binder, _) = make_binder(OntologyMode::Exploratory);
    let plan = binder
        .bind(&parse("RETURN 1 AS x UNION ALL RETURN 2 AS x").unwrap())
        .expect("UNION ALL binds");
    let [GraphOp::Union { all, inputs }] = plan.ops.as_slice() else {
        panic!("expected one UNION op")
    };
    assert!(*all);
    assert_eq!(inputs.len(), 2);
    assert!(inputs.iter().all(|branch| !branch.ops.is_empty()));
}

#[test]
fn union_rejects_mixed_modes_and_different_columns() {
    let (binder, _) = make_binder(OntologyMode::Exploratory);
    for (query, message) in [
        (
            "RETURN 1 AS x UNION RETURN 2 AS x UNION ALL RETURN 3 AS x",
            "InvalidCombinationOfUnion",
        ),
        (
            "RETURN 1 AS x UNION RETURN 2 AS y",
            "DifferentColumnsInUnion",
        ),
        ("CREATE (:A) UNION CREATE (:B)", "DifferentColumnsInUnion"),
    ] {
        let errors = binder
            .bind(&parse(query).unwrap())
            .expect_err("UNION should fail");
        assert!(errors.iter().any(|error| error.message.contains(message)));
    }
}

#[test]
fn procedure_literal_type_checks_cover_nullability_and_scalar_domains() {
    let field = |type_name: &str, nullable| ProcedureField {
        name: "arg".into(),
        type_name: type_name.into(),
        nullable,
    };
    assert!(procedure_argument_type_matches(
        &parsed_return_expr("RETURN null"),
        &field("STRING", true)
    ));
    assert!(!procedure_argument_type_matches(
        &parsed_return_expr("RETURN null"),
        &field("STRING", false)
    ));
    assert!(procedure_argument_type_matches(
        &parsed_return_expr("RETURN 1"),
        &field("INTEGER", false)
    ));
    assert!(procedure_argument_type_matches(
        &parsed_return_expr("RETURN 1"),
        &field("NUMBER", false)
    ));
    assert!(!procedure_argument_type_matches(
        &parsed_return_expr("RETURN 1"),
        &field("STRING", false)
    ));
    assert!(procedure_argument_type_matches(
        &parsed_return_expr("RETURN 1.5"),
        &field("FLOAT", false)
    ));
    assert!(!procedure_argument_type_matches(
        &parsed_return_expr("RETURN 1.5"),
        &field("INTEGER", false)
    ));
    assert!(procedure_argument_type_matches(
        &parsed_return_expr("RETURN 'x'"),
        &field("STRING", false)
    ));
    assert!(!procedure_argument_type_matches(
        &parsed_return_expr("RETURN 'x'"),
        &field("BOOLEAN", false)
    ));
    assert!(procedure_argument_type_matches(
        &parsed_return_expr("RETURN true"),
        &field("BOOLEAN", false)
    ));
    assert!(!procedure_argument_type_matches(
        &parsed_return_expr("RETURN true"),
        &field("STRING", false)
    ));
    assert!(procedure_argument_type_matches(
        &parsed_return_expr("RETURN (1)"),
        &field("INTEGER", false)
    ));
    assert!(procedure_argument_type_matches(
        &parsed_return_expr("RETURN a"),
        &field("ANY", false)
    ));
}

#[test]
fn binder_exercises_fragmented_clause_and_expression_error_paths() {
    let cases = [
        ("MATCH (n) WHERE n.active = true RETURN n", true),
        ("MATCH (n) SET missing.value = 1 RETURN n", false),
        ("MATCH (n) REMOVE missing.value RETURN n", false),
        ("MATCH (n) DELETE n.name", true),
        ("MATCH (n) WITH n.name RETURN n", false),
        ("MATCH (n) WITH count(*) AS total RETURN total", true),
        (
            "MATCH (n) RETURN n ORDER BY n.name DESC SKIP (1 + 2) LIMIT toInteger(3.5)",
            true,
        ),
        ("MATCH (n) RETURN percentileCont(n.value)", false),
        (
            "MATCH (n) RETURN percentileCont(DISTINCT n.value, 0.5)",
            false,
        ),
        ("MATCH (n) RETURN 'a' + 'b'", true),
        ("MATCH (n) RETURN NOT (n.value IN [1, 2])", true),
        (
            "MATCH (n) WHERE exists { (n)-->(m) WHERE count(*) > 0 } RETURN n",
            false,
        ),
        ("MATCH (n) RETURN [(n)-->(m) WHERE count(*) > 0 | m]", false),
        ("MATCH (n) WHERE (n)-->(m) OR (n)-->(x) RETURN n", false),
        ("MATCH (n) WHERE n.active OR (n)-->(m) RETURN n", false),
        ("UNWIND [1, 2] AS x RETURN x", true),
    ];
    for (query, should_bind) in cases {
        let ast = parse(query).unwrap_or_else(|error| panic!("query={query}: {error}"));
        let (binder, _) = make_binder(OntologyMode::Exploratory);
        let result = binder.bind(&ast);
        assert_eq!(
            result.is_ok(),
            should_bind,
            "query={query}, result={result:?}"
        );
    }
}

#[test]
fn binder_query_matrix_freezes_union_write_and_predicate_boundaries() {
    let cases = [
        ("RETURN 1 AS x UNION RETURN 2 AS x", true),
        ("RETURN 1 AS x UNION ALL RETURN 2 AS x", true),
        (
            "RETURN 1 AS x UNION RETURN 2 AS x UNION ALL RETURN 3 AS x",
            false,
        ),
        ("RETURN 1 AS x UNION RETURN 2 AS y", false),
        (
            "MERGE (n:Person {id: 1}) ON CREATE SET n.name = 'Ada' ON MATCH SET n.name = 'Grace' RETURN n",
            true,
        ),
        (
            "MATCH (n:Person) SET n += {score: 1}, n:Employee RETURN n",
            true,
        ),
        ("MATCH (n:Person) SET n = {score: 1} RETURN n", true),
        ("MATCH (n:Person) REMOVE n.score, n:Employee RETURN n", true),
        ("MATCH (n:Person) WHERE n:Employee RETURN n", true),
        ("MATCH (a)-[r:KNOWS]->(b) WHERE r:LIKES RETURN r", true),
        ("UNWIND [1, 2] AS x RETURN all(y IN [x] WHERE y > 0)", true),
        (
            "MATCH (n) RETURN [x IN [1, 2] WHERE x > 1 | x + 1] AS values",
            true,
        ),
        ("MATCH (n) RETURN n:Person", true),
        ("WITH 1 AS x RETURN x:Person", false),
        (
            "MATCH (a)-[r:KNOWS*1..2 {since: 2020, active: true}]->(b) RETURN r",
            true,
        ),
        (
            "MERGE (n:Person) ON CREATE SET missing += {score: 1} RETURN n",
            false,
        ),
        (
            "MERGE (n:Person) ON MATCH SET missing:Employee RETURN n",
            false,
        ),
        ("MATCH (n) SET missing += {score: 1} RETURN n", false),
        ("MATCH (n) SET missing:Person RETURN n", false),
        ("MATCH (n) REMOVE missing:Person RETURN n", false),
        ("MATCH (n) DELETE [n]", false),
        ("MATCH (n) RETURN NOT (n.score IN [1, 2])", true),
        ("MATCH (n) RETURN missing:Person", false),
        ("MATCH (n) CALL missing.procedure() RETURN n", false),
        ("MATCH (n) RETURN exists { (n)-->(m) }", false),
    ];

    for (query, should_bind) in cases {
        let ast = parse(query).unwrap_or_else(|error| panic!("query={query}: {error}"));
        let (binder, _) = make_binder(OntologyMode::Exploratory);
        let result = binder.bind(&ast);
        assert_eq!(
            result.is_ok(),
            should_bind,
            "query={query}, result={result:?}"
        );
    }
}

#[test]
fn exact_zero_binder_semantic_branches_have_public_query_oracles() {
    let cases = [
        ("RETURN 1 + 2 AS value", true),
        ("RETURN count(*) AS x UNION RETURN count(*) AS x", true),
        (
            "MATCH p=(a)-[r]->(b) WITH p, count(*) AS total RETURN p, total",
            true,
        ),
        (
            "MATCH p=(a)-[r]->(b) WITH p, count(*) + 1 AS total RETURN p, total",
            true,
        ),
        (
            "MATCH (a)-[r]->(b) WITH r, count(*) + 1 AS total RETURN r, total",
            true,
        ),
        (
            "WITH 7 AS x RETURN any(x IN [1, 2] WHERE x > 1) AS found, x",
            true,
        ),
    ];
    for (query, should_bind) in cases {
        let (binder, _) = make_binder(OntologyMode::Exploratory);
        let result = binder.bind(&parse(query).unwrap());
        assert_eq!(
            result.is_ok(),
            should_bind,
            "query={query}, result={result:?}"
        );
    }
}

#[test]
fn exact_zero_union_empty_branches_are_reported_without_panicking() {
    let parsed = parse("RETURN 1 AS x").unwrap();
    for clauses in [
        vec![
            AstClause::Union(graphforge_ast::UnionClause {
                all: false,
                span: Span::new(0, 5),
            }),
            parsed.clauses[0].clone(),
        ],
        vec![
            parsed.clauses[0].clone(),
            AstClause::Union(graphforge_ast::UnionClause {
                all: false,
                span: Span::new(10, 15),
            }),
        ],
    ] {
        let query = AstQuery {
            dialect: parsed.dialect,
            clauses,
            span: Span::new(0, 15),
        };
        let (binder, _) = make_binder(OntologyMode::Exploratory);
        let errors = binder
            .bind(&query)
            .expect_err("empty UNION branch must fail");
        assert!(errors.iter().any(|error| {
            error.kind == BindErrorKind::InvalidArgument
                && error.message == "UNION requires a query on both sides"
        }));
    }
}

#[test]
fn exact_zero_direct_property_guards_cover_path_identity_and_write_targets() {
    let (binder, _) = make_binder(OntologyMode::Exploratory);
    let mut state = empty_state(OntologyMode::Exploratory);
    state.path_vars.insert(
        "p".into(),
        PathBinding {
            nodes: vec![VarId(0)],
            segments: Vec::new(),
        },
    );
    let path_property = parsed_return_expr("RETURN p.name");
    binder.lower_expr(&path_property, path_property.span(), &mut state);
    assert!(state.errors.iter().any(|error| {
        error.kind == BindErrorKind::InvalidArgument
            && error.message.contains("not valid on a path")
    }));

    state.vars.insert("r".into(), VarId(1));
    state.var_kinds.insert(VarId(1), VarKind::Relationship);
    let wrong_identity = parsed_return_expr("RETURN r.node_uuid");
    binder.lower_expr(&wrong_identity, wrong_identity.span(), &mut state);
    assert!(state.errors.iter().any(|error| {
        error.kind == BindErrorKind::InvalidArgument
            && error.message.contains("valid only on a node")
    }));

    let malformed = PropertyAccess {
        object: Box::new(Expr::Literal(Literal::Int(1, Span::new(0, 1)))),
        key: "name".into(),
        span: Span::new(0, 6),
    };
    assert!(
        binder
            .resolve_write_target(&malformed, &mut state)
            .is_none()
    );
    assert!(state.errors.iter().any(|error| {
        error.kind == BindErrorKind::InvalidDeleteTarget
            && error
                .message
                .contains("write target must be a bound variable")
    }));
}
