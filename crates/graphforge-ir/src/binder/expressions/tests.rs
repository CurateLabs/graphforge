use super::super::tests::{
    empty_state, expect_bind_error, make_binder, parsed_return_expr, procedure_binder,
};
use super::super::{BindError, BindErrorKind, Binder};
use crate::ExprId;
use crate::catalog::RuntimeCatalog;
use crate::expr::{IrExpr, IrLiteral};
use crate::plan::{GraphOp, GraphPlan, OntologyMode};
use graphforge_ast::{BinaryOpKind as AstBinOp, Expr};
use graphforge_cypher::parse;
use graphforge_ontology::{OntologyCompiler, OntologyDoc, OntologyHandle};
use graphforge_value::PropertyId;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

fn strict_property_binder(shadow_inherited: bool) -> (Binder, Arc<Mutex<RuntimeCatalog>>) {
    let shadow = if shadow_inherited {
        r#", {"owner":"Host","name":"inherited","type":"utf8"}"#
    } else {
        ""
    };
    let doc: OntologyDoc = serde_json::from_str(&format!(
            r#"{{"ontology_id":"strict-properties","version":"1","entity_types":[{{"name":"Asset"}},{{"name":"Host","parent":"Asset"}}],"relation_types":[{{"name":"R","src":"Host","dst":"Host"}},{{"name":"S","src":"Host","dst":"Host"}}],"properties":[{{"owner":"Asset","name":"inherited","type":"utf8"}},{{"owner":"Host","name":"direct","type":"utf8"}},{{"owner":"R","name":"weight","type":"int64"}},{{"owner":"S","name":"weight","type":"int64"}},{{"owner":"Asset","name":"shared","type":"utf8"}},{{"owner":"R","name":"shared","type":"utf8"}}{shadow}]}}"#
        ))
        .unwrap();
    let ontology = OntologyCompiler::compile(&doc).unwrap();
    let catalog = Arc::new(Mutex::new(RuntimeCatalog::new()));
    catalog
        .lock()
        .unwrap()
        .intern_property("preexisting", None)
        .unwrap();
    (
        Binder::new(
            Some(OntologyHandle::new(ontology)),
            Arc::clone(&catalog),
            OntologyMode::Strict,
        ),
        catalog,
    )
}

fn plan_property_ids(plan: &GraphPlan) -> Vec<PropertyId> {
    (0..u32::try_from(plan.exprs.len()).unwrap())
        .filter_map(|index| match plan.exprs.get(ExprId(index)) {
            IrExpr::PropertyAccess { prop, .. } => Some(*prop),
            _ => None,
        })
        .collect()
}

fn property_error(binder: &Binder, query: &str, kind: BindErrorKind, span: &str) -> BindError {
    let errors = binder.bind(&parse(query).unwrap()).expect_err(query);
    assert_eq!(errors.len(), 1, "{query}: {errors:?}");
    let error = errors.into_iter().next().unwrap();
    assert_eq!(error.kind, kind);
    assert_eq!(&query[error.span.start..error.span.end], span);
    error
}

#[test]
fn return_rejects_unknown_function_at_bind() {
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    let ast = parse("MATCH (a) RETURN foo(a)").unwrap();
    let errors = binder.bind(&ast).expect_err("unknown function must fail");
    assert!(
        errors
            .iter()
            .any(|error| error.message.contains("unknown function `foo`"))
    );
}

#[test]
fn direct_graph_function_kind_mismatches_are_rejected() {
    let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
    for query in [
        "MATCH (n) RETURN type(n)",
        "MATCH ()-[r]->() RETURN labels(r)",
        "MATCH (n) RETURN length(n)",
        "MATCH ()-[r]->() RETURN length(r)",
        "MATCH p = (n) RETURN labels(p)",
    ] {
        let ast = parse(query).expect("parse");
        let errors = binder.bind(&ast).expect_err("expected invalid argument");
        assert!(
            errors
                .iter()
                .any(|error| error.kind == BindErrorKind::InvalidArgument),
            "expected InvalidArgument for {query}, got {errors:?}"
        );
    }
}

#[test]
fn overflowing_float_literal_is_rejected_at_bind() {
    expect_bind_error("RETURN 1.34E999", BindErrorKind::InvalidArgument);
}

#[test]
fn strict_mode_allows_durable_identity_fields() {
    let (binder, _) = make_binder(OntologyMode::Strict);
    let ast = parse(
            "MATCH (source)-[relationship]->(target) RETURN source.node_uuid, relationship.edge_uuid, target.node_uuid",
        )
        .unwrap();

    binder
        .bind(&ast)
        .expect("structural identity fields are not ontology properties");
}

#[test]
fn strict_properties_emit_owner_scoped_runtime_ids() {
    let (binder, catalog) = strict_property_binder(false);
    let ast = parse("MATCH (host:Host)-[connection:R]->() RETURN host.direct, host.inherited, host.shared, connection.weight, connection.shared").unwrap();
    let plan = binder.bind(&ast).unwrap();
    let ids = plan_property_ids(&plan);
    let catalog = catalog.lock().unwrap();
    let names = ids
        .iter()
        .map(|id| {
            catalog
                .property_name(
                    id.runtime_id()
                        .expect("strict property retains runtime identity"),
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(names, ["direct", "inherited", "shared", "weight", "shared"]);
    assert!(
        ids.iter()
            .all(|id| id.runtime_id().is_some_and(|id| id.get() > 0))
    );
    assert_ne!(ids[2], ids[4]);
}

#[test]
fn strict_property_writes_and_inline_filters_share_owner_rules() {
    let (binder, catalog) = strict_property_binder(false);
    let ast = parse("MATCH (host:Host)-[connection:R]->() SET host.direct = 'ready', host.inherited = 'asset', connection.weight = 7 REMOVE host.direct, connection.weight").unwrap();
    let plan = binder.bind(&ast).unwrap();
    let catalog = catalog.lock().unwrap();
    let write_ids = plan
        .ops
        .iter()
        .flat_map(|op| match op {
            GraphOp::Set { items, .. } => items.iter().map(|item| item.prop).collect(),
            GraphOp::Remove { items, .. } => items.iter().map(|item| item.prop).collect(),
            _ => Vec::new(),
        })
        .collect::<Vec<_>>();
    assert_eq!(write_ids.len(), 5);
    assert!(write_ids.iter().all(|id| {
        catalog
            .property_name(
                id.runtime_id()
                    .expect("strict property retains runtime identity"),
            )
            .is_some()
    }));
    drop(catalog);

    for query in [
        "MATCH (:Host {direct: 'ready'})-[:R {weight: 7}]->() RETURN 1",
        "MATCH (:Host {inherited: 'asset'})-[:R*1..2 {weight: 7}]->() RETURN 1",
    ] {
        binder
            .bind(&parse(query).unwrap())
            .expect("anonymous fixed and variable-length owners should bind");
    }
}

#[test]
fn strict_properties_reject_wrong_owner_and_ambiguity_with_exact_spans() {
    let (binder, _) = strict_property_binder(false);
    for (query, span) in [
        ("MATCH (host:Host) RETURN host.weight", "host.weight"),
        ("MATCH ()-[r:R]->() RETURN r.direct", "r.direct"),
        ("MATCH (host:Host) RETURN host.missing", "host.missing"),
        (
            "MATCH (host:Host) WITH host AS forwarded RETURN forwarded.weight",
            "forwarded.weight",
        ),
        (
            "MATCH ()-[r:R]->() WITH r AS forwarded RETURN forwarded.direct",
            "forwarded.direct",
        ),
    ] {
        property_error(&binder, query, BindErrorKind::UnknownProperty, span);
    }
    for (query, span) in [
        ("MATCH (:Host {weight: 7}) RETURN 1", "weight"),
        ("MATCH ()-[:R {missing: 7}]->() RETURN 1", "missing"),
    ] {
        property_error(&binder, query, BindErrorKind::UnknownProperty, span);
    }
    property_error(
        &binder,
        "MATCH ()-[*1..2 {weight: 7}]->() RETURN 1",
        BindErrorKind::AmbiguousProperty,
        "weight",
    );

    let (binder, _) = strict_property_binder(true);
    let query = "MATCH (host:Host) RETURN host.inherited";
    let error = property_error(
        &binder,
        query,
        BindErrorKind::AmbiguousProperty,
        "host.inherited",
    );
    assert!(error.message.contains("Asset, Host"));
    property_error(
        &binder,
        "MATCH (:Host {inherited: 7}) RETURN 1",
        BindErrorKind::AmbiguousProperty,
        "inherited",
    );
}

#[test]
fn strict_mode_rejects_mismatched_durable_identity_fields() {
    for query in [
        "MATCH (node) RETURN node.edge_uuid",
        "MATCH ()-[relationship]->() RETURN relationship.node_uuid",
    ] {
        let (binder, _) = make_binder(OntologyMode::Strict);
        let errors = binder
            .bind(&parse(query).unwrap())
            .expect_err("identity fields must match the bound entity kind");
        assert!(errors.iter().any(|error| {
            error.kind == BindErrorKind::InvalidArgument && error.message.contains("valid only on")
        }));
    }
}

#[test]
fn structural_identity_fields_are_read_only() {
    for query in [
        "MATCH (node) SET node.node_uuid = 'replacement'",
        "MATCH (node) REMOVE node.node_uuid",
        "MATCH ()-[relationship]->() SET relationship.edge_uuid = 'replacement'",
        "MATCH ()-[relationship]->() REMOVE relationship.edge_uuid",
    ] {
        let (binder, _) = make_binder(OntologyMode::Strict);
        let errors = binder
            .bind(&parse(query).unwrap())
            .expect_err("identity fields must not be mutable");
        assert!(errors.iter().any(|error| {
            error.kind == BindErrorKind::InvalidArgument && error.message.contains("is read-only")
        }));
    }
}

#[test]
fn undeclared_variable_in_return_is_error() {
    let (binder, _) = make_binder(OntologyMode::Exploratory);
    let ast = parse("MATCH (a:Person) RETURN x").unwrap();
    let result = binder.bind(&ast);
    assert!(result.is_err());
    let errors = result.unwrap_err();
    assert!(
        errors
            .iter()
            .any(|e| e.kind == BindErrorKind::UndeclaredVariable)
    );
}

#[test]
fn typed_uuid_parameters_are_identity_only_across_expression_surfaces() {
    let params = HashMap::from([("id".into(), IrLiteral::Uuid([0x55; 16]))]);
    for query in [
        "MATCH (n:Person) WHERE NOT (n.node_uuid = $id) RETURN n",
        "MATCH (n:Person) WHERE n.node_uuid <> $id RETURN n",
        "MATCH (n:Person) WHERE n.node_uuid > $id RETURN n",
        "MATCH (n:Person) WHERE n.node_uuid IN [$id] RETURN n",
        "RETURN $id",
        "RETURN toString($id)",
        "RETURN size([$id])",
        "RETURN [$id]",
        "RETURN 1 AS value SKIP $id",
        "RETURN 1 AS value LIMIT $id",
        "MATCH (n:Person) WHERE (($id = n.name) OR false) RETURN n",
        "MATCH (n:Person) WHERE n.name IN [$id] RETURN n",
        "MATCH (n:Person) RETURN n.name = $id AS bad",
        "MATCH (n:Person) WITH n, n.name = $id AS bad RETURN bad",
        "MATCH (n:Person) RETURN n.name AS name ORDER BY n.name = $id",
        "MATCH (n:Person) UNWIND [n.name = $id] AS bad RETURN bad",
        "MATCH (n:Person {probe: n.name = $id}) RETURN n",
        "MATCH (n:Person) SET n.probe = (n.name = $id) RETURN n",
        "MATCH (n:Person) MERGE (m:Other {probe: n.name = $id}) RETURN m",
        "MATCH (n:Person) DELETE (n.name = $id)",
        "MATCH (n:Person)-[r:KNOWS]->() WHERE r.node_uuid = $id RETURN r",
        "MATCH (n:Person)-[r:KNOWS]->() WHERE n.edge_uuid = $id RETURN n",
    ] {
        let (binder, _) = make_binder(OntologyMode::Exploratory);
        let errors = binder
            .with_parameter_literals(&params)
            .bind(&parse(query).unwrap())
            .expect_err("typed UUID must not reach a non-identity expression");
        assert!(
                errors.iter().any(|error| {
                    error.kind == BindErrorKind::InvalidArgument
                        && error.message
                            == "typed UUID parameter `$id` is only supported as a direct node_uuid or edge_uuid identity equality predicate"
                }),
                "query={query} errors={errors:?}"
            );
    }

    let errors = procedure_binder()
        .with_parameter_literals(&params)
        .bind(&parse("MATCH (n:Person) CALL test.proc(n.name = $id) YIELD out RETURN out").unwrap())
        .expect_err("CALL argument must enforce typed UUID identity semantics");
    assert!(
        errors.iter().any(|error| {
            error.kind == BindErrorKind::InvalidArgument
                && error.message.starts_with("typed UUID parameter `$id`")
        }),
        "errors={errors:?}"
    );
}

#[test]
fn typed_uuid_parameters_allow_only_kind_correct_identity_fields() {
    let params = HashMap::from([("id".into(), IrLiteral::Uuid([0x55; 16]))]);
    for query in [
        "MATCH (n:Person) WHERE n.node_uuid = $id RETURN n.node_uuid",
        "MATCH (n:Person) WHERE $id = n.node_uuid RETURN n.node_uuid",
        "MATCH ()-[r:KNOWS]->() WHERE r.edge_uuid = $id RETURN r.edge_uuid",
        "MATCH ()-[r:KNOWS]->() WHERE $id = r.edge_uuid RETURN r.edge_uuid",
    ] {
        let (binder, _) = make_binder(OntologyMode::Exploratory);
        binder
            .with_parameter_literals(&params)
            .bind(&parse(query).unwrap())
            .unwrap_or_else(|errors| panic!("query={query} errors={errors:?}"));
    }

    for nested in [
        IrLiteral::List(vec![IrLiteral::Uuid([0x55; 16])]),
        IrLiteral::Map(vec![(
            "nested".into(),
            IrLiteral::List(vec![IrLiteral::Uuid([0x55; 16])]),
        )]),
    ] {
        let (binder, _) = make_binder(OntologyMode::Exploratory);
        let errors = binder
            .with_parameter_literals(&HashMap::from([("id".into(), nested)]))
            .bind(&parse("MATCH (n:Person) WHERE n.node_uuid = $id RETURN n.node_uuid").unwrap())
            .expect_err("containers containing UUID values are never identity scalars");
        assert!(errors.iter().any(|error| {
            error.kind == BindErrorKind::InvalidArgument
                && error.message.starts_with("typed UUID parameter `$id`")
        }));
    }
}

#[test]
fn typed_uuid_discovery_traverses_every_expression_container() {
    let params = HashMap::from([("id".into(), IrLiteral::Uuid([0x55; 16]))]);
    let (binder, _) = make_binder(OntologyMode::Exploratory);
    let binder = binder.with_parameter_literals(&params);
    let cases = [
        "$id",
        "($id)",
        "-$id",
        "[$id]",
        "{value: $id}",
        "coalesce(null, $id)",
        "1 + $id",
        "CASE $id WHEN null THEN 0 ELSE 1 END",
        "CASE 1 WHEN $id THEN 0 ELSE 1 END",
        "CASE 1 WHEN 0 THEN $id ELSE 1 END",
        "CASE 1 WHEN 0 THEN 1 ELSE $id END",
        "[x IN [$id] WHERE x IS NOT NULL | x]",
        "[x IN [1] WHERE $id IS NOT NULL | x]",
        "[x IN [1] | $id]",
        "all(x IN [$id] WHERE x IS NOT NULL)",
        "all(x IN [1] WHERE $id IS NOT NULL)",
        "[(a)-->(b) WHERE $id IS NOT NULL | a]",
        "[(a)-->(b) | $id]",
        "$id IS NULL",
        "$id IN [1]",
        "1 IN [$id]",
        "$id STARTS WITH 'x'",
        "'x' STARTS WITH $id",
        "$id =~ 'x'",
        "'x' =~ $id",
    ];
    for source in cases {
        let expression = parsed_return_expr(&format!("RETURN {source}"));
        assert_eq!(
            binder.typed_uuid_param_in(&expression),
            Some("id"),
            "{source}"
        );
    }
    assert_eq!(
        binder.typed_uuid_param_in(&parsed_return_expr("RETURN [1, 2, 3]")),
        None
    );
}

#[test]
fn strict_without_ontology_rejects_property_resolution() {
    let (binder, _) = make_binder(OntologyMode::Strict);
    let ast = parse("MATCH (n:Person) RETURN n.unknown").unwrap();
    let errors = binder.bind(&ast).unwrap_err();
    assert!(errors.iter().any(|error| {
        error.kind == BindErrorKind::UnknownProperty
            && error.message == "unknown property `unknown` (strict mode has no ontology)"
    }));
}

#[test]
fn exact_zero_concat_ast_lowers_to_string_function() {
    let mut expression = parsed_return_expr("RETURN 'left' + 'right'");
    let Expr::BinaryOp(binary) = &mut expression else {
        panic!("expected binary expression")
    };
    binary.op = AstBinOp::Concat;
    let (binder, _) = make_binder(OntologyMode::Exploratory);
    let mut state = empty_state(OntologyMode::Exploratory);
    let id = binder.lower_expr(&expression, expression.span(), &mut state);
    assert!(state.errors.is_empty());
    let plan = state.builder.build();
    assert!(matches!(
        plan.exprs.get(id),
        IrExpr::FunctionCall { name, args } if name == "string.concat" && args.len() == 2
    ));
}

#[test]
fn exact_zero_strict_property_owner_matrix_resolves_declared_properties() {
    use graphforge_ontology::{
        EntityTypeDef, OntologyCompiler, OntologyDoc, OntologyHandle, PropertyDef,
        PropertyValueType, RelationTypeDef, SemanticFlags,
    };

    let doc = OntologyDoc {
        ontology_id: "property-owner-test".into(),
        version: "1.0".into(),
        entity_types: vec![EntityTypeDef {
            name: "Person".into(),
            r#abstract: false,
            parent: None,
        }],
        relation_types: vec![RelationTypeDef {
            name: "KNOWS".into(),
            src: "Person".into(),
            dst: "Person".into(),
            inverse: None,
            semantic: SemanticFlags::default(),
        }],
        properties: vec![
            PropertyDef {
                owner: "Person".into(),
                name: "name".into(),
                value_type: PropertyValueType::Utf8,
                nullable: true,
                multivalued: false,
                default_json: None,
            },
            PropertyDef {
                owner: "KNOWS".into(),
                name: "since".into(),
                value_type: PropertyValueType::Int64,
                nullable: true,
                multivalued: false,
                default_json: None,
            },
        ],
        constraints: vec![],
        migrations: vec![],
    };
    let handle = OntologyHandle::new(OntologyCompiler::compile(&doc).unwrap());
    for query in [
        "MATCH (n:Person) RETURN n.name",
        "MATCH (n) RETURN n.name",
        "MATCH ()-[r:KNOWS]->() RETURN r.since",
        "MATCH ()-[r]->() RETURN r.since",
    ] {
        let binder = Binder::new(
            Some(handle.clone()),
            Arc::new(Mutex::new(RuntimeCatalog::new())),
            OntologyMode::Strict,
        );
        binder
            .bind(&parse(query).unwrap())
            .unwrap_or_else(|errors| panic!("query={query}, errors={errors:?}"));
    }
}
