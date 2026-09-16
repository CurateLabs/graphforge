use super::super::*;
use datafusion::logical_expr::LogicalPlan as DfLogicalPlan;
use graphforge_core::TypeId;
use graphforge_ir::expr::{IrExpr, IrLiteral};
use graphforge_ir::{Direction, GraphPlan, VarId};

#[test]
fn statement_driver_only_write_forms_are_rejected_by_relational_lowering() {
    use graphforge_ir::{LabelItem, RemovePropItem, SetMapItem};

    let dir = tempfile::tempdir().unwrap();
    let lowerer = GraphPlanLowerer::new_for_writes(
        &graphforge_storage::lowering_snapshot(
            Some(
                &graphforge_storage::GraphCatalog::open(
                    dir.path(),
                    None,
                    &graphforge_ir::RuntimeCatalog::new(),
                )
                .unwrap(),
            ),
            Some(dir.path()),
        )
        .unwrap(),
        None,
        graphforge_core::OntologyMode::Exploratory,
    )
    .unwrap();

    let mut set_builder = GraphPlan::builder("openCypher");
    let map = set_builder.push_expr(IrExpr::MapLiteral(vec![]));
    let set_map = set_builder
        .push_op(GraphOp::Set {
            items: vec![],
            map_items: vec![SetMapItem {
                target: VarId(1),
                map,
                replace: false,
            }],
            label_items: vec![],
        })
        .build();
    assert!(
        lowerer
            .lower_plan(&set_map)
            .unwrap_err()
            .to_string()
            .contains("statement driver")
    );

    let set_labels = GraphPlan::builder("openCypher")
        .push_op(GraphOp::Set {
            items: vec![],
            map_items: vec![],
            label_items: vec![LabelItem {
                target: VarId(1),
                labels: vec![EntityTypeId::ontology(TypeId(7)).unwrap()],
            }],
        })
        .build();
    assert!(
        lowerer
            .lower_plan(&set_labels)
            .unwrap_err()
            .to_string()
            .contains("statement driver")
    );

    let remove_labels = GraphPlan::builder("openCypher")
        .push_op(GraphOp::Remove {
            items: Vec::<RemovePropItem>::new(),
            label_items: vec![LabelItem {
                target: VarId(1),
                labels: vec![EntityTypeId::ontology(TypeId(7)).unwrap()],
            }],
        })
        .build();
    assert!(
        lowerer
            .lower_plan(&remove_labels)
            .unwrap_err()
            .to_string()
            .contains("statement driver")
    );
}

fn create_plan_with_props() -> GraphPlan {
    use graphforge_ir::{CreateNodeSpec, CreatePattern};
    let mut builder = GraphPlan::builder("openCypher");
    // Property map {name: 'Alice'} in the arena.
    let name_lit = builder.push_expr(IrExpr::Literal(IrLiteral::Str("Alice".into())));
    let map = builder.push_expr(IrExpr::MapLiteral(vec![("name".into(), name_lit)]));
    builder
        .push_op(GraphOp::Create {
            pattern: CreatePattern {
                nodes: vec![CreateNodeSpec {
                    var: VarId(0),
                    labels: vec![EntityTypeId::ontology(TypeId(0)).unwrap()],
                    properties: Some(map),
                    is_reference: false,
                }],
                edges: vec![],
            },
        })
        .build()
}

#[test]
fn create_lowers_to_extension_with_write_target() {
    let dir = tempfile::TempDir::new().unwrap();
    let lowerer = GraphPlanLowerer::new_for_writes(
        &graphforge_storage::lowering_snapshot(
            Some(
                &graphforge_storage::GraphCatalog::open(
                    dir.path(),
                    None,
                    &graphforge_ir::RuntimeCatalog::new(),
                )
                .unwrap(),
            ),
            Some(dir.path()),
        )
        .unwrap(),
        None,
        graphforge_core::OntologyMode::Exploratory,
    )
    .unwrap();
    let plan = create_plan_with_props();
    let lp = lowerer.lower_plan(&plan).unwrap();
    assert!(
        matches!(lp, DfLogicalPlan::Extension(_)),
        "expected Extension (GraphCreateNode), got {lp:?}"
    );
}

#[test]
fn create_followed_by_read_emits_created_rows_for_real_pipeline() {
    use graphforge_ir::CreateNodeSpec;
    use graphforge_plan::GraphCreateNode;

    let dir = tempfile::TempDir::new().unwrap();
    let lowerer = GraphPlanLowerer::new_for_writes(
        &graphforge_storage::lowering_snapshot(
            Some(
                &graphforge_storage::GraphCatalog::open(
                    dir.path(),
                    None,
                    &graphforge_ir::RuntimeCatalog::new(),
                )
                .unwrap(),
            ),
            Some(dir.path()),
        )
        .unwrap(),
        None,
        graphforge_core::OntologyMode::Exploratory,
    )
    .unwrap();
    let mut builder = GraphPlan::builder("openCypher");
    let returned = builder.push_expr(IrExpr::VarRef(VarId(0)));
    let plan = builder
        .push_op(GraphOp::Create {
            pattern: CreatePattern {
                nodes: vec![CreateNodeSpec {
                    var: VarId(0),
                    labels: vec![],
                    properties: None,
                    is_reference: false,
                }],
                edges: vec![],
            },
        })
        .push_op(GraphOp::Project {
            items: vec![ProjectItem {
                expr: returned,
                alias: Some("created".into()),
                out_var: None,
            }],
            distinct: false,
        })
        .build();

    let lowered = lowerer.lower_plan(&plan).unwrap();
    let DfLogicalPlan::Projection(project) = lowered else {
        panic!("expected trailing projection");
    };
    let DfLogicalPlan::Extension(create) = project.input.as_ref() else {
        panic!("expected emitting CREATE input");
    };
    let create = create
        .node
        .as_any()
        .downcast_ref::<GraphCreateNode>()
        .expect("GraphCreateNode");
    assert!(
        !datafusion::logical_expr::UserDefinedLogicalNodeCore::schema(create)
            .fields()
            .is_empty()
    );
}

#[test]
fn create_without_write_target_errors() {
    // The read-only `new` constructor has no write target.
    let lowerer = GraphPlanLowerer::new(None, None).unwrap();
    let plan = create_plan_with_props();
    let result = lowerer.lower_plan(&plan);
    assert!(
        result.is_err(),
        "CREATE without a write target should error"
    );
}

#[test]
fn new_for_reads_does_not_authorize_writes() {
    // `new_for_reads` grants read-side directory access (for var-length
    // Expand) but must NOT open the write path — only `new_for_writes`
    // authorizes CREATE.
    let dir = tempfile::TempDir::new().unwrap();
    let lowerer = GraphPlanLowerer::new_for_reads(
        &graphforge_storage::lowering_snapshot(
            Some(
                &graphforge_storage::GraphCatalog::open(
                    dir.path(),
                    None,
                    &graphforge_ir::RuntimeCatalog::new(),
                )
                .unwrap(),
            ),
            Some(dir.path()),
        )
        .unwrap(),
        None,
        graphforge_core::OntologyMode::Exploratory,
    )
    .unwrap();
    let result = lowerer.lower_plan(&create_plan_with_props());
    assert!(
        result.is_err(),
        "new_for_reads must not authorize CREATE; only new_for_writes does"
    );
}

#[test]
fn create_non_literal_property_lowers_as_computed() {
    use graphforge_ir::{CreateNodeSpec, CreatePattern};
    let dir = tempfile::TempDir::new().unwrap();
    let lowerer = GraphPlanLowerer::new_for_writes(
        &graphforge_storage::lowering_snapshot(
            Some(
                &graphforge_storage::GraphCatalog::open(
                    dir.path(),
                    None,
                    &graphforge_ir::RuntimeCatalog::new(),
                )
                .unwrap(),
            ),
            Some(dir.path()),
        )
        .unwrap(),
        None,
        graphforge_core::OntologyMode::Exploratory,
    )
    .unwrap();
    let mut builder = GraphPlan::builder("openCypher");
    // A non-literal property value (here a parameter) is no longer rejected
    // (#814): it lowers to a row-dependent computed `Expr` on the create node,
    // evaluated per row by the execution layer.
    let param = builder.push_expr(IrExpr::Parameter("p".into()));
    let map = builder.push_expr(IrExpr::MapLiteral(vec![("name".into(), param)]));
    let plan = builder
        .push_op(GraphOp::Create {
            pattern: CreatePattern {
                nodes: vec![CreateNodeSpec {
                    var: VarId(0),
                    labels: vec![],
                    properties: Some(map),
                    is_reference: false,
                }],
                edges: vec![],
            },
        })
        .build();
    let logical = lowerer
        .lower_plan(&plan)
        .expect("a non-literal CREATE property lowers to a computed expr");
    let datafusion::logical_expr::LogicalPlan::Extension(ext) = &logical else {
        panic!("CREATE lowers to an Extension node");
    };
    let create = ext
        .node
        .as_any()
        .downcast_ref::<graphforge_plan::GraphCreateNode>()
        .expect("a GraphCreateNode");
    assert!(
        create.nodes[0].properties.is_empty(),
        "the parameter value is not a baked literal"
    );
    assert_eq!(
        create.nodes[0].computed_properties.len(),
        1,
        "the parameter value is a row-dependent computed property"
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn created_rows_schema_preserves_input_skips_references_and_types_minted_nodes() {
    use std::collections::HashMap;

    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::common::{DFSchema, TableReference};
    use datafusion::logical_expr::{col, lit};
    use graphforge_plan::ResolvedNodeSpec;

    let input = Arc::new(
        DFSchema::new_with_metadata(
            vec![(
                Some(TableReference::bare("input")),
                Arc::new(Field::new("seed", DataType::Int64, false)),
            )],
            HashMap::new(),
        )
        .unwrap(),
    );
    let reference = ResolvedNodeSpec {
        var: 1,
        label_ids: vec![EntityTypeId::decode(7).unwrap()],
        label_names: vec!["Existing".into()],
        properties: vec![("ignored".into(), IrLiteral::Int(1))],
        computed_properties: vec![],
        is_reference: true,
    };
    let minted = ResolvedNodeSpec {
        var: 2,
        label_ids: vec![
            EntityTypeId::decode(8).unwrap(),
            EntityTypeId::decode(9).unwrap(),
        ],
        label_names: vec!["New".into(), "Tagged".into()],
        properties: vec![("active".into(), IrLiteral::Bool(true))],
        computed_properties: vec![("copied_seed".into(), col("seed") + lit(1_i64))],
        is_reference: false,
    };

    let schema = GraphPlanLowerer::created_rows_schema(&[reference, minted], &input).unwrap();
    let fields: Vec<_> = schema
        .iter()
        .map(|(qualifier, field)| {
            (
                qualifier.map(ToString::to_string),
                field.name().clone(),
                field.data_type().clone(),
                field.is_nullable(),
            )
        })
        .collect();

    assert_eq!(
        fields.len(),
        7,
        "one input plus four identity and two property fields"
    );
    assert_eq!(
        fields[0],
        (Some("input".into()), "seed".into(), DataType::Int64, false)
    );
    assert_eq!(
        fields[1..5]
            .iter()
            .map(|(q, name, ty, nullable)| { (q.clone(), name.clone(), ty.clone(), *nullable) })
            .collect::<Vec<_>>(),
        vec![
            (
                Some("var_2".into()),
                "node_uuid".into(),
                DataType::FixedSizeBinary(16),
                false
            ),
            (
                Some("var_2".into()),
                "node_id".into(),
                DataType::UInt64,
                false,
            ),
            (
                Some("var_2".into()),
                "type_id".into(),
                DataType::UInt32,
                false,
            ),
            (
                Some("var_2".into()),
                "type_ids".into(),
                DataType::List(Arc::new(Field::new("item", DataType::UInt32, false))),
                false,
            ),
        ]
    );
    assert_eq!(
        fields[5],
        (
            Some("var_2".into()),
            "active".into(),
            DataType::Boolean,
            true
        )
    );
    assert_eq!(
        fields[6],
        (
            Some("var_2".into()),
            "copied_seed".into(),
            DataType::Int64,
            true,
        )
    );
    assert!(
        fields
            .iter()
            .all(|(q, _, _, _)| q.as_deref() != Some("var_1")),
        "reference nodes are passed through only and never duplicated"
    );
}

#[test]
fn created_rows_schema_rejects_reserved_and_unbound_computed_properties() {
    use graphforge_plan::ResolvedNodeSpec;

    let input = Arc::new(datafusion::common::DFSchema::empty());
    for reserved in ["node_uuid", "node_id", "type_id", "type_ids"] {
        let spec = ResolvedNodeSpec {
            var: 3,
            label_ids: vec![],
            label_names: vec![],
            properties: vec![(reserved.into(), IrLiteral::Null)],
            computed_properties: vec![],
            is_reference: false,
        };
        let error = GraphPlanLowerer::created_rows_schema(&[spec], &input).unwrap_err();
        assert_eq!(
            error.to_string(),
            format!(
                "unsupported expression: CREATE property `{reserved}` collides with a reserved node topology field"
            )
        );
    }

    let unbound = ResolvedNodeSpec {
        var: 4,
        label_ids: vec![],
        label_names: vec![],
        properties: vec![],
        computed_properties: vec![("value".into(), datafusion::logical_expr::col("missing"))],
        is_reference: false,
    };
    let error = GraphPlanLowerer::created_rows_schema(&[unbound], &input).unwrap_err();
    assert!(
        error.to_string().contains("No field named missing"),
        "unbound computed properties must retain the DataFusion schema error: {error}"
    );
}

/// `MATCH (n) DELETE n` over a real read dir, so the NodeScan binds
/// `var_0.node_uuid` for the delete-target kind resolution.
fn match_delete_plan(detach: bool) -> GraphPlan {
    GraphPlan::builder("openCypher")
        .push_op(GraphOp::NodeScan {
            var: VarId(0),
            ty: None,
        })
        .push_op(GraphOp::Delete {
            vars: vec![VarId(0)],
            exprs: vec![],
            detach,
        })
        .build()
}

#[test]
fn delete_lowers_to_extension_with_write_target() {
    let dir = tempfile::TempDir::new().unwrap();
    let rc = graphforge_ir::RuntimeCatalog::new();
    let catalog = graphforge_storage::GraphCatalog::open(dir.path(), None, &rc).unwrap();
    let lowerer = GraphPlanLowerer::new_for_writes(
        &graphforge_storage::lowering_snapshot(Some(&catalog), Some(dir.path())).unwrap(),
        None,
        graphforge_core::OntologyMode::Exploratory,
    )
    .unwrap();
    let lp = lowerer.lower_plan(&match_delete_plan(false)).unwrap();
    assert!(
        matches!(lp, DfLogicalPlan::Extension(_)),
        "expected Extension (GraphDeleteNode), got {lp:?}"
    );
}

#[test]
fn delete_without_write_target_errors() {
    let dir = tempfile::TempDir::new().unwrap();
    let rc = graphforge_ir::RuntimeCatalog::new();
    let catalog = graphforge_storage::GraphCatalog::open(dir.path(), None, &rc).unwrap();
    // `new_for_reads` grants read access but not the write path.
    let lowerer = GraphPlanLowerer::new_for_reads(
        &graphforge_storage::lowering_snapshot(Some(&catalog), Some(dir.path())).unwrap(),
        None,
        graphforge_core::OntologyMode::Exploratory,
    )
    .unwrap();
    assert!(
        lowerer.lower_plan(&match_delete_plan(false)).is_err(),
        "DELETE without a write target should error"
    );
}

#[test]
fn delete_edge_variable_resolves_is_edge_flag() {
    use graphforge_plan::GraphDeleteNode;

    // `MATCH ()-[r]->() DELETE r`: the edge var must resolve to a DeleteTarget
    // with is_edge=true (the input schema carries var_1.edge_uuid, not
    // node_uuid), so the executor reads the edge identity column.
    let dir = tempfile::TempDir::new().unwrap();
    let rc = graphforge_ir::RuntimeCatalog::new();
    let catalog = graphforge_storage::GraphCatalog::open(dir.path(), None, &rc).unwrap();
    let lowerer = GraphPlanLowerer::new_for_writes(
        &graphforge_storage::lowering_snapshot(Some(&catalog), Some(dir.path())).unwrap(),
        None,
        graphforge_core::OntologyMode::Exploratory,
    )
    .unwrap();
    let plan = GraphPlan::builder("openCypher")
        .push_op(GraphOp::NodeScan {
            var: VarId(0),
            ty: None,
        })
        .push_op(GraphOp::Expand {
            src: VarId(0),
            edge: VarId(1),
            dst: VarId(2),
            rel_ty: None,
            dir: Direction::Out,
            min_hops: 1,
            max_hops: Some(1),
        })
        .push_op(GraphOp::Delete {
            vars: vec![VarId(1)],
            exprs: vec![],
            detach: false,
        })
        .build();

    let lp = lowerer.lower_plan(&plan).unwrap();
    let DfLogicalPlan::Extension(ext) = &lp else {
        panic!("expected Extension (GraphDeleteNode), got {lp:?}");
    };
    let node = ext
        .node
        .as_any()
        .downcast_ref::<GraphDeleteNode>()
        .expect("GraphDeleteNode");
    assert_eq!(node.targets.len(), 1, "one delete target");
    assert_eq!(node.targets[0].var, 1);
    assert!(
        node.targets[0].is_edge,
        "the edge var must resolve to is_edge=true"
    );
}

#[test]
fn write_kind_resolution_covers_node_typed_edge_and_invalid_targets() {
    use std::collections::HashMap;

    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::common::{DFSchema, TableReference};

    let schema = |names: &[&str]| {
        Arc::new(
            DFSchema::new_with_metadata(
                names
                    .iter()
                    .map(|name| {
                        (
                            Some(TableReference::bare("var_7")),
                            Arc::new(Field::new(*name, DataType::Utf8, true)),
                        )
                    })
                    .collect(),
                HashMap::new(),
            )
            .unwrap(),
        )
    };

    assert!(
        !GraphPlanLowerer::resolve_write_kind(&schema(&["node_uuid"]), VarId(7), "SET").unwrap()
    );
    assert!(
        GraphPlanLowerer::resolve_write_kind(
            &schema(&["edge_uuid", "rel_type_name"]),
            VarId(7),
            "REMOVE"
        )
        .unwrap()
    );

    let untyped_edge =
        GraphPlanLowerer::resolve_write_kind(&schema(&["edge_uuid"]), VarId(7), "SET").unwrap_err();
    assert!(untyped_edge.to_string().contains("known relation type"));

    let unbound =
        GraphPlanLowerer::resolve_write_kind(&schema(&[]), VarId(7), "REMOVE").unwrap_err();
    assert!(unbound.to_string().contains("must be bound"));

    // A malformed schema carrying both identities resolves as a node, the
    // same precedence used by DELETE target classification.
    assert!(
        !GraphPlanLowerer::resolve_write_kind(
            &schema(&["node_uuid", "edge_uuid", "rel_type_name"]),
            VarId(7),
            "SET"
        )
        .unwrap()
    );
}

fn writes_lowerer<'a>(
    catalog: &'a graphforge_storage::GraphCatalog,
    dir: &'a std::path::Path,
) -> GraphPlanLowerer {
    GraphPlanLowerer::new_for_writes(
        &graphforge_storage::lowering_snapshot(Some(catalog), Some(dir)).unwrap(),
        None,
        graphforge_core::OntologyMode::Exploratory,
    )
    .unwrap()
}

#[test]
fn set_literal_value_lowers_to_extension() {
    use graphforge_ir::SetPropItem;
    let dir = tempfile::TempDir::new().unwrap();
    let rc = graphforge_ir::RuntimeCatalog::new();
    let catalog = graphforge_storage::GraphCatalog::open(dir.path(), None, &rc).unwrap();
    let lowerer = writes_lowerer(&catalog, dir.path());

    let mut builder = GraphPlan::builder("openCypher");
    let value = builder.push_expr(IrExpr::Literal(IrLiteral::Int(42)));
    let plan = builder
        .push_op(GraphOp::NodeScan {
            var: VarId(0),
            ty: None,
        })
        .push_op(GraphOp::Set {
            items: vec![SetPropItem {
                target: VarId(0),
                prop: PropertyId::ontology(graphforge_core::PropId(0)).unwrap(),
                prop_name: "age".into(),
                value,
            }],
            map_items: vec![],
            label_items: vec![],
        })
        .build();
    let lp = lowerer.lower_plan(&plan).unwrap();
    let DfLogicalPlan::Extension(ext) = &lp else {
        panic!("expected Extension (GraphSetNode), got {lp:?}");
    };
    let node = ext.node.as_any().downcast_ref::<GraphSetNode>().unwrap();
    assert_eq!(node.targets.len(), 1);
    assert_eq!(node.targets[0].prop_name, "age");
    assert!(!node.targets[0].is_edge, "node target");
}

#[test]
fn set_runtime_expr_value_lowers_against_input_schema() {
    // `SET n.age = n.age + 1` — the value references the matched-row column
    // `var_0.age`; it must lower without error (the topology scan carries the
    // bound var, and the value expr resolves against the input schema).
    use graphforge_ir::SetPropItem;
    let dir = tempfile::TempDir::new().unwrap();
    let rc = graphforge_ir::RuntimeCatalog::new();
    let catalog = graphforge_storage::GraphCatalog::open(dir.path(), None, &rc).unwrap();
    let lowerer = writes_lowerer(&catalog, dir.path());

    let mut builder = GraphPlan::builder("openCypher");
    // n.age + 1
    let var = builder.push_expr(IrExpr::VarRef(VarId(0)));
    let age = builder.push_expr(IrExpr::PropertyAccess {
        base: var,
        prop: PropertyId::ontology(graphforge_core::PropId(0)).unwrap(),
    });
    let one = builder.push_expr(IrExpr::Literal(IrLiteral::Int(1)));
    let sum = builder.push_expr(IrExpr::BinaryOp {
        op: graphforge_ir::expr::BinaryOpKind::Add,
        left: age,
        right: one,
    });
    let plan = builder
        .push_op(GraphOp::NodeScan {
            var: VarId(0),
            ty: None,
        })
        .push_op(GraphOp::Set {
            items: vec![SetPropItem {
                target: VarId(0),
                prop: PropertyId::ontology(graphforge_core::PropId(0)).unwrap(),
                prop_name: "age".into(),
                value: sum,
            }],
            map_items: vec![],
            label_items: vec![],
        })
        .build();
    // The runtime value expr is carried onto the SET node, not collapsed.
    let lp = lowerer.lower_plan(&plan).unwrap();
    assert!(matches!(lp, DfLogicalPlan::Extension(_)));
}

#[test]
fn remove_lowers_to_extension() {
    use graphforge_ir::RemovePropItem;
    let dir = tempfile::TempDir::new().unwrap();
    let rc = graphforge_ir::RuntimeCatalog::new();
    let catalog = graphforge_storage::GraphCatalog::open(dir.path(), None, &rc).unwrap();
    let lowerer = writes_lowerer(&catalog, dir.path());

    let plan = GraphPlan::builder("openCypher")
        .push_op(GraphOp::NodeScan {
            var: VarId(0),
            ty: None,
        })
        .push_op(GraphOp::Remove {
            items: vec![RemovePropItem {
                target: VarId(0),
                prop: PropertyId::ontology(graphforge_core::PropId(0)).unwrap(),
                prop_name: "age".into(),
            }],
            label_items: vec![],
        })
        .build();
    let lp = lowerer.lower_plan(&plan).unwrap();
    let DfLogicalPlan::Extension(ext) = &lp else {
        panic!("expected Extension (GraphRemoveNode), got {lp:?}");
    };
    let node = ext.node.as_any().downcast_ref::<GraphRemoveNode>().unwrap();
    assert_eq!(node.targets.len(), 1);
    assert_eq!(node.targets[0].prop_name, "age");
}

#[test]
fn set_without_write_target_errors() {
    use graphforge_ir::SetPropItem;
    let dir = tempfile::TempDir::new().unwrap();
    let rc = graphforge_ir::RuntimeCatalog::new();
    let catalog = graphforge_storage::GraphCatalog::open(dir.path(), None, &rc).unwrap();
    // Read-side dir access only — no write authorization.
    let lowerer = GraphPlanLowerer::new_for_reads(
        &graphforge_storage::lowering_snapshot(Some(&catalog), Some(dir.path())).unwrap(),
        None,
        graphforge_core::OntologyMode::Exploratory,
    )
    .unwrap();
    let mut builder = GraphPlan::builder("openCypher");
    let value = builder.push_expr(IrExpr::Literal(IrLiteral::Int(1)));
    let plan = builder
        .push_op(GraphOp::NodeScan {
            var: VarId(0),
            ty: None,
        })
        .push_op(GraphOp::Set {
            items: vec![SetPropItem {
                target: VarId(0),
                prop: PropertyId::ontology(graphforge_core::PropId(0)).unwrap(),
                prop_name: "age".into(),
                value,
            }],
            map_items: vec![],
            label_items: vec![],
        })
        .build();
    assert!(
        lowerer.lower_plan(&plan).is_err(),
        "SET without a write target should error"
    );
}
