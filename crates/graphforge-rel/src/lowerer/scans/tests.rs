use super::super::tests::make_catalog_and_lowerer;
use super::super::*;
use datafusion::logical_expr::LogicalPlan as DfLogicalPlan;
use graphforge_core::TypeId;

use graphforge_ir::{GraphPlan, VarId};

#[test]
fn node_scan_no_type_produces_table_scan() {
    let (_dir, catalog, _rc) = make_catalog_and_lowerer();
    let lowerer = GraphPlanLowerer::new(
        Some(&graphforge_storage::lowering_snapshot(Some(&catalog), None).unwrap()),
        None,
    )
    .unwrap();

    let plan = GraphPlan::builder("openCypher")
        .push_op(GraphOp::NodeScan {
            var: VarId(0),
            ty: None,
        })
        .build();
    let lp = lowerer.lower_plan(&plan).unwrap();
    assert!(
        matches!(lp, DfLogicalPlan::TableScan(_)),
        "expected TableScan, got {lp:?}"
    );
}

#[test]
fn node_scan_with_type_produces_filter_over_scan() {
    let (_dir, catalog, _rc) = make_catalog_and_lowerer();
    let lowerer = GraphPlanLowerer::new(
        Some(&graphforge_storage::lowering_snapshot(Some(&catalog), None).unwrap()),
        None,
    )
    .unwrap();

    let plan = GraphPlan::builder("openCypher")
        .push_op(GraphOp::NodeScan {
            var: VarId(0),
            ty: Some(EntityTypeId::ontology(TypeId(1)).unwrap()),
        })
        .build();
    let lp = lowerer.lower_plan(&plan).unwrap();
    assert!(
        matches!(lp, DfLogicalPlan::Filter(_)),
        "expected Filter over TableScan, got {lp:?}"
    );
}

#[test]
fn typed_edge_scan_unknown_type_id_returns_error() {
    // TypeId(42) is not in the type_id_to_rel_name map (no ontology),
    // so lower_plan must return an error rather than silently falling back.
    let lowerer = GraphPlanLowerer::new(None, None).unwrap();

    let plan = GraphPlan::builder("openCypher")
        .push_op(GraphOp::TypedEdgeScan {
            var: VarId(0),
            rel_ty: RelationTypeId::ontology(TypeId(42)).unwrap(),
        })
        .build();
    let result = lowerer.lower_plan(&plan);
    assert!(
        result.is_err(),
        "unknown TypeId should return an error, not silently fall back"
    );
}

#[test]
fn edge_scan_wildcard_produces_table_scan() {
    let lowerer = GraphPlanLowerer::new(None, None).unwrap();

    let plan = GraphPlan::builder("openCypher")
        .push_op(GraphOp::EdgeScan {
            var: VarId(0),
            ty: None,
        })
        .build();
    let lp = lowerer.lower_plan(&plan).unwrap();
    assert!(
        matches!(lp, DfLogicalPlan::TableScan(_)),
        "expected TableScan, got {lp:?}"
    );
}

/// A snapshot captured for a value-free plan names `_untyped` by key alone.
fn omitted_property_snapshot() -> LoweringSnapshot {
    let name = PropertyId::runtime(graphforge_value::RuntimePropId::new(1).unwrap());
    LoweringSnapshot {
        property_names: HashMap::from([(name, "name".to_owned())]),
        node_schema: Some(TOPOLOGY_NODES_SCHEMA.clone()),
        node_properties: std::collections::BTreeMap::from([(
            "_untyped".to_owned(),
            graphforge_ir::arrow_schema::PROPERTY_BASE_SCHEMA.clone(),
        )]),
        property_schemas_omitted: true,
        ..LoweringSnapshot::default()
    }
}

fn scanned_tables(plan: &DfLogicalPlan, tables: &mut Vec<graphforge_plan::GraphReadTable>) {
    if let DfLogicalPlan::TableScan(scan) = plan
        && let Some(source) = scan
            .source
            .downcast_ref::<graphforge_plan::GraphReadSource>()
    {
        tables.push(source.table.clone());
    }
    for input in plan.inputs() {
        scanned_tables(input, tables);
    }
}

#[test]
fn omitted_property_schemas_join_routes_by_key_and_refuse_value_reads() {
    let snapshot = omitted_property_snapshot();
    let lowerer =
        GraphPlanLowerer::new_for_reads(&snapshot, None, OntologyMode::Exploratory).unwrap();
    let plan_returning = |value: fn(&mut ExprArena) -> ExprId| {
        let mut exprs = ExprArena::new();
        let expr = value(&mut exprs);
        let mut plan = GraphPlan::builder("openCypher")
            .push_op(GraphOp::NodeScan {
                var: VarId(0),
                ty: None,
            })
            .build();
        plan.ops.push(GraphOp::Project {
            items: vec![graphforge_ir::ProjectItem {
                expr,
                alias: Some("v".into()),
                out_var: None,
            }],
            distinct: false,
        });
        plan.exprs = exprs;
        plan
    };

    // A value-free plan keeps its route join, by key and without admission.
    let lowered = lowerer
        .lower_plan(&plan_returning(|exprs| {
            exprs.push(IrExpr::Literal(graphforge_ir::IrLiteral::Int(1)))
        }))
        .unwrap();
    let mut tables = Vec::new();
    scanned_tables(&lowered, &mut tables);
    assert_eq!(
        tables,
        vec![
            graphforge_plan::GraphReadTable::Nodes,
            graphforge_plan::GraphReadTable::PropertyKeys("_untyped".into()),
        ]
    );

    // Reading a value, a whole node or its keys cannot compile against a
    // snapshot that never captured the values' schema.
    for value in [
        (|exprs: &mut ExprArena| {
            let base = exprs.push(IrExpr::VarRef(VarId(0)));
            exprs.push(IrExpr::PropertyAccess {
                base,
                prop: PropertyId::runtime(graphforge_value::RuntimePropId::new(1).unwrap()),
            })
        }) as fn(&mut ExprArena) -> ExprId,
        |exprs| exprs.push(IrExpr::VarRef(VarId(0))),
        |exprs| {
            let base = exprs.push(IrExpr::VarRef(VarId(0)));
            exprs.push(IrExpr::FunctionCall {
                name: "keys".into(),
                args: vec![base],
            })
        },
    ] {
        let plan = plan_returning(value);
        let error = lowerer.lower_plan(&plan).unwrap_err();
        assert!(
            matches!(&error, GfError::Plan(message)
                if message == "lowering snapshot omits the property schemas this plan reads"),
            "{error:?}"
        );
    }
}
