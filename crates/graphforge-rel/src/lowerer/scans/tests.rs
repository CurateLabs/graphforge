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
