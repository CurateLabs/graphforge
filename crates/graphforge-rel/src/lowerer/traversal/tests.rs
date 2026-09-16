use super::super::tests::make_catalog_and_lowerer;
use super::super::*;
use datafusion::logical_expr::LogicalPlan as DfLogicalPlan;
use graphforge_core::TypeId;

use graphforge_ir::{Direction, GraphPlan, VarId};

fn var_len_plan() -> GraphPlan {
    GraphPlan::builder("openCypher")
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
            max_hops: Some(3), // variable-length — emits VarLenExpandNode
        })
        .build()
}

#[test]
fn expand_var_len_produces_extension_node() {
    use datafusion::logical_expr::UserDefinedLogicalNodeCore;
    use graphforge_plan::VarLenExpandNode;

    let (dir, catalog, _rc) = make_catalog_and_lowerer();
    let lowerer = GraphPlanLowerer::new_for_reads(
        &graphforge_storage::lowering_snapshot(Some(&catalog), Some(dir.path())).unwrap(),
        None,
        OntologyMode::Strict,
    )
    .unwrap();

    let lp = lowerer.lower_plan(&var_len_plan()).unwrap();
    let DfLogicalPlan::Extension(ext) = &lp else {
        panic!("expected Extension (VarLenExpandNode), got {lp:?}");
    };
    let node = ext
        .node
        .as_any()
        .downcast_ref::<VarLenExpandNode>()
        .expect("VarLenExpandNode");

    // Logical pattern fields and the semantic binding contract.
    assert_eq!(node.src_var, 0);
    assert_eq!(node.dst_var, 2);
    assert_eq!(node.direction, Direction::Out);
    assert_eq!(node.min_hops, 1);
    assert_eq!(node.max_hops, Some(3));
    assert_eq!(node.read_contract.as_ref(), Some(&lowerer.read_contract()));
    assert!(!format!("{node:?}").contains(&dir.path().display().to_string()));

    // Output schema carries the destination node's columns, qualified
    // `var_2`, so a downstream `RETURN b.node_id` can resolve them.
    let schema = UserDefinedLogicalNodeCore::schema(node);
    let dst = datafusion::common::TableReference::bare("var_2");
    assert!(schema.field_with_qualified_name(&dst, "node_id").is_ok());
}

#[test]
fn expand_var_len_without_dataset_snapshot_errors() {
    // Schema-only lowering has no dataset snapshot for edge reads.
    let (_dir, catalog, _rc) = make_catalog_and_lowerer();
    let lowerer = GraphPlanLowerer::new(
        Some(&graphforge_storage::lowering_snapshot(Some(&catalog), None).unwrap()),
        None,
    )
    .unwrap();
    let err = lowerer.lower_plan(&var_len_plan()).unwrap_err();
    assert!(
        err.to_string()
            .contains("variable-length expand requires a dataset snapshot"),
        "expected a missing dataset snapshot error, got: {err}"
    );
}

#[test]
fn expand_single_hop_out_produces_join() {
    let lowerer = GraphPlanLowerer::new(None, None).unwrap();

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
        .build();
    let lp = lowerer.lower_plan(&plan).unwrap();
    // Top-level should be an inner join (dst scan joined onto edge scan)
    assert!(
        matches!(lp, DfLogicalPlan::Join(_)),
        "expected Join, got {lp:?}"
    );
}

#[test]
fn fixed_hop_dst_label_is_preserved_as_filter() {
    // A binder emits `NodeScan{a} → Expand → NodeScan{b, ty}` for
    // `(a)-[:R]->(b:Label)`. The trailing `NodeScan{b}` is a no-op (b is
    // already bound by Expand), but b's label must still filter the result
    // rather than being dropped (#718). The optimizer leaves a `Filter` on
    // `var_2.type_id` somewhere in the tree.
    let lowerer = GraphPlanLowerer::new(None, None).unwrap();
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
        .push_op(GraphOp::NodeScan {
            var: VarId(2),
            ty: Some(EntityTypeId::ontology(TypeId(7)).unwrap()),
        })
        .build();
    let lp = lowerer.lower_plan(&plan).unwrap();
    let rendered = lp.display_indent_schema().to_string();
    assert!(
        rendered.contains("array_has(var_2.type_ids, UInt32(7))"),
        "destination label filter must be applied, got:\n{rendered}"
    );
}

/// Single-hop typed plan over an interned KNOWS relation; returns the
/// fixture pieces plus the relation TypeId.
fn typed_single_hop_fixture(
    dir: Direction,
) -> (
    tempfile::TempDir,
    graphforge_storage::GraphCatalog,
    GraphPlan,
) {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut rc = graphforge_ir::RuntimeCatalog::new();
    let rel = RelationTypeId::runtime(rc.intern_relation_type("KNOWS").unwrap());
    let catalog = graphforge_storage::GraphCatalog::open(tmp.path(), None, &rc).unwrap();
    let plan = GraphPlan::builder("openCypher")
        .push_op(GraphOp::NodeScan {
            var: VarId(0),
            ty: None,
        })
        .push_op(GraphOp::Expand {
            src: VarId(0),
            edge: VarId(1),
            dst: VarId(2),
            rel_ty: Some(rel),
            dir,
            min_hops: 1,
            max_hops: Some(1),
        })
        .build();
    (tmp, catalog, plan)
}

fn assert_typed_route_projection(plan: &DfLogicalPlan) -> &Extension {
    let DfLogicalPlan::Projection(projection) = plan else {
        panic!("expected typed route projection, got {plan:?}");
    };
    let mut expected = projection
        .input
        .schema()
        .columns()
        .into_iter()
        .map(DfExpr::Column)
        .collect::<Vec<_>>();
    expected.push(
        datafusion::logical_expr::lit("KNOWS").alias_qualified(Some("var_1"), "rel_type_name"),
    );
    assert_eq!(
        projection.expr, expected,
        "preserve all input columns and exact route"
    );
    let DfLogicalPlan::Extension(extension) = projection.input.as_ref() else {
        panic!("expected ExpandNode directly beneath route projection, got {plan:?}");
    };
    extension
}

#[test]
fn project_backed_single_hop_emits_expand_extension_node() {
    use datafusion::logical_expr::UserDefinedLogicalNodeCore;

    let (tmp, catalog, plan) = typed_single_hop_fixture(Direction::Out);
    let lowerer = GraphPlanLowerer::new_for_reads(
        &graphforge_storage::lowering_snapshot(Some(&catalog), Some(tmp.path())).unwrap(),
        None,
        OntologyMode::Strict,
    )
    .unwrap();

    let lp = lowerer.lower_plan(&plan).unwrap();
    let ext = assert_typed_route_projection(&lp);
    let node = ext
        .node
        .as_any()
        .downcast_ref::<graphforge_plan::ExpandNode>()
        .expect("ExpandNode");
    assert_eq!(node.rel_type_name, "KNOWS");
    assert_eq!(node.src_var, 0);
    assert_eq!(node.edge_var, 1);
    assert_eq!(node.dst_var, 2);
    assert_eq!(node.direction, Direction::Out);
    assert_eq!(node.read_contract.as_ref(), Some(&lowerer.read_contract()));
    assert!(
        node.read_contract
            .as_ref()
            .unwrap()
            .relations
            .iter()
            .any(|(_, name)| name == "KNOWS")
    );
    assert!(!format!("{node:?}").contains(&tmp.path().display().to_string()));
    assert_eq!(node.edge_prop_count, 0, "no edge_properties file on disk");

    // Schema parity essentials: edge topology under var_1, dst under var_2.
    let schema = UserDefinedLogicalNodeCore::schema(node);
    let edge = datafusion::common::TableReference::bare("var_1");
    let dst = datafusion::common::TableReference::bare("var_2");
    for name in ["edge_uuid", "src_id", "dst_id", "edge_id"] {
        assert!(
            schema.field_with_qualified_name(&edge, name).is_ok(),
            "{name}"
        );
    }
    assert!(schema.field_with_qualified_name(&dst, "node_id").is_ok());
}

#[test]
fn project_backed_undirected_single_hop_emits_plain_extension() {
    let (tmp, catalog, plan) = typed_single_hop_fixture(Direction::Undirected);
    let lowerer = GraphPlanLowerer::new_for_reads(
        &graphforge_storage::lowering_snapshot(Some(&catalog), Some(tmp.path())).unwrap(),
        None,
        OntologyMode::Strict,
    )
    .unwrap();

    let lp = lowerer.lower_plan(&plan).unwrap();
    // No DISTINCT wrapper: the self-loop dedup happens inside ExpandExec
    // (a wrapping Distinct trips DataFusion's duplicate-field-name
    // disambiguation against the extension's multi-var schema).
    let ext = assert_typed_route_projection(&lp);
    let node = ext
        .node
        .as_any()
        .downcast_ref::<graphforge_plan::ExpandNode>()
        .expect("ExpandNode");
    assert_eq!(node.direction, Direction::Undirected);
}

#[test]
fn schema_only_single_hop_keeps_join_path() {
    let (_tmp, catalog, plan) = typed_single_hop_fixture(Direction::Out);
    let lowerer = GraphPlanLowerer::new(
        Some(&graphforge_storage::lowering_snapshot(Some(&catalog), None).unwrap()),
        None,
    )
    .unwrap();
    let lp = lowerer.lower_plan(&plan).unwrap();
    assert!(
        matches!(lp, DfLogicalPlan::Join(_)),
        "expected Join chain, got {lp:?}"
    );
}

#[test]
fn exploratory_mode_emits_expand_with_dynamic_edge_schema() {
    use datafusion::logical_expr::UserDefinedLogicalNodeCore;

    let (tmp, catalog, plan) = typed_single_hop_fixture(Direction::Out);
    let lowerer = GraphPlanLowerer::new_for_reads(
        &graphforge_storage::lowering_snapshot(Some(&catalog), Some(tmp.path())).unwrap(),
        None,
        OntologyMode::Exploratory,
    )
    .unwrap();
    let lp = lowerer.lower_plan(&plan).unwrap();
    let DfLogicalPlan::Extension(ext) = &lp else {
        panic!("expected exploratory ExpandNode, got {lp:?}");
    };
    let node = ext
        .node
        .as_any()
        .downcast_ref::<graphforge_plan::ExpandNode>()
        .expect("ExpandNode");
    let edge = datafusion::common::TableReference::bare("var_1");
    assert!(
        UserDefinedLogicalNodeCore::schema(node)
            .field_with_qualified_name(&edge, "rel_type_name")
            .is_ok()
    );
}

#[test]
fn wildcard_single_hop_emits_expand_extension() {
    let tmp = tempfile::TempDir::new().unwrap();
    let rc = graphforge_ir::RuntimeCatalog::new();
    let catalog = graphforge_storage::GraphCatalog::open(tmp.path(), None, &rc).unwrap();
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
        .build();
    let lowerer = GraphPlanLowerer::new_for_reads(
        &graphforge_storage::lowering_snapshot(Some(&catalog), Some(tmp.path())).unwrap(),
        None,
        OntologyMode::Strict,
    )
    .unwrap();
    let lp = lowerer.lower_plan(&plan).unwrap();
    let DfLogicalPlan::Extension(ext) = &lp else {
        panic!("expected wildcard ExpandNode, got {lp:?}");
    };
    let node = ext
        .node
        .as_any()
        .downcast_ref::<graphforge_plan::ExpandNode>()
        .expect("ExpandNode");
    assert_eq!(node.rel_type_name, "*");
    assert_eq!(node.rel_ty, None);
}

#[test]
fn unbound_source_errors_on_provider_path() {
    // The adjacency path must not bypass the source-binding invariant: an
    // Expand whose src var was never bound is a lowering error, not a
    // silently column-0-seeded ExpandNode.
    let tmp = tempfile::TempDir::new().unwrap();
    let mut rc = graphforge_ir::RuntimeCatalog::new();
    let rel = RelationTypeId::runtime(rc.intern_relation_type("KNOWS").unwrap());
    let catalog = graphforge_storage::GraphCatalog::open(tmp.path(), None, &rc).unwrap();
    // No NodeScan: src VarId(0) is never registered.
    let plan = GraphPlan::builder("openCypher")
        .push_op(GraphOp::Expand {
            src: VarId(0),
            edge: VarId(1),
            dst: VarId(2),
            rel_ty: Some(rel),
            dir: Direction::Out,
            min_hops: 1,
            max_hops: Some(1),
        })
        .build();
    let lowerer = GraphPlanLowerer::new_for_reads(
        &graphforge_storage::lowering_snapshot(Some(&catalog), Some(tmp.path())).unwrap(),
        None,
        OntologyMode::Strict,
    )
    .unwrap();
    let err = lowerer.lower_plan(&plan).unwrap_err();
    assert!(
        err.to_string().contains("unbound") || err.to_string().contains("Unbound"),
        "expected an unbound-variable error, got {err:?}"
    );
}

#[test]
fn exact_zero_var_len_unknown_relation_and_directional_join_paths() {
    let unknown = GraphPlan::builder("openCypher")
        .push_op(GraphOp::NodeScan {
            var: VarId(0),
            ty: None,
        })
        .push_op(GraphOp::Expand {
            src: VarId(0),
            edge: VarId(1),
            dst: VarId(2),
            rel_ty: Some(RelationTypeId::ontology(TypeId(999_999)).unwrap()),
            dir: Direction::Out,
            min_hops: 1,
            max_hops: Some(3),
        })
        .build();
    assert!(
        GraphPlanLowerer::new(None, None)
            .unwrap()
            .lower_plan(&unknown)
            .unwrap_err()
            .to_string()
            .contains("has no known relation name")
    );

    for direction in [Direction::In, Direction::Undirected] {
        let plan = GraphPlan::builder("openCypher")
            .push_op(GraphOp::NodeScan {
                var: VarId(0),
                ty: None,
            })
            .push_op(GraphOp::NodeScan {
                var: VarId(2),
                ty: None,
            })
            .push_op(GraphOp::Expand {
                src: VarId(0),
                edge: VarId(1),
                dst: VarId(2),
                rel_ty: None,
                dir: direction,
                min_hops: 1,
                max_hops: Some(1),
            })
            .build();
        let lowered = GraphPlanLowerer::new(None, None)
            .unwrap()
            .lower_plan(&plan)
            .unwrap();
        let rendered = lowered.display_indent_schema().to_string();
        assert!(rendered.contains("Filter"), "{direction:?}: {rendered}");
    }
}
