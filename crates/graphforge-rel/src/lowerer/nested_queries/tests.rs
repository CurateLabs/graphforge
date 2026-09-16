use super::super::scans::table_source;
use super::super::tests::make_catalog_and_lowerer;
use super::super::*;
use super::*;
use datafusion::logical_expr::LogicalPlan as DfLogicalPlan;
use graphforge_core::TypeId;
use graphforge_ir::expr::{IrExpr, IrLiteral};
use graphforge_ir::{Direction, GraphPlan, VarId};

fn node_scan_with_alias(alias: &str) -> LogicalPlan {
    let schema = std::sync::Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
        datafusion::arrow::datatypes::Field::new(
            "node_id",
            datafusion::arrow::datatypes::DataType::UInt64,
            false,
        ),
    ]));
    LogicalPlanBuilder::scan(alias, table_source(schema), None)
        .and_then(LogicalPlanBuilder::build)
        .unwrap()
}

#[test]
fn optional_join_keys_use_var_map_qualifiers() {
    let outer = node_scan_with_alias("projected_node");
    let inner = node_scan_with_alias("pattern_node");
    let mut outer_vm = VarMap::new();
    outer_vm.insert(VarId(7), "projected_node");
    let mut inner_vm = VarMap::new();
    inner_vm.insert(VarId(7), "pattern_node");

    let (join_keys, inner_keep_idx) = optional_join_keys(&outer, &inner, &outer_vm, &inner_vm);

    assert_eq!(join_keys, vec![(0, 0)]);
    assert_eq!(inner_keep_idx, Vec::<usize>::new());
}

#[test]
fn correlated_subquery_shapes_fail_with_precise_contract_errors() {
    let lowerer = GraphPlanLowerer::new(None, None).unwrap();

    let no_alternatives = GraphPlan::builder("openCypher")
        .push_op(GraphOp::Exists {
            child: Box::new(
                GraphPlan::builder("openCypher")
                    .push_op(GraphOp::Union {
                        all: true,
                        inputs: vec![],
                    })
                    .build(),
            ),
            negated: false,
        })
        .build();
    assert!(
        lowerer
            .lower_plan(&no_alternatives)
            .unwrap_err()
            .to_string()
            .contains("no alternatives")
    );

    let uncorrelated = GraphPlan::builder("openCypher")
        .push_op(GraphOp::Exists {
            child: Box::new(GraphPlan::builder("openCypher").build()),
            negated: true,
        })
        .build();
    assert!(
        lowerer
            .lower_plan(&uncorrelated)
            .unwrap_err()
            .to_string()
            .contains("share at least one bound variable")
    );

    let empty_comprehension = GraphPlan::builder("openCypher")
        .push_op(GraphOp::PatternComprehension {
            child: Box::new(GraphPlan::builder("openCypher").build()),
            output: VarId(10),
        })
        .build();
    assert!(
        lowerer
            .lower_plan(&empty_comprehension)
            .unwrap_err()
            .to_string()
            .contains("child is empty")
    );

    let wrong_terminal = GraphPlan::builder("openCypher")
        .push_op(GraphOp::PatternComprehension {
            child: Box::new(
                GraphPlan::builder("openCypher")
                    .push_op(GraphOp::Limit { count: 1 })
                    .build(),
            ),
            output: VarId(10),
        })
        .build();
    assert!(
        lowerer
            .lower_plan(&wrong_terminal)
            .unwrap_err()
            .to_string()
            .contains("must end in a value projection")
    );
}

#[test]
fn pattern_comprehension_projection_contract_is_strict() {
    let lowerer = GraphPlanLowerer::new(None, None).unwrap();
    let cases = [
        (
            true,
            PATTERN_COMPREHENSION_VALUE_ALIAS,
            "exactly one non-distinct",
        ),
        (false, "wrong_alias", "invalid value projection"),
    ];
    for (distinct, alias, expected) in cases {
        let mut child = GraphPlan::builder("openCypher");
        let value = child.push_expr(IrExpr::Literal(IrLiteral::Int(1)));
        child.push_op_mut(GraphOp::Project {
            items: vec![ProjectItem {
                expr: value,
                alias: Some(alias.into()),
                out_var: None,
            }],
            distinct,
        });
        let plan = GraphPlan::builder("openCypher")
            .push_op(GraphOp::PatternComprehension {
                child: Box::new(child.build()),
                output: VarId(11),
            })
            .build();
        assert!(
            lowerer
                .lower_plan(&plan)
                .unwrap_err()
                .to_string()
                .contains(expected)
        );
    }
}

#[test]
fn union_requires_two_branch_plans() {
    let (_dir, catalog, _rc) = make_catalog_and_lowerer();
    let lowerer = GraphPlanLowerer::new(
        Some(&graphforge_storage::lowering_snapshot(Some(&catalog), None).unwrap()),
        None,
    )
    .unwrap();
    let plan = GraphPlan::builder("openCypher")
        .push_op(GraphOp::Union {
            all: true,
            inputs: vec![GraphPlan::builder("openCypher").build()],
        })
        .build();

    let error = lowerer
        .lower_plan(&plan)
        .expect_err("one branch is invalid");
    assert!(error.to_string().contains("at least two branch plans"));
}

#[test]
fn optional_produces_extension_node() {
    let (_dir, catalog, _rc) = make_catalog_and_lowerer();
    let lowerer = GraphPlanLowerer::new(
        Some(&graphforge_storage::lowering_snapshot(Some(&catalog), None).unwrap()),
        None,
    )
    .unwrap();

    let child = GraphPlan::builder("openCypher")
        .push_op(GraphOp::NodeScan {
            var: VarId(1),
            ty: None,
        })
        .build();
    let plan = GraphPlan::builder("openCypher")
        .push_op(GraphOp::NodeScan {
            var: VarId(0),
            ty: None,
        })
        .push_op(GraphOp::Optional {
            child: Box::new(child),
        })
        .build();
    let lp = lowerer.lower_plan(&plan).unwrap();
    assert!(
        matches!(lp, DfLogicalPlan::Extension(_)),
        "expected Extension (OptionalMatchNode), got {lp:?}"
    );
}

#[test]
fn optional_node_output_schema_appends_nullable_inner() {
    use datafusion::logical_expr::UserDefinedLogicalNodeCore;
    use graphforge_plan::OptionalMatchNode;

    let (_dir, catalog, _rc) = make_catalog_and_lowerer();
    let lowerer = GraphPlanLowerer::new(
        Some(&graphforge_storage::lowering_snapshot(Some(&catalog), None).unwrap()),
        None,
    )
    .unwrap();

    // Outer binds var_0; the optional child binds a fresh, unshared var_1,
    // so `join_keys` is empty and all 5 inner columns are kept. This test
    // pins the *schema* contract (outer ++ nullable inner); the shared-var
    // exclusion (non-empty join keys) is covered by
    // `optional_child_with_shared_var_excludes_outer_columns` below.
    let child = GraphPlan::builder("openCypher")
        .push_op(GraphOp::NodeScan {
            var: VarId(1),
            ty: None,
        })
        .build();
    let plan = GraphPlan::builder("openCypher")
        .push_op(GraphOp::NodeScan {
            var: VarId(0),
            ty: None,
        })
        .push_op(GraphOp::Optional {
            child: Box::new(child),
        })
        .build();
    let lp = lowerer.lower_plan(&plan).unwrap();
    let DfLogicalPlan::Extension(ext) = &lp else {
        panic!("expected Extension (OptionalMatchNode), got {lp:?}");
    };
    let node = ext
        .node
        .as_any()
        .downcast_ref::<OptionalMatchNode>()
        .expect("OptionalMatchNode");

    // Output = outer (6 topology cols) ++ inner (6 cols, all nullable).
    let schema = UserDefinedLogicalNodeCore::schema(node);
    assert_eq!(schema.fields().len(), 12, "outer(6) + inner(6)");
    for i in 0..6 {
        assert!(
            !schema.field(i).is_nullable(),
            "outer col {i} stays non-null"
        );
    }
    for i in 6..12 {
        assert!(
            schema.field(i).is_nullable(),
            "inner col {i} must be nullable for null-shaping"
        );
    }
}

#[test]
fn optional_child_with_shared_var_excludes_outer_columns() {
    use datafusion::logical_expr::UserDefinedLogicalNodeCore;
    use graphforge_plan::OptionalMatchNode;

    // `MATCH (a) OPTIONAL MATCH (a)-[:R]->(b)`: the optional child now lowers
    // to a real join that re-binds the shared `a` (var_0). Its columns must
    // NOT be appended again (they live on the outer side) — otherwise the
    // node's schema would carry duplicate `var_0` fields (#718).
    let (_dir, catalog, _rc) = make_catalog_and_lowerer();
    let lowerer = GraphPlanLowerer::new(
        Some(&graphforge_storage::lowering_snapshot(Some(&catalog), None).unwrap()),
        None,
    )
    .unwrap();
    let child = GraphPlan::builder("openCypher")
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
    let plan = GraphPlan::builder("openCypher")
        .push_op(GraphOp::NodeScan {
            var: VarId(0),
            ty: None,
        })
        .push_op(GraphOp::Optional {
            child: Box::new(child),
        })
        .build();
    let lp = lowerer.lower_plan(&plan).unwrap();
    let DfLogicalPlan::Extension(ext) = &lp else {
        panic!("expected Extension (OptionalMatchNode), got {lp:?}");
    };
    let node = ext
        .node
        .as_any()
        .downcast_ref::<OptionalMatchNode>()
        .expect("OptionalMatchNode");

    // The shared var_0 is a join key, so the inner output drops all 6 of its
    // columns — keeping only the exploratory edge (var_1, 8 cols) and dst
    // (var_2, 6).
    assert_eq!(node.join_keys.len(), 1, "var_0 is the shared join key");
    let schema = UserDefinedLogicalNodeCore::schema(node);
    // outer var_0 (6) ++ inner kept (edge 8 + var_2 6 = 14) = 20.
    assert_eq!(schema.fields().len(), 20, "no duplicate var_0 columns");
    // Building the schema would have panicked on a duplicate qualified field
    // if var_0's columns were appended, so reaching here proves exclusion.
}

#[test]
fn exact_zero_recursive_plan_reference_and_binding_analysis() {
    let wanted = VarId(17);
    let other = VarId(18);
    let mut referenced_builder = GraphPlan::builder("openCypher");
    referenced_builder.push_expr(IrExpr::VarRef(wanted));
    let referenced = referenced_builder.build();
    let mut unrelated_builder = GraphPlan::builder("openCypher");
    unrelated_builder.push_expr(IrExpr::VarRef(other));
    let unrelated = unrelated_builder.build();

    assert!(plan_references_var(&referenced, wanted));
    assert!(!plan_references_var(&unrelated, wanted));

    let wrappers = [
        GraphOp::Optional {
            child: Box::new(referenced.clone()),
        },
        GraphOp::Exists {
            child: Box::new(referenced.clone()),
            negated: false,
        },
        GraphOp::PatternComprehension {
            child: Box::new(referenced.clone()),
            output: VarId(20),
        },
        GraphOp::ListElementPatternComprehension {
            list_expr: ExprId(0),
            loop_var: VarId(21),
            child: Box::new(referenced.clone()),
            pattern_output: VarId(22),
            filter: None,
            projection: None,
            output: VarId(23),
        },
        GraphOp::Union {
            all: true,
            inputs: vec![unrelated.clone(), referenced.clone()],
        },
    ];
    for wrapper in wrappers {
        let plan = GraphPlan::builder("openCypher").push_op(wrapper).build();
        assert!(plan_references_var(&plan, wanted));
    }

    for op in [
        GraphOp::NodeScan {
            var: wanted,
            ty: None,
        },
        GraphOp::EdgeScan {
            var: wanted,
            ty: None,
        },
        GraphOp::TypedEdgeScan {
            var: wanted,
            rel_ty: RelationTypeId::ontology(TypeId(1)).unwrap(),
        },
        GraphOp::Expand {
            src: other,
            edge: wanted,
            dst: VarId(19),
            rel_ty: None,
            dir: Direction::Out,
            min_hops: 1,
            max_hops: Some(1),
        },
    ] {
        assert!(graph_op_binds_var(&op, wanted));
    }
    assert!(!graph_op_binds_var(
        &GraphOp::NodeScan {
            var: other,
            ty: None,
        },
        wanted
    ));

    let mut outer = VarMap::new();
    outer.insert(wanted, "outer");
    assert!(!full_subquery_needs_outer_input(&unrelated, &outer));
    assert!(full_subquery_needs_outer_input(&referenced, &outer));
    let mut locally_bound_builder = GraphPlan::builder("openCypher");
    locally_bound_builder.push_op_mut(GraphOp::NodeScan {
        var: wanted,
        ty: None,
    });
    locally_bound_builder.push_expr(IrExpr::VarRef(wanted));
    let locally_bound = locally_bound_builder.build();
    assert!(!full_subquery_needs_outer_input(&locally_bound, &outer));
}

#[test]
fn exact_zero_optional_scope_promotes_only_inner_entity_shapes() {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    let scan = |alias: &str, field: Field| {
        LogicalPlanBuilder::scan(
            alias,
            table_source(Arc::new(Schema::new(vec![field]))),
            None,
        )
        .and_then(LogicalPlanBuilder::build)
        .unwrap()
    };
    let outer = scan("outer_scalar", Field::new("value", DataType::Int64, true));
    let inner = scan("inner_node", Field::new("node_uuid", DataType::Utf8, false));
    let promoted = VarId(30);
    let absent_outer = VarId(31);
    let mut child_vm = VarMap::new();
    child_vm.insert(promoted, "inner_node");
    child_vm.insert(absent_outer, "inner_node");
    let mut outer_vm = VarMap::new();
    outer_vm.insert(promoted, "outer_scalar");

    promote_optional_entity_vars(&outer, &inner, &child_vm, &mut outer_vm);
    assert_eq!(outer_vm.get(promoted), Some("inner_node"));
    assert_eq!(outer_vm.get(absent_outer), None);

    let already_entity = scan("outer_node", Field::new("node_uuid", DataType::Utf8, false));
    outer_vm.insert(promoted, "outer_node");
    promote_optional_entity_vars(&already_entity, &inner, &child_vm, &mut outer_vm);
    assert_eq!(outer_vm.get(promoted), Some("outer_node"));
}
