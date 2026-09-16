use super::traversal::expand_bound_edge_single_dir;
use super::writes::{
    const_eval_scalar, contains_map_literal, eval_map_literal, reject_map_property_value,
};
use super::*;
use datafusion::logical_expr::LogicalPlan as DfLogicalPlan;
use graphforge_core::TypeId;
use graphforge_ir::expr::{IrExpr, IrLiteral};
use graphforge_ir::{ExprArena, GraphPlan, VarId};

fn admission_runtime() -> graphforge_ontology::OntologyRuntime {
    let doc = graphforge_ontology::OntologyLoader::load_yaml(
        br#"ontology_id: admission
version: "1"
entity_types:
  - name: Person
relation_types:
  - name: KNOWS
    src: Person
    dst: Person
    semantic:
      transitive: true
"#
        .as_slice(),
    )
    .unwrap();
    graphforge_ontology::OntologyCompiler::compile(&doc).unwrap()
}

#[test]
fn lowerer_admission_rejects_public_runtime_identity_bypass() {
    for entity in [true, false] {
        for invalid in [graphforge_value::TYPE_LOCAL_ID_LIMIT, 1 << 31, u32::MAX] {
            let mut runtime = admission_runtime();
            if entity {
                runtime.entity_name_to_id.insert("Person".into(), invalid);
            } else {
                runtime.relation_name_to_id.insert("KNOWS".into(), invalid);
            }
            let handle = OntologyHandle::new(runtime);
            for result in [
                GraphPlanLowerer::new(None, Some(&handle)),
                GraphPlanLowerer::new_for_reads(
                    &LoweringSnapshot::default(),
                    Some(&handle),
                    OntologyMode::Strict,
                ),
                GraphPlanLowerer::new_for_writes(
                    &LoweringSnapshot::default(),
                    Some(&handle),
                    OntologyMode::Strict,
                ),
            ] {
                assert!(
                    matches!(result, Err(GfError::Validation(_))),
                    "entity={entity}, id={invalid}"
                );
            }
            if !entity {
                assert!(build_inference_rules(Some(&handle)).is_err());
            }
        }
    }
}

#[test]
fn lowerer_admission_preserves_independent_declared_namespaces_and_boundary() {
    for raw in [0, graphforge_value::TYPE_LOCAL_ID_LIMIT - 1] {
        let mut runtime = admission_runtime();
        runtime.entity_name_to_id.insert("Person".into(), raw);
        runtime.relation_name_to_id.insert("KNOWS".into(), raw);
        let handle = OntologyHandle::new(runtime);
        let lowerer = GraphPlanLowerer::new(None, Some(&handle)).unwrap();
        assert_eq!(
            lowerer.type_id_to_entity_name[&EntityTypeId::ontology(TypeId(raw)).unwrap()],
            "Person"
        );
        assert_eq!(
            lowerer.type_id_to_rel_name[&RelationTypeId::ontology(TypeId(raw)).unwrap()],
            "KNOWS"
        );
    }
}

fn empty_base() -> LogicalPlan {
    LogicalPlanBuilder::empty(false).build().unwrap()
}

#[test]
fn unsupported_error_mapping_preserves_success_and_exact_diagnostic() {
    let success: Result<u8, &str> = Ok(7);
    assert_eq!(success.map_unsupported_expr().unwrap(), 7);

    let failure: Result<u8, &str> = Err("planner diagnostic: var_9");
    assert_eq!(
        failure.map_unsupported_expr().unwrap_err().to_string(),
        "unsupported expression: planner diagnostic: var_9"
    );
}

pub(super) fn make_catalog_and_lowerer() -> (
    tempfile::TempDir,
    graphforge_storage::GraphCatalog,
    graphforge_ir::RuntimeCatalog,
) {
    let dir = tempfile::TempDir::new().unwrap();
    let rc = graphforge_ir::RuntimeCatalog::new();
    let catalog = graphforge_storage::GraphCatalog::open(dir.path(), None, &rc).unwrap();
    (dir, catalog, rc)
}

#[test]
fn filter_lowers_predicate() {
    let (_dir, catalog, _rc) = make_catalog_and_lowerer();
    let lowerer = GraphPlanLowerer::new(
        Some(&graphforge_storage::lowering_snapshot(Some(&catalog), None).unwrap()),
        None,
    )
    .unwrap();

    let mut arena = ExprArena::new();
    let lit = arena.push(IrExpr::Literal(IrLiteral::Bool(true)));
    let var_map = VarMap::new();
    let expr_lowerer = ExprLowerer::new(&arena, None, &var_map);

    let result = lowerer
        .lower_op(
            &GraphOp::Filter { predicate: lit },
            empty_base(),
            &arena,
            &var_map,
            &expr_lowerer,
        )
        .unwrap();

    assert!(
        matches!(result, DfLogicalPlan::Filter(_)),
        "expected Filter, got {result:?}"
    );
}

#[test]
fn project_lowers_columns() {
    let (_dir, catalog, _rc) = make_catalog_and_lowerer();
    let lowerer = GraphPlanLowerer::new(
        Some(&graphforge_storage::lowering_snapshot(Some(&catalog), None).unwrap()),
        None,
    )
    .unwrap();

    let mut arena = ExprArena::new();
    let lit = arena.push(IrExpr::Literal(IrLiteral::Int(1)));
    let item = ProjectItem {
        expr: lit,
        alias: Some("x".into()),
        out_var: None,
    };
    let var_map = VarMap::new();
    let expr_lowerer = ExprLowerer::new(&arena, None, &var_map);

    let result = lowerer
        .lower_op(
            &GraphOp::Project {
                items: vec![item],
                distinct: false,
            },
            empty_base(),
            &arena,
            &var_map,
            &expr_lowerer,
        )
        .unwrap();

    assert!(
        matches!(result, DfLogicalPlan::Projection(_)),
        "expected Projection, got {result:?}"
    );
}

#[test]
fn project_with_distinct_wraps_in_distinct() {
    let (_dir, catalog, _rc) = make_catalog_and_lowerer();
    let lowerer = GraphPlanLowerer::new(
        Some(&graphforge_storage::lowering_snapshot(Some(&catalog), None).unwrap()),
        None,
    )
    .unwrap();

    let mut arena = ExprArena::new();
    let lit = arena.push(IrExpr::Literal(IrLiteral::Int(1)));
    let item = ProjectItem {
        expr: lit,
        alias: None,
        out_var: None,
    };
    let var_map = VarMap::new();
    let expr_lowerer = ExprLowerer::new(&arena, None, &var_map);

    let result = lowerer
        .lower_op(
            &GraphOp::Project {
                items: vec![item],
                distinct: true,
            },
            empty_base(),
            &arena,
            &var_map,
            &expr_lowerer,
        )
        .unwrap();

    assert!(
        matches!(result, DfLogicalPlan::Distinct(_)),
        "expected Distinct, got {result:?}"
    );
}

#[test]
fn with_where_scalar_alias_uses_projected_scope_then_drops_inputs() {
    let (_dir, catalog, _rc) = make_catalog_and_lowerer();
    let lowerer = GraphPlanLowerer::new(
        Some(&graphforge_storage::lowering_snapshot(Some(&catalog), None).unwrap()),
        None,
    )
    .unwrap();
    let mut builder = GraphPlan::builder("openCypher");
    let value = builder.push_expr(IrExpr::Literal(IrLiteral::Bool(true)));
    let predicate = builder.push_expr(IrExpr::VarRef(VarId(9)));
    let plan = builder
        .push_op(GraphOp::With {
            items: vec![ProjectItem {
                expr: value,
                alias: Some("keep".into()),
                out_var: Some(VarId(9)),
            }],
            distinct: false,
            where_predicate: Some(predicate),
        })
        .build();

    let lowered = lowerer.lower_plan(&plan).unwrap();
    let DfLogicalPlan::Projection(final_projection) = lowered else {
        panic!("expected final WITH projection");
    };
    assert_eq!(final_projection.schema.fields().len(), 1);
    assert_eq!(final_projection.schema.field(0).name(), "keep");
    assert!(matches!(
        final_projection.input.as_ref(),
        DfLogicalPlan::Filter(_)
    ));
}

#[test]
fn with_where_forwards_complete_node_shape_through_new_scope() {
    let (_dir, catalog, _rc) = make_catalog_and_lowerer();
    let lowerer = GraphPlanLowerer::new(
        Some(&graphforge_storage::lowering_snapshot(Some(&catalog), None).unwrap()),
        None,
    )
    .unwrap();
    let mut builder = GraphPlan::builder("openCypher");
    let node = builder.push_expr(IrExpr::VarRef(VarId(0)));
    let predicate = builder.push_expr(IrExpr::Literal(IrLiteral::Bool(true)));
    let plan = builder
        .push_op(GraphOp::NodeScan {
            var: VarId(0),
            ty: None,
        })
        .push_op(GraphOp::With {
            items: vec![ProjectItem {
                expr: node,
                alias: Some("n".into()),
                out_var: Some(VarId(0)),
            }],
            distinct: false,
            where_predicate: Some(predicate),
        })
        .build();

    let lowered = lowerer.lower_plan(&plan).unwrap();
    let DfLogicalPlan::Projection(final_projection) = lowered else {
        panic!("expected final WITH projection");
    };
    assert!(final_projection.schema.fields().len() >= 4);
    assert!(
        final_projection.schema.iter().all(|(qualifier, _)| {
            qualifier.is_some_and(|qualifier| qualifier.table() == "var_0")
        })
    );
    assert!(matches!(
        final_projection.input.as_ref(),
        DfLogicalPlan::Filter(_)
    ));
}

#[test]
fn aggregate_count_star() {
    let (_dir, catalog, _rc) = make_catalog_and_lowerer();
    let lowerer = GraphPlanLowerer::new(
        Some(&graphforge_storage::lowering_snapshot(Some(&catalog), None).unwrap()),
        None,
    )
    .unwrap();

    let arena = ExprArena::new();
    let agg = AggExpr {
        func: AggFunc::Count,
        arg: None,
        percentile: None,
        alias: "total".into(),
        out_var: None,
    };
    let var_map = VarMap::new();
    let expr_lowerer = ExprLowerer::new(&arena, None, &var_map);

    let result = lowerer
        .lower_op(
            &GraphOp::Aggregate {
                group_by: vec![],
                group_aliases: vec![],
                group_vars: vec![],
                aggs: vec![agg],
            },
            empty_base(),
            &arena,
            &var_map,
            &expr_lowerer,
        )
        .unwrap();

    assert!(
        matches!(result, DfLogicalPlan::Aggregate(_)),
        "expected Aggregate, got {result:?}"
    );
}

#[test]
fn aggregate_function_argument_contract_matrix_and_specialized_paths() {
    use datafusion::arrow::datatypes::{DataType, Field, Fields};
    use datafusion::logical_expr::lit;

    for (function, expected) in [
        (
            AggFunc::CountDistinct,
            "COUNT DISTINCT requires an argument",
        ),
        (AggFunc::Sum, "SUM requires an argument"),
        (AggFunc::SumDistinct, "SUM DISTINCT requires an argument"),
        (AggFunc::Avg, "AVG requires an argument"),
        (AggFunc::AvgDistinct, "AVG DISTINCT requires an argument"),
        (AggFunc::Min, "MIN requires an argument"),
        (AggFunc::Max, "MAX requires an argument"),
        (AggFunc::Collect, "COLLECT requires an argument"),
        (AggFunc::CollectDistinct, "COLLECT requires an argument"),
        (
            AggFunc::PercentileDisc,
            "percentileDisc requires a value argument",
        ),
        (
            AggFunc::PercentileCont,
            "percentileCont requires a value argument",
        ),
    ] {
        assert_eq!(
            lower_agg_func(function, None, None, None)
                .unwrap_err()
                .to_string(),
            format!("unsupported expression: {expected}")
        );
    }

    for function in [AggFunc::PercentileDisc, AggFunc::PercentileCont] {
        assert!(
            lower_agg_func(function, Some(lit(1_i64)), None, Some(&DataType::Int64))
                .unwrap_err()
                .to_string()
                .contains("percentile argument")
        );
    }

    for function in [
        AggFunc::Count,
        AggFunc::CountDistinct,
        AggFunc::Sum,
        AggFunc::SumDistinct,
        AggFunc::Avg,
        AggFunc::AvgDistinct,
        AggFunc::Min,
        AggFunc::Max,
        AggFunc::Collect,
        AggFunc::CollectDistinct,
    ] {
        let arg = (function != AggFunc::Count).then(|| lit(1_i64));
        assert!(lower_agg_func(function, arg, None, Some(&DataType::Int64)).is_ok());
    }
    assert!(
        lower_agg_func(
            AggFunc::Avg,
            Some(lit(datafusion::scalar::ScalarValue::Null)),
            None,
            Some(&DataType::Null),
        )
        .is_ok()
    );
    assert!(
        lower_agg_func(
            AggFunc::AvgDistinct,
            Some(lit(datafusion::scalar::ScalarValue::Null)),
            None,
            Some(&DataType::Null),
        )
        .is_ok()
    );

    let heterogeneous = DataType::Struct(Fields::from(vec![
        Field::new(graphforge_value::heterogeneous::TAG, DataType::Int8, false),
        Field::new(
            graphforge_value::heterogeneous::payload_field(0),
            DataType::Int64,
            true,
        ),
    ]));
    for function in [AggFunc::Min, AggFunc::Max] {
        let expression = lower_agg_func(
            function,
            Some(lit(datafusion::scalar::ScalarValue::Null)),
            None,
            Some(&heterogeneous),
        )
        .unwrap();
        assert!(format!("{expression}").contains("cypher_"));
    }
}

#[test]
fn sort_lowers_keys() {
    let (_dir, catalog, _rc) = make_catalog_and_lowerer();
    let lowerer = GraphPlanLowerer::new(
        Some(&graphforge_storage::lowering_snapshot(Some(&catalog), None).unwrap()),
        None,
    )
    .unwrap();

    // Use a literal rather than a VarRef to avoid schema validation on
    // the empty base relation (DataFusion rejects unknown column names).
    let mut arena = ExprArena::new();
    let lit = arena.push(IrExpr::Literal(IrLiteral::Int(1)));
    let var_map = VarMap::new();
    let key = graphforge_ir::SortKey {
        expr: lit,
        order: SortOrder::Desc,
        nulls_first: false,
    };
    let expr_lowerer = ExprLowerer::new(&arena, None, &var_map);

    let result = lowerer
        .lower_op(
            &GraphOp::Sort { keys: vec![key] },
            empty_base(),
            &arena,
            &var_map,
            &expr_lowerer,
        )
        .unwrap();

    assert!(
        matches!(result, DfLogicalPlan::Sort(_)),
        "expected Sort, got {result:?}"
    );
}

#[test]
fn limit_lowers_correctly() {
    let (_dir, catalog, _rc) = make_catalog_and_lowerer();
    let lowerer = GraphPlanLowerer::new(
        Some(&graphforge_storage::lowering_snapshot(Some(&catalog), None).unwrap()),
        None,
    )
    .unwrap();

    let arena = ExprArena::new();
    let var_map = VarMap::new();
    let expr_lowerer = ExprLowerer::new(&arena, None, &var_map);

    let result = lowerer
        .lower_op(
            &GraphOp::Limit { count: 10 },
            empty_base(),
            &arena,
            &var_map,
            &expr_lowerer,
        )
        .unwrap();

    assert!(
        matches!(result, DfLogicalPlan::Limit(_)),
        "expected Limit, got {result:?}"
    );
}

#[test]
fn skip_lowers_correctly() {
    let (_dir, catalog, _rc) = make_catalog_and_lowerer();
    let lowerer = GraphPlanLowerer::new(
        Some(&graphforge_storage::lowering_snapshot(Some(&catalog), None).unwrap()),
        None,
    )
    .unwrap();

    let arena = ExprArena::new();
    let var_map = VarMap::new();
    let expr_lowerer = ExprLowerer::new(&arena, None, &var_map);

    let result = lowerer
        .lower_op(
            &GraphOp::Skip { count: 5 },
            empty_base(),
            &arena,
            &var_map,
            &expr_lowerer,
        )
        .unwrap();

    // Skip is implemented as Limit with a non-zero skip offset
    assert!(
        matches!(result, DfLogicalPlan::Limit(_)),
        "expected Limit (skip), got {result:?}"
    );
}

#[test]
fn unsupported_op_returns_error() {
    let (_dir, catalog, _rc) = make_catalog_and_lowerer();
    let lowerer = GraphPlanLowerer::new(
        Some(&graphforge_storage::lowering_snapshot(Some(&catalog), None).unwrap()),
        None,
    )
    .unwrap();

    let arena = ExprArena::new();
    let var_map = VarMap::new();
    let expr_lowerer = ExprLowerer::new(&arena, None, &var_map);

    let result = lowerer.lower_op(
        &GraphOp::NodeScan {
            var: VarId(0),
            ty: None,
        },
        empty_base(),
        &arena,
        &var_map,
        &expr_lowerer,
    );
    assert!(
        matches!(result, Err(LoweringError::UnsupportedExpr(_))),
        "expected UnsupportedExpr for NodeScan"
    );
}

#[test]
fn terminal_suffix_uses_supplied_schema_and_preserves_scope() {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::DFSchema;

    let lowerer = GraphPlanLowerer::new(None, None).unwrap();
    let schema = Arc::new(
        DFSchema::try_from(Schema::new(vec![Field::new(
            "seed",
            DataType::Int64,
            false,
        )]))
        .unwrap(),
    );
    let mut arena = ExprArena::new();
    let literal = arena.push(IrExpr::Literal(IrLiteral::Int(9)));
    let mut vars = VarMap::new();
    vars.insert(VarId(4), "seed");
    let suffix = lowerer
        .lower_terminal_suffix(
            &[GraphOp::Project {
                items: vec![ProjectItem {
                    expr: literal,
                    alias: Some("answer".into()),
                    out_var: Some(VarId(5)),
                }],
                distinct: false,
            }],
            &arena,
            &mut vars,
            schema,
        )
        .unwrap();
    assert!(matches!(suffix, DfLogicalPlan::Projection(_)));
    assert_eq!(vars.get(VarId(5)), Some("answer"));
}

#[test]
fn lower_plan_empty_ops_succeeds() {
    let (_dir, catalog, _rc) = make_catalog_and_lowerer();
    let lowerer = GraphPlanLowerer::new(
        Some(&graphforge_storage::lowering_snapshot(Some(&catalog), None).unwrap()),
        None,
    )
    .unwrap();

    let plan = GraphPlan::builder("openCypher").build();
    let result = lowerer.lower_plan(&plan);
    assert!(result.is_ok(), "empty op pipeline should succeed");
}

#[test]
fn integration_filter_project_limit_pipeline() {
    let (_dir, catalog, _rc) = make_catalog_and_lowerer();
    let lowerer = GraphPlanLowerer::new(
        Some(&graphforge_storage::lowering_snapshot(Some(&catalog), None).unwrap()),
        None,
    )
    .unwrap();

    // Build the plan using GraphPlanBuilder so expressions live in plan.exprs.
    let mut builder = GraphPlan::builder("openCypher");
    let pred = builder.push_expr(IrExpr::Literal(IrLiteral::Bool(true)));
    let col_expr = builder.push_expr(IrExpr::Literal(IrLiteral::Str("n".into())));

    let plan = builder
        .push_op(GraphOp::Filter { predicate: pred })
        .push_op(GraphOp::Project {
            items: vec![ProjectItem {
                expr: col_expr,
                alias: Some("name".into()),
                out_var: None,
            }],
            distinct: false,
        })
        .push_op(GraphOp::Limit { count: 10 })
        .build();

    let lp = lowerer.lower_plan(&plan).unwrap();
    // Outermost plan node should be Limit
    assert!(
        matches!(lp, DfLogicalPlan::Limit(_)),
        "expected Limit at top, got {lp:?}"
    );
}

#[test]
fn unwind_produces_extension_node() {
    let lowerer = GraphPlanLowerer::new(None, None).unwrap();

    let mut builder = GraphPlan::builder("openCypher");
    let list_expr = builder.push_expr(IrExpr::Literal(IrLiteral::Int(1)));
    let plan = builder
        .push_op(GraphOp::Unwind {
            list_expr,
            alias: VarId(0),
        })
        .build();
    let lp = lowerer.lower_plan(&plan).unwrap();
    assert!(
        matches!(lp, DfLogicalPlan::Extension(_)),
        "expected Extension (UnwindNode), got {lp:?}"
    );
}

#[test]
fn pure_lowering_helpers_cover_constants_collections_and_aggregate_contracts() {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::DFSchema;
    use datafusion::logical_expr::expr_fn::col;
    use datafusion::logical_expr::{Operator, lit};
    use datafusion::scalar::ScalarValue;

    assert_eq!(
        const_eval_scalar(&lit(ScalarValue::Int64(Some(7)))),
        Some(ScalarValue::Int64(Some(7)))
    );
    assert_eq!(
        const_eval_scalar(&DfExpr::BinaryExpr(
            datafusion::logical_expr::BinaryExpr::new(
                Box::new(lit(ScalarValue::Int64(Some(2)))),
                Operator::Plus,
                Box::new(lit(ScalarValue::Int64(Some(3)))),
            )
        )),
        Some(ScalarValue::Int64(Some(5)))
    );
    assert_eq!(const_eval_scalar(&col("missing")), None);

    for literal in [
        IrLiteral::Null,
        IrLiteral::Int(1),
        IrLiteral::List(vec![IrLiteral::Int(1)]),
    ] {
        assert!(!contains_map_literal(&literal));
        reject_map_property_value("safe", &literal).unwrap();
    }
    for literal in [
        IrLiteral::Map(vec![]),
        IrLiteral::List(vec![IrLiteral::Map(vec![])]),
    ] {
        assert!(contains_map_literal(&literal));
        assert!(
            reject_map_property_value("nested", &literal)
                .unwrap_err()
                .to_string()
                .contains("cannot store map values")
        );
    }
    assert_eq!(var_alias(VarId(42)), "var_42");
    assert!(build_type_id_map(None).unwrap().is_empty());
    assert!(build_entity_id_map(None).unwrap().is_empty());
    assert!(build_inference_rules(None).unwrap().is_empty());

    let schema = Arc::new(
        DFSchema::try_from(Schema::new(vec![
            Field::new("small", DataType::new_list(DataType::Utf8, true), true),
            Field::new(
                "large",
                DataType::new_large_list(DataType::Int32, true),
                true,
            ),
            Field::new(
                "fixed",
                DataType::FixedSizeList(
                    Arc::new(Field::new_list_field(DataType::Boolean, true)),
                    2,
                ),
                true,
            ),
        ]))
        .unwrap(),
    );
    for (name, expected) in [
        ("small", DataType::Utf8),
        ("large", DataType::Int32),
        ("fixed", DataType::Boolean),
    ] {
        assert_eq!(
            unwind_element_field(&col(name), &schema, std::iter::empty::<&str>()).data_type(),
            &expected
        );
    }
    let unknown = unwind_element_field(&col("unknown"), &schema, ["z", "a", "a"]);
    let DataType::Struct(fields) = unknown.data_type() else {
        panic!("unknown typed UNWIND must expose a map-shaped element")
    };
    assert_eq!(
        fields.iter().map(|f| f.name().as_str()).collect::<Vec<_>>(),
        ["a", "z"]
    );

    let value = Some(col("value"));
    for function in [
        AggFunc::Count,
        AggFunc::CountDistinct,
        AggFunc::Sum,
        AggFunc::SumDistinct,
        AggFunc::Avg,
        AggFunc::AvgDistinct,
        AggFunc::Min,
        AggFunc::Max,
        AggFunc::Collect,
        AggFunc::CollectDistinct,
    ] {
        assert!(
            lower_agg_func(function, value.clone(), None, Some(&DataType::Int64)).is_ok(),
            "{function:?}"
        );
    }
    for function in [AggFunc::PercentileDisc, AggFunc::PercentileCont] {
        assert!(
            lower_agg_func(
                function,
                value.clone(),
                Some(lit(ScalarValue::Float64(Some(0.5)))),
                Some(&DataType::Int64),
            )
            .is_ok()
        );
    }
    assert!(lower_agg_func(AggFunc::Count, None, None, None).is_ok());
    for function in [
        AggFunc::CountDistinct,
        AggFunc::Sum,
        AggFunc::SumDistinct,
        AggFunc::Avg,
        AggFunc::AvgDistinct,
        AggFunc::Min,
        AggFunc::Max,
        AggFunc::Collect,
        AggFunc::CollectDistinct,
        AggFunc::PercentileDisc,
        AggFunc::PercentileCont,
    ] {
        assert!(
            lower_agg_func(function, None, None, None).is_err(),
            "{function:?}"
        );
    }
    for function in [AggFunc::Min, AggFunc::Max] {
        let heterogeneous = DataType::Struct(
            vec![Field::new(
                graphforge_value::heterogeneous::TAG,
                DataType::Int8,
                false,
            )]
            .into(),
        );
        assert!(
            format!(
                "{}",
                lower_agg_func(function, value.clone(), None, Some(&heterogeneous),).unwrap()
            )
            .contains("cypher_")
        );
    }
}

#[test]
fn exact_zero_validation_errors_are_specific_and_non_interpolated() {
    let lowerer = GraphPlanLowerer::new(None, None).unwrap();
    let empty_vm = VarMap::new();
    let no_alternatives = lowerer
        .lower_exists_alternatives(&[], false, empty_base(), &empty_vm)
        .unwrap_err();
    assert_eq!(
        no_alternatives.to_string(),
        "unsupported expression: pattern predicate has no alternatives"
    );

    let uncorrelated = GraphPlan::builder("openCypher").build();
    let uncorrelated_error = lowerer
        .lower_exists_alternatives(&[uncorrelated], true, empty_base(), &empty_vm)
        .unwrap_err();
    assert!(
        uncorrelated_error
            .to_string()
            .contains("must share at least one bound variable")
    );

    let mut arena = ExprArena::new();
    let not_a_map = arena.push(IrExpr::Literal(IrLiteral::Int(1)));
    let map_error =
        eval_map_literal(&lowerer, Some(not_a_map), &arena, &empty_vm, None).unwrap_err();
    assert!(
        map_error
            .to_string()
            .contains("CREATE properties must be a map literal")
    );
    assert_eq!(
        eval_map_literal(&lowerer, None, &arena, &empty_vm, None).unwrap(),
        (Vec::new(), Vec::new())
    );

    let mut edge_vm = VarMap::new();
    let bound_error = expand_bound_edge_single_dir(
        VarId(2),
        Some(RelationTypeId::ontology(TypeId(999_999)).unwrap()),
        "var_0",
        "var_1",
        true,
        empty_base(),
        &mut edge_vm,
        &HashMap::new(),
        None,
    )
    .unwrap_err();
    assert!(bound_error.to_string().contains("TypeId(999999)"));
    assert!(bound_error.to_string().contains("no known relation name"));
}

#[test]
fn exact_zero_pagination_accepts_platform_boundary_values() {
    assert!(matches!(
        lower_limit(0, empty_base()).unwrap(),
        DfLogicalPlan::Limit(_)
    ));
    assert!(matches!(
        lower_skip(0, empty_base()).unwrap(),
        DfLogicalPlan::Limit(_)
    ));
    assert!(matches!(
        lower_limit(u64::try_from(usize::MAX).unwrap(), empty_base()).unwrap(),
        DfLogicalPlan::Limit(_)
    ));
    assert!(matches!(
        lower_skip(u64::try_from(usize::MAX).unwrap(), empty_base()).unwrap(),
        DfLogicalPlan::Limit(_)
    ));
}
