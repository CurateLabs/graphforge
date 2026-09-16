use arrow::array::{Array, Int64Array, StringArray};
use datafusion::common::TableReference;
use graphforge_ir::Direction;
use graphforge_ir::{
    BinaryOpKind, CaseArm, CreateEdgeSpec, CreateNodeSpec, CreatePattern, PropId, UnaryOpKind,
    VarId,
};

use super::*;

#[test]
fn label_rewrite_reports_missing_type_ids_column_for_add_and_remove() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "seed",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .unwrap();
    let mut frontier = Frontier {
        df_schema: DFSchema::try_from(schema.as_ref().clone()).unwrap(),
        batches: vec![batch],
    };
    let add = frontier
        .add_node_labels(VarId(0), &[EntityTypeId::decode(1).unwrap()], &[true])
        .unwrap_err();
    assert!(
        add.to_string()
            .contains("MERGE label target has no type_ids column")
    );
    let remove = frontier
        .remove_node_labels(VarId(0), &[EntityTypeId::decode(1).unwrap()])
        .unwrap_err();
    assert!(
        remove
            .to_string()
            .contains("REMOVE label target has no type_ids column")
    );
}

#[test]
fn computed_columns_report_missing_and_misaligned_results() {
    let empty: Vec<crate::CreateComputed> = Vec::new();
    assert_eq!(computed_type(&empty, 3, "score"), DataType::Null);
    let error = computed_array(&empty, 0, 3, "score", 1).unwrap_err();
    assert!(error.to_string().contains("was not evaluated"));

    let missing = vec![HashMap::from([(3, Vec::new())])];
    assert_eq!(computed_type(&missing, 3, "score"), DataType::Null);
    let error = computed_array(&missing, 0, 3, "score", 1).unwrap_err();
    assert!(error.to_string().contains("was not evaluated"));

    let values = Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef;
    let computed = vec![HashMap::from([(3, vec![("score".into(), values)])])];
    assert_eq!(computed_type(&computed, 3, "score"), DataType::Int64);
    let error = computed_array(&computed, 0, 3, "score", 1).unwrap_err();
    assert!(error.to_string().contains("2 rows, expected 1"));
    assert_eq!(
        computed_array(&computed, 0, 3, "score", 2).unwrap().len(),
        2
    );
}

#[test]
fn create_recorder_exposes_node_identity_slices() {
    let mut recorder = CreateRecorder::default();
    assert!(recorder.node_identities(8).is_none());
    recorder.record_node(
        8,
        [1; 16],
        9,
        graphforge_value::PrimaryEntityTypeId::decode(10).unwrap(),
    );
    recorder.record_node(
        8,
        [2; 16],
        11,
        graphforge_value::PrimaryEntityTypeId::decode(12).unwrap(),
    );
    let (uuids, node_ids, type_ids) = recorder.node_identities(8).unwrap();
    assert_eq!(uuids, &[[1; 16], [2; 16]]);
    assert_eq!(node_ids, &[9, 11]);
    assert_eq!(
        type_ids.iter().map(|id| id.encode()).collect::<Vec<_>>(),
        &[10, 12]
    );
}

#[test]
fn frontier_property_overlay_validates_batch_alignment() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "seed",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 2]))],
    )
    .unwrap();
    let mut frontier = Frontier {
        df_schema: DFSchema::try_from(schema.as_ref().clone()).unwrap(),
        batches: vec![batch],
    };
    let error = frontier
        .overlay_property(VarId(1), "score", vec![])
        .unwrap_err();
    assert!(error.to_string().contains("batch count"));
    let error = frontier
        .overlay_property(VarId(1), "score", vec![Arc::new(Int64Array::from(vec![1]))])
        .unwrap_err();
    assert!(error.to_string().contains("1 rows, expected 2"));
    frontier
        .overlay_property(
            VarId(1),
            "score",
            vec![Arc::new(Int64Array::from(vec![3, 4]))],
        )
        .unwrap();
    assert_eq!(frontier.batches[0].num_columns(), 2);
    frontier.take_rows(&[1]).unwrap();
    assert_eq!(frontier.num_rows(), 1);
}

#[test]
fn recreating_a_removed_label_token_cancels_its_removal() {
    let dir = tempfile::tempdir().unwrap();
    let mut ctx = StatementWriteContext::new(dir.path(), OntologyMode::Exploratory).unwrap();
    ctx.known_labels.insert(EntityTypeId::decode(7).unwrap());

    ctx.record_removed_label_tokens([EntityTypeId::decode(7).unwrap()]);
    ctx.record_label_tokens([EntityTypeId::decode(7).unwrap()]);

    assert_eq!(ctx.mutation.counters.labels_removed, 0);
    assert_eq!(ctx.mutation.counters.labels_added, 0);
}

#[test]
fn expression_variable_collection_walks_every_composite_shape_once() {
    let mut arena = ExprArena::default();
    let var0 = arena.push(IrExpr::VarRef(VarId(0)));
    let var1 = arena.push(IrExpr::VarRef(VarId(1)));
    let var2 = arena.push(IrExpr::VarRef(VarId(2)));
    let property = arena.push(IrExpr::PropertyAccess {
        base: var0,
        prop: graphforge_value::PropertyId::ontology(PropId(7)).unwrap(),
    });
    let unary = arena.push(IrExpr::UnaryOp {
        op: UnaryOpKind::Neg,
        expr: var1,
    });
    let binary = arena.push(IrExpr::BinaryOp {
        op: BinaryOpKind::Add,
        left: property,
        right: unary,
    });
    let function = arena.push(IrExpr::FunctionCall {
        name: "coalesce".into(),
        args: vec![binary, var2, var2],
    });
    let list = arena.push(IrExpr::ListLiteral(vec![function, var0]));
    let map = arena.push(IrExpr::MapLiteral(vec![
        ("items".into(), list),
        ("fallback".into(), var1),
    ]));
    let case = arena.push(IrExpr::Case {
        operand: Some(var0),
        arms: vec![CaseArm {
            when: var1,
            then: map,
        }],
        else_expr: Some(var2),
    });
    let comprehension = arena.push(IrExpr::ListComprehension {
        loop_var: VarId(9),
        list,
        filter: Some(binary),
        projection: Some(case),
    });

    let mut collected = HashSet::new();
    collect_expr_vars(&arena, comprehension, &mut collected, &mut HashSet::new());
    assert_eq!(collected, HashSet::from([VarId(0), VarId(1), VarId(2)]));

    collect_expr_vars(&arena, comprehension, &mut collected, &mut HashSet::new());
    assert_eq!(
        collected.len(),
        3,
        "revisiting expressions does not duplicate vars"
    );
}

fn create_op() -> GraphOp {
    GraphOp::Create {
        pattern: CreatePattern::default(),
    }
}

fn merge_op() -> GraphOp {
    GraphOp::Merge {
        pattern: CreatePattern::default(),
        on_create: vec![],
        on_match: vec![],
    }
}

fn delete_op() -> GraphOp {
    GraphOp::Delete {
        vars: vec![VarId(0)],
        exprs: vec![],
        detach: false,
    }
}

fn scan_op() -> GraphOp {
    GraphOp::NodeScan {
        var: VarId(0),
        ty: None,
    }
}

fn project_op() -> GraphOp {
    GraphOp::Project {
        items: vec![],
        distinct: false,
    }
}

#[test]
fn split_accepts_prefix_then_writes_in_order() {
    let ops = vec![scan_op(), create_op(), delete_op()];
    let split = split_write_plan(&ops).unwrap();
    assert_eq!(split.prefix_len, 1);
    assert_eq!(split.write_ops, vec![1, 2]);
    assert_eq!(split.read_suffix_start, None);
}

#[test]
fn split_accepts_empty_prefix() {
    // Standalone CREATE … DELETE: the unit-row prefix has zero ops.
    let ops = vec![create_op(), delete_op()];
    let split = split_write_plan(&ops).unwrap();
    assert_eq!(split.prefix_len, 0);
    assert_eq!(split.write_ops, vec![0, 1]);
    assert_eq!(split.read_suffix_start, None);
}

#[test]
fn split_accepts_terminal_return_after_writes() {
    let ops = vec![create_op(), delete_op(), project_op()];
    let split = split_write_plan(&ops).unwrap();
    assert_eq!(split.prefix_len, 0);
    assert_eq!(split.write_ops, vec![0, 1]);
    assert_eq!(split.read_suffix_start, Some(2));
}

#[test]
fn split_tracks_writes_across_intermediate_graph_reads() {
    let ops = vec![scan_op(), create_op(), scan_op(), delete_op()];
    let split = split_write_plan(&ops).unwrap();
    assert_eq!(split.prefix_len, 1);
    assert_eq!(split.write_ops, vec![1, 3]);
    assert_eq!(split.read_suffix_start, None);
}

#[test]
fn split_accepts_merge_mixed_with_other_writes() {
    let ops = vec![scan_op(), merge_op(), delete_op()];
    let split = split_write_plan(&ops).unwrap();
    assert_eq!(split.prefix_len, 1);
    assert_eq!(split.write_ops, vec![1, 2]);
    assert_eq!(split.read_suffix_start, None);
}

#[test]
fn split_errors_on_read_only_plan() {
    let err = split_write_plan(&[scan_op()]).unwrap_err();
    assert!(matches!(err, GfError::Plan(_)), "got {err:?}");
}

fn create_pattern_op(created: VarId, source: Option<VarId>) -> GraphOp {
    let mut pattern = CreatePattern {
        nodes: vec![CreateNodeSpec {
            var: created,
            labels: vec![],
            properties: None,
            is_reference: false,
        }],
        edges: vec![],
    };
    if let Some(source) = source {
        pattern.nodes.push(CreateNodeSpec {
            var: source,
            labels: vec![],
            properties: None,
            is_reference: true,
        });
        pattern.edges.push(CreateEdgeSpec {
            var: VarId(created.0 + 100),
            src: source,
            dst: created,
            rel_type: None,
            direction: Direction::Out,
            properties: None,
        });
    }
    GraphOp::Create { pattern }
}

#[test]
fn create_retention_keeps_only_bindings_read_by_later_clauses() {
    let ops = vec![
        create_pattern_op(VarId(0), None),
        create_pattern_op(VarId(1), Some(VarId(0))),
        create_pattern_op(VarId(2), None),
    ];
    let split = split_write_plan(&ops).unwrap();
    let retention = create_retention_by_write(&ops, &ExprArena::new(), &split).unwrap();

    assert_eq!(retention[&0], HashSet::from([VarId(0)]));
    assert!(retention[&1].is_empty());
    assert!(retention[&2].is_empty());
}

#[test]
fn create_retention_is_disabled_across_a_terminal_projection() {
    let ops = vec![create_pattern_op(VarId(0), None), project_op()];
    let split = split_write_plan(&ops).unwrap();
    assert!(create_retention_by_write(&ops, &ExprArena::new(), &split).is_none());
}

/// A two-batch frontier with one unqualified Int64 column `x`.
fn two_batch_frontier() -> Frontier {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    let b1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 2]))],
    )
    .unwrap();
    let b2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![3]))],
    )
    .unwrap();
    let df_schema = DFSchema::try_from(schema.as_ref().clone()).unwrap();
    Frontier {
        df_schema,
        batches: vec![b1, b2],
    }
}

#[test]
fn frontier_append_node_var_resolves_qualified() {
    let mut f = two_batch_frontier();
    assert_eq!(f.num_rows(), 3);

    let uuids = [[1u8; 16], [2u8; 16], [3u8; 16]];
    f.append_node_var(7, &uuids, &[10, 11, 12], &[0, 0, 0])
        .unwrap();

    // Qualified resolution against the logical schema, positional into
    // the physical batches.
    let qual = TableReference::bare("var_7");
    let idx = f
        .df_schema
        .index_of_column_by_name(Some(&qual), "node_uuid")
        .expect("var_7.node_uuid resolves");
    assert_eq!(idx, 1, "appended right after the prefix column");
    for batch in &f.batches {
        assert_eq!(
            batch.num_columns(),
            5,
            "uuid + node_id + primary and complete label ids added"
        );
    }
    // Per-batch row alignment: batch 0 carries rows 0-1, batch 1 row 2.
    let arr = f.batches[1].column(idx);
    let arr = arr
        .as_any()
        .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(arr.value(0), [3u8; 16]);
}

#[test]
fn frontier_append_edge_var_handles_null_rel_names() {
    let mut f = two_batch_frontier();
    let uuids = [[9u8; 16]; 3];
    f.append_edge_var(
        4,
        &uuids,
        &[[1u8; 16]; 3],
        &[[2u8; 16]; 3],
        &[Some("KNOWS".into()), None, Some("KNOWS".into())],
    )
    .unwrap();

    let qual = TableReference::bare("var_4");
    let uuid_idx = f
        .df_schema
        .index_of_column_by_name(Some(&qual), "edge_uuid")
        .expect("var_4.edge_uuid resolves");
    let name_idx = f
        .df_schema
        .index_of_column_by_name(Some(&qual), "rel_type_name")
        .expect("var_4.rel_type_name resolves");
    assert_eq!((uuid_idx, name_idx), (1, 4));
    let names = f.batches[0].column(name_idx);
    let names = names
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "KNOWS");
    assert!(names.is_null(1));
}

#[test]
fn frontier_append_on_empty_frontier_is_fine() {
    // Zero-row prefix (e.g. a MATCH with no hits): appends must not
    // choke on empty builders.
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    let empty = RecordBatch::new_empty(Arc::clone(&schema));
    let mut f = Frontier {
        df_schema: DFSchema::try_from(schema.as_ref().clone()).unwrap(),
        batches: vec![empty],
    };
    f.append_node_var(1, &[], &[], &[]).unwrap();
    assert_eq!(f.num_rows(), 0);
    assert_eq!(f.batches[0].num_columns(), 5);
}

#[test]
fn terminal_input_materializes_an_empty_batch_when_frontier_has_none() {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    let frontier = Frontier {
        df_schema: DFSchema::try_from(schema.as_ref().clone()).unwrap(),
        batches: vec![],
    };

    let (input_schema, batches) = terminal_input(&frontier);
    assert_eq!(input_schema, schema);
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 0);
}

#[test]
fn terminal_global_count_handles_nullable_columns_aliases_and_fallthrough() {
    use datafusion::functions_aggregate::expr_fn::count;
    use datafusion::logical_expr::{EmptyRelation, LogicalPlan, LogicalPlanBuilder, col, lit};

    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, true)]));
    let df_schema = Arc::new(DFSchema::try_from(schema.as_ref().clone()).unwrap());
    let frontier = Frontier {
        df_schema: df_schema.as_ref().clone(),
        batches: vec![
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(StringArray::from(vec![
                    Some("a"),
                    None,
                    Some("b"),
                ]))],
            )
            .unwrap(),
        ],
    };
    let input = LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: false,
        schema: df_schema,
    });
    let aggregate = LogicalPlanBuilder::from(input)
        .aggregate(
            Vec::<DfExpr>::new(),
            vec![count(col("x")).alias("present"), count(lit(1_i64))],
        )
        .unwrap()
        .build()
        .unwrap();
    let batch = terminal_global_count(&aggregate, &frontier)
        .unwrap()
        .unwrap();
    let present = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let all = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!((present.value(0), all.value(0)), (2, 3));

    let grouped = LogicalPlanBuilder::from(LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: false,
        schema: Arc::new(DFSchema::try_from(schema.as_ref().clone()).unwrap()),
    }))
    .aggregate(vec![col("x")], vec![count(lit(1_i64))])
    .unwrap()
    .build()
    .unwrap();
    assert!(
        terminal_global_count(&grouped, &frontier)
            .unwrap()
            .is_none()
    );
    assert!(
        terminal_global_count(
            &LogicalPlanBuilder::empty(false).build().unwrap(),
            &frontier
        )
        .unwrap()
        .is_none()
    );
}
