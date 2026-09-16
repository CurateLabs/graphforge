use crate::mutation::WriteCounters;
use arrow::array::{Array, FixedSizeBinaryBuilder, Int64Array, StringArray, UInt32Array};
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::UInt32Type;
use datafusion::common::TableReference;
use graphforge_core::OntologyMode;
use graphforge_ir::ExprArena;
use graphforge_ir::IrExpr;
use graphforge_ir::{IrLiteral, PropId, RemovePropItem, SetPropItem, VarId};
use graphforge_rel::GraphPlanLowerer;

use super::*;

#[test]
fn map_replacement_math_and_pending_routing_are_exact() {
    let present = HashSet::from(["a".to_owned(), "b".to_owned(), "c".to_owned()]);
    let updates = HashMap::from([
        ("a".to_owned(), IrLiteral::Int(1)),
        ("d".to_owned(), IrLiteral::Int(4)),
    ]);
    let nulls = HashSet::from(["b".to_owned(), "z".to_owned()]);
    let (removals, replaced) = map_removals(false, &present, &updates, &nulls);
    assert_eq!(removals, HashSet::from(["b".to_owned()]));
    assert_eq!(replaced, 2);
    let (removals, replaced) = map_removals(true, &present, &updates, &nulls);
    assert_eq!(removals, HashSet::from(["b".to_owned(), "c".to_owned()]));
    assert_eq!(replaced, 3);

    let dir = tempfile::tempdir().unwrap();
    let mut ctx = StatementWriteContext::new(dir.path(), OntologyMode::Exploratory).unwrap();
    let node = [7_u8; 16];
    let uuid = graphforge_core::uuid::from_bytes(&node);
    ctx.writer.create_node_with_labels(uuid, &[]).unwrap();
    apply_map_updates(&mut ctx, false, &node, "Person", updates.clone()).unwrap();
    remove_map_complement(&mut ctx, false, &node, "Person", &removals);

    let committed = [8_u8; 16];
    apply_map_updates(&mut ctx, false, &committed, "Person", updates).unwrap();
    remove_map_complement(&mut ctx, false, &committed, "Person", &removals);
    let set_nodes = &ctx.set_acc.nodes["Person"];
    let remove_nodes = &ctx.remove_acc.nodes["Person"];
    assert!(set_nodes.contains_key(&committed));
    assert!(!set_nodes.contains_key(&node));
    assert!(remove_nodes.contains_key(&committed));
    assert!(!remove_nodes.contains_key(&node));
}

#[test]
fn delete_scalar_accepts_null_and_rejects_scalar_values() {
    let mut nodes = HashSet::new();
    let mut edges = HashSet::new();
    collect_delete_scalar(&ScalarValue::Null, &mut nodes, &mut edges).unwrap();
    assert!(nodes.is_empty() && edges.is_empty());
    let error =
        collect_delete_scalar(&ScalarValue::Int64(Some(1)), &mut nodes, &mut edges).unwrap_err();
    assert!(error.to_string().contains("node, relationship, or path"));
}

fn nullable_uuids(values: &[Option<[u8; 16]>]) -> ArrayRef {
    let mut builder = FixedSizeBinaryBuilder::with_capacity(values.len(), 16);
    for value in values {
        match value {
            Some(value) => builder.append_value(value).unwrap(),
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

fn write_frontier(is_edge: bool) -> Frontier {
    let uuid_name = if is_edge { "edge_uuid" } else { "node_uuid" };
    let mut fields = vec![Field::new(uuid_name, DataType::FixedSizeBinary(16), true)];
    let mut columns = vec![nullable_uuids(&[Some([7; 16]), Some([7; 16]), None])];
    if is_edge {
        fields.push(Field::new("rel_type_name", DataType::Utf8, false));
        columns.push(Arc::new(StringArray::from(vec!["KNOWS"; 3])));
    } else {
        fields.push(Field::new("type_id", DataType::UInt32, false));
        columns.push(Arc::new(UInt32Array::from(vec![3; 3])));
    }
    fields.push(Field::new("score", DataType::Int64, true));
    columns.push(Arc::new(Int64Array::from(vec![Some(1), Some(1), Some(1)])));
    let schema = Arc::new(Schema::new(fields.clone()));
    let qualifier = TableReference::bare("var_1");
    Frontier {
        df_schema: DFSchema::new_with_metadata(
            fields
                .into_iter()
                .map(|field| (Some(qualifier.clone()), Arc::new(field)))
                .collect(),
            HashMap::new(),
        )
        .unwrap(),
        batches: vec![RecordBatch::try_new(schema, columns).unwrap()],
    }
}

fn phase_env<'a>(
    lowerer: &'a GraphPlanLowerer,
    exprs: &'a ExprArena,
    dir: &'a Path,
    params: &'a HashMap<String, IrLiteral>,
) -> PhaseEnv<'a> {
    PhaseEnv {
        inventory: None,
        lowerer,
        exprs,
        dir,
        mode: OntologyMode::Exploratory,
        params,
        type_map: HashMap::new(),
        hydration: crate::path_hydration::HydrationResource::new(
            std::sync::Arc::new(crate::read_resource::GraphReadContext {
                health: crate::mutation::MutationHealth::default(),
                dir: dir.to_path_buf(),
                mode: OntologyMode::Exploratory,
                ontology: None,
                catalog: std::sync::Arc::new(
                    graphforge_storage::GraphCatalog::open(
                        dir,
                        None,
                        &graphforge_ir::RuntimeCatalog::new(),
                    )
                    .unwrap(),
                ),
            }),
            std::sync::Arc::new(datafusion::execution::memory_pool::UnboundedMemoryPool::default()),
        ),
    }
}

#[test]
fn ambiguous_edge_owner_refuses_before_accumulation_or_publication() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer =
        graphforge_storage::GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, 1).unwrap();
    for route in ["KNOWS", "_exploratory"] {
        writer
            .set_edge_properties(
                &graphforge_core::uuid::Uuid::from_bytes([7; 16]),
                Some(route),
                HashMap::from([("score".into(), IrLiteral::Int(1))]),
            )
            .unwrap();
    }
    writer.flush().unwrap();
    let before = graphforge_storage::capture_graph_files(dir.path())
        .unwrap()
        .0;
    let catalog = graphforge_storage::GraphCatalog::open(
        dir.path(),
        None,
        &graphforge_ir::RuntimeCatalog::new(),
    )
    .unwrap();
    let lowerer = GraphPlanLowerer::new_for_writes(
        &graphforge_storage::lowering_snapshot(Some(&catalog), Some(dir.path())).unwrap(),
        None,
        OntologyMode::Exploratory,
    )
    .unwrap();
    let mut exprs = ExprArena::new();
    let value = exprs.push(IrExpr::Literal(IrLiteral::Int(42)));
    let params = HashMap::new();
    let env = phase_env(&lowerer, &exprs, dir.path(), &params);
    let mut var_map = VarMap::new();
    var_map.insert(VarId(1), "var_1");
    for remove in [false, true] {
        let mut ctx = StatementWriteContext::new(dir.path(), OntologyMode::Exploratory).unwrap();
        let mut frontier = write_frontier(true);
        let result = if remove {
            run_remove_phase(
                &env,
                &[RemovePropItem {
                    target: VarId(1),
                    prop: graphforge_value::PropertyId::ontology(PropId(9)).unwrap(),
                    prop_name: "score".into(),
                }],
                &mut frontier,
                &mut ctx,
            )
        } else {
            run_set_phase(
                &env,
                &[SetPropItem {
                    target: VarId(1),
                    prop: graphforge_value::PropertyId::ontology(PropId(9)).unwrap(),
                    prop_name: "score".into(),
                    value,
                }],
                &mut frontier,
                &var_map,
                &mut ctx,
            )
        };
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("edge property owner is ambiguous")
        );
        assert!(ctx.set_acc.nodes.is_empty() && ctx.set_acc.edges.is_empty());
        assert!(ctx.remove_acc.nodes.is_empty() && ctx.remove_acc.edges.is_empty());
        assert_eq!(ctx.mutation.counters, WriteCounters::default());
        let after = graphforge_storage::capture_graph_files(dir.path())
            .unwrap()
            .0;
        assert_eq!(before, after);
    }
}

#[test]
fn set_accumulates_nodes_and_edges_once_across_duplicate_and_null_rows() {
    for is_edge in [false, true] {
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
            OntologyMode::Exploratory,
        )
        .unwrap();
        let mut exprs = ExprArena::new();
        let value = exprs.push(IrExpr::Literal(IrLiteral::Int(42)));
        let params = HashMap::new();
        let env = phase_env(&lowerer, &exprs, dir.path(), &params);
        let mut frontier = write_frontier(is_edge);
        let mut var_map = VarMap::new();
        var_map.insert(VarId(1), "var_1");
        let mut ctx = StatementWriteContext::new(dir.path(), OntologyMode::Exploratory).unwrap();

        run_set_phase(
            &env,
            &[SetPropItem {
                target: VarId(1),
                prop: graphforge_value::PropertyId::ontology(PropId(9)).unwrap(),
                prop_name: "score".into(),
                value,
            }],
            &mut frontier,
            &var_map,
            &mut ctx,
        )
        .unwrap();

        let accumulated = if is_edge {
            &ctx.set_acc.edges["KNOWS"]
        } else {
            &ctx.set_acc.nodes["_untyped"]
        };
        assert_eq!(
            accumulated[&[7; 16]]["score"],
            IrLiteral::Int(42),
            "wrong accumulated value for is_edge={is_edge}"
        );
        assert_eq!(accumulated.len(), 1, "duplicate rows must coalesce");
        assert_eq!(ctx.mutation.counters.properties_set, 1);
        assert_eq!(ctx.mutation.counters.properties_removed, 1);
    }
}

#[test]
fn remove_accumulates_nodes_and_edges_and_ignores_null_optional_rows() {
    for is_edge in [false, true] {
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
            OntologyMode::Exploratory,
        )
        .unwrap();
        let exprs = ExprArena::new();
        let params = HashMap::new();
        let env = phase_env(&lowerer, &exprs, dir.path(), &params);
        let mut frontier = write_frontier(is_edge);
        frontier.batches[0] = RecordBatch::try_new(
            Arc::clone(frontier.batches[0].schema_ref()),
            vec![
                nullable_uuids(&[Some([7; 16]), Some([8; 16]), None]),
                Arc::clone(frontier.batches[0].column(1)),
                Arc::clone(frontier.batches[0].column(2)),
            ],
        )
        .unwrap();
        let mut ctx = StatementWriteContext::new(dir.path(), OntologyMode::Exploratory).unwrap();

        run_remove_phase(
            &env,
            &[RemovePropItem {
                target: VarId(1),
                prop: graphforge_value::PropertyId::ontology(PropId(9)).unwrap(),
                prop_name: "score".into(),
            }],
            &mut frontier,
            &mut ctx,
        )
        .unwrap();

        let accumulated = if is_edge {
            &ctx.remove_acc.edges["KNOWS"]
        } else {
            &ctx.remove_acc.nodes["_untyped"]
        };
        assert_eq!(accumulated[&[7; 16]], HashSet::from(["score".into()]));
        assert_eq!(accumulated[&[8; 16]], HashSet::from(["score".into()]));
        assert_eq!(accumulated.len(), 2);
        assert_eq!(ctx.mutation.counters.properties_removed, 2);
    }
}

#[test]
fn set_and_remove_reject_malformed_identity_and_edge_routing_columns() {
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
        OntologyMode::Exploratory,
    )
    .unwrap();
    let mut exprs = ExprArena::new();
    let value = exprs.push(IrExpr::Literal(IrLiteral::Int(42)));
    let params = HashMap::new();
    let env = phase_env(&lowerer, &exprs, dir.path(), &params);
    let mut var_map = VarMap::new();
    var_map.insert(VarId(1), "var_1");

    let mut malformed_uuid = write_frontier(false);
    malformed_uuid.batches[0] = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("node_uuid", DataType::Int64, false),
            Field::new("type_id", DataType::UInt32, false),
            Field::new("score", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::clone(malformed_uuid.batches[0].column(1)),
            Arc::clone(malformed_uuid.batches[0].column(2)),
        ],
    )
    .unwrap();
    let mut ctx = StatementWriteContext::new(dir.path(), OntologyMode::Exploratory).unwrap();
    let err = run_set_phase(
        &env,
        &[SetPropItem {
            target: VarId(1),
            prop: graphforge_value::PropertyId::ontology(PropId(9)).unwrap(),
            prop_name: "score".into(),
            value,
        }],
        &mut malformed_uuid,
        &var_map,
        &mut ctx,
    )
    .unwrap_err();
    assert_eq!(
        err.to_string(),
        "execution error: expected FixedSizeBinary(16) at column 0"
    );
    assert!(ctx.set_acc.nodes.is_empty());
    assert!(ctx.set_acc.edges.is_empty());
    assert_eq!(ctx.mutation.counters, WriteCounters::default());

    let mut malformed_route = write_frontier(true);
    malformed_route.batches[0] = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("edge_uuid", DataType::FixedSizeBinary(16), true),
            Field::new("rel_type_name", DataType::Int64, false),
            Field::new("score", DataType::Int64, true),
        ])),
        vec![
            Arc::clone(malformed_route.batches[0].column(0)),
            Arc::new(Int64Array::from(vec![1, 1, 1])),
            Arc::clone(malformed_route.batches[0].column(2)),
        ],
    )
    .unwrap();
    let mut ctx = StatementWriteContext::new(dir.path(), OntologyMode::Exploratory).unwrap();
    let err = run_remove_phase(
        &env,
        &[RemovePropItem {
            target: VarId(1),
            prop: graphforge_value::PropertyId::ontology(PropId(9)).unwrap(),
            prop_name: "score".into(),
        }],
        &mut malformed_route,
        &mut ctx,
    )
    .unwrap_err();
    assert_eq!(
        err.to_string(),
        "execution error: rel_type_name is not a string column"
    );
    assert!(ctx.remove_acc.nodes.is_empty());
    assert!(ctx.remove_acc.edges.is_empty());
    assert_eq!(ctx.mutation.counters, WriteCounters::default());

    let mut pending_ctx =
        StatementWriteContext::new(dir.path(), OntologyMode::Exploratory).unwrap();
    let src = graphforge_core::uuid::new_v7();
    let dst = graphforge_core::uuid::new_v7();
    let edge = graphforge_core::uuid::Uuid::from_bytes([7; 16]);
    pending_ctx
        .writer
        .create_node(
            src,
            EntityTypeId::ontology(graphforge_core::TypeId(1)).unwrap(),
        )
        .unwrap();
    pending_ctx
        .writer
        .create_node(
            dst,
            EntityTypeId::ontology(graphforge_core::TypeId(1)).unwrap(),
        )
        .unwrap();
    pending_ctx
        .writer
        .create_edge(edge, "KNOWS", &src, &dst)
        .unwrap();
    pending_ctx
        .writer
        .merge_pending_edge_props(
            &[7; 16],
            Some("KNOWS"),
            HashMap::from([("score".into(), IrLiteral::Int(1))]),
        )
        .unwrap();
    let err = run_remove_phase(
        &env,
        &[RemovePropItem {
            target: VarId(1),
            prop: graphforge_value::PropertyId::ontology(PropId(9)).unwrap(),
            prop_name: "score".into(),
        }],
        &mut malformed_route,
        &mut pending_ctx,
    )
    .unwrap_err();
    assert_eq!(
        err.to_string(),
        "execution error: rel_type_name is not a string column"
    );
    assert_eq!(pending_ctx.mutation.counters, WriteCounters::default());
}

#[test]
fn write_kind_and_pending_remove_paths_fail_closed_and_route_exactly() {
    let qualifier = TableReference::bare("var_1");
    let unknown = DFSchema::empty();
    let error = resolve_kind(&unknown, VarId(1), "SET").unwrap_err();
    assert!(error.to_string().contains("must be bound"));
    let untyped_edge = DFSchema::new_with_metadata(
        vec![(
            Some(qualifier),
            Arc::new(Field::new(
                "edge_uuid",
                DataType::FixedSizeBinary(16),
                false,
            )),
        )],
        HashMap::new(),
    )
    .unwrap();
    let error = resolve_kind(&untyped_edge, VarId(1), "REMOVE").unwrap_err();
    assert!(error.to_string().contains("known relation type"));

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
        OntologyMode::Exploratory,
    )
    .unwrap();
    let exprs = ExprArena::new();
    let params = HashMap::new();
    let env = phase_env(&lowerer, &exprs, dir.path(), &params);
    for is_edge in [false, true] {
        let mut frontier = write_frontier(is_edge);
        let mut ctx = StatementWriteContext::new(dir.path(), OntologyMode::Exploratory).unwrap();
        if is_edge {
            let src = graphforge_core::uuid::new_v7();
            let dst = graphforge_core::uuid::new_v7();
            ctx.writer
                .create_node(
                    src,
                    EntityTypeId::ontology(graphforge_core::TypeId(1)).unwrap(),
                )
                .unwrap();
            ctx.writer
                .create_node(
                    dst,
                    EntityTypeId::ontology(graphforge_core::TypeId(1)).unwrap(),
                )
                .unwrap();
            ctx.writer
                .create_edge(
                    graphforge_core::uuid::Uuid::from_bytes([7; 16]),
                    "KNOWS",
                    &src,
                    &dst,
                )
                .unwrap();
            ctx.writer
                .merge_pending_edge_props(
                    &[7; 16],
                    Some("KNOWS"),
                    HashMap::from([("score".into(), IrLiteral::Int(1))]),
                )
                .unwrap();
        } else {
            ctx.writer
                .create_node(
                    graphforge_core::uuid::Uuid::from_bytes([7; 16]),
                    EntityTypeId::ontology(graphforge_core::TypeId(1)).unwrap(),
                )
                .unwrap();
            ctx.writer
                .merge_pending_node_props(
                    &[7; 16],
                    None,
                    HashMap::from([("score".into(), IrLiteral::Int(1))]),
                )
                .unwrap();
        }
        run_remove_phase(
            &env,
            &[RemovePropItem {
                target: VarId(1),
                prop: graphforge_value::PropertyId::ontology(PropId(9)).unwrap(),
                prop_name: "score".into(),
            }],
            &mut frontier,
            &mut ctx,
        )
        .unwrap();
        assert_eq!(ctx.mutation.counters.properties_removed, 2);
    }
}

#[test]
fn label_phase_routes_pending_cancellation_and_deleted_entities() {
    let make_frontier = || {
        let fields = vec![
            Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
            Field::new(
                "type_ids",
                DataType::List(Arc::new(Field::new("item", DataType::UInt32, false))),
                false,
            ),
        ];
        let schema = Arc::new(Schema::new(fields.clone()));
        let labels = ListArray::from_iter_primitive::<UInt32Type, _, _>([
            Some(vec![Some(1)]),
            Some(vec![Some(1)]),
        ]);
        let labels = ListArray::new(
            Arc::new(Field::new("item", DataType::UInt32, false)),
            labels.offsets().clone(),
            labels.values().clone(),
            labels.nulls().cloned(),
        );
        let qualifier = TableReference::bare("var_1");
        Frontier {
            df_schema: DFSchema::new_with_metadata(
                fields
                    .into_iter()
                    .map(|field| (Some(qualifier.clone()), Arc::new(field)))
                    .collect(),
                HashMap::new(),
            )
            .unwrap(),
            batches: vec![
                RecordBatch::try_new(
                    schema,
                    vec![
                        nullable_uuids(&[Some([7; 16]), Some([7; 16])]),
                        Arc::new(labels),
                    ],
                )
                .unwrap(),
            ],
        }
    };
    let dir = tempfile::tempdir().unwrap();
    let add = graphforge_ir::LabelItem {
        target: VarId(1),
        labels: vec![EntityTypeId::ontology(graphforge_core::TypeId(2)).unwrap()],
    };
    let remove = graphforge_ir::LabelItem {
        target: VarId(1),
        labels: vec![EntityTypeId::ontology(graphforge_core::TypeId(1)).unwrap()],
    };

    let mut pending = StatementWriteContext::new(dir.path(), OntologyMode::Exploratory).unwrap();
    pending
        .writer
        .create_node(
            graphforge_core::uuid::Uuid::from_bytes([7; 16]),
            EntityTypeId::ontology(graphforge_core::TypeId(1)).unwrap(),
        )
        .unwrap();
    run_label_phase(&[add.clone()], true, &mut make_frontier(), &mut pending).unwrap();
    run_label_phase(&[remove.clone()], false, &mut make_frontier(), &mut pending).unwrap();

    let mut persisted = StatementWriteContext::new(dir.path(), OntologyMode::Exploratory).unwrap();
    persisted
        .label_removals
        .insert([7; 16], HashSet::from([EntityTypeId::decode(2).unwrap()]));
    run_label_phase(&[add.clone()], true, &mut make_frontier(), &mut persisted).unwrap();
    assert!(persisted.label_removals[&[7; 16]].is_empty());
    persisted
        .label_additions
        .insert([7; 16], HashSet::from([EntityTypeId::decode(1).unwrap()]));
    run_label_phase(
        &[remove.clone()],
        false,
        &mut make_frontier(),
        &mut persisted,
    )
    .unwrap();
    assert!(persisted.label_additions[&[7; 16]].is_empty());

    let mut deleted = StatementWriteContext::new(dir.path(), OntologyMode::Exploratory).unwrap();
    deleted.deleted.insert([7; 16]);
    let error = run_label_phase(&[add], true, &mut make_frontier(), &mut deleted).unwrap_err();
    assert!(error.to_string().contains("deleted in this statement"));
}
