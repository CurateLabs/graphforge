use super::*;
use crate::tests::test_write_resource;
use crate::write_driver;
use arrow::array::ArrayRef;
use arrow::array::FixedSizeBinaryBuilder;
use arrow::array::Int64Array;
use arrow::array::RecordBatch;
use arrow::array::UInt64Array;
use arrow::datatypes::DataType;
use arrow::datatypes::Schema;
use datafusion::common::DFSchema;
use datafusion::logical_expr::LogicalPlanBuilder;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use graphforge_core::OntologyMode;
use graphforge_ir::IrLiteral;
use graphforge_plan::GraphCreateNode;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use tempfile::TempDir;

#[test]
fn create_writer_covers_typed_edges_properties_recording_and_persisted_lookup() {
    use graphforge_core::uuid::{new_v7, to_bytes};
    use graphforge_plan::{ResolvedEdgeSpec, ResolvedNodeSpec};

    let dir = TempDir::new().unwrap();
    let arrow_schema = Arc::new(Schema::empty());
    let input = RecordBatch::try_new_with_options(
        Arc::clone(&arrow_schema),
        vec![],
        &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(2)),
    )
    .unwrap();
    let nodes = vec![
        ResolvedNodeSpec {
            var: 1,
            label_ids: vec![graphforge_value::EntityTypeId::decode(7).unwrap()],
            label_names: vec!["Person".into()],
            properties: vec![("name".into(), IrLiteral::Str("Alice".into()))],
            computed_properties: vec![],
            is_reference: false,
        },
        ResolvedNodeSpec {
            var: 2,
            label_ids: vec![
                graphforge_value::EntityTypeId::decode(7).unwrap(),
                graphforge_value::EntityTypeId::decode(8).unwrap(),
            ],
            label_names: vec!["Person".into(), "Employee".into()],
            properties: vec![("missing".into(), IrLiteral::Null)],
            computed_properties: vec![],
            is_reference: false,
        },
    ];
    let edge = ResolvedEdgeSpec {
        var: 3,
        src: 1,
        dst: 2,
        rel_type_id: Some(graphforge_value::RelationTypeId::decode(9).unwrap()),
        rel_type_name: Some("KNOWS".into()),
        direction: graphforge_ir::Direction::In,
        properties: vec![("since".into(), IrLiteral::Int(2020))],
        computed_properties: vec![],
    };
    let cfg = CreateConfig {
        semantic_composition_fingerprint: None,
        nodes: nodes.clone(),
        edges: vec![edge.clone()],
        ref_cols: vec![],
        in_df_schema: Arc::new(DFSchema::empty()),
        dir: dir.path().to_path_buf(),
        mode: OntologyMode::Exploratory,
        out_schema: Arc::clone(&arrow_schema),
    };
    validate_edge_specs(&cfg).unwrap();
    assert!(build_ref_by_var(&cfg).is_empty());

    let mut writer =
        graphforge_storage::GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, 1).unwrap();
    let mut recorder = write_driver::CreateRecorder::default();
    let mut tally = CreateTally::default();
    write_batch_creates(
        &cfg,
        &mut writer,
        &input,
        &HashMap::new(),
        CreateExtras {
            recorder: Some(&mut recorder),
            ..CreateExtras::default()
        },
        &mut tally,
    )
    .unwrap();
    assert_eq!((tally.nodes_created, tally.edges_created), (4, 2));
    assert_eq!(tally.properties_set, 4);
    assert_eq!(distinct_created_labels(&nodes, tally.nodes_created), 2);
    assert_eq!(recorder.node_identities(1).unwrap().0.len(), 2);
    writer.flush().unwrap();
    assert_eq!(persisted_node_ids(dir.path()).unwrap().len(), 4);

    let invalid_untyped = CreateConfig {
        semantic_composition_fingerprint: None,
        nodes: nodes.clone(),
        edges: vec![ResolvedEdgeSpec {
            rel_type_id: None,
            rel_type_name: None,
            ..edge.clone()
        }],
        ref_cols: vec![],
        in_df_schema: Arc::new(DFSchema::empty()),
        dir: dir.path().to_path_buf(),
        mode: OntologyMode::Exploratory,
        out_schema: Arc::clone(&arrow_schema),
    };
    assert!(
        validate_edge_specs(&invalid_untyped)
            .unwrap_err()
            .to_string()
            .contains("relationship type")
    );
    let invalid_undirected = CreateConfig {
        semantic_composition_fingerprint: None,
        nodes,
        edges: vec![ResolvedEdgeSpec {
            direction: graphforge_ir::Direction::Undirected,
            ..edge
        }],
        ref_cols: vec![],
        in_df_schema: Arc::new(DFSchema::empty()),
        dir: dir.path().to_path_buf(),
        mode: OntologyMode::Exploratory,
        out_schema: arrow_schema,
    };
    assert!(
        validate_edge_specs(&invalid_undirected)
            .unwrap_err()
            .to_string()
            .contains("undirected")
    );

    let known = new_v7();
    let mut writer =
        graphforge_storage::GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, 2).unwrap();
    writer.create_node_with_labels(known, &[]).unwrap();
    writer.flush().unwrap();
    assert!(
        persisted_node_ids(dir.path())
            .unwrap()
            .contains_key(&to_bytes(&known))
    );
}

#[test]
fn create_identity_and_emit_helpers_reject_every_incomplete_shape() {
    use graphforge_plan::ResolvedNodeSpec;

    let mut uuid_builder = FixedSizeBinaryBuilder::with_capacity(2, 16);
    uuid_builder.append_value([1; 16]).unwrap();
    uuid_builder.append_null();
    let uuid_array = Arc::new(uuid_builder.finish()) as ArrayRef;
    let batch = RecordBatch::try_from_iter([("node_uuid", Arc::clone(&uuid_array))]).unwrap();
    let cols = RefNodeCols {
        var: 4,
        uuid_idx: 0,
        uuid_child_idx: None,
        node_id_idx: Some(0),
    };
    assert_eq!(
        referenced_node_uuid(&batch, &cols, 0).unwrap().as_bytes(),
        &[1; 16]
    );
    assert!(
        referenced_node_uuid(&batch, &cols, 1)
            .unwrap_err()
            .to_string()
            .contains("null")
    );

    let non_struct =
        RecordBatch::try_from_iter([("entity", Arc::new(Int64Array::from(vec![1])) as ArrayRef)])
            .unwrap();
    let nested = RefNodeCols {
        var: 5,
        uuid_idx: 0,
        uuid_child_idx: Some(0),
        node_id_idx: None,
    };
    assert!(
        referenced_node_uuid(&non_struct, &nested, 0)
            .unwrap_err()
            .to_string()
            .contains("not a struct")
    );

    let spec = ResolvedNodeSpec {
        var: 7,
        label_ids: vec![graphforge_value::EntityTypeId::decode(1).unwrap()],
        label_names: vec!["Person".into()],
        properties: vec![],
        computed_properties: vec![],
        is_reference: false,
    };
    let mut out = Vec::new();
    assert_eq!(
        append_created_node_output_cols(
            &spec,
            1,
            &CreateComputed::new(),
            &write_driver::CreateRecorder::default(),
            &mut out,
        )
        .unwrap_err()
        .to_string(),
        "execution error: emit-rows CREATE did not record identities for var 7"
    );

    let mut recorder = write_driver::CreateRecorder::default();
    recorder.record_node(
        7,
        [1; 16],
        1,
        graphforge_value::PrimaryEntityTypeId::decode(1).unwrap(),
    );
    assert!(
        append_created_node_output_cols(&spec, 2, &CreateComputed::new(), &recorder, &mut out,)
            .unwrap_err()
            .to_string()
            .contains("incomplete emitted identities")
    );

    let computed = HashMap::from([(
        7,
        vec![(
            "score".into(),
            Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
        )],
    )]);
    assert!(
        append_created_node_output_cols(&spec, 1, &computed, &recorder, &mut out)
            .unwrap_err()
            .to_string()
            .contains("computed property column")
    );
}

#[test]
fn create_writer_fails_closed_for_unbound_or_unpersisted_references() {
    use graphforge_plan::{ResolvedEdgeSpec, ResolvedNodeSpec};

    let dir = TempDir::new().unwrap();
    let schema = Arc::new(Schema::empty());
    let empty = RecordBatch::try_new_with_options(
        Arc::clone(&schema),
        vec![],
        &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .unwrap();
    let reference = ResolvedNodeSpec {
        var: 1,
        label_ids: vec![],
        label_names: vec![],
        properties: vec![],
        computed_properties: vec![],
        is_reference: true,
    };
    let mut writer =
        graphforge_storage::GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, 1).unwrap();
    let cfg = CreateConfig {
        semantic_composition_fingerprint: None,
        nodes: vec![reference.clone()],
        edges: vec![],
        ref_cols: vec![],
        in_df_schema: Arc::new(DFSchema::empty()),
        dir: dir.path().to_path_buf(),
        mode: OntologyMode::Exploratory,
        out_schema: Arc::clone(&schema),
    };
    let error = write_batch_creates(
        &cfg,
        &mut writer,
        &empty,
        &HashMap::new(),
        CreateExtras::default(),
        &mut CreateTally::default(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("not found in the input schema"));

    let mut uuid_builder = FixedSizeBinaryBuilder::with_capacity(1, 16);
    uuid_builder.append_value([7; 16]).unwrap();
    let uuid = Arc::new(uuid_builder.finish()) as ArrayRef;
    let node_ids = Arc::new(UInt64Array::from(vec![None])) as ArrayRef;
    let bound =
        RecordBatch::try_from_iter([("node_uuid", Arc::clone(&uuid)), ("node_id", node_ids)])
            .unwrap();
    let cols = RefNodeCols {
        var: 1,
        uuid_idx: 0,
        uuid_child_idx: None,
        node_id_idx: Some(1),
    };
    let refs = HashMap::from([(1, &cols)]);
    let error = write_batch_creates(
        &cfg,
        &mut writer,
        &bound,
        &refs,
        CreateExtras::default(),
        &mut CreateTally::default(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("node_id is null"));

    let node_ids = Arc::new(UInt64Array::from(vec![Some(99)])) as ArrayRef;
    let uuid_only =
        RecordBatch::try_from_iter([("node_uuid", uuid), ("node_id", node_ids)]).unwrap();
    let refs = HashMap::from([(1, &cols)]);
    let deleted = HashSet::from([[7; 16]]);
    let error = write_batch_creates(
        &cfg,
        &mut writer,
        &uuid_only,
        &refs,
        CreateExtras {
            deleted: Some(&deleted),
            ..CreateExtras::default()
        },
        &mut CreateTally::default(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("deleted earlier"));

    let edge = ResolvedEdgeSpec {
        var: 3,
        src: 1,
        dst: 2,
        rel_type_id: Some(graphforge_value::RelationTypeId::decode(9).unwrap()),
        rel_type_name: Some("KNOWS".into()),
        direction: graphforge_ir::Direction::Out,
        properties: vec![],
        computed_properties: vec![],
    };
    let mut edge_cfg = CreateConfig {
        semantic_composition_fingerprint: None,
        nodes: vec![],
        edges: vec![edge.clone()],
        ref_cols: vec![],
        in_df_schema: Arc::new(DFSchema::empty()),
        dir: dir.path().to_path_buf(),
        mode: OntologyMode::Exploratory,
        out_schema: Arc::clone(&schema),
    };
    let error = write_batch_creates(
        &edge_cfg,
        &mut writer,
        &empty,
        &HashMap::new(),
        CreateExtras::default(),
        &mut CreateTally::default(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("unbound src"));
    edge_cfg.nodes.push(ResolvedNodeSpec {
        var: 1,
        is_reference: false,
        ..reference
    });
    let error = write_batch_creates(
        &edge_cfg,
        &mut writer,
        &empty,
        &HashMap::new(),
        CreateExtras::default(),
        &mut CreateTally::default(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("unbound dst"));
}

#[test]
fn emit_rows_create_runs_the_writer_and_shapes_created_identity_columns() {
    use graphforge_plan::ResolvedNodeSpec;

    let dir = TempDir::new().unwrap();
    let input_schema = Arc::new(Schema::empty());
    let input = RecordBatch::try_new_with_options(
        Arc::clone(&input_schema),
        vec![],
        &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .unwrap();
    let item = Arc::new(arrow::datatypes::Field::new(
        "item",
        arrow::datatypes::DataType::UInt32,
        false,
    ));
    let output_schema = Arc::new(Schema::new(vec![
        arrow::datatypes::Field::new(
            "node_uuid",
            arrow::datatypes::DataType::FixedSizeBinary(16),
            false,
        ),
        arrow::datatypes::Field::new("node_id", arrow::datatypes::DataType::UInt64, false),
        arrow::datatypes::Field::new("type_id", arrow::datatypes::DataType::UInt32, false),
        arrow::datatypes::Field::new("type_ids", arrow::datatypes::DataType::List(item), false),
        arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
    ]));
    let cfg = CreateConfig {
        semantic_composition_fingerprint: None,
        nodes: vec![ResolvedNodeSpec {
            var: 1,
            label_ids: vec![graphforge_value::EntityTypeId::decode(7).unwrap()],
            label_names: vec!["Person".into()],
            properties: vec![("name".into(), IrLiteral::Str("Ada".into()))],
            computed_properties: vec![],
            is_reference: false,
        }],
        edges: vec![],
        ref_cols: vec![],
        in_df_schema: Arc::new(DFSchema::empty()),
        dir: dir.path().to_path_buf(),
        mode: OntologyMode::Exploratory,
        out_schema: output_schema,
    };
    let mut writer =
        graphforge_storage::GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, 1).unwrap();
    let mut tally = CreateTally::default();

    let emitted = emit_batch_creates(
        &cfg,
        &mut writer,
        &input,
        &CreateComputed::new(),
        &HashMap::new(),
        None,
        &mut tally,
    )
    .unwrap();

    assert_eq!(emitted.num_rows(), 1);
    assert_eq!(emitted.num_columns(), 5);
    assert_eq!((tally.nodes_created, tally.properties_set), (1, 1));
}

#[tokio::test]
async fn graph_create_exec_emits_rows_records_effects_and_preserves_empty_input() {
    use datafusion_datasource::memory::MemorySourceConfig;
    use graphforge_plan::ResolvedNodeSpec;

    let run = |rows: usize| async move {
        let dir = TempDir::new().unwrap();
        // Keep a physical column in the frontier: DataFusion's in-memory
        // source normalizes a zero-column batch to zero rows.
        let input_schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
            "frontier",
            arrow::datatypes::DataType::UInt32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&input_schema),
            vec![Arc::new(arrow::array::UInt32Array::from_iter_values(
                0..rows as u32,
            ))],
        )
        .unwrap();
        let physical =
            MemorySourceConfig::try_new_from_batches(Arc::clone(&input_schema), vec![batch])
                .unwrap();
        let logical = Arc::new(LogicalPlanBuilder::empty(false).build().unwrap());
        let item = Arc::new(arrow::datatypes::Field::new(
            "item",
            arrow::datatypes::DataType::UInt32,
            false,
        ));
        let output_schema = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("frontier", arrow::datatypes::DataType::UInt32, false),
            arrow::datatypes::Field::new(
                "node_uuid",
                arrow::datatypes::DataType::FixedSizeBinary(16),
                false,
            ),
            arrow::datatypes::Field::new("node_id", arrow::datatypes::DataType::UInt64, false),
            arrow::datatypes::Field::new("type_id", arrow::datatypes::DataType::UInt32, false),
            arrow::datatypes::Field::new("type_ids", arrow::datatypes::DataType::List(item), false),
            arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
        ]));
        let node = GraphCreateNode::new_emitting(
            logical,
            vec![ResolvedNodeSpec {
                var: 1,
                label_ids: vec![graphforge_value::EntityTypeId::decode(7).unwrap()],
                label_names: vec!["Person".into()],
                properties: vec![("name".into(), IrLiteral::Str("Ada".into()))],
                computed_properties: vec![],
                is_reference: false,
            }],
            vec![],
            Arc::new(DFSchema::try_from(output_schema.as_ref().clone()).unwrap()),
        );
        let exec = Arc::new(
            GraphCreateExec::new(&node, physical, &test_write_resource(dir.path())).unwrap(),
        );
        assert!(exec.emits_rows());
        let batches = collect(exec.clone(), SessionContext::new().task_ctx())
            .await
            .unwrap();
        let physical: Arc<dyn ExecutionPlan> = exec.clone();
        let discovered = create_tally_in_plan(&physical).unwrap();
        assert_eq!(discovered.nodes_created, exec.effects().nodes_created);
        assert_eq!(discovered.properties_set, exec.effects().properties_set);
        (batches, exec.effects(), dir)
    };

    let (batches, effects, project) = run(1).await;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    assert_eq!((effects.nodes_created, effects.properties_set), (1, 1));
    assert_eq!(persisted_node_ids(project.path()).unwrap().len(), 1);

    let (empty, effects, _) = run(0).await;
    assert_eq!(empty.len(), 1);
    assert_eq!(empty[0].num_rows(), 0);
    assert_eq!(
        (
            effects.nodes_created,
            effects.edges_created,
            effects.properties_set,
            effects.labels_added,
        ),
        (0, 0, 0, 0)
    );
}

#[test]
fn referenced_node_columns_resolve_qualified_unqualified_and_struct_shapes() {
    use datafusion::arrow::datatypes::{Field, Fields};
    use datafusion::common::TableReference;

    let qualified = DFSchema::new_with_metadata(
        vec![
            (
                Some(TableReference::bare("var_2")),
                Arc::new(Field::new(
                    "node_uuid",
                    DataType::FixedSizeBinary(16),
                    false,
                )),
            ),
            (
                Some(TableReference::bare("var_2")),
                Arc::new(Field::new("node_id", DataType::UInt64, false)),
            ),
        ],
        HashMap::new(),
    )
    .unwrap();
    let resolved = RefNodeCols::resolve(&qualified, 2).unwrap();
    assert_eq!(
        (resolved.var, resolved.uuid_idx, resolved.node_id_idx),
        (2, 0, Some(1))
    );
    assert_eq!(resolved.uuid_child_idx, None);

    let unqualified = DFSchema::try_from(Schema::new(vec![
        Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("node_id", DataType::UInt64, false),
    ]))
    .unwrap();
    let resolved = RefNodeCols::resolve_with_alias(&unqualified, 4, "renamed").unwrap();
    assert_eq!(
        (resolved.var, resolved.uuid_idx, resolved.node_id_idx),
        (4, 0, Some(1))
    );

    let node_fields = Fields::from(vec![Field::new(
        "node_uuid",
        DataType::FixedSizeBinary(16),
        false,
    )]);
    let direct_struct = DFSchema::try_from(Schema::new(vec![Field::new(
        "entity",
        DataType::Struct(node_fields.clone()),
        true,
    )]))
    .unwrap();
    let resolved = RefNodeCols::resolve_with_alias(&direct_struct, 6, "entity").unwrap();
    assert_eq!(
        (
            resolved.uuid_idx,
            resolved.uuid_child_idx,
            resolved.node_id_idx
        ),
        (0, Some(0), None)
    );

    let dynamic_struct = DFSchema::try_from(Schema::new(vec![Field::new(
        "dynamic",
        DataType::Struct(Fields::from(vec![Field::new(
            graphforge_value::heterogeneous::payload_field(8),
            DataType::Struct(node_fields),
            true,
        )])),
        true,
    )]))
    .unwrap();
    let resolved = RefNodeCols::resolve_struct_at(&dynamic_struct, 8, 0).unwrap();
    assert_eq!(resolved.uuid_child_idx, None);
    assert!(RefNodeCols::resolve_struct_at(&unqualified, 9, 0).is_none());
    assert!(RefNodeCols::resolve_with_alias(&direct_struct, 10, "missing").is_none());
}
