use super::*;
use crate::SideEffects;
use crate::create_exec::persisted_node_ids;
use crate::mutation;
use arrow::array::ArrayRef;
use arrow::array::FixedSizeBinaryBuilder;
use arrow::array::RecordBatch;
use arrow::datatypes::DataType;
use arrow::datatypes::Schema;
use datafusion::common::DFSchema;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::lit;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::PlanProperties;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::execution_plan::Boundedness;
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::prelude::SessionContext;
use graphforge_core::OntologyMode;
use graphforge_plan::GraphDeleteNode;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

#[test]
fn set_and_remove_batch_contracts_route_rows_and_skip_null_identities() {
    let mut uuid_builder = FixedSizeBinaryBuilder::with_capacity(3, 16);
    uuid_builder.append_value([1; 16]).unwrap();
    uuid_builder.append_null();
    uuid_builder.append_value([2; 16]).unwrap();
    let batch = RecordBatch::try_from_iter([
        ("node_uuid", Arc::new(uuid_builder.finish()) as ArrayRef),
        (
            "type_id",
            Arc::new(arrow::array::UInt32Array::from(vec![7, 7, 8])) as ArrayRef,
        ),
    ])
    .unwrap();
    let df_schema = DFSchema::try_from(batch.schema().as_ref().clone()).unwrap();
    let target = WriteCol {
        prop_name: "score".into(),
        uuid_idx: 0,
        is_edge: false,
        type_id_idx: Some(1),
        rel_name_idx: None,
    };
    let logical_value = lit(42_i64);
    let physical_value =
        create_physical_expr(&logical_value, &df_schema, &ExecutionProps::new()).unwrap();
    let type_map = HashMap::from([
        (
            graphforge_value::EntityTypeId::decode(7).unwrap(),
            "Person".into(),
        ),
        (
            graphforge_value::EntityTypeId::decode(8).unwrap(),
            "Employee".into(),
        ),
    ]);

    let mut set = SetAccumulator::default();
    accumulate_set_batch(
        &batch,
        &[(target.clone(), logical_value)],
        &[physical_value],
        OntologyMode::Strict,
        &type_map,
        &mut set,
    )
    .unwrap();
    assert_eq!(set.nodes["Person"].len(), 1);
    assert_eq!(set.nodes["Employee"].len(), 1);
    assert!(!set.nodes.values().any(|rows| rows.contains_key(&[0; 16])));

    let mut remove = RemoveAccumulator::default();
    accumulate_remove_batch(
        &batch,
        &[target],
        OntologyMode::Strict,
        &type_map,
        &mut remove,
    )
    .unwrap();
    assert_eq!(remove.nodes["Person"].len(), 1);
    assert_eq!(remove.nodes["Employee"].len(), 1);
}

#[tokio::test]
async fn delete_exec_enforces_bound_edge_and_detach_semantics() {
    use arrow::datatypes::Field;
    use datafusion_datasource::memory::MemorySourceConfig;
    use graphforge_core::uuid::{new_v7, to_bytes};

    let run = |detach: bool, include_edge: bool| async move {
        let dir = TempDir::new().unwrap();
        let node = new_v7();
        let other = new_v7();
        let edge = new_v7();
        let mut writer =
            graphforge_storage::GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, 1)
                .unwrap();
        writer
            .create_node(node, graphforge_value::EntityTypeId::decode(1).unwrap())
            .unwrap();
        writer
            .create_node(other, graphforge_value::EntityTypeId::decode(1).unwrap())
            .unwrap();
        writer.create_edge(edge, "KNOWS", &node, &other).unwrap();
        writer.flush().unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("edge_uuid", DataType::FixedSizeBinary(16), false),
        ]));
        let mut nodes = FixedSizeBinaryBuilder::with_capacity(1, 16);
        nodes.append_value(to_bytes(&node)).unwrap();
        let mut edges = FixedSizeBinaryBuilder::with_capacity(1, 16);
        edges.append_value(to_bytes(&edge)).unwrap();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(nodes.finish()), Arc::new(edges.finish())],
        )
        .unwrap();
        let input = MemorySourceConfig::try_new_from_batches(schema, vec![batch]).unwrap();
        let summary = GraphDeleteNode::summary_schema();
        let exec: Arc<dyn ExecutionPlan> = Arc::new(GraphDeleteExec {
            mutation_health: mutation::MutationHealth::default(),
            input,
            cols: if include_edge {
                vec![
                    DeleteCol {
                        uuid_idx: 0,
                        is_edge: false,
                    },
                    DeleteCol {
                        uuid_idx: 1,
                        is_edge: true,
                    },
                ]
            } else {
                vec![DeleteCol {
                    uuid_idx: 0,
                    is_edge: false,
                }]
            },
            detach,
            dir: dir.path().to_path_buf(),
            schema: Arc::clone(&summary),
            props: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(summary),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        });
        (collect(exec, SessionContext::new().task_ctx()).await, dir)
    };

    let (error, _) = run(false, false).await;
    assert!(
        error
            .unwrap_err()
            .to_string()
            .contains("still has relationships")
    );

    let (batches, project) = run(false, true).await;
    let effects = SideEffects::from_summary(&batches.unwrap());
    assert_eq!(
        (effects.nodes_deleted, effects.relationships_deleted),
        (1, 1)
    );
    assert_eq!(persisted_node_ids(project.path()).unwrap().len(), 1);

    let (batches, project) = run(true, false).await;
    let effects = SideEffects::from_summary(&batches.unwrap());
    assert_eq!(
        (effects.nodes_deleted, effects.relationships_deleted),
        (1, 1)
    );
    assert_eq!(persisted_node_ids(project.path()).unwrap().len(), 1);
}
