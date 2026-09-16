use super::*;
use crate::adjacency::PersistentAdjacencyProvider;
use crate::create_exec::GraphCreateExec;
use crate::expand_exec::ExpandConfig;
use crate::expand_exec::build_edge_list_column;
use crate::row_exec::OptionalConfig;
use crate::row_exec::optional_join;
use crate::session::ExecutionSession;
use crate::session::GraphForgeExtensionPlanner;
use crate::write_exec::GraphDeleteExec;
use crate::write_exec::GraphRemoveExec;
use crate::write_exec::GraphSetExec;
use arrow::array::ArrayRef;
use arrow::array::RecordBatch;
use arrow::array::UInt64Array;
use arrow::datatypes::DataType;
use arrow::datatypes::Schema;
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::LogicalPlanBuilder;
use datafusion::logical_expr::UserDefinedLogicalNode;
use datafusion::logical_expr::lit;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_planner::DefaultPhysicalPlanner;
use datafusion::physical_planner::ExtensionPlanner;
use datafusion::prelude::SessionContext;
use graphforge_core::OntologyMode;
use graphforge_ir::Direction;
use graphforge_ir::RuntimeCatalog;
use graphforge_plan::DeleteTarget;
use graphforge_plan::GraphCreateNode;
use graphforge_plan::GraphDeleteNode;
use graphforge_plan::GraphRemoveNode;
use graphforge_plan::GraphSetNode;
use graphforge_plan::OptionalMatchNode;
use graphforge_plan::RemoveTarget;
use graphforge_plan::SetTarget;
use graphforge_plan::VarLenExpandNode;
use graphforge_rel::GraphPlanLowerer;
use graphforge_storage::GraphCatalog;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

pub(super) fn test_write_resource(dir: &std::path::Path) -> write_resource::BoundWriteResource {
    let catalog = GraphCatalog::open(dir, None, &RuntimeCatalog::new()).unwrap();
    ExecutionSession::new_with_target(catalog, None, dir.to_path_buf(), OntologyMode::Exploratory)
        .unwrap()
        .write_resource()
        .unwrap()
}

pub(super) fn empty_write_input() -> (Arc<LogicalPlan>, Arc<dyn ExecutionPlan>) {
    let logical = Arc::new(LogicalPlanBuilder::empty(false).build().unwrap());
    let physical: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::new(
        logical.schema().as_arrow().clone(),
    )));
    (logical, physical)
}

#[test]
fn direct_write_constructors_reject_incompatible_logical_contracts() {
    let dir = TempDir::new().unwrap();
    let mut runtime = RuntimeCatalog::new();
    runtime.intern_label("DestinationMeaning").unwrap();
    let catalog = GraphCatalog::open(dir.path(), None, &runtime).unwrap();
    let mut contract = GraphPlanLowerer::new(
        Some(&graphforge_storage::lowering_snapshot(Some(&catalog), None).unwrap()),
        None,
    )
    .unwrap()
    .read_contract();
    assert_eq!(contract.labels.len(), 1);
    contract.labels[0].1 = "DifferentMeaning".into();
    let contract = Some(contract);
    let session = ExecutionSession::new_with_target(
        catalog,
        None,
        dir.path().to_path_buf(),
        OntologyMode::Exploratory,
    )
    .unwrap();
    let resource = session.write_resource().unwrap();
    let before = std::fs::read_dir(dir.path()).unwrap().count();
    let (input, physical) = empty_write_input();
    let errors = [
        GraphCreateExec::new(
            &GraphCreateNode::new(input.clone(), vec![], vec![])
                .with_write_contract(contract.clone()),
            physical.clone(),
            &resource,
        )
        .unwrap_err(),
        GraphDeleteExec::new(
            &GraphDeleteNode::new(input.clone(), vec![], false)
                .with_write_contract(contract.clone()),
            physical.clone(),
            &resource,
        )
        .unwrap_err(),
        GraphSetExec::new(
            &GraphSetNode::new(input.clone(), vec![]).with_write_contract(contract.clone()),
            physical.clone(),
            &resource,
        )
        .unwrap_err(),
        GraphRemoveExec::new(
            &GraphRemoveNode::new(input, vec![]).with_write_contract(contract),
            physical,
            &resource,
        )
        .unwrap_err(),
    ];
    for error in errors {
        assert!(
            error.to_string().contains("GF_WRITE_RESOURCE_INCOMPATIBLE"),
            "{error}"
        );
    }
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), before);
}

#[test]
fn write_summary_reads_named_counters_and_ignores_missing_or_empty_rows() {
    let batch = RecordBatch::try_from_iter([
        (
            "properties_set",
            Arc::new(UInt64Array::from(vec![7])) as ArrayRef,
        ),
        (
            "nodes_created",
            Arc::new(UInt64Array::from(vec![2])) as ArrayRef,
        ),
        (
            "edges_deleted",
            Arc::new(UInt64Array::from(vec![3])) as ArrayRef,
        ),
    ])
    .unwrap();

    assert_eq!(
        SideEffects::from_summary(&[batch]),
        SideEffects {
            nodes_created: 2,
            relationships_deleted: 3,
            properties_set: 7,
            ..SideEffects::default()
        }
    );
    assert_eq!(SideEffects::from_summary(&[]), SideEffects::default());
    assert_eq!(
        SideEffects::from_summary(&[RecordBatch::try_from_iter([(
            "nodes_created",
            Arc::new(UInt64Array::from(Vec::<u64>::new())) as ArrayRef,
        )])
        .unwrap()]),
        SideEffects::default()
    );
}

#[test]
fn wave11_low_level_expand_and_optional_schema_guards_fail_closed() {
    use arrow::array::{FixedSizeBinaryArray, Int64Array, StringArray};
    use arrow::datatypes::Field;

    let int_schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let ints = RecordBatch::try_new(
        int_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .unwrap();
    assert!(
        u64_column(&ints, 0)
            .unwrap_err()
            .to_string()
            .contains("UInt64")
    );
    assert!(
        string_column(&ints, "missing")
            .unwrap_err()
            .to_string()
            .contains("missing column")
    );
    assert!(
        string_column(&ints, "value")
            .unwrap_err()
            .to_string()
            .contains("Utf8")
    );

    let short_schema = Arc::new(Schema::new(vec![Field::new(
        "uuid",
        DataType::FixedSizeBinary(8),
        false,
    )]));
    let short = FixedSizeBinaryArray::try_from_iter([&[1_u8; 8][..]].into_iter()).unwrap();
    let short_batch = RecordBatch::try_new(short_schema.clone(), vec![Arc::new(short)]).unwrap();
    let optional = OptionalConfig {
        join_keys: vec![(0, 0)],
        inner_keep_idx: vec![],
        out_schema: short_schema.clone(),
        outer_schema: short_schema.clone(),
        inner_schema: short_schema,
    };
    assert!(
        optional_join(&optional, &[short_batch.clone()], &[short_batch])
            .unwrap_err()
            .to_string()
            .contains("not a 16-byte UUID")
    );

    let dir = TempDir::new().unwrap();
    let cfg = ExpandConfig {
        rel_type_name: "KNOWS".into(),
        direction: Direction::Out,
        min_hops: 1,
        max_hops: Some(1),
        dir: dir.path().to_path_buf(),
        mode: OntologyMode::Exploratory,
        src_col_idx: 0,
        out_schema: Arc::new(Schema::empty()),
        provider: Arc::new(PersistentAdjacencyProvider::new(
            dir.path().to_path_buf(),
            OntologyMode::Exploratory,
        )),
    };
    assert!(
        build_edge_list_column(&cfg, &[], &HashMap::new())
            .unwrap_err()
            .to_string()
            .contains("no edge-list column")
    );
    let mut not_list = cfg;
    not_list.out_schema = int_schema;
    assert!(
        build_edge_list_column(&not_list, &[], &HashMap::new())
            .unwrap_err()
            .to_string()
            .contains("must be a List")
    );

    let utf8 = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8,
            false,
        )])),
        vec![Arc::new(StringArray::from(vec!["x"]))],
    )
    .unwrap();
    assert!(u64_column(&utf8, 0).is_err());
}

#[test]
fn write_execs_expose_stable_plan_contracts_and_reject_invalid_shape() {
    let dir = TempDir::new().unwrap();
    let (logical, physical) = empty_write_input();
    let create: Arc<dyn ExecutionPlan> = Arc::new(
        GraphCreateExec::new(
            &GraphCreateNode::new(logical.clone(), vec![], vec![]),
            physical.clone(),
            &test_write_resource(dir.path()),
        )
        .unwrap(),
    );
    let delete: Arc<dyn ExecutionPlan> = Arc::new(
        GraphDeleteExec::new(
            &GraphDeleteNode::new(
                logical.clone(),
                vec![DeleteTarget {
                    var: 0,
                    is_edge: false,
                }],
                true,
            ),
            physical.clone(),
            &test_write_resource(dir.path()),
        )
        .unwrap(),
    );
    let set: Arc<dyn ExecutionPlan> = Arc::new(
        GraphSetExec::new(
            &GraphSetNode::new(
                logical.clone(),
                vec![SetTarget {
                    var: 0,
                    is_edge: false,
                    prop_name: "score".into(),
                    value: lit(1_i64),
                }],
            ),
            physical.clone(),
            &test_write_resource(dir.path()),
        )
        .unwrap(),
    );
    let remove: Arc<dyn ExecutionPlan> = Arc::new(
        GraphRemoveExec::new(
            &GraphRemoveNode::new(
                logical,
                vec![RemoveTarget {
                    var: 0,
                    is_edge: false,
                    prop_name: "score".into(),
                }],
            ),
            physical,
            &test_write_resource(dir.path()),
        )
        .unwrap(),
    );

    for (plan, expected_name) in [
        (create, "GraphCreateExec"),
        (delete, "GraphDeleteExec"),
        (set, "GraphSetExec"),
        (remove, "GraphRemoveExec"),
    ] {
        assert_eq!(plan.name(), expected_name);
        assert!(
            plan.is::<GraphCreateExec>()
                || plan.is::<GraphDeleteExec>()
                || plan.is::<GraphSetExec>()
                || plan.is::<GraphRemoveExec>()
        );
        assert_eq!(plan.children().len(), 1);
        assert_eq!(plan.properties().output_partitioning().partition_count(), 1);
        assert!(format!("{plan:?}").contains(expected_name));
        assert!(
            datafusion::physical_plan::displayable(plan.as_ref())
                .one_line()
                .to_string()
                .contains(expected_name)
        );
        let missing_child = plan.clone().with_new_children(vec![]).unwrap_err();
        assert!(missing_child.to_string().contains("needs one child"));
        let invalid_partition = match plan.execute(1, SessionContext::new().task_ctx()) {
            Ok(_) => panic!("{expected_name} accepted invalid partition 1"),
            Err(error) => error,
        };
        assert!(
            invalid_partition
                .to_string()
                .contains("only has partition 0")
        );
    }
}

#[tokio::test]
async fn empty_write_execs_dispatch_and_report_zero_changes() {
    let dir = TempDir::new().unwrap();
    let (logical, physical) = empty_write_input();
    let plans: Vec<Arc<dyn ExecutionPlan>> = vec![
        Arc::new(
            GraphCreateExec::new(
                &GraphCreateNode::new(logical.clone(), vec![], vec![]),
                physical.clone(),
                &test_write_resource(dir.path()),
            )
            .unwrap(),
        ),
        Arc::new(
            GraphDeleteExec::new(
                &GraphDeleteNode::new(logical.clone(), vec![], false),
                physical.clone(),
                &test_write_resource(dir.path()),
            )
            .unwrap(),
        ),
        Arc::new(
            GraphSetExec::new(
                &GraphSetNode::new(logical.clone(), vec![]),
                physical.clone(),
                &test_write_resource(dir.path()),
            )
            .unwrap(),
        ),
        Arc::new(
            GraphRemoveExec::new(
                &GraphRemoveNode::new(logical, vec![]),
                physical,
                &test_write_resource(dir.path()),
            )
            .unwrap(),
        ),
    ];

    for plan in plans {
        let batches = datafusion::physical_plan::collect(plan, SessionContext::new().task_ctx())
            .await
            .unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        for column in batches[0].columns() {
            assert_eq!(
                column
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .value(0),
                0
            );
        }
    }
}

#[tokio::test]
async fn wave11_physical_graph_execs_reject_missing_children_and_invalid_partitions() {
    use arrow::datatypes::Field;
    use datafusion::logical_expr::col;

    let dir = TempDir::new().unwrap();
    let (input, physical) = empty_write_input();
    let expand = graphforge_plan::ExpandNode::new(
        input.clone(),
        "KNOWS",
        1,
        2,
        3,
        graphforge_ir::Direction::Out,
        None,
        vec![],
        vec![],
        vec![],
    );
    let var_len = VarLenExpandNode::new(
        input.clone(),
        "KNOWS",
        1,
        Some(2),
        1,
        2,
        3,
        graphforge_ir::Direction::Out,
        None,
        vec![],
        graphforge_plan::var_len_edge_list_field(&[]),
    );
    let optional = OptionalMatchNode::new(input.clone(), input.clone(), vec![], vec![]);
    let unwind = graphforge_plan::UnwindNode::new(
        input.clone(),
        col("missing_list"),
        "item",
        &Field::new("item", DataType::Int64, true),
    );
    let infer = graphforge_plan::OntologyInferNode::new(
        input,
        "KNOWS",
        "transitive:KNOWS",
        "conservative_min",
    );
    let catalog = GraphCatalog::open(dir.path(), None, &RuntimeCatalog::new()).unwrap();
    let session = ExecutionSession::new_with_target(
        catalog,
        None,
        dir.path().to_path_buf(),
        OntologyMode::Strict,
    )
    .unwrap();
    let state = session.context().state();
    let planner = DefaultPhysicalPlanner::default();
    let extension = GraphForgeExtensionPlanner;

    let mut one_child = Vec::<Arc<dyn ExecutionPlan>>::new();
    for node in [
        &expand as &dyn UserDefinedLogicalNode,
        &var_len as &dyn UserDefinedLogicalNode,
        &unwind as &dyn UserDefinedLogicalNode,
        &infer as &dyn UserDefinedLogicalNode,
    ] {
        one_child.push(
            extension
                .plan_extension(&planner, node, &[], std::slice::from_ref(&physical), &state)
                .await
                .unwrap()
                .unwrap(),
        );
    }
    let optional = extension
        .plan_extension(
            &planner,
            &optional,
            &[],
            &[physical.clone(), physical],
            &state,
        )
        .await
        .unwrap()
        .unwrap();

    for plan in &one_child {
        assert!(format!("{plan:?}").contains(plan.name()));
        assert!(
            datafusion::physical_plan::displayable(plan.as_ref())
                .one_line()
                .to_string()
                .contains(plan.name())
        );
        assert!(
            plan.clone()
                .with_new_children(vec![])
                .unwrap_err()
                .to_string()
                .contains("needs one child")
        );
    }
    for plan in one_child.iter().take(3).chain(std::iter::once(&optional)) {
        let error = match plan.execute(1, SessionContext::new().task_ctx()) {
            Ok(_) => panic!("{} accepted invalid partition", plan.name()),
            Err(error) => error,
        };
        assert!(error.to_string().contains("only has partition 0"));
    }
    assert!(
        optional
            .clone()
            .with_new_children(vec![])
            .unwrap_err()
            .to_string()
            .contains("needs two children")
    );
}
