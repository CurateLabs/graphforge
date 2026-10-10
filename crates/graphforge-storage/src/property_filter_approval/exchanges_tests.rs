use super::*;
use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{ScalarValue, config::ConfigOptions};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::equivalence::ConstExpr;
use datafusion::physical_expr::expressions::CastExpr;
use datafusion::physical_optimizer::{PhysicalOptimizerRule, sanity_checker::SanityCheckPlan};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::filter::FilterExecBuilder;
use datafusion::physical_plan::test::TestMemoryExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, PlanProperties, SendableRecordBatchStream, collect,
};

fn source() -> Arc<dyn ExecutionPlan> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("key", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![90, 91, 91, 92])),
            Arc::new(Int64Array::from(vec![-1, 1, 1, 2])),
        ],
    )
    .unwrap();
    TestMemoryExec::try_new_exec(&[vec![batch.clone()], vec![batch]], schema, None).unwrap()
}

fn rr(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    Arc::new(RepartitionExec::try_new(input, Partitioning::RoundRobinBatch(4)).unwrap())
}

fn predicate() -> Arc<dyn PhysicalExpr> {
    Arc::new(BinaryExpr::new(
        Arc::new(Column::new("key", 1)),
        Operator::Gt,
        Arc::new(Literal::new(ScalarValue::Int64(Some(0)))),
    ))
}

async fn rows(plan: Arc<dyn ExecutionPlan>) -> Vec<(i64, i64)> {
    let mut result = Vec::new();
    for batch in collect(plan, Arc::new(TaskContext::default()))
        .await
        .unwrap()
    {
        let first = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let second = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        result.extend((0..batch.num_rows()).map(|row| (first.value(row), second.value(row))));
    }
    result.sort_unstable();
    result
}

#[tokio::test]
async fn exchanges_preserve_duplicate_columns_embedded_projection_and_filtered_multisets() {
    let leaf = source();
    let projection: Arc<dyn ExecutionPlan> = Arc::new(
        ProjectionExec::try_new(
            vec![
                (
                    Arc::new(Column::new("key", 0)) as Arc<dyn PhysicalExpr>,
                    "key".into(),
                ),
                (
                    Arc::new(Column::new("key", 1)) as Arc<dyn PhysicalExpr>,
                    "key".into(),
                ),
            ],
            leaf.clone(),
        )
        .unwrap(),
    );
    let filter: Arc<dyn ExecutionPlan> = Arc::new(
        FilterExecBuilder::new(predicate(), rr(projection.clone()))
            .apply_projection(Some(vec![1, 0]))
            .unwrap()
            .build()
            .unwrap(),
    );
    let original: Arc<dyn ExecutionPlan> = Arc::new(
        RepartitionExec::try_new(
            filter.clone(),
            Partitioning::Hash(vec![Arc::new(Column::new("key", 0))], 4),
        )
        .unwrap(),
    );
    let rewritten = elide(original.clone()).unwrap();
    assert_eq!(rewritten.schema(), original.schema());
    let retained = rewritten.downcast_ref::<FilterExec>().unwrap();
    assert_eq!(
        retained.projection().as_deref(),
        Some([1_usize, 0].as_slice())
    );
    assert!(retained.predicate().dyn_eq(predicate().as_ref()));
    assert!(Arc::ptr_eq(retained.input(), &projection));
    assert_eq!(retained.input().children().len(), 1);
    assert!(Arc::ptr_eq(retained.input().children()[0], &leaf));
    SanityCheckPlan::new()
        .optimize(rewritten.clone(), &ConfigOptions::default())
        .unwrap();
    let before = rows(original).await;
    assert_eq!(
        before,
        vec![(1, 91), (1, 91), (1, 91), (1, 91), (2, 92), (2, 92)]
    );
    assert_eq!(rows(rewritten).await, before);
}

#[tokio::test]
async fn exchanges_preserve_batch_dependent_short_circuit_errors() {
    let schema = source().schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![90, 91])),
            Arc::new(Int64Array::from(vec![1, 0])),
        ],
    )
    .unwrap();
    let leaf: Arc<dyn ExecutionPlan> =
        TestMemoryExec::try_new_exec(&[vec![batch]], schema, None).unwrap();
    let hash: Arc<dyn ExecutionPlan> = Arc::new(
        RepartitionExec::try_new(
            leaf,
            Partitioning::Hash(vec![Arc::new(Column::new("key", 0))], 64),
        )
        .unwrap(),
    );
    let lhs = Arc::new(BinaryExpr::new(
        Arc::new(Column::new("key", 0)),
        Operator::Eq,
        Arc::new(Literal::new(ScalarValue::Int64(Some(90)))),
    ));
    let divide = Arc::new(BinaryExpr::new(
        Arc::new(Literal::new(ScalarValue::Int64(Some(100)))),
        Operator::Divide,
        Arc::new(Column::new("key", 1)),
    ));
    let rhs = Arc::new(BinaryExpr::new(
        divide,
        Operator::Gt,
        Arc::new(Literal::new(ScalarValue::Int64(Some(0)))),
    ));
    let predicate = Arc::new(BinaryExpr::new(lhs, Operator::And, rhs));
    let original: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(predicate, hash).unwrap());
    assert_eq!(rows(original.clone()).await, vec![(90, 1)]);
    let retained = elide(original.clone()).unwrap();
    assert!(
        Arc::ptr_eq(&original, &retained),
        "fallible short-circuit must retain batch boundaries"
    );
    let fresh = datafusion::physical_plan::execution_plan::reset_plan_states(retained).unwrap();
    assert_eq!(rows(fresh).await, vec![(90, 1)]);
}

#[tokio::test]
async fn exchanges_preserve_native_unsigned_type_membership() {
    use arrow::array::{Array, ListBuilder, UInt32Builder};
    let mut tags = ListBuilder::new(UInt32Builder::new());
    tags.values().append_value(3);
    tags.append(true);
    tags.values().append_value(4);
    tags.append(true);
    let tags = tags.finish();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "type_ids",
        tags.data_type().clone(),
        false,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(tags)]).unwrap();
    let input: Arc<dyn ExecutionPlan> =
        TestMemoryExec::try_new_exec(&[vec![batch]], schema.clone(), None).unwrap();
    let membership: Arc<dyn PhysicalExpr> = Arc::new(
        ScalarFunctionExpr::try_new(
            datafusion::functions_nested::array_has::array_has_udf(),
            vec![
                Arc::new(Column::new("type_ids", 0)),
                Arc::new(Literal::new(ScalarValue::UInt32(Some(3)))),
            ],
            schema.as_ref(),
            Arc::new(ConfigOptions::default()),
        )
        .unwrap(),
    );
    let filter: Arc<dyn ExecutionPlan> =
        Arc::new(FilterExec::try_new(membership, rr(input.clone())).unwrap());
    let rewritten = elide(filter.clone()).unwrap();
    assert!(Arc::ptr_eq(rewritten.children()[0], &input));
    for plan in [filter, rewritten] {
        let batches = collect(plan, Arc::new(TaskContext::default()))
            .await
            .unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    }
}

#[test]
fn exchanges_stop_at_fetch_computed_keys_and_other_operators() {
    let limited: Arc<dyn ExecutionPlan> = Arc::new(
        FilterExecBuilder::new(predicate(), rr(source()))
            .with_fetch(Some(1))
            .build()
            .unwrap(),
    );
    assert!(Arc::ptr_eq(&limited, &elide(limited.clone()).unwrap()));
    let computed: Arc<dyn ExecutionPlan> = Arc::new(
        RepartitionExec::try_new(
            source(),
            Partitioning::Hash(
                vec![Arc::new(CastExpr::new(
                    Arc::new(Column::new("key", 0)),
                    DataType::Int32,
                    None,
                ))],
                4,
            ),
        )
        .unwrap(),
    );
    assert!(Arc::ptr_eq(&computed, &elide(computed.clone()).unwrap()));
    let inner = rr(source());
    let barrier: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(inner.clone()));
    let rewritten = elide(rr(barrier.clone())).unwrap();
    assert!(Arc::ptr_eq(&rewritten, &barrier));
    assert!(Arc::ptr_eq(rewritten.children()[0], &inner));
}

#[test]
fn exchanges_do_not_traverse_volatile_filters_or_projections() {
    let input = rr(source());
    let random: Arc<dyn PhysicalExpr> = Arc::new(
        ScalarFunctionExpr::try_new(
            datafusion::functions::math::random(),
            vec![],
            input.schema().as_ref(),
            Arc::new(ConfigOptions::default()),
        )
        .unwrap(),
    );
    let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        random.clone(),
        Operator::Gt,
        Arc::new(Literal::new(ScalarValue::Float64(Some(0.5)))),
    ));
    let filter: Arc<dyn ExecutionPlan> =
        Arc::new(FilterExec::try_new(predicate, input.clone()).unwrap());
    assert!(Arc::ptr_eq(&filter, &elide(filter.clone()).unwrap()));
    let projection: Arc<dyn ExecutionPlan> =
        Arc::new(ProjectionExec::try_new(vec![(random, "random".into())], input).unwrap());
    assert!(Arc::ptr_eq(
        &projection,
        &elide(projection.clone()).unwrap()
    ));
}

#[derive(Debug)]
struct HeterogeneousInput {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl DisplayAs for HeterogeneousInput {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HeterogeneousInput")
    }
}

impl ExecutionPlan for HeterogeneousInput {
    fn name(&self) -> &str {
        "HeterogeneousInput"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Err(DataFusionError::Plan(
            "test input is a traversal barrier".into(),
        ))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        self.input.execute(partition, context)
    }
}

#[test]
fn exchanges_retain_heterogeneous_constants_hidden_by_repartition() {
    let input = source();
    let mut properties = input.properties().as_ref().clone();
    properties
        .eq_properties
        .add_constants([ConstExpr::new(
            Arc::new(Column::new("key", 0)),
            AcrossPartitions::Heterogeneous,
        )])
        .unwrap();
    let child: Arc<dyn ExecutionPlan> = Arc::new(HeterogeneousInput {
        input,
        properties: Arc::new(properties),
    });
    let exchange = rr(child);
    assert!(
        exchange.equivalence_properties().constants().is_empty(),
        "native exchange masks the input constant"
    );
    assert!(Arc::ptr_eq(&exchange, &elide(exchange.clone()).unwrap()));
}

#[test]
fn exchanges_preserve_ordered_and_unbounded_inputs() {
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::{execution_plan::Boundedness, sorts::sort::SortExec};
    let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
        Arc::new(Column::new("key", 0)),
        Default::default(),
    )])
    .unwrap();
    let ordered: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(ordering, source()));
    let exchange = rr(ordered);
    assert!(Arc::ptr_eq(&exchange, &elide(exchange.clone()).unwrap()));
    let input = source();
    let properties = input
        .properties()
        .as_ref()
        .clone()
        .with_boundedness(Boundedness::Unbounded {
            requires_infinite_memory: false,
        });
    let unbounded: Arc<dyn ExecutionPlan> = Arc::new(HeterogeneousInput {
        input,
        properties: Arc::new(properties),
    });
    let exchange = rr(unbounded);
    assert!(Arc::ptr_eq(&exchange, &elide(exchange.clone()).unwrap()));
}
