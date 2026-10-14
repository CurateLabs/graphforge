//! Profiling consumes batches without retaining their buffers and keeps the
//! same cancellation and error behavior as a collecting query.
use super::*;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use std::fmt;
use std::sync::Weak;

#[derive(Clone, Copy, Debug)]
enum Outcome {
    Batches,
    Pending,
    Error,
    Panic,
    TaskPanic,
}

#[derive(Debug)]
struct DrainProbeExec {
    schema: arrow::datatypes::SchemaRef,
    props: Arc<PlanProperties>,
    outcome: Outcome,
}

impl DisplayAs for DrainProbeExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DrainProbeExec")
    }
}

impl ExecutionPlan for DrainProbeExec {
    fn name(&self) -> &str {
        "DrainProbeExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert!(children.is_empty());
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _: Arc<datafusion::execution::TaskContext>,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
        assert_eq!(partition, 0);
        let schema = Arc::clone(&self.schema);
        let outcome = self.outcome;
        let stream = futures::stream::unfold(
            (None::<Weak<dyn arrow::array::Array>>, 0),
            move |(previous, emitted)| {
                let schema = Arc::clone(&schema);
                async move {
                    assert!(
                        previous.is_none_or(|previous| previous.upgrade().is_none()),
                        "profiling retained the previous output batch"
                    );
                    match outcome {
                        Outcome::Pending => std::future::pending::<()>().await,
                        Outcome::Error => {
                            return Some((
                                Err(DataFusionError::ResourcesExhausted(
                                    "drain probe budget".into(),
                                )),
                                (None, emitted),
                            ));
                        }
                        Outcome::Panic => panic!("drain probe panic"),
                        Outcome::TaskPanic => {
                            let error = tokio::spawn(async { panic!("drain probe task panic") })
                                .await
                                .unwrap_err();
                            return Some((
                                Err(DataFusionError::External(Box::new(error))),
                                (None, emitted),
                            ));
                        }
                        Outcome::Batches => {}
                    }
                    if emitted == 16 {
                        return None;
                    }
                    let array: arrow::array::ArrayRef =
                        Arc::new(arrow::array::Int64Array::from_iter_values(0..1_024));
                    let previous = Arc::downgrade(&array);
                    let batch =
                        arrow::record_batch::RecordBatch::try_new(schema, vec![array]).unwrap();
                    Some((Ok(batch), (Some(previous), emitted + 1)))
                }
            },
        );
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        )))
    }
}

fn probe(
    outcome: Outcome,
    context: &datafusion::prelude::SessionContext,
) -> (
    tempfile::TempDir,
    Arc<dyn ExecutionPlan>,
    Arc<HydrationResource>,
) {
    let (dir, descriptor) = minimal_fixture();
    let original = test_udf(descriptor);
    let resource = HydrationResource::new(
        Arc::clone(&original.resource.graph),
        Arc::clone(context.task_ctx().memory_pool()),
    );
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("value", DataType::Int64, false),
    ]));
    let empty = datafusion::physical_plan::empty::EmptyExec::new(Arc::clone(&schema));
    let input = Arc::new(DrainProbeExec {
        schema,
        props: Arc::clone(empty.properties()),
        outcome,
    });
    let plan: Arc<dyn ExecutionPlan> = Arc::new(HydrationExec {
        input,
        resource: Arc::clone(&resource),
    });
    (dir, plan, resource)
}

fn assert_cancelled(resource: &HydrationResource) {
    assert!(matches!(
        resource.check(),
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    assert_eq!(resource.pool.reserved(), 0);
}

#[tokio::test]
async fn profiling_releases_each_batch_before_polling_the_next_and_cancels_at_eof() {
    let _guard = PathHydrationTestGuard::arm();
    let context = datafusion::prelude::SessionContext::new();
    let (_dir, plan, resource) = probe(Outcome::Batches, &context);
    drain_guarded(plan, context.task_ctx()).await.unwrap();
    assert_cancelled(&resource);
}

#[tokio::test]
async fn profiling_keeps_the_original_resource_error_and_cancels() {
    let _guard = PathHydrationTestGuard::arm();
    let context = datafusion::prelude::SessionContext::new();
    let (_dir, plan, resource) = probe(Outcome::Error, &context);
    let error = drain_guarded(plan, context.task_ctx()).await.unwrap_err();
    assert!(
        matches!(error, DataFusionError::ResourcesExhausted(ref message) if message == "drain probe budget")
    );
    assert_cancelled(&resource);
}

#[tokio::test]
async fn profiling_maps_polling_and_spawned_panics_to_named_execution_errors() {
    let _guard = PathHydrationTestGuard::arm();
    for outcome in [Outcome::Panic, Outcome::TaskPanic] {
        let context = datafusion::prelude::SessionContext::new();
        let (_dir, plan, resource) = probe(outcome, &context);
        let error = drain_guarded(plan, context.task_ctx()).await.unwrap_err();
        let typed = graphforge_core::GfError::from_execution_error(error);
        assert!(
            matches!(typed, graphforge_core::GfError::Execution(_)),
            "{typed}"
        );
        assert!(typed.to_string().contains("DrainProbeExec"), "{typed}");
        assert!(typed.to_string().contains("drain probe"), "{typed}");
        assert_cancelled(&resource);
    }
}

#[tokio::test]
async fn dropping_in_progress_profiling_cancels_its_query_resources() {
    let _guard = PathHydrationTestGuard::arm();
    let context = datafusion::prelude::SessionContext::new();
    let (_dir, plan, resource) = probe(Outcome::Pending, &context);
    let mut future = Box::pin(drain_guarded(plan, context.task_ctx()));
    assert!(futures::poll!(future.as_mut()).is_pending());
    resource.check().unwrap();
    drop(future);
    assert_cancelled(&resource);
}
