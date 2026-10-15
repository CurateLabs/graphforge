use super::*;
use arrow::array::UInt64Array;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::DFSchema;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::prelude::{SessionConfig, SessionContext};
use graphforge_plan::CorrelatedSeedNode;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;

const INPUT_FAILURE: &str = "anchored input exhausted after its first exchange batch";

#[derive(Debug, Default)]
struct InputGate {
    entered: Notify,
    release: Notify,
}

/// A real exchange's first batch passes through before the controlled failure.
/// Stalling the next poll leaves the batch retained and charged by preparation.
#[derive(Debug)]
struct ObservedInput {
    input: Arc<dyn ExecutionPlan>,
    executions: Arc<AtomicUsize>,
    gate: Option<Arc<InputGate>>,
}

impl DisplayAs for ObservedInput {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ObservedInput")
    }
}

impl ExecutionPlan for ObservedInput {
    fn name(&self) -> &str {
        "ObservedInput"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 1);
        Ok(Arc::new(Self {
            input: Arc::clone(&children[0]),
            executions: Arc::clone(&self.executions),
            gate: self.gate.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.executions.fetch_add(1, Ordering::SeqCst);
        let input = self.input.execute(partition, context)?;
        let Some(gate) = self.gate.clone() else {
            return Ok(input);
        };
        let stream = futures::stream::try_unfold((input, false), move |(mut input, consumed)| {
            let gate = Arc::clone(&gate);
            async move {
                if consumed {
                    gate.entered.notify_one();
                    gate.release.notified().await;
                    return Err(DataFusionError::ResourcesExhausted(INPUT_FAILURE.into()));
                }
                input
                    .try_next()
                    .await
                    .map(|batch| batch.map(|batch| (batch, (input, true))))
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

struct Fixture {
    plan: Arc<AnchoredExpansionExec>,
    context: Arc<TaskContext>,
    pool: Arc<dyn MemoryPool>,
    input_executions: Arc<AtomicUsize>,
    expansion_executions: Arc<AtomicUsize>,
    input_values: std::sync::Weak<dyn arrow::array::Array>,
}

fn fixture(gate: Option<Arc<InputGate>>, empty: bool) -> Fixture {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "key",
        DataType::UInt64,
        false,
    )]));
    let values: arrow::array::ArrayRef = Arc::new(UInt64Array::from(vec![1, 2, 3, 4]));
    let input_values = Arc::downgrade(&values);
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![values]).unwrap();
    let batches = if empty { vec![] } else { vec![batch] };
    let input = MemorySourceConfig::try_new_exec(&[batches], Arc::clone(&schema), None).unwrap();
    // Even a single exchange output is one-shot: a second execute(0) takes
    // an already-consumed channel and reproduces the original panic.
    let input =
        Arc::new(RepartitionExec::try_new(input, Partitioning::RoundRobinBatch(1)).unwrap());
    let input_executions = Arc::new(AtomicUsize::new(0));
    let input = Arc::new(ObservedInput {
        input,
        executions: Arc::clone(&input_executions),
        gate,
    });
    let seed = CorrelatedSeedNode::new(Arc::new(
        DFSchema::try_from(schema.as_ref().clone()).unwrap(),
    ));
    let expansion = Arc::new(super::super::CorrelatedSeedExec::new(&seed));
    let expansion =
        Arc::new(RepartitionExec::try_new(expansion, Partitioning::RoundRobinBatch(2)).unwrap());
    let expansion_executions = Arc::new(AtomicUsize::new(0));
    let expansion = Arc::new(ObservedInput {
        input: expansion,
        executions: Arc::clone(&expansion_executions),
        gate: None,
    });
    let plan = Arc::new(AnchoredExpansionExec::assemble(
        input,
        expansion,
        vec![(0, 0)],
        vec![],
        seed.id(),
        schema,
    ));
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool))
        .build_arc()
        .unwrap();
    let context = SessionContext::new_with_config_rt(SessionConfig::new(), runtime).task_ctx();
    Fixture {
        plan,
        context,
        pool,
        input_executions,
        expansion_executions,
        input_values,
    }
}

fn underlying(error: &DataFusionError) -> &DataFusionError {
    match error {
        DataFusionError::Shared(inner) => underlying(inner),
        error => error,
    }
}

fn same_error(left: &DataFusionError, right: &DataFusionError) {
    let (DataFusionError::Shared(left), DataFusionError::Shared(right)) = (left, right) else {
        panic!("preparation errors must retain one shared outcome");
    };
    assert!(Arc::ptr_eq(left, right));
}

async fn failure_or_cancel(cancel: bool) {
    let gate = Arc::new(InputGate::default());
    let fixture = fixture(Some(Arc::clone(&gate)), false);
    let mut first = fixture
        .plan
        .execute(0, Arc::clone(&fixture.context))
        .unwrap();
    let mut waiting = fixture
        .plan
        .execute(1, Arc::clone(&fixture.context))
        .unwrap();
    let mut first_poll = Box::pin(first.next());
    tokio::select! {
        () = gate.entered.notified() => {},
        result = &mut first_poll => panic!("preparation ended before the gate: {result:?}"),
    }
    assert!(
        fixture.pool.reserved() > 0,
        "consumed input must be charged"
    );
    let mut waiting_poll = Box::pin(waiting.next());
    assert!(futures::poll!(&mut waiting_poll).is_pending());
    let original = if cancel {
        drop(first_poll);
        drop(first);
        None
    } else {
        gate.release.notify_one();
        let error = first_poll.await.unwrap().unwrap_err();
        assert!(
            matches!(underlying(&error), DataFusionError::ResourcesExhausted(text) if text == INPUT_FAILURE)
        );
        drop(first);
        Some(error)
    };
    let error = waiting_poll.await.unwrap().unwrap_err();
    if let Some(original) = original.as_ref() {
        same_error(original, &error);
    } else {
        assert!(
            matches!(underlying(&error), DataFusionError::Execution(text) if text.contains("preparation cancelled"))
        );
    }
    let mut later = fixture
        .plan
        .execute(0, Arc::clone(&fixture.context))
        .unwrap();
    let later_error = later.next().await.unwrap().unwrap_err();
    same_error(&error, &later_error);
    assert!(waiting.next().await.is_none());
    assert!(later.next().await.is_none());
    assert_eq!(fixture.input_executions.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.expansion_executions.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.pool.reserved(), 0);
    drop(waiting);
    drop(later);
    let weak = Arc::downgrade(&fixture.plan.shared);
    let values = fixture.input_values.clone();
    drop(fixture);
    assert!(
        weak.upgrade().is_none(),
        "no self-retaining preparation task"
    );
    assert!(
        values.upgrade().is_none(),
        "failed input buffers must be released"
    );
}

#[tokio::test]
async fn anchored_preparation_failure_is_terminal_for_waiting_partitions() {
    failure_or_cancel(false).await;
}

#[tokio::test]
async fn anchored_preparation_cancellation_does_not_reexecute_consumed_exchange() {
    failure_or_cancel(true).await;
}

#[tokio::test]
async fn anchored_preparation_success_shares_input_and_preserves_rows() {
    let fixture = fixture(None, false);
    let plan: Arc<dyn ExecutionPlan> = fixture.plan.clone();
    let batches = collect(plan, Arc::clone(&fixture.context)).await.unwrap();
    let mut keys = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    keys.sort_unstable();
    assert_eq!(keys, [1, 2, 3, 4]);
    assert_eq!(fixture.input_executions.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.expansion_executions.load(Ordering::SeqCst), 2);
    let pool = Arc::clone(&fixture.pool);
    assert!(pool.reserved() > 0);
    drop(fixture);
    assert_eq!(pool.reserved(), 0);
}

#[tokio::test]
async fn anchored_preparation_empty_input_never_executes_expansion() {
    let fixture = fixture(None, true);
    let plan: Arc<dyn ExecutionPlan> = fixture.plan.clone();
    let batches = collect(plan, Arc::clone(&fixture.context)).await.unwrap();
    assert!(batches.is_empty());
    assert_eq!(fixture.input_executions.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.expansion_executions.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.pool.reserved(), 0);
}
