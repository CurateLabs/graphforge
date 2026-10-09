//! Bounded UUID nominations observed on approved hash-join build inputs.

use std::collections::BTreeSet;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;

use arrow::array::Array;
use arrow::array::FixedSizeBinaryArray;
use arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::execution::memory_pool::MemoryReservation;
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::physical_plan::PlanProperties;
use datafusion::physical_plan::RecordBatchStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::metrics::MetricsSet;
use futures::Stream;
use tokio::sync::watch;

pub(crate) const NOMINATION_BASE_BYTES: usize = 512;
// BTreeSet nodes are not reserve-capable. This deliberately conservative per
// UUID charge covers node metadata and unused key slots before each insertion.
pub(crate) const NOMINATION_BYTES_PER_UUID: usize = 256;

#[derive(Clone, Debug)]
enum NominationStatus {
    Pending,
    Complete,
    Unknown,
    Failed(Arc<str>),
}

/// One completed build-side UUID set, shared by its matching probe scans.
#[derive(Debug)]
pub(crate) struct UuidBuildKeyNomination {
    status: watch::Sender<NominationStatus>,
    collecting: Mutex<BTreeSet<[u8; 16]>>,
    completed: OnceLock<BTreeSet<[u8; 16]>>,
    reservation: Mutex<Option<MemoryReservation>>,
    started: AtomicBool,
}

impl UuidBuildKeyNomination {
    pub(crate) fn new() -> Arc<Self> {
        let (status, _) = watch::channel(NominationStatus::Pending);
        Arc::new(Self {
            status,
            collecting: Mutex::new(BTreeSet::new()),
            completed: OnceLock::new(),
            reservation: Mutex::new(None),
            started: AtomicBool::new(false),
        })
    }

    fn start(&self, context: &TaskContext) -> Result<(), DataFusionError> {
        if self.started.swap(true, Ordering::AcqRel) {
            return Err(DataFusionError::Internal(
                "UUID build nomination tap executed more than once".into(),
            ));
        }
        let reservation =
            MemoryConsumer::new("GraphForge UUID build nomination").register(context.memory_pool());
        reservation.try_grow(NOMINATION_BASE_BYTES)?;
        *self
            .reservation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(reservation);
        Ok(())
    }

    fn observe(&self, values: &FixedSizeBinaryArray) -> Result<(), DataFusionError> {
        for row in 0..values.len() {
            if values.is_null(row) {
                continue;
            }
            let uuid: [u8; 16] = values.value(row).try_into().map_err(|_| {
                DataFusionError::Execution(
                    "UUID build nomination received a non-16-byte key".into(),
                )
            })?;

            let exists = self
                .collecting
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .contains(&uuid);
            if exists {
                continue;
            }

            {
                let reservation = self
                    .reservation
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let reservation = reservation.as_ref().ok_or_else(|| {
                    DataFusionError::Internal(
                        "UUID build nomination tap has no memory reservation".into(),
                    )
                })?;
                reservation.try_grow(NOMINATION_BYTES_PER_UUID)?;
            }

            let inserted = self
                .collecting
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(uuid);
            if !inserted {
                let reservation = self
                    .reservation
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Some(reservation) = reservation.as_ref() {
                    reservation.shrink(NOMINATION_BYTES_PER_UUID);
                }
            }
        }
        Ok(())
    }

    fn complete(&self) {
        let keys = std::mem::take(
            &mut *self
                .collecting
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        if self.completed.set(keys).is_ok() {
            self.set_terminal(NominationStatus::Complete);
        } else {
            self.fail("UUID build nomination completed more than once");
        }
    }

    fn fail(&self, error: impl Into<Arc<str>>) {
        self.set_terminal(NominationStatus::Failed(error.into()));
    }

    fn unknown(&self) {
        self.set_terminal(NominationStatus::Unknown);
    }

    fn set_terminal(&self, terminal: NominationStatus) {
        self.status.send_if_modified(|status| {
            if matches!(status, NominationStatus::Pending) {
                *status = terminal;
                true
            } else {
                false
            }
        });
    }

    pub(crate) fn ids(&self) -> Option<&BTreeSet<[u8; 16]>> {
        self.completed.get()
    }

    pub(crate) async fn wait(
        &self,
        consumer_closed: &tokio::sync::mpsc::Sender<
            Result<arrow::record_batch::RecordBatch, DataFusionError>,
        >,
    ) -> Result<bool, DataFusionError> {
        let mut status = self.status.subscribe();
        loop {
            match status.borrow_and_update().clone() {
                NominationStatus::Pending => {}
                NominationStatus::Complete => return Ok(true),
                NominationStatus::Unknown => {
                    return Err(DataFusionError::Execution(
                        "UUID build nomination ended before successful completion".into(),
                    ));
                }
                NominationStatus::Failed(error) => {
                    return Err(DataFusionError::Execution(format!(
                        "UUID build nomination failed: {error}"
                    )));
                }
            }
            tokio::select! {
                () = consumer_closed.closed() => return Ok(false),
                changed = status.changed() => {
                    if changed.is_err() {
                        return Err(DataFusionError::Execution(
                            "UUID build nomination producer disappeared".into(),
                        ));
                    }
                }
            }
        }
    }
}

/// Transparent tap over one hash-join build input. The final physical rule
/// supplies a one-partition coalesced input for CollectLeft joins.
#[derive(Debug)]
pub(crate) struct UuidBuildKeyTapExec {
    input: Arc<dyn ExecutionPlan>,
    uuid_column: usize,
    nomination: Arc<UuidBuildKeyNomination>,
}

impl UuidBuildKeyTapExec {
    pub(crate) fn new(
        input: Arc<dyn ExecutionPlan>,
        uuid_column: usize,
        nomination: Arc<UuidBuildKeyNomination>,
    ) -> Self {
        Self {
            input,
            uuid_column,
            nomination,
        }
    }
}

impl DisplayAs for UuidBuildKeyTapExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "UuidBuildKeyTapExec: column={}", self.uuid_column)
    }
}

impl ExecutionPlan for UuidBuildKeyTapExec {
    fn name(&self) -> &'static str {
        "UuidBuildKeyTapExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "UuidBuildKeyTapExec requires one child".into(),
            ));
        }
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            self.uuid_column,
            Arc::clone(&self.nomination),
        )))
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(Self::new(
            Arc::clone(&self.input),
            self.uuid_column,
            UuidBuildKeyNomination::new(),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.input.metrics()
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        if partition != 0 || self.input.output_partitioning().partition_count() != 1 {
            let error = DataFusionError::Internal(format!(
                "UuidBuildKeyTapExec requires one coalesced build partition, got partition {partition} of {}",
                self.input.output_partitioning().partition_count()
            ));
            self.nomination.fail(error.to_string());
            return Err(error);
        }
        if let Err(error) = self.nomination.start(&context) {
            self.nomination.fail(error.to_string());
            return Err(error);
        }
        let schema = self.input.schema();
        let Some(field) = schema.fields().get(self.uuid_column) else {
            let error = DataFusionError::Internal(format!(
                "UUID build key column {} is absent from build schema",
                self.uuid_column
            ));
            self.nomination.fail(error.to_string());
            return Err(error);
        };
        if field.data_type() != &arrow::datatypes::DataType::FixedSizeBinary(16) {
            let error = DataFusionError::Internal(format!(
                "UUID build key column {} has unexpected type {}",
                self.uuid_column,
                field.data_type()
            ));
            self.nomination.fail(error.to_string());
            return Err(error);
        }
        let input = match self.input.execute(partition, context) {
            Ok(input) => input,
            Err(error) => {
                self.nomination.fail(error.to_string());
                return Err(error);
            }
        };
        Ok(Box::pin(UuidBuildKeyTapStream {
            input,
            schema,
            uuid_column: self.uuid_column,
            nomination: Arc::clone(&self.nomination),
            completed: false,
        }))
    }
}

struct UuidBuildKeyTapStream {
    input: SendableRecordBatchStream,
    schema: SchemaRef,
    uuid_column: usize,
    nomination: Arc<UuidBuildKeyNomination>,
    completed: bool,
}

impl Stream for UuidBuildKeyTapStream {
    type Item = Result<arrow::record_batch::RecordBatch, DataFusionError>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.completed {
            return Poll::Ready(None);
        }
        match this.input.as_mut().poll_next(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Err(error))) => {
                this.nomination.fail(error.to_string());
                this.completed = true;
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(Some(Ok(batch))) => {
                let Some(values) = batch
                    .columns()
                    .get(this.uuid_column)
                    .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
                else {
                    let error = DataFusionError::Execution(format!(
                        "UUID build key column {} is not FixedSizeBinary",
                        this.uuid_column
                    ));
                    this.nomination.fail(error.to_string());
                    this.completed = true;
                    return Poll::Ready(Some(Err(error)));
                };
                match this.nomination.observe(values) {
                    Ok(()) => Poll::Ready(Some(Ok(batch))),
                    Err(error) => {
                        this.nomination.fail(error.to_string());
                        this.completed = true;
                        Poll::Ready(Some(Err(error)))
                    }
                }
            }
            Poll::Ready(None) => {
                this.nomination.complete();
                this.completed = true;
                Poll::Ready(None)
            }
        }
    }
}

impl RecordBatchStream for UuidBuildKeyTapStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Drop for UuidBuildKeyTapStream {
    fn drop(&mut self) {
        if !self.completed {
            self.nomination.unknown();
        }
    }
}
