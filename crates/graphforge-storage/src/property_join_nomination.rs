//! Bounded UUID nominations observed on approved hash-join build inputs.

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;

use crate::uuid_set::CompactUuidSet;
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
    collecting: Mutex<CompactUuidSet>,
    completed: OnceLock<CompactUuidSet>,
    reservation: Mutex<Option<MemoryReservation>>,
    started: AtomicBool,
}

impl UuidBuildKeyNomination {
    pub(crate) fn new() -> Arc<Self> {
        let (status, _) = watch::channel(NominationStatus::Pending);
        Arc::new(Self {
            status,
            collecting: Mutex::new(CompactUuidSet::default()),
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

            let mut collecting = self
                .collecting
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if collecting.contains(&uuid) {
                continue;
            }
            let mut reservation = self
                .reservation
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let reservation = reservation.as_mut().ok_or_else(|| {
                DataFusionError::Internal(
                    "UUID build nomination tap has no memory reservation".into(),
                )
            })?;

            if collecting.needs_growth_for(&uuid) {
                let new_capacity = collecting.capacity_for_next_insert().map_err(|()| {
                    DataFusionError::ResourcesExhausted("UUID nomination capacity overflow".into())
                })?;
                let requested_bytes = CompactUuidSet::storage_bytes_for_capacity(new_capacity)
                    .ok_or_else(|| {
                        DataFusionError::ResourcesExhausted(
                            "UUID nomination allocation size overflow".into(),
                        )
                    })?;
                let old_bytes = collecting.storage_bytes();
                reservation.try_grow(requested_bytes)?;
                let mut replacement = match CompactUuidSet::allocate(new_capacity) {
                    Ok(set) => set,
                    Err(error) => {
                        reservation.shrink(requested_bytes);
                        return Err(DataFusionError::ResourcesExhausted(format!(
                            "cannot allocate UUID nomination set: {error}"
                        )));
                    }
                };
                let actual_bytes = replacement.storage_bytes();
                debug_assert_eq!(actual_bytes, requested_bytes);
                replacement.reinsert_all(&collecting);
                let inserted = replacement.insert_without_growing(uuid);
                debug_assert!(inserted);
                let old = std::mem::replace(&mut *collecting, replacement);
                drop(old);
                reservation.shrink(old_bytes);
            } else {
                let inserted = collecting.insert_without_growing(uuid);
                debug_assert!(inserted);
            }
        }
        Ok(())
    }

    fn complete_empty(&self, mut keys: CompactUuidSet) -> Result<(), DataFusionError> {
        let released = keys.install_sorted(Vec::new());
        if released > 0
            && let Some(reservation) = self
                .reservation
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
        {
            reservation.shrink(released);
        }
        self.completed.set(keys).map_err(|_| {
            DataFusionError::Internal("UUID build nomination completed more than once".into())
        })?;
        self.set_terminal(NominationStatus::Complete);
        Ok(())
    }

    fn complete(&self) -> Result<(), DataFusionError> {
        let mut keys = std::mem::take(
            &mut *self
                .collecting
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        let released = keys.compact_occupied_prefix();
        if released > 0 {
            let reservation = self
                .reservation
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(reservation) = reservation.as_ref() {
                reservation.shrink(released);
            }
        }
        if keys.is_empty() {
            return self.complete_empty(keys);
        }
        let table_bytes = keys.storage_bytes();
        let Some(sorted_bytes) = keys.sorted_storage_bytes() else {
            drop(keys);
            if let Some(reservation) = self
                .reservation
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_mut()
            {
                reservation.shrink(table_bytes);
            }
            let error =
                DataFusionError::ResourcesExhausted("UUID sorted allocation size overflow".into());
            self.fail(error.to_string());
            return Err(error);
        };
        let mut reservation_guard = self
            .reservation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let reservation = reservation_guard.as_mut().ok_or_else(|| {
            DataFusionError::Internal("UUID build nomination has no memory reservation".into())
        })?;
        if let Err(error) = reservation.try_grow(sorted_bytes) {
            drop(reservation_guard);
            drop(keys);
            if let Some(reservation) = self
                .reservation
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_mut()
            {
                reservation.shrink(table_bytes);
            }
            self.fail(error.to_string());
            return Err(DataFusionError::ResourcesExhausted(format!(
                "cannot reserve compact UUID nomination: {error}"
            )));
        }
        let sorted = match keys.allocate_sorted_prefix() {
            Ok(sorted) => sorted,
            Err(error) => {
                reservation.shrink(sorted_bytes);
                drop(reservation_guard);
                drop(keys);
                if let Some(reservation) = self
                    .reservation
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .as_mut()
                {
                    reservation.shrink(table_bytes);
                }
                let error = DataFusionError::ResourcesExhausted(format!(
                    "cannot allocate compact UUID nomination: {error}"
                ));
                self.fail(error.to_string());
                return Err(error);
            }
        };
        let released = keys.install_sorted(sorted);
        reservation.shrink(released);
        drop(reservation_guard);
        if self.completed.set(keys).is_ok() {
            self.set_terminal(NominationStatus::Complete);
            Ok(())
        } else {
            self.fail("UUID build nomination completed more than once");
            Err(DataFusionError::Internal(
                "UUID build nomination completed more than once".into(),
            ))
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

    pub(crate) fn ids(&self) -> Option<&CompactUuidSet> {
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

    pub(crate) fn uuid_column(&self) -> usize {
        self.uuid_column
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
                this.completed = true;
                match this.nomination.complete() {
                    Ok(()) => Poll::Ready(None),
                    Err(error) => {
                        this.nomination.fail(error.to_string());
                        Poll::Ready(Some(Err(error)))
                    }
                }
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

#[cfg(test)]
mod compact_set_budget_tests {
    use std::sync::Arc;

    use arrow::array::FixedSizeBinaryBuilder;
    use datafusion::execution::TaskContext;
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;

    use crate::uuid_set::CompactUuidSet;

    use super::NOMINATION_BASE_BYTES;
    use super::UuidBuildKeyNomination;

    fn context(pool: Arc<dyn MemoryPool>) -> TaskContext {
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(pool)
            .build_arc()
            .unwrap();
        TaskContext::default().with_runtime(runtime)
    }

    #[test]
    fn denied_growth_keeps_old_credit_until_the_set_drops() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1_200));
        let nomination = UuidBuildKeyNomination::new();
        nomination.start(&context(Arc::clone(&pool))).unwrap();
        let mut builder = FixedSizeBinaryBuilder::new(16);
        for value in 0..9_u128 {
            builder.append_value(value.to_be_bytes()).unwrap();
        }

        assert!(nomination.observe(&builder.finish()).is_err());
        let collecting = nomination
            .collecting
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(collecting.len(), 8);
        drop(collecting);
        let reservation = nomination
            .reservation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let expected = 512 + CompactUuidSet::storage_bytes_for_capacity(16).unwrap();
        assert_eq!(reservation.as_ref().unwrap().size(), expected);
        assert_eq!(pool.reserved(), expected);
        drop(reservation);
        drop(nomination);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn denied_sorted_compaction_keeps_base_credit_until_the_set_drops() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(807));
        let nomination = UuidBuildKeyNomination::new();
        nomination.start(&context(Arc::clone(&pool))).unwrap();
        let mut builder = FixedSizeBinaryBuilder::new(16);
        builder.append_value(1_u128.to_be_bytes()).unwrap();
        nomination.observe(&builder.finish()).unwrap();

        let held = MemoryConsumer::new("held memory").register(&pool);
        held.try_grow(7).unwrap();
        assert!(nomination.complete().is_err());
        assert!(nomination.ids().is_none());
        assert_eq!(pool.reserved(), NOMINATION_BASE_BYTES + 7);
        let reservation = nomination
            .reservation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(reservation.as_ref().unwrap().size(), NOMINATION_BASE_BYTES);
        drop(reservation);
        drop(nomination);
        assert_eq!(pool.reserved(), 7);
        drop(held);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn two_million_distinct_uuids_fit_a_bounded_reservation() {
        const MIB: usize = 1024 * 1024;
        const IDS: usize = 2_000_000;
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(144 * MIB));
        let held_join_input = MemoryConsumer::new("test hash join input").register(&pool);
        held_join_input.try_grow(32 * MIB).unwrap();
        let nomination = UuidBuildKeyNomination::new();
        nomination.start(&context(Arc::clone(&pool))).unwrap();

        let batch_rows = 32_768;
        for start in (0..IDS).step_by(batch_rows) {
            let end = (start + batch_rows).min(IDS);
            let mut builder = FixedSizeBinaryBuilder::new(16);
            for value in start..end {
                builder.append_value((value as u128).to_be_bytes()).unwrap();
            }
            nomination.observe(&builder.finish()).unwrap();
        }
        let mut duplicates = FixedSizeBinaryBuilder::new(16);
        duplicates.append_value((0_u128).to_be_bytes()).unwrap();
        duplicates
            .append_value(((IDS - 1) as u128).to_be_bytes())
            .unwrap();
        duplicates.append_null();
        nomination.observe(&duplicates.finish()).unwrap();
        nomination.complete().unwrap();

        {
            let ids = nomination.ids().expect("complete set is published");
            assert_eq!(ids.len(), IDS);
            assert!(ids.contains(&0_u128.to_be_bytes()));
            assert!(ids.contains(&((IDS - 1) as u128).to_be_bytes()));
        }
        assert!(pool.reserved() < 100 * MIB);
        drop(nomination);
        assert_eq!(pool.reserved(), 32 * MIB);
        drop(held_join_input);
        assert_eq!(pool.reserved(), 0);
    }

    fn observe_distinct(nomination: &UuidBuildKeyNomination, ids: usize) {
        for start in (0..ids).step_by(32_768) {
            let end = (start + 32_768).min(ids);
            let mut builder = FixedSizeBinaryBuilder::new(16);
            for value in start..end {
                builder.append_value((value as u128).to_be_bytes()).unwrap();
            }
            nomination.observe(&builder.finish()).unwrap();
        }
    }

    #[test]
    fn compact_completion_frees_unused_chunks_before_sort_allocation_under_join_pressure() {
        const MIB: usize = 1024 * 1024;
        const IDS: usize = 2_097_153;
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(512 * MIB));
        let context = context(Arc::clone(&pool));
        let left_join_input = MemoryConsumer::new("left hash join input").register(&pool);
        left_join_input.try_grow(134 * MIB + MIB / 2).unwrap();

        let first = UuidBuildKeyNomination::new();
        let second = UuidBuildKeyNomination::new();
        first.start(&context).unwrap();
        observe_distinct(&first, IDS);
        first.complete().unwrap();

        second.start(&context).unwrap();
        observe_distinct(&second, IDS);
        let right_join_input = MemoryConsumer::new("right hash join input").register(&pool);
        right_join_input.try_grow(112 * MIB).unwrap();
        assert!(pool.reserved() > 380 * MIB);
        second.complete().unwrap();

        for nomination in [&first, &second] {
            let ids = nomination.ids().expect("completed set is published");
            assert_eq!(ids.len(), IDS);
            assert!(ids.contains(&0_u128.to_be_bytes()));
            assert!(ids.contains(&((IDS - 1) as u128).to_be_bytes()));
            let reservation = nomination
                .reservation
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert_eq!(
                reservation.as_ref().unwrap().size(),
                NOMINATION_BASE_BYTES + ids.storage_bytes()
            );
        }
        let sorter = MemoryConsumer::new("external sort").register(&pool);
        sorter.try_grow(10 * MIB).unwrap();
        drop(first);
        drop(second);
        assert_eq!(pool.reserved(), 134 * MIB + MIB / 2 + 112 * MIB + 10 * MIB);
        drop(sorter);
        drop(left_join_input);
        drop(right_join_input);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn empty_nomination_completes_without_a_memory_reservation() {
        let nomination = UuidBuildKeyNomination::new();
        nomination.complete().unwrap();
        assert!(nomination.ids().unwrap().is_empty());
    }
}
