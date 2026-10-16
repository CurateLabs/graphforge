//! Real sorted-source handoff, including refusal before writing older rows.
use super::*;
use datafusion::common::DataFusionError;
use datafusion::execution::memory_pool::MemoryReservation;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Debug)]
struct SourceHandoffPool {
    inner: GreedyMemoryPool,
    insertion: usize,
    source: usize,
    obsolete: usize,
    refuse_second: bool,
    insertion_calls: AtomicUsize,
    spilling: AtomicBool,
    handoffs: AtomicUsize,
    writer_attempts: AtomicUsize,
    sorter_held: AtomicUsize,
    minimum_writer_source: AtomicUsize,
}
impl std::fmt::Display for SourceHandoffPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "sorted source handoff control")
    }
}
impl MemoryPool for SourceHandoffPool {
    fn name(&self) -> &str {
        "sorted source handoff control"
    }
    fn grow(&self, reservation: &MemoryReservation, bytes: usize) {
        self.inner.grow(reservation, bytes);
        if reservation.consumer().name().starts_with("ExternalSorter[") {
            self.sorter_held.fetch_add(bytes, Ordering::Relaxed);
        }
    }
    fn shrink(&self, reservation: &MemoryReservation, bytes: usize) {
        self.inner.shrink(reservation, bytes);
        if reservation.consumer().name().starts_with("ExternalSorter[") {
            self.sorter_held.fetch_sub(bytes, Ordering::Relaxed);
        }
    }
    fn try_grow(&self, reservation: &MemoryReservation, bytes: usize) -> Result<()> {
        let name = reservation.consumer().name();
        let sorter = name.starts_with("ExternalSorter[");
        if sorter {
            if bytes == self.insertion
                && !self.spilling.load(Ordering::Relaxed)
                && self.insertion_calls.fetch_add(1, Ordering::Relaxed) == 2
            {
                self.spilling.store(true, Ordering::Relaxed);
                return Err(DataFusionError::ResourcesExhausted(
                    "control forces the third split input batch to spill".into(),
                ));
            }
            if self.spilling.load(Ordering::Relaxed) {
                if bytes == self.obsolete {
                    return Err(DataFusionError::ResourcesExhausted(
                        "sorted handoff has source space, without a second pre-sort copy allowance"
                            .into(),
                    ));
                }
                if bytes == self.source {
                    let ordinal = self.handoffs.fetch_add(1, Ordering::Relaxed) + 1;
                    if self.refuse_second && ordinal == 2 {
                        return Err(DataFusionError::ResourcesExhausted(
                            "control refuses the second sorted source before writing".into(),
                        ));
                    }
                }
            }
        }
        if name.starts_with("SpillWriter") {
            self.writer_attempts.fetch_add(1, Ordering::Relaxed);
            self.minimum_writer_source
                .fetch_min(self.sorter_held.load(Ordering::Relaxed), Ordering::Relaxed);
        }
        self.inner.try_grow(reservation, bytes)?;
        if sorter {
            self.sorter_held.fetch_add(bytes, Ordering::Relaxed);
        }
        Ok(())
    }
    fn reserved(&self) -> usize {
        self.inner.reserved()
    }
}

async fn source_handoff(refuse_second: bool) -> Result<()> {
    use arrow::array::Int64Array;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::context::{SessionConfig, SessionContext};
    use datafusion::physical_expr::{PhysicalSortExpr, expressions::Column};
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::sorts::sort::{SortExec, sort_batch_chunked};

    const INPUT_ROWS: usize = 128;
    const OUTPUT_ROWS: usize = 64;
    let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
    let batches = [0, 1]
        .map(|partition| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from_iter_values(
                    (partition * INPUT_ROWS as i64..(partition + 1) * INPUT_ROWS as i64).rev(),
                ))],
            )
            .unwrap()
        })
        .to_vec();
    let order = [PhysicalSortExpr::new_default(Arc::new(Column::new(
        "key", 0,
    )))]
    .into();
    let sorted = sort_batch_chunked(&batches[0], &order, OUTPUT_ROWS)?;
    assert_eq!(sorted.len(), 2);
    let slice_size = |batch: &RecordBatch| -> Result<usize> {
        batch.columns().iter().try_fold(0, |sum, column| {
            Ok(sum + column.to_data().get_slice_memory_size()?)
        })
    };
    let source = physical_backing(&sorted[0]);
    assert_eq!(physical_backing(&sorted[1]), source);
    // DataSourceExec always applies BatchSplitStream at the session's batch
    // size. Its 64-row slices still own their complete 128-row input backing.
    let split_input = batches[0].slice(0, OUTPUT_ROWS);
    let insertion = physical_backing(&split_input) + slice_size(&split_input)?;
    drop(split_input);
    let obsolete = source + slice_size(&sorted[0])?;
    assert_ne!(source, insertion);
    assert_ne!(source, obsolete);
    assert_ne!(insertion, obsolete);
    drop(sorted);
    let observed = Arc::new(SourceHandoffPool {
        inner: GreedyMemoryPool::new(8 * 1024 * 1024),
        insertion,
        source,
        obsolete,
        refuse_second,
        insertion_calls: AtomicUsize::new(0),
        spilling: AtomicBool::new(false),
        handoffs: AtomicUsize::new(0),
        writer_attempts: AtomicUsize::new(0),
        sorter_held: AtomicUsize::new(0),
        minimum_writer_source: AtomicUsize::new(usize::MAX),
    });
    let pool: Arc<dyn MemoryPool> = observed.clone();
    let directory = tempfile::tempdir()?;
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool))
        .with_temp_file_path(directory.path().to_path_buf())
        .build_arc()?;
    let context = SessionContext::new_with_config_rt(
        SessionConfig::new()
            .with_batch_size(OUTPUT_ROWS)
            .with_sort_spill_reservation_bytes(64 * 1024),
        runtime,
    );
    let input = MemorySourceConfig::try_new_exec(&[batches], schema, None)?;
    let sort = SortExec::new(order, input);
    let mut stream = sort.execute(0, context.task_ctx())?;
    if refuse_second {
        let error = stream.next().await.unwrap().expect_err(
            "one older admitted batch cannot authorize retaining the refused incoming source",
        );
        assert!(matches!(
            error.find_root(),
            DataFusionError::ResourcesExhausted(_)
        ));
        assert_eq!(observed.handoffs.load(Ordering::Relaxed), 2);
        assert_eq!(observed.writer_attempts.load(Ordering::Relaxed), 0);
        assert_eq!(sort.metrics().unwrap().spill_count().unwrap(), 0);
        assert_eq!(
            context
                .runtime_env()
                .disk_manager
                .spilling_progress()
                .active_files_count,
            0
        );
    } else {
        let mut expected = 0_i64;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for value in values.values() {
                assert_eq!(*value, expected);
                expected += 1;
            }
        }
        assert_eq!(expected, 2 * INPUT_ROWS as i64);
        assert!(observed.handoffs.load(Ordering::Relaxed) >= 2);
        assert!(sort.metrics().unwrap().spill_count().unwrap() > 0);
        assert!(observed.writer_attempts.load(Ordering::Relaxed) > 0);
        assert!(observed.minimum_writer_source.load(Ordering::Relaxed) >= 2 * source);
    }
    drop(stream);
    drop(sort);
    assert_eq!(pool.reserved(), 0);
    assert_eq!(observed.sorter_held.load(Ordering::Relaxed), 0);
    assert_eq!(
        context
            .runtime_env()
            .disk_manager
            .spilling_progress()
            .active_files_count,
        0
    );
    Ok(())
}

#[tokio::test]
async fn spill_admission_sorted_handoff_keeps_actual_source_authority_through_writing() -> Result<()>
{
    source_handoff(false).await
}

#[tokio::test]
async fn spill_admission_refused_sorted_handoff_returns_before_writing_older_admitted_rows()
-> Result<()> {
    source_handoff(true).await
}
