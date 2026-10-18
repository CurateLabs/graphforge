//! Real sorted-source handoff, including refusal before any later write.
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
    writer_attempts_at_refusal: AtomicUsize,
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
                        self.writer_attempts_at_refusal.store(
                            self.writer_attempts.load(Ordering::Relaxed),
                            Ordering::Relaxed,
                        );
                        return Err(DataFusionError::ResourcesExhausted(
                            "control refuses the second sorted source before its write".into(),
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
    source_handoff_capture(refuse_second, false, None)
        .await
        .map(|_| ())
}
async fn source_handoff_capture(
    refuse_second: bool,
    compound: bool,
    capture: Option<&datafusion_physical_plan::terminal_ownership::Session>,
) -> Result<(
    Option<RecordBatch>,
    Arc<dyn MemoryPool>,
    Option<DataFusionError>,
)> {
    use arrow::array::Int64Array;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::context::{SessionConfig, SessionContext};
    use datafusion::physical_expr::{PhysicalSortExpr, expressions::Column};
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::sorts::sort::{SortExec, sort_batch_chunked};

    const INPUT_ROWS: usize = 128;
    const OUTPUT_ROWS: usize = 64;
    let mut fields = vec![Field::new("key", DataType::Int64, false)];
    if compound {
        fields.push(Field::new("secondary", DataType::Int64, false));
    }
    let schema = Arc::new(Schema::new(fields));
    let batches = [0, 1]
        .map(|partition| {
            RecordBatch::try_new(Arc::clone(&schema), {
                let keys =
                    (partition * INPUT_ROWS as i64..(partition + 1) * INPUT_ROWS as i64).rev();
                let mut columns: Vec<arrow::array::ArrayRef> =
                    vec![Arc::new(Int64Array::from_iter_values(keys.clone()))];
                if compound {
                    columns.push(Arc::new(Int64Array::from_iter_values(
                        keys.map(|key| key % 4),
                    )));
                }
                columns
            })
            .unwrap()
        })
        .to_vec();
    let mut order = vec![PhysicalSortExpr::new_default(Arc::new(Column::new(
        "key", 0,
    )))];
    if compound {
        order.push(PhysicalSortExpr::new_default(Arc::new(Column::new(
            "secondary",
            1,
        ))));
    }
    let order = datafusion::physical_expr::LexOrdering::new(order).expect("nonempty sort keys");
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
        writer_attempts_at_refusal: AtomicUsize::new(0),
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
    let mut escaped = None;
    let mut observed_error = None;
    if refuse_second {
        let error = stream.next().await.unwrap().expect_err(
            "one older admitted batch cannot authorize retaining the refused incoming source",
        );
        assert!(matches!(
            error.find_root(),
            DataFusionError::ResourcesExhausted(_)
        ));
        assert_eq!(observed.handoffs.load(Ordering::Relaxed), 2);
        // The first source was fully admitted and written before the next
        // source was polled. A refused source must cause no subsequent write.
        let written_before_refusal = observed.writer_attempts_at_refusal.load(Ordering::Relaxed);
        assert!(written_before_refusal > 0);
        assert_eq!(
            observed.writer_attempts.load(Ordering::Relaxed),
            written_before_refusal,
        );
        observed_error = Some(error);
        assert_eq!(sort.metrics().unwrap().spill_count().unwrap(), 1);
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
            if let Some(capture) = capture {
                if escaped.is_none() {
                    capture.snapshot(0);
                    escaped = Some(batch.clone());
                }
            }
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            if compound {
                let secondary = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for (key, secondary) in values.values().iter().zip(secondary.values().iter()) {
                    assert_eq!(*secondary, *key % 4);
                }
            }
            for value in values.values() {
                assert_eq!(*value, expected);
                expected += 1;
            }
        }
        assert_eq!(expected, 2 * INPUT_ROWS as i64);
        assert!(observed.handoffs.load(Ordering::Relaxed) >= 2);
        assert!(sort.metrics().unwrap().spill_count().unwrap() > 0);
        assert!(observed.writer_attempts.load(Ordering::Relaxed) > 0);
    }
    assert!(observed.minimum_writer_source.load(Ordering::Relaxed) >= source);
    drop(stream);
    drop(sort);
    if escaped.is_none() {
        assert_eq!(pool.reserved(), 0);
    } else {
        assert!(
            pool.reserved() > 0,
            "escaped engine output must retain its actual buffer claims"
        );
    }
    assert_eq!(observed.sorter_held.load(Ordering::Relaxed), 0);
    assert_eq!(
        context
            .runtime_env()
            .disk_manager
            .spilling_progress()
            .active_files_count,
        0
    );
    Ok((escaped, pool, observed_error))
}

#[tokio::test]
async fn spill_admission_sorted_handoff_keeps_actual_source_authority_through_writing() -> Result<()>
{
    source_handoff(false).await
}

#[tokio::test]
async fn spill_admission_refused_sorted_handoff_returns_before_any_later_write() -> Result<()> {
    source_handoff(true).await
}

#[tokio::test]
async fn terminal_ownership_real_spill_two_calls_retains_actual_claims() -> Result<()> {
    ownership_spill_calls(false).await
}
#[tokio::test]
async fn terminal_ownership_real_spill_rows_two_calls_retains_actual_claims() -> Result<()> {
    ownership_spill_calls(true).await
}
async fn ownership_spill_calls(compound: bool) -> Result<()> {
    use datafusion_physical_plan::terminal_ownership::{Envelope, Session};
    let directory = tempfile::tempdir()?;
    let name = if compound {
        "spill-row-two-call.jsonl"
    } else {
        "spill-two-call.jsonl"
    };
    let path = directory.path().join(name);
    let capture = Session::create(&path, Envelope::default())?;
    capture.begin_call()?;
    let (first, pool1, error) = source_handoff_capture(false, compound, Some(&capture)).await?;
    assert!(error.is_none());
    capture.snapshot(0);
    capture.end_call(0);
    capture.begin_call()?;
    let (second, pool2, error) = source_handoff_capture(false, compound, Some(&capture)).await?;
    assert!(error.is_none());
    assert!(first.is_some() && second.is_some());
    assert!(pool1.reserved() > 0 && pool2.reserved() > 0);
    drop(first);
    drop(second);
    assert_eq!(pool1.reserved(), 0);
    assert_eq!(pool2.reserved(), 0);
    capture.snapshot(0);
    capture.end_call(0);
    capture.finish()?;
    let content = std::fs::read_to_string(&path)?;
    assert!(content.contains("\"owner_kind\":9"));
    assert!(content.contains("\"owner_kind\":10"));
    assert!(content.contains("\"owner_kind\":11"));
    assert!(content.contains("\"owner_kind\":15"));
    assert!(content.contains("\"owner_kind\":16"));
    assert!(content.contains("\"type\":35"));
    if let Some(out) = std::env::var_os("GRAPHFORGE_OWNERSHIP_EVIDENCE_DIR") {
        std::fs::create_dir_all(&out)?;
        std::fs::copy(path, std::path::PathBuf::from(out).join(name))?;
    }
    Ok(())
}
#[tokio::test]
async fn terminal_ownership_refusal_does_not_reuse_another_calls_closure() -> Result<()> {
    use datafusion_physical_plan::terminal_ownership::{Envelope, Session};
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("refusal-two-call.jsonl");
    let capture = Session::create(&path, Envelope::default())?;
    for _ in 0..2 {
        capture.begin_call()?;
        let (escaped, pool, error) = source_handoff_capture(true, false, Some(&capture)).await?;
        assert!(escaped.is_none() && error.is_some());
        assert_eq!(pool.reserved(), 0);
        capture.snapshot(1);
        capture.end_call_error(error.as_ref().unwrap());
    }
    capture.finish()?;
    if let Some(out) = std::env::var_os("GRAPHFORGE_OWNERSHIP_EVIDENCE_DIR") {
        std::fs::create_dir_all(&out)?;
        std::fs::copy(
            path,
            std::path::PathBuf::from(out).join("refusal-two-call.jsonl"),
        )?;
    }
    Ok(())
}
