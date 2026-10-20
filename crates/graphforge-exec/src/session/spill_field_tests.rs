//! Full legacy Field protection and separately funded escaped output.
use super::*;
use arrow::array::{ArrayRef, Int32Array};
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr, expressions::Column};
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::metrics::BaselineMetrics;
use datafusion::physical_plan::sorts::streaming_merge::{SortedSpillFile, StreamingMergeBuilder};

#[derive(Debug)]
struct ObservedPool {
    inner: Arc<dyn MemoryPool>,
    changed: tokio::sync::Notify,
}
impl std::fmt::Display for ObservedPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Field ownership observation")
    }
}
impl MemoryPool for ObservedPool {
    fn name(&self) -> &str {
        "Field ownership observation"
    }
    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer);
    }
    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer);
    }
    fn grow(&self, r: &datafusion::execution::memory_pool::MemoryReservation, n: usize) {
        self.inner.grow(r, n);
    }
    fn try_grow(
        &self,
        r: &datafusion::execution::memory_pool::MemoryReservation,
        n: usize,
    ) -> Result<()> {
        self.inner.try_grow(r, n)
    }
    fn shrink(&self, r: &datafusion::execution::memory_pool::MemoryReservation, n: usize) {
        self.inner.shrink(r, n);
        self.changed.notify_one();
    }
    fn reserved(&self) -> usize {
        self.inner.reserved()
    }
}

fn field_merge(
    pool: &Arc<dyn MemoryPool>,
    batches: &[RecordBatch],
    can_spill: bool,
) -> Result<(SendableRecordBatchStream, usize)> {
    let schema = batches[0].schema();
    let manager = manager(Arc::clone(pool), Arc::clone(&schema));
    let mut files = Vec::new();
    let mut prepaid = 0;
    for batch in batches {
        let mut writer = manager.create_in_progress_file("legacy Field ownership control")?;
        let maximum = writer.append_batch(batch)?;
        files.push(SortedSpillFile {
            file: writer.finish()?.unwrap(),
            max_record_batch_memory: maximum,
        });
        prepaid += 4 * maximum;
    }
    let sponsor = if can_spill {
        "GroupedHashAggregateStream[field ownership]"
    } else {
        "fixed field control"
    };
    let reservation = MemoryConsumer::new(sponsor)
        .with_can_spill(can_spill)
        .register(pool);
    reservation.try_grow(prepaid)?;
    let order = LexOrdering::new(vec![PhysicalSortExpr::new_default(Arc::new(Column::new(
        schema.field(0).name(),
        0,
    )))])
    .unwrap();
    let stream = StreamingMergeBuilder::new()
        .with_sorted_spill_files(files)
        .with_spill_manager(manager)
        .with_schema(schema)
        .with_expressions(&order)
        .with_metrics(BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0))
        .with_batch_size(16)
        .with_reservation(reservation)
        .build()?;
    Ok((stream, prepaid))
}

async fn escaped_field(can_spill: bool, termination: usize) -> Result<()> {
    const LIMIT: usize = 8 * 1024 * 1024;
    const ROWS: usize = 512;
    let inner: Arc<dyn MemoryPool> = if can_spill {
        Arc::new(crate::query_pool::QueryMemoryPool::new(LIMIT))
    } else {
        Arc::new(GreedyMemoryPool::new(LIMIT))
    };
    let observed = Arc::new(ObservedPool {
        inner,
        changed: tokio::sync::Notify::new(),
    });
    let pool: Arc<dyn MemoryPool> = observed.clone();
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let batches = [0, 1].map(|parity| {
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from_iter_values(
                    (0..ROWS).map(|i| (2 * i + parity) as i32),
                )) as ArrayRef,
                Arc::new(StringArray::from_iter_values(
                    (0..ROWS).map(|i| format!("{:064}", 2 * i + parity)),
                )),
            ],
        )
        .unwrap()
    });
    let (mut stream, prepaid) = field_merge(&pool, &batches, can_spill)?;
    let check = |batch: &RecordBatch, expected: &mut usize| {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let text = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert_eq!(keys.value(row), *expected as i32);
            assert_eq!(text.value(row), format!("{:064}", *expected));
            *expected += 1;
        }
    };
    let first = stream.next().await.unwrap()?;
    let mut expected = 0;
    check(&first, &mut expected);
    let primitive = first.column(0).slice(first.num_rows() - 1, 1);
    let primitive_bytes = primitive.to_data().buffers()[0].capacity();
    drop(first);
    let second = stream.next().await.unwrap()?;
    check(&second, &mut expected);
    let text = second.column(1).slice(second.num_rows() - 1, 1);
    let text_bytes = text.to_data().buffers()[1].capacity();
    drop(second);
    let mut pressure = None;
    if termination == 1 {
        let p = MemoryConsumer::new("independent fixed-output refusal").register(&pool);
        p.try_grow(LIMIT - pool.reserved())?;
        pressure = Some(p);
        // The pool is full: the remaining fresh outputs are served from the
        // working set the merge admitted at file selection.
        while let Some(batch) = stream.next().await {
            check(&batch?, &mut expected);
        }
        assert_eq!(expected, 2 * ROWS);
        assert!(stream.next().await.is_none());
    } else if termination == 0 {
        while let Some(batch) = stream.next().await {
            check(&batch?, &mut expected);
        }
        assert_eq!(expected, 2 * ROWS);
        assert!(stream.next().await.is_none());
    }
    assert_eq!(stream.schema(), schema);
    drop(stream);
    drop(pressure);
    // Async prefetch cancellation may still be returning its already-funded
    // source tickets. Wait on real release notifications, never a retry/sleep.
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let changed = observed.changed.notified();
            if pool.reserved() < prepaid / 2 {
                break;
            }
            changed.await;
        }
    })
    .await
    .expect("inner data and the unused fixed floor retire");
    assert!(pool.reserved() >= primitive_bytes + text_bytes);
    let before = pool.reserved();
    drop(primitive);
    assert!(
        pool.reserved() < before,
        "one output cannot pin its sibling output"
    );
    assert!(pool.reserved() >= text_bytes);
    drop(text);
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let changed = observed.changed.notified();
            if pool.reserved() == 0 {
                break;
            }
            changed.await;
        }
    })
    .await
    .expect("last escaped output and cancelled producers release all authority");
    Ok(())
}

#[tokio::test]
async fn spill_admission_fixed_field_funds_multiple_fresh_outputs_through_eof() -> Result<()> {
    escaped_field(false, 0).await
}
#[tokio::test]
async fn spill_admission_fixed_field_serves_fresh_outputs_from_its_working_set_under_a_full_pool()
-> Result<()> {
    escaped_field(false, 1).await
}
#[tokio::test]
async fn spill_admission_fixed_field_drop_retires_data_before_escaped_outputs() -> Result<()> {
    escaped_field(false, 2).await
}
#[tokio::test]
async fn spill_admission_fixed_field_preserves_real_spillable_sponsor_policy() -> Result<()> {
    escaped_field(true, 0).await
}

#[derive(Debug)]
struct PrefetchPool {
    inner: CopyPhasePool,
    watch: std::sync::atomic::AtomicBool,
    shrunk_before_prefetch: std::sync::atomic::AtomicUsize,
}
impl std::fmt::Display for PrefetchPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "legacy prefetch observation")
    }
}
impl MemoryPool for PrefetchPool {
    fn name(&self) -> &str {
        "legacy prefetch observation"
    }
    fn grow(&self, r: &datafusion::execution::memory_pool::MemoryReservation, n: usize) {
        self.inner.grow(r, n);
    }
    fn try_grow(
        &self,
        r: &datafusion::execution::memory_pool::MemoryReservation,
        n: usize,
    ) -> Result<()> {
        self.inner.try_grow(r, n)
    }
    fn shrink(&self, r: &datafusion::execution::memory_pool::MemoryReservation, n: usize) {
        if n > 0
            && r.consumer().name() == "fixed field control"
            && self.watch.load(std::sync::atomic::Ordering::Acquire)
            && !self
                .inner
                .reading
                .load(std::sync::atomic::Ordering::Acquire)
        {
            self.shrunk_before_prefetch
                .fetch_add(n, std::sync::atomic::Ordering::Relaxed);
        }
        self.inner.shrink(r, n);
    }
    fn reserved(&self) -> usize {
        self.inner.reserved()
    }
}

pub(super) async fn observe_legacy_prefetch() -> Result<()> {
    use arrow::array::Int64Array;
    let observed = Arc::new(PrefetchPool {
        inner: CopyPhasePool {
            inner: GreedyMemoryPool::new(16 * 1024 * 1024),
            reading: std::sync::atomic::AtomicBool::new(false),
            restored: std::sync::atomic::AtomicBool::new(false),
            release: (std::sync::Mutex::new(false), std::sync::Condvar::new()),
            started: tokio::sync::Notify::new(),
            changed: tokio::sync::Notify::new(),
        },
        watch: std::sync::atomic::AtomicBool::new(false),
        shrunk_before_prefetch: std::sync::atomic::AtomicUsize::new(0),
    });
    struct Release(Arc<PrefetchPool>);
    impl Drop for Release {
        fn drop(&mut self) {
            *self.0.inner.release.0.lock().unwrap() = true;
            self.0.inner.release.1.notify_all();
        }
    }
    let release = Release(Arc::clone(&observed));
    let pool: Arc<dyn MemoryPool> = observed.clone();
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("view", DataType::Utf8View, false),
    ]));
    let batches = [0, 1].map(|parity| {
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from_iter_values(
                    (0..512).map(|i| (2 * i + parity) as i64),
                )),
                Arc::new(StringViewArray::from_iter_values(std::iter::repeat_n(
                    "v".repeat(64),
                    512,
                ))),
            ],
        )
        .unwrap()
    });
    let (mut stream, _) = field_merge(&pool, &batches, false)?;
    observed
        .watch
        .store(true, std::sync::atomic::Ordering::Release);
    {
        let read = stream.next();
        tokio::pin!(read);
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            tokio::select! {
                _ = observed.inner.started.notified() => {},
                result = &mut read => panic!("prefetch barrier bypassed: {result:?}"),
            }
        })
        .await
        .expect("legacy reader starts with fixed sponsor protection");
    }
    assert_eq!(
        observed
            .shrunk_before_prefetch
            .load(std::sync::atomic::Ordering::Relaxed),
        0,
        "unsupported Field payload cannot release its production guard before prefetch"
    );
    drop(release);
    let mut rows = 0;
    while let Some(batch) = stream.next().await {
        rows += batch?.num_rows();
    }
    assert_eq!(rows, 1024);
    drop(stream);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}
