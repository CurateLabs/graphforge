//! Disjoint layout inspection and IPC encoding need their maximum, not their sum.
use super::spill_admission_tests::manager;
use arrow::array::{Int64Array, RecordBatch, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::Result;
use datafusion::execution::memory_pool::{
    GreedyMemoryPool, MemoryConsumer, MemoryPool, MemoryReservation,
};
use futures::StreamExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
struct WriterPool {
    inner: GreedyMemoryPool,
    layout: AtomicUsize,
    encoding: AtomicUsize,
}
impl std::fmt::Display for WriterPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "spill writer phase control")
    }
}
impl MemoryPool for WriterPool {
    fn name(&self) -> &str {
        "spill writer phase control"
    }
    fn grow(&self, r: &MemoryReservation, n: usize) {
        self.inner.grow(r, n);
    }
    fn try_grow(&self, r: &MemoryReservation, n: usize) -> Result<()> {
        match r.consumer().name() {
            "SpillWriterMetadata" => {
                self.layout.fetch_max(r.size() + n, Ordering::Relaxed);
            }
            "SpillWriterEncode" => {
                // The first grant covers inspection, whether its authority is
                // later resized or encoding obtains a separate reservation.
                if r.size() == 0 && self.layout.load(Ordering::Relaxed) == 0 {
                    self.layout.store(n, Ordering::Relaxed);
                }
                self.encoding.fetch_max(r.size() + n, Ordering::Relaxed);
            }
            _ => {}
        }
        self.inner.try_grow(r, n)
    }
    fn shrink(&self, r: &MemoryReservation, n: usize) {
        self.inner.shrink(r, n);
    }
    fn reserved(&self) -> usize {
        self.inner.reserved()
    }
}

#[tokio::test]
async fn spill_writer_layout_and_encoding_share_disjoint_workspace() -> Result<()> {
    const LIMIT: usize = 4 * 1024 * 1024;
    let observed = Arc::new(WriterPool {
        inner: GreedyMemoryPool::new(LIMIT),
        layout: AtomicUsize::new(0),
        encoding: AtomicUsize::new(0),
    });
    let pool: Arc<dyn MemoryPool> = observed.clone();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, false),
            Field::new("trace_id", DataType::Utf8View, false),
        ])),
        vec![
            Arc::new(Int64Array::from_iter_values(0..64)),
            Arc::new(StringViewArray::from_iter_values(
                (0..64).map(|v| format!("{v:032x}")),
            )),
        ],
    )?;
    let source = MemoryConsumer::new("retained exchange input").register(&pool);
    source.try_grow(batch.get_array_memory_size())?;
    let spill = manager(Arc::clone(&pool), batch.schema());
    // A real roomy write observes both independently checked phase requests.
    // Constrain the next write to their maximum, with source fully retained.
    let roomy = spill
        .spill_record_batch_and_finish(std::slice::from_ref(&batch), "roomy writer")?
        .unwrap();
    drop(roomy);
    let layout = observed.layout.load(Ordering::Relaxed);
    let encoding = observed.encoding.load(Ordering::Relaxed);
    assert!(layout > 0 && encoding > layout);
    assert_eq!(pool.reserved(), source.size());
    let pressure = MemoryConsumer::new("independent writer pressure").register(&pool);
    pressure.try_grow(LIMIT - pool.reserved() - encoding.max(layout))?;
    let retained = pool.reserved();
    let file = spill
        .spill_record_batch_and_finish(std::slice::from_ref(&batch), "tight writer")?
        .expect("both nonoverlapping phases fit while source remains admitted");
    assert_eq!(pool.reserved(), retained);
    drop(pressure);
    let mut read = spill.read_spill_as_stream_unbuffered(file, None)?;
    let output = read.next().await.unwrap()?;
    assert_eq!(output, batch);
    assert!(read.next().await.is_none());
    drop(read);
    drop(output);
    drop(batch);
    drop(source);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}
