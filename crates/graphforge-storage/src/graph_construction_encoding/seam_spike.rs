//! Test-support-only encode seam experiment (#1465), never selected by default.
//!
//! A single admitted worker executes StreamingTableExec -> DataSinkExec. The
//! sink owns only the already-open Parquet writer, including its cache-window
//! syncs. Authentication, final fsync, namespace installation and receipts
//! remain with the calling encoder.
//! The pool reserves Arrow input bytes, not all process memory: writer and
//! compression allocations still require the experiment's separate total
//! process limit. No framework spill becomes durable authority.
use super::{CountingWriter, storage};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, SendableRecordBatchStream,
};
use futures::{StreamExt, stream};
use graphforge_core::GfError;
use parquet::arrow::ArrowWriter;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

type Writer = ArrowWriter<CountingWriter>;

pub(super) fn enabled() -> Result<bool, GfError> {
    match std::env::var("GF_ENCODE_SEAM_SPIKE") {
        Err(std::env::VarError::NotPresent) => Ok(false),
        Ok(mode) if mode == "baseline" => Ok(false),
        Ok(mode) if mode == "datafusion" => Ok(true),
        _ => Err(storage("invalid encode seam experiment mode")),
    }
}

#[derive(Debug)]
struct BatchPartition(RecordBatch);

impl PartitionStream for BatchPartition {
    fn schema(&self) -> &SchemaRef {
        self.0.schema_ref()
    }

    fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
        Box::pin(RecordBatchStreamAdapter::new(
            self.0.schema(),
            stream::iter([Ok(self.0.clone())]),
        ))
    }
}

struct ParquetSink {
    schema: SchemaRef,
    original: RecordBatch,
    writer: Mutex<Option<Writer>>,
    completed: Mutex<Option<CountingWriter>>,
    stop: Arc<AtomicBool>,
    metrics: ExecutionPlanMetricsSet,
}

impl fmt::Debug for ParquetSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("GraphForgeParquetSink")
    }
}

impl DisplayAs for ParquetSink {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("GraphForgeParquetSink")
    }
}

fn execution(error: impl fmt::Display) -> DataFusionError {
    DataFusionError::Execution(error.to_string())
}

impl ParquetSink {
    fn reject_cancelled(&self) -> DfResult<()> {
        if self.stop.load(Ordering::Acquire) {
            Err(execution("construction encoding cancelled"))
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl DataSink for ParquetSink {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _: &Arc<TaskContext>,
    ) -> DfResult<u64> {
        let mut writer = self
            .writer
            .lock()
            .map_err(execution)?
            .take()
            .ok_or_else(|| execution("encode sink executed twice"))?;
        let batches = MetricBuilder::new(&self.metrics).counter("input_batches", 0);
        let rows = MetricBuilder::new(&self.metrics).counter("encoded_rows", 0);
        let copies = MetricBuilder::new(&self.metrics).counter("seam_copied_arrow_bytes", 0);
        let retained = MetricBuilder::new(&self.metrics).gauge("writer_retained_bytes", 0);
        let mut count = 0_u64;
        while let Some(batch) = data.next().await {
            self.reject_cancelled()?;
            let batch = batch?;
            for (actual, original) in batch.columns().iter().zip(self.original.columns()) {
                if !Arc::ptr_eq(actual, original) {
                    copies.add(actual.get_array_memory_size());
                }
            }
            batches.add(1);
            rows.add(batch.num_rows());
            count += batch.num_rows() as u64;
            writer.write(&batch).map_err(execution)?;
            retained.set(writer.memory_size());
        }
        self.reject_cancelled()?;
        // Final compression and the Parquet footer execute in the same admitted
        // worker; final fsync and publication remain outside the framework.
        let completed = writer.into_inner().map_err(execution)?;
        *self.completed.lock().map_err(execution)? = Some(completed);
        Ok(count)
    }
}

fn execute(
    batch: RecordBatch,
    writer: Writer,
    stop: Arc<AtomicBool>,
    pool_bytes: usize,
) -> Result<CountingWriter, GfError> {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(pool_bytes));
    let reservation = MemoryConsumer::new("encode-spike-arrow-input").register(&pool);
    reservation
        .try_grow(batch.get_array_memory_size())
        .map_err(storage)?;
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool))
        .build_arc()
        .map_err(storage)?;
    let context = Arc::new(TaskContext::default().with_runtime(runtime));
    let schema = batch.schema();
    let source = StreamingTableExec::try_new(
        Arc::clone(&schema),
        vec![Arc::new(BatchPartition(batch.clone()))],
        None,
        [],
        false,
        None,
    )
    .map_err(storage)?;
    let sink = Arc::new(ParquetSink {
        schema,
        original: batch,
        writer: Mutex::new(Some(writer)),
        completed: Mutex::new(None),
        stop,
        metrics: ExecutionPlanMetricsSet::new(),
    });
    let plan = DataSinkExec::new(Arc::new(source), sink.clone(), None);
    if plan.properties().output_partitioning().partition_count() != 1 {
        return Err(storage("encode seam unexpectedly repartitioned"));
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(storage)?;
    runtime.block_on(async {
        let mut output = plan.execute(0, context).map_err(storage)?;
        while let Some(batch) = output.next().await {
            batch.map_err(storage)?;
        }
        Ok::<(), GfError>(())
    })?;
    if std::env::var_os("GF_ENCODE_SEAM_METRICS").is_some() {
        eprintln!(
            "ENCODE_SEAM {}",
            serde_json::json!({
                "plan": datafusion::physical_plan::displayable(&plan).indent(false).to_string(),
                "reserved_arrow_bytes": reservation.size(),
                "metrics": sink.metrics.clone_inner().iter().map(|metric| (metric.value().name().to_owned(), metric.value().as_usize())).collect::<std::collections::BTreeMap<_, _>>(),
            })
        );
    }
    let completed = sink
        .completed
        .lock()
        .map_err(storage)?
        .take()
        .ok_or_else(|| storage("encode sink did not return its descriptor"))?;
    drop(plan);
    drop(sink);
    drop(runtime);
    drop(reservation);
    if pool.reserved() != 0 {
        return Err(storage("encode seam leaked a memory reservation"));
    }
    Ok(completed)
}

/// One current-thread runtime on one scoped worker; no unbounded blocking pool.
/// The non-Send cancellation callback stays on the coordinator. Joining the
/// worker before returning keeps a cancelled encoder from leaving background IO.
pub(super) fn write(
    batch: &RecordBatch,
    writer: Writer,
    cancelled: &mut impl FnMut() -> bool,
) -> Result<CountingWriter, GfError> {
    let pool_bytes = match std::env::var("GF_ENCODE_SEAM_POOL_BYTES") {
        Ok(value) => value.parse::<usize>().map_err(storage)?,
        Err(std::env::VarError::NotPresent) => 64 << 20,
        Err(error) => return Err(storage(error)),
    };
    let stop = Arc::new(AtomicBool::new(false));
    let batch = batch.clone();
    std::thread::scope(|scope| {
        let (sender, receiver) = mpsc::sync_channel(1);
        let worker_stop = Arc::clone(&stop);
        let worker = scope.spawn(move || {
            let capture = std::env::var_os("GF_ENCODE_SEAM_METRICS").map(|_| {
                crate::concurrency_attribution::RegionCapture::start("encode_seam_worker")
            });
            let result = execute(batch, writer, worker_stop, pool_bytes);
            if let Some(capture) = capture {
                eprintln!(
                    "ENCODE_SEAM_WORKER {}",
                    serde_json::to_string(&capture.finish())
                        .expect("region snapshot is serializable")
                );
            }
            let _ = sender.send(result);
        });
        let mut was_cancelled = false;
        let result = loop {
            was_cancelled |= cancelled();
            if was_cancelled {
                stop.store(true, Ordering::Release);
            }
            match receiver.recv_timeout(Duration::from_millis(5)) {
                Ok(result) => break result,
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    break Err(storage("encode seam worker disconnected"));
                }
            }
        };
        worker
            .join()
            .map_err(|_| storage("encode seam worker panicked"))?;
        if was_cancelled || cancelled() {
            return Err(storage("construction encoding cancelled"));
        }
        result
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::UInt64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use sha2::Digest;

    #[test]
    fn sink_honors_cancellation_between_batches_before_finishing() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(UInt64Array::from(vec![1, 2]))],
        )
        .unwrap();
        let writer = ArrowWriter::try_new(
            CountingWriter {
                inner: graphforge_filesystem::DurableFileCacheWriter::with_window_bytes(
                    tempfile::tempfile().unwrap(),
                    std::num::NonZeroU64::new(4096).unwrap(),
                )
                .unwrap(),
                counter: super::super::IoCounter::default(),
                digest: sha2::Sha256::new(),
            },
            schema.clone(),
            None,
        )
        .unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let sink = ParquetSink {
            schema: schema.clone(),
            original: batch.clone(),
            writer: Mutex::new(Some(writer)),
            completed: Mutex::new(None),
            stop: stop.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
        };
        // Stream polling is a deterministic handshake: cancellation is raised
        // only after the first batch has been written, before the second one.
        let next = batch.clone();
        let batches = stream::iter([Ok(batch)]).chain(stream::once(async move {
            stop.store(true, Ordering::Release);
            Ok(next)
        }));
        let input = Box::pin(RecordBatchStreamAdapter::new(schema, batches));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let error = runtime
            .block_on(sink.write_all(input, &Arc::new(TaskContext::default())))
            .unwrap_err();
        assert!(error.to_string().contains("cancelled"));
        assert!(sink.completed.lock().unwrap().is_none());
        assert_eq!(
            sink.metrics
                .clone_inner()
                .sum_by_name("encoded_rows")
                .unwrap()
                .as_usize(),
            2
        );
    }
}
