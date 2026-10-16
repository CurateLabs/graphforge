//! Real IPC aggregate inputs release unused backing after their final use.
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, SpillMetrics};
use datafusion::physical_plan::spill::SpillManager;
use futures::StreamExt;

#[derive(Debug)]
struct AggregateIpcPartition {
    schema: Arc<Schema>,
    file: datafusion::execution::disk_manager::RefCountedTempFile,
    pressure: Arc<std::sync::Mutex<Option<datafusion::execution::memory_pool::MemoryReservation>>>,
    escaped: Arc<std::sync::Mutex<Option<ArrayRef>>>,
    original: Arc<std::sync::Mutex<Option<std::sync::Weak<dyn Array>>>>,
    body: Arc<std::sync::atomic::AtomicUsize>,
    retain_alias: bool,
}

impl datafusion::physical_plan::streaming::PartitionStream for AggregateIpcPartition {
    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    fn execute(
        &self,
        ctx: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::physical_plan::SendableRecordBatchStream {
        let schema = Arc::clone(&self.schema);
        let output_schema = Arc::clone(&schema);
        let file = self.file.clone();
        let pressure = Arc::clone(&self.pressure);
        let escaped = Arc::clone(&self.escaped);
        let original = Arc::clone(&self.original);
        let body = Arc::clone(&self.body);
        let retain_alias = self.retain_alias;
        let one = futures::stream::once(async move {
            let pool = Arc::clone(ctx.memory_pool());
            let reader = SpillManager::new(
                ctx.runtime_env(),
                SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
                schema,
            );
            let mut stream = reader.read_spill_as_stream_unbuffered(file, None)?;
            let batch = stream.next().await.unwrap()?;
            // No reader, future or source-side batch cache survives the yield.
            // futures::stream::once drops the completed future before Some.
            drop(stream);
            let text = batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let full_body = text.values().capacity();
            assert!(full_body > 2 * 1024 * 1024);
            for index in [0, 1] {
                let primitive = batch
                    .column(index)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                assert_eq!(primitive.values().inner().capacity(), full_body);
                assert_eq!(
                    primitive.values().inner().data_ptr(),
                    text.values().data_ptr(),
                    "small aggregate inputs share the unused ballast's complete physical body"
                );
            }
            assert!(pool.reserved() >= full_body);
            body.store(full_body, std::sync::atomic::Ordering::Relaxed);
            *original.lock().unwrap() = Some(Arc::downgrade(batch.column(2)));
            if retain_alias {
                *escaped.lock().unwrap() = Some(batch.column(2).slice(17, 1));
            }
            let independent =
                MemoryConsumer::new("independent aggregate input pressure").register(&pool);
            independent.try_grow(7 * 256 * 1024)?;
            *pressure.lock().unwrap() = Some(independent);
            Ok::<_, DataFusionError>(batch)
        });
        Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(output_schema, one),
        )
    }
}

fn aggregate_input_batch() -> Result<RecordBatch> {
    let rows = 8192_i64;
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
            Field::new("unused", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from_iter_values(0..rows)),
            Arc::new(Int64Array::from_iter_values((0..rows).map(|key| 3 * key))),
            Arc::new(StringArray::from_iter_values(
                (0..rows).map(|key| format!("{key:0256}")),
            )),
        ],
    )?)
}

async fn aggregate_input_case(
    file: datafusion::execution::disk_manager::RefCountedTempFile,
    schema: Arc<Schema>,
    limit: usize,
    retain_alias: bool,
) -> Result<Vec<RecordBatch>> {
    use datafusion::execution::TaskContext;
    use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
    use datafusion::physical_plan::streaming::StreamingTableExec;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(limit));
    // Isolate the ordinary aggregate update boundary. This NEW direct control
    // reports a refusal instead of invoking an unrelated sort/spill recovery.
    // No production pool, policy, query, batch-size or fixture knob changes.
    let env = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool))
        .with_disk_manager_builder(
            DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
        )
        .build_arc()?;
    let context = Arc::new(TaskContext::default().with_runtime(env));
    let pressure = Arc::new(std::sync::Mutex::new(None));
    let escaped = Arc::new(std::sync::Mutex::new(None));
    let original = Arc::new(std::sync::Mutex::new(None));
    let body = Arc::new(AtomicUsize::new(0));
    let partition = Arc::new(AggregateIpcPartition {
        schema: Arc::clone(&schema),
        file,
        pressure: Arc::clone(&pressure),
        escaped: Arc::clone(&escaped),
        original: Arc::clone(&original),
        body: Arc::clone(&body),
        retain_alias,
    });
    // Build directly so the optimizer cannot drop the unused ballast column.
    let input = Arc::new(StreamingTableExec::try_new(
        Arc::clone(&schema),
        vec![partition],
        None,
        [],
        false,
        None,
    )?);
    let expression = Arc::new(
        AggregateExprBuilder::new(
            datafusion::functions_aggregate::min_max::max_udaf(),
            vec![col("value", &schema)?],
        )
        .schema(Arc::clone(&schema))
        .alias("max_value")
        .build()?,
    );
    let aggregate = AggregateExec::try_new(
        AggregateMode::Single,
        PhysicalGroupBy::new_single(vec![(col("key", &schema)?, "key".to_owned())]),
        vec![expression],
        vec![None],
        input,
        schema,
    )?;
    let mut stream = aggregate.execute(0, context)?;
    let result = async {
        let mut output = Vec::new();
        while let Some(batch) = stream.next().await {
            output.push(batch?);
        }
        Ok::<_, DataFusionError>(output)
    }
    .await;
    drop(stream);
    drop(aggregate);
    assert_eq!(
        original.lock().unwrap().as_ref().unwrap().strong_count(),
        0,
        "the original source column wrapper has no hidden source cache"
    );
    let physical = body.load(Ordering::Relaxed);
    assert!(physical > 2 * 1024 * 1024);
    if retain_alias {
        let error =
            result.expect_err("a real live ballast alias must still prevent aggregate growth");
        assert!(
            matches!(error.find_root(), DataFusionError::ResourcesExhausted(_)),
            "{error}"
        );
        assert!(
            error.to_string().contains("GroupedHashAggregate"),
            "{error}"
        );
        assert!(pool.reserved() >= physical + 7 * 256 * 1024);
        drop(pressure.lock().unwrap().take());
        assert!(pool.reserved() >= physical);
        let alias = escaped.lock().unwrap().take().unwrap();
        assert_eq!(
            alias
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            format!("{:0256}", 17)
        );
        assert_eq!(
            alias
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .values()
                .capacity(),
            physical
        );
        drop(alias);
        assert_eq!(
            pool.reserved(),
            0,
            "last physical alias releases its real IPC ticket"
        );
        return Ok(Vec::new());
    }
    assert_eq!(
        pool.reserved(),
        7 * 256 * 1024,
        "completed aggregate work retains no source-body or reader authority"
    );
    drop(pressure.lock().unwrap().take());
    assert_eq!(pool.reserved(), 0);
    result
}

#[tokio::test]
async fn spill_aggregate_input_last_use_releases_unused_body_before_state_admission() -> Result<()>
{
    use arrow::ipc::reader::StreamReader;
    use datafusion::common::config::SpillCompression;
    let source = aggregate_input_batch()?;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(32 * 1024 * 1024));
    let writer_env = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool))
        .build_arc()?;
    let writer = SpillManager::new(
        writer_env,
        SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
        source.schema(),
    )
    .with_compression_type(SpillCompression::Uncompressed);
    let file = writer
        .spill_record_batch_and_finish(
            std::slice::from_ref(&source),
            "aggregate input last-use control",
        )?
        .unwrap();
    assert_eq!(pool.reserved(), 0);
    let mut ordinary = StreamReader::try_new(std::fs::File::open(file.path())?, None)?;
    assert_eq!(ordinary.next().unwrap()?, source);
    assert!(ordinary.next().is_none());
    drop(ordinary);

    // 8192 rows contribute ~2.16 MiB of full body, dominated by unused 256-byte
    // strings. Fixed 1.75 MiB pressure leaves ~80 KiB at 4 MiB, below the two
    // 8192-entry primitive state vectors alone. Releasing the input body
    // leaves >2 MiB for the complete copied group/hash/max state.
    fn cells(batches: &[RecordBatch]) -> std::collections::BTreeMap<i64, i64> {
        let mut result = std::collections::BTreeMap::new();
        for batch in batches {
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let values = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                assert!(!keys.is_null(row));
                assert!(!values.is_null(row));
                assert!(result.insert(keys.value(row), values.value(row)).is_none());
            }
        }
        result
    }
    let expected = (0..8192_i64)
        .map(|key| (key, 3 * key))
        .collect::<std::collections::BTreeMap<_, _>>();
    let roomy = aggregate_input_case(file.clone(), source.schema(), 8 * 1024 * 1024, false).await?;
    assert_eq!(cells(&roomy), expected);
    let tight = aggregate_input_case(file.clone(), source.schema(), 4 * 1024 * 1024, false).await?;
    assert_eq!(cells(&tight), expected);
    assert_eq!(tight[0].schema(), roomy[0].schema());
    // A true escaped slice is deliberately protected even after the aggregate
    // finishes grouping; no manual retirement may free its body early.
    aggregate_input_case(file, source.schema(), 4 * 1024 * 1024, true).await?;
    Ok(())
}
