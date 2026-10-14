//! Execute run shaping with real variable-width Arrow allocations.

use super::*;

use arrow::array::{ArrayRef, BinaryViewArray, Int64Array, StringArray, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::memory_pool::MemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionContext;

use crate::query_pool::QueryMemoryPool;

const RUN_BYTES: usize = 64 * 1024;
const POOL_BYTES: usize = 16 * 1024 * 1024;

fn wide_batch(view: bool) -> RecordBatch {
    let rows = 2_048;
    let strings = (0..rows)
        .map(|row| (row % 7 != 0).then(|| format!("{row:04}:{}", "x".repeat(128 + row % 2_048))))
        .collect::<Vec<_>>();
    let text: ArrayRef = if view {
        Arc::new(StringViewArray::from(strings))
    } else {
        Arc::new(StringArray::from(strings))
    };
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("position", DataType::Int64, false),
            Field::new("text", text.data_type().clone(), true),
        ])),
        vec![Arc::new(Int64Array::from_iter_values(0..rows as i64)), text],
    )
    .unwrap()
}

fn context(pool: &Arc<QueryMemoryPool>) -> Arc<TaskContext> {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(pool) as Arc<dyn MemoryPool>)
        .build_arc()
        .unwrap();
    SessionContext::new_with_config_rt(Default::default(), runtime).task_ctx()
}

async fn assert_compact_runs(view: bool) {
    let batch = wide_batch(view);
    let source_bytes = batch.get_array_memory_size();
    assert!(source_bytes > 10 * RUN_BYTES);
    let source = MemorySourceConfig::try_new_exec(
        &[vec![RecordBatch::new_empty(batch.schema()), batch.clone()]],
        batch.schema(),
        None,
    )
    .unwrap();
    let plan = SortRunCoalesceExec::new(source, RUN_BYTES);
    let pool = Arc::new(QueryMemoryPool::new(POOL_BYTES));
    let mut stream = plan.execute(0, context(&pool)).unwrap();
    let mut runs = Vec::new();
    while let Some(run) = stream.next().await {
        let run = run.unwrap();
        assert_eq!(run.schema(), batch.schema());
        assert!(run.num_rows() > 0 && run.num_rows() < batch.num_rows());
        assert!(
            run.get_array_memory_size() <= RUN_BYTES,
            "{} rows retain {} bytes against {RUN_BYTES}",
            run.num_rows(),
            run.get_array_memory_size()
        );
        // Pending source ownership is fully charged after each transfer, and
        // is released once the final compact window has been emitted.
        assert!(pool.reserved() == source_bytes || pool.reserved() == 0);
        runs.push(run);
    }
    assert!(runs.len() > 10);
    assert_eq!(pool.reserved(), 0);
    let joined = concat_batches(&batch.schema(), &runs).unwrap();
    assert_eq!(joined, batch, "all nulls, values and row ordering survive");
    if !view {
        let source_pointer = batch.column(1).to_data().buffers()[1].as_ptr();
        for run in &runs {
            assert_ne!(
                run.column(1).to_data().buffers()[1].as_ptr(),
                source_pointer
            );
        }
    }
    assert!(pool.peak_gauge().load(std::sync::atomic::Ordering::Relaxed) < POOL_BYTES);
}

#[tokio::test]
async fn oversized_strings_are_copied_into_bounded_runs_in_source_order() {
    assert_compact_runs(false).await;
}

#[tokio::test]
async fn oversized_string_views_do_not_retain_the_source_backing_buffers() {
    assert_compact_runs(true).await;
}

#[test]
fn sizing_many_view_runs_does_not_repeatedly_scan_the_remaining_batch() {
    let rows = 8_192;
    let value = "x".repeat(4_096);
    let arrays: [ArrayRef; 2] = [
        Arc::new(StringViewArray::from(vec![Some(value.as_str()); rows])),
        Arc::new(BinaryViewArray::from(vec![Some(value.as_bytes()); rows])),
    ];
    for array in arrays {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            array.data_type().clone(),
            true,
        )]));
        let source = RecordBatch::try_new(schema, vec![array]).unwrap();
        let target = 1024 * 1024;
        let mut offset = 0;
        let mut visits = 0;
        let mut runs = 0;
        while offset < rows {
            let length = compact_window_rows(rows - offset, target, |length| {
                visits += length;
                compact_charge(&source.slice(offset, length))
            });
            assert!(length > 0);
            assert!(compact_charge(&source.slice(offset, length)) <= target);
            if offset + length < rows {
                assert!(compact_charge(&source.slice(offset, length + 1)) > target);
            }
            offset += length;
            runs += 1;
        }
        assert_eq!(offset, rows);
        assert!(runs > 64, "fixture must produce many small runs: {runs}");
        assert!(
            visits <= 16 * rows,
            "{}: {visits} values inspected for {rows} rows in {runs} runs",
            source.schema().field(0).data_type(),
        );
    }
}

#[tokio::test]
async fn dropping_a_partial_run_stream_releases_its_pending_source_charge() {
    let batch = wide_batch(false);
    let source_bytes = batch.get_array_memory_size();
    let source =
        MemorySourceConfig::try_new_exec(&[vec![batch.clone()]], batch.schema(), None).unwrap();
    let plan = SortRunCoalesceExec::new(source, RUN_BYTES);
    let pool = Arc::new(QueryMemoryPool::new(POOL_BYTES));
    let mut stream = plan.execute(0, context(&pool)).unwrap();
    assert!(stream.next().await.unwrap().is_ok());
    assert_eq!(pool.reserved(), source_bytes);
    drop(stream);
    assert_eq!(pool.reserved(), 0);
}

fn stream_from_results(
    schema: SchemaRef,
    batches: Vec<Result<RecordBatch>>,
    pool: &Arc<QueryMemoryPool>,
) -> SortRunCoalesceStream {
    let input = RecordBatchStreamAdapter::new(Arc::clone(&schema), futures::stream::iter(batches));
    SortRunCoalesceStream {
        schema,
        input: Some(Box::pin(input)),
        run_bytes: RUN_BYTES,
        buffered: Vec::new(),
        buffered_bytes: 0,
        pending: None,
        reservation: MemoryConsumer::new("SortRunCoalesce[0]")
            .register(&(Arc::clone(pool) as Arc<dyn MemoryPool>)),
    }
}

#[tokio::test]
async fn an_upstream_error_discards_buffered_rows_and_releases_their_charge() {
    let batch = wide_batch(false).slice(0, 1);
    // Force a compact, genuinely small source allocation.
    let batch = concat_batches(
        &batch.schema(),
        [&batch, &RecordBatch::new_empty(batch.schema())],
    )
    .unwrap();
    let pool = Arc::new(QueryMemoryPool::new(POOL_BYTES));
    let mut stream = stream_from_results(
        batch.schema(),
        vec![
            Ok(batch),
            Err(DataFusionError::Execution("source failed".into())),
        ],
        &pool,
    );
    let error = stream.next().await.unwrap().unwrap_err();
    assert!(error.to_string().contains("source failed"));
    assert_eq!(pool.reserved(), 0);
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn a_full_pool_refuses_pending_input_without_retaining_unaccounted_rows() {
    let batch = wide_batch(false);
    let pool = Arc::new(QueryMemoryPool::new(POOL_BYTES));
    let other =
        MemoryConsumer::new("producer state").register(&(Arc::clone(&pool) as Arc<dyn MemoryPool>));
    other.try_grow(POOL_BYTES).unwrap();
    let mut stream = stream_from_results(batch.schema(), vec![Ok(batch)], &pool);
    assert!(matches!(
        stream.next().await.unwrap(),
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    assert_eq!(pool.reserved(), POOL_BYTES);
    assert!(stream.next().await.is_none());
    other.free();
    assert_eq!(pool.reserved(), 0);
}

#[tokio::test]
async fn a_refused_compact_copy_releases_the_admitted_pending_source() {
    let batch = wide_batch(false);
    let source_bytes = batch.get_array_memory_size();
    let pool = Arc::new(QueryMemoryPool::new(source_bytes));
    let mut stream = stream_from_results(batch.schema(), vec![Ok(batch)], &pool);
    assert!(matches!(
        stream.next().await.unwrap(),
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    assert_eq!(pool.reserved(), 0);
    assert!(stream.next().await.is_none());
}
