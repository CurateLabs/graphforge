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

// Every selected outer row references a tiny interval of a much larger child.
// Keep the populated spans underneath null parents: concat copies those too.
fn nested_batch() -> RecordBatch {
    use arrow::array::{ListArray, StructArray};
    use arrow_buffer::{NullBuffer, OffsetBuffer};

    let rows = 1_024;
    let values: ArrayRef = Arc::new(Int64Array::from_iter_values(0..rows * 3));
    let inner: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Int64, true)),
        OffsetBuffer::from_lengths((0..rows).map(|_| 3)),
        values,
        Some(NullBuffer::from_iter((0..rows).map(|row| row % 5 != 0))),
    ));
    let lists: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", inner.data_type().clone(), true)),
        OffsetBuffer::from_lengths((0..rows).map(|row| usize::from(row % 7 != 0))),
        inner,
        Some(NullBuffer::from_iter((0..rows).map(|row| row % 3 != 0))),
    ));
    let text: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..rows).map(|row| format!("{row:04}:{}", "x".repeat(256))),
    ));
    let views: ArrayRef = Arc::new(StringViewArray::from_iter_values(
        (0..rows).map(|row| format!("{row:04}:{}", "v".repeat(256))),
    ));
    let fields = vec![
        Arc::new(Field::new("text", text.data_type().clone(), true)),
        Arc::new(Field::new("view", views.data_type().clone(), true)),
    ];
    let records: ArrayRef = Arc::new(StructArray::new(
        fields.into(),
        vec![text, views],
        Some(NullBuffer::from_iter((0..rows).map(|row| row % 4 != 0))),
    ));
    let records: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", records.data_type().clone(), true)),
        OffsetBuffer::from_lengths((0..rows).map(|_| 1)),
        records,
        None,
    ));
    let schema = Arc::new(Schema::new(vec![
        Field::new("position", DataType::Int64, false),
        Field::new("lists", lists.data_type().clone(), true),
        Field::new("records", records.data_type().clone(), true),
    ]));
    // All levels retain nonzero offsets and the unselected source backing.
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values(0..rows)),
            lists,
            records,
        ],
    )
    .unwrap()
    .slice(17, 127)
}

// Inspect actual admission boundaries, including the final window's copy.
// Any post-copy reconciliation grow also fails this proof: the bound must be
// sufficient before concat, while the complete source is still held.
#[derive(Debug)]
struct CompactSourcePool {
    inner: QueryMemoryPool,
    source_bytes: usize,
    requests: std::sync::atomic::AtomicUsize,
}

impl fmt::Display for CompactSourcePool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "compact source admission proof")
    }
}

impl MemoryPool for CompactSourcePool {
    fn name(&self) -> &str {
        "compact source admission proof"
    }

    fn grow(&self, reservation: &MemoryReservation, bytes: usize) {
        self.inner.grow(reservation, bytes);
    }

    fn shrink(&self, reservation: &MemoryReservation, bytes: usize) {
        self.inner.shrink(reservation, bytes);
    }

    fn try_grow(&self, reservation: &MemoryReservation, bytes: usize) -> Result<()> {
        if self
            .requests
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            == 0
        {
            assert_eq!(bytes, self.source_bytes);
        } else {
            assert_eq!(reservation.size(), self.source_bytes);
        }
        self.inner.try_grow(reservation, bytes)
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }
}

async fn execute_nested_runs(batch: &RecordBatch, capacity: usize) -> Vec<RecordBatch> {
    let pool = Arc::new(CompactSourcePool {
        inner: QueryMemoryPool::new(capacity),
        source_bytes: batch.get_array_memory_size(),
        requests: std::sync::atomic::AtomicUsize::new(0),
    });
    let source =
        MemorySourceConfig::try_new_exec(&[vec![batch.clone()]], batch.schema(), None).unwrap();
    let plan = SortRunCoalesceExec::new(source, RUN_BYTES);
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool) as Arc<dyn MemoryPool>)
        .build_arc()
        .unwrap();
    let context = SessionContext::new_with_config_rt(Default::default(), runtime).task_ctx();
    let mut stream = plan.execute(0, context).unwrap();
    let mut runs = Vec::new();
    let mut emitted = 0;
    while let Some(run) = stream.next().await {
        let run = run.expect("selected nested window fits alongside its admitted source");
        emitted += run.num_rows();
        assert_eq!(run.schema(), batch.schema());
        assert!(run.get_array_memory_size() <= RUN_BYTES);
        assert_eq!(
            pool.reserved(),
            if emitted < batch.num_rows() {
                batch.get_array_memory_size()
            } else {
                0
            },
            "full source remains charged through every nonfinal window"
        );
        runs.push(run);
    }
    assert_eq!(pool.reserved(), 0);
    assert!(runs.len() > 1);
    assert_eq!(
        pool.requests.load(std::sync::atomic::Ordering::Relaxed),
        runs.len() + 1,
        "one source admission and one pre-copy admission per window"
    );
    assert_eq!(concat_batches(&batch.schema(), &runs).unwrap(), *batch);
    runs
}

#[tokio::test]
async fn selected_nested_windows_fit_a_fixed_pool_with_exact_roomy_oracle() {
    let batch = nested_batch();
    let source_bytes = batch.get_array_memory_size();
    assert!(source_bytes > 8 * RUN_BYTES);
    let roomy = execute_nested_runs(&batch, 16 * 1024 * 1024).await;
    // Fixed allowance, independent of either the old or replacement estimate.
    let constrained = execute_nested_runs(&batch, source_bytes + 2 * RUN_BYTES).await;
    assert_eq!(constrained, roomy, "exact schema, values, nulls and order");
}

fn batch_with_column(array: ArrayRef) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            array.data_type().clone(),
            true,
        )])),
        vec![array],
    )
    .unwrap()
}

fn assert_physical_copy_bound(batch: &RecordBatch) -> RecordBatch {
    let copied = concat_batches(
        &batch.schema(),
        [batch, &RecordBatch::new_empty(batch.schema())],
    )
    .unwrap();
    let compact = RecordBatch::try_new(
        batch.schema(),
        copied
            .columns()
            .iter()
            .map(|array| make_array(compact_views(array.to_data()).unwrap()))
            .collect(),
    )
    .unwrap();
    assert_eq!(compact, *batch);
    assert!(
        compact_charge(batch) >= compact.get_array_memory_size(),
        "{}: copy bound {} below actual {}",
        batch.schema(),
        compact_charge(batch),
        compact.get_array_memory_size()
    );
    compact
}

#[test]
fn nested_copy_bound_includes_populated_null_spans_and_allocation_rounding() {
    use arrow::array::{LargeListArray, ListArray, StructArray};
    use arrow_buffer::{NullBuffer, OffsetBuffer};

    let text: ArrayRef = Arc::new(StringArray::from(vec!["x".repeat(65); 9]));
    let records: ArrayRef = Arc::new(StructArray::new(
        vec![Arc::new(Field::new("text", DataType::Utf8, true))].into(),
        vec![text],
        Some(NullBuffer::new_null(9)),
    ));
    let small: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", records.data_type().clone(), true)),
        OffsetBuffer::new(vec![0, 3, 3, 6, 9].into()),
        Arc::clone(&records),
        Some(NullBuffer::from(vec![true, true, false, true])),
    ));
    let large: ArrayRef = Arc::new(LargeListArray::new(
        Arc::new(Field::new("item", records.data_type().clone(), true)),
        OffsetBuffer::new(vec![0_i64, 3, 3, 6, 9].into()),
        records,
        Some(NullBuffer::from(vec![true, true, false, true])),
    ));
    for array in [small, large] {
        // Empty row followed by a null list with populated null Struct children.
        let batch = batch_with_column(array.slice(1, 2));
        let compact = assert_physical_copy_bound(&batch);
        let data = compact.column(0).to_data();
        assert_eq!(data.child_data()[0].len(), 3);
        assert_eq!(data.child_data()[0].child_data()[0].buffers()[1].len(), 195);
    }
}

#[test]
fn view_copy_bound_counts_raw_null_payload_and_carried_buffer_handles() {
    use arrow_buffer::{Buffer, NullBuffer};

    let value = "v".repeat(32_769);
    let arrays: [ArrayRef; 2] = [
        Arc::new(StringViewArray::from(vec![value.as_str(); 3])),
        Arc::new(BinaryViewArray::from(vec![value.as_bytes(); 3])),
    ];
    for array in arrays {
        let mut data = array.to_data().into_builder();
        // append_array carries all handles, even those unused by selected views.
        for _ in 0..128 {
            data = data.add_buffer(Buffer::from(vec![0_u8; 1]));
        }
        let array = make_array(data.nulls(Some(NullBuffer::new_null(3))).build().unwrap());
        let batch = batch_with_column(array.slice(1, 2));
        let compact = assert_physical_copy_bound(&batch);
        // GC copies both physical spans despite their logical NULL values.
        assert_eq!(compact.column(0).null_count(), 2);
        assert_eq!(
            compact.column(0).to_data().buffers()[1].len(),
            2 * value.len()
        );
        assert!(compact_charge(&batch) >= 2 * value.len() + 128 * size_of::<Buffer>());
    }
}

#[test]
fn dictionary_and_list_view_bounds_keep_whole_children() {
    use arrow::array::{DictionaryArray, Int32Array, LargeListViewArray, ListViewArray};
    use arrow::datatypes::Int32Type;

    let values: ArrayRef = Arc::new(StringArray::from(vec!["x".repeat(1_025); 1_024]));
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0, 1, 2]), Arc::clone(&values))
            .unwrap(),
    );
    let field = Arc::new(Field::new("item", DataType::Utf8, true));
    let list: ArrayRef = Arc::new(ListViewArray::new(
        Arc::clone(&field),
        vec![0_i32, 1, 2].into(),
        vec![1_i32, 1, 1].into(),
        Arc::clone(&values),
        None,
    ));
    let large_list: ArrayRef = Arc::new(LargeListViewArray::new(
        field,
        vec![0_i64, 1, 2].into(),
        vec![1_i64, 1, 1].into(),
        values,
        None,
    ));
    for array in [dictionary, list, large_list] {
        let batch = batch_with_column(array.slice(1, 1));
        assert!(compact_charge(&batch) > 1_024 * 1_025);
        assert_physical_copy_bound(&batch);
    }
}

#[tokio::test]
async fn nested_compaction_releases_source_on_drop_and_ordinary_error() {
    let batch = nested_batch();
    let pool = Arc::new(QueryMemoryPool::new(
        batch.get_array_memory_size() + 2 * RUN_BYTES,
    ));
    let mut stream = stream_from_results(batch.schema(), vec![Ok(batch.clone())], &pool);
    assert!(stream.next().await.unwrap().is_ok());
    assert_eq!(pool.reserved(), batch.get_array_memory_size());
    drop(stream);
    assert_eq!(pool.reserved(), 0);

    let mut stream = stream_from_results(
        batch.schema(),
        vec![
            Ok(batch.clone()),
            Err(DataFusionError::Execution("nested source failed".into())),
        ],
        &pool,
    );
    loop {
        match stream.next().await.unwrap() {
            Ok(_) => continue,
            Err(error) => {
                assert!(error.to_string().contains("nested source failed"));
                break;
            }
        }
    }
    assert_eq!(pool.reserved(), 0);
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn genuinely_oversized_nested_row_refuses_before_the_compact_copy() {
    use arrow::array::ListArray;
    use arrow_buffer::OffsetBuffer;

    let values: ArrayRef = Arc::new(StringArray::from(vec!["x".repeat(4 * RUN_BYTES); 2]));
    let list: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        OffsetBuffer::from_lengths([1, 1]),
        values,
        None,
    ));
    let batch = batch_with_column(list);
    let source_bytes = batch.get_array_memory_size();
    let admitted_copy = compact_charge(&batch.slice(0, 1));
    let pool = Arc::new(QueryMemoryPool::new(source_bytes + RUN_BYTES));
    let mut stream = stream_from_results(batch.schema(), vec![Ok(batch)], &pool);
    let error = stream.next().await.unwrap().unwrap_err();
    assert!(matches!(error, DataFusionError::ResourcesExhausted(_)));
    assert!(error.to_string().contains(&format!(
        "Failed to allocate additional {} for SortRunCoalesce[0]",
        datafusion::execution::memory_pool::human_readable_size(admitted_copy)
    )));
    // The initial source admission succeeded; the pre-concat copy request did not.
    assert_eq!(
        pool.peak_gauge().load(std::sync::atomic::Ordering::Relaxed),
        source_bytes
    );
    assert_eq!(pool.reserved(), 0);
    assert!(stream.next().await.is_none());
}

#[test]
fn compact_allocation_rounds_each_buffer_and_checks_overflow() {
    assert_eq!(compact_allocation(0), Some(0));
    assert_eq!(compact_allocation(1), Some(64));
    assert_eq!(compact_allocation(64), Some(64));
    assert_eq!(compact_allocation(65), Some(128));
    assert_eq!(compact_allocation(usize::MAX), None);
}

#[test]
fn fixed_size_list_descendants_keep_their_selected_child_window() {
    use arrow::array::{FixedSizeListArray, ListArray};
    use arrow_buffer::OffsetBuffer;

    let values: ArrayRef = Arc::new(StringArray::from(vec!["x".repeat(65); 2_048]));
    let fixed: ArrayRef = Arc::new(FixedSizeListArray::new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        2,
        values,
        None,
    ));
    let list: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", fixed.data_type().clone(), true)),
        OffsetBuffer::from_lengths([1; 1_024]),
        fixed,
        None,
    ));
    let batch = batch_with_column(list.slice(17, 2));
    assert!(compact_charge(&batch) < RUN_BYTES);
    assert_physical_copy_bound(&batch);
}

#[test]
fn dictionary_keys_and_list_view_offsets_copy_only_the_selected_rows() {
    use arrow::array::{DictionaryArray, Int32Array, LargeListViewArray, ListViewArray};
    use arrow::datatypes::Int32Type;

    let rows = 65_536;
    let values: ArrayRef = Arc::new(StringArray::from(vec!["whole dictionary value"]));
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0; rows]), Arc::clone(&values))
            .unwrap(),
    );
    let field = Arc::new(Field::new("item", DataType::Utf8, true));
    let list: ArrayRef = Arc::new(ListViewArray::new(
        Arc::clone(&field),
        vec![0_i32; rows].into(),
        vec![1_i32; rows].into(),
        Arc::clone(&values),
        None,
    ));
    let large: ArrayRef = Arc::new(LargeListViewArray::new(
        field,
        vec![0_i64; rows].into(),
        vec![1_i64; rows].into(),
        values,
        None,
    ));
    for array in [dictionary, list, large] {
        let batch = batch_with_column(array.slice(17, 2));
        assert!(batch.get_array_memory_size() > RUN_BYTES);
        assert!(compact_charge(&batch) < RUN_BYTES);
        assert_physical_copy_bound(&batch);
    }
}

fn dictionary_workspace_batch() -> RecordBatch {
    use arrow::array::{DictionaryArray, Int8Array, Int32Array};
    use arrow::datatypes::Int32Type;

    // Duplicate values still occupy distinct dictionary slots and masked-value
    // entries. The interner is sized before it can deduplicate their values.
    let values: ArrayRef = Arc::new(Int8Array::from_iter_values(
        (0..8_192).map(|value| i8::try_from(value % 128).unwrap()),
    ));
    let keys = Int32Array::from_iter_values((0..65_536).map(|row| row % 8_192));
    batch_with_column(Arc::new(
        DictionaryArray::<Int32Type>::try_new(keys, values).unwrap(),
    ))
}

#[test]
fn dictionary_copy_bound_covers_simultaneous_merge_workspace() {
    use arrow::datatypes::Int32Type;
    use std::collections::BTreeSet;

    let batch = dictionary_workspace_batch();
    let rows = compact_window_rows(batch.num_rows(), RUN_BYTES, |rows| {
        compact_charge(&batch.slice(0, rows))
    });
    // Check fixed windows as well: shrinking the chosen window must not hide
    // under-admission at the original failing cardinality or bucket boundaries.
    for rows in [rows, 1, 128, 1_024, 5_456, 8_192] {
        let selected = batch.slice(0, rows);
        let dictionary = selected.column(0).as_dictionary::<Int32Type>();
        let referenced = dictionary
            .keys()
            .iter()
            .flatten()
            .collect::<BTreeSet<_>>()
            .len();
        assert!(
            dictionary.values().len() >= rows,
            "Arrow's merge heuristic applies"
        );
        // Pinned dictionary.rs: Interner::new uses the bit length of N+128,
        // including an extra power-of-two step when N+128 is itself a power of two.
        let shift = (u64::try_from(referenced).unwrap() + 128).leading_zeros();
        let buckets = usize::try_from((u64::MAX >> shift) + 1).unwrap();
        // Immediately after allocating the first mapping, the consumed masked
        // Vec, interner and indices capacities are all still resident. This lower
        // bound excludes the earlier values_mask and every output allocation.
        let simultaneous = referenced * size_of::<(usize, Option<&[u8]>)>()
            + buckets * size_of::<Option<(Option<&[u8]>, i32)>>()
            + referenced * size_of::<(usize, usize)>()
            + dictionary.values().len() * size_of::<i32>();
        assert!(
            compact_charge(&selected) >= simultaneous,
            "{rows} selected rows admit {} bytes below {simultaneous} bytes of simultaneous dictionary scratch",
            compact_charge(&selected),
        );
    }
}

#[tokio::test]
async fn dictionary_merge_workspace_fits_fixed_headroom_and_releases_on_drop() {
    let batch = dictionary_workspace_batch();
    let source_bytes = batch.get_array_memory_size();
    let mut outputs = Vec::new();
    for capacity in [16 * 1024 * 1024, source_bytes + 2 * RUN_BYTES] {
        let pool = Arc::new(QueryMemoryPool::new(capacity));
        let source =
            MemorySourceConfig::try_new_exec(&[vec![batch.clone()]], batch.schema(), None).unwrap();
        let plan = SortRunCoalesceExec::new(source, RUN_BYTES);
        let mut stream = plan.execute(0, context(&pool)).unwrap();
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first, batch.slice(0, first.num_rows()));
        assert_eq!(pool.reserved(), source_bytes);
        assert!(pool.peak_gauge().load(std::sync::atomic::Ordering::Relaxed) <= capacity);
        drop(stream);
        assert_eq!(pool.reserved(), 0);
        outputs.push(first);
    }
    assert_eq!(outputs[0], outputs[1], "exact roomy first-window oracle");
}

#[test]
fn empty_array_data_can_omit_its_offset_sentinel() {
    use arrow::array::ArrayData;
    use arrow_buffer::Buffer;

    let item = Arc::new(Field::new("item", DataType::Int64, true));
    for data_type in [
        DataType::Utf8,
        DataType::Binary,
        DataType::LargeUtf8,
        DataType::LargeBinary,
        DataType::List(Arc::clone(&item)),
        DataType::LargeList(item),
    ] {
        let mut builder = ArrayData::builder(data_type.clone())
            .len(0)
            .add_buffer(Buffer::from_vec(Vec::<i64>::new()));
        if matches!(data_type, DataType::List(_) | DataType::LargeList(_)) {
            builder = builder.add_child_data(Int64Array::from(Vec::<i64>::new()).to_data());
        } else {
            builder = builder.add_buffer(Buffer::from(Vec::<u8>::new()));
        }
        let data = builder.build().unwrap();
        assert!(data.buffers()[0].is_empty());
        // Concrete arrays canonicalize the sentinel; raw ArrayData remains a
        // valid input to the recursive sizing helper before that conversion.
        assert!(!make_array(data.clone()).to_data().buffers()[0].is_empty());
        assert_eq!(compact_offset_span(&data), Some((0, 0)));
        assert!(compact_array_charge(&data, false).is_some());
    }
}

#[test]
fn dictionary_key_overflow_also_admits_merge_workspace() {
    use arrow::array::{DictionaryArray, Int8Array};
    use arrow::datatypes::Int8Type;

    let values: ArrayRef = Arc::new(Int8Array::from_iter_values(
        (0..256).map(|value| i8::try_from(value % 128).unwrap()),
    ));
    let keys = Int8Array::from_iter_values((0..512).map(|row| i8::try_from(row % 128).unwrap()));
    let array: ArrayRef = Arc::new(DictionaryArray::<Int8Type>::try_new(keys, values).unwrap());
    let batch = batch_with_column(array);
    // Values < output rows, but the dictionary slot count overflows Int8.
    // All 128 valid nonnegative keys are referenced; N+128 is a power of two,
    // so Arrow's Interner allocates 512 buckets, not 256.
    let simultaneous = 128 * size_of::<(usize, Option<&[u8]>)>()
        + 512 * size_of::<Option<(Option<&[u8]>, i8)>>()
        + 128 * size_of::<(usize, usize)>()
        + 256 * size_of::<i8>();
    assert!(compact_charge(&batch) >= simultaneous);
    assert_physical_copy_bound(&batch);
}
