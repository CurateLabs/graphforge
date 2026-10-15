//! Qualified merge-operation bounds with real IPC, NULL spans and fixed pools.
use super::*;
use arrow::array::{
    ArrayRef, FixedSizeBinaryArray, ListArray, StructArray, TimestampMicrosecondArray, UInt32Array,
    UInt64Array,
};
use arrow::buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use datafusion::physical_expr::{PhysicalSortExpr, expressions::Column};
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::metrics::BaselineMetrics;
use datafusion::physical_plan::sorts::streaming_merge::{SortedSpillFile, StreamingMergeBuilder};

fn merge(
    pool: &Arc<dyn MemoryPool>,
    batches: &[RecordBatch],
    rows: usize,
) -> Result<(SendableRecordBatchStream, usize)> {
    merge_by(pool, batches, rows, &[0])
}
fn merge_by(
    pool: &Arc<dyn MemoryPool>,
    batches: &[RecordBatch],
    rows: usize,
    keys: &[usize],
) -> Result<(SendableRecordBatchStream, usize)> {
    merge_sponsored(pool, batches, rows, keys, false)
}
fn merge_sponsored(
    pool: &Arc<dyn MemoryPool>,
    batches: &[RecordBatch],
    rows: usize,
    keys: &[usize],
    can_spill: bool,
) -> Result<(SendableRecordBatchStream, usize)> {
    merge_sponsored_codec(pool, batches, rows, keys, can_spill, None)
}
fn merge_sponsored_codec(
    pool: &Arc<dyn MemoryPool>,
    batches: &[RecordBatch],
    rows: usize,
    keys: &[usize],
    can_spill: bool,
    codec: Option<datafusion::common::config::SpillCompression>,
) -> Result<(SendableRecordBatchStream, usize)> {
    let schema = batches[0].schema();
    let mut manager = manager(Arc::clone(pool), Arc::clone(&schema));
    if let Some(codec) = codec {
        manager = manager.with_compression_type(codec);
    }
    let mut prepaid = 0;
    let mut files = Vec::new();
    for batch in batches {
        let mut writer = manager.create_in_progress_file("selected-cell merge control")?;
        let maximum = writer.append_batch(batch)?;
        files.push(SortedSpillFile {
            file: writer.finish()?.unwrap(),
            max_record_batch_memory: maximum,
        });
        prepaid += 4 * maximum;
    }
    let reservation = MemoryConsumer::new("copy phase control")
        .with_can_spill(can_spill)
        .register(pool);
    reservation.try_grow(prepaid)?;
    let order = datafusion::physical_expr::LexOrdering::new(
        keys.iter()
            .map(|index| {
                PhysicalSortExpr::new_default(Arc::new(Column::new(
                    schema.field(*index).name(),
                    *index,
                )))
            })
            .collect::<Vec<_>>(),
    )
    .unwrap();
    let stream = StreamingMergeBuilder::new()
        .with_sorted_spill_files(files)
        .with_spill_manager(manager)
        .with_schema(schema)
        .with_expressions(&order)
        .with_metrics(BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0))
        .with_batch_size(rows)
        .with_reservation(reservation)
        .build()?;
    Ok((stream, prepaid))
}
fn keys(parity: usize, rows: usize) -> FixedSizeBinaryArray {
    FixedSizeBinaryArray::try_from_iter((0..rows).map(|i| {
        let mut key = [0; 16];
        key[8..].copy_from_slice(&((2 * i + parity) as u64).to_be_bytes());
        key
    }))
    .unwrap()
}
fn stored_null_strings(parity: usize, rows: usize, divisor: usize) -> StringArray {
    StringArray::new(
        OffsetBuffer::new(ScalarBuffer::from(
            (0..=rows).map(|i| (128 * i) as i32).collect::<Vec<_>>(),
        )),
        Buffer::from_vec(vec![b'x'; 128 * rows]),
        Some(NullBuffer::from(
            (0..rows)
                .map(|i| (2 * i + parity) % divisor != 0)
                .collect::<Vec<_>>(),
        )),
    )
}
fn rich_batch(parity: usize, rows: usize) -> RecordBatch {
    let key: ArrayRef = Arc::new(keys(parity, rows));
    let offsets = OffsetBuffer::new(ScalarBuffer::from(
        (0..=rows).map(|i| i as i32).collect::<Vec<_>>(),
    ));
    let types: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(DataType::UInt32, false)),
        offsets.clone(),
        Arc::new(UInt32Array::from_iter_values(
            (0..rows).map(|i| ((2 * i + parity) % 7) as u32),
        )),
        Some(NullBuffer::from(
            (0..rows)
                .map(|i| (2 * i + parity) % 2 != 0)
                .collect::<Vec<_>>(),
        )),
    ));
    let edge_fields = vec![
        Arc::new(Field::new(
            "edge_uuid",
            DataType::FixedSizeBinary(16),
            false,
        )),
        Arc::new(Field::new("src_uuid", DataType::FixedSizeBinary(16), false)),
        Arc::new(Field::new("dst_uuid", DataType::FixedSizeBinary(16), false)),
        Arc::new(Field::new("rel_type", DataType::Utf8, true)),
    ];
    let edges = Arc::new(StructArray::new(
        edge_fields.into(),
        vec![
            Arc::clone(&key),
            Arc::clone(&key),
            Arc::clone(&key),
            Arc::new(stored_null_strings(parity, rows, 5)),
        ],
        Some(NullBuffer::from(
            (0..rows)
                .map(|i| (2 * i + parity) % 7 != 0)
                .collect::<Vec<_>>(),
        )),
    ));
    let rels: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(edges.data_type().clone(), true)),
        offsets,
        edges,
        Some(NullBuffer::from(
            (0..rows)
                .map(|i| (2 * i + parity) % 3 != 0)
                .collect::<Vec<_>>(),
        )),
    ));
    let timestamp: ArrayRef = Arc::new(
        TimestampMicrosecondArray::from_iter_values((0..rows).map(|i| (2 * i + parity) as i64))
            .with_timezone("UTC"),
    );
    let columns: Vec<ArrayRef> = vec![
        key,
        Arc::new(UInt64Array::from_iter_values(
            (0..rows).map(|i| (2 * i + parity) as u64),
        )),
        types,
        timestamp,
        Arc::new(stored_null_strings(parity, rows, 3)),
        rels,
    ];
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .zip(["key", "id", "type_ids", "created_at", "name", "rels"])
            .map(|(a, name)| Field::new(name, a.data_type().clone(), true))
            .collect::<Vec<_>>(),
    ));
    RecordBatch::try_new(schema, columns).unwrap()
}

#[tokio::test]
async fn spill_admission_selected_nested_cells_complete_when_only_current_copy_workspace_fits()
-> Result<()> {
    const LIMIT: usize = 16 * 1024 * 1024;
    const HEADROOM: usize = 1024 * 1024;
    const ROWS: usize = 2048;
    let observed = Arc::new(CopyPhasePool {
        inner: GreedyMemoryPool::new(LIMIT),
        reading: std::sync::atomic::AtomicBool::new(false),
        restored: std::sync::atomic::AtomicBool::new(false),
        release: (std::sync::Mutex::new(false), std::sync::Condvar::new()),
        started: tokio::sync::Notify::new(),
        changed: tokio::sync::Notify::new(),
    });
    struct Release(Arc<CopyPhasePool>);
    impl Drop for Release {
        fn drop(&mut self) {
            *self.0.release.0.lock().unwrap() = true;
            self.0.release.1.notify_all();
        }
    }
    let release = Release(Arc::clone(&observed));
    let pool: Arc<dyn MemoryPool> = observed.clone();
    let batches = [rich_batch(0, ROWS), rich_batch(1, ROWS)];
    let joined = arrow::compute::concat_batches(&batches[0].schema(), &batches)?;
    let order = arrow::array::UInt32Array::from_iter_values(
        (0..ROWS).flat_map(|i| [i as u32, (ROWS + i) as u32]),
    );
    let oracle = arrow::compute::take_record_batch(&joined, &order)?;
    let (mut stream, prepaid) = merge(&pool, &batches, 16)?;
    {
        let read = stream.next();
        tokio::pin!(read);
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            tokio::select! { _ = observed.started.notified() => {}, result = &mut read => panic!("reader barrier bypassed: {result:?}") }
        }).await.expect("real reader reaches its admitted boundary");
    }
    assert!(
        prepaid / 2 > HEADROOM,
        "the old full copy envelope cannot fit this headroom"
    );
    let pressure = MemoryConsumer::new("independent nested-copy pressure").register(&pool);
    pressure.try_grow(LIMIT - pool.reserved() - HEADROOM)?;
    drop(release);
    let mut offset = 0;
    let mut alias = None;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        assert_eq!(batch, oracle.slice(offset, batch.num_rows()));
        let names = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            names.values().len(),
            128 * batch.num_rows(),
            "NULL string stored spans are copied too"
        );
        let rels = batch
            .column(5)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(
            rels.values().len(),
            batch.num_rows(),
            "NULL list stored spans still copy their children"
        );
        let edges = rels
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let text = edges
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            text.values().len(),
            128 * batch.num_rows(),
            "Struct/child NULLs cannot discard stored byte spans from admission"
        );
        if alias.is_none() {
            alias = Some(batch.column(4).slice(0, 1));
        }
        offset += batch.num_rows();
    }
    assert_eq!(offset, 2 * ROWS);
    drop(stream);
    drop(pressure);
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let changed = observed.changed.notified();
            if pool.reserved() < HEADROOM {
                break;
            }
            changed.await;
        }
    })
    .await
    .expect("unrelated sources and workers release");
    let alias = alias.unwrap();
    assert!(pool.reserved() >= alias.to_data().buffers()[1].capacity());
    drop(alias);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn spill_admission_row_constructor_refuses_unavailable_vector_workspace_before_allocation()
-> Result<()> {
    const LIMIT: usize = 16 * 1024 * 1024;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(LIMIT));
    let batches = [rich_batch(0, 2), rich_batch(1, 2)];
    let (mut stream, _) = merge(&pool, &batches, LIMIT / size_of::<(usize, usize)>())?;
    let error = stream
        .next()
        .await
        .unwrap()
        .expect_err("constructor vectors cannot exceed their available fixed pool");
    assert!(
        matches!(
            error.find_root(),
            datafusion::common::DataFusionError::ResourcesExhausted(_)
        ),
        "{error}"
    );
    drop(stream);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn spill_admission_flat_view_payload_preserves_its_complete_source_alias() -> Result<()> {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(16 * 1024 * 1024));
    let batches = [0, 1].map(|parity| {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(keys(parity, 32)),
            Arc::new(StringViewArray::from_iter_values(std::iter::repeat_n(
                "view".repeat(64),
                32,
            ))),
        ];
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::FixedSizeBinary(16), false),
            Field::new("view", DataType::Utf8View, false),
        ]));
        RecordBatch::try_new(schema, columns).unwrap()
    });
    let (mut stream, _) = merge(&pool, &batches, 8)?;
    let first = stream.next().await.unwrap()?;
    let alias = first.column(1).slice(0, 1);
    let mut rows = first.num_rows();
    drop(first);
    while let Some(batch) = stream.next().await {
        rows += batch?.num_rows();
    }
    assert_eq!(rows, 64);
    drop(stream);
    assert!(pool.reserved() >= alias.to_data().get_buffer_memory_size());
    assert_eq!(
        alias
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap()
            .value(0),
        "view".repeat(64)
    );
    drop(alias);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn spill_admission_builder_vector_growth_keeps_old_and_new_capacity_admitted() -> Result<()> {
    const ROWS: usize = 32;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(2 * 1024 * 1024));
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::FixedSizeBinary(16), false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let manager = manager(Arc::clone(&pool), Arc::clone(&schema));
    let mut prepaid = 0;
    let mut files = Vec::new();
    for parity in 0..2 {
        let mut writer = manager.create_in_progress_file("tiny multi-batch sources")?;
        let mut maximum = 0;
        for index in 0..ROWS {
            let mut key = [0; 16];
            key[8..].copy_from_slice(&((2 * index + parity) as u64).to_be_bytes());
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(FixedSizeBinaryArray::try_from_iter([key].into_iter())?),
                    Arc::new(StringArray::from(vec![format!(
                        "row-{:03}",
                        2 * index + parity
                    )])),
                ],
            )?;
            maximum = maximum.max(writer.append_batch(&batch)?);
        }
        files.push(SortedSpillFile {
            file: writer.finish()?.unwrap(),
            max_record_batch_memory: maximum,
        });
        prepaid += 4 * maximum;
    }
    let reservation = MemoryConsumer::new("vector growth control").register(&pool);
    reservation.try_grow(prepaid)?;
    let order = [PhysicalSortExpr::new_default(Arc::new(Column::new(
        "key", 0,
    )))]
    .into();
    let mut stream = StreamingMergeBuilder::new()
        .with_sorted_spill_files(files)
        .with_spill_manager(manager)
        .with_schema(schema)
        .with_expressions(&order)
        .with_metrics(BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0))
        .with_batch_size(128)
        .with_reservation(reservation)
        .build()?;
    // 64 one-row batches must coexist before the first output. The builder's
    // initial 2*streams=4 slots therefore grows repeatedly, including realloc.
    let output = stream.next().await.unwrap()?;
    assert_eq!(output.num_rows(), 2 * ROWS);
    let values = output
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    for index in 0..2 * ROWS {
        assert_eq!(values.value(index), format!("row-{index:03}"));
    }
    assert!(stream.next().await.is_none());
    let alias = output.column(1).slice(0, 1);
    drop(output);
    drop(stream);
    assert!(pool.reserved() >= alias.to_data().buffers()[1].capacity());
    drop(alias);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn spill_admission_fixed_compound_columns_preserve_nullable_boolean_and_float_bits()
-> Result<()> {
    use arrow::array::{BooleanArray, Float64Array, Int64Array};
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(2 * 1024 * 1024));
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("flag", DataType::Boolean, true),
        Field::new("float", DataType::Float64, false),
    ]));
    let batches = [0, 1].map(|parity| {
        let ids = (0..32).map(|i| 2 * i + parity).collect::<Vec<_>>();
        let floats = ids
            .iter()
            .map(|id| match id % 4 {
                0 => -0.0_f64,
                1 => 0.0_f64,
                2 => f64::from_bits(0x7ff8_0000_0000_0001),
                _ => *id as f64,
            })
            .collect::<Vec<_>>();
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids.clone())),
                Arc::new(BooleanArray::from(
                    ids.iter()
                        .map(|id| (*id % 3 != 0).then_some(*id % 2 != 0))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(floats)),
            ],
        )
        .unwrap()
    });
    let (mut stream, _) = merge_by(&pool, &batches, 8, &[0, 1, 2])?;
    let mut expected = 0_i64;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let flags = batch
            .column(1)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let floats = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert_eq!(ids.value(row), expected);
            assert_eq!(flags.is_valid(row), expected % 3 != 0);
            if flags.is_valid(row) {
                assert_eq!(flags.value(row), expected % 2 != 0);
            }
            let value = match expected % 4 {
                0 => -0.0_f64,
                1 => 0.0_f64,
                2 => f64::from_bits(0x7ff8_0000_0000_0001),
                _ => expected as f64,
            };
            assert_eq!(floats.value(row).to_bits(), value.to_bits());
            expected += 1;
        }
    }
    assert_eq!(expected, 64);
    drop(stream);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

async fn escaped_input_credit(can_spill: bool) -> Result<()> {
    const ROWS: usize = 64;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(16 * 1024 * 1024));
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::FixedSizeBinary(16), false),
        Field::new("small", DataType::Utf8View, false),
        Field::new("large", DataType::Utf8View, false),
    ]));
    let batches = [0, 1].map(|parity| {
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(keys(parity, ROWS)),
                Arc::new(StringViewArray::from_iter_values(std::iter::repeat_n(
                    "s".repeat(64),
                    ROWS,
                ))),
                Arc::new(StringViewArray::from_iter_values(std::iter::repeat_n(
                    "l".repeat(4096),
                    ROWS,
                ))),
            ],
        )
        .unwrap()
    });
    // Compression gives the small and large payloads distinct physical owners.
    // An uncompressed IPC body's complete backing legitimately includes both,
    // and StringView slices preserve every source data-buffer handle.
    let (mut stream, prepaid) = merge_sponsored_codec(
        &pool,
        &batches,
        8,
        &[0],
        can_spill,
        Some(datafusion::common::config::SpillCompression::Zstd),
    )?;
    let first = stream.next().await.unwrap()?;
    let alias = first.column(1).slice(0, 1);
    assert_eq!(
        alias
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap()
            .value(0),
        "s".repeat(64)
    );
    let small = alias.as_any().downcast_ref::<StringViewArray>().unwrap();
    let large = first
        .column(2)
        .as_any()
        .downcast_ref::<StringViewArray>()
        .unwrap();
    assert!(
        small
            .data_buffers()
            .iter()
            .all(
                |left| large.data_buffers().iter().all(|right| left.capacity() == 0
                    || right.capacity() == 0
                    || left.data_ptr() != right.data_ptr())
            ),
        "the codec must actually yield distinct small/large physical payload owners"
    );
    let mut owners = std::collections::HashMap::new();
    // A view slice retains EVERY data-buffer handle, even handles unused by
    // this row. Count complete physical owners, including its descriptor.
    for buffer in alias.to_data().buffers() {
        owners
            .entry(buffer.data_ptr())
            .and_modify(|capacity: &mut usize| {
                *capacity = (*capacity).max(buffer.capacity());
            })
            .or_insert(buffer.capacity());
    }
    let source: usize = owners.values().sum();
    assert!(
        source < prepaid / 4,
        "the fixture must have a genuinely small physical escaped set: {source} of {prepaid}"
    );
    drop(first);
    let mut rows = 8;
    while let Some(batch) = stream.next().await {
        rows += batch?.num_rows();
    }
    assert_eq!(rows, 2 * ROWS);
    // EOF retires reader roots even while this fused stream remains retained.
    let at_eof = pool.reserved();
    drop(stream);
    assert!(
        pool.reserved() >= source,
        "a one-row alias keeps its whole physical owner"
    );
    if can_spill {
        assert!(
            pool.reserved() >= prepaid / 2,
            "spillable sponsors keep the existing reservation/loan lifecycle"
        );
    } else {
        assert!(
            pool.reserved() < prepaid / 4,
            "a tiny escaped leaf cannot pin unused reader credit or sibling payloads: {} of {prepaid}",
            pool.reserved()
        );
        assert!(
            at_eof < prepaid / 4,
            "retaining the fused stream cannot retain an open reader root: {at_eof} of {prepaid}"
        );
    }
    drop(alias);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}
#[tokio::test]
async fn spill_admission_retired_reader_credit_follows_only_the_tiny_escaped_owner() -> Result<()> {
    escaped_input_credit(false).await
}
#[tokio::test]
async fn spill_admission_spillable_sponsor_keeps_its_existing_credit_lifecycle() -> Result<()> {
    escaped_input_credit(true).await
}

#[tokio::test]
async fn spill_admission_variable_compound_columns_preserve_nulls_and_encoding_boundaries()
-> Result<()> {
    use arrow::array::Int64Array;
    const ROWS: usize = 128;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(4 * 1024 * 1024));
    let lengths = [0, 1, 8, 9, 12, 13, 31, 32, 33, 63, 64, 65, 1024];
    let text = |id: usize| (id % 7 != 0).then(|| "x".repeat(lengths[id % lengths.len()]));
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("view", DataType::Utf8View, true),
    ]));
    let batches = [0, 1].map(|parity| {
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from_iter_values(
                    (0..ROWS).map(|i| (2 * i + parity) as i64),
                )),
                Arc::new(StringViewArray::from(
                    (0..ROWS).map(|i| text(2 * i + parity)).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    });
    let (mut stream, _) = merge_by(&pool, &batches, 7, &[0, 1])?;
    let mut expected = 0;
    let mut alias = None;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let views = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert_eq!(keys.value(row), expected as i64);
            assert_eq!(views.iter().nth(row).unwrap(), text(expected).as_deref());
            expected += 1;
        }
        if alias.is_none() {
            alias = Some(batch.column(1).slice(0, 1));
        }
    }
    assert_eq!(expected, 2 * ROWS);
    drop(stream);
    drop(alias);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn spill_admission_prepaid_reader_cancel_keeps_the_active_root_until_worker_exit()
-> Result<()> {
    let observed = Arc::new(BlockingReadPool {
        inner: GreedyMemoryPool::new(16 * 1024 * 1024),
        block_prepaid: true,
        blocked: std::sync::atomic::AtomicBool::new(false),
        release: (std::sync::Mutex::new(false), std::sync::Condvar::new()),
        started: tokio::sync::Notify::new(),
        changed: tokio::sync::Notify::new(),
    });
    struct Release(Arc<BlockingReadPool>);
    impl Drop for Release {
        fn drop(&mut self) {
            *self.0.release.0.lock().unwrap() = true;
            self.0.release.1.notify_all();
        }
    }
    let release = Release(Arc::clone(&observed));
    let pool: Arc<dyn MemoryPool> = observed.clone();
    let batches = [rich_batch(0, 64), rich_batch(1, 64)];
    let (mut stream, prepaid) = merge(&pool, &batches, 8)?;
    {
        let read = stream.next();
        tokio::pin!(read);
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            tokio::select! {
                _ = observed.started.notified() => {},
                result = &mut read => panic!("worker barrier bypassed: {result:?}"),
            }
        })
        .await
        .expect("a real prepaid reader reaches its decode boundary");
    }
    drop(stream);
    assert!(
        pool.reserved() >= prepaid / 2,
        "last async stream drop cannot retire a still-active root or its credit"
    );
    drop(release);
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
    .expect("the final worker retires credit and releases every guard");
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

fn primitive_field_batch(parity: usize, rows: usize) -> RecordBatch {
    use arrow::array::Int64Array;
    // Slice a larger allocation; repeated keys and stored NULL byte spans must
    // survive the real writer, Field cursor and selected interleave unchanged.
    let key = Int64Array::from(
        (0..rows + 2)
            .map(|i| (i > 1).then_some(((2 * i + parity) / 3) as i64))
            .collect::<Vec<_>>(),
    );
    let text = stored_null_strings(parity, rows + 2, 5);
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, true),
            Field::new("value", DataType::Utf8, true),
        ])),
        vec![Arc::new(key), Arc::new(text)],
    )
    .unwrap()
    .slice(1, rows + 1)
}

#[tokio::test]
async fn spill_admission_primitive_field_releases_idle_copy_and_retains_escaped_output()
-> Result<()> {
    const LIMIT: usize = 16 * 1024 * 1024;
    const HEADROOM: usize = 1024 * 1024;
    const ROWS: usize = 2048;
    let observed = Arc::new(CopyPhasePool {
        inner: GreedyMemoryPool::new(LIMIT),
        reading: std::sync::atomic::AtomicBool::new(false),
        restored: std::sync::atomic::AtomicBool::new(false),
        release: (std::sync::Mutex::new(false), std::sync::Condvar::new()),
        started: tokio::sync::Notify::new(),
        changed: tokio::sync::Notify::new(),
    });
    struct Release(Arc<CopyPhasePool>);
    impl Drop for Release {
        fn drop(&mut self) {
            *self.0.release.0.lock().unwrap() = true;
            self.0.release.1.notify_all();
        }
    }
    let release = Release(Arc::clone(&observed));
    let pool: Arc<dyn MemoryPool> = observed.clone();
    let batches = [
        primitive_field_batch(0, ROWS),
        primitive_field_batch(1, ROWS),
    ];
    let roomy: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(LIMIT));
    let (mut oracle_stream, _) = merge(&roomy, &batches, 16)?;
    let mut oracle_batches = Vec::new();
    while let Some(batch) = oracle_stream.next().await {
        oracle_batches.push(batch?);
    }
    let oracle = arrow::compute::concat_batches(&batches[0].schema(), &oracle_batches)?;
    drop(oracle_batches);
    drop(oracle_stream);
    assert_eq!(roomy.reserved(), 0);

    let (mut stream, prepaid) = merge(&pool, &batches, 16)?;
    {
        let read = stream.next();
        tokio::pin!(read);
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            tokio::select! {
                _ = observed.started.notified() => {},
                result = &mut read => panic!("reader barrier bypassed: {result:?}"),
            }
        })
        .await
        .expect("a real primitive-field reader reaches its metadata boundary");
    }
    assert!(prepaid / 2 > HEADROOM);
    let pressure = MemoryConsumer::new("independent primitive-copy pressure").register(&pool);
    pressure.try_grow(LIMIT - pool.reserved() - HEADROOM)?;
    drop(release);
    let mut offset = 0;
    let mut alias = None;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        assert_eq!(batch, oracle.slice(offset, batch.num_rows()));
        let text = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(text.values().len(), 128 * batch.num_rows());
        if alias.is_none() {
            alias = Some(batch.column(1).slice(batch.num_rows() - 1, 1));
        }
        offset += batch.num_rows();
    }
    assert_eq!(offset, 2 * (ROWS + 1));
    drop(stream);
    drop(pressure);
    let alias = alias.unwrap();
    assert!(pool.reserved() >= alias.to_data().buffers()[1].capacity());
    drop(alias);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn spill_admission_primitive_field_constructor_refuses_before_prefetch() -> Result<()> {
    const LIMIT: usize = 16 * 1024 * 1024;
    let observed = Arc::new(CopyPhasePool {
        inner: GreedyMemoryPool::new(LIMIT),
        reading: std::sync::atomic::AtomicBool::new(false),
        restored: std::sync::atomic::AtomicBool::new(false),
        release: (std::sync::Mutex::new(true), std::sync::Condvar::new()),
        started: tokio::sync::Notify::new(),
        changed: tokio::sync::Notify::new(),
    });
    let pool: Arc<dyn MemoryPool> = observed.clone();
    let batches = [primitive_field_batch(0, 2), primitive_field_batch(1, 2)];
    let (mut stream, _) = merge(&pool, &batches, LIMIT / size_of::<(usize, usize)>())?;
    let error = stream.next().await.unwrap().expect_err(
        "the complete constructor index allocation must be admitted before reader prefetch",
    );
    assert!(matches!(
        error.find_root(),
        datafusion::common::DataFusionError::ResourcesExhausted(_)
    ));
    assert!(!observed.reading.load(std::sync::atomic::Ordering::Acquire));
    drop(stream);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn spill_admission_primitive_field_builder_grows_with_live_sources_and_output_alias()
-> Result<()> {
    use arrow::array::Int64Array;
    const ROWS: usize = 32;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(2 * 1024 * 1024));
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let manager = manager(Arc::clone(&pool), Arc::clone(&schema));
    let mut files = Vec::new();
    let mut prepaid = 0;
    for parity in 0..2 {
        let mut writer = manager.create_in_progress_file("primitive multi-batch source")?;
        let mut maximum = 0;
        for index in 0..ROWS {
            let id = 2 * index + parity;
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(vec![id as i64])),
                    Arc::new(StringArray::from(vec![format!("row-{id:03}")])),
                ],
            )?;
            maximum = maximum.max(writer.append_batch(&batch)?);
        }
        files.push(SortedSpillFile {
            file: writer.finish()?.unwrap(),
            max_record_batch_memory: maximum,
        });
        prepaid += 4 * maximum;
    }
    let reservation = MemoryConsumer::new("primitive growth control").register(&pool);
    reservation.try_grow(prepaid)?;
    let order = [PhysicalSortExpr::new_default(Arc::new(Column::new(
        "key", 0,
    )))]
    .into();
    let mut stream = StreamingMergeBuilder::new()
        .with_sorted_spill_files(files)
        .with_spill_manager(manager)
        .with_schema(schema)
        .with_expressions(&order)
        .with_metrics(BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0))
        .with_batch_size(128)
        .with_reservation(reservation)
        .build()?;
    // Sixty-four one-row source batches grow the initial four builder slots.
    let output = stream.next().await.unwrap()?;
    assert_eq!(output.num_rows(), 2 * ROWS);
    let text = output
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    for row in 0..2 * ROWS {
        assert_eq!(text.value(row), format!("row-{row:03}"));
    }
    assert!(stream.next().await.is_none());
    let alias = output.column(1).slice(2 * ROWS - 1, 1);
    drop(output);
    drop(stream);
    assert!(pool.reserved() >= alias.to_data().buffers()[1].capacity());
    drop(alias);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn spill_admission_primitive_field_counts_presized_empty_input_vec_before_prefetch()
-> Result<()> {
    const LIMIT: usize = 2 * 1024 * 1024;
    let observed = Arc::new(CopyPhasePool {
        inner: GreedyMemoryPool::new(LIMIT),
        reading: std::sync::atomic::AtomicBool::new(false),
        restored: std::sync::atomic::AtomicBool::new(false),
        release: (std::sync::Mutex::new(true), std::sync::Condvar::new()),
        started: tokio::sync::Notify::new(),
        changed: tokio::sync::Notify::new(),
    });
    let pool: Arc<dyn MemoryPool> = observed.clone();
    let batches = [primitive_field_batch(0, 2), primitive_field_batch(1, 2)];
    let schema = batches[0].schema();
    let manager = manager(Arc::clone(&pool), Arc::clone(&schema));
    let mut files = Vec::new();
    let mut prepaid = 0;
    for batch in &batches {
        let mut writer = manager.create_in_progress_file("pre-sized empty input vector")?;
        let maximum = writer.append_batch(batch)?;
        files.push(SortedSpillFile {
            file: writer.finish()?.unwrap(),
            max_record_batch_memory: maximum,
        });
        prepaid += 4 * maximum;
    }
    let reservation = MemoryConsumer::new("pre-sized vector sponsor").register(&pool);
    reservation.try_grow(prepaid)?;
    let order = [PhysicalSortExpr::new_default(Arc::new(Column::new(
        "key", 0,
    )))]
    .into();
    // Caller supplies an empty but allocated Vec. Its capacity is real backing,
    // despite all logical merge sources coming from the admitted IPC readers.
    let streams = Vec::with_capacity(LIMIT / size_of::<SendableRecordBatchStream>());
    let mut stream = StreamingMergeBuilder::new()
        .with_streams(streams)
        .with_sorted_spill_files(files)
        .with_spill_manager(manager)
        .with_schema(schema)
        .with_expressions(&order)
        .with_metrics(BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0))
        .with_batch_size(8)
        .with_reservation(reservation)
        .build()?;
    let error = stream
        .next()
        .await
        .unwrap()
        .expect_err("the incoming Vec backing and new Fuse Vec cannot coexist in this pool");
    assert!(matches!(
        error.find_root(),
        datafusion::common::DataFusionError::ResourcesExhausted(_)
    ));
    assert!(!observed.reading.load(std::sync::atomic::Ordering::Acquire));
    drop(stream);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn spill_admission_primitive_field_does_not_migrate_view_payloads() -> Result<()> {
    super::field_tests::observe_legacy_prefetch().await
}
