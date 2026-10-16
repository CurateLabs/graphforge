//! Real IPC merge controls for serial construction and metadata handoffs.
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryArray, Int64Array, ListArray, RecordBatch, StructArray,
    TimestampMicrosecondArray, UInt32Array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_expr::{PhysicalSortExpr, expressions::Column};
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, SpillMetrics};
use datafusion::physical_plan::sorts::streaming_merge::{SortedSpillFile, StreamingMergeBuilder};
use datafusion::physical_plan::spill::SpillManager;
use futures::StreamExt;

// This is the finite target contract of the tighter Bytes control proof.
// Pthread Mutex may own a separate OnceBox allocation; pointer width alone
// does not qualify it. Other targets must exercise the unchanged old fees.
fn tight_metadata_platform() -> bool {
    cfg!(any(
        target_os = "linux",
        all(target_os = "windows", not(target_vendor = "win7")),
        all(target_os = "macos", target_pointer_width = "64")
    )) && matches!(usize::BITS, 32 | 64)
}

fn key(parity: usize, rows: usize) -> FixedSizeBinaryArray {
    FixedSizeBinaryArray::try_from_iter((0..rows).map(|row| {
        let mut value = [0_u8; 16];
        value[8..].copy_from_slice(&((2 * row + parity) as u64).to_be_bytes());
        value
    }))
    .unwrap()
}

fn list(parity: usize, rows: usize) -> ListArray {
    ListArray::new(
        Arc::new(Field::new_list_field(DataType::UInt32, false)),
        OffsetBuffer::new(ScalarBuffer::from(
            (0..=rows).map(|row| row as i32).collect::<Vec<_>>(),
        )),
        Arc::new(UInt32Array::from_iter_values(
            (0..rows).map(|row| (2 * row + parity) as u32),
        )),
        Some(NullBuffer::from(
            (0..rows)
                .map(|row| (2 * row + parity) % 3 != 0)
                .collect::<Vec<_>>(),
        )),
    )
}

fn nested_batch(parity: usize, rows: usize) -> RecordBatch {
    let labels: ArrayRef = Arc::new(list(parity, rows));
    let nested: ArrayRef = Arc::new(StructArray::new(
        vec![Arc::new(Field::new(
            "labels",
            labels.data_type().clone(),
            true,
        ))]
        .into(),
        vec![labels],
        Some(NullBuffer::from(
            (0..rows)
                .map(|row| (2 * row + parity) % 5 != 0)
                .collect::<Vec<_>>(),
        )),
    ));
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(key(parity, rows)),
        Arc::new(list(parity, rows)),
        nested,
    ];
    for offset in 0..4 {
        columns.push(Arc::new(
            TimestampMicrosecondArray::from_iter_values(
                (0..rows).map(|row| (2 * row + parity + offset) as i64),
            )
            .with_timezone("UTC"),
        ));
    }
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                Field::new(format!("column_{index}"), column.data_type().clone(), true)
            })
            .collect::<Vec<_>>(),
    ));
    RecordBatch::try_new(schema, columns).unwrap()
}

fn merge(
    pool: &Arc<dyn MemoryPool>,
    batches: &[RecordBatch],
    output_rows: usize,
) -> Result<SendableRecordBatchStream> {
    let schema = batches[0].schema();
    let env = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(pool))
        .build_arc()?;
    let manager = SpillManager::new(
        env,
        SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
        Arc::clone(&schema),
    );
    let mut files = Vec::new();
    let mut prepaid = 0;
    for batch in batches {
        let mut writer = manager.create_in_progress_file("serial copy phase control")?;
        let maximum = writer.append_batch(batch)?;
        files.push(SortedSpillFile {
            file: writer.finish()?.unwrap(),
            max_record_batch_memory: maximum,
        });
        prepaid += 4 * maximum;
    }
    let reservation = MemoryConsumer::new("serial copy phase control").register(pool);
    reservation.try_grow(prepaid)?;
    let ordering = [PhysicalSortExpr::new_default(Arc::new(Column::new(
        schema.field(0).name(),
        0,
    )))]
    .into();
    StreamingMergeBuilder::new()
        .with_sorted_spill_files(files)
        .with_spill_manager(manager)
        .with_schema(schema)
        .with_expressions(&ordering)
        .with_metrics(BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0))
        .with_batch_size(output_rows)
        .with_reservation(reservation)
        .build()
}

fn oracle(batches: &[RecordBatch]) -> Result<RecordBatch> {
    let joined = arrow::compute::concat_batches(&batches[0].schema(), batches)?;
    let rows = batches[0].num_rows();
    let indices =
        UInt32Array::from_iter_values((0..rows).flat_map(|row| [row as u32, (rows + row) as u32]));
    Ok(arrow::compute::take_record_batch(&joined, &indices)?)
}

#[tokio::test]
async fn copy_phase_serial_nested_columns_fit_fixed_headroom_with_escaped_output() -> Result<()> {
    const LIMIT: usize = 16 * 1024 * 1024;
    const HEADROOM: usize = 384 * 1024;
    const ROWS: usize = 8192;
    const OUTPUT_ROWS: usize = 4096;
    let batches = [nested_batch(0, ROWS), nested_batch(1, ROWS)];
    let expected = oracle(&batches)?;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(LIMIT));
    let mut stream = merge(&pool, &batches, OUTPUT_ROWS)?;
    // First output completes both real IPC reads and UUID row conversion before
    // introducing pressure. The test therefore isolates the next copy operation.
    let first = stream.next().await.unwrap()?;
    assert_eq!(first, expected.slice(0, OUTPUT_ROWS));
    let escaped = first.column(3).slice(17, 1);
    let escaped_capacity = escaped.to_data().buffers()[0].capacity();
    assert!(escaped_capacity >= OUTPUT_ROWS * size_of::<i64>());
    drop(first);
    let pressure = MemoryConsumer::new("independent work during serial copy").register(&pool);
    pressure.try_grow(LIMIT - pool.reserved() - HEADROOM)?;
    let qualified = tight_metadata_platform();
    let mut refused = false;
    let mut offset = OUTPUT_ROWS;
    while let Some(batch) = stream.next().await {
        let batch = match batch {
            Ok(batch) => {
                assert!(
                    qualified,
                    "unqualified target must retain the old copy fees"
                );
                batch
            }
            Err(error) => {
                assert!(!qualified, "qualified serial copy must complete: {error}");
                assert!(
                    matches!(error.find_root(), DataFusionError::ResourcesExhausted(_)),
                    "{error}"
                );
                assert!(
                    error.to_string().contains("serial copy phase control"),
                    "{error}"
                );
                refused = true;
                break;
            }
        };
        assert_eq!(batch, expected.slice(offset, batch.num_rows()));
        let labels = batch
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(labels.values().len(), batch.num_rows());
        assert!(
            labels
                .values()
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
                .eq((offset..offset + batch.num_rows()).map(|row| row as u32)),
            "stored children under NULL lists preserve every raw value"
        );
        let nested = batch
            .column(2)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let labels = nested
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(
            labels.values().len(),
            batch.num_rows(),
            "NULL parents cannot discard stored list child spans"
        );
        assert!(
            labels
                .values()
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
                .eq((offset..offset + batch.num_rows()).map(|row| row as u32)),
            "stored children under NULL Struct/List parents preserve every raw value"
        );
        offset += batch.num_rows();
    }
    if qualified {
        assert_eq!(offset, 2 * ROWS);
    } else {
        assert!(
            refused,
            "the fixed quota must exercise original-fee refusal"
        );
        assert_eq!(offset, OUTPUT_ROWS);
    }
    drop(stream);
    drop(pressure);
    assert!(pool.reserved() >= escaped_capacity);
    assert_eq!(
        escaped
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .value(0),
        17
    );
    drop(escaped);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn copy_phase_reconciliation_refusal_keeps_prior_output_and_releases_partial_copy()
-> Result<()> {
    const LIMIT: usize = 8 * 1024 * 1024;
    const HEADROOM: usize = 64 * 1024;
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::FixedSizeBinary(16), false),
        Field::new("value".repeat(3277), DataType::Int64, false),
    ]));
    let batches = [0, 1].map(|parity| {
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(key(parity, 64)),
                Arc::new(Int64Array::from_iter_values(
                    (0..64).map(|row| (2 * row + parity) as i64),
                )),
            ],
        )
        .unwrap()
    });
    let expected = oracle(&batches)?;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(LIMIT));
    let mut stream = merge(&pool, &batches, 16)?;
    let first = stream.next().await.unwrap()?;
    assert_eq!(first, expected.slice(0, 16));
    let escaped = first.column(1).slice(0, 1);
    let full_capacity = escaped.to_data().buffers()[0].capacity();
    drop(first);
    let pressure = MemoryConsumer::new("independent work before output handoff").register(&pool);
    pressure.try_grow(LIMIT - pool.reserved() - HEADROOM)?;
    let error = stream.next().await.unwrap().expect_err(
        "the tiny kernel cannot authorize allocation of the larger output metadata handoff",
    );
    assert!(matches!(
        error.find_root(),
        DataFusionError::ResourcesExhausted(_)
    ));
    if tight_metadata_platform() {
        assert!(
            error.to_string().contains("Merge output reconciliation"),
            "{error}"
        );
    } else {
        assert!(
            error.to_string().contains("serial copy phase control"),
            "{error}"
        );
        assert!(
            !error.to_string().contains("Merge output reconciliation"),
            "{error}"
        );
    }
    drop(stream);
    drop(pressure);
    assert!(pool.reserved() >= full_capacity);
    assert_eq!(
        escaped
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        0
    );
    drop(escaped);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

// Append to the existing copy_phase_tests.rs module; no product helper/API is added.
fn multi_frame_merge(
    pool: &Arc<dyn MemoryPool>,
    batches: &[RecordBatch; 2],
    frame_rows: usize,
    output_rows: usize,
) -> Result<(SendableRecordBatchStream, usize)> {
    let schema = batches[0].schema();
    let env = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(pool))
        .build_arc()?;
    let manager = SpillManager::new(
        env,
        SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
        Arc::clone(&schema),
    );
    let mut files = Vec::new();
    let mut prepaid = 0;
    for batch in batches {
        let mut writer = manager.create_in_progress_file("consumed merge input control")?;
        let mut maximum = 0;
        for offset in (0..batch.num_rows()).step_by(frame_rows) {
            maximum = maximum.max(writer.append_batch(&batch.slice(offset, frame_rows))?);
        }
        files.push(SortedSpillFile {
            file: writer.finish()?.unwrap(),
            max_record_batch_memory: maximum,
        });
        prepaid += 4 * maximum;
    }
    let reservation = MemoryConsumer::new("consumed merge input control").register(pool);
    reservation.try_grow(prepaid)?;
    let ordering = [PhysicalSortExpr::new_default(Arc::new(Column::new(
        schema.field(0).name(),
        0,
    )))]
    .into();
    let stream = StreamingMergeBuilder::new()
        .with_sorted_spill_files(files)
        .with_spill_manager(manager)
        .with_schema(schema)
        .with_expressions(&ordering)
        .with_metrics(BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0))
        .with_batch_size(output_rows)
        .with_reservation(reservation)
        .build()?;
    // The unqualified-target path intentionally keeps the old input sponsor.
    // Two selected sources use the existing two-batch buffered-read authority.
    Ok((stream, prepaid / 2))
}

#[tokio::test]
async fn copy_phase_releases_consumed_ipc_inputs_with_pending_refills_and_live_output() -> Result<()>
{
    const LIMIT: usize = 32 * 1024 * 1024;
    const FRAME_ROWS: usize = 8192;
    const FRAMES: usize = 3;
    const OUTPUT_ROWS: usize = 4095;
    // This covers the 64KiB index Vec, nested schema fees, container capacities,
    // RowConverter/merge controls, Arc/Box owners and all reusable carrier Vecs.
    // Physical Rows and the escaped payload are counted separately below.
    const OTHER_RESIDENT: usize = 512 * 1024;
    let batches = [
        nested_batch(0, FRAMES * FRAME_ROWS),
        nested_batch(1, FRAMES * FRAME_ROWS),
    ];
    let expected = oracle(&batches)?;
    // Both private reusable Rows slots start empty, clear before append, and
    // never append more than FRAME_ROWS fixed-width, non-NULL UUID keys. Use
    // Arrow's public actual Rows allocation as an independent upper bound for
    // each slot, rather than mirroring our admission helper's construction fee.
    let converter = arrow::row::RowConverter::new(vec![arrow::row::SortField::new(
        DataType::FixedSizeBinary(16),
    )])?;
    let mut rows = converter.empty_rows(0, 0);
    converter.append(&mut rows, &[batches[0].column(0).slice(0, FRAME_ROWS)])?;
    let one_rows_cache = rows.size();
    drop(rows);
    drop(converter);

    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(LIMIT));
    let (mut stream, old_input_floor) =
        multi_frame_merge(&pool, &batches, FRAME_ROWS, OUTPUT_ROWS)?;
    let first = stream.next().await.unwrap()?;
    assert_eq!(first, expected.slice(0, OUTPUT_ROWS));
    let escaped = first.column(3).slice(17, 1);
    let escaped_capacity = escaped.to_data().buffers()[0].capacity();
    assert!(escaped_capacity >= OUTPUT_ROWS * size_of::<i64>());
    drop(first);
    let mut offset = OUTPUT_ROWS;
    let mut last_output_rows = OUTPUT_ROWS;
    while offset < expected.num_rows() {
        let batch = stream
            .next()
            .await
            .expect("all source rows must be emitted")?;
        last_output_rows = batch.num_rows();
        assert_eq!(batch, expected.slice(offset, batch.num_rows()));
        for column in [1, 2] {
            let array = if column == 1 {
                batch.column(column).as_ref()
            } else {
                batch
                    .column(column)
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap()
                    .column(0)
                    .as_ref()
            };
            let list = array.as_any().downcast_ref::<ListArray>().unwrap();
            assert_eq!(list.values().len(), batch.num_rows());
            assert!(
                list.values()
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
                    .eq((offset..offset + batch.num_rows()).map(|row| row as u32)),
                "pending-index remapping preserves stored children under NULL parents"
            );
        }
        offset += batch.num_rows();
    }
    assert_eq!(offset, FRAMES * FRAME_ROWS * 2);
    // A final partial output requires the merge to observe both input EOFs.
    // Keep its inner merge alive: polling the OUTER stream to None would retire
    // the whole inner stream and hide stale source ownership from this check.
    assert_eq!(last_output_rows, 12);
    // Qualified reader sponsors have retired, and the fresh output leaf cannot
    // need either final source body's ownership.
    let resident_ceiling = 4 * one_rows_cache + escaped_capacity + OTHER_RESIDENT;
    if tight_metadata_platform() {
        assert!(
            pool.reserved() <= resident_ceiling,
            "consumed IPC bodies survived actual EOF: held={} ceiling={resident_ceiling}",
            pool.reserved()
        );
        // Both inputs reached actual EOF before the final partial output.
        // Neither reusable Rows slots nor previous-key cursors can have a
        // future user; keep the original source-body assertion above too.
        let eof_resident_ceiling = escaped_capacity + OTHER_RESIDENT;
        assert!(
            pool.reserved() <= eof_resident_ceiling,
            "exhausted merge retains row-key caches: held={} ceiling={eof_resident_ceiling}",
            pool.reserved()
        );
    } else {
        assert!(
            pool.reserved() >= old_input_floor,
            "unqualified targets preserve the existing complete input sponsor"
        );
    }
    assert_eq!(
        escaped
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .value(0),
        17
    );
    assert!(stream.next().await.is_none());
    drop(stream);
    assert!(pool.reserved() >= escaped_capacity);
    assert_eq!(
        escaped
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .value(0),
        17
    );
    drop(escaped);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

// Real ordinary IPC is an independent payload oracle; the merge under test
// must still use its separate private spill writer/reader and Row capability.
#[cfg(all(
    any(
        target_os = "linux",
        all(target_os = "windows", not(target_vendor = "win7")),
        all(target_os = "macos", target_pointer_width = "64")
    ),
    any(target_pointer_width = "32", target_pointer_width = "64")
))]
fn copy_phase_plain_ipc(batch: &RecordBatch) -> Result<RecordBatch> {
    let mut bytes = Vec::new();
    {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut bytes, &batch.schema())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    assert!(
        bytes.len() < 64 * 1024,
        "fixture IPC must stay physically small"
    );
    let mut reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)?;
    let decoded = reader.next().expect("one ordinary IPC batch")?;
    assert!(reader.next().is_none());
    Ok(decoded)
}

// This control exercises the sealed dynamic Row path only. A legacy kernel
// without checked output admission cannot safely consume the huge logical
// Null child used by the separate overflow control below.
#[cfg(all(
    any(
        target_os = "linux",
        all(target_os = "windows", not(target_vendor = "win7")),
        all(target_os = "macos", target_pointer_width = "64")
    ),
    any(target_pointer_width = "32", target_pointer_width = "64")
))]
#[tokio::test]
async fn copy_phase_row_dictionary_payload_refuses_before_an_unbounded_kernel() -> Result<()> {
    use arrow::array::{DictionaryArray, Int32Array};
    use arrow::datatypes::Int32Type;

    let batches = (0..2)
        .map(|parity| -> Result<RecordBatch> {
            let dictionary: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![Some(2), None, Some(0), Some(2)]),
                Arc::new(key(0, 3)),
            )?);
            let schema = Arc::new(Schema::new(vec![
                Field::new("uuid_key", DataType::FixedSizeBinary(16), false),
                Field::new("dictionary_payload", dictionary.data_type().clone(), true),
            ]));
            let batch = RecordBatch::try_new(schema, vec![Arc::new(key(parity, 4)), dictionary])?;
            let decoded = copy_phase_plain_ipc(&batch)?;
            assert_eq!(decoded, batch);
            Ok(decoded)
        })
        .collect::<Result<Vec<_>>>()?;
    let expected = oracle(&batches)?;
    assert_eq!(expected.num_rows(), 8);
    // A physically roomy pool makes this a missing-bound refusal control,
    // rather than a pressure failure before the unsupported payload is reached.
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(16 * 1024 * 1024));
    let mut stream = merge(&pool, &batches, 8)?;
    let error = stream
        .next()
        .await
        .expect("the unsupported output kernel must produce a typed error")
        .expect_err("a UUID Row key does not qualify dictionary payload allocation");
    assert!(
        matches!(error.find_root(), DataFusionError::ResourcesExhausted(_)),
        "{error}"
    );
    assert!(
        error
            .to_string()
            .contains("Row interleave lacks a checked payload allocation bound"),
        "{error}"
    );
    assert!(
        error.to_string().contains("serial copy phase control"),
        "{error}"
    );
    drop(stream);
    assert!(
        pool.reserved() > 0,
        "live dictionary source aliases retain the writer's buffer authority"
    );
    drop(expected);
    drop(batches);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[cfg(all(
    any(
        target_os = "linux",
        all(target_os = "windows", not(target_vendor = "win7")),
        all(target_os = "macos", target_pointer_width = "64")
    ),
    target_pointer_width = "64"
))]
#[tokio::test]
async fn copy_phase_offset_overflow_emits_small_prefix_before_large_allocation_refusal()
-> Result<()> {
    use arrow::array::NullArray;

    let batches = (0..2)
        .map(|parity| -> Result<RecordBatch> {
            // NullArray owns only its length. This is a VALID list with a huge
            // logical child, not an invalid offset or a GiB payload allocation.
            let child = Arc::new(NullArray::new(i32::MAX as usize));
            assert_eq!(child.get_buffer_memory_size(), 0);
            assert!(child.to_data().buffers().is_empty());
            let list: ArrayRef = Arc::new(ListArray::try_new(
                Arc::new(Field::new_list_field(DataType::Null, true)),
                OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 0, i32::MAX])),
                child,
                None,
            )?);
            assert_eq!(list.to_data().buffers()[0].len(), 3 * size_of::<i32>());
            let schema = Arc::new(Schema::new(vec![
                Field::new("uuid_key", DataType::FixedSizeBinary(16), false),
                Field::new("metadata_only_list", list.data_type().clone(), false),
            ]));
            let batch = RecordBatch::try_new(schema, vec![Arc::new(key(parity, 2)), list])?;
            let decoded = copy_phase_plain_ipc(&batch)?;
            let decoded_list = decoded
                .column(1)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            assert_eq!(decoded_list.value_offsets(), &[0, 0, i32::MAX]);
            assert_eq!(decoded_list.values().len(), i32::MAX as usize);
            assert_eq!(decoded_list.values().get_buffer_memory_size(), 0);
            Ok(decoded)
        })
        .collect::<Result<Vec<_>>>()?;
    const LIMIT: usize = 16 * 1024 * 1024;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(LIMIT));
    let mut stream = merge(&pool, &batches, 4)?;
    // Four selected rows overflow i32 child offsets. The existing retry halves
    // them to the first two empty lists, which require no child-index storage.
    let first = stream.next().await.expect("small prefix must be emitted")?;
    assert_eq!(first.num_rows(), 2);
    let expected_keys = FixedSizeBinaryArray::try_from_iter([0_u64, 1].into_iter().map(|value| {
        let mut key = [0_u8; 16];
        key[8..].copy_from_slice(&value.to_be_bytes());
        key
    }))?;
    assert_eq!(first.column(0).as_ref(), &expected_keys as &dyn Array);
    let lists = first
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(lists.value_offsets(), &[0, 0, 0]);
    assert_eq!(lists.values().len(), 0);
    drop(first);
    // The remaining pair also overflows; retrying one row is representable,
    // but its >32 GiB child-index Vec must be refused BEFORE its allocation.
    let error = stream
        .next()
        .await
        .expect("huge prefix must refuse safely")
        .expect_err("logical Null spans cannot authorize a huge child-index allocation");
    assert!(
        matches!(error.find_root(), DataFusionError::ResourcesExhausted(_)),
        "{error}"
    );
    assert!(
        error.to_string().contains("serial copy phase control"),
        "{error}"
    );
    assert!(pool.reserved() <= LIMIT);
    drop(stream);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}
