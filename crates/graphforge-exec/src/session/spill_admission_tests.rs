//! Real spill I/O controls for admission and escaped Arrow owners (#2146).
use std::sync::Arc;

use arrow::array::{Array, RecordBatch, StringArray, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::Result;
use datafusion::common::utils::memory::get_record_batch_memory_size as physical_backing;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, SpillMetrics};
use futures::StreamExt;

use datafusion::physical_plan::spill::SpillManager;

fn manager(pool: Arc<dyn MemoryPool>, schema: Arc<Schema>) -> SpillManager {
    let env = RuntimeEnvBuilder::new()
        .with_memory_pool(pool)
        .build_arc()
        .unwrap();
    SpillManager::new(
        env,
        SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
        schema,
    )
}

#[tokio::test]
async fn spill_admission_follows_yielded_batch_clones_and_slices_after_stream_drop() -> Result<()> {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(16 * 1024 * 1024));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8,
        false,
    )]));
    let text = "v".repeat(4096);
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from_iter_values(
            std::iter::repeat_n(text.as_str(), 64),
        ))],
    )?;
    let manager = manager(Arc::clone(&pool), schema);
    let file = manager
        .spill_record_batch_and_finish(std::slice::from_ref(&batch), "alias control")?
        .unwrap();
    let mut stream = manager.read_spill_as_stream_unbuffered(file, None)?;
    let read = stream.next().await.unwrap()?;
    assert_eq!(
        read, batch,
        "roomy real IPC roundtrip preserves every cell and schema"
    );
    let alias = read.slice(17, 1);
    assert!(
        alias.get_array_memory_size() >= 64 * 4096,
        "a slice retains the full decoded backing"
    );
    assert!(
        pool.reserved() >= physical_backing(&read),
        "yielded IPC backing must be admitted"
    );
    drop(read);
    drop(stream);
    assert!(
        pool.reserved() >= physical_backing(&alias),
        "stream drop cannot release an escaped slice's authority"
    );
    drop(alias);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[test]
fn spill_admission_refuses_view_gc_and_ipc_workspace_when_only_source_fits() -> Result<()> {
    const LIMIT: usize = 4 * 1024 * 1024;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(LIMIT));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8View,
        false,
    )]));
    let text = "view".repeat(1024);
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringViewArray::from_iter_values(
            std::iter::repeat_n(text.as_str(), 64),
        ))],
    )?;
    let source = MemoryConsumer::new("test retained spill source").register(&pool);
    source.try_grow(batch.get_array_memory_size())?;
    let manager = manager(Arc::clone(&pool), schema);
    let mut file = manager.create_in_progress_file("view workspace control")?;
    let pressure = MemoryConsumer::new("test independent pressure").register(&pool);
    pressure.try_grow(LIMIT - pool.reserved() - 1024)?;
    let retained = pool.reserved();
    let error = file
        .append_batch(&batch)
        .expect_err("source admission cannot authorize view-GC and IPC copies");
    assert!(
        matches!(
            error.find_root(),
            datafusion::common::DataFusionError::ResourcesExhausted(_)
        ),
        "{error}"
    );
    drop(file);
    assert_eq!(
        pool.reserved(),
        retained,
        "failed workspace admission preserves the source and independent pressure"
    );
    drop(batch);
    drop(source);
    drop(pressure);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn spill_admission_view_gc_encodes_compact_copies_while_full_source_capacity_stays_admitted()
-> Result<()> {
    use arrow::array::{Int64Array, builder::StringViewBuilder};
    use arrow::buffer::NullBuffer;

    async fn roundtrip(limit: usize, constrain: bool) -> Result<Vec<(i64, Option<String>)>> {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(limit));
        let mut builder = StringViewBuilder::with_capacity(256)
            .with_fixed_block_size(256 * 1024)
            .with_deduplicate_strings();
        for _ in 0..256 {
            builder.append_value("v".repeat(32));
        }
        let original = builder.finish();
        assert_eq!(original.data_buffers()[0].capacity(), 256 * 1024);
        // NULL cells deliberately retain non-inline raw views. GC copies each
        // raw reference, even when repeated or hidden by the validity bitmap.
        let strings = StringViewArray::new(
            original.views().clone(),
            original.data_buffers().to_vec(),
            Some(NullBuffer::from(
                (0..256).map(|row| row % 3 != 0).collect::<Vec<_>>(),
            )),
        );
        drop(original);
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, false),
            Field::new("value", DataType::Utf8View, true),
        ]));
        let input = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from_iter_values(0..256)),
                Arc::new(strings),
            ],
        )?
        .slice(97, 64);
        let source = MemoryConsumer::new("wide spill source capacity").register(&pool);
        source.try_grow(input.get_array_memory_size())?;
        assert!(source.size() >= 256 * 1024);
        let manager = manager(Arc::clone(&pool), schema);
        let pressure = MemoryConsumer::new("independent compact-spill pressure").register(&pool);
        if constrain {
            // The former fourfold source-capacity encoding bound exceeds 1MiB;
            // actual GC and encoding fit, without borrowing source authority.
            pressure.try_grow(limit - pool.reserved() - 128 * 1024)?;
        }
        let baseline = pool.reserved();
        let mut writer = manager.create_in_progress_file("compact view workspace")?;
        writer.append_batch(&input)?;
        assert_eq!(
            pool.reserved(),
            baseline,
            "temporary GC/encoding workspace releases without touching live sources"
        );
        let file = writer.finish()?.unwrap();
        drop(writer);
        drop(pressure);
        let mut reader = manager.read_spill_as_stream_unbuffered(file, None)?;
        let output = reader.next().await.unwrap()?;
        assert_eq!(output, input);
        let views = output
            .column(1)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        assert_eq!(
            views
                .data_buffers()
                .iter()
                .map(|buffer| buffer.len())
                .sum::<usize>(),
            64 * 32,
            "GC copies duplicate references and NULL stored views in full"
        );
        assert!(reader.next().await.is_none());
        drop(reader);
        drop(input);
        drop(source);
        assert!(pool.reserved() >= physical_backing(&output));
        let groups = output
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        // Scalar copies make the roomy oracle independent of IPC buffer guards.
        let oracle = (0..output.num_rows())
            .map(|row| {
                (
                    groups.value(row),
                    views.is_valid(row).then(|| views.value(row).to_owned()),
                )
            })
            .collect();
        drop(output);
        assert_eq!(pool.reserved(), 0);
        Ok(oracle)
    }

    let roomy = roundtrip(8 * 1024 * 1024, false).await?;
    let constrained = roundtrip(2 * 1024 * 1024, true).await?;
    assert_eq!(constrained, roomy);
    Ok(())
}

#[tokio::test]
async fn spill_admission_dictionary_tracker_releases_keys_and_values_at_writer_drop() -> Result<()>
{
    use arrow::array::{DictionaryArray, Int8Array};
    use arrow::datatypes::Int8Type;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(16 * 1024 * 1024));
    let values = Arc::new(StringArray::from(vec!["dictionary-value".repeat(4096)]));
    let dictionary = DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![0; 8192]), values)?;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "dictionary",
        dictionary.data_type().clone(),
        false,
    )]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(dictionary)])?;
    let manager = manager(Arc::clone(&pool), schema);
    let mut writing = manager.create_in_progress_file("dictionary retention control")?;
    writing.append_batch(&batch)?;
    let retained = batch.get_array_memory_size();
    drop(batch);
    assert!(
        pool.reserved() >= retained,
        "the IPC writer caches complete dictionary columns, including keys"
    );
    let file = writing.finish()?.unwrap();
    assert!(
        writing.finish().is_err(),
        "a finalized spill cannot finish twice"
    );
    let mut empty = manager.create_in_progress_file("empty finish control")?;
    assert!(empty.finish()?.is_none());
    drop(empty);
    assert_eq!(
        pool.reserved(),
        0,
        "finish must destroy the dictionary tracker and codec context"
    );
    let mut stream = manager.read_spill_as_stream_unbuffered(file, None)?;
    let output = stream.next().await.unwrap()?;
    assert_eq!(output.num_rows(), 8192);
    assert!(pool.reserved() >= physical_backing(&output));
    let alias = output.slice(8191, 1);
    drop(output);
    drop(stream);
    assert!(pool.reserved() >= physical_backing(&alias));
    drop(alias);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn spill_admission_preserves_nested_null_views_and_reused_dictionaries_with_each_codec()
-> Result<()> {
    use arrow::array::{Array, DictionaryArray, Int8Array, ListArray};
    use arrow::datatypes::{Int8Type, Int64Type};
    use datafusion::common::config::SpillCompression;
    for codec in [
        SpillCompression::Uncompressed,
        SpillCompression::Lz4Frame,
        SpillCompression::Zstd,
    ] {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64 * 1024 * 1024));
        let values = Arc::new(StringArray::from(vec![
            Some("shared-dictionary-value".repeat(1024)),
            None,
        ]));
        let dictionary = Arc::new(DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(0), Some(1), None]),
            values,
        )?);
        let list = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>([
            Some(vec![Some(9), None]),
            None,
            Some(vec![]),
        ]));
        let view = Arc::new(StringViewArray::from(vec![
            Some("view-payload".repeat(2048)),
            None,
            Some("inline".to_owned()),
        ]));
        let schema = Arc::new(Schema::new(vec![
            Field::new("dictionary", dictionary.data_type().clone(), true),
            Field::new("nested", list.data_type().clone(), true),
            Field::new("view", DataType::Utf8View, true),
        ]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![dictionary, list, view])?;
        let manager = manager(Arc::clone(&pool), schema).with_compression_type(codec);
        // The second batch reuses the dictionary: Arrow emits no replacement
        // message, so the reader must preserve its previous dictionary backing.
        let file = manager
            .spill_record_batch_and_finish(&[batch.clone(), batch.clone()], "nested codec control")?
            .unwrap();
        let mut stream = manager.read_spill_as_stream_unbuffered(file, None)?;
        let first = stream.next().await.unwrap().map_err(|error| {
            datafusion::common::DataFusionError::Context(
                format!("codec {codec:?}, first batch"),
                Box::new(error),
            )
        })?;
        let second = stream.next().await.unwrap().map_err(|error| {
            datafusion::common::DataFusionError::Context(
                format!("codec {codec:?}, reused dictionary"),
                Box::new(error),
            )
        })?;
        assert_eq!(first, batch, "codec {codec:?}");
        assert_eq!(second, batch, "codec {codec:?}");
        assert!(stream.next().await.is_none());
        let alias = first.slice(0, 1);
        drop(first);
        drop(second);
        drop(stream);
        drop(batch);
        assert!(
            pool.reserved() >= physical_backing(&alias),
            "codec {codec:?}"
        );
        drop(alias);
        assert_eq!(pool.reserved(), 0, "codec {codec:?}");
    }
    Ok(())
}

#[tokio::test]
async fn spill_admission_refuses_single_file_decode_and_cleans_up_before_output() -> Result<()> {
    const LIMIT: usize = 16 * 1024 * 1024;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(LIMIT));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8,
        false,
    )]));
    let text = "payload".repeat(4096);
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from_iter_values(
            std::iter::repeat_n(text.as_str(), 64),
        ))],
    )?;
    let manager = manager(Arc::clone(&pool), schema);
    let file = manager
        .spill_record_batch_and_finish(&[batch], "decode refusal control")?
        .unwrap();
    let pressure = MemoryConsumer::new("independent read pressure").register(&pool);
    pressure.try_grow(LIMIT - 1024)?;
    let mut stream = manager.read_spill_as_stream_unbuffered(file, None)?;
    let error = stream
        .next()
        .await
        .unwrap()
        .expect_err("single-file IPC allocation requires admission");
    assert!(
        matches!(
            error.find_root(),
            datafusion::common::DataFusionError::ResourcesExhausted(_)
        ),
        "{error}"
    );
    drop(stream);
    assert_eq!(pool.reserved(), LIMIT - 1024);
    drop(pressure);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

/// Blocks a real admitted decode on its worker thread. No product-only I/O hook
/// is needed: the test pool observes the real consumer's admission boundary.
#[derive(Debug)]
struct BlockingReadPool {
    inner: GreedyMemoryPool,
    block_prepaid: bool,
    blocked: std::sync::atomic::AtomicBool,
    release: (std::sync::Mutex<bool>, std::sync::Condvar),
    started: tokio::sync::Notify,
    changed: tokio::sync::Notify,
}
impl std::fmt::Display for BlockingReadPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "blocking read control")
    }
}
impl MemoryPool for BlockingReadPool {
    fn name(&self) -> &str {
        "blocking read control"
    }
    fn grow(
        &self,
        reservation: &datafusion::execution::memory_pool::MemoryReservation,
        bytes: usize,
    ) {
        self.inner.grow(reservation, bytes);
    }
    fn shrink(
        &self,
        reservation: &datafusion::execution::memory_pool::MemoryReservation,
        bytes: usize,
    ) {
        self.inner.shrink(reservation, bytes);
        self.changed.notify_one();
    }
    fn try_grow(
        &self,
        reservation: &datafusion::execution::memory_pool::MemoryReservation,
        bytes: usize,
    ) -> Result<()> {
        self.inner.try_grow(reservation, bytes)?;
        if (bytes > 0 || self.block_prepaid)
            && reservation.consumer().name() == "SpillReaderDecode"
            && !self.blocked.swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            self.started.notify_one();
            let (lock, wake) = &self.release;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = wake.wait(released).unwrap();
            }
        }
        Ok(())
    }
    fn reserved(&self) -> usize {
        self.inner.reserved()
    }
}

#[tokio::test]
async fn spill_admission_active_blocking_read_keeps_authority_after_stream_cancel() -> Result<()> {
    let observed = Arc::new(BlockingReadPool {
        block_prepaid: false,
        inner: GreedyMemoryPool::new(16 * 1024 * 1024),
        blocked: std::sync::atomic::AtomicBool::new(false),
        release: (std::sync::Mutex::new(false), std::sync::Condvar::new()),
        started: tokio::sync::Notify::new(),
        changed: tokio::sync::Notify::new(),
    });
    // Always release the worker if an assertion unwinds, so the runtime cannot
    // hang waiting for a deliberately blocked spawn_blocking task on teardown.
    struct Release(Arc<BlockingReadPool>);
    impl Drop for Release {
        fn drop(&mut self) {
            *self.0.release.0.lock().unwrap() = true;
            self.0.release.1.notify_all();
        }
    }
    let release = Release(Arc::clone(&observed));
    let pool: Arc<dyn MemoryPool> = observed.clone();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8,
        false,
    )]));
    let text = "worker".repeat(4096);
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(vec![text]))],
    )?;
    let manager = manager(Arc::clone(&pool), schema);
    let file = manager
        .spill_record_batch_and_finish(&[batch], "blocking cancellation control")?
        .unwrap();
    let mut stream = manager.read_spill_as_stream_unbuffered(file, None)?;
    {
        let read = stream.next();
        tokio::pin!(read);
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            tokio::select! {
                _ = observed.started.notified() => {},
                result = &mut read => panic!("read completed before the admitted worker barrier: {result:?}"),
            }
        }).await.expect("worker reaches its real admission boundary");
    }
    let active = pool.reserved();
    assert!(active > 0);
    drop(stream);
    assert_eq!(
        pool.reserved(),
        active,
        "aborting a handle cannot free the active blocking worker's allocation authority"
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
    .expect("cancelled worker exits and releases every reservation");
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn spill_admission_bufferless_null_and_zero_column_rows_still_require_read_workspace()
-> Result<()> {
    use arrow::array::{NullArray, RecordBatchOptions};
    const ROWS: usize = 1_000_000;
    const LIMIT: usize = 1024 * 1024;
    for null_column in [false, true] {
        let schema = Arc::new(if null_column {
            Schema::new(vec![Field::new("null", DataType::Null, true)])
        } else {
            Schema::empty()
        });
        let columns: Vec<arrow::array::ArrayRef> = if null_column {
            vec![Arc::new(NullArray::new(ROWS))]
        } else {
            vec![]
        };
        let batch = RecordBatch::try_new_with_options(
            Arc::clone(&schema),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(ROWS)),
        )?;
        assert!(
            batch
                .columns()
                .iter()
                .all(|column| column.get_buffer_memory_size() == 0)
        );
        if null_column {
            assert_eq!(
                batch.column(0).get_array_memory_size(),
                NullArray::new(1).get_array_memory_size(),
                "NullArray owns one constant header, independent of logical row count"
            );
        } else {
            assert_eq!(batch.get_array_memory_size(), 0);
        }
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(LIMIT));
        let manager = manager(Arc::clone(&pool), schema);
        let file = manager
            .spill_record_batch_and_finish(std::slice::from_ref(&batch), "bufferless control")?
            .unwrap();
        let mut stream = manager.read_spill_as_stream_unbuffered(file.clone(), None)?;
        let output = stream.next().await.unwrap()?;
        assert_eq!(output, batch);
        assert_eq!(output.num_rows(), ROWS);
        let slice = output.slice(ROWS - 1, 1);
        assert!(
            slice
                .columns()
                .iter()
                .all(|column| column.get_buffer_memory_size() == 0)
        );
        if null_column {
            assert!(slice.column(0).as_any().is::<NullArray>());
            assert_eq!(slice.column(0).logical_null_count(), 1);
        }
        assert!(stream.next().await.is_none());
        drop(stream);
        assert_eq!(
            pool.reserved(),
            0,
            "bufferless headers have no data-scaled backing or final-buffer claim token"
        );
        drop(output);
        drop(slice);
        let pressure = MemoryConsumer::new("bufferless decode pressure").register(&pool);
        pressure.try_grow(LIMIT - 1024)?;
        let mut refused = manager.read_spill_as_stream_unbuffered(file, None)?;
        let error =
            refused.next().await.unwrap().expect_err(
                "bufferless output still requires schema and decode workspace admission",
            );
        assert!(
            matches!(
                error.find_root(),
                datafusion::common::DataFusionError::ResourcesExhausted(_)
            ),
            "{error}"
        );
        drop(refused);
        assert_eq!(pool.reserved(), LIMIT - 1024);
        drop(pressure);
        assert_eq!(pool.reserved(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn spill_admission_accounts_for_synthesized_validity_of_zero_payload_struct_rows()
-> Result<()> {
    use arrow::array::StructArray;
    const LIMIT: usize = 1024 * 1024;
    let array = Arc::new(StructArray::new_empty_fields(1_000_000, None));
    assert_eq!(array.get_buffer_memory_size(), 0);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "empty_struct",
        array.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array])?;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(LIMIT));
    let manager = manager(Arc::clone(&pool), schema);
    let file = manager
        .spill_record_batch_and_finish(
            std::slice::from_ref(&batch),
            "synthetic validity roomy control",
        )?
        .unwrap();
    let mut stream = manager.read_spill_as_stream_unbuffered(file, None)?;
    assert_eq!(stream.next().await.unwrap()?, batch);
    assert!(stream.next().await.is_none());
    drop(stream);
    assert_eq!(pool.reserved(), 0);
    let pressure = MemoryConsumer::new("synthetic validity independent pressure").register(&pool);
    pressure.try_grow(LIMIT - 32 * 1024)?;
    let mut writer = manager.create_in_progress_file("synthetic validity refusal")?;
    let error = writer.append_batch(&batch).expect_err(
        "IPC must admit its synthesized 125000-byte validity bitmap before constructing it",
    );
    assert!(
        matches!(
            error.find_root(),
            datafusion::common::DataFusionError::ResourcesExhausted(_)
        ),
        "{error}"
    );
    drop(writer);
    assert_eq!(pool.reserved(), LIMIT - 32 * 1024);
    drop(pressure);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn spill_admission_counts_distinct_decoded_owners_and_complete_sliced_backing() -> Result<()>
{
    use datafusion::common::config::SpillCompression;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(16 * 1024 * 1024));
    let schema = Arc::new(Schema::new(vec![
        Field::new("left", DataType::Utf8, false),
        Field::new("right", DataType::Utf8, false),
    ]));
    let left = "left-value".repeat(4096);
    let right = "right-value".repeat(4096);
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                left.as_str(),
                16,
            ))),
            Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                right.as_str(),
                16,
            ))),
        ],
    )?;
    let manager = manager(Arc::clone(&pool), schema).with_compression_type(SpillCompression::Zstd);
    let file = manager
        .spill_record_batch_and_finish(std::slice::from_ref(&batch), "distinct owner control")?
        .unwrap();
    let mut stream = manager.read_spill_as_stream_unbuffered(file, None)?;
    let output = stream.next().await.unwrap()?;
    assert_eq!(output, batch);
    let alias = output.slice(15, 1);
    let complete_values = {
        let left = alias.column(0).to_data();
        let right = alias.column(1).to_data();
        let left = &left.buffers()[1];
        let right = &right.buffers()[1];
        assert_ne!(
            left.data_ptr(),
            right.data_ptr(),
            "independent compressed value buffers decode to distinct owners"
        );
        assert!(left.capacity() >= 16 * 4096 * "left-value".len());
        assert!(right.capacity() >= 16 * 4096 * "right-value".len());
        left.capacity() + right.capacity()
    };
    assert!(
        physical_backing(&alias) >= complete_values,
        "distinct physical owners must both count"
    );
    assert!(pool.reserved() >= physical_backing(&alias));
    drop(output);
    drop(stream);
    assert!(
        pool.reserved() >= physical_backing(&alias),
        "one-row slices retain complete backing after stream drop"
    );
    drop(alias);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

/// Observes real merge admission and pauses the first reader before it can
/// produce a batch. The pool never supplies free credit or changes a bound.
#[derive(Debug)]
struct CopyPhasePool {
    inner: GreedyMemoryPool,
    reading: std::sync::atomic::AtomicBool,
    restored: std::sync::atomic::AtomicBool,
    release: (std::sync::Mutex<bool>, std::sync::Condvar),
    started: tokio::sync::Notify,
    changed: tokio::sync::Notify,
}
impl std::fmt::Display for CopyPhasePool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "copy phase observation")
    }
}
impl MemoryPool for CopyPhasePool {
    fn name(&self) -> &str {
        "copy phase observation"
    }
    fn grow(&self, r: &datafusion::execution::memory_pool::MemoryReservation, bytes: usize) {
        self.inner.grow(r, bytes);
    }
    fn shrink(&self, r: &datafusion::execution::memory_pool::MemoryReservation, bytes: usize) {
        self.inner.shrink(r, bytes);
        self.changed.notify_one();
    }
    fn try_grow(
        &self,
        r: &datafusion::execution::memory_pool::MemoryReservation,
        bytes: usize,
    ) -> Result<()> {
        self.inner.try_grow(r, bytes)?;
        if r.consumer().name() == "copy phase control"
            && bytes > 0
            && self.reading.load(std::sync::atomic::Ordering::Acquire)
        {
            self.restored
                .store(true, std::sync::atomic::Ordering::Release);
        }
        if r.consumer().name() == "SpillReaderMetadata"
            && !self.reading.swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            self.started.notify_one();
            let mut released = self.release.0.lock().unwrap();
            while !*released {
                released = self.release.1.wait(released).unwrap();
            }
        }
        Ok(())
    }
    fn reserved(&self) -> usize {
        self.inner.reserved()
    }
}

async fn copy_phase_merge(refuse_next_copy: bool) -> Result<()> {
    use arrow::array::FixedSizeBinaryArray;
    use datafusion::physical_expr::{PhysicalSortExpr, expressions::Column};
    use datafusion::physical_plan::metrics::BaselineMetrics;
    use datafusion::physical_plan::sorts::streaming_merge::{
        SortedSpillFile, StreamingMergeBuilder,
    };
    const LIMIT: usize = 8 * 1024 * 1024;
    const ROWS: usize = 256;
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
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::FixedSizeBinary(16), false),
        Field::new("left", DataType::Utf8, false),
        Field::new("right", DataType::Utf8, false),
    ]));
    let manager = manager(Arc::clone(&pool), Arc::clone(&schema));
    let mut files = Vec::new();
    let mut maximum = 0;
    for parity in 0..2 {
        let keys = FixedSizeBinaryArray::try_from_iter((0..ROWS).map(|i| {
            let mut key = [0; 16];
            key[8..].copy_from_slice(&((2 * i + parity) as u64).to_be_bytes());
            key
        }))?;
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(keys),
                Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                    "l".repeat(1024),
                    ROWS,
                ))),
                Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                    "r".repeat(1024),
                    ROWS,
                ))),
            ],
        )?;
        maximum = physical_backing(&batch);
        let file = manager
            .spill_record_batch_and_finish(&[batch], "copy phase merge")?
            .unwrap();
        files.push(SortedSpillFile {
            file,
            max_record_batch_memory: maximum,
        });
    }
    assert_eq!(pool.reserved(), 0);
    // Existing multi-run admission: two input files, two buffered batches per
    // file, and the original raw-plus-copy factor of two.
    let prepaid = 2 * 2 * 2 * maximum;
    let reservation = MemoryConsumer::new("copy phase control").register(&pool);
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
        .with_batch_size(ROWS)
        .with_reservation(reservation)
        .build()?;
    {
        let first = stream.next();
        tokio::pin!(first);
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            tokio::select! {
                _ = observed.started.notified() => {},
                result = &mut first => panic!("merge completed before the reader barrier: {result:?}"),
            }
        }).await.expect("real IPC reader reaches admission");
    }
    assert!(
        pool.reserved() < prepaid,
        "idle copy scratch must be released while the admitted reader is paused"
    );
    drop(release);
    let first = stream.next().await.unwrap()?;
    assert_eq!(first.num_rows(), ROWS);
    assert!(
        observed.restored.load(std::sync::atomic::Ordering::Acquire),
        "a real copy phase must fallibly reacquire its admitted workspace"
    );
    let alias = first.column(1).slice(0, 1);
    let alias_capacity = alias.to_data().buffers()[1].capacity();
    let mut expected = 0;
    let check = |batch: &RecordBatch, expected: &mut usize| {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let left = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let right = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert_eq!(&keys.value(row)[8..], &(*expected as u64).to_be_bytes());
            assert_eq!(left.value(row), "l".repeat(1024));
            assert_eq!(right.value(row), "r".repeat(1024));
            *expected += 1;
        }
    };
    check(&first, &mut expected);
    drop(first);
    if refuse_next_copy {
        let pressure = MemoryConsumer::new("independent copy pressure").register(&pool);
        pressure.try_grow(LIMIT - pool.reserved())?;
        let error = stream
            .next()
            .await
            .unwrap()
            .expect_err("unavailable copy workspace must refuse before interleave");
        assert!(
            matches!(
                error.find_root(),
                datafusion::common::DataFusionError::ResourcesExhausted(_)
            ),
            "{error}"
        );
        drop(stream);
        drop(pressure);
    } else {
        while let Some(batch) = stream.next().await {
            check(&batch?, &mut expected);
        }
        assert_eq!(expected, 2 * ROWS);
        drop(stream);
    }
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let changed = observed.changed.notified();
            if pool.reserved() < 2 * alias_capacity {
                break;
            }
            changed.await;
        }
    })
    .await
    .expect("workers and unrelated copied sibling backing release");
    assert!(
        pool.reserved() >= alias_capacity,
        "fresh output slice retains its complete copied owner after merge drop"
    );
    assert!(
        pool.reserved() < 2 * alias_capacity,
        "one copied leaf cannot pin its unrelated copied sibling"
    );
    drop(alias);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn spill_admission_merge_releases_idle_copy_scratch_and_preserves_fresh_output_aliases()
-> Result<()> {
    copy_phase_merge(false).await
}

#[tokio::test]
async fn spill_admission_merge_copy_reacquisition_refuses_and_cleans_up_with_alias_alive()
-> Result<()> {
    copy_phase_merge(true).await
}

#[path = "spill_copy_tests.rs"]
mod copy_tests;

#[path = "spill_source_tests.rs"]
mod source_tests;

#[path = "spill_credit_tests.rs"]
mod credit_tests;
