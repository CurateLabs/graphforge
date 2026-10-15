//! Physical IPC ownership, dictionary provenance and producer retirement.
use super::*;

#[tokio::test]
async fn spill_admission_projected_compressed_leaf_releases_distinct_sibling_owner() -> Result<()> {
    use datafusion::common::config::SpillCompression;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(16 * 1024 * 1024));
    let batch = RecordBatch::try_from_iter([
        (
            "small",
            Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                "s".repeat(256),
                64,
            ))) as arrow::array::ArrayRef,
        ),
        (
            "large",
            Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                "l".repeat(4096),
                64,
            ))) as arrow::array::ArrayRef,
        ),
    ])?;
    let manager =
        manager(Arc::clone(&pool), batch.schema()).with_compression_type(SpillCompression::Zstd);
    let file = manager
        .spill_record_batch_and_finish(std::slice::from_ref(&batch), "projected owner control")?
        .unwrap();
    let mut stream = manager.read_spill_as_stream_unbuffered(file, None)?;
    let output = stream.next().await.unwrap()?;
    assert_eq!(output, batch);
    let small = output.column(0).slice(63, 1);
    let large = output.column(1).slice(63, 1);
    let large_capacity = large.to_data().buffers()[1].capacity();
    assert_ne!(
        small.to_data().buffers()[1].data_ptr(),
        large.to_data().buffers()[1].data_ptr()
    );
    drop(output);
    assert!(stream.next().await.is_none());
    drop(stream);
    let both = pool.reserved();
    drop(large);
    assert!(
        both - pool.reserved() >= large_capacity,
        "the small leaf cannot pin a distinct sibling payload: both={both} small={} large_capacity={large_capacity}",
        pool.reserved()
    );
    assert!(pool.reserved() >= small.to_data().buffers()[1].capacity());
    drop(small);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn spill_admission_reused_dictionary_keeps_values_without_pinning_keys_or_sibling_payload()
-> Result<()> {
    use arrow::array::{ArrayRef, DictionaryArray, Int32Array};
    use arrow::datatypes::Int32Type;
    use datafusion::common::config::SpillCompression;
    for codec in [
        SpillCompression::Uncompressed,
        SpillCompression::Zstd,
        SpillCompression::Lz4Frame,
    ] {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(32 * 1024 * 1024));
        let values: ArrayRef = Arc::new(StringArray::from(vec![
            "dictionary".repeat(4096),
            "value".repeat(4096),
        ]));
        let batches = [0, 1].map(|parity| {
            RecordBatch::try_from_iter([
                (
                    "dict",
                    Arc::new(
                        DictionaryArray::<Int32Type>::try_new(
                            Int32Array::from(
                                (0..64).map(|i| Some((i + parity) % 2)).collect::<Vec<_>>(),
                            ),
                            Arc::clone(&values),
                        )
                        .unwrap(),
                    ) as ArrayRef,
                ),
                (
                    "sibling",
                    Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                        "s".repeat(4096),
                        64,
                    ))) as ArrayRef,
                ),
            ])
            .unwrap()
        });
        let expected = batches.clone();
        let manager = manager(Arc::clone(&pool), batches[0].schema()).with_compression_type(codec);
        let mut writer = manager.create_in_progress_file("dictionary owner reuse control")?;
        for batch in &batches {
            writer.append_batch(batch)?;
        }
        let file = writer.finish()?.unwrap();
        drop(writer);
        drop(batches);
        drop(values);
        // Expected arrays held writer-source tokens; free them after values are
        // checked, before examining decoded ownership independently below.
        let mut stream = manager.read_spill_as_stream_unbuffered(file, None)?;
        let first = stream.next().await.unwrap()?;
        assert_eq!(first, expected[0]);
        let first_values = Arc::clone(
            first
                .column(0)
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .unwrap()
                .values(),
        );
        let second = stream.next().await.unwrap()?;
        assert_eq!(second, expected[1]);
        let second_values = Arc::clone(
            second
                .column(0)
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .unwrap()
                .values(),
        );
        assert_eq!(
            first_values.to_data().buffers()[1].data_ptr(),
            second_values.to_data().buffers()[1].data_ptr(),
            "the IPC reader really reuses its dictionary values ({codec:?})"
        );
        drop(expected);
        drop(first);
        drop(second);
        assert!(stream.next().await.is_none());
        drop(stream);
        let retained = pool.reserved();
        drop(first_values);
        assert_eq!(
            pool.reserved(),
            retained,
            "one remaining value alias keeps exactly its existing owner"
        );
        assert!(
            retained
                >= physical_backing(&RecordBatch::try_from_iter([(
                    "values",
                    Arc::clone(&second_values)
                )])?)
        );
        // With compression, sibling values have their own physical allocation;
        // without it, the whole dictionary message body still belongs to values.
        assert!(
            retained < 256 * 1024,
            "dictionary values cannot retain unrelated keys/sibling payload: {retained}, {codec:?}"
        );
        drop(second_values);
        assert_eq!(pool.reserved(), 0, "{codec:?}");
    }
    Ok(())
}
