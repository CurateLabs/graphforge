use super::*;
use arrow::array::{NullArray, UInt64Array};
use arrow::datatypes::{Field, Schema};
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};
use std::sync::Arc;

#[test]
fn prepaid_batch_transfers_backing_without_readmitting_and_last_slice_releases_it() {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(256 * 1024));
    let grant = MemoryConsumer::new("prepaid read control").register(&pool);
    grant.try_grow(128 * 1024).unwrap();
    let values: ArrayRef = Arc::new(UInt64Array::from_iter_values(0..8192));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)])),
        vec![values],
    )
    .unwrap();
    let backing = batch.column(0).get_buffer_memory_size();
    let source = PrepaidBatch::seal(batch, ReadGrant::from_reserved(grant)).unwrap();
    let slice = source.batch().column(0).slice(9, 1);
    assert!(
        source.funds_buffer(
            slice
                .as_primitive::<arrow::datatypes::UInt64Type>()
                .values()
                .inner()
        )
    );
    assert!(pool.reserved() >= backing + token_bytes());
    drop(source);
    assert_eq!(pool.reserved(), backing + token_bytes());
    assert_eq!(
        slice
            .as_primitive::<arrow::datatypes::UInt64Type>()
            .value(0),
        9
    );
    drop(slice);
    assert_eq!(pool.reserved(), 0);
}

#[test]
fn prepaid_batch_bufferless_and_empty_buffers_have_only_an_internal_owner() {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64 * 1024));
    for rows in [0, 7] {
        let credit = MemoryConsumer::new("prepaid empty control").register(&pool);
        credit.try_grow(16 * 1024).unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("empty", DataType::Null, true),
                Field::new("number", DataType::UInt64, true),
            ])),
            vec![
                Arc::new(NullArray::new(rows)),
                Arc::new(UInt64Array::new_null(rows)),
            ],
        )
        .unwrap();
        let sealed = PrepaidBatch::seal(batch, ReadGrant::from_reserved(credit)).unwrap();
        assert_eq!(sealed.batch().num_rows(), rows);
        assert!(pool.reserved() > 0);
        drop(sealed);
        assert_eq!(pool.reserved(), 0);
    }
    let credit = MemoryConsumer::new("prepaid zero column control").register(&pool);
    credit.try_grow(4096).unwrap();
    let sealed = PrepaidBatch::seal(
        RecordBatch::new_empty(Arc::new(Schema::empty())),
        ReadGrant::from_reserved(credit),
    )
    .unwrap();
    assert_eq!(sealed.batch().num_columns(), 0);
    drop(sealed);
    assert_eq!(pool.reserved(), 0);
}

#[test]
fn prepaid_batch_static_bytes_keep_the_reported_backing_through_the_last_slice() {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64 * 1024));
    let credit = MemoryConsumer::new("static backing control").register(&pool);
    credit.try_grow(4096).unwrap();
    // Pinned Bytes::capacity reports the full supplied24-byte static region;
    // the Buffer doc comment suggesting Custom capacity0 is stale. This
    // fixture has a known producer, not an arbitrary external-owner proof.
    let buffer = Buffer::from(bytes::Bytes::from_static(b"unfunded external source"));
    assert_eq!(buffer.capacity(), 24);
    let values = arrow::buffer::ScalarBuffer::new(buffer, 0, 8);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "static",
            DataType::UInt8,
            false,
        )])),
        vec![Arc::new(arrow::array::UInt8Array::new(values, None))],
    )
    .unwrap();
    let source = PrepaidBatch::seal(batch, ReadGrant::from_reserved(credit)).unwrap();
    let escaped = source.batch().column(0).slice(3, 1);
    drop(source);
    assert_eq!(pool.reserved(), 24 + token_bytes());
    assert_eq!(
        escaped
            .as_primitive::<arrow::datatypes::UInt8Type>()
            .value(0),
        b'u'
    );
    drop(escaped);
    assert_eq!(pool.reserved(), 0);
}
