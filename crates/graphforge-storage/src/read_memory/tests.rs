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

#[test]
fn prepaid_nested_view_dictionary_aliases_keep_the_full_backing_and_release_last() {
    use arrow::array::{DictionaryArray, Int8Array, ListArray, StringViewArray, StructArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::Int8Type;

    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(16 * 1024 * 1024));
    let credit = MemoryConsumer::new("nested property source").register(&pool);
    credit.try_grow(8 * 1024 * 1024).unwrap();
    let text = "a view must pin its complete authenticated source ".repeat(8192);
    let view: ArrayRef = Arc::new(StringViewArray::from(vec![
        text.as_str(),
        "inline",
        text.as_str(),
    ]));
    let values = view.as_string_view();
    assert!(!values.data_buffers().is_empty());
    let payload = values
        .data_buffers()
        .iter()
        .map(Buffer::capacity)
        .sum::<usize>();
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(2), None, Some(0)]),
            view.clone(),
        )
        .unwrap(),
    );
    let item = Arc::new(Field::new("item", dictionary.data_type().clone(), true));
    let list: ArrayRef = Arc::new(ListArray::new(
        item,
        OffsetBuffer::from_lengths([2, 1]),
        dictionary,
        None,
    ));
    let nested = StructArray::from(vec![(
        Arc::new(Field::new("nested", list.data_type().clone(), true)),
        list,
    )]);
    let nested: ArrayRef = Arc::new(nested);
    // A second column aliases the same view backing. Replacing its claim must
    // retain the actual credit, never release it with the first array wrapper.
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("record", nested.data_type().clone(), true),
            Field::new("view", view.data_type().clone(), true),
        ])),
        vec![nested, view.slice(0, 2)],
    )
    .unwrap();
    drop(view);
    let sealed = PrepaidBatch::seal(batch, ReadGrant::from_reserved(credit)).unwrap();
    let dictionary = sealed
        .batch()
        .column(0)
        .as_struct()
        .column(0)
        .as_list::<i32>()
        .values()
        .slice(2, 1);
    let alias = sealed.batch().column(1).slice(0, 1);
    let expected = dictionary.to_data();
    drop(sealed);
    assert!(pool.reserved() >= payload);
    assert_eq!(alias.as_string_view().value(0), text);
    drop(alias);
    assert!(
        pool.reserved() >= payload,
        "dictionary values still own complete view backing"
    );
    assert_eq!(dictionary.to_data(), expected);
    drop(expected);
    drop(dictionary);
    assert_eq!(pool.reserved(), 0);
}

#[test]
fn read_grant_refusal_and_transfer_keep_the_original_consumer_and_owners() {
    use std::sync::Mutex;
    struct Admission(Mutex<datafusion::execution::memory_pool::MemoryReservation>);
    impl ReadAdmission for Admission {
        fn reserve(&self, bytes: usize) -> Result<ReadGrant> {
            let reservation = self.0.lock().unwrap();
            reservation.try_grow(bytes)?;
            Ok(ReadGrant::from_reserved(reservation.split(bytes)))
        }
        fn check_cancelled(&self) -> Result<()> {
            Ok(())
        }
    }
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(4096));
    let admission = Admission(Mutex::new(
        MemoryConsumer::new("authenticated property source").register(&pool),
    ));
    let mut grant = admission.reserve(1024).unwrap();
    let sibling = grant.split(512).unwrap();
    let error = grant.grow(&admission, 8192).unwrap_err();
    assert!(
        error.to_string().contains("authenticated property source"),
        "{error}"
    );
    assert_eq!(grant.size(), 512);
    assert_eq!(pool.reserved(), 1024);
    drop(sibling);
    assert_eq!(pool.reserved(), 512);
    drop(grant);
    drop(admission);
    assert_eq!(pool.reserved(), 0);
}

#[test]
fn empty_nested_view_keeps_its_container_authority_until_the_internal_owner_drops() {
    use arrow::array::{StringViewArray, StructArray};
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64 * 1024));
    let credit = MemoryConsumer::new("empty nested source").register(&pool);
    credit.try_grow(56 * 1024).unwrap();
    // The view has no logical values or payload bytes, but its descriptor
    // list is a real heap allocation larger than the scalar/header owners.
    let view: ArrayRef = Arc::new(StringViewArray::new(
        arrow::buffer::ScalarBuffer::from(Vec::<u128>::new()),
        vec![Buffer::from_vec(Vec::<u8>::new()); 1024],
        None,
    ));
    let array: ArrayRef = Arc::new(StructArray::from(vec![(
        Arc::new(Field::new("view", DataType::Utf8View, true)),
        view,
    )]));
    assert_eq!(array.get_buffer_memory_size(), 0);
    let containers = container_bytes(array.as_ref()).unwrap();
    assert!(containers > 0);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "nested",
            array.data_type().clone(),
            true,
        )])),
        vec![array],
    )
    .unwrap();
    let source = PrepaidBatch::seal(batch, ReadGrant::from_reserved(credit)).unwrap();
    assert!(
        pool.reserved()
            >= containers
                + std::mem::size_of::<MemoryReservation>()
                + 2 * std::mem::size_of::<usize>()
    );
    drop(source);
    assert_eq!(pool.reserved(), 0);
}
