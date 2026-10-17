//! Real IPC preparation and resident schema ownership, observed through the query pool.
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Int64Array, ListArray, RecordBatch, StringArray, StructArray,
    TimestampMillisecondArray,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, SpillMetrics};
use datafusion::physical_plan::spill::SpillManager;
use futures::StreamExt;

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

fn schema_batch() -> Result<RecordBatch> {
    let text: ArrayRef = Arc::new(StringArray::from_iter_values([
        "first".repeat(256),
        "second".repeat(256),
        "third".repeat(256),
        "last".repeat(256),
    ]));
    let times: ArrayRef = Arc::new(
        TimestampMillisecondArray::from(vec![Some(1), None, Some(3), Some(4)])
            .with_timezone("Europe/London"),
    );
    let lists: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([
        Some(vec![Some(11), None]),
        None,
        Some(vec![Some(31)]),
        Some(vec![Some(41), Some(42)]),
    ]));
    let nested: ArrayRef = Arc::new(StructArray::from(vec![(
        Arc::new(Field::new("entries", lists.data_type().clone(), true)),
        lists,
    )]));
    let field_metadata = (0..8)
        .map(|i| (format!("field-key-{i}"), format!("field-value-{i}")))
        .collect::<HashMap<_, _>>();
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("value".repeat(3277), DataType::Utf8, false).with_metadata(field_metadata),
            Field::new("when", times.data_type().clone(), true),
            Field::new("nested", nested.data_type().clone(), false),
        ],
        HashMap::from([("source".to_owned(), "real IPC resident schema".to_owned())]),
    ));
    Ok(RecordBatch::try_new(schema, vec![text, times, nested])?)
}

#[tokio::test]
async fn spill_schema_preparation_releases_workspace_with_last_buffer_and_schema_alive()
-> Result<()> {
    let expected = schema_batch()?;
    let writer_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(8 * 1024 * 1024));
    let writer = manager(Arc::clone(&writer_pool), expected.schema());
    let file = writer
        .spill_record_batch_and_finish(std::slice::from_ref(&expected), "resident schema control")?
        .unwrap();
    assert_eq!(writer_pool.reserved(), 0);

    // The roomy control runs first. The fixed small pool is sufficient for the
    // same real preparation/decode, then permits unrelated work once parsing ends.
    for limit in [8 * 1024 * 1024, 512 * 1024] {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(limit));
        let reader = manager(Arc::clone(&pool), expected.schema());
        let mut stream = reader.read_spill_as_stream_unbuffered(file.clone(), None)?;
        let output = stream.next().await.unwrap()?;
        assert_eq!(
            output, expected,
            "all cells and complete nested metadata round-trip"
        );
        let schema_alias = output.schema();
        let leaf = output.column(0).slice(3, 1);
        let full_values = leaf
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .values()
            .capacity();
        assert!(
            full_values > 4 * 1024,
            "one row aliases the complete decoded body"
        );
        drop(output);
        drop(stream);
        let held = pool.reserved();
        assert!(
            held >= full_values + schema_alias.field(0).name().capacity(),
            "last value buffer still funds its full backing and parsed schema: {held}"
        );
        let pressure =
            MemoryConsumer::new("independent work after schema preparation").register(&pool);
        pressure.try_grow(384 * 1024).expect(
            "completed IPC schema preparation must not occupy the resident schema's lifetime",
        );
        assert_eq!(
            leaf.as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "last".repeat(256)
        );
        assert_eq!(schema_alias, expected.schema());
        drop(pressure);
        assert_eq!(
            pool.reserved(),
            held,
            "pressure does not borrow the escaped owner's grant"
        );
        drop(schema_alias);
        drop(leaf);
        assert_eq!(
            pool.reserved(),
            0,
            "the final buffer releases resident schema ownership"
        );
    }
    Ok(())
}

#[tokio::test]
async fn spill_schema_preparation_still_refuses_before_parsing_when_construction_cannot_fit()
-> Result<()> {
    let expected = schema_batch()?;
    let writer_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(8 * 1024 * 1024));
    let writer = manager(writer_pool, expected.schema());
    let file = writer
        .spill_record_batch_and_finish(
            std::slice::from_ref(&expected),
            "schema preparation refusal",
        )?
        .unwrap();
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64 * 1024));
    let reader = manager(Arc::clone(&pool), expected.schema());
    let mut stream = reader.read_spill_as_stream_unbuffered(file, None)?;
    let error = stream
        .next()
        .await
        .unwrap()
        .expect_err("preparation still needs its complete grant");
    assert!(
        matches!(error.find_root(), DataFusionError::ResourcesExhausted(_)),
        "{error}"
    );
    assert!(error.to_string().contains("SpillReaderMetadata"), "{error}");
    drop(stream);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

fn wide_struct_batch() -> Result<RecordBatch> {
    let mut columns = Vec::<ArrayRef>::new();
    let mut fields = Vec::new();
    for group in 0..12 {
        for width in [5, 9] {
            let mut children = Vec::new();
            for child in 0..width {
                let values = (0..8)
                    .map(|row| i64::from(group * 10_000 + width * 100 + child * 10 + row))
                    .collect::<Vec<_>>();
                let array: ArrayRef = if child == width - 1 {
                    Arc::new(TimestampMillisecondArray::from(values).with_timezone("Europe/London"))
                } else {
                    Arc::new(Int64Array::from(values))
                };
                children.push((
                    Arc::new(Field::new(
                        format!("child-{child}"),
                        array.data_type().clone(),
                        false,
                    )),
                    array,
                ));
            }
            let array: ArrayRef = Arc::new(StructArray::from(children));
            fields.push(Field::new(
                format!("group-{group}-width-{width}"),
                array.data_type().clone(),
                false,
            ));
            columns.push(array);
        }
    }
    let lists: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([
        Some(vec![Some(11), None]),
        None,
        Some(vec![Some(31)]),
        Some(vec![Some(41), Some(42)]),
        Some(vec![]),
        Some(vec![Some(61)]),
        None,
        Some(vec![Some(81), Some(82)]),
    ]));
    fields.push(Field::new("list", lists.data_type().clone(), true));
    columns.push(lists);
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

#[tokio::test]
async fn spill_array_preparation_releases_temporary_headers_while_decoded_containers_stay_live()
-> Result<()> {
    let expected = wide_struct_batch()?;
    let writer_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(8 * 1024 * 1024));
    let writer = manager(Arc::clone(&writer_pool), expected.schema());
    let file = writer
        .spill_record_batch_and_finish(
            std::slice::from_ref(&expected),
            "resident array metadata control",
        )?
        .unwrap();
    assert_eq!(writer_pool.reserved(), 0);

    // Struct widths 5/9 exercise spare child-Vec capacity from the IPC push
    // constructor. There are 168 primitive children: their former preparation
    // headers alone exceed the fixed 128 KiB left by independent pressure.
    for limit in [8 * 1024 * 1024, 1024 * 1024] {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(limit));
        let reader = manager(Arc::clone(&pool), expected.schema());
        let mut stream = reader.read_spill_as_stream_unbuffered(file.clone(), None)?;
        let output = stream.next().await.unwrap()?;
        assert_eq!(
            output, expected,
            "all nested cells, fields, nulls and timezone survive real IPC"
        );
        let schema_alias = output.schema();
        let first = output
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(first.num_columns(), 5);
        assert_eq!(
            output
                .column(1)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
                .num_columns(),
            9
        );
        let leaf = first.column(0).slice(7, 1);
        let backing = leaf
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .inner()
            .capacity();
        assert!(backing > 8, "one value retains the whole IPC body backing");
        drop(stream);
        let pressure =
            MemoryConsumer::new("independent work after array preparation").register(&pool);
        let pressure_result = pressure.try_grow(896 * 1024);
        let inline_mutex = cfg!(any(
            target_os = "linux",
            all(target_os = "windows", not(target_vendor = "win7")),
        )) && matches!(usize::BITS, 32 | 64);
        if limit == 1024 * 1024 && !inline_mutex {
            let error = pressure_result.expect_err(
                "unqualified mutex representations retain the previous complete metadata fee",
            );
            assert!(
                matches!(error.find_root(), DataFusionError::ResourcesExhausted(_)),
                "{error}"
            );
        } else {
            pressure_result.expect(
                "ended ArrayData preparation must not occupy the decoded containers' resident lifetime",
            );
        }
        assert_eq!(
            output, expected,
            "pressure cannot retire still-live decoded arrays or child vectors"
        );
        drop(output);
        assert!(pool.reserved() - pressure.size() >= backing);
        assert_eq!(
            leaf.as_any().downcast_ref::<Int64Array>().unwrap().value(0),
            507
        );
        assert_eq!(schema_alias, expected.schema());
        drop(pressure);
        assert!(pool.reserved() >= backing);
        drop(schema_alias);
        drop(leaf);
        assert_eq!(
            pool.reserved(),
            0,
            "last physical buffer releases resident metadata"
        );
    }
    Ok(())
}

const NESTED_FRAME_ROWS: usize = 2048;
const NESTED_FRAME_LIMIT: usize = 512 * 1024;

fn nested_frame_batch() -> Result<RecordBatch> {
    use arrow::array::{FixedSizeBinaryArray, TimestampMicrosecondArray, UInt32Array, UInt64Array};
    use arrow::buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};

    let rows = NESTED_FRAME_ROWS;
    let uuid: ArrayRef = Arc::new(FixedSizeBinaryArray::try_from_iter((0..rows).map(|row| {
        let mut key = [0_u8; 16];
        key[8..].copy_from_slice(&(row as u64).to_be_bytes());
        key
    }))?);
    let ids: ArrayRef = Arc::new(UInt64Array::from_iter_values(
        (0..rows).map(|row| row as u64),
    ));
    let types: ArrayRef = Arc::new(UInt32Array::from_iter_values(
        (0..rows).map(|row| (row % 7) as u32),
    ));
    let list_values: ArrayRef = Arc::new(UInt32Array::new(
        ScalarBuffer::from((0..2 * rows).map(|value| value as u32).collect::<Vec<_>>()),
        Some(NullBuffer::from(
            (0..2 * rows)
                .map(|value| value % 5 != 0)
                .collect::<Vec<_>>(),
        )),
    ));
    let lists: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(DataType::UInt32, true)),
        OffsetBuffer::new(ScalarBuffer::from(
            (0..=rows).map(|row| (2 * row) as i32).collect::<Vec<_>>(),
        )),
        list_values,
        Some(NullBuffer::from(
            (0..rows).map(|row| row % 3 != 0).collect::<Vec<_>>(),
        )),
    ));
    let numbers: ArrayRef = Arc::new(Int64Array::new(
        ScalarBuffer::from((0..rows).map(|row| row as i64 - 100).collect::<Vec<_>>()),
        Some(NullBuffer::from(
            (0..rows).map(|row| row % 11 != 0).collect::<Vec<_>>(),
        )),
    ));
    let times: ArrayRef = Arc::new(
        TimestampMicrosecondArray::new(
            ScalarBuffer::from(
                (0..rows)
                    .map(|row| 1_700_000_000_000_000_i64 + row as i64)
                    .collect::<Vec<_>>(),
            ),
            Some(NullBuffer::from(
                (0..rows).map(|row| row % 13 != 0).collect::<Vec<_>>(),
            )),
        )
        .with_timezone("UTC"),
    );
    let strings: ArrayRef = Arc::new(StringArray::new(
        OffsetBuffer::new(ScalarBuffer::from(
            (0..=rows).map(|row| (128 * row) as i32).collect::<Vec<_>>(),
        )),
        Buffer::from_vec(
            (0..rows)
                .flat_map(|row| format!("{row:0128}").into_bytes())
                .collect(),
        ),
        Some(NullBuffer::from(
            (0..rows).map(|row| row % 5 != 0).collect::<Vec<_>>(),
        )),
    ));
    let struct_fields = vec![
        Arc::new(Field::new("number", numbers.data_type().clone(), true)),
        Arc::new(Field::new("when", times.data_type().clone(), true)),
        Arc::new(Field::new("text", strings.data_type().clone(), true)),
    ]
    .into();
    let nested: ArrayRef = Arc::new(StructArray::new(
        struct_fields,
        vec![numbers, times, strings],
        Some(NullBuffer::from(
            (0..rows).map(|row| row % 7 != 0).collect::<Vec<_>>(),
        )),
    ));
    let columns = vec![uuid, ids, types, lists, nested];
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                Field::new(format!("field-{index}"), column.data_type().clone(), true)
            })
            .collect::<Vec<_>>(),
    ));
    Ok(RecordBatch::try_new(schema, columns)?)
}

struct NestedFrameFile {
    file: datafusion::execution::disk_manager::RefCountedTempFile,
    bytes: Vec<u8>,
    body: usize,
    buffers: usize,
    nodes: usize,
}

fn write_nested_frame(batch: &RecordBatch) -> Result<NestedFrameFile> {
    use arrow::ipc::{MessageHeader, MetadataVersion};
    use datafusion::common::config::SpillCompression;

    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(8 * 1024 * 1024));
    let writer = manager(Arc::clone(&pool), batch.schema())
        .with_compression_type(SpillCompression::Uncompressed);
    let file = writer
        .spill_record_batch_and_finish(
            std::slice::from_ref(batch),
            "aligned nested IPC reader proof",
        )?
        .unwrap();
    assert_eq!(pool.reserved(), 0);
    let bytes = std::fs::read(file.path())?;
    let mut position = 0;
    loop {
        let mut len = i32::from_le_bytes(bytes[position..position + 4].try_into().unwrap());
        position += 4;
        if len == -1 {
            len = i32::from_le_bytes(bytes[position..position + 4].try_into().unwrap());
            position += 4;
        }
        let metadata = &bytes[position..position + len as usize];
        let message = arrow::ipc::root_as_message(metadata).unwrap();
        let body = usize::try_from(message.bodyLength()).unwrap();
        if message.header_type() == MessageHeader::RecordBatch {
            assert_eq!(message.version(), MetadataVersion::V5);
            let record = message.header_as_record_batch().unwrap();
            assert!(record.compression().is_none());
            assert!(record.variadicBufferCounts().is_none());
            assert_eq!(record.nodes().unwrap().len(), 9);
            assert_eq!(record.buffers().unwrap().len(), 18);
            let buffers = position
                + (record.buffers().unwrap().get(0) as *const arrow::ipc::Buffer as usize
                    - metadata.as_ptr() as usize);
            let nodes = position
                + (record.nodes().unwrap().get(0) as *const arrow::ipc::FieldNode as usize
                    - metadata.as_ptr() as usize);
            return Ok(NestedFrameFile {
                file,
                bytes,
                body,
                buffers,
                nodes,
            });
        }
        assert_ne!(message.header_type(), MessageHeader::DictionaryBatch);
        position += len as usize + body;
    }
}

fn assert_nested_frame_raw_children(actual: &RecordBatch, expected: &RecordBatch) {
    use arrow::array::{TimestampMicrosecondArray, UInt32Array};
    let actual_list = actual
        .column(3)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let expected_list = expected
        .column(3)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(actual_list.value_offsets(), expected_list.value_offsets());
    let actual_values = actual_list
        .values()
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();
    let expected_values = expected_list
        .values()
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();
    assert_eq!(
        actual_values.values(),
        expected_values.values(),
        "stored values under NULL list parents/children survive"
    );
    assert_eq!(actual_values.nulls(), expected_values.nulls());
    let actual_struct = actual
        .column(4)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let expected_struct = expected
        .column(4)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(
        actual_struct
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        expected_struct
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
    );
    assert_eq!(
        actual_struct
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .values(),
        expected_struct
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .values()
    );
    let actual_text = actual_struct
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let expected_text = expected_struct
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(actual_text.value_offsets(), expected_text.value_offsets());
    assert_eq!(
        actual_text.value_data(),
        expected_text.value_data(),
        "stored bytes under NULL structs/text survive"
    );
}

fn assert_nested_frame_one_owner(
    data: &arrow::array::ArrayData,
    body: usize,
    base: &mut Option<usize>,
) {
    for buffer in data
        .buffers()
        .iter()
        .chain(data.nulls().map(|nulls| nulls.buffer()))
    {
        if buffer.is_empty() {
            continue;
        }
        let allocation = buffer.data_ptr().as_ptr() as usize;
        assert_eq!(
            buffer.capacity(),
            body,
            "every decoded slice retains the full IPC allocation"
        );
        assert_eq!(allocation % 8, 0);
        assert_eq!(buffer.as_ptr() as usize % 8, 0);
        assert_eq!(
            *base.get_or_insert(allocation),
            allocation,
            "recursive arrays have exactly one physical body owner"
        );
    }
    for child in data.child_data() {
        assert_nested_frame_one_owner(child, body, base);
    }
}

#[tokio::test]
async fn spill_admission_nested_frame_uses_one_aligned_body_and_preserves_escaped_leaf()
-> Result<()> {
    use arrow::array::TimestampMicrosecondArray;
    use arrow::ipc::reader::StreamReader;
    let expected = nested_frame_batch()?;
    let fixture = write_nested_frame(&expected)?;
    // A priori: 2048 rows carry 16B UUID +8B ID +4B type +4B list offsets
    // +8B list children +8B integer +8B timestamp +4B text offsets +128B text.
    // Bitmap/padding is below 8 KiB. One ~379KiB body and unchanged full
    // headers/schema/reader/ledger fees fit 512KiB; two bodies cannot fit.
    assert!((352 * 1024..400 * 1024).contains(&fixture.body));
    assert!(2 * fixture.body > NESTED_FRAME_LIMIT);
    let mut ordinary = StreamReader::try_new(std::fs::File::open(fixture.file.path())?, None)?;
    let oracle = ordinary.next().unwrap()?;
    assert_eq!(oracle, expected);
    assert_nested_frame_raw_children(&oracle, &expected);
    assert!(ordinary.next().is_none());
    drop(ordinary);

    for limit in [8 * 1024 * 1024, NESTED_FRAME_LIMIT] {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(limit));
        let reader = manager(Arc::clone(&pool), expected.schema());
        let mut stream = reader.read_spill_as_stream_unbuffered(fixture.file.clone(), None)?;
        let output = stream.next().await.unwrap()?;
        assert_eq!(
            output, oracle,
            "complete schema, all cells/nulls/timezone round-trip"
        );
        assert_nested_frame_raw_children(&output, &expected);
        let mut base = None;
        for column in output.columns() {
            assert_nested_frame_one_owner(&column.to_data(), fixture.body, &mut base);
        }
        assert!(base.is_some());
        let schema_alias = output.schema();
        let nested = output
            .column(4)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let leaf = nested.column(1).slice(17, 1);
        drop(output);
        assert!(stream.next().await.is_none());
        drop(stream);
        let held = pool.reserved();
        assert!(
            held >= fixture.body,
            "one timestamp slice still funds the complete shared body: {held}"
        );
        assert_eq!(schema_alias, expected.schema());
        let last_alias = leaf.slice(0, 1);
        drop(schema_alias);
        drop(leaf);
        assert_eq!(
            pool.reserved(),
            held,
            "last shallow alias preserves the same physical sponsor"
        );
        assert_eq!(
            last_alias
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .value(0),
            1_700_000_000_000_017_i64
        );
        drop(last_alias);
        assert_eq!(pool.reserved(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn spill_admission_nested_frame_invalid_descriptors_keep_full_predecode_bound() -> Result<()>
{
    let fixture = write_nested_frame(&nested_frame_batch()?)?;
    // DFS node order: UUID, UInt64, UInt32, List, UInt32 child, Struct,
    // Int64, timestamp, UTF8. Buffer order follows those same decoder branches.
    // List offsets are descriptor7, child data9, timestamp data14.
    let cases: Vec<(&str, usize, Vec<u8>)> = vec![
        (
            "misaligned-list-offsets",
            fixture.buffers + 7 * 16,
            1_i64.to_le_bytes().to_vec(),
        ),
        (
            "short-list-offsets",
            fixture.buffers + 7 * 16 + 8,
            4_i64.to_le_bytes().to_vec(),
        ),
        (
            "child-data-outside-body",
            fixture.buffers + 9 * 16,
            (fixture.body as i64 + 8).to_le_bytes().to_vec(),
        ),
        (
            "short-child-values",
            fixture.buffers + 9 * 16 + 8,
            4_i64.to_le_bytes().to_vec(),
        ),
        (
            "negative-list-child-length",
            fixture.nodes + 4 * 16,
            (-1_i64).to_le_bytes().to_vec(),
        ),
        (
            "struct-child-length-mismatch",
            fixture.nodes + 6 * 16,
            (NESTED_FRAME_ROWS as i64 - 1).to_le_bytes().to_vec(),
        ),
        (
            "child-null-count-too-large",
            fixture.nodes + 4 * 16 + 8,
            (2 * NESTED_FRAME_ROWS as i64 + 1).to_le_bytes().to_vec(),
        ),
    ];
    for (name, offset, replacement) in cases {
        let mut bytes = fixture.bytes.clone();
        bytes[offset..offset + replacement.len()].copy_from_slice(&replacement);
        std::fs::write(fixture.file.path(), bytes)?;
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(NESTED_FRAME_LIMIT));
        let reader = manager(Arc::clone(&pool), nested_frame_batch()?.schema());
        let mut stream = reader.read_spill_as_stream_unbuffered(fixture.file.clone(), None)?;
        let error = stream
            .next()
            .await
            .unwrap()
            .expect_err("unqualified nested frame keeps complete predecode bound");
        assert!(
            matches!(error.find_root(), DataFusionError::ResourcesExhausted(_)),
            "{name}: {error}"
        );
        assert!(
            error.to_string().contains("SpillReaderDecode"),
            "{name}: {error}"
        );
        drop(stream);
        assert_eq!(pool.reserved(), 0, "{name}");
    }
    std::fs::write(fixture.file.path(), fixture.bytes)?;
    Ok(())
}
