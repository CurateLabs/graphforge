//! Spill reads of a nested IPC frame, observed through the query pool.
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, ListArray, RecordBatch, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
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
    let bytes = std::fs::read(file.path().expect("local spill file"))?;
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

#[tokio::test]
async fn a_nested_frame_with_invalid_descriptors_is_refused_before_decode() -> Result<()> {
    // DataFusion decodes spill frames without Arrow validation; GraphForge's
    // validated spill files refuse each corrupt descriptor with a typed error
    // before that decoder, holding no pool memory (#2354).
    let fixture = write_nested_frame(&nested_frame_batch()?)?;
    let dir = tempfile::tempdir()?;
    let files = datafusion::execution::disk_manager::DiskManagerBuilder::default().with_mode(
        datafusion::execution::disk_manager::DiskManagerMode::Directories(vec![
            dir.path().to_path_buf(),
        ]),
    );
    let validated = crate::validated_spill::validated_disk_manager(files)?;
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
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(NESTED_FRAME_LIMIT));
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&pool))
            .with_disk_manager_builder(validated.clone())
            .build_arc()?;
        let reader = SpillManager::new(
            runtime,
            SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
            nested_frame_batch()?.schema(),
        );
        let file = reader
            .spill_record_batch_and_finish(&[nested_frame_batch()?], "corrupt nested frame")?
            .unwrap();
        let path = file.path().expect("local spill file");
        assert_eq!(
            std::fs::read(path)?,
            fixture.bytes,
            "{name}: same frame layout"
        );
        std::fs::write(path, &bytes)?;
        let mut stream = reader.read_spill_as_stream_unbuffered(file, None)?;
        let Err(error) = stream.next().await.unwrap() else {
            panic!("{name}: a corrupt nested frame decoded");
        };
        assert!(
            matches!(
                error.find_root(),
                DataFusionError::External(inner)
                    if inner.is::<crate::validated_spill::CorruptSpillFile>()
            ),
            "{name}: {error}"
        );
        assert!(stream.next().await.is_none(), "{name}");
        drop(stream);
        assert_eq!(pool.reserved(), 0, "{name}");
    }
    Ok(())
}
