//! Checked one-body IPC admission, with real files and an unchanged fixed pool.
use super::*;
use arrow::array::{AsArray, Float64Array, Int64Array, UInt64Array};
use arrow::buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::ipc::{MessageHeader, MetadataVersion, reader::StreamReader};
use datafusion::common::config::SpillCompression;
use datafusion::execution::disk_manager::RefCountedTempFile;

const READ_LIMIT: usize = 128 * 1024;
const ROWS: usize = 512;

struct FrameFile {
    file: RefCountedTempFile,
    schema: Arc<Schema>,
    bytes: Vec<u8>,
    body: usize,
    buffers: usize,
    nodes: usize,
    metadata: std::ops::Range<usize>,
}
fn flat_batch(rows: usize) -> RecordBatch {
    let count = rows + 6;
    let keys = Int64Array::from(
        (0..count)
            .map(|i| (i % 7 != 0).then_some(i as i64))
            .collect::<Vec<_>>(),
    );
    let text = StringArray::new(
        OffsetBuffer::new(ScalarBuffer::from(
            (0..=count).map(|i| (128 * i) as i32).collect::<Vec<_>>(),
        )),
        Buffer::from_vec(
            (0..count)
                .flat_map(|i| format!("{i:0128}").into_bytes())
                .collect(),
        ),
        Some(NullBuffer::from(
            (0..count).map(|i| i % 5 != 0).collect::<Vec<_>>(),
        )),
    );
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, true),
            Field::new("value", DataType::Utf8, true),
        ])),
        vec![Arc::new(keys), Arc::new(text)],
    )
    .unwrap()
    .slice(3, rows)
}
fn write_frame(batch: &RecordBatch, codec: SpillCompression) -> Result<FrameFile> {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(16 * 1024 * 1024));
    let manager = manager(pool, batch.schema()).with_compression_type(codec);
    let file = manager
        .spill_record_batch_and_finish(std::slice::from_ref(batch), "checked flat IPC frame")?
        .unwrap();
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
            let buffers = position
                + (record.buffers().unwrap().get(0) as *const arrow::ipc::Buffer as usize
                    - metadata.as_ptr() as usize);
            let nodes = position
                + (record.nodes().unwrap().get(0) as *const arrow::ipc::FieldNode as usize
                    - metadata.as_ptr() as usize);
            return Ok(FrameFile {
                file,
                schema: batch.schema(),
                bytes,
                body,
                buffers,
                nodes,
                metadata: position..position + len as usize,
            });
        }
        position += len as usize + body;
    }
}
fn with_variadic(fixture: &FrameFile) -> Vec<u8> {
    let original = &fixture.bytes[fixture.metadata.clone()];
    let message = arrow::ipc::root_as_message(original).unwrap();
    let record = message.header_as_record_batch().unwrap();
    let table = record._tab.loc();
    let vtable_delta = i32::from_le_bytes(original[table..table + 4].try_into().unwrap());
    let old_vtable = (table as isize - vtable_delta as isize) as usize;
    let old_len =
        u16::from_le_bytes(original[old_vtable..old_vtable + 2].try_into().unwrap()) as usize;
    let length = usize::from(arrow::ipc::RecordBatch::VT_VARIADICBUFFERCOUNTS) + 2;
    let mut vtable = vec![0_u8; length];
    vtable[..old_len.min(length)]
        .copy_from_slice(&original[old_vtable..old_vtable + old_len.min(length)]);
    vtable[..2].copy_from_slice(&(length as u16).to_le_bytes());
    // Reuse the existing small row-count scalar as a relative u32 pointer to
    // a newly appended EMPTY vector. The original row count is unchanged;
    // ordinary Arrow also accepts this frame if the predicate regresses.
    let length_field = record._tab.vtable().get(arrow::ipc::RecordBatch::VT_LENGTH);
    let empty_vector = table + usize::from(length_field) + record.length() as usize;
    assert_eq!((empty_vector + 4) % 8, 0);
    let slot = usize::from(arrow::ipc::RecordBatch::VT_VARIADICBUFFERCOUNTS);
    vtable[slot..slot + 2].copy_from_slice(&length_field.to_le_bytes());
    let mut metadata = original.to_vec();
    let new_vtable = metadata.len();
    metadata.extend_from_slice(&vtable);
    assert!(empty_vector > metadata.len());
    metadata.resize(empty_vector + 4, 0);
    while metadata.len() % 8 != 0 {
        metadata.push(0);
    }
    metadata[table..table + 4]
        .copy_from_slice(&((table as isize - new_vtable as isize) as i32).to_le_bytes());
    assert!(
        arrow::ipc::root_as_message(&metadata)
            .unwrap()
            .header_as_record_batch()
            .unwrap()
            .variadicBufferCounts()
            .is_some()
    );
    let mut bytes = fixture.bytes[..fixture.metadata.start].to_vec();
    bytes[fixture.metadata.start - 4..fixture.metadata.start]
        .copy_from_slice(&(metadata.len() as i32).to_le_bytes());
    bytes.extend_from_slice(&metadata);
    bytes.extend_from_slice(&fixture.bytes[fixture.metadata.end..]);
    bytes
}
async fn read_tight(fixture: &FrameFile, schema: Arc<Schema>) -> Result<RecordBatch> {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(READ_LIMIT));
    let manager = manager(Arc::clone(&pool), schema);
    let mut stream = manager.read_spill_as_stream_unbuffered(fixture.file.clone(), None)?;
    let output = stream.next().await.unwrap()?;
    assert!(stream.next().await.is_none());
    drop(stream);
    assert!(pool.reserved() >= fixture.body);
    Ok(output)
}

#[tokio::test]
async fn spill_admission_flat_frame_uses_one_full_aligned_body_and_preserves_escaped_slice()
-> Result<()> {
    let batch = flat_batch(ROWS);
    let fixture = write_frame(&batch, SpillCompression::Uncompressed)?;
    assert!(
        (64 * 1024..80 * 1024).contains(&fixture.body),
        "the fixed128KiB pool must fit one complete body, not two"
    );
    let mut ordinary = StreamReader::try_new(std::fs::File::open(fixture.file.path())?, None)?;
    let oracle = ordinary.next().unwrap()?;
    assert_eq!(oracle, batch);
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(READ_LIMIT));
    let manager = manager(Arc::clone(&pool), batch.schema());
    let mut stream = manager.read_spill_as_stream_unbuffered(fixture.file.clone(), None)?;
    let output = stream.next().await.unwrap()?;
    assert_eq!(output, oracle);
    let mut base = None;
    for column in output.columns() {
        let data = column.to_data();
        for buffer in data
            .buffers()
            .iter()
            .chain(data.nulls().map(|n| n.buffer()))
        {
            if buffer.capacity() == 0 {
                continue;
            }
            assert_eq!(buffer.capacity(), fixture.body);
            assert_eq!(buffer.data_ptr().as_ptr() as usize % 8, 0);
            assert_eq!(buffer.as_ptr() as usize % 8, 0);
            assert_eq!(
                *base.get_or_insert(buffer.data_ptr()),
                buffer.data_ptr(),
                "all qualified buffers share the complete physical message body"
            );
        }
    }
    let alias = output.column(1).slice(17, 1);
    drop(output);
    assert!(stream.next().await.is_none());
    drop(stream);
    assert!(pool.reserved() >= fixture.body);
    drop(alias);
    assert_eq!(pool.reserved(), 0);

    let empty = flat_batch(0);
    let fixture = write_frame(&empty, SpillCompression::Uncompressed)?;
    let output = read_tight(&fixture, empty.schema()).await?;
    assert_eq!(output, empty);
    Ok(())
}

#[tokio::test]
async fn spill_admission_flat_frame_unqualified_descriptors_keep_predecode_admission() -> Result<()>
{
    let fixture = write_frame(&flat_batch(ROWS), SpillCompression::Uncompressed)?;
    // Scalar slots below are pinned Arrow IPC Buffer/FieldNode structs: two
    // little-endian i64 values; a vector's u32 length precedes its first item.
    let cases: Vec<(&str, usize, Vec<u8>)> = vec![
        (
            "unaligned-null-buffer",
            fixture.buffers,
            1_i64.to_le_bytes().to_vec(),
        ),
        (
            "negative-offset",
            fixture.buffers,
            (-1_i64).to_le_bytes().to_vec(),
        ),
        (
            "out-of-body-offset",
            fixture.buffers,
            (fixture.body as i64 + 1).to_le_bytes().to_vec(),
        ),
        (
            "overflowing-end",
            fixture.buffers + 16,
            i64::MAX.to_le_bytes().to_vec(),
        ),
        (
            "missing-null-bitmap",
            fixture.buffers + 8,
            0_i64.to_le_bytes().to_vec(),
        ),
        (
            "short-values",
            fixture.buffers + 16 + 8,
            (8 * (ROWS as i64 - 1)).to_le_bytes().to_vec(),
        ),
        (
            "short-offsets",
            fixture.buffers + 3 * 16 + 8,
            (4 * ROWS as i64).to_le_bytes().to_vec(),
        ),
        (
            "negative-node-length",
            fixture.nodes,
            (-1_i64).to_le_bytes().to_vec(),
        ),
        (
            "invalid-null-count",
            fixture.nodes + 8,
            (ROWS as i64 + 1).to_le_bytes().to_vec(),
        ),
        (
            "wrong-node-count",
            fixture.nodes - 4,
            0_u32.to_le_bytes().to_vec(),
        ),
        (
            "wrong-buffer-count",
            fixture.buffers - 4,
            0_u32.to_le_bytes().to_vec(),
        ),
    ];
    for (name, position, replacement) in cases {
        let mut bytes = fixture.bytes.clone();
        bytes[position..position + replacement.len()].copy_from_slice(&replacement);
        std::fs::write(fixture.file.path(), bytes)?;
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(READ_LIMIT));
        let manager = manager(Arc::clone(&pool), Arc::clone(&fixture.schema));
        let mut stream = manager.read_spill_as_stream_unbuffered(fixture.file.clone(), None)?;
        let error = stream.next().await.unwrap().expect_err(name);
        assert!(
            matches!(
                error.find_root(),
                datafusion::common::DataFusionError::ResourcesExhausted(_)
            ),
            "{name}: {error}"
        );
        drop(stream);
        assert_eq!(pool.reserved(), 0, "{name}");
    }
    std::fs::write(fixture.file.path(), &fixture.bytes)?;
    Ok(())
}

#[tokio::test]
async fn spill_admission_flat_frame_compression_unknown_types_and_variadic_stay_unqualified()
-> Result<()> {
    let batch = flat_batch(ROWS);
    let unknown = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Float64, true),
            Field::new("value", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Float64Array::from_iter(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .map(|v| v.map(|v| v as f64)),
            )),
            Arc::clone(batch.column(1)),
        ],
    )?;
    for (source, codec, variadic) in [
        (batch.clone(), SpillCompression::Zstd, false),
        (unknown, SpillCompression::Uncompressed, false),
        (flat_batch(ROWS + 4), SpillCompression::Uncompressed, true),
    ] {
        let fixture = write_frame(&source, codec)?;
        let mut ordinary = StreamReader::try_new(std::fs::File::open(fixture.file.path())?, None)?;
        assert_eq!(ordinary.next().unwrap()?, source);
        if variadic {
            std::fs::write(fixture.file.path(), with_variadic(&fixture))?;
            let mut ordinary =
                StreamReader::try_new(std::fs::File::open(fixture.file.path())?, None)?;
            assert_eq!(ordinary.next().unwrap()?, source);
        }
        let schema = source.schema();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(READ_LIMIT));
        let manager = manager(Arc::clone(&pool), schema);
        let mut stream = manager.read_spill_as_stream_unbuffered(fixture.file.clone(), None)?;
        let error = stream
            .next()
            .await
            .unwrap()
            .expect_err("unqualified body needs the original full workspace");
        assert!(
            matches!(
                error.find_root(),
                datafusion::common::DataFusionError::ResourcesExhausted(_)
            ),
            "{codec:?}: {error}"
        );
        drop(stream);
        assert_eq!(pool.reserved(), 0);
    }
    Ok(())
}

/// All frames from one private reader share one parsed heap schema. The fixed
/// pool fits the real bodies and per-batch array metadata, but cannot fit a
/// separate schema-construction bound for each of these retained batches.
#[tokio::test]
async fn spill_admission_flat_frame_shares_only_the_actual_reader_schema_until_last_body_drop()
-> Result<()> {
    let source = flat_batch(4);
    let writer_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(16 * 1024 * 1024));
    let writer = manager(writer_pool, source.schema());
    let file = writer
        .spill_record_batch_and_finish(&vec![source.clone(); 24], "shared parsed schema frames")?
        .unwrap();
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(128 * 1024));
    let reader = manager(Arc::clone(&pool), source.schema());
    let mut stream = reader.read_spill_as_stream_unbuffered(file.clone(), None)?;
    let mut batches = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        assert_eq!(batch, source);
        assert!(
            !Arc::ptr_eq(batch.schema_ref(), source.schema_ref()),
            "the expected external schema is equal but separately allocated"
        );
        if let Some(first) = batches.first() {
            assert!(Arc::ptr_eq(
                batch.schema_ref(),
                RecordBatch::schema_ref(first)
            ));
        }
        batches.push(batch);
    }
    assert_eq!(batches.len(), 24);
    // A second reader of the identical file owns a distinct parsed schema.
    let independent_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(128 * 1024));
    let independent = manager(Arc::clone(&independent_pool), source.schema());
    let mut other = independent.read_spill_as_stream_unbuffered(file, None)?;
    let other_batch = other.next().await.unwrap()?;
    assert!(!Arc::ptr_eq(
        batches[0].schema_ref(),
        other_batch.schema_ref()
    ));
    drop(other_batch);
    drop(other);
    assert_eq!(independent_pool.reserved(), 0);

    let alias = batches[0].column(1).slice(1, 1);
    let body = batches[0].column(1).as_string::<i32>().values().capacity();
    drop(batches);
    let after_eof = pool.reserved();
    assert!(
        after_eof > body,
        "last buffer must retain schema and metadata authority"
    );
    drop(stream);
    assert_eq!(
        pool.reserved(),
        after_eof,
        "EOF already dropped reader-owned data"
    );
    assert_eq!(
        alias.as_string::<i32>().value(0),
        source.column(1).as_string::<i32>().value(1)
    );
    drop(alias);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

/// Real encoder growth under a fixed workspace quota, with full source backing
/// separately admitted. Nonzero offsets and NULL cells keep stored byte spans.
#[tokio::test]
async fn spill_admission_flat_writer_admits_real_growth_and_rebased_null_spans() -> Result<()> {
    let inputs = vec![flat_batch(8192), flat_batch(0), flat_batch(512)];
    let schema = inputs[0].schema();
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(16 * 1024 * 1024));
    let source = MemoryConsumer::new("writer source backing").register(&pool);
    source.try_grow(inputs.iter().map(physical_backing).sum())?;
    let pressure = MemoryConsumer::new("writer fixed pressure").register(&pool);
    pressure.try_grow(16 * 1024 * 1024 - pool.reserved() - 1536 * 1024)?;
    let baseline = pool.reserved();
    let writer = manager(Arc::clone(&pool), schema);
    let file = writer
        .spill_record_batch_and_finish(&inputs, "flat writer real workspace")?
        .unwrap();
    assert_eq!(
        pool.reserved(),
        baseline,
        "temporary encoder authority releases after finish"
    );
    let mut ordinary = StreamReader::try_new(std::fs::File::open(file.path())?, None)?;
    for input in &inputs {
        let output = ordinary.next().unwrap()?;
        assert_eq!(&output, input);
        let expected = input.column(1).as_string::<i32>();
        let actual = output.column(1).as_string::<i32>();
        let offsets = expected.value_offsets();
        assert_eq!(
            actual.value_data(),
            &expected.value_data()[offsets[0] as usize..offsets[input.num_rows()] as usize]
        );
    }
    assert!(ordinary.next().is_none());
    drop(pressure);
    drop(source);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn spill_admission_flat_writer_refuses_before_encoding_and_keeps_unqualified_bounds()
-> Result<()> {
    let flat = flat_batch(8192);
    let unknown = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::UInt64, true),
            Field::new("value", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(UInt64Array::from_iter(
                (0..flat.num_rows()).map(|i| Some(i as u64)),
            )),
            flat.column(1).clone(),
        ],
    )?;
    for (batch, codec, headroom) in [
        (flat.clone(), SpillCompression::Uncompressed, 64 * 1024),
        (unknown, SpillCompression::Uncompressed, 1536 * 1024),
        (flat, SpillCompression::Zstd, 1536 * 1024),
    ] {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(16 * 1024 * 1024));
        let source = MemoryConsumer::new("refusing writer source").register(&pool);
        source.try_grow(physical_backing(&batch))?;
        let pressure = MemoryConsumer::new("refusing writer pressure").register(&pool);
        pressure.try_grow(16 * 1024 * 1024 - pool.reserved() - headroom)?;
        let baseline = pool.reserved();
        let writer = manager(Arc::clone(&pool), batch.schema()).with_compression_type(codec);
        let mut pending = writer.create_in_progress_file("before encoder allocation")?;
        let error = pending
            .append_batch(&batch)
            .expect_err("source-correct workspace must be admitted before encoding");
        assert!(
            matches!(
                error.find_root(),
                datafusion::common::DataFusionError::ResourcesExhausted(_)
            ),
            "{codec:?}: {error}"
        );
        assert!(error.to_string().contains("SpillWriterEncode"), "{error}");
        assert!(
            pending.finish()?.is_none(),
            "refusal must precede stream/schema/body writes"
        );
        assert_eq!(pool.reserved(), baseline);
        drop(pressure);
        drop(source);
        assert_eq!(pool.reserved(), 0);
    }
    Ok(())
}
