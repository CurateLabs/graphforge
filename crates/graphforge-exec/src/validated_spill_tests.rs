//! Direct spill-file fixtures for frame validation before DataFusion decodes (#2354).
use std::collections::VecDeque;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, DictionaryArray, Int64Array, ListArray, RecordBatch, StringArray, StructArray,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use arrow::ipc::MessageHeader;
use bytes::Bytes;
use datafusion::common::config::SpillCompression;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::SpillFile;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, SpillMetrics};
use datafusion::physical_plan::spill::SpillManager;
use futures::StreamExt;

use super::{CorruptSpillFile, FrameValidator, validated_disk_manager};

fn validated_runtime(dir: &std::path::Path, pool: Arc<dyn MemoryPool>) -> Arc<RuntimeEnv> {
    let files = DiskManagerBuilder::default()
        .with_mode(DiskManagerMode::Directories(vec![dir.to_path_buf()]));
    RuntimeEnvBuilder::new()
        .with_memory_pool(pool)
        .with_disk_manager_builder(validated_disk_manager(files).unwrap())
        .build_arc()
        .unwrap()
}

fn manager(runtime: Arc<RuntimeEnv>, schema: Arc<Schema>) -> SpillManager {
    SpillManager::new(
        runtime,
        SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
        schema,
    )
}

fn batches() -> Result<Vec<RecordBatch>> {
    (0..3)
        .map(|part: i64| {
            let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(
                (0..512).map(|row| part * 512 + row),
            ));
            let text: ArrayRef = Arc::new(
                (0..512)
                    .map(|row| (row % 5 != 0).then(|| format!("text-{part}-{row}")))
                    .collect::<StringArray>(),
            );
            let lists: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
                (0..512_i32)
                    .map(|row| (row % 7 != 0).then(|| (0..row % 4).map(Some).collect::<Vec<_>>())),
            ));
            let labels: ArrayRef = Arc::new(
                (0..512)
                    .map(|row| ["Person", "Place", "Thing"][row % 3])
                    .collect::<DictionaryArray<Int32Type>>(),
            );
            let nested: ArrayRef = Arc::new(StructArray::from(vec![(
                Arc::new(Field::new("entries", lists.data_type().clone(), true)),
                Arc::clone(&lists),
            )]));
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("text", DataType::Utf8, true),
                Field::new("list", lists.data_type().clone(), true),
                Field::new("label", labels.data_type().clone(), false),
                Field::new("nested", nested.data_type().clone(), false),
            ]));
            Ok(RecordBatch::try_new(
                schema,
                vec![ids, text, lists, labels, nested],
            )?)
        })
        .collect()
}

fn list_batch() -> Result<RecordBatch> {
    let lists: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
        (0..64).map(|row| Some((0..row % 5).map(Some).collect::<Vec<_>>())),
    ));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "list",
        lists.data_type().clone(),
        false,
    )]));
    Ok(RecordBatch::try_new(schema, vec![lists])?)
}

/// Byte positions of the first record batch frame of an IPC stream.
struct RecordFrame {
    metadata_start: usize,
    metadata_len: usize,
    body_start: usize,
    /// `(offset, length)` of each buffer, relative to the body.
    buffers: Vec<(usize, usize)>,
}

fn first_record_frame(bytes: &[u8]) -> RecordFrame {
    let word = |at: usize| u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap());
    let size = |value: i64| usize::try_from(value).unwrap();
    let mut position = 0;
    loop {
        let mut len = word(position) as usize;
        position += 4;
        if len == 0xffff_ffff {
            len = word(position) as usize;
            position += 4;
        }
        let metadata = &bytes[position..position + len];
        let message = arrow::ipc::root_as_message(metadata).unwrap();
        let body = size(message.bodyLength());
        if message.header_type() == MessageHeader::RecordBatch {
            let record = message.header_as_record_batch().unwrap();
            return RecordFrame {
                metadata_start: position,
                metadata_len: len,
                body_start: position + len,
                buffers: record
                    .buffers()
                    .unwrap()
                    .iter()
                    .map(|buffer| (size(buffer.offset()), size(buffer.length())))
                    .collect(),
            };
        }
        position += len + body;
    }
}

async fn read_all(manager: &SpillManager, file: Arc<dyn SpillFile>) -> Result<Vec<RecordBatch>> {
    let mut stream = manager.read_spill_as_stream_unbuffered(file, None)?;
    let mut out = Vec::new();
    while let Some(batch) = stream.next().await {
        out.push(batch?);
    }
    Ok(out)
}

fn assert_corrupt(error: &DataFusionError, reason: &str) {
    let DataFusionError::External(inner) = error.find_root() else {
        panic!("expected a corrupt spill file error, got {error}");
    };
    let corrupt = inner
        .downcast_ref::<CorruptSpillFile>()
        .unwrap_or_else(|| panic!("expected a corrupt spill file error, got {error}"));
    assert!(
        corrupt.to_string().contains(reason),
        "{reason:?} not in {corrupt}"
    );
}

fn spill_list_frame(
    dir: &std::path::Path,
    pool: &Arc<dyn MemoryPool>,
) -> Result<(SpillManager, Arc<dyn SpillFile>, Vec<u8>)> {
    let batch = list_batch()?;
    let manager = manager(validated_runtime(dir, Arc::clone(pool)), batch.schema())
        .with_compression_type(SpillCompression::Uncompressed);
    let file = manager
        .spill_record_batch_and_finish(std::slice::from_ref(&batch), "corrupt offsets")?
        .unwrap();
    let bytes = std::fs::read(file.path().unwrap())?;
    Ok((manager, file, bytes))
}

#[tokio::test]
async fn a_valid_spill_round_trips_exactly_through_validation() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let expected = batches()?;
    for codec in [
        SpillCompression::Uncompressed,
        SpillCompression::Lz4Frame,
        SpillCompression::Zstd,
    ] {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64 * 1024 * 1024));
        let manager = manager(validated_runtime(dir.path(), pool), expected[0].schema())
            .with_compression_type(codec);
        let file = manager
            .spill_record_batch_and_finish(&expected, "valid spill")?
            .unwrap();
        let on_disk = std::fs::read(file.path().unwrap())?;
        let mut forwarded = Vec::new();
        let mut bytes = file.read_stream()?;
        while let Some(chunk) = bytes.next().await {
            forwarded.extend_from_slice(&chunk?);
        }
        assert_eq!(
            forwarded, on_disk,
            "{codec:?}: forwarded bytes are the file"
        );
        assert_eq!(read_all(&manager, file).await?, expected, "{codec:?}");
    }
    Ok(())
}

#[tokio::test]
async fn a_decreasing_list_offset_is_refused_before_decode() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(8 * 1024 * 1024));
    let (manager, file, bytes) = spill_list_frame(dir.path(), &pool)?;
    let frame = first_record_frame(&bytes);
    // Buffers: list validity, list offsets, child validity, child values.
    let (offsets, length) = frame.buffers[1];
    assert!(length >= 4 * 65);
    for (name, slot, value) in [
        ("decreasing", 3, 0_i32),
        ("negative", 1, -5),
        ("past-the-child", 64, 1 << 20),
    ] {
        let mut corrupt = bytes.clone();
        let at = frame.body_start + offsets + 4 * slot;
        corrupt[at..at + 4].copy_from_slice(&value.to_le_bytes());
        std::fs::write(file.path().unwrap(), &corrupt)?;
        let error = read_all(&manager, Arc::clone(&file)).await.expect_err(name);
        assert_corrupt(&error, "Arrow refused a frame");
        assert_eq!(pool.reserved(), 0, "{name}");
    }
    std::fs::write(file.path().unwrap(), &bytes)?;
    assert_eq!(read_all(&manager, file).await?, vec![list_batch()?]);
    Ok(())
}

#[tokio::test]
async fn a_frame_claiming_more_bytes_than_the_file_holds_is_refused_without_allocating_them()
-> Result<()> {
    let dir = tempfile::tempdir()?;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(8 * 1024 * 1024));
    let (manager, file, bytes) = spill_list_frame(dir.path(), &pool)?;
    let frame = first_record_frame(&bytes);
    let metadata = &bytes[frame.metadata_start..frame.metadata_start + frame.metadata_len];
    let body_len = arrow::ipc::root_as_message(metadata).unwrap().bodyLength();
    let needle = body_len.to_le_bytes();
    let found: Vec<usize> = metadata
        .windows(8)
        .enumerate()
        .filter_map(|(index, window)| (window == needle).then_some(index))
        .collect();
    assert_eq!(found.len(), 1, "the body length is stored once");
    let mut corrupt = bytes.clone();
    let at = frame.metadata_start + found[0];
    corrupt[at..at + 8].copy_from_slice(&(i64::MAX / 2).to_le_bytes());
    std::fs::write(file.path().unwrap(), &corrupt)?;
    let error = read_all(&manager, Arc::clone(&file))
        .await
        .expect_err("claimed body");
    // An `i64::MAX / 2` allocation would abort; the frame is refused at EOF.
    assert_corrupt(&error, "ends inside a frame");

    std::fs::write(file.path().unwrap(), &bytes[..frame.body_start + 8])?;
    let error = read_all(&manager, Arc::clone(&file))
        .await
        .expect_err("truncated body");
    assert_corrupt(&error, "ends inside a frame");
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn validation_is_independent_of_chunk_boundaries() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(8 * 1024 * 1024));
    let (_, _, bytes) = spill_list_frame(dir.path(), &pool)?;
    let frame = first_record_frame(&bytes);
    let mut corrupt = bytes.clone();
    let at = frame.body_start + frame.buffers[1].0 + 4 * 3;
    corrupt[at..at + 4].copy_from_slice(&0_i32.to_le_bytes());
    for chunk in [1, 3, 8, 13, bytes.len()] {
        let mut validator = FrameValidator::default();
        let mut ready = VecDeque::new();
        for piece in bytes.chunks(chunk) {
            validator.push(Bytes::copy_from_slice(piece), &mut ready)?;
        }
        validator.finish()?;
        let forwarded: Vec<u8> = ready.iter().flat_map(|bytes| bytes.to_vec()).collect();
        assert_eq!(forwarded, bytes, "chunk {chunk}");

        let mut validator = FrameValidator::default();
        let mut ready = VecDeque::new();
        let error = corrupt
            .chunks(chunk)
            .try_for_each(|piece| validator.push(Bytes::copy_from_slice(piece), &mut ready))
            .expect_err("corrupt offsets");
        assert_corrupt(&error, "Arrow refused a frame");
        let forwarded: usize = ready.iter().map(Bytes::len).sum();
        assert!(
            forwarded <= frame.metadata_start - 8,
            "chunk {chunk}: nothing of the corrupt frame is forwarded"
        );
    }
    Ok(())
}

#[tokio::test]
async fn the_session_runtime_validates_spill_files_and_keeps_its_cap() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let resources = crate::session::SessionResourceConfig {
        spill_enabled: true,
        spill_directory: Some(dir.path().to_path_buf()),
        spill_max_bytes: Some(64 * 1024),
        ..Default::default()
    };
    let (runtime, _) = crate::session::session_runtime(&resources).unwrap();
    assert_eq!(runtime.disk_manager.max_temp_directory_size(), 64 * 1024);
    let batch = list_batch()?;
    let manager = manager(Arc::clone(&runtime), batch.schema())
        .with_compression_type(SpillCompression::Uncompressed);
    let file = manager
        .spill_record_batch_and_finish(std::slice::from_ref(&batch), "session spill")?
        .unwrap();
    assert!(file.path().unwrap().starts_with(dir.path()));
    let bytes = std::fs::read(file.path().unwrap())?;
    let frame = first_record_frame(&bytes);
    let mut corrupt = bytes.clone();
    let at = frame.body_start + frame.buffers[1].0 + 4 * 3;
    corrupt[at..at + 4].copy_from_slice(&0_i32.to_le_bytes());
    std::fs::write(file.path().unwrap(), &corrupt)?;
    let error = read_all(&manager, Arc::clone(&file))
        .await
        .expect_err("corrupt offsets");
    assert_corrupt(&error, "Arrow refused a frame");
    std::fs::write(file.path().unwrap(), &bytes)?;
    assert_eq!(read_all(&manager, file).await?, vec![batch.clone()]);

    let large = arrow::compute::concat_batches(&batch.schema(), &vec![batch; 1024])?;
    let Err(error) =
        manager.spill_record_batch_and_finish(std::slice::from_ref(&large), "over the cap")
    else {
        panic!("the spill cap still applies");
    };
    assert!(error.to_string().contains("allowable limit"), "{error}");
    Ok(())
}
