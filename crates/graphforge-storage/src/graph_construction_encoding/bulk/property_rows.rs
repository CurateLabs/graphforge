//! Exact-schema property rows on disposable, CRC-protected Arrow IPC runs.
//!
//! Run fan-in is two. Binary levels compact runs during intake, so neither
//! decoded payload nor the run inventory grows with the source's batch count.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::UInt32Array;
use arrow::compute::concat_batches;
use arrow::ipc::{reader::StreamReader, writer::StreamWriter};
use arrow::record_batch::RecordBatch;

use super::scratch::{Scratch, crc32c};
use super::tables::check_cancelled;
use super::{AtomicBool, ConstructionChunkKind, GfError, GraphConstructionBudgets, storage};

const FRAME_TARGET: usize = 1 << 20;
#[cfg(test)]
thread_local! { static FORCED_FRAME_BYTES: std::cell::Cell<Option<usize>> = const {std::cell::Cell::new(None)}; }
#[cfg(test)]
pub(crate) struct ForcedPropertyFrames;
#[cfg(test)]
impl ForcedPropertyFrames {
    pub(crate) fn set(bytes: usize) -> Self {
        FORCED_FRAME_BYTES.with(|forced| forced.set(Some(bytes.max(1))));
        Self
    }
}
#[cfg(test)]
impl Drop for ForcedPropertyFrames {
    fn drop(&mut self) {
        FORCED_FRAME_BYTES.with(|forced| forced.set(None));
    }
}

const HEADER: usize = 16;

struct Group {
    levels: Vec<Option<PathBuf>>,
}

pub(super) struct PropertyRows<'a> {
    scratch: &'a Scratch,
    kind: ConstructionChunkKind,
    budgets: GraphConstructionBudgets,
    schema_bytes: usize,
    frame_target: usize,
    groups: Mutex<BTreeMap<String, Group>>,
    next_file: AtomicU64,
    written: AtomicU64,
    read: AtomicU64,
}

pub(super) struct SortedGroup {
    pub(super) path: PathBuf,
}

/// Retain compact pieces in a binary tree. At most logarithmically many Arrow
/// headers exist; concatenation moves each row at most once per tree level.
pub(super) struct BatchAccumulator {
    levels: Vec<Option<RecordBatch>>,
}

impl BatchAccumulator {
    pub(super) fn new() -> Self {
        Self { levels: Vec::new() }
    }

    pub(super) fn push(&mut self, mut batch: RecordBatch) -> Result<(), GfError> {
        let mut level = 0;
        loop {
            if level == self.levels.len() {
                self.levels.push(Some(batch));
                return Ok(());
            }
            let Some(previous) = self.levels[level].take() else {
                self.levels[level] = Some(batch);
                return Ok(());
            };
            batch = concat_batches(&batch.schema(), [&previous, &batch]).map_err(storage)?;
            level += 1;
        }
    }

    pub(super) fn finish(&mut self) -> Result<Option<RecordBatch>, GfError> {
        let batches = self
            .levels
            .iter_mut()
            .rev()
            .filter_map(Option::take)
            .collect::<Vec<_>>();
        self.levels.clear();
        let Some(first) = batches.first() else {
            return Ok(None);
        };
        if batches.len() == 1 {
            return Ok(batches.into_iter().next());
        }
        concat_batches(&first.schema(), &batches)
            .map(Some)
            .map_err(storage)
    }
}

impl<'a> PropertyRows<'a> {
    pub(super) fn new(
        scratch: &'a Scratch,
        kind: ConstructionChunkKind,
        budgets: GraphConstructionBudgets,
        schema_bytes: u64,
    ) -> Self {
        let frame_target = FRAME_TARGET;
        #[cfg(test)]
        let frame_target = FORCED_FRAME_BYTES
            .with(std::cell::Cell::get)
            .unwrap_or(frame_target);
        Self {
            scratch,
            kind,
            budgets,
            frame_target,
            schema_bytes: usize::try_from(schema_bytes).unwrap_or(usize::MAX),
            groups: Mutex::new(BTreeMap::new()),
            next_file: AtomicU64::new(0),
            written: AtomicU64::new(0),
            read: AtomicU64::new(0),
        }
    }

    pub(super) fn path(&self) -> Result<PathBuf, GfError> {
        let id = self.next_file.fetch_add(1, Ordering::Relaxed);
        let path = self
            .scratch
            .file(&format!("property-{:?}-{id:016}.frames", self.kind));
        File::create(&path).map_err(storage)?;
        Ok(path)
    }

    pub(super) fn written_bytes(&self) -> u64 {
        self.written.load(Ordering::Relaxed)
    }
    pub(super) fn read_bytes(&self) -> u64 {
        self.read.load(Ordering::Relaxed)
    }

    pub(super) fn write(&self, path: &Path, batch: &RecordBatch) -> Result<(), GfError> {
        let mut payload = Vec::new();
        {
            let mut writer =
                StreamWriter::try_new(&mut payload, &batch.schema()).map_err(storage)?;
            writer.write(batch).map_err(storage)?;
            writer.finish().map_err(storage)?;
        }
        // This ceiling includes IPC alignment, offsets and schema metadata.
        // Refuse corrupt/unbounded frames before allocating on the read side.
        if payload.len() > self.frame_limit() {
            return Err(storage(
                "property scratch frame exceeds its reserved workspace",
            ));
        }
        let mut file = OpenOptions::new()
            .append(true)
            .open(path)
            .map_err(storage)?;
        let mut header = [0_u8; HEADER];
        header[..8].copy_from_slice(&(payload.len() as u64).to_le_bytes());
        header[8..12].copy_from_slice(&crc32c(&payload).to_le_bytes());
        header[12..].copy_from_slice(
            &u32::try_from(batch.num_rows())
                .map_err(storage)?
                .to_le_bytes(),
        );
        file.write_all(&header).map_err(storage)?;
        file.write_all(&payload).map_err(storage)?;
        let bytes = (HEADER + payload.len()) as u64;
        self.written.fetch_add(bytes, Ordering::Relaxed);
        // Property frames stay until their final consumer reads them, so this
        // raises the scratch peak and nothing releases it early.
        self.scratch.occupy(bytes);
        Ok(())
    }

    fn frame_limit(&self) -> usize {
        self.budgets
            .max_batch_bytes
            .saturating_mul(2)
            .saturating_add(self.schema_bytes.saturating_mul(4))
            .saturating_add(1 << 20)
    }

    pub(super) fn reader<'r>(&'r self, path: &Path) -> Result<RowsReader<'r, 'a>, GfError> {
        Ok(RowsReader {
            rows: self,
            file: File::open(path).map_err(storage)?,
        })
    }

    /// Compact a bounded range before retaining it; Arrow slices can retain
    /// arbitrarily large source buffers, including hidden nested children.
    pub(super) fn copy_range(
        batch: &RecordBatch,
        start: usize,
        len: usize,
    ) -> Result<RecordBatch, GfError> {
        let indices = UInt32Array::from(
            (start..start + len)
                .map(u32::try_from)
                .collect::<Result<Vec<_>, _>>()
                .map_err(storage)?,
        );
        let columns = batch
            .columns()
            .iter()
            .map(|column| arrow::compute::take(column.as_ref(), &indices, None).map_err(storage))
            .collect::<Result<Vec<_>, _>>()?;
        RecordBatch::try_new(batch.schema(), columns).map_err(storage)
    }

    fn write_compact(&self, path: &Path, batch: &RecordBatch) -> Result<(), GfError> {
        let bytes = batch.get_array_memory_size();
        let step = (batch.num_rows().saturating_mul(self.frame_target) / bytes.max(1)).max(1);
        let mut offset = 0;
        while offset < batch.num_rows() {
            let len = step.min(batch.num_rows() - offset);
            let compact = Self::copy_range(batch, offset, len)?;
            self.write(path, &compact)?;
            offset += len;
        }
        Ok(())
    }

    pub(super) fn ingest(&self, batch: &RecordBatch, cancel: &AtomicBool) -> Result<(), GfError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let digest = crate::graph_construction::normalized_schema_digest(batch.schema().as_ref());
        let uuid_name = match self.kind {
            ConstructionChunkKind::Node => "node_uuid",
            ConstructionChunkKind::Edge => "edge_uuid",
        };
        let uuids = crate::graph_construction::batch_uuid_column(batch, uuid_name)?;
        let mut indexes =
            (0..u32::try_from(batch.num_rows()).map_err(storage)?).collect::<Vec<_>>();
        indexes.sort_unstable_by(|left, right| {
            uuids
                .value(*left as usize)
                .cmp(uuids.value(*right as usize))
        });
        let indexes = UInt32Array::from(indexes);
        let columns = batch
            .columns()
            .iter()
            .map(|column| arrow::compute::take(column.as_ref(), &indexes, None).map_err(storage))
            .collect::<Result<Vec<_>, _>>()?;
        let sorted = RecordBatch::try_new(batch.schema(), columns).map_err(storage)?;
        let mut path = self.path()?;
        self.write_compact(&path, &sorted)?;
        drop(sorted);
        let mut groups = self
            .groups
            .lock()
            .map_err(|_| storage("property schema lock poisoned"))?;
        if !groups.contains_key(&digest) && groups.len() >= self.budgets.max_schema_groups {
            return Err(storage("construction schema-group budget exhausted"));
        }
        let group = groups
            .entry(digest)
            .or_insert_with(|| Group { levels: Vec::new() });
        let mut level = 0;
        loop {
            check_cancelled(cancel)?;
            if level == group.levels.len() {
                group.levels.push(Some(path));
                break;
            }
            let Some(previous) = group.levels[level].take() else {
                group.levels[level] = Some(path);
                break;
            };
            let merged = self.merge(&previous, &path, uuid_name, cancel)?;
            std::fs::remove_file(previous).map_err(storage)?;
            std::fs::remove_file(path).map_err(storage)?;
            path = merged;
            level += 1;
        }
        Ok(())
    }

    pub(super) fn finish(&self, cancel: &AtomicBool) -> Result<Vec<SortedGroup>, GfError> {
        let groups = std::mem::take(
            &mut *self
                .groups
                .lock()
                .map_err(|_| storage("property schema lock poisoned"))?,
        );
        let uuid = match self.kind {
            ConstructionChunkKind::Node => "node_uuid",
            ConstructionChunkKind::Edge => "edge_uuid",
        };
        let mut result = Vec::with_capacity(groups.len());
        for group in groups.into_values() {
            let mut paths = group.levels.into_iter().rev().flatten();
            let Some(mut path) = paths.next() else {
                continue;
            };
            for next in paths {
                let merged = self.merge(&path, &next, uuid, cancel)?;
                std::fs::remove_file(path).map_err(storage)?;
                std::fs::remove_file(next).map_err(storage)?;
                path = merged;
            }
            result.push(SortedGroup { path });
        }
        Ok(result)
    }

    fn merge(
        &self,
        left: &Path,
        right: &Path,
        uuid_name: &str,
        cancel: &AtomicBool,
    ) -> Result<PathBuf, GfError> {
        let output = self.path()?;
        let mut readers = [self.reader(left)?, self.reader(right)?];
        let mut batches = [readers[0].next()?, readers[1].next()?];
        let mut offsets = [0_usize; 2];
        let mut accumulator = BatchAccumulator::new();
        let mut accumulated_bytes = 0;
        let mut accumulated_rows = 0;
        while batches.iter().any(Option::is_some) {
            check_cancelled(cancel)?;
            let side = match (&batches[0], &batches[1]) {
                (Some(a), Some(b)) => usize::from(
                    crate::graph_construction::batch_uuid_column(a, uuid_name)?.value(offsets[0])
                        > crate::graph_construction::batch_uuid_column(b, uuid_name)?
                            .value(offsets[1]),
                ),
                (Some(_), None) => 0,
                (None, Some(_)) => 1,
                (None, None) => break,
            };
            let batch = batches[side].as_ref().expect("selected live run");
            let start = offsets[side];
            let mut end = start + 1;
            // Copy contiguous winning rows together, rather than making one
            // RecordBatch per row for an already sorted run.
            if let Some(other) = &batches[1 - side] {
                let key = crate::graph_construction::batch_uuid_column(other, uuid_name)?
                    .value(offsets[1 - side]);
                let uuids = crate::graph_construction::batch_uuid_column(batch, uuid_name)?;
                while end < batch.num_rows() && uuids.value(end) <= key {
                    end += 1;
                }
            } else {
                end = batch.num_rows();
            }
            let piece = Self::copy_range(batch, start, end - start)?;
            if accumulated_rows + piece.num_rows() > self.budgets.max_batch_rows {
                if let Some(pending) = accumulator.finish()? {
                    self.write(&output, &pending)?;
                }
                accumulated_bytes = 0;
                accumulated_rows = 0;
            }
            accumulated_rows += piece.num_rows();
            accumulated_bytes += piece.get_array_memory_size();
            accumulator.push(piece)?;
            offsets[side] = end;
            if end == batch.num_rows() {
                batches[side] = readers[side].next()?;
                offsets[side] = 0;
            }
            if accumulated_bytes >= self.frame_target {
                self.write(
                    &output,
                    &accumulator.finish()?.expect("nonempty accumulator"),
                )?;
                accumulated_bytes = 0;
                accumulated_rows = 0;
            }
        }
        if let Some(batch) = accumulator.finish()? {
            self.write(&output, &batch)?;
        }
        Ok(output)
    }
}

pub(super) struct RowsReader<'r, 's> {
    rows: &'r PropertyRows<'s>,
    file: File,
}

impl RowsReader<'_, '_> {
    pub(super) fn next(&mut self) -> Result<Option<RecordBatch>, GfError> {
        let mut header = [0_u8; HEADER];
        let n = self.file.read(&mut header[..1]).map_err(storage)?;
        if n == 0 {
            return Ok(None);
        }
        self.file
            .read_exact(&mut header[1..])
            .map_err(|_| storage("truncated property scratch header"))?;
        let size = usize::try_from(u64::from_le_bytes(
            header[..8].try_into().expect("eight bytes"),
        ))
        .map_err(storage)?;
        if size > self.rows.frame_limit() {
            return Err(storage(
                "property scratch frame exceeds its reserved workspace",
            ));
        }
        let mut payload = vec![0_u8; size];
        self.file
            .read_exact(&mut payload)
            .map_err(|_| storage("truncated property scratch payload"))?;
        self.rows
            .read
            .fetch_add((HEADER + size) as u64, Ordering::Relaxed);
        if crc32c(&payload) != u32::from_le_bytes(header[8..12].try_into().expect("four bytes")) {
            return Err(storage("property scratch CRC32C mismatch"));
        }
        let expected_rows =
            u32::from_le_bytes(header[12..].try_into().expect("four bytes")) as usize;
        if expected_rows > self.rows.budgets.max_batch_rows {
            return Err(storage(
                "property scratch frame row count exceeds its reservation",
            ));
        }
        validate_ipc(
            &payload,
            expected_rows,
            self.rows.budgets.max_property_columns
                + match self.rows.kind {
                    ConstructionChunkKind::Node => 2,
                    ConstructionChunkKind::Edge => 4,
                },
            self.rows
                .schema_bytes
                .saturating_mul(4)
                .saturating_add(1 << 20),
        )?;
        let mut stream =
            StreamReader::try_new(std::io::Cursor::new(payload), None).map_err(storage)?;
        let batch = stream
            .next()
            .transpose()
            .map_err(storage)?
            .ok_or_else(|| storage("property scratch frame has no batch"))?;
        if stream.next().is_some()
            || batch.num_rows()
                != u32::from_le_bytes(header[12..].try_into().expect("four bytes")) as usize
        {
            return Err(storage("property scratch frame row count differs"));
        }
        if batch.get_array_memory_size() > self.rows.frame_limit() {
            return Err(storage(
                "property scratch decoded frame exceeds its reservation",
            ));
        }
        Ok(Some(batch))
    }
}

/// Validate all allocation-bearing IPC lengths before Arrow's stream reader
/// allocates a declared message body. CRC protects accidental corruption, not
/// the safety of lengths in a frame with a recomputed checksum.
fn validate_ipc(
    payload: &[u8],
    rows: usize,
    max_fields: usize,
    schema_limit: usize,
) -> Result<(), GfError> {
    let invalid = || storage("invalid property scratch IPC lengths");
    let mut offset = 0_usize;
    let mut batches = 0;
    let mut schema_seen = false;
    loop {
        let prefix = payload
            .get(offset..offset.checked_add(4).ok_or_else(invalid)?)
            .ok_or_else(invalid)?;
        let mut size = i32::from_le_bytes(prefix.try_into().expect("four bytes"));
        offset += 4;
        if size == -1 {
            let prefix = payload
                .get(offset..offset.checked_add(4).ok_or_else(invalid)?)
                .ok_or_else(invalid)?;
            size = i32::from_le_bytes(prefix.try_into().expect("four bytes"));
            offset += 4;
        }
        if size == 0 {
            return if offset == payload.len() && batches == 1 {
                Ok(())
            } else {
                Err(invalid())
            };
        }
        let end = offset
            .checked_add(usize::try_from(size).map_err(|_| invalid())?)
            .ok_or_else(invalid)?;
        let message = arrow::ipc::root_as_message(payload.get(offset..end).ok_or_else(invalid)?)
            .map_err(storage)?;
        offset = end;
        let body_size = usize::try_from(message.bodyLength()).map_err(|_| invalid())?;
        offset = offset.checked_add(body_size).ok_or_else(invalid)?;
        if offset > payload.len() {
            return Err(invalid());
        }
        match message.header_type() {
            arrow::ipc::MessageHeader::Schema => {
                if schema_seen || batches != 0 || body_size != 0 {
                    return Err(invalid());
                }
                schema_seen = true;
                let schema = message.header_as_schema().ok_or_else(invalid)?;
                if schema
                    .fields()
                    .is_some_and(|fields| fields.len() > max_fields)
                {
                    return Err(invalid());
                }
                let mut schema_bytes = 0;
                if let Some(metadata) = schema.custom_metadata() {
                    charge_metadata(metadata, &mut schema_bytes, schema_limit)?;
                }
                if let Some(fields) = schema.fields() {
                    for field in fields {
                        charge_field(field, &mut schema_bytes, schema_limit, 0)?;
                    }
                }
            }
            arrow::ipc::MessageHeader::RecordBatch => {
                if !schema_seen || batches != 0 {
                    return Err(invalid());
                }
                let batch = message.header_as_record_batch().ok_or_else(invalid)?;
                if usize::try_from(batch.length()).map_err(|_| invalid())? != rows {
                    return Err(invalid());
                }
                validate_record(batch, body_size, payload.len())?;
                batches += 1;
            }
            _ => return Err(invalid()),
        }
    }
}

fn validate_record(
    batch: arrow::ipc::RecordBatch<'_>,
    body: usize,
    frame: usize,
) -> Result<(), GfError> {
    let invalid = || storage("invalid property scratch IPC lengths");
    if batch.compression().is_some() || batch.length() < 0 {
        return Err(invalid());
    }
    let mut total_buffers = 0_usize;
    if let Some(buffers) = batch.buffers() {
        for buffer in buffers {
            let offset = usize::try_from(buffer.offset()).map_err(|_| invalid())?;
            let size = usize::try_from(buffer.length()).map_err(|_| invalid())?;
            total_buffers = total_buffers.saturating_add(size);
            if total_buffers > body || offset.checked_add(size).is_none_or(|end| end > body) {
                return Err(invalid());
            }
        }
    }
    if let Some(nodes) = batch.nodes() {
        for node in nodes {
            if node.length() < 0
                || node.null_count() < 0
                || node.null_count() > node.length()
                || u64::try_from(node.length()).map_err(|_| invalid())?
                    > (frame as u64).saturating_mul(8)
            {
                return Err(invalid());
            }
        }
    }
    if let Some(counts) = batch.variadicBufferCounts() {
        let buffers = batch.buffers().map_or(0, |buffers| buffers.len());
        if counts
            .iter()
            .any(|count| usize::try_from(count).map_or(true, |count| count > buffers))
        {
            return Err(invalid());
        }
    }
    Ok(())
}

fn charge_metadata<'a>(
    metadata: impl IntoIterator<Item = arrow::ipc::KeyValue<'a>>,
    bytes: &mut usize,
    limit: usize,
) -> Result<(), GfError> {
    for entry in metadata {
        *bytes = bytes
            .saturating_add(entry.key().map_or(0, str::len))
            .saturating_add(entry.value().map_or(0, str::len))
            .saturating_add(128);
        if *bytes > limit {
            return Err(storage("property scratch schema exceeds its reservation"));
        }
    }
    Ok(())
}

fn charge_field(
    field: arrow::ipc::Field<'_>,
    bytes: &mut usize,
    limit: usize,
    depth: usize,
) -> Result<(), GfError> {
    *bytes = bytes
        .saturating_add(field.name().map_or(0, str::len))
        .saturating_add(128);
    if let Some(timestamp) = field.type_as_timestamp() {
        *bytes = bytes.saturating_add(timestamp.timezone().map_or(0, str::len));
    }
    if let Some(union) = field.type_as_union() {
        *bytes = bytes.saturating_add(union.typeIds().map_or(0, |ids| ids.len()).saturating_mul(4));
    }

    if *bytes > limit || depth > 64 {
        return Err(storage("property scratch schema exceeds its reservation"));
    }
    if let Some(metadata) = field.custom_metadata() {
        charge_metadata(metadata, bytes, limit)?;
    }
    if let Some(children) = field.children() {
        for child in children {
            charge_field(child, bytes, limit, depth + 1)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, ArrayRef, FixedSizeBinaryArray, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn batch(start: u64, count: usize) -> RecordBatch {
        let ids = (start..start + count as u64)
            .rev()
            .map(|id| {
                let mut bytes = [0; 16];
                bytes[8..].copy_from_slice(&id.to_be_bytes());
                bytes
            })
            .collect::<Vec<_>>();
        RecordBatch::try_new(
            std::sync::Arc::new(Schema::new(vec![
                Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
                Field::new("label", DataType::Utf8, false),
                Field::new("value", DataType::Int64, true),
            ])),
            vec![
                std::sync::Arc::new(
                    FixedSizeBinaryArray::try_from_iter(ids.iter().map(|id| id.as_slice()))
                        .unwrap(),
                ) as ArrayRef,
                std::sync::Arc::new(StringArray::from(vec!["Person"; count])),
                std::sync::Arc::new(Int64Array::from(
                    (0..count)
                        .map(|i| (i % 3 != 0).then_some(i as i64))
                        .collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    #[test]
    fn scratch_traffic_matches_file_lengths_including_repeated_scans() {
        let root = tempfile::tempdir().unwrap();
        let directory = super::super::StableDirectory::open(root.path()).unwrap();
        let scratch = Scratch::create(&directory).unwrap();
        let rows = PropertyRows::new(
            &scratch,
            ConstructionChunkKind::Node,
            GraphConstructionBudgets::default(),
            0,
        );
        let path = rows.path().unwrap();
        let before_write = rows.written_bytes();
        rows.write(&path, &batch(0, 3)).unwrap();
        rows.write(&path, &batch(3, 4)).unwrap();
        let file_bytes = std::fs::metadata(&path).unwrap().len();
        assert_eq!(rows.written_bytes() - before_write, file_bytes);
        let before_read = rows.read_bytes();
        for _ in 0..2 {
            let mut reader = rows.reader(&path).unwrap();
            let mut count = 0;
            while let Some(batch) = reader.next().unwrap() {
                count += batch.num_rows();
            }
            assert_eq!(count, 7);
        }
        assert_eq!(rows.read_bytes() - before_read, 2 * file_bytes);
    }

    #[test]
    fn binary_runs_sort_globally_without_retaining_one_run_per_batch() {
        let root = tempfile::tempdir().unwrap();
        let directory = super::super::StableDirectory::open(root.path()).unwrap();
        let scratch = Scratch::create(&directory).unwrap();
        let rows = PropertyRows::new(
            &scratch,
            ConstructionChunkKind::Node,
            GraphConstructionBudgets::default(),
            0,
        );
        let cancel = AtomicBool::new(false);
        for index in (0..65).rev() {
            rows.ingest(&batch(index * 32, 32), &cancel).unwrap();
        }
        assert!(
            rows.groups
                .lock()
                .unwrap()
                .values()
                .all(|group| group.levels.len() <= 7)
        );
        let groups = rows.finish(&cancel).unwrap();
        assert_eq!(groups.len(), 1);
        let mut reader = rows.reader(&groups[0].path).unwrap();
        let mut expected = 0_u64;
        while let Some(batch) = reader.next().unwrap() {
            let uuids = crate::graph_construction::batch_uuid_column(&batch, "node_uuid").unwrap();
            for row in 0..batch.num_rows() {
                assert_eq!(&uuids.value(row)[8..], &expected.to_be_bytes());
                expected += 1;
            }
        }
        assert_eq!(expected, 65 * 32);
        assert!(rows.written_bytes() > 0 && rows.read_bytes() > 0);
    }

    #[test]
    fn ipc_type_metadata_is_charged_before_schema_instantiation() {
        let root = tempfile::tempdir().unwrap();
        let directory = super::super::StableDirectory::open(root.path()).unwrap();
        let scratch = Scratch::create(&directory).unwrap();
        let rows = PropertyRows::new(
            &scratch,
            ConstructionChunkKind::Node,
            GraphConstructionBudgets::default(),
            0,
        );
        let zone = std::iter::repeat_n('x', 2 << 20).collect::<String>();
        let temporal = arrow::array::TimestampNanosecondArray::from(vec![0; 3]).with_timezone(zone);
        let schema = std::sync::Arc::new(Schema::new(vec![Field::new(
            "when",
            temporal.data_type().clone(),
            true,
        )]));
        let input = RecordBatch::try_new(schema, vec![std::sync::Arc::new(temporal)]).unwrap();
        let path = rows.path().unwrap();
        rows.write(&path, &input).unwrap();
        assert!(
            rows.reader(&path)
                .unwrap()
                .next()
                .unwrap_err()
                .to_string()
                .contains("schema exceeds")
        );
    }

    #[test]
    fn frames_reject_crc_corruption_truncation_and_unbounded_ipc_bodies() {
        let root = tempfile::tempdir().unwrap();
        let directory = super::super::StableDirectory::open(root.path()).unwrap();
        let scratch = Scratch::create(&directory).unwrap();
        let rows = PropertyRows::new(
            &scratch,
            ConstructionChunkKind::Node,
            GraphConstructionBudgets::default(),
            0,
        );
        let path = rows.path().unwrap();
        rows.write(&path, &batch(0, 3)).unwrap();
        let original = std::fs::read(&path).unwrap();
        let mut corrupt = original.clone();
        *corrupt.last_mut().unwrap() ^= 1;
        std::fs::write(&path, corrupt).unwrap();
        assert!(
            rows.reader(&path)
                .unwrap()
                .next()
                .unwrap_err()
                .to_string()
                .contains("CRC32C")
        );
        std::fs::write(&path, &original[..original.len() - 1]).unwrap();
        assert!(
            rows.reader(&path)
                .unwrap()
                .next()
                .unwrap_err()
                .to_string()
                .contains("truncated")
        );
        let mut corrupt = original.clone();
        let payload = &mut corrupt[HEADER..];
        let schema_size = i32::from_le_bytes(payload[4..8].try_into().unwrap()) as usize;
        let record_start = 8 + schema_size;
        let record_size = i32::from_le_bytes(
            payload[record_start + 4..record_start + 8]
                .try_into()
                .unwrap(),
        ) as usize;
        let metadata_start = record_start + 8;
        let message =
            arrow::ipc::root_as_message(&payload[metadata_start..metadata_start + record_size])
                .unwrap();
        let length = metadata_start
            + message._tab.loc()
            + usize::from(
                message
                    ._tab
                    .vtable()
                    .get(arrow::ipc::Message::VT_BODYLENGTH),
            );
        payload[length..length + 8].copy_from_slice(&i64::MAX.to_le_bytes());
        let crc = crc32c(payload);
        corrupt[8..12].copy_from_slice(&crc.to_le_bytes());
        std::fs::write(&path, corrupt).unwrap();
        assert!(
            rows.reader(&path)
                .unwrap()
                .next()
                .unwrap_err()
                .to_string()
                .contains("IPC lengths")
        );

        let mut corrupt = original.clone();
        let payload = &mut corrupt[HEADER..];
        let message =
            arrow::ipc::root_as_message(&payload[metadata_start..metadata_start + record_size])
                .unwrap();
        let batch = message.header_as_record_batch().unwrap();
        let slot = metadata_start
            + batch._tab.loc()
            + usize::from(batch._tab.vtable().get(arrow::ipc::RecordBatch::VT_BUFFERS));
        let vector =
            slot + u32::from_le_bytes(payload[slot..slot + 4].try_into().unwrap()) as usize;
        payload[vector + 4 + 8..vector + 4 + 16].copy_from_slice(&i64::MAX.to_le_bytes());
        let crc = crc32c(payload);
        corrupt[8..12].copy_from_slice(&crc.to_le_bytes());
        std::fs::write(&path, corrupt).unwrap();
        assert!(
            rows.reader(&path)
                .unwrap()
                .next()
                .unwrap_err()
                .to_string()
                .contains("IPC lengths")
        );
    }
}
