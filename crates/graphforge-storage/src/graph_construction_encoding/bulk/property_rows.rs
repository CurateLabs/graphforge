//! Exact-schema property rows on disposable, CRC-protected Arrow IPC runs.
//!
//! Property rows must reach the overlay writer in identity order, grouped by
//! exact schema, while the decoded input is bounded (#1916). The route is an
//! external merge sort sized from the memory budget (#1938):
//!
//! 1. Each worker retains the batches of its current task, per schema group,
//!    until a run's worth of bytes (`PropertySizing::run_bytes`) is held, then
//!    sorts them once by identity and writes one run. A run is written once.
//! 2. A group with more runs than the merge fan-in is reduced by merging only
//!    its smallest runs, in parallel, so every byte is rewritten at most as
//!    often as the run count requires and not once per binary level.
//! 3. The remaining runs merge in a single pass into identity-range segments,
//!    one task per range, so the segments in range order are the sorted group.
//!
//! A schema without property columns writes no overlay and, in the catalog,
//! only says which owners it holds and how often. Such a group keeps a small
//! summary of its owners instead of its rows (#1938).
//!
//! The retained bytes of all workers share one gate, so concurrent intake can
//! never hold more than the budget derived for it, however many workers run.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::{
    Array, BinaryArray, LargeBinaryArray, LargeStringArray, StringArray, UInt32Array,
};
use arrow::compute::interleave_record_batch;
use arrow::datatypes::DataType;
use arrow::ipc::{reader::StreamReader, writer::StreamWriter};
use arrow::record_batch::RecordBatch;

use super::gate::ByteGate;
use super::scratch::{Scratch, crc32c};
use super::tables::check_cancelled;
use super::{
    AtomicBool, ConstructionChunkKind, GfError, GraphConstructionBudgets, required_string, storage,
};

pub(super) use super::property_merge::SortedGroup;

const DEFAULT_FRAME_BYTES: usize = 1 << 20;
/// Bytes one retained row adds while its run is sorted: the 16-byte identity,
/// two ordinals, and the sort's own working copy.
const KEY_BYTES: usize = 32;
#[cfg(test)]
#[path = "property_rows_test_support.rs"]
mod test_support;
#[cfg(test)]
use test_support::{FORCED_FRAME_BYTES, FORCED_SIZING};
#[cfg(test)]
pub(crate) use test_support::{ForcedPropertyFrames, ForcedPropertySizing};

pub(super) const HEADER: usize = 16;
/// Columns every scratch row leads with: the identity and the owner (a node's
/// label, an edge's relation type). The properties follow.
pub(super) const REQUIRED_COLUMNS: usize = 2;

/// How the budget divides among run formation and merging.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct PropertySizing {
    /// Bytes one worker retains before it sorts and writes a run.
    pub(super) run_bytes: usize,
    /// Bytes every worker together may retain, including sort keys.
    pub(super) retained_bytes: u64,
    /// Runs one merge holds open.
    pub(super) fan_in: usize,
    /// Target size of one scratch frame.
    pub(super) frame_bytes: usize,
}

impl PropertySizing {
    /// The sizing of a build that runs one worker.
    pub(super) const SERIAL: Self = Self {
        run_bytes: 64 << 20,
        retained_bytes: 128 << 20,
        fan_in: 16,
        frame_bytes: DEFAULT_FRAME_BYTES,
    };
}

/// One frame of a run: where it lies and the identities that bound it.
#[derive(Clone, Copy, Debug)]
pub(super) struct FrameMeta {
    pub(super) offset: u64,
    pub(super) bytes: u64,
    pub(super) rows: u32,
    pub(super) first: [u8; 16],
    pub(super) last: [u8; 16],
}

/// A file of frames whose rows are in identity order.
#[derive(Debug)]
pub(super) struct Run {
    pub(super) path: PathBuf,
    pub(super) frames: Vec<FrameMeta>,
    pub(super) rows: u64,
}

impl Run {
    pub(super) fn bytes(&self) -> u64 {
        self.frames.iter().map(|frame| frame.bytes).sum()
    }
}

pub(super) struct PropertyRows<'a> {
    scratch: &'a Scratch,
    kind: ConstructionChunkKind,
    pub(super) budgets: GraphConstructionBudgets,
    schema_bytes: usize,
    pub(super) frame_target: usize,
    pub(super) sizing: PropertySizing,
    pub(super) gate: ByteGate,
    pub(super) groups: Mutex<Groups>,
    next_file: AtomicU64,
    written: AtomicU64,
    read: AtomicU64,
    runs_formed: AtomicU64,
    merge_inputs_peak: AtomicU64,
}

/// For each owner of a group without properties: its smallest identity and
/// its row count.
pub(super) type BareOwners = BTreeMap<String, ([u8; 16], u64)>;

/// The schema groups of one kind: sorted runs for those with properties, a
/// summary of owners for those without.
#[derive(Default)]
pub(super) struct Groups {
    pub(super) runs: BTreeMap<String, Vec<Run>>,
    pub(super) bare: BTreeMap<String, BareOwners>,
}

/// Pending batches of one schema group in a worker's current run.
#[derive(Default)]
struct Pending {
    batches: Vec<RecordBatch>,
}

/// One worker's view of the intake: batches wait here, per schema group, until
/// a run's worth is held. Dropping it returns whatever it still holds.
pub(super) struct RunSink<'r, 'a> {
    rows: &'r PropertyRows<'a>,
    pending: BTreeMap<String, Pending>,
    bare: BTreeMap<String, BareOwners>,
    held: u64,
}

impl RunSink<'_, '_> {
    /// Retain `batch` for the next run of its schema group.
    pub(super) fn push(&mut self, batch: &RecordBatch, cancel: &AtomicBool) -> Result<(), GfError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let digest = crate::graph_construction::normalized_schema_digest(batch.schema().as_ref());
        if !self.pending.contains_key(&digest)
            && !self.bare.contains_key(&digest)
            && self.pending.len() + self.bare.len() >= self.rows.budgets.max_schema_groups
        {
            return Err(storage("construction schema-group budget exhausted"));
        }
        if batch.num_columns() == self.rows.source_required() {
            return self.note_bare(digest, batch);
        }
        let sizing = self.rows.sizing;
        let need = (batch.get_array_memory_size() as u64)
            .saturating_add((batch.num_rows() * KEY_BYTES) as u64)
            .min(sizing.retained_bytes);
        // A run is as large as the worker's share allows, never smaller than
        // one batch.
        if self.held > 0 && self.held.saturating_add(need) > sizing.run_bytes as u64 {
            self.flush(cancel)?;
        }
        if !self.rows.gate.try_acquire(need)? {
            // Others hold the pool. Return this worker's share first, so the
            // wait below can never be for bytes this worker itself holds.
            self.flush(cancel)?;
            self.rows.gate.acquire(need, cancel)?;
        }
        self.held += need;
        // Grouped by the schema the source stated; stored without an edge's
        // endpoints, which the edge records already carry.
        let kept = self.rows.scratch_columns(batch)?;
        self.pending.entry(digest).or_default().batches.push(kept);
        Ok(())
    }

    /// Count the owners of a batch that has no property columns.
    fn note_bare(&mut self, digest: String, batch: &RecordBatch) -> Result<(), GfError> {
        let owners = required_string(batch, self.rows.owner_name())?;
        let uuids = crate::graph_construction::batch_uuid_column(batch, self.rows.uuid_name())?;
        let seen = self.bare.entry(digest).or_default();
        let mut row = 0;
        while row < batch.num_rows() {
            let owner = owners.value(row);
            let mut end = row + 1;
            while end < batch.num_rows() && owners.value(end) == owner {
                end += 1;
            }
            let smallest = (row..end)
                .map(|at| <[u8; 16]>::try_from(uuids.value(at)).map_err(storage))
                .try_fold(None::<[u8; 16]>, |smallest, uuid| {
                    uuid.map(|uuid| Some(smallest.map_or(uuid, |smallest| smallest.min(uuid))))
                })?
                .expect("a nonempty range");
            let count = (end - row) as u64;
            if let Some(entry) = seen.get_mut(owner) {
                entry.0 = entry.0.min(smallest);
                entry.1 += count;
            } else {
                seen.insert(owner.to_owned(), (smallest, count));
            }
            row = end;
        }
        Ok(())
    }

    /// Sort and write every pending group as one run each.
    fn flush(&mut self, cancel: &AtomicBool) -> Result<(), GfError> {
        self.rows.add_bare(std::mem::take(&mut self.bare))?;
        for (digest, pending) in std::mem::take(&mut self.pending) {
            check_cancelled(cancel)?;
            let run = self.rows.write_run(&pending.batches, cancel)?;
            drop(pending);
            self.rows.add_run(digest, run)?;
        }
        self.rows.gate.release(self.held);
        self.held = 0;
        Ok(())
    }

    /// Write whatever is still retained.
    pub(super) fn finish(mut self, cancel: &AtomicBool) -> Result<(), GfError> {
        self.flush(cancel)
    }
}

impl Drop for RunSink<'_, '_> {
    fn drop(&mut self) {
        // An abandoned sink (an error elsewhere) must not strand the pool.
        if self.held > 0 {
            self.rows.gate.release(self.held);
            self.held = 0;
        }
    }
}

/// Appends frames to one run file and records where each lies.
pub(super) struct RunWriter<'p, 'a> {
    rows: &'p PropertyRows<'a>,
    path: PathBuf,
    file: std::io::BufWriter<File>,
    offset: u64,
    frames: Vec<FrameMeta>,
    total: u64,
}

impl RunWriter<'_, '_> {
    pub(super) fn append(
        &mut self,
        batch: &RecordBatch,
        first: [u8; 16],
        last: [u8; 16],
    ) -> Result<(), GfError> {
        let frame = self.rows.encode_frame(batch)?;
        self.file.write_all(&frame).map_err(storage)?;
        self.rows
            .written
            .fetch_add(frame.len() as u64, Ordering::Relaxed);
        let rows = u32::try_from(batch.num_rows()).map_err(storage)?;
        self.frames.push(FrameMeta {
            offset: self.offset,
            bytes: frame.len() as u64,
            rows,
            first,
            last,
        });
        self.offset += frame.len() as u64;
        self.total += u64::from(rows);
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<Run, GfError> {
        self.file.flush().map_err(storage)?;
        Ok(Run {
            path: self.path,
            frames: self.frames,
            rows: self.total,
        })
    }
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
            batch = arrow::compute::concat_batches(&batch.schema(), [&previous, &batch])
                .map_err(storage)?;
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
        arrow::compute::concat_batches(&first.schema(), &batches)
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
        sizing: PropertySizing,
    ) -> Self {
        #[cfg(test)]
        let sizing =
            FORCED_SIZING
                .with(std::cell::Cell::get)
                .map_or(sizing, |(run_bytes, fan_in)| PropertySizing {
                    run_bytes,
                    retained_bytes: sizing.retained_bytes.max(run_bytes as u64),
                    fan_in,
                    ..sizing
                });
        let frame_target = sizing.frame_bytes.max(1);
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
            sizing,
            gate: ByteGate::new(sizing.retained_bytes),
            groups: Mutex::new(Groups::default()),
            next_file: AtomicU64::new(0),
            written: AtomicU64::new(0),
            read: AtomicU64::new(0),
            runs_formed: AtomicU64::new(0),
            merge_inputs_peak: AtomicU64::new(0),
        }
    }

    /// Columns a source batch of this kind leads with, before its properties.
    fn source_required(&self) -> usize {
        match self.kind {
            ConstructionChunkKind::Node => 2,
            ConstructionChunkKind::Edge => 4,
        }
    }

    /// The column that names the owner of a row's properties.
    pub(super) fn owner_name(&self) -> &'static str {
        match self.kind {
            ConstructionChunkKind::Node => "label",
            ConstructionChunkKind::Edge => "rel_type",
        }
    }

    pub(super) fn uuid_name(&self) -> &'static str {
        match self.kind {
            ConstructionChunkKind::Node => "node_uuid",
            ConstructionChunkKind::Edge => "edge_uuid",
        }
    }

    /// `batch` as scratch rows keep it: identity, owner, properties.
    fn scratch_columns(&self, batch: &RecordBatch) -> Result<RecordBatch, GfError> {
        if self.source_required() == REQUIRED_COLUMNS {
            return Ok(batch.clone());
        }
        let columns = (0..REQUIRED_COLUMNS)
            .chain(self.source_required()..batch.num_columns())
            .collect::<Vec<_>>();
        batch.project(&columns).map_err(storage)
    }

    /// A worker's intake. Create one per task and `finish` it.
    pub(super) fn sink(&self) -> RunSink<'_, 'a> {
        RunSink {
            rows: self,
            pending: BTreeMap::new(),
            bare: BTreeMap::new(),
            held: 0,
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
    /// The most bytes concurrent intake held at once.
    pub(super) fn peak_retained_bytes(&self) -> u64 {
        self.gate.peak()
    }

    /// The most runs any one merge held open.
    pub(super) fn merge_inputs_peak(&self) -> u64 {
        self.merge_inputs_peak.load(Ordering::Relaxed)
    }

    pub(super) fn note_merge_inputs(&self, inputs: usize) {
        self.merge_inputs_peak
            .fetch_max(inputs as u64, Ordering::Relaxed);
    }

    /// Sorted runs formed from the input.
    pub(super) fn runs_formed(&self) -> u64 {
        self.runs_formed.load(Ordering::Relaxed)
    }

    fn add_run(&self, digest: String, run: Run) -> Result<(), GfError> {
        let mut groups = self
            .groups
            .lock()
            .map_err(|_| storage("property schema lock poisoned"))?;
        if !groups.runs.contains_key(&digest)
            && !groups.bare.contains_key(&digest)
            && groups.runs.len() + groups.bare.len() >= self.budgets.max_schema_groups
        {
            return Err(storage("construction schema-group budget exhausted"));
        }
        groups.runs.entry(digest).or_default().push(run);
        self.runs_formed.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Fold a worker's owner summaries into the groups.
    fn add_bare(&self, seen: BTreeMap<String, BareOwners>) -> Result<(), GfError> {
        if seen.is_empty() {
            return Ok(());
        }
        let mut groups = self
            .groups
            .lock()
            .map_err(|_| storage("property schema lock poisoned"))?;
        for (digest, owners) in seen {
            if !groups.runs.contains_key(&digest)
                && !groups.bare.contains_key(&digest)
                && groups.runs.len() + groups.bare.len() >= self.budgets.max_schema_groups
            {
                return Err(storage("construction schema-group budget exhausted"));
            }
            let total = groups.bare.entry(digest).or_default();
            for (owner, (smallest, count)) in owners {
                let entry = total.entry(owner).or_insert((smallest, 0));
                entry.0 = entry.0.min(smallest);
                entry.1 += count;
            }
        }
        Ok(())
    }

    /// A new run file for the merge or the sink to fill.
    pub(super) fn run_writer(&self) -> Result<RunWriter<'_, 'a>, GfError> {
        let path = self.path()?;
        let file = OpenOptions::new()
            .write(true)
            .open(&path)
            .map_err(storage)?;
        Ok(RunWriter {
            rows: self,
            path,
            file: std::io::BufWriter::with_capacity(1 << 20, file),
            offset: 0,
            frames: Vec::new(),
            total: 0,
        })
    }

    /// Sort `batches` (one schema group) by identity and write them as one run.
    fn write_run(&self, batches: &[RecordBatch], cancel: &AtomicBool) -> Result<Run, GfError> {
        let uuid_name = self.uuid_name();
        let total_rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        let mut keys = Vec::with_capacity(total_rows);
        for (ordinal, batch) in batches.iter().enumerate() {
            let uuids = crate::graph_construction::batch_uuid_column(batch, uuid_name)?;
            let ordinal = u32::try_from(ordinal).map_err(storage)?;
            for row in 0..batch.num_rows() {
                keys.push((
                    <[u8; 16]>::try_from(uuids.value(row)).map_err(storage)?,
                    ordinal,
                    u32::try_from(row).map_err(storage)?,
                ));
            }
        }
        keys.sort_unstable();
        let refs = batches.iter().collect::<Vec<_>>();
        let max_rows = self.budgets.max_batch_rows;
        let mut writer = self.run_writer()?;
        let mut indices = Vec::with_capacity(max_rows);
        let mut chunk_bytes = 0_usize;
        let mut chunk_first = None;
        let mut chunk_last = None;
        for key in &keys {
            check_cancelled(cancel)?;
            let batch = &batches[key.1 as usize];
            let row = key.2 as usize;
            let row_bytes = Self::row_bytes(batch, row)?;
            if indices.is_empty()
                && row_bytes > self.frame_target
                && batch.get_array_memory_size() > self.budgets.max_batch_bytes
            {
                return Err(storage(
                    "wide property row exceeds its validated source batch window",
                ));
            }
            let next_bytes = chunk_bytes
                .checked_add(row_bytes)
                .ok_or_else(|| storage("property frame byte total overflows"))?;
            if !indices.is_empty() && (indices.len() >= max_rows || next_bytes > self.frame_target)
            {
                let frame = interleave_record_batch(&refs, &indices).map_err(storage)?;
                writer.append(
                    &frame,
                    chunk_first.expect("a nonempty property frame"),
                    chunk_last.expect("a nonempty property frame"),
                )?;
                crate::graph_construction::construction_failpoint("bulk.during_property_run");
                indices.clear();
                chunk_bytes = 0;
                chunk_first = None;
            }
            indices.push((key.1 as usize, row));
            chunk_bytes = chunk_bytes
                .checked_add(row_bytes)
                .ok_or_else(|| storage("property frame byte total overflows"))?;
            chunk_first.get_or_insert(key.0);
            chunk_last = Some(key.0);
        }
        if !indices.is_empty() {
            let frame = interleave_record_batch(&refs, &indices).map_err(storage)?;
            writer.append(
                &frame,
                chunk_first.expect("a nonempty property frame"),
                chunk_last.expect("a nonempty property frame"),
            )?;
            crate::graph_construction::construction_failpoint("bulk.during_property_run");
        }
        writer.finish()
    }

    /// Logical Arrow bytes contributed by one row, including nested children,
    /// variable-width offsets, and validity. Computing this before interleave
    /// keeps skewed keys from materializing an over-budget output frame.
    pub(super) fn row_bytes(batch: &RecordBatch, row: usize) -> Result<usize, GfError> {
        batch.columns().iter().try_fold(0_usize, |total, column| {
            let bytes = Self::column_row_bytes(column.as_ref(), row)?;
            total
                .checked_add(bytes)
                .ok_or_else(|| storage("property row byte total overflows"))
        })
    }

    fn column_row_bytes(array: &dyn Array, row: usize) -> Result<usize, GfError> {
        let data_type = array.data_type();
        let fast = match data_type {
            DataType::FixedSizeBinary(width) => usize::try_from(*width).ok(),
            DataType::Boolean => Some(1),
            DataType::Utf8 => array
                .as_any()
                .downcast_ref::<StringArray>()
                .and_then(|values| usize::try_from(values.value_length(row)).ok())
                .and_then(|length| length.checked_add(4)),
            DataType::LargeUtf8 => array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .and_then(|values| usize::try_from(values.value_length(row)).ok())
                .map(|length| length + 8),
            DataType::Binary => array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .and_then(|values| usize::try_from(values.value_length(row)).ok())
                .and_then(|length| length.checked_add(4)),
            DataType::LargeBinary => array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .and_then(|values| usize::try_from(values.value_length(row)).ok())
                .map(|length| length + 8),
            data_type if data_type.primitive_width().is_some() => data_type.primitive_width(),
            _ => None,
        };
        if let Some(bytes) = fast {
            // Charge a byte for every nullable row. A non-null one-row slice
            // drops its validity buffer, but a multirow output may retain it.
            return bytes
                .checked_add(usize::from(array.nulls().is_some()))
                .ok_or_else(|| storage("property row byte total overflows"));
        }
        Self::array_data_row_bytes(&array.to_data(), row)
    }

    fn array_data_row_bytes(data: &arrow::array::ArrayData, row: usize) -> Result<usize, GfError> {
        let data_type = data.data_type();
        let offset = data
            .offset()
            .checked_add(row)
            .ok_or_else(|| storage("property row offset overflows"))?;
        let value_bytes = match data_type {
            DataType::Utf8 | DataType::Binary => Self::variable_value_bytes(data, row, false)?,
            DataType::LargeUtf8 | DataType::LargeBinary => {
                Self::variable_value_bytes(data, row, true)?
            }
            DataType::List(_) | DataType::Map(_, _) => Self::list_row_bytes(data, row, false)?,
            DataType::LargeList(_) => Self::list_row_bytes(data, row, true)?,
            DataType::ListView(_) => Self::list_view_row_bytes(data, row, false)?,
            DataType::LargeListView(_) => Self::list_view_row_bytes(data, row, true)?,
            DataType::FixedSizeList(_, width) => Self::fixed_list_row_bytes(data, offset, *width)?,
            DataType::Struct(_) => Self::struct_row_bytes(data, offset)?,
            DataType::Dictionary(key_type, _) => Self::dictionary_row_bytes(data, key_type, row)?,
            DataType::FixedSizeBinary(width) => usize::try_from(*width).map_err(storage)?,
            DataType::Boolean => 1,
            data_type if data_type.primitive_width().is_some() => data_type
                .primitive_width()
                .expect("checked primitive width"),
            _ => data
                .slice(row, 1)
                .get_slice_memory_size()
                .map_err(storage)?,
        };
        value_bytes
            .checked_add(usize::from(data.nulls().is_some()))
            .ok_or_else(|| storage("property row byte total overflows"))
    }

    fn variable_value_bytes(
        data: &arrow::array::ArrayData,
        row: usize,
        large: bool,
    ) -> Result<usize, GfError> {
        let (start, end, width) = if large {
            let offsets = data.buffer::<i64>(0);
            (
                usize::try_from(offsets[row]).map_err(storage)?,
                usize::try_from(offsets[row + 1]).map_err(storage)?,
                8,
            )
        } else {
            let offsets = data.buffer::<i32>(0);
            (
                usize::try_from(offsets[row]).map_err(storage)?,
                usize::try_from(offsets[row + 1]).map_err(storage)?,
                4,
            )
        };
        end.checked_sub(start)
            .and_then(|bytes| bytes.checked_add(width))
            .ok_or_else(|| storage("property value range is invalid"))
    }

    fn list_row_bytes(
        data: &arrow::array::ArrayData,
        row: usize,
        large: bool,
    ) -> Result<usize, GfError> {
        let (start, end, width) = if large {
            let offsets = data.buffer::<i64>(0);
            (
                usize::try_from(offsets[row]).map_err(storage)?,
                usize::try_from(offsets[row + 1]).map_err(storage)?,
                8,
            )
        } else {
            let offsets = data.buffer::<i32>(0);
            (
                usize::try_from(offsets[row]).map_err(storage)?,
                usize::try_from(offsets[row + 1]).map_err(storage)?,
                4,
            )
        };
        let len = end
            .checked_sub(start)
            .ok_or_else(|| storage("property list range is invalid"))?;
        let child = data
            .child_data()
            .first()
            .ok_or_else(|| storage("property list has no child data"))?;
        Self::array_range_bytes(child, start, len)?
            .checked_add(width)
            .ok_or_else(|| storage("property row byte total overflows"))
    }

    fn list_view_row_bytes(
        data: &arrow::array::ArrayData,
        row: usize,
        large: bool,
    ) -> Result<usize, GfError> {
        let (start, len, width) = if large {
            let offsets = data.buffer::<i64>(0);
            let sizes = data.buffer::<i64>(1);
            (
                usize::try_from(offsets[row]).map_err(storage)?,
                usize::try_from(sizes[row]).map_err(storage)?,
                16,
            )
        } else {
            let offsets = data.buffer::<i32>(0);
            let sizes = data.buffer::<i32>(1);
            (
                usize::try_from(offsets[row]).map_err(storage)?,
                usize::try_from(sizes[row]).map_err(storage)?,
                8,
            )
        };
        let child = data
            .child_data()
            .first()
            .ok_or_else(|| storage("property list view has no child data"))?;
        Self::array_range_bytes(child, start, len)?
            .checked_add(width)
            .ok_or_else(|| storage("property row byte total overflows"))
    }

    fn fixed_list_row_bytes(
        data: &arrow::array::ArrayData,
        offset: usize,
        width: i32,
    ) -> Result<usize, GfError> {
        let width = usize::try_from(width).map_err(storage)?;
        let start = offset
            .checked_mul(width)
            .ok_or_else(|| storage("property list offset overflows"))?;
        let child = data
            .child_data()
            .first()
            .ok_or_else(|| storage("property list has no child data"))?;
        Self::array_range_bytes(child, start, width)
    }

    fn struct_row_bytes(data: &arrow::array::ArrayData, offset: usize) -> Result<usize, GfError> {
        data.child_data().iter().try_fold(0_usize, |total, child| {
            total
                .checked_add(Self::array_data_row_bytes(child, offset)?)
                .ok_or_else(|| storage("property row byte total overflows"))
        })
    }

    fn dictionary_row_bytes(
        data: &arrow::array::ArrayData,
        key_type: &DataType,
        row: usize,
    ) -> Result<usize, GfError> {
        let key_bytes = key_type
            .primitive_width()
            .ok_or_else(|| storage("property dictionary key is not primitive"))?;
        if data.is_null(row) {
            return Ok(key_bytes);
        }
        let child = data
            .child_data()
            .first()
            .ok_or_else(|| storage("property dictionary has no values"))?;
        let key = Self::dictionary_index(data, key_type, row)?;
        key_bytes
            .checked_add(Self::array_data_row_bytes(child, key)?)
            .ok_or_else(|| storage("property row byte total overflows"))
    }

    fn dictionary_index(
        data: &arrow::array::ArrayData,
        key_type: &DataType,
        row: usize,
    ) -> Result<usize, GfError> {
        macro_rules! index {
            ($key:ty) => {
                usize::try_from(data.buffer::<$key>(0)[row]).map_err(storage)
            };
        }
        match key_type {
            DataType::Int8 => index!(i8),
            DataType::Int16 => index!(i16),
            DataType::Int32 => index!(i32),
            DataType::Int64 => index!(i64),
            DataType::UInt8 => index!(u8),
            DataType::UInt16 => index!(u16),
            DataType::UInt32 => index!(u32),
            DataType::UInt64 => index!(u64),
            _ => Err(storage("property dictionary key is not an integer")),
        }
    }

    fn array_range_bytes(
        data: &arrow::array::ArrayData,
        start: usize,
        len: usize,
    ) -> Result<usize, GfError> {
        let end = start
            .checked_add(len)
            .ok_or_else(|| storage("property child range overflows"))?;
        (start..end).try_fold(0_usize, |total, row| {
            total
                .checked_add(Self::array_data_row_bytes(data, row)?)
                .ok_or_else(|| storage("property row byte total overflows"))
        })
    }

    fn encode_frame(&self, batch: &RecordBatch) -> Result<Vec<u8>, GfError> {
        let mut frame = vec![0_u8; HEADER];
        {
            let mut writer = StreamWriter::try_new(&mut frame, &batch.schema()).map_err(storage)?;
            writer.write(batch).map_err(storage)?;
            writer.finish().map_err(storage)?;
        }
        let payload = frame.len() - HEADER;
        // This ceiling includes IPC alignment, offsets and schema metadata.
        // Refuse corrupt/unbounded frames before allocating on the read side.
        if payload > self.frame_limit() {
            return Err(storage(
                "property scratch frame exceeds its reserved workspace",
            ));
        }
        let crc = crc32c(&frame[HEADER..]);
        frame[..8].copy_from_slice(&(payload as u64).to_le_bytes());
        frame[8..12].copy_from_slice(&crc.to_le_bytes());
        frame[12..HEADER].copy_from_slice(
            &u32::try_from(batch.num_rows())
                .map_err(storage)?
                .to_le_bytes(),
        );
        Ok(frame)
    }

    /// Append `batch` to the frame file `path` (windows and projections).
    pub(super) fn write(&self, path: &Path, batch: &RecordBatch) -> Result<(), GfError> {
        let frame = self.encode_frame(batch)?;
        OpenOptions::new()
            .append(true)
            .open(path)
            .map_err(storage)?
            .write_all(&frame)
            .map_err(storage)?;
        self.written
            .fetch_add(frame.len() as u64, Ordering::Relaxed);
        Ok(())
    }

    pub(super) fn frame_limit(&self) -> usize {
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

    /// The rows of a sorted group, in identity order.
    pub(super) fn group_reader<'r>(&'r self, group: &'r SortedGroup) -> GroupReader<'r, 'a> {
        GroupReader {
            rows: self,
            segments: group.segments.iter(),
            current: None,
        }
    }
}

/// Reads a sorted group's segments one after another.
pub(super) struct GroupReader<'r, 'a> {
    rows: &'r PropertyRows<'a>,
    segments: std::slice::Iter<'r, Run>,
    current: Option<RowsReader<'r, 'a>>,
}

impl GroupReader<'_, '_> {
    pub(super) fn next(&mut self) -> Result<Option<RecordBatch>, GfError> {
        loop {
            if let Some(reader) = &mut self.current {
                if let Some(batch) = reader.next()? {
                    return Ok(Some(batch));
                }
                self.current = None;
            }
            let Some(segment) = self.segments.next() else {
                return Ok(None);
            };
            self.current = Some(self.rows.reader(&segment.path)?);
        }
    }
}

pub(super) struct RowsReader<'r, 's> {
    pub(super) rows: &'r PropertyRows<'s>,
    pub(super) file: File,
}

impl RowsReader<'_, '_> {
    /// Continue reading at the frame that starts `offset` bytes into the file.
    pub(super) fn seek(&mut self, offset: u64) -> Result<(), GfError> {
        self.file.seek(SeekFrom::Start(offset)).map_err(storage)?;
        Ok(())
    }

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
            self.rows.budgets.max_property_columns + REQUIRED_COLUMNS,
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
#[path = "property_rows_tests.rs"]
mod tests;
