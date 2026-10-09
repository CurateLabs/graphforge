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

use arrow::array::UInt32Array;
use arrow::compute::interleave_record_batch;
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
#[cfg(test)]
thread_local! { static FORCED_SIZING: std::cell::Cell<Option<(usize, usize)>> = const {std::cell::Cell::new(None)}; }
/// Forces the run size and merge fan-in of the builds the current test thread
/// runs, until dropped, so a small input spans many runs and merge levels.
#[cfg(test)]
pub(crate) struct ForcedPropertySizing;
#[cfg(test)]
impl ForcedPropertySizing {
    pub(crate) fn set(run_bytes: usize, fan_in: usize) -> Self {
        FORCED_SIZING.with(|forced| forced.set(Some((run_bytes.max(1), fan_in.max(2)))));
        Self
    }
}
#[cfg(test)]
impl Drop for ForcedPropertySizing {
    fn drop(&mut self) {
        FORCED_SIZING.with(|forced| forced.set(None));
    }
}

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

    /// Bytes of the batches held now, which the gate must have granted.
    #[cfg(test)]
    fn retained_bytes(&self) -> u64 {
        self.pending
            .values()
            .flat_map(|pending| &pending.batches)
            .map(|batch| (batch.get_array_memory_size() + batch.num_rows() * KEY_BYTES) as u64)
            .sum()
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
        let total_bytes = batches
            .iter()
            .map(RecordBatch::get_array_memory_size)
            .sum::<usize>();
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
        let step = self.rows_per_frame(total_bytes, total_rows);
        let mut writer = self.run_writer()?;
        let mut indices = Vec::with_capacity(step);
        for chunk in keys.chunks(step) {
            check_cancelled(cancel)?;
            indices.clear();
            indices.extend(chunk.iter().map(|key| (key.1 as usize, key.2 as usize)));
            let frame = interleave_record_batch(&refs, &indices).map_err(storage)?;
            writer.append(&frame, chunk[0].0, chunk[chunk.len() - 1].0)?;
            crate::graph_construction::construction_failpoint("bulk.during_property_run");
        }
        writer.finish()
    }

    /// Rows that make a frame of about the target size, at this row width.
    pub(super) fn rows_per_frame(&self, bytes: usize, rows: usize) -> usize {
        (self.frame_target / (bytes / rows.max(1)).max(1)).clamp(1, self.budgets.max_batch_rows)
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
            PropertySizing::SERIAL,
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

    /// `batch(start, 8)` without its properties.
    fn narrow(start: u64) -> RecordBatch {
        batch(start, 8).project(&[0, 1]).unwrap()
    }

    fn rows_of(rows: &PropertyRows<'_>, group: &SortedGroup) -> Vec<u64> {
        let mut reader = rows.group_reader(group);
        let mut seen = Vec::new();
        while let Some(batch) = reader.next().unwrap() {
            let uuids = crate::graph_construction::batch_uuid_column(&batch, "node_uuid").unwrap();
            for row in 0..batch.num_rows() {
                seen.push(u64::from_be_bytes(
                    uuids.value(row)[8..].try_into().unwrap(),
                ));
            }
        }
        seen
    }

    fn rows_with(
        scratch: &Scratch,
        run_bytes: usize,
        fan_in: usize,
        retained_bytes: u64,
    ) -> PropertyRows<'_> {
        PropertyRows::new(
            scratch,
            ConstructionChunkKind::Node,
            GraphConstructionBudgets::default(),
            0,
            PropertySizing {
                run_bytes,
                retained_bytes,
                fan_in,
                frame_bytes: 4096,
            },
        )
    }

    /// Shuffled batches of 32 identities each, from `threads` concurrent sinks.
    fn ingest(rows: &PropertyRows<'_>, batches: u64, threads: u64) {
        let cancel = AtomicBool::new(false);
        std::thread::scope(|scope| {
            for thread in 0..threads {
                let cancel = &cancel;
                scope.spawn(move || {
                    let mut sink = rows.sink();
                    for index in (0..batches).filter(|index| index % threads == thread).rev() {
                        sink.push(&batch(index * 32, 32), cancel).unwrap();
                        // What this worker holds is always paid for.
                        assert!(sink.held >= sink.retained_bytes().min(rows.sizing.retained_bytes));
                        assert!(sink.held <= rows.sizing.retained_bytes);
                    }
                    sink.finish(cancel).unwrap();
                });
            }
        });
    }

    #[test]
    fn runs_merge_into_one_sorted_stream_at_every_fan_in_and_run_size() {
        for (run_bytes, fan_in, threads) in [
            (1, 2, 1),
            (1, 3, 4),
            (3 << 10, 2, 3),
            (3 << 10, 5, 2),
            (1 << 20, 16, 1),
            (1 << 20, 2, 4),
        ] {
            let root = tempfile::tempdir().unwrap();
            let directory = super::super::StableDirectory::open(root.path()).unwrap();
            let scratch = Scratch::create(&directory).unwrap();
            let rows = rows_with(&scratch, run_bytes, fan_in, 1 << 20);
            ingest(&rows, 65, threads);
            let formed = rows.runs_formed();
            assert!(
                run_bytes > 1 << 10 || formed >= 65,
                "run_bytes {run_bytes}: {formed} runs"
            );
            let groups = rows.finish(&AtomicBool::new(false)).unwrap();
            assert_eq!(groups.len(), 1, "run_bytes {run_bytes} fan_in {fan_in}");
            let seen = rows_of(&rows, &groups[0]);
            assert_eq!(
                seen,
                (0..65 * 32).collect::<Vec<_>>(),
                "run_bytes {run_bytes} fan_in {fan_in} threads {threads}"
            );
            assert!(rows.written_bytes() > 0 && rows.read_bytes() > 0);
        }
    }

    #[test]
    fn few_runs_are_not_rewritten_by_a_wide_enough_merge() {
        // 65 runs under a fan-in of 64 merge once into segments; the input is
        // written once and the segments once: no per-level rewriting.
        let root = tempfile::tempdir().unwrap();
        let directory = super::super::StableDirectory::open(root.path()).unwrap();
        let scratch = Scratch::create(&directory).unwrap();
        let rows = rows_with(&scratch, 1, 64, 1 << 20);
        ingest(&rows, 65, 1);
        let runs_written = rows.written_bytes();
        let groups = rows.finish(&AtomicBool::new(false)).unwrap();
        let merged_written = rows.written_bytes() - runs_written;
        // One run per batch; the 65th batch makes one merge of the two
        // smallest runs, and the final merge rewrites everything once more.
        assert!(
            merged_written <= runs_written + runs_written / 8,
            "runs {runs_written} merged {merged_written}"
        );
        assert_eq!(rows_of(&rows, &groups[0]).len(), 65 * 32);
    }

    #[test]
    fn concurrent_intake_never_holds_more_than_the_gate_admits() {
        let root = tempfile::tempdir().unwrap();
        let directory = super::super::StableDirectory::open(root.path()).unwrap();
        let scratch = Scratch::create(&directory).unwrap();
        let one = batch(0, 32);
        let need = (one.get_array_memory_size() + 32 * KEY_BYTES) as u64;
        // Room for exactly two batches while eight threads push: the rest wait.
        let rows = rows_with(&scratch, usize::MAX >> 1, 4, 2 * need);
        ingest(&rows, 64, 8);
        assert_eq!(rows.gate.free(), 2 * need);
        // Eight threads shared room for two batches, and used it.
        assert!(rows.peak_retained_bytes() >= need && rows.peak_retained_bytes() <= 2 * need);
        let groups = rows.finish(&AtomicBool::new(false)).unwrap();
        assert_eq!(rows_of(&rows, &groups[0]), (0..64 * 32).collect::<Vec<_>>());
    }

    #[test]
    fn an_abandoned_sink_returns_its_bytes_to_the_gate() {
        let root = tempfile::tempdir().unwrap();
        let directory = super::super::StableDirectory::open(root.path()).unwrap();
        let scratch = Scratch::create(&directory).unwrap();
        let one = batch(0, 32);
        let need = (one.get_array_memory_size() + 32 * KEY_BYTES) as u64;
        let rows = rows_with(&scratch, usize::MAX >> 1, 4, need);
        let cancel = AtomicBool::new(false);
        let mut sink = rows.sink();
        sink.push(&one, &cancel).unwrap();
        drop(sink);
        // Were the bytes stranded, this would wait for the 20 ms poll forever.
        let mut sink = rows.sink();
        sink.push(&batch(32, 32), &cancel).unwrap();
        sink.finish(&cancel).unwrap();
    }

    #[test]
    fn a_range_merge_yields_exactly_the_rows_inside_the_range() {
        let root = tempfile::tempdir().unwrap();
        let directory = super::super::StableDirectory::open(root.path()).unwrap();
        let scratch = Scratch::create(&directory).unwrap();
        let rows = rows_with(&scratch, 1, 64, 1 << 20);
        ingest(&rows, 20, 1);
        let runs = std::mem::take(&mut rows.groups.lock().unwrap().runs)
            .into_values()
            .next()
            .unwrap();
        let id = |value: u64| {
            let mut bytes = [0_u8; 16];
            bytes[8..].copy_from_slice(&value.to_be_bytes());
            bytes
        };
        let refs = runs.iter().collect::<Vec<_>>();
        let cancel = AtomicBool::new(false);
        for (lower, upper) in [
            (None, None),
            (Some(100), Some(101)),
            (Some(31), Some(33)),
            (None, Some(7)),
            (Some(630), None),
            (Some(10_000), None),
            (Some(5), Some(5)),
        ] {
            let merged = rows
                .merge(&refs, lower.map(id), upper.map(id), &cancel)
                .unwrap();
            let group = SortedGroup {
                segments: vec![merged],
                bare_owners: None,
            };
            let seen = rows_of(&rows, &group);
            let expected = (0..20 * 32)
                .filter(|value| lower.is_none_or(|lower| *value >= lower))
                .filter(|value| upper.is_none_or(|upper| *value < upper))
                .collect::<Vec<_>>();
            assert_eq!(seen, expected, "{lower:?}..{upper:?}");
        }
    }

    #[test]
    fn identities_repeated_across_runs_are_all_kept() {
        let root = tempfile::tempdir().unwrap();
        let directory = super::super::StableDirectory::open(root.path()).unwrap();
        let scratch = Scratch::create(&directory).unwrap();
        let rows = rows_with(&scratch, 1, 2, 1 << 20);
        let cancel = AtomicBool::new(false);
        let mut sink = rows.sink();
        for _ in 0..5 {
            sink.push(&batch(0, 32), &cancel).unwrap();
        }
        sink.finish(&cancel).unwrap();
        let groups = rows.finish(&cancel).unwrap();
        let seen = rows_of(&rows, &groups[0]);
        assert_eq!(seen.len(), 5 * 32);
        assert!(seen.windows(2).all(|pair| pair[0] <= pair[1]));
    }

    #[test]
    fn schemas_stay_in_separate_groups_and_the_group_budget_is_enforced() {
        let root = tempfile::tempdir().unwrap();
        let directory = super::super::StableDirectory::open(root.path()).unwrap();
        let scratch = Scratch::create(&directory).unwrap();
        let rows = rows_with(&scratch, 1 << 20, 4, 1 << 20);
        let cancel = AtomicBool::new(false);
        let mut sink = rows.sink();
        sink.push(&batch(0, 8), &cancel).unwrap();
        sink.push(&narrow(8), &cancel).unwrap();
        sink.push(&batch(16, 8), &cancel).unwrap();
        sink.finish(&cancel).unwrap();
        let groups = rows.finish(&cancel).unwrap();
        assert_eq!(groups.len(), 2);
        // The schema without properties keeps its owners, not its rows.
        let (bare, with_rows) = groups
            .iter()
            .partition::<Vec<_>, _>(|group| group.bare_owners.is_some());
        assert_eq!((bare.len(), with_rows.len()), (1, 1));
        assert_eq!(bare[0].bare_owners, Some(vec![("Person".to_owned(), 8)]));
        assert!(bare[0].segments.is_empty());
        assert_eq!(rows_of(&rows, with_rows[0]).len(), 16);

        let limited = PropertyRows::new(
            &scratch,
            ConstructionChunkKind::Node,
            GraphConstructionBudgets {
                max_schema_groups: 1,
                ..GraphConstructionBudgets::default()
            },
            0,
            PropertySizing::SERIAL,
        );
        let mut sink = limited.sink();
        sink.push(&batch(0, 8), &cancel).unwrap();
        let error = sink.push(&narrow(8), &cancel).unwrap_err();
        assert!(error.to_string().contains("schema-group budget"), "{error}");
    }

    /// A node batch with a label per row and no properties.
    fn labelled(ids: &[u64], labels: &[&str]) -> RecordBatch {
        let uuids = ids
            .iter()
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
            ])),
            vec![
                std::sync::Arc::new(
                    FixedSizeBinaryArray::try_from_iter(uuids.iter().map(|id| id.as_slice()))
                        .unwrap(),
                ) as ArrayRef,
                std::sync::Arc::new(StringArray::from(labels.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn a_group_without_properties_orders_owners_by_first_appearance_in_identity_order() {
        let root = tempfile::tempdir().unwrap();
        let directory = super::super::StableDirectory::open(root.path()).unwrap();
        let scratch = Scratch::create(&directory).unwrap();
        let rows = rows_with(&scratch, 1 << 20, 4, 1 << 20);
        let cancel = AtomicBool::new(false);
        // Identity order is 1:Pet 2:Person 3:Person 4:City 5:Pet 6:City 7:Pet; the
        // batches arrive out of order and from two workers.
        std::thread::scope(|scope| {
            scope.spawn(|| {
                let mut sink = rows.sink();
                sink.push(&labelled(&[6, 3, 5], &["City", "Person", "Pet"]), &cancel)
                    .unwrap();
                sink.push(&labelled(&[7], &["Pet"]), &cancel).unwrap();
                sink.finish(&cancel).unwrap();
            });
            scope.spawn(|| {
                let mut sink = rows.sink();
                sink.push(&labelled(&[4, 2, 1], &["City", "Person", "Pet"]), &cancel)
                    .unwrap();
                sink.finish(&cancel).unwrap();
            });
        });
        let groups = rows.finish(&cancel).unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(
            groups[0].bare_owners,
            Some(vec![
                ("Pet".to_owned(), 3),
                ("Person".to_owned(), 2),
                ("City".to_owned(), 2),
            ])
        );
        assert_eq!(
            rows.written_bytes(),
            0,
            "a bare group must write no scratch"
        );
        assert_eq!(rows.runs_formed(), 0);
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
            PropertySizing::SERIAL,
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
            PropertySizing::SERIAL,
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
