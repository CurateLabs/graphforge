//! Bounded-memory identity checks by external sorting.
//!
//! Identities are computed directly from (label, id), so the converter only
//! needs two set checks: no (label, id) is defined twice, and every edge
//! endpoint is defined. Both run over fixed-width [`Key`] records that are
//! buffered up to the memory budget, sorted, and written as runs under
//! `<output>/.spill/`. A k-way merge of the node runs finds duplicates; a merge
//! join of the endpoint runs against the merged node keys finds dangling
//! endpoints. More runs than the merge fan-in are first merged in groups, so
//! the number of open runs never exceeds it.
//!
//! Memory: the key buffer plus one run writer, or `fan_in` run readers plus one
//! writer, each fit inside the budget. Nothing here grows with input size; the
//! run queue is a pair of sequence numbers.

use std::cell::Cell;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use serde_json::{Value, json};

use crate::error::{Cause, ConvertError, io_error};

/// Bytes of one encoded [`Key`].
pub const KEY_RECORD_BYTES: u64 = 24;
pub const DEFAULT_MEMORY_BUDGET_BYTES: u64 = 256 << 20;
/// Three records: a two-way merge needs two readers and a writer.
pub const MIN_MEMORY_BUDGET_BYTES: u64 = 3 * KEY_RECORD_BYTES;
const MAX_FAN_IN: u64 = 128;
const TARGET_READ_BYTES: u64 = 1 << 20;
pub const SPILL_DIR: &str = ".spill";

/// One node definition or one edge endpoint. The derived order sorts by
/// identity, then by input position: `file` is the index of the (table, file)
/// occurrence in mapping order and `position` is the 1-based row for a node,
/// `row << 1 | side` for an endpoint (source 0, target 1). Position order is
/// therefore the order the in-memory converter visited rows in.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct Key {
    pub label: u32,
    pub id: i64,
    pub file: u32,
    pub position: u64,
}

impl Key {
    fn same_identity(&self, other: &Self) -> bool {
        self.label == other.label && self.id == other.id
    }

    fn input_order(&self) -> (u32, u64) {
        (self.file, self.position)
    }

    fn encode(&self) -> [u8; 24] {
        let mut bytes = [0_u8; 24];
        bytes[..4].copy_from_slice(&self.label.to_le_bytes());
        bytes[4..12].copy_from_slice(&self.id.to_le_bytes());
        bytes[12..16].copy_from_slice(&self.file.to_le_bytes());
        bytes[16..].copy_from_slice(&self.position.to_le_bytes());
        bytes
    }

    fn decode(bytes: &[u8; 24]) -> Self {
        let (label, rest) = bytes.split_first_chunk::<4>().expect("24 bytes");
        let (id, rest) = rest.split_first_chunk::<8>().expect("20 bytes");
        let (file, rest) = rest.split_first_chunk::<4>().expect("12 bytes");
        let position = rest.first_chunk::<8>().expect("8 bytes");
        Self {
            label: u32::from_le_bytes(*label),
            id: i64::from_le_bytes(*id),
            file: u32::from_le_bytes(*file),
            position: u64::from_le_bytes(*position),
        }
    }
}

/// How a memory budget divides between the key buffer and merge buffers.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Budget {
    bytes: u64,
    fan_in: usize,
    io_buffer_bytes: usize,
    buffer_records: usize,
}

impl Budget {
    pub fn new(bytes: u64) -> Result<Self, ConvertError> {
        if bytes < MIN_MEMORY_BUDGET_BYTES {
            return Err(ConvertError::new(
                Cause::InvalidMemoryBudget,
                format!("memory budget {bytes} is below the minimum {MIN_MEMORY_BUDGET_BYTES}"),
            ));
        }
        let fan_in = (bytes / TARGET_READ_BYTES)
            .saturating_sub(1)
            .clamp(2, MAX_FAN_IN);
        let io = (bytes / (fan_in + 1) / KEY_RECORD_BYTES).max(1) * KEY_RECORD_BYTES;
        let records = ((bytes - io) / KEY_RECORD_BYTES).max(1);
        let too_large = || ConvertError::new(Cause::InvalidMemoryBudget, "budget exceeds usize");
        Ok(Self {
            bytes,
            fan_in: usize::try_from(fan_in).map_err(|_| too_large())?,
            io_buffer_bytes: usize::try_from(io).map_err(|_| too_large())?,
            buffer_records: usize::try_from(records).map_err(|_| too_large())?,
        })
    }

    pub fn describe(&self) -> Value {
        json!({
            "memory_budget_bytes": self.bytes,
            "key_record_bytes": KEY_RECORD_BYTES,
            "buffer_records": self.buffer_records,
            "merge_fan_in": self.fan_in,
            "io_buffer_bytes": self.io_buffer_bytes,
        })
    }
}

/// The spill directory. Dropping it removes every run, so an error return or
/// an unwinding panic leaves no spill files behind.
pub(crate) struct SpillDir {
    path: PathBuf,
    next_run: Cell<u64>,
}

impl SpillDir {
    pub fn create(output_dir: &Path) -> Result<Self, ConvertError> {
        let path = output_dir.join(SPILL_DIR);
        fs::create_dir(&path).map_err(|error| io_error(&path.display().to_string(), &error))?;
        Ok(Self {
            path,
            next_run: Cell::new(0),
        })
    }

    fn run_path(&self, run: u64) -> PathBuf {
        self.path.join(format!("{run:010}.keys"))
    }

    fn allocate(&self) -> u64 {
        let run = self.next_run.get();
        self.next_run.set(run + 1);
        run
    }

    /// Removes the directory and reports a failure to do so.
    pub fn close(self) -> Result<(), ConvertError> {
        fs::remove_dir_all(&self.path)
            .map_err(|error| io_error(&self.path.display().to_string(), &error))
    }
}

impl Drop for SpillDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

struct RunWriter {
    writer: BufWriter<File>,
    path: PathBuf,
    last: Option<Key>,
    distinct: bool,
    records: u64,
}

impl RunWriter {
    fn create(path: PathBuf, budget: &Budget, distinct: bool) -> Result<Self, ConvertError> {
        let file =
            File::create(&path).map_err(|error| io_error(&path.display().to_string(), &error))?;
        Ok(Self {
            writer: BufWriter::with_capacity(budget.io_buffer_bytes, file),
            path,
            last: None,
            distinct,
            records: 0,
        })
    }

    /// Appends `key`; a distinct writer keeps only the first key of each
    /// identity, which is the earliest in input order.
    fn push(&mut self, key: Key) -> Result<(), ConvertError> {
        if self.distinct && self.last.is_some_and(|last| last.same_identity(&key)) {
            return Ok(());
        }
        self.last = Some(key);
        self.records += 1;
        self.writer
            .write_all(&key.encode())
            .map_err(|error| io_error(&self.path.display().to_string(), &error))
    }

    fn finish(self) -> Result<u64, ConvertError> {
        self.writer
            .into_inner()
            .map_err(|error| io_error(&self.path.display().to_string(), error.error()))?;
        Ok(self.records)
    }
}

struct RunReader {
    reader: BufReader<File>,
    path: PathBuf,
}

impl RunReader {
    fn open(path: PathBuf, budget: &Budget) -> Result<Self, ConvertError> {
        let file =
            File::open(&path).map_err(|error| io_error(&path.display().to_string(), &error))?;
        Ok(Self {
            reader: BufReader::with_capacity(budget.io_buffer_bytes, file),
            path,
        })
    }

    fn next(&mut self) -> Result<Option<Key>, ConvertError> {
        let mut bytes = [0_u8; 24];
        let mut filled = 0;
        while filled < bytes.len() {
            let read = self
                .reader
                .read(&mut bytes[filled..])
                .map_err(|error| io_error(&self.path.display().to_string(), &error))?;
            if read == 0 {
                if filled == 0 {
                    return Ok(None);
                }
                return Err(ConvertError::new(
                    Cause::Io,
                    format!("{}: truncated key record", self.path.display()),
                ));
            }
            filled += read;
        }
        Ok(Some(Key::decode(&bytes)))
    }
}

/// Spill statistics for one key kind, recorded in the manifest.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct SpillStats {
    pub records: u64,
    pub records_spilled: u64,
    pub runs: u64,
    pub peak_buffered_records: u64,
    pub intermediate_merges: u64,
}

impl SpillStats {
    pub fn describe(&self) -> Value {
        json!({
            "records": self.records,
            "records_spilled": self.records_spilled,
            "runs": self.runs,
            "peak_buffered_records": self.peak_buffered_records,
            "intermediate_merges": self.intermediate_merges,
        })
    }
}

/// Buffers keys up to the budget and writes each full buffer as a sorted run.
pub(crate) struct KeySorter<'a> {
    dir: &'a SpillDir,
    budget: Budget,
    distinct: bool,
    buffer: Vec<Key>,
    first_run: u64,
    stats: SpillStats,
}

impl<'a> KeySorter<'a> {
    /// `distinct` sorters keep one key per identity (the earliest), which is
    /// all the endpoint check needs.
    pub fn new(dir: &'a SpillDir, budget: Budget, distinct: bool) -> Self {
        Self {
            dir,
            budget,
            distinct,
            buffer: Vec::with_capacity(budget.buffer_records),
            first_run: dir.next_run.get(),
            stats: SpillStats::default(),
        }
    }

    pub fn push(&mut self, key: Key) -> Result<(), ConvertError> {
        if self.buffer.len() == self.budget.buffer_records {
            self.spill()?;
        }
        self.buffer.push(key);
        self.stats.records += 1;
        Ok(())
    }

    fn spill(&mut self) -> Result<(), ConvertError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        self.stats.peak_buffered_records = self
            .stats
            .peak_buffered_records
            .max(self.buffer.len() as u64);
        self.buffer.sort_unstable();
        let mut writer = RunWriter::create(
            self.dir.run_path(self.dir.allocate()),
            &self.budget,
            self.distinct,
        )?;
        for key in self.buffer.drain(..) {
            writer.push(key)?;
        }
        self.stats.records_spilled += writer.finish()?;
        self.stats.runs += 1;
        Ok(())
    }

    /// Writes the last run, releases the buffer, and merges runs in groups
    /// until at most `fan_in` remain.
    pub fn finish(mut self) -> Result<SortedRuns<'a>, ConvertError> {
        self.spill()?;
        self.buffer = Vec::new();
        let mut runs = SortedRuns {
            dir: self.dir,
            budget: self.budget,
            distinct: self.distinct,
            first: self.first_run,
            end: self.dir.next_run.get(),
            stats: self.stats,
        };
        runs.reduce()?;
        Ok(runs)
    }
}

/// Runs `first..end` of one key kind, at most `fan_in` of them after
/// [`KeySorter::finish`].
pub(crate) struct SortedRuns<'a> {
    dir: &'a SpillDir,
    budget: Budget,
    distinct: bool,
    first: u64,
    end: u64,
    stats: SpillStats,
}

impl SortedRuns<'_> {
    pub fn stats(&self) -> SpillStats {
        self.stats
    }

    fn reduce(&mut self) -> Result<(), ConvertError> {
        let fan_in = self.budget.fan_in as u64;
        while self.end - self.first > fan_in {
            let group = self.first..self.first + fan_in;
            let output = self.dir.allocate();
            debug_assert_eq!(output, self.end);
            let mut merge = Merge::open(self.dir, &self.budget, group.clone(), self.distinct)?;
            let mut writer =
                RunWriter::create(self.dir.run_path(output), &self.budget, self.distinct)?;
            while let Some(key) = merge.next()? {
                writer.push(key)?;
            }
            writer.finish()?;
            drop(merge);
            self.remove(group)?;
            self.first += fan_in;
            self.end = output + 1;
            self.stats.intermediate_merges += 1;
        }
        Ok(())
    }

    fn remove(&self, runs: std::ops::Range<u64>) -> Result<(), ConvertError> {
        for run in runs {
            let path = self.dir.run_path(run);
            fs::remove_file(&path)
                .map_err(|error| io_error(&path.display().to_string(), &error))?;
        }
        Ok(())
    }

    fn merge(&self) -> Result<Merge, ConvertError> {
        Merge::open(self.dir, &self.budget, self.first..self.end, self.distinct)
    }
}

/// K-way merge over runs; a distinct merge keeps the earliest key of each
/// identity.
struct Merge {
    readers: Vec<RunReader>,
    heap: BinaryHeap<Reverse<(Key, usize)>>,
    distinct: bool,
    last: Option<Key>,
}

impl Merge {
    fn open(
        dir: &SpillDir,
        budget: &Budget,
        runs: std::ops::Range<u64>,
        distinct: bool,
    ) -> Result<Self, ConvertError> {
        let mut readers = Vec::with_capacity(budget.fan_in);
        let mut heap = BinaryHeap::with_capacity(budget.fan_in);
        for run in runs {
            let mut reader = RunReader::open(dir.run_path(run), budget)?;
            if let Some(key) = reader.next()? {
                heap.push(Reverse((key, readers.len())));
            }
            readers.push(reader);
        }
        Ok(Self {
            readers,
            heap,
            distinct,
            last: None,
        })
    }

    fn next(&mut self) -> Result<Option<Key>, ConvertError> {
        while let Some(Reverse((key, index))) = self.heap.pop() {
            if let Some(next) = self.readers[index].next()? {
                self.heap.push(Reverse((next, index)));
            }
            if self.distinct && self.last.is_some_and(|last| last.same_identity(&key)) {
                continue;
            }
            self.last = Some(key);
            return Ok(Some(key));
        }
        Ok(None)
    }
}

/// The earliest second definition of any (label, id): the duplicate the
/// in-memory converter reported, with the definition it collided with.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Duplicate {
    pub first: Key,
    pub second: Key,
}

/// The sorted distinct node identities, kept as one run for the endpoint join.
pub(crate) struct NodeKeys {
    run: u64,
}

/// Merges node runs, writes the distinct node identities to one run, and
/// returns the earliest duplicate in input order, if any.
pub(crate) fn check_nodes(
    runs: &SortedRuns<'_>,
) -> Result<(NodeKeys, Option<Duplicate>), ConvertError> {
    let mut merge = runs.merge()?;
    let run = runs.dir.allocate();
    let mut writer = RunWriter::create(runs.dir.run_path(run), &runs.budget, true)?;
    let mut group: Option<(Key, u64)> = None;
    let mut duplicate: Option<Duplicate> = None;
    while let Some(key) = merge.next()? {
        writer.push(key)?;
        match &mut group {
            Some((first, count)) if first.same_identity(&key) => {
                *count += 1;
                let earlier =
                    duplicate.is_none_or(|found| key.input_order() < found.second.input_order());
                if *count == 2 && earlier {
                    duplicate = Some(Duplicate {
                        first: *first,
                        second: key,
                    });
                }
            }
            _ => group = Some((key, 1)),
        }
    }
    writer.finish()?;
    drop(merge);
    runs.remove(runs.first..runs.end)?;
    Ok((NodeKeys { run }, duplicate))
}

/// Merge-joins endpoint keys against node identities and returns the
/// earliest dangling endpoint in input order, if any.
pub(crate) fn find_dangling(
    endpoints: &SortedRuns<'_>,
    nodes: &NodeKeys,
) -> Result<Option<Key>, ConvertError> {
    let mut merge = endpoints.merge()?;
    let mut defined = RunReader::open(endpoints.dir.run_path(nodes.run), &endpoints.budget)?;
    let mut node = defined.next()?;
    let mut dangling: Option<Key> = None;
    while let Some(key) = merge.next()? {
        while let Some(candidate) = node {
            if (candidate.label, candidate.id) >= (key.label, key.id) {
                break;
            }
            node = defined.next()?;
        }
        let present = node.is_some_and(|candidate| candidate.same_identity(&key));
        let earlier = dangling.is_none_or(|found| key.input_order() < found.input_order());
        if !present && earlier {
            dangling = Some(key);
        }
    }
    Ok(dangling)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(label: u32, id: i64, file: u32, position: u64) -> Key {
        Key {
            label,
            id,
            file,
            position,
        }
    }

    #[test]
    fn key_encoding_round_trips_extremes() {
        for value in [
            key(0, i64::MIN, 0, 0),
            key(u32::MAX, i64::MAX, u32::MAX, u64::MAX),
            key(7, -1, 3, 9),
        ] {
            assert_eq!(Key::decode(&value.encode()), value);
        }
    }

    #[test]
    fn budget_division_fits_the_budget() {
        for bytes in [
            MIN_MEMORY_BUDGET_BYTES,
            240,
            1 << 20,
            16 << 20,
            DEFAULT_MEMORY_BUDGET_BYTES,
        ] {
            let budget = Budget::new(bytes).unwrap();
            let buffer = budget.buffer_records as u64 * KEY_RECORD_BYTES;
            assert!(buffer + budget.io_buffer_bytes as u64 <= bytes, "{bytes}");
            assert!(
                (budget.fan_in as u64 + 1) * budget.io_buffer_bytes as u64 <= bytes,
                "{bytes}"
            );
            assert!(budget.fan_in >= 2);
        }
        assert_eq!(
            Budget::new(MIN_MEMORY_BUDGET_BYTES - 1)
                .unwrap_err()
                .cause(),
            Cause::InvalidMemoryBudget
        );
    }

    #[test]
    fn multi_pass_merge_keeps_every_key_in_order() {
        let scratch = tempfile::tempdir().unwrap();
        let dir = SpillDir::create(scratch.path()).unwrap();
        let budget = Budget::new(MIN_MEMORY_BUDGET_BYTES).unwrap();
        let mut sorter = KeySorter::new(&dir, budget, false);
        let mut expected = Vec::new();
        for index in 0..50_u64 {
            let value = key(0, ((index * 37) % 23) as i64, 0, index);
            expected.push(value);
            sorter.push(value).unwrap();
        }
        let runs = sorter.finish().unwrap();
        assert!(runs.stats().intermediate_merges > 0);
        assert!(runs.end - runs.first <= budget.fan_in as u64);
        let mut merge = runs.merge().unwrap();
        let mut merged = Vec::new();
        while let Some(value) = merge.next().unwrap() {
            merged.push(value);
        }
        expected.sort();
        assert_eq!(merged, expected);
        drop(merge);
        dir.close().unwrap();
        assert!(!scratch.path().join(SPILL_DIR).exists());
    }
}
