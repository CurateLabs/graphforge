//! External processing of over-budget fixed-width partitions (#1585, ADR 0047).
//!
//! A fixed-width partition whose materialization would exceed the recorded
//! `max_partition_bytes` is sorted with bounded memory instead of refusing the
//! ingest, when the session's recorded `max_external_partition_bytes` admits
//! it. This is GraphForge's own bounded external merge, the mechanism #1585's
//! predeclared comparison selected
//! (`docs/development/evidence/external-partition-comparison-1585.md`).
//!
//! * **Sort phase, on a load worker.** The partition's sealed segments are read
//!   in routing order, in runs of at most `max_partition_bytes`. Each run is
//!   sorted in memory and written to a construction-owned artifact temporary
//!   with an XXH64 checksum. No run is larger than the resident budget, so the
//!   memory bound is the one the resident path has.
//! * **Merge phase, on the coordinator.** The runs stream through a k-way
//!   merge into the same consumer the resident path feeds, so the shaped output
//!   bytes are identical.
//!
//! Runs are transient scratch, never recovery authority. They are artifact
//! temporaries (`.artifact-xrun-…-<random>.tmp`), so session open and
//! recovery reclaim any a crash leaves behind, and every exit path unlinks
//! them. The sealed segments remain the only resume state. Each run's length,
//! record count and checksum are verified as the merge reads it, so a mutated
//! or truncated run is refused before anything derived from it can publish.
//!
//! Only partitions without a detail codec take this path: identities and the
//! staged and resolved endpoint families. The endpoint families are the ones
//! that need it, because one high-degree node's records cannot be split across
//! partitions. Duplicate and order checks stay on the coordinator's consumer,
//! so they apply to merged records as to resident ones. Detail-codec
//! partitions keep the refusal.

use super::partition_load::{PartitionLoadCounters, abandon_if_stopped};
use super::{merge_cache_release_evidence, open_fixed_reader, read_run_record, storage};
use crate::construction_directory::ConstructionDirectory as StableDirectory;
use crate::corruption_checksum::Checksum;
use graphforge_core::GfError;
use graphforge_filesystem::{FileIdentity, file_identity};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::ffi::OsString;
use std::io::{BufReader, BufWriter, Read, Write};
use std::sync::atomic::AtomicBool;

/// Buffered read and write size for runs.
const RUN_IO_BYTES: usize = 256 << 10;

/// One sorted run on disk.
struct Run {
    temporary: OsString,
    identity: FileIdentity,
    records: u64,
    xxh64: u64,
}

/// A partition sorted into on-disk runs, ready for the coordinator to merge.
///
/// Dropping it, on any path, unlinks every run.
pub(super) struct ExternalPartition<const N: usize> {
    root: StableDirectory,
    runs: Vec<Run>,
}

impl<const N: usize> Drop for ExternalPartition<N> {
    fn drop(&mut self) {
        for run in self.runs.drain(..) {
            let _ = self
                .root
                .unlink_child_if_identity(run.temporary.as_os_str(), run.identity);
        }
    }
}

/// The artifact-temp target of a run: `xrun-p<ordinal>`. Recovery reclaims
/// temporaries with this target; it is never a canonical artifact name, so no
/// receipt can name a run.
pub(super) fn is_external_run_target(target: &str) -> bool {
    target.strip_prefix("xrun-p").is_some_and(|ordinal| {
        !ordinal.is_empty()
            && ordinal.len() <= 20
            && ordinal.bytes().all(|byte| byte.is_ascii_digit())
    })
}

/// The run size, in records, for a partition budget.
pub(super) fn run_records<const N: usize>(max_partition_bytes: u64) -> usize {
    usize::try_from(max_partition_bytes / N as u64)
        .unwrap_or(usize::MAX)
        .max(1)
}

/// Sort one over-budget fixed-width partition into runs, on a load worker.
///
/// The shared evidence is never touched here. Everything observed comes back
/// in [`PartitionLoadCounters`] for the coordinator to merge in partition
/// order, as for a resident load.
///
/// # Errors
/// Refuses when the partition exceeds `max_external_partition_bytes`, when
/// its record count differs from the routed count, on any I/O failure, and
/// when the coordinator has stopped.
pub(super) fn sort_into_runs<const N: usize>(
    root: &StableDirectory,
    names: &[String],
    expected_records: Option<u64>,
    max_partition_bytes: u64,
    max_external_partition_bytes: u64,
    stop: &AtomicBool,
) -> Result<(ExternalPartition<N>, PartitionLoadCounters), GfError> {
    if names.is_empty() {
        return Err(storage("partition has no sealed segments"));
    }
    let mut partition = ExternalPartition {
        root: root.try_clone().map_err(storage)?,
        runs: Vec::new(),
    };
    let mut counters = PartitionLoadCounters::default();
    for name in names {
        let length = root
            .open_child_file(std::ffi::OsStr::new(name))
            .and_then(|file| file.metadata())
            .map_err(storage)?
            .len();
        counters.spill_bytes = counters
            .spill_bytes
            .checked_add(length)
            .ok_or_else(|| storage("partition spill byte count overflows"))?;
    }
    if counters.spill_bytes > max_external_partition_bytes {
        return Err(storage(format!(
            "partition requires {} bytes of external scratch, exceeds recorded external budget {max_external_partition_bytes}",
            counters.spill_bytes
        )));
    }
    let capacity = run_records::<N>(max_partition_bytes);
    let mut run: Vec<[u8; N]> = Vec::with_capacity(capacity.min(1 << 20));
    let mut records = 0_u64;
    for name in names {
        let (mut reader, counter, _segment_bytes) = open_fixed_reader(root, name)?;
        let segment = (|| -> Result<(), GfError> {
            while let Some(record) = read_run_record::<N>(&mut reader, None)? {
                run.push(record);
                records += 1;
                // Polled on the partition's count: the run's resets per run.
                abandon_if_stopped(usize::try_from(records).unwrap_or(0), stop)?;
                if run.len() >= capacity {
                    partition.write_run(&mut run, &mut counters)?;
                }
            }
            Ok(())
        })();
        let released = reader
            .get_mut()
            .inner
            .finish()
            .map_err(storage)
            .and_then(|release| merge_cache_release_evidence(&mut counters.cache_release, release));
        segment?;
        released?;
        let (read_bytes, read_operations) = counter.values();
        counters.read_bytes = counters
            .read_bytes
            .checked_add(read_bytes)
            .ok_or_else(|| storage("partition read byte count overflows"))?;
        counters.read_operations = counters
            .read_operations
            .checked_add(read_operations)
            .ok_or_else(|| storage("partition read operation count overflows"))?;
    }
    if !run.is_empty() {
        partition.write_run(&mut run, &mut counters)?;
    }
    if expected_records.is_some_and(|expected| records != expected) {
        return Err(storage("partition differs from admitted record count"));
    }
    counters.records = records;
    Ok((partition, counters))
}

impl<const N: usize> ExternalPartition<N> {
    /// Sort `run` and write it as one checksummed run temporary, then clear it.
    fn write_run(
        &mut self,
        run: &mut Vec<[u8; N]>,
        counters: &mut PartitionLoadCounters,
    ) -> Result<(), GfError> {
        // Whole records compare as bytes, and equal records are identical, so
        // any sort yields the same byte sequence the resident sort does.
        run.sort_unstable();
        let target = format!("xrun-p{}", self.runs.len());
        debug_assert!(is_external_run_target(&target));
        let temporary = super::artifact_temp(&target);
        let file = self
            .root
            .create_replaceable_child_file(temporary.as_os_str())
            .map_err(storage)?;
        let identity = file_identity(&file).map_err(storage)?;
        // Registered before writing, so a failed write still unlinks it.
        self.runs.push(Run {
            temporary,
            identity,
            records: 0,
            xxh64: 0,
        });
        let mut checksum = Checksum::new();
        let mut writer = BufWriter::with_capacity(RUN_IO_BYTES, file);
        for record in run.iter() {
            checksum.update(record);
            writer.write_all(record).map_err(storage)?;
        }
        #[cfg(test)]
        if tests::FAIL_RUN_WRITE_AFTER
            .with(std::cell::Cell::get)
            .is_some_and(|runs| self.runs.len() > runs)
        {
            // A write that fails once the file holds data, as a full disk does.
            return Err(storage("injected external run write failure"));
        }
        writer.flush().map_err(storage)?;
        let records = run.len() as u64;
        let entry = self
            .runs
            .last_mut()
            .ok_or_else(|| storage("external run registration lost"))?;
        entry.records = records;
        entry.xxh64 = checksum.finish();
        counters.external_runs += 1;
        counters.external_run_bytes = counters
            .external_run_bytes
            .checked_add(records * N as u64)
            .ok_or_else(|| storage("external run byte count overflows"))?;
        counters.external_peak_run_records = counters.external_peak_run_records.max(records);
        run.clear();
        super::construction_failpoint("shape.external_run.after_write");
        Ok(())
    }

    /// Stream every record in sorted order into `consume`, verifying each run
    /// as it is read. Called on the coordinator.
    ///
    /// # Errors
    /// Refuses a run whose length, record count or checksum differs from what
    /// was written, and propagates `consume`'s errors.
    pub(super) fn for_each_record(
        self,
        mut consume: impl FnMut(&[u8]) -> Result<(), GfError>,
    ) -> Result<(), GfError> {
        let mut streams = Vec::with_capacity(self.runs.len());
        for run in &self.runs {
            let file = self
                .root
                .open_child_file(run.temporary.as_os_str())
                .map_err(storage)?;
            if file_identity(&file).map_err(storage)? != run.identity {
                return Err(storage("external run identity changed"));
            }
            if file.metadata().map_err(storage)?.len() != run.records * N as u64 {
                return Err(storage("external run length differs from what was written"));
            }
            streams.push(RunStream {
                reader: BufReader::with_capacity(RUN_IO_BYTES, file),
                checksum: Checksum::new(),
                read: 0,
                expected: run.records,
                xxh64: run.xxh64,
            });
        }
        let mut heap = BinaryHeap::with_capacity(streams.len());
        for (index, stream) in streams.iter_mut().enumerate() {
            if let Some(record) = stream.next::<N>()? {
                heap.push(Reverse((record, index)));
            }
        }
        while let Some(Reverse((record, index))) = heap.pop() {
            consume(&record)?;
            if let Some(next) = streams[index].next::<N>()? {
                heap.push(Reverse((next, index)));
            }
        }
        Ok(())
    }
}

/// One run being merged, verified as it is consumed.
struct RunStream {
    reader: BufReader<std::fs::File>,
    checksum: Checksum,
    read: u64,
    expected: u64,
    xxh64: u64,
}

impl RunStream {
    /// The next record, or `None` once the run is exhausted and verified.
    fn next<const N: usize>(&mut self) -> Result<Option<[u8; N]>, GfError> {
        if self.read == self.expected {
            let mut probe = [0_u8; 1];
            if self.reader.read(&mut probe).map_err(storage)? != 0 {
                return Err(storage("external run is longer than what was written"));
            }
            if self.checksum.finish() != self.xxh64 {
                return Err(storage(
                    "external run checksum differs from what was written",
                ));
            }
            return Ok(None);
        }
        let mut record = [0_u8; N];
        self.reader
            .read_exact(&mut record)
            .map_err(|_| storage("external run is truncated"))?;
        self.checksum.update(&record);
        self.read += 1;
        Ok(Some(record))
    }
}

#[cfg(test)]
mod tests;
