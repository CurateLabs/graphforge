//! Merging sorted property runs (#1938).
//!
//! A merge holds one decoded frame per input, picks the smallest identity
//! among them, and writes the winners in frame-sized chunks gathered with one
//! `interleave` per column. A run knows the identities that bound each of its
//! frames, so a merge can be restricted to an identity range: it seeks to the
//! first frame that can hold the range and stops at the first frame beyond it.
//! The segments of consecutive ranges, in range order, are the merged run.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use arrow::array::{Array, FixedSizeBinaryArray};
use arrow::compute::interleave_record_batch;
use arrow::record_batch::RecordBatch;
use rayon::prelude::*;

use super::property_rows::{BareOwners, FrameMeta, Groups, PropertyRows, Run};
use super::tables::check_cancelled;
use super::{AtomicBool, GfError, storage};

type Uuid = [u8; 16];

/// A sorted property group: its segments, in identity order.
pub(super) struct SortedGroup {
    pub(super) segments: Vec<Run>,
    /// A group without property columns keeps no rows: its owners, in order of
    /// first appearance by identity, with how many rows each holds.
    pub(super) bare_owners: Option<Vec<(String, u64)>>,
}

impl SortedGroup {
    fn bare(owners: &BareOwners) -> Self {
        let mut order = owners
            .iter()
            .map(|(owner, (smallest, count))| (*smallest, owner.clone(), *count))
            .collect::<Vec<_>>();
        order.sort_unstable();
        Self {
            segments: Vec::new(),
            bare_owners: Some(
                order
                    .into_iter()
                    .map(|(_, owner, count)| (owner, count))
                    .collect(),
            ),
        }
    }
}

/// Target size of one merged segment. Ranges of this size keep every core
/// busy on a large group without making files for a small one.
const SEGMENT_BYTES: u64 = 32 << 20;
const MAX_SEGMENTS: u64 = 256;

/// One run's cursor inside a merge.
struct Input<'r, 'a> {
    reader: super::property_rows::RowsReader<'r, 'a>,
    frames: &'r [FrameMeta],
    next_frame: usize,
    lower: Option<Uuid>,
    upper: Option<Uuid>,
    batch: Option<RecordBatch>,
    uuids: Option<FixedSizeBinaryArray>,
    row: usize,
    end: usize,
    /// Position of `batch` in the chunk being gathered.
    slot: Option<usize>,
}

fn key(uuids: &FixedSizeBinaryArray, row: usize) -> Uuid {
    <Uuid>::try_from(uuids.value(row)).expect("16-byte identity")
}

/// First row of `uuids` for which `before` no longer holds.
fn partition(uuids: &FixedSizeBinaryArray, before: impl Fn(&Uuid) -> bool) -> usize {
    let (mut low, mut high) = (0, uuids.len());
    while low < high {
        let middle = low + (high - low) / 2;
        if before(&key(uuids, middle)) {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    low
}

impl<'r, 'a> Input<'r, 'a> {
    fn open(
        rows: &'r PropertyRows<'a>,
        run: &'r Run,
        lower: Option<Uuid>,
        upper: Option<Uuid>,
    ) -> Result<Self, GfError> {
        let first = lower.map_or(0, |lower| {
            run.frames.partition_point(|frame| frame.last < lower)
        });
        let mut reader = rows.reader(&run.path)?;
        if let Some(frame) = run.frames.get(first) {
            reader.seek(frame.offset)?;
        }
        Ok(Self {
            reader,
            frames: &run.frames[first..],
            next_frame: 0,
            lower,
            upper,
            batch: None,
            uuids: None,
            row: 0,
            end: 0,
            slot: None,
        })
    }

    /// Load the next frame that holds rows of the range. `false` at its end.
    fn load(&mut self, uuid_name: &str) -> Result<bool, GfError> {
        while self.next_frame < self.frames.len() {
            let meta = self.frames[self.next_frame];
            if self.upper.is_some_and(|upper| meta.first >= upper) {
                break;
            }
            let batch = self
                .reader
                .next()?
                .ok_or_else(|| storage("a property run ended before its frame index"))?;
            self.next_frame += 1;
            let uuids = crate::graph_construction::batch_uuid_column(&batch, uuid_name)?.clone();
            let start = self
                .lower
                .take()
                .map_or(0, |lower| partition(&uuids, |uuid| *uuid < lower));
            let end = self
                .upper
                .map_or(uuids.len(), |upper| partition(&uuids, |uuid| *uuid < upper));
            if start >= end {
                continue;
            }
            self.row = start;
            self.end = end;
            self.uuids = Some(uuids);
            self.batch = Some(batch);
            self.slot = None;
            return Ok(true);
        }
        self.batch = None;
        self.uuids = None;
        Ok(false)
    }

    fn current(&self) -> Uuid {
        key(self.uuids.as_ref().expect("a loaded frame"), self.row)
    }
}

impl PropertyRows<'_> {
    /// Merge `runs` into one run holding the rows with identities in
    /// `[lower, upper)`; `None` leaves that side open.
    pub(super) fn merge(
        &self,
        runs: &[&Run],
        lower: Option<Uuid>,
        upper: Option<Uuid>,
        cancel: &AtomicBool,
    ) -> Result<Run, GfError> {
        let uuid_name = self.uuid_name();
        self.note_merge_inputs(runs.len());
        let mut inputs = runs
            .iter()
            .map(|run| Input::open(self, run, lower, upper))
            .collect::<Result<Vec<_>, _>>()?;
        let (bytes, rows) = runs.iter().fold((0_usize, 0_usize), |(bytes, rows), run| {
            (
                bytes.saturating_add(usize::try_from(run.bytes()).unwrap_or(usize::MAX)),
                rows.saturating_add(usize::try_from(run.rows).unwrap_or(usize::MAX)),
            )
        });
        let step = self.rows_per_frame(bytes, rows);
        let mut heap = BinaryHeap::with_capacity(inputs.len());
        for (index, input) in inputs.iter_mut().enumerate() {
            if input.load(uuid_name)? {
                heap.push(Reverse((input.current(), index)));
            }
        }
        let mut writer = self.run_writer()?;
        let mut batches = Vec::<RecordBatch>::new();
        let mut indices = Vec::<(usize, usize)>::with_capacity(step);
        let mut bounds = None::<(Uuid, Uuid)>;
        while let Some(Reverse((uuid, index))) = heap.pop() {
            let input = &mut inputs[index];
            let slot = if let Some(slot) = input.slot {
                slot
            } else {
                batches.push(input.batch.clone().expect("a loaded frame"));
                input.slot = Some(batches.len() - 1);
                batches.len() - 1
            };
            indices.push((slot, input.row));
            bounds = Some(bounds.map_or((uuid, uuid), |(first, _)| (first, uuid)));
            input.row += 1;
            if input.row < input.end || input.load(uuid_name)? {
                heap.push(Reverse((input.current(), index)));
            }
            if indices.len() == step {
                check_cancelled(cancel)?;
                Self::flush_chunk(&mut writer, &mut batches, &mut indices, &mut bounds)?;
                crate::graph_construction::construction_failpoint("bulk.during_property_merge");
                for input in &mut inputs {
                    input.slot = None;
                }
            }
        }
        Self::flush_chunk(&mut writer, &mut batches, &mut indices, &mut bounds)?;
        writer.finish()
    }

    fn flush_chunk(
        writer: &mut super::property_rows::RunWriter<'_, '_>,
        batches: &mut Vec<RecordBatch>,
        indices: &mut Vec<(usize, usize)>,
        bounds: &mut Option<(Uuid, Uuid)>,
    ) -> Result<(), GfError> {
        if let Some((first, last)) = bounds.take() {
            let refs = batches.iter().collect::<Vec<_>>();
            let frame = interleave_record_batch(&refs, indices).map_err(storage)?;
            writer.append(&frame, first, last)?;
        }
        batches.clear();
        indices.clear();
        Ok(())
    }

    /// Identities that split the runs' rows into `parts` ranges of about equal
    /// size, from the bounds the frames state.
    fn splitters(runs: &[Run], parts: usize) -> Vec<Uuid> {
        let mut firsts = runs
            .iter()
            .flat_map(|run| run.frames.iter().map(|frame| (frame.first, frame.rows)))
            .collect::<Vec<_>>();
        firsts.sort_unstable();
        let total = firsts.iter().map(|(_, rows)| u64::from(*rows)).sum::<u64>();
        let mut splitters = Vec::with_capacity(parts);
        let mut seen = 0_u64;
        let mut next = 1_u64;
        for (first, rows) in firsts {
            while next < parts as u64 && seen >= total * next / parts as u64 {
                splitters.push(first);
                next += 1;
            }
            seen += u64::from(rows);
        }
        splitters.dedup();
        splitters
    }

    /// Reduce every group to at most `fan_in` runs by merging its smallest
    /// runs, in parallel.
    fn reduce(&self, groups: &mut [Vec<Run>], cancel: &AtomicBool) -> Result<(), GfError> {
        let fan_in = self.sizing.fan_in.max(2);
        // Each merge removes `inputs - 1` runs, so just enough merge, all at
        // once, to leave `fan_in`; if that needs more runs than there are, the
        // level merges them all and the next level finishes.
        loop {
            let mut jobs = Vec::new();
            for (group, runs) in groups.iter_mut().enumerate() {
                if runs.len() <= fan_in {
                    continue;
                }
                // Smallest first; the path breaks ties so the choice is stable.
                runs.sort_by(|left, right| (left.rows, &left.path).cmp(&(right.rows, &right.path)));
                let excess = runs.len() - fan_in;
                let mut merges = excess.div_ceil(fan_in - 1);
                let mut take = excess + merges;
                if take > runs.len() {
                    take = runs.len();
                    merges = take.div_ceil(fan_in);
                }
                let consumed = runs.drain(..take).collect::<Vec<_>>();
                let (base, extra) = (consumed.len() / merges, consumed.len() % merges);
                let mut consumed = consumed.into_iter();
                for merge in 0..merges {
                    let inputs = consumed
                        .by_ref()
                        .take(base + usize::from(merge < extra))
                        .collect::<Vec<_>>();
                    if inputs.len() == 1 {
                        // A lone run has nothing to merge with at this level.
                        runs.extend(inputs);
                    } else {
                        jobs.push((group, inputs));
                    }
                }
            }
            if jobs.is_empty() {
                break;
            }
            let merged = jobs
                .into_par_iter()
                .map(|(group, inputs)| {
                    let refs = inputs.iter().collect::<Vec<_>>();
                    let run = self.merge(&refs, None, None, cancel)?;
                    for input in &inputs {
                        std::fs::remove_file(&input.path).map_err(storage)?;
                    }
                    Ok((group, run))
                })
                .collect::<Result<Vec<_>, GfError>>()?;
            for (group, run) in merged {
                groups[group].push(run);
            }
        }
        Ok(())
    }

    /// Sort every schema group: reduce each to at most `fan_in` runs, then
    /// merge what is left into identity-range segments in parallel.
    pub(super) fn finish(&self, cancel: &AtomicBool) -> Result<Vec<SortedGroup>, GfError> {
        let Groups { runs, bare } = std::mem::take(
            &mut *self
                .groups
                .lock()
                .map_err(|_| storage("property schema lock poisoned"))?,
        );
        let (digests, mut groups): (Vec<String>, Vec<Vec<Run>>) = runs.into_iter().unzip();
        self.reduce(&mut groups, cancel)?;
        // One merge per identity range of every group with several runs.
        let mut ranges = Vec::new();
        for (group, runs) in groups.iter().enumerate() {
            if runs.len() < 2 {
                continue;
            }
            let bytes = runs.iter().map(Run::bytes).sum::<u64>();
            let parts = usize::try_from(bytes.div_ceil(SEGMENT_BYTES).clamp(1, MAX_SEGMENTS))
                .map_err(storage)?;
            let splitters = Self::splitters(runs, parts);
            for part in 0..=splitters.len() {
                ranges.push((
                    group,
                    part.checked_sub(1).map(|index| splitters[index]),
                    splitters.get(part).copied(),
                ));
            }
        }
        let segments = ranges
            .into_par_iter()
            .map(|(group, lower, upper)| {
                let refs = groups[group].iter().collect::<Vec<_>>();
                Ok((group, self.merge(&refs, lower, upper, cancel)?))
            })
            .collect::<Result<Vec<_>, GfError>>()?;
        // Groups keep the order of their schema digests, bare ones among them.
        let mut sorted = std::collections::BTreeMap::new();
        let mut merged_groups = (0..groups.len()).map(|_| Vec::new()).collect::<Vec<_>>();
        for (group, segment) in segments {
            if segment.rows == 0 {
                std::fs::remove_file(&segment.path).map_err(storage)?;
            } else {
                merged_groups[group].push(segment);
            }
        }
        for (group, (digest, runs)) in digests.into_iter().zip(groups).enumerate() {
            let segments = if runs.len() >= 2 {
                for run in &runs {
                    std::fs::remove_file(&run.path).map_err(storage)?;
                }
                std::mem::take(&mut merged_groups[group])
            } else {
                runs
            };
            if !segments.is_empty() {
                sorted.insert(
                    digest,
                    SortedGroup {
                        segments,
                        bare_owners: None,
                    },
                );
            }
        }
        for (digest, owners) in &bare {
            sorted.insert(digest.clone(), SortedGroup::bare(owners));
        }
        Ok(sorted.into_values().collect())
    }
}
