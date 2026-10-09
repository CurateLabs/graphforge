//! Merging sorted property runs (#1938).
//!
//! A merge holds one decoded frame per input, picks the smallest identity
//! among them, and writes the winners in frame-sized chunks through the shared
//! gather path. List children are extended as ranges to avoid per-child index
//! arrays. A run knows the identities that bound each of its
//! frames, so a merge can be restricted to an identity range: it seeks to the
//! first frame that can hold the range and stops at the first frame beyond it.
//! The segments of consecutive ranges, in range order, are the merged run.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, VecDeque};

use arrow::array::{Array, FixedSizeBinaryArray};
use arrow::record_batch::RecordBatch;
use rayon::prelude::*;

use super::property_gather::gather_record_batch;
use super::property_rows::{BareOwners, FrameMeta, Groups, PropertyRows, Run};
use super::tables::check_cancelled;
use super::{AtomicBool, GfError, storage};

type Uuid = [u8; 16];
const CANCEL_CHECK_ROWS: usize = 256;

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
    fn load(&mut self, uuid_name: &str, cancel: &AtomicBool) -> Result<bool, GfError> {
        self.batch = None;
        self.uuids = None;
        self.slot = None;
        while self.next_frame < self.frames.len() {
            check_cancelled(cancel)?;
            let meta = self.frames[self.next_frame];
            if self.upper.is_some_and(|upper| meta.first >= upper) {
                break;
            }
            let batch = self
                .reader
                .next_expected(&meta)?
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
    fn flush_merge_chunk(
        writer: &mut super::property_rows::RunWriter<'_, '_>,
        batches: &mut Vec<RecordBatch>,
        indices: &mut Vec<(usize, usize)>,
        bounds: &mut Option<(Uuid, Uuid)>,
        max_row_bytes: &mut usize,
        inputs: &mut [Input<'_, '_>],
        cancel: &AtomicBool,
    ) -> Result<(), GfError> {
        check_cancelled(cancel)?;
        Self::flush_chunk(writer, batches, indices, bounds, max_row_bytes)?;
        crate::graph_construction::construction_failpoint("bulk.during_property_merge");
        for input in inputs {
            input.slot = None;
        }
        Ok(())
    }

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
        check_cancelled(cancel)?;
        let job_cost = self.merge_job_cost(runs, lower, upper);
        let _reservation = self.merge_gate.hold_strict(job_cost, cancel)?;
        check_cancelled(cancel)?;
        self.note_merge_inputs(runs.len());
        let mut inputs = runs
            .iter()
            .map(|run| Input::open(self, run, lower, upper))
            .collect::<Result<Vec<_>, _>>()?;
        let max_rows = self.budgets.max_batch_rows;
        let mut heap = Self::initial_heap(&mut inputs, uuid_name, cancel)?;
        let mut writer = self.run_writer()?;
        let mut batches = Vec::<RecordBatch>::new();
        let mut indices = Vec::<(usize, usize)>::with_capacity(max_rows);
        let mut chunk_bytes = 0_usize;
        let mut chunk_max_row_bytes = 0_usize;
        let mut bounds = None::<(Uuid, Uuid)>;
        let mut processed_rows = 0_usize;
        while let Some(Reverse((uuid, index))) = heap.pop() {
            let source_batch = inputs[index].batch.as_ref().expect("a loaded frame");
            let row_bytes = Self::row_bytes(source_batch, inputs[index].row)?;
            let next_bytes = chunk_bytes
                .checked_add(row_bytes)
                .ok_or_else(|| storage("property frame byte total overflows"))?;
            if !indices.is_empty() && (indices.len() >= max_rows || next_bytes > self.frame_target)
            {
                Self::flush_merge_chunk(
                    &mut writer,
                    &mut batches,
                    &mut indices,
                    &mut bounds,
                    &mut chunk_max_row_bytes,
                    &mut inputs,
                    cancel,
                )?;
                chunk_bytes = 0;
            }
            let input = &mut inputs[index];
            let slot = if let Some(slot) = input.slot {
                slot
            } else {
                batches.push(input.batch.clone().expect("a loaded frame"));
                input.slot = Some(batches.len() - 1);
                batches.len() - 1
            };
            indices.push((slot, input.row));
            chunk_bytes = chunk_bytes
                .checked_add(row_bytes)
                .ok_or_else(|| storage("property frame byte total overflows"))?;
            chunk_max_row_bytes = chunk_max_row_bytes.max(row_bytes);
            bounds = Some(bounds.map_or((uuid, uuid), |(first, _)| (first, uuid)));
            input.row += 1;
            processed_rows = processed_rows.saturating_add(1);
            if processed_rows.is_multiple_of(CANCEL_CHECK_ROWS) {
                check_cancelled(cancel)?;
            }
            if input.row < input.end {
                heap.push(Reverse((input.current(), index)));
            } else {
                check_cancelled(cancel)?;
                if !indices.is_empty() {
                    Self::flush_merge_chunk(
                        &mut writer,
                        &mut batches,
                        &mut indices,
                        &mut bounds,
                        &mut chunk_max_row_bytes,
                        &mut inputs,
                        cancel,
                    )?;
                    chunk_bytes = 0;
                }
                batches.clear();
                if inputs[index].load(uuid_name, cancel)? {
                    heap.push(Reverse((inputs[index].current(), index)));
                }
            }
            if indices.len() == max_rows {
                Self::flush_merge_chunk(
                    &mut writer,
                    &mut batches,
                    &mut indices,
                    &mut bounds,
                    &mut chunk_max_row_bytes,
                    &mut inputs,
                    cancel,
                )?;
                chunk_bytes = 0;
            }
        }
        check_cancelled(cancel)?;
        Self::flush_chunk(
            &mut writer,
            &mut batches,
            &mut indices,
            &mut bounds,
            &mut chunk_max_row_bytes,
        )?;
        writer.finish()
    }

    fn initial_heap(
        inputs: &mut [Input<'_, '_>],
        uuid_name: &str,
        cancel: &AtomicBool,
    ) -> Result<BinaryHeap<Reverse<(Uuid, usize)>>, GfError> {
        let mut heap = BinaryHeap::with_capacity(inputs.len());
        for (index, input) in inputs.iter_mut().enumerate() {
            if input.load(uuid_name, cancel)? {
                heap.push(Reverse((input.current(), index)));
            }
        }
        Ok(heap)
    }

    pub(super) fn merge_job_cost(
        &self,
        runs: &[&Run],
        lower: Option<Uuid>,
        upper: Option<Uuid>,
    ) -> u64 {
        let mut retained = 0_u64;
        let mut decode_peak = 0_u64;
        let mut max_row = 0_u64;
        let mut max_envelope = 0_u64;
        let mut max_messages = 0_u64;
        let mut max_headers = 0_u64;
        for run in runs {
            let mut run_retained = 0_u64;
            let mut run_decode = 0_u64;
            for frame in run.frames.iter().filter(|frame| {
                lower.is_none_or(|lower| frame.last >= lower)
                    && upper.is_none_or(|upper| frame.first < upper)
            }) {
                run_retained =
                    run_retained.max(frame.body_bytes.saturating_add(frame.header_bytes));
                run_decode = run_decode.max(
                    frame
                        .bytes
                        .saturating_add(frame.message_bytes.saturating_mul(2)),
                );
                max_row = max_row.max(frame.max_row_bytes);
                max_envelope = max_envelope.max(frame.output_envelope_bytes);
                max_messages = max_messages.max(frame.message_bytes);
                max_headers = max_headers.max(frame.header_bytes);
            }
            retained = retained.saturating_add(run_retained);
            decode_peak = decode_peak.max(run_decode);
        }
        let gather = (self.frame_target as u64).max(max_row);
        let encoded_output = gather.saturating_add(max_envelope);
        let decode_transient = decode_peak;
        let output_transient = gather
            .saturating_mul(2)
            .saturating_add(encoded_output.saturating_mul(4))
            .saturating_add(max_messages.saturating_mul(4))
            .saturating_add(max_headers);
        let inputs = runs.len() as u64;
        let indices = (self.budgets.max_batch_rows as u64)
            .saturating_mul(std::mem::size_of::<(usize, usize)>() as u64);
        let per_input = (std::mem::size_of::<Input<'_, '_>>() as u64)
            .saturating_add(2 * std::mem::size_of::<Reverse<(Uuid, usize)>>() as u64)
            .saturating_add(2 * std::mem::size_of::<&RecordBatch>() as u64);
        let job_headers = (1_u64 << 20)
            .saturating_add(indices)
            .saturating_add(inputs.saturating_mul(per_input));
        retained
            .saturating_add(decode_transient.max(output_transient))
            .saturating_add(job_headers)
    }

    fn flush_chunk(
        writer: &mut super::property_rows::RunWriter<'_, '_>,
        batches: &mut Vec<RecordBatch>,
        indices: &mut Vec<(usize, usize)>,
        bounds: &mut Option<(Uuid, Uuid)>,
        max_row_bytes: &mut usize,
    ) -> Result<(), GfError> {
        if let Some((first, last)) = bounds.take() {
            let refs = batches.iter().collect::<Vec<_>>();
            let frame = gather_record_batch(&refs, indices).map_err(storage)?;
            writer.append(&frame, first, last, *max_row_bytes)?;
        }
        batches.clear();
        indices.clear();
        *max_row_bytes = 0;
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

    /// Reduce every group until both its nominal fan-in and whole-job byte
    /// reservation fit. A count-only fan-in is insufficient when a run has a
    /// row much larger than the usual frame target.
    fn reduce(&self, groups: &mut [Vec<Run>], cancel: &AtomicBool) -> Result<(), GfError> {
        let fan_in = self.sizing.fan_in.max(2);
        loop {
            let mut jobs = Vec::new();
            for (group, runs) in groups.iter_mut().enumerate() {
                if runs.len() < 2 {
                    continue;
                }
                let full = runs.iter().collect::<Vec<_>>();
                let whole_cost = self.merge_job_cost(&full, None, None);
                if runs.len() <= fan_in && whole_cost <= self.merge_budget_bytes() {
                    continue;
                }
                // Prefer runs with the smallest actual frame reservation; the
                // path breaks ties so the choice is stable.
                runs.sort_by(|left, right| {
                    let left_cost = self.merge_job_cost(&[left], None, None);
                    let right_cost = self.merge_job_cost(&[right], None, None);
                    (left_cost, &left.path).cmp(&(right_cost, &right.path))
                });
                let mut pending = std::mem::take(runs).into_iter().collect::<VecDeque<_>>();
                let excess = pending.len().saturating_sub(fan_in);
                let mut target_inputs = excess + excess.div_ceil(fan_in - 1);
                if target_inputs > pending.len() {
                    target_inputs = pending.len();
                }
                if excess == 0 {
                    target_inputs = pending.len();
                }
                let mut selected = 0;
                while selected < target_inputs {
                    let Some(first) = pending.pop_front() else {
                        break;
                    };
                    selected += 1;
                    let mut inputs = vec![first];
                    while inputs.len() < fan_in && selected < target_inputs {
                        let Some(candidate) = pending.pop_front() else {
                            break;
                        };
                        let mut refs = inputs.iter().collect::<Vec<_>>();
                        refs.push(&candidate);
                        if self.merge_job_cost(&refs, None, None) > self.merge_budget_bytes() {
                            pending.push_front(candidate);
                            break;
                        }
                        inputs.push(candidate);
                        selected += 1;
                    }
                    if inputs.len() == 1 {
                        runs.extend(inputs);
                    } else {
                        jobs.push((group, inputs));
                    }
                }
                runs.extend(pending);
            }
            if jobs.is_empty() {
                if groups.iter().any(|runs| {
                    runs.len() > fan_in
                        || (runs.len() >= 2
                            && self.merge_job_cost(&runs.iter().collect::<Vec<_>>(), None, None)
                                > self.merge_budget_bytes())
                }) {
                    return Err(GfError::Project {
                        code: graphforge_core::ProjectErrorCode::ResourceLimit,
                        message: "no pair of property runs fits the validated merge workspace"
                            .into(),
                    });
                }
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

    /// Reduce each schema group until fan-in and whole-job byte limits fit,
    /// then merge it into identity-range segments in parallel.
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
