//! UUID range partitioning of scratch records (#1900, #1929).
//!
//! Edge records and node records both scatter into UUID range partitions. The
//! boundaries come from the tasks' footer bounds, or from a sample of tasks.
//! The scatter observes each partition's real bounds, and a partition that
//! does not fit a worker's reservation splits by radix steps that read it once
//! and write its children once. The result is an ordered inventory of leaves
//! plus the smallest UUID of each, which routes later records to their leaf.

use std::path::PathBuf;
use std::sync::atomic::AtomicBool;

use rayon::prelude::*;

use super::budget::ScratchPlan;
use super::scratch::{Appender, Partitions, Scatter, Scratch, read_blocks};
use super::tables::{Tasks, check_cancelled};
use super::{BulkSource, GfError, storage};

// ------------------------------------------------------------- splitters

/// Histogram resolution of the footer-bound splitters, as a power of two.
const BOUND_BUCKET_BITS: u32 = 20;

/// Boundaries from the tasks' footer bounds, when every task states them: each
/// task's rows are spread evenly between its bounds, which is exact for UUIDs
/// that arrive in order and uniform for UUIDs that do not. The histogram spans
/// the smallest to the largest bound, not the whole UUID space: time-ordered
/// identities share their leading bytes.
#[allow(clippy::cast_precision_loss)] // row counts are estimates here
fn splitters_from_bounds(
    sources: &[BulkSource<'_>],
    tasks: &Tasks,
    wanted: usize,
) -> Option<Vec<[u8; 16]>> {
    let mut stated = Vec::with_capacity(tasks.items.len());
    for &(source, task, rows) in &tasks.items {
        if rows > 0 {
            let (low, high) = sources[source].reader.uuid_bounds(task)?;
            let (low, high) = (u128::from_be_bytes(low), u128::from_be_bytes(high));
            if high < low {
                return None;
            }
            stated.push((rows as f64, low, high));
        }
    }
    let origin = stated.iter().map(|(_, low, _)| *low).min()?;
    let end = stated.iter().map(|(_, _, high)| *high).max()?;
    // Buckets of `1 << shift` UUIDs, at most `1 << BOUND_BUCKET_BITS` of them.
    let shift = (128 - (end - origin).leading_zeros()).saturating_sub(BOUND_BUCKET_BITS);
    let buckets = usize::try_from((end - origin) >> shift).ok()? + 1;
    let bucket = |value: u128| usize::try_from((value - origin) >> shift).ok();
    let mut slope = vec![0.0_f64; buckets + 1];
    let mut total = 0.0_f64;
    for (rows, low, high) in stated {
        let (first, last) = (bucket(low)?, bucket(high)?);
        let rate = rows / (last - first + 1) as f64;
        slope[first] += rate;
        slope[last + 1] -= rate;
        total += rows;
    }
    let mut splitters = Vec::new();
    let (mut rate, mut cumulative, mut next) = (0.0_f64, 0.0_f64, 1_usize);
    for (index, change) in slope.iter().take(buckets).enumerate() {
        rate += change;
        cumulative += rate;
        while next < wanted && cumulative >= total * next as f64 / wanted as f64 {
            // Everything up to this bucket sorts below the boundary.
            if index + 1 < buckets {
                splitters.push((origin + (((index + 1) as u128) << shift)).to_be_bytes());
            }
            next += 1;
        }
    }
    splitters.dedup();
    Some(splitters)
}

/// UUID range boundaries that split the rows of `column` into about `wanted`
/// partitions of equal size.
///
/// They come from the row-group bounds in the footers when the source states
/// them. Otherwise (Arrow files, or UUIDs with nulls, which are derived) from a
/// sample of evenly spread tasks: the minimum and maximum of a group of
/// unordered UUIDs say nothing about how they are distributed, but a sample
/// of tasks does, and it covers sorted input too.
pub(super) fn uuid_splitters(
    sources: &[BulkSource<'_>],
    tasks: &Tasks,
    wanted: usize,
    column: &str,
    cancel: &AtomicBool,
) -> Result<Vec<[u8; 16]>, GfError> {
    if wanted <= 1 || tasks.items.is_empty() {
        return Ok(Vec::new());
    }
    if let Some(splitters) = splitters_from_bounds(sources, tasks, wanted) {
        return Ok(splitters);
    }
    let chosen_count = tasks.items.len().min(64);
    let chosen = (0..chosen_count)
        .map(|index| tasks.items[index * tasks.items.len() / chosen_count])
        .collect::<Vec<_>>();
    let chosen_rows = chosen.iter().map(|(_, _, rows)| *rows).sum::<usize>();
    // Enough samples per partition for a balanced split, never the whole input.
    let step = (chosen_rows / (wanted * 256).max(16_384)).max(1);
    let mut sample = chosen
        .par_iter()
        .map(|&(source, task, _)| {
            check_cancelled(cancel)?;
            let mut uuids = Vec::new();
            let mut seen = 0_usize;
            sources[source].reader.read_task(task, &mut |batch| {
                let identities = crate::graph_construction::batch_uuid_column(&batch, column)?;
                for row in 0..batch.num_rows() {
                    if seen.is_multiple_of(step) {
                        uuids.push(
                            <[u8; 16]>::try_from(identities.value(row)).expect("16-byte identity"),
                        );
                    }
                    seen += 1;
                }
                Ok(())
            })?;
            Ok(uuids)
        })
        .collect::<Result<Vec<_>, GfError>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    if sample.is_empty() {
        return Ok(Vec::new());
    }
    sample.par_sort_unstable();
    let mut splitters = (1..wanted)
        .map(|part| sample[part * sample.len() / wanted])
        .collect::<Vec<_>>();
    splitters.dedup();
    Ok(splitters)
}

pub(super) fn partition_of(splitters: &[[u8; 16]], uuid: &[u8; 16]) -> usize {
    splitters.partition_point(|splitter| splitter <= uuid)
}

// -------------------------------------------------------- skew refinement

pub(super) type UuidBounds = Option<([u8; 16], [u8; 16])>;

pub(super) fn observe(bounds: &mut UuidBounds, uuid: [u8; 16]) {
    *bounds = Some(bounds.map_or((uuid, uuid), |(low, high)| (low.min(uuid), high.max(uuid))));
}

/// Only oversized UUID ranges take extra scratch passes. A radix step splits
/// at the first differing byte of the observed bounds, skipping common UUID
/// prefixes without rereading them. Equal UUIDs always follow the same leaf.
struct Refinement<'a> {
    scratch: &'a Scratch,
    spec: RangeSpec,
    limit: u64,
    staging_bytes: usize,
    cancel: &'a AtomicBool,
    steps: u64,
    leaves: Vec<(PathBuf, u64)>,
    /// The smallest UUID of each leaf, in leaf order; `None` for an empty one.
    lows: Vec<Option<[u8; 16]>>,
    pending: Option<PendingLeaf<'a>>,
    outputs: u64,
}

struct PendingLeaf<'a> {
    writer: Appender<'a>,
    path: PathBuf,
    rows: u64,
    low: Option<[u8; 16]>,
}

/// What a set of range partitions holds: fixed-width records that start with
/// a 16-byte UUID.
#[derive(Clone, Copy)]
pub(super) struct RangeSpec {
    /// Bytes per record.
    pub(super) width: usize,
    /// Bytes a worker reserves per record while it builds a leaf; the bound on
    /// a leaf's rows follows from it.
    pub(super) row_bytes: u64,
    /// The kind of identity the records name, for refusals.
    pub(super) what: &'static str,
    /// Scratch file prefix, unique to the record set.
    pub(super) prefix: &'static str,
    /// Failpoint hit after every block a refinement reads.
    pub(super) failpoint: &'static str,
}

impl Refinement<'_> {
    fn finish_pending(&mut self) -> Result<(), GfError> {
        if let Some(PendingLeaf {
            writer,
            path,
            rows,
            low,
        }) = self.pending.take()
        {
            writer.finish()?;
            self.leaves.push((path, rows));
            self.lows.push(low);
        }
        Ok(())
    }

    /// Greedily pack adjacent radix leaves without ever retaining their path
    /// inventory. One open output consumes each tiny leaf once; it never
    /// recopies the accumulated output when a subsequent leaf arrives.
    fn leaf(
        &mut self,
        path: PathBuf,
        rows: u64,
        low: Option<[u8; 16]>,
        coalesce: bool,
    ) -> Result<(), GfError> {
        if !coalesce {
            self.finish_pending()?;
            self.leaves.push((path, rows));
            self.lows.push(low);
            return Ok(());
        }
        if self
            .pending
            .as_ref()
            .is_some_and(|pending| pending.rows + rows > self.limit)
        {
            self.finish_pending()?;
        }
        if rows == self.limit && self.pending.is_none() {
            self.leaves.push((path, rows));
            self.lows.push(low);
            return Ok(());
        }
        if self.pending.is_none() {
            let output = self.scratch.file(&format!(
                "{}-coalesced-{:06}.blocks",
                self.spec.prefix, self.outputs
            ));
            self.outputs += 1;
            self.pending = Some(PendingLeaf {
                writer: Appender::create(self.scratch, &output, self.staging_bytes)?,
                path: output,
                rows: 0,
                low,
            });
        }
        let pending = self.pending.as_mut().expect("an output is open");
        let (width, what) = (self.spec.width, self.spec.what);
        let mut copied = 0_u64;
        read_blocks(self.scratch, &path, |payload| {
            check_cancelled(self.cancel)?;
            if !payload.len().is_multiple_of(width) {
                return Err(storage(format!(
                    "a {what} scratch block has a partial record"
                )));
            }
            for record in payload.chunks_exact(width) {
                pending.writer.push(record)?;
                copied += 1;
            }
            Ok(())
        })?;
        if copied != rows {
            return Err(storage(format!("a {what} scratch partition lost records")));
        }
        pending.rows += copied;
        std::fs::remove_file(path).map_err(storage)?;
        if pending.rows == self.limit {
            self.finish_pending()?;
        }
        Ok(())
    }

    fn partition(
        &mut self,
        path: PathBuf,
        rows: u64,
        bounds: UuidBounds,
        coalesce: bool,
    ) -> Result<(), GfError> {
        check_cancelled(self.cancel)?;
        if rows <= self.limit {
            return self.leaf(path, rows, bounds.map(|(low, _)| low), coalesce);
        }
        let (width, what) = (self.spec.width, self.spec.what);
        let (low, high) =
            bounds.ok_or_else(|| storage(format!("a {what} partition lost its UUID bounds")))?;
        let Some(byte) = low.iter().zip(high).position(|(low, high)| *low != high) else {
            return Err(storage(format!(
                "duplicate identity across construction runs ({what})"
            )));
        };
        let prefix = format!("{}-refinement-{:06}", self.spec.prefix, self.steps);
        self.steps += 1;
        let children = Partitions::create(self.scratch, &prefix, 256, width)?;
        let mut scatter = Scatter::new(self.scratch, &children, self.staging_bytes);
        let mut bounds = vec![None; 256];
        let mut read = 0_u64;
        read_blocks(self.scratch, &path, |payload| {
            check_cancelled(self.cancel)?;
            if !payload.len().is_multiple_of(width) {
                return Err(storage(format!(
                    "a {what} scratch block has a partial record"
                )));
            }
            for bytes in payload.chunks_exact(width) {
                let uuid: [u8; 16] = bytes[..16].try_into().expect("16-byte identity");
                let child = usize::from(uuid[byte]);
                observe(&mut bounds[child], uuid);
                scatter.push(child, bytes)?;
                read += 1;
            }
            crate::graph_construction::construction_failpoint(self.spec.failpoint);
            Ok(())
        })?;
        if read != rows {
            return Err(storage(format!("a {what} scratch partition lost records")));
        }
        scatter.finish()?;
        let counts = children.counts()?;
        std::fs::remove_file(path).map_err(storage)?;
        for (child, count) in counts.into_iter().enumerate() {
            let path = children.path(child).to_path_buf();
            if count == 0 {
                std::fs::remove_file(path).map_err(storage)?;
            } else {
                self.partition(path, count, bounds[child], true)?;
            }
        }
        Ok(())
    }
}

/// Return a globally UUID-ordered inventory whose nonempty leaves all fit a
/// worker's reservation. Every refinement reads a parent once and writes its
/// children once. Streaming coalescing then packs small adjacent children to
/// keep the final inventory proportional to total rows divided by the limit;
/// it does not reopen the registered input source.
pub(super) fn refine_partitions(
    partitions: &Partitions,
    bounds: &[UuidBounds],
    spec: RangeSpec,
    plan: &ScratchPlan,
    scratch: &Scratch,
    cancel: &AtomicBool,
) -> Result<Refined, GfError> {
    let written = scratch.written_bytes();
    let read = scratch.read_bytes();
    // Sequential refinement shares one total scatter allowance across all 256
    // children. Each child's capacity is rounded down to a whole record.
    let staging = usize::try_from((plan.gate_bytes / 16 / 256).max(spec.width as u64))
        .unwrap_or(spec.width)
        .min(8 << 10);
    let mut refinement = Refinement {
        scratch,
        spec,
        limit: (plan.gate_bytes / (2 * plan.concurrency as u64) / spec.row_bytes).max(1),
        staging_bytes: staging,
        cancel,
        steps: 0,
        leaves: Vec::new(),
        lows: Vec::new(),
        pending: None,
        outputs: 0,
    };
    let counts = partitions.counts()?;
    for (part, count) in counts.into_iter().enumerate() {
        refinement.partition(
            partitions.path(part).to_path_buf(),
            count,
            bounds[part],
            false,
        )?;
    }
    refinement.finish_pending()?;
    Ok(Refined {
        partitions: Partitions::from_inventory(refinement.leaves, spec.width),
        lows: refinement.lows,
        write_bytes: scratch.written_bytes() - written,
        read_bytes: scratch.read_bytes() - read,
        steps: refinement.steps,
    })
}

/// The leaves of a refined record set.
pub(super) struct Refined {
    /// Ordered leaves; every nonempty one fits a worker's reservation.
    pub(super) partitions: Partitions,
    /// The smallest UUID of each leaf, `None` for an empty one.
    pub(super) lows: Vec<Option<[u8; 16]>>,
    /// Scratch traffic the refinement added.
    pub(super) write_bytes: u64,
    pub(super) read_bytes: u64,
    pub(super) steps: u64,
}

#[cfg(test)]
mod refinement_tests {
    use super::super::scratch_edges::{EDGE_RANGES, EDGE_RECORD, EdgeRecord};
    use super::*;
    use crate::graph_construction_encoding::StableDirectory;

    /// Refine edge-shaped records, as the edge pass does.
    fn refine(
        partitions: &Partitions,
        bounds: &[UuidBounds],
        plan: &ScratchPlan,
        scratch: &Scratch,
        cancel: &AtomicBool,
    ) -> Result<(Partitions, u64, u64, u64), GfError> {
        let refined = refine_partitions(partitions, bounds, EDGE_RANGES, plan, scratch, cancel)?;
        Ok((
            refined.partitions,
            refined.write_bytes,
            refined.read_bytes,
            refined.steps,
        ))
    }

    fn initial(scratch: &Scratch, uuids: &[[u8; 16]]) -> (Partitions, Vec<UuidBounds>) {
        let partitions = Partitions::create(scratch, "initial", 1, EDGE_RECORD).unwrap();
        let mut scatter = Scatter::new(scratch, &partitions, 28 * 20);
        let mut bounds = None;
        for (index, uuid) in uuids.iter().copied().enumerate() {
            observe(&mut bounds, uuid);
            scatter
                .push(
                    0,
                    &EdgeRecord {
                        uuid,
                        src: u32::try_from(index + 1).unwrap(),
                        dst: 3,
                        rel: 7,
                    }
                    .encode(),
                )
                .unwrap();
        }
        scatter.finish().unwrap();
        (partitions, vec![bounds])
    }

    fn plan(limit: u64) -> ScratchPlan {
        ScratchPlan::sized(1, 1, 1, limit * 44 * 2, 4096)
    }

    #[test]
    fn clustered_uuids_and_outliers_refine_into_bounded_ordered_ranges() {
        let root = tempfile::tempdir().unwrap();
        let directory = StableDirectory::open(root.path()).unwrap();
        let scratch = Scratch::create(&directory).unwrap();
        let mut uuids = (0..600_u16)
            .rev()
            .map(|index| {
                let mut uuid = [0x77; 16];
                uuid[14..].copy_from_slice(&index.to_be_bytes());
                uuid
            })
            .collect::<Vec<_>>();
        uuids.extend([[0; 16], [0xff; 16]]);
        let (partitions, bounds) = initial(&scratch, &uuids);
        let original = partitions.path(0).to_path_buf();
        let cancel = AtomicBool::new(false);
        let (partitions, written, read, steps) =
            refine(&partitions, &bounds, &plan(40), &scratch, &cancel).unwrap();
        assert!(!original.exists());
        assert!(steps >= 3);
        assert!(written > 0 && read > 0);
        assert!(
            partitions
                .counts()
                .unwrap()
                .iter()
                .all(|count| *count <= 40)
        );
        let mut got = Vec::new();
        for part in 0..partitions.len() {
            let mut records = Vec::new();
            partitions
                .read(&scratch, part, |payload| {
                    records.extend(payload.chunks_exact(EDGE_RECORD).map(EdgeRecord::decode));
                    Ok(())
                })
                .unwrap();
            records.sort_unstable_by_key(|record| record.uuid);
            got.extend(
                records
                    .into_iter()
                    .map(|record| (record.uuid, record.src, record.dst, record.rel)),
            );
        }
        let mut expected = uuids
            .into_iter()
            .enumerate()
            .map(|(index, uuid)| (uuid, u32::try_from(index + 1).unwrap(), 3, 7))
            .collect::<Vec<_>>();
        expected.sort_unstable_by_key(|record| record.0);
        assert_eq!(got, expected);
        assert_eq!(scratch.read_bytes(), scratch.written_bytes());
    }

    #[test]
    fn repeated_radix_fanout_coalesces_small_children_as_they_arrive() {
        let root = tempfile::tempdir().unwrap();
        let directory = StableDirectory::open(root.path()).unwrap();
        let scratch = Scratch::create(&directory).unwrap();
        let mut uuids = Vec::new();
        // Every group is one row over the limit, yet a byte split yields
        // 255 singleton outliers and one two-row child.
        for group in 0..8_u8 {
            for child in 0..=255_u8 {
                let mut uuid = [0x66; 16];
                uuid[1] = group;
                uuid[2] = child;
                uuid[15] = 0;
                uuids.push(uuid);
                if child == 0 {
                    uuid[15] = 1;
                    uuids.push(uuid);
                }
            }
        }
        let expected = uuids
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        let rows = uuids.len() as u64;
        let (partitions, bounds) = initial(&scratch, &uuids);
        let cancel = AtomicBool::new(false);
        let (partitions, _, _, _) =
            refine(&partitions, &bounds, &plan(256), &scratch, &cancel).unwrap();
        assert!(partitions.len() as u64 <= 1 + 2 * rows.div_ceil(256));
        assert!(
            partitions
                .counts()
                .unwrap()
                .iter()
                .all(|count| *count <= 256)
        );
        let mut got = Vec::new();
        for part in 0..partitions.len() {
            let mut local = Vec::new();
            partitions
                .read(&scratch, part, |payload| {
                    local.extend(
                        payload
                            .chunks_exact(EDGE_RECORD)
                            .map(|bytes| EdgeRecord::decode(bytes).uuid),
                    );
                    Ok(())
                })
                .unwrap();
            local.sort_unstable();
            got.extend(local);
        }
        assert_eq!(got, expected.into_iter().collect::<Vec<_>>());
        assert_eq!(scratch.read_bytes(), scratch.written_bytes());
    }

    #[test]
    fn a_shared_fifteen_byte_prefix_is_skipped_in_one_refinement() {
        let root = tempfile::tempdir().unwrap();
        let directory = StableDirectory::open(root.path()).unwrap();
        let scratch = Scratch::create(&directory).unwrap();
        let uuids = (0..200_u8)
            .map(|last| {
                let mut uuid = [0x44; 16];
                uuid[15] = last;
                uuid
            })
            .collect::<Vec<_>>();
        let (partitions, bounds) = initial(&scratch, &uuids);
        let cancel = AtomicBool::new(false);
        let (_, _, _, steps) = refine(&partitions, &bounds, &plan(10), &scratch, &cancel).unwrap();
        assert_eq!(steps, 1);
    }

    #[test]
    fn refinement_checks_the_parent_block_crc_before_using_records() {
        let root = tempfile::tempdir().unwrap();
        let directory = StableDirectory::open(root.path()).unwrap();
        let scratch = Scratch::create(&directory).unwrap();
        let uuids = (0..100_u8)
            .map(|last| {
                let mut uuid = [0x44; 16];
                uuid[15] = last;
                uuid
            })
            .collect::<Vec<_>>();
        let (partitions, bounds) = initial(&scratch, &uuids);
        let path = partitions.path(0);
        let mut bytes = std::fs::read(path).unwrap();
        bytes[8] ^= 1;
        std::fs::write(path, bytes).unwrap();
        let cancel = AtomicBool::new(false);
        let error = refine(&partitions, &bounds, &plan(10), &scratch, &cancel)
            .err()
            .unwrap();
        assert!(error.to_string().contains("CRC32C"), "{error}");
    }

    #[test]
    fn an_oversized_equal_uuid_range_is_refused_without_recursion() {
        let root = tempfile::tempdir().unwrap();
        let directory = StableDirectory::open(root.path()).unwrap();
        let scratch = Scratch::create(&directory).unwrap();
        let (partitions, bounds) = initial(&scratch, &[[0x44; 16]; 100]);
        let cancel = AtomicBool::new(false);
        let error = refine(&partitions, &bounds, &plan(10), &scratch, &cancel)
            .err()
            .unwrap();
        assert!(error.to_string().contains("duplicate identity"), "{error}");
    }
}
