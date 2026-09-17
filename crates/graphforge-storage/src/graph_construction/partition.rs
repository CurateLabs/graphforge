//! Range partitioning over the construction identity sort key.
//!
//! Shaping sorts every staged identity, detail, endpoint and row record into
//! one global UUID order. A *range* partition over that key has the property
//! that `concat(partition_0, .., partition_{P-1})` is already globally sorted,
//! so the external merge tree that used to produce that order is unnecessary:
//! each partition is sorted independently and the outputs are concatenated.
//!
//! # Why the splitters are sampled and not computed
//!
//! The obvious range partition is a formula over the high bits of the key,
//! `p = be_u128(uuid) >> (128 - log2(P))`. That is wrong here, and the reason
//! is recorded in `graphforge-core/src/uuid.rs`: every write path mints
//! **UUIDv7**, whose 48 most-significant bits are a Unix-millisecond timestamp.
//! Every identity minted during one ingest therefore shares a near-identical
//! 48-bit prefix, and a high-bit formula would route essentially every row into
//! a single partition. User-supplied UUIDs arriving through the import path may
//! be distributed differently again, which makes any fixed formula wrong in
//! both directions.
//!
//! So the splitters are **data**: a deterministic systematic sample of the
//! staged identity domain is sorted and cut at even quantiles, and the chosen
//! splitters are recorded in the shape intent before any partition is written.
//! Determinism comes from the splitters being recorded, not from a formula. A
//! resumed or re-run import reuses the recorded splitters and partitions
//! identically.
//!
//! This must not be "simplified" back into a formula later. A collapsed,
//! one-partition run is perfectly deterministic and passes every byte-equality
//! test, so determinism tests cannot catch the regression. [`PartitionBalance`]
//! is what catches it, and it is load-bearing rather than a nicety.

use super::storage;
use graphforge_core::GfError;

/// Number of range partitions requested by a construction session.
///
/// This is a recorded format parameter, never derived from the machine: it must
/// not depend on `available_parallelism()` or on thread count, or the same
/// logical input would stage differently on differently-sized hosts.
///
/// This is also the floor of the data-driven scaling target below
/// [`TARGET_ROWS_PER_PARTITION`] (#1439): below that pivot, the effective cut
/// is exactly this value, matching the operating point already proven at
/// production's largest ladder rung before the scaling target engages.
pub(crate) const DEFAULT_PARTITION_COUNT: u32 = 256;

/// Upper bound on the recorded partition count.
pub(crate) const MAX_PARTITION_COUNT: u32 = 4_096;

/// Target rows materialized per partition once the data-driven scaling term
/// is the binding constraint (#1439).
///
/// Each partition is materialized whole in memory and sorted, so resident
/// partition memory is proportional to rows-per-partition, not to the
/// partition *count*. A cut bounded only by a flat partition-count ceiling
/// (the pre-#1439 behaviour) holds partition count constant past that
/// ceiling and lets partition *size* grow with data instead, which is exactly
/// what the RSS plateau gate exists to catch.
///
/// `4_194_304 / 16_384 == 256 == DEFAULT_PARTITION_COUNT`: this constant is
/// chosen so the scaling term reproduces the already-proven 256-partition
/// operating point at that record count and grows partitions proportionally
/// with data beyond it, reaching exactly `MAX_PARTITION_COUNT` (4,096) at
/// 67,108,864 records.
const TARGET_ROWS_PER_PARTITION: u64 = 16_384;

/// Sample points drawn per requested partition. Oversampling the quantile
/// estimate is what keeps the partitions close to balanced; 64 points per
/// partition is the usual sample-sort factor.
const SAMPLE_POINTS_PER_PARTITION: u64 = 64;

/// Hard ceiling on retained sample points, so sampling stays O(1) in memory.
const MAX_SAMPLE_POINTS: u64 = 1 << 20;

/// No partition may hold more than `BALANCE_TOLERANCE` times the mean.
pub(crate) const BALANCE_TOLERANCE: u64 = 4;

/// Balance is only meaningful once a partition could hold this many rows on
/// average. Below it the quantiles are estimated from fewer points than there
/// are partitions, the splitter set collapses by deduplication, and a "max is
/// far above the mean" reading carries no information.
const BALANCE_MIN_MEAN_ROWS: u64 = 16;

/// Never cut more partitions than the balance check can meaningfully assert on.
///
/// Each partition costs a durable spill artifact, with its own barrier and
/// writer receipt, in every family. Cutting the full recorded count regardless
/// of input size makes that cost a constant: a two-thousand-row graph would pay
/// the same durability price as a sixty-seven-million-row one, and shaping's
/// barrier count would stop tracking the work it protects. Bounding the cut by
/// the recorded record count keeps the cost proportional.
///
/// This does not make the partition count machine-dependent, which is the thing
/// R1 forbids. The bound is a pure function of the staged record count recorded
/// in the chunk receipts, so the same logical input cuts the same partitions on
/// any host. This is a *small*-scale guard only: it is the binding constraint
/// far below [`DEFAULT_PARTITION_COUNT`] records, and does nothing once the
/// data-driven [`TARGET_ROWS_PER_PARTITION`] term takes over (#1439).
const MIN_ROWS_PER_PARTITION: u64 = BALANCE_MIN_MEAN_ROWS;

/// The recorded range-partition authority.
///
/// `splitters` is strictly increasing and holds `partitions() - 1` entries.
/// Partition `p` owns every key `k` with
/// `splitters[p - 1] <= k < splitters[p]`, taking the missing bounds as
/// unbounded. The mapping is monotone, which is the whole point: concatenating
/// the partitions in index order reproduces the global sort order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PartitionPlan {
    partition_count: u32,
    splitters: Vec<[u8; 16]>,
}

impl PartitionPlan {
    /// The single-partition plan. Shaping degenerates to the sequential sort it
    /// always was, which is the correct behaviour for an empty identity domain.
    pub(crate) fn single(partition_count: u32) -> Self {
        Self {
            partition_count,
            splitters: Vec::new(),
        }
    }

    /// Rebuild a plan from durably recorded splitters.
    ///
    /// # Errors
    /// Returns an error when the recorded partition count is out of range or
    /// the recorded splitters are not strictly increasing, because either would
    /// mean the recorded authority no longer describes a range partition.
    pub(crate) fn from_recorded(
        partition_count: u32,
        splitters: Vec<[u8; 16]>,
    ) -> Result<Self, GfError> {
        validate_partition_count(partition_count)?;
        if splitters.len() >= partition_count as usize {
            return Err(storage("recorded splitters exceed the partition count"));
        }
        if splitters.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(storage("recorded splitters are not strictly increasing"));
        }
        Ok(Self {
            partition_count,
            splitters,
        })
    }

    /// Choose splitters at even quantiles of a sorted sample.
    ///
    /// The sample must already be sorted. Duplicate quantiles collapse, so an
    /// input with fewer distinct keys than requested partitions simply yields
    /// fewer partitions rather than empty ones.
    ///
    /// # Errors
    /// Returns an error when the recorded partition count is out of range.
    pub(crate) fn from_sorted_sample(
        partition_count: u32,
        cut: u32,
        sample: &[[u8; 16]],
    ) -> Result<Self, GfError> {
        validate_partition_count(partition_count)?;
        if cut == 0 || cut > partition_count {
            return Err(storage("splitter cut exceeds the recorded partition count"));
        }
        if sample.is_empty() || cut == 1 {
            return Ok(Self::single(partition_count));
        }
        let length = sample.len() as u64;
        let mut splitters: Vec<[u8; 16]> = Vec::new();
        for index in 1..u64::from(cut) {
            let position = index
                .checked_mul(length)
                .ok_or_else(|| storage("splitter quantile index overflows"))?
                / u64::from(cut);
            let candidate = sample[usize::try_from(position)
                .map_err(storage)?
                .min(sample.len() - 1)];
            // Partition 0 must be reachable, so never admit the minimum key as
            // a boundary, and keep the set strictly increasing.
            if candidate <= sample[0] {
                continue;
            }
            if splitters.last().is_some_and(|last| *last >= candidate) {
                continue;
            }
            splitters.push(candidate);
        }
        Ok(Self {
            partition_count,
            splitters,
        })
    }

    /// The recorded format parameter, which is what the session requested.
    pub(crate) const fn partition_count(&self) -> u32 {
        self.partition_count
    }

    /// The effective number of partitions this plan produces.
    pub(crate) fn partitions(&self) -> usize {
        self.splitters.len() + 1
    }

    /// The recorded splitters.
    pub(crate) fn splitters(&self) -> &[[u8; 16]] {
        &self.splitters
    }

    /// The partition owning `key`.
    pub(crate) fn partition_of(&self, key: &[u8; 16]) -> usize {
        self.splitters.partition_point(|splitter| splitter <= key)
    }
}

fn validate_partition_count(partition_count: u32) -> Result<(), GfError> {
    if partition_count == 0 || partition_count > MAX_PARTITION_COUNT {
        return Err(storage("construction partition count is out of range"));
    }
    Ok(())
}

/// Deterministic systematic sampler over the staged identity domain.
///
/// The stride is a pure function of the recorded chunk receipts (the total
/// staged identity record count) and the recorded partition count, so the
/// retained sample depends only on the input, never on timing, buffer sizes or
/// the order in which files happened to be read.
///
/// Sampling is index-driven rather than streaming: the positions are known
/// before a byte is read, so the pass seeks to the sample points instead of
/// scanning the identity domain. It reads `O(partition_count)` records, not
/// `O(rows)`.
pub(crate) struct IdentitySampler {
    stride: u64,
    offset: u64,
    total: u64,
    cut: u32,
    sample: Vec<[u8; 16]>,
}

impl IdentitySampler {
    /// Build a sampler for `total_records` staged identities.
    ///
    /// # Errors
    /// Returns an error when the requested partition count is out of range.
    pub(crate) fn new(partition_count: u32, total_records: u64) -> Result<Self, GfError> {
        validate_partition_count(partition_count)?;
        // Two independent bounds, combined by `min`, so each stays a pure
        // function of `total_records` and the requested ceiling never widens
        // either of them (#1439):
        //
        // - `small_scale_floor`: below ~`DEFAULT_PARTITION_COUNT * 16` records
        //   this is the binding term, exactly as before #1439. It keeps a
        //   two-thousand-row graph from paying a large graph's durability
        //   price.
        // - `data_driven_target`: floored at `DEFAULT_PARTITION_COUNT`, so it
        //   is inactive (equal to the floor) at and below the record count
        //   that floor already covers, and grows partition count
        //   proportionally with data beyond it. This is what keeps
        //   rows-per-partition, and therefore resident partition memory,
        //   roughly constant at scale instead of letting partition count
        //   plateau while partition size keeps growing.
        //
        // The requested `partition_count` remains the hard ceiling over both.
        let small_scale_floor = (total_records / MIN_ROWS_PER_PARTITION).max(1);
        let data_driven_target = (total_records / TARGET_ROWS_PER_PARTITION)
            .max(u64::from(DEFAULT_PARTITION_COUNT));
        let cut = u32::try_from(
            u64::from(partition_count).min(small_scale_floor.min(data_driven_target)),
        )
        .map_err(storage)?;
        let target = u64::from(cut)
            .saturating_mul(SAMPLE_POINTS_PER_PARTITION)
            .clamp(1, MAX_SAMPLE_POINTS);
        let stride = total_records.div_ceil(target).max(1);
        // Sample the middle of each stride window rather than its first row, so
        // a stride that happens to align with chunk boundaries does not bias
        // the sample toward chunk minima.
        let offset = stride / 2;
        Ok(Self {
            stride,
            offset,
            total: total_records,
            cut,
            sample: Vec::new(),
        })
    }

    /// Global record indices to sample, ascending.
    pub(crate) fn positions(&self) -> impl Iterator<Item = u64> + use<> {
        let (offset, stride, total) = (self.offset, self.stride, self.total);
        (0..)
            .map(move |step| offset + step * stride)
            .take_while(move |index| *index < total)
    }

    /// Accept one sampled identity key.
    ///
    /// # Errors
    /// Returns an error when more keys are admitted than positions exist.
    pub(crate) fn admit(&mut self, key: [u8; 16]) -> Result<(), GfError> {
        if self.sample.len() as u64 >= self.total {
            return Err(storage("identity sample exceeds the staged domain"));
        }
        self.sample.push(key);
        Ok(())
    }

    /// Sort the retained sample and cut it into a plan.
    ///
    /// # Errors
    /// Returns an error when the requested partition count is out of range.
    pub(crate) fn into_plan(mut self, partition_count: u32) -> Result<PartitionPlan, GfError> {
        self.sample.sort_unstable();
        PartitionPlan::from_sorted_sample(partition_count, self.cut, &self.sample)
    }

    /// The number of partitions this sample will be cut into, bounded below by
    /// [`MIN_ROWS_PER_PARTITION`] rows per partition. The outcome is visible in
    /// production as `shape_partitions`; this exposes the bound itself so the
    /// unit tests can pin it directly.
    #[cfg(test)]
    pub(crate) const fn cut(&self) -> u32 {
        self.cut
    }

    /// Number of retained sample points, for evidence.
    pub(crate) fn sampled_records(&self) -> u64 {
        self.sample.len() as u64
    }

    /// Size of the staged identity domain the stride was derived from.
    pub(crate) const fn source_records(&self) -> u64 {
        self.total
    }
}

/// Measured row counts per partition.
///
/// A catastrophically skewed partitioning is indistinguishable from success
/// under any byte-equality determinism test, because a one-partition run is
/// perfectly deterministic. This is the check that tells them apart.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct PartitionBalance {
    rows: Vec<u64>,
}

impl PartitionBalance {
    /// An all-zero balance over `partitions` partitions.
    pub(crate) fn new(partitions: usize) -> Self {
        Self {
            rows: vec![0; partitions],
        }
    }

    /// Charge one row to `partition`.
    ///
    /// # Errors
    /// Returns an error when the partition is out of range or the count
    /// overflows.
    pub(crate) fn record(&mut self, partition: usize) -> Result<(), GfError> {
        let slot = self
            .rows
            .get_mut(partition)
            .ok_or_else(|| storage("partition index is out of range"))?;
        *slot = slot
            .checked_add(1)
            .ok_or_else(|| storage("partition row count overflows"))?;
        Ok(())
    }

    /// Per-partition row counts, in partition order.
    pub(crate) fn rows(&self) -> &[u64] {
        &self.rows
    }

    /// Total routed rows.
    pub(crate) fn total(&self) -> u64 {
        self.rows.iter().copied().sum()
    }

    /// The largest partition's row count.
    pub(crate) fn max_rows(&self) -> u64 {
        self.rows.iter().copied().max().unwrap_or(0)
    }

    /// Refuse a partitioning whose largest partition exceeds
    /// [`BALANCE_TOLERANCE`] times the mean.
    ///
    /// The check is skipped below [`BALANCE_MIN_MEAN_ROWS`] rows per partition,
    /// where the quantile estimate has fewer points than partitions and the
    /// ratio carries no information.
    ///
    /// # Errors
    /// Returns an error when the partitioning is skewed beyond the tolerance.
    pub(crate) fn assert_balanced(&self, context: &str) -> Result<(), GfError> {
        let partitions = self.rows.len() as u64;
        if partitions == 0 {
            return Err(storage("partition balance has no partitions"));
        }
        let total = self.total();
        if total / partitions < BALANCE_MIN_MEAN_ROWS {
            return Ok(());
        }
        let max = self.max_rows();
        let scaled = max
            .checked_mul(partitions)
            .ok_or_else(|| storage("partition balance ratio overflows"))?;
        let budget = total
            .checked_mul(BALANCE_TOLERANCE)
            .ok_or_else(|| storage("partition balance budget overflows"))?;
        if scaled > budget {
            return Err(storage(format!(
                "{context} range partitioning is skewed: largest partition holds {max} of \
                 {total} rows across {partitions} partitions, above the {BALANCE_TOLERANCE}x \
                 mean tolerance"
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
