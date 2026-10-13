//! Key-range cut points for partitioned property scans (#1984).
//!
//! A scan of one property route splits the UUID key space into disjoint
//! half-open intervals, one per output partition. A key's base, mutation and
//! tombstone rows all carry the same UUID, so they fall in one interval and the
//! newest-wins merge stays correct inside each partition.
//!
//! The cuts balance rows, not key space. Every row group of every fragment
//! contributes its row count over the UUID span its footer statistics declare,
//! and each cut is placed where the cumulative row count reaches its share.
//! Footers are already authenticated and memoized for the scan itself, so
//! choosing cuts opens no data page. Any cuts are correct; skewed cuts cost
//! only balance.

use super::selective_reads::{column_leaf, uuid_bounds};
use super::{AuthenticatedPropertyInventory, GfError, PropertyRouteKind};
use crate::uuid_set::UuidInterval;

#[cfg(test)]
mod tests;

/// The rows a span of the key space holds, assumed uniform within the span.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct KeySegment {
    pub(super) low: u128,
    pub(super) high: u128,
    pub(super) rows: u64,
}

impl KeySegment {
    /// The expected rows of this segment strictly below `limit`.
    #[allow(
        clippy::cast_precision_loss,
        reason = "cut placement is a balance estimate; any cut is correct"
    )]
    fn rows_below(&self, limit: u128) -> f64 {
        if limit <= self.low {
            0.0
        } else if limit > self.high {
            self.rows as f64
        } else {
            // `high - low + 1` can reach 2^128, so the width is formed in f64.
            let width = (self.high - self.low) as f64 + 1.0;
            self.rows as f64 * ((limit - self.low) as f64 / width)
        }
    }
}

impl AuthenticatedPropertyInventory {
    /// Up to `partitions - 1` strictly ascending, non-zero cut points that
    /// split the route's key space into row-balanced intervals. Fewer cuts mean
    /// the route's rows do not separate further (a single key, no statistics, or
    /// an empty route); the missing trailing partitions are then empty.
    pub(crate) fn route_partition_cuts(
        &self,
        kind: PropertyRouteKind,
        route: &str,
        partitions: usize,
    ) -> Result<Vec<[u8; 16]>, GfError> {
        if partitions < 2 {
            return Ok(Vec::new());
        }
        let Some(fragments) = self.routes.get(&(kind, route.to_owned())) else {
            return Ok(Vec::new());
        };
        // The same footer admission the scan performs, so a route that cannot
        // be summarized refuses here rather than being partitioned.
        self.route_summary(kind, route)?;
        let mut segments = Vec::new();
        for fragment in fragments {
            let footer = self.fragment_footer_of(fragment, kind, route)?;
            let Some(leaf) = column_leaf(&footer.metadata, kind.uuid_field()) else {
                continue;
            };
            for group in footer.metadata.row_groups() {
                let Some((low, high)) = uuid_bounds(group, leaf) else {
                    continue;
                };
                segments.push(KeySegment {
                    low: u128::from_be_bytes(low),
                    high: u128::from_be_bytes(high),
                    rows: u64::try_from(group.num_rows()).unwrap_or(0),
                });
            }
        }
        Ok(balanced_cuts(&segments, partitions))
    }
}

/// Cut points placing an equal expected row count in each of `partitions`
/// intervals. Strictly ascending and never zero, so interval `i` is
/// `cuts[i - 1]..cuts[i]` with the first starting at zero and the last
/// unbounded.
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "row shares are estimates; the cut is found by bisection over exact u128 keys"
)]
pub(super) fn balanced_cuts(segments: &[KeySegment], partitions: usize) -> Vec<[u8; 16]> {
    let total: f64 = segments.iter().map(|segment| segment.rows as f64).sum();
    if partitions < 2 || total <= 0.0 {
        return Vec::new();
    }
    let mut cuts: Vec<u128> = Vec::with_capacity(partitions - 1);
    for share in 1..partitions {
        let target = total * share as f64 / partitions as f64;
        let rows_below = |limit: u128| -> f64 {
            segments
                .iter()
                .map(|segment| segment.rows_below(limit))
                .sum()
        };
        // The smallest key with at least `target` rows below it. `rows_below`
        // is monotone in the key, so bisection is exact on integers.
        let (mut low, mut high) = (0_u128, u128::MAX);
        while low < high {
            let middle = low + (high - low) / 2;
            if rows_below(middle) >= target {
                high = middle;
            } else {
                low = middle + 1;
            }
        }
        if low > 0 && cuts.last().is_none_or(|last| low > *last) {
            cuts.push(low);
        }
    }
    cuts.into_iter().map(u128::to_be_bytes).collect()
}

/// The key interval of output partition `index` for these cuts. A partition
/// past the last cut gets an empty interval.
pub(crate) fn partition_interval(cuts: &[[u8; 16]], index: usize) -> UuidInterval {
    match index.cmp(&cuts.len()) {
        std::cmp::Ordering::Greater => ([0; 16], Some([0; 16])),
        ordering => {
            let low = index.checked_sub(1).map_or([0; 16], |prior| cuts[prior]);
            let high = (ordering == std::cmp::Ordering::Less).then(|| cuts[index]);
            (low, high)
        }
    }
}
