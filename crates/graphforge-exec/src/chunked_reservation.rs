//! A memory reservation that touches the shared pool once per chunk, not once
//! per charged item (#1985).
//!
//! A [`MemoryReservation`] calls into the pool on every `try_grow` and
//! `shrink`. The pool serialises those calls behind one lock, so a consumer that
//! charges per path state or per reached row makes every partition of a parallel
//! operator queue on it: the expansion CPU grew 2x once each input partition
//! owned a reservation. [`ChunkedReservation`] charges the pool in chunks and
//! tracks the exact usage locally, so the pool sees O(bytes / chunk) calls
//! while the consumer still accounts every byte.
//!
//! The accounting stays exact where it matters:
//!
//! - `used` is the exact sum of the charges not yet released; the pool holds
//!   `used` plus a slack that is never negative.
//! - A growth rounds up to a chunk. When the pool refuses the chunk the request
//!   falls back to the exact missing bytes, so a query that really needs more
//!   than the budget still fails, with the pool's own typed error, at the limit.
//!   The slack other partitions hold can bring that failure forward by at most
//!   [`MAX_SLACK_CHUNKS`] chunks per partition.
//! - A release gives the surplus back once the slack exceeds
//!   [`MAX_SLACK_CHUNKS`] chunks, down to one chunk. A charge and a release of
//!   the same few bytes (the BFS queue's push and pop) therefore never reach
//!   the pool.
//! - Dropping the reservation returns everything it holds.

use datafusion::error::Result;
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use std::sync::Arc;

/// Smallest chunk: bounds the pool calls of a small budget.
const MIN_CHUNK_BYTES: usize = 16 * 1024;
/// Largest chunk: bounds the slack a partition holds against a large budget.
const MAX_CHUNK_BYTES: usize = 1024 * 1024;
/// A budget is split into at least this many chunks, so that a partition's
/// slack stays a small share of it.
const CHUNKS_PER_BUDGET: usize = 512;
/// The slack, in chunks, above which a release gives bytes back to the pool.
const MAX_SLACK_CHUNKS: usize = 2;

/// A non-spillable reservation charged to the pool in chunks. See the module
/// documentation.
#[derive(Debug)]
pub(crate) struct ChunkedReservation {
    inner: MemoryReservation,
    /// Exact bytes charged and not yet released.
    used: usize,
    chunk: usize,
    /// The pool refused a chunk at `tight_mark` bytes of usage: charge exactly
    /// until usage falls a chunk below it, so a budget that is nearly spent is
    /// not asked for chunks it will refuse.
    tight_mark: Option<usize>,
}

impl ChunkedReservation {
    /// Register a non-spillable consumer named `name` with `pool`, with the
    /// chunk sized from the pool's budget.
    pub(crate) fn register(name: &'static str, pool: &Arc<dyn MemoryPool>) -> Self {
        let inner = MemoryConsumer::new(name)
            .with_can_spill(false)
            .register(pool);
        Self::new(inner, chunk_for(&pool.memory_limit()))
    }

    fn new(inner: MemoryReservation, chunk: usize) -> Self {
        Self {
            inner,
            used: 0,
            chunk,
            tight_mark: None,
        }
    }

    /// Charge `bytes`. Fails, with the pool's error, only when the pool cannot
    /// hold the exact usage; the failed charge is not recorded.
    pub(crate) fn try_grow(&mut self, bytes: usize) -> Result<()> {
        let needed = self.used.saturating_add(bytes);
        let held = self.inner.size();
        if needed <= held {
            self.used = needed;
            return Ok(());
        }
        let missing = needed - held;
        if self.tight_mark.is_none() {
            // Round the request up to whole chunks.
            let ask = missing.div_ceil(self.chunk).saturating_mul(self.chunk);
            if self.inner.try_grow(ask).is_ok() {
                self.used = needed;
                return Ok(());
            }
            self.tight_mark = Some(self.used);
        }
        self.inner.try_grow(missing)?;
        self.used = needed;
        Ok(())
    }

    /// Release `bytes` previously charged.
    pub(crate) fn shrink(&mut self, bytes: usize) {
        debug_assert!(
            bytes <= self.used,
            "released {bytes} bytes of a {} byte charge",
            self.used
        );
        self.used = self.used.saturating_sub(bytes);
        if let Some(mark) = self.tight_mark
            && self.used.saturating_add(self.chunk) <= mark
        {
            self.tight_mark = None;
        }
        let slack = self.inner.size() - self.used;
        if slack > MAX_SLACK_CHUNKS.saturating_mul(self.chunk) {
            self.inner.shrink(slack - self.chunk);
        }
    }
}

/// The chunk for a pool with `limit`: a small share of a finite budget, between
/// [`MIN_CHUNK_BYTES`] and [`MAX_CHUNK_BYTES`].
fn chunk_for(limit: &MemoryLimit) -> usize {
    match limit {
        MemoryLimit::Finite(bytes) => {
            (bytes / CHUNKS_PER_BUDGET).clamp(MIN_CHUNK_BYTES, MAX_CHUNK_BYTES)
        }
        MemoryLimit::Infinite | MemoryLimit::Unknown => MAX_CHUNK_BYTES,
    }
}

#[cfg(test)]
mod tests;
