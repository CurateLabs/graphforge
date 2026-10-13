//! The query memory pool: DataFusion's greedy pool, except that a grouped
//! aggregate that has spilled is guaranteed the memory its spills need, and a
//! floor of memory to merge them in (#1985).
//!
//! DataFusion's grouped aggregate spills in two steps that a greedy pool can
//! starve, and each fails a query that should spill, depending on how its
//! partitions and the exchange feeding them happen to interleave.
//!
//! # The sort of a spill
//!
//! A spill emits the whole state, frees the reservation that held it, and only
//! then reserves memory to sort the emitted batch before writing it out. The
//! emitted batch lives outside the pool in between, and the sort reservation is
//! a fresh request for the memory just released (plus a slice of one batch
//! more). On a greedy pool another partition can take the released memory
//! first, and the request fails with `Failed to reserve memory for sort during
//! spill`.
//!
//! The pool therefore remembers, for each such aggregate, that it has just
//! released its state in a bulk shrink, and grants its next request that the
//! pool cannot serve (up to the size of the pool) whatever else has been
//! reserved since. The bytes already exist: the emitted batch the pool stopped
//! counting a moment earlier holds the whole state, including the groups of the
//! last input batch the released reservation never counted, and the sort only
//! reorders it. A refusal could only fail the query, while granting it lets the
//! reserved total pass the budget by at most the batches being sorted, after
//! which every other request is refused until they are released.
//!
//! The grant is a loan. The request it serves is lent to the consumer, and the
//! grant is renewed once the consumer has released that many bytes. A spill
//! does exactly that: it releases the sort memory when the sorted batch is
//! written, and next restarts its empty state, a request a sibling or a
//! repartition can refuse in the gap, and which fails the query the same way
//! because there is no state left to spill. The loan is repaid by the release
//! that precedes the request, so the restart is served too. A consumer holds at
//! most one loan, and loses it when it unregisters.
//!
//! # The merge of the spill files
//!
//! When an aggregate that has spilled finishes its input, it merges its spill
//! files. The merge reserves its read buffers one stream at a time, two batches
//! per stream. When a request is refused and fewer than the minimum number of
//! streams are chosen, it frees what it reserved and retries with one batch per
//! stream; a refusal there fails the query. A loan serves one request beyond
//! the free memory, but the merge makes two consecutive requests, so a loan
//! cannot guarantee the merge.
//!
//! The pool is full at that point when an exchange is holding the input of a
//! busy partition. DataFusion's repartition spills only when the pool refuses a
//! batch, so on a greedy pool it keeps everything up to the limit, and an idle
//! aggregate beside it, one that has already spilled and waits for its input to
//! end, cannot reserve a megabyte to merge (a skewed key distribution makes one
//! partition busy and the other idle).
//!
//! The pool therefore reserves a floor for every aggregate that has spilled,
//! from its first spill until it unregisters: the other consumers that can
//! spill are admitted only while the reserved total plus the unfilled floors of
//! the others stays within the pool, and a request that keeps the aggregate
//! within its own floor is always admitted. An exchange or a sibling that is
//! refused spills by writing, as it already does, instead of taking the memory
//! the merge will need. The floor is
//! [`SPILLER_FLOOR_BYTES`] (the reserve DataFusion's sort pre-reserves for the
//! same purpose, `sort_spill_reservation_bytes`), lowered so that all floors
//! together stay within half the pool.
//!
//! A consumer that cannot spill is not kept from the floors. A refusal fails
//! its query, whatever it asks for: the ungrouped aggregate that counts the
//! groups reserves a few bytes when the first of them arrives, and with the
//! floors kept from it that request failed while the pool had megabytes free.
//! It sees the limit of the pool alone, as on DataFusion's greedy pool, and the
//! loans do not count against it: a loan has the reserved total past the limit
//! for the length of a sort, and an ungrouped aggregate that asked for no bytes
//! at all was refused in that window (a request for no bytes is always served).
//! If it takes memory a floor or a loan was reserved from, the aggregate that
//! owns it still has it, so the pool then passes its limit by that much.
//!
//! The floor is a minimum, not an exact bound. The merge needs two streams of
//! two batches each, and a batch holds the groups of one emit: an aggregate
//! whose groups are far larger than a quarter of the floor, such as one
//! `collect(DISTINCT ...)` of tens of megabytes in one group, merges with the
//! free memory and the loan, as it did before the floor, and fails when the pool
//! has neither.
//!
//! Everything else is first-come within the pool limit, as in DataFusion's
//! greedy pool: sorts reserve their merge memory up front, and only grouped
//! aggregates, identified by name, are affected. Other consumers that can spill
//! (sorts, joins) get no floor; they are neither capped nor guaranteed here.

use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};
use std::sync::{Mutex, MutexGuard};

use datafusion::common::{DataFusionError, Result};
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation, human_readable_size,
};

/// DataFusion's name prefix for the memory consumer of a grouped hash
/// aggregate, the only spillable consumer that reserves memory to spill.
pub(crate) const AGGREGATE_CONSUMER_PREFIX: &str = "GroupedHashAggregateStream";

/// The most the pool reserves for one aggregate that has spilled, the size of
/// the reserve DataFusion's sort sets aside to merge its spills
/// (`sort_spill_reservation_bytes`).
const SPILLER_FLOOR_BYTES: usize = 10 * 1024 * 1024;

/// A greedy memory pool of `pool_size` bytes with the floors and the grant
/// described in the module documentation.
#[derive(Debug)]
pub(crate) struct QueryMemoryPool {
    pool_size: usize,
    state: Mutex<State>,
}

#[derive(Debug, Default)]
struct State {
    /// Bytes reserved by every consumer.
    reserved: usize,
    /// The grouped aggregate consumers that have spilled, by consumer id.
    spillers: HashMap<usize, Spiller>,
}

/// A grouped aggregate that has spilled.
#[derive(Debug, Clone, Copy)]
struct Spiller {
    /// Its right to a request the pool cannot serve.
    grant: Grant,
    /// Bytes the consumer holds. Reservations split off a consumer share its
    /// registration without telling the pool, so the pool sums the changes
    /// of the consumer, not of a reservation.
    held: usize,
}

/// A spilling aggregate's right to a request the pool cannot serve.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Grant {
    /// The consumer released its state in a bulk shrink, or repaid its loan,
    /// and has not made a request the pool refused since.
    Armed,
    /// A request was served beyond the free memory; this many bytes of it are
    /// not released yet.
    Lent(usize),
}

impl QueryMemoryPool {
    /// A pool of `pool_size` bytes.
    pub(crate) fn new(pool_size: usize) -> Self {
        Self {
            pool_size,
            state: Mutex::new(State::default()),
        }
    }

    fn locked(&self) -> MutexGuard<'_, State> {
        // The counters stay consistent if a holder panicked: every update is
        // one addition or subtraction.
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn is_spilling_aggregate(consumer: &MemoryConsumer) -> bool {
        consumer.can_spill() && consumer.name().starts_with(AGGREGATE_CONSUMER_PREFIX)
    }

    /// The bytes the pool reserves for each of `spillers` aggregates: the
    /// floors together never pass half the pool, so the other consumers keep
    /// the rest.
    fn floor(&self, spillers: usize) -> usize {
        SPILLER_FLOOR_BYTES.min(self.pool_size / 2 / spillers.max(1))
    }

    /// The bytes lent to the spillers, which the reserved total counts beyond
    /// the pool.
    fn lent(state: &State) -> usize {
        state
            .spillers
            .values()
            .map(|spiller| match spiller.grant {
                Grant::Lent(lent) => lent,
                Grant::Armed => 0,
            })
            .sum()
    }

    /// The bytes reserved for the floors of the spillers other than `consumer`
    /// that they do not hold yet.
    fn unfilled_floors_except(&self, state: &State, consumer: usize) -> usize {
        let floor = self.floor(state.spillers.len());
        state
            .spillers
            .iter()
            .filter(|(id, _)| **id != consumer)
            .map(|(_, spiller)| floor.saturating_sub(spiller.held))
            .sum()
    }
}

impl MemoryPool for QueryMemoryPool {
    fn name(&self) -> &str {
        "query"
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.locked().spillers.remove(&consumer.id());
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        let mut state = self.locked();
        state.reserved += additional;
        if let Some(spiller) = state.spillers.get_mut(&reservation.consumer().id()) {
            spiller.held += additional;
        }
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        let mut state = self.locked();
        state.reserved = state.reserved.saturating_sub(shrink);
        let id = reservation.consumer().id();
        if let Some(spiller) = state.spillers.get_mut(&id) {
            spiller.held = spiller.held.saturating_sub(shrink);
        }
        if shrink == 0 {
            return;
        }
        // The reservation is already reduced: a bulk shrink released at least
        // as much as it kept.
        let bulk = shrink >= reservation.size();
        match state.spillers.get_mut(&id) {
            Some(spiller) if bulk => spiller.grant = Grant::Armed,
            Some(spiller) => {
                if let Grant::Lent(lent) = spiller.grant {
                    spiller.grant = match lent.saturating_sub(shrink) {
                        0 => Grant::Armed,
                        lent => Grant::Lent(lent),
                    };
                }
            }
            None if bulk && Self::is_spilling_aggregate(reservation.consumer()) => {
                state.spillers.insert(
                    id,
                    Spiller {
                        grant: Grant::Armed,
                        held: reservation.size(),
                    },
                );
            }
            None => {}
        }
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        // Nothing to reserve: nothing to refuse, and nothing to lend. A request
        // for no bytes must not use up a grant or fail a query because a loan
        // has the pool over its limit.
        if additional == 0 {
            return Ok(());
        }
        let mut state = self.locked();
        let id = reservation.consumer().id();
        // A consumer that can spill recovers from a refusal by spilling, so
        // the floors are kept from it. One that cannot spill fails its query
        // on a refusal, so it is refused only at the limit of the pool, which
        // the loans do not count against: they are bytes the pool stopped
        // counting and took back, and they are repaid within a spill.
        let (reserved, others) = if reservation.consumer().can_spill() {
            (state.reserved, self.unfilled_floors_except(&state, id))
        } else {
            (state.reserved.saturating_sub(Self::lent(&state)), 0)
        };
        // A request that keeps a spiller within its floor is served, whatever
        // else the pool holds: the floor is reserved for it. Any other request
        // is served while it leaves the floors of the other spillers unfilled
        // memory to be filled. Either way a spill's small requests (the emit
        // and the reset of the state) leave the grant in place for the one
        // that sorts the emitted batch.
        let within_floor = state.spillers.get(&id).is_some_and(|spiller| {
            spiller.held.saturating_add(additional) <= self.floor(state.spillers.len())
        });
        let fits = reserved.saturating_add(additional).saturating_add(others) <= self.pool_size;
        if !(within_floor || fits) {
            let lend = additional <= self.pool_size
                && state
                    .spillers
                    .get(&id)
                    .is_some_and(|spiller| spiller.grant == Grant::Armed);
            if !lend {
                let available = self
                    .pool_size
                    .saturating_sub(reserved)
                    .saturating_sub(others);
                return Err(DataFusionError::ResourcesExhausted(format!(
                    "Failed to allocate additional {} for {} with {} already allocated for this reservation - {} remain available for the total memory pool: {}(used: {}, pool_size: {})",
                    human_readable_size(additional),
                    reservation.consumer().name(),
                    human_readable_size(reservation.size()),
                    human_readable_size(available),
                    self.name(),
                    human_readable_size(reserved),
                    human_readable_size(self.pool_size),
                )));
            }
            if let Some(spiller) = state.spillers.get_mut(&id) {
                spiller.grant = Grant::Lent(additional);
            }
        }
        state.reserved += additional;
        if let Some(spiller) = state.spillers.get_mut(&id) {
            spiller.held += additional;
        }
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.locked().reserved
    }

    fn memory_limit(&self) -> MemoryLimit {
        MemoryLimit::Finite(self.pool_size)
    }
}

impl Display for QueryMemoryPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}(used: {}, pool_size: {})",
            self.name(),
            human_readable_size(self.locked().reserved),
            human_readable_size(self.pool_size)
        )
    }
}

#[cfg(test)]
mod tests;
