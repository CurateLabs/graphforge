//! The query memory pool: DataFusion's greedy pool, except that a spilling
//! aggregate keeps the memory it has just released long enough to sort it
//! (#1985).
//!
//! DataFusion's grouped aggregate spills by emitting its whole state, freeing
//! the reservation that held it, and only then reserving memory to sort the
//! emitted batch before writing it out. The emitted batch lives outside the
//! pool in between, and the sort reservation is a fresh request for the memory
//! just released (plus a slice of one batch more). On a greedy pool another
//! partition of the same aggregate can take the released memory first, and the
//! request fails with `Failed to reserve memory for sort during spill`: a query
//! that should spill fails, depending on how its partitions happen to
//! interleave.
//!
//! The pool therefore remembers, for each spilling aggregate consumer, that it
//! has just released its state in a bulk shrink, and grants its next request
//! that the pool cannot serve (up to the size of the pool) whatever else has
//! been reserved since. The bytes already exist: the emitted batch the pool
//! stopped counting a moment earlier holds the whole state, including the
//! groups of the last input batch the released reservation never counted, and
//! the sort only reorders it. A refusal could only fail the query, while
//! granting it lets the reserved total pass the budget by at most the batches
//! being sorted, after which every other request is refused until they are
//! released.
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
//! Everything else is first-come within the pool limit, as in DataFusion's
//! greedy pool: sorts reserve their merge memory up front and repartitions spill
//! by writing, so only grouped aggregates, identified by name, are affected.

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

/// A greedy memory pool of `pool_size` bytes with the grant described in the
/// module documentation.
#[derive(Debug)]
pub(crate) struct QueryMemoryPool {
    pool_size: usize,
    state: Mutex<State>,
}

#[derive(Debug, Default)]
struct State {
    /// Bytes reserved by every consumer.
    reserved: usize,
    /// The grant of each spilling aggregate consumer that has one.
    grants: HashMap<usize, Grant>,
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
}

impl MemoryPool for QueryMemoryPool {
    fn name(&self) -> &str {
        "query"
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.locked().grants.remove(&consumer.id());
    }

    fn grow(&self, _reservation: &MemoryReservation, additional: usize) {
        self.locked().reserved += additional;
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        let mut state = self.locked();
        state.reserved = state.reserved.saturating_sub(shrink);
        if shrink == 0 || !Self::is_spilling_aggregate(reservation.consumer()) {
            return;
        }
        let id = reservation.consumer().id();
        // The reservation is already reduced: a bulk shrink released at least
        // as much as it kept.
        if shrink >= reservation.size() {
            state.grants.insert(id, Grant::Armed);
        } else if let Some(Grant::Lent(lent)) = state.grants.get(&id).copied() {
            let grant = match lent.saturating_sub(shrink) {
                0 => Grant::Armed,
                lent => Grant::Lent(lent),
            };
            state.grants.insert(id, grant);
        }
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        let mut state = self.locked();
        let available = self.pool_size.saturating_sub(state.reserved);
        // A request the free memory serves leaves the grant in place: a spill
        // makes several small requests (the emit and the reset of the state)
        // before the one that sorts the emitted batch.
        let id = reservation.consumer().id();
        let granted = additional > available
            && additional <= self.pool_size
            && state.grants.get(&id) == Some(&Grant::Armed);
        if granted {
            state.grants.insert(id, Grant::Lent(additional));
        } else if additional > available {
            return Err(DataFusionError::ResourcesExhausted(format!(
                "Failed to allocate additional {} for {} with {} already allocated for this reservation - {} remain available for the total memory pool: {}(used: {}, pool_size: {})",
                human_readable_size(additional),
                reservation.consumer().name(),
                human_readable_size(reservation.size()),
                human_readable_size(available),
                self.name(),
                human_readable_size(state.reserved),
                human_readable_size(self.pool_size),
            )));
        }
        state.reserved += additional;
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
