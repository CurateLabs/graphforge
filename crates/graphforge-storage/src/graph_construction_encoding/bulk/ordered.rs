//! Ordered, memory-gated execution of the scratch partitions (#1900).
//!
//! Partitions are claimed in order and run concurrently. Two orderings keep
//! that safe and deterministic:
//!
//! - The memory gate grants reservations in claim order. A partition that is
//!   waiting for a turn is therefore never starved of memory by a later one,
//!   and the sum of the reservations in flight never exceeds the capacity.
//! - A turn serializes the one step of a partition that depends on the
//!   partitions before it (carrying a window or a CSR shard across the
//!   boundary), in partition order, while everything else overlaps.
//!
//! Neither wait can deadlock: a partition waits only for earlier partitions,
//! and the earliest unfinished partition waits for nothing.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::Duration;

use super::{GfError, storage};

struct State {
    available: u64,
    next_grant: usize,
    turns: Vec<usize>,
    aborted: bool,
}

pub(super) struct Ordered<'a> {
    capacity: u64,
    state: Mutex<State>,
    changed: Condvar,
    cancel: &'a AtomicBool,
}

fn cancelled() -> GfError {
    storage("construction encoding cancelled")
}

impl<'a> Ordered<'a> {
    /// `capacity` bytes to reserve from and `lanes` independent turn sequences.
    pub(super) fn new(capacity: u64, lanes: usize, cancel: &'a AtomicBool) -> Self {
        Self {
            capacity,
            state: Mutex::new(State {
                available: capacity,
                next_grant: 0,
                turns: vec![0; lanes],
                aborted: false,
            }),
            changed: Condvar::new(),
            cancel,
        }
    }

    fn wait<T>(&self, mut ready: impl FnMut(&mut State) -> Option<T>) -> Result<T, GfError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| storage("scratch scheduler lock poisoned"))?;
        loop {
            if state.aborted || self.cancel.load(Ordering::Acquire) {
                return Err(cancelled());
            }
            if let Some(value) = ready(&mut state) {
                self.changed.notify_all();
                return Ok(value);
            }
            state = self
                .changed
                .wait_timeout(state, Duration::from_millis(20))
                .map_err(|_| storage("scratch scheduler lock poisoned"))?
                .0;
        }
    }

    /// Reserve `cost` bytes for partition `ticket`, after every earlier ticket.
    pub(super) fn acquire(&self, ticket: usize, cost: u64) -> Result<(), GfError> {
        if cost > self.capacity {
            return Err(GfError::Project {
                code: graphforge_core::ProjectErrorCode::ResourceLimit,
                message: format!(
                    "graph construction encoding: a scratch partition needs {cost} bytes, \
                     more than the {} byte memory budget allows",
                    self.capacity
                ),
            });
        }
        self.wait(|state| {
            if state.next_grant == ticket && state.available >= cost {
                state.available -= cost;
                state.next_grant += 1;
                Some(())
            } else {
                None
            }
        })
    }

    pub(super) fn release(&self, cost: u64) {
        if let Ok(mut state) = self.state.lock() {
            state.available += cost;
        }
        self.changed.notify_all();
    }

    /// Block until it is partition `ticket`'s turn on `lane`.
    pub(super) fn wait_turn(&self, lane: usize, ticket: usize) -> Result<(), GfError> {
        self.wait(|state| (state.turns[lane] == ticket).then_some(()))
    }

    /// Hand `lane` to the next partition.
    pub(super) fn pass_turn(&self, lane: usize) {
        if let Ok(mut state) = self.state.lock() {
            state.turns[lane] += 1;
        }
        self.changed.notify_all();
    }

    fn abort(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.aborted = true;
        }
        self.changed.notify_all();
    }
}

/// Run `body(index)` for `0..count` on `concurrency` threads, claiming indices
/// in order and reserving `cost(index)` bytes from `ordered` around each.
pub(super) fn run_ordered(
    count: usize,
    concurrency: usize,
    ordered: &Ordered<'_>,
    cost: impl Fn(usize) -> u64 + Sync,
    body: impl Fn(usize) -> Result<(), GfError> + Sync,
) -> Result<(), GfError> {
    let next = AtomicUsize::new(0);
    let first_error = Mutex::new(None::<GfError>);
    std::thread::scope(|scope| {
        for _ in 0..concurrency.clamp(1, count.max(1)) {
            scope.spawn(|| {
                loop {
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    if index >= count {
                        return;
                    }
                    let reserved = cost(index);
                    let outcome = ordered.acquire(index, reserved).and_then(|()| {
                        let outcome =
                            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| body(index)))
                                .unwrap_or_else(|_| {
                                    Err(storage("a scratch partition worker panicked"))
                                });
                        ordered.release(reserved);
                        outcome
                    });
                    if let Err(error) = outcome {
                        if let Ok(mut slot) = first_error.lock() {
                            // A cancellation seen by a bystander must not mask the cause.
                            let bystander = error.to_string().contains("cancelled");
                            if slot.is_none()
                                || (!bystander && slot.as_ref().is_some_and(is_cancelled))
                            {
                                *slot = Some(error);
                            }
                        }
                        ordered.abort();
                        return;
                    }
                }
            });
        }
    });
    match first_error.into_inner() {
        Ok(None) => Ok(()),
        Ok(Some(error)) => Err(error),
        Err(_) => Err(storage("scratch scheduler lock poisoned")),
    }
}

fn is_cancelled(error: &GfError) -> bool {
    error.to_string().contains("cancelled")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;

    #[test]
    fn reservations_never_exceed_the_capacity_and_turns_run_in_order() {
        let cancel = AtomicBool::new(false);
        let ordered = Ordered::new(100, 1, &cancel);
        let in_flight = AtomicU64::new(0);
        let peak = AtomicU64::new(0);
        let order = Mutex::new(Vec::new());
        run_ordered(
            40,
            8,
            &ordered,
            |index| 10 + (index as u64 % 4) * 10,
            |index| {
                let cost = 10 + (index as u64 % 4) * 10;
                let now = in_flight.fetch_add(cost, Ordering::SeqCst) + cost;
                peak.fetch_max(now, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(1));
                ordered.wait_turn(0, index)?;
                order.lock().unwrap().push(index);
                ordered.pass_turn(0);
                in_flight.fetch_sub(cost, Ordering::SeqCst);
                Ok(())
            },
        )
        .unwrap();
        assert!(peak.load(Ordering::SeqCst) <= 100);
        assert_eq!(*order.lock().unwrap(), (0..40).collect::<Vec<_>>());
    }

    #[test]
    fn a_failure_releases_every_waiter_and_names_the_cause() {
        let cancel = AtomicBool::new(false);
        let ordered = Ordered::new(100, 1, &cancel);
        let error = run_ordered(
            16,
            4,
            &ordered,
            |_| 10,
            |index| {
                if index == 3 {
                    return Err(storage("boom"));
                }
                ordered.wait_turn(0, index)?;
                ordered.pass_turn(0);
                Ok(())
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("boom"), "{error}");
    }

    #[test]
    fn a_partition_larger_than_the_budget_is_refused() {
        let cancel = AtomicBool::new(false);
        let ordered = Ordered::new(100, 1, &cancel);
        let error = run_ordered(1, 1, &ordered, |_| 101, |_| Ok(())).unwrap_err();
        assert!(error.to_string().contains("memory budget"), "{error}");
    }
}
