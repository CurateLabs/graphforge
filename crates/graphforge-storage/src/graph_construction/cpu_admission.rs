//! Construction CPU admission (#1586, ADR 0047).
//!
//! One instance-wide limit on how many parallel construction lanes may run at
//! once, across every import on the instance. The API sizes it as
//! `compute_threads - reserve`, so construction never takes the whole
//! instance CPU budget and queries keep a share while an import runs.
//!
//! A lane is a unit of parallel construction work besides the construction's
//! own calling thread: a finish-time partition load worker, or an import
//! normalization batch in flight on the compute pool. The calling thread is
//! not counted; one is inherent to running an import at all.
//!
//! Admission is scheduling only. A holder that is granted fewer lanes than it
//! asked for runs with fewer; published bytes and construction evidence do not
//! depend on the count (the partition-load schedule-independence tests and
//! ADR 0038 hold them equal at every worker count). Waiting for a lane polls
//! the caller's cancellation, so a cancelled import never blocks on another
//! import's lease.

use graphforge_core::GfError;
use std::num::NonZeroUsize;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

/// How often a waiting holder re-checks its cancellation callback.
const CANCEL_POLL: Duration = Duration::from_millis(5);

#[derive(Debug, Default)]
struct AdmissionState {
    in_use: usize,
    peak: usize,
}

/// Instance-wide limit on concurrent parallel construction lanes.
#[derive(Debug)]
pub struct ConstructionCpuAdmission {
    limit: NonZeroUsize,
    state: Mutex<AdmissionState>,
    released: Condvar,
}

impl ConstructionCpuAdmission {
    /// An admission granting at most `limit` lanes at once.
    #[must_use]
    pub fn new(limit: NonZeroUsize) -> Self {
        Self {
            limit,
            state: Mutex::new(AdmissionState::default()),
            released: Condvar::new(),
        }
    }

    /// The configured lane limit.
    #[must_use]
    pub fn limit(&self) -> usize {
        self.limit.get()
    }

    /// Lanes currently leased.
    #[must_use]
    pub fn in_use(&self) -> usize {
        self.state.lock().map_or(0, |state| state.in_use)
    }

    /// The most lanes ever leased at once since this admission was created.
    #[must_use]
    pub fn peak(&self) -> usize {
        self.state.lock().map_or(0, |state| state.peak)
    }

    /// Lease between one and `want` lanes, waiting until at least one is free.
    ///
    /// Takes every free lane up to `want`, so a lone import runs at full width
    /// and concurrent imports share the limit.
    ///
    /// # Errors
    /// Returns `construction cancelled` when `cancelled` reports true while
    /// waiting, and a storage error if the admission state is poisoned.
    pub fn acquire(
        self: &Arc<Self>,
        want: NonZeroUsize,
        cancelled: &mut dyn FnMut() -> bool,
    ) -> Result<ConstructionCpuLease, GfError> {
        let mut state = self.state.lock().map_err(poisoned)?;
        loop {
            let free = self.limit.get().saturating_sub(state.in_use);
            if let Some(free) = NonZeroUsize::new(free) {
                let lanes = want.min(free);
                state.in_use += lanes.get();
                state.peak = state.peak.max(state.in_use);
                return Ok(ConstructionCpuLease {
                    admission: Arc::clone(self),
                    lanes,
                });
            }
            if cancelled() {
                return Err(super::storage("construction cancelled"));
            }
            state = self
                .released
                .wait_timeout(state, CANCEL_POLL)
                .map_err(poisoned)?
                .0;
        }
    }

    fn release(&self, lanes: usize) {
        // A poisoned lock can only follow a panic while it was held, which the
        // admission itself never does; recover the count rather than leak it.
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        state.in_use = state.in_use.saturating_sub(lanes);
        drop(state);
        self.released.notify_all();
    }
}

fn poisoned<T>(_: std::sync::PoisonError<T>) -> GfError {
    super::storage("construction CPU admission poisoned")
}

/// Lanes held from a [`ConstructionCpuAdmission`]; released on drop.
#[derive(Debug)]
pub struct ConstructionCpuLease {
    admission: Arc<ConstructionCpuAdmission>,
    lanes: NonZeroUsize,
}

impl ConstructionCpuLease {
    /// Lanes granted, between one and the number requested.
    #[must_use]
    pub fn lanes(&self) -> NonZeroUsize {
        self.lanes
    }
}

impl Drop for ConstructionCpuLease {
    fn drop(&mut self) {
        self.admission.release(self.lanes.get());
    }
}

#[cfg(test)]
mod tests;
