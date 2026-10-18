// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! A private prepaid bank: numeric tickets recycle while real producers live.
//! Retirement releases unused credit; escaped buffers retain only their tickets.
use super::admission::{add, mul};
use crate::terminal_ownership as od;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::memory_pool::MemoryReservation;
use std::sync::{Arc, Mutex};

#[derive(Debug, PartialEq, Eq)]
enum Lifecycle {
    Open,
    Retiring,
    Closed,
}
#[derive(Debug)]
struct State {
    authority: MemoryReservation,
    available: usize,
    outstanding: usize,
    producers: usize,
    lifecycle: Lifecycle,
}
// Exact pre-observation layouts used by all existing admission formulas.
#[allow(dead_code)]
struct ChargedCredit {
    state: Mutex<State>,
    _control: Option<MemoryReservation>,
}
#[allow(dead_code)]
struct ChargedReaderPermit(Arc<Credit>);
#[derive(Debug)]
pub(crate) struct Credit {
    state: Mutex<State>,
    // Separate from the bank invariant and held through the last ticket.
    _control: Option<MemoryReservation>,
    diagnostic_piece: od::Piece,
    _diagnostic_control_piece: od::Piece,
    _diagnostic_control: od::Life,
    diagnostic: od::Life,
}
impl Drop for Credit {
    fn drop(&mut self) {
        self.diagnostic.begin_drop();
    }
}
impl Credit {
    pub(super) fn diagnostic_id(&self) -> u64 {
        self.diagnostic.id()
    }
    fn observe(&self, state: &State) {
        self.diagnostic_piece
            .refresh(&state.authority, self.diagnostic.id());
        self.diagnostic.update(|o| {
            o.piece = self.diagnostic_piece.id();
            o.held = state.authority.size() as u64;
            o.a = state.available as u64;
            o.b = state.outstanding as u64;
            o.c = state.producers as u64;
            o.d = match state.lifecycle {
                Lifecycle::Open => 0,
                Lifecycle::Retiring => 1,
                Lifecycle::Closed => 2,
            };
        });
    }
    /// Legacy sponsors keep their original reservation lifetime/pool policy.
    pub(crate) fn new(authority: MemoryReservation) -> Arc<Self> {
        Self::build(authority, None, 0, None)
    }
    fn build(
        authority: MemoryReservation,
        control: Option<MemoryReservation>,
        producers: usize,
        observed: Option<(od::Life, od::Piece)>,
    ) -> Arc<Self> {
        let _transition = od::Transition::begin();
        let (diagnostic, diagnostic_piece) = observed.unwrap_or_else(|| {
            let owner = od::Life::inherited(od::Kind::Credit);
            let piece = od::Piece::new(&authority, owner.id());
            (owner, piece)
        });
        diagnostic_piece.refresh(&authority, diagnostic.id());
        let (diagnostic_control_piece, diagnostic_control) =
            if let Some(control) = &control {
                let owner = od::Life::new(od::Kind::BatchMetadata, diagnostic.id(), 0);
                let piece = diagnostic_piece.child(control, owner.id(), control.size());
                owner.update(|o| {
                    o.piece = piece.id();
                    o.bytes = control.size() as u64;
                    o.held = control.size() as u64;
                    o.a = 3;
                    o.flags = 4;
                });
                diagnostic.edge(owner.id(), 7, 0);
                (piece, owner)
            } else {
                // Legacy credit has no separately qualified control reservation.
                // Its accounting remains unchanged, but metadata proof is partial.
                diagnostic.gap(od::GAP_UNSUPPORTED);
                (od::Piece::default(), od::Life::default())
            };
        diagnostic.update(|o| {
            o.piece = diagnostic_piece.id();
            o.held = authority.size() as u64;
            o.a = authority.size() as u64;
            o.c = producers as u64;
        });
        let state = Mutex::new(State {
            available: authority.size(),
            authority,
            outstanding: 0,
            producers,
            lifecycle: Lifecycle::Open,
        });
        if control.is_some() {
            // A pthread-backed mutex can lazily allocate on its first lock.
            // Its checked heap fee is already held, and this unique local
            // state cannot race to allocate transient OnceBox candidates.
            drop(state.lock().unwrap());
        }
        Arc::new(Self {
            state,
            _control: control,
            diagnostic_piece,
            _diagnostic_control_piece: diagnostic_control_piece,
            _diagnostic_control: diagnostic_control,
            diagnostic,
        })
    }
    /// Called only after all-IPC/schema eligibility, before moving any credit.
    /// All root permits are constructed and registration sealed before any
    /// reader/prefetch is started. There is no fallible step between recording
    /// the root count and constructing the preadmitted Arc permits.
    pub(crate) fn with_readers(
        authority: MemoryReservation,
        readers: usize,
    ) -> Result<(Arc<Self>, Vec<Arc<ReaderPermit>>)> {
        Self::with_readers_and_control(authority, readers, 0)
    }
    /// An owned FIFO donor also retains its literal-name registration after
    /// encoding ends. That checked control fee cannot become borrowable credit.
    pub(super) fn with_readers_and_control(
        authority: MemoryReservation,
        readers: usize,
        owned_control: usize,
    ) -> Result<(Arc<Self>, Vec<Arc<ReaderPermit>>)> {
        let _transition = od::Transition::begin();
        if authority.consumer().can_spill() || readers == 0 {
            return Err(DataFusionError::Internal(
                "Retiring IPC credit requires a nonspilling sponsor and readers".into(),
            ));
        }
        let mutex_heap =
            super::resident_metadata::std_mutex_heap_bound().ok_or_else(|| {
                DataFusionError::ResourcesExhausted(
                    "IPC reader credit mutex control is not bounded on this target"
                        .into(),
                )
            })?;
        // Adopt the actual donor BEFORE splitting control from it. The donor
        // certificate still has its complete pre-split held amount here.
        let diagnostic = od::Life::inherited(od::Kind::Credit);
        let diagnostic_piece = od::Piece::new(&authority, diagnostic.id());
        // Keep the original authority's failure-path destruction inside the
        // transition, without changing its order relative to product locals.
        let authority = authority;
        let control = add(
            add(owned_control, mutex_heap)?,
            add(
                add(size_of::<ChargedCredit>(), 2 * size_of::<usize>())?,
                add(
                    size_of::<Vec<Arc<ReaderPermit>>>(),
                    mul(
                        readers,
                        add(
                            add(
                                size_of::<ChargedReaderPermit>(),
                                2 * size_of::<usize>(),
                            )?,
                            size_of::<Arc<ReaderPermit>>(),
                        )?,
                    )?,
                )?,
            )?,
        )?;
        authority.try_grow(control.saturating_sub(authority.size()))?;
        let control = authority.split(control);
        let mut permits = Vec::new();
        permits.try_reserve_exact(readers).map_err(|error| {
            DataFusionError::ResourcesExhausted(format!(
                "IPC reader registration allocation failed: {error}"
            ))
        })?;
        let credit = Self::build(
            authority,
            Some(control),
            readers,
            Some((diagnostic, diagnostic_piece)),
        );
        for _ in 0..readers {
            permits.push(Arc::new(ReaderPermit::new(Arc::clone(&credit))));
        }
        Ok((credit, permits))
    }
    pub(super) fn take(&self, wanted: usize) -> usize {
        let _transition = od::Transition::begin();
        let mut state = self.state.lock().unwrap();
        if state.lifecycle != Lifecycle::Open {
            return 0;
        }
        let amount = wanted.min(state.available);
        state.available -= amount;
        state.outstanding += amount; // bounded by the admitted original total
        debug_assert_eq!(state.authority.size(), state.available + state.outstanding);
        self.observe(&state);
        amount
    }
    /// No allocation/admission/wait for workers. Stock reservation shrink can
    /// lock the pool. No Arrow walk/claim/drop occurs under this bank mutex.
    pub(super) fn give(&self, amount: usize) {
        if amount == 0 {
            return;
        }
        let _transition = od::Transition::begin();
        let mut state = self.state.lock().unwrap();
        state.outstanding -= amount;
        if state.lifecycle == Lifecycle::Open {
            state.available += amount;
        } else {
            state.authority.shrink(amount);
            if state.outstanding == 0 {
                state.lifecycle = Lifecycle::Closed;
            }
        }
        debug_assert_eq!(state.authority.size(), state.available + state.outstanding);
        self.observe(&state);
    }
    fn retire_reader(&self) {
        let _transition = od::Transition::begin();
        let mut state = self.state.lock().unwrap();
        state.producers -= 1;
        if state.producers == 0 {
            state.lifecycle = Lifecycle::Retiring;
            let available = std::mem::take(&mut state.available);
            if available != 0 {
                state.authority.shrink(available);
            }
            if state.outstanding == 0 {
                state.lifecycle = Lifecycle::Closed;
            }
        }
        debug_assert_eq!(state.authority.size(), state.available + state.outstanding);
        self.observe(&state);
    }
}
/// Stream and active workers clone the SAME root permit. Buffer tickets never
/// own this permit, so aliases cannot keep registration/available credit open.
#[derive(Debug)]
pub(crate) struct ReaderPermit(Arc<Credit>, od::Life);
impl ReaderPermit {
    fn new(credit: Arc<Credit>) -> Self {
        let diagnostic = od::Life::new(od::Kind::ReaderPermit, credit.diagnostic_id(), 0);
        diagnostic.edge(credit.diagnostic_id(), 9, 0);
        Self(credit, diagnostic)
    }
}
impl Drop for ReaderPermit {
    fn drop(&mut self) {
        self.1.begin_drop();
        self.0.retire_reader();
    }
}

#[cfg(feature = "test_utils")]
pub(crate) fn verify_ownership_controls() -> Result<()> {
    ownership_tests::retiring_credit_follows_worker_and_escaped_ticket_lifetimes()?;
    ownership_tests::failed_fresh_admission_returns_exact_borrowed_credit()?;
    ownership_tests::original_permit_layout_is_one_arc();
    Ok(())
}

#[cfg(any(test, feature = "test_utils"))]
mod ownership_tests {
    use super::*;
    use crate::spill::admission::reserve_with_credit;
    use datafusion_execution::memory_pool::{
        GreedyMemoryPool, MemoryConsumer, MemoryPool,
    };

    #[cfg_attr(test, test)]
    pub(super) fn retiring_credit_follows_worker_and_escaped_ticket_lifetimes()
    -> Result<()> {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(32768));
        let authority = MemoryConsumer::new("IPC sponsor").register(&pool);
        authority.try_grow(32768)?;
        let (credit, permits) = Credit::with_readers(authority, 2)?;
        let worker = Arc::clone(&permits[0]);
        let mut work =
            reserve_with_credit(&pool, 256, "IPC decode", Some(Arc::clone(&credit)))?;
        let escaped = work.split(96);
        assert_eq!(pool.reserved(), 32768);
        drop(permits);
        assert_eq!(credit.state.lock().unwrap().producers, 1);
        drop(worker);
        {
            let state = credit.state.lock().unwrap();
            assert_eq!(state.producers, 0);
            assert_eq!(state.available, 0);
            assert_eq!(state.outstanding, 256);
            assert_eq!(state.authority.size(), 256);
            assert_eq!(state.lifecycle, Lifecycle::Retiring);
        }
        drop(work);
        assert_eq!(credit.state.lock().unwrap().outstanding, 96);
        drop(escaped);
        {
            let state = credit.state.lock().unwrap();
            assert_eq!(state.outstanding, 0);
            assert_eq!(state.authority.size(), 0);
            assert_eq!(state.lifecycle, Lifecycle::Closed);
        }
        assert_eq!(pool.reserved(), credit._control.as_ref().unwrap().size());
        drop(credit);
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    #[cfg_attr(test, test)]
    pub(super) fn failed_fresh_admission_returns_exact_borrowed_credit() -> Result<()> {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(32768));
        let authority = MemoryConsumer::new("IPC sponsor").register(&pool);
        authority.try_grow(32768)?;
        let (credit, permits) = Credit::with_readers(authority, 1)?;
        let available = credit.state.lock().unwrap().available;
        let refused =
            reserve_with_credit(&pool, 32769, "IPC decode", Some(Arc::clone(&credit)));
        assert!(refused.is_err());
        {
            let state = credit.state.lock().unwrap();
            assert_eq!(state.available, available);
            assert_eq!(state.outstanding, 0);
            assert_eq!(state.authority.size(), available);
        }
        assert_eq!(pool.reserved(), 32768);
        drop(permits);
        drop(credit);
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    #[cfg_attr(test, test)]
    pub(super) fn original_permit_layout_is_one_arc() {
        assert_eq!(size_of::<ChargedReaderPermit>(), size_of::<Arc<Credit>>());
    }
}
