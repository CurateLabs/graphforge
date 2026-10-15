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
#[derive(Debug)]
pub(crate) struct Credit {
    state: Mutex<State>,
    // Separate from the bank invariant and held through the last ticket.
    _control: Option<MemoryReservation>,
}
impl Credit {
    /// Legacy sponsors keep their original reservation lifetime/pool policy.
    pub(crate) fn new(authority: MemoryReservation) -> Arc<Self> {
        Self::build(authority, None, 0)
    }
    fn build(
        authority: MemoryReservation,
        control: Option<MemoryReservation>,
        producers: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                available: authority.size(),
                authority,
                outstanding: 0,
                producers,
                lifecycle: Lifecycle::Open,
            }),
            _control: control,
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
        if authority.consumer().can_spill() || readers == 0 {
            return Err(DataFusionError::Internal(
                "Retiring IPC credit requires a nonspilling sponsor and readers".into(),
            ));
        }
        let control = add(
            add(size_of::<Self>(), 2 * size_of::<usize>())?,
            add(
                size_of::<Vec<Arc<ReaderPermit>>>(),
                mul(
                    readers,
                    add(
                        add(size_of::<ReaderPermit>(), 2 * size_of::<usize>())?,
                        size_of::<Arc<ReaderPermit>>(),
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
        let credit = Self::build(authority, Some(control), readers);
        for _ in 0..readers {
            permits.push(Arc::new(ReaderPermit(Arc::clone(&credit))));
        }
        Ok((credit, permits))
    }
    pub(super) fn take(&self, wanted: usize) -> usize {
        let mut state = self.state.lock().unwrap();
        if state.lifecycle != Lifecycle::Open {
            return 0;
        }
        let amount = wanted.min(state.available);
        state.available -= amount;
        state.outstanding += amount; // bounded by the admitted original total
        debug_assert_eq!(state.authority.size(), state.available + state.outstanding);
        amount
    }
    /// No allocation/admission/wait for workers. Stock reservation shrink can
    /// lock the pool. No Arrow walk/claim/drop occurs under this bank mutex.
    pub(super) fn give(&self, amount: usize) {
        if amount == 0 {
            return;
        }
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
    }
    fn retire_reader(&self) {
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
    }
}
/// Stream and active workers clone the SAME root permit. Buffer tickets never
/// own this permit, so aliases cannot keep registration/available credit open.
#[derive(Debug)]
pub(crate) struct ReaderPermit(Arc<Credit>);
impl Drop for ReaderPermit {
    fn drop(&mut self) {
        self.0.retire_reader();
    }
}
