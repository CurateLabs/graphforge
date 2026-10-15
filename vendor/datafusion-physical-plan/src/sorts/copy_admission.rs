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

//! Copy authority follows allocation phases and the last retained backing.
//! Qualified kernels use checked current-operation bounds; other kernels keep
//! the existing merge-copy envelope. This is not a universal kernel estimator.
use crate::spill::admission::{add, array_metadata, mul, schema_bound, source_metadata};
use arrow::array::ArrayRef;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow_buffer::{Buffer, MemoryPool, MemoryReservation as ArrowReservation};
use arrow_data::ArrayData;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::memory_pool::MemoryReservation;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

fn bound_error() -> DataFusionError {
    DataFusionError::ResourcesExhausted("Merge copy exceeds its admitted envelope".into())
}
#[derive(Debug, PartialEq, Eq)]
enum FieldLifetime {
    None,
    Live,
    Retired,
}
#[derive(Debug)]
struct State {
    authority: MemoryReservation,
    rows: usize,
    outputs: usize,
    active: bool,
    preparing: bool,
    field: FieldLifetime,
}
/// The allocation proof used by this merge, separate from its original envelope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CopyCapability {
    Legacy,
    Row { encoded_width: usize },
    PrimitiveField,
}
#[derive(Debug)]
pub(super) struct CopyAdmission {
    envelope: usize,
    capability: CopyCapability,
    state: Mutex<State>,
}
impl CopyAdmission {
    pub(super) fn new(authority: MemoryReservation) -> Arc<Self> {
        Arc::new(Self {
            envelope: authority.size(),
            capability: CopyCapability::Legacy,
            state: Mutex::new(State {
                authority,
                rows: 0,
                outputs: 0,
                active: false,
                preparing: false,
                field: FieldLifetime::None,
            }),
        })
    }
    pub(super) fn preparing(
        authority: MemoryReservation,
        metadata: usize,
        capability: CopyCapability,
    ) -> Result<Arc<Self>> {
        let envelope = authority.size();
        // This occurs before starting any prefetched reader. Constructor
        // allowance remains earmarked until both row/merge constructors finish.
        authority.try_resize(metadata)?;
        Ok(Arc::new(Self {
            envelope,
            capability,
            state: Mutex::new(State {
                authority,
                rows: 0,
                outputs: 0,
                active: false,
                preparing: true,
                field: FieldLifetime::None,
            }),
        }))
    }
    pub(super) fn encoded_width(&self) -> Option<usize> {
        match self.capability {
            CopyCapability::Row { encoded_width } => Some(encoded_width),
            _ => None,
        }
    }
    pub(super) fn capability(&self) -> CopyCapability {
        self.capability
    }
    pub(super) fn bounded_operations(&self) -> bool {
        self.capability != CopyCapability::Legacy
    }
    // Dynamic outputs can retain this Arc after every cursor/stream lease has
    // gone. Keep its control allocation directly, without a Resident self-cycle.
    fn control(&self, state: &State) -> usize {
        if self.bounded_operations() || state.field != FieldLifetime::None {
            size_of::<Self>() + 2 * size_of::<usize>()
        } else {
            0
        }
    }
    fn base(&self, state: &State, rows: usize) -> usize {
        // Metadata is admitted before resident creation. A live Field keeps
        // the entire original production guard, including unphased key GC.
        let resident = self.control(state) + rows;
        if state.field == FieldLifetime::Live {
            self.envelope.max(resident)
        } else {
            resident
        }
    }
    fn checked_base(&self, state: &State, rows: usize) -> Result<usize> {
        let resident = add(self.control(state), rows)?;
        Ok(if state.field == FieldLifetime::Live {
            self.envelope.max(resident)
        } else {
            resident
        })
    }
    fn retained(&self, state: &State) -> usize {
        self.base(state, state.rows) + state.outputs
    }
    pub(super) fn preparing_field(&self, metadata: usize) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        if self.capability != CopyCapability::Legacy
            || state.field != FieldLifetime::None
            || state.active
        {
            return Err(bound_error());
        }
        let control = add(size_of::<Self>(), 2 * size_of::<usize>())?;
        let target = self.envelope.max(add(control, metadata)?);
        if target > state.authority.size() {
            state.authority.try_grow(target - state.authority.size())?;
        }
        state.field = FieldLifetime::Live;
        state.preparing = true;
        Ok(())
    }
    /// The wrapper must destroy inner cursor/GC/failed-phase data first.
    pub(super) fn retire_field(&self) {
        let mut state = self.state.lock().unwrap();
        if state.field == FieldLifetime::Live {
            state.field = FieldLifetime::Retired;
            state.preparing = false;
            if !state.active {
                let excess = state.authority.size() - self.retained(&state);
                state.authority.shrink(excess);
            }
        }
    }
    pub(super) fn has_output_bounds(&self) -> bool {
        self.bounded_operations()
            || self.state.lock().unwrap().field != FieldLifetime::None
    }
    pub(super) fn begin_interleave(
        self: &Arc<Self>,
        workspace: Option<usize>,
    ) -> Result<Phase> {
        if workspace.is_none() && self.state.lock().unwrap().field != FieldLifetime::None
        {
            return Err(DataFusionError::ResourcesExhausted(
                "Legacy Field interleave lacks a checked payload allocation bound".into(),
            ));
        }
        self.begin_operation(workspace)
    }
    pub(super) fn finish_preparing(&self) {
        let mut state = self.state.lock().unwrap();
        if state.preparing {
            state.preparing = false;
            let target = self.retained(&state);
            let excess = state.authority.size() - target;
            state.authority.shrink(excess);
        }
    }
    pub(super) fn begin(self: &Arc<Self>) -> Result<Phase> {
        self.begin_operation(None)
    }
    pub(super) fn begin_operation(
        self: &Arc<Self>,
        workspace: Option<usize>,
    ) -> Result<Phase> {
        let mut state = self.state.lock().unwrap();
        if state.active {
            return Err(bound_error());
        }
        // Escaped previous outputs cannot pay for another kernel invocation.
        let target = if state.preparing {
            state.authority.size()
        } else if state.field != FieldLifetime::None {
            add(self.retained(&state), workspace.unwrap_or(0))?
        } else if let Some(workspace) = workspace.filter(|_| self.bounded_operations()) {
            add(self.retained(&state), workspace)?
        } else {
            add(
                self.envelope.max(add(self.control(&state), state.rows)?),
                state.outputs,
            )?
        };
        if target > state.authority.size() {
            state.authority.try_grow(target - state.authority.size())?;
        }
        state.active = true;
        Ok(Phase {
            admission: Arc::clone(self),
        })
    }
    fn resident(self: &Arc<Self>, bytes: usize, output: bool) -> Result<Arc<Resident>> {
        let mut state = self.state.lock().unwrap();
        let bytes = add(bytes, add(size_of::<Resident>(), 2 * size_of::<usize>())?)?;
        let total = if output {
            add(self.retained(&state), bytes)?
        } else {
            add(
                self.checked_base(&state, add(state.rows, bytes)?)?,
                state.outputs,
            )?
        };
        if !state.active || total > state.authority.size() {
            return Err(bound_error());
        }
        if output {
            state.outputs = add(state.outputs, bytes)?;
        } else {
            state.rows = add(state.rows, bytes)?;
        }
        Ok(Arc::new(Resident {
            admission: Arc::clone(self),
            bytes: AtomicUsize::new(bytes),
            output,
        }))
    }
}
#[derive(Debug)]
pub(super) struct Resident {
    admission: Arc<CopyAdmission>,
    bytes: AtomicUsize,
    output: bool,
}
impl Drop for Resident {
    fn drop(&mut self) {
        let mut state = self.admission.state.lock().unwrap();
        let bytes = self.bytes.load(Ordering::Relaxed);
        if self.output {
            state.outputs -= bytes;
        } else {
            state.rows -= bytes;
        }
        if !state.active && !state.preparing {
            let target = self.admission.retained(&state);
            let excess = state.authority.size() - target;
            state.authority.shrink(excess);
        }
    }
}
#[derive(Debug)]
pub(super) struct Phase {
    admission: Arc<CopyAdmission>,
}
impl Phase {
    pub(super) fn resident(&self, bytes: usize) -> Result<Arc<Resident>> {
        self.admission.resident(bytes, false)
    }
    fn output(&self, bytes: usize) -> Result<Arc<Resident>> {
        self.admission.resident(bytes, true)
    }
    pub(super) fn resize(&self, resident: &Arc<Resident>, bytes: usize) -> Result<()> {
        if !Arc::ptr_eq(&self.admission, &resident.admission) {
            return Err(bound_error());
        }
        let bytes = add(bytes, add(size_of::<Resident>(), 2 * size_of::<usize>())?)?;
        let mut state = self.admission.state.lock().unwrap();
        let old = resident.bytes.load(Ordering::Relaxed);
        let total = add(
            self.admission
                .checked_base(&state, add(state.rows - old, bytes)?)?,
            state.outputs,
        )?;
        if !state.active || resident.output || total > state.authority.size() {
            return Err(bound_error());
        }
        state.rows = add(state.rows - old, bytes)?;
        resident.bytes.store(bytes, Ordering::Relaxed);
        Ok(())
    }
    // New bookkeeping is additional to the admitted kernel workspace. Grow
    // before its construction, never in response to an allocated output.
    fn extra(&self, bytes: usize) -> Result<()> {
        let state = self.admission.state.lock().unwrap();
        add(state.authority.size(), bytes)?;
        state.authority.try_grow(bytes)
    }
}
impl Drop for Phase {
    fn drop(&mut self) {
        let mut state = self.admission.state.lock().unwrap();
        state.active = false;
        if !state.preparing {
            let target = self.admission.retained(&state);
            let excess = state.authority.size() - target;
            state.authority.shrink(excess);
        }
    }
}

#[derive(Debug)]
struct Owner {
    base: NonNull<u8>,
    capacity: usize,
    lease: Option<Arc<Resident>>,
}
fn walk(data: &ArrayData, f: &mut impl FnMut(&Buffer) -> Result<()>) -> Result<()> {
    for buffer in data.buffers() {
        f(buffer)?;
    }
    if let Some(nulls) = data.nulls() {
        f(nulls.buffer())?;
    }
    for child in data.child_data() {
        walk(child, f)?;
    }
    Ok(())
}
fn insert(owners: &mut Vec<Owner>, buffer: &Buffer) -> Result<()> {
    // Empty buffers can share a dangling base without sharing allocation.
    if buffer.capacity() == 0 {
        return Ok(());
    }
    if owners.len() == owners.capacity() {
        return Err(bound_error());
    }
    owners.push(Owner {
        base: buffer.data_ptr(),
        capacity: buffer.capacity(),
        lease: None,
    });
    Ok(())
}
fn dedup(owners: &mut Vec<Owner>) {
    owners.sort_unstable_by_key(|o| o.base);
    owners.dedup_by(|later, earlier| {
        if later.base == earlier.base {
            earlier.capacity = earlier.capacity.max(later.capacity);
            true
        } else {
            false
        }
    });
}
/// This ledger borrows no Arrow owner. Its caller keeps every source batch
/// alive through reconciliation, so its numeric bases cannot suffer ABA reuse.
pub(super) struct SourceOwners {
    owners: Vec<Owner>,
    slots: usize,
    metadata: usize,
}
impl SourceOwners {
    pub(super) fn new<'a>(
        phase: &Phase,
        batches: impl Clone + Iterator<Item = &'a RecordBatch>,
    ) -> Result<Self> {
        let metadata = batches
            .clone()
            .try_fold(0, |sum, b| add(sum, source_metadata(b)?))?;
        phase.extra(metadata)?;
        // to_data's temporary headers are now admitted before counting slots.
        let mut buffers = 0;
        let mut nodes = 0;
        fn count(data: &ArrayData, buffers: &mut usize, nodes: &mut usize) -> Result<()> {
            *nodes = add(*nodes, 1)?;
            *buffers = add(
                *buffers,
                add(data.buffers().len(), usize::from(data.nulls().is_some()))?,
            )?;
            for child in data.child_data() {
                count(child, buffers, nodes)?;
            }
            Ok(())
        }
        for batch in batches.clone() {
            for col in batch.columns() {
                count(&col.to_data(), &mut buffers, &mut nodes)?;
            }
        }
        let slots = add(add(buffers, nodes)?, 1)?;
        // Two bounded ledgers plus all per-owner Arc/Box tokens, including
        // transient replacement tokens for repeated references to one owner.
        phase.extra(add(
            mul(slots, mul(2, size_of::<Owner>())?)?,
            mul(slots, 512)?,
        )?)?;
        let mut owners = Vec::new();
        owners.try_reserve_exact(slots).map_err(|_| bound_error())?;
        for batch in batches {
            for col in batch.columns() {
                walk(&col.to_data(), &mut |b| insert(&mut owners, b))?;
            }
        }
        dedup(&mut owners);
        Ok(Self {
            owners,
            slots,
            metadata,
        })
    }
    fn existing(&self, buffer: &Buffer) -> Result<bool> {
        match self
            .owners
            .binary_search_by_key(&buffer.data_ptr(), |o| o.base)
        {
            Ok(i) if self.owners[i].capacity >= buffer.capacity() => Ok(true),
            Ok(_) => Err(bound_error()),
            Err(_) => Ok(false),
        }
    }
    pub(super) fn retain_fresh(
        &self,
        phase: &Phase,
        schema: &SchemaRef,
        columns: &[ArrayRef],
    ) -> Result<()> {
        let metadata = columns.iter().try_fold(schema_bound(schema)?, |sum, a| {
            add(sum, array_metadata(a.as_ref())?)
        })?;
        if metadata > self.metadata {
            return Err(bound_error());
        }
        let mut fresh = Vec::new();
        fresh
            .try_reserve_exact(self.slots)
            .map_err(|_| bound_error())?;
        for col in columns {
            walk(&col.to_data(), &mut |buffer| {
                if !self.existing(buffer)? {
                    insert(&mut fresh, buffer)?;
                }
                Ok(())
            })?;
        }
        dedup(&mut fresh);
        let headers = phase.output(metadata)?;
        for owner in &mut fresh {
            owner.lease = Some(phase.output(add(owner.capacity, 256)?)?);
        }
        for col in columns {
            walk(&col.to_data(), &mut |buffer| {
                if buffer.capacity() != 0 && !self.existing(buffer)? {
                    let i = fresh
                        .binary_search_by_key(&buffer.data_ptr(), |o| o.base)
                        .map_err(|_| bound_error())?;
                    buffer.claim(&Claims {
                        lease: Arc::clone(fresh[i].lease.as_ref().unwrap()),
                        headers: Arc::clone(&headers),
                    });
                }
                Ok(())
            })?;
        }
        Ok(())
    }
}
#[derive(Debug)]
struct Claims {
    lease: Arc<Resident>,
    headers: Arc<Resident>,
}
#[derive(Debug)]
struct Claimed {
    _lease: Arc<Resident>,
    _headers: Arc<Resident>,
    size: usize,
}
impl ArrowReservation for Claimed {
    fn size(&self) -> usize {
        self.size
    }
    fn resize(&mut self, size: usize) {
        // Immutable engine output only: a later mutable allocation needs its
        // operator's normal admission; this fixed lifetime token cannot grow it.
        self.size = size;
    }
}
impl MemoryPool for Claims {
    fn reserve(&self, size: usize) -> Box<dyn ArrowReservation> {
        Box::new(Claimed {
            _lease: Arc::clone(&self.lease),
            _headers: Arc::clone(&self.headers),
            size,
        })
    }
    fn available(&self) -> isize {
        0
    }
    fn used(&self) -> usize {
        self.lease.bytes.load(Ordering::Relaxed)
    }
    fn capacity(&self) -> usize {
        self.lease.bytes.load(Ordering::Relaxed)
    }
}
