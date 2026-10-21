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
//! Qualified kernels use checked current-operation bounds. Controlled Row
//! kernels without a proof refuse before execution; unqualified merges retain
//! their existing envelope. This is not a universal kernel estimator.
use super::builder::BufferedBatch;
use crate::spill::admission::{add, array_metadata, mul, schema_bound, source_metadata};
use crate::terminal_ownership as od;
use arrow::array::ArrayRef;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow_buffer::{Buffer, MemoryPool, MemoryReservation as ArrowReservation};
use arrow_data::ArrayData;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::memory_pool::{
    MemoryConsumer, MemoryPool as DataFusionPool, MemoryReservation,
};
use std::alloc::Layout;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// The consumer the outputs of a merge of spill files are charged to once
/// they escape the merge; it cannot spill them.
pub(super) const OUTPUT_CONSUMER: &str = "MergeOutput";
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
    /// The outputs that escaped the merge and are still held by whoever
    /// consumes them. The merge cannot spill them, so they are charged to a
    /// consumer that cannot spill ([`OUTPUT_CONSUMER`]), which the pool
    /// refuses only at its limit, while the authority keeps the pass's working
    /// set with the floors the pool keeps from a consumer that can spill. An
    /// output is charged there at the merge's next operation, as its bytes
    /// grew the authority before: until then it is within the working set.
    output: MemoryReservation,
    rows: usize,
    /// Bytes of every live output.
    outputs: usize,
    /// Bytes of the live outputs charged to `output`; the rest are within
    /// the authority.
    charged: usize,
    active: bool,
    preparing: bool,
    field: FieldLifetime,
    /// What the authority never falls below once preparing has finished: the
    /// resident bytes of the constructors and the working set above them.
    /// Resident bytes that arrive later draw on it.
    floor: usize,
}
/// The allocation proof used by this merge, separate from its original envelope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CopyCapability {
    Legacy,
    Row { encoded_width: usize },
    PrimitiveField,
}
#[derive(Debug)]
#[allow(dead_code)]
pub(super) struct ChargedCopyAdmission {
    envelope: usize,
    working_set: usize,
    capability: CopyCapability,
    state: Mutex<State>,
    _registration: MemoryReservation,
}

#[derive(Debug)]
pub(super) struct CopyAdmission {
    envelope: usize,
    /// Bytes of kernel workspace plus one live output, admitted before the
    /// first operation and held until the last: an operation that fits within
    /// them draws on them, and only an output past them asks the pool again.
    working_set: usize,
    capability: CopyCapability,
    state: Mutex<State>,
    /// The registration of the output consumer, paid from the authority and
    /// held through the last output.
    _registration: MemoryReservation,
    diagnostic_piece: od::Piece,
    diagnostic_output_piece: od::Piece,
    diagnostic: od::Life,
}
impl CopyAdmission {
    pub(super) fn charged_size() -> usize {
        size_of::<ChargedCopyAdmission>()
    }
    fn observe(&self, state: &State) {
        self.diagnostic_piece
            .refresh(&state.authority, self.diagnostic.id());
        self.diagnostic_output_piece
            .refresh(&state.output, self.diagnostic.id());
        self.diagnostic.update(|o| {
            o.piece = self.diagnostic_piece.id();
            o.object = self.envelope as u64;
            o.held = state.authority.size() as u64;
            o.bytes = Self::control_bytes() as u64;
            o.a = state.rows as u64;
            o.b = state.outputs as u64;
            o.c = u64::from(state.active)
                | (u64::from(state.preparing) << 1)
                | match state.field {
                    FieldLifetime::None => 0,
                    FieldLifetime::Live => 4,
                    FieldLifetime::Retired => 8,
                };
            o.d = match self.capability {
                CopyCapability::Legacy => 0,
                CopyCapability::PrimitiveField => 2,
                CopyCapability::Row { encoded_width } => {
                    1 | ((encoded_width as u64) << 2)
                }
            };
            o.flags = 4;
        });
    }

    fn control_bytes() -> usize {
        size_of::<ChargedCopyAdmission>()
            + 2 * size_of::<usize>()
            + crate::spill::resident_metadata::std_mutex_heap_bound().unwrap_or(0)
    }
    /// `working_set` is held in `authority` above the envelope, for a merge
    /// that reads with a Field cursor (zero for any other).
    pub(super) fn new(
        authority: MemoryReservation,
        pool: &Arc<dyn DataFusionPool>,
        working_set: usize,
    ) -> Result<Arc<Self>> {
        // Preserve sufficient original authority. Admit any missing known
        // control bytes, and the output consumer's registration, BEFORE the
        // first Mutex lock or Arc allocation.
        let missing_control = add(Self::control_bytes(), Self::registration_bytes()?)?
            .saturating_sub(authority.size().saturating_sub(working_set));
        if missing_control > 0 {
            authority.try_grow(missing_control)?;
        }
        let (output, registration) = Self::output_consumer(&authority, pool)?;
        let envelope = authority.size().saturating_sub(working_set);
        let diagnostic = od::Life::inherited(od::Kind::Copy);
        let diagnostic_piece = od::Piece::new(&authority, diagnostic.id());
        let diagnostic_output_piece = od::Piece::new(&output, diagnostic.id());
        diagnostic.update(|o| {
            o.piece = diagnostic_piece.id();
            o.object = envelope as u64;
            o.held = authority.size() as u64;
            o.bytes = Self::control_bytes() as u64;
            o.flags = 4;
        });
        let state = Mutex::new(State {
            authority,
            output,
            rows: 0,
            outputs: 0,
            charged: 0,
            active: false,
            preparing: false,
            field: FieldLifetime::None,
            floor: 0,
        });
        // On macOS, the first lock allocates one pthread mutex Box. Initialize
        // while uniquely owned, so no OnceBox initializer can allocate a loser
        // Box concurrently. The State's authority survives mutex destruction.
        drop(state.lock().unwrap());
        Ok(Arc::new(Self {
            envelope,
            working_set,
            capability: CopyCapability::Legacy,
            state,
            _registration: registration,
            diagnostic,
            diagnostic_piece,
            diagnostic_output_piece,
        }))
    }
    /// What registering the output consumer takes, which the pass admits
    /// with the copies' metadata.
    pub(super) fn registration_bytes() -> Result<usize> {
        crate::spill::resident_metadata::literal_registration_bytes(OUTPUT_CONSUMER)
            .ok_or_else(bound_error)
    }
    /// The consumer the merge's escaped outputs are charged to, registered
    /// on the sponsor's pool with its registration split from the authority,
    /// which admitted it, and kept apart from the authority's bytes as a
    /// bank's control is.
    fn output_consumer(
        authority: &MemoryReservation,
        pool: &Arc<dyn DataFusionPool>,
    ) -> Result<(MemoryReservation, MemoryReservation)> {
        let registration = authority.split(Self::registration_bytes()?);
        let output = MemoryConsumer::new(OUTPUT_CONSUMER).register(pool);
        Ok((output, registration))
    }
    pub(super) fn preparing(
        authority: MemoryReservation,
        pool: &Arc<dyn DataFusionPool>,
        metadata: usize,
        capability: CopyCapability,
        working_set: usize,
    ) -> Result<Arc<Self>> {
        let envelope = authority.size();
        // This occurs before starting any prefetched reader. Constructor
        // allowance remains earmarked until both row/merge constructors finish.
        // The working set was admitted at file selection and stays held.
        let heap = crate::spill::resident_metadata::std_mutex_heap_bound().unwrap_or(0);
        authority.try_resize(add(add(metadata, heap)?, working_set)?)?;
        let (output, registration) = Self::output_consumer(&authority, pool)?;
        let diagnostic = od::Life::inherited(od::Kind::Copy);
        let diagnostic_piece = od::Piece::new(&authority, diagnostic.id());
        let diagnostic_output_piece = od::Piece::new(&output, diagnostic.id());
        diagnostic.update(|o| {
            o.piece = diagnostic_piece.id();
            o.object = envelope as u64;
            o.held = authority.size() as u64;
            o.bytes = Self::control_bytes() as u64;
            o.flags = 4;
            o.c = 2;
            o.d = match capability {
                CopyCapability::Legacy => 0,
                CopyCapability::PrimitiveField => 2,
                CopyCapability::Row { encoded_width } => {
                    1 | ((encoded_width as u64) << 2)
                }
            };
        });
        let state = Mutex::new(State {
            authority,
            output,
            rows: 0,
            outputs: 0,
            charged: 0,
            active: false,
            preparing: true,
            field: FieldLifetime::None,
            floor: 0,
        });
        // Full constructor grant remains live through this unique-owner first
        // lock and the later Arc allocation. No public Arc exists yet.
        drop(state.lock().unwrap());
        Ok(Arc::new(Self {
            envelope,
            working_set,
            capability,
            state,
            _registration: registration,
            diagnostic,
            diagnostic_piece,
            diagnostic_output_piece,
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
            Self::control_bytes()
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
    /// The bytes of the live outputs not charged to the output consumer yet.
    fn pending_outputs(state: &State) -> usize {
        state.outputs - state.charged
    }
    /// What the authority has live: the resident bytes and the outputs not
    /// charged to the output consumer yet.
    fn live(&self, state: &State) -> usize {
        self.base(state, state.rows) + Self::pending_outputs(state)
    }
    /// Charges the outputs the authority holds beyond what its floor carries
    /// beside the operation's workspace to the output consumer (the working
    /// set is sized for one live output, which rides within it as before): a
    /// request a consumer that cannot spill makes, refused only at the pool's
    /// limit, and fatal then, as the growth of the authority by the same
    /// bytes was. The authority keeps them until the operation's target takes
    /// them out.
    fn charge_outputs(&self, state: &mut State, workspace: usize) -> Result<()> {
        let room = state
            .floor
            .saturating_sub(add(self.base(state, state.rows), workspace)?);
        let charge = Self::pending_outputs(state).saturating_sub(room);
        if charge > 0 {
            state.output.try_grow(charge)?;
            state.charged += charge;
        }
        Ok(())
    }
    /// What the authority keeps between operations: the live bytes, and never
    /// less than the floor.
    fn retained(&self, state: &State) -> usize {
        self.live(state).max(state.floor)
    }
    /// What an operation needs: its workspace beside the live bytes, within
    /// the floor when they fit it, so that only the excess asks the pool.
    fn operating(&self, state: &State, workspace: usize) -> Result<usize> {
        Ok(state.floor.max(add(self.live(state), workspace)?))
    }
    pub(super) fn preparing_field(&self, metadata: usize) -> Result<()> {
        let _transition = od::Transition::begin();
        let mut state = self.state.lock().unwrap();
        if self.capability != CopyCapability::Legacy
            || state.field != FieldLifetime::None
            || state.active
        {
            return Err(bound_error());
        }
        let control = Self::control_bytes();
        let target = add(self.envelope.max(add(control, metadata)?), self.working_set)?;
        if target > state.authority.size() {
            state.authority.try_grow(target - state.authority.size())?;
        }
        state.field = FieldLifetime::Live;
        state.preparing = true;
        self.observe(&state);
        Ok(())
    }
    /// The last output of the merge has been built: its copies have no further
    /// use for the working set, and the outputs keep only their own bytes.
    pub(super) fn release_working_set(&self) {
        let _transition = od::Transition::begin();
        let mut state = self.state.lock().unwrap();
        state.floor = 0;
        if !state.active && !state.preparing {
            let excess = state.authority.size().saturating_sub(self.retained(&state));
            state.authority.shrink(excess);
        }
        self.observe(&state);
    }
    /// The wrapper must destroy inner cursor/GC/failed-phase data first. The
    /// merge is over: the escaped outputs keep only their own bytes.
    pub(super) fn retire(&self) {
        let _transition = od::Transition::begin();
        let mut state = self.state.lock().unwrap();
        state.floor = 0;
        if state.field == FieldLifetime::Live {
            state.field = FieldLifetime::Retired;
            state.preparing = false;
        }
        if !state.active && !state.preparing {
            let excess = state.authority.size().saturating_sub(self.retained(&state));
            state.authority.shrink(excess);
        }
        self.observe(&state);
    }
    pub(super) fn has_output_bounds(&self) -> bool {
        self.bounded_operations()
            || self.state.lock().unwrap().field != FieldLifetime::None
    }
    pub(super) fn begin_interleave(
        self: &Arc<Self>,
        workspace: Option<usize>,
    ) -> Result<Phase> {
        let _transition = od::Transition::begin();
        if workspace.is_none() && matches!(self.capability, CopyCapability::Row { .. }) {
            let state = self.state.lock().unwrap();
            return Err(DataFusionError::ResourcesExhausted(format!(
                "Row interleave lacks a checked payload allocation bound: {}",
                state.authority.consumer().name(),
            )));
        }
        if workspace.is_none() && self.state.lock().unwrap().field != FieldLifetime::None
        {
            return Err(DataFusionError::ResourcesExhausted(
                "Legacy Field interleave lacks a checked payload allocation bound".into(),
            ));
        }
        self.begin_operation(workspace)
    }
    pub(super) fn finish_preparing(&self) {
        let _transition = od::Transition::begin();
        let mut state = self.state.lock().unwrap();
        if state.preparing {
            state.preparing = false;
            if self.working_set > 0 {
                state.floor = self.live(&state) + self.working_set;
            }
            let target = self.retained(&state);
            let excess = state.authority.size().saturating_sub(target);
            state.authority.shrink(excess);
            self.observe(&state);
        }
    }
    pub(super) fn begin(self: &Arc<Self>) -> Result<Phase> {
        self.begin_operation(None)
    }
    pub(super) fn begin_operation(
        self: &Arc<Self>,
        workspace: Option<usize>,
    ) -> Result<Phase> {
        let _transition = od::Transition::begin();
        let mut state = self.state.lock().unwrap();
        if state.active {
            return Err(bound_error());
        }
        // Escaped previous outputs cannot pay for another kernel invocation:
        // those the floor cannot carry move to the output consumer, and the
        // operation's target no longer counts them.
        if !state.preparing {
            self.charge_outputs(&mut state, workspace.unwrap_or(0))?;
        }
        let target = if state.preparing {
            state.authority.size()
        } else if state.field != FieldLifetime::None {
            self.operating(&state, workspace.unwrap_or(0))?
        } else if let Some(workspace) = workspace.filter(|_| self.bounded_operations()) {
            self.operating(&state, workspace)?
        } else {
            add(
                self.envelope.max(add(self.control(&state), state.rows)?),
                Self::pending_outputs(&state),
            )?
        };
        match target.cmp(&state.authority.size()) {
            std::cmp::Ordering::Greater => {
                state.authority.try_grow(target - state.authority.size())?;
            }
            // Outputs charged to the output consumer leave the authority.
            std::cmp::Ordering::Less => state.authority.shrink(state.authority.size() - target),
            std::cmp::Ordering::Equal => {}
        }
        state.active = true;
        self.observe(&state);
        Ok(Phase {
            diagnostic: od::Life::new(
                od::Kind::Copy,
                self.diagnostic.id(),
                self.diagnostic_piece.id(),
            ),
            admission: Arc::clone(self),
        })
    }
    fn resident(self: &Arc<Self>, bytes: usize, output: bool) -> Result<Arc<Resident>> {
        let _transition = od::Transition::begin();
        let mut state = self.state.lock().unwrap();
        let bytes = add(
            bytes,
            add(size_of::<ChargedResident>(), 2 * size_of::<usize>())?,
        )?;
        let total = if output {
            add(self.live(&state), bytes)?
        } else {
            add(
                self.checked_base(&state, add(state.rows, bytes)?)?,
                Self::pending_outputs(&state),
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
        self.observe(&state);
        let diagnostic = od::Life::new(
            od::Kind::Resident,
            self.diagnostic.id(),
            self.diagnostic_piece.id(),
        );
        diagnostic.update(|o| {
            o.bytes = bytes as u64;
            o.held = state.authority.size() as u64;
            o.a = u64::from(output);
        });
        Ok(Arc::new(Resident {
            diagnostic,
            admission: Arc::clone(self),
            bytes: AtomicUsize::new(bytes),
            output,
        }))
    }
}
#[derive(Debug)]
#[allow(dead_code)]
pub(super) struct ChargedResident {
    admission: Arc<CopyAdmission>,
    bytes: AtomicUsize,
    output: bool,
}

#[derive(Debug)]
pub(super) struct Resident {
    admission: Arc<CopyAdmission>,
    bytes: AtomicUsize,
    output: bool,
    diagnostic: od::Life,
}
impl Resident {
    pub(super) fn diagnostic_id(&self) -> u64 {
        self.diagnostic.id()
    }
}
impl Drop for Resident {
    fn drop(&mut self) {
        self.diagnostic.begin_drop();
        let _transition = od::Transition::begin();
        let mut state = self.admission.state.lock().unwrap();
        let bytes = self.bytes.load(Ordering::Relaxed);
        if self.output {
            // The output consumer gives back what it was charged; an output
            // not charged yet leaves the authority with the shrink below.
            let charged = bytes.min(state.charged);
            if charged > 0 {
                state.output.shrink(charged);
                state.charged -= charged;
            }
            state.outputs -= bytes;
        } else {
            state.rows -= bytes;
        }
        if !state.active && !state.preparing {
            let target = self.admission.retained(&state);
            let excess = state.authority.size().saturating_sub(target);
            state.authority.shrink(excess);
        }
        self.admission.observe(&state);
    }
}
#[derive(Debug)]
#[allow(dead_code)]
pub(super) struct ChargedPhase {
    admission: Arc<CopyAdmission>,
}

#[derive(Debug)]
pub(super) struct Phase {
    admission: Arc<CopyAdmission>,
    diagnostic: od::Life,
}
impl Phase {
    pub(super) fn resident(&self, bytes: usize) -> Result<Arc<Resident>> {
        self.admission.resident(bytes, false)
    }
    fn output(&self, bytes: usize) -> Result<Arc<Resident>> {
        self.admission.resident(bytes, true)
    }
    pub(super) fn resize(&self, resident: &Arc<Resident>, bytes: usize) -> Result<()> {
        let _transition = od::Transition::begin();
        if !Arc::ptr_eq(&self.admission, &resident.admission) {
            return Err(bound_error());
        }
        let bytes = add(
            bytes,
            add(size_of::<ChargedResident>(), 2 * size_of::<usize>())?,
        )?;
        let mut state = self.admission.state.lock().unwrap();
        let old = resident.bytes.load(Ordering::Relaxed);
        let total = add(
            self.admission
                .checked_base(&state, add(state.rows - old, bytes)?)?,
            CopyAdmission::pending_outputs(&state),
        )?;
        if !state.active || resident.output || total > state.authority.size() {
            return Err(bound_error());
        }
        state.rows = add(state.rows - old, bytes)?;
        resident.bytes.store(bytes, Ordering::Relaxed);
        resident.diagnostic.update(|o| o.bytes = bytes as u64);
        self.admission.observe(&state);
        Ok(())
    }
    // New bookkeeping is additional to the admitted kernel workspace. Grow
    // before its construction, never in response to an allocated output.
    fn extra(&self, bytes: usize) -> Result<()> {
        let _transition = od::Transition::begin();
        let state = self.admission.state.lock().unwrap();
        add(state.authority.size(), bytes)?;
        let result = state.authority.try_grow(bytes);
        self.admission.observe(&state);
        result
    }
    /// Replace dead operation workspace, preserving every actual resident and
    /// the live Field production floor in the SAME original reservation.
    pub(super) fn transition(&self, operation: usize) -> Result<()> {
        let _transition = od::Transition::begin();
        let state = self.admission.state.lock().unwrap();
        if !state.active || state.preparing {
            return Err(bound_error());
        }
        let result = state
            .authority
            .try_resize(self.admission.operating(&state, operation)?);
        self.admission.observe(&state);
        result
    }
}
impl Drop for Phase {
    fn drop(&mut self) {
        self.diagnostic.begin_drop();
        let _transition = od::Transition::begin();
        let mut state = self.admission.state.lock().unwrap();
        state.active = false;
        if !state.preparing {
            let target = self.admission.retained(&state);
            let excess = state.authority.size().saturating_sub(target);
            state.authority.shrink(excess);
        }
        self.admission.observe(&state);
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

/// Only the private all-IPC copy path uses this plan. The caller pins all
/// source batches through reconciliation; schema equality alone is not proof
/// that datatype Field/Fields trees share allocations with the declared schema.
pub(super) struct CopyPlan {
    slots: usize,
    ledger: usize,
    walk: usize,
    metadata: usize,
    kernel: usize,
    reconciliation: usize,
    payload: usize,
}
impl CopyPlan {
    pub(super) fn workspace(&self) -> Result<usize> {
        // The output must acquire its lifetime claims before this invocation
        // can release its authority. One progress grant must fund that entire
        // handoff, including metadata constructed after the Arrow kernel.
        Ok(add(self.ledger, self.walk.max(self.kernel))?.max(self.reconciliation))
    }
}
impl SourceOwners {
    pub(super) fn plan(
        batches: &[BufferedBatch],
        schema: &SchemaRef,
        payload: &super::copy_bounds::SerialPayload,
    ) -> Result<Option<CopyPlan>> {
        use crate::spill::resident_metadata;
        // This unchanged per-owner allowance covers the opaque Bytes Arc and
        // two overlapping claim boxes. Prove the whole control allocation
        // BEFORE choosing this path; other targets keep their existing fees.
        let control = resident_metadata::raw_bytes_arc_bound().and_then(|bytes| {
            size_of::<ChargedClaimed>()
                .checked_mul(2)?
                .checked_add(bytes)
        });
        if !control.is_some_and(|bytes| bytes <= 256) {
            return Ok(None);
        }
        let Some(first) = batches.first() else {
            return Ok(None);
        };
        let mut buffers = 0;
        for input in batches {
            let batch = &input.batch;
            let Some(headers) = resident_metadata::batch(batch) else {
                return Ok(None);
            };
            buffers = add(buffers, headers.buffers)?;
        }
        let Some(headers) = resident_metadata::batch(&first.batch) else {
            return Ok(None);
        };
        let mut walk = 0;
        let mut kernel_scratch = 0;
        for column in 0..first.batch.num_columns() {
            let mut active_sources = 0;
            for input in batches {
                let batch = &input.batch;
                let size = array_metadata(batch.column(column).as_ref())?;
                walk = walk.max(size);
                active_sources = add(active_sources, size)?;
            }
            // FixedBinary fallback materializes ALL source ArrayData trees,
            // reference Vecs and closures simultaneously. Recursive List/Struct
            // parent references remain live; the full column tree covers them.
            kernel_scratch = kernel_scratch.max(active_sources);
        }
        let slots = add(buffers, 1)?;
        let ledger = mul(slots, size_of::<Owner>())?;
        Layout::array::<Owner>(slots).map_err(|_| bound_error())?;
        let column_capacity = if first.batch.num_columns() == 0 {
            0
        } else {
            mul(first.batch.num_columns(), 2)?.max(4)
        };
        // Root Result<Vec> may reallocate old+new backing while collecting
        // columns. The resident header bound already includes the new backing.
        let root_growth = mul(column_capacity, size_of::<ArrayRef>())?;
        let kernel_metadata = add(
            add(headers.headers, mul(buffers, 256)?)?,
            add(kernel_scratch, root_growth)?,
        )?;
        // Output arrays clone actual first-source datatype subtrees; output
        // RecordBatch clones the declared root. Retain BOTH old schema fees.
        let metadata = add(
            schema_bound(schema)?,
            schema_bound(first.batch.schema_ref())?,
        )?;
        let resident_arc = resident_metadata::arc_bytes(Layout::new::<ChargedResident>())
            .ok_or_else(bound_error)?;
        // Interleave preserves the concrete datatype tree, so its array
        // headers fit the first IPC source's constructor bound. A null buffer
        // can come from any source: the SUM of source buffers bounds all
        // output buffers, fresh owner slots, and their claims. Keep the source
        // ledger and output backing live through the reconciliation walk.
        let reconciliation = add(
            mul(ledger, 2)?,
            add(
                add(
                    payload.resident,
                    add(add(metadata, headers.headers)?, resident_arc)?,
                )?,
                add(mul(buffers, add(256, resident_arc)?)?, walk)?,
            )?,
        )?;
        Ok(Some(CopyPlan {
            slots,
            ledger,
            walk,
            metadata,
            kernel: add(payload.peak, kernel_metadata)?,
            reconciliation,
            payload: payload.resident,
        }))
    }
    pub(super) fn new_planned(
        batches: &[BufferedBatch],
        plan: &CopyPlan,
    ) -> Result<Self> {
        let mut owners = Vec::new();
        owners
            .try_reserve_exact(plan.slots)
            .map_err(|_| bound_error())?;
        for input in batches {
            let batch = &input.batch;
            for column in batch.columns() {
                walk(&column.to_data(), &mut |buffer| insert(&mut owners, buffer))?;
            }
        }
        dedup(&mut owners);
        Ok(Self {
            owners,
            slots: plan.slots,
            metadata: plan.metadata,
        })
    }
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
    pub(super) fn retain_planned(
        &self,
        phase: &Phase,
        columns: &[ArrayRef],
        plan: &CopyPlan,
    ) -> Result<()> {
        use crate::spill::resident_metadata;
        let headers = resident_metadata::columns(columns).ok_or_else(bound_error)?;
        let resident_arc = resident_metadata::arc_bytes(Layout::new::<ChargedResident>())
            .ok_or_else(bound_error)?;
        // Resident Arc allocation is separately explicit. The preserved opaque
        // Bytes/claim allowance was proved before the kernel in plan().
        let mut physical = 0;
        let mut buffers = 0;
        for column in columns {
            walk(&column.to_data(), &mut |buffer| {
                if buffer.capacity() != 0 && !self.existing(buffer)? {
                    physical = add(physical, buffer.capacity())?;
                    buffers = add(buffers, 1)?;
                }
                Ok(())
            })?;
        }
        if physical > plan.payload {
            return Err(bound_error());
        }
        let slots = add(headers.buffers, 1)?;
        Layout::array::<Owner>(slots).map_err(|_| bound_error())?;
        let metadata = add(self.metadata, headers.headers)?;
        let operation = add(
            add(plan.ledger, mul(slots, size_of::<Owner>())?)?,
            add(
                add(physical, add(metadata, resident_arc)?)?,
                add(mul(buffers, add(256, resident_arc)?)?, plan.walk)?,
            )?,
        )?;
        if operation > plan.reconciliation {
            return Err(bound_error());
        }
        // Both phases were admitted before executing the kernel. Its scratch
        // is now dead, so release only the excess while every source/output
        // owner remains live; this transition cannot need a second grant.
        phase.transition(operation).map_err(|error| {
            DataFusionError::Context(
                "Merge output reconciliation".into(),
                Box::new(error),
            )
        })?;
        let mut fresh = Vec::new();
        fresh.try_reserve_exact(slots).map_err(|_| bound_error())?;
        for column in columns {
            walk(&column.to_data(), &mut |buffer| {
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
        for column in columns {
            walk(&column.to_data(), &mut |buffer| {
                if buffer.capacity() != 0 && !self.existing(buffer)? {
                    let index = fresh
                        .binary_search_by_key(&buffer.data_ptr(), |owner| owner.base)
                        .map_err(|_| bound_error())?;
                    buffer.claim(&Claims {
                        diagnostic_base: buffer.data_ptr().as_ptr() as usize,
                        diagnostic_capacity: buffer.capacity(),
                        lease: Arc::clone(fresh[index].lease.as_ref().unwrap()),
                        headers: Arc::clone(&headers),
                    });
                }
                Ok(())
            })?;
        }
        Ok(())
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
                        diagnostic_base: buffer.data_ptr().as_ptr() as usize,
                        diagnostic_capacity: buffer.capacity(),
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
    diagnostic_base: usize,
    diagnostic_capacity: usize,
    lease: Arc<Resident>,
    headers: Arc<Resident>,
}
#[derive(Debug)]
#[allow(dead_code)]
pub(super) struct ChargedClaimed {
    _lease: Arc<Resident>,
    _headers: Arc<Resident>,
    size: usize,
}

#[derive(Debug)]
struct Claimed {
    _lease: Arc<Resident>,
    _headers: Arc<Resident>,
    size: usize,
    diagnostic: od::Life,
}
impl ArrowReservation for Claimed {
    fn size(&self) -> usize {
        self.size
    }
    fn resize(&mut self, size: usize) {
        // Immutable engine output only: a later mutable allocation needs its
        // operator's normal admission; this fixed lifetime token cannot grow it.
        self.size = size;
        self.diagnostic.update(|o| o.bytes = size as u64);
    }
}
impl MemoryPool for Claims {
    fn reserve(&self, size: usize) -> Box<dyn ArrowReservation> {
        let diagnostic = od::Life::new(od::Kind::Claim, self.lease.diagnostic_id(), 0);
        diagnostic.update(|o| {
            o.bytes = size as u64;
            o.held = self.lease.bytes.load(Ordering::Relaxed) as u64;
        });
        diagnostic.raw(35, self.diagnostic_base, self.diagnostic_capacity, size);
        diagnostic.edge(self.lease.diagnostic_id(), 2, size);
        diagnostic.edge(self.headers.diagnostic_id(), 7, 0);
        Box::new(Claimed {
            diagnostic,
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

impl Drop for CopyAdmission {
    fn drop(&mut self) {
        self.diagnostic.begin_drop();
    }
}
impl Drop for Claimed {
    fn drop(&mut self) {
        self.diagnostic.begin_drop();
    }
}
