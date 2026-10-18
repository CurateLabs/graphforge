//! Opt-in, bounded development evidence for selected sort ownership boundaries.
//!
//! Addresses are observations scoped to live generation IDs, never funding IDs.
//! Product operations run outside this recorder's mutex. A snapshot with an open
//! transition is explicitly incomplete; this is not a query-wide atomic inventory.
use std::cell::Cell;
use std::fmt::{self, Write as _};
use std::fs::{File, OpenOptions};
use std::io::{self, Write as _};
use std::path::Path;
use std::sync::{Mutex, OnceLock};

pub mod arrays;

pub const LINE_BYTES: usize = 1024;
pub const GAP_UNSUPPORTED: u64 = 1;
pub const GAP_UNKNOWN_SPONSOR: u64 = 2;
pub const GAP_ROWS_STORAGE: u64 = 4;
pub const GAP_DEFERRED_MERGE: u64 = 8;
pub const GAP_QUEUE: u64 = 16;
pub const POISON_CAPACITY: u64 = 1;
pub const POISON_ARITHMETIC: u64 = 2;
pub const POISON_IO: u64 = 4;
pub const POISON_PROTOCOL: u64 = 8;

/// Numeric owner kinds. Only these values are accepted by the matching assessor.
#[derive(Clone, Copy, Debug)]
#[repr(u64)]
pub enum Kind {
    Sorter = 1,
    Source = 2,
    Merge = 3,
    Builder = 4,
    Batch = 5,
    Field = 6,
    Row = 7,
    Key = 8,
    Copy = 9,
    Resident = 10,
    Claim = 11,
    Queue = 12,
    Producer = 13,
    Output = 14,
    Budget = 15,
    Credit = 16,
    ReaderPermit = 17,
    BatchMetadata = 18,
}
#[derive(Clone, Copy, Debug, Default)]
pub struct OwnerRecord {
    pub call: u64,
    pub id: u64,
    pub owner_kind: u64,
    pub parent: u64,
    pub piece: u64,
    pub object: u64,
    pub bytes: u64,
    pub held: u64,
    pub a: u64,
    pub b: u64,
    pub c: u64,
    pub d: u64,
    pub flags: u64,
}
#[derive(Clone, Copy, Debug, Default)]
pub struct PieceRecord {
    pub call: u64,
    pub id: u64,
    pub root: u64,
    pub holder: u64,
    pub consumer: u64,
    pub held: u64,
    pub split_from: u64,
    pub split_bytes: u64,
    pub flags: u64,
}
#[derive(Clone, Copy, Debug, Default)]
pub struct EdgeRecord {
    pub call: u64,
    pub from: u64,
    pub to: u64,
    pub relation: u64,
    pub bytes: u64,
}
#[derive(Clone, Copy, Debug)]
pub struct Envelope {
    pub calls: usize,
    pub slots: usize,
    pub descriptors: usize,
    pub edges: usize,
    pub columns: usize,
    pub nodes: usize,
    pub depth: usize,
    pub buffers: usize,
}
impl Default for Envelope {
    fn default() -> Self {
        Self {
            calls: 2,
            slots: 512,
            descriptors: 4096,
            edges: 1024,
            columns: 32,
            nodes: 128,
            depth: 8,
            buffers: 256,
        }
    }
}
#[derive(Clone, Copy, Debug)]
pub struct Bounds {
    pub records: usize,
    pub bytes: usize,
    pub observer_bytes: usize,
}
impl Envelope {
    pub fn bounds(self) -> io::Result<Bounds> {
        let bad = || {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "ownership envelope overflow or zero",
            )
        };
        if self.calls == 0
            || self.calls > 64
            || self.slots == 0
            || self.slots > 65536
            || self.descriptors == 0
            || self.descriptors > 1048576
            || self.edges > 1048576
            || self.columns == 0
            || self.nodes == 0
            || self.depth == 0
            || self.buffers == 0
        {
            return Err(bad());
        }
        let cut = self
            .slots
            .checked_mul(2)
            .and_then(|v| v.checked_add(self.descriptors))
            .and_then(|v| v.checked_add(self.edges))
            .ok_or_else(bad)?;
        let records = cut
            .checked_add(2)
            .and_then(|v| v.checked_mul(2))
            .and_then(|v| v.checked_add(1))
            .and_then(|v| v.checked_mul(self.calls))
            .and_then(|v| v.checked_add(2))
            .ok_or_else(bad)?;
        let bytes = records.checked_mul(LINE_BYTES).ok_or_else(bad)?;
        let live = self
            .slots
            .checked_mul(
                size_of::<Option<OwnerRecord>>() + size_of::<Option<PieceRecord>>(),
            )
            .and_then(|v| {
                self.descriptors
                    .checked_mul(size_of::<Option<arrays::Backing>>())
                    .and_then(|d| v.checked_add(d))
            })
            .and_then(|v| {
                self.edges
                    .checked_mul(size_of::<Option<EdgeRecord>>())
                    .and_then(|d| v.checked_add(d))
            })
            .ok_or_else(bad)?;
        // Conservative arena allowance includes live slots plus both reserved cuts
        // for every call, even though cuts are streamed without retaining copies.
        let observer_bytes = self
            .calls
            .checked_mul(2)
            .and_then(|v| v.checked_add(1))
            .and_then(|v| v.checked_mul(live))
            .and_then(|v| {
                v.checked_add(
                    size_of::<Recorder>()
                        + size_of::<Mutex<Option<Recorder>>>()
                        + LINE_BYTES,
                )
            })
            .ok_or_else(bad)?;
        Ok(Bounds {
            records,
            bytes,
            observer_bytes,
        })
    }
}
struct Line {
    bytes: [u8; LINE_BYTES],
    len: usize,
}
impl Line {
    fn new() -> Self {
        Self {
            bytes: [0; LINE_BYTES],
            len: 0,
        }
    }
}
impl fmt::Write for Line {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        let end = self.len.checked_add(s.len()).ok_or(fmt::Error)?;
        // One byte is permanently reserved for LF.
        if end >= LINE_BYTES {
            return Err(fmt::Error);
        }
        self.bytes[self.len..end].copy_from_slice(s.as_bytes());
        self.len = end;
        Ok(())
    }
}
struct Recorder {
    file: File,
    envelope: Envelope,
    bounds: Bounds,
    owners: Vec<Option<OwnerRecord>>,
    pieces: Vec<Option<PieceRecord>>,
    backings: Vec<Option<arrays::Backing>>,
    edges: Vec<Option<EdgeRecord>>,
    next: u64,
    call: u64,
    completed: u64,
    snapshots: u64,
    epoch: u64,
    in_flight: u64,
    poison: u64,
    gaps: u64,
    constructed: u64,
    retired: u64,
    operations: u64,
    records: u64,
    bytes: u64,
    checksum: u64,
}
impl Recorder {
    fn count(value: &mut u64, poison: &mut u64) {
        if let Some(next) = value.checked_add(1) {
            *value = next;
        } else {
            *poison |= POISON_ARITHMETIC;
        }
    }
    fn id(&mut self) -> u64 {
        let id = self.next;
        Self::count(&mut self.next, &mut self.poison);
        id
    }
    fn emit(&mut self, build: impl FnOnce(&mut Line) -> fmt::Result) {
        let mut line = Line::new();
        if build(&mut line).is_err() {
            self.poison |= POISON_CAPACITY;
            return;
        }
        line.bytes[line.len] = b'\n';
        line.len += 1;
        if self.records >= self.bounds.records as u64
            || self
                .bytes
                .checked_add(line.len as u64)
                .is_none_or(|v| v > self.bounds.bytes as u64)
        {
            self.poison |= POISON_CAPACITY;
            return;
        }
        if self.file.write_all(&line.bytes[..line.len]).is_err() {
            self.poison |= POISON_IO;
            return;
        }
        self.records += 1;
        self.bytes += line.len as u64;
        for byte in &line.bytes[..line.len] {
            self.checksum =
                (self.checksum ^ u64::from(*byte)).wrapping_mul(0x100000001b3);
        }
    }
    fn cut(&mut self, outcome: u64) {
        if self.call == 0 || self.snapshots >= 2 {
            self.poison |= POISON_PROTOCOL;
            return;
        }
        self.snapshots += 1;
        let (
            call,
            snapshot,
            epoch,
            in_flight,
            poison,
            gaps,
            constructed,
            retired,
            operations,
        ) = (
            self.call,
            self.snapshots,
            self.epoch,
            self.in_flight,
            self.poison,
            self.gaps,
            self.constructed,
            self.retired,
            self.operations,
        );
        self.emit(|l| write!(l,"{{\"kind\":\"snapshot_begin\",\"call\":{call},\"snapshot\":{snapshot},\"outcome\":{outcome},\"epoch\":{epoch},\"in_flight\":{in_flight},\"poison\":{poison},\"gaps\":{gaps},\"constructed\":{constructed},\"retired\":{retired},\"operations\":{operations}}}"));
        let mut owners = 0;
        for i in 0..self.owners.len() {
            if let Some(r) = self.owners[i] {
                owners += 1;
                self.emit(|l| write!(l,"{{\"kind\":\"owner\",\"call\":{},\"id\":{},\"owner_kind\":{},\"parent\":{},\"piece\":{},\"object\":{},\"bytes\":{},\"held\":{},\"a\":{},\"b\":{},\"c\":{},\"d\":{},\"flags\":{}}}",r.call,r.id,r.owner_kind,r.parent,r.piece,r.object,r.bytes,r.held,r.a,r.b,r.c,r.d,r.flags));
            }
        }
        let mut pieces = 0;
        for i in 0..self.pieces.len() {
            if let Some(r) = self.pieces[i] {
                pieces += 1;
                self.emit(|l| write!(l,"{{\"kind\":\"piece\",\"call\":{},\"id\":{},\"root\":{},\"holder\":{},\"consumer\":{},\"held\":{},\"split_from\":{},\"split_bytes\":{},\"flags\":{}}}",r.call,r.id,r.root,r.holder,r.consumer,r.held,r.split_from,r.split_bytes,r.flags));
            }
        }
        let mut backings = 0;
        for i in 0..self.backings.len() {
            if let Some(r) = self.backings[i] {
                let backing_call = self
                    .owners
                    .iter()
                    .flatten()
                    .find(|o| o.id == r.owner)
                    .map(|o| o.call)
                    .unwrap_or(0);
                backings += 1;
                self.emit(|l| write!(l,"{{\"kind\":\"backing\",\"call\":{backing_call},\"owner\":{},\"node\":{},\"parent_node\":{},\"column\":{},\"type\":{},\"object\":{},\"base\":{},\"capacity\":{},\"offset\":{},\"length\":{},\"nulls\":{},\"flags\":{},\"children\":{},\"buffers\":{}}}",r.owner,r.node,r.parent_node,r.column,r.data_type,r.object,r.base,r.capacity,r.offset,r.length,r.nulls,r.flags,r.children,r.buffers));
            }
        }
        let mut edges = 0;
        for i in 0..self.edges.len() {
            if let Some(r) = self.edges[i] {
                edges += 1;
                self.emit(|l| write!(l,"{{\"kind\":\"edge\",\"call\":{},\"from\":{},\"to\":{},\"relation\":{},\"bytes\":{}}}",r.call,r.from,r.to,r.relation,r.bytes));
            }
        }
        let complete = self.in_flight == 0 && self.poison == 0 && self.gaps == 0;
        self.emit(|l| write!(l,"{{\"kind\":\"snapshot_end\",\"call\":{call},\"snapshot\":{snapshot},\"owners\":{owners},\"pieces\":{pieces},\"backings\":{backings},\"edges\":{edges},\"complete\":{complete}}}"));
    }
}
static RECORDER: OnceLock<Mutex<Option<Recorder>>> = OnceLock::new();
fn slots<T: Clone>(count: usize) -> io::Result<Vec<Option<T>>> {
    let mut v = Vec::new();
    v.try_reserve_exact(count)
        .map_err(|_| io::Error::other("ownership arena allocation failed"))?;
    v.resize(count, None);
    Ok(v)
}
fn with<R>(f: impl FnOnce(&mut Recorder) -> R) -> Option<R> {
    if !cfg!(feature = "test_utils") {
        return None;
    }
    let lock = RECORDER.get()?;
    match lock.lock() {
        Ok(mut r) => r.as_mut().map(f),
        Err(_) => None,
    }
}
/// A single process-local capture. File creation is exclusive. Dropping without
/// `finish` deliberately leaves an uncommitted, incomplete capture.
pub struct Session {
    finished: bool,
}
impl Session {
    pub fn create(path: &Path, envelope: Envelope) -> io::Result<Self> {
        if !cfg!(feature = "test_utils") {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "enable ownership-diagnostics",
            ));
        }
        let bounds = envelope.bounds()?;
        let mut global = RECORDER
            .get_or_init(|| Mutex::new(None))
            .lock()
            .map_err(|_| io::Error::other("ownership recorder poisoned"))?;
        if global.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "ownership capture active",
            ));
        }
        let file = OpenOptions::new().create_new(true).write(true).open(path)?;
        let mut r = Recorder {
            file,
            envelope,
            bounds,
            owners: slots(envelope.slots)?,
            pieces: slots(envelope.slots)?,
            backings: slots(envelope.descriptors)?,
            edges: slots(envelope.edges)?,
            next: 1,
            call: 0,
            completed: 0,
            snapshots: 0,
            epoch: 0,
            in_flight: 0,
            poison: 0,
            gaps: 0,
            constructed: 0,
            retired: 0,
            operations: 0,
            records: 0,
            bytes: 0,
            checksum: 0xcbf29ce484222325,
        };
        r.emit(|l| write!(l,"{{\"kind\":\"session\",\"protocol\":\"terminal-ownership/1\",\"calls\":{},\"slots\":{},\"descriptors\":{},\"edges\":{},\"columns\":{},\"nodes\":{},\"depth\":{},\"buffers\":{},\"line_bytes\":{LINE_BYTES},\"record_limit\":{},\"byte_limit\":{},\"observer_bytes\":{},\"owner_bytes\":{},\"piece_bytes\":{},\"backing_bytes\":{},\"edge_bytes\":{},\"fixed_bytes\":{}}}",envelope.calls,envelope.slots,envelope.descriptors,envelope.edges,envelope.columns,envelope.nodes,envelope.depth,envelope.buffers,bounds.records,bounds.bytes,bounds.observer_bytes,size_of::<Option<OwnerRecord>>(),size_of::<Option<PieceRecord>>(),size_of::<Option<arrays::Backing>>(),size_of::<Option<EdgeRecord>>(),size_of::<Recorder>()+size_of::<Mutex<Option<Recorder>>>()+LINE_BYTES));
        if r.poison != 0 {
            return Err(io::Error::other("ownership header write failed"));
        }
        *global = Some(r);
        Ok(Self { finished: false })
    }
    pub fn begin_call(&self) -> io::Result<u64> {
        with(|r| {
            if r.call != 0 || r.completed >= r.envelope.calls as u64 {
                r.poison |= POISON_PROTOCOL;
                return Err(io::Error::other("previous call has not ended"));
            }
            r.call = r.completed + 1;
            r.snapshots = 0;
            Ok(r.call)
        })
        .unwrap_or_else(|| Err(io::Error::other("no ownership capture")))
    }
    pub fn snapshot(&self, outcome: u64) {
        snapshot(outcome);
    }
    pub fn end_call(&self, outcome: u64) {
        with(|r| {
            let (call, snapshots) = (r.call, r.snapshots);
            if call == 0 || snapshots != 2 {
                r.poison |= POISON_PROTOCOL;
            }
            let poison = r.poison;
            r.emit(|l|write!(l,"{{\"kind\":\"run_end\",\"call\":{call},\"outcome\":{outcome},\"snapshots\":{snapshots},\"poison\":{poison}}}"));
            r.completed += 1;
            r.call = 0;
        });
    }
    pub fn finish(mut self) -> io::Result<()> {
        let mut guard = RECORDER
            .get()
            .ok_or_else(|| io::Error::other("no capture"))?
            .lock()
            .map_err(|_| io::Error::other("ownership mutex poisoned"))?;
        let mut r = guard.take().ok_or_else(|| io::Error::other("no capture"))?;
        let complete = r.poison == 0
            && r.in_flight == 0
            && r.call == 0
            && r.completed == r.envelope.calls as u64;
        let (calls, records, bytes, checksum) =
            (r.completed, r.records, r.bytes, r.checksum);
        r.emit(|l|write!(l,"{{\"kind\":\"session_end\",\"calls\":{calls},\"records\":{records},\"bytes\":{bytes},\"checksum\":{checksum},\"complete\":{complete}}}"));
        self.finished = true;
        r.file.flush()?;
        if !complete || r.poison != 0 {
            return Err(io::Error::other("ownership capture incomplete"));
        }
        Ok(())
    }
}
impl Drop for Session {
    fn drop(&mut self) {
        if !self.finished {
            if let Some(lock) = RECORDER.get() {
                if let Ok(mut r) = lock.lock() {
                    *r = None;
                }
            }
        }
    }
}
pub fn snapshot(outcome: u64) {
    with(|r| r.cut(outcome));
}
pub(crate) fn gap(bits: u64) {
    with(|r| r.gaps |= bits);
}
thread_local! { static ORIGIN: Cell<(u64,u64)> = const { Cell::new((0,0)) }; }
pub(crate) fn origin() -> (u64, u64) {
    ORIGIN.with(Cell::get)
}
pub(crate) fn scoped<R>(owner: u64, piece: u64, f: impl FnOnce() -> R) -> R {
    struct Reset((u64, u64));
    impl Drop for Reset {
        fn drop(&mut self) {
            ORIGIN.with(|o| o.set(self.0));
        }
    }
    let _reset = Reset(ORIGIN.with(|o| o.replace((owner, piece))));
    f()
}
/// Numeric sentinel placed after original fields. Its drop observes retirement
/// of those references, not native allocation release or surviving aliases.
#[derive(Debug, Default)]
pub(crate) struct Life {
    #[cfg(feature = "test_utils")]
    id: u64,
}
impl Life {
    pub(crate) fn new(kind: Kind, parent: u64, piece: u64) -> Self {
        let id = with(|r| {
            if r.call == 0 {
                return 0;
            }
            let Some(index) = r.owners.iter().position(Option::is_none) else {
                r.poison |= POISON_CAPACITY;
                return 0;
            };
            let id = r.id();
            r.owners[index] = Some(OwnerRecord {
                call: r.call,
                id,
                owner_kind: kind as u64,
                parent,
                piece,
                ..Default::default()
            });
            Recorder::count(&mut r.constructed, &mut r.poison);
            id
        })
        .unwrap_or(0);
        #[cfg(feature = "test_utils")]
        {
            Self { id }
        }
        #[cfg(not(feature = "test_utils"))]
        {
            let _ = id;
            Self {}
        }
    }
    pub(crate) fn inherited(kind: Kind) -> Self {
        let (parent, piece) = origin();
        Self::new(kind, parent, piece)
    }
    pub(crate) fn id(&self) -> u64 {
        #[cfg(feature = "test_utils")]
        {
            self.id
        }
        #[cfg(not(feature = "test_utils"))]
        {
            0
        }
    }
    pub(crate) fn update(&self, f: impl FnOnce(&mut OwnerRecord)) {
        let id = self.id();
        if id == 0 {
            return;
        }
        with(|r| {
            if let Some(o) = r.owners.iter_mut().flatten().find(|o| o.id == id) {
                f(o);
            } else {
                r.poison |= POISON_PROTOCOL;
            }
        });
    }
    pub(crate) fn batch(&self, batch: &arrow::array::RecordBatch) {
        let id = self.id();
        if id == 0 {
            return;
        }
        with(|r| {
            for b in &mut r.backings {
                if b.as_ref().is_some_and(|b| b.owner == id) {
                    *b = None;
                }
            }
            let e = arrays::Envelope {
                nodes: r.envelope.nodes,
                depth: r.envelope.depth,
                buffers: r.envelope.buffers,
                columns: r.envelope.columns,
            };
            let result = arrays::visit_batch(batch, id, e, |b| {
                if let Some(slot) = r.backings.iter_mut().find(|b| b.is_none()) {
                    *slot = Some(b);
                    true
                } else {
                    false
                }
            });
            if result.is_err() {
                r.gaps |= GAP_UNSUPPORTED;
            }
        });
    }

    pub(crate) fn array(&self, array: &dyn arrow::array::Array) {
        let id = self.id();
        if id == 0 {
            return;
        }
        with(|r| {
            let e = arrays::Envelope {
                nodes: r.envelope.nodes,
                depth: r.envelope.depth,
                buffers: r.envelope.buffers,
                columns: r.envelope.columns,
            };
            let result = arrays::visit_array(array, id, e, |b| {
                if let Some(slot) = r.backings.iter_mut().find(|b| b.is_none()) {
                    *slot = Some(b);
                    true
                } else {
                    false
                }
            });
            if result.is_err() {
                r.gaps |= GAP_UNSUPPORTED;
            }
        });
    }
    pub(crate) fn raw(
        &self,
        data_type: u64,
        base: usize,
        capacity: usize,
        length: usize,
    ) {
        let id = self.id();
        if id == 0 {
            return;
        }
        with(|r| {
            let node = 1 + r
                .backings
                .iter()
                .flatten()
                .filter(|b| b.owner == id)
                .count() as u64;
            let b = arrays::Backing {
                owner: id,
                node,
                parent_node: 0,
                column: node - 1,
                data_type,
                object: base as u64,
                base: base as u64,
                capacity: capacity as u64,
                offset: 0,
                length: length as u64,
                nulls: 0,
                flags: arrays::VALUES,
                children: 0,
                buffers: 0,
            };
            if let Some(slot) = r.backings.iter_mut().find(|b| b.is_none()) {
                *slot = Some(b);
            } else {
                r.poison |= POISON_CAPACITY;
            }
        });
    }
    pub(crate) fn clear_backings(&self) {
        let id = self.id();
        with(|r| {
            for b in &mut r.backings {
                if b.as_ref().is_some_and(|b| b.owner == id) {
                    *b = None;
                }
            }
        });
    }
    pub(crate) fn edge(&self, to: u64, relation: u64, bytes: usize) {
        let from = self.id();
        if from == 0 || to == 0 {
            return;
        }
        with(|r| {
            if let Some(e) = r
                .edges
                .iter_mut()
                .flatten()
                .find(|e| e.from == from && e.to == to && e.relation == relation)
            {
                e.bytes = bytes as u64;
                return;
            }
            if let Some(e) = r.edges.iter_mut().find(|e| e.is_none()) {
                *e = Some(EdgeRecord {
                    call: r.call,
                    from,
                    to,
                    relation,
                    bytes: bytes as u64,
                });
            } else {
                r.poison |= POISON_CAPACITY;
            }
        });
    }
}
impl Drop for Life {
    fn drop(&mut self) {
        let id = self.id();
        if id == 0 {
            return;
        }
        with(|r| {
            for o in &mut r.owners {
                if let Some(o) = o {
                    if o.parent == id {
                        o.parent = 0;
                        o.flags |= 2;
                    }
                }
                if o.as_ref().is_some_and(|o| o.id == id) {
                    *o = None;
                    Recorder::count(&mut r.retired, &mut r.poison);
                }
            }
            for b in &mut r.backings {
                if b.as_ref().is_some_and(|b| b.owner == id) {
                    *b = None;
                }
            }
            for e in &mut r.edges {
                if e.as_ref().is_some_and(|e| e.to == id && e.from != id) {
                    r.gaps |= GAP_UNKNOWN_SPONSOR;
                }
                if e.as_ref().is_some_and(|e| e.from == id || e.to == id) {
                    *e = None;
                }
            }
        });
    }
}
#[derive(Debug, Default)]
pub(crate) struct Piece {
    #[cfg(feature = "test_utils")]
    id: u64,
}
impl Piece {
    pub(crate) fn new(
        reservation: &datafusion_execution::memory_pool::MemoryReservation,
        holder: u64,
    ) -> Self {
        let moved = MOVING.with(|m| m.replace(0));
        let parent = origin().1;
        let id = with(|r| {
            if r.call == 0 {
                return 0;
            }
            if moved != 0 {
                if let Some(p) = r.pieces.iter_mut().flatten().find(|p| {
                    p.id == moved && p.consumer == reservation.consumer().id() as u64
                }) {
                    p.holder = holder;
                    p.held = reservation.size() as u64;
                    return moved;
                }
                r.poison |= POISON_PROTOCOL;
                return 0;
            }
            let Some(i) = r.pieces.iter().position(Option::is_none) else {
                r.poison |= POISON_CAPACITY;
                return 0;
            };
            let ancestor = r
                .pieces
                .iter()
                .flatten()
                .find(|p| {
                    p.id == parent && p.consumer == reservation.consumer().id() as u64
                })
                .copied();
            let id = r.id();
            let root = ancestor.map_or(id, |p| p.root);
            r.pieces[i] = Some(PieceRecord {
                call: r.call,
                id,
                root,
                holder,
                consumer: reservation.consumer().id() as u64,
                held: reservation.size() as u64,
                split_from: if ancestor.is_some() { root } else { 0 },
                split_bytes: 0,
                flags: if ancestor.is_some() { 2 } else { 0 },
            });
            id
        })
        .unwrap_or(0);
        #[cfg(feature = "test_utils")]
        {
            Self { id }
        }
        #[cfg(not(feature = "test_utils"))]
        {
            let _ = id;
            Self {}
        }
    }
    /// Keep the logical piece identity across an actual take/move. The target
    /// constructor consumes MOVING synchronously; no TLS scope spans a future.
    pub(crate) fn transfer<R>(mut self, owner: u64, f: impl FnOnce() -> R) -> R {
        let id = self.id();
        #[cfg(feature = "test_utils")]
        {
            self.id = 0;
        }
        struct Reset(u64);
        impl Drop for Reset {
            fn drop(&mut self) {
                let unclaimed = MOVING.with(|m| m.replace(self.0));
                if unclaimed != 0 {
                    drop(Piece::adopt(unclaimed));
                }
            }
        }
        let _reset = Reset(MOVING.with(|m| m.replace(id)));
        scoped(owner, id, f)
    }
    pub(crate) fn adopt(id: u64) -> Self {
        #[cfg(feature = "test_utils")]
        {
            Self { id }
        }
        #[cfg(not(feature = "test_utils"))]
        {
            let _ = id;
            Self {}
        }
    }
    pub(crate) fn id(&self) -> u64 {
        #[cfg(feature = "test_utils")]
        {
            self.id
        }
        #[cfg(not(feature = "test_utils"))]
        {
            0
        }
    }
    pub(crate) fn refresh(
        &self,
        reservation: &datafusion_execution::memory_pool::MemoryReservation,
        holder: u64,
    ) {
        let id = self.id();
        if id == 0 {
            return;
        }
        with(|r| {
            if let Some(p) = r.pieces.iter_mut().flatten().find(|p| p.id == id) {
                p.held = reservation.size() as u64;
                p.holder = holder;
            }
        });
    }
    pub(crate) fn child(
        &self,
        reservation: &datafusion_execution::memory_pool::MemoryReservation,
        holder: u64,
        bytes: usize,
    ) -> Self {
        let child = Self::new(reservation, holder);
        let (id, child_id) = (self.id(), child.id());
        with(|r| {
            let root = r
                .pieces
                .iter()
                .flatten()
                .find(|p| p.id == id)
                .map(|p| p.root)
                .unwrap_or(0);
            if let Some(p) = r.pieces.iter_mut().flatten().find(|p| p.id == child_id) {
                p.root = root;
                p.split_from = root;
                p.split_bytes = bytes as u64;
                p.flags = 1;
            }
        });
        child
    }
}
impl Drop for Piece {
    fn drop(&mut self) {
        let id = self.id();
        if id == 0 {
            return;
        }
        with(|r| {
            let root = r
                .pieces
                .iter()
                .flatten()
                .find(|p| p.id == id)
                .map(|p| p.root)
                .unwrap_or(0);
            let children = r
                .pieces
                .iter()
                .flatten()
                .any(|p| p.id != id && p.root == id);
            for p in &mut r.pieces {
                if p.as_ref().is_some_and(|p| p.id == id) {
                    if children {
                        let p = p.as_mut().unwrap();
                        p.holder = 0;
                        p.held = 0;
                        p.flags |= 4;
                    } else {
                        *p = None;
                    }
                }
            }
            // Retired root certificates have bounded lifetime: only live descendants
            // keep them. Moving/taking a piece keeps its identity, never a history chain.
            if root != id
                && !r
                    .pieces
                    .iter()
                    .flatten()
                    .any(|p| p.id != root && p.root == root)
            {
                for p in &mut r.pieces {
                    if p.as_ref().is_some_and(|p| p.id == root && p.flags & 4 != 0) {
                        *p = None;
                    }
                }
            }
        });
    }
}
/// Epoch guard. Never owns the observer lock while product work executes.
pub(crate) struct Transition {
    active: bool,
}
impl Transition {
    pub(crate) fn begin() -> Self {
        let active = with(|r| {
            Recorder::count(&mut r.in_flight, &mut r.poison);
            Recorder::count(&mut r.epoch, &mut r.poison);
            Recorder::count(&mut r.operations, &mut r.poison);
        })
        .is_some();
        Self { active }
    }
}
impl Drop for Transition {
    fn drop(&mut self) {
        if self.active {
            with(|r| {
                if r.in_flight == 0 {
                    r.poison |= POISON_PROTOCOL;
                } else {
                    r.in_flight -= 1;
                }
                Recorder::count(&mut r.epoch, &mut r.poison);
            });
        }
    }
}

#[cfg(any(test, feature = "test_utils"))]
mod tests;

/// Receiver/producer observations wrap existing owners without changing channel
/// capacity, permit acquisition, item type, FIFO order or cancellation behavior.
pub(crate) struct QueueStream {
    inner: crate::SendableRecordBatchStream,
    retired: QueueRetirement,
    diagnostic: Life,
}
struct QueueRetirement(u64);
impl Drop for QueueRetirement {
    fn drop(&mut self) {
        loop {
            let id = with(|r| {
                r.owners
                    .iter()
                    .flatten()
                    .find(|o| {
                        o.parent == self.0
                            && o.owner_kind == Kind::Batch as u64
                            && o.d == 1
                    })
                    .map(|o| o.id)
            })
            .flatten();
            if let Some(id) = id {
                retire_id(id);
            } else {
                break;
            }
        }
    }
}
impl QueueStream {
    pub(crate) fn new(inner: crate::SendableRecordBatchStream, diagnostic: Life) -> Self {
        Self {
            inner,
            retired: QueueRetirement(diagnostic.id()),
            diagnostic,
        }
    }
}
impl futures::Stream for QueueStream {
    type Item = datafusion_common::Result<arrow::array::RecordBatch>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let _transition = Transition::begin();
        let result = self.inner.as_mut().poll_next(cx);
        if matches!(result, std::task::Poll::Ready(Some(_))) {
            let ordinal = self.diagnostic.increment(1);
            queue_receive(self.diagnostic.id(), ordinal);
        }
        result
    }
}
impl crate::RecordBatchStream for QueueStream {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.inner.schema()
    }
}
pub(crate) struct ProducerInput {
    pub(crate) input: crate::SendableRecordBatchStream,
    pub(crate) diagnostic: Life,
}

/// Original IntoIter owner followed only by numeric sentinels. Yielded batches
/// leave this ownership occurrence; no batch is cloned or retained by evidence.
pub(crate) struct Chunks {
    inner: std::vec::IntoIter<arrow::array::RecordBatch>,
    lives: std::vec::IntoIter<Life>,
}
impl Chunks {
    pub(crate) fn new(
        batches: Vec<arrow::array::RecordBatch>,
        parent: u64,
        piece: u64,
    ) -> Self {
        let mut lives = Vec::new();
        let limit = with(|r| r.envelope.slots).unwrap_or(0);
        if batches.len() > limit || lives.try_reserve_exact(batches.len()).is_err() {
            gap(GAP_UNSUPPORTED);
        } else {
            for batch in &batches {
                let life = Life::new(Kind::Batch, parent, piece);
                life.update(|o| {
                    o.a = batch.num_rows() as u64;
                    o.b = batch.num_columns() as u64;
                });
                life.batch(batch);
                lives.push(life);
            }
        }
        Self {
            inner: batches.into_iter(),
            lives: lives.into_iter(),
        }
    }
}
impl Iterator for Chunks {
    type Item = arrow::array::RecordBatch;
    fn next(&mut self) -> Option<Self::Item> {
        let next = self.inner.next();
        if next.is_some() {
            if let Some(life) = self.lives.next() {
                publish_handoff(life.into_id());
            }
        }
        next
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

thread_local! { static MOVING: Cell<u64> = const {Cell::new(0)}; }
thread_local! { static HANDOFF: Cell<u64> = const {Cell::new(0)}; }
pub(crate) fn clear_handoff() {
    HANDOFF.with(|h| {
        let old = h.replace(0);
        if old != 0 {
            gap(GAP_UNKNOWN_SPONSOR);
            retire_id(old);
        }
    });
}
pub(crate) fn take_handoff() -> Life {
    Life::adopt(HANDOFF.with(|h| h.replace(0)))
}
fn publish_handoff(id: u64) {
    HANDOFF.with(|h| {
        if h.replace(id) != 0 {
            gap(GAP_UNKNOWN_SPONSOR);
        }
    });
}
impl Life {
    pub(crate) fn into_id(mut self) -> u64 {
        let id = self.id();
        #[cfg(feature = "test_utils")]
        {
            self.id = 0;
        }
        id
    }
    pub(crate) fn adopt(id: u64) -> Self {
        #[cfg(feature = "test_utils")]
        {
            Self { id }
        }
        #[cfg(not(feature = "test_utils"))]
        {
            let _ = id;
            Self {}
        }
    }
    pub(crate) fn queued(self, queue: u64, ordinal: u64) {
        self.update(|o| {
            o.parent = queue;
            o.c = ordinal;
            o.d = 1;
        });
        let _ = self.into_id();
    }
    pub(crate) fn increment(&self, field: u8) -> u64 {
        let id = self.id();
        with(|r| {
            if let Some(o) = r.owners.iter_mut().flatten().find(|o| o.id == id) {
                let value = match field {
                    0 => &mut o.a,
                    1 => &mut o.b,
                    2 => &mut o.c,
                    _ => &mut o.d,
                };
                let old = *value;
                Recorder::count(value, &mut r.poison);
                old
            } else {
                0
            }
        })
        .unwrap_or(0)
    }
}
fn retire_id(id: u64) {
    drop(Life::adopt(id));
}
pub(crate) fn queue_receive(queue: u64, ordinal: u64) {
    let id = with(|r| {
        r.owners
            .iter_mut()
            .flatten()
            .find(|o| {
                o.owner_kind == Kind::Batch as u64
                    && o.parent == queue
                    && o.c == ordinal
                    && o.d == 1
            })
            .map(|o| {
                o.d = 2;
                o.id
            })
    })
    .flatten()
    .unwrap_or(0);
    if id != 0 {
        publish_handoff(id);
    }
}

impl Life {
    pub(crate) fn rows(&self, rows: &arrow::row::Rows) {
        #[cfg(feature = "test_utils")]
        {
            let storage = rows.diagnostic_storage();
            self.clear_backings();
            self.update(|o| {
                o.a = rows.num_rows() as u64;
                o.b = 3;
                o.bytes = rows.size() as u64;
                o.object = storage.config_ptr as u64;
                o.flags |= 8;
            });
            self.raw(
                36,
                storage.buffer_ptr,
                storage.buffer_capacity,
                storage.buffer_len,
            );
            match (
                storage.offsets_capacity.checked_mul(size_of::<usize>()),
                storage.offsets_len.checked_mul(size_of::<usize>()),
            ) {
                (Some(cap), Some(len)) => self.raw(37, storage.offsets_ptr, cap, len),
                _ => gap(GAP_ROWS_STORAGE),
            }
            self.raw(
                38,
                storage.fields_ptr,
                storage.fields_bytes,
                storage.fields_bytes,
            );
            if !storage.fields_metadata_complete {
                gap(GAP_ROWS_STORAGE);
            }
        }
        #[cfg(not(feature = "test_utils"))]
        {
            let _ = rows;
        }
    }
    pub(crate) fn converter(&self, converter: &arrow::row::RowConverter) {
        #[cfg(feature = "test_utils")]
        {
            let storage = converter.diagnostic_storage();
            self.clear_backings();
            self.update(|o| o.b = 2);
            self.raw(
                38,
                storage.fields_ptr,
                storage.fields_bytes,
                storage.fields_bytes,
            );
            match (
                storage.codecs_capacity.checked_mul(storage.codec_bytes),
                storage.codecs_len.checked_mul(storage.codec_bytes),
            ) {
                (Some(cap), Some(len)) => self.raw(39, storage.codecs_ptr, cap, len),
                _ => gap(GAP_ROWS_STORAGE),
            }
            if !storage.stateless_codecs || !storage.fields_metadata_complete {
                gap(GAP_ROWS_STORAGE);
            }
        }
        #[cfg(not(feature = "test_utils"))]
        {
            let _ = converter;
        }
    }
}
#[doc(hidden)]
#[cfg(feature = "test_utils")]
pub fn verify_recorder_controls() {
    tests::run_controls();
    arrays::verify_controls();
}
