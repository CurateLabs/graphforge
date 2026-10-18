//! Opt-in, bounded development evidence for selected sort ownership boundaries.
//!
//! Addresses are observations scoped to live generation IDs, never funding IDs.
//! Product operations run outside this recorder's mutex. A snapshot with an open
//! transition is explicitly incomplete; this is not a query-wide atomic inventory.
use std::fmt::{self, Write as _};
use std::fs::{File, OpenOptions};
use std::io::{self, Write as _};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex, OnceLock};

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
/// Flags: 1 opaque layout; 2 LIVE owner whose construction parent retired
/// (creation history unavailable; parent is current observed containment);
/// 4 metadata-only occurrence; 8 Rows raw-storage occurrence. `coverage` is a
/// live semantic gap mask and is carried through numeric ownership handoffs.
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
    pub coverage: u64,
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
    pub metadata_per_slot: usize,
    pub tls_bytes: usize,
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
            || self.depth > 16
            || self.columns > self.descriptors
            || self.nodes > self.descriptors
            || self.buffers > self.descriptors
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
        if records > 100000 || bytes > 64 * 1024 * 1024 {
            return Err(bad());
        }
        // Named sum dominates each owner's added fields, one numeric sidecar
        // element and the simultaneously live exact-reserve sidecar headers.
        // No sidecar grows after construction; guard elements consume owner slots.
        let metadata_per_slot = 4 * size_of::<Life>()
            + 2 * size_of::<Piece>()
            + 2 * size_of::<Option<Transition>>()
            + size_of::<Vec<Life>>()
            + size_of::<std::vec::IntoIter<Life>>()
            + size_of::<QueueStream>()
            + size_of::<QueueRetirement>()
            + 2 * size_of::<Option<ThreadState>>();
        // Bridge state is in bounded arena slots, not per-thread TLS. A slot
        // retires as soon as origin/move/handoff are all empty; thread churn
        // therefore consumes no cumulative allowance.
        let tls_bytes = 0;
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
                        + size_of::<Condvar>()
                        + 3 * size_of::<AtomicU64>()
                        + LINE_BYTES,
                )
            })
            .and_then(|v| {
                self.slots
                    .checked_mul(metadata_per_slot)
                    .and_then(|m| v.checked_add(m))
            })
            .and_then(|v| v.checked_add(tls_bytes))
            .ok_or_else(bad)?;
        Ok(Bounds {
            records,
            bytes,
            observer_bytes,
            metadata_per_slot,
            tls_bytes,
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
#[derive(Clone, Copy, Default)]
struct Anchor {
    attempt: u64,
    stage: u64,
    failed_piece: u64,
    consumer: u64,
    request: u64,
    error_ptr: u64,
    error_len: u64,
    error_hash: u64,
    error_truncated: bool,
}
fn error_anchor(error: &datafusion_common::DataFusionError) -> Anchor {
    if let datafusion_common::DataFusionError::ResourcesExhausted(message) =
        error.find_root()
    {
        let mut hash = 0xcbf29ce484222325u64;
        for b in message.as_bytes().iter().take(65536) {
            hash = (hash ^ u64::from(*b)).wrapping_mul(0x100000001b3);
        }
        Anchor {
            error_ptr: message.as_ptr() as usize as u64,
            error_len: message.len() as u64,
            error_hash: hash,
            error_truncated: message.len() > 65536,
            ..Default::default()
        }
    } else {
        Anchor::default()
    }
}
#[derive(Clone, Copy)]
struct ThreadState {
    thread: std::thread::ThreadId,
    origin: (u64, u64),
    moving: (u64, u64),
    handoff: u64,
}
impl ThreadState {
    fn empty(thread: std::thread::ThreadId) -> Self {
        Self {
            thread,
            origin: (0, 0),
            moving: (0, 0),
            handoff: 0,
        }
    }
    fn vacant(&self) -> bool {
        self.origin == (0, 0) && self.moving == (0, 0) && self.handoff == 0
    }
}
struct Recorder {
    generation: u64,
    file: File,
    anchor: Anchor,
    threads: Vec<Option<ThreadState>>,
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
        let id = next_identifier(&NEXT_IDENTIFIER).unwrap_or_else(|| {
            self.poison |= POISON_ARITHMETIC;
            0
        });
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
        let live_gaps = self
            .owners
            .iter()
            .flatten()
            .fold(self.gaps, |g, o| g | o.coverage);
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
            live_gaps,
            self.constructed,
            self.retired,
            self.operations,
        );
        let anchor = if snapshot == 1 {
            self.anchor
        } else {
            Anchor::default()
        };
        self.emit(|l| write!(l,"{{\"kind\":\"snapshot_begin\",\"call\":{call},\"snapshot\":{snapshot},\"outcome\":{outcome},\"epoch\":{epoch},\"in_flight\":{in_flight},\"poison\":{poison},\"gaps\":{gaps},\"constructed\":{constructed},\"retired\":{retired},\"operations\":{operations},\"attempt\":{},\"stage\":{},\"failed_piece\":{},\"consumer\":{},\"request\":{},\"error_ptr\":{},\"error_len\":{},\"error_hash\":{},\"error_truncated\":{}}}",anchor.attempt,anchor.stage,anchor.failed_piece,anchor.consumer,anchor.request,anchor.error_ptr,anchor.error_len,anchor.error_hash,anchor.error_truncated));
        let mut owners = 0;
        for i in 0..self.owners.len() {
            if let Some(r) = self.owners[i] {
                owners += 1;
                self.emit(|l| write!(l,"{{\"kind\":\"owner\",\"call\":{},\"id\":{},\"owner_kind\":{},\"parent\":{},\"piece\":{},\"object\":{},\"bytes\":{},\"held\":{},\"a\":{},\"b\":{},\"c\":{},\"d\":{},\"flags\":{},\"coverage\":{}}}",r.call,r.id,r.owner_kind,r.parent,r.piece,r.object,r.bytes,r.held,r.a,r.b,r.c,r.d,r.flags,r.coverage));
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
        let complete = self.in_flight == 0 && self.poison == 0 && live_gaps == 0;
        self.emit(|l| write!(l,"{{\"kind\":\"snapshot_end\",\"call\":{call},\"snapshot\":{snapshot},\"owners\":{owners},\"pieces\":{pieces},\"backings\":{backings},\"edges\":{edges},\"complete\":{complete}}}"));
    }
}
static RECORDER: OnceLock<Mutex<Option<Recorder>>> = OnceLock::new();
static RETIREMENT: Condvar = Condvar::new();
// Never reused, including when an abandoned capture leaves product owners live.
static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);
// Raw IDs cross a few existing numeric hook boundaries. They never alias a
// later Session, even if that Session starts between the caller and insertion.
static NEXT_IDENTIFIER: AtomicU64 = AtomicU64::new(1);
fn next_identifier(counter: &AtomicU64) -> Option<u64> {
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
        .ok()
}
static ACTIVE_GENERATION: AtomicU64 = AtomicU64::new(0);
fn generation() -> u64 {
    #[cfg(feature = "test_utils")]
    {
        ACTIVE_GENERATION.load(Ordering::Acquire)
    }
    #[cfg(not(feature = "test_utils"))]
    {
        0
    }
}
fn with_generation<R>(generation: u64, f: impl FnOnce(&mut Recorder) -> R) -> Option<R> {
    if generation == 0 {
        return None;
    }
    with(|r| (r.generation == generation).then(|| f(r))).flatten()
}
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
    generation: u64,
    finished: bool,
}
impl Session {
    pub fn create(path: &Path, envelope: Envelope) -> io::Result<Self> {
        if !cfg!(feature = "test_utils") {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "enable development test_utils",
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
        let generation = NEXT_GENERATION
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |g| g.checked_add(1))
            .map_err(|_| io::Error::other("ownership session generation exhausted"))?;
        let file = OpenOptions::new().create_new(true).write(true).open(path)?;
        let mut r = Recorder {
            generation,
            file,
            anchor: Anchor::default(),
            threads: slots(
                envelope
                    .slots
                    .checked_mul(2)
                    .ok_or_else(|| io::Error::other("thread bridge overflow"))?,
            )?,
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
        r.emit(|l| write!(l,"{{\"kind\":\"session\",\"protocol\":\"terminal-ownership/1\",\"calls\":{},\"slots\":{},\"descriptors\":{},\"edges\":{},\"columns\":{},\"nodes\":{},\"depth\":{},\"buffers\":{},\"line_bytes\":{LINE_BYTES},\"record_limit\":{},\"byte_limit\":{},\"observer_bytes\":{},\"owner_bytes\":{},\"piece_bytes\":{},\"backing_bytes\":{},\"edge_bytes\":{},\"fixed_bytes\":{},\"metadata_per_slot\":{},\"tls_bytes\":{}}}",envelope.calls,envelope.slots,envelope.descriptors,envelope.edges,envelope.columns,envelope.nodes,envelope.depth,envelope.buffers,bounds.records,bounds.bytes,bounds.observer_bytes,size_of::<Option<OwnerRecord>>(),size_of::<Option<PieceRecord>>(),size_of::<Option<arrays::Backing>>(),size_of::<Option<EdgeRecord>>(),size_of::<Recorder>()+size_of::<Mutex<Option<Recorder>>>()+size_of::<Condvar>()+3*size_of::<AtomicU64>()+LINE_BYTES,bounds.metadata_per_slot,bounds.tls_bytes));
        if r.poison != 0 {
            return Err(io::Error::other("ownership header write failed"));
        }
        *global = Some(r);
        ACTIVE_GENERATION.store(generation, Ordering::Release);
        Ok(Self {
            generation,
            finished: false,
        })
    }
    /// Wait for actual numeric sentinels after original fields have dropped.
    /// This is a development synchronization primitive, not a timing oracle.
    pub fn wait_for_retirement(&self, kind: Kind) -> io::Result<()> {
        let mut guard = RECORDER
            .get()
            .ok_or_else(|| io::Error::other("no capture"))?
            .lock()
            .map_err(|_| io::Error::other("ownership mutex poisoned"))?;
        loop {
            let r = guard
                .as_ref()
                .filter(|r| r.generation == self.generation)
                .ok_or_else(|| io::Error::other("ownership session retired"))?;
            if !r
                .owners
                .iter()
                .flatten()
                .any(|o| o.owner_kind == kind as u64)
            {
                return Ok(());
            }
            guard = RETIREMENT
                .wait(guard)
                .map_err(|_| io::Error::other("ownership mutex poisoned"))?;
        }
    }
    pub fn begin_call(&self) -> io::Result<u64> {
        with_generation(self.generation, |r| {
            if r.call != 0 || r.completed >= r.envelope.calls as u64 {
                r.poison |= POISON_PROTOCOL;
                return Err(io::Error::other("previous call has not ended"));
            }
            r.call = r.completed + 1;
            r.snapshots = 0;
            r.anchor = Anchor::default();
            Ok(r.call)
        })
        .unwrap_or_else(|| Err(io::Error::other("no ownership capture")))
    }
    pub fn snapshot(&self, outcome: u64) {
        with_generation(self.generation, |r| r.cut(outcome));
    }
    pub fn end_call(&self, outcome: u64) {
        self.end_call_key(outcome, Anchor::default());
    }
    /// Corroborate the selected refusal with the actual error returned to the
    /// development caller. Pointer/length/hash alone are not generation IDs.
    pub fn end_call_error(&self, error: &datafusion_common::DataFusionError) {
        self.end_call_key(1, error_anchor(error));
    }
    fn end_call_key(&self, outcome: u64, key: Anchor) {
        with_generation(self.generation, |r| {
            let (call, snapshots) = (r.call, r.snapshots);
            if call == 0 || snapshots != 2 {
                r.poison |= POISON_PROTOCOL;
            }
            let poison = r.poison;
            r.emit(|l|write!(l,"{{\"kind\":\"run_end\",\"call\":{call},\"outcome\":{outcome},\"snapshots\":{snapshots},\"poison\":{poison},\"error_ptr\":{},\"error_len\":{},\"error_hash\":{},\"error_truncated\":{}}}",key.error_ptr,key.error_len,key.error_hash,key.error_truncated));
            Recorder::count(&mut r.completed, &mut r.poison);
            r.call = 0;
        });
    }
    pub fn finish(mut self) -> io::Result<()> {
        let mut guard = RECORDER
            .get()
            .ok_or_else(|| io::Error::other("no capture"))?
            .lock()
            .map_err(|_| io::Error::other("ownership mutex poisoned"))?;
        if !guard
            .as_ref()
            .is_some_and(|r| r.generation == self.generation)
        {
            return Err(io::Error::other("ownership session retired"));
        }
        let mut r = guard.take().ok_or_else(|| io::Error::other("no capture"))?;
        ACTIVE_GENERATION.store(0, Ordering::Release);
        RETIREMENT.notify_all();
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
                    if r.as_ref().is_some_and(|r| r.generation == self.generation) {
                        *r = None;
                        ACTIVE_GENERATION.store(0, Ordering::Release);
                        RETIREMENT.notify_all();
                    }
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
fn read_thread() -> ThreadState {
    let thread = std::thread::current().id();
    with(|r| {
        r.threads
            .iter()
            .flatten()
            .find(|t| t.thread == thread)
            .copied()
    })
    .flatten()
    .unwrap_or_else(|| ThreadState::empty(thread))
}
fn update_thread_generation<R>(
    generation: u64,
    f: impl FnOnce(&mut ThreadState) -> R,
) -> Option<R> {
    if generation == 0 {
        return None;
    }
    let thread = std::thread::current().id();
    with_generation(generation, |r| {
        let old = r
            .threads
            .iter()
            .position(|t| t.as_ref().is_some_and(|t| t.thread == thread));
        let mut state = old
            .and_then(|i| r.threads[i])
            .unwrap_or_else(|| ThreadState::empty(thread));
        let result = f(&mut state);
        if state.vacant() {
            if let Some(i) = old {
                r.threads[i] = None;
            }
        } else if let Some(i) = old {
            r.threads[i] = Some(state);
        } else if let Some(i) = r.threads.iter().position(Option::is_none) {
            r.threads[i] = Some(state);
        } else {
            r.poison |= POISON_CAPACITY;
        }
        result
    })
}
pub(crate) fn origin() -> (u64, u64) {
    if generation() == 0 {
        return (0, 0);
    }
    read_thread().origin
}
pub(crate) fn scoped<R>(owner: u64, piece: u64, f: impl FnOnce() -> R) -> R {
    scoped_generation(generation(), owner, piece, f)
}
fn scoped_generation<R>(
    generation: u64,
    owner: u64,
    piece: u64,
    f: impl FnOnce() -> R,
) -> R {
    if generation == 0 {
        return f();
    }
    struct Reset((u64, u64), u64);
    impl Drop for Reset {
        fn drop(&mut self) {
            update_thread_generation(self.1, |t| t.origin = self.0);
        }
    }
    let old = update_thread_generation(generation, |t| {
        std::mem::replace(&mut t.origin, (owner, piece))
    })
    .unwrap_or((0, 0));
    let _reset = Reset(old, generation);
    f()
}
/// Numeric sentinel placed after original fields. Its drop observes retirement
/// of those references, not native allocation release or surviving aliases.
#[derive(Debug, Default)]
pub(crate) struct Life {
    #[cfg(feature = "test_utils")]
    generation: u64,
    #[cfg(feature = "test_utils")]
    id: u64,
    #[cfg(feature = "test_utils")]
    dropping: bool,
}
impl Life {
    fn generation(&self) -> u64 {
        #[cfg(feature = "test_utils")]
        {
            self.generation
        }
        #[cfg(not(feature = "test_utils"))]
        {
            0
        }
    }
    fn insert(r: &mut Recorder, kind: Kind, parent: u64, piece: u64) -> (u64, u64) {
        if r.call == 0 {
            r.poison |= POISON_PROTOCOL;
            return (0, r.generation);
        }
        if r.poison != 0 {
            return (0, r.generation);
        }
        if (parent != 0 && !r.owners.iter().flatten().any(|o| o.id == parent))
            || (piece != 0 && !r.pieces.iter().flatten().any(|p| p.id == piece))
        {
            r.gaps |= GAP_UNKNOWN_SPONSOR;
            return (0, r.generation);
        }
        let Some(index) = r.owners.iter().position(Option::is_none) else {
            r.poison |= POISON_CAPACITY;
            return (0, r.generation);
        };
        let id = r.id();
        if id == 0 {
            return (0, r.generation);
        }
        r.owners[index] = Some(OwnerRecord {
            call: r.call,
            id,
            owner_kind: kind as u64,
            parent,
            piece,
            ..Default::default()
        });
        Recorder::count(&mut r.constructed, &mut r.poison);
        (id, r.generation)
    }
    fn from_record((id, generation): (u64, u64)) -> Self {
        #[cfg(feature = "test_utils")]
        {
            Self {
                id,
                generation,
                dropping: false,
            }
        }
        #[cfg(not(feature = "test_utils"))]
        {
            let _ = (id, generation);
            Self {}
        }
    }
    pub(crate) fn new(kind: Kind, parent: u64, piece: u64) -> Self {
        Self::new_generation(kind, parent, piece, generation())
    }
    fn new_generation(kind: Kind, parent: u64, piece: u64, generation: u64) -> Self {
        Self::from_record(
            with_generation(generation, |r| Self::insert(r, kind, parent, piece))
                .unwrap_or((0, 0)),
        )
    }
    pub(crate) fn inherited(kind: Kind) -> Self {
        if generation() == 0 {
            return Self::default();
        }
        // The thread bridge and owner insertion belong to the same capture.
        Self::from_record(
            with(|r| {
                let thread = std::thread::current().id();
                let (parent, piece) = r
                    .threads
                    .iter()
                    .flatten()
                    .find(|t| t.thread == thread)
                    .map_or((0, 0), |t| t.origin);
                Self::insert(r, kind, parent, piece)
            })
            .unwrap_or((0, 0)),
        )
    }
    pub(crate) fn id(&self) -> u64 {
        #[cfg(feature = "test_utils")]
        {
            if self.generation == generation() {
                self.id
            } else {
                0
            }
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
        with_generation(self.generation(), |r| {
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
        with_generation(self.generation(), |r| {
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
            if let Err(error) = result {
                if matches!(error, arrays::Error::Quota | arrays::Error::Callback) {
                    r.poison |= POISON_CAPACITY;
                } else if let Some(o) = r.owners.iter_mut().flatten().find(|o| o.id == id)
                {
                    o.coverage |= GAP_UNSUPPORTED;
                }
            }
        });
    }

    pub(crate) fn array(&self, array: &dyn arrow::array::Array) {
        let id = self.id();
        if id == 0 {
            return;
        }
        with_generation(self.generation(), |r| {
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
            if let Err(error) = result {
                if matches!(error, arrays::Error::Quota | arrays::Error::Callback) {
                    r.poison |= POISON_CAPACITY;
                } else if let Some(o) = r.owners.iter_mut().flatten().find(|o| o.id == id)
                {
                    o.coverage |= GAP_UNSUPPORTED;
                }
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
        with_generation(self.generation(), |r| {
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
    pub(crate) fn gap(&self, bits: u64) {
        self.update(|o| o.coverage |= bits);
    }
    pub(crate) fn clear_backings(&self) {
        let id = self.id();
        with_generation(self.generation(), |r| {
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
        with_generation(self.generation(), |r| {
            if !r.owners.iter().flatten().any(|o| o.id == to) {
                r.gaps |= GAP_UNKNOWN_SPONSOR;
                return;
            }
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
        with_generation(self.generation(), |r| {
            let coverage = r
                .owners
                .iter()
                .flatten()
                .find(|o| o.id == id)
                .map_or(0, |o| o.coverage);
            if coverage != 0 {
                for edge in r.edges.iter().flatten() {
                    if edge.to == id && edge.from != id {
                        if let Some(o) =
                            r.owners.iter_mut().flatten().find(|o| o.id == edge.from)
                        {
                            o.coverage |= coverage;
                        }
                    }
                }
            }
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
                    RETIREMENT.notify_all();
                }
            }
            for b in &mut r.backings {
                if b.as_ref().is_some_and(|b| b.owner == id) {
                    *b = None;
                }
            }
            for e in &mut r.edges {
                if r.in_flight == 0
                    && e.as_ref().is_some_and(|e| e.to == id && e.from != id)
                {
                    r.gaps |= GAP_UNKNOWN_SPONSOR;
                }
                if e.as_ref().is_some_and(|e| e.from == id || e.to == id) {
                    *e = None;
                }
            }
            #[cfg(feature = "test_utils")]
            if self.dropping {
                if r.in_flight == 0 {
                    r.poison |= POISON_PROTOCOL;
                } else {
                    r.in_flight -= 1;
                }
                Recorder::count(&mut r.epoch, &mut r.poison);
            }
        });
    }
}
#[derive(Debug, Default)]
pub(crate) struct Piece {
    #[cfg(feature = "test_utils")]
    generation: u64,
    #[cfg(feature = "test_utils")]
    id: u64,
}
impl Piece {
    fn generation(&self) -> u64 {
        #[cfg(feature = "test_utils")]
        {
            self.generation
        }
        #[cfg(not(feature = "test_utils"))]
        {
            0
        }
    }
    pub(crate) fn new(
        reservation: &datafusion_execution::memory_pool::MemoryReservation,
        holder: u64,
    ) -> Self {
        let generation = generation();
        if generation == 0 {
            return Self::default();
        }
        Self::new_generation(reservation, holder, generation)
    }
    fn new_generation(
        reservation: &datafusion_execution::memory_pool::MemoryReservation,
        holder: u64,
        generation: u64,
    ) -> Self {
        let (id, generation) = with_generation(generation, |r| {
            let thread = std::thread::current().id();
            let state = r
                .threads
                .iter()
                .flatten()
                .find(|t| t.thread == thread)
                .copied()
                .unwrap_or_else(|| ThreadState::empty(thread));
            let (moved, target) = state.moving;
            let parent = state.origin.1;
            if r.call == 0 {
                r.poison |= POISON_PROTOCOL;
                return (0, r.generation);
            }
            if r.poison != 0 {
                return (0, r.generation);
            }
            if holder == 0
                || !r.owners.iter().flatten().any(|o| o.id == holder)
                || (parent != 0 && !r.pieces.iter().flatten().any(|p| p.id == parent))
            {
                r.gaps |= GAP_UNKNOWN_SPONSOR;
                return (0, r.generation);
            }
            if moved != 0 {
                let kind = r
                    .owners
                    .iter()
                    .flatten()
                    .find(|o| o.id == holder)
                    .map(|o| o.owner_kind)
                    .unwrap_or(0);
                if let Some(p) = r.pieces.iter_mut().flatten().find(|p| {
                    p.id == moved
                        && p.consumer == reservation.consumer().id() as u64
                        && p.held == reservation.size() as u64
                        && (target == 0 || target == kind)
                }) {
                    let thread = std::thread::current().id();
                    if let Some(t) =
                        r.threads.iter_mut().flatten().find(|t| t.thread == thread)
                    {
                        t.moving = (0, 0);
                    }
                    p.holder = holder;
                    return (moved, r.generation);
                }
            }
            let Some(i) = r.pieces.iter().position(Option::is_none) else {
                r.poison |= POISON_CAPACITY;
                return (0, r.generation);
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
            (id, r.generation)
        })
        .unwrap_or((0, 0));
        #[cfg(feature = "test_utils")]
        {
            Self { id, generation }
        }
        #[cfg(not(feature = "test_utils"))]
        {
            let _ = (id, generation);
            Self {}
        }
    }
    /// Keep the logical piece identity across an actual take/move. The target
    /// constructor consumes MOVING synchronously; no TLS scope spans a future.
    pub(crate) fn transfer<R>(self, owner: u64, f: impl FnOnce() -> R) -> R {
        self.transfer_to(owner, 0, f)
    }
    pub(crate) fn transfer_to<R>(
        mut self,
        owner: u64,
        target: u64,
        f: impl FnOnce() -> R,
    ) -> R {
        let id = self.id();
        if id == 0 {
            return f();
        }
        let _transition = Transition::begin_generation(self.generation());
        #[cfg(feature = "test_utils")]
        {
            self.id = 0;
        }
        struct Reset((u64, u64), u64);
        impl Drop for Reset {
            fn drop(&mut self) {
                let (unclaimed, _) = update_thread_generation(self.1, |t| {
                    std::mem::replace(&mut t.moving, self.0)
                })
                .unwrap_or((0, 0));
                if unclaimed != 0 {
                    drop(Piece::adopt_generation(unclaimed, self.1));
                }
            }
        }
        let _reset = Reset(
            update_thread_generation(self.generation(), |t| {
                std::mem::replace(&mut t.moving, (id, target))
            })
            .unwrap_or((0, 0)),
            self.generation(),
        );
        scoped_generation(self.generation(), owner, id, f)
    }
    fn adopt_generation(id: u64, generation: u64) -> Self {
        #[cfg(feature = "test_utils")]
        {
            Self { id, generation }
        }
        #[cfg(not(feature = "test_utils"))]
        {
            let _ = (id, generation);
            Self {}
        }
    }
    pub(crate) fn id(&self) -> u64 {
        #[cfg(feature = "test_utils")]
        {
            if self.generation == generation() {
                self.id
            } else {
                0
            }
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
        with_generation(self.generation(), |r| {
            if let Some(p) = r.pieces.iter_mut().flatten().find(|p| p.id == id) {
                p.held = reservation.size() as u64;
                p.holder = holder;
            }
        });
    }
    pub(crate) fn empty_successor(
        &self,
        reservation: &datafusion_execution::memory_pool::MemoryReservation,
        holder: u64,
    ) -> Self {
        if self.id() == 0 {
            return Self::default();
        }
        scoped_generation(self.generation(), holder, self.id(), || {
            Self::new_generation(reservation, holder, self.generation())
        })
    }
    pub(crate) fn child(
        &self,
        reservation: &datafusion_execution::memory_pool::MemoryReservation,
        holder: u64,
        bytes: usize,
    ) -> Self {
        if self.id() == 0 {
            return Self::default();
        }
        let child = Self::new_generation(reservation, holder, self.generation());
        let (id, child_id) = (self.id(), child.id());
        with_generation(self.generation(), |r| {
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
        with_generation(self.generation(), |r| {
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
    #[cfg(feature = "test_utils")]
    generation: u64,
}
impl Transition {
    pub(crate) fn begin() -> Self {
        Self::begin_generation(generation())
    }
    fn begin_generation(generation: u64) -> Self {
        let generation = with_generation(generation, |r| {
            Recorder::count(&mut r.in_flight, &mut r.poison);
            Recorder::count(&mut r.epoch, &mut r.poison);
            Recorder::count(&mut r.operations, &mut r.poison);
            r.generation
        })
        .unwrap_or(0);
        #[cfg(feature = "test_utils")]
        {
            Self { generation }
        }
        #[cfg(not(feature = "test_utils"))]
        {
            let _ = generation;
            Self {}
        }
    }
}
impl Drop for Transition {
    fn drop(&mut self) {
        #[cfg(feature = "test_utils")]
        if self.generation != 0 {
            with_generation(self.generation, |r| {
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
    // Read through field drop order, after the original receiver.
    #[allow(dead_code)]
    retired: QueueRetirement,
    diagnostic: Life,
}
struct QueueRetirement(u64, u64);
impl Drop for QueueRetirement {
    fn drop(&mut self) {
        loop {
            let id = with_generation(self.1, |r| {
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
                drop(Life::adopt_generation(id, self.1));
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
            retired: QueueRetirement(diagnostic.id(), diagnostic.generation()),
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
        let _transition = Transition::begin_generation(self.diagnostic.generation());
        let result = self.inner.as_mut().poll_next(cx);
        if matches!(result, std::task::Poll::Ready(Some(_))) {
            let ordinal = self.diagnostic.increment(1);
            queue_receive(self.diagnostic.id(), ordinal, self.diagnostic.generation());
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
    // The decoder may hold intermediate allocations across Pending. Keep the
    // cut explicitly open until its batch is observed and the original send ends.
    #[cfg(feature = "test_utils")]
    pub(crate) polling: Option<Transition>,
}

impl ProducerInput {
    pub(crate) fn begin_poll(&mut self) {
        #[cfg(feature = "test_utils")]
        {
            self.polling =
                Some(Transition::begin_generation(self.diagnostic.generation()));
        }
    }
    pub(crate) fn finish_poll(&mut self) {
        #[cfg(feature = "test_utils")]
        {
            self.polling = None;
        }
    }
    pub(crate) fn clear_handoff(&self) {
        clear_handoff_generation(self.diagnostic.generation());
    }
    pub(crate) fn take_handoff(&self) -> Life {
        take_handoff_generation(self.diagnostic.generation())
    }
    pub(crate) fn batch(&self) -> Life {
        Life::new_generation(
            Kind::Batch,
            self.diagnostic.id(),
            0,
            self.diagnostic.generation(),
        )
    }
}

/// Original IntoIter owner followed only by numeric sentinels. Yielded batches
/// leave this ownership occurrence; no batch is cloned or retained by evidence.
pub(crate) struct Chunks {
    inner: std::vec::IntoIter<arrow::array::RecordBatch>,
    lives: std::vec::IntoIter<Life>,
}
impl Iterator for Chunks {
    type Item = arrow::array::RecordBatch;
    fn next(&mut self) -> Option<Self::Item> {
        let next = self.inner.next();
        if next.is_some() {
            if let Some(life) = self.lives.next() {
                let generation = life.generation();
                publish_handoff_generation(life.into_id(), generation);
            }
        }
        next
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

fn clear_handoff_generation(generation: u64) {
    let old =
        update_thread_generation(generation, |t| std::mem::replace(&mut t.handoff, 0))
            .unwrap_or(0);
    if old != 0 {
        with_generation(generation, |r| r.gaps |= GAP_UNKNOWN_SPONSOR);
        drop(Life::adopt_generation(old, generation));
    }
}
pub(crate) fn take_handoff() -> Life {
    take_handoff_generation(generation())
}
fn take_handoff_generation(generation: u64) -> Life {
    Life::adopt_generation(
        update_thread_generation(generation, |t| std::mem::replace(&mut t.handoff, 0))
            .unwrap_or(0),
        generation,
    )
}
fn publish_handoff_generation(id: u64, generation: u64) {
    let old =
        update_thread_generation(generation, |t| std::mem::replace(&mut t.handoff, id))
            .unwrap_or(0);
    if old != 0 {
        with_generation(generation, |r| r.gaps |= GAP_UNKNOWN_SPONSOR);
        drop(Life::adopt_generation(old, generation));
    }
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
    fn adopt_generation(id: u64, generation: u64) -> Self {
        #[cfg(feature = "test_utils")]
        {
            Self {
                id,
                generation,
                dropping: false,
            }
        }
        #[cfg(not(feature = "test_utils"))]
        {
            let _ = (id, generation);
            Self {}
        }
    }
    pub(crate) fn observe_claim_aliases(&self) {
        let id = self.id();
        if id == 0 {
            return;
        }
        with_generation(self.generation(), |r| {
            for i in 0..r.backings.len() {
                let Some(b) = r.backings[i].filter(|b| {
                    b.owner == id && b.flags & arrays::ARRAY == 0 && b.capacity != 0
                }) else {
                    continue;
                };
                let claim = r
                    .backings
                    .iter()
                    .flatten()
                    .find(|claim| {
                        claim.data_type == 35
                            && claim.base == b.base
                            && claim.capacity == b.capacity
                            && r.owners.iter().flatten().any(|o| {
                                o.id == claim.owner && o.owner_kind == Kind::Claim as u64
                            })
                    })
                    .map(|b| b.owner);
                let Some(claim) = claim else {
                    if let Some(o) = r.owners.iter_mut().flatten().find(|o| o.id == id) {
                        o.coverage |= GAP_QUEUE;
                    }
                    continue;
                };
                if r.edges
                    .iter()
                    .flatten()
                    .any(|e| e.from == id && e.to == claim && e.relation == 3)
                {
                    continue;
                }
                if let Some(edge) = r.edges.iter_mut().find(|e| e.is_none()) {
                    *edge = Some(EdgeRecord {
                        call: r.call,
                        from: id,
                        to: claim,
                        relation: 3,
                        bytes: b.capacity,
                    });
                } else {
                    r.poison |= POISON_CAPACITY;
                }
            }
        });
    }
    pub(crate) fn queued(self, queue: u64, ordinal: u64) {
        let id = self.id();
        let queued = with_generation(self.generation(), |r| {
            if !r
                .owners
                .iter()
                .flatten()
                .any(|o| o.id == queue && o.owner_kind == Kind::Queue as u64)
            {
                r.gaps |= GAP_UNKNOWN_SPONSOR;
                return false;
            }
            if let Some(o) = r.owners.iter_mut().flatten().find(|o| o.id == id) {
                o.parent = queue;
                o.c = ordinal;
                o.d = 1;
                true
            } else {
                false
            }
        })
        .unwrap_or(false);
        if queued {
            let _ = self.into_id();
        }
    }
    pub(crate) fn increment(&self, field: u8) -> u64 {
        let id = self.id();
        with_generation(self.generation(), |r| {
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
pub(crate) fn queue_receive(queue: u64, ordinal: u64, generation: u64) {
    let id = with_generation(generation, |r| {
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
        publish_handoff_generation(id, generation);
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
                self.gap(GAP_ROWS_STORAGE);
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
                self.gap(GAP_ROWS_STORAGE);
            } else {
                // Complete borrowed codec and datatype descriptors replace the
                // constructor's provisional opaque-layout marker.
                self.update(|o| o.flags &= !1);
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
    crate::spill::admission::verify_ownership_controls().expect("IPC budget controls");
}

impl Life {
    pub(crate) fn begin_drop(&mut self) {
        #[cfg(feature = "test_utils")]
        if self.id != 0 && !self.dropping {
            self.dropping = true;
            with_generation(self.generation(), |r| {
                Recorder::count(&mut r.in_flight, &mut r.poison);
                Recorder::count(&mut r.epoch, &mut r.poison);
                Recorder::count(&mut r.operations, &mut r.poison);
            });
        }
    }
}
pub(crate) fn sidecar_permitted(count: usize) -> bool {
    with(|r| {
        let free = r.owners.iter().filter(|o| o.is_none()).count();
        let okay = r.poison == 0 && count <= free;
        if !okay {
            r.poison |= POISON_CAPACITY;
        }
        okay
    })
    .unwrap_or(false)
}
impl Drop for QueueStream {
    fn drop(&mut self) {
        self.diagnostic.begin_drop();
        let id = self.diagnostic.id();
        with_generation(self.diagnostic.generation(), |r| {
            for o in r.owners.iter_mut().flatten() {
                if o.parent == id && o.owner_kind == Kind::Producer as u64 {
                    o.c = 1;
                }
            }
        });
    }
}
impl Drop for ProducerInput {
    fn drop(&mut self) {
        self.diagnostic.begin_drop();
    }
}

pub(crate) fn snapshot_failure(
    stage: u64,
    piece: u64,
    consumer: usize,
    request: usize,
    error: &datafusion_common::DataFusionError,
) {
    let mut anchor = error_anchor(error);
    with(|r| {
        if r.snapshots != 0 {
            r.poison |= POISON_PROTOCOL;
            return;
        }
        anchor.attempt = r.id();
        anchor.stage = stage;
        anchor.failed_piece = piece;
        anchor.consumer = consumer as u64;
        anchor.request = request as u64;
        if anchor.error_ptr == 0 || anchor.error_truncated {
            r.gaps |= GAP_UNSUPPORTED;
        }
        r.anchor = anchor;
        r.cut(1);
    });
}

/// Original input owner followed by its numeric sentinel; no extra Arrow owner.
pub(crate) struct InputBatch {
    batch: Option<arrow::array::RecordBatch>,
    diagnostic: Life,
    completion: Option<Transition>,
}
impl InputBatch {
    pub(crate) fn batch(&self) -> &arrow::array::RecordBatch {
        self.batch.as_ref().expect("live input batch")
    }
    pub(crate) fn pending(batch: arrow::array::RecordBatch, parent: u64) -> Self {
        let handed = take_handoff();
        let diagnostic = if handed.id() != 0 {
            handed
        } else {
            Life::new(Kind::Output, parent, 0)
        };
        diagnostic.batch(&batch);
        diagnostic.update(|o| {
            o.parent = parent;
            o.owner_kind = Kind::Output as u64;
            o.piece = 0;
            o.flags |= 16;
            o.a = batch.num_rows() as u64;
            o.b = batch.num_columns() as u64;
        });
        Self {
            batch: Some(batch),
            diagnostic,
            completion: None,
        }
    }
    pub(crate) fn admitted(&self, piece: u64, held: usize) {
        self.diagnostic.update(|o| {
            o.piece = piece;
            o.held = held as u64;
            o.flags &= !16;
        });
    }
    pub(crate) fn into_parts(mut self) -> (arrow::array::RecordBatch, Life) {
        let batch = self.batch.take().expect("one live batch");
        let life = std::mem::take(&mut self.diagnostic);
        (batch, life)
    }

    pub(crate) fn complete(&mut self, transition: Transition) {
        self.completion = Some(transition);
    }
    pub(crate) fn new(batch: arrow::array::RecordBatch, parent: u64, piece: u64) -> Self {
        let diagnostic = Life::new(Kind::Batch, parent, piece);
        diagnostic.batch(&batch);
        diagnostic.update(|o| {
            o.a = batch.num_rows() as u64;
            o.b = batch.num_columns() as u64;
        });
        Self {
            batch: Some(batch),
            diagnostic,
            completion: None,
        }
    }
}
impl Drop for InputBatch {
    fn drop(&mut self) {
        self.diagnostic.begin_drop();
    }
}
/// Declared before the original sorted Vec so failure destroys its Arrow fields
/// before these numeric records retire. Success moves the same IntoIter onward.
#[derive(Default)]
pub(crate) struct BatchLives {
    #[cfg(feature = "test_utils")]
    inner: Vec<Life>,
}
impl BatchLives {
    pub(crate) fn capture(&mut self, batches: &[arrow::array::RecordBatch], parent: u64) {
        #[cfg(feature = "test_utils")]
        {
            if !sidecar_permitted(batches.len())
                || self.inner.try_reserve_exact(batches.len()).is_err()
            {
                gap(GAP_UNSUPPORTED);
                return;
            }
            for batch in batches {
                let life = Life::new(Kind::Batch, parent, 0);
                life.batch(batch);
                life.update(|o| {
                    o.a = batch.num_rows() as u64;
                    o.b = batch.num_columns() as u64;
                    o.flags = 16;
                });
                self.inner.push(life);
            }
        }
        #[cfg(not(feature = "test_utils"))]
        {
            let _ = (batches, parent);
        }
    }
    pub(crate) fn admitted(&self, piece: u64) {
        #[cfg(feature = "test_utils")]
        for life in &self.inner {
            life.update(|o| {
                o.piece = piece;
                o.flags &= !16;
            });
        }
        #[cfg(not(feature = "test_utils"))]
        {
            let _ = piece;
        }
    }
}
impl Chunks {
    pub(crate) fn from_observed(
        batches: Vec<arrow::array::RecordBatch>,
        lives: BatchLives,
    ) -> Self {
        #[cfg(feature = "test_utils")]
        {
            Self {
                inner: batches.into_iter(),
                lives: lives.inner.into_iter(),
            }
        }
        #[cfg(not(feature = "test_utils"))]
        {
            let _ = lives;
            Self {
                inner: batches.into_iter(),
                lives: Vec::new().into_iter(),
            }
        }
    }
}
