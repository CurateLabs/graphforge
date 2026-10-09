//! Scratch files of the over-budget bulk build (#1900).
//!
//! Scratch is private to one build attempt. It is never synced and never
//! hashed: every block carries a CRC32C that a reader checks, a crash discards
//! the whole directory, and the next attempt starts by deleting it (restart,
//! not resume; ADR 0038 as amended by ADR 0058).
//!
//! A scratch file is a sequence of blocks: `payload_len: u32 LE`,
//! `crc32c(payload): u32 LE`, then the payload, a whole number of fixed-width
//! records. Blocks from concurrent writers interleave in arrival order, so a
//! reader sorts what it loads and nothing depends on that order.
//!
//! Most sets keep one physical file per partition. The endpoint references
//! (#1929) are the exception: a skewed leaf could hold its whole input while
//! the endpoint pass produces its output, so their set splits every partition
//! into physical segments of a bounded logical size. A destructive read
//! ([`Partitions::read_reclaiming`]) verifies a segment, feeds its payload to
//! the consumer, closes the reader and deletes that segment before the next
//! opens, so the input a resolving worker retains is one segment, not the
//! leaf's whole reference list.

use std::fs::{File, OpenOptions};
use std::io::{BufRead, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use super::{GfError, StableDirectory, storage};

/// Directory name of the scratch tree below the construction root.
pub(crate) const SCRATCH_DIRECTORY: &str = "bulk-scratch";

const HEADER: usize = 8;

// ---------------------------------------------------------------- CRC32C

/// Slicing-by-8 tables of the Castagnoli polynomial (reflected).
#[allow(clippy::cast_possible_truncation)] // `index` is below 256
const fn crc_tables() -> [[u32; 256]; 8] {
    const POLY: u32 = 0x82F6_3B78;
    let mut tables = [[0_u32; 256]; 8];
    let mut index = 0;
    while index < 256 {
        let mut value = index as u32;
        let mut bit = 0;
        while bit < 8 {
            value = if value & 1 == 1 {
                (value >> 1) ^ POLY
            } else {
                value >> 1
            };
            bit += 1;
        }
        tables[0][index] = value;
        index += 1;
    }
    let mut slice = 1;
    while slice < 8 {
        let mut index = 0;
        while index < 256 {
            let previous = tables[slice - 1][index];
            tables[slice][index] = (previous >> 8) ^ tables[0][(previous & 0xff) as usize];
            index += 1;
        }
        slice += 1;
    }
    tables
}

static CRC_TABLES: [[u32; 256]; 8] = crc_tables();

/// CRC32C (Castagnoli) of `data`.
pub(super) fn crc32c(data: &[u8]) -> u32 {
    let tables = &CRC_TABLES;
    let mut crc = !0_u32;
    let mut chunks = data.chunks_exact(8);
    for chunk in &mut chunks {
        let low = u32::from_le_bytes(chunk[..4].try_into().expect("4 bytes")) ^ crc;
        let high = u32::from_le_bytes(chunk[4..].try_into().expect("4 bytes"));
        crc = tables[7][(low & 0xff) as usize]
            ^ tables[6][((low >> 8) & 0xff) as usize]
            ^ tables[5][((low >> 16) & 0xff) as usize]
            ^ tables[4][(low >> 24) as usize]
            ^ tables[3][(high & 0xff) as usize]
            ^ tables[2][((high >> 8) & 0xff) as usize]
            ^ tables[1][((high >> 16) & 0xff) as usize]
            ^ tables[0][(high >> 24) as usize];
    }
    for byte in chunks.remainder() {
        crc = (crc >> 8) ^ tables[0][((crc ^ u32::from(*byte)) & 0xff) as usize];
    }
    !crc
}

// -------------------------------------------------------------- directory

/// Delete whatever an earlier attempt left below the construction root `root`.
/// Recovery and every new attempt call it: scratch is never resumed.
pub(crate) fn discard_scratch(root: &Path) -> Result<(), GfError> {
    match std::fs::remove_dir_all(root.join(SCRATCH_DIRECTORY)) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(storage(error)),
    }
}

/// The scratch tree of one build attempt. Dropping it deletes the tree, so an
/// error, a cancellation and a success all leave nothing behind.
pub(super) struct Scratch {
    path: PathBuf,
    written: AtomicU64,
    read: AtomicU64,
    /// Bytes reserved for scratch files that still exist, and the largest
    /// value the reservation ever reached. A writer reserves an append's
    /// complete logical length before the write can grow its file and keeps
    /// the reservation when the write fails, because a partial write may
    /// exist and the attempt tears the tree down anyway. Owned files are
    /// reclaimed as soon as their final read completes, so the peak is a
    /// conservative bound on what a build held at once: logical reserved
    /// file bytes, bytes still buffered in the writer included. It is not
    /// the filesystem's allocated blocks and not an exact physical overlap.
    occupied: AtomicU64,
    peak_occupied: AtomicU64,
}

impl Scratch {
    /// A fresh, empty scratch tree below `root`.
    pub(super) fn create(root: &StableDirectory) -> Result<Self, GfError> {
        discard_scratch(root.path())?;
        let path = root.path().join(SCRATCH_DIRECTORY);
        std::fs::create_dir(&path).map_err(storage)?;
        Ok(Self {
            path,
            written: AtomicU64::new(0),
            read: AtomicU64::new(0),
            occupied: AtomicU64::new(0),
            peak_occupied: AtomicU64::new(0),
        })
    }

    pub(super) fn file(&self, name: &str) -> PathBuf {
        self.path.join(name)
    }

    /// Bytes written to scratch so far, headers included.
    pub(super) fn written_bytes(&self) -> u64 {
        self.written.load(Ordering::Relaxed)
    }

    /// Bytes read back from scratch so far, headers included.
    pub(super) fn read_bytes(&self) -> u64 {
        self.read.load(Ordering::Relaxed)
    }

    /// Reserve `bytes` of logical occupancy before a write can grow a
    /// scratch file, and raise the peak. Reserving first keeps the peak at
    /// the real overlap even when another worker reclaims a file between
    /// this write and its charge. Every append calls this once per block,
    /// so the tracker stays a pair of counters and never scans the
    /// directory. Overflow is an accounting error, not a saturation.
    pub(super) fn occupy(&self, bytes: u64) -> Result<(), GfError> {
        let previous = self
            .occupied
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(bytes)
            })
            .map_err(|_| storage("scratch occupancy overflowed"))?;
        // `fetch_update` yields the value before the reservation, so the
        // peak is raised with the reserved total instead. The `checked_add`
        // already proved this sum cannot overflow.
        self.peak_occupied
            .fetch_max(previous + bytes, Ordering::Relaxed);
        Ok(())
    }

    /// Record that `bytes` left the tree with a reclaimed file. A release
    /// must match bytes an earlier reservation counted; underflow is an
    /// accounting error, not a saturation.
    fn release(&self, bytes: u64) -> Result<(), GfError> {
        self.occupied
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_sub(bytes)
            })
            .map_err(|_| storage("scratch occupancy accounting underflow"))?;
        Ok(())
    }

    /// The largest number of bytes reserved for scratch files at once.
    pub(super) fn peak_occupied_bytes(&self) -> u64 {
        self.peak_occupied.load(Ordering::Relaxed)
    }

    /// Delete a scratch file whose final read has completed and verified,
    /// subtracting the bytes it occupied from the live occupancy. The length
    /// is measured here rather than accumulated by its writer, so a file any
    /// writer produced (a refinement output, a partition a refinement kept)
    /// is counted once, exactly.
    pub(super) fn reclaim_file(&self, path: &Path) -> Result<(), GfError> {
        let bytes = std::fs::metadata(path).map_err(storage)?.len();
        std::fs::remove_file(path).map_err(storage)?;
        self.release(bytes)
    }

    /// Delete the tree now and report a failure, instead of leaving it to `Drop`.
    pub(super) fn remove(self) -> Result<(), GfError> {
        let outcome = std::fs::remove_dir_all(&self.path);
        std::mem::forget(self);
        match outcome {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(storage(error)),
        }
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

// ------------------------------------------------------------ partitions

/// Whether a segmented partition still accepts writes and reads. A
/// destructive read is terminal: it either consumes the whole partition or
/// fails it, and neither a failed nor a consumed partition is reused.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SegmentLifecycle {
    /// Writers may append; readers may run.
    Writing,
    /// A destructive read holds the partition: appends and reads are refused
    /// until the pass ends one way or the other.
    Reading,
    /// A destructive read verified and reclaimed every segment.
    Consumed,
    /// A destructive read failed part way. The partition is a partial input:
    /// no later writer or reader may reuse what it still holds, and the
    /// first unreclaimed segment ordinal tracks what cleanup owes.
    Failed,
}

/// The physical shape of a segmented set: every segment of every partition
/// holds at most `cap` logical bytes, headers included, and a full staging
/// block of `payload` record bytes plus its header never exceeds it.
#[derive(Clone, Copy)]
struct SegmentLayout {
    cap: usize,
    payload: usize,
}

/// One partition's append progress, behind the lock that orders its appends.
/// A fixed number of scalars per partition, never a Vec over segments: the
/// state stays O(logical partitions) however many physical segments a
/// skewed partition rotates through. The exact byte size of this layout is
/// charged against the builder's memory budget when the whole-builder
/// integration lands (#1929); this bounded slice does not claim that
/// admission.
struct Progress {
    /// Records appended to the partition, across every segment.
    records: u64,
    /// The segment new blocks rotate into (segmented partitions only).
    segment: u64,
    /// Logical bytes of the current segment, headers included (segmented
    /// partitions only). The final segment's expected length at read time.
    segment_bytes: u64,
    /// Destructive-read lifecycle (segmented partitions only; unsegmented
    /// partitions never leave [`SegmentLifecycle::Writing`]).
    lifecycle: SegmentLifecycle,
    /// The lowest segment ordinal that may still exist on disk: every
    /// segment below it was verified, read and reclaimed (segmented
    /// partitions only). Suffix cleanup starts here and never releases a
    /// segment twice.
    first_live: u64,
}

impl Progress {
    fn writing(records: u64) -> Self {
        Self {
            records,
            segment: 0,
            segment_bytes: 0,
            lifecycle: SegmentLifecycle::Writing,
            first_live: 0,
        }
    }
}

/// A set of scratch files that concurrent writers append blocks to.
pub(super) struct Partitions {
    paths: Vec<PathBuf>,
    /// Records appended to each file, behind the lock that orders its appends.
    state: Vec<Mutex<Progress>>,
    width: usize,
    /// Bytes this set's files received and gave back, block headers included.
    written: AtomicU64,
    read: AtomicU64,
    /// Whether this set owns its files and may reclaim them. Both constructors
    /// below own what this build wrote. A set that ever wraps files imported
    /// from outside this scratch tree must be constructed as a borrower, whose
    /// files [`Self::reclaim`] never deletes.
    owned: bool,
    /// Which files a final read has already reclaimed: a file is deleted once.
    /// Unsegmented sets only; a segmented set tracks its reclaimed prefix per
    /// partition in [`Progress::first_live`] instead.
    consumed: Vec<AtomicBool>,
    /// The physical segmentation of this set, when its partitions split into
    /// capped segments ([`Self::create_segmented`]).
    segments: Option<SegmentLayout>,
}

impl Partitions {
    /// `count` empty files named `{prefix}-{index}` of `width`-byte records.
    /// The set owns the files it creates here.
    pub(super) fn create(
        scratch: &Scratch,
        prefix: &str,
        count: usize,
        width: usize,
    ) -> Result<Self, GfError> {
        let paths = (0..count)
            .map(|index| scratch.file(&format!("{prefix}-{index:06}.blocks")))
            .collect::<Vec<_>>();
        for path in &paths {
            File::create(path).map_err(storage)?;
        }
        Ok(Self {
            state: (0..count)
                .map(|_| Mutex::new(Progress::writing(0)))
                .collect(),
            paths,
            width,
            written: AtomicU64::new(0),
            read: AtomicU64::new(0),
            owned: true,
            consumed: (0..count).map(|_| AtomicBool::new(false)).collect(),
            segments: None,
        })
    }

    /// `count` empty files named `{prefix}-{index}` of `width`-byte records,
    /// split into physical segments of at most `cap` logical bytes, headers
    /// included. The set owns the files it creates here.
    ///
    /// A segment cap must hold a block header and at least one record;
    /// anything smaller — or a width whose header-plus-record floor cannot
    /// even be computed — is a typed refusal before any file exists. A
    /// [`Scatter`] staging this set clamps its buffers to the largest
    /// record-aligned payload a segment can hold, so a full staging block
    /// always fits its segment and rotation only happens between blocks.
    /// Segment 0 keeps the set's base path; later segments derive their
    /// names from it ([`Self::segment_path`]). Every initial segment is
    /// created exclusively: a set whose base path already exists is
    /// refused, never truncates another owner's file.
    pub(super) fn create_segmented(
        scratch: &Scratch,
        prefix: &str,
        count: usize,
        width: usize,
        cap: usize,
    ) -> Result<Self, GfError> {
        if width == 0 {
            return Err(storage(
                "a segmented scratch partition needs a positive record width",
            ));
        }
        let floor = HEADER.checked_add(width).ok_or_else(|| {
            storage("a segmented scratch partition cap must hold a block header and one record")
        })?;
        if cap < floor {
            return Err(storage(
                "a segmented scratch partition cap must hold a block header and one record",
            ));
        }
        let layout = SegmentLayout {
            cap,
            payload: (cap - HEADER) / width * width,
        };
        let paths = (0..count)
            .map(|index| scratch.file(&format!("{prefix}-{index:06}.blocks")))
            .collect::<Vec<_>>();
        for path in &paths {
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)
                .map_err(storage)?;
        }
        Ok(Self {
            state: (0..count)
                .map(|_| Mutex::new(Progress::writing(0)))
                .collect(),
            paths,
            width,
            written: AtomicU64::new(0),
            read: AtomicU64::new(0),
            owned: true,
            consumed: (0..count).map(|_| AtomicBool::new(false)).collect(),
            segments: Some(layout),
        })
    }

    /// Bytes appended to this set's files so far.
    pub(super) fn written_bytes(&self) -> u64 {
        self.written.load(Ordering::Relaxed)
    }

    /// Bytes [`Self::read`] has given back so far.
    pub(super) fn read_bytes(&self) -> u64 {
        self.read.load(Ordering::Relaxed)
    }

    /// Reuse existing verified-block files in the caller's logical order.
    /// Construction does not read or rewrite their records.
    ///
    /// The inventory owns its files: they are outputs this build wrote (the
    /// leaves a refinement kept), so a final read may reclaim them. This is
    /// not a licence to reclaim files imported from outside the scratch tree;
    /// such an inventory must be built as a borrower instead.
    pub(super) fn from_inventory(inventory: Vec<(PathBuf, u64)>, width: usize) -> Self {
        let (paths, counts): (Vec<_>, Vec<_>) = inventory.into_iter().unzip();
        let consumed = paths.iter().map(|_| AtomicBool::new(false)).collect();
        Self {
            paths,
            state: counts
                .into_iter()
                .map(|records| Mutex::new(Progress::writing(records)))
                .collect(),
            width,
            written: AtomicU64::new(0),
            read: AtomicU64::new(0),
            owned: true,
            consumed,
            segments: None,
        }
    }

    pub(super) fn len(&self) -> usize {
        self.paths.len()
    }

    /// The base path of partition `index`: segment 0 of a segmented set.
    /// Later segments of a segmented set live beside it under names
    /// [`Self::segment_path`] derives, so this path alone never enumerates
    /// the physical files of a segmented partition.
    pub(super) fn path(&self, index: usize) -> &Path {
        &self.paths[index]
    }

    /// The physical file of segment `segment` of partition `index`. Segment
    /// 0 is the partition's base path; later segments derive deterministic
    /// names from it, so a caller can address any segment without a
    /// directory listing.
    fn segment_path(&self, index: usize, segment: u64) -> PathBuf {
        let base = &self.paths[index];
        if segment == 0 {
            return base.clone();
        }
        let stem = base
            .file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or_default();
        base.with_file_name(format!("{stem}-seg{segment:06}.blocks"))
    }

    /// Append one block. `block` holds [`HEADER`] reserved bytes, then records.
    ///
    /// Every total this append will commit — the aggregate record count, the
    /// next segment ordinal and the current segment's final length — is
    /// computed with checked arithmetic before any owned file is created or
    /// one byte moves, so a refused append leaves the partition exactly as it
    /// was. Once an owned file has been created or written, any failure is
    /// terminal for a segmented partition: it is failed for reuse while the
    /// reservation for the attempted block stays charged, and an exclusively
    /// created next segment stays owned for suffix cleanup even if its first
    /// write fails.
    pub(super) fn append(
        &self,
        scratch: &Scratch,
        index: usize,
        block: &mut [u8],
    ) -> Result<(), GfError> {
        let payload = block
            .len()
            .checked_sub(HEADER)
            .ok_or_else(|| storage("a scratch block is shorter than its header"))?;
        let records = payload / self.width;
        let length = u32::try_from(payload).map_err(storage)?;
        let crc = crc32c(&block[HEADER..]);
        block[..4].copy_from_slice(&length.to_le_bytes());
        block[4..HEADER].copy_from_slice(&crc.to_le_bytes());
        let mut progress = self.state[index]
            .lock()
            .map_err(|_| storage("scratch partition lock poisoned"))?;
        // A segmented partition refuses blocks its lifecycle or its segment
        // cap cannot hold before any byte moves or any file is created.
        let mut rotate_to = None;
        let mut grown = progress
            .segment_bytes
            .checked_add(block.len() as u64)
            .ok_or_else(|| storage("scratch segment length overflowed"))?;
        if let Some(layout) = self.segments {
            if progress.lifecycle != SegmentLifecycle::Writing {
                return Err(storage(
                    "a segmented scratch partition no longer accepts appends",
                ));
            }
            if payload == 0 {
                return Err(storage(
                    "a segmented scratch partition refuses empty blocks",
                ));
            }
            if !payload.is_multiple_of(self.width) {
                return Err(storage("a scratch block has a partial record"));
            }
            if block.len() > layout.cap {
                return Err(storage(
                    "a scratch block is larger than the segment cap of its partition",
                ));
            }
            // Rotate before this block would take the current segment past
            // the cap. The next segment is created exclusively, so a
            // segmented set never appends into another owner's file.
            if grown > layout.cap as u64 {
                rotate_to = Some(
                    progress
                        .segment
                        .checked_add(1)
                        .ok_or_else(|| storage("scratch segment ordinal overflowed"))?,
                );
            }
        }
        let next_records = progress
            .records
            .checked_add(records as u64)
            .ok_or_else(|| storage("scratch partition record count overflowed"))?;
        // Past this point the append mutates owned state.
        if let Some(next) = rotate_to {
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(self.segment_path(index, next))
                .map_err(storage)?;
            progress.segment = next;
            progress.segment_bytes = 0;
            grown = block.len() as u64;
        }
        // Reserve the whole block before the write can grow the file, and
        // keep the reservation when the write fails: a partial write may
        // exist and the attempt tears the tree down either way.
        if let Err(error) = scratch.occupy(block.len() as u64) {
            // Still holding this partition's lock: fail it in place rather
            // than through [`Self::fail_segmented`], which locks again.
            if rotate_to.is_some() {
                progress.lifecycle = SegmentLifecycle::Failed;
            }
            return Err(error);
        }
        let opened = if self.segments.is_some() {
            OpenOptions::new()
                .append(true)
                .open(self.segment_path(index, progress.segment))
        } else {
            OpenOptions::new().append(true).open(&self.paths[index])
        };
        let failure = match opened {
            Ok(mut file) => file.write_all(block).err(),
            Err(error) => Some(error),
        };
        if let Some(error) = failure {
            if self.segments.is_some() {
                progress.lifecycle = SegmentLifecycle::Failed;
            }
            return Err(storage(error));
        }
        // Only a successful write commits totals.
        progress.records = next_records;
        if self.segments.is_some() {
            progress.segment_bytes = grown;
        }
        scratch
            .written
            .fetch_add(block.len() as u64, Ordering::Relaxed);
        self.written
            .fetch_add(block.len() as u64, Ordering::Relaxed);
        Ok(())
    }

    /// Records appended to every file so far.
    pub(super) fn counts(&self) -> Result<Vec<u64>, GfError> {
        self.state
            .iter()
            .map(|progress| {
                progress
                    .lock()
                    .map(|progress| progress.records)
                    .map_err(|_| storage("scratch partition lock poisoned"))
            })
            .collect()
    }

    /// Records appended to partition `index`, allocating nothing per call.
    fn count(&self, index: usize) -> Result<u64, GfError> {
        self.state[index]
            .lock()
            .map(|progress| progress.records)
            .map_err(|_| storage("scratch partition lock poisoned"))
    }

    /// Delete file `index` once its final read has completed and verified:
    /// the bytes it occupied leave the live count. Owned files only, and each
    /// file exactly once, so a file read on one path is never deleted twice or
    /// read back after its data moved on. The cumulative read and write
    /// counters are unaffected.
    ///
    /// A segmented set deletes every segment it still owns, from its first
    /// live ordinal to its last, each exactly once; it never stops at the
    /// base path. The cleanup claims the partition's terminal lifecycle
    /// under its lock and holds that lock through the deletions — no
    /// callback runs here — so a concurrent cleanup serializes behind it
    /// instead of deleting the same file twice, and no writer or reader can
    /// interleave. A partition whose destructive read is still running
    /// refuses, because the reader owns its remaining segments. A consumed
    /// partition is an immediate no-op; a failed partition cleans its
    /// remaining suffix but never returns to Writing.
    pub(super) fn reclaim(&self, scratch: &Scratch, index: usize) -> Result<(), GfError> {
        if !self.owned {
            return Ok(());
        }
        if self.segments.is_none() {
            if self.consumed[index].swap(true, Ordering::AcqRel) {
                return Ok(());
            }
            return scratch.reclaim_file(&self.paths[index]);
        }
        let mut progress = self.state[index]
            .lock()
            .map_err(|_| storage("scratch partition lock poisoned"))?;
        match progress.lifecycle {
            SegmentLifecycle::Consumed => return Ok(()),
            SegmentLifecycle::Reading => {
                return Err(storage(
                    "a segmented scratch partition being destructively read owns its remaining segments",
                ));
            }
            SegmentLifecycle::Writing | SegmentLifecycle::Failed => {}
        }
        for segment in progress.first_live..=progress.segment {
            let path = self.segment_path(index, segment);
            // A file that is already gone charges nothing back: an ordinal
            // removed outside the set keeps its conservative reservation. A
            // failing deletion is not proof the file left, so its ordinal
            // stays first-live and the suffix is never released twice.
            if path.exists() {
                scratch.reclaim_file(&path)?;
            }
            progress.first_live = progress.first_live.max(segment.saturating_add(1));
        }
        if progress.lifecycle == SegmentLifecycle::Writing {
            progress.lifecycle = SegmentLifecycle::Consumed;
        }
        Ok(())
    }

    /// Read file `index` block by block, handing each verified payload to `visit`.
    ///
    /// An unsegmented set keeps its ordinary whole-file read. A segmented set
    /// reads every physical segment of the partition in order and reclaims
    /// nothing, and it admits exclusively: no append, destructive read,
    /// second read or cleanup may interleave with the pass. The aggregate
    /// count and the final segment's written length must verify exactly, and
    /// only a fully verified pass restores Writing — a failed pass is a
    /// terminal [`SegmentLifecycle::Failed`]. On success the aggregate count
    /// and the read counter are preserved, so a non-destructive pass sees
    /// exactly what a destructive one verifies.
    pub(super) fn read(
        &self,
        scratch: &Scratch,
        index: usize,
        mut visit: impl FnMut(&[u8]) -> Result<(), GfError>,
    ) -> Result<(), GfError> {
        let Some(layout) = self.segments else {
            let mut reader = BlockReader::open(scratch, &self.paths[index])?;
            let mut payload = Vec::new();
            let outcome = loop {
                match reader.next_block(&mut payload) {
                    Ok(true) => {
                        if let Err(error) = visit(&payload) {
                            break Err(error);
                        }
                    }
                    Ok(false) => break Ok(()),
                    Err(error) => break Err(error),
                }
            };
            self.read.fetch_add(reader.bytes, Ordering::Relaxed);
            return outcome;
        };
        // Admit exclusively under the partition lock, then release it: the
        // callbacks below run without any lock held.
        let (expected, last, final_bytes) = {
            let mut progress = self.state[index]
                .lock()
                .map_err(|_| storage("scratch partition lock poisoned"))?;
            if progress.lifecycle != SegmentLifecycle::Writing {
                return Err(storage(
                    "a segmented scratch partition no longer accepts reads",
                ));
            }
            progress.lifecycle = SegmentLifecycle::Reading;
            (progress.records, progress.segment, progress.segment_bytes)
        };
        match self.stream_segments(
            scratch,
            index,
            layout,
            last,
            final_bytes,
            expected,
            None,
            false,
            &mut visit,
        ) {
            Ok(()) => {
                // Nothing else can have changed the lifecycle while this
                // pass held Reading: restore Writing only on full success.
                let mut progress = self.state[index]
                    .lock()
                    .map_err(|_| storage("scratch partition lock poisoned"))?;
                if progress.lifecycle == SegmentLifecycle::Reading {
                    progress.lifecycle = SegmentLifecycle::Writing;
                }
                Ok(())
            }
            Err(error) => {
                self.fail_segmented(index);
                Err(error)
            }
        }
    }

    /// Read partition `index` once, destructively: every physical segment is
    /// read in order, each verified payload is handed to `visit`, and the
    /// segment's file is reclaimed — reader closed first — before the next
    /// segment opens. A skewed partition therefore retains at most one
    /// segment of input however many records it holds.
    ///
    /// The read is terminal. The partition enters a reading state before the
    /// first byte, so no append or second read can interleave with the
    /// destructive pass. Cancellation is observed before every frame load,
    /// before every callback and before every unlink. The records observed
    /// must equal the count scattered into the partition exactly, checked at
    /// the final segment's end *before* that segment is reclaimed: a
    /// truncated, empty or missing segment is a typed refusal that leaves the
    /// current file on disk. A clean end of a non-final segment just advances
    /// to the next one — a missing ordinal is refused when it is opened, not
    /// mistaken for an end. Any CRC, geometry, callback, cancellation or I/O
    /// failure is terminal too — the partition is failed, never reports
    /// success, keeps its first unreclaimed segment for [`Self::reclaim`] or
    /// the scratch teardown, and the segments verified before the failure may
    /// already be gone.
    pub(super) fn read_reclaiming(
        &self,
        scratch: &Scratch,
        index: usize,
        cancel: &AtomicBool,
        mut visit: impl FnMut(&[u8]) -> Result<(), GfError>,
    ) -> Result<(), GfError> {
        let Some(layout) = self.segments else {
            // An unsegmented partition keeps its one-file lifetime: the
            // aggregate count must still verify before the file is reclaimed,
            // with the same per-frame cancellation, whole-record, overshoot
            // and overflow guards the segmented stream applies.
            let expected = self.count(index)?;
            let mut observed = 0_u64;
            let mut reader = BlockReader::open(scratch, &self.paths[index])?;
            let mut payload = Vec::new();
            let outcome = loop {
                if cancel.load(Ordering::Acquire) {
                    break Err(storage("construction encoding cancelled"));
                }
                match reader.next_block(&mut payload) {
                    Ok(true) => {
                        if !payload.len().is_multiple_of(self.width) {
                            break Err(storage("a scratch block has a partial record"));
                        }
                        observed = match observed.checked_add((payload.len() / self.width) as u64) {
                            Some(next) => next,
                            None => break Err(storage("scratch record count overflowed")),
                        };
                        if observed > expected {
                            break Err(storage(
                                "a scratch partition holds more records than were scattered",
                            ));
                        }
                        if cancel.load(Ordering::Acquire) {
                            break Err(storage("construction encoding cancelled"));
                        }
                        if let Err(error) = visit(&payload) {
                            break Err(error);
                        }
                    }
                    Ok(false) => {
                        if observed != expected {
                            break Err(storage("a scratch partition lost records"));
                        }
                        break Ok(());
                    }
                    Err(error) => break Err(error),
                }
            };
            self.read.fetch_add(reader.bytes, Ordering::Relaxed);
            drop(reader);
            outcome?;
            return self.reclaim(scratch, index);
        };
        // Snapshot the partition's final shape under its lock and enter the
        // reading lifecycle before any byte moves.
        let (expected, last, final_bytes) = {
            let mut progress = self.state[index]
                .lock()
                .map_err(|_| storage("scratch partition lock poisoned"))?;
            if progress.lifecycle != SegmentLifecycle::Writing {
                return Err(storage(
                    "a segmented scratch partition no longer accepts reads",
                ));
            }
            progress.lifecycle = SegmentLifecycle::Reading;
            (progress.records, progress.segment, progress.segment_bytes)
        };
        match self.stream_segments(
            scratch,
            index,
            layout,
            last,
            final_bytes,
            expected,
            Some(cancel),
            true,
            &mut visit,
        ) {
            Ok(()) => {
                // Nothing else can have changed the lifecycle while this
                // pass held Reading: the whole partition is consumed.
                let mut progress = self.state[index]
                    .lock()
                    .map_err(|_| storage("scratch partition lock poisoned"))?;
                if progress.lifecycle == SegmentLifecycle::Reading {
                    progress.lifecycle = SegmentLifecycle::Consumed;
                }
                Ok(())
            }
            Err(error) => {
                self.fail_segmented(index);
                Err(error)
            }
        }
    }

    /// Mark a segmented partition terminally failed: a destructive read that
    /// did not finish, or an append that already mutated owned state. The
    /// first unreclaimed ordinal and the current segment bracket exactly what
    /// cleanup and the scratch teardown still owe, and a failed partition
    /// never returns to Writing.
    fn fail_segmented(&self, index: usize) {
        if let Ok(mut progress) = self.state[index].lock() {
            if matches!(
                progress.lifecycle,
                SegmentLifecycle::Writing | SegmentLifecycle::Reading
            ) {
                progress.lifecycle = SegmentLifecycle::Failed;
            }
        }
    }

    /// Stream one segmented partition segment by segment: every verified
    /// payload goes to `visit`, and with `reclaim_segments` each segment's
    /// file is reclaimed — reader closed first — before the next opens. A
    /// given `cancel` token is observed before every frame load, before every
    /// callback and before every unlink, including an empty final segment's.
    /// A clean end of a non-final segment advances to the next one; only the
    /// final segment's end checks the aggregate count and the exact final
    /// length, before that segment is unlinked. No lock is held across the
    /// callbacks.
    #[allow(clippy::too_many_lines)]
    fn stream_segments(
        &self,
        scratch: &Scratch,
        index: usize,
        layout: SegmentLayout,
        last: u64,
        final_bytes: u64,
        expected: u64,
        cancel: Option<&AtomicBool>,
        reclaim_segments: bool,
        visit: &mut impl FnMut(&[u8]) -> Result<(), GfError>,
    ) -> Result<(), GfError> {
        let cancelled =
            |cancel: Option<&AtomicBool>| cancel.is_some_and(|token| token.load(Ordering::Acquire));
        let mut observed = 0_u64;
        for segment in 0..=last {
            if cancelled(cancel) {
                return Err(storage("construction encoding cancelled"));
            }
            let mut reader = match BlockReader::open_segment(
                scratch,
                &self.segment_path(index, segment),
                self.width,
                layout.cap,
            ) {
                Ok(reader) => reader,
                Err(error) => return Err(error),
            };
            let mut payload = Vec::new();
            let outcome = loop {
                if cancelled(cancel) {
                    break Err(storage("construction encoding cancelled"));
                }
                match reader.next_bounded_block(&mut payload) {
                    Ok(true) => {
                        observed = match observed.checked_add((payload.len() / self.width) as u64) {
                            Some(next) => next,
                            None => break Err(storage("scratch record count overflowed")),
                        };
                        if observed > expected {
                            break Err(storage(
                                "a scratch partition holds more records than were scattered",
                            ));
                        }
                        if cancelled(cancel) {
                            break Err(storage("construction encoding cancelled"));
                        }
                        if let Err(error) = visit(&payload) {
                            break Err(error);
                        }
                    }
                    Ok(false) => {
                        // A missing ordinal is refused at open, never a
                        // clean end. The final segment must still hold the
                        // exact scattered count and length.
                        if segment == last {
                            if observed != expected {
                                break Err(storage("a scratch partition lost records"));
                            }
                            if reader.bytes != final_bytes {
                                break Err(storage(
                                    "a scratch partition's last segment is shorter than it was written",
                                ));
                            }
                        }
                        break Ok(());
                    }
                    Err(error) => break Err(error),
                }
            };
            self.read.fetch_add(reader.bytes, Ordering::Relaxed);
            // Close the reader before unlinking: an open handle would keep
            // the reclaimed inode alive on the filesystem.
            drop(reader);
            match outcome {
                Ok(()) => {
                    if cancelled(cancel) {
                        return Err(storage("construction encoding cancelled"));
                    }
                    if reclaim_segments {
                        self.reclaim_segment(scratch, index, segment)?;
                    }
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    /// Reclaim segment `segment` of partition `index` once its verified read
    /// has finished, advancing the first live ordinal so no later pass
    /// releases it again. The occupancy leaves with the file.
    fn reclaim_segment(
        &self,
        scratch: &Scratch,
        index: usize,
        segment: u64,
    ) -> Result<(), GfError> {
        scratch.reclaim_file(&self.segment_path(index, segment))?;
        let mut progress = self.state[index]
            .lock()
            .map_err(|_| storage("scratch partition lock poisoned"))?;
        progress.first_live = progress.first_live.max(segment.saturating_add(1));
        Ok(())
    }
}

/// Routes a UUID to the one range leaf that can hold it.
///
/// Leaves are disjoint UUID ranges in ascending order, so each is described
/// by the smallest UUID it holds: a UUID belongs to the last leaf whose
/// smallest UUID does not exceed it. A UUID below every leaf (an endpoint that
/// names no node) goes to the first, where a lookup misses.
pub(super) struct LeafRouter {
    lows: Vec<[u8; 16]>,
    leaves: Vec<usize>,
}

impl LeafRouter {
    /// `lows[leaf]` is the smallest UUID in `leaf`, or `None` for an empty leaf.
    pub(super) fn new(lows: &[Option<[u8; 16]>]) -> Self {
        let (leaves, lows) = lows
            .iter()
            .enumerate()
            .filter_map(|(leaf, low)| low.map(|low| (leaf, low)))
            .unzip();
        Self { lows, leaves }
    }

    /// The leaf `uuid` belongs to, or `None` when every leaf is empty.
    pub(super) fn route(&self, uuid: &[u8; 16]) -> Option<usize> {
        let after = self.lows.partition_point(|low| low <= uuid);
        self.leaves.get(after.saturating_sub(1)).copied()
    }
}

/// Pulls the blocks of one scratch file in order, verifying each CRC32C.
pub(super) struct BlockReader<'a> {
    scratch: &'a Scratch,
    file: std::io::BufReader<File>,
    /// Bytes this reader has verified so far, headers included.
    bytes: u64,
    /// Bytes of this file still unread, taken from the metadata of the
    /// handle this reader actually opened. Every frame's claimed payload is
    /// checked against what remains before the claim can reach an
    /// allocation, so no reader — ordinary or segmented — can be grown into
    /// an oversized buffer by a corrupt header.
    remaining: u64,
    /// The record geometry of a segmented reader: its frames must carry
    /// whole non-empty records. Ordinary readers keep the historical
    /// empty-frame semantics.
    width: Option<usize>,
}

impl<'a> BlockReader<'a> {
    pub(super) fn open(scratch: &'a Scratch, path: &Path) -> Result<Self, GfError> {
        Self::open_whole(scratch, File::open(path).map_err(storage)?, None)
    }

    /// Open one physical segment of a segmented partition. The opened
    /// handle's own length is admitted before the first frame — there is no
    /// separate stat to race the open — a segment longer than its cap is
    /// refused here, and that length bounds every frame claim the reader
    /// will honor.
    fn open_segment(
        scratch: &'a Scratch,
        path: &Path,
        width: usize,
        cap: usize,
    ) -> Result<Self, GfError> {
        let file = File::open(path).map_err(storage)?;
        let length = file.metadata().map_err(storage)?.len();
        if length > cap as u64 {
            return Err(storage(
                "a scratch segment is longer than its partition's segment cap",
            ));
        }
        Self::open_whole(scratch, file, Some(width))
    }

    fn open_whole(scratch: &'a Scratch, file: File, width: Option<usize>) -> Result<Self, GfError> {
        let remaining = file.metadata().map_err(storage)?.len();
        Ok(Self {
            scratch,
            file: std::io::BufReader::with_capacity(1 << 20, file),
            bytes: 0,
            remaining,
            width,
        })
    }

    /// The next verified payload in `payload`, or `false` at a clean end.
    pub(super) fn next_block(&mut self, payload: &mut Vec<u8>) -> Result<bool, GfError> {
        self.next_frame(payload)
    }

    /// The next verified payload of a segment whose length was admitted at
    /// open: the frame geometry is checked against the remaining bytes and
    /// the record width before the claimed length can reach `resize`.
    fn next_bounded_block(&mut self, payload: &mut Vec<u8>) -> Result<bool, GfError> {
        self.next_frame(payload)
    }

    fn next_frame(&mut self, payload: &mut Vec<u8>) -> Result<bool, GfError> {
        // A clean end lands exactly on a block boundary.
        if self.file.fill_buf().map_err(storage)?.is_empty() {
            return Ok(false);
        }
        let mut header = [0_u8; HEADER];
        self.file
            .read_exact(&mut header)
            .map_err(|_| storage("a scratch block header is truncated"))?;
        let length = u32::from_le_bytes(header[..4].try_into().expect("4 bytes")) as usize;
        let expected = u32::from_le_bytes(header[4..].try_into().expect("4 bytes"));
        let usable = self
            .remaining
            .checked_sub(HEADER as u64)
            .ok_or_else(|| storage("a scratch block header is truncated"))?;
        if length as u64 > usable {
            return Err(storage(
                "a scratch block claims more payload than its admitted length holds",
            ));
        }
        if let Some(width) = self.width {
            if length == 0 {
                return Err(storage("a scratch block frame is empty"));
            }
            if !length.is_multiple_of(width) {
                return Err(storage("a scratch block has a partial record"));
            }
        }
        payload.resize(length, 0);
        self.file
            .read_exact(payload)
            .map_err(|_| storage("a scratch block payload is truncated"))?;
        if crc32c(payload) != expected {
            return Err(storage("a scratch block failed its CRC32C check"));
        }
        self.remaining = usable - length as u64;
        self.scratch
            .read
            .fetch_add((HEADER + length) as u64, Ordering::Relaxed);
        self.bytes += (HEADER + length) as u64;
        Ok(true)
    }
}

/// Read one scratch file block by block, handing each verified payload to `visit`.
pub(super) fn read_blocks(
    scratch: &Scratch,
    path: &Path,
    mut visit: impl FnMut(&[u8]) -> Result<(), GfError>,
) -> Result<(), GfError> {
    let mut reader = BlockReader::open(scratch, path)?;
    let mut payload = Vec::new();
    while reader.next_block(&mut payload)? {
        visit(&payload)?;
    }
    Ok(())
}

/// Per-thread staging of records into the files of a [`Partitions`].
///
/// Each file has one buffer; a full buffer is appended as one block, so
/// concurrent scatterers cost one lock acquisition per block, not per record.
pub(super) struct Scatter<'a> {
    scratch: &'a Scratch,
    partitions: &'a Partitions,
    buffers: Vec<Vec<u8>>,
    capacity: usize,
}

impl<'a> Scatter<'a> {
    /// `capacity` is the staging size per file, a multiple of the record
    /// width. A segmented set clamps it to the largest record-aligned
    /// payload one of its segments can hold, so a full staging block plus
    /// its header never exceeds the segment cap and rotation happens only
    /// between blocks.
    pub(super) fn new(scratch: &'a Scratch, partitions: &'a Partitions, capacity: usize) -> Self {
        let mut capacity = (capacity / partitions.width).max(1) * partitions.width;
        if let Some(layout) = &partitions.segments {
            capacity = capacity.min(layout.payload);
        }
        Self {
            scratch,
            partitions,
            buffers: (0..partitions.len()).map(|_| Vec::new()).collect(),
            capacity,
        }
    }

    /// Stage one record (`width` bytes) for file `index`.
    pub(super) fn push(&mut self, index: usize, record: &[u8]) -> Result<(), GfError> {
        let buffer = &mut self.buffers[index];
        if buffer.is_empty() {
            buffer.reserve_exact(HEADER + self.capacity);
            buffer.extend_from_slice(&[0; HEADER]);
        }
        buffer.extend_from_slice(record);
        if buffer.len() - HEADER >= self.capacity {
            self.partitions.append(self.scratch, index, buffer)?;
            buffer.clear();
        }
        Ok(())
    }

    /// Append every partial buffer.
    pub(super) fn finish(mut self) -> Result<(), GfError> {
        for (index, buffer) in self.buffers.iter_mut().enumerate() {
            if buffer.len() > HEADER {
                self.partitions.append(self.scratch, index, buffer)?;
                buffer.clear();
            }
        }
        Ok(())
    }
}

/// Appends records to a single scratch file in order, as blocks.
pub(super) struct Appender<'a> {
    scratch: &'a Scratch,
    file: File,
    block: Vec<u8>,
    capacity: usize,
}

impl<'a> Appender<'a> {
    pub(super) fn create(
        scratch: &'a Scratch,
        path: &Path,
        capacity: usize,
    ) -> Result<Self, GfError> {
        Ok(Self {
            scratch,
            file: File::create(path).map_err(storage)?,
            block: vec![0; HEADER],
            capacity: capacity.max(1),
        })
    }

    pub(super) fn push(&mut self, record: &[u8]) -> Result<(), GfError> {
        self.block.extend_from_slice(record);
        if self.block.len() - HEADER >= self.capacity {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), GfError> {
        if self.block.len() == HEADER {
            return Ok(());
        }
        let length = u32::try_from(self.block.len() - HEADER).map_err(storage)?;
        let crc = crc32c(&self.block[HEADER..]);
        self.block[..4].copy_from_slice(&length.to_le_bytes());
        self.block[4..HEADER].copy_from_slice(&crc.to_le_bytes());
        // Reserve the whole block before the write can grow the file, and
        // keep the reservation when the write fails: a partial write may
        // exist and the attempt tears the tree down either way.
        self.scratch.occupy(self.block.len() as u64)?;
        self.file.write_all(&self.block).map_err(storage)?;
        self.scratch
            .written
            .fetch_add(self.block.len() as u64, Ordering::Relaxed);
        self.block.truncate(HEADER);
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<(), GfError> {
        self.flush()
    }
}

#[cfg(test)]
#[path = "scratch_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "scratch_segments_tests.rs"]
mod segments_tests;

#[cfg(test)]
#[path = "scratch_test_support.rs"]
mod test_support;
