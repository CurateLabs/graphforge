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

/// A set of scratch files that concurrent writers append blocks to.
pub(super) struct Partitions {
    paths: Vec<PathBuf>,
    /// Records appended to each file, behind the lock that orders its appends.
    state: Vec<Mutex<u64>>,
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
    consumed: Vec<AtomicBool>,
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
            state: (0..count).map(|_| Mutex::new(0)).collect(),
            paths,
            width,
            written: AtomicU64::new(0),
            read: AtomicU64::new(0),
            owned: true,
            consumed: (0..count).map(|_| AtomicBool::new(false)).collect(),
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
            state: counts.into_iter().map(Mutex::new).collect(),
            width,
            written: AtomicU64::new(0),
            read: AtomicU64::new(0),
            owned: true,
            consumed,
        }
    }

    pub(super) fn len(&self) -> usize {
        self.paths.len()
    }

    pub(super) fn path(&self, index: usize) -> &Path {
        &self.paths[index]
    }

    /// Append one block. `block` holds [`HEADER`] reserved bytes, then records.
    pub(super) fn append(
        &self,
        scratch: &Scratch,
        index: usize,
        block: &mut [u8],
    ) -> Result<(), GfError> {
        let payload = block.len() - HEADER;
        debug_assert!(payload.is_multiple_of(self.width));
        let length = u32::try_from(payload).map_err(storage)?;
        let crc = crc32c(&block[HEADER..]);
        block[..4].copy_from_slice(&length.to_le_bytes());
        block[4..HEADER].copy_from_slice(&crc.to_le_bytes());
        let mut records = self.state[index]
            .lock()
            .map_err(|_| storage("scratch partition lock poisoned"))?;
        // Reserve the whole block before the write can grow the file, and
        // keep the reservation when the write fails: a partial write may
        // exist and the attempt tears the tree down either way.
        scratch.occupy(block.len() as u64)?;
        OpenOptions::new()
            .append(true)
            .open(&self.paths[index])
            .map_err(storage)?
            .write_all(block)
            .map_err(storage)?;
        *records += (payload / self.width) as u64;
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
            .map(|records| {
                records
                    .lock()
                    .map(|records| *records)
                    .map_err(|_| storage("scratch partition lock poisoned"))
            })
            .collect()
    }

    /// Delete file `index` once its final read has completed and verified:
    /// the bytes it occupied leave the live count. Owned files only, and each
    /// file exactly once, so a file read on one path is never deleted twice or
    /// read back after its data moved on. The cumulative read and write
    /// counters are unaffected.
    pub(super) fn reclaim(&self, scratch: &Scratch, index: usize) -> Result<(), GfError> {
        if !self.owned {
            return Ok(());
        }
        if self.consumed[index].swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        scratch.reclaim_file(&self.paths[index])
    }

    /// Read file `index` block by block, handing each verified payload to `visit`.
    pub(super) fn read(
        &self,
        scratch: &Scratch,
        index: usize,
        mut visit: impl FnMut(&[u8]) -> Result<(), GfError>,
    ) -> Result<(), GfError> {
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
        outcome
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
}

impl<'a> BlockReader<'a> {
    pub(super) fn open(scratch: &'a Scratch, path: &Path) -> Result<Self, GfError> {
        Ok(Self {
            scratch,
            file: std::io::BufReader::with_capacity(1 << 20, File::open(path).map_err(storage)?),
            bytes: 0,
        })
    }

    /// The next verified payload in `payload`, or `false` at a clean end.
    pub(super) fn next_block(&mut self, payload: &mut Vec<u8>) -> Result<bool, GfError> {
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
        payload.resize(length, 0);
        self.file
            .read_exact(payload)
            .map_err(|_| storage("a scratch block payload is truncated"))?;
        if crc32c(payload) != expected {
            return Err(storage("a scratch block failed its CRC32C check"));
        }
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
    /// `capacity` is the staging size per file, a multiple of the record width.
    pub(super) fn new(scratch: &'a Scratch, partitions: &'a Partitions, capacity: usize) -> Self {
        let capacity = (capacity / partitions.width).max(1) * partitions.width;
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
#[path = "scratch_test_support.rs"]
mod test_support;
