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
use std::sync::atomic::{AtomicU64, Ordering};

use super::{GfError, StableDirectory, storage};

/// Directory name of the scratch tree below the construction root.
pub(crate) const SCRATCH_DIRECTORY: &str = "bulk-scratch";

const HEADER: usize = 8;

// ---------------------------------------------------------------- CRC32C

/// Slicing-by-8 tables of the Castagnoli polynomial (reflected).
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
}

impl Partitions {
    /// `count` empty files named `{prefix}-{index}` of `width`-byte records.
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
        })
    }

    pub(super) fn len(&self) -> usize {
        self.paths.len()
    }

    #[cfg(test)]
    pub(super) fn path(&self, index: usize) -> &Path {
        &self.paths[index]
    }

    /// Append one block. `block` holds [`HEADER`] reserved bytes, then records.
    fn append(&self, scratch: &Scratch, index: usize, block: &mut [u8]) -> Result<(), GfError> {
        let payload = block.len() - HEADER;
        debug_assert!(payload.is_multiple_of(self.width));
        let length = u32::try_from(payload).map_err(storage)?;
        let crc = crc32c(&block[HEADER..]);
        block[..4].copy_from_slice(&length.to_le_bytes());
        block[4..HEADER].copy_from_slice(&crc.to_le_bytes());
        let mut records = self.state[index]
            .lock()
            .map_err(|_| storage("scratch partition lock poisoned"))?;
        OpenOptions::new()
            .append(true)
            .open(&self.paths[index])
            .map_err(storage)?
            .write_all(block)
            .map_err(storage)?;
        *records += (payload / self.width) as u64;
        scratch.written.fetch_add(block.len() as u64, Ordering::Relaxed);
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

    /// Read file `index` block by block, handing each verified payload to `visit`.
    pub(super) fn read(
        &self,
        scratch: &Scratch,
        index: usize,
        visit: impl FnMut(&[u8]) -> Result<(), GfError>,
    ) -> Result<(), GfError> {
        read_blocks(scratch, &self.paths[index], visit)
    }
}

/// Pulls the blocks of one scratch file in order, verifying each CRC32C.
pub(super) struct BlockReader<'a> {
    scratch: &'a Scratch,
    file: std::io::BufReader<File>,
}

impl<'a> BlockReader<'a> {
    pub(super) fn open(scratch: &'a Scratch, path: &Path) -> Result<Self, GfError> {
        Ok(Self {
            scratch,
            file: std::io::BufReader::with_capacity(1 << 20, File::open(path).map_err(storage)?),
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
    pub(super) fn create(scratch: &'a Scratch, path: &Path, capacity: usize) -> Result<Self, GfError> {
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
mod tests {
    use super::*;

    #[test]
    fn crc32c_matches_the_published_check_value() {
        // RFC 3720 appendix B.4 and the usual "123456789" check value.
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(crc32c(&[0_u8; 32]), 0x8A91_36AA);
        assert_eq!(crc32c(&[0xff_u8; 32]), 0x62A8_AB43);
        assert_eq!(crc32c(b""), 0);
        let ascending = (0_u8..32).collect::<Vec<_>>();
        assert_eq!(crc32c(&ascending), 0x46DD_794E);
    }

    #[test]
    fn a_flipped_byte_is_caught_when_a_block_is_read_back() {
        let root = tempfile::tempdir().unwrap();
        let directory = StableDirectory::open(root.path()).unwrap();
        let scratch = Scratch::create(&directory).unwrap();
        let partitions = Partitions::create(&scratch, "p", 2, 4).unwrap();
        let mut scatter = Scatter::new(&scratch, &partitions, 16);
        for value in 0_u32..10 {
            scatter.push((value % 2) as usize, &value.to_le_bytes()).unwrap();
        }
        scatter.finish().unwrap();
        assert_eq!(partitions.counts().unwrap(), vec![5, 5]);
        let mut seen = 0;
        partitions
            .read(&scratch, 0, |payload| {
                seen += payload.len() / 4;
                Ok(())
            })
            .unwrap();
        assert_eq!(seen, 5);
        let mut bytes = std::fs::read(partitions.path(0)).unwrap();
        *bytes.last_mut().unwrap() ^= 1;
        std::fs::write(partitions.path(0), bytes).unwrap();
        let error = partitions.read(&scratch, 0, |_| Ok(())).unwrap_err();
        assert!(error.to_string().contains("CRC32C"), "{error}");
        scratch.remove().unwrap();
        assert!(!root.path().join(SCRATCH_DIRECTORY).exists());
    }

    #[test]
    fn dropping_scratch_deletes_the_tree() {
        let root = tempfile::tempdir().unwrap();
        let directory = StableDirectory::open(root.path()).unwrap();
        let scratch = Scratch::create(&directory).unwrap();
        std::fs::write(scratch.file("x"), b"x").unwrap();
        drop(scratch);
        assert!(!root.path().join(SCRATCH_DIRECTORY).exists());
    }
}
