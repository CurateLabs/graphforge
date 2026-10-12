//! Peak live heap during a sequential persisted-CSR walk.
//!
//! A counting global allocator tracks the bytes this process holds live and the
//! peak it reaches, so retaining many small allocations is caught as well as one
//! large one. The walk's memory has a stated bound, per shard `S` with decoded
//! size `D(S)` (the format's buffer lengths for its rows and entries):
//!
//! ```text
//! peak_live <= 2 * (D(S) + METADATA)   two encoded copies of the shard file:
//!                                      the `Vec` `codec::read` returns, and the
//!                                      buffer Arrow's `FileReader::read_block`
//!                                      allocates for the encoded batch while
//!                                      that `Vec` is still live
//!            + D(S)                    Arrow's decoded column buffers, which
//!                                      outlive the first pass over a batch
//!            + D(S)                    the `CsrIndex` `decode_csr` copies them
//!                                      into, sized exactly
//!            + 32 * max_row_degree     spanning-row buffers: two `Vec<u64>`, each
//!                                      at most twice its length after growth
//!            + SLACK                   test-harness and allocator bookkeeping
//! ```
//!
//! An encoded copy is at most `D(S) + METADATA`, the format's own admission
//! limit (`encoded_limit`). A well-compressed shard is far below it, and a
//! shard Arrow stores raw is not decoded into new buffers, so the terms are only
//! all large at once for data that compresses a little: the weakly compressible
//! fixture below, whose measured peak exceeds the earlier bound of one encoded
//! copy (`previous`) and stays under the full one (`current`).
//! The spanning-row term applies only to a row that continues across shards;
//! every other row is visited in place. The bound depends on the shard size and
//! the widest spanning row, never on the number of shards.
#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};

use graphforge_storage::adjacency::{CsrIndex, ShardedCsrIndex, read_csr, write_sharded_csr};
use tempfile::TempDir;

struct Counting;
static CURRENT: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

fn grow(bytes: usize) {
    let now = CURRENT.fetch_add(bytes, Ordering::Relaxed) + bytes;
    PEAK.fetch_max(now, Ordering::Relaxed);
}

// SAFETY: every method defers to `System`, which upholds the `GlobalAlloc`
// contract; the counters only observe sizes.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            grow(layout.size());
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            grow(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        CURRENT.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new: usize) -> *mut u8 {
        let moved = unsafe { System.realloc(pointer, layout, new) };
        if !moved.is_null() {
            if new >= layout.size() {
                grow(new - layout.size());
            } else {
                CURRENT.fetch_sub(layout.size() - new, Ordering::Relaxed);
            }
        }
        moved
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// The counters are process-wide, so the tests that read them do not overlap.
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Bytes this process holds live above its level when `operation` started, at
/// the highest point `operation` reached.
fn peak_live_bytes<T>(operation: impl FnOnce() -> T) -> (T, u64) {
    let baseline = CURRENT.load(Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);
    let result = operation();
    let peak = PEAK.load(Ordering::Relaxed);
    (result, peak.saturating_sub(baseline) as u64)
}

const DEGREE: u64 = 16;
/// Test-harness threads and allocator bookkeeping allocate concurrently.
const SLACK: u64 = 64 * 1024;
/// Format constant: `encoded_limit` is the decoded size plus this much metadata.
const METADATA_MAX_BYTES: u64 = 16 * 1024;

#[derive(Clone, Copy)]
struct Shape {
    nodes: u64,
    shard_edges: usize,
    /// Edge and neighbor ids are pseudo-random 56-bit values: an encoded shard
    /// is still compressed (the top byte is zero) but only to about 7/8.
    random_ids: bool,
}

/// Dense, repetitive ids: the shards compress to a small fraction of their size.
const COMPRESSIBLE: Shape = Shape {
    nodes: 4096,
    shard_edges: 4096,
    random_ids: false,
};
/// Random ids: an encoded shard is close to its decoded size yet still
/// compressed, so Arrow allocates decoded buffers on top of both encoded copies.
const WEAKLY_COMPRESSIBLE: Shape = Shape {
    nodes: 65_536,
    shard_edges: 32_768,
    random_ids: true,
};

/// Every node has `DEGREE` neighbors; `hub`, if given, is one node of that
/// degree instead. Each shard holds whole rows except where the hub spans shards.
fn csr(shape: Shape, hub: Option<(u64, u64)>) -> CsrIndex {
    let mut csr = CsrIndex {
        offsets: vec![0],
        ..CsrIndex::default()
    };
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    let mut random = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let mut edge = 0;
    for node in 0..shape.nodes {
        let degree = match hub {
            Some((hub_node, hub_degree)) if hub_node == node => hub_degree,
            _ => DEGREE,
        };
        for step in 0..degree {
            if shape.random_ids {
                csr.edge_ids.push(random() >> 8);
                csr.neighbor_ids.push(random() >> 8);
            } else {
                csr.edge_ids.push(edge);
                csr.neighbor_ids.push((node + step + 1) % shape.nodes);
            }
            edge += 1;
        }
        csr.offsets.push(csr.edge_ids.len() as u64);
    }
    csr
}

/// Bytes of one decoded shard: the validity bitmaps, 64-bit offsets and the two
/// 64-bit id columns the format admits for `nodes` rows and `edges` entries.
const fn decoded_shard_bytes(nodes: u64, edges: u64) -> u64 {
    nodes.div_ceil(8) + 8 * (nodes + 1) + 3 * edges.div_ceil(8) + 16 * edges
}

struct Bounds {
    /// Counts one encoded copy of the shard.
    previous: u64,
    /// Counts the second encoded copy `read_block` makes.
    current: u64,
    decoded: u64,
}

fn bounds(shape: Shape, max_spanning_degree: u64) -> Bounds {
    // A shard is `shard_edges` entries over at most this many rows (a shard of
    // hub entries has one row).
    let rows = shape.shard_edges as u64 / DEGREE + 2;
    let decoded = decoded_shard_bytes(rows, shape.shard_edges as u64);
    let encoded = decoded + METADATA_MAX_BYTES;
    let rest = 2 * decoded + 32 * max_spanning_degree + SLACK;
    Bounds {
        previous: encoded + rest,
        current: 2 * encoded + rest,
        decoded,
    }
}

/// Walk `source` and check its peak live heap against the stated bound.
/// `max_spanning_degree` is the degree of the widest row that crosses shards.
/// Returns the walk's peak and the bounds it was held to.
fn assert_walk_is_bounded(
    shape: Shape,
    source: &CsrIndex,
    max_spanning_degree: u64,
) -> (u64, Bounds) {
    let _guard = serial();
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("WALK.out.csr");
    write_sharded_csr(&path, source, shape.shard_edges).unwrap();
    let index = ShardedCsrIndex::open(&path).unwrap();

    let bounds = bounds(shape, max_spanning_degree);
    let bound = bounds.current;
    let whole = decoded_shard_bytes(shape.nodes, source.edge_ids.len() as u64);
    assert!(
        bound < whole,
        "bound {bound} must be below the {whole} bytes of every decoded shard, or it \
         could not tell streaming from retaining"
    );

    let mut visited = 0_u64;
    let mut entries = 0_u64;
    let (result, peak) = peak_live_bytes(|| {
        index.walk_rows(|_, row| {
            visited += 1;
            entries += row.len() as u64;
            Ok(())
        })
    });
    result.unwrap();
    assert_eq!(visited, shape.nodes);
    assert_eq!(entries, source.edge_ids.len() as u64);
    assert!(
        peak <= bound,
        "walk held {peak} live bytes; the bound is {bound} \
         (decoded shard {}, widest spanning row {max_spanning_degree})",
        bounds.decoded
    );
    assert!(peak > 0, "the counting allocator saw nothing");

    // Pulling through the cursor directly has the same bound.
    let fresh = ShardedCsrIndex::open(&path).unwrap();
    let mut pulled = 0_u64;
    let (result, pull_peak) = peak_live_bytes(|| -> Result<(), graphforge_core::GfError> {
        let mut cursor = fresh.row_cursor();
        while let Some((_, row)) = cursor.next_row()? {
            pulled += row.len() as u64;
        }
        Ok(())
    });
    result.unwrap();
    assert_eq!(pulled, source.edge_ids.len() as u64);
    assert!(
        pull_peak <= bound,
        "cursor held {pull_peak} live bytes; the bound is {bound}"
    );

    // The check can fail: materializing the whole CSR is far over the bound.
    let (full, peak_full) = peak_live_bytes(|| read_csr(&path));
    assert_eq!(&full.unwrap(), source);
    assert!(
        peak_full > bound,
        "full read held only {peak_full} live bytes; the bound is {bound}"
    );
    (peak.max(pull_peak), bounds)
}

#[test]
fn walk_peak_live_heap_is_bounded_by_one_shard_when_no_row_spans_shards() {
    assert_walk_is_bounded(COMPRESSIBLE, &csr(COMPRESSIBLE, None), 0);
}

#[test]
fn walk_peak_live_heap_is_bounded_by_one_shard_plus_the_widest_spanning_row() {
    // 16,384 entries over 4,096-entry shards: the hub crosses at least four.
    const HUB_DEGREE: u64 = 16_384;
    let hub = Some((COMPRESSIBLE.nodes / 2, HUB_DEGREE));
    assert_walk_is_bounded(COMPRESSIBLE, &csr(COMPRESSIBLE, hub), HUB_DEGREE);
}

#[test]
fn weakly_compressible_shards_exceed_the_one_encoded_copy_bound() {
    let (peak, bounds) =
        assert_walk_is_bounded(WEAKLY_COMPRESSIBLE, &csr(WEAKLY_COMPRESSIBLE, None), 0);
    eprintln!(
        "DBG peak={peak} previous={} current={}",
        bounds.previous, bounds.current
    );
    assert!(
        peak > bounds.previous,
        "peak {peak} fits the one-encoded-copy bound {}; the fixture does not show the second copy",
        bounds.previous
    );
}
