//! Peak live heap during a sequential persisted-CSR walk.
//!
//! A counting global allocator tracks the bytes this process holds live and the
//! peak it reaches, so retaining many small allocations is caught as well as one
//! large one. The walk's memory has a stated bound:
//!
//! ```text
//! peak_live <= encoded_limit(shard)        the shard file as read
//!            + 2 * decoded(shard)          Arrow's decoded batch and the CSR copy
//!            + 32 * max_row_degree         spanning-row buffers: two Vec<u64>, each
//!                                          at most twice its length after growth
//!            + SLACK                       test-harness and allocator bookkeeping
//! ```
//!
//! The spanning-row term applies only to a row that continues across shards;
//! every other row is visited in place, so a graph without such a row pays
//! nothing for it. The bound depends on the shard size and the widest spanning
//! row, never on the number of shards.
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

const NODES: u64 = 4096;
const DEGREE: u64 = 16;
const SHARD_EDGES: usize = 4096;
/// Test-harness threads and allocator bookkeeping allocate concurrently.
const SLACK: u64 = 64 * 1024;
/// Format constant: `encoded_limit` is the decoded size plus this much metadata.
const METADATA_MAX_BYTES: u64 = 16 * 1024;

/// Every node has `DEGREE` neighbors; `hub`, if given, is one node of that
/// degree instead. Each `SHARD_EDGES`-entry shard holds whole rows except where
/// the hub spans shards.
fn csr(hub: Option<(u64, u64)>) -> CsrIndex {
    let mut csr = CsrIndex {
        offsets: vec![0],
        ..CsrIndex::default()
    };
    let mut edge = 0;
    for node in 0..NODES {
        let degree = match hub {
            Some((hub_node, hub_degree)) if hub_node == node => hub_degree,
            _ => DEGREE,
        };
        for step in 0..degree {
            csr.edge_ids.push(edge);
            csr.neighbor_ids.push((node + step + 1) % NODES);
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

/// Walk `source` and check its peak live heap against the stated bound.
/// `max_spanning_degree` is the degree of the widest row that crosses shards.
fn assert_walk_is_bounded(source: &CsrIndex, max_spanning_degree: u64) {
    let _guard = serial();
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("WALK.out.csr");
    write_sharded_csr(&path, source, SHARD_EDGES).unwrap();
    let index = ShardedCsrIndex::open(&path).unwrap();

    // A shard decodes to at most SHARD_EDGES entries over at most NODES rows
    // (a shard of hub entries has one row).
    let decoded = decoded_shard_bytes(NODES, SHARD_EDGES as u64);
    let bound = (decoded + METADATA_MAX_BYTES) + 2 * decoded + 32 * max_spanning_degree + SLACK;
    let whole = decoded_shard_bytes(NODES, source.edge_ids.len() as u64);
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
    assert_eq!(visited, NODES);
    assert_eq!(entries, source.edge_ids.len() as u64);
    assert!(
        peak <= bound,
        "walk held {peak} live bytes; the bound is {bound} \
         (decoded shard {decoded}, widest spanning row {max_spanning_degree})"
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
}

#[test]
fn walk_peak_live_heap_is_bounded_by_one_shard_when_no_row_spans_shards() {
    assert_walk_is_bounded(&csr(None), 0);
}

#[test]
fn walk_peak_live_heap_is_bounded_by_one_shard_plus_the_widest_spanning_row() {
    // 16,384 entries over 4,096-entry shards: the hub crosses at least four.
    const HUB_DEGREE: u64 = 16_384;
    assert_walk_is_bounded(&csr(Some((NODES / 2, HUB_DEGREE))), HUB_DEGREE);
}
