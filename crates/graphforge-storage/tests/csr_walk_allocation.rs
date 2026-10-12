//! Allocation-bound coverage for the sequential persisted-CSR walk: a full pass
//! must never allocate more than one decoded shard at a time.
#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use graphforge_storage::adjacency::{CsrIndex, ShardedCsrIndex, read_csr, write_sharded_csr};
use tempfile::TempDir;

struct AllocationCounter;

thread_local! {
    static TRACK_ALLOCATIONS: Cell<bool> = const { Cell::new(false) };
    static LARGEST_ALLOCATION: Cell<usize> = const { Cell::new(0) };
}

#[global_allocator]
static TEST_ALLOCATOR: AllocationCounter = AllocationCounter;

fn record_allocation(size: usize) {
    if TRACK_ALLOCATIONS.try_with(Cell::get).unwrap_or(false) {
        let _ = LARGEST_ALLOCATION.try_with(|largest| largest.set(largest.get().max(size)));
    }
}

unsafe impl GlobalAlloc for AllocationCounter {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            record_allocation(layout.size());
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            record_allocation(layout.size());
        }
        pointer
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let pointer = unsafe { System.realloc(pointer, layout, size) };
        if !pointer.is_null() {
            record_allocation(size);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
    }
}

struct StopAllocationTracking;

impl Drop for StopAllocationTracking {
    fn drop(&mut self) {
        TRACK_ALLOCATIONS.with(|active| active.set(false));
    }
}

fn largest_allocation<T>(operation: impl FnOnce() -> T) -> (T, usize) {
    LARGEST_ALLOCATION.with(|largest| largest.set(0));
    TRACK_ALLOCATIONS.with(|active| active.set(true));
    let stop = StopAllocationTracking;
    let result = operation();
    let largest = LARGEST_ALLOCATION.with(Cell::get);
    drop(stop);
    (result, largest)
}

const NODES: u64 = 4096;
const DEGREE: u64 = 16;
const SHARD_EDGES: usize = 4096;

/// Every node has `DEGREE` neighbors, so each `SHARD_EDGES`-entry shard holds
/// exactly `SHARD_EDGES / DEGREE` whole rows.
fn uniform_csr() -> CsrIndex {
    let mut csr = CsrIndex {
        offsets: vec![0],
        ..CsrIndex::default()
    };
    for node in 0..NODES {
        for step in 0..DEGREE {
            csr.edge_ids.push(node * DEGREE + step);
            csr.neighbor_ids.push((node + step + 1) % NODES);
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

#[test]
fn walking_shards_never_allocates_more_than_one_decoded_shard() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("WALK.out.csr");
    let csr = uniform_csr();
    write_sharded_csr(&path, &csr, SHARD_EDGES).unwrap();
    let index = ShardedCsrIndex::open(&path).unwrap();
    let one_shard = decoded_shard_bytes(SHARD_EDGES as u64 / DEGREE, SHARD_EDGES as u64);
    assert_eq!(
        index.shard_count() as u64,
        NODES * DEGREE / SHARD_EDGES as u64
    );

    let mut checksum = 0_u64;
    let (result, largest) = largest_allocation(|| {
        index.walk_rows(|node, row| {
            assert_eq!(row.len() as u64, DEGREE);
            checksum = checksum.wrapping_add(node ^ row.neighbor_ids[0]);
            Ok(())
        })
    });
    result.unwrap();
    assert_ne!(checksum, 0);
    assert!(
        (largest as u64) <= one_shard,
        "walk allocated {largest} bytes at once; one decoded shard is {one_shard}"
    );

    // The check can fail: materializing the whole CSR is far over the bound.
    let (full, largest_full) = largest_allocation(|| read_csr(&path));
    assert_eq!(full.unwrap(), csr);
    assert!(
        (largest_full as u64) > one_shard,
        "full read allocated only {largest_full} bytes at once"
    );
}
