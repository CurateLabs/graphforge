//! #1506 sort/partition kernel measurements. Run on a quiet host with the
//! scratch directory on an admitted (ext4/xfs/btrfs) volume:
//!
//! ```text
//! cargo run --release -p graphforge-storage --features test-support \
//!   --example sort_partition_spike -- <scratch-dir> [rounds] [records]
//! ```
//!
//! Prints one JSON evidence document. A counting global allocator supplies the
//! transient heap peaks.
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};

use graphforge_storage::graph_construction::{
    HeapProbe, SortPartitionBenchConfig, run_sort_partition_bench,
};

struct Counting;

static LIVE: AtomicU64 = AtomicU64::new(0);
static PEAK: AtomicU64 = AtomicU64::new(0);

// SAFETY: delegates every call to `System` unchanged; only counts sizes.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded with the caller's layout.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            let live =
                LIVE.fetch_add(layout.size() as u64, Ordering::Relaxed) + layout.size() as u64;
            PEAK.fetch_max(live, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: forwarded with the caller's pointer and layout.
        unsafe { System.dealloc(pointer, layout) };
        LIVE.fetch_sub(layout.size() as u64, Ordering::Relaxed);
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: forwarded with the caller's pointer, layout and size.
        let moved = unsafe { System.realloc(pointer, layout, new_size) };
        if !moved.is_null() {
            if new_size >= layout.size() {
                let grown = (new_size - layout.size()) as u64;
                let live = LIVE.fetch_add(grown, Ordering::Relaxed) + grown;
                PEAK.fetch_max(live, Ordering::Relaxed);
            } else {
                LIVE.fetch_sub((layout.size() - new_size) as u64, Ordering::Relaxed);
            }
        }
        moved
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

struct Probe;

impl HeapProbe for Probe {
    fn reset_peak(&self) -> u64 {
        let live = LIVE.load(Ordering::Relaxed);
        PEAK.store(live, Ordering::Relaxed);
        live
    }

    fn peak(&self) -> u64 {
        PEAK.load(Ordering::Relaxed)
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let scratch = args
        .next()
        .expect("usage: sort_partition_spike <scratch-dir> [rounds] [records]");
    let rounds = args
        .next()
        .map_or(5, |value| value.parse().expect("rounds"));
    let records = args
        .next()
        .map_or(1 << 20, |value| value.parse().expect("records"));
    let config = SortPartitionBenchConfig {
        rounds,
        records,
        // 4 Mi hub endpoints are 132 MiB against a 32 MiB budget.
        hub_records: 4 << 20,
        budget_bytes: 32 << 20,
        batch_records: 8_192,
        partition_records: 4 << 20,
        partitions: 256,
        scratch: scratch.into(),
    };
    let evidence = run_sort_partition_bench(&config, &Probe).expect("sort/partition spike");
    println!("{}", serde_json::to_string_pretty(&evidence).expect("json"));
}
