//! Disabled hot-path allocation proof in a harness-free test process.
//!
//! Libtest's runner can allocate concurrently with a test even when there is
//! only one test. Keep this proof process-wide and remove that unrelated runner.
use graphforge_observability::{Attributes, RecordStatus, Signal, TelemetryRuntime};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

struct CountingAllocator;
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn disabled_recording_performs_no_allocations() {
    let runtime = TelemetryRuntime::default();
    let before = ALLOCATIONS.load(Ordering::Relaxed);
    for _ in 0..100_000 {
        assert_eq!(
            runtime.record(Signal::OperationCount, Attributes::default()),
            RecordStatus::Disabled
        );
    }
    assert_eq!(ALLOCATIONS.load(Ordering::Relaxed), before);
}

fn main() {
    // Prove the global counter observes a real allocation before measuring the
    // disabled path. The allocation remains observable in optimized builds.
    let before = ALLOCATIONS.load(Ordering::Relaxed);
    let allocation = std::hint::black_box(Box::new(std::hint::black_box(42_u64)));
    assert!(ALLOCATIONS.load(Ordering::Relaxed) > before);
    drop(allocation);

    disabled_recording_performs_no_allocations();
}
