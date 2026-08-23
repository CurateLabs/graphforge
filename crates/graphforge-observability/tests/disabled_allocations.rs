//! Disabled hot-path allocation proof in an isolated test process.
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

#[test]
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
