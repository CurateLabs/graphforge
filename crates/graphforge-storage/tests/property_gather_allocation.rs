//! Allocation-bound coverage for the production Arrow row gather helper.
#![allow(unsafe_code)]

#[path = "../src/graph_construction_encoding/bulk/property_gather.rs"]
mod property_gather;

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, ListArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

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

fn large_boolean_list_batch(children: usize) -> RecordBatch {
    let values = Arc::new(BooleanArray::from(
        (0..children)
            .map(|index| index % 3 != 1)
            .collect::<Vec<_>>(),
    )) as ArrayRef;
    let list = ListArray::new(
        Arc::new(Field::new("item", DataType::Boolean, false)),
        OffsetBuffer::new(vec![0_i32, i32::try_from(children).unwrap()].into()),
        values,
        None,
    );
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "values",
            DataType::List(Arc::new(Field::new("item", DataType::Boolean, false))),
            false,
        )])),
        vec![Arc::new(list)],
    )
    .unwrap()
}

#[test]
fn large_boolean_list_gather_and_ipc_write_avoid_child_index_allocation() {
    const CHILDREN: usize = 8 << 20;
    let source = large_boolean_list_batch(CHILDREN);
    assert!(source.get_array_memory_size() < (8 << 20));
    let sources = [&source];
    let indices = [(0, 0)];

    let ((gathered, encoded), largest) = largest_allocation(|| {
        let gathered = property_gather::gather_record_batch(&sources, &indices).unwrap();
        let mut encoded = Vec::new();
        let mut writer = StreamWriter::try_new(&mut encoded, &gathered.schema()).unwrap();
        writer.write(&gathered).unwrap();
        writer.finish().unwrap();
        drop(writer);
        (gathered, encoded)
    });

    assert_eq!(gathered.num_rows(), 1);
    assert!(!encoded.is_empty());
    assert!(
        largest < CHILDREN * 8,
        "list gather and IPC write allocated {largest} bytes at once for {CHILDREN} children"
    );
}
