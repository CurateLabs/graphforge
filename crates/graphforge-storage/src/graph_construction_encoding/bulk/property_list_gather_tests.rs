mod property_list_gather_tests {
    use super::*;

    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;
    use std::sync::Arc;

    use arrow::array::{
        Array, ArrayRef, BooleanArray, FixedSizeBinaryArray, Int64Array, ListArray, StringArray,
        StructArray,
    };
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::compute::interleave_record_batch;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::StreamWriter;

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

    fn list_boolean_batch(ids: &[u64], children_per_row: usize) -> RecordBatch {
        let total_children = ids.len() * children_per_row;
        let children = (0..total_children)
            .map(|index| (index % 3) != 1)
            .collect::<Vec<_>>();
        let values = Arc::new(BooleanArray::from(children)) as ArrayRef;
        let offsets = (0..=ids.len())
            .map(|row| i32::try_from(row * children_per_row).unwrap())
            .collect::<Vec<_>>();
        let list = ListArray::new(
            Arc::new(Field::new("item", DataType::Boolean, false)),
            OffsetBuffer::new(offsets.into()),
            values,
            None,
        );
        let id_bytes = ids
            .iter()
            .map(|id| {
                let mut bytes = [0_u8; 16];
                bytes[8..].copy_from_slice(&id.to_be_bytes());
                bytes
            })
            .collect::<Vec<_>>();
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
                Field::new("label", DataType::Utf8, false),
                Field::new(
                    "values",
                    DataType::List(Arc::new(Field::new("item", DataType::Boolean, false))),
                    false,
                ),
            ])),
            vec![
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(id_bytes.iter().map(|id| id.as_slice()))
                        .unwrap(),
                ),
                Arc::new(StringArray::from(vec!["Person"; ids.len()])),
                Arc::new(list),
            ],
        )
        .unwrap()
    }

    fn nested_list_batch(ids: [u64; 4], variant: bool) -> RecordBatch {
        let values = if variant {
            BooleanArray::from(vec![
                Some(false),
                Some(true),
                None,
                Some(false),
                Some(true),
                None,
            ])
        } else {
            BooleanArray::from(vec![
                Some(true),
                None,
                Some(false),
                Some(true),
                None,
                Some(false),
            ])
        };
        let inner = ListArray::new(
            Arc::new(Field::new("item", DataType::Boolean, true)),
            OffsetBuffer::new(vec![0_i32, 2, 2, 5, 6].into()),
            Arc::new(values),
            Some(NullBuffer::from(vec![true, false, true, true])),
        );
        let outer = ListArray::new(
            Arc::new(Field::new(
                "item",
                DataType::List(Arc::new(Field::new("item", DataType::Boolean, true))),
                true,
            )),
            OffsetBuffer::new(vec![0_i32, 2, 3, 3, 4].into()),
            Arc::new(inner),
            Some(NullBuffer::from(vec![true, true, false, true])),
        );
        let nested_type = DataType::List(Arc::new(Field::new(
            "item",
            DataType::List(Arc::new(Field::new("item", DataType::Boolean, true))),
            true,
        )));
        let nested_record = StructArray::new(
            vec![
                Arc::new(Field::new("nested", nested_type.clone(), true)),
                Arc::new(Field::new("marker", DataType::Int64, true)),
            ]
            .into(),
            vec![
                Arc::new(outer),
                Arc::new(Int64Array::from(vec![10_i64, 11, 12, 13])),
            ],
            Some(NullBuffer::from(vec![true, true, false, true])),
        );
        let id_bytes = ids
            .iter()
            .map(|id| {
                let mut bytes = [0_u8; 16];
                bytes[8..].copy_from_slice(&id.to_be_bytes());
                bytes
            })
            .collect::<Vec<_>>();
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
                Field::new("label", DataType::Utf8, false),
                Field::new(
                    "record",
                    DataType::Struct(
                        vec![
                            Field::new("nested", nested_type, true),
                            Field::new("marker", DataType::Int64, true),
                        ]
                        .into(),
                    ),
                    true,
                ),
            ])),
            vec![
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(id_bytes.iter().map(|id| id.as_slice()))
                        .unwrap(),
                ),
                Arc::new(StringArray::from(vec!["Person"; ids.len()])),
                Arc::new(nested_record),
            ],
        )
        .unwrap()
    }

    fn ipc_bytes(batch: &RecordBatch) -> Vec<u8> {
        let mut bytes = Vec::new();
        let mut writer = StreamWriter::try_new(&mut bytes, &batch.schema()).unwrap();
        writer.write(batch).unwrap();
        writer.finish().unwrap();
        drop(writer);
        bytes
    }

    #[test]
    fn list_gather_matches_arrow_for_nested_null_sliced_and_duplicate_rows() {
        let first = nested_list_batch([0, 1, 2, 3], false).slice(1, 3);
        let second = nested_list_batch([4, 5, 6, 7], true).slice(1, 3);
        let sources = [&first, &second];
        let indices = [(1, 1), (0, 0), (0, 0), (1, 0), (0, 2)];
        let gathered = super::super::gather_record_batch(&sources, &indices).unwrap();
        let arrow_gathered = interleave_record_batch(&sources, &indices).unwrap();
        assert_eq!(ipc_bytes(&gathered), ipc_bytes(&arrow_gathered));

        let empty = super::super::gather_record_batch(&sources, &[]).unwrap();
        assert_eq!(empty.num_rows(), 0);
        assert_eq!(empty.schema(), first.schema());
    }

    #[test]
    fn large_boolean_lists_gather_without_a_per_child_index_allocation() {
        const CHILDREN: usize = 8 << 20;
        let root = tempfile::tempdir().unwrap();
        let directory = super::super::super::StableDirectory::open(root.path()).unwrap();
        let scratch = Scratch::create(&directory).unwrap();
        let rows = rows_with(&scratch, 8 << 20, 8, 128 << 20);
        let batch = list_boolean_batch(&[7], CHILDREN);
        assert!(batch.get_array_memory_size() < rows.budgets.max_batch_bytes);
        let sources = [&batch];
        let indices = [(0, 0)];

        let (gathered, gather_largest) =
            largest_allocation(|| super::super::gather_record_batch(&sources, &indices).unwrap());
        assert_eq!(gathered.num_rows(), 1);
        assert!(
            gather_largest < CHILDREN * 8,
            "list gather allocated {gather_largest} bytes at once for {CHILDREN} children"
        );

        let cancel = AtomicBool::new(false);
        let (run, write_largest) =
            largest_allocation(|| rows.write_run(&[batch], &cancel).unwrap());
        assert_eq!(run.rows, 1);
        assert!(
            write_largest < CHILDREN * 8,
            "run formation allocated {write_largest} bytes at once for {CHILDREN} children"
        );
    }

    #[test]
    fn large_boolean_lists_survive_reduction_and_final_ranges() {
        const CHILDREN: usize = 8 << 20;
        const RUNS: usize = 33;
        let root = tempfile::tempdir().unwrap();
        let directory = super::super::super::StableDirectory::open(root.path()).unwrap();
        let scratch = Scratch::create(&directory).unwrap();
        let rows = rows_with(&scratch, 8 << 20, 8, 128 << 20);
        let cancel = AtomicBool::new(false);
        let mut runs = Vec::with_capacity(RUNS);
        for id in 0..RUNS as u64 {
            runs.push(
                rows.write_run(&[list_boolean_batch(&[id], CHILDREN)], &cancel)
                    .unwrap(),
            );
        }
        assert!(runs.iter().map(Run::bytes).sum::<u64>() > (32 << 20));
        rows.groups
            .lock()
            .unwrap()
            .runs
            .insert("boolean-list".into(), runs);

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .build()
            .unwrap();
        let (groups, largest) =
            pool.install(|| largest_allocation(|| rows.finish(&cancel).unwrap()));
        assert!(largest < CHILDREN * 8);
        assert_eq!(groups.len(), 1);
        assert!(groups[0].segments.len() > 1);
        let batches = rows.group_reader(&groups[0]);
        let mut reader = batches;
        let mut seen = Vec::new();
        while let Some(batch) = reader.next().unwrap() {
            let ids = crate::graph_construction::batch_uuid_column(&batch, "node_uuid").unwrap();
            let lists = batch
                .column(2)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                seen.push(u64::from_be_bytes(ids.value(row)[8..].try_into().unwrap()));
                assert_eq!(lists.value_length(row), CHILDREN as i32);
                let values = lists
                    .value(row)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap();
                assert!(values.value(0));
                assert!(!values.value(1));
                assert!(!values.value(CHILDREN - 1));
            }
        }
        assert_eq!(seen, (0..RUNS as u64).collect::<Vec<_>>());
    }
}
