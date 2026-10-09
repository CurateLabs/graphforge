    fn skewed_property_batch(task: usize, large: &str) -> RecordBatch {
        const ROWS: usize = 1024;
        let first_empty = 32 + task * (ROWS - 1);
        let ids = std::iter::once(task as u128)
            .chain((first_empty..first_empty + ROWS - 1).map(|id| id as u128))
            .map(u128::to_be_bytes)
            .collect::<Vec<_>>();
        let values = std::iter::once(large)
            .chain(std::iter::repeat_n("", ROWS - 1))
            .collect::<Vec<_>>();
        RecordBatch::try_new(
            std::sync::Arc::new(Schema::new(vec![
                Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
                Field::new("label", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, false),
            ])),
            vec![
                std::sync::Arc::new(
                    FixedSizeBinaryArray::try_from_iter(ids.iter().map(|id| id.as_slice()))
                        .unwrap(),
                ) as ArrayRef,
                std::sync::Arc::new(StringArray::from(vec!["Person"; ROWS])),
                std::sync::Arc::new(StringArray::from(values)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn skewed_property_merge_bounds_actual_bytes_and_preserves_uuid_order() {
        let root = tempfile::tempdir().unwrap();
        let directory = super::super::StableDirectory::open(root.path()).unwrap();
        let scratch = Scratch::create(&directory).unwrap();
        let mut budgets = GraphConstructionBudgets::default();
        budgets.max_batch_rows = 1024;
        budgets.max_batch_bytes = 256 << 10;
        let rows = new_rows(
            &scratch,
            ConstructionChunkKind::Node,
            budgets,
            0,
            PropertySizing {
                run_bytes: 8 << 20,
                retained_bytes: 24 << 20,
                fan_in: 32,
                frame_bytes: 192 << 10,
            },
        );
        let large = "x".repeat(100 << 10);
        let cancel = AtomicBool::new(false);
        let source_batches = (0..32)
            .map(|task| {
                let batch = skewed_property_batch(task, &large);
                assert!(batch.get_array_memory_size() <= budgets.max_batch_bytes);
                batch
            })
            .collect::<Vec<_>>();
        let single_run = rows.write_run(&source_batches, &cancel).unwrap();
        assert!(single_run.frames.len() >= 32);
        assert!(
            single_run
                .frames
                .iter()
                .all(|frame| frame.bytes <= rows.frame_limit() as u64 + HEADER as u64)
        );
        let runs = source_batches
            .iter()
            .map(|batch| {
                rows.write_run(std::slice::from_ref(batch), &cancel)
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let inputs = runs.iter().collect::<Vec<_>>();
        let merged = rows.merge(&inputs, None, None, &cancel).unwrap();

        assert!(merged.frames.len() >= 32);
        assert!(
            merged
                .frames
                .iter()
                .all(|frame| frame.bytes <= rows.frame_limit() as u64 + HEADER as u64)
        );
        let mut reader = rows.reader(&merged.path).unwrap();
        let mut seen = 0_usize;
        while let Some(batch) = reader.next().unwrap() {
            let ids = batch
                .column_by_name("node_uuid")
                .unwrap()
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            let values = batch
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                let expected = (seen as u128).to_be_bytes();
                assert_eq!(ids.value(row), expected);
                if seen < 32 {
                    assert_eq!(values.value(row), large);
                } else {
                    assert_eq!(values.value(row), "");
                }
                seen += 1;
            }
        }
        assert_eq!(seen, 32 * 1024);
    }

    #[test]
    fn property_frame_accepts_one_row_larger_than_its_target() {
        let root = tempfile::tempdir().unwrap();
        let directory = super::super::StableDirectory::open(root.path()).unwrap();
        let scratch = Scratch::create(&directory).unwrap();
        let mut budgets = GraphConstructionBudgets::default();
        budgets.max_batch_rows = 1024;
        budgets.max_batch_bytes = 256 << 10;
        let rows = new_rows(
            &scratch,
            ConstructionChunkKind::Node,
            budgets,
            0,
            PropertySizing {
                run_bytes: 8 << 20,
                retained_bytes: 24 << 20,
                fan_in: 32,
                frame_bytes: 64 << 10,
            },
        );
        let batch = skewed_property_batch(0, &"x".repeat(100 << 10));
        assert!(batch.get_array_memory_size() <= budgets.max_batch_bytes);
        assert!(PropertyRows::row_bytes(&batch, 0).unwrap() > rows.frame_target);
        let run = rows.write_run(&[batch], &AtomicBool::new(false)).unwrap();
        assert_eq!(run.rows, 1024);
    }

    #[test]
    fn nested_property_rows_charge_only_their_referenced_child_ranges() {
        use arrow::array::{Int32Array, ListArray};
        use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
        use std::sync::Arc;

        let root = tempfile::tempdir().unwrap();
        let directory = super::super::StableDirectory::open(root.path()).unwrap();
        let scratch = Scratch::create(&directory).unwrap();
        let mut budgets = GraphConstructionBudgets::default();
        budgets.max_batch_rows = 1024;
        budgets.max_batch_bytes = 2 << 20;
        let rows = new_rows(
            &scratch,
            ConstructionChunkKind::Node,
            budgets,
            0,
            PropertySizing {
                run_bytes: 8 << 20,
                retained_bytes: 24 << 20,
                fan_in: 32,
                frame_bytes: 192 << 10,
            },
        );
        let ids = (0..1024)
            .map(|id| (id as u128).to_be_bytes())
            .collect::<Vec<_>>();
        let values = Arc::new(Int32Array::from_iter_values(0..(256 * 1024)));
        let list = ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, false)),
            OffsetBuffer::new(ScalarBuffer::from((0..=1024).collect::<Vec<_>>())),
            values,
            Some(NullBuffer::from(
                (0..1024).map(|row| row != 17).collect::<Vec<_>>(),
            )),
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
                Field::new("label", DataType::Utf8, false),
                Field::new(
                    "value",
                    DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
                    true,
                ),
            ])),
            vec![
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(ids.iter().map(|id| id.as_slice()))
                        .unwrap(),
                ) as ArrayRef,
                Arc::new(StringArray::from(vec!["Person"; 1024])),
                Arc::new(list),
            ],
        )
        .unwrap();
        assert!(batch.get_array_memory_size() <= budgets.max_batch_bytes);
        assert!(PropertyRows::row_bytes(&batch, 0).unwrap() < 1024);

        let run = rows.write_run(&[batch], &AtomicBool::new(false)).unwrap();
        assert_eq!(run.frames.len(), 1);
        assert_eq!(run.frames[0].rows, 1024);
    }

#[test]
fn sliced_struct_rows_keep_the_wide_child_in_its_logical_row() {
    use arrow::array::{Int64Array, StructArray};
    use std::sync::Arc;

    let root = tempfile::tempdir().unwrap();
    let directory = super::super::StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let budgets = GraphConstructionBudgets {
        max_batch_rows: 1024,
        max_batch_bytes: 256 << 10,
        ..GraphConstructionBudgets::default()
    };
    let rows = new_rows(
        &scratch,
        ConstructionChunkKind::Node,
        budgets,
        0,
        PropertySizing {
            run_bytes: 8 << 20,
            retained_bytes: 24 << 20,
            fan_in: 32,
            frame_bytes: 192 << 10,
        },
    );
    let wide = "x".repeat(100 << 10);
    let child_fields = vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("timezone", DataType::Utf8, false),
    ];
    let structure = StructArray::new(
        child_fields.clone().into(),
        vec![
            Arc::new(Int64Array::from(vec![0, 1, 2])),
            Arc::new(StringArray::from(vec!["hidden", "", wide.as_str()])),
        ],
        None,
    ).slice(1, 2);
    let data = structure.to_data();
    assert_eq!(data.offset(), 0);
    // `StructArray::slice` normalizes the child to a zero logical offset while
    // retaining the sliced buffer range; the logical-row assertions below are
    // the meaningful contract for the nested payload.
    assert_eq!(data.child_data()[1].offset(), 0);
    let batches = (0..32).map(|task| {
        let ids = [(32 + task as u128).to_be_bytes(), (task as u128).to_be_bytes()];
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
                Field::new("label", DataType::Utf8, false),
                Field::new("value", DataType::Struct(child_fields.clone().into()), false),
            ])),
            vec![
                Arc::new(FixedSizeBinaryArray::try_from_iter(ids.iter().map(|id| id.as_slice())).unwrap()) as ArrayRef,
                Arc::new(StringArray::from(vec!["Person"; 2])),
                Arc::new(structure.clone()),
            ],
        ).unwrap()
    }).collect::<Vec<_>>();
    assert!(PropertyRows::row_bytes(&batches[0], 1).unwrap() >= wide.len());
    let run = rows.write_run(&batches, &AtomicBool::new(false)).unwrap();
    assert!(run.frames.len() >= 32);
    let mut reader = rows.reader(&run.path).unwrap();
    let mut seen = 0_u128;
    while let Some(batch) = reader.next().unwrap() {
        let ids = batch.column(0).as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
        let structures = batch.column(2).as_any().downcast_ref::<StructArray>().unwrap();
        let timezones = structures.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        for row in 0..batch.num_rows() {
            assert_eq!(ids.value(row), seen.to_be_bytes());
            assert_eq!(timezones.value(row), if seen < 32 { wide.as_str() } else { "" });
            seen += 1;
        }
    }
    assert_eq!(seen, 64);
}
