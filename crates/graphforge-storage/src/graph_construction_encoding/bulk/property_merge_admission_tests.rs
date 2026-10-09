fn wide_row(id: u64, width: usize, alias_value: bool) -> RecordBatch {
    use std::sync::Arc;

    let value = "v".repeat(width);
    let values = Arc::new(StringArray::from(vec![value])) as ArrayRef;
    let mut fields = vec![
        Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("label", DataType::Utf8, false),
        Field::new("value_a", DataType::Utf8, false),
    ];
    let mut id_bytes = [0_u8; 16];
    id_bytes[8..].copy_from_slice(&id.to_be_bytes());
    let mut columns = vec![
        Arc::new(
            FixedSizeBinaryArray::try_from_iter([id_bytes].iter().map(|bytes| bytes.as_slice()))
                .unwrap(),
        ) as ArrayRef,
        Arc::new(StringArray::from(vec!["Person"])) as ArrayRef,
        Arc::clone(&values),
    ];
    if alias_value {
        fields.push(Field::new("value_b", DataType::Utf8, false));
        columns.push(values);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

fn insert_wide_runs(rows: &PropertyRows<'_>, count: usize, width: usize, alias: bool) -> Vec<Run> {
    let cancel = AtomicBool::new(false);
    (0..count)
        .map(|id| {
            rows.write_run(&[wide_row(id as u64, width, alias)], &cancel)
                .unwrap()
        })
        .collect()
}

fn alternating_wide_batch(first: u64, rows: usize, width: usize) -> RecordBatch {
    use std::sync::Arc;

    let value = "w".repeat(width);
    let ids = (0..rows)
        .map(|row| {
            let mut id = [0_u8; 16];
            id[8..].copy_from_slice(&(first + 2 * row as u64).to_be_bytes());
            id
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("label", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(
                FixedSizeBinaryArray::try_from_iter(ids.iter().map(|id| id.as_slice())).unwrap(),
            ),
            Arc::new(StringArray::from(vec!["Person"; rows])),
            Arc::new(StringArray::from(vec![value.as_str(); rows])),
        ],
    )
    .unwrap()
}

#[test]
fn wide_frame_reduction_admits_pairs_and_keeps_every_merge_under_the_shared_pool() {
    use std::sync::Arc;

    let root = tempfile::tempdir().unwrap();
    let directory = super::super::StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let mut budgets = GraphConstructionBudgets::default();
    budgets.max_batch_bytes = 4 << 20;
    budgets.max_batch_rows = 8;
    budgets.max_property_columns = 4;
    let capacity = 32 << 20;
    let rows = PropertyRows::new_with_merge_gate(
        &scratch,
        ConstructionChunkKind::Node,
        budgets,
        0,
        PropertySizing {
            run_bytes: 8 << 20,
            retained_bytes: 8 << 20,
            fan_in: 8,
            frame_bytes: 64 << 10,
        },
        Arc::new(super::super::gate::ByteGate::new(capacity)),
        Arc::new(super::super::property_rows::FrameIndexBudget::new(
            super::super::property_rows::FRAME_INDEX_LIMIT_BYTES,
        )),
    );
    let runs = insert_wide_runs(&rows, 8, 1_500_000, true);
    let refs = runs.iter().collect::<Vec<_>>();
    let pair_cost = rows.merge_job_cost(&refs[..2], None, None);
    let all_cost = rows.merge_job_cost(&refs, None, None);
    assert!(
        pair_cost <= capacity,
        "pair reservation {pair_cost} > {capacity}"
    );
    assert!(
        all_cost > capacity,
        "test must exercise wide-job reduction: {all_cost}"
    );
    for run in &runs {
        let mut reader = rows.reader(&run.path).unwrap();
        let decoded = reader.next_expected(&run.frames[0]).unwrap().unwrap();
        assert!(
            decoded.get_array_memory_size() as u64 > run.frames[0].body_bytes,
            "the decoded arrays share the IPC body allocation, so summing array sizes overcounts it"
        );
    }
    rows.groups.lock().unwrap().runs.insert("wide".into(), runs);
    let groups = rows.finish(&AtomicBool::new(false)).unwrap();
    assert_eq!(rows_of(&rows, &groups[0]), (0..8).collect::<Vec<_>>());
    assert!(rows.merge_inputs_peak() <= 8);
    assert!(rows.merge_gate.peak() <= rows.merge_budget_bytes());
    assert!(rows.merge_gate.peak() >= pair_cost);
}

#[test]
fn final_property_ranges_keep_global_uuid_order_above_one_segment() {
    use std::sync::Arc;

    let root = tempfile::tempdir().unwrap();
    let directory = super::super::StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let rows = rows_with(&scratch, 8 << 20, 8, 16 << 20);
    let cancel = AtomicBool::new(false);
    let mut runs = Vec::new();
    for parity in 0..2_u64 {
        let ids = (parity..40)
            .step_by(2)
            .map(u128::from)
            .map(u128::to_be_bytes)
            .collect::<Vec<_>>();
        let value = "x".repeat(1 << 20);
        let values = vec![value.as_str(); ids.len()];
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
                Field::new("label", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(ids.iter().map(|bytes| bytes.as_slice()))
                        .unwrap(),
                ),
                Arc::new(StringArray::from(vec!["Person"; ids.len()])),
                Arc::new(StringArray::from(values)),
            ],
        )
        .unwrap();
        runs.push(rows.write_run(&[batch], &cancel).unwrap());
    }
    assert!(runs.iter().map(Run::bytes).sum::<u64>() > (32 << 20));
    rows.groups
        .lock()
        .unwrap()
        .runs
        .insert("segments".into(), runs);
    let groups = rows.finish(&cancel).unwrap();
    assert!(groups[0].segments.len() > 1);
    let expected = (0..40).collect::<Vec<_>>();
    assert_eq!(rows_of(&rows, &groups[0]), expected);

    let mut reversed = Vec::new();
    for run in groups[0].segments.iter().rev() {
        let mut reader = rows.reader(&run.path).unwrap();
        while let Some(batch) = reader.next().unwrap() {
            let uuids = crate::graph_construction::batch_uuid_column(&batch, "node_uuid").unwrap();
            for row in 0..batch.num_rows() {
                reversed.push(u64::from_be_bytes(
                    uuids.value(row)[8..].try_into().unwrap(),
                ));
            }
        }
    }
    assert_ne!(
        reversed, expected,
        "segment order is part of global UUID order"
    );
}

#[test]
fn pre_cancelled_wide_merge_does_not_read_or_reserve_frames() {
    let root = tempfile::tempdir().unwrap();
    let directory = super::super::StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let rows = rows_with(&scratch, 8 << 20, 8, 16 << 20);
    let runs = insert_wide_runs(&rows, 2, 128 << 10, false);
    let refs = runs.iter().collect::<Vec<_>>();
    let before_read = rows.read_bytes();
    let error = rows
        .merge(&refs, None, None, &AtomicBool::new(true))
        .unwrap_err();
    assert!(error.to_string().contains("cancelled"), "{error}");
    assert_eq!(rows.read_bytes(), before_read);
    assert_eq!(rows.merge_gate.peak(), 0);
}

#[test]
fn cancellation_after_a_byte_flush_stops_before_the_next_output_flush() {
    use std::sync::atomic::Ordering;
    use std::time::{Duration, Instant};

    let root = tempfile::tempdir().unwrap();
    let directory = super::super::StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let mut rows = rows_with(&scratch, 8 << 20, 8, 16 << 20);
    rows.frame_target = 256 << 10;
    let batch_a = alternating_wide_batch(0, 8, 128 << 10);
    let batch_b = alternating_wide_batch(1, 8, 128 << 10);
    let mut runs = Vec::new();
    for batch in [&batch_a, &batch_b] {
        let uuids = crate::graph_construction::batch_uuid_column(batch, "node_uuid").unwrap();
        let max_row = (0..batch.num_rows())
            .map(|row| PropertyRows::row_bytes(batch, row).unwrap())
            .max()
            .unwrap();
        let mut writer = rows.run_writer().unwrap();
        writer
            .append(
                batch,
                <[u8; 16]>::try_from(uuids.value(0)).unwrap(),
                <[u8; 16]>::try_from(uuids.value(batch.num_rows() - 1)).unwrap(),
                max_row,
            )
            .unwrap();
        runs.push(writer.finish().unwrap());
    }
    let refs = runs.iter().collect::<Vec<_>>();
    let cancel = AtomicBool::new(false);
    let before = rows.written_bytes();
    std::thread::scope(|scope| {
        let worker = scope.spawn(|| rows.merge(&refs, None, None, &cancel));
        let deadline = Instant::now() + Duration::from_secs(10);
        while rows.written_bytes() == before && !worker.is_finished() {
            assert!(
                Instant::now() < deadline,
                "merge did not reach its first byte flush"
            );
            std::thread::yield_now();
        }
        assert!(
            rows.written_bytes() > before,
            "first byte flush must complete"
        );
        cancel.store(true, Ordering::Release);
        let error = worker.join().unwrap().unwrap_err();
        assert!(error.to_string().contains("cancelled"), "{error}");
    });
}

#[test]
fn frame_index_growth_is_charged_and_released_with_runs() {
    use std::sync::Arc;

    let root = tempfile::tempdir().unwrap();
    let directory = super::super::StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let budget = Arc::new(super::super::property_rows::FrameIndexBudget::new(
        super::super::property_rows::FRAME_INDEX_ENTRY_BYTES,
    ));
    let rows = PropertyRows::new_with_merge_gate(
        &scratch,
        ConstructionChunkKind::Node,
        GraphConstructionBudgets::default(),
        0,
        PropertySizing::SERIAL,
        Arc::new(super::super::gate::ByteGate::new(1 << 20)),
        Arc::clone(&budget),
    );
    let cancel = AtomicBool::new(false);
    let retained = rows.write_run(&[batch(0, 1)], &cancel).unwrap();
    let error = rows.write_run(&[batch(1, 1)], &cancel).unwrap_err();
    assert!(matches!(
        error,
        GfError::Project {
            code: graphforge_core::ProjectErrorCode::ResourceLimit,
            ..
        }
    ));
    drop(retained);
    let replacement = rows.write_run(&[batch(2, 1)], &cancel).unwrap();
    assert_eq!(replacement.rows, 1);
}

#[test]
fn raw_sliced_struct_child_offsets_contribute_the_logical_wide_value() {
    use arrow::array::{ArrayData, Int64Array};

    let wide = "z".repeat(100 << 10);
    let fields = vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("timezone", DataType::Utf8, false),
    ];
    let timestamps = Int64Array::from(vec![0, 1, 2]).to_data().slice(1, 2);
    let timezones = StringArray::from(vec!["hidden", "", wide.as_str()])
        .to_data()
        .slice(1, 2);
    let data = ArrayData::builder(DataType::Struct(fields.into()))
        .len(2)
        .child_data(vec![timestamps, timezones])
        .build()
        .unwrap();
    assert_eq!(data.offset(), 0);
    assert_eq!(data.child_data()[1].offset(), 1);

    let measured = PropertyRows::struct_row_bytes(&data, 1).unwrap();
    assert!(
        measured >= wide.len(),
        "logical wide value measured {measured}"
    );
    assert!(matches!(
        PropertyRows::struct_row_bytes(&data, 0),
        Ok(bytes) if bytes < wide.len()
    ));
}
