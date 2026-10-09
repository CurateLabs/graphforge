use super::*;
use arrow::array::{Array, ArrayRef, FixedSizeBinaryArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};

include!("property_rows_frame_tests.rs");
include!("property_merge_admission_tests.rs");

impl RunSink<'_, '_> {
    /// Bytes of the batches held now, which the gate must have granted.
    fn retained_bytes(&self) -> u64 {
        self.pending
            .values()
            .flat_map(|pending| &pending.batches)
            .map(|batch| (batch.get_array_memory_size() + batch.num_rows() * KEY_BYTES) as u64)
            .sum()
    }
}

fn batch(start: u64, count: usize) -> RecordBatch {
    let ids = (start..start + count as u64)
        .rev()
        .map(|id| {
            let mut bytes = [0; 16];
            bytes[8..].copy_from_slice(&id.to_be_bytes());
            bytes
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(
        std::sync::Arc::new(Schema::new(vec![
            Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("label", DataType::Utf8, false),
            Field::new("value", DataType::Int64, true),
        ])),
        vec![
            std::sync::Arc::new(
                FixedSizeBinaryArray::try_from_iter(ids.iter().map(|id| id.as_slice())).unwrap(),
            ) as ArrayRef,
            std::sync::Arc::new(StringArray::from(vec!["Person"; count])),
            std::sync::Arc::new(Int64Array::from(
                (0..count)
                    .map(|i| (i % 3 != 0).then_some(i as i64))
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn new_rows(
    scratch: &Scratch,
    kind: ConstructionChunkKind,
    budgets: GraphConstructionBudgets,
    schema_bytes: u64,
    sizing: PropertySizing,
) -> PropertyRows<'_> {
    let merge_capacity = super::super::budget::property_merge_capacity(
        budgets,
        schema_bytes,
        0,
        sizing.retained_bytes,
        0,
    );
    PropertyRows::new_with_merge_gate(
        scratch,
        kind,
        budgets,
        schema_bytes,
        sizing,
        std::sync::Arc::new(super::super::gate::ByteGate::new(merge_capacity)),
        std::sync::Arc::new(super::super::property_rows::FrameIndexBudget::new(
            super::super::property_rows::FRAME_INDEX_LIMIT_BYTES,
        )),
    )
}

#[test]
fn scratch_traffic_matches_file_lengths_including_repeated_scans() {
    let root = tempfile::tempdir().unwrap();
    let directory = super::super::StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let rows = new_rows(
        &scratch,
        ConstructionChunkKind::Node,
        GraphConstructionBudgets::default(),
        0,
        PropertySizing::SERIAL,
    );
    let path = rows.path().unwrap();
    let before_write = rows.written_bytes();
    rows.write(&path, &batch(0, 3)).unwrap();
    rows.write(&path, &batch(3, 4)).unwrap();
    let file_bytes = std::fs::metadata(&path).unwrap().len();
    assert_eq!(rows.written_bytes() - before_write, file_bytes);
    let before_read = rows.read_bytes();
    for _ in 0..2 {
        let mut reader = rows.reader(&path).unwrap();
        let mut count = 0;
        while let Some(batch) = reader.next().unwrap() {
            count += batch.num_rows();
        }
        assert_eq!(count, 7);
    }
    assert_eq!(rows.read_bytes() - before_read, 2 * file_bytes);
}

/// `batch(start, 8)` without its properties.
fn narrow(start: u64) -> RecordBatch {
    batch(start, 8).project(&[0, 1]).unwrap()
}

fn rows_of(rows: &PropertyRows<'_>, group: &SortedGroup) -> Vec<u64> {
    let mut reader = rows.group_reader(group);
    let mut seen = Vec::new();
    while let Some(batch) = reader.next().unwrap() {
        let uuids = crate::graph_construction::batch_uuid_column(&batch, "node_uuid").unwrap();
        for row in 0..batch.num_rows() {
            seen.push(u64::from_be_bytes(
                uuids.value(row)[8..].try_into().unwrap(),
            ));
        }
    }
    seen
}

fn rows_with(
    scratch: &Scratch,
    run_bytes: usize,
    fan_in: usize,
    retained_bytes: u64,
) -> PropertyRows<'_> {
    new_rows(
        scratch,
        ConstructionChunkKind::Node,
        GraphConstructionBudgets::default(),
        0,
        PropertySizing {
            run_bytes,
            retained_bytes,
            fan_in,
            frame_bytes: 4096,
        },
    )
}

/// Shuffled batches of 32 identities each, from `threads` concurrent sinks.
fn ingest(rows: &PropertyRows<'_>, batches: u64, threads: u64) {
    let cancel = AtomicBool::new(false);
    std::thread::scope(|scope| {
        for thread in 0..threads {
            let cancel = &cancel;
            scope.spawn(move || {
                let mut sink = rows.sink();
                for index in (0..batches).filter(|index| index % threads == thread).rev() {
                    sink.push(&batch(index * 32, 32), cancel).unwrap();
                    // What this worker holds is always paid for.
                    assert!(sink.held >= sink.retained_bytes().min(rows.sizing.retained_bytes));
                    assert!(sink.held <= rows.sizing.retained_bytes);
                }
                sink.finish(cancel).unwrap();
            });
        }
    });
}

#[test]
fn runs_merge_into_one_sorted_stream_at_every_fan_in_and_run_size() {
    for (run_bytes, fan_in, threads) in [
        (1, 2, 1),
        (1, 3, 4),
        (3 << 10, 2, 3),
        (3 << 10, 5, 2),
        (1 << 20, 16, 1),
        (1 << 20, 2, 4),
    ] {
        let root = tempfile::tempdir().unwrap();
        let directory = super::super::StableDirectory::open(root.path()).unwrap();
        let scratch = Scratch::create(&directory).unwrap();
        let rows = rows_with(&scratch, run_bytes, fan_in, 1 << 20);
        ingest(&rows, 65, threads);
        let formed = rows.runs_formed();
        assert!(
            run_bytes > 1 << 10 || formed >= 65,
            "run_bytes {run_bytes}: {formed} runs"
        );
        let groups = rows.finish(&AtomicBool::new(false)).unwrap();
        assert_eq!(groups.len(), 1, "run_bytes {run_bytes} fan_in {fan_in}");
        let seen = rows_of(&rows, &groups[0]);
        assert_eq!(
            seen,
            (0..65 * 32).collect::<Vec<_>>(),
            "run_bytes {run_bytes} fan_in {fan_in} threads {threads}"
        );
        assert!(rows.written_bytes() > 0 && rows.read_bytes() > 0);
        // No merge held more runs open than the fan-in, however many there were.
        assert!(
            rows.merge_inputs_peak() <= fan_in as u64,
            "{} runs open at fan-in {fan_in}",
            rows.merge_inputs_peak()
        );
        if formed > fan_in as u64 {
            assert!(rows.merge_inputs_peak() >= 2);
        }
    }
}

#[test]
fn few_runs_are_not_rewritten_by_a_wide_enough_merge() {
    // 65 runs under a fan-in of 64 merge once into segments; the input is
    // written once and the segments once: no per-level rewriting.
    let root = tempfile::tempdir().unwrap();
    let directory = super::super::StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let rows = rows_with(&scratch, 1, 64, 1 << 20);
    ingest(&rows, 65, 1);
    let runs_written = rows.written_bytes();
    let groups = rows.finish(&AtomicBool::new(false)).unwrap();
    let merged_written = rows.written_bytes() - runs_written;
    // One run per batch; the 65th batch makes one merge of the two
    // smallest runs, and the final merge rewrites everything once more.
    assert!(
        merged_written <= runs_written + runs_written / 8,
        "runs {runs_written} merged {merged_written}"
    );
    assert_eq!(rows_of(&rows, &groups[0]).len(), 65 * 32);
}

#[test]
fn concurrent_intake_never_holds_more_than_the_gate_admits() {
    let root = tempfile::tempdir().unwrap();
    let directory = super::super::StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let one = batch(0, 32);
    let need = (one.get_array_memory_size() + 32 * KEY_BYTES) as u64;
    // Room for exactly two batches while eight threads push: the rest wait.
    let rows = rows_with(&scratch, usize::MAX >> 1, 4, 2 * need);
    ingest(&rows, 64, 8);
    assert_eq!(rows.gate.free(), 2 * need);
    // Eight threads shared room for two batches, and used it.
    assert!(rows.peak_retained_bytes() >= need && rows.peak_retained_bytes() <= 2 * need);
    let groups = rows.finish(&AtomicBool::new(false)).unwrap();
    assert_eq!(rows_of(&rows, &groups[0]), (0..64 * 32).collect::<Vec<_>>());
}

#[test]
fn an_abandoned_sink_returns_its_bytes_to_the_gate() {
    let root = tempfile::tempdir().unwrap();
    let directory = super::super::StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let one = batch(0, 32);
    let need = (one.get_array_memory_size() + 32 * KEY_BYTES) as u64;
    let rows = rows_with(&scratch, usize::MAX >> 1, 4, need);
    let cancel = AtomicBool::new(false);
    let mut sink = rows.sink();
    sink.push(&one, &cancel).unwrap();
    drop(sink);
    // Were the bytes stranded, this would wait for the 20 ms poll forever.
    let mut sink = rows.sink();
    sink.push(&batch(32, 32), &cancel).unwrap();
    sink.finish(&cancel).unwrap();
}

#[test]
fn a_range_merge_yields_exactly_the_rows_inside_the_range() {
    let root = tempfile::tempdir().unwrap();
    let directory = super::super::StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let rows = rows_with(&scratch, 1, 64, 1 << 20);
    ingest(&rows, 20, 1);
    let runs = std::mem::take(&mut rows.groups.lock().unwrap().runs)
        .into_values()
        .next()
        .unwrap();
    let id = |value: u64| {
        let mut bytes = [0_u8; 16];
        bytes[8..].copy_from_slice(&value.to_be_bytes());
        bytes
    };
    let refs = runs.iter().collect::<Vec<_>>();
    let cancel = AtomicBool::new(false);
    for (lower, upper) in [
        (None, None),
        (Some(100), Some(101)),
        (Some(31), Some(33)),
        (None, Some(7)),
        (Some(630), None),
        (Some(10_000), None),
        (Some(5), Some(5)),
    ] {
        let merged = rows
            .merge(&refs, lower.map(id), upper.map(id), &cancel)
            .unwrap();
        let group = SortedGroup {
            segments: vec![merged],
            bare_owners: None,
        };
        let seen = rows_of(&rows, &group);
        let expected = (0..20 * 32)
            .filter(|value| lower.is_none_or(|lower| *value >= lower))
            .filter(|value| upper.is_none_or(|upper| *value < upper))
            .collect::<Vec<_>>();
        assert_eq!(seen, expected, "{lower:?}..{upper:?}");
    }
}

#[test]
fn identities_repeated_across_runs_are_all_kept() {
    let root = tempfile::tempdir().unwrap();
    let directory = super::super::StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let rows = rows_with(&scratch, 1, 2, 1 << 20);
    let cancel = AtomicBool::new(false);
    let mut sink = rows.sink();
    for _ in 0..5 {
        sink.push(&batch(0, 32), &cancel).unwrap();
    }
    sink.finish(&cancel).unwrap();
    let groups = rows.finish(&cancel).unwrap();
    let seen = rows_of(&rows, &groups[0]);
    assert_eq!(seen.len(), 5 * 32);
    assert!(seen.windows(2).all(|pair| pair[0] <= pair[1]));
}

#[test]
fn schemas_stay_in_separate_groups_and_the_group_budget_is_enforced() {
    let root = tempfile::tempdir().unwrap();
    let directory = super::super::StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let rows = rows_with(&scratch, 1 << 20, 4, 1 << 20);
    let cancel = AtomicBool::new(false);
    let mut sink = rows.sink();
    sink.push(&batch(0, 8), &cancel).unwrap();
    sink.push(&narrow(8), &cancel).unwrap();
    sink.push(&batch(16, 8), &cancel).unwrap();
    sink.finish(&cancel).unwrap();
    let groups = rows.finish(&cancel).unwrap();
    assert_eq!(groups.len(), 2);
    // The schema without properties keeps its owners, not its rows.
    let (bare, with_rows) = groups
        .iter()
        .partition::<Vec<_>, _>(|group| group.bare_owners.is_some());
    assert_eq!((bare.len(), with_rows.len()), (1, 1));
    assert_eq!(bare[0].bare_owners, Some(vec![("Person".to_owned(), 8)]));
    assert!(bare[0].segments.is_empty());
    assert_eq!(rows_of(&rows, with_rows[0]).len(), 16);

    let limited = new_rows(
        &scratch,
        ConstructionChunkKind::Node,
        GraphConstructionBudgets {
            max_schema_groups: 1,
            ..GraphConstructionBudgets::default()
        },
        0,
        PropertySizing::SERIAL,
    );
    let mut sink = limited.sink();
    sink.push(&batch(0, 8), &cancel).unwrap();
    let error = sink.push(&narrow(8), &cancel).unwrap_err();
    assert!(error.to_string().contains("schema-group budget"), "{error}");
}

/// A node batch with a label per row and no properties.
fn labelled(ids: &[u64], labels: &[&str]) -> RecordBatch {
    let uuids = ids
        .iter()
        .map(|id| {
            let mut bytes = [0; 16];
            bytes[8..].copy_from_slice(&id.to_be_bytes());
            bytes
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(
        std::sync::Arc::new(Schema::new(vec![
            Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("label", DataType::Utf8, false),
        ])),
        vec![
            std::sync::Arc::new(
                FixedSizeBinaryArray::try_from_iter(uuids.iter().map(|id| id.as_slice())).unwrap(),
            ) as ArrayRef,
            std::sync::Arc::new(StringArray::from(labels.to_vec())),
        ],
    )
    .unwrap()
}

#[test]
fn a_group_without_properties_orders_owners_by_first_appearance_in_identity_order() {
    let root = tempfile::tempdir().unwrap();
    let directory = super::super::StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let rows = rows_with(&scratch, 1 << 20, 4, 1 << 20);
    let cancel = AtomicBool::new(false);
    // Identity order is 1:Pet 2:Person 3:Person 4:City 5:Pet 6:City 7:Pet; the
    // batches arrive out of order and from two workers.
    std::thread::scope(|scope| {
        scope.spawn(|| {
            let mut sink = rows.sink();
            sink.push(&labelled(&[6, 3, 5], &["City", "Person", "Pet"]), &cancel)
                .unwrap();
            sink.push(&labelled(&[7], &["Pet"]), &cancel).unwrap();
            sink.finish(&cancel).unwrap();
        });
        scope.spawn(|| {
            let mut sink = rows.sink();
            sink.push(&labelled(&[4, 2, 1], &["City", "Person", "Pet"]), &cancel)
                .unwrap();
            sink.finish(&cancel).unwrap();
        });
    });
    let groups = rows.finish(&cancel).unwrap();
    assert_eq!(groups.len(), 1);
    assert_eq!(
        groups[0].bare_owners,
        Some(vec![
            ("Pet".to_owned(), 3),
            ("Person".to_owned(), 2),
            ("City".to_owned(), 2),
        ])
    );
    assert_eq!(
        rows.written_bytes(),
        0,
        "a bare group must write no scratch"
    );
    assert_eq!(rows.runs_formed(), 0);
}

#[test]
fn ipc_type_metadata_is_charged_before_schema_instantiation() {
    let root = tempfile::tempdir().unwrap();
    let directory = super::super::StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let rows = new_rows(
        &scratch,
        ConstructionChunkKind::Node,
        GraphConstructionBudgets::default(),
        0,
        PropertySizing::SERIAL,
    );
    let zone = std::iter::repeat_n('x', 2 << 20).collect::<String>();
    let temporal = arrow::array::TimestampNanosecondArray::from(vec![0; 3]).with_timezone(zone);
    let schema = std::sync::Arc::new(Schema::new(vec![Field::new(
        "when",
        temporal.data_type().clone(),
        true,
    )]));
    let input = RecordBatch::try_new(schema, vec![std::sync::Arc::new(temporal)]).unwrap();
    let path = rows.path().unwrap();
    rows.write(&path, &input).unwrap();
    assert!(
        rows.reader(&path)
            .unwrap()
            .next()
            .unwrap_err()
            .to_string()
            .contains("schema exceeds")
    );
}

#[test]
fn frames_reject_crc_corruption_truncation_and_unbounded_ipc_bodies() {
    let root = tempfile::tempdir().unwrap();
    let directory = super::super::StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let rows = new_rows(
        &scratch,
        ConstructionChunkKind::Node,
        GraphConstructionBudgets::default(),
        0,
        PropertySizing::SERIAL,
    );
    let path = rows.path().unwrap();
    rows.write(&path, &batch(0, 3)).unwrap();
    let original = std::fs::read(&path).unwrap();
    let mut corrupt = original.clone();
    *corrupt.last_mut().unwrap() ^= 1;
    std::fs::write(&path, corrupt).unwrap();
    assert!(
        rows.reader(&path)
            .unwrap()
            .next()
            .unwrap_err()
            .to_string()
            .contains("CRC32C")
    );
    std::fs::write(&path, &original[..original.len() - 1]).unwrap();
    assert!(
        rows.reader(&path)
            .unwrap()
            .next()
            .unwrap_err()
            .to_string()
            .contains("truncated")
    );
    let mut corrupt = original.clone();
    let payload = &mut corrupt[HEADER..];
    let schema_size = i32::from_le_bytes(payload[4..8].try_into().unwrap()) as usize;
    let record_start = 8 + schema_size;
    let record_size = i32::from_le_bytes(
        payload[record_start + 4..record_start + 8]
            .try_into()
            .unwrap(),
    ) as usize;
    let metadata_start = record_start + 8;
    let message =
        arrow::ipc::root_as_message(&payload[metadata_start..metadata_start + record_size])
            .unwrap();
    let length = metadata_start
        + message._tab.loc()
        + usize::from(
            message
                ._tab
                .vtable()
                .get(arrow::ipc::Message::VT_BODYLENGTH),
        );
    payload[length..length + 8].copy_from_slice(&i64::MAX.to_le_bytes());
    let crc = crc32c(payload);
    corrupt[8..12].copy_from_slice(&crc.to_le_bytes());
    std::fs::write(&path, corrupt).unwrap();
    assert!(
        rows.reader(&path)
            .unwrap()
            .next()
            .unwrap_err()
            .to_string()
            .contains("IPC lengths")
    );

    let mut corrupt = original.clone();
    let payload = &mut corrupt[HEADER..];
    let message =
        arrow::ipc::root_as_message(&payload[metadata_start..metadata_start + record_size])
            .unwrap();
    let batch = message.header_as_record_batch().unwrap();
    let slot = metadata_start
        + batch._tab.loc()
        + usize::from(batch._tab.vtable().get(arrow::ipc::RecordBatch::VT_BUFFERS));
    let vector = slot + u32::from_le_bytes(payload[slot..slot + 4].try_into().unwrap()) as usize;
    payload[vector + 4 + 8..vector + 4 + 16].copy_from_slice(&i64::MAX.to_le_bytes());
    let crc = crc32c(payload);
    corrupt[8..12].copy_from_slice(&crc.to_le_bytes());
    std::fs::write(&path, corrupt).unwrap();
    assert!(
        rows.reader(&path)
            .unwrap()
            .next()
            .unwrap_err()
            .to_string()
            .contains("IPC lengths")
    );
}
