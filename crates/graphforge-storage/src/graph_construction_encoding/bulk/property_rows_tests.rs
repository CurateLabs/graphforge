use super::*;
use arrow::array::{Array, ArrayRef, FixedSizeBinaryArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};

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

#[test]
fn scratch_traffic_matches_file_lengths_including_repeated_scans() {
    let root = tempfile::tempdir().unwrap();
    let directory = super::super::StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let rows = PropertyRows::new(
        &scratch,
        ConstructionChunkKind::Node,
        GraphConstructionBudgets::default(),
        0,
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

#[test]
fn binary_runs_sort_globally_without_retaining_one_run_per_batch() {
    let root = tempfile::tempdir().unwrap();
    let directory = super::super::StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let rows = PropertyRows::new(
        &scratch,
        ConstructionChunkKind::Node,
        GraphConstructionBudgets::default(),
        0,
    );
    let cancel = AtomicBool::new(false);
    for index in (0..65).rev() {
        rows.ingest(&batch(index * 32, 32), &cancel).unwrap();
    }
    assert!(
        rows.groups
            .lock()
            .unwrap()
            .values()
            .all(|group| group.levels.len() <= 7)
    );
    let groups = rows.finish(&cancel).unwrap();
    assert_eq!(groups.len(), 1);
    let mut reader = rows.reader(&groups[0].path).unwrap();
    let mut expected = 0_u64;
    while let Some(batch) = reader.next().unwrap() {
        let uuids = crate::graph_construction::batch_uuid_column(&batch, "node_uuid").unwrap();
        for row in 0..batch.num_rows() {
            assert_eq!(&uuids.value(row)[8..], &expected.to_be_bytes());
            expected += 1;
        }
    }
    assert_eq!(expected, 65 * 32);
    assert!(rows.written_bytes() > 0 && rows.read_bytes() > 0);
}

#[test]
fn ipc_type_metadata_is_charged_before_schema_instantiation() {
    let root = tempfile::tempdir().unwrap();
    let directory = super::super::StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let rows = PropertyRows::new(
        &scratch,
        ConstructionChunkKind::Node,
        GraphConstructionBudgets::default(),
        0,
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
    let rows = PropertyRows::new(
        &scratch,
        ConstructionChunkKind::Node,
        GraphConstructionBudgets::default(),
        0,
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

fn live_scratch_bytes(directory: &super::super::StableDirectory) -> u64 {
    let scratch = directory
        .path()
        .join(super::super::scratch::SCRATCH_DIRECTORY);
    std::fs::read_dir(scratch)
        .unwrap()
        .map(|entry| entry.unwrap().metadata().unwrap().len())
        .sum()
}

#[test]
fn repeated_property_merges_track_live_files_and_the_final_consume_releases_everything() {
    let root = tempfile::tempdir().unwrap();
    let directory = super::super::StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let rows = PropertyRows::new(
        &scratch,
        ConstructionChunkKind::Node,
        GraphConstructionBudgets::default(),
        0,
    );
    let cancel = AtomicBool::new(false);
    for index in (0..65).rev() {
        rows.ingest(&batch(index * 32, 32), &cancel).unwrap();
    }
    // The tracker counts exactly the runs that still exist: every merge
    // released its inputs once the merged output was complete.
    let live = scratch.occupied_bytes();
    assert!(live > 0);
    assert_eq!(live, live_scratch_bytes(&directory));
    let groups = rows.finish(&cancel).unwrap();
    assert_eq!(groups.len(), 1);
    assert_eq!(
        scratch.occupied_bytes(),
        std::fs::metadata(&groups[0].path).unwrap().len()
    );
    // Cumulative frame traffic strictly exceeds the peak: merge inputs left
    // the occupancy before later levels wrote on top of them.
    assert!(scratch.peak_occupied_bytes() < rows.written_bytes());
    // The final consume verifies every frame; reclaiming the spent group
    // then empties the tracker, so nothing is left in the tree.
    let mut reader = rows.reader(&groups[0].path).unwrap();
    let mut expected = 0_u64;
    while let Some(batch) = reader.next().unwrap() {
        let uuids = crate::graph_construction::batch_uuid_column(&batch, "node_uuid").unwrap();
        for row in 0..batch.num_rows() {
            assert_eq!(&uuids.value(row)[8..], &expected.to_be_bytes());
            expected += 1;
        }
    }
    assert_eq!(expected, 65 * 32);
    rows.reclaim(&groups[0].path).unwrap();
    assert_eq!(scratch.occupied_bytes(), 0);
    assert!(!groups[0].path.exists());
    scratch.remove().unwrap();
}

#[test]
fn a_failed_property_write_keeps_its_reservation() {
    // Reserve-before-write: the frame is charged before its write can grow
    // the file, and the charge survives the failure because a partial write
    // may exist. The target is removed so the write fails on open, before
    // any byte moves; a charge after the write would leave the tracker empty.
    let root = tempfile::tempdir().unwrap();
    let directory = super::super::StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let rows = PropertyRows::new(
        &scratch,
        ConstructionChunkKind::Node,
        GraphConstructionBudgets::default(),
        0,
    );
    let path = rows.path().unwrap();
    std::fs::remove_file(&path).unwrap();
    rows.write(&path, &batch(0, 3)).unwrap_err();
    assert_eq!(rows.written_bytes(), 0);
    let reserved = scratch.occupied_bytes();
    assert!(reserved > 0);
    assert_eq!(scratch.peak_occupied_bytes(), reserved);
    scratch.remove().unwrap();
}

#[test]
fn reclaiming_bytes_no_writer_reserved_is_an_accounting_error() {
    let root = tempfile::tempdir().unwrap();
    let directory = super::super::StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let path = scratch.file("unaccounted.frames");
    std::fs::write(&path, b"sevenish").unwrap();
    let error = scratch.reclaim_file(&path).unwrap_err();
    assert!(error.to_string().contains("underflow"), "{error}");
    scratch.remove().unwrap();
}
