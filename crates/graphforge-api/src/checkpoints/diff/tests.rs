use super::*;
use crate::PageToken;
use crate::checkpoints::CheckpointRequest;
use crate::checkpoints::tests::operation;
use arrow::array::StringArray;
use tempfile::tempdir;

#[test]
fn checkpoint_diff_batches_preserve_change_kinds_and_nullable_sides() {
    fn participant(
        rows: u64,
        schema: u8,
        content: u8,
    ) -> graphforge_storage::ProjectParticipantDescriptor {
        graphforge_storage::ProjectParticipantDescriptor {
            capability_id: "graph".into(),
            capability_version: 1,
            record_family_id: "snapshot".into(),
            record_version: 1,
            encoding: "arrow-ipc".into(),
            schema_fingerprint: [schema; 32],
            row_count: rows,
            content_sha256: [content; 32],
        }
    }

    let added = ("graph".into(), "added".into(), "snapshot".into());
    let removed = ("graph".into(), "removed".into(), "snapshot".into());
    let unchanged = ("graph".into(), "same".into(), "snapshot".into());
    let modified = ("graph".into(), "changed".into(), "snapshot".into());
    let keys = vec![
        added.clone(),
        removed.clone(),
        unchanged.clone(),
        modified.clone(),
    ];
    let left = Inventory::from([
        (removed, participant(1, 1, 1)),
        (unchanged.clone(), participant(2, 2, 2)),
        (modified.clone(), participant(3, 3, 3)),
    ]);
    let right = Inventory::from([
        (added, participant(4, 4, 4)),
        (unchanged, participant(2, 2, 2)),
        (modified, participant(5, 3, 9)),
    ]);
    let summary = summary_batch(Uuid::nil(), Uuid::max(), &keys, &left, &right, None).unwrap();
    let batch = &summary.batches[0];
    let kinds = batch
        .column_by_name("change_kind")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        (0..kinds.len()).map(|i| kinds.value(i)).collect::<Vec<_>>(),
        vec!["added", "removed", "unchanged", "modified"]
    );
    let from_rows = batch.column_by_name("from_row_count").unwrap();
    let to_rows = batch.column_by_name("to_row_count").unwrap();
    assert!(from_rows.is_null(0));
    assert!(to_rows.is_null(1));

    let record_uuid = Uuid::from_u128(42);
    let records = vec![
        RecordChange {
            scope: "graph".into(),
            family: "nodes".into(),
            record_uuid: Some(record_uuid),
            identity: [7; 32],
            kind: "modified",
            from: Some([8; 32]),
            to: Some([9; 32]),
        },
        RecordChange {
            scope: "graph".into(),
            family: "edges".into(),
            record_uuid: None,
            identity: [10; 32],
            kind: "added",
            from: None,
            to: Some([11; 32]),
        },
    ];
    let records = record_batch(Uuid::nil(), Uuid::max(), &records, None).unwrap();
    let batch = &records.batches[0];
    let uuids = batch
        .column_by_name("record_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(uuids.value(0), record_uuid.as_bytes());
    assert!(uuids.is_null(1));
    assert!(
        batch
            .column_by_name("from_record_fingerprint")
            .unwrap()
            .is_null(1)
    );
}

#[test]
fn checkpoint_row_projection_uuid_and_parquet_failures_are_structured() {
    let uuid = {
        let mut bytes = [41_u8; 16];
        bytes[6] = (bytes[6] & 0x0f) | 0x70;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Uuid::from_bytes(bytes)
    };
    let uuids =
        FixedSizeBinaryArray::try_from_iter([uuid.as_bytes().as_slice()].into_iter()).unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "record_uuid",
            DataType::FixedSizeBinary(16),
            false,
        )])),
        vec![Arc::new(uuids)],
    )
    .unwrap();
    let projected = project_row(&batch, 0, &["record_uuid"]).unwrap();
    assert_eq!(projected.num_rows(), 1);
    assert_eq!(record_uuid(&batch, 0, "record_uuid").unwrap(), uuid);
    assert_eq!(
        project_row(&batch, 0, &["missing"]).unwrap_err().code(),
        "GF_SCHEMA_MISMATCH"
    );

    let strings = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "record_uuid",
            DataType::Utf8,
            false,
        )])),
        vec![Arc::new(StringArray::from(vec!["not-a-uuid"]))],
    )
    .unwrap();
    assert_eq!(
        record_uuid(&strings, 0, "record_uuid").unwrap_err().code(),
        "GF_SCHEMA_MISMATCH"
    );
    let nulls = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "record_uuid",
            DataType::FixedSizeBinary(16),
            true,
        )])),
        vec![Arc::new(FixedSizeBinaryArray::new_null(16, 1))],
    )
    .unwrap();
    assert_eq!(
        record_uuid(&nulls, 0, "record_uuid").unwrap_err().code(),
        "GF_SCHEMA_MISMATCH"
    );
    assert_eq!(
        read_parquet(b"not parquet", &PageRequest::default())
            .unwrap_err()
            .code(),
        "GF_VALIDATION"
    );
    let adapters = record_adapters().unwrap();
    assert!(adapters.contains_key(&("knowledge", "assertions")));
    assert!(adapters.contains_key(&("provenance", "events")));
}

#[test]
fn record_diff_uses_registered_logical_adapters() {
    let directory = tempdir().unwrap();
    let graph = GraphForge::new(Some(directory.path().to_str().unwrap())).unwrap();
    graph
        .enable_capability(crate::EnableCapabilityRequest {
            context: crate::WriteContext {
                operation_uuid: operation(20),
                actor_uuid: None,
            },
            capability_id: crate::CapabilityId::Knowledge,
            capability_version: 1,
        })
        .unwrap();
    graph
        .checkpoint(CheckpointRequest {
            name: "A".into(),
            description: None,
            idempotency_key: operation(21),
            actor_uuid: None,
        })
        .unwrap();
    graph.execute("CREATE (:Person {name: 'added'})").unwrap();
    graph
        .checkpoint(CheckpointRequest {
            name: "B".into(),
            description: None,
            idempotency_key: operation(22),
            actor_uuid: None,
        })
        .unwrap();
    let diff = graph
        .diff_checkpoints(DiffCheckpointsRequest {
            from: CheckpointSelector::Named("A".into()),
            to: CheckpointSelector::Named("B".into()),
            scope: CheckpointDiffScope::All,
            detail: CheckpointDiffDetail::Records,
            page: PageRequest::default(),
        })
        .unwrap();
    assert_eq!(diff.batches[0].num_rows(), 1);
    assert_eq!(diff.schema.field(5).name(), "record_identity_fingerprint");
}

#[test]
fn record_diff_pagination_cancellation_and_bounds() {
    let directory = tempdir().unwrap();
    let graph = GraphForge::new(Some(directory.path().to_str().unwrap())).unwrap();
    graph
        .checkpoint(CheckpointRequest {
            name: "BeforeRecords".into(),
            description: None,
            idempotency_key: operation(3_000),
            actor_uuid: None,
        })
        .unwrap();
    graph
        .execute("CREATE (:Person), (:Person), (:Person)")
        .unwrap();
    graph
        .checkpoint(CheckpointRequest {
            name: "AfterRecords".into(),
            description: None,
            idempotency_key: operation(3_001),
            actor_uuid: None,
        })
        .unwrap();

    let request = |page| DiffCheckpointsRequest {
        from: CheckpointSelector::Named("BeforeRecords".into()),
        to: CheckpointSelector::Named("AfterRecords".into()),
        scope: CheckpointDiffScope::Graph,
        detail: CheckpointDiffDetail::Records,
        page,
    };
    let first = graph
        .diff_checkpoints(request(PageRequest {
            limit: 1,
            after: None,
            cancellation: None,
        }))
        .unwrap();
    assert_eq!(first.stats.rows_produced, 1);
    let token =
        PageToken::parse(first.schema.metadata()["graphforge.next_page_token"].as_str()).unwrap();
    let second = graph
        .diff_checkpoints(request(PageRequest {
            limit: 1,
            after: Some(token),
            cancellation: None,
        }))
        .unwrap();
    assert_eq!(second.stats.rows_produced, 1);
    let token =
        PageToken::parse(second.schema.metadata()["graphforge.next_page_token"].as_str()).unwrap();
    let third = graph
        .diff_checkpoints(request(PageRequest {
            limit: 1,
            after: Some(token),
            cancellation: None,
        }))
        .unwrap();
    assert_eq!(third.stats.rows_produced, 1);
    let first_ids = first.batches[0]
        .column_by_name("record_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let second_ids = second.batches[0]
        .column_by_name("record_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let third_ids = third.batches[0]
        .column_by_name("record_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert!(first_ids.value(0) < second_ids.value(0));
    assert!(second_ids.value(0) < third_ids.value(0));

    let cancelled = crate::CancellationToken::new();
    cancelled.cancel();
    assert_eq!(
        graph
            .diff_checkpoints(request(PageRequest {
                limit: 1,
                after: None,
                cancellation: Some(cancelled),
            }))
            .unwrap_err()
            .code(),
        "GF_CANCELLED"
    );
    for limit in [0, 10_001] {
        let error = graph
            .diff_checkpoints(request(PageRequest {
                limit,
                after: None,
                cancellation: None,
            }))
            .unwrap_err();
        assert_eq!(error.code(), "GF_VALIDATION");
        assert!(error.to_string().contains("1..=10000"));
    }
}
