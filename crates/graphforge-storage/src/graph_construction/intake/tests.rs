use super::super::tests::{edge_batch, fixed, node_batch, open};
use super::super::*;
use super::*;
use arrow::array::{BinaryArray, Int64Array, StringArray};
use std::sync::Arc;
use tempfile::TempDir;

#[test]
fn row_artifact_retains_dynamic_properties_and_is_uuid_sorted() {
    let root = TempDir::new().unwrap();
    let mut session = open(&root, 99);
    let schema = Arc::new(Schema::new(vec![
        Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("label", DataType::Utf8, false),
        Field::new("score", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(fixed(&[3_u128.to_be_bytes(), 1_u128.to_be_bytes()])),
            Arc::new(StringArray::from(vec!["Person", "Person"])),
            Arc::new(Int64Array::from(vec![30, 10])),
        ],
    )
    .unwrap();
    let receipt = session
        .append(ConstructionChunkKind::Node, "properties", &batch)
        .unwrap();
    let file = session
        .root
        .open_child_file(OsStr::new(&receipt.parquet.name))
        .unwrap();
    let batches = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let uuids = uuid_column(&batches[0], "node_uuid").unwrap();
    let scores = batches[0]
        .column_by_name("score")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(uuid_value(uuids, 0).unwrap(), 1_u128.to_be_bytes());
    assert_eq!(scores.values(), &[10, 30]);
    drop(session);

    let resumed = open(&root, 99);
    assert!(resumed.checkpoint.session_now_micros > 0);
    assert_eq!(
        resumed.read_receipt(0).unwrap().schema_sha256,
        receipt.schema_sha256
    );
}

#[test]
fn replay_binds_each_property_schema_and_rejects_unsupported_staging_types() {
    let root = TempDir::new().unwrap();
    let mut session = open(&root, 98);
    let batch_with = |property: &str| {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
                Field::new("label", DataType::Utf8, false),
                Field::new(property, DataType::Int64, false),
            ])),
            vec![
                Arc::new(fixed(&[1_u128.to_be_bytes()])),
                Arc::new(StringArray::from(vec!["Person"])),
                Arc::new(Int64Array::from(vec![7])),
            ],
        )
        .unwrap()
    };
    session
        .append(
            ConstructionChunkKind::Node,
            "schema-bound",
            &batch_with("score"),
        )
        .unwrap();
    assert!(
        session
            .append(
                ConstructionChunkKind::Node,
                "schema-bound",
                &batch_with("renamed")
            )
            .unwrap_err()
            .to_string()
            .contains("conflicting")
    );
    let heterogeneous = session
        .append(
            ConstructionChunkKind::Node,
            "different-chunk-schema",
            &batch_with("renamed"),
        )
        .unwrap();
    assert_ne!(
        heterogeneous.schema_sha256,
        session.read_receipt(0).unwrap().schema_sha256
    );

    let unsupported = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("label", DataType::Utf8, false),
            Field::new("payload", DataType::Binary, false),
        ])),
        vec![
            Arc::new(fixed(&[2_u128.to_be_bytes()])),
            Arc::new(StringArray::from(vec!["Person"])),
            Arc::new(BinaryArray::from(vec![b"bytes".as_slice()])),
        ],
    )
    .unwrap();
    assert!(
        session
            .append(ConstructionChunkKind::Node, "unsupported", &unsupported)
            .unwrap_err()
            .to_string()
            .contains("unsupported")
    );
}

#[test]
fn journal_is_constant_control_state_and_seal_reopens_every_artifact() {
    for chunks in [1_u64, 2, 4] {
        let root = TempDir::new().unwrap();
        let operation = 100 + u128::from(chunks);
        let mut session = open(&root, operation);
        for chunk in 0..chunks {
            session
                .append(
                    ConstructionChunkKind::Node,
                    &format!("n-{chunk}"),
                    &node_batch(1 + u128::from(chunk) * 32, 32),
                )
                .unwrap();
        }
        assert_eq!(session.accepted_chunks(), chunks);
        assert_eq!(session.evidence().input_rows, chunks * 32);
        assert_eq!(session.evidence().peak_batch_rows, 32);
        assert_eq!(session.evidence().peak_run_records, 64);
        assert_eq!(session.evidence().prior_topology_rows_decoded, 0);
        assert_eq!(session.evidence().current_transitions, 0);
        assert!(session.evidence().write_operations < session.evidence().input_rows);
        assert!(session.evidence().fsync_operations > 0);
        assert!(session.evidence().peak_accounted_live_bytes > 0);
        let staging_storage =
            &session.evidence().storage_current[&crate::ArtifactCategory::ConstructionStaging];
        assert_eq!(staging_storage.logical_references, chunks * 3);
        assert_eq!(staging_storage.physical_objects, chunks * 3);
        assert_eq!(
            session.evidence().storage_transient_peak_allocated_bytes
                [&crate::ArtifactCategory::ConstructionStaging],
            staging_storage.allocated_bytes
        );
        assert_eq!(
            session
                .evidence()
                .storage_transient_peak_total_allocated_bytes,
            session
                .evidence()
                .storage_current
                .values()
                .map(|totals| totals.allocated_bytes)
                .sum::<u64>()
        );
        let persisted_storage = session.evidence().storage_current.clone();
        let checkpoint_bytes = session
            .root
            .open_child_file(OsStr::new(CHECKPOINT))
            .unwrap()
            .metadata()
            .unwrap()
            .len();
        assert!(checkpoint_bytes < MAX_CONTROL_BYTES);
        drop(session);
        let mut session = GraphConstructionSession::resume_with_mode_and_lifecycle(
            root.path(),
            Uuid::from_u128(operation),
            graphforge_core::OntologyMode::Exploratory,
            GraphConstructionBudgets::default(),
            crate::filesystem_admission::ProjectLifecycleMode::Durable,
        )
        .unwrap();
        assert_eq!(session.evidence().storage_current, persisted_storage);
        session.seal().unwrap();
        assert_eq!(session.state(), GraphConstructionState::Sealed);
        assert!(session.evidence().authentication_read_bytes > 0);
        let sealed_staging =
            &session.evidence().storage_current[&crate::ArtifactCategory::ConstructionStaging];
        let sealed_staging_peak = session.evidence().storage_transient_peak_allocated_bytes
            [&crate::ArtifactCategory::ConstructionStaging];
        assert!(
            sealed_staging_peak >= sealed_staging.allocated_bytes,
            "encoded artifacts advanced current staging allocation without its category peak"
        );
        let sealed_current = session.evidence().storage_current.clone();
        let sealed_peaks = session
            .evidence()
            .storage_transient_peak_allocated_bytes
            .clone();
        drop(session);
        let resumed = GraphConstructionSession::resume_with_mode_and_lifecycle(
            root.path(),
            Uuid::from_u128(operation),
            graphforge_core::OntologyMode::Exploratory,
            GraphConstructionBudgets::default(),
            crate::filesystem_admission::ProjectLifecycleMode::Durable,
        )
        .unwrap();
        assert_eq!(resumed.evidence().storage_current, sealed_current);
        assert_eq!(
            resumed.evidence().storage_transient_peak_allocated_bytes,
            sealed_peaks
        );
    }
}

/// Lowers this thread's checkpoint write bound until dropped.
struct CheckpointLimit;

impl CheckpointLimit {
    fn set(limit: u64) -> Self {
        super::super::controls::CHECKPOINT_LIMIT_OVERRIDE.with(|cell| cell.set(Some(limit)));
        Self
    }
}

impl Drop for CheckpointLimit {
    fn drop(&mut self) {
        super::super::controls::CHECKPOINT_LIMIT_OVERRIDE.with(|cell| cell.set(None));
    }
}

type CategoryTotals =
    std::collections::BTreeMap<crate::ArtifactCategory, crate::ArtifactStorageTotals>;
type CategoryPeaks = std::collections::BTreeMap<crate::ArtifactCategory, u64>;

/// Ledger, its authority hash, current and authority category totals,
/// recorded and authority peaks, and the total peak.
type AllocationEvidence = (
    std::collections::BTreeMap<String, u64>,
    String,
    CategoryTotals,
    CategoryTotals,
    CategoryPeaks,
    CategoryPeaks,
    u64,
);

/// The allocation evidence a reopen must reproduce exactly.
fn allocation_evidence(evidence: &GraphConstructionEvidence) -> AllocationEvidence {
    (
        evidence.storage_active_identity_allocated_bytes.clone(),
        crate::storage_attribution::identity_map_authority_sha256(
            &evidence.storage_active_identity_allocated_bytes,
        ),
        evidence.storage_current.clone(),
        // Category totals equal the identity union (the category==identity
        // invariant), or this refuses.
        evidence.storage_category_authorities().unwrap(),
        evidence.storage_transient_peak_allocated_bytes.clone(),
        evidence.storage_transient_peak_authorities().unwrap(),
        evidence.storage_transient_peak_total_allocated_bytes,
    )
}

/// #900. The staging checkpoint is rewritten on every accepted chunk, so its
/// size must not depend on how many chunks have been accepted.
///
/// S25 failed staging after 4,459 chunks because the checkpoint persisted one
/// allocation-ledger entry per staged artifact: 17,324 of them, 1,039,441 of
/// its 1,048,477 bytes, past the 1 MiB control bound, with the whole record
/// rewritten per chunk. The invariant is asserted as a slope, as #1526 did for
/// the shape end, because a threshold passes on any tree that merely still has
/// margin. The write bound is lowered to 2 KiB above the one-chunk checkpoint
/// only so the old record, which grew by three entries per node chunk,
/// provably crosses it inside this fixture.
#[test]
fn staging_checkpoint_is_independent_of_accepted_chunk_count() {
    let root = TempDir::new().unwrap();
    let operation = 900_u128;
    let mut session = open(&root, operation);
    let checkpoint_path = session.root.path().join(CHECKPOINT);
    let persisted = || -> serde_json::Value {
        serde_json::from_slice(&std::fs::read(&checkpoint_path).unwrap()).unwrap()
    };
    let append = |session: &mut GraphConstructionSession, chunk: u64| {
        session
            .append(
                ConstructionChunkKind::Node,
                &format!("n-{chunk}"),
                &node_batch(1 + u128::from(chunk) * 2, 2),
            )
            .unwrap();
    };
    // The first write fills the per-category maps the initial record lacks;
    // the bound is measured from the record that has them.
    append(&mut session, 0);
    let bound = std::fs::metadata(&checkpoint_path).unwrap().len() + 2048;
    let limit = CheckpointLimit::set(bound);
    let mut sizes = std::collections::BTreeMap::new();
    for chunk in 1..48_u64 {
        append(&mut session, chunk);
        let accepted = chunk + 1;
        if accepted == 16 || accepted == 48 {
            // Every staged entry is omitted, from the first chunk on.
            let control = persisted();
            assert_eq!(
                control["evidence"]["storage_active_identity_allocated_bytes"],
                serde_json::json!({}),
                "{accepted} chunks"
            );
            assert_eq!(
                control["staged_ledger_from_sequence"],
                serde_json::json!(0),
                "{accepted} chunks"
            );
            sizes.insert(accepted, std::fs::metadata(&checkpoint_path).unwrap().len());
        }
    }
    let ledger = &session.evidence().storage_active_identity_allocated_bytes;
    assert_eq!(ledger.len(), 48 * 3);
    // The positive control: the record the old writer produced from this same
    // state — the whole ledger, without transition history — is over the
    // bound this run stayed under.
    let mut old_record = session.checkpoint.clone();
    old_record.evidence.storage_allocation_transitions.clear();
    let old_bytes = serde_json::to_vec(&old_record).unwrap().len() as u64;
    assert!(
        old_bytes > bound,
        "the fixture must stage past the old record's bound: {old_bytes} <= {bound}"
    );
    // The slope: 32 more chunks, 96 more staged artifacts, and the record
    // grows by less than one ledger entry (only counter digits change).
    let smallest_entry = ledger
        .iter()
        .map(|(key, allocated)| key.len() + allocated.to_string().len() + 4)
        .min()
        .unwrap() as u64;
    let growth = sizes[&48] - sizes[&16];
    assert!(
        growth < smallest_entry,
        "staging checkpoint grew {growth} bytes over 32 chunks ({sizes:?})"
    );

    // Reopening restores exactly the ledger the writer held, from the receipt
    // journal, and keeps every allocation authority.
    let before = allocation_evidence(session.evidence());
    drop(session);
    let mut session = open(&root, operation);
    assert_eq!(allocation_evidence(session.evidence()), before);
    // The restored index keeps later writes bounded too.
    session
        .append(ConstructionChunkKind::Node, "n-48", &node_batch(97, 2))
        .unwrap();
    session.seal().unwrap();
    assert_eq!(
        persisted()["evidence"]["storage_active_identity_allocated_bytes"],
        serde_json::json!({})
    );
    let sealed = allocation_evidence(session.evidence());
    drop(session);
    let mut session = open(&root, operation);
    assert_eq!(allocation_evidence(session.evidence()), sealed);

    // Once supersession retires the staged inputs nothing is omitted: the
    // persisted ledger is the whole ledger, as before #900.
    drop(limit);
    session.shape_canonical_with_cancellation(|| false).unwrap();
    assert!(session.checkpoint.inputs_retired);
    let control = persisted();
    assert!(control.get("staged_ledger_from_sequence").is_none());
    let shaped = allocation_evidence(session.evidence());
    assert_eq!(
        serde_json::from_value::<std::collections::BTreeMap<String, u64>>(
            control["evidence"]["storage_active_identity_allocated_bytes"].clone()
        )
        .unwrap(),
        shaped.0
    );
    drop(session);
    let session = open(&root, operation);
    assert_eq!(allocation_evidence(session.evidence()), shaped);
}

#[test]
fn schema_group_admission_is_constant_and_budgeted() {
    let root = TempDir::new().unwrap();
    let mut session = GraphConstructionSession::open(
        root.path(),
        Uuid::from_u128(6_999),
        0,
        GraphConstructionBudgets {
            max_schema_groups: 1,
            ..GraphConstructionBudgets::default()
        },
    )
    .unwrap();
    session
        .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 2))
        .unwrap();
    let error = session
        .append(
            ConstructionChunkKind::Edge,
            "edges",
            &edge_batch(100, 1, 2, 1),
        )
        .unwrap_err();
    assert!(error.to_string().contains("schema-group budget"));
    assert_eq!(session.checkpoint.node_schema_sha256.len(), 1);
    assert!(session.checkpoint.edge_schema_sha256.is_empty());
}

#[test]
fn logical_digest_is_independent_of_arrow_slice_layout() {
    let whole = node_batch(10, 6);
    let sliced = whole.slice(2, 3);
    let rebuilt = node_batch(12, 3);
    assert_eq!(
        logical_batch_digest(ConstructionChunkKind::Node, &sliced).unwrap(),
        logical_batch_digest(ConstructionChunkKind::Node, &rebuilt).unwrap()
    );
}

#[test]
fn replay_is_idempotent_and_reauthenticates_artifacts() {
    let root = TempDir::new().unwrap();
    let mut session = open(&root, 200);
    let batch = node_batch(1, 8);
    let first = session
        .append(ConstructionChunkKind::Node, "nodes", &batch)
        .unwrap();
    assert_eq!(
        session
            .append(ConstructionChunkKind::Node, "nodes", &batch)
            .unwrap(),
        first
    );
    assert_eq!(session.accepted_chunks(), 1);
    assert_eq!(session.evidence().replayed_chunks, 1);
    assert!(session.evidence().replay_validation_read_bytes > 0);
    assert!(session.evidence().replay_validation_read_operations > 0);
    assert!(
        session
            .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 9))
            .is_err()
    );
}

#[test]
fn cancellation_recovers_private_intent_without_accepting_a_chunk() {
    let root = TempDir::new().unwrap();
    let operation = Uuid::from_u128(300);
    let mut session = open(&root, 300);
    let mut polls = 0_u8;
    assert!(
        session
            .append_with_cancellation(
                ConstructionChunkKind::Node,
                "nodes",
                &node_batch(1, 8),
                || {
                    polls = polls.checked_add(1).expect("cancellation poll overflow");
                    polls == 2
                },
            )
            .is_err()
    );
    drop(session);
    let resumed = GraphConstructionSession::open(
        root.path(),
        operation,
        0,
        GraphConstructionBudgets::default(),
    )
    .unwrap();
    assert_eq!(resumed.accepted_chunks(), 0);
    assert!(resumed.root.open_child_file(OsStr::new(INTENT)).is_err());
}

#[test]
fn non_canonical_property_columns_are_refused_before_staging() {
    let root = TempDir::new().unwrap();
    let mut session = open(&root, 97);
    let item = |data_type: DataType| Arc::new(Field::new("item", data_type, true));
    let scalars = [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::Float32,
        DataType::LargeUtf8,
    ];
    let mut non_canonical = Vec::new();
    for scalar in scalars {
        non_canonical.push(DataType::List(item(scalar.clone())));
        non_canonical.push(DataType::LargeList(item(scalar.clone())));
        non_canonical.push(scalar);
    }
    non_canonical.push(DataType::LargeList(item(DataType::Int64)));
    let mut values = arrow::array::ListBuilder::new(arrow::array::Int64Builder::new());
    values.append_value([Some(1), None]);
    let list: arrow::array::ArrayRef = Arc::new(values.finish());
    let scalar: arrow::array::ArrayRef = Arc::new(Int64Array::from(vec![Some(1)]));
    for data_type in non_canonical {
        let source = if matches!(data_type, DataType::List(_) | DataType::LargeList(_)) {
            &list
        } else {
            &scalar
        };
        let column = arrow::compute::cast(source, &data_type).unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
                Field::new("label", DataType::Utf8, false),
                Field::new("narrow", data_type.clone(), true),
            ])),
            vec![
                Arc::new(fixed(&[1_u128.to_be_bytes()])),
                Arc::new(StringArray::from(vec!["Person"])),
                column,
            ],
        )
        .unwrap();
        let error = session
            .append(ConstructionChunkKind::Node, "narrow", &batch)
            .unwrap_err();
        assert!(
            matches!(
                &error,
                GfError::Api {
                    code: graphforge_core::ApiErrorCode::SchemaMismatch,
                    message,
                } if message.contains("narrow") && message.contains("non-canonical")
            ),
            "{data_type}: {error:?}"
        );
        assert_eq!(session.accepted_chunks(), 0, "{data_type}");
    }
    assert!(
        root.path()
            .read_dir()
            .unwrap()
            .flatten()
            .all(|entry| !entry.file_name().to_string_lossy().ends_with(".parquet")),
        "a refused chunk must not stage a Parquet artifact"
    );
    let receipt = session
        .append(ConstructionChunkKind::Node, "canonical", &node_batch(1, 1))
        .unwrap();
    assert_eq!(receipt.rows, 1);
    assert_eq!(session.accepted_chunks(), 1);
}
