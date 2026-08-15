//! Integration tests for the authoritative graph delta journal (#752).

use std::fs;

use graphforge_storage::{
    GRAPH_CAPABILITY_ID, GRAPH_CAPABILITY_VERSION, GraphDeltaJournalLimits, GraphDeltaOp,
    GraphDeltaOpKind, GraphDeltaPayload, GraphDeltaPublishRequest, GraphFileEntry, GraphFileRole,
    GraphFilesInventory, ProjectCapability, ProjectGenerationRequest, ProjectStageOutcome,
    ReconstructedGraphState, capture_graph_files, decode_delta_run, delta_run_relative_path,
    empty_workspace_participants, encode_delta_run, list_delta_runs, open_or_initialize_project,
    publish_graph_delta, reconstruct_graph_state, resolve_project_generation,
    stage_base_graph_workspace, stage_project_generation_with_graph_tree,
};
use uuid::Uuid;

fn sample_ops() -> Vec<GraphDeltaOp> {
    let edge = Uuid::now_v7();
    let src = Uuid::now_v7();
    let dst = Uuid::now_v7();
    vec![
        GraphDeltaOp {
            operation_uuid: Uuid::now_v7(),
            kind: GraphDeltaOpKind::UpsertNode,
            payload: GraphDeltaPayload::UpsertNode {
                node_uuid: src.hyphenated().to_string(),
                type_ids: vec![1],
            },
        },
        GraphDeltaOp {
            operation_uuid: Uuid::now_v7(),
            kind: GraphDeltaOpKind::UpsertNode,
            payload: GraphDeltaPayload::UpsertNode {
                node_uuid: dst.hyphenated().to_string(),
                type_ids: vec![1],
            },
        },
        GraphDeltaOp {
            operation_uuid: Uuid::now_v7(),
            kind: GraphDeltaOpKind::UpsertEdge,
            payload: GraphDeltaPayload::UpsertEdge {
                edge_uuid: edge.hyphenated().to_string(),
                src_uuid: src.hyphenated().to_string(),
                dst_uuid: dst.hyphenated().to_string(),
                rel_type: "KNOWS".into(),
            },
        },
    ]
}

fn publish_base(container: &std::path::Path, parquet_a: &[u8], parquet_b: &[u8]) -> Uuid {
    open_or_initialize_project(container).unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let mut base = ReconstructedGraphState::default();
    base.nodes
        .insert("00000000-0000-7000-8000-000000000001".into(), vec![1]);
    stage_base_graph_workspace(
        workspace.path(),
        &[
            ("topology/nodes.parquet", parquet_a),
            ("topology/edges/KNOWS.parquet", parquet_b),
        ],
        Some(&base),
    )
    .unwrap();
    let (_, files) = capture_graph_files(workspace.path()).unwrap();
    let mut participants = empty_workspace_participants().unwrap();
    participants.insert(0, files);
    let generation_uuid = Uuid::now_v7();
    let request = ProjectGenerationRequest {
        transaction_uuid: Uuid::now_v7(),
        generation_uuid,
        capabilities: vec![
            ProjectCapability {
                capability_id: GRAPH_CAPABILITY_ID.into(),
                capability_version: GRAPH_CAPABILITY_VERSION,
            },
            ProjectCapability {
                capability_id: "workspace".into(),
                capability_version: 1,
            },
        ],
        participants,
    };
    let ProjectStageOutcome::Staged(staged) =
        stage_project_generation_with_graph_tree(container, &request, Some(workspace.path()))
            .unwrap()
    else {
        panic!("base publication replayed");
    };
    staged
        .validate(|_| Ok(()), |_, _| Ok(()))
        .unwrap()
        .publish()
        .unwrap();
    generation_uuid
}

#[test]
fn encode_decode_round_trip_and_golden_magic() {
    let ops = sample_ops();
    let bytes = encode_delta_run(
        1,
        Uuid::now_v7(),
        Uuid::now_v7(),
        &ops,
        GraphDeltaJournalLimits::default(),
    )
    .unwrap();
    assert_eq!(&bytes[..4], b"GFDR");
    let decoded = decode_delta_run(&bytes, Some(1), GraphDeltaJournalLimits::default()).unwrap();
    assert_eq!(decoded.records.len(), ops.len());
    assert_eq!(decoded.records[0].operation_uuid, ops[0].operation_uuid);
}

#[test]
fn torn_truncated_reordered_and_checksum_invalid_fail_closed() {
    let ops = sample_ops();
    let mut bytes = encode_delta_run(
        1,
        Uuid::now_v7(),
        Uuid::now_v7(),
        &ops,
        GraphDeltaJournalLimits::default(),
    )
    .unwrap();
    let truncated = &bytes[..bytes.len() / 2];
    assert_eq!(
        decode_delta_run(truncated, Some(1), GraphDeltaJournalLimits::default())
            .unwrap_err()
            .code(),
        "GF_PROJECT_CORRUPT"
    );
    let mut corrupted = bytes.clone();
    let flip = corrupted.len() / 3;
    corrupted[flip] ^= 0xff;
    assert_eq!(
        decode_delta_run(&corrupted, Some(1), GraphDeltaJournalLimits::default())
            .unwrap_err()
            .code(),
        "GF_PROJECT_CORRUPT"
    );
    bytes[8..16].copy_from_slice(&2u64.to_le_bytes());
    assert!(decode_delta_run(&bytes, Some(1), GraphDeltaJournalLimits::default()).is_err());
}

#[test]
fn small_write_preserves_unchanged_parquet_and_reopen_replays() {
    let root = tempfile::tempdir().unwrap();
    let _base = publish_base(root.path(), b"NODES-PARQUET-V1", b"EDGES-PARQUET-V1");
    let parent = resolve_project_generation(root.path()).unwrap();
    let parent_inventory = parent.graph_files_inventory().unwrap().unwrap();
    let parent_nodes_digest = parent_inventory
        .files
        .iter()
        .find(|entry| entry.relative_path == "topology/nodes.parquet")
        .unwrap()
        .content_sha256
        .clone();

    let ops = sample_ops();
    let edge_uuid = match &ops[2].payload {
        GraphDeltaPayload::UpsertEdge { edge_uuid, .. } => edge_uuid.clone(),
        _ => unreachable!(),
    };
    let receipt = publish_graph_delta(
        root.path(),
        &GraphDeltaPublishRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
            run_uuid: Uuid::now_v7(),
            operations: ops,
            limits: GraphDeltaJournalLimits::default(),
        },
    )
    .unwrap();
    assert!(receipt.preserved_base_parquet_digests);
    assert!(receipt.unchanged_base_files >= 2);

    let reopened = resolve_project_generation(root.path()).unwrap();
    let inventory = reopened.graph_files_inventory().unwrap().unwrap();
    let child_nodes_digest = inventory
        .files
        .iter()
        .find(|entry| entry.relative_path == "topology/nodes.parquet")
        .unwrap()
        .content_sha256
        .clone();
    assert_eq!(parent_nodes_digest, child_nodes_digest);

    let (state, evidence) = reconstruct_graph_state(
        &reopened.graph_tree_root(),
        &inventory,
        GraphDeltaJournalLimits::default(),
    )
    .unwrap();
    assert_eq!(evidence.runs_replayed, 1);
    assert!(state.edges.contains_key(&edge_uuid));
    assert_eq!(state.fingerprint(), receipt.state_fingerprint);
}

#[test]
fn exact_retry_transaction_is_idempotent_and_conflict_is_typed() {
    let root = tempfile::tempdir().unwrap();
    publish_base(root.path(), b"N", b"E");
    let ops = sample_ops();
    let transaction_uuid = Uuid::now_v7();
    let generation_uuid = Uuid::now_v7();
    let run_uuid = Uuid::now_v7();
    let request = GraphDeltaPublishRequest {
        transaction_uuid,
        generation_uuid,
        run_uuid,
        operations: ops.clone(),
        limits: GraphDeltaJournalLimits::default(),
    };
    let first = publish_graph_delta(root.path(), &request).unwrap();
    assert!(!first.publication.idempotent_replay);
    let second = publish_graph_delta(root.path(), &request).unwrap();
    assert!(second.publication.idempotent_replay);
    assert_eq!(
        first.publication.generation_uuid,
        second.publication.generation_uuid
    );

    let mut conflicting = ops;
    if let GraphDeltaPayload::UpsertEdge { rel_type, .. } = &mut conflicting[2].payload {
        *rel_type = "OTHER".into();
    }
    let err = publish_graph_delta(
        root.path(),
        &GraphDeltaPublishRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
            run_uuid: Uuid::now_v7(),
            operations: conflicting,
            limits: GraphDeltaJournalLimits::default(),
        },
    )
    .unwrap_err();
    assert_eq!(err.code(), "GF_IDEMPOTENCY_CONFLICT");
}

#[test]
fn missing_run_referenced_by_inventory_fails_closed() {
    let root = tempfile::tempdir().unwrap();
    publish_base(root.path(), b"N", b"E");
    publish_graph_delta(
        root.path(),
        &GraphDeltaPublishRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
            run_uuid: Uuid::now_v7(),
            operations: sample_ops(),
            limits: GraphDeltaJournalLimits::default(),
        },
    )
    .unwrap();
    let resolved = resolve_project_generation(root.path()).unwrap();
    let inventory = resolved.graph_files_inventory().unwrap().unwrap();
    let run_path = resolved.graph_tree_root().join(delta_run_relative_path(1));
    fs::remove_file(&run_path).unwrap();
    let err = reconstruct_graph_state(
        &resolved.graph_tree_root(),
        &inventory,
        GraphDeltaJournalLimits::default(),
    )
    .unwrap_err();
    assert_eq!(err.code(), "GF_PROJECT_CORRUPT");
}

#[test]
fn resource_limits_reject_tiny_run_accumulation() {
    let limits = GraphDeltaJournalLimits {
        max_runs: 1,
        ..GraphDeltaJournalLimits::default()
    };
    let root = tempfile::tempdir().unwrap();
    publish_base(root.path(), b"N", b"E");
    publish_graph_delta(
        root.path(),
        &GraphDeltaPublishRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
            run_uuid: Uuid::now_v7(),
            operations: sample_ops(),
            limits,
        },
    )
    .unwrap();
    let err = publish_graph_delta(
        root.path(),
        &GraphDeltaPublishRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
            run_uuid: Uuid::now_v7(),
            operations: sample_ops(),
            limits,
        },
    )
    .unwrap_err();
    assert!(err.to_string().contains("GF_RESOURCE_LIMIT"));
}

#[test]
fn legacy_v1_project_without_deltas_remains_readable() {
    let root = tempfile::tempdir().unwrap();
    publish_base(root.path(), b"LEGACY", b"BASE");
    let resolved = resolve_project_generation(root.path()).unwrap();
    let inventory = resolved.graph_files_inventory().unwrap().unwrap();
    assert!(
        list_delta_runs(&inventory, GraphDeltaJournalLimits::default())
            .unwrap()
            .is_empty()
    );
    let (state, evidence) = reconstruct_graph_state(
        &resolved.graph_tree_root(),
        &inventory,
        GraphDeltaJournalLimits::default(),
    )
    .unwrap();
    assert_eq!(evidence.runs_replayed, 0);
    assert!(
        state
            .nodes
            .contains_key("00000000-0000-7000-8000-000000000001")
    );
}

#[test]
fn gapped_run_sequence_in_inventory_fails_closed() {
    let inventory = GraphFilesInventory {
        format: "graphforge-graph-files".into(),
        format_version: 1,
        files: vec![
            GraphFileEntry {
                relative_path: delta_run_relative_path(1),
                byte_length: 1,
                content_sha256: "a".repeat(64),
                role: GraphFileRole::Delta,
            },
            GraphFileEntry {
                relative_path: delta_run_relative_path(3),
                byte_length: 2,
                content_sha256: "b".repeat(64),
                role: GraphFileRole::Delta,
            },
        ],
        file_count: 2,
        total_byte_length: 3,
    };
    let err = list_delta_runs(&inventory, GraphDeltaJournalLimits::default()).unwrap_err();
    assert_eq!(err.code(), "GF_PROJECT_CORRUPT");
}
