//! Integration tests for graph delta → Parquet compaction (#753).

use std::sync::atomic::AtomicBool;

use graphforge_core::{OntologyMode, TypeId};
use graphforge_storage::{
    CheckpointCreateRequest, GRAPH_CAPABILITY_ID, GRAPH_CAPABILITY_VERSION,
    GraphDeltaCompactionLimits, GraphDeltaCompactionPolicy, GraphDeltaCompactionRequest,
    GraphDeltaJournalLimits, GraphDeltaOp, GraphDeltaOpKind, GraphDeltaPayload,
    GraphDeltaPublishRequest, GraphWriter, ProjectCapability, ProjectGenerationRequest,
    ProjectRetentionLimits, ProjectRetentionPolicy, ProjectStageOutcome, capture_graph_files,
    compact_graph_delta, create_checkpoint, empty_workspace_participants, execute_project_cleanup,
    graph_delta_compaction_status, list_delta_runs, open_or_initialize_project,
    preview_graph_delta_compaction, preview_project_cleanup, publish_graph_delta,
    reconstruct_graph_state, resolve_project_generation, stage_project_generation_with_graph_tree,
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
            payload: GraphDeltaPayload::UpsertNodeV2 {
                node_uuid: src.hyphenated().to_string(),
                node_id: 1,
                type_ids: vec![1],
                created_at_micros: 1,
                updated_at_micros: 1,
            },
        },
        GraphDeltaOp {
            operation_uuid: Uuid::now_v7(),
            kind: GraphDeltaOpKind::UpsertNode,
            payload: GraphDeltaPayload::UpsertNodeV2 {
                node_uuid: dst.hyphenated().to_string(),
                node_id: 2,
                type_ids: vec![1],
                created_at_micros: 2,
                updated_at_micros: 2,
            },
        },
        GraphDeltaOp {
            operation_uuid: Uuid::now_v7(),
            kind: GraphDeltaOpKind::UpsertEdge,
            payload: GraphDeltaPayload::UpsertEdgeV2 {
                edge_uuid: edge.hyphenated().to_string(),
                src_uuid: src.hyphenated().to_string(),
                dst_uuid: dst.hyphenated().to_string(),
                rel_type: "KNOWS".into(),
                edge_id: 1,
                src_id: 1,
                dst_id: 2,
                created_at_micros: 3,
            },
        },
    ]
}

fn publish_base(container: &std::path::Path) -> Uuid {
    open_or_initialize_project(container).unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let mut writer = GraphWriter::open_at(
        workspace.path(),
        OntologyMode::Strict,
        1_700_000_000_000_000,
    )
    .unwrap();
    writer
        .create_node(
            Uuid::parse_str("00000000-0000-7000-8000-000000000001").unwrap(),
            TypeId(1),
        )
        .unwrap();
    writer.flush().unwrap();
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

fn publish_delta(container: &std::path::Path, ops: Vec<GraphDeltaOp>) -> [u8; 32] {
    let receipt = publish_graph_delta(
        container,
        &GraphDeltaPublishRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
            run_uuid: Uuid::now_v7(),
            operations: ops,
            limits: GraphDeltaJournalLimits::default(),
        },
    )
    .unwrap();
    receipt.state_fingerprint
}

fn default_request() -> GraphDeltaCompactionRequest {
    GraphDeltaCompactionRequest {
        transaction_uuid: Uuid::now_v7(),
        generation_uuid: Uuid::now_v7(),
        through_run_sequence: None,
        limits: GraphDeltaCompactionLimits::default(),
        cleanup_after_commit: false,
        cleanup_policy: ProjectRetentionPolicy {
            retained_ancestors: 0,
        },
        cleanup_limits: ProjectRetentionLimits::default(),
    }
}

#[test]
fn compacted_parquet_matches_base_plus_deltas_fingerprint() {
    let root = tempfile::tempdir().unwrap();
    publish_base(root.path());
    let before = publish_delta(root.path(), sample_ops());
    let before2 = publish_delta(root.path(), sample_ops());
    assert_ne!(before, before2);

    let resolved = resolve_project_generation(root.path()).unwrap();
    let inventory = resolved.graph_files_inventory().unwrap().unwrap();
    let (pre_state, pre_evidence) = reconstruct_graph_state(
        &resolved.graph_tree_root(),
        &inventory,
        GraphDeltaJournalLimits::default(),
    )
    .unwrap();
    assert_eq!(pre_evidence.runs_replayed, 2);
    let expected = pre_state.fingerprint();

    let report = compact_graph_delta(root.path(), &default_request(), None).unwrap();
    assert!(!report.dry_run);
    assert_eq!(report.compacted_runs, 2);
    assert_eq!(report.retained_suffix_runs, 0);
    assert_eq!(report.state_fingerprint, expected);
    assert!(report.input_rows >= 6);
    assert!(report.output_bytes > 0);
    assert!(report.elapsed_ms < u64::MAX);

    let reopened = resolve_project_generation(root.path()).unwrap();
    let inventory = reopened.graph_files_inventory().unwrap().unwrap();
    assert!(
        list_delta_runs(&inventory, GraphDeltaJournalLimits::default())
            .unwrap()
            .is_empty()
    );
    let (post_state, post_evidence) = reconstruct_graph_state(
        &reopened.graph_tree_root(),
        &inventory,
        GraphDeltaJournalLimits::default(),
    )
    .unwrap();
    assert_eq!(post_evidence.runs_replayed, 0);
    assert_eq!(post_state.fingerprint(), expected);
    assert_eq!(post_state.nodes, pre_state.nodes);
    assert_eq!(post_state.edges, pre_state.edges);
    assert_eq!(post_state.node_properties, pre_state.node_properties);
    assert_eq!(post_state.edge_properties, pre_state.edge_properties);
}

#[test]
fn concurrent_suffix_runs_survive_prefix_compaction() {
    let root = tempfile::tempdir().unwrap();
    publish_base(root.path());
    publish_delta(root.path(), sample_ops());
    publish_delta(root.path(), sample_ops());
    let third = publish_delta(root.path(), sample_ops());

    let resolved = resolve_project_generation(root.path()).unwrap();
    let inventory = resolved.graph_files_inventory().unwrap().unwrap();
    let (pre_state, _) = reconstruct_graph_state(
        &resolved.graph_tree_root(),
        &inventory,
        GraphDeltaJournalLimits::default(),
    )
    .unwrap();
    assert_eq!(pre_state.fingerprint(), third);

    let mut request = default_request();
    request.through_run_sequence = Some(2);
    let report = compact_graph_delta(root.path(), &request, None).unwrap();
    assert_eq!(report.compacted_runs, 2);
    assert_eq!(report.retained_suffix_runs, 1);
    assert_eq!(report.state_fingerprint, third);

    let reopened = resolve_project_generation(root.path()).unwrap();
    let inventory = reopened.graph_files_inventory().unwrap().unwrap();
    assert_eq!(
        list_delta_runs(&inventory, GraphDeltaJournalLimits::default())
            .unwrap()
            .len(),
        1
    );
    let (post_state, evidence) = reconstruct_graph_state(
        &reopened.graph_tree_root(),
        &inventory,
        GraphDeltaJournalLimits::default(),
    )
    .unwrap();
    assert_eq!(evidence.runs_replayed, 1);
    assert_eq!(post_state.fingerprint(), third);
}

#[test]
fn checkpoint_retains_exact_prior_bytes_after_compaction_and_cleanup() {
    let root = tempfile::tempdir().unwrap();
    let _base_gen = publish_base(root.path());
    publish_delta(root.path(), sample_ops());
    let (pinned_generation, pinned_run_digest) = {
        let parent = resolve_project_generation(root.path()).unwrap();
        let pinned_generation = parent.generation_uuid();
        let pinned_inventory = parent.graph_files_inventory().unwrap().unwrap();
        let pinned_run_digest = pinned_inventory
            .files
            .iter()
            .find(|entry| entry.relative_path.contains("run_"))
            .unwrap()
            .content_sha256
            .clone();
        (pinned_generation, pinned_run_digest)
    };

    create_checkpoint(
        root.path(),
        &CheckpointCreateRequest {
            operation_uuid: Uuid::now_v7(),
            name: "pre-compact".into(),
            description: None,
            actor_uuid: None,
        },
    )
    .unwrap();

    let mut request = default_request();
    request.cleanup_after_commit = true;
    request.cleanup_policy = ProjectRetentionPolicy {
        retained_ancestors: 0,
    };
    let report = compact_graph_delta(root.path(), &request, None).unwrap();
    assert_ne!(report.output_generation_uuid.unwrap(), pinned_generation);
    let cleanup = report.cleanup.as_ref().unwrap();
    // Checkpoint root keeps the pre-compact generation reachable; older
    // unpinned ancestors may still be reclaimed.
    assert!(
        cleanup
            .entries
            .iter()
            .any(|entry| entry.generation_uuid == Some(pinned_generation)
                && entry.disposition.as_str() == "reachable")
    );

    let reachability_preview = preview_project_cleanup(
        root.path(),
        ProjectRetentionPolicy {
            retained_ancestors: 0,
        },
        ProjectRetentionLimits::default(),
    )
    .unwrap();
    assert!(
        reachability_preview
            .entries
            .iter()
            .any(|entry| entry.generation_uuid == Some(pinned_generation)
                && entry.disposition.as_str() == "reachable")
    );

    let current = resolve_project_generation(root.path()).unwrap();
    assert_ne!(current.generation_uuid(), pinned_generation);
    drop(current);
    let prior_path = root
        .path()
        .join("generations")
        .join(pinned_generation.hyphenated().to_string())
        .join("graph")
        .join("deltas")
        .join("run_0000000000000001.gfdr");
    assert!(prior_path.exists());
    let bytes = std::fs::read(&prior_path).unwrap();
    let digest = {
        use sha2::{Digest, Sha256};
        Sha256::digest(&bytes)
            .iter()
            .fold(String::new(), |mut out, byte| {
                use std::fmt::Write;
                let _ = write!(out, "{byte:02x}");
                out
            })
    };
    assert_eq!(digest, pinned_run_digest);
}

#[test]
fn cleanup_reclaims_unreachable_inputs_after_compaction() {
    let root = tempfile::tempdir().unwrap();
    publish_base(root.path());
    publish_delta(root.path(), sample_ops());
    let old_generation = {
        let parent = resolve_project_generation(root.path()).unwrap();
        parent.generation_uuid()
    };

    let mut request = default_request();
    request.cleanup_after_commit = false;
    compact_graph_delta(root.path(), &request, None).unwrap();
    {
        let current = resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid();
        assert_ne!(current, old_generation);
    }

    let cleaned = execute_project_cleanup(
        root.path(),
        ProjectRetentionPolicy {
            retained_ancestors: 0,
        },
        ProjectRetentionLimits::default(),
    )
    .unwrap();
    assert!(cleaned.removed >= 1, "expected at least one removal");
    let old_path = root
        .path()
        .join("generations")
        .join(old_generation.hyphenated().to_string());
    assert!(
        !old_path.exists(),
        "unreachable compacted input must be removed; path still exists: {}",
        old_path.display()
    );
}

#[test]
fn memory_budget_fails_closed_independently_of_graph_fixture_size() {
    let root = tempfile::tempdir().unwrap();
    publish_base(root.path());
    publish_delta(root.path(), sample_ops());
    let mut request = default_request();
    request.limits.max_memory_bytes = 1;
    let err = compact_graph_delta(root.path(), &request, None).unwrap_err();
    assert!(err.to_string().contains("GF_RESOURCE_LIMIT"));
    // CURRENT unchanged.
    let resolved = resolve_project_generation(root.path()).unwrap();
    let inventory = resolved.graph_files_inventory().unwrap().unwrap();
    assert_eq!(
        list_delta_runs(&inventory, GraphDeltaJournalLimits::default())
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn cancellation_leaves_prior_generation_authoritative() {
    let root = tempfile::tempdir().unwrap();
    publish_base(root.path());
    publish_delta(root.path(), sample_ops());
    let before = resolve_project_generation(root.path())
        .unwrap()
        .generation_uuid();
    let cancel = AtomicBool::new(true);
    let err = compact_graph_delta(root.path(), &default_request(), Some(&cancel)).unwrap_err();
    assert!(err.to_string().contains("GF_CANCELLED"));
    assert_eq!(
        resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid(),
        before
    );
}

#[test]
fn exact_retry_compaction_is_idempotent() {
    let root = tempfile::tempdir().unwrap();
    publish_base(root.path());
    publish_delta(root.path(), sample_ops());
    let request = default_request();
    let first = compact_graph_delta(root.path(), &request, None).unwrap();
    let second = compact_graph_delta(root.path(), &request, None).unwrap();
    assert_eq!(first.output_generation_uuid, second.output_generation_uuid);
    assert!(second.publication.as_ref().unwrap().idempotent_replay);
}

#[test]
fn preview_and_status_report_runs_bytes_and_policy_triggers() {
    let root = tempfile::tempdir().unwrap();
    publish_base(root.path());
    publish_delta(root.path(), sample_ops());
    publish_delta(root.path(), sample_ops());

    let preview = preview_graph_delta_compaction(root.path(), &default_request(), None).unwrap();
    assert!(preview.dry_run);
    assert_eq!(preview.compacted_runs, 2);
    assert!(preview.input_bytes > 0);
    assert_eq!(
        resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid(),
        preview.input_generation_uuid
    );

    let status = graph_delta_compaction_status(
        root.path(),
        GraphDeltaCompactionPolicy {
            compact_when_runs: Some(2),
            compact_when_run_bytes: None,
            compact_when_replay_memory_bytes: None,
        },
        GraphDeltaJournalLimits::default(),
    )
    .unwrap();
    assert!(status.should_compact);
    assert!(
        status
            .trigger_reasons
            .iter()
            .any(|reason| reason.contains("run_count"))
    );
}
