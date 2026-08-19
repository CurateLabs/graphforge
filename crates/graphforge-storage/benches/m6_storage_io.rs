//! Durable M6 filesystem paths for CodSpeed walltime (#782).
//!
//! Fixture construction happens through `with_inputs`, outside the measured
//! closure. Every sample owns a private temporary project root.

use divan::Bencher;
use graphforge_core::{OntologyMode, TypeId};
use graphforge_storage::{
    GRAPH_CAPABILITY_ID, GRAPH_CAPABILITY_VERSION, GraphDeltaCompactionLimits,
    GraphDeltaCompactionRequest, GraphDeltaJournalLimits, GraphDeltaOp, GraphDeltaOpKind,
    GraphDeltaPayload, GraphDeltaPublishRequest, GraphWriter, ProjectCapability,
    ProjectGenerationRequest, ProjectRetentionLimits, ProjectRetentionPolicy, ProjectStageOutcome,
    capture_graph_files, compact_graph_delta, empty_workspace_participants,
    execute_project_cleanup, inspect_project_reachability, open_or_initialize_project,
    publish_graph_delta, recover_project_on_open, resolve_project_generation,
    stage_project_generation_with_graph_tree,
};
use uuid::Uuid;

fn main() {
    divan::main();
}

fn prepared_publication() -> (
    tempfile::TempDir,
    tempfile::TempDir,
    ProjectGenerationRequest,
) {
    let root = tempfile::tempdir().unwrap();
    open_or_initialize_project(root.path()).unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let mut writer = GraphWriter::open_at(workspace.path(), OntologyMode::Strict, 1).unwrap();
    writer.create_node(Uuid::from_u128(1), TypeId(1)).unwrap();
    writer.flush().unwrap();
    let (_, files) = capture_graph_files(workspace.path()).unwrap();
    let mut participants = empty_workspace_participants().unwrap();
    participants.insert(0, files);
    let request = ProjectGenerationRequest {
        transaction_uuid: Uuid::now_v7(),
        generation_uuid: Uuid::now_v7(),
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
    (root, workspace, request)
}

fn publish_base(root: &std::path::Path) {
    let (_, workspace, request) = prepared_publication();
    let ProjectStageOutcome::Staged(staged) =
        stage_project_generation_with_graph_tree(root, &request, Some(workspace.path())).unwrap()
    else {
        panic!("fresh publication replayed")
    };
    staged
        .validate(|_| Ok(()), |_, _| Ok(()))
        .unwrap()
        .publish()
        .unwrap();
}

fn publish_delta(root: &std::path::Path) {
    publish_graph_delta(
        root,
        &GraphDeltaPublishRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
            run_uuid: Uuid::now_v7(),
            operations: vec![GraphDeltaOp {
                operation_uuid: Uuid::now_v7(),
                kind: GraphDeltaOpKind::UpsertNode,
                payload: GraphDeltaPayload::UpsertNodeV2 {
                    node_uuid: Uuid::from_u128(2).to_string(),
                    node_id: 2,
                    type_ids: vec![1],
                    created_at_micros: 2,
                    updated_at_micros: 2,
                },
            }],
            limits: GraphDeltaJournalLimits::default(),
        },
    )
    .unwrap();
}

fn seed_generation_chain(root: &std::path::Path, delta_count: usize) {
    publish_base(root);
    for _ in 0..delta_count {
        publish_delta(root);
    }
}

#[divan::bench]
fn durable_commit(bencher: Bencher) {
    bencher
        .with_inputs(prepared_publication)
        .bench_local_values(|(root, workspace, request)| {
            let ProjectStageOutcome::Staged(staged) = stage_project_generation_with_graph_tree(
                root.path(),
                &request,
                Some(workspace.path()),
            )
            .unwrap() else {
                panic!("fresh publication replayed")
            };
            staged
                .validate(|_| Ok(()), |_, _| Ok(()))
                .unwrap()
                .publish()
                .unwrap()
        });
}

#[divan::bench]
fn durable_open(bencher: Bencher) {
    bencher
        .with_inputs(|| {
            let root = tempfile::tempdir().unwrap();
            open_or_initialize_project(root.path()).unwrap();
            seed_generation_chain(root.path(), 1);
            root
        })
        .bench_local_refs(|root| resolve_project_generation(root.path()).unwrap());
}

#[divan::bench]
fn recovery_scan(bencher: Bencher) {
    bencher
        .with_inputs(|| {
            let root = tempfile::tempdir().unwrap();
            open_or_initialize_project(root.path()).unwrap();
            seed_generation_chain(root.path(), 1);
            root
        })
        .bench_local_refs(|root| recover_project_on_open(root.path()).unwrap());
}

#[divan::bench]
fn reachability_scan(bencher: Bencher) {
    bencher
        .with_inputs(|| {
            let root = tempfile::tempdir().unwrap();
            open_or_initialize_project(root.path()).unwrap();
            seed_generation_chain(root.path(), 5);
            root
        })
        .bench_local_refs(|root| {
            inspect_project_reachability(
                root.path(),
                ProjectRetentionPolicy::default(),
                ProjectRetentionLimits::default(),
            )
            .unwrap()
        });
}

#[divan::bench]
fn garbage_collection(bencher: Bencher) {
    bencher
        .with_inputs(|| {
            let root = tempfile::tempdir().unwrap();
            open_or_initialize_project(root.path()).unwrap();
            seed_generation_chain(root.path(), 5);
            root
        })
        .bench_local_refs(|root| {
            execute_project_cleanup(
                root.path(),
                ProjectRetentionPolicy::default(),
                ProjectRetentionLimits::default(),
            )
            .unwrap()
        });
}

#[divan::bench]
fn spill_compaction(bencher: Bencher) {
    bencher
        .with_inputs(|| {
            let root = tempfile::tempdir().unwrap();
            open_or_initialize_project(root.path()).unwrap();
            publish_base(root.path());
            publish_delta(root.path());
            root
        })
        .bench_local_values(|root| {
            let limits = GraphDeltaCompactionLimits::default();
            compact_graph_delta(
                root.path(),
                &GraphDeltaCompactionRequest {
                    transaction_uuid: Uuid::now_v7(),
                    generation_uuid: Uuid::now_v7(),
                    through_run_sequence: None,
                    limits,
                    cleanup_after_commit: false,
                    cleanup_policy: ProjectRetentionPolicy::default(),
                    cleanup_limits: ProjectRetentionLimits::default(),
                },
                None,
            )
            .unwrap()
        });
}
