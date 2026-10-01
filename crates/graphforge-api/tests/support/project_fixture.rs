//! Test-only current file-backed graph publication from a raw workspace.

use std::path::Path;

use graphforge_core::uuid::Uuid;
use graphforge_storage::{
    GRAPH_CAPABILITY_ID, GRAPH_CAPABILITY_VERSION, ProjectCapability, ProjectGenerationRequest,
    ProjectStageOutcome, UuidIndexBuildLimits, capture_graph_files, rebuild_v4_ordinal_identity,
    resolve_project_generation, stage_project_generation_with_graph_tree,
};

/// Publish current file-backed authority after constructing v4 ordinal
/// identity artifacts in the exact workspace being committed.
pub(crate) fn publish_graph_workspace_v4(container: &Path, workspace: &Path) {
    let _ = graphforge_storage::open_or_initialize_project(container).unwrap();
    rebuild_v4_ordinal_identity(workspace, UuidIndexBuildLimits::default()).unwrap();
    publish_graph_workspace(container, workspace);
}

pub(crate) fn publish_graph_workspace(container: &Path, workspace: &Path) {
    let _ = graphforge_storage::open_or_initialize_project(container).unwrap();
    let parent = resolve_project_generation(container).unwrap();
    let expected_parent = parent.generation_uuid();
    drop(parent);

    let (_, graph_participant) = capture_graph_files(workspace).unwrap();
    let mut participants = graphforge_storage::empty_workspace_participants().unwrap();
    participants.insert(0, graph_participant);
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
    let ProjectStageOutcome::Staged(staged) =
        stage_project_generation_with_graph_tree(container, &request, Some(workspace)).unwrap()
    else {
        panic!("fresh file-backed fixture publication unexpectedly replayed");
    };
    staged
        .validate(
            |_| Ok(()),
            |actual_parent, _| {
                assert_eq!(actual_parent.generation_uuid(), expected_parent);
                Ok(())
            },
        )
        .unwrap()
        .publish()
        .unwrap();
}

/// Publish a copy of `source`'s current graph as an expanded (V1) generation of
/// a fresh project at `target`. Mutating commits publish compact roots, so an
/// expanded generation (as projects published before that change are) has to be
/// built this way: the source's payloads are materialized into a private
/// workspace on the project volume and republished with a generation tree.
pub(crate) fn publish_expanded_copy(source: &Path, target: &Path) {
    let generation = resolve_project_generation(source).unwrap();
    let inventory = generation.graph_files_inventory().unwrap().unwrap();
    let workspace = tempfile::tempdir_in(source).unwrap();
    graphforge_storage::materialize_graph_objects(source, &inventory, workspace.path()).unwrap();
    publish_graph_workspace(target, workspace.path());
}
