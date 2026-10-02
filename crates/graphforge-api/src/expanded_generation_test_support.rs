//! Test fixtures for expanded (V1) generations.
//!
//! Every mutating commit publishes a compact root (#1388), so a project the
//! facade wrote is never expanded. Generations published before that change
//! are, and they carry hazards a compact root cannot: a generation tree that a
//! read-only open may alias, and a whole-tree open. These helpers reproduce
//! such a generation from a facade's own workspace so those paths stay tested.

use std::path::Path;

use graphforge_storage::{
    ProjectCapability, ProjectGenerationRequest, ProjectParticipant, ProjectParticipantEncoding,
    ProjectStageOutcome,
};

use crate::GraphForge;

/// Republish the facade's current generation as an expanded (V1) generation
/// tree, over the facade's own live workspace.
pub(crate) fn expand_current_generation(graph: &GraphForge) {
    let root = graph.resolved_generation.container_root();
    publish_expanded_graph_workspace(root, graph.dir().path(), graph.lifecycle_mode);
}

/// Expand the facade's generation and reopen the project over it. The returned
/// facade hydrates the expanded tree; its next commit converts it to compact.
pub(crate) fn into_expanded(graph: GraphForge) -> GraphForge {
    expand_current_generation(&graph);
    let path = graph.path().expect("a durable project").to_path_buf();
    drop(graph);
    GraphForge::new(path.to_str()).expect("reopen the expanded project")
}

/// Publish `workspace` as an expanded graph participant over the project's
/// current generation, carrying every other participant forward unchanged.
pub(crate) fn publish_expanded_graph_workspace(
    project: &Path,
    workspace: &Path,
    mode: graphforge_storage::filesystem_admission::ProjectLifecycleMode,
) {
    let (_, graph_participant) = graphforge_storage::capture_graph_files(workspace).unwrap();
    let current = graphforge_storage::resolve_project_generation(project).unwrap();
    let capabilities = current
        .capabilities()
        .into_iter()
        .map(|capability| ProjectCapability {
            capability_id: capability.capability_id,
            capability_version: capability.capability_version,
        })
        .collect();
    let mut participants = current
        .participant_snapshots()
        .unwrap()
        .into_iter()
        .filter(|snapshot| {
            snapshot.capability_id != graphforge_storage::GRAPH_CAPABILITY_ID
                || snapshot.record_family_id != graphforge_storage::GRAPH_FILES_FAMILY
        })
        .map(|snapshot| ProjectParticipant {
            capability_id: snapshot.capability_id,
            capability_version: snapshot.capability_version,
            record_family_id: snapshot.record_family_id,
            record_version: snapshot.record_version,
            encoding: match snapshot.encoding.as_str() {
                "parquet" => ProjectParticipantEncoding::Parquet,
                "arrow" => ProjectParticipantEncoding::Arrow,
                "json" => ProjectParticipantEncoding::Json,
                other => panic!("unsupported fixture participant encoding {other}"),
            },
            schema_fingerprint: snapshot.schema_fingerprint,
            row_count: snapshot.row_count,
            bytes: snapshot.bytes,
        })
        .collect::<Vec<_>>();
    participants.push(graph_participant);
    let request = ProjectGenerationRequest {
        transaction_uuid: uuid::Uuid::new_v4(),
        generation_uuid: uuid::Uuid::new_v4(),
        capabilities,
        participants,
    };
    let ProjectStageOutcome::Staged(staged) =
        graphforge_storage::stage_project_generation_with_graph_tree_mode(
            project,
            &request,
            Some(workspace),
            mode,
        )
        .unwrap()
    else {
        panic!("expanded graph publication unexpectedly replayed");
    };
    staged
        .validate(|_| Ok(()), |_, _| Ok(()))
        .unwrap()
        .publish()
        .unwrap();
}
