//! Prepare only reviewed native objects and their authenticated dependency closure.
use super::{ResearchUpstreamResolution, UpdateResearchBranchRequest, invalid, preview};
use crate::{
    CancellationToken, GfError, GraphForge,
    branches::{fields, publication},
};
use graphforge_storage::research_versions::{
    PreparedBranchContent, RegisterResearchVersion, ResearchGraphSelection,
    prepare_branch_selection,
};
use std::collections::{BTreeMap, BTreeSet};

pub(super) fn prepare(
    owner: &GraphForge,
    command: &publication::Command,
    snapshot: &preview::Preview,
    request: &UpdateResearchBranchRequest,
    selected: &BTreeMap<fields::Key, ResearchUpstreamResolution>,
    capture: Option<&RegisterResearchVersion>,
    cancel: &CancellationToken,
) -> Result<PreparedBranchContent, GfError> {
    let active: BTreeSet<_> = selected
        .iter()
        .filter(|(_, resolution)| {
            matches!(
                resolution,
                ResearchUpstreamResolution::AdoptUpstream | ResearchUpstreamResolution::RetainBoth
            )
        })
        .map(|(key, _)| (key.0.clone(), key.1))
        .filter(|object| {
            snapshot
                .upstream
                .fields
                .contains_key(&(object.0.clone(), object.1, "$object".into()))
        })
        .collect();
    let objects =
        crate::slices::branch::dependency_objects(&snapshot.upstream_graph, &active, cancel)?;
    let generation = snapshot.upstream_graph.generation_for_read()?;
    let replacements = crate::branches::domains::retained(&generation, objects.clone())?;
    let evidence = crate::research_versions::complete_evidence(&generation)?
        .into_iter()
        .filter(|e| {
            use graphforge_storage::research_versions::ResearchEvidenceReference::{
                ExternalOnly, Local, Unverifiable,
            };
            let id = match e {
                Local { artifact_uuid, .. }
                | ExternalOnly { artifact_uuid, .. }
                | Unverifiable { artifact_uuid } => artifact_uuid,
            };
            objects.contains(&("artifact".into(), *id))
        })
        .collect();
    let source_version = capture
        .map(|spec| spec.version_uuid)
        .or(snapshot.upstream.version)
        .ok_or_else(|| invalid("exact upstream Version is unavailable"))?;
    let spec = RegisterResearchVersion {
        version_uuid: preview::identity(request.operation_uuid, "selected_upstream"),
        context_uuid: preview::identity(request.operation_uuid, "selected_upstream_context"),
        source_generation_uuid: snapshot.upstream.generation,
        source_version: Some(source_version),
        selection: Some(
            generation
                .participant_descriptors()?
                .iter()
                .filter(|p| {
                    p.capability_id == "graph"
                        || p.capability_id == "workspace"
                            && matches!(
                                p.record_family_id.as_str(),
                                "ontology" | "ontology_composition" | "configuration"
                            )
                })
                .map(
                    |p| graphforge_storage::research_versions::ResearchParticipantKey {
                        capability: p.capability_id.clone(),
                        family: p.record_family_id.clone(),
                    },
                )
                .collect(),
        ),
        required_versions: BTreeSet::new(),
        label: None,
        description: None,
        created_at: request.created_at,
        evidence,
    };
    let graph = graph_selection(&objects);
    let prepared = prepare_branch_selection(
        &command.root,
        &spec,
        capture,
        Some(&graph),
        &replacements,
        cancel.flag(),
    )?;
    let mut view = crate::branches::private_view::open(owner, &prepared)?;
    view.read_only = false;
    let keys = selected
        .iter()
        .filter(|(_, resolution)| {
            matches!(
                resolution,
                ResearchUpstreamResolution::AdoptUpstream | ResearchUpstreamResolution::RetainBoth
            )
        })
        .map(|(key, _)| key.clone())
        .collect();
    crate::branches::field_application::redact_properties(&view, &keys, cancel)?;
    graphforge_storage::research_versions::prepare_branch_content(
        &command.root,
        &view.generation_for_read()?,
        prepared.version.clone(),
        cancel.flag(),
    )
}

fn graph_selection(objects: &fields::Objects) -> ResearchGraphSelection {
    ResearchGraphSelection {
        nodes: objects
            .iter()
            .filter(|(kind, _)| kind == "node")
            .map(|(_, id)| *id)
            .collect(),
        edges: objects
            .iter()
            .filter(|(kind, _)| kind == "edge")
            .map(|(_, id)| *id)
            .collect(),
        induced_edges: false,
        exclude_properties: BTreeSet::new(),
    }
}
