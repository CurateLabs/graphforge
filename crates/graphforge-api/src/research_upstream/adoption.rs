//! Compose selected owner state privately before the shared Branch publication.
use super::{ResearchUpstreamResolution, UpdateResearchBranchRequest, invalid, preview};
use crate::{
    CancellationToken, GfError, GraphForge,
    branches::{fields, publication},
};
use graphforge_storage::research_versions::{
    RegisterResearchVersion, ResearchVersionRecord, prepare_branch_content,
    replace_prepared_branch_domains,
};
use std::collections::BTreeMap;

pub(super) fn apply(
    owner: &GraphForge,
    command: &publication::Command,
    snapshot: &preview::Preview,
    request: &UpdateResearchBranchRequest,
    selected: &BTreeMap<fields::Key, ResearchUpstreamResolution>,
    capture: Option<&RegisterResearchVersion>,
    cancel: &CancellationToken,
) -> Result<(GraphForge, ResearchVersionRecord), GfError> {
    let (graph, mut version) =
        crate::branches::edit::prepare(owner, command, request.preview.branch_uuid)?;
    if !selected.values().any(|resolution| {
        matches!(
            resolution,
            ResearchUpstreamResolution::AdoptUpstream | ResearchUpstreamResolution::RetainBoth
        )
    }) {
        return Ok((graph, version));
    }
    // Graph and ontology integration uses the shared typed owners once available.
    if selected.iter().any(|(key, resolution)| {
        matches!(
            resolution,
            ResearchUpstreamResolution::AdoptUpstream | ResearchUpstreamResolution::RetainBoth
        ) && (matches!(key.0.as_str(), "node" | "edge") || key.0.starts_with("ontology"))
    }) {
        return Err(invalid(
            "selected graph or ontology application requires the native typed owner integration",
        ));
    }
    let proof =
        super::projection::prepare(owner, command, snapshot, request, selected, capture, cancel)?;
    let source = crate::branches::private_view::open(owner, &proof)?;
    let mut domains = crate::branches::merge_domains::merge(&graph, &source)?;
    // Incoming preference history must not change any unreviewed Source choice.
    // Explicit adoption appends a new event through the native owner below.
    domains.retain(|participant| {
        !(participant.capability_id == "knowledge"
            && participant.record_family_id == "artifact_preference_events")
    });
    version.version_uuid = request.version_uuid;
    version.created_at = request.created_at;
    let mut prepared = prepare_branch_content(
        &command.root,
        &graph.generation_for_read()?,
        version,
        cancel.flag(),
    )?;
    for evidence in &proof.version.content.evidence {
        if !prepared.version.content.evidence.contains(evidence) {
            prepared.version.content.evidence.push(evidence.clone());
        }
    }
    let keep = prepared
        .version
        .content
        .participants
        .iter()
        .map(|participant| participant.key.clone())
        .collect();
    replace_prepared_branch_domains(&command.root, &mut prepared, &keep, &domains, cancel.flag())?;
    let mut graph = crate::branches::private_view::open(owner, &prepared)?;
    graph.read_only = false;
    super::preferences::apply(&graph, &source, request, selected, cancel)?;
    let actual = fields::read(&graph, cancel)?;
    for (key, resolution) in selected {
        if matches!(resolution, ResearchUpstreamResolution::AdoptUpstream)
            && actual.get(key) != snapshot.upstream.fields.get(key)
        {
            return Err(invalid(
                "prepared Branch does not contain the exact reviewed upstream value",
            ));
        }
    }
    Ok((graph, prepared.version.clone()))
}
