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
    let (mut graph, mut version) =
        crate::branches::edit::prepare(owner, command, request.preview.branch_uuid)?;
    if !selected.values().any(|resolution| {
        matches!(
            resolution,
            ResearchUpstreamResolution::AdoptUpstream | ResearchUpstreamResolution::RetainBoth
        )
    }) {
        return Ok((graph, version));
    }
    let proof =
        super::projection::prepare(owner, command, snapshot, request, selected, capture, cancel)?;
    let source = crate::branches::private_view::open(owner, &proof)?;
    let changes: Vec<_> = selected
        .iter()
        .filter(|(_, resolution)| matches!(resolution, ResearchUpstreamResolution::AdoptUpstream))
        .map(|(key, _)| crate::branches::field_application::FieldChange {
            unit: crate::ResearchFieldIdentity {
                object_kind: key.0.clone(),
                object_uuid: key.1,
                field: key.2.clone(),
            },
            value_sha256: snapshot.upstream.fields.get(key).copied(),
        })
        .collect();
    crate::branches::field_ontology::apply(
        &mut graph,
        &source,
        &crate::branches::field_application::MutationContext {
            operation_uuid: request.operation_uuid,
            actor_uuid: request.actor_uuid,
        },
        &changes,
        cancel,
    )?;
    let mut domains = crate::branches::merge_domains::merge(&graph, &source)?;
    crate::branches::field_application::apply(&graph, &source, &changes, cancel)?;
    for (key, resolution) in selected {
        if matches!(resolution, ResearchUpstreamResolution::RetainBoth) {
            super::retain_both::apply(&graph, &source, key, cancel)?;
        }
    }

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
