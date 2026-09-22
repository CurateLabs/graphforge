//! Re-project frozen content to exactly the newly accepted fields and proof closure.
use super::{ReviewResearchProposalRequest, SubmitResearchProposalRequest, preview::Preview};
use crate::{
    CancellationToken, GfError, GraphForge, ResearchFieldIdentity,
    branches::{fields, publication},
};
use graphforge_storage::research_versions::{PreparedResearchContent, ResearchProposalItem};

pub(super) fn prepare(
    owner: &GraphForge,
    command: &publication::Command,
    preview: &Preview,
    request: &ReviewResearchProposalRequest,
    items: &[ResearchProposalItem],
    cancellation: &CancellationToken,
) -> Result<PreparedResearchContent, GfError> {
    let source_fields = fields::read(&preview.source, cancellation)?;
    let mut members = crate::SliceMembers::default();
    for item in items {
        if !source_fields.contains_key(&(
            item.unit.object_kind.clone(),
            item.unit.object_uuid,
            "$object".into(),
        )) {
            continue;
        }
        match item.unit.object_kind.as_str() {
            "node" => &mut members.nodes,
            "edge" => &mut members.edges,
            "assertion" => &mut members.assertions,
            "source" => &mut members.sources,
            "artifact" => &mut members.artifacts,
            _ => continue,
        }
        .insert(item.unit.object_uuid);
    }
    let frozen = owner.freeze_slice(
        &crate::SliceRequest {
            request_uuid: super::identity(request.operation_uuid, "accepted_selection"),
            source: crate::SliceSource::Version {
                version_uuid: preview.proposal.payload_version_uuid,
            },
            selector: crate::SliceSelector::Direct { members },
            include: crate::SliceMembers::default(),
            exclude: crate::SliceMembers::default(),
            limits: crate::SliceLimits::default(),
        },
        cancellation,
    )?;
    let mut frozen_ipc = Vec::new();
    let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut frozen_ipc, &frozen.schema)
        .map_err(|error| GfError::Validation(error.to_string()))?;
    for batch in &frozen.batches {
        writer
            .write(batch)
            .map_err(|error| GfError::Validation(error.to_string()))?;
    }
    writer
        .finish()
        .map_err(|error| GfError::Validation(error.to_string()))?;
    drop(writer);
    let projection = &preview.registry.versions[&preview.proposal.payload_version_uuid];
    let selection = SubmitResearchProposalRequest {
        operation_uuid: request.operation_uuid,
        expected_generation_uuid: request.expected_generation_uuid,
        proposal_uuid: request.proposal_uuid,
        source_branch_uuid: projection.context_uuid,
        source_version_uuid: projection.version_uuid,
        frozen_ipc,
        fields: items
            .iter()
            .map(|item| ResearchFieldIdentity {
                object_kind: item.unit.object_kind.clone(),
                object_uuid: item.unit.object_uuid,
                field: item.unit.field.clone(),
            })
            .collect(),
        actor_uuid: request.actor_uuid,
        created_at: request.created_at,
        motivation: String::new(),
        policy: String::new(),
    };
    let mut proof = super::selection::freeze(owner, command, &selection, cancellation)?.prepared;
    // The retained selected proof is a projection of the original submitted
    // Branch Version; intermediate payload identity is not rewritten as origin.
    proof.version.content.source_version = Some(preview.proposal.source_version_uuid);
    Ok(proof)
}
