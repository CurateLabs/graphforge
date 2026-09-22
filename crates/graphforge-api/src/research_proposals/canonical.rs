//! Append integration and explicit promotion to the owning authority in one review.
use super::{ReviewResearchProposalRequest, invalid, preview::Preview};
use crate::{
    CancellationToken, GfError, GraphForge, ResearchContext, ResearchDecisionInput,
    branches::publication,
    knowledge::knowledge_error,
    research_claims::{authority, ledger},
};
use graphforge_knowledge::research::{
    ResearchDecisionKind, ResearchDecisionRecord, ResearchSubjectKind,
};
use graphforge_storage::research_versions::{
    PreparedResearchContent, ResearchDecisionPublication, ResearchProposalDecision,
    ResearchProposalDestination, ResearchProposalItem,
};
use std::collections::BTreeMap;
use uuid::Uuid;

pub(super) fn prepare(
    owner: &GraphForge,
    command: &publication::Command,
    preview: &Preview,
    request: &ReviewResearchProposalRequest,
    destination: Option<&PreparedResearchContent>,
    items: &[ResearchProposalItem],
    cancellation: &CancellationToken,
) -> Result<Option<Box<ResearchDecisionPublication>>, GfError> {
    if request.promotions.len() > 256 {
        return Err(invalid("too many reviewed canonical promotions"));
    }
    let parent = graphforge_storage::resolve_generation_by_uuid(&command.root, preview.generation)?;
    let context = match preview.proposal.destination {
        ResearchProposalDestination::Project { .. } => ResearchContext::Project,
        ResearchProposalDestination::Branch { branch_uuid } => {
            ResearchContext::Branch { branch_uuid }
        }
    };
    let authority = authority::resolve(&parent, &context, request.community_uuid)?;
    let history = ledger::read_decisions(&parent)?;
    let prepared_view = destination
        .map(|prepared| crate::branches::private_view::open(owner, prepared))
        .transpose()?;
    let graph = prepared_view.as_ref().unwrap_or(&preview.destination);
    let mut subjects = BTreeMap::new();
    for item in items.iter().filter(|item| item.value_sha256.is_some()) {
        let kind = match item.unit.object_kind.as_str() {
            "node" => ResearchSubjectKind::Node,
            "edge" => ResearchSubjectKind::Edge,
            "assertion" => ResearchSubjectKind::Assertion,
            _ => continue,
        };
        subjects.insert((item.unit.object_kind.clone(), item.unit.object_uuid), kind);
    }
    let mut inputs: Vec<_> = subjects
        .into_iter()
        .map(|((_, id), kind)| ResearchDecisionInput {
            decision_uuid: Uuid::now_v7(),
            subject_kind: kind,
            subject_uuid: id,
            kind: ResearchDecisionKind::Integrate,
            source_version_uuid: Some(preview.proposal.source_version_uuid),
        })
        .collect();
    for input in &request.promotions {
        cancellation.checkpoint()?;
        let kind = match input.subject_kind {
            ResearchSubjectKind::Node => "node",
            ResearchSubjectKind::Edge => "edge",
            ResearchSubjectKind::Assertion => "assertion",
        };
        if input.kind != ResearchDecisionKind::Promote
            || input
                .source_version_uuid
                .is_some_and(|id| id != preview.proposal.source_version_uuid)
            || !preview.proposal.items.iter().any(|item| {
                item.unit.object_kind == kind
                    && item.unit.object_uuid == input.subject_uuid
                    && request.decisions.get(&item.item_uuid)
                        == Some(&ResearchProposalDecision::Accept)
            })
        {
            return Err(invalid(
                "canonical promotion must explicitly name accepted selected research",
            ));
        }
        let mut input = input.clone();
        input.source_version_uuid = Some(preview.proposal.source_version_uuid);
        inputs.push(input);
    }
    if inputs.is_empty() {
        return Ok(None);
    }
    let mut events = Vec::new();
    for (index, input) in inputs.iter().enumerate() {
        cancellation.checkpoint()?;
        crate::research_claims::decisions::validate_subject(graph, input)?;
        events.push(ResearchDecisionRecord {
            sequence: (history.events().len() + index + 1) as u64,
            decision_uuid: input.decision_uuid,
            operation_uuid: request.operation_uuid,
            request_sha256: command.intent,
            authority: authority.clone(),
            subject_kind: input.subject_kind,
            subject_uuid: input.subject_uuid,
            kind: input.kind,
            creator_uuid: request.actor_uuid,
            source_version_uuid: input.source_version_uuid,
            recorded_at: request.created_at,
        });
    }
    let merged = history.append(events).map_err(knowledge_error)?;
    let participant = ledger::encode_decisions(&merged)?;
    Ok(Some(Box::new(ResearchDecisionPublication {
        record_version: participant.record_version,
        schema_sha256: participant.schema_fingerprint,
        row_count: participant.row_count,
        bytes: participant.bytes,
    })))
}
