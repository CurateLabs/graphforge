//! Compose existing native owners privately, then publish exactly one Branch Version.
use super::{
    ChangeResearchBranchClaimRequest, CreateResearchClaimRequest, RelateResearchClaimsRequest,
    ResearchClaimChange, ResearchClaimDraft, create, ledger, publication,
};
use crate::{
    CancellationToken, GfError, GraphForge, OperationId, RecordAssertionStatusRequest,
    RecordReasoningRequest, ResearchOperationReceipt, SupersedeAssertionRequest, WriteContext,
    branches::{edit, publication as branch_publication},
    knowledge::{knowledge_error, ledger as k},
};
use graphforge_knowledge::{
    AssertionStatus, ReasoningContentFormat, ReasoningKind,
    research::{
        ClaimRelationKind, ClaimRelationRecord, ResearchSuppressionLedger,
        ResearchSuppressionRecord,
    },
};
use sha2::{Digest, Sha256};
use uuid::Uuid;

impl GraphForge {
    /// Challenge, revise, relate, create or suppress knowledge in one independent Branch.
    /// Parent/sibling assertions, canonical decisions and raw graph objects are preserved.
    pub fn change_research_branch_claim(
        &mut self,
        request: &ChangeResearchBranchClaimRequest,
        cancellation: &CancellationToken,
    ) -> Result<ResearchOperationReceipt, GfError> {
        if request.version_uuid.is_nil() || request.creator_uuid.is_nil() {
            return Err(GfError::Validation(
                "Branch claim change requires Version and creator identities".into(),
            ));
        }
        let command = branch_publication::begin(
            self,
            request.operation_uuid,
            request.expected_generation_uuid,
            request,
            cancellation,
        )?;
        if let Some(receipt) = command.replay(self)? {
            return Ok(receipt);
        }
        let (mut graph, mut version) = edit::prepare(self, &command, request.branch_uuid)?;
        apply(&mut graph, request, cancellation)?;
        cancellation.checkpoint()?;
        let generation = graph.generation_for_read()?;
        ledger::read_claims(&generation)?
            .validate_references(
                &k::read_ledger(&generation)?,
                &k::read_supersession_ledger(&generation)?,
            )
            .map_err(knowledge_error)?;
        version.version_uuid = request.version_uuid;
        version.created_at = request.created_at;
        edit::finish(
            self,
            command,
            &graph,
            version,
            request.operation_uuid,
            cancellation,
        )
    }
}
fn apply(
    graph: &mut GraphForge,
    request: &ChangeResearchBranchClaimRequest,
    cancellation: &CancellationToken,
) -> Result<(), GfError> {
    match &request.change {
        ResearchClaimChange::Create { claim } => {
            create_claim(graph, request, claim, claim.assertion_uuid, cancellation)?;
        }
        ResearchClaimChange::Relate { relation } => {
            if relation.creator_uuid != request.creator_uuid {
                return Err(GfError::Validation(
                    "Branch claim relation creator differs from publication creator".into(),
                ));
            }
            graph.relate_research_claims(
                &RelateResearchClaimsRequest {
                    operation_uuid: step(request, b"relation"),
                    expected_generation_uuid: graph.generation_for_read()?.generation_uuid(),
                    relation: relation.clone(),
                },
                cancellation,
            )?;
        }
        ResearchClaimChange::Challenge {
            assertion_uuid,
            status_event_uuid,
            reasoning_uuid,
            rationale,
            provenance_uuid,
        } => {
            rationale_record(
                graph,
                request,
                *assertion_uuid,
                *reasoning_uuid,
                rationale,
                *provenance_uuid,
            )?;
            cancellation.checkpoint()?;
            graph.record_assertion_status(RecordAssertionStatusRequest {
                context: context(request, b"challenge"),
                status_event_uuid: *status_event_uuid,
                assertion_uuid: *assertion_uuid,
                status: AssertionStatus::Disputed,
                confidence_uuid: None,
                reasoning_uuid: Some(*reasoning_uuid),
                provenance_uuid: *provenance_uuid,
            })?;
        }
        ResearchClaimChange::Revise { .. } => apply_revision(graph, request, cancellation)?,
        ResearchClaimChange::Suppress {
            suppression_uuid,
            assertion_uuid,
            provenance_uuid,
        } => {
            suppress(
                graph,
                request,
                *suppression_uuid,
                *assertion_uuid,
                *provenance_uuid,
                cancellation,
            )?;
        }
    }
    Ok(())
}
fn create_claim(
    graph: &mut GraphForge,
    request: &ChangeResearchBranchClaimRequest,
    claim: &ResearchClaimDraft,
    concept: Uuid,
    cancellation: &CancellationToken,
) -> Result<(), GfError> {
    create::create(
        graph,
        &CreateResearchClaimRequest {
            operation_uuid: step(request, b"create"),
            expected_generation_uuid: graph.generation_for_read()?.generation_uuid(),
            assertion_uuid: claim.assertion_uuid,
            claim: claim.claim.clone(),
            graph_refs: claim.graph_refs.clone(),
            category: claim.category,
            creator_uuid: request.creator_uuid,
            run_uuid: claim.run_uuid,
            created_at: request.created_at,
        },
        Some((request.branch_uuid, request.version_uuid)),
        concept,
        cancellation,
    )?;
    Ok(())
}
fn rationale_record(
    graph: &GraphForge,
    request: &ChangeResearchBranchClaimRequest,
    assertion_uuid: Uuid,
    reasoning_uuid: Uuid,
    rationale: &str,
    provenance_uuid: Uuid,
) -> Result<(), GfError> {
    graph.record_reasoning(RecordReasoningRequest {
        context: context(request, b"reasoning"),
        reasoning_uuid,
        assertion_uuid,
        kind: ReasoningKind::DecisionRationale,
        content_format: ReasoningContentFormat::TextPlain,
        content: rationale.as_bytes().to_vec(),
        supersedes_reasoning_uuid: None,
        provenance_uuid,
    })?;
    Ok(())
}
fn suppress(
    graph: &mut GraphForge,
    request: &ChangeResearchBranchClaimRequest,
    suppression_uuid: Uuid,
    assertion_uuid: Uuid,
    provenance_uuid: Uuid,
    cancellation: &CancellationToken,
) -> Result<(), GfError> {
    let parent = graph.generation_for_read()?;
    parent.require_capability("epistemic", 1)?;
    if !k::read_ledger(&parent)?
        .assertions
        .iter()
        .any(|row| row.assertion_uuid == assertion_uuid)
        || !crate::provenance::read_ledger(&parent)?
            .events
            .iter()
            .any(|row| row.provenance_uuid == provenance_uuid)
    {
        return Err(GfError::Validation(
            "suppression assertion or producing provenance is unavailable".into(),
        ));
    }
    let staged = ResearchSuppressionLedger::new(vec![ResearchSuppressionRecord {
        suppression_uuid,
        assertion_uuid,
        context_uuid: request.branch_uuid,
        creator_uuid: request.creator_uuid,
        provenance_uuid,
        recorded_at: request.created_at,
    }])
    .map_err(knowledge_error)?;
    let merged = ledger::read_suppressions(&parent)?
        .merge(&staged)
        .map_err(knowledge_error)?;
    let replacements = ledger::encode_suppressions(&merged)?;
    let operation = step(request, b"suppression");
    let generation =
        k::knowledge_generation_uuid(b"claim_suppression", OperationId(operation), &replacements);
    publication::publish(
        graph,
        &parent,
        operation,
        generation,
        replacements,
        cancellation,
    )
}
fn context(request: &ChangeResearchBranchClaimRequest, phase: &[u8]) -> WriteContext {
    WriteContext {
        operation_uuid: OperationId(step(request, phase)),
        actor_uuid: Some(request.creator_uuid),
    }
}
fn step(request: &ChangeResearchBranchClaimRequest, phase: &[u8]) -> Uuid {
    let mut hash = Sha256::new();
    hash.update(b"graphforge-branch-claim-preparation/1");
    hash.update(request.operation_uuid.as_bytes());
    hash.update(phase);
    graphforge_core::canonical::uuid_v8(hash.finalize().into())
}

fn apply_revision(
    graph: &mut GraphForge,
    request: &ChangeResearchBranchClaimRequest,
    cancellation: &CancellationToken,
) -> Result<(), GfError> {
    let ResearchClaimChange::Revise {
        prior_assertion_uuid,
        claim,
        supersession_uuid,
        status_event_uuid,
        reasoning_uuid,
        rationale,
        provenance_uuid,
        relation_uuid,
    } = &request.change
    else {
        return Err(GfError::Validation("expected a revision action".into()));
    };
    let generation = graph.generation_for_read()?;
    let concept = ledger::read_claims(&generation)?
        .conceptual_origin(
            *prior_assertion_uuid,
            &k::read_supersession_ledger(&generation)?,
        )
        .map_err(knowledge_error)?;
    create_claim(graph, request, claim, concept, cancellation)?;
    rationale_record(
        graph,
        request,
        *prior_assertion_uuid,
        *reasoning_uuid,
        rationale,
        *provenance_uuid,
    )?;
    cancellation.checkpoint()?;
    graph.supersede_assertion(SupersedeAssertionRequest {
        context: context(request, b"supersession"),
        supersession_uuid: *supersession_uuid,
        prior_assertion_uuid: *prior_assertion_uuid,
        replacement_assertion_uuid: claim.assertion_uuid,
        status_event_uuid: *status_event_uuid,
        reasoning_uuid: *reasoning_uuid,
        provenance_uuid: *provenance_uuid,
    })?;
    graph.relate_research_claims(
        &RelateResearchClaimsRequest {
            operation_uuid: step(request, b"relation"),
            expected_generation_uuid: graph.generation_for_read()?.generation_uuid(),
            relation: ClaimRelationRecord {
                relation_uuid: *relation_uuid,
                source_assertion_uuid: claim.assertion_uuid,
                target_assertion_uuid: *prior_assertion_uuid,
                kind: ClaimRelationKind::Supersedes,
                creator_uuid: request.creator_uuid,
                provenance_uuid: *provenance_uuid,
                recorded_at: request.created_at,
            },
        },
        cancellation,
    )?;
    Ok(())
}
