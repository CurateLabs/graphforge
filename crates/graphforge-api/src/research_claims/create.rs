//! Atomic classified claims reuse the existing assertion and provenance owners.
use super::{CreateResearchClaimRequest, ledger, publication};
use crate::{
    CancellationToken, CreateAssertionRequest, ExecutionResult, GfError, GraphForge, OperationId,
    WriteContext,
    knowledge::{self, knowledge_error, ledger as k},
};
use graphforge_core::ProjectErrorCode;
use graphforge_knowledge::research::{ResearchClaimLedger, ResearchClaimRecord};
use uuid::Uuid;

impl GraphForge {
    /// Create one immutable classified assertion without granting canonical acceptance.
    /// Existing evidence/status operations retain their native owners and semantics.
    pub fn create_research_claim(
        &mut self,
        request: &CreateResearchClaimRequest,
        cancellation: &CancellationToken,
    ) -> Result<ExecutionResult, GfError> {
        create(self, request, None, request.assertion_uuid, cancellation)
    }
}

pub(super) fn create(
    owner: &mut GraphForge,
    request: &CreateResearchClaimRequest,
    origin: Option<(Uuid, Uuid)>,
    concept: Uuid,
    cancellation: &CancellationToken,
) -> Result<ExecutionResult, GfError> {
    owner.graph_visibility.health.check()?;
    cancellation.checkpoint()?;
    if owner.read_only {
        return Err(GfError::Project {
            code: ProjectErrorCode::ReadOnlyView,
            message: "read-only research cannot create claims".into(),
        });
    }
    if request.operation_uuid.is_nil()
        || request.creator_uuid.is_nil()
        || request.expected_generation_uuid.is_nil()
        || request.graph_refs.len() > 4096
        || request.claim.len() > 1024 * 1024
    {
        return Err(GfError::Validation(
            "research claim has invalid identity or exceeds request bounds".into(),
        ));
    }
    let parent =
        graphforge_storage::resolve_project_generation(owner.resolved_generation.container_root())?;
    let (assertion_request, staged, classifications) = stage(request, origin, concept)?;
    let attempt = publication::Attempt::begin(
        &parent,
        request.operation_uuid,
        b"graphforge-create-research-claim/1",
        &(request, origin, concept),
    )?;
    if attempt.replay {
        let root = parent.container_root().to_path_buf();
        owner.refresh_current_authority(
            &root,
            None,
            None,
            request.expected_generation_uuid,
            true,
        )?;
        return Ok(knowledge::assertion_result(
            classifications.claim_batch().map_err(knowledge_error)?,
        ));
    }
    parent.require_capability("knowledge", 1)?;
    parent.require_capability("provenance", 1)?;
    parent.require_capability("epistemic", 1)?;
    let existing = ledger::read_claims(&parent)?;
    let merged = existing.merge(&classifications).map_err(knowledge_error)?;
    let assertions = k::read_ledger(&parent)?;
    let all_assertions = assertions.merge(&staged).map_err(knowledge_error)?;
    merged
        .validate_references(&all_assertions, &k::read_supersession_ledger(&parent)?)
        .map_err(knowledge_error)?;
    if parent.generation_uuid() != request.expected_generation_uuid
        || owner.generation_for_read()?.generation_uuid() != parent.generation_uuid()
    {
        return Err(publication::conflict());
    }
    knowledge::validate_graph_refs(owner, &request.graph_refs)?;
    if let Some(run) = request.run_uuid
        && !crate::algorithm_runs::read_ledger(&parent)?
            .runs
            .iter()
            .any(|row| row.run_uuid == run)
    {
        return Err(GfError::Validation(
            "research claim producer run is unavailable".into(),
        ));
    }
    let provenance =
        k::merged_provenance(&parent, &assertion_request, &staged, request.created_at)?;
    let mut replacements = k::encode_ledger(&all_assertions)?;
    replacements.extend(crate::provenance::encode_ledger(&provenance)?);
    replacements.extend(ledger::encode_claims(&merged)?);
    let generation = k::knowledge_generation_uuid(
        b"research_claim",
        OperationId(request.operation_uuid),
        &replacements,
    );
    replacements.push(attempt.receipt(generation)?);
    publication::publish(
        owner,
        &parent,
        request.operation_uuid,
        generation,
        replacements,
        cancellation,
    )?;
    Ok(knowledge::assertion_result(
        classifications.claim_batch().map_err(knowledge_error)?,
    ))
}

fn stage(
    request: &CreateResearchClaimRequest,
    origin: Option<(Uuid, Uuid)>,
    concept: Uuid,
) -> Result<
    (
        CreateAssertionRequest,
        graphforge_knowledge::AssertionLedger,
        ResearchClaimLedger,
    ),
    GfError,
> {
    let assertion_request = CreateAssertionRequest {
        context: WriteContext {
            operation_uuid: OperationId(request.operation_uuid),
            actor_uuid: Some(request.creator_uuid),
        },
        assertion_uuid: request.assertion_uuid,
        claim: request.claim.clone(),
        graph_refs: request.graph_refs.clone(),
    };
    let staged = knowledge::staged_assertion(&assertion_request, request.created_at)?;
    let record = ResearchClaimRecord {
        assertion_uuid: request.assertion_uuid,
        conceptual_uuid: concept,
        category: request.category,
        creator_uuid: request.creator_uuid,
        run_uuid: request.run_uuid,
        origin_branch_uuid: origin.map(|value| value.0),
        origin_version_uuid: origin.map(|value| value.1),
        provenance_uuid: staged.assertions[0].provenance_uuid,
        recorded_at: request.created_at,
    };
    let classifications =
        ResearchClaimLedger::new(vec![record], vec![]).map_err(knowledge_error)?;
    Ok((assertion_request, staged, classifications))
}
