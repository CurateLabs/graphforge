//! Explicit contextual decisions publish independently of graph and belief status.
use super::{
    RecordResearchDecisionsRequest, ResearchContext, ResearchDecisionInput, authority, ledger,
    publication,
};
use crate::{
    AssertionGraphRefInput, CancellationToken, ExecutionResult, GfError, GraphForge,
    knowledge::{assertion_result, knowledge_error},
};
use graphforge_core::ProjectErrorCode;
use graphforge_knowledge::{
    AssertionGraphRole, GraphObjectKind,
    research::{ResearchDecisionLedger, ResearchDecisionRecord, ResearchSubjectKind},
};
use sha2::{Digest, Sha256};
use uuid::Uuid;

impl GraphForge {
    /// Inspect current explicit canonical choices in one exact authority scope.
    /// Integration and revoked choices are absent; raw graph results are unchanged.
    pub fn research_canonical_choices(
        &self,
        context: &ResearchContext,
        community_uuid: Option<Uuid>,
    ) -> Result<ExecutionResult, GfError> {
        use graphforge_knowledge::research::ResearchDecisionKind;
        let generation = self.generation_for_read()?;
        let authority = authority::resolve(&generation, context, community_uuid)?;
        let history = ledger::read_decisions(&generation)?;
        let mut latest = std::collections::HashMap::new();
        for row in history
            .events()
            .iter()
            .filter(|row| row.authority == authority && row.kind != ResearchDecisionKind::Integrate)
        {
            latest.insert(
                (row.subject_kind.as_str(), row.subject_uuid),
                row.decision_uuid,
            );
        }
        selected(&history, |row| {
            row.authority == authority
                && row.kind == ResearchDecisionKind::Promote
                && latest.get(&(row.subject_kind.as_str(), row.subject_uuid))
                    == Some(&row.decision_uuid)
        })
    }
    /// Append explicit integration/promotion/revocation decisions in one native context.
    /// Exact retries return their original records, even after later decisions.
    pub fn record_research_decisions(
        &mut self,
        request: &RecordResearchDecisionsRequest,
        cancellation: &CancellationToken,
    ) -> Result<ExecutionResult, GfError> {
        self.graph_visibility.health.check()?;
        cancellation.checkpoint()?;
        if self.read_only {
            return Err(GfError::Project {
                code: ProjectErrorCode::ReadOnlyView,
                message: "read-only research cannot record canonical decisions".into(),
            });
        }
        let intent = request_fingerprint(request)?;
        let root = self.resolved_generation.container_root().to_path_buf();
        let parent = graphforge_storage::resolve_project_generation(&root)?;
        let history = ledger::read_decisions(&parent)?;
        if let Some(old) = history
            .events()
            .iter()
            .find(|row| row.operation_uuid == request.operation_uuid)
        {
            if old.request_sha256 != intent {
                return Err(GfError::Project {
                    code: ProjectErrorCode::TransactionConflict,
                    message: "research decision operation has conflicting content".into(),
                });
            }
            self.refresh_current_authority(
                &root,
                None,
                None,
                request.expected_generation_uuid,
                true,
            )?;
            return operation_result(&history, request.operation_uuid);
        }
        if parent.generation_uuid() != request.expected_generation_uuid
            || self.generation_for_read()?.generation_uuid() != parent.generation_uuid()
        {
            return Err(publication::conflict());
        }
        let authority = authority::resolve(&parent, &request.context, request.community_uuid)?;
        authority::with_context(self, &request.context, |graph| {
            for input in &request.decisions {
                cancellation.checkpoint()?;
                validate_subject(graph, input)?;
            }
            Ok(())
        })?;
        for input in &request.decisions {
            if let Some(id) = input.source_version_uuid {
                cancellation.checkpoint()?;
                let source = self.research_version(id)?;
                validate_subject(
                    &crate::research_versions::materialize_version(self, &source)?,
                    input,
                )?;
            }
        }
        let events = request
            .decisions
            .iter()
            .enumerate()
            .map(|(index, input)| ResearchDecisionRecord {
                sequence: (history.events().len() + index + 1) as u64,
                decision_uuid: input.decision_uuid,
                operation_uuid: request.operation_uuid,
                request_sha256: intent,
                authority: authority.clone(),
                subject_kind: input.subject_kind,
                subject_uuid: input.subject_uuid,
                kind: input.kind,
                creator_uuid: request.creator_uuid,
                source_version_uuid: input.source_version_uuid,
                recorded_at: request.recorded_at,
            })
            .collect();
        let merged = history.append(events).map_err(knowledge_error)?;
        let mut digest = Sha256::new();
        digest.update(b"graphforge-research-decisions-publication/1");
        digest.update(intent);
        let generation = graphforge_core::canonical::uuid_v8(digest.finalize().into());
        let mut replacements = vec![ledger::encode_decisions(&merged)?];
        if parent.capability("research")?.is_none() {
            replacements.push(
                graphforge_storage::research_versions::ResearchRegistry::default().participant()?,
            );
        }
        publication::publish(
            self,
            &parent,
            request.operation_uuid,
            generation,
            replacements,
            cancellation,
        )?;
        operation_result(&merged, request.operation_uuid)
    }

    /// Inspect explicit current authority history for this exact context/community.
    /// History survives restore, including decisions whose subjects are no longer present.
    pub fn research_decision_history(
        &self,
        context: &ResearchContext,
        community_uuid: Option<Uuid>,
    ) -> Result<ExecutionResult, GfError> {
        self.graph_visibility.health.check()?;
        let generation = self.generation_for_read()?;
        let authority = authority::resolve(&generation, context, community_uuid)?;
        let history = ledger::read_decisions(&generation)?;
        selected(&history, |row| row.authority == authority)
    }
}
fn request_fingerprint(request: &RecordResearchDecisionsRequest) -> Result<[u8; 32], GfError> {
    if request.operation_uuid.is_nil()
        || request.expected_generation_uuid.is_nil()
        || request.creator_uuid.is_nil()
        || request.decisions.is_empty()
        || request.decisions.len() > 256
    {
        return Err(GfError::Validation("research decisions require valid operation, CURRENT, creator and 1..256 explicit decisions".into()));
    }
    let bytes = serde_json::to_vec(request)
        .map_err(|_| GfError::Validation("invalid research decision request".into()))?;
    if bytes.len() > 256 * 1024 {
        return Err(GfError::Project {
            code: ProjectErrorCode::ResourceLimit,
            message: "research decision request exceeds byte limit".into(),
        });
    }
    let mut digest = Sha256::new();
    digest.update(b"graphforge-research-decisions-request/1");
    digest.update(bytes);
    Ok(digest.finalize().into())
}
fn validate_subject(graph: &GraphForge, input: &ResearchDecisionInput) -> Result<(), GfError> {
    match input.subject_kind {
        ResearchSubjectKind::Assertion => {
            let assertions = crate::knowledge::read_ledger(&graph.generation_for_read()?)?;
            if !assertions
                .assertions
                .iter()
                .any(|row| row.assertion_uuid == input.subject_uuid)
            {
                return Err(GfError::Validation(
                    "decision assertion is unavailable in the explicit research context".into(),
                ));
            }
            Ok(())
        }
        kind => crate::knowledge::validate_graph_refs(
            graph,
            &[AssertionGraphRefInput {
                graph_uuid: input.subject_uuid,
                graph_kind: if kind == ResearchSubjectKind::Node {
                    GraphObjectKind::Node
                } else {
                    GraphObjectKind::Edge
                },
                role: AssertionGraphRole::Subject,
                ordinal: 0,
            }],
        ),
    }
}
fn operation_result(
    history: &ResearchDecisionLedger,
    operation: Uuid,
) -> Result<ExecutionResult, GfError> {
    selected(history, |row| row.operation_uuid == operation)
}
fn selected(
    history: &ResearchDecisionLedger,
    select: impl Fn(&ResearchDecisionRecord) -> bool,
) -> Result<ExecutionResult, GfError> {
    let batch = history.batch().map_err(knowledge_error)?;
    let mask =
        arrow::array::BooleanArray::from(history.events().iter().map(select).collect::<Vec<_>>());
    let batch = arrow::compute::filter_record_batch(&batch, &mask)
        .map_err(|e| GfError::Execution(e.to_string()))?;
    Ok(assertion_result(batch))
}
