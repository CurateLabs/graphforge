//! Native creation from current, historical or Branch research.
use super::{BranchSource, CreateResearchBranchRequest, publication, unavailable};
use crate::{CancellationToken, GfError, GraphForge};
use graphforge_storage::research_versions::{
    RegisterResearchVersion, ResearchBranchRecord, ResearchEvidenceReference, ResearchMutation,
    ResearchOperationReceipt, prepare_branch_selection,
};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use uuid::Uuid;

struct Source {
    version: Uuid,
    generation: Uuid,
    parent_branch: Option<Uuid>,
    evidence: Vec<ResearchEvidenceReference>,
    capture: Option<Box<RegisterResearchVersion>>,
}
impl GraphForge {
    /// Create an independent whole-source Branch inside this Project. The exact
    /// initial Version shares authenticated content and preserves origin identity.
    pub fn create_research_branch(
        &mut self,
        request: &CreateResearchBranchRequest,
        cancellation: &CancellationToken,
    ) -> Result<ResearchOperationReceipt, GfError> {
        if request.branch_uuid.is_nil()
            || request.version_uuid.is_nil()
            || request.creator_uuid.is_nil()
            || request.label.is_empty()
            || request.label.len() > 4096
        {
            return Err(GfError::Validation(
                "Branch identity, creator or label is invalid".into(),
            ));
        }
        let command = publication::begin(
            self,
            request.operation_uuid,
            request.expected_generation_uuid,
            request,
            cancellation,
        )?;
        if let Some(receipt) = command.replay(self)? {
            return Ok(receipt);
        }
        let selected = super::selection::authenticate(self, &request.source, cancellation)?;
        let source = resolve_source(
            &command,
            request,
            selected.as_ref().map(|s| s.version.version_uuid),
        )?;
        let mut spec = RegisterResearchVersion {
            version_uuid: request.version_uuid,
            context_uuid: request.branch_uuid,
            source_generation_uuid: source.generation,
            source_version: Some(source.version),
            selection: None,
            required_versions: BTreeSet::new(),
            label: Some(request.label.clone()),
            description: None,
            created_at: request.created_at,
            evidence: source.evidence,
        };
        let selection_sha256 = selected.as_ref().map_or_else(
            || Sha256::digest(b"graphforge-whole-branch-selection/1").into(),
            |s| s.selector_sha256,
        );
        let active = selected.as_ref().map(|s| s.active.clone());
        let mut prepared = if let Some(selected) = selected {
            super::selection::prepare(&command.root, selected, &mut spec, cancellation)?
        } else {
            prepare_branch_selection(
                &command.root,
                &spec,
                source.capture.as_deref(),
                None,
                &[],
                cancellation.flag(),
            )?
        };
        super::baseline::initialize(
            self,
            &mut prepared,
            request.operation_uuid,
            active.as_ref(),
            cancellation,
        )?;
        let project_uuid = crate::research_claims::authority::project_uuid(
            &graphforge_storage::resolve_project_generation(&command.root)?,
        )?;
        let creation = ResearchBranchRecord {
            branch_uuid: request.branch_uuid,
            project_uuid,
            parent_branch_uuid: source.parent_branch,
            origin_version_uuid: source.version,
            base_version_uuid: request.version_uuid,
            creator_uuid: request.creator_uuid,
            created_at: request.created_at,
            label: request.label.clone(),
            selection_sha256,
        };
        let mutation = ResearchMutation::PublishBranch {
            intent_sha256: command.intent,
            origin_capture: source.capture,
            creation: Some(creation),
            version: Box::new(prepared.version.clone()),
        };
        let outcome = command.publish(self, mutation, cancellation);
        drop(prepared);
        outcome
    }
}
fn resolve_source(
    command: &publication::Command,
    request: &CreateResearchBranchRequest,
    selected_version: Option<Uuid>,
) -> Result<Source, GfError> {
    let version = match request.source {
        BranchSource::Current {
            origin_version_uuid,
            context_uuid,
        } => {
            if origin_version_uuid.is_nil()
                || context_uuid.is_nil()
                || context_uuid == request.branch_uuid
            {
                return Err(GfError::Validation(
                    "current origin requires distinct Project and Branch identities".into(),
                ));
            }
            let generation = graphforge_storage::resolve_generation_by_uuid(
                &command.root,
                request.expected_generation_uuid,
            )?;
            let evidence = crate::research_versions::complete_evidence(&generation)?;
            let capture = RegisterResearchVersion {
                version_uuid: origin_version_uuid,
                context_uuid,
                source_generation_uuid: generation.generation_uuid(),
                selection: None,
                source_version: None,
                required_versions: BTreeSet::new(),
                label: None,
                description: None,
                created_at: request.created_at,
                evidence: evidence.clone(),
            };
            return Ok(Source {
                version: origin_version_uuid,
                generation: generation.generation_uuid(),
                parent_branch: None,
                evidence,
                capture: Some(Box::new(capture)),
            });
        }
        BranchSource::Slice { .. } => selected_version.ok_or_else(unavailable)?,
        BranchSource::Version { version_uuid } => version_uuid,
        BranchSource::Branch { branch_uuid } => {
            if !command.registry.branches.contains_key(&branch_uuid) {
                return Err(unavailable());
            }
            *command
                .registry
                .heads
                .get(&branch_uuid)
                .ok_or_else(unavailable)?
        }
    };
    let record = command
        .registry
        .versions
        .get(&version)
        .ok_or_else(unavailable)?;
    Ok(Source {
        version,
        generation: record.content.generation_uuid,
        parent_branch: command
            .registry
            .branches
            .contains_key(&record.context_uuid)
            .then_some(record.context_uuid),
        evidence: record.content.evidence.clone(),
        capture: None,
    })
}
