//! One metadata publication and authority reconciliation, including ambiguous errors.
use crate::{CancellationToken, GfError, GraphForge, knowledge::ledger::snapshot_to_participant};
use graphforge_core::ProjectErrorCode;
use graphforge_storage::{
    ProjectCapability, ProjectGenerationRequest, ProjectParticipant, ProjectStageOutcome,
    ResolvedProjectGeneration,
};
use uuid::Uuid;

pub(super) fn publish(
    owner: &mut GraphForge,
    parent: &ResolvedProjectGeneration,
    operation: Uuid,
    generation: Uuid,
    replacements: Vec<ProjectParticipant>,
    cancellation: &CancellationToken,
) -> Result<(), GfError> {
    let root = parent.container_root().to_path_buf();
    let expected = parent.generation_uuid();
    let outcome = (|| {
        cancellation.checkpoint()?;
        let graph_objects = graphforge_storage::begin_graph_object_publication(&root)?;
        let mut participants = parent
            .participant_snapshots()?
            .into_iter()
            .filter(|p| {
                !replacements.iter().any(|r| {
                    r.capability_id == p.capability_id && r.record_family_id == p.record_family_id
                })
            })
            .map(snapshot_to_participant)
            .collect::<Result<Vec<_>, _>>()?;
        let mut capabilities: Vec<_> = parent
            .capabilities()
            .into_iter()
            .map(|c| ProjectCapability {
                capability_id: c.capability_id,
                capability_version: c.capability_version,
            })
            .collect();
        for p in &replacements {
            if !capabilities
                .iter()
                .any(|c| c.capability_id == p.capability_id)
            {
                capabilities.push(ProjectCapability {
                    capability_id: p.capability_id.clone(),
                    capability_version: p.capability_version,
                });
            }
        }
        participants.extend(replacements);
        participants.sort_by(|a, b| {
            (&a.capability_id, &a.record_family_id).cmp(&(&b.capability_id, &b.record_family_id))
        });
        let request = ProjectGenerationRequest {
            transaction_uuid: operation,
            generation_uuid: generation,
            capabilities,
            participants,
        };
        let receipt = match owner.stage_project_generation(&request)? {
            ProjectStageOutcome::AlreadyPublished(receipt) => receipt,
            ProjectStageOutcome::Staged(staged) => staged
                .validate(
                    |_| cancellation.checkpoint(),
                    |actual, _| {
                        cancellation.checkpoint()?;
                        if actual.generation_uuid() != expected {
                            return Err(conflict());
                        }
                        Ok(())
                    },
                )?
                .publish_with_graph_objects_cancellable(&graph_objects, &mut || {
                    cancellation.is_cancelled()
                })?,
        };
        Ok(receipt.generation_uuid)
    })();
    if let Err(error) =
        owner.refresh_current_authority(&root, outcome.as_ref().ok().copied(), None, expected, true)
    {
        owner.graph_visibility.health.fail(&error);
        return Err(outcome.err().unwrap_or(error));
    }
    outcome.map(|_| ())
}
pub(super) fn conflict() -> GfError {
    GfError::Project {
        code: ProjectErrorCode::WriteConflict,
        message: "research publication CURRENT precondition changed".into(),
    }
}

/// Current-history receipt prevents immutable row replay from bypassing operation identity.
pub(super) struct Attempt {
    registry: graphforge_storage::research_versions::ResearchRegistry,
    fingerprint: [u8; 32],
    operation: Uuid,
    pub replay: bool,
}
impl Attempt {
    pub(super) fn begin(
        parent: &ResolvedProjectGeneration,
        operation: Uuid,
        domain: &[u8],
        request: &impl serde::Serialize,
    ) -> Result<Self, GfError> {
        use sha2::{Digest, Sha256};
        let registry = graphforge_storage::research_versions::read_research_registry(parent)?;
        let bytes = serde_json::to_vec(request)
            .map_err(|_| GfError::Validation("invalid research request".into()))?;
        if bytes.len() > 2 * 1024 * 1024 {
            return Err(GfError::Project {
                code: ProjectErrorCode::ResourceLimit,
                message: "research request exceeds byte bound".into(),
            });
        }
        let mut digest = Sha256::new();
        digest.update(domain);
        digest.update(bytes);
        let fingerprint = digest.finalize().into();
        let replay = if let Some(receipt) = registry.receipts.get(&operation) {
            if receipt.request_sha256 != fingerprint || receipt.intent_sha256 != Some(fingerprint) {
                return Err(GfError::Project {
                    code: ProjectErrorCode::TransactionConflict,
                    message: "research operation identity has conflicting request content".into(),
                });
            }
            true
        } else {
            false
        };
        Ok(Self {
            registry,
            fingerprint,
            operation,
            replay,
        })
    }
    pub(super) fn receipt(mut self, generation: Uuid) -> Result<ProjectParticipant, GfError> {
        self.registry.receipts.insert(
            self.operation,
            graphforge_storage::research_versions::ResearchOperationReceipt {
                intent_sha256: Some(self.fingerprint),
                operation_uuid: self.operation,
                request_sha256: self.fingerprint,
                generation_uuid: generation,
                version_uuid: None,
            },
        );
        self.registry.participant()
    }
}
