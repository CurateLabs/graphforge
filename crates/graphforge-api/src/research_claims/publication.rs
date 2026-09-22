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
                .publish()?,
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
