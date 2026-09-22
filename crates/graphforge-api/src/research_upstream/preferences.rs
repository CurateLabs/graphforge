//! Preference state changes go through the native append-only Source owner.
use super::{ResearchUpstreamResolution, UpdateResearchBranchRequest, invalid, preview};
use crate::{
    CancellationToken, GfError, GraphForge, OperationId, SetPreferredArtifactRequest, WriteContext,
    branches::fields,
};
use std::collections::BTreeMap;

pub(super) fn apply(
    destination: &GraphForge,
    source: &GraphForge,
    request: &UpdateResearchBranchRequest,
    selected: &BTreeMap<fields::Key, ResearchUpstreamResolution>,
    cancel: &CancellationToken,
) -> Result<(), GfError> {
    let generation = source.generation_for_read()?;
    if generation.capability("knowledge")?.is_none() {
        return Ok(());
    }
    let incoming = crate::knowledge::ledger::read_preference_ledger(&generation)?;
    for (key, resolution) in selected {
        if key.0 != "source" || key.2 != "$preferred_artifact" {
            continue;
        }
        match resolution {
            ResearchUpstreamResolution::KeepLocal | ResearchUpstreamResolution::Explain { .. } => {
                continue;
            }
            ResearchUpstreamResolution::RetainBoth => {
                return Err(invalid(
                    "a Source has one preferred Artifact; choose an explicit preference or explanatory assertion",
                ));
            }
            ResearchUpstreamResolution::AdoptUpstream => {}
        }
        cancel.checkpoint()?;
        let desired = incoming.current_preferred_artifact(key.1);
        let current =
            crate::knowledge::ledger::read_preference_ledger(&destination.generation_for_read()?)?
                .current_preferred_artifact(key.1);
        if desired == current {
            continue;
        }
        let artifact_uuid = desired.ok_or_else(|| invalid("native Source preference history cannot clear an existing preference; keep the local preference or select another Artifact"))?;
        destination.set_preferred_artifact(SetPreferredArtifactRequest {
            context: WriteContext {
                operation_uuid: OperationId(preview::identity(
                    request.operation_uuid,
                    &format!("preference:{}", key.1),
                )),
                actor_uuid: Some(request.actor_uuid),
            },
            preference_event_uuid: uuid::Uuid::now_v7(),
            source_uuid: key.1,
            artifact_uuid,
            reason: format!("Reviewed upstream incorporation {}", request.operation_uuid),
        })?;
    }
    Ok(())
}
