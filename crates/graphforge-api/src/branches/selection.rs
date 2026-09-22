//! Frozen Slice preparation and pre-publication domain validation.
use super::{domains, publication};
use crate::{CancellationToken, GfError, GraphForge, slices::branch};
use graphforge_storage::research_versions::{
    PreparedBranchContent, RegisterResearchVersion, ResearchParticipantKey,
};
use std::collections::BTreeSet;

pub(super) fn prepare(
    command: &publication::Command,
    mut selected: branch::BranchSelection,
    spec: &mut RegisterResearchVersion,
    cancellation: &CancellationToken,
) -> Result<PreparedBranchContent, GfError> {
    let replacements = domains::selected(&selected)?;
    // Carry only immutable ontology/workspace configuration plus the projected
    // graph. Domain rows are rebuilt by their owners, never copied wholesale.
    spec.selection = Some(
        selected
            .version
            .content
            .participants
            .iter()
            .filter(|p| {
                p.key.capability == "graph"
                    || (p.key.capability == "workspace"
                        && matches!(
                            p.key.family.as_str(),
                            "ontology" | "ontology_composition" | "configuration" | "branch_fields"
                        ))
            })
            .map(|p| p.key.clone())
            .collect::<BTreeSet<ResearchParticipantKey>>(),
    );
    spec.evidence.clone_from(&selected.evidence);
    if selected
        .version
        .content
        .participants
        .iter()
        .any(|p| p.key.capability == "workspace" && p.key.family == "branch_fields")
    {
        super::baseline::preserve_selected(
            &command.root,
            &selected.view,
            &mut selected.prepared,
            &selected.active.union(&selected.required).cloned().collect(),
            cancellation,
        )?;
    }
    selected.prepared.version.version_uuid = spec.version_uuid;
    selected.prepared.version.context_uuid = spec.context_uuid;
    selected.prepared.version.created_at = spec.created_at;
    selected.prepared.version.label.clone_from(&spec.label);
    graphforge_storage::research_versions::replace_prepared_branch_domains(
        &command.root,
        &mut selected.prepared,
        spec.selection.as_ref().expect("selected participant keys"),
        &replacements,
        cancellation.flag(),
    )?;
    let prepared = selected.prepared;
    let private = tempfile::tempdir().map_err(|error| GfError::Storage(error.to_string()))?;
    let generation = graphforge_storage::research_versions::materialize_prepared_branch(
        &command.root,
        &prepared,
        private.path(),
    )?;
    crate::checkpoints::validate_research_source(&generation)?;
    cancellation.checkpoint()?;
    Ok(prepared)
}

pub(super) fn authenticate(
    owner: &GraphForge,
    source: &super::BranchSource,
    cancellation: &CancellationToken,
) -> Result<Option<branch::BranchSelection>, GfError> {
    match source {
        super::BranchSource::Slice { frozen_ipc } => {
            branch::authenticate(owner, frozen_ipc, cancellation).map(Some)
        }
        _ => Ok(None),
    }
}
