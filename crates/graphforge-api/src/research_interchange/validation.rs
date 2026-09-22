//! Native historical validation before portable target admission.
use crate::{CancellationToken, GfError, GraphForge};
use graphforge_storage::{
    ResolvedProjectGeneration,
    research_versions::{ResearchRegistry, ResearchVersionRecord},
};

pub(crate) fn validate(
    generation: &ResolvedProjectGeneration,
    version: &ResearchVersionRecord,
    registry: &ResearchRegistry,
) -> Result<(), GfError> {
    let supported = crate::portable::supported_capabilities();
    for capability in generation.capabilities() {
        if !supported.iter().any(|reader| {
            reader.capability_id == capability.capability_id
                && reader.capability_version == capability.capability_version
        }) {
            return Err(GfError::Validation(
                "archived research capability is unsupported by this reader".into(),
            ));
        }
    }
    crate::branches::domain_bounds::preflight(generation)?;
    crate::checkpoints::validate_research_source(generation)?;
    if generation.capability("knowledge")?.is_some() {
        use crate::knowledge::ledger as k;
        k::read_source_ledger(generation)?;
        k::read_artifact_ledger(generation)?;
        k::read_derivation_ledger(generation)?;
        k::read_preference_ledger(generation)?;
        k::read_retention_ledger(generation)?;
    }
    if generation.capability("epistemic")?.is_some() {
        crate::research_claims::ledger::read_claims(generation)?;
        crate::research_claims::ledger::read_suppressions(generation)?;
    }
    let mut graph = GraphForge::open_resolved_with_lifecycle_mode(
        generation.container_root().to_path_buf(),
        generation.clone(),
        true,
        graphforge_storage::filesystem_admission::ProjectLifecycleMode::Ephemeral,
    )?;
    graph.resource_policy.memory_budget_bytes = 64 * 1024 * 1024;
    graph.resource_policy.spill_enabled = false;
    let fields = crate::branches::fields::read(&graph, &CancellationToken::new())?;
    for row in crate::branches::baseline::read(&graph)?.values() {
        if !registry.identities.contains_key(&row.origin)
            || row
                .incorporated
                .is_some_and(|id| !registry.identities.contains_key(&id))
        {
            return Err(invalid());
        }
    }
    for archive in registry.interchange.values() {
        for mapping in archive.accepted.values() {
            if archive.proof_exports.get(&mapping.proof_version_uuid) != Some(&version.version_uuid)
            {
                continue;
            }
            let key = (
                mapping.unit.object_kind.clone(),
                mapping.unit.object_uuid,
                mapping.unit.field.clone(),
            );
            if fields.get(&key) != mapping.value_sha256.as_ref() {
                return Err(invalid());
            }
        }
    }
    Ok(())
}
fn invalid() -> GfError {
    GfError::Validation("invalid archived baseline citation or accepted proof commitment".into())
}

#[cfg(test)]
mod tests;
