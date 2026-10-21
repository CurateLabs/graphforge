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
    let options = crate::GraphForgeOptions::default();
    let (_, resource_policy) = options.clone().validate()?;
    // The streaming field comparison must hold one bounded decode batch at a
    // time inside the private query pool, never a whole-graph field map.
    let mut resource_policy = resource_policy.with_private_query_budget(64 * 1024 * 1024);
    resource_policy.batch_size = 256;
    resource_policy.target_partitions = 1;
    let graph = GraphForge::open_resolved_with_lifecycle_resources(
        generation.container_root().to_path_buf(),
        generation.clone(),
        true,
        graphforge_storage::filesystem_admission::ProjectLifecycleMode::Ephemeral,
        options,
        resource_policy,
    )?;
    // Streaming comparison: retain only the keys the registry commits for this
    // Version, never a whole-graph field map.
    let commitments = || {
        registry
            .interchange
            .values()
            .flat_map(|archive| {
                archive.accepted.values().filter(|mapping| {
                    archive.proof_exports.get(&mapping.proof_version_uuid)
                        == Some(&version.version_uuid)
                })
            })
            .map(|mapping| {
                (
                    (
                        mapping.unit.object_kind.clone(),
                        mapping.unit.object_uuid,
                        mapping.unit.field.clone(),
                    ),
                    mapping.value_sha256,
                )
            })
    };
    let wanted = commitments().map(|(key, _)| key).collect();
    let fields = crate::branches::fields::read_keys(&graph, &wanted, &CancellationToken::new())?;
    for row in crate::branches::baseline::read(&graph)?.values() {
        if !registry.identities.contains_key(&row.origin)
            || row
                .incorporated
                .is_some_and(|id| !registry.identities.contains_key(&id))
        {
            return Err(invalid());
        }
    }
    // An absent value commits a deleted or missing field and must stay absent.
    for (key, digest) in commitments() {
        if fields.get(&key) != digest.as_ref() {
            return Err(invalid());
        }
    }
    Ok(())
}
fn invalid() -> GfError {
    GfError::Validation("invalid archived baseline citation or accepted proof commitment".into())
}

#[cfg(test)]
mod tests;
