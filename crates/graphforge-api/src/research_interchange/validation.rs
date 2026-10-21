//! Native historical validation before portable target admission.
use crate::{GfError, GraphForge};
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
    let mut expected = std::collections::BTreeMap::new();
    for archive in registry.interchange.values() {
        for mapping in archive.accepted.values() {
            if archive.proof_exports.get(&mapping.proof_version_uuid) != Some(&version.version_uuid)
            {
                continue;
            }
            let Some(digest) = mapping.value_sha256 else {
                // Such a mapping can never match a scanned commitment.
                return Err(invalid());
            };
            let key = (
                mapping.unit.object_kind.clone(),
                mapping.unit.object_uuid,
                mapping.unit.field.clone(),
            );
            // Two accepted mappings for one key must commit the same value.
            match expected.entry(key) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(digest);
                }
                std::collections::btree_map::Entry::Occupied(entry) if *entry.get() == digest => {}
                std::collections::btree_map::Entry::Occupied(_) => return Err(invalid()),
            }
        }
    }
    // Streaming comparison against the registry's accepted mappings: memory is
    // the expected set plus one engine batch, never a whole-graph field map.
    if crate::branches::fields::verify_expected(&graph, &expected)? != expected.len() {
        return Err(invalid());
    }
    for row in crate::branches::baseline::read(&graph)?.values() {
        if !registry.identities.contains_key(&row.origin)
            || row
                .incorporated
                .is_some_and(|id| !registry.identities.contains_key(&id))
        {
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
