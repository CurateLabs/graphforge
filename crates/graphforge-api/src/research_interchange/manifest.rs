//! Bounded selected genealogy and identity citations without ancestor payload roots.
use crate::{CancellationToken, GfError, GraphForge};
use graphforge_storage::research_versions::{
    ResearchInterchangeManifest, ResearchInterchangeSelection, ResearchRegistry,
    ResearchVersionRecord,
};
use std::collections::BTreeMap;
use uuid::Uuid;

pub(super) fn build(
    owner: &GraphForge,
    registry: &ResearchRegistry,
    selected: &ResearchVersionRecord,
    prepared: &[&graphforge_storage::research_versions::PreparedResearchContent],
    proofs: &super::accepted::Proofs,
    cancellation: &CancellationToken,
) -> Result<ResearchInterchangeManifest, GfError> {
    let mut versions = BTreeMap::new();
    let mut pending = vec![selected.version_uuid];
    pending.extend(proofs.exports.values().copied());
    while let Some(id) = pending.pop() {
        cancellation.checkpoint()?;
        if versions.contains_key(&id) {
            continue;
        }
        let version = registry.versions.get(&id).ok_or_else(|| {
            GfError::Validation("required research Version is unavailable".into())
        })?;
        versions.insert(id, version.clone());
        pending.extend(version.content.required_versions.iter().copied());
    }
    let mut genealogy = BTreeMap::new();
    let mut identities = BTreeMap::new();
    for version in versions.values() {
        cite(registry, &mut identities, version.version_uuid)?;
        if let Some(source) = version.content.source_version {
            cite(registry, &mut identities, source)?;
        }
        let mut branch = registry.historical_branch(version.context_uuid);
        while let Some(record) = branch {
            if genealogy
                .insert(record.branch_uuid, record.clone())
                .is_some()
            {
                break;
            }
            cite(registry, &mut identities, record.base_version_uuid)?;
            cite(registry, &mut identities, record.origin_version_uuid)?;
            branch = record
                .parent_branch_uuid
                .and_then(|id| registry.historical_branch(id));
        }
        let view = if let Some(content) = prepared
            .iter()
            .find(|content| content.version.version_uuid == version.version_uuid)
        {
            crate::branches::private_view::open(owner, content)?
        } else {
            crate::research_versions::materialize_version(owner, version)?
        };
        for row in crate::branches::baseline::read(&view)?.values() {
            cite(registry, &mut identities, row.origin)?;
            if let Some(id) = row.incorporated {
                cite(registry, &mut identities, id)?;
            }
        }
    }
    for mapping in proofs.accepted.values() {
        for id in [
            mapping.source_version_uuid,
            mapping.destination_version_uuid,
            mapping.proof_version_uuid,
        ] {
            cite(registry, &mut identities, id)?;
        }
        let mut branch = registry.historical_branch(mapping.source_branch_uuid);
        while let Some(record) = branch {
            if genealogy
                .insert(record.branch_uuid, record.clone())
                .is_some()
            {
                break;
            }
            cite(registry, &mut identities, record.base_version_uuid)?;
            cite(registry, &mut identities, record.origin_version_uuid)?;
            branch = record
                .parent_branch_uuid
                .and_then(|id| registry.historical_branch(id));
        }
    }
    let source_project_uuid = registry
        .historical_project(selected.version_uuid)
        .or_else(|| {
            registry
                .historical_branch(selected.context_uuid)
                .map(|branch| branch.project_uuid)
        })
        .or_else(|| {
            selected
                .content
                .source_version
                .and_then(|id| registry.historical_project(id))
        })
        .unwrap_or(crate::research_claims::authority::project_uuid(
            &owner.generation_for_read()?,
        )?);
    let manifest = ResearchInterchangeManifest {
        contract_version: 1,
        research_capability_version: graphforge_storage::research_versions::RESEARCH_VERSION,
        producer: format!(
            "graphforge-storage/{};research-interchange/1",
            env!("CARGO_PKG_VERSION")
        ),
        source_project_uuid,
        fork_project_uuid: None,
        fork: None,
        selected_version_uuid: selected.version_uuid,
        selection: ResearchInterchangeSelection::Complete {
            source_version_uuid: selected.version_uuid,
        },
        versions,
        identities,
        genealogy,
        accepted: proofs.accepted.clone(),
        proof_exports: proofs.exports.clone(),
    };
    manifest.validate()?;
    Ok(manifest)
}

fn cite(
    registry: &ResearchRegistry,
    identities: &mut BTreeMap<Uuid, [u8; 32]>,
    id: Uuid,
) -> Result<(), GfError> {
    let identity = registry
        .identities
        .get(&id)
        .ok_or_else(|| GfError::Validation("research provenance identity is unavailable".into()))?;
    identities.insert(id, *identity);
    Ok(())
}
