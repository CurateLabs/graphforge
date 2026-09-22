//! Immutable Branch creation metadata; context heads remain in the registry.
use super::{GfError, ResearchRegistry, Uuid, invalid};
use serde::{Deserialize, Serialize};

pub(super) fn stage_origin(
    root: &std::path::Path,
    registry: &mut ResearchRegistry,
    spec: &super::RegisterResearchVersion,
) -> Result<(), GfError> {
    if spec.selection.is_some()
        || spec.source_version.is_some()
        || !spec.required_versions.is_empty()
        || registry.identities.contains_key(&spec.version_uuid)
        || registry.branches.contains_key(&spec.context_uuid)
    {
        return Err(invalid(
            "current Branch origin must be a fresh complete Project capture",
        ));
    }
    let origin = super::capture(root, spec, registry)?;
    registry
        .identities
        .insert(origin.version_uuid, super::identity_digest(&origin)?);
    registry.versions.insert(origin.version_uuid, origin);
    registry.validate()
}

pub(super) fn publish_with_origin(
    root: &std::path::Path,
    registry: &mut ResearchRegistry,
    origin: Option<&super::RegisterResearchVersion>,
    creation: Option<&ResearchBranchRecord>,
    version: &super::ResearchVersionRecord,
) -> Result<Uuid, GfError> {
    if let Some(origin) = origin {
        if creation.is_none_or(|record| record.origin_version_uuid != origin.version_uuid) {
            return Err(invalid(
                "current origin capture requires matching Branch creation",
            ));
        }
        stage_origin(root, registry, origin)?;
    }
    let id = publish(root, registry, creation, version)?;
    if let Some(origin) = origin {
        // Preserve immutable identity evidence without retaining a whole parent
        // merely because the selected Branch records its origin.
        registry.versions.remove(&origin.version_uuid);
    }
    Ok(id)
}

pub(super) fn publish(
    root: &std::path::Path,
    registry: &mut ResearchRegistry,
    creation: Option<&ResearchBranchRecord>,
    version: &super::ResearchVersionRecord,
) -> Result<Uuid, GfError> {
    if version.content.source_version.is_none()
        || registry.identities.contains_key(&version.version_uuid)
    {
        return Err(invalid(
            "Branch publication requires a fresh selected Version",
        ));
    }
    if let Some(record) = creation {
        let origin = registry
            .versions
            .get(&record.origin_version_uuid)
            .ok_or_else(|| invalid("Branch creation origin Version is unavailable"))?;
        if record
            .parent_branch_uuid
            .is_some_and(|parent| origin.context_uuid != parent)
            || (record.parent_branch_uuid.is_none()
                && registry.branches.contains_key(&origin.context_uuid))
            || registry
                .branches
                .values()
                .next()
                .is_some_and(|existing| existing.project_uuid != record.project_uuid)
        {
            return Err(invalid(
                "Branch creation origin differs from its immediate parent or Project",
            ));
        }
        if registry.branches.contains_key(&record.branch_uuid)
            || registry.heads.contains_key(&record.branch_uuid)
            || record.branch_uuid != version.context_uuid
            || record.base_version_uuid != version.version_uuid
            || Some(record.origin_version_uuid) != version.content.source_version
        {
            return Err(invalid(
                "Branch creation identity or selected base conflicts",
            ));
        }
        registry.branches.insert(record.branch_uuid, record.clone());
    } else if !registry.branches.contains_key(&version.context_uuid) {
        return Err(invalid("Branch publication context is unavailable"));
    }
    // No temporary preparation generation becomes a durable source pin. Verify
    // the complete CAS closure before it can become an authoritative head.
    super::retained_content::inspect(root, version, None)?;
    let id = super::insert_version(registry, version.clone())?;
    registry.materialized.insert(id);
    validate(registry)?;
    Ok(id)
}

/// Immutable genealogy and creation contract for one independently evolving Branch.
/// Membership, field baselines and contributions are Version-frozen Parquet data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchBranchRecord {
    /// Research context identity; also the key in the registry head mapping.
    pub branch_uuid: Uuid,
    /// Stable ultimate Project identity, never a storage generation UUID.
    pub project_uuid: Uuid,
    /// Immediate parent Branch, or None for Project research.
    pub parent_branch_uuid: Option<Uuid>,
    /// Exact immediate origin Version; genealogy alone is not a retention root.
    pub origin_version_uuid: Uuid,
    /// Selected initial Branch state retained for comparison, not the whole parent.
    pub base_version_uuid: Uuid,
    /// Caller-recorded creator identity; not remote authentication.
    pub creator_uuid: Uuid,
    /// UTC creation time in microseconds.
    pub created_at: i64,
    /// Immutable creation label.
    pub label: String,
    /// Exact creation selection commitment, separate from mutable membership.
    pub selection_sha256: [u8; 32],
}

pub(super) fn validate(registry: &ResearchRegistry) -> Result<(), GfError> {
    for (id, branch) in &registry.branches {
        if id.is_nil()
            || *id != branch.branch_uuid
            || branch.project_uuid.is_nil()
            || branch.origin_version_uuid.is_nil()
            || branch.creator_uuid.is_nil()
            || branch.label.is_empty()
            || branch.label.len() > 4096
            || !registry.heads.contains_key(id)
            || registry
                .heads
                .get(id)
                .and_then(|head| registry.versions.get(head))
                .is_none_or(|head| head.content.source_version.is_none())
            || registry
                .versions
                .get(&branch.base_version_uuid)
                .is_none_or(|base| base.context_uuid != *id)
        {
            return Err(invalid("Branch identity, base or context head is invalid"));
        }
        // Parent records outlive selected payloads. No origin Version lookup is
        // required here: releasing unrelated parent history must remain possible.
        let mut parent = branch.parent_branch_uuid;
        let mut seen = std::collections::BTreeSet::from([*id]);
        while let Some(parent_id) = parent {
            if !seen.insert(parent_id) {
                return Err(invalid("Branch genealogy contains a cycle"));
            }
            let record = registry
                .branches
                .get(&parent_id)
                .ok_or_else(|| invalid("Branch immediate parent is unavailable"))?;
            if record.project_uuid != branch.project_uuid {
                return Err(invalid("Branch genealogy crosses Project authority"));
            }
            parent = record.parent_branch_uuid;
        }
    }
    Ok(())
}
