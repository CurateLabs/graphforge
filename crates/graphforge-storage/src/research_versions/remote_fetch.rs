//! Exact fetched-head admission; importing content never advances the local Branch.
use super::{
    GfError, ResearchInterchangeManifest, ResearchInterchangeSelection, ResearchRegistry,
    ResearchRemoteBranch, Uuid, invalid,
};
use serde::{Deserialize, Serialize};

/// Compact source binding returned by fetch and required for remote incorporation.
/// The native operation receipt authenticates this binding and its immutable archive.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchRemoteFetchBinding {
    /// Existing native durable operation identity, never a second receipt store.
    pub operation_uuid: Uuid,
    /// Exact owning Project generation before this fetch.
    pub expected_generation_uuid: Uuid,
    /// Canonical repository/ref and exact remote Project/Branch/head identity.
    pub reference: ResearchRemoteBranch,
    /// Commitment to the fully verified canonical discovery lineage snapshot.
    pub lineage_sha256: [u8; 32],
    /// Verified semantic portable package identity for the selected remote Version.
    pub package_sha256: [u8; 32],
    /// Verified downloaded transport object identity from the bound discovery inventory.
    pub object_sha256: [u8; 32],
}

/// Native fetch request witness, reconstructed from an admitted immutable archive.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchRemoteBranchFetch {
    /// Exact receipt/repository binding used by a later upstream review.
    pub binding: ResearchRemoteFetchBinding,
    /// Existing native interchange authority, independently validated before admission.
    pub archive: Box<ResearchInterchangeManifest>,
}

impl ResearchRemoteBranchFetch {
    /// Check same-Project, exact same-Branch creation and immutable identity closure.
    /// The caller separately verifies package bytes and the source operation receipt.
    /// This method does not install objects, change heads or refresh citations.
    pub fn validate_identity(
        &self,
        registry: &ResearchRegistry,
        project_uuid: Uuid,
    ) -> Result<(), GfError> {
        let reference = &self.binding.reference;
        reference.key()?;
        self.archive.validate()?;
        if self.binding.operation_uuid.is_nil()
            || self.binding.expected_generation_uuid.is_nil()
            || reference.project_uuid != project_uuid
            || self.archive.source_project_uuid != project_uuid
            || self.archive.selected_version_uuid != reference.head_version_uuid
            || self.archive.fork.is_some()
            || !matches!(self.archive.selection,
                ResearchInterchangeSelection::Complete { source_version_uuid }
                    if source_version_uuid == reference.head_version_uuid)
        {
            return Err(invalid("fetch is not a complete same-Project remote head"));
        }
        let branch = registry
            .branches
            .get(&reference.branch_uuid)
            .ok_or_else(|| invalid("fetch requires an existing local Branch"))?;
        let version = self
            .archive
            .versions
            .get(&reference.head_version_uuid)
            .ok_or_else(|| invalid("fetched complete Version is unavailable"))?;
        if branch.project_uuid != project_uuid
            || !registry.heads.contains_key(&reference.branch_uuid)
            || self.archive.genealogy.get(&reference.branch_uuid) != Some(branch)
            || version.context_uuid != reference.branch_uuid
            || version.content.source_version.is_some()
            || self.archive.identities.get(&reference.head_version_uuid)
                != Some(&reference.identity_sha256)
        {
            return Err(invalid("fetched head differs from native Branch authority"));
        }
        self.validate_collisions(registry)?;
        let key = reference.key()?;
        if registry.remote_branches.get(&key).is_some_and(|old| {
            old.project_uuid != reference.project_uuid || old.branch_uuid != reference.branch_uuid
        }) {
            return Err(invalid(
                "fetch would rebind the repository ref to another Branch",
            ));
        }
        Ok(())
    }

    fn validate_collisions(&self, registry: &ResearchRegistry) -> Result<(), GfError> {
        let reference = &self.binding.reference;
        if registry
            .interchange
            .get(&reference.head_version_uuid)
            .is_some_and(|known| known != self.archive.as_ref())
        {
            return Err(invalid(
                "fetch conflicts with the admitted immutable archive",
            ));
        }
        for (id, branch) in &self.archive.genealogy {
            if registry
                .historical_branch(*id)
                .is_some_and(|known| known != branch)
            {
                return Err(invalid("fetched native Branch creation conflicts"));
            }
        }
        for (id, project) in &self.archive.version_projects {
            if registry
                .historical_project(*id)
                .is_some_and(|known| known != *project)
            {
                return Err(invalid("fetched Version Project authority conflicts"));
            }
        }
        for (id, digest) in &self.archive.identities {
            if registry
                .identities
                .get(id)
                .is_some_and(|known| known != digest)
            {
                return Err(invalid("fetched immutable Version identity conflicts"));
            }
        }
        for (id, parents) in &self.archive.ancestry {
            if registry
                .ancestry
                .get(id)
                .is_some_and(|known| known != parents)
            {
                return Err(invalid("fetched immutable Version ancestry conflicts"));
            }
        }
        Ok(())
    }
}
