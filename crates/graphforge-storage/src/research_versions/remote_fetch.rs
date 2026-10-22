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
    /// Tags the verified lineage serves on Versions this fetch admits. They are
    /// installed by the same publication as the admission, so a fetch never leaves
    /// them half applied (ADR 0061).
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub tags: std::collections::BTreeMap<String, Uuid>,
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
        let imported = self.archive.registry()?;
        for (id, root) in &imported.roots {
            if registry.roots.get(id).is_some_and(|known| known != root) {
                return Err(invalid("fetch conflicts with a native retention root"));
            }
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

impl ResearchRemoteBranchFetch {
    /// Reconstruct the exact canonical native request, without paths or caller receipts.
    #[must_use]
    pub fn operation(&self) -> super::ResearchOperation {
        super::ResearchOperation {
            operation_uuid: self.binding.operation_uuid,
            expected_generation_uuid: self.binding.expected_generation_uuid,
            mutation: super::ResearchMutation::FetchRemoteBranch {
                fetch: Box::new(self.clone()),
            },
        }
    }

    /// Authenticate this complete request against the owning native receipt registry.
    pub fn authenticate_receipt(
        &self,
        registry: &ResearchRegistry,
    ) -> Result<super::ResearchOperationReceipt, GfError> {
        use sha2::Digest as _;
        let digest: [u8; 32] = sha2::Sha256::digest(super::json(&self.operation())?).into();
        let receipt = registry
            .receipts
            .get(&self.binding.operation_uuid)
            .ok_or_else(|| invalid("remote fetch native receipt is unavailable"))?;
        if receipt.request_sha256 != digest
            || receipt.version_uuid != Some(self.binding.reference.head_version_uuid)
        {
            return Err(invalid(
                "remote fetch native receipt does not bind this exact request",
            ));
        }
        Ok(receipt.clone())
    }
}

impl ResearchRemoteFetchBinding {
    /// Rebuild an immutable fetch from native archive authority and authenticate its receipt.
    pub fn authenticate(
        &self,
        registry: &ResearchRegistry,
    ) -> Result<ResearchRemoteBranchFetch, GfError> {
        let archive = registry
            .interchange
            .get(&self.reference.head_version_uuid)
            .ok_or_else(|| invalid("fetched native archive is unavailable"))?;
        let fetch = ResearchRemoteBranchFetch {
            binding: self.clone(),
            archive: Box::new(archive.clone()),
        };
        fetch.validate_identity(registry, self.reference.project_uuid)?;
        fetch.authenticate_receipt(registry)?;
        Ok(fetch)
    }
}

pub(super) fn validate_request(operation: &super::ResearchOperation) -> Result<(), GfError> {
    if let super::ResearchMutation::FetchRemoteBranch { fetch } = &operation.mutation
        && (operation.operation_uuid != fetch.binding.operation_uuid
            || operation.expected_generation_uuid != fetch.binding.expected_generation_uuid)
    {
        return Err(invalid(
            "fetch binding differs from the native operation owner",
        ));
    }
    Ok(())
}

pub(super) fn apply(
    registry: &mut ResearchRegistry,
    fetch: &ResearchRemoteBranchFetch,
) -> Result<Uuid, GfError> {
    let reference = &fetch.binding.reference;
    fetch.validate_identity(registry, reference.project_uuid)?;
    let imported = fetch.archive.registry()?;
    for (id, version) in imported.versions {
        registry.versions.entry(id).or_insert(version);
    }
    registry.identities.extend(imported.identities);
    registry.ancestry.extend(imported.ancestry);
    registry.materialized.extend(imported.materialized);
    registry.roots.extend(imported.roots);
    registry
        .interchange
        .entry(reference.head_version_uuid)
        .or_insert_with(|| *fetch.archive.clone());
    for (name, target) in &fetch.binding.tags {
        super::local_state::install_served_tag(registry, name, *target)?;
    }
    registry
        .remote_branches
        .insert(reference.key()?, reference.clone());
    registry.validate()?;
    Ok(reference.head_version_uuid)
}

pub(super) fn preserve(
    before: &ResearchRegistry,
    after: &ResearchRegistry,
    parent: &crate::ResolvedProjectGeneration,
    candidate_generation: Uuid,
    witness: Option<&ResearchRemoteBranchFetch>,
    published: Option<&super::ResearchRemoteBranch>,
) -> Result<(), GfError> {
    let refreshed = witness
        .map(|fetch| fetch.binding.reference.key())
        .transpose()?;
    let recorded = published
        .map(super::ResearchRemoteBranch::key)
        .transpose()?;
    for (key, reference) in &before.remote_branches {
        if Some(key) != refreshed.as_ref()
            && Some(key) != recorded.as_ref()
            && after.remote_branches.get(key) != Some(reference)
        {
            return Err(invalid(
                "publication cannot rewrite remote Branch citations",
            ));
        }
    }
    if let (Some(reference), Some(key)) = (published, &recorded)
        && (after.remote_branches.get(key) != Some(reference)
            || before.heads != after.heads
            || before.branches != after.branches
            || after
                .remote_branches
                .keys()
                .any(|other| other != key && !before.remote_branches.contains_key(other)))
    {
        return Err(invalid(
            "a published-head record changed local authority or another citation",
        ));
    }
    if let Some(fetch) = witness {
        fetch.validate_identity(before, fetch.binding.reference.project_uuid)?;
        let receipt = fetch.authenticate_receipt(after)?;
        if before.receipts.contains_key(&fetch.binding.operation_uuid)
            || fetch.binding.expected_generation_uuid != parent.generation_uuid()
            || receipt.generation_uuid != candidate_generation
            || before.heads != after.heads
            || before.branches != after.branches
            || after
                .interchange
                .get(&fetch.binding.reference.head_version_uuid)
                != Some(fetch.archive.as_ref())
            || after
                .remote_branches
                .get(refreshed.as_ref().expect("fetch key"))
                != Some(&fetch.binding.reference)
            || after.remote_branches.keys().any(|key| {
                !before.remote_branches.contains_key(key) && Some(key) != refreshed.as_ref()
            })
            || fetch
                .binding
                .tags
                .iter()
                .any(|(name, target)| after.tags.get(name) != Some(target))
            || after.tags.iter().any(|(name, target)| {
                before
                    .tags
                    .get(name)
                    .or_else(|| fetch.binding.tags.get(name))
                    != Some(target)
            })
        {
            return Err(invalid(
                "fetch changed local authority or differs from its native receipt",
            ));
        }
    }
    Ok(())
}

/// Copy only authenticated immutable research content while holding its native CAS lease.
/// The caller performs full native package validation before invoking this preparation.
pub fn stage_remote_branch_fetch(
    source: &crate::ResolvedProjectGeneration,
    target: &std::path::Path,
    fetch: &ResearchRemoteBranchFetch,
) -> Result<crate::GraphObjectPublicationLease, GfError> {
    let source_registry = super::read_research_registry(source)?;
    if source_registry
        .interchange
        .get(&fetch.binding.reference.head_version_uuid)
        != Some(fetch.archive.as_ref())
    {
        return Err(invalid(
            "fetch source does not own the admitted immutable archive",
        ));
    }
    let lease = crate::begin_graph_object_publication(target)?;
    for version in fetch.archive.versions.values() {
        super::interchange::copy_version(
            source.container_root(),
            target,
            version,
            &source_registry,
        )?;
    }
    Ok(lease)
}

impl ResearchRemoteFetchBinding {
    /// Authenticate retained native content and require the current exact remote citation.
    /// Historical review validation uses `authenticate` without retargeting its endpoint.
    pub fn require_current(
        &self,
        registry: &ResearchRegistry,
        branch: Uuid,
    ) -> Result<(), GfError> {
        self.authenticate(registry)?;
        if self.reference.branch_uuid != branch
            || !registry
                .materialized
                .contains(&self.reference.head_version_uuid)
        {
            return Err(invalid(
                "remote review requires retained same-Branch content",
            ));
        }
        if registry.remote_branches.get(&self.reference.key()?) != Some(&self.reference) {
            return Err(super::error(
                super::ProjectErrorCode::WriteConflict,
                "remote review endpoint changed; review the current fetched head",
            ));
        }
        Ok(())
    }
}
