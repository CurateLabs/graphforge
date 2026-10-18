//! Read-only discovery citations: never Branch creation or retained content authority.
use super::{
    BTreeMap, GfError, MAX_CONTEXTS, ResearchInterchangeSelection, ResearchRegistry, Uuid, invalid,
};
use serde::{Deserialize, Serialize};

/// A published remote Branch reference authenticated by the clone's discovery boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchRemoteBranch {
    /// Canonical HTTPS repository URL, including its origin.
    pub repository_url: String,
    /// Original Project authority.
    pub project_uuid: Uuid,
    /// Published canonical ref name.
    pub ref_name: String,
    /// Branch creation identity; no creation record is inferred.
    pub branch_uuid: Uuid,
    /// Published immutable head.
    pub head_version_uuid: Uuid,
    /// Native immutable head commitment.
    pub identity_sha256: [u8; 32],
}
impl ResearchRemoteBranch {
    /// Validate and return the unique origin/repository/ref binding key.
    pub fn key(&self) -> Result<String, GfError> {
        let url = url::Url::parse(&self.repository_url)
            .map_err(|_| invalid("remote repository URL is invalid"))?;
        let pieces: Vec<_> = url.path().trim_start_matches('/').split('/').collect();
        let component = |s: &str| {
            !s.is_empty()
                && s.len() <= 100
                && s.as_bytes()
                    .iter()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || b"._-".contains(c))
                && s != "."
                && s != ".."
        };
        if self.repository_url.len() > 4096
            || url.scheme() != "https"
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || url.as_str() != self.repository_url
            || pieces.len() != 2
            || !pieces.iter().all(|s| component(s))
            || self.project_uuid.is_nil()
            || self.branch_uuid.is_nil()
            || self.head_version_uuid.is_nil()
            || self.ref_name.is_empty()
            || self.ref_name.len() > 4096
            || self.ref_name.split('/').any(|s| {
                s.is_empty()
                    || matches!(s, "." | "..")
                    || s.chars()
                        .any(|c| c.is_control() || c.is_whitespace() || "\\?#%".contains(c))
            })
        {
            return Err(invalid("remote Branch citation is not canonical"));
        }
        Ok(format!("{}#{}", self.repository_url, self.ref_name))
    }
}
impl ResearchRegistry {
    pub(super) fn validate_remote_branches(&self) -> Result<(), GfError> {
        if self.remote_branches.len() > super::MAX_CONTEXTS {
            return Err(invalid("remote Branch reference capacity exceeded"));
        }
        for (key, reference) in &self.remote_branches {
            if *key != reference.key()? {
                return Err(invalid("remote Branch reference key conflicts"));
            }
        }
        Ok(())
    }
}

/// Apply the existing imported-Branch admission as one atomic registry mutation.
pub(super) fn activate_imported_branch(
    registry: &mut ResearchRegistry,
    branch_uuid: &Uuid,
    head_version_uuid: &Uuid,
    project_uuid: &Uuid,
    remote_branches: &[ResearchRemoteBranch],
) -> Result<Option<Uuid>, GfError> {
    let archive = registry
        .interchange
        .get(head_version_uuid)
        .ok_or_else(|| invalid("selected imported Branch archive is unavailable"))?;
    if archive.source_project_uuid != *project_uuid
        || archive.fork.is_some()
        || !matches!(
            archive.selection,
            ResearchInterchangeSelection::Complete { .. }
        )
    {
        return Err(invalid(
            "only a same-Project complete imported Branch can continue",
        ));
    }
    let branch = archive
        .genealogy
        .get(branch_uuid)
        .ok_or_else(|| invalid("selected imported Branch genealogy is unavailable"))?
        .clone();
    let head = registry
        .versions
        .get(head_version_uuid)
        .ok_or_else(|| invalid("selected imported Branch head is unavailable"))?;
    if branch.project_uuid != *project_uuid
        || head.context_uuid != *branch_uuid
        || registry
            .heads
            .get(branch_uuid)
            .is_some_and(|old| old != head_version_uuid)
        || registry
            .branches
            .get(branch_uuid)
            .is_some_and(|old| old != &branch)
    {
        return Err(invalid(
            "selected imported Branch conflicts with native authority",
        ));
    }
    let mut references = BTreeMap::new();
    let mut selected = false;
    let mut repository = None;
    if remote_branches.len() > MAX_CONTEXTS {
        return Err(invalid("remote Branch reference capacity exceeded"));
    }
    for reference in remote_branches {
        let key = reference.key()?;
        if reference.project_uuid != *project_uuid
            || references.insert(key.clone(), reference.clone()).is_some()
            || registry
                .remote_branches
                .get(&key)
                .is_some_and(|old| old != reference)
            || repository.is_some_and(|old: &str| old != reference.repository_url)
        {
            return Err(invalid("conflicting imported remote Branch reference"));
        }
        repository = Some(reference.repository_url.as_str());
        if reference.branch_uuid == *branch_uuid {
            if reference.head_version_uuid != *head_version_uuid
                || registry.identities.get(head_version_uuid) != Some(&reference.identity_sha256)
            {
                return Err(invalid(
                    "selected remote Branch does not bind the imported head",
                ));
            }
            selected = true;
        }
    }
    if !selected {
        return Err(invalid("selected Branch remote reference is absent"));
    }
    registry.remote_branches.extend(references);
    registry.branches.insert(*branch_uuid, branch);
    registry.heads.insert(*branch_uuid, *head_version_uuid);
    Ok(Some(*head_version_uuid))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn reference() -> ResearchRemoteBranch {
        ResearchRemoteBranch {
            repository_url: "https://hub.example/owner/repo".into(),
            project_uuid: Uuid::from_u128(1),
            ref_name: "feature/claims".into(),
            branch_uuid: Uuid::from_u128(2),
            head_version_uuid: Uuid::from_u128(3),
            identity_sha256: [4; 32],
        }
    }
    #[test]
    fn remote_citations_require_canonical_origin_repository_ref_and_current_revision() {
        let item = reference();
        let key = item.key().unwrap();
        for url in [
            "http://hub.example/owner/repo",
            "https://user:secret@hub.example/owner/repo",
            "https://hub.example/owner/repo?token=opaque",
            "https://hub.example/owner/repo#main",
            "https://hub.example/owner/repo/",
            "https://hub.example/owner/%72epo",
        ] {
            assert!(
                ResearchRemoteBranch {
                    repository_url: url.into(),
                    ..item.clone()
                }
                .key()
                .is_err()
            );
        }
        for name in ["", "../main", "main//part", "main\\part", "has space"] {
            assert!(
                ResearchRemoteBranch {
                    ref_name: name.into(),
                    ..item.clone()
                }
                .key()
                .is_err()
            );
        }
        let mut registry = ResearchRegistry::default();
        registry.remote_branches.insert(key, item);
        registry.validate_remote_branches().unwrap();
        for revision in [6, 7] {
            assert!(registry.validate_remote_revision(revision).is_err());
        }
        registry.validate_remote_revision(8).unwrap();
        assert!(
            registry.heads.is_empty()
                && registry.versions.is_empty()
                && registry.materialized.is_empty()
                && registry.identities.is_empty()
        );
    }
}
