//! Historical provenance survives ordinary publications without acquiring live authority.
use super::super::{BTreeMap, GfError, ResearchBranchRecord, ResearchRegistry, Uuid, invalid};

pub(in crate::research_versions) fn validate_registry(
    registry: &ResearchRegistry,
) -> Result<(), GfError> {
    if registry.interchange.len() > super::super::MAX_CONTEXTS {
        return Err(invalid("research interchange history capacity exceeded"));
    }
    let mut branches = BTreeMap::new();
    let mut projects = BTreeMap::new();
    for (id, archive) in &registry.interchange {
        archive.validate()?;
        if *id != archive.selected_version_uuid {
            return Err(invalid(
                "imported research archive key differs from selection",
            ));
        }
        for (version, project) in archive.version_projects.iter().chain(
            archive
                .proof_exports
                .iter()
                .map(|(original, exported)| (original, &archive.version_projects[exported])),
        ) {
            if projects
                .insert(*version, *project)
                .is_some_and(|old| old != *project)
            {
                return Err(invalid("conflicting imported Version Project citations"));
            }
        }
        for (version, identity) in &archive.identities {
            if registry.identities.get(version) != Some(identity) {
                return Err(invalid(
                    "imported research identity differs from permanent ledger",
                ));
            }
        }
        for (id, record) in &archive.genealogy {
            if branches
                .insert(*id, record)
                .is_some_and(|old| old != record)
                || registry
                    .branches
                    .get(id)
                    .is_some_and(|active| active != record)
            {
                return Err(invalid("conflicting imported Branch genealogy"));
            }
        }
    }
    Ok(())
}

pub(in crate::research_versions) fn preserve(
    before: &ResearchRegistry,
    after: &ResearchRegistry,
) -> Result<(), GfError> {
    for (id, archive) in &before.interchange {
        if after.interchange.get(id) != Some(archive) {
            return Err(invalid(
                "publication cannot rewrite imported research provenance",
            ));
        }
    }
    Ok(())
}

impl ResearchRegistry {
    /// Find creation metadata in active or imported history without creating a head.
    #[must_use]
    pub fn historical_branch(&self, branch_uuid: Uuid) -> Option<&ResearchBranchRecord> {
        self.branches.get(&branch_uuid).or_else(|| {
            self.interchange
                .values()
                .find_map(|archive| archive.genealogy.get(&branch_uuid))
        })
    }

    /// Original Project citation for an imported Version, never local governance.
    #[must_use]
    pub fn historical_project(&self, version_uuid: Uuid) -> Option<Uuid> {
        self.interchange.values().find_map(|archive| {
            archive
                .version_projects
                .get(&version_uuid)
                .or_else(|| {
                    archive
                        .proof_exports
                        .get(&version_uuid)
                        .and_then(|exported| archive.version_projects.get(exported))
                })
                .copied()
        })
    }
}
