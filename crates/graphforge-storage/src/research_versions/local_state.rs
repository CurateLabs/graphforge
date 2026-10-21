//! Local-only registry state: Version tags and the current Branch (ADR 0061).
//!
//! Both fields are omitted from the registry when empty, so a registry that
//! never used them keeps its bytes and identity digests. Neither enters a Version
//! identity, an interchange archive or the lineage document, except that a
//! published tag travels in the lineage through the publish path.
use super::{GfError, MAX_RECEIPTS, ProjectErrorCode, ResearchRegistry, Uuid, error};

/// Upper bound of a tag name, the discovery ref bound.
const MAX_TAG_NAME_BYTES: usize = 4_096;

fn refused(message: &str) -> GfError {
    GfError::Validation(message.into())
}

fn valid_tag_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_TAG_NAME_BYTES
        && !name
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte == b' ')
}

impl ResearchRegistry {
    pub(super) fn validate_local_state(&self) -> Result<(), GfError> {
        if self.tags.len() > MAX_RECEIPTS {
            return Err(error(
                ProjectErrorCode::ResourceLimit,
                "research tag capacity exceeded",
            ));
        }
        for (name, target) in &self.tags {
            if !valid_tag_name(name) || !self.identities.contains_key(target) {
                return Err(super::invalid(
                    "research tag is malformed or names an unknown Version identity",
                ));
            }
        }
        Ok(())
    }
}

/// Create or delete one tag. A tag names any Version of the permanent identity
/// ledger, a released one included, and never moves: it is deleted and created again.
pub(super) fn set_tag(
    registry: &mut ResearchRegistry,
    name: &str,
    version_uuid: Option<Uuid>,
) -> Result<(), GfError> {
    if !valid_tag_name(name) {
        return Err(refused("research tag name is empty, too long or malformed"));
    }
    let Some(target) = version_uuid else {
        return registry
            .tags
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| refused("research tag does not exist"));
    };
    if !registry.identities.contains_key(&target) {
        return Err(refused("research tag names an unknown Version"));
    }
    match registry.tags.get(name) {
        Some(existing) if *existing == target => Ok(()),
        Some(_) => Err(refused(
            "GF_RESEARCH_TAG_EXISTS: the tag names another Version; delete it first",
        )),
        None => {
            if registry.tags.len() >= MAX_RECEIPTS {
                return Err(error(
                    ProjectErrorCode::ResourceLimit,
                    "research tag capacity exceeded",
                ));
            }
            registry.tags.insert(name.into(), target);
            Ok(())
        }
    }
}

/// Select the Branch porcelain verbs act on.
pub(super) fn select_branch(
    registry: &mut ResearchRegistry,
    branch_uuid: Uuid,
) -> Result<(), GfError> {
    if !registry.branches.contains_key(&branch_uuid) || !registry.heads.contains_key(&branch_uuid) {
        return Err(refused("research Branch to select does not exist"));
    }
    registry.current_branch = Some(branch_uuid);
    Ok(())
}
