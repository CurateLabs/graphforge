//! Versions are commits: parents, credited signatures and the permanent ancestry ledger.
//!
//! A Version's parents are part of its immutable identity. The registry also
//! keeps every recorded parent list in `ancestry`, which outlives released
//! payloads exactly as `identities` does, so descent stays walkable.
use super::{
    BTreeMap, BTreeSet, GfError, ProjectErrorCode, ResearchRegistry, ResearchVersionRecord, Uuid,
    error, invalid,
};
use serde::{Deserialize, Serialize};

/// Maximum parents of one Version. Operations record one (prior head or origin)
/// or two (a merge); the bound keeps the ledger and every record finite.
pub const MAX_PARENTS: usize = 8;

/// Where restored or incorporated content came from, without making it a parent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResearchVersionProvenance {
    /// The content is a restore of this earlier Version of the same context.
    Restored {
        /// Restored-from Version.
        version_uuid: Uuid,
    },
    /// A frozen Slice of this Version was brought into the prior head.
    Brought {
        /// Slice source Version.
        version_uuid: Uuid,
    },
}

impl ResearchVersionProvenance {
    /// The cited Version.
    #[must_use]
    pub fn version_uuid(&self) -> Uuid {
        match self {
            Self::Restored { version_uuid } | Self::Brought { version_uuid } => *version_uuid,
        }
    }
}

fn valid_parents(id: Uuid, parents: &[Uuid], identities: &BTreeMap<Uuid, [u8; 32]>) -> bool {
    let mut seen = BTreeSet::new();
    parents.len() <= MAX_PARENTS
        && parents
            .iter()
            .all(|parent| !parent.is_nil() && *parent != id && seen.insert(*parent))
        && parents.iter().all(|parent| identities.contains_key(parent))
}

/// Validate one retained record's commit metadata against the permanent ledgers.
pub(super) fn validate_record(
    registry: &ResearchRegistry,
    version: &ResearchVersionRecord,
) -> Result<(), GfError> {
    let id = version.version_uuid;
    if !valid_parents(id, &version.parents, &registry.identities) {
        return Err(invalid(
            "research Version parents are unbounded, duplicated, self-referential or unknown",
        ));
    }
    if version
        .author
        .iter()
        .chain(&version.committer)
        .any(|signature| signature.validate().is_err())
    {
        return Err(invalid("research Version signature is invalid"));
    }
    if version.provenance.as_ref().is_some_and(|provenance| {
        let source = provenance.version_uuid();
        source == id || !registry.identities.contains_key(&source)
    }) {
        return Err(invalid("research Version provenance is unknown or itself"));
    }
    let recorded = registry.ancestry.get(&id);
    if (version.parents.is_empty() && recorded.is_some())
        || (!version.parents.is_empty() && recorded != Some(&version.parents))
    {
        return Err(invalid(
            "research Version parents differ from the permanent ancestry ledger",
        ));
    }
    Ok(())
}

/// Validate the ledger itself: known identities and acyclic. Its capacity,
/// `MAX_RECEIPTS`, is checked with the registry's other bounds.
pub(super) fn validate(registry: &ResearchRegistry) -> Result<(), GfError> {
    for (id, parents) in &registry.ancestry {
        if parents.is_empty()
            || !registry.identities.contains_key(id)
            || !valid_parents(*id, parents, &registry.identities)
        {
            return Err(invalid("research ancestry entry is empty or unknown"));
        }
    }
    // Explicit stack traversal: an untrusted ledger may be deep.
    let mut done = BTreeSet::new();
    for start in registry.ancestry.keys() {
        let mut visiting = BTreeSet::new();
        let mut stack = vec![(*start, false)];
        while let Some((id, exiting)) = stack.pop() {
            if exiting {
                visiting.remove(&id);
                done.insert(id);
                continue;
            }
            if done.contains(&id) {
                continue;
            }
            if !visiting.insert(id) {
                return Err(invalid("research ancestry contains a cycle"));
            }
            stack.push((id, true));
            for parent in registry.ancestry.get(&id).into_iter().flatten() {
                stack.push((*parent, false));
            }
        }
    }
    Ok(())
}

/// Record a newly inserted Version's parents permanently.
pub(super) fn record(
    registry: &mut ResearchRegistry,
    version: &ResearchVersionRecord,
) -> Result<(), GfError> {
    if version.parents.is_empty() {
        return Ok(());
    }
    if !valid_parents(version.version_uuid, &version.parents, &registry.identities) {
        return Err(invalid(
            "research Version parents are unbounded, duplicated, self-referential or unknown",
        ));
    }
    if registry
        .ancestry
        .get(&version.version_uuid)
        .is_some_and(|old| *old != version.parents)
    {
        return Err(error(
            ProjectErrorCode::TransactionConflict,
            "immutable research Version ancestry conflicts",
        ));
    }
    registry
        .ancestry
        .insert(version.version_uuid, version.parents.clone());
    Ok(())
}

/// Require the exact parents an operation must record.
pub(super) fn require(version: &ResearchVersionRecord, expected: &[Uuid]) -> Result<(), GfError> {
    if version.parents != expected {
        return Err(invalid(
            "research Version parents differ from the operation's prior head and merge source",
        ));
    }
    Ok(())
}

/// The prior head followed by an optional second (merge) parent.
pub(super) fn prior_head_then(
    registry: &ResearchRegistry,
    context: Uuid,
    merged: Option<Uuid>,
) -> Vec<Uuid> {
    registry
        .heads
        .get(&context)
        .copied()
        .into_iter()
        .chain(merged)
        .collect()
}

/// Every publication preserves existing ancestry exactly, like identities.
pub(super) fn preserve(before: &ResearchRegistry, after: &ResearchRegistry) -> Result<(), GfError> {
    for (id, parents) in &before.ancestry {
        if after.ancestry.get(id) != Some(parents) {
            return Err(invalid(
                "publication cannot erase or rewrite research Version ancestry",
            ));
        }
    }
    Ok(())
}

impl ResearchRegistry {
    /// Every recorded ancestor of a Version, nearest first (breadth-first, first
    /// parents before merge parents). Released Versions remain in the walk.
    #[must_use]
    pub fn ancestors(&self, version_uuid: Uuid) -> Vec<Uuid> {
        let mut seen = BTreeSet::from([version_uuid]);
        let mut order = Vec::new();
        let mut queue = std::collections::VecDeque::from([version_uuid]);
        while let Some(id) = queue.pop_front() {
            for parent in self.ancestry.get(&id).into_iter().flatten() {
                if seen.insert(*parent) {
                    order.push(*parent);
                    queue.push_back(*parent);
                }
            }
        }
        order
    }
}
