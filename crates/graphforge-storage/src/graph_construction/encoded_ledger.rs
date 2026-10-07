//! Inventory-derived encoded entries of the construction allocation ledger (#900).
//!
//! Canonical encoding installs one allocation-ledger entry per encoded
//! artifact, and every checkpoint written after it persisted them all. The S26
//! rung encoded 21,525 artifacts (about 1.2 MB of entries) and failed the very
//! checkpoint write that pins the encoded inventory, past `MAX_CONTROL_BYTES`.
//!
//! The pinned inventory (`Checkpoint::encoding_inventory_sha256`) names every
//! encoded artifact, and the session never moves, links or removes one before
//! the discard that deletes the whole tree. So the checkpoint omits the encoded
//! entries as a whole and records only the native-identity authority digest of
//! the omitted set (`Checkpoint::encoded_ledger_sha256`). Opening a session
//! reads the authenticated inventory, re-derives each artifact's identity and
//! allocation from the file it names, and restores the entries only if they
//! reproduce that digest, before anything can read or rewrite the ledger. A
//! replaced inode or a changed allocation therefore refuses the open; it can
//! never be adopted as authority.
//!
//! The omission is all or nothing, and it is derived at each write from the
//! ledger rather than tracked beside it: every artifact of the pinned inventory
//! must be in the ledger at the allocation recorded when it was encoded, or
//! the ledger is persisted as it is. Entries are omitted only while the record
//! being written pins the inventory they were derived from.

use std::collections::BTreeMap;

use super::{
    Checkpoint, GfError, GraphConstructionState, OsStr, StableDirectory, file_identity, hex,
    is_canonical_sha256, storage,
};

/// The identity and allocation of every artifact of one pinned encoded
/// inventory, by ledger key, as recorded when it was encoded or restored.
#[derive(Clone, Debug)]
pub(super) struct EncodedIdentityIndex {
    inventory_sha256: String,
    entries: BTreeMap<String, u64>,
}

impl EncodedIdentityIndex {
    pub(super) const fn new(inventory_sha256: String, entries: BTreeMap<String, u64>) -> Self {
        Self {
            inventory_sha256,
            entries,
        }
    }

    /// The ledger without this index's entries, and their authority digest,
    /// when the record pins this index's inventory and holds every one of its
    /// entries at its recorded allocation; `None` persists the ledger whole.
    pub(super) fn elide(
        &self,
        pinned_inventory_sha256: Option<&str>,
        ledger: &BTreeMap<String, u64>,
    ) -> Option<(BTreeMap<String, u64>, String)> {
        if self.entries.is_empty()
            || pinned_inventory_sha256 != Some(self.inventory_sha256.as_str())
            || self
                .entries
                .iter()
                .any(|(key, allocated)| ledger.get(key) != Some(allocated))
        {
            return None;
        }
        let persisted = ledger
            .iter()
            .filter(|(key, _)| !self.entries.contains_key(*key))
            .map(|(key, allocated)| (key.clone(), *allocated))
            .collect();
        Some((persisted, encoded_ledger_sha256(&self.entries)))
    }
}

/// Authority digest over a set of encoded ledger entries.
pub(super) fn encoded_ledger_sha256(entries: &BTreeMap<String, u64>) -> String {
    crate::storage_attribution::identity_map_authority_sha256(entries)
}

/// The ledger key and filesystem space usage of the encoded artifact at the
/// normalized inventory path `relative` below the encoded `graph` directory.
pub(super) fn encoded_artifact_allocation(
    graph: &StableDirectory,
    relative: &str,
) -> Result<(String, graphforge_filesystem::FileSpaceUsage), GfError> {
    let components = std::path::Path::new(relative)
        .components()
        .map(|component| match component {
            std::path::Component::Normal(value) => Ok(value.to_owned()),
            _ => Err(storage("encoded artifact path is not normalized")),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let (name, directories) = components
        .split_last()
        .ok_or_else(|| storage("encoded artifact path is empty"))?;
    let mut directory = graph.try_clone().map_err(storage)?;
    for child in directories {
        directory = directory.open_child_directory(child).map_err(storage)?;
    }
    let file = directory.open_child_file(name).map_err(storage)?;
    let identity = file_identity(&file).map_err(storage)?;
    let usage = graphforge_filesystem::file_space_usage(&file).map_err(storage)?;
    Ok((
        format!("{:016x}:{}", identity.volume_serial, hex(&identity.file_id)),
        usage,
    ))
}

/// Restore the encoded ledger entries the checkpoint omitted, from the pinned
/// inventory and the files it names, authenticated by the recorded digest.
///
/// An aborted session keeps its persisted form, exactly as
/// `restore_staged_ledger` does: its only remaining operation is the discard
/// that removes its tree, which may already have unlinked encoded files.
pub(super) fn restore_encoded_ledger(
    root: &StableDirectory,
    checkpoint: &mut Checkpoint,
) -> Result<(), GfError> {
    if checkpoint.state == GraphConstructionState::Aborted {
        return Ok(());
    }
    let Some(recorded) = checkpoint.encoded_ledger_sha256.take() else {
        return Ok(());
    };
    if !recorded
        .strip_prefix("sha256:")
        .is_some_and(is_canonical_sha256)
    {
        return Err(storage("checkpoint encoded ledger digest is invalid"));
    }
    let pinned = checkpoint
        .encoding_inventory_sha256
        .clone()
        .ok_or_else(|| storage("checkpoint omits encoded ledger entries without an inventory"))?;
    let output = root
        .open_child_directory(OsStr::new("encoded-v1"))
        .map_err(storage)?;
    let inventory = crate::graph_construction_encoding::read_inventory(&output)?
        .ok_or_else(|| storage("encoded inventory is absent"))?;
    if crate::graph_construction_encoding::inventory_authority_sha256(&inventory)? != pinned {
        return Err(storage(
            "encoded inventory differs from checkpoint authority",
        ));
    }
    let graph = output
        .open_child_directory(OsStr::new("graph"))
        .map_err(storage)?;
    let mut entries = BTreeMap::new();
    for artifact in &inventory.artifacts {
        let (key, usage) = encoded_artifact_allocation(&graph, &artifact.path)?;
        if entries.insert(key, usage.allocated_bytes).is_some() {
            return Err(storage("encoded inventory names one file identity twice"));
        }
    }
    if encoded_ledger_sha256(&entries) != recorded {
        return Err(storage(
            "encoded artifact identities or allocations differ from checkpoint",
        ));
    }
    let ledger = &mut checkpoint.evidence.storage_active_identity_allocated_bytes;
    for (key, allocated) in &entries {
        if ledger.insert(key.clone(), *allocated).is_some() {
            return Err(storage(
                "checkpoint persisted an encoded ledger entry it also omitted",
            ));
        }
    }
    checkpoint.encoded_index = Some(EncodedIdentityIndex::new(pinned, entries));
    Ok(())
}

#[cfg(test)]
mod tests;
