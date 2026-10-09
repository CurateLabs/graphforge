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
//! entries as a whole and records only a control digest of the omitted set
//! (`Checkpoint::encoded_ledger_sha256`). Opening a session reads the
//! authenticated inventory, re-derives each artifact's identity and
//! allocation from the file it names, and restores the entries only if their
//! identities reproduce that digest and their length matches the pinned
//! inventory, before anything can read or rewrite the ledger.
//! Allocation is observed accounting, not identity: delayed allocation may
//! change it without changing the inode or content (#1881). The payload is not
//! re-read here; its content is authenticated at the commit-boundary admission
//! and the CAS install, and its links at supersession reclaim. Reopen reconciles current allocation totals and
//! monotonic peaks with the restored observations.
//!
//! The omission is all or nothing, and it is derived at each write from the
//! ledger rather than tracked beside it: every artifact of the pinned inventory
//! must be in the ledger at the allocation recorded when encoded or restored, or
//! the ledger is persisted as it is. Entries are omitted only while the record
//! being written pins the inventory they were derived from.

use std::collections::BTreeMap;

use super::{
    Checkpoint, Digest, File, GfError, GraphConstructionEvidence, GraphConstructionState, OsStr,
    Sha256, StableDirectory, file_identity, hex, is_canonical_sha256, storage,
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

/// Control authority digest over the encoded file identities, length-prefixed
/// in key order. Observed allocations are accounting and do not authenticate
/// identity. The v2 domain replaces the allocation-bearing pre-v1 contract;
/// older omission digests fail closed (see construction-supersession.md).
pub(super) fn encoded_ledger_sha256(entries: &BTreeMap<String, u64>) -> String {
    let mut digest = Sha256::new();
    digest.update(b"graphforge-construction-encoded-ledger-v2\0");
    for key in entries.keys() {
        digest.update((key.len() as u64).to_be_bytes());
        digest.update(key.as_bytes());
    }
    hex(&digest.finalize())
}

/// The ledger key and filesystem space usage of the encoded artifact at the
/// normalized inventory path `relative` below the encoded `graph` directory.
pub(super) fn encoded_artifact_allocation(
    graph: &StableDirectory,
    relative: &str,
) -> Result<(String, graphforge_filesystem::FileSpaceUsage), GfError> {
    let (_, file) = open_encoded_artifact(graph, relative)?;
    let identity = file_identity(&file).map_err(storage)?;
    let usage = graphforge_filesystem::file_space_usage(&file).map_err(storage)?;
    Ok((
        format!("{:016x}:{}", identity.volume_serial, hex(&identity.file_id)),
        usage,
    ))
}

fn open_encoded_artifact(
    graph: &StableDirectory,
    relative: &str,
) -> Result<(StableDirectory, File), GfError> {
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
    Ok((directory, file))
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
    if !is_canonical_sha256(&recorded) {
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
        let (_, file) = open_encoded_artifact(&graph, &artifact.path)?;
        let identity = file_identity(&file).map_err(storage)?;
        // Identity (the key below, reproduced by the digest) and length. The
        // payload is not re-read and its links are not judged here: content
        // and link authority are authenticated at the boundaries that consume
        // the artifact (supersession reclaim, commit-boundary admission and
        // the CAS install), as before.
        if file.metadata().map_err(storage)?.len() != artifact.bytes {
            return Err(storage("encoded artifact length changed"));
        }
        let usage = graphforge_filesystem::file_space_usage(&file).map_err(storage)?;
        let key = format!("{:016x}:{}", identity.volume_serial, hex(&identity.file_id));
        if entries.insert(key, usage.allocated_bytes).is_some() {
            return Err(storage("encoded inventory names one file identity twice"));
        }
    }
    if encoded_ledger_sha256(&entries) != recorded {
        return Err(storage(
            "encoded artifact identities differ from checkpoint",
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
    reconcile_observed_allocation(&mut checkpoint.evidence)?;
    checkpoint.encoded_index = Some(EncodedIdentityIndex::new(pinned, entries));
    Ok(())
}

/// Encoded entries are construction staging. Reobserve only their allocation:
/// logical/object totals and all other categories retain their recorded values.
fn reconcile_observed_allocation(evidence: &mut GraphConstructionEvidence) -> Result<(), GfError> {
    if evidence.storage_current != evidence.storage_receipt_category_authorities {
        return Err(storage("construction storage category authority differs"));
    }
    // Validate persisted peak authority before raising any high-water marks.
    evidence.storage_transient_peak_authorities()?;
    let category = crate::ArtifactCategory::ConstructionStaging;
    let total = evidence
        .storage_active_identity_allocated_bytes
        .values()
        .try_fold(0_u64, |sum, value| sum.checked_add(*value))
        .ok_or_else(|| storage("restored construction allocation overflows"))?;
    let other = evidence
        .storage_current
        .iter()
        .filter(|(key, _)| **key != category)
        .try_fold(0_u64, |sum, (_, value)| {
            sum.checked_add(value.allocated_bytes)
        })
        .ok_or_else(|| storage("other construction allocation overflows"))?;
    let observed = total
        .checked_sub(other)
        .ok_or_else(|| storage("restored construction allocation underflows"))?;
    for categories in [
        &mut evidence.storage_current,
        &mut evidence.storage_receipt_category_authorities,
    ] {
        categories
            .get_mut(&category)
            .ok_or_else(|| storage("construction staging category is absent"))?
            .allocated_bytes = observed;
    }
    for peaks in [
        &mut evidence.storage_transient_peak_allocated_bytes,
        &mut evidence.storage_receipt_transient_peak_authorities,
    ] {
        let peak = peaks
            .get_mut(&category)
            .ok_or_else(|| storage("construction staging peak is absent"))?;
        *peak = (*peak).max(observed);
    }
    evidence.storage_transient_peak_total_allocated_bytes = evidence
        .storage_transient_peak_total_allocated_bytes
        .max(total);
    evidence.storage_category_authorities()?;
    Ok(())
}

#[cfg(test)]
mod tests;
