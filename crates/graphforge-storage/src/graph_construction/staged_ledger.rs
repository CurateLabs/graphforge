//! Receipt-derived staged entries of the construction allocation ledger (#900).
//!
//! Every accepted chunk enters the active-identity ledger with exactly the
//! identity and allocated bytes its immutable receipt records, and the receipt
//! journal stays installed for the life of the session. Persisting those
//! entries in the checkpoint as well made the checkpoint grow by one entry per
//! staged artifact and be rewritten whole on every accepted chunk: the S25
//! rung failed staging at 4,459 chunks with 17,324 entries filling 1,039,441
//! of its 1,048,477 bytes, past `MAX_CONTROL_BYTES`.
//!
//! The checkpoint therefore omits a suffix of whole chunks whose ledger
//! entries all equal their receipts, recording only where that suffix starts
//! (`Checkpoint::staged_ledger_from_sequence`). Opening a session restores the
//! omitted entries from the receipt chain, authenticated against the
//! checkpoint's tail digest, before anything can read or rewrite the ledger.
//! The restored ledger is the one the writer held, entry for entry: nothing is
//! omitted unless its receipt reproduces it, and nothing a later phase
//! installed or retired is touched.
//!
//! The suffix is derived from the ledger at each write rather than tracked
//! beside it. Staging omits every staged entry; a shape end omits the inputs
//! behind its last sealing boundary that are still awaiting supersession; once
//! supersession has retired the inputs nothing is omitted, and the record is
//! byte-for-byte what it was before this module existed.

use std::collections::BTreeMap;

use super::{
    ArtifactReceipt, Checkpoint, ConstructionChunkReceipt, GfError, StableDirectory,
    control_sha256, storage,
};

/// The identity and allocation of every accepted chunk artifact, by ledger key.
///
/// Never persisted. Staged artifacts all coexist until the session is sealed,
/// so their keys are distinct; a duplicate is refused here exactly as the
/// ledger refuses installing an identity twice.
#[derive(Clone, Debug, Default)]
pub(super) struct StagedIdentityIndex {
    /// Ledger key to `(sequence, allocated bytes)` from the chunk receipt.
    entries: BTreeMap<String, (u64, u64)>,
    /// Artifacts each accepted sequence staged, in sequence order.
    artifacts: Vec<u8>,
}

/// The ledger key of one staged artifact, as `advance_checkpoint` installs it.
pub(super) fn staged_ledger_key(artifact: &ArtifactReceipt) -> String {
    format!(
        "{:016x}:{}",
        artifact.identity.volume_serial, artifact.identity.file_id
    )
}

fn receipt_artifacts(receipt: &ConstructionChunkReceipt) -> impl Iterator<Item = &ArtifactReceipt> {
    [&receipt.parquet, &receipt.identities, &receipt.details]
        .into_iter()
        .chain(receipt.endpoints.iter())
}

impl StagedIdentityIndex {
    /// Accepted sequences the index covers.
    pub(super) fn sequences(&self) -> u64 {
        self.artifacts.len() as u64
    }

    /// Record the next accepted chunk's artifacts.
    pub(super) fn push(&mut self, receipt: &ConstructionChunkReceipt) -> Result<(), GfError> {
        if receipt.sequence != self.sequences() {
            return Err(storage(
                "staged allocation index sequence is not contiguous",
            ));
        }
        let mut count = 0_u8;
        for artifact in receipt_artifacts(receipt) {
            if self
                .entries
                .insert(
                    staged_ledger_key(artifact),
                    (receipt.sequence, artifact.allocated_bytes),
                )
                .is_some()
            {
                return Err(storage(
                    "staged receipt journal names one file identity twice",
                ));
            }
            count += 1;
        }
        self.artifacts.push(count);
        Ok(())
    }

    /// Split `ledger` into the entries the checkpoint must persist and the
    /// first sequence of the omitted suffix, or `None` when nothing can be
    /// omitted and the ledger is persisted as it is.
    ///
    /// A sequence is omitted only when every one of its artifacts is in the
    /// ledger at its receipt's allocation, and only together with every later
    /// sequence; restoring `[from, next_sequence)` from the receipts then
    /// reproduces the ledger exactly. Linear in the ledger and the index: both
    /// are walked once in key order, with no per-entry lookup.
    pub(super) fn elide(
        &self,
        ledger: &BTreeMap<String, u64>,
        next_sequence: u64,
    ) -> Result<Option<(BTreeMap<String, u64>, u64)>, GfError> {
        if self.sequences() != next_sequence {
            return Err(storage(
                "staged allocation index differs from the receipt journal",
            ));
        }
        let mut held = vec![0_u8; self.artifacts.len()];
        self.walk(ledger, |_, _, staged| {
            if let Some(sequence) = staged {
                held[sequence] += 1;
            }
        });
        let mut from = self.artifacts.len();
        while from > 0 && held[from - 1] == self.artifacts[from - 1] {
            from -= 1;
        }
        if from == self.artifacts.len() {
            return Ok(None);
        }
        let mut persisted = Vec::new();
        self.walk(ledger, |key, allocated, staged| {
            if staged.is_none_or(|sequence| sequence < from) {
                persisted.push((key.clone(), allocated));
            }
        });
        Ok(Some((persisted.into_iter().collect(), from as u64)))
    }

    /// Visit every ledger entry with the sequence of the staged artifact it
    /// equals, if any: same key and the receipt's allocated bytes.
    fn walk(
        &self,
        ledger: &BTreeMap<String, u64>,
        mut visit: impl FnMut(&String, u64, Option<usize>),
    ) {
        let mut staged = self.entries.iter().peekable();
        for (key, allocated) in ledger {
            while staged.peek().is_some_and(|(candidate, _)| *candidate < key) {
                staged.next();
            }
            let sequence = staged
                .peek()
                .filter(|(candidate, (_, bytes))| *candidate == key && bytes == allocated)
                .map(|(_, (sequence, _))| {
                    usize::try_from(*sequence).expect("index sequences fit the index length")
                });
            visit(key, *allocated, sequence);
        }
    }
}

/// Rebuild the staged index from the receipt journal and restore the ledger
/// entries the checkpoint omitted.
///
/// The receipts are authenticated as a chain ending at the checkpoint's tail
/// digest before any of them is believed; that chain is what makes the
/// journal, not the checkpoint, the authority for staged allocation.
pub(super) fn restore_staged_ledger(
    root: &StableDirectory,
    checkpoint: &mut Checkpoint,
) -> Result<(), GfError> {
    let mut index = StagedIdentityIndex::default();
    let mut previous = None;
    for sequence in 0..checkpoint.next_sequence {
        let receipt = super::intake::read_checkpoint_receipt(root, checkpoint, sequence)?;
        if receipt.prior_receipt_sha256 != previous {
            return Err(storage("staged receipt chain changed"));
        }
        previous = Some(control_sha256(&receipt)?);
        index.push(&receipt)?;
    }
    if previous != checkpoint.last_receipt_sha256 {
        return Err(storage(
            "staged receipt journal tail differs from checkpoint",
        ));
    }
    if let Some(from) = checkpoint.staged_ledger_from_sequence.take() {
        if from >= checkpoint.next_sequence {
            return Err(storage("checkpoint staged ledger suffix is out of range"));
        }
        let ledger = &mut checkpoint.evidence.storage_active_identity_allocated_bytes;
        for (key, (sequence, allocated)) in &index.entries {
            if *sequence >= from && ledger.insert(key.clone(), *allocated).is_some() {
                return Err(storage(
                    "checkpoint persisted a staged ledger entry it also omitted",
                ));
            }
        }
    }
    checkpoint.staged_index = index;
    Ok(())
}

#[cfg(test)]
mod tests;
