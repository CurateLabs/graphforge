//! Explicit, user-invoked, read-only integrity verification for a committed
//! project.
//!
//! This is the storage entry point behind `graphforge verify`, S7 of the
//! ingest write-path redesign (#1384). #1384's accepted decision moves "is
//! the store still intact" out of the write path entirely: nothing in this
//! module is ever called during ingest or construction. It exists so an
//! operator can ask, on demand, whether the retained store still matches
//! what was written -- exactly the shape of ParadeDB's explicit
//! segment-checksum verify command, which runs only on request and never
//! inline with a write.
//!
//! # What is checked
//! - The selected generation's `manifest.json` and every declared
//!   participant are re-read fresh from disk and their SHA-256 recomputed
//!   against the manifest's own recorded digest ([`ResolvedProjectGeneration::participant_snapshot`]
//!   performs this exact independent read-and-compare).
//! - Every object retained under `graph-objects/sha256/**` is re-read in
//!   full and its SHA-256 recomputed against its own bucket/name, using the
//!   object store's own public [`crate::verify_graph_object`] entry point
//!   for each one. This walks the object store directly rather than one
//!   generation's manifest, so it covers every payload reachable from any
//!   retained generation or checkpoint, plus objects only pending garbage
//!   collection. It deliberately does not reach into the object store's
//!   private install/lock internals -- this module only ever opens files it
//!   finds by name and asks the object store's own public API whether they
//!   are still what they claim to be.
//!
//! # What is not (yet) checked
//! A retained generation other than the selected one owns its own
//! `manifest.json` and participant files outside the content-addressed
//! store; this pass does not independently re-hash those (their
//! content-addressed payloads are still covered by the full object-store
//! sweep). Extending coverage to every retained generation's own control
//! files can reuse the same `generations/` enumeration
//! [`crate::capture_project_storage_identity_union`] already performs.
//!
//! # Relationship to ordinary project open
//! Ordinary project open (`GraphForge::new`'s workspace hydration) already
//! eagerly authenticates much of the reachable content-addressed tree, so a
//! corrupted object the current generation references is often refused
//! before any subcommand's own logic runs, with a generic validation error
//! rather than this module's structured report. That refusal is not
//! exhaustive or guaranteed for every reachable object, and it never
//! reaches objects outside the current generation at all (a superseded
//! object pending garbage collection, or one reachable only from another
//! retained checkpoint). This module's distinct, load-bearing value is that
//! strictly larger and unconditional set: every retained object, on
//! demand, regardless of what a prior open happened to touch.
//!
//! # Threat model and substrate assumptions
//! Per #1384's accepted decision and ADR 0013, this defends against
//! corruption, partial writes, and crash-time inconsistency on a substrate
//! that does not checksum file data -- not an active same-identity
//! adversary. If either assumption changes, this decision (and the
//! authentication regime it is one half of) must be revisited beside the
//! code that depends on it.
//!
//! # Privacy
//! [`ProjectVerifyReport`] and [`VerifyCategoryCounts`] hold only a
//! contract string, a generation UUID, and integer counts. No graph data,
//! query text, path, or identifier ever appears in the counted fields; a
//! caller that must not surface even the generation UUID (the CLI's JSON
//! receipt, for example) drops that one field before printing, the same way
//! `storage_attribution_receipt_from_snapshot` strips identity before it
//! reaches the CLI.

use std::io::Read as _;
use std::path::Path;

use graphforge_core::GfError;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::ResolvedProjectGeneration;

const MAX_CONTROL_OBJECT_BYTES: u64 = 64 * 1024 * 1024;

/// Pass/fail counts for one verification category. Counts only: never a
/// digest, a path, or any other identifier.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerifyCategoryCounts {
    /// Objects considered in this category.
    pub objects_checked: u64,
    /// Objects that re-hashed to their recorded or self-declared digest.
    pub objects_passed: u64,
    /// Objects that did not, or that could not be read at all.
    pub objects_failed: u64,
    /// Payload bytes re-read to answer this category.
    pub bytes_read: u64,
}

impl VerifyCategoryCounts {
    fn record(&mut self, bytes_read: u64, passed: bool) -> Result<(), GfError> {
        self.objects_checked = checked_add(self.objects_checked, 1)?;
        self.bytes_read = checked_add(self.bytes_read, bytes_read)?;
        if passed {
            self.objects_passed = checked_add(self.objects_passed, 1)?;
        } else {
            self.objects_failed = checked_add(self.objects_failed, 1)?;
        }
        Ok(())
    }
}

/// Structured result of one explicit `graphforge verify` run.
///
/// The generation UUID is retained here for API consumers that legitimately
/// need to know which generation was checked; a CLI or other externally
/// facing surface should omit it from anything it prints, matching the
/// existing storage-attribution receipt convention.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectVerifyReport {
    /// Stable machine-readable contract identifier.
    pub contract: String,
    /// The generation this run verified.
    pub generation_uuid: uuid::Uuid,
    /// The generation's manifest and every declared participant.
    pub catalog_and_participants: VerifyCategoryCounts,
    /// Every retained content-addressed graph payload object.
    pub content_addressed_objects: VerifyCategoryCounts,
    /// `true` only when every category reports zero failures.
    pub ok: bool,
}

/// Re-read and re-authenticate an already-committed project's retained
/// store. Read-only: nothing here mutates, repairs, or publishes anything.
///
/// # Errors
/// Returns an error only when the object store itself is structurally
/// broken (an unopenable bucket, a non-canonical object name). A content
/// mismatch on an individual manifest, participant, or content-addressed
/// object is counted in the returned report, not raised as an `Err`, so one
/// damaged object does not stop the sweep from covering the rest of the
/// store.
pub fn verify_project_store(
    generation: &ResolvedProjectGeneration,
) -> Result<ProjectVerifyReport, GfError> {
    let mut catalog_and_participants = VerifyCategoryCounts::default();
    verify_generation_manifest(generation, &mut catalog_and_participants)?;
    verify_participants(generation, &mut catalog_and_participants)?;

    let mut content_addressed_objects = VerifyCategoryCounts::default();
    verify_content_addressed_objects(generation.container_root(), &mut content_addressed_objects)?;

    let ok = catalog_and_participants.objects_failed == 0
        && content_addressed_objects.objects_failed == 0;
    Ok(ProjectVerifyReport {
        contract: "graphforge-verify/1".to_owned(),
        generation_uuid: generation.generation_uuid(),
        catalog_and_participants,
        content_addressed_objects,
        ok,
    })
}

/// Re-read and re-hash every retained content-addressed object under
/// `graph-objects/sha256/**`, comparing its recomputed SHA-256 against its
/// own bucket/name via the object store's own public
/// [`crate::verify_graph_object`]. This walks the on-disk layout by name
/// only; it never reaches into the object store's private lock or install
/// state, so it stays a pure, arm's-length reader of whatever the object
/// store already publishes.
fn verify_content_addressed_objects(
    container_root: &Path,
    counts: &mut VerifyCategoryCounts,
) -> Result<(), GfError> {
    let sha256_root = container_root.join("graph-objects").join("sha256");
    if !sha256_root.is_dir() {
        return Ok(());
    }
    for prefix_entry in std::fs::read_dir(&sha256_root)
        .map_err(|error| GfError::Storage(format!("read graph object prefixes: {error}")))?
    {
        let prefix_entry = prefix_entry.map_err(|error| {
            GfError::Storage(format!("read graph object prefix entry: {error}"))
        })?;
        if !prefix_entry
            .file_type()
            .map_err(|error| GfError::Storage(format!("inspect graph object prefix: {error}")))?
            .is_dir()
        {
            continue;
        }
        let Some(prefix) = prefix_entry.file_name().to_str().map(str::to_owned) else {
            return Err(GfError::Validation(
                "graph object prefix is not UTF-8".into(),
            ));
        };
        if !is_canonical_hex_segment(&prefix, 2) {
            continue;
        }
        for object_entry in std::fs::read_dir(prefix_entry.path())
            .map_err(|error| GfError::Storage(format!("read graph object bucket: {error}")))?
        {
            let object_entry = object_entry
                .map_err(|error| GfError::Storage(format!("read graph object entry: {error}")))?;
            if !object_entry
                .file_type()
                .map_err(|error| GfError::Storage(format!("inspect graph object entry: {error}")))?
                .is_file()
            {
                continue;
            }
            let Some(suffix) = object_entry.file_name().to_str().map(str::to_owned) else {
                return Err(GfError::Validation("graph object name is not UTF-8".into()));
            };
            if !is_canonical_hex_segment(&suffix, 62) {
                continue;
            }
            let digest = format!("{prefix}{suffix}");
            let length = object_entry
                .metadata()
                .map_err(|error| GfError::Storage(format!("inspect graph object: {error}")))?
                .len();
            let passed = crate::verify_graph_object(container_root, &digest, length).is_ok();
            counts.record(length, passed)?;
        }
    }
    Ok(())
}

fn is_canonical_hex_segment(value: &str, expected_len: usize) -> bool {
    value.len() == expected_len
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn verify_generation_manifest(
    generation: &ResolvedProjectGeneration,
    counts: &mut VerifyCategoryCounts,
) -> Result<(), GfError> {
    let path = generation.generation_root().join("manifest.json");
    match read_bounded(&path, MAX_CONTROL_OBJECT_BYTES) {
        Ok(bytes) => {
            let digest: [u8; 32] = Sha256::digest(&bytes).into();
            counts.record(bytes.len() as u64, digest == generation.manifest_sha256())
        }
        Err(_) => counts.record(0, false),
    }
}

fn verify_participants(
    generation: &ResolvedProjectGeneration,
    counts: &mut VerifyCategoryCounts,
) -> Result<(), GfError> {
    for descriptor in generation.participant_descriptors()? {
        match generation
            .participant_snapshot(&descriptor.capability_id, &descriptor.record_family_id)
        {
            Ok(Some(snapshot)) => {
                counts.record(
                    u64::try_from(snapshot.bytes.len()).unwrap_or(u64::MAX),
                    true,
                )?;
            }
            Ok(None) | Err(_) => counts.record(0, false)?,
        }
    }
    Ok(())
}

fn read_bounded(path: &Path, maximum: u64) -> Result<Vec<u8>, GfError> {
    let file = std::fs::File::open(path)
        .map_err(|error| GfError::Storage(format!("open project control file: {error}")))?;
    let metadata = file
        .metadata()
        .map_err(|error| GfError::Storage(format!("inspect project control file: {error}")))?;
    if !metadata.is_file() || metadata.len() > maximum {
        return Err(GfError::Validation(
            "project control file exceeds verification bound".into(),
        ));
    }
    let mut bytes = Vec::with_capacity(usize::try_from(metadata.len()).unwrap_or_default());
    file.take(maximum + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| GfError::Storage(format!("read project control file: {error}")))?;
    Ok(bytes)
}

fn checked_add(left: u64, right: u64) -> Result<u64, GfError> {
    left.checked_add(right)
        .ok_or_else(|| GfError::Validation("verify counter overflow".into()))
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::{
        graph_object_path, graph_object_store::corrupt_sealed_graph_object_for_test,
        install_graph_object_bytes, open_or_initialize_project,
    };

    #[test]
    fn empty_project_verifies_clean() {
        let project = tempdir().unwrap();
        let generation = open_or_initialize_project(project.path()).unwrap();
        let report = verify_project_store(&generation).unwrap();
        assert!(report.ok);
        assert_eq!(report.content_addressed_objects.objects_checked, 0);
        assert_eq!(report.content_addressed_objects.objects_failed, 0);
    }

    #[test]
    fn detects_a_bit_flip_in_a_retained_content_addressed_object() {
        let project = tempdir().unwrap();
        let generation = open_or_initialize_project(project.path()).unwrap();

        let (digest, _) =
            install_graph_object_bytes(project.path(), b"verify-fixture-payload").unwrap();

        let clean = verify_project_store(&generation).unwrap();
        assert!(clean.ok);
        assert_eq!(clean.content_addressed_objects.objects_checked, 1);
        assert_eq!(clean.content_addressed_objects.objects_failed, 0);

        // Flip one bit in the sealed, content-addressed object without
        // changing its length -- the same corruption shape #1269's
        // regression test exercises against the shaping path.
        let object_path = graph_object_path(project.path(), &digest).unwrap();
        let original = std::fs::read(&object_path).unwrap();
        let mut mutated = original.clone();
        mutated[0] ^= 0xFF;
        corrupt_sealed_graph_object_for_test(&object_path, &mutated);

        let dirty = verify_project_store(&generation).unwrap();
        assert!(!dirty.ok);
        assert_eq!(dirty.content_addressed_objects.objects_checked, 1);
        assert_eq!(dirty.content_addressed_objects.objects_failed, 1);

        // Verify never mutates, repairs, or removes anything it examines:
        // re-running it reports the exact same corruption, not a cleaned-up
        // store, and the bytes on disk are still the corrupted ones.
        let rechecked = verify_project_store(&generation).unwrap();
        assert_eq!(rechecked, dirty);
        assert_eq!(std::fs::read(&object_path).unwrap(), mutated);
    }
}
