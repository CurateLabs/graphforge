//! Authenticated single-file reads from a portable package.
//!
//! [`PortablePackageIndex`] runs the storage-owned package scan once (the same
//! scan [`super::verify_portable`] uses, for bundles and expanded directories),
//! so every entry it can serve is already length- and SHA-256-verified against
//! the authenticated manifest. Reads then reuse the scanner's bounded,
//! no-follow, identity-checked entry reader; there is no second package parser.
//! A participant is located through the runtime map (`capability_id` and
//! `record_family_id` to `participant_id` to its component file), never by path
//! convention.

use super::{
    AtomicBool, ControlSha256, Digest, Entry, MANIFEST_PATH, Manifest, Path, PortableError,
    PortableErrorCode, PortableLimits, PortableMode, RUNTIME_MAP_PATH, Reopen, authenticated_entries,
    constant_time_eq, decode_runtime_map, hex, parse_manifest, preflight, read_entry_bytes,
    scan_entries,
};
use graphforge_core::portable::PortableParticipantId;
use std::collections::BTreeMap;
use std::path::PathBuf;

/// One package file as listed by the authenticated manifest.
///
/// The `(path, length, sha256)` triple is the whole authentication claim:
/// [`PortablePackageIndex::read`] serves bytes only when the manifest lists
/// exactly this triple.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PortableFileRef {
    /// Package-relative path.
    pub path: String,
    /// Exact byte length listed by the manifest.
    pub length: u64,
    /// Lowercase hexadecimal SHA-256 listed by the manifest.
    pub sha256: String,
}

/// A fully scanned package that serves bounded, digest-verified file reads.
///
/// Opening scans (and thereby authenticates) every entry once and binds the
/// result to an expected semantic package digest, so a package swapped after a
/// prior verification cannot be read through a stale expectation.
pub struct PortablePackageIndex {
    source: PathBuf,
    entries: Vec<Entry>,
    manifest: Manifest,
    limits: PortableLimits,
}

impl std::fmt::Debug for PortablePackageIndex {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PortablePackageIndex")
            .field("package_digest", &self.manifest.package_digest)
            .field("entries", &self.entries.len())
            .finish_non_exhaustive()
    }
}

impl PortablePackageIndex {
    /// Scan a bundle or expanded package without following symlinks.
    ///
    /// # Errors
    /// Returns the verifier's structured error for any structural, bound,
    /// digest, or concurrent-mutation failure, and `DigestMismatch` when the
    /// scanned package digest differs from `expected_package_digest`.
    pub fn open(
        source: &Path,
        expected_package_digest: &str,
        limits: PortableLimits,
        cancelled: Option<&AtomicBool>,
    ) -> Result<Self, PortableError> {
        preflight(source, limits, cancelled)?;
        let (report, entries) = scan_entries(
            source,
            PortableMode::StructureOnly,
            limits,
            cancelled,
            None,
            None,
        )?;
        if !constant_time_eq(
            report.package_digest.as_bytes(),
            expected_package_digest.as_bytes(),
        ) {
            return Err(PortableError::at(
                PortableErrorCode::DigestMismatch,
                MANIFEST_PATH,
                "package digest differs from the expected package",
            ));
        }
        let (manifest, _) = parse_manifest(&read_entry_bytes(&entries, MANIFEST_PATH)?, limits)?;
        Ok(Self {
            source: source.to_path_buf(),
            entries,
            manifest,
            limits,
        })
    }

    fn reopen(&self) -> Reopen<'_> {
        if self.entries.iter().any(|entry| entry.offset.is_some()) {
            Reopen::Indexed(&self.source)
        } else {
            Reopen::Directory(&self.source)
        }
    }

    /// Count the authenticated manifest's components by `kind`.
    ///
    /// Only kinds with at least one component appear.
    #[must_use]
    pub fn component_kind_counts(&self) -> BTreeMap<String, u64> {
        let mut counts = BTreeMap::new();
        for component in &self.manifest.components {
            *counts.entry(component.kind.clone()).or_insert(0) += 1;
        }
        counts
    }

    /// Locate the single file of a participant through the runtime map.
    ///
    /// Returns `Ok(None)` when the package has no runtime map or the map does
    /// not name the participant.
    ///
    /// # Errors
    /// Returns `InvalidStructure` when the participant is named twice or is not
    /// exactly one manifest-listed file.
    pub fn participant_file(
        &self,
        participant: &PortableParticipantId,
    ) -> Result<Option<PortableFileRef>, PortableError> {
        let Some(map_entry) = self
            .entries
            .iter()
            .find(|entry| entry.path == RUNTIME_MAP_PATH)
        else {
            return Ok(None);
        };
        let map_bytes = map_entry.bytes.as_deref().ok_or_else(|| {
            PortableError::at(
                PortableErrorCode::InvalidStructure,
                RUNTIME_MAP_PATH,
                "runtime map bytes unavailable",
            )
        })?;
        let (_, runtime) = decode_runtime_map(map_bytes)?;
        let mut named = runtime.participants.iter().filter(|candidate| {
            candidate.capability_id == participant.capability_id
                && candidate.record_family_id == participant.record_family_id
        });
        let Some(mapped) = named.next() else {
            return Ok(None);
        };
        if named.next().is_some() {
            return Err(PortableError::at(
                PortableErrorCode::InvalidStructure,
                RUNTIME_MAP_PATH,
                "participant identity is named more than once",
            ));
        }
        let component = self
            .manifest
            .components
            .iter()
            .find(|component| component.participant_id == mapped.participant_id)
            .ok_or_else(|| {
                PortableError::at(
                    PortableErrorCode::InvalidStructure,
                    RUNTIME_MAP_PATH,
                    "participant has no manifest component",
                )
            })?;
        let [file] = component.files.as_slice() else {
            return Err(PortableError::at(
                PortableErrorCode::InvalidStructure,
                RUNTIME_MAP_PATH,
                "participant is not exactly one file",
            ));
        };
        Ok(Some(PortableFileRef {
            path: file.path.clone(),
            length: file.length,
            sha256: file.sha256.clone(),
        }))
    }

    /// Read one manifest-listed file, bounded by `max_bytes` and the package
    /// limits, and verify its length and SHA-256 against `file`.
    ///
    /// # Errors
    /// Returns `InvalidStructure` when the manifest does not list `file`
    /// exactly, `LimitExceeded` when it exceeds the bound, `DigestMismatch`
    /// when the bytes disagree with the listed length or digest, and
    /// `ConcurrentMutation` when the package changed since it was scanned.
    pub fn read(
        &self,
        file: &PortableFileRef,
        max_bytes: u64,
        cancelled: Option<&AtomicBool>,
    ) -> Result<Vec<u8>, PortableError> {
        let listed = self
            .manifest
            .components
            .iter()
            .flat_map(|component| &component.files)
            .any(|candidate| {
                candidate.path == file.path
                    && candidate.length == file.length
                    && candidate.sha256 == file.sha256
            });
        if !listed {
            return Err(PortableError::at(
                PortableErrorCode::InvalidStructure,
                &file.path,
                "file is not listed by the manifest",
            ));
        }
        let bound = max_bytes.min(self.limits.max_entry_bytes);
        if file.length > bound {
            return Err(PortableError::at(
                PortableErrorCode::LimitExceeded,
                &file.path,
                "file exceeds the read bound",
            ));
        }
        let bytes =
            authenticated_entries::read(self.reopen(), &self.entries, &file.path, bound, cancelled)?;
        let digest = hex(&<[u8; 32]>::from(ControlSha256::digest(&bytes)));
        if bytes.len() as u64 != file.length
            || !constant_time_eq(digest.as_bytes(), file.sha256.as_bytes())
        {
            return Err(PortableError::at(
                PortableErrorCode::DigestMismatch,
                &file.path,
                "file length or digest differs from the manifest",
            ));
        }
        Ok(bytes)
    }
}
