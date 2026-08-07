//! Resolution of the one committed immutable project generation.
//!
//! `CURRENT` is the only commit authority. This module deliberately does not
//! enumerate `generations/`, inspect transaction journals, or decode any
//! participant table.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

#[cfg(windows)]
use std::sync::{Condvar, Mutex, OnceLock};

use atomicwrites::{AllowOverwrite, AtomicFile};
use graphforge_core::{GfError, ProjectErrorCode};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::project_failpoint;

/// Immutable project-format marker path.
pub const FORMAT_FILE: &str = "FORMAT";
/// Sole committed-generation pointer path.
pub const CURRENT_FILE: &str = "CURRENT";
/// Exact bytes accepted for a v0.5 project container.
pub const PROJECT_FORMAT_BYTES: &[u8] = b"graphforge-project/v1\n";

const MAX_FORMAT_BYTES: u64 = 64;
const MAX_CURRENT_BYTES: u64 = 1_024;
const MAX_MANIFEST_BYTES: u64 = 4 * 1024 * 1024;
const MANIFEST_FILE: &str = "manifest.json";
const LEASE_FILE: &str = "lease.lock";
const PARTICIPANTS_DIR: &str = "participants";
const MAX_PARTICIPANT_INVENTORY_ENTRIES: usize = 100_000;

/// One capability declared by the committed generation manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectCapabilityDescriptor {
    /// Stable lowercase machine identifier.
    pub capability_id: String,
    /// Positive capability contract version.
    pub capability_version: u32,
}

/// One verified immutable participant copied from a committed generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectParticipantSnapshot {
    /// Stable capability ID.
    pub capability_id: String,
    /// Capability contract version.
    pub capability_version: u32,
    /// Stable record-family ID.
    pub record_family_id: String,
    /// Record contract version.
    pub record_version: u32,
    /// Persisted encoding (`parquet`, `arrow`, or `json`).
    pub encoding: String,
    /// Canonical schema fingerprint.
    pub schema_fingerprint: [u8; 32],
    /// Logical row count.
    pub row_count: u64,
    /// Exact verified persisted bytes.
    pub bytes: Vec<u8>,
}

/// Manifest-only participant identity used by bounded checkpoint summaries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectParticipantDescriptor {
    /// Owning capability ID.
    pub capability_id: String,
    /// Owning capability contract version.
    pub capability_version: u32,
    /// Stable record-family ID.
    pub record_family_id: String,
    /// Record contract version.
    pub record_version: u32,
    /// Persisted encoding.
    pub encoding: String,
    /// Canonical schema fingerprint.
    pub schema_fingerprint: [u8; 32],
    /// Logical row count.
    pub row_count: u64,
    /// Exact persisted content digest.
    pub content_sha256: [u8; 32],
}

/// A validated, lifetime-pinned view of one committed project generation.
///
/// The retained lease handle gives publication/cleanup work a stable file
/// identity to lock. The lock protocol itself lands with atomic publication
/// and recovery; resolution never follows `CURRENT` again after construction.
#[derive(Debug, Clone)]
pub struct ResolvedProjectGeneration {
    container_root: PathBuf,
    generation_uuid: Uuid,
    generation_root: PathBuf,
    manifest_sha256: [u8; 32],
    manifest: Arc<GenerationManifest>,
    _lease_handle: Arc<GenerationLease>,
}

#[derive(Debug)]
struct GenerationLease(File);

impl Drop for GenerationLease {
    fn drop(&mut self) {
        let _ = crate::file_lock::unlock(&self.0);
    }
}

impl ResolvedProjectGeneration {
    /// Canonical project container root used for this resolution.
    #[must_use]
    pub fn container_root(&self) -> &Path {
        &self.container_root
    }

    /// UUID named by the validated `CURRENT` record.
    #[must_use]
    pub const fn generation_uuid(&self) -> Uuid {
        self.generation_uuid
    }

    /// Immutable selected generation root.
    #[must_use]
    pub fn generation_root(&self) -> &Path {
        &self.generation_root
    }

    /// Root beneath which every generation participant is resolved.
    #[must_use]
    pub fn participants_root(&self) -> PathBuf {
        self.generation_root.join(PARTICIPANTS_DIR)
    }

    /// SHA-256 over the exact selected `manifest.json` bytes.
    #[must_use]
    pub const fn manifest_sha256(&self) -> [u8; 32] {
        self.manifest_sha256
    }

    /// Return the manifest-declared capabilities in canonical ID order.
    ///
    /// This reads no participant files and does not infer capabilities from
    /// directory contents.
    #[must_use]
    pub fn capabilities(&self) -> Vec<ProjectCapabilityDescriptor> {
        self.manifest
            .capabilities
            .iter()
            .map(|capability| ProjectCapabilityDescriptor {
                capability_id: capability.capability_id.clone(),
                capability_version: capability.capability_version,
            })
            .collect()
    }

    /// Look up one manifest-declared capability without opening participants.
    ///
    /// # Errors
    /// Returns `GF_PROJECT_CORRUPT` for an invalid machine identifier.
    pub fn capability(
        &self,
        capability_id: &str,
    ) -> Result<Option<ProjectCapabilityDescriptor>, GfError> {
        validate_machine_id(capability_id)?;
        Ok(self
            .manifest
            .capabilities
            .binary_search_by(|entry| entry.capability_id.as_str().cmp(capability_id))
            .ok()
            .map(|index| {
                let capability = &self.manifest.capabilities[index];
                ProjectCapabilityDescriptor {
                    capability_id: capability.capability_id.clone(),
                    capability_version: capability.capability_version,
                }
            }))
    }

    /// Require one exact capability version before a domain opens its files.
    ///
    /// This consults only the committed manifest. Domain APIs call it before
    /// resolving or decoding any capability participant.
    ///
    /// # Errors
    /// Returns `GF_CAPABILITY_DISABLED` when absent and
    /// `GF_UNSUPPORTED_CAPABILITY_VERSION` for any other declared version.
    pub fn require_capability(
        &self,
        capability_id: &str,
        supported_version: u32,
    ) -> Result<ProjectCapabilityDescriptor, GfError> {
        let Some(capability) = self.capability(capability_id)? else {
            return Err(project_error(
                ProjectErrorCode::CapabilityDisabled,
                format!("capability {capability_id} is not enabled"),
            ));
        };
        if capability.capability_version != supported_version {
            return Err(project_error(
                ProjectErrorCode::UnsupportedCapabilityVersion,
                format!(
                    "capability {capability_id}@{} is not supported",
                    capability.capability_version
                ),
            ));
        }
        Ok(capability)
    }

    /// UUID of the selected generation's verified parent, when present.
    #[must_use]
    pub fn parent_generation_uuid(&self) -> Option<Uuid> {
        self.manifest
            .parent_generation_uuid
            .as_deref()
            .and_then(|value| Uuid::parse_str(value).ok())
    }

    /// Resolve a requested participant path without accepting caller path
    /// components or parsing any participant bytes.
    ///
    /// # Errors
    /// Returns `GF_PROJECT_CORRUPT` if the manifest path is not a normalized,
    /// contained, non-link path or if the requested descriptor is absent.
    pub fn participant_path(
        &self,
        capability_id: &str,
        record_family_id: &str,
    ) -> Result<PathBuf, GfError> {
        validate_machine_id(capability_id)?;
        validate_machine_id(record_family_id)?;
        let descriptor = self
            .manifest
            .participants
            .iter()
            .find(|entry| {
                entry.capability_id == capability_id && entry.record_family_id == record_family_id
            })
            .ok_or_else(|| corrupt("requested participant is absent from generation manifest"))?;
        let relative = Path::new(&descriptor.relative_path);
        validate_relative_path(relative)?;
        let participants_root = self.participants_root();
        let candidate = participants_root.join(relative);
        reject_link_components(&participants_root, relative)?;
        Ok(candidate)
    }

    /// Read and verify every participant in canonical manifest order.
    ///
    /// This is an explicit write-orchestration operation used to carry
    /// unchanged participants into a complete replacement generation. Normal
    /// graph opens and capability inspection never call it.
    ///
    /// # Errors
    /// Returns `GF_PROJECT_CORRUPT` when a participant is missing, linked,
    /// oversized relative to its manifest, or fails its exact content digest.
    pub fn participant_snapshots(&self) -> Result<Vec<ProjectParticipantSnapshot>, GfError> {
        let mut snapshots = Vec::with_capacity(self.manifest.participants.len());
        for descriptor in &self.manifest.participants {
            snapshots.push(self.read_participant_snapshot(descriptor)?);
        }
        Ok(snapshots)
    }

    /// Return the canonical manifest inventory without opening participant bytes.
    ///
    /// # Errors
    /// Returns `GF_PROJECT_CORRUPT` if a committed digest is malformed.
    pub fn participant_descriptors(&self) -> Result<Vec<ProjectParticipantDescriptor>, GfError> {
        self.manifest
            .participants
            .iter()
            .map(|entry| {
                Ok(ProjectParticipantDescriptor {
                    capability_id: entry.capability_id.clone(),
                    capability_version: entry.capability_version,
                    record_family_id: entry.record_family_id.clone(),
                    record_version: entry.record_version,
                    encoding: entry.encoding.clone(),
                    schema_fingerprint: parse_sha256(&entry.schema_fingerprint)?,
                    row_count: entry.row_count,
                    content_sha256: parse_sha256(&entry.content_sha256)?,
                })
            })
            .collect()
    }

    /// Read and verify one requested participant without opening any sibling
    /// capability or record family.
    ///
    /// # Errors
    /// Returns `GF_PROJECT_CORRUPT` when the requested participant exists but
    /// is missing, linked, oversized, or fails its exact content digest.
    pub fn participant_snapshot(
        &self,
        capability_id: &str,
        record_family_id: &str,
    ) -> Result<Option<ProjectParticipantSnapshot>, GfError> {
        validate_machine_id(capability_id)?;
        validate_machine_id(record_family_id)?;
        self.manifest
            .participants
            .iter()
            .find(|entry| {
                entry.capability_id == capability_id && entry.record_family_id == record_family_id
            })
            .map(|descriptor| self.read_participant_snapshot(descriptor))
            .transpose()
    }

    fn read_participant_snapshot(
        &self,
        descriptor: &ParticipantDescriptor,
    ) -> Result<ProjectParticipantSnapshot, GfError> {
        let path =
            self.participant_path(&descriptor.capability_id, &descriptor.record_family_id)?;
        let bytes = read_exact_participant(&path, descriptor.byte_length)?;
        let digest: [u8; 32] = Sha256::digest(&bytes).into();
        if digest != parse_sha256(&descriptor.content_sha256)? {
            return Err(corrupt(
                "participant content digest does not match manifest",
            ));
        }
        Ok(ProjectParticipantSnapshot {
            capability_id: descriptor.capability_id.clone(),
            capability_version: descriptor.capability_version,
            record_family_id: descriptor.record_family_id.clone(),
            record_version: descriptor.record_version,
            encoding: descriptor.encoding.clone(),
            schema_fingerprint: parse_sha256(&descriptor.schema_fingerprint)?,
            row_count: descriptor.row_count,
            bytes,
        })
    }

    /// Prove that the physical participant tree exactly matches the manifest.
    ///
    /// Capability transitions call this before copying a complete generation,
    /// so untracked graph files can never be silently dropped. This bounded
    /// scan is an explicit write precondition; normal opens and capability
    /// reads remain manifest-only.
    ///
    /// # Errors
    /// Returns `GF_TRANSACTION_FAILED` for an untracked, missing, linked,
    /// non-UTF-8, or excessively large inventory.
    pub fn validate_complete_participant_inventory(&self) -> Result<(), GfError> {
        let expected = self
            .manifest
            .participants
            .iter()
            .map(|entry| entry.relative_path.clone())
            .collect::<BTreeSet<_>>();
        let root = self.participants_root();
        let mut directories = vec![root.clone()];
        let mut observed = BTreeSet::new();
        let mut entry_count = 0_usize;
        while let Some(directory) = directories.pop() {
            for entry in std::fs::read_dir(&directory)
                .map_err(|_| transaction_failed("participant inventory cannot be read"))?
            {
                entry_count = entry_count.saturating_add(1);
                if entry_count > MAX_PARTICIPANT_INVENTORY_ENTRIES {
                    return Err(transaction_failed(
                        "participant inventory exceeds the entry limit",
                    ));
                }
                let entry =
                    entry.map_err(|_| transaction_failed("participant entry cannot be read"))?;
                let file_type = entry
                    .file_type()
                    .map_err(|_| transaction_failed("participant type cannot be read"))?;
                if file_type.is_symlink() {
                    return Err(transaction_failed("participant inventory contains a link"));
                }
                let path = entry.path();
                if file_type.is_dir() {
                    directories.push(path);
                } else if file_type.is_file() {
                    let relative = path
                        .strip_prefix(&root)
                        .map_err(|_| transaction_failed("participant path is not contained"))?;
                    validate_relative_path(relative)
                        .map_err(|_| transaction_failed("participant path is invalid"))?;
                    let relative = relative
                        .to_str()
                        .ok_or_else(|| transaction_failed("participant path is not UTF-8"))?
                        .replace(std::path::MAIN_SEPARATOR, "/");
                    observed.insert(relative);
                } else {
                    return Err(transaction_failed(
                        "participant inventory contains a special file",
                    ));
                }
            }
        }
        if observed != expected {
            return Err(transaction_failed(
                "participant inventory does not match the committed manifest",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CurrentRecord {
    format: String,
    format_version: u32,
    generation_uuid: String,
    generation_manifest_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GenerationManifest {
    format: String,
    format_version: u32,
    generation_uuid: String,
    parent_generation_uuid: Option<String>,
    transaction_uuid: String,
    capabilities: Vec<CapabilityDescriptor>,
    participants: Vec<ParticipantDescriptor>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CapabilityDescriptor {
    capability_id: String,
    capability_version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ParticipantDescriptor {
    capability_id: String,
    capability_version: u32,
    record_family_id: String,
    record_version: u32,
    relative_path: String,
    encoding: String,
    byte_length: u64,
    row_count: u64,
    schema_fingerprint: String,
    content_sha256: String,
}

/// Resolve exactly the generation named by `CURRENT`.
///
/// This performs bounded reads of `FORMAT`, `CURRENT`, and the selected
/// generation manifest only. It never enumerates directories or opens
/// participant data.
///
/// # Errors
/// Returns a stable project-format error for unsupported, uninitialized, or
/// corrupt project roots.
pub fn resolve_project_generation(
    container_root: impl AsRef<Path>,
) -> Result<ResolvedProjectGeneration, GfError> {
    let supplied_root = container_root.as_ref();
    reject_root_link(supplied_root)?;
    let root = supplied_root
        .canonicalize()
        .map_err(|_| unsupported("project root does not exist or is inaccessible"))?;

    let format_path = root.join(FORMAT_FILE);
    let format_bytes = read_bounded_regular_file(&format_path, MAX_FORMAT_BYTES)
        .map_err(|_| unsupported("project root does not contain the supported FORMAT marker"))?;
    if format_bytes != PROJECT_FORMAT_BYTES {
        return Err(unsupported("project FORMAT marker is not supported"));
    }

    let current_path = root.join(CURRENT_FILE);
    loop {
        if !current_path.exists() {
            return Err(project_error(
                ProjectErrorCode::ProjectUninitialized,
                "project has no committed generation",
            ));
        }
        let current_bytes = read_bounded_regular_file(&current_path, MAX_CURRENT_BYTES)
            .map_err(|_| corrupt("CURRENT is missing, linked, oversized, or unreadable"))?;
        let current: CurrentRecord = parse_canonical_json_line(&current_bytes, "CURRENT")?;
        validate_format(&current.format, current.format_version)?;
        let generation_uuid = parse_canonical_uuid(&current.generation_uuid)?;
        let expected_manifest_digest = parse_sha256(&current.generation_manifest_sha256)?;

        let generations_dir = root.join("generations");
        reject_exact_directory(&generations_dir)?;
        let selected_dir = generations_dir.join(generation_uuid.hyphenated().to_string());
        reject_exact_directory(&selected_dir)?;
        let lease_path = selected_dir.join(LEASE_FILE);
        let lease = acquire_generation_lease(&lease_path)?;

        // A cleanup may have raced between pointer resolution and lease
        // acquisition. Only a byte-identical reread can pin this generation.
        let confirmed_current = read_bounded_regular_file(&current_path, MAX_CURRENT_BYTES)
            .map_err(|_| corrupt("CURRENT changed to an invalid record during open"))?;
        if confirmed_current != current_bytes {
            continue;
        }
        reject_exact_directory(&selected_dir)?;

        let manifest_path = selected_dir.join(MANIFEST_FILE);
        let manifest_bytes = read_bounded_regular_file(&manifest_path, MAX_MANIFEST_BYTES)
            .map_err(|_| corrupt("selected generation manifest is missing or invalid"))?;
        let actual_digest: [u8; 32] = Sha256::digest(&manifest_bytes).into();
        if actual_digest != expected_manifest_digest {
            return Err(corrupt(
                "selected generation manifest digest does not match CURRENT",
            ));
        }
        let manifest: GenerationManifest =
            parse_canonical_json_line(&manifest_bytes, "generation manifest")?;
        validate_manifest(&manifest, generation_uuid)?;
        reject_exact_directory(&selected_dir.join(PARTICIPANTS_DIR))?;

        return Ok(ResolvedProjectGeneration {
            container_root: root,
            generation_uuid,
            generation_root: selected_dir,
            manifest_sha256: actual_digest,
            manifest: Arc::new(manifest),
            _lease_handle: Arc::new(lease),
        });
    }
}

pub(crate) fn resolve_verified_generation(
    container_root: &Path,
    generation_uuid: Uuid,
    expected_manifest_digest: [u8; 32],
) -> Result<ResolvedProjectGeneration, GfError> {
    reject_root_link(container_root)?;
    let root = container_root
        .canonicalize()
        .map_err(|_| unsupported("project root does not exist or is inaccessible"))?;
    let format_bytes = read_bounded_regular_file(&root.join(FORMAT_FILE), MAX_FORMAT_BYTES)
        .map_err(|_| unsupported("project root does not contain the supported FORMAT marker"))?;
    if format_bytes != PROJECT_FORMAT_BYTES {
        return Err(unsupported("project FORMAT marker is not supported"));
    }
    let generations_dir = root.join("generations");
    reject_exact_directory(&generations_dir)?;
    let selected_dir = generations_dir.join(generation_uuid.hyphenated().to_string());
    reject_exact_directory(&selected_dir)?;
    let lease = acquire_generation_lease(&selected_dir.join(LEASE_FILE))?;
    reject_exact_directory(&selected_dir)?;
    let manifest_bytes =
        read_bounded_regular_file(&selected_dir.join(MANIFEST_FILE), MAX_MANIFEST_BYTES)
            .map_err(|_| corrupt("selected generation manifest is missing or invalid"))?;
    let actual_digest: [u8; 32] = Sha256::digest(&manifest_bytes).into();
    if actual_digest != expected_manifest_digest {
        return Err(corrupt(
            "selected generation manifest digest does not match checkpoint",
        ));
    }
    let manifest: GenerationManifest =
        parse_canonical_json_line(&manifest_bytes, "generation manifest")?;
    validate_manifest(&manifest, generation_uuid)?;
    reject_exact_directory(&selected_dir.join(PARTICIPANTS_DIR))?;
    Ok(ResolvedProjectGeneration {
        container_root: root,
        generation_uuid,
        generation_root: selected_dir,
        manifest_sha256: actual_digest,
        manifest: Arc::new(manifest),
        _lease_handle: Arc::new(lease),
    })
}

/// Open a supported project or create the first committed generation in an
/// explicitly empty directory.
///
/// The project-root directory itself is locked exclusively across the
/// empty-root decision and initial publication. This does not add a file to an
/// unsupported root, and concurrent first openers therefore converge on the
/// generation published by the lock winner.
///
/// # Errors
/// Returns `GF_UNSUPPORTED_PROJECT_FORMAT` without mutation when the directory
/// is not empty, or a storage error if creating the new container fails.
pub fn open_or_initialize_project(
    container_root: impl AsRef<Path>,
) -> Result<ResolvedProjectGeneration, GfError> {
    let root = container_root.as_ref();
    reject_root_link(root)?;
    let _root_lock = lock_project_root(root)?;
    let mut entries = std::fs::read_dir(root).map_err(|error| {
        GfError::Storage(format!("failed to inspect new project root: {error}"))
    })?;
    if entries
        .next()
        .transpose()
        .map_err(|error| GfError::Storage(format!("failed to inspect new project root: {error}")))?
        .is_some()
    {
        return match resolve_project_generation(root) {
            Err(error) if error.code() == "GF_PROJECT_UNINITIALIZED" => {
                let generation_uuid = validate_resumable_uninitialized_layout(root)?;
                initialize_empty_generation(root, false, Some(generation_uuid))
            }
            result => result,
        };
    }
    initialize_empty_generation(root, true, None)
}

fn validate_resumable_uninitialized_layout(root: &Path) -> Result<Uuid, GfError> {
    let mut root_entries = std::fs::read_dir(root)
        .map_err(|_| unsupported("uninitialized project layout cannot be inspected"))?;
    let mut root_count = 0_usize;
    for entry in &mut root_entries {
        let entry = entry.map_err(|_| unsupported("uninitialized project layout is unreadable"))?;
        let name = entry.file_name();
        if crate::project_publication::cleanup_atomicwrite_temp(&entry.path())? {
            continue;
        }
        root_count += 1;
        if root_count > 2 {
            return Err(unsupported(
                "uninitialized project contains unknown root entries",
            ));
        }
        if name != FORMAT_FILE && name != "generations" {
            return Err(unsupported(
                "uninitialized project contains unknown root entries",
            ));
        }
    }
    let generations = root.join("generations");
    reject_exact_directory(&generations)
        .map_err(|_| unsupported("uninitialized generations directory is invalid"))?;
    let mut generation_count = 0_usize;
    let mut generation_uuids = Vec::new();
    for entry in std::fs::read_dir(&generations)
        .map_err(|_| unsupported("uninitialized generations cannot be inspected"))?
    {
        generation_count += 1;
        if generation_count > 16 {
            return Err(unsupported(
                "uninitialized project contains too many private generations",
            ));
        }
        let entry =
            entry.map_err(|_| unsupported("uninitialized generation entry is unreadable"))?;
        let name = entry
            .file_name()
            .to_str()
            .map(str::to_owned)
            .ok_or_else(|| unsupported("uninitialized generation UUID is invalid"))?;
        let generation_uuid = parse_canonical_uuid(&name)
            .map_err(|_| unsupported("uninitialized generation UUID is invalid"))?;
        validate_partial_generation(&entry.path())?;
        generation_uuids.push(generation_uuid);
    }
    generation_uuids.sort_unstable();
    let generation_uuid = generation_uuids
        .pop()
        .ok_or_else(|| unsupported("uninitialized project has no private generation"))?;
    for abandoned_uuid in generation_uuids {
        remove_partial_generation(&generations, abandoned_uuid)?;
    }
    reset_partial_generation(&generations, generation_uuid)?;
    sync_directory(&generations)?;
    Ok(generation_uuid)
}

fn validate_partial_generation(generation: &Path) -> Result<(), GfError> {
    reject_exact_directory(generation)
        .map_err(|_| unsupported("uninitialized generation directory is invalid"))?;
    let mut count = 0_usize;
    for entry in std::fs::read_dir(generation)
        .map_err(|_| unsupported("uninitialized generation cannot be inspected"))?
    {
        count += 1;
        if count > 3 {
            return Err(unsupported(
                "uninitialized generation contains unknown entries",
            ));
        }
        let entry =
            entry.map_err(|_| unsupported("uninitialized generation entry is unreadable"))?;
        let name = entry.file_name();
        if name == PARTICIPANTS_DIR {
            reject_exact_directory(&entry.path())
                .map_err(|_| unsupported("uninitialized participants directory is invalid"))?;
            validate_partial_workspace_participants(&entry.path())?;
        } else if name == LEASE_FILE || name == MANIFEST_FILE {
            let metadata = std::fs::symlink_metadata(entry.path())
                .map_err(|_| unsupported("uninitialized generation file is unreadable"))?;
            if !metadata.is_file() || metadata.file_type().is_symlink() {
                return Err(unsupported("uninitialized generation file is invalid"));
            }
        } else {
            return Err(unsupported(
                "uninitialized generation contains unknown entries",
            ));
        }
    }
    Ok(())
}

fn remove_partial_generation(generations: &Path, generation_uuid: Uuid) -> Result<(), GfError> {
    let generation = generations.join(generation_uuid.hyphenated().to_string());
    reset_partial_generation(generations, generation_uuid)?;
    let participants = generation.join(PARTICIPANTS_DIR);
    if participants.exists() {
        std::fs::remove_dir(&participants).map_err(|error| {
            GfError::Storage(format!(
                "failed to remove interrupted participants directory: {error}"
            ))
        })?;
    }
    std::fs::remove_dir(&generation).map_err(|error| {
        GfError::Storage(format!(
            "failed to remove interrupted generation directory: {error}"
        ))
    })
}

fn reset_partial_generation(generations: &Path, generation_uuid: Uuid) -> Result<(), GfError> {
    let generation = generations.join(generation_uuid.hyphenated().to_string());
    let workspace = generation.join(PARTICIPANTS_DIR).join("workspace");
    if workspace.exists() {
        for family in ["configuration.json", "ontology.json"] {
            let path = workspace.join(family);
            if path.exists() {
                std::fs::remove_file(&path).map_err(|error| {
                    GfError::Storage(format!("failed to reset workspace participant: {error}"))
                })?;
            }
        }
        std::fs::remove_dir(&workspace).map_err(|error| {
            GfError::Storage(format!("failed to reset workspace directory: {error}"))
        })?;
    }
    for name in [LEASE_FILE, MANIFEST_FILE] {
        let path = generation.join(name);
        if path.exists() {
            std::fs::remove_file(&path).map_err(|error| {
                GfError::Storage(format!("failed to reset interrupted generation: {error}"))
            })?;
        }
    }
    sync_directory(&generation)
}

fn validate_partial_workspace_participants(participants: &Path) -> Result<(), GfError> {
    let entries = std::fs::read_dir(participants)
        .map_err(|_| unsupported("uninitialized participants cannot be inspected"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| unsupported("uninitialized participant entry is unreadable"))?;
    if entries.is_empty() {
        return Ok(());
    }
    if entries.len() != 1 || entries[0].file_name() != "workspace" {
        return Err(unsupported(
            "uninitialized participants contain unknown entries",
        ));
    }
    reject_exact_directory(&entries[0].path())
        .map_err(|_| unsupported("uninitialized workspace directory is invalid"))?;
    let mut names = std::fs::read_dir(entries[0].path())
        .map_err(|_| unsupported("uninitialized workspace cannot be inspected"))?
        .map(|entry| {
            let entry =
                entry.map_err(|_| unsupported("uninitialized workspace entry is unreadable"))?;
            let metadata = std::fs::symlink_metadata(entry.path())
                .map_err(|_| unsupported("uninitialized workspace entry is unreadable"))?;
            if !metadata.is_file() || metadata.file_type().is_symlink() {
                return Err(unsupported("uninitialized workspace entry is invalid"));
            }
            entry
                .file_name()
                .into_string()
                .map_err(|_| unsupported("uninitialized workspace entry name is invalid"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    names.sort();
    if names != ["configuration.json", "ontology.json"] && names != ["configuration.json"] {
        return Err(unsupported(
            "uninitialized workspace contains unknown entries",
        ));
    }
    Ok(())
}

fn initialize_empty_generation(
    root: &Path,
    write_format: bool,
    generation_uuid: Option<Uuid>,
) -> Result<ResolvedProjectGeneration, GfError> {
    let generation_uuid = generation_uuid.unwrap_or_else(Uuid::now_v7);
    let transaction_uuid = Uuid::now_v7();
    let generation_root = root
        .join("generations")
        .join(generation_uuid.hyphenated().to_string());
    let participants_root = generation_root.join(PARTICIPANTS_DIR);
    std::fs::create_dir_all(&participants_root)
        .map_err(|error| GfError::Storage(format!("failed to create project layout: {error}")))?;
    if write_format {
        let format_path = root.join(FORMAT_FILE);
        write_new_synced(&format_path, PROJECT_FORMAT_BYTES, "project format")?;
        project_failpoint::hit(
            "project.after_format_fsync",
            Some(transaction_uuid),
            Some(generation_uuid),
            "FORMAT",
            false,
        )?;
    }
    let participant_descriptors = install_empty_workspace_participants(&participants_root)?;
    sync_directory(&participants_root)?;
    sync_directory(&generation_root)?;
    sync_directory(&root.join("generations"))?;
    sync_directory(root)?;
    if write_format {
        project_failpoint::hit(
            "project.after_container_dir_fsync",
            Some(transaction_uuid),
            Some(generation_uuid),
            "CONTAINER",
            false,
        )?;
    }
    write_new_synced(&generation_root.join(LEASE_FILE), &[], "generation lease")?;
    let manifest = GenerationManifest {
        format: "graphforge-generation".into(),
        format_version: 1,
        generation_uuid: generation_uuid.hyphenated().to_string(),
        parent_generation_uuid: None,
        transaction_uuid: transaction_uuid.hyphenated().to_string(),
        capabilities: vec![
            CapabilityDescriptor {
                capability_id: "graph".into(),
                capability_version: 1,
            },
            CapabilityDescriptor {
                capability_id: "workspace".into(),
                capability_version: 1,
            },
        ],
        participants: participant_descriptors,
    };
    let mut manifest_bytes = serde_json::to_vec(&manifest)
        .map_err(|error| GfError::Storage(format!("failed to encode generation: {error}")))?;
    manifest_bytes.push(b'\n');
    write_new_synced(
        &generation_root.join(MANIFEST_FILE),
        &manifest_bytes,
        "generation",
    )?;
    sync_directory(&generation_root)?;
    sync_directory(&root.join("generations"))?;
    let digest: [u8; 32] = Sha256::digest(&manifest_bytes).into();
    let current = CurrentRecord {
        format: "graphforge-project".into(),
        format_version: 1,
        generation_uuid: generation_uuid.hyphenated().to_string(),
        generation_manifest_sha256: sha256_hex(digest),
    };
    let mut current_bytes = serde_json::to_vec(&current)
        .map_err(|error| GfError::Storage(format!("failed to encode CURRENT: {error}")))?;
    current_bytes.push(b'\n');
    AtomicFile::new(root.join(CURRENT_FILE), AllowOverwrite)
        .write(|file| {
            use std::io::Write as _;

            file.write_all(&current_bytes)?;
            file.sync_all()
        })
        .map_err(|error| GfError::Storage(format!("failed to write CURRENT: {error}")))?;
    sync_directory(root)?;
    resolve_project_generation(root)
}

fn install_empty_workspace_participants(
    participants_root: &Path,
) -> Result<Vec<ParticipantDescriptor>, GfError> {
    let workspace_participants = crate::workspace_participants::empty_workspace_participants()?;
    let workspace_root = participants_root.join("workspace");
    std::fs::create_dir_all(&workspace_root)
        .map_err(|error| GfError::Storage(format!("failed to create project layout: {error}")))?;
    let mut descriptors = Vec::with_capacity(workspace_participants.len());
    for participant in workspace_participants {
        let relative_path = format!(
            "{}/{}.json",
            participant.capability_id, participant.record_family_id
        );
        write_new_synced(
            &participants_root.join(&relative_path),
            &participant.bytes,
            "workspace participant",
        )?;
        descriptors.push(ParticipantDescriptor {
            capability_id: participant.capability_id,
            capability_version: participant.capability_version,
            record_family_id: participant.record_family_id,
            record_version: participant.record_version,
            relative_path,
            encoding: "json".into(),
            byte_length: participant.bytes.len() as u64,
            row_count: participant.row_count,
            schema_fingerprint: sha256_hex(participant.schema_fingerprint),
            content_sha256: sha256_hex(Sha256::digest(&participant.bytes).into()),
        });
    }
    descriptors.sort_by(|left, right| {
        (
            &left.capability_id,
            &left.record_family_id,
            &left.relative_path,
        )
            .cmp(&(
                &right.capability_id,
                &right.record_family_id,
                &right.relative_path,
            ))
    });
    sync_directory(&workspace_root)?;
    Ok(descriptors)
}

fn write_new_synced(path: &Path, bytes: &[u8], name: &str) -> Result<(), GfError> {
    use std::io::Write as _;

    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| GfError::Storage(format!("failed to create {name}: {error}")))?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|error| GfError::Storage(format!("failed to write {name}: {error}")))
}

#[cfg(unix)]
fn lock_project_root(path: &Path) -> Result<File, GfError> {
    let root = File::open(path)
        .map_err(|error| GfError::Storage(format!("failed to open project root: {error}")))?;
    crate::file_lock::lock_exclusive(&root)
        .map_err(|error| GfError::Storage(format!("failed to lock project root: {error}")))?;
    Ok(root)
}

#[cfg(windows)]
static LOCAL_PROJECT_ROOT_LOCKS: OnceLock<(Mutex<BTreeSet<String>>, Condvar)> = OnceLock::new();

#[cfg(windows)]
struct LocalProjectRootLock {
    name: String,
}

#[cfg(windows)]
impl Drop for LocalProjectRootLock {
    fn drop(&mut self) {
        let (active, available) =
            LOCAL_PROJECT_ROOT_LOCKS.get_or_init(|| (Mutex::new(BTreeSet::new()), Condvar::new()));
        let mut active = active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        active.remove(&self.name);
        available.notify_all();
    }
}

#[cfg(windows)]
struct WindowsProjectRootLock {
    kernel: Option<named_lock::NamedLockGuard>,
    _local: LocalProjectRootLock,
}

#[cfg(windows)]
impl Drop for WindowsProjectRootLock {
    fn drop(&mut self) {
        // Release the kernel mutex before waking a compatible initializer in
        // this process. External owners remain fail-fast through `try_lock`.
        drop(self.kernel.take());
    }
}

#[cfg(windows)]
fn acquire_local_project_root_lock(name: &str) -> LocalProjectRootLock {
    let (active, available) =
        LOCAL_PROJECT_ROOT_LOCKS.get_or_init(|| (Mutex::new(BTreeSet::new()), Condvar::new()));
    let mut active = active
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    while active.contains(name) {
        active = available
            .wait(active)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
    }
    active.insert(name.to_owned());
    LocalProjectRootLock {
        name: name.to_owned(),
    }
}

#[cfg(windows)]
fn lock_project_root(path: &Path) -> Result<WindowsProjectRootLock, GfError> {
    let canonical = std::fs::canonicalize(path)
        .map_err(|error| GfError::Storage(format!("failed to lock project root: {error}")))?;
    let identity = canonical.as_os_str().to_string_lossy().to_lowercase();
    let digest: [u8; 32] = Sha256::digest(identity.as_bytes()).into();
    let name = format!("GraphForge.ProjectRoot.{}", sha256_hex(digest));
    let local = acquire_local_project_root_lock(&name);
    let lock = named_lock::NamedLock::create(&name)
        .map_err(|error| GfError::Storage(format!("failed to lock project root: {error}")))?;

    match lock.try_lock() {
        Ok(kernel) => Ok(WindowsProjectRootLock {
            kernel: Some(kernel),
            _local: local,
        }),
        Err(named_lock::Error::WouldBlock) => Err(project_error(
            ProjectErrorCode::WriterBusy,
            "phase=PROJECT_ROOT_LOCK committed=false cause=busy",
        )),
        Err(error) => Err(GfError::Storage(format!(
            "failed to lock project root: {error}"
        ))),
    }
}

#[cfg(all(not(unix), not(windows)))]
fn lock_project_root(_path: &Path) -> Result<File, GfError> {
    Err(GfError::Storage(
        "project-root locks are unsupported on this platform".into(),
    ))
}

fn sync_directory(path: &Path) -> Result<(), GfError> {
    crate::project_publication::sync_directory(path).map_err(|error| match error {
        GfError::Storage(message) => {
            GfError::Storage(format!("failed to sync project directory: {message}"))
        }
        other => other,
    })
}

fn validate_format(format: &str, version: u32) -> Result<(), GfError> {
    if format != "graphforge-project" || version != 1 {
        return Err(unsupported("project format version is not supported"));
    }
    Ok(())
}

fn validate_manifest(manifest: &GenerationManifest, expected: Uuid) -> Result<(), GfError> {
    if manifest.format != "graphforge-generation" || manifest.format_version != 1 {
        return Err(corrupt("generation manifest format is invalid"));
    }
    if parse_canonical_uuid(&manifest.generation_uuid)? != expected {
        return Err(corrupt("generation manifest UUID does not match CURRENT"));
    }
    parse_canonical_uuid(&manifest.transaction_uuid)?;
    if let Some(parent) = &manifest.parent_generation_uuid {
        parse_canonical_uuid(parent)?;
    }
    if !manifest
        .capabilities
        .windows(2)
        .all(|pair| pair[0].capability_id < pair[1].capability_id)
    {
        return Err(corrupt(
            "generation capabilities are not in canonical order",
        ));
    }
    for capability in &manifest.capabilities {
        validate_machine_id(&capability.capability_id)?;
        if capability.capability_version == 0 {
            return Err(corrupt("capability version must be positive"));
        }
    }
    if manifest
        .capabilities
        .binary_search_by(|entry| entry.capability_id.as_str().cmp("graph"))
        .ok()
        .map(|index| manifest.capabilities[index].capability_version)
        != Some(1)
    {
        return Err(corrupt(
            "generation manifest must declare graph capability version 1",
        ));
    }
    if !manifest.participants.windows(2).all(|pair| {
        (
            &pair[0].capability_id,
            &pair[0].record_family_id,
            &pair[0].relative_path,
        ) < (
            &pair[1].capability_id,
            &pair[1].record_family_id,
            &pair[1].relative_path,
        )
    }) {
        return Err(corrupt(
            "generation participants are not in canonical order",
        ));
    }
    for participant in &manifest.participants {
        validate_machine_id(&participant.capability_id)?;
        validate_machine_id(&participant.record_family_id)?;
        if participant.capability_version == 0 || participant.record_version == 0 {
            return Err(corrupt("participant contract versions must be positive"));
        }
        let capability = manifest
            .capabilities
            .binary_search_by(|entry| entry.capability_id.cmp(&participant.capability_id))
            .ok()
            .map(|index| &manifest.capabilities[index])
            .ok_or_else(|| corrupt("participant capability is not declared"))?;
        if capability.capability_version != participant.capability_version {
            return Err(corrupt(
                "participant capability version does not match its declaration",
            ));
        }
        validate_relative_path(Path::new(&participant.relative_path))?;
        parse_sha256(&participant.content_sha256)?;
        parse_sha256(&participant.schema_fingerprint)?;
    }
    Ok(())
}

pub(crate) fn validated_generation_parent(
    root: &Path,
    generation_uuid: Uuid,
) -> Result<Option<Uuid>, GfError> {
    validated_generation_metadata(root, generation_uuid).map(|(parent, _)| parent)
}

pub(crate) fn validated_generation_manifest_sha256(
    root: &Path,
    generation_uuid: Uuid,
) -> Result<[u8; 32], GfError> {
    validated_generation_metadata(root, generation_uuid).map(|(_, digest)| digest)
}

fn validated_generation_metadata(
    root: &Path,
    generation_uuid: Uuid,
) -> Result<(Option<Uuid>, [u8; 32]), GfError> {
    let generation_root = root
        .join("generations")
        .join(generation_uuid.hyphenated().to_string());
    reject_exact_directory(&generation_root)?;
    let manifest_bytes =
        read_bounded_regular_file(&generation_root.join(MANIFEST_FILE), MAX_MANIFEST_BYTES)
            .map_err(|_| corrupt("retained ancestor manifest is missing or invalid"))?;
    let manifest: GenerationManifest =
        parse_canonical_json_line(&manifest_bytes, "retained ancestor manifest")?;
    validate_manifest(&manifest, generation_uuid)?;
    let parent = manifest
        .parent_generation_uuid
        .as_deref()
        .map(parse_canonical_uuid)
        .transpose()?;
    Ok((parent, Sha256::digest(&manifest_bytes).into()))
}

fn parse_canonical_json_line<T>(bytes: &[u8], name: &str) -> Result<T, GfError>
where
    T: serde::de::DeserializeOwned + Serialize,
{
    if !bytes.ends_with(b"\n") || bytes[..bytes.len().saturating_sub(1)].contains(&b'\n') {
        return Err(corrupt(format!("{name} is not one canonical JSON line")));
    }
    let parsed: T =
        serde_json::from_slice(bytes).map_err(|_| corrupt(format!("{name} is invalid JSON")))?;
    let mut canonical =
        serde_json::to_vec(&parsed).map_err(|_| corrupt(format!("{name} cannot be encoded")))?;
    canonical.push(b'\n');
    if canonical != bytes {
        return Err(corrupt(format!("{name} is not canonical JSON")));
    }
    Ok(parsed)
}

fn parse_canonical_uuid(value: &str) -> Result<Uuid, GfError> {
    let parsed = Uuid::parse_str(value).map_err(|_| corrupt("UUID is invalid"))?;
    if parsed.hyphenated().to_string() != value {
        return Err(corrupt("UUID is not lowercase canonical hyphenated form"));
    }
    Ok(parsed)
}

fn parse_sha256(value: &str) -> Result<[u8; 32], GfError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(corrupt("SHA-256 is not 64 lowercase hexadecimal bytes"));
    }
    let mut output = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        output[index] = (hex_nibble(pair[0]) << 4) | hex_nibble(pair[1]);
    }
    Ok(output)
}

const fn hex_nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => 0,
    }
}

fn sha256_hex(bytes: [u8; 32]) -> String {
    bytes
        .iter()
        .fold(String::with_capacity(64), |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to String is infallible");
            output
        })
}

fn validate_machine_id(value: &str) -> Result<(), GfError> {
    if value.is_empty()
        || value.len() > 64
        || !value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
        })
    {
        return Err(corrupt("machine identifier is not lowercase ASCII"));
    }
    Ok(())
}

fn validate_relative_path(path: &Path) -> Result<(), GfError> {
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path.components().any(|component| {
            !matches!(component, Component::Normal(_)) || component.as_os_str().to_str().is_none()
        })
    {
        return Err(corrupt(
            "participant path is not a normalized relative path",
        ));
    }
    Ok(())
}

fn reject_root_link(path: &Path) -> Result<(), GfError> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|_| unsupported("project root does not exist or is inaccessible"))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(unsupported(
            "project root must be a real local directory, not a link",
        ));
    }
    Ok(())
}

fn reject_exact_directory(path: &Path) -> Result<(), GfError> {
    let metadata =
        std::fs::symlink_metadata(path).map_err(|_| corrupt("required directory is missing"))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(corrupt("required directory is linked or not a directory"));
    }
    Ok(())
}

fn reject_link_components(root: &Path, relative: &Path) -> Result<(), GfError> {
    let mut candidate = root.to_path_buf();
    for component in relative.components() {
        let Component::Normal(part) = component else {
            return Err(corrupt("participant path escapes its generation"));
        };
        candidate.push(part);
        let metadata = std::fs::symlink_metadata(&candidate)
            .map_err(|_| corrupt("participant path is missing"))?;
        if metadata.file_type().is_symlink() {
            return Err(corrupt("participant path contains a link"));
        }
    }
    Ok(())
}

fn open_regular_file(path: &Path) -> Result<File, std::io::Error> {
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(std::io::Error::other("not a regular non-link file"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(std::io::Error::other("hard-linked project file"));
        }
    }
    OpenOptions::new().read(true).open(path)
}

fn acquire_generation_lease(path: &Path) -> Result<GenerationLease, GfError> {
    let handle = open_regular_file(path)
        .map_err(|_| corrupt("selected generation lease is missing or invalid"))?;
    crate::file_lock::lock_shared(&handle)
        .map_err(|_| corrupt("selected generation lease cannot be acquired"))?;
    Ok(GenerationLease(handle))
}

fn read_bounded_regular_file(path: &Path, maximum: u64) -> Result<Vec<u8>, std::io::Error> {
    let file = open_regular_file(path)?;
    let metadata = file.metadata()?;
    if metadata.len() > maximum {
        return Err(std::io::Error::other("file exceeds bounded read limit"));
    }
    let capacity = usize::try_from(metadata.len())
        .map_err(|_| std::io::Error::other("file length does not fit address space"))?;
    let mut bytes = Vec::with_capacity(capacity);
    file.take(maximum + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > maximum {
        return Err(std::io::Error::other("file exceeds bounded read limit"));
    }
    Ok(bytes)
}

fn read_exact_participant(path: &Path, expected_length: u64) -> Result<Vec<u8>, GfError> {
    let file = open_regular_file(path)
        .map_err(|_| corrupt("participant is missing, linked, or unreadable"))?;
    let metadata = file
        .metadata()
        .map_err(|_| corrupt("participant metadata is unreadable"))?;
    if metadata.len() != expected_length {
        return Err(corrupt("participant byte length does not match manifest"));
    }
    let capacity = usize::try_from(expected_length)
        .map_err(|_| corrupt("participant byte length exceeds address space"))?;
    let mut bytes = Vec::with_capacity(capacity);
    file.take(expected_length.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|_| corrupt("participant cannot be read"))?;
    if u64::try_from(bytes.len()).ok() != Some(expected_length) {
        return Err(corrupt("participant byte length changed while reading"));
    }
    Ok(bytes)
}

fn unsupported(message: impl Into<String>) -> GfError {
    project_error(ProjectErrorCode::UnsupportedProjectFormat, message)
}

fn corrupt(message: impl Into<String>) -> GfError {
    project_error(ProjectErrorCode::ProjectCorrupt, message)
}

fn transaction_failed(message: impl Into<String>) -> GfError {
    project_error(ProjectErrorCode::TransactionFailed, message)
}

fn project_error(code: ProjectErrorCode, message: impl Into<String>) -> GfError {
    GfError::Project {
        code,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::{Arc, Barrier};

    #[cfg(windows)]
    const ROOT_LOCK_CONTENDER: &str =
        "project_generation::tests::windows_project_root_lock_contender";
    #[cfg(windows)]
    const ROOT_LOCK_ABANDONER: &str =
        "project_generation::tests::windows_project_root_lock_abandoner";

    #[cfg(windows)]
    fn wait_for_lock_subprocess(mut child: std::process::Child, proof: &str) {
        use std::time::Duration;
        use wait_timeout::ChildExt as _;

        let status = child
            .wait_timeout(Duration::from_secs(30))
            .unwrap_or_else(|error| panic!("failed to wait for {proof}: {error}"));
        let Some(status) = status else {
            child.kill().expect("failed to kill timed-out subprocess");
            child.wait().expect("failed to reap timed-out subprocess");
            panic!("{proof} timed out after 30 seconds");
        };
        assert!(status.success(), "{proof} failed with {status}");
    }

    fn canonical_line<T: Serialize>(value: &T) -> Vec<u8> {
        let mut bytes = serde_json::to_vec(value).unwrap();
        bytes.push(b'\n');
        bytes
    }

    fn install_generation(root: &Path, generation_uuid: Uuid) -> [u8; 32] {
        let generation_root = root
            .join("generations")
            .join(generation_uuid.hyphenated().to_string());
        fs::create_dir_all(generation_root.join(PARTICIPANTS_DIR)).unwrap();
        fs::write(generation_root.join(LEASE_FILE), []).unwrap();
        let manifest = GenerationManifest {
            format: "graphforge-generation".into(),
            format_version: 1,
            generation_uuid: generation_uuid.hyphenated().to_string(),
            parent_generation_uuid: None,
            transaction_uuid: Uuid::now_v7().hyphenated().to_string(),
            capabilities: vec![CapabilityDescriptor {
                capability_id: "graph".into(),
                capability_version: 1,
            }],
            participants: vec![],
        };
        let bytes = canonical_line(&manifest);
        fs::write(generation_root.join(MANIFEST_FILE), &bytes).unwrap();
        Sha256::digest(&bytes).into()
    }

    fn write_current(root: &Path, generation_uuid: Uuid, digest: [u8; 32]) {
        let current = CurrentRecord {
            format: "graphforge-project".into(),
            format_version: 1,
            generation_uuid: generation_uuid.hyphenated().to_string(),
            generation_manifest_sha256: sha256_hex(digest),
        };
        fs::write(root.join(CURRENT_FILE), canonical_line(&current)).unwrap();
    }

    fn project() -> (tempfile::TempDir, Uuid) {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join(FORMAT_FILE), PROJECT_FORMAT_BYTES).unwrap();
        fs::create_dir(root.path().join("generations")).unwrap();
        let generation_uuid = Uuid::now_v7();
        let digest = install_generation(root.path(), generation_uuid);
        write_current(root.path(), generation_uuid, digest);
        (root, generation_uuid)
    }

    fn open_generation_lease(root: &Path, generation: Uuid) -> File {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(
                root.join("generations")
                    .join(generation.hyphenated().to_string())
                    .join(LEASE_FILE),
            )
            .unwrap()
    }

    fn assert_code(error: GfError, expected: &str) {
        assert_eq!(error.code(), expected, "{error}");
    }

    #[test]
    fn initial_generation_declares_graph_and_workspace_capabilities() {
        let root = tempfile::tempdir().unwrap();

        let resolved = open_or_initialize_project(root.path()).unwrap();

        assert_eq!(
            resolved.capabilities(),
            vec![
                ProjectCapabilityDescriptor {
                    capability_id: "graph".into(),
                    capability_version: 1,
                },
                ProjectCapabilityDescriptor {
                    capability_id: "workspace".into(),
                    capability_version: 1,
                },
            ]
        );
        let ontology = resolved
            .participant_snapshot("workspace", "ontology")
            .unwrap()
            .unwrap();
        assert_eq!(
            crate::WorkspaceOntology::from_canonical_json(&ontology.bytes)
                .unwrap()
                .mode,
            crate::WorkspaceOntologyMode::None
        );
        let configuration = resolved
            .participant_snapshot("workspace", "configuration")
            .unwrap()
            .unwrap();
        assert_eq!(
            crate::WorkspaceConfiguration::from_canonical_json(&configuration.bytes)
                .unwrap()
                .ontology_mode,
            crate::WorkspaceOntologyMode::None
        );
        assert_eq!(
            resolved.capability("graph").unwrap(),
            Some(ProjectCapabilityDescriptor {
                capability_id: "graph".into(),
                capability_version: 1,
            })
        );
        assert_eq!(resolved.capability("knowledge").unwrap(), None);
        assert_eq!(
            resolved
                .require_capability("knowledge", 1)
                .unwrap_err()
                .code(),
            "GF_CAPABILITY_DISABLED"
        );
        assert_eq!(
            resolved.require_capability("graph", 2).unwrap_err().code(),
            "GF_UNSUPPORTED_CAPABILITY_VERSION"
        );
    }

    #[test]
    fn resolves_only_the_generation_named_by_current() {
        let (root, expected) = project();
        let abandoned = Uuid::now_v7();
        install_generation(root.path(), abandoned);

        let resolved = resolve_project_generation(root.path()).unwrap();

        assert_eq!(resolved.generation_uuid(), expected);
        assert_ne!(resolved.generation_uuid(), abandoned);
    }

    #[test]
    fn concurrent_first_openers_converge_on_one_generation() {
        let root = tempfile::tempdir().unwrap();
        let barrier = Arc::new(Barrier::new(3));
        let mut workers = Vec::new();
        for _ in 0..2 {
            let root = root.path().to_owned();
            let barrier = Arc::clone(&barrier);
            workers.push(std::thread::spawn(move || {
                barrier.wait();
                open_or_initialize_project(root).unwrap().generation_uuid()
            }));
        }

        barrier.wait();
        let first = workers.remove(0).join().unwrap();
        let second = workers.remove(0).join().unwrap();

        assert_eq!(first, second);
        assert_eq!(
            fs::read_dir(root.path().join("generations"))
                .unwrap()
                .count(),
            1
        );
    }

    #[test]
    fn project_root_lock_is_released_for_reopen() {
        let root = tempfile::tempdir().unwrap();

        let first = open_or_initialize_project(root.path()).unwrap();
        let generation = first.generation_uuid();
        let reopened = open_or_initialize_project(root.path()).unwrap();

        assert_eq!(reopened.generation_uuid(), generation);
        assert_eq!(first.generation_uuid(), generation);
    }

    #[cfg(windows)]
    #[test]
    fn windows_project_root_lock_allows_owner_to_inspect_directory() {
        let root = tempfile::tempdir().unwrap();
        let _owner = lock_project_root(root.path()).unwrap();

        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 0);
    }

    #[cfg(windows)]
    #[test]
    fn windows_project_directory_sync_uses_write_capable_handle() {
        let root = tempfile::tempdir().unwrap();

        sync_directory(root.path()).unwrap();
    }

    #[cfg(windows)]
    #[test]
    fn windows_project_root_lock_fails_closed_and_releases() {
        use std::process::Command;

        let root = tempfile::tempdir().unwrap();
        let owner = lock_project_root(root.path()).unwrap();

        let contender = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg(ROOT_LOCK_CONTENDER)
            .arg("--nocapture")
            .env("GRAPHFORGE_TEST_PROJECT_ROOT", root.path())
            .spawn()
            .unwrap();
        wait_for_lock_subprocess(contender, "subprocess contention proof");

        drop(owner);
        open_or_initialize_project(root.path()).unwrap();
    }

    #[cfg(windows)]
    #[test]
    fn windows_project_root_lock_contender() {
        let Ok(root) = std::env::var("GRAPHFORGE_TEST_PROJECT_ROOT") else {
            return;
        };

        let contention = open_or_initialize_project(PathBuf::from(root)).unwrap_err();
        assert_code(contention, "GF_WRITER_BUSY");
    }

    #[cfg(windows)]
    #[test]
    fn windows_project_root_lock_recovers_abandoned_owner() {
        use std::process::Command;

        let root = tempfile::tempdir().unwrap();
        let abandoner = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg(ROOT_LOCK_ABANDONER)
            .arg("--nocapture")
            .env("GRAPHFORGE_TEST_PROJECT_ROOT", root.path())
            .spawn()
            .unwrap();
        wait_for_lock_subprocess(abandoner, "subprocess abandonment proof");

        let _recovered = lock_project_root(root.path()).unwrap();
    }

    #[cfg(windows)]
    #[test]
    fn windows_project_root_lock_abandoner() {
        let Ok(root) = std::env::var("GRAPHFORGE_TEST_PROJECT_ROOT") else {
            return;
        };

        let root = PathBuf::from(root);
        let _owner = lock_project_root(&root).unwrap();
        std::process::exit(0);
    }

    #[test]
    fn repeated_interrupted_initializations_reuse_one_validated_generation() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join(FORMAT_FILE), PROJECT_FORMAT_BYTES).unwrap();
        fs::create_dir(root.path().join("generations")).unwrap();
        let mut generations = Vec::new();
        for _ in 0..16 {
            let generation = Uuid::now_v7();
            install_generation(root.path(), generation);
            generations.push(generation);
        }
        generations.sort_unstable();

        let resolved = open_or_initialize_project(root.path()).unwrap();

        assert_eq!(resolved.generation_uuid(), *generations.last().unwrap());
        assert_eq!(
            fs::read_dir(root.path().join("generations"))
                .unwrap()
                .count(),
            1
        );
    }

    #[test]
    fn interrupted_current_temp_is_removed_before_initialization_resumes() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join(FORMAT_FILE), PROJECT_FORMAT_BYTES).unwrap();
        fs::create_dir(root.path().join("generations")).unwrap();
        install_generation(root.path(), Uuid::now_v7());
        let writer_temp = root.path().join(".atomicwriteAb12Cd");
        fs::create_dir(&writer_temp).unwrap();
        fs::write(writer_temp.join("tmpfile.tmp"), b"partial CURRENT").unwrap();

        open_or_initialize_project(root.path()).unwrap();

        assert!(!writer_temp.exists());
    }

    #[test]
    fn interrupted_initialization_rejects_unknown_layout_entries_without_mutation() {
        fn partial_project() -> (tempfile::TempDir, PathBuf) {
            let root = tempfile::tempdir().unwrap();
            fs::write(root.path().join(FORMAT_FILE), PROJECT_FORMAT_BYTES).unwrap();
            let generation = root
                .path()
                .join("generations")
                .join(Uuid::now_v7().hyphenated().to_string());
            fs::create_dir_all(&generation).unwrap();
            (root, generation)
        }

        let (root, _) = partial_project();
        fs::write(root.path().join("caller-data"), b"preserve").unwrap();
        assert_code(
            open_or_initialize_project(root.path()).unwrap_err(),
            "GF_UNSUPPORTED_PROJECT_FORMAT",
        );
        assert_eq!(
            fs::read(root.path().join("caller-data")).unwrap(),
            b"preserve"
        );

        let (root, generation) = partial_project();
        fs::write(generation.join("caller-data"), b"preserve").unwrap();
        assert_code(
            open_or_initialize_project(root.path()).unwrap_err(),
            "GF_UNSUPPORTED_PROJECT_FORMAT",
        );
        assert_eq!(
            fs::read(generation.join("caller-data")).unwrap(),
            b"preserve"
        );

        let (root, generation) = partial_project();
        let participants = generation.join(PARTICIPANTS_DIR);
        fs::create_dir(&participants).unwrap();
        fs::write(participants.join("unknown"), b"preserve").unwrap();
        assert_code(
            open_or_initialize_project(root.path()).unwrap_err(),
            "GF_UNSUPPORTED_PROJECT_FORMAT",
        );
        assert_eq!(fs::read(participants.join("unknown")).unwrap(), b"preserve");

        let (root, generation) = partial_project();
        let workspace = generation.join(PARTICIPANTS_DIR).join("workspace");
        fs::create_dir_all(&workspace).unwrap();
        fs::write(workspace.join("configuration.json"), b"{}").unwrap();
        fs::write(workspace.join("unknown.json"), b"preserve").unwrap();
        assert_code(
            open_or_initialize_project(root.path()).unwrap_err(),
            "GF_UNSUPPORTED_PROJECT_FORMAT",
        );
        assert_eq!(
            fs::read(workspace.join("unknown.json")).unwrap(),
            b"preserve"
        );
    }

    #[test]
    fn wave9_interrupted_initialization_enforces_bounded_private_generation_layout() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join(FORMAT_FILE), PROJECT_FORMAT_BYTES).unwrap();
        let generations = root.path().join("generations");
        fs::create_dir(&generations).unwrap();
        for _ in 0..17 {
            fs::create_dir(generations.join(Uuid::now_v7().hyphenated().to_string())).unwrap();
        }
        assert_code(
            open_or_initialize_project(root.path()).unwrap_err(),
            "GF_UNSUPPORTED_PROJECT_FORMAT",
        );

        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join(FORMAT_FILE), PROJECT_FORMAT_BYTES).unwrap();
        let generation = root
            .path()
            .join("generations")
            .join(Uuid::now_v7().hyphenated().to_string());
        fs::create_dir_all(&generation).unwrap();
        for name in [PARTICIPANTS_DIR, LEASE_FILE, MANIFEST_FILE] {
            let path = generation.join(name);
            if name == PARTICIPANTS_DIR {
                fs::create_dir(path).unwrap();
            } else {
                fs::write(path, b"partial").unwrap();
            }
        }
        fs::write(generation.join("fourth-entry"), b"caller").unwrap();
        assert_code(
            open_or_initialize_project(root.path()).unwrap_err(),
            "GF_UNSUPPORTED_PROJECT_FORMAT",
        );
        assert_eq!(
            fs::read(generation.join("fourth-entry")).unwrap(),
            b"caller"
        );
    }

    #[cfg(unix)]
    #[test]
    fn wave9_participant_inventory_rejects_links_and_special_files() {
        use std::os::unix::fs::symlink;
        use std::os::unix::net::UnixListener;

        for kind in ["link", "socket"] {
            let root = tempfile::Builder::new()
                .prefix("gf")
                .tempdir_in("/tmp")
                .unwrap();
            let resolved = open_or_initialize_project(root.path()).unwrap();
            let participants = resolved.participants_root();
            let hostile = participants.join(format!("hostile-{kind}"));
            if kind == "link" {
                symlink(root.path().join(CURRENT_FILE), &hostile).unwrap();
            } else {
                let _listener = UnixListener::bind(&hostile).unwrap();
            }
            assert_code(
                resolved
                    .validate_complete_participant_inventory()
                    .unwrap_err(),
                "GF_TRANSACTION_FAILED",
            );
        }
    }

    #[test]
    fn existing_resolution_remains_pinned_after_current_changes() {
        let (root, first) = project();
        let old_reader = resolve_project_generation(root.path()).unwrap();
        let second = Uuid::now_v7();
        let digest = install_generation(root.path(), second);
        write_current(root.path(), second, digest);

        let new_reader = resolve_project_generation(root.path()).unwrap();

        assert_eq!(old_reader.generation_uuid(), first);
        assert_eq!(new_reader.generation_uuid(), second);
        assert_ne!(old_reader.generation_root(), new_reader.generation_root());
    }

    #[test]
    fn resolved_reader_holds_the_generation_lease() {
        let (root, generation) = project();
        let resolved = resolve_project_generation(root.path()).unwrap();
        let lease = open_generation_lease(root.path(), generation);

        assert!(!crate::file_lock::try_lock_exclusive(&lease).unwrap());
        drop(resolved);
        assert!(crate::file_lock::try_lock_exclusive(&lease).unwrap());
    }

    #[test]
    fn cloned_reader_holds_the_generation_lease_until_the_last_drop() {
        let (root, generation) = project();
        let resolved = resolve_project_generation(root.path()).unwrap();
        let cloned = resolved.clone();
        let lease = open_generation_lease(root.path(), generation);

        drop(resolved);
        assert!(!crate::file_lock::try_lock_exclusive(&lease).unwrap());
        drop(cloned);
        assert!(crate::file_lock::try_lock_exclusive(&lease).unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn final_reader_drop_unlocks_a_duplicated_lease_description() {
        let (root, generation) = project();
        let resolved = resolve_project_generation(root.path()).unwrap();
        let duplicated_lease = resolved._lease_handle.0.try_clone().unwrap();
        let lease = open_generation_lease(root.path(), generation);

        assert!(!crate::file_lock::try_lock_exclusive(&lease).unwrap());
        drop(resolved);
        assert!(duplicated_lease.metadata().is_ok());
        assert!(crate::file_lock::try_lock_exclusive(&lease).unwrap());
    }

    #[test]
    fn rejects_pre_v1_root_without_mutation() {
        let root = tempfile::tempdir().unwrap();
        fs::create_dir(root.path().join("topology")).unwrap();
        fs::write(root.path().join("topology/nodes.parquet"), b"old").unwrap();
        let before = fs::read(root.path().join("topology/nodes.parquet")).unwrap();

        let error = resolve_project_generation(root.path()).unwrap_err();

        assert_code(error, "GF_UNSUPPORTED_PROJECT_FORMAT");
        assert_eq!(
            fs::read(root.path().join("topology/nodes.parquet")).unwrap(),
            before
        );
        assert!(!root.path().join(FORMAT_FILE).exists());
    }

    #[test]
    fn exact_format_without_current_is_uninitialized() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join(FORMAT_FILE), PROJECT_FORMAT_BYTES).unwrap();

        let error = resolve_project_generation(root.path()).unwrap_err();

        assert_code(error, "GF_PROJECT_UNINITIALIZED");
    }

    #[test]
    fn future_format_is_unsupported() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join(FORMAT_FILE), b"graphforge-project/v2\n").unwrap();

        let error = resolve_project_generation(root.path()).unwrap_err();

        assert_code(error, "GF_UNSUPPORTED_PROJECT_FORMAT");
    }

    #[test]
    fn noncanonical_current_is_corrupt() {
        let (root, _) = project();
        let bytes = fs::read(root.path().join(CURRENT_FILE)).unwrap();
        let spaced = String::from_utf8(bytes).unwrap().replace("\":\"", "\": \"");
        fs::write(root.path().join(CURRENT_FILE), spaced).unwrap();

        let error = resolve_project_generation(root.path()).unwrap_err();

        assert_code(error, "GF_PROJECT_CORRUPT");
    }

    #[test]
    fn manifest_digest_mismatch_is_corrupt() {
        let (root, generation) = project();
        write_current(root.path(), generation, [0_u8; 32]);

        let error = resolve_project_generation(root.path()).unwrap_err();

        assert_code(error, "GF_PROJECT_CORRUPT");
        let lease = open_generation_lease(root.path(), generation);
        assert!(crate::file_lock::try_lock_exclusive(&lease).unwrap());
    }

    #[test]
    fn participant_path_rejects_traversal_from_manifest() {
        let (root, generation) = project();
        let generation_root = root
            .path()
            .join("generations")
            .join(generation.hyphenated().to_string());
        let manifest = GenerationManifest {
            format: "graphforge-generation".into(),
            format_version: 1,
            generation_uuid: generation.hyphenated().to_string(),
            parent_generation_uuid: None,
            transaction_uuid: Uuid::now_v7().hyphenated().to_string(),
            capabilities: vec![CapabilityDescriptor {
                capability_id: "graph".into(),
                capability_version: 1,
            }],
            participants: vec![ParticipantDescriptor {
                capability_id: "graph".into(),
                capability_version: 1,
                record_family_id: "topology".into(),
                record_version: 1,
                relative_path: "../outside.parquet".into(),
                encoding: "parquet".into(),
                byte_length: 0,
                row_count: 0,
                schema_fingerprint: "0".repeat(64),
                content_sha256: "0".repeat(64),
            }],
        };
        let bytes = canonical_line(&manifest);
        fs::write(generation_root.join(MANIFEST_FILE), &bytes).unwrap();
        write_current(root.path(), generation, Sha256::digest(&bytes).into());

        let error = resolve_project_generation(root.path()).unwrap_err();

        assert_code(error, "GF_PROJECT_CORRUPT");
    }

    #[test]
    fn resolution_does_not_open_unrequested_participant_tables() {
        let (root, generation) = project();
        let generation_root = root
            .path()
            .join("generations")
            .join(generation.hyphenated().to_string());
        let manifest = GenerationManifest {
            format: "graphforge-generation".into(),
            format_version: 1,
            generation_uuid: generation.hyphenated().to_string(),
            parent_generation_uuid: None,
            transaction_uuid: Uuid::now_v7().hyphenated().to_string(),
            capabilities: vec![
                CapabilityDescriptor {
                    capability_id: "graph".into(),
                    capability_version: 1,
                },
                CapabilityDescriptor {
                    capability_id: "knowledge".into(),
                    capability_version: 1,
                },
            ],
            participants: vec![ParticipantDescriptor {
                capability_id: "knowledge".into(),
                capability_version: 1,
                record_family_id: "assertions".into(),
                record_version: 1,
                relative_path: "knowledge/assertions.parquet".into(),
                encoding: "parquet".into(),
                byte_length: 99,
                row_count: 1,
                schema_fingerprint: "0".repeat(64),
                content_sha256: "0".repeat(64),
            }],
        };
        let bytes = canonical_line(&manifest);
        fs::write(generation_root.join(MANIFEST_FILE), &bytes).unwrap();
        write_current(root.path(), generation, Sha256::digest(&bytes).into());
        // The declared table deliberately does not exist. Generic graph-only
        // resolution validates authority, not unrequested capability bytes.
        assert!(
            !generation_root
                .join("participants/knowledge/assertions.parquet")
                .exists()
        );

        let resolved = resolve_project_generation(root.path()).unwrap();

        assert_eq!(resolved.generation_uuid(), generation);
    }

    #[cfg(unix)]
    #[test]
    fn selected_generation_symlink_is_corrupt() {
        use std::os::unix::fs::symlink;

        let (root, generation) = project();
        let generation_root = root
            .path()
            .join("generations")
            .join(generation.hyphenated().to_string());
        fs::remove_file(generation_root.join(LEASE_FILE)).unwrap();
        symlink("/dev/null", generation_root.join(LEASE_FILE)).unwrap();

        let error = resolve_project_generation(root.path()).unwrap_err();

        assert_code(error, "GF_PROJECT_CORRUPT");
    }

    #[test]
    fn canonical_identity_and_manifest_validation_matrix_is_total() {
        let expected = Uuid::now_v7();
        assert_eq!(
            parse_canonical_uuid(&expected.hyphenated().to_string()).unwrap(),
            expected
        );
        for value in [
            "not-a-uuid".to_owned(),
            expected.simple().to_string(),
            expected.hyphenated().to_string().to_uppercase(),
        ] {
            assert_code(
                parse_canonical_uuid(&value).unwrap_err(),
                "GF_PROJECT_CORRUPT",
            );
        }
        assert_eq!(parse_sha256(&"00".repeat(32)).unwrap(), [0; 32]);
        for value in ["00".to_owned(), "AA".repeat(32), "gg".repeat(32)] {
            assert_code(parse_sha256(&value).unwrap_err(), "GF_PROJECT_CORRUPT");
        }
        for value in ["", "Upper", "has/slash", "has space", ".", ".."] {
            assert_code(
                validate_machine_id(value).unwrap_err(),
                "GF_PROJECT_CORRUPT",
            );
        }
        assert!(validate_machine_id("graph_data-1").is_ok());

        let base = GenerationManifest {
            format: "graphforge-generation".into(),
            format_version: 1,
            generation_uuid: expected.hyphenated().to_string(),
            parent_generation_uuid: None,
            transaction_uuid: Uuid::now_v7().hyphenated().to_string(),
            capabilities: vec![CapabilityDescriptor {
                capability_id: "graph".into(),
                capability_version: 1,
            }],
            participants: vec![],
        };
        assert!(validate_manifest(&base, expected).is_ok());
        let mutations: Vec<Box<dyn Fn(&mut GenerationManifest)>> = vec![
            Box::new(|manifest| manifest.format = "future".into()),
            Box::new(|manifest| manifest.format_version = 2),
            Box::new(|manifest| manifest.generation_uuid = Uuid::now_v7().to_string()),
            Box::new(|manifest| manifest.transaction_uuid = "bad".into()),
            Box::new(|manifest| manifest.capabilities[0].capability_id = "Upper".into()),
            Box::new(|manifest| manifest.capabilities[0].capability_version = 0),
            Box::new(|manifest| manifest.capabilities.push(manifest.capabilities[0].clone())),
        ];
        for mutate in mutations {
            let mut manifest = base.clone();
            mutate(&mut manifest);
            assert!(validate_manifest(&manifest, expected).is_err());
        }

        let participant = ParticipantDescriptor {
            capability_id: "graph".into(),
            capability_version: 1,
            record_family_id: "topology".into(),
            record_version: 1,
            relative_path: "topology/nodes.parquet".into(),
            encoding: "parquet".into(),
            byte_length: 0,
            row_count: 0,
            schema_fingerprint: "0".repeat(64),
            content_sha256: "0".repeat(64),
        };
        let participant_mutations: Vec<Box<dyn Fn(&mut ParticipantDescriptor)>> = vec![
            Box::new(|entry| entry.capability_id = "missing".into()),
            Box::new(|entry| entry.capability_version = 2),
            Box::new(|entry| entry.record_family_id = "Upper".into()),
            Box::new(|entry| entry.record_version = 0),
            Box::new(|entry| entry.relative_path = "/absolute".into()),
            Box::new(|entry| entry.relative_path = "../escape".into()),
            Box::new(|entry| entry.schema_fingerprint = "short".into()),
            Box::new(|entry| entry.content_sha256 = "GG".repeat(32)),
        ];
        for mutate in participant_mutations {
            let mut manifest = base.clone();
            let mut entry = participant.clone();
            mutate(&mut entry);
            manifest.participants.push(entry);
            assert!(validate_manifest(&manifest, expected).is_err());
        }

        let mut valid = base.clone();
        valid.participants.push(participant.clone());
        assert!(validate_manifest(&valid, expected).is_ok());
        valid.participants.push(participant);
        assert!(validate_manifest(&valid, expected).is_err());
    }

    #[test]
    fn bounded_regular_file_rejects_missing_directory_and_oversize_without_mutation() {
        let root = tempfile::tempdir().unwrap();
        let missing = root.path().join("missing");
        assert!(read_bounded_regular_file(&missing, 4).is_err());
        let directory = root.path().join("directory");
        std::fs::create_dir(&directory).unwrap();
        assert!(read_bounded_regular_file(&directory, 4).is_err());
        let oversized = root.path().join("oversized");
        std::fs::write(&oversized, b"12345").unwrap();
        assert!(read_bounded_regular_file(&oversized, 4).is_err());
        assert_eq!(std::fs::read(&oversized).unwrap(), b"12345");
        assert_eq!(read_bounded_regular_file(&oversized, 5).unwrap(), b"12345");
    }

    #[test]
    fn verified_reopen_rejects_wrong_digest_and_missing_generation_without_current_mutation() {
        let (root, generation_uuid) = project();
        let current = resolve_project_generation(root.path()).unwrap();
        let current_bytes = std::fs::read(root.path().join(CURRENT_FILE)).unwrap();

        assert_code(
            resolve_verified_generation(root.path(), generation_uuid, [0x55; 32]).unwrap_err(),
            "GF_PROJECT_CORRUPT",
        );
        assert_code(
            resolve_verified_generation(root.path(), Uuid::now_v7(), current.manifest_sha256())
                .unwrap_err(),
            "GF_PROJECT_CORRUPT",
        );
        assert_eq!(
            std::fs::read(root.path().join(CURRENT_FILE)).unwrap(),
            current_bytes
        );
        assert_eq!(
            resolve_project_generation(root.path())
                .unwrap()
                .generation_uuid(),
            generation_uuid
        );
    }

    #[test]
    fn participant_snapshot_rejects_same_length_content_tampering_after_reopen() {
        let root = tempfile::tempdir().unwrap();
        let resolved = open_or_initialize_project(root.path()).unwrap();
        let generation_uuid = resolved.generation_uuid();
        let snapshot = resolved
            .participant_snapshot("workspace", "configuration")
            .unwrap()
            .unwrap();
        let path = resolved
            .participant_path("workspace", "configuration")
            .unwrap();
        let mut tampered = snapshot.bytes.clone();
        tampered[0] ^= 1;
        fs::write(&path, tampered).unwrap();
        drop(resolved);

        let reopened = resolve_project_generation(root.path()).unwrap();
        assert_eq!(reopened.generation_uuid(), generation_uuid);
        assert_code(
            reopened
                .participant_snapshot("workspace", "configuration")
                .unwrap_err(),
            "GF_PROJECT_CORRUPT",
        );
    }
}
