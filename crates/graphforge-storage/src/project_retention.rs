//! Bounded snapshot retention and orphan garbage collection.
//!
//! Dry-run and execution share one verified reachability oracle
//! ([`crate::project_recovery::compute_reachable_generations`]). Age, PID,
//! newest UUID, and directory enumeration never grant cleanup authority.
//! Live leases, checkpoint roots, CURRENT, and retained ancestors are never
//! selected. Concurrent publication or checkpoint mutation returns
//! `GF_WRITER_BUSY` without unsafe cleanup.

use std::collections::BTreeSet;
use std::path::Path;
use std::time::Instant;

use graphforge_core::{GfError, ProjectErrorCode};
use uuid::Uuid;

use crate::project_checkpoints::checkpoint_retention_roots_after_writer_lock;
use crate::project_generation::{resolve_project_generation, validated_generation_manifest_sha256};
use crate::project_publication::{GENERATIONS_DIR, open_regular_lock};
use crate::project_recovery::{
    DEFAULT_RETAINED_ANCESTORS, GenerationCleanupOutcome, MAX_RETAINED_ANCESTORS, TRASH_DIR,
    acquire_recovery_lock, bounded_directory_entries, cleanup_trash_generation,
    cleanup_unreachable_generation, compute_reachable_generations, map_recovery_resolution,
    parse_canonical_uuid, project_error, reject_real_directory, storage_io,
};

/// Default entry bound for one retention/GC invocation.
pub const DEFAULT_RETENTION_MAX_ENTRIES: usize = 10_000;
/// Default scanned-byte bound for one retention/GC invocation.
pub const DEFAULT_RETENTION_MAX_BYTES: u64 = 64 * 1024 * 1024 * 1024;
/// Default work-unit bound (candidates acted on or classified per call).
pub const DEFAULT_RETENTION_MAX_WORK_UNITS: usize = 1_024;
/// Default cleanup batch size when executing removals.
pub const DEFAULT_RETENTION_CLEANUP_BATCH: usize = 64;

/// Finite CURRENT-ancestor retention policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectRetentionPolicy {
    /// Verified ancestors of CURRENT to retain (CURRENT itself is always kept).
    pub retained_ancestors: usize,
}

impl Default for ProjectRetentionPolicy {
    fn default() -> Self {
        Self {
            retained_ancestors: DEFAULT_RETAINED_ANCESTORS,
        }
    }
}

impl ProjectRetentionPolicy {
    /// Validate configured bounds.
    ///
    /// # Errors
    /// Returns `GF_RESOURCE_LIMIT` when `retained_ancestors` exceeds the max.
    pub fn validate(self) -> Result<Self, GfError> {
        if self.retained_ancestors > MAX_RETAINED_ANCESTORS {
            return Err(project_error(
                ProjectErrorCode::ResourceLimit,
                format!(
                    "retained_ancestors={} exceeds MAX_RETAINED_ANCESTORS={MAX_RETAINED_ANCESTORS}",
                    self.retained_ancestors
                ),
            ));
        }
        Ok(self)
    }
}

/// Resource bounds for one inspect/preview/execute invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectRetentionLimits {
    /// Maximum filesystem entries inspected under generations/ and trash/.
    pub max_entries: usize,
    /// Maximum bytes walked while classifying or reporting remaining size.
    pub max_bytes_scanned: u64,
    /// Maximum candidate classification/work units per invocation.
    pub max_work_units: usize,
    /// Maximum removals performed in one execute call (0 = unlimited within work).
    pub cleanup_batch: usize,
}

impl Default for ProjectRetentionLimits {
    fn default() -> Self {
        Self {
            max_entries: DEFAULT_RETENTION_MAX_ENTRIES,
            max_bytes_scanned: DEFAULT_RETENTION_MAX_BYTES,
            max_work_units: DEFAULT_RETENTION_MAX_WORK_UNITS,
            cleanup_batch: DEFAULT_RETENTION_CLEANUP_BATCH,
        }
    }
}

impl ProjectRetentionLimits {
    /// Validate configured bounds (all must be non-zero except cleanup_batch).
    ///
    /// # Errors
    /// Returns `GF_RESOURCE_LIMIT` for zero or inverted bounds.
    pub fn validate(self) -> Result<Self, GfError> {
        if self.max_entries == 0 {
            return Err(limit_error("max_entries", 0));
        }
        if self.max_bytes_scanned == 0 {
            return Err(limit_error("max_bytes_scanned", 0));
        }
        if self.max_work_units == 0 {
            return Err(limit_error("max_work_units", 0));
        }
        Ok(self)
    }
}

/// Where a classified entry lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectCleanupLocation {
    /// Under `generations/<uuid>/`.
    Generations,
    /// Under `trash/<uuid>/`.
    Trash,
}

impl ProjectCleanupLocation {
    /// Stable machine-readable location token.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Generations => "generations",
            Self::Trash => "trash",
        }
    }
}

/// Disposition assigned by the shared classification oracle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectCleanupDisposition {
    /// Proven unreachable and eligible for move/delete.
    Candidate,
    /// Reachable via CURRENT, retained ancestors, or checkpoint roots.
    Reachable,
    /// Unreachable but a live generation lease blocked exclusive acquisition.
    SkippedLive,
    /// Linked, special, or invalid machine path — preserved, never deleted.
    Quarantined,
    /// Unclassified noncanonical name — preserved, never deleted.
    Unknown,
}

impl ProjectCleanupDisposition {
    /// Stable machine-readable disposition token.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Candidate => "candidate",
            Self::Reachable => "reachable",
            Self::SkippedLive => "skipped_live",
            Self::Quarantined => "quarantined",
            Self::Unknown => "unknown",
        }
    }
}

/// One classified filesystem entry from the shared oracle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectCleanupEntry {
    /// Canonical generation UUID when the entry name is a hyphenated UUID.
    pub generation_uuid: Option<Uuid>,
    /// Generations vs trash.
    pub location: ProjectCleanupLocation,
    /// Oracle disposition.
    pub disposition: ProjectCleanupDisposition,
    /// Bounded size of the entry tree when measured.
    pub bytes: u64,
}

/// Reachability inspection report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectReachabilityReport {
    /// Validated CURRENT generation.
    pub selected_generation_uuid: Uuid,
    /// Policy used for the ancestor window.
    pub policy: ProjectRetentionPolicy,
    /// Verified reachable generation UUIDs (CURRENT + ancestors + checkpoints).
    pub reachable: BTreeSet<Uuid>,
    /// Active checkpoint root UUIDs included in reachability.
    pub checkpoint_roots: BTreeSet<Uuid>,
    /// Ancestor UUIDs retained by policy (excludes CURRENT and checkpoint-only).
    pub retained_ancestors: BTreeSet<Uuid>,
    /// Entries scanned while inspecting.
    pub entries_scanned: u64,
    /// Bytes walked while inspecting.
    pub bytes_scanned: u64,
    /// Elapsed wall time in milliseconds.
    pub elapsed_ms: u64,
}

/// Shared dry-run / execute cleanup report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectCleanupReport {
    /// Whether this pass only classified (true) or also removed (false).
    pub dry_run: bool,
    /// Validated CURRENT generation after lock acquisition.
    pub selected_generation_uuid: Uuid,
    /// Policy used for the ancestor window.
    pub policy: ProjectRetentionPolicy,
    /// Verified reachable generation count.
    pub reachable_count: u64,
    /// Entries classified as cleanup candidates.
    pub candidates: u64,
    /// Candidates removed (always 0 for dry-run).
    pub removed: u64,
    /// Unreachable entries skipped because a live lease was held.
    pub skipped_live: u64,
    /// Hostile/invalid entries preserved in place.
    pub quarantined: u64,
    /// Noncanonical/unclassified entries preserved in place.
    pub unknown: u64,
    /// Bytes still present after the pass (or that would remain on dry-run).
    pub remaining_bytes: u64,
    /// Bytes walked during classification/size accounting.
    pub bytes_scanned: u64,
    /// Filesystem entries inspected.
    pub entries_scanned: u64,
    /// Work units consumed (classified candidates / acted removals).
    pub work_units: u64,
    /// True when a bound stopped further work before the tree was exhausted.
    pub bounded: bool,
    /// Classified entries (capped by work units).
    pub entries: Vec<ProjectCleanupEntry>,
    /// Elapsed wall time in milliseconds.
    pub elapsed_ms: u64,
}

/// Inspect verified generation reachability under the recovery lock order.
///
/// # Errors
/// Returns `GF_WRITER_BUSY` when publication/checkpoint locks are held,
/// `GF_PROJECT_CORRUPT` for ambiguous authority, and `GF_RESOURCE_LIMIT` for
/// bound exhaustion.
pub fn inspect_project_reachability(
    container_root: impl AsRef<Path>,
    policy: ProjectRetentionPolicy,
    limits: ProjectRetentionLimits,
) -> Result<ProjectReachabilityReport, GfError> {
    inspect_project_reachability_with_mode(
        container_root,
        policy,
        limits,
        crate::filesystem_admission::ProjectLifecycleMode::Durable,
    )
}

/// Inspect reachability using the lifecycle mode established by the owning facade.
///
/// # Errors
/// Returns the same errors as [`inspect_project_reachability`].
pub fn inspect_project_reachability_with_mode(
    container_root: impl AsRef<Path>,
    policy: ProjectRetentionPolicy,
    limits: ProjectRetentionLimits,
    mode: crate::filesystem_admission::ProjectLifecycleMode,
) -> Result<ProjectReachabilityReport, GfError> {
    let started = Instant::now();
    let admission = crate::filesystem_admission::admit_project_lifecycle(
        container_root,
        mode,
        crate::filesystem_admission::ProjectRootRequirement::Existing,
    )?;
    admission.revalidate_identity()?;
    let policy = policy.validate()?;
    let limits = limits.validate()?;
    let root = admission.root();
    let writer_lock = acquire_recovery_lock(root)?;
    let selected = resolve_project_generation(root).map_err(map_recovery_resolution)?;
    let checkpoint_roots = checkpoint_retention_roots_after_writer_lock(root)?;
    let reachable = compute_reachable_generations(
        root,
        &selected,
        &checkpoint_roots.roots,
        policy.retained_ancestors,
    )?;
    let checkpoint_set = checkpoint_roots
        .roots
        .iter()
        .map(|(uuid, _)| *uuid)
        .collect::<BTreeSet<_>>();
    let mut retained_ancestors = reachable.clone();
    retained_ancestors.remove(&selected.generation_uuid());
    for uuid in &checkpoint_set {
        retained_ancestors.remove(uuid);
    }

    let mut entries_scanned = 0_u64;
    let mut bytes_scanned = 0_u64;
    let generations_root = root.join(GENERATIONS_DIR);
    if generations_root.exists() {
        let (entries, bytes) = scan_tree_bounded(
            &generations_root,
            limits.max_entries,
            limits.max_bytes_scanned,
        )?;
        entries_scanned = entries_scanned.saturating_add(entries);
        bytes_scanned = bytes_scanned.saturating_add(bytes);
    }
    let trash_root = root.join(TRASH_DIR);
    if trash_root.exists() {
        let (entries, bytes) =
            scan_tree_bounded(&trash_root, limits.max_entries, limits.max_bytes_scanned)?;
        entries_scanned = entries_scanned.saturating_add(entries);
        bytes_scanned = bytes_scanned.saturating_add(bytes);
        if entries_scanned > limits.max_entries as u64 {
            return Err(limit_error("max_entries", limits.max_entries as u64));
        }
        if bytes_scanned > limits.max_bytes_scanned {
            return Err(limit_error("max_bytes_scanned", limits.max_bytes_scanned));
        }
    }

    drop(checkpoint_roots);
    crate::file_lock::unlock(&writer_lock).map_err(storage_io)?;
    drop(writer_lock);

    Ok(ProjectReachabilityReport {
        selected_generation_uuid: selected.generation_uuid(),
        policy,
        reachable,
        checkpoint_roots: checkpoint_set,
        retained_ancestors,
        entries_scanned,
        bytes_scanned,
        elapsed_ms: elapsed_ms(started),
    })
}

/// Preview cleanup using the same classification oracle as execute.
///
/// # Errors
/// Same typed busy/corrupt/limit failures as [`execute_project_cleanup`].
pub fn preview_project_cleanup(
    container_root: impl AsRef<Path>,
    policy: ProjectRetentionPolicy,
    limits: ProjectRetentionLimits,
) -> Result<ProjectCleanupReport, GfError> {
    preview_project_cleanup_with_mode(
        container_root,
        policy,
        limits,
        crate::filesystem_admission::ProjectLifecycleMode::Durable,
    )
}

/// Preview cleanup using the lifecycle mode established by the owning facade.
///
/// # Errors
/// Returns the same errors as [`preview_project_cleanup`].
pub fn preview_project_cleanup_with_mode(
    container_root: impl AsRef<Path>,
    policy: ProjectRetentionPolicy,
    limits: ProjectRetentionLimits,
    mode: crate::filesystem_admission::ProjectLifecycleMode,
) -> Result<ProjectCleanupReport, GfError> {
    run_cleanup(container_root.as_ref(), policy, limits, true, mode)
}

/// Execute bounded orphan/unreachable cleanup.
///
/// # Errors
/// Returns `GF_WRITER_BUSY` for concurrent publication/checkpoint mutation,
/// `GF_PROJECT_CORRUPT` for ambiguous authority, `GF_RESOURCE_LIMIT` when
/// bounds are exhausted, and storage IO errors for disk-full/rename failures.
pub fn execute_project_cleanup(
    container_root: impl AsRef<Path>,
    policy: ProjectRetentionPolicy,
    limits: ProjectRetentionLimits,
) -> Result<ProjectCleanupReport, GfError> {
    execute_project_cleanup_with_mode(
        container_root,
        policy,
        limits,
        crate::filesystem_admission::ProjectLifecycleMode::Durable,
    )
}

/// Execute cleanup using the lifecycle mode established by the owning facade.
///
/// # Errors
/// Returns the same errors as [`execute_project_cleanup`].
pub fn execute_project_cleanup_with_mode(
    container_root: impl AsRef<Path>,
    policy: ProjectRetentionPolicy,
    limits: ProjectRetentionLimits,
    mode: crate::filesystem_admission::ProjectLifecycleMode,
) -> Result<ProjectCleanupReport, GfError> {
    run_cleanup(container_root.as_ref(), policy, limits, false, mode)
}

fn run_cleanup(
    root: &Path,
    policy: ProjectRetentionPolicy,
    limits: ProjectRetentionLimits,
    dry_run: bool,
    mode: crate::filesystem_admission::ProjectLifecycleMode,
) -> Result<ProjectCleanupReport, GfError> {
    let started = Instant::now();
    let admission = crate::filesystem_admission::admit_project_lifecycle(
        root,
        mode,
        crate::filesystem_admission::ProjectRootRequirement::Existing,
    )?;
    admission.revalidate_identity()?;
    let root = admission.root();
    let policy = policy.validate()?;
    let limits = limits.validate()?;
    let writer_lock = acquire_recovery_lock(root)?;
    let selected = resolve_project_generation(root).map_err(map_recovery_resolution)?;
    let checkpoint_roots = checkpoint_retention_roots_after_writer_lock(root)?;
    let classification = classify_cleanup_candidates(
        root,
        &selected,
        &checkpoint_roots.roots,
        policy,
        limits,
        dry_run,
    )?;

    let mut removed = 0_u64;
    let mut skipped_live = classification.skipped_live;
    let mut bounded = classification.bounded;
    if !dry_run {
        let batch_limit = if limits.cleanup_batch == 0 {
            classification.candidate_uuids.len()
        } else {
            limits
                .cleanup_batch
                .min(classification.candidate_uuids.len())
        };
        for (index, (location, uuid)) in classification.candidate_uuids.iter().enumerate() {
            if index >= batch_limit {
                bounded = true;
                break;
            }
            let outcome = match location {
                ProjectCleanupLocation::Generations => cleanup_unreachable_generation(
                    root,
                    *uuid,
                    &checkpoint_roots.roots,
                    policy.retained_ancestors,
                )?,
                ProjectCleanupLocation::Trash => cleanup_trash_generation(root, *uuid)?,
            };
            match outcome {
                GenerationCleanupOutcome::Removed => removed += 1,
                GenerationCleanupOutcome::SkippedLiveLease => skipped_live += 1,
                GenerationCleanupOutcome::Retained | GenerationCleanupOutcome::Absent => {}
            }
        }
    }

    let remaining_bytes = remaining_project_bytes(root, limits)?;

    drop(checkpoint_roots);
    crate::file_lock::unlock(&writer_lock).map_err(storage_io)?;
    drop(writer_lock);

    Ok(ProjectCleanupReport {
        dry_run,
        selected_generation_uuid: selected.generation_uuid(),
        policy,
        reachable_count: classification.reachable_count,
        candidates: classification.candidates,
        removed,
        skipped_live,
        quarantined: classification.quarantined,
        unknown: classification.unknown,
        remaining_bytes,
        bytes_scanned: classification.bytes_scanned,
        entries_scanned: classification.entries_scanned,
        work_units: classification.work_units,
        bounded,
        entries: classification.entries,
        elapsed_ms: elapsed_ms(started),
    })
}

struct Classification {
    reachable_count: u64,
    candidates: u64,
    skipped_live: u64,
    quarantined: u64,
    unknown: u64,
    bytes_scanned: u64,
    entries_scanned: u64,
    work_units: u64,
    bounded: bool,
    entries: Vec<ProjectCleanupEntry>,
    candidate_uuids: Vec<(ProjectCleanupLocation, Uuid)>,
}

fn classify_cleanup_candidates(
    root: &Path,
    selected: &crate::ResolvedProjectGeneration,
    checkpoint_roots: &[(Uuid, [u8; 32])],
    policy: ProjectRetentionPolicy,
    limits: ProjectRetentionLimits,
    dry_run: bool,
) -> Result<Classification, GfError> {
    let reachable =
        compute_reachable_generations(root, selected, checkpoint_roots, policy.retained_ancestors)?;
    let mut classification = Classification {
        reachable_count: reachable.len() as u64,
        candidates: 0,
        skipped_live: 0,
        quarantined: 0,
        unknown: 0,
        bytes_scanned: 0,
        entries_scanned: 0,
        work_units: 0,
        bounded: false,
        entries: Vec::new(),
        candidate_uuids: Vec::new(),
    };

    classify_directory(
        root,
        &root.join(GENERATIONS_DIR),
        ProjectCleanupLocation::Generations,
        &reachable,
        limits,
        dry_run,
        &mut classification,
    )?;
    classify_directory(
        root,
        &root.join(TRASH_DIR),
        ProjectCleanupLocation::Trash,
        &reachable,
        limits,
        dry_run,
        &mut classification,
    )?;
    Ok(classification)
}

fn classify_directory(
    root: &Path,
    directory: &Path,
    location: ProjectCleanupLocation,
    reachable: &BTreeSet<Uuid>,
    limits: ProjectRetentionLimits,
    dry_run: bool,
    classification: &mut Classification,
) -> Result<(), GfError> {
    if !directory.exists() {
        return Ok(());
    }
    // Hostile top-level generations/trash (symlink / non-dir) fails closed.
    if let Err(error) = reject_real_directory(directory) {
        // Count as quarantined machine state; do not recurse.
        classification.quarantined = classification.quarantined.saturating_add(1);
        classification.entries.push(ProjectCleanupEntry {
            generation_uuid: None,
            location,
            disposition: ProjectCleanupDisposition::Quarantined,
            bytes: 0,
        });
        let _ = error;
        return Ok(());
    }

    let mut paths = bounded_directory_entries(directory)?;
    paths.sort();
    for path in paths {
        classification.entries_scanned = classification.entries_scanned.saturating_add(1);
        if classification.entries_scanned > limits.max_entries as u64 {
            return Err(limit_error("max_entries", limits.max_entries as u64));
        }
        if classification.work_units >= limits.max_work_units as u64 {
            classification.bounded = true;
            break;
        }

        let meta = std::fs::symlink_metadata(&path).map_err(storage_io)?;
        if meta.file_type().is_symlink() || !meta.is_dir() {
            // Do not recurse into linked/special entries; preserve and count.
            push_entry(
                classification,
                None,
                location,
                ProjectCleanupDisposition::Quarantined,
                0,
            );
            classification.quarantined += 1;
            continue;
        }

        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            let bytes = account_entry_bytes(&path, limits, classification)?;
            push_entry(
                classification,
                None,
                location,
                ProjectCleanupDisposition::Unknown,
                bytes,
            );
            classification.unknown += 1;
            continue;
        };
        let Some(uuid) = parse_canonical_uuid(name) else {
            let bytes = account_entry_bytes(&path, limits, classification)?;
            push_entry(
                classification,
                None,
                location,
                ProjectCleanupDisposition::Unknown,
                bytes,
            );
            classification.unknown += 1;
            continue;
        };

        if reachable.contains(&uuid) {
            let bytes = account_entry_bytes(&path, limits, classification)?;
            push_entry(
                classification,
                Some(uuid),
                location,
                ProjectCleanupDisposition::Reachable,
                bytes,
            );
            continue;
        }

        match location {
            ProjectCleanupLocation::Generations => {
                classify_generation_entry(root, &path, uuid, limits, dry_run, classification)?;
            }
            ProjectCleanupLocation::Trash => {
                let bytes = account_entry_bytes(&path, limits, classification)?;
                // Trash is already invisible to readers; CURRENT identity still
                // fails closed inside cleanup_trash_generation.
                push_entry(
                    classification,
                    Some(uuid),
                    location,
                    ProjectCleanupDisposition::Candidate,
                    bytes,
                );
                classification.candidates += 1;
                classification
                    .candidate_uuids
                    .push((ProjectCleanupLocation::Trash, uuid));
            }
        }
    }
    Ok(())
}

fn classify_generation_entry(
    root: &Path,
    path: &Path,
    uuid: Uuid,
    limits: ProjectRetentionLimits,
    dry_run: bool,
    classification: &mut Classification,
) -> Result<(), GfError> {
    // dry_run and execute share this classification path; execute acts on
    // candidate_uuids after classification.
    let _ = dry_run;
    if validated_generation_manifest_sha256(root, uuid).is_ok() {
        let bytes = account_entry_bytes(path, limits, classification)?;
        if generation_lease_is_live(path)? {
            push_entry(
                classification,
                Some(uuid),
                ProjectCleanupLocation::Generations,
                ProjectCleanupDisposition::SkippedLive,
                bytes,
            );
            classification.skipped_live += 1;
        } else {
            push_entry(
                classification,
                Some(uuid),
                ProjectCleanupLocation::Generations,
                ProjectCleanupDisposition::Candidate,
                bytes,
            );
            classification.candidates += 1;
            classification
                .candidate_uuids
                .push((ProjectCleanupLocation::Generations, uuid));
        }
    } else {
        let bytes = account_entry_bytes(path, limits, classification)?;
        push_entry(
            classification,
            Some(uuid),
            ProjectCleanupLocation::Generations,
            ProjectCleanupDisposition::Quarantined,
            bytes,
        );
        classification.quarantined += 1;
    }
    Ok(())
}

fn push_entry(
    classification: &mut Classification,
    generation_uuid: Option<Uuid>,
    location: ProjectCleanupLocation,
    disposition: ProjectCleanupDisposition,
    bytes: u64,
) {
    classification.work_units = classification.work_units.saturating_add(1);
    classification.entries.push(ProjectCleanupEntry {
        generation_uuid,
        location,
        disposition,
        bytes,
    });
}

fn generation_lease_is_live(generation_path: &Path) -> Result<bool, GfError> {
    let lease_path = generation_path.join("lease.lock");
    if !lease_path.exists() {
        return Ok(false);
    }
    let lease = open_regular_lock(&lease_path)?;
    if crate::file_lock::try_lock_exclusive(&lease).map_err(storage_io)? {
        crate::file_lock::unlock(&lease).map_err(storage_io)?;
        Ok(false)
    } else {
        Ok(true)
    }
}

fn account_entry_bytes(
    path: &Path,
    limits: ProjectRetentionLimits,
    classification: &mut Classification,
) -> Result<u64, GfError> {
    let remaining = limits
        .max_bytes_scanned
        .saturating_sub(classification.bytes_scanned);
    let (entries, bytes) = scan_tree_bounded(path, limits.max_entries, remaining)?;
    classification.entries_scanned = classification.entries_scanned.saturating_add(entries);
    classification.bytes_scanned = classification.bytes_scanned.saturating_add(bytes);
    if classification.entries_scanned > limits.max_entries as u64 {
        return Err(limit_error("max_entries", limits.max_entries as u64));
    }
    if classification.bytes_scanned > limits.max_bytes_scanned {
        return Err(limit_error("max_bytes_scanned", limits.max_bytes_scanned));
    }
    Ok(bytes)
}

fn remaining_project_bytes(root: &Path, limits: ProjectRetentionLimits) -> Result<u64, GfError> {
    let mut total = 0_u64;
    for relative in [GENERATIONS_DIR, TRASH_DIR] {
        let path = root.join(relative);
        if !path.exists() {
            continue;
        }
        let (_entries, bytes) =
            scan_tree_bounded(&path, limits.max_entries, limits.max_bytes_scanned)?;
        total = total.saturating_add(bytes);
    }
    Ok(total)
}

fn scan_tree_bounded(
    root: &Path,
    max_entries: usize,
    max_bytes: u64,
) -> Result<(u64, u64), GfError> {
    let meta = std::fs::symlink_metadata(root).map_err(storage_io)?;
    if meta.file_type().is_symlink() {
        // Callers classify linked roots/entries as quarantined; size walks never
        // follow links and never treat them as reachability authority.
        return Ok((1, 0));
    }
    let mut entries = 0_u64;
    let mut bytes = 0_u64;
    let mut stack = vec![root.to_path_buf()];
    while let Some(path) = stack.pop() {
        entries = entries.saturating_add(1);
        if entries > max_entries as u64 {
            return Err(limit_error("max_entries", max_entries as u64));
        }
        let meta = std::fs::symlink_metadata(&path).map_err(storage_io)?;
        if meta.file_type().is_symlink() {
            continue;
        }
        if meta.is_file() {
            bytes = bytes.saturating_add(meta.len());
            if bytes > max_bytes {
                return Err(limit_error("max_bytes_scanned", max_bytes));
            }
            continue;
        }
        if !meta.is_dir() {
            // Special files: count as a byte for progress, do not recurse.
            bytes = bytes.saturating_add(1);
            continue;
        }
        for entry in std::fs::read_dir(&path).map_err(storage_io)? {
            stack.push(entry.map_err(storage_io)?.path());
        }
    }
    Ok((entries, bytes))
}

fn limit_error(resource: &str, limit: u64) -> GfError {
    project_error(
        ProjectErrorCode::ResourceLimit,
        format!("phase=RETENTION committed=false resource={resource} limit={limit}"),
    )
}

fn elapsed_ms(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::project_checkpoints::{
        CheckpointCreateRequest, CheckpointDeleteRequest, create_checkpoint, delete_checkpoint,
    };
    use crate::project_publication::{
        ProjectCapability, ProjectGenerationRequest, ProjectParticipant,
        ProjectParticipantEncoding, ProjectStageOutcome, ensure_machine_directory,
        stage_project_generation,
    };
    use crate::{open_or_initialize_project, resolve_project_generation};
    use sha2::{Digest, Sha256};
    use std::fs;
    use std::path::PathBuf;
    use std::process::Command;

    #[cfg(unix)]
    use std::os::unix::fs::symlink;

    fn participant(capability: &str, family: &str) -> ProjectParticipant {
        let bytes = format!("{capability}:{family}").into_bytes();
        ProjectParticipant {
            capability_id: capability.into(),
            capability_version: 1,
            record_family_id: family.into(),
            record_version: 1,
            encoding: ProjectParticipantEncoding::Parquet,
            schema_fingerprint: Sha256::digest(format!("{capability}/{family}")).into(),
            row_count: 1,
            bytes,
        }
    }

    fn publish_one(root: &Path) -> Uuid {
        let request = ProjectGenerationRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
            capabilities: vec![ProjectCapability {
                capability_id: "graph".into(),
                capability_version: 1,
            }],
            participants: vec![participant("graph", "nodes")],
        };
        let ProjectStageOutcome::Staged(staged) = stage_project_generation(root, &request).unwrap()
        else {
            panic!("unexpected replay");
        };
        staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .unwrap()
            .publish()
            .unwrap();
        request.generation_uuid
    }

    fn policy(ancestors: usize) -> ProjectRetentionPolicy {
        ProjectRetentionPolicy {
            retained_ancestors: ancestors,
        }
    }

    fn limits(
        max_entries: usize,
        max_bytes: u64,
        max_work: usize,
        batch: usize,
    ) -> ProjectRetentionLimits {
        ProjectRetentionLimits {
            max_entries,
            max_bytes_scanned: max_bytes,
            max_work_units: max_work,
            cleanup_batch: batch,
        }
    }

    #[test]
    fn dry_run_and_execute_share_reachability_oracle() {
        let root = tempfile::tempdir().unwrap();
        open_or_initialize_project(root.path()).unwrap();
        let old = publish_one(root.path());
        for _ in 0..4 {
            publish_one(root.path());
        }
        let current = resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid();

        let preview =
            preview_project_cleanup(root.path(), policy(2), ProjectRetentionLimits::default())
                .unwrap();
        let execute =
            execute_project_cleanup(root.path(), policy(2), ProjectRetentionLimits::default())
                .unwrap();

        assert_eq!(preview.selected_generation_uuid, current);
        assert_eq!(execute.selected_generation_uuid, current);
        assert!(preview.dry_run);
        assert!(!execute.dry_run);
        assert_eq!(preview.reachable_count, execute.reachable_count);
        assert!(preview.candidates >= 1);
        assert_eq!(preview.candidates, execute.candidates);
        assert!(execute.removed >= 1);
        assert!(execute.removed <= execute.candidates);
        assert!(
            !root
                .path()
                .join(GENERATIONS_DIR)
                .join(old.hyphenated().to_string())
                .exists()
        );
        assert!(
            root.path()
                .join(GENERATIONS_DIR)
                .join(current.hyphenated().to_string())
                .exists()
        );
    }

    #[test]
    fn checkpoint_root_and_live_lease_are_never_selected() {
        let root = tempfile::tempdir().unwrap();
        open_or_initialize_project(root.path()).unwrap();
        let pinned = publish_one(root.path());
        create_checkpoint(
            root.path(),
            &CheckpointCreateRequest {
                operation_uuid: Uuid::now_v7(),
                name: "pin".into(),
                description: None,
                actor_uuid: None,
            },
        )
        .unwrap();
        for _ in 0..4 {
            publish_one(root.path());
        }

        let reach =
            inspect_project_reachability(root.path(), policy(2), ProjectRetentionLimits::default())
                .unwrap();
        assert!(reach.reachable.contains(&pinned));
        assert!(reach.checkpoint_roots.contains(&pinned));

        let preview =
            preview_project_cleanup(root.path(), policy(2), ProjectRetentionLimits::default())
                .unwrap();
        assert!(
            preview
                .entries
                .iter()
                .filter(|entry| entry.generation_uuid == Some(pinned))
                .all(|entry| entry.disposition == ProjectCleanupDisposition::Reachable)
        );

        let generation_path = root
            .path()
            .join(GENERATIONS_DIR)
            .join(pinned.hyphenated().to_string());
        let lease = open_regular_lock(&generation_path.join("lease.lock")).unwrap();
        crate::file_lock::lock_shared(&lease).unwrap();
        delete_checkpoint(
            root.path(),
            &CheckpointDeleteRequest {
                operation_uuid: Uuid::now_v7(),
                name: "pin".into(),
                actor_uuid: None,
            },
        )
        .unwrap();

        let preview =
            preview_project_cleanup(root.path(), policy(0), ProjectRetentionLimits::default())
                .unwrap();
        assert!(
            preview.entries.iter().any(|entry| {
                entry.generation_uuid == Some(pinned)
                    && entry.disposition == ProjectCleanupDisposition::SkippedLive
            }),
            "live lease must skip without selecting for delete: {preview:?}"
        );
        assert!(generation_path.exists());

        crate::file_lock::unlock(&lease).unwrap();
        drop(lease);
        let executed =
            execute_project_cleanup(root.path(), policy(0), ProjectRetentionLimits::default())
                .unwrap();
        assert!(executed.removed >= 1);
        assert!(!generation_path.exists());
    }

    #[test]
    fn concurrent_writer_returns_typed_busy_without_cleanup() {
        let root = tempfile::tempdir().unwrap();
        open_or_initialize_project(root.path()).unwrap();
        publish_one(root.path());
        let lock_dir = ensure_machine_directory(
            root.path(),
            Path::new(crate::project_publication::LOCKS_DIR),
        )
        .unwrap();
        let lock = open_regular_lock(&lock_dir.join(crate::project_publication::WRITER_LOCK_FILE))
            .unwrap();
        crate::file_lock::lock_exclusive(&lock).unwrap();

        let error = preview_project_cleanup(
            root.path(),
            ProjectRetentionPolicy::default(),
            ProjectRetentionLimits::default(),
        )
        .unwrap_err();
        assert_eq!(error.code(), "GF_WRITER_BUSY");
    }

    #[test]
    fn work_and_byte_limits_are_fail_closed() {
        let root = tempfile::tempdir().unwrap();
        open_or_initialize_project(root.path()).unwrap();
        for _ in 0..5 {
            publish_one(root.path());
        }

        let error = preview_project_cleanup(root.path(), policy(0), limits(1, u64::MAX, 64, 8))
            .unwrap_err();
        assert_eq!(error.code(), "GF_RESOURCE_LIMIT");
        assert!(error.to_string().contains("max_entries"));

        let error =
            preview_project_cleanup(root.path(), policy(0), limits(10_000, 1, 64, 8)).unwrap_err();
        assert_eq!(error.code(), "GF_RESOURCE_LIMIT");
        assert!(error.to_string().contains("max_bytes_scanned"));

        let report =
            preview_project_cleanup(root.path(), policy(0), limits(10_000, u64::MAX, 1, 8))
                .unwrap();
        assert!(report.bounded || report.work_units <= 1);
    }

    #[test]
    fn malicious_filesystem_entries_are_quarantined_not_deleted() {
        let root = tempfile::tempdir().unwrap();
        open_or_initialize_project(root.path()).unwrap();
        let current = publish_one(root.path());
        let generations = root.path().join(GENERATIONS_DIR);
        fs::write(generations.join("not-a-uuid"), b"caller").unwrap();
        #[cfg(unix)]
        {
            let target = generations.join(current.hyphenated().to_string());
            let linked = generations.join("linked-entry");
            symlink(&target, &linked).unwrap();
            assert!(linked.exists());
        }

        let preview = preview_project_cleanup(
            root.path(),
            ProjectRetentionPolicy::default(),
            ProjectRetentionLimits::default(),
        )
        .unwrap();
        assert!(preview.quarantined >= 1 || preview.unknown >= 1);
        assert!(generations.join("not-a-uuid").exists());
        assert_eq!(
            execute_project_cleanup(
                root.path(),
                ProjectRetentionPolicy::default(),
                ProjectRetentionLimits::default(),
            )
            .unwrap()
            .selected_generation_uuid,
            current
        );
        assert!(generations.join("not-a-uuid").exists());
        assert!(generations.join(current.hyphenated().to_string()).exists());
        #[cfg(unix)]
        assert!(generations.join("linked-entry").exists());
    }

    #[test]
    fn reports_reconcile_counts_and_remaining_bytes() {
        let root = tempfile::tempdir().unwrap();
        open_or_initialize_project(root.path()).unwrap();
        for _ in 0..4 {
            publish_one(root.path());
        }
        let preview =
            preview_project_cleanup(root.path(), policy(1), ProjectRetentionLimits::default())
                .unwrap();
        assert!(preview.remaining_bytes > 0);
        assert_eq!(
            preview.candidates
                + preview.skipped_live
                + preview.quarantined
                + preview.unknown
                + preview
                    .entries
                    .iter()
                    .filter(|e| e.disposition == ProjectCleanupDisposition::Reachable)
                    .count() as u64,
            preview.entries.len() as u64
        );

        let executed =
            execute_project_cleanup(root.path(), policy(1), ProjectRetentionLimits::default())
                .unwrap();
        assert!(executed.removed <= executed.candidates);
        assert!(executed.remaining_bytes <= preview.remaining_bytes);
    }

    #[test]
    fn gc_crash_after_move_is_idempotent_on_reopen_cleanup() {
        let root = tempfile::tempdir().unwrap();
        open_or_initialize_project(root.path()).unwrap();
        let old = publish_one(root.path());
        for _ in 0..3 {
            publish_one(root.path());
        }
        let current = resolve_project_generation(root.path())
            .unwrap()
            .generation_uuid();

        let status = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("project_retention::tests::subprocess_cleanup_with_failpoint")
            .arg("--nocapture")
            .env("GRAPHFORGE_TEST_PROJECT_ROOT", root.path())
            .env(
                "GRAPHFORGE_PROJECT_FAILPOINTS",
                "graphforge-internal-subprocess-v1",
            )
            .env("GRAPHFORGE_PROJECT_FAILPOINT", "project.after_gc_move")
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(crate::project_failpoint::exit_code()));

        assert_eq!(
            resolve_project_generation(root.path())
                .unwrap()
                .generation_uuid(),
            current
        );
        let trash_root = root.path().join(TRASH_DIR);
        assert!(
            trash_root.exists()
                && !std::fs::read_dir(&trash_root)
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap()
                    .is_empty(),
            "crash after move must leave work in trash for idempotent retry"
        );

        let again =
            execute_project_cleanup(root.path(), policy(2), ProjectRetentionLimits::default())
                .unwrap();
        assert_eq!(again.selected_generation_uuid, current);
        assert!(
            !root
                .path()
                .join(GENERATIONS_DIR)
                .join(old.hyphenated().to_string())
                .exists()
        );
        // Trash from the interrupted move is cleared idempotently.
        assert!(
            !trash_root.join(old.hyphenated().to_string()).exists()
                || again.removed >= 1
                || again.candidates == 0
        );
        let trash_remaining = if trash_root.exists() {
            std::fs::read_dir(&trash_root)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
                .len()
        } else {
            0
        };
        assert_eq!(trash_remaining, 0);
    }

    #[test]
    fn subprocess_cleanup_with_failpoint() {
        if std::env::var("GRAPHFORGE_TEST_PROJECT_ROOT").is_err() {
            return;
        }
        let root = PathBuf::from(std::env::var("GRAPHFORGE_TEST_PROJECT_ROOT").unwrap());
        let _ = execute_project_cleanup(&root, policy(2), ProjectRetentionLimits::default());
    }
}
