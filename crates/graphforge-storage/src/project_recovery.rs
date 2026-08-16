//! Deterministic recovery for interrupted project-generation transactions.
//!
//! `CURRENT` remains the sole authority. Recovery classifies advisory journals
//! while holding the kernel writer lock and only removes UUID-named abandoned
//! generations after rechecking reachability and reader leases.

use std::collections::BTreeSet;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::Instant;

use graphforge_core::{GfError, ProjectErrorCode};
use uuid::Uuid;

use crate::project_checkpoints::checkpoint_retention_roots_after_writer_lock;
use crate::project_failpoint;
use crate::project_generation::{
    CURRENT_FILE, ResolvedProjectGeneration, open_or_initialize_project_admitted,
    resolve_project_generation, validated_generation_manifest_sha256, validated_generation_parent,
};
use crate::project_publication::{
    ATTEMPTS_DIR, GENERATIONS_DIR, JournalPhase, LOCKS_DIR, TRANSACTIONS_DIR, WRITER_LOCK_FILE,
    ensure_machine_directory, open_regular_lock, open_transaction_lock, read_journal,
    sync_directory, write_journal,
};

pub(crate) const TRASH_DIR: &str = "trash";
pub(crate) const MAX_RECOVERY_ENTRIES: usize = 10_000;
/// Default CURRENT-ancestor retention window (ADR 0013).
pub const DEFAULT_RETAINED_ANCESTORS: usize = 2;
/// Upper bound for configured ancestor retention.
pub const MAX_RETAINED_ANCESTORS: usize = 1024;

/// Stable summary of one recovery pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectRecoveryReport {
    /// Generation selected by the exact committed pointer.
    pub selected_generation_uuid: Uuid,
    /// Journals repaired from a durable pre-publication phase to `PUBLISHED`.
    pub repaired_journals: u64,
    /// Uncommitted journals classified as `ABORTED`.
    pub aborted_journals: u64,
    /// Abandoned private generation or trash directories removed.
    pub removed_generations: u64,
    /// Unknown machine-owned entries preserved for explicit inspection.
    pub preserved_unknown_entries: u64,
}

/// Why an open-lifecycle recovery pass did not complete cleanup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectRecoveryDeferral {
    /// A live writer holds `locks/writer.lock`; snapshot open remains safe.
    LiveWriterOwnsKernelLock,
}

impl ProjectRecoveryDeferral {
    /// Stable machine-readable deferral token (no paths or payload data).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LiveWriterOwnsKernelLock => "live_writer_owns_kernel_lock",
        }
    }
}

/// Authority class of the generation pinned by open/recovery.
///
/// Recovery never elects by UUID, time, or directory order; only a validated
/// `CURRENT` pointer or an explicit checkpoint pin grants authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectRecoveryGenerationClass {
    /// Exact generation named by a validated `CURRENT` record.
    CommittedCurrent,
    /// Exact generation named by a validated active checkpoint row.
    CheckpointPinned,
}

impl ProjectRecoveryGenerationClass {
    /// Stable machine-readable class token.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CommittedCurrent => "committed_current",
            Self::CheckpointPinned => "checkpoint_pinned",
        }
    }
}

/// Open-path context for recovery evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectOpenRecoveryKind {
    /// Ordinary project open inspected (and possibly cleaned) abandoned state.
    ProjectOpen,
    /// First committed generation / resumed initialization; cleanup not applicable.
    Initialization,
    /// Read-only checkpoint pin; recovery cleanup intentionally skipped.
    CheckpointView,
}

/// Safe recovery summary exposed through open evidence / the Rust facade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectOpenRecoveryEvidence {
    /// Which open path produced this evidence.
    pub kind: ProjectOpenRecoveryKind,
    /// Generation selected by validated `CURRENT` (or the checkpoint pin).
    pub selected_generation_uuid: Uuid,
    /// Authority class of [`Self::selected_generation_uuid`].
    pub selected_generation_class: ProjectRecoveryGenerationClass,
    /// Whether bounded inspection found candidate recovery work.
    pub work_detected: bool,
    /// Journals repaired to `PUBLISHED`.
    pub repaired_journals: u64,
    /// Journals classified as `ABORTED`.
    pub aborted_journals: u64,
    /// Abandoned private generation or trash directories removed/quarantined.
    pub removed_generations: u64,
    /// Unknown or unclassified entries preserved for inspection.
    pub preserved_unknown_entries: u64,
    /// Present when cleanup was deferred because recovery locks were busy.
    pub deferred: Option<ProjectRecoveryDeferral>,
    /// Wall-clock duration of the recovery/inspection phase in milliseconds.
    pub elapsed_ms: u64,
}

impl ProjectOpenRecoveryEvidence {
    /// Evidence for first initialization (no recovery pass).
    #[must_use]
    pub fn initialization(selected_generation_uuid: Uuid) -> Self {
        Self {
            kind: ProjectOpenRecoveryKind::Initialization,
            selected_generation_uuid,
            selected_generation_class: ProjectRecoveryGenerationClass::CommittedCurrent,
            work_detected: false,
            repaired_journals: 0,
            aborted_journals: 0,
            removed_generations: 0,
            preserved_unknown_entries: 0,
            deferred: None,
            elapsed_ms: 0,
        }
    }

    /// Evidence for a read-only checkpoint open (recovery cleanup skipped).
    #[must_use]
    pub fn checkpoint_view(selected_generation_uuid: Uuid) -> Self {
        Self {
            kind: ProjectOpenRecoveryKind::CheckpointView,
            selected_generation_uuid,
            selected_generation_class: ProjectRecoveryGenerationClass::CheckpointPinned,
            work_detected: false,
            repaired_journals: 0,
            aborted_journals: 0,
            removed_generations: 0,
            preserved_unknown_entries: 0,
            deferred: None,
            elapsed_ms: 0,
        }
    }

    /// Evidence for an already-clean committed open with no recovery work.
    #[must_use]
    pub fn clean_open(selected_generation_uuid: Uuid) -> Self {
        Self {
            kind: ProjectOpenRecoveryKind::ProjectOpen,
            selected_generation_uuid,
            selected_generation_class: ProjectRecoveryGenerationClass::CommittedCurrent,
            work_detected: false,
            repaired_journals: 0,
            aborted_journals: 0,
            removed_generations: 0,
            preserved_unknown_entries: 0,
            deferred: None,
            elapsed_ms: 0,
        }
    }
}

/// Open or initialize a project, then run recovery-on-open when applicable.
///
/// First initialization and resumed uninitialized layouts skip cleanup and
/// report [`ProjectOpenRecoveryKind::Initialization`]. Ordinary reopens resolve
/// and validate `CURRENT`, then recover abandoned state when locks are free.
///
/// # Errors
/// Propagates initialization, resolution, and recovery integrity errors.
/// A live writer never fails an otherwise valid snapshot open: cleanup is
/// reported as deferred instead of `GF_WRITER_BUSY`.
pub fn open_or_initialize_project_with_recovery(
    container_root: impl AsRef<Path>,
) -> Result<(ResolvedProjectGeneration, ProjectOpenRecoveryEvidence), GfError> {
    open_or_initialize_project_with_recovery_for_mode(
        container_root.as_ref(),
        crate::filesystem_admission::ProjectLifecycleMode::Durable,
    )
}

/// Open or initialize an explicitly ephemeral project and apply the same
/// bounded recovery protocol without requiring a durable-volume probe.
///
/// # Errors
/// Returns the same initialization and recovery errors as
/// [`open_or_initialize_project_with_recovery`].
pub fn open_or_initialize_ephemeral_project_with_recovery(
    container_root: impl AsRef<Path>,
) -> Result<(ResolvedProjectGeneration, ProjectOpenRecoveryEvidence), GfError> {
    open_or_initialize_project_with_recovery_for_mode(
        container_root.as_ref(),
        crate::filesystem_admission::ProjectLifecycleMode::Ephemeral,
    )
}

fn open_or_initialize_project_with_recovery_for_mode(
    root: &Path,
    mode: crate::filesystem_admission::ProjectLifecycleMode,
) -> Result<(ResolvedProjectGeneration, ProjectOpenRecoveryEvidence), GfError> {
    let admission = crate::filesystem_admission::admit_project_lifecycle(
        root,
        mode,
        crate::filesystem_admission::ProjectRootRequirement::CreateIfMissing,
    )?;
    admission.revalidate_identity()?;
    let root = admission.root();
    if root.join(CURRENT_FILE).exists() {
        return recover_project_on_open_admitted(root);
    }
    let resolved = open_or_initialize_project_admitted(root)?;
    Ok((
        resolved.clone(),
        ProjectOpenRecoveryEvidence::initialization(resolved.generation_uuid()),
    ))
}

/// Resolve `CURRENT`, then recover interrupted transactions as part of open.
///
/// Cleanup runs only when bounded inspection finds candidate work and the
/// existing writer/transaction/checkpoint lock order can be acquired. When a
/// live writer owns those locks, this returns the validated committed
/// generation with [`ProjectRecoveryDeferral::LiveWriterOwnsKernelLock`].
///
/// # Errors
/// Returns typed integrity errors when `CURRENT`/manifest authority is
/// ambiguous. Never elects a generation by UUID, time, or directory order.
pub fn recover_project_on_open(
    container_root: impl AsRef<Path>,
) -> Result<(ResolvedProjectGeneration, ProjectOpenRecoveryEvidence), GfError> {
    let admission = crate::filesystem_admission::admit_project_lifecycle(
        container_root,
        crate::filesystem_admission::ProjectLifecycleMode::Durable,
        crate::filesystem_admission::ProjectRootRequirement::Existing,
    )?;
    admission.revalidate_identity()?;
    recover_project_on_open_admitted(admission.root())
}

fn recover_project_on_open_admitted(
    root: &Path,
) -> Result<(ResolvedProjectGeneration, ProjectOpenRecoveryEvidence), GfError> {
    let started = Instant::now();
    let selected = resolve_project_generation(root).map_err(map_recovery_resolution)?;
    let work_detected = project_needs_recovery_pass(root)?;
    if !work_detected {
        return Ok((
            selected.clone(),
            ProjectOpenRecoveryEvidence {
                kind: ProjectOpenRecoveryKind::ProjectOpen,
                selected_generation_uuid: selected.generation_uuid(),
                selected_generation_class: ProjectRecoveryGenerationClass::CommittedCurrent,
                work_detected: false,
                repaired_journals: 0,
                aborted_journals: 0,
                removed_generations: 0,
                preserved_unknown_entries: 0,
                deferred: None,
                elapsed_ms: elapsed_ms(started),
            },
        ));
    }

    match recover_project_transactions_admitted(root) {
        Ok(report) => {
            let selected = resolve_project_generation(root).map_err(map_recovery_resolution)?;
            Ok((
                selected.clone(),
                evidence_from_report(&report, selected.generation_uuid(), started, None),
            ))
        }
        Err(error) if error.code() == "GF_WRITER_BUSY" => {
            let selected = resolve_project_generation(root).map_err(map_recovery_resolution)?;
            Ok((
                selected.clone(),
                ProjectOpenRecoveryEvidence {
                    kind: ProjectOpenRecoveryKind::ProjectOpen,
                    selected_generation_uuid: selected.generation_uuid(),
                    selected_generation_class: ProjectRecoveryGenerationClass::CommittedCurrent,
                    work_detected: true,
                    repaired_journals: 0,
                    aborted_journals: 0,
                    removed_generations: 0,
                    preserved_unknown_entries: 0,
                    deferred: Some(ProjectRecoveryDeferral::LiveWriterOwnsKernelLock),
                    elapsed_ms: elapsed_ms(started),
                },
            ))
        }
        Err(error) => Err(error),
    }
}

fn evidence_from_report(
    report: &ProjectRecoveryReport,
    selected_generation_uuid: Uuid,
    started: Instant,
    deferred: Option<ProjectRecoveryDeferral>,
) -> ProjectOpenRecoveryEvidence {
    ProjectOpenRecoveryEvidence {
        kind: ProjectOpenRecoveryKind::ProjectOpen,
        selected_generation_uuid,
        selected_generation_class: ProjectRecoveryGenerationClass::CommittedCurrent,
        work_detected: true,
        repaired_journals: report.repaired_journals,
        aborted_journals: report.aborted_journals,
        removed_generations: report.removed_generations,
        preserved_unknown_entries: report.preserved_unknown_entries,
        deferred,
        elapsed_ms: elapsed_ms(started),
    }
}

fn elapsed_ms(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// Bounded, link-safe inspection for journals/attempts/trash that may need work.
fn project_needs_recovery_pass(root: &Path) -> Result<bool, GfError> {
    let trash_root = root.join(TRASH_DIR);
    if trash_root.exists() {
        reject_real_directory(&trash_root)?;
        if !bounded_directory_entries(&trash_root)?.is_empty() {
            return Ok(true);
        }
    }

    let transactions_root = root.join(TRANSACTIONS_DIR);
    if !transactions_root.exists() {
        return Ok(false);
    }
    reject_real_directory(&transactions_root)?;
    for path in bounded_directory_entries(&transactions_root)? {
        if looks_like_atomicwrite_temp(&path) {
            return Ok(true);
        }
        let Some(transaction_uuid) = journal_file_uuid(&path) else {
            // Noncanonical entry: recovery must fail closed without deleting it.
            return Ok(true);
        };
        let Ok(journal) = read_journal(&path) else {
            return Ok(true);
        };
        if parse_canonical_uuid(&journal.transaction_uuid) != Some(transaction_uuid) {
            return Ok(true);
        }
        match journal.phase {
            JournalPhase::Published => {}
            JournalPhase::Aborted => {
                let Some(generation_uuid) = parse_canonical_uuid(&journal.generation_uuid) else {
                    return Ok(true);
                };
                if aborted_journal_still_needs_cleanup(
                    root,
                    transaction_uuid,
                    generation_uuid,
                    &journal,
                ) {
                    return Ok(true);
                }
            }
            JournalPhase::Preparing
            | JournalPhase::Staged
            | JournalPhase::Validated
            | JournalPhase::Durable => return Ok(true),
        }
    }
    Ok(false)
}

fn aborted_journal_still_needs_cleanup(
    root: &Path,
    transaction_uuid: Uuid,
    generation_uuid: Uuid,
    journal: &crate::project_publication::JournalRecord,
) -> bool {
    let generation_name = generation_uuid.hyphenated().to_string();
    if root.join(GENERATIONS_DIR).join(&generation_name).exists() {
        return true;
    }
    if root.join(TRASH_DIR).join(&generation_name).exists() {
        return true;
    }
    root.join(ATTEMPTS_DIR)
        .join(transaction_uuid.hyphenated().to_string())
        .join(&journal.request_fingerprint)
        .exists()
}

fn looks_like_atomicwrite_temp(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    let Some(suffix) = name.strip_prefix(".atomicwrite") else {
        return false;
    };
    suffix.len() == 6 && suffix.bytes().all(|byte| byte.is_ascii_alphanumeric())
}

/// Recover interrupted project transactions without changing commit authority.
///
/// The function acquires `writer.lock` non-blockingly. It never uses PID,
/// timestamp, heartbeat, or owner metadata to infer that a writer is dead.
///
/// # Errors
/// Returns `GF_WRITER_BUSY` while a live writer owns the kernel lock and
/// `GF_PROJECT_CORRUPT` with recovery guidance for ambiguous journal or
/// committed-pointer state.
pub fn recover_project_transactions(
    container_root: impl AsRef<Path>,
) -> Result<ProjectRecoveryReport, GfError> {
    let admission = crate::filesystem_admission::admit_project_lifecycle(
        container_root,
        crate::filesystem_admission::ProjectLifecycleMode::Durable,
        crate::filesystem_admission::ProjectRootRequirement::Existing,
    )?;
    admission.revalidate_identity()?;
    recover_project_transactions_admitted(admission.root())
}

/// Remove an explicitly selected durable project only while no writer owns the
/// project mutation lock.
///
/// Lifecycle admission remains held after the nonblocking writer-idle check,
/// so no new admitted mutator can enter between that check and exact-root
/// removal. The writer handle is released before deletion for Windows
/// compatibility; retained lifecycle identity still prevents root substitution.
///
/// # Errors
/// Returns `GF_WRITER_BUSY` when a live writer exists, or the same typed
/// admission/removal failures as the shared filesystem lifecycle.
pub fn remove_durable_project_root(container_root: impl AsRef<Path>) -> Result<(), GfError> {
    let admission = crate::filesystem_admission::admit_project_lifecycle(
        container_root,
        crate::filesystem_admission::ProjectLifecycleMode::Durable,
        crate::filesystem_admission::ProjectRootRequirement::Existing,
    )?;
    admission.revalidate_identity()?;
    let writer_lock = acquire_recovery_lock(admission.root())?;
    crate::file_lock::unlock(&writer_lock).map_err(storage_io)?;
    drop(writer_lock);
    admission.revalidate_identity()?;
    admission.remove_project_root()
}

fn recover_project_transactions_admitted(root: &Path) -> Result<ProjectRecoveryReport, GfError> {
    let selected = resolve_project_generation(root).map_err(map_recovery_resolution)?;
    let root = selected.container_root().to_owned();
    let writer_lock = acquire_recovery_lock(&root)?;

    // A writer may have published between the read-side resolution and lock
    // acquisition. Only the post-lock resolution is authoritative here.
    let selected = resolve_project_generation(&root).map_err(map_recovery_resolution)?;
    let selected_uuid = selected.generation_uuid();
    let checkpoint_roots = checkpoint_retention_roots_after_writer_lock(&root)?;
    let retained = compute_reachable_generations(
        &root,
        &selected,
        &checkpoint_roots.roots,
        DEFAULT_RETAINED_ANCESTORS,
    )?;
    let mut report = ProjectRecoveryReport {
        selected_generation_uuid: selected_uuid,
        repaired_journals: 0,
        aborted_journals: 0,
        removed_generations: 0,
        preserved_unknown_entries: 0,
    };

    let transactions_root = root.join(TRANSACTIONS_DIR);
    if transactions_root.exists() {
        recover_journals(&root, &transactions_root, &selected, &retained, &mut report)?;
    }
    report.preserved_unknown_entries += count_unknown_generation_entries(&root, &retained)?;
    // Release checkpoints.lock before writer.lock so concurrent mutators never
    // observe the issue #275 signature (writer free, checkpoints still held).
    // Matches MutationLocks::Drop order in project_checkpoints.
    drop(checkpoint_roots);
    crate::file_lock::unlock(&writer_lock).map_err(storage_io)?;
    drop(writer_lock);
    Ok(report)
}

pub(crate) fn acquire_recovery_lock(root: &Path) -> Result<File, GfError> {
    let lock_dir = ensure_machine_directory(root, Path::new(LOCKS_DIR))?;
    sync_directory(root)?;
    let writer_lock = open_regular_lock(&lock_dir.join(WRITER_LOCK_FILE))?;
    if !crate::file_lock::try_lock_exclusive(&writer_lock).map_err(storage_io)? {
        return Err(project_error(
            ProjectErrorCode::WriterBusy,
            "phase=RECOVERY committed=false cause=live_writer_owns_kernel_lock",
        ));
    }
    Ok(writer_lock)
}

fn recover_journals(
    root: &Path,
    transactions_root: &Path,
    selected: &crate::ResolvedProjectGeneration,
    retained: &BTreeSet<Uuid>,
    report: &mut ProjectRecoveryReport,
) -> Result<(), GfError> {
    let mut journal_paths = bounded_directory_entries(transactions_root)?;
    journal_paths.sort();
    for journal_path in journal_paths {
        if crate::project_publication::cleanup_atomicwrite_temp(&journal_path)? {
            continue;
        }
        let Some(transaction_uuid) = journal_file_uuid(&journal_path) else {
            return Err(recovery_corrupt(
                "transaction directory contains a noncanonical journal entry",
            ));
        };
        let transaction_lock = open_transaction_lock(root, transaction_uuid)?;
        if !crate::file_lock::try_lock_exclusive(&transaction_lock).map_err(storage_io)? {
            // A live optimistic writer owns this exact attempt. The global
            // commit lock prevents it from publishing during this recovery
            // pass, while its transaction lease prevents false abandonment.
            continue;
        }
        let mut journal = read_journal(&journal_path)
            .map_err(|_| recovery_corrupt("transaction journal is torn, invalid, or ambiguous"))?;
        if parse_canonical_uuid(&journal.transaction_uuid) != Some(transaction_uuid) {
            return Err(recovery_corrupt(
                "transaction journal identity does not match its file name",
            ));
        }
        let generation_uuid = parse_canonical_uuid(&journal.generation_uuid)
            .ok_or_else(|| recovery_corrupt("transaction generation UUID is invalid"))?;

        if generation_uuid == selected.generation_uuid() {
            repair_reachable_journal(
                &journal_path,
                &mut journal,
                selected.manifest_sha256(),
                report,
            )?;
        } else if retained.contains(&generation_uuid) {
            repair_reachable_journal(
                &journal_path,
                &mut journal,
                validated_generation_manifest_sha256(root, generation_uuid)?,
                report,
            )?;
        } else {
            if journal.phase != JournalPhase::Published && journal.phase != JournalPhase::Aborted {
                journal.phase = JournalPhase::Aborted;
                write_journal(&journal_path, &journal)?;
                report.aborted_journals += 1;
            }
            report.removed_generations += cleanup_abandoned_generation(
                root,
                transaction_uuid,
                generation_uuid,
                &journal.request_fingerprint,
                retained,
            )?;
        }
    }
    Ok(())
}

fn repair_reachable_journal(
    path: &Path,
    journal: &mut crate::project_publication::JournalRecord,
    manifest_sha256: [u8; 32],
    report: &mut ProjectRecoveryReport,
) -> Result<(), GfError> {
    let expected_digest = digest_hex(manifest_sha256);
    if journal.generation_manifest_sha256.as_deref() != Some(expected_digest.as_str()) {
        return Err(recovery_corrupt(
            "journal for committed generation does not match CURRENT manifest digest",
        ));
    }
    if journal.phase != JournalPhase::Published {
        journal.phase = JournalPhase::Published;
        write_journal(path, journal)?;
        report.repaired_journals += 1;
    }
    Ok(())
}

/// Verified reachability oracle shared by recovery and retention/GC.
///
/// Reachability is exactly: validated `CURRENT`, its configured finite verified
/// ancestor window, and every active checkpoint root whose manifest digest
/// matches the registry. Directory enumeration, age, PID, and UUID order never
/// grant reachability. Live reader leases are enforced separately by exclusive
/// lease acquisition before any move/delete.
pub(crate) fn compute_reachable_generations(
    root: &Path,
    selected: &crate::ResolvedProjectGeneration,
    checkpoint_roots: &[(Uuid, [u8; 32])],
    retained_ancestors: usize,
) -> Result<BTreeSet<Uuid>, GfError> {
    if retained_ancestors > MAX_RETAINED_ANCESTORS {
        return Err(project_error(
            ProjectErrorCode::ResourceLimit,
            format!(
                "retained_ancestors={retained_ancestors} exceeds MAX_RETAINED_ANCESTORS={MAX_RETAINED_ANCESTORS}"
            ),
        ));
    }
    let mut retained = BTreeSet::new();
    retained.insert(selected.generation_uuid());
    let mut parent = selected.parent_generation_uuid();
    for _ in 0..retained_ancestors {
        let Some(uuid) = parent else {
            break;
        };
        if !retained.insert(uuid) {
            return Err(recovery_corrupt("generation ancestry contains a cycle"));
        }
        parent = validated_generation_parent(root, uuid)?;
    }
    for (uuid, expected_digest) in checkpoint_roots {
        let actual_digest = validated_generation_manifest_sha256(root, *uuid)?;
        if actual_digest != *expected_digest {
            return Err(recovery_corrupt(
                "checkpoint generation manifest digest does not match registry",
            ));
        }
        retained.insert(*uuid);
    }
    Ok(retained)
}

/// Outcome of one unreachable-generation cleanup attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GenerationCleanupOutcome {
    /// Generation directory moved to trash and deleted.
    Removed,
    /// A live shared/exclusive lease blocked cleanup.
    SkippedLiveLease,
    /// No generation directory was present.
    Absent,
    /// Generation is in the verified reachable set.
    Retained,
}

/// Move an unreachable generation to trash and delete it (ADR 0013 GC path).
///
/// Rechecks reachability and `CURRENT` under the caller's writer lock before
/// the move. A busy lease skips without waiting. Crash boundaries reuse the
/// existing `project.after_gc_move` / `project.after_gc_delete` failpoints.
pub(crate) fn cleanup_unreachable_generation(
    root: &Path,
    generation_uuid: Uuid,
    checkpoint_roots: &[(Uuid, [u8; 32])],
    retained_ancestors: usize,
) -> Result<GenerationCleanupOutcome, GfError> {
    let generation_name = generation_uuid.hyphenated().to_string();
    let generation_path = root.join(GENERATIONS_DIR).join(&generation_name);
    let trash_root = ensure_machine_directory(root, Path::new(TRASH_DIR))?;
    let trash_path = trash_root.join(&generation_name);

    if trash_path.exists() {
        remove_trash_entry(
            root,
            &trash_root,
            &trash_path,
            generation_uuid,
            generation_uuid,
        )?;
        return Ok(GenerationCleanupOutcome::Removed);
    }
    if !generation_path.exists() {
        return Ok(GenerationCleanupOutcome::Absent);
    }
    reject_real_directory(&generation_path)?;
    let lease_path = generation_path.join("lease.lock");
    let _lease = if lease_path.exists() {
        let lease = open_regular_lock(&lease_path)?;
        if !crate::file_lock::try_lock_exclusive(&lease).map_err(storage_io)? {
            return Ok(GenerationCleanupOutcome::SkippedLiveLease);
        }
        Some(lease)
    } else {
        None
    };

    let current = resolve_project_generation(root).map_err(map_recovery_resolution)?;
    if current.generation_uuid() == generation_uuid {
        return Ok(GenerationCleanupOutcome::Retained);
    }
    // Fresh reachability after lease acquisition (ADR 0013 step 4).
    let reachable =
        compute_reachable_generations(root, &current, checkpoint_roots, retained_ancestors)?;
    if reachable.contains(&generation_uuid) {
        return Ok(GenerationCleanupOutcome::Retained);
    }

    std::fs::rename(&generation_path, &trash_path).map_err(storage_io)?;
    sync_directory(&root.join(GENERATIONS_DIR))?;
    sync_directory(&trash_root)?;
    project_failpoint::hit(
        "project.after_gc_move",
        Some(generation_uuid),
        Some(generation_uuid),
        "GC",
        true,
    )?;
    remove_trash_entry(
        root,
        &trash_root,
        &trash_path,
        generation_uuid,
        generation_uuid,
    )?;
    Ok(GenerationCleanupOutcome::Removed)
}

fn cleanup_abandoned_generation(
    root: &Path,
    transaction_uuid: Uuid,
    generation_uuid: Uuid,
    request_fingerprint: &str,
    retained: &BTreeSet<Uuid>,
) -> Result<u64, GfError> {
    if retained.contains(&generation_uuid) {
        return Ok(0);
    }
    let generation_name = generation_uuid.hyphenated().to_string();
    let generation_path = root.join(GENERATIONS_DIR).join(&generation_name);
    let attempt_path = root
        .join(ATTEMPTS_DIR)
        .join(transaction_uuid.hyphenated().to_string())
        .join(request_fingerprint);
    let mut removed = 0;
    if attempt_path.exists() {
        reject_real_directory(&attempt_path)?;
        std::fs::remove_dir_all(&attempt_path).map_err(storage_io)?;
        sync_directory(
            attempt_path
                .parent()
                .expect("machine attempt path has a parent"),
        )?;
        let transaction_attempt_root = attempt_path
            .parent()
            .expect("machine attempt path has a parent");
        match std::fs::remove_dir(transaction_attempt_root) {
            Ok(()) => sync_directory(&root.join(ATTEMPTS_DIR))?,
            Err(error) if error.kind() == std::io::ErrorKind::DirectoryNotEmpty => {}
            Err(error) => return Err(storage_io(error)),
        }
        removed = 1;
    }
    let trash_root = ensure_machine_directory(root, Path::new(TRASH_DIR))?;
    let trash_path = trash_root.join(&generation_name);

    if trash_path.exists() {
        remove_trash_entry(
            root,
            &trash_root,
            &trash_path,
            transaction_uuid,
            generation_uuid,
        )?;
        return Ok(1);
    }
    if !generation_path.exists() {
        return Ok(removed);
    }
    reject_real_directory(&generation_path)?;
    let lease_path = generation_path.join("lease.lock");
    let _lease = if lease_path.exists() {
        let lease = open_regular_lock(&lease_path)?;
        if !crate::file_lock::try_lock_exclusive(&lease).map_err(storage_io)? {
            return Ok(0);
        }
        Some(lease)
    } else {
        None
    };

    let current = resolve_project_generation(root).map_err(map_recovery_resolution)?;
    if current.generation_uuid() == generation_uuid {
        return Err(recovery_corrupt(
            "cleanup candidate became the committed generation",
        ));
    }
    std::fs::rename(&generation_path, &trash_path).map_err(storage_io)?;
    sync_directory(&root.join(GENERATIONS_DIR))?;
    sync_directory(&trash_root)?;
    project_failpoint::hit(
        "project.after_gc_move",
        Some(transaction_uuid),
        Some(generation_uuid),
        "GC",
        true,
    )?;
    remove_trash_entry(
        root,
        &trash_root,
        &trash_path,
        transaction_uuid,
        generation_uuid,
    )?;
    Ok(1)
}

fn remove_trash_entry(
    root: &Path,
    trash_root: &Path,
    trash_path: &Path,
    transaction_uuid: Uuid,
    generation_uuid: Uuid,
) -> Result<(), GfError> {
    reject_real_directory(trash_path)?;
    if resolve_project_generation(root)
        .map_err(map_recovery_resolution)?
        .generation_uuid()
        == generation_uuid
    {
        return Err(recovery_corrupt(
            "trash entry is reachable from the committed pointer",
        ));
    }
    std::fs::remove_dir_all(trash_path).map_err(storage_io)?;
    sync_directory(trash_root)?;
    project_failpoint::hit(
        "project.after_gc_delete",
        Some(transaction_uuid),
        Some(generation_uuid),
        "GC",
        true,
    )
}

/// Delete a trash entry that is not the committed generation.
pub(crate) fn cleanup_trash_generation(
    root: &Path,
    generation_uuid: Uuid,
) -> Result<GenerationCleanupOutcome, GfError> {
    let trash_root = root.join(TRASH_DIR);
    if !trash_root.exists() {
        return Ok(GenerationCleanupOutcome::Absent);
    }
    reject_real_directory(&trash_root)?;
    let trash_path = trash_root.join(generation_uuid.hyphenated().to_string());
    if !trash_path.exists() {
        return Ok(GenerationCleanupOutcome::Absent);
    }
    remove_trash_entry(
        root,
        &trash_root,
        &trash_path,
        generation_uuid,
        generation_uuid,
    )?;
    Ok(GenerationCleanupOutcome::Removed)
}

fn count_unknown_generation_entries(
    root: &Path,
    retained: &BTreeSet<Uuid>,
) -> Result<u64, GfError> {
    let generations_root = root.join(GENERATIONS_DIR);
    let entries = bounded_directory_entries(&generations_root)?;
    let mut unknown = 0_u64;
    for path in entries {
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            unknown += 1;
            continue;
        };
        let Some(uuid) = parse_canonical_uuid(name) else {
            unknown += 1;
            continue;
        };
        if !retained.contains(&uuid) {
            // A valid UUID directory without a recoverable journal is
            // preserved; directory enumeration never grants cleanup authority.
            unknown += 1;
        }
    }
    Ok(unknown)
}

pub(crate) fn bounded_directory_entries(root: &Path) -> Result<Vec<PathBuf>, GfError> {
    reject_real_directory(root)?;
    let mut entries = Vec::new();
    for entry in std::fs::read_dir(root).map_err(storage_io)? {
        if entries.len() >= MAX_RECOVERY_ENTRIES {
            return Err(recovery_corrupt("recovery entry limit exceeded"));
        }
        entries.push(entry.map_err(storage_io)?.path());
    }
    Ok(entries)
}

fn journal_file_uuid(path: &Path) -> Option<Uuid> {
    let file_name = path.file_name()?.to_str()?;
    let stem = file_name.strip_suffix(".json")?;
    parse_canonical_uuid(stem)
}

pub(crate) fn parse_canonical_uuid(value: &str) -> Option<Uuid> {
    let uuid = Uuid::parse_str(value).ok()?;
    (uuid.hyphenated().to_string() == value).then_some(uuid)
}

pub(crate) fn reject_real_directory(path: &Path) -> Result<(), GfError> {
    let metadata = std::fs::symlink_metadata(path).map_err(storage_io)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(recovery_corrupt(
            "recovery path is linked or not a directory",
        ));
    }
    Ok(())
}

fn digest_hex(digest: [u8; 32]) -> String {
    use std::fmt::Write as _;

    digest
        .iter()
        .fold(String::with_capacity(64), |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to String cannot fail");
            output
        })
}

pub(crate) fn map_recovery_resolution(error: GfError) -> GfError {
    if error.code() == "GF_PROJECT_CORRUPT" {
        recovery_corrupt("committed publication record is invalid or ambiguous")
    } else {
        error
    }
}

pub(crate) fn storage_io(error: impl std::fmt::Display) -> GfError {
    GfError::Storage(error.to_string())
}

pub(crate) fn project_error(code: ProjectErrorCode, message: impl Into<String>) -> GfError {
    GfError::Project {
        code,
        message: message.into(),
    }
}

pub(crate) fn recovery_corrupt(cause: &str) -> GfError {
    project_error(
        ProjectErrorCode::ProjectCorrupt,
        format!(
            "phase=RECOVERY committed=unknown cause={cause}; preserve the project and restore \
             CURRENT plus its exact committed generation from a verified backup"
        ),
    )
}

#[cfg(test)]
mod tests {
    use std::process::Command;
    use std::time::{Duration, Instant};

    use sha2::{Digest, Sha256};

    use super::*;
    use crate::{
        ProjectCapability, ProjectGenerationRequest, ProjectParticipant,
        ProjectParticipantEncoding, ProjectStageOutcome, open_or_initialize_project,
        stage_project_generation, stage_project_generation_optimistic,
    };

    const ENABLE_COOKIE: &str = "graphforge-internal-subprocess-v1";
    const WRITER_HELPER: &str = "project_recovery::tests::subprocess_publication_writer";
    const RECOVERY_HELPER: &str = "project_recovery::tests::subprocess_recovery_runner";
    const INITIALIZER_HELPER: &str = "project_recovery::tests::subprocess_initializer";
    const PRE_COMMIT_FAILPOINTS: &[&str] = &[
        "project.after_writer_lock",
        "project.after_journal_preparing",
        "project.after_participant_write",
        "project.after_participant_fsync",
        "project.after_participant_dir_fsync",
        "project.after_journal_staged",
        "project.after_domain_validation",
        "project.after_composite_validation",
        "project.after_journal_validated",
        "project.after_manifest_write",
        "project.after_manifest_fsync",
        "project.after_generation_dir_fsync",
        "project.after_journal_durable",
        "project.after_current_temp_write",
        "project.after_current_temp_fsync",
        "project.before_current_replace",
    ];
    const POST_COMMIT_FAILPOINTS: &[&str] = &[
        "project.after_current_replace",
        "project.after_root_fsync",
        "project.after_journal_published",
    ];

    fn wait_for_writer_lock_release(root: &Path) {
        let lock = open_regular_lock(&root.join(LOCKS_DIR).join(WRITER_LOCK_FILE)).unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            if crate::file_lock::try_lock_exclusive(&lock).unwrap() {
                let acquired_at = Instant::now();
                crate::file_lock::unlock(&lock).unwrap();
                assert!(
                    acquired_at < deadline,
                    "writer.lock remained owned after recovery completed"
                );
                return;
            }
            assert!(
                Instant::now() < deadline,
                "writer.lock remained owned after recovery completed"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }

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

    fn participants(set: &str) -> Vec<ProjectParticipant> {
        match set {
            "graph" => vec![participant("graph", "nodes")],
            "provenance" => vec![
                participant("graph", "nodes"),
                participant("provenance", "events"),
            ],
            "knowledge" => vec![
                participant("graph", "nodes"),
                participant("provenance", "events"),
                participant("knowledge", "assertions"),
            ],
            other => panic!("unknown test participant set {other}"),
        }
    }

    fn capabilities(set: &str) -> Vec<ProjectCapability> {
        let mut capabilities = vec![ProjectCapability {
            capability_id: "graph".into(),
            capability_version: 1,
        }];
        if matches!(set, "provenance" | "knowledge") {
            capabilities.push(ProjectCapability {
                capability_id: "provenance".into(),
                capability_version: 1,
            });
        }
        if set == "knowledge" {
            capabilities.push(ProjectCapability {
                capability_id: "knowledge".into(),
                capability_version: 1,
            });
        }
        capabilities
    }

    fn spawn_writer(
        root: &Path,
        transaction_uuid: Uuid,
        generation_uuid: Uuid,
        set: &str,
        failpoint: &str,
    ) -> std::process::ExitStatus {
        Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg(WRITER_HELPER)
            .arg("--nocapture")
            .env("GRAPHFORGE_TEST_PROJECT_ROOT", root)
            .env(
                "GRAPHFORGE_TEST_TRANSACTION_UUID",
                transaction_uuid.hyphenated().to_string(),
            )
            .env(
                "GRAPHFORGE_TEST_GENERATION_UUID",
                generation_uuid.hyphenated().to_string(),
            )
            .env("GRAPHFORGE_TEST_PARTICIPANT_SET", set)
            .env("GRAPHFORGE_PROJECT_FAILPOINTS", ENABLE_COOKIE)
            .env("GRAPHFORGE_PROJECT_FAILPOINT", failpoint)
            .status()
            .unwrap()
    }

    fn spawn_optimistic_writer(
        root: &Path,
        transaction_uuid: Uuid,
        generation_uuid: Uuid,
        failpoint: &str,
    ) -> std::process::ExitStatus {
        Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg(WRITER_HELPER)
            .arg("--nocapture")
            .env("GRAPHFORGE_TEST_PROJECT_ROOT", root)
            .env(
                "GRAPHFORGE_TEST_TRANSACTION_UUID",
                transaction_uuid.hyphenated().to_string(),
            )
            .env(
                "GRAPHFORGE_TEST_GENERATION_UUID",
                generation_uuid.hyphenated().to_string(),
            )
            .env("GRAPHFORGE_TEST_PARTICIPANT_SET", "graph")
            .env("GRAPHFORGE_TEST_OPTIMISTIC", "1")
            .env("GRAPHFORGE_PROJECT_FAILPOINTS", ENABLE_COOKIE)
            .env("GRAPHFORGE_PROJECT_FAILPOINT", failpoint)
            .status()
            .unwrap()
    }

    fn assert_reopen(root: &Path, expected: Uuid, set: &str, expect_child: bool) {
        let before_recovery = resolve_project_generation(root).unwrap();
        assert_eq!(before_recovery.generation_uuid(), expected);
        let expected_manifest_digest = before_recovery.manifest_sha256();
        let first = recover_project_transactions(root).unwrap();
        assert_eq!(first.selected_generation_uuid, expected);
        let resolved = resolve_project_generation(root).unwrap();
        assert_eq!(resolved.generation_uuid(), expected);
        assert_eq!(resolved.manifest_sha256(), expected_manifest_digest);
        let manifest_bytes =
            std::fs::read(resolved.generation_root().join("manifest.json")).unwrap();
        let manifest_digest: [u8; 32] = Sha256::digest(&manifest_bytes).into();
        assert_eq!(manifest_digest, expected_manifest_digest);
        let manifest: serde_json::Value = serde_json::from_slice(&manifest_bytes).unwrap();
        if expect_child {
            for participant in participants(set) {
                let path = resolved
                    .participant_path(&participant.capability_id, &participant.record_family_id)
                    .unwrap();
                let bytes = std::fs::read(path).unwrap();
                assert_eq!(bytes, participant.bytes);
                let persisted = manifest["participants"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|entry| {
                        entry["capability_id"] == participant.capability_id
                            && entry["record_family_id"] == participant.record_family_id
                    })
                    .unwrap();
                let content_digest: [u8; 32] = Sha256::digest(&bytes).into();
                assert_eq!(
                    persisted["content_sha256"].as_str().unwrap(),
                    digest_hex(content_digest)
                );
            }
        }
        wait_for_writer_lock_release(root);
        let second = recover_project_transactions(root).unwrap();
        assert_eq!(second.selected_generation_uuid, expected);
        assert_eq!(second.repaired_journals, 0);
        assert_eq!(second.aborted_journals, 0);
    }

    #[test]
    fn subprocess_kill_matrix_never_exposes_a_partial_generation() {
        for (failpoint, committed) in PRE_COMMIT_FAILPOINTS
            .iter()
            .map(|name| (*name, false))
            .chain(POST_COMMIT_FAILPOINTS.iter().map(|name| (*name, true)))
        {
            let root = tempfile::tempdir().unwrap();
            let parent = open_or_initialize_project(root.path())
                .unwrap()
                .generation_uuid();
            let transaction_uuid = Uuid::now_v7();
            let generation_uuid = Uuid::now_v7();
            let status = spawn_writer(
                root.path(),
                transaction_uuid,
                generation_uuid,
                "graph",
                failpoint,
            );
            assert_eq!(
                status.code(),
                Some(crate::project_failpoint::exit_code()),
                "{failpoint} did not terminate at the named boundary"
            );
            assert_reopen(
                root.path(),
                if committed { generation_uuid } else { parent },
                "graph",
                committed,
            );
        }
    }

    #[test]
    fn optimistic_commit_failpoints_never_expose_a_partial_generation() {
        for failpoint in [
            "project.after_optimistic_commit_lock",
            "project.after_optimistic_promotion",
        ] {
            let root = tempfile::tempdir().unwrap();
            let parent = open_or_initialize_project(root.path())
                .unwrap()
                .generation_uuid();
            let status =
                spawn_optimistic_writer(root.path(), Uuid::now_v7(), Uuid::now_v7(), failpoint);
            assert_eq!(status.code(), Some(crate::project_failpoint::exit_code()));
            assert_reopen(root.path(), parent, "graph", false);
        }
    }

    #[test]
    fn container_creation_failpoints_resume_only_exact_current_format() {
        for failpoint in [
            "project.after_format_fsync",
            "project.after_container_dir_fsync",
        ] {
            for (active, killed) in [
                (failpoint.to_owned(), true),
                (format!("{failpoint}.error"), false),
            ] {
                let root = tempfile::tempdir().unwrap();
                let status = Command::new(std::env::current_exe().unwrap())
                    .arg("--exact")
                    .arg(INITIALIZER_HELPER)
                    .arg("--nocapture")
                    .env("GRAPHFORGE_TEST_PROJECT_ROOT", root.path())
                    .env("GRAPHFORGE_PROJECT_FAILPOINTS", ENABLE_COOKIE)
                    .env("GRAPHFORGE_PROJECT_FAILPOINT", active)
                    .status()
                    .unwrap();
                if killed {
                    assert_eq!(status.code(), Some(crate::project_failpoint::exit_code()));
                } else {
                    assert!(status.success());
                }

                let reopened = open_or_initialize_project(root.path()).unwrap();
                assert_eq!(
                    resolve_project_generation(root.path())
                        .unwrap()
                        .generation_uuid(),
                    reopened.generation_uuid()
                );
            }
        }
    }

    #[test]
    fn exact_format_does_not_authorize_mutating_an_unknown_layout() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(
            root.path().join(crate::FORMAT_FILE),
            crate::PROJECT_FORMAT_BYTES,
        )
        .unwrap();
        std::fs::create_dir(root.path().join(GENERATIONS_DIR)).unwrap();
        std::fs::write(root.path().join("unknown.db"), b"do-not-touch").unwrap();
        let before = std::fs::read(root.path().join("unknown.db")).unwrap();

        let error = open_or_initialize_project(root.path()).unwrap_err();

        assert_eq!(error.code(), "GF_UNSUPPORTED_PROJECT_FORMAT");
        assert_eq!(
            std::fs::read(root.path().join("unknown.db")).unwrap(),
            before
        );
        assert!(!root.path().join(crate::CURRENT_FILE).exists());
    }

    #[test]
    fn recovery_removes_only_validated_atomic_journal_temps() {
        let root = tempfile::tempdir().unwrap();
        open_or_initialize_project(root.path()).unwrap();
        let transactions = root.path().join(TRANSACTIONS_DIR);
        std::fs::create_dir(&transactions).unwrap();
        let writer_temp = transactions.join(".atomicwriteZ9y8X7");
        std::fs::create_dir(&writer_temp).unwrap();
        std::fs::write(writer_temp.join("tmpfile.tmp"), b"partial journal").unwrap();

        recover_project_transactions(root.path()).unwrap();

        assert!(!writer_temp.exists());
    }

    #[test]
    fn recovery_rejects_spoofed_atomic_journal_temp() {
        let root = tempfile::tempdir().unwrap();
        open_or_initialize_project(root.path()).unwrap();
        let transactions = root.path().join(TRANSACTIONS_DIR);
        std::fs::create_dir(&transactions).unwrap();
        let spoofed = transactions.join(".atomicwriteZ9y8X7");
        std::fs::create_dir(&spoofed).unwrap();
        std::fs::write(spoofed.join("unexpected"), b"preserve").unwrap();

        let error = recover_project_transactions(root.path()).unwrap_err();

        assert_eq!(error.code(), "GF_PROJECT_CORRUPT");
        assert_eq!(
            std::fs::read(spoofed.join("unexpected")).unwrap(),
            b"preserve"
        );
    }

    #[test]
    fn injected_operation_errors_report_exact_commit_state() {
        for failpoint in PRE_COMMIT_FAILPOINTS
            .iter()
            .copied()
            .chain(std::iter::once("project.after_current_replace"))
        {
            let root = tempfile::tempdir().unwrap();
            let parent = open_or_initialize_project(root.path())
                .unwrap()
                .generation_uuid();
            let transaction_uuid = Uuid::now_v7();
            let generation_uuid = Uuid::now_v7();
            let status = spawn_writer(
                root.path(),
                transaction_uuid,
                generation_uuid,
                "graph",
                &format!("{failpoint}.error"),
            );
            assert!(status.success(), "{failpoint}.error helper failed");
            let committed = failpoint == "project.after_current_replace";
            assert_reopen(
                root.path(),
                if committed { generation_uuid } else { parent },
                "graph",
                committed,
            );
        }
    }

    #[test]
    fn provenance_and_knowledge_sets_follow_the_same_commit_boundary() {
        for set in ["provenance", "knowledge"] {
            for (failpoint, committed) in [
                ("project.after_participant_fsync", false),
                ("project.after_current_replace", true),
            ] {
                let root = tempfile::tempdir().unwrap();
                let parent = open_or_initialize_project(root.path())
                    .unwrap()
                    .generation_uuid();
                let transaction_uuid = Uuid::now_v7();
                let generation_uuid = Uuid::now_v7();
                let status = spawn_writer(
                    root.path(),
                    transaction_uuid,
                    generation_uuid,
                    set,
                    failpoint,
                );
                assert_eq!(status.code(), Some(crate::project_failpoint::exit_code()));
                assert_reopen(
                    root.path(),
                    if committed { generation_uuid } else { parent },
                    set,
                    committed,
                );
            }
        }
    }

    #[test]
    fn recovered_aborted_transaction_can_retry_with_identical_inputs() {
        let root = tempfile::tempdir().unwrap();
        open_or_initialize_project(root.path()).unwrap();
        let transaction_uuid = Uuid::now_v7();
        let generation_uuid = Uuid::now_v7();
        let status = spawn_writer(
            root.path(),
            transaction_uuid,
            generation_uuid,
            "graph",
            "project.after_journal_staged",
        );
        assert_eq!(status.code(), Some(crate::project_failpoint::exit_code()));
        recover_project_transactions(root.path()).unwrap();
        wait_for_writer_lock_release(root.path());
        let request = ProjectGenerationRequest {
            transaction_uuid,
            generation_uuid,
            capabilities: capabilities("graph"),
            participants: participants("graph"),
        };

        let ProjectStageOutcome::Staged(staged) =
            stage_project_generation(root.path(), &request).unwrap()
        else {
            panic!("aborted transaction unexpectedly replayed as published");
        };
        staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .unwrap()
            .publish()
            .unwrap();
        assert_eq!(
            resolve_project_generation(root.path())
                .unwrap()
                .generation_uuid(),
            generation_uuid
        );
    }

    #[test]
    fn reachable_ancestor_with_stale_journal_is_repaired_not_deleted() {
        let root = tempfile::tempdir().unwrap();
        open_or_initialize_project(root.path()).unwrap();
        let first = ProjectGenerationRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
            capabilities: capabilities("graph"),
            participants: participants("graph"),
        };
        let ProjectStageOutcome::Staged(staged) =
            stage_project_generation(root.path(), &first).unwrap()
        else {
            panic!("new transaction replayed");
        };
        staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .unwrap()
            .publish()
            .unwrap();
        let first_journal_path = root
            .path()
            .join(TRANSACTIONS_DIR)
            .join(format!("{}.json", first.transaction_uuid.hyphenated()));
        let mut first_journal = read_journal(&first_journal_path).unwrap();
        first_journal.phase = JournalPhase::Durable;
        write_journal(&first_journal_path, &first_journal).unwrap();

        let second = ProjectGenerationRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
            capabilities: capabilities("graph"),
            participants: participants("graph"),
        };
        let ProjectStageOutcome::Staged(staged) =
            stage_project_generation(root.path(), &second).unwrap()
        else {
            panic!("new transaction replayed");
        };
        staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .unwrap()
            .publish()
            .unwrap();

        let report = recover_project_transactions(root.path()).unwrap();

        assert_eq!(report.repaired_journals, 1);
        assert_eq!(
            read_journal(&first_journal_path).unwrap().phase,
            JournalPhase::Published
        );
        assert!(
            root.path()
                .join(GENERATIONS_DIR)
                .join(first.generation_uuid.hyphenated().to_string())
                .exists()
        );
    }

    #[test]
    fn torn_journal_fails_closed_without_changing_current() {
        let root = tempfile::tempdir().unwrap();
        let parent = open_or_initialize_project(root.path())
            .unwrap()
            .generation_uuid();
        let transaction_uuid = Uuid::now_v7();
        let generation_uuid = Uuid::now_v7();
        let status = spawn_writer(
            root.path(),
            transaction_uuid,
            generation_uuid,
            "graph",
            "project.after_journal_staged",
        );
        assert_eq!(status.code(), Some(crate::project_failpoint::exit_code()));
        std::fs::write(
            root.path()
                .join(TRANSACTIONS_DIR)
                .join(format!("{}.json", transaction_uuid.hyphenated())),
            b"{torn",
        )
        .unwrap();

        let error = recover_project_transactions(root.path()).unwrap_err();
        assert_eq!(error.code(), "GF_PROJECT_CORRUPT");
        assert!(error.to_string().contains("verified backup"));
        assert_eq!(
            resolve_project_generation(root.path())
                .unwrap()
                .generation_uuid(),
            parent
        );
    }

    #[test]
    fn invalid_current_returns_recovery_guidance_without_election() {
        let root = tempfile::tempdir().unwrap();
        let selected = open_or_initialize_project(root.path())
            .unwrap()
            .generation_uuid();
        std::fs::write(root.path().join(crate::CURRENT_FILE), b"{invalid\n").unwrap();

        let error = recover_project_transactions(root.path()).unwrap_err();

        assert_eq!(error.code(), "GF_PROJECT_CORRUPT");
        assert!(error.to_string().contains("verified backup"));
        assert!(
            root.path()
                .join(GENERATIONS_DIR)
                .join(selected.hyphenated().to_string())
                .exists()
        );
    }

    #[test]
    fn live_writer_lock_blocks_recovery_without_metadata_heuristics() {
        let root = tempfile::tempdir().unwrap();
        open_or_initialize_project(root.path()).unwrap();
        let lock_dir = ensure_machine_directory(root.path(), Path::new(LOCKS_DIR)).unwrap();
        let lock = open_regular_lock(&lock_dir.join(WRITER_LOCK_FILE)).unwrap();
        crate::file_lock::lock_exclusive(&lock).unwrap();

        let error = recover_project_transactions(root.path()).unwrap_err();

        assert_eq!(error.code(), "GF_WRITER_BUSY");
    }

    #[test]
    fn live_writer_lock_blocks_project_removal_until_the_writer_is_idle() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("project");
        open_or_initialize_project(&root).unwrap();
        let lock_dir = ensure_machine_directory(&root, Path::new(LOCKS_DIR)).unwrap();
        let lock = open_regular_lock(&lock_dir.join(WRITER_LOCK_FILE)).unwrap();
        crate::file_lock::lock_exclusive(&lock).unwrap();

        let error = remove_durable_project_root(&root).unwrap_err();

        assert_eq!(error.code(), "GF_WRITER_BUSY");
        assert!(root.exists());
        crate::file_lock::unlock(&lock).unwrap();
        drop(lock);

        remove_durable_project_root(&root).unwrap();
        assert!(!root.exists());
        assert_eq!(
            std::fs::read_dir(parent.path())
                .unwrap()
                .filter_map(Result::ok)
                .filter(|entry| {
                    entry
                        .file_name()
                        .to_string_lossy()
                        .starts_with(".graphforge-admission-")
                })
                .count(),
            1,
            "the persistent parent-scoped admission lock must survive removal"
        );
    }

    #[test]
    fn corrupt_knowledge_bytes_do_not_block_graph_only_reopen_or_recovery() {
        let root = tempfile::tempdir().unwrap();
        open_or_initialize_project(root.path()).unwrap();
        let request = ProjectGenerationRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
            capabilities: capabilities("knowledge"),
            participants: participants("knowledge"),
        };
        let ProjectStageOutcome::Staged(staged) =
            stage_project_generation(root.path(), &request).unwrap()
        else {
            panic!("new transaction replayed");
        };
        staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .unwrap()
            .publish()
            .unwrap();
        let resolved = resolve_project_generation(root.path()).unwrap();
        std::fs::write(
            resolved
                .participant_path("knowledge", "assertions")
                .unwrap(),
            b"future-or-corrupt-knowledge",
        )
        .unwrap();

        let report = recover_project_transactions(root.path()).unwrap();
        assert_eq!(report.selected_generation_uuid, request.generation_uuid);
        let graph = resolve_project_generation(root.path())
            .unwrap()
            .participant_path("graph", "nodes")
            .unwrap();
        assert_eq!(std::fs::read(graph).unwrap(), b"graph:nodes");
    }

    #[test]
    fn gc_crash_points_preserve_current_and_recover_idempotently() {
        for failpoint in ["project.after_gc_move", "project.after_gc_delete"] {
            let root = tempfile::tempdir().unwrap();
            let current = open_or_initialize_project(root.path())
                .unwrap()
                .generation_uuid();
            let transaction_uuid = Uuid::now_v7();
            let generation_uuid = Uuid::now_v7();
            let status = spawn_writer(
                root.path(),
                transaction_uuid,
                generation_uuid,
                "graph",
                "project.after_journal_staged",
            );
            assert_eq!(status.code(), Some(crate::project_failpoint::exit_code()));
            let status = Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg(RECOVERY_HELPER)
                .arg("--nocapture")
                .env("GRAPHFORGE_TEST_PROJECT_ROOT", root.path())
                .env("GRAPHFORGE_PROJECT_FAILPOINTS", ENABLE_COOKIE)
                .env("GRAPHFORGE_PROJECT_FAILPOINT", failpoint)
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(crate::project_failpoint::exit_code()));

            assert_reopen(root.path(), current, "graph", false);
            assert!(
                !root
                    .path()
                    .join(GENERATIONS_DIR)
                    .join(generation_uuid.hyphenated().to_string())
                    .exists()
            );
        }
    }

    #[test]
    fn open_recovers_pre_publication_kill_to_parent_idempotently() {
        let root = tempfile::tempdir().unwrap();
        let parent = open_or_initialize_project(root.path())
            .unwrap()
            .generation_uuid();
        let abandoned = Uuid::now_v7();
        let status = spawn_writer(
            root.path(),
            Uuid::now_v7(),
            abandoned,
            "graph",
            "project.after_journal_durable",
        );
        assert_eq!(status.code(), Some(crate::project_failpoint::exit_code()));

        let (first, evidence) = open_or_initialize_project_with_recovery(root.path()).unwrap();
        assert_eq!(first.generation_uuid(), parent);
        assert_eq!(
            evidence.selected_generation_class,
            ProjectRecoveryGenerationClass::CommittedCurrent
        );
        assert_eq!(evidence.kind, ProjectOpenRecoveryKind::ProjectOpen);
        assert!(evidence.work_detected);
        assert!(evidence.aborted_journals >= 1 || evidence.removed_generations >= 1);
        assert!(evidence.deferred.is_none());
        assert!(
            !root
                .path()
                .join(GENERATIONS_DIR)
                .join(abandoned.hyphenated().to_string())
                .exists()
        );

        let (second, again) = open_or_initialize_project_with_recovery(root.path()).unwrap();
        assert_eq!(second.generation_uuid(), parent);
        assert_eq!(again.selected_generation_uuid, parent);
        assert!(!again.work_detected);
        assert_eq!(again.aborted_journals, 0);
        assert_eq!(again.removed_generations, 0);
    }

    #[test]
    fn open_recovers_post_publication_kill_to_child_and_repairs_journal() {
        let root = tempfile::tempdir().unwrap();
        open_or_initialize_project(root.path()).unwrap();
        let child = Uuid::now_v7();
        let status = spawn_writer(
            root.path(),
            Uuid::now_v7(),
            child,
            "graph",
            "project.after_current_replace",
        );
        assert_eq!(status.code(), Some(crate::project_failpoint::exit_code()));

        let (opened, evidence) = open_or_initialize_project_with_recovery(root.path()).unwrap();
        assert_eq!(opened.generation_uuid(), child);
        assert_eq!(
            evidence.selected_generation_class.as_str(),
            "committed_current"
        );
        assert!(evidence.work_detected);
        assert!(evidence.repaired_journals >= 1);
        assert!(evidence.deferred.is_none());

        let (again, second) = open_or_initialize_project_with_recovery(root.path()).unwrap();
        assert_eq!(again.generation_uuid(), child);
        assert!(!second.work_detected);
        assert_eq!(second.repaired_journals, 0);
    }

    #[test]
    fn open_with_live_writer_defers_cleanup_and_pins_valid_current() {
        let root = tempfile::tempdir().unwrap();
        let selected = open_or_initialize_project(root.path())
            .unwrap()
            .generation_uuid();
        // Leave an abandoned journal so open detects work and attempts recovery.
        let status = spawn_writer(
            root.path(),
            Uuid::now_v7(),
            Uuid::now_v7(),
            "graph",
            "project.after_journal_staged",
        );
        assert_eq!(status.code(), Some(crate::project_failpoint::exit_code()));
        let lock_dir = ensure_machine_directory(root.path(), Path::new(LOCKS_DIR)).unwrap();
        let lock = open_regular_lock(&lock_dir.join(WRITER_LOCK_FILE)).unwrap();
        crate::file_lock::lock_exclusive(&lock).unwrap();

        let (opened, evidence) = open_or_initialize_project_with_recovery(root.path()).unwrap();
        assert_eq!(opened.generation_uuid(), selected);
        assert!(evidence.work_detected);
        assert_eq!(
            evidence.deferred,
            Some(ProjectRecoveryDeferral::LiveWriterOwnsKernelLock)
        );
        assert_eq!(
            evidence.deferred.unwrap().as_str(),
            "live_writer_owns_kernel_lock"
        );
        assert_eq!(evidence.repaired_journals, 0);
        assert_eq!(evidence.removed_generations, 0);
    }

    #[test]
    fn open_corrupt_current_fails_closed_without_fallback_election() {
        let root = tempfile::tempdir().unwrap();
        let selected = open_or_initialize_project(root.path())
            .unwrap()
            .generation_uuid();
        std::fs::write(root.path().join(CURRENT_FILE), b"{invalid\n").unwrap();

        let error = open_or_initialize_project_with_recovery(root.path()).unwrap_err();

        assert_eq!(error.code(), "GF_PROJECT_CORRUPT");
        assert!(error.to_string().contains("verified backup"));
        assert!(
            root.path()
                .join(GENERATIONS_DIR)
                .join(selected.hyphenated().to_string())
                .exists()
        );
    }

    #[test]
    fn initialization_open_reports_distinct_recovery_kind() {
        let root = tempfile::tempdir().unwrap();
        let (opened, evidence) = open_or_initialize_project_with_recovery(root.path()).unwrap();
        assert_eq!(evidence.kind, ProjectOpenRecoveryKind::Initialization);
        assert_eq!(evidence.selected_generation_uuid, opened.generation_uuid());
        assert!(!evidence.work_detected);
        assert!(evidence.deferred.is_none());
    }

    #[test]
    fn subprocess_publication_writer() {
        if std::env::var("GRAPHFORGE_TEST_PROJECT_ROOT").is_err() {
            return;
        }
        let root = PathBuf::from(std::env::var("GRAPHFORGE_TEST_PROJECT_ROOT").unwrap());
        let transaction_uuid =
            Uuid::parse_str(&std::env::var("GRAPHFORGE_TEST_TRANSACTION_UUID").unwrap()).unwrap();
        let generation_uuid =
            Uuid::parse_str(&std::env::var("GRAPHFORGE_TEST_GENERATION_UUID").unwrap()).unwrap();
        let set = std::env::var("GRAPHFORGE_TEST_PARTICIPANT_SET").unwrap();
        let active = std::env::var("GRAPHFORGE_PROJECT_FAILPOINT").unwrap();
        let request = ProjectGenerationRequest {
            transaction_uuid,
            generation_uuid,
            capabilities: capabilities(&set),
            participants: participants(&set),
        };
        let result = (|| {
            let outcome = if std::env::var("GRAPHFORGE_TEST_OPTIMISTIC").is_ok() {
                stage_project_generation_optimistic(
                    &root,
                    &request,
                    Sha256::digest(b"optimistic-subprocess-operation").into(),
                )?
            } else {
                stage_project_generation(&root, &request)?
            };
            let ProjectStageOutcome::Staged(staged) = outcome else {
                panic!("new transaction replayed");
            };
            staged.validate(|_| Ok(()), |_, _| Ok(()))?.publish()
        })();
        let error = result.expect_err("configured failpoint did not fire");
        assert_eq!(error.code(), "GF_PUBLICATION_FAILED");
        assert!(
            error
                .to_string()
                .contains(if active == "project.after_current_replace.error" {
                    "committed=true"
                } else {
                    "committed=false"
                })
        );
    }

    #[test]
    fn subprocess_recovery_runner() {
        if std::env::var("GRAPHFORGE_TEST_PROJECT_ROOT").is_err() {
            return;
        }
        let root = PathBuf::from(std::env::var("GRAPHFORGE_TEST_PROJECT_ROOT").unwrap());
        recover_project_transactions(root).unwrap();
        panic!("configured recovery failpoint did not fire");
    }

    #[test]
    fn subprocess_initializer() {
        if std::env::var("GRAPHFORGE_TEST_PROJECT_ROOT").is_err() {
            return;
        }
        let root = PathBuf::from(std::env::var("GRAPHFORGE_TEST_PROJECT_ROOT").unwrap());
        let active = std::env::var("GRAPHFORGE_PROJECT_FAILPOINT").unwrap();
        let result = open_or_initialize_project(root);
        if active.ends_with(".error") {
            let error = result.expect_err("configured initialization error did not fire");
            assert_eq!(error.code(), "GF_PUBLICATION_FAILED");
            assert!(error.to_string().contains("committed=false"));
            return;
        }
        result.unwrap();
        panic!("configured initialization failpoint did not fire");
    }
}
