//! What an import proves before it extracts a byte: the package is admitted,
//! the target's filesystem is supported, and the disk beside the target holds
//! the import's peak (#1770).

use super::outcome::{storage, with_io_context};
use super::{PortableError, PortableErrorCode, PortableLimits};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use uuid::Uuid;

/// Free bytes on the volume that holds a path.
pub(super) type AvailableSpace = fn(&Path) -> std::io::Result<u64>;

pub(super) fn filesystem_available_space(path: &Path) -> std::io::Result<u64> {
    fs4::available_space(path)
}

/// The private stage and ownership marker an import of `target` uses.
pub(super) fn stage_paths(
    target: &Path,
    transaction_uuid: Uuid,
) -> Result<(PathBuf, PathBuf), PortableError> {
    let name = target
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            PortableError::new(PortableErrorCode::InvalidPath, "invalid import target")
        })?;
    let parent = target.parent().unwrap_or_else(|| Path::new("."));
    let id = transaction_uuid.hyphenated();
    Ok((
        parent.join(format!(".{name}.portable-v2-{id}")),
        parent.join(format!(".{name}.portable-v2-{id}.owner")),
    ))
}

/// Refuse an import that cannot finish, before anything is written for it.
///
/// The package's controls are admitted first (read-only), then the target's
/// filesystem through the same durable-publication probe project admission
/// runs, then the free space on the volume that will hold the stage and the
/// target. Each refusal keeps its own identity: an unsupported filesystem
/// stays `GF_UNSUPPORTED_FILESYSTEM`, and a volume that cannot hold the peak
/// is an insufficient-space refusal.
///
/// An import whose transaction already committed only replays, so it is
/// admitted against the replay's peak, not a fresh import's. What an
/// interrupted attempt's stage frees is credited, and the returned credit is
/// checked again where `claim_stage` reclaims that stage.
pub(super) fn admit(
    source: &Path,
    target: &Path,
    transaction_uuid: Uuid,
    limits: PortableLimits,
    cancelled: Option<&AtomicBool>,
    available: AvailableSpace,
) -> Result<StageCredit, PortableError> {
    let footprint = crate::project_portable::preflight(source, limits, cancelled)?;
    crate::filesystem_admission::filesystem_durability_preflight(target)
        .map_err(|error| filesystem_refusal(&error, target))?;
    let peak = if committed(target, transaction_uuid) {
        footprint.replay_peak_bytes()
    } else {
        footprint.peak_bytes()
    };
    let credit = StageCredit(held_bytes(target, transaction_uuid, limits)?);
    let required = peak.saturating_sub(credit.0);
    let volume = target
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let free = available(volume).map_err(|error| {
        PortableError::new(
            PortableErrorCode::Io,
            "cannot measure free disk space for import",
        )
        .with_cause(error.kind().to_string())
    })?;
    if free < required {
        return Err(insufficient(free, required));
    }
    Ok(credit)
}

fn insufficient(free: u64, required: u64) -> PortableError {
    PortableError::new(
        PortableErrorCode::InsufficientSpace,
        "insufficient disk space for import",
    )
    .with_cause(format!(
        "{free} bytes available beside the target, about {required} needed"
    ))
}

/// Whether this transaction already published into `target`: the same
/// read-only journal probe the import's replay detection uses, taken before
/// any lock. A commit it cannot see yet (one only recovery would classify) is
/// admitted as a fresh import, which needs more, never less.
fn committed(target: &Path, transaction_uuid: Uuid) -> bool {
    target.is_dir()
        && crate::published_project_transaction(target, transaction_uuid)
            .ok()
            .flatten()
            .is_some()
}

/// Bytes admission credited for an interrupted attempt's stage.
pub(super) struct StageCredit(pub(super) u64);

impl StageCredit {
    /// Refuse to reclaim a stage that no longer holds what admission credited.
    ///
    /// Admission scanned the stage; the reclaim removes it a moment later. The
    /// reclaim re-scans with the same rules and refuses if the stage now
    /// frees less (a file grew a second link, was replaced by a sparse one,
    /// or was removed), because the free-space check relied on the credit.
    pub(super) fn confirm_at_reclaim(
        &self,
        stage: &Path,
        limits: PortableLimits,
    ) -> Result<(), PortableError> {
        let mut remaining = entry_bound(limits);
        let reclaimable = reclaimable_bytes(stage, &mut remaining);
        if reclaimable < self.0 {
            return Err(PortableError::new(
                PortableErrorCode::InsufficientSpace,
                "import staging changed after its disk space was admitted",
            )
            .with_cause(format!(
                "reclaiming it frees {reclaimable} bytes, admission credited {}",
                self.0
            )));
        }
        Ok(())
    }
}

fn filesystem_refusal(error: &crate::GfError, target: &Path) -> PortableError {
    if error.code() == "GF_UNSUPPORTED_FILESYSTEM" {
        return PortableError::new(
            PortableErrorCode::UnsupportedFilesystem,
            "target filesystem is not supported",
        )
        .with_cause(crate::portable_cause::sanitized_cause(&error.to_string()));
    }
    with_io_context(storage(error), "admit import target", target)
}

/// Bytes an interrupted attempt of this same import holds in its stage, which
/// the retry removes before it writes again.
///
/// Only the stage `claim_stage` would reuse counts: a real directory beside
/// the target whose ownership marker is a regular file naming this
/// transaction. Within it only regular files with a single link count, and
/// each for what the filesystem actually allocated: a sparse file frees only
/// its allocated blocks, and a hard-linked one frees nothing by being removed.
/// The target is not credited: it is not removed.
pub(super) fn held_bytes(
    target: &Path,
    transaction_uuid: Uuid,
    limits: PortableLimits,
) -> Result<u64, PortableError> {
    let (stage, owner) = stage_paths(target, transaction_uuid)?;
    let marker = transaction_uuid.hyphenated().to_string();
    let marker_names_us = fs::symlink_metadata(&owner).is_ok_and(|metadata| metadata.is_file())
        && fs::read_to_string(&owner).ok().as_deref() == Some(marker.as_str());
    let staged = fs::symlink_metadata(&stage).is_ok_and(|metadata| metadata.is_dir());
    if !marker_names_us || !staged {
        return Ok(0);
    }
    let mut remaining = entry_bound(limits);
    Ok(reclaimable_bytes(&stage, &mut remaining))
}

fn entry_bound(limits: PortableLimits) -> u64 {
    limits.max_entries.saturating_mul(2).saturating_add(1024)
}

/// Bytes the retry of `transaction_uuid` into `target` would free by removing
/// its interrupted stage: the one credit both a clone's budget and the import's
/// admission use. A retry that cannot be proven this transaction's own, or an
/// unreadable stage, frees nothing.
#[must_use]
pub fn portable_import_held_bytes(
    target: &Path,
    transaction_uuid: Uuid,
    limits: PortableLimits,
) -> u64 {
    held_bytes(target, transaction_uuid, limits).unwrap_or(0)
}

fn reclaimable_bytes(directory: &Path, remaining: &mut u64) -> u64 {
    let Ok(entries) = fs::read_dir(directory) else {
        return 0;
    };
    let mut total = 0_u64;
    for entry in entries.flatten() {
        if *remaining == 0 {
            break;
        }
        *remaining -= 1;
        let path = entry.path();
        let Ok(metadata) = fs::symlink_metadata(&path) else {
            continue;
        };
        if metadata.is_dir() {
            total = total.saturating_add(reclaimable_bytes(&path, remaining));
        } else if metadata.is_file()
            && let Ok(file) = crate::project_portable_export::open_source_no_follow(&path)
            && graphforge_filesystem::file_link_count(&file).is_ok_and(|links| links == 1)
            && let Ok(usage) = graphforge_filesystem::file_space_usage(&file)
        {
            total = total.saturating_add(usage.allocated_bytes);
        }
    }
    total
}
