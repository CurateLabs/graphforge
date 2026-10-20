//! Native retry admission and commitment evidence across interrupted publication.
use super::{GfError, Path, PortableError, PortableErrorCode, ProjectPublicationReceipt, Uuid};
use super::{fs, read_bounded_payload, semantically_pristine_generation};
use std::sync::atomic::AtomicBool;

/// Read-only admission of this import's unfinished native target.
/// Recovery and request-fingerprint validation remain owned by the importer.
pub fn portable_import_pending(target: &Path, transaction: Uuid) -> Result<bool, GfError> {
    let name = target
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| GfError::Validation("invalid import target".into()))?;
    let owner = target.with_file_name(format!(".{name}.portable-v2-{transaction}.owner"));
    let expected = transaction.hyphenated().to_string();
    let Ok(bytes) = read_bounded_payload(&owner, 36, "import ownership") else {
        return Ok(false);
    };
    if bytes != expected.as_bytes() {
        return Ok(false);
    }
    let bootstrap = graphforge_core::uuid::portable_bootstrap_generation(&transaction);
    match crate::resolve_project_generation(target) {
        Ok(generation) => Ok(generation.generation_uuid() == bootstrap
            && semantically_pristine_generation(target)? == Some(bootstrap)),
        Err(GfError::Project {
            code: graphforge_core::ProjectErrorCode::ProjectUninitialized,
            ..
        }) => {
            // FORMAT was authenticated by the resolver. Only this import's
            // deterministic bootstrap is allowed to resume without CURRENT.
            if !matches!(fs::symlink_metadata(target.join("CURRENT")),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound)
            {
                return Ok(false);
            }
            let generations = target.join("generations");
            let mut entries =
                fs::read_dir(&generations).map_err(|e| GfError::Storage(e.to_string()))?;
            let Some(entry) = entries
                .next()
                .transpose()
                .map_err(|e| GfError::Storage(e.to_string()))?
            else {
                return Ok(false);
            };
            Ok(
                entry.file_name() == bootstrap.hyphenated().to_string().as_str()
                    && entry
                        .file_type()
                        .map_err(|e| GfError::Storage(e.to_string()))?
                        .is_dir()
                    && entries.next().is_none(),
            )
        }
        Err(_) => Ok(false),
    }
}

pub(super) fn aborted_retry(
    target: &Path,
    transaction: Uuid,
    generation: Uuid,
) -> Result<bool, PortableError> {
    let path = target
        .join("transactions")
        .join(format!("{transaction}.json"));
    if !path.try_exists().map_err(|_| {
        PortableError::new(PortableErrorCode::Io, "cannot inspect import transaction")
    })? {
        return Ok(false);
    }
    let journal =
        crate::project_publication::read_journal(&path).map_err(|error| storage(&error))?;
    // This only admits a still-pristine parent for preparation. The publication
    // kernel subsequently enforces the exact saved request fingerprint.
    Ok(
        journal.phase == crate::project_publication::JournalPhase::Aborted
            && journal.transaction_uuid == transaction.to_string()
            && journal.generation_uuid == generation.to_string(),
    )
}

pub(super) fn committed(
    receipt: &ProjectPublicationReceipt,
    package_digest: &str,
) -> graphforge_core::portable::PortableCommittedImport {
    graphforge_core::portable::PortableCommittedImport {
        operation_uuid: receipt.transaction_uuid,
        generation_uuid: receipt.generation_uuid,
        generation_manifest_sha256: receipt.generation_manifest_sha256,
        package_digest: package_digest.into(),
    }
}

pub(super) fn publication_error(
    target: &Path,
    transaction: Uuid,
    generation: Uuid,
    package_digest: &str,
    error: &GfError,
) -> PortableError {
    let mut failure = storage(error);
    // Recovery takes the native writer lock and classifies the journal against
    // CURRENT. It never promotes an abandoned pre-CURRENT candidate.
    if crate::recover_project_transactions(target).is_ok()
        && let Ok(Some(receipt)) = crate::published_project_transaction(target, transaction)
        && receipt.generation_uuid == generation
    {
        failure.committed_import = Some(Box::new(committed(&receipt, package_digest)));
    }
    failure
}

pub(super) fn preserve_commit(
    mut cleanup_error: PortableError,
    original: &PortableError,
) -> PortableError {
    cleanup_error
        .committed_import
        .clone_from(&original.committed_import);
    cleanup_error
}

pub(super) fn storage(error: &GfError) -> PortableError {
    if error.code() == "GF_IDEMPOTENCY_CONFLICT" {
        return PortableError::new(
            PortableErrorCode::ConcurrentMutation,
            "import operation identity conflicts with its original request",
        );
    }
    PortableError::new(PortableErrorCode::Io, "portable import publication failed")
        .with_cause(crate::portable_cause::sanitized_cause(&error.to_string()))
}

pub(super) fn with_io_context(
    mut error: PortableError,
    action: &str,
    path: &Path,
) -> PortableError {
    if error.code == PortableErrorCode::Io {
        let cause = error.cause.clone().unwrap_or_else(|| error.to_string());
        error.cause = Some(crate::portable_cause::sanitized_cause(&format!(
            "{action} at {}: {cause}",
            path.display()
        )));
    }
    error
}

pub(super) fn storage_or_cancel(error: &GfError, cancelled: Option<&AtomicBool>) -> PortableError {
    if cancelled.is_some_and(|flag| flag.load(std::sync::atomic::Ordering::Relaxed)) {
        PortableError::new(PortableErrorCode::Cancelled, "verification cancelled")
    } else {
        storage(error)
    }
}
