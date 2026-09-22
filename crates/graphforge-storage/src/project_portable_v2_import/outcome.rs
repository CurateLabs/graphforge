//! Commitment evidence survives post-publication acknowledgement and cleanup errors.
use super::{GfError, Path, PortableV2Error, PortableV2ErrorCode, ProjectPublicationReceipt, Uuid};
use std::sync::atomic::AtomicBool;

pub(super) fn aborted_retry(
    target: &Path,
    transaction: Uuid,
    generation: Uuid,
) -> Result<bool, PortableV2Error> {
    let path = target
        .join("transactions")
        .join(format!("{transaction}.json"));
    if !path.try_exists().map_err(|_| {
        PortableV2Error::new(PortableV2ErrorCode::Io, "cannot inspect import transaction")
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
) -> graphforge_core::portable::PortableV2CommittedImport {
    graphforge_core::portable::PortableV2CommittedImport {
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
) -> PortableV2Error {
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
    mut cleanup_error: PortableV2Error,
    original: &PortableV2Error,
) -> PortableV2Error {
    cleanup_error
        .committed_import
        .clone_from(&original.committed_import);
    cleanup_error
}

pub(super) fn storage(error: &GfError) -> PortableV2Error {
    if error.code() == "GF_IDEMPOTENCY_CONFLICT" {
        return PortableV2Error::new(
            PortableV2ErrorCode::ConcurrentMutation,
            "import operation identity conflicts with its original request",
        );
    }
    PortableV2Error::new(
        PortableV2ErrorCode::Io,
        "portable import publication failed",
    )
}

pub(super) fn storage_or_cancel(
    error: &GfError,
    cancelled: Option<&AtomicBool>,
) -> PortableV2Error {
    if cancelled.is_some_and(|flag| flag.load(std::sync::atomic::Ordering::Relaxed)) {
        PortableV2Error::new(PortableV2ErrorCode::Cancelled, "verification cancelled")
    } else {
        storage(error)
    }
}
