//! Strict live native identity inventories for authenticated import cleanup.

use super::{PortableV2Error, PortableV2ErrorCode};
use std::fs;

pub(super) fn record_import_file_identity(
    file: &fs::File,
    identities: &mut std::collections::BTreeMap<String, u64>,
) -> Result<(), PortableV2Error> {
    let identity = graphforge_filesystem::file_identity(file).map_err(|_| {
        PortableV2Error::new(
            PortableV2ErrorCode::Io,
            "cannot identify owned import artifact",
        )
    })?;
    let usage = graphforge_filesystem::file_space_usage(file).map_err(|_| {
        PortableV2Error::new(
            PortableV2ErrorCode::Io,
            "cannot measure owned import artifact",
        )
    })?;
    let mut file_id = String::with_capacity(32);
    for byte in identity.file_id {
        use std::fmt::Write as _;
        write!(&mut file_id, "{byte:02x}").expect("writing to String cannot fail");
    }
    let key = format!("{:016x}:{file_id}", identity.volume_serial);
    if let Some(previous) = identities.insert(key, usage.allocated_bytes)
        && previous != usage.allocated_bytes
    {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::ConcurrentMutation,
            "live import aliases disagree on allocated bytes",
        ));
    }
    Ok(())
}

pub(super) fn capture_import_tree(
    directory: &graphforge_filesystem::StableDirectory,
    identities: &mut std::collections::BTreeMap<String, u64>,
    remaining: &mut usize,
) -> Result<(), PortableV2Error> {
    let names = directory.child_names_bounded(*remaining).map_err(|_| {
        PortableV2Error::new(
            PortableV2ErrorCode::Io,
            "completed import staging exceeds identity bound",
        )
    })?;
    *remaining = remaining.saturating_sub(names.len());
    for name in names {
        if let Ok(child) = directory.open_child_directory(&name) {
            capture_import_tree(&child, identities, remaining)?;
        } else {
            let file = directory.open_child_file(&name).map_err(|_| {
                PortableV2Error::new(
                    PortableV2ErrorCode::Io,
                    "cannot authenticate completed import entry",
                )
            })?;
            record_import_file_identity(&file, identities)?;
        }
    }
    Ok(())
}
/// Observe declared destination authorities after verification and lifecycle admission.
/// Private preparation, CAS temporary objects and other attempts' active leases
/// are outside this import's ownership; immutable retained generations and
/// sealed CAS files remain counted until their explicit retirement.
pub(super) fn observe_destination(
    allocation: &crate::StorageAllocationOperation,
    root: &std::path::Path,
    existing: Option<&crate::ResolvedProjectGeneration>,
) -> Result<(), PortableV2Error> {
    let control_paths = crate::StorageAllocationOperation::project_paths(root)
        .map_err(|error| super::storage(&error))?;
    allocation
        .observe_paths(&[
            control_paths[1].clone(),
            root.join("FORMAT"),
            root.join("CURRENT"),
            root.join(crate::project_publication::LOCKS_DIR)
                .join(crate::project_publication::WRITER_LOCK_FILE),
        ])
        .map_err(|error| super::storage(&error))?;
    if existing.is_some() {
        let generations = graphforge_filesystem::StableDirectory::open(&root.join("generations"))
            .map_err(|error| {
            super::storage(&graphforge_core::GfError::Storage(error.to_string()))
        })?;
        for name in generations.child_names_bounded(4096).map_err(|error| {
            super::storage(&graphforge_core::GfError::Storage(error.to_string()))
        })? {
            let text = name.to_str().ok_or_else(|| {
                PortableV2Error::new(
                    PortableV2ErrorCode::Io,
                    "retained generation name is not UTF-8",
                )
            })?;
            let uuid = uuid::Uuid::parse_str(text).map_err(|_| {
                PortableV2Error::new(
                    PortableV2ErrorCode::Io,
                    "retained generation name is not a UUID",
                )
            })?;
            let generation = crate::resolve_generation_by_uuid(root, uuid)
                .map_err(|error| super::storage(&error))?;
            allocation
                .observe_paths(&[generation.generation_root().to_path_buf()])
                .map_err(|error| super::storage(&error))?;
        }
    }
    crate::graph_object_store::capture_retained_graph_object_identities_observed(
        root,
        Some(allocation),
    )
    .map_err(|error| super::storage(&error))?;
    Ok(())
}

/// Final cleanup and peak qualification retain post-publication commit evidence.
pub(super) fn finish_receipt(
    stage: &std::path::Path,
    owner: &std::path::Path,
    stage_identity: graphforge_filesystem::FileIdentity,
    mut receipt: super::PortableV2ImportReceipt,
    entry_count: usize,
    allocation: &crate::StorageAllocationOperation,
) -> Result<super::PortableV2ImportReceipt, PortableV2Error> {
    let committed = super::outcome::committed(&receipt.publication, &receipt.package_digest);
    super::finalize_import_materialization_cleanup(
        stage,
        owner,
        stage_identity,
        &mut receipt,
        entry_count,
        Some(allocation),
    )
    .map_err(|error| error.with_committed_import(committed.clone()))?;
    receipt.transient_peak_allocated_bytes = allocation
        .totals()
        .map_err(|error| super::storage(&error).with_committed_import(committed))?
        .1;
    Ok(receipt)
}
