//! Allocation observation at CSR temporary installation boundaries.

use super::storage_err;
use graphforge_core::GfError;
use std::path::Path;
use tempfile::NamedTempFile;

fn csr_io_error(action: &str, path: &Path, error: impl std::fmt::Display) -> GfError {
    storage_err(format!("{action} at {}: {error}", path.display()))
}

pub(super) fn observe_csr_barriers<T>(
    operation: impl FnOnce() -> Result<T, GfError>,
) -> Result<T, GfError> {
    let (result, barriers) = crate::durable_commit::observe_barriers(operation);
    crate::lifecycle_io::record_fsync(crate::StorageIoPhase::ReadPathScan, barriers);
    result
}

pub(super) fn promote_shards(source: &Path, destination: &Path) -> Result<(), GfError> {
    observe_csr_barriers(|| {
        crate::durable_commit::promote_no_replace(source, destination, || Ok(()), || Ok(()))
            .map_err(|error| {
                storage_err(format!(
                    "promote adjacency shards from {} to {}: {error}",
                    source.display(),
                    destination.display()
                ))
            })
    })
}

pub(super) fn write_csr_shard_bytes_observed(
    path: &Path,
    bytes: &[u8],
    allocation: Option<&crate::StorageAllocationOperation>,
) -> Result<(), GfError> {
    use std::io::Write as _;

    let parent = path.parent().ok_or_else(|| {
        GfError::Storage(format!(
            "CSR path {} has no parent directory",
            path.display()
        ))
    })?;
    std::fs::create_dir_all(parent)
        .map_err(|error| csr_io_error("create CSR parent", parent, error))?;
    let file_name = path
        .file_name()
        .map_or_else(|| "csr".to_owned(), |n| n.to_string_lossy().into_owned());
    let tmp = tempfile::Builder::new()
        .prefix(&format!("{file_name}."))
        .suffix(".tmp")
        .tempfile_in(parent)
        .map_err(|error| csr_io_error("create CSR temporary", path, error))?;
    tmp.as_file()
        .write_all(bytes)
        .map_err(|error| csr_io_error("write CSR temporary", tmp.path(), error))?;
    persist_temp_observed(tmp, path, allocation)?;
    Ok(())
}

/// Atomically rename `tmp` into place at `path`.
pub(super) fn persist_temp_observed(
    tmp: NamedTempFile,
    path: &Path,
    allocation: Option<&crate::StorageAllocationOperation>,
) -> Result<(), GfError> {
    observe_csr_barriers(|| persist_sealed_temp(tmp, path, allocation))
}

fn persist_sealed_temp(
    tmp: NamedTempFile,
    path: &Path,
    allocation: Option<&crate::StorageAllocationOperation>,
) -> Result<(), GfError> {
    let temporary = tmp.path().to_path_buf();
    if let Some(allocation) = allocation {
        allocation.replace_file_at(&temporary, tmp.as_file())?;
    }
    let parent = graphforge_filesystem::StableDirectory::open(
        path.parent()
            .ok_or_else(|| storage_err("CSR path has no parent"))?,
    )
    .map_err(|error| csr_io_error("open CSR parent", path, error))?;
    let identity = graphforge_filesystem::file_identity(tmp.as_file())
        .map_err(|error| csr_io_error("identify CSR temporary", &temporary, error))?;
    let name = temporary
        .file_name()
        .ok_or_else(|| storage_err("CSR temporary has no name"))?
        .to_owned();
    let (file, retained_path) = tmp
        .keep()
        .map_err(|error| csr_io_error("retain CSR temporary", &temporary, error.error))?;
    debug_assert_eq!(retained_path, temporary);
    crate::durable_commit::SealedArtifact::seal_existing(
        &parent, &name, file, identity, allocation,
    )
    .map_err(|error| csr_io_error("seal CSR temporary", &temporary, error))?
    .make_visible(
        path.file_name()
            .ok_or_else(|| storage_err("CSR target has no name"))?,
        crate::durable_commit::PublishMode::Replace,
        || Ok(()),
    )
    .map_err(|error| csr_io_error("publish CSR temporary", path, error))?
    .acknowledge(allocation)
    .map_err(|error| csr_io_error("acknowledge CSR publication", path, error))?;
    Ok(())
}
