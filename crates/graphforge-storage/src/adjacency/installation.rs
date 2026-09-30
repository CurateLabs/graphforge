//! Allocation observation at CSR temporary installation boundaries.

use super::storage_err;
use graphforge_core::GfError;
use std::path::Path;
use tempfile::NamedTempFile;

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
    std::fs::create_dir_all(parent).map_err(storage_err)?;
    let file_name = path
        .file_name()
        .map_or_else(|| "csr".to_owned(), |n| n.to_string_lossy().into_owned());
    let tmp = tempfile::Builder::new()
        .prefix(&format!("{file_name}."))
        .suffix(".tmp")
        .tempfile_in(parent)
        .map_err(storage_err)?;
    tmp.as_file().write_all(bytes).map_err(storage_err)?;
    persist_temp_observed(tmp, path, allocation)?;
    Ok(())
}

/// Atomically rename `tmp` into place at `path`.
pub(super) fn persist_temp_observed(
    tmp: NamedTempFile,
    path: &Path,
    allocation: Option<&crate::StorageAllocationOperation>,
) -> Result<(), GfError> {
    let temporary = tmp.path().to_path_buf();
    if let Some(allocation) = allocation {
        allocation.replace_file_at(&temporary, tmp.as_file())?;
    }
    let file = tmp
        .persist(path)
        .map_err(|error| storage_err(error.error))?;
    if let Some(allocation) = allocation {
        allocation.remove_file_at(path)?;
        allocation.remove_file_at(&temporary)?;
        allocation.replace_file_at(path, &file)?;
    }
    Ok(())
}
