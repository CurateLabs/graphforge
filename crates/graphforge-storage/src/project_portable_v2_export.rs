//! Bounded deterministic portable-project v2 complete-package export.

use std::collections::{BTreeMap, BTreeSet};
#[cfg(windows)]
use std::fs::OpenOptions;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use crate::{
    PortableV2Error, PortableV2ErrorCode, PortableV2Limits, PortableV2Mode, PortableV2PackageClass,
    verify_portable_v2,
};
use uuid::Uuid;
mod planning;
mod transport;
use planning::{inspect, package_class};
pub use planning::{plan_complete_portable_v2, plan_selected_portable_v2};
use transport::{bundle, entries, expanded};

type ExportError = PortableV2Error;

const BAGIT: &[u8] = b"BagIt-Version: 1.0\nTag-File-Character-Encoding: UTF-8\n";
const BAG_INFO: &[u8] = b"Bag-Software-Agent: GraphForge portable-v2\nBagging-Date: 1970-01-01\n";
const USTAR_MAX_ENTRY_BYTES: u64 = 0o77_777_777_777;

/// Finite planner and streaming-writer budgets.
pub type PortableV2ExportLimits = PortableV2Limits;

pub use graphforge_core::portable::{PortableV2ExportProgress, PortableV2Output};

#[derive(Debug, Clone)]
struct PlannedFile {
    source: PlannedSource,
    path: String,
    length: u64,
    digest: [u8; 32],
}
#[derive(Debug, Clone)]
enum PlannedSource {
    File {
        path: PathBuf,
        identity: Identity,
    },
    Cas {
        lease: crate::graph_object_store::GraphObjectReadLease,
        digest: String,
        length: u64,
    },
    Control(Vec<u8>),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Identity {
    #[cfg(unix)]
    dev: u64,
    #[cfg(unix)]
    ino: u64,
    len: u64,
    modified: Option<std::time::SystemTime>,
}
#[derive(Clone)]
/// Immutable pinned-generation metadata plan; contains no payload buffers.
pub struct PortableV2ExportPlan {
    generation_uuid: Uuid,
    files: Vec<PlannedFile>,
    manifest: Vec<u8>,
    package_digest: [u8; 32],
    payload_bytes: u64,
    selection_fingerprint: String,
    package_class: PortableV2PackageClass,
    /// Keeps subset materialization alive for the plan lifetime.
    retained_subset: Option<std::sync::Arc<tempfile::TempDir>>,
}
impl std::fmt::Debug for PortableV2ExportPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PortableV2ExportPlan")
            .field("generation_uuid", &self.generation_uuid)
            .field("entry_count", &self.files.len())
            .field("package_digest", &hex(self.package_digest))
            .field("payload_bytes", &self.payload_bytes)
            .field("selection_fingerprint", &self.selection_fingerprint)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
/// Durable publication receipt with separate semantic and transport identities.
pub struct PortableV2ExportReceipt {
    /// Pinned source generation.
    pub generation_uuid: Uuid,
    /// Semantic package identity shared by both representations.
    pub package_digest: [u8; 32],
    /// Representation-specific transport identity.
    pub transport_digest: [u8; 32],
    /// Verified physical package entry count, including tag records.
    pub entry_count: usize,
    /// Source payload bytes, excluding tags and manifest.
    pub payload_bytes: u64,
    /// Published representation.
    pub output: PortableV2Output,
    /// Fingerprint of the immutable content-free selection preview used by the writer.
    pub selection_fingerprint: String,
    /// Exact native allocation of the published package for lifecycle evidence.
    #[doc(hidden)]
    pub allocation_identity_allocated_bytes: BTreeMap<String, u64>,
    /// Logical EOF bytes of the exact published identity union.
    #[doc(hidden)]
    pub allocation_logical_bytes: u64,
    /// Distinct physical files in the exact published identity union.
    #[doc(hidden)]
    pub allocation_physical_objects: u64,
}

impl PartialEq for PortableV2ExportReceipt {
    fn eq(&self, other: &Self) -> bool {
        self.generation_uuid == other.generation_uuid
            && self.package_digest == other.package_digest
            && self.transport_digest == other.transport_digest
            && self.entry_count == other.entry_count
            && self.payload_bytes == other.payload_bytes
            && self.output == other.output
            && self.selection_fingerprint == other.selection_fingerprint
            && self.allocation_logical_bytes == other.allocation_logical_bytes
            && self.allocation_physical_objects == other.allocation_physical_objects
    }
}

impl Eq for PortableV2ExportReceipt {}

#[derive(Default)]
struct ExportAllocationObserver {
    allocated: BTreeMap<String, u64>,
    logical: BTreeMap<String, u64>,
    operation: Option<crate::StorageAllocationOperation>,
    routes: BTreeMap<String, PathBuf>,
}

// Refresh after a failed write before staging cleanup, preserving the original error.
fn observed_write_result(
    result: Result<(), ExportError>,
    file: &File,
    allocation: &mut ExportAllocationObserver,
) -> Result<(), ExportError> {
    if result.is_err() && allocation.operation.is_some() {
        let _ = allocation.observe(file);
    }
    result
}

impl ExportAllocationObserver {
    fn register(&mut self, path: &Path, file: &File) -> Result<(), ExportError> {
        if self.operation.is_some() {
            let identity = graphforge_filesystem::file_identity(file).map_err(storage)?;
            let key = crate::storage_attribution::native_identity_key(
                identity.volume_serial,
                &identity.file_id,
            );
            self.routes.insert(key, path.to_path_buf());
            self.observe(file)?;
        }
        Ok(())
    }

    fn remove(&self, path: &Path) {
        remove(path);
        if let Some(operation) = &self.operation {
            for route in self.routes.values() {
                if route.starts_with(path)
                    && fs::symlink_metadata(route)
                        .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
                {
                    let _ = operation.remove_file_at(route);
                }
            }
        }
    }

    fn published(&mut self, stage: &Path, destination: &Path) -> Result<(), ExportError> {
        if let Some(operation) = &self.operation {
            for (identity, route) in &mut self.routes {
                let relative = route.strip_prefix(stage).map_err(storage)?;
                let final_path = if relative.as_os_str().is_empty() {
                    destination.to_path_buf()
                } else {
                    destination.join(relative)
                };
                operation
                    .transition(
                        &crate::StorageAllocationOperation::file_owner(&final_path)
                            .map_err(storage)?,
                        &crate::StorageAllocationTransition {
                            installed: BTreeMap::from([(
                                identity.clone(),
                                self.allocated[identity],
                            )]),
                            removed: BTreeSet::default(),
                        },
                    )
                    .map_err(storage)?;
                operation.remove_file_at(route).map_err(storage)?;
                *route = final_path;
            }
        }
        Ok(())
    }
    fn observe(&mut self, file: &File) -> Result<(), ExportError> {
        let identity = graphforge_filesystem::file_identity(file).map_err(storage)?;
        let usage = graphforge_filesystem::file_space_usage(file).map_err(storage)?;
        let mut file_id = String::with_capacity(32);
        for byte in identity.file_id {
            use std::fmt::Write as _;
            write!(&mut file_id, "{byte:02x}").expect("writing to String cannot fail");
        }
        let key = format!("{:016x}:{file_id}", identity.volume_serial);
        if let Some(operation) = &self.operation {
            let route = self
                .routes
                .get(&key)
                .ok_or_else(|| err("GF_STORAGE_ERROR", "unregistered export allocation route"))?;
            operation.replace_file_at(route, file).map_err(storage)?;
        }
        self.allocated.insert(key.clone(), usage.allocated_bytes);
        self.logical.insert(key, usage.logical_bytes);
        Ok(())
    }
}

/// Stream and durably publish one representation, cleaning private staging on failure.
pub fn export_complete_portable_v2(
    plan: &PortableV2ExportPlan,
    destination: impl AsRef<Path>,
    output: PortableV2Output,
    limits: PortableV2ExportLimits,
    cancelled: &AtomicBool,
    progress: impl FnMut(PortableV2ExportProgress),
) -> Result<PortableV2ExportReceipt, ExportError> {
    export_complete_portable_v2_with_allocation(
        plan,
        destination,
        output,
        limits,
        cancelled,
        progress,
        None,
    )
}

/// Export with an explicit first-party allocation context.
#[doc(hidden)]
pub fn export_complete_portable_v2_with_allocation(
    plan: &PortableV2ExportPlan,
    destination: impl AsRef<Path>,
    output: PortableV2Output,
    limits: PortableV2ExportLimits,
    cancelled: &AtomicBool,
    mut progress: impl FnMut(PortableV2ExportProgress),
    operation: Option<&crate::StorageAllocationOperation>,
) -> Result<PortableV2ExportReceipt, ExportError> {
    validate_limits(limits)?;
    if output == PortableV2Output::Bundle
        && entries(plan, limits.max_tag_manifest_bytes)?
            .iter()
            .any(|(_, source)| source.len() > USTAR_MAX_ENTRY_BYTES)
    {
        return Err(limit("bundle entry exceeds ustar size field"));
    }
    let destination = destination.as_ref();
    let resolved = if operation.is_some() && destination.is_relative() {
        Some(std::env::current_dir().map_err(storage)?.join(destination))
    } else {
        None
    };
    let dst = resolved.as_deref().unwrap_or(destination);
    reject_destination(dst)?;
    let parent = dst
        .parent()
        .ok_or_else(|| err("GF_INVALID_DESTINATION", "destination has no parent"))?;
    let name = dst
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| err("GF_INVALID_DESTINATION", "invalid destination name"))?;
    let stage = parent.join(format!(".{name}.{}.partial", Uuid::new_v4()));
    let is_cancelled = || cancelled.load(Ordering::Relaxed);
    let mut allocation = ExportAllocationObserver {
        operation: operation.cloned(),
        ..Default::default()
    };
    let result = match output {
        PortableV2Output::Expanded => expanded(
            plan,
            &stage,
            limits,
            &is_cancelled,
            &mut progress,
            &mut allocation,
        ),
        PortableV2Output::Bundle => bundle(
            plan,
            &stage,
            limits,
            &is_cancelled,
            &mut progress,
            &mut allocation,
        ),
    };
    let digest = match result {
        Ok(d) => d,
        Err(e) => {
            allocation.remove(&stage);
            return Err(e.with_allocation_identities(allocation.allocated));
        }
    };
    let allocation_logical_bytes = allocation.logical.values().copied().sum();
    let allocation_physical_objects = allocation.logical.len() as u64;
    let staged_allocation = allocation.allocated.clone();
    if is_cancelled() {
        allocation.remove(&stage);
        return Err(err("GF_CANCELLED", "portable export cancelled")
            .with_allocation_identities(staged_allocation));
    }
    let verified =
        verify_written_export(plan, &stage, digest, limits, cancelled).map_err(|error| {
            allocation.remove(&stage);
            error.with_allocation_identities(staged_allocation.clone())
        })?;
    publish_no_replace(&stage, dst).map_err(|error| {
        allocation.remove(&stage);
        storage(error).with_allocation_identities(staged_allocation.clone())
    })?;
    allocation.published(&stage, dst)?;
    if let Err(error) = sync_dir(parent) {
        allocation.remove(dst);
        return Err(error.with_allocation_identities(staged_allocation));
    }
    Ok(PortableV2ExportReceipt {
        generation_uuid: plan.generation_uuid,
        package_digest: plan.package_digest,
        transport_digest: digest,
        entry_count: usize::try_from(verified.entry_count)
            .map_err(|_| limit("verified entry count exceeds platform capacity"))?,
        payload_bytes: plan.payload_bytes,
        output,
        selection_fingerprint: plan.selection_fingerprint.clone(),
        allocation_identity_allocated_bytes: staged_allocation,
        allocation_logical_bytes,
        allocation_physical_objects,
    })
}

fn verify_written_export(
    plan: &PortableV2ExportPlan,
    stage: &Path,
    digest: [u8; 32],
    limits: PortableV2ExportLimits,
    cancelled: &AtomicBool,
) -> Result<crate::PortableV2Report, ExportError> {
    let verified = verify_portable_v2(stage, PortableV2Mode::Full, limits, Some(cancelled))?;
    let expected_transport = format!("sha256:{}", hex(digest));
    if verified.package_class != plan.package_class
        || verified.package_digest != format!("sha256:{}", hex(plan.package_digest))
    {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::DigestMismatch,
            "writer and verifier semantic receipts disagree",
        ));
    }
    if verified.transport_digest.as_deref() != Some(expected_transport.as_str()) {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::DigestMismatch,
            "writer and verifier transport receipts disagree",
        ));
    }
    Ok(verified)
}

/// Repack a fully verified expanded portable-v2 package into canonical bundle bytes.
///
/// This preserves the semantic manifest byte-for-byte and exists for deterministic
/// checked-in contract artifacts; normal project export should use
/// [`export_complete_portable_v2`].
pub fn repack_verified_expanded_portable_v2(
    source: impl AsRef<Path>,
    destination: impl AsRef<Path>,
    limits: PortableV2ExportLimits,
    cancelled: &AtomicBool,
) -> Result<PortableV2ExportReceipt, ExportError> {
    let snapshot = tempfile::tempdir().map_err(storage)?;
    let snapshot_root = snapshot.path().join("verified");
    copy_expanded_snapshot(source.as_ref(), &snapshot_root, cancelled)?;
    let report = crate::verify_portable_v2(
        &snapshot_root,
        crate::PortableV2Mode::Full,
        limits,
        Some(cancelled),
    )?;
    let source = snapshot_root.as_path();
    let manifest = fs::read(source.join("data/graphforge-project.json")).map_err(storage)?;
    let value: serde_json::Value = serde_json::from_slice(&manifest).map_err(storage)?;
    let generation_uuid = value
        .pointer("/source_generation/generation_uuid")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| err("GF_INVALID_MANIFEST", "source generation is missing"))?
        .parse()
        .map_err(|_| err("GF_INVALID_MANIFEST", "source generation is invalid"))?;
    let package_class_name = value
        .get("package_class")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| err("GF_INVALID_MANIFEST", "package class is missing"))?;
    let mut package_digest = [0_u8; 32];
    let digest = report
        .package_digest
        .strip_prefix("sha256:")
        .ok_or_else(|| err("GF_INVALID_MANIFEST", "package digest is invalid"))?;
    if digest.len() != 64 {
        return Err(err("GF_INVALID_MANIFEST", "package digest is invalid"));
    }
    for (index, byte) in package_digest.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&digest[index * 2..index * 2 + 2], 16)
            .map_err(|_| err("GF_INVALID_MANIFEST", "package digest is invalid"))?;
    }
    let mut files = Vec::new();
    let mut total = 0_u64;
    for component in value
        .get("components")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| err("GF_INVALID_MANIFEST", "components are missing"))?
    {
        for file in component
            .get("files")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| err("GF_INVALID_MANIFEST", "component files are missing"))?
        {
            let path = file
                .get("path")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| err("GF_INVALID_MANIFEST", "component path is missing"))?;
            files.push(inspect(&source.join(path), path, limits, &mut total)?);
        }
    }
    files.sort_by(|left, right| left.path.cmp(&right.path));
    let plan = PortableV2ExportPlan {
        generation_uuid,
        files,
        manifest,
        package_digest,
        payload_bytes: total,
        selection_fingerprint: report.package_digest.clone(),
        package_class: package_class(package_class_name)?,
        retained_subset: None,
    };
    export_complete_portable_v2(
        &plan,
        destination,
        PortableV2Output::Bundle,
        limits,
        cancelled,
        |_| {},
    )
}

fn copy_expanded_snapshot(
    source: &Path,
    destination: &Path,
    cancelled: &AtomicBool,
) -> Result<(), ExportError> {
    fs::create_dir(destination).map_err(storage)?;
    let mut pending = vec![(source.to_path_buf(), destination.to_path_buf())];
    while let Some((input, output)) = pending.pop() {
        for entry in fs::read_dir(&input).map_err(storage)? {
            if cancelled.load(std::sync::atomic::Ordering::Relaxed) {
                return Err(err("GF_CANCELLED", "portable export cancelled"));
            }
            let entry = entry.map_err(storage)?;
            let metadata = fs::symlink_metadata(entry.path()).map_err(storage)?;
            let target = output.join(entry.file_name());
            if metadata.file_type().is_symlink() {
                return Err(err(
                    "GF_UNSUPPORTED_ENTRY_TYPE",
                    "expanded portable source contains a symlink",
                ));
            }
            if metadata.is_dir() {
                fs::create_dir(&target).map_err(storage)?;
                pending.push((entry.path(), target));
            } else if metadata.is_file() {
                fs::copy(entry.path(), target).map_err(storage)?;
            } else {
                return Err(err(
                    "GF_UNSUPPORTED_ENTRY_TYPE",
                    "expanded portable source contains a non-file entry",
                ));
            }
        }
    }
    Ok(())
}

fn identity(m: &fs::Metadata) -> Result<Identity, ExportError> {
    if !m.is_file() || m.file_type().is_symlink() {
        return Err(err("GF_UNSUPPORTED_ENTRY_TYPE", "not a regular file"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if m.nlink() != 1 {
            return Err(err("GF_UNSUPPORTED_ENTRY_TYPE", "hard-linked source"));
        }
        Ok(Identity {
            dev: m.dev(),
            ino: m.ino(),
            len: m.len(),
            modified: m.modified().ok(),
        })
    }
    #[cfg(not(unix))]
    {
        Ok(Identity {
            len: m.len(),
            modified: m.modified().ok(),
        })
    }
}
#[cfg(unix)]
pub(crate) fn open_source_no_follow(path: &Path) -> Result<File, ExportError> {
    use std::path::Component;
    if fs::symlink_metadata(path)
        .map_err(storage)?
        .file_type()
        .is_symlink()
    {
        return Err(err("GF_UNSUPPORTED_ENTRY_TYPE", "source is a link"));
    }
    // Resolve fixed platform aliases (for example macOS /var -> /private/var),
    // then pin every component of that canonical path with openat+NOFOLLOW.
    let canonical = path.canonicalize().map_err(storage)?;
    let components = canonical
        .components()
        .filter_map(|component| match component {
            Component::Normal(value) => Some(Ok(value)),
            Component::RootDir | Component::CurDir => None,
            Component::ParentDir | Component::Prefix(_) => Some(Err(err(
                "GF_UNSUPPORTED_ENTRY_TYPE",
                "source path contains an unsafe component",
            ))),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let Some((last, parents)) = components.split_last() else {
        return Err(err("GF_UNSUPPORTED_ENTRY_TYPE", "source is not a file"));
    };
    let mut directory = rustix::fs::open(
        if canonical.is_absolute() {
            Path::new("/")
        } else {
            Path::new(".")
        },
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::DIRECTORY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .map_err(storage)?;
    for component in parents {
        directory = rustix::fs::openat(
            &directory,
            *component,
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::DIRECTORY
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )
        .map_err(storage)?;
    }
    let descriptor = rustix::fs::openat(
        &directory,
        *last,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::NONBLOCK
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .map_err(storage)?;
    Ok(descriptor.into())
}
#[cfg(windows)]
pub(crate) fn open_source_no_follow(path: &Path) -> Result<File, ExportError> {
    use std::os::windows::fs::OpenOptionsExt;
    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    reject_windows_reparse_components(path)?;
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
        .open(path)
        .map_err(storage)?;
    reject_windows_reparse_components(path)?;
    Ok(file)
}
#[cfg(windows)]
fn reject_windows_reparse_components(path: &Path) -> Result<(), ExportError> {
    use std::os::windows::fs::MetadataExt as _;
    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
    let mut current = PathBuf::new();
    for component in path.components() {
        current.push(component.as_os_str());
        // A Windows prefix is not yet a filesystem root. In particular,
        // canonicalize produces a verbatim disk prefix (\\?\C:) which cannot
        // be queried until its following RootDir component has been appended.
        // Check that root and every subsequent component normally.
        if matches!(component, std::path::Component::Prefix(_)) || current.as_os_str().is_empty() {
            continue;
        }
        let metadata = fs::symlink_metadata(&current).map_err(storage)?;
        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(err(
                "GF_UNSUPPORTED_ENTRY_TYPE",
                "source path contains a reparse point",
            ));
        }
    }
    Ok(())
}
fn validate_limits(l: PortableV2ExportLimits) -> Result<(), ExportError> {
    if l.max_components == 0
        || l.max_entries == 0
        || l.max_manifest_bytes == 0
        || l.max_tag_manifest_bytes == 0
        || l.max_path_bytes == 0
        || l.copy_buffer_bytes == 0
        || l.copy_buffer_bytes > 64 * 1024 * 1024
    {
        return Err(limit("invalid limits"));
    }
    Ok(())
}
fn reject_destination(p: &Path) -> Result<(), ExportError> {
    if p.exists() {
        return Err(err("GF_DESTINATION_EXISTS", "destination exists"));
    }
    let q = p
        .parent()
        .ok_or_else(|| err("GF_INVALID_DESTINATION", "missing parent"))?;
    let m = fs::symlink_metadata(q).map_err(storage)?;
    if !m.is_dir() || m.file_type().is_symlink() {
        return Err(err("GF_INVALID_DESTINATION", "unsafe parent"));
    }
    Ok(())
}
pub(crate) fn publish_no_replace(stage: &Path, destination: &Path) -> std::io::Result<()> {
    graphforge_filesystem::rename_no_replace(stage, destination)
}
fn sync_dir(p: &Path) -> Result<(), ExportError> {
    sync_directory_handle(p).map_err(storage)
}
#[cfg(not(windows))]
fn sync_directory_handle(p: &Path) -> std::io::Result<()> {
    File::open(p)?.sync_all()
}
#[cfg(windows)]
fn sync_directory_handle(p: &Path) -> std::io::Result<()> {
    use std::os::windows::fs::OpenOptionsExt as _;
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    OpenOptions::new()
        .write(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(p)?
        .sync_all()
}
fn remove(p: &Path) {
    if p.is_dir() {
        let _ = fs::remove_dir_all(p);
    } else {
        let _ = fs::remove_file(p);
    }
}
fn hex(d: [u8; 32]) -> String {
    use std::fmt::Write as _;
    d.iter()
        .fold(String::with_capacity(64), |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to String cannot fail");
            output
        })
}
fn limit(m: &str) -> ExportError {
    let _ = m;
    PortableV2Error::new(PortableV2ErrorCode::LimitExceeded, "export limit exceeded")
}
fn err(c: &str, m: &str) -> ExportError {
    let code = match c {
        "GF_CANCELLED" => PortableV2ErrorCode::Cancelled,
        "GF_LIMIT_EXCEEDED" => PortableV2ErrorCode::LimitExceeded,
        "GF_SOURCE_CHANGED" => PortableV2ErrorCode::ConcurrentMutation,
        "GF_INVALID_PORTABLE_PATH" => PortableV2ErrorCode::InvalidPath,
        "GF_UNSUPPORTED_ENTRY_TYPE" => PortableV2ErrorCode::InvalidStructure,
        "GF_DUPLICATE_PORTABLE_PATH" => PortableV2ErrorCode::DuplicateEntry,
        "GF_INTEGRITY_FAILED" => PortableV2ErrorCode::DigestMismatch,
        _ => PortableV2ErrorCode::Io,
    };
    let _ = m;
    PortableV2Error::new(code, "portable-v2 export failed")
}
fn storage(e: impl std::fmt::Display) -> ExportError {
    let _ = e;
    PortableV2Error::new(PortableV2ErrorCode::Io, "portable-v2 I/O failed")
}

#[cfg(test)]
mod tests {
    use super::planning::exact_identity;
    use super::transport::open_planned_source;
    use super::*;
    use crate::project_portable_v2::{
        PortableV2ExactIdentity, PortableV2OntologyComposition, canonical_json,
    };
    use crate::{
        PortableV2SelectionProfile, PortableV2SelectionRequest, ResolvedProjectGeneration,
        preview_portable_v2_selection,
    };
    use sha2::{Digest, Sha256};
    use std::io::{Read, Write};

    #[test]
    fn canonical_source_path_retains_no_follow_admission() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("payload.json");
        fs::write(&source, b"authenticated payload").unwrap();
        let canonical = source.canonicalize().unwrap();
        #[cfg(windows)]
        assert!(matches!(
            canonical.components().next(),
            Some(std::path::Component::Prefix(prefix)) if prefix.kind().is_verbatim()
        ));
        let mut total = 0;
        let planned = inspect(
            &canonical,
            "data/payload.json",
            PortableV2ExportLimits::default(),
            &mut total,
        )
        .unwrap();
        assert_eq!(total, 21);
        assert_eq!(planned.length, 21);
        assert_eq!(
            planned.digest,
            <[u8; 32]>::from(Sha256::digest(b"authenticated payload"))
        );
        let (mut file, _) = open_planned_source(&planned).unwrap();
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"authenticated payload");
        assert!(open_source_no_follow(&root.path().join("missing.json")).is_err());
    }

    use crate::open_or_initialize_project;
    use arrow::array::StringArray;
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    fn compact_graph_generation() -> (tempfile::TempDir, ResolvedProjectGeneration) {
        let project = tempfile::tempdir().unwrap();
        open_or_initialize_project(project.path()).unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let relative = PathBuf::from("topology/nodes/part-00000.parquet");
        fs::create_dir_all(workspace.path().join(relative.parent().unwrap())).unwrap();
        fs::write(workspace.path().join(&relative), b"compact graph payload").unwrap();
        let lease = crate::begin_graph_object_publication(project.path()).unwrap();
        let mut state = crate::GraphManifestState::empty();
        let (root, _) =
            crate::append_graph_files_v2(&lease, workspace.path(), &mut state, &[relative], &[])
                .unwrap();
        let request = crate::ProjectGenerationRequest {
            transaction_uuid: Uuid::new_v4(),
            generation_uuid: Uuid::new_v4(),
            capabilities: vec![crate::ProjectCapability {
                capability_id: crate::GRAPH_CAPABILITY_ID.into(),
                capability_version: 1,
            }],
            participants: vec![crate::graph_files_root_participant(&root).unwrap()],
        };
        let crate::ProjectStageOutcome::Staged(staged) =
            crate::stage_project_generation(project.path(), &request).unwrap()
        else {
            panic!("fresh compact generation replayed");
        };
        staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .unwrap()
            .publish_with_graph_objects(&lease)
            .unwrap();
        drop(lease);
        let generation = crate::resolve_project_generation(project.path()).unwrap();
        (project, generation)
    }

    fn graph_generation() -> (tempfile::TempDir, ResolvedProjectGeneration) {
        graph_generation_with_composition(false)
    }

    fn graph_generation_with_composition(
        include_composition: bool,
    ) -> (tempfile::TempDir, ResolvedProjectGeneration) {
        let project = tempfile::tempdir().unwrap();
        let parent = open_or_initialize_project(project.path()).unwrap();
        let tree = tempfile::tempdir().unwrap();
        fs::write(tree.path().join("a.parquet"), b"graph-a").unwrap();
        fs::create_dir(tree.path().join("properties")).unwrap();
        let properties = RecordBatch::try_from_iter(vec![(
            "name",
            std::sync::Arc::new(StringArray::from(vec!["person"])) as arrow::array::ArrayRef,
        )])
        .unwrap();
        let mut writer = ArrowWriter::try_new(
            fs::File::create(tree.path().join("properties/Person.parquet")).unwrap(),
            properties.schema(),
            None,
        )
        .unwrap();
        writer.write(&properties).unwrap();
        writer.close().unwrap();
        let (_, inventory) = crate::capture_graph_files(tree.path()).unwrap();
        let mut participants = crate::empty_workspace_participants().unwrap();
        participants.insert(0, inventory);
        if include_composition {
            let document = graphforge_ontology::OntologyDoc {
                ontology_id: "https://graphforge.dev/ontology/portable".into(),
                version: "release-2026.08".into(),
                entity_types: vec![],
                relation_types: vec![],
                properties: vec![],
                constraints: vec![],
                migrations: vec![],
            };
            let legacy = crate::WorkspaceOntology {
                contract_version: 1,
                mode: crate::WorkspaceOntologyMode::Strict,
                source_format: Some(crate::WorkspaceOntologySourceFormat::Json),
                canonical_ontology_sha256: Some("a".repeat(64)),
                canonical_ontology: Some(serde_json::to_value(document).unwrap()),
            };
            let composition = crate::WorkspaceOntologyComposition::virtual_legacy(&legacy)
                .unwrap()
                .unwrap();
            participants.push(composition.to_project_participant().unwrap());
            participants.sort_by(|left, right| {
                (&left.capability_id, &left.record_family_id)
                    .cmp(&(&right.capability_id, &right.record_family_id))
            });
        }
        let request = crate::ProjectGenerationRequest {
            transaction_uuid: Uuid::new_v4(),
            generation_uuid: Uuid::new_v4(),
            capabilities: vec![
                crate::ProjectCapability {
                    capability_id: "graph".into(),
                    capability_version: 1,
                },
                crate::ProjectCapability {
                    capability_id: "workspace".into(),
                    capability_version: 1,
                },
            ],
            participants,
        };
        let crate::ProjectStageOutcome::Staged(staged) =
            crate::stage_project_generation_with_graph_tree(
                project.path(),
                &request,
                Some(tree.path()),
            )
            .unwrap()
        else {
            panic!("fresh graph generation replayed");
        };
        staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .unwrap()
            .publish()
            .unwrap();
        drop(parent);
        let generation = crate::resolve_project_generation(project.path()).unwrap();
        (project, generation)
    }

    fn graph_generation_with_bridge() -> (tempfile::TempDir, ResolvedProjectGeneration) {
        use graphforge_ontology::{
            ActivationMode, AuthoredModule, BridgeAssertion, BridgeDocument, BridgePredicate,
            BridgeProvenance, BridgeSetId, CompositionLimits, EntityTypeDef,
            InventoryCompileRequest, MappingMethod, OntologyDoc, OntologyModuleId, QualifiedSymbol,
            SymbolKind, bridge_document_digest, compile_inventory, module_document_digest,
        };

        let module = |ontology_id: &str| {
            let document = OntologyDoc {
                ontology_id: ontology_id.into(),
                version: "opaque-v1".into(),
                entity_types: vec![EntityTypeDef {
                    name: "Person".into(),
                    r#abstract: false,
                    parent: None,
                }],
                relation_types: Vec::new(),
                properties: Vec::new(),
                constraints: Vec::new(),
                migrations: Vec::new(),
            };
            AuthoredModule {
                id: OntologyModuleId {
                    ontology_id: document.ontology_id.clone(),
                    authored_version: document.version.clone(),
                    canonical_digest: module_document_digest(&document).unwrap(),
                },
                dependencies: Vec::new(),
                doc: document,
                allow_projected_identity: false,
            }
        };
        let source = module("https://graphforge.dev/ontology/source");
        let target = module("https://graphforge.dev/ontology/target");
        let qualified = |module: &AuthoredModule| QualifiedSymbol {
            module: module.id.clone(),
            kind: SymbolKind::Entity,
            local_id: "Person".into(),
        };
        let bridge = BridgeDocument {
            bridge_id: "https://graphforge.dev/bridge/person".into(),
            authored_version: "bridge-v1".into(),
            source_modules: vec![source.id.clone()],
            target_modules: vec![target.id.clone()],
            dependencies: Vec::new(),
            shared_surfaces: Vec::new(),
            assertions: vec![BridgeAssertion {
                source: qualified(&source),
                target: qualified(&target),
                predicate: BridgePredicate::Equivalent,
                directional: false,
                provenance: BridgeProvenance {
                    method: MappingMethod::Authored,
                    confidence: None,
                    justification: "portable fixture".into(),
                    evidence_refs: Vec::new(),
                },
                valid_from: None,
                valid_to: None,
            }],
            enforcement: Some(ActivationMode::Strict),
        };
        let bridge_id = BridgeSetId {
            bridge_id: bridge.bridge_id.clone(),
            authored_version: bridge.authored_version.clone(),
            canonical_digest: bridge_document_digest(&bridge).unwrap(),
        };
        let compiled = compile_inventory(InventoryCompileRequest {
            modules: &[source, target],
            bridges: &[bridge_id],
            activation: &[],
            profile_default: ActivationMode::Strict,
            limits: CompositionLimits::default(),
            cancelled: None,
        })
        .unwrap();
        let composition =
            crate::WorkspaceOntologyComposition::from_compiled(&compiled, vec![bridge]);

        let project = tempfile::tempdir().unwrap();
        let parent = open_or_initialize_project(project.path()).unwrap();
        let mut participants = crate::empty_workspace_participants().unwrap();
        participants.push(composition.to_project_participant().unwrap());
        participants.sort_by(|left, right| {
            (&left.capability_id, &left.record_family_id)
                .cmp(&(&right.capability_id, &right.record_family_id))
        });
        let request = crate::ProjectGenerationRequest {
            transaction_uuid: Uuid::new_v4(),
            generation_uuid: Uuid::new_v4(),
            capabilities: vec![
                crate::ProjectCapability {
                    capability_id: "graph".into(),
                    capability_version: 1,
                },
                crate::ProjectCapability {
                    capability_id: "workspace".into(),
                    capability_version: 1,
                },
            ],
            participants,
        };
        let crate::ProjectStageOutcome::Staged(staged) =
            crate::stage_project_generation(project.path(), &request).unwrap()
        else {
            panic!("fresh composition generation replayed");
        };
        staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .unwrap()
            .publish()
            .unwrap();
        drop(parent);
        let generation = crate::resolve_project_generation(project.path()).unwrap();
        (project, generation)
    }

    fn graph_generation_with_transitive_bridge_chain() -> (
        tempfile::TempDir,
        ResolvedProjectGeneration,
        PortableV2ExactIdentity,
        String,
    ) {
        use graphforge_ontology::{
            ActivationMode, AuthoredModule, BridgeAssertion, BridgeDocument, BridgePredicate,
            BridgeProvenance, BridgeSetId, CompositionLimits, EntityTypeDef,
            InventoryCompileRequest, MappingMethod, OntologyDoc, OntologyModuleId, QualifiedSymbol,
            SymbolKind, bridge_document_digest, compile_inventory, module_document_digest,
        };
        let module = |name: &str, dependencies: Vec<OntologyModuleId>| {
            let document = OntologyDoc {
                ontology_id: format!("https://graphforge.dev/ontology/{name}"),
                version: "v1".into(),
                entity_types: vec![EntityTypeDef {
                    name: "Person".into(),
                    r#abstract: false,
                    parent: None,
                }],
                relation_types: vec![],
                properties: vec![],
                constraints: vec![],
                migrations: vec![],
            };
            AuthoredModule {
                id: OntologyModuleId {
                    ontology_id: document.ontology_id.clone(),
                    authored_version: document.version.clone(),
                    canonical_digest: module_document_digest(&document).unwrap(),
                },
                dependencies,
                doc: document,
                allow_projected_identity: false,
            }
        };
        let base = module("base", vec![]);
        let source = module("source-chain", vec![base.id.clone()]);
        let target = module("target-chain", vec![]);
        let unrelated = module("unrelated", vec![]);
        let assertion = |from: &AuthoredModule, to: &AuthoredModule| BridgeAssertion {
            source: QualifiedSymbol {
                module: from.id.clone(),
                kind: SymbolKind::Entity,
                local_id: "Person".into(),
            },
            target: QualifiedSymbol {
                module: to.id.clone(),
                kind: SymbolKind::Entity,
                local_id: "Person".into(),
            },
            predicate: BridgePredicate::Equivalent,
            directional: false,
            provenance: BridgeProvenance {
                method: MappingMethod::Authored,
                confidence: None,
                justification: "transitive portable closure fixture".into(),
                evidence_refs: vec![],
            },
            valid_from: None,
            valid_to: None,
        };
        let bridge_a = BridgeDocument {
            bridge_id: "https://graphforge.dev/bridge/a".into(),
            authored_version: "v1".into(),
            source_modules: vec![base.id.clone()],
            target_modules: vec![target.id.clone()],
            dependencies: vec![],
            shared_surfaces: vec![],
            assertions: vec![assertion(&base, &target)],
            enforcement: Some(ActivationMode::Advisory),
        };
        let bridge_a_id = BridgeSetId {
            bridge_id: bridge_a.bridge_id.clone(),
            authored_version: bridge_a.authored_version.clone(),
            canonical_digest: bridge_document_digest(&bridge_a).unwrap(),
        };
        let bridge_b = BridgeDocument {
            bridge_id: "https://graphforge.dev/bridge/b".into(),
            authored_version: "v1".into(),
            source_modules: vec![source.id.clone()],
            target_modules: vec![target.id.clone()],
            dependencies: vec![bridge_a_id.clone()],
            shared_surfaces: vec![],
            assertions: vec![assertion(&source, &target)],
            enforcement: Some(ActivationMode::Strict),
        };
        let bridge_b_id = BridgeSetId {
            bridge_id: bridge_b.bridge_id.clone(),
            authored_version: bridge_b.authored_version.clone(),
            canonical_digest: bridge_document_digest(&bridge_b).unwrap(),
        };
        let modules = [base, source, target, unrelated];
        let compiled = compile_inventory(InventoryCompileRequest {
            modules: &modules,
            bridges: &[bridge_a_id, bridge_b_id.clone()],
            activation: &[],
            profile_default: ActivationMode::Strict,
            limits: CompositionLimits::default(),
            cancelled: None,
        })
        .unwrap();
        let composition =
            crate::WorkspaceOntologyComposition::from_compiled(&compiled, vec![bridge_a, bridge_b]);
        let root_identity = exact_identity(
            &bridge_b_id.bridge_id,
            &bridge_b_id.authored_version,
            &bridge_b_id.canonical_digest,
        );
        let unrelated_digest = modules[3].id.canonical_digest.clone();
        let project = tempfile::tempdir().unwrap();
        open_or_initialize_project(project.path()).unwrap();
        let mut participants = crate::empty_workspace_participants().unwrap();
        participants.push(composition.to_project_participant().unwrap());
        participants.sort_by(|left, right| {
            (&left.capability_id, &left.record_family_id)
                .cmp(&(&right.capability_id, &right.record_family_id))
        });
        let request = crate::ProjectGenerationRequest {
            transaction_uuid: Uuid::new_v4(),
            generation_uuid: Uuid::new_v4(),
            capabilities: vec![
                crate::ProjectCapability {
                    capability_id: "graph".into(),
                    capability_version: 1,
                },
                crate::ProjectCapability {
                    capability_id: "workspace".into(),
                    capability_version: 1,
                },
            ],
            participants,
        };
        let crate::ProjectStageOutcome::Staged(staged) =
            crate::stage_project_generation(project.path(), &request).unwrap()
        else {
            panic!("fresh transitive composition replayed");
        };
        staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .unwrap()
            .publish()
            .unwrap();
        let generation = crate::resolve_project_generation(project.path()).unwrap();
        (project, generation, root_identity, unrelated_digest)
    }

    fn resign_test_manifest(plan: &mut PortableV2ExportPlan) {
        let mut manifest: serde_json::Value = serde_json::from_slice(&plan.manifest).unwrap();
        manifest.as_object_mut().unwrap().remove("package_digest");
        let semantic = canonical_json(&manifest).unwrap();
        let mut digest = Sha256::new();
        digest.update(b"graphforge-project/2\0");
        digest.update(semantic);
        plan.package_digest = digest.finalize().into();
        manifest.as_object_mut().unwrap().insert(
            "package_digest".into(),
            serde_json::Value::String(format!("sha256:{}", hex(plan.package_digest))),
        );
        plan.manifest = canonical_json(&manifest).unwrap();
    }

    fn replace_test_control(plan: &mut PortableV2ExportPlan, path: &str, bytes: Vec<u8>) {
        let file = plan
            .files
            .iter_mut()
            .find(|file| file.path == path)
            .unwrap();
        let old_length = file.length;
        file.length = bytes.len() as u64;
        file.digest = Sha256::digest(&bytes).into();
        file.source = PlannedSource::Control(bytes);
        plan.payload_bytes = plan
            .payload_bytes
            .checked_sub(old_length)
            .unwrap()
            .checked_add(file.length)
            .unwrap();

        let mut manifest: serde_json::Value = serde_json::from_slice(&plan.manifest).unwrap();
        let descriptor = manifest["components"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .flat_map(|component| component["files"].as_array_mut().unwrap())
            .find(|descriptor| descriptor["path"] == path)
            .unwrap();
        descriptor["length"] = file.length.into();
        descriptor["sha256"] = hex(file.digest).into();
        plan.manifest = canonical_json(&manifest).unwrap();
        resign_test_manifest(plan);
    }

    fn write_test_representations(plan: &PortableV2ExportPlan, root: &Path) -> (PathBuf, PathBuf) {
        let expanded_path = root.join("hostile.gfproject");
        let bundle_path = root.join("hostile.gfpb");
        let limits = PortableV2ExportLimits::default();
        let mut allocation = ExportAllocationObserver::default();
        expanded(
            plan,
            &expanded_path,
            limits,
            &|| false,
            &mut |_| {},
            &mut allocation,
        )
        .unwrap();
        bundle(
            plan,
            &bundle_path,
            limits,
            &|| false,
            &mut |_| {},
            &mut allocation,
        )
        .unwrap();
        (expanded_path, bundle_path)
    }

    #[test]
    fn export_failed_partial_write_is_observed_before_cleanup() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("partial");
        let mut file = File::create(&path).unwrap();
        let operation = crate::StorageAllocationOperation::default();
        let mut allocation = ExportAllocationObserver {
            operation: Some(operation.clone()),
            ..Default::default()
        };
        allocation.register(&path, &file).unwrap();
        file.write_all(&vec![1_u8; 32768]).unwrap();
        file.sync_all().unwrap();
        let actual = graphforge_filesystem::file_space_usage(&file)
            .unwrap()
            .allocated_bytes;
        assert!(actual > 0);
        let original = err("GF_CANCELLED", "injected error after partial write");
        let returned = observed_write_result(Err(original), &file, &mut allocation).unwrap_err();
        assert_eq!(returned.code, PortableV2ErrorCode::Cancelled);
        assert_eq!(operation.totals().unwrap(), (actual, actual));
        drop(file);
        allocation.remove(&path);
        assert_eq!(operation.totals().unwrap(), (0, actual));
        assert!(!path.exists());
    }

    #[test]
    fn export_operation_tracks_published_routes_and_cancelled_staging() {
        let (_project, generation) = graph_generation_with_composition(true);
        let limits = PortableV2ExportLimits::default();
        let plan = plan_complete_portable_v2(&generation, limits).unwrap();
        for output in [PortableV2Output::Expanded, PortableV2Output::Bundle] {
            let root = tempfile::tempdir().unwrap();
            let retained = root.path().join("retained");
            fs::write(&retained, vec![1_u8; 8192]).unwrap();
            let operation =
                crate::StorageAllocationOperation::from_paths(&[root.path().to_path_buf()])
                    .unwrap();
            let baseline = operation.totals().unwrap().0;
            let destination = root.path().join("package");
            let cancelled = AtomicBool::new(false);
            let receipt = export_complete_portable_v2_with_allocation(
                &plan,
                &destination,
                output,
                limits,
                &cancelled,
                |_| {},
                Some(&operation),
            )
            .unwrap();
            let expected = baseline
                + receipt
                    .allocation_identity_allocated_bytes
                    .values()
                    .sum::<u64>();
            assert_eq!(operation.totals().unwrap(), (expected, expected));
            let actual =
                crate::StorageAllocationOperation::from_paths(&[root.path().to_path_buf()])
                    .unwrap();
            assert_eq!(operation.snapshot().unwrap(), actual.snapshot().unwrap());

            let cancelled_destination = root.path().join("cancelled");
            let error = export_complete_portable_v2_with_allocation(
                &plan,
                &cancelled_destination,
                output,
                limits,
                &cancelled,
                |_| cancelled.store(true, Ordering::Relaxed),
                Some(&operation),
            )
            .unwrap_err();
            assert_eq!(error.code, PortableV2ErrorCode::Cancelled);
            assert!(!cancelled_destination.exists());
            assert_eq!(operation.totals().unwrap().0, expected);
            assert!(operation.totals().unwrap().1 > expected);
            assert_eq!(fs::read_dir(root.path()).unwrap().count(), 2);
        }
    }

    #[test]
    fn composition_projection_has_one_identity_in_both_forms_and_is_not_runtime_authority() {
        let (_project, generation) = graph_generation_with_composition(true);
        let limits = PortableV2ExportLimits::default();
        let plan = plan_complete_portable_v2(&generation, limits).unwrap();
        let output = tempfile::tempdir().unwrap();
        let expanded = output.path().join("composition.gfproject");
        let bundle = output.path().join("composition.gfpb");
        let cancelled = AtomicBool::new(false);
        let expanded_receipt = export_complete_portable_v2(
            &plan,
            &expanded,
            PortableV2Output::Expanded,
            limits,
            &cancelled,
            |_| {},
        )
        .unwrap();
        let bundle_receipt = export_complete_portable_v2(
            &plan,
            &bundle,
            PortableV2Output::Bundle,
            limits,
            &cancelled,
            |_| {},
        )
        .unwrap();
        assert_eq!(
            expanded_receipt.package_digest,
            bundle_receipt.package_digest
        );
        assert!(
            expanded_receipt.allocation_identity_allocated_bytes.len() > 1,
            "expanded writer must report each exact published identity"
        );
        assert_eq!(
            bundle_receipt.allocation_identity_allocated_bytes.len(),
            1,
            "bundle writer must report its one exact published identity"
        );
        let expanded_report =
            verify_portable_v2(&expanded, PortableV2Mode::Full, limits, Some(&cancelled)).unwrap();
        let bundle_report =
            verify_portable_v2(&bundle, PortableV2Mode::Full, limits, Some(&cancelled)).unwrap();
        assert_eq!(
            expanded_report.ontology_composition,
            bundle_report.ontology_composition
        );
        assert!(expanded_report.ontology_composition.is_some());

        let runtime: serde_json::Value = serde_json::from_slice(
            &fs::read(expanded.join(
                "data/components/compatibility/graphforge-runtime-map/runtime-generation.json",
            ))
            .unwrap(),
        )
        .unwrap();
        assert!(
            runtime["participants"]
                .as_array()
                .unwrap()
                .iter()
                .all(|participant| {
                    participant["record_family_id"] != crate::WORKSPACE_ONTOLOGY_COMPOSITION_FAMILY
                })
        );

        let supported = generation
            .capabilities()
            .into_iter()
            .map(|capability| crate::ProjectCapability {
                capability_id: capability.capability_id,
                capability_version: capability.capability_version,
            })
            .collect::<Vec<_>>();
        let imported = output.path().join("imported-project");
        let import_receipt = crate::import_complete_portable_v2(
            &expanded,
            &imported,
            Uuid::new_v4(),
            Uuid::new_v4(),
            &supported,
            limits,
            Some(&cancelled),
        )
        .unwrap();
        assert!(import_receipt.staged_composition.is_some());
        let reopened = crate::resolve_project_generation(&imported).unwrap();
        let staged = crate::load_portable_ontology_staging(&reopened, limits)
            .unwrap()
            .expect("verified composition remains durably staged");
        assert_eq!(
            staged.package_digest,
            format!("sha256:{}", hex(expanded_receipt.package_digest))
        );
        assert!(
            reopened
                .participant_snapshots()
                .unwrap()
                .iter()
                .all(|participant| participant.record_family_id
                    != crate::WORKSPACE_ONTOLOGY_COMPOSITION_FAMILY)
        );

        let cancelled_before_import = AtomicBool::new(true);
        let cancelled_target = output.path().join("cancelled-project");
        let error = crate::import_complete_portable_v2(
            &expanded,
            &cancelled_target,
            Uuid::new_v4(),
            Uuid::new_v4(),
            &supported,
            limits,
            Some(&cancelled_before_import),
        )
        .unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::Cancelled);
        assert!(!cancelled_target.exists());
    }

    #[test]
    fn tck_evidence_changes_package_identity_without_changing_composition_identity() {
        let (_project, generation) = graph_generation_with_composition(true);
        let limits = PortableV2ExportLimits::default();
        let base = plan_complete_portable_v2(&generation, limits).unwrap();
        let base_manifest: serde_json::Value = serde_json::from_slice(&base.manifest).unwrap();
        let composition = base
            .files
            .iter()
            .find(|file| file.path == crate::project_portable_v2::ONTOLOGY_COMPOSITION_PATH);
        let PlannedSource::Control(composition_bytes) = &composition.unwrap().source else {
            panic!("composition must be inline")
        };
        let composition: PortableV2OntologyComposition =
            serde_json::from_slice(composition_bytes).unwrap();

        let evidence_bytes = canonical_json(&serde_json::json!({
            "contract": "graphforge-tck-evidence/1",
            "passed": 3897,
            "total": 3897
        }))
        .unwrap();
        let evidence_digest: [u8; 32] = Sha256::digest(&evidence_bytes).into();
        let evidence_id = "tck-certification-evidence";
        let evidence_path = "data/components/evidence/tck-certification-evidence/report.json";
        let mut with_evidence = base.clone();
        with_evidence.payload_bytes += evidence_bytes.len() as u64;
        with_evidence.files.push(PlannedFile {
            source: PlannedSource::Control(evidence_bytes.clone()),
            path: evidence_path.into(),
            length: evidence_bytes.len() as u64,
            digest: evidence_digest,
        });
        with_evidence
            .files
            .sort_by(|left, right| left.path.as_bytes().cmp(right.path.as_bytes()));

        let mut manifest = base_manifest.clone();
        manifest["selection"]["roots"]
            .as_array_mut()
            .unwrap()
            .push(evidence_id.into());
        manifest["selection"]["roots"]
            .as_array_mut()
            .unwrap()
            .sort_by(|left, right| left.as_str().cmp(&right.as_str()));
        manifest["components"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({
                "kind": "evidence",
                "participant_id": evidence_id,
                "required_dependencies": [],
                "files": [{
                    "media_type": "application/json",
                    "path": evidence_path,
                    "length": evidence_bytes.len(),
                    "sha256": hex(evidence_digest)
                }]
            }));
        manifest["components"]
            .as_array_mut()
            .unwrap()
            .sort_by(|left, right| {
                (left["kind"].as_str(), left["participant_id"].as_str())
                    .cmp(&(right["kind"].as_str(), right["participant_id"].as_str()))
            });
        with_evidence.manifest = canonical_json(&manifest).unwrap();
        resign_test_manifest(&mut with_evidence);
        assert_ne!(base.package_digest, with_evidence.package_digest);

        let output = tempfile::tempdir().unwrap();
        let (expanded, bundle) = write_test_representations(&with_evidence, output.path());
        let expanded_report =
            verify_portable_v2(&expanded, PortableV2Mode::Full, limits, None).unwrap();
        let bundle_report =
            verify_portable_v2(&bundle, PortableV2Mode::Full, limits, None).unwrap();
        assert_eq!(expanded_report.package_digest, bundle_report.package_digest);
        assert_eq!(
            expanded_report.ontology_composition,
            Some(composition.clone())
        );
        assert_eq!(
            bundle_report.ontology_composition,
            Some(composition.clone())
        );
        assert!(
            !serde_json::to_string(&composition)
                .unwrap()
                .contains("tck-certification-evidence")
        );
    }

    #[test]
    fn semantic_tamper_and_future_feature_precede_payload() {
        let (_project, generation) = graph_generation_with_composition(true);
        let limits = PortableV2ExportLimits::default();
        let original = plan_complete_portable_v2(&generation, limits).unwrap();
        let module_path = original
            .files
            .iter()
            .find(|file| file.path.ends_with("/module.json"))
            .unwrap()
            .path
            .clone();
        let module_bytes = match &original
            .files
            .iter()
            .find(|file| file.path == module_path)
            .unwrap()
            .source
        {
            PlannedSource::Control(bytes) => bytes.clone(),
            PlannedSource::File { .. } | PlannedSource::Cas { .. } => {
                panic!("projected module must be an inline control")
            }
        };
        let mut module: serde_json::Value = serde_json::from_slice(&module_bytes).unwrap();
        module["ontology_id"] = "https://graphforge.dev/ontology/tampered".into();
        let tampered_module = canonical_json(&module).unwrap();

        let mut semantic_tamper = original.clone();
        replace_test_control(&mut semantic_tamper, &module_path, tampered_module.clone());
        let outputs = tempfile::tempdir().unwrap();
        let (expanded, bundle) = write_test_representations(&semantic_tamper, outputs.path());
        for package in [&expanded, &bundle] {
            let error =
                verify_portable_v2(package, PortableV2Mode::Full, limits, None).unwrap_err();
            assert!(matches!(
                error.code,
                PortableV2ErrorCode::InvalidStructure | PortableV2ErrorCode::DigestMismatch
            ));
            assert_eq!(error.entry.as_deref(), Some(module_path.as_str()));
        }

        let mut future = semantic_tamper;
        let control_path = crate::project_portable_v2::ONTOLOGY_COMPOSITION_PATH;
        let control_bytes = match &future
            .files
            .iter()
            .find(|file| file.path == control_path)
            .unwrap()
            .source
        {
            PlannedSource::Control(bytes) => bytes.clone(),
            PlannedSource::File { .. } | PlannedSource::Cas { .. } => {
                panic!("composition must be an inline control")
            }
        };
        let mut control: serde_json::Value = serde_json::from_slice(&control_bytes).unwrap();
        control["required_features"]
            .as_array_mut()
            .unwrap()
            .push("future-contract@2".into());
        control["required_features"]
            .as_array_mut()
            .unwrap()
            .sort_by(|left, right| left.as_str().cmp(&right.as_str()));
        control
            .as_object_mut()
            .unwrap()
            .remove("composition_digest");
        let mut digest = Sha256::new();
        digest.update(b"graphforge-ontology-composition/1\0");
        digest.update(canonical_json(&control).unwrap());
        control["composition_digest"] = format!("sha256:{}", hex(digest.finalize().into())).into();
        replace_test_control(&mut future, control_path, canonical_json(&control).unwrap());
        let outputs = tempfile::tempdir().unwrap();
        let (expanded, bundle) = write_test_representations(&future, outputs.path());
        for package in [&expanded, &bundle] {
            let error =
                verify_portable_v2(package, PortableV2Mode::Full, limits, None).unwrap_err();
            assert_eq!(error.code, PortableV2ErrorCode::UnsupportedFuture);
            assert_eq!(error.entry.as_deref(), Some(control_path));
        }
    }

    #[test]
    fn bridge_semantic_tamper_fails_in_both_representations() {
        let (_project, generation) = graph_generation_with_bridge();
        let limits = PortableV2ExportLimits::default();
        let mut plan = plan_complete_portable_v2(&generation, limits).unwrap();
        let bridge_path = plan
            .files
            .iter()
            .find(|file| file.path.ends_with("/bridge.json"))
            .unwrap()
            .path
            .clone();
        let bytes = match &plan
            .files
            .iter()
            .find(|file| file.path == bridge_path)
            .unwrap()
            .source
        {
            PlannedSource::Control(bytes) => bytes.clone(),
            PlannedSource::File { .. } | PlannedSource::Cas { .. } => {
                panic!("projected bridge must be an inline control")
            }
        };
        let mut bridge: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        bridge["authored_version"] = "tampered-v2".into();
        replace_test_control(&mut plan, &bridge_path, canonical_json(&bridge).unwrap());
        let outputs = tempfile::tempdir().unwrap();
        let (expanded, bundle) = write_test_representations(&plan, outputs.path());
        for package in [&expanded, &bundle] {
            let error =
                verify_portable_v2(package, PortableV2Mode::Full, limits, None).unwrap_err();
            assert_eq!(error.code, PortableV2ErrorCode::DigestMismatch);
            assert_eq!(error.entry.as_deref(), Some(bridge_path.as_str()));
        }
    }

    #[test]
    fn semantic_descriptor_kind_path_and_media_mismatches_fail_in_both_forms() {
        let (_project, generation) = graph_generation_with_composition(true);
        let limits = PortableV2ExportLimits::default();
        let original = plan_complete_portable_v2(&generation, limits).unwrap();
        let module_path = original
            .files
            .iter()
            .find(|file| file.path.ends_with("/module.json"))
            .unwrap()
            .path
            .clone();
        for mutation in ["kind", "path", "media"] {
            let mut plan = original.clone();
            let mut manifest: serde_json::Value = serde_json::from_slice(&plan.manifest).unwrap();
            let component = manifest["components"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|component| {
                    component["files"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|file| file["path"] == module_path)
                })
                .unwrap();
            match mutation {
                "kind" => component["kind"] = "schema".into(),
                "path" => {
                    component["files"][0]["path"] =
                        "data/components/ontology/wrong/module.json".into()
                }
                "media" => component["files"][0]["media_type"] = "application/octet-stream".into(),
                _ => unreachable!(),
            }
            plan.manifest = canonical_json(&manifest).unwrap();
            resign_test_manifest(&mut plan);
            let outputs = tempfile::tempdir().unwrap();
            let (expanded, bundle) = write_test_representations(&plan, outputs.path());
            for package in [&expanded, &bundle] {
                let error =
                    verify_portable_v2(package, PortableV2Mode::Full, limits, None).unwrap_err();
                assert!(
                    matches!(
                        error.code,
                        PortableV2ErrorCode::Incompatible
                            | PortableV2ErrorCode::InvalidStructure
                            | PortableV2ErrorCode::DigestMismatch
                    ),
                    "{mutation}: {error:?}"
                );
            }
        }
    }

    #[test]
    fn versioned_m9_interchange_ledger_covers_required_matrix() {
        let ledger: serde_json::Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/portable-v2/m9-interchange-cases.json"
        ))
        .unwrap();
        assert_eq!(
            ledger["contract"],
            "graphforge-portable-v2-m9-interchange-cases/1"
        );
        assert_eq!(
            ledger["representations"],
            serde_json::json!(["expanded", "bundle"])
        );
        assert_eq!(
            ledger["positive_package_classes"],
            serde_json::json!([
                "complete",
                "ontology-only",
                "component-selective",
                "graph-data-subset"
            ])
        );
        let cases = ledger["negative_cases"]
            .as_array()
            .unwrap()
            .iter()
            .map(|case| case["name"].as_str().unwrap())
            .collect::<BTreeSet<_>>();
        for required in [
            "future-manifest-capability-before-payload",
            "future-composition-feature-before-semantic-payload",
            "module-semantic-digest-tamper",
            "bridge-semantic-digest-tamper",
            "semantic-descriptor-kind-path-media-mismatch",
            "semantic-payload-budget",
            "cancelled-private-materialization",
            "durable-staging-replay",
            "durable-staging-transaction-conflict",
            "durable-staging-publication-failure",
        ] {
            assert!(cases.contains(required), "missing {required}");
        }
    }

    #[test]
    fn complete_ontology_data_and_custom_profiles_keep_composition_closure() {
        let (_project, generation) = graph_generation_with_composition(true);
        let limits = PortableV2ExportLimits::default();
        let requests = [
            PortableV2SelectionRequest {
                profile: PortableV2SelectionProfile::Complete,
                strict: false,
            },
            PortableV2SelectionRequest {
                profile: PortableV2SelectionProfile::OntologyOnly,
                strict: false,
            },
            PortableV2SelectionRequest {
                profile: PortableV2SelectionProfile::DataComponents,
                strict: false,
            },
            PortableV2SelectionRequest {
                profile: PortableV2SelectionProfile::Custom(vec![crate::PortableV2ParticipantId {
                    capability_id: crate::GRAPH_CAPABILITY_ID.into(),
                    record_family_id: crate::GRAPH_FILES_FAMILY.into(),
                }]),
                strict: false,
            },
        ];
        for (index, request) in requests.iter().enumerate() {
            let selection = preview_portable_v2_selection(&generation, request, limits).unwrap();
            assert!(selection.includes(
                crate::WORKSPACE_CAPABILITY_ID,
                crate::WORKSPACE_ONTOLOGY_COMPOSITION_FAMILY
            ));
            assert!(!selection.projected.is_empty());
            let mut reports = Vec::new();
            for representation in [PortableV2Output::Expanded, PortableV2Output::Bundle] {
                let plan = plan_selected_portable_v2(&generation, &selection, limits).unwrap();
                let output = tempfile::tempdir().unwrap();
                let extension = if representation == PortableV2Output::Expanded {
                    "gfproject"
                } else {
                    "gfpb"
                };
                let destination = output.path().join(format!("class-{index}.{extension}"));
                export_complete_portable_v2(
                    &plan,
                    &destination,
                    representation,
                    limits,
                    &AtomicBool::new(false),
                    |_| {},
                )
                .unwrap();
                let report =
                    verify_portable_v2(&destination, PortableV2Mode::Full, limits, None).unwrap();
                assert!(report.ontology_composition.is_some());
                reports.push(report);
            }
            assert_eq!(reports[0].package_digest, reports[1].package_digest);
            assert_eq!(
                reports[0].ontology_composition,
                reports[1].ontology_composition
            );
            assert_eq!(
                reports[0].ontology_composition_entries,
                reports[1].ontology_composition_entries
            );
        }

        let complete = preview_portable_v2_selection(&generation, &requests[0], limits).unwrap();
        let exact = complete.projected[0].identity.clone();
        let projected = preview_portable_v2_selection(
            &generation,
            &PortableV2SelectionRequest {
                profile: PortableV2SelectionProfile::OntologyComposition(vec![exact.clone()]),
                strict: false,
            },
            limits,
        )
        .unwrap();
        assert!(projected.projected.iter().any(|entry| {
            entry.identity == exact && entry.reason == crate::PortableV2SelectionReason::Requested
        }));
        assert!(projected.estimated_payload_bytes > 0);
    }

    #[test]
    fn exact_composition_selection_closes_bridges_without_widening() {
        let (_project, generation) = graph_generation_with_bridge();
        let limits = PortableV2ExportLimits::default();
        let all = preview_portable_v2_selection(
            &generation,
            &PortableV2SelectionRequest {
                profile: PortableV2SelectionProfile::Complete,
                strict: false,
            },
            limits,
        )
        .unwrap();
        let source = all
            .projected
            .iter()
            .find(|entry| entry.identity.id.ends_with("/source"))
            .unwrap()
            .identity
            .clone();
        let bridge = all
            .projected
            .iter()
            .find(|entry| entry.identity.id.contains("/bridge/"))
            .unwrap()
            .identity
            .clone();

        let module_only = preview_portable_v2_selection(
            &generation,
            &PortableV2SelectionRequest {
                profile: PortableV2SelectionProfile::OntologyComposition(vec![source.clone()]),
                strict: true,
            },
            limits,
        )
        .unwrap();
        assert_eq!(module_only.projected.len(), 1);
        assert_eq!(module_only.projected[0].identity, source);
        let module_plan = plan_selected_portable_v2(&generation, &module_only, limits).unwrap();
        let module_manifest: serde_json::Value =
            serde_json::from_slice(&module_plan.manifest).unwrap();
        let control = module_plan
            .files
            .iter()
            .find(|file| file.path == crate::project_portable_v2::ONTOLOGY_COMPOSITION_PATH)
            .unwrap();
        let PlannedSource::Control(bytes) = &control.source else {
            panic!("composition must be inline")
        };
        let control: PortableV2OntologyComposition = serde_json::from_slice(bytes).unwrap();
        assert_eq!(control.modules.len(), 1);
        assert!(control.bridge_sets.is_empty());
        assert_eq!(module_manifest["package_class"], "component-selective");

        let bridge_closed = preview_portable_v2_selection(
            &generation,
            &PortableV2SelectionRequest {
                profile: PortableV2SelectionProfile::OntologyComposition(vec![bridge.clone()]),
                strict: true,
            },
            limits,
        )
        .unwrap();
        assert_eq!(bridge_closed.projected.len(), 3);
        assert_eq!(
            bridge_closed
                .projected
                .iter()
                .filter(|entry| entry.reason == crate::PortableV2SelectionReason::Requested)
                .count(),
            1
        );
        assert!(
            bridge_closed
                .projected
                .iter()
                .any(|entry| entry.identity == bridge)
        );
        assert!(
            bridge_closed
                .projected
                .iter()
                .filter(|entry| entry.kind == "ontology")
                .all(|entry| entry.reason
                    == crate::PortableV2SelectionReason::RequiredOntologyComposition)
        );

        let missing = PortableV2ExactIdentity {
            id: "https://graphforge.dev/ontology/absent".into(),
            version: "v1".into(),
            content_digest: format!("sha256:{}", "f".repeat(64)),
        };
        let error = preview_portable_v2_selection(
            &generation,
            &PortableV2SelectionRequest {
                profile: PortableV2SelectionProfile::OntologyComposition(vec![missing]),
                strict: true,
            },
            limits,
        )
        .unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::Incompatible);
    }

    #[test]
    fn transitive_module_and_bridge_closure_is_exact_in_both_forms() {
        let (_project, generation, root_bridge, unrelated_digest) =
            graph_generation_with_transitive_bridge_chain();
        let limits = PortableV2ExportLimits::default();
        let selection = preview_portable_v2_selection(
            &generation,
            &PortableV2SelectionRequest {
                profile: PortableV2SelectionProfile::OntologyComposition(vec![root_bridge.clone()]),
                strict: true,
            },
            limits,
        )
        .unwrap();
        assert_eq!(selection.projected.len(), 5);
        assert_eq!(
            selection
                .projected
                .iter()
                .filter(|entry| entry.reason == crate::PortableV2SelectionReason::Requested)
                .count(),
            1
        );
        assert!(
            selection
                .projected
                .iter()
                .any(|entry| entry.identity == root_bridge)
        );
        assert!(
            !selection
                .projected
                .iter()
                .any(|entry| entry.identity.content_digest.ends_with(&unrelated_digest))
        );

        let plan = plan_selected_portable_v2(&generation, &selection, limits).unwrap();
        let manifest: serde_json::Value = serde_json::from_slice(&plan.manifest).unwrap();
        let components = manifest["components"].as_array().unwrap();
        assert_eq!(
            components
                .iter()
                .filter(|component| matches!(
                    component["kind"].as_str(),
                    Some("ontology" | "schema")
                ))
                .count(),
            5
        );
        assert!(
            !serde_json::to_string(&manifest)
                .unwrap()
                .contains(&unrelated_digest)
        );
        let control_file = plan
            .files
            .iter()
            .find(|file| file.path == crate::project_portable_v2::ONTOLOGY_COMPOSITION_PATH)
            .unwrap();
        let PlannedSource::Control(control_bytes) = &control_file.source else {
            panic!("composition must be inline")
        };
        let control: PortableV2OntologyComposition = serde_json::from_slice(control_bytes).unwrap();
        assert_eq!(control.modules.len(), 3);
        assert_eq!(control.bridge_sets.len(), 2);
        assert!(
            !control
                .modules
                .iter()
                .any(|module| module.content_digest.ends_with(&unrelated_digest))
        );

        let output = tempfile::tempdir().unwrap();
        let mut reports = Vec::new();
        for representation in [PortableV2Output::Expanded, PortableV2Output::Bundle] {
            let path = output
                .path()
                .join(if representation == PortableV2Output::Expanded {
                    "chain.gfproject"
                } else {
                    "chain.gfpb"
                });
            export_complete_portable_v2(
                &plan,
                &path,
                representation,
                limits,
                &AtomicBool::new(false),
                |_| {},
            )
            .unwrap();
            reports.push(verify_portable_v2(&path, PortableV2Mode::Full, limits, None).unwrap());
        }
        assert_eq!(reports[0].package_digest, reports[1].package_digest);
        assert_eq!(
            reports[0].ontology_composition,
            reports[1].ontology_composition
        );
        assert_eq!(reports[0].ontology_composition_entries.len(), 5);
        assert_eq!(
            reports[0].ontology_composition_entries,
            reports[1].ontology_composition_entries
        );
    }

    #[test]
    fn graph_tree_round_trips_equivalently_from_both_representations() {
        let (_project, generation) = graph_generation();
        let limits = PortableV2ExportLimits::default();
        let plan = plan_complete_portable_v2(&generation, limits).unwrap();
        let output = tempfile::tempdir().unwrap();
        let expanded = output.path().join("graph.gfproject");
        let bundle = output.path().join("graph.gfpb");
        let cancelled = AtomicBool::new(false);
        export_complete_portable_v2(
            &plan,
            &expanded,
            PortableV2Output::Expanded,
            limits,
            &cancelled,
            |_| {},
        )
        .unwrap();
        export_complete_portable_v2(
            &plan,
            &bundle,
            PortableV2Output::Bundle,
            limits,
            &cancelled,
            |_| {},
        )
        .unwrap();
        let supported = generation
            .capabilities()
            .into_iter()
            .map(|capability| crate::ProjectCapability {
                capability_id: capability.capability_id,
                capability_version: capability.capability_version,
            })
            .collect::<Vec<_>>();
        let expanded_target = output.path().join("expanded");
        let bundle_target = output.path().join("bundle");
        for (source, target) in [(&expanded, &expanded_target), (&bundle, &bundle_target)] {
            crate::import_complete_portable_v2(
                source,
                target,
                Uuid::new_v4(),
                Uuid::new_v4(),
                &supported,
                limits,
                None,
            )
            .unwrap();
        }
        let expanded = crate::resolve_project_generation(&expanded_target).unwrap();
        let bundle = crate::resolve_project_generation(&bundle_target).unwrap();
        assert_eq!(
            expanded.graph_files_inventory().unwrap(),
            bundle.graph_files_inventory().unwrap()
        );
        assert_eq!(
            tree_bytes(&expanded.graph_tree_root()),
            tree_bytes(&bundle.graph_tree_root())
        );
    }

    #[test]
    fn expanded_and_bundle_share_semantic_identity_and_are_deterministic() {
        let project = tempfile::tempdir().unwrap();
        let generation = open_or_initialize_project(project.path()).unwrap();
        let limits = PortableV2ExportLimits {
            copy_buffer_bytes: 7,
            ..Default::default()
        };
        let plan = plan_complete_portable_v2(&generation, limits).unwrap();
        let out = tempfile::tempdir().unwrap();
        let expanded = out.path().join("complete.gfproject");
        let first = out.path().join("first.gfpb");
        let second = out.path().join("second.gfpb");
        let cancelled = AtomicBool::new(false);
        let mut expanded_progress = Vec::new();
        let a = export_complete_portable_v2(
            &plan,
            &expanded,
            PortableV2Output::Expanded,
            limits,
            &cancelled,
            |progress| expanded_progress.push(progress),
        )
        .unwrap();
        let final_progress = expanded_progress.last().unwrap();
        assert_eq!(
            final_progress.entries_completed,
            final_progress.entries_total
        );
        assert_eq!(final_progress.bytes_completed, final_progress.bytes_total);
        let b = export_complete_portable_v2(
            &plan,
            &first,
            PortableV2Output::Bundle,
            limits,
            &cancelled,
            |_| {},
        )
        .unwrap();
        let c = export_complete_portable_v2(
            &plan,
            &second,
            PortableV2Output::Bundle,
            limits,
            &cancelled,
            |_| {},
        )
        .unwrap();
        assert_eq!(a.package_digest, b.package_digest);
        assert_eq!(b, c);
        assert_eq!(fs::read(&first).unwrap(), fs::read(second).unwrap());
        assert_eq!(&fs::read(expanded.join("bagit.txt")).unwrap(), BAGIT);
        assert!(!expanded.join("CURRENT").exists());
        assert!(!expanded.join("lease.lock").exists());
        let expanded_stage = out.path().join("expanded-stage");
        let bundle_stage = out.path().join("bundle-stage");
        let expanded_report = crate::materialize_verified_portable_v2(
            &expanded,
            &expanded_stage,
            limits,
            Some(&cancelled),
        )
        .unwrap();
        let bundle_report = crate::materialize_verified_portable_v2(
            &first,
            &bundle_stage,
            limits,
            Some(&cancelled),
        )
        .unwrap();
        assert_eq!(expanded_report.package_digest, bundle_report.package_digest);
        assert_eq!(tree_bytes(&expanded_stage), tree_bytes(&bundle_stage));
        let runtime =
            fs::read(expanded_stage.join(
                "data/components/compatibility/graphforge-runtime-map/runtime-generation.json",
            ))
            .unwrap();
        let runtime: serde_json::Value = serde_json::from_slice(&runtime).unwrap();
        assert_eq!(runtime["contract"], "graphforge-runtime-generation-map/1");
        assert!(runtime.get("host_path").is_none());
        assert!(runtime.get("secret").is_none());

        let supported = generation
            .capabilities()
            .into_iter()
            .map(|capability| crate::ProjectCapability {
                capability_id: capability.capability_id,
                capability_version: capability.capability_version,
            })
            .collect::<Vec<_>>();
        let expanded_target = out.path().join("expanded-project");
        let bundle_target = out.path().join("bundle-project");
        let mut progress = Vec::new();
        let expanded_transaction = Uuid::new_v4();
        let expanded_generation = Uuid::new_v4();
        let expanded_import = crate::import_complete_portable_v2_with_progress(
            &expanded,
            &expanded_target,
            expanded_transaction,
            expanded_generation,
            &supported,
            limits,
            Some(&cancelled),
            |event| progress.push(event),
        )
        .unwrap();
        assert_eq!(
            progress.iter().map(|event| event.phase).collect::<Vec<_>>(),
            vec![
                crate::PortableV2ImportPhase::Verifying,
                crate::PortableV2ImportPhase::Materialized,
                crate::PortableV2ImportPhase::Published,
            ]
        );
        let expected_package_digest = format!("sha256:{}", hex(a.package_digest));
        assert!(progress.iter().skip(1).all(|event| {
            event.entries > 0
                && event.bytes > 0
                && event.package_digest.as_deref() == Some(expected_package_digest.as_str())
        }));
        let bundle_import = crate::import_complete_portable_v2(
            &first,
            &bundle_target,
            Uuid::new_v4(),
            Uuid::new_v4(),
            &supported,
            limits,
            Some(&cancelled),
        )
        .unwrap();
        assert_eq!(expanded_import.package_digest, bundle_import.package_digest);
        let expanded_reopened = crate::resolve_project_generation(&expanded_target).unwrap();
        let bundle_reopened = crate::resolve_project_generation(&bundle_target).unwrap();
        assert_eq!(
            expanded_reopened.capabilities(),
            bundle_reopened.capabilities()
        );
        assert_eq!(
            expanded_reopened.participant_snapshots().unwrap(),
            bundle_reopened.participant_snapshots().unwrap()
        );
        let protected_generation = expanded_reopened.generation_uuid();
        let replay = crate::import_complete_portable_v2(
            &expanded,
            &expanded_target,
            expanded_transaction,
            expanded_generation,
            &supported,
            limits,
            Some(&cancelled),
        )
        .unwrap();
        assert!(replay.publication.idempotent_replay);
        assert_eq!(replay.publication.generation_uuid, protected_generation);
        let overwrite = crate::import_complete_portable_v2(
            &expanded,
            &expanded_target,
            Uuid::new_v4(),
            Uuid::new_v4(),
            &supported,
            limits,
            Some(&cancelled),
        )
        .unwrap_err();
        assert_eq!(overwrite.code, PortableV2ErrorCode::Io);
        assert_eq!(
            crate::resolve_project_generation(&expanded_target)
                .unwrap()
                .generation_uuid(),
            protected_generation
        );
        let cancelled = AtomicBool::new(true);
        let cancelled_target = out.path().join("cancelled-project");
        let cancelled_error = crate::import_complete_portable_v2(
            &expanded,
            &cancelled_target,
            Uuid::new_v4(),
            Uuid::new_v4(),
            &supported,
            limits,
            Some(&cancelled),
        )
        .unwrap_err();
        assert_eq!(cancelled_error.code, PortableV2ErrorCode::Cancelled);
        assert!(!cancelled_target.exists());
        fs::write(
            expanded.join(
                "data/components/compatibility/graphforge-runtime-map/runtime-generation.json",
            ),
            b"{}",
        )
        .unwrap();
        let corrupt_target = out.path().join("corrupt-project");
        let corrupt = crate::import_complete_portable_v2(
            &expanded,
            &corrupt_target,
            Uuid::new_v4(),
            Uuid::new_v4(),
            &supported,
            limits,
            None,
        )
        .unwrap_err();
        assert_eq!(corrupt.code, PortableV2ErrorCode::DigestMismatch);
        assert!(!corrupt_target.exists());
    }

    #[test]
    fn cancellation_and_limits_never_publish_a_destination() {
        let project = tempfile::tempdir().unwrap();
        let generation = open_or_initialize_project(project.path()).unwrap();
        let limits = PortableV2ExportLimits::default();
        let plan = plan_complete_portable_v2(&generation, limits).unwrap();
        let out = tempfile::tempdir().unwrap();
        let destination = out.path().join("cancelled.gfpb");
        let cancelled = AtomicBool::new(true);
        let error = export_complete_portable_v2(
            &plan,
            &destination,
            PortableV2Output::Bundle,
            limits,
            &cancelled,
            |_| {},
        )
        .unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::Cancelled);
        assert!(!destination.exists());
        assert!(fs::read_dir(out.path()).unwrap().next().is_none());
        let limited = PortableV2ExportLimits {
            max_entries: 1,
            ..limits
        };
        let error = plan_complete_portable_v2(&generation, limited).unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::LimitExceeded);
    }

    #[test]
    fn compact_plan_is_reusable_across_representations_cancelled_retry_and_cas_tamper() {
        let (project, generation) = compact_graph_generation();
        let limits = PortableV2ExportLimits::default();
        let plan = plan_complete_portable_v2(&generation, limits).unwrap();
        assert!(
            plan.files
                .iter()
                .any(|file| matches!(file.source, PlannedSource::Cas { .. }))
        );
        let out = tempfile::tempdir().unwrap();
        let expanded = out.path().join("compact.gfproject");
        let bundle = out.path().join("compact.gfpb");
        let cancelled_bundle = out.path().join("cancelled.gfpb");
        let active = AtomicBool::new(false);
        let expanded_receipt = export_complete_portable_v2(
            &plan,
            &expanded,
            PortableV2Output::Expanded,
            limits,
            &active,
            |_| {},
        )
        .unwrap();
        let cancelled = AtomicBool::new(true);
        assert_eq!(
            export_complete_portable_v2(
                &plan,
                &cancelled_bundle,
                PortableV2Output::Bundle,
                limits,
                &cancelled,
                |_| {},
            )
            .unwrap_err()
            .code,
            PortableV2ErrorCode::Cancelled
        );
        assert!(!cancelled_bundle.exists());
        let bundle_receipt = export_complete_portable_v2(
            &plan,
            &bundle,
            PortableV2Output::Bundle,
            limits,
            &active,
            |_| {},
        )
        .unwrap();
        assert_eq!(
            expanded_receipt.package_digest,
            bundle_receipt.package_digest
        );
        verify_portable_v2(&expanded, PortableV2Mode::Full, limits, Some(&active)).unwrap();
        verify_portable_v2(&bundle, PortableV2Mode::Full, limits, Some(&active)).unwrap();

        let payload_digest = hex(Sha256::digest(b"compact graph payload").into());
        let (digest, length) = plan
            .files
            .iter()
            .find_map(|file| match &file.source {
                PlannedSource::Cas { digest, length, .. } if digest == &payload_digest => {
                    Some((digest.clone(), *length))
                }
                PlannedSource::File { .. } | PlannedSource::Control(_) => None,
                PlannedSource::Cas { .. } => None,
            })
            .unwrap();
        let object = crate::graph_object_store::graph_object_path(project.path(), &digest).unwrap();
        let mut permissions = fs::metadata(&object).unwrap().permissions();
        permissions.set_readonly(false);
        fs::set_permissions(&object, permissions).unwrap();
        fs::write(&object, vec![b'x'; usize::try_from(length).unwrap()]).unwrap();
        let tampered = out.path().join("tampered.gfpb");
        let error = export_complete_portable_v2(
            &plan,
            &tampered,
            PortableV2Output::Bundle,
            limits,
            &active,
            |_| {},
        )
        .unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::ConcurrentMutation);
        assert!(!tampered.exists());
    }

    #[test]
    fn destination_replacement_and_source_mutation_fail_closed() {
        let project = tempfile::tempdir().unwrap();
        let generation = open_or_initialize_project(project.path()).unwrap();
        let limits = PortableV2ExportLimits {
            copy_buffer_bytes: 3,
            ..Default::default()
        };
        let plan = plan_complete_portable_v2(&generation, limits).unwrap();
        let out = tempfile::tempdir().unwrap();
        let destination = out.path().join("raced.gfpb");
        let mut replaced = false;
        let cancelled = AtomicBool::new(false);
        let error = export_complete_portable_v2(
            &plan,
            &destination,
            PortableV2Output::Bundle,
            limits,
            &cancelled,
            |_| {
                if !replaced {
                    fs::write(&destination, b"attacker").unwrap();
                    replaced = true;
                }
            },
        )
        .unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::Io);
        assert_eq!(fs::read(&destination).unwrap(), b"attacker");
        assert_eq!(fs::read_dir(out.path()).unwrap().count(), 1);

        let source = plan
            .files
            .iter()
            .find_map(|file| match &file.source {
                PlannedSource::File { path, .. } => Some(path),
                PlannedSource::Control(_) | PlannedSource::Cas { .. } => None,
            })
            .unwrap();
        let original = fs::read(source).unwrap();
        fs::write(source, vec![b'x'; original.len()]).unwrap();
        let mutated = out.path().join("mutated.gfpb");
        let error = export_complete_portable_v2(
            &plan,
            &mutated,
            PortableV2Output::Bundle,
            limits,
            &cancelled,
            |_| {},
        )
        .unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::ConcurrentMutation);
        assert!(
            !error.allocation_identity_allocated_bytes.is_empty(),
            "partial bundle allocation must survive typed failure"
        );
        assert!(!mutated.exists());
    }

    #[cfg(unix)]
    #[test]
    fn source_symlink_is_rejected_without_following_it() {
        use std::os::unix::fs::symlink;
        let root = tempfile::tempdir().unwrap();
        let real = root.path().join("real");
        fs::write(&real, b"secret").unwrap();
        let linked = root.path().join("linked");
        symlink(&real, &linked).unwrap();
        let mut total = 0;
        let error = inspect(
            &linked,
            "data/components/settings/settings/secret.bin",
            PortableV2ExportLimits::default(),
            &mut total,
        )
        .unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::InvalidStructure);
        assert_eq!(total, 0);
    }

    fn tree_bytes(root: &Path) -> Vec<(String, Vec<u8>)> {
        fn walk(root: &Path, directory: &Path, output: &mut Vec<(String, Vec<u8>)>) {
            for entry in fs::read_dir(directory).unwrap() {
                let entry = entry.unwrap();
                if entry.file_type().unwrap().is_dir() {
                    walk(root, &entry.path(), output);
                } else {
                    output.push((
                        entry
                            .path()
                            .strip_prefix(root)
                            .unwrap()
                            .to_string_lossy()
                            .replace(std::path::MAIN_SEPARATOR, "/"),
                        fs::read(entry.path()).unwrap(),
                    ));
                }
            }
        }
        let mut output = Vec::new();
        walk(root, root, &mut output);
        output.sort_by(|left, right| left.0.cmp(&right.0));
        output
    }
}
