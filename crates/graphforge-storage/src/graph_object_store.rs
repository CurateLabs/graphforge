//! Project-level immutable content-addressed graph objects.
//!
//! Manifest updates, authenticated materialization, installation, and collection
//! have private owners. Shared leases and lifecycle locks stay here.

use std::collections::{BTreeMap, BTreeSet};
#[cfg(windows)]
use std::fs::OpenOptions;
use std::fs::{self, File};
use std::io::{Read, Seek, Write};
use std::path::{Component, Path, PathBuf};

use graphforge_core::{GfError, ProjectErrorCode};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::graph_manifest::verify_object_bytes;
use crate::{
    GRAPH_FILES_V2_FORMAT, GRAPH_FILES_V2_VERSION, GRAPH_MANIFEST_NODE_FORMAT,
    GRAPH_MANIFEST_NODE_VERSION, GRAPH_RADIX_DEPTH, GraphFilesInventory, GraphFilesOpenEvidence,
    GraphFilesOpenStrategy, GraphFilesRootV2, GraphManifestNode, GraphManifestNodeKind,
};
use graphforge_filesystem::StableDirectory;
use parquet::file::reader::{ChunkReader, Length};

/// Project-relative root of immutable graph objects.
pub const GRAPH_OBJECTS_DIR: &str = "graph-objects";
const SHA256_DIR: &str = "sha256";
const TEMP_DIR: &str = "tmp";
const ACTIVE_DIR: &str = "active";
const LIFECYCLE_LOCK: &str = "lifecycle.lock";
/// Maximum bytes consumed by one instrumented CAS copy/hash operation.
pub const GRAPH_OBJECT_IO_BUFFER_BYTES: usize = 64 * 1024;
const BUFFER_BYTES: usize = GRAPH_OBJECT_IO_BUFFER_BYTES;

#[cfg(test)]
thread_local! {
    static RETURNED_ERROR_BOUNDARY: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) };
    static BEFORE_OBJECT_LINK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
fn returned_error_boundary(name: &str) -> Result<(), GfError> {
    if name == "install:temp-sealed" {
        let hook = BEFORE_OBJECT_LINK.with(|current| current.borrow_mut().take());
        if let Some(hook) = hook {
            hook();
        }
    }
    if RETURNED_ERROR_BOUNDARY.with(|boundary| boundary.borrow().as_deref() == Some(name)) {
        return Err(GfError::Storage(format!(
            "injected graph object returned error at {name}"
        )));
    }
    Ok(())
}

#[cfg(not(test))]
#[allow(clippy::unnecessary_wraps)]
fn returned_error_boundary(_name: &str) -> Result<(), GfError> {
    Ok(())
}

/// Kernel-visible lease protecting CAS objects installed by one publication.
pub struct GraphObjectPublicationLease {
    cas: CasRoot,
    lease_name: std::ffi::OsString,
    lease_identity: graphforge_filesystem::FileIdentity,
    file: Option<File>,
}

struct HeldCasLocks<'a> {
    #[cfg(unix)]
    objects: &'a StableDirectory,
    lifecycle: &'a File,
    #[cfg(unix)]
    objects_locked: bool,
    lifecycle_locked: bool,
}

impl HeldCasLocks<'_> {
    fn disarm(mut self) {
        self.lifecycle_locked = false;
        #[cfg(unix)]
        {
            self.objects_locked = false;
        }
    }
}

impl Drop for HeldCasLocks<'_> {
    fn drop(&mut self) {
        if self.lifecycle_locked {
            let _ = crate::file_lock::unlock(self.lifecycle);
        }
        #[cfg(unix)]
        if self.objects_locked {
            let _ = self.objects.unlock();
        }
    }
}

struct PendingPublication<'a> {
    cas: &'a CasRoot,
    lease_name: std::ffi::OsString,
    lease_identity: Option<graphforge_filesystem::FileIdentity>,
    file: Option<File>,
    lease_locked: bool,
}

impl Drop for PendingPublication<'_> {
    fn drop(&mut self) {
        if self.lease_locked
            && let Some(file) = self.file.as_ref()
        {
            let _ = crate::file_lock::unlock(file);
        }
        self.file.take();
        if let Some(identity) = self.lease_identity {
            let _ = self
                .cas
                .active
                .unlink_child_if_identity(&self.lease_name, identity);
            let _ = self.cas.active.sync();
        }
    }
}

/// Exclusive guard spanning GC root discovery through sweep.
pub(crate) struct GraphObjectGcGuard {
    cas: CasRoot,
}

struct CasRoot {
    allocation: Option<crate::StorageAllocationOperation>,
    diagnostic_root: PathBuf,
    project: StableDirectory,
    objects: StableDirectory,
    sha256: StableDirectory,
    tmp: StableDirectory,
    active: StableDirectory,
    lifecycle: File,
    lifecycle_identity: graphforge_filesystem::FileIdentity,
}

struct ReadOnlyCasRoot {
    diagnostic_root: PathBuf,
    project: StableDirectory,
    objects: StableDirectory,
    sha256: StableDirectory,
    lifecycle: File,
    lifecycle_identity: graphforge_filesystem::FileIdentity,
    locks_held: bool,
}

#[cfg_attr(windows, allow(unused_variables))]
fn lock_cas_shared<'a>(
    objects: &'a StableDirectory,
    lifecycle: &'a File,
    root: &Path,
    action: &str,
) -> Result<HeldCasLocks<'a>, GfError> {
    #[cfg(unix)]
    objects.lock_shared().map_err(|error| {
        storage(
            &format!("lock graph object directory for {action}"),
            root,
            error,
        )
    })?;
    let mut held = HeldCasLocks {
        #[cfg(unix)]
        objects,
        lifecycle,
        #[cfg(unix)]
        objects_locked: true,
        lifecycle_locked: false,
    };
    #[cfg(all(test, unix))]
    returned_error_boundary(&format!("{action}:objects-lock"))?;
    crate::file_lock::lock_shared(lifecycle).map_err(|error| {
        storage(
            &format!("lock graph object {action} lifecycle"),
            root,
            error,
        )
    })?;
    held.lifecycle_locked = true;
    #[cfg(test)]
    returned_error_boundary(&format!("{action}:lifecycle-lock"))?;
    Ok(held)
}

#[cfg_attr(windows, allow(unused_variables))]
fn lock_cas_exclusive<'a>(
    objects: &'a StableDirectory,
    lifecycle: &'a File,
    root: &Path,
) -> Result<HeldCasLocks<'a>, GfError> {
    #[cfg(unix)]
    objects
        .lock_exclusive()
        .map_err(|error| storage("lock graph object directory for GC", root, error))?;
    let mut held = HeldCasLocks {
        #[cfg(unix)]
        objects,
        lifecycle,
        #[cfg(unix)]
        objects_locked: true,
        lifecycle_locked: false,
    };
    #[cfg(all(test, unix))]
    returned_error_boundary("gc:objects-lock")?;
    crate::file_lock::lock_exclusive(lifecycle)
        .map_err(|error| storage("lock graph object GC lifecycle", root, error))?;
    held.lifecycle_locked = true;
    returned_error_boundary("gc:lifecycle-lock")?;
    Ok(held)
}

fn revalidate_lifecycle(
    objects: &StableDirectory,
    lifecycle: &File,
    expected_identity: graphforge_filesystem::FileIdentity,
) -> std::io::Result<()> {
    if graphforge_filesystem::file_link_count(lifecycle)? != 1 {
        return Err(std::io::Error::other("lifecycle lock is multiply linked"));
    }
    let named = objects.open_child_file(std::ffi::OsStr::new(LIFECYCLE_LOCK))?;
    if graphforge_filesystem::file_identity(&named)? != expected_identity {
        return Err(std::io::Error::other("lifecycle identity changed"));
    }
    Ok(())
}

impl CasRoot {
    fn open_mutable(root: &Path) -> Result<Self, GfError> {
        let project = StableDirectory::open(root)
            .map_err(|error| storage("open stable project root", root, error))?;
        let objects = project
            .create_child_directory(std::ffi::OsStr::new(GRAPH_OBJECTS_DIR))
            .map_err(|error| storage("open stable graph object root", root, error))?;
        let sha256 = objects
            .create_child_directory(std::ffi::OsStr::new(SHA256_DIR))
            .map_err(|error| storage("open stable graph object digest root", root, error))?;
        let tmp = objects
            .create_child_directory(std::ffi::OsStr::new(TEMP_DIR))
            .map_err(|error| storage("open stable graph object temporary root", root, error))?;
        let active = objects
            .create_child_directory(std::ffi::OsStr::new(ACTIVE_DIR))
            .map_err(|error| storage("open stable graph object active root", root, error))?;
        let lifecycle = objects
            .open_or_create_child_file(std::ffi::OsStr::new(LIFECYCLE_LOCK))
            .map_err(|error| storage("open stable graph object lifecycle", root, error))?;
        if graphforge_filesystem::file_link_count(&lifecycle)
            .map_err(|error| storage("inspect graph object lifecycle links", root, error))?
            != 1
        {
            return Err(validation("graph object lifecycle lock is multiply linked"));
        }
        let lifecycle_identity = graphforge_filesystem::file_identity(&lifecycle)
            .map_err(|error| storage("inspect graph object lifecycle identity", root, error))?;
        Ok(Self {
            allocation: None,
            diagnostic_root: root.to_path_buf(),
            project,
            objects,
            sha256,
            tmp,
            active,
            lifecycle,
            lifecycle_identity,
        })
    }

    fn revalidate_named(&self) -> Result<(), GfError> {
        self.project
            .revalidate_named()
            .and_then(|()| self.objects.revalidate_named())
            .and_then(|()| self.sha256.revalidate_named())
            .and_then(|()| self.tmp.revalidate_named())
            .and_then(|()| self.active.revalidate_named())
            .and_then(|()| {
                revalidate_lifecycle(&self.objects, &self.lifecycle, self.lifecycle_identity)
            })
            .map_err(|error| {
                storage(
                    "revalidate stable graph object root",
                    &self.diagnostic_root,
                    error,
                )
            })
    }

    fn digest_bucket(&self, digest: &str, create: bool) -> Result<StableDirectory, GfError> {
        validate_digest(digest)?;
        let name = std::ffi::OsStr::new(&digest[..2]);
        let result = if create {
            self.sha256.create_child_directory(name)
        } else {
            self.sha256.open_child_directory(name)
        };
        result.map_err(|error| {
            storage(
                "open stable graph object bucket",
                &self.diagnostic_root,
                error,
            )
        })
    }

    fn open_digest(&self, digest: &str) -> Result<File, GfError> {
        let bucket = self.digest_bucket(digest, false)?;
        bucket
            .open_child_file(std::ffi::OsStr::new(&digest[2..]))
            .map_err(|error| storage("open stable graph object", &self.diagnostic_root, error))
    }
}

impl ReadOnlyCasRoot {
    fn open(root: &Path) -> Result<Self, GfError> {
        let project = StableDirectory::open(root)
            .map_err(|error| storage("open stable project root", root, error))?;
        let objects = project
            .open_child_directory(std::ffi::OsStr::new(GRAPH_OBJECTS_DIR))
            .map_err(|error| storage("open existing graph object root", root, error))?;
        let sha256 = objects
            .open_child_directory(std::ffi::OsStr::new(SHA256_DIR))
            .map_err(|error| storage("open existing graph object digest root", root, error))?;
        let lifecycle = objects
            .open_child_file(std::ffi::OsStr::new(LIFECYCLE_LOCK))
            .map_err(|error| storage("open existing graph object lifecycle", root, error))?;
        if graphforge_filesystem::file_link_count(&lifecycle)
            .map_err(|error| storage("inspect graph object lifecycle links", root, error))?
            != 1
        {
            return Err(validation("graph object lifecycle lock is multiply linked"));
        }
        let lifecycle_identity = graphforge_filesystem::file_identity(&lifecycle)
            .map_err(|error| storage("inspect graph object lifecycle identity", root, error))?;
        let cas = Self {
            diagnostic_root: root.to_path_buf(),
            project,
            objects,
            sha256,
            lifecycle,
            lifecycle_identity,
            locks_held: false,
        };
        let locks = lock_cas_shared(&cas.objects, &cas.lifecycle, root, "reading")?;
        cas.revalidate_named()?;
        returned_error_boundary("reading:revalidate")?;
        locks.disarm();
        let mut cas = cas;
        cas.locks_held = true;
        Ok(cas)
    }

    fn revalidate_named(&self) -> Result<(), GfError> {
        self.project
            .revalidate_named()
            .and_then(|()| self.objects.revalidate_named())
            .and_then(|()| self.sha256.revalidate_named())
            .and_then(|()| {
                revalidate_lifecycle(&self.objects, &self.lifecycle, self.lifecycle_identity)
            })
            .map_err(|error| {
                storage(
                    "revalidate read-only graph object root",
                    &self.diagnostic_root,
                    error,
                )
            })
    }

    fn digest_bucket(&self, digest: &str) -> Result<StableDirectory, GfError> {
        validate_digest(digest)?;
        self.sha256
            .open_child_directory(std::ffi::OsStr::new(&digest[..2]))
            .map_err(|error| {
                storage(
                    "open stable graph object bucket",
                    &self.diagnostic_root,
                    error,
                )
            })
    }

    fn open_digest(&self, digest: &str) -> Result<File, GfError> {
        self.digest_bucket(digest)?
            .open_child_file(std::ffi::OsStr::new(&digest[2..]))
            .map_err(|error| storage("open stable graph object", &self.diagnostic_root, error))
    }
}

impl Drop for ReadOnlyCasRoot {
    fn drop(&mut self) {
        if self.locks_held {
            let _ = crate::file_lock::unlock(&self.lifecycle);
            #[cfg(unix)]
            let _ = self.objects.unlock();
        }
    }
}

impl Drop for GraphObjectGcGuard {
    fn drop(&mut self) {
        let _ = crate::file_lock::unlock(&self.cas.lifecycle);
        #[cfg(unix)]
        let _ = self.cas.objects.unlock();
    }
}

impl Drop for GraphObjectPublicationLease {
    fn drop(&mut self) {
        if let Some(file) = self.file.take() {
            let _ = crate::file_lock::unlock(&file);
            drop(file);
        }
        let _ = self
            .cas
            .active
            .unlink_child_if_identity(&self.lease_name, self.lease_identity);
        let _ = self.cas.active.sync();
        let _ = crate::file_lock::unlock(&self.cas.lifecycle);
        #[cfg(unix)]
        let _ = self.cas.objects.unlock();
    }
}

impl GraphObjectPublicationLease {
    /// Attach explicit private allocation evidence before this lease installs objects.
    #[doc(hidden)]
    pub fn set_allocation_operation(
        &mut self,
        allocation: Option<crate::StorageAllocationOperation>,
    ) {
        self.cas.allocation = allocation;
    }

    /// Revalidate the stable CAS root immediately before publishing `CURRENT`.
    pub fn revalidate_for_publish(&self) -> Result<(), GfError> {
        validate_publication_identity(self)
    }

    pub(crate) fn revalidate_for_root(&self, root: &Path) -> Result<(), GfError> {
        let requested_root = crate::filesystem_admission::open_directory_handle(root)
            .map_err(|error| storage("open requested graph object root", root, error))?;
        if self.cas.project.identity()
            != graphforge_filesystem::file_identity(&requested_root)
                .map_err(|error| storage("inspect requested graph object root", root, error))?
        {
            return Err(validation(
                "graph object lease belongs to a different project",
            ));
        }
        self.revalidate_for_publish()
    }
}

/// Begin a CAS installation attempt and hold its lease through CURRENT.
pub fn begin_graph_object_publication(root: &Path) -> Result<GraphObjectPublicationLease, GfError> {
    let cas = CasRoot::open_mutable(root)?;
    let locks = lock_cas_shared(&cas.objects, &cas.lifecycle, root, "publication")?;
    cas.revalidate_named()?;
    returned_error_boundary("publication:revalidate")?;
    let lease_name = std::ffi::OsString::from(format!("{}.lock", Uuid::new_v4().hyphenated()));
    let file = cas
        .active
        .create_child_file(&lease_name)
        .map_err(|error| storage("create graph object publication lease", root, error))?;
    let mut pending = PendingPublication {
        cas: &cas,
        lease_name: lease_name.clone(),
        lease_identity: None,
        file: Some(file),
        lease_locked: false,
    };
    returned_error_boundary("publication:lease-create")?;
    let lease_identity = graphforge_filesystem::file_identity(pending.file.as_ref().unwrap())
        .map_err(|error| storage("inspect graph object publication lease", root, error))?;
    pending.lease_identity = Some(lease_identity);
    returned_error_boundary("publication:lease-identity")?;
    crate::file_lock::lock_exclusive(pending.file.as_ref().unwrap())
        .map_err(|error| storage("lock graph object publication lease", root, error))?;
    pending.lease_locked = true;
    returned_error_boundary("publication:lease-lock")?;
    pending
        .file
        .as_ref()
        .unwrap()
        .sync_all()
        .map_err(|error| storage("sync graph object publication lease", root, error))?;
    returned_error_boundary("publication:lease-sync")?;
    cas.active
        .sync()
        .map_err(|error| storage("sync graph object active directory", root, error))?;
    returned_error_boundary("publication:active-sync")?;
    let file = pending.file.take().unwrap();
    pending.lease_locked = false;
    pending.lease_identity = None;
    drop(pending);
    locks.disarm();
    Ok(GraphObjectPublicationLease {
        cas,
        lease_name,
        lease_identity,
        file: Some(file),
    })
}

/// Return true when any live CAS publication lease prevents safe sweeping.
/// Unlocked lease files are crash residue and are removed while the caller
/// holds the project writer/recovery lock.
pub fn graph_object_publication_is_live(root: &Path) -> Result<bool, GfError> {
    let cas = CasRoot::open_mutable(root)?;
    let entries = cas
        .active
        .child_names()
        .map_err(|error| storage("read graph object active directory", root, error))?;
    let mut live = false;
    for entry in entries {
        let name = entry
            .clone()
            .into_string()
            .map_err(|_| validation("graph object lease name is not UTF-8"))?;
        let uuid_text = name
            .strip_suffix(".lock")
            .ok_or_else(|| validation("graph object lease name is not canonical"))?;
        let uuid = Uuid::parse_str(uuid_text)
            .map_err(|_| validation("graph object lease name is not canonical"))?;
        if uuid.hyphenated().to_string() != uuid_text {
            return Err(validation("graph object lease name is not canonical"));
        }
        let file = cas
            .active
            .open_child_file(&entry)
            .map_err(|error| storage("open graph object lease", root, error))?;
        let identity = graphforge_filesystem::file_identity(&file)
            .map_err(|error| storage("inspect graph object lease", root, error))?;
        if crate::file_lock::try_lock_exclusive(&file)
            .map_err(|error| storage("probe graph object lease", root, error))?
        {
            crate::file_lock::unlock(&file)
                .map_err(|error| storage("unlock graph object lease", root, error))?;
            drop(file);
            cas.active
                .unlink_child_if_identity(&entry, identity)
                .map_err(|error| storage("remove stale graph object lease", root, error))?;
            cas.active
                .sync()
                .map_err(|error| storage("sync graph object active directory", root, error))?;
        } else {
            live = true;
        }
    }
    Ok(live)
}

/// Exact application-observed work for one object installation.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GraphObjectInstallEvidence {
    /// Source payload bytes read and hashed.
    pub bytes_hashed: u64,
    /// Logical payload bytes newly installed into the object store.
    pub bytes_installed: u64,
    /// Whether an already installed exact object satisfied the request.
    pub reused_existing: bool,
    /// A temporary object was written and finalized, possibly losing a publication race.
    pub attempted_install: bool,
    /// Non-empty source or authentication reads completed by the application.
    pub read_calls: u64,
    /// Temporary-object write submissions completed by the application.
    pub write_calls: u64,
    /// Payload bytes submitted to temporary-object writers.
    pub write_bytes: u64,
    /// File and directory durability barriers completed by this installation.
    pub fsync_calls: u64,
    /// Completed synchronization of the temporary payload file.
    pub file_fsync_calls: u64,
    /// Completed synchronization of object and temporary namespaces.
    pub directory_fsync_calls: u64,
}

/// Content-free application work accumulated from actual CAS operations.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphObjectIoTotals {
    /// Bytes consumed by authentication and source reads.
    pub read_bytes: u64,
    /// Nonempty application read submissions.
    pub read_calls: u64,
    /// Bytes submitted to object writers.
    pub write_bytes: u64,
    /// Nonempty application write submissions.
    pub write_calls: u64,
    /// Completed file synchronization operations, including cache rollovers.
    pub file_fsync_calls: u64,
    /// Completed namespace synchronization operations.
    pub directory_fsync_calls: u64,
    /// Objects newly installed; shared objects are charged on first install only.
    pub installed_objects: u64,
    /// Completed temporary-object installation attempts, including concurrent losers.
    pub install_attempts: u64,
    /// Install requests satisfied by authenticated existing objects.
    pub reused_objects: u64,
    /// Logical bytes newly installed.
    pub installed_bytes: u64,
}

impl GraphObjectIoTotals {
    fn add_install(&mut self, evidence: &GraphObjectInstallEvidence) -> Result<(), GfError> {
        if evidence
            .file_fsync_calls
            .checked_add(evidence.directory_fsync_calls)
            != Some(evidence.fsync_calls)
        {
            return Err(validation(
                "CAS synchronization components do not reconcile",
            ));
        }
        self.checked_add_assign(&Self {
            read_bytes: evidence.bytes_hashed,
            read_calls: evidence.read_calls,
            write_bytes: evidence.write_bytes,
            write_calls: evidence.write_calls,
            file_fsync_calls: evidence.file_fsync_calls,
            directory_fsync_calls: evidence.directory_fsync_calls,
            installed_objects: u64::from(!evidence.reused_existing),
            install_attempts: u64::from(evidence.attempted_install),
            reused_objects: u64::from(evidence.reused_existing),
            installed_bytes: evidence.bytes_installed,
        })
    }

    fn add_read(&mut self, io: ReadIoEvidence) -> Result<(), GfError> {
        self.checked_add_assign(&Self {
            read_bytes: io.bytes,
            read_calls: io.calls,
            ..Self::default()
        })
    }

    fn checked_add_assign(&mut self, other: &Self) -> Result<(), GfError> {
        for (counter, value) in [
            (&mut self.read_bytes, other.read_bytes),
            (&mut self.read_calls, other.read_calls),
            (&mut self.write_bytes, other.write_bytes),
            (&mut self.write_calls, other.write_calls),
            (&mut self.file_fsync_calls, other.file_fsync_calls),
            (&mut self.directory_fsync_calls, other.directory_fsync_calls),
            (&mut self.installed_objects, other.installed_objects),
            (&mut self.install_attempts, other.install_attempts),
            (&mut self.reused_objects, other.reused_objects),
            (&mut self.installed_bytes, other.installed_bytes),
        ] {
            *counter = counter
                .checked_add(value)
                .ok_or_else(|| validation("CAS component counter overflows"))?;
        }
        Ok(())
    }
}

/// Measured CAS work separated by the authority that performed it.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphPublicationIo {
    /// Completed append invocations represented by these components.
    pub publications: u64,
    /// Authenticated prior logical entries across those invocations.
    pub initial_entries: u64,
    /// Sealed and tombstoned paths submitted to path-copy updates.
    pub changed_paths: u64,
    /// Canonical payload installation and source authentication.
    pub payload: GraphObjectIoTotals,
    /// Immutable manifest-node installation and reuse authentication.
    pub manifest: GraphObjectIoTotals,
    /// Existing manifest nodes read during path-copy updates.
    pub manifest_reads: GraphObjectIoTotals,
}

impl GraphPublicationIo {
    /// Add one completed invocation without replaying earlier receipt work.
    pub fn checked_add_assign(&mut self, other: &Self) -> Result<(), GfError> {
        for (counter, value) in [
            (&mut self.publications, other.publications),
            (&mut self.initial_entries, other.initial_entries),
            (&mut self.changed_paths, other.changed_paths),
        ] {
            *counter = counter
                .checked_add(value)
                .ok_or_else(|| validation("CAS publication inventory overflows"))?;
        }
        self.payload.checked_add_assign(&other.payload)?;
        self.manifest.checked_add_assign(&other.manifest)?;
        self.manifest_reads
            .checked_add_assign(&other.manifest_reads)
    }

    /// Reconcile the independently accumulated operation components.
    pub fn totals(&self) -> Result<GraphObjectIoTotals, GfError> {
        let mut totals = self.payload.clone();
        totals.checked_add_assign(&self.manifest)?;
        totals.checked_add_assign(&self.manifest_reads)?;
        Ok(totals)
    }
}

/// One-time v1 expanded-tree to v2 object-store migration evidence.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GraphFilesMigrationEvidence {
    /// Payload objects examined.
    pub payload_objects: u64,
    /// Source payload bytes hashed.
    pub payload_bytes_hashed: u64,
    /// Logical payload and segment bytes newly installed.
    pub bytes_installed: u64,
}

/// Exact publication work, including authentication of the caller's prior cache.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GraphFilesAppendEvidence {
    /// New/replaced and tombstoned descriptors examined.
    pub changed_entries_examined: u64,
    /// Prior descriptors authenticated before publication.
    pub prior_entries_examined: u64,
    /// New/replaced payload bytes hashed, including verification passes.
    pub payload_bytes_hashed: u64,
    /// Logical object bytes newly installed.
    pub bytes_installed: u64,
    /// Actual nonempty payload, manifest-install and path-authentication reads.
    pub read_calls: u64,
    /// Actual payload and manifest write submissions performed by object installation.
    pub write_calls: u64,
    /// Payload bytes submitted to object writers.
    pub write_bytes: u64,
    /// File and directory durability barriers completed by object installation.
    pub fsync_calls: u64,
    /// Independent payload, manifest install and manifest path-read components.
    pub publication_io: GraphPublicationIo,
}

/// A graph file whose content identity was established by an upstream durable
/// writer. Publication still authenticates these bytes while installing them;
/// this capability only removes a redundant standalone pre-hash pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AuthenticatedGraphFile {
    pub(crate) relative_path: PathBuf,
    pub(crate) byte_length: u64,
    pub(crate) content_sha256: String,
}

/// Mark/sweep evidence for project-level graph objects.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GraphObjectGcEvidence {
    /// Reachable segment and payload objects marked.
    pub objects_marked: u64,
    /// Unreachable objects removed.
    pub objects_removed: u64,
    /// Physical unreachable bytes removed.
    pub bytes_removed: u64,
    /// Exact native identities and allocated bytes removed by this GC receipt.
    pub removed_identity_allocated_bytes: BTreeMap<String, u64>,
}

#[cfg(test)]
pub(crate) fn begin_graph_object_gc(root: &Path) -> Result<GraphObjectGcGuard, GfError> {
    let cas = CasRoot::open_mutable(root)?;
    let locks = lock_cas_exclusive(&cas.objects, &cas.lifecycle, root)?;
    cas.revalidate_named()?;
    returned_error_boundary("gc:revalidate")?;
    locks.disarm();
    Ok(GraphObjectGcGuard { cas })
}

pub(crate) fn try_begin_graph_object_gc(
    root: &Path,
) -> Result<Option<GraphObjectGcGuard>, GfError> {
    let cas = CasRoot::open_mutable(root)?;
    #[cfg(unix)]
    if !cas
        .objects
        .try_lock_exclusive()
        .map_err(|error| storage("try graph object directory for GC", root, error))?
    {
        return Ok(None);
    }
    let mut locks = HeldCasLocks {
        #[cfg(unix)]
        objects: &cas.objects,
        lifecycle: &cas.lifecycle,
        #[cfg(unix)]
        objects_locked: true,
        lifecycle_locked: false,
    };
    #[cfg(all(test, unix))]
    returned_error_boundary("try-gc:objects-lock")?;
    if !crate::file_lock::try_lock_exclusive(&cas.lifecycle)
        .map_err(|error| storage("try graph object GC lifecycle", root, error))?
    {
        return Ok(None);
    }
    locks.lifecycle_locked = true;
    returned_error_boundary("try-gc:lifecycle-lock")?;
    cas.revalidate_named()?;
    returned_error_boundary("try-gc:revalidate")?;
    locks.disarm();
    Ok(Some(GraphObjectGcGuard { cas }))
}

#[cfg(windows)]
fn validate_directory_identity(file: &File, path: &Path) -> Result<(), GfError> {
    let descriptor = file
        .metadata()
        .map_err(|error| storage("inspect lifecycle directory descriptor", path, error))?;
    let named = fs::symlink_metadata(path)
        .map_err(|error| storage("inspect lifecycle directory path", path, error))?;
    if !descriptor.is_dir() || !named.is_dir() || named.file_type().is_symlink() {
        return Err(validation(
            "graph object lifecycle directory is not a real directory",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if descriptor.dev() != named.dev() || descriptor.ino() != named.ino() {
            return Err(validation(
                "graph object lifecycle directory identity changed",
            ));
        }
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt as _;
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
        if named.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
            || graphforge_filesystem::file_identity(file)
                .map_err(|error| storage("inspect lifecycle directory identity", path, error))?
                != graphforge_filesystem::path_identity(path)
                    .map_err(|error| storage("inspect lifecycle directory identity", path, error))?
        {
            return Err(validation(
                "graph object lifecycle directory identity changed",
            ));
        }
    }
    Ok(())
}

fn validate_publication_identity(lease: &GraphObjectPublicationLease) -> Result<(), GfError> {
    lease.cas.revalidate_named()
}

/// Resolve a digest to its admitted project-level object path.
pub fn graph_object_path(root: &Path, digest: &str) -> Result<PathBuf, GfError> {
    validate_digest(digest)?;
    Ok(root
        .join(GRAPH_OBJECTS_DIR)
        .join(SHA256_DIR)
        .join(&digest[..2])
        .join(&digest[2..]))
}

#[cfg(test)]
pub(crate) fn corrupt_sealed_graph_object_for_test(path: &Path, bytes: &[u8]) {
    let metadata = fs::metadata(path).expect("inspect sealed graph object fixture");
    assert!(
        metadata.permissions().readonly(),
        "graph object fixture must be sealed before hostile corruption"
    );
    let mut permissions = metadata.permissions();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        permissions.set_mode(permissions.mode() | 0o200);
    }
    #[cfg(not(unix))]
    permissions.set_readonly(false);
    fs::set_permissions(path, permissions)
        .expect("make exact graph object fixture owner-writable for hostile corruption");
    fs::write(path, bytes).expect("corrupt exact sealed graph object fixture");
}

/// Read and cryptographically verify an immutable object.
pub fn read_graph_object(
    root: &Path,
    digest: &str,
    expected_length: u64,
) -> Result<Vec<u8>, GfError> {
    let cas = ReadOnlyCasRoot::open(root)?;
    let mut file = cas.open_digest(digest)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|error| storage("read stable graph object", root, error))?;
    verify_object_bytes(digest, expected_length, &bytes)?;
    Ok(bytes)
}

/// Stream-verify a payload object without retaining it in memory.
pub fn verify_graph_object(root: &Path, digest: &str, expected_length: u64) -> Result<(), GfError> {
    let cas = ReadOnlyCasRoot::open(root)?;
    verify_file(cas.open_digest(digest)?, digest, expected_length, root)
}

pub(crate) fn verify_graph_object_with_lease(
    lease: &GraphObjectPublicationLease,
    digest: &str,
    expected_length: u64,
) -> Result<(), GfError> {
    lease.cas.revalidate_named()?;
    verify_file(
        lease.cas.open_digest(digest)?,
        digest,
        expected_length,
        &lease.cas.diagnostic_root,
    )
}

/// Read an object whose digest is known before its declared logical length.
/// `max_length` bounds allocation for untrusted manifest objects.
pub fn read_graph_object_by_digest(
    root: &Path,
    digest: &str,
    max_length: u64,
) -> Result<Vec<u8>, GfError> {
    let cas = ReadOnlyCasRoot::open(root)?;
    read_graph_object_by_digest_from_read_only_cas(&cas, digest, max_length)
}

/// Open and stream-authenticate one immutable CAS object without allocating its payload.
pub struct AuthenticatedGraphObject {
    file: File,
    authenticated_length: u64,
    _cas: std::sync::Arc<ReadOnlyCasRoot>,
}

#[derive(Clone)]
pub(crate) struct GraphObjectReadLease {
    cas: std::sync::Arc<ReadOnlyCasRoot>,
}

impl std::fmt::Debug for GraphObjectReadLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GraphObjectReadLease")
            .finish_non_exhaustive()
    }
}

pub(crate) fn begin_graph_object_read(root: &Path) -> Result<GraphObjectReadLease, GfError> {
    Ok(GraphObjectReadLease {
        cas: std::sync::Arc::new(ReadOnlyCasRoot::open(root)?),
    })
}

impl GraphObjectReadLease {
    /// Authenticate only a construction compaction input, retaining its CAS lease.
    pub(crate) fn open_for_construction(
        &self,
        digest: &str,
        expected_length: u64,
        cancelled: &mut impl FnMut() -> bool,
    ) -> Result<
        (
            AuthenticatedGraphObject,
            GraphObjectIoTotals,
            graphforge_filesystem::FileCacheReleaseEvidence,
        ),
        GfError,
    > {
        let file = self.cas.open_digest(digest)?;
        let metadata = file.metadata().map_err(|error| {
            storage(
                "inspect construction input",
                &self.cas.diagnostic_root,
                error,
            )
        })?;
        if !metadata.is_file()
            || metadata.len() != expected_length
            || !metadata.permissions().readonly()
        {
            return Err(validation("construction object authority changed"));
        }
        let retained = file
            .try_clone()
            .map_err(|error| storage("pin construction input", &self.cas.diagnostic_root, error))?;
        let mut reader =
            graphforge_filesystem::FileCacheReleasingReader::new(file).map_err(|error| {
                storage("bound construction input", &self.cas.diagnostic_root, error)
            })?;
        let mut totals = GraphObjectIoTotals::default();
        let mut digest_state = Sha256::new();
        let mut buffer = vec![0; 64 * 1024];
        let verified = (|| {
            loop {
                if cancelled() {
                    return Err(validation("construction ordinal authentication cancelled"));
                }
                let count = reader.read(&mut buffer).map_err(|error| {
                    storage(
                        "authenticate construction input",
                        &self.cas.diagnostic_root,
                        error,
                    )
                })?;
                if count == 0 {
                    break;
                }
                totals.read_bytes = totals
                    .read_bytes
                    .checked_add(count as u64)
                    .ok_or_else(|| validation("construction read bytes overflow"))?;
                totals.read_calls = totals
                    .read_calls
                    .checked_add(1)
                    .ok_or_else(|| validation("construction read calls overflow"))?;
                digest_state.update(&buffer[..count]);
            }
            if totals.read_bytes != expected_length
                || hex_digest(digest_state.finalize().into()) != digest
            {
                return Err(validation(
                    "construction object digest does not match its address",
                ));
            }
            Ok(())
        })();
        let released = reader.finish().map_err(|error| {
            storage(
                "release construction input",
                &self.cas.diagnostic_root,
                error,
            )
        });
        let cache = match (verified, released) {
            (Ok(()), Ok(cache)) => cache,
            (Err(primary), Ok(_)) => return Err(primary),
            (Ok(()), Err(error)) => return Err(error),
            (Err(primary), Err(cleanup)) => {
                return Err(validation(format!("{primary}; {cleanup}")));
            }
        };
        Ok((
            AuthenticatedGraphObject {
                file: retained,
                authenticated_length: expected_length,
                _cas: std::sync::Arc::clone(&self.cas),
            },
            totals,
            cache,
        ))
    }

    pub(crate) fn open(
        &self,
        digest: &str,
        expected_length: u64,
    ) -> Result<AuthenticatedGraphObject, GfError> {
        open_graph_object_with_read_lease(self, digest, expected_length)
    }

    /// Open one immutable CAS object for identity and space attribution only.
    ///
    /// This binds the descriptor the same way [`Self::open`] does — stable
    /// no-follow CAS traversal, exact digest address, exact declared length,
    /// read-only permission — but does not stream the payload through
    /// SHA-256. Storage attribution never consumes object content; it reads
    /// file identity and space usage from the descriptor. Content
    /// re-authentication of the retained store is the explicit
    /// `graphforge verify` command's job (`crate::verify_project_store`).
    pub(crate) fn open_for_attribution(
        &self,
        digest: &str,
        expected_length: u64,
    ) -> Result<File, GfError> {
        let file = self.cas.open_digest(digest)?;
        let metadata = file.metadata().map_err(|error| {
            storage(
                "inspect stable graph object",
                &self.cas.diagnostic_root,
                error,
            )
        })?;
        if !metadata.is_file()
            || metadata.len() != expected_length
            || !metadata.permissions().readonly()
        {
            return Err(validation("graph object authority changed"));
        }
        Ok(file)
    }
}

impl std::fmt::Debug for AuthenticatedGraphObject {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthenticatedGraphObject")
            .finish_non_exhaustive()
    }
}

impl AuthenticatedGraphObject {
    pub(crate) fn try_clone_file(&self) -> std::io::Result<File> {
        self.file.try_clone()
    }

    pub(crate) fn authenticated_length(&self) -> u64 {
        self.authenticated_length
    }
}

impl AsRef<File> for AuthenticatedGraphObject {
    fn as_ref(&self) -> &File {
        &self.file
    }
}

impl Read for AuthenticatedGraphObject {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.file.read(buffer)
    }
}

impl Seek for AuthenticatedGraphObject {
    fn seek(&mut self, position: std::io::SeekFrom) -> std::io::Result<u64> {
        self.file.seek(position)
    }
}

impl Length for AuthenticatedGraphObject {
    fn len(&self) -> u64 {
        self.authenticated_length
    }
}

impl ChunkReader for AuthenticatedGraphObject {
    type T = std::io::BufReader<File>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        let mut file = self.file.try_clone()?;
        file.seek(std::io::SeekFrom::Start(start))?;
        Ok(std::io::BufReader::new(file))
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<bytes::Bytes> {
        let mut file = self.file.try_clone()?;
        file.seek(std::io::SeekFrom::Start(start))?;
        let mut value = vec![0; length];
        file.read_exact(&mut value)?;
        Ok(value.into())
    }
}

/// Retain and stream-authenticate the exact immutable object selected by a graph manifest.
/// The returned Parquet reader keeps its CAS read lease and file descriptor alive.
///
/// # Errors
/// Rejects changed ownership, length or digest and reports filesystem failures.
pub fn open_graph_object_by_digest(
    root: &Path,
    digest: &str,
    expected_length: u64,
) -> Result<AuthenticatedGraphObject, GfError> {
    begin_graph_object_read(root)?.open(digest, expected_length)
}

fn open_graph_object_with_read_lease(
    lease: &GraphObjectReadLease,
    digest: &str,
    expected_length: u64,
) -> Result<AuthenticatedGraphObject, GfError> {
    let mut file = lease.cas.open_digest(digest)?;
    let metadata = file.metadata().map_err(|error| {
        storage(
            "inspect stable graph object",
            &lease.cas.diagnostic_root,
            error,
        )
    })?;
    // CAS payloads are deliberately hard-linked into private materializations,
    // so their link count is not an authority signal. The stable no-follow CAS
    // traversal, exact digest address, exact length, and streamed digest bind
    // this descriptor without relaxing path-native child-file admission.
    if !metadata.is_file()
        || metadata.len() != expected_length
        || !metadata.permissions().readonly()
    {
        return Err(validation("graph object authority changed"));
    }
    let mut hasher = Sha256::new();
    let mut block = vec![0_u8; 1 << 20];
    let mut authenticated = ReadIoEvidence::default();
    loop {
        let count = file.read(&mut block).map_err(|error| {
            storage(
                "authenticate graph object",
                &lease.cas.diagnostic_root,
                error,
            )
        })?;
        if count == 0 {
            break;
        }
        authenticated.bytes = authenticated
            .bytes
            .checked_add(count as u64)
            .ok_or_else(|| validation("graph object lease read bytes overflow"))?;
        authenticated.calls = authenticated
            .calls
            .checked_add(1)
            .ok_or_else(|| validation("graph object lease read calls overflow"))?;
        hasher.update(&block[..count]);
    }
    if hex_digest(hasher.finalize().into()) != digest {
        return Err(validation("graph object digest does not match its address"));
    }
    crate::lifecycle_io::record_read(
        crate::StorageIoPhase::HydrationVerification,
        authenticated.bytes,
        authenticated.calls,
    );
    crate::lifecycle_io::record_objects(crate::StorageIoPhase::HydrationVerification, 1);
    file.rewind()
        .map_err(|error| storage("rewind graph object", &lease.cas.diagnostic_root, error))?;
    Ok(AuthenticatedGraphObject {
        file,
        authenticated_length: expected_length,
        _cas: std::sync::Arc::clone(&lease.cas),
    })
}

pub(crate) fn read_graph_object_counted(
    root: &Path,
    digest: &str,
    maximum: u64,
    totals: &mut GraphObjectIoTotals,
) -> Result<Vec<u8>, GfError> {
    let cas = ReadOnlyCasRoot::open(root)?;
    let (bytes, io) = read_graph_object_by_digest_file_counted(
        cas.open_digest(digest)?,
        digest,
        maximum,
        &cas.diagnostic_root,
    )?;
    totals.read_bytes = totals
        .read_bytes
        .checked_add(io.bytes)
        .ok_or_else(|| validation("object read byte count overflows"))?;
    totals.read_calls = totals
        .read_calls
        .checked_add(io.calls)
        .ok_or_else(|| validation("object read call count overflows"))?;
    Ok(bytes)
}

fn read_graph_object_by_digest_from_read_only_cas(
    cas: &ReadOnlyCasRoot,
    digest: &str,
    max_length: u64,
) -> Result<Vec<u8>, GfError> {
    read_graph_object_by_digest_file(
        cas.open_digest(digest)?,
        digest,
        max_length,
        &cas.diagnostic_root,
    )
}

fn read_graph_object_by_digest_from_cas(
    cas: &CasRoot,
    digest: &str,
    max_length: u64,
) -> Result<Vec<u8>, GfError> {
    read_graph_object_by_digest_file(
        cas.open_digest(digest)?,
        digest,
        max_length,
        &cas.diagnostic_root,
    )
}

fn read_graph_object_by_digest_file(
    file: File,
    digest: &str,
    max_length: u64,
    diagnostic_root: &Path,
) -> Result<Vec<u8>, GfError> {
    read_graph_object_by_digest_file_counted(file, digest, max_length, diagnostic_root)
        .map(|(bytes, _)| bytes)
}

fn read_graph_object_by_digest_file_counted(
    mut file: File,
    digest: &str,
    max_length: u64,
    diagnostic_root: &Path,
) -> Result<(Vec<u8>, ReadIoEvidence), GfError> {
    let metadata = file
        .metadata()
        .map_err(|error| storage("inspect stable graph object", diagnostic_root, error))?;
    if !metadata.is_file() || metadata.len() > max_length {
        return Err(validation(
            "graph object exceeds admitted length or is not regular",
        ));
    }
    let mut bytes = Vec::with_capacity(usize::try_from(metadata.len()).unwrap_or(0));
    let mut io = ReadIoEvidence::default();
    let mut buffer = vec![0; BUFFER_BYTES];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| storage("read stable graph object", diagnostic_root, error))?;
        if read == 0 {
            break;
        }
        io.bytes = io
            .bytes
            .checked_add(read as u64)
            .filter(|total| *total <= max_length)
            .ok_or_else(|| validation("manifest read exceeds admitted byte bound"))?;
        io.calls = io
            .calls
            .checked_add(1)
            .ok_or_else(|| validation("manifest read call count overflows"))?;
        bytes.extend_from_slice(&buffer[..read]);
    }
    if hex_digest(Sha256::digest(&bytes).into()) != digest {
        return Err(validation("graph object digest does not match its address"));
    }
    crate::lifecycle_io::record_read(
        crate::StorageIoPhase::HydrationVerification,
        io.bytes,
        io.calls,
    );
    crate::lifecycle_io::record_objects(crate::StorageIoPhase::HydrationVerification, 1);
    Ok((bytes, io))
}

pub(crate) fn read_graph_object_by_digest_with_lease(
    lease: &GraphObjectPublicationLease,
    digest: &str,
    max_length: u64,
) -> Result<Vec<u8>, GfError> {
    lease.cas.revalidate_named()?;
    read_graph_object_by_digest_from_cas(&lease.cas, digest, max_length)
}

fn checked_read_io_sum(
    left: ReadIoEvidence,
    right: ReadIoEvidence,
) -> Result<ReadIoEvidence, GfError> {
    Ok(ReadIoEvidence {
        bytes: left
            .bytes
            .checked_add(right.bytes)
            .ok_or_else(|| validation("sealed object read byte count overflows"))?,
        calls: left
            .calls
            .checked_add(right.calls)
            .ok_or_else(|| validation("sealed object read call count overflows"))?,
    })
}

fn verify_file(
    file: File,
    digest: &str,
    expected_length: u64,
    diagnostic: &Path,
) -> Result<(), GfError> {
    verify_file_counted(file, digest, expected_length, diagnostic).map(|_| ())
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ReadIoEvidence {
    bytes: u64,
    calls: u64,
}

fn verify_file_counted(
    file: File,
    digest: &str,
    expected_length: u64,
    diagnostic: &Path,
) -> Result<ReadIoEvidence, GfError> {
    let metadata = file
        .metadata()
        .map_err(|error| storage("inspect graph object handle", diagnostic, error))?;
    if !metadata.is_file() || metadata.len() != expected_length {
        return Err(validation(
            "graph object handle is not the declared regular file",
        ));
    }
    let mut file = graphforge_filesystem::FileCacheReleasingReader::new(file)
        .map_err(|error| storage("open bounded graph object handle", diagnostic, error))?;
    let mut hasher = Sha256::new();
    let mut io = ReadIoEvidence::default();
    let mut buffer = vec![0_u8; BUFFER_BYTES];
    let verified = (|| -> Result<(), GfError> {
        loop {
            let read = file
                .read(&mut buffer)
                .map_err(|error| storage("read graph object handle", diagnostic, error))?;
            if read == 0 {
                break;
            }
            io.bytes = io
                .bytes
                .checked_add(
                    u64::try_from(read)
                        .map_err(|_| validation("object authentication read length exceeds u64"))?,
                )
                .ok_or_else(|| validation("object authentication read bytes overflow"))?;
            io.calls = io
                .calls
                .checked_add(1)
                .ok_or_else(|| validation("object authentication read calls overflow"))?;
            hasher.update(&buffer[..read]);
        }
        if hex_digest(hasher.finalize().into()) != digest {
            return Err(validation("graph object digest does not match its address"));
        }
        Ok(())
    })();
    let released = file.finish().map_err(|error| {
        storage(
            "release graph object authentication cache",
            diagnostic,
            error,
        )
    });
    match (verified, released) {
        (Ok(()), Ok(_)) => {
            crate::lifecycle_io::record_read(
                crate::StorageIoPhase::HydrationVerification,
                io.bytes,
                io.calls,
            );
            crate::lifecycle_io::record_objects(crate::StorageIoPhase::HydrationVerification, 1);
            Ok(io)
        }
        (Ok(()), Err(error)) => Err(error),
        (Err(primary), Ok(_)) => Err(primary),
        (Err(primary), Err(release)) => Err(storage(
            "authenticate graph object and release consumed cache",
            diagnostic,
            std::io::Error::other(format!("{primary}; {release}")),
        )),
    }
}

fn verify_stream(
    file: &mut impl Read,
    digest: &str,
    expected_length: u64,
    diagnostic: &Path,
) -> Result<(), GfError> {
    verify_stream_counted(file, digest, expected_length, diagnostic).map(|_| ())
}

fn verify_stream_counted(
    file: &mut impl Read,
    digest: &str,
    expected_length: u64,
    diagnostic: &Path,
) -> Result<ReadIoEvidence, GfError> {
    let mut hasher = Sha256::new();
    let mut total = 0_u64;
    let mut calls = 0_u64;
    let mut buffer = vec![0_u8; BUFFER_BYTES];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| storage("read graph object handle", diagnostic, error))?;
        if read == 0 {
            break;
        }
        total = total
            .checked_add(
                u64::try_from(read)
                    .map_err(|_| validation("object stream read size exceeds u64"))?,
            )
            .ok_or_else(|| validation("object stream byte count overflows"))?;
        calls = calls
            .checked_add(1)
            .ok_or_else(|| validation("object stream call count overflows"))?;
        hasher.update(&buffer[..read]);
    }
    if total != expected_length || hex_digest(hasher.finalize().into()) != digest {
        return Err(validation("graph object digest does not match its address"));
    }
    crate::lifecycle_io::record_read(crate::StorageIoPhase::HydrationVerification, total, calls);
    crate::lifecycle_io::record_objects(crate::StorageIoPhase::HydrationVerification, 1);
    Ok(ReadIoEvidence {
        bytes: total,
        calls,
    })
}

fn hash_regular_file(path: &Path) -> Result<([u8; 32], ReadIoEvidence), GfError> {
    let mut file =
        File::open(path).map_err(|error| storage("open sealed graph file", path, error))?;
    let mut io = ReadIoEvidence::default();
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; BUFFER_BYTES];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| storage("read sealed graph file", path, error))?;
        if read == 0 {
            break;
        }
        io.bytes = io
            .bytes
            .checked_add(read as u64)
            .ok_or_else(|| validation("source authentication bytes overflow"))?;
        io.calls = io
            .calls
            .checked_add(1)
            .ok_or_else(|| validation("source authentication calls overflow"))?;
        hasher.update(&buffer[..read]);
    }
    Ok((hasher.finalize().into(), io))
}

fn validate_logical_path(path: &Path) -> Result<(), GfError> {
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, std::path::Component::Normal(_)))
    {
        return Err(validation("invalid graph object logical path"));
    }
    if path
        .components()
        .next()
        .is_some_and(|component| component.as_os_str() == std::ffi::OsStr::new(".graphforge-cache"))
    {
        return Err(validation("derived cache path cannot be sealed"));
    }
    Ok(())
}

fn validate_digest(digest: &str) -> Result<(), GfError> {
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(validation(
            "graph object digest must be 64 lowercase hex characters",
        ));
    }
    Ok(())
}

fn hex_digest(digest: [u8; 32]) -> String {
    use std::fmt::Write as _;
    digest
        .iter()
        .fold(String::with_capacity(64), |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        })
}

fn validation(message: impl Into<String>) -> GfError {
    GfError::Validation(message.into())
}

fn storage(action: &str, path: &Path, error: impl std::fmt::Display) -> GfError {
    GfError::Storage(format!("{action} at {}: {error}", path.display()))
}

#[cfg(test)]
mod tests;

mod gc;
mod installation;
mod manifest_tree;
mod materialization;

pub(crate) use gc::capture_retained_graph_object_identities;
#[allow(
    unused_imports,
    reason = "preserve the existing staged CAS root API across feature and test configurations"
)]
pub use gc::gc_graph_objects;
pub(crate) use gc::gc_graph_objects_with_evidence_guarded;
#[allow(
    unused_imports,
    reason = "preserve the existing staged CAS root API across feature and test configurations"
)]
pub use installation::install_graph_object_bytes;
use installation::install_graph_object_bytes_with_lease;
#[allow(
    unused_imports,
    reason = "preserve the existing staged CAS root API across feature and test configurations"
)]
pub use installation::install_graph_object_file;
use installation::install_graph_object_file_with_lease;
pub use manifest_tree::GraphManifestState;
#[allow(
    unused_imports,
    reason = "preserve the existing staged CAS root API across feature and test configurations"
)]
pub(crate) use manifest_tree::append_authenticated_graph_files_v2;
pub(crate) use manifest_tree::append_authenticated_mapped_graph_files;
pub use manifest_tree::append_graph_files_v2;
pub(crate) use manifest_tree::append_mapped_import_graph_files;
pub(crate) use manifest_tree::append_replayed_graph_files;
#[allow(
    unused_imports,
    reason = "preserve the existing staged CAS root API across feature and test configurations"
)]
pub use manifest_tree::migrate_graph_files_v1_to_v2;
pub use manifest_tree::prepare_graph_files_replacement;
pub(crate) use manifest_tree::replace_replayed_graph_files;
pub use materialization::materialize_graph_objects;
