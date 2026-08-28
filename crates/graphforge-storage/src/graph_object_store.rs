//! Project-level immutable content-addressed graph objects.

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
const BUFFER_BYTES: usize = 64 * 1024;

#[cfg(test)]
thread_local! {
    static RETURNED_ERROR_BOUNDARY: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
fn returned_error_boundary(name: &str) -> Result<(), GfError> {
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
    diagnostic_root: PathBuf,
    project: StableDirectory,
    objects: StableDirectory,
    sha256: StableDirectory,
    tmp: StableDirectory,
    active: StableDirectory,
    lifecycle: File,
    lifecycle_identity: graphforge_filesystem::FileIdentity,
}

struct TemporaryObject {
    name: std::ffi::OsString,
    #[cfg(unix)]
    file: File,
    #[cfg(windows)]
    file: graphforge_filesystem::WindowsCasWriter,
    identity: graphforge_filesystem::FileIdentity,
}

struct SealedTemporaryObject {
    name: std::ffi::OsString,
    file: File,
    identity: graphforge_filesystem::FileIdentity,
}

#[cfg(unix)]
type CasTemporaryWriter = File;
#[cfg(windows)]
type CasTemporaryWriter = graphforge_filesystem::WindowsCasWriter;

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
    /// Non-empty source or authentication reads completed by the application.
    pub read_calls: u64,
    /// Temporary-object write submissions completed by the application.
    pub write_calls: u64,
    /// Payload bytes submitted to temporary-object writers.
    pub write_bytes: u64,
    /// File and directory durability barriers completed by this installation.
    pub fsync_calls: u64,
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
    /// Actual non-empty payload reads performed by object installation.
    pub read_calls: u64,
    /// Actual payload write submissions performed by object installation.
    pub write_calls: u64,
    /// Payload bytes submitted to object writers.
    pub write_bytes: u64,
    /// File and directory durability barriers completed by object installation.
    pub fsync_calls: u64,
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

/// Storage-owned, root-bound state for a sequence of path-copy publications.
///
/// Opening an existing root authenticates its inventory exactly once. Callers
/// cannot replace the cached inventory independently of the root.
#[derive(Debug, Clone, Default)]
pub struct GraphManifestState {
    project_identity: Option<graphforge_filesystem::FileIdentity>,
    root: Option<GraphFilesRootV2>,
    entries: BTreeMap<String, crate::GraphFileEntry>,
}

impl GraphManifestState {
    /// Start an empty authenticated publication sequence.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            project_identity: None,
            root: None,
            entries: BTreeMap::new(),
        }
    }

    /// Authenticate an existing root once, then retain its exact inventory.
    pub fn open(
        lease: &GraphObjectPublicationLease,
        root: GraphFilesRootV2,
        limits: crate::GraphManifestLimits,
    ) -> Result<(Self, crate::GraphManifestResolveEvidence), GfError> {
        validate_publication_identity(lease)?;
        let (entries, evidence) = crate::resolve_graph_manifest(&root, limits, |digest| {
            read_graph_object_by_digest_from_cas(&lease.cas, digest, 64 * 1024 * 1024)
        })?;
        Ok((
            Self {
                project_identity: Some(lease.cas.project.identity()),
                root: Some(root),
                entries: entries
                    .into_iter()
                    .map(|entry| (entry.relative_path.clone(), entry))
                    .collect(),
            },
            evidence,
        ))
    }

    /// Current authenticated compact root, if one has been published.
    #[must_use]
    pub const fn root(&self) -> Option<&GraphFilesRootV2> {
        self.root.as_ref()
    }

    /// Current entries in canonical logical-path order.
    #[must_use]
    pub fn entries(&self) -> impl ExactSizeIterator<Item = &crate::GraphFileEntry> {
        self.entries.values()
    }
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
}

/// Trace compact generation roots, then sweep unreachable CAS objects.
/// Marking completes successfully before any deletion begins.
pub fn gc_graph_objects(
    root: &Path,
    roots: &[GraphFilesRootV2],
    limits: crate::GraphManifestLimits,
) -> Result<GraphObjectGcEvidence, GfError> {
    let guard = try_begin_graph_object_gc(root)?.ok_or_else(|| GfError::Project {
        code: ProjectErrorCode::WriterBusy,
        message: "phase=GRAPH_OBJECT_GC committed=false cause=live_publication".into(),
    })?;
    gc_graph_objects_guarded(&guard, roots, limits)
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

#[allow(clippy::too_many_lines)]
pub(crate) fn gc_graph_objects_guarded(
    guard: &GraphObjectGcGuard,
    roots: &[GraphFilesRootV2],
    limits: crate::GraphManifestLimits,
) -> Result<GraphObjectGcEvidence, GfError> {
    guard.cas.revalidate_named()?;
    let mut marked = BTreeSet::new();
    for graph_root in roots {
        let mut segment_digests = Vec::new();
        let (files, _) = crate::resolve_graph_manifest(graph_root, limits, |digest| {
            segment_digests.push(digest.to_owned());
            read_graph_object_by_digest_from_cas(&guard.cas, digest, 64 * 1024 * 1024)
        })?;
        marked.extend(segment_digests);
        marked.extend(files.into_iter().map(|entry| entry.content_sha256));
    }
    let mut candidates = Vec::new();
    for prefix in guard.cas.sha256.child_names().map_err(|error| {
        storage(
            "read stable graph object prefixes",
            &guard.cas.diagnostic_root,
            error,
        )
    })? {
        let prefix_text = prefix
            .to_str()
            .ok_or_else(|| validation("graph object prefix is not UTF-8"))?;
        if prefix_text.len() != 2
            || !prefix_text
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err(validation("graph object prefix is not canonical"));
        }
        let bucket = guard
            .cas
            .sha256
            .open_child_directory(&prefix)
            .map_err(|error| {
                storage(
                    "open stable graph object bucket",
                    &guard.cas.diagnostic_root,
                    error,
                )
            })?;
        for object in bucket.child_names().map_err(|error| {
            storage(
                "read stable graph object bucket",
                &guard.cas.diagnostic_root,
                error,
            )
        })? {
            let suffix = object
                .to_str()
                .ok_or_else(|| validation("graph object name is not UTF-8"))?;
            let digest = format!("{prefix_text}{suffix}");
            validate_digest(&digest)?;
            let file = bucket.open_child_file(&object).map_err(|error| {
                storage(
                    "open stable graph object candidate",
                    &guard.cas.diagnostic_root,
                    error,
                )
            })?;
            let metadata = file.metadata().map_err(|error| {
                storage(
                    "inspect stable graph object candidate",
                    &guard.cas.diagnostic_root,
                    error,
                )
            })?;
            if !metadata.is_file() {
                return Err(validation(
                    "graph object bucket contains a non-regular object",
                ));
            }
            if !marked.contains(&digest) {
                let identity = graphforge_filesystem::file_identity(&file).map_err(|error| {
                    storage(
                        "identify graph object candidate",
                        &guard.cas.diagnostic_root,
                        error,
                    )
                })?;
                candidates.push((prefix.clone(), object, identity, metadata.len()));
            }
        }
    }
    let mut evidence = GraphObjectGcEvidence {
        objects_marked: u64::try_from(marked.len()).unwrap_or(u64::MAX),
        ..GraphObjectGcEvidence::default()
    };
    for (prefix, object, identity, bytes) in candidates {
        let bucket = guard
            .cas
            .sha256
            .open_child_directory(&prefix)
            .map_err(|error| {
                storage(
                    "reopen stable graph object bucket",
                    &guard.cas.diagnostic_root,
                    error,
                )
            })?;
        bucket
            .unlink_child_if_identity(&object, identity)
            .map_err(|error| {
                storage(
                    "remove unreachable stable graph object",
                    &guard.cas.diagnostic_root,
                    error,
                )
            })?;
        evidence.objects_removed = evidence.objects_removed.saturating_add(1);
        evidence.bytes_removed = evidence.bytes_removed.saturating_add(bytes);
    }
    Ok(evidence)
}

/// Seal only changed logical files into a structurally shared radix manifest.
#[allow(clippy::too_many_lines)]
pub fn append_graph_files_v2(
    lease: &GraphObjectPublicationLease,
    workspace: &Path,
    state: &mut GraphManifestState,
    sealed_paths: &[PathBuf],
    tombstones: &[String],
) -> Result<(GraphFilesRootV2, GraphFilesAppendEvidence), GfError> {
    append_graph_files_v2_inner(lease, workspace, state, sealed_paths, None, tombstones)
}

/// Publish writer-authenticated files with one copy-and-hash authentication
/// pass. The expected digest is never trusted without that install-time pass.
pub(crate) fn append_authenticated_graph_files_v2(
    lease: &GraphObjectPublicationLease,
    workspace: &Path,
    state: &mut GraphManifestState,
    sealed_files: &[AuthenticatedGraphFile],
    tombstones: &[String],
) -> Result<(GraphFilesRootV2, GraphFilesAppendEvidence), GfError> {
    let paths = sealed_files
        .iter()
        .map(|file| file.relative_path.clone())
        .collect::<Vec<_>>();
    append_graph_files_v2_inner(
        lease,
        workspace,
        state,
        &paths,
        Some(sealed_files),
        tombstones,
    )
}

#[allow(clippy::too_many_lines)]
fn append_graph_files_v2_inner(
    lease: &GraphObjectPublicationLease,
    workspace: &Path,
    state: &mut GraphManifestState,
    sealed_paths: &[PathBuf],
    authenticated: Option<&[AuthenticatedGraphFile]>,
    tombstones: &[String],
) -> Result<(GraphFilesRootV2, GraphFilesAppendEvidence), GfError> {
    validate_publication_identity(lease)?;
    if state
        .project_identity
        .is_some_and(|identity| identity != lease.cas.project.identity())
    {
        return Err(validation(
            "graph manifest state belongs to a different project",
        ));
    }
    let mut additions = Vec::with_capacity(sealed_paths.len());
    let mut evidence = GraphFilesAppendEvidence::default();
    let mut sealed_names = BTreeSet::new();
    for relative in sealed_paths {
        validate_logical_path(relative)?;
        let name = relative
            .to_str()
            .ok_or_else(|| validation("sealed graph path is not UTF-8"))?;
        if !sealed_names.insert(name.to_owned()) {
            return Err(validation("sealed graph paths contain a duplicate"));
        }
    }
    let mut tombstone_names = BTreeSet::new();
    for path in tombstones {
        validate_logical_path(Path::new(path))?;
        if !tombstone_names.insert(path.clone()) {
            return Err(validation("graph tombstones contain a duplicate"));
        }
        if sealed_names.contains(path) {
            return Err(validation("graph path is both sealed and tombstoned"));
        }
    }
    let mut logical_byte_length = state
        .root
        .as_ref()
        .map_or(0, |root| root.logical_byte_length);
    for (index, relative) in sealed_paths.iter().enumerate() {
        let source = workspace.join(relative);
        let (digest, expected_length, prehashed_bytes) = if let Some(files) = authenticated {
            let expected = files
                .get(index)
                .ok_or_else(|| validation("authenticated graph inventory is incomplete"))?;
            if expected.relative_path != *relative {
                return Err(validation("authenticated graph file metadata changed"));
            }
            validate_digest(&expected.content_sha256)?;
            (expected.content_sha256.clone(), expected.byte_length, 0)
        } else {
            let metadata = fs::symlink_metadata(&source)
                .map_err(|error| storage("inspect sealed graph file", &source, error))?;
            if !metadata.is_file() || metadata.file_type().is_symlink() {
                return Err(validation("sealed graph path is not a regular file"));
            }
            (
                hex_digest(hash_regular_file(&source)?),
                metadata.len(),
                metadata.len(),
            )
        };
        let installed =
            install_graph_object_file_with_lease(lease, &source, &digest, expected_length)?;
        evidence.payload_bytes_hashed = evidence
            .payload_bytes_hashed
            .saturating_add(prehashed_bytes)
            .saturating_add(installed.bytes_hashed);
        evidence.bytes_installed = evidence
            .bytes_installed
            .saturating_add(installed.bytes_installed);
        evidence.read_calls = evidence.read_calls.saturating_add(installed.read_calls);
        evidence.write_calls = evidence.write_calls.saturating_add(installed.write_calls);
        evidence.write_bytes = evidence.write_bytes.saturating_add(installed.write_bytes);
        evidence.fsync_calls = evidence.fsync_calls.saturating_add(installed.fsync_calls);
        let relative_path = relative
            .to_str()
            .ok_or_else(|| validation("sealed graph path is not UTF-8"))?
            .to_owned();
        let entry = crate::GraphFileEntry {
            relative_path: relative_path.clone(),
            byte_length: expected_length,
            content_sha256: digest,
            role: crate::graph_files::infer_role(relative),
        };
        if let Some(previous) = state.entries.get(&relative_path) {
            logical_byte_length = logical_byte_length.saturating_sub(previous.byte_length);
        }
        logical_byte_length = logical_byte_length
            .checked_add(entry.byte_length)
            .ok_or_else(|| validation("graph files v2 byte total overflow"))?;
        additions.push(entry);
    }
    additions.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    let mut tombstones = tombstones.to_vec();
    tombstones.sort();
    for path in &tombstones {
        if let Some(previous) = state.entries.get(path) {
            logical_byte_length = logical_byte_length.saturating_sub(previous.byte_length);
        }
    }
    // All payload objects are authenticated before any manifest node can
    // reference them. A returned error here therefore leaves only unreferenced,
    // retry-safe CAS state.
    returned_error_boundary("append:before-manifest-reference")?;
    evidence.changed_entries_examined =
        u64::try_from(additions.len().saturating_add(tombstones.len())).unwrap_or(u64::MAX);
    let mut root_digest = match state.root.as_ref() {
        Some(previous) => previous.root_node_sha256.clone(),
        None => install_manifest_node(lease, &empty_branch(0), &mut evidence.bytes_installed)?,
    };
    for entry in &additions {
        let relative_path = entry.relative_path.clone();
        root_digest = update_manifest_path(
            lease,
            Some(&root_digest),
            0,
            &relative_path,
            Some(entry.clone()),
            &mut evidence.bytes_installed,
        )?
        .ok_or_else(|| validation("radix update unexpectedly removed the root"))?;
    }
    for path in &tombstones {
        root_digest = update_manifest_path(
            lease,
            Some(&root_digest),
            0,
            path,
            None,
            &mut evidence.bytes_installed,
        )?
        .unwrap_or(install_manifest_node(
            lease,
            &empty_branch(0),
            &mut evidence.bytes_installed,
        )?);
    }
    let added_new = additions
        .iter()
        .filter(|entry| !state.entries.contains_key(&entry.relative_path))
        .count();
    let removed_existing = tombstones
        .iter()
        .filter(|path| state.entries.contains_key(path.as_str()))
        .count();
    let logical_file_count = state
        .entries
        .len()
        .checked_add(added_new)
        .and_then(|count| count.checked_sub(removed_existing))
        .ok_or_else(|| validation("graph files v2 file total overflow"))?;
    let root = GraphFilesRootV2 {
        format: GRAPH_FILES_V2_FORMAT.into(),
        format_version: GRAPH_FILES_V2_VERSION,
        root_node_sha256: root_digest,
        logical_file_count: u64::try_from(logical_file_count).unwrap_or(u64::MAX),
        logical_byte_length,
    };
    // Commit the root-bound cache only after every payload and Patricia node
    // operation succeeded. An error above leaves both fields unchanged.
    for entry in additions {
        state.entries.insert(entry.relative_path.clone(), entry);
    }
    for path in tombstones {
        state.entries.remove(&path);
    }
    state.root = Some(root.clone());
    state.project_identity = Some(lease.cas.project.identity());
    Ok((root, evidence))
}

/// Import a verified v1 graph tree into a self-contained v2 radix root.
pub fn migrate_graph_files_v1_to_v2(
    lease: &GraphObjectPublicationLease,
    graph_root: &Path,
    inventory: &GraphFilesInventory,
) -> Result<(GraphFilesRootV2, GraphFilesMigrationEvidence), GfError> {
    // Authenticate the complete expanded contract before installing even one
    // payload object; malformed caller-owned structs cannot create partial CAS.
    crate::encode_inventory(inventory)?;
    validate_publication_identity(lease)?;
    let mut evidence = GraphFilesMigrationEvidence::default();
    for entry in &inventory.files {
        let source = crate::graph_files::resolve_v1_inventory_entry(graph_root, entry)?;
        let installed = install_graph_object_file_with_lease(
            lease,
            &source,
            &entry.content_sha256,
            entry.byte_length,
        )?;
        evidence.payload_objects = evidence.payload_objects.saturating_add(1);
        evidence.payload_bytes_hashed = evidence
            .payload_bytes_hashed
            .saturating_add(installed.bytes_hashed);
        evidence.bytes_installed = evidence
            .bytes_installed
            .saturating_add(installed.bytes_installed);
    }
    let mut root_digest =
        install_manifest_node(lease, &empty_branch(0), &mut evidence.bytes_installed)?;
    for entry in &inventory.files {
        let mut canonical_entry = entry.clone();
        canonical_entry.relative_path =
            crate::graph_files::canonical_inventory_relative_text(&entry.relative_path)?;
        let canonical_path = canonical_entry.relative_path.clone();
        root_digest = update_manifest_path(
            lease,
            Some(&root_digest),
            0,
            &canonical_path,
            Some(canonical_entry),
            &mut evidence.bytes_installed,
        )?
        .ok_or_else(|| validation("radix migration unexpectedly removed the root"))?;
    }
    Ok((
        GraphFilesRootV2 {
            format: GRAPH_FILES_V2_FORMAT.into(),
            format_version: GRAPH_FILES_V2_VERSION,
            root_node_sha256: root_digest,
            logical_file_count: inventory.file_count,
            logical_byte_length: inventory.total_byte_length,
        },
        evidence,
    ))
}

fn empty_branch(depth: u8) -> GraphManifestNode {
    GraphManifestNode {
        format: GRAPH_MANIFEST_NODE_FORMAT.into(),
        format_version: GRAPH_MANIFEST_NODE_VERSION,
        depth,
        prefix: String::new(),
        kind: GraphManifestNodeKind::Branch {
            children: BTreeMap::new(),
        },
    }
}

fn install_manifest_node(
    lease: &GraphObjectPublicationLease,
    node: &GraphManifestNode,
    bytes_installed: &mut u64,
) -> Result<String, GfError> {
    let bytes = crate::encode_graph_manifest_node(node)?;
    let (digest, evidence) = install_graph_object_bytes_with_lease(lease, &bytes)?;
    *bytes_installed = bytes_installed.saturating_add(evidence.bytes_installed);
    Ok(digest)
}

fn load_manifest_node(
    lease: &GraphObjectPublicationLease,
    digest: &str,
    expected_depth: u8,
) -> Result<GraphManifestNode, GfError> {
    let bytes = read_graph_object_by_digest_from_cas(&lease.cas, digest, 64 * 1024 * 1024)?;
    let node = crate::decode_graph_manifest_node(&bytes)?;
    if node.depth != expected_depth {
        return Err(validation(
            "graph manifest radix depth mismatch during update",
        ));
    }
    Ok(node)
}

fn update_manifest_path(
    lease: &GraphObjectPublicationLease,
    current_digest: Option<&str>,
    depth: u8,
    path: &str,
    replacement: Option<crate::GraphFileEntry>,
    bytes_installed: &mut u64,
) -> Result<Option<String>, GfError> {
    let path_digest = hex_digest(crate::graph_manifest::logical_path_digest(path));
    update_manifest_digest(
        lease,
        current_digest,
        depth,
        path,
        &path_digest,
        replacement,
        bytes_installed,
    )
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn update_manifest_digest(
    lease: &GraphObjectPublicationLease,
    current_digest: Option<&str>,
    depth: u8,
    path: &str,
    path_digest: &str,
    replacement: Option<crate::GraphFileEntry>,
    bytes_installed: &mut u64,
) -> Result<Option<String>, GfError> {
    let Some(current_digest) = current_digest else {
        return replacement
            .map(|entry| {
                install_manifest_node(
                    lease,
                    &leaf_node(
                        depth,
                        &path_digest[usize::from(depth)..],
                        path_digest,
                        vec![entry],
                    ),
                    bytes_installed,
                )
            })
            .transpose();
    };
    let mut node = load_manifest_node(lease, current_digest, depth)?;
    let start = usize::from(depth);
    let common = node
        .prefix
        .bytes()
        .zip(path_digest.as_bytes()[start..].iter().copied())
        .take_while(|(left, right)| left == right)
        .count();
    if common != node.prefix.len() {
        let Some(entry) = replacement else {
            return Ok(Some(current_digest.to_owned()));
        };
        let split_depth = depth
            .checked_add(u8::try_from(common).map_err(|_| validation("Patricia split overflow"))?)
            .ok_or_else(|| validation("Patricia split overflow"))?;
        let old_edge = node.prefix[common..=common].to_owned();
        node.depth = split_depth + 1;
        node.prefix = node.prefix[common + 1..].to_owned();
        let old_digest = install_manifest_node(lease, &node, bytes_installed)?;
        let new_edge = path_digest[usize::from(split_depth)..=usize::from(split_depth)].to_owned();
        if old_edge == new_edge {
            return Err(validation("Patricia split did not diverge"));
        }
        let new_digest = install_manifest_node(
            lease,
            &leaf_node(
                split_depth + 1,
                &path_digest[usize::from(split_depth) + 1..],
                path_digest,
                vec![entry],
            ),
            bytes_installed,
        )?;
        let children = BTreeMap::from([(old_edge, old_digest), (new_edge, new_digest)]);
        return install_manifest_node(
            lease,
            &branch_node(depth, &path_digest[start..start + common], children),
            bytes_installed,
        )
        .map(Some);
    }
    let payload_depth = depth
        .checked_add(
            u8::try_from(node.prefix.len()).map_err(|_| validation("Patricia depth overflow"))?,
        )
        .ok_or_else(|| validation("Patricia depth overflow"))?;
    match node.kind {
        GraphManifestNodeKind::Leaf {
            path_sha256,
            mut entries,
        } => {
            if path_sha256 != path_digest {
                return Err(validation("Patricia leaf route mismatch"));
            }
            match entries.binary_search_by(|entry| entry.relative_path.as_str().cmp(path)) {
                Ok(index) => match replacement {
                    Some(entry) => entries[index] = entry,
                    None => {
                        entries.remove(index);
                    }
                },
                Err(index) => {
                    if let Some(entry) = replacement {
                        entries.insert(index, entry);
                    } else {
                        return Ok(Some(current_digest.to_owned()));
                    }
                }
            }
            if entries.is_empty() {
                Ok(None)
            } else {
                install_manifest_node(
                    lease,
                    &leaf_node(depth, &node.prefix, path_digest, entries),
                    bytes_installed,
                )
                .map(Some)
            }
        }
        GraphManifestNodeKind::Branch { mut children } => {
            if payload_depth >= GRAPH_RADIX_DEPTH {
                return Err(validation("Patricia branch exceeds digest route"));
            }
            let edge =
                path_digest[usize::from(payload_depth)..=usize::from(payload_depth)].to_owned();
            let child = update_manifest_digest(
                lease,
                children.get(&edge).map(String::as_str),
                payload_depth + 1,
                path,
                path_digest,
                replacement,
                bytes_installed,
            )?;
            match child {
                Some(digest) => {
                    children.insert(edge, digest);
                }
                None => {
                    children.remove(&edge);
                }
            }
            match children.len() {
                0 => Ok(None),
                1 => {
                    let (edge, child_digest) = children.into_iter().next().expect("one child");
                    let mut child = load_manifest_node(lease, &child_digest, payload_depth + 1)?;
                    child.depth = depth;
                    child.prefix = format!("{}{}{}", node.prefix, edge, child.prefix);
                    install_manifest_node(lease, &child, bytes_installed).map(Some)
                }
                _ => install_manifest_node(
                    lease,
                    &branch_node(depth, &node.prefix, children),
                    bytes_installed,
                )
                .map(Some),
            }
        }
    }
}

fn branch_node(depth: u8, prefix: &str, children: BTreeMap<String, String>) -> GraphManifestNode {
    GraphManifestNode {
        format: GRAPH_MANIFEST_NODE_FORMAT.into(),
        format_version: GRAPH_MANIFEST_NODE_VERSION,
        depth,
        prefix: prefix.to_owned(),
        kind: GraphManifestNodeKind::Branch { children },
    }
}

fn leaf_node(
    depth: u8,
    prefix: &str,
    path_sha256: &str,
    entries: Vec<crate::GraphFileEntry>,
) -> GraphManifestNode {
    GraphManifestNode {
        format: GRAPH_MANIFEST_NODE_FORMAT.into(),
        format_version: GRAPH_MANIFEST_NODE_VERSION,
        depth,
        prefix: prefix.to_owned(),
        kind: GraphManifestNodeKind::Leaf {
            path_sha256: path_sha256.to_owned(),
            entries,
        },
    }
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

/// Install exact in-memory bytes under their SHA-256 identity.
pub fn install_graph_object_bytes(
    root: &Path,
    bytes: &[u8],
) -> Result<(String, GraphObjectInstallEvidence), GfError> {
    let lease = begin_graph_object_publication(root)?;
    install_graph_object_bytes_with_lease(&lease, bytes)
}

fn install_graph_object_bytes_with_lease(
    lease: &GraphObjectPublicationLease,
    bytes: &[u8],
) -> Result<(String, GraphObjectInstallEvidence), GfError> {
    let digest = hex_digest(Sha256::digest(bytes).into());
    let expected_length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    install_object(&lease.cas, &digest, expected_length, false, |file| {
        file.write_all(bytes).map_err(|error| {
            storage(
                "write temporary graph object",
                &lease.cas.diagnostic_root,
                error,
            )
        })?;
        file.sync_all().map_err(|error| {
            storage(
                "fsync temporary graph object",
                &lease.cas.diagnostic_root,
                error,
            )
        })?;
        // The source is already resident memory; only the mandatory temporary
        // file verification below is an application-observed payload read.
        Ok(0)
    })
    .map(|mut evidence| {
        if !evidence.reused_existing {
            evidence.write_bytes = expected_length;
            evidence.write_calls = u64::from(!bytes.is_empty());
            evidence.fsync_calls = evidence.fsync_calls.saturating_add(1);
        }
        (digest, evidence)
    })
}

/// Stream, hash, and install a new payload object from a regular source file.
pub fn install_graph_object_file(
    root: &Path,
    source: &Path,
    expected_digest: &str,
    expected_length: u64,
) -> Result<GraphObjectInstallEvidence, GfError> {
    let lease = begin_graph_object_publication(root)?;
    install_graph_object_file_with_lease(&lease, source, expected_digest, expected_length)
}

fn install_graph_object_file_with_lease(
    lease: &GraphObjectPublicationLease,
    source: &Path,
    expected_digest: &str,
    expected_length: u64,
) -> Result<GraphObjectInstallEvidence, GfError> {
    validate_digest(expected_digest)?;
    let metadata = fs::symlink_metadata(source)
        .map_err(|error| storage("inspect graph object source", source, error))?;
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() != expected_length
    {
        return Err(validation(
            "graph object source is not the declared regular file",
        ));
    }
    let read_calls = std::cell::Cell::new(0_u64);
    let write_calls = std::cell::Cell::new(0_u64);
    let result = install_object(
        &lease.cas,
        expected_digest,
        expected_length,
        true,
        |output| {
            let mut input = File::open(source)
                .map_err(|error| storage("open graph object source", source, error))?;
            let mut hasher = Sha256::new();
            let mut total = 0_u64;
            let mut buffer = vec![0_u8; BUFFER_BYTES];
            loop {
                let read = input
                    .read(&mut buffer)
                    .map_err(|error| storage("read graph object source", source, error))?;
                if read == 0 {
                    break;
                }
                read_calls.set(read_calls.get().saturating_add(1));
                output.write_all(&buffer[..read]).map_err(|error| {
                    storage(
                        "write temporary graph object",
                        &lease.cas.diagnostic_root,
                        error,
                    )
                })?;
                write_calls.set(write_calls.get().saturating_add(1));
                hasher.update(&buffer[..read]);
                total = total.saturating_add(u64::try_from(read).unwrap_or(u64::MAX));
            }
            if total != expected_length || hex_digest(hasher.finalize().into()) != expected_digest {
                return Err(validation(
                    "graph object source digest or length changed during install",
                ));
            }
            output.sync_all().map_err(|error| {
                storage(
                    "fsync temporary graph object",
                    &lease.cas.diagnostic_root,
                    error,
                )
            })?;
            Ok(total)
        },
    );
    result.map(|mut evidence| {
        if !evidence.reused_existing {
            evidence.read_calls = evidence.read_calls.saturating_add(read_calls.get());
            evidence.write_calls = evidence.write_calls.saturating_add(write_calls.get());
            evidence.write_bytes = evidence.write_bytes.saturating_add(expected_length);
            evidence.fsync_calls = evidence.fsync_calls.saturating_add(1);
        }
        evidence
    })
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

/// Materialize a verified logical inventory into a private graph tree. Ordinary
/// immutable payloads reuse CAS inodes; the v4 ordinal authority facet is
/// copied into single-link files because its reader intentionally rejects
/// shared inodes. The target must be empty.
pub fn materialize_graph_objects(
    root: &Path,
    inventory: &GraphFilesInventory,
    target: &Path,
) -> Result<GraphFilesOpenEvidence, GfError> {
    let lease = begin_graph_object_publication(root)?;
    let _target_guard = open_empty_materialization_target(target)?;
    let target_directory = StableDirectory::open(target)
        .map_err(|error| storage("retain stable materialization target", target, error))?;
    let mut evidence = GraphFilesOpenEvidence {
        strategy: GraphFilesOpenStrategy::PrivateMaterialize,
        files_validated: inventory.file_count,
        bytes_validated: inventory.total_byte_length,
        ..GraphFilesOpenEvidence::default()
    };
    for entry in &inventory.files {
        let materialized = materialize_from_cas(&lease.cas, &target_directory, entry)?;
        evidence.application_read_bytes = evidence
            .application_read_bytes
            .saturating_add(materialized.read_bytes);
        evidence.application_read_calls = evidence
            .application_read_calls
            .saturating_add(materialized.read_calls);
        evidence.application_write_bytes = evidence
            .application_write_bytes
            .saturating_add(materialized.write_bytes);
        evidence.application_write_calls = evidence
            .application_write_calls
            .saturating_add(materialized.write_calls);
        evidence.fsync_calls = evidence
            .fsync_calls
            .saturating_add(materialized.fsync_calls);
        if materialized.copied {
            evidence.files_copied = evidence.files_copied.saturating_add(1);
            evidence.bytes_copied = evidence.bytes_copied.saturating_add(entry.byte_length);
        } else {
            evidence.files_reused = evidence.files_reused.saturating_add(1);
            evidence.bytes_reused = evidence.bytes_reused.saturating_add(entry.byte_length);
        }
    }
    lease.revalidate_for_publish()?;
    Ok(evidence)
}

fn materialize_from_cas(
    cas: &CasRoot,
    target: &StableDirectory,
    entry: &crate::GraphFileEntry,
) -> Result<MaterializeIoEvidence, GfError> {
    validate_logical_path(Path::new(&entry.relative_path))?;
    let bucket = cas.digest_bucket(&entry.content_sha256, false)?;
    let source_name = std::ffi::OsStr::new(&entry.content_sha256[2..]);
    let source = bucket
        .open_child_file(source_name)
        .map_err(|error| storage("open materialization source", &cas.diagnostic_root, error))?;
    let source_identity = graphforge_filesystem::file_identity(&source).map_err(|error| {
        storage(
            "identify materialization source",
            &cas.diagnostic_root,
            error,
        )
    })?;
    let path = Path::new(&entry.relative_path);
    let mut parent: Option<StableDirectory> = None;
    let mut components = path.components().peekable();
    while let Some(component) = components.next() {
        let Component::Normal(name) = component else {
            return Err(validation("invalid materialization path component"));
        };
        if components.peek().is_some() {
            let directory = parent.as_ref().unwrap_or(target);
            parent = Some(directory.create_child_directory(name).map_err(|error| {
                storage(
                    "create stable materialization directory",
                    &cas.diagnostic_root,
                    error,
                )
            })?);
        } else {
            let parent = parent.as_ref().unwrap_or(target);
            if requires_single_link_materialization(&entry.relative_path) {
                return copy_single_link_materialized_object(cas, &source, parent, name, entry);
            }
            let (installed, installed_identity) = bucket
                .link_child_into(source_name, &source, source_identity, parent, name)
                .map_err(|error| {
                    storage(
                        "install stable materialized object",
                        &cas.diagnostic_root,
                        error,
                    )
                })?;
            let verified = verify_file_counted(
                installed,
                &entry.content_sha256,
                entry.byte_length,
                &cas.diagnostic_root,
            );
            match verified {
                Ok(io) => {
                    return Ok(MaterializeIoEvidence {
                        read_bytes: io.bytes,
                        read_calls: io.calls,
                        ..MaterializeIoEvidence::default()
                    });
                }
                Err(error) => {
                    let _ = parent.unlink_child_if_identity(name, installed_identity);
                    return Err(error);
                }
            }
        }
    }
    Err(validation("materialization path has no final component"))
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct MaterializeIoEvidence {
    copied: bool,
    read_bytes: u64,
    read_calls: u64,
    write_bytes: u64,
    write_calls: u64,
    fsync_calls: u64,
}

fn requires_single_link_materialization(relative_path: &str) -> bool {
    let Some(name) = relative_path.strip_prefix("topology/uuid-membership/") else {
        return false;
    };
    !name.contains('/')
        && (matches!(
            name,
            "ordinal-v4-manifest.json" | "ordinal-v4-receipt.json" | "ordinal-v4.lock"
        ) || (!name.starts_with(".v4-")
            && crate::uuid_membership::is_exact_private_v4_name(name)))
}

fn copy_single_link_materialized_object(
    cas: &CasRoot,
    source: &File,
    parent: &StableDirectory,
    name: &std::ffi::OsStr,
    entry: &crate::GraphFileEntry,
) -> Result<MaterializeIoEvidence, GfError> {
    let temporary_name = std::ffi::OsString::from(format!(
        ".ordinal-v4-materialize-{}.tmp",
        Uuid::new_v4().simple()
    ));
    let mut input = source
        .try_clone()
        .map_err(|error| storage("clone materialization source", &cas.diagnostic_root, error))?;
    input
        .rewind()
        .map_err(|error| storage("rewind materialization source", &cas.diagnostic_root, error))?;
    let mut output = parent
        .create_replaceable_child_file(&temporary_name)
        .map_err(|error| {
            storage(
                "create private materialization file",
                &cas.diagnostic_root,
                error,
            )
        })?;
    let output_identity = graphforge_filesystem::file_identity(&output).map_err(|error| {
        storage(
            "identify private materialization file",
            &cas.diagnostic_root,
            error,
        )
    })?;
    let mut installed = false;
    let result = (|| -> Result<MaterializeIoEvidence, GfError> {
        let mut io = copy_and_authenticate_materialized_object(
            &mut input,
            &mut output,
            entry,
            &cas.diagnostic_root,
        )?;
        drop(output);
        parent
            .replace_child(&temporary_name, output_identity, name)
            .map_err(|error| {
                storage(
                    "install private materialization file",
                    &cas.diagnostic_root,
                    error,
                )
            })?;
        installed = true;
        parent.sync().map_err(|error| {
            storage(
                "sync private materialization directory",
                &cas.diagnostic_root,
                error,
            )
        })?;
        io.fsync_calls = io.fsync_calls.saturating_add(1);
        let installed = parent.open_child_file(name).map_err(|error| {
            storage(
                "open private materialization file",
                &cas.diagnostic_root,
                error,
            )
        })?;
        if graphforge_filesystem::file_link_count(&installed).map_err(|error| {
            storage(
                "inspect private materialization links",
                &cas.diagnostic_root,
                error,
            )
        })? != 1
        {
            return Err(validation(
                "private materialization file is multiply linked",
            ));
        }
        let verified = verify_file_counted(
            installed,
            &entry.content_sha256,
            entry.byte_length,
            &cas.diagnostic_root,
        )?;
        io.read_bytes = io.read_bytes.saturating_add(verified.bytes);
        io.read_calls = io.read_calls.saturating_add(verified.calls);
        io.copied = true;
        Ok(io)
    })();
    if result.is_err() {
        let cleanup_name = if installed { name } else { &temporary_name };
        let _ = parent.unlink_child_if_identity(cleanup_name, output_identity);
        let _ = parent.sync();
    }
    result
}

fn copy_and_authenticate_materialized_object(
    input: &mut File,
    output: &mut File,
    entry: &crate::GraphFileEntry,
    diagnostic_root: &Path,
) -> Result<MaterializeIoEvidence, GfError> {
    let mut digest = Sha256::new();
    let mut length = 0_u64;
    let mut read_calls = 0_u64;
    let mut write_calls = 0_u64;
    let mut buffer = vec![0_u8; BUFFER_BYTES].into_boxed_slice();
    loop {
        let read = input
            .read(&mut buffer)
            .map_err(|error| storage("read materialization source", diagnostic_root, error))?;
        if read == 0 {
            break;
        }
        read_calls = read_calls.saturating_add(1);
        output.write_all(&buffer[..read]).map_err(|error| {
            storage("write private materialization file", diagnostic_root, error)
        })?;
        write_calls = write_calls.saturating_add(1);
        digest.update(&buffer[..read]);
        length = length
            .checked_add(read as u64)
            .ok_or_else(|| validation("private materialization length overflows"))?;
    }
    if length != entry.byte_length || hex_digest(digest.finalize().into()) != entry.content_sha256 {
        return Err(validation(
            "private materialization bytes do not match inventory",
        ));
    }
    output
        .sync_all()
        .map_err(|error| storage("sync private materialization file", diagnostic_root, error))?;
    Ok(MaterializeIoEvidence {
        copied: true,
        read_bytes: length,
        read_calls,
        write_bytes: length,
        write_calls,
        fsync_calls: 1,
    })
}

#[cfg(unix)]
fn open_empty_materialization_target(target: &Path) -> Result<std::os::fd::OwnedFd, GfError> {
    use rustix::fs::{Mode, OFlags};

    let parent = target
        .parent()
        .ok_or_else(|| validation("graph object target has no parent"))?;
    let name = target
        .file_name()
        .ok_or_else(|| validation("graph object target has no final component"))?;
    let canonical_parent = parent
        .canonicalize()
        .map_err(|error| storage("resolve graph object target parent", parent, error))?;
    let parent_fd = open_directory_no_follow(&canonical_parent)?;
    match rustix::fs::mkdirat(&parent_fd, name, Mode::from_bits_truncate(0o700)) {
        Ok(()) | Err(rustix::io::Errno::EXIST) => {}
        Err(error) => return Err(storage("create graph object target", target, error)),
    }
    let directory = rustix::fs::openat(
        &parent_fd,
        name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|error| {
        storage(
            "open graph object target without following links",
            target,
            error,
        )
    })?;
    if target
        .read_dir()
        .map_err(|error| storage("read graph object target", target, error))?
        .next()
        .is_some()
    {
        return Err(validation(
            "graph object materialization target is not empty",
        ));
    }
    Ok(directory)
}

#[cfg(unix)]
fn open_directory_no_follow(path: &Path) -> Result<std::os::fd::OwnedFd, GfError> {
    use rustix::fs::{Mode, OFlags};

    let mut directory = rustix::fs::open(
        if path.is_absolute() {
            Path::new("/")
        } else {
            Path::new(".")
        },
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|error| storage("open graph object directory root", path, error))?;
    for component in path.components() {
        let Component::Normal(name) = component else {
            continue;
        };
        directory = rustix::fs::openat(
            &directory,
            name,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|error| {
            storage(
                "open graph object directory without following links",
                path,
                error,
            )
        })?;
    }
    Ok(directory)
}

#[cfg(unix)]
#[allow(dead_code)]
fn link_materialized_object(
    target: &std::os::fd::OwnedFd,
    source: &Path,
    relative: &str,
    expected_digest: &str,
    expected_length: u64,
) -> Result<(), GfError> {
    use rustix::fs::{AtFlags, Mode, OFlags};

    let path = Path::new(relative);
    validate_logical_path(path)?;
    let mut directory = target
        .try_clone()
        .map_err(|error| storage("clone graph object target", path, error))?;
    let mut components = path
        .components()
        .filter_map(|component| match component {
            Component::Normal(name) => Some(name),
            _ => None,
        })
        .peekable();
    while let Some(component) = components.next() {
        if components.peek().is_none() {
            rustix::fs::linkat(
                rustix::fs::CWD,
                source,
                &directory,
                component,
                AtFlags::empty(),
            )
            .map_err(|error| storage("link logical graph object", path, error))?;
            let verified = (|| {
                let linked = rustix::fs::openat(
                    &directory,
                    component,
                    OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
                    Mode::empty(),
                )
                .map_err(|error| {
                    storage(
                        "open materialized object without following links",
                        path,
                        error,
                    )
                })?;
                let mut file: File = linked.into();
                let metadata = file
                    .metadata()
                    .map_err(|error| storage("inspect materialized object", path, error))?;
                if !metadata.is_file() || metadata.len() != expected_length {
                    return Err(validation("materialized graph object identity is invalid"));
                }
                let mut hasher = Sha256::new();
                let mut buffer = vec![0_u8; BUFFER_BYTES];
                loop {
                    let read = file
                        .read(&mut buffer)
                        .map_err(|error| storage("verify materialized object", path, error))?;
                    if read == 0 {
                        break;
                    }
                    hasher.update(&buffer[..read]);
                }
                if hex_digest(hasher.finalize().into()) != expected_digest {
                    return Err(validation("materialized graph object digest mismatch"));
                }
                Ok(())
            })();
            if verified.is_err() {
                let _ = rustix::fs::unlinkat(&directory, component, AtFlags::empty());
            }
            return verified;
        }
        match rustix::fs::mkdirat(&directory, component, Mode::from_bits_truncate(0o700)) {
            Ok(()) | Err(rustix::io::Errno::EXIST) => {}
            Err(error) => {
                return Err(storage(
                    "create logical graph object directory",
                    path,
                    error,
                ));
            }
        }
        directory = rustix::fs::openat(
            &directory,
            component,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|error| {
            storage(
                "open logical graph object directory without following links",
                path,
                error,
            )
        })?;
    }
    Err(validation("graph object logical path is empty"))
}

#[cfg(windows)]
struct WindowsMaterializationTarget {
    path: PathBuf,
    _guards: Vec<File>,
}

#[cfg(windows)]
fn open_empty_materialization_target(
    target: &Path,
) -> Result<WindowsMaterializationTarget, GfError> {
    let parent = target
        .parent()
        .ok_or_else(|| validation("graph object target has no parent"))?;
    let mut guards = windows_directory_guards(parent)?;
    match fs::create_dir(target) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => {
            return Err(storage(
                "create graph object materialization target",
                target,
                error,
            ));
        }
    }
    let target_handle = crate::filesystem_admission::open_directory_handle(target)
        .map_err(|error| storage("open graph object materialization target", target, error))?;
    validate_directory_identity(&target_handle, target)?;
    guards.push(target_handle);
    if target
        .read_dir()
        .map_err(|error| storage("read graph object target", target, error))?
        .next()
        .is_some()
    {
        return Err(validation(
            "graph object materialization target is not empty",
        ));
    }
    Ok(WindowsMaterializationTarget {
        path: target.to_path_buf(),
        _guards: guards,
    })
}

#[cfg(windows)]
fn windows_directory_guards(path: &Path) -> Result<Vec<File>, GfError> {
    use std::os::windows::fs::MetadataExt as _;
    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
    let mut paths = path.ancestors().collect::<Vec<_>>();
    paths.reverse();
    let mut guards = Vec::with_capacity(paths.len());
    for component in paths {
        let metadata = fs::symlink_metadata(component)
            .map_err(|error| storage("inspect materialization directory", component, error))?;
        if !metadata.is_dir() || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(validation("materialization path contains a reparse point"));
        }
        let guard = crate::filesystem_admission::open_directory_handle(component)
            .map_err(|error| storage("retain materialization directory", component, error))?;
        validate_directory_identity(&guard, component)?;
        guards.push(guard);
    }
    Ok(guards)
}

#[cfg(windows)]
fn windows_create_guarded_directories(
    root: &Path,
    relative_parent: &Path,
) -> Result<Vec<File>, GfError> {
    let mut path = root.to_path_buf();
    let mut guards = Vec::new();
    for component in relative_parent.components() {
        let Component::Normal(name) = component else {
            return Err(validation("invalid materialization directory component"));
        };
        path.push(name);
        match fs::create_dir(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(storage("create materialization directory", &path, error)),
        }
        let guard = crate::filesystem_admission::open_directory_handle(&path)
            .map_err(|error| storage("retain materialization directory", &path, error))?;
        validate_directory_identity(&guard, &path)?;
        guards.push(guard);
    }
    Ok(guards)
}

#[cfg(all(not(unix), not(windows)))]
fn open_empty_materialization_target(target: &Path) -> Result<PathBuf, GfError> {
    let _ = target;
    Err(validation(
        "graph object materialization is unsupported on this platform",
    ))
}

#[cfg(windows)]
#[allow(dead_code)]
fn link_materialized_object(
    target: &WindowsMaterializationTarget,
    source: &Path,
    relative: &str,
    expected_digest: &str,
    expected_length: u64,
) -> Result<(), GfError> {
    use std::os::windows::fs::OpenOptionsExt as _;
    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_SHARE_WRITE: u32 = 0x0000_0002;
    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    let destination = target.path.join(relative);
    let relative_parent = Path::new(relative)
        .parent()
        .ok_or_else(|| validation("materialized object has no parent"))?;
    let _guards = windows_create_guarded_directories(&target.path, relative_parent)?;
    let source_file = OpenOptions::new()
        .read(true)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
        .open(source)
        .map_err(|error| {
            storage(
                "open graph object source without following reparse points",
                source,
                error,
            )
        })?;
    let source_identity = graphforge_filesystem::file_identity(&source_file)
        .map_err(|error| storage("inspect graph object source identity", source, error))?;
    if source_identity
        != graphforge_filesystem::path_identity(source)
            .map_err(|error| storage("inspect graph object source identity", source, error))?
    {
        return Err(validation("graph object source identity changed"));
    }
    fs::hard_link(source, &destination)
        .map_err(|error| storage("link logical graph object", &destination, error))?;
    let verified = (|| {
        let mut linked = OpenOptions::new()
            .read(true)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
            .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
            .open(&destination)
            .map_err(|error| storage("open materialized object", &destination, error))?;
        let metadata = linked
            .metadata()
            .map_err(|error| storage("inspect materialized object", &destination, error))?;
        if !metadata.is_file()
            || metadata.file_type().is_symlink()
            || metadata.len() != expected_length
        {
            return Err(validation("materialized graph object identity is invalid"));
        }
        let linked_identity = graphforge_filesystem::file_identity(&linked).map_err(|error| {
            storage("inspect materialized object identity", &destination, error)
        })?;
        if linked_identity != source_identity
            || linked_identity
                != graphforge_filesystem::path_identity(&destination).map_err(|error| {
                    storage("inspect materialized object identity", &destination, error)
                })?
            || source_identity
                != graphforge_filesystem::path_identity(source).map_err(|error| {
                    storage("revalidate graph object source identity", source, error)
                })?
        {
            return Err(validation("materialized graph object identity changed"));
        }
        let mut hasher = Sha256::new();
        let mut buffer = vec![0_u8; BUFFER_BYTES];
        loop {
            let read = linked
                .read(&mut buffer)
                .map_err(|error| storage("verify materialized object", &destination, error))?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
        (hex_digest(hasher.finalize().into()) == expected_digest)
            .then_some(())
            .ok_or_else(|| validation("materialized graph object digest mismatch"))
    })();
    if let Err(error) = verified {
        let _ = fs::remove_file(&destination);
        return Err(error);
    }
    Ok(())
}

#[cfg(all(not(unix), not(windows)))]
#[allow(dead_code)]
fn link_materialized_object(
    _target: &PathBuf,
    _source: &Path,
    _relative: &str,
    _expected_digest: &str,
    _expected_length: u64,
) -> Result<(), GfError> {
    Err(validation(
        "graph object materialization is unsupported on this platform",
    ))
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
pub(crate) struct AuthenticatedGraphObject {
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
    pub(crate) fn open(
        &self,
        digest: &str,
        expected_length: u64,
    ) -> Result<AuthenticatedGraphObject, GfError> {
        open_graph_object_with_read_lease(self, digest, expected_length)
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

pub(crate) fn open_graph_object_by_digest(
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
        hasher.update(&block[..count]);
    }
    if hex_digest(hasher.finalize().into()) != digest {
        return Err(validation("graph object digest does not match its address"));
    }
    file.rewind()
        .map_err(|error| storage("rewind graph object", &lease.cas.diagnostic_root, error))?;
    Ok(AuthenticatedGraphObject {
        file,
        authenticated_length: expected_length,
        _cas: std::sync::Arc::clone(&lease.cas),
    })
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
    mut file: File,
    digest: &str,
    max_length: u64,
    diagnostic_root: &Path,
) -> Result<Vec<u8>, GfError> {
    let metadata = file
        .metadata()
        .map_err(|error| storage("inspect stable graph object", diagnostic_root, error))?;
    if !metadata.is_file() || metadata.len() > max_length {
        return Err(validation(
            "graph object exceeds admitted length or is not regular",
        ));
    }
    let mut bytes = Vec::with_capacity(usize::try_from(metadata.len()).unwrap_or(0));
    file.read_to_end(&mut bytes)
        .map_err(|error| storage("read stable graph object", diagnostic_root, error))?;
    if hex_digest(Sha256::digest(&bytes).into()) != digest {
        return Err(validation("graph object digest does not match its address"));
    }
    Ok(bytes)
}

pub(crate) fn read_graph_object_by_digest_with_lease(
    lease: &GraphObjectPublicationLease,
    digest: &str,
    max_length: u64,
) -> Result<Vec<u8>, GfError> {
    lease.cas.revalidate_named()?;
    read_graph_object_by_digest_from_cas(&lease.cas, digest, max_length)
}

fn install_object<F>(
    cas: &CasRoot,
    digest: &str,
    expected_length: u64,
    writer_authenticated: bool,
    write_temporary: F,
) -> Result<GraphObjectInstallEvidence, GfError>
where
    F: FnOnce(&mut CasTemporaryWriter) -> Result<u64, GfError>,
{
    validate_digest(digest)?;
    let bucket = cas.digest_bucket(digest, true)?;
    let destination_name = std::ffi::OsStr::new(&digest[2..]);
    if let Some(evidence) =
        try_reuse_existing_object(cas, &bucket, destination_name, digest, expected_length)?
    {
        return Ok(evidence);
    }
    let temporary_name = std::ffi::OsString::from(Uuid::new_v4().hyphenated().to_string());
    #[cfg(unix)]
    let temporary = cas.tmp.create_child_file(&temporary_name);
    #[cfg(windows)]
    let temporary = cas.tmp.create_cas_child_file(&temporary_name);
    let mut temporary = temporary.map_err(|error| {
        storage(
            "create stable temporary graph object",
            &cas.diagnostic_root,
            error,
        )
    })?;
    #[cfg(unix)]
    let temporary_identity = graphforge_filesystem::file_identity(&temporary).map_err(|error| {
        storage(
            "inspect temporary graph object",
            &cas.diagnostic_root,
            error,
        )
    })?;
    #[cfg(windows)]
    let temporary_identity = temporary.identity();
    let bytes_hashed = write_temporary(&mut temporary)?;
    let preseal_io = if writer_authenticated || cfg!(windows) {
        ReadIoEvidence::default()
    } else {
        temporary.rewind().map_err(|error| {
            storage("rewind temporary graph object", &cas.diagnostic_root, error)
        })?;
        verify_stream_counted(
            &mut temporary,
            digest,
            expected_length,
            &cas.diagnostic_root,
        )?
    };
    // Windows must close the writable handle and reopen an exact-identity,
    // protected read handle before publication. That transition authenticates
    // the complete payload below, so a second pre-seal read would be redundant.
    let (installed, sealed_bytes_hashed, concurrent_io) = finalize_temporary_object(
        cas,
        &bucket,
        TemporaryObject {
            name: temporary_name,
            file: temporary,
            identity: temporary_identity,
        },
        digest,
        expected_length,
    )?;
    Ok(GraphObjectInstallEvidence {
        bytes_hashed: bytes_hashed
            .saturating_add(preseal_io.bytes)
            .saturating_add(sealed_bytes_hashed)
            .saturating_add(if installed { 0 } else { expected_length }),
        bytes_installed: if installed { expected_length } else { 0 },
        reused_existing: !installed,
        read_calls: preseal_io.calls.saturating_add(concurrent_io.calls),
        // The source-copy/authentication submissions are added by the caller.
        // Fresh installation always durably synchronizes the destination bucket
        // and temporary namespace; reuse performs neither operation here.
        fsync_calls: if installed { 2 } else { 0 },
        ..GraphObjectInstallEvidence::default()
    })
}

fn reused_object_evidence(expected_length: u64, io: ReadIoEvidence) -> GraphObjectInstallEvidence {
    debug_assert_eq!(io.bytes, expected_length);
    GraphObjectInstallEvidence {
        bytes_hashed: io.bytes,
        reused_existing: true,
        read_calls: io.calls,
        ..GraphObjectInstallEvidence::default()
    }
}

#[cfg(unix)]
fn try_reuse_existing_object(
    cas: &CasRoot,
    bucket: &StableDirectory,
    destination_name: &std::ffi::OsStr,
    digest: &str,
    expected_length: u64,
) -> Result<Option<GraphObjectInstallEvidence>, GfError> {
    let file = match bucket.open_child_file(destination_name) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(storage(
                "open existing graph object",
                &cas.diagnostic_root,
                error,
            ));
        }
    };
    let io = verify_and_seal_graph_object_counted(
        &file,
        digest,
        expected_length,
        &graph_object_path(&cas.diagnostic_root, digest)?,
        &cas.diagnostic_root,
    )?;
    Ok(Some(reused_object_evidence(expected_length, io)))
}

#[cfg(windows)]
fn try_reuse_existing_object(
    cas: &CasRoot,
    bucket: &StableDirectory,
    destination_name: &std::ffi::OsStr,
    digest: &str,
    expected_length: u64,
) -> Result<Option<GraphObjectInstallEvidence>, GfError> {
    let mut adoption_io = ReadIoEvidence::default();
    let file = match bucket.open_cas_child_file(destination_name) {
        Ok(file) => file.into_file(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(canonical_error) => {
            let mut legacy = bucket
                .open_legacy_cas_child_for_adoption(destination_name)
                .map_err(|legacy_error| {
                    storage(
                        "reject unsealed or busy graph object",
                        &cas.diagnostic_root,
                        if legacy_error.kind() == std::io::ErrorKind::NotFound {
                            canonical_error
                        } else {
                            legacy_error
                        },
                    )
                })?;
            adoption_io = verify_stream_counted(
                &mut legacy,
                digest,
                expected_length,
                &cas.diagnostic_root,
            )?;
            bucket
                .adopt_legacy_cas_child(destination_name, legacy)
                .map(graphforge_filesystem::WindowsSealedCasFile::into_file)
                .map_err(|error| {
                    storage(
                        "adopt authenticated legacy graph object",
                        &cas.diagnostic_root,
                        error,
                    )
                })?
        }
    };
    let io = verify_and_seal_graph_object_counted(
        &file,
        digest,
        expected_length,
        &graph_object_path(&cas.diagnostic_root, digest)?,
        &cas.diagnostic_root,
    )?;
    let total = ReadIoEvidence {
        bytes: adoption_io.bytes.saturating_add(io.bytes),
        calls: adoption_io.calls.saturating_add(io.calls),
    };
    let mut evidence = reused_object_evidence(expected_length, total);
    evidence.bytes_hashed = total.bytes;
    Ok(Some(evidence))
}

fn finalize_temporary_object(
    cas: &CasRoot,
    bucket: &StableDirectory,
    temporary: TemporaryObject,
    digest: &str,
    expected_length: u64,
) -> Result<(bool, u64, ReadIoEvidence), GfError> {
    let destination_name = std::ffi::OsStr::new(&digest[2..]);
    let temporary_path = cas
        .diagnostic_root
        .join(GRAPH_OBJECTS_DIR)
        .join(TEMP_DIR)
        .join(&temporary.name);
    #[cfg(unix)]
    let (temporary, sealed_io) = {
        seal_graph_object(&temporary.file, &temporary_path, &cas.diagnostic_root)?;
        (
            SealedTemporaryObject {
                name: temporary.name,
                file: temporary.file,
                identity: temporary.identity,
            },
            ReadIoEvidence::default(),
        )
    };
    #[cfg(windows)]
    let (temporary, sealed_io) = transition_temporary_to_sealed_reader(
        &cas.tmp,
        temporary,
        digest,
        expected_length,
        &cas.diagnostic_root,
    )?;
    let sealed_bytes_hashed = sealed_io.bytes;
    let sealed_metadata = temporary
        .file
        .metadata()
        .map_err(|error| storage("reinspect fresh graph object", &cas.diagnostic_root, error))?;
    if graphforge_filesystem::file_identity(&temporary.file)
        .map_err(|error| storage("reidentify fresh graph object", &cas.diagnostic_root, error))?
        != temporary.identity
        || !sealed_metadata.is_file()
        || sealed_metadata.len() != expected_length
        || !sealed_metadata.permissions().readonly()
    {
        return Err(validation("fresh graph object post-hash authority changed"));
    }
    returned_error_boundary("install:temp-sealed")?;
    let mut concurrent_io = ReadIoEvidence::default();
    let installed = if let Ok((_installed, _identity)) = cas.tmp.link_child_into(
        &temporary.name,
        &temporary.file,
        temporary.identity,
        bucket,
        destination_name,
    ) {
        true
    } else {
        #[cfg(unix)]
        let existing = bucket.open_child_file(destination_name);
        #[cfg(windows)]
        let existing = bucket
            .open_cas_child_file(destination_name)
            .map(graphforge_filesystem::WindowsSealedCasFile::into_file);
        let existing = existing.map_err(|error| {
            storage(
                "open concurrently installed graph object",
                &cas.diagnostic_root,
                error,
            )
        })?;
        concurrent_io = verify_and_seal_graph_object_counted(
            &existing,
            digest,
            expected_length,
            &graph_object_path(&cas.diagnostic_root, digest)?,
            &cas.diagnostic_root,
        )?;
        false
    };
    returned_error_boundary("install:final-linked")?;
    // The destination namespace must be durable before its temporary alias is
    // removed; after a crash, retry can therefore authenticate the final CAS
    // name without depending on the temporary namespace.
    bucket.sync().map_err(|error| {
        storage(
            "sync stable graph object bucket",
            &cas.diagnostic_root,
            error,
        )
    })?;
    returned_error_boundary("install:bucket-synced")?;
    // Windows cannot open a deletion handle while the original temporary
    // handle remains open without delete sharing. Publication and concurrent
    // winner authentication are complete, so release it before exact-identity
    // cleanup; the fresh CAS-owned inode remains sealed at its final name.
    drop(temporary.file);
    cas.tmp
        .unlink_child_if_identity(&temporary.name, temporary.identity)
        .map_err(|error| {
            storage(
                "remove stable temporary graph object",
                &cas.diagnostic_root,
                error,
            )
        })?;
    returned_error_boundary("install:temp-unlinked")?;
    cas.tmp.sync().map_err(|error| {
        storage(
            "sync stable graph object temporary directory",
            &cas.diagnostic_root,
            error,
        )
    })?;
    Ok((
        installed,
        sealed_bytes_hashed,
        ReadIoEvidence {
            bytes: sealed_io.bytes.saturating_add(concurrent_io.bytes),
            calls: sealed_io.calls.saturating_add(concurrent_io.calls),
        },
    ))
}

#[cfg(windows)]
fn transition_temporary_to_sealed_reader(
    temporary_directory: &StableDirectory,
    temporary: TemporaryObject,
    digest: &str,
    expected_length: u64,
    diagnostic: &Path,
) -> Result<(SealedTemporaryObject, ReadIoEvidence), GfError> {
    temporary.file.sync_all().map_err(|error| {
        storage(
            "sync temporary graph object before sealing",
            diagnostic,
            error,
        )
    })?;
    let identity = temporary.identity;
    let name = temporary.name;
    let file = temporary_directory
        .seal_cas_child_file(&name, temporary.file)
        .map(graphforge_filesystem::WindowsSealedCasFile::into_file)
        .map_err(|error| storage("reopen sealed temporary graph object", diagnostic, error))?;
    if graphforge_filesystem::file_identity(&file).map_err(|error| {
        storage(
            "reidentify sealed temporary graph object",
            diagnostic,
            error,
        )
    })? != identity
    {
        return Err(validation(
            "temporary graph object identity changed while sealing",
        ));
    }
    let io = verify_file_counted(
        file.try_clone().map_err(|error| {
            storage(
                "clone sealed temporary graph object for authentication",
                diagnostic,
                error,
            )
        })?,
        digest,
        expected_length,
        diagnostic,
    )?;
    Ok((
        SealedTemporaryObject {
            name,
            file,
            identity,
        },
        io,
    ))
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
    mut file: File,
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
    let mut hasher = Sha256::new();
    let mut io = ReadIoEvidence::default();
    let mut buffer = vec![0_u8; BUFFER_BYTES];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| storage("read graph object handle", diagnostic, error))?;
        if read == 0 {
            break;
        }
        io.bytes = io.bytes.saturating_add(read as u64);
        io.calls = io.calls.saturating_add(1);
        hasher.update(&buffer[..read]);
    }
    if hex_digest(hasher.finalize().into()) != digest {
        return Err(validation("graph object digest does not match its address"));
    }
    Ok(io)
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
        total = total.saturating_add(u64::try_from(read).unwrap_or(u64::MAX));
        calls = calls.saturating_add(1);
        hasher.update(&buffer[..read]);
    }
    if total != expected_length || hex_digest(hasher.finalize().into()) != digest {
        return Err(validation("graph object digest does not match its address"));
    }
    Ok(ReadIoEvidence {
        bytes: total,
        calls,
    })
}

fn verify_and_seal_graph_object(
    file: &File,
    digest: &str,
    expected_length: u64,
    object_path: &Path,
    diagnostic: &Path,
) -> Result<(), GfError> {
    verify_and_seal_graph_object_counted(file, digest, expected_length, object_path, diagnostic)
        .map(|_| ())
}

fn verify_and_seal_graph_object_counted(
    file: &File,
    digest: &str,
    expected_length: u64,
    object_path: &Path,
    diagnostic: &Path,
) -> Result<ReadIoEvidence, GfError> {
    // Reuse is safe only after the exact opened inode is no longer writable.
    // Hashing first would leave a window in which the already-authenticated
    // bytes could be changed before the subsequent chmod.
    #[cfg(unix)]
    seal_graph_object(file, object_path, diagnostic)?;
    #[cfg(windows)]
    {
        let _ = object_path;
        if !file
            .metadata()
            .map_err(|error| storage("inspect sealed graph object", diagnostic, error))?
            .permissions()
            .readonly()
        {
            return Err(validation("graph object is not canonically sealed"));
        }
    }
    let io = verify_file_counted(
        file.try_clone()
            .map_err(|error| storage("clone graph object for authentication", diagnostic, error))?,
        digest,
        expected_length,
        diagnostic,
    )?;
    if !file
        .metadata()
        .map_err(|error| storage("reinspect sealed graph object", diagnostic, error))?
        .permissions()
        .readonly()
    {
        return Err(validation(
            "graph object became writable during authentication",
        ));
    }
    Ok(io)
}

#[cfg(unix)]
fn seal_graph_object(file: &File, object_path: &Path, diagnostic: &Path) -> Result<(), GfError> {
    let _ = object_path;
    let mut permissions = file
        .metadata()
        .map_err(|error| storage("inspect graph object permissions", diagnostic, error))?
        .permissions();
    permissions.set_readonly(true);
    file.set_permissions(permissions)
        .map_err(|error| storage("seal graph object permissions", diagnostic, error))?;
    Ok(())
}

fn hash_regular_file(path: &Path) -> Result<[u8; 32], GfError> {
    let mut file =
        File::open(path).map_err(|error| storage("open sealed graph file", path, error))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; BUFFER_BYTES];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| storage("read sealed graph file", path, error))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().into())
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
mod tests {
    use super::*;

    fn inject_returned_error(boundary: Option<&str>) {
        RETURNED_ERROR_BOUNDARY.with(|current| {
            *current.borrow_mut() = boundary.map(str::to_owned);
        });
    }

    fn assert_injected_error(error: GfError, boundary: &str) {
        match error {
            GfError::Storage(message) => assert_eq!(
                message,
                format!("injected graph object returned error at {boundary}")
            ),
            other => panic!("unexpected injected error: {other}"),
        }
    }

    #[test]
    fn install_crash_boundaries_leave_only_valid_or_recoverable_unreferenced_state() {
        let boundaries = [
            "install:temp-sealed",
            "install:final-linked",
            "install:bucket-synced",
            "install:temp-unlinked",
            "append:before-manifest-reference",
        ];
        for boundary in boundaries {
            let container = tempfile::tempdir().unwrap();
            let workspace = tempfile::tempdir().unwrap();
            let relative_path = PathBuf::from("boundary.parquet");
            let payload = vec![0x5a_u8; 4096];
            fs::write(workspace.path().join(&relative_path), &payload).unwrap();
            let digest = hex_digest(Sha256::digest(&payload).into());
            let sealed = [AuthenticatedGraphFile {
                relative_path,
                byte_length: payload.len() as u64,
                content_sha256: digest.clone(),
            }];
            let lease = begin_graph_object_publication(container.path()).unwrap();
            let mut failed_state = GraphManifestState::empty();

            inject_returned_error(Some(boundary));
            let error = append_authenticated_graph_files_v2(
                &lease,
                workspace.path(),
                &mut failed_state,
                &sealed,
                &[],
            )
            .unwrap_err();
            inject_returned_error(None);
            assert_injected_error(error, boundary);
            assert!(
                failed_state.root().is_none(),
                "{boundary} exposed a manifest reference"
            );

            let object = graph_object_path(container.path(), &digest).unwrap();
            let recoverable_temporary_exists = container
                .path()
                .join(GRAPH_OBJECTS_DIR)
                .join(TEMP_DIR)
                .read_dir()
                .unwrap()
                .next()
                .is_some();
            if object.exists() {
                verify_graph_object(container.path(), &digest, payload.len() as u64).unwrap();
            } else {
                assert!(
                    recoverable_temporary_exists,
                    "{boundary} lost both the valid object and recoverable temporary"
                );
            }

            let mut retry_state = GraphManifestState::empty();
            append_authenticated_graph_files_v2(
                &lease,
                workspace.path(),
                &mut retry_state,
                &sealed,
                &[],
            )
            .unwrap();
            assert!(retry_state.root().is_some());
            verify_graph_object(container.path(), &digest, payload.len() as u64).unwrap();
        }
        inject_returned_error(None);
    }

    #[test]
    fn returned_errors_release_every_cas_lock_and_pending_lease_boundary() {
        let root = tempfile::tempdir().unwrap();
        drop(begin_graph_object_publication(root.path()).unwrap());

        #[cfg(unix)]
        let reading_boundaries = [
            "reading:objects-lock",
            "reading:lifecycle-lock",
            "reading:revalidate",
        ]
        .as_slice();
        #[cfg(not(unix))]
        let reading_boundaries = ["reading:lifecycle-lock", "reading:revalidate"].as_slice();
        for &boundary in reading_boundaries {
            inject_returned_error(Some(boundary));
            let error = ReadOnlyCasRoot::open(root.path()).err().unwrap();
            assert_injected_error(error, boundary);
            inject_returned_error(None);
            drop(try_begin_graph_object_gc(root.path()).unwrap().unwrap());
        }

        #[cfg(unix)]
        let gc_boundaries = ["gc:objects-lock", "gc:lifecycle-lock", "gc:revalidate"].as_slice();
        #[cfg(not(unix))]
        let gc_boundaries = ["gc:lifecycle-lock", "gc:revalidate"].as_slice();
        for &boundary in gc_boundaries {
            inject_returned_error(Some(boundary));
            let error = begin_graph_object_gc(root.path()).err().unwrap();
            assert_injected_error(error, boundary);
            inject_returned_error(None);
            drop(try_begin_graph_object_gc(root.path()).unwrap().unwrap());
        }

        #[cfg(unix)]
        let try_gc_boundaries = [
            "try-gc:objects-lock",
            "try-gc:lifecycle-lock",
            "try-gc:revalidate",
        ]
        .as_slice();
        #[cfg(not(unix))]
        let try_gc_boundaries = ["try-gc:lifecycle-lock", "try-gc:revalidate"].as_slice();
        for &boundary in try_gc_boundaries {
            inject_returned_error(Some(boundary));
            let error = try_begin_graph_object_gc(root.path()).err().unwrap();
            assert_injected_error(error, boundary);
            inject_returned_error(None);
            drop(try_begin_graph_object_gc(root.path()).unwrap().unwrap());
        }

        #[cfg(unix)]
        let publication_boundaries = [
            "publication:objects-lock",
            "publication:lifecycle-lock",
            "publication:revalidate",
            "publication:lease-create",
            "publication:lease-identity",
            "publication:lease-lock",
            "publication:lease-sync",
            "publication:active-sync",
        ]
        .as_slice();
        #[cfg(not(unix))]
        let publication_boundaries = [
            "publication:lifecycle-lock",
            "publication:revalidate",
            "publication:lease-create",
            "publication:lease-identity",
            "publication:lease-lock",
            "publication:lease-sync",
            "publication:active-sync",
        ]
        .as_slice();
        for &boundary in publication_boundaries {
            inject_returned_error(Some(boundary));
            let error = begin_graph_object_publication(root.path()).err().unwrap();
            assert_injected_error(error, boundary);
            inject_returned_error(None);
            let residue_count = fs::read_dir(root.path().join(GRAPH_OBJECTS_DIR).join(ACTIVE_DIR))
                .unwrap()
                .count();
            assert_eq!(
                residue_count,
                usize::from(boundary == "publication:lease-create")
            );
            assert!(!graph_object_publication_is_live(root.path()).unwrap());
            assert_eq!(
                fs::read_dir(root.path().join(GRAPH_OBJECTS_DIR).join(ACTIVE_DIR))
                    .unwrap()
                    .count(),
                0
            );
            drop(begin_graph_object_publication(root.path()).unwrap());
            drop(try_begin_graph_object_gc(root.path()).unwrap().unwrap());
            drop(ReadOnlyCasRoot::open(root.path()).unwrap());
        }
    }

    #[test]
    fn pure_reads_require_only_existing_digest_namespace_and_never_create() {
        let root = tempfile::tempdir().unwrap();
        let payload = b"read-only payload";
        let digest = hex_digest(Sha256::digest(payload).into());
        let objects = root.path().join(GRAPH_OBJECTS_DIR);
        let sha256 = objects.join(SHA256_DIR);
        let bucket = sha256.join(&digest[..2]);
        fs::create_dir_all(&bucket).unwrap();
        fs::write(bucket.join(&digest[2..]), payload).unwrap();
        fs::write(objects.join(LIFECYCLE_LOCK), b"").unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            fs::set_permissions(bucket.join(&digest[2..]), fs::Permissions::from_mode(0o400))
                .unwrap();
            fs::set_permissions(
                objects.join(LIFECYCLE_LOCK),
                fs::Permissions::from_mode(0o400),
            )
            .unwrap();
            fs::set_permissions(&bucket, fs::Permissions::from_mode(0o500)).unwrap();
            fs::set_permissions(&sha256, fs::Permissions::from_mode(0o500)).unwrap();
            fs::set_permissions(&objects, fs::Permissions::from_mode(0o500)).unwrap();
        }

        let namespace_before = fs::read_dir(&objects)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<BTreeSet<_>>();
        let digest_namespace_before = fs::read_dir(&sha256)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<BTreeSet<_>>();
        let bucket_namespace_before = fs::read_dir(&bucket)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            read_graph_object(root.path(), &digest, payload.len() as u64).unwrap(),
            payload
        );
        verify_graph_object(root.path(), &digest, payload.len() as u64).unwrap();
        assert_eq!(
            read_graph_object_by_digest(root.path(), &digest, 1024).unwrap(),
            payload
        );
        let namespace_after = fs::read_dir(&objects)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<BTreeSet<_>>();
        assert_eq!(namespace_after, namespace_before);
        assert_eq!(
            namespace_after,
            BTreeSet::from([SHA256_DIR.into(), LIFECYCLE_LOCK.into()])
        );
        assert_eq!(
            fs::read_dir(&sha256)
                .unwrap()
                .map(|entry| entry.unwrap().file_name())
                .collect::<BTreeSet<_>>(),
            digest_namespace_before
        );
        assert_eq!(
            fs::read_dir(&bucket)
                .unwrap()
                .map(|entry| entry.unwrap().file_name())
                .collect::<BTreeSet<_>>(),
            bucket_namespace_before
        );
        assert_eq!(fs::read(bucket.join(&digest[2..])).unwrap(), payload);

        #[cfg(windows)]
        {
            let lifecycle = objects.join(LIFECYCLE_LOCK);
            let object = bucket.join(&digest[2..]);
            let mut permissions = fs::metadata(&lifecycle).unwrap().permissions();
            permissions.set_readonly(true);
            fs::set_permissions(&lifecycle, permissions).unwrap();
            let mut permissions = fs::metadata(&object).unwrap().permissions();
            permissions.set_readonly(true);
            fs::set_permissions(&object, permissions).unwrap();
            verify_graph_object(root.path(), &digest, payload.len() as u64).unwrap();
            let mut permissions = fs::metadata(&lifecycle).unwrap().permissions();
            permissions.set_readonly(false);
            fs::set_permissions(&lifecycle, permissions).unwrap();
            let mut permissions = fs::metadata(&object).unwrap().permissions();
            permissions.set_readonly(false);
            fs::set_permissions(&object, permissions).unwrap();
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let target_owner = tempfile::tempdir().unwrap();
            let inventory = GraphFilesInventory {
                format: "graphforge-graph-files".into(),
                format_version: 1,
                files: vec![crate::GraphFileEntry {
                    relative_path: "payload.bin".into(),
                    byte_length: payload.len() as u64,
                    content_sha256: digest.clone(),
                    role: crate::GraphFileRole::Other,
                }],
                file_count: 1,
                total_byte_length: payload.len() as u64,
            };
            assert!(
                materialize_graph_objects(
                    root.path(),
                    &inventory,
                    &target_owner.path().join("readonly-target")
                )
                .is_err()
            );
            assert_eq!(
                fs::read_dir(&objects)
                    .unwrap()
                    .map(|entry| entry.unwrap().file_name())
                    .collect::<BTreeSet<_>>(),
                namespace_before
            );
            fs::set_permissions(&objects, fs::Permissions::from_mode(0o700)).unwrap();
            fs::set_permissions(
                objects.join(LIFECYCLE_LOCK),
                fs::Permissions::from_mode(0o600),
            )
            .unwrap();
            fs::set_permissions(&sha256, fs::Permissions::from_mode(0o700)).unwrap();
            fs::set_permissions(&bucket, fs::Permissions::from_mode(0o700)).unwrap();
            fs::set_permissions(bucket.join(&digest[2..]), fs::Permissions::from_mode(0o600))
                .unwrap();
            materialize_graph_objects(
                root.path(),
                &inventory,
                &target_owner.path().join("writable-target"),
            )
            .unwrap();
            assert!(objects.join(TEMP_DIR).is_dir());
            assert!(objects.join(ACTIVE_DIR).is_dir());
            assert!(objects.join(LIFECYCLE_LOCK).is_file());
        }

        let missing = tempfile::tempdir().unwrap();
        assert!(read_graph_object(missing.path(), &digest, payload.len() as u64).is_err());
        assert!(!missing.path().join(GRAPH_OBJECTS_DIR).exists());
    }

    #[test]
    fn read_only_guard_pins_cas_against_gc() {
        let root = tempfile::tempdir().unwrap();
        let lease = begin_graph_object_publication(root.path()).unwrap();
        drop(lease);
        let reader = ReadOnlyCasRoot::open(root.path()).unwrap();
        assert!(matches!(try_begin_graph_object_gc(root.path()), Ok(None)));
        drop(reader);
        assert!(matches!(
            try_begin_graph_object_gc(root.path()),
            Ok(Some(_))
        ));
    }

    #[test]
    fn read_only_lifecycle_rejects_multiple_links() {
        let root = tempfile::tempdir().unwrap();
        let objects = root.path().join(GRAPH_OBJECTS_DIR);
        fs::create_dir_all(objects.join(SHA256_DIR)).unwrap();
        let outside = root.path().join("outside");
        fs::write(&outside, b"").unwrap();
        fs::hard_link(&outside, objects.join(LIFECYCLE_LOCK)).unwrap();
        assert!(ReadOnlyCasRoot::open(root.path()).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn read_only_lifecycle_rejects_links_fifos_and_sockets_without_blocking() {
        use std::os::unix::fs::symlink;
        use std::os::unix::net::UnixListener;
        use std::process::Command;

        let prepare = || {
            let root = tempfile::tempdir().unwrap();
            let objects = root.path().join(GRAPH_OBJECTS_DIR);
            fs::create_dir_all(objects.join(SHA256_DIR)).unwrap();
            (root, objects)
        };

        const FIFO_HELPER: &str = "GRAPHFORGE_READ_ONLY_CAS_FIFO_HELPER";
        if std::env::var_os(FIFO_HELPER).is_some() {
            let (root, objects) = prepare();
            assert!(
                Command::new("mkfifo")
                    .arg(objects.join(LIFECYCLE_LOCK))
                    .status()
                    .unwrap()
                    .success()
            );
            assert!(ReadOnlyCasRoot::open(root.path()).is_err());
            return;
        }

        let (root, objects) = prepare();
        let outside = root.path().join("outside");
        fs::write(&outside, b"").unwrap();
        symlink(&outside, objects.join(LIFECYCLE_LOCK)).unwrap();
        assert!(ReadOnlyCasRoot::open(root.path()).is_err());

        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "graph_object_store::tests::read_only_lifecycle_rejects_links_fifos_and_sockets_without_blocking",
                "--nocapture",
            ])
            .env(FIFO_HELPER, "1")
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                assert!(status.success());
                break;
            }
            if std::time::Instant::now() >= deadline {
                child.kill().unwrap();
                panic!("read-only lifecycle FIFO open blocked");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        let (root, objects) = prepare();
        let _socket = UnixListener::bind(objects.join(LIFECYCLE_LOCK)).unwrap();
        assert!(ReadOnlyCasRoot::open(root.path()).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn lifecycle_lock_rejects_symlink_and_pathname_substitution() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let object_root = root.path().join(GRAPH_OBJECTS_DIR);
        fs::create_dir_all(&object_root).unwrap();
        let outside = root.path().join("outside.lock");
        fs::write(&outside, b"").unwrap();
        symlink(&outside, object_root.join(LIFECYCLE_LOCK)).unwrap();
        assert!(begin_graph_object_publication(root.path()).is_err());

        fs::remove_file(object_root.join(LIFECYCLE_LOCK)).unwrap();
        let lease = begin_graph_object_publication(root.path()).unwrap();
        let displaced = object_root.join("displaced.lock");
        fs::rename(object_root.join(LIFECYCLE_LOCK), &displaced).unwrap();
        fs::write(object_root.join(LIFECYCLE_LOCK), b"").unwrap();

        assert!(lease.revalidate_for_publish().is_err());
        assert!(matches!(try_begin_graph_object_gc(root.path()), Ok(None)));

        let other_root = tempfile::tempdir().unwrap();
        let lease = begin_graph_object_publication(other_root.path()).unwrap();
        let object_root = other_root.path().join(GRAPH_OBJECTS_DIR);
        fs::rename(&object_root, other_root.path().join("displaced-objects")).unwrap();
        fs::create_dir(&object_root).unwrap();
        assert!(lease.revalidate_for_publish().is_err());
        assert!(matches!(
            try_begin_graph_object_gc(other_root.path()),
            Ok(Some(_))
        ));
        assert!(lease.revalidate_for_publish().is_err());
    }

    #[cfg(windows)]
    #[test]
    fn windows_lifecycle_handles_deny_coordination_path_replacement() {
        let root = tempfile::tempdir().unwrap();
        let lease = begin_graph_object_publication(root.path()).unwrap();
        let lifecycle = root.path().join(GRAPH_OBJECTS_DIR).join(LIFECYCLE_LOCK);
        let replacement = lifecycle.with_extension("replacement");
        assert!(fs::rename(&lifecycle, replacement).is_err());
        lease.revalidate_for_publish().unwrap();
    }

    #[cfg(windows)]
    #[test]
    fn windows_materialization_rejects_regular_cas_substitution() {
        let objects = tempfile::tempdir().unwrap();
        let (digest, _) = install_graph_object_bytes(objects.path(), b"payload").unwrap();
        let source = graph_object_path(objects.path(), &digest).unwrap();
        fs::remove_file(&source).unwrap();
        fs::write(&source, b"hostile").unwrap();
        let owner = tempfile::tempdir().unwrap();
        let target_path = owner.path().join("materialized");
        let target = open_empty_materialization_target(&target_path).unwrap();
        assert!(link_materialized_object(&target, &source, "payload.bin", &digest, 7).is_err());
        assert!(!target_path.join("payload.bin").exists());
    }

    #[test]
    fn publication_and_gc_lifecycles_are_mutually_exclusive() {
        use std::sync::mpsc::{self, TryRecvError};
        use std::time::Duration;

        let root = tempfile::tempdir().unwrap();
        let publication = begin_graph_object_publication(root.path()).unwrap();
        assert!(matches!(
            gc_graph_objects(root.path(), &[], crate::GraphManifestLimits::default()),
            Err(GfError::Project {
                code: ProjectErrorCode::WriterBusy,
                ..
            })
        ));
        let path = root.path().to_path_buf();
        let (gc_tx, gc_rx) = mpsc::channel();
        let gc_thread = std::thread::spawn(move || {
            let guard = begin_graph_object_gc(&path).unwrap();
            gc_tx.send(guard).unwrap();
        });
        assert!(matches!(gc_rx.try_recv(), Err(TryRecvError::Empty)));
        drop(publication);
        let gc = gc_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        gc_thread.join().unwrap();

        let path = root.path().to_path_buf();
        let (publish_tx, publish_rx) = mpsc::channel();
        let publish_thread = std::thread::spawn(move || {
            let lease = begin_graph_object_publication(&path).unwrap();
            publish_tx.send(lease).unwrap();
        });
        assert!(matches!(publish_rx.try_recv(), Err(TryRecvError::Empty)));
        drop(gc);
        let publication = publish_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        publish_thread.join().unwrap();
        drop(publication);
    }

    #[test]
    fn authenticated_reader_keeps_cas_sealed_through_descriptor_consumption() {
        let root = tempfile::tempdir().unwrap();
        let payload = b"authenticated payload";
        let (digest, _) = install_graph_object_bytes(root.path(), payload).unwrap();
        let mut reader =
            open_graph_object_by_digest(root.path(), &digest, payload.len() as u64).unwrap();
        assert_eq!(reader.len(), payload.len() as u64);
        let path = graph_object_path(root.path(), &digest).unwrap();

        let mutation = fs::OpenOptions::new().write(true).open(&path);
        assert!(
            mutation.is_err(),
            "sealed CAS object accepted in-place mutation"
        );

        let mut decoded = Vec::new();
        reader.read_to_end(&mut decoded).unwrap();
        assert_eq!(decoded, payload);
    }

    #[cfg(unix)]
    #[test]
    fn reuse_seals_the_authenticated_descriptor_not_a_replaced_path() {
        use std::os::unix::fs::PermissionsExt;

        let root = tempfile::tempdir().unwrap();
        let payload = b"descriptor-bound payload";
        let (digest, _) = install_graph_object_bytes(root.path(), payload).unwrap();
        let path = graph_object_path(root.path(), &digest).unwrap();
        let displaced = path.with_extension("displaced");
        let descriptor = File::open(&path).unwrap();

        fs::rename(&path, &displaced).unwrap();
        fs::set_permissions(&displaced, fs::Permissions::from_mode(0o600)).unwrap();
        fs::write(&path, b"hostile replacement").unwrap();
        verify_and_seal_graph_object(
            &descriptor,
            &digest,
            payload.len() as u64,
            &displaced,
            root.path(),
        )
        .unwrap();

        assert!(fs::metadata(&displaced).unwrap().permissions().readonly());
        assert!(!fs::metadata(&path).unwrap().permissions().readonly());
        assert!(open_graph_object_by_digest(root.path(), &digest, payload.len() as u64).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn reuse_seals_same_inode_before_digest_authentication() {
        let root = tempfile::tempdir().unwrap();
        let payload = b"seal-before-authentication";
        let digest = hex_digest(Sha256::digest(payload).into());
        let path = root.path().join("candidate");
        fs::write(&path, payload).unwrap();
        let descriptor = File::open(&path).unwrap();

        seal_graph_object(&descriptor, &path, root.path()).unwrap();
        assert!(fs::OpenOptions::new().write(true).open(&path).is_err());
        verify_file(
            descriptor.try_clone().unwrap(),
            &digest,
            payload.len() as u64,
            root.path(),
        )
        .unwrap();
        assert!(descriptor.metadata().unwrap().permissions().readonly());
    }

    #[cfg(unix)]
    #[test]
    fn fresh_write_descriptor_can_be_sealed_without_losing_named_identity() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("fresh-object");
        let mut descriptor = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        descriptor.write_all(b"fresh payload").unwrap();
        descriptor.sync_all().unwrap();
        let identity = graphforge_filesystem::file_identity(&descriptor).unwrap();

        seal_graph_object(&descriptor, &path, root.path()).unwrap();

        assert_eq!(
            graphforge_filesystem::path_identity(&path).unwrap(),
            identity
        );
        assert!(descriptor.metadata().unwrap().permissions().readonly());
    }

    #[cfg(windows)]
    #[test]
    fn fresh_cas_transition_excludes_writers_before_and_after_seal() {
        use std::os::windows::fs::OpenOptionsExt as _;
        use std::process::Command;

        const HELPER: &str = "GRAPHFORGE_CAS_WRITER_EXCLUSION_HELPER";
        const FILE_SHARE_READ: u32 = 0x0000_0001;
        const FILE_SHARE_WRITE: u32 = 0x0000_0002;
        if let Some(path) = std::env::var_os(HELPER) {
            let writer = OpenOptions::new()
                .write(true)
                .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
                .open(path);
            assert!(writer.is_err(), "child unexpectedly acquired CAS writer");
            return;
        }

        let root = tempfile::tempdir().unwrap();
        let directory = StableDirectory::open(root.path()).unwrap();
        let name = std::ffi::OsStr::new("temporary");
        let path = root.path().join(name);
        let payload = b"sealed only after exclusive read admission";
        let mut writer = directory.create_cas_child_file(name).unwrap();
        writer.write_all(payload).unwrap();

        let assert_child_writer_denied = || {
            let status = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "graph_object_store::tests::fresh_cas_transition_excludes_writers_before_and_after_seal",
                    "--nocapture",
                ])
                .env(HELPER, &path)
                .status()
                .unwrap();
            assert!(status.success());
        };
        assert_child_writer_denied();
        let sealed = directory.seal_cas_child_file(name, writer).unwrap();
        assert_child_writer_denied();
        assert_eq!(
            graphforge_filesystem::file_identity(&sealed.into_file()).unwrap(),
            graphforge_filesystem::path_identity(&path).unwrap()
        );
    }

    #[cfg(windows)]
    #[test]
    fn ordinary_owner_can_cleanup_fresh_cas_temp_and_gc_sealed_object() {
        let root = tempfile::tempdir().unwrap();
        let payload = b"owner-only Windows CAS cleanup";

        let (digest, evidence) = install_graph_object_bytes(root.path(), payload).unwrap();
        assert!(!evidence.reused_existing);
        let object = graph_object_path(root.path(), &digest).unwrap();
        assert!(object.exists());

        // Installation must remove the sealed temporary hard link using the
        // same owner capability available to a restricted, non-admin process.
        let lease = begin_graph_object_publication(root.path()).unwrap();
        assert!(lease.cas.tmp.child_names().unwrap().is_empty());
        drop(lease);

        // GC uses that same narrow capability on the canonical sealed name.
        let reclaimed =
            gc_graph_objects(root.path(), &[], crate::GraphManifestLimits::default()).unwrap();
        assert_eq!(reclaimed.objects_removed, 1);
        assert_eq!(reclaimed.bytes_removed, payload.len() as u64);
        assert!(!object.exists());
    }

    #[cfg(windows)]
    #[test]
    fn reuse_rejects_planted_unsealed_object_without_mutating_it() {
        let root = tempfile::tempdir().unwrap();
        let payload = b"planted writable object";
        let digest = hex_digest(Sha256::digest(payload).into());
        let lease = begin_graph_object_publication(root.path()).unwrap();
        let bucket = lease.cas.digest_bucket(&digest, true).unwrap();
        let name = std::ffi::OsStr::new(&digest[2..]);
        let path = graph_object_path(root.path(), &digest).unwrap();
        fs::write(&path, payload).unwrap();
        assert!(!fs::metadata(&path).unwrap().permissions().readonly());
        assert!(bucket.open_cas_child_file(name).is_err());
        drop(lease);

        assert!(install_graph_object_bytes(root.path(), payload).is_err());
        assert_eq!(fs::read(&path).unwrap(), payload);
        assert!(!fs::metadata(&path).unwrap().permissions().readonly());
    }

    #[cfg(windows)]
    #[test]
    fn authenticated_released_readonly_object_is_adopted_canonically() {
        use std::os::windows::fs::OpenOptionsExt as _;

        const FILE_SHARE_READ: u32 = 0x0000_0001;
        const FILE_SHARE_WRITE: u32 = 0x0000_0002;
        let root = tempfile::tempdir().unwrap();
        let payload = b"released readonly inherited-dacl object";
        let digest = hex_digest(Sha256::digest(payload).into());
        let lease = begin_graph_object_publication(root.path()).unwrap();
        let _bucket = lease.cas.digest_bucket(&digest, true).unwrap();
        let path = graph_object_path(root.path(), &digest).unwrap();
        fs::write(&path, payload).unwrap();
        let mut permissions = fs::metadata(&path).unwrap().permissions();
        permissions.set_readonly(true);
        fs::set_permissions(&path, permissions).unwrap();
        drop(lease);

        let (_, evidence) = install_graph_object_bytes(root.path(), payload).unwrap();
        assert!(evidence.reused_existing);
        let writer = OpenOptions::new()
            .write(true)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
            .open(&path);
        assert!(writer.is_err(), "canonical adoption retained write access");
    }

    #[cfg(unix)]
    #[test]
    fn materialization_rejects_target_and_intermediate_symlink_escape() {
        use std::os::unix::fs::symlink;

        let objects = tempfile::tempdir().unwrap();
        let (digest, _) = install_graph_object_bytes(objects.path(), b"payload").unwrap();
        let entry = crate::GraphFileEntry {
            relative_path: "nested/payload.bin".into(),
            byte_length: 7,
            content_sha256: digest,
            role: crate::GraphFileRole::Other,
        };
        let inventory = crate::graph_files::inventory_from_entries(vec![entry.clone()]).unwrap();

        let owner = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let linked_target = owner.path().join("linked-target");
        symlink(outside.path(), &linked_target).unwrap();
        assert!(materialize_graph_objects(objects.path(), &inventory, &linked_target).is_err());
        assert!(!outside.path().join("nested/payload.bin").exists());

        let target = owner.path().join("real-target");
        let directory = open_empty_materialization_target(&target).unwrap();
        symlink(outside.path(), target.join("nested")).unwrap();
        assert!(
            link_materialized_object(
                &directory,
                &graph_object_path(objects.path(), &entry.content_sha256).unwrap(),
                &entry.relative_path,
                &entry.content_sha256,
                entry.byte_length,
            )
            .is_err()
        );
        assert!(!outside.path().join("payload.bin").exists());
    }

    #[test]
    fn materialization_preserves_ordinary_workspace_relative_paths() {
        let root = tempfile::tempdir().unwrap();
        let payload = b"ordinary topology payload";
        let (digest, _) = install_graph_object_bytes(root.path(), payload).unwrap();
        let inventory = crate::graph_files::inventory_from_entries(vec![crate::GraphFileEntry {
            relative_path: "topology/edges/knows.parquet".into(),
            byte_length: payload.len() as u64,
            content_sha256: digest,
            role: crate::GraphFileRole::Topology,
        }])
        .unwrap();
        let owner = tempfile::tempdir().unwrap();
        let target = owner.path().join("workspace");

        let evidence = materialize_graph_objects(root.path(), &inventory, &target).unwrap();

        assert_eq!(
            fs::read(target.join("topology/edges/knows.parquet")).unwrap(),
            payload
        );
        assert!(!target.join("files").exists());
        assert_eq!(evidence.files_reused, 1);
        assert_eq!(evidence.bytes_reused, payload.len() as u64);
        assert_eq!(evidence.files_copied, 0);
        assert_eq!(evidence.application_read_bytes, payload.len() as u64);
        assert_eq!(evidence.application_read_calls, 1);
        assert_eq!(evidence.application_write_bytes, 0);
        assert_eq!(evidence.application_write_calls, 0);
        assert_eq!(evidence.fsync_calls, 0);
    }

    #[test]
    fn materialization_gives_v4_ordinal_authority_private_single_link_inodes() {
        let root = tempfile::tempdir().unwrap();
        let files = [
            ("topology/uuid-membership/ordinal-v4.lock", &b""[..]),
            (
                "topology/uuid-membership/ordinal-v4-manifest.json",
                &b"manifest"[..],
            ),
            (
                "topology/uuid-membership/ordinal-v4-receipt.json",
                &b"receipt"[..],
            ),
            (
                "topology/uuid-membership/ordinal-v4-1-0123456789abcdef.uuidx",
                &b"ordinal"[..],
            ),
        ];
        let mut entries = Vec::new();
        for (relative_path, payload) in files {
            let (digest, _) = install_graph_object_bytes(root.path(), payload).unwrap();
            entries.push(crate::GraphFileEntry {
                relative_path: relative_path.to_owned(),
                byte_length: payload.len() as u64,
                content_sha256: digest,
                role: crate::GraphFileRole::Topology,
            });
        }
        entries.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
        let inventory = crate::graph_files::inventory_from_entries(entries).unwrap();
        let owner = tempfile::tempdir().unwrap();
        let target = owner.path().join("workspace");

        let evidence = materialize_graph_objects(root.path(), &inventory, &target).unwrap();

        assert_eq!(evidence.files_copied, inventory.file_count);
        assert_eq!(evidence.files_reused, 0);
        let nonempty_bytes = inventory.total_byte_length;
        let nonempty_files = inventory
            .files
            .iter()
            .filter(|entry| entry.byte_length != 0)
            .count() as u64;
        assert_eq!(evidence.application_read_bytes, nonempty_bytes * 2);
        assert_eq!(evidence.application_read_calls, nonempty_files * 2);
        assert_eq!(evidence.application_write_bytes, nonempty_bytes);
        assert_eq!(evidence.application_write_calls, nonempty_files);
        assert_eq!(evidence.fsync_calls, inventory.file_count * 2);
        for entry in &inventory.files {
            let file = File::open(target.join(&entry.relative_path)).unwrap();
            assert_eq!(graphforge_filesystem::file_link_count(&file).unwrap(), 1);
            assert_eq!(
                fs::read(target.join(&entry.relative_path)).unwrap().len() as u64,
                entry.byte_length
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn materialization_rejects_substituted_cas_source_and_removes_destination() {
        use std::os::unix::fs::symlink;

        let objects = tempfile::tempdir().unwrap();
        let (digest, _) = install_graph_object_bytes(objects.path(), b"payload").unwrap();
        let source = graph_object_path(objects.path(), &digest).unwrap();
        let target_root = tempfile::tempdir().unwrap();
        let target = target_root.path().join("materialized");

        let original = source.with_extension("original");
        fs::rename(&source, &original).unwrap();
        let malicious = objects.path().join("malicious");
        fs::write(&malicious, b"hostile").unwrap();
        symlink(&malicious, &source).unwrap();
        let directory = open_empty_materialization_target(&target).unwrap();
        assert!(link_materialized_object(&directory, &source, "payload.bin", &digest, 7).is_err());
        assert!(!target.join("payload.bin").exists());

        fs::remove_file(&source).unwrap();
        fs::write(&source, b"hostile").unwrap();
        let second_target = target_root.path().join("materialized-regular");
        let directory = open_empty_materialization_target(&second_target).unwrap();
        assert!(link_materialized_object(&directory, &source, "payload.bin", &digest, 7).is_err());
        assert!(!second_target.join("payload.bin").exists());
    }

    #[test]
    fn installs_once_reuses_exact_object_and_rejects_tampering() {
        let root = tempfile::tempdir().unwrap();
        let (digest, first) = install_graph_object_bytes(root.path(), b"payload").unwrap();
        assert_eq!(first.bytes_hashed, 7);
        assert_eq!(first.bytes_installed, 7);
        assert!(!first.reused_existing);
        assert_eq!(first.read_calls, 1);
        assert_eq!(first.write_bytes, 7);
        assert_eq!(first.write_calls, 1);
        assert_eq!(first.fsync_calls, 3);
        assert!(
            root.path()
                .join(GRAPH_OBJECTS_DIR)
                .join(TEMP_DIR)
                .read_dir()
                .unwrap()
                .next()
                .is_none(),
            "fresh installation retained its sealed temporary alias"
        );
        let (_, second) = install_graph_object_bytes(root.path(), b"payload").unwrap();
        assert!(second.reused_existing);
        assert_eq!(second.read_calls, 1);
        assert_eq!(second.write_bytes, 0);
        assert_eq!(second.write_calls, 0);
        assert_eq!(second.fsync_calls, 0);
        assert_eq!(
            read_graph_object(root.path(), &digest, 7).unwrap(),
            b"payload"
        );

        corrupt_sealed_graph_object_for_test(
            &graph_object_path(root.path(), &digest).unwrap(),
            b"corrupt",
        );
        assert!(install_graph_object_bytes(root.path(), b"payload").is_err());
    }

    #[cfg(windows)]
    #[test]
    fn windows_fresh_sealed_install_removes_its_temporary_alias() {
        let root = tempfile::tempdir().unwrap();
        install_graph_object_bytes(root.path(), b"windows sealed payload").unwrap();

        assert!(
            root.path()
                .join(GRAPH_OBJECTS_DIR)
                .join(TEMP_DIR)
                .read_dir()
                .unwrap()
                .next()
                .is_none()
        );
    }

    #[test]
    fn rejects_unsafe_digest_and_source_mismatch() {
        let root = tempfile::tempdir().unwrap();
        assert!(graph_object_path(root.path(), "../escape").is_err());
        let source = root.path().join("source");
        fs::write(&source, b"payload").unwrap();
        assert!(install_graph_object_file(root.path(), &source, &"0".repeat(64), 7).is_err());
        assert!(
            install_graph_object_file(
                root.path(),
                &source,
                &hex_digest(Sha256::digest(b"payload").into()),
                8
            )
            .is_err()
        );
    }

    #[test]
    fn publication_lease_blocks_sweep_probe_and_stale_residue_is_reclaimed() {
        let root = tempfile::tempdir().unwrap();
        let lease = begin_graph_object_publication(root.path()).unwrap();
        assert!(graph_object_publication_is_live(root.path()).unwrap());
        let active = root.path().join(GRAPH_OBJECTS_DIR).join(ACTIVE_DIR);
        drop(lease);
        let stale = active.join("00000000-0000-0000-0000-000000000000.lock");
        fs::write(&stale, []).unwrap();
        assert!(!graph_object_publication_is_live(root.path()).unwrap());
        assert!(!stale.exists());
    }

    #[test]
    fn publication_lease_probe_fails_closed_on_noncanonical_residue() {
        let root = tempfile::tempdir().unwrap();
        let active = root.path().join(GRAPH_OBJECTS_DIR).join(ACTIVE_DIR);
        fs::create_dir_all(&active).unwrap();
        let hostile = active.join("caller-owned");
        fs::write(&hostile, b"preserve").unwrap();
        assert!(graph_object_publication_is_live(root.path()).is_err());
        assert_eq!(fs::read(&hostile).unwrap(), b"preserve");
    }

    #[test]
    fn migrates_v1_tree_once_and_reopens_from_compact_root() {
        let container = tempfile::tempdir().unwrap();
        let graph = tempfile::tempdir().unwrap();
        fs::create_dir_all(graph.path().join("topology/edges/knows")).unwrap();
        fs::write(
            graph.path().join("topology/edges/knows/1-1.parquet"),
            b"edge",
        )
        .unwrap();
        let (inventory, _) = crate::capture_graph_files(graph.path()).unwrap();
        let lease = begin_graph_object_publication(container.path()).unwrap();
        let (root, evidence) =
            migrate_graph_files_v1_to_v2(&lease, graph.path(), &inventory).unwrap();
        assert_eq!(evidence.payload_objects, 1);
        assert_eq!(evidence.payload_bytes_hashed, 4);
        let (files, _) =
            crate::resolve_graph_manifest(&root, crate::GraphManifestLimits::default(), |digest| {
                read_graph_object_by_digest(container.path(), digest, 1024 * 1024)
            })
            .unwrap();
        assert_eq!(files, inventory.files);
    }

    #[cfg(unix)]
    #[test]
    fn migrates_windows_authored_v1_path_into_canonical_v2_manifest() {
        let container = tempfile::tempdir().unwrap();
        let graph = tempfile::tempdir().unwrap();
        let source = graph.path().join("topology/nodes.parquet");
        fs::create_dir_all(source.parent().unwrap()).unwrap();
        fs::write(&source, b"nodes").unwrap();
        let inventory = crate::GraphFilesInventory {
            format: "graphforge-graph-files".into(),
            format_version: 1,
            files: vec![crate::GraphFileEntry {
                relative_path: "topology\\nodes.parquet".into(),
                byte_length: 5,
                content_sha256: hex_digest(Sha256::digest(b"nodes").into()),
                role: crate::GraphFileRole::Topology,
            }],
            file_count: 1,
            total_byte_length: 5,
        };
        let lease = begin_graph_object_publication(container.path()).unwrap();
        let (root, _) = migrate_graph_files_v1_to_v2(&lease, graph.path(), &inventory).unwrap();
        let (files, _) =
            crate::resolve_graph_manifest(&root, crate::GraphManifestLimits::default(), |digest| {
                read_graph_object_by_digest(container.path(), digest, 1024 * 1024)
            })
            .unwrap();
        assert_eq!(files[0].relative_path, "topology/nodes.parquet");
    }

    #[test]
    fn migration_rejects_noncanonical_v1_before_installing_objects() {
        let graph = tempfile::tempdir().unwrap();
        fs::write(graph.path().join("a.parquet"), b"a").unwrap();
        fs::write(graph.path().join("b.parquet"), b"bb").unwrap();
        let (valid, _) = crate::capture_graph_files(graph.path()).unwrap();
        let mut invalid = Vec::new();
        let mut wrong_count = valid.clone();
        wrong_count.file_count += 1;
        invalid.push(wrong_count);
        let mut wrong_total = valid.clone();
        wrong_total.total_byte_length += 1;
        invalid.push(wrong_total);
        let mut unordered = valid.clone();
        unordered.files.reverse();
        invalid.push(unordered);
        let mut duplicate = valid.clone();
        duplicate.files[1] = duplicate.files[0].clone();
        duplicate.total_byte_length = duplicate.files.iter().map(|entry| entry.byte_length).sum();
        invalid.push(duplicate);

        for inventory in invalid {
            let container = tempfile::tempdir().unwrap();
            let lease = begin_graph_object_publication(container.path()).unwrap();
            assert!(migrate_graph_files_v1_to_v2(&lease, graph.path(), &inventory,).is_err());
            let digest_root = container.path().join(GRAPH_OBJECTS_DIR).join(SHA256_DIR);
            assert_eq!(
                fs::read_dir(digest_root)
                    .unwrap()
                    .map(Result::unwrap)
                    .count(),
                0
            );
        }
    }

    #[test]
    fn repeated_v2_appends_examine_only_changed_descriptors() {
        let container = tempfile::tempdir().unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let lease = begin_graph_object_publication(container.path()).unwrap();
        let mut state = GraphManifestState::empty();
        for ordinal in 0_u8..8 {
            let relative = PathBuf::from(format!(
                "topology/edges/knows/{ordinal:020}-{ordinal:020}.parquet"
            ));
            let path = workspace.path().join(&relative);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(&path, [ordinal]).unwrap();
            let (root, evidence) =
                append_graph_files_v2(&lease, workspace.path(), &mut state, &[relative], &[])
                    .unwrap();
            assert_eq!(evidence.changed_entries_examined, 1);
            assert_eq!(evidence.prior_entries_examined, 0);
            assert_eq!(state.root(), Some(&root));
        }
        let root = state.root().unwrap().clone();
        let (resolved, evidence) =
            crate::resolve_graph_manifest(&root, crate::GraphManifestLimits::default(), |digest| {
                read_graph_object_by_digest(container.path(), digest, 1024 * 1024)
            })
            .unwrap();
        assert_eq!(resolved.len(), 8);
        assert!(evidence.segments_examined <= 1 + u64::from(GRAPH_RADIX_DEPTH) * 8);

        let deleted = "topology/edges/knows/00000000000000000003-00000000000000000003.parquet";
        let (root, delete_evidence) =
            append_graph_files_v2(&lease, workspace.path(), &mut state, &[], &[deleted.into()])
                .unwrap();
        assert_eq!(delete_evidence.changed_entries_examined, 1);
        assert_eq!(delete_evidence.prior_entries_examined, 0);
        let (resolved, _) =
            crate::resolve_graph_manifest(&root, crate::GraphManifestLimits::default(), |digest| {
                read_graph_object_by_digest(container.path(), digest, 1024 * 1024)
            })
            .unwrap();
        assert_eq!(resolved.len(), 7);
        assert!(!resolved.iter().any(|entry| entry.relative_path == deleted));
    }

    #[test]
    fn radix_root_is_deterministic_across_descriptor_order() {
        let workspace = tempfile::tempdir().unwrap();
        let paths: Vec<_> = (0_u8..12)
            .map(|ordinal| {
                PathBuf::from(format!(
                    "topology/nodes/{ordinal:020}-{ordinal:020}.parquet"
                ))
            })
            .collect();
        for (ordinal, relative) in paths.iter().enumerate() {
            let source = workspace.path().join(relative);
            fs::create_dir_all(source.parent().unwrap()).unwrap();
            fs::write(source, [u8::try_from(ordinal).unwrap()]).unwrap();
        }

        let first = tempfile::tempdir().unwrap();
        let first_lease = begin_graph_object_publication(first.path()).unwrap();
        let mut first_state = GraphManifestState::empty();
        let (first_root, _) = append_graph_files_v2(
            &first_lease,
            workspace.path(),
            &mut first_state,
            &paths,
            &[],
        )
        .unwrap();

        let second = tempfile::tempdir().unwrap();
        let second_lease = begin_graph_object_publication(second.path()).unwrap();
        let mut reversed = paths.clone();
        reversed.reverse();
        let mut second_state = GraphManifestState::empty();
        let (second_root, _) = append_graph_files_v2(
            &second_lease,
            workspace.path(),
            &mut second_state,
            &reversed,
            &[],
        )
        .unwrap();
        assert_eq!(first_root, second_root);
    }

    #[test]
    fn patricia_delete_collapse_and_readd_converge_to_fresh_roots() {
        let workspace = tempfile::tempdir().unwrap();
        let paths = [PathBuf::from("a.parquet"), PathBuf::from("b.parquet")];
        for (ordinal, path) in paths.iter().enumerate() {
            fs::write(
                workspace.path().join(path),
                [u8::try_from(ordinal).unwrap()],
            )
            .unwrap();
        }
        let container = tempfile::tempdir().unwrap();
        let lease = begin_graph_object_publication(container.path()).unwrap();
        let mut state = GraphManifestState::empty();
        let (both, _) =
            append_graph_files_v2(&lease, workspace.path(), &mut state, &paths, &[]).unwrap();
        let (survivor, _) = append_graph_files_v2(
            &lease,
            workspace.path(),
            &mut state,
            &[],
            &["b.parquet".into()],
        )
        .unwrap();

        let fresh = tempfile::tempdir().unwrap();
        let fresh_lease = begin_graph_object_publication(fresh.path()).unwrap();
        let mut fresh_state = GraphManifestState::empty();
        let (fresh_survivor, _) = append_graph_files_v2(
            &fresh_lease,
            workspace.path(),
            &mut fresh_state,
            &paths[..1],
            &[],
        )
        .unwrap();
        assert_eq!(survivor, fresh_survivor);

        let (readded, _) =
            append_graph_files_v2(&lease, workspace.path(), &mut state, &paths[1..], &[]).unwrap();
        assert_eq!(readded, both);
    }

    #[test]
    fn root_bound_state_opens_once_and_failed_append_is_transactional() {
        let container = tempfile::tempdir().unwrap();
        let workspace = tempfile::tempdir().unwrap();
        fs::write(workspace.path().join("a.parquet"), b"a").unwrap();
        fs::write(workspace.path().join("b.parquet"), b"b").unwrap();
        let lease = begin_graph_object_publication(container.path()).unwrap();
        let mut initial = GraphManifestState::empty();
        let (root, _) = append_graph_files_v2(
            &lease,
            workspace.path(),
            &mut initial,
            &[PathBuf::from("a.parquet")],
            &[],
        )
        .unwrap();
        let (mut reopened, open_evidence) =
            GraphManifestState::open(&lease, root.clone(), crate::GraphManifestLimits::default())
                .unwrap();
        assert_eq!(open_evidence.entries_examined, 1);
        let (_, append_evidence) = append_graph_files_v2(
            &lease,
            workspace.path(),
            &mut reopened,
            &[PathBuf::from("b.parquet")],
            &[],
        )
        .unwrap();
        assert_eq!(append_evidence.prior_entries_examined, 0);

        let before_root = reopened.root().unwrap().clone();
        let before_entries = reopened.entries().cloned().collect::<Vec<_>>();
        fs::remove_file(
            graph_object_path(container.path(), &before_root.root_node_sha256).unwrap(),
        )
        .unwrap();
        fs::write(workspace.path().join("c.parquet"), b"c").unwrap();
        assert!(
            append_graph_files_v2(
                &lease,
                workspace.path(),
                &mut reopened,
                &[PathBuf::from("c.parquet")],
                &[],
            )
            .is_err()
        );
        assert_eq!(reopened.root(), Some(&before_root));
        assert_eq!(
            reopened.entries().cloned().collect::<Vec<_>>(),
            before_entries
        );
    }

    #[test]
    fn patricia_s20_inventory_has_linear_resolve_work() {
        const FILES: usize = 277;
        let container = tempfile::tempdir().unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let paths = (0..FILES)
            .map(|ordinal| PathBuf::from(format!("shards/{ordinal:06}.parquet")))
            .collect::<Vec<_>>();
        fs::create_dir_all(workspace.path().join("shards")).unwrap();
        for (ordinal, path) in paths.iter().enumerate() {
            fs::write(workspace.path().join(path), ordinal.to_le_bytes()).unwrap();
        }
        let lease = begin_graph_object_publication(container.path()).unwrap();
        let mut state = GraphManifestState::empty();
        let (root, _) =
            append_graph_files_v2(&lease, workspace.path(), &mut state, &paths, &[]).unwrap();
        let (resolved, evidence) =
            crate::resolve_graph_manifest(&root, crate::GraphManifestLimits::default(), |digest| {
                read_graph_object_by_digest(container.path(), digest, 1024 * 1024)
            })
            .unwrap();
        assert_eq!(resolved.len(), FILES);
        assert!(evidence.segments_examined <= (2 * FILES - 1) as u64);
        assert!(evidence.work_units <= (4 * FILES - 2) as u64);
    }

    #[test]
    fn authenticated_publication_hashes_each_payload_once_with_constant_factor() {
        for files in [1_usize, 2, 4] {
            let container = tempfile::tempdir().unwrap();
            let workspace = tempfile::tempdir().unwrap();
            let sealed = (0..files)
                .map(|ordinal| {
                    let payload = vec![u8::try_from(ordinal + 1).unwrap(); 4096];
                    let relative_path = PathBuf::from(format!("shards/{ordinal}.parquet"));
                    let source = workspace.path().join(&relative_path);
                    fs::create_dir_all(source.parent().unwrap()).unwrap();
                    fs::write(source, &payload).unwrap();
                    AuthenticatedGraphFile {
                        relative_path,
                        byte_length: payload.len() as u64,
                        content_sha256: hex_digest(Sha256::digest(&payload).into()),
                    }
                })
                .collect::<Vec<_>>();
            let lease = begin_graph_object_publication(container.path()).unwrap();
            let mut state = GraphManifestState::empty();
            let (_, evidence) = append_authenticated_graph_files_v2(
                &lease,
                workspace.path(),
                &mut state,
                &sealed,
                &[],
            )
            .unwrap();
            assert_eq!(
                evidence.payload_bytes_hashed,
                u64::try_from(files * 4096).unwrap()
            );
            let mut replay_state = GraphManifestState::empty();
            let (_, replay_evidence) = append_authenticated_graph_files_v2(
                &lease,
                workspace.path(),
                &mut replay_state,
                &sealed,
                &[],
            )
            .unwrap();
            assert_eq!(
                replay_evidence.payload_bytes_hashed,
                u64::try_from(files * 4096).unwrap(),
                "CAS reuse must report its one mandatory physical authentication read"
            );
            drop(lease);
            let reclaimed =
                gc_graph_objects(container.path(), &[], crate::GraphManifestLimits::default())
                    .unwrap();
            assert!(reclaimed.objects_removed >= files as u64);
        }
    }

    #[test]
    fn authenticated_publication_still_rejects_corrupt_writer_output() {
        let container = tempfile::tempdir().unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let relative_path = PathBuf::from("corrupt.parquet");
        fs::write(workspace.path().join(&relative_path), b"mutated!").unwrap();
        let sealed = [AuthenticatedGraphFile {
            relative_path,
            byte_length: 8,
            content_sha256: hex_digest(Sha256::digest(b"original").into()),
        }];
        let lease = begin_graph_object_publication(container.path()).unwrap();
        let mut state = GraphManifestState::empty();
        assert!(
            append_authenticated_graph_files_v2(
                &lease,
                workspace.path(),
                &mut state,
                &sealed,
                &[],
            )
            .is_err()
        );
        assert!(state.root().is_none());
    }

    #[test]
    fn cas_copy_isolated_from_preexisting_writable_source_descriptor() {
        let container = tempfile::tempdir().unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let relative_path = PathBuf::from("owned.parquet");
        let payload = vec![9_u8; 4096];
        let workspace_path = workspace.path().join(&relative_path);
        fs::write(&workspace_path, &payload).unwrap();
        let mut hostile = std::fs::OpenOptions::new()
            .write(true)
            .open(&workspace_path)
            .unwrap();
        let sealed = [AuthenticatedGraphFile {
            relative_path,
            byte_length: payload.len() as u64,
            content_sha256: hex_digest(Sha256::digest(&payload).into()),
        }];
        let lease = begin_graph_object_publication(container.path()).unwrap();
        let mut state = GraphManifestState::empty();
        append_authenticated_graph_files_v2(&lease, workspace.path(), &mut state, &sealed, &[])
            .unwrap();
        assert!(workspace_path.exists());
        hostile.rewind().unwrap();
        hostile.write_all(&vec![0x44; payload.len()]).unwrap();
        hostile.sync_all().unwrap();
        verify_graph_object(
            container.path(),
            &sealed[0].content_sha256,
            payload.len() as u64,
        )
        .unwrap();
    }

    #[test]
    fn duplicate_and_conflicting_delta_inputs_fail_before_publication() {
        let container = tempfile::tempdir().unwrap();
        let workspace = tempfile::tempdir().unwrap();
        fs::write(workspace.path().join("a.parquet"), b"a").unwrap();
        let lease = begin_graph_object_publication(container.path()).unwrap();
        let mut state = GraphManifestState::empty();
        let path = PathBuf::from("a.parquet");
        assert!(
            append_graph_files_v2(
                &lease,
                workspace.path(),
                &mut state,
                &[path.clone(), path.clone()],
                &[],
            )
            .is_err()
        );
        assert!(state.root().is_none());
        assert!(
            append_graph_files_v2(
                &lease,
                workspace.path(),
                &mut state,
                &[path],
                &["a.parquet".into()],
            )
            .is_err()
        );
        assert!(state.root().is_none());
    }

    #[test]
    fn gc_traces_segment_and_payload_roots_before_sweeping() {
        let container = tempfile::tempdir().unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let lease = begin_graph_object_publication(container.path()).unwrap();
        let relative = PathBuf::from("topology/nodes/1-1.parquet");
        let source = workspace.path().join(&relative);
        fs::create_dir_all(source.parent().unwrap()).unwrap();
        fs::write(&source, b"node").unwrap();
        let mut state = GraphManifestState::empty();
        let (root, _) =
            append_graph_files_v2(&lease, workspace.path(), &mut state, &[relative], &[]).unwrap();
        drop(lease);
        let (orphan, _) = install_graph_object_bytes(container.path(), b"orphan").unwrap();
        let evidence = gc_graph_objects(
            container.path(),
            std::slice::from_ref(&root),
            crate::GraphManifestLimits::default(),
        )
        .unwrap();
        assert_eq!(evidence.objects_marked, 2);
        // The initial empty root and the explicit orphan are both unreachable.
        assert_eq!(evidence.objects_removed, 3);
        assert!(
            !graph_object_path(container.path(), &orphan)
                .unwrap()
                .exists()
        );

        let (another_orphan, _) = install_graph_object_bytes(container.path(), b"another").unwrap();
        corrupt_sealed_graph_object_for_test(
            &graph_object_path(container.path(), &root.root_node_sha256).unwrap(),
            b"tampered",
        );
        assert!(
            gc_graph_objects(
                container.path(),
                &[root],
                crate::GraphManifestLimits::default()
            )
            .is_err()
        );
        assert!(
            graph_object_path(container.path(), &another_orphan)
                .unwrap()
                .exists()
        );
    }
}
