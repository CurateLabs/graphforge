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

/// Project-relative root of immutable graph objects.
pub const GRAPH_OBJECTS_DIR: &str = "graph-objects";
const SHA256_DIR: &str = "sha256";
const TEMP_DIR: &str = "tmp";
const ACTIVE_DIR: &str = "active";
const LIFECYCLE_LOCK: &str = "lifecycle.lock";
const BUFFER_BYTES: usize = 64 * 1024;

/// Kernel-visible lease protecting CAS objects installed by one publication.
pub struct GraphObjectPublicationLease {
    cas: CasRoot,
    lease_name: std::ffi::OsString,
    lease_identity: graphforge_filesystem::FileIdentity,
    file: File,
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

impl CasRoot {
    fn open(root: &Path) -> Result<Self, GfError> {
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
                self.objects
                    .open_child_file(std::ffi::OsStr::new(LIFECYCLE_LOCK))
                    .and_then(|file| {
                        (graphforge_filesystem::file_identity(&file)? == self.lifecycle_identity)
                            .then_some(())
                            .ok_or_else(|| std::io::Error::other("lifecycle identity changed"))
                    })
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

impl Drop for GraphObjectGcGuard {
    fn drop(&mut self) {
        let _ = crate::file_lock::unlock(&self.cas.lifecycle);
        let _ = self.cas.objects.unlock();
    }
}

impl Drop for GraphObjectPublicationLease {
    fn drop(&mut self) {
        let _ = crate::file_lock::unlock(&self.file);
        let _ = self
            .cas
            .active
            .unlink_child_if_identity(&self.lease_name, self.lease_identity);
        let _ = crate::file_lock::unlock(&self.cas.lifecycle);
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
    let cas = CasRoot::open(root)?;
    cas.objects
        .lock_shared()
        .map_err(|error| storage("lock graph object directory for publication", root, error))?;
    crate::file_lock::lock_shared(&cas.lifecycle)
        .inspect_err(|_| {
            let _ = cas.objects.unlock();
        })
        .map_err(|error| storage("lock graph object publication lifecycle", root, error))?;
    cas.revalidate_named()?;
    let lease_name = std::ffi::OsString::from(format!("{}.lock", Uuid::new_v4().hyphenated()));
    let file = cas
        .active
        .create_child_file(&lease_name)
        .map_err(|error| storage("create graph object publication lease", root, error))?;
    let lease_identity = graphforge_filesystem::file_identity(&file)
        .map_err(|error| storage("inspect graph object publication lease", root, error))?;
    crate::file_lock::lock_exclusive(&file)
        .map_err(|error| storage("lock graph object publication lease", root, error))?;
    file.sync_all()
        .map_err(|error| storage("sync graph object publication lease", root, error))?;
    cas.active
        .sync()
        .map_err(|error| storage("sync graph object active directory", root, error))?;
    Ok(GraphObjectPublicationLease {
        cas,
        lease_name,
        lease_identity,
        file,
    })
}

/// Return true when any live CAS publication lease prevents safe sweeping.
/// Unlocked lease files are crash residue and are removed while the caller
/// holds the project writer/recovery lock.
pub fn graph_object_publication_is_live(root: &Path) -> Result<bool, GfError> {
    let cas = CasRoot::open(root)?;
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
            cas.active
                .unlink_child_if_identity(&entry, identity)
                .map_err(|error| storage("remove stale graph object lease", root, error))?;
        } else {
            live = true;
        }
    }
    Ok(live)
}

/// Exact physical work for one object installation.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GraphObjectInstallEvidence {
    /// Source payload bytes read and hashed.
    pub bytes_hashed: u64,
    /// New physical bytes installed into the object store.
    pub bytes_installed: u64,
    /// Whether an already installed exact object satisfied the request.
    pub reused_existing: bool,
}

/// One-time v1 expanded-tree to v2 object-store migration evidence.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GraphFilesMigrationEvidence {
    /// Payload objects examined.
    pub payload_objects: u64,
    /// Source payload bytes hashed.
    pub payload_bytes_hashed: u64,
    /// New physical payload and segment bytes installed.
    pub bytes_installed: u64,
}

/// Exact incremental publication work; prior manifest entries are supplied by
/// the owning facade's pinned cache and are never enumerated here.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GraphFilesAppendEvidence {
    /// New/replaced and tombstoned descriptors examined.
    pub changed_entries_examined: u64,
    /// Prior descriptors examined by publication (always zero).
    pub prior_entries_examined: u64,
    /// New/replaced payload bytes hashed, including verification passes.
    pub payload_bytes_hashed: u64,
    /// New physical object bytes installed.
    pub bytes_installed: u64,
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
    let cas = CasRoot::open(root)?;
    cas.objects
        .lock_exclusive()
        .map_err(|error| storage("lock graph object directory for GC", root, error))?;
    crate::file_lock::lock_exclusive(&cas.lifecycle)
        .inspect_err(|_| {
            let _ = cas.objects.unlock();
        })
        .map_err(|error| storage("lock graph object GC lifecycle", root, error))?;
    cas.revalidate_named()?;
    Ok(GraphObjectGcGuard { cas })
}

pub(crate) fn try_begin_graph_object_gc(
    root: &Path,
) -> Result<Option<GraphObjectGcGuard>, GfError> {
    let cas = CasRoot::open(root)?;
    if !cas
        .objects
        .try_lock_exclusive()
        .map_err(|error| storage("try graph object directory for GC", root, error))?
    {
        return Ok(None);
    }
    if !crate::file_lock::try_lock_exclusive(&cas.lifecycle)
        .map_err(|error| storage("try graph object GC lifecycle", root, error))?
    {
        let _ = cas.objects.unlock();
        return Ok(None);
    }
    cas.revalidate_named()?;
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
pub fn append_graph_files_v2(
    lease: &GraphObjectPublicationLease,
    workspace: &Path,
    previous_root: Option<&GraphFilesRootV2>,
    live_entries: &mut BTreeMap<String, crate::GraphFileEntry>,
    sealed_paths: &[PathBuf],
    tombstones: &[String],
) -> Result<(GraphFilesRootV2, GraphFilesAppendEvidence), GfError> {
    validate_publication_identity(lease)?;
    let mut additions = Vec::with_capacity(sealed_paths.len());
    let mut evidence = GraphFilesAppendEvidence::default();
    let mut logical_byte_length = previous_root.map_or(0, |root| root.logical_byte_length);
    for relative in sealed_paths {
        validate_logical_path(relative)?;
        let source = workspace.join(relative);
        let metadata = fs::symlink_metadata(&source)
            .map_err(|error| storage("inspect sealed graph file", &source, error))?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(validation("sealed graph path is not a regular file"));
        }
        let digest = hash_regular_file(&source)?;
        let digest = hex_digest(digest);
        let installed =
            install_graph_object_file_with_lease(lease, &source, &digest, metadata.len())?;
        evidence.payload_bytes_hashed = evidence
            .payload_bytes_hashed
            .saturating_add(metadata.len())
            .saturating_add(installed.bytes_hashed);
        evidence.bytes_installed = evidence
            .bytes_installed
            .saturating_add(installed.bytes_installed);
        let relative_path = relative
            .to_str()
            .ok_or_else(|| validation("sealed graph path is not UTF-8"))?
            .to_owned();
        let entry = crate::GraphFileEntry {
            relative_path: relative_path.clone(),
            byte_length: metadata.len(),
            content_sha256: digest,
            role: crate::graph_files::infer_role(relative),
        };
        if let Some(previous) = live_entries.insert(relative_path, entry.clone()) {
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
    tombstones.dedup();
    for path in &tombstones {
        validate_logical_path(Path::new(path))?;
        if let Some(previous) = live_entries.remove(path) {
            logical_byte_length = logical_byte_length.saturating_sub(previous.byte_length);
        }
    }
    evidence.changed_entries_examined =
        u64::try_from(additions.len().saturating_add(tombstones.len())).unwrap_or(u64::MAX);
    let mut root_digest = match previous_root {
        Some(previous) => previous.root_node_sha256.clone(),
        None => install_manifest_node(lease, &empty_branch(0), &mut evidence.bytes_installed)?,
    };
    for entry in additions {
        let relative_path = entry.relative_path.clone();
        root_digest = update_manifest_path(
            lease,
            Some(&root_digest),
            0,
            &relative_path,
            Some(entry),
            &mut evidence.bytes_installed,
        )?
        .ok_or_else(|| validation("radix update unexpectedly removed the root"))?;
    }
    for path in tombstones {
        root_digest = update_manifest_path(
            lease,
            Some(&root_digest),
            0,
            &path,
            None,
            &mut evidence.bytes_installed,
        )?
        .unwrap_or(install_manifest_node(
            lease,
            &empty_branch(0),
            &mut evidence.bytes_installed,
        )?);
    }
    Ok((
        GraphFilesRootV2 {
            format: GRAPH_FILES_V2_FORMAT.into(),
            format_version: GRAPH_FILES_V2_VERSION,
            root_node_sha256: root_digest,
            logical_file_count: u64::try_from(live_entries.len()).unwrap_or(u64::MAX),
            logical_byte_length,
        },
        evidence,
    ))
}

/// Import a verified v1 graph tree into a self-contained v2 radix root.
pub fn migrate_graph_files_v1_to_v2(
    lease: &GraphObjectPublicationLease,
    graph_root: &Path,
    inventory: &GraphFilesInventory,
) -> Result<(GraphFilesRootV2, GraphFilesMigrationEvidence), GfError> {
    validate_publication_identity(lease)?;
    let mut evidence = GraphFilesMigrationEvidence::default();
    for entry in &inventory.files {
        let installed = install_graph_object_file_with_lease(
            lease,
            &graph_root.join(&entry.relative_path),
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
        root_digest = update_manifest_path(
            lease,
            Some(&root_digest),
            0,
            &entry.relative_path,
            Some(entry.clone()),
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
    let path_digest = crate::graph_manifest::logical_path_digest(path);
    if depth == GRAPH_RADIX_DEPTH {
        let mut entries = match current_digest {
            Some(digest) => match load_manifest_node(lease, digest, depth)?.kind {
                GraphManifestNodeKind::Leaf {
                    path_sha256,
                    entries,
                } => {
                    if path_sha256 != hex_digest(path_digest) {
                        return Err(validation("graph manifest collision leaf digest mismatch"));
                    }
                    entries
                }
                GraphManifestNodeKind::Branch { .. } => {
                    return Err(validation("graph manifest terminal node is not a leaf"));
                }
            },
            None => Vec::new(),
        };
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
                }
            }
        }
        if entries.is_empty() {
            return Ok(None);
        }
        let node = GraphManifestNode {
            format: GRAPH_MANIFEST_NODE_FORMAT.into(),
            format_version: GRAPH_MANIFEST_NODE_VERSION,
            depth,
            kind: GraphManifestNodeKind::Leaf {
                path_sha256: hex_digest(path_digest),
                entries,
            },
        };
        return install_manifest_node(lease, &node, bytes_installed).map(Some);
    }

    let mut children = match current_digest {
        Some(digest) => match load_manifest_node(lease, digest, depth)?.kind {
            GraphManifestNodeKind::Branch { children } => children,
            GraphManifestNodeKind::Leaf { .. } => {
                return Err(validation(
                    "graph manifest non-terminal node is not a branch",
                ));
            }
        },
        None => BTreeMap::new(),
    };
    let nibble = format!(
        "{:x}",
        crate::graph_manifest::radix_nibble(&path_digest, depth)
    );
    let child = update_manifest_path(
        lease,
        children.get(&nibble).map(String::as_str),
        depth + 1,
        path,
        replacement,
        bytes_installed,
    )?;
    match child {
        Some(digest) => {
            children.insert(nibble, digest);
        }
        None => {
            children.remove(&nibble);
        }
    }
    if children.is_empty() && depth != 0 {
        return Ok(None);
    }
    install_manifest_node(
        lease,
        &GraphManifestNode {
            format: GRAPH_MANIFEST_NODE_FORMAT.into(),
            format_version: GRAPH_MANIFEST_NODE_VERSION,
            depth,
            kind: GraphManifestNodeKind::Branch { children },
        },
        bytes_installed,
    )
    .map(Some)
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
    install_object(&lease.cas, &digest, expected_length, |file| {
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
        Ok(expected_length)
    })
    .map(|evidence| (digest, evidence))
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
    install_object(&lease.cas, expected_digest, expected_length, |output| {
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
            output.write_all(&buffer[..read]).map_err(|error| {
                storage(
                    "write temporary graph object",
                    &lease.cas.diagnostic_root,
                    error,
                )
            })?;
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
    })
}

/// Read and cryptographically verify an immutable object.
pub fn read_graph_object(
    root: &Path,
    digest: &str,
    expected_length: u64,
) -> Result<Vec<u8>, GfError> {
    let cas = CasRoot::open(root)?;
    let mut file = cas.open_digest(digest)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|error| storage("read stable graph object", root, error))?;
    verify_object_bytes(digest, expected_length, &bytes)?;
    Ok(bytes)
}

/// Stream-verify a payload object without retaining it in memory.
pub fn verify_graph_object(root: &Path, digest: &str, expected_length: u64) -> Result<(), GfError> {
    let cas = CasRoot::open(root)?;
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

/// Materialize a verified logical inventory as hard links to immutable CAS
/// objects. The target must be empty.
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
        materialize_from_cas(&lease.cas, &target_directory, entry)?;
        evidence.files_reused = evidence.files_reused.saturating_add(1);
        evidence.bytes_reused = evidence.bytes_reused.saturating_add(entry.byte_length);
    }
    lease.revalidate_for_publish()?;
    Ok(evidence)
}

fn materialize_from_cas(
    cas: &CasRoot,
    target: &StableDirectory,
    entry: &crate::GraphFileEntry,
) -> Result<(), GfError> {
    validate_logical_path(Path::new(&entry.relative_path))?;
    let bucket = cas.digest_bucket(&entry.content_sha256, false)?;
    let source_name = std::ffi::OsStr::new(&entry.content_sha256[2..]);
    let source = bucket
        .open_child_file(source_name)
        .map_err(|error| storage("open materialization source", &cas.diagnostic_root, error))?;
    verify_file(
        source.try_clone().map_err(|error| {
            storage("clone materialization source", &cas.diagnostic_root, error)
        })?,
        &entry.content_sha256,
        entry.byte_length,
        &cas.diagnostic_root,
    )?;
    let source_identity = graphforge_filesystem::file_identity(&source).map_err(|error| {
        storage(
            "identify materialization source",
            &cas.diagnostic_root,
            error,
        )
    })?;
    let path = Path::new(&entry.relative_path);
    let mut parent = target
        .create_child_directory(std::ffi::OsStr::new("files"))
        .map_err(|error| {
            storage(
                "create stable materialization files root",
                &cas.diagnostic_root,
                error,
            )
        })?;
    let mut components = path.components().peekable();
    while let Some(component) = components.next() {
        let Component::Normal(name) = component else {
            return Err(validation("invalid materialization path component"));
        };
        if components.peek().is_some() {
            parent = parent.create_child_directory(name).map_err(|error| {
                storage(
                    "create stable materialization directory",
                    &cas.diagnostic_root,
                    error,
                )
            })?;
        } else {
            let (installed, installed_identity) = bucket
                .link_child_into(source_name, &source, source_identity, &parent, name)
                .map_err(|error| {
                    storage(
                        "install stable materialized object",
                        &cas.diagnostic_root,
                        error,
                    )
                })?;
            if let Err(error) = verify_file(
                installed,
                &entry.content_sha256,
                entry.byte_length,
                &cas.diagnostic_root,
            ) {
                let _ = parent.unlink_child_if_identity(name, installed_identity);
                return Err(error);
            }
        }
    }
    Ok(())
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
    let cas = CasRoot::open(root)?;
    read_graph_object_by_digest_from_cas(&cas, digest, max_length)
}

fn read_graph_object_by_digest_from_cas(
    cas: &CasRoot,
    digest: &str,
    max_length: u64,
) -> Result<Vec<u8>, GfError> {
    let mut file = cas.open_digest(digest)?;
    let metadata = file
        .metadata()
        .map_err(|error| storage("inspect stable graph object", &cas.diagnostic_root, error))?;
    if !metadata.is_file() || metadata.len() > max_length {
        return Err(validation(
            "graph object exceeds admitted length or is not regular",
        ));
    }
    let mut bytes = Vec::with_capacity(usize::try_from(metadata.len()).unwrap_or(0));
    file.read_to_end(&mut bytes)
        .map_err(|error| storage("read stable graph object", &cas.diagnostic_root, error))?;
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
    write_temporary: F,
) -> Result<GraphObjectInstallEvidence, GfError>
where
    F: FnOnce(&mut File) -> Result<u64, GfError>,
{
    validate_digest(digest)?;
    let bucket = cas.digest_bucket(digest, true)?;
    let destination_name = std::ffi::OsStr::new(&digest[2..]);
    if let Ok(file) = bucket.open_child_file(destination_name) {
        verify_file(file, digest, expected_length, &cas.diagnostic_root)?;
        return Ok(GraphObjectInstallEvidence {
            reused_existing: true,
            ..GraphObjectInstallEvidence::default()
        });
    }
    let temporary_name = std::ffi::OsString::from(Uuid::new_v4().hyphenated().to_string());
    let mut temporary = cas
        .tmp
        .create_child_file(&temporary_name)
        .map_err(|error| {
            storage(
                "create stable temporary graph object",
                &cas.diagnostic_root,
                error,
            )
        })?;
    let temporary_identity = graphforge_filesystem::file_identity(&temporary).map_err(|error| {
        storage(
            "inspect temporary graph object",
            &cas.diagnostic_root,
            error,
        )
    })?;
    let bytes_hashed = write_temporary(&mut temporary)?;
    temporary
        .rewind()
        .map_err(|error| storage("rewind temporary graph object", &cas.diagnostic_root, error))?;
    verify_file(
        temporary.try_clone().map_err(|error| {
            storage("clone temporary graph object", &cas.diagnostic_root, error)
        })?,
        digest,
        expected_length,
        &cas.diagnostic_root,
    )?;
    let installed = if let Ok((_installed, _identity)) = cas.tmp.link_child_into(
        &temporary_name,
        &temporary,
        temporary_identity,
        &bucket,
        destination_name,
    ) {
        true
    } else {
        let existing = bucket.open_child_file(destination_name).map_err(|error| {
            storage(
                "open concurrently installed graph object",
                &cas.diagnostic_root,
                error,
            )
        })?;
        verify_file(existing, digest, expected_length, &cas.diagnostic_root)?;
        false
    };
    cas.tmp
        .unlink_child_if_identity(&temporary_name, temporary_identity)
        .map_err(|error| {
            storage(
                "remove stable temporary graph object",
                &cas.diagnostic_root,
                error,
            )
        })?;
    bucket.sync().map_err(|error| {
        storage(
            "sync stable graph object bucket",
            &cas.diagnostic_root,
            error,
        )
    })?;
    Ok(GraphObjectInstallEvidence {
        bytes_hashed,
        bytes_installed: if installed { expected_length } else { 0 },
        reused_existing: !installed,
    })
}

fn verify_file(
    mut file: File,
    digest: &str,
    expected_length: u64,
    diagnostic: &Path,
) -> Result<(), GfError> {
    let metadata = file
        .metadata()
        .map_err(|error| storage("inspect graph object handle", diagnostic, error))?;
    if !metadata.is_file() || metadata.len() != expected_length {
        return Err(validation(
            "graph object handle is not the declared regular file",
        ));
    }
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; BUFFER_BYTES];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| storage("read graph object handle", diagnostic, error))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    if hex_digest(hasher.finalize().into()) != digest {
        return Err(validation("graph object digest does not match its address"));
    }
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
        let (_, second) = install_graph_object_bytes(root.path(), b"payload").unwrap();
        assert!(second.reused_existing);
        assert_eq!(
            read_graph_object(root.path(), &digest, 7).unwrap(),
            b"payload"
        );

        fs::write(graph_object_path(root.path(), &digest).unwrap(), b"corrupt").unwrap();
        assert!(install_graph_object_bytes(root.path(), b"payload").is_err());
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

    #[test]
    fn repeated_v2_appends_examine_only_changed_descriptors() {
        let container = tempfile::tempdir().unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let lease = begin_graph_object_publication(container.path()).unwrap();
        let mut live = BTreeMap::new();
        let mut previous = None;
        for ordinal in 0_u8..8 {
            let relative = PathBuf::from(format!(
                "topology/edges/knows/{ordinal:020}-{ordinal:020}.parquet"
            ));
            let path = workspace.path().join(&relative);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(&path, [ordinal]).unwrap();
            let (root, evidence) = append_graph_files_v2(
                &lease,
                workspace.path(),
                previous.as_ref(),
                &mut live,
                &[relative],
                &[],
            )
            .unwrap();
            assert_eq!(evidence.changed_entries_examined, 1);
            assert_eq!(evidence.prior_entries_examined, 0);
            previous = Some(root);
        }
        let root = previous.unwrap();
        let (resolved, evidence) =
            crate::resolve_graph_manifest(&root, crate::GraphManifestLimits::default(), |digest| {
                read_graph_object_by_digest(container.path(), digest, 1024 * 1024)
            })
            .unwrap();
        assert_eq!(resolved.len(), 8);
        assert!(evidence.segments_examined <= 1 + u64::from(GRAPH_RADIX_DEPTH) * 8);

        let deleted = "topology/edges/knows/00000000000000000003-00000000000000000003.parquet";
        let (root, delete_evidence) = append_graph_files_v2(
            &lease,
            workspace.path(),
            Some(&root),
            &mut live,
            &[],
            &[deleted.into()],
        )
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
        let mut first_live = BTreeMap::new();
        let (first_root, _) = append_graph_files_v2(
            &first_lease,
            workspace.path(),
            None,
            &mut first_live,
            &paths,
            &[],
        )
        .unwrap();

        let second = tempfile::tempdir().unwrap();
        let second_lease = begin_graph_object_publication(second.path()).unwrap();
        let mut reversed = paths.clone();
        reversed.reverse();
        let mut second_live = BTreeMap::new();
        let (second_root, _) = append_graph_files_v2(
            &second_lease,
            workspace.path(),
            None,
            &mut second_live,
            &reversed,
            &[],
        )
        .unwrap();
        assert_eq!(first_root, second_root);
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
        let mut live = BTreeMap::new();
        let (root, _) =
            append_graph_files_v2(&lease, workspace.path(), None, &mut live, &[relative], &[])
                .unwrap();
        drop(lease);
        let (orphan, _) = install_graph_object_bytes(container.path(), b"orphan").unwrap();
        let evidence = gc_graph_objects(
            container.path(),
            std::slice::from_ref(&root),
            crate::GraphManifestLimits::default(),
        )
        .unwrap();
        assert_eq!(evidence.objects_marked, u64::from(GRAPH_RADIX_DEPTH) + 2);
        // The initial empty root and the explicit orphan are both unreachable.
        assert_eq!(evidence.objects_removed, 2);
        assert!(
            !graph_object_path(container.path(), &orphan)
                .unwrap()
                .exists()
        );

        let (another_orphan, _) = install_graph_object_bytes(container.path(), b"another").unwrap();
        fs::write(
            graph_object_path(container.path(), &root.root_node_sha256).unwrap(),
            b"tampered",
        )
        .unwrap();
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
