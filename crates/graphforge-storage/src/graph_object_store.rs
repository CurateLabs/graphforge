//! Project-level immutable content-addressed graph objects.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
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

/// Project-relative root of immutable graph objects.
pub const GRAPH_OBJECTS_DIR: &str = "graph-objects";
const SHA256_DIR: &str = "sha256";
const TEMP_DIR: &str = "tmp";
const ACTIVE_DIR: &str = "active";
const LIFECYCLE_LOCK: &str = "lifecycle.lock";
const BUFFER_BYTES: usize = 64 * 1024;

/// Kernel-visible lease protecting CAS objects installed by one publication.
pub struct GraphObjectPublicationLease {
    root: PathBuf,
    path: PathBuf,
    file: File,
    lifecycle_directory: File,
    lifecycle_directory_path: PathBuf,
    lifecycle: File,
    lifecycle_path: PathBuf,
}

/// Exclusive guard spanning GC root discovery through sweep.
pub(crate) struct GraphObjectGcGuard {
    root: PathBuf,
    lifecycle_directory: File,
    lifecycle_directory_path: PathBuf,
    lifecycle: File,
    lifecycle_path: PathBuf,
}

impl Drop for GraphObjectGcGuard {
    fn drop(&mut self) {
        let _ = crate::file_lock::unlock(&self.lifecycle);
        let _ = crate::file_lock::unlock(&self.lifecycle_directory);
    }
}

impl Drop for GraphObjectPublicationLease {
    fn drop(&mut self) {
        let _ = crate::file_lock::unlock(&self.file);
        let _ = fs::remove_file(&self.path);
        let _ = crate::file_lock::unlock(&self.lifecycle);
        let _ = crate::file_lock::unlock(&self.lifecycle_directory);
    }
}

/// Begin a CAS installation attempt and hold its lease through CURRENT.
pub fn begin_graph_object_publication(root: &Path) -> Result<GraphObjectPublicationLease, GfError> {
    let object_root = root.join(GRAPH_OBJECTS_DIR);
    fs::create_dir_all(&object_root)
        .map_err(|error| storage("create graph object directory", &object_root, error))?;
    let (lifecycle_directory, lifecycle, lifecycle_path) = open_lifecycle_lock(&object_root)?;
    crate::file_lock::lock_shared(&lifecycle_directory)
        .map_err(|error| storage("lock graph object lifecycle directory", &object_root, error))?;
    crate::file_lock::lock_shared(&lifecycle).map_err(|error| {
        storage(
            "lock graph object publication lifecycle",
            &lifecycle_path,
            error,
        )
    })?;
    validate_directory_identity(&lifecycle_directory, &object_root)?;
    validate_lock_identity(&lifecycle, &lifecycle_path)?;
    let active = object_root.join(ACTIVE_DIR);
    fs::create_dir_all(&active)
        .map_err(|error| storage("create graph object active directory", &active, error))?;
    let path = active.join(format!("{}.lock", Uuid::new_v4().hyphenated()));
    let file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)
        .map_err(|error| storage("create graph object publication lease", &path, error))?;
    crate::file_lock::lock_exclusive(&file)
        .map_err(|error| storage("lock graph object publication lease", &path, error))?;
    file.sync_all()
        .map_err(|error| storage("sync graph object publication lease", &path, error))?;
    sync_directory(&active)?;
    Ok(GraphObjectPublicationLease {
        root: root.to_path_buf(),
        path,
        file,
        lifecycle_directory,
        lifecycle_directory_path: object_root,
        lifecycle,
        lifecycle_path,
    })
}

/// Return true when any live CAS publication lease prevents safe sweeping.
/// Unlocked lease files are crash residue and are removed while the caller
/// holds the project writer/recovery lock.
pub fn graph_object_publication_is_live(root: &Path) -> Result<bool, GfError> {
    let active = root.join(GRAPH_OBJECTS_DIR).join(ACTIVE_DIR);
    let entries = match fs::read_dir(&active) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(storage(
                "read graph object active directory",
                &active,
                error,
            ));
        }
    };
    let mut live = false;
    for entry in entries {
        let entry = entry.map_err(|error| storage("read graph object lease", &active, error))?;
        let path = entry.path();
        let name = entry
            .file_name()
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
        let metadata = fs::symlink_metadata(&path)
            .map_err(|error| storage("inspect graph object lease", &path, error))?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(validation(
                "graph object active directory contains an invalid entry",
            ));
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|error| storage("open graph object lease", &path, error))?;
        if crate::file_lock::try_lock_exclusive(&file)
            .map_err(|error| storage("probe graph object lease", &path, error))?
        {
            crate::file_lock::unlock(&file)
                .map_err(|error| storage("unlock graph object lease", &path, error))?;
            fs::remove_file(&path)
                .map_err(|error| storage("remove stale graph object lease", &path, error))?;
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
    let (root, lifecycle_directory, lifecycle, lifecycle_path) = open_graph_object_lifecycle(root)?;
    crate::file_lock::lock_exclusive(&lifecycle_directory)
        .map_err(|error| storage("lock graph object GC directory", &root, error))?;
    crate::file_lock::lock_exclusive(&lifecycle)
        .map_err(|error| storage("lock graph object GC lifecycle", &lifecycle_path, error))?;
    validate_directory_identity(&lifecycle_directory, &root.join(GRAPH_OBJECTS_DIR))?;
    validate_lock_identity(&lifecycle, &lifecycle_path)?;
    let lifecycle_directory_path = root.join(GRAPH_OBJECTS_DIR);
    Ok(GraphObjectGcGuard {
        root,
        lifecycle_directory,
        lifecycle_directory_path,
        lifecycle,
        lifecycle_path,
    })
}

pub(crate) fn try_begin_graph_object_gc(
    root: &Path,
) -> Result<Option<GraphObjectGcGuard>, GfError> {
    let (root, lifecycle_directory, lifecycle, lifecycle_path) = open_graph_object_lifecycle(root)?;
    if !crate::file_lock::try_lock_exclusive(&lifecycle_directory)
        .map_err(|error| storage("try graph object GC directory", &root, error))?
    {
        return Ok(None);
    }
    if !crate::file_lock::try_lock_exclusive(&lifecycle)
        .map_err(|error| storage("try graph object GC lifecycle", &lifecycle_path, error))?
    {
        let _ = crate::file_lock::unlock(&lifecycle_directory);
        return Ok(None);
    }
    validate_directory_identity(&lifecycle_directory, &root.join(GRAPH_OBJECTS_DIR))?;
    validate_lock_identity(&lifecycle, &lifecycle_path)?;
    let lifecycle_directory_path = root.join(GRAPH_OBJECTS_DIR);
    Ok(Some(GraphObjectGcGuard {
        root,
        lifecycle_directory,
        lifecycle_directory_path,
        lifecycle,
        lifecycle_path,
    }))
}

fn open_graph_object_lifecycle(root: &Path) -> Result<(PathBuf, File, File, PathBuf), GfError> {
    let object_root = root.join(GRAPH_OBJECTS_DIR);
    fs::create_dir_all(&object_root)
        .map_err(|error| storage("create graph object directory", &object_root, error))?;
    let (directory, lifecycle, lifecycle_path) = open_lifecycle_lock(&object_root)?;
    Ok((root.to_path_buf(), directory, lifecycle, lifecycle_path))
}

#[cfg(unix)]
fn open_lifecycle_lock(object_root: &Path) -> Result<(File, File, PathBuf), GfError> {
    use rustix::fs::{AtFlags, Mode, OFlags};

    let directory = rustix::fs::open(
        object_root,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|error| storage("open graph object lock directory", object_root, error))?;
    let descriptor = rustix::fs::openat(
        &directory,
        LIFECYCLE_LOCK,
        OFlags::RDWR | OFlags::CREATE | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::from_bits_truncate(0o600),
    )
    .map_err(|error| storage("open graph object lifecycle lock", object_root, error))?;
    let descriptor_stat = rustix::fs::fstat(&descriptor).map_err(|error| {
        storage(
            "inspect graph object lifecycle descriptor",
            object_root,
            error,
        )
    })?;
    let path_stat = rustix::fs::statat(&directory, LIFECYCLE_LOCK, AtFlags::SYMLINK_NOFOLLOW)
        .map_err(|error| storage("inspect graph object lifecycle path", object_root, error))?;
    if descriptor_stat.st_dev != path_stat.st_dev
        || descriptor_stat.st_ino != path_stat.st_ino
        || descriptor_stat.st_nlink != 1
        || path_stat.st_nlink != 1
    {
        return Err(validation(
            "graph object lifecycle lock identity is unstable",
        ));
    }
    let file: File = descriptor.into();
    let directory: File = directory.into();
    file.sync_all()
        .map_err(|error| storage("sync graph object lifecycle lock", object_root, error))?;
    sync_directory(object_root)?;
    Ok((directory, file, object_root.join(LIFECYCLE_LOCK)))
}

#[cfg(not(unix))]
fn open_lifecycle_lock(object_root: &Path) -> Result<(File, File, PathBuf), GfError> {
    let path = object_root.join(LIFECYCLE_LOCK);
    let file = crate::project_publication::open_regular_lock(&path)?;
    file.sync_all()
        .map_err(|error| storage("sync graph object lifecycle lock", &path, error))?;
    sync_directory(object_root)?;
    let directory = File::open(object_root)
        .map_err(|error| storage("open graph object lock directory", object_root, error))?;
    Ok((directory, file, path))
}

fn validate_lock_identity(file: &File, path: &Path) -> Result<(), GfError> {
    let descriptor = file
        .metadata()
        .map_err(|error| storage("inspect lifecycle lock descriptor", path, error))?;
    let named = fs::symlink_metadata(path)
        .map_err(|error| storage("inspect lifecycle lock path", path, error))?;
    if !descriptor.is_file() || !named.is_file() || named.file_type().is_symlink() {
        return Err(validation(
            "graph object lifecycle lock is not a regular file",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if descriptor.dev() != named.dev()
            || descriptor.ino() != named.ino()
            || descriptor.nlink() != 1
            || named.nlink() != 1
        {
            return Err(validation("graph object lifecycle lock identity changed"));
        }
    }
    Ok(())
}

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
    Ok(())
}

pub(crate) fn gc_graph_objects_guarded(
    guard: &GraphObjectGcGuard,
    roots: &[GraphFilesRootV2],
    limits: crate::GraphManifestLimits,
) -> Result<GraphObjectGcEvidence, GfError> {
    validate_directory_identity(&guard.lifecycle_directory, &guard.lifecycle_directory_path)?;
    validate_lock_identity(&guard.lifecycle, &guard.lifecycle_path)?;
    let root = &guard.root;
    let mut marked = BTreeSet::new();
    for graph_root in roots {
        let mut segment_digests = Vec::new();
        let (files, _) = crate::resolve_graph_manifest(graph_root, limits, |digest| {
            segment_digests.push(digest.to_owned());
            read_graph_object_by_digest(root, digest, 64 * 1024 * 1024)
        })?;
        marked.extend(segment_digests);
        marked.extend(files.into_iter().map(|entry| entry.content_sha256));
    }
    let mut candidates = Vec::new();
    let sha_root = root.join(GRAPH_OBJECTS_DIR).join(SHA256_DIR);
    if sha_root.exists() {
        for prefix in fs::read_dir(&sha_root)
            .map_err(|error| storage("read graph object prefixes", &sha_root, error))?
        {
            let prefix =
                prefix.map_err(|error| storage("read graph object prefix", &sha_root, error))?;
            if !prefix
                .file_type()
                .map_err(|error| storage("inspect graph object prefix", &prefix.path(), error))?
                .is_dir()
            {
                return Err(validation(
                    "graph object store contains a non-directory prefix",
                ));
            }
            let prefix_name = prefix
                .file_name()
                .into_string()
                .map_err(|_| validation("graph object prefix is not UTF-8"))?;
            for object in fs::read_dir(prefix.path())
                .map_err(|error| storage("read graph object bucket", &prefix.path(), error))?
            {
                let object = object
                    .map_err(|error| storage("read graph object entry", &prefix.path(), error))?;
                let file_type = object
                    .file_type()
                    .map_err(|error| storage("inspect graph object type", &object.path(), error))?;
                if !file_type.is_file() || file_type.is_symlink() {
                    return Err(validation(
                        "graph object bucket contains a non-regular object",
                    ));
                }
                let metadata = object.metadata().map_err(|error| {
                    storage("inspect graph object entry", &object.path(), error)
                })?;
                let suffix = object
                    .file_name()
                    .into_string()
                    .map_err(|_| validation("graph object name is not UTF-8"))?;
                let digest = format!("{prefix_name}{suffix}");
                validate_digest(&digest)?;
                if !marked.contains(&digest) {
                    candidates.push((object.path(), metadata.len()));
                }
            }
        }
    }
    let mut evidence = GraphObjectGcEvidence {
        objects_marked: u64::try_from(marked.len()).unwrap_or(u64::MAX),
        ..GraphObjectGcEvidence::default()
    };
    for (path, bytes) in candidates {
        fs::remove_file(&path)
            .map_err(|error| storage("remove unreachable graph object", &path, error))?;
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
    validate_directory_identity(&lease.lifecycle_directory, &lease.lifecycle_directory_path)?;
    validate_lock_identity(&lease.lifecycle, &lease.lifecycle_path)?;
    let root = &lease.root;
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
        let installed = install_graph_object_file(root, &source, &digest, metadata.len())?;
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
        None => install_manifest_node(root, &empty_branch(0), &mut evidence.bytes_installed)?,
    };
    for entry in additions {
        let relative_path = entry.relative_path.clone();
        root_digest = update_manifest_path(
            root,
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
            root,
            Some(&root_digest),
            0,
            &path,
            None,
            &mut evidence.bytes_installed,
        )?
        .unwrap_or(install_manifest_node(
            root,
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
    validate_directory_identity(&lease.lifecycle_directory, &lease.lifecycle_directory_path)?;
    validate_lock_identity(&lease.lifecycle, &lease.lifecycle_path)?;
    let root = &lease.root;
    let mut evidence = GraphFilesMigrationEvidence::default();
    for entry in &inventory.files {
        let installed = install_graph_object_file(
            root,
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
        install_manifest_node(root, &empty_branch(0), &mut evidence.bytes_installed)?;
    for entry in &inventory.files {
        root_digest = update_manifest_path(
            root,
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
    root: &Path,
    node: &GraphManifestNode,
    bytes_installed: &mut u64,
) -> Result<String, GfError> {
    let bytes = crate::encode_graph_manifest_node(node)?;
    let (digest, evidence) = install_graph_object_bytes(root, &bytes)?;
    *bytes_installed = bytes_installed.saturating_add(evidence.bytes_installed);
    Ok(digest)
}

fn load_manifest_node(
    root: &Path,
    digest: &str,
    expected_depth: u8,
) -> Result<GraphManifestNode, GfError> {
    let bytes = read_graph_object_by_digest(root, digest, 64 * 1024 * 1024)?;
    let node = crate::decode_graph_manifest_node(&bytes)?;
    if node.depth != expected_depth {
        return Err(validation(
            "graph manifest radix depth mismatch during update",
        ));
    }
    Ok(node)
}

fn update_manifest_path(
    root: &Path,
    current_digest: Option<&str>,
    depth: u8,
    path: &str,
    replacement: Option<crate::GraphFileEntry>,
    bytes_installed: &mut u64,
) -> Result<Option<String>, GfError> {
    let path_digest = crate::graph_manifest::logical_path_digest(path);
    if depth == GRAPH_RADIX_DEPTH {
        let mut entries = match current_digest {
            Some(digest) => match load_manifest_node(root, digest, depth)?.kind {
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
        return install_manifest_node(root, &node, bytes_installed).map(Some);
    }

    let mut children = match current_digest {
        Some(digest) => match load_manifest_node(root, digest, depth)?.kind {
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
        root,
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
        root,
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
    let digest = hex_digest(Sha256::digest(bytes).into());
    let expected_length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    install_object(root, &digest, expected_length, |temporary| {
        let mut file = create_new(temporary)?;
        file.write_all(bytes)
            .map_err(|error| storage("write temporary graph object", temporary, error))?;
        file.sync_all()
            .map_err(|error| storage("fsync temporary graph object", temporary, error))?;
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
    validate_digest(expected_digest)?;
    let metadata = fs::symlink_metadata(source)
        .map_err(|error| storage("inspect graph object source", source, error))?;
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() != expected_length
    {
        return Err(validation(
            "graph object source is not the declared regular file",
        ));
    }
    install_object(root, expected_digest, expected_length, |temporary| {
        let mut input = File::open(source)
            .map_err(|error| storage("open graph object source", source, error))?;
        let mut output = create_new(temporary)?;
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
            output
                .write_all(&buffer[..read])
                .map_err(|error| storage("write temporary graph object", temporary, error))?;
            hasher.update(&buffer[..read]);
            total = total.saturating_add(u64::try_from(read).unwrap_or(u64::MAX));
        }
        if total != expected_length || hex_digest(hasher.finalize().into()) != expected_digest {
            return Err(validation(
                "graph object source digest or length changed during install",
            ));
        }
        output
            .sync_all()
            .map_err(|error| storage("fsync temporary graph object", temporary, error))?;
        Ok(total)
    })
}

/// Read and cryptographically verify an immutable object.
pub fn read_graph_object(
    root: &Path,
    digest: &str,
    expected_length: u64,
) -> Result<Vec<u8>, GfError> {
    let path = graph_object_path(root, digest)?;
    let bytes = fs::read(&path).map_err(|error| storage("read graph object", &path, error))?;
    verify_object_bytes(digest, expected_length, &bytes)?;
    Ok(bytes)
}

/// Stream-verify a payload object without retaining it in memory.
pub fn verify_graph_object(root: &Path, digest: &str, expected_length: u64) -> Result<(), GfError> {
    let path = graph_object_path(root, digest)?;
    let metadata = fs::symlink_metadata(&path)
        .map_err(|error| storage("inspect graph object", &path, error))?;
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() != expected_length
    {
        return Err(validation(
            "graph object path is not the declared regular file",
        ));
    }
    let mut file = File::open(&path).map_err(|error| storage("open graph object", &path, error))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; BUFFER_BYTES];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| storage("read graph object", &path, error))?;
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

/// Materialize a verified logical inventory as hard links to immutable CAS
/// objects. The target must be empty.
pub fn materialize_graph_objects(
    root: &Path,
    inventory: &GraphFilesInventory,
    target: &Path,
) -> Result<GraphFilesOpenEvidence, GfError> {
    let target_directory = open_empty_materialization_target(target)?;
    let mut evidence = GraphFilesOpenEvidence {
        strategy: GraphFilesOpenStrategy::PrivateMaterialize,
        files_validated: inventory.file_count,
        bytes_validated: inventory.total_byte_length,
        ..GraphFilesOpenEvidence::default()
    };
    for entry in &inventory.files {
        let source = graph_object_path(root, &entry.content_sha256)?;
        link_materialized_object(
            &target_directory,
            &source,
            &entry.relative_path,
            &entry.content_sha256,
            entry.byte_length,
        )?;
        evidence.files_reused = evidence.files_reused.saturating_add(1);
        evidence.bytes_reused = evidence.bytes_reused.saturating_add(entry.byte_length);
    }
    Ok(evidence)
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
                    OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
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

#[cfg(not(unix))]
fn open_empty_materialization_target(target: &Path) -> Result<PathBuf, GfError> {
    if let Ok(metadata) = fs::symlink_metadata(target)
        && (metadata.file_type().is_symlink() || !metadata.is_dir())
    {
        return Err(validation("graph object target is not a real directory"));
    }
    fs::create_dir_all(target)
        .map_err(|error| storage("create graph object materialization target", target, error))?;
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
    Ok(target.to_path_buf())
}

#[cfg(not(unix))]
fn link_materialized_object(
    target: &PathBuf,
    source: &Path,
    relative: &str,
    expected_digest: &str,
    expected_length: u64,
) -> Result<(), GfError> {
    let destination = target.join(relative);
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| storage("create logical graph object directory", parent, error))?;
    }
    fs::hard_link(source, &destination)
        .map_err(|error| storage("link logical graph object", &destination, error))?;
    let verified = fs::symlink_metadata(&destination)
        .map_err(|error| storage("inspect materialized object", &destination, error))
        .and_then(|metadata| {
            if !metadata.is_file()
                || metadata.file_type().is_symlink()
                || metadata.len() != expected_length
            {
                return Err(validation("materialized graph object identity is invalid"));
            }
            (hex_digest(hash_regular_file(&destination)?) == expected_digest)
                .then_some(())
                .ok_or_else(|| validation("materialized graph object digest mismatch"))
        });
    if let Err(error) = verified {
        let _ = fs::remove_file(&destination);
        return Err(error);
    }
    Ok(())
}

/// Read an object whose digest is known before its declared logical length.
/// `max_length` bounds allocation for untrusted manifest objects.
pub fn read_graph_object_by_digest(
    root: &Path,
    digest: &str,
    max_length: u64,
) -> Result<Vec<u8>, GfError> {
    let path = graph_object_path(root, digest)?;
    let metadata = fs::symlink_metadata(&path)
        .map_err(|error| storage("inspect graph object", &path, error))?;
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() > max_length {
        return Err(validation(
            "graph object exceeds admitted length or is not regular",
        ));
    }
    let bytes = fs::read(&path).map_err(|error| storage("read graph object", &path, error))?;
    if hex_digest(Sha256::digest(&bytes).into()) != digest {
        return Err(validation("graph object digest does not match its address"));
    }
    Ok(bytes)
}

fn install_object<F>(
    root: &Path,
    digest: &str,
    expected_length: u64,
    write_temporary: F,
) -> Result<GraphObjectInstallEvidence, GfError>
where
    F: FnOnce(&Path) -> Result<u64, GfError>,
{
    let destination = graph_object_path(root, digest)?;
    if destination.exists() {
        verify_existing(&destination, digest, expected_length)?;
        return Ok(GraphObjectInstallEvidence {
            reused_existing: true,
            ..GraphObjectInstallEvidence::default()
        });
    }
    let temporary_root = root.join(GRAPH_OBJECTS_DIR).join(TEMP_DIR);
    fs::create_dir_all(&temporary_root).map_err(|error| {
        storage(
            "create graph object temporary directory",
            &temporary_root,
            error,
        )
    })?;
    let temporary = temporary_root.join(Uuid::new_v4().hyphenated().to_string());
    let bytes_hashed = write_temporary(&temporary)?;
    verify_existing(&temporary, digest, expected_length)?;
    let parent = destination
        .parent()
        .ok_or_else(|| validation("graph object destination has no parent"))?;
    fs::create_dir_all(parent)
        .map_err(|error| storage("create graph object digest directory", parent, error))?;
    let installed = match fs::hard_link(&temporary, &destination) {
        Ok(()) => true,
        Err(_) if destination.exists() => {
            verify_existing(&destination, digest, expected_length)?;
            false
        }
        Err(error) => {
            return Err(storage(
                "atomically install graph object",
                &destination,
                error,
            ));
        }
    };
    fs::remove_file(&temporary)
        .map_err(|error| storage("remove temporary graph object", &temporary, error))?;
    sync_directory(parent)?;
    Ok(GraphObjectInstallEvidence {
        bytes_hashed,
        bytes_installed: if installed { expected_length } else { 0 },
        reused_existing: !installed,
    })
}

fn verify_existing(path: &Path, digest: &str, expected_length: u64) -> Result<(), GfError> {
    let metadata =
        fs::symlink_metadata(path).map_err(|error| storage("inspect graph object", path, error))?;
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() != expected_length
    {
        return Err(validation(
            "graph object path is not the declared regular file",
        ));
    }
    let mut file = File::open(path).map_err(|error| storage("open graph object", path, error))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; BUFFER_BYTES];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| storage("read graph object", path, error))?;
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

fn create_new(path: &Path) -> Result<File, GfError> {
    OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|error| storage("create temporary graph object", path, error))
}

fn sync_directory(path: &Path) -> Result<(), GfError> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|error| storage("fsync graph object directory", path, error))
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

        assert!(validate_lock_identity(&lease.lifecycle, &lease.lifecycle_path).is_err());
        assert!(matches!(try_begin_graph_object_gc(root.path()), Ok(None)));

        let other_root = tempfile::tempdir().unwrap();
        let lease = begin_graph_object_publication(other_root.path()).unwrap();
        let object_root = other_root.path().join(GRAPH_OBJECTS_DIR);
        fs::rename(&object_root, other_root.path().join("displaced-objects")).unwrap();
        fs::create_dir(&object_root).unwrap();
        assert!(
            validate_directory_identity(
                &lease.lifecycle_directory,
                &lease.lifecycle_directory_path,
            )
            .is_err()
        );
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
