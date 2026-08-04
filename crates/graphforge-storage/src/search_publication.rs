//! Coordinated, crash-safe publication for text and vector search artifacts.
//!
//! A per-key advisory file lock serializes builders across threads and
//! processes. Builds occur in a unique sibling directory, are synchronized,
//! renamed into an immutable version directory, then made visible by atomically
//! replacing a small `current.json` pointer. The prior pointer remains readable
//! until that final replace.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use super::search_manifest::{
    SearchArtifactError, SearchArtifactKey, SearchIndexKind, SearchManifest, SearchSourceSnapshot,
};

const CURRENT_FILE: &str = "current.json";
const MANIFEST_FILE: &str = "manifest.json";
const VERSIONS_DIR: &str = "versions";
const BUILD_PREFIX: &str = ".build-";
const VERSION_PREFIX: &str = "version-";
const MAX_CURRENT_BYTES: usize = 4096;

/// Bounds for lock waits and reopen cleanup.
#[derive(Clone, Copy, Debug)]
pub struct SearchCoordinationLimits {
    /// Maximum time to wait for another builder of the same key.
    pub lock_timeout: Duration,
    /// Cooperative checkpoint interval while waiting.
    pub lock_poll_interval: Duration,
    /// Maximum filesystem entries inspected during one cleanup pass.
    pub cleanup_entries: usize,
}

impl Default for SearchCoordinationLimits {
    fn default() -> Self {
        Self {
            lock_timeout: Duration::from_secs(30),
            lock_poll_interval: Duration::from_millis(25),
            cleanup_entries: 10_000,
        }
    }
}

/// Whether a coordinated call may reuse an exactly matching fresh artifact.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SearchPublicationMode {
    /// Reuse a verified fresh artifact; otherwise build and publish.
    ReuseFresh,
    /// Always build and atomically replace the active pointer.
    Replace,
}

/// Immutable publication parameters supplied by a concrete backend.
#[derive(Clone, Copy, Debug)]
pub struct SearchPublicationPlan<'a> {
    /// Normalized artifact identity.
    pub key: &'a SearchArtifactKey,
    /// Pinned backend version.
    pub backend_version: &'a str,
    /// Pinned scoring/tokenization/vector contract version.
    pub contract_version: &'a str,
    /// Required vector dimension; absent for text.
    pub dimension: Option<u32>,
    /// Fresh reuse or forced replacement.
    pub mode: SearchPublicationMode,
}

/// A verified currently published search artifact.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublishedSearchArtifact {
    /// Immutable version directory containing backend files and manifest.
    pub path: PathBuf,
    /// Parsed completed manifest.
    pub manifest: SearchManifest,
}

/// Result of one coordinated build request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SearchPublicationOutcome {
    /// Another request or an earlier run already published an exact match.
    Reused(PublishedSearchArtifact),
    /// This request published a new immutable version.
    Published {
        /// Newly active artifact.
        artifact: PublishedSearchArtifact,
        /// One for the normal path, two after a concurrent mutation retry.
        attempts: u8,
    },
}

/// Decision returned by an atomic primary-data update builder.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SearchUpdateBuild {
    /// The current immutable publication already contains the requested data.
    ReuseCurrent,
    /// The builder wrote a complete replacement into the supplied directory.
    Publish,
}

/// Coordinate freshness reuse, a bounded mutation retry, and atomic
/// publication under the artifact-key writer lock.
///
/// `snapshot` must return both the current committed search generation and the
/// canonical fingerprint of the relevant graph snapshot. `validate_current`
/// validates backend files before a fresh manifest can be reused; a text
/// backend reports rebuildable corruption as
/// [`SearchArtifactError::CorruptDerivedIndex`]. `build` writes only inside the
/// supplied temporary directory. `checkpoint` enforces cooperative
/// cancellation and backend time/resource limits.
///
/// # Errors
/// Distinguishes cancellation, lock failure, corrupt primary vectors,
/// resource exhaustion, build failure, I/O, and repeated concurrent mutation.
pub fn coordinate_search_publication<S, V, B, C>(
    project_dir: &Path,
    plan: SearchPublicationPlan<'_>,
    limits: SearchCoordinationLimits,
    mut snapshot: S,
    mut validate_current: V,
    mut build: B,
    mut checkpoint: C,
) -> Result<SearchPublicationOutcome, SearchArtifactError>
where
    S: FnMut() -> Result<SearchSourceSnapshot, SearchArtifactError>,
    V: FnMut(&PublishedSearchArtifact) -> Result<(), SearchArtifactError>,
    B: FnMut(&Path, &SearchSourceSnapshot) -> Result<(), SearchArtifactError>,
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    let root = plan.key.artifact_root(project_dir);
    std::fs::create_dir_all(&root).map_err(|source| io("create artifact root", &root, source))?;
    let _writer = SearchWriterLock::acquire(&root, limits, &mut checkpoint)?;
    checkpoint()?;

    let initial = snapshot()?;
    match current_search_artifact(project_dir, plan.key) {
        Ok(Some(artifact)) if plan.mode == SearchPublicationMode::ReuseFresh => {
            match artifact.manifest.verify_fresh(
                plan.key,
                plan.backend_version,
                plan.contract_version,
                plan.dimension,
                &initial,
            ) {
                Ok(()) => match validate_current(&artifact) {
                    Ok(()) => return Ok(SearchPublicationOutcome::Reused(artifact)),
                    Err(error)
                        if plan.key.kind() == SearchIndexKind::Text
                            && rebuildable_metadata(&error) => {}
                    Err(error) if plan.key.kind() == SearchIndexKind::Vector => {
                        return Err(primary_vector_error(root, error));
                    }
                    Err(error) => return Err(error),
                },
                Err(SearchArtifactError::Stale { .. })
                    if plan.key.kind() == SearchIndexKind::Text => {}
                Err(error) => return Err(error),
            }
        }
        Ok(Some(_) | None) => {}
        Err(error) if plan.key.kind() == SearchIndexKind::Text && rebuildable_metadata(&error) => {}
        Err(error) if plan.key.kind() == SearchIndexKind::Vector => {
            return Err(primary_vector_error(root, error));
        }
        Err(error) => return Err(error),
    }

    let mut before = initial;
    for attempt in 1_u8..=2 {
        checkpoint()?;
        let publication = PendingPublication::new(&root)?;
        build(publication.path(), &before)?;
        checkpoint()?;
        let after = snapshot()?;
        if before != after {
            if attempt == 2 {
                return Err(SearchArtifactError::ConcurrentMutation);
            }
            before = after;
            continue;
        }
        let manifest = SearchManifest::for_key(
            plan.key,
            plan.backend_version,
            plan.contract_version,
            plan.dimension,
            &before,
            true,
        )?;
        let artifact = publication.publish(&manifest)?;
        return Ok(SearchPublicationOutcome::Published {
            artifact,
            attempts: attempt,
        });
    }
    unreachable!("the bounded publication loop returns on both terminal paths")
}

/// Coordinate an atomic update that may inspect and reuse the current artifact.
///
/// This is the primary-data counterpart to [`coordinate_search_publication`].
/// The per-key writer lock is held while `build` sees the current immutable
/// publication.  Returning [`SearchUpdateBuild::ReuseCurrent`] makes an
/// idempotent update a no-op; returning [`SearchUpdateBuild::Publish`] uses the
/// same synchronized immutable-directory and atomic-pointer protocol as a
/// derived build.  Both decisions recheck the graph snapshot, with one bounded
/// retry, so membership validation cannot race a supported graph mutation.
///
/// `build` must write only inside its supplied temporary directory.  It also
/// receives the cooperative checkpoint callback so backend work cannot publish
/// after cancellation.
///
/// # Errors
/// Distinguishes missing/corrupt primary data, cancellation, lock failure,
/// resource exhaustion, build failure, I/O, and repeated concurrent mutation.
pub fn coordinate_search_update<S, B, C>(
    project_dir: &Path,
    plan: SearchPublicationPlan<'_>,
    limits: SearchCoordinationLimits,
    mut snapshot: S,
    mut build: B,
    mut checkpoint: C,
) -> Result<SearchPublicationOutcome, SearchArtifactError>
where
    S: FnMut() -> Result<SearchSourceSnapshot, SearchArtifactError>,
    B: FnMut(
        Option<&PublishedSearchArtifact>,
        &Path,
        &SearchSourceSnapshot,
        &mut C,
    ) -> Result<SearchUpdateBuild, SearchArtifactError>,
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    if plan.mode != SearchPublicationMode::Replace {
        return Err(SearchArtifactError::Build(
            "atomic search updates require replacement mode".to_owned(),
        ));
    }
    let root = plan.key.artifact_root(project_dir);
    std::fs::create_dir_all(&root).map_err(|source| io("create artifact root", &root, source))?;
    let _writer = SearchWriterLock::acquire(&root, limits, &mut checkpoint)?;
    checkpoint()?;
    let current = match current_search_artifact(project_dir, plan.key) {
        Ok(current) => current,
        Err(error) if plan.key.kind() == SearchIndexKind::Vector => {
            return Err(primary_vector_error(root, error));
        }
        Err(error) => return Err(error),
    };

    let mut before = snapshot()?;
    for attempt in 1_u8..=2 {
        checkpoint()?;
        let publication = PendingPublication::new(&root)?;
        let decision = build(
            current.as_ref(),
            publication.path(),
            &before,
            &mut checkpoint,
        )?;
        checkpoint()?;
        let after = snapshot()?;
        if before != after {
            if attempt == 2 {
                return Err(SearchArtifactError::ConcurrentMutation);
            }
            before = after;
            continue;
        }
        match decision {
            SearchUpdateBuild::ReuseCurrent => {
                return current
                    .clone()
                    .map(SearchPublicationOutcome::Reused)
                    .ok_or_else(|| {
                        SearchArtifactError::Build(
                            "update requested reuse without a current artifact".to_owned(),
                        )
                    });
            }
            SearchUpdateBuild::Publish => {
                let manifest = SearchManifest::for_key(
                    plan.key,
                    plan.backend_version,
                    plan.contract_version,
                    plan.dimension,
                    &before,
                    true,
                )?;
                let artifact = publication.publish(&manifest)?;
                return Ok(SearchPublicationOutcome::Published {
                    artifact,
                    attempts: attempt,
                });
            }
        }
    }
    unreachable!("the bounded update loop returns on both terminal paths")
}

/// Resolve and parse the currently published immutable version.
///
/// Missing `current.json` is `Ok(None)`. A torn, incompatible, incomplete, or
/// traversal-like pointer/manifest is a structured error and is never served.
///
/// # Errors
/// Returns a manifest, resource, or filesystem error.
pub fn current_search_artifact(
    project_dir: &Path,
    key: &SearchArtifactKey,
) -> Result<Option<PublishedSearchArtifact>, SearchArtifactError> {
    let root = key.artifact_root(project_dir);
    let pointer = root.join(CURRENT_FILE);
    let bytes = match std::fs::read(&pointer) {
        Ok(bytes) => bytes,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(io("read current pointer", &pointer, source)),
    };
    if bytes.len() > MAX_CURRENT_BYTES {
        return Err(SearchArtifactError::ResourceExhausted {
            resource: "current_pointer_bytes",
            limit: MAX_CURRENT_BYTES as u64,
        });
    }
    let value: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|error| SearchArtifactError::CorruptManifest {
            path: pointer.clone(),
            reason: error.to_string(),
        })?;
    let version = value
        .as_object()
        .and_then(|object| object.get("version"))
        .and_then(serde_json::Value::as_str)
        .filter(|name| valid_owned_name(name, VERSION_PREFIX))
        .ok_or_else(|| SearchArtifactError::CorruptManifest {
            path: pointer,
            reason: "expected a safe version pointer".to_owned(),
        })?;
    let artifact_dir = root.join(VERSIONS_DIR).join(version);
    let manifest_path = artifact_dir.join(MANIFEST_FILE);
    let manifest_bytes = match std::fs::read(&manifest_path) {
        Ok(bytes) => bytes,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            return Err(SearchArtifactError::Missing {
                path: manifest_path,
            });
        }
        Err(source) => return Err(io("read published manifest", &manifest_path, source)),
    };
    let manifest = SearchManifest::from_json(&manifest_path, &manifest_bytes)?;
    if !manifest.completed {
        return Err(SearchArtifactError::CorruptManifest {
            path: manifest_path,
            reason: "published manifest is not completed".to_owned(),
        });
    }
    Ok(Some(PublishedSearchArtifact {
        path: artifact_dir,
        manifest,
    }))
}

/// Remove abandoned GraphForge search build directories and pointer temp files
/// after reopen.
///
/// Only the known `indexes/search/` and `embeddings/` trees are traversed.
/// Only exact GraphForge-owned names are removed; symlinks and unrecognized
/// user files are preserved. The entire bounded scan completes before any
/// deletion starts.
///
/// # Errors
/// Returns an I/O or cleanup-entry resource error. Exceeding the bound removes
/// nothing.
pub fn cleanup_abandoned_search_builds(
    project_dir: &Path,
    max_entries: usize,
) -> Result<usize, SearchArtifactError> {
    let roots = [
        project_dir.join("indexes").join("search"),
        project_dir.join("embeddings"),
    ];
    let mut stack = roots.to_vec();
    let mut inspected = 0_usize;
    let mut remove = Vec::new();
    while let Some(directory) = stack.pop() {
        let entries = match std::fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => continue,
            Err(source) => return Err(io("scan abandoned builds", &directory, source)),
        };
        for entry in entries {
            inspected = inspected
                .checked_add(1)
                .ok_or(SearchArtifactError::ResourceExhausted {
                    resource: "cleanup_entries",
                    limit: max_entries as u64,
                })?;
            if inspected > max_entries {
                return Err(SearchArtifactError::ResourceExhausted {
                    resource: "cleanup_entries",
                    limit: max_entries as u64,
                });
            }
            let entry = entry.map_err(|source| io("read cleanup entry", &directory, source))?;
            let file_type = entry
                .file_type()
                .map_err(|source| io("read cleanup file type", &entry.path(), source))?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if file_type.is_dir() && valid_owned_name(&name, BUILD_PREFIX) {
                remove.push((entry.path(), true));
            } else if file_type.is_dir() && !file_type.is_symlink() {
                stack.push(entry.path());
            } else if file_type.is_file() && valid_pointer_temp(&name) {
                remove.push((entry.path(), false));
            }
        }
    }

    remove.sort_unstable_by(|left, right| right.0.cmp(&left.0));
    for (path, directory) in &remove {
        if *directory {
            std::fs::remove_dir_all(path)
                .map_err(|source| io("remove abandoned build", path, source))?;
        } else {
            std::fs::remove_file(path)
                .map_err(|source| io("remove abandoned pointer", path, source))?;
        }
    }
    Ok(remove.len())
}

struct SearchWriterLock {
    file: File,
}

impl SearchWriterLock {
    fn acquire<C>(
        root: &Path,
        limits: SearchCoordinationLimits,
        checkpoint: &mut C,
    ) -> Result<Self, SearchArtifactError>
    where
        C: FnMut() -> Result<(), SearchArtifactError>,
    {
        let path = root.join(".writer.lock");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|source| SearchArtifactError::Lock {
                path: path.clone(),
                reason: source.to_string(),
            })?;
        let started = Instant::now();
        loop {
            match file.try_lock() {
                Ok(()) => return Ok(Self { file }),
                Err(std::fs::TryLockError::WouldBlock) => {
                    checkpoint()?;
                    if started.elapsed() >= limits.lock_timeout {
                        return Err(SearchArtifactError::Lock {
                            path,
                            reason: format!(
                                "timed out after {} ms",
                                limits.lock_timeout.as_millis()
                            ),
                        });
                    }
                    std::thread::sleep(limits.lock_poll_interval);
                }
                Err(std::fs::TryLockError::Error(source)) => {
                    return Err(SearchArtifactError::Lock {
                        path,
                        reason: source.to_string(),
                    });
                }
            }
        }
    }
}

impl Drop for SearchWriterLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

struct PendingPublication {
    root: PathBuf,
    temp: tempfile::TempDir,
}

impl PendingPublication {
    fn new(root: &Path) -> Result<Self, SearchArtifactError> {
        let temp = tempfile::Builder::new()
            .prefix(BUILD_PREFIX)
            .tempdir_in(root)
            .map_err(|source| io("create build directory", root, source))?;
        Ok(Self {
            root: root.to_path_buf(),
            temp,
        })
    }

    fn path(&self) -> &Path {
        self.temp.path()
    }

    fn publish(
        self,
        manifest: &SearchManifest,
    ) -> Result<PublishedSearchArtifact, SearchArtifactError> {
        let manifest_path = self.temp.path().join(MANIFEST_FILE);
        let manifest_bytes = manifest.to_canonical_json()?;
        write_synced_file(&manifest_path, &manifest_bytes)?;
        sync_tree(self.temp.path())?;

        let token = self
            .temp
            .path()
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.strip_prefix(BUILD_PREFIX))
            .filter(|token| {
                !token.is_empty() && token.bytes().all(|byte| byte.is_ascii_alphanumeric())
            })
            .ok_or_else(|| {
                SearchArtifactError::Build("temporary build name is invalid".to_owned())
            })?;
        let version_name = format!("{VERSION_PREFIX}{token}");
        let versions = self.root.join(VERSIONS_DIR);
        std::fs::create_dir_all(&versions)
            .map_err(|source| io("create versions directory", &versions, source))?;
        let version_path = versions.join(&version_name);
        let temp_path = self.temp.keep();
        if let Err(source) = std::fs::rename(&temp_path, &version_path) {
            let _ = std::fs::remove_dir_all(&temp_path);
            return Err(io("publish immutable version", &version_path, source));
        }
        sync_directory(&versions)?;

        let pointer = serde_json::to_vec(&serde_json::json!({ "version": version_name }))
            .map_err(|error| SearchArtifactError::Build(error.to_string()))?;
        persist_synced_pointer(&self.root.join(CURRENT_FILE), &pointer)?;
        sync_directory(&self.root)?;
        Ok(PublishedSearchArtifact {
            path: version_path,
            manifest: manifest.clone(),
        })
    }
}

fn rebuildable_metadata(error: &SearchArtifactError) -> bool {
    matches!(
        error,
        SearchArtifactError::Missing { .. }
            | SearchArtifactError::CorruptManifest { .. }
            | SearchArtifactError::CorruptDerivedIndex { .. }
            | SearchArtifactError::IncompatibleManifest { .. }
            | SearchArtifactError::Stale { .. }
            | SearchArtifactError::ResourceExhausted {
                resource: "manifest_bytes" | "current_pointer_bytes",
                ..
            }
    )
}

fn primary_vector_error(root: PathBuf, error: SearchArtifactError) -> SearchArtifactError {
    match error {
        error @ SearchArtifactError::CorruptPrimaryVectors { .. } => error,
        error => SearchArtifactError::CorruptPrimaryVectors {
            path: root,
            reason: error.to_string(),
        },
    }
}

fn write_synced_file(path: &Path, bytes: &[u8]) -> Result<(), SearchArtifactError> {
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
        .map_err(|source| io("create publication file", path, source))?;
    file.write_all(bytes)
        .map_err(|source| io("write publication file", path, source))?;
    file.sync_all()
        .map_err(|source| io("sync publication file", path, source))
}

fn persist_synced_pointer(path: &Path, bytes: &[u8]) -> Result<(), SearchArtifactError> {
    let parent = path
        .parent()
        .ok_or_else(|| SearchArtifactError::Build("current pointer has no parent".to_owned()))?;
    let mut temp = tempfile::Builder::new()
        .prefix("current.json.")
        .suffix(".tmp")
        .tempfile_in(parent)
        .map_err(|source| io("create current pointer temp", path, source))?;
    temp.write_all(bytes)
        .map_err(|source| io("write current pointer temp", path, source))?;
    temp.as_file()
        .sync_all()
        .map_err(|source| io("sync current pointer temp", path, source))?;
    temp.persist(path)
        .map_err(|error| io("publish current pointer", path, error.error))?;
    Ok(())
}

fn sync_tree(root: &Path) -> Result<(), SearchArtifactError> {
    let mut directories = vec![root.to_path_buf()];
    let mut files = Vec::new();
    let mut cursor = 0;
    while cursor < directories.len() {
        let directory = directories[cursor].clone();
        cursor += 1;
        let entries = std::fs::read_dir(&directory)
            .map_err(|source| io("scan build for sync", &directory, source))?;
        for entry in entries {
            let entry = entry.map_err(|source| io("read build entry", &directory, source))?;
            let file_type = entry
                .file_type()
                .map_err(|source| io("read build file type", &entry.path(), source))?;
            if file_type.is_symlink() {
                return Err(SearchArtifactError::Build(format!(
                    "search build must not contain symlink {}",
                    entry.path().display()
                )));
            }
            if file_type.is_dir() {
                directories.push(entry.path());
            } else if file_type.is_file() {
                files.push(entry.path());
            }
        }
    }
    files.sort_unstable();
    for path in files {
        sync_file(&path)?;
    }
    directories.sort_unstable_by_key(|path| std::cmp::Reverse(path.components().count()));
    for directory in directories {
        sync_directory(&directory)?;
    }
    Ok(())
}

fn sync_file(path: &Path) -> Result<(), SearchArtifactError> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .and_then(|file| file.sync_all())
        .map_err(|source| io("sync build file", path, source))
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<(), SearchArtifactError> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|source| io("sync directory", path, source))
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<(), SearchArtifactError> {
    Ok(())
}

fn valid_owned_name(name: &str, prefix: &str) -> bool {
    name.strip_prefix(prefix).is_some_and(|token| {
        !token.is_empty() && token.bytes().all(|byte| byte.is_ascii_alphanumeric())
    })
}

fn valid_pointer_temp(name: &str) -> bool {
    name.strip_prefix("current.json.")
        .and_then(|name| name.strip_suffix(".tmp"))
        .is_some_and(|token| {
            !token.is_empty() && token.bytes().all(|byte| byte.is_ascii_alphanumeric())
        })
}

fn io(operation: &'static str, path: &Path, source: std::io::Error) -> SearchArtifactError {
    SearchArtifactError::Io {
        operation,
        path: path.to_path_buf(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};

    use tempfile::TempDir;

    use super::*;

    fn key() -> SearchArtifactKey {
        SearchArtifactKey::text("Person", ["name"]).unwrap()
    }

    fn snapshot(generation: u64) -> SearchSourceSnapshot {
        SearchSourceSnapshot {
            generation,
            fingerprint: format!("gf-fnv1a256:{generation:064x}"),
        }
    }

    fn plan<'a>(
        key: &'a SearchArtifactKey,
        mode: SearchPublicationMode,
    ) -> SearchPublicationPlan<'a> {
        SearchPublicationPlan {
            key,
            backend_version: "tantivy-0.25",
            contract_version: "graphforge_text_v1",
            dimension: None,
            mode,
        }
    }

    #[test]
    fn publish_is_invisible_until_current_pointer_swap() {
        let dir = TempDir::new().unwrap();
        let key = key();
        let root = key.artifact_root(dir.path());
        std::fs::create_dir_all(&root).unwrap();
        let pending = PendingPublication::new(&root).unwrap();
        std::fs::write(pending.path().join("index"), b"complete").unwrap();
        assert!(current_search_artifact(dir.path(), &key).unwrap().is_none());

        let manifest = SearchManifest::for_key(
            &key,
            "tantivy-0.25",
            "graphforge_text_v1",
            None,
            &snapshot(1),
            true,
        )
        .unwrap();
        let published = pending.publish(&manifest).unwrap();
        assert_eq!(
            current_search_artifact(dir.path(), &key).unwrap(),
            Some(published)
        );
    }

    #[test]
    fn sync_tree_flushes_regular_files_without_mutating_contents() {
        let dir = TempDir::new().unwrap();
        let nested = dir.path().join("nested");
        std::fs::create_dir(&nested).unwrap();
        let first = dir.path().join("first");
        let second = nested.join("second");
        std::fs::write(&first, b"one").unwrap();
        std::fs::write(&second, b"two").unwrap();

        sync_tree(dir.path()).unwrap();

        assert_eq!(std::fs::read(first).unwrap(), b"one");
        assert_eq!(std::fs::read(second).unwrap(), b"two");
    }

    #[test]
    fn forced_rebuild_atomically_replaces_pointer_and_keeps_old_version() {
        let dir = TempDir::new().unwrap();
        let key = key();
        let first = coordinate_search_publication(
            dir.path(),
            plan(&key, SearchPublicationMode::Replace),
            SearchCoordinationLimits::default(),
            || Ok(snapshot(1)),
            |_| Ok(()),
            |path, _| {
                std::fs::write(path.join("index"), b"first")
                    .map_err(|error| SearchArtifactError::Build(error.to_string()))
            },
            || Ok(()),
        )
        .unwrap();
        let first_path = match first {
            SearchPublicationOutcome::Published { artifact, .. } => artifact.path,
            SearchPublicationOutcome::Reused(_) => panic!("forced build reused"),
        };
        let second = coordinate_search_publication(
            dir.path(),
            plan(&key, SearchPublicationMode::Replace),
            SearchCoordinationLimits::default(),
            || Ok(snapshot(1)),
            |_| Ok(()),
            |path, _| {
                std::fs::write(path.join("index"), b"second")
                    .map_err(|error| SearchArtifactError::Build(error.to_string()))
            },
            || Ok(()),
        )
        .unwrap();
        let second_path = match second {
            SearchPublicationOutcome::Published { artifact, .. } => artifact.path,
            SearchPublicationOutcome::Reused(_) => panic!("forced build reused"),
        };
        assert_ne!(first_path, second_path);
        assert!(
            first_path.exists(),
            "old readers retain an immutable version"
        );
        assert_eq!(
            current_search_artifact(dir.path(), &key)
                .unwrap()
                .unwrap()
                .path,
            second_path
        );
    }

    #[test]
    fn failed_replacement_keeps_the_previous_publication() {
        let dir = TempDir::new().unwrap();
        let key = key();
        coordinate_search_publication(
            dir.path(),
            plan(&key, SearchPublicationMode::Replace),
            SearchCoordinationLimits::default(),
            || Ok(snapshot(1)),
            |_| Ok(()),
            |path, _| {
                std::fs::write(path.join("index"), b"committed")
                    .map_err(|error| SearchArtifactError::Build(error.to_string()))
            },
            || Ok(()),
        )
        .unwrap();
        let previous = current_search_artifact(dir.path(), &key).unwrap().unwrap();

        let error = coordinate_search_publication(
            dir.path(),
            plan(&key, SearchPublicationMode::Replace),
            SearchCoordinationLimits::default(),
            || Ok(snapshot(1)),
            |_| Ok(()),
            |path, _| {
                std::fs::write(path.join("index"), b"partial").unwrap();
                Err(SearchArtifactError::Build("injected failure".to_owned()))
            },
            || Ok(()),
        )
        .unwrap_err();
        assert!(matches!(error, SearchArtifactError::Build(_)));
        assert_eq!(
            current_search_artifact(dir.path(), &key).unwrap().unwrap(),
            previous
        );
        assert_eq!(
            std::fs::read(previous.path.join("index")).unwrap(),
            b"committed"
        );
    }

    #[test]
    fn fresh_lazy_request_reuses_without_running_builder() {
        let dir = TempDir::new().unwrap();
        let key = key();
        let builds = AtomicUsize::new(0);
        let run = |mode| {
            coordinate_search_publication(
                dir.path(),
                plan(&key, mode),
                SearchCoordinationLimits::default(),
                || Ok(snapshot(1)),
                |_| Ok(()),
                |path, _| {
                    builds.fetch_add(1, Ordering::SeqCst);
                    std::fs::write(path.join("index"), b"data")
                        .map_err(|error| SearchArtifactError::Build(error.to_string()))
                },
                || Ok(()),
            )
        };
        assert!(matches!(
            run(SearchPublicationMode::ReuseFresh).unwrap(),
            SearchPublicationOutcome::Published { .. }
        ));
        assert!(matches!(
            run(SearchPublicationMode::ReuseFresh).unwrap(),
            SearchPublicationOutcome::Reused(_)
        ));
        assert_eq!(builds.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn corrupt_derived_backend_is_rebuilt_before_reuse() {
        let dir = TempDir::new().unwrap();
        let key = key();
        let builds = AtomicUsize::new(0);
        let run = || {
            coordinate_search_publication(
                dir.path(),
                plan(&key, SearchPublicationMode::ReuseFresh),
                SearchCoordinationLimits::default(),
                || Ok(snapshot(1)),
                |artifact| {
                    let path = artifact.path.join("index");
                    let bytes = std::fs::read(&path).map_err(|error| {
                        SearchArtifactError::CorruptDerivedIndex {
                            path: path.clone(),
                            reason: error.to_string(),
                        }
                    })?;
                    if bytes == b"valid" {
                        Ok(())
                    } else {
                        Err(SearchArtifactError::CorruptDerivedIndex {
                            path,
                            reason: "backend validation failed".to_owned(),
                        })
                    }
                },
                |path, _| {
                    builds.fetch_add(1, Ordering::SeqCst);
                    std::fs::write(path.join("index"), b"valid")
                        .map_err(|error| SearchArtifactError::Build(error.to_string()))
                },
                || Ok(()),
            )
        };
        assert!(matches!(
            run().unwrap(),
            SearchPublicationOutcome::Published { .. }
        ));
        let current = current_search_artifact(dir.path(), &key).unwrap().unwrap();
        std::fs::write(current.path.join("index"), b"corrupt").unwrap();
        assert!(matches!(
            run().unwrap(),
            SearchPublicationOutcome::Published { .. }
        ));
        assert_eq!(builds.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn mutation_retries_once_and_second_mutation_fails_closed() {
        let dir = TempDir::new().unwrap();
        let key = key();
        let reads = AtomicUsize::new(0);
        let outcome = coordinate_search_publication(
            dir.path(),
            plan(&key, SearchPublicationMode::Replace),
            SearchCoordinationLimits::default(),
            || {
                let call = reads.fetch_add(1, Ordering::SeqCst);
                Ok(snapshot(u64::from(call >= 1)))
            },
            |_| Ok(()),
            |path, source| {
                std::fs::write(path.join("source"), source.generation.to_string())
                    .map_err(|error| SearchArtifactError::Build(error.to_string()))
            },
            || Ok(()),
        )
        .unwrap();
        assert!(matches!(
            outcome,
            SearchPublicationOutcome::Published { attempts: 2, .. }
        ));

        let reads = AtomicUsize::new(0);
        let error = coordinate_search_publication(
            dir.path(),
            plan(&key, SearchPublicationMode::Replace),
            SearchCoordinationLimits::default(),
            || Ok(snapshot(reads.fetch_add(1, Ordering::SeqCst) as u64)),
            |_| Ok(()),
            |path, _| {
                std::fs::write(path.join("index"), b"data")
                    .map_err(|error| SearchArtifactError::Build(error.to_string()))
            },
            || Ok(()),
        )
        .unwrap_err();
        assert!(matches!(error, SearchArtifactError::ConcurrentMutation));
    }

    #[test]
    fn atomic_update_requires_replace_and_cannot_reuse_missing_publication() {
        let dir = TempDir::new().unwrap();
        let key = key();
        let error = coordinate_search_update(
            dir.path(),
            plan(&key, SearchPublicationMode::ReuseFresh),
            SearchCoordinationLimits::default(),
            || Ok(snapshot(1)),
            |_, _, _, _| Ok(SearchUpdateBuild::Publish),
            || Ok(()),
        )
        .unwrap_err();
        assert!(
            matches!(error, SearchArtifactError::Build(reason) if reason.contains("replacement mode"))
        );

        let error = coordinate_search_update(
            dir.path(),
            plan(&key, SearchPublicationMode::Replace),
            SearchCoordinationLimits::default(),
            || Ok(snapshot(1)),
            |current, _, _, _| {
                assert!(current.is_none());
                Ok(SearchUpdateBuild::ReuseCurrent)
            },
            || Ok(()),
        )
        .unwrap_err();
        assert!(
            matches!(error, SearchArtifactError::Build(reason) if reason.contains("without a current artifact"))
        );
        assert!(current_search_artifact(dir.path(), &key).unwrap().is_none());
    }

    #[test]
    fn same_key_requests_serialize_and_share_one_lazy_build() {
        let dir = Arc::new(TempDir::new().unwrap());
        let key = Arc::new(key());
        let builds = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(2));
        let mut handles = Vec::new();
        for _ in 0..2 {
            let dir = Arc::clone(&dir);
            let key = Arc::clone(&key);
            let builds = Arc::clone(&builds);
            let barrier = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                coordinate_search_publication(
                    dir.path(),
                    plan(&key, SearchPublicationMode::ReuseFresh),
                    SearchCoordinationLimits::default(),
                    || Ok(snapshot(1)),
                    |_| Ok(()),
                    |path, _| {
                        builds.fetch_add(1, Ordering::SeqCst);
                        std::thread::sleep(Duration::from_millis(75));
                        std::fs::write(path.join("index"), b"data")
                            .map_err(|error| SearchArtifactError::Build(error.to_string()))
                    },
                    || Ok(()),
                )
            }));
        }
        let outcomes = handles
            .into_iter()
            .map(|handle| handle.join().unwrap().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(builds.load(Ordering::SeqCst), 1);
        assert!(
            outcomes
                .iter()
                .any(|outcome| matches!(outcome, SearchPublicationOutcome::Reused(_)))
        );
    }

    #[test]
    fn cancellation_while_waiting_does_not_publish() {
        let dir = TempDir::new().unwrap();
        let key = key();
        let root = key.artifact_root(dir.path());
        std::fs::create_dir_all(&root).unwrap();
        let lock =
            SearchWriterLock::acquire(&root, SearchCoordinationLimits::default(), &mut || Ok(()))
                .unwrap();
        let cancelled = AtomicBool::new(false);
        let limits = SearchCoordinationLimits {
            lock_timeout: Duration::from_secs(1),
            lock_poll_interval: Duration::from_millis(1),
            ..SearchCoordinationLimits::default()
        };
        let result = SearchWriterLock::acquire(&root, limits, &mut || {
            if cancelled.swap(true, Ordering::SeqCst) {
                Err(SearchArtifactError::Cancelled)
            } else {
                Ok(())
            }
        });
        drop(lock);
        assert!(matches!(result, Err(SearchArtifactError::Cancelled)));
        assert!(current_search_artifact(dir.path(), &key).unwrap().is_none());
    }

    #[test]
    fn cancellation_after_build_does_not_publish_partial_output() {
        let dir = TempDir::new().unwrap();
        let key = key();
        let checkpoints = AtomicUsize::new(0);
        let error = coordinate_search_publication(
            dir.path(),
            plan(&key, SearchPublicationMode::Replace),
            SearchCoordinationLimits::default(),
            || Ok(snapshot(1)),
            |_| Ok(()),
            |path, _| {
                std::fs::write(path.join("index"), b"partial")
                    .map_err(|error| SearchArtifactError::Build(error.to_string()))
            },
            || {
                if checkpoints.fetch_add(1, Ordering::SeqCst) >= 2 {
                    Err(SearchArtifactError::Cancelled)
                } else {
                    Ok(())
                }
            },
        )
        .unwrap_err();
        assert!(matches!(error, SearchArtifactError::Cancelled));
        assert!(current_search_artifact(dir.path(), &key).unwrap().is_none());
    }

    #[test]
    fn cleanup_is_bounded_scoped_and_preserves_unknown_files() {
        let dir = TempDir::new().unwrap();
        let search = dir.path().join("indexes/search/text/key");
        let embeddings = dir.path().join("embeddings/space/key");
        let notes = dir.path().join("notes");
        for path in [&search, &embeddings, &notes] {
            std::fs::create_dir_all(path).unwrap();
        }
        let stale = [
            search.join(".build-Abc123"),
            embeddings.join(".build-Xyz789"),
        ];
        for path in &stale {
            std::fs::create_dir_all(path).unwrap();
            std::fs::write(path.join("partial"), b"x").unwrap();
        }
        let pointer_temp = search.join("current.json.Qwe456.tmp");
        std::fs::write(&pointer_temp, b"x").unwrap();
        let preserved = [
            search.join(".build-bad-name"),
            embeddings.join("vectors.parquet"),
            notes.join(".build-Abc123"),
        ];
        for path in &preserved {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }
            std::fs::write(path, b"keep").unwrap();
        }

        assert!(matches!(
            cleanup_abandoned_search_builds(dir.path(), 1),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "cleanup_entries",
                ..
            })
        ));
        assert!(stale.iter().all(|path| path.exists()));
        assert_eq!(cleanup_abandoned_search_builds(dir.path(), 100).unwrap(), 3);
        assert!(stale.iter().all(|path| !path.exists()));
        assert!(!pointer_temp.exists());
        assert!(preserved.iter().all(|path| path.exists()));
    }

    #[test]
    fn missing_published_text_manifest_is_rebuilt() {
        let dir = TempDir::new().unwrap();
        let key = key();
        let root = key.artifact_root(dir.path());
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join(CURRENT_FILE), br#"{"version":"version-Abc123"}"#).unwrap();
        let builds = AtomicUsize::new(0);
        let outcome = coordinate_search_publication(
            dir.path(),
            plan(&key, SearchPublicationMode::ReuseFresh),
            SearchCoordinationLimits::default(),
            || Ok(snapshot(1)),
            |_| Ok(()),
            |path, _| {
                builds.fetch_add(1, Ordering::SeqCst);
                std::fs::write(path.join("index"), b"rebuilt")
                    .map_err(|error| SearchArtifactError::Build(error.to_string()))
            },
            || Ok(()),
        )
        .unwrap();
        assert!(matches!(
            outcome,
            SearchPublicationOutcome::Published { attempts: 1, .. }
        ));
        assert_eq!(builds.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn corrupt_and_incompatible_text_manifests_are_rebuilt() {
        for incompatible in [false, true] {
            let dir = TempDir::new().unwrap();
            let key = key();
            let builds = AtomicUsize::new(0);
            let run = || {
                coordinate_search_publication(
                    dir.path(),
                    plan(&key, SearchPublicationMode::ReuseFresh),
                    SearchCoordinationLimits::default(),
                    || Ok(snapshot(1)),
                    |_| Ok(()),
                    |path, _| {
                        builds.fetch_add(1, Ordering::SeqCst);
                        std::fs::write(path.join("index"), b"complete")
                            .map_err(|error| SearchArtifactError::Build(error.to_string()))
                    },
                    || Ok(()),
                )
            };
            run().unwrap();
            let current = current_search_artifact(dir.path(), &key).unwrap().unwrap();
            let manifest_path = current.path.join(MANIFEST_FILE);
            if incompatible {
                let mut manifest: serde_json::Value =
                    serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
                manifest["manifest_version"] = serde_json::Value::from(99);
                std::fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
            } else {
                std::fs::write(&manifest_path, b"corrupt").unwrap();
            }

            assert!(matches!(
                run().unwrap(),
                SearchPublicationOutcome::Published { attempts: 1, .. }
            ));
            assert_eq!(builds.load(Ordering::SeqCst), 2);
        }
    }

    #[test]
    fn corrupt_vector_metadata_is_not_discarded() {
        let dir = TempDir::new().unwrap();
        let key = SearchArtifactKey::vector("Person", "semantic").unwrap();
        let root = key.artifact_root(dir.path());
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join(CURRENT_FILE), b"corrupt").unwrap();
        let error = coordinate_search_publication(
            dir.path(),
            SearchPublicationPlan {
                key: &key,
                backend_version: "exact-cosine-v1",
                contract_version: "vector-v1",
                dimension: Some(3),
                mode: SearchPublicationMode::Replace,
            },
            SearchCoordinationLimits::default(),
            || Ok(snapshot(1)),
            |_| Ok(()),
            |_, _| Ok(()),
            || Ok(()),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            SearchArtifactError::CorruptPrimaryVectors { .. }
        ));
        assert_eq!(std::fs::read(root.join(CURRENT_FILE)).unwrap(), b"corrupt");
    }

    #[test]
    fn current_pointer_malformed_state_matrix_is_exact_and_non_mutating() {
        let key = key();
        let cases: Vec<Vec<u8>> = vec![
            b"not-json".to_vec(),
            br#"{}"#.to_vec(),
            br#"{"version":"../escape"}"#.to_vec(),
            br#"{"version":"version-bad/slash"}"#.to_vec(),
            vec![b'x'; MAX_CURRENT_BYTES + 1],
        ];
        for bytes in cases {
            let dir = TempDir::new().unwrap();
            let root = key.artifact_root(dir.path());
            std::fs::create_dir_all(&root).unwrap();
            let pointer = root.join(CURRENT_FILE);
            std::fs::write(&pointer, &bytes).unwrap();
            let result = current_search_artifact(dir.path(), &key);
            assert!(matches!(
                result,
                Err(SearchArtifactError::CorruptManifest { .. })
                    | Err(SearchArtifactError::ResourceExhausted {
                        resource: "current_pointer_bytes",
                        ..
                    })
            ));
            assert_eq!(std::fs::read(&pointer).unwrap(), bytes);
            assert!(!root.join(VERSIONS_DIR).exists());
        }
    }

    #[test]
    fn rebuildability_primary_wrapping_and_owned_name_matrices_are_total() {
        let path = PathBuf::from("artifact");
        let rebuildable = [
            SearchArtifactError::Missing { path: path.clone() },
            SearchArtifactError::CorruptManifest {
                path: path.clone(),
                reason: "bad".into(),
            },
            SearchArtifactError::CorruptDerivedIndex {
                path: path.clone(),
                reason: "bad".into(),
            },
            SearchArtifactError::IncompatibleManifest {
                path: path.clone(),
                found: 2,
                supported: 1,
            },
            SearchArtifactError::Stale {
                reason: "old".into(),
            },
            SearchArtifactError::ResourceExhausted {
                resource: "manifest_bytes",
                limit: 1,
            },
            SearchArtifactError::ResourceExhausted {
                resource: "current_pointer_bytes",
                limit: 1,
            },
        ];
        for error in &rebuildable {
            assert!(rebuildable_metadata(error), "{error}");
        }
        for error in [
            SearchArtifactError::Cancelled,
            SearchArtifactError::ConcurrentMutation,
            SearchArtifactError::ResourceExhausted {
                resource: "other",
                limit: 1,
            },
            SearchArtifactError::Build("bad".into()),
        ] {
            assert!(!rebuildable_metadata(&error), "{error}");
        }

        let primary = SearchArtifactError::CorruptPrimaryVectors {
            path: path.clone(),
            reason: "primary".into(),
        };
        assert!(matches!(
            primary_vector_error(path.clone(), primary),
            SearchArtifactError::CorruptPrimaryVectors { reason, .. } if reason == "primary"
        ));
        assert!(matches!(
            primary_vector_error(path.clone(), SearchArtifactError::Cancelled),
            SearchArtifactError::CorruptPrimaryVectors { path: actual, reason }
                if actual == path && reason.contains("cancelled")
        ));

        for valid in ["build-A1", "version-z9"] {
            let prefix = if valid.starts_with("build") {
                "build-"
            } else {
                "version-"
            };
            assert!(valid_owned_name(valid, prefix));
        }
        for invalid in ["build-", "build-a/b", "build-a_b", "other-a"] {
            assert!(!valid_owned_name(invalid, "build-"));
        }
        for (name, expected) in [
            ("current.json.A1.tmp", true),
            ("current.json..tmp", false),
            ("current.json.a_b.tmp", false),
            ("current.json.a", false),
        ] {
            assert_eq!(valid_pointer_temp(name), expected);
        }
    }

    #[cfg(unix)]
    #[test]
    fn syncing_build_with_symlink_fails_without_following_or_mutating_target() {
        use std::os::unix::fs::symlink;

        let build = TempDir::new().unwrap();
        let external = TempDir::new().unwrap();
        let target = external.path().join("secret");
        std::fs::write(&target, b"caller bytes").unwrap();
        let link = build.path().join("linked");
        symlink(&target, &link).unwrap();
        assert!(matches!(
            sync_tree(build.path()),
            Err(SearchArtifactError::Build(_))
        ));
        assert_eq!(std::fs::read(&target).unwrap(), b"caller bytes");
        assert!(link.symlink_metadata().unwrap().file_type().is_symlink());
    }

    #[test]
    fn wave10_writer_timeout_and_cleanup_bounds_are_fail_closed() {
        let project = TempDir::new().unwrap();
        let root = key().artifact_root(project.path());
        std::fs::create_dir_all(&root).unwrap();
        let first =
            SearchWriterLock::acquire(&root, SearchCoordinationLimits::default(), &mut || Ok(()))
                .unwrap();
        let zero_wait = SearchCoordinationLimits {
            lock_timeout: Duration::ZERO,
            lock_poll_interval: Duration::ZERO,
            ..SearchCoordinationLimits::default()
        };
        assert!(matches!(
            SearchWriterLock::acquire(&root, zero_wait, &mut || Ok(())),
            Err(SearchArtifactError::Lock { .. })
        ));
        drop(first);

        let cleanup = project.path().join("indexes/search/owned");
        std::fs::create_dir_all(&cleanup).unwrap();
        std::fs::write(cleanup.join("caller"), b"preserve").unwrap();
        assert!(matches!(
            cleanup_abandoned_search_builds(project.path(), 0),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "cleanup_entries",
                ..
            })
        ));
        assert_eq!(std::fs::read(cleanup.join("caller")).unwrap(), b"preserve");
    }
}
