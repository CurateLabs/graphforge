//! Durable caller-facing names for compatibility-addressed embedding spaces.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::{
    EmbeddingCompatibilityId, EmbeddingDisplayName, SearchArtifactError, SearchCoordinationLimits,
};

/// Embedding-space catalog schema implemented by this release.
pub const EMBEDDING_SPACE_CATALOG_VERSION: u32 = 1;
/// Maximum accepted catalog bytes by default.
pub const MAX_EMBEDDING_SPACE_CATALOG_BYTES: usize = 64 * 1024;
/// Maximum accepted named spaces by default.
pub const MAX_EMBEDDING_SPACE_CATALOG_ENTRIES: usize = 1_024;

const EMBEDDINGS_DIR: &str = "embeddings";
const CATALOG_FILE: &str = "catalog.json";
const CATALOG_LOCK: &str = ".catalog.lock";

/// Resource and coordination bounds for embedding-space catalog access.
#[derive(Clone, Copy, Debug)]
pub struct EmbeddingSpaceCatalogLimits {
    /// Maximum canonical JSON bytes.
    pub metadata_bytes: usize,
    /// Maximum distinct display names.
    pub entries: usize,
    /// Catalog-writer lock bounds.
    pub coordination: SearchCoordinationLimits,
}

impl Default for EmbeddingSpaceCatalogLimits {
    fn default() -> Self {
        Self {
            metadata_bytes: MAX_EMBEDDING_SPACE_CATALOG_BYTES,
            entries: MAX_EMBEDDING_SPACE_CATALOG_ENTRIES,
            coordination: SearchCoordinationLimits::default(),
        }
    }
}

/// One deterministic caller-facing name to compatibility-identity binding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmbeddingSpaceCatalogEntry {
    display_name: EmbeddingDisplayName,
    compatibility_id: EmbeddingCompatibilityId,
}

impl EmbeddingSpaceCatalogEntry {
    /// Normalized caller-facing name, never a path component.
    #[must_use]
    pub const fn display_name(&self) -> &EmbeddingDisplayName {
        &self.display_name
    }

    /// Exact compatibility lineage selected by this name.
    #[must_use]
    pub const fn compatibility_id(&self) -> EmbeddingCompatibilityId {
        self.compatibility_id
    }
}

/// Fully validated catalog returned in deterministic display-name order.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EmbeddingSpaceCatalog {
    spaces: BTreeMap<EmbeddingDisplayName, EmbeddingCompatibilityId>,
    default: Option<EmbeddingDisplayName>,
}

impl EmbeddingSpaceCatalog {
    /// Number of distinct named spaces.
    #[must_use]
    pub fn len(&self) -> usize {
        self.spaces.len()
    }

    /// Whether no display names are configured.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.spaces.is_empty()
    }

    /// Exact compatibility identity bound to one normalized name.
    #[must_use]
    pub fn get(&self, display_name: &EmbeddingDisplayName) -> Option<EmbeddingCompatibilityId> {
        self.spaces.get(display_name).copied()
    }

    /// Optional configured default and its exact compatibility identity.
    #[must_use]
    pub fn selected_default(&self) -> Option<EmbeddingSpaceCatalogEntry> {
        self.default.as_ref().map(|display_name| {
            let compatibility_id = self.spaces[display_name];
            EmbeddingSpaceCatalogEntry {
                display_name: display_name.clone(),
                compatibility_id,
            }
        })
    }

    /// All bindings in deterministic normalized display-name order.
    #[must_use]
    pub fn entries(&self) -> Vec<EmbeddingSpaceCatalogEntry> {
        self.spaces
            .iter()
            .map(
                |(display_name, compatibility_id)| EmbeddingSpaceCatalogEntry {
                    display_name: display_name.clone(),
                    compatibility_id: *compatibility_id,
                },
            )
            .collect()
    }

    fn to_canonical_json(
        &self,
        limits: EmbeddingSpaceCatalogLimits,
    ) -> Result<Vec<u8>, SearchArtifactError> {
        validate_limits(limits)?;
        if self.spaces.len() > limits.entries {
            return Err(exhausted("embedding_space_catalog_entries", limits.entries));
        }
        let spaces = self
            .spaces
            .iter()
            .map(|(display_name, compatibility_id)| WireEntry {
                display_name: display_name.as_str(),
                compatibility_id: compatibility_id.to_hex(),
            })
            .collect::<Vec<_>>();
        let bytes = serde_json::to_vec(&WireCatalog {
            catalog_version: EMBEDDING_SPACE_CATALOG_VERSION,
            default: self.default.as_ref().map(EmbeddingDisplayName::as_str),
            spaces,
        })
        .map_err(|error| invalid("embedding space catalog", error.to_string()))?;
        if bytes.len() > limits.metadata_bytes {
            return Err(exhausted(
                "embedding_space_catalog_bytes",
                limits.metadata_bytes,
            ));
        }
        Ok(bytes)
    }
}

/// One explicit mutation of the durable display-name catalog.
#[derive(Clone, Copy, Debug)]
pub enum EmbeddingSpaceCatalogUpdate<'a> {
    /// Bind a normalized display name to one exact compatibility identity.
    Bind {
        /// Caller-facing name.
        display_name: &'a str,
        /// Exact compatibility lineage.
        compatibility_id: EmbeddingCompatibilityId,
        /// Permit replacing a different existing identity when `true`.
        replace: bool,
    },
    /// Remove one name. Missing names are idempotent.
    Remove {
        /// Caller-facing name.
        display_name: &'a str,
    },
    /// Select an existing name as default, or clear the default with `None`.
    SetDefault {
        /// Existing caller-facing name, or `None`.
        display_name: Option<&'a str>,
    },
}

/// Read and validate the durable embedding-space catalog.
///
/// A missing file returns an empty catalog without creating directories.
///
/// # Errors
/// Returns structured cancellation, limit, corruption, incompatibility, or I/O errors.
pub fn read_embedding_space_catalog<C>(
    project_dir: &Path,
    limits: EmbeddingSpaceCatalogLimits,
    mut checkpoint: C,
) -> Result<EmbeddingSpaceCatalog, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    validate_limits(limits)?;
    checkpoint()?;
    read_catalog_file(&catalog_path(project_dir), limits)
}

/// Apply one catalog mutation under a bounded cross-process writer lock.
///
/// Exact-idempotent binds and missing removals do not rewrite durable bytes.
/// Publication uses a synchronized temporary file and atomic replacement.
///
/// # Errors
/// Returns structured validation, cancellation, lock, limit, corruption, or I/O errors.
pub fn update_embedding_space_catalog<C>(
    project_dir: &Path,
    update: EmbeddingSpaceCatalogUpdate<'_>,
    limits: EmbeddingSpaceCatalogLimits,
    checkpoint: C,
) -> Result<EmbeddingSpaceCatalog, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    mutate_embedding_space_catalog(project_dir, update, limits, checkpoint)
        .map(|(catalog, _changed)| catalog)
}

/// Remove one display name and report the mutation outcome from inside the
/// catalog writer lock.
///
/// Missing names are idempotent and return `false`. If the removed name was
/// the selected default, the default is cleared in the same atomic update.
///
/// # Errors
/// Returns structured validation, cancellation, lock, limit, corruption, or I/O errors.
pub fn remove_embedding_space_catalog_entry<C>(
    project_dir: &Path,
    display_name: &str,
    limits: EmbeddingSpaceCatalogLimits,
    checkpoint: C,
) -> Result<bool, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    mutate_embedding_space_catalog(
        project_dir,
        EmbeddingSpaceCatalogUpdate::Remove { display_name },
        limits,
        checkpoint,
    )
    .map(|(_catalog, changed)| changed)
}

/// Bind one display name only while its compatibility lineage is present and
/// not being deleted.
///
/// The existence check occurs inside the catalog writer lock so a concurrent
/// deletion cannot leave a newly dangling alias.
///
/// # Errors
/// Returns structured validation, cancellation, lock, corruption, or I/O errors.
pub fn bind_existing_embedding_space_catalog_entry<C>(
    project_dir: &Path,
    display_name: &str,
    compatibility_id: EmbeddingCompatibilityId,
    replace: bool,
    limits: EmbeddingSpaceCatalogLimits,
    mut checkpoint: C,
) -> Result<EmbeddingSpaceCatalog, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    validate_limits(limits)?;
    checkpoint()?;
    let embeddings = project_dir.join(EMBEDDINGS_DIR);
    ensure_owned_directory(&embeddings)?;
    let _lock = CatalogWriterLock::acquire(&embeddings, limits.coordination, &mut checkpoint)?;
    checkpoint()?;
    ensure_bindable_lineage(project_dir, compatibility_id)?;

    let path = embeddings.join(CATALOG_FILE);
    let mut catalog = read_catalog_file(&path, limits)?;
    let changed = apply_update(
        &mut catalog,
        EmbeddingSpaceCatalogUpdate::Bind {
            display_name,
            compatibility_id,
            replace,
        },
        limits,
    )?;
    if changed {
        checkpoint()?;
        persist_synced_file(&path, &catalog.to_canonical_json(limits)?)?;
    }
    Ok(catalog)
}

/// Remove every display name that targets one compatibility identity.
///
/// The configured default is cleared when it names any removed alias.
///
/// # Errors
/// Returns structured cancellation, lock, limit, corruption, or I/O errors.
pub fn remove_embedding_space_catalog_identity<C>(
    project_dir: &Path,
    compatibility_id: EmbeddingCompatibilityId,
    limits: EmbeddingSpaceCatalogLimits,
    mut checkpoint: C,
) -> Result<usize, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    validate_limits(limits)?;
    checkpoint()?;
    let embeddings = project_dir.join(EMBEDDINGS_DIR);
    ensure_owned_directory(&embeddings)?;
    let _lock = CatalogWriterLock::acquire(&embeddings, limits.coordination, &mut checkpoint)?;
    checkpoint()?;

    let path = embeddings.join(CATALOG_FILE);
    let mut catalog = read_catalog_file(&path, limits)?;
    let removed_names = catalog
        .spaces
        .iter()
        .filter_map(|(name, current)| (*current == compatibility_id).then_some(name.clone()))
        .collect::<Vec<_>>();
    if removed_names.is_empty() {
        return Ok(0);
    }
    for name in &removed_names {
        catalog.spaces.remove(name);
    }
    if catalog
        .default
        .as_ref()
        .is_some_and(|name| removed_names.contains(name))
    {
        catalog.default = None;
    }
    checkpoint()?;
    persist_synced_file(&path, &catalog.to_canonical_json(limits)?)?;
    Ok(removed_names.len())
}

fn mutate_embedding_space_catalog<C>(
    project_dir: &Path,
    update: EmbeddingSpaceCatalogUpdate<'_>,
    limits: EmbeddingSpaceCatalogLimits,
    mut checkpoint: C,
) -> Result<(EmbeddingSpaceCatalog, bool), SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    validate_limits(limits)?;
    checkpoint()?;
    let embeddings = project_dir.join(EMBEDDINGS_DIR);
    ensure_owned_directory(&embeddings)?;
    let _lock = CatalogWriterLock::acquire(&embeddings, limits.coordination, &mut checkpoint)?;
    checkpoint()?;

    let path = embeddings.join(CATALOG_FILE);
    let mut catalog = read_catalog_file(&path, limits)?;
    let changed = apply_update(&mut catalog, update, limits)?;
    if changed {
        checkpoint()?;
        let bytes = catalog.to_canonical_json(limits)?;
        checkpoint()?;
        persist_synced_file(&path, &bytes)?;
    }
    Ok((catalog, changed))
}

fn apply_update(
    catalog: &mut EmbeddingSpaceCatalog,
    update: EmbeddingSpaceCatalogUpdate<'_>,
    limits: EmbeddingSpaceCatalogLimits,
) -> Result<bool, SearchArtifactError> {
    match update {
        EmbeddingSpaceCatalogUpdate::Bind {
            display_name,
            compatibility_id,
            replace,
        } => {
            let display_name = EmbeddingDisplayName::new(display_name)?;
            match catalog.spaces.get(&display_name).copied() {
                Some(current) if current == compatibility_id => Ok(false),
                Some(_) if !replace => Err(invalid(
                    "embedding display name",
                    "is already bound to a different compatibility identity",
                )),
                _ => {
                    if !catalog.spaces.contains_key(&display_name)
                        && catalog.spaces.len() >= limits.entries
                    {
                        return Err(exhausted("embedding_space_catalog_entries", limits.entries));
                    }
                    catalog.spaces.insert(display_name, compatibility_id);
                    Ok(true)
                }
            }
        }
        EmbeddingSpaceCatalogUpdate::Remove { display_name } => {
            let display_name = EmbeddingDisplayName::new(display_name)?;
            let removed = catalog.spaces.remove(&display_name).is_some();
            if catalog.default.as_ref() == Some(&display_name) {
                catalog.default = None;
            }
            Ok(removed)
        }
        EmbeddingSpaceCatalogUpdate::SetDefault { display_name } => {
            let display_name = display_name.map(EmbeddingDisplayName::new).transpose()?;
            if display_name
                .as_ref()
                .is_some_and(|name| !catalog.spaces.contains_key(name))
            {
                return Err(invalid(
                    "embedding default space",
                    "must name an existing catalog entry",
                ));
            }
            if catalog.default == display_name {
                Ok(false)
            } else {
                catalog.default = display_name;
                Ok(true)
            }
        }
    }
}

fn read_catalog_file(
    path: &Path,
    limits: EmbeddingSpaceCatalogLimits,
) -> Result<EmbeddingSpaceCatalog, SearchArtifactError> {
    if !path_exists(path)? {
        return Ok(EmbeddingSpaceCatalog::default());
    }
    ensure_regular_file(path)?;
    let metadata = std::fs::metadata(path)
        .map_err(|source| io("inspect embedding space catalog", path, source))?;
    if metadata.len() > limits.metadata_bytes as u64 {
        return Err(exhausted(
            "embedding_space_catalog_bytes",
            limits.metadata_bytes,
        ));
    }
    let bytes =
        std::fs::read(path).map_err(|source| io("read embedding space catalog", path, source))?;
    let raw: RawCatalog =
        serde_json::from_slice(&bytes).map_err(|error| corrupt(path, error.to_string()))?;
    if raw.catalog_version != u64::from(EMBEDDING_SPACE_CATALOG_VERSION) {
        return Err(SearchArtifactError::IncompatibleManifest {
            path: path.to_path_buf(),
            found: raw.catalog_version,
            supported: EMBEDDING_SPACE_CATALOG_VERSION,
        });
    }
    if raw.spaces.len() > limits.entries {
        return Err(exhausted("embedding_space_catalog_entries", limits.entries));
    }

    let mut spaces = BTreeMap::new();
    let mut seen = BTreeSet::new();
    for entry in raw.spaces {
        let display_name = EmbeddingDisplayName::new(&entry.display_name)
            .map_err(|error| corrupt(path, error.to_string()))?;
        if !seen.insert(display_name.clone()) {
            return Err(corrupt(path, "duplicate embedding display name"));
        }
        let compatibility_id = EmbeddingCompatibilityId::from_hex(&entry.compatibility_id)
            .map_err(|error| corrupt(path, error.to_string()))?;
        spaces.insert(display_name, compatibility_id);
    }
    let default = raw
        .default
        .map(|value| EmbeddingDisplayName::new(&value))
        .transpose()
        .map_err(|error| corrupt(path, error.to_string()))?;
    if default
        .as_ref()
        .is_some_and(|display_name| !spaces.contains_key(display_name))
    {
        return Err(corrupt(path, "default does not name a catalog entry"));
    }
    let catalog = EmbeddingSpaceCatalog { spaces, default };
    let canonical = catalog
        .to_canonical_json(limits)
        .map_err(|error| corrupt(path, error.to_string()))?;
    if canonical != bytes {
        return Err(corrupt(path, "catalog bytes are not exact canonical JSON"));
    }
    Ok(catalog)
}

#[derive(Serialize)]
struct WireCatalog<'a> {
    catalog_version: u32,
    default: Option<&'a str>,
    spaces: Vec<WireEntry<'a>>,
}

#[derive(Serialize)]
struct WireEntry<'a> {
    display_name: &'a str,
    compatibility_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCatalog {
    catalog_version: u64,
    default: Option<String>,
    spaces: Vec<RawEntry>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawEntry {
    display_name: String,
    compatibility_id: String,
}

struct CatalogWriterLock {
    file: File,
}

impl CatalogWriterLock {
    fn acquire<C>(
        embeddings: &Path,
        limits: SearchCoordinationLimits,
        checkpoint: &mut C,
    ) -> Result<Self, SearchArtifactError>
    where
        C: FnMut() -> Result<(), SearchArtifactError>,
    {
        let path = embeddings.join(CATALOG_LOCK);
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

impl Drop for CatalogWriterLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

fn validate_limits(limits: EmbeddingSpaceCatalogLimits) -> Result<(), SearchArtifactError> {
    if limits.metadata_bytes == 0 || limits.entries == 0 {
        Err(invalid(
            "embedding space catalog limits",
            "must be non-zero",
        ))
    } else {
        Ok(())
    }
}

fn catalog_path(project_dir: &Path) -> PathBuf {
    project_dir.join(EMBEDDINGS_DIR).join(CATALOG_FILE)
}

fn ensure_bindable_lineage(
    project_dir: &Path,
    compatibility_id: EmbeddingCompatibilityId,
) -> Result<(), SearchArtifactError> {
    let marker = crate::embedding_publication::deletion_marker(project_dir, compatibility_id);
    if path_exists(&marker)? {
        return Err(invalid(
            "embedding compatibility identity",
            "deletion is in progress",
        ));
    }
    let root = project_dir
        .join(EMBEDDINGS_DIR)
        .join("spaces")
        .join(compatibility_id.to_hex());
    let metadata = std::fs::symlink_metadata(&root).map_err(|source| {
        if source.kind() == std::io::ErrorKind::NotFound {
            invalid(
                "embedding compatibility identity",
                "is not a published lineage",
            )
        } else {
            io("inspect embedding lineage", &root, source)
        }
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(corrupt(
            &root,
            "expected an owned embedding lineage directory",
        ));
    }
    ensure_regular_file(&root.join("space.json"))
}

fn ensure_owned_directory(path: &Path) -> Result<(), SearchArtifactError> {
    if path_exists(path)? {
        let metadata = std::fs::symlink_metadata(path)
            .map_err(|source| io("inspect embedding catalog directory", path, source))?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(corrupt(path, "expected an owned directory"));
        }
        return Ok(());
    }
    std::fs::create_dir_all(path)
        .map_err(|source| io("create embedding catalog directory", path, source))?;
    sync_directory(path.parent().unwrap_or(path))
}

fn ensure_regular_file(path: &Path) -> Result<(), SearchArtifactError> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|source| io("inspect embedding space catalog", path, source))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(corrupt(path, "expected a regular file"));
    }
    Ok(())
}

fn persist_synced_file(path: &Path, bytes: &[u8]) -> Result<(), SearchArtifactError> {
    let parent = path
        .parent()
        .ok_or_else(|| invalid("embedding space catalog", "path has no parent"))?;
    let mut temp = tempfile::Builder::new()
        .prefix(".catalog.json.")
        .suffix(".tmp")
        .tempfile_in(parent)
        .map_err(|source| io("create embedding catalog temp", path, source))?;
    temp.write_all(bytes)
        .map_err(|source| io("write embedding catalog temp", path, source))?;
    temp.as_file()
        .sync_all()
        .map_err(|source| io("sync embedding catalog temp", path, source))?;
    temp.persist(path)
        .map_err(|error| io("publish embedding space catalog", path, error.error))?;
    sync_directory(parent)
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<(), SearchArtifactError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| io("sync embedding catalog directory", path, source))
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<(), SearchArtifactError> {
    Ok(())
}

fn path_exists(path: &Path) -> Result<bool, SearchArtifactError> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(io("inspect embedding catalog path", path, source)),
    }
}

fn invalid(field: &'static str, reason: impl Into<String>) -> SearchArtifactError {
    SearchArtifactError::InvalidSelector {
        field,
        reason: reason.into(),
    }
}

fn corrupt(path: &Path, reason: impl Into<String>) -> SearchArtifactError {
    SearchArtifactError::CorruptManifest {
        path: path.to_path_buf(),
        reason: reason.into(),
    }
}

fn exhausted(resource: &'static str, limit: usize) -> SearchArtifactError {
    SearchArtifactError::ResourceExhausted {
        resource,
        limit: limit as u64,
    }
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
    use std::cell::Cell;

    use super::*;

    fn id(value: u8) -> EmbeddingCompatibilityId {
        EmbeddingCompatibilityId::from_hex(&format!("{value:02x}").repeat(32)).unwrap()
    }

    fn read(project: &Path) -> EmbeddingSpaceCatalog {
        read_embedding_space_catalog(project, EmbeddingSpaceCatalogLimits::default(), || Ok(()))
            .unwrap()
    }

    fn update(project: &Path, mutation: EmbeddingSpaceCatalogUpdate<'_>) -> EmbeddingSpaceCatalog {
        update_embedding_space_catalog(
            project,
            mutation,
            EmbeddingSpaceCatalogLimits::default(),
            || Ok(()),
        )
        .unwrap()
    }

    #[test]
    fn missing_catalog_is_empty_and_does_not_create_files() {
        let project = tempfile::tempdir().unwrap();
        assert!(read(project.path()).is_empty());
        assert!(!project.path().join(EMBEDDINGS_DIR).exists());
    }

    #[test]
    fn bindings_replacement_default_and_removal_are_durable_and_ordered() {
        let project = tempfile::tempdir().unwrap();
        update(
            project.path(),
            EmbeddingSpaceCatalogUpdate::Bind {
                display_name: "zeta",
                compatibility_id: id(1),
                replace: false,
            },
        );
        let path = catalog_path(project.path());
        let first_bytes = std::fs::read(&path).unwrap();
        update(
            project.path(),
            EmbeddingSpaceCatalogUpdate::Bind {
                display_name: "zeta",
                compatibility_id: id(1),
                replace: false,
            },
        );
        assert_eq!(std::fs::read(&path).unwrap(), first_bytes);

        assert!(matches!(
            update_embedding_space_catalog(
                project.path(),
                EmbeddingSpaceCatalogUpdate::Bind {
                    display_name: "zeta",
                    compatibility_id: id(2),
                    replace: false,
                },
                EmbeddingSpaceCatalogLimits::default(),
                || Ok(())
            ),
            Err(SearchArtifactError::InvalidSelector { .. })
        ));
        update(
            project.path(),
            EmbeddingSpaceCatalogUpdate::Bind {
                display_name: "zeta",
                compatibility_id: id(2),
                replace: true,
            },
        );
        update(
            project.path(),
            EmbeddingSpaceCatalogUpdate::Bind {
                display_name: "alpha",
                compatibility_id: id(3),
                replace: false,
            },
        );
        let selected = update(
            project.path(),
            EmbeddingSpaceCatalogUpdate::SetDefault {
                display_name: Some("zeta"),
            },
        );
        assert_eq!(
            selected
                .entries()
                .iter()
                .map(|entry| entry.display_name().as_str())
                .collect::<Vec<_>>(),
            ["alpha", "zeta"]
        );
        assert_eq!(
            selected.selected_default().unwrap().compatibility_id(),
            id(2)
        );
        assert_eq!(read(project.path()), selected);

        let removed = update(
            project.path(),
            EmbeddingSpaceCatalogUpdate::Remove {
                display_name: "zeta",
            },
        );
        assert!(removed.selected_default().is_none());
        let bytes = std::fs::read_to_string(&path).unwrap();
        assert!(!bytes.contains("zeta"));
        assert!(!bytes.contains("credential"));
        assert!(!bytes.contains("vector"));
        assert!(!bytes.contains("payload"));
    }

    #[test]
    fn invalid_default_corruption_version_limits_and_cancellation_fail_closed() {
        let project = tempfile::tempdir().unwrap();
        assert!(matches!(
            update_embedding_space_catalog(
                project.path(),
                EmbeddingSpaceCatalogUpdate::SetDefault {
                    display_name: Some("missing"),
                },
                EmbeddingSpaceCatalogLimits::default(),
                || Ok(())
            ),
            Err(SearchArtifactError::InvalidSelector { .. })
        ));
        update(
            project.path(),
            EmbeddingSpaceCatalogUpdate::Bind {
                display_name: "stable",
                compatibility_id: id(1),
                replace: false,
            },
        );
        let path = catalog_path(project.path());
        let stable = std::fs::read(&path).unwrap();
        let calls = Cell::new(0_u8);
        assert!(matches!(
            update_embedding_space_catalog(
                project.path(),
                EmbeddingSpaceCatalogUpdate::Bind {
                    display_name: "cancelled",
                    compatibility_id: id(2),
                    replace: false,
                },
                EmbeddingSpaceCatalogLimits::default(),
                || {
                    let next = calls.get() + 1;
                    calls.set(next);
                    if next >= 4 {
                        Err(SearchArtifactError::Cancelled)
                    } else {
                        Ok(())
                    }
                }
            ),
            Err(SearchArtifactError::Cancelled)
        ));
        assert_eq!(std::fs::read(&path).unwrap(), stable);

        std::fs::write(
            &path,
            b"{\"catalog_version\":2,\"default\":null,\"spaces\":[]}",
        )
        .unwrap();
        assert!(matches!(
            read_embedding_space_catalog(
                project.path(),
                EmbeddingSpaceCatalogLimits::default(),
                || Ok(())
            ),
            Err(SearchArtifactError::IncompatibleManifest { .. })
        ));
        std::fs::write(
            &path,
            b"{\"catalog_version\":1,\"default\":null,\"spaces\":[],\"extra\":1}",
        )
        .unwrap();
        assert!(matches!(
            read_embedding_space_catalog(
                project.path(),
                EmbeddingSpaceCatalogLimits::default(),
                || Ok(())
            ),
            Err(SearchArtifactError::CorruptManifest { .. })
        ));
        std::fs::write(
            &path,
            format!(
                "{{\"catalog_version\":1,\"default\":null,\"spaces\":[{{\"display_name\":\"duplicate\",\"compatibility_id\":\"{}\"}},{{\"display_name\":\"duplicate\",\"compatibility_id\":\"{}\"}}]}}",
                id(1).to_hex(),
                id(2).to_hex()
            ),
        )
        .unwrap();
        assert!(matches!(
            read_embedding_space_catalog(
                project.path(),
                EmbeddingSpaceCatalogLimits::default(),
                || Ok(())
            ),
            Err(SearchArtifactError::CorruptManifest { .. })
        ));
        std::fs::write(
            &path,
            b"{ \"catalog_version\": 1, \"default\": null, \"spaces\": [] }",
        )
        .unwrap();
        assert!(matches!(
            read_embedding_space_catalog(
                project.path(),
                EmbeddingSpaceCatalogLimits::default(),
                || Ok(())
            ),
            Err(SearchArtifactError::CorruptManifest { .. })
        ));
        assert!(matches!(
            read_embedding_space_catalog(
                project.path(),
                EmbeddingSpaceCatalogLimits {
                    metadata_bytes: 8,
                    ..EmbeddingSpaceCatalogLimits::default()
                },
                || Ok(())
            ),
            Err(SearchArtifactError::ResourceExhausted { .. })
        ));
    }

    #[cfg(not(unix))]
    #[test]
    fn directory_sync_is_a_supported_noop() {
        let directory = tempfile::tempdir().unwrap();
        sync_directory(directory.path()).unwrap();
    }
}
