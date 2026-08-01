//! Bounded discovery of compatibility-addressed embedding generations.

use std::path::{Path, PathBuf};

use crate::{
    EmbeddingCompatibilityDescriptor, EmbeddingCompatibilityId, EmbeddingGenerationPublication,
    SearchArtifactError, VectorStoreLimits, current_embedding_generation,
};

/// Maximum compatibility lineages returned by one default discovery.
pub const MAX_DISCOVERED_EMBEDDING_SPACES: usize = 1_024;
/// Maximum filesystem entries inspected by one default discovery.
pub const MAX_EMBEDDING_SPACE_DIRECTORY_ENTRIES: usize = 2_048;
/// Maximum bytes accepted from one compatibility descriptor.
pub const MAX_DISCOVERED_EMBEDDING_DESCRIPTOR_BYTES: u64 = 64 * 1024;

const EMBEDDINGS_DIR: &str = "embeddings";
const SPACES_DIR: &str = "spaces";
const DESCRIPTOR_FILE: &str = "space.json";

/// Resource bounds for complete embedding-space discovery.
#[derive(Clone, Copy, Debug)]
pub struct EmbeddingSpaceDiscoveryLimits {
    /// Maximum compatibility lineages returned.
    pub spaces: usize,
    /// Maximum raw entries inspected under `embeddings/spaces`.
    pub directory_entries: usize,
    /// Maximum bytes accepted from one `space.json` descriptor.
    pub descriptor_bytes: u64,
    /// Bounds used to validate every active primary vector generation.
    pub vectors: VectorStoreLimits,
}

impl Default for EmbeddingSpaceDiscoveryLimits {
    fn default() -> Self {
        Self {
            spaces: MAX_DISCOVERED_EMBEDDING_SPACES,
            directory_entries: MAX_EMBEDDING_SPACE_DIRECTORY_ENTRIES,
            descriptor_bytes: MAX_DISCOVERED_EMBEDDING_DESCRIPTOR_BYTES,
            vectors: VectorStoreLimits::default(),
        }
    }
}

/// One validated compatibility lineage and its optional complete active generation.
#[derive(Clone, Debug, PartialEq)]
pub struct DiscoveredEmbeddingSpace {
    compatibility_id: EmbeddingCompatibilityId,
    descriptor: EmbeddingCompatibilityDescriptor,
    active: Option<EmbeddingGenerationPublication>,
}

impl DiscoveredEmbeddingSpace {
    /// Exact digest-addressed compatibility identity.
    #[must_use]
    pub const fn compatibility_id(&self) -> EmbeddingCompatibilityId {
        self.compatibility_id
    }

    /// Fully validated compatibility descriptor reopened from `space.json`.
    #[must_use]
    pub const fn descriptor(&self) -> &EmbeddingCompatibilityDescriptor {
        &self.descriptor
    }

    /// Fully verified active generation, or `None` before first publication.
    #[must_use]
    pub const fn active(&self) -> Option<&EmbeddingGenerationPublication> {
        self.active.as_ref()
    }
}

/// Discover every compatibility-addressed embedding lineage deterministically.
///
/// A missing `embeddings/spaces` tree returns an empty vector without creating
/// directories. Every present lineage is validated from its path through its
/// descriptor and, when active, through the complete primary-vector reopen path.
///
/// # Errors
/// Returns structured cancellation, limit, corruption, incompatibility, or I/O errors.
pub fn discover_embedding_spaces<C>(
    project_dir: &Path,
    limits: EmbeddingSpaceDiscoveryLimits,
    mut checkpoint: C,
) -> Result<Vec<DiscoveredEmbeddingSpace>, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    validate_limits(limits)?;
    checkpoint()?;
    let embeddings = project_dir.join(EMBEDDINGS_DIR);
    if !path_exists(&embeddings)? {
        return Ok(Vec::new());
    }
    ensure_owned_directory(&embeddings)?;
    let spaces_root = embeddings.join(SPACES_DIR);
    if !path_exists(&spaces_root)? {
        return Ok(Vec::new());
    }
    ensure_owned_directory(&spaces_root)?;

    let mut discovered = Vec::new();
    let mut inspected = 0_usize;
    let entries = std::fs::read_dir(&spaces_root)
        .map_err(|source| io("enumerate embedding spaces", &spaces_root, source))?;
    for entry in entries {
        checkpoint()?;
        inspected = inspected.checked_add(1).ok_or_else(|| {
            exhausted(
                "embedding_space_directory_entries",
                limits.directory_entries,
            )
        })?;
        if inspected > limits.directory_entries {
            return Err(exhausted(
                "embedding_space_directory_entries",
                limits.directory_entries,
            ));
        }
        if discovered.len() >= limits.spaces {
            return Err(exhausted("embedding_spaces", limits.spaces));
        }
        let entry =
            entry.map_err(|source| io("read embedding space entry", &spaces_root, source))?;
        let path = entry.path();
        ensure_owned_directory(&path)?;
        let path_identity = entry
            .file_name()
            .to_str()
            .ok_or_else(|| corrupt(&path, "space directory name is not UTF-8"))
            .and_then(|value| {
                EmbeddingCompatibilityId::from_hex(value)
                    .map_err(|error| corrupt(&path, error.to_string()))
            })?;
        if path_exists(&crate::embedding_publication::deletion_marker(
            project_dir,
            path_identity,
        ))? {
            continue;
        }
        let descriptor = read_descriptor(&path, limits.descriptor_bytes)?;
        let descriptor_identity = descriptor.compatibility_id()?;
        if descriptor_identity != path_identity {
            return Err(corrupt(
                &path,
                "descriptor compatibility identity does not match its directory",
            ));
        }
        checkpoint()?;
        let active = current_embedding_generation(
            project_dir,
            &descriptor,
            limits.vectors,
            &mut checkpoint,
        )?;
        discovered.push(DiscoveredEmbeddingSpace {
            compatibility_id: path_identity,
            descriptor,
            active,
        });
    }
    discovered.sort_unstable_by_key(DiscoveredEmbeddingSpace::compatibility_id);
    Ok(discovered)
}

fn read_descriptor(
    space_root: &Path,
    max_bytes: u64,
) -> Result<EmbeddingCompatibilityDescriptor, SearchArtifactError> {
    let path = space_root.join(DESCRIPTOR_FILE);
    ensure_regular_file(&path)?;
    let metadata = std::fs::metadata(&path)
        .map_err(|source| io("inspect embedding descriptor", &path, source))?;
    if metadata.len() > max_bytes {
        return Err(SearchArtifactError::ResourceExhausted {
            resource: "embedding_descriptor_bytes",
            limit: max_bytes,
        });
    }
    let bytes =
        std::fs::read(&path).map_err(|source| io("read embedding descriptor", &path, source))?;
    EmbeddingCompatibilityDescriptor::from_json(&path, &bytes)
        .map_err(|error| primary_from(space_root, error))
}

fn validate_limits(limits: EmbeddingSpaceDiscoveryLimits) -> Result<(), SearchArtifactError> {
    if limits.spaces == 0 || limits.directory_entries == 0 || limits.descriptor_bytes == 0 {
        Err(invalid(
            "embedding discovery limits",
            "must all be non-zero",
        ))
    } else {
        Ok(())
    }
}

fn ensure_owned_directory(path: &Path) -> Result<(), SearchArtifactError> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|source| io("inspect embedding directory", path, source))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(corrupt(path, "expected an owned directory"));
    }
    Ok(())
}

fn ensure_regular_file(path: &Path) -> Result<(), SearchArtifactError> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|source| io("inspect embedding descriptor", path, source))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(corrupt(path, "expected a regular descriptor file"));
    }
    Ok(())
}

fn path_exists(path: &Path) -> Result<bool, SearchArtifactError> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(io("inspect embedding path", path, source)),
    }
}

fn primary_from(path: &Path, error: SearchArtifactError) -> SearchArtifactError {
    match error {
        SearchArtifactError::IncompatibleManifest { .. }
        | SearchArtifactError::ResourceExhausted { .. }
        | SearchArtifactError::Cancelled => error,
        other => SearchArtifactError::CorruptPrimaryVectors {
            path: path.to_path_buf(),
            reason: other.to_string(),
        },
    }
}

fn invalid(field: &'static str, reason: impl Into<String>) -> SearchArtifactError {
    SearchArtifactError::InvalidSelector {
        field,
        reason: reason.into(),
    }
}

fn corrupt(path: &Path, reason: impl Into<String>) -> SearchArtifactError {
    SearchArtifactError::CorruptPrimaryVectors {
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
        path: PathBuf::from(path),
        source,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;
    use crate::{
        EmbeddingBatchRow, EmbeddingCompatibilityInput, EmbeddingDistance, EmbeddingNormalization,
        EmbeddingProducerIdentity, EmbeddingPublicationRequest, EmbeddingSourceState,
        EmbeddingValueType, SearchCoordinationLimits, VECTOR_DATA_FILE, ValidatedEmbeddingBatch,
        publish_embedding_generation, validate_embedding_batch,
    };

    fn descriptor(model: &str) -> EmbeddingCompatibilityDescriptor {
        EmbeddingCompatibilityDescriptor::new(EmbeddingCompatibilityInput {
            producer: EmbeddingProducerIdentity::Local {
                implementation: "discovery-test".to_owned(),
                model: model.to_owned(),
                revision: "r1".to_owned(),
                contract_version: "v1".to_owned(),
            },
            dimensions: 2,
            value_type: EmbeddingValueType::Float32,
            normalization: EmbeddingNormalization::None,
            distance: EmbeddingDistance::Cosine,
            tokenizer: None,
            chunking: None,
            hyperparameters: BTreeMap::new(),
            input_recipe: BTreeMap::from([("property".to_owned(), "body".into())]),
            source_projection_recipe: BTreeMap::from([("label".to_owned(), "Document".into())]),
        })
        .unwrap()
    }

    fn batch(uuid: [u8; 16]) -> ValidatedEmbeddingBatch {
        validate_embedding_batch(
            vec![EmbeddingBatchRow {
                node_uuid: uuid,
                vector: vec![1.0, 2.0],
            }],
            &BTreeSet::from([uuid]),
            2,
            EmbeddingNormalization::None,
            VectorStoreLimits::default(),
            || Ok(()),
        )
        .unwrap()
    }

    fn publish(
        project: &Path,
        descriptor: &EmbeddingCompatibilityDescriptor,
        marker: u8,
    ) -> PathBuf {
        publish_embedding_generation(
            project,
            EmbeddingPublicationRequest {
                descriptor,
                source: EmbeddingSourceState::new(1, [marker; 32], [marker + 1; 32], 1),
                batch: &batch([marker; 16]),
                generated_at_micros: 10,
                committed_at_micros: 11,
            },
            VectorStoreLimits::default(),
            SearchCoordinationLimits::default(),
            || Ok(()),
        )
        .unwrap()
        .publication()
        .path
        .clone()
    }

    fn descriptor_only(project: &Path, descriptor: &EmbeddingCompatibilityDescriptor) {
        let identity = descriptor.compatibility_id().unwrap();
        let root = project
            .join(EMBEDDINGS_DIR)
            .join(SPACES_DIR)
            .join(identity.to_hex());
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(
            root.join(DESCRIPTOR_FILE),
            descriptor.to_canonical_json().unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn missing_and_empty_trees_return_no_spaces_without_creating_files() {
        let project = tempfile::tempdir().unwrap();
        assert!(
            discover_embedding_spaces(
                project.path(),
                EmbeddingSpaceDiscoveryLimits::default(),
                || Ok(())
            )
            .unwrap()
            .is_empty()
        );
        assert!(!project.path().join(EMBEDDINGS_DIR).exists());
        std::fs::create_dir_all(project.path().join(EMBEDDINGS_DIR).join(SPACES_DIR)).unwrap();
        assert!(
            discover_embedding_spaces(
                project.path(),
                EmbeddingSpaceDiscoveryLimits::default(),
                || Ok(())
            )
            .unwrap()
            .is_empty()
        );
    }

    #[test]
    fn active_and_descriptor_only_lineages_are_sorted_and_reopen_completely() {
        let project = tempfile::tempdir().unwrap();
        let a = descriptor("a");
        let b = descriptor("b");
        let _ = publish(project.path(), &b, 2);
        descriptor_only(project.path(), &a);

        let discovered = discover_embedding_spaces(
            project.path(),
            EmbeddingSpaceDiscoveryLimits::default(),
            || Ok(()),
        )
        .unwrap();
        assert_eq!(discovered.len(), 2);
        assert!(
            discovered
                .windows(2)
                .all(|pair| pair[0].compatibility_id() < pair[1].compatibility_id())
        );
        let active = discovered
            .iter()
            .find(|space| space.descriptor() == &b)
            .unwrap()
            .active()
            .unwrap();
        assert_eq!(active.manifest.vector_count(), 1);
        assert!(
            discovered
                .iter()
                .find(|space| space.descriptor() == &a)
                .unwrap()
                .active()
                .is_none()
        );
    }

    #[test]
    fn hostile_entries_identity_corruption_limits_and_cancellation_fail_closed() {
        let project = tempfile::tempdir().unwrap();
        let stable = descriptor("stable");
        descriptor_only(project.path(), &stable);
        let spaces = project.path().join(EMBEDDINGS_DIR).join(SPACES_DIR);

        std::fs::write(spaces.join("not-a-space"), b"hostile").unwrap();
        assert!(matches!(
            discover_embedding_spaces(
                project.path(),
                EmbeddingSpaceDiscoveryLimits::default(),
                || Ok(())
            ),
            Err(SearchArtifactError::CorruptPrimaryVectors { .. })
        ));
        std::fs::remove_file(spaces.join("not-a-space")).unwrap();

        let wrong = descriptor("wrong");
        let stable_root = spaces.join(stable.compatibility_id().unwrap().to_hex());
        std::fs::write(
            stable_root.join(DESCRIPTOR_FILE),
            wrong.to_canonical_json().unwrap(),
        )
        .unwrap();
        assert!(matches!(
            discover_embedding_spaces(
                project.path(),
                EmbeddingSpaceDiscoveryLimits::default(),
                || Ok(())
            ),
            Err(SearchArtifactError::CorruptPrimaryVectors { .. })
        ));
        std::fs::write(
            stable_root.join(DESCRIPTOR_FILE),
            stable.to_canonical_json().unwrap(),
        )
        .unwrap();

        assert!(matches!(
            discover_embedding_spaces(
                project.path(),
                EmbeddingSpaceDiscoveryLimits {
                    spaces: 1,
                    directory_entries: 1,
                    descriptor_bytes: 8,
                    vectors: VectorStoreLimits::default(),
                },
                || Ok(())
            ),
            Err(SearchArtifactError::ResourceExhausted { .. })
        ));
        assert!(matches!(
            discover_embedding_spaces(
                project.path(),
                EmbeddingSpaceDiscoveryLimits::default(),
                || Err(SearchArtifactError::Cancelled)
            ),
            Err(SearchArtifactError::Cancelled)
        ));
    }

    #[test]
    fn corrupt_active_primary_vectors_are_never_reported_as_absent() {
        let project = tempfile::tempdir().unwrap();
        let descriptor = descriptor("corrupt-primary");
        let generation = publish(project.path(), &descriptor, 4);
        std::fs::write(generation.join(VECTOR_DATA_FILE), b"corrupt").unwrap();
        assert!(matches!(
            discover_embedding_spaces(
                project.path(),
                EmbeddingSpaceDiscoveryLimits::default(),
                || Ok(())
            ),
            Err(SearchArtifactError::CorruptPrimaryVectors { .. })
        ));
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_lineage_fails_closed() {
        use std::os::unix::fs::symlink;

        let project = tempfile::tempdir().unwrap();
        let spaces = project.path().join(EMBEDDINGS_DIR).join(SPACES_DIR);
        std::fs::create_dir_all(&spaces).unwrap();
        let target = tempfile::tempdir().unwrap();
        symlink(target.path(), spaces.join("0".repeat(64))).unwrap();
        assert!(matches!(
            discover_embedding_spaces(
                project.path(),
                EmbeddingSpaceDiscoveryLimits::default(),
                || Ok(())
            ),
            Err(SearchArtifactError::CorruptPrimaryVectors { .. })
        ));
    }
}
