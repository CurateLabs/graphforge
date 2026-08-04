//! Crash-safe publication and reopen validation for complete embedding generations.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::{
    EmbeddingCompatibilityDescriptor, EmbeddingCompatibilityId, EmbeddingContentDigest,
    EmbeddingGenerationId, EmbeddingGenerationManifest, EmbeddingGenerationManifestInput,
    EmbeddingPublicationFingerprint, EmbeddingSourceState, EmbeddingSpaceCatalogLimits,
    SearchArtifactError, SearchCoordinationLimits, StoredVector, VECTOR_DATA_FILE,
    ValidatedEmbeddingBatch, VectorStoreLimits, read_vector_snapshot,
    remove_embedding_space_catalog_identity, write_vector_snapshot,
};

const SPACE_FILE: &str = "space.json";
const ACTIVE_FILE: &str = "active.json";
const MANIFEST_FILE: &str = "manifest.json";
const GENERATIONS_DIR: &str = "generations";
const BUILD_PREFIX: &str = ".build-";
const POINTER_VERSION: u32 = 1;
const MAX_ACTIVE_BYTES: u64 = 4 * 1024;
const MAX_DESCRIPTOR_BYTES: u64 = 64 * 1024;
const MAX_MANIFEST_BYTES: u64 = 64 * 1024;
const HASH_BUFFER_BYTES: usize = 64 * 1024;
pub(crate) const EMBEDDING_DELETION_PREFIX: &str = ".deleting-";
const EMBEDDING_WRITER_LOCK_PREFIX: &str = ".writer-";

/// Complete inputs for one immutable embedding generation.
#[derive(Clone, Copy, Debug)]
pub struct EmbeddingPublicationRequest<'a> {
    /// Exact versioned compatibility descriptor for this lineage.
    pub descriptor: &'a EmbeddingCompatibilityDescriptor,
    /// Exact committed graph inputs used by the producer.
    pub source: EmbeddingSourceState,
    /// Complete validated UUID/vector projection.
    pub batch: &'a ValidatedEmbeddingBatch,
    /// Producer completion time in UTC microseconds since Unix epoch.
    pub generated_at_micros: i64,
    /// Durable publication time in UTC microseconds since Unix epoch.
    pub committed_at_micros: i64,
}

/// One fully validated active embedding generation.
#[derive(Clone, Debug, PartialEq)]
pub struct EmbeddingGenerationPublication {
    /// Digest-addressed immutable generation directory.
    pub path: PathBuf,
    /// Exact compatibility descriptor reopened from `space.json`.
    pub descriptor: EmbeddingCompatibilityDescriptor,
    /// Exact completed generation manifest.
    pub manifest: EmbeddingGenerationManifest,
}

/// Result of an atomic generation publication.
#[derive(Clone, Debug, PartialEq)]
pub enum EmbeddingPublicationOutcome {
    /// The exact complete immutable generation was already present and verified.
    Reused(EmbeddingGenerationPublication),
    /// A new complete generation became active.
    Published(EmbeddingGenerationPublication),
}

impl EmbeddingPublicationOutcome {
    /// The verified active generation selected by this operation.
    #[must_use]
    pub const fn publication(&self) -> &EmbeddingGenerationPublication {
        match self {
            Self::Reused(publication) | Self::Published(publication) => publication,
        }
    }
}

/// Publish one complete generation without exposing private or partial data.
///
/// Publication is serialized per compatibility identity. The immutable vector
/// tree and completed manifest are synchronized before the final atomic active
/// pointer replacement. Repeating identical compatibility/source/content
/// verifies and reuses the same generation directory.
///
/// # Errors
/// Rejects incompatible dimensions or descriptors, corrupt primary data,
/// configured resource exhaustion, cancellation, lock failure, and I/O errors.
pub fn publish_embedding_generation<C>(
    project_dir: &Path,
    request: EmbeddingPublicationRequest<'_>,
    vector_limits: VectorStoreLimits,
    coordination: SearchCoordinationLimits,
    mut checkpoint: C,
) -> Result<EmbeddingPublicationOutcome, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    checkpoint()?;
    let dimension = usize::try_from(request.descriptor.dimensions()).map_err(|_| {
        invalid(
            "embedding dimension",
            "cannot be represented on this platform",
        )
    })?;
    if request.batch.dimension() != dimension {
        return Err(invalid(
            "embedding batch",
            format!(
                "dimension {} does not match compatibility dimension {dimension}",
                request.batch.dimension()
            ),
        ));
    }
    let compatibility_id = request.descriptor.compatibility_id()?;
    let root = prepare_space_root(project_dir, compatibility_id)?;
    let _writer =
        EmbeddingWriterLock::acquire(project_dir, compatibility_id, coordination, &mut checkpoint)?;
    if path_exists(&deletion_marker(project_dir, compatibility_id))? {
        return Err(invalid(
            "embedding compatibility identity",
            "deletion is in progress",
        ));
    }
    persist_or_verify_descriptor(&root, request.descriptor, compatibility_id)?;

    let provisional = EmbeddingGenerationManifest::new(EmbeddingGenerationManifestInput {
        compatibility_id,
        source: request.source,
        content_digest: request.batch.content_digest(),
        vector_count: u64::try_from(request.batch.rows().len()).map_err(|_| {
            SearchArtifactError::ResourceExhausted {
                resource: "embedding_rows",
                limit: vector_limits.stored_vectors as u64,
            }
        })?,
        dimension: request.descriptor.dimensions(),
        generated_at_micros: request.generated_at_micros,
        committed_at_micros: request.committed_at_micros,
        publication_fingerprint: EmbeddingPublicationFingerprint::from_hex(&"0".repeat(64))?,
    })?;
    let generation_id = provisional.generation_id();
    let generations = root.join(GENERATIONS_DIR);
    ensure_owned_directory(&generations)?;
    let generation_path = generations.join(generation_id.to_hex());

    if path_exists(&generation_path)? {
        let publication = validate_generation(
            &root,
            request.descriptor,
            compatibility_id,
            generation_id,
            vector_limits,
            &mut checkpoint,
        )?;
        checkpoint()?;
        persist_active_pointer(&root, compatibility_id, generation_id)?;
        return Ok(EmbeddingPublicationOutcome::Reused(publication));
    }

    let (private, manifest) = build_private_generation(
        &root,
        request,
        vector_limits,
        compatibility_id,
        generation_id,
        &mut checkpoint,
    )?;

    let private_path = private.keep();
    if let Err(source) = std::fs::rename(&private_path, &generation_path) {
        let _ = std::fs::remove_dir_all(&private_path);
        return Err(io(
            "publish immutable embedding generation",
            &generation_path,
            source,
        ));
    }
    sync_directory(&generations)?;
    checkpoint()?;
    persist_active_pointer(&root, compatibility_id, generation_id)?;
    sync_directory(&root)?;

    Ok(EmbeddingPublicationOutcome::Published(
        EmbeddingGenerationPublication {
            path: generation_path,
            descriptor: request.descriptor.clone(),
            manifest,
        },
    ))
}

/// Delete one complete compatibility lineage and every catalog alias that targets it.
///
/// The per-lineage writer lock lives outside the deleted directory. A durable
/// marker blocks alias binding if interruption occurs before catalog cleanup;
/// because aliases are removed last, retry by the original display name can
/// always complete an interrupted deletion.
///
/// # Errors
/// Returns structured cancellation, lock, catalog, corruption, or I/O errors.
pub fn delete_embedding_space_lineage<C>(
    project_dir: &Path,
    compatibility_id: EmbeddingCompatibilityId,
    catalog_limits: EmbeddingSpaceCatalogLimits,
    coordination: SearchCoordinationLimits,
    mut checkpoint: C,
) -> Result<bool, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    checkpoint()?;
    let embeddings = project_dir.join("embeddings");
    if !path_exists(&embeddings)? {
        return Ok(false);
    }
    ensure_existing_directory(&embeddings)?;
    let spaces = embeddings.join("spaces");
    let root = space_root(project_dir, compatibility_id);
    if path_exists(&root)? {
        ensure_space_ancestors(project_dir, &root)?;
    }

    let _writer =
        EmbeddingWriterLock::acquire(project_dir, compatibility_id, coordination, &mut checkpoint)?;
    let marker = deletion_marker(project_dir, compatibility_id);
    if !path_exists(&marker)? {
        write_synced_file(&marker, compatibility_id.to_hex().as_bytes())?;
        sync_directory(&embeddings)?;
    }
    checkpoint()?;

    let removed_root = if path_exists(&root)? {
        ensure_space_ancestors(project_dir, &root)?;
        std::fs::remove_dir_all(&root)
            .map_err(|source| io("delete embedding space lineage", &root, source))?;
        sync_directory(&spaces)?;
        true
    } else {
        false
    };
    checkpoint()?;
    std::fs::remove_file(&marker)
        .map_err(|source| io("clear embedding deletion marker", &marker, source))?;
    sync_directory(&embeddings)?;
    checkpoint()?;
    let removed_aliases = remove_embedding_space_catalog_identity(
        project_dir,
        compatibility_id,
        catalog_limits,
        &mut checkpoint,
    )?;
    Ok(removed_root || removed_aliases > 0)
}

fn build_private_generation<C>(
    root: &Path,
    request: EmbeddingPublicationRequest<'_>,
    vector_limits: VectorStoreLimits,
    compatibility_id: EmbeddingCompatibilityId,
    generation_id: EmbeddingGenerationId,
    checkpoint: &mut C,
) -> Result<(tempfile::TempDir, EmbeddingGenerationManifest), SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    let private = tempfile::Builder::new()
        .prefix(BUILD_PREFIX)
        .tempdir_in(root)
        .map_err(|source| io("create private embedding generation", root, source))?;
    let rows = request
        .batch
        .rows()
        .iter()
        .map(|row| StoredVector {
            node_uuid: row.node_uuid,
            vector: row.vector.clone(),
            updated_at_micros: request.generated_at_micros,
        })
        .collect::<Vec<_>>();
    let vector_path = write_vector_snapshot(
        private.path(),
        &rows,
        request.batch.dimension(),
        vector_limits,
        &mut *checkpoint,
    )?;
    let publication_fingerprint =
        hash_file(&vector_path, vector_limits.parquet_bytes, &mut *checkpoint)?;
    let manifest = EmbeddingGenerationManifest::new(EmbeddingGenerationManifestInput {
        compatibility_id,
        source: request.source,
        content_digest: request.batch.content_digest(),
        vector_count: u64::try_from(rows.len()).map_err(|_| {
            SearchArtifactError::ResourceExhausted {
                resource: "embedding_rows",
                limit: vector_limits.stored_vectors as u64,
            }
        })?,
        dimension: request.descriptor.dimensions(),
        generated_at_micros: request.generated_at_micros,
        committed_at_micros: request.committed_at_micros,
        publication_fingerprint,
    })?;
    debug_assert_eq!(manifest.generation_id(), generation_id);
    checkpoint()?;
    write_synced_file(
        &private.path().join(MANIFEST_FILE),
        &manifest.to_canonical_json()?,
    )?;
    sync_tree(private.path())?;
    checkpoint()?;
    Ok((private, manifest))
}

/// Reopen and fully validate the active generation for one descriptor.
///
/// A missing space root or active pointer returns `Ok(None)`. Once an active
/// pointer exists, descriptor, pointer, manifest, and vector corruption fail
/// closed and are never represented as an absent generation.
///
/// # Errors
/// Returns structured incompatibility, corruption, resource, cancellation, or
/// filesystem errors.
pub fn current_embedding_generation<C>(
    project_dir: &Path,
    descriptor: &EmbeddingCompatibilityDescriptor,
    vector_limits: VectorStoreLimits,
    mut checkpoint: C,
) -> Result<Option<EmbeddingGenerationPublication>, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    checkpoint()?;
    let compatibility_id = descriptor.compatibility_id()?;
    let root = space_root(project_dir, compatibility_id);
    if !path_exists(&root)? {
        return Ok(None);
    }
    ensure_space_ancestors(project_dir, &root)?;
    let active_path = root.join(ACTIVE_FILE);
    if !path_exists(&active_path)? {
        return Ok(None);
    }
    let reopened_descriptor = read_descriptor(&root, descriptor, compatibility_id)?;
    let pointer = read_active_pointer(&active_path)?;
    if pointer.compatibility_id != compatibility_id {
        return Err(corrupt_primary(
            &active_path,
            "active pointer compatibility identity does not match its space path",
        ));
    }
    validate_generation(
        &root,
        &reopened_descriptor,
        compatibility_id,
        pointer.generation_id,
        vector_limits,
        &mut checkpoint,
    )
    .map(Some)
}

fn validate_generation<C>(
    root: &Path,
    descriptor: &EmbeddingCompatibilityDescriptor,
    compatibility_id: EmbeddingCompatibilityId,
    generation_id: EmbeddingGenerationId,
    vector_limits: VectorStoreLimits,
    checkpoint: &mut C,
) -> Result<EmbeddingGenerationPublication, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    checkpoint()?;
    let path = root.join(GENERATIONS_DIR).join(generation_id.to_hex());
    ensure_existing_directory(&path)?;
    validate_generation_layout(&path)?;
    let manifest_path = path.join(MANIFEST_FILE);
    let manifest_bytes = read_bounded_file(&manifest_path, MAX_MANIFEST_BYTES)?;
    let manifest = EmbeddingGenerationManifest::from_json(&manifest_path, &manifest_bytes)
        .map_err(|error| primary_from(&path, error))?;
    if manifest.compatibility_id() != compatibility_id {
        return Err(corrupt_primary(
            &manifest_path,
            "manifest compatibility identity does not match its space path",
        ));
    }
    if manifest.generation_id() != generation_id {
        return Err(corrupt_primary(
            &manifest_path,
            "manifest generation identity does not match its generation path",
        ));
    }
    if manifest.dimension() != descriptor.dimensions() {
        return Err(corrupt_primary(
            &manifest_path,
            "manifest dimension does not match compatibility descriptor",
        ));
    }
    let vector_path = path.join(VECTOR_DATA_FILE);
    let fingerprint = hash_file(&vector_path, vector_limits.parquet_bytes, checkpoint)
        .map_err(|error| primary_from(&path, error))?;
    if fingerprint != manifest.publication_fingerprint() {
        return Err(corrupt_primary(
            &vector_path,
            "vector file fingerprint does not match generation manifest",
        ));
    }
    let dimension = usize::try_from(manifest.dimension())
        .map_err(|_| corrupt_primary(&manifest_path, "manifest dimension cannot be represented"))?;
    let rows = read_vector_snapshot(&path, dimension, vector_limits, &mut *checkpoint)
        .map_err(|error| primary_from(&path, error))?;
    if u64::try_from(rows.len()).ok() != Some(manifest.vector_count()) {
        return Err(corrupt_primary(
            &vector_path,
            "vector row count does not match generation manifest",
        ));
    }
    if content_digest(&rows, checkpoint)? != manifest.content_digest() {
        return Err(corrupt_primary(
            &vector_path,
            "canonical UUID/vector content digest does not match generation manifest",
        ));
    }
    Ok(EmbeddingGenerationPublication {
        path,
        descriptor: descriptor.clone(),
        manifest,
    })
}

fn prepare_space_root(
    project_dir: &Path,
    compatibility_id: EmbeddingCompatibilityId,
) -> Result<PathBuf, SearchArtifactError> {
    let embeddings = project_dir.join("embeddings");
    ensure_owned_directory(&embeddings)?;
    let spaces = embeddings.join("spaces");
    ensure_owned_directory(&spaces)?;
    let root = spaces.join(compatibility_id.to_hex());
    ensure_owned_directory(&root)?;
    Ok(root)
}

fn space_root(project_dir: &Path, compatibility_id: EmbeddingCompatibilityId) -> PathBuf {
    project_dir
        .join("embeddings")
        .join("spaces")
        .join(compatibility_id.to_hex())
}

fn ensure_space_ancestors(project_dir: &Path, root: &Path) -> Result<(), SearchArtifactError> {
    ensure_existing_directory(&project_dir.join("embeddings"))?;
    ensure_existing_directory(&project_dir.join("embeddings").join("spaces"))?;
    ensure_existing_directory(root)
}

fn validate_generation_layout(path: &Path) -> Result<(), SearchArtifactError> {
    let mut names = Vec::new();
    for entry in std::fs::read_dir(path)
        .map_err(|source| io("scan immutable embedding generation", path, source))?
    {
        let entry = entry
            .map_err(|source| io("read immutable embedding generation entry", path, source))?;
        let file_type = entry.file_type().map_err(|source| {
            io(
                "inspect immutable embedding generation entry",
                &entry.path(),
                source,
            )
        })?;
        if file_type.is_symlink() || !file_type.is_file() {
            return Err(corrupt_primary(
                &entry.path(),
                "immutable embedding generation entries must be regular files",
            ));
        }
        let name = entry.file_name().into_string().map_err(|_| {
            corrupt_primary(
                &entry.path(),
                "immutable embedding generation file name must be UTF-8",
            )
        })?;
        names.push(name);
        if names.len() > 2 {
            return Err(corrupt_primary(
                path,
                "immutable embedding generation contains unexpected files",
            ));
        }
    }
    names.sort_unstable();
    if names != [MANIFEST_FILE, VECTOR_DATA_FILE] {
        return Err(corrupt_primary(
            path,
            "immutable embedding generation must contain manifest.json and vectors.parquet",
        ));
    }
    Ok(())
}

fn persist_or_verify_descriptor(
    root: &Path,
    descriptor: &EmbeddingCompatibilityDescriptor,
    compatibility_id: EmbeddingCompatibilityId,
) -> Result<(), SearchArtifactError> {
    let path = root.join(SPACE_FILE);
    if path_exists(&path)? {
        read_descriptor(root, descriptor, compatibility_id).map(|_| ())
    } else {
        let bytes = descriptor.to_canonical_json()?;
        persist_synced_file(&path, ".space.json.", &bytes)?;
        sync_directory(root)
    }
}

fn read_descriptor(
    root: &Path,
    requested: &EmbeddingCompatibilityDescriptor,
    compatibility_id: EmbeddingCompatibilityId,
) -> Result<EmbeddingCompatibilityDescriptor, SearchArtifactError> {
    let path = root.join(SPACE_FILE);
    let bytes = read_bounded_file(&path, MAX_DESCRIPTOR_BYTES)?;
    let descriptor = EmbeddingCompatibilityDescriptor::from_json(&path, &bytes)
        .map_err(|error| primary_from(root, error))?;
    let reopened_id = descriptor
        .compatibility_id()
        .map_err(|error| primary_from(root, error))?;
    if reopened_id != compatibility_id || &descriptor != requested {
        return Err(corrupt_primary(
            &path,
            "compatibility descriptor does not match requested identity",
        ));
    }
    Ok(descriptor)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawActivePointer {
    pointer_version: u32,
    compatibility_id: String,
    generation_id: String,
    checksum: String,
}

struct ActivePointer {
    compatibility_id: EmbeddingCompatibilityId,
    generation_id: EmbeddingGenerationId,
}

fn read_active_pointer(path: &Path) -> Result<ActivePointer, SearchArtifactError> {
    let bytes = read_bounded_file(path, MAX_ACTIVE_BYTES)?;
    let raw: RawActivePointer =
        serde_json::from_slice(&bytes).map_err(|error| corrupt_primary(path, error.to_string()))?;
    if raw.pointer_version != POINTER_VERSION {
        return Err(corrupt_primary(path, "unsupported active pointer version"));
    }
    let compatibility_id = EmbeddingCompatibilityId::from_hex(&raw.compatibility_id)
        .map_err(|error| corrupt_primary(path, error.to_string()))?;
    let generation_id = EmbeddingGenerationId::from_hex(&raw.generation_id)
        .map_err(|error| corrupt_primary(path, error.to_string()))?;
    let checksum = active_checksum(compatibility_id, generation_id);
    if raw.checksum != checksum {
        return Err(corrupt_primary(path, "active pointer checksum mismatch"));
    }
    let canonical = active_pointer_bytes(compatibility_id, generation_id)?;
    if canonical != bytes {
        return Err(corrupt_primary(
            path,
            "active pointer bytes are not exact canonical JSON",
        ));
    }
    Ok(ActivePointer {
        compatibility_id,
        generation_id,
    })
}

fn persist_active_pointer(
    root: &Path,
    compatibility_id: EmbeddingCompatibilityId,
    generation_id: EmbeddingGenerationId,
) -> Result<(), SearchArtifactError> {
    let bytes = active_pointer_bytes(compatibility_id, generation_id)?;
    persist_synced_file(&root.join(ACTIVE_FILE), ".active.json.", &bytes)
}

fn active_pointer_bytes(
    compatibility_id: EmbeddingCompatibilityId,
    generation_id: EmbeddingGenerationId,
) -> Result<Vec<u8>, SearchArtifactError> {
    serde_json::to_vec(&serde_json::json!({
        "checksum": active_checksum(compatibility_id, generation_id),
        "compatibility_id": compatibility_id.to_hex(),
        "generation_id": generation_id.to_hex(),
        "pointer_version": POINTER_VERSION,
    }))
    .map_err(|error| SearchArtifactError::Build(error.to_string()))
}

fn active_checksum(
    compatibility_id: EmbeddingCompatibilityId,
    generation_id: EmbeddingGenerationId,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge.embedding.active.v1\0");
    hasher.update(compatibility_id.to_hex().as_bytes());
    hasher.update(generation_id.to_hex().as_bytes());
    format!("{:x}", hasher.finalize())
}

fn content_digest<C>(
    rows: &[StoredVector],
    checkpoint: &mut C,
) -> Result<EmbeddingContentDigest, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    let mut hasher = Sha256::new();
    for row in rows {
        checkpoint()?;
        hasher.update(row.node_uuid);
        for value in &row.vector {
            hasher.update(value.to_le_bytes());
        }
    }
    EmbeddingContentDigest::from_hex(&format!("{:x}", hasher.finalize()))
}

fn hash_file<C>(
    path: &Path,
    max_bytes: u64,
    checkpoint: &mut C,
) -> Result<EmbeddingPublicationFingerprint, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    ensure_regular_file(path)?;
    let metadata =
        std::fs::metadata(path).map_err(|source| io("inspect embedding file", path, source))?;
    if metadata.len() > max_bytes {
        return Err(SearchArtifactError::ResourceExhausted {
            resource: "vector_parquet_bytes",
            limit: max_bytes,
        });
    }
    let mut file = File::open(path).map_err(|source| io("open embedding file", path, source))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; HASH_BUFFER_BYTES];
    loop {
        checkpoint()?;
        let read = file
            .read(&mut buffer)
            .map_err(|source| io("read embedding file", path, source))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    EmbeddingPublicationFingerprint::from_hex(&format!("{:x}", hasher.finalize()))
}

struct EmbeddingWriterLock {
    file: File,
}

impl EmbeddingWriterLock {
    fn acquire<C>(
        project_dir: &Path,
        compatibility_id: EmbeddingCompatibilityId,
        limits: SearchCoordinationLimits,
        checkpoint: &mut C,
    ) -> Result<Self, SearchArtifactError>
    where
        C: FnMut() -> Result<(), SearchArtifactError>,
    {
        let embeddings = project_dir.join("embeddings");
        ensure_owned_directory(&embeddings)?;
        let path = embeddings.join(format!(
            "{EMBEDDING_WRITER_LOCK_PREFIX}{}.lock",
            compatibility_id.to_hex()
        ));
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

pub(crate) fn deletion_marker(
    project_dir: &Path,
    compatibility_id: EmbeddingCompatibilityId,
) -> PathBuf {
    project_dir.join("embeddings").join(format!(
        "{EMBEDDING_DELETION_PREFIX}{}",
        compatibility_id.to_hex()
    ))
}

impl Drop for EmbeddingWriterLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

fn persist_synced_file(path: &Path, prefix: &str, bytes: &[u8]) -> Result<(), SearchArtifactError> {
    let parent = path
        .parent()
        .ok_or_else(|| SearchArtifactError::Build("publication file has no parent".to_owned()))?;
    let mut temp = tempfile::Builder::new()
        .prefix(prefix)
        .suffix(".tmp")
        .tempfile_in(parent)
        .map_err(|source| io("create embedding metadata temp", path, source))?;
    temp.write_all(bytes)
        .map_err(|source| io("write embedding metadata temp", path, source))?;
    temp.as_file()
        .sync_all()
        .map_err(|source| io("sync embedding metadata temp", path, source))?;
    temp.persist(path)
        .map_err(|error| io("publish embedding metadata", path, error.error))?;
    sync_directory(parent)
}

fn write_synced_file(path: &Path, bytes: &[u8]) -> Result<(), SearchArtifactError> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|source| io("create embedding generation file", path, source))?;
    file.write_all(bytes)
        .map_err(|source| io("write embedding generation file", path, source))?;
    file.sync_all()
        .map_err(|source| io("sync embedding generation file", path, source))
}

fn read_bounded_file(path: &Path, max_bytes: u64) -> Result<Vec<u8>, SearchArtifactError> {
    ensure_regular_file(path)?;
    let metadata =
        std::fs::metadata(path).map_err(|source| io("inspect embedding metadata", path, source))?;
    if metadata.len() > max_bytes {
        return Err(SearchArtifactError::ResourceExhausted {
            resource: "embedding_metadata_bytes",
            limit: max_bytes,
        });
    }
    std::fs::read(path).map_err(|source| io("read embedding metadata", path, source))
}

fn sync_tree(root: &Path) -> Result<(), SearchArtifactError> {
    let mut directories = vec![root.to_path_buf()];
    let mut files = Vec::new();
    let mut cursor = 0;
    while cursor < directories.len() {
        let directory = directories[cursor].clone();
        cursor += 1;
        for entry in std::fs::read_dir(&directory)
            .map_err(|source| io("scan embedding generation", &directory, source))?
        {
            let entry = entry
                .map_err(|source| io("read embedding generation entry", &directory, source))?;
            let file_type = entry.file_type().map_err(|source| {
                io("inspect embedding generation entry", &entry.path(), source)
            })?;
            if file_type.is_symlink() {
                return Err(corrupt_primary(
                    &entry.path(),
                    "embedding generation must not contain symlinks",
                ));
            }
            if file_type.is_dir() {
                directories.push(entry.path());
            } else if file_type.is_file() {
                files.push(entry.path());
            } else {
                return Err(corrupt_primary(
                    &entry.path(),
                    "embedding generation contains unsupported file type",
                ));
            }
        }
    }
    files.sort_unstable();
    for file in files {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file)
            .and_then(|file| file.sync_all())
            .map_err(|source| io("sync embedding generation file", &file, source))?;
    }
    directories.sort_unstable_by_key(|path| std::cmp::Reverse(path.components().count()));
    for directory in directories {
        sync_directory(&directory)?;
    }
    Ok(())
}

fn ensure_owned_directory(path: &Path) -> Result<(), SearchArtifactError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => Err(
            corrupt_primary(path, "embedding path must be a real directory"),
        ),
        Ok(_) => Ok(()),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => std::fs::create_dir(path)
            .map_err(|source| io("create embedding directory", path, source)),
        Err(source) => Err(io("inspect embedding directory", path, source)),
    }
}

fn ensure_existing_directory(path: &Path) -> Result<(), SearchArtifactError> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|source| io("inspect embedding directory", path, source))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(corrupt_primary(
            path,
            "embedding path must be a real directory",
        ));
    }
    Ok(())
}

fn ensure_regular_file(path: &Path) -> Result<(), SearchArtifactError> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|source| io("inspect embedding file", path, source))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(corrupt_primary(
            path,
            "embedding path must be a regular file",
        ));
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

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<(), SearchArtifactError> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|source| io("sync embedding directory", path, source))
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<(), SearchArtifactError> {
    Ok(())
}

fn primary_from(path: &Path, error: SearchArtifactError) -> SearchArtifactError {
    match error {
        error @ (SearchArtifactError::Cancelled
        | SearchArtifactError::ResourceExhausted { .. }
        | SearchArtifactError::Lock { .. }
        | SearchArtifactError::Io { .. }
        | SearchArtifactError::CorruptPrimaryVectors { .. }) => error,
        error => corrupt_primary(path, error.to_string()),
    }
}

fn invalid(field: &'static str, reason: impl Into<String>) -> SearchArtifactError {
    SearchArtifactError::InvalidSelector {
        field,
        reason: reason.into(),
    }
}

fn corrupt_primary(path: &Path, reason: impl Into<String>) -> SearchArtifactError {
    SearchArtifactError::CorruptPrimaryVectors {
        path: path.to_path_buf(),
        reason: reason.into(),
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
    use std::collections::{BTreeMap, BTreeSet};
    use std::time::Duration;

    use serde_json::json;

    use super::*;
    use crate::{
        EmbeddingBatchRow, EmbeddingCompatibilityInput, EmbeddingDistance, EmbeddingNormalization,
        EmbeddingProducerIdentity, EmbeddingValueType, validate_embedding_batch,
    };

    const NODE: [u8; 16] = [7; 16];

    fn descriptor(model: &str, dimension: u32) -> EmbeddingCompatibilityDescriptor {
        EmbeddingCompatibilityDescriptor::new(EmbeddingCompatibilityInput {
            producer: EmbeddingProducerIdentity::Local {
                implementation: "test-adapter".to_owned(),
                model: model.to_owned(),
                revision: "r1".to_owned(),
                contract_version: "v1".to_owned(),
            },
            dimensions: dimension,
            value_type: EmbeddingValueType::Float32,
            normalization: EmbeddingNormalization::None,
            distance: EmbeddingDistance::Cosine,
            tokenizer: None,
            chunking: None,
            hyperparameters: BTreeMap::new(),
            input_recipe: BTreeMap::from([("property".to_owned(), json!("body"))]),
            source_projection_recipe: BTreeMap::from([("label".to_owned(), json!("Document"))]),
        })
        .unwrap()
    }

    fn source(generation: u64) -> EmbeddingSourceState {
        EmbeddingSourceState::new(generation, [generation as u8; 32], [9; 32], 1)
    }

    fn batch(values: &[f32]) -> ValidatedEmbeddingBatch {
        validate_embedding_batch(
            vec![EmbeddingBatchRow {
                node_uuid: NODE,
                vector: values.to_vec(),
            }],
            &BTreeSet::from([NODE]),
            values.len(),
            EmbeddingNormalization::None,
            VectorStoreLimits::default(),
            || Ok(()),
        )
        .unwrap()
    }

    fn request<'a>(
        descriptor: &'a EmbeddingCompatibilityDescriptor,
        source: EmbeddingSourceState,
        batch: &'a ValidatedEmbeddingBatch,
        committed_at_micros: i64,
    ) -> EmbeddingPublicationRequest<'a> {
        EmbeddingPublicationRequest {
            descriptor,
            source,
            batch,
            generated_at_micros: 10,
            committed_at_micros,
        }
    }

    fn publish(
        dir: &Path,
        descriptor: &EmbeddingCompatibilityDescriptor,
        source: EmbeddingSourceState,
        batch: &ValidatedEmbeddingBatch,
        committed_at_micros: i64,
    ) -> EmbeddingPublicationOutcome {
        publish_embedding_generation(
            dir,
            request(descriptor, source, batch, committed_at_micros),
            VectorStoreLimits::default(),
            SearchCoordinationLimits::default(),
            || Ok(()),
        )
        .unwrap()
    }

    #[test]
    fn complete_publication_reopens_and_identical_content_is_reused() {
        let dir = tempfile::tempdir().unwrap();
        let descriptor = descriptor("model-a", 2);
        let batch = batch(&[1.0, 2.0]);
        let first = publish(dir.path(), &descriptor, source(1), &batch, 20);
        assert!(matches!(first, EmbeddingPublicationOutcome::Published(_)));
        let reopened = current_embedding_generation(
            dir.path(),
            &descriptor,
            VectorStoreLimits::default(),
            || Ok(()),
        )
        .unwrap()
        .unwrap();
        assert_eq!(reopened, first.publication().clone());

        let second = publish(dir.path(), &descriptor, source(1), &batch, 99);
        assert!(matches!(second, EmbeddingPublicationOutcome::Reused(_)));
        assert_eq!(second.publication(), first.publication());
        assert_eq!(
            std::fs::read_dir(first.publication().path.parent().unwrap())
                .unwrap()
                .count(),
            1
        );
    }

    #[test]
    fn failed_replacement_never_changes_the_active_generation() {
        let dir = tempfile::tempdir().unwrap();
        let descriptor = descriptor("model-a", 2);
        let first_batch = batch(&[1.0, 2.0]);
        let first = publish(dir.path(), &descriptor, source(1), &first_batch, 20);
        let second_batch = batch(&[3.0, 4.0]);
        let generations = first.publication().path.parent().unwrap().to_path_buf();
        let error = publish_embedding_generation(
            dir.path(),
            request(&descriptor, source(2), &second_batch, 30),
            VectorStoreLimits::default(),
            SearchCoordinationLimits::default(),
            || {
                if std::fs::read_dir(&generations)
                    .map(|entries| entries.count() > 1)
                    .unwrap_or(false)
                {
                    Err(SearchArtifactError::Cancelled)
                } else {
                    Ok(())
                }
            },
        )
        .unwrap_err();
        assert!(matches!(error, SearchArtifactError::Cancelled));
        let active = current_embedding_generation(
            dir.path(),
            &descriptor,
            VectorStoreLimits::default(),
            || Ok(()),
        )
        .unwrap()
        .unwrap();
        assert_eq!(active, first.publication().clone());
    }

    #[test]
    fn compatibility_lineages_are_independent_for_the_same_uuid() {
        let dir = tempfile::tempdir().unwrap();
        let left_descriptor = descriptor("model-a", 2);
        let right_descriptor = descriptor("model-b", 3);
        let left_batch = batch(&[1.0, 2.0]);
        let right_batch = batch(&[1.0, 2.0, 3.0]);
        let left = publish(dir.path(), &left_descriptor, source(1), &left_batch, 20);
        let right = publish(dir.path(), &right_descriptor, source(1), &right_batch, 20);
        assert_ne!(left.publication().path, right.publication().path);
        assert!(left.publication().path.exists());
        assert!(right.publication().path.exists());
    }

    #[test]
    fn pointer_descriptor_and_vector_corruption_fail_closed() {
        let cases = ["pointer", "descriptor", "vector"];
        for case in cases {
            let dir = tempfile::tempdir().unwrap();
            let descriptor = descriptor("model-a", 2);
            let batch = batch(&[1.0, 2.0]);
            let published = publish(dir.path(), &descriptor, source(1), &batch, 20);
            let compatibility = descriptor.compatibility_id().unwrap().to_hex();
            let root = dir.path().join("embeddings/spaces").join(compatibility);
            let path = match case {
                "pointer" => root.join(ACTIVE_FILE),
                "descriptor" => root.join(SPACE_FILE),
                "vector" => published.publication().path.join(VECTOR_DATA_FILE),
                _ => unreachable!(),
            };
            std::fs::write(path, b"corrupt").unwrap();
            assert!(matches!(
                current_embedding_generation(
                    dir.path(),
                    &descriptor,
                    VectorStoreLimits::default(),
                    || Ok(()),
                ),
                Err(SearchArtifactError::CorruptPrimaryVectors { .. })
            ));
        }
    }

    #[test]
    fn canonical_manifest_identity_and_dimension_mismatches_fail_closed() {
        let descriptor = descriptor("model-a", 2);
        let compatibility = descriptor.compatibility_id().unwrap();
        let vectors = batch(&[1.0, 2.0]);
        for case in ["compatibility", "generation", "dimension"] {
            let dir = tempfile::tempdir().unwrap();
            let published = publish(dir.path(), &descriptor, source(1), &vectors, 20);
            let original = &published.publication().manifest;
            let manifest = EmbeddingGenerationManifest::new(EmbeddingGenerationManifestInput {
                compatibility_id: if case == "compatibility" {
                    EmbeddingCompatibilityId::from_hex(&"11".repeat(32)).unwrap()
                } else {
                    compatibility
                },
                source: if case == "generation" {
                    source(2)
                } else {
                    source(1)
                },
                content_digest: vectors.content_digest(),
                vector_count: 1,
                dimension: if case == "dimension" { 3 } else { 2 },
                generated_at_micros: 10,
                committed_at_micros: 20,
                publication_fingerprint: original.publication_fingerprint(),
            })
            .unwrap();
            std::fs::write(
                published.publication().path.join(MANIFEST_FILE),
                manifest.to_canonical_json().unwrap(),
            )
            .unwrap();
            let error = current_embedding_generation(
                dir.path(),
                &descriptor,
                VectorStoreLimits::default(),
                || Ok(()),
            )
            .unwrap_err();
            assert!(matches!(
                error,
                SearchArtifactError::CorruptPrimaryVectors { .. }
            ));
        }
    }

    #[test]
    fn canonical_manifest_rejects_vector_row_count_and_content_drift() {
        let descriptor = descriptor("model-a", 2);
        let compatibility = descriptor.compatibility_id().unwrap();
        let original_vectors = batch(&[1.0, 2.0]);
        for case in ["row-count", "content"] {
            let dir = tempfile::tempdir().unwrap();
            let published = publish(dir.path(), &descriptor, source(1), &original_vectors, 20);
            let generation = &published.publication().path;
            let replacement = batch(&[3.0, 4.0]);
            let replacement_rows = replacement
                .rows()
                .iter()
                .map(|row| StoredVector {
                    node_uuid: row.node_uuid,
                    vector: row.vector.clone(),
                    updated_at_micros: 10,
                })
                .collect::<Vec<_>>();
            let rows: &[StoredVector] = if case == "row-count" {
                &[]
            } else {
                &replacement_rows
            };
            let vector_path =
                write_vector_snapshot(generation, rows, 2, VectorStoreLimits::default(), || Ok(()))
                    .unwrap();
            let fingerprint = hash_file(
                &vector_path,
                VectorStoreLimits::default().parquet_bytes,
                &mut || Ok(()),
            )
            .unwrap();
            let manifest = EmbeddingGenerationManifest::new(EmbeddingGenerationManifestInput {
                compatibility_id: compatibility,
                source: source(1),
                content_digest: original_vectors.content_digest(),
                vector_count: 1,
                dimension: 2,
                generated_at_micros: 10,
                committed_at_micros: 20,
                publication_fingerprint: fingerprint,
            })
            .unwrap();
            std::fs::write(
                generation.join(MANIFEST_FILE),
                manifest.to_canonical_json().unwrap(),
            )
            .unwrap();
            assert!(matches!(
                current_embedding_generation(
                    dir.path(),
                    &descriptor,
                    VectorStoreLimits::default(),
                    || Ok(()),
                ),
                Err(SearchArtifactError::CorruptPrimaryVectors { .. })
            ));
        }
    }

    #[test]
    fn persisted_descriptor_must_match_the_exact_requested_contract() {
        let dir = tempfile::tempdir().unwrap();
        let expected_descriptor = descriptor("model-a", 2);
        let vectors = batch(&[1.0, 2.0]);
        publish(dir.path(), &expected_descriptor, source(1), &vectors, 20);
        let compatibility = expected_descriptor.compatibility_id().unwrap();
        let root = space_root(dir.path(), compatibility);
        let other = descriptor("model-b", 2);
        assert!(matches!(
            read_descriptor(&root, &other, compatibility),
            Err(SearchArtifactError::CorruptPrimaryVectors { .. })
        ));
        assert!(root.join(SPACE_FILE).is_file());
    }

    #[test]
    fn private_and_traversal_like_trees_are_never_visible() {
        let dir = tempfile::tempdir().unwrap();
        let descriptor = descriptor("model-a", 2);
        let compatibility = descriptor.compatibility_id().unwrap().to_hex();
        let root = dir.path().join("embeddings/spaces").join(compatibility);
        std::fs::create_dir_all(root.join(".build-crashed")).unwrap();
        assert!(
            current_embedding_generation(
                dir.path(),
                &descriptor,
                VectorStoreLimits::default(),
                || Ok(()),
            )
            .unwrap()
            .is_none()
        );
        let batch = batch(&[1.0, 2.0]);
        publish(dir.path(), &descriptor, source(1), &batch, 20);
        std::fs::write(
            root.join(ACTIVE_FILE),
            br#"{"pointer_version":1,"compatibility_id":"..","generation_id":"..","checksum":"no"}"#,
        )
        .unwrap();
        assert!(matches!(
            current_embedding_generation(
                dir.path(),
                &descriptor,
                VectorStoreLimits::default(),
                || Ok(()),
            ),
            Err(SearchArtifactError::CorruptPrimaryVectors { .. })
        ));
    }

    #[test]
    fn unexpected_generation_entries_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let descriptor = descriptor("model-a", 2);
        let batch = batch(&[1.0, 2.0]);
        let published = publish(dir.path(), &descriptor, source(1), &batch, 20);
        std::fs::write(published.publication().path.join("unexpected"), b"data").unwrap();
        assert!(matches!(
            current_embedding_generation(
                dir.path(),
                &descriptor,
                VectorStoreLimits::default(),
                || Ok(()),
            ),
            Err(SearchArtifactError::CorruptPrimaryVectors { .. })
        ));
    }

    #[test]
    fn active_pointer_malformed_state_matrix_fails_closed_without_repointing() {
        let descriptor = descriptor("model-a", 2);
        let compatibility = descriptor.compatibility_id().unwrap();
        let vectors = batch(&[1.0, 2.0]);
        let cases = [
            br#"{"pointer_version":2,"compatibility_id":"bad","generation_id":"bad","checksum":"bad"}"#.as_slice(),
            br#"{"pointer_version":1,"compatibility_id":"bad","generation_id":"bad","checksum":"bad"}"#,
            br#"{"pointer_version":1,"compatibility_id":"0000000000000000000000000000000000000000000000000000000000000000","generation_id":"bad","checksum":"bad"}"#,
            br#"{"pointer_version":1,"compatibility_id":"0000000000000000000000000000000000000000000000000000000000000000","generation_id":"0000000000000000000000000000000000000000000000000000000000000000","checksum":"bad"}"#,
        ];
        for bytes in cases {
            let dir = tempfile::tempdir().unwrap();
            let published = publish(dir.path(), &descriptor, source(1), &vectors, 20);
            let active = space_root(dir.path(), compatibility).join(ACTIVE_FILE);
            let generation = published.publication().path.clone();
            std::fs::write(&active, bytes).unwrap();
            let before = std::fs::read(&active).unwrap();
            assert!(matches!(
                current_embedding_generation(
                    dir.path(),
                    &descriptor,
                    VectorStoreLimits::default(),
                    || Ok(()),
                ),
                Err(SearchArtifactError::CorruptPrimaryVectors { .. })
            ));
            assert_eq!(std::fs::read(&active).unwrap(), before);
            assert!(generation.is_dir());
        }
    }

    #[test]
    fn canonical_active_pointer_identity_and_encoding_mismatches_fail_closed() {
        let descriptor = descriptor("model-a", 2);
        let compatibility = descriptor.compatibility_id().unwrap();
        let vectors = batch(&[1.0, 2.0]);

        let dir = tempfile::tempdir().unwrap();
        let published = publish(dir.path(), &descriptor, source(1), &vectors, 20);
        let active = space_root(dir.path(), compatibility).join(ACTIVE_FILE);
        let other = EmbeddingCompatibilityId::from_hex(&"11".repeat(32)).unwrap();
        std::fs::write(
            &active,
            active_pointer_bytes(other, published.publication().manifest.generation_id()).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            current_embedding_generation(
                dir.path(),
                &descriptor,
                VectorStoreLimits::default(),
                || Ok(()),
            ),
            Err(SearchArtifactError::CorruptPrimaryVectors { .. })
        ));

        let dir = tempfile::tempdir().unwrap();
        publish(dir.path(), &descriptor, source(1), &vectors, 20);
        let active = space_root(dir.path(), compatibility).join(ACTIVE_FILE);
        let mut noncanonical = vec![b' '];
        noncanonical.extend_from_slice(&std::fs::read(&active).unwrap());
        std::fs::write(&active, noncanonical).unwrap();
        assert!(matches!(
            current_embedding_generation(
                dir.path(),
                &descriptor,
                VectorStoreLimits::default(),
                || Ok(()),
            ),
            Err(SearchArtifactError::CorruptPrimaryVectors { .. })
        ));
    }

    #[test]
    fn generation_layout_and_metadata_limits_fail_closed_without_mutation() {
        let descriptor = descriptor("model-a", 2);
        let vectors = batch(&[1.0, 2.0]);
        for case in [
            "missing-manifest",
            "manifest-dimension",
            "directory-entry",
            "oversized-active",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let published = publish(dir.path(), &descriptor, source(1), &vectors, 20);
            let compatibility = descriptor.compatibility_id().unwrap();
            let root = space_root(dir.path(), compatibility);
            match case {
                "missing-manifest" => {
                    std::fs::remove_file(published.publication().path.join(MANIFEST_FILE)).unwrap();
                }
                "manifest-dimension" => {
                    let path = published.publication().path.join(MANIFEST_FILE);
                    let mut manifest: serde_json::Value =
                        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
                    manifest["dimension"] = serde_json::json!(3);
                    std::fs::write(path, serde_json::to_vec(&manifest).unwrap()).unwrap();
                }
                "directory-entry" => {
                    std::fs::create_dir(published.publication().path.join("nested")).unwrap();
                }
                "oversized-active" => {
                    std::fs::write(
                        root.join(ACTIVE_FILE),
                        vec![b'x'; MAX_ACTIVE_BYTES as usize + 1],
                    )
                    .unwrap();
                }
                _ => unreachable!(),
            }
            let before = std::fs::read_dir(&root).unwrap().count();
            let error = current_embedding_generation(
                dir.path(),
                &descriptor,
                VectorStoreLimits::default(),
                || Ok(()),
            )
            .unwrap_err();
            assert!(matches!(
                error,
                SearchArtifactError::CorruptPrimaryVectors { .. }
                    | SearchArtifactError::ResourceExhausted { .. }
            ));
            assert_eq!(std::fs::read_dir(&root).unwrap().count(), before);
        }
    }

    #[test]
    fn publication_rejects_dimension_cancellation_and_deletion_marker_without_state() {
        let dir = tempfile::tempdir().unwrap();
        let descriptor = descriptor("model-a", 2);
        let wrong = batch(&[1.0, 2.0, 3.0]);
        let error = publish_embedding_generation(
            dir.path(),
            request(&descriptor, source(1), &wrong, 20),
            VectorStoreLimits::default(),
            SearchCoordinationLimits::default(),
            || Ok(()),
        )
        .unwrap_err();
        assert!(matches!(error, SearchArtifactError::InvalidSelector { .. }));
        assert!(!dir.path().join("embeddings").exists());

        let vectors = batch(&[1.0, 2.0]);
        let error = publish_embedding_generation(
            dir.path(),
            request(&descriptor, source(1), &vectors, 20),
            VectorStoreLimits::default(),
            SearchCoordinationLimits::default(),
            || Err(SearchArtifactError::Cancelled),
        )
        .unwrap_err();
        assert!(matches!(error, SearchArtifactError::Cancelled));

        let compatibility = descriptor.compatibility_id().unwrap();
        std::fs::create_dir_all(dir.path().join("embeddings")).unwrap();
        std::fs::write(deletion_marker(dir.path(), compatibility), b"deleting").unwrap();
        let error = publish_embedding_generation(
            dir.path(),
            request(&descriptor, source(1), &vectors, 20),
            VectorStoreLimits::default(),
            SearchCoordinationLimits::default(),
            || Ok(()),
        )
        .unwrap_err();
        assert!(matches!(error, SearchArtifactError::InvalidSelector { .. }));
        assert!(
            current_embedding_generation(
                dir.path(),
                &descriptor,
                VectorStoreLimits::default(),
                || Ok(()),
            )
            .unwrap()
            .is_none()
        );
    }

    #[test]
    fn deletion_removes_generation_and_alias_and_is_idempotent_across_reopen() {
        let empty = tempfile::tempdir().unwrap();
        let absent = EmbeddingCompatibilityId::from_hex(&"11".repeat(32)).unwrap();
        assert!(
            !delete_embedding_space_lineage(
                empty.path(),
                absent,
                EmbeddingSpaceCatalogLimits::default(),
                SearchCoordinationLimits::default(),
                || Ok(()),
            )
            .unwrap()
        );
        assert!(!empty.path().join("embeddings").exists());

        let project = tempfile::tempdir().unwrap();
        let descriptor = descriptor("delete-me", 2);
        let vectors = batch(&[1.0, 2.0]);
        let published = publish(project.path(), &descriptor, source(1), &vectors, 20);
        let compatibility = descriptor.compatibility_id().unwrap();
        crate::bind_existing_embedding_space_catalog_entry(
            project.path(),
            "semantic",
            compatibility,
            false,
            EmbeddingSpaceCatalogLimits::default(),
            || Ok(()),
        )
        .unwrap();
        assert!(
            delete_embedding_space_lineage(
                project.path(),
                compatibility,
                EmbeddingSpaceCatalogLimits::default(),
                SearchCoordinationLimits::default(),
                || Ok(()),
            )
            .unwrap()
        );
        assert!(!published.publication().path.exists());
        assert!(!deletion_marker(project.path(), compatibility).exists());
        assert!(
            crate::read_embedding_space_catalog(
                project.path(),
                EmbeddingSpaceCatalogLimits::default(),
                || Ok(()),
            )
            .unwrap()
            .is_empty()
        );
        assert!(
            current_embedding_generation(
                project.path(),
                &descriptor,
                VectorStoreLimits::default(),
                || Ok(()),
            )
            .unwrap()
            .is_none()
        );
        assert!(
            !delete_embedding_space_lineage(
                project.path(),
                compatibility,
                EmbeddingSpaceCatalogLimits::default(),
                SearchCoordinationLimits::default(),
                || Ok(()),
            )
            .unwrap()
        );
    }

    #[test]
    fn interrupted_deletion_retains_marker_and_retry_completes_without_resurrection() {
        let project = tempfile::tempdir().unwrap();
        let descriptor = descriptor("interrupted-delete", 2);
        let vectors = batch(&[1.0, 2.0]);
        let published = publish(project.path(), &descriptor, source(1), &vectors, 20);
        let compatibility = descriptor.compatibility_id().unwrap();
        let marker = deletion_marker(project.path(), compatibility);
        let root = space_root(project.path(), compatibility);
        let error = delete_embedding_space_lineage(
            project.path(),
            compatibility,
            EmbeddingSpaceCatalogLimits::default(),
            SearchCoordinationLimits::default(),
            || {
                if marker.exists() && root.exists() {
                    Err(SearchArtifactError::Cancelled)
                } else {
                    Ok(())
                }
            },
        )
        .unwrap_err();
        assert!(matches!(error, SearchArtifactError::Cancelled));
        assert!(marker.is_file());
        assert!(published.publication().path.is_dir());

        assert!(
            delete_embedding_space_lineage(
                project.path(),
                compatibility,
                EmbeddingSpaceCatalogLimits::default(),
                SearchCoordinationLimits::default(),
                || Ok(()),
            )
            .unwrap()
        );
        assert!(!marker.exists());
        assert!(!root.exists());
    }

    #[test]
    fn publication_filesystem_and_error_boundaries_are_fail_closed() {
        let project = tempfile::tempdir().unwrap();
        let compatibility = EmbeddingCompatibilityId::from_hex(&"22".repeat(32)).unwrap();
        let first = EmbeddingWriterLock::acquire(
            project.path(),
            compatibility,
            SearchCoordinationLimits::default(),
            &mut || Ok(()),
        )
        .unwrap();
        let limits = SearchCoordinationLimits {
            lock_timeout: Duration::ZERO,
            lock_poll_interval: Duration::ZERO,
            ..SearchCoordinationLimits::default()
        };
        assert!(matches!(
            EmbeddingWriterLock::acquire(project.path(), compatibility, limits, &mut || Ok(())),
            Err(SearchArtifactError::Lock { .. })
        ));
        drop(first);

        let tree = project.path().join("tree");
        std::fs::create_dir(&tree).unwrap();
        std::fs::create_dir(tree.join("nested")).unwrap();
        std::fs::write(tree.join("nested/data"), b"payload").unwrap();
        sync_tree(&tree).unwrap();
        assert!(ensure_owned_directory(&tree).is_ok());
        assert!(ensure_existing_directory(&tree).is_ok());
        assert!(ensure_regular_file(&tree.join("nested/data")).is_ok());
        assert!(path_exists(&tree.join("nested/data")).unwrap());
        assert!(!path_exists(&tree.join("absent")).unwrap());

        assert!(matches!(
            read_bounded_file(&tree.join("nested/data"), 1),
            Err(SearchArtifactError::ResourceExhausted { .. })
        ));
        assert!(matches!(
            hash_file(&tree.join("nested/data"), 1, &mut || Ok(())),
            Err(SearchArtifactError::ResourceExhausted { .. })
        ));
        assert!(matches!(
            hash_file(&tree.join("nested/data"), 100, &mut || Err(
                SearchArtifactError::Cancelled
            )),
            Err(SearchArtifactError::Cancelled)
        ));
        assert!(matches!(
            ensure_existing_directory(&tree.join("nested/data")),
            Err(SearchArtifactError::CorruptPrimaryVectors { .. })
        ));
        assert!(matches!(
            ensure_regular_file(&tree),
            Err(SearchArtifactError::CorruptPrimaryVectors { .. })
        ));

        let mapped = primary_from(
            &tree,
            SearchArtifactError::InvalidSelector {
                field: "test",
                reason: "invalid".into(),
            },
        );
        assert!(matches!(
            mapped,
            SearchArtifactError::CorruptPrimaryVectors { .. }
        ));
        for error in [
            SearchArtifactError::Cancelled,
            SearchArtifactError::ResourceExhausted {
                resource: "test",
                limit: 1,
            },
        ] {
            assert!(matches!(
                primary_from(&tree, error),
                SearchArtifactError::Cancelled | SearchArtifactError::ResourceExhausted { .. }
            ));
        }
        assert!(matches!(
            io("test operation", &tree, std::io::Error::other("failure")),
            SearchArtifactError::Io { .. }
        ));
    }

    #[cfg(unix)]
    #[test]
    fn publication_tree_rejects_symlinks() {
        use std::os::unix::fs::symlink;

        let project = tempfile::tempdir().unwrap();
        let tree = project.path().join("tree");
        std::fs::create_dir(&tree).unwrap();
        symlink(project.path(), tree.join("link")).unwrap();
        assert!(matches!(
            sync_tree(&tree),
            Err(SearchArtifactError::CorruptPrimaryVectors { .. })
        ));
        assert!(matches!(
            ensure_owned_directory(&tree.join("link")),
            Err(SearchArtifactError::CorruptPrimaryVectors { .. })
        ));

        std::fs::remove_file(tree.join("link")).unwrap();
        std::fs::write(tree.join(MANIFEST_FILE), b"manifest").unwrap();
        std::fs::write(tree.join(VECTOR_DATA_FILE), b"vectors").unwrap();
        symlink(project.path(), tree.join("link")).unwrap();
        assert!(matches!(
            validate_generation_layout(&tree),
            Err(SearchArtifactError::CorruptPrimaryVectors { .. })
        ));
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_embedding_ancestor_fails_closed() {
        use std::os::unix::fs::symlink;

        let project = tempfile::tempdir().unwrap();
        let external = tempfile::tempdir().unwrap();
        symlink(external.path(), project.path().join("embeddings")).unwrap();
        let descriptor = descriptor("model-a", 2);
        let compatibility = descriptor.compatibility_id().unwrap().to_hex();
        std::fs::create_dir_all(external.path().join("spaces").join(compatibility)).unwrap();
        assert!(matches!(
            current_embedding_generation(
                project.path(),
                &descriptor,
                VectorStoreLimits::default(),
                || Ok(()),
            ),
            Err(SearchArtifactError::CorruptPrimaryVectors { .. })
        ));
    }

    #[test]
    fn publication_dimension_mismatch_fails_before_creating_storage() {
        let project = tempfile::tempdir().unwrap();
        let descriptor = descriptor("model-a", 3);
        let vectors = batch(&[1.0, 2.0]);

        let error = publish_embedding_generation(
            project.path(),
            request(&descriptor, source(1), &vectors, 20),
            VectorStoreLimits::default(),
            SearchCoordinationLimits::default(),
            || Ok(()),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            SearchArtifactError::InvalidSelector {
                field: "embedding batch",
                ..
            }
        ));
        assert!(!project.path().join("embeddings").exists());
    }

    #[test]
    fn public_reopen_distinguishes_absent_space_and_absent_active_pointer() {
        let dir = tempfile::tempdir().unwrap();
        let descriptor = descriptor("absent", 2);
        assert!(
            current_embedding_generation(
                dir.path(),
                &descriptor,
                VectorStoreLimits::default(),
                || Ok(())
            )
            .unwrap()
            .is_none()
        );

        let compatibility_id = descriptor.compatibility_id().unwrap();
        let root = space_root(dir.path(), compatibility_id);
        std::fs::create_dir_all(&root).unwrap();
        assert!(
            current_embedding_generation(
                dir.path(),
                &descriptor,
                VectorStoreLimits::default(),
                || Ok(())
            )
            .unwrap()
            .is_none()
        );
    }

    #[test]
    fn wave10_generation_layout_rejects_nonfiles_and_excess_inventory() {
        for kind in ["directory", "excess"] {
            let root = tempfile::tempdir().unwrap();
            std::fs::write(root.path().join(MANIFEST_FILE), b"manifest").unwrap();
            std::fs::write(root.path().join(VECTOR_DATA_FILE), b"vectors").unwrap();
            if kind == "directory" {
                std::fs::create_dir(root.path().join("unexpected")).unwrap();
            } else {
                std::fs::write(root.path().join("unexpected"), b"caller").unwrap();
            }
            assert!(matches!(
                validate_generation_layout(root.path()),
                Err(SearchArtifactError::CorruptPrimaryVectors { .. })
            ));
        }
    }

    #[cfg(unix)]
    #[test]
    fn wave10_generation_sync_rejects_special_files() {
        use std::os::unix::net::UnixListener;

        let root = tempfile::Builder::new()
            .prefix("gf")
            .tempdir_in("/tmp")
            .unwrap();
        let _socket = UnixListener::bind(root.path().join("socket")).unwrap();
        assert!(matches!(
            sync_tree(root.path()),
            Err(SearchArtifactError::CorruptPrimaryVectors { .. })
        ));
    }

    #[test]
    fn wave13_embedding_path_helpers_fail_closed_on_regular_file_ancestors() {
        let root = tempfile::tempdir().unwrap();
        let ancestor = root.path().join("regular-file");
        std::fs::write(&ancestor, b"caller data").unwrap();
        let child = ancestor.join("child");

        assert!(matches!(
            ensure_owned_directory(&child),
            Err(SearchArtifactError::Io { .. })
        ));
        assert!(matches!(
            path_exists(&child),
            Err(SearchArtifactError::Io { .. })
        ));
        assert!(matches!(
            ensure_existing_directory(&child),
            Err(SearchArtifactError::Io { .. })
        ));
        assert!(matches!(
            ensure_regular_file(&child),
            Err(SearchArtifactError::Io { .. })
        ));
    }
}
