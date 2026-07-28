//! Durable per-lineage mutation evidence for embedding freshness.

use std::collections::BTreeSet;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Instant;

use serde::Deserialize;

use crate::{
    EmbeddingCompatibilityId, EmbeddingGenerationId, EmbeddingGenerationManifest,
    EmbeddingMutationObservation, EmbeddingSourceFingerprint, EmbeddingSourceState,
    SearchArtifactError, SearchCoordinationLimits,
};

/// Mutation journal schema implemented by this release.
pub const EMBEDDING_MUTATION_JOURNAL_VERSION: u32 = 1;
const JOURNAL_FILE: &str = "mutations.json";
const MAX_DEFAULT_BYTES: usize = 64 * 1024 * 1024;

/// Bounds for one durable lineage mutation journal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EmbeddingMutationJournalLimits {
    /// Maximum distinct changed UUIDs retained since publication.
    pub changed_uuids: usize,
    /// Maximum canonical journal bytes.
    pub metadata_bytes: usize,
}

impl Default for EmbeddingMutationJournalLimits {
    fn default() -> Self {
        Self {
            changed_uuids: 1_000_000,
            metadata_bytes: MAX_DEFAULT_BYTES,
        }
    }
}

/// One relevant committed graph mutation supplied by the write-path hook.
#[derive(Clone, Copy, Debug)]
pub struct EmbeddingMutationBatch<'a> {
    /// Durable source state immediately after this committed batch.
    pub current_source: EmbeddingSourceState,
    /// Raw stable UUIDs changed in dependency scope by this batch.
    pub changed_uuids: &'a [[u8; 16]],
    /// Whether this batch changed topology for a structural space.
    pub structural_mutation: bool,
    /// Whether the hook proved the complete affected scope.
    pub scope_proven: bool,
}

/// Exact durable mutation evidence since one active generation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmbeddingMutationJournal {
    compatibility_id: EmbeddingCompatibilityId,
    generation_id: EmbeddingGenerationId,
    recorded_source: EmbeddingSourceFingerprint,
    current_source: EmbeddingSourceState,
    changed_uuids: BTreeSet<[u8; 16]>,
    relevant_committed_batches: u64,
    structural_mutation: bool,
    scope_proven: bool,
}

impl EmbeddingMutationJournal {
    /// Compatibility lineage bound to this evidence.
    #[must_use]
    pub const fn compatibility_id(&self) -> EmbeddingCompatibilityId {
        self.compatibility_id
    }

    /// Active complete generation bound to this evidence.
    #[must_use]
    pub const fn generation_id(&self) -> EmbeddingGenerationId {
        self.generation_id
    }

    /// Sorted distinct changed UUIDs retained since reset.
    #[must_use]
    pub fn changed_uuids(&self) -> &BTreeSet<[u8; 16]> {
        &self.changed_uuids
    }

    /// Reconstruct the exact pure freshness-policy observation.
    ///
    /// # Errors
    /// Returns a validation error only if durable fields are internally inconsistent.
    pub fn observation(&self) -> Result<EmbeddingMutationObservation, SearchArtifactError> {
        let changed_distinct_uuids = u64::try_from(self.changed_uuids.len())
            .map_err(|_| exhausted("embedding_mutation_changed_uuids", self.changed_uuids.len()))?;
        EmbeddingMutationObservation::new(
            self.current_source,
            changed_distinct_uuids,
            self.relevant_committed_batches,
            self.structural_mutation,
            self.scope_proven,
        )
    }

    fn reset(manifest: &EmbeddingGenerationManifest) -> Self {
        Self {
            compatibility_id: manifest.compatibility_id(),
            generation_id: manifest.generation_id(),
            recorded_source: manifest.source().fingerprint(),
            current_source: manifest.source(),
            changed_uuids: BTreeSet::new(),
            relevant_committed_batches: 0,
            structural_mutation: false,
            scope_proven: true,
        }
    }

    fn to_json(
        &self,
        limits: EmbeddingMutationJournalLimits,
    ) -> Result<Vec<u8>, SearchArtifactError> {
        let changed = self.changed_uuids.iter().map(encode).collect::<Vec<_>>();
        let source = self.current_source;
        let bytes = serde_json::to_vec(&serde_json::json!({
            "changed_uuids": changed,
            "compatibility_id": self.compatibility_id.to_hex(),
            "current_dependency_input_digest": encode(&source.dependency_input_digest()),
            "current_eligible_uuid_count": source.eligible_uuid_count(),
            "current_graph_generation": source.graph_generation(),
            "current_label_membership_digest": encode(&source.label_membership_digest()),
            "current_source_fingerprint": source.fingerprint().to_hex(),
            "generation_id": self.generation_id.to_hex(),
            "journal_version": EMBEDDING_MUTATION_JOURNAL_VERSION,
            "recorded_source_fingerprint": self.recorded_source.to_hex(),
            "relevant_committed_batches": self.relevant_committed_batches,
            "scope_proven": self.scope_proven,
            "structural_mutation": self.structural_mutation,
        }))
        .map_err(|error| SearchArtifactError::Build(error.to_string()))?;
        if bytes.len() > limits.metadata_bytes {
            return Err(exhausted(
                "embedding_mutation_journal_bytes",
                limits.metadata_bytes,
            ));
        }
        Ok(bytes)
    }
}

/// Atomically reset durable evidence to one verified active generation.
pub fn reset_embedding_mutation_journal<C>(
    project_dir: &Path,
    manifest: &EmbeddingGenerationManifest,
    limits: EmbeddingMutationJournalLimits,
    coordination: SearchCoordinationLimits,
    mut checkpoint: C,
) -> Result<EmbeddingMutationJournal, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    checkpoint()?;
    let root = checked_space_root(project_dir, manifest.compatibility_id())?;
    let _lock = WriterLock::acquire(&root, coordination, &mut checkpoint)?;
    let path = root.join(JOURNAL_FILE);
    ensure_optional_regular_file(&path)?;
    let journal = EmbeddingMutationJournal::reset(manifest);
    let bytes = journal.to_json(limits)?;
    checkpoint()?;
    persist(&path, &bytes)?;
    Ok(journal)
}

/// Merge one relevant committed batch without losing the prior durable journal.
pub fn merge_embedding_mutation_batch<C>(
    project_dir: &Path,
    manifest: &EmbeddingGenerationManifest,
    batch: EmbeddingMutationBatch<'_>,
    limits: EmbeddingMutationJournalLimits,
    coordination: SearchCoordinationLimits,
    mut checkpoint: C,
) -> Result<EmbeddingMutationJournal, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    checkpoint()?;
    let root = checked_space_root(project_dir, manifest.compatibility_id())?;
    let _lock = WriterLock::acquire(&root, coordination, &mut checkpoint)?;
    let path = root.join(JOURNAL_FILE);
    let mut journal = read_linked(&path, manifest, limits)?
        .ok_or_else(|| SearchArtifactError::Missing { path: path.clone() })?;
    if batch.current_source.graph_generation() <= journal.current_source.graph_generation() {
        return Err(invalid(
            "embedding mutation batch",
            "current graph generation must increase",
        ));
    }
    for uuid in batch.changed_uuids {
        checkpoint()?;
        journal.changed_uuids.insert(*uuid);
        if journal.changed_uuids.len() > limits.changed_uuids {
            return Err(exhausted(
                "embedding_mutation_changed_uuids",
                limits.changed_uuids,
            ));
        }
    }
    journal.relevant_committed_batches = journal
        .relevant_committed_batches
        .checked_add(1)
        .ok_or_else(|| exhausted("embedding_mutation_batches", usize::MAX))?;
    journal.current_source = batch.current_source;
    journal.structural_mutation |= batch.structural_mutation;
    journal.scope_proven &= batch.scope_proven;
    let bytes = journal.to_json(limits)?;
    checkpoint()?;
    persist(&path, &bytes)?;
    Ok(journal)
}

/// Reopen exact durable evidence linked to the verified active generation.
pub fn read_embedding_mutation_journal(
    project_dir: &Path,
    manifest: &EmbeddingGenerationManifest,
    limits: EmbeddingMutationJournalLimits,
) -> Result<Option<EmbeddingMutationJournal>, SearchArtifactError> {
    let path = checked_space_root(project_dir, manifest.compatibility_id())?.join(JOURNAL_FILE);
    read_linked(&path, manifest, limits)
}

fn read_linked(
    path: &Path,
    manifest: &EmbeddingGenerationManifest,
    limits: EmbeddingMutationJournalLimits,
) -> Result<Option<EmbeddingMutationJournal>, SearchArtifactError> {
    let Some(bytes) = bounded_read(path, limits.metadata_bytes)? else {
        return Ok(None);
    };
    let raw: RawJournal =
        serde_json::from_slice(&bytes).map_err(|error| corrupt(path, error.to_string()))?;
    if raw.journal_version != u64::from(EMBEDDING_MUTATION_JOURNAL_VERSION) {
        return Err(SearchArtifactError::IncompatibleManifest {
            path: path.to_path_buf(),
            found: raw.journal_version,
            supported: EMBEDDING_MUTATION_JOURNAL_VERSION,
        });
    }
    let compatibility_id = EmbeddingCompatibilityId::from_hex(&raw.compatibility_id)
        .map_err(|error| corrupt(path, error.to_string()))?;
    let generation_id = EmbeddingGenerationId::from_hex(&raw.generation_id)
        .map_err(|error| corrupt(path, error.to_string()))?;
    let recorded_source = EmbeddingSourceFingerprint::from_hex(&raw.recorded_source_fingerprint)
        .map_err(|error| corrupt(path, error.to_string()))?;
    if compatibility_id != manifest.compatibility_id()
        || generation_id != manifest.generation_id()
        || recorded_source != manifest.source().fingerprint()
    {
        return Err(corrupt(
            path,
            "journal linkage does not match active generation",
        ));
    }
    let current_source = EmbeddingSourceState::new(
        raw.current_graph_generation,
        decode32(path, &raw.current_label_membership_digest)?,
        decode32(path, &raw.current_dependency_input_digest)?,
        raw.current_eligible_uuid_count,
    );
    if current_source.fingerprint().to_hex() != raw.current_source_fingerprint {
        return Err(corrupt(path, "current source fingerprint mismatch"));
    }
    if current_source.graph_generation() < manifest.source().graph_generation() {
        return Err(corrupt(
            path,
            "current source predates the recorded generation source",
        ));
    }
    let mut changed_uuids = BTreeSet::new();
    for value in raw.changed_uuids {
        if !changed_uuids.insert(decode16(path, &value)?) {
            return Err(corrupt(path, "duplicate changed UUID"));
        }
    }
    if changed_uuids.len() > limits.changed_uuids {
        return Err(exhausted(
            "embedding_mutation_changed_uuids",
            limits.changed_uuids,
        ));
    }
    let journal = EmbeddingMutationJournal {
        compatibility_id,
        generation_id,
        recorded_source,
        current_source,
        changed_uuids,
        relevant_committed_batches: raw.relevant_committed_batches,
        structural_mutation: raw.structural_mutation,
        scope_proven: raw.scope_proven,
    };
    journal
        .observation()
        .map_err(|error| corrupt(path, error.to_string()))?;
    if journal.to_json(limits)? != bytes {
        return Err(corrupt(path, "journal bytes are not exact canonical JSON"));
    }
    Ok(Some(journal))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawJournal {
    journal_version: u64,
    compatibility_id: String,
    generation_id: String,
    recorded_source_fingerprint: String,
    current_source_fingerprint: String,
    current_graph_generation: u64,
    current_label_membership_digest: String,
    current_dependency_input_digest: String,
    current_eligible_uuid_count: u64,
    changed_uuids: Vec<String>,
    relevant_committed_batches: u64,
    structural_mutation: bool,
    scope_proven: bool,
}

fn checked_space_root(
    project: &Path,
    id: EmbeddingCompatibilityId,
) -> Result<PathBuf, SearchArtifactError> {
    let embeddings = project.join("embeddings");
    ensure_real_directory(&embeddings)?;
    let spaces = embeddings.join("spaces");
    ensure_real_directory(&spaces)?;
    let root = spaces.join(id.to_hex());
    ensure_real_directory(&root)?;
    Ok(root)
}

fn ensure_real_directory(path: &Path) -> Result<(), SearchArtifactError> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|source| io("inspect embedding directory", path, source))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(corrupt(path, "embedding path must be a real directory"));
    }
    Ok(())
}

fn ensure_optional_regular_file(path: &Path) -> Result<(), SearchArtifactError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            Err(corrupt(path, "journal path must be a regular file"))
        }
        Ok(_) => Ok(()),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(io("inspect mutation journal", path, source)),
    }
}

fn bounded_read(path: &Path, max: usize) -> Result<Option<Vec<u8>>, SearchArtifactError> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(io("inspect mutation journal", path, source)),
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(corrupt(path, "journal must be a regular file"));
    }
    if metadata.len() > usize_limit(max) {
        return Err(exhausted("embedding_mutation_journal_bytes", max));
    }
    std::fs::read(path)
        .map(Some)
        .map_err(|source| io("read mutation journal", path, source))
}

fn persist(path: &Path, bytes: &[u8]) -> Result<(), SearchArtifactError> {
    ensure_optional_regular_file(path)?;
    let parent = path
        .parent()
        .ok_or_else(|| SearchArtifactError::Build("journal has no parent".to_owned()))?;
    let mut temp = tempfile::Builder::new()
        .prefix(".mutations.json.")
        .suffix(".tmp")
        .tempfile_in(parent)
        .map_err(|source| io("create mutation journal temp", path, source))?;
    temp.write_all(bytes)
        .map_err(|source| io("write mutation journal", path, source))?;
    temp.as_file()
        .sync_all()
        .map_err(|source| io("sync mutation journal", path, source))?;
    temp.persist(path)
        .map_err(|error| io("publish mutation journal", path, error.error))?;
    sync_dir(parent)
}

struct WriterLock {
    file: File,
}

impl WriterLock {
    fn acquire<C>(
        root: &Path,
        limits: SearchCoordinationLimits,
        checkpoint: &mut C,
    ) -> Result<Self, SearchArtifactError>
    where
        C: FnMut() -> Result<(), SearchArtifactError>,
    {
        let path = root.join(".writer.lock");
        ensure_optional_regular_file(&path)?;
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
        let start = Instant::now();
        loop {
            match file.try_lock() {
                Ok(()) => return Ok(Self { file }),
                Err(std::fs::TryLockError::WouldBlock) => {
                    checkpoint()?;
                    if start.elapsed() >= limits.lock_timeout {
                        return Err(SearchArtifactError::Lock {
                            path,
                            reason: "timed out waiting for embedding writer".to_owned(),
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

impl Drop for WriterLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

fn encode<const N: usize>(value: &[u8; N]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(N * 2);
    for byte in value {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn decode16(path: &Path, value: &str) -> Result<[u8; 16], SearchArtifactError> {
    decode(path, value)
}

fn decode32(path: &Path, value: &str) -> Result<[u8; 32], SearchArtifactError> {
    decode(path, value)
}

fn decode<const N: usize>(path: &Path, value: &str) -> Result<[u8; N], SearchArtifactError> {
    if value.len() != N * 2
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(corrupt(path, "invalid lowercase hexadecimal field"));
    }
    let mut output = [0; N];
    for (index, byte) in output.iter_mut().enumerate() {
        let offset = index * 2;
        *byte = u8::from_str_radix(&value[offset..offset + 2], 16)
            .map_err(|error| corrupt(path, error.to_string()))?;
    }
    Ok(output)
}

#[cfg(unix)]
fn sync_dir(path: &Path) -> Result<(), SearchArtifactError> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|source| io("sync mutation journal directory", path, source))
}

#[cfg(not(unix))]
fn sync_dir(_: &Path) -> Result<(), SearchArtifactError> {
    Ok(())
}

fn usize_limit(limit: usize) -> u64 {
    u64::try_from(limit).unwrap_or(u64::MAX)
}

fn exhausted(resource: &'static str, limit: usize) -> SearchArtifactError {
    SearchArtifactError::ResourceExhausted {
        resource,
        limit: usize_limit(limit),
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

fn io(operation: &'static str, path: &Path, source: std::io::Error) -> SearchArtifactError {
    SearchArtifactError::Io {
        operation,
        path: path.to_path_buf(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;
    use crate::{
        EmbeddingContentDigest, EmbeddingGenerationManifestInput, EmbeddingPublicationFingerprint,
    };

    const UUID_A: [u8; 16] = [1; 16];
    const UUID_B: [u8; 16] = [2; 16];
    const UUID_C: [u8; 16] = [3; 16];

    fn compatibility(marker: u8) -> EmbeddingCompatibilityId {
        EmbeddingCompatibilityId::from_hex(&format!("{marker:02x}").repeat(32)).unwrap()
    }

    fn source(generation: u64, marker: u8) -> EmbeddingSourceState {
        EmbeddingSourceState::new(generation, [marker; 32], [marker + 1; 32], 3)
    }

    fn manifest(marker: u8) -> EmbeddingGenerationManifest {
        EmbeddingGenerationManifest::new(EmbeddingGenerationManifestInput {
            compatibility_id: compatibility(marker),
            source: source(10, marker),
            content_digest: EmbeddingContentDigest::digest(&[marker]),
            vector_count: 3,
            dimension: 2,
            generated_at_micros: 20,
            committed_at_micros: 21,
            publication_fingerprint: EmbeddingPublicationFingerprint::digest(&[marker]),
        })
        .unwrap()
    }

    fn create_space(project: &Path, manifest: &EmbeddingGenerationManifest) -> PathBuf {
        let root = project
            .join("embeddings")
            .join("spaces")
            .join(manifest.compatibility_id().to_hex());
        std::fs::create_dir_all(&root).unwrap();
        root
    }

    fn reset(project: &Path, manifest: &EmbeddingGenerationManifest) -> EmbeddingMutationJournal {
        reset_embedding_mutation_journal(
            project,
            manifest,
            EmbeddingMutationJournalLimits::default(),
            SearchCoordinationLimits::default(),
            || Ok(()),
        )
        .unwrap()
    }

    fn merge(
        project: &Path,
        manifest: &EmbeddingGenerationManifest,
        batch: EmbeddingMutationBatch<'_>,
    ) -> EmbeddingMutationJournal {
        merge_embedding_mutation_batch(
            project,
            manifest,
            batch,
            EmbeddingMutationJournalLimits::default(),
            SearchCoordinationLimits::default(),
            || Ok(()),
        )
        .unwrap()
    }

    #[test]
    fn reset_merge_and_reopen_reconstruct_exact_observation() {
        let project = tempfile::tempdir().unwrap();
        let manifest = manifest(1);
        let root = create_space(project.path(), &manifest);

        let initial = reset(project.path(), &manifest);
        assert_eq!(initial.current_source, manifest.source());
        assert!(initial.changed_uuids().is_empty());

        merge(
            project.path(),
            &manifest,
            EmbeddingMutationBatch {
                current_source: source(11, 2),
                changed_uuids: &[UUID_B, UUID_A, UUID_A],
                structural_mutation: false,
                scope_proven: true,
            },
        );
        let merged = merge(
            project.path(),
            &manifest,
            EmbeddingMutationBatch {
                current_source: source(12, 3),
                changed_uuids: &[UUID_C, UUID_A],
                structural_mutation: true,
                scope_proven: false,
            },
        );
        let reopened = read_embedding_mutation_journal(
            project.path(),
            &manifest,
            EmbeddingMutationJournalLimits::default(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(reopened, merged);
        assert_eq!(
            reopened.changed_uuids().iter().copied().collect::<Vec<_>>(),
            [UUID_A, UUID_B, UUID_C]
        );
        let observation = reopened.observation().unwrap();
        assert_eq!(observation.current_source(), source(12, 3));
        assert_eq!(observation.changed_distinct_uuids(), 3);
        assert_eq!(observation.relevant_committed_batches(), 2);
        assert!(observation.structural_mutation());
        assert!(!observation.scope_proven());

        let raw: Value =
            serde_json::from_slice(&std::fs::read(root.join(JOURNAL_FILE)).unwrap()).unwrap();
        assert_eq!(
            raw["changed_uuids"],
            json!([encode(&UUID_A), encode(&UUID_B), encode(&UUID_C)])
        );
        assert_eq!(reset(project.path(), &manifest), initial);
    }

    #[test]
    fn missing_journal_is_distinct_and_lineages_are_byte_isolated() {
        let project = tempfile::tempdir().unwrap();
        let left = manifest(4);
        let right = manifest(5);
        let left_root = create_space(project.path(), &left);
        let right_root = create_space(project.path(), &right);
        assert!(
            read_embedding_mutation_journal(
                project.path(),
                &left,
                EmbeddingMutationJournalLimits::default(),
            )
            .unwrap()
            .is_none()
        );

        reset(project.path(), &left);
        reset(project.path(), &right);
        let right_before = std::fs::read(right_root.join(JOURNAL_FILE)).unwrap();
        merge(
            project.path(),
            &left,
            EmbeddingMutationBatch {
                current_source: source(11, 6),
                changed_uuids: &[UUID_A],
                structural_mutation: false,
                scope_proven: true,
            },
        );
        assert_ne!(
            std::fs::read(left_root.join(JOURNAL_FILE)).unwrap(),
            right_before
        );
        assert_eq!(
            std::fs::read(right_root.join(JOURNAL_FILE)).unwrap(),
            right_before
        );
    }

    #[test]
    fn cancellation_limits_monotonicity_and_overflow_preserve_prior_bytes() {
        let project = tempfile::tempdir().unwrap();
        let manifest = manifest(7);
        let root = create_space(project.path(), &manifest);
        reset(project.path(), &manifest);
        let path = root.join(JOURNAL_FILE);

        let before = std::fs::read(&path).unwrap();
        let mut checkpoints = 0;
        let cancelled = merge_embedding_mutation_batch(
            project.path(),
            &manifest,
            EmbeddingMutationBatch {
                current_source: source(11, 8),
                changed_uuids: &[UUID_A, UUID_B],
                structural_mutation: false,
                scope_proven: true,
            },
            EmbeddingMutationJournalLimits::default(),
            SearchCoordinationLimits::default(),
            || {
                checkpoints += 1;
                if checkpoints == 3 {
                    Err(SearchArtifactError::Cancelled)
                } else {
                    Ok(())
                }
            },
        )
        .unwrap_err();
        assert!(matches!(cancelled, SearchArtifactError::Cancelled));
        assert_eq!(std::fs::read(&path).unwrap(), before);

        let limited = merge_embedding_mutation_batch(
            project.path(),
            &manifest,
            EmbeddingMutationBatch {
                current_source: source(11, 8),
                changed_uuids: &[UUID_A],
                structural_mutation: false,
                scope_proven: true,
            },
            EmbeddingMutationJournalLimits {
                changed_uuids: 0,
                metadata_bytes: MAX_DEFAULT_BYTES,
            },
            SearchCoordinationLimits::default(),
            || Ok(()),
        )
        .unwrap_err();
        assert!(matches!(
            limited,
            SearchArtifactError::ResourceExhausted { .. }
        ));
        assert_eq!(std::fs::read(&path).unwrap(), before);

        let non_monotonic = merge_embedding_mutation_batch(
            project.path(),
            &manifest,
            EmbeddingMutationBatch {
                current_source: source(10, 8),
                changed_uuids: &[],
                structural_mutation: false,
                scope_proven: true,
            },
            EmbeddingMutationJournalLimits::default(),
            SearchCoordinationLimits::default(),
            || Ok(()),
        )
        .unwrap_err();
        assert!(matches!(
            non_monotonic,
            SearchArtifactError::InvalidSelector { .. }
        ));
        assert_eq!(std::fs::read(&path).unwrap(), before);

        let mut raw: Value = serde_json::from_slice(&before).unwrap();
        raw["relevant_committed_batches"] = json!(u64::MAX);
        let maximum = serde_json::to_vec(&raw).unwrap();
        std::fs::write(&path, &maximum).unwrap();
        let overflow = merge_embedding_mutation_batch(
            project.path(),
            &manifest,
            EmbeddingMutationBatch {
                current_source: source(11, 8),
                changed_uuids: &[],
                structural_mutation: false,
                scope_proven: true,
            },
            EmbeddingMutationJournalLimits::default(),
            SearchCoordinationLimits::default(),
            || Ok(()),
        )
        .unwrap_err();
        assert!(matches!(
            overflow,
            SearchArtifactError::ResourceExhausted { .. }
        ));
        assert_eq!(std::fs::read(&path).unwrap(), maximum);
    }

    #[test]
    fn reset_cancellation_and_metadata_limit_preserve_prior_bytes() {
        let project = tempfile::tempdir().unwrap();
        let manifest = manifest(8);
        let root = create_space(project.path(), &manifest);
        reset(project.path(), &manifest);
        let path = root.join(JOURNAL_FILE);
        let before = std::fs::read(&path).unwrap();

        let mut checkpoints = 0;
        let cancelled = reset_embedding_mutation_journal(
            project.path(),
            &manifest,
            EmbeddingMutationJournalLimits::default(),
            SearchCoordinationLimits::default(),
            || {
                checkpoints += 1;
                if checkpoints == 2 {
                    Err(SearchArtifactError::Cancelled)
                } else {
                    Ok(())
                }
            },
        )
        .unwrap_err();
        assert!(matches!(cancelled, SearchArtifactError::Cancelled));
        assert_eq!(std::fs::read(&path).unwrap(), before);

        let limited = reset_embedding_mutation_journal(
            project.path(),
            &manifest,
            EmbeddingMutationJournalLimits {
                changed_uuids: 1,
                metadata_bytes: 1,
            },
            SearchCoordinationLimits::default(),
            || Ok(()),
        )
        .unwrap_err();
        assert!(matches!(
            limited,
            SearchArtifactError::ResourceExhausted { .. }
        ));
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }

    #[test]
    fn corruption_incompatible_versions_and_linkage_mismatch_fail_closed() {
        let cases = ["duplicate", "unknown", "noncanonical", "version"];
        for case in cases {
            let project = tempfile::tempdir().unwrap();
            let manifest = manifest(9);
            let root = create_space(project.path(), &manifest);
            reset(project.path(), &manifest);
            let path = root.join(JOURNAL_FILE);
            let canonical = std::fs::read(&path).unwrap();
            let replacement = match case {
                "duplicate" => {
                    let mut bytes = br#"{"journal_version":1,"#.to_vec();
                    bytes.extend_from_slice(&canonical[1..]);
                    bytes
                }
                "unknown" => {
                    let mut raw: Value = serde_json::from_slice(&canonical).unwrap();
                    raw["unexpected"] = json!(true);
                    serde_json::to_vec(&raw).unwrap()
                }
                "noncanonical" => {
                    let mut bytes = canonical.clone();
                    bytes.push(b'\n');
                    bytes
                }
                "version" => {
                    let mut raw: Value = serde_json::from_slice(&canonical).unwrap();
                    raw["journal_version"] = json!(99);
                    serde_json::to_vec(&raw).unwrap()
                }
                _ => unreachable!(),
            };
            std::fs::write(&path, &replacement).unwrap();
            let error = read_embedding_mutation_journal(
                project.path(),
                &manifest,
                EmbeddingMutationJournalLimits::default(),
            )
            .unwrap_err();
            if case == "version" {
                assert!(matches!(
                    error,
                    SearchArtifactError::IncompatibleManifest { found: 99, .. }
                ));
            } else {
                assert!(matches!(error, SearchArtifactError::CorruptManifest { .. }));
            }
            assert_eq!(std::fs::read(&path).unwrap(), replacement);
        }

        let project = tempfile::tempdir().unwrap();
        let active = manifest(10);
        let other = manifest(11);
        let root = create_space(project.path(), &active);
        reset(project.path(), &active);
        let path = root.join(JOURNAL_FILE);
        let before = std::fs::read(&path).unwrap();
        let mut raw: Value = serde_json::from_slice(&before).unwrap();
        raw["generation_id"] = json!(other.generation_id().to_hex());
        let mismatch = serde_json::to_vec(&raw).unwrap();
        std::fs::write(&path, &mismatch).unwrap();
        assert!(matches!(
            read_embedding_mutation_journal(
                project.path(),
                &active,
                EmbeddingMutationJournalLimits::default(),
            ),
            Err(SearchArtifactError::CorruptManifest { .. })
        ));
        assert_eq!(std::fs::read(&path).unwrap(), mismatch);
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_ancestors_journal_and_lock_fail_closed() {
        use std::os::unix::fs::symlink;

        let external = tempfile::tempdir().unwrap();
        let project = tempfile::tempdir().unwrap();
        let manifest = manifest(12);
        symlink(external.path(), project.path().join("embeddings")).unwrap();
        assert!(matches!(
            reset_embedding_mutation_journal(
                project.path(),
                &manifest,
                EmbeddingMutationJournalLimits::default(),
                SearchCoordinationLimits::default(),
                || Ok(()),
            ),
            Err(SearchArtifactError::CorruptManifest { .. })
        ));

        let project = tempfile::tempdir().unwrap();
        let root = create_space(project.path(), &manifest);
        let external_file = external.path().join("external");
        std::fs::write(&external_file, b"unchanged").unwrap();
        symlink(&external_file, root.join(JOURNAL_FILE)).unwrap();
        assert!(matches!(
            reset_embedding_mutation_journal(
                project.path(),
                &manifest,
                EmbeddingMutationJournalLimits::default(),
                SearchCoordinationLimits::default(),
                || Ok(()),
            ),
            Err(SearchArtifactError::CorruptManifest { .. })
        ));
        assert_eq!(std::fs::read(&external_file).unwrap(), b"unchanged");

        std::fs::remove_file(root.join(JOURNAL_FILE)).unwrap();
        std::fs::remove_file(root.join(".writer.lock")).unwrap();
        symlink(&external_file, root.join(".writer.lock")).unwrap();
        assert!(matches!(
            reset_embedding_mutation_journal(
                project.path(),
                &manifest,
                EmbeddingMutationJournalLimits::default(),
                SearchCoordinationLimits::default(),
                || Ok(()),
            ),
            Err(SearchArtifactError::CorruptManifest { .. })
        ));
        assert_eq!(std::fs::read(&external_file).unwrap(), b"unchanged");
    }
}
