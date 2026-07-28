//! Durable source-state and complete-generation metadata for embeddings.

use std::fmt;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{
    EmbeddingCompatibilityId, EmbeddingContentDigest, EmbeddingGenerationId,
    EmbeddingSourceFingerprint, SearchArtifactError,
};

/// Generation manifest schema implemented by this release.
pub const EMBEDDING_GENERATION_MANIFEST_VERSION: u32 = 1;
/// Maximum accepted generation manifest bytes.
pub const MAX_EMBEDDING_GENERATION_MANIFEST_BYTES: usize = 64 * 1024;
const SOURCE_FINGERPRINT_DOMAIN: &[u8] = b"graphforge_embedding_source_v1";

/// SHA-256 fingerprint of the exact published vector file bytes.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EmbeddingPublicationFingerprint([u8; 32]);

impl EmbeddingPublicationFingerprint {
    /// Digest exact publication-file bytes.
    #[must_use]
    pub fn digest(bytes: &[u8]) -> Self {
        Self(Sha256::digest(bytes).into())
    }

    /// Parse exactly 64 lowercase hexadecimal digits.
    ///
    /// # Errors
    /// Rejects uppercase, non-hexadecimal, and wrong-width input.
    pub fn from_hex(value: &str) -> Result<Self, SearchArtifactError> {
        parse_digest("publication_fingerprint", value).map(Self)
    }

    /// Lowercase hexadecimal representation.
    #[must_use]
    pub fn to_hex(self) -> String {
        encode_digest(self.0)
    }
}

impl fmt::Debug for EmbeddingPublicationFingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("EmbeddingPublicationFingerprint")
            .field(&self.to_hex())
            .finish()
    }
}

impl fmt::Display for EmbeddingPublicationFingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_hex())
    }
}

/// Exact committed graph inputs used to build one complete generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EmbeddingSourceState {
    graph_generation: u64,
    label_membership_digest: [u8; 32],
    dependency_input_digest: [u8; 32],
    eligible_uuid_count: u64,
    fingerprint: EmbeddingSourceFingerprint,
}

impl EmbeddingSourceState {
    /// Freeze source components and derive their versioned SHA-256 identity.
    #[must_use]
    pub fn new(
        graph_generation: u64,
        label_membership_digest: [u8; 32],
        dependency_input_digest: [u8; 32],
        eligible_uuid_count: u64,
    ) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(SOURCE_FINGERPRINT_DOMAIN);
        hasher.update(graph_generation.to_le_bytes());
        hasher.update(label_membership_digest);
        hasher.update(dependency_input_digest);
        hasher.update(eligible_uuid_count.to_le_bytes());
        let fingerprint = EmbeddingSourceFingerprint::from_hex(&format!("{:x}", hasher.finalize()))
            .expect("SHA-256 output is always a valid lowercase source fingerprint");
        Self {
            graph_generation,
            label_membership_digest,
            dependency_input_digest,
            eligible_uuid_count,
            fingerprint,
        }
    }

    /// Committed graph mutation generation.
    #[must_use]
    pub const fn graph_generation(self) -> u64 {
        self.graph_generation
    }

    /// Digest of canonical selected-label UUID membership.
    #[must_use]
    pub const fn label_membership_digest(self) -> [u8; 32] {
        self.label_membership_digest
    }

    /// Digest of canonical topology/property dependencies.
    #[must_use]
    pub const fn dependency_input_digest(self) -> [u8; 32] {
        self.dependency_input_digest
    }

    /// Complete eligible UUID count.
    #[must_use]
    pub const fn eligible_uuid_count(self) -> u64 {
        self.eligible_uuid_count
    }

    /// Versioned source fingerprint used in generation identity.
    #[must_use]
    pub const fn fingerprint(self) -> EmbeddingSourceFingerprint {
        self.fingerprint
    }
}

/// Validated input for one completed generation manifest.
#[derive(Clone, Copy, Debug)]
pub struct EmbeddingGenerationManifestInput {
    /// Validated space compatibility identity.
    pub compatibility_id: EmbeddingCompatibilityId,
    /// Exact committed source state.
    pub source: EmbeddingSourceState,
    /// Canonical UUID/vector content digest.
    pub content_digest: EmbeddingContentDigest,
    /// Rows in the complete generation.
    pub vector_count: u64,
    /// Fixed Float32 vector width.
    pub dimension: u32,
    /// Producer completion time in UTC microseconds since Unix epoch.
    pub generated_at_micros: i64,
    /// Durable publication time in UTC microseconds since Unix epoch.
    pub committed_at_micros: i64,
    /// Digest of exact persisted vector-file bytes.
    pub publication_fingerprint: EmbeddingPublicationFingerprint,
}

/// Durable metadata for one validated, complete embedding generation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmbeddingGenerationManifest {
    compatibility_id: EmbeddingCompatibilityId,
    generation_id: EmbeddingGenerationId,
    source: EmbeddingSourceState,
    content_digest: EmbeddingContentDigest,
    vector_count: u64,
    dimension: u32,
    generated_at_micros: i64,
    committed_at_micros: i64,
    publication_fingerprint: EmbeddingPublicationFingerprint,
}

impl EmbeddingGenerationManifest {
    /// Validate a complete generation and derive its content-idempotent ID.
    ///
    /// # Errors
    /// Rejects incomplete coverage, zero dimensions, and invalid timestamps.
    pub fn new(input: EmbeddingGenerationManifestInput) -> Result<Self, SearchArtifactError> {
        if input.vector_count != input.source.eligible_uuid_count {
            return Err(invalid(
                "embedding generation",
                "vector_count must equal eligible_uuid_count",
            ));
        }
        if input.dimension == 0 {
            return Err(invalid(
                "embedding generation dimension",
                "must be greater than zero",
            ));
        }
        if input.generated_at_micros < 0 || input.committed_at_micros < 0 {
            return Err(invalid(
                "embedding generation timestamp",
                "must be non-negative UTC microseconds",
            ));
        }
        if input.committed_at_micros < input.generated_at_micros {
            return Err(invalid(
                "embedding generation timestamp",
                "committed_at_micros must not precede generated_at_micros",
            ));
        }
        let generation_id = EmbeddingGenerationId::for_generation(
            input.compatibility_id,
            input.source.fingerprint,
            input.content_digest,
        );
        Ok(Self {
            compatibility_id: input.compatibility_id,
            generation_id,
            source: input.source,
            content_digest: input.content_digest,
            vector_count: input.vector_count,
            dimension: input.dimension,
            generated_at_micros: input.generated_at_micros,
            committed_at_micros: input.committed_at_micros,
            publication_fingerprint: input.publication_fingerprint,
        })
    }

    /// Serialize exact compact deterministic JSON.
    ///
    /// # Errors
    /// Returns a structured corruption error only for unexpected serialization failure.
    pub fn to_canonical_json(&self) -> Result<Vec<u8>, SearchArtifactError> {
        serde_json::to_vec(&serde_json::json!({
            "committed_at_micros": self.committed_at_micros,
            "compatibility_id": self.compatibility_id.to_hex(),
            "content_digest": self.content_digest.to_hex(),
            "dependency_input_digest": encode_digest(self.source.dependency_input_digest),
            "dimension": self.dimension,
            "eligible_uuid_count": self.source.eligible_uuid_count,
            "generated_at_micros": self.generated_at_micros,
            "generation_id": self.generation_id.to_hex(),
            "graph_generation": self.source.graph_generation,
            "label_membership_digest": encode_digest(self.source.label_membership_digest),
            "manifest_version": EMBEDDING_GENERATION_MANIFEST_VERSION,
            "publication_fingerprint": self.publication_fingerprint.to_hex(),
            "source_fingerprint": self.source.fingerprint.to_hex(),
            "vector_count": self.vector_count,
        }))
        .map_err(|error| corrupt(Path::new("<memory>"), error.to_string()))
    }

    /// Parse exact canonical manifest bytes and re-run every invariant.
    ///
    /// # Errors
    /// Distinguishes oversized, corrupt, and incompatible persisted metadata.
    pub fn from_json(path: &Path, bytes: &[u8]) -> Result<Self, SearchArtifactError> {
        if bytes.len() > MAX_EMBEDDING_GENERATION_MANIFEST_BYTES {
            return Err(SearchArtifactError::ResourceExhausted {
                resource: "embedding_generation_manifest_bytes",
                limit: MAX_EMBEDDING_GENERATION_MANIFEST_BYTES as u64,
            });
        }
        let value: Value =
            serde_json::from_slice(bytes).map_err(|error| corrupt(path, error.to_string()))?;
        let object = value
            .as_object()
            .ok_or_else(|| corrupt(path, "expected a JSON object"))?;
        let version = object
            .get("manifest_version")
            .and_then(Value::as_u64)
            .ok_or_else(|| corrupt(path, "manifest_version must be an unsigned integer"))?;
        if version != u64::from(EMBEDDING_GENERATION_MANIFEST_VERSION) {
            return Err(SearchArtifactError::IncompatibleManifest {
                path: path.to_path_buf(),
                found: version,
                supported: EMBEDDING_GENERATION_MANIFEST_VERSION,
            });
        }
        let raw: RawEmbeddingGenerationManifest =
            serde_json::from_value(value).map_err(|error| corrupt(path, error.to_string()))?;
        debug_assert_eq!(raw.manifest_version, EMBEDDING_GENERATION_MANIFEST_VERSION);
        let compatibility_id = EmbeddingCompatibilityId::from_hex(&raw.compatibility_id)
            .map_err(|error| corrupt(path, error.to_string()))?;
        let content_digest = EmbeddingContentDigest::from_hex(&raw.content_digest)
            .map_err(|error| corrupt(path, error.to_string()))?;
        let expected_generation = EmbeddingGenerationId::from_hex(&raw.generation_id)
            .map_err(|error| corrupt(path, error.to_string()))?;
        let expected_source = EmbeddingSourceFingerprint::from_hex(&raw.source_fingerprint)
            .map_err(|error| corrupt(path, error.to_string()))?;
        let source = EmbeddingSourceState::new(
            raw.graph_generation,
            parse_digest("label_membership_digest", &raw.label_membership_digest)
                .map_err(|error| corrupt(path, error.to_string()))?,
            parse_digest("dependency_input_digest", &raw.dependency_input_digest)
                .map_err(|error| corrupt(path, error.to_string()))?,
            raw.eligible_uuid_count,
        );
        if source.fingerprint != expected_source {
            return Err(corrupt(
                path,
                "source_fingerprint does not match source state",
            ));
        }
        let manifest = Self::new(EmbeddingGenerationManifestInput {
            compatibility_id,
            source,
            content_digest,
            vector_count: raw.vector_count,
            dimension: raw.dimension,
            generated_at_micros: raw.generated_at_micros,
            committed_at_micros: raw.committed_at_micros,
            publication_fingerprint: EmbeddingPublicationFingerprint::from_hex(
                &raw.publication_fingerprint,
            )
            .map_err(|error| corrupt(path, error.to_string()))?,
        })
        .map_err(|error| corrupt(path, error.to_string()))?;
        if manifest.generation_id != expected_generation {
            return Err(corrupt(
                path,
                "generation_id does not match compatibility, source, and content",
            ));
        }
        if manifest
            .to_canonical_json()
            .map_err(|error| corrupt(path, error.to_string()))?
            != bytes
        {
            return Err(corrupt(path, "manifest bytes are not exact canonical JSON"));
        }
        Ok(manifest)
    }

    /// Content-idempotent generation identity.
    #[must_use]
    pub const fn generation_id(&self) -> EmbeddingGenerationId {
        self.generation_id
    }

    /// Compatibility lineage identity.
    #[must_use]
    pub const fn compatibility_id(&self) -> EmbeddingCompatibilityId {
        self.compatibility_id
    }

    /// Exact source state.
    #[must_use]
    pub const fn source(&self) -> EmbeddingSourceState {
        self.source
    }

    /// Canonical content digest.
    #[must_use]
    pub const fn content_digest(&self) -> EmbeddingContentDigest {
        self.content_digest
    }

    /// Complete vector row count.
    #[must_use]
    pub const fn vector_count(&self) -> u64 {
        self.vector_count
    }

    /// Fixed vector width.
    #[must_use]
    pub const fn dimension(&self) -> u32 {
        self.dimension
    }

    /// Producer completion time in UTC microseconds since Unix epoch.
    #[must_use]
    pub const fn generated_at_micros(&self) -> i64 {
        self.generated_at_micros
    }

    /// Durable publication time in UTC microseconds since Unix epoch.
    #[must_use]
    pub const fn committed_at_micros(&self) -> i64 {
        self.committed_at_micros
    }

    /// Exact persisted vector-file fingerprint.
    #[must_use]
    pub const fn publication_fingerprint(&self) -> EmbeddingPublicationFingerprint {
        self.publication_fingerprint
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawEmbeddingGenerationManifest {
    manifest_version: u32,
    compatibility_id: String,
    generation_id: String,
    source_fingerprint: String,
    graph_generation: u64,
    label_membership_digest: String,
    dependency_input_digest: String,
    eligible_uuid_count: u64,
    content_digest: String,
    vector_count: u64,
    dimension: u32,
    generated_at_micros: i64,
    committed_at_micros: i64,
    publication_fingerprint: String,
}

fn parse_digest(field: &'static str, value: &str) -> Result<[u8; 32], SearchArtifactError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(invalid(
            field,
            "must be exactly 64 lowercase hexadecimal digits",
        ));
    }
    let mut output = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        output[index] = (hex_nibble(pair[0]) << 4) | hex_nibble(pair[1]);
    }
    Ok(output)
}

fn encode_digest(value: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in value {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn hex_nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => unreachable!("parse_digest validates lowercase hexadecimal input"),
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
        path: PathBuf::from(path),
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest_input() -> EmbeddingGenerationManifestInput {
        EmbeddingGenerationManifestInput {
            compatibility_id: EmbeddingCompatibilityId::from_hex(&"11".repeat(32)).unwrap(),
            source: EmbeddingSourceState::new(7, [2; 32], [3; 32], 2),
            content_digest: EmbeddingContentDigest::digest(b"canonical rows"),
            vector_count: 2,
            dimension: 4,
            generated_at_micros: 100,
            committed_at_micros: 101,
            publication_fingerprint: EmbeddingPublicationFingerprint::digest(b"parquet"),
        }
    }

    #[test]
    fn every_source_component_changes_fingerprint() {
        let base = EmbeddingSourceState::new(1, [2; 32], [3; 32], 4).fingerprint();
        let changed = [
            EmbeddingSourceState::new(2, [2; 32], [3; 32], 4).fingerprint(),
            EmbeddingSourceState::new(1, [9; 32], [3; 32], 4).fingerprint(),
            EmbeddingSourceState::new(1, [2; 32], [9; 32], 4).fingerprint(),
            EmbeddingSourceState::new(1, [2; 32], [3; 32], 5).fingerprint(),
        ];
        assert!(changed.into_iter().all(|value| value != base));
    }

    #[test]
    fn complete_manifest_round_trips_exactly() {
        let manifest = EmbeddingGenerationManifest::new(manifest_input()).unwrap();
        let bytes = manifest.to_canonical_json().unwrap();
        let reopened =
            EmbeddingGenerationManifest::from_json(Path::new("manifest.json"), &bytes).unwrap();
        assert_eq!(reopened, manifest);
        assert_eq!(reopened.to_canonical_json().unwrap(), bytes);
        assert_eq!(
            reopened.generation_id(),
            EmbeddingGenerationId::for_generation(
                reopened.compatibility_id(),
                reopened.source().fingerprint(),
                reopened.content_digest()
            )
        );
    }

    #[test]
    fn constructor_rejects_incomplete_dimensions_and_timestamps() {
        let mut incomplete = manifest_input();
        incomplete.vector_count = 1;
        assert!(EmbeddingGenerationManifest::new(incomplete).is_err());

        let mut zero_dimension = manifest_input();
        zero_dimension.dimension = 0;
        assert!(EmbeddingGenerationManifest::new(zero_dimension).is_err());

        let mut negative = manifest_input();
        negative.generated_at_micros = -1;
        assert!(EmbeddingGenerationManifest::new(negative).is_err());

        let mut reversed = manifest_input();
        reversed.committed_at_micros = 99;
        assert!(EmbeddingGenerationManifest::new(reversed).is_err());
    }

    #[test]
    fn reopen_rejects_identity_digest_and_shape_corruption() {
        let manifest = EmbeddingGenerationManifest::new(manifest_input()).unwrap();
        let mut value: Value =
            serde_json::from_slice(&manifest.to_canonical_json().unwrap()).unwrap();
        value["generation_id"] = Value::String("22".repeat(32));
        assert!(matches!(
            EmbeddingGenerationManifest::from_json(
                Path::new("manifest.json"),
                &serde_json::to_vec(&value).unwrap()
            ),
            Err(SearchArtifactError::CorruptManifest { .. })
        ));

        value["generation_id"] = Value::String(manifest.generation_id().to_hex());
        value["content_digest"] = Value::String("not-a-digest".to_owned());
        assert!(matches!(
            EmbeddingGenerationManifest::from_json(
                Path::new("manifest.json"),
                &serde_json::to_vec(&value).unwrap()
            ),
            Err(SearchArtifactError::CorruptManifest { .. })
        ));

        value["content_digest"] = Value::String(manifest.content_digest().to_hex());
        value["unknown"] = Value::Bool(true);
        assert!(matches!(
            EmbeddingGenerationManifest::from_json(
                Path::new("manifest.json"),
                &serde_json::to_vec(&value).unwrap()
            ),
            Err(SearchArtifactError::CorruptManifest { .. })
        ));
    }

    #[test]
    fn reopen_rejects_duplicate_noncanonical_version_and_size() {
        let manifest = EmbeddingGenerationManifest::new(manifest_input()).unwrap();
        let canonical = String::from_utf8(manifest.to_canonical_json().unwrap()).unwrap();
        let duplicate = canonical.replacen(r#""dimension":4"#, r#""dimension":4,"dimension":4"#, 1);
        assert_ne!(duplicate, canonical);
        let padded = format!(" {canonical}");
        for bytes in [duplicate.as_bytes(), padded.as_bytes()] {
            assert!(matches!(
                EmbeddingGenerationManifest::from_json(Path::new("manifest.json"), bytes),
                Err(SearchArtifactError::CorruptManifest { .. })
            ));
        }

        let mut value: Value = serde_json::from_str(&canonical).unwrap();
        value["manifest_version"] = Value::from(99);
        assert!(matches!(
            EmbeddingGenerationManifest::from_json(
                Path::new("manifest.json"),
                &serde_json::to_vec(&value).unwrap()
            ),
            Err(SearchArtifactError::IncompatibleManifest {
                found: 99,
                supported: EMBEDDING_GENERATION_MANIFEST_VERSION,
                ..
            })
        ));

        let oversized = vec![b' '; MAX_EMBEDDING_GENERATION_MANIFEST_BYTES + 1];
        assert!(matches!(
            EmbeddingGenerationManifest::from_json(Path::new("manifest.json"), &oversized),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "embedding_generation_manifest_bytes",
                ..
            })
        ));
    }
}
