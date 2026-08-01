//! Versioned, collision-safe identities for primary embedding generations.

use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use unicode_normalization::UnicodeNormalization;

use crate::SearchArtifactError;

/// Compatibility descriptor schema implemented by this release.
pub const EMBEDDING_IDENTITY_VERSION: u32 = 1;
/// Maximum normalized bytes in a caller-facing display name.
pub const MAX_EMBEDDING_DISPLAY_NAME_BYTES: usize = 255;
const MAX_IDENTITY_TEXT_BYTES: usize = 1_024;
const MAX_IDENTITY_JSON_BYTES: usize = 64 * 1024;
const MAX_IDENTITY_JSON_DEPTH: usize = 32;

/// A normalized caller-facing alias. It is never used as a filesystem path.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EmbeddingDisplayName(String);

impl EmbeddingDisplayName {
    /// Normalize a display name to NFC and enforce the v1 safety contract.
    ///
    /// # Errors
    /// Rejects empty, surrounding-whitespace, control, path-like, or oversized names.
    pub fn new(value: &str) -> Result<Self, SearchArtifactError> {
        let normalized: String = value.nfc().collect();
        if normalized.is_empty() {
            return Err(invalid("embedding display name", "must not be empty"));
        }
        if normalized.trim() != normalized {
            return Err(invalid(
                "embedding display name",
                "must not have leading or trailing whitespace",
            ));
        }
        if normalized.chars().any(char::is_control) {
            return Err(invalid(
                "embedding display name",
                "must not contain control characters",
            ));
        }
        if normalized.contains('/')
            || normalized.contains('\\')
            || matches!(normalized.as_str(), "." | "..")
        {
            return Err(invalid(
                "embedding display name",
                "must not be a path or contain path separators",
            ));
        }
        if normalized.len() > MAX_EMBEDDING_DISPLAY_NAME_BYTES {
            return Err(invalid(
                "embedding display name",
                format!(
                    "{} UTF-8 bytes exceeds {MAX_EMBEDDING_DISPLAY_NAME_BYTES}",
                    normalized.len()
                ),
            ));
        }
        Ok(Self(normalized))
    }

    /// Normalized case-sensitive display text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Producer category and fields that participate in compatibility.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EmbeddingProducerIdentity {
    /// Read-only M18 algorithm output.
    M18 {
        /// Stable algorithm token.
        algorithm: String,
        /// Frozen algorithm contract version.
        algorithm_version: String,
    },
    /// Process-local or embedded model implementation.
    Local {
        /// Adapter or implementation identity.
        implementation: String,
        /// Model identifier.
        model: String,
        /// Immutable revision, or the literal `unavailable`.
        revision: String,
        /// Adapter response contract.
        contract_version: String,
    },
    /// Caller-registered callback contract.
    Callback {
        /// Stable callback contract identity, never a function address.
        callback_contract: String,
        /// Callback request/response version.
        contract_version: String,
    },
    /// Explicit remote provider selection.
    Remote {
        /// Normalized provider token.
        provider: String,
        /// Provider model identifier.
        model: String,
        /// Immutable revision, or the literal `unavailable`.
        revision: String,
        /// Provider response contract version.
        response_contract_version: String,
    },
    /// Complete caller-supplied UUID/vector batch.
    CallerSupplied {
        /// Caller batch contract version.
        contract_version: String,
    },
}

/// Persisted numeric value contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EmbeddingValueType {
    /// IEEE-754 binary32 values.
    Float32,
}

/// Persisted vector normalization contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EmbeddingNormalization {
    /// Values are stored without normalization.
    None,
    /// Every vector is stored with unit L2 norm.
    L2,
}

/// Persisted retrieval distance contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EmbeddingDistance {
    /// Exact cosine similarity.
    Cosine,
}

/// How token counts in a producer contract were obtained.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TokenCountClass {
    /// Exact tokenizer implemented locally.
    ExactLocal,
    /// Exact count reported by the selected provider.
    ProviderReported,
    /// Conservative approximation, never represented as exact.
    Approximate,
}

/// Tokenizer and input-limit identity for text-derived embeddings.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenizerIdentity {
    /// Tokenizer identifier, or the literal `unavailable`.
    pub identifier: String,
    /// Immutable tokenizer version, or the literal `unavailable`.
    pub version: String,
    /// Exact/provider/approximate count classification.
    pub count_class: TokenCountClass,
    /// Maximum supported tokens in one model input.
    pub max_input_tokens: u64,
    /// Versioned text-normalization token.
    pub normalization: String,
}

/// Explicit chunking contract; silent truncation is never represented.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkingIdentity {
    /// Maximum tokens in one chunk.
    pub chunk_size_tokens: u64,
    /// Tokens repeated between adjacent chunks.
    pub overlap_tokens: u64,
    /// Versioned aggregation token.
    pub aggregation: String,
    /// Must be the literal `reject`; oversize unchunked input fails.
    pub truncation_policy: String,
}

/// Untrusted compatibility fields before validation and canonicalization.
#[derive(Clone, Debug)]
pub struct EmbeddingCompatibilityInput {
    /// Statically distinct producer identity.
    pub producer: EmbeddingProducerIdentity,
    /// Fixed vector width.
    pub dimensions: u32,
    /// Persisted numeric type.
    pub value_type: EmbeddingValueType,
    /// Persisted normalization.
    pub normalization: EmbeddingNormalization,
    /// Persisted distance contract.
    pub distance: EmbeddingDistance,
    /// Tokenizer contract for text-derived producers.
    pub tokenizer: Option<TokenizerIdentity>,
    /// Explicit chunking contract, if any.
    pub chunking: Option<ChunkingIdentity>,
    /// Normalized algorithm/provider hyperparameters.
    pub hyperparameters: BTreeMap<String, Value>,
    /// Non-empty normalized input/property recipe.
    pub input_recipe: BTreeMap<String, Value>,
    /// Non-empty normalized graph projection recipe.
    pub source_projection_recipe: BTreeMap<String, Value>,
}

/// Validated compatibility descriptor whose canonical JSON is identity input.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct EmbeddingCompatibilityDescriptor {
    schema_version: u32,
    producer: EmbeddingProducerIdentity,
    dimensions: u32,
    value_type: EmbeddingValueType,
    normalization: EmbeddingNormalization,
    distance: EmbeddingDistance,
    #[serde(skip_serializing_if = "Option::is_none")]
    tokenizer: Option<TokenizerIdentity>,
    #[serde(skip_serializing_if = "Option::is_none")]
    chunking: Option<ChunkingIdentity>,
    hyperparameters: BTreeMap<String, Value>,
    input_recipe: BTreeMap<String, Value>,
    source_projection_recipe: BTreeMap<String, Value>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawEmbeddingCompatibilityDescriptor {
    schema_version: u32,
    producer: EmbeddingProducerIdentity,
    dimensions: u32,
    value_type: EmbeddingValueType,
    normalization: EmbeddingNormalization,
    distance: EmbeddingDistance,
    tokenizer: Option<TokenizerIdentity>,
    chunking: Option<ChunkingIdentity>,
    hyperparameters: BTreeMap<String, Value>,
    input_recipe: BTreeMap<String, Value>,
    source_projection_recipe: BTreeMap<String, Value>,
}

impl EmbeddingCompatibilityDescriptor {
    /// Validate all compatibility fields and freeze schema version 1.
    ///
    /// # Errors
    /// Rejects incomplete producer/tokenizer/chunking identities, empty recipes,
    /// unnormalized strings, excessive nesting, and oversized canonical JSON.
    pub fn new(input: EmbeddingCompatibilityInput) -> Result<Self, SearchArtifactError> {
        validate_producer(&input.producer)?;
        if input.dimensions == 0 {
            return Err(invalid("embedding dimensions", "must be greater than zero"));
        }
        validate_tokenizer(input.tokenizer.as_ref())?;
        validate_chunking(input.chunking.as_ref(), input.tokenizer.as_ref())?;
        if matches!(input.producer, EmbeddingProducerIdentity::Remote { .. })
            && input.tokenizer.is_none()
        {
            return Err(invalid(
                "embedding tokenizer",
                "remote producers require an explicit tokenizer contract",
            ));
        }
        validate_json_map("embedding hyperparameters", &input.hyperparameters, false)?;
        validate_json_map("embedding input recipe", &input.input_recipe, true)?;
        validate_json_map(
            "embedding source projection recipe",
            &input.source_projection_recipe,
            true,
        )?;
        let descriptor = Self {
            schema_version: EMBEDDING_IDENTITY_VERSION,
            producer: input.producer,
            dimensions: input.dimensions,
            value_type: input.value_type,
            normalization: input.normalization,
            distance: input.distance,
            tokenizer: input.tokenizer,
            chunking: input.chunking,
            hyperparameters: input.hyperparameters,
            input_recipe: input.input_recipe,
            source_projection_recipe: input.source_projection_recipe,
        };
        descriptor.to_canonical_json()?;
        Ok(descriptor)
    }

    /// Parse one exact canonical persisted descriptor and re-run validation.
    ///
    /// # Errors
    /// Distinguishes oversized, malformed/corrupt, and incompatible descriptor
    /// bytes. Unknown fields and noncanonical encodings fail closed.
    pub fn from_json(path: &Path, bytes: &[u8]) -> Result<Self, SearchArtifactError> {
        if bytes.len() > MAX_IDENTITY_JSON_BYTES {
            return Err(SearchArtifactError::ResourceExhausted {
                resource: "embedding_descriptor_bytes",
                limit: MAX_IDENTITY_JSON_BYTES as u64,
            });
        }
        let value: Value = serde_json::from_slice(bytes)
            .map_err(|error| corrupt_descriptor(path, error.to_string()))?;
        let object = value
            .as_object()
            .ok_or_else(|| corrupt_descriptor(path, "expected a JSON object"))?;
        let version = object
            .get("schema_version")
            .and_then(Value::as_u64)
            .ok_or_else(|| {
                corrupt_descriptor(path, "schema_version must be an unsigned integer")
            })?;
        if version != u64::from(EMBEDDING_IDENTITY_VERSION) {
            return Err(SearchArtifactError::IncompatibleManifest {
                path: path.to_path_buf(),
                found: version,
                supported: EMBEDDING_IDENTITY_VERSION,
            });
        }
        let raw: RawEmbeddingCompatibilityDescriptor = serde_json::from_value(value)
            .map_err(|error| corrupt_descriptor(path, error.to_string()))?;
        let descriptor = Self::new(EmbeddingCompatibilityInput {
            producer: raw.producer,
            dimensions: raw.dimensions,
            value_type: raw.value_type,
            normalization: raw.normalization,
            distance: raw.distance,
            tokenizer: raw.tokenizer,
            chunking: raw.chunking,
            hyperparameters: raw.hyperparameters,
            input_recipe: raw.input_recipe,
            source_projection_recipe: raw.source_projection_recipe,
        })
        .map_err(|error| corrupt_descriptor(path, error.to_string()))?;
        debug_assert_eq!(raw.schema_version, EMBEDDING_IDENTITY_VERSION);
        let canonical = descriptor
            .to_canonical_json()
            .map_err(|error| corrupt_descriptor(path, error.to_string()))?;
        if canonical != bytes {
            return Err(corrupt_descriptor(
                path,
                "descriptor bytes are not exact canonical JSON",
            ));
        }
        Ok(descriptor)
    }

    /// Serialize compact JSON with every object key sorted by UTF-8 bytes.
    ///
    /// # Errors
    /// Returns a structured validation error if serialization exceeds the v1 bound.
    pub fn to_canonical_json(&self) -> Result<Vec<u8>, SearchArtifactError> {
        let value = serde_json::to_value(self)
            .map_err(|error| invalid("embedding identity", error.to_string()))?;
        let mut output = Vec::new();
        write_canonical_value(&value, &mut output)?;
        if output.len() > MAX_IDENTITY_JSON_BYTES {
            return Err(invalid(
                "embedding identity",
                format!(
                    "{} canonical JSON bytes exceeds {MAX_IDENTITY_JSON_BYTES}",
                    output.len()
                ),
            ));
        }
        Ok(output)
    }

    /// SHA-256 compatibility identity over canonical descriptor JSON.
    ///
    /// # Errors
    /// Propagates canonical serialization validation.
    pub fn compatibility_id(&self) -> Result<EmbeddingCompatibilityId, SearchArtifactError> {
        self.to_canonical_json()
            .map(|bytes| EmbeddingCompatibilityId(hash_bytes(&bytes)))
    }

    /// Fixed vector width.
    #[must_use]
    pub const fn dimensions(&self) -> u32 {
        self.dimensions
    }

    /// Persisted vector normalization contract.
    #[must_use]
    pub const fn normalization(&self) -> EmbeddingNormalization {
        self.normalization
    }

    /// Statically distinct producer descriptor.
    #[must_use]
    pub const fn producer(&self) -> &EmbeddingProducerIdentity {
        &self.producer
    }

    /// Persisted tokenizer identity for text-derived embeddings.
    #[must_use]
    pub const fn tokenizer(&self) -> Option<&TokenizerIdentity> {
        self.tokenizer.as_ref()
    }

    /// Persisted explicit chunking contract, if any.
    #[must_use]
    pub const fn chunking(&self) -> Option<&ChunkingIdentity> {
        self.chunking.as_ref()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Sha256Value([u8; 32]);

macro_rules! identity_type {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(Sha256Value);

        impl $name {
            /// Parse exactly 64 lowercase hexadecimal digits.
            ///
            /// # Errors
            /// Rejects uppercase, non-hexadecimal, or wrong-width input.
            pub fn from_hex(value: &str) -> Result<Self, SearchArtifactError> {
                parse_digest(value).map(Self)
            }

            /// Lowercase hexadecimal representation.
            #[must_use]
            pub fn to_hex(self) -> String {
                encode_digest(self.0)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_tuple(stringify!($name))
                    .field(&self.to_hex())
                    .finish()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.to_hex())
            }
        }
    };
}

identity_type!(
    EmbeddingCompatibilityId,
    "SHA-256 identity of one validated compatibility descriptor."
);
identity_type!(
    EmbeddingSourceFingerprint,
    "SHA-256 fingerprint of one committed source projection."
);
identity_type!(
    EmbeddingContentDigest,
    "SHA-256 digest of canonical UUID/vector content."
);
identity_type!(
    EmbeddingGenerationId,
    "SHA-256 identity of compatibility, source, and canonical content."
);

impl EmbeddingSourceFingerprint {
    /// Digest canonical committed-source bytes.
    #[must_use]
    pub fn digest(bytes: &[u8]) -> Self {
        Self(hash_bytes(bytes))
    }

    fn bytes(self) -> [u8; 32] {
        self.0.0
    }
}

impl EmbeddingContentDigest {
    /// Digest canonical UUID/vector content bytes.
    #[must_use]
    pub fn digest(bytes: &[u8]) -> Self {
        Self(hash_bytes(bytes))
    }

    fn bytes(self) -> [u8; 32] {
        self.0.0
    }
}

impl EmbeddingCompatibilityId {
    fn bytes(self) -> [u8; 32] {
        self.0.0
    }
}

impl EmbeddingGenerationId {
    /// Hash fixed-width compatibility, source, and content digests in that order.
    #[must_use]
    pub fn for_generation(
        compatibility: EmbeddingCompatibilityId,
        source: EmbeddingSourceFingerprint,
        content: EmbeddingContentDigest,
    ) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(compatibility.bytes());
        hasher.update(source.bytes());
        hasher.update(content.bytes());
        Self(Sha256Value(hasher.finalize().into()))
    }
}

fn validate_producer(producer: &EmbeddingProducerIdentity) -> Result<(), SearchArtifactError> {
    let fields: &[(&str, &str)] = match producer {
        EmbeddingProducerIdentity::M18 {
            algorithm,
            algorithm_version,
        } => &[
            ("algorithm", algorithm),
            ("algorithm_version", algorithm_version),
        ],
        EmbeddingProducerIdentity::Local {
            implementation,
            model,
            revision,
            contract_version,
        } => &[
            ("implementation", implementation),
            ("model", model),
            ("revision", revision),
            ("contract_version", contract_version),
        ],
        EmbeddingProducerIdentity::Callback {
            callback_contract,
            contract_version,
        } => &[
            ("callback_contract", callback_contract),
            ("contract_version", contract_version),
        ],
        EmbeddingProducerIdentity::Remote {
            provider,
            model,
            revision,
            response_contract_version,
        } => &[
            ("provider", provider),
            ("model", model),
            ("revision", revision),
            ("response_contract_version", response_contract_version),
        ],
        EmbeddingProducerIdentity::CallerSupplied { contract_version } => {
            &[("contract_version", contract_version)]
        }
    };
    for &(field, value) in fields {
        validate_identity_text(field, value)?;
    }
    Ok(())
}

fn validate_tokenizer(tokenizer: Option<&TokenizerIdentity>) -> Result<(), SearchArtifactError> {
    let Some(tokenizer) = tokenizer else {
        return Ok(());
    };
    validate_identity_text("tokenizer identifier", &tokenizer.identifier)?;
    validate_identity_text("tokenizer version", &tokenizer.version)?;
    validate_identity_text("tokenizer normalization", &tokenizer.normalization)?;
    if tokenizer.max_input_tokens == 0 {
        return Err(invalid(
            "tokenizer max_input_tokens",
            "must be greater than zero",
        ));
    }
    Ok(())
}

fn validate_chunking(
    chunking: Option<&ChunkingIdentity>,
    tokenizer: Option<&TokenizerIdentity>,
) -> Result<(), SearchArtifactError> {
    let Some(chunking) = chunking else {
        return Ok(());
    };
    let tokenizer = tokenizer.ok_or_else(|| {
        invalid(
            "embedding chunking",
            "requires an explicit tokenizer contract",
        )
    })?;
    if chunking.chunk_size_tokens == 0 || chunking.chunk_size_tokens > tokenizer.max_input_tokens {
        return Err(invalid(
            "chunk_size_tokens",
            "must be within the tokenizer input limit",
        ));
    }
    if chunking.overlap_tokens >= chunking.chunk_size_tokens {
        return Err(invalid(
            "overlap_tokens",
            "must be smaller than chunk_size_tokens",
        ));
    }
    validate_identity_text("chunk aggregation", &chunking.aggregation)?;
    if chunking.truncation_policy != "reject" {
        return Err(invalid(
            "truncation_policy",
            "must be reject; silent truncation is forbidden",
        ));
    }
    Ok(())
}

fn validate_json_map(
    field: &'static str,
    map: &BTreeMap<String, Value>,
    require_non_empty: bool,
) -> Result<(), SearchArtifactError> {
    if require_non_empty && map.is_empty() {
        return Err(invalid(field, "must not be empty"));
    }
    validate_json_value(field, &Value::Object(map.clone().into_iter().collect()), 0)
}

fn validate_json_value(
    field: &'static str,
    value: &Value,
    depth: usize,
) -> Result<(), SearchArtifactError> {
    if depth > MAX_IDENTITY_JSON_DEPTH {
        return Err(invalid(field, "exceeds maximum JSON nesting depth"));
    }
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) => Ok(()),
        Value::String(value) => validate_identity_text(field, value),
        Value::Array(values) => {
            for value in values {
                validate_json_value(field, value, depth + 1)?;
            }
            Ok(())
        }
        Value::Object(object) => {
            for (key, value) in object {
                validate_identity_text(field, key)?;
                if reserved_compatibility_key(key) {
                    return Err(invalid(
                        field,
                        format!("reserved non-compatibility field {key:?}"),
                    ));
                }
                validate_json_value(field, value, depth + 1)?;
            }
            Ok(())
        }
    }
}

fn validate_identity_text(field: &'static str, value: &str) -> Result<(), SearchArtifactError> {
    if value.is_empty() {
        return Err(invalid(field, "must not be empty"));
    }
    if value.trim() != value {
        return Err(invalid(field, "must not have surrounding whitespace"));
    }
    if value.chars().any(char::is_control) {
        return Err(invalid(field, "must not contain control characters"));
    }
    if !value.nfc().eq(value.chars()) {
        return Err(invalid(field, "must be normalized to Unicode NFC"));
    }
    if value.len() > MAX_IDENTITY_TEXT_BYTES {
        return Err(invalid(
            field,
            format!(
                "{} UTF-8 bytes exceeds {MAX_IDENTITY_TEXT_BYTES}",
                value.len()
            ),
        ));
    }
    Ok(())
}

fn reserved_compatibility_key(key: &str) -> bool {
    matches!(
        key.to_ascii_lowercase().as_str(),
        "alias"
            | "algorithm_run_uuid"
            | "api_key"
            | "apikey"
            | "authorization"
            | "committed_at"
            | "credential"
            | "credentials"
            | "generated_at"
            | "generation_id"
            | "password"
            | "refresh_token"
            | "run_id"
            | "run_uuid"
            | "secret"
            | "source_fingerprint"
            | "timestamp"
            | "token"
            | "access_token"
    )
}

fn write_canonical_value(value: &Value, output: &mut Vec<u8>) -> Result<(), SearchArtifactError> {
    match value {
        Value::Null => output.extend_from_slice(b"null"),
        Value::Bool(value) => output.extend_from_slice(if *value { b"true" } else { b"false" }),
        Value::Number(value) => output.extend_from_slice(value.to_string().as_bytes()),
        Value::String(value) => serde_json::to_writer(output, value)
            .map_err(|error| invalid("embedding identity", error.to_string()))?,
        Value::Array(values) => {
            output.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    output.push(b',');
                }
                write_canonical_value(value, output)?;
            }
            output.push(b']');
        }
        Value::Object(object) => {
            output.push(b'{');
            let mut keys = object.keys().collect::<Vec<_>>();
            keys.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
            for (index, key) in keys.into_iter().enumerate() {
                if index != 0 {
                    output.push(b',');
                }
                serde_json::to_writer(&mut *output, key)
                    .map_err(|error| invalid("embedding identity", error.to_string()))?;
                output.push(b':');
                write_canonical_value(&object[key], output)?;
            }
            output.push(b'}');
        }
    }
    Ok(())
}

fn hash_bytes(bytes: &[u8]) -> Sha256Value {
    Sha256Value(Sha256::digest(bytes).into())
}

fn encode_digest(value: Sha256Value) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in value.0 {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn parse_digest(value: &str) -> Result<Sha256Value, SearchArtifactError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(invalid(
            "embedding digest",
            "must be exactly 64 lowercase hexadecimal digits",
        ));
    }
    let mut bytes = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        bytes[index] = (hex_nibble(pair[0]) << 4) | hex_nibble(pair[1]);
    }
    Ok(Sha256Value(bytes))
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

fn corrupt_descriptor(path: &Path, reason: impl Into<String>) -> SearchArtifactError {
    SearchArtifactError::CorruptManifest {
        path: PathBuf::from(path),
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tokenizer() -> TokenizerIdentity {
        TokenizerIdentity {
            identifier: "tokenizer-v1".to_owned(),
            version: "1".to_owned(),
            count_class: TokenCountClass::ExactLocal,
            max_input_tokens: 512,
            normalization: "nfc-v1".to_owned(),
        }
    }

    fn input(producer: EmbeddingProducerIdentity) -> EmbeddingCompatibilityInput {
        let tokenizer =
            matches!(&producer, EmbeddingProducerIdentity::Remote { .. }).then(tokenizer);
        EmbeddingCompatibilityInput {
            producer,
            dimensions: 4,
            value_type: EmbeddingValueType::Float32,
            normalization: EmbeddingNormalization::L2,
            distance: EmbeddingDistance::Cosine,
            tokenizer,
            chunking: None,
            hyperparameters: BTreeMap::new(),
            input_recipe: BTreeMap::from([("properties".to_owned(), serde_json::json!(["text"]))]),
            source_projection_recipe: BTreeMap::from([(
                "label".to_owned(),
                Value::String("Paper".to_owned()),
            )]),
        }
    }

    fn caller() -> EmbeddingProducerIdentity {
        EmbeddingProducerIdentity::CallerSupplied {
            contract_version: "caller-batch-v1".to_owned(),
        }
    }

    fn remote_input() -> EmbeddingCompatibilityInput {
        let mut input = input(EmbeddingProducerIdentity::Remote {
            provider: "openrouter".to_owned(),
            model: "provider/model".to_owned(),
            revision: "rev-1".to_owned(),
            response_contract_version: "remote-v1".to_owned(),
        });
        input.chunking = Some(ChunkingIdentity {
            chunk_size_tokens: 256,
            overlap_tokens: 32,
            aggregation: "mean-v1".to_owned(),
            truncation_policy: "reject".to_owned(),
        });
        input
            .hyperparameters
            .insert("temperature".to_owned(), serde_json::json!(0));
        input
    }

    fn compatibility_id(input: EmbeddingCompatibilityInput) -> EmbeddingCompatibilityId {
        EmbeddingCompatibilityDescriptor::new(input)
            .unwrap()
            .compatibility_id()
            .unwrap()
    }

    #[test]
    fn display_names_normalize_without_case_folding() {
        let composed = EmbeddingDisplayName::new("Café").unwrap();
        let decomposed = EmbeddingDisplayName::new("Cafe\u{301}").unwrap();
        assert_eq!(composed, decomposed);
        assert_ne!(
            EmbeddingDisplayName::new("Space").unwrap(),
            EmbeddingDisplayName::new("space").unwrap()
        );
        for invalid in ["", " space", "space ", ".", "..", "a/b", "a\\b", "a\n"] {
            assert!(EmbeddingDisplayName::new(invalid).is_err(), "{invalid:?}");
        }
    }

    #[test]
    fn canonical_identity_ignores_map_insertion_order() {
        let mut left = input(caller());
        let mut right = input(caller());
        left.hyperparameters
            .insert("z".to_owned(), serde_json::json!({"b": 2, "a": 1}));
        left.hyperparameters
            .insert("a".to_owned(), Value::Bool(true));
        right
            .hyperparameters
            .insert("a".to_owned(), Value::Bool(true));
        right
            .hyperparameters
            .insert("z".to_owned(), serde_json::json!({"a": 1, "b": 2}));
        let left = EmbeddingCompatibilityDescriptor::new(left).unwrap();
        let right = EmbeddingCompatibilityDescriptor::new(right).unwrap();
        assert_eq!(
            left.to_canonical_json().unwrap(),
            right.to_canonical_json().unwrap()
        );
        assert_eq!(
            left.compatibility_id().unwrap(),
            right.compatibility_id().unwrap()
        );
    }

    #[test]
    fn every_producer_kind_is_explicit_in_identity() {
        let producers = [
            EmbeddingProducerIdentity::M18 {
                algorithm: "node2vec".to_owned(),
                algorithm_version: "node2vec-v1".to_owned(),
            },
            EmbeddingProducerIdentity::Local {
                implementation: "local-runtime".to_owned(),
                model: "model".to_owned(),
                revision: "rev-1".to_owned(),
                contract_version: "local-v1".to_owned(),
            },
            EmbeddingProducerIdentity::Callback {
                callback_contract: "callback-a".to_owned(),
                contract_version: "callback-v1".to_owned(),
            },
            EmbeddingProducerIdentity::Remote {
                provider: "openrouter".to_owned(),
                model: "provider/model".to_owned(),
                revision: "unavailable".to_owned(),
                response_contract_version: "remote-v1".to_owned(),
            },
            caller(),
        ];
        let mut identities = Vec::new();
        for producer in producers {
            identities.push(
                EmbeddingCompatibilityDescriptor::new(input(producer))
                    .unwrap()
                    .compatibility_id()
                    .unwrap(),
            );
        }
        identities.sort_unstable();
        identities.dedup();
        assert_eq!(identities.len(), 5);
    }

    #[test]
    fn every_configurable_descriptor_field_participates_in_identity() {
        let base = remote_input();
        let base_descriptor = EmbeddingCompatibilityDescriptor::new(base.clone()).unwrap();
        let canonical = String::from_utf8(base_descriptor.to_canonical_json().unwrap()).unwrap();
        assert!(canonical.contains(r#""schema_version":1"#));
        assert!(canonical.contains(r#""value_type":"float32""#));
        assert!(canonical.contains(r#""distance":"cosine""#));
        let base_id = base_descriptor.compatibility_id().unwrap();

        let mutations: &[fn(&mut EmbeddingCompatibilityInput)] = &[
            |input| {
                let EmbeddingProducerIdentity::Remote { provider, .. } = &mut input.producer else {
                    unreachable!()
                };
                *provider = "other-provider".to_owned();
            },
            |input| {
                let EmbeddingProducerIdentity::Remote { model, .. } = &mut input.producer else {
                    unreachable!()
                };
                *model = "provider/other-model".to_owned();
            },
            |input| {
                let EmbeddingProducerIdentity::Remote { revision, .. } = &mut input.producer else {
                    unreachable!()
                };
                *revision = "rev-2".to_owned();
            },
            |input| {
                let EmbeddingProducerIdentity::Remote {
                    response_contract_version,
                    ..
                } = &mut input.producer
                else {
                    unreachable!()
                };
                *response_contract_version = "remote-v2".to_owned();
            },
            |input| input.dimensions += 1,
            |input| input.normalization = EmbeddingNormalization::None,
            |input| {
                input.tokenizer.as_mut().unwrap().identifier = "tokenizer-v2".to_owned();
            },
            |input| input.tokenizer.as_mut().unwrap().version = "2".to_owned(),
            |input| {
                input.tokenizer.as_mut().unwrap().count_class = TokenCountClass::ProviderReported;
            },
            |input| input.tokenizer.as_mut().unwrap().max_input_tokens += 1,
            |input| {
                input.tokenizer.as_mut().unwrap().normalization = "nfc-v2".to_owned();
            },
            |input| input.chunking.as_mut().unwrap().chunk_size_tokens -= 1,
            |input| input.chunking.as_mut().unwrap().overlap_tokens += 1,
            |input| input.chunking.as_mut().unwrap().aggregation = "max-v1".to_owned(),
            |input| {
                input
                    .hyperparameters
                    .insert("temperature".to_owned(), serde_json::json!(1));
            },
            |input| {
                input
                    .input_recipe
                    .insert("separator".to_owned(), serde_json::json!("|"));
            },
            |input| {
                input
                    .source_projection_recipe
                    .insert("directed".to_owned(), serde_json::json!(true));
            },
        ];

        for mutate in mutations {
            let mut changed = base.clone();
            mutate(&mut changed);
            assert_ne!(compatibility_id(changed), base_id);
        }
    }

    #[test]
    fn generation_identity_uses_all_three_fixed_digests() {
        let compatibility = EmbeddingCompatibilityDescriptor::new(input(caller()))
            .unwrap()
            .compatibility_id()
            .unwrap();
        let source = EmbeddingSourceFingerprint::digest(b"source");
        let content = EmbeddingContentDigest::digest(b"content");
        let generation = EmbeddingGenerationId::for_generation(compatibility, source, content);
        assert_eq!(
            generation,
            EmbeddingGenerationId::from_hex(&generation.to_hex()).unwrap()
        );
        assert_ne!(
            generation,
            EmbeddingGenerationId::for_generation(
                compatibility,
                source,
                EmbeddingContentDigest::digest(b"changed")
            )
        );
        assert!(EmbeddingGenerationId::from_hex(&"A".repeat(64)).is_err());
        assert!(EmbeddingGenerationId::from_hex("abcd").is_err());
    }

    #[test]
    fn invalid_dimensions_recipes_and_chunking_fail_closed() {
        let mut zero = input(caller());
        zero.dimensions = 0;
        assert!(EmbeddingCompatibilityDescriptor::new(zero).is_err());

        let mut empty_recipe = input(caller());
        empty_recipe.input_recipe.clear();
        assert!(EmbeddingCompatibilityDescriptor::new(empty_recipe).is_err());

        let mut chunked = input(caller());
        chunked.tokenizer = Some(tokenizer());
        chunked.chunking = Some(ChunkingIdentity {
            chunk_size_tokens: 512,
            overlap_tokens: 512,
            aggregation: "mean-v1".to_owned(),
            truncation_policy: "truncate".to_owned(),
        });
        assert!(EmbeddingCompatibilityDescriptor::new(chunked).is_err());

        let mut remote_without_tokenizer = input(EmbeddingProducerIdentity::Remote {
            provider: "openrouter".to_owned(),
            model: "provider/model".to_owned(),
            revision: "unavailable".to_owned(),
            response_contract_version: "remote-v1".to_owned(),
        });
        remote_without_tokenizer.tokenizer = None;
        assert!(EmbeddingCompatibilityDescriptor::new(remote_without_tokenizer).is_err());

        let mut secret = input(caller());
        secret.hyperparameters.insert(
            "api_key".to_owned(),
            Value::String("must-not-persist".to_owned()),
        );
        assert!(EmbeddingCompatibilityDescriptor::new(secret).is_err());
    }

    #[test]
    fn persisted_descriptor_reopens_only_from_exact_canonical_bytes() {
        let descriptor = EmbeddingCompatibilityDescriptor::new(remote_input()).unwrap();
        let canonical = descriptor.to_canonical_json().unwrap();
        let reopened =
            EmbeddingCompatibilityDescriptor::from_json(Path::new("space.json"), &canonical)
                .unwrap();
        assert_eq!(reopened, descriptor);
        assert_eq!(
            reopened.compatibility_id().unwrap(),
            descriptor.compatibility_id().unwrap()
        );

        let mut padded = vec![b' '];
        padded.extend_from_slice(&canonical);
        assert!(matches!(
            EmbeddingCompatibilityDescriptor::from_json(Path::new("space.json"), &padded),
            Err(SearchArtifactError::CorruptManifest { .. })
        ));
    }

    #[test]
    fn persisted_descriptor_rejects_unknown_invalid_and_malformed_fields() {
        let descriptor = EmbeddingCompatibilityDescriptor::new(remote_input()).unwrap();
        let mut value: Value =
            serde_json::from_slice(&descriptor.to_canonical_json().unwrap()).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("alias".to_owned(), Value::String("forbidden".to_owned()));
        let unknown = serde_json::to_vec(&value).unwrap();
        assert!(matches!(
            EmbeddingCompatibilityDescriptor::from_json(Path::new("space.json"), &unknown),
            Err(SearchArtifactError::CorruptManifest { .. })
        ));

        value.as_object_mut().unwrap().remove("alias");
        value["dimensions"] = Value::from(0);
        let invalid = serde_json::to_vec(&value).unwrap();
        assert!(matches!(
            EmbeddingCompatibilityDescriptor::from_json(Path::new("space.json"), &invalid),
            Err(SearchArtifactError::CorruptManifest { .. })
        ));
        assert!(matches!(
            EmbeddingCompatibilityDescriptor::from_json(Path::new("space.json"), b"{"),
            Err(SearchArtifactError::CorruptManifest { .. })
        ));
    }

    #[test]
    fn persisted_descriptor_rejects_duplicate_keys_and_noncanonical_unicode() {
        let descriptor = EmbeddingCompatibilityDescriptor::new(remote_input()).unwrap();
        let canonical = String::from_utf8(descriptor.to_canonical_json().unwrap()).unwrap();
        let duplicate =
            canonical.replacen(r#""dimensions":4"#, r#""dimensions":4,"dimensions":4"#, 1);
        assert_ne!(duplicate, canonical);
        assert!(matches!(
            EmbeddingCompatibilityDescriptor::from_json(
                Path::new("space.json"),
                duplicate.as_bytes()
            ),
            Err(SearchArtifactError::CorruptManifest { .. })
        ));

        let mut unicode_input = remote_input();
        unicode_input
            .input_recipe
            .insert("unicode".to_owned(), Value::String("Café".to_owned()));
        let unicode_descriptor = EmbeddingCompatibilityDescriptor::new(unicode_input).unwrap();
        let canonical_unicode =
            String::from_utf8(unicode_descriptor.to_canonical_json().unwrap()).unwrap();
        let decomposed = canonical_unicode.replace("Café", "Cafe\u{301}");
        assert_ne!(decomposed, canonical_unicode);
        assert!(matches!(
            EmbeddingCompatibilityDescriptor::from_json(
                Path::new("space.json"),
                decomposed.as_bytes()
            ),
            Err(SearchArtifactError::CorruptManifest { .. })
        ));
    }

    #[test]
    fn persisted_descriptor_distinguishes_version_and_size() {
        let descriptor = EmbeddingCompatibilityDescriptor::new(remote_input()).unwrap();
        let mut value: Value =
            serde_json::from_slice(&descriptor.to_canonical_json().unwrap()).unwrap();
        value["schema_version"] = Value::from(99);
        let incompatible = serde_json::to_vec(&value).unwrap();
        assert!(matches!(
            EmbeddingCompatibilityDescriptor::from_json(Path::new("space.json"), &incompatible),
            Err(SearchArtifactError::IncompatibleManifest {
                found: 99,
                supported: EMBEDDING_IDENTITY_VERSION,
                ..
            })
        ));

        let oversized = vec![b' '; MAX_IDENTITY_JSON_BYTES + 1];
        assert!(matches!(
            EmbeddingCompatibilityDescriptor::from_json(Path::new("space.json"), &oversized),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "embedding_descriptor_bytes",
                ..
            })
        ));
    }
}
