//! Provider-neutral model capability and failure vocabulary.

use std::collections::BTreeSet;
use std::fmt;

use graphforge_storage::{ChunkingIdentity, SearchArtifactError, TokenizerIdentity};

/// Default selected only after a caller explicitly chooses remote inference.
pub const DEFAULT_REMOTE_PROVIDER: &str = "openrouter";
const MAX_IDENTITY_BYTES: usize = 1_024;

/// One statically named model capability.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ProviderCapability {
    /// Embed a canonical UUID-keyed document batch.
    DocumentEmbeddings,
    /// Embed one search query.
    QueryEmbeddings,
    /// Rerank a bounded UUID-keyed candidate set.
    CandidateReranking,
}

/// Explicit capabilities advertised by one exact model contract.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderCapabilities(BTreeSet<ProviderCapability>);

impl ProviderCapabilities {
    /// Validate a non-empty capability set.
    ///
    /// # Errors
    /// Rejects a model that advertises no usable capability.
    pub fn new(
        values: impl IntoIterator<Item = ProviderCapability>,
    ) -> Result<Self, SearchArtifactError> {
        let values = values.into_iter().collect::<BTreeSet<_>>();
        if values.is_empty() {
            return Err(invalid(
                "provider capabilities",
                "must advertise at least one capability",
            ));
        }
        Ok(Self(values))
    }

    /// Whether the exact model advertises one capability.
    #[must_use]
    pub fn supports(&self, capability: ProviderCapability) -> bool {
        self.0.contains(&capability)
    }
}

/// Resolved provider/model/tokenizer identity without credentials.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderModelContract {
    provider: String,
    model: String,
    revision: String,
    response_contract_version: String,
    capabilities: ProviderCapabilities,
    tokenizer: TokenizerIdentity,
    chunking: Option<ChunkingIdentity>,
}

impl ProviderModelContract {
    /// Resolve and validate one remote model contract.
    ///
    /// `None` resolves to [`DEFAULT_REMOTE_PROVIDER`]. An explicitly named
    /// provider is never substituted or used as a fallback.
    ///
    /// # Errors
    /// Rejects malformed identity, tokenizer, or chunking fields.
    pub fn remote(
        provider: Option<&str>,
        model: &str,
        revision: &str,
        response_contract_version: &str,
        capabilities: ProviderCapabilities,
        tokenizer: TokenizerIdentity,
        chunking: Option<ChunkingIdentity>,
    ) -> Result<Self, SearchArtifactError> {
        let contract = Self {
            provider: match provider {
                Some(value) => normalize_provider(value)?,
                None => DEFAULT_REMOTE_PROVIDER.to_owned(),
            },
            model: identity("provider model", model)?,
            revision: identity("provider revision", revision)?,
            response_contract_version: identity(
                "provider response contract version",
                response_contract_version,
            )?,
            capabilities,
            tokenizer,
            chunking,
        };
        validate_tokenizer(&contract.tokenizer)?;
        validate_chunking(contract.chunking.as_ref(), &contract.tokenizer)?;
        Ok(contract)
    }

    /// Normalized provider token.
    #[must_use]
    pub fn provider(&self) -> &str {
        &self.provider
    }

    /// Exact provider model identifier.
    #[must_use]
    pub fn model(&self) -> &str {
        &self.model
    }

    /// Immutable revision, or the literal `unavailable`.
    #[must_use]
    pub fn revision(&self) -> &str {
        &self.revision
    }

    /// Versioned provider response contract.
    #[must_use]
    pub fn response_contract_version(&self) -> &str {
        &self.response_contract_version
    }

    /// Explicit advertised capabilities.
    #[must_use]
    pub const fn capabilities(&self) -> &ProviderCapabilities {
        &self.capabilities
    }

    /// Resolved tokenizer/counting behavior.
    #[must_use]
    pub const fn tokenizer(&self) -> &TokenizerIdentity {
        &self.tokenizer
    }

    /// Explicit chunking behavior, if selected.
    #[must_use]
    pub const fn chunking(&self) -> Option<&ChunkingIdentity> {
        self.chunking.as_ref()
    }

    /// Reject a capability not advertised by this exact model.
    ///
    /// # Errors
    /// Returns a stable redacted error before any provider work begins.
    pub fn require(&self, capability: ProviderCapability) -> ProviderResult<()> {
        if self.capabilities.supports(capability) {
            Ok(())
        } else {
            Err(ProviderError::new(
                self,
                ProviderFailureClass::UnsupportedCapability,
            ))
        }
    }
}

/// Named upper bounds shared by provider adapter invocations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProviderRequestLimits {
    /// Maximum items in one invocation.
    pub items: usize,
    /// Maximum outbound UTF-8 bytes.
    pub input_bytes: usize,
    /// Maximum counted input tokens.
    pub input_tokens: u64,
    /// Maximum numeric response values.
    pub output_values: usize,
    /// Maximum provider calls including retries and chunks.
    pub provider_calls: usize,
}

impl Default for ProviderRequestLimits {
    fn default() -> Self {
        Self {
            items: 1_024,
            input_bytes: 8 * 1024 * 1024,
            input_tokens: 1_000_000,
            output_values: 16_777_216,
            provider_calls: 128,
        }
    }
}

impl ProviderRequestLimits {
    /// Validate that every named bound is usable.
    ///
    /// # Errors
    /// Rejects any zero bound.
    pub fn validate(self) -> Result<Self, SearchArtifactError> {
        if self.items == 0
            || self.input_bytes == 0
            || self.input_tokens == 0
            || self.output_values == 0
            || self.provider_calls == 0
        {
            return Err(invalid(
                "provider request limits",
                "every named bound must be non-zero",
            ));
        }
        Ok(self)
    }
}

/// Stable redacted provider failure classes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProviderFailureClass {
    /// Request shape, ordering, identity, text, or exact-contract validation failed.
    InvalidRequest,
    /// The model does not advertise the requested capability.
    UnsupportedCapability,
    /// Cooperative cancellation stopped private work.
    Cancelled,
    /// Credentials were absent or rejected.
    Authentication,
    /// A named token/item/call/spend bound was exceeded.
    ResourceExhausted,
    /// The provider deadline elapsed.
    Timeout,
    /// The configured transport failed.
    Transport,
    /// A response violated its declared contract.
    MalformedResponse,
    /// The provider rejected or rate-limited a request.
    ProviderRejected,
}

impl ProviderFailureClass {
    const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidRequest => "invalid_request",
            Self::UnsupportedCapability => "unsupported_capability",
            Self::Cancelled => "cancelled",
            Self::Authentication => "authentication",
            Self::ResourceExhausted => "resource_exhausted",
            Self::Timeout => "timeout",
            Self::Transport => "transport",
            Self::MalformedResponse => "malformed_response",
            Self::ProviderRejected => "provider_rejected",
        }
    }
}

/// Content-free provider error with no credential, payload, or response body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderError {
    class: ProviderFailureClass,
    provider: String,
    model: String,
}

impl ProviderError {
    /// Construct one redacted failure.
    #[must_use]
    pub fn new(contract: &ProviderModelContract, class: ProviderFailureClass) -> Self {
        Self {
            class,
            provider: contract.provider.clone(),
            model: contract.model.clone(),
        }
    }

    /// Stable failure class.
    #[must_use]
    pub const fn class(&self) -> ProviderFailureClass {
        self.class
    }

    /// Normalized non-secret provider token.
    #[must_use]
    pub fn provider(&self) -> &str {
        &self.provider
    }

    /// Non-secret model identifier.
    #[must_use]
    pub fn model(&self) -> &str {
        &self.model
    }
}

impl fmt::Display for ProviderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "provider invocation failed: class={} provider={} model={}",
            self.class.as_str(),
            self.provider,
            self.model
        )
    }
}

impl std::error::Error for ProviderError {}

/// Result returned across provider capability boundaries.
pub type ProviderResult<T> = Result<T, ProviderError>;

fn normalize_provider(value: &str) -> Result<String, SearchArtifactError> {
    if value.is_empty()
        || value.trim() != value
        || value.len() > MAX_IDENTITY_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(invalid("provider", "must be a bounded ASCII token"));
    }
    Ok(value.to_ascii_lowercase())
}

fn identity(field: &'static str, value: &str) -> Result<String, SearchArtifactError> {
    if value.is_empty()
        || value.trim() != value
        || value.len() > MAX_IDENTITY_BYTES
        || value.chars().any(char::is_control)
        || value.starts_with('/')
        || value.ends_with('/')
        || value.contains("//")
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'/'))
    {
        return Err(invalid(
            field,
            "must be a bounded ASCII identifier with safe path segments",
        ));
    }
    Ok(value.to_owned())
}

fn validate_tokenizer(tokenizer: &TokenizerIdentity) -> Result<(), SearchArtifactError> {
    identity("tokenizer identifier", &tokenizer.identifier)?;
    identity("tokenizer version", &tokenizer.version)?;
    identity("tokenizer normalization", &tokenizer.normalization)?;
    if tokenizer.max_input_tokens == 0 {
        return Err(invalid("tokenizer input limit", "must be non-zero"));
    }
    Ok(())
}

fn validate_chunking(
    chunking: Option<&ChunkingIdentity>,
    tokenizer: &TokenizerIdentity,
) -> Result<(), SearchArtifactError> {
    let Some(chunking) = chunking else {
        return Ok(());
    };
    if chunking.chunk_size_tokens == 0
        || chunking.chunk_size_tokens > tokenizer.max_input_tokens
        || chunking.overlap_tokens >= chunking.chunk_size_tokens
        || chunking.truncation_policy != "reject"
    {
        return Err(invalid("provider chunking", "violates tokenizer bounds"));
    }
    identity("provider chunk aggregation", &chunking.aggregation)?;
    Ok(())
}

fn invalid(field: &'static str, reason: impl Into<String>) -> SearchArtifactError {
    SearchArtifactError::InvalidSelector {
        field,
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use graphforge_storage::TokenCountClass;

    use super::*;

    fn tokenizer() -> TokenizerIdentity {
        TokenizerIdentity {
            identifier: "provider-tokenizer".into(),
            version: "1".into(),
            count_class: TokenCountClass::ProviderReported,
            max_input_tokens: 16,
            normalization: "nfc".into(),
        }
    }

    fn capabilities() -> ProviderCapabilities {
        ProviderCapabilities::new([
            ProviderCapability::DocumentEmbeddings,
            ProviderCapability::QueryEmbeddings,
        ])
        .unwrap()
    }

    fn contract() -> ProviderModelContract {
        ProviderModelContract::remote(
            None,
            "vendor/model",
            "unavailable",
            "v1",
            capabilities(),
            tokenizer(),
            None,
        )
        .unwrap()
    }

    #[test]
    fn remote_default_identity_and_capabilities_are_explicit() {
        let contract = contract();
        assert_eq!(contract.provider(), "openrouter");
        assert_eq!(contract.model(), "vendor/model");
        assert_eq!(contract.revision(), "unavailable");
        assert_eq!(contract.response_contract_version(), "v1");
        assert!(
            contract
                .capabilities()
                .supports(ProviderCapability::DocumentEmbeddings)
        );
        assert!(
            contract
                .require(ProviderCapability::CandidateReranking)
                .is_err()
        );

        let explicit = ProviderModelContract::remote(
            Some("Custom.Provider"),
            "vendor/model",
            "r1",
            "v2",
            ProviderCapabilities::new([ProviderCapability::CandidateReranking]).unwrap(),
            tokenizer(),
            None,
        )
        .unwrap();
        assert_eq!(explicit.provider(), "custom.provider");
    }

    #[test]
    fn invalid_identity_tokenizer_chunking_and_limits_fail_closed() {
        assert!(ProviderCapabilities::new([]).is_err());
        assert!(
            ProviderModelContract::remote(
                Some(" bad"),
                "vendor/model",
                "r1",
                "v1",
                capabilities(),
                tokenizer(),
                None,
            )
            .is_err()
        );
        assert!(
            ProviderModelContract::remote(
                None,
                "https://user:secret@example.com/model",
                "r1",
                "v1",
                capabilities(),
                tokenizer(),
                None,
            )
            .is_err()
        );
        let mut invalid_tokenizer = tokenizer();
        invalid_tokenizer.max_input_tokens = 0;
        assert!(
            ProviderModelContract::remote(
                None,
                "vendor/model",
                "r1",
                "v1",
                capabilities(),
                invalid_tokenizer,
                None,
            )
            .is_err()
        );
        let invalid_chunking = ChunkingIdentity {
            chunk_size_tokens: 8,
            overlap_tokens: 8,
            aggregation: "mean".into(),
            truncation_policy: "reject".into(),
        };
        assert!(
            ProviderModelContract::remote(
                None,
                "vendor/model",
                "r1",
                "v1",
                capabilities(),
                tokenizer(),
                Some(invalid_chunking),
            )
            .is_err()
        );
        for invalid in [
            ProviderRequestLimits {
                items: 0,
                ..ProviderRequestLimits::default()
            },
            ProviderRequestLimits {
                input_bytes: 0,
                ..ProviderRequestLimits::default()
            },
            ProviderRequestLimits {
                input_tokens: 0,
                ..ProviderRequestLimits::default()
            },
            ProviderRequestLimits {
                output_values: 0,
                ..ProviderRequestLimits::default()
            },
            ProviderRequestLimits {
                provider_calls: 0,
                ..ProviderRequestLimits::default()
            },
        ] {
            assert!(invalid.validate().is_err());
        }
    }

    #[test]
    fn provider_errors_are_stable_and_content_free() {
        let contract = contract();
        let error = ProviderError::new(&contract, ProviderFailureClass::Timeout);
        assert_eq!(error.class(), ProviderFailureClass::Timeout);
        assert_eq!(error.provider(), "openrouter");
        assert_eq!(error.model(), "vendor/model");
        assert_eq!(
            error.to_string(),
            "provider invocation failed: class=timeout provider=openrouter model=vendor/model"
        );
        assert!(!error.to_string().contains("secret"));
        assert!(!error.to_string().contains("payload"));
    }
}
