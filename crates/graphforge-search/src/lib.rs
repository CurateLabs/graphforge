//! Graph-native search backend primitives.
//!
//! search keeps backend mechanics in this crate while `graphforge-api` owns selector
//! resolution and Arrow result shaping. Text indexes contain UUID identity and
//! selected graph string properties only; no execution surrogate or knowledge
//! field crosses this boundary.
#![forbid(unsafe_code)]

pub mod analyzer;
pub mod embedding_lifecycle;
pub mod embedding_proactive;
pub mod embedding_query;
pub mod embedding_refresh;
pub mod embedding_scheduler;
pub mod embedding_source;
pub mod find;
pub mod fusion;
pub mod indexing;
pub mod lifecycle;
pub mod provider;
pub mod provider_adapter;
pub mod provider_batching;
pub mod provider_execution;
pub mod provider_openrouter;
pub mod provider_publication;
pub mod provider_reranking;
pub mod provider_response;
pub mod source;
pub mod text_index;
pub mod vector_lifecycle;

pub use analyzer::{
    TEXT_ANALYZER_NAME, TEXT_CONTRACT_VERSION, analyze_query, graphforge_text_analyzer,
    register_text_analyzer,
};
pub use embedding_lifecycle::{EmbeddingReadLimits, PreparedEmbeddingRead, prepare_embedding_read};
pub use embedding_proactive::{
    ProactiveEmbeddingRefreshOutcome, ProactiveEmbeddingRefreshRequest,
    drive_next_embedding_refresh,
};
pub use embedding_query::{
    EmbeddingGenerationQuery, EmbeddingVectorQuery, search_embedding_generation,
};
pub use embedding_refresh::{
    EmbeddingRefreshLimits, EmbeddingRefreshRequest, prepare_embedding_read_lazily,
    refresh_embedding_generation,
};
pub use embedding_scheduler::{
    EmbeddingRefreshCompletion, EmbeddingRefreshLease, EmbeddingRefreshScheduler,
    EmbeddingSchedulerLimits, EmbeddingSchedulerSnapshot, EmbeddingSchedulerState,
    ScheduledEmbeddingRefresh,
};
pub use embedding_source::{EmbeddingSourceCaptureLimits, capture_embedding_source};
pub use find::{FindSearchLimits, FindSearchRequest, search_graph_native};
pub use fusion::{
    FusedSearchHit, MAX_FUSION_RESULTS, MatchedOn, RRF_RANK_CONSTANT, SearchChannelHit,
    reciprocal_rank_fusion,
};
pub use indexing::{
    SearchIndexLimits, SearchIndexOutcome, SearchIndexRequest, prepare_search_index,
};
pub use lifecycle::{
    LazyTextRequest, PublishedTextIndex, TextIndexFreshnessInspection, TextIndexFreshnessReason,
    TextIndexFreshnessState, TextIndexPreparation, TextIndexRequest, TextLifecycleLimits,
    inspect_text_index_freshness, prepare_default_text_index, prepare_explicit_text_index,
    prepare_text_index, search_default_text, search_published_text,
};
pub use provider::{
    DEFAULT_REMOTE_PROVIDER, ProviderCapabilities, ProviderCapability, ProviderError,
    ProviderFailureClass, ProviderModelContract, ProviderRequestLimits, ProviderResult,
};
pub use provider_adapter::{
    CandidateReranker, DocumentEmbeddingInput, DocumentEmbeddingOutput, DocumentEmbeddingProvider,
    DocumentEmbeddingRequest, QueryEmbeddingOutput, QueryEmbeddingProvider, QueryEmbeddingRequest,
    RerankCandidate, RerankOutput, RerankRequest, embed_documents, embed_query, rerank_candidates,
};
pub use provider_batching::{
    DocumentEmbeddingBatchOptions, DocumentEmbeddingBatchPlan, ProviderBatchCostEstimator,
    ProviderBatchLimits, ProviderBatchShape, execute_document_embedding_batches,
};
pub use provider_execution::{
    ProviderCheckpoint, ProviderExecutionController, ProviderExecutionLimits,
    ProviderExecutionRuntime, ProviderExecutionSnapshot, ProviderWorkEstimate,
    StandardProviderExecutionRuntime,
};
pub use provider_openrouter::{
    OPENROUTER_EMBEDDINGS_PATH, OPENROUTER_RERANK_PATH, OpenRouterAdapter, OpenRouterEndpoint,
    OpenRouterHttpTransport, OpenRouterOwnedAdapter, OpenRouterTransport, OpenRouterTransportError,
    OpenRouterTransportRequest, OpenRouterTransportResponse, OpenRouterWireLimits,
};
pub use provider_publication::{
    ProviderEmbeddingPublicationRequest, ProviderPublicationError,
    publish_provider_embedding_generation,
};
pub use provider_reranking::{
    CANONICAL_UNRERANKED_POLICY, RERANK_SCORE_POLICY, RerankAdvisoryPolicy, RerankApplication,
    RerankAuditIdentity, RerankCostEstimator, RerankFailurePolicy, RerankOmissionAdvisory,
    RerankStatus, RerankWorkShape, apply_reranking, omit_reranking,
};
pub use provider_response::{
    ValidatedDocumentEmbeddings, ValidatedQueryEmbedding, ValidatedRerankResponse,
    ValidatedRerankRow, validate_document_embedding_response, validate_query_embedding_response,
    validate_rerank_response,
};
pub use source::{TextDocument, TextSourceProjection, project_text_source};
pub use text_index::{
    TEXT_BACKEND_VERSION, TextIndexBuildOutcome, TextSearchHit, build_text_index,
    search_text_index, validate_text_index,
};
pub use vector_lifecycle::{
    VectorIndexRequest, VectorLifecycleLimits, project_label_members, search_graph_vectors,
    upsert_graph_vector,
};

/// Named resource bounds shared by text projection, indexing, and search.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TextSearchLimits {
    /// Maximum UTF-8 bytes accepted in one plain-text query.
    pub query_bytes: usize,
    /// Maximum analyzed tokens, including repeated tokens.
    pub query_tokens: usize,
    /// Maximum UTF-8 bytes in one property selector.
    pub selector_bytes: usize,
    /// Maximum selected string-property fields.
    pub selected_properties: usize,
    /// Maximum topology rows inspected for label membership.
    pub topology_rows: usize,
    /// Maximum property rows inspected across all Parquet stems.
    pub property_rows: usize,
    /// Maximum documents written to or opened from one index.
    pub documents: usize,
    /// Maximum committed graph-source bytes read during projection.
    pub source_bytes: u64,
    /// Tantivy memory budget for the deterministic single indexing worker.
    pub writer_memory_bytes: usize,
    /// Maximum bytes in one completed Tantivy index directory.
    pub index_bytes: u64,
    /// Maximum `documents * selected_properties` indexing work.
    pub build_work: usize,
    /// Maximum requested result count.
    pub results: usize,
    /// Maximum `documents * query_tokens * selected_properties` search work.
    pub search_work: usize,
}

impl Default for TextSearchLimits {
    fn default() -> Self {
        Self {
            query_bytes: 16 * 1024,
            query_tokens: 256,
            selector_bytes: 128,
            selected_properties: 64,
            topology_rows: 1_000_000,
            property_rows: 5_000_000,
            documents: 1_000_000,
            source_bytes: 4 * 1024 * 1024 * 1024,
            writer_memory_bytes: 64 * 1024 * 1024,
            index_bytes: 4 * 1024 * 1024 * 1024,
            build_work: 64_000_000,
            results: 10_000,
            search_work: 100_000_000,
        }
    }
}
