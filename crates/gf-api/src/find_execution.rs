//! Canonical find composition with explicit reranking and schema-neutral diagnostics.

use arrow::record_batch::RecordBatch;
use gf_search::{
    ProviderExecutionLimits, ProviderModelContract, ProviderRequestLimits, RerankAdvisoryPolicy,
    RerankFailurePolicy, RerankStatus,
};

use super::provider_find::provider_gf_error;
use super::{
    EmbeddingSpaceReadDecision, FindOptions, GfError, GraphForge, ProviderRerankError,
    ProviderRerankExecution, ProviderRerankRequest,
};

const MAX_RERANK_CANDIDATES: usize = 10_000;

/// Explicit bounded rerank requested after canonical retrieval.
#[derive(Clone, Debug)]
pub struct FindRerankOptions {
    /// Explicit rerank query sent to the configured provider.
    pub query: String,
    /// Sorted unique graph properties projected for candidates.
    pub properties: Vec<String>,
    /// Maximum canonical candidates retrieved before reranking.
    pub candidate_depth: usize,
    /// Exact provider/model/tokenizer/chunking contract.
    pub contract: ProviderModelContract,
    /// Exact per-provider invocation bounds.
    pub request_limits: ProviderRequestLimits,
    /// Retry, deadline, exposure, rate, and spend bounds.
    pub execution_limits: ProviderExecutionLimits,
    /// Named terminal provider-failure behavior.
    pub failure_policy: RerankFailurePolicy,
}

/// Canonical find options plus optional reranking and advisory policy.
#[derive(Clone, Debug)]
pub struct FindExecutionOptions {
    /// Existing canonical retrieval contract.
    pub find: FindOptions,
    /// Explicit rerank request, or `None` for canonical retrieval.
    pub rerank: Option<FindRerankOptions>,
    /// Sole compatible configured reranker when reranking is omitted.
    pub omitted_reranker: Option<ProviderModelContract>,
    /// Whether an omitted compatible reranker emits an advisory.
    pub advisory_policy: RerankAdvisoryPolicy,
}

impl Default for FindExecutionOptions {
    fn default() -> Self {
        Self {
            find: FindOptions::default(),
            rerank: None,
            omitted_reranker: None,
            advisory_policy: RerankAdvisoryPolicy::Emit,
        }
    }
}

/// Payload-free diagnostic emitted outside canonical Arrow rows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FindDiagnostic {
    /// The last complete substantially stale generation was explicitly served.
    ForcedStale {
        /// Stable Rust-owned diagnostic string.
        diagnostic: String,
    },
    /// One configured compatible reranker was deliberately omitted.
    RerankSuggested {
        /// Normalized non-secret provider identifier.
        provider: String,
        /// Exact non-secret model identifier.
        model: String,
    },
}

/// Canonical Arrow output plus non-conditional diagnostics and rerank status.
#[derive(Debug)]
pub struct FindExecutionResult {
    batch: RecordBatch,
    diagnostics: Vec<FindDiagnostic>,
    rerank_status: Option<RerankStatus>,
}

impl FindExecutionResult {
    /// Consume the result while keeping metadata separate from Arrow.
    #[must_use]
    pub fn into_parts(self) -> (RecordBatch, Vec<FindDiagnostic>, Option<RerankStatus>) {
        (self.batch, self.diagnostics, self.rerank_status)
    }
}

impl GraphForge {
    /// Execute canonical retrieval with optional explicit reranking and diagnostics.
    /// # Errors
    /// Returns structured failures without partial rows.
    pub fn find_with_diagnostics(
        &self,
        options: FindExecutionOptions,
        execution: Option<ProviderRerankExecution<'_>>,
    ) -> Result<FindExecutionResult, GfError> {
        let FindExecutionOptions {
            find,
            rerank,
            omitted_reranker,
            advisory_policy,
        } = options;
        if rerank.is_some() != execution.is_some() {
            return Err(validation(
                "rerank options and provider execution must be supplied together",
            ));
        }
        if rerank.is_some() && omitted_reranker.is_some() {
            return Err(validation(
                "explicit rerank cannot also declare an omitted reranker",
            ));
        }
        let mut retrieval = find.clone();
        if let Some(request) = &rerank {
            if find.limit == 0
                || !(find.limit..=MAX_RERANK_CANDIDATES).contains(&request.candidate_depth)
            {
                return Err(validation(
                    "rerank requires 1 <= find limit <= candidate_depth <= 10000",
                ));
            }
            retrieval.limit = request.candidate_depth;
        }
        let canonical = self.find(retrieval)?;
        let mut diagnostics = self.forced_stale_diagnostics(&find)?;
        match (rerank, execution) {
            (None, None) => {
                if let Some(contract) = omitted_reranker {
                    contract
                        .require(gf_search::ProviderCapability::CandidateReranking)
                        .map_err(|error| provider_gf_error(&error))?;
                    if advisory_policy == RerankAdvisoryPolicy::Emit {
                        diagnostics.push(FindDiagnostic::RerankSuggested {
                            provider: contract.provider().to_owned(),
                            model: contract.model().to_owned(),
                        });
                    }
                }
                Ok(FindExecutionResult {
                    batch: canonical,
                    diagnostics,
                    rerank_status: None,
                })
            }
            (Some(request), Some(execution)) => {
                let rerank_request = ProviderRerankRequest {
                    label: find.label.expect("canonical retrieval validated label"),
                    query: request.query,
                    properties: request.properties,
                    candidate_depth: request.candidate_depth,
                    limit: find.limit,
                    contract: request.contract,
                    request_limits: request.request_limits,
                    execution_limits: request.execution_limits,
                    failure_policy: request.failure_policy,
                };
                let reranked = self
                    .rerank_find_results(&canonical, &rerank_request, execution)
                    .map_err(rerank_error)?;
                let (batch, status) = reranked.into_parts();
                Ok(FindExecutionResult {
                    batch,
                    diagnostics,
                    rerank_status: Some(status),
                })
            }
            _ => unreachable!("rerank options and execution pairing validated above"),
        }
    }

    fn forced_stale_diagnostics(&self, find: &FindOptions) -> Result<Vec<FindDiagnostic>, GfError> {
        if !find.force_stale {
            return Ok(Vec::new());
        }
        let inspection = self.inspect_embedding_space_freshness(find.space.as_deref(), true)?;
        match inspection.decision {
            EmbeddingSpaceReadDecision::ServeForcedStale { diagnostic } => {
                Ok(vec![FindDiagnostic::ForcedStale { diagnostic }])
            }
            _ => Ok(Vec::new()),
        }
    }
}

fn rerank_error(error: ProviderRerankError) -> GfError {
    match error {
        ProviderRerankError::Api(error) => error,
        ProviderRerankError::Artifact(error) => error.into(),
        ProviderRerankError::Provider(error) => provider_gf_error(&error),
    }
}

fn validation(message: impl Into<String>) -> GfError {
    GfError::Validation(message.into())
}
