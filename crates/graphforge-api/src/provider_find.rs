//! Explicit provider-compatible semantic query execution for `find`.

use std::error::Error;
use std::fmt;
use std::sync::{Arc, Mutex};

use arrow::record_batch::RecordBatch;
use graphforge_search::{
    ProviderError, ProviderExecutionController, ProviderExecutionLimits, ProviderExecutionRuntime,
    ProviderFailureClass, ProviderModelContract, ProviderRequestLimits, ProviderResult,
    ProviderWorkEstimate, QueryEmbeddingProvider, QueryEmbeddingRequest, embed_query,
    validate_query_embedding_response,
};
use graphforge_storage::{
    EmbeddingNormalization, EmbeddingProducerIdentity, EmbeddingReadDecision, SearchArtifactError,
    VectorStoreLimits,
};

use super::{FindOptions, GfError, GraphForge, ProviderArtifactCheckpoint, ProviderTokenCounter};

/// Payload-free provider-query counts passed to the caller's cost estimator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProviderQueryWorkShape {
    input_bytes: usize,
    input_tokens: u64,
}

impl ProviderQueryWorkShape {
    /// Outbound UTF-8 bytes without their contents.
    #[must_use]
    pub const fn input_bytes(self) -> usize {
        self.input_bytes
    }

    /// Counted tokens under the exact persisted tokenizer contract.
    #[must_use]
    pub const fn input_tokens(self) -> u64 {
        self.input_tokens
    }
}

/// Provider-specific query pricing boundary that receives counts only.
pub type ProviderQueryCostEstimator<'a> =
    dyn FnMut(ProviderQueryWorkShape) -> ProviderResult<u64> + 'a;

const MAX_CONFIGURED_QUERY_PROVIDERS: usize = 16;
type OwnedProviderTokenCounter =
    Box<dyn FnMut(&ProviderModelContract, &str) -> ProviderResult<u64> + Send>;
type OwnedProviderQueryCostEstimator =
    Box<dyn FnMut(ProviderQueryWorkShape) -> ProviderResult<u64> + Send>;
type OwnedProviderCheckpoint = Box<dyn FnMut() -> Result<(), SearchArtifactError> + Send>;

/// One process-local query provider and its bounded execution dependencies.
///
/// The owned callbacks deliberately omit `Debug`; credentials, transports,
/// query text, generated vectors, and provider payloads remain runtime-only.
pub struct ConfiguredProviderFindRuntime {
    provider: Box<dyn QueryEmbeddingProvider + Send>,
    runtime: Box<dyn ProviderExecutionRuntime + Send>,
    count_tokens: OwnedProviderTokenCounter,
    estimate_cost: OwnedProviderQueryCostEstimator,
    checkpoint: OwnedProviderCheckpoint,
    request_limits: ProviderRequestLimits,
    execution_limits: ProviderExecutionLimits,
}

impl ConfiguredProviderFindRuntime {
    /// Own one exact provider runtime for later ordinary `find` calls.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new<P, R, C, E, K>(
        provider: P,
        runtime: R,
        count_tokens: C,
        estimate_cost: E,
        checkpoint: K,
        request_limits: ProviderRequestLimits,
        execution_limits: ProviderExecutionLimits,
    ) -> Self
    where
        P: QueryEmbeddingProvider + Send + 'static,
        R: ProviderExecutionRuntime + Send + 'static,
        C: FnMut(&ProviderModelContract, &str) -> ProviderResult<u64> + Send + 'static,
        E: FnMut(ProviderQueryWorkShape) -> ProviderResult<u64> + Send + 'static,
        K: FnMut() -> Result<(), SearchArtifactError> + Send + 'static,
    {
        Self {
            provider: Box::new(provider),
            runtime: Box::new(runtime),
            count_tokens: Box::new(count_tokens),
            estimate_cost: Box::new(estimate_cost),
            checkpoint: Box::new(checkpoint),
            request_limits,
            execution_limits,
        }
    }

    fn contract(&self) -> &ProviderModelContract {
        self.provider.contract()
    }

    fn execution(&mut self) -> ProviderFindExecution<'_> {
        ProviderFindExecution::new(
            self.provider.as_mut(),
            self.runtime.as_mut(),
            self.count_tokens.as_mut(),
            self.estimate_cost.as_mut(),
            self.checkpoint.as_mut(),
            self.request_limits,
            self.execution_limits,
        )
    }
}

/// Borrowed runtime dependencies for one explicit semantic-query `find`.
///
/// This bundle deliberately omits `Debug`: providers, callbacks, credentials,
/// transports, query text, and generated vectors stay runtime-only.
pub struct ProviderFindExecution<'a> {
    provider: &'a mut dyn QueryEmbeddingProvider,
    runtime: &'a mut dyn ProviderExecutionRuntime,
    count_tokens: &'a mut ProviderTokenCounter<'a>,
    estimate_cost: &'a mut ProviderQueryCostEstimator<'a>,
    checkpoint: &'a mut ProviderArtifactCheckpoint<'a>,
    request_limits: ProviderRequestLimits,
    execution_limits: ProviderExecutionLimits,
}

impl<'a> ProviderFindExecution<'a> {
    /// Assemble explicit provider execution dependencies and named limits.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        provider: &'a mut dyn QueryEmbeddingProvider,
        runtime: &'a mut dyn ProviderExecutionRuntime,
        count_tokens: &'a mut ProviderTokenCounter<'a>,
        estimate_cost: &'a mut ProviderQueryCostEstimator<'a>,
        checkpoint: &'a mut ProviderArtifactCheckpoint<'a>,
        request_limits: ProviderRequestLimits,
        execution_limits: ProviderExecutionLimits,
    ) -> Self {
        Self {
            provider,
            runtime,
            count_tokens,
            estimate_cost,
            checkpoint,
            request_limits,
            execution_limits,
        }
    }
}

struct ProviderQueryRunner<'a> {
    contract: ProviderModelContract,
    provider: &'a mut dyn QueryEmbeddingProvider,
    runtime: &'a mut dyn ProviderExecutionRuntime,
    count_tokens: &'a mut ProviderTokenCounter<'a>,
    estimate_cost: &'a mut ProviderQueryCostEstimator<'a>,
    checkpoint: &'a mut ProviderArtifactCheckpoint<'a>,
    request_limits: ProviderRequestLimits,
    controller: ProviderExecutionController,
}

impl<'a> ProviderQueryRunner<'a> {
    fn new(execution: ProviderFindExecution<'a>) -> ProviderResult<Self> {
        let ProviderFindExecution {
            provider,
            runtime,
            count_tokens,
            estimate_cost,
            checkpoint,
            request_limits,
            execution_limits,
        } = execution;
        let contract = provider.contract().clone();
        let controller = ProviderExecutionController::new(&contract, execution_limits, runtime)?;
        Ok(Self {
            contract,
            provider,
            runtime,
            count_tokens,
            estimate_cost,
            checkpoint,
            request_limits,
            controller,
        })
    }

    fn embed(
        &mut self,
        query: &str,
        dimensions: u32,
        normalization: EmbeddingNormalization,
    ) -> Result<Vec<f32>, ProviderFindError> {
        let token_count = (self.count_tokens)(&self.contract, query)?;
        let request =
            QueryEmbeddingRequest::new(&self.contract, query, token_count, self.request_limits)?;
        let shape = ProviderQueryWorkShape {
            input_bytes: query.len(),
            input_tokens: token_count,
        };
        let estimated_cost = (self.estimate_cost)(shape)?;
        let work = ProviderWorkEstimate::new(
            &self.contract,
            1,
            shape.input_bytes,
            shape.input_tokens,
            estimated_cost,
        )?;
        let dimension =
            usize::try_from(dimensions).map_err(|_| SearchArtifactError::InvalidSelector {
                field: "embedding dimension",
                reason: "cannot be represented on this platform".to_owned(),
            })?;
        with_artifact_checkpoint(&self.contract, self.checkpoint, |guarded| {
            self.controller
                .execute(work, self.runtime, guarded, &mut |attempt_checkpoint| {
                    let output = embed_query(self.provider, &request, attempt_checkpoint)?;
                    validate_query_embedding_response(
                        &request,
                        output,
                        dimension,
                        normalization,
                        VectorStoreLimits::default(),
                        attempt_checkpoint,
                    )
                    .map(graphforge_search::ValidatedQueryEmbedding::into_vector)
                })
        })
    }
}

/// Structured facade, artifact, or redacted provider-query failure.
#[derive(Debug)]
pub enum ProviderFindError {
    /// Public option, label, alias, freshness, retrieval, or Arrow failure.
    Api(GfError),
    /// Search lifecycle, cancellation, resource, or concurrent-mutation failure.
    Artifact(SearchArtifactError),
    /// Redacted provider/model/tokenizer/capability/execution failure.
    Provider(ProviderError),
}

impl fmt::Display for ProviderFindError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Api(error) => error.fmt(formatter),
            Self::Artifact(error) => error.fmt(formatter),
            Self::Provider(error) => error.fmt(formatter),
        }
    }
}

impl Error for ProviderFindError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Api(error) => Some(error),
            Self::Artifact(error) => Some(error),
            Self::Provider(error) => Some(error),
        }
    }
}

impl From<GfError> for ProviderFindError {
    fn from(error: GfError) -> Self {
        Self::Api(error)
    }
}

impl From<SearchArtifactError> for ProviderFindError {
    fn from(error: SearchArtifactError) -> Self {
        Self::Artifact(error)
    }
}

impl From<ProviderError> for ProviderFindError {
    fn from(error: ProviderError) -> Self {
        Self::Provider(error)
    }
}

impl From<ProviderFindError> for GfError {
    fn from(error: ProviderFindError) -> Self {
        match error {
            ProviderFindError::Api(error) => error,
            ProviderFindError::Artifact(error) => error.into(),
            ProviderFindError::Provider(error) => provider_gf_error(&error),
        }
    }
}

impl GraphForge {
    /// Register or replace one exact process-local semantic-query runtime.
    ///
    /// # Errors
    /// Rejects providers without query-embedding capability or a full registry.
    pub fn configure_provider_find_runtime(
        &self,
        configured: ConfiguredProviderFindRuntime,
    ) -> Result<(), GfError> {
        configured
            .contract()
            .require(graphforge_search::ProviderCapability::QueryEmbeddings)
            .map_err(|error| provider_gf_error(&error))?;
        let mut runtimes = self
            .provider_find_runtimes
            .lock()
            .map_err(|_| provider_registry_unavailable())?;
        for existing in runtimes.iter() {
            let mut existing = existing
                .lock()
                .map_err(|_| provider_registry_unavailable())?;
            if same_runtime_identity(existing.contract(), configured.contract()) {
                *existing = configured;
                return Ok(());
            }
        }
        if runtimes.len() == MAX_CONFIGURED_QUERY_PROVIDERS {
            return Err(GfError::Validation(
                "configured query provider limit exceeded".to_owned(),
            ));
        }
        runtimes.push(Arc::new(Mutex::new(configured)));
        Ok(())
    }

    pub(crate) fn find_with_configured_provider(
        &self,
        options: FindOptions,
    ) -> Result<RecordBatch, GfError> {
        validate_options(&options).map_err(GfError::from)?;
        let space = options.space.as_deref().expect("validated provider space");
        let prepared = self.prepare_embedding_space_read(Some(space), options.force_stale)?;
        let descriptor = &prepared.publication().descriptor;
        let configured = {
            let runtimes = self
                .provider_find_runtimes
                .lock()
                .map_err(|_| provider_registry_unavailable())?;
            let mut selected = None;
            for configured in runtimes.iter() {
                let contract_matches = {
                    let configured = configured
                        .lock()
                        .map_err(|_| provider_registry_unavailable())?;
                    validate_contract(
                        descriptor.producer(),
                        descriptor.tokenizer(),
                        descriptor.chunking(),
                        configured.contract(),
                    )
                    .is_ok()
                };
                if contract_matches {
                    selected = Some(Arc::clone(configured));
                    break;
                }
            }
            selected
        }
        .ok_or_else(|| {
            GfError::Validation(
                "no configured query provider matches the selected embedding space".to_owned(),
            )
        })?;
        let mut configured = configured
            .lock()
            .map_err(|_| provider_registry_unavailable())?;
        self.find_with_provider(options, configured.execution())
            .map_err(Into::into)
    }

    /// Embed one explicit semantic query under the selected space contract and search it.
    ///
    /// The ordinary [`GraphForge::find`] path remains offline and dependency-free. This
    /// statically distinct entry point requires caller-injected provider/runtime dependencies,
    /// validates them against the persisted space identity before outbound work, and discards
    /// any result if the selected alias changes during provider or retrieval work.
    ///
    /// # Errors
    /// Returns structured option, identity, tokenizer, capability, cost, cancellation,
    /// provider-response, freshness, concurrency, retrieval, or Arrow failures. No query text,
    /// generated vector, credential, or provider payload is persisted or included in errors.
    pub fn find_with_provider(
        &self,
        options: FindOptions,
        execution: ProviderFindExecution<'_>,
    ) -> Result<RecordBatch, ProviderFindError> {
        validate_options(&options)?;
        let FindOptions {
            query,
            label,
            vector: _,
            similar_to: _,
            semantic_query,
            limit,
            space,
            force_stale,
        } = options;
        let semantic_query = semantic_query.expect("validated semantic query");
        let space = space.expect("validated space");
        let mut runner = ProviderQueryRunner::new(execution)?;

        for attempt in 1_u8..=2 {
            let prepared = self.prepare_embedding_space_read(Some(&space), force_stale)?;
            validate_read_decision(&prepared)?;
            let descriptor = &prepared.publication().descriptor;
            validate_contract(
                descriptor.producer(),
                descriptor.tokenizer(),
                descriptor.chunking(),
                &runner.contract,
            )?;
            let compatibility_id = prepared.publication().manifest.compatibility_id();
            let vector = runner.embed(
                &semantic_query,
                descriptor.dimensions(),
                descriptor.normalization(),
            )?;

            if self
                .prepare_embedding_space_read(Some(&space), force_stale)?
                .publication()
                .manifest
                .compatibility_id()
                != compatibility_id
            {
                if attempt == 2 {
                    return Err(SearchArtifactError::ConcurrentMutation.into());
                }
                continue;
            }

            let raw_options = FindOptions {
                query: query.clone(),
                label: label.clone(),
                vector: Some(vector),
                similar_to: None,
                semantic_query: None,
                limit,
                space: Some(space.clone()),
                force_stale,
            };
            let batch = self.find(raw_options)?;
            if self
                .prepare_embedding_space_read(Some(&space), force_stale)?
                .publication()
                .manifest
                .compatibility_id()
                == compatibility_id
            {
                return Ok(batch);
            }
            if attempt == 2 {
                return Err(SearchArtifactError::ConcurrentMutation.into());
            }
        }
        unreachable!("bounded provider find retry returns on both terminal paths")
    }
}

fn same_runtime_identity(left: &ProviderModelContract, right: &ProviderModelContract) -> bool {
    left.provider() == right.provider()
        && left.model() == right.model()
        && left.revision() == right.revision()
        && left.response_contract_version() == right.response_contract_version()
        && left.tokenizer() == right.tokenizer()
        && left.chunking() == right.chunking()
}

fn provider_registry_unavailable() -> GfError {
    GfError::Execution("configured query provider runtime unavailable".to_owned())
}

pub(crate) fn provider_gf_error(error: &ProviderError) -> GfError {
    let class = match error.class() {
        ProviderFailureClass::InvalidRequest => "invalid_request",
        ProviderFailureClass::UnsupportedCapability => "unsupported_capability",
        ProviderFailureClass::Cancelled => "cancelled",
        ProviderFailureClass::Authentication => "authentication",
        ProviderFailureClass::ResourceExhausted => "resource_exhausted",
        ProviderFailureClass::Timeout => "timeout",
        ProviderFailureClass::Transport => "transport",
        ProviderFailureClass::MalformedResponse => "malformed_response",
        ProviderFailureClass::ProviderRejected => "provider_rejected",
    };
    GfError::Provider {
        class: class.to_owned(),
        provider: error.provider().to_owned(),
        model: error.model().to_owned(),
    }
}

fn validate_options(options: &FindOptions) -> Result<(), ProviderFindError> {
    if options.label.is_none() {
        return Err(GfError::Validation("find requires label".to_owned()).into());
    }
    if options.space.is_none() {
        return Err(GfError::Validation("semantic_query requires space".to_owned()).into());
    }
    if options.vector.is_some() || options.similar_to.is_some() {
        return Err(GfError::Validation(
            "semantic_query is mutually exclusive with vector and similar_to".to_owned(),
        )
        .into());
    }
    match options.semantic_query.as_deref() {
        Some(query) if !query.is_empty() => {}
        _ => {
            return Err(GfError::Validation(
                "find_with_provider requires semantic_query".to_owned(),
            )
            .into());
        }
    }
    if !(1..=10_000).contains(&options.limit) {
        return Err(
            GfError::Validation("find limit must be between 1 and 10000".to_owned()).into(),
        );
    }
    Ok(())
}

fn validate_read_decision(
    prepared: &graphforge_search::PreparedEmbeddingRead,
) -> Result<(), SearchArtifactError> {
    match prepared.decision() {
        EmbeddingReadDecision::ServeFresh
        | EmbeddingReadDecision::ServeStale { .. }
        | EmbeddingReadDecision::ServeForcedStale { .. } => Ok(()),
        EmbeddingReadDecision::RefreshRequired { reason } => Err(SearchArtifactError::Stale {
            reason: format!(
                "embedding space is substantially stale: {}",
                reason.as_str()
            ),
        }),
    }
}

fn validate_contract(
    producer: &EmbeddingProducerIdentity,
    tokenizer: Option<&graphforge_storage::TokenizerIdentity>,
    chunking: Option<&graphforge_storage::ChunkingIdentity>,
    contract: &ProviderModelContract,
) -> Result<(), ProviderFindError> {
    let EmbeddingProducerIdentity::Remote {
        provider,
        model,
        revision,
        response_contract_version,
    } = producer
    else {
        return Err(GfError::Validation(
            "selected embedding space cannot embed arbitrary text".to_owned(),
        )
        .into());
    };
    contract.require(graphforge_search::ProviderCapability::QueryEmbeddings)?;
    if provider != contract.provider()
        || model != contract.model()
        || revision != contract.revision()
        || response_contract_version != contract.response_contract_version()
        || tokenizer != Some(contract.tokenizer())
        || chunking != contract.chunking()
    {
        return Err(ProviderError::new(contract, ProviderFailureClass::InvalidRequest).into());
    }
    Ok(())
}

fn with_artifact_checkpoint<T, F>(
    contract: &ProviderModelContract,
    checkpoint: &mut ProviderArtifactCheckpoint<'_>,
    operation: F,
) -> Result<T, ProviderFindError>
where
    F: FnOnce(&mut dyn FnMut() -> ProviderResult<()>) -> ProviderResult<T>,
{
    let mut artifact_failure = None;
    let result = operation(&mut || match checkpoint() {
        Ok(()) => Ok(()),
        Err(error) => {
            artifact_failure = Some(error);
            Err(ProviderError::new(
                contract,
                ProviderFailureClass::Cancelled,
            ))
        }
    });
    if let Some(error) = artifact_failure {
        return Err(error.into());
    }
    result.map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::time::Duration;

    use arrow::array::{FixedSizeBinaryArray, StringArray};
    use graphforge_search::{
        DocumentEmbeddingOutput, DocumentEmbeddingProvider, DocumentEmbeddingRequest,
        ProviderCapabilities, ProviderCapability, QueryEmbeddingOutput,
        StandardProviderExecutionRuntime,
    };
    use graphforge_storage::{TokenCountClass, TokenizerIdentity};

    use super::*;
    use crate::{
        CallerEmbeddingBatchRequest, CallerEmbeddingBatchRow, CallerEmbeddingDistance,
        CallerEmbeddingNormalization, NodeHandle, NodeSelector, PropValue,
        ProviderEmbeddingDistance, ProviderEmbeddingExecution, ProviderEmbeddingNormalization,
        ProviderEmbeddingPlanRequest,
    };

    struct FakeProvider {
        contract: ProviderModelContract,
        query_vector: Vec<f32>,
        document_calls: usize,
        query_calls: usize,
    }

    impl DocumentEmbeddingProvider for FakeProvider {
        fn contract(&self) -> &ProviderModelContract {
            &self.contract
        }

        fn provide_documents(
            &mut self,
            request: &DocumentEmbeddingRequest<'_>,
            checkpoint: &mut dyn FnMut() -> ProviderResult<()>,
        ) -> ProviderResult<Vec<DocumentEmbeddingOutput>> {
            checkpoint()?;
            self.document_calls += 1;
            Ok(request
                .inputs()
                .iter()
                .map(|input| DocumentEmbeddingOutput {
                    node_uuid: input.node_uuid,
                    vector: if input.text.contains("alpha") {
                        vec![1.0, 0.0]
                    } else {
                        vec![0.0, 1.0]
                    },
                })
                .collect())
        }
    }

    impl QueryEmbeddingProvider for FakeProvider {
        fn contract(&self) -> &ProviderModelContract {
            &self.contract
        }

        fn provide_query(
            &mut self,
            _: &QueryEmbeddingRequest<'_>,
            checkpoint: &mut dyn FnMut() -> ProviderResult<()>,
        ) -> ProviderResult<QueryEmbeddingOutput> {
            checkpoint()?;
            self.query_calls += 1;
            Ok(QueryEmbeddingOutput {
                vector: self.query_vector.clone(),
            })
        }
    }

    fn contract(model: &str) -> ProviderModelContract {
        ProviderModelContract::remote(
            None,
            model,
            "revision-1",
            "wire-v1",
            ProviderCapabilities::new([
                ProviderCapability::DocumentEmbeddings,
                ProviderCapability::QueryEmbeddings,
            ])
            .unwrap(),
            TokenizerIdentity {
                identifier: "test-tokenizer".to_owned(),
                version: "v1".to_owned(),
                count_class: TokenCountClass::ExactLocal,
                max_input_tokens: 1_024,
                normalization: "nfc".to_owned(),
            },
            None,
        )
        .unwrap()
    }

    #[test]
    fn provider_find_error_preserves_display_source_and_conversion_domains() {
        let api = ProviderFindError::from(GfError::Validation("bad query".into()));
        assert_eq!(api.to_string(), "validation error: bad query");
        assert!(api.source().is_some());
        assert_eq!(GfError::from(api).code(), "GF_VALIDATION");

        let artifact = ProviderFindError::from(SearchArtifactError::Cancelled);
        assert_eq!(artifact.to_string(), "search operation cancelled");
        assert!(artifact.source().is_some());
        assert_eq!(GfError::from(artifact).code(), "GF_EXECUTION");

        let provider = ProviderFindError::from(ProviderError::new(
            &contract("error-model"),
            graphforge_search::ProviderFailureClass::Timeout,
        ));
        assert!(provider.to_string().contains("error-model"));
        assert!(provider.source().is_some());
        assert_eq!(GfError::from(provider).code(), "GF_EXECUTION");
    }

    #[test]
    fn provider_failure_mapping_is_closed_and_payload_free() {
        let expected = [
            (ProviderFailureClass::InvalidRequest, "invalid_request"),
            (
                ProviderFailureClass::UnsupportedCapability,
                "unsupported_capability",
            ),
            (ProviderFailureClass::Cancelled, "cancelled"),
            (ProviderFailureClass::Authentication, "authentication"),
            (
                ProviderFailureClass::ResourceExhausted,
                "resource_exhausted",
            ),
            (ProviderFailureClass::Timeout, "timeout"),
            (ProviderFailureClass::Transport, "transport"),
            (
                ProviderFailureClass::MalformedResponse,
                "malformed_response",
            ),
            (ProviderFailureClass::ProviderRejected, "provider_rejected"),
        ];
        let contract = contract("safe-model");
        for (failure, class) in expected {
            let mapped = provider_gf_error(&ProviderError::new(&contract, failure));
            assert!(matches!(
                mapped,
                GfError::Provider {
                    class: ref actual,
                    provider: ref actual_provider,
                    model: ref actual_model,
                } if actual == class
                    && actual_provider == "openrouter"
                    && actual_model == "safe-model"
            ));
        }
    }

    #[test]
    fn provider_find_options_reject_each_ambiguous_or_unbounded_shape() {
        let mut cases = Vec::new();
        let mut missing_label = options();
        missing_label.label = None;
        cases.push(missing_label);
        let mut missing_space = options();
        missing_space.space = None;
        cases.push(missing_space);
        let mut vector = options();
        vector.vector = Some(vec![1.0, 0.0]);
        cases.push(vector);
        let mut similar = options();
        similar.similar_to = Some(NodeSelector::Uuid(uuid::Uuid::nil()));
        cases.push(similar);
        let mut missing_query = options();
        missing_query.semantic_query = None;
        cases.push(missing_query);
        let mut empty_query = options();
        empty_query.semantic_query = Some(String::new());
        cases.push(empty_query);
        let mut zero_limit = options();
        zero_limit.limit = 0;
        cases.push(zero_limit);
        let mut excessive_limit = options();
        excessive_limit.limit = 10_001;
        cases.push(excessive_limit);

        for invalid in cases {
            assert!(matches!(
                validate_options(&invalid),
                Err(ProviderFindError::Api(GfError::Validation(_)))
            ));
        }
        let valid = options();
        validate_options(&valid).unwrap();
        let shape = ProviderQueryWorkShape {
            input_bytes: 11,
            input_tokens: 3,
        };
        assert_eq!(shape.input_bytes(), 11);
        assert_eq!(shape.input_tokens(), 3);
    }

    fn provider(model: &str, query_vector: Vec<f32>) -> FakeProvider {
        FakeProvider {
            contract: contract(model),
            query_vector,
            document_calls: 0,
            query_calls: 0,
        }
    }

    fn node(graph: &GraphForge, body: &str) -> NodeHandle {
        graph
            .add_node(
                "Document",
                &HashMap::from([("body".to_owned(), PropValue::Str(body.to_owned()))]),
            )
            .unwrap()
    }

    fn provider_request(
        display_name: &str,
        property: &str,
        contract: ProviderModelContract,
    ) -> ProviderEmbeddingPlanRequest {
        ProviderEmbeddingPlanRequest {
            display_name: display_name.to_owned(),
            label: "Document".to_owned(),
            properties: vec![property.to_owned()],
            contract,
            dimensions: 2,
            normalization: ProviderEmbeddingNormalization::L2,
            distance: ProviderEmbeddingDistance::Cosine,
            request_limits: ProviderRequestLimits::default(),
            batch_limits: graphforge_search::ProviderBatchLimits::default(),
            execution_limits: execution_limits(),
            replace_alias: false,
        }
    }

    fn execution_limits() -> ProviderExecutionLimits {
        ProviderExecutionLimits {
            retries: 0,
            timeout: Duration::from_secs(5),
            ..ProviderExecutionLimits::default()
        }
    }

    fn publish_provider_space_as(
        graph: &GraphForge,
        provider: &mut FakeProvider,
        display_name: &str,
        property: &str,
    ) -> crate::EmbeddingSpaceInfo {
        let request = provider_request(display_name, property, provider.contract.clone());
        let mut runtime = StandardProviderExecutionRuntime::new();
        let mut count_tokens =
            |_: &ProviderModelContract, text: &str| Ok(u64::try_from(text.len()).unwrap());
        let mut estimate_cost = |_: graphforge_search::ProviderBatchShape| Ok(0);
        let mut checkpoint = || Ok(());
        graph
            .publish_provider_embeddings(
                &request,
                ProviderEmbeddingExecution::new(
                    provider,
                    &mut runtime,
                    &mut count_tokens,
                    &mut estimate_cost,
                    &mut checkpoint,
                ),
            )
            .unwrap()
    }

    fn publish_provider_space(graph: &GraphForge, provider: &mut FakeProvider) {
        publish_provider_space_as(graph, provider, "semantic", "body");
    }

    fn options() -> FindOptions {
        FindOptions {
            label: Some("Document".to_owned()),
            semantic_query: Some("alpha meaning".to_owned()),
            space: Some("semantic".to_owned()),
            limit: 2,
            ..FindOptions::default()
        }
    }

    fn execute_find(
        graph: &GraphForge,
        options: FindOptions,
        provider: &mut FakeProvider,
        checkpoint: &mut ProviderArtifactCheckpoint<'_>,
    ) -> Result<RecordBatch, ProviderFindError> {
        let mut runtime = StandardProviderExecutionRuntime::new();
        let mut count_tokens =
            |_: &ProviderModelContract, text: &str| Ok(u64::try_from(text.len()).unwrap());
        let mut estimate_cost =
            |shape: ProviderQueryWorkShape| Ok(shape.input_tokens().saturating_add(1));
        graph.find_with_provider(
            options,
            ProviderFindExecution::new(
                provider,
                &mut runtime,
                &mut count_tokens,
                &mut estimate_cost,
                checkpoint,
                ProviderRequestLimits::default(),
                execution_limits(),
            ),
        )
    }

    fn configured(provider: FakeProvider) -> ConfiguredProviderFindRuntime {
        ConfiguredProviderFindRuntime::new(
            provider,
            StandardProviderExecutionRuntime::new(),
            |_: &ProviderModelContract, text: &str| Ok(u64::try_from(text.len()).unwrap()),
            |_: ProviderQueryWorkShape| Ok(0),
            || Ok(()),
            ProviderRequestLimits::default(),
            execution_limits(),
        )
    }

    fn first_uuid(batch: &RecordBatch) -> [u8; 16] {
        batch
            .column_by_name("node_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(0)
            .try_into()
            .unwrap()
    }

    #[test]
    fn embeds_query_and_returns_canonical_vector_arrow_results() {
        let graph = GraphForge::new(None).unwrap();
        let alpha = node(&graph, "alpha systems");
        node(&graph, "beta systems");
        let mut provider = provider("vendor/model", vec![1.0, 0.0]);
        publish_provider_space(&graph, &mut provider);
        let mut checkpoint = || Ok(());

        let batch = execute_find(&graph, options(), &mut provider, &mut checkpoint).unwrap();

        assert_eq!(provider.query_calls, 1);
        assert_eq!(first_uuid(&batch), *alpha.uuid.as_bytes());
        assert_eq!(
            batch
                .column_by_name("matched_on")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "vector"
        );
    }

    #[test]
    fn ordinary_find_resolves_and_replaces_exact_configured_runtime() {
        let graph = GraphForge::new(None).unwrap();
        let alpha = node(&graph, "alpha systems");
        node(&graph, "beta systems");
        let mut publisher = provider("vendor/model", vec![1.0, 0.0]);
        publish_provider_space(&graph, &mut publisher);

        assert!(matches!(graph.find(options()), Err(GfError::Validation(_))));
        graph
            .configure_provider_find_runtime(configured(provider("vendor/other", vec![0.0, 1.0])))
            .unwrap();
        graph
            .configure_provider_find_runtime(configured(provider("vendor/model", vec![0.0, 1.0])))
            .unwrap();
        graph
            .configure_provider_find_runtime(configured(provider("vendor/model", vec![1.0, 0.0])))
            .unwrap();

        let batch = graph.find(options()).unwrap();
        assert_eq!(first_uuid(&batch), *alpha.uuid.as_bytes());

        graph
            .configure_provider_find_runtime(configured(provider("vendor/model", vec![1.0])))
            .unwrap();
        assert!(matches!(
            graph.find(options()),
            Err(GfError::Provider {
                ref class,
                ref provider,
                ref model,
            }) if class == "malformed_response"
                && provider == "openrouter"
                && model == "vendor/model"
        ));
    }

    #[test]
    fn rejects_contract_mismatch_before_provider_invocation() {
        let graph = GraphForge::new(None).unwrap();
        node(&graph, "alpha systems");
        let mut publisher = provider("vendor/model", vec![1.0, 0.0]);
        publish_provider_space(&graph, &mut publisher);
        let mut mismatch = provider("vendor/other", vec![1.0, 0.0]);
        let mut checkpoint = || Ok(());

        let error = execute_find(&graph, options(), &mut mismatch, &mut checkpoint).unwrap_err();

        assert!(matches!(
            error,
            ProviderFindError::Provider(ref error)
                if error.class() == ProviderFailureClass::InvalidRequest
        ));
        assert_eq!(mismatch.query_calls, 0);
    }

    #[test]
    fn rejects_caller_space_and_malformed_query_vector() {
        let caller_graph = GraphForge::new(None).unwrap();
        let caller_node = node(&caller_graph, "alpha systems");
        caller_graph
            .publish_caller_embeddings(CallerEmbeddingBatchRequest {
                display_name: "semantic".to_owned(),
                contract_version: "caller-v1".to_owned(),
                dimensions: 2,
                normalization: CallerEmbeddingNormalization::L2,
                distance: CallerEmbeddingDistance::Cosine,
                source_projection_recipe: BTreeMap::from([(
                    "label".to_owned(),
                    "Document".to_owned(),
                )]),
                rows: vec![CallerEmbeddingBatchRow {
                    node: NodeSelector::Handle(caller_node),
                    vector: vec![1.0, 0.0],
                }],
                replace_alias: false,
            })
            .unwrap();
        let mut query_provider = provider("vendor/model", vec![1.0, 0.0]);
        let mut checkpoint = || Ok(());
        let error = execute_find(
            &caller_graph,
            options(),
            &mut query_provider,
            &mut checkpoint,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            ProviderFindError::Api(GfError::Validation(_))
        ));
        assert_eq!(query_provider.query_calls, 0);

        let graph = GraphForge::new(None).unwrap();
        node(&graph, "alpha systems");
        let mut publisher = provider("vendor/model", vec![1.0, 0.0]);
        publish_provider_space(&graph, &mut publisher);
        publisher.query_vector = vec![1.0];
        let error = execute_find(&graph, options(), &mut publisher, &mut checkpoint).unwrap_err();
        assert!(matches!(
            error,
            ProviderFindError::Provider(ref error)
                if error.class() == ProviderFailureClass::MalformedResponse
        ));
    }

    #[test]
    fn cancellation_is_structured_before_query_provider_invocation() {
        let graph = GraphForge::new(None).unwrap();
        node(&graph, "alpha systems");
        let mut provider = provider("vendor/model", vec![1.0, 0.0]);
        publish_provider_space(&graph, &mut provider);
        let mut checkpoint = || Err(SearchArtifactError::Cancelled);

        let error = execute_find(&graph, options(), &mut provider, &mut checkpoint).unwrap_err();

        assert!(matches!(
            error,
            ProviderFindError::Artifact(SearchArtifactError::Cancelled)
        ));
        assert_eq!(provider.query_calls, 0);
    }

    #[test]
    fn alias_lineage_change_retries_once_and_second_change_fails_structurally() {
        let graph = GraphForge::new(None).unwrap();
        let alpha = graph
            .add_node(
                "Document",
                &HashMap::from([
                    ("body".to_owned(), PropValue::Str("alpha body".to_owned())),
                    (
                        "summary".to_owned(),
                        PropValue::Str("alpha summary".to_owned()),
                    ),
                ]),
            )
            .unwrap();
        let mut provider = provider("vendor/model", vec![1.0, 0.0]);
        let original = publish_provider_space_as(&graph, &mut provider, "semantic", "body");
        let replacement =
            publish_provider_space_as(&graph, &mut provider, "replacement", "summary");
        let mut runtime = StandardProviderExecutionRuntime::new();
        let mut counts = 0;
        let replacement_id = replacement.compatibility_id;
        let mut count_tokens = |_: &ProviderModelContract, text: &str| {
            counts += 1;
            if counts == 1 {
                graph
                    .bind_embedding_space_alias("semantic", &replacement_id, true)
                    .unwrap();
            }
            Ok(u64::try_from(text.len()).unwrap())
        };
        let mut estimate_cost = |_: ProviderQueryWorkShape| Ok(0);
        let mut checkpoint = || Ok(());

        let batch = graph
            .find_with_provider(
                options(),
                ProviderFindExecution::new(
                    &mut provider,
                    &mut runtime,
                    &mut count_tokens,
                    &mut estimate_cost,
                    &mut checkpoint,
                    ProviderRequestLimits::default(),
                    execution_limits(),
                ),
            )
            .unwrap();

        assert_eq!(counts, 2);
        assert_eq!(provider.query_calls, 2);
        assert_eq!(first_uuid(&batch), *alpha.uuid.as_bytes());
        assert_eq!(
            graph
                .embedding_space(Some("semantic"))
                .unwrap()
                .compatibility_id,
            replacement_id
        );

        graph
            .bind_embedding_space_alias("semantic", &original.compatibility_id, true)
            .unwrap();
        let mut runtime = StandardProviderExecutionRuntime::new();
        let mut counts = 0;
        let original_id = original.compatibility_id;
        let mut count_tokens = |_: &ProviderModelContract, text: &str| {
            counts += 1;
            let target = if counts == 1 {
                &replacement_id
            } else {
                &original_id
            };
            graph
                .bind_embedding_space_alias("semantic", target, true)
                .unwrap();
            Ok(u64::try_from(text.len()).unwrap())
        };
        let mut estimate_cost = |_: ProviderQueryWorkShape| Ok(0);
        let mut checkpoint = || Ok(());
        let error = graph
            .find_with_provider(
                options(),
                ProviderFindExecution::new(
                    &mut provider,
                    &mut runtime,
                    &mut count_tokens,
                    &mut estimate_cost,
                    &mut checkpoint,
                    ProviderRequestLimits::default(),
                    execution_limits(),
                ),
            )
            .unwrap_err();
        assert!(matches!(
            error,
            ProviderFindError::Artifact(SearchArtifactError::ConcurrentMutation)
        ));
        assert_eq!(counts, 2);
        assert_eq!(provider.query_calls, 4);
    }
}
