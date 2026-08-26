//! Rust-owned configured OpenRouter workflow shared by public bindings.

use std::time::Duration;

use graphforge_search::{
    EmbeddingRefreshCompletion, OpenRouterHttpTransport, OpenRouterOwnedAdapter,
    OpenRouterWireLimits, ProviderCapabilities, ProviderExecutionController,
    ProviderExecutionLimits, ProviderFailureClass, ProviderModelContract, ProviderRequestLimits,
    ProviderResult, RerankWorkShape, StandardProviderExecutionRuntime,
};
use graphforge_storage::{
    ChunkingIdentity, EmbeddingCompatibilityId, EmbeddingSourceState, SearchArtifactError,
    TokenCountClass, TokenizerIdentity,
};

use super::{
    ConfiguredProviderFindRuntime, EmbeddingRefreshInspection, EmbeddingSpaceInfo,
    FindExecutionOptions, FindExecutionResult, GfError, GraphForge, ProviderEmbeddingExecution,
    ProviderEmbeddingExecutionError, ProviderEmbeddingPlanError, ProviderEmbeddingPlanInspection,
    ProviderEmbeddingPlanRequest, ProviderQueryWorkShape, ProviderRerankExecution,
};
use crate::provider_find::provider_gf_error;

const TOKENIZER_IDENTIFIER: &str = "graphforge_utf8_byte_upper_bound";
const TOKENIZER_VERSION: &str = "v1";
const TOKENIZER_NORMALIZATION: &str = "utf8_bytes_v1";

/// Non-secret configuration shared by every operation in one OpenRouter session.
#[derive(Clone, Debug)]
pub struct OpenRouterProviderSessionConfig {
    /// Explicit HTTPS origin, or loopback HTTP for deterministic local testing.
    pub origin: String,
    /// Exact provider model identifier.
    pub model: String,
    /// Immutable model revision, or `unavailable`.
    pub revision: String,
    /// Versioned OpenRouter response contract.
    pub response_contract_version: String,
    /// Capabilities advertised by this exact model.
    pub capabilities: ProviderCapabilities,
    /// Conservative maximum model input tokens.
    pub max_input_tokens: u64,
    /// Explicit chunking contract, or fail-closed unchunked behavior.
    pub chunking: Option<ChunkingIdentity>,
    /// Independent serialized request and response byte bounds.
    pub wire_limits: OpenRouterWireLimits,
    /// Per-provider invocation bounds.
    pub request_limits: ProviderRequestLimits,
    /// Retry, deadline, exposure, rate, and spend bounds.
    pub execution_limits: ProviderExecutionLimits,
    /// Global bound for each HTTP invocation.
    pub transport_timeout: Duration,
    /// Caller-configured conservative cost estimate per counted input token.
    pub estimated_cost_microunits_per_token: u64,
}

/// Runtime-only credential and one exact configured provider workflow.
///
/// This type deliberately omits `Clone` and `Debug`; bearer material never enters
/// durable metadata, diagnostics, or binding representations.
pub struct OpenRouterProviderSession {
    config: OpenRouterProviderSessionConfig,
    bearer_credential: String,
    contract: ProviderModelContract,
}

/// Runtime-only producer recipe for one exact provider embedding lineage.
///
/// This deliberately omits `Clone` and `Debug`; its credential-bearing session
/// is shared only behind an `Arc` owned by the facade worker registry.
pub(crate) struct ConfiguredProviderRefreshRuntime {
    compatibility_id: EmbeddingCompatibilityId,
    session: OpenRouterProviderSession,
    request: ProviderEmbeddingPlanRequest,
}

impl ConfiguredProviderRefreshRuntime {
    pub(crate) const fn compatibility_id(&self) -> EmbeddingCompatibilityId {
        self.compatibility_id
    }

    pub(crate) fn display_name(&self) -> &str {
        &self.request.display_name
    }

    pub(crate) fn capture_source(
        &self,
        graph: &GraphForge,
    ) -> Result<EmbeddingSourceState, ProviderEmbeddingPlanError> {
        graph
            .capture_provider_source(&self.request, &mut || Ok(()))
            .map(|(_, source)| source)
    }

    pub(crate) fn refresh(
        &self,
        graph: &GraphForge,
    ) -> Result<EmbeddingSourceState, ProviderEmbeddingExecutionError> {
        self.session
            .execute_refresh_embeddings(graph, &self.request)?;
        let (_, lineage) = graph
            .resolve_embedding_space_lineage(Some(&self.request.display_name))
            .map_err(ProviderEmbeddingExecutionError::Api)?;
        lineage
            .active()
            .map(|active| active.manifest.source())
            .ok_or_else(|| {
                ProviderEmbeddingExecutionError::Api(GfError::Validation(
                    "successful provider refresh did not publish an active generation".into(),
                ))
            })
    }

    pub(crate) fn completion<T>(
        result: &Result<T, ProviderEmbeddingExecutionError>,
    ) -> EmbeddingRefreshCompletion {
        match result {
            Ok(_) => EmbeddingRefreshCompletion::Succeeded,
            Err(
                ProviderEmbeddingExecutionError::Plan(ProviderEmbeddingPlanError::Artifact(
                    SearchArtifactError::Cancelled,
                ))
                | ProviderEmbeddingExecutionError::Publication(
                    graphforge_search::ProviderPublicationError::Artifact(
                        SearchArtifactError::Cancelled,
                    ),
                ),
            ) => EmbeddingRefreshCompletion::Cancelled,
            Err(
                ProviderEmbeddingExecutionError::Plan(ProviderEmbeddingPlanError::Provider(error))
                | ProviderEmbeddingExecutionError::Publication(
                    graphforge_search::ProviderPublicationError::Provider(error),
                ),
            ) if error.class() == ProviderFailureClass::Cancelled => {
                EmbeddingRefreshCompletion::Cancelled
            }
            Err(_) => EmbeddingRefreshCompletion::Failed,
        }
    }
}

impl OpenRouterProviderSession {
    /// Validate and own one OpenRouter configuration and runtime-only credential.
    ///
    /// Token counts use a conservative UTF-8 byte upper bound and are explicitly
    /// persisted as approximate; the session never claims provider-exact tokenization.
    ///
    /// # Errors
    /// Rejects invalid origins, credentials, identities, capabilities, limits, or
    /// execution budgets before any provider request can begin.
    pub fn new(
        config: OpenRouterProviderSessionConfig,
        bearer_credential: String,
    ) -> Result<Self, GfError> {
        let tokenizer = TokenizerIdentity {
            identifier: TOKENIZER_IDENTIFIER.to_owned(),
            version: TOKENIZER_VERSION.to_owned(),
            count_class: TokenCountClass::Approximate,
            max_input_tokens: config.max_input_tokens,
            normalization: TOKENIZER_NORMALIZATION.to_owned(),
        };
        let contract = ProviderModelContract::remote(
            None,
            &config.model,
            &config.revision,
            &config.response_contract_version,
            config.capabilities.clone(),
            tokenizer,
            config.chunking.clone(),
        )?;
        config.request_limits.validate()?;
        let runtime = StandardProviderExecutionRuntime::new();
        ProviderExecutionController::new(&contract, config.execution_limits, &runtime)
            .map_err(|error| provider_gf_error(&error))?;
        let transport = OpenRouterHttpTransport::new(&config.origin, config.transport_timeout)?;
        OpenRouterOwnedAdapter::new(
            contract.clone(),
            bearer_credential.clone(),
            transport,
            config.wire_limits,
        )
        .map_err(|error| provider_gf_error(&error))?;
        Ok(Self {
            config,
            bearer_credential,
            contract,
        })
    }

    /// Exact non-secret provider/model/tokenizer/chunking contract.
    #[must_use]
    pub const fn contract(&self) -> &ProviderModelContract {
        &self.contract
    }

    /// Inspect one explicit property-embedding plan without outbound work.
    ///
    /// # Errors
    /// Returns structured request, source, token, limit, or compatibility failures.
    pub fn inspect_embedding_plan(
        &self,
        graph: &GraphForge,
        request: &ProviderEmbeddingPlanRequest,
    ) -> Result<ProviderEmbeddingPlanInspection, ProviderEmbeddingPlanError> {
        self.validate_embedding_request(request)
            .map_err(ProviderEmbeddingPlanError::Api)?;
        graph.inspect_provider_embedding_plan(request, count_tokens)
    }

    /// Execute and atomically publish one confirmed property-embedding plan.
    ///
    /// # Errors
    /// Returns structured plan, provider, cancellation, limit, or publication failures.
    pub fn publish_embeddings(
        &self,
        graph: &GraphForge,
        request: &ProviderEmbeddingPlanRequest,
    ) -> Result<EmbeddingSpaceInfo, ProviderEmbeddingExecutionError> {
        self.validate_embedding_request(request)
            .map_err(ProviderEmbeddingExecutionError::Api)?;
        let mut adapter = self
            .adapter()
            .map_err(ProviderEmbeddingExecutionError::from)?;
        let mut runtime = StandardProviderExecutionRuntime::new();
        let contract = &self.contract;
        let rate = self.config.estimated_cost_microunits_per_token;
        let mut estimate_cost = move |shape: graphforge_search::ProviderBatchShape| {
            estimate_cost(contract, shape.input_tokens(), rate)
        };
        let mut checkpoint = || Ok(());
        let space = graph.publish_provider_embeddings(
            request,
            ProviderEmbeddingExecution::new(
                &mut adapter,
                &mut runtime,
                &mut count_tokens,
                &mut estimate_cost,
                &mut checkpoint,
            ),
        )?;
        graph
            .register_provider_refresh_runtime(
                self.refresh_runtime(request, &space.compatibility_id)?,
            )
            .map_err(ProviderEmbeddingExecutionError::Api)?;
        Ok(space)
    }

    /// Refresh one compatible provider-produced space through the same atomic path.
    ///
    /// # Errors
    /// Returns structured identity, provider, cancellation, limit, or publication failures.
    pub fn refresh_embeddings(
        &self,
        graph: &GraphForge,
        request: &ProviderEmbeddingPlanRequest,
    ) -> Result<EmbeddingRefreshInspection, ProviderEmbeddingExecutionError> {
        self.execute_refresh_embeddings(graph, request)?;
        let inspection = graph
            .inspect_embedding_refresh(Some(&request.display_name))
            .map_err(ProviderEmbeddingExecutionError::Api)?;
        graph
            .register_provider_refresh_runtime(
                self.refresh_runtime(request, &inspection.compatibility_id)?,
            )
            .map_err(ProviderEmbeddingExecutionError::Api)?;
        Ok(inspection)
    }

    fn execute_refresh_embeddings(
        &self,
        graph: &GraphForge,
        request: &ProviderEmbeddingPlanRequest,
    ) -> Result<(), ProviderEmbeddingExecutionError> {
        self.validate_embedding_request(request)
            .map_err(ProviderEmbeddingExecutionError::Api)?;
        let mut adapter = self
            .adapter()
            .map_err(ProviderEmbeddingExecutionError::from)?;
        let mut runtime = StandardProviderExecutionRuntime::new();
        let contract = &self.contract;
        let rate = self.config.estimated_cost_microunits_per_token;
        let mut estimate_cost = move |shape: graphforge_search::ProviderBatchShape| {
            estimate_cost(contract, shape.input_tokens(), rate)
        };
        let mut checkpoint = || Ok(());
        graph.execute_provider_embedding_refresh(
            request,
            ProviderEmbeddingExecution::new(
                &mut adapter,
                &mut runtime,
                &mut count_tokens,
                &mut estimate_cost,
                &mut checkpoint,
            ),
        )
    }

    /// Run ordinary configured semantic retrieval with optional explicit reranking.
    ///
    /// The existing canonical Arrow batch remains unchanged by diagnostics. When
    /// reranking is omitted, advisory emission or suppression is delegated to the
    /// existing Rust-owned policy in `FindExecutionOptions`.
    ///
    /// # Errors
    /// Returns structured configuration, compatibility, provider, retrieval, or rerank failures.
    pub fn find(
        &self,
        graph: &GraphForge,
        options: FindExecutionOptions,
    ) -> Result<FindExecutionResult, GfError> {
        self.validate_find_options(&options)?;
        if options.find.semantic_query.is_some() {
            graph.configure_provider_find_runtime(self.query_runtime()?)?;
        }
        if options.rerank.is_none() {
            return graph.find_with_diagnostics(options, None);
        }
        let mut adapter = self.adapter().map_err(|error| provider_gf_error(&error))?;
        let mut runtime = StandardProviderExecutionRuntime::new();
        let contract = &self.contract;
        let rate = self.config.estimated_cost_microunits_per_token;
        let mut estimate_cost =
            move |shape: RerankWorkShape| estimate_cost(contract, shape.input_tokens(), rate);
        let mut checkpoint = || Ok(());
        graph.find_with_diagnostics(
            options,
            Some(ProviderRerankExecution::new(
                &mut adapter,
                &mut runtime,
                &mut count_tokens,
                &mut estimate_cost,
                &mut checkpoint,
            )),
        )
    }

    fn query_runtime(&self) -> Result<ConfiguredProviderFindRuntime, GfError> {
        let adapter = self.adapter().map_err(|error| provider_gf_error(&error))?;
        let contract = self.contract.clone();
        let rate = self.config.estimated_cost_microunits_per_token;
        Ok(ConfiguredProviderFindRuntime::new(
            adapter,
            StandardProviderExecutionRuntime::new(),
            count_tokens,
            move |shape: ProviderQueryWorkShape| {
                estimate_cost(&contract, shape.input_tokens(), rate)
            },
            || Ok(()),
            self.config.request_limits,
            self.config.execution_limits,
        ))
    }

    fn refresh_runtime(
        &self,
        request: &ProviderEmbeddingPlanRequest,
        compatibility_id: &str,
    ) -> Result<ConfiguredProviderRefreshRuntime, GfError> {
        let session = Self::new(self.config.clone(), self.bearer_credential.clone())?;
        let request = ProviderEmbeddingPlanRequest {
            display_name: request.display_name.clone(),
            label: request.label.clone(),
            properties: request.properties.clone(),
            contract: session.contract().clone(),
            dimensions: request.dimensions,
            normalization: request.normalization,
            distance: request.distance,
            request_limits: request.request_limits,
            batch_limits: request.batch_limits,
            execution_limits: request.execution_limits,
            replace_alias: request.replace_alias,
        };
        Ok(ConfiguredProviderRefreshRuntime {
            compatibility_id: EmbeddingCompatibilityId::from_hex(compatibility_id)?,
            session,
            request,
        })
    }

    fn adapter(&self) -> ProviderResult<OpenRouterOwnedAdapter<OpenRouterHttpTransport>> {
        let transport =
            OpenRouterHttpTransport::new(&self.config.origin, self.config.transport_timeout)
                .map_err(|_| {
                    graphforge_search::ProviderError::new(
                        &self.contract,
                        graphforge_search::ProviderFailureClass::InvalidRequest,
                    )
                })?;
        OpenRouterOwnedAdapter::new(
            self.contract.clone(),
            self.bearer_credential.clone(),
            transport,
            self.config.wire_limits,
        )
    }

    fn validate_embedding_request(
        &self,
        request: &ProviderEmbeddingPlanRequest,
    ) -> Result<(), GfError> {
        if request.contract != self.contract
            || request.request_limits != self.config.request_limits
            || request.execution_limits != self.config.execution_limits
        {
            return Err(validation(
                "provider embedding request does not match the configured session contract and limits",
            ));
        }
        Ok(())
    }

    fn validate_find_options(&self, options: &FindExecutionOptions) -> Result<(), GfError> {
        if let Some(rerank) = &options.rerank
            && (rerank.contract != self.contract
                || rerank.request_limits != self.config.request_limits
                || rerank.execution_limits != self.config.execution_limits)
        {
            return Err(validation(
                "rerank request does not match the configured session contract and limits",
            ));
        }
        if let Some(omitted) = &options.omitted_reranker
            && omitted != &self.contract
        {
            return Err(validation(
                "omitted reranker does not match the configured session contract",
            ));
        }
        Ok(())
    }
}

fn count_tokens(contract: &ProviderModelContract, text: &str) -> ProviderResult<u64> {
    text.as_bytes()
        .iter()
        .try_fold(0_u64, |count, _| {
            count.checked_add(1).ok_or_else(|| {
                graphforge_search::ProviderError::new(
                    contract,
                    graphforge_search::ProviderFailureClass::ResourceExhausted,
                )
            })
        })
        .map(|count| count.max(1))
}

fn estimate_cost(
    contract: &ProviderModelContract,
    input_tokens: u64,
    rate: u64,
) -> ProviderResult<u64> {
    input_tokens.checked_mul(rate).ok_or_else(|| {
        graphforge_search::ProviderError::new(
            contract,
            graphforge_search::ProviderFailureClass::ResourceExhausted,
        )
    })
}

fn validation(message: impl Into<String>) -> GfError {
    GfError::Validation(message.into())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::atomic::Ordering;
    use std::thread;

    use graphforge_search::{ProviderCapability, ProviderFailureClass};
    use serde_json::{Value, json};

    use super::*;

    fn failing_refresh_session() -> (OpenRouterProviderSession, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let server = thread::spawn(move || {
            for call in 0..2 {
                let (mut stream, _) = listener.accept().unwrap();
                let mut received = Vec::new();
                let (headers_end, content_length) = loop {
                    let mut chunk = [0_u8; 1024];
                    let count = stream.read(&mut chunk).unwrap();
                    assert!(count > 0);
                    received.extend_from_slice(&chunk[..count]);
                    let Some(headers_end) =
                        received.windows(4).position(|part| part == b"\r\n\r\n")
                    else {
                        continue;
                    };
                    let headers = String::from_utf8_lossy(&received[..headers_end]);
                    let content_length = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .map(str::to_owned)
                        })
                        .unwrap()
                        .parse::<usize>()
                        .unwrap();
                    if received.len() >= headers_end + 4 + content_length {
                        break (headers_end, content_length);
                    }
                };
                let body = &received[headers_end + 4..headers_end + 4 + content_length];
                let payload: Value = serde_json::from_slice(body).unwrap();
                if call == 0 {
                    assert_eq!(payload["input"].as_array().unwrap().len(), 2);
                    let response = serde_json::to_vec(&json!({
                        "model":"vendor/model",
                        "data":[
                            {"index":0,"embedding":[1.0,0.0]},
                            {"index":1,"embedding":[0.0,1.0]}
                        ]
                    }))
                    .unwrap();
                    write!(
                        stream,
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        response.len()
                    )
                    .unwrap();
                    stream.write_all(&response).unwrap();
                } else {
                    assert_eq!(payload["input"].as_array().unwrap().len(), 3);
                    let response = br#"{"error":"temporarily unavailable"}"#;
                    write!(
                        stream,
                        "HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        response.len()
                    )
                    .unwrap();
                    stream.write_all(response).unwrap();
                }
            }
        });
        let contract = ProviderModelContract::remote(
            None,
            "vendor/model",
            "revision",
            "v1",
            ProviderCapabilities::new([
                ProviderCapability::DocumentEmbeddings,
                ProviderCapability::QueryEmbeddings,
                ProviderCapability::CandidateReranking,
            ])
            .unwrap(),
            TokenizerIdentity {
                identifier: TOKENIZER_IDENTIFIER.into(),
                version: TOKENIZER_VERSION.into(),
                count_class: TokenCountClass::Approximate,
                max_input_tokens: 10_000,
                normalization: TOKENIZER_NORMALIZATION.into(),
            },
            None,
        )
        .unwrap();
        let session = OpenRouterProviderSession {
            config: OpenRouterProviderSessionConfig {
                origin,
                model: "vendor/model".into(),
                revision: "revision".into(),
                response_contract_version: "v1".into(),
                capabilities: contract.capabilities().clone(),
                max_input_tokens: 10_000,
                chunking: None,
                wire_limits: OpenRouterWireLimits::default(),
                request_limits: ProviderRequestLimits::default(),
                execution_limits: ProviderExecutionLimits {
                    retries: 0,
                    ..ProviderExecutionLimits::default()
                },
                transport_timeout: Duration::from_secs(2),
                estimated_cost_microunits_per_token: 1,
            },
            bearer_credential: "test-secret".into(),
            contract,
        };
        (session, server)
    }

    #[test]
    fn proactive_failure_completion_is_driven_without_wall_clock_polling() {
        let (session, server) = failing_refresh_session();
        let graph = GraphForge::new(None).unwrap();
        for title in ["First", "Second"] {
            graph
                .add_node(
                    "Paper",
                    &HashMap::from([("title".into(), crate::PropValue::Str(title.into()))]),
                )
                .unwrap();
        }
        let request = ProviderEmbeddingPlanRequest {
            display_name: "semantic".into(),
            label: "Paper".into(),
            properties: vec!["title".into()],
            contract: session.contract().clone(),
            dimensions: 2,
            normalization: crate::ProviderEmbeddingNormalization::None,
            distance: crate::ProviderEmbeddingDistance::Cosine,
            request_limits: ProviderRequestLimits::default(),
            batch_limits: crate::ProviderBatchLimits::default(),
            execution_limits: session.config.execution_limits,
            replace_alias: false,
        };
        session.publish_embeddings(&graph, &request).unwrap();
        let original = graph
            .embedding_space(Some("semantic"))
            .unwrap()
            .active
            .unwrap();

        // Own the driver token so the mutation queues real proactive work but
        // cannot race a detached worker. Drive that exact queue synchronously
        // at a monotonic time beyond every valid debounce deadline.
        graph
            .provider_refresh_driver_active
            .store(true, Ordering::Release);
        graph
            .add_node(
                "Paper",
                &HashMap::from([("title".into(), crate::PropValue::Str("Third".into()))]),
            )
            .unwrap();
        let queued = graph.inspect_embedding_refresh(Some("semantic")).unwrap();
        assert!(queued.worker.selected_lineage_queued);
        assert_eq!(queued.worker.failed, 0);
        graph.drive_ready_provider_refreshes_at(Duration::MAX);
        graph
            .provider_refresh_driver_active
            .store(false, Ordering::Release);

        let failed = graph.inspect_embedding_refresh(Some("semantic")).unwrap();
        assert!(!failed.worker.selected_lineage_queued);
        assert!(!failed.worker.selected_lineage_in_flight);
        assert_eq!(failed.worker.failed, 1);
        assert_eq!(failed.worker.succeeded, 0);
        assert!(matches!(
            failed.last_outcome.map(|outcome| outcome.status),
            Some(graphforge_storage::EmbeddingRefreshOutcomeStatus::Failed(
                graphforge_storage::EmbeddingRefreshFailureClass::Provider
            ))
        ));
        let active = graph
            .embedding_space(Some("semantic"))
            .unwrap()
            .active
            .unwrap();
        assert_eq!(active.generation_id, original.generation_id);
        assert_eq!(active.vector_count, original.vector_count);
        server.join().unwrap();
    }

    #[test]
    fn cost_estimates_fail_closed_on_overflow() {
        let contract = ProviderModelContract::remote(
            None,
            "vendor/model",
            "r1",
            "v1",
            ProviderCapabilities::new([ProviderCapability::QueryEmbeddings]).unwrap(),
            TokenizerIdentity {
                identifier: TOKENIZER_IDENTIFIER.to_owned(),
                version: TOKENIZER_VERSION.to_owned(),
                count_class: TokenCountClass::Approximate,
                max_input_tokens: 1_024,
                normalization: TOKENIZER_NORMALIZATION.to_owned(),
            },
            None,
        )
        .unwrap();

        let error = estimate_cost(&contract, 2, u64::MAX).unwrap_err();

        assert_eq!(error.class(), ProviderFailureClass::ResourceExhausted);
    }

    #[test]
    fn refresh_completion_and_token_count_cover_every_terminal_domain() {
        let contract = ProviderModelContract::remote(
            None,
            "vendor/model",
            "r1",
            "v1",
            ProviderCapabilities::new([ProviderCapability::DocumentEmbeddings]).unwrap(),
            TokenizerIdentity {
                identifier: TOKENIZER_IDENTIFIER.into(),
                version: TOKENIZER_VERSION.into(),
                count_class: TokenCountClass::Approximate,
                max_input_tokens: 1_024,
                normalization: TOKENIZER_NORMALIZATION.into(),
            },
            None,
        )
        .unwrap();
        assert_eq!(count_tokens(&contract, "").unwrap(), 1);
        assert_eq!(count_tokens(&contract, "é").unwrap(), 2);
        assert_eq!(
            ConfiguredProviderRefreshRuntime::completion(&Ok(())),
            EmbeddingRefreshCompletion::Succeeded
        );
        for error in [
            ProviderEmbeddingExecutionError::Plan(ProviderEmbeddingPlanError::Artifact(
                SearchArtifactError::Cancelled,
            )),
            ProviderEmbeddingExecutionError::Publication(
                graphforge_search::ProviderPublicationError::Artifact(
                    SearchArtifactError::Cancelled,
                ),
            ),
            ProviderEmbeddingExecutionError::Plan(ProviderEmbeddingPlanError::Provider(
                graphforge_search::ProviderError::new(&contract, ProviderFailureClass::Cancelled),
            )),
            ProviderEmbeddingExecutionError::Publication(
                graphforge_search::ProviderPublicationError::Provider(
                    graphforge_search::ProviderError::new(
                        &contract,
                        ProviderFailureClass::Cancelled,
                    ),
                ),
            ),
        ] {
            assert_eq!(
                ConfiguredProviderRefreshRuntime::completion::<()>(&Err(error)),
                EmbeddingRefreshCompletion::Cancelled
            );
        }
        assert_eq!(
            ConfiguredProviderRefreshRuntime::completion::<()>(&Err(
                ProviderEmbeddingExecutionError::Api(GfError::Validation("invalid".into()))
            )),
            EmbeddingRefreshCompletion::Failed
        );
    }
}
