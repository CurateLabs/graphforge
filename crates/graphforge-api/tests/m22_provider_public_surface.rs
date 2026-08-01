//! Direct provider-backed M19 facade evidence with in-process deterministic fakes.

use std::collections::HashMap;
use std::time::Duration;

use arrow::array::FixedSizeBinaryArray;
use graphforge_api::{
    CandidateReranker, DocumentEmbeddingOutput, DocumentEmbeddingProvider,
    DocumentEmbeddingRequest, FindOptions, GraphForge, OpenRouterProviderSession,
    OpenRouterProviderSessionConfig, OpenRouterWireLimits, PropValue, ProviderBatchLimits,
    ProviderBatchShape, ProviderCapabilities, ProviderCapability, ProviderEmbeddingDistance,
    ProviderEmbeddingExecution, ProviderEmbeddingNormalization, ProviderEmbeddingPlanRequest,
    ProviderExecutionLimits, ProviderModelContract, ProviderRequestLimits, ProviderRerankExecution,
    ProviderRerankRequest, ProviderResult, RerankFailurePolicy, RerankOutput, RerankStatus,
    StandardProviderExecutionRuntime,
};
use graphforge_search::RerankRequest;
use graphforge_storage::{TokenCountClass, TokenizerIdentity};

struct FakeProvider {
    contract: ProviderModelContract,
    calls: usize,
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
        self.calls += 1;
        Ok(request
            .inputs()
            .iter()
            .map(|input| DocumentEmbeddingOutput {
                node_uuid: input.node_uuid,
                vector: vec![3.0, 4.0],
            })
            .collect())
    }
}

impl CandidateReranker for FakeProvider {
    fn contract(&self) -> &ProviderModelContract {
        &self.contract
    }

    fn provide_rerank(
        &mut self,
        request: &RerankRequest<'_>,
        checkpoint: &mut dyn FnMut() -> ProviderResult<()>,
    ) -> ProviderResult<Vec<RerankOutput>> {
        checkpoint()?;
        self.calls += 1;
        Ok(request
            .candidates()
            .iter()
            .enumerate()
            .map(|(index, candidate)| RerankOutput {
                node_uuid: candidate.node_uuid,
                score: if index == 0 { 0.1 } else { 0.9 },
            })
            .collect())
    }
}

fn contract() -> ProviderModelContract {
    ProviderModelContract::remote(
        None,
        "vendor/model",
        "revision-1",
        "wire-v1",
        ProviderCapabilities::new([
            ProviderCapability::DocumentEmbeddings,
            ProviderCapability::CandidateReranking,
        ])
        .unwrap(),
        TokenizerIdentity {
            identifier: "test-tokenizer".into(),
            version: "v1".into(),
            count_class: TokenCountClass::ExactLocal,
            max_input_tokens: 1_024,
            normalization: "nfc".into(),
        },
        None,
    )
    .unwrap()
}

fn embedding_request(contract: ProviderModelContract) -> ProviderEmbeddingPlanRequest {
    ProviderEmbeddingPlanRequest {
        display_name: "semantic".into(),
        label: "Document".into(),
        properties: vec!["body".into()],
        contract,
        dimensions: 2,
        normalization: ProviderEmbeddingNormalization::L2,
        distance: ProviderEmbeddingDistance::Cosine,
        request_limits: ProviderRequestLimits::default(),
        batch_limits: ProviderBatchLimits::default(),
        execution_limits: ProviderExecutionLimits::default(),
        replace_alias: false,
    }
}

fn add_documents(graph: &GraphForge, bodies: &[&str]) {
    for body in bodies {
        graph
            .add_node(
                "Document",
                &HashMap::from([("body".into(), PropValue::Str((*body).into()))]),
            )
            .unwrap();
    }
}

fn publish_embeddings(
    graph: &GraphForge,
    request: &ProviderEmbeddingPlanRequest,
    provider: &mut FakeProvider,
) {
    let mut runtime = StandardProviderExecutionRuntime::new();
    let mut count_tokens =
        |_: &ProviderModelContract, text: &str| Ok(u64::try_from(text.len()).unwrap());
    let mut estimate_cost = |shape: ProviderBatchShape| Ok(shape.input_tokens());
    let mut checkpoint = || Ok(());
    let execution = ProviderEmbeddingExecution::new(
        provider,
        &mut runtime,
        &mut count_tokens,
        &mut estimate_cost,
        &mut checkpoint,
    );
    graph
        .publish_provider_embeddings(request, execution)
        .unwrap();
}

fn refresh_embeddings(
    graph: &GraphForge,
    request: &ProviderEmbeddingPlanRequest,
    provider: &mut FakeProvider,
) -> graphforge_api::EmbeddingRefreshInspection {
    let mut runtime = StandardProviderExecutionRuntime::new();
    let mut count_tokens =
        |_: &ProviderModelContract, text: &str| Ok(u64::try_from(text.len()).unwrap());
    let mut estimate_cost = |shape: ProviderBatchShape| Ok(shape.input_tokens());
    let mut checkpoint = || Ok(());
    graph
        .refresh_provider_embeddings(
            request,
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

fn rerank(
    graph: &GraphForge,
    canonical: &arrow::record_batch::RecordBatch,
    request: &ProviderRerankRequest,
    provider: &mut FakeProvider,
) -> graphforge_api::ProviderRerankedFindResult {
    let mut runtime = StandardProviderExecutionRuntime::new();
    let mut count_tokens =
        |_: &ProviderModelContract, text: &str| Ok(u64::try_from(text.len()).unwrap());
    let mut estimate_cost = |shape: graphforge_api::RerankWorkShape| Ok(shape.input_tokens());
    let mut checkpoint = || Ok(());
    graph
        .rerank_find_results(
            canonical,
            request,
            ProviderRerankExecution::new(
                provider,
                &mut runtime,
                &mut count_tokens,
                &mut estimate_cost,
                &mut checkpoint,
            ),
        )
        .unwrap()
}

fn assert_reranked(
    reranked: &graphforge_api::ProviderRerankedFindResult,
    repeated: &graphforge_api::ProviderRerankedFindResult,
    original: &[u8],
) {
    assert_eq!(reranked.batch(), repeated.batch());
    assert_eq!(reranked.status(), repeated.status());
    assert!(matches!(reranked.status(), RerankStatus::Reranked { .. }));
    let ids = reranked
        .batch()
        .column_by_name("node_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_ne!(ids.value(0), original);
}

#[test]
fn provider_refresh_and_rerank_public_facades_are_covered() {
    let graph = GraphForge::new(None).unwrap();
    add_documents(&graph, &["alpha systems", "beta systems"]);
    let provider_contract = contract();
    let request = embedding_request(provider_contract.clone());
    let mut provider = FakeProvider {
        contract: provider_contract.clone(),
        calls: 0,
    };
    publish_embeddings(&graph, &request, &mut provider);
    add_documents(&graph, &["gamma systems"]);
    let mut refresh_runtime = StandardProviderExecutionRuntime::new();
    let mut refresh_count_tokens =
        |_: &ProviderModelContract, text: &str| Ok(u64::try_from(text.len()).unwrap());
    let mut refresh_estimate_cost = |shape: ProviderBatchShape| Ok(shape.input_tokens());
    let mut refresh_checkpoint = || Ok(());
    let refresh = graph
        .refresh_provider_embeddings(
            &request,
            ProviderEmbeddingExecution::new(
                &mut provider,
                &mut refresh_runtime,
                &mut refresh_count_tokens,
                &mut refresh_estimate_cost,
                &mut refresh_checkpoint,
            ),
        )
        .unwrap();
    let repeated_refresh = refresh_embeddings(&graph, &request, &mut provider);
    assert_eq!(refresh.compatibility_id, repeated_refresh.compatibility_id);
    assert!(refresh.last_outcome.is_some());
    assert_eq!(
        graph
            .embedding_space(Some("semantic"))
            .unwrap()
            .active
            .unwrap()
            .vector_count,
        3
    );
    assert_eq!(provider.calls, 3);

    graph
        .index_search(
            "Document",
            graphforge_api::SearchIndexOptions::Text {
                properties: Some(vec!["body".into()]),
                rebuild: false,
            },
        )
        .unwrap();
    let canonical = graph
        .find(FindOptions {
            query: Some("systems".into()),
            label: Some("Document".into()),
            limit: 2,
            ..FindOptions::default()
        })
        .unwrap();
    let original = canonical
        .column_by_name("node_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap()
        .value(0)
        .to_vec();
    let rerank_request = ProviderRerankRequest {
        label: "Document".into(),
        query: "beta first".into(),
        properties: vec!["body".into()],
        candidate_depth: 2,
        limit: 2,
        contract: provider_contract,
        request_limits: ProviderRequestLimits::default(),
        execution_limits: ProviderExecutionLimits::default(),
        failure_policy: RerankFailurePolicy::Error,
    };
    let mut rerank_runtime = StandardProviderExecutionRuntime::new();
    let mut rerank_count_tokens =
        |_: &ProviderModelContract, text: &str| Ok(u64::try_from(text.len()).unwrap());
    let mut rerank_estimate_cost =
        |shape: graphforge_api::RerankWorkShape| Ok(shape.input_tokens());
    let mut rerank_checkpoint = || Ok(());
    let reranked = graph
        .rerank_find_results(
            &canonical,
            &rerank_request,
            ProviderRerankExecution::new(
                &mut provider,
                &mut rerank_runtime,
                &mut rerank_count_tokens,
                &mut rerank_estimate_cost,
                &mut rerank_checkpoint,
            ),
        )
        .unwrap();
    let repeated = rerank(&graph, &canonical, &rerank_request, &mut provider);
    assert_reranked(&reranked, &repeated, &original);
}

#[test]
fn openrouter_session_refresh_embeddings_rejects_drift_without_transport() {
    let session = OpenRouterProviderSession::new(
        OpenRouterProviderSessionConfig {
            origin: "http://127.0.0.1:9".into(),
            model: "configured/model".into(),
            revision: "revision".into(),
            response_contract_version: "v1".into(),
            capabilities: ProviderCapabilities::new([ProviderCapability::DocumentEmbeddings])
                .unwrap(),
            max_input_tokens: 1_024,
            chunking: None,
            wire_limits: OpenRouterWireLimits::default(),
            request_limits: ProviderRequestLimits::default(),
            execution_limits: ProviderExecutionLimits::default(),
            transport_timeout: Duration::from_millis(1),
            estimated_cost_microunits_per_token: 1,
        },
        "unused-secret".into(),
    )
    .unwrap();
    let graph = GraphForge::new(None).unwrap();
    let request = embedding_request(contract());
    let first = session
        .refresh_embeddings(&graph, &request)
        .unwrap_err()
        .to_string();
    let repeated = session
        .refresh_embeddings(&graph, &request)
        .unwrap_err()
        .to_string();
    assert_eq!(first, repeated);
    assert_eq!(
        first,
        "validation error: provider embedding request does not match the configured session contract and limits"
    );
}
