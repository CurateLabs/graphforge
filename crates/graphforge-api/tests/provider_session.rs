//! End-to-end configured provider workflow over a deterministic loopback server.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;
use std::time::Duration;

use arrow::array::FixedSizeBinaryArray;
use graphforge_api::{
    FindDiagnostic, FindExecutionOptions, FindOptions, FindRerankOptions, GraphForge,
    OpenRouterProviderSession, OpenRouterProviderSessionConfig, OpenRouterWireLimits, PropValue,
    ProviderBatchLimits, ProviderCapabilities, ProviderCapability, ProviderEmbeddingDistance,
    ProviderEmbeddingNormalization, ProviderEmbeddingPlanRequest, ProviderExecutionLimits,
    ProviderRequestLimits, RerankAdvisoryPolicy, RerankFailurePolicy, RerankStatus,
    TokenCountClass,
};
use serde_json::{Value, json};

fn mock_openrouter() -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        for call in 0..4 {
            let (mut stream, _) = listener.accept().unwrap();
            let request = read_request(&mut stream);
            let (headers, body) = request.split_once("\r\n\r\n").unwrap();
            assert!(
                headers
                    .to_ascii_lowercase()
                    .contains("authorization: bearer test-secret\r\n")
            );
            let payload: Value = serde_json::from_str(body).unwrap();
            let response = match call {
                0 => {
                    assert!(headers.starts_with("POST /api/v1/embeddings HTTP/1.1"));
                    assert_eq!(payload["input"].as_array().unwrap().len(), 2);
                    json!({"model":"vendor/model","data":[
                        {"index":0,"embedding":[1.0,0.0]},
                        {"index":1,"embedding":[0.0,1.0]}
                    ]})
                }
                1 => {
                    assert!(headers.starts_with("POST /api/v1/embeddings HTTP/1.1"));
                    assert!(payload["input"].is_string());
                    json!({"model":"vendor/model","data":[
                        {"index":0,"embedding":[1.0,0.0]}
                    ]})
                }
                2 => {
                    assert!(headers.starts_with("POST /api/v1/rerank HTTP/1.1"));
                    assert_eq!(payload["documents"].as_array().unwrap().len(), 2);
                    json!({"model":"vendor/model","results":[
                        {"index":0,"relevance_score":0.1},
                        {"index":1,"relevance_score":0.9}
                    ]})
                }
                3 => {
                    assert!(headers.starts_with("POST /api/v1/embeddings HTTP/1.1"));
                    assert_eq!(payload["input"].as_array().unwrap().len(), 4);
                    json!({"model":"vendor/model","data":[
                        {"index":0,"embedding":[1.0,0.0]},
                        {"index":1,"embedding":[0.0,1.0]},
                        {"index":2,"embedding":[0.5,0.5]},
                        {"index":3,"embedding":[0.25,0.75]}
                    ]})
                }
                _ => unreachable!(),
            };
            let response = serde_json::to_vec(&response).unwrap();
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                response.len()
            )
            .unwrap();
            stream.write_all(&response).unwrap();
        }
    });
    (origin, server)
}

fn mock_failing_refresh() -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let server = thread::spawn(move || {
        for call in 0..2 {
            let (mut stream, _) = listener.accept().unwrap();
            let request = read_request(&mut stream);
            let (headers, body) = request.split_once("\r\n\r\n").unwrap();
            assert!(headers.starts_with("POST /api/v1/embeddings HTTP/1.1"));
            let payload: Value = serde_json::from_str(body).unwrap();
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
    (origin, server)
}

fn read_request(stream: &mut impl Read) -> String {
    let mut received = Vec::new();
    loop {
        let mut chunk = [0_u8; 1024];
        let count = stream.read(&mut chunk).unwrap();
        assert!(count > 0);
        received.extend_from_slice(&chunk[..count]);
        let Some(headers_end) = received.windows(4).position(|part| part == b"\r\n\r\n") else {
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
            return String::from_utf8(received).unwrap();
        }
    }
}

fn config(origin: String) -> OpenRouterProviderSessionConfig {
    OpenRouterProviderSessionConfig {
        origin,
        model: "vendor/model".to_owned(),
        revision: "revision".to_owned(),
        response_contract_version: "v1".to_owned(),
        capabilities: ProviderCapabilities::new([
            ProviderCapability::DocumentEmbeddings,
            ProviderCapability::QueryEmbeddings,
            ProviderCapability::CandidateReranking,
        ])
        .unwrap(),
        max_input_tokens: 10_000,
        chunking: None,
        wire_limits: OpenRouterWireLimits::default(),
        request_limits: ProviderRequestLimits::default(),
        execution_limits: ProviderExecutionLimits::default(),
        transport_timeout: Duration::from_secs(2),
        estimated_cost_microunits_per_token: 1,
    }
}

fn node(graph: &GraphForge, title: &str) -> [u8; 16] {
    *graph
        .add_node(
            "Paper",
            &HashMap::from([("title".to_owned(), PropValue::Str(title.to_owned()))]),
        )
        .unwrap()
        .uuid
        .as_bytes()
}

#[test]
fn configured_session_composes_embedding_query_rerank_and_advisory() {
    let (origin, server) = mock_openrouter();
    let session = OpenRouterProviderSession::new(config(origin), "test-secret".to_owned()).unwrap();
    assert_eq!(session.contract().provider(), "openrouter");
    assert_eq!(
        session.contract().tokenizer().count_class,
        TokenCountClass::Approximate
    );

    let graph = GraphForge::new(None).unwrap();
    let first = node(&graph, "First");
    let second = node(&graph, "Second");
    let request = ProviderEmbeddingPlanRequest {
        display_name: "semantic".to_owned(),
        label: "Paper".to_owned(),
        properties: vec!["title".to_owned()],
        contract: session.contract().clone(),
        dimensions: 2,
        normalization: ProviderEmbeddingNormalization::None,
        distance: ProviderEmbeddingDistance::Cosine,
        request_limits: ProviderRequestLimits::default(),
        batch_limits: ProviderBatchLimits::default(),
        execution_limits: ProviderExecutionLimits::default(),
        replace_alias: false,
    };
    let inspection = session.inspect_embedding_plan(&graph, &request).unwrap();
    assert_eq!(inspection.selected_nodes, 2);
    assert_eq!(inspection.token_count_class, TokenCountClass::Approximate);
    session.publish_embeddings(&graph, &request).unwrap();

    let canonical = FindOptions {
        label: Some("Paper".to_owned()),
        vector: Some(vec![1.0, 0.0]),
        limit: 2,
        space: Some("semantic".to_owned()),
        ..FindOptions::default()
    };
    let baseline = graph.find(canonical.clone()).unwrap();
    let emitted = session
        .find(
            &graph,
            FindExecutionOptions {
                find: canonical.clone(),
                omitted_reranker: Some(session.contract().clone()),
                advisory_policy: RerankAdvisoryPolicy::Emit,
                ..FindExecutionOptions::default()
            },
        )
        .unwrap();
    let (emitted_batch, diagnostics, status) = emitted.into_parts();
    assert_eq!(baseline, emitted_batch);
    assert!(status.is_none());
    assert!(matches!(
        diagnostics.as_slice(),
        [FindDiagnostic::RerankSuggested { provider, model }]
            if provider == "openrouter" && model == "vendor/model"
    ));
    let suppressed = session
        .find(
            &graph,
            FindExecutionOptions {
                find: canonical,
                omitted_reranker: Some(session.contract().clone()),
                advisory_policy: RerankAdvisoryPolicy::Suppress,
                ..FindExecutionOptions::default()
            },
        )
        .unwrap();
    let (suppressed_batch, diagnostics, _) = suppressed.into_parts();
    assert_eq!(baseline, suppressed_batch);
    assert!(diagnostics.is_empty());

    let explicit = session
        .find(
            &graph,
            FindExecutionOptions {
                find: FindOptions {
                    label: Some("Paper".to_owned()),
                    semantic_query: Some("meaning".to_owned()),
                    limit: 2,
                    space: Some("semantic".to_owned()),
                    ..FindOptions::default()
                },
                rerank: Some(FindRerankOptions {
                    query: "meaning".to_owned(),
                    properties: vec!["title".to_owned()],
                    candidate_depth: 2,
                    contract: session.contract().clone(),
                    request_limits: ProviderRequestLimits::default(),
                    execution_limits: ProviderExecutionLimits::default(),
                    failure_policy: RerankFailurePolicy::Error,
                }),
                omitted_reranker: None,
                advisory_policy: RerankAdvisoryPolicy::Emit,
            },
        )
        .unwrap();
    let (batch, diagnostics, status) = explicit.into_parts();
    assert!(diagnostics.is_empty());
    assert!(matches!(status, Some(RerankStatus::Reranked { .. })));
    let uuids = batch
        .column_by_name("node_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(uuids.value(0), second);
    assert_eq!(uuids.value(1), first);

    let initial_generation = graph
        .embedding_space(Some("semantic"))
        .unwrap()
        .active
        .unwrap()
        .generation_id;
    node(&graph, "Third");
    node(&graph, "Fourth");
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    let refreshed = loop {
        let inspection = graph.inspect_embedding_refresh(Some("semantic")).unwrap();
        if inspection.worker.succeeded == 1 {
            break inspection;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "proactive provider refresh did not complete"
        );
        thread::sleep(Duration::from_millis(25));
    };
    assert!(refreshed.worker.coalesced_notices >= 1);
    assert_eq!(refreshed.worker.failed, 0);
    assert!(matches!(
        refreshed.last_outcome.map(|outcome| outcome.status),
        Some(graphforge_api::EmbeddingRefreshOutcomeStatus::Succeeded)
    ));
    let active = graph
        .embedding_space(Some("semantic"))
        .unwrap()
        .active
        .unwrap();
    assert_eq!(active.vector_count, 4);
    assert_ne!(active.generation_id, initial_generation);
    server.join().unwrap();

    graph
        .add_node(
            "Other",
            &HashMap::from([("note".to_owned(), PropValue::Str("Unrelated".to_owned()))]),
        )
        .unwrap();
    graph
        .execute("MATCH (n:Other) SET n.note = 'still not selected'")
        .unwrap();
    thread::sleep(Duration::from_millis(600));
    let unrelated = graph.inspect_embedding_refresh(Some("semantic")).unwrap();
    assert_eq!(unrelated.worker.succeeded, 1);
    assert_eq!(unrelated.worker.failed, 0);

    graph
        .set_embedding_refresh_project_policy(graphforge_api::EmbeddingRefreshProjectPolicy {
            proactive: false,
            debounce: Duration::from_millis(500),
            max_concurrent_jobs: 2,
        })
        .unwrap();
    node(&graph, "Fifth");
    thread::sleep(Duration::from_millis(600));
    let disabled = graph.inspect_embedding_refresh(Some("semantic")).unwrap();
    assert_eq!(disabled.worker.succeeded, 0);
    assert_eq!(disabled.worker.failed, 0);
    assert!(matches!(
        disabled.freshness.map(|freshness| freshness.state),
        Some(graphforge_api::EmbeddingSpaceFreshnessState::SubstantiallyStale)
    ));
}

#[test]
fn configuration_failures_are_redacted_before_transport() {
    let error = OpenRouterProviderSession::new(
        config("https://example.com/path".to_owned()),
        "credential that must stay private".to_owned(),
    )
    .err()
    .expect("invalid origin must fail");
    let rendered = error.to_string();
    assert!(!rendered.contains("credential"));
    assert!(!rendered.contains("private"));
}

#[test]
fn proactive_provider_failure_preserves_the_active_generation() {
    let (origin, server) = mock_failing_refresh();
    let mut provider_config = config(origin);
    provider_config.execution_limits.retries = 0;
    let execution_limits = provider_config.execution_limits;
    let session =
        OpenRouterProviderSession::new(provider_config, "test-secret".to_owned()).unwrap();
    let graph = GraphForge::new(None).unwrap();
    node(&graph, "First");
    node(&graph, "Second");
    let request = ProviderEmbeddingPlanRequest {
        display_name: "semantic".to_owned(),
        label: "Paper".to_owned(),
        properties: vec!["title".to_owned()],
        contract: session.contract().clone(),
        dimensions: 2,
        normalization: ProviderEmbeddingNormalization::None,
        distance: ProviderEmbeddingDistance::Cosine,
        request_limits: ProviderRequestLimits::default(),
        batch_limits: ProviderBatchLimits::default(),
        execution_limits,
        replace_alias: false,
    };
    session.publish_embeddings(&graph, &request).unwrap();
    let original = graph
        .embedding_space(Some("semantic"))
        .unwrap()
        .active
        .unwrap();

    node(&graph, "Third");
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    let failed = loop {
        let inspection = graph.inspect_embedding_refresh(Some("semantic")).unwrap();
        if inspection.worker.failed == 1 {
            break inspection;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "proactive provider failure was not recorded"
        );
        thread::sleep(Duration::from_millis(25));
    };
    assert_eq!(failed.worker.succeeded, 0);
    assert!(matches!(
        failed.last_outcome.map(|outcome| outcome.status),
        Some(graphforge_api::EmbeddingRefreshOutcomeStatus::Failed(
            graphforge_api::EmbeddingRefreshFailureClass::Provider
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
