//! OpenRouter wire contract over an injectable byte transport.

use std::io::{self, Write};
use std::net::IpAddr;
use std::str::FromStr;
use std::time::Duration;

use graphforge_storage::SearchArtifactError;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{
    CandidateReranker, DocumentEmbeddingOutput, DocumentEmbeddingProvider,
    DocumentEmbeddingRequest, ProviderError, ProviderFailureClass, ProviderModelContract,
    ProviderResult, QueryEmbeddingOutput, QueryEmbeddingProvider, QueryEmbeddingRequest,
    RerankOutput, RerankRequest,
};

/// Fixed OpenRouter embeddings endpoint path.
pub const OPENROUTER_EMBEDDINGS_PATH: &str = "/api/v1/embeddings";
/// Fixed OpenRouter reranking endpoint path.
pub const OPENROUTER_RERANK_PATH: &str = "/api/v1/rerank";

/// One fixed POST endpoint. Callers cannot inject origins or paths.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpenRouterEndpoint {
    /// Document or query embeddings.
    Embeddings,
    /// Candidate reranking.
    Rerank,
}

impl OpenRouterEndpoint {
    /// Fixed path under the transport's configured OpenRouter origin.
    #[must_use]
    pub const fn path(self) -> &'static str {
        match self {
            Self::Embeddings => OPENROUTER_EMBEDDINGS_PATH,
            Self::Rerank => OPENROUTER_RERANK_PATH,
        }
    }
}

/// Independent serialized wire byte bounds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OpenRouterWireLimits {
    /// Maximum serialized request bytes.
    pub request_bytes: usize,
    /// Maximum response bytes accepted before parsing.
    pub response_bytes: usize,
}

impl Default for OpenRouterWireLimits {
    fn default() -> Self {
        Self {
            request_bytes: 16 * 1024 * 1024,
            response_bytes: 64 * 1024 * 1024,
        }
    }
}

/// Payload-free failures produced below the provider adapter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpenRouterTransportError {
    /// Caller cancellation stopped transport work.
    Cancelled,
    /// The transport deadline elapsed.
    Timeout,
    /// The response exceeded its configured byte bound.
    ResourceExhausted,
    /// The configured transport failed without an HTTP response.
    Transport,
}

/// Ephemeral POST request. It deliberately omits `Debug`.
pub struct OpenRouterTransportRequest<'a> {
    endpoint: OpenRouterEndpoint,
    bearer_credential: &'a str,
    body: &'a [u8],
    response_byte_limit: usize,
}

impl OpenRouterTransportRequest<'_> {
    /// Fixed endpoint identity.
    #[must_use]
    pub const fn endpoint(&self) -> OpenRouterEndpoint {
        self.endpoint
    }

    /// Borrowed credential for the transport's Authorization header.
    #[must_use]
    pub const fn bearer_credential(&self) -> &str {
        self.bearer_credential
    }

    /// Serialized JSON body, exposed only to the configured transport.
    #[must_use]
    pub const fn body(&self) -> &[u8] {
        self.body
    }

    /// Maximum response bytes the transport may read before aborting.
    #[must_use]
    pub const fn response_byte_limit(&self) -> usize {
        self.response_byte_limit
    }
}

/// Owned HTTP response. It deliberately omits `Debug`.
pub struct OpenRouterTransportResponse {
    status: u16,
    body: Vec<u8>,
}

impl OpenRouterTransportResponse {
    /// Construct one status/body response from the injected transport.
    #[must_use]
    pub fn new(status: u16, body: Vec<u8>) -> Self {
        Self { status, body }
    }
}

/// Injectable synchronous byte transport; implementations own origin and HTTP details.
pub trait OpenRouterTransport {
    /// POST one fixed-endpoint request without retaining its borrowed secrets.
    fn post(
        &mut self,
        request: OpenRouterTransportRequest<'_>,
        checkpoint: &mut dyn FnMut() -> ProviderResult<()>,
    ) -> Result<OpenRouterTransportResponse, OpenRouterTransportError>;
}

/// Bounded synchronous HTTP transport for one explicit OpenRouter origin.
///
/// The transport disables redirects and ambient proxies. It contains no
/// credential; bearer material exists only in each borrowed request.
pub struct OpenRouterHttpTransport {
    origin: String,
    agent: ureq::Agent,
}

impl OpenRouterHttpTransport {
    /// Validate an HTTPS origin (or loopback HTTP for deterministic local tests).
    ///
    /// # Errors
    /// Rejects a zero timeout, credentials/path/query in the origin, unsupported
    /// schemes, and non-loopback cleartext origins.
    pub fn new(origin: &str, timeout: Duration) -> Result<Self, SearchArtifactError> {
        if timeout.is_zero() {
            return Err(invalid_origin("timeout must be non-zero"));
        }
        let uri = ureq::http::Uri::from_str(origin)
            .map_err(|_| invalid_origin("must be an absolute HTTP(S) origin"))?;
        let scheme = uri
            .scheme_str()
            .ok_or_else(|| invalid_origin("must include a scheme"))?;
        let authority = uri
            .authority()
            .ok_or_else(|| invalid_origin("must include a host"))?;
        if authority.as_str().contains('@') {
            return Err(invalid_origin("must not include credentials"));
        }
        if uri.path() != "/" || uri.query().is_some() {
            return Err(invalid_origin("must not include a path or query"));
        }
        let host = uri
            .host()
            .ok_or_else(|| invalid_origin("must include a host"))?;
        let host = host.trim_matches(['[', ']']);
        let loopback = host.eq_ignore_ascii_case("localhost")
            || host
                .parse::<IpAddr>()
                .is_ok_and(|address| address.is_loopback());
        if scheme != "https" && !(scheme == "http" && loopback) {
            return Err(invalid_origin("must use HTTPS unless the host is loopback"));
        }
        let config = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .max_redirects(0)
            .proxy(None)
            .timeout_global(Some(timeout))
            .build();
        Ok(Self {
            origin: origin.trim_end_matches('/').to_owned(),
            agent: ureq::Agent::new_with_config(config),
        })
    }
}

impl OpenRouterTransport for OpenRouterHttpTransport {
    fn post(
        &mut self,
        request: OpenRouterTransportRequest<'_>,
        checkpoint: &mut dyn FnMut() -> ProviderResult<()>,
    ) -> Result<OpenRouterTransportResponse, OpenRouterTransportError> {
        checkpoint().map_err(|_| OpenRouterTransportError::Cancelled)?;
        let authorization = format!("Bearer {}", request.bearer_credential());
        let mut response = self
            .agent
            .post(format!("{}{}", self.origin, request.endpoint().path()))
            .header("authorization", authorization)
            .header("content-type", "application/json")
            .send(request.body())
            .map_err(|error| map_http_error(&error))?;
        checkpoint().map_err(|_| OpenRouterTransportError::Cancelled)?;
        let status = response.status().as_u16();
        let limit = u64::try_from(request.response_byte_limit())
            .map_err(|_| OpenRouterTransportError::ResourceExhausted)?;
        let body = response
            .body_mut()
            .with_config()
            .limit(limit)
            .read_to_vec()
            .map_err(|error| map_http_error(&error))?;
        checkpoint().map_err(|_| OpenRouterTransportError::Cancelled)?;
        Ok(OpenRouterTransportResponse::new(status, body))
    }
}

/// Owned OpenRouter adapter suitable for process-local provider registries.
///
/// Credential and transport are intentionally non-debuggable and never enter
/// the public provider/model contract.
pub struct OpenRouterOwnedAdapter<T: OpenRouterTransport> {
    contract: ProviderModelContract,
    bearer_credential: String,
    transport: T,
    limits: OpenRouterWireLimits,
}

impl<T: OpenRouterTransport> OpenRouterOwnedAdapter<T> {
    /// Validate and retain one exact owned provider session.
    ///
    /// # Errors
    /// Rejects a non-OpenRouter contract, malformed credential, or zero limit.
    pub fn new(
        contract: ProviderModelContract,
        bearer_credential: String,
        mut transport: T,
        limits: OpenRouterWireLimits,
    ) -> ProviderResult<Self> {
        OpenRouterAdapter::new(&contract, &bearer_credential, &mut transport, limits)?;
        Ok(Self {
            contract,
            bearer_credential,
            transport,
            limits,
        })
    }

    fn borrowed(&mut self) -> ProviderResult<OpenRouterAdapter<'_, T>> {
        OpenRouterAdapter::new(
            &self.contract,
            &self.bearer_credential,
            &mut self.transport,
            self.limits,
        )
    }
}

impl<T: OpenRouterTransport> DocumentEmbeddingProvider for OpenRouterOwnedAdapter<T> {
    fn contract(&self) -> &ProviderModelContract {
        &self.contract
    }

    fn provide_documents(
        &mut self,
        request: &DocumentEmbeddingRequest<'_>,
        checkpoint: &mut dyn FnMut() -> ProviderResult<()>,
    ) -> ProviderResult<Vec<DocumentEmbeddingOutput>> {
        self.borrowed()?.provide_documents(request, checkpoint)
    }
}

impl<T: OpenRouterTransport> QueryEmbeddingProvider for OpenRouterOwnedAdapter<T> {
    fn contract(&self) -> &ProviderModelContract {
        &self.contract
    }

    fn provide_query(
        &mut self,
        request: &QueryEmbeddingRequest<'_>,
        checkpoint: &mut dyn FnMut() -> ProviderResult<()>,
    ) -> ProviderResult<QueryEmbeddingOutput> {
        self.borrowed()?.provide_query(request, checkpoint)
    }
}

impl<T: OpenRouterTransport> CandidateReranker for OpenRouterOwnedAdapter<T> {
    fn contract(&self) -> &ProviderModelContract {
        &self.contract
    }

    fn provide_rerank(
        &mut self,
        request: &RerankRequest<'_>,
        checkpoint: &mut dyn FnMut() -> ProviderResult<()>,
    ) -> ProviderResult<Vec<RerankOutput>> {
        self.borrowed()?.provide_rerank(request, checkpoint)
    }
}

/// OpenRouter provider adapter. Credential and transport are borrowed and non-debuggable.
pub struct OpenRouterAdapter<'a, T: OpenRouterTransport> {
    contract: ProviderModelContract,
    bearer_credential: &'a str,
    transport: &'a mut T,
    limits: OpenRouterWireLimits,
}

impl<'a, T: OpenRouterTransport> OpenRouterAdapter<'a, T> {
    /// Validate the exact provider, credential, and wire limits.
    ///
    /// # Errors
    /// Rejects non-OpenRouter contracts, malformed credentials, or zero limits.
    pub fn new(
        contract: &ProviderModelContract,
        bearer_credential: &'a str,
        transport: &'a mut T,
        limits: OpenRouterWireLimits,
    ) -> ProviderResult<Self> {
        if contract.provider() != "openrouter" {
            return Err(failure(contract, ProviderFailureClass::InvalidRequest));
        }
        if bearer_credential.is_empty()
            || !bearer_credential
                .bytes()
                .all(|byte| byte.is_ascii_graphic())
        {
            return Err(failure(contract, ProviderFailureClass::Authentication));
        }
        if limits.request_bytes == 0 || limits.response_bytes == 0 {
            return Err(failure(contract, ProviderFailureClass::InvalidRequest));
        }
        Ok(Self {
            contract: contract.clone(),
            bearer_credential,
            transport,
            limits,
        })
    }

    fn invoke<Request: Serialize, Response: DeserializeOwned>(
        &mut self,
        endpoint: OpenRouterEndpoint,
        request: &Request,
        checkpoint: &mut dyn FnMut() -> ProviderResult<()>,
    ) -> ProviderResult<Response> {
        checkpoint()?;
        let body = serialize_bounded(&self.contract, request, self.limits.request_bytes)?;
        checkpoint()?;
        let response = self
            .transport
            .post(
                OpenRouterTransportRequest {
                    endpoint,
                    bearer_credential: self.bearer_credential,
                    body: &body,
                    response_byte_limit: self.limits.response_bytes,
                },
                checkpoint,
            )
            .map_err(|error| map_transport(&self.contract, error))?;
        checkpoint()?;
        if response.body.len() > self.limits.response_bytes {
            return Err(failure(
                &self.contract,
                ProviderFailureClass::ResourceExhausted,
            ));
        }
        require_success(&self.contract, response.status)?;
        let parsed = serde_json::from_slice(&response.body)
            .map_err(|_| failure(&self.contract, ProviderFailureClass::MalformedResponse))?;
        checkpoint()?;
        Ok(parsed)
    }

    fn require_request(&self, contract: &ProviderModelContract) -> ProviderResult<()> {
        if contract == &self.contract {
            Ok(())
        } else {
            Err(failure(
                &self.contract,
                ProviderFailureClass::InvalidRequest,
            ))
        }
    }
}

impl<T: OpenRouterTransport> DocumentEmbeddingProvider for OpenRouterAdapter<'_, T> {
    fn contract(&self) -> &ProviderModelContract {
        &self.contract
    }

    fn provide_documents(
        &mut self,
        request: &DocumentEmbeddingRequest<'_>,
        checkpoint: &mut dyn FnMut() -> ProviderResult<()>,
    ) -> ProviderResult<Vec<DocumentEmbeddingOutput>> {
        self.require_request(request.contract())?;
        let model = self.contract.model().to_owned();
        let payload = EmbeddingRequestBody {
            model: &model,
            input: request
                .inputs()
                .iter()
                .map(|input| input.text)
                .collect::<Vec<_>>(),
            provider: Routing::STRICT,
        };
        let response: EmbeddingResponse =
            self.invoke(OpenRouterEndpoint::Embeddings, &payload, checkpoint)?;
        validate_model(&self.contract, &response.model)?;
        if response.data.len() != request.inputs().len() {
            return Err(malformed(&self.contract));
        }
        let mut outputs = Vec::with_capacity(response.data.len());
        for (index, (input, item)) in request.inputs().iter().zip(response.data).enumerate() {
            checkpoint()?;
            if item.index != index {
                return Err(malformed(&self.contract));
            }
            outputs.push(DocumentEmbeddingOutput {
                node_uuid: input.node_uuid,
                vector: validate_embedding(&self.contract, item.embedding)?,
            });
        }
        Ok(outputs)
    }
}

impl<T: OpenRouterTransport> QueryEmbeddingProvider for OpenRouterAdapter<'_, T> {
    fn contract(&self) -> &ProviderModelContract {
        &self.contract
    }

    fn provide_query(
        &mut self,
        request: &QueryEmbeddingRequest<'_>,
        checkpoint: &mut dyn FnMut() -> ProviderResult<()>,
    ) -> ProviderResult<QueryEmbeddingOutput> {
        self.require_request(request.contract())?;
        let model = self.contract.model().to_owned();
        let payload = EmbeddingRequestBody {
            model: &model,
            input: request.text(),
            provider: Routing::STRICT,
        };
        let response: EmbeddingResponse =
            self.invoke(OpenRouterEndpoint::Embeddings, &payload, checkpoint)?;
        validate_model(&self.contract, &response.model)?;
        if response.data.len() != 1 || response.data[0].index != 0 {
            return Err(malformed(&self.contract));
        }
        Ok(QueryEmbeddingOutput {
            vector: validate_embedding(
                &self.contract,
                response
                    .data
                    .into_iter()
                    .next()
                    .expect("length checked")
                    .embedding,
            )?,
        })
    }
}

impl<T: OpenRouterTransport> CandidateReranker for OpenRouterAdapter<'_, T> {
    fn contract(&self) -> &ProviderModelContract {
        &self.contract
    }

    fn provide_rerank(
        &mut self,
        request: &RerankRequest<'_>,
        checkpoint: &mut dyn FnMut() -> ProviderResult<()>,
    ) -> ProviderResult<Vec<RerankOutput>> {
        self.require_request(request.contract())?;
        let model = self.contract.model().to_owned();
        let payload = RerankRequestBody {
            model: &model,
            query: request.query(),
            documents: request
                .candidates()
                .iter()
                .map(|candidate| candidate.text)
                .collect(),
            top_n: request.candidates().len(),
            provider: Routing::STRICT,
        };
        let response: RerankResponse =
            self.invoke(OpenRouterEndpoint::Rerank, &payload, checkpoint)?;
        validate_model(&self.contract, &response.model)?;
        if response.results.len() != request.candidates().len() {
            return Err(malformed(&self.contract));
        }
        let mut scores = vec![None; request.candidates().len()];
        for result in response.results {
            checkpoint()?;
            let Some(slot) = scores.get_mut(result.index) else {
                return Err(malformed(&self.contract));
            };
            if slot.replace(result.relevance_score).is_some() {
                return Err(malformed(&self.contract));
            }
        }
        request
            .candidates()
            .iter()
            .zip(scores)
            .map(|(candidate, score)| {
                Ok(RerankOutput {
                    node_uuid: candidate.node_uuid,
                    score: score.ok_or_else(|| malformed(&self.contract))?,
                })
            })
            .collect()
    }
}

#[derive(Serialize)]
struct EmbeddingRequestBody<'a, T: Serialize> {
    model: &'a str,
    input: T,
    provider: Routing,
}

#[derive(Serialize)]
struct RerankRequestBody<'a> {
    model: &'a str,
    query: &'a str,
    documents: Vec<&'a str>,
    top_n: usize,
    provider: Routing,
}

#[derive(Clone, Copy, Serialize)]
struct Routing {
    allow_fallbacks: bool,
    data_collection: &'static str,
}

impl Routing {
    const STRICT: Self = Self {
        allow_fallbacks: false,
        data_collection: "deny",
    };
}

#[derive(Deserialize)]
struct EmbeddingResponse {
    model: String,
    data: Vec<EmbeddingResponseItem>,
}

#[derive(Deserialize)]
struct EmbeddingResponseItem {
    index: usize,
    embedding: Vec<f32>,
}

#[derive(Deserialize)]
struct RerankResponse {
    model: String,
    results: Vec<RerankResponseItem>,
}

#[derive(Deserialize)]
struct RerankResponseItem {
    index: usize,
    relevance_score: f64,
}

struct BoundedBody {
    bytes: Vec<u8>,
    limit: usize,
    exhausted: bool,
}

impl Write for BoundedBody {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        let Some(total) = self.bytes.len().checked_add(input.len()) else {
            self.exhausted = true;
            return Err(io::Error::other("request byte limit exceeded"));
        };
        if total > self.limit {
            self.exhausted = true;
            return Err(io::Error::other("request byte limit exceeded"));
        }
        self.bytes.extend_from_slice(input);
        Ok(input.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn serialize_bounded<T: Serialize>(
    contract: &ProviderModelContract,
    value: &T,
    limit: usize,
) -> ProviderResult<Vec<u8>> {
    let mut output = BoundedBody {
        bytes: Vec::new(),
        limit,
        exhausted: false,
    };
    if serde_json::to_writer(&mut output, value).is_err() {
        let class = if output.exhausted {
            ProviderFailureClass::ResourceExhausted
        } else {
            ProviderFailureClass::InvalidRequest
        };
        return Err(failure(contract, class));
    }
    Ok(output.bytes)
}

fn validate_model(contract: &ProviderModelContract, model: &str) -> ProviderResult<()> {
    if model == contract.model() {
        Ok(())
    } else {
        Err(malformed(contract))
    }
}

fn validate_embedding(
    contract: &ProviderModelContract,
    vector: Vec<f32>,
) -> ProviderResult<Vec<f32>> {
    if vector.is_empty() || vector.iter().any(|value| !value.is_finite()) {
        Err(malformed(contract))
    } else {
        Ok(vector)
    }
}

fn require_success(contract: &ProviderModelContract, status: u16) -> ProviderResult<()> {
    let class = match status {
        200..=299 => return Ok(()),
        400 | 405 | 409 | 422 => ProviderFailureClass::InvalidRequest,
        401 | 403 => ProviderFailureClass::Authentication,
        402 | 413 => ProviderFailureClass::ResourceExhausted,
        404 => ProviderFailureClass::UnsupportedCapability,
        408 | 504 => ProviderFailureClass::Timeout,
        _ => ProviderFailureClass::ProviderRejected,
    };
    Err(failure(contract, class))
}

fn map_transport(
    contract: &ProviderModelContract,
    error: OpenRouterTransportError,
) -> ProviderError {
    let class = match error {
        OpenRouterTransportError::Cancelled => ProviderFailureClass::Cancelled,
        OpenRouterTransportError::Timeout => ProviderFailureClass::Timeout,
        OpenRouterTransportError::ResourceExhausted => ProviderFailureClass::ResourceExhausted,
        OpenRouterTransportError::Transport => ProviderFailureClass::Transport,
    };
    failure(contract, class)
}

fn map_http_error(error: &ureq::Error) -> OpenRouterTransportError {
    match error {
        ureq::Error::Timeout(_) => OpenRouterTransportError::Timeout,
        ureq::Error::BodyExceedsLimit(_) => OpenRouterTransportError::ResourceExhausted,
        _ => OpenRouterTransportError::Transport,
    }
}

fn invalid_origin(reason: impl Into<String>) -> SearchArtifactError {
    SearchArtifactError::InvalidSelector {
        field: "OpenRouter origin",
        reason: reason.into(),
    }
}

fn malformed(contract: &ProviderModelContract) -> ProviderError {
    failure(contract, ProviderFailureClass::MalformedResponse)
}

fn failure(contract: &ProviderModelContract, class: ProviderFailureClass) -> ProviderError {
    ProviderError::new(contract, class)
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::io::Read;
    use std::net::TcpListener;
    use std::thread;

    use graphforge_storage::{TokenCountClass, TokenizerIdentity};
    use serde_json::{Value, json};

    use crate::{
        DocumentEmbeddingInput, ProviderCapabilities, ProviderCapability, ProviderRequestLimits,
        QueryEmbeddingRequest, RerankCandidate,
    };

    use super::*;

    struct ExpectedCall {
        endpoint: OpenRouterEndpoint,
        body: Value,
        status: u16,
        response: Vec<u8>,
    }

    struct FakeTransport {
        expected_credential: String,
        calls: VecDeque<ExpectedCall>,
    }

    impl OpenRouterTransport for FakeTransport {
        fn post(
            &mut self,
            request: OpenRouterTransportRequest<'_>,
            checkpoint: &mut dyn FnMut() -> ProviderResult<()>,
        ) -> Result<OpenRouterTransportResponse, OpenRouterTransportError> {
            checkpoint().map_err(|_| OpenRouterTransportError::Cancelled)?;
            let call = self.calls.pop_front().expect("unexpected call");
            assert_eq!(request.endpoint(), call.endpoint);
            assert_eq!(request.endpoint().path(), call.endpoint.path());
            assert_eq!(request.bearer_credential(), self.expected_credential);
            assert!(request.response_byte_limit() > 0);
            assert_eq!(
                serde_json::from_slice::<Value>(request.body()).unwrap(),
                call.body
            );
            Ok(OpenRouterTransportResponse::new(call.status, call.response))
        }
    }

    fn contract() -> ProviderModelContract {
        ProviderModelContract::remote(
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
                identifier: "provider-tokenizer".into(),
                version: "1".into(),
                count_class: TokenCountClass::ProviderReported,
                max_input_tokens: 16,
                normalization: "nfc".into(),
            },
            None,
        )
        .unwrap()
    }

    fn call(endpoint: OpenRouterEndpoint, body: Value, response: Value) -> ExpectedCall {
        ExpectedCall {
            endpoint,
            body,
            status: 200,
            response: serde_json::to_vec(&response).unwrap(),
        }
    }

    #[test]
    fn fixed_endpoints_and_bounded_serialization_are_pure_and_exact() {
        assert_eq!(
            OpenRouterEndpoint::Embeddings.path(),
            OPENROUTER_EMBEDDINGS_PATH
        );
        assert_eq!(OpenRouterEndpoint::Rerank.path(), OPENROUTER_RERANK_PATH);
        let contract = contract();
        assert_eq!(
            serialize_bounded(&contract, &json!({"value": 1}), 64).unwrap(),
            br#"{"value":1}"#
        );
        assert_eq!(
            serialize_bounded(&contract, &json!({"value": 1}), 2)
                .unwrap_err()
                .class(),
            ProviderFailureClass::ResourceExhausted
        );
        let mut body = BoundedBody {
            bytes: Vec::new(),
            limit: 2,
            exhausted: false,
        };
        assert_eq!(body.write(b"ok").unwrap(), 2);
        assert!(body.write(b"!").is_err());
        assert!(body.exhausted);
        body.flush().unwrap();
    }

    #[test]
    fn three_capabilities_use_fixed_strict_wire_contracts_and_canonical_identity() {
        let contract = contract();
        let credential = "private-token";
        let routing = json!({"allow_fallbacks": false, "data_collection": "deny"});
        let transport = FakeTransport {
            expected_credential: credential.into(),
            calls: VecDeque::from([
                call(
                    OpenRouterEndpoint::Embeddings,
                    json!({"model":"vendor/model","input":["first","second"],"provider":routing}),
                    json!({"model":"vendor/model","data":[
                        {"index":0,"embedding":[1.0,0.0]},
                        {"index":1,"embedding":[0.0,1.0]}
                    ]}),
                ),
                call(
                    OpenRouterEndpoint::Embeddings,
                    json!({"model":"vendor/model","input":"query","provider":routing}),
                    json!({"model":"vendor/model","data":[{"index":0,"embedding":[0.5,0.5]}]}),
                ),
                call(
                    OpenRouterEndpoint::Rerank,
                    json!({
                        "model":"vendor/model","query":"query","documents":["first","second"],
                        "top_n":2,"provider":routing
                    }),
                    json!({"model":"vendor/model","results":[
                        {"index":1,"relevance_score":0.9},
                        {"index":0,"relevance_score":0.4}
                    ]}),
                ),
            ]),
        };
        let mut adapter = OpenRouterOwnedAdapter::new(
            contract.clone(),
            credential.to_owned(),
            transport,
            OpenRouterWireLimits::default(),
        )
        .unwrap();
        let inputs = [
            DocumentEmbeddingInput {
                node_uuid: [1; 16],
                text: "first",
                token_count: 1,
            },
            DocumentEmbeddingInput {
                node_uuid: [2; 16],
                text: "second",
                token_count: 1,
            },
        ];
        let documents =
            DocumentEmbeddingRequest::new(&contract, &inputs, ProviderRequestLimits::default())
                .unwrap();
        let document_outputs = adapter
            .provide_documents(&documents, &mut || Ok(()))
            .unwrap();
        assert_eq!(document_outputs[0].node_uuid, [1; 16]);
        assert_eq!(document_outputs[1].node_uuid, [2; 16]);

        let query =
            QueryEmbeddingRequest::new(&contract, "query", 1, ProviderRequestLimits::default())
                .unwrap();
        assert_eq!(
            adapter
                .provide_query(&query, &mut || Ok(()))
                .unwrap()
                .vector,
            [0.5, 0.5]
        );

        let candidates = [
            RerankCandidate {
                node_uuid: [1; 16],
                retrieval_rank: 1,
                text: "first",
                token_count: 1,
            },
            RerankCandidate {
                node_uuid: [2; 16],
                retrieval_rank: 2,
                text: "second",
                token_count: 1,
            },
        ];
        let rerank = RerankRequest::new(
            &contract,
            "query",
            1,
            &candidates,
            ProviderRequestLimits::default(),
        )
        .unwrap();
        let reranked = adapter.provide_rerank(&rerank, &mut || Ok(())).unwrap();
        assert_eq!(reranked[0].node_uuid, [1; 16]);
        assert_eq!(reranked[0].score, 0.4);
        assert_eq!(reranked[1].node_uuid, [2; 16]);
        assert_eq!(reranked[1].score, 0.9);
        assert!(adapter.transport.calls.is_empty());
    }

    #[test]
    fn http_transport_restricts_origins_headers_paths_and_response_bytes() {
        for invalid in [
            "http://example.com",
            "https://example.com/path",
            "https://user@example.com",
        ] {
            assert!(OpenRouterHttpTransport::new(invalid, Duration::from_secs(1)).is_err());
        }
        assert!(OpenRouterHttpTransport::new("https://example.com", Duration::ZERO).is_err());

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut received = Vec::new();
            loop {
                let mut chunk = [0_u8; 512];
                let count = stream.read(&mut chunk).unwrap();
                assert!(count > 0);
                received.extend_from_slice(&chunk[..count]);
                let Some(headers_end) = received.windows(4).position(|part| part == b"\r\n\r\n")
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
                    break;
                }
            }
            let request = String::from_utf8_lossy(&received).to_ascii_lowercase();
            assert!(request.starts_with("post /api/v1/embeddings http/1.1\r\n"));
            assert!(request.contains("authorization: bearer private-token\r\n"));
            assert!(request.contains("content-type: application/json\r\n"));
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\n12345",
                )
                .unwrap();
        });
        let mut transport = OpenRouterHttpTransport::new(&origin, Duration::from_secs(2)).unwrap();
        let error = match transport.post(
            OpenRouterTransportRequest {
                endpoint: OpenRouterEndpoint::Embeddings,
                bearer_credential: "private-token",
                body: b"{}",
                response_byte_limit: 4,
            },
            &mut || Ok(()),
        ) {
            Err(error) => error,
            Ok(_) => panic!("oversized response unexpectedly succeeded"),
        };
        assert_eq!(error, OpenRouterTransportError::ResourceExhausted);
        assert_eq!(
            map_transport(&contract(), error).class(),
            ProviderFailureClass::ResourceExhausted
        );
        server.join().unwrap();
    }

    #[test]
    fn response_status_shape_model_index_and_bytes_are_redacted_and_atomic() {
        let contract = contract();
        let input = [DocumentEmbeddingInput {
            node_uuid: [1; 16],
            text: "secret source",
            token_count: 2,
        }];
        let request =
            DocumentEmbeddingRequest::new(&contract, &input, ProviderRequestLimits::default())
                .unwrap();
        let cases = [
            (
                401,
                b"secret response".to_vec(),
                ProviderFailureClass::Authentication,
            ),
            (
                429,
                b"secret response".to_vec(),
                ProviderFailureClass::ProviderRejected,
            ),
            (
                200,
                b"not-json".to_vec(),
                ProviderFailureClass::MalformedResponse,
            ),
            (
                200,
                serde_json::to_vec(
                    &json!({"model":"other/model","data":[{"index":0,"embedding":[1.0]}]}),
                )
                .unwrap(),
                ProviderFailureClass::MalformedResponse,
            ),
            (
                200,
                serde_json::to_vec(
                    &json!({"model":"vendor/model","data":[{"index":1,"embedding":[1.0]}]}),
                )
                .unwrap(),
                ProviderFailureClass::MalformedResponse,
            ),
            (
                200,
                br#"{"model":"vendor/model","data":[{"index":0,"embedding":[1e100]}]}"#.to_vec(),
                ProviderFailureClass::MalformedResponse,
            ),
        ];
        for (status, response, expected) in cases {
            let mut transport = FakeTransport {
                expected_credential: "private-token".into(),
                calls: VecDeque::from([ExpectedCall {
                    endpoint: OpenRouterEndpoint::Embeddings,
                    body: json!({
                        "model":"vendor/model","input":["secret source"],
                        "provider":{"allow_fallbacks":false,"data_collection":"deny"}
                    }),
                    status,
                    response,
                }]),
            };
            let error = OpenRouterAdapter::new(
                &contract,
                "private-token",
                &mut transport,
                OpenRouterWireLimits::default(),
            )
            .unwrap()
            .provide_documents(&request, &mut || Ok(()))
            .err()
            .unwrap();
            assert_eq!(error.class(), expected);
            let rendered = format!("{error:?} {error}");
            assert!(!rendered.contains("private-token"));
            assert!(!rendered.contains("secret source"));
            assert!(!rendered.contains("secret response"));
        }

        let response = serde_json::to_vec(&json!({
            "model":"vendor/model","data":[{"index":0,"embedding":[1.0]}]
        }))
        .unwrap();
        let mut transport = FakeTransport {
            expected_credential: "private-token".into(),
            calls: VecDeque::from([ExpectedCall {
                endpoint: OpenRouterEndpoint::Embeddings,
                body: json!({
                    "model":"vendor/model","input":["secret source"],
                    "provider":{"allow_fallbacks":false,"data_collection":"deny"}
                }),
                status: 200,
                response: response.clone(),
            }]),
        };
        let error = OpenRouterAdapter::new(
            &contract,
            "private-token",
            &mut transport,
            OpenRouterWireLimits {
                request_bytes: usize::MAX,
                response_bytes: response.len() - 1,
            },
        )
        .unwrap()
        .provide_documents(&request, &mut || Ok(()))
        .err()
        .unwrap();
        assert_eq!(error.class(), ProviderFailureClass::ResourceExhausted);
    }

    #[test]
    fn construction_transport_and_cancellation_fail_without_payloads() {
        let contract = contract();
        let mut transport = FakeTransport {
            expected_credential: String::new(),
            calls: VecDeque::new(),
        };
        assert_eq!(
            OpenRouterAdapter::new(
                &contract,
                "",
                &mut transport,
                OpenRouterWireLimits::default(),
            )
            .err()
            .unwrap()
            .class(),
            ProviderFailureClass::Authentication
        );
        for credential in ["contains space", "line\nbreak"] {
            assert_eq!(
                OpenRouterAdapter::new(
                    &contract,
                    credential,
                    &mut transport,
                    OpenRouterWireLimits::default(),
                )
                .err()
                .unwrap()
                .class(),
                ProviderFailureClass::Authentication
            );
        }
        for limits in [
            OpenRouterWireLimits {
                request_bytes: 0,
                response_bytes: 1,
            },
            OpenRouterWireLimits {
                request_bytes: 1,
                response_bytes: 0,
            },
        ] {
            assert_eq!(
                OpenRouterAdapter::new(&contract, "token", &mut transport, limits)
                    .err()
                    .unwrap()
                    .class(),
                ProviderFailureClass::InvalidRequest
            );
        }

        struct FailingTransport(OpenRouterTransportError);
        impl OpenRouterTransport for FailingTransport {
            fn post(
                &mut self,
                _: OpenRouterTransportRequest<'_>,
                _: &mut dyn FnMut() -> ProviderResult<()>,
            ) -> Result<OpenRouterTransportResponse, OpenRouterTransportError> {
                Err(self.0)
            }
        }
        let query =
            QueryEmbeddingRequest::new(&contract, "query", 1, ProviderRequestLimits::default())
                .unwrap();
        for (wire, class) in [
            (
                OpenRouterTransportError::Transport,
                ProviderFailureClass::Transport,
            ),
            (
                OpenRouterTransportError::Timeout,
                ProviderFailureClass::Timeout,
            ),
            (
                OpenRouterTransportError::Cancelled,
                ProviderFailureClass::Cancelled,
            ),
        ] {
            let mut transport = FailingTransport(wire);
            let error = OpenRouterAdapter::new(
                &contract,
                "private-token",
                &mut transport,
                OpenRouterWireLimits::default(),
            )
            .unwrap()
            .provide_query(&query, &mut || Ok(()))
            .err()
            .unwrap();
            assert_eq!(error.class(), class);
        }
        let mut transport = FailingTransport(OpenRouterTransportError::Transport);
        let error = OpenRouterAdapter::new(
            &contract,
            "private-token",
            &mut transport,
            OpenRouterWireLimits::default(),
        )
        .unwrap()
        .provide_query(&query, &mut || {
            Err(failure(&contract, ProviderFailureClass::Cancelled))
        })
        .err()
        .unwrap();
        assert_eq!(error.class(), ProviderFailureClass::Cancelled);
    }

    #[test]
    fn adapter_rejects_foreign_provider_and_request_contracts_before_transport() {
        let contract = contract();
        let foreign = ProviderModelContract::remote(
            Some("another-provider"),
            "vendor/model",
            "revision",
            "v1",
            ProviderCapabilities::new([
                ProviderCapability::DocumentEmbeddings,
                ProviderCapability::QueryEmbeddings,
                ProviderCapability::CandidateReranking,
            ])
            .unwrap(),
            contract.tokenizer().clone(),
            None,
        )
        .unwrap();
        let mut transport = FakeTransport {
            expected_credential: "token".into(),
            calls: VecDeque::new(),
        };
        assert_eq!(
            OpenRouterAdapter::new(
                &foreign,
                "token",
                &mut transport,
                OpenRouterWireLimits::default(),
            )
            .err()
            .unwrap()
            .class(),
            ProviderFailureClass::InvalidRequest
        );

        let input = [DocumentEmbeddingInput {
            node_uuid: [1; 16],
            text: "document",
            token_count: 1,
        }];
        let documents =
            DocumentEmbeddingRequest::new(&foreign, &input, ProviderRequestLimits::default())
                .unwrap();
        let query =
            QueryEmbeddingRequest::new(&foreign, "query", 1, ProviderRequestLimits::default())
                .unwrap();
        let candidates = [RerankCandidate {
            node_uuid: [1; 16],
            retrieval_rank: 1,
            text: "document",
            token_count: 1,
        }];
        let rerank = RerankRequest::new(
            &foreign,
            "query",
            1,
            &candidates,
            ProviderRequestLimits::default(),
        )
        .unwrap();
        let mut adapter = OpenRouterAdapter::new(
            &contract,
            "token",
            &mut transport,
            OpenRouterWireLimits::default(),
        )
        .unwrap();
        assert_eq!(DocumentEmbeddingProvider::contract(&adapter), &contract);
        assert_eq!(QueryEmbeddingProvider::contract(&adapter), &contract);
        assert_eq!(CandidateReranker::contract(&adapter), &contract);
        let document_error = match adapter.provide_documents(&documents, &mut || Ok(())) {
            Ok(_) => panic!("foreign document contract must fail"),
            Err(error) => error,
        };
        let query_error = match adapter.provide_query(&query, &mut || Ok(())) {
            Ok(_) => panic!("foreign query contract must fail"),
            Err(error) => error,
        };
        let rerank_error = match adapter.provide_rerank(&rerank, &mut || Ok(())) {
            Ok(_) => panic!("foreign rerank contract must fail"),
            Err(error) => error,
        };
        for error in [document_error, query_error, rerank_error] {
            assert_eq!(error.class(), ProviderFailureClass::InvalidRequest);
        }
    }

    #[test]
    fn request_limit_statuses_and_rerank_indices_are_structured() {
        let contract = contract();
        for (status, class) in [
            (400, ProviderFailureClass::InvalidRequest),
            (401, ProviderFailureClass::Authentication),
            (402, ProviderFailureClass::ResourceExhausted),
            (404, ProviderFailureClass::UnsupportedCapability),
            (408, ProviderFailureClass::Timeout),
            (429, ProviderFailureClass::ProviderRejected),
            (500, ProviderFailureClass::ProviderRejected),
        ] {
            assert_eq!(
                require_success(&contract, status).unwrap_err().class(),
                class
            );
        }

        let query =
            QueryEmbeddingRequest::new(&contract, "query", 1, ProviderRequestLimits::default())
                .unwrap();
        let mut transport = FakeTransport {
            expected_credential: "private-token".into(),
            calls: VecDeque::new(),
        };
        let error = OpenRouterAdapter::new(
            &contract,
            "private-token",
            &mut transport,
            OpenRouterWireLimits {
                request_bytes: 1,
                response_bytes: 1,
            },
        )
        .unwrap()
        .provide_query(&query, &mut || Ok(()))
        .err()
        .unwrap();
        assert_eq!(error.class(), ProviderFailureClass::ResourceExhausted);

        let candidates = [
            RerankCandidate {
                node_uuid: [1; 16],
                retrieval_rank: 1,
                text: "first",
                token_count: 1,
            },
            RerankCandidate {
                node_uuid: [2; 16],
                retrieval_rank: 2,
                text: "second",
                token_count: 1,
            },
        ];
        let request = RerankRequest::new(
            &contract,
            "query",
            1,
            &candidates,
            ProviderRequestLimits::default(),
        )
        .unwrap();
        for results in [
            json!([
                {"index":0,"relevance_score":0.9},
                {"index":0,"relevance_score":0.4}
            ]),
            json!([
                {"index":0,"relevance_score":0.9},
                {"index":2,"relevance_score":0.4}
            ]),
        ] {
            let mut transport = FakeTransport {
                expected_credential: "private-token".into(),
                calls: VecDeque::from([call(
                    OpenRouterEndpoint::Rerank,
                    json!({
                        "model":"vendor/model","query":"query","documents":["first","second"],
                        "top_n":2,
                        "provider":{"allow_fallbacks":false,"data_collection":"deny"}
                    }),
                    json!({"model":"vendor/model","results":results}),
                )]),
            };
            let error = OpenRouterAdapter::new(
                &contract,
                "private-token",
                &mut transport,
                OpenRouterWireLimits::default(),
            )
            .unwrap()
            .provide_rerank(&request, &mut || Ok(()))
            .err()
            .unwrap();
            assert_eq!(error.class(), ProviderFailureClass::MalformedResponse);
        }
    }
}
