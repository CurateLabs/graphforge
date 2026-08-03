//! Explicit inspection and execution of provider reranking over canonical find results.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;

use arrow::array::{Array, FixedSizeBinaryArray, Float64Array, StringArray};
use arrow::record_batch::RecordBatch;
use graphforge_search::{
    CandidateReranker, FusedSearchHit, MatchedOn, ProviderExecutionController,
    ProviderExecutionLimits, ProviderExecutionRuntime, ProviderFailureClass, ProviderModelContract,
    ProviderRequestLimits, ProviderResult, RerankApplication, RerankCandidate, RerankCostEstimator,
    RerankFailurePolicy, RerankRequest, RerankStatus, TextSearchLimits, apply_reranking,
    project_text_source,
};
use graphforge_storage::{SearchArtifactError, generation::read_search_generation};

use super::search_output::shape_search_output;
use super::{GfError, GraphForge, ProviderArtifactCheckpoint, ProviderTokenCounter};

const MAX_RERANK_CANDIDATES: usize = 10_000;

/// Exact graph projection and provider contract for one explicit rerank.
pub struct ProviderRerankRequest {
    /// Required graph label used by the canonical retrieval.
    pub label: String,
    /// Explicit rerank query sent to the provider.
    pub query: String,
    /// Explicit outbound string properties in canonical order.
    pub properties: Vec<String>,
    /// Maximum canonical retrieval candidates sent to the reranker.
    pub candidate_depth: usize,
    /// Maximum reranked rows returned.
    pub limit: usize,
    /// Exact provider/model/tokenizer/chunking identity.
    pub contract: ProviderModelContract,
    /// Per-provider invocation bounds.
    pub request_limits: ProviderRequestLimits,
    /// Retry, deadline, exposure, rate, and spend bounds.
    pub execution_limits: ProviderExecutionLimits,
    /// Explicit terminal provider-failure behavior.
    pub failure_policy: RerankFailurePolicy,
}

/// Content-free dry-run information produced before provider work.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderRerankPlanInspection {
    /// Committed graph generation whose properties were projected.
    pub graph_generation: u64,
    /// Required graph label.
    pub label: String,
    /// Sorted explicit outbound string properties.
    pub properties: Vec<String>,
    /// Exact normalized provider.
    pub provider: String,
    /// Exact provider model.
    pub model: String,
    /// Immutable model revision or `unavailable`.
    pub revision: String,
    /// Versioned provider response contract.
    pub response_contract_version: String,
    /// Exact tokenizer identifier.
    pub tokenizer_identifier: String,
    /// Immutable tokenizer version.
    pub tokenizer_version: String,
    /// Exact tokenizer count class.
    pub token_count_class: graphforge_storage::TokenCountClass,
    /// Maximum supported tokens in one model input.
    pub model_input_tokens: u64,
    /// Versioned tokenizer normalization contract.
    pub tokenizer_normalization: String,
    /// Explicit chunking/input-shaping identity, if configured.
    pub chunking: Option<graphforge_storage::ChunkingIdentity>,
    /// Configured canonical retrieval candidate bound.
    pub candidate_depth: usize,
    /// Actual candidates selected from the supplied canonical batch.
    pub selected_candidates: usize,
    /// Maximum reranked rows returned.
    pub limit: usize,
    /// Query and candidates' serialized UTF-8 bytes without their contents.
    pub input_bytes: usize,
    /// Query and candidates' counted tokens.
    pub input_tokens: u64,
    /// Exact per-request limits.
    pub request_limits: ProviderRequestLimits,
    /// Exact execution limits.
    pub execution_limits: ProviderExecutionLimits,
    /// Explicit terminal failure policy.
    pub failure_policy: RerankFailurePolicy,
}

/// Runtime-only dependencies for one explicit rerank.
pub struct ProviderRerankExecution<'a> {
    provider: &'a mut dyn CandidateReranker,
    runtime: &'a mut dyn ProviderExecutionRuntime,
    count_tokens: &'a mut ProviderTokenCounter<'a>,
    estimate_cost: &'a mut RerankCostEstimator<'a>,
    checkpoint: &'a mut ProviderArtifactCheckpoint<'a>,
}

impl<'a> ProviderRerankExecution<'a> {
    /// Assemble caller-owned provider execution dependencies.
    #[must_use]
    pub fn new(
        provider: &'a mut dyn CandidateReranker,
        runtime: &'a mut dyn ProviderExecutionRuntime,
        count_tokens: &'a mut ProviderTokenCounter<'a>,
        estimate_cost: &'a mut RerankCostEstimator<'a>,
        checkpoint: &'a mut ProviderArtifactCheckpoint<'a>,
    ) -> Self {
        Self {
            provider,
            runtime,
            count_tokens,
            estimate_cost,
            checkpoint,
        }
    }
}

/// Canonical Arrow output plus non-conditional rerank interpretation.
#[derive(Debug)]
pub struct ProviderRerankedFindResult {
    batch: RecordBatch,
    status: RerankStatus,
}

impl ProviderRerankedFindResult {
    /// Canonical UUID/property/score/channel Arrow result.
    #[must_use]
    pub const fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    /// Reranked, empty, or explicit-fallback interpretation outside Arrow rows.
    #[must_use]
    pub const fn status(&self) -> &RerankStatus {
        &self.status
    }

    /// Consume the result while keeping status outside canonical rows.
    #[must_use]
    pub fn into_parts(self) -> (RecordBatch, RerankStatus) {
        (self.batch, self.status)
    }
}

/// Structured facade, graph-artifact, or redacted provider-rerank failure.
#[derive(Debug)]
pub enum ProviderRerankError {
    /// Public request, canonical Arrow, label, property, or shaping failure.
    Api(GfError),
    /// Cancellation, resource, source, or concurrent-mutation failure.
    Artifact(SearchArtifactError),
    /// Redacted provider/model/tokenizer/capability/execution failure.
    Provider(graphforge_search::ProviderError),
}

impl fmt::Display for ProviderRerankError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Api(error) => error.fmt(formatter),
            Self::Artifact(error) => error.fmt(formatter),
            Self::Provider(error) => error.fmt(formatter),
        }
    }
}

impl Error for ProviderRerankError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Api(error) => Some(error),
            Self::Artifact(error) => Some(error),
            Self::Provider(error) => Some(error),
        }
    }
}

impl From<GfError> for ProviderRerankError {
    fn from(error: GfError) -> Self {
        Self::Api(error)
    }
}

impl From<SearchArtifactError> for ProviderRerankError {
    fn from(error: SearchArtifactError) -> Self {
        Self::Artifact(error)
    }
}

impl From<graphforge_search::ProviderError> for ProviderRerankError {
    fn from(error: graphforge_search::ProviderError) -> Self {
        Self::Provider(error)
    }
}

struct PreparedRerankCandidate {
    node_uuid: [u8; 16],
    text: String,
    token_count: u64,
}

struct PreparedRerankPlan {
    hits: Vec<FusedSearchHit>,
    query_token_count: u64,
    candidates: Vec<PreparedRerankCandidate>,
    input_bytes: usize,
    input_tokens: u64,
}

impl GraphForge {
    /// Inspect exact candidate and tokenizer work without invoking a reranker.
    ///
    /// Returned inspection and errors never retain query text, candidate text,
    /// vectors, credentials, or provider payloads.
    ///
    /// # Errors
    /// Rejects malformed canonical input, labels, properties, candidate bounds,
    /// provider capability, tokenizer counts, limits, graph source, or cancellation.
    pub fn inspect_provider_rerank_plan<F>(
        &self,
        canonical: &RecordBatch,
        request: &ProviderRerankRequest,
        mut count_tokens: F,
    ) -> Result<ProviderRerankPlanInspection, ProviderRerankError>
    where
        F: FnMut(&ProviderModelContract, &str) -> ProviderResult<u64>,
    {
        let generation = read_search_generation(&self.dir)?;
        let prepared =
            self.prepare_rerank_plan(canonical, request, &mut count_tokens, &mut || Ok(()))?;
        let runtime = graphforge_search::StandardProviderExecutionRuntime::new();
        ProviderExecutionController::new(&request.contract, request.execution_limits, &runtime)?;
        Ok(inspection(request, generation, &prepared))
    }

    /// Explicitly rerank one canonical find result through caller-injected dependencies.
    ///
    /// The supplied batch may come from ordinary [`GraphForge::find`] or
    /// [`GraphForge::find_with_provider`]. Reranking status stays outside Arrow rows.
    ///
    /// # Errors
    /// Returns structured canonical-input, graph, tokenizer, provider, response,
    /// cancellation, resource, concurrency, or Arrow-shaping failures without partial rows.
    pub fn rerank_find_results(
        &self,
        canonical: &RecordBatch,
        request: &ProviderRerankRequest,
        execution: ProviderRerankExecution<'_>,
    ) -> Result<ProviderRerankedFindResult, ProviderRerankError> {
        let ProviderRerankExecution {
            provider,
            runtime,
            count_tokens,
            estimate_cost,
            checkpoint,
        } = execution;
        if provider.contract() != &request.contract {
            return Err(graphforge_search::ProviderError::new(
                &request.contract,
                ProviderFailureClass::InvalidRequest,
            )
            .into());
        }
        let mut controller =
            ProviderExecutionController::new(&request.contract, request.execution_limits, runtime)?;

        for attempt in 1_u8..=2 {
            let generation = read_search_generation(&self.dir)?;
            let prepared =
                self.prepare_rerank_plan(canonical, request, count_tokens, checkpoint)?;
            let candidates = prepared
                .candidates
                .iter()
                .enumerate()
                .map(|(index, candidate)| RerankCandidate {
                    node_uuid: candidate.node_uuid,
                    retrieval_rank: index + 1,
                    text: &candidate.text,
                    token_count: candidate.token_count,
                })
                .collect::<Vec<_>>();
            let rerank_request = if candidates.is_empty() {
                None
            } else {
                Some(RerankRequest::new(
                    &request.contract,
                    &request.query,
                    prepared.query_token_count,
                    &candidates,
                    request.request_limits,
                )?)
            };
            let application =
                with_artifact_checkpoint(&request.contract, checkpoint, |provider_checkpoint| {
                    apply_reranking(
                        &prepared.hits,
                        rerank_request.as_ref(),
                        provider,
                        &mut controller,
                        runtime,
                        request.failure_policy,
                        estimate_cost,
                        provider_checkpoint,
                    )
                })?;
            if generation != read_search_generation(&self.dir)? {
                if attempt == 2 {
                    return Err(SearchArtifactError::ConcurrentMutation.into());
                }
                continue;
            }
            let result = self.shape_rerank_application(request, application)?;
            if generation == read_search_generation(&self.dir)? {
                return Ok(result);
            }
            if attempt == 2 {
                return Err(SearchArtifactError::ConcurrentMutation.into());
            }
        }
        unreachable!("bounded rerank retry returns on both terminal paths")
    }

    fn prepare_rerank_plan<F, C>(
        &self,
        canonical: &RecordBatch,
        request: &ProviderRerankRequest,
        count_tokens: &mut F,
        checkpoint: &mut C,
    ) -> Result<PreparedRerankPlan, ProviderRerankError>
    where
        F: FnMut(&ProviderModelContract, &str) -> ProviderResult<u64> + ?Sized,
        C: FnMut() -> Result<(), SearchArtifactError> + ?Sized,
    {
        validate_request(request)?;
        request
            .contract
            .require(graphforge_search::ProviderCapability::CandidateReranking)?;
        let mut hits = canonical_hits(canonical)?;
        hits.truncate(request.candidate_depth);
        let label_id = self.search_label_id(&request.label)?;
        let projection = project_text_source(
            &self.dir,
            label_id,
            Some(&request.properties),
            TextSearchLimits::default(),
            &mut *checkpoint,
        )?;
        let projected = projection
            .documents
            .into_iter()
            .map(|document| (document.node_uuid, document.fields))
            .collect::<BTreeMap<_, _>>();
        let query_token_count = count_tokens(&request.contract, &request.query)?;
        let mut input_bytes = request.query.len();
        let mut input_tokens = query_token_count;
        let mut candidates = Vec::with_capacity(hits.len());
        for hit in &hits {
            checkpoint()?;
            let fields = projected
                .get(&hit.node_uuid)
                .ok_or_else(|| validation("canonical rerank UUID is not a current label member"))?;
            if fields.is_empty() {
                return Err(validation(
                    "rerank properties must produce text for every selected candidate",
                )
                .into());
            }
            let text = serde_json::to_string(fields)
                .map_err(|error| validation(format!("serialize rerank candidate: {error}")))?;
            let token_count = count_tokens(&request.contract, &text)?;
            input_bytes = input_bytes
                .checked_add(text.len())
                .ok_or_else(|| exhausted(&request.contract))?;
            input_tokens = input_tokens
                .checked_add(token_count)
                .ok_or_else(|| exhausted(&request.contract))?;
            candidates.push(PreparedRerankCandidate {
                node_uuid: hit.node_uuid,
                text,
                token_count,
            });
        }
        let rerank_candidates = candidates
            .iter()
            .enumerate()
            .map(|(index, candidate)| RerankCandidate {
                node_uuid: candidate.node_uuid,
                retrieval_rank: index + 1,
                text: &candidate.text,
                token_count: candidate.token_count,
            })
            .collect::<Vec<_>>();
        if !rerank_candidates.is_empty() {
            RerankRequest::new(
                &request.contract,
                &request.query,
                query_token_count,
                &rerank_candidates,
                request.request_limits,
            )?;
        }
        Ok(PreparedRerankPlan {
            hits,
            query_token_count,
            candidates,
            input_bytes,
            input_tokens,
        })
    }

    fn shape_rerank_application(
        &self,
        request: &ProviderRerankRequest,
        application: RerankApplication,
    ) -> Result<ProviderRerankedFindResult, ProviderRerankError> {
        let (mut hits, status) = application.into_parts();
        hits.truncate(request.limit);
        let label_id = self.search_label_id(&request.label)?;
        let batch = shape_search_output(&self.dir, label_id, &hits)?;
        Ok(ProviderRerankedFindResult { batch, status })
    }
}

fn validate_request(request: &ProviderRerankRequest) -> Result<(), ProviderRerankError> {
    if request.label.is_empty() || request.label.trim() != request.label {
        return Err(validation("rerank requires a valid label").into());
    }
    if request.query.is_empty() {
        return Err(validation("rerank requires a non-empty query").into());
    }
    if request.properties.is_empty() {
        return Err(validation("rerank properties must be explicit").into());
    }
    let mut properties = request.properties.clone();
    properties.sort_unstable();
    properties.dedup();
    if properties != request.properties
        || properties.iter().any(|property| {
            property.is_empty()
                || property.trim() != property
                || property.chars().any(char::is_control)
                || property == "node_uuid"
        })
    {
        return Err(validation(
            "rerank properties must be sorted unique valid graph property names",
        )
        .into());
    }
    if !(1..=MAX_RERANK_CANDIDATES).contains(&request.candidate_depth)
        || request.limit == 0
        || request.limit > request.candidate_depth
    {
        return Err(validation("rerank requires 1 <= limit <= candidate_depth <= 10000").into());
    }
    request.request_limits.validate()?;
    Ok(())
}

fn canonical_hits(batch: &RecordBatch) -> Result<Vec<FusedSearchHit>, ProviderRerankError> {
    if batch
        .schema()
        .metadata()
        .get("graphforge.verb")
        .map(String::as_str)
        != Some("find")
    {
        return Err(validation("rerank input must be a canonical find result").into());
    }
    let uuids = batch
        .column_by_name("node_uuid")
        .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
        .filter(|column| column.value_length() == 16)
        .ok_or_else(|| validation("canonical find node_uuid must be FixedSizeBinary(16)"))?;
    let scores = batch
        .column_by_name("score")
        .and_then(|column| column.as_any().downcast_ref::<Float64Array>())
        .ok_or_else(|| validation("canonical find score must be Float64"))?;
    let channels = batch
        .column_by_name("matched_on")
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| validation("canonical find matched_on must be Utf8"))?;
    if uuids.null_count() != 0 || scores.null_count() != 0 || channels.null_count() != 0 {
        return Err(
            validation("canonical find identity, score, and channel cannot be null").into(),
        );
    }
    let mut hits = Vec::with_capacity(batch.num_rows());
    let mut seen = BTreeSet::new();
    for row in 0..batch.num_rows() {
        let node_uuid: [u8; 16] = uuids
            .value(row)
            .try_into()
            .map_err(|_| validation("canonical find node_uuid must contain 16 bytes"))?;
        let score = scores.value(row);
        if !seen.insert(node_uuid) || !score.is_finite() {
            return Err(
                validation("canonical find UUIDs must be unique with finite scores").into(),
            );
        }
        let matched_on = match channels.value(row) {
            "text" => MatchedOn::Text,
            "vector" => MatchedOn::Vector,
            "text+vector" => MatchedOn::TextAndVector,
            _ => return Err(validation("canonical find matched_on token is invalid").into()),
        };
        hits.push(FusedSearchHit {
            node_uuid,
            score,
            matched_on,
        });
    }
    if hits.windows(2).any(|pair| {
        pair[0].score.total_cmp(&pair[1].score).is_lt()
            || (pair[0].score.total_cmp(&pair[1].score) == Ordering::Equal
                && pair[0].node_uuid >= pair[1].node_uuid)
    }) {
        return Err(validation("canonical find rows are not in deterministic rank order").into());
    }
    Ok(hits)
}

fn inspection(
    request: &ProviderRerankRequest,
    graph_generation: u64,
    prepared: &PreparedRerankPlan,
) -> ProviderRerankPlanInspection {
    ProviderRerankPlanInspection {
        graph_generation,
        label: request.label.clone(),
        properties: request.properties.clone(),
        provider: request.contract.provider().to_owned(),
        model: request.contract.model().to_owned(),
        revision: request.contract.revision().to_owned(),
        response_contract_version: request.contract.response_contract_version().to_owned(),
        tokenizer_identifier: request.contract.tokenizer().identifier.clone(),
        tokenizer_version: request.contract.tokenizer().version.clone(),
        token_count_class: request.contract.tokenizer().count_class,
        model_input_tokens: request.contract.tokenizer().max_input_tokens,
        tokenizer_normalization: request.contract.tokenizer().normalization.clone(),
        chunking: request.contract.chunking().cloned(),
        candidate_depth: request.candidate_depth,
        selected_candidates: prepared.candidates.len(),
        limit: request.limit,
        input_bytes: prepared.input_bytes,
        input_tokens: prepared.input_tokens,
        request_limits: request.request_limits,
        execution_limits: request.execution_limits,
        failure_policy: request.failure_policy,
    }
}

fn with_artifact_checkpoint<T, F>(
    contract: &ProviderModelContract,
    checkpoint: &mut ProviderArtifactCheckpoint<'_>,
    operation: F,
) -> Result<T, ProviderRerankError>
where
    F: FnOnce(&mut dyn FnMut() -> ProviderResult<()>) -> ProviderResult<T>,
{
    let mut artifact_failure = None;
    let result = operation(&mut || match checkpoint() {
        Ok(()) => Ok(()),
        Err(error) => {
            artifact_failure = Some(error);
            Err(graphforge_search::ProviderError::new(
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

fn validation(message: impl Into<String>) -> GfError {
    GfError::Validation(message.into())
}

fn exhausted(contract: &ProviderModelContract) -> ProviderRerankError {
    graphforge_search::ProviderError::new(contract, ProviderFailureClass::ResourceExhausted).into()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use graphforge_search::{
        ProviderCapabilities, ProviderCapability, RerankOutput, StandardProviderExecutionRuntime,
    };
    use graphforge_storage::{TokenCountClass, TokenizerIdentity};

    use super::*;
    use crate::{FindDiagnostic, FindExecutionOptions, FindOptions, FindRerankOptions, PropValue};

    #[derive(Clone, Copy)]
    enum Mode {
        Success,
        Timeout,
        Malformed,
    }

    struct FakeReranker {
        contract: ProviderModelContract,
        mode: Mode,
        calls: usize,
    }

    impl CandidateReranker for FakeReranker {
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
            if matches!(self.mode, Mode::Timeout) {
                return Err(graphforge_search::ProviderError::new(
                    &self.contract,
                    ProviderFailureClass::Timeout,
                ));
            }
            let mut outputs = request
                .candidates()
                .iter()
                .enumerate()
                .map(|(index, candidate)| RerankOutput {
                    node_uuid: candidate.node_uuid,
                    score: index as f64,
                })
                .collect::<Vec<_>>();
            if matches!(self.mode, Mode::Malformed) {
                outputs[0].node_uuid = [0xff; 16];
            }
            Ok(outputs)
        }
    }

    fn contract(model: &str) -> ProviderModelContract {
        ProviderModelContract::remote(
            None,
            model,
            "revision-1",
            "wire-v1",
            ProviderCapabilities::new([ProviderCapability::CandidateReranking]).unwrap(),
            TokenizerIdentity {
                identifier: "rerank-tokenizer".to_owned(),
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
    fn provider_rerank_error_preserves_display_source_and_failure_domain() {
        let api = ProviderRerankError::from(GfError::Validation("bad rerank".into()));
        assert_eq!(api.to_string(), "validation error: bad rerank");
        assert!(api.source().is_some());

        let artifact = ProviderRerankError::from(SearchArtifactError::Cancelled);
        assert_eq!(artifact.to_string(), "search operation cancelled");
        assert!(artifact.source().is_some());

        let provider = ProviderRerankError::from(graphforge_search::ProviderError::new(
            &contract("rerank-error-model"),
            graphforge_search::ProviderFailureClass::ProviderRejected,
        ));
        assert!(provider.to_string().contains("rerank-error-model"));
        assert!(provider.source().is_some());
    }

    fn request(failure_policy: RerankFailurePolicy) -> ProviderRerankRequest {
        ProviderRerankRequest {
            label: "Document".to_owned(),
            query: "rerank intent".to_owned(),
            properties: vec!["title".to_owned()],
            candidate_depth: 2,
            limit: 2,
            contract: contract("vendor/reranker"),
            request_limits: ProviderRequestLimits::default(),
            execution_limits: ProviderExecutionLimits {
                retries: 0,
                timeout: Duration::from_secs(5),
                ..ProviderExecutionLimits::default()
            },
            failure_policy,
        }
    }

    fn add_document(graph: &GraphForge, title: &str) {
        graph
            .add_node(
                "Document",
                &HashMap::from([("title".to_owned(), PropValue::Str(title.to_owned()))]),
            )
            .unwrap();
    }

    fn canonical(graph: &GraphForge, query: &str) -> RecordBatch {
        graph
            .find(FindOptions {
                query: Some(query.to_owned()),
                label: Some("Document".to_owned()),
                limit: 2,
                ..FindOptions::default()
            })
            .unwrap()
    }

    fn uuids(batch: &RecordBatch) -> Vec<[u8; 16]> {
        batch
            .column_by_name("node_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .iter()
            .map(|value| value.unwrap().try_into().unwrap())
            .collect()
    }

    fn scores(batch: &RecordBatch) -> Vec<f64> {
        batch
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    fn execute(
        graph: &GraphForge,
        canonical: &RecordBatch,
        request: &ProviderRerankRequest,
        provider: &mut FakeReranker,
        count_tokens: &mut ProviderTokenCounter<'_>,
        checkpoint: &mut ProviderArtifactCheckpoint<'_>,
    ) -> Result<ProviderRerankedFindResult, ProviderRerankError> {
        let mut runtime = StandardProviderExecutionRuntime::new();
        let mut estimate_cost = |shape: graphforge_search::RerankWorkShape| {
            Ok(u64::try_from(shape.candidates()).unwrap())
        };
        graph.rerank_find_results(
            canonical,
            request,
            ProviderRerankExecution::new(
                provider,
                &mut runtime,
                count_tokens,
                &mut estimate_cost,
                checkpoint,
            ),
        )
    }

    #[test]
    fn detailed_find_keeps_omission_neutral_and_reranks_explicitly() {
        let graph = GraphForge::new(None).unwrap();
        add_document(&graph, "alpha systems");
        add_document(&graph, "beta systems");
        let find = FindOptions {
            query: Some("systems".into()),
            label: Some("Document".into()),
            limit: 2,
            ..FindOptions::default()
        };
        let canonical = graph.find(find.clone()).unwrap();
        let omitted = graph
            .find_with_diagnostics(
                FindExecutionOptions {
                    find: find.clone(),
                    omitted_reranker: Some(contract("vendor/reranker")),
                    ..FindExecutionOptions::default()
                },
                None,
            )
            .unwrap();
        let (omitted, diagnostics, _) = omitted.into_parts();
        assert_eq!(canonical, omitted);
        assert!(matches!(
            diagnostics.as_slice(),
            [FindDiagnostic::RerankSuggested { provider, model }]
                if provider == "openrouter" && model == "vendor/reranker"
        ));
        let contract = contract("vendor/reranker");
        let mut provider = FakeReranker {
            contract: contract.clone(),
            mode: Mode::Success,
            calls: 0,
        };
        let mut runtime = StandardProviderExecutionRuntime::new();
        let mut count_tokens = |_: &ProviderModelContract, text: &str| Ok(text.len() as u64);
        let mut estimate_cost =
            |shape: graphforge_search::RerankWorkShape| Ok(shape.candidates() as u64);
        let mut checkpoint = || Ok(());
        let reranked = graph
            .find_with_diagnostics(
                FindExecutionOptions {
                    find,
                    rerank: Some(FindRerankOptions {
                        query: "systems".into(),
                        properties: vec!["title".into()],
                        candidate_depth: 2,
                        contract,
                        request_limits: ProviderRequestLimits::default(),
                        execution_limits: ProviderExecutionLimits::default(),
                        failure_policy: RerankFailurePolicy::Error,
                    }),
                    ..FindExecutionOptions::default()
                },
                Some(ProviderRerankExecution::new(
                    &mut provider,
                    &mut runtime,
                    &mut count_tokens,
                    &mut estimate_cost,
                    &mut checkpoint,
                )),
            )
            .unwrap();
        let (reranked, diagnostics, status) = reranked.into_parts();
        let mut expected = uuids(&canonical);
        expected.reverse();
        assert_eq!(uuids(&reranked), expected);
        assert!(diagnostics.is_empty());
        assert!(matches!(status, Some(RerankStatus::Reranked { .. })));
    }

    #[test]
    fn inspection_is_payload_free_and_execution_reorders_canonical_arrow() {
        let graph = GraphForge::new(None).unwrap();
        add_document(&graph, "alpha systems");
        add_document(&graph, "beta systems");
        let canonical = canonical(&graph, "systems");
        let request = request(RerankFailurePolicy::Error);
        let inspection = graph
            .inspect_provider_rerank_plan(&canonical, &request, |_, text| {
                Ok(u64::try_from(text.len()).unwrap())
            })
            .unwrap();
        assert_eq!(inspection.selected_candidates, 2);
        assert_eq!(inspection.properties, ["title"]);
        assert_eq!(
            inspection.provider,
            graphforge_search::DEFAULT_REMOTE_PROVIDER
        );
        assert_eq!(inspection.model, "vendor/reranker");
        let debug = format!("{inspection:?}");
        assert!(!debug.contains("rerank intent"));
        assert!(!debug.contains("alpha systems"));

        let before = uuids(&canonical);
        let mut provider = FakeReranker {
            contract: request.contract.clone(),
            mode: Mode::Success,
            calls: 0,
        };
        let mut count_tokens =
            |_: &ProviderModelContract, text: &str| Ok(u64::try_from(text.len()).unwrap());
        let mut checkpoint = || Ok(());
        let result = execute(
            &graph,
            &canonical,
            &request,
            &mut provider,
            &mut count_tokens,
            &mut checkpoint,
        )
        .unwrap();

        assert_eq!(provider.calls, 1);
        assert_eq!(uuids(result.batch()), [before[1], before[0]]);
        assert_eq!(scores(result.batch()), [1.0, 0.0]);
        assert_eq!(
            result.batch().schema().fields(),
            canonical.schema().fields()
        );
        assert!(matches!(result.status(), RerankStatus::Reranked { .. }));
    }

    #[test]
    fn default_error_and_explicit_canonical_fallback_are_distinct() {
        let graph = GraphForge::new(None).unwrap();
        add_document(&graph, "alpha systems");
        add_document(&graph, "beta systems");
        let canonical = canonical(&graph, "systems");
        let mut provider = FakeReranker {
            contract: contract("vendor/reranker"),
            mode: Mode::Timeout,
            calls: 0,
        };
        let mut count_tokens =
            |_: &ProviderModelContract, text: &str| Ok(u64::try_from(text.len()).unwrap());
        let mut checkpoint = || Ok(());
        let error_request = request(RerankFailurePolicy::Error);
        let error = execute(
            &graph,
            &canonical,
            &error_request,
            &mut provider,
            &mut count_tokens,
            &mut checkpoint,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            ProviderRerankError::Provider(ref error)
                if error.class() == ProviderFailureClass::Timeout
        ));

        let fallback_request = request(RerankFailurePolicy::CanonicalUnreranked);
        let result = execute(
            &graph,
            &canonical,
            &fallback_request,
            &mut provider,
            &mut count_tokens,
            &mut checkpoint,
        )
        .unwrap();
        assert_eq!(uuids(result.batch()), uuids(&canonical));
        assert_eq!(scores(result.batch()), scores(&canonical));
        assert!(matches!(
            result.status(),
            RerankStatus::CanonicalUnreranked {
                failure: ProviderFailureClass::Timeout,
                ..
            }
        ));
    }

    #[test]
    fn mismatch_malformed_and_cancellation_fail_without_partial_rows() {
        let graph = GraphForge::new(None).unwrap();
        add_document(&graph, "alpha systems");
        add_document(&graph, "beta systems");
        let canonical = canonical(&graph, "systems");
        let request = request(RerankFailurePolicy::Error);
        let mut mismatch = FakeReranker {
            contract: contract("vendor/other"),
            mode: Mode::Success,
            calls: 0,
        };
        let mut count_tokens =
            |_: &ProviderModelContract, text: &str| Ok(u64::try_from(text.len()).unwrap());
        let mut checkpoint = || Ok(());
        let error = execute(
            &graph,
            &canonical,
            &request,
            &mut mismatch,
            &mut count_tokens,
            &mut checkpoint,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            ProviderRerankError::Provider(ref error)
                if error.class() == ProviderFailureClass::InvalidRequest
        ));
        assert_eq!(mismatch.calls, 0);

        let mut malformed = FakeReranker {
            contract: request.contract.clone(),
            mode: Mode::Malformed,
            calls: 0,
        };
        let error = execute(
            &graph,
            &canonical,
            &request,
            &mut malformed,
            &mut count_tokens,
            &mut checkpoint,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            ProviderRerankError::Provider(ref error)
                if error.class() == ProviderFailureClass::MalformedResponse
        ));

        let mut cancelled = FakeReranker {
            contract: request.contract.clone(),
            mode: Mode::Success,
            calls: 0,
        };
        let mut checkpoint = || Err(SearchArtifactError::Cancelled);
        let error = execute(
            &graph,
            &canonical,
            &request,
            &mut cancelled,
            &mut count_tokens,
            &mut checkpoint,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            ProviderRerankError::Artifact(SearchArtifactError::Cancelled)
        ));
        assert_eq!(cancelled.calls, 0);
    }

    #[test]
    fn empty_input_skips_provider_and_graph_change_retries_once() {
        let graph = GraphForge::new(None).unwrap();
        add_document(&graph, "alpha systems");
        add_document(&graph, "beta systems");
        let empty = canonical(&graph, "no-match-token");
        let request = request(RerankFailurePolicy::Error);
        let mut provider = FakeReranker {
            contract: request.contract.clone(),
            mode: Mode::Success,
            calls: 0,
        };
        let mut count_tokens =
            |_: &ProviderModelContract, text: &str| Ok(u64::try_from(text.len()).unwrap());
        let mut checkpoint = || Ok(());
        let result = execute(
            &graph,
            &empty,
            &request,
            &mut provider,
            &mut count_tokens,
            &mut checkpoint,
        )
        .unwrap();
        assert_eq!(result.batch().num_rows(), 0);
        assert_eq!(provider.calls, 0);
        assert!(matches!(result.status(), RerankStatus::NoCandidates { .. }));

        let canonical = canonical(&graph, "systems");
        let mut mutated = false;
        let mut count_tokens = |_: &ProviderModelContract, text: &str| {
            if text == "rerank intent" && !mutated {
                mutated = true;
                add_document(&graph, "one mutation");
            }
            Ok(u64::try_from(text.len()).unwrap())
        };
        let result = execute(
            &graph,
            &canonical,
            &request,
            &mut provider,
            &mut count_tokens,
            &mut checkpoint,
        )
        .unwrap();
        assert_eq!(result.batch().num_rows(), 2);
        assert_eq!(provider.calls, 2);
    }

    #[test]
    fn second_graph_change_returns_concurrent_mutation() {
        let graph = GraphForge::new(None).unwrap();
        add_document(&graph, "alpha systems");
        add_document(&graph, "beta systems");
        let canonical = canonical(&graph, "systems");
        let request = request(RerankFailurePolicy::Error);
        let mut provider = FakeReranker {
            contract: request.contract.clone(),
            mode: Mode::Success,
            calls: 0,
        };
        let mut mutations = 0;
        let mut count_tokens = |_: &ProviderModelContract, text: &str| {
            if text == "rerank intent" {
                mutations += 1;
                add_document(&graph, &format!("mutation {mutations}"));
            }
            Ok(u64::try_from(text.len()).unwrap())
        };
        let mut checkpoint = || Ok(());
        let error = execute(
            &graph,
            &canonical,
            &request,
            &mut provider,
            &mut count_tokens,
            &mut checkpoint,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            ProviderRerankError::Artifact(SearchArtifactError::ConcurrentMutation)
        ));
        assert_eq!(provider.calls, 2);
        assert_eq!(mutations, 2);
    }
}
