//! Content-free inspection of explicit provider property-embedding plans.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use gf_search::{
    DocumentEmbeddingBatchOptions, DocumentEmbeddingBatchPlan, DocumentEmbeddingInput,
    ProviderBatchLimits, ProviderCapability, ProviderEmbeddingPublicationRequest,
    ProviderExecutionController, ProviderExecutionLimits, ProviderModelContract,
    ProviderRequestLimits, ProviderResult, StandardProviderExecutionRuntime, TextSearchLimits,
    TextSourceProjection, capture_embedding_source, project_text_source,
};
use gf_storage::{
    ChunkingIdentity, EmbeddingCompatibilityDescriptor, EmbeddingCompatibilityInput,
    EmbeddingDisplayName, EmbeddingDistance, EmbeddingNormalization, EmbeddingProducerIdentity,
    EmbeddingSourceState, EmbeddingValueType, SearchArtifactError, SearchSourcePart,
    TokenCountClass, VectorStoreLimits,
};

use super::{GfError, GraphForge};

const INPUT_RECIPE: &str = "canonical-property-json-v1";
const LABEL_SOURCE_PART: &str = "provider-label-membership-v1";
const DEPENDENCY_SOURCE_PART: &str = "provider-property-values-v1";

/// Stored normalization for one provider-produced space.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProviderEmbeddingNormalization {
    /// Preserve finite provider coordinates.
    None,
    /// Require and store L2-normalized vectors.
    L2,
}

/// Supported exact-search distance contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProviderEmbeddingDistance {
    /// Exact cosine similarity.
    Cosine,
}

/// Explicit graph projection and exact provider work contract.
pub struct ProviderEmbeddingPlanRequest {
    /// User-visible embedding-space name.
    pub display_name: String,
    /// Required graph label.
    pub label: String,
    /// Explicit outbound string properties; discovery is forbidden.
    pub properties: Vec<String>,
    /// Exact provider/model/tokenizer/chunking identity.
    pub contract: ProviderModelContract,
    /// Fixed response dimension.
    pub dimensions: u32,
    /// Stored normalization.
    pub normalization: ProviderEmbeddingNormalization,
    /// Exact retrieval distance.
    pub distance: ProviderEmbeddingDistance,
    /// Per-adapter invocation bounds.
    pub request_limits: ProviderRequestLimits,
    /// Deterministic batch split bounds.
    pub batch_limits: ProviderBatchLimits,
    /// Retry, deadline, exposure, rate, and spend bounds.
    pub execution_limits: ProviderExecutionLimits,
    /// Permit a later execution to reassign an occupied alias.
    pub replace_alias: bool,
}

/// One payload-free deterministic provider batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProviderEmbeddingPlannedBatch {
    /// Documents in this batch.
    pub items: usize,
    /// Serialized outbound UTF-8 bytes without their contents.
    pub input_bytes: usize,
    /// Counted tokens under the exact tokenizer contract.
    pub input_tokens: u64,
}

/// Content-free confirmation data produced before any provider call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderEmbeddingPlanInspection {
    /// Validated display name.
    pub display_name: String,
    /// Compatibility identity of the exact requested space.
    pub compatibility_id: String,
    /// Fingerprint of the committed source projection.
    pub source_fingerprint: String,
    /// Committed graph generation inspected.
    pub graph_generation: u64,
    /// Required label.
    pub label: String,
    /// Sorted explicit outbound properties.
    pub properties: Vec<String>,
    /// Exact normalized provider.
    pub provider: String,
    /// Exact model.
    pub model: String,
    /// Immutable model revision or `unavailable`.
    pub revision: String,
    /// Versioned response contract.
    pub response_contract_version: String,
    /// Exact tokenizer identifier.
    pub tokenizer_identifier: String,
    /// Immutable tokenizer version.
    pub tokenizer_version: String,
    /// Whether token counts are local-exact, provider-reported, or approximate.
    pub token_count_class: TokenCountClass,
    /// Maximum supported tokens in one model input.
    pub model_input_tokens: u64,
    /// Versioned tokenizer text-normalization contract.
    pub tokenizer_normalization: String,
    /// Explicit chunking and reject-truncation contract, if configured.
    pub chunking: Option<ChunkingIdentity>,
    /// Fixed response dimension.
    pub dimensions: u32,
    /// Stored normalization.
    pub normalization: ProviderEmbeddingNormalization,
    /// Exact retrieval distance.
    pub distance: ProviderEmbeddingDistance,
    /// UUIDs with at least one selected string value.
    pub selected_nodes: usize,
    /// Total outbound UTF-8 bytes.
    pub input_bytes: usize,
    /// Total counted outbound tokens.
    pub input_tokens: u64,
    /// Exact deterministic batches.
    pub batches: Vec<ProviderEmbeddingPlannedBatch>,
    /// Exact per-request limits.
    pub request_limits: ProviderRequestLimits,
    /// Exact deterministic batching limits.
    pub batch_limits: ProviderBatchLimits,
    /// Exact execution limits for the confirmed run.
    pub execution_limits: ProviderExecutionLimits,
}

pub(crate) struct PreparedProviderDocument {
    pub(crate) node_uuid: [u8; 16],
    pub(crate) text: String,
    pub(crate) token_count: u64,
}

pub(crate) struct PreparedProviderPlan {
    pub(crate) display_name: String,
    pub(crate) descriptor: EmbeddingCompatibilityDescriptor,
    pub(crate) source: EmbeddingSourceState,
    pub(crate) properties: Vec<String>,
    pub(crate) documents: Vec<PreparedProviderDocument>,
    pub(crate) options: DocumentEmbeddingBatchOptions,
}

/// Structured API, source, or redacted provider-plan failure.
#[derive(Debug)]
pub enum ProviderEmbeddingPlanError {
    /// Public facade validation or graph error.
    Api(GfError),
    /// Source projection, cancellation, or resource failure.
    Artifact(SearchArtifactError),
    /// Redacted provider/model/tokenizer contract failure.
    Provider(gf_search::ProviderError),
}

impl fmt::Display for ProviderEmbeddingPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Api(error) => error.fmt(formatter),
            Self::Artifact(error) => error.fmt(formatter),
            Self::Provider(error) => error.fmt(formatter),
        }
    }
}

impl Error for ProviderEmbeddingPlanError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Api(error) => Some(error),
            Self::Artifact(error) => Some(error),
            Self::Provider(error) => Some(error),
        }
    }
}

impl From<GfError> for ProviderEmbeddingPlanError {
    fn from(error: GfError) -> Self {
        Self::Api(error)
    }
}

impl From<SearchArtifactError> for ProviderEmbeddingPlanError {
    fn from(error: SearchArtifactError) -> Self {
        Self::Artifact(error)
    }
}

impl From<gf_search::ProviderError> for ProviderEmbeddingPlanError {
    fn from(error: gf_search::ProviderError) -> Self {
        Self::Provider(error)
    }
}

impl GraphForge {
    /// Inspect one exact property-to-provider plan without outbound work.
    ///
    /// `count_tokens` receives only ephemeral serialized property text and must
    /// count under `request.contract.tokenizer()`. Returned inspection and
    /// errors never retain source text, vectors, credentials, or provider
    /// payloads.
    ///
    /// # Errors
    /// Rejects invalid identity, label, explicit properties, alias ownership,
    /// token counts, tokenizer/model limits, batching, source corruption,
    /// cancellation, or resource exhaustion before a provider is invoked.
    pub fn inspect_provider_embedding_plan<F>(
        &self,
        request: &ProviderEmbeddingPlanRequest,
        mut count_tokens: F,
    ) -> Result<ProviderEmbeddingPlanInspection, ProviderEmbeddingPlanError>
    where
        F: FnMut(&ProviderModelContract, &str) -> ProviderResult<u64>,
    {
        let prepared = self.prepare_provider_plan(request, &mut count_tokens, &mut || Ok(()))?;
        let runtime = StandardProviderExecutionRuntime::new();
        ProviderExecutionController::new(&request.contract, request.execution_limits, &runtime)?;

        let (batches, input_bytes, input_tokens) =
            plan_batches(request, prepared.options, &prepared.documents, &mut || {
                Ok(())
            })?;

        Ok(ProviderEmbeddingPlanInspection {
            display_name: prepared.display_name,
            compatibility_id: prepared.descriptor.compatibility_id()?.to_hex(),
            source_fingerprint: prepared.source.fingerprint().to_hex(),
            graph_generation: prepared.source.graph_generation(),
            label: request.label.clone(),
            properties: prepared.properties,
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
            dimensions: request.dimensions,
            normalization: request.normalization,
            distance: request.distance,
            selected_nodes: prepared.documents.len(),
            input_bytes,
            input_tokens,
            batches,
            request_limits: request.request_limits,
            batch_limits: request.batch_limits,
            execution_limits: request.execution_limits,
        })
    }

    pub(crate) fn prepare_provider_plan<F, C>(
        &self,
        request: &ProviderEmbeddingPlanRequest,
        count_tokens: &mut F,
        checkpoint: &mut C,
    ) -> Result<PreparedProviderPlan, ProviderEmbeddingPlanError>
    where
        F: FnMut(&ProviderModelContract, &str) -> ProviderResult<u64> + ?Sized,
        C: FnMut() -> Result<(), SearchArtifactError> + ?Sized,
    {
        let display_name = EmbeddingDisplayName::new(&request.display_name)?;
        validate_provider_request(request)?;
        let (projection, source) = self.capture_provider_source(request, checkpoint)?;
        let normalization = storage_normalization(request.normalization);
        let options = provider_batch_options(request, normalization)?;
        let descriptor = descriptor(
            request,
            &projection.properties,
            normalization,
            storage_distance(request.distance),
        )?;
        ProviderEmbeddingPublicationRequest::new(&descriptor, &request.contract, options, 0, 0)?;
        self.preflight_provider_alias(
            display_name.as_str(),
            &descriptor.compatibility_id()?.to_hex(),
            request.replace_alias,
        )?;
        let documents =
            prepare_documents(&projection, &request.contract, count_tokens, checkpoint)?;
        Ok(PreparedProviderPlan {
            display_name: display_name.as_str().to_owned(),
            descriptor,
            source,
            properties: projection.properties,
            documents,
            options,
        })
    }

    pub(crate) fn capture_provider_source<C>(
        &self,
        request: &ProviderEmbeddingPlanRequest,
        checkpoint: &mut C,
    ) -> Result<(TextSourceProjection, EmbeddingSourceState), ProviderEmbeddingPlanError>
    where
        C: FnMut() -> Result<(), SearchArtifactError> + ?Sized,
    {
        validate_provider_request(request)?;
        let label_id = self.search_label_id(&request.label)?;
        let projection = project_text_source(
            &self.dir,
            label_id,
            Some(&request.properties),
            TextSearchLimits::default(),
            &mut *checkpoint,
        )?;
        let source = capture_projected_source(&self.dir, &projection, checkpoint)?;
        Ok((projection, source))
    }

    pub(crate) fn preflight_provider_alias(
        &self,
        display_name: &str,
        compatibility_id: &str,
        replace_alias: bool,
    ) -> Result<(), ProviderEmbeddingPlanError> {
        if !replace_alias
            && self.embedding_spaces()?.iter().any(|space| {
                space.aliases.iter().any(|alias| alias == display_name)
                    && space.compatibility_id != compatibility_id
            })
        {
            return Err(validation(
                "embedding alias already targets another compatibility identity; explicit replacement is required",
            ));
        }
        Ok(())
    }
}

fn validate_explicit_properties(properties: &[String]) -> Result<(), ProviderEmbeddingPlanError> {
    let mut canonical = properties.to_vec();
    canonical.sort_unstable();
    canonical.dedup();
    if canonical.len() != properties.len()
        || properties.iter().any(|property| {
            property.is_empty()
                || property.trim() != property
                || property.chars().any(char::is_control)
                || property == "node_uuid"
        })
    {
        return Err(validation(
            "provider embedding properties must be unique valid names",
        ));
    }
    Ok(())
}

fn validate_provider_request(
    request: &ProviderEmbeddingPlanRequest,
) -> Result<(), ProviderEmbeddingPlanError> {
    if request.properties.is_empty() {
        return Err(validation("provider embedding properties must be explicit"));
    }
    validate_explicit_properties(&request.properties)?;
    request
        .contract
        .require(ProviderCapability::DocumentEmbeddings)?;
    Ok(())
}

fn provider_batch_options(
    request: &ProviderEmbeddingPlanRequest,
    normalization: EmbeddingNormalization,
) -> Result<DocumentEmbeddingBatchOptions, ProviderEmbeddingPlanError> {
    Ok(DocumentEmbeddingBatchOptions {
        request_limits: request.request_limits,
        batch_limits: request.batch_limits,
        dimension: usize::try_from(request.dimensions)
            .map_err(|_| validation("provider dimensions cannot be represented"))?,
        normalization,
        vector_limits: VectorStoreLimits::default(),
    })
}

fn descriptor(
    request: &ProviderEmbeddingPlanRequest,
    properties: &[String],
    normalization: EmbeddingNormalization,
    distance: EmbeddingDistance,
) -> Result<EmbeddingCompatibilityDescriptor, SearchArtifactError> {
    EmbeddingCompatibilityDescriptor::new(EmbeddingCompatibilityInput {
        producer: EmbeddingProducerIdentity::Remote {
            provider: request.contract.provider().to_owned(),
            model: request.contract.model().to_owned(),
            revision: request.contract.revision().to_owned(),
            response_contract_version: request.contract.response_contract_version().to_owned(),
        },
        dimensions: request.dimensions,
        value_type: EmbeddingValueType::Float32,
        normalization,
        distance,
        tokenizer: Some(request.contract.tokenizer().clone()),
        chunking: request.contract.chunking().cloned(),
        hyperparameters: BTreeMap::new(),
        input_recipe: BTreeMap::from([
            ("format".to_owned(), INPUT_RECIPE.into()),
            ("properties".to_owned(), serde_json::json!(properties)),
        ]),
        source_projection_recipe: BTreeMap::from([
            ("label".to_owned(), request.label.clone().into()),
            ("properties".to_owned(), serde_json::json!(properties)),
        ]),
    })
}

fn prepare_documents<F, C>(
    projection: &TextSourceProjection,
    contract: &ProviderModelContract,
    count_tokens: &mut F,
    checkpoint: &mut C,
) -> Result<Vec<PreparedProviderDocument>, ProviderEmbeddingPlanError>
where
    F: FnMut(&ProviderModelContract, &str) -> ProviderResult<u64> + ?Sized,
    C: FnMut() -> Result<(), SearchArtifactError> + ?Sized,
{
    let mut prepared = Vec::new();
    for document in &projection.documents {
        checkpoint()?;
        if document.fields.is_empty() {
            continue;
        }
        let text = serde_json::to_string(&document.fields)
            .map_err(|error| validation(format!("serialize provider document: {error}")))?;
        let token_count = count_tokens(contract, &text)?;
        prepared.push(PreparedProviderDocument {
            node_uuid: document.node_uuid,
            text,
            token_count,
        });
    }
    checkpoint()?;
    Ok(prepared)
}

fn plan_batches<C>(
    request: &ProviderEmbeddingPlanRequest,
    options: DocumentEmbeddingBatchOptions,
    prepared: &[PreparedProviderDocument],
    checkpoint: &mut C,
) -> Result<(Vec<ProviderEmbeddingPlannedBatch>, usize, u64), ProviderEmbeddingPlanError>
where
    C: FnMut() -> Result<(), SearchArtifactError> + ?Sized,
{
    let inputs = prepared
        .iter()
        .map(|document| DocumentEmbeddingInput {
            node_uuid: document.node_uuid,
            text: &document.text,
            token_count: document.token_count,
        })
        .collect::<Vec<_>>();
    let batches = if inputs.is_empty() {
        Vec::new()
    } else {
        let mut artifact_failure = None;
        let plan =
            DocumentEmbeddingBatchPlan::new(&request.contract, &inputs, options, &mut || {
                match checkpoint() {
                    Ok(()) => Ok(()),
                    Err(error) => {
                        artifact_failure = Some(error);
                        Err(gf_search::ProviderError::new(
                            &request.contract,
                            gf_search::ProviderFailureClass::Cancelled,
                        ))
                    }
                }
            });
        if let Some(error) = artifact_failure {
            return Err(error.into());
        }
        let plan = plan?;
        plan.batches()
            .iter()
            .map(|batch| ProviderEmbeddingPlannedBatch {
                items: batch.inputs().len(),
                input_bytes: batch.inputs().iter().map(|input| input.text.len()).sum(),
                input_tokens: batch.inputs().iter().map(|input| input.token_count).sum(),
            })
            .collect()
    };
    let input_bytes = batches.iter().try_fold(0_usize, |total, batch| {
        total
            .checked_add(batch.input_bytes)
            .ok_or_else(|| validation("provider input bytes overflow"))
    })?;
    let input_tokens = batches.iter().try_fold(0_u64, |total, batch| {
        total
            .checked_add(batch.input_tokens)
            .ok_or_else(|| validation("provider input tokens overflow"))
    })?;
    if batches.len() > request.execution_limits.provider_calls
        || input_tokens > request.execution_limits.input_token_exposure
    {
        return Err(ProviderEmbeddingPlanError::Provider(
            gf_search::ProviderError::new(
                &request.contract,
                gf_search::ProviderFailureClass::ResourceExhausted,
            ),
        ));
    }
    Ok((batches, input_bytes, input_tokens))
}

fn capture_projected_source<C>(
    project_dir: &std::path::Path,
    projection: &TextSourceProjection,
    checkpoint: &mut C,
) -> Result<EmbeddingSourceState, ProviderEmbeddingPlanError>
where
    C: FnMut() -> Result<(), SearchArtifactError> + ?Sized,
{
    let mut labels = Vec::with_capacity(projection.documents.len() * 16);
    let mut dependencies = Vec::new();
    encode_properties(&projection.properties, &mut dependencies)?;
    for document in &projection.documents {
        checkpoint()?;
        labels.extend_from_slice(&document.node_uuid);
        encode_document(document.node_uuid, &document.fields, &mut dependencies)?;
    }
    let eligible_count = projection
        .documents
        .iter()
        .filter(|document| !document.fields.is_empty())
        .count();
    capture_embedding_source(
        project_dir,
        &[SearchSourcePart {
            name: LABEL_SOURCE_PART,
            bytes: &labels,
        }],
        &[SearchSourcePart {
            name: DEPENDENCY_SOURCE_PART,
            bytes: &dependencies,
        }],
        u64::try_from(eligible_count)
            .map_err(|_| validation("provider item count cannot be represented"))?,
        gf_search::EmbeddingSourceCaptureLimits::default(),
        checkpoint,
    )
    .map_err(Into::into)
}

fn encode_properties(
    properties: &[String],
    output: &mut Vec<u8>,
) -> Result<(), ProviderEmbeddingPlanError> {
    for property in properties {
        encode_bytes(property.as_bytes(), output)?;
    }
    Ok(())
}

fn encode_document(
    node_uuid: [u8; 16],
    fields: &BTreeMap<String, String>,
    output: &mut Vec<u8>,
) -> Result<(), ProviderEmbeddingPlanError> {
    output.extend_from_slice(&node_uuid);
    for (name, value) in fields {
        encode_bytes(name.as_bytes(), output)?;
        encode_bytes(value.as_bytes(), output)?;
    }
    Ok(())
}

fn encode_bytes(bytes: &[u8], output: &mut Vec<u8>) -> Result<(), ProviderEmbeddingPlanError> {
    let length = u64::try_from(bytes.len())
        .map_err(|_| validation("provider source field is too large to encode"))?;
    output.extend_from_slice(&length.to_le_bytes());
    output.extend_from_slice(bytes);
    Ok(())
}

const fn storage_normalization(value: ProviderEmbeddingNormalization) -> EmbeddingNormalization {
    match value {
        ProviderEmbeddingNormalization::None => EmbeddingNormalization::None,
        ProviderEmbeddingNormalization::L2 => EmbeddingNormalization::L2,
    }
}

const fn storage_distance(value: ProviderEmbeddingDistance) -> EmbeddingDistance {
    match value {
        ProviderEmbeddingDistance::Cosine => EmbeddingDistance::Cosine,
    }
}

fn validation(message: impl Into<String>) -> ProviderEmbeddingPlanError {
    ProviderEmbeddingPlanError::Api(GfError::Validation(message.into()))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use gf_search::{ProviderCapabilities, ProviderFailureClass};
    use gf_storage::{TokenCountClass, TokenizerIdentity};

    use super::*;
    use crate::PropValue;

    fn contract(max_input_tokens: u64) -> ProviderModelContract {
        ProviderModelContract::remote(
            None,
            "vendor/model",
            "revision-1",
            "wire-v1",
            ProviderCapabilities::new([ProviderCapability::DocumentEmbeddings]).unwrap(),
            TokenizerIdentity {
                identifier: "test-tokenizer".to_owned(),
                version: "v1".to_owned(),
                count_class: TokenCountClass::ExactLocal,
                max_input_tokens,
                normalization: "nfc".to_owned(),
            },
            None,
        )
        .unwrap()
    }

    fn request(properties: &[&str], max_input_tokens: u64) -> ProviderEmbeddingPlanRequest {
        ProviderEmbeddingPlanRequest {
            display_name: "semantic".to_owned(),
            label: "Document".to_owned(),
            properties: properties.iter().map(|value| (*value).to_owned()).collect(),
            contract: contract(max_input_tokens),
            dimensions: 2,
            normalization: ProviderEmbeddingNormalization::L2,
            distance: ProviderEmbeddingDistance::Cosine,
            request_limits: ProviderRequestLimits::default(),
            batch_limits: ProviderBatchLimits {
                items: 1,
                input_bytes: 1_024,
                input_tokens: 1_024,
            },
            execution_limits: ProviderExecutionLimits {
                timeout: Duration::from_secs(5),
                ..ProviderExecutionLimits::default()
            },
            replace_alias: false,
        }
    }

    fn add_document(graph: &GraphForge, title: Option<&str>, body: Option<&str>) {
        let mut properties = HashMap::new();
        if let Some(title) = title {
            properties.insert("title".to_owned(), PropValue::Str(title.to_owned()));
        }
        if let Some(body) = body {
            properties.insert("body".to_owned(), PropValue::Str(body.to_owned()));
        }
        graph.add_node("Document", &properties).unwrap();
    }

    #[test]
    fn inspection_is_order_independent_content_free_and_openrouter_explicit() {
        let graph = GraphForge::new(None).unwrap();
        add_document(&graph, Some("secret title"), Some("private body"));
        add_document(&graph, Some("second title"), Some("second body"));

        let first = graph
            .inspect_provider_embedding_plan(&request(&["title", "body"], 1_024), |_, text| {
                Ok(u64::try_from(text.len()).unwrap())
            })
            .unwrap();
        let reordered = graph
            .inspect_provider_embedding_plan(&request(&["body", "title"], 1_024), |_, text| {
                Ok(u64::try_from(text.len()).unwrap())
            })
            .unwrap();

        assert_eq!(first.provider, "openrouter");
        assert_eq!(first.tokenizer_identifier, "test-tokenizer");
        assert_eq!(first.tokenizer_version, "v1");
        assert_eq!(first.token_count_class, TokenCountClass::ExactLocal);
        assert_eq!(first.model_input_tokens, 1_024);
        assert_eq!(first.tokenizer_normalization, "nfc");
        assert_eq!(first.chunking, None);
        assert_eq!(first.properties, ["body", "title"]);
        assert_eq!(first.selected_nodes, 2);
        assert_eq!(first.batches.len(), 2);
        assert_eq!(first.compatibility_id, reordered.compatibility_id);
        assert_eq!(first.source_fingerprint, reordered.source_fingerprint);
        assert_eq!(first.batches, reordered.batches);
        let diagnostic = format!("{first:?}");
        assert!(!diagnostic.contains("secret title"));
        assert!(!diagnostic.contains("private body"));
    }

    #[test]
    fn empty_projection_is_stable_and_never_calls_the_counter() {
        let graph = GraphForge::new(None).unwrap();
        add_document(&graph, None, None);
        let mut calls = 0_usize;
        let inspection = graph
            .inspect_provider_embedding_plan(&request(&["body"], 64), |_, _| {
                calls += 1;
                Ok(1)
            })
            .unwrap();

        assert_eq!(calls, 0);
        assert_eq!(inspection.selected_nodes, 0);
        assert_eq!(inspection.input_bytes, 0);
        assert_eq!(inspection.input_tokens, 0);
        assert!(inspection.batches.is_empty());
    }

    #[test]
    fn duplicate_properties_token_limits_and_missing_capability_fail_closed() {
        let graph = GraphForge::new(None).unwrap();
        add_document(&graph, None, Some("private body"));
        assert!(matches!(
            graph.inspect_provider_embedding_plan(&request(&["body", "body"], 64), |_, _| Ok(1)),
            Err(ProviderEmbeddingPlanError::Api(GfError::Validation(_)))
        ));
        assert!(matches!(
            graph.inspect_provider_embedding_plan(&request(&["body"], 2), |_, _| Ok(3)),
            Err(ProviderEmbeddingPlanError::Provider(ref error))
                if error.class() == ProviderFailureClass::ResourceExhausted
        ));

        let mut unsupported = request(&["body"], 64);
        unsupported.contract = ProviderModelContract::remote(
            None,
            "vendor/reranker",
            "revision-1",
            "wire-v1",
            ProviderCapabilities::new([ProviderCapability::CandidateReranking]).unwrap(),
            unsupported.contract.tokenizer().clone(),
            None,
        )
        .unwrap();
        assert!(matches!(
            graph.inspect_provider_embedding_plan(&unsupported, |_, _| Ok(1)),
            Err(ProviderEmbeddingPlanError::Provider(ref error))
                if error.class() == ProviderFailureClass::UnsupportedCapability
        ));
    }
}
