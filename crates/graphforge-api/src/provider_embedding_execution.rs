//! Bounded provider execution and atomic embedding-generation publication.

use std::cell::Cell;
use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::sync::MutexGuard;
use std::time::{SystemTime, UNIX_EPOCH};

use graphforge_search::{
    DocumentEmbeddingBatchPlan, DocumentEmbeddingInput, DocumentEmbeddingProvider,
    EmbeddingRefreshLimits, ProviderBatchCostEstimator, ProviderEmbeddingPublicationRequest,
    ProviderError, ProviderExecutionController, ProviderExecutionRuntime, ProviderFailureClass,
    ProviderModelContract, ProviderPublicationError, ProviderResult,
    execute_document_embedding_batches, publish_provider_embedding_generation,
};
use graphforge_storage::embedding_refresh_config::{
    EmbeddingRefreshOutcomeAttempt, record_embedding_refresh_outcome,
};
use graphforge_storage::{
    EmbeddingCompatibilityId, EmbeddingNormalization, EmbeddingRefreshConfigLimits,
    EmbeddingRefreshFailureClass, EmbeddingRefreshOutcomeStatus, EmbeddingSourceState,
    SearchArtifactError, ValidatedEmbeddingBatch, validate_embedding_batch,
};

use super::provider_embedding::{
    PreparedProviderDocument, PreparedProviderPlan, ProviderEmbeddingPlanError,
    ProviderEmbeddingPlanRequest,
};
use super::{EmbeddingRefreshInspection, EmbeddingSpaceInfo, GfError, GraphForge};

/// Exact tokenizer counter used by provider plan preparation.
pub type ProviderTokenCounter<'a> =
    dyn FnMut(&ProviderModelContract, &str) -> ProviderResult<u64> + 'a;

/// Cooperative artifact/source cancellation checkpoint.
pub type ProviderArtifactCheckpoint<'a> = dyn FnMut() -> Result<(), SearchArtifactError> + 'a;

/// Borrowed runtime dependencies for one provider publication.
///
/// This bundle deliberately omits `Debug`: callbacks, credentials, and
/// transports remain runtime-only and cannot enter diagnostics.
pub struct ProviderEmbeddingExecution<'a> {
    provider: &'a mut dyn DocumentEmbeddingProvider,
    runtime: &'a mut dyn ProviderExecutionRuntime,
    count_tokens: &'a mut ProviderTokenCounter<'a>,
    estimate_cost: &'a mut ProviderBatchCostEstimator<'a>,
    checkpoint: &'a mut ProviderArtifactCheckpoint<'a>,
}

impl<'a> ProviderEmbeddingExecution<'a> {
    /// Assemble explicit provider execution dependencies without owning them.
    #[must_use]
    pub fn new(
        provider: &'a mut dyn DocumentEmbeddingProvider,
        runtime: &'a mut dyn ProviderExecutionRuntime,
        count_tokens: &'a mut ProviderTokenCounter<'a>,
        estimate_cost: &'a mut ProviderBatchCostEstimator<'a>,
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

/// Structured plan, provider-publication, or final facade failure.
#[derive(Debug)]
pub enum ProviderEmbeddingExecutionError {
    /// Explicit plan preparation or source validation failed.
    Plan(ProviderEmbeddingPlanError),
    /// Provider execution or atomic publication failed.
    Publication(ProviderPublicationError),
    /// The complete generation published, but final alias inspection failed.
    Api(GfError),
}

impl fmt::Display for ProviderEmbeddingExecutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Plan(error) => error.fmt(formatter),
            Self::Publication(error) => error.fmt(formatter),
            Self::Api(error) => error.fmt(formatter),
        }
    }
}

impl Error for ProviderEmbeddingExecutionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Plan(error) => Some(error),
            Self::Publication(error) => Some(error),
            Self::Api(error) => Some(error),
        }
    }
}

impl From<ProviderEmbeddingPlanError> for ProviderEmbeddingExecutionError {
    fn from(error: ProviderEmbeddingPlanError) -> Self {
        Self::Plan(error)
    }
}

impl From<ProviderPublicationError> for ProviderEmbeddingExecutionError {
    fn from(error: ProviderPublicationError) -> Self {
        Self::Publication(error)
    }
}

impl From<GfError> for ProviderEmbeddingExecutionError {
    fn from(error: GfError) -> Self {
        Self::Api(error)
    }
}

impl From<ProviderError> for ProviderEmbeddingExecutionError {
    fn from(error: ProviderError) -> Self {
        Self::Publication(ProviderPublicationError::Provider(error))
    }
}

impl From<SearchArtifactError> for ProviderEmbeddingExecutionError {
    fn from(error: SearchArtifactError) -> Self {
        Self::Publication(ProviderPublicationError::Artifact(error))
    }
}

impl GraphForge {
    /// Execute and atomically publish one explicit provider embedding plan.
    ///
    /// The complete projection, tokenization, provider work, and validation are
    /// rebuilt for the single bounded mutation retry. Execution budgets span
    /// both attempts. The display alias is bound only after a complete UUID
    /// generation is active.
    ///
    /// # Errors
    /// Returns structured plan, redacted provider, cancellation, source,
    /// concurrency, persistence, or alias failures. Failed work never publishes
    /// a partial batch or mutates the requested alias.
    pub fn publish_provider_embeddings(
        &self,
        request: &ProviderEmbeddingPlanRequest,
        execution: ProviderEmbeddingExecution<'_>,
    ) -> Result<EmbeddingSpaceInfo, ProviderEmbeddingExecutionError> {
        self.execute_provider_embeddings(request, execution, None)
            .map(|(space, _)| space)
    }

    /// Explicitly refresh one existing provider-produced embedding space.
    ///
    /// The request display name must already resolve to the exact compatibility
    /// lineage produced by the request. Identity drift fails before outbound
    /// provider work. The existing bounded atomic publication path is reused,
    /// and the exact attempted source plus a content-free terminal outcome are
    /// persisted for refresh inspection.
    ///
    /// # Errors
    /// Returns structured alias/default, identity, provider, cancellation,
    /// concurrency, persistence, or outcome-recording failures. A secondary
    /// failure while recording a failed attempt never replaces the original.
    pub fn refresh_provider_embeddings(
        &self,
        request: &ProviderEmbeddingPlanRequest,
        execution: ProviderEmbeddingExecution<'_>,
    ) -> Result<EmbeddingRefreshInspection, ProviderEmbeddingExecutionError> {
        self.execute_provider_embedding_refresh(request, execution)?;
        self.inspect_embedding_refresh(Some(&request.display_name))
            .map_err(Into::into)
    }

    pub(crate) fn execute_provider_embedding_refresh(
        &self,
        request: &ProviderEmbeddingPlanRequest,
        execution: ProviderEmbeddingExecution<'_>,
    ) -> Result<(), ProviderEmbeddingExecutionError> {
        let (_, lineage) = self
            .resolve_embedding_space_lineage(Some(&request.display_name))
            .map_err(ProviderEmbeddingExecutionError::Api)?;
        self.execute_provider_embeddings(request, execution, Some(lineage.compatibility_id()))?;
        Ok(())
    }

    fn begin_provider_publication_visibility(
        &self,
    ) -> Result<MutexGuard<'_, ()>, SearchArtifactError> {
        self.embedding_refresh_visibility.lock().map_err(|_| {
            SearchArtifactError::Build("embedding refresh visibility lock is poisoned".to_owned())
        })
    }

    fn execute_provider_embeddings(
        &self,
        request: &ProviderEmbeddingPlanRequest,
        execution: ProviderEmbeddingExecution<'_>,
        refresh_lineage: Option<EmbeddingCompatibilityId>,
    ) -> Result<(EmbeddingSpaceInfo, EmbeddingSourceState), ProviderEmbeddingExecutionError> {
        let ProviderEmbeddingExecution {
            provider,
            runtime,
            count_tokens,
            estimate_cost,
            checkpoint,
        } = execution;
        let prepared = self.prepare_provider_plan(request, count_tokens, checkpoint)?;
        let attempted_source = Cell::new(prepared.source);
        if let Some(expected) = refresh_lineage
            && prepared.descriptor.compatibility_id()? != expected
        {
            let error = ProviderEmbeddingExecutionError::Publication(
                SearchArtifactError::InvalidSelector {
                    field: "provider refresh compatibility",
                    reason: "request identity does not match the selected embedding space"
                        .to_owned(),
                }
                .into(),
            );
            record_failed_refresh(self, expected, attempted_source.get(), &error);
            return Err(error);
        }
        if let Err(error) = validate_provider_contract(provider, &request.contract) {
            if let Some(lineage) = refresh_lineage {
                record_failed_refresh(self, lineage, attempted_source.get(), &error);
            }
            return Err(error);
        }
        let descriptor = prepared.descriptor.clone();
        let now = transaction_time_micros();
        let publication = ProviderEmbeddingPublicationRequest::new(
            &descriptor,
            &request.contract,
            prepared.options,
            now,
            now,
        )?;
        let mut controller =
            ProviderExecutionController::new(&request.contract, request.execution_limits, runtime)?;
        let mut first_attempt = Some(prepared);

        let result = publish_provider_embedding_generation(
            &self.dir,
            &publication,
            EmbeddingRefreshLimits::default(),
            |artifact_checkpoint| {
                let prepared = match first_attempt.take() {
                    Some(prepared) => prepared,
                    None => self
                        .prepare_provider_plan(request, count_tokens, artifact_checkpoint)
                        .map_err(publication_plan_error)?,
                };
                attempted_source.set(prepared.source);
                if prepared.descriptor != descriptor {
                    return Err(ProviderPublicationError::Provider(ProviderError::new(
                        &request.contract,
                        ProviderFailureClass::InvalidRequest,
                    )));
                }
                let source = prepared.source;
                let batch = execute_prepared_plan(
                    provider,
                    &prepared,
                    &mut controller,
                    runtime,
                    estimate_cost,
                    artifact_checkpoint,
                )?;
                Ok((source, batch))
            },
            || {
                self.capture_provider_source(request, &mut || Ok(()))
                    .map(|(_, source)| source)
                    .map_err(plan_artifact_error)
            },
            || self.begin_provider_publication_visibility(),
            checkpoint,
        )
        .map_err(ProviderEmbeddingExecutionError::from)
        .and_then(|_| {
            self.bind_embedding_space_alias(
                &request.display_name,
                &descriptor.compatibility_id()?.to_hex(),
                request.replace_alias,
            )
            .map_err(Into::into)
        })
        .map(|space| (space, attempted_source.get()));

        if let Some(lineage) = refresh_lineage {
            match &result {
                Ok((_, source)) => record_refresh_outcome(
                    self,
                    lineage,
                    *source,
                    EmbeddingRefreshOutcomeStatus::Succeeded,
                )?,
                Err(error) => record_failed_refresh(self, lineage, attempted_source.get(), error),
            }
        }
        result
    }
}

fn validate_provider_contract(
    provider: &dyn DocumentEmbeddingProvider,
    contract: &ProviderModelContract,
) -> Result<(), ProviderEmbeddingExecutionError> {
    if provider.contract() == contract {
        return Ok(());
    }
    Err(ProviderPublicationError::Provider(ProviderError::new(
        contract,
        ProviderFailureClass::InvalidRequest,
    ))
    .into())
}

fn record_failed_refresh(
    graph: &GraphForge,
    lineage: EmbeddingCompatibilityId,
    source: EmbeddingSourceState,
    error: &ProviderEmbeddingExecutionError,
) {
    let _ = record_refresh_outcome(graph, lineage, source, outcome_status(error));
}

fn record_refresh_outcome(
    graph: &GraphForge,
    lineage: EmbeddingCompatibilityId,
    source: EmbeddingSourceState,
    status: EmbeddingRefreshOutcomeStatus,
) -> Result<(), ProviderEmbeddingExecutionError> {
    record_embedding_refresh_outcome(
        &graph.dir,
        lineage,
        EmbeddingRefreshOutcomeAttempt {
            status,
            graph_generation: source.graph_generation(),
            source_fingerprint: source.fingerprint(),
        },
        EmbeddingRefreshConfigLimits::default(),
        || Ok(()),
    )?;
    graph.publish_workspace_update()?;
    Ok(())
}

fn outcome_status(error: &ProviderEmbeddingExecutionError) -> EmbeddingRefreshOutcomeStatus {
    match error {
        ProviderEmbeddingExecutionError::Plan(ProviderEmbeddingPlanError::Provider(error))
        | ProviderEmbeddingExecutionError::Publication(ProviderPublicationError::Provider(error)) => {
            provider_outcome(error.class())
        }
        ProviderEmbeddingExecutionError::Plan(ProviderEmbeddingPlanError::Artifact(error))
        | ProviderEmbeddingExecutionError::Publication(ProviderPublicationError::Artifact(error)) => {
            artifact_outcome(error)
        }
        ProviderEmbeddingExecutionError::Plan(ProviderEmbeddingPlanError::Api(error))
        | ProviderEmbeddingExecutionError::Api(error) => api_outcome(error),
    }
}

const fn provider_outcome(class: ProviderFailureClass) -> EmbeddingRefreshOutcomeStatus {
    match class {
        ProviderFailureClass::Cancelled => EmbeddingRefreshOutcomeStatus::Cancelled,
        ProviderFailureClass::ResourceExhausted => {
            EmbeddingRefreshOutcomeStatus::Failed(EmbeddingRefreshFailureClass::ResourceExhausted)
        }
        ProviderFailureClass::InvalidRequest | ProviderFailureClass::MalformedResponse => {
            EmbeddingRefreshOutcomeStatus::Failed(EmbeddingRefreshFailureClass::Validation)
        }
        ProviderFailureClass::UnsupportedCapability => {
            EmbeddingRefreshOutcomeStatus::Failed(EmbeddingRefreshFailureClass::Incompatible)
        }
        ProviderFailureClass::Authentication => {
            EmbeddingRefreshOutcomeStatus::Failed(EmbeddingRefreshFailureClass::Unavailable)
        }
        ProviderFailureClass::Timeout
        | ProviderFailureClass::Transport
        | ProviderFailureClass::ProviderRejected => {
            EmbeddingRefreshOutcomeStatus::Failed(EmbeddingRefreshFailureClass::Provider)
        }
    }
}

fn artifact_outcome(error: &SearchArtifactError) -> EmbeddingRefreshOutcomeStatus {
    let class = match error {
        SearchArtifactError::Cancelled => return EmbeddingRefreshOutcomeStatus::Cancelled,
        SearchArtifactError::ResourceExhausted { .. } => {
            EmbeddingRefreshFailureClass::ResourceExhausted
        }
        SearchArtifactError::ConcurrentMutation => EmbeddingRefreshFailureClass::ConcurrentMutation,
        SearchArtifactError::InvalidSelector { .. } => EmbeddingRefreshFailureClass::Incompatible,
        SearchArtifactError::CorruptManifest { .. }
        | SearchArtifactError::CorruptDerivedIndex { .. }
        | SearchArtifactError::CorruptPrimaryVectors { .. } => {
            EmbeddingRefreshFailureClass::Corrupt
        }
        SearchArtifactError::IncompatibleManifest { .. } | SearchArtifactError::Stale { .. } => {
            EmbeddingRefreshFailureClass::Incompatible
        }
        SearchArtifactError::Missing { .. } => EmbeddingRefreshFailureClass::Unavailable,
        SearchArtifactError::Build(_)
        | SearchArtifactError::SourceSnapshot { .. }
        | SearchArtifactError::Lock { .. }
        | SearchArtifactError::Io { .. } => EmbeddingRefreshFailureClass::Storage,
    };
    EmbeddingRefreshOutcomeStatus::Failed(class)
}

fn api_outcome(error: &GfError) -> EmbeddingRefreshOutcomeStatus {
    let class = match error {
        GfError::Storage(_) | GfError::Project { .. } => EmbeddingRefreshFailureClass::Storage,
        GfError::Lifecycle(_) => EmbeddingRefreshFailureClass::ConcurrentMutation,
        GfError::NotImplemented(_) => EmbeddingRefreshFailureClass::Unavailable,
        GfError::Execution(_) | GfError::Provider { .. } => EmbeddingRefreshFailureClass::Provider,
        GfError::Parse { .. }
        | GfError::Bind { .. }
        | GfError::Plan(_)
        | GfError::Api { .. }
        | GfError::Validation(_)
        | GfError::Ontology(_) => EmbeddingRefreshFailureClass::Validation,
    };
    EmbeddingRefreshOutcomeStatus::Failed(class)
}

fn execute_prepared_plan<C>(
    provider: &mut dyn DocumentEmbeddingProvider,
    prepared: &PreparedProviderPlan,
    controller: &mut ProviderExecutionController,
    runtime: &mut dyn ProviderExecutionRuntime,
    estimate_cost: &mut ProviderBatchCostEstimator<'_>,
    checkpoint: &mut C,
) -> Result<ValidatedEmbeddingBatch, ProviderPublicationError>
where
    C: FnMut() -> Result<(), SearchArtifactError> + ?Sized,
{
    if prepared.documents.is_empty() {
        return validate_embedding_batch(
            Vec::new(),
            &BTreeSet::new(),
            prepared.options.dimension,
            EmbeddingNormalization::None,
            prepared.options.vector_limits,
            checkpoint,
        )
        .map_err(Into::into);
    }
    let inputs = provider_inputs(&prepared.documents);
    let contract = provider.contract().clone();
    let plan = with_artifact_checkpoint(&contract, checkpoint, |provider_checkpoint| {
        DocumentEmbeddingBatchPlan::new(&contract, &inputs, prepared.options, provider_checkpoint)
    })?;
    with_artifact_checkpoint(&contract, checkpoint, |provider_checkpoint| {
        execute_document_embedding_batches(
            provider,
            &plan,
            controller,
            runtime,
            estimate_cost,
            provider_checkpoint,
        )
    })
}

fn provider_inputs(documents: &[PreparedProviderDocument]) -> Vec<DocumentEmbeddingInput<'_>> {
    documents
        .iter()
        .map(|document| DocumentEmbeddingInput {
            node_uuid: document.node_uuid,
            text: &document.text,
            token_count: document.token_count,
        })
        .collect()
}

fn with_artifact_checkpoint<T, C, F>(
    contract: &ProviderModelContract,
    checkpoint: &mut C,
    operation: F,
) -> Result<T, ProviderPublicationError>
where
    C: FnMut() -> Result<(), SearchArtifactError> + ?Sized,
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

fn publication_plan_error(error: ProviderEmbeddingPlanError) -> ProviderPublicationError {
    match error {
        ProviderEmbeddingPlanError::Artifact(error) => error.into(),
        ProviderEmbeddingPlanError::Provider(error) => error.into(),
        ProviderEmbeddingPlanError::Api(error) => {
            SearchArtifactError::Build(error.to_string()).into()
        }
    }
}

fn plan_artifact_error(error: ProviderEmbeddingPlanError) -> SearchArtifactError {
    match error {
        ProviderEmbeddingPlanError::Artifact(error) => error,
        ProviderEmbeddingPlanError::Provider(error) => {
            SearchArtifactError::Build(error.to_string())
        }
        ProviderEmbeddingPlanError::Api(error) => SearchArtifactError::Build(error.to_string()),
    }
}

fn transaction_time_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            i64::try_from(duration.as_micros()).unwrap_or(i64::MAX)
        })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use graphforge_search::{
        DocumentEmbeddingOutput, DocumentEmbeddingRequest, ProviderBatchLimits, ProviderBatchShape,
        ProviderCapabilities, ProviderCapability, ProviderExecutionLimits, ProviderRequestLimits,
        StandardProviderExecutionRuntime,
    };
    use graphforge_storage::{TokenCountClass, TokenizerIdentity};

    use super::*;
    use crate::{
        PropValue, ProviderEmbeddingDistance, ProviderEmbeddingNormalization,
        ProviderEmbeddingPlanRequest,
    };

    struct FakeProvider<'a> {
        contract: ProviderModelContract,
        mutate: Option<&'a GraphForge>,
        mutate_every_call: bool,
        failure: Option<ProviderFailureClass>,
        calls: usize,
    }

    impl DocumentEmbeddingProvider for FakeProvider<'_> {
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
            if let Some(class) = self.failure {
                return Err(ProviderError::new(&self.contract, class));
            }
            if let Some(graph) = self.mutate
                && (self.calls == 1 || self.mutate_every_call)
            {
                add_document(graph, &format!("mutation-{}", self.calls));
            }
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

    fn contract(model: &str) -> ProviderModelContract {
        ProviderModelContract::remote(
            None,
            model,
            "revision-1",
            "wire-v1",
            ProviderCapabilities::new([ProviderCapability::DocumentEmbeddings]).unwrap(),
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

    fn request() -> ProviderEmbeddingPlanRequest {
        ProviderEmbeddingPlanRequest {
            display_name: "semantic".to_owned(),
            label: "Document".to_owned(),
            properties: vec!["body".to_owned()],
            contract: contract("vendor/model"),
            dimensions: 2,
            normalization: ProviderEmbeddingNormalization::L2,
            distance: ProviderEmbeddingDistance::Cosine,
            request_limits: ProviderRequestLimits::default(),
            batch_limits: ProviderBatchLimits::default(),
            execution_limits: ProviderExecutionLimits {
                retries: 0,
                timeout: Duration::from_secs(5),
                ..ProviderExecutionLimits::default()
            },
            replace_alias: false,
        }
    }

    fn add_document(graph: &GraphForge, body: &str) {
        graph
            .add_node(
                "Document",
                &HashMap::from([("body".to_owned(), PropValue::Str(body.to_owned()))]),
            )
            .unwrap();
    }

    fn publish(
        graph: &GraphForge,
        request: &ProviderEmbeddingPlanRequest,
        provider: &mut FakeProvider<'_>,
    ) -> Result<EmbeddingSpaceInfo, ProviderEmbeddingExecutionError> {
        let mut runtime = StandardProviderExecutionRuntime::new();
        let mut count_tokens =
            |_: &ProviderModelContract, text: &str| Ok(u64::try_from(text.len()).unwrap());
        let mut estimate_cost =
            |shape: ProviderBatchShape| Ok(u64::try_from(shape.items()).unwrap());
        let mut checkpoint = || Ok(());
        graph.publish_provider_embeddings(
            request,
            ProviderEmbeddingExecution::new(
                provider,
                &mut runtime,
                &mut count_tokens,
                &mut estimate_cost,
                &mut checkpoint,
            ),
        )
    }

    fn refresh(
        graph: &GraphForge,
        request: &ProviderEmbeddingPlanRequest,
        provider: &mut FakeProvider<'_>,
    ) -> Result<EmbeddingRefreshInspection, ProviderEmbeddingExecutionError> {
        let mut runtime = StandardProviderExecutionRuntime::new();
        let mut count_tokens =
            |_: &ProviderModelContract, text: &str| Ok(u64::try_from(text.len()).unwrap());
        let mut estimate_cost =
            |shape: ProviderBatchShape| Ok(u64::try_from(shape.items()).unwrap());
        let mut checkpoint = || Ok(());
        graph.refresh_provider_embeddings(
            request,
            ProviderEmbeddingExecution::new(
                provider,
                &mut runtime,
                &mut count_tokens,
                &mut estimate_cost,
                &mut checkpoint,
            ),
        )
    }

    #[test]
    fn complete_generation_is_atomic_aliased_and_reopenable() {
        let directory = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(directory.path().to_str()).unwrap();
        add_document(&graph, "private body");
        let request = request();
        let mut provider = FakeProvider {
            contract: request.contract.clone(),
            mutate: None,
            mutate_every_call: false,
            failure: None,
            calls: 0,
        };

        let published = publish(&graph, &request, &mut provider).unwrap();
        assert_eq!(provider.calls, 1);
        assert_eq!(published.aliases, ["semantic"]);
        assert_eq!(published.active.as_ref().unwrap().vector_count, 1);
        drop(graph);

        let reopened = GraphForge::new(directory.path().to_str()).unwrap();
        assert_eq!(
            reopened
                .embedding_space(Some("semantic"))
                .unwrap()
                .active
                .unwrap()
                .vector_count,
            1
        );
    }

    #[test]
    fn mutation_rebuilds_once_and_second_mutation_fails_without_alias() {
        let graph = GraphForge::new(None).unwrap();
        add_document(&graph, "first");
        let request = request();
        let mut once = FakeProvider {
            contract: request.contract.clone(),
            mutate: Some(&graph),
            mutate_every_call: false,
            failure: None,
            calls: 0,
        };

        let published = publish(&graph, &request, &mut once).unwrap();
        assert_eq!(once.calls, 2);
        assert_eq!(published.active.unwrap().vector_count, 2);

        let another = GraphForge::new(None).unwrap();
        add_document(&another, "first");
        let mut twice = FakeProvider {
            contract: request.contract.clone(),
            mutate: Some(&another),
            mutate_every_call: true,
            failure: None,
            calls: 0,
        };
        let error = publish(&another, &request, &mut twice).unwrap_err();
        assert!(matches!(
            error,
            ProviderEmbeddingExecutionError::Publication(ProviderPublicationError::Artifact(
                SearchArtifactError::ConcurrentMutation
            ))
        ));
        assert_eq!(twice.calls, 2);
        assert!(another.embedding_spaces().unwrap().is_empty());
    }

    #[test]
    fn empty_projection_and_contract_mismatch_never_call_provider() {
        let graph = GraphForge::new(None).unwrap();
        graph.add_node("Document", &HashMap::new()).unwrap();
        let request = request();
        let mut empty = FakeProvider {
            contract: request.contract.clone(),
            mutate: None,
            mutate_every_call: false,
            failure: None,
            calls: 0,
        };
        let published = publish(&graph, &request, &mut empty).unwrap();
        assert_eq!(empty.calls, 0);
        assert_eq!(published.active.unwrap().vector_count, 0);

        let other = GraphForge::new(None).unwrap();
        add_document(&other, "private");
        let mut mismatch = FakeProvider {
            contract: contract("vendor/other"),
            mutate: None,
            mutate_every_call: false,
            failure: None,
            calls: 0,
        };
        let error = publish(&other, &request, &mut mismatch).unwrap_err();
        assert!(matches!(
            error,
            ProviderEmbeddingExecutionError::Publication(
                ProviderPublicationError::Provider(ref error)
            ) if error.class() == ProviderFailureClass::InvalidRequest
        ));
        assert_eq!(mismatch.calls, 0);
        assert!(other.embedding_spaces().unwrap().is_empty());
    }

    #[test]
    fn cancellation_is_structured_before_provider_or_alias_mutation() {
        let graph = GraphForge::new(None).unwrap();
        add_document(&graph, "private");
        let request = request();
        let mut provider = FakeProvider {
            contract: request.contract.clone(),
            mutate: None,
            mutate_every_call: false,
            failure: None,
            calls: 0,
        };
        let mut runtime = StandardProviderExecutionRuntime::new();
        let mut count_tokens = |_: &ProviderModelContract, _: &str| Ok(1);
        let mut estimate_cost = |_: ProviderBatchShape| Ok(0);
        let mut checkpoint = || Err(SearchArtifactError::Cancelled);

        let error = graph
            .publish_provider_embeddings(
                &request,
                ProviderEmbeddingExecution::new(
                    &mut provider,
                    &mut runtime,
                    &mut count_tokens,
                    &mut estimate_cost,
                    &mut checkpoint,
                ),
            )
            .unwrap_err();
        assert!(matches!(
            error,
            ProviderEmbeddingExecutionError::Plan(ProviderEmbeddingPlanError::Artifact(
                SearchArtifactError::Cancelled
            ))
        ));
        assert_eq!(provider.calls, 0);
        assert!(graph.embedding_spaces().unwrap().is_empty());
    }

    #[test]
    fn explicit_refresh_records_exact_success_and_survives_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(directory.path().to_str()).unwrap();
        add_document(&graph, "first");
        let request = request();
        let mut initial = FakeProvider {
            contract: request.contract.clone(),
            mutate: None,
            mutate_every_call: false,
            failure: None,
            calls: 0,
        };
        publish(&graph, &request, &mut initial).unwrap();
        add_document(&graph, "second");

        let mut provider = FakeProvider {
            contract: request.contract.clone(),
            mutate: None,
            mutate_every_call: false,
            failure: None,
            calls: 0,
        };
        let refreshed = refresh(&graph, &request, &mut provider).unwrap();
        assert_eq!(provider.calls, 1);
        let outcome = refreshed.last_outcome.unwrap();
        assert_eq!(outcome.status, EmbeddingRefreshOutcomeStatus::Succeeded);
        assert_eq!(
            outcome.graph_generation,
            graphforge_storage::generation::read_search_generation(&graph.dir).unwrap()
        );
        assert_eq!(
            refreshed.freshness.as_ref().unwrap().compatibility_id,
            refreshed.compatibility_id
        );

        drop(graph);
        let reopened = GraphForge::new(directory.path().to_str()).unwrap();
        assert_eq!(
            reopened
                .inspect_embedding_refresh(Some("semantic"))
                .unwrap()
                .last_outcome,
            Some(outcome)
        );
    }

    #[test]
    fn refresh_identity_provider_failure_and_cancellation_are_content_free() {
        let graph = GraphForge::new(None).unwrap();
        add_document(&graph, "private");
        let base_request = request();
        let mut initial = FakeProvider {
            contract: base_request.contract.clone(),
            mutate: None,
            mutate_every_call: false,
            failure: None,
            calls: 0,
        };
        publish(&graph, &base_request, &mut initial).unwrap();

        let mut drift = request();
        drift.contract = contract("vendor/drifted");
        let mut mismatched = FakeProvider {
            contract: drift.contract.clone(),
            mutate: None,
            mutate_every_call: false,
            failure: None,
            calls: 0,
        };
        assert!(refresh(&graph, &drift, &mut mismatched).is_err());
        assert_eq!(mismatched.calls, 0);

        let mut failed = FakeProvider {
            contract: base_request.contract.clone(),
            mutate: None,
            mutate_every_call: false,
            failure: Some(ProviderFailureClass::Transport),
            calls: 0,
        };
        let original = refresh(&graph, &base_request, &mut failed).unwrap_err();
        assert!(matches!(
            original,
            ProviderEmbeddingExecutionError::Publication(
                ProviderPublicationError::Provider(ref error)
            ) if error.class() == ProviderFailureClass::Transport
        ));
        assert_eq!(failed.calls, 1);
        assert!(matches!(
            graph
                .inspect_embedding_refresh(Some("semantic"))
                .unwrap()
                .last_outcome
                .unwrap()
                .status,
            EmbeddingRefreshOutcomeStatus::Failed(EmbeddingRefreshFailureClass::Provider)
        ));

        let mut cancelled = FakeProvider {
            contract: base_request.contract.clone(),
            mutate: None,
            mutate_every_call: false,
            failure: Some(ProviderFailureClass::Cancelled),
            calls: 0,
        };
        let original = refresh(&graph, &base_request, &mut cancelled).unwrap_err();
        assert!(matches!(
            original,
            ProviderEmbeddingExecutionError::Publication(
                ProviderPublicationError::Provider(ref error)
            ) if error.class() == ProviderFailureClass::Cancelled
        ));
        assert_eq!(cancelled.calls, 1);
        assert_eq!(
            graph
                .inspect_embedding_refresh(Some("semantic"))
                .unwrap()
                .last_outcome
                .unwrap()
                .status,
            EmbeddingRefreshOutcomeStatus::Cancelled
        );
    }
}
