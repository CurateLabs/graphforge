//! Atomic publication of complete provider-produced embedding generations.

use std::error::Error;
use std::fmt;
use std::path::Path;

use graphforge_storage::{
    EmbeddingCompatibilityDescriptor, EmbeddingProducerIdentity, EmbeddingPublicationOutcome,
    EmbeddingPublicationRequest, EmbeddingSourceState, SearchArtifactError,
    ValidatedEmbeddingBatch, publish_embedding_generation, reset_embedding_mutation_journal,
    vector_schema,
};

use crate::{
    DocumentEmbeddingBatchOptions, EmbeddingRefreshLimits, ProviderError, ProviderFailureClass,
    ProviderModelContract, ProviderResult,
};
#[cfg(test)]
use crate::{
    DocumentEmbeddingBatchPlan, DocumentEmbeddingProvider, ProviderBatchCostEstimator,
    ProviderExecutionController, ProviderExecutionRuntime, execute_document_embedding_batches,
};

/// Typed failure from provider production or durable generation publication.
#[derive(Debug)]
pub enum ProviderPublicationError {
    /// Redacted provider/model failure without payload contents.
    Provider(ProviderError),
    /// Structured source, persistence, cancellation, corruption, or lock failure.
    Artifact(SearchArtifactError),
}

impl fmt::Display for ProviderPublicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Provider(error) => error.fmt(formatter),
            Self::Artifact(error) => error.fmt(formatter),
        }
    }
}

impl Error for ProviderPublicationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Provider(error) => Some(error),
            Self::Artifact(error) => Some(error),
        }
    }
}

impl From<ProviderError> for ProviderPublicationError {
    fn from(error: ProviderError) -> Self {
        Self::Provider(error)
    }
}

impl From<SearchArtifactError> for ProviderPublicationError {
    fn from(error: SearchArtifactError) -> Self {
        Self::Artifact(error)
    }
}

/// Preflighted durable identity and provider-batch contract.
///
/// This type deliberately omits `Debug` so future private execution fields
/// cannot accidentally enter diagnostics.
pub struct ProviderEmbeddingPublicationRequest<'a> {
    descriptor: &'a EmbeddingCompatibilityDescriptor,
    contract: ProviderModelContract,
    options: DocumentEmbeddingBatchOptions,
    generated_at_micros: i64,
    committed_at_micros: i64,
}

impl<'a> ProviderEmbeddingPublicationRequest<'a> {
    /// Bind an exact provider/batch contract to one durable remote-space identity.
    ///
    /// # Errors
    /// Rejects provider/model/revision/response-contract, tokenizer, chunking,
    /// dimension, or normalization mismatch before any provider call.
    pub fn new(
        descriptor: &'a EmbeddingCompatibilityDescriptor,
        contract: &ProviderModelContract,
        options: DocumentEmbeddingBatchOptions,
        generated_at_micros: i64,
        committed_at_micros: i64,
    ) -> ProviderResult<Self> {
        validate_compatibility(descriptor, contract, options)?;
        Ok(Self {
            descriptor,
            contract: contract.clone(),
            options,
            generated_at_micros,
            committed_at_micros,
        })
    }

    /// Exact versioned durable compatibility descriptor.
    #[must_use]
    pub const fn descriptor(&self) -> &EmbeddingCompatibilityDescriptor {
        self.descriptor
    }

    /// Exact provider model contract.
    #[must_use]
    pub const fn contract(&self) -> &ProviderModelContract {
        &self.contract
    }

    /// Exact provider batching and vector contract.
    #[must_use]
    pub const fn options(&self) -> DocumentEmbeddingBatchOptions {
        self.options
    }
}

/// Produce and atomically publish one complete provider embedding generation.
///
/// `prepare_attempt` must freshly project the committed source and complete the
/// private provider run on every invocation. That whole unit is retried at most
/// once when the post-production source capture changes. Provider execution
/// budgets owned by the callback must span both attempts. No partial batch
/// reaches the publication boundary. `begin_publication_visibility` is invoked
/// only after the source is stable and its returned guard remains alive through
/// the active-generation relink and mutation-journal reset.
///
/// # Errors
/// Returns redacted provider failures without collapsing them into storage
/// errors. Source, cancellation, publication, corruption, lock, and journal
/// failures remain structured [`SearchArtifactError`] values.
pub fn publish_provider_embedding_generation<P, S, V, C, G>(
    project_dir: &Path,
    request: &ProviderEmbeddingPublicationRequest<'_>,
    limits: EmbeddingRefreshLimits,
    mut prepare_attempt: P,
    mut capture_source: S,
    mut begin_publication_visibility: V,
    mut checkpoint: C,
) -> Result<EmbeddingPublicationOutcome, ProviderPublicationError>
where
    P: FnMut(
        &mut dyn FnMut() -> Result<(), SearchArtifactError>,
    )
        -> Result<(EmbeddingSourceState, ValidatedEmbeddingBatch), ProviderPublicationError>,
    S: FnMut() -> Result<EmbeddingSourceState, SearchArtifactError>,
    V: FnMut() -> Result<G, SearchArtifactError>,
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    for attempt in 1_u8..=2 {
        checkpoint()?;
        let (before, batch) = prepare_attempt(&mut checkpoint)?;
        checkpoint()?;
        let after = capture_source()?;
        if before != after {
            if attempt == 2 {
                return Err(SearchArtifactError::ConcurrentMutation.into());
            }
            continue;
        }

        let _visibility = begin_publication_visibility()?;
        let outcome = publish_embedding_generation(
            project_dir,
            EmbeddingPublicationRequest {
                descriptor: request.descriptor,
                source: before,
                batch: &batch,
                generated_at_micros: request.generated_at_micros,
                committed_at_micros: request.committed_at_micros,
            },
            limits.read.vectors,
            limits.coordination,
            &mut checkpoint,
        )?;
        reset_embedding_mutation_journal(
            project_dir,
            &outcome.publication().manifest,
            limits.read.journal,
            limits.coordination,
            &mut checkpoint,
        )?;
        return Ok(outcome);
    }
    unreachable!("the bounded provider publication loop has two terminal attempts")
}

#[cfg(test)]
fn execute_with_artifact_checkpoint<C>(
    provider: &mut dyn DocumentEmbeddingProvider,
    plan: &DocumentEmbeddingBatchPlan<'_>,
    controller: &mut ProviderExecutionController,
    runtime: &mut dyn ProviderExecutionRuntime,
    estimate_cost: &mut ProviderBatchCostEstimator<'_>,
    checkpoint: &mut C,
) -> Result<graphforge_storage::ValidatedEmbeddingBatch, ProviderPublicationError>
where
    C: FnMut() -> Result<(), SearchArtifactError> + ?Sized,
{
    let mut artifact_failure = None;
    let result = {
        let mut provider_checkpoint = || match checkpoint() {
            Ok(()) => Ok(()),
            Err(error) => {
                artifact_failure = Some(error);
                Err(ProviderError::new(
                    plan.contract(),
                    ProviderFailureClass::Cancelled,
                ))
            }
        };
        execute_document_embedding_batches(
            provider,
            plan,
            controller,
            runtime,
            estimate_cost,
            &mut provider_checkpoint,
        )
    };
    if let Some(error) = artifact_failure {
        return Err(error.into());
    }
    result.map_err(Into::into)
}

fn validate_compatibility(
    descriptor: &EmbeddingCompatibilityDescriptor,
    contract: &ProviderModelContract,
    options: DocumentEmbeddingBatchOptions,
) -> ProviderResult<()> {
    let options_valid = options.dimension != 0
        && options.batch_limits.items != 0
        && options.batch_limits.input_bytes != 0
        && options.batch_limits.input_tokens != 0
        && options.request_limits.validate().is_ok()
        && vector_schema(options.dimension, options.vector_limits).is_ok();
    let producer_matches = matches!(
        descriptor.producer(),
        EmbeddingProducerIdentity::Remote {
            provider,
            model,
            revision,
            response_contract_version,
        } if provider == contract.provider()
            && model == contract.model()
            && revision == contract.revision()
            && response_contract_version == contract.response_contract_version()
    );
    let dimensions_match = usize::try_from(descriptor.dimensions())
        .is_ok_and(|dimensions| dimensions == options.dimension);
    if !producer_matches
        || !options_valid
        || !dimensions_match
        || descriptor.normalization() != options.normalization
        || descriptor.tokenizer() != Some(contract.tokenizer())
        || descriptor.chunking() != contract.chunking()
    {
        return Err(invalid_request(contract));
    }
    Ok(())
}

fn invalid_request(contract: &ProviderModelContract) -> ProviderError {
    ProviderError::new(contract, ProviderFailureClass::InvalidRequest)
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::collections::BTreeMap;
    use std::fs;
    use std::time::Duration;

    use graphforge_storage::{
        ChunkingIdentity, EmbeddingCompatibilityInput, EmbeddingDistance, EmbeddingNormalization,
        EmbeddingValueType, TokenCountClass, TokenizerIdentity, VectorStoreLimits,
        current_embedding_generation, read_vector_snapshot,
    };

    use crate::{
        DocumentEmbeddingBatchOptions, DocumentEmbeddingInput, DocumentEmbeddingOutput,
        ProviderBatchLimits, ProviderCapabilities, ProviderCapability, ProviderCheckpoint,
        ProviderExecutionLimits, ProviderRequestLimits,
    };

    use super::*;

    struct FakeRuntime {
        now: Duration,
    }

    impl ProviderExecutionRuntime for FakeRuntime {
        fn elapsed(&self) -> Duration {
            self.now
        }

        fn wait(
            &mut self,
            duration: Duration,
            checkpoint: &mut ProviderCheckpoint<'_>,
        ) -> ProviderResult<()> {
            checkpoint()?;
            self.now = self.now.saturating_add(duration);
            checkpoint()
        }
    }

    struct FakeProvider {
        contract: ProviderModelContract,
        calls: usize,
        fail_at: Option<usize>,
    }

    impl DocumentEmbeddingProvider for FakeProvider {
        fn contract(&self) -> &ProviderModelContract {
            &self.contract
        }

        fn provide_documents(
            &mut self,
            request: &crate::DocumentEmbeddingRequest<'_>,
            checkpoint: &mut ProviderCheckpoint<'_>,
        ) -> ProviderResult<Vec<DocumentEmbeddingOutput>> {
            checkpoint()?;
            self.calls += 1;
            if self.fail_at == Some(self.calls) {
                return Err(ProviderError::new(
                    &self.contract,
                    ProviderFailureClass::Timeout,
                ));
            }
            Ok(request
                .inputs()
                .iter()
                .map(|input| DocumentEmbeddingOutput {
                    node_uuid: input.node_uuid,
                    vector: if input.text.starts_with("new-") {
                        vec![8.0, 6.0]
                    } else {
                        vec![3.0, 4.0]
                    },
                })
                .collect())
        }
    }

    fn contract(model: &str) -> ProviderModelContract {
        contract_with_tokenizer(model, "1", None)
    }

    fn contract_with_tokenizer(
        model: &str,
        tokenizer_version: &str,
        chunking: Option<ChunkingIdentity>,
    ) -> ProviderModelContract {
        ProviderModelContract::remote(
            None,
            model,
            "revision",
            "wire-v1",
            ProviderCapabilities::new([ProviderCapability::DocumentEmbeddings]).unwrap(),
            TokenizerIdentity {
                identifier: "provider-tokenizer".into(),
                version: tokenizer_version.into(),
                count_class: TokenCountClass::ProviderReported,
                max_input_tokens: 32,
                normalization: "nfc".into(),
            },
            chunking,
        )
        .unwrap()
    }

    fn descriptor(
        contract: &ProviderModelContract,
        dimensions: u32,
        normalization: EmbeddingNormalization,
    ) -> EmbeddingCompatibilityDescriptor {
        EmbeddingCompatibilityDescriptor::new(EmbeddingCompatibilityInput {
            producer: EmbeddingProducerIdentity::Remote {
                provider: contract.provider().into(),
                model: contract.model().into(),
                revision: contract.revision().into(),
                response_contract_version: contract.response_contract_version().into(),
            },
            dimensions,
            value_type: EmbeddingValueType::Float32,
            normalization,
            distance: EmbeddingDistance::Cosine,
            tokenizer: Some(contract.tokenizer().clone()),
            chunking: contract.chunking().cloned(),
            hyperparameters: BTreeMap::new(),
            input_recipe: BTreeMap::from([("property".into(), "body".into())]),
            source_projection_recipe: BTreeMap::from([("label".into(), "Document".into())]),
        })
        .unwrap()
    }

    fn inputs() -> Vec<DocumentEmbeddingInput<'static>> {
        vec![
            DocumentEmbeddingInput {
                node_uuid: [1; 16],
                text: "private-source-text-one",
                token_count: 3,
            },
            DocumentEmbeddingInput {
                node_uuid: [2; 16],
                text: "private-source-text-two",
                token_count: 3,
            },
            DocumentEmbeddingInput {
                node_uuid: [3; 16],
                text: "private-source-text-three",
                token_count: 3,
            },
        ]
    }

    fn changed_inputs() -> Vec<DocumentEmbeddingInput<'static>> {
        vec![
            DocumentEmbeddingInput {
                node_uuid: [1; 16],
                text: "new-private-source-text-one",
                token_count: 3,
            },
            DocumentEmbeddingInput {
                node_uuid: [2; 16],
                text: "new-private-source-text-two",
                token_count: 3,
            },
            DocumentEmbeddingInput {
                node_uuid: [3; 16],
                text: "new-private-source-text-three",
                token_count: 3,
            },
        ]
    }

    fn options() -> DocumentEmbeddingBatchOptions {
        DocumentEmbeddingBatchOptions {
            request_limits: ProviderRequestLimits::default(),
            batch_limits: ProviderBatchLimits {
                items: 2,
                input_bytes: 128,
                input_tokens: 16,
            },
            dimension: 2,
            normalization: EmbeddingNormalization::L2,
            vector_limits: VectorStoreLimits::default(),
        }
    }

    fn source(generation: u64, marker: u8) -> EmbeddingSourceState {
        EmbeddingSourceState::new(generation, [marker; 32], [marker + 1; 32], 3)
    }

    fn controller(
        contract: &ProviderModelContract,
        runtime: &FakeRuntime,
    ) -> ProviderExecutionController {
        ProviderExecutionController::new(
            contract,
            ProviderExecutionLimits {
                retries: 0,
                ..ProviderExecutionLimits::default()
            },
            runtime,
        )
        .unwrap()
    }

    fn request<'a>(
        descriptor: &'a EmbeddingCompatibilityDescriptor,
        contract: &ProviderModelContract,
    ) -> ProviderEmbeddingPublicationRequest<'a> {
        ProviderEmbeddingPublicationRequest::new(descriptor, contract, options(), 20, 21).unwrap()
    }

    fn prepare_attempt(
        source: EmbeddingSourceState,
        contract: &ProviderModelContract,
        inputs: &[DocumentEmbeddingInput<'_>],
        provider: &mut dyn DocumentEmbeddingProvider,
        controller: &mut ProviderExecutionController,
        runtime: &mut dyn ProviderExecutionRuntime,
        checkpoint: &mut dyn FnMut() -> Result<(), SearchArtifactError>,
    ) -> Result<(EmbeddingSourceState, ValidatedEmbeddingBatch), ProviderPublicationError> {
        let plan = DocumentEmbeddingBatchPlan::new(contract, inputs, options(), &mut || Ok(()))?;
        let batch = execute_with_artifact_checkpoint(
            provider,
            &plan,
            controller,
            runtime,
            &mut |_| Ok(0),
            checkpoint,
        )?;
        Ok((source, batch))
    }

    fn all_file_bytes(root: &Path) -> Vec<u8> {
        let mut bytes = Vec::new();
        let Ok(entries) = fs::read_dir(root) else {
            return bytes;
        };
        for entry in entries {
            let path = entry.unwrap().path();
            if path.is_dir() {
                bytes.extend(all_file_bytes(&path));
            } else {
                bytes.extend(fs::read(path).unwrap());
            }
        }
        bytes
    }

    #[test]
    fn complete_provider_batches_publish_atomically_without_source_text() {
        let dir = tempfile::tempdir().unwrap();
        let contract = contract("vendor/model");
        let descriptor = descriptor(&contract, 2, EmbeddingNormalization::L2);
        let inputs = inputs();
        let request = request(&descriptor, &contract);
        let mut provider = FakeProvider {
            contract: contract.clone(),
            calls: 0,
            fail_at: None,
        };
        let mut runtime = FakeRuntime {
            now: Duration::ZERO,
        };
        let mut controller = controller(&contract, &runtime);
        let outcome = publish_provider_embedding_generation(
            dir.path(),
            &request,
            EmbeddingRefreshLimits::default(),
            |checkpoint| {
                prepare_attempt(
                    source(10, 1),
                    &contract,
                    &inputs,
                    &mut provider,
                    &mut controller,
                    &mut runtime,
                    checkpoint,
                )
            },
            || Ok(source(10, 1)),
            || Ok(()),
            || Ok(()),
        )
        .unwrap();

        assert_eq!(provider.calls, 2);
        assert_eq!(outcome.publication().manifest.source(), source(10, 1));
        assert_eq!(outcome.publication().manifest.vector_count(), 3);
        let durable = all_file_bytes(dir.path());
        assert!(
            !durable
                .windows(b"private-source-text".len())
                .any(|value| value == b"private-source-text")
        );
    }

    #[test]
    fn incompatible_identity_fails_before_provider_work() {
        let exact_contract = contract("vendor/model");
        let other = contract("vendor/other");
        let other_tokenizer = contract_with_tokenizer("vendor/model", "2", None);
        let other_chunking = contract_with_tokenizer(
            "vendor/model",
            "1",
            Some(ChunkingIdentity {
                chunk_size_tokens: 16,
                overlap_tokens: 2,
                aggregation: "mean-v1".into(),
                truncation_policy: "reject".into(),
            }),
        );
        for descriptor in [
            descriptor(&other, 2, EmbeddingNormalization::L2),
            descriptor(&exact_contract, 3, EmbeddingNormalization::L2),
            descriptor(&exact_contract, 2, EmbeddingNormalization::None),
            descriptor(&other_tokenizer, 2, EmbeddingNormalization::L2),
            descriptor(&other_chunking, 2, EmbeddingNormalization::L2),
        ] {
            assert_eq!(
                ProviderEmbeddingPublicationRequest::new(
                    &descriptor,
                    &exact_contract,
                    options(),
                    20,
                    21,
                )
                .err()
                .unwrap()
                .class(),
                ProviderFailureClass::InvalidRequest
            );
        }
        let mut invalid_options = options();
        invalid_options.batch_limits.items = 0;
        assert_eq!(
            ProviderEmbeddingPublicationRequest::new(
                &descriptor(&exact_contract, 2, EmbeddingNormalization::L2),
                &exact_contract,
                invalid_options,
                20,
                21,
            )
            .err()
            .unwrap()
            .class(),
            ProviderFailureClass::InvalidRequest
        );
    }

    #[test]
    fn later_batch_failure_preserves_prior_generation() {
        let dir = tempfile::tempdir().unwrap();
        let contract = contract("vendor/model");
        let descriptor = descriptor(&contract, 2, EmbeddingNormalization::L2);
        let inputs = inputs();
        let request = request(&descriptor, &contract);
        let mut runtime = FakeRuntime {
            now: Duration::ZERO,
        };
        let mut successful = FakeProvider {
            contract: contract.clone(),
            calls: 0,
            fail_at: None,
        };
        let mut first_controller = controller(&contract, &runtime);
        let prior = publish_provider_embedding_generation(
            dir.path(),
            &request,
            EmbeddingRefreshLimits::default(),
            |checkpoint| {
                prepare_attempt(
                    source(10, 1),
                    &contract,
                    &inputs,
                    &mut successful,
                    &mut first_controller,
                    &mut runtime,
                    checkpoint,
                )
            },
            || Ok(source(10, 1)),
            || Ok(()),
            || Ok(()),
        )
        .unwrap()
        .publication()
        .manifest
        .generation_id();

        let mut failing = FakeProvider {
            contract: contract.clone(),
            calls: 0,
            fail_at: Some(2),
        };
        let mut second_controller = controller(&contract, &runtime);
        let error = publish_provider_embedding_generation(
            dir.path(),
            &request,
            EmbeddingRefreshLimits::default(),
            |checkpoint| {
                prepare_attempt(
                    source(11, 2),
                    &contract,
                    &inputs,
                    &mut failing,
                    &mut second_controller,
                    &mut runtime,
                    checkpoint,
                )
            },
            || Ok(source(11, 2)),
            || Ok(()),
            || Ok(()),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            ProviderPublicationError::Provider(ref error)
                if error.class() == ProviderFailureClass::Timeout
        ));
        assert_eq!(failing.calls, 2);
        assert_eq!(
            current_embedding_generation(
                dir.path(),
                &descriptor,
                VectorStoreLimits::default(),
                || Ok(())
            )
            .unwrap()
            .unwrap()
            .manifest
            .generation_id(),
            prior
        );
    }

    #[test]
    fn mutation_retry_rebuilds_and_publishes_the_second_attempt_batch() {
        let dir = tempfile::tempdir().unwrap();
        let contract = contract("vendor/model");
        let descriptor = descriptor(&contract, 2, EmbeddingNormalization::L2);
        let request = request(&descriptor, &contract);
        let original = inputs();
        let changed = changed_inputs();
        let mut provider = FakeProvider {
            contract: contract.clone(),
            calls: 0,
            fail_at: None,
        };
        let mut runtime = FakeRuntime {
            now: Duration::ZERO,
        };
        let mut controller = controller(&contract, &runtime);
        let prepares = Cell::new(0_u8);

        let outcome = publish_provider_embedding_generation(
            dir.path(),
            &request,
            EmbeddingRefreshLimits::default(),
            |checkpoint| {
                let attempt = prepares.replace(prepares.get() + 1);
                let (attempt_source, attempt_inputs) = if attempt == 0 {
                    (source(10, 1), original.as_slice())
                } else {
                    (source(11, 2), changed.as_slice())
                };
                prepare_attempt(
                    attempt_source,
                    &contract,
                    attempt_inputs,
                    &mut provider,
                    &mut controller,
                    &mut runtime,
                    checkpoint,
                )
            },
            || Ok(source(11, 2)),
            || Ok(()),
            || Ok(()),
        )
        .unwrap();

        assert_eq!(prepares.get(), 2);
        assert_eq!(provider.calls, 4);
        assert_eq!(outcome.publication().manifest.source(), source(11, 2));
        let rows = read_vector_snapshot(
            &outcome.publication().path,
            2,
            VectorStoreLimits::default(),
            || Ok(()),
        )
        .unwrap();
        assert!(rows.iter().all(|row| row.vector == [0.8, 0.6]));
    }

    #[test]
    fn two_source_mutations_return_artifact_error_without_publication() {
        let dir = tempfile::tempdir().unwrap();
        let contract = contract("vendor/model");
        let descriptor = descriptor(&contract, 2, EmbeddingNormalization::L2);
        let inputs = inputs();
        let request = request(&descriptor, &contract);
        let mut provider = FakeProvider {
            contract: contract.clone(),
            calls: 0,
            fail_at: None,
        };
        let mut runtime = FakeRuntime {
            now: Duration::ZERO,
        };
        let mut controller = controller(&contract, &runtime);
        let prepares = Cell::new(0_u8);
        let captures = Cell::new(0_u8);
        let error = publish_provider_embedding_generation(
            dir.path(),
            &request,
            EmbeddingRefreshLimits::default(),
            |checkpoint| {
                let attempt = prepares.replace(prepares.get() + 1);
                prepare_attempt(
                    source(u64::from(attempt) * 2 + 1, attempt * 2 + 1),
                    &contract,
                    &inputs,
                    &mut provider,
                    &mut controller,
                    &mut runtime,
                    checkpoint,
                )
            },
            || {
                let call = captures.replace(captures.get() + 1);
                Ok(source(u64::from(call) * 2 + 2, call * 2 + 2))
            },
            || Ok(()),
            || Ok(()),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            ProviderPublicationError::Artifact(SearchArtifactError::ConcurrentMutation)
        ));
        assert_eq!(provider.calls, 4);
        assert!(
            current_embedding_generation(
                dir.path(),
                &descriptor,
                VectorStoreLimits::default(),
                || Ok(())
            )
            .unwrap()
            .is_none()
        );
    }

    #[test]
    fn artifact_cancellation_survives_the_provider_checkpoint_bridge() {
        let dir = tempfile::tempdir().unwrap();
        let contract = contract("vendor/model");
        let descriptor = descriptor(&contract, 2, EmbeddingNormalization::L2);
        let inputs = inputs();
        let request = request(&descriptor, &contract);
        let mut provider = FakeProvider {
            contract: contract.clone(),
            calls: 0,
            fail_at: None,
        };
        let mut runtime = FakeRuntime {
            now: Duration::ZERO,
        };
        let mut controller = controller(&contract, &runtime);
        let checkpoints = Cell::new(0_u8);
        let error = publish_provider_embedding_generation(
            dir.path(),
            &request,
            EmbeddingRefreshLimits::default(),
            |checkpoint| {
                prepare_attempt(
                    source(10, 1),
                    &contract,
                    &inputs,
                    &mut provider,
                    &mut controller,
                    &mut runtime,
                    checkpoint,
                )
            },
            || Ok(source(10, 1)),
            || Ok(()),
            || {
                checkpoints.set(checkpoints.get() + 1);
                if checkpoints.get() >= 4 {
                    Err(SearchArtifactError::Cancelled)
                } else {
                    Ok(())
                }
            },
        )
        .unwrap_err();

        assert!(matches!(
            error,
            ProviderPublicationError::Artifact(SearchArtifactError::Cancelled)
        ));
        assert!(
            current_embedding_generation(
                dir.path(),
                &descriptor,
                VectorStoreLimits::default(),
                || Ok(())
            )
            .unwrap()
            .is_none()
        );
    }
}
