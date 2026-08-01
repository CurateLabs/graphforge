//! Deterministic document-embedding batch planning and execution.

use std::collections::BTreeSet;

use graphforge_storage::{
    EmbeddingNormalization, SearchArtifactError, ValidatedEmbeddingBatch, VectorStoreLimits,
    validate_embedding_batch, vector_schema,
};

use crate::{
    DocumentEmbeddingInput, DocumentEmbeddingProvider, DocumentEmbeddingRequest,
    ProviderCheckpoint, ProviderError, ProviderExecutionController, ProviderExecutionRuntime,
    ProviderFailureClass, ProviderModelContract, ProviderRequestLimits, ProviderResult,
    ProviderWorkEstimate, embed_documents, validate_document_embedding_response,
};

/// Deterministic per-call batch caps.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProviderBatchLimits {
    /// Maximum inputs in one provider call.
    pub items: usize,
    /// Maximum outbound UTF-8 bytes in one provider call.
    pub input_bytes: usize,
    /// Maximum counted input tokens in one provider call.
    pub input_tokens: u64,
}

impl Default for ProviderBatchLimits {
    fn default() -> Self {
        Self {
            items: 64,
            input_bytes: 1024 * 1024,
            input_tokens: 100_000,
        }
    }
}

/// Complete document-batch planning and response-validation options.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DocumentEmbeddingBatchOptions {
    /// Per-request provider preflight limits.
    pub request_limits: ProviderRequestLimits,
    /// Deterministic split caps.
    pub batch_limits: ProviderBatchLimits,
    /// Exact response vector dimension.
    pub dimension: usize,
    /// Required stored normalization contract.
    pub normalization: EmbeddingNormalization,
    /// Final vector validation and complete-set limits.
    pub vector_limits: VectorStoreLimits,
}

/// Payload-free counts passed to the caller's cost estimator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProviderBatchShape {
    items: usize,
    input_bytes: usize,
    input_tokens: u64,
}

impl ProviderBatchShape {
    /// Inputs in this exact batch.
    #[must_use]
    pub const fn items(self) -> usize {
        self.items
    }

    /// Outbound UTF-8 bytes without their contents.
    #[must_use]
    pub const fn input_bytes(self) -> usize {
        self.input_bytes
    }

    /// Counted input tokens in this exact batch.
    #[must_use]
    pub const fn input_tokens(self) -> u64 {
        self.input_tokens
    }
}

/// Provider-specific pricing boundary that receives counts only.
pub type ProviderBatchCostEstimator<'a> = dyn FnMut(ProviderBatchShape) -> ProviderResult<u64> + 'a;

/// Fully preflighted document requests. It deliberately omits `Debug`.
pub struct DocumentEmbeddingBatchPlan<'a> {
    contract: ProviderModelContract,
    requests: Vec<DocumentEmbeddingRequest<'a>>,
    eligible_nodes: BTreeSet<[u8; 16]>,
    options: DocumentEmbeddingBatchOptions,
}

impl<'a> DocumentEmbeddingBatchPlan<'a> {
    /// Preflight every logical input and create deterministic request slices.
    ///
    /// # Errors
    /// Rejects empty/non-canonical input, zero token counts, invalid limits,
    /// single-item exhaustion, tokenizer overflow, cancellation, or vector
    /// limits before any adapter call can begin.
    pub fn new(
        contract: &ProviderModelContract,
        inputs: &'a [DocumentEmbeddingInput<'a>],
        options: DocumentEmbeddingBatchOptions,
        checkpoint: &mut ProviderCheckpoint<'_>,
    ) -> ProviderResult<Self> {
        checkpoint()?;
        contract.require(crate::ProviderCapability::DocumentEmbeddings)?;
        validate_options(contract, inputs.len(), options)?;
        if inputs.is_empty()
            || inputs
                .windows(2)
                .any(|pair| pair[0].node_uuid >= pair[1].node_uuid)
        {
            return Err(failure(contract, ProviderFailureClass::InvalidRequest));
        }

        let request_limits = effective_request_limits(options);
        let mut ranges = Vec::new();
        let mut start = 0_usize;
        let mut items = 0_usize;
        let mut bytes = 0_usize;
        let mut tokens = 0_u64;
        for (index, input) in inputs.iter().enumerate() {
            checkpoint()?;
            if input.text.is_empty() || input.token_count == 0 {
                return Err(failure(contract, ProviderFailureClass::InvalidRequest));
            }
            let input_bytes = input.text.len();
            if input_bytes > request_limits.input_bytes
                || input.token_count > request_limits.input_tokens
                || input.token_count > contract.tokenizer().max_input_tokens
            {
                return Err(exhausted(contract));
            }
            let next_items = items.checked_add(1).ok_or_else(|| exhausted(contract))?;
            let next_bytes = bytes
                .checked_add(input_bytes)
                .ok_or_else(|| exhausted(contract))?;
            let next_tokens = tokens
                .checked_add(input.token_count)
                .ok_or_else(|| exhausted(contract))?;
            if items != 0
                && (next_items > request_limits.items
                    || next_bytes > request_limits.input_bytes
                    || next_tokens > request_limits.input_tokens)
            {
                ranges.push(start..index);
                start = index;
                items = 1;
                bytes = input_bytes;
                tokens = input.token_count;
            } else {
                items = next_items;
                bytes = next_bytes;
                tokens = next_tokens;
            }
        }
        ranges.push(start..inputs.len());

        let mut requests = Vec::with_capacity(ranges.len());
        for range in ranges {
            checkpoint()?;
            requests.push(DocumentEmbeddingRequest::new(
                contract,
                &inputs[range],
                request_limits,
            )?);
        }
        let eligible_nodes = inputs.iter().map(|input| input.node_uuid).collect();
        Ok(Self {
            contract: contract.clone(),
            requests,
            eligible_nodes,
            options,
        })
    }

    /// Exact model contract preflighted for every request.
    #[must_use]
    pub const fn contract(&self) -> &ProviderModelContract {
        &self.contract
    }

    /// Deterministic provider requests in canonical UUID order.
    #[must_use]
    pub fn batches(&self) -> &[DocumentEmbeddingRequest<'a>] {
        &self.requests
    }

    /// Number of logical UUID inputs covered exactly once.
    #[must_use]
    pub fn input_count(&self) -> usize {
        self.eligible_nodes.len()
    }

    /// Exact vector width validated for every response and final batch.
    #[must_use]
    pub const fn dimension(&self) -> usize {
        self.options.dimension
    }

    /// Required durable normalization contract.
    #[must_use]
    pub const fn normalization(&self) -> EmbeddingNormalization {
        self.options.normalization
    }
}

/// Execute every planned batch and return one complete validated batch.
///
/// # Errors
/// Rejects contract mismatch, cost-estimation failure, bounded execution
/// failure, malformed provider output, final resource exhaustion, or
/// cancellation. No partial validated batch is returned.
pub fn execute_document_embedding_batches(
    provider: &mut dyn DocumentEmbeddingProvider,
    plan: &DocumentEmbeddingBatchPlan<'_>,
    controller: &mut ProviderExecutionController,
    runtime: &mut dyn ProviderExecutionRuntime,
    estimate_cost: &mut ProviderBatchCostEstimator<'_>,
    checkpoint: &mut ProviderCheckpoint<'_>,
) -> ProviderResult<ValidatedEmbeddingBatch> {
    if provider.contract() != plan.contract() || controller.contract() != plan.contract() {
        return Err(failure(
            plan.contract(),
            ProviderFailureClass::InvalidRequest,
        ));
    }
    checkpoint()?;
    let mut rows = Vec::with_capacity(plan.input_count());
    for request in plan.batches() {
        checkpoint()?;
        let shape = batch_shape(request, plan.contract())?;
        let cost_units = estimate_cost(shape)?;
        let work = ProviderWorkEstimate::new(
            plan.contract(),
            shape.items(),
            shape.input_bytes(),
            shape.input_tokens(),
            cost_units,
        )?;
        let validated = controller.execute(work, runtime, checkpoint, &mut |guarded| {
            let outputs = embed_documents(provider, request, guarded)?;
            validate_document_embedding_response(
                request,
                outputs,
                plan.options.dimension,
                plan.options.normalization,
                plan.options.vector_limits,
                guarded,
            )
        })?;
        rows.extend(validated.into_batch().into_rows());
    }
    validate_complete_batch(plan, rows, checkpoint)
}

fn validate_options(
    contract: &ProviderModelContract,
    inputs: usize,
    options: DocumentEmbeddingBatchOptions,
) -> ProviderResult<()> {
    if options.batch_limits.items == 0
        || options.batch_limits.input_bytes == 0
        || options.batch_limits.input_tokens == 0
        || options.dimension == 0
    {
        return Err(failure(contract, ProviderFailureClass::InvalidRequest));
    }
    options
        .request_limits
        .validate()
        .map_err(|_| failure(contract, ProviderFailureClass::InvalidRequest))?;
    vector_schema(options.dimension, options.vector_limits)
        .map_err(|error| storage_failure(contract, &error))?;
    let cells = inputs
        .checked_mul(options.dimension)
        .ok_or_else(|| exhausted(contract))?;
    if inputs > options.vector_limits.stored_vectors
        || inputs > options.vector_limits.eligible_nodes
        || cells > options.vector_limits.vector_cells
    {
        return Err(exhausted(contract));
    }
    Ok(())
}

fn effective_request_limits(options: DocumentEmbeddingBatchOptions) -> ProviderRequestLimits {
    ProviderRequestLimits {
        items: options.request_limits.items.min(options.batch_limits.items),
        input_bytes: options
            .request_limits
            .input_bytes
            .min(options.batch_limits.input_bytes),
        input_tokens: options
            .request_limits
            .input_tokens
            .min(options.batch_limits.input_tokens),
        ..options.request_limits
    }
}

fn batch_shape(
    request: &DocumentEmbeddingRequest<'_>,
    contract: &ProviderModelContract,
) -> ProviderResult<ProviderBatchShape> {
    let mut input_bytes = 0_usize;
    let mut input_tokens = 0_u64;
    for input in request.inputs() {
        input_bytes = input_bytes
            .checked_add(input.text.len())
            .ok_or_else(|| exhausted(contract))?;
        input_tokens = input_tokens
            .checked_add(input.token_count)
            .ok_or_else(|| exhausted(contract))?;
    }
    Ok(ProviderBatchShape {
        items: request.inputs().len(),
        input_bytes,
        input_tokens,
    })
}

fn validate_complete_batch(
    plan: &DocumentEmbeddingBatchPlan<'_>,
    rows: Vec<graphforge_storage::EmbeddingBatchRow>,
    checkpoint: &mut ProviderCheckpoint<'_>,
) -> ProviderResult<ValidatedEmbeddingBatch> {
    let mut cancellation = None;
    let result = validate_embedding_batch(
        rows,
        &plan.eligible_nodes,
        plan.options.dimension,
        EmbeddingNormalization::None,
        plan.options.vector_limits,
        || match checkpoint() {
            Ok(()) => Ok(()),
            Err(error) => {
                cancellation = Some(error);
                Err(SearchArtifactError::Cancelled)
            }
        },
    );
    if let Some(error) = cancellation {
        return Err(error);
    }
    result.map_err(|error| storage_failure(plan.contract(), &error))
}

fn storage_failure(contract: &ProviderModelContract, error: &SearchArtifactError) -> ProviderError {
    match error {
        SearchArtifactError::ResourceExhausted { .. } => exhausted(contract),
        SearchArtifactError::Cancelled => failure(contract, ProviderFailureClass::Cancelled),
        _ => failure(contract, ProviderFailureClass::MalformedResponse),
    }
}

fn exhausted(contract: &ProviderModelContract) -> ProviderError {
    failure(contract, ProviderFailureClass::ResourceExhausted)
}

fn failure(contract: &ProviderModelContract, class: ProviderFailureClass) -> ProviderError {
    ProviderError::new(contract, class)
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::time::Duration;

    use graphforge_storage::{TokenCountClass, TokenizerIdentity};

    use crate::{
        DocumentEmbeddingOutput, ProviderCapabilities, ProviderCapability, ProviderExecutionLimits,
        StandardProviderExecutionRuntime,
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
        calls: Vec<Vec<[u8; 16]>>,
        failure_call: Option<usize>,
        transient_first: bool,
        reorder: bool,
        wrong_dimension: bool,
        non_finite: bool,
    }

    impl DocumentEmbeddingProvider for FakeProvider {
        fn contract(&self) -> &ProviderModelContract {
            &self.contract
        }

        fn provide_documents(
            &mut self,
            request: &DocumentEmbeddingRequest<'_>,
            checkpoint: &mut ProviderCheckpoint<'_>,
        ) -> ProviderResult<Vec<DocumentEmbeddingOutput>> {
            checkpoint()?;
            self.calls.push(
                request
                    .inputs()
                    .iter()
                    .map(|input| input.node_uuid)
                    .collect(),
            );
            let call = self.calls.len();
            if self.transient_first && call == 1 {
                return Err(failure(&self.contract, ProviderFailureClass::Transport));
            }
            if self.failure_call == Some(call) {
                return Ok(Vec::new());
            }
            let mut outputs = request
                .inputs()
                .iter()
                .map(|input| DocumentEmbeddingOutput {
                    node_uuid: input.node_uuid,
                    vector: vec![3.0, 4.0],
                })
                .collect::<Vec<_>>();
            if self.reorder {
                outputs.reverse();
            }
            if self.wrong_dimension {
                outputs[0].vector.push(0.0);
            }
            if self.non_finite {
                outputs[0].vector[0] = f32::NAN;
            }
            Ok(outputs)
        }
    }

    fn test_contract(model: &str) -> ProviderModelContract {
        ProviderModelContract::remote(
            None,
            model,
            "revision",
            "v1",
            ProviderCapabilities::new([ProviderCapability::DocumentEmbeddings]).unwrap(),
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

    fn test_inputs() -> Vec<DocumentEmbeddingInput<'static>> {
        vec![
            DocumentEmbeddingInput {
                node_uuid: [1; 16],
                text: "aa",
                token_count: 2,
            },
            DocumentEmbeddingInput {
                node_uuid: [2; 16],
                text: "bbb",
                token_count: 2,
            },
            DocumentEmbeddingInput {
                node_uuid: [3; 16],
                text: "cccc",
                token_count: 3,
            },
        ]
    }

    fn options() -> DocumentEmbeddingBatchOptions {
        DocumentEmbeddingBatchOptions {
            request_limits: ProviderRequestLimits::default(),
            batch_limits: ProviderBatchLimits {
                items: 2,
                input_bytes: 5,
                input_tokens: 4,
            },
            dimension: 2,
            normalization: EmbeddingNormalization::L2,
            vector_limits: VectorStoreLimits::default(),
        }
    }

    fn test_provider(contract: &ProviderModelContract) -> FakeProvider {
        FakeProvider {
            contract: contract.clone(),
            calls: Vec::new(),
            failure_call: None,
            transient_first: false,
            reorder: false,
            wrong_dimension: false,
            non_finite: false,
        }
    }

    fn execute(
        provider: &mut FakeProvider,
        plan: &DocumentEmbeddingBatchPlan<'_>,
    ) -> ProviderResult<ValidatedEmbeddingBatch> {
        let mut runtime = FakeRuntime {
            now: Duration::ZERO,
        };
        let mut controller = ProviderExecutionController::new(
            plan.contract(),
            ProviderExecutionLimits::default(),
            &runtime,
        )?;
        execute_document_embedding_batches(
            provider,
            plan,
            &mut controller,
            &mut runtime,
            &mut |shape| Ok(shape.input_tokens()),
            &mut || Ok(()),
        )
    }

    #[test]
    fn planning_is_deterministic_and_preflights_every_input() {
        let contract = test_contract("vendor/model");
        let inputs = test_inputs();
        let plan =
            DocumentEmbeddingBatchPlan::new(&contract, &inputs, options(), &mut || Ok(())).unwrap();
        assert_eq!(plan.input_count(), 3);
        assert_eq!(plan.batches().len(), 2);
        assert_eq!(plan.batches()[0].inputs().len(), 2);
        assert_eq!(plan.batches()[1].inputs()[0].node_uuid, [3; 16]);

        let mut invalid = test_inputs();
        invalid[2].text = "";
        assert_eq!(
            DocumentEmbeddingBatchPlan::new(&contract, &invalid, options(), &mut || Ok(()))
                .err()
                .unwrap()
                .class(),
            ProviderFailureClass::InvalidRequest
        );
        invalid[2].text = "cccc";
        invalid[2].node_uuid = [1; 16];
        assert_eq!(
            DocumentEmbeddingBatchPlan::new(&contract, &invalid, options(), &mut || Ok(()))
                .err()
                .unwrap()
                .class(),
            ProviderFailureClass::InvalidRequest
        );
    }

    #[test]
    fn single_item_and_final_vector_limits_fail_before_provider_work() {
        let contract = test_contract("vendor/model");
        let inputs = test_inputs();
        let mut limited = options();
        limited.batch_limits.input_bytes = 3;
        assert_eq!(
            DocumentEmbeddingBatchPlan::new(&contract, &inputs, limited, &mut || Ok(()))
                .err()
                .unwrap()
                .class(),
            ProviderFailureClass::ResourceExhausted
        );
        limited = options();
        limited.vector_limits.vector_cells = 5;
        assert_eq!(
            DocumentEmbeddingBatchPlan::new(&contract, &inputs, limited, &mut || Ok(()))
                .err()
                .unwrap()
                .class(),
            ProviderFailureClass::ResourceExhausted
        );
    }

    #[test]
    fn multi_batch_success_retries_and_returns_one_complete_canonical_batch() {
        let contract = test_contract("vendor/model");
        let inputs = test_inputs();
        let plan =
            DocumentEmbeddingBatchPlan::new(&contract, &inputs, options(), &mut || Ok(())).unwrap();
        let mut provider = test_provider(&contract);
        provider.transient_first = true;
        let batch = execute(&mut provider, &plan).unwrap();
        assert_eq!(provider.calls.len(), 3);
        assert_eq!(provider.calls[0], provider.calls[1]);
        assert_eq!(provider.calls[2], vec![[3; 16]]);
        assert_eq!(batch.rows().len(), 3);
        assert_eq!(batch.rows()[0].node_uuid, [1; 16]);
        assert_eq!(batch.rows()[2].node_uuid, [3; 16]);
        assert_eq!(batch.rows()[0].vector, vec![0.6, 0.8]);
    }

    #[test]
    fn malformed_sub_batches_return_no_validated_value() {
        let contract = test_contract("vendor/model");
        let inputs = test_inputs();
        let plan =
            DocumentEmbeddingBatchPlan::new(&contract, &inputs, options(), &mut || Ok(())).unwrap();
        let mut partial = test_provider(&contract);
        partial.failure_call = Some(2);
        assert_eq!(
            execute(&mut partial, &plan).unwrap_err().class(),
            ProviderFailureClass::MalformedResponse
        );
        assert_eq!(partial.calls.len(), 2);

        let mut reordered = test_provider(&contract);
        reordered.reorder = true;
        assert_eq!(
            execute(&mut reordered, &plan).unwrap_err().class(),
            ProviderFailureClass::MalformedResponse
        );

        let mut wrong_dimension = test_provider(&contract);
        wrong_dimension.wrong_dimension = true;
        assert_eq!(
            execute(&mut wrong_dimension, &plan).unwrap_err().class(),
            ProviderFailureClass::MalformedResponse
        );

        let mut non_finite = test_provider(&contract);
        non_finite.non_finite = true;
        assert_eq!(
            execute(&mut non_finite, &plan).unwrap_err().class(),
            ProviderFailureClass::MalformedResponse
        );
    }

    #[test]
    fn mismatch_cost_failure_and_cancellation_happen_before_partial_return() {
        let contract = test_contract("vendor/model");
        let inputs = test_inputs();
        let plan =
            DocumentEmbeddingBatchPlan::new(&contract, &inputs, options(), &mut || Ok(())).unwrap();
        let mut provider = test_provider(&contract);
        let other = test_contract("vendor/other");
        let mut runtime = FakeRuntime {
            now: Duration::ZERO,
        };
        let mut controller =
            ProviderExecutionController::new(&other, ProviderExecutionLimits::default(), &runtime)
                .unwrap();
        assert_eq!(
            execute_document_embedding_batches(
                &mut provider,
                &plan,
                &mut controller,
                &mut runtime,
                &mut |_| Ok(0),
                &mut || Ok(())
            )
            .unwrap_err()
            .class(),
            ProviderFailureClass::InvalidRequest
        );
        assert!(provider.calls.is_empty());

        let mut controller = ProviderExecutionController::new(
            &contract,
            ProviderExecutionLimits::default(),
            &runtime,
        )
        .unwrap();
        assert_eq!(
            execute_document_embedding_batches(
                &mut provider,
                &plan,
                &mut controller,
                &mut runtime,
                &mut |_| Err(failure(&contract, ProviderFailureClass::ResourceExhausted)),
                &mut || Ok(())
            )
            .unwrap_err()
            .class(),
            ProviderFailureClass::ResourceExhausted
        );
        assert!(provider.calls.is_empty());

        let mut budgeted = test_provider(&contract);
        let mut controller = ProviderExecutionController::new(
            &contract,
            ProviderExecutionLimits {
                provider_calls: 1,
                ..ProviderExecutionLimits::default()
            },
            &runtime,
        )
        .unwrap();
        assert_eq!(
            execute_document_embedding_batches(
                &mut budgeted,
                &plan,
                &mut controller,
                &mut runtime,
                &mut |_| Ok(0),
                &mut || Ok(())
            )
            .unwrap_err()
            .class(),
            ProviderFailureClass::ResourceExhausted
        );
        assert_eq!(budgeted.calls.len(), 1);

        let checkpoints = Cell::new(0);
        let mut controller = ProviderExecutionController::new(
            &contract,
            ProviderExecutionLimits::default(),
            &runtime,
        )
        .unwrap();
        assert_eq!(
            execute_document_embedding_batches(
                &mut provider,
                &plan,
                &mut controller,
                &mut runtime,
                &mut |_| Ok(0),
                &mut || {
                    checkpoints.set(checkpoints.get() + 1);
                    if checkpoints.get() > 1 {
                        Err(failure(&contract, ProviderFailureClass::Cancelled))
                    } else {
                        Ok(())
                    }
                }
            )
            .unwrap_err()
            .class(),
            ProviderFailureClass::Cancelled
        );
        assert!(provider.calls.is_empty());
    }

    #[test]
    fn standard_runtime_constructs_without_external_dependencies() {
        let runtime = StandardProviderExecutionRuntime::new();
        assert!(runtime.elapsed() < Duration::from_secs(1));
    }
}
