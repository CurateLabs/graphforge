//! Validation and canonical reassociation for untrusted provider outputs.

use std::collections::BTreeSet;

use gf_storage::{
    EmbeddingBatchRow, EmbeddingNormalization, SearchArtifactError, ValidatedEmbeddingBatch,
    VectorStoreLimits, validate_embedding_batch,
};

use crate::{
    DocumentEmbeddingOutput, DocumentEmbeddingRequest, ProviderError, ProviderFailureClass,
    ProviderModelContract, ProviderResult, QueryEmbeddingOutput, QueryEmbeddingRequest,
    RerankOutput, RerankRequest,
};

/// Validated document embeddings. It deliberately omits `Debug`.
pub struct ValidatedDocumentEmbeddings {
    batch: ValidatedEmbeddingBatch,
}

impl ValidatedDocumentEmbeddings {
    /// Canonical storage batch ready for a private generation.
    #[must_use]
    pub const fn batch(&self) -> &ValidatedEmbeddingBatch {
        &self.batch
    }

    /// Consume the payload-opaque wrapper.
    #[must_use]
    pub fn into_batch(self) -> ValidatedEmbeddingBatch {
        self.batch
    }
}

/// Validated query embedding. It deliberately omits `Debug`.
pub struct ValidatedQueryEmbedding {
    vector: Vec<f32>,
}

impl ValidatedQueryEmbedding {
    /// Fixed-dimension query vector.
    #[must_use]
    pub fn vector(&self) -> &[f32] {
        &self.vector
    }

    /// Consume the payload-opaque wrapper.
    #[must_use]
    pub fn into_vector(self) -> Vec<f32> {
        self.vector
    }
}

/// One validated rerank score in canonical retrieval order.
#[derive(Clone, Copy, PartialEq)]
pub struct ValidatedRerankRow {
    /// Stable graph identity.
    node_uuid: [u8; 16],
    /// Original one-based canonical retrieval rank.
    retrieval_rank: usize,
    /// Finite provider score, not yet sorted or normalized.
    score: f64,
}

impl ValidatedRerankRow {
    /// Stable graph identity.
    #[must_use]
    pub const fn node_uuid(&self) -> [u8; 16] {
        self.node_uuid
    }

    /// Original one-based canonical retrieval rank.
    #[must_use]
    pub const fn retrieval_rank(&self) -> usize {
        self.retrieval_rank
    }

    /// Finite provider score, not yet sorted or normalized.
    #[must_use]
    pub const fn score(&self) -> f64 {
        self.score
    }
}

/// Validated rerank response. It deliberately omits `Debug`.
pub struct ValidatedRerankResponse {
    rows: Vec<ValidatedRerankRow>,
}

impl ValidatedRerankResponse {
    /// Rows in the exact canonical request order.
    #[must_use]
    pub fn rows(&self) -> &[ValidatedRerankRow] {
        &self.rows
    }

    /// Consume the payload-opaque wrapper.
    #[must_use]
    pub fn into_rows(self) -> Vec<ValidatedRerankRow> {
        self.rows
    }
}

/// Validate and reassociate one untrusted document-embedding response.
///
/// # Errors
/// Rejects count, UUID order, dimension, numeric, normalization, resource, and
/// cancellation failures without returning a partial batch.
pub fn validate_document_embedding_response(
    request: &DocumentEmbeddingRequest<'_>,
    outputs: Vec<DocumentEmbeddingOutput>,
    dimension: usize,
    normalization: EmbeddingNormalization,
    vector_limits: VectorStoreLimits,
    checkpoint: &mut dyn FnMut() -> ProviderResult<()>,
) -> ProviderResult<ValidatedDocumentEmbeddings> {
    checkpoint()?;
    if outputs.len() != request.inputs().len() || dimension == 0 {
        return Err(malformed(request.contract()));
    }
    validate_output_values(
        request.contract(),
        outputs.len(),
        dimension,
        request.limits().output_values,
    )?;

    let mut rows = Vec::with_capacity(outputs.len());
    let mut eligible_nodes = BTreeSet::new();
    for (input, output) in request.inputs().iter().zip(outputs) {
        checkpoint()?;
        if output.node_uuid != input.node_uuid || output.vector.len() != dimension {
            return Err(malformed(request.contract()));
        }
        eligible_nodes.insert(input.node_uuid);
        rows.push(EmbeddingBatchRow {
            node_uuid: output.node_uuid,
            vector: output.vector,
        });
    }

    let batch = validate_storage_batch(
        request.contract(),
        rows,
        &eligible_nodes,
        dimension,
        normalization,
        vector_limits,
        checkpoint,
    )?;
    Ok(ValidatedDocumentEmbeddings { batch })
}

/// Validate one untrusted query-embedding response.
///
/// # Errors
/// Rejects dimension, numeric, normalization, resource, and cancellation
/// failures without returning a partial vector.
pub fn validate_query_embedding_response(
    request: &QueryEmbeddingRequest<'_>,
    output: QueryEmbeddingOutput,
    dimension: usize,
    normalization: EmbeddingNormalization,
    vector_limits: VectorStoreLimits,
    checkpoint: &mut dyn FnMut() -> ProviderResult<()>,
) -> ProviderResult<ValidatedQueryEmbedding> {
    checkpoint()?;
    if dimension == 0 || output.vector.len() != dimension {
        return Err(malformed(request.contract()));
    }
    validate_output_values(
        request.contract(),
        1,
        dimension,
        request.limits().output_values,
    )?;

    let query_uuid = [0; 16];
    let batch = validate_storage_batch(
        request.contract(),
        vec![EmbeddingBatchRow {
            node_uuid: query_uuid,
            vector: output.vector,
        }],
        &BTreeSet::from([query_uuid]),
        dimension,
        normalization,
        vector_limits,
        checkpoint,
    )?;
    let mut rows = batch.into_rows();
    let row = rows.pop().ok_or_else(|| malformed(request.contract()))?;
    Ok(ValidatedQueryEmbedding { vector: row.vector })
}

/// Validate and reassociate one untrusted rerank response.
///
/// # Errors
/// Rejects count, UUID order, non-finite score, resource, and cancellation
/// failures without returning partial scores.
pub fn validate_rerank_response(
    request: &RerankRequest<'_>,
    outputs: Vec<RerankOutput>,
    checkpoint: &mut dyn FnMut() -> ProviderResult<()>,
) -> ProviderResult<ValidatedRerankResponse> {
    checkpoint()?;
    if outputs.len() != request.candidates().len() {
        return Err(malformed(request.contract()));
    }
    if outputs.len() > request.limits().output_values {
        return Err(failure(
            request.contract(),
            ProviderFailureClass::ResourceExhausted,
        ));
    }

    let mut rows = Vec::with_capacity(outputs.len());
    for (candidate, output) in request.candidates().iter().zip(outputs) {
        checkpoint()?;
        if output.node_uuid != candidate.node_uuid || !output.score.is_finite() {
            return Err(malformed(request.contract()));
        }
        rows.push(ValidatedRerankRow {
            node_uuid: candidate.node_uuid,
            retrieval_rank: candidate.retrieval_rank,
            score: output.score,
        });
    }
    Ok(ValidatedRerankResponse { rows })
}

fn validate_output_values(
    contract: &ProviderModelContract,
    items: usize,
    dimension: usize,
    limit: usize,
) -> ProviderResult<()> {
    let values = items
        .checked_mul(dimension)
        .ok_or_else(|| failure(contract, ProviderFailureClass::ResourceExhausted))?;
    if values > limit {
        return Err(failure(contract, ProviderFailureClass::ResourceExhausted));
    }
    Ok(())
}

fn validate_storage_batch(
    contract: &ProviderModelContract,
    rows: Vec<EmbeddingBatchRow>,
    eligible_nodes: &BTreeSet<[u8; 16]>,
    dimension: usize,
    normalization: EmbeddingNormalization,
    limits: VectorStoreLimits,
    checkpoint: &mut dyn FnMut() -> ProviderResult<()>,
) -> ProviderResult<ValidatedEmbeddingBatch> {
    let mut cancellation = None;
    let result = validate_embedding_batch(
        rows,
        eligible_nodes,
        dimension,
        normalization,
        limits,
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
    result.map_err(|error| match error {
        SearchArtifactError::ResourceExhausted { .. } => {
            failure(contract, ProviderFailureClass::ResourceExhausted)
        }
        SearchArtifactError::Cancelled => failure(contract, ProviderFailureClass::Cancelled),
        _ => malformed(contract),
    })
}

fn malformed(contract: &ProviderModelContract) -> ProviderError {
    failure(contract, ProviderFailureClass::MalformedResponse)
}

fn failure(contract: &ProviderModelContract, class: ProviderFailureClass) -> ProviderError {
    ProviderError::new(contract, class)
}

#[cfg(test)]
mod tests {
    use gf_storage::{
        EmbeddingNormalization, TokenCountClass, TokenizerIdentity, VectorStoreLimits,
    };

    use crate::{
        DocumentEmbeddingInput, ProviderCapabilities, ProviderCapability, ProviderRequestLimits,
        QueryEmbeddingRequest, RerankCandidate,
    };

    use super::*;

    fn contract(capability: ProviderCapability) -> ProviderModelContract {
        ProviderModelContract::remote(
            None,
            "vendor/model",
            "revision",
            "v1",
            ProviderCapabilities::new([capability]).unwrap(),
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

    #[test]
    fn document_response_validates_identity_dimension_and_normalization() {
        let contract = contract(ProviderCapability::DocumentEmbeddings);
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
        let request =
            DocumentEmbeddingRequest::new(&contract, &inputs, ProviderRequestLimits::default())
                .unwrap();
        let validated = validate_document_embedding_response(
            &request,
            vec![
                DocumentEmbeddingOutput {
                    node_uuid: [1; 16],
                    vector: vec![3.0, 4.0],
                },
                DocumentEmbeddingOutput {
                    node_uuid: [2; 16],
                    vector: vec![0.0, 2.0],
                },
            ],
            2,
            EmbeddingNormalization::L2,
            VectorStoreLimits::default(),
            &mut || Ok(()),
        )
        .unwrap();
        assert_eq!(validated.batch().rows()[0].node_uuid, [1; 16]);
        assert_eq!(validated.batch().rows()[0].vector, vec![0.6, 0.8]);
        assert_eq!(validated.batch().rows()[1].vector, vec![0.0, 1.0]);
    }

    #[test]
    fn document_response_rejects_partial_reordered_dimension_numeric_and_limits() {
        let contract = contract(ProviderCapability::DocumentEmbeddings);
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
        let request = DocumentEmbeddingRequest::new(
            &contract,
            &inputs,
            ProviderRequestLimits {
                output_values: 4,
                ..ProviderRequestLimits::default()
            },
        )
        .unwrap();
        let invalid = [
            vec![DocumentEmbeddingOutput {
                node_uuid: [1; 16],
                vector: vec![1.0, 0.0],
            }],
            vec![
                DocumentEmbeddingOutput {
                    node_uuid: [2; 16],
                    vector: vec![1.0, 0.0],
                },
                DocumentEmbeddingOutput {
                    node_uuid: [1; 16],
                    vector: vec![0.0, 1.0],
                },
            ],
            vec![
                DocumentEmbeddingOutput {
                    node_uuid: [1; 16],
                    vector: vec![1.0],
                },
                DocumentEmbeddingOutput {
                    node_uuid: [2; 16],
                    vector: vec![0.0, 1.0],
                },
            ],
            vec![
                DocumentEmbeddingOutput {
                    node_uuid: [1; 16],
                    vector: vec![f32::NAN, 1.0],
                },
                DocumentEmbeddingOutput {
                    node_uuid: [2; 16],
                    vector: vec![0.0, 1.0],
                },
            ],
        ];
        for outputs in invalid {
            assert!(
                validate_document_embedding_response(
                    &request,
                    outputs,
                    2,
                    EmbeddingNormalization::None,
                    VectorStoreLimits::default(),
                    &mut || Ok(())
                )
                .is_err()
            );
        }

        let limited_request = DocumentEmbeddingRequest::new(
            &contract,
            &inputs,
            ProviderRequestLimits {
                output_values: 3,
                ..ProviderRequestLimits::default()
            },
        )
        .unwrap();
        assert!(matches!(
            validate_document_embedding_response(
                &limited_request,
                vec![
                    DocumentEmbeddingOutput {
                        node_uuid: [1; 16],
                        vector: vec![1.0, 0.0],
                    },
                    DocumentEmbeddingOutput {
                        node_uuid: [2; 16],
                        vector: vec![0.0, 1.0],
                    },
                ],
                2,
                EmbeddingNormalization::None,
                VectorStoreLimits::default(),
                &mut || Ok(())
            ),
            Err(error) if error.class() == ProviderFailureClass::ResourceExhausted
        ));
    }

    #[test]
    fn query_and_rerank_validate_without_exposing_partial_values() {
        let query_contract = contract(ProviderCapability::QueryEmbeddings);
        let query_request = QueryEmbeddingRequest::new(
            &query_contract,
            "query",
            1,
            ProviderRequestLimits::default(),
        )
        .unwrap();
        let query = validate_query_embedding_response(
            &query_request,
            QueryEmbeddingOutput {
                vector: vec![3.0, 4.0],
            },
            2,
            EmbeddingNormalization::L2,
            VectorStoreLimits::default(),
            &mut || Ok(()),
        )
        .unwrap();
        assert_eq!(query.vector(), &[0.6, 0.8]);
        for vector in [vec![1.0], vec![f32::INFINITY, 1.0], vec![0.0, 0.0]] {
            assert!(
                validate_query_embedding_response(
                    &query_request,
                    QueryEmbeddingOutput { vector },
                    2,
                    EmbeddingNormalization::None,
                    VectorStoreLimits::default(),
                    &mut || Ok(())
                )
                .is_err()
            );
        }

        let rerank_contract = contract(ProviderCapability::CandidateReranking);
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
        let rerank_request = RerankRequest::new(
            &rerank_contract,
            "rerank query",
            2,
            &candidates,
            ProviderRequestLimits::default(),
        )
        .unwrap();
        let reranked = validate_rerank_response(
            &rerank_request,
            vec![
                RerankOutput {
                    node_uuid: [1; 16],
                    score: 0.25,
                },
                RerankOutput {
                    node_uuid: [2; 16],
                    score: -0.5,
                },
            ],
            &mut || Ok(()),
        )
        .unwrap();
        assert_eq!(reranked.rows()[1].node_uuid(), [2; 16]);
        assert_eq!(reranked.rows()[1].retrieval_rank(), 2);
        assert_eq!(reranked.rows()[1].score(), -0.5);

        for outputs in [
            vec![RerankOutput {
                node_uuid: [1; 16],
                score: 1.0,
            }],
            vec![
                RerankOutput {
                    node_uuid: [2; 16],
                    score: 1.0,
                },
                RerankOutput {
                    node_uuid: [1; 16],
                    score: 0.0,
                },
            ],
            vec![
                RerankOutput {
                    node_uuid: [1; 16],
                    score: f64::NAN,
                },
                RerankOutput {
                    node_uuid: [2; 16],
                    score: 0.0,
                },
            ],
        ] {
            assert!(validate_rerank_response(&rerank_request, outputs, &mut || Ok(())).is_err());
        }
    }

    #[test]
    fn cancellation_and_errors_are_redacted_and_classified() {
        let contract = contract(ProviderCapability::QueryEmbeddings);
        let request = QueryEmbeddingRequest::new(
            &contract,
            "private query text",
            1,
            ProviderRequestLimits::default(),
        )
        .unwrap();
        let result = validate_query_embedding_response(
            &request,
            QueryEmbeddingOutput {
                vector: vec![1.0, 0.0],
            },
            2,
            EmbeddingNormalization::None,
            VectorStoreLimits::default(),
            &mut || {
                Err(ProviderError::new(
                    &contract,
                    ProviderFailureClass::Cancelled,
                ))
            },
        );
        let Err(error) = result else {
            panic!("cancellation must return no validated vector");
        };
        assert_eq!(error.class(), ProviderFailureClass::Cancelled);
        assert!(!error.to_string().contains("private query text"));

        let malformed = validate_query_embedding_response(
            &request,
            QueryEmbeddingOutput {
                vector: vec![f32::NAN, 1.0],
            },
            2,
            EmbeddingNormalization::None,
            VectorStoreLimits::default(),
            &mut || Ok(()),
        );
        let Err(error) = malformed else {
            panic!("non-finite response must fail");
        };
        assert_eq!(error.class(), ProviderFailureClass::MalformedResponse);
    }
}
