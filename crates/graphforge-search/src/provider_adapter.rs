//! Statically distinct provider adapter request and response boundaries.

use std::collections::BTreeSet;

use crate::{
    ProviderCapability, ProviderError, ProviderFailureClass, ProviderModelContract,
    ProviderRequestLimits, ProviderResult,
};

/// One ephemeral document input. It deliberately omits `Debug`.
#[derive(Clone, Copy)]
pub struct DocumentEmbeddingInput<'a> {
    /// Stable graph identity.
    pub node_uuid: [u8; 16],
    /// Explicit outbound text.
    pub text: &'a str,
    /// Count under the resolved tokenizer contract.
    pub token_count: u64,
}

/// Validated UUID-ordered document request. It deliberately omits `Debug`.
pub struct DocumentEmbeddingRequest<'a> {
    contract: ProviderModelContract,
    inputs: &'a [DocumentEmbeddingInput<'a>],
    limits: ProviderRequestLimits,
}

impl<'a> DocumentEmbeddingRequest<'a> {
    /// Validate one document request against an exact model contract.
    ///
    /// # Errors
    /// Rejects unsupported capability, non-canonical UUID order, empty text,
    /// tokenizer overflow, or aggregate item/byte/token exhaustion.
    pub fn new(
        contract: &ProviderModelContract,
        inputs: &'a [DocumentEmbeddingInput<'a>],
        limits: ProviderRequestLimits,
    ) -> ProviderResult<Self> {
        contract.require(ProviderCapability::DocumentEmbeddings)?;
        validate_limits(contract, limits)?;
        validate_item_count(contract, inputs.len(), limits)?;
        if inputs
            .windows(2)
            .any(|pair| pair[0].node_uuid >= pair[1].node_uuid)
        {
            return Err(failure(contract, ProviderFailureClass::InvalidRequest));
        }
        validate_text_work(
            contract,
            inputs.iter().map(|input| (input.text, input.token_count)),
            limits,
        )?;
        Ok(Self {
            contract: contract.clone(),
            inputs,
            limits,
        })
    }

    /// Exact model contract used during preflight.
    #[must_use]
    pub const fn contract(&self) -> &ProviderModelContract {
        &self.contract
    }

    /// Canonical UUID-ordered inputs.
    #[must_use]
    pub const fn inputs(&self) -> &[DocumentEmbeddingInput<'a>] {
        self.inputs
    }

    /// Validated invocation limits.
    #[must_use]
    pub const fn limits(&self) -> ProviderRequestLimits {
        self.limits
    }
}

/// Validated query request. It deliberately omits `Debug`.
pub struct QueryEmbeddingRequest<'a> {
    contract: ProviderModelContract,
    text: &'a str,
    token_count: u64,
    limits: ProviderRequestLimits,
}

impl<'a> QueryEmbeddingRequest<'a> {
    /// Validate one query request against an exact model contract.
    ///
    /// # Errors
    /// Rejects unsupported capability, empty text, tokenizer overflow, or
    /// aggregate byte/token exhaustion.
    pub fn new(
        contract: &ProviderModelContract,
        text: &'a str,
        token_count: u64,
        limits: ProviderRequestLimits,
    ) -> ProviderResult<Self> {
        contract.require(ProviderCapability::QueryEmbeddings)?;
        validate_limits(contract, limits)?;
        validate_item_count(contract, 1, limits)?;
        validate_text_work(contract, std::iter::once((text, token_count)), limits)?;
        Ok(Self {
            contract: contract.clone(),
            text,
            token_count,
            limits,
        })
    }

    /// Exact model contract used during preflight.
    #[must_use]
    pub const fn contract(&self) -> &ProviderModelContract {
        &self.contract
    }

    /// Explicit query text.
    #[must_use]
    pub const fn text(&self) -> &str {
        self.text
    }

    /// Count under the resolved tokenizer contract.
    #[must_use]
    pub const fn token_count(&self) -> u64 {
        self.token_count
    }

    /// Validated invocation limits.
    #[must_use]
    pub const fn limits(&self) -> ProviderRequestLimits {
        self.limits
    }
}

/// One ephemeral canonical retrieval candidate. It deliberately omits `Debug`.
#[derive(Clone, Copy)]
pub struct RerankCandidate<'a> {
    /// Stable graph identity.
    pub node_uuid: [u8; 16],
    /// One-based canonical retrieval rank.
    pub retrieval_rank: usize,
    /// Explicit outbound candidate text.
    pub text: &'a str,
    /// Count under the resolved tokenizer contract.
    pub token_count: u64,
}

/// Validated retrieval-rank-ordered rerank request. It omits `Debug`.
pub struct RerankRequest<'a> {
    contract: ProviderModelContract,
    query: &'a str,
    query_token_count: u64,
    candidates: &'a [RerankCandidate<'a>],
    limits: ProviderRequestLimits,
}

impl<'a> RerankRequest<'a> {
    /// Validate one rerank request against an exact model contract.
    ///
    /// # Errors
    /// Rejects unsupported capability, non-canonical/duplicate identity,
    /// empty query/text, tokenizer overflow, or aggregate exhaustion.
    pub fn new(
        contract: &ProviderModelContract,
        query: &'a str,
        query_token_count: u64,
        candidates: &'a [RerankCandidate<'a>],
        limits: ProviderRequestLimits,
    ) -> ProviderResult<Self> {
        contract.require(ProviderCapability::CandidateReranking)?;
        validate_limits(contract, limits)?;
        validate_item_count(contract, candidates.len(), limits)?;
        if query.is_empty() || query_token_count == 0 {
            return Err(failure(contract, ProviderFailureClass::InvalidRequest));
        }
        let mut uuids = BTreeSet::new();
        if candidates.iter().enumerate().any(|(index, candidate)| {
            candidate.retrieval_rank != index + 1 || !uuids.insert(candidate.node_uuid)
        }) {
            return Err(failure(contract, ProviderFailureClass::InvalidRequest));
        }
        validate_text_work(
            contract,
            std::iter::once((query, query_token_count)).chain(
                candidates
                    .iter()
                    .map(|candidate| (candidate.text, candidate.token_count)),
            ),
            limits,
        )?;
        Ok(Self {
            contract: contract.clone(),
            query,
            query_token_count,
            candidates,
            limits,
        })
    }

    /// Exact model contract used during preflight.
    #[must_use]
    pub const fn contract(&self) -> &ProviderModelContract {
        &self.contract
    }

    /// Explicit outbound rerank query.
    #[must_use]
    pub const fn query(&self) -> &str {
        self.query
    }

    /// Query count under the resolved tokenizer contract.
    #[must_use]
    pub const fn query_token_count(&self) -> u64 {
        self.query_token_count
    }

    /// Canonical one-based retrieval-order candidates.
    #[must_use]
    pub const fn candidates(&self) -> &[RerankCandidate<'a>] {
        self.candidates
    }

    /// Validated invocation limits.
    #[must_use]
    pub const fn limits(&self) -> ProviderRequestLimits {
        self.limits
    }
}

/// One UUID-associated untrusted document embedding response.
pub struct DocumentEmbeddingOutput {
    /// Stable graph identity echoed by the adapter.
    pub node_uuid: [u8; 16],
    /// Numeric output validated by a later execution boundary.
    pub vector: Vec<f32>,
}

/// One untrusted query embedding response.
pub struct QueryEmbeddingOutput {
    /// Numeric output validated by a later execution boundary.
    pub vector: Vec<f32>,
}

/// One UUID-associated untrusted reranking response.
pub struct RerankOutput {
    /// Stable candidate identity echoed by the adapter.
    pub node_uuid: [u8; 16],
    /// Provider score validated by a later execution boundary.
    pub score: f64,
}

/// Adapter boundary for canonical UUID-keyed document embeddings.
pub trait DocumentEmbeddingProvider {
    /// Exact model contract used by this adapter.
    fn contract(&self) -> &ProviderModelContract;

    /// Execute one preflighted request.
    fn provide_documents(
        &mut self,
        request: &DocumentEmbeddingRequest<'_>,
        checkpoint: &mut dyn FnMut() -> ProviderResult<()>,
    ) -> ProviderResult<Vec<DocumentEmbeddingOutput>>;
}

/// Adapter boundary for one compatible query embedding.
pub trait QueryEmbeddingProvider {
    /// Exact model contract used by this adapter.
    fn contract(&self) -> &ProviderModelContract;

    /// Execute one preflighted request.
    fn provide_query(
        &mut self,
        request: &QueryEmbeddingRequest<'_>,
        checkpoint: &mut dyn FnMut() -> ProviderResult<()>,
    ) -> ProviderResult<QueryEmbeddingOutput>;
}

/// Adapter boundary for explicit bounded post-retrieval reranking.
pub trait CandidateReranker {
    /// Exact model contract used by this adapter.
    fn contract(&self) -> &ProviderModelContract;

    /// Execute one preflighted request.
    fn provide_rerank(
        &mut self,
        request: &RerankRequest<'_>,
        checkpoint: &mut dyn FnMut() -> ProviderResult<()>,
    ) -> ProviderResult<Vec<RerankOutput>>;
}

/// Dispatch one exact document request after contract and cancellation checks.
pub fn embed_documents(
    provider: &mut dyn DocumentEmbeddingProvider,
    request: &DocumentEmbeddingRequest<'_>,
    checkpoint: &mut dyn FnMut() -> ProviderResult<()>,
) -> ProviderResult<Vec<DocumentEmbeddingOutput>> {
    validate_dispatch(provider.contract(), request.contract())?;
    checkpoint()?;
    provider.provide_documents(request, checkpoint)
}

/// Dispatch one exact query request after contract and cancellation checks.
pub fn embed_query(
    provider: &mut dyn QueryEmbeddingProvider,
    request: &QueryEmbeddingRequest<'_>,
    checkpoint: &mut dyn FnMut() -> ProviderResult<()>,
) -> ProviderResult<QueryEmbeddingOutput> {
    validate_dispatch(provider.contract(), request.contract())?;
    checkpoint()?;
    provider.provide_query(request, checkpoint)
}

/// Dispatch one exact rerank request after contract and cancellation checks.
pub fn rerank_candidates(
    provider: &mut dyn CandidateReranker,
    request: &RerankRequest<'_>,
    checkpoint: &mut dyn FnMut() -> ProviderResult<()>,
) -> ProviderResult<Vec<RerankOutput>> {
    validate_dispatch(provider.contract(), request.contract())?;
    checkpoint()?;
    provider.provide_rerank(request, checkpoint)
}

fn validate_dispatch(
    provider: &ProviderModelContract,
    request: &ProviderModelContract,
) -> ProviderResult<()> {
    if provider == request {
        Ok(())
    } else {
        Err(failure(provider, ProviderFailureClass::InvalidRequest))
    }
}

fn validate_limits(
    contract: &ProviderModelContract,
    limits: ProviderRequestLimits,
) -> ProviderResult<()> {
    limits
        .validate()
        .map(|_| ())
        .map_err(|_| failure(contract, ProviderFailureClass::ResourceExhausted))
}

fn validate_text_work<'a>(
    contract: &ProviderModelContract,
    inputs: impl IntoIterator<Item = (&'a str, u64)>,
    limits: ProviderRequestLimits,
) -> ProviderResult<()> {
    let mut bytes = 0_usize;
    let mut tokens = 0_u64;
    for (text, token_count) in inputs {
        if text.is_empty() {
            return Err(failure(contract, ProviderFailureClass::InvalidRequest));
        }
        if token_count > contract.tokenizer().max_input_tokens {
            return Err(failure(contract, ProviderFailureClass::ResourceExhausted));
        }
        bytes = bytes
            .checked_add(text.len())
            .ok_or_else(|| failure(contract, ProviderFailureClass::ResourceExhausted))?;
        tokens = tokens
            .checked_add(token_count)
            .ok_or_else(|| failure(contract, ProviderFailureClass::ResourceExhausted))?;
    }
    if bytes > limits.input_bytes || tokens > limits.input_tokens {
        return Err(failure(contract, ProviderFailureClass::ResourceExhausted));
    }
    Ok(())
}

fn validate_item_count(
    contract: &ProviderModelContract,
    items: usize,
    limits: ProviderRequestLimits,
) -> ProviderResult<()> {
    if items == 0 {
        return Err(failure(contract, ProviderFailureClass::InvalidRequest));
    }
    if items > limits.items {
        return Err(failure(contract, ProviderFailureClass::ResourceExhausted));
    }
    Ok(())
}

fn failure(contract: &ProviderModelContract, class: ProviderFailureClass) -> ProviderError {
    ProviderError::new(contract, class)
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use graphforge_storage::{TokenCountClass, TokenizerIdentity};

    use crate::ProviderCapabilities;

    use super::*;

    fn contract(capabilities: &[ProviderCapability], model: &str) -> ProviderModelContract {
        ProviderModelContract::remote(
            None,
            model,
            "unavailable",
            "v1",
            ProviderCapabilities::new(capabilities.iter().copied()).unwrap(),
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

    struct Fake {
        contract: ProviderModelContract,
        calls: Cell<usize>,
    }

    impl DocumentEmbeddingProvider for Fake {
        fn contract(&self) -> &ProviderModelContract {
            &self.contract
        }

        fn provide_documents(
            &mut self,
            request: &DocumentEmbeddingRequest<'_>,
            checkpoint: &mut dyn FnMut() -> ProviderResult<()>,
        ) -> ProviderResult<Vec<DocumentEmbeddingOutput>> {
            checkpoint()?;
            self.calls.set(self.calls.get() + 1);
            Ok(request
                .inputs()
                .iter()
                .map(|input| DocumentEmbeddingOutput {
                    node_uuid: input.node_uuid,
                    vector: vec![1.0],
                })
                .collect())
        }
    }

    impl QueryEmbeddingProvider for Fake {
        fn contract(&self) -> &ProviderModelContract {
            &self.contract
        }

        fn provide_query(
            &mut self,
            _: &QueryEmbeddingRequest<'_>,
            checkpoint: &mut dyn FnMut() -> ProviderResult<()>,
        ) -> ProviderResult<QueryEmbeddingOutput> {
            checkpoint()?;
            self.calls.set(self.calls.get() + 1);
            Ok(QueryEmbeddingOutput { vector: vec![2.0] })
        }
    }

    impl CandidateReranker for Fake {
        fn contract(&self) -> &ProviderModelContract {
            &self.contract
        }

        fn provide_rerank(
            &mut self,
            request: &RerankRequest<'_>,
            checkpoint: &mut dyn FnMut() -> ProviderResult<()>,
        ) -> ProviderResult<Vec<RerankOutput>> {
            checkpoint()?;
            assert_eq!(request.query(), "rerank query");
            assert_eq!(request.query_token_count(), 2);
            self.calls.set(self.calls.get() + 1);
            Ok(request
                .candidates()
                .iter()
                .map(|candidate| RerankOutput {
                    node_uuid: candidate.node_uuid,
                    score: 1.0,
                })
                .collect())
        }
    }

    #[test]
    fn document_preflight_rejects_capability_order_text_and_limits() {
        let document = DocumentEmbeddingInput {
            node_uuid: [1; 16],
            text: "document",
            token_count: 1,
        };
        let query_only = contract(&[ProviderCapability::QueryEmbeddings], "vendor/query");
        assert!(
            DocumentEmbeddingRequest::new(
                &query_only,
                &[document],
                ProviderRequestLimits::default()
            )
            .is_err()
        );

        let document_contract = contract(
            &[ProviderCapability::DocumentEmbeddings],
            "vendor/documents",
        );
        let reversed = [
            DocumentEmbeddingInput {
                node_uuid: [2; 16],
                text: "second",
                token_count: 1,
            },
            DocumentEmbeddingInput {
                node_uuid: [1; 16],
                text: "first",
                token_count: 1,
            },
        ];
        assert!(
            DocumentEmbeddingRequest::new(
                &document_contract,
                &reversed,
                ProviderRequestLimits::default()
            )
            .is_err()
        );
        for input in [
            DocumentEmbeddingInput {
                node_uuid: [1; 16],
                text: "",
                token_count: 1,
            },
            DocumentEmbeddingInput {
                node_uuid: [1; 16],
                text: "document",
                token_count: 17,
            },
        ] {
            assert!(
                DocumentEmbeddingRequest::new(
                    &document_contract,
                    &[input],
                    ProviderRequestLimits::default()
                )
                .is_err()
            );
        }
        assert!(
            DocumentEmbeddingRequest::new(
                &document_contract,
                &[document],
                ProviderRequestLimits {
                    input_bytes: 1,
                    ..ProviderRequestLimits::default()
                }
            )
            .is_err()
        );
    }

    #[test]
    fn canonical_requests_dispatch_through_three_distinct_boundaries() {
        let model_contract = contract(
            &[
                ProviderCapability::DocumentEmbeddings,
                ProviderCapability::QueryEmbeddings,
                ProviderCapability::CandidateReranking,
            ],
            "vendor/all",
        );
        let mut fake = Fake {
            contract: model_contract.clone(),
            calls: Cell::new(0),
        };
        let documents = [DocumentEmbeddingInput {
            node_uuid: [1; 16],
            text: "document",
            token_count: 1,
        }];
        let document_request = DocumentEmbeddingRequest::new(
            &model_contract,
            &documents,
            ProviderRequestLimits::default(),
        )
        .unwrap();
        assert_eq!(
            embed_documents(&mut fake, &document_request, &mut || Ok(())).unwrap()[0].node_uuid,
            [1; 16]
        );

        let query = QueryEmbeddingRequest::new(
            &model_contract,
            "query",
            1,
            ProviderRequestLimits::default(),
        )
        .unwrap();
        assert_eq!(
            embed_query(&mut fake, &query, &mut || Ok(()))
                .unwrap()
                .vector,
            vec![2.0]
        );

        let candidates = [RerankCandidate {
            node_uuid: [1; 16],
            retrieval_rank: 1,
            text: "candidate",
            token_count: 1,
        }];
        let rerank = RerankRequest::new(
            &model_contract,
            "rerank query",
            2,
            &candidates,
            ProviderRequestLimits::default(),
        )
        .unwrap();
        assert_eq!(rerank.query(), "rerank query");
        assert_eq!(rerank.query_token_count(), 2);
        assert_eq!(
            rerank_candidates(&mut fake, &rerank, &mut || Ok(())).unwrap()[0].node_uuid,
            [1; 16]
        );
        assert_eq!(fake.calls.get(), 3);
    }

    #[test]
    fn rerank_identity_query_limits_contract_mismatch_and_cancel_are_preflighted() {
        let model_contract = contract(
            &[
                ProviderCapability::QueryEmbeddings,
                ProviderCapability::CandidateReranking,
            ],
            "vendor/a",
        );
        let duplicate = [
            RerankCandidate {
                node_uuid: [1; 16],
                retrieval_rank: 1,
                text: "first",
                token_count: 1,
            },
            RerankCandidate {
                node_uuid: [1; 16],
                retrieval_rank: 2,
                text: "second",
                token_count: 1,
            },
        ];
        assert!(
            RerankRequest::new(
                &model_contract,
                "query",
                1,
                &duplicate,
                ProviderRequestLimits::default()
            )
            .is_err()
        );

        let non_canonical = [
            RerankCandidate {
                node_uuid: [1; 16],
                retrieval_rank: 1,
                text: "first",
                token_count: 1,
            },
            RerankCandidate {
                node_uuid: [2; 16],
                retrieval_rank: 3,
                text: "second",
                token_count: 1,
            },
        ];
        assert!(
            RerankRequest::new(
                &model_contract,
                "query",
                1,
                &non_canonical,
                ProviderRequestLimits::default()
            )
            .is_err()
        );

        let query_only = contract(&[ProviderCapability::QueryEmbeddings], "vendor/query");
        assert!(
            RerankRequest::new(
                &query_only,
                "query",
                1,
                &non_canonical[..1],
                ProviderRequestLimits::default()
            )
            .is_err()
        );

        let empty = RerankRequest::new(
            &model_contract,
            "query",
            1,
            &[],
            ProviderRequestLimits::default(),
        );
        let Err(empty_error) = empty else {
            panic!("empty rerank input must fail preflight");
        };
        assert_eq!(empty_error.class(), ProviderFailureClass::InvalidRequest);

        for candidate in [
            RerankCandidate {
                node_uuid: [1; 16],
                retrieval_rank: 1,
                text: "",
                token_count: 1,
            },
            RerankCandidate {
                node_uuid: [1; 16],
                retrieval_rank: 1,
                text: "candidate",
                token_count: 17,
            },
        ] {
            assert!(
                RerankRequest::new(
                    &model_contract,
                    "query",
                    1,
                    &[candidate],
                    ProviderRequestLimits::default()
                )
                .is_err()
            );
        }

        let canonical = [
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
        for (query, query_tokens) in [("", 1), ("query", 0), ("query", 17)] {
            assert!(
                RerankRequest::new(
                    &model_contract,
                    query,
                    query_tokens,
                    &canonical,
                    ProviderRequestLimits::default(),
                )
                .is_err()
            );
        }
        for limits in [
            ProviderRequestLimits {
                items: 1,
                ..ProviderRequestLimits::default()
            },
            ProviderRequestLimits {
                input_bytes: 1,
                ..ProviderRequestLimits::default()
            },
            ProviderRequestLimits {
                input_tokens: 1,
                ..ProviderRequestLimits::default()
            },
        ] {
            assert!(RerankRequest::new(&model_contract, "query", 1, &canonical, limits).is_err());
        }
        assert!(
            QueryEmbeddingRequest::new(
                &model_contract,
                "query",
                1,
                ProviderRequestLimits {
                    input_tokens: 0,
                    ..ProviderRequestLimits::default()
                }
            )
            .is_err()
        );

        let query = QueryEmbeddingRequest::new(
            &model_contract,
            "query",
            1,
            ProviderRequestLimits::default(),
        )
        .unwrap();
        let mut wrong = Fake {
            contract: contract(&[ProviderCapability::QueryEmbeddings], "vendor/different"),
            calls: Cell::new(0),
        };
        assert!(embed_query(&mut wrong, &query, &mut || Ok(())).is_err());

        let mut matching = Fake {
            contract: model_contract.clone(),
            calls: Cell::new(0),
        };
        let result = embed_query(&mut matching, &query, &mut || {
            Err(ProviderError::new(
                &model_contract,
                ProviderFailureClass::Cancelled,
            ))
        });
        let Err(error) = result else {
            panic!("cancellation must fail before provider dispatch");
        };
        assert_eq!(error.class(), ProviderFailureClass::Cancelled);
        assert_eq!(matching.calls.get(), 0);
    }
}
