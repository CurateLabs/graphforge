//! Explicit bounded post-retrieval reranking over canonical search hits.

use std::cmp::Ordering;

use gf_storage::{ChunkingIdentity, TokenizerIdentity};

use crate::{
    CandidateReranker, FusedSearchHit, ProviderCheckpoint, ProviderError,
    ProviderExecutionController, ProviderExecutionRuntime, ProviderFailureClass,
    ProviderModelContract, ProviderResult, ProviderWorkEstimate, RerankRequest, rerank_candidates,
    validate_rerank_response,
};

/// Stable v1 provider-score policy: retain finite raw scores without rescaling.
pub const RERANK_SCORE_POLICY: &str = "raw_finite_v1";
/// Only explicit policy that permits canonical results after a reranker failure.
pub const CANONICAL_UNRERANKED_POLICY: &str = "canonical_unreranked";

/// Caller policy for a terminal reranker failure.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RerankFailurePolicy {
    /// Return the redacted provider failure.
    #[default]
    Error,
    /// Return unchanged canonical retrieval with an explicit non-reranked status.
    CanonicalUnreranked,
}

impl RerankFailurePolicy {
    /// Stable public token for option parsing and audit output.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::CanonicalUnreranked => CANONICAL_UNRERANKED_POLICY,
        }
    }
}

/// Whether an omitted compatible reranker produces an advisory.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RerankAdvisoryPolicy {
    /// Return one payload-free advisory.
    #[default]
    Emit,
    /// Suppress the advisory without changing canonical retrieval.
    Suppress,
}

/// Payload-free advisory for an omitted compatible reranker.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RerankOmissionAdvisory {
    provider: String,
    model: String,
}

impl RerankOmissionAdvisory {
    /// Normalized provider token.
    #[must_use]
    pub fn provider(&self) -> &str {
        &self.provider
    }

    /// Exact non-secret model identifier.
    #[must_use]
    pub fn model(&self) -> &str {
        &self.model
    }
}

/// Payload-free counts passed to the caller's rerank cost estimator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RerankWorkShape {
    candidates: usize,
    input_bytes: usize,
    input_tokens: u64,
}

impl RerankWorkShape {
    /// Candidate documents sent in this exact invocation.
    #[must_use]
    pub const fn candidates(self) -> usize {
        self.candidates
    }

    /// Query and candidate UTF-8 bytes without their contents.
    #[must_use]
    pub const fn input_bytes(self) -> usize {
        self.input_bytes
    }

    /// Query and candidate tokens under the resolved tokenizer contract.
    #[must_use]
    pub const fn input_tokens(self) -> u64 {
        self.input_tokens
    }
}

/// Provider-specific rerank pricing boundary that receives counts only.
pub type RerankCostEstimator<'a> = dyn FnMut(RerankWorkShape) -> ProviderResult<u64> + 'a;

/// Auditable non-secret identity for one explicit rerank request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RerankAuditIdentity {
    provider: String,
    model: String,
    revision: String,
    response_contract_version: String,
    tokenizer: TokenizerIdentity,
    chunking: Option<ChunkingIdentity>,
    candidate_count: usize,
    input_bytes: usize,
    input_tokens: u64,
}

impl RerankAuditIdentity {
    /// Normalized provider token.
    #[must_use]
    pub fn provider(&self) -> &str {
        &self.provider
    }

    /// Exact model identifier.
    #[must_use]
    pub fn model(&self) -> &str {
        &self.model
    }

    /// Immutable revision, or the literal `unavailable`.
    #[must_use]
    pub fn revision(&self) -> &str {
        &self.revision
    }

    /// Versioned provider response contract.
    #[must_use]
    pub fn response_contract_version(&self) -> &str {
        &self.response_contract_version
    }

    /// Tokenizer/counting identity used for query and documents.
    #[must_use]
    pub const fn tokenizer(&self) -> &TokenizerIdentity {
        &self.tokenizer
    }

    /// Explicit chunking/input-shaping identity, if any.
    #[must_use]
    pub const fn chunking(&self) -> Option<&ChunkingIdentity> {
        self.chunking.as_ref()
    }

    /// Number of bounded canonical candidates considered.
    #[must_use]
    pub const fn candidate_count(&self) -> usize {
        self.candidate_count
    }

    /// Total outbound UTF-8 bytes without their contents.
    #[must_use]
    pub const fn input_bytes(&self) -> usize {
        self.input_bytes
    }

    /// Total counted outbound tokens.
    #[must_use]
    pub const fn input_tokens(&self) -> u64 {
        self.input_tokens
    }

    /// Stable score interpretation policy.
    #[must_use]
    pub const fn score_policy(&self) -> &'static str {
        RERANK_SCORE_POLICY
    }
}

/// Explicit interpretation of the returned search hits.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RerankStatus {
    /// Canonical retrieval was returned because reranking was omitted.
    Canonical {
        /// Optional suppressible compatible-reranker advisory.
        advisory: Option<RerankOmissionAdvisory>,
    },
    /// A complete provider response produced the returned order and scores.
    Reranked {
        /// Exact non-secret provider and request identity.
        audit: RerankAuditIdentity,
    },
    /// No canonical candidates existed, so no provider call was made.
    NoCandidates {
        /// Configured provider identity with zero request counts.
        audit: RerankAuditIdentity,
    },
    /// Explicit fallback returned byte-equivalent canonical retrieval.
    CanonicalUnreranked {
        /// Exact non-secret identity of the failed requested rerank.
        audit: RerankAuditIdentity,
        /// Stable redacted terminal provider failure class.
        failure: ProviderFailureClass,
    },
}

/// Search hits plus a non-conditional rerank status carried outside Arrow rows.
#[derive(Clone, Debug, PartialEq)]
pub struct RerankApplication {
    hits: Vec<FusedSearchHit>,
    status: RerankStatus,
}

impl RerankApplication {
    /// Returned graph-native UUID hits.
    #[must_use]
    pub fn hits(&self) -> &[FusedSearchHit] {
        &self.hits
    }

    /// Canonical/reranked/fallback interpretation and neutral metadata.
    #[must_use]
    pub const fn status(&self) -> &RerankStatus {
        &self.status
    }

    /// Consume the result while keeping metadata outside canonical rows.
    #[must_use]
    pub fn into_parts(self) -> (Vec<FusedSearchHit>, RerankStatus) {
        (self.hits, self.status)
    }
}

/// Return canonical retrieval unchanged when reranking is omitted.
///
/// # Errors
/// Rejects a configured model that does not advertise candidate reranking.
pub fn omit_reranking(
    canonical_hits: &[FusedSearchHit],
    configured: Option<&ProviderModelContract>,
    advisory_policy: RerankAdvisoryPolicy,
) -> ProviderResult<RerankApplication> {
    let advisory = match (configured, advisory_policy) {
        (Some(contract), RerankAdvisoryPolicy::Emit) => {
            contract.require(crate::ProviderCapability::CandidateReranking)?;
            Some(RerankOmissionAdvisory {
                provider: contract.provider().to_owned(),
                model: contract.model().to_owned(),
            })
        }
        (Some(contract), RerankAdvisoryPolicy::Suppress) => {
            contract.require(crate::ProviderCapability::CandidateReranking)?;
            None
        }
        (None, _) => None,
    };
    Ok(RerankApplication {
        hits: canonical_hits.to_vec(),
        status: RerankStatus::Canonical { advisory },
    })
}

/// Apply one explicit bounded rerank request after canonical retrieval.
///
/// `request` is `None` only for an empty canonical candidate set; that path
/// validates the configured capability and performs no provider call.
///
/// # Errors
/// Rejects contract/candidate mismatch, invalid empty-request pairing,
/// resource exhaustion, cancellation, malformed responses, and terminal
/// provider failures unless the explicit fallback policy applies.
#[allow(clippy::too_many_arguments)]
pub fn apply_reranking(
    canonical_hits: &[FusedSearchHit],
    request: Option<&RerankRequest<'_>>,
    provider: &mut dyn CandidateReranker,
    controller: &mut ProviderExecutionController,
    runtime: &mut dyn ProviderExecutionRuntime,
    failure_policy: RerankFailurePolicy,
    estimate_cost: &mut RerankCostEstimator<'_>,
    checkpoint: &mut ProviderCheckpoint<'_>,
) -> ProviderResult<RerankApplication> {
    let contract = provider.contract();
    contract.require(crate::ProviderCapability::CandidateReranking)?;
    if controller.contract() != contract {
        return Err(invalid(contract));
    }

    if canonical_hits.is_empty() {
        if request.is_some() {
            return Err(invalid(contract));
        }
        checkpoint()?;
        return Ok(RerankApplication {
            hits: Vec::new(),
            status: RerankStatus::NoCandidates {
                audit: audit(
                    contract,
                    RerankWorkShape {
                        candidates: 0,
                        input_bytes: 0,
                        input_tokens: 0,
                    },
                ),
            },
        });
    }

    let request = request.ok_or_else(|| invalid(contract))?;
    if request.contract() != contract || !candidates_match(canonical_hits, request) {
        return Err(invalid(contract));
    }
    checkpoint()?;
    let shape = work_shape(request)?;
    let audit = audit(contract, shape);
    let cost_units = estimate_cost(shape)?;
    let work = ProviderWorkEstimate::new(
        contract,
        shape.candidates,
        shape.input_bytes,
        shape.input_tokens,
        cost_units,
    )?;
    let result = controller.execute(work, runtime, checkpoint, &mut |guarded| {
        let outputs = rerank_candidates(provider, request, guarded)?;
        validate_rerank_response(request, outputs, guarded)
    });

    let validated = match result {
        Ok(value) => value,
        Err(error)
            if failure_policy == RerankFailurePolicy::CanonicalUnreranked
                && fallback_allowed(error.class()) =>
        {
            return Ok(RerankApplication {
                hits: canonical_hits.to_vec(),
                status: RerankStatus::CanonicalUnreranked {
                    audit,
                    failure: error.class(),
                },
            });
        }
        Err(error) => return Err(error),
    };

    let mut hits = validated
        .into_rows()
        .into_iter()
        .zip(canonical_hits)
        .map(|(row, original)| FusedSearchHit {
            node_uuid: row.node_uuid(),
            score: row.score(),
            matched_on: original.matched_on,
        })
        .collect::<Vec<_>>();
    hits.sort_unstable_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| left.node_uuid.cmp(&right.node_uuid))
    });
    Ok(RerankApplication {
        hits,
        status: RerankStatus::Reranked { audit },
    })
}

fn candidates_match(canonical_hits: &[FusedSearchHit], request: &RerankRequest<'_>) -> bool {
    canonical_hits.iter().all(|hit| hit.score.is_finite())
        && canonical_hits
            .windows(2)
            .all(|pair| match pair[0].score.total_cmp(&pair[1].score) {
                Ordering::Greater => true,
                Ordering::Equal => pair[0].node_uuid < pair[1].node_uuid,
                Ordering::Less => false,
            })
        && canonical_hits.len() == request.candidates().len()
        && canonical_hits
            .iter()
            .zip(request.candidates())
            .enumerate()
            .all(|(index, (hit, candidate))| {
                candidate.node_uuid == hit.node_uuid && candidate.retrieval_rank == index + 1
            })
}

fn work_shape(request: &RerankRequest<'_>) -> ProviderResult<RerankWorkShape> {
    let contract = request.contract();
    let mut input_bytes = request.query().len();
    let mut input_tokens = request.query_token_count();
    for candidate in request.candidates() {
        input_bytes = input_bytes
            .checked_add(candidate.text.len())
            .ok_or_else(|| exhausted(contract))?;
        input_tokens = input_tokens
            .checked_add(candidate.token_count)
            .ok_or_else(|| exhausted(contract))?;
    }
    Ok(RerankWorkShape {
        candidates: request.candidates().len(),
        input_bytes,
        input_tokens,
    })
}

fn audit(contract: &ProviderModelContract, shape: RerankWorkShape) -> RerankAuditIdentity {
    RerankAuditIdentity {
        provider: contract.provider().to_owned(),
        model: contract.model().to_owned(),
        revision: contract.revision().to_owned(),
        response_contract_version: contract.response_contract_version().to_owned(),
        tokenizer: contract.tokenizer().clone(),
        chunking: contract.chunking().cloned(),
        candidate_count: shape.candidates,
        input_bytes: shape.input_bytes,
        input_tokens: shape.input_tokens,
    }
}

const fn fallback_allowed(class: ProviderFailureClass) -> bool {
    !matches!(
        class,
        ProviderFailureClass::Cancelled
            | ProviderFailureClass::InvalidRequest
            | ProviderFailureClass::UnsupportedCapability
    )
}

fn invalid(contract: &ProviderModelContract) -> ProviderError {
    ProviderError::new(contract, ProviderFailureClass::InvalidRequest)
}

fn exhausted(contract: &ProviderModelContract) -> ProviderError {
    ProviderError::new(contract, ProviderFailureClass::ResourceExhausted)
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::time::Duration;

    use gf_storage::{TokenCountClass, TokenizerIdentity};

    use crate::{
        MatchedOn, ProviderCapabilities, ProviderCapability, ProviderExecutionLimits,
        ProviderRequestLimits, RerankCandidate, RerankOutput,
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

    #[derive(Clone, Copy)]
    enum Mode {
        Success,
        Timeout,
        Malformed,
    }

    struct FakeReranker {
        contract: ProviderModelContract,
        calls: usize,
        mode: Mode,
    }

    impl CandidateReranker for FakeReranker {
        fn contract(&self) -> &ProviderModelContract {
            &self.contract
        }

        fn provide_rerank(
            &mut self,
            request: &RerankRequest<'_>,
            checkpoint: &mut ProviderCheckpoint<'_>,
        ) -> ProviderResult<Vec<RerankOutput>> {
            checkpoint()?;
            self.calls += 1;
            if matches!(self.mode, Mode::Timeout) {
                return Err(ProviderError::new(
                    &self.contract,
                    ProviderFailureClass::Timeout,
                ));
            }
            let scores = [0.4, 0.9, 0.9];
            let mut outputs = request
                .candidates()
                .iter()
                .zip(scores)
                .map(|(candidate, score)| RerankOutput {
                    node_uuid: candidate.node_uuid,
                    score,
                })
                .collect::<Vec<_>>();
            if matches!(self.mode, Mode::Malformed) {
                outputs[0].node_uuid = [9; 16];
            }
            Ok(outputs)
        }
    }

    fn test_contract(capability: ProviderCapability) -> ProviderModelContract {
        ProviderModelContract::remote(
            None,
            "vendor/reranker",
            "revision-1",
            "wire-v1",
            ProviderCapabilities::new([capability]).unwrap(),
            TokenizerIdentity {
                identifier: "provider-tokenizer".into(),
                version: "1".into(),
                count_class: TokenCountClass::ProviderReported,
                max_input_tokens: 32,
                normalization: "nfc".into(),
            },
            None,
        )
        .unwrap()
    }

    fn hits() -> Vec<FusedSearchHit> {
        vec![
            FusedSearchHit {
                node_uuid: [1; 16],
                score: 0.03,
                matched_on: MatchedOn::Text,
            },
            FusedSearchHit {
                node_uuid: [2; 16],
                score: 0.02,
                matched_on: MatchedOn::TextAndVector,
            },
            FusedSearchHit {
                node_uuid: [3; 16],
                score: 0.01,
                matched_on: MatchedOn::Vector,
            },
        ]
    }

    fn candidates() -> Vec<RerankCandidate<'static>> {
        vec![
            RerankCandidate {
                node_uuid: [1; 16],
                retrieval_rank: 1,
                text: "private-one",
                token_count: 1,
            },
            RerankCandidate {
                node_uuid: [2; 16],
                retrieval_rank: 2,
                text: "private-two",
                token_count: 1,
            },
            RerankCandidate {
                node_uuid: [3; 16],
                retrieval_rank: 3,
                text: "private-three",
                token_count: 1,
            },
        ]
    }

    fn request<'a>(
        contract: &ProviderModelContract,
        candidates: &'a [RerankCandidate<'a>],
    ) -> RerankRequest<'a> {
        RerankRequest::new(
            contract,
            "private-query",
            2,
            candidates,
            ProviderRequestLimits::default(),
        )
        .unwrap()
    }

    fn test_controller(
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

    fn test_provider(contract: &ProviderModelContract, mode: Mode) -> FakeReranker {
        FakeReranker {
            contract: contract.clone(),
            calls: 0,
            mode,
        }
    }

    #[test]
    fn omission_is_byte_equivalent_and_advisory_is_suppressible() {
        let canonical = hits();
        let contract = test_contract(ProviderCapability::CandidateReranking);
        let omitted = omit_reranking(&canonical, None, RerankAdvisoryPolicy::Emit).unwrap();
        assert_eq!(omitted.hits(), canonical);
        assert!(matches!(
            omitted.status(),
            RerankStatus::Canonical { advisory: None }
        ));

        let advised =
            omit_reranking(&canonical, Some(&contract), RerankAdvisoryPolicy::Emit).unwrap();
        assert_eq!(advised.hits(), canonical);
        let RerankStatus::Canonical {
            advisory: Some(advisory),
        } = advised.status()
        else {
            panic!("compatible omitted reranker should advise");
        };
        assert_eq!(advisory.provider(), "openrouter");
        assert_eq!(advisory.model(), "vendor/reranker");

        let suppressed =
            omit_reranking(&canonical, Some(&contract), RerankAdvisoryPolicy::Suppress).unwrap();
        assert_eq!(suppressed.hits(), canonical);
        assert!(matches!(
            suppressed.status(),
            RerankStatus::Canonical { advisory: None }
        ));

        let incompatible = test_contract(ProviderCapability::QueryEmbeddings);
        assert_eq!(
            omit_reranking(&canonical, Some(&incompatible), RerankAdvisoryPolicy::Emit)
                .unwrap_err()
                .class(),
            ProviderFailureClass::UnsupportedCapability
        );
    }

    #[test]
    fn explicit_success_orders_scores_and_uuid_ties_with_neutral_audit() {
        let canonical = hits();
        let contract = test_contract(ProviderCapability::CandidateReranking);
        let candidates = candidates();
        let request = request(&contract, &candidates);
        let mut provider = test_provider(&contract, Mode::Success);
        let mut runtime = FakeRuntime {
            now: Duration::ZERO,
        };
        let mut controller = test_controller(&contract, &runtime);
        let shape = Cell::new(None);
        let result = apply_reranking(
            &canonical,
            Some(&request),
            &mut provider,
            &mut controller,
            &mut runtime,
            RerankFailurePolicy::Error,
            &mut |value| {
                shape.set(Some(value));
                Ok(7)
            },
            &mut || Ok(()),
        )
        .unwrap();

        assert_eq!(provider.calls, 1);
        assert_eq!(result.hits()[0].node_uuid, [2; 16]);
        assert_eq!(result.hits()[1].node_uuid, [3; 16]);
        assert_eq!(result.hits()[2].node_uuid, [1; 16]);
        assert_eq!(result.hits()[0].matched_on, MatchedOn::TextAndVector);
        assert_eq!(result.hits()[1].matched_on, MatchedOn::Vector);
        assert_eq!(result.hits()[2].matched_on, MatchedOn::Text);
        let RerankStatus::Reranked { audit } = result.status() else {
            panic!("successful provider response should be reranked");
        };
        assert_eq!(audit.provider(), "openrouter");
        assert_eq!(audit.model(), "vendor/reranker");
        assert_eq!(audit.revision(), "revision-1");
        assert_eq!(audit.response_contract_version(), "wire-v1");
        assert_eq!(audit.candidate_count(), 3);
        assert_eq!(audit.input_tokens(), 5);
        assert_eq!(audit.score_policy(), RERANK_SCORE_POLICY);
        assert_eq!(shape.get().unwrap().candidates(), 3);
        assert!(!format!("{audit:?}").contains("private"));
    }

    #[test]
    fn default_error_and_explicit_fallback_are_distinct() {
        let canonical = hits();
        let contract = test_contract(ProviderCapability::CandidateReranking);
        let candidates = candidates();
        let request = request(&contract, &candidates);
        let mut runtime = FakeRuntime {
            now: Duration::ZERO,
        };

        let mut failing = test_provider(&contract, Mode::Timeout);
        let mut strict_controller = test_controller(&contract, &runtime);
        assert_eq!(
            apply_reranking(
                &canonical,
                Some(&request),
                &mut failing,
                &mut strict_controller,
                &mut runtime,
                RerankFailurePolicy::Error,
                &mut |_| Ok(0),
                &mut || Ok(())
            )
            .unwrap_err()
            .class(),
            ProviderFailureClass::Timeout
        );

        let mut fallback = test_provider(&contract, Mode::Timeout);
        let mut fallback_controller = test_controller(&contract, &runtime);
        let result = apply_reranking(
            &canonical,
            Some(&request),
            &mut fallback,
            &mut fallback_controller,
            &mut runtime,
            RerankFailurePolicy::CanonicalUnreranked,
            &mut |_| Ok(0),
            &mut || Ok(()),
        )
        .unwrap();
        assert_eq!(result.hits(), canonical);
        assert!(matches!(
            result.status(),
            RerankStatus::CanonicalUnreranked {
                failure: ProviderFailureClass::Timeout,
                ..
            }
        ));

        let mut malformed = test_provider(&contract, Mode::Malformed);
        let mut malformed_controller = test_controller(&contract, &runtime);
        assert_eq!(
            apply_reranking(
                &canonical,
                Some(&request),
                &mut malformed,
                &mut malformed_controller,
                &mut runtime,
                RerankFailurePolicy::Error,
                &mut |_| Ok(0),
                &mut || Ok(())
            )
            .unwrap_err()
            .class(),
            ProviderFailureClass::MalformedResponse
        );
    }

    #[test]
    fn preflight_cancellation_and_empty_candidates_never_call_provider() {
        let contract = test_contract(ProviderCapability::CandidateReranking);
        let candidates = candidates();
        let request = request(&contract, &candidates);
        let mut mismatched_hits = hits();
        mismatched_hits[0].node_uuid = [9; 16];
        let mut runtime = FakeRuntime {
            now: Duration::ZERO,
        };
        let mut provider = test_provider(&contract, Mode::Success);
        let mut controller = test_controller(&contract, &runtime);
        assert_eq!(
            apply_reranking(
                &mismatched_hits,
                Some(&request),
                &mut provider,
                &mut controller,
                &mut runtime,
                RerankFailurePolicy::CanonicalUnreranked,
                &mut |_| Ok(0),
                &mut || Ok(())
            )
            .unwrap_err()
            .class(),
            ProviderFailureClass::InvalidRequest
        );
        assert_eq!(provider.calls, 0);

        let mut cancelled = test_provider(&contract, Mode::Success);
        let mut cancelled_controller = test_controller(&contract, &runtime);
        assert_eq!(
            apply_reranking(
                &hits(),
                Some(&request),
                &mut cancelled,
                &mut cancelled_controller,
                &mut runtime,
                RerankFailurePolicy::CanonicalUnreranked,
                &mut |_| Ok(0),
                &mut || {
                    Err(ProviderError::new(
                        &contract,
                        ProviderFailureClass::Cancelled,
                    ))
                }
            )
            .unwrap_err()
            .class(),
            ProviderFailureClass::Cancelled
        );
        assert_eq!(cancelled.calls, 0);

        let mut empty = test_provider(&contract, Mode::Success);
        let mut empty_controller = test_controller(&contract, &runtime);
        let result = apply_reranking(
            &[],
            None,
            &mut empty,
            &mut empty_controller,
            &mut runtime,
            RerankFailurePolicy::Error,
            &mut |_| Ok(0),
            &mut || Ok(()),
        )
        .unwrap();
        assert!(result.hits().is_empty());
        assert!(matches!(result.status(), RerankStatus::NoCandidates { .. }));
        assert_eq!(empty.calls, 0);
    }
}
