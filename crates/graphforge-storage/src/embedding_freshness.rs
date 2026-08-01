//! Deterministic freshness policy for verified embedding generations.

use crate::{
    EmbeddingCompatibilityId, EmbeddingGenerationId, EmbeddingGenerationManifest,
    EmbeddingSourceFingerprint, EmbeddingSourceState, SearchArtifactError,
};

/// Changed-UUID percentage that makes a generation substantially stale.
pub const EMBEDDING_SUBSTANTIAL_CHANGED_PERCENT: u64 = 5;
/// Relevant committed mutation batches that make a generation substantially stale.
pub const EMBEDDING_SUBSTANTIAL_MUTATION_BATCHES: u64 = 128;

/// Durable mutation evidence observed after one generation source snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EmbeddingMutationObservation {
    current_source: EmbeddingSourceState,
    changed_distinct_uuids: u64,
    relevant_committed_batches: u64,
    structural_mutation: bool,
    scope_proven: bool,
}

impl EmbeddingMutationObservation {
    /// Validate one bounded freshness observation.
    ///
    /// # Errors
    /// Rejects changed UUID or structural evidence without a committed batch.
    pub fn new(
        current_source: EmbeddingSourceState,
        changed_distinct_uuids: u64,
        relevant_committed_batches: u64,
        structural_mutation: bool,
        scope_proven: bool,
    ) -> Result<Self, SearchArtifactError> {
        if relevant_committed_batches == 0 && (changed_distinct_uuids != 0 || structural_mutation) {
            return Err(invalid(
                "embedding mutation observation",
                "changed UUID and structural evidence require a committed relevant batch",
            ));
        }
        Ok(Self {
            current_source,
            changed_distinct_uuids,
            relevant_committed_batches,
            structural_mutation,
            scope_proven,
        })
    }

    /// Current durable dependency projection.
    #[must_use]
    pub const fn current_source(self) -> EmbeddingSourceState {
        self.current_source
    }

    /// Distinct UUIDs changed across relevant committed batches.
    #[must_use]
    pub const fn changed_distinct_uuids(self) -> u64 {
        self.changed_distinct_uuids
    }

    /// Relevant committed mutation batches since the recorded source.
    #[must_use]
    pub const fn relevant_committed_batches(self) -> u64 {
        self.relevant_committed_batches
    }

    /// Whether any relevant topology mutation occurred.
    #[must_use]
    pub const fn structural_mutation(self) -> bool {
        self.structural_mutation
    }

    /// Whether mutation scope was proven from durable metadata.
    #[must_use]
    pub const fn scope_proven(self) -> bool {
        self.scope_proven
    }
}

/// Freshness state for one verified, compatible, complete generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EmbeddingFreshnessState {
    /// Recorded and current dependency projections are exactly identical.
    Fresh,
    /// A bounded relevant mutation occurred below every substantial threshold.
    Stale,
    /// Ordinary search must refresh successfully before serving.
    SubstantiallyStale,
}

/// Stable reason for a non-fresh state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EmbeddingFreshnessReason {
    /// Relevant scope cannot be proven from durable metadata.
    UnprovenMutationScope,
    /// A structural space observed a relevant topology mutation.
    StructuralMutation,
    /// Any relevant mutation affected a recorded empty generation.
    EmptyGenerationMutation,
    /// At least five percent of recorded eligible UUIDs changed.
    ChangedUuidFraction,
    /// At least 128 relevant mutation batches accumulated.
    MutationBatchLimit,
    /// A known relevant mutation remains below substantial thresholds.
    RelevantMutation,
}

impl EmbeddingFreshnessReason {
    /// Stable bounded diagnostic token.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UnprovenMutationScope => "unproven_mutation_scope",
            Self::StructuralMutation => "structural_mutation",
            Self::EmptyGenerationMutation => "empty_generation_mutation",
            Self::ChangedUuidFraction => "changed_uuid_fraction",
            Self::MutationBatchLimit => "mutation_batch_limit",
            Self::RelevantMutation => "relevant_mutation",
        }
    }
}

/// Classified freshness plus the exact current durable source state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EmbeddingFreshness {
    state: EmbeddingFreshnessState,
    reason: Option<EmbeddingFreshnessReason>,
    current_source: EmbeddingSourceState,
}

impl EmbeddingFreshness {
    /// Classified state.
    #[must_use]
    pub const fn state(self) -> EmbeddingFreshnessState {
        self.state
    }

    /// Stable reason, absent only for `fresh`.
    #[must_use]
    pub const fn reason(self) -> Option<EmbeddingFreshnessReason> {
        self.reason
    }

    /// Current durable dependency projection used for classification.
    #[must_use]
    pub const fn current_source(self) -> EmbeddingSourceState {
        self.current_source
    }
}

/// Stable observable diagnostic emitted only for an explicit forced stale read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EmbeddingForcedStaleDiagnostic {
    /// Compatibility lineage selected by the caller.
    pub compatibility_id: EmbeddingCompatibilityId,
    /// Last complete immutable generation served.
    pub generation_id: EmbeddingGenerationId,
    /// Source fingerprint recorded by that generation.
    pub recorded_source: EmbeddingSourceFingerprint,
    /// Current durable dependency fingerprint.
    pub current_source: EmbeddingSourceFingerprint,
    /// Deterministic substantial-staleness reason.
    pub reason: EmbeddingFreshnessReason,
}

impl EmbeddingForcedStaleDiagnostic {
    /// Stable, bounded, content-free diagnostic representation.
    #[must_use]
    pub fn stable_message(self) -> String {
        format!(
            "embedding_force_stale:v1 compatibility_id={} generation_id={} recorded_source={} current_source={} reason={}",
            self.compatibility_id,
            self.generation_id,
            self.recorded_source,
            self.current_source,
            self.reason.as_str(),
        )
    }
}

/// Serving decision after compatibility and primary-data validation succeeded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EmbeddingReadDecision {
    /// Serve the verified fresh generation.
    ServeFresh,
    /// Serve a mildly stale generation while refresh is queued.
    ServeStale {
        /// Stable non-substantial reason.
        reason: EmbeddingFreshnessReason,
    },
    /// Refresh must succeed before ordinary search may serve.
    RefreshRequired {
        /// Stable substantial-staleness reason.
        reason: EmbeddingFreshnessReason,
    },
    /// Explicitly serve the last complete substantially stale generation.
    ServeForcedStale {
        /// Stable observable forced-read diagnostic.
        diagnostic: EmbeddingForcedStaleDiagnostic,
    },
}

/// Classify one verified generation against current durable mutation evidence.
///
/// Precedence is deterministic: unproven scope, structural mutation, empty
/// generation, changed UUID fraction, mutation-batch limit, then ordinary
/// relevant mutation. Wall-clock timestamps never participate.
///
/// # Errors
/// Rejects observations that predate the generation, impossible batch counts,
/// unexplained fingerprint changes, and checked threshold arithmetic overflow.
pub fn classify_embedding_freshness(
    manifest: &EmbeddingGenerationManifest,
    observation: EmbeddingMutationObservation,
) -> Result<EmbeddingFreshness, SearchArtifactError> {
    let recorded = manifest.source();
    let current = observation.current_source;
    if current.graph_generation() < recorded.graph_generation() {
        return Err(invalid(
            "embedding freshness observation",
            "current graph generation predates the recorded generation",
        ));
    }
    let generation_delta = current.graph_generation() - recorded.graph_generation();
    if observation.relevant_committed_batches > generation_delta {
        return Err(invalid(
            "embedding freshness observation",
            "relevant batch count exceeds the committed graph generation delta",
        ));
    }
    let no_relevant_evidence = observation.relevant_committed_batches == 0
        && observation.changed_distinct_uuids == 0
        && !observation.structural_mutation
        && observation.scope_proven;
    if recorded.fingerprint() == current.fingerprint() && no_relevant_evidence {
        return Ok(EmbeddingFreshness {
            state: EmbeddingFreshnessState::Fresh,
            reason: None,
            current_source: current,
        });
    }
    if recorded.fingerprint() != current.fingerprint()
        && no_relevant_evidence
        && observation.scope_proven
    {
        return Err(invalid(
            "embedding freshness observation",
            "source fingerprint changed without a relevant mutation or unproven scope",
        ));
    }

    let reason = if !observation.scope_proven {
        EmbeddingFreshnessReason::UnprovenMutationScope
    } else if observation.structural_mutation {
        EmbeddingFreshnessReason::StructuralMutation
    } else if recorded.eligible_uuid_count() == 0 {
        EmbeddingFreshnessReason::EmptyGenerationMutation
    } else if changed_fraction_is_substantial(
        observation.changed_distinct_uuids,
        recorded.eligible_uuid_count(),
    )? {
        EmbeddingFreshnessReason::ChangedUuidFraction
    } else if observation.relevant_committed_batches >= EMBEDDING_SUBSTANTIAL_MUTATION_BATCHES {
        EmbeddingFreshnessReason::MutationBatchLimit
    } else {
        EmbeddingFreshnessReason::RelevantMutation
    };
    let state = if reason == EmbeddingFreshnessReason::RelevantMutation {
        EmbeddingFreshnessState::Stale
    } else {
        EmbeddingFreshnessState::SubstantiallyStale
    };
    Ok(EmbeddingFreshness {
        state,
        reason: Some(reason),
        current_source: current,
    })
}

/// Select the serving boundary for a classified verified generation.
#[must_use]
pub fn decide_embedding_read(
    manifest: &EmbeddingGenerationManifest,
    freshness: EmbeddingFreshness,
    force_stale: bool,
) -> EmbeddingReadDecision {
    match freshness.state {
        EmbeddingFreshnessState::Fresh => EmbeddingReadDecision::ServeFresh,
        EmbeddingFreshnessState::Stale => EmbeddingReadDecision::ServeStale {
            reason: freshness
                .reason
                .expect("a stale classification always has a reason"),
        },
        EmbeddingFreshnessState::SubstantiallyStale if force_stale => {
            EmbeddingReadDecision::ServeForcedStale {
                diagnostic: EmbeddingForcedStaleDiagnostic {
                    compatibility_id: manifest.compatibility_id(),
                    generation_id: manifest.generation_id(),
                    recorded_source: manifest.source().fingerprint(),
                    current_source: freshness.current_source.fingerprint(),
                    reason: freshness
                        .reason
                        .expect("a substantially stale classification always has a reason"),
                },
            }
        }
        EmbeddingFreshnessState::SubstantiallyStale => EmbeddingReadDecision::RefreshRequired {
            reason: freshness
                .reason
                .expect("a substantially stale classification always has a reason"),
        },
    }
}

fn changed_fraction_is_substantial(
    changed: u64,
    recorded_eligible: u64,
) -> Result<bool, SearchArtifactError> {
    let changed_percent = changed.checked_mul(100).ok_or_else(arithmetic_overflow)?;
    let threshold = recorded_eligible
        .max(1)
        .checked_mul(EMBEDDING_SUBSTANTIAL_CHANGED_PERCENT)
        .ok_or_else(arithmetic_overflow)?;
    Ok(changed_percent >= threshold)
}

fn arithmetic_overflow() -> SearchArtifactError {
    SearchArtifactError::ResourceExhausted {
        resource: "embedding_freshness_arithmetic",
        limit: u64::MAX,
    }
}

fn invalid(field: &'static str, reason: impl Into<String>) -> SearchArtifactError {
    SearchArtifactError::InvalidSelector {
        field,
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        EmbeddingContentDigest, EmbeddingGenerationManifestInput, EmbeddingPublicationFingerprint,
    };

    fn source(generation: u64, eligible: u64, marker: u8) -> EmbeddingSourceState {
        EmbeddingSourceState::new(
            generation,
            [marker; 32],
            [marker.wrapping_add(1); 32],
            eligible,
        )
    }

    fn manifest(recorded: EmbeddingSourceState, generated_at: i64) -> EmbeddingGenerationManifest {
        EmbeddingGenerationManifest::new(EmbeddingGenerationManifestInput {
            compatibility_id: EmbeddingCompatibilityId::from_hex(&"11".repeat(32)).unwrap(),
            source: recorded,
            content_digest: EmbeddingContentDigest::from_hex(&"22".repeat(32)).unwrap(),
            vector_count: recorded.eligible_uuid_count(),
            dimension: 2,
            generated_at_micros: generated_at,
            committed_at_micros: generated_at + 1,
            publication_fingerprint: EmbeddingPublicationFingerprint::from_hex(&"33".repeat(32))
                .unwrap(),
        })
        .unwrap()
    }

    fn observation(
        current: EmbeddingSourceState,
        changed: u64,
        batches: u64,
        structural: bool,
        proven: bool,
    ) -> EmbeddingMutationObservation {
        EmbeddingMutationObservation::new(current, changed, batches, structural, proven).unwrap()
    }

    #[test]
    fn exact_source_without_mutation_evidence_is_fresh_and_time_independent() {
        let recorded = source(7, 100, 1);
        let observation = observation(recorded, 0, 0, false, true);
        let early = classify_embedding_freshness(&manifest(recorded, 10), observation).unwrap();
        let late = classify_embedding_freshness(&manifest(recorded, 9_999), observation).unwrap();
        assert_eq!(early, late);
        assert_eq!(early.state(), EmbeddingFreshnessState::Fresh);
        assert_eq!(early.reason(), None);
        assert_eq!(
            decide_embedding_read(&manifest(recorded, 10), early, true),
            EmbeddingReadDecision::ServeFresh
        );
    }

    #[test]
    fn substantial_reason_precedence_is_deterministic() {
        let recorded = source(1, 100, 1);
        let current = source(130, 100, 2);
        let cases = [
            (
                observation(current, 100, 129, true, false),
                EmbeddingFreshnessReason::UnprovenMutationScope,
            ),
            (
                observation(current, 100, 129, true, true),
                EmbeddingFreshnessReason::StructuralMutation,
            ),
            (
                observation(current, 5, 127, false, true),
                EmbeddingFreshnessReason::ChangedUuidFraction,
            ),
            (
                observation(current, 4, 128, false, true),
                EmbeddingFreshnessReason::MutationBatchLimit,
            ),
        ];
        for (observation, expected) in cases {
            let freshness =
                classify_embedding_freshness(&manifest(recorded, 10), observation).unwrap();
            assert_eq!(
                freshness.state(),
                EmbeddingFreshnessState::SubstantiallyStale
            );
            assert_eq!(freshness.reason(), Some(expected));
        }
    }

    #[test]
    fn any_relevant_change_to_an_empty_generation_is_substantial() {
        let recorded = source(1, 0, 1);
        let freshness = classify_embedding_freshness(
            &manifest(recorded, 10),
            observation(source(2, 1, 2), 1, 1, false, true),
        )
        .unwrap();
        assert_eq!(
            freshness.reason(),
            Some(EmbeddingFreshnessReason::EmptyGenerationMutation)
        );
    }

    #[test]
    fn changed_uuid_fraction_uses_exact_five_percent_boundary() {
        let recorded = source(1, 100, 1);
        let below = classify_embedding_freshness(
            &manifest(recorded, 10),
            observation(source(2, 100, 2), 4, 1, false, true),
        )
        .unwrap();
        let exact = classify_embedding_freshness(
            &manifest(recorded, 10),
            observation(source(2, 100, 2), 5, 1, false, true),
        )
        .unwrap();
        assert_eq!(below.state(), EmbeddingFreshnessState::Stale);
        assert_eq!(
            exact.reason(),
            Some(EmbeddingFreshnessReason::ChangedUuidFraction)
        );
    }

    #[test]
    fn mutation_batch_limit_uses_exact_128_boundary() {
        let recorded = source(1, 1_000, 1);
        let below = classify_embedding_freshness(
            &manifest(recorded, 10),
            observation(source(128, 1_000, 2), 0, 127, false, true),
        )
        .unwrap();
        let exact = classify_embedding_freshness(
            &manifest(recorded, 10),
            observation(source(129, 1_000, 2), 0, 128, false, true),
        )
        .unwrap();
        assert_eq!(below.state(), EmbeddingFreshnessState::Stale);
        assert_eq!(
            exact.reason(),
            Some(EmbeddingFreshnessReason::MutationBatchLimit)
        );
    }

    #[test]
    fn forced_stale_is_explicit_deterministic_and_content_free() {
        let recorded = source(1, 100, 1);
        let manifest = manifest(recorded, 10);
        let freshness = classify_embedding_freshness(
            &manifest,
            observation(source(2, 100, 2), 100, 1, false, true),
        )
        .unwrap();
        assert_eq!(
            decide_embedding_read(&manifest, freshness, false),
            EmbeddingReadDecision::RefreshRequired {
                reason: EmbeddingFreshnessReason::ChangedUuidFraction,
            }
        );
        let EmbeddingReadDecision::ServeForcedStale { diagnostic } =
            decide_embedding_read(&manifest, freshness, true)
        else {
            panic!("force must select the last complete generation");
        };
        let message = diagnostic.stable_message();
        assert_eq!(message, diagnostic.stable_message());
        assert!(message.starts_with("embedding_force_stale:v1 compatibility_id="));
        assert!(message.ends_with("reason=changed_uuid_fraction"));
        assert!(!message.contains("vector"));
        assert!(!message.contains("body"));
    }

    #[test]
    fn inconsistent_observations_and_overflow_are_structured() {
        let recorded = source(5, 100, 1);
        assert!(EmbeddingMutationObservation::new(recorded, 1, 0, false, true).is_err());
        for observation in [
            observation(source(4, 100, 2), 0, 0, false, false),
            observation(source(6, 100, 2), 0, 2, false, true),
            observation(source(6, 100, 2), 0, 0, false, true),
        ] {
            assert!(classify_embedding_freshness(&manifest(recorded, 10), observation).is_err());
        }

        let overflow_recorded = source(1, u64::MAX, 1);
        let error = classify_embedding_freshness(
            &manifest(overflow_recorded, 10),
            observation(source(2, u64::MAX, 2), u64::MAX, 1, false, true),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            SearchArtifactError::ResourceExhausted {
                resource: "embedding_freshness_arithmetic",
                ..
            }
        ));
    }
}
