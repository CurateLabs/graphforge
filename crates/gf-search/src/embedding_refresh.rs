//! Bounded complete-generation refresh for embedding spaces.

use std::path::Path;

use gf_storage::{
    EmbeddingCompatibilityDescriptor, EmbeddingPublicationOutcome, EmbeddingPublicationRequest,
    EmbeddingReadDecision, EmbeddingSourceState, SearchArtifactError, SearchCoordinationLimits,
    ValidatedEmbeddingBatch, publish_embedding_generation, reset_embedding_mutation_journal,
};

use crate::{EmbeddingReadLimits, PreparedEmbeddingRead, prepare_embedding_read};

/// Bounds shared by complete publication, journal reset, and reopened reads.
#[derive(Clone, Copy, Debug, Default)]
pub struct EmbeddingRefreshLimits {
    /// Vector reopen/publication and mutation-journal bounds.
    pub read: EmbeddingReadLimits,
    /// Per-lineage lock and cleanup timing bounds.
    pub coordination: SearchCoordinationLimits,
}

/// Stable publication metadata for one bounded refresh invocation.
#[derive(Clone, Copy, Debug)]
pub struct EmbeddingRefreshRequest<'a> {
    /// Exact versioned compatibility lineage to refresh.
    pub descriptor: &'a EmbeddingCompatibilityDescriptor,
    /// Producer completion time in UTC microseconds since Unix epoch.
    pub generated_at_micros: i64,
    /// Durable publication time in UTC microseconds since Unix epoch.
    pub committed_at_micros: i64,
}

/// Produce and atomically publish one complete generation against a stable source.
///
/// The complete capture/produce/verify attempt runs at most twice. Publication
/// occurs only when source states immediately before and after production are
/// identical. The new active generation is then bound to a reset durable
/// mutation journal. A failed reset leaves reads fail-closed on missing or
/// mismatched evidence rather than making the generation accidentally fresh.
///
/// # Errors
/// Propagates source, producer, validation, publication, journal, cancellation,
/// resource, corruption, and lock errors. Two changing attempts return
/// [`SearchArtifactError::ConcurrentMutation`].
pub fn refresh_embedding_generation<S, P, C>(
    project_dir: &Path,
    request: EmbeddingRefreshRequest<'_>,
    limits: EmbeddingRefreshLimits,
    mut capture_source: S,
    mut produce: P,
    mut checkpoint: C,
) -> Result<EmbeddingPublicationOutcome, SearchArtifactError>
where
    S: FnMut() -> Result<EmbeddingSourceState, SearchArtifactError>,
    P: FnMut(EmbeddingSourceState) -> Result<ValidatedEmbeddingBatch, SearchArtifactError>,
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    for attempt in 1_u8..=2 {
        checkpoint()?;
        let before = capture_source()?;
        checkpoint()?;
        let batch = produce(before)?;
        checkpoint()?;
        let after = capture_source()?;
        if before != after {
            if attempt == 2 {
                return Err(SearchArtifactError::ConcurrentMutation);
            }
            continue;
        }
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
    unreachable!("the bounded refresh loop returns on both terminal attempts")
}

/// Prepare an embedding read, refreshing any ordinary stale state first.
///
/// Existing forced-stale reads return immediately with their stable diagnostic
/// and never invoke the producer. Fresh reads also avoid producer work. Missing,
/// stale, and substantially stale ordinary reads run the bounded complete
/// refresh and then require the reopened result to classify as fresh.
///
/// # Errors
/// Propagates read-gate and refresh errors. A replacement that is not fresh on
/// its final reopen fails closed as a concurrent mutation.
#[allow(clippy::too_many_arguments)]
pub fn prepare_embedding_read_lazily<S, P, C>(
    project_dir: &Path,
    request: EmbeddingRefreshRequest<'_>,
    force_stale: bool,
    limits: EmbeddingRefreshLimits,
    mut capture_source: S,
    mut produce: P,
    mut checkpoint: C,
) -> Result<PreparedEmbeddingRead, SearchArtifactError>
where
    S: FnMut() -> Result<EmbeddingSourceState, SearchArtifactError>,
    P: FnMut(EmbeddingSourceState) -> Result<ValidatedEmbeddingBatch, SearchArtifactError>,
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    let current = capture_source()?;
    if let Some(prepared) = prepare_embedding_read(
        project_dir,
        request.descriptor,
        current,
        force_stale,
        limits.read,
        &mut checkpoint,
    )? && matches!(
        prepared.decision(),
        EmbeddingReadDecision::ServeFresh | EmbeddingReadDecision::ServeForcedStale { .. }
    ) {
        return Ok(prepared);
    }

    refresh_embedding_generation(
        project_dir,
        request,
        limits,
        &mut capture_source,
        &mut produce,
        &mut checkpoint,
    )?;
    let refreshed_source = capture_source()?;
    let refreshed = prepare_embedding_read(
        project_dir,
        request.descriptor,
        refreshed_source,
        false,
        limits.read,
        &mut checkpoint,
    )?
    .ok_or_else(|| SearchArtifactError::Missing {
        path: project_dir.join("embeddings/spaces"),
    })?;
    if !matches!(refreshed.decision(), EmbeddingReadDecision::ServeFresh) {
        return Err(SearchArtifactError::ConcurrentMutation);
    }
    Ok(refreshed)
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::collections::{BTreeMap, BTreeSet};

    use gf_storage::{
        EmbeddingBatchRow, EmbeddingCompatibilityInput, EmbeddingDistance, EmbeddingMutationBatch,
        EmbeddingMutationJournalLimits, EmbeddingNormalization, EmbeddingProducerIdentity,
        EmbeddingValueType, VectorStoreLimits, merge_embedding_mutation_batch,
        read_embedding_mutation_journal, validate_embedding_batch,
    };

    use super::*;

    const UUID: [u8; 16] = [1; 16];

    fn descriptor(model: &str) -> EmbeddingCompatibilityDescriptor {
        descriptor_with_dimensions(model, 2)
    }

    fn descriptor_with_dimensions(
        model: &str,
        dimensions: u32,
    ) -> EmbeddingCompatibilityDescriptor {
        EmbeddingCompatibilityDescriptor::new(EmbeddingCompatibilityInput {
            producer: EmbeddingProducerIdentity::Local {
                implementation: "test".into(),
                model: model.into(),
                revision: "r1".into(),
                contract_version: "v1".into(),
            },
            dimensions,
            value_type: EmbeddingValueType::Float32,
            normalization: EmbeddingNormalization::None,
            distance: EmbeddingDistance::Cosine,
            tokenizer: None,
            chunking: None,
            hyperparameters: BTreeMap::new(),
            input_recipe: BTreeMap::from([("property".into(), "body".into())]),
            source_projection_recipe: BTreeMap::from([("label".into(), "Document".into())]),
        })
        .unwrap()
    }

    fn source(generation: u64, marker: u8, count: u64) -> EmbeddingSourceState {
        EmbeddingSourceState::new(generation, [marker; 32], [marker + 1; 32], count)
    }

    fn batch(count: u8, marker: f32) -> ValidatedEmbeddingBatch {
        let eligible = (1..=count)
            .map(|value| [value; 16])
            .collect::<BTreeSet<_>>();
        validate_embedding_batch(
            (1..=count)
                .map(|value| EmbeddingBatchRow {
                    node_uuid: [value; 16],
                    vector: vec![f32::from(value), marker],
                })
                .collect(),
            &eligible,
            2,
            EmbeddingNormalization::None,
            VectorStoreLimits::default(),
            || Ok(()),
        )
        .unwrap()
    }

    fn request(descriptor: &EmbeddingCompatibilityDescriptor) -> EmbeddingRefreshRequest<'_> {
        EmbeddingRefreshRequest {
            descriptor,
            generated_at_micros: 20,
            committed_at_micros: 21,
        }
    }

    #[test]
    fn stable_refresh_publishes_and_resets_durable_evidence() {
        let dir = tempfile::tempdir().unwrap();
        let descriptor = descriptor("a");
        let current = source(10, 1, 1);
        let outcome = refresh_embedding_generation(
            dir.path(),
            request(&descriptor),
            EmbeddingRefreshLimits::default(),
            || Ok(current),
            |_| Ok(batch(1, 1.0)),
            || Ok(()),
        )
        .unwrap();
        let journal = read_embedding_mutation_journal(
            dir.path(),
            &outcome.publication().manifest,
            EmbeddingMutationJournalLimits::default(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(journal.observation().unwrap().current_source(), current);
        assert!(matches!(
            prepare_embedding_read(
                dir.path(),
                &descriptor,
                current,
                false,
                EmbeddingReadLimits::default(),
                || Ok(())
            )
            .unwrap()
            .unwrap()
            .decision(),
            EmbeddingReadDecision::ServeFresh
        ));
        assert!(matches!(
            refresh_embedding_generation(
                dir.path(),
                request(&descriptor),
                EmbeddingRefreshLimits::default(),
                || Ok(current),
                |_| Ok(batch(1, 1.0)),
                || Ok(()),
            )
            .unwrap(),
            EmbeddingPublicationOutcome::Reused(_)
        ));
    }

    #[test]
    fn one_mutation_retries_and_two_mutations_publish_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let descriptor = descriptor("a");
        let calls = Cell::new(0_u8);
        let produced = Cell::new(0_u8);
        let outcome = refresh_embedding_generation(
            dir.path(),
            request(&descriptor),
            EmbeddingRefreshLimits::default(),
            || {
                let call = calls.replace(calls.get() + 1);
                Ok(if call == 0 {
                    source(10, 1, 1)
                } else {
                    source(11, 2, 1)
                })
            },
            |_| {
                produced.set(produced.get() + 1);
                Ok(batch(1, 1.0))
            },
            || Ok(()),
        )
        .unwrap();
        assert_eq!(outcome.publication().manifest.source(), source(11, 2, 1));
        assert_eq!(produced.get(), 2);

        let raced = tempfile::tempdir().unwrap();
        let calls = Cell::new(0_u8);
        assert!(matches!(
            refresh_embedding_generation(
                raced.path(),
                request(&descriptor),
                EmbeddingRefreshLimits::default(),
                || {
                    let call = calls.replace(calls.get() + 1);
                    Ok(source(u64::from(call) + 1, call + 1, 1))
                },
                |_| Ok(batch(1, 1.0)),
                || Ok(()),
            ),
            Err(SearchArtifactError::ConcurrentMutation)
        ));
        assert!(!raced.path().join("embeddings").exists());
    }

    #[test]
    fn producer_failure_and_cancellation_preserve_prior_generation() {
        let dir = tempfile::tempdir().unwrap();
        let descriptor = descriptor("a");
        let recorded = source(10, 1, 1);
        refresh_embedding_generation(
            dir.path(),
            request(&descriptor),
            EmbeddingRefreshLimits::default(),
            || Ok(recorded),
            |_| Ok(batch(1, 1.0)),
            || Ok(()),
        )
        .unwrap();
        assert!(matches!(
            refresh_embedding_generation(
                dir.path(),
                request(&descriptor),
                EmbeddingRefreshLimits::default(),
                || Ok(source(11, 2, 1)),
                |_| Err(SearchArtifactError::Build("provider failed".into())),
                || Ok(()),
            ),
            Err(SearchArtifactError::Build(_))
        ));
        assert!(matches!(
            refresh_embedding_generation(
                dir.path(),
                request(&descriptor),
                EmbeddingRefreshLimits::default(),
                || Ok(source(11, 2, 1)),
                |_| Ok(batch(1, 2.0)),
                || Err(SearchArtifactError::Cancelled),
            ),
            Err(SearchArtifactError::Cancelled)
        ));
        let incompatible = descriptor_with_dimensions("incompatible", 3);
        assert!(matches!(
            refresh_embedding_generation(
                dir.path(),
                request(&incompatible),
                EmbeddingRefreshLimits::default(),
                || Ok(source(11, 2, 1)),
                |_| Ok(batch(1, 2.0)),
                || Ok(()),
            ),
            Err(SearchArtifactError::InvalidSelector { .. })
        ));
        assert!(matches!(
            prepare_embedding_read(
                dir.path(),
                &descriptor,
                recorded,
                false,
                EmbeddingReadLimits::default(),
                || Ok(())
            )
            .unwrap()
            .unwrap()
            .decision(),
            EmbeddingReadDecision::ServeFresh
        ));
    }

    #[test]
    fn journal_reset_failure_leaves_the_replacement_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let descriptor = descriptor("a");
        let recorded = source(10, 1, 1);
        refresh_embedding_generation(
            dir.path(),
            request(&descriptor),
            EmbeddingRefreshLimits::default(),
            || Ok(recorded),
            |_| Ok(batch(1, 1.0)),
            || Ok(()),
        )
        .unwrap();
        let limits = EmbeddingRefreshLimits {
            read: EmbeddingReadLimits {
                journal: EmbeddingMutationJournalLimits {
                    metadata_bytes: 1,
                    ..EmbeddingMutationJournalLimits::default()
                },
                ..EmbeddingReadLimits::default()
            },
            coordination: SearchCoordinationLimits::default(),
        };
        let replacement = source(11, 2, 1);
        assert!(matches!(
            refresh_embedding_generation(
                dir.path(),
                request(&descriptor),
                limits,
                || Ok(replacement),
                |_| Ok(batch(1, 2.0)),
                || Ok(()),
            ),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "embedding_mutation_journal_bytes",
                ..
            })
        ));
        assert!(matches!(
            prepare_embedding_read(
                dir.path(),
                &descriptor,
                replacement,
                false,
                EmbeddingReadLimits::default(),
                || Ok(())
            ),
            Err(SearchArtifactError::CorruptManifest { .. })
        ));
    }

    #[test]
    fn compatibility_lineages_refresh_independently() {
        let dir = tempfile::tempdir().unwrap();
        let first = descriptor("a");
        let second = descriptor("b");
        let current = source(10, 1, 1);
        let first_outcome = refresh_embedding_generation(
            dir.path(),
            request(&first),
            EmbeddingRefreshLimits::default(),
            || Ok(current),
            |_| Ok(batch(1, 1.0)),
            || Ok(()),
        )
        .unwrap();
        let second_outcome = refresh_embedding_generation(
            dir.path(),
            request(&second),
            EmbeddingRefreshLimits::default(),
            || Ok(current),
            |_| Ok(batch(1, 2.0)),
            || Ok(()),
        )
        .unwrap();
        assert_ne!(
            first_outcome.publication().manifest.compatibility_id(),
            second_outcome.publication().manifest.compatibility_id()
        );
        assert!(matches!(
            prepare_embedding_read(
                dir.path(),
                &first,
                current,
                false,
                EmbeddingReadLimits::default(),
                || Ok(())
            )
            .unwrap()
            .unwrap()
            .decision(),
            EmbeddingReadDecision::ServeFresh
        ));
    }

    #[test]
    fn lazy_refreshes_substantial_staleness_but_force_skips_producer() {
        let dir = tempfile::tempdir().unwrap();
        let descriptor = descriptor("a");
        let recorded = source(10, 1, 21);
        let initial = refresh_embedding_generation(
            dir.path(),
            request(&descriptor),
            EmbeddingRefreshLimits::default(),
            || Ok(recorded),
            |_| Ok(batch(21, 1.0)),
            || Ok(()),
        )
        .unwrap();
        let stale = source(11, 2, 21);
        merge_embedding_mutation_batch(
            dir.path(),
            &initial.publication().manifest,
            EmbeddingMutationBatch {
                current_source: stale,
                changed_uuids: &[UUID],
                structural_mutation: true,
                scope_proven: true,
            },
            EmbeddingMutationJournalLimits::default(),
            SearchCoordinationLimits::default(),
            || Ok(()),
        )
        .unwrap();
        let produced = Cell::new(0_u8);
        let forced = prepare_embedding_read_lazily(
            dir.path(),
            request(&descriptor),
            true,
            EmbeddingRefreshLimits::default(),
            || Ok(stale),
            |_| {
                produced.set(produced.get() + 1);
                Ok(batch(21, 2.0))
            },
            || Ok(()),
        )
        .unwrap();
        assert!(matches!(
            forced.decision(),
            EmbeddingReadDecision::ServeForcedStale { .. }
        ));
        assert_eq!(produced.get(), 0);
        let refreshed = prepare_embedding_read_lazily(
            dir.path(),
            request(&descriptor),
            false,
            EmbeddingRefreshLimits::default(),
            || Ok(stale),
            |_| {
                produced.set(produced.get() + 1);
                Ok(batch(21, 2.0))
            },
            || Ok(()),
        )
        .unwrap();
        assert!(matches!(
            refreshed.decision(),
            EmbeddingReadDecision::ServeFresh
        ));
        assert_eq!(produced.get(), 1);
    }
}
