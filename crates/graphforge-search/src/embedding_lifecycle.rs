//! Durable freshness gates for complete embedding-generation reads.

use std::path::Path;

use graphforge_storage::{
    EmbeddingCompatibilityDescriptor, EmbeddingFreshness, EmbeddingGenerationPublication,
    EmbeddingMutationJournalLimits, EmbeddingMutationObservation, EmbeddingReadDecision,
    EmbeddingSourceState, SearchArtifactError, VectorStoreLimits, classify_embedding_freshness,
    current_embedding_generation, decide_embedding_read, read_embedding_mutation_journal,
};

/// Resource limits for validated generation reopen and mutation evidence.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EmbeddingReadLimits {
    /// Primary vector validation and file-read bounds.
    pub vectors: VectorStoreLimits,
    /// Mutation journal metadata and distinct-UUID bounds.
    pub journal: EmbeddingMutationJournalLimits,
}

/// One fully verified generation plus its deterministic serving boundary.
#[derive(Clone, Debug, PartialEq)]
pub struct PreparedEmbeddingRead {
    publication: EmbeddingGenerationPublication,
    freshness: EmbeddingFreshness,
    decision: EmbeddingReadDecision,
}

impl PreparedEmbeddingRead {
    /// Fully validated active generation selected for the compatibility lineage.
    #[must_use]
    pub const fn publication(&self) -> &EmbeddingGenerationPublication {
        &self.publication
    }

    /// Durable freshness classification reconstructed for this read.
    #[must_use]
    pub const fn freshness(&self) -> EmbeddingFreshness {
        self.freshness
    }

    /// Serving boundary, including explicit forced-stale diagnostics.
    #[must_use]
    pub const fn decision(&self) -> EmbeddingReadDecision {
        self.decision
    }
}

/// Reopen one complete generation and enforce its durable freshness boundary.
///
/// The caller supplies the current source state captured under its graph-read
/// boundary. Exact journal evidence is trusted only when it describes that
/// same state. A missing or older journal is conservatively treated as
/// unproven mutation scope, so process restart or hook lag cannot manufacture
/// freshness. This function never refreshes or changes persisted bytes.
///
/// # Errors
/// Returns structured selector, corruption, incompatibility, source-regression,
/// resource, cancellation, or storage errors. Force cannot bypass these errors.
pub fn prepare_embedding_read<C>(
    project_dir: &Path,
    descriptor: &EmbeddingCompatibilityDescriptor,
    current_source: EmbeddingSourceState,
    force_stale: bool,
    limits: EmbeddingReadLimits,
    mut checkpoint: C,
) -> Result<Option<PreparedEmbeddingRead>, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    let Some(publication) =
        current_embedding_generation(project_dir, descriptor, limits.vectors, &mut checkpoint)?
    else {
        return Ok(None);
    };
    checkpoint()?;
    let journal =
        read_embedding_mutation_journal(project_dir, &publication.manifest, limits.journal)?;
    let observation = match journal {
        Some(journal) => reconcile_journal(journal.observation()?, current_source)?,
        None => unproven_observation(current_source)?,
    };
    let freshness = classify_embedding_freshness(&publication.manifest, observation)?;
    let decision = decide_embedding_read(&publication.manifest, freshness, force_stale);
    Ok(Some(PreparedEmbeddingRead {
        publication,
        freshness,
        decision,
    }))
}

fn reconcile_journal(
    durable: EmbeddingMutationObservation,
    current: EmbeddingSourceState,
) -> Result<EmbeddingMutationObservation, SearchArtifactError> {
    let durable_source = durable.current_source();
    match current
        .graph_generation()
        .cmp(&durable_source.graph_generation())
    {
        std::cmp::Ordering::Less => Err(invalid(
            "embedding current source",
            "caller source predates durable mutation evidence",
        )),
        std::cmp::Ordering::Equal if current != durable_source => Err(invalid(
            "embedding current source",
            "caller and journal disagree at the same graph generation",
        )),
        std::cmp::Ordering::Equal => Ok(durable),
        std::cmp::Ordering::Greater => unproven_observation(current),
    }
}

fn unproven_observation(
    current: EmbeddingSourceState,
) -> Result<EmbeddingMutationObservation, SearchArtifactError> {
    EmbeddingMutationObservation::new(current, 0, 0, false, false)
}

fn invalid(field: &'static str, reason: impl Into<String>) -> SearchArtifactError {
    SearchArtifactError::InvalidSelector {
        field,
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;
    use graphforge_storage::{
        EmbeddingBatchRow, EmbeddingCompatibilityInput, EmbeddingDistance, EmbeddingMutationBatch,
        EmbeddingNormalization, EmbeddingProducerIdentity, EmbeddingPublicationRequest,
        EmbeddingReadDecision, EmbeddingValueType, SearchCoordinationLimits,
        ValidatedEmbeddingBatch, merge_embedding_mutation_batch, publish_embedding_generation,
        reset_embedding_mutation_journal, validate_embedding_batch,
    };

    const UUID_A: [u8; 16] = [1; 16];

    fn descriptor(model: &str) -> EmbeddingCompatibilityDescriptor {
        EmbeddingCompatibilityDescriptor::new(EmbeddingCompatibilityInput {
            producer: EmbeddingProducerIdentity::Local {
                implementation: "test-adapter".to_owned(),
                model: model.to_owned(),
                revision: "r1".to_owned(),
                contract_version: "v1".to_owned(),
            },
            dimensions: 2,
            value_type: EmbeddingValueType::Float32,
            normalization: EmbeddingNormalization::None,
            distance: EmbeddingDistance::Cosine,
            tokenizer: None,
            chunking: None,
            hyperparameters: BTreeMap::new(),
            input_recipe: BTreeMap::from([("property".to_owned(), "body".into())]),
            source_projection_recipe: BTreeMap::from([("label".to_owned(), "Document".into())]),
        })
        .unwrap()
    }

    fn source(generation: u64, marker: u8, eligible: u64) -> EmbeddingSourceState {
        EmbeddingSourceState::new(generation, [marker; 32], [marker + 1; 32], eligible)
    }

    fn batch(count: u8) -> ValidatedEmbeddingBatch {
        let eligible = (1..=count)
            .map(|value| [value; 16])
            .collect::<BTreeSet<_>>();
        validate_embedding_batch(
            (1..=count)
                .map(|value| EmbeddingBatchRow {
                    node_uuid: [value; 16],
                    vector: vec![f32::from(value), 1.0],
                })
                .collect(),
            &eligible,
            2,
            EmbeddingNormalization::None,
            graphforge_storage::VectorStoreLimits::default(),
            || Ok(()),
        )
        .unwrap()
    }

    fn publish(
        project: &Path,
        descriptor: &EmbeddingCompatibilityDescriptor,
        source: EmbeddingSourceState,
        batch: &ValidatedEmbeddingBatch,
    ) -> EmbeddingGenerationPublication {
        publish_embedding_generation(
            project,
            EmbeddingPublicationRequest {
                descriptor,
                source,
                batch,
                generated_at_micros: 20,
                committed_at_micros: 21,
            },
            VectorStoreLimits::default(),
            SearchCoordinationLimits::default(),
            || Ok(()),
        )
        .unwrap()
        .publication()
        .clone()
    }

    fn reset(project: &Path, publication: &EmbeddingGenerationPublication) {
        reset_embedding_mutation_journal(
            project,
            &publication.manifest,
            EmbeddingMutationJournalLimits::default(),
            SearchCoordinationLimits::default(),
            || Ok(()),
        )
        .unwrap();
    }

    fn prepare(
        project: &Path,
        descriptor: &EmbeddingCompatibilityDescriptor,
        current: EmbeddingSourceState,
        force: bool,
    ) -> PreparedEmbeddingRead {
        prepare_embedding_read(
            project,
            descriptor,
            current,
            force,
            EmbeddingReadLimits::default(),
            || Ok(()),
        )
        .unwrap()
        .unwrap()
    }

    #[test]
    fn fresh_stale_substantial_and_forced_decisions_survive_reopen() {
        let project = tempfile::tempdir().unwrap();
        let descriptor = descriptor("model-a");
        let vectors = batch(21);
        let recorded = source(10, 1, 21);
        let publication = publish(project.path(), &descriptor, recorded, &vectors);
        reset(project.path(), &publication);

        let fresh = prepare(project.path(), &descriptor, recorded, false);
        assert!(matches!(
            fresh.decision(),
            EmbeddingReadDecision::ServeFresh
        ));

        let mildly_stale_source = source(11, 2, 21);
        merge_embedding_mutation_batch(
            project.path(),
            &publication.manifest,
            EmbeddingMutationBatch {
                current_source: mildly_stale_source,
                changed_uuids: &[UUID_A],
                structural_mutation: false,
                scope_proven: true,
            },
            EmbeddingMutationJournalLimits::default(),
            SearchCoordinationLimits::default(),
            || Ok(()),
        )
        .unwrap();
        let stale = prepare(project.path(), &descriptor, mildly_stale_source, false);
        assert!(matches!(
            stale.decision(),
            EmbeddingReadDecision::ServeStale { .. }
        ));

        let substantial_source = source(12, 3, 21);
        merge_embedding_mutation_batch(
            project.path(),
            &publication.manifest,
            EmbeddingMutationBatch {
                current_source: substantial_source,
                changed_uuids: &[],
                structural_mutation: true,
                scope_proven: true,
            },
            EmbeddingMutationJournalLimits::default(),
            SearchCoordinationLimits::default(),
            || Ok(()),
        )
        .unwrap();
        let blocked = prepare(project.path(), &descriptor, substantial_source, false);
        assert!(matches!(
            blocked.decision(),
            EmbeddingReadDecision::RefreshRequired { .. }
        ));
        let forced = prepare(project.path(), &descriptor, substantial_source, true);
        let EmbeddingReadDecision::ServeForcedStale { diagnostic } = forced.decision() else {
            panic!("expected an explicit forced-stale decision")
        };
        assert_eq!(
            diagnostic.generation_id,
            publication.manifest.generation_id()
        );
        assert_eq!(diagnostic.current_source, substantial_source.fingerprint());
    }

    #[test]
    fn missing_and_lagging_journals_are_unproven_and_substantial() {
        let project = tempfile::tempdir().unwrap();
        let descriptor = descriptor("model-a");
        let vectors = batch(1);
        let recorded = source(10, 1, 1);
        let publication = publish(project.path(), &descriptor, recorded, &vectors);

        let missing = prepare(project.path(), &descriptor, recorded, false);
        assert!(matches!(
            missing.decision(),
            EmbeddingReadDecision::RefreshRequired { .. }
        ));

        reset(project.path(), &publication);
        let newer = source(11, 2, 1);
        let lagging = prepare(project.path(), &descriptor, newer, false);
        assert!(matches!(
            lagging.decision(),
            EmbeddingReadDecision::RefreshRequired { .. }
        ));
    }

    #[test]
    fn source_regression_conflict_and_corruption_fail_closed() {
        let project = tempfile::tempdir().unwrap();
        let descriptor = descriptor("model-a");
        let vectors = batch(1);
        let recorded = source(10, 1, 1);
        let publication = publish(project.path(), &descriptor, recorded, &vectors);
        reset(project.path(), &publication);
        let current = source(11, 2, 1);
        merge_embedding_mutation_batch(
            project.path(),
            &publication.manifest,
            EmbeddingMutationBatch {
                current_source: current,
                changed_uuids: &[UUID_A],
                structural_mutation: false,
                scope_proven: true,
            },
            EmbeddingMutationJournalLimits::default(),
            SearchCoordinationLimits::default(),
            || Ok(()),
        )
        .unwrap();

        for invalid_source in [recorded, source(11, 9, 1)] {
            assert!(matches!(
                prepare_embedding_read(
                    project.path(),
                    &descriptor,
                    invalid_source,
                    true,
                    EmbeddingReadLimits::default(),
                    || Ok(()),
                ),
                Err(SearchArtifactError::InvalidSelector { .. })
            ));
        }

        let journal = project
            .path()
            .join("embeddings/spaces")
            .join(descriptor.compatibility_id().unwrap().to_hex())
            .join("mutations.json");
        std::fs::write(journal, b"corrupt").unwrap();
        assert!(matches!(
            prepare_embedding_read(
                project.path(),
                &descriptor,
                current,
                true,
                EmbeddingReadLimits::default(),
                || Ok(()),
            ),
            Err(SearchArtifactError::CorruptManifest { .. })
        ));
    }

    #[test]
    fn cancellation_is_read_only_and_lineages_remain_independent() {
        let project = tempfile::tempdir().unwrap();
        let left_descriptor = descriptor("model-a");
        let right_descriptor = descriptor("model-b");
        let vectors = batch(1);
        let left_source = source(10, 1, 1);
        let right_source = source(20, 2, 1);
        let left = publish(project.path(), &left_descriptor, left_source, &vectors);
        let right = publish(project.path(), &right_descriptor, right_source, &vectors);
        reset(project.path(), &left);
        reset(project.path(), &right);
        let left_journal = left
            .path
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("mutations.json");
        let right_journal = right
            .path
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("mutations.json");
        let left_before = std::fs::read(&left_journal).unwrap();
        let right_before = std::fs::read(&right_journal).unwrap();

        assert!(matches!(
            prepare_embedding_read(
                project.path(),
                &left_descriptor,
                left_source,
                false,
                EmbeddingReadLimits::default(),
                || Err(SearchArtifactError::Cancelled),
            ),
            Err(SearchArtifactError::Cancelled)
        ));
        assert_eq!(std::fs::read(&left_journal).unwrap(), left_before);
        assert_eq!(std::fs::read(&right_journal).unwrap(), right_before);
        assert!(matches!(
            prepare(project.path(), &right_descriptor, right_source, false).decision(),
            EmbeddingReadDecision::ServeFresh
        ));
    }
}
