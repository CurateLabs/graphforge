//! Provider-neutral driver from proactive scheduler leases to atomic refresh.

use std::path::Path;
use std::time::Duration;

use gf_storage::{
    EmbeddingCompatibilityDescriptor, EmbeddingPublicationOutcome, EmbeddingSourceState,
    SearchArtifactError, ValidatedEmbeddingBatch,
};

use crate::{
    EmbeddingRefreshCompletion, EmbeddingRefreshLease, EmbeddingRefreshLimits,
    EmbeddingRefreshRequest, EmbeddingRefreshScheduler, refresh_embedding_generation,
};

/// Owned lineage metadata resolved for one scheduled refresh lease.
#[derive(Clone, Debug)]
pub struct ProactiveEmbeddingRefreshRequest {
    /// Exact versioned compatibility descriptor for the leased lineage.
    pub descriptor: EmbeddingCompatibilityDescriptor,
    /// Producer completion time in UTC microseconds since Unix epoch.
    pub generated_at_micros: i64,
    /// Durable publication time in UTC microseconds since Unix epoch.
    pub committed_at_micros: i64,
}

/// One completed proactive lease and its atomic publication result.
#[derive(Clone, Debug)]
pub struct ProactiveEmbeddingRefreshOutcome {
    lease: EmbeddingRefreshLease,
    publication: EmbeddingPublicationOutcome,
}

impl ProactiveEmbeddingRefreshOutcome {
    /// Exact scheduler lease completed by this invocation.
    #[must_use]
    pub const fn lease(&self) -> EmbeddingRefreshLease {
        self.lease
    }

    /// Complete published or content-idempotently reused generation.
    #[must_use]
    pub const fn publication(&self) -> &EmbeddingPublicationOutcome {
        &self.publication
    }
}

/// Claim and drive at most one ready proactive embedding refresh.
///
/// The host resolves provider-neutral lineage metadata and supplies source and
/// producer callbacks for the exact lease. This driver validates compatibility
/// and source progression, delegates only through the bounded complete refresh
/// boundary, and releases the lease with one deterministic terminal class.
/// Returning Ok(None) means no work was ready.
///
/// # Errors
/// Propagates scheduler-claim, descriptor, source, producer, cancellation,
/// concurrent-mutation, validation, publication, and storage errors. Once a
/// lease is claimed, terminal scheduler bookkeeping is always attempted before
/// the original work result is returned; a bookkeeping error never replaces a
/// completed publication or the original work error.
#[allow(clippy::too_many_arguments)]
pub fn drive_next_embedding_refresh<R, S, P, C>(
    project_dir: &Path,
    scheduler: &mut EmbeddingRefreshScheduler,
    now: Duration,
    limits: EmbeddingRefreshLimits,
    mut resolve: R,
    mut capture_source: S,
    mut produce: P,
    mut checkpoint: C,
) -> Result<Option<ProactiveEmbeddingRefreshOutcome>, SearchArtifactError>
where
    R: FnMut(
        EmbeddingRefreshLease,
    ) -> Result<ProactiveEmbeddingRefreshRequest, SearchArtifactError>,
    S: FnMut(EmbeddingRefreshLease) -> Result<EmbeddingSourceState, SearchArtifactError>,
    P: FnMut(
        EmbeddingRefreshLease,
        EmbeddingSourceState,
    ) -> Result<ValidatedEmbeddingBatch, SearchArtifactError>,
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    let Some(lease) = scheduler.claim_ready(now, &mut checkpoint)? else {
        return Ok(None);
    };

    let result = (|| {
        checkpoint()?;
        let resolved = resolve(lease)?;
        let compatibility_id = resolved.descriptor.compatibility_id()?;
        if compatibility_id != lease.compatibility_id() {
            return Err(invalid(
                "proactive embedding descriptor",
                "compatibility identity does not match the scheduler lease",
            ));
        }
        let request = EmbeddingRefreshRequest {
            descriptor: &resolved.descriptor,
            generated_at_micros: resolved.generated_at_micros,
            committed_at_micros: resolved.committed_at_micros,
        };
        refresh_embedding_generation(
            project_dir,
            request,
            limits,
            || {
                let current = capture_source(lease)?;
                validate_source_progress(lease.source(), current)?;
                Ok(current)
            },
            |source| produce(lease, source),
            &mut checkpoint,
        )
    })();

    let completion = match &result {
        Ok(_) => EmbeddingRefreshCompletion::Succeeded,
        Err(SearchArtifactError::Cancelled) => EmbeddingRefreshCompletion::Cancelled,
        Err(_) => EmbeddingRefreshCompletion::Failed,
    };
    let completion_result = scheduler.complete(lease, completion, || Ok(()));

    preserve_refresh_result(result, completion_result)
        .map(|publication| Some(ProactiveEmbeddingRefreshOutcome { lease, publication }))
}

fn preserve_refresh_result<T>(
    work_result: Result<T, SearchArtifactError>,
    completion_result: Result<(), SearchArtifactError>,
) -> Result<T, SearchArtifactError> {
    drop(completion_result);
    work_result
}

fn validate_source_progress(
    leased: EmbeddingSourceState,
    current: EmbeddingSourceState,
) -> Result<(), SearchArtifactError> {
    match current.graph_generation().cmp(&leased.graph_generation()) {
        std::cmp::Ordering::Less => Err(invalid(
            "proactive embedding source",
            "current graph generation predates the scheduler lease",
        )),
        std::cmp::Ordering::Equal if current != leased => Err(invalid(
            "proactive embedding source",
            "current source conflicts with the scheduler lease at the same graph generation",
        )),
        std::cmp::Ordering::Equal | std::cmp::Ordering::Greater => Ok(()),
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
    use std::cell::{Cell, RefCell};
    use std::collections::{BTreeMap, BTreeSet};

    use gf_storage::{
        EmbeddingBatchRow, EmbeddingCompatibilityInput, EmbeddingDistance, EmbeddingNormalization,
        EmbeddingProducerIdentity, EmbeddingValueType, VectorStoreLimits,
        current_embedding_generation, validate_embedding_batch,
    };

    use crate::EmbeddingSchedulerLimits;

    use super::*;

    const UUID: [u8; 16] = [1; 16];

    fn descriptor(model: &str, dimensions: u32) -> EmbeddingCompatibilityDescriptor {
        EmbeddingCompatibilityDescriptor::new(EmbeddingCompatibilityInput {
            producer: EmbeddingProducerIdentity::Local {
                implementation: "proactive-test".to_owned(),
                model: model.to_owned(),
                revision: "r1".to_owned(),
                contract_version: "v1".to_owned(),
            },
            dimensions,
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

    fn source(generation: u64, marker: u8) -> EmbeddingSourceState {
        EmbeddingSourceState::new(generation, [marker; 32], [marker + 1; 32], 1)
    }

    fn batch(dimensions: usize, marker: f32) -> ValidatedEmbeddingBatch {
        validate_embedding_batch(
            vec![EmbeddingBatchRow {
                node_uuid: UUID,
                vector: (0..dimensions).map(|index| marker + index as f32).collect(),
            }],
            &BTreeSet::from([UUID]),
            dimensions,
            EmbeddingNormalization::None,
            VectorStoreLimits::default(),
            || Ok(()),
        )
        .unwrap()
    }

    fn scheduler() -> EmbeddingRefreshScheduler {
        EmbeddingRefreshScheduler::new(EmbeddingSchedulerLimits {
            debounce: Duration::from_millis(1),
            ..EmbeddingSchedulerLimits::default()
        })
        .unwrap()
    }

    #[test]
    fn completion_errors_never_replace_the_authoritative_refresh_result() {
        let publication = preserve_refresh_result(
            Ok(7_u8),
            Err(SearchArtifactError::Build("bookkeeping failed".to_owned())),
        )
        .unwrap();
        assert_eq!(publication, 7);

        let error = preserve_refresh_result::<u8>(
            Err(SearchArtifactError::Cancelled),
            Err(SearchArtifactError::Build("bookkeeping failed".to_owned())),
        )
        .unwrap_err();
        assert!(matches!(error, SearchArtifactError::Cancelled));
    }

    fn request(descriptor: &EmbeddingCompatibilityDescriptor) -> ProactiveEmbeddingRefreshRequest {
        ProactiveEmbeddingRefreshRequest {
            descriptor: descriptor.clone(),
            generated_at_micros: 20,
            committed_at_micros: 21,
        }
    }

    #[test]
    fn no_ready_work_is_a_deterministic_noop() {
        let dir = tempfile::tempdir().unwrap();
        let descriptor = descriptor("a", 2);
        let mut scheduler = scheduler();
        let outcome = drive_next_embedding_refresh(
            dir.path(),
            &mut scheduler,
            Duration::ZERO,
            EmbeddingRefreshLimits::default(),
            |_| Ok(request(&descriptor)),
            |_| Ok(source(1, 1)),
            |_, _| Ok(batch(2, 1.0)),
            || Ok(()),
        )
        .unwrap();
        assert!(outcome.is_none());
        assert_eq!(scheduler.snapshot().unwrap().succeeded, 0);
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
    fn coalesced_notice_publishes_newest_complete_generation() {
        let dir = tempfile::tempdir().unwrap();
        let descriptor = descriptor("a", 2);
        let compatibility_id = descriptor.compatibility_id().unwrap();
        let mut scheduler = scheduler();
        scheduler
            .enqueue(compatibility_id, source(10, 1), Duration::ZERO, || Ok(()))
            .unwrap();
        scheduler
            .enqueue(
                compatibility_id,
                source(11, 2),
                Duration::from_millis(1),
                || Ok(()),
            )
            .unwrap();
        let captures = Cell::new(0);
        let produced = Cell::new(0);
        let outcome = drive_next_embedding_refresh(
            dir.path(),
            &mut scheduler,
            Duration::from_millis(2),
            EmbeddingRefreshLimits::default(),
            |lease| {
                assert_eq!(lease.source(), source(11, 2));
                Ok(request(&descriptor))
            },
            |lease| {
                captures.set(captures.get() + 1);
                Ok(lease.source())
            },
            |lease, observed| {
                produced.set(produced.get() + 1);
                assert_eq!(lease.source(), observed);
                Ok(batch(2, 1.0))
            },
            || Ok(()),
        )
        .unwrap()
        .unwrap();
        assert_eq!(outcome.lease().source(), source(11, 2));
        assert_eq!(
            outcome.publication().publication().manifest.source(),
            source(11, 2)
        );
        assert_eq!(captures.get(), 2);
        assert_eq!(produced.get(), 1);
        let snapshot = scheduler.snapshot().unwrap();
        assert_eq!(snapshot.coalesced_notices, 1);
        assert_eq!(snapshot.succeeded, 1);
        assert!(snapshot.queued.is_empty());
        assert!(snapshot.in_flight.is_empty());
    }

    #[test]
    fn descriptor_and_source_mismatches_release_failed_leases_before_production() {
        let dir = tempfile::tempdir().unwrap();
        let descriptor_a = descriptor("a", 2);
        let descriptor_b = descriptor("b", 2);
        let compatibility_id = descriptor_a.compatibility_id().unwrap();
        let produced = Cell::new(false);

        for current in [source(9, 1), source(10, 2)] {
            let mut scheduler = scheduler();
            scheduler
                .enqueue(compatibility_id, source(10, 1), Duration::ZERO, || Ok(()))
                .unwrap();
            let error = drive_next_embedding_refresh(
                dir.path(),
                &mut scheduler,
                Duration::from_millis(1),
                EmbeddingRefreshLimits::default(),
                |_| Ok(request(&descriptor_a)),
                |_| Ok(current),
                |_, _| {
                    produced.set(true);
                    Ok(batch(2, 1.0))
                },
                || Ok(()),
            )
            .unwrap_err();
            assert!(matches!(error, SearchArtifactError::InvalidSelector { .. }));
            let snapshot = scheduler.snapshot().unwrap();
            assert_eq!(snapshot.failed, 1);
            assert!(snapshot.in_flight.is_empty());
        }

        let mut scheduler = scheduler();
        scheduler
            .enqueue(compatibility_id, source(10, 1), Duration::ZERO, || Ok(()))
            .unwrap();
        let error = drive_next_embedding_refresh(
            dir.path(),
            &mut scheduler,
            Duration::from_millis(1),
            EmbeddingRefreshLimits::default(),
            |_| Ok(request(&descriptor_b)),
            |_| Ok(source(10, 1)),
            |_, _| {
                produced.set(true);
                Ok(batch(2, 1.0))
            },
            || Ok(()),
        )
        .unwrap_err();
        assert!(matches!(error, SearchArtifactError::InvalidSelector { .. }));
        assert!(!produced.get());
        assert_eq!(scheduler.snapshot().unwrap().failed, 1);
    }

    #[test]
    fn failures_and_cancellation_preserve_the_prior_complete_generation() {
        let dir = tempfile::tempdir().unwrap();
        let descriptor = descriptor("a", 2);
        let compatibility_id = descriptor.compatibility_id().unwrap();
        let mut scheduler = scheduler();
        scheduler
            .enqueue(compatibility_id, source(10, 1), Duration::ZERO, || Ok(()))
            .unwrap();
        drive_next_embedding_refresh(
            dir.path(),
            &mut scheduler,
            Duration::from_millis(1),
            EmbeddingRefreshLimits::default(),
            |_| Ok(request(&descriptor)),
            |_| Ok(source(10, 1)),
            |_, _| Ok(batch(2, 1.0)),
            || Ok(()),
        )
        .unwrap()
        .unwrap();
        let prior = current_embedding_generation(
            dir.path(),
            &descriptor,
            VectorStoreLimits::default(),
            || Ok(()),
        )
        .unwrap()
        .unwrap();

        scheduler
            .enqueue(
                compatibility_id,
                source(11, 2),
                Duration::from_millis(2),
                || Ok(()),
            )
            .unwrap();
        let error = drive_next_embedding_refresh(
            dir.path(),
            &mut scheduler,
            Duration::from_millis(3),
            EmbeddingRefreshLimits::default(),
            |_| Ok(request(&descriptor)),
            |_| Ok(source(11, 2)),
            |_, _| Err(SearchArtifactError::Build("provider failed".to_owned())),
            || Ok(()),
        )
        .unwrap_err();
        assert!(matches!(error, SearchArtifactError::Build(_)));

        scheduler
            .enqueue(
                compatibility_id,
                source(12, 3),
                Duration::from_millis(4),
                || Ok(()),
            )
            .unwrap();
        let error = drive_next_embedding_refresh(
            dir.path(),
            &mut scheduler,
            Duration::from_millis(5),
            EmbeddingRefreshLimits::default(),
            |_| Ok(request(&descriptor)),
            |_| Ok(source(12, 3)),
            |_, _| Err(SearchArtifactError::Cancelled),
            || Ok(()),
        )
        .unwrap_err();
        assert!(matches!(error, SearchArtifactError::Cancelled));

        scheduler
            .enqueue(
                compatibility_id,
                source(13, 4),
                Duration::from_millis(6),
                || Ok(()),
            )
            .unwrap();
        let changing =
            RefCell::new([source(13, 4), source(14, 5), source(15, 6), source(16, 7)].into_iter());
        let error = drive_next_embedding_refresh(
            dir.path(),
            &mut scheduler,
            Duration::from_millis(7),
            EmbeddingRefreshLimits::default(),
            |_| Ok(request(&descriptor)),
            |_| Ok(changing.borrow_mut().next().unwrap()),
            |_, _| Ok(batch(2, 2.0)),
            || Ok(()),
        )
        .unwrap_err();
        assert!(matches!(error, SearchArtifactError::ConcurrentMutation));

        let active = current_embedding_generation(
            dir.path(),
            &descriptor,
            VectorStoreLimits::default(),
            || Ok(()),
        )
        .unwrap()
        .unwrap();
        assert_eq!(active, prior);
        let snapshot = scheduler.snapshot().unwrap();
        assert_eq!(snapshot.succeeded, 1);
        assert_eq!(snapshot.failed, 2);
        assert_eq!(snapshot.cancelled, 1);
        assert!(snapshot.in_flight.is_empty());
    }

    #[test]
    fn independent_lineages_route_and_publish_without_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let descriptor_a = descriptor("a", 2);
        let descriptor_b = descriptor("b", 3);
        let id_a = descriptor_a.compatibility_id().unwrap();
        let id_b = descriptor_b.compatibility_id().unwrap();
        let mut scheduler = scheduler();
        scheduler
            .enqueue(id_b, source(20, 2), Duration::ZERO, || Ok(()))
            .unwrap();
        scheduler
            .enqueue(id_a, source(10, 1), Duration::ZERO, || Ok(()))
            .unwrap();

        for _ in 0..2 {
            drive_next_embedding_refresh(
                dir.path(),
                &mut scheduler,
                Duration::from_millis(1),
                EmbeddingRefreshLimits::default(),
                |lease| {
                    if lease.compatibility_id() == id_a {
                        Ok(request(&descriptor_a))
                    } else {
                        Ok(request(&descriptor_b))
                    }
                },
                |lease| Ok(lease.source()),
                |lease, _| {
                    if lease.compatibility_id() == id_a {
                        Ok(batch(2, 1.0))
                    } else {
                        Ok(batch(3, 2.0))
                    }
                },
                || Ok(()),
            )
            .unwrap()
            .unwrap();
        }

        let active_a = current_embedding_generation(
            dir.path(),
            &descriptor_a,
            VectorStoreLimits::default(),
            || Ok(()),
        )
        .unwrap()
        .unwrap();
        let active_b = current_embedding_generation(
            dir.path(),
            &descriptor_b,
            VectorStoreLimits::default(),
            || Ok(()),
        )
        .unwrap()
        .unwrap();
        assert_eq!(active_a.manifest.dimension(), 2);
        assert_eq!(active_b.manifest.dimension(), 3);
        assert_ne!(active_a.path, active_b.path);
        assert_eq!(scheduler.snapshot().unwrap().succeeded, 2);
    }
}
