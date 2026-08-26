//! Bounded process-local scheduling for proactive embedding refresh.

use std::collections::BTreeMap;
use std::time::Duration;

use graphforge_storage::{EmbeddingCompatibilityId, EmbeddingSourceState, SearchArtifactError};

/// Resource and debounce bounds for one process-local scheduler.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EmbeddingSchedulerLimits {
    /// Maximum queued or coalesced follow-up lineages.
    pub queued_lineages: usize,
    /// Maximum disjoint lineages leased at once.
    pub in_flight_lineages: usize,
    /// Delay after the newest notice before a lineage becomes ready.
    pub debounce: Duration,
    /// Maximum entries inspected by one deterministic claim or snapshot.
    pub inspected_entries: usize,
}

impl Default for EmbeddingSchedulerLimits {
    fn default() -> Self {
        Self {
            queued_lineages: 1_024,
            in_flight_lineages: 2,
            debounce: Duration::from_millis(250),
            inspected_entries: 2_048,
        }
    }
}

/// Stable scheduler lifecycle state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EmbeddingSchedulerState {
    /// Notices and leases are accepted.
    Running,
    /// New work is rejected and queued work has been discarded.
    Shutdown,
}

/// Content-free terminal class reported by the refresh driver.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EmbeddingRefreshCompletion {
    /// One complete generation refresh succeeded or reused exact content.
    Succeeded,
    /// A provider, validation, publication, or other refresh operation failed.
    Failed,
    /// Cooperative cancellation stopped private work.
    Cancelled,
}

/// One deterministic lease for a complete lineage refresh.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EmbeddingRefreshLease {
    lease_id: u64,
    compatibility_id: EmbeddingCompatibilityId,
    source: EmbeddingSourceState,
}

impl EmbeddingRefreshLease {
    /// Monotonic process-local lease identity.
    #[must_use]
    pub const fn lease_id(self) -> u64 {
        self.lease_id
    }

    /// Exact compatibility lineage selected for refresh.
    #[must_use]
    pub const fn compatibility_id(self) -> EmbeddingCompatibilityId {
        self.compatibility_id
    }

    /// Newest source state coalesced into this lease.
    #[must_use]
    pub const fn source(self) -> EmbeddingSourceState {
        self.source
    }
}

/// One queued lineage visible through bounded scheduler inspection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScheduledEmbeddingRefresh {
    /// Exact compatibility lineage.
    pub compatibility_id: EmbeddingCompatibilityId,
    /// Newest coalesced source state.
    pub source: EmbeddingSourceState,
    /// Caller-supplied monotonic deadline.
    pub ready_at: Duration,
}

/// Bounded, deterministic scheduler inspection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmbeddingSchedulerSnapshot {
    /// Current scheduler lifecycle.
    pub state: EmbeddingSchedulerState,
    /// Deadline/identity-ordered queued work.
    pub queued: Vec<ScheduledEmbeddingRefresh>,
    /// Identity-ordered active leases.
    pub in_flight: Vec<EmbeddingRefreshLease>,
    /// Repeated notices folded into existing work.
    pub coalesced_notices: u64,
    /// Successful completed leases.
    pub succeeded: u64,
    /// Failed completed leases.
    pub failed: u64,
    /// Cancelled completed leases.
    pub cancelled: u64,
}

#[derive(Clone, Copy, Debug)]
struct QueuedRefresh {
    source: EmbeddingSourceState,
    ready_at: Duration,
}

#[derive(Clone, Copy, Debug)]
struct ActiveRefresh {
    lease: EmbeddingRefreshLease,
    follow_up: Option<QueuedRefresh>,
}

/// Provider-neutral queue and lease state for proactive embedding refresh.
///
/// This type owns no thread, runtime, provider, or durable bytes. A host records
/// mutation notices, claims ready leases, delegates each lease to the complete
/// refresh boundary, and reports only a bounded terminal class.
#[derive(Debug)]
pub struct EmbeddingRefreshScheduler {
    limits: EmbeddingSchedulerLimits,
    state: EmbeddingSchedulerState,
    queued: BTreeMap<EmbeddingCompatibilityId, QueuedRefresh>,
    active: BTreeMap<EmbeddingCompatibilityId, ActiveRefresh>,
    next_lease_id: u64,
    coalesced_notices: u64,
    succeeded: u64,
    failed: u64,
    cancelled: u64,
}

impl EmbeddingRefreshScheduler {
    /// Create an empty process-local scheduler.
    ///
    /// # Errors
    /// Rejects zero limits, zero debounce, or an inspection limit too small to
    /// cover every bounded queue and lease entry.
    pub fn new(limits: EmbeddingSchedulerLimits) -> Result<Self, SearchArtifactError> {
        if limits.queued_lineages == 0 {
            return Err(invalid(
                "embedding scheduler queued_lineages",
                "must be non-zero",
            ));
        }
        if limits.in_flight_lineages == 0 {
            return Err(invalid(
                "embedding scheduler in_flight_lineages",
                "must be non-zero",
            ));
        }
        if limits.debounce.is_zero() {
            return Err(invalid("embedding scheduler debounce", "must be non-zero"));
        }
        let required_inspection = limits
            .queued_lineages
            .checked_add(limits.in_flight_lineages)
            .ok_or_else(|| exhausted("embedding_scheduler_inspected_entries", usize::MAX))?;
        if limits.inspected_entries < required_inspection {
            return Err(invalid(
                "embedding scheduler inspected_entries",
                "must cover the bounded queue and in-flight sets",
            ));
        }
        Ok(Self {
            limits,
            state: EmbeddingSchedulerState::Running,
            queued: BTreeMap::new(),
            active: BTreeMap::new(),
            next_lease_id: 1,
            coalesced_notices: 0,
            succeeded: 0,
            failed: 0,
            cancelled: 0,
        })
    }

    /// Coalesce one relevant mutation notice into bounded proactive work.
    ///
    /// The caller supplies a monotonic process-local time. Repeated compatible
    /// notices reset the deadline; newer sources replace older pending work.
    ///
    /// # Errors
    /// Rejects shutdown, cancellation, queue exhaustion, time overflow, source
    /// regression, and same-generation source conflicts.
    pub fn enqueue<C>(
        &mut self,
        compatibility_id: EmbeddingCompatibilityId,
        source: EmbeddingSourceState,
        now: Duration,
        checkpoint: C,
    ) -> Result<(), SearchArtifactError>
    where
        C: FnMut() -> Result<(), SearchArtifactError>,
    {
        self.enqueue_with_debounce(
            compatibility_id,
            source,
            now,
            self.limits.debounce,
            checkpoint,
        )
    }

    /// Coalesce one relevant mutation using the lineage's resolved debounce.
    ///
    /// # Errors
    /// As [`Self::enqueue`], and rejects a zero lineage debounce.
    pub fn enqueue_with_debounce<C>(
        &mut self,
        compatibility_id: EmbeddingCompatibilityId,
        source: EmbeddingSourceState,
        now: Duration,
        debounce: Duration,
        mut checkpoint: C,
    ) -> Result<(), SearchArtifactError>
    where
        C: FnMut() -> Result<(), SearchArtifactError>,
    {
        checkpoint()?;
        self.ensure_running()?;
        if debounce.is_zero() {
            return Err(invalid(
                "embedding scheduler lineage debounce",
                "must be non-zero",
            ));
        }
        let ready_at = now
            .checked_add(debounce)
            .ok_or_else(|| exhausted("embedding_scheduler_deadline", usize::MAX))?;
        let pending_count = self.pending_count();

        if let Some(active) = self.active.get_mut(&compatibility_id) {
            let prior = active
                .follow_up
                .map_or(active.lease.source, |queued| queued.source);
            validate_source_progress(prior, source)?;
            let coalesced_notices = next_counter(
                self.coalesced_notices,
                "embedding_scheduler_coalesced_notices",
            )?;
            if active.follow_up.is_none() && source == active.lease.source {
                self.coalesced_notices = coalesced_notices;
                return Ok(());
            }
            if active.follow_up.is_none() && pending_count >= self.limits.queued_lineages {
                return Err(exhausted(
                    "embedding_scheduler_queued_lineages",
                    self.limits.queued_lineages,
                ));
            }
            active.follow_up = Some(QueuedRefresh { source, ready_at });
            self.coalesced_notices = coalesced_notices;
            return Ok(());
        }

        if let Some(queued) = self.queued.get_mut(&compatibility_id) {
            validate_source_progress(queued.source, source)?;
            let coalesced_notices = next_counter(
                self.coalesced_notices,
                "embedding_scheduler_coalesced_notices",
            )?;
            *queued = QueuedRefresh { source, ready_at };
            self.coalesced_notices = coalesced_notices;
            return Ok(());
        }

        if pending_count >= self.limits.queued_lineages {
            return Err(exhausted(
                "embedding_scheduler_queued_lineages",
                self.limits.queued_lineages,
            ));
        }
        self.queued
            .insert(compatibility_id, QueuedRefresh { source, ready_at });
        Ok(())
    }

    /// Lease the next deadline/identity-ordered ready lineage.
    ///
    /// # Errors
    /// Rejects shutdown, cancellation, lease-id exhaustion, or an impossible
    /// inspection bound. Returns `Ok(None)` when nothing is ready or all
    /// concurrency slots are occupied.
    pub fn claim_ready<C>(
        &mut self,
        now: Duration,
        mut checkpoint: C,
    ) -> Result<Option<EmbeddingRefreshLease>, SearchArtifactError>
    where
        C: FnMut() -> Result<(), SearchArtifactError>,
    {
        checkpoint()?;
        self.ensure_running()?;
        if self.active.len() >= self.limits.in_flight_lineages {
            return Ok(None);
        }
        self.ensure_inspectable()?;
        let selected = self
            .queued
            .iter()
            .filter(|(_, queued)| queued.ready_at <= now)
            .min_by_key(|(compatibility_id, queued)| (queued.ready_at, **compatibility_id))
            .map(|(compatibility_id, _)| *compatibility_id);
        let Some(compatibility_id) = selected else {
            return Ok(None);
        };
        let next_lease_id = self
            .next_lease_id
            .checked_add(1)
            .ok_or_else(|| exhausted("embedding_scheduler_lease_ids", usize::MAX))?;
        let queued = self
            .queued
            .remove(&compatibility_id)
            .expect("selected queued lineage must remain present");
        let lease = EmbeddingRefreshLease {
            lease_id: self.next_lease_id,
            compatibility_id,
            source: queued.source,
        };
        self.next_lease_id = next_lease_id;
        self.active.insert(
            compatibility_id,
            ActiveRefresh {
                lease,
                follow_up: None,
            },
        );
        Ok(Some(lease))
    }

    /// Release one exact active lease and retain any coalesced follow-up.
    ///
    /// # Errors
    /// Rejects cancellation, an unknown/stale lease, or counter exhaustion.
    pub fn complete<C>(
        &mut self,
        lease: EmbeddingRefreshLease,
        completion: EmbeddingRefreshCompletion,
        checkpoint: C,
    ) -> Result<(), SearchArtifactError>
    where
        C: FnMut() -> Result<(), SearchArtifactError>,
    {
        let covered_source =
            (completion == EmbeddingRefreshCompletion::Succeeded).then_some(lease.source);
        self.complete_through(lease, completion, covered_source, checkpoint)
    }

    /// Release one exact active lease and acknowledge the newest source covered
    /// by a successful refresh.
    ///
    /// A provider may publish a source newer than the source originally leased
    /// because mutations can coalesce while provider work is in flight. Any
    /// follow-up already covered by that exact published source is discarded;
    /// genuinely newer work remains queued. Failed and cancelled leases do not
    /// claim source coverage and retain their follow-up.
    ///
    /// # Errors
    /// As [`Self::complete`], and rejects missing/regressed/conflicting success
    /// coverage or coverage attached to a non-success completion.
    pub fn complete_through<C>(
        &mut self,
        lease: EmbeddingRefreshLease,
        completion: EmbeddingRefreshCompletion,
        covered_source: Option<EmbeddingSourceState>,
        mut checkpoint: C,
    ) -> Result<(), SearchArtifactError>
    where
        C: FnMut() -> Result<(), SearchArtifactError>,
    {
        checkpoint()?;
        let Some(active) = self.active.get(&lease.compatibility_id) else {
            return Err(invalid("embedding scheduler lease", "is not active"));
        };
        if active.lease != lease {
            return Err(invalid(
                "embedding scheduler lease",
                "does not match the active lease",
            ));
        }
        let retained_follow_up = match (completion, covered_source) {
            (EmbeddingRefreshCompletion::Succeeded, Some(covered)) => {
                validate_source_progress(lease.source, covered)?;
                active
                    .follow_up
                    .map(|follow_up| retain_follow_up_after_coverage(follow_up, covered))
                    .transpose()?
                    .flatten()
            }
            (EmbeddingRefreshCompletion::Succeeded, None) => {
                return Err(invalid(
                    "embedding scheduler completion coverage",
                    "is required for success",
                ));
            }
            (EmbeddingRefreshCompletion::Failed | EmbeddingRefreshCompletion::Cancelled, None) => {
                active.follow_up
            }
            (
                EmbeddingRefreshCompletion::Failed | EmbeddingRefreshCompletion::Cancelled,
                Some(_),
            ) => {
                return Err(invalid(
                    "embedding scheduler completion coverage",
                    "is only valid for success",
                ));
            }
        };
        let completed = match completion {
            EmbeddingRefreshCompletion::Succeeded => {
                next_counter(self.succeeded, "embedding_scheduler_succeeded")?
            }
            EmbeddingRefreshCompletion::Failed => {
                next_counter(self.failed, "embedding_scheduler_failed")?
            }
            EmbeddingRefreshCompletion::Cancelled => {
                next_counter(self.cancelled, "embedding_scheduler_cancelled")?
            }
        };
        self.active
            .remove(&lease.compatibility_id)
            .expect("validated active lease must remain present");
        match completion {
            EmbeddingRefreshCompletion::Succeeded => self.succeeded = completed,
            EmbeddingRefreshCompletion::Failed => self.failed = completed,
            EmbeddingRefreshCompletion::Cancelled => self.cancelled = completed,
        }
        if self.state == EmbeddingSchedulerState::Running
            && let Some(follow_up) = retained_follow_up
        {
            self.queued.insert(lease.compatibility_id, follow_up);
        }
        Ok(())
    }

    /// Stop accepting or leasing work and discard every queued follow-up.
    pub fn shutdown(&mut self) {
        self.state = EmbeddingSchedulerState::Shutdown;
        self.queued.clear();
        for active in self.active.values_mut() {
            active.follow_up = None;
        }
    }

    /// Return bounded content-free scheduler state.
    ///
    /// # Errors
    /// Rejects an impossible inspection bound.
    pub fn snapshot(&self) -> Result<EmbeddingSchedulerSnapshot, SearchArtifactError> {
        self.ensure_inspectable()?;
        let mut queued = self
            .queued
            .iter()
            .map(|(compatibility_id, queued)| ScheduledEmbeddingRefresh {
                compatibility_id: *compatibility_id,
                source: queued.source,
                ready_at: queued.ready_at,
            })
            .collect::<Vec<_>>();
        for (compatibility_id, active) in &self.active {
            if let Some(follow_up) = active.follow_up {
                queued.push(ScheduledEmbeddingRefresh {
                    compatibility_id: *compatibility_id,
                    source: follow_up.source,
                    ready_at: follow_up.ready_at,
                });
            }
        }
        queued.sort_by_key(|work| (work.ready_at, work.compatibility_id));
        Ok(EmbeddingSchedulerSnapshot {
            state: self.state,
            queued,
            in_flight: self.active.values().map(|active| active.lease).collect(),
            coalesced_notices: self.coalesced_notices,
            succeeded: self.succeeded,
            failed: self.failed,
            cancelled: self.cancelled,
        })
    }

    fn pending_count(&self) -> usize {
        self.queued.len()
            + self
                .active
                .values()
                .filter(|active| active.follow_up.is_some())
                .count()
    }

    fn ensure_running(&self) -> Result<(), SearchArtifactError> {
        if self.state == EmbeddingSchedulerState::Shutdown {
            return Err(SearchArtifactError::Cancelled);
        }
        Ok(())
    }

    fn ensure_inspectable(&self) -> Result<(), SearchArtifactError> {
        let entries = self
            .pending_count()
            .checked_add(self.active.len())
            .ok_or_else(|| exhausted("embedding_scheduler_inspected_entries", usize::MAX))?;
        if entries > self.limits.inspected_entries {
            return Err(exhausted(
                "embedding_scheduler_inspected_entries",
                self.limits.inspected_entries,
            ));
        }
        Ok(())
    }
}

fn retain_follow_up_after_coverage(
    follow_up: QueuedRefresh,
    covered: EmbeddingSourceState,
) -> Result<Option<QueuedRefresh>, SearchArtifactError> {
    match follow_up
        .source
        .graph_generation()
        .cmp(&covered.graph_generation())
    {
        std::cmp::Ordering::Less => Ok(None),
        std::cmp::Ordering::Equal if follow_up.source == covered => Ok(None),
        std::cmp::Ordering::Equal => Err(invalid(
            "embedding scheduler completion coverage",
            "conflicts with queued source at the same graph generation",
        )),
        std::cmp::Ordering::Greater => Ok(Some(follow_up)),
    }
}

fn validate_source_progress(
    prior: EmbeddingSourceState,
    next: EmbeddingSourceState,
) -> Result<(), SearchArtifactError> {
    match next.graph_generation().cmp(&prior.graph_generation()) {
        std::cmp::Ordering::Less => Err(invalid(
            "embedding scheduler source",
            "graph generation regressed",
        )),
        std::cmp::Ordering::Equal if next != prior => Err(invalid(
            "embedding scheduler source",
            "conflicts at the same graph generation",
        )),
        std::cmp::Ordering::Equal | std::cmp::Ordering::Greater => Ok(()),
    }
}

fn next_counter(counter: u64, resource: &'static str) -> Result<u64, SearchArtifactError> {
    counter
        .checked_add(1)
        .ok_or(SearchArtifactError::ResourceExhausted {
            resource,
            limit: u64::MAX,
        })
}

fn invalid(field: &'static str, reason: impl Into<String>) -> SearchArtifactError {
    SearchArtifactError::InvalidSelector {
        field,
        reason: reason.into(),
    }
}

fn exhausted(resource: &'static str, limit: usize) -> SearchArtifactError {
    SearchArtifactError::ResourceExhausted {
        resource,
        limit: u64::try_from(limit).unwrap_or(u64::MAX),
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;

    fn id(marker: u8) -> EmbeddingCompatibilityId {
        EmbeddingCompatibilityId::from_hex(&format!("{marker:02x}{}", "00".repeat(31))).unwrap()
    }

    fn source(generation: u64, marker: u8) -> EmbeddingSourceState {
        EmbeddingSourceState::new(generation, [marker; 32], [marker + 1; 32], 10)
    }

    fn new_scheduler() -> EmbeddingRefreshScheduler {
        EmbeddingRefreshScheduler::new(EmbeddingSchedulerLimits {
            debounce: Duration::from_millis(10),
            ..EmbeddingSchedulerLimits::default()
        })
        .unwrap()
    }

    #[test]
    fn constructor_rejects_each_public_zero_or_insufficient_limit() {
        let valid = EmbeddingSchedulerLimits::default();
        for limits in [
            EmbeddingSchedulerLimits {
                queued_lineages: 0,
                ..valid
            },
            EmbeddingSchedulerLimits {
                in_flight_lineages: 0,
                ..valid
            },
            EmbeddingSchedulerLimits {
                debounce: Duration::ZERO,
                ..valid
            },
            EmbeddingSchedulerLimits {
                inspected_entries: valid.queued_lineages + valid.in_flight_lineages - 1,
                ..valid
            },
        ] {
            assert!(matches!(
                EmbeddingRefreshScheduler::new(limits),
                Err(SearchArtifactError::InvalidSelector { .. })
            ));
        }
    }

    #[test]
    fn same_lineage_notices_coalesce_to_newest_debounced_source() {
        let mut scheduler = new_scheduler();
        scheduler
            .enqueue(id(1), source(10, 1), Duration::ZERO, || Ok(()))
            .unwrap();
        scheduler
            .enqueue(id(1), source(11, 2), Duration::from_millis(5), || Ok(()))
            .unwrap();
        assert!(
            scheduler
                .claim_ready(Duration::from_millis(14), || Ok(()))
                .unwrap()
                .is_none()
        );
        let lease = scheduler
            .claim_ready(Duration::from_millis(15), || Ok(()))
            .unwrap()
            .unwrap();
        assert_eq!(lease.source(), source(11, 2));
        assert_eq!(scheduler.snapshot().unwrap().coalesced_notices, 1);
    }

    #[test]
    fn resolved_lineage_debounce_overrides_the_project_default() {
        let mut scheduler = new_scheduler();
        scheduler
            .enqueue_with_debounce(
                id(1),
                source(10, 1),
                Duration::ZERO,
                Duration::from_millis(25),
                || Ok(()),
            )
            .unwrap();
        assert!(
            scheduler
                .claim_ready(Duration::from_millis(24), || Ok(()))
                .unwrap()
                .is_none()
        );
        assert!(
            scheduler
                .claim_ready(Duration::from_millis(25), || Ok(()))
                .unwrap()
                .is_some()
        );

        let error = new_scheduler()
            .enqueue_with_debounce(id(1), source(10, 1), Duration::ZERO, Duration::ZERO, || {
                Ok(())
            })
            .unwrap_err();
        assert!(error.to_string().contains("debounce"));
    }

    #[test]
    fn leases_serialize_lineages_and_bound_disjoint_progress() {
        let mut scheduler = EmbeddingRefreshScheduler::new(EmbeddingSchedulerLimits {
            in_flight_lineages: 1,
            debounce: Duration::from_millis(1),
            ..EmbeddingSchedulerLimits::default()
        })
        .unwrap();
        scheduler
            .enqueue(id(2), source(10, 2), Duration::ZERO, || Ok(()))
            .unwrap();
        scheduler
            .enqueue(id(1), source(10, 1), Duration::ZERO, || Ok(()))
            .unwrap();
        let first = scheduler
            .claim_ready(Duration::from_millis(1), || Ok(()))
            .unwrap()
            .unwrap();
        assert_eq!(first.compatibility_id(), id(1));
        assert!(
            scheduler
                .claim_ready(Duration::from_millis(1), || Ok(()))
                .unwrap()
                .is_none()
        );
        scheduler
            .complete(first, EmbeddingRefreshCompletion::Succeeded, || Ok(()))
            .unwrap();
        assert_eq!(
            scheduler
                .claim_ready(Duration::from_millis(1), || Ok(()))
                .unwrap()
                .unwrap()
                .compatibility_id(),
            id(2)
        );
    }

    #[test]
    fn midflight_mutations_create_one_follow_up() {
        let mut scheduler = new_scheduler();
        scheduler
            .enqueue(id(1), source(10, 1), Duration::ZERO, || Ok(()))
            .unwrap();
        let lease = scheduler
            .claim_ready(Duration::from_millis(10), || Ok(()))
            .unwrap()
            .unwrap();
        scheduler
            .enqueue(id(1), source(10, 1), Duration::from_millis(10), || Ok(()))
            .unwrap();
        assert!(scheduler.snapshot().unwrap().queued.is_empty());
        scheduler
            .enqueue(id(1), source(11, 2), Duration::from_millis(11), || Ok(()))
            .unwrap();
        scheduler
            .enqueue(id(1), source(12, 3), Duration::from_millis(12), || Ok(()))
            .unwrap();
        scheduler
            .complete(lease, EmbeddingRefreshCompletion::Failed, || Ok(()))
            .unwrap();
        assert!(
            scheduler
                .claim_ready(Duration::from_millis(21), || Ok(()))
                .unwrap()
                .is_none()
        );
        assert_eq!(
            scheduler
                .claim_ready(Duration::from_millis(22), || Ok(()))
                .unwrap()
                .unwrap()
                .source(),
            source(12, 3)
        );
        assert_eq!(scheduler.snapshot().unwrap().failed, 1);
    }

    #[test]
    fn successful_refresh_discards_only_follow_up_covered_by_published_source() {
        let mut covered = new_scheduler();
        covered
            .enqueue(id(1), source(10, 1), Duration::ZERO, || Ok(()))
            .unwrap();
        let lease = covered
            .claim_ready(Duration::from_millis(10), || Ok(()))
            .unwrap()
            .unwrap();
        covered
            .enqueue(id(1), source(11, 2), Duration::from_millis(11), || Ok(()))
            .unwrap();
        covered
            .complete_through(
                lease,
                EmbeddingRefreshCompletion::Succeeded,
                Some(source(11, 2)),
                || Ok(()),
            )
            .unwrap();
        let snapshot = covered.snapshot().unwrap();
        assert!(snapshot.queued.is_empty());
        assert!(snapshot.in_flight.is_empty());
        assert_eq!(snapshot.succeeded, 1);

        let mut newer = new_scheduler();
        newer
            .enqueue(id(1), source(10, 1), Duration::ZERO, || Ok(()))
            .unwrap();
        let lease = newer
            .claim_ready(Duration::from_millis(10), || Ok(()))
            .unwrap()
            .unwrap();
        newer
            .enqueue(id(1), source(12, 3), Duration::from_millis(12), || Ok(()))
            .unwrap();
        newer
            .complete_through(
                lease,
                EmbeddingRefreshCompletion::Succeeded,
                Some(source(11, 2)),
                || Ok(()),
            )
            .unwrap();
        assert_eq!(newer.snapshot().unwrap().queued[0].source, source(12, 3));
    }

    #[test]
    fn limits_regression_conflict_and_cancellation_are_structured() {
        assert!(
            EmbeddingRefreshScheduler::new(EmbeddingSchedulerLimits {
                queued_lineages: 0,
                ..EmbeddingSchedulerLimits::default()
            })
            .is_err()
        );
        let mut scheduler = EmbeddingRefreshScheduler::new(EmbeddingSchedulerLimits {
            queued_lineages: 1,
            in_flight_lineages: 1,
            debounce: Duration::from_millis(1),
            inspected_entries: 2,
        })
        .unwrap();
        scheduler
            .enqueue(id(1), source(10, 1), Duration::ZERO, || Ok(()))
            .unwrap();
        assert!(matches!(
            scheduler.enqueue(id(2), source(10, 2), Duration::ZERO, || Ok(())),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "embedding_scheduler_queued_lineages",
                ..
            })
        ));
        assert!(matches!(
            scheduler.enqueue(id(1), source(9, 1), Duration::ZERO, || Ok(())),
            Err(SearchArtifactError::InvalidSelector { .. })
        ));
        assert!(matches!(
            scheduler.enqueue(id(1), source(10, 2), Duration::ZERO, || Ok(())),
            Err(SearchArtifactError::InvalidSelector { .. })
        ));
        assert!(matches!(
            scheduler.claim_ready(Duration::from_millis(1), || {
                Err(SearchArtifactError::Cancelled)
            }),
            Err(SearchArtifactError::Cancelled)
        ));
    }

    #[test]
    fn shutdown_clears_private_work_and_reopen_is_empty() {
        let mut scheduler = new_scheduler();
        scheduler
            .enqueue(id(1), source(10, 1), Duration::ZERO, || Ok(()))
            .unwrap();
        let lease = scheduler
            .claim_ready(Duration::from_millis(10), || Ok(()))
            .unwrap()
            .unwrap();
        scheduler
            .enqueue(id(1), source(11, 2), Duration::from_millis(11), || Ok(()))
            .unwrap();
        scheduler.shutdown();
        let snapshot = scheduler.snapshot().unwrap();
        assert_eq!(snapshot.state, EmbeddingSchedulerState::Shutdown);
        assert!(snapshot.queued.is_empty());
        assert_eq!(snapshot.in_flight, vec![lease]);
        assert!(matches!(
            scheduler.claim_ready(Duration::MAX, || Ok(())),
            Err(SearchArtifactError::Cancelled)
        ));
        scheduler
            .complete(lease, EmbeddingRefreshCompletion::Cancelled, || Ok(()))
            .unwrap();
        assert!(scheduler.snapshot().unwrap().in_flight.is_empty());

        let reopened = new_scheduler();
        assert!(reopened.snapshot().unwrap().queued.is_empty());
        assert!(reopened.snapshot().unwrap().in_flight.is_empty());
    }

    #[test]
    fn checkpoints_bound_enqueue_claim_and_complete() {
        let calls = Cell::new(0_u8);
        let mut scheduler = new_scheduler();
        scheduler
            .enqueue(id(1), source(10, 1), Duration::ZERO, || {
                calls.set(calls.get() + 1);
                Ok(())
            })
            .unwrap();
        let lease = scheduler
            .claim_ready(Duration::from_millis(10), || {
                calls.set(calls.get() + 1);
                Ok(())
            })
            .unwrap()
            .unwrap();
        scheduler
            .complete(lease, EmbeddingRefreshCompletion::Succeeded, || {
                calls.set(calls.get() + 1);
                Ok(())
            })
            .unwrap();
        assert_eq!(calls.get(), 3);
    }
}
