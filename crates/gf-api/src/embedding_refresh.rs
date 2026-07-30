//! Public refresh policy and process-local worker inspection.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;

use gf_search::{
    EmbeddingRefreshScheduler, EmbeddingSchedulerLimits, EmbeddingSchedulerSnapshot,
    EmbeddingSchedulerState,
};
use gf_storage::{
    EmbeddingCompatibilityId, EmbeddingMutationBatch, EmbeddingMutationJournalLimits,
    EmbeddingRefreshConfig, EmbeddingRefreshConfigLimits, EmbeddingRefreshConfigUpdate,
    EmbeddingRefreshOutcomeRecord, EmbeddingRefreshProjectPolicy, EmbeddingRefreshSpacePolicy,
    ResolvedEmbeddingRefreshPolicy, SearchArtifactError, SearchCoordinationLimits,
    merge_embedding_mutation_batch, read_embedding_refresh_config, update_embedding_refresh_config,
};

use super::provider_session::ConfiguredProviderRefreshRuntime;
use super::{EmbeddingSpaceFreshnessInspection, GfError, GraphForge};

/// Lifecycle of the refresh worker owned by this exact embedded process.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EmbeddingRefreshWorkerState {
    /// This instance accepts refresh notices and leases.
    Running,
    /// This instance has stopped accepting refresh work.
    Shutdown,
}

/// Content-free process-local worker counters for one inspected lineage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmbeddingRefreshWorkerInspection {
    /// Lifecycle of this exact `GraphForge` instance's worker.
    pub state: EmbeddingRefreshWorkerState,
    /// Every queued or coalesced lineage in this process.
    pub queued_lineages: usize,
    /// Every active refresh lease in this process.
    pub in_flight_lineages: usize,
    /// Whether the inspected lineage has queued work.
    pub selected_lineage_queued: bool,
    /// Whether the inspected lineage has an active lease.
    pub selected_lineage_in_flight: bool,
    /// Repeated notices folded into existing process-local work.
    pub coalesced_notices: u64,
    /// Successful leases completed by this process.
    pub succeeded: u64,
    /// Failed leases completed by this process.
    pub failed: u64,
    /// Cancelled leases completed by this process.
    pub cancelled: u64,
}

/// Durable and process-local refresh state for one verified embedding lineage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmbeddingRefreshInspection {
    /// Exact compatibility lineage selected by alias or configured default.
    pub compatibility_id: String,
    /// Durable project-wide refresh defaults.
    pub project_policy: EmbeddingRefreshProjectPolicy,
    /// Optional durable override for this lineage.
    pub space_policy: Option<EmbeddingRefreshSpacePolicy>,
    /// Fully resolved effective policy for this lineage.
    pub resolved_policy: ResolvedEmbeddingRefreshPolicy,
    /// Durable content-free terminal outcome, when one has been recorded.
    pub last_outcome: Option<EmbeddingRefreshOutcomeRecord>,
    /// Active-generation freshness, absent before a first publication.
    pub freshness: Option<EmbeddingSpaceFreshnessInspection>,
    /// Live status for this process only; it makes no post-drop promise.
    pub worker: EmbeddingRefreshWorkerInspection,
}

impl GraphForge {
    pub(crate) fn register_provider_refresh_runtime(
        &self,
        runtime: ConfiguredProviderRefreshRuntime,
    ) -> Result<(), GfError> {
        let compatibility_id = runtime.compatibility_id();
        let mut runtimes = self
            .provider_refresh_runtimes
            .lock()
            .map_err(|_| validation("provider refresh runtime lock is poisoned"))?;
        runtimes.retain(|current| current.compatibility_id() != compatibility_id);
        runtimes.push(Arc::new(runtime));
        runtimes.sort_unstable_by_key(|current| current.compatibility_id());
        Ok(())
    }

    pub(crate) fn notice_provider_embedding_mutation(&self) {
        let runtimes = match self.provider_refresh_runtimes.lock() {
            Ok(runtimes) => runtimes.clone(),
            Err(_) => return,
        };
        let now = self.embedding_refresh_epoch.elapsed();
        for runtime in runtimes {
            let Ok(_visibility) = self.embedding_refresh_visibility.lock() else {
                continue;
            };
            let Ok((_, lineage)) =
                self.resolve_embedding_space_lineage(Some(runtime.display_name()))
            else {
                continue;
            };
            if lineage.compatibility_id() != runtime.compatibility_id() {
                continue;
            }
            let Some(active) = lineage.active() else {
                continue;
            };
            let Ok(current_source) = runtime.capture_source(self) else {
                continue;
            };
            if same_projection(active.manifest.source(), current_source) {
                continue;
            }
            if merge_embedding_mutation_batch(
                &self.dir,
                &active.manifest,
                EmbeddingMutationBatch {
                    current_source,
                    changed_uuids: &[],
                    structural_mutation: false,
                    scope_proven: false,
                },
                EmbeddingMutationJournalLimits::default(),
                SearchCoordinationLimits::default(),
                || Ok(()),
            )
            .is_err()
            {
                continue;
            }
            let Ok(config) = read_refresh_config(&self.dir) else {
                continue;
            };
            let policy = config.resolved_policy(runtime.compatibility_id());
            if !policy.proactive {
                continue;
            }
            let queued = self
                .embedding_refresh_scheduler
                .lock()
                .map_err(|_| ())
                .and_then(|mut scheduler| {
                    scheduler
                        .enqueue_with_debounce(
                            runtime.compatibility_id(),
                            current_source,
                            now,
                            policy.debounce,
                            || Ok(()),
                        )
                        .map_err(|_| ())
                })
                .is_ok();
            if queued {
                self.spawn_provider_refresh_driver();
            }
        }
    }

    fn spawn_provider_refresh_driver(&self) {
        if self
            .provider_refresh_driver_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let worker = self.refresh_worker_handle();
        if thread::Builder::new()
            .name("graphforge-embedding-refresh".to_owned())
            .spawn(move || {
                worker.run_provider_refresh_driver();
            })
            .is_err()
        {
            self.provider_refresh_driver_active
                .store(false, Ordering::Release);
        }
    }

    fn run_provider_refresh_driver(&self) {
        loop {
            thread::sleep(Duration::from_millis(10));
            self.drive_ready_provider_refreshes();
            if self.has_queued_provider_refreshes() {
                continue;
            }
            self.provider_refresh_driver_active
                .store(false, Ordering::Release);
            if self.has_queued_provider_refreshes()
                && self
                    .provider_refresh_driver_active
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                continue;
            }
            break;
        }
    }

    fn has_queued_provider_refreshes(&self) -> bool {
        self.embedding_refresh_scheduler
            .lock()
            .ok()
            .and_then(|scheduler| scheduler.snapshot().ok())
            .is_some_and(|snapshot| !snapshot.queued.is_empty())
    }

    fn drive_ready_provider_refreshes(&self) {
        loop {
            let lease = match self.embedding_refresh_scheduler.lock() {
                Ok(mut scheduler) => scheduler
                    .claim_ready(self.embedding_refresh_epoch.elapsed(), || Ok(()))
                    .ok()
                    .flatten(),
                Err(_) => None,
            };
            let Some(lease) = lease else {
                break;
            };
            let runtime = self
                .provider_refresh_runtimes
                .lock()
                .ok()
                .and_then(|runtimes| {
                    runtimes
                        .iter()
                        .find(|runtime| runtime.compatibility_id() == lease.compatibility_id())
                        .cloned()
                });
            let completion = match runtime {
                Some(runtime) => {
                    let result = runtime.refresh(self);
                    ConfiguredProviderRefreshRuntime::completion(&result)
                }
                None => gf_search::EmbeddingRefreshCompletion::Failed,
            };
            if let Ok(mut scheduler) = self.embedding_refresh_scheduler.lock() {
                let _ = scheduler.complete(lease, completion, || Ok(()));
            }
        }
    }

    fn refresh_worker_handle(&self) -> Self {
        Self {
            identity: self.identity.clone(),
            path: self.path.clone(),
            resolved_generation: self.resolved_generation.clone(),
            read_only: self.read_only,
            current_generation_uuid: Arc::clone(&self.current_generation_uuid),
            clock: std::sync::Mutex::new(Arc::clone(
                &self.clock.lock().expect("clock lock poisoned"),
            )),
            dir: self.dir.clone(),
            workspace_guard: Arc::clone(&self.workspace_guard),
            tempdir: self.tempdir.clone(),
            ontology: self.ontology.clone(),
            runtime_catalog: Arc::clone(&self.runtime_catalog),
            procedures: Arc::clone(&self.procedures),
            ontology_mode: self.ontology_mode,
            adjacency_provider: Arc::clone(&self.adjacency_provider),
            adjacency_visibility: Arc::clone(&self.adjacency_visibility),
            embedding_refresh_scheduler: Arc::clone(&self.embedding_refresh_scheduler),
            embedding_refresh_epoch: self.embedding_refresh_epoch,
            embedding_refresh_visibility: Arc::clone(&self.embedding_refresh_visibility),
            graph_visibility: Arc::clone(&self.graph_visibility),
            write_options: self.write_options,
            provider_refresh_driver_active: Arc::clone(&self.provider_refresh_driver_active),
            provider_refresh_runtimes: Arc::clone(&self.provider_refresh_runtimes),
            provider_find_runtimes: Arc::clone(&self.provider_find_runtimes),
            runtime: Arc::clone(&self.runtime),
        }
    }

    /// Read the durable project-wide embedding refresh policy.
    ///
    /// # Errors
    /// Returns structured cancellation, corruption, incompatibility, resource,
    /// or storage errors.
    pub fn embedding_refresh_project_policy(
        &self,
    ) -> Result<EmbeddingRefreshProjectPolicy, GfError> {
        Ok(read_refresh_config(&self.dir)?.project_policy())
    }

    /// Replace durable project-wide refresh defaults and the idle local worker.
    ///
    /// Live reconfiguration is rejected while this instance has queued or
    /// active work, so no lease is silently discarded. Other processes observe
    /// the durable policy on their next read or reopen.
    ///
    /// # Errors
    /// Returns validation when this worker is busy, plus structured storage,
    /// cancellation, corruption, incompatibility, or resource errors.
    pub fn set_embedding_refresh_project_policy(
        &self,
        policy: EmbeddingRefreshProjectPolicy,
    ) -> Result<EmbeddingRefreshProjectPolicy, GfError> {
        let replacement = scheduler_for_policy(policy)?;
        let mut scheduler = self
            .embedding_refresh_scheduler
            .lock()
            .map_err(|_| validation("embedding refresh scheduler lock is poisoned"))?;
        ensure_idle(&scheduler.snapshot()?)?;
        let config = update_embedding_refresh_config(
            &self.dir,
            EmbeddingRefreshConfigUpdate::SetProjectPolicy(policy),
            EmbeddingRefreshConfigLimits::default(),
            || Ok(()),
        )?;
        *scheduler = replacement;
        self.publish_workspace_update()?;
        Ok(config.project_policy())
    }

    /// Set or clear one verified lineage's durable refresh-policy override.
    ///
    /// `display_name=None` resolves the configured default. `policy=None`
    /// restores project defaults while retaining the last terminal outcome.
    ///
    /// # Errors
    /// Returns structured alias/default, storage, cancellation, corruption,
    /// incompatibility, validation, or resource errors.
    pub fn set_embedding_refresh_space_policy(
        &self,
        display_name: Option<&str>,
        policy: Option<EmbeddingRefreshSpacePolicy>,
    ) -> Result<EmbeddingRefreshInspection, GfError> {
        let (_, lineage) = self.resolve_embedding_space_lineage(display_name)?;
        update_embedding_refresh_config(
            &self.dir,
            EmbeddingRefreshConfigUpdate::SetSpacePolicy {
                compatibility_id: lineage.compatibility_id(),
                policy,
            },
            EmbeddingRefreshConfigLimits::default(),
            || Ok(()),
        )?;
        self.publish_workspace_update()?;
        self.inspect_embedding_refresh(display_name)
    }

    /// Inspect durable policy/outcome and this process's worker state.
    ///
    /// The worker counters are intentionally process-local. Reopen reconstructs
    /// durable policy and outcomes but starts with no queued or active work.
    ///
    /// # Errors
    /// Returns structured alias/default, freshness, storage, cancellation,
    /// corruption, incompatibility, validation, or resource errors.
    pub fn inspect_embedding_refresh(
        &self,
        display_name: Option<&str>,
    ) -> Result<EmbeddingRefreshInspection, GfError> {
        let (space, lineage) = self.resolve_embedding_space_lineage(display_name)?;
        let compatibility_id = lineage.compatibility_id();
        let config = read_refresh_config(&self.dir)?;
        let state = config
            .spaces()
            .into_iter()
            .find(|state| state.compatibility_id == compatibility_id);
        let snapshot = self
            .embedding_refresh_scheduler
            .lock()
            .map_err(|_| validation("embedding refresh scheduler lock is poisoned"))?
            .snapshot()?;
        let freshness = if space.active.is_some() {
            Some(self.inspect_embedding_space_freshness(display_name, false)?)
        } else {
            None
        };
        Ok(EmbeddingRefreshInspection {
            compatibility_id: compatibility_id.to_hex(),
            project_policy: config.project_policy(),
            space_policy: state.as_ref().and_then(|state| state.policy),
            resolved_policy: config.resolved_policy(compatibility_id),
            last_outcome: state.and_then(|state| state.last_outcome),
            freshness,
            worker: worker_inspection(&snapshot, compatibility_id),
        })
    }
}

fn same_projection(
    recorded: gf_storage::EmbeddingSourceState,
    current: gf_storage::EmbeddingSourceState,
) -> bool {
    recorded.label_membership_digest() == current.label_membership_digest()
        && recorded.dependency_input_digest() == current.dependency_input_digest()
        && recorded.eligible_uuid_count() == current.eligible_uuid_count()
}

pub(crate) fn initialize_embedding_refresh_scheduler(
    project_dir: &Path,
) -> Result<EmbeddingRefreshScheduler, GfError> {
    scheduler_for_policy(read_refresh_config(project_dir)?.project_policy()).map_err(Into::into)
}

fn read_refresh_config(project_dir: &Path) -> Result<EmbeddingRefreshConfig, GfError> {
    read_embedding_refresh_config(project_dir, EmbeddingRefreshConfigLimits::default(), || {
        Ok(())
    })
    .map_err(Into::into)
}

fn scheduler_for_policy(
    policy: EmbeddingRefreshProjectPolicy,
) -> Result<EmbeddingRefreshScheduler, SearchArtifactError> {
    EmbeddingRefreshScheduler::new(EmbeddingSchedulerLimits {
        in_flight_lineages: policy.max_concurrent_jobs,
        debounce: policy.debounce,
        ..EmbeddingSchedulerLimits::default()
    })
}

fn ensure_idle(snapshot: &EmbeddingSchedulerSnapshot) -> Result<(), GfError> {
    if snapshot.queued.is_empty() && snapshot.in_flight.is_empty() {
        Ok(())
    } else {
        Err(validation(
            "embedding refresh policy cannot change while this process has queued or active work",
        ))
    }
}

fn worker_inspection(
    snapshot: &EmbeddingSchedulerSnapshot,
    compatibility_id: EmbeddingCompatibilityId,
) -> EmbeddingRefreshWorkerInspection {
    EmbeddingRefreshWorkerInspection {
        state: match snapshot.state {
            EmbeddingSchedulerState::Running => EmbeddingRefreshWorkerState::Running,
            EmbeddingSchedulerState::Shutdown => EmbeddingRefreshWorkerState::Shutdown,
        },
        queued_lineages: snapshot.queued.len(),
        in_flight_lineages: snapshot.in_flight.len(),
        selected_lineage_queued: snapshot
            .queued
            .iter()
            .any(|work| work.compatibility_id == compatibility_id),
        selected_lineage_in_flight: snapshot
            .in_flight
            .iter()
            .any(|lease| lease.compatibility_id() == compatibility_id),
        coalesced_notices: snapshot.coalesced_notices,
        succeeded: snapshot.succeeded,
        failed: snapshot.failed,
        cancelled: snapshot.cancelled,
    }
}

fn validation(message: impl Into<String>) -> GfError {
    GfError::Validation(message.into())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use super::*;
    use crate::{
        CallerEmbeddingBatchRequest, CallerEmbeddingDistance, CallerEmbeddingNormalization,
    };

    fn publish_empty(graph: &GraphForge, display_name: &str) -> String {
        graph
            .publish_caller_embeddings(CallerEmbeddingBatchRequest {
                display_name: display_name.to_owned(),
                contract_version: "v1".to_owned(),
                dimensions: 2,
                normalization: CallerEmbeddingNormalization::None,
                distance: CallerEmbeddingDistance::Cosine,
                source_projection_recipe: BTreeMap::from([(
                    "label".to_owned(),
                    "Document".to_owned(),
                )]),
                rows: Vec::new(),
                replace_alias: false,
            })
            .unwrap()
            .compatibility_id
    }

    #[test]
    fn defaults_and_space_overrides_are_durable_and_content_free() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
        let compatibility_id = publish_empty(&graph, "semantic");

        assert_eq!(
            graph.embedding_refresh_project_policy().unwrap(),
            EmbeddingRefreshProjectPolicy::default()
        );
        let initial = graph.inspect_embedding_refresh(Some("semantic")).unwrap();
        assert_eq!(initial.compatibility_id, compatibility_id);
        assert_eq!(initial.space_policy, None);
        assert_eq!(
            initial.resolved_policy,
            ResolvedEmbeddingRefreshPolicy {
                proactive: true,
                debounce: Duration::from_millis(500),
                max_concurrent_jobs: 2,
            }
        );
        assert_eq!(initial.last_outcome, None);
        assert_eq!(initial.worker.state, EmbeddingRefreshWorkerState::Running);
        assert_eq!(initial.worker.queued_lineages, 0);
        assert_eq!(initial.worker.in_flight_lineages, 0);

        let project_policy = EmbeddingRefreshProjectPolicy {
            proactive: true,
            debounce: Duration::from_millis(750),
            max_concurrent_jobs: 1,
        };
        assert_eq!(
            graph
                .set_embedding_refresh_project_policy(project_policy)
                .unwrap(),
            project_policy
        );
        let policy = EmbeddingRefreshSpacePolicy {
            proactive: Some(false),
            debounce: Some(Duration::from_secs(2)),
        };
        let updated = graph
            .set_embedding_refresh_space_policy(Some("semantic"), Some(policy))
            .unwrap();
        assert_eq!(updated.space_policy, Some(policy));
        assert!(!updated.resolved_policy.proactive);
        assert_eq!(updated.resolved_policy.max_concurrent_jobs, 1);

        let outcome = EmbeddingRefreshOutcomeRecord {
            status: gf_storage::EmbeddingRefreshOutcomeStatus::Failed(
                gf_storage::EmbeddingRefreshFailureClass::Provider,
            ),
            graph_generation: 1,
            source_fingerprint: gf_storage::EmbeddingSourceFingerprint::digest(b"source"),
            completed_at_micros: 10,
        };
        update_embedding_refresh_config(
            &graph.dir,
            EmbeddingRefreshConfigUpdate::RecordOutcome {
                compatibility_id: EmbeddingCompatibilityId::from_hex(&compatibility_id).unwrap(),
                outcome,
            },
            EmbeddingRefreshConfigLimits::default(),
            || Ok(()),
        )
        .unwrap();
        graph.publish_workspace_update().unwrap();
        assert_eq!(
            graph
                .inspect_embedding_refresh(Some("semantic"))
                .unwrap()
                .last_outcome,
            Some(outcome)
        );

        drop(graph);
        let reopened = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
        let reopened = reopened
            .inspect_embedding_refresh(Some("semantic"))
            .unwrap();
        assert_eq!(reopened.project_policy, project_policy);
        assert_eq!(reopened.space_policy, Some(policy));
        assert_eq!(reopened.last_outcome, Some(outcome));
        assert_eq!(reopened.worker.queued_lineages, 0);
        assert_eq!(reopened.worker.in_flight_lineages, 0);
        assert_eq!(reopened.worker.coalesced_notices, 0);
    }

    #[test]
    fn project_policy_rebuilds_only_an_idle_bounded_worker() {
        let graph = GraphForge::new(None).unwrap();
        let compatibility_id = publish_empty(&graph, "semantic");
        let compatibility_id = EmbeddingCompatibilityId::from_hex(&compatibility_id).unwrap();
        let source = gf_storage::EmbeddingSourceState::new(1, [1; 32], [2; 32], 0);
        graph
            .embedding_refresh_scheduler
            .lock()
            .unwrap()
            .enqueue(compatibility_id, source, Duration::from_secs(1), || Ok(()))
            .unwrap();
        let queued = graph.inspect_embedding_refresh(Some("semantic")).unwrap();
        assert_eq!(queued.worker.queued_lineages, 1);
        assert!(queued.worker.selected_lineage_queued);

        let error = graph
            .set_embedding_refresh_project_policy(EmbeddingRefreshProjectPolicy {
                proactive: false,
                debounce: Duration::from_secs(1),
                max_concurrent_jobs: 1,
            })
            .unwrap_err();
        assert!(error.to_string().contains("queued or active work"));
        assert_eq!(
            graph.embedding_refresh_project_policy().unwrap(),
            EmbeddingRefreshProjectPolicy::default()
        );
    }

    #[test]
    fn policy_validation_aliases_and_cancellation_remain_structured() {
        let graph = GraphForge::new(None).unwrap();
        let error = graph
            .set_embedding_refresh_project_policy(EmbeddingRefreshProjectPolicy {
                proactive: true,
                debounce: Duration::ZERO,
                max_concurrent_jobs: 2,
            })
            .unwrap_err();
        assert!(error.to_string().contains("debounce"));
        assert!(matches!(
            graph
                .inspect_embedding_refresh(Some("missing"))
                .unwrap_err(),
            GfError::Validation(_)
        ));

        let compatibility_id = publish_empty(&graph, "semantic");
        graph.embedding_refresh_scheduler.lock().unwrap().shutdown();
        let error = graph
            .embedding_refresh_scheduler
            .lock()
            .unwrap()
            .enqueue(
                EmbeddingCompatibilityId::from_hex(&compatibility_id).unwrap(),
                gf_storage::EmbeddingSourceState::new(1, [1; 32], [2; 32], 0),
                Duration::ZERO,
                || Ok(()),
            )
            .unwrap_err();
        assert!(matches!(error, SearchArtifactError::Cancelled));
    }
}
