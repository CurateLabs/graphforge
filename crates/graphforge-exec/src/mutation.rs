//! Shared mutation accounting. Storage staging and publication consume the same
//! state for statement and analyst mutations.

use std::collections::{BTreeMap, HashSet};

/// The openCypher write counters a statement reports.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WriteCounters {
    pub nodes_created: u64,
    pub edges_created: u64,
    pub nodes_deleted: u64,
    pub edges_deleted: u64,
    pub properties_set: u64,
    pub properties_removed: u64,
    pub labels_added: u64,
    pub labels_removed: u64,
}

/// Deduplicated effects and property counters for one mutation boundary.
#[derive(Default)]
pub(crate) struct MutationState {
    pub counters: WriteCounters,
    pub property_sets: HashSet<(bool, [u8; 16], String)>,
    pub effects: BTreeMap<
        crate::MutationKind,
        (
            HashSet<crate::MutationSubject>,
            HashSet<crate::MutationSubject>,
        ),
    >,
}

impl MutationState {
    pub(crate) fn record_mutation_input(
        &mut self,
        kind: crate::MutationKind,
        subject_kind: crate::MutationSubjectKind,
        uuid: [u8; 16],
    ) {
        self.effects
            .entry(kind)
            .or_default()
            .0
            .insert(crate::MutationSubject {
                uuid,
                kind: subject_kind,
            });
    }

    pub(crate) fn record_mutation_output(
        &mut self,
        kind: crate::MutationKind,
        subject_kind: crate::MutationSubjectKind,
        uuid: [u8; 16],
    ) {
        self.effects
            .entry(kind)
            .or_default()
            .1
            .insert(crate::MutationSubject {
                uuid,
                kind: subject_kind,
            });
    }

    pub(crate) fn mutation_receipt(&self) -> crate::MutationReceipt {
        crate::MutationReceipt::from_accumulators(self.effects.clone())
    }

    pub(crate) fn record_property_set(
        &mut self,
        is_edge: bool,
        uuid: [u8; 16],
        name: &str,
    ) -> bool {
        if self.property_sets.insert((is_edge, uuid, name.to_owned())) {
            self.counters.properties_set += 1;
            true
        } else {
            false
        }
    }
}

impl MutationState {
    /// Commit a statement's staged files with the existing topology index and
    /// adjacency-delta participant. Property-only commits do not open a writer.
    pub(crate) fn commit_topology(
        &mut self,
        staged: graphforge_storage::RewriteBatch,
        writer: &mut graphforge_storage::GraphWriter,
        dir: &std::path::Path,
        deleted_node_ids: &HashSet<[u8; 16]>,
        deleted_edge_ids: &HashSet<[u8; 16]>,
    ) -> Result<(), graphforge_core::GfError> {
        use graphforge_core::uuid::Uuid;
        // Adjacency delta segment (#765): a statement is pure-append iff it stages
        // no deletes (SET/REMOVE never touch topology). Pure-append → record the
        // created edges so the index serves them without a rebuild; otherwise the
        // statement breaks the chain at this generation (a DELETE invalidates the
        // incremental path), so write no segment and clear any stale file there.
        let pure_append = deleted_node_ids.is_empty() && deleted_edge_ids.is_empty();
        let pending = writer.take_pending_delta();
        let deleted_nodes = deleted_node_ids
            .iter()
            .copied()
            .map(Uuid::from_bytes)
            .collect::<Vec<_>>();
        let deleted_edges = deleted_edge_ids
            .iter()
            .copied()
            .map(Uuid::from_bytes)
            .collect::<Vec<_>>();
        if let Some(generation) =
            writer.commit_topology_aware_with_uuid_index(staged, deleted_nodes, deleted_edges)?
        {
            if pure_append {
                writer.write_segment_best_effort(generation, &pending);
            } else {
                graphforge_storage::adjacency_delta::discard_segment(dir, generation);
            }
        }
        Ok(())
    }
}

/// Neutral, deterministic effects and counters from one mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutationOutcome {
    /// Deduplicated subjects grouped by mutation kind.
    pub receipt: crate::MutationReceipt,
    /// Existing statement counter semantics, shared with analyst writes.
    pub side_effects: crate::SideEffects,
}

/// A mutation's private catalog, accounting and staged files.
///
/// Binding uses `catalog()` before constructing the execution session, so the
/// session's admitted identity contract and the staged catalog share one view.
/// Preparing a mutation does not publish or install its catalog in the facade.
pub struct MutationTransaction {
    catalog: std::sync::Arc<std::sync::Mutex<graphforge_ir::RuntimeCatalog>>,
    pub(crate) state: MutationState,
    pub(crate) staged: graphforge_storage::RewriteBatch,
    admitted: Option<(
        crate::write_resource::BoundWriteResource,
        arrow::record_batch::RecordBatch,
    )>,
    validate_catalog: bool,
    prepared: bool,
    topology: Option<(
        graphforge_storage::GraphWriter,
        HashSet<[u8; 16]>,
        HashSet<[u8; 16]>,
    )>,
}

impl MutationTransaction {
    /// Begin with an isolated copy of the currently visible catalog.
    #[must_use]
    pub fn new(catalog: &graphforge_ir::RuntimeCatalog) -> Self {
        Self {
            catalog: std::sync::Arc::new(std::sync::Mutex::new(catalog.clone())),
            state: MutationState::default(),
            staged: graphforge_storage::RewriteBatch::new(),
            topology: None,
            admitted: None,
            validate_catalog: true,
            prepared: false,
        }
    }

    pub(crate) fn local() -> Self {
        let mut transaction = Self::new(&graphforge_ir::RuntimeCatalog::new());
        transaction.validate_catalog = false;
        transaction
    }

    pub(crate) fn ensure_unprepared(&self) -> Result<(), graphforge_core::GfError> {
        if self.prepared {
            return Err(graphforge_core::GfError::Execution(
                "mutation transaction already prepared".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn admit(
        &mut self,
        resource: &crate::write_resource::BoundWriteResource,
    ) -> Result<(), graphforge_core::GfError> {
        resource.health.check()?;
        let catalog = self.catalog.lock().expect("mutation catalog poisoned");
        let snapshot = catalog.to_record_batch();
        if let Some((admitted, expected)) = &self.admitted {
            admitted.health.check()?;
            if !admitted.same_authority(resource) || *expected != snapshot {
                return Err(graphforge_core::GfError::Execution(
                    "GF_WRITE_RESOURCE_INCOMPATIBLE: mutation authority or catalog changed".into(),
                ));
            }
        } else {
            if self.validate_catalog {
                resource
                    .validate_catalog(&catalog)
                    .map_err(graphforge_core::GfError::from_plan_error)?;
            }
            self.admitted = Some((resource.clone(), snapshot));
        }
        Ok(())
    }

    /// The working identity snapshot used by mutation binding and execution.
    #[must_use]
    pub fn catalog(&self) -> std::sync::Arc<std::sync::Mutex<graphforge_ir::RuntimeCatalog>> {
        std::sync::Arc::clone(&self.catalog)
    }

    /// Observe an analyst property through the same catalog owner as Cypher.
    ///
    /// # Errors
    /// Returns an error if the runtime catalog cannot allocate the identity.
    pub fn intern_property(
        &self,
        name: &str,
        owner: Option<&str>,
    ) -> Result<(), graphforge_core::GfError> {
        self.catalog
            .lock()
            .expect("mutation catalog poisoned")
            .intern_property(name, owner)?;
        Ok(())
    }

    /// Stage property updates under explicit write authority and accumulate the
    /// same entity/property counters and neutral effects used by Cypher SET.
    ///
    /// # Errors
    /// Returns an error if the authenticated property rewrite cannot be staged.
    pub fn stage_node_properties(
        &mut self,
        resource: &crate::write_resource::BoundWriteResource,
        inventory: &graphforge_storage::AuthenticatedPropertyInventory,
        stem: &str,
        updates: &std::collections::HashMap<
            [u8; 16],
            std::collections::HashMap<String, graphforge_ir::IrLiteral>,
        >,
    ) -> Result<u64, graphforge_core::GfError> {
        self.admit(resource)?;
        self.ensure_unprepared()?;
        self.prepared = true;
        let counts = graphforge_storage::stage_set_node_properties_authenticated_with_counts(
            &mut self.staged,
            resource.directory(),
            inventory,
            stem,
            updates,
        )?;
        for (uuid, properties) in updates {
            for name in properties.keys() {
                self.state.record_property_set(false, *uuid, name);
            }
            self.state.record_mutation_output(
                crate::MutationKind::SetProperty,
                crate::MutationSubjectKind::Node,
                *uuid,
            );
        }
        self.state.counters.properties_removed += counts.properties_replaced;
        self.prepared = true;
        Ok(counts.entities_touched)
    }

    /// The shared receipt and counter result for this mutation.
    #[must_use]
    pub fn outcome(&self) -> MutationOutcome {
        let c = self.state.counters;
        MutationOutcome {
            receipt: self.receipt(),
            side_effects: crate::SideEffects {
                nodes_created: c.nodes_created,
                nodes_deleted: c.nodes_deleted,
                relationships_created: c.edges_created,
                relationships_deleted: c.edges_deleted,
                properties_set: c.properties_set,
                properties_removed: c.properties_removed,
                labels_added: c.labels_added,
                labels_removed: c.labels_removed,
            },
        }
    }

    /// Deterministically ordered effects accumulated by the mutation.
    #[must_use]
    pub fn receipt(&self) -> crate::MutationReceipt {
        self.state.mutation_receipt()
    }
}

impl MutationTransaction {
    pub(crate) fn prepare_statement(
        &mut self,
        mut context: crate::write_driver::StatementWriteContext,
        resource: &crate::write_resource::BoundWriteResource,
    ) -> Result<(), graphforge_core::GfError> {
        self.admit(resource)?;
        self.ensure_unprepared()?;
        self.prepared = true;
        self.staged = crate::write_driver::stage_statement(&mut context, resource.directory())?;
        self.prepared = true;
        self.state = context.mutation;
        self.topology = Some((
            context.writer,
            context.pending_node_deletes,
            context.pending_edge_deletes,
        ));
        Ok(())
    }

    /// Stage the working catalog in the same rewrite as graph properties.
    ///
    /// # Errors
    /// Returns a storage error if the catalog cannot be staged.
    pub fn stage_catalog(
        &mut self,
        resource: &crate::write_resource::BoundWriteResource,
    ) -> Result<(), graphforge_core::GfError> {
        self.admit(resource)?;
        if !self.prepared {
            return Err(graphforge_core::GfError::Execution(
                "mutation transaction has not been prepared".into(),
            ));
        }
        let batch = &self.admitted.as_ref().expect("admitted above").1;
        self.staged.stage(
            &resource
                .directory()
                .join("topology/runtime_catalog.parquet"),
            batch.schema(),
            batch,
        )
    }

    pub(crate) fn commit_local(
        &mut self,
        resource: &crate::write_resource::BoundWriteResource,
    ) -> Result<(), graphforge_core::GfError> {
        self.admit(resource)?;
        let staged = std::mem::replace(&mut self.staged, graphforge_storage::RewriteBatch::new());
        match &mut self.topology {
            Some((writer, nodes, edges)) => {
                self.state
                    .commit_topology(staged, writer, resource.directory(), nodes, edges)
            }
            None => staged.commit_at(resource.directory()),
        }
    }
}

/// The generation owner supplies publication and recovery without exposing
/// facade internals to the statement driver or analyst staging code.
pub trait MutationLifecycle {
    /// Publish the locally committed graph and its working catalog.
    fn publish(
        &mut self,
        outcome: &MutationOutcome,
        catalog: &graphforge_ir::RuntimeCatalog,
    ) -> Result<(), graphforge_core::GfError>;
    /// Refresh retained resources after a successful mutation.
    fn complete(&mut self) -> Result<(), graphforge_core::GfError>;
    /// Restore the prior state, or reconcile an already published generation.
    /// Implementations must determine authority before restoring any files.
    fn abort(&mut self) -> Result<(), graphforge_core::GfError>;
}

impl MutationTransaction {
    /// Commit staged graph/catalog files and publish through the generation
    /// owner. Every failure, including local rewrite failure, shares one abort.
    ///
    /// # Errors
    /// Returns staging, local commit, publication or recovery errors.
    pub fn commit(
        mut self,
        resource: &crate::write_resource::BoundWriteResource,
        persist_catalog: bool,
        lifecycle: &mut impl MutationLifecycle,
    ) -> Result<(), graphforge_core::GfError> {
        let result = (|| {
            self.admit(resource)?;
            if !self.prepared {
                return Err(graphforge_core::GfError::Execution(
                    "mutation transaction has not been prepared".into(),
                ));
            }
            if persist_catalog {
                self.stage_catalog(resource)?;
            }
            self.commit_local(resource)?;
            let catalog = graphforge_ir::RuntimeCatalog::from_record_batch(
                &self.admitted.as_ref().expect("admitted above").1,
            )?;
            lifecycle.publish(&self.outcome(), &catalog)?;
            lifecycle.complete()
        })();
        match result {
            Ok(()) => Ok(()),
            Err(error) => self.abort(lifecycle, error),
        }
    }

    /// Abort a preparation failure through the same generation-aware recovery
    /// path used for commit errors. Uninstalled temporary files are discarded.
    ///
    /// # Errors
    /// Returns the original error unless recovery itself fails.
    pub fn abort<T>(
        mut self,
        lifecycle: &mut impl MutationLifecycle,
        error: graphforge_core::GfError,
    ) -> Result<T, graphforge_core::GfError> {
        self.staged = graphforge_storage::RewriteBatch::new();
        self.topology = None;
        lifecycle.abort()?;
        Err(error)
    }
}

pub(crate) struct LocalMutationLifecycle<'a> {
    checkpoint: graphforge_storage::GraphWorkspaceCheckpoint,
    session: &'a crate::ExecutionSession,
    resource: crate::write_resource::BoundWriteResource,
}

impl<'a> LocalMutationLifecycle<'a> {
    pub(crate) fn new(
        session: &'a crate::ExecutionSession,
        resource: crate::write_resource::BoundWriteResource,
    ) -> Result<Self, graphforge_core::GfError> {
        // Preserve GraphWriter::open_at's fresh-target behavior after explicit
        // resource admission, before capturing the empty rollback baseline.
        std::fs::create_dir_all(resource.directory())
            .map_err(|error| graphforge_core::GfError::Storage(error.to_string()))?;
        Ok(Self {
            checkpoint: graphforge_storage::GraphWorkspaceCheckpoint::capture(
                resource.directory(),
            )?,
            session,
            resource,
        })
    }
}

impl MutationLifecycle for LocalMutationLifecycle<'_> {
    fn publish(
        &mut self,
        _: &MutationOutcome,
        _: &graphforge_ir::RuntimeCatalog,
    ) -> Result<(), graphforge_core::GfError> {
        Ok(())
    }
    fn complete(&mut self) -> Result<(), graphforge_core::GfError> {
        self.session
            .catalog
            .refresh_property_inventory(self.resource.directory())
            .map_err(graphforge_core::GfError::from_execution_error)?;
        self.session.adjacency_provider.invalidate();
        Ok(())
    }
    fn abort(&mut self) -> Result<(), graphforge_core::GfError> {
        let health = self.session.mutation_health.clone();
        health.recover(|| {
            self.checkpoint.restore(self.resource.directory())?;
            self.complete()
        })
    }
}

/// Shared availability of mutable execution resources during and after recovery.
/// Recovery invalidates existing lazy streams. A failed recovery permanently
/// denies this owner; reopening establishes a fresh owner from durable authority.
#[derive(Debug, Clone, Default)]
pub struct MutationHealth(std::sync::Arc<std::sync::Mutex<MutationHealthState>>);

#[derive(Debug, Default)]
struct MutationHealthState {
    epoch: u64,
    recovering: bool,
    failure: Option<String>,
}

impl MutationHealthState {
    fn check(&self) -> Result<(), graphforge_core::GfError> {
        if let Some(reason) = &self.failure {
            return Err(graphforge_core::GfError::Storage(format!(
                "mutation workspace unavailable after failed recovery: {reason}"
            )));
        }
        if self.recovering {
            return Err(graphforge_core::GfError::Storage(
                "mutation workspace unavailable during recovery".into(),
            ));
        }
        Ok(())
    }
}

impl MutationHealth {
    /// Deny subsequent mutation/read work and retained stream pulls.
    pub fn fail(&self, error: &graphforge_core::GfError) {
        self.0
            .lock()
            .expect("mutation health poisoned")
            .failure
            .get_or_insert_with(|| error.to_string());
    }

    /// Check whether workspace recovery has left resources usable.
    ///
    /// # Errors
    /// Returns an error during recovery or after this owner becomes unavailable.
    pub fn check(&self) -> Result<(), graphforge_core::GfError> {
        self.0.lock().expect("mutation health poisoned").check()
    }

    /// Fence mutable resources for the entire restoration operation.
    ///
    /// # Errors
    /// Rejects overlapping recovery and retains any restoration failure. Success
    /// cannot clear a failure recorded by another operation on the same owner.
    pub fn recover(
        &self,
        restore: impl FnOnce() -> Result<(), graphforge_core::GfError>,
    ) -> Result<(), graphforge_core::GfError> {
        let epoch = {
            let mut state = self.0.lock().expect("mutation health poisoned");
            state.check()?;
            state.epoch = state.epoch.checked_add(1).ok_or_else(|| {
                graphforge_core::GfError::Storage("mutation recovery epoch exhausted".into())
            })?;
            state.recovering = true;
            state.epoch
        };
        let result = restore();
        let mut state = self.0.lock().expect("mutation health poisoned");
        if let Err(error) = &result {
            state.failure.get_or_insert_with(|| error.to_string());
        }
        if state.epoch == epoch {
            state.recovering = false;
        }
        result?;
        state.check()
    }

    fn stream_epoch(&self) -> Result<u64, graphforge_core::GfError> {
        let state = self.0.lock().expect("mutation health poisoned");
        state.check()?;
        Ok(state.epoch)
    }

    fn check_stream_epoch(&self, epoch: Option<u64>) -> Result<(), graphforge_core::GfError> {
        if Some(self.stream_epoch()?) != epoch {
            return Err(graphforge_core::GfError::Storage(
                "mutation workspace stream invalidated by recovery".into(),
            ));
        }
        Ok(())
    }

    /// Retain the same health owner and recovery epoch for every lazy pull.
    #[must_use]
    pub fn guard_stream(
        &self,
        mut stream: crate::SendableRecordBatchStream,
    ) -> crate::SendableRecordBatchStream {
        let schema = stream.schema();
        let health = self.clone();
        let epoch = health.stream_epoch().ok();
        let mut failed = false;
        let guarded = futures::stream::poll_fn(move |cx| {
            if failed {
                return std::task::Poll::Ready(None);
            }
            let result = health.check_stream_epoch(epoch).and_then(|()| {
                let result = stream.as_mut().poll_next(cx);
                // Recovery may have started while the inner poll read files.
                // Never expose its batch, even when restoration already ended.
                health.check_stream_epoch(epoch)?;
                Ok(result)
            });
            match result {
                Ok(result) => result,
                Err(error) => {
                    failed = true;
                    std::task::Poll::Ready(Some(Err(
                        datafusion::common::DataFusionError::External(Box::new(error)),
                    )))
                }
            }
        });
        Box::pin(datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(schema, guarded))
    }
}

#[cfg(test)]
mod health_tests {
    use super::MutationHealth;
    use futures::StreamExt;

    #[tokio::test]
    async fn recovery_during_inner_poll_discards_batch_and_invalidates_old_stream() {
        let health = MutationHealth::default();
        let schema = std::sync::Arc::new(arrow::datatypes::Schema::empty());
        let batch = arrow::record_batch::RecordBatch::new_empty(schema.clone());
        let inner_health = health.clone();
        let inner_batch = batch.clone();
        let stream = futures::stream::poll_fn(move |_| {
            inner_health.recover(|| Ok(())).unwrap();
            std::task::Poll::Ready(Some(Ok(inner_batch.clone())))
        });
        let mut old = health.guard_stream(Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                schema.clone(),
                stream,
            ),
        ));
        assert!(
            old.next()
                .await
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("invalidated")
        );
        assert!(old.next().await.is_none());
        health.check().unwrap();
        let mut fresh = health.guard_stream(Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                schema,
                futures::stream::iter([Ok(batch)]),
            ),
        ));
        assert!(fresh.next().await.unwrap().is_ok());
    }

    #[test]
    fn successful_restore_cannot_clear_concurrent_failure_or_admit_nested_recovery() {
        let health = MutationHealth::default();
        let error = health
            .recover(|| {
                assert!(health.check().is_err());
                assert!(
                    health
                        .recover(|| panic!("overlapping restore ran"))
                        .is_err()
                );
                health.fail(&graphforge_core::GfError::Storage(
                    "independent failure".into(),
                ));
                Ok(())
            })
            .unwrap_err();
        assert!(error.to_string().contains("independent failure"));
        assert!(health.check().is_err());
    }
}
