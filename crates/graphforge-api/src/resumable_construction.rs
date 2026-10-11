//! Public Rust facade for resumable, bounded, disk-owned graph construction.

#[cfg(all(test, feature = "portable"))]
mod codec_tests;

use arrow::record_batch::RecordBatch;
use graphforge_core::hash_observation::ControlSha256 as Sha256;
use graphforge_core::uuid::Uuid;
use graphforge_storage::concurrency_attribution::RegionScope;
use sha2::Digest;

use crate::{
    ConstructionChunkReceipt, GfError, GraphConstructionBudgets, GraphConstructionEvidence,
    GraphConstructionState, GraphForge, OntologyMode,
};

/// Content-free durable progress for one construction session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphConstructionProgress {
    /// Durable session identifier required to resume after interruption.
    pub session_uuid: Uuid,
    /// Private lifecycle state; sealing does not imply publication.
    pub state: GraphConstructionState,
    /// Topology generation pinned when the session began.
    pub parent_topology_generation: u64,
    /// Number of durably accepted chunks.
    pub accepted_chunks: u64,
    /// Whether the sole project publication commit point was crossed.
    pub publication_committed: bool,
    /// Storage-measured bounded-work evidence.
    pub evidence: GraphConstructionEvidence,
}

/// Receipt for the sole project-generation transition owned by a session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphConstructionPublicationReceipt {
    /// Published project generation.
    pub generation_uuid: Uuid,
    /// Whether the same durable publication was returned on retry.
    pub idempotent_replay: bool,
}

/// Owned resumable construction handle. Arrow chunks remain storage-owned.
pub struct GraphConstructionSession<'a> {
    graph: &'a GraphForge,
    session_uuid: Uuid,
    inner: graphforge_storage::GraphConstructionSession,
    /// Keep the admitted source workspace stable while storage retains its capabilities.
    _parent_workspace: super::GraphWorkspace,
}

impl GraphForge {
    /// Begin a bounded construction pinned to the current committed graph.
    ///
    /// On an empty project the accepted chunks are spooled, each as one
    /// durable Arrow IPC file, and sealing builds the generation from them
    /// with the bulk builder. An accepted chunk survives a crash and resumes
    /// either way. A construction pinned to a non-empty graph stages its chunks.
    pub fn begin_graph_construction(
        &self,
        budgets: GraphConstructionBudgets,
    ) -> Result<GraphConstructionSession<'_>, GfError> {
        let mut session = self.open_graph_construction(Uuid::now_v7(), budgets, false)?;
        session.inner.spool_chunks();
        Ok(session)
    }

    /// Begin a construction whose chunks are staged rather than spooled: each
    /// accepted chunk becomes durable, authenticated Parquet and sorted runs,
    /// and sealing shapes and encodes them in a fixed window of memory, however
    /// large the graph. [`Self::begin_graph_construction`] builds an initial
    /// graph on the bulk builder, in memory or on bounded scratch files when it
    /// exceeds the memory budget, and stages only a build whose node tables
    /// alone exceed that budget. Use this to measure the staged lifecycle
    /// itself. Appends to a non-empty graph always stage.
    pub fn begin_staged_graph_construction(
        &self,
        budgets: GraphConstructionBudgets,
    ) -> Result<GraphConstructionSession<'_>, GfError> {
        self.open_graph_construction(Uuid::now_v7(), budgets, false)
    }

    /// Resume a construction using the opaque durable identifier returned at begin.
    pub fn resume_graph_construction(
        &self,
        session_uuid: Uuid,
        budgets: GraphConstructionBudgets,
    ) -> Result<GraphConstructionSession<'_>, GfError> {
        self.open_graph_construction(session_uuid, budgets, true)
    }

    /// Open or resume the storage session for `open_graph_construction`,
    /// selecting the semantic authority and lifecycle this facade carries.
    fn open_storage_construction(
        &self,
        dir: &std::path::Path,
        session_uuid: Uuid,
        parent_topology_generation: u64,
        budgets: GraphConstructionBudgets,
        resume: bool,
    ) -> Result<graphforge_storage::GraphConstructionSession, GfError> {
        let project = self.resolved_generation.container_root();
        let inner = if let Some(allocation) = &self.allocation_operation {
            let authority = if self.ontology_mode == OntologyMode::Exploratory {
                None
            } else {
                Some(graphforge_storage::ConstructionSemanticAuthority {
                    composition: self
                        .workspace_ontology_composition()?
                        .ok_or_else(|| validation("construction semantic composition is absent"))?,
                    bindings: self
                        .semantic_storage_bindings
                        .lock()
                        .expect("semantic storage binding lock poisoned")
                        .clone()
                        .ok_or_else(|| validation("construction semantic bindings are absent"))?,
                })
            };
            graphforge_storage::GraphConstructionSession::open_with_allocation(
                project,
                dir,
                session_uuid,
                parent_topology_generation,
                self.ontology_mode,
                authority,
                budgets,
                self.lifecycle_mode,
                resume,
                allocation,
            )?
        } else if self.ontology_mode == OntologyMode::Exploratory {
            if resume {
                graphforge_storage::GraphConstructionSession::resume_with_mode_and_lifecycle_from_graph(
                    project,
                    dir,
                    session_uuid,
                    self.ontology_mode,
                    budgets,
                    self.lifecycle_mode,
                )?
            } else {
                graphforge_storage::GraphConstructionSession::open_with_mode_and_lifecycle_from_graph(
                    project,
                    dir,
                    session_uuid,
                    parent_topology_generation,
                    self.ontology_mode,
                    budgets,
                    self.lifecycle_mode,
                )?
            }
        } else {
            let composition = self
                .workspace_ontology_composition()?
                .ok_or_else(|| validation("construction semantic composition is absent"))?;
            let bindings = self
                .semantic_storage_bindings
                .lock()
                .expect("semantic storage binding lock poisoned")
                .clone()
                .ok_or_else(|| validation("construction semantic bindings are absent"))?;
            let authority = graphforge_storage::ConstructionSemanticAuthority {
                composition,
                bindings,
            };
            if resume {
                graphforge_storage::GraphConstructionSession::resume_with_semantic_authority_and_lifecycle_from_graph(
                    project,
                    dir,
                    session_uuid,
                    self.ontology_mode,
                    budgets,
                    authority,
                    self.lifecycle_mode,
                )?
            } else {
                graphforge_storage::GraphConstructionSession::open_with_semantic_authority_and_lifecycle_from_graph(
                    project,
                    dir,
                    session_uuid,
                    parent_topology_generation,
                    self.ontology_mode,
                    budgets,
                    authority,
                    self.lifecycle_mode,
                )?
            }
        };
        Ok(inner)
    }

    fn open_graph_construction(
        &self,
        session_uuid: Uuid,
        budgets: GraphConstructionBudgets,
        resume: bool,
    ) -> Result<GraphConstructionSession<'_>, GfError> {
        if self.read_only {
            return Err(validation("historical graph views cannot construct"));
        }
        let _visibility = self.graph_visibility.read()?;
        let workspace = self.workspace_for_session();
        let dir = workspace.path();
        let parent_topology_generation = graphforge_storage::read_topology_generation(dir)?;
        let mut inner = self.open_storage_construction(
            dir,
            session_uuid,
            parent_topology_generation,
            budgets,
            resume,
        )?;
        // #1586: every import on this instance draws its parallel lanes from
        // one admission, so construction never takes the whole CPU budget.
        inner.set_cpu_admission(Some(std::sync::Arc::clone(
            &self.construction_cpu_admission,
        )));
        Ok(GraphConstructionSession {
            graph: self,
            session_uuid,
            inner,
            _parent_workspace: workspace,
        })
    }
}

impl GraphConstructionSession<'_> {
    /// Durable opaque identifier used to reopen this session.
    #[must_use]
    pub const fn session_uuid(&self) -> Uuid {
        self.session_uuid
    }

    /// Append one node Arrow chunk. Accepted property columns are normalized
    /// to their canonical persisted types first.
    pub fn append_nodes(
        &mut self,
        chunk_id: &str,
        batch: &RecordBatch,
    ) -> Result<ConstructionChunkReceipt, GfError> {
        self.append(
            graphforge_storage::ConstructionChunkKind::Node,
            chunk_id,
            batch,
            None,
        )
    }

    /// Append one node chunk while polling cooperative cancellation.
    pub fn append_nodes_with_cancellation(
        &mut self,
        chunk_id: &str,
        batch: &RecordBatch,
        cancellation: &crate::CancellationToken,
    ) -> Result<ConstructionChunkReceipt, GfError> {
        self.append(
            graphforge_storage::ConstructionChunkKind::Node,
            chunk_id,
            batch,
            Some(cancellation),
        )
    }

    /// Append one edge Arrow chunk after all node chunks. Accepted property
    /// columns are normalized to their canonical persisted types first.
    pub fn append_edges(
        &mut self,
        chunk_id: &str,
        batch: &RecordBatch,
    ) -> Result<ConstructionChunkReceipt, GfError> {
        self.append(
            graphforge_storage::ConstructionChunkKind::Edge,
            chunk_id,
            batch,
            None,
        )
    }

    /// Append one edge chunk while polling cooperative cancellation.
    pub fn append_edges_with_cancellation(
        &mut self,
        chunk_id: &str,
        batch: &RecordBatch,
        cancellation: &crate::CancellationToken,
    ) -> Result<ConstructionChunkReceipt, GfError> {
        self.append(
            graphforge_storage::ConstructionChunkKind::Edge,
            chunk_id,
            batch,
            Some(cancellation),
        )
    }

    /// The single construction boundary every bulk producer crosses: import
    /// sessions (CLI, Python, Node) and direct Rust construction alike.
    fn append(
        &mut self,
        kind: graphforge_storage::ConstructionChunkKind,
        chunk_id: &str,
        batch: &RecordBatch,
        cancellation: Option<&crate::CancellationToken>,
    ) -> Result<ConstructionChunkReceipt, GfError> {
        let batch = canonical_property_columns(kind, batch)?;
        match cancellation {
            Some(token) => self
                .inner
                .append_with_cancellation(kind, chunk_id, &batch, || token.is_cancelled()),
            None => self.inner.append(kind, chunk_id, &batch),
        }
    }

    /// Return durable content-free lifecycle and bounded-work progress.
    #[must_use]
    pub fn progress(&self) -> GraphConstructionProgress {
        GraphConstructionProgress {
            session_uuid: self.session_uuid,
            state: self.inner.state(),
            parent_topology_generation: self.inner.parent_topology_generation(),
            accepted_chunks: self.inner.accepted_chunks(),
            publication_committed: self.inner.publication_committed(),
            evidence: self.inner.reported_evidence(),
        }
    }

    /// Complete global shape and endpoint validation without publishing CURRENT.
    pub(crate) fn validate_and_seal(
        &mut self,
        cancellation: Option<&crate::CancellationToken>,
    ) -> Result<(), GfError> {
        self.prepare_encoding(cancellation)?;
        Ok(())
    }

    /// Seal, shape, encode, and atomically publish exactly one generation.
    pub fn seal_and_publish(&mut self) -> Result<GraphConstructionPublicationReceipt, GfError> {
        self.seal_and_publish_inner(None)
    }

    /// Seal and publish while polling cooperative cancellation before `CURRENT`.
    pub fn seal_and_publish_with_cancellation(
        &mut self,
        cancellation: &crate::CancellationToken,
    ) -> Result<GraphConstructionPublicationReceipt, GfError> {
        self.seal_and_publish_inner(Some(cancellation))
    }

    #[allow(clippy::too_many_lines)]
    fn seal_and_publish_inner(
        &mut self,
        cancellation: Option<&crate::CancellationToken>,
    ) -> Result<GraphConstructionPublicationReceipt, GfError> {
        if let Some(token) = cancellation {
            token.checkpoint()?;
        }
        let _visibility = self.graph.graph_visibility.lock()?;
        let _adjacency_visibility = self
            .graph
            .adjacency_visibility
            .write()
            .expect("adjacency visibility lock poisoned");
        let target = target_generation_uuid(self.session_uuid);
        let transaction = derived_uuid(self.session_uuid, b"transaction");
        let outcome = if let Some(replay) =
            self.inner
                .replay_committed_publication(target, transaction, || {
                    cancellation.is_some_and(crate::CancellationToken::is_cancelled)
                })? {
            let current = graphforge_storage::resolve_project_generation(
                self.graph.resolved_generation.container_root(),
            )?;
            if current.generation_uuid() != replay.generation_uuid {
                return Ok(GraphConstructionPublicationReceipt {
                    generation_uuid: replay.generation_uuid,
                    idempotent_replay: true,
                });
            }
            PublicationOutcome::FromCurrent(replay)
        } else {
            let prepare = RegionScope::named("prepare_encoding");
            let encoding = self.prepare_encoding(cancellation)?;
            drop(prepare);
            if let Some(token) = cancellation {
                token.checkpoint()?;
            }
            // Reader preparation runs against the durable candidate before
            // CURRENT. Failure leaves CURRENT unchanged, so a candidate that
            // cannot be hydrated or authorized never becomes visible; a
            // refresh that once had to recover a committed generation now
            // fails closed before the commit point.
            let graph = self.graph;
            let mut prepared: Option<PreparedGenerationRefresh> = None;
            let mut prepare_readers =
                |candidate: &graphforge_storage::ResolvedProjectGeneration| -> Result<(), GfError> {
                    refresh_boundary(RefreshBoundary::BeforeHydrate)?;
                    if candidate.generation_uuid() != target {
                        return Err(GfError::Storage(
                            "durable publication candidate differs from the construction target"
                                .into(),
                        ));
                    }
                    let hydration = RegionScope::named("hydration");
                    let (prepared_dir, prepared_guard, hydration_evidence) =
                        super::hydrate_graph_workspace(candidate, false)?;
                    drop(hydration);
                    refresh_boundary(RefreshBoundary::AfterHydrate)?;
                    let read_authority = RegionScope::named("read_authority");
                    let runtime_catalog = super::load_runtime_catalog(&prepared_dir)?;
                    let read_authority_prepared =
                        graph.prepare_generation_read_authority(candidate, &prepared_dir)?;
                    drop(read_authority);
                    prepared = Some(PreparedGenerationRefresh {
                        workspace: super::GraphWorkspace::new(
                            prepared_dir,
                            prepared_guard,
                            &read_authority_prepared.properties,
                        )?,
                        runtime_catalog,
                        read_authority: read_authority_prepared,
                        hydration_evidence,
                    });
                    Ok(())
                };
            let published = self.inner.publish_canonical_with_cancellation(
                &encoding,
                target,
                transaction,
                || cancellation.is_some_and(crate::CancellationToken::is_cancelled),
                Some(&mut prepare_readers),
            )?;
            match prepared {
                Some(refreshed) => {
                    let refreshed = Box::new(refreshed);
                    self.inner
                        .record_hydration_evidence(&refreshed.hydration_evidence)?;
                    // The commit point is behind us: CURRENT already names the
                    // published generation, so an install-boundary failure
                    // keeps the committed-generation recovery contract.
                    refresh_boundary(RefreshBoundary::BeforeInstall).map_err(
                        |error: GfError| {
                            GfError::Storage(format!(
                                "phase=POST_PUBLICATION_REFRESH committed=true generation_uuid={} recovery=reopen_or_resume cause={error}",
                                published.generation_uuid
                            ))
                        },
                    )?;
                    PublicationOutcome::Prepared(published, refreshed)
                }
                // The publisher replayed an already-published transaction
                // without invoking preparation; refresh from CURRENT below.
                None => PublicationOutcome::FromCurrent(published),
            }
        };
        let (published, refreshed) = match outcome {
            PublicationOutcome::Prepared(published, refreshed) => (published, refreshed),
            PublicationOutcome::FromCurrent(published) => {
                let graph = self.graph;
                let inner = &mut self.inner;
                let refresh = (|| {
                    refresh_boundary(RefreshBoundary::BeforeHydrate)?;
                    let hydration = RegionScope::named("hydration");
                    let resolved = graphforge_storage::resolve_project_generation(
                        graph.resolved_generation.container_root(),
                    )?;
                    if resolved.generation_uuid() != published.generation_uuid {
                        return Err(GfError::Storage(
                            "construction publication did not resolve its exact generation".into(),
                        ));
                    }
                    let (prepared_dir, prepared_guard, hydration_evidence) =
                        super::hydrate_graph_workspace(&resolved, false)?;
                    inner.record_hydration_evidence(&hydration_evidence)?;
                    drop(hydration);
                    refresh_boundary(RefreshBoundary::AfterHydrate)?;
                    let read_authority = RegionScope::named("read_authority");
                    let runtime_catalog = super::load_runtime_catalog(&prepared_dir)?;
                    let read_authority_prepared =
                        graph.prepare_generation_read_authority(&resolved, &prepared_dir)?;
                    drop(read_authority);
                    refresh_boundary(RefreshBoundary::BeforeInstall)?;
                    Ok(PreparedGenerationRefresh {
                        workspace: super::GraphWorkspace::new(
                            prepared_dir,
                            prepared_guard,
                            &read_authority_prepared.properties,
                        )?,
                        runtime_catalog,
                        read_authority: read_authority_prepared,
                        hydration_evidence,
                    })
                })();
                let refreshed = refresh.map_err(|error: GfError| {
                    GfError::Storage(format!(
                        "phase=POST_PUBLICATION_REFRESH committed=true generation_uuid={} recovery=reopen_or_resume cause={error}",
                        published.generation_uuid
                    ))
                })?;
                (published, Box::new(refreshed))
            }
        };
        // Readers retain stable paths and capabilities. Never rename a workspace
        // pinned by an ordinal handle or an unconsumed stream (including Windows).
        // All fallible preparation precedes this transition under write visibility.
        let old_workspace = self.graph.replace_workspace_owner(refreshed.workspace);
        *self
            .graph
            .runtime_catalog
            .lock()
            .expect("runtime catalog poisoned") = refreshed.runtime_catalog;
        self.graph.install_prepared_generation_read_authority(
            published.generation_uuid,
            refreshed.read_authority,
        );
        *self
            .graph
            .identity_probe
            .lock()
            .expect("UUID membership lock poisoned") = None;
        // Release old reader handles before their workspace; streams own their pins.
        drop(old_workspace);
        Ok(GraphConstructionPublicationReceipt {
            generation_uuid: published.generation_uuid,
            idempotent_replay: published.idempotent_replay,
        })
    }

    /// Build the generation from the planned sources with the bulk builder
    /// (#1883) and seal it. Equivalent to staging every row and calling
    /// [`Self::validate_and_seal`], without staging.
    pub(crate) fn build_initial(
        &mut self,
        plan: &graphforge_storage::BulkBuildPlan<'_>,
        cancellation: Option<&crate::CancellationToken>,
    ) -> Result<graphforge_storage::BulkBuildReport, GfError> {
        let topology_generation = self.inner.parent_topology_generation().saturating_add(1);
        self.inner
            .prepare_bulk_encoding(topology_generation, plan, || {
                cancellation.is_some_and(crate::CancellationToken::is_cancelled)
            })?;
        Ok(self.inner.bulk_build_report())
    }

    fn prepare_encoding(
        &mut self,
        cancellation: Option<&crate::CancellationToken>,
    ) -> Result<graphforge_storage::GraphConstructionEncoding, GfError> {
        let topology_generation = self.inner.parent_topology_generation().saturating_add(1);
        if self.inner.is_spooled() {
            // The route is recorded before any work and read back on every
            // retry: a retry never re-decides from live memory.
            let budget = crate::import_session::bulk_source::bulk_build_memory_budget()?;
            let route = if let Some(route) = self.inner.seal_route() {
                route
            } else {
                let route = self.inner.spool_seal_route(budget)?;
                self.inner.record_seal_route(route)?
            };
            let cancelled = || cancellation.is_some_and(crate::CancellationToken::is_cancelled);
            match route {
                graphforge_storage::SealRoute::Bulk => {
                    return self.inner.prepare_spooled_bulk_encoding(
                        topology_generation,
                        budget,
                        cancelled,
                    );
                }
                graphforge_storage::SealRoute::ReplayStaged => {
                    self.inner.replay_spool_to_staged(cancelled)?;
                }
            }
        }
        if self.inner.state() == GraphConstructionState::Staging {
            self.inner
                .seal_and_prepare_canonical_encoding_with_cancellation(topology_generation, || {
                    cancellation.is_some_and(crate::CancellationToken::is_cancelled)
                })
        } else {
            self.inner
                .prepare_canonical_encoding_with_cancellation(topology_generation, || {
                    cancellation.is_some_and(crate::CancellationToken::is_cancelled)
                })
        }
    }

    /// Abort an unsealed session without changing project authority.
    pub fn abort(&mut self) -> Result<(), GfError> {
        self.inner.abort()
    }

    /// Reclaim every authenticated private artifact of an unpublished session.
    pub(crate) fn discard(self) -> Result<(), GfError> {
        self.inner.discard()
    }
}

/// How a completed publication delivers its reader refresh.
enum PublicationOutcome {
    /// CURRENT already names the replayed target; refresh from CURRENT with
    /// the committed generation's recovery contract.
    FromCurrent(graphforge_storage::ProjectPublicationReceipt),
    /// Fresh publication; readers were prepared against the durable candidate
    /// before CURRENT, so installation alone remains.
    Prepared(
        graphforge_storage::ProjectPublicationReceipt,
        Box<PreparedGenerationRefresh>,
    ),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RefreshBoundary {
    BeforeHydrate,
    AfterHydrate,
    BeforeInstall,
}

/// Reader workspace and authorities prepared for one publication outcome,
/// ready to install atomically under write visibility.
struct PreparedGenerationRefresh {
    workspace: super::GraphWorkspace,
    runtime_catalog: super::RuntimeCatalog,
    read_authority: super::PreparedGenerationReadAuthority,
    hydration_evidence: graphforge_storage::GraphFilesOpenEvidence,
}

#[cfg(not(test))]
#[allow(
    clippy::unnecessary_wraps,
    reason = "test and production boundary hooks intentionally share one fallible signature"
)]
fn refresh_boundary(_: RefreshBoundary) -> Result<(), GfError> {
    Ok(())
}

#[cfg(test)]
fn refresh_boundary(boundary: RefreshBoundary) -> Result<(), GfError> {
    let requested = REFRESH_FAILURE.with(std::cell::Cell::get);
    if requested == Some(boundary) {
        return Err(GfError::Storage(format!(
            "injected construction refresh failure at {boundary:?}"
        )));
    }
    Ok(())
}

#[cfg(test)]
thread_local! {
    static REFRESH_FAILURE: std::cell::Cell<Option<RefreshBoundary>> = const { std::cell::Cell::new(None) };
}

/// Rewrite every accepted property column into its canonical persisted type
/// (`graphforge_storage::schemas::canonical_property_data_type`): narrower
/// integers to `Int64`, `Float32` to `Float64`, `LargeUtf8` to `Utf8` and
/// `LargeList` to `List`, recursively. Each conversion is lossless and keeps
/// nulls, field names, nullability and metadata. Columns that are already
/// canonical, required topology columns, and columns no canonical form exists
/// for are passed through unchanged, so storage refuses the last.
pub(crate) fn canonical_property_columns(
    kind: graphforge_storage::ConstructionChunkKind,
    batch: &RecordBatch,
) -> Result<RecordBatch, GfError> {
    let required = match kind {
        graphforge_storage::ConstructionChunkKind::Node => {
            graphforge_storage::CONSTRUCTION_NODE_SCHEMA.fields().len()
        }
        graphforge_storage::ConstructionChunkKind::Edge => {
            graphforge_storage::CONSTRUCTION_EDGE_SCHEMA.fields().len()
        }
    };
    let schema = batch.schema();
    let target = |field: &arrow::datatypes::Field| {
        graphforge_storage::schemas::canonical_property_data_type(field.data_type())
            .filter(|canonical| canonical != field.data_type())
    };
    if schema
        .fields()
        .iter()
        .skip(required)
        .all(|field| target(field).is_none())
    {
        return Ok(batch.clone());
    }
    let lossless = arrow::compute::CastOptions {
        safe: false,
        ..arrow::compute::CastOptions::default()
    };
    let mut fields = Vec::with_capacity(schema.fields().len());
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (index, (field, column)) in schema.fields().iter().zip(batch.columns()).enumerate() {
        if let Some(canonical) = (index >= required).then(|| target(field)).flatten() {
            let normalized = arrow::compute::cast_with_options(column, &canonical, &lossless)
                .map_err(|error| {
                    validation(format!(
                        "property column {} cannot be normalized from {} to {canonical}: {error}",
                        field.name(),
                        field.data_type()
                    ))
                })?;
            fields.push(std::sync::Arc::new(
                field.as_ref().clone().with_data_type(canonical),
            ));
            columns.push(normalized);
        } else {
            fields.push(std::sync::Arc::clone(field));
            columns.push(std::sync::Arc::clone(column));
        }
    }
    RecordBatch::try_new(
        std::sync::Arc::new(arrow::datatypes::Schema::new_with_metadata(
            fields,
            schema.metadata().clone(),
        )),
        columns,
    )
    .map_err(|error| validation(error.to_string()))
}

/// The generation a construction session publishes: the one `CURRENT` names
/// once its publication has swapped, whether or not the session knows it.
pub(crate) fn target_generation_uuid(session: Uuid) -> Uuid {
    derived_uuid(session, b"generation")
}

fn derived_uuid(operation: Uuid, domain: &[u8]) -> Uuid {
    let mut digest = Sha256::new();
    digest.update(b"graphforge-construction-publication/v1\0");
    digest.update(operation.as_bytes());
    digest.update(domain);
    graphforge_core::canonical::uuid_v8(digest.finalize().into())
}

fn validation(message: impl Into<String>) -> GfError {
    GfError::Validation(message.into())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, FixedSizeBinaryArray, StringArray};
    use arrow::record_batch::RecordBatch;

    use super::*;

    fn nodes(ids: &[Uuid]) -> RecordBatch {
        let uuids =
            FixedSizeBinaryArray::try_from_iter(ids.iter().map(|uuid| uuid.as_bytes().as_slice()))
                .unwrap();
        RecordBatch::try_new(
            graphforge_storage::CONSTRUCTION_NODE_SCHEMA.clone(),
            vec![
                Arc::new(uuids) as ArrayRef,
                Arc::new(StringArray::from(vec!["Person"; ids.len()])) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn edges(ids: &[Uuid], endpoints: &[(Uuid, Uuid)]) -> RecordBatch {
        let edge_uuids =
            FixedSizeBinaryArray::try_from_iter(ids.iter().map(|uuid| uuid.as_bytes().as_slice()))
                .unwrap();
        let sources = FixedSizeBinaryArray::try_from_iter(
            endpoints
                .iter()
                .map(|(source, _)| source.as_bytes().as_slice()),
        )
        .unwrap();
        let targets = FixedSizeBinaryArray::try_from_iter(
            endpoints
                .iter()
                .map(|(_, target)| target.as_bytes().as_slice()),
        )
        .unwrap();
        RecordBatch::try_new(
            graphforge_storage::CONSTRUCTION_EDGE_SCHEMA.clone(),
            vec![
                Arc::new(edge_uuids) as ArrayRef,
                Arc::new(StringArray::from(vec!["KNOWS"; ids.len()])) as ArrayRef,
                Arc::new(sources) as ArrayRef,
                Arc::new(targets) as ArrayRef,
            ],
        )
        .unwrap()
    }

    /// A published construction ships its adjacency CSR (#1388): a fresh
    /// process finds it current in the hydrated workspace without rebuilding,
    /// hop queries answer from it, and a corrupted published shard is never
    /// served. Opening reads no shard, so the refusal is on the shard's first
    /// touch: the shard reader refuses it by checksum and the provider answers
    /// the hop from authenticated topology instead.
    #[test]
    fn published_construction_serves_its_adjacency_index_and_refuses_corruption() {
        use graphforge_storage::adjacency::AdjacencyFreshnessState;

        let directory = tempfile::TempDir::new().unwrap();
        let path = directory.path().to_str().unwrap();
        let graph = GraphForge::new(Some(path)).unwrap();
        let node_ids: Vec<Uuid> = (0..4).map(|_| Uuid::now_v7()).collect();
        let edge_ids: Vec<Uuid> = (0..3).map(|_| Uuid::now_v7()).collect();
        let mut session = graph.begin_graph_construction(Default::default()).unwrap();
        session.append_nodes("nodes", &nodes(&node_ids)).unwrap();
        session
            .append_edges(
                "edges",
                &edges(
                    &edge_ids,
                    &[
                        (node_ids[0], node_ids[1]),
                        (node_ids[1], node_ids[2]),
                        (node_ids[2], node_ids[3]),
                    ],
                ),
            )
            .unwrap();
        session.seal_and_publish().unwrap();
        drop(session);
        let inspection = graph.inspect_adjacency().unwrap();
        assert_eq!(inspection.state, AdjacencyFreshnessState::Current);
        assert_eq!(inspection.artifact_source_generation, Some(1));
        drop(graph);

        let hop_count = |graph: &GraphForge| {
            graph
                .execute("MATCH (a)-[r]->(b)-[s]->(c) RETURN count(*) AS n")
                .unwrap()
                .batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .value(0)
        };
        let reopened = GraphForge::new(Some(path)).unwrap();
        assert!(graphforge_storage::adjacency::manifest_path(&reopened.dir()).is_file());
        let inspection = reopened.inspect_adjacency().unwrap();
        assert_eq!(inspection.state, AdjacencyFreshnessState::Current);
        assert_eq!(inspection.artifact_source_generation, Some(1));
        assert_construction_relationships(
            &reopened,
            &[
                [node_ids[0], edge_ids[0], node_ids[1]],
                [node_ids[1], edge_ids[1], node_ids[2]],
                [node_ids[2], edge_ids[2], node_ids[3]],
            ],
        );
        assert_eq!(hop_count(&reopened), 2);
        drop(reopened);

        let generation = graphforge_storage::resolve_project_generation(directory.path()).unwrap();
        let inventory = generation.graph_files_inventory().unwrap().unwrap();
        let shard = inventory
            .files
            .iter()
            .find(|entry| {
                entry.relative_path.starts_with("indexes/adjacency/")
                    && entry.relative_path.ends_with(".csr")
            })
            .expect("published CSR shard");
        let object = graphforge_storage::graph_object_path(
            generation.container_root(),
            &shard.content_sha256,
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&object).unwrap().permissions();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            permissions.set_mode(0o600);
        }
        #[cfg(not(unix))]
        permissions.set_readonly(false);
        std::fs::set_permissions(&object, permissions).unwrap();
        let mut bytes = std::fs::read(&object).unwrap();
        bytes[0] ^= 1;
        std::fs::write(&object, bytes).unwrap();
        // The full-admission API still names the corrupted object.
        let fresh = graphforge_storage::resolve_project_generation(directory.path()).unwrap();
        assert!(fresh.graph_files_inventory().is_err());
        drop(fresh);

        // Opening reads no shard payload, so it succeeds.
        let corrupted = GraphForge::new(Some(path)).unwrap();
        // The shard's own reader refuses it on first touch.
        let mut refused = 0;
        for manifest in inventory
            .files
            .iter()
            .filter(|entry| entry.relative_path.ends_with(".csr.json"))
        {
            let logical = corrupted
                .dir()
                .join(manifest.relative_path.trim_end_matches(".json"));
            let index = graphforge_storage::adjacency::ShardedCsrIndex::open(&logical).unwrap();
            for node in 0..index.node_count() {
                if let Err(error) = index.row(node) {
                    assert!(error.to_string().contains("checksum mismatch"), "{error}");
                    refused += 1;
                    break;
                }
            }
        }
        // Identical shard bytes are one content-addressed object linked at
        // several logical paths, so every path that names it is refused.
        assert!(refused >= 1, "the corrupted shard must be refused");
        // Hop queries stay correct: a refused shard is never served.
        assert_eq!(hop_count(&corrupted), 2);
    }

    #[test]
    fn cancelled_public_construction_preserves_current() {
        let graph = GraphForge::new(None).unwrap();
        let root = graph.resolved_generation.container_root().to_path_buf();
        let parent = graphforge_storage::resolve_project_generation(&root)
            .unwrap()
            .generation_uuid();
        let mut session = graph.begin_graph_construction(Default::default()).unwrap();
        let first = Uuid::now_v7();
        let second = Uuid::now_v7();
        let cancellation = crate::CancellationToken::new();
        session
            .append_nodes_with_cancellation("nodes", &nodes(&[first, second]), &cancellation)
            .unwrap();
        session
            .append_edges_with_cancellation(
                "edges",
                &edges(&[Uuid::now_v7()], &[(first, second)]),
                &cancellation,
            )
            .unwrap();
        cancellation.cancel();
        let error = session
            .seal_and_publish_with_cancellation(&cancellation)
            .unwrap_err();
        assert_eq!(error.code(), "GF_CANCELLED");
        assert_eq!(
            graphforge_storage::resolve_project_generation(&root)
                .unwrap()
                .generation_uuid(),
            parent
        );
    }

    #[test]
    fn pre_publication_reader_preparation_failure_fails_closed() {
        for boundary in [
            RefreshBoundary::BeforeHydrate,
            RefreshBoundary::AfterHydrate,
        ] {
            let directory = tempfile::TempDir::new().unwrap();
            let path = directory.path().to_str().unwrap().to_owned();
            let graph = GraphForge::new(Some(&path)).unwrap();
            let root = graph.resolved_generation.container_root().to_path_buf();
            let parent = graphforge_storage::resolve_project_generation(&root)
                .unwrap()
                .generation_uuid();
            let old_workspace = graph.workspace_for_session();
            let old_generation = *graph.current_generation_uuid.lock().unwrap();
            let mut session = graph.begin_graph_construction(Default::default()).unwrap();
            session
                .append_nodes("nodes", &nodes(&[Uuid::now_v7()]))
                .unwrap();
            REFRESH_FAILURE.with(|failure| failure.set(Some(boundary)));
            let error = session.seal_and_publish().unwrap_err();
            REFRESH_FAILURE.with(|failure| failure.set(None));
            // The commit point was never crossed: CURRENT is unchanged and
            // the candidate never became visible.
            assert!(!session.progress().publication_committed);
            let session_uuid = session.progress().session_uuid;
            assert_eq!(
                graphforge_storage::resolve_project_generation(&root)
                    .unwrap()
                    .generation_uuid(),
                parent
            );
            let message = error.to_string();
            assert!(message.contains("committed=false"), "{message}");
            assert_eq!(old_workspace.path(), graph.workspace_for_session().path());
            assert_eq!(
                *graph.current_generation_uuid.lock().unwrap(),
                old_generation
            );
            assert_eq!(graph.node_count("Person").unwrap(), 0);
            // The interrupted attempt leaves a PREPARING transaction journal;
            // the documented recovery path is reopen (recovery runs on open),
            // then resume the session and publish.
            drop(session);
            drop(graph);
            let reopened = GraphForge::new(Some(&path)).unwrap();
            assert_eq!(
                graphforge_storage::resolve_project_generation(
                    &reopened.resolved_generation.container_root()
                )
                .unwrap()
                .generation_uuid(),
                parent
            );
            let mut session = reopened
                .resume_graph_construction(session_uuid, Default::default())
                .unwrap();
            let retry = session.seal_and_publish().unwrap();
            assert!(!retry.idempotent_replay);
            assert_ne!(retry.generation_uuid, parent);
            assert_eq!(reopened.node_count("Person").unwrap(), 1);
            assert_ne!(
                old_workspace.path(),
                reopened.workspace_for_session().path()
            );
        }
    }

    #[test]
    fn before_install_refresh_failure_reports_committed_authority() {
        let graph = GraphForge::new(None).unwrap();
        let root = graph.resolved_generation.container_root().to_path_buf();
        let old_workspace = graph.workspace_for_session();
        let old_generation = *graph.current_generation_uuid.lock().unwrap();
        let mut session = graph.begin_graph_construction(Default::default()).unwrap();
        session
            .append_nodes("nodes", &nodes(&[Uuid::now_v7()]))
            .unwrap();
        REFRESH_FAILURE.with(|failure| failure.set(Some(RefreshBoundary::BeforeInstall)));
        let error = session.seal_and_publish().unwrap_err();
        REFRESH_FAILURE.with(|failure| failure.set(None));
        assert!(session.progress().publication_committed);
        let committed = graphforge_storage::resolve_project_generation(&root)
            .unwrap()
            .generation_uuid();
        let message = error.to_string();
        assert!(message.contains("phase=POST_PUBLICATION_REFRESH"));
        assert!(message.contains("committed=true"));
        assert!(message.contains(&format!("generation_uuid={committed}")));
        assert!(message.contains("recovery=reopen_or_resume"));
        assert_eq!(old_workspace.path(), graph.workspace_for_session().path());
        assert_eq!(
            *graph.current_generation_uuid.lock().unwrap(),
            old_generation
        );
        assert_eq!(graph.node_count("Person").unwrap(), 0);
        let retry = session.seal_and_publish().unwrap();
        assert!(retry.idempotent_replay);
        assert_eq!(retry.generation_uuid, committed);
        assert_eq!(graph.node_count("Person").unwrap(), 1);
        assert_ne!(old_workspace.path(), graph.workspace_for_session().path());
    }

    fn relationship_identities(batches: &[RecordBatch]) -> std::collections::BTreeSet<[Uuid; 3]> {
        let identities: std::collections::BTreeSet<_> = batches
            .iter()
            .flat_map(|batch| {
                let columns: Vec<_> = (0..3)
                    .map(|column| {
                        batch
                            .column(column)
                            .as_any()
                            .downcast_ref::<FixedSizeBinaryArray>()
                            .unwrap()
                    })
                    .collect();
                for column in &columns {
                    assert_eq!(column.null_count(), 0);
                }
                (0..batch.num_rows())
                    .map(|row| {
                        std::array::from_fn(|column| {
                            Uuid::from_slice(columns[column].value(row)).unwrap()
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(
            identities.len(),
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            "relationship rows must not be duplicated"
        );
        identities
    }

    const RELATIONSHIP_IDENTITIES: &str = "MATCH (a)-[r:KNOWS]->(b) RETURN a.node_uuid AS source, r.edge_uuid AS edge, b.node_uuid AS target";

    fn assert_construction_relationships(graph: &GraphForge, expected: &[[Uuid; 3]]) {
        let result = graph.execute(RELATIONSHIP_IDENTITIES).unwrap();
        assert_eq!(
            relationship_identities(&result.batches),
            expected.iter().copied().collect()
        );
        for query in [
            "MATCH ()-[r:KNOWS]->() RETURN count(r) AS n",
            "MATCH ()-[r:KNOWS]->() RETURN count(*) AS n",
        ] {
            let result = graph.execute(query).unwrap();
            assert_eq!(
                result
                    .batches
                    .iter()
                    .map(RecordBatch::num_rows)
                    .sum::<usize>(),
                1
            );
            assert_eq!(result.batches[0].column(0).null_count(), 0);
            assert_eq!(
                result.batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap()
                    .value(0),
                expected.len() as i64
            );
        }
    }

    #[test]
    fn graphforge_multi_chunk_construction_publishes_once_and_reopens() {
        let graph = GraphForge::new(None).unwrap();
        let root = graph.resolved_generation.container_root().to_path_buf();
        let parent = graphforge_storage::resolve_project_generation(&root)
            .unwrap()
            .generation_uuid();
        let budgets = graphforge_storage::GraphConstructionBudgets::default();
        let mut aborted = graph.begin_graph_construction(budgets).unwrap();
        aborted.abort().unwrap();
        assert_eq!(
            aborted.progress().state,
            graphforge_storage::GraphConstructionState::Aborted
        );
        assert_eq!(
            graphforge_storage::resolve_project_generation(&root)
                .unwrap()
                .generation_uuid(),
            parent
        );
        drop(aborted);

        let node_ids = [Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7()];
        let edge_ids = [Uuid::now_v7(), Uuid::now_v7()];

        let mut session = graph.begin_graph_construction(budgets).unwrap();
        let session_uuid = session.session_uuid();
        session
            .append_nodes("nodes-a", &nodes(&node_ids[..2]))
            .unwrap();
        session
            .append_nodes("nodes-b", &nodes(&node_ids[2..]))
            .unwrap();
        session
            .append_edges(
                "edges",
                &edges(
                    &edge_ids,
                    &[(node_ids[0], node_ids[1]), (node_ids[1], node_ids[2])],
                ),
            )
            .unwrap();
        let progress = session.progress();
        assert_eq!(progress.session_uuid, session_uuid);
        assert_eq!(progress.accepted_chunks, 3);
        assert_eq!(progress.evidence.input_rows, 5);

        let first = session.seal_and_publish().unwrap();
        assert!(!first.idempotent_replay);
        assert_ne!(first.generation_uuid, parent);
        assert_eq!(
            graphforge_storage::resolve_project_generation(&root)
                .unwrap()
                .generation_uuid(),
            first.generation_uuid
        );
        drop(session);
        let original_relationships = [
            [node_ids[0], edge_ids[0], node_ids[1]],
            [node_ids[1], edge_ids[1], node_ids[2]],
        ];
        assert_construction_relationships(&graph, &original_relationships);

        let mut resumed = graph
            .resume_graph_construction(session_uuid, budgets)
            .unwrap();
        let replay = resumed.seal_and_publish().unwrap();
        assert_eq!(replay.generation_uuid, first.generation_uuid);
        assert!(replay.idempotent_replay);
        assert_eq!(
            graphforge_storage::resolve_project_generation(&root)
                .unwrap()
                .generation_uuid(),
            first.generation_uuid
        );
        drop(resumed);
        assert_construction_relationships(&graph, &original_relationships);

        let index = graphforge_storage::TopologyIdentityProbe::open_dir(&graph.dir()).unwrap();
        assert_eq!(index.count(graphforge_storage::UuidIndexKind::Node), 3);
        assert_eq!(index.count(graphforge_storage::UuidIndexKind::Edge), 2);
        let catalog = graph.runtime_catalog.lock().unwrap();
        assert!(catalog.contains_entity_type("Person"));
        assert!(catalog.contains_relation_type("KNOWS"));
        drop(catalog);

        let first_inventory = graphforge_storage::resolve_project_generation(&root)
            .unwrap()
            .graph_files_inventory()
            .unwrap()
            .unwrap();
        let mut rejected = graph.begin_graph_construction(budgets).unwrap();
        rejected
            .append_nodes("duplicate-parent-node", &nodes(&node_ids[..1]))
            .unwrap();
        assert!(rejected.seal_and_publish().is_err());
        assert_eq!(
            graphforge_storage::resolve_project_generation(&root)
                .unwrap()
                .generation_uuid(),
            first.generation_uuid
        );
        drop(rejected);

        // Keep the lazy old-generation stream unpolled while child construction
        // retires both private predecessors and advances CURRENT.
        let old_stream = graph
            .execute_stream("MATCH (n:Person) RETURN n.node_uuid")
            .unwrap();
        let original_workspace = graph.workspace_for_session();
        let (old_relationship_stream, _, old_runtime_guard) = graph
            .execute_stream_owned(RELATIONSHIP_IDENTITIES, &std::collections::HashMap::new())
            .unwrap();
        assert_eq!(
            original_workspace.path(),
            old_runtime_guard.workspace.path()
        );
        let added_node = Uuid::now_v7();
        let added_edge = Uuid::now_v7();
        let mut child = graph.begin_graph_construction(budgets).unwrap();
        let child_session_uuid = child.session_uuid();
        child
            .append_nodes("child-node", &nodes(&[added_node]))
            .unwrap();
        child
            .append_edges(
                "child-edge",
                &edges(&[added_edge], &[(node_ids[2], added_node)]),
            )
            .unwrap();
        let child_receipt = child.seal_and_publish().unwrap();
        assert_ne!(child_receipt.generation_uuid, first.generation_uuid);
        assert_eq!(
            child
                .progress()
                .evidence
                .current_merge_temporary_allocated_bytes,
            0
        );
        drop(child);
        use futures::TryStreamExt;
        let old_batches: Vec<RecordBatch> = graph
            .block_on(async {
                old_stream
                    .try_collect()
                    .await
                    .map_err(|error| GfError::Storage(format!("{error}")))
            })
            .unwrap();
        let old_ids: std::collections::BTreeSet<Uuid> = old_batches
            .iter()
            .flat_map(|batch| {
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap();
                (0..batch.num_rows())
                    .map(|row| Uuid::from_slice(ids.value(row)).unwrap())
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(old_ids, node_ids.into_iter().collect());
        assert_ne!(original_workspace.path(), graph.dir().path());
        assert!(original_workspace.path().is_dir());
        let old_relationship_batches: Vec<RecordBatch> = old_runtime_guard
            .block_on(async {
                old_relationship_stream
                    .try_collect()
                    .await
                    .map_err(|error| GfError::Storage(format!("{error}")))
            })
            .unwrap();
        assert_eq!(
            relationship_identities(&old_relationship_batches),
            original_relationships.into_iter().collect()
        );
        let current_relationships = [
            original_relationships[0],
            original_relationships[1],
            [node_ids[2], added_edge, added_node],
        ];
        assert_construction_relationships(&graph, &current_relationships);
        let reopened = GraphForge::new(root.to_str()).unwrap();
        assert_construction_relationships(&reopened, &current_relationships);

        let resolved_child = graphforge_storage::resolve_project_generation(&root).unwrap();
        assert_eq!(
            resolved_child.generation_uuid(),
            child_receipt.generation_uuid
        );
        let child_inventory = resolved_child.graph_files_inventory().unwrap().unwrap();
        let marker_path = "topology/runtime_entity_label_encoding.json";
        let parent_marker = first_inventory
            .files
            .iter()
            .find(|entry| entry.relative_path == marker_path)
            .unwrap();
        let child_marker = child_inventory
            .files
            .iter()
            .find(|entry| entry.relative_path == marker_path)
            .unwrap();
        assert_eq!(parent_marker.content_sha256, child_marker.content_sha256);
        assert_eq!(parent_marker.byte_length, child_marker.byte_length);
        assert!(first_inventory.files.iter().any(|parent_entry| {
            child_inventory.files.iter().any(|child_entry| {
                parent_entry.content_sha256 == child_entry.content_sha256
                    && parent_entry.byte_length == child_entry.byte_length
            })
        }));

        let child_index =
            graphforge_storage::TopologyIdentityProbe::open_dir(&graph.dir()).unwrap();
        assert_eq!(
            child_index.count(graphforge_storage::UuidIndexKind::Node),
            4
        );
        assert_eq!(
            child_index.count(graphforge_storage::UuidIndexKind::Edge),
            3
        );
        let child_catalog = graph.runtime_catalog.lock().unwrap();
        assert!(child_catalog.contains_entity_type("Person"));
        assert!(child_catalog.contains_relation_type("KNOWS"));
        drop(child_catalog);

        let mut child_replay = graph
            .resume_graph_construction(child_session_uuid, budgets)
            .unwrap();
        let replay_receipt = child_replay.seal_and_publish().unwrap();
        assert_eq!(
            replay_receipt.generation_uuid,
            child_receipt.generation_uuid
        );
        assert!(replay_receipt.idempotent_replay);
        assert_eq!(
            graphforge_storage::resolve_project_generation(&root)
                .unwrap()
                .generation_uuid(),
            child_receipt.generation_uuid
        );
        drop(child_replay);
        let mut historical_replay = graph
            .resume_graph_construction(session_uuid, budgets)
            .unwrap();
        let historical = historical_replay.seal_and_publish().unwrap();
        assert!(historical.idempotent_replay);
        assert_eq!(historical.generation_uuid, first.generation_uuid);
        assert_eq!(
            graphforge_storage::resolve_project_generation(&root)
                .unwrap()
                .generation_uuid(),
            child_receipt.generation_uuid
        );
        drop(historical_replay);
        assert_construction_relationships(&graph, &current_relationships);
        let current_index =
            graphforge_storage::TopologyIdentityProbe::open_dir(&graph.dir()).unwrap();
        assert_eq!(
            current_index.count(graphforge_storage::UuidIndexKind::Node),
            4
        );
        assert_eq!(
            current_index.count(graphforge_storage::UuidIndexKind::Edge),
            3
        );
        drop(current_index);
        let reopened = GraphForge::new(root.to_str()).unwrap();
        let final_node = Uuid::now_v7();
        let final_edge = Uuid::now_v7();
        let mut next = reopened.begin_graph_construction(budgets).unwrap();
        next.append_nodes("reopened-node", &nodes(&[final_node]))
            .unwrap();
        next.append_edges(
            "reopened-edge",
            &edges(&[final_edge], &[(added_node, final_node)]),
        )
        .unwrap();
        let cancelled = crate::CancellationToken::new();
        cancelled.cancel();
        assert_eq!(
            next.seal_and_publish_with_cancellation(&cancelled)
                .unwrap_err()
                .code(),
            "GF_CANCELLED"
        );
        assert_construction_relationships(&reopened, &current_relationships);
        next.seal_and_publish().unwrap();
        let final_relationships = [
            current_relationships[0],
            current_relationships[1],
            current_relationships[2],
            [added_node, final_edge, final_node],
        ];
        assert_construction_relationships(&reopened, &final_relationships);
        assert_construction_relationships(
            &GraphForge::new(root.to_str()).unwrap(),
            &final_relationships,
        );
    }

    #[test]
    fn construction_application_reads_reconcile_and_scale_at_one_two_four() {
        let mut observations = Vec::new();
        // Hydration no longer owns node-linear reads (#1388): the forward and
        // ordinal identity runs are hard-linked, not copied and verified, so
        // its reads are the small controls and may not grow with the rows.
        let mut hydration_reads = Vec::new();
        // CAS install reads, by staged payload bytes. Windows copies every
        // object so they follow the payload; elsewhere the install links the
        // encoder's file and they are only the small manifest controls (#1899).
        let mut cas_reads = Vec::new();
        // Each node retains 16 identity bytes and at least 18 compact detail bytes.
        // 4,096 rows therefore exceed 100,000 payload bytes before Parquet/control
        // overhead; retain the same dominance threshold and every phase ceiling.
        for scale in [4_096_usize, 8_192, 16_384] {
            let graph = GraphForge::new(None).unwrap();
            // The staged lifecycle's phase reads are what this measures; the
            // staged path still serves appends and over-budget builds.
            let mut session = graph
                .begin_staged_graph_construction(Default::default())
                .unwrap();
            let ids = (0..scale).map(|_| Uuid::now_v7()).collect::<Vec<_>>();
            session.append_nodes("nodes", &nodes(&ids)).unwrap();
            session.seal_and_publish().unwrap();
            let evidence = &session.progress().evidence;
            assert_eq!(evidence.seal_application_read_bytes, 0);
            assert!(evidence.shape_application_read_bytes > 0);
            assert!(evidence.encode_application_read_bytes > 0);
            assert!(evidence.publication_application_read_bytes > 0);
            assert!(evidence.cas_application_read_bytes > 0);
            assert!(evidence.hydration_application_read_bytes > 0);
            let reconciled = [
                evidence.seal_application_read_bytes,
                evidence.shape_application_read_bytes,
                evidence.encode_application_read_bytes,
                evidence.publication_application_read_bytes,
                evidence.cas_application_read_bytes,
                evidence.hydration_application_read_bytes,
                evidence.recovery_application_read_bytes,
            ]
            .into_iter()
            .try_fold(0_u64, u64::checked_add)
            .unwrap();
            assert_eq!(evidence.total_application_read_bytes().unwrap(), reconciled);
            let payload = evidence.write_bytes;
            assert!(
                payload > 100_000,
                "fixture must dominate fixed control overhead"
            );
            for (phase, ceiling) in [
                (evidence.shape_application_read_bytes, 24_u64),
                (evidence.encode_application_read_bytes, 24),
                (evidence.publication_application_read_bytes, 2),
                (evidence.cas_application_read_bytes, 24),
                (evidence.hydration_application_read_bytes, 24),
                (evidence.recovery_application_read_bytes, 24),
                (reconciled, 80),
            ] {
                assert!(
                    phase <= payload.saturating_mul(ceiling),
                    "phase exceeded its fixed application bytes-per-staged-payload ceiling"
                );
            }
            let cas = &evidence.cas_publication_io;
            if cfg!(windows) {
                // Windows copies each encoded file into the object store, reading
                // the source once.
                assert_eq!(cas.payload.read_bytes, evidence.canonical_output_bytes);
            } else {
                // Elsewhere the install links the encoder's file (#1899): a fresh
                // store has no existing object to authenticate, so nothing is read.
                assert_eq!(cas.payload.read_bytes, 0);
                assert_eq!(cas.payload.write_bytes, 0);
                assert_eq!(cas.payload.installed_bytes, evidence.canonical_output_bytes);
            }
            assert!(cas.manifest_reads.read_bytes > 0);
            assert!(cas.manifest_reads.read_calls > 0);
            let measured_cas_reads = [
                cas.payload.read_bytes,
                cas.manifest.read_bytes,
                cas.manifest_reads.read_bytes,
            ]
            .into_iter()
            .try_fold(0_u64, u64::checked_add)
            .unwrap();
            assert_eq!(evidence.cas_application_read_bytes, measured_cas_reads);
            assert!(evidence.staged_and_retained_disk_bytes >= payload);
            observations.push((
                payload,
                [
                    evidence.shape_application_read_bytes,
                    evidence.encode_application_read_bytes,
                    if cfg!(windows) {
                        evidence.cas_application_read_bytes
                    } else {
                        0
                    },
                    evidence.recovery_application_read_bytes,
                    reconciled,
                ],
            ));
            cas_reads.push((payload, evidence.cas_application_read_bytes));
            hydration_reads.push((scale as u64, evidence.hydration_application_read_bytes));
        }
        if !cfg!(windows) {
            for adjacent in cas_reads.windows(2) {
                let ((prior_payload, prior), (next_payload, next)) = (adjacent[0], adjacent[1]);
                assert!(next_payload * 10 >= prior_payload * 17);
                assert!(
                    next * 10 < prior * 15,
                    "CAS install reads followed the payload: {prior} -> {next} bytes while \
                     payload went {prior_payload} -> {next_payload}"
                );
            }
        }
        for adjacent in hydration_reads.windows(2) {
            let ((prior_rows, prior), (next_rows, next)) = (adjacent[0], adjacent[1]);
            assert!(
                next.saturating_sub(prior) < next_rows - prior_rows,
                "hydration reads grew {prior} -> {next} bytes for {} added rows",
                next_rows - prior_rows
            );
        }
        for adjacent in observations.windows(2) {
            let (prior_payload, prior_phases) = adjacent[0];
            let (next_payload, next_phases) = adjacent[1];
            assert!(next_payload * 10 >= prior_payload * 17);
            assert!(next_payload * 10 <= prior_payload * 23);
            for (prior, next) in prior_phases.into_iter().zip(next_phases) {
                assert!(
                    next * 10 >= prior * 15,
                    "payload-owned phase grew below 1.5x"
                );
                assert!(
                    next * 10 <= prior * 25,
                    "payload-owned phase grew above 2.5x"
                );
                let prior_normalized = prior.saturating_mul(1_000_000) / prior_payload;
                let next_normalized = next.saturating_mul(1_000_000) / next_payload;
                assert!(next_normalized * 2 >= prior_normalized);
                assert!(next_normalized <= prior_normalized.saturating_mul(2));
            }
        }
    }

    /// The same four nodes and three edges as chunks, accepted by `session`.
    fn accept_chain(session: &mut GraphConstructionSession<'_>) -> ([Uuid; 4], [Uuid; 3]) {
        let node_ids: [Uuid; 4] = std::array::from_fn(|_| Uuid::now_v7());
        let edge_ids: [Uuid; 3] = std::array::from_fn(|_| Uuid::now_v7());
        session
            .append_nodes("nodes-a", &nodes(&node_ids[..2]))
            .unwrap();
        session
            .append_nodes("nodes-b", &nodes(&node_ids[2..]))
            .unwrap();
        session
            .append_edges(
                "edges",
                &edges(
                    &edge_ids,
                    &[
                        (node_ids[0], node_ids[1]),
                        (node_ids[1], node_ids[2]),
                        (node_ids[2], node_ids[3]),
                    ],
                ),
            )
            .unwrap();
        (node_ids, edge_ids)
    }

    fn chain_relationships(nodes: &[Uuid; 4], edges: &[Uuid; 3]) -> Vec<[Uuid; 3]> {
        (0..3).map(|i| [nodes[i], edges[i], nodes[i + 1]]).collect()
    }

    /// An initial build through the chunk API runs on the bulk builder: the
    /// builder reports the rows it built and no chunk was staged. The same
    /// chunks pinned to the staged path stage them and the builder builds
    /// nothing, and both publish the same graph.
    #[test]
    fn chunk_api_initial_builds_take_the_bulk_path() {
        let spooled = GraphForge::new(None).unwrap();
        let mut session = spooled
            .begin_graph_construction(Default::default())
            .unwrap();
        let (node_ids, edge_ids) = accept_chain(&mut session);
        assert_eq!(session.progress().accepted_chunks, 3);
        session.seal_and_publish().unwrap();
        let report = session.inner.bulk_build_report();
        assert_eq!((report.nodes, report.edges), (4, 3));
        assert!(report.passes.contains_key("nodes") && report.passes.contains_key("edges"));
        assert_eq!(session.progress().evidence.input_batches, 3);
        assert_eq!(session.progress().evidence.parquet_shards, 0);
        assert_construction_relationships(&spooled, &chain_relationships(&node_ids, &edge_ids));

        let staged = GraphForge::new(None).unwrap();
        let mut session = staged
            .begin_staged_graph_construction(Default::default())
            .unwrap();
        let (node_ids, edge_ids) = accept_chain(&mut session);
        session.seal_and_publish().unwrap();
        let report = session.inner.bulk_build_report();
        assert_eq!((report.nodes, report.edges), (0, 0));
        assert_eq!(session.progress().evidence.input_batches, 3);
        assert_construction_relationships(&staged, &chain_relationships(&node_ids, &edge_ids));
    }

    /// A construction pinned to a non-empty graph is an append: it stages.
    #[test]
    fn chunk_api_appends_stage() {
        let graph = GraphForge::new(None).unwrap();
        let mut first = graph.begin_graph_construction(Default::default()).unwrap();
        accept_chain(&mut first);
        first.seal_and_publish().unwrap();
        let mut append = graph.begin_graph_construction(Default::default()).unwrap();
        append
            .append_nodes("more", &nodes(&[Uuid::now_v7()]))
            .unwrap();
        append.seal_and_publish().unwrap();
        assert_eq!(append.progress().evidence.input_batches, 1);
        assert_eq!(append.inner.bulk_build_report().nodes, 0);
    }

    fn force_budget(budget: Option<u64>) {
        crate::import_session::bulk_source::TEST_BUDGET.with(|slot| slot.set(budget));
    }

    /// Resident budget small enough that the in-memory estimate of any test
    /// graph exceeds it, large enough for its node tables: the scratch route.
    const SCRATCH_BUDGET: u64 = 800 << 20;

    /// The seal route is decided once and stored with the session. A seal that
    /// is interrupted under one memory condition is completed under another
    /// without re-deciding: a build that recorded the bulk route keeps it and
    /// runs on scratch when memory shrank; one that recorded the staged replay
    /// (as a binary did before node tables could go to scratch, #1929) keeps it
    /// when memory grew.
    #[test]
    fn a_retried_seal_keeps_the_route_it_recorded_whatever_the_budget_now_says() {
        for (first_budget, second_budget, expected) in [
            (0, u64::MAX, graphforge_storage::SealRoute::ReplayStaged),
            (
                u64::MAX,
                SCRATCH_BUDGET,
                graphforge_storage::SealRoute::Bulk,
            ),
        ] {
            let graph = GraphForge::new(None).unwrap();
            let mut session = graph.begin_graph_construction(Default::default()).unwrap();
            let (node_ids, edge_ids) = accept_chain(&mut session);
            if expected == graphforge_storage::SealRoute::ReplayStaged {
                // No plan chooses this route for want of memory any more; an
                // earlier binary recorded it.
                session.inner.record_seal_route(expected).unwrap();
            }
            let cancelled = crate::CancellationToken::new();
            cancelled.cancel();
            force_budget(Some(first_budget));
            let interrupted = session.validate_and_seal(Some(&cancelled)).unwrap_err();
            assert!(
                interrupted.to_string().contains("cancelled"),
                "{interrupted}"
            );
            assert_eq!(session.inner.seal_route(), Some(expected));
            force_budget(Some(second_budget));
            session.seal_and_publish().unwrap();
            force_budget(None);
            assert_eq!(session.inner.seal_route(), Some(expected));
            let built = session.inner.bulk_build_report();
            match expected {
                graphforge_storage::SealRoute::Bulk => {
                    assert_eq!((built.nodes, built.edges), (4, 3));
                    assert!(built.scratch_write_bytes > 0, "{built:?}");
                }
                graphforge_storage::SealRoute::ReplayStaged => {
                    assert_eq!((built.nodes, built.edges), (0, 0));
                    assert_eq!(session.progress().evidence.input_batches, 3);
                }
            }
            assert_construction_relationships(&graph, &chain_relationships(&node_ids, &edge_ids));
        }
    }

    /// Above the fixed workspace and below the node tables of the test chain:
    /// the node tables go to scratch too. (512 MiB is the fixed workspace of
    /// ADR 0058; the report below fails loudly if the constants drift.)
    const NODE_SCRATCH_BUDGET: u64 = (512 << 20) + 100;

    /// A chunk-API build whose node tables do not fit takes the bulk route and
    /// puts them on scratch; a budget below the fixed workspace is refused
    /// before decoding, keeps the bulk route and builds once memory returns.
    /// Neither stages.
    #[test]
    fn a_chunk_api_build_whose_node_tables_exceed_the_budget_stays_on_the_bulk_route() {
        let graph = GraphForge::new(None).unwrap();
        let mut session = graph.begin_graph_construction(Default::default()).unwrap();
        let (node_ids, edge_ids) = accept_chain(&mut session);
        force_budget(Some(NODE_SCRATCH_BUDGET));
        let sealed = session.seal_and_publish();
        force_budget(None);
        sealed.unwrap();
        assert_eq!(
            session.inner.seal_route(),
            Some(graphforge_storage::SealRoute::Bulk)
        );
        let report = session.inner.bulk_build_report();
        assert_eq!((report.nodes, report.edges), (4, 3));
        assert!(report.node_partitions > 0, "{report:?}");
        assert!(report.endpoint_scratch_write_bytes > 0, "{report:?}");
        assert_eq!(
            report.endpoint_scratch_read_bytes,
            report.endpoint_scratch_write_bytes
        );
        assert_eq!(session.progress().evidence.parquet_shards, 0);
        assert_construction_relationships(&graph, &chain_relationships(&node_ids, &edge_ids));

        let graph = GraphForge::new(None).unwrap();
        let mut session = graph.begin_graph_construction(Default::default()).unwrap();
        let (node_ids, edge_ids) = accept_chain(&mut session);
        force_budget(Some(1));
        let refused = session.seal_and_publish().unwrap_err();
        force_budget(None);
        assert!(
            matches!(
                refused,
                GfError::Project {
                    code: graphforge_core::ProjectErrorCode::ResourceLimit,
                    ..
                }
            ),
            "{refused}"
        );
        assert_eq!(
            session.inner.seal_route(),
            Some(graphforge_storage::SealRoute::Bulk)
        );
        session.seal_and_publish().unwrap();
        assert_eq!(session.progress().evidence.input_batches, 3);
        assert_construction_relationships(&graph, &chain_relationships(&node_ids, &edge_ids));
    }

    /// An over-budget initial build through the chunk API runs the bulk
    /// builder's scratch route over the spool. It neither stages nor refuses,
    /// and publishes the graph the in-memory route does.
    #[test]
    fn an_over_budget_chunk_api_build_runs_on_scratch_without_staging() {
        let graph = GraphForge::new(None).unwrap();
        let mut session = graph.begin_graph_construction(Default::default()).unwrap();
        let (node_ids, edge_ids) = accept_chain(&mut session);
        force_budget(Some(SCRATCH_BUDGET));
        let sealed = session.seal_and_publish();
        force_budget(None);
        sealed.unwrap();
        assert_eq!(
            session.inner.seal_route(),
            Some(graphforge_storage::SealRoute::Bulk)
        );
        let report = session.inner.bulk_build_report();
        assert_eq!((report.nodes, report.edges), (4, 3));
        assert!(
            report.scratch_write_bytes > 0 && report.scratch_read_bytes > 0,
            "{report:?}"
        );
        assert_eq!(session.progress().evidence.parquet_shards, 0);
        assert_construction_relationships(&graph, &chain_relationships(&node_ids, &edge_ids));
    }
}
