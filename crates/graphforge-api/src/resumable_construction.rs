//! Public Rust facade for resumable, bounded, disk-owned graph construction.

#[cfg(test)]
mod codec_tests;

use arrow::record_batch::RecordBatch;
use graphforge_core::uuid::Uuid;
use sha2::{Digest, Sha256};

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
    pub fn begin_graph_construction(
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

    /// Append one canonical node Arrow chunk.
    pub fn append_nodes(
        &mut self,
        chunk_id: &str,
        batch: &RecordBatch,
    ) -> Result<ConstructionChunkReceipt, GfError> {
        self.inner.append(
            graphforge_storage::ConstructionChunkKind::Node,
            chunk_id,
            batch,
        )
    }

    /// Append one node chunk while polling cooperative cancellation.
    pub fn append_nodes_with_cancellation(
        &mut self,
        chunk_id: &str,
        batch: &RecordBatch,
        cancellation: &crate::CancellationToken,
    ) -> Result<ConstructionChunkReceipt, GfError> {
        self.inner.append_with_cancellation(
            graphforge_storage::ConstructionChunkKind::Node,
            chunk_id,
            batch,
            || cancellation.is_cancelled(),
        )
    }

    /// Append one canonical edge Arrow chunk after all node chunks.
    pub fn append_edges(
        &mut self,
        chunk_id: &str,
        batch: &RecordBatch,
    ) -> Result<ConstructionChunkReceipt, GfError> {
        self.inner.append(
            graphforge_storage::ConstructionChunkKind::Edge,
            chunk_id,
            batch,
        )
    }

    /// Append one edge chunk while polling cooperative cancellation.
    pub fn append_edges_with_cancellation(
        &mut self,
        chunk_id: &str,
        batch: &RecordBatch,
        cancellation: &crate::CancellationToken,
    ) -> Result<ConstructionChunkReceipt, GfError> {
        self.inner.append_with_cancellation(
            graphforge_storage::ConstructionChunkKind::Edge,
            chunk_id,
            batch,
            || cancellation.is_cancelled(),
        )
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
            evidence: self.inner.evidence().clone(),
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
        let target = derived_uuid(self.session_uuid, b"generation");
        let transaction = derived_uuid(self.session_uuid, b"transaction");
        let published = if let Some(replay) =
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
            replay
        } else {
            let encoding = self.prepare_encoding(cancellation)?;
            if let Some(token) = cancellation {
                token.checkpoint()?;
            }
            self.inner.publish_canonical_with_cancellation(
                &encoding,
                target,
                transaction,
                || cancellation.is_some_and(crate::CancellationToken::is_cancelled),
            )?
        };

        let refresh = (|| {
            refresh_boundary(RefreshBoundary::BeforeHydrate)?;
            let root = self.graph.resolved_generation.container_root();
            let resolved = graphforge_storage::resolve_project_generation(root)?;
            if resolved.generation_uuid() != published.generation_uuid {
                return Err(GfError::Storage(
                    "construction publication did not resolve its exact generation".into(),
                ));
            }
            let (prepared_dir, prepared_guard, hydration_evidence) =
                super::hydrate_graph_workspace(&resolved, false)?;
            self.inner.record_hydration_evidence(&hydration_evidence)?;
            refresh_boundary(RefreshBoundary::AfterHydrate)?;
            let runtime_catalog = super::load_runtime_catalog(&prepared_dir)?;
            let prepared = self
                .graph
                .prepare_generation_read_authority(&resolved, &prepared_dir)?;
            refresh_boundary(RefreshBoundary::BeforeInstall)?;
            Ok((
                resolved,
                super::GraphWorkspace {
                    dir: prepared_dir,
                    _owner: prepared_guard,
                },
                runtime_catalog,
                prepared,
            ))
        })();
        let (resolved, prepared_guard, runtime_catalog, prepared) =
            refresh.map_err(|error: GfError| {
                GfError::Storage(format!(
                    "phase=POST_PUBLICATION_REFRESH committed=true generation_uuid={} recovery=reopen_or_resume cause={error}",
                    published.generation_uuid
                ))
            })?;
        // Readers retain stable paths and capabilities. Never rename a workspace
        // pinned by an ordinal handle or an unconsumed stream (including Windows).
        // All fallible preparation precedes this transition under write visibility.
        let old_workspace = self.graph.replace_workspace_owner(prepared_guard);
        *self
            .graph
            .runtime_catalog
            .lock()
            .expect("runtime catalog poisoned") = runtime_catalog;
        self.graph
            .install_prepared_generation_read_authority(resolved.generation_uuid(), prepared);
        *self
            .graph
            .uuid_membership_index
            .lock()
            .expect("UUID membership lock poisoned") = None;
        // Release old reader handles before their workspace; streams own their pins.
        drop(old_workspace);
        Ok(GraphConstructionPublicationReceipt {
            generation_uuid: published.generation_uuid,
            idempotent_replay: published.idempotent_replay,
        })
    }

    fn prepare_encoding(
        &mut self,
        cancellation: Option<&crate::CancellationToken>,
    ) -> Result<graphforge_storage::GraphConstructionEncoding, GfError> {
        let topology_generation = self.inner.parent_topology_generation().saturating_add(1);
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RefreshBoundary {
    BeforeHydrate,
    AfterHydrate,
    BeforeInstall,
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
    fn post_publication_refresh_failure_reports_committed_authority() {
        for boundary in [
            RefreshBoundary::BeforeHydrate,
            RefreshBoundary::AfterHydrate,
            RefreshBoundary::BeforeInstall,
        ] {
            let graph = GraphForge::new(None).unwrap();
            let root = graph.resolved_generation.container_root().to_path_buf();
            let old_workspace = graph.workspace_for_session();
            let old_generation = *graph.current_generation_uuid.lock().unwrap();
            let mut session = graph.begin_graph_construction(Default::default()).unwrap();
            session
                .append_nodes("nodes", &nodes(&[Uuid::now_v7()]))
                .unwrap();
            REFRESH_FAILURE.with(|failure| failure.set(Some(boundary)));
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

        let index = graphforge_storage::UuidMembershipIndex::open(&graph.dir()).unwrap();
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

        let child_index = graphforge_storage::UuidMembershipIndex::open(&graph.dir()).unwrap();
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
        let current_index = graphforge_storage::UuidMembershipIndex::open(&graph.dir()).unwrap();
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
        // Each node retains 16 identity bytes and at least 18 compact detail bytes.
        // 4,096 rows therefore exceed 100,000 payload bytes before Parquet/control
        // overhead; retain the same dominance threshold and every phase ceiling.
        for scale in [4_096_usize, 8_192, 16_384] {
            let graph = GraphForge::new(None).unwrap();
            let mut session = graph.begin_graph_construction(Default::default()).unwrap();
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
            assert_eq!(cas.payload.read_bytes, evidence.canonical_output_bytes);
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
                    evidence.cas_application_read_bytes,
                    evidence.hydration_application_read_bytes,
                    evidence.recovery_application_read_bytes,
                    reconciled,
                ],
            ));
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
}
