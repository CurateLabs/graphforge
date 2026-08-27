//! Public Rust facade for resumable, bounded, disk-owned graph construction.

use arrow::record_batch::RecordBatch;
use graphforge_core::uuid::Uuid;
use sha2::{Digest, Sha256};
use std::path::Path;

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
        let parent_topology_generation = graphforge_storage::read_topology_generation(&self.dir)?;
        let project = self.resolved_generation.container_root();
        let inner = if self.ontology_mode == OntologyMode::Exploratory {
            if resume {
                graphforge_storage::GraphConstructionSession::resume_with_mode_and_lifecycle_from_graph(
                    project,
                    &self.dir,
                    session_uuid,
                    self.ontology_mode,
                    budgets,
                    self.lifecycle_mode,
                )?
            } else {
                graphforge_storage::GraphConstructionSession::open_with_mode_and_lifecycle_from_graph(
                    project,
                    &self.dir,
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
                    &self.dir,
                    session_uuid,
                    self.ontology_mode,
                    budgets,
                    authority,
                    self.lifecycle_mode,
                )?
            } else {
                graphforge_storage::GraphConstructionSession::open_with_semantic_authority_and_lifecycle_from_graph(
                    project,
                    &self.dir,
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
            evidence: self.inner.evidence().clone(),
        }
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
        if self.inner.state() == GraphConstructionState::Staging {
            self.inner.seal()?;
        }
        let topology_generation = self.inner.parent_topology_generation().saturating_add(1);
        let encoding = self
            .inner
            .prepare_canonical_encoding_with_cancellation(topology_generation, || {
                cancellation.is_some_and(crate::CancellationToken::is_cancelled)
            })?;
        if let Some(token) = cancellation {
            token.checkpoint()?;
        }
        let target = derived_uuid(self.session_uuid, b"generation");
        let transaction = derived_uuid(self.session_uuid, b"transaction");
        let published = self
            .inner
            .publish_canonical(&encoding, target, transaction)?;

        let root = self.graph.resolved_generation.container_root();
        let resolved = graphforge_storage::resolve_project_generation(root)?;
        if resolved.generation_uuid() != published.generation_uuid {
            return Err(GfError::Storage(
                "construction publication did not resolve its exact generation".into(),
            ));
        }
        let (prepared_dir, prepared_guard, _) = super::hydrate_graph_workspace(&resolved, false)?;
        let runtime_catalog = super::load_runtime_catalog(&prepared_dir)?;
        let property_inventory = std::sync::Arc::new(
            graphforge_storage::AuthenticatedPropertyInventory::from_resolved_generation(
                &resolved,
            )?,
        );
        replace_workspace(&prepared_dir, &self.graph.dir)?;
        drop(prepared_guard);
        *self
            .graph
            .runtime_catalog
            .lock()
            .expect("runtime catalog poisoned") = runtime_catalog;
        *self
            .graph
            .property_authority
            .lock()
            .expect("property authority lock poisoned") = super::GenerationPropertyAuthority {
            generation_uuid: resolved.generation_uuid(),
            inventory: property_inventory,
        };
        *self
            .graph
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned") = resolved.generation_uuid();
        *self
            .graph
            .uuid_membership_index
            .lock()
            .expect("UUID membership lock poisoned") = None;
        self.graph.adjacency_provider.invalidate();
        Ok(GraphConstructionPublicationReceipt {
            generation_uuid: published.generation_uuid,
            idempotent_replay: published.idempotent_replay,
        })
    }

    /// Abort an unsealed session without changing project authority.
    pub fn abort(&mut self) -> Result<(), GfError> {
        self.inner.abort()
    }
}

fn replace_workspace(prepared: &Path, target: &Path) -> Result<(), GfError> {
    refresh_boundary(RefreshBoundary::BeforeMove)?;
    let parent = target.parent().ok_or_else(|| {
        GfError::Storage("graph workspace has no parent for atomic replacement".into())
    })?;
    let backup = parent.join(format!(".graphforge-workspace-backup-{}", Uuid::now_v7()));
    std::fs::rename(target, &backup).map_err(|error| {
        GfError::Storage(format!("failed to preserve graph workspace: {error}"))
    })?;
    if let Err(error) = refresh_boundary(RefreshBoundary::AfterOldMoved) {
        restore_workspace(&backup, target, None)?;
        return Err(error);
    }
    if let Err(error) = std::fs::rename(prepared, target) {
        restore_workspace(&backup, target, None)?;
        return Err(GfError::Storage(format!(
            "failed to install prepared graph workspace: {error}"
        )));
    }
    if let Err(error) = refresh_boundary(RefreshBoundary::AfterNewInstalled) {
        restore_workspace(&backup, target, Some(prepared))?;
        return Err(error);
    }
    // The backup is no longer authoritative. Failure to reclaim it cannot make
    // the already-installed workspace or in-memory replacement state invalid.
    let _ = std::fs::remove_dir_all(backup);
    Ok(())
}

fn restore_workspace(backup: &Path, target: &Path, prepared: Option<&Path>) -> Result<(), GfError> {
    if let Some(prepared) = prepared {
        std::fs::rename(target, prepared).map_err(|error| {
            GfError::Storage(format!(
                "failed to withdraw prepared graph workspace during rollback: {error}"
            ))
        })?;
    }
    std::fs::rename(backup, target).map_err(|error| {
        GfError::Storage(format!("failed to restore prior graph workspace: {error}"))
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RefreshBoundary {
    BeforeMove,
    AfterOldMoved,
    AfterNewInstalled,
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

    use arrow::array::{ArrayRef, FixedSizeBinaryArray, StringArray};
    use arrow::record_batch::RecordBatch;

    use super::*;

    #[test]
    fn refresh_boundaries_preserve_the_prior_workspace_on_failure() {
        for boundary in [
            RefreshBoundary::BeforeMove,
            RefreshBoundary::AfterOldMoved,
            RefreshBoundary::AfterNewInstalled,
        ] {
            let parent = tempfile::tempdir().unwrap();
            let target = parent.path().join("live");
            let prepared = parent.path().join("prepared");
            std::fs::create_dir(&target).unwrap();
            std::fs::create_dir(&prepared).unwrap();
            std::fs::write(target.join("generation"), b"old").unwrap();
            std::fs::write(prepared.join("generation"), b"new").unwrap();

            REFRESH_FAILURE.with(|failure| failure.set(Some(boundary)));
            let error = replace_workspace(&prepared, &target).unwrap_err();
            REFRESH_FAILURE.with(|failure| failure.set(None));

            assert!(
                error
                    .to_string()
                    .contains("injected construction refresh failure")
            );
            assert_eq!(std::fs::read(target.join("generation")).unwrap(), b"old");
            assert_eq!(std::fs::read(prepared.join("generation")).unwrap(), b"new");
        }
    }

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
        session
            .append_nodes("nodes", &nodes(&[Uuid::now_v7()]))
            .unwrap();
        let cancellation = crate::CancellationToken::new();
        cancellation.cancel();
        assert!(
            session
                .seal_and_publish_with_cancellation(&cancellation)
                .is_err()
        );
        assert_eq!(
            graphforge_storage::resolve_project_generation(&root)
                .unwrap()
                .generation_uuid(),
            parent
        );
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

        let index = graphforge_storage::UuidMembershipIndex::open(&graph.dir).unwrap();
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
        drop(child);

        let resolved_child = graphforge_storage::resolve_project_generation(&root).unwrap();
        assert_eq!(
            resolved_child.generation_uuid(),
            child_receipt.generation_uuid
        );
        let child_inventory = resolved_child.graph_files_inventory().unwrap().unwrap();
        assert!(first_inventory.files.iter().any(|parent_entry| {
            child_inventory.files.iter().any(|child_entry| {
                parent_entry.content_sha256 == child_entry.content_sha256
                    && parent_entry.byte_length == child_entry.byte_length
            })
        }));

        let child_index = graphforge_storage::UuidMembershipIndex::open(&graph.dir).unwrap();
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
    }
}
