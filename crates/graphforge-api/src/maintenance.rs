//! Thin GraphForge facade over Rust-owned retention/GC and delta compaction.
//!
//! Bindings and CLI must call these methods rather than reimplementing
//! reachability, cleanup, delta replay, or compaction logic.

use graphforge_core::{GfError, ProjectErrorCode};
use graphforge_storage::{
    GraphDeltaCompactionPolicy, GraphDeltaCompactionReport, GraphDeltaCompactionRequest,
    GraphDeltaCompactionStatus, GraphDeltaJournalLimits, ProjectCleanupReport,
    ProjectReachabilityReport, ProjectRetentionLimits, ProjectRetentionPolicy,
    compact_graph_delta_for_parent_with_mode, graph_delta_compaction_status_with_mode,
    inspect_project_reachability_with_mode, preview_graph_delta_compaction_with_mode,
    preview_project_cleanup_with_mode,
};

use crate::{CancellationToken, GraphForge};

impl GraphForge {
    fn require_mutable_project_root(&self) -> Result<&std::path::Path, GfError> {
        if self.read_only {
            return Err(GfError::Project {
                code: ProjectErrorCode::ReadOnlyView,
                message: "checkpoint views cannot run project maintenance".into(),
            });
        }
        Ok(self.resolved_generation.container_root())
    }

    /// Inspect verified generation reachability for retention/GC planning.
    pub fn inspect_project_reachability(
        &self,
        policy: ProjectRetentionPolicy,
        limits: ProjectRetentionLimits,
    ) -> Result<ProjectReachabilityReport, GfError> {
        let root = self.require_mutable_project_root()?;
        inspect_project_reachability_with_mode(root, policy, limits, self.lifecycle_mode)
    }

    /// Preview retention/GC candidates without removing anything.
    pub fn preview_project_cleanup(
        &self,
        policy: ProjectRetentionPolicy,
        limits: ProjectRetentionLimits,
    ) -> Result<ProjectCleanupReport, GfError> {
        let root = self.require_mutable_project_root()?;
        preview_project_cleanup_with_mode(root, policy, limits, self.lifecycle_mode)
    }

    /// Execute retention/GC for unreachable generations using the shared oracle.
    pub fn execute_project_cleanup(
        &self,
        policy: ProjectRetentionPolicy,
        limits: ProjectRetentionLimits,
    ) -> Result<ProjectCleanupReport, GfError> {
        let root = self.require_mutable_project_root()?;
        graphforge_storage::execute_project_cleanup_with_mode(
            root,
            policy,
            limits,
            self.lifecycle_mode,
        )
    }

    /// Report whether CURRENT's verified delta chain should compact under policy.
    pub fn graph_delta_compaction_status(
        &self,
        policy: GraphDeltaCompactionPolicy,
        limits: GraphDeltaJournalLimits,
    ) -> Result<GraphDeltaCompactionStatus, GfError> {
        let root = self.require_mutable_project_root()?;
        graph_delta_compaction_status_with_mode(root, policy, limits, self.lifecycle_mode)
    }

    /// Preview delta compaction without publishing CURRENT.
    pub fn preview_graph_delta_compaction(
        &self,
        request: &GraphDeltaCompactionRequest,
        cancellation: Option<&CancellationToken>,
    ) -> Result<GraphDeltaCompactionReport, GfError> {
        let root = self.require_mutable_project_root()?;
        preview_graph_delta_compaction_with_mode(
            root,
            request,
            cancellation.map(CancellationToken::flag),
            self.lifecycle_mode,
        )
    }

    /// Compact the verified delta chain into a new Parquet generation and
    /// refresh this facade for subsequent reads and mutations. Existing streams
    /// retain their original workspace. Current-format compaction requires the
    /// full chain; a facade made stale by another publisher must be reopened.
    pub fn compact_graph_delta(
        &mut self,
        request: &GraphDeltaCompactionRequest,
        cancellation: Option<&CancellationToken>,
    ) -> Result<GraphDeltaCompactionReport, GfError> {
        let root = self.require_mutable_project_root()?.to_path_buf();
        let visibility = std::sync::Arc::clone(&self.graph_visibility);
        let _visibility = visibility.acquire(cancellation)?;
        if let Some(token) = cancellation {
            token.checkpoint()?;
        }
        let parent = self.generation_for_read()?;
        let expected_parent = *self
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        if parent.generation_uuid() != expected_parent {
            return Err(GfError::Project {
                code: ProjectErrorCode::WriteConflict,
                message: "project generation changed before facade compaction; reopen the facade"
                    .into(),
            });
        }
        let result = compact_graph_delta_for_parent_with_mode(
            &root,
            request,
            cancellation.map(CancellationToken::flag),
            self.lifecycle_mode,
            expected_parent,
        );
        // Storage stages privately. An unchanged CURRENT needs no restoration.
        // After publication (including a returned error), refresh from actual
        // durable authority rather than reinstalling the previous working tree.
        let refresh = (|| {
            let current = self.generation_for_read()?;
            if current.generation_uuid() == expected_parent {
                return Ok(());
            }
            if result.is_err()
                && !(current.generation_uuid() == request.generation_uuid
                    && current.transaction_uuid() == request.transaction_uuid
                    && current.parent_generation_uuid() == Some(expected_parent))
                && graphforge_storage::published_project_transaction(
                    &root,
                    request.transaction_uuid,
                )?
                .is_none_or(|receipt| receipt.generation_uuid != request.generation_uuid)
            {
                // A competing writer advanced CURRENT; this failed operation
                // did not publish and must not change the facade's old view.
                return Ok(());
            }
            // CURRENT authenticates the manifest's transaction and parent even
            // when the publication journal has not yet reached Published.
            if crate::composite_publish::administrative_contract(&current)?
                != crate::composite_publish::administrative_contract(&parent)?
            {
                return Err(GfError::Project {
                    code: ProjectErrorCode::WriteConflict,
                    message: "workspace authority changed during compaction; reopen the facade"
                        .into(),
                });
            }
            // Prepare a separate bounded workspace. Existing lazy streams keep
            // their old paths and immutable identity handles, including on
            // Windows where retained capabilities deliberately deny deletion.
            let (dir, workspace, evidence) = crate::hydrate_graph_workspace(&current, false)?;
            let prepared = self.prepare_generation_read_authority(&current, &dir)?;
            let catalog = crate::load_runtime_catalog(&dir)?;
            let bindings = graphforge_storage::semantic_storage_bindings(&current)?;
            let old_workspace = std::mem::replace(&mut self.workspace_guard, workspace);
            self.dir = dir;
            self.graph_open_evidence = evidence;
            self.install_prepared_generation_read_authority(current.generation_uuid(), prepared);
            self.resolved_generation = current;
            *self
                .runtime_catalog
                .lock()
                .expect("runtime catalog poisoned") = catalog;
            *self
                .semantic_storage_bindings
                .lock()
                .expect("semantic storage binding lock poisoned") = bindings;
            *self
                .uuid_membership_index
                .lock()
                .expect("UUID membership index lock poisoned") = None;
            drop(old_workspace);
            Ok(())
        })();
        if let Err(error) = refresh {
            self.graph_visibility.health.fail(&error);
            return Err(error);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use tempfile::TempDir;
    use uuid::Uuid;

    use crate::{OperationId, WriteContext};
    use graphforge_storage::{GraphDeltaCompactionLimits, GraphDeltaCompactionRequest};

    #[test]
    fn facade_exposes_transaction_and_maintenance_ops() {
        let directory = TempDir::new().unwrap();
        let mut graph = GraphForge::new(directory.path().to_str()).unwrap();
        let _ = graph.project_open_recovery();
        graph
            .inspect_project_reachability(
                ProjectRetentionPolicy::default(),
                ProjectRetentionLimits::default(),
            )
            .unwrap();
        let preview = graph
            .preview_project_cleanup(
                ProjectRetentionPolicy::default(),
                ProjectRetentionLimits::default(),
            )
            .unwrap();
        let executed = graph
            .execute_project_cleanup(
                ProjectRetentionPolicy::default(),
                ProjectRetentionLimits::default(),
            )
            .unwrap();
        assert_eq!(preview.candidates, executed.candidates);
        let tx = graph
            .begin_transaction(WriteContext {
                operation_uuid: OperationId(Uuid::now_v7()),
                actor_uuid: None,
            })
            .unwrap();
        tx.stage_cypher("CREATE (:Person {name: 'A'})", HashMap::new())
            .unwrap();
        tx.commit(&graph).unwrap();

        graph
            .graph_delta_compaction_status(
                GraphDeltaCompactionPolicy::default(),
                GraphDeltaJournalLimits::default(),
            )
            .unwrap();

        let request = GraphDeltaCompactionRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
            through_run_sequence: None,
            limits: GraphDeltaCompactionLimits::default(),
            cleanup_after_commit: false,
            cleanup_policy: ProjectRetentionPolicy::default(),
            cleanup_limits: ProjectRetentionLimits::default(),
        };
        let _ = graph.preview_graph_delta_compaction(&request, None);
        let _ = graph.compact_graph_delta(&request, None);
    }

    #[test]
    fn unlabelled_creation_then_label_before_publication_retains_absent_primary_on_reopen() {
        use arrow::array::{Int64Array, ListArray, UInt32Array};

        let directory = TempDir::new().unwrap();
        let graph = GraphForge::new(directory.path().to_str()).unwrap();
        graph.execute("CREATE (n {rank: 7}) SET n:Person").unwrap();
        drop(graph);

        let reopened = GraphForge::new(directory.path().to_str()).unwrap();
        let result = reopened
            .execute("MATCH (n:Person) RETURN n.rank AS rank")
            .unwrap();
        assert_eq!(
            result
                .batches
                .iter()
                .map(|batch| batch.num_rows())
                .sum::<usize>(),
            1
        );
        assert_eq!(
            result.batches[0]
                .column_by_name("rank")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            7
        );
        let generation = graphforge_storage::resolve_project_generation(directory.path()).unwrap();
        let nodes = graphforge_storage::read_nodes(&generation.graph_tree_root()).unwrap();
        assert_eq!(nodes.iter().map(|batch| batch.num_rows()).sum::<usize>(), 1);
        let primary = nodes[0]
            .column_by_name("type_id")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(primary.value(0), u32::MAX);
        let memberships = nodes[0]
            .column_by_name("type_ids")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value(0);
        let memberships = memberships.as_any().downcast_ref::<UInt32Array>().unwrap();
        assert_eq!(memberships.len(), 1);
        assert!(graphforge_value::EntityTypeId::decode(memberships.value(0)).is_ok());
    }

    #[test]
    fn facade_reopen_queries_typed_delta_materialized_from_canonical_parquet() {
        use arrow::array::Int64Array;
        use graphforge_ir::IrLiteral;
        use graphforge_storage::{
            GraphDeltaOp, GraphDeltaOpKind, GraphDeltaPayload, GraphDeltaPublishRequest,
            encode_graph_delta_value,
        };

        let directory = TempDir::new().unwrap();
        let graph = GraphForge::new(directory.path().to_str()).unwrap();
        graph.execute("CREATE (:Base)").unwrap();
        drop(graph);

        let replayed_node = Uuid::now_v7().hyphenated().to_string();
        graphforge_storage::publish_graph_delta(
            directory.path(),
            &GraphDeltaPublishRequest {
                transaction_uuid: Uuid::now_v7(),
                generation_uuid: Uuid::now_v7(),
                run_uuid: Uuid::now_v7(),
                operations: vec![
                    GraphDeltaOp {
                        operation_uuid: Uuid::now_v7(),
                        kind: GraphDeltaOpKind::UpsertNode,
                        payload: GraphDeltaPayload::UpsertNodeV2 {
                            node_uuid: replayed_node.clone(),
                            node_id: 2,
                            type_ids: Vec::new(),
                            created_at_micros: 1_700_000_000_000_001,
                            updated_at_micros: 1_700_000_000_000_001,
                        },
                    },
                    GraphDeltaOp {
                        operation_uuid: Uuid::now_v7(),
                        kind: GraphDeltaOpKind::SetNodeProperty,
                        payload: GraphDeltaPayload::SetNodeProperty {
                            node_uuid: replayed_node,
                            property_stem: "_untyped".into(),
                            key: "rank".into(),
                            value: encode_graph_delta_value(&IrLiteral::Int(7)).unwrap(),
                        },
                    },
                ],
                limits: GraphDeltaJournalLimits::default(),
            },
        )
        .unwrap();

        let reopened = GraphForge::new(directory.path().to_str()).unwrap();
        let result = reopened
            .execute("MATCH (n) RETURN count(n) AS total")
            .unwrap();
        let count = result.batches[0]
            .column_by_name("total")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, 2);
        let result = reopened
            .execute("MATCH (n) WHERE n.rank = 7 RETURN n.rank AS rank")
            .unwrap();
        let rank = result.batches[0]
            .column_by_name("rank")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(rank, 7);
        reopened
            .checkpoint(crate::CheckpointRequest {
                name: "Delta replay".into(),
                description: Some("typed GFDR checkpoint parity".into()),
                idempotency_key: crate::OperationId(Uuid::now_v7()),
                actor_uuid: None,
            })
            .unwrap();
        let checkpoint = reopened.open_checkpoint("Delta replay").unwrap();
        let result = checkpoint
            .execute("MATCH (n) WHERE n.rank = 7 RETURN n.rank AS rank")
            .unwrap();
        assert_eq!(
            result.batches[0]
                .column_by_name("rank")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            7
        );
    }

    #[test]
    fn in_memory_retention_uses_ephemeral_lifecycle_mode() {
        let graph = GraphForge::new(None).unwrap();
        graph
            .inspect_project_reachability(
                ProjectRetentionPolicy::default(),
                ProjectRetentionLimits::default(),
            )
            .unwrap();
        graph
            .preview_project_cleanup(
                ProjectRetentionPolicy::default(),
                ProjectRetentionLimits::default(),
            )
            .unwrap();
        graph
            .execute_project_cleanup(
                ProjectRetentionPolicy::default(),
                ProjectRetentionLimits::default(),
            )
            .unwrap();
    }

    #[test]
    fn in_memory_compaction_cleanup_uses_ephemeral_lifecycle_mode() {
        let mut graph = GraphForge::new(None).unwrap();
        let created = graph
            .execute("CREATE (n:Person) RETURN n.node_uuid")
            .unwrap();
        let ids = created.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            .unwrap();
        let node = Uuid::from_slice(ids.value(0)).unwrap();
        graph
            .publish_composite_transaction(crate::CompositeTransactionRequest {
                contract_version: crate::COMPOSITE_TRANSACTION_CONTRACT_VERSION,
                context: WriteContext {
                    operation_uuid: OperationId(Uuid::now_v7()),
                    actor_uuid: None,
                },
                graph_mutations: vec![crate::CompositeGraphMutation::SetNodeProperty {
                    node_uuid: node,
                    property: "score".into(),
                    value: crate::PropValue::Int(7),
                }],
                knowledge: crate::CompositeKnowledgeParticipants::default(),
            })
            .unwrap();
        *graph.uuid_membership_index.lock().unwrap() =
            Some(graphforge_storage::UuidMembershipIndex::open(&graph.dir).unwrap());
        let old_dir = graph.dir.clone();
        let snapshot = graph
            .execute_stream("MATCH (n) RETURN n.node_uuid, n.score")
            .unwrap();
        let request = GraphDeltaCompactionRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
            through_run_sequence: None,
            limits: GraphDeltaCompactionLimits::default(),
            cleanup_after_commit: true,
            cleanup_policy: ProjectRetentionPolicy::default(),
            cleanup_limits: ProjectRetentionLimits::default(),
        };

        let report = graph.compact_graph_delta(&request, None).unwrap();

        assert!(report.cleanup.is_some());
        assert_ne!(graph.dir, old_dir);
        assert!(graph.uuid_membership_index.lock().unwrap().is_none());
        assert!(old_dir.exists(), "active stream retains old workspace");
        drop(snapshot);
        assert!(!old_dir.exists(), "last stream releases old workspace");
    }
}
