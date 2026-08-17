//! Thin GraphForge facade over Rust-owned retention/GC and delta compaction.
//!
//! Bindings and CLI must call these methods rather than reimplementing
//! reachability, cleanup, delta replay, or compaction logic.

use graphforge_core::{GfError, ProjectErrorCode};
use graphforge_storage::{
    GraphDeltaCompactionPolicy, GraphDeltaCompactionReport, GraphDeltaCompactionRequest,
    GraphDeltaCompactionStatus, GraphDeltaJournalLimits, ProjectCleanupReport,
    ProjectReachabilityReport, ProjectRetentionLimits, ProjectRetentionPolicy,
    compact_graph_delta_with_mode, graph_delta_compaction_status_with_mode,
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

    /// Compact a contiguous verified delta prefix into a new Parquet generation.
    pub fn compact_graph_delta(
        &self,
        request: &GraphDeltaCompactionRequest,
        cancellation: Option<&CancellationToken>,
    ) -> Result<GraphDeltaCompactionReport, GfError> {
        let root = self.require_mutable_project_root()?;
        compact_graph_delta_with_mode(
            root,
            request,
            cancellation.map(CancellationToken::flag),
            self.lifecycle_mode,
        )
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
        let graph = GraphForge::new(directory.path().to_str()).unwrap();
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
        use graphforge_storage::{
            GraphDeltaOp, GraphDeltaOpKind, GraphDeltaPayload, GraphDeltaPublishRequest,
            ProjectCapability, ProjectGenerationRequest, ProjectStageOutcome,
            ReconstructedGraphState,
        };

        let graph = GraphForge::new(None).unwrap();
        let root = graph.resolved_generation.container_root();
        let workspace = tempfile::tempdir().unwrap();
        graphforge_storage::stage_base_graph_workspace(
            workspace.path(),
            &[
                ("topology/nodes.parquet", b"nodes"),
                ("topology/edges.parquet", b"edges"),
            ],
            Some(&ReconstructedGraphState::default()),
        )
        .unwrap();
        let (_, files) = graphforge_storage::capture_graph_files(workspace.path()).unwrap();
        let mut participants = graphforge_storage::empty_workspace_participants().unwrap();
        participants.insert(0, files);
        let base = ProjectGenerationRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
            capabilities: vec![
                ProjectCapability {
                    capability_id: "graph".into(),
                    capability_version: 1,
                },
                ProjectCapability {
                    capability_id: "workspace".into(),
                    capability_version: 1,
                },
            ],
            participants,
        };
        let ProjectStageOutcome::Staged(staged) =
            graphforge_storage::stage_project_generation_with_graph_tree_mode(
                root,
                &base,
                Some(workspace.path()),
                graph.lifecycle_mode,
            )
            .unwrap()
        else {
            panic!("base publication unexpectedly replayed");
        };
        staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .unwrap()
            .publish()
            .unwrap();
        graphforge_storage::publish_graph_delta_with_mode(
            root,
            &GraphDeltaPublishRequest {
                transaction_uuid: Uuid::now_v7(),
                generation_uuid: Uuid::now_v7(),
                run_uuid: Uuid::now_v7(),
                operations: vec![GraphDeltaOp {
                    operation_uuid: Uuid::now_v7(),
                    kind: GraphDeltaOpKind::UpsertNode,
                    payload: GraphDeltaPayload::UpsertNode {
                        node_uuid: Uuid::now_v7().hyphenated().to_string(),
                        type_ids: vec![1],
                    },
                }],
                limits: GraphDeltaJournalLimits::default(),
            },
            graph.lifecycle_mode,
        )
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
    }
}
