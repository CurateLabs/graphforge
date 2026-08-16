//! Thin GraphForge facade over Rust-owned retention/GC and delta compaction.
//!
//! Bindings and CLI must call these methods rather than reimplementing
//! reachability, cleanup, delta replay, or compaction logic.

use graphforge_core::{GfError, ProjectErrorCode};
use graphforge_storage::{
    GraphDeltaCompactionPolicy, GraphDeltaCompactionReport, GraphDeltaCompactionRequest,
    GraphDeltaCompactionStatus, GraphDeltaJournalLimits, ProjectCleanupReport,
    ProjectReachabilityReport, ProjectRetentionLimits, ProjectRetentionPolicy, compact_graph_delta,
    graph_delta_compaction_status, inspect_project_reachability, preview_graph_delta_compaction,
    preview_project_cleanup,
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
        inspect_project_reachability(root, policy, limits)
    }

    /// Preview retention/GC candidates without removing anything.
    pub fn preview_project_cleanup(
        &self,
        policy: ProjectRetentionPolicy,
        limits: ProjectRetentionLimits,
    ) -> Result<ProjectCleanupReport, GfError> {
        let root = self.require_mutable_project_root()?;
        preview_project_cleanup(root, policy, limits)
    }

    /// Execute retention/GC for unreachable generations using the shared oracle.
    pub fn execute_project_cleanup(
        &self,
        policy: ProjectRetentionPolicy,
        limits: ProjectRetentionLimits,
    ) -> Result<ProjectCleanupReport, GfError> {
        let root = self.require_mutable_project_root()?;
        graphforge_storage::execute_project_cleanup(root, policy, limits)
    }

    /// Report whether CURRENT's verified delta chain should compact under policy.
    pub fn graph_delta_compaction_status(
        &self,
        policy: GraphDeltaCompactionPolicy,
        limits: GraphDeltaJournalLimits,
    ) -> Result<GraphDeltaCompactionStatus, GfError> {
        let root = self.require_mutable_project_root()?;
        graph_delta_compaction_status(root, policy, limits)
    }

    /// Preview delta compaction without publishing CURRENT.
    pub fn preview_graph_delta_compaction(
        &self,
        request: &GraphDeltaCompactionRequest,
        cancellation: Option<&CancellationToken>,
    ) -> Result<GraphDeltaCompactionReport, GfError> {
        let root = self.require_mutable_project_root()?;
        preview_graph_delta_compaction(root, request, cancellation.map(CancellationToken::flag))
    }

    /// Compact a contiguous verified delta prefix into a new Parquet generation.
    pub fn compact_graph_delta(
        &self,
        request: &GraphDeltaCompactionRequest,
        cancellation: Option<&CancellationToken>,
    ) -> Result<GraphDeltaCompactionReport, GfError> {
        let root = self.require_mutable_project_root()?;
        compact_graph_delta(root, request, cancellation.map(CancellationToken::flag))
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
}
