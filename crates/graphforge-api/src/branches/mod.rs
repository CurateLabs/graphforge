//! Native independent Branch contexts inside the owning Project CURRENT.
mod baseline;
mod bring;
mod claim_fields;
mod create;
pub(crate) mod domain_bounds;
mod domain_state;
mod domains;
pub(crate) mod edit;
mod fields;
mod import_graph;
mod merge_domains;
mod model;
mod ontology;
mod private_view;
pub(crate) mod publication;
mod reference;
mod selection;
mod suppress;
pub use model::*;

use crate::{CancellationToken, GfError, GraphForge};
use graphforge_storage::research_versions::{
    ResearchBranchRecord, ResearchMutation, ResearchOperationReceipt,
};
use uuid::Uuid;
pub(crate) fn inspect_fields(graph: &GraphForge) -> Result<crate::ExecutionResult, GfError> {
    baseline::inspect(graph)
}

/// An immutable effective Branch view. All query and analyst reads use this graph.
#[derive(Debug)]
pub struct ResearchBranchView {
    record: ResearchBranchRecord,
    version_uuid: Uuid,
    graph: GraphForge,
}
impl ResearchBranchView {
    /// Immutable Branch creation and genealogy, separate from the current head.
    pub fn record(&self) -> &ResearchBranchRecord {
        &self.record
    }
    /// Typed object/field origin, incorporated baseline and local status.
    pub fn fields(&self) -> Result<crate::ExecutionResult, GfError> {
        baseline::inspect(&self.graph)
    }
    /// Version-qualified citations separate from active research.
    pub fn references(&self) -> Result<crate::ExecutionResult, GfError> {
        reference::inspect(&self.graph)
    }
    /// Exact immutable research Version opened by this handle.
    pub fn version_uuid(&self) -> Uuid {
        self.version_uuid
    }
    /// Read-only native facade for the effective Branch graph and domain ledgers.
    /// Mutation attempts are refused by the same native read-only contract.
    pub fn graph(&self) -> &GraphForge {
        &self.graph
    }
}
impl GraphForge {
    /// Inspect the immutable creation selector as exact typed membership and roles.
    /// The original predicate digest is retained separately; this definition never
    /// reevaluates predicates or follows later parent or Branch changes.
    pub fn research_branch_selection(
        &self,
        branch_uuid: Uuid,
    ) -> Result<crate::ExecutionResult, GfError> {
        let current = graphforge_storage::resolve_project_generation(
            self.resolved_generation.container_root(),
        )?;
        let registry = graphforge_storage::research_versions::read_research_registry(&current)?;
        let record = registry
            .branches
            .get(&branch_uuid)
            .ok_or_else(unavailable)?;
        let base = registry
            .versions
            .get(&record.base_version_uuid)
            .ok_or_else(unavailable)?;
        let graph = crate::research_versions::materialize_version(self, base)?;
        baseline::selection(&graph, record)
    }

    /// Open a Branch at its exact current Version without following later changes.
    pub fn open_research_branch(&self, branch_uuid: Uuid) -> Result<ResearchBranchView, GfError> {
        let current = graphforge_storage::resolve_project_generation(
            self.resolved_generation.container_root(),
        )?;
        let registry = graphforge_storage::research_versions::read_research_registry(&current)?;
        let record = registry
            .branches
            .get(&branch_uuid)
            .cloned()
            .ok_or_else(unavailable)?;
        let version_uuid = *registry.heads.get(&branch_uuid).ok_or_else(unavailable)?;
        let version = registry
            .versions
            .get(&version_uuid)
            .ok_or_else(unavailable)?;
        let graph = crate::research_versions::materialize_version(self, version)?;
        Ok(ResearchBranchView {
            record,
            version_uuid,
            graph,
        })
    }

    /// Restore frozen research only within the named Branch; CURRENT history and
    /// parent/sibling research stay intact. Exact request retry returns its receipt.
    pub fn restore_research_branch(
        &mut self,
        request: &RestoreResearchBranchRequest,
        cancellation: &CancellationToken,
    ) -> Result<ResearchOperationReceipt, GfError> {
        let command = publication::begin(
            self,
            request.operation_uuid,
            request.expected_generation_uuid,
            request,
            cancellation,
        )?;
        if let Some(receipt) = command.replay(self)? {
            return Ok(receipt);
        }
        if !command.registry.branches.contains_key(&request.branch_uuid) {
            return Err(unavailable());
        }
        let mut version = command
            .registry
            .versions
            .get(&request.source_version_uuid)
            .cloned()
            .ok_or_else(unavailable)?;
        if version.context_uuid != request.branch_uuid || request.version_uuid.is_nil() {
            return Err(GfError::Validation(
                "Branch restore requires its owning context and a fresh Version".into(),
            ));
        }
        version.version_uuid = request.version_uuid;
        version.created_at = request.created_at;
        let mutation = ResearchMutation::PublishBranch {
            intent_sha256: command.intent,
            origin_capture: None,
            creation: None,
            version: Box::new(version),
        };
        command.publish(self, mutation, cancellation)
    }
}
fn unavailable() -> GfError {
    GfError::Api {
        code: graphforge_core::ApiErrorCode::ResultNotRetained,
        message: "Branch or exact research Version is unavailable".into(),
    }
}
