//! Prepare native Branch edits privately, then publish through Project CURRENT.
use super::{ExecuteResearchBranchRequest, publication, unavailable};
use crate::{CancellationToken, GfError, GraphForge};
use graphforge_storage::research_versions::{
    ResearchMutation, ResearchOperationReceipt, prepare_branch_content,
};
impl GraphForge {
    /// Execute a native graph change against one Branch and publish a fresh
    /// immutable Version. Parent and sibling graphs are never mutation targets.
    pub fn execute_research_branch(
        &mut self,
        request: &ExecuteResearchBranchRequest,
        cancellation: &CancellationToken,
    ) -> Result<ResearchOperationReceipt, GfError> {
        if request.query.len() > 1024 * 1024 || request.version_uuid.is_nil() {
            return Err(GfError::Validation(
                "Branch query or new Version identity is invalid".into(),
            ));
        }
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
        let (graph, mut version) = prepare(self, &command, request.branch_uuid)?;
        cancellation.checkpoint()?;
        graph.execute(&request.query)?;
        cancellation.checkpoint()?;
        version.version_uuid = request.version_uuid;
        version.created_at = request.created_at;
        finish(
            self,
            command,
            &graph,
            version,
            request.operation_uuid,
            cancellation,
        )
    }
}

pub(super) fn prepare(
    owner: &GraphForge,
    command: &publication::Command,
    branch_uuid: uuid::Uuid,
) -> Result<
    (
        GraphForge,
        graphforge_storage::research_versions::ResearchVersionRecord,
    ),
    GfError,
> {
    if !command.registry.branches.contains_key(&branch_uuid) {
        return Err(unavailable());
    }
    let head = command
        .registry
        .heads
        .get(&branch_uuid)
        .ok_or_else(unavailable)?;
    let version = command
        .registry
        .versions
        .get(head)
        .cloned()
        .ok_or_else(unavailable)?;
    let mut graph = crate::research_versions::materialize_version(owner, &version)?;
    graph.read_only = false;
    graph.resource_policy.memory_budget_bytes = 64 * 1024 * 1024;
    graph.resource_policy.spill_enabled = false;
    Ok((graph, version))
}

pub(super) fn finish(
    owner: &mut GraphForge,
    command: publication::Command,
    graph: &GraphForge,
    mut version: graphforge_storage::research_versions::ResearchVersionRecord,
    operation: uuid::Uuid,
    cancellation: &CancellationToken,
) -> Result<ResearchOperationReceipt, GfError> {
    cancellation.checkpoint()?;
    let generation = graph.generation_for_read()?;
    version.content.evidence = crate::research_versions::complete_evidence(&generation)?;
    let mut prepared =
        prepare_branch_content(&command.root, &generation, version, cancellation.flag())?;
    super::baseline::update(owner, graph, &mut prepared, operation, cancellation)?;
    let mutation = ResearchMutation::PublishBranch {
        intent_sha256: command.intent,
        origin_capture: None,
        creation: None,
        version: Box::new(prepared.version.clone()),
    };
    let outcome = command.publish(owner, mutation, cancellation);
    drop(prepared);
    outcome
}
