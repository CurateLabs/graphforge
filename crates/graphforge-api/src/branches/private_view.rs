//! Private native execution for already authenticated unpublished Branch content.
use crate::{GfError, GraphForge};
use graphforge_storage::research_versions::{PreparedBranchContent, materialize_prepared_branch};
use std::sync::Arc;

pub(super) fn open(
    owner: &GraphForge,
    prepared: &PreparedBranchContent,
) -> Result<GraphForge, GfError> {
    let directory = Arc::new(tempfile::tempdir().map_err(|e| GfError::Storage(e.to_string()))?);
    let generation = materialize_prepared_branch(
        owner.resolved_generation.container_root(),
        prepared,
        directory.path(),
    )?;
    crate::checkpoints::validate_research_source(&generation)?;
    let mut graph = GraphForge::open_resolved_with_options(
        directory.path().to_path_buf(),
        generation.clone(),
        true,
        owner.write_options.clone(),
        owner.resource_policy.clone(),
        graphforge_storage::ProjectOpenRecoveryEvidence::checkpoint_view(
            generation.generation_uuid(),
        ),
    )?;
    graph.lifecycle_mode =
        graphforge_storage::filesystem_admission::ProjectLifecycleMode::Ephemeral;
    graph.research_materialization = Some(directory);
    graph.resource_policy.memory_budget_bytes = 64 * 1024 * 1024;
    graph.resource_policy.spill_enabled = false;
    Ok(graph)
}
