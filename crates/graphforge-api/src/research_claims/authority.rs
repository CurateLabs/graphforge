//! Derive Project authority from the native owner, never caller-provided UUIDs.
use super::{ResearchContext, ledger};
use crate::{GfError, GraphForge};
use graphforge_knowledge::research::ResearchAuthority;
use graphforge_storage::{ResolvedProjectGeneration, research_versions::read_research_registry};
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub(crate) fn project_uuid(g: &ResolvedProjectGeneration) -> Result<Uuid, GfError> {
    let registry = read_research_registry(g)?;
    let decisions = ledger::read_decisions(g)?;
    let registered = registry
        .branches
        .values()
        .next()
        .map(|branch| branch.project_uuid);
    let recorded = decisions
        .events()
        .first()
        .map(|event| event.authority.project_uuid);
    let imported = registry.interchange.values().next().map(|archive| {
        archive
            .fork_project_uuid
            .unwrap_or(archive.source_project_uuid)
    });
    let id = if let Some(id) = imported.or(registered).or(recorded) {
        id
    } else {
        let identity = graphforge_storage::summarize_research_project(g.container_root())?.identity;
        let mut digest = Sha256::new();
        digest.update(b"graphforge-project-lineage/1");
        digest.update(identity.volume_serial.to_le_bytes());
        digest.update(identity.file_id_hex.as_bytes());
        graphforge_core::canonical::uuid_v8(digest.finalize().into())
    };
    if registry
        .branches
        .values()
        .any(|branch| branch.project_uuid != id)
        || decisions
            .events()
            .iter()
            .any(|event| event.authority.project_uuid != id)
    {
        return Err(GfError::Validation(
            "research authority does not match the owning Project".into(),
        ));
    }
    Ok(id)
}
pub(crate) fn resolve(
    g: &ResolvedProjectGeneration,
    context: &ResearchContext,
    community_uuid: Option<Uuid>,
) -> Result<ResearchAuthority, GfError> {
    let project_uuid = project_uuid(g)?;
    if community_uuid.is_some_and(|id| id.is_nil()) {
        return Err(GfError::Validation(
            "community identity must be non-nil".into(),
        ));
    }
    let context_uuid = match context {
        ResearchContext::Project => project_uuid,
        ResearchContext::Branch { branch_uuid } => {
            let registry = read_research_registry(g)?;
            if !registry.branches.contains_key(branch_uuid) {
                return Err(GfError::Validation(
                    "research Branch context is unavailable".into(),
                ));
            }
            *branch_uuid
        }
    };
    Ok(ResearchAuthority {
        project_uuid,
        community_uuid,
        context_uuid,
    })
}
pub(super) fn with_context<T>(
    owner: &GraphForge,
    generation: &ResolvedProjectGeneration,
    context: &ResearchContext,
    f: impl FnOnce(&GraphForge, &ResolvedProjectGeneration) -> Result<T, GfError>,
) -> Result<T, GfError> {
    match context {
        ResearchContext::Project => f(owner, generation),
        ResearchContext::Branch { branch_uuid } => {
            let registry = read_research_registry(generation)?;
            let head = registry
                .heads
                .get(branch_uuid)
                .and_then(|id| registry.versions.get(id))
                .ok_or_else(|| GfError::Validation("research Branch head is unavailable".into()))?;
            let graph = crate::research_versions::materialize_version(owner, head)?;
            let generation = graph.generation_for_read()?;
            f(&graph, &generation)
        }
    }
}

pub(crate) fn require_owner(owner: &GraphForge) -> Result<(), GfError> {
    if owner.read_only && owner.research_materialization.is_some() {
        return Err(GfError::Validation("contextual research inspection requires the owning Project facade and an explicit Branch context".into()));
    }
    Ok(())
}
