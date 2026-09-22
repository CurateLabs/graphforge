//! One publication installs the reviewed Branch and immutable review evidence.
use super::{
    GfError, RegisterResearchVersion, ResearchRegistry, ResearchUpstreamReview,
    ResearchVersionRecord, Uuid, invalid,
};

pub(super) fn apply(
    root: &std::path::Path,
    registry: &mut ResearchRegistry,
    review: &ResearchUpstreamReview,
    source_capture: Option<&RegisterResearchVersion>,
    version: &ResearchVersionRecord,
) -> Result<Uuid, GfError> {
    let branch = registry
        .branches
        .get(&review.branch_uuid)
        .cloned()
        .ok_or_else(|| invalid("upstream destination Branch is unavailable"))?;
    if review.sequence != registry.upstream.reviews.len() as u64 + 1
        || registry
            .upstream
            .reviews
            .contains_key(&review.operation_uuid)
        || registry.heads.get(&review.branch_uuid) != Some(&review.prior_version_uuid)
        || review.original_base_version_uuid != branch.base_version_uuid
        || review.version_uuid != version.version_uuid
        || version.context_uuid != branch.branch_uuid
        || review.preview_generation_uuid
            != crate::resolve_project_generation(root)?.generation_uuid()
    {
        return Err(invalid(
            "upstream review does not identify the exact local Branch head and publication",
        ));
    }
    match (branch.parent_branch_uuid, source_capture) {
        (Some(parent), None) => {
            if registry.heads.get(&parent) != Some(&review.upstream_version_uuid) {
                return Err(invalid(
                    "upstream review does not identify the exact immediate parent head",
                ));
            }
        }
        (None, Some(capture)) => {
            if capture.version_uuid != review.upstream_version_uuid
                || capture.context_uuid != branch.project_uuid
                || capture.source_generation_uuid != review.preview_generation_uuid
            {
                return Err(invalid(
                    "upstream Project capture differs from its reviewed authority",
                ));
            }
            super::branches::stage_origin(root, registry, capture)?;
        }
        _ => {
            return Err(invalid(
                "upstream origin must be its immediate parent Branch or complete Project",
            ));
        }
    }
    let id = super::branches::publish(root, registry, None, version)?;
    if let Some(capture) = source_capture {
        // Keep exact immutable identity, not unrelated whole-parent payload.
        registry.versions.remove(&capture.version_uuid);
    }
    registry
        .upstream
        .reviews
        .insert(review.operation_uuid, review.clone());
    Ok(id)
}

pub(super) fn validate_preview_generation(
    request: &super::ResearchOperation,
) -> Result<(), GfError> {
    if let super::ResearchMutation::UpdateBranch { review, .. } = &request.mutation
        && (review.operation_uuid != request.operation_uuid
            || review.preview_generation_uuid != request.expected_generation_uuid)
    {
        return Err(super::invalid(
            "upstream review identity differs from the publication request",
        ));
    }
    Ok(())
}
