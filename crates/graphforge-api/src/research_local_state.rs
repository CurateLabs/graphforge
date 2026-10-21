//! Tags and the current Branch: local registry state published through the
//! ordinary research publication (ADR 0061).
use graphforge_core::GfError;
use graphforge_storage::research_versions::{
    ResearchMutation, ResearchOperation, ResearchOperationReceipt,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{CancellationToken, GraphForge};

/// Create or delete one tag on an immutable Version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetResearchTagRequest {
    /// Stable operation identity for exact replay.
    pub operation_uuid: Uuid,
    /// Previewed Project CURRENT; never silently refreshed for a mutation.
    pub expected_generation_uuid: Uuid,
    /// Tag name; a valid discovery ref.
    pub name: String,
    /// Version the tag names, or `None` to delete the tag.
    pub version_uuid: Option<Uuid>,
}

/// Select the Branch the porcelain verbs act on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SelectResearchBranchRequest {
    /// Stable operation identity for exact replay.
    pub operation_uuid: Uuid,
    /// Previewed Project CURRENT.
    pub expected_generation_uuid: Uuid,
    /// Existing Branch with a head.
    pub branch_uuid: Uuid,
}

impl GraphForge {
    /// Create a tag on a Version, or delete one. A tag never moves: creating an
    /// existing tag with another target is refused with `GF_RESEARCH_TAG_EXISTS`.
    pub fn set_research_tag(
        &mut self,
        request: &SetResearchTagRequest,
        cancellation: &CancellationToken,
    ) -> Result<ResearchOperationReceipt, GfError> {
        graphforge_discovery::check_ref_name(&request.name)
            .map_err(|_| GfError::Validation("a tag name must be a valid ref name".into()))?;
        self.commit_research_version_operation(
            ResearchOperation {
                operation_uuid: request.operation_uuid,
                expected_generation_uuid: request.expected_generation_uuid,
                mutation: ResearchMutation::SetTag {
                    name: request.name.clone(),
                    version_uuid: request.version_uuid,
                },
            },
            cancellation,
        )
    }

    /// Select the Branch the porcelain verbs act on.
    pub fn select_research_branch(
        &mut self,
        request: &SelectResearchBranchRequest,
        cancellation: &CancellationToken,
    ) -> Result<ResearchOperationReceipt, GfError> {
        self.commit_research_version_operation(
            ResearchOperation {
                operation_uuid: request.operation_uuid,
                expected_generation_uuid: request.expected_generation_uuid,
                mutation: ResearchMutation::SelectBranch {
                    branch_uuid: request.branch_uuid,
                },
            },
            cancellation,
        )
    }
}
