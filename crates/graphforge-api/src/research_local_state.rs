//! Tags and the current Branch: local registry state published through the
//! ordinary research publication (ADR 0061).
use graphforge_core::GfError;
use graphforge_storage::research_versions::{
    ResearchMutation, ResearchOperation, ResearchOperationReceipt, ResearchRemoteBranch,
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

/// Record the head a Hub confirmed it serves for a local Branch's ref.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordResearchPublicationRequest {
    /// Stable operation identity for exact replay.
    pub operation_uuid: Uuid,
    /// Previewed Project CURRENT.
    pub expected_generation_uuid: Uuid,
    /// Local Branch that was published; the ref is its label.
    pub branch_uuid: Uuid,
    /// Canonical HTTPS repository URL the publish receipt names.
    pub repository_url: String,
    /// Version the Hub confirmed as the ref's head: the Branch head or an ancestor.
    pub head_version_uuid: Uuid,
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

    /// Record the head a publish receipt confirmed for a Branch's ref as its
    /// remote citation, the record clone and fetch maintain. The citation is
    /// built from native authority: only the repository comes from the caller.
    pub fn record_research_publication(
        &mut self,
        request: &RecordResearchPublicationRequest,
        cancellation: &CancellationToken,
    ) -> Result<ResearchOperationReceipt, GfError> {
        let invalid = |message: &str| GfError::Validation(message.into());
        let registry = self.research_version_retention()?;
        let branch = registry
            .branches
            .get(&request.branch_uuid)
            .ok_or_else(|| invalid("the published Branch does not exist"))?;
        let identity = registry
            .identities
            .get(&request.head_version_uuid)
            .ok_or_else(|| invalid("the published head is not a known Version"))?;
        self.publish_research_operation(
            &ResearchOperation {
                operation_uuid: request.operation_uuid,
                expected_generation_uuid: request.expected_generation_uuid,
                mutation: ResearchMutation::RecordPublishedBranch {
                    reference: ResearchRemoteBranch {
                        repository_url: request.repository_url.clone(),
                        project_uuid: branch.project_uuid,
                        ref_name: branch.label.clone(),
                        branch_uuid: request.branch_uuid,
                        head_version_uuid: request.head_version_uuid,
                        identity_sha256: *identity,
                    },
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
