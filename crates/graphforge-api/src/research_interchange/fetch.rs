//! Same-Branch fetch inputs; discovery verification precedes native publication.
use crate::{DiscoveryResearchVersionRequest, ResearchOperationReceipt};
pub use graphforge_storage::research_versions::ResearchRemoteFetchBinding;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Verify one advertised remote Branch head without replacing local content or head.
/// This request contains no provider credentials and performs no network access itself.
pub struct FetchResearchBranchRequest<'a> {
    /// Caller-owned durable native operation identity.
    pub operation_uuid: Uuid,
    /// Owning Project CURRENT before a new fetch.
    pub expected_generation_uuid: Uuid,
    /// Existing local Branch whose exact creation identity must match the remote.
    pub branch_uuid: Uuid,
    /// Canonical HTTPS repository URL whose identity must match discovery.
    pub repository_url: &'a str,
    /// Full native research-package verification; the Version must be its Branch head.
    pub discovery: DiscoveryResearchVersionRequest<'a>,
}

/// Durable fetch control result; existing native receipts own exact replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FetchedResearchBranch {
    /// Exact source commitment to supply to an explicit remote upstream preview.
    pub binding: ResearchRemoteFetchBinding,
    /// Existing native operation receipt; fetch never manufactures a Branch head.
    pub receipt: ResearchOperationReceipt,
}
