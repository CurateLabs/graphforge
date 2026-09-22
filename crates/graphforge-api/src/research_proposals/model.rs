//! Closed native requests; bindings transport these without interpreting review.
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Inspect frozen selected items against one pinned destination revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreviewResearchProposalRequest {
    /// Exact immutable submitted proposal.
    pub proposal_uuid: Uuid,
}

/// Freeze explicitly selected fields from one exact Branch Version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubmitResearchProposalRequest {
    /// Stable public operation identity for exact replay.
    pub operation_uuid: Uuid,
    /// Previewed owning Project CURRENT.
    pub expected_generation_uuid: Uuid,
    /// Fresh immutable proposal identity.
    pub proposal_uuid: Uuid,
    /// Owning analyst Branch, whose immediate parent is the destination.
    pub source_branch_uuid: Uuid,
    /// Exact source Version, not an implicit live head.
    pub source_version_uuid: Uuid,
    /// Authenticated frozen native Slice defining selected objects and evidence.
    pub frozen_ipc: Vec<u8>,
    /// Exact selected native fields, at most 256; no implicit whole-Version selection.
    pub fields: Vec<crate::ResearchFieldIdentity>,
    /// Recorded actor, not remote authentication.
    pub actor_uuid: Uuid,
    /// UTC microseconds.
    pub created_at: i64,
    /// Bounded submission motivation.
    pub motivation: String,
    /// Bounded caller policy context.
    pub policy: String,
}

/// Publish exact reviewed item decisions against their native preview.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReviewResearchProposalRequest {
    /// Stable public operation identity.
    pub operation_uuid: Uuid,
    /// Exact CURRENT from the preview.
    pub expected_generation_uuid: Uuid,
    /// Immutable frozen submission.
    pub proposal_uuid: Uuid,
    /// Native preview commitment, not a caller assertion of conflict freedom.
    pub preview_sha256: [u8; 32],
    /// One explicit decision for every submitted item.
    pub decisions: std::collections::BTreeMap<
        Uuid,
        graphforge_storage::research_versions::ResearchProposalDecision,
    >,
    /// Explicit use-proposed resolution for conflicting accepted fields.
    pub resolve_conflicts: std::collections::BTreeSet<Uuid>,
    /// Explicitly acknowledge external-only or unverifiable evidence context.
    /// This never changes its availability or claims that bytes were retained.
    pub acknowledge_evidence: std::collections::BTreeSet<Uuid>,
    /// Optional explicit canonical promotions, separate from integration.
    pub promotions: Vec<crate::ResearchDecisionInput>,
    /// Optional exact destination community scope.
    pub community_uuid: Option<Uuid>,
    /// Recorded reviewer, not remote authentication.
    pub actor_uuid: Uuid,
    /// UTC microseconds; native publication order is authoritative.
    pub created_at: i64,
    /// Bounded review explanation.
    pub explanation: String,
    /// Bounded caller policy context.
    pub policy: String,
}

/// Release an obsolete frozen proposal payload without expiring its receipts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseResearchProposalRequest {
    /// Stable operation identity for exact replay.
    pub operation_uuid: Uuid,
    /// Required owning Project CURRENT.
    pub expected_generation_uuid: Uuid,
    /// Frozen payload to withdraw; accepted proof roots remain independent.
    pub proposal_uuid: Uuid,
}

/// Native Arrow history detail for one immutable proposal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResearchProposalHistoryDetail {
    /// Item disposition and accepted divergence at the current Branch head.
    Items,
    /// Ordered immutable item-level review decisions.
    Reviews,
    /// Exact permanent contribution-to-destination mappings.
    Accepted,
}

/// Bounded, generation-qualified native proposal history inspection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchProposalHistoryRequest {
    /// Immutable proposal whose item and review history is inspected.
    pub proposal_uuid: Uuid,
    /// Explicit data-bearing view.
    pub detail: ResearchProposalHistoryDetail,
    /// Maximum returned rows, 1..=1000.
    pub page_size: usize,
    /// Native continuation bound to the request and exact CURRENT.
    pub after: Option<String>,
}
