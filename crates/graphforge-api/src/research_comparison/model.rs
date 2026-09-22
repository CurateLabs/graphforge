//! Exact native comparison inputs. None of these operations publishes research.
use serde::{Deserialize, Serialize};
use uuid::Uuid;
/// Research state to resolve once against the owning Project's pinned CURRENT.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResearchComparisonEndpoint {
    /// Owning Project research at the pinned generation.
    Project,
    /// Branch head resolved once; later publications do not change this page.
    Branch {
        /// Stable Branch context.
        branch_uuid: Uuid,
    },
    /// Exact retained complete or selected research Version.
    Version {
        /// Immutable Version identity.
        version_uuid: Uuid,
    },
}
/// Exact native object/field unit; runtime catalog identifiers are never accepted.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchFieldIdentity {
    /// Native object kind, such as node, edge, assertion, source or ontology.
    pub object_kind: String,
    /// Public object UUID, or native semantic ontology identity commitment.
    pub object_uuid: Uuid,
    /// Native field name from Branch field inspection.
    pub field: String,
}
/// Explicit accepted subset mapping for comparison before Proposal receipts supply it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchAcceptedContribution {
    /// Exact source Version that supplied this contribution.
    pub source_version_uuid: Uuid,
    /// Native per-field contribution identity from that Version.
    pub contribution_uuid: Uuid,
    /// Exact accepted destination Version, independently qualified per unit.
    pub destination_version_uuid: Uuid,
    /// One accepted typed object/field, never a whole-Version shortcut.
    pub unit: ResearchFieldIdentity,
}
/// Data-bearing output detail.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResearchComparisonDetail {
    /// Deterministically sorted item-level change and dependency rows.
    Changes,
    /// One Arrow row with semantic indicators, counts and exact endpoint identities.
    Summary,
}
/// Explicit authority snapshot; Version creation time never implies canonical acceptance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchComparisonAuthority {
    /// Native Project or Branch context whose current history is inspected.
    pub context: crate::ResearchContext,
    /// Optional exact community scope.
    pub community_uuid: Option<Uuid>,
    /// Inclusive global decision sequence; None means the pinned current history.
    pub through_sequence: Option<u64>,
}
/// Bounded native semantic research comparison, local/earlier endpoint on the left.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchComparisonRequest {
    /// Local Branch or earlier research endpoint.
    pub left: ResearchComparisonEndpoint,
    /// Upstream, other Branch, or later research endpoint.
    pub right: ResearchComparisonEndpoint,
    /// Explicit scoped canonical history, independent of frozen left content.
    pub left_authority: Option<ResearchComparisonAuthority>,
    /// Explicit scoped canonical history, independent of frozen right content.
    pub right_authority: Option<ResearchComparisonAuthority>,
    /// Changes or summary, both native Arrow results.
    pub detail: ResearchComparisonDetail,
    /// Independently authenticated accepted units from possibly different Versions.
    #[serde(default)]
    pub accepted: Vec<ResearchAcceptedContribution>,
    /// Maximum admitted combined endpoint fields, 1..=40000.
    pub max_fields: usize,
    /// Maximum semantic field working bytes and, independently, output bytes, 1024..=67108864.
    pub max_bytes: usize,
    /// Bounded page size, 1..=1000.
    pub page_size: usize,
    /// Opaque native continuation; fails if request or resolved authority changes.
    pub after: Option<String>,
}
