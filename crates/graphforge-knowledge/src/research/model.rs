//! Typed research metadata references existing immutable assertions and evidence.
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Research interpretation category; evidence remains in Source/Artifact owners.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResearchCategory {
    /// Machine-produced extraction, with producer run/provenance retained.
    MachineExtraction,
    /// Explicit analyst statement.
    AnalystAssertion,
    /// An interpretation of existing material.
    Interpretation,
    /// A provisional claim; hypothesis groups retain their existing owner.
    Hypothesis,
    /// A theoretical claim or explanation.
    Theory,
    /// An analyst annotation expressed as an immutable assertion.
    Annotation,
}

/// Closed relations between immutable claims; no implicit winner is inferred.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClaimRelationKind {
    /// A coexisting alternative.
    AlternativeTo,
    /// An explicit contradiction.
    Contradicts,
    /// A challenge that need not establish a logical contradiction.
    Disputes,
    /// A more specific interpretation.
    Refines,
    /// A successor, validated with the existing supersession owner.
    Supersedes,
    /// Explicit support, independent of confidence and canonical acceptance.
    Supports,
}

/// Exact acceptance authority; context identity is never graph object identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchAuthority {
    /// Owning Project lineage identity.
    pub project_uuid: Uuid,
    /// Optional explicitly named community, not an authentication credential.
    pub community_uuid: Option<Uuid>,
    /// Project research context or Branch UUID.
    pub context_uuid: Uuid,
}

/// Typed subject of a scoped canonical decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResearchSubjectKind {
    /// Public node identity, never a runtime catalog ID.
    Node,
    /// Public relationship identity.
    Edge,
    /// Existing immutable knowledge assertion.
    Assertion,
}

/// Explicit decisions; integration does not imply canonical promotion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResearchDecisionKind {
    /// Introduce research into this context without changing canonical choices.
    Integrate,
    /// Explicitly accept this subject as canonical in this authority scope.
    Promote,
    /// Explicitly revoke this scope's canonical acceptance.
    Revoke,
}

/// Immutable classification and conceptual lineage of an existing assertion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchClaimRecord {
    /// Existing immutable assertion; its claim/evidence bytes are not duplicated.
    pub assertion_uuid: Uuid,
    /// Stable conceptual lineage across immutable successors.
    pub conceptual_uuid: Uuid,
    /// Exact research category.
    pub category: ResearchCategory,
    /// Analyst/agent metadata, not remote authorization.
    pub creator_uuid: Uuid,
    /// Producing run when the assertion comes from machine execution.
    pub run_uuid: Option<Uuid>,
    /// Original Branch, if any.
    pub origin_branch_uuid: Option<Uuid>,
    /// Exact original Version when created or incorporated in versioned research.
    pub origin_version_uuid: Option<Uuid>,
    /// Existing producing provenance event.
    pub provenance_uuid: Uuid,
    /// UTC microseconds at creation.
    pub recorded_at: i64,
}

/// One immutable explicit relation between two existing assertions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimRelationRecord {
    /// Stable identity for exact replay.
    pub relation_uuid: Uuid,
    /// Subject claim.
    pub source_assertion_uuid: Uuid,
    /// Related claim.
    pub target_assertion_uuid: Uuid,
    /// Closed relationship semantics.
    pub kind: ClaimRelationKind,
    /// Creating analyst/agent metadata.
    pub creator_uuid: Uuid,
    /// Producing provenance event.
    pub provenance_uuid: Uuid,
    /// UTC microseconds at creation.
    pub recorded_at: i64,
}

/// Append-only current authority history, separate from frozen research.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchDecisionRecord {
    /// Monotonic native publication order; caller timestamps do not order authority.
    pub sequence: u64,
    /// Immutable event identity.
    pub decision_uuid: Uuid,
    /// Stable publication operation identity.
    pub operation_uuid: Uuid,
    /// Canonical public request commitment for exact replay/conflict detection.
    pub request_sha256: [u8; 32],
    /// Destination authority and context.
    pub authority: ResearchAuthority,
    /// Typed accepted/integrated subject.
    pub subject_kind: ResearchSubjectKind,
    /// Public subject UUID.
    pub subject_uuid: Uuid,
    /// Explicit integration/promotion/revocation decision.
    pub kind: ResearchDecisionKind,
    /// Analyst/agent metadata.
    pub creator_uuid: Uuid,
    /// Exact incorporated source Version, when applicable.
    pub source_version_uuid: Option<Uuid>,
    /// UTC microseconds recorded with the decision.
    pub recorded_at: i64,
}
