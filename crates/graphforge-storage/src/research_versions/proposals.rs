//! Restore-independent immutable submission, review and contribution history.
use super::{GfError, ResearchRegistry, Uuid, invalid};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Destination authority is tagged: a Project UUID is never a Branch UUID.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResearchProposalDestination {
    /// Ultimate owning Project research.
    Project {
        /// Stable owning Project identity.
        project_uuid: Uuid,
    },
    /// Immediate parent Branch research.
    Branch {
        /// Existing immediate parent Branch identity.
        branch_uuid: Uuid,
    },
}

/// One precisely selected typed research field.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchProposalUnit {
    /// Native object family, not a runtime catalog identity.
    pub object_kind: String,
    /// Public object identity.
    pub object_uuid: Uuid,
    /// Exact field name from native Branch inspection.
    pub field: String,
}

/// An immutable selected contribution revision. Absence means selected deletion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchProposalItem {
    /// Stable item identity within this submission.
    pub item_uuid: Uuid,
    /// Exact selected field.
    pub unit: ResearchProposalUnit,
    /// Original stable contribution, preserved when forwarding to another parent.
    pub contribution_uuid: Uuid,
    /// Typed value commitment, or explicit deletion.
    pub value_sha256: Option<[u8; 32]>,
    /// Incorporation baseline used to detect concurrent parent edits.
    pub baseline_sha256: Option<[u8; 32]>,
    /// Selected items required by this item, never silently accepted.
    pub required_items: BTreeSet<Uuid>,
}

/// Frozen submission metadata remains even after an obsolete payload is released.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchProposalRecord {
    /// Immutable proposal identity.
    pub proposal_uuid: Uuid,
    /// Exact original Branch identity.
    pub source_branch_uuid: Uuid,
    /// Exact original Branch Version, distinct from retained selected proof.
    pub source_version_uuid: Uuid,
    /// Immediate parent authority fixed at submission.
    pub destination: ResearchProposalDestination,
    /// Independently retained selected projection; never a Branch head.
    pub payload_version_uuid: Uuid,
    /// Submission operation whose receipt commits this record.
    pub operation_uuid: Uuid,
    /// Recorded actor; not remote authentication.
    pub actor_uuid: Uuid,
    /// UTC microseconds.
    pub created_at: i64,
    /// Bounded explicit submission motivation.
    pub motivation: String,
    /// Bounded caller-recorded policy context.
    pub policy: String,
    /// Canonically ordered selected items.
    pub items: Vec<ResearchProposalItem>,
}

/// Explicit item-level review decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResearchProposalDecision {
    /// Integrate this exact contribution revision.
    Accept,
    /// Decline this revision without mutating the parent.
    Reject,
    /// Keep this item pending for later review.
    Defer,
}

/// One durable review event; later decisions append rather than rewrite history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchProposalReview {
    /// Contiguous native publication order, independent of caller clocks and UUIDs.
    pub sequence: u64,
    /// Publication operation identity and receipt key.
    pub operation_uuid: Uuid,
    /// Reviewed frozen proposal.
    pub proposal_uuid: Uuid,
    /// Exact CURRENT approved by the preview.
    pub preview_generation_uuid: Uuid,
    /// Exact native item/dependency preview commitment.
    pub preview_sha256: [u8; 32],
    /// Explicit use-proposed conflict resolutions in this review.
    pub resolved_conflicts: BTreeSet<Uuid>,
    /// Explicit acknowledgement of external or unverifiable evidence context.
    pub acknowledged_evidence: BTreeSet<Uuid>,
    /// Resulting destination revision, absent when no new content was accepted.
    pub destination_version_uuid: Option<Uuid>,
    /// Recorded reviewer, not authentication.
    pub actor_uuid: Uuid,
    /// UTC microseconds.
    pub created_at: i64,
    /// Bounded review explanation.
    pub explanation: String,
    /// Bounded policy context.
    pub policy: String,
    /// Exactly one decision for every item in the frozen proposal.
    pub decisions: BTreeMap<Uuid, ResearchProposalDecision>,
    /// Exact newly committed accepted mappings, independent of operation replay.
    pub mappings: BTreeSet<Uuid>,
}

/// Immutable destination-scoped evidence preventing a second application.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchAcceptedMapping {
    /// Native identity of this exact destination/contribution/revision mapping.
    pub mapping_uuid: Uuid,
    /// Destination authority; nested integration changes this key.
    pub destination: ResearchProposalDestination,
    /// Exact selected field.
    pub unit: ResearchProposalUnit,
    /// Stable original contribution.
    pub contribution_uuid: Uuid,
    /// Accepted typed value or deletion commitment.
    pub value_sha256: Option<[u8; 32]>,
    /// Original source Branch.
    pub source_branch_uuid: Uuid,
    /// Original exact source Version citation.
    pub source_version_uuid: Uuid,
    /// Exact destination revision citation, not a permanent whole-payload root.
    pub destination_version_uuid: Uuid,
    /// Retained accepted selected evidence, independently rooted.
    pub proof_version_uuid: Uuid,
    /// Committing review operation.
    pub operation_uuid: Uuid,
    /// Original proposal item identity.
    pub item_uuid: Uuid,
}

/// Bounded immutable review history; never part of frozen research content.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchProposalHistory {
    /// Immutable submissions.
    pub proposals: BTreeMap<Uuid, ResearchProposalRecord>,
    /// Immutable reviews indexed by publication operation.
    pub reviews: BTreeMap<Uuid, ResearchProposalReview>,
    /// Permanent exact contribution deduplication commitments.
    pub accepted: BTreeMap<Uuid, ResearchAcceptedMapping>,
    /// Terminal release operation for obsolete frozen payload roots.
    pub released: BTreeMap<Uuid, Uuid>,
}

/// Domain-owner encoded canonical decision history for the same review publication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchDecisionPublication {
    /// Registered native Parquet record version.
    pub record_version: u32,
    /// Registered canonical decision schema identity.
    pub schema_sha256: [u8; 32],
    /// Complete immutable decision history row count.
    pub row_count: u64,
    /// Complete domain-validated Parquet participant.
    pub bytes: Vec<u8>,
}

impl ResearchAcceptedMapping {
    /// Deterministic destination-scoped semantic identity, independent of operation.
    pub fn identity(&self) -> Result<Uuid, GfError> {
        use sha2::{Digest, Sha256};
        let bytes = super::json(&(
            &self.destination,
            &self.unit,
            self.contribution_uuid,
            self.value_sha256,
        ))?;
        let mut hash = Sha256::new();
        hash.update(b"graphforge-accepted-contribution/1");
        hash.update(bytes);
        Ok(graphforge_core::canonical::uuid_v8(hash.finalize().into()))
    }
}

pub(super) fn preserve(before: &ResearchRegistry, after: &ResearchRegistry) -> Result<(), GfError> {
    let old = &before.proposals;
    let new = &after.proposals;
    if old
        .proposals
        .iter()
        .any(|(id, row)| new.proposals.get(id) != Some(row))
        || old
            .reviews
            .iter()
            .any(|(id, row)| new.reviews.get(id) != Some(row))
        || old
            .accepted
            .iter()
            .any(|(id, row)| new.accepted.get(id) != Some(row))
        || old
            .released
            .iter()
            .any(|(id, row)| new.released.get(id) != Some(row))
    {
        return Err(invalid(
            "publication cannot erase or rewrite Proposal acceptance history",
        ));
    }
    Ok(())
}
