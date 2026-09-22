//! Exact immutable record commitments use the shared canonical envelope.
use super::{
    ClaimRelationRecord, RESEARCH_RECORD_VERSION, ResearchClaimRecord, ResearchDecisionRecord,
};
use crate::KnowledgeError;
use graphforge_core::canonical::{
    CANONICAL_CONTRACT_VERSION, CanonicalDomain, CanonicalWriter, fingerprint,
};
use uuid::Uuid;

impl ResearchClaimRecord {
    /// Commit all immutable metadata, including original producer and origin.
    pub fn fingerprint(&self) -> Result<[u8; 32], KnowledgeError> {
        let mut w = CanonicalWriter::new();
        w.u32(RESEARCH_RECORD_VERSION)?;
        w.raw(self.assertion_uuid.as_bytes())?;
        w.raw(self.conceptual_uuid.as_bytes())?;
        w.text(self.category.as_str())?;
        w.raw(self.creator_uuid.as_bytes())?;
        optional_uuid(&mut w, self.run_uuid)?;
        optional_uuid(&mut w, self.origin_branch_uuid)?;
        optional_uuid(&mut w, self.origin_version_uuid)?;
        w.raw(self.provenance_uuid.as_bytes())?;
        w.i64(self.recorded_at)?;
        finish(CanonicalDomain::ResearchClaim, w)
    }
}
impl ClaimRelationRecord {
    /// Commit the direction, meaning and original producer of a claim relation.
    pub fn fingerprint(&self) -> Result<[u8; 32], KnowledgeError> {
        let mut w = CanonicalWriter::new();
        w.u32(RESEARCH_RECORD_VERSION)?;
        w.raw(self.relation_uuid.as_bytes())?;
        w.raw(self.source_assertion_uuid.as_bytes())?;
        w.raw(self.target_assertion_uuid.as_bytes())?;
        w.text(self.kind.as_str())?;
        w.raw(self.creator_uuid.as_bytes())?;
        w.raw(self.provenance_uuid.as_bytes())?;
        w.i64(self.recorded_at)?;
        finish(CanonicalDomain::ResearchClaimRelation, w)
    }
}
impl ResearchDecisionRecord {
    /// Commit exact publication order, destination authority and explicit decision.
    pub fn fingerprint(&self) -> Result<[u8; 32], KnowledgeError> {
        let mut w = CanonicalWriter::new();
        w.u32(RESEARCH_RECORD_VERSION)?;
        w.u64(self.sequence)?;
        w.raw(self.decision_uuid.as_bytes())?;
        w.raw(self.operation_uuid.as_bytes())?;
        w.raw(&self.request_sha256)?;
        w.raw(self.authority.project_uuid.as_bytes())?;
        optional_uuid(&mut w, self.authority.community_uuid)?;
        w.raw(self.authority.context_uuid.as_bytes())?;
        w.text(self.subject_kind.as_str())?;
        w.raw(self.subject_uuid.as_bytes())?;
        w.text(self.kind.as_str())?;
        w.raw(self.creator_uuid.as_bytes())?;
        optional_uuid(&mut w, self.source_version_uuid)?;
        w.i64(self.recorded_at)?;
        finish(CanonicalDomain::ResearchDecision, w)
    }
}
fn optional_uuid(w: &mut CanonicalWriter, value: Option<Uuid>) -> Result<(), KnowledgeError> {
    if let Some(id) = value {
        w.u8(1)?;
        w.raw(id.as_bytes())?;
    } else {
        w.u8(0)?;
    }
    Ok(())
}
fn finish(domain: CanonicalDomain, w: CanonicalWriter) -> Result<[u8; 32], KnowledgeError> {
    Ok(fingerprint(
        domain,
        CANONICAL_CONTRACT_VERSION,
        &w.finish(),
    )?)
}

impl super::ResearchSuppressionRecord {
    /// Commit exact scoped suppression without changing assertion or graph bytes.
    pub fn fingerprint(&self) -> Result<[u8; 32], KnowledgeError> {
        let mut w = CanonicalWriter::new();
        w.u32(RESEARCH_RECORD_VERSION)?;
        for id in [
            self.suppression_uuid,
            self.assertion_uuid,
            self.context_uuid,
            self.creator_uuid,
            self.provenance_uuid,
        ] {
            w.raw(id.as_bytes())?;
        }
        w.i64(self.recorded_at)?;
        finish(CanonicalDomain::ResearchClaimSuppression, w)
    }
}
