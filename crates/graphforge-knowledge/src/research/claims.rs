//! Immutable research metadata joins existing assertions rather than copying them.
use super::{
    ClaimRelationKind, ClaimRelationRecord, MAX_RESEARCH_ROWS, ResearchCategory,
    ResearchClaimRecord,
};
use crate::{
    AssertionLedger, AssertionSupersessionLedger, KnowledgeError, invalid, require_uuid, require_v7,
};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

/// Frozen research classifications and explicit claim relations.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResearchClaimLedger {
    claims: Vec<ResearchClaimRecord>,
    relations: Vec<ClaimRelationRecord>,
}
impl ResearchClaimLedger {
    /// Resolve stable conceptual origin through classified and legacy successors.
    pub fn conceptual_origin(
        &self,
        assertion: Uuid,
        supersessions: &AssertionSupersessionLedger,
    ) -> Result<Uuid, KnowledgeError> {
        let concepts = self
            .claims
            .iter()
            .map(|row| (row.assertion_uuid, row.conceptual_uuid))
            .collect();
        super::lineage::resolve(&concepts, supersessions)?.get(&assertion).copied().unwrap_or(Some(assertion)).ok_or_else(|| invalid("research_claim.conceptual_uuid", "multiple incompatible conceptual ancestors require an explicit alternative, not revision"))
    }
    /// Encode frozen classifications as native Arrow.
    pub fn claim_batch(&self) -> Result<arrow::record_batch::RecordBatch, KnowledgeError> {
        super::encoding::claim_batch(&self.claims)
    }
    /// Encode explicit claim relations as native Arrow.
    pub fn relation_batch(&self) -> Result<arrow::record_batch::RecordBatch, KnowledgeError> {
        super::encoding::relation_batch(&self.relations)
    }
    /// Decode bounded frozen metadata, validating immutable identities.
    pub fn from_batches(
        claims: &[arrow::record_batch::RecordBatch],
        relations: &[arrow::record_batch::RecordBatch],
    ) -> Result<Self, KnowledgeError> {
        Self::new(
            super::encoding::claim_rows(claims)?,
            super::encoding::relation_rows(relations)?,
        )
    }
    /// Validate identities and deterministic ordering without inventing legacy metadata.
    pub fn new(
        mut claims: Vec<ResearchClaimRecord>,
        mut relations: Vec<ClaimRelationRecord>,
    ) -> Result<Self, KnowledgeError> {
        bounded(claims.len(), "research_claims")?;
        bounded(relations.len(), "claim_relations")?;
        let mut ids = HashSet::new();
        for row in &claims {
            validate_claim(row)?;
            if !ids.insert(row.assertion_uuid) {
                return Err(KnowledgeError::Duplicate("research_claim.assertion_uuid"));
            }
        }
        ids.clear();
        for row in &relations {
            validate_relation(row)?;
            if !ids.insert(row.relation_uuid) {
                return Err(KnowledgeError::Duplicate("claim_relation.relation_uuid"));
            }
        }
        claims.sort_by_key(|row| row.assertion_uuid);
        relations.sort_by_key(|row| row.relation_uuid);
        Ok(Self { claims, relations })
    }

    /// Exact metadata replay is idempotent; immutable identity rewrites conflict.
    pub fn merge(&self, staged: &Self) -> Result<Self, KnowledgeError> {
        let claims = append(
            &self.claims,
            &staged.claims,
            |row| row.assertion_uuid,
            "research_claim.assertion_uuid",
        )?;
        let relations = append(
            &self.relations,
            &staged.relations,
            |row| row.relation_uuid,
            "claim_relation.relation_uuid",
        )?;
        Self::new(claims, relations)
    }

    /// Validate joins against the existing immutable owners before publication.
    pub fn validate_references(
        &self,
        assertions: &AssertionLedger,
        supersessions: &AssertionSupersessionLedger,
    ) -> Result<(), KnowledgeError> {
        let assertions: HashMap<_, _> = assertions
            .assertions
            .iter()
            .map(|row| (row.assertion_uuid, row))
            .collect();
        let concepts: HashMap<_, _> = self
            .claims
            .iter()
            .map(|row| (row.assertion_uuid, row.conceptual_uuid))
            .collect();
        let successors: HashSet<_> = supersessions
            .relations()
            .iter()
            .map(|row| (row.replacement_assertion_uuid, row.prior_assertion_uuid))
            .collect();
        for row in &self.claims {
            let assertion = assertions
                .get(&row.assertion_uuid)
                .ok_or(KnowledgeError::Dangling("research_claim.assertion_uuid"))?;
            if assertion.provenance_uuid != row.provenance_uuid
                || assertion.recorded_at_micros != row.recorded_at
            {
                return Err(invalid(
                    "research_claim.origin",
                    "classification must preserve the assertion's producing provenance and time",
                ));
            }
        }
        super::lineage::validate(&concepts, supersessions)?;
        for row in &self.relations {
            if !assertions.contains_key(&row.source_assertion_uuid)
                || !assertions.contains_key(&row.target_assertion_uuid)
            {
                return Err(KnowledgeError::Dangling("claim_relation.assertion_uuid"));
            }
            if row.kind == ClaimRelationKind::Supersedes
                && !successors.contains(&(row.source_assertion_uuid, row.target_assertion_uuid))
            {
                return Err(KnowledgeError::Dangling("claim_relation.supersession"));
            }
        }
        Ok(())
    }

    /// Existing assertions with no row remain unclassified/statusless.
    #[must_use]
    pub fn claims(&self) -> &[ResearchClaimRecord] {
        &self.claims
    }
    /// All explicit alternatives and relations; no winner is inferred.
    #[must_use]
    pub fn relations(&self) -> &[ClaimRelationRecord] {
        &self.relations
    }
}

fn bounded(observed: usize, participant: &'static str) -> Result<(), KnowledgeError> {
    if observed > MAX_RESEARCH_ROWS {
        return Err(KnowledgeError::Limit {
            participant,
            observed,
            limit: MAX_RESEARCH_ROWS,
        });
    }
    Ok(())
}
fn append<T: Clone + PartialEq>(
    existing: &[T],
    staged: &[T],
    id: impl Fn(&T) -> Uuid,
    field: &'static str,
) -> Result<Vec<T>, KnowledgeError> {
    let by_id: HashMap<_, _> = existing.iter().map(|row| (id(row), row)).collect();
    let mut rows = existing.to_vec();
    for row in staged {
        if let Some(old) = by_id.get(&id(row)) {
            if *old != row {
                return Err(KnowledgeError::Conflict(field));
            }
        } else {
            bounded(rows.len() + 1, field)?;
            rows.push(row.clone());
        }
    }
    Ok(rows)
}
fn validate_claim(row: &ResearchClaimRecord) -> Result<(), KnowledgeError> {
    require_v7(row.assertion_uuid, "assertion_uuid")?;
    for (id, field) in [
        (row.conceptual_uuid, "conceptual_uuid"),
        (row.creator_uuid, "creator_uuid"),
        (row.provenance_uuid, "provenance_uuid"),
    ] {
        require_uuid(id, field)?;
    }
    for (id, field) in [
        (row.run_uuid, "run_uuid"),
        (row.origin_branch_uuid, "origin_branch_uuid"),
        (row.origin_version_uuid, "origin_version_uuid"),
    ] {
        if let Some(id) = id {
            require_uuid(id, field)?;
        }
    }
    if row.category == ResearchCategory::MachineExtraction && row.run_uuid.is_none() {
        return Err(invalid(
            "research_claim.run_uuid",
            "machine extraction requires its producer run",
        ));
    }
    if row.origin_branch_uuid.is_some() && row.origin_version_uuid.is_none() {
        return Err(invalid(
            "research_claim.origin_version_uuid",
            "Branch origin requires an exact Version",
        ));
    }
    Ok(())
}
fn validate_relation(row: &ClaimRelationRecord) -> Result<(), KnowledgeError> {
    for (id, field) in [
        (row.relation_uuid, "relation_uuid"),
        (row.source_assertion_uuid, "source_assertion_uuid"),
        (row.target_assertion_uuid, "target_assertion_uuid"),
    ] {
        require_v7(id, field)?;
    }
    require_uuid(row.creator_uuid, "creator_uuid")?;
    require_uuid(row.provenance_uuid, "provenance_uuid")?;
    if row.source_assertion_uuid == row.target_assertion_uuid {
        return Err(invalid(
            "claim_relation.target_assertion_uuid",
            "a claim relation requires distinct assertions",
        ));
    }
    Ok(())
}
