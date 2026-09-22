//! Append-only scope events preserve immutable assertions and shared graph objects.
use super::{MAX_RESEARCH_ROWS, ResearchSuppressionRecord, encoding};
use crate::{KnowledgeError, require_uuid, require_v7};
use arrow::record_batch::RecordBatch;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

/// Frozen suppression history, independent of supported/canonical status.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResearchSuppressionLedger {
    events: Vec<ResearchSuppressionRecord>,
}
impl ResearchSuppressionLedger {
    /// Validate exact immutable event identities and deterministic ordering.
    pub fn new(mut events: Vec<ResearchSuppressionRecord>) -> Result<Self, KnowledgeError> {
        if events.len() > MAX_RESEARCH_ROWS {
            return Err(KnowledgeError::Limit {
                participant: "claim_suppressions",
                observed: events.len(),
                limit: MAX_RESEARCH_ROWS,
            });
        }
        let mut ids = HashSet::new();
        for row in &events {
            require_v7(row.suppression_uuid, "suppression_uuid")?;
            require_v7(row.assertion_uuid, "assertion_uuid")?;
            for (id, field) in [
                (row.context_uuid, "context_uuid"),
                (row.creator_uuid, "creator_uuid"),
                (row.provenance_uuid, "provenance_uuid"),
            ] {
                require_uuid(id, field)?;
            }
            if !ids.insert(row.suppression_uuid) {
                return Err(KnowledgeError::Duplicate("suppression_uuid"));
            }
        }
        events.sort_by_key(|row| row.suppression_uuid);
        Ok(Self { events })
    }
    /// Merge selected histories without rewriting an existing event.
    pub fn merge(&self, staged: &Self) -> Result<Self, KnowledgeError> {
        let old: HashMap<_, _> = self
            .events
            .iter()
            .map(|row| (row.suppression_uuid, row))
            .collect();
        let mut rows = self.events.clone();
        for row in &staged.events {
            if let Some(existing) = old.get(&row.suppression_uuid) {
                if *existing != row {
                    return Err(KnowledgeError::Conflict("suppression_uuid"));
                }
            } else {
                if rows.len() == MAX_RESEARCH_ROWS {
                    return Err(KnowledgeError::Limit {
                        participant: "claim_suppressions",
                        observed: rows.len() + 1,
                        limit: MAX_RESEARCH_ROWS,
                    });
                }
                rows.push(row.clone());
            }
        }
        Self::new(rows)
    }
    /// Inspect retained history including inherited events.
    #[must_use]
    pub fn events(&self) -> &[ResearchSuppressionRecord] {
        &self.events
    }
    /// Suppression in the exact context or its frozen inherited context chain.
    #[must_use]
    pub fn is_suppressed(&self, assertion: Uuid, contexts: &HashSet<Uuid>) -> bool {
        self.events
            .iter()
            .any(|row| row.assertion_uuid == assertion && contexts.contains(&row.context_uuid))
    }
    /// Encode typed scope events as Arrow.
    pub fn batch(&self) -> Result<RecordBatch, KnowledgeError> {
        encoding::suppression_batch(&self.events)
    }
    /// Decode bounded immutable scope events.
    pub fn from_batches(batches: &[RecordBatch]) -> Result<Self, KnowledgeError> {
        Self::new(encoding::suppression_rows(batches)?)
    }
}
