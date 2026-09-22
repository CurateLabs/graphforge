//! Append-only decisions are ordered by publication, not caller-provided dates.
use super::{
    MAX_RESEARCH_ROWS, ResearchAuthority, ResearchDecisionKind, ResearchDecisionRecord,
    ResearchSubjectKind,
};
use crate::{KnowledgeError, invalid, require_uuid, require_v7};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

/// Immutable canonical/integration history across all contexts of one container.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResearchDecisionLedger {
    events: Vec<ResearchDecisionRecord>,
}
impl ResearchDecisionLedger {
    /// Encode complete current history as native Arrow.
    pub fn batch(&self) -> Result<arrow::record_batch::RecordBatch, KnowledgeError> {
        super::encoding::decision_batch(&self.events)
    }
    /// Decode complete history; filtered views are not valid publication ledgers.
    pub fn from_batches(
        batches: &[arrow::record_batch::RecordBatch],
    ) -> Result<Self, KnowledgeError> {
        Self::new(super::encoding::decision_rows(batches)?)
    }
    /// Validate complete history; a missing ordinal is corruption, not expiry.
    pub fn new(mut events: Vec<ResearchDecisionRecord>) -> Result<Self, KnowledgeError> {
        if events.len() > MAX_RESEARCH_ROWS {
            return Err(KnowledgeError::Limit {
                participant: "research_decisions",
                observed: events.len(),
                limit: MAX_RESEARCH_ROWS,
            });
        }
        events.sort_by_key(|event| event.sequence);
        let mut ids = HashSet::new();
        let mut requests = HashMap::new();
        for (index, event) in events.iter().enumerate() {
            validate(event)?;
            if event.sequence != index as u64 + 1 {
                return Err(invalid(
                    "research_decision.sequence",
                    "history must have contiguous publication order",
                ));
            }
            if !ids.insert(event.decision_uuid) {
                return Err(KnowledgeError::Duplicate("decision_uuid"));
            }
            if requests
                .insert(event.operation_uuid, event.request_sha256)
                .is_some_and(|old| old != event.request_sha256)
            {
                return Err(KnowledgeError::Conflict("research_decision.operation_uuid"));
            }
        }
        Ok(Self { events })
    }

    /// Append exact records or return an unchanged ledger on exact event replay.
    pub fn append(&self, staged: Vec<ResearchDecisionRecord>) -> Result<Self, KnowledgeError> {
        let mut events = self.events.clone();
        let by_id: HashMap<_, _> = self
            .events
            .iter()
            .map(|event| (event.decision_uuid, event))
            .collect();
        for event in staged {
            if let Some(existing) = by_id.get(&event.decision_uuid) {
                if *existing != &event {
                    return Err(KnowledgeError::Conflict("decision_uuid"));
                }
            } else {
                events.push(event);
            }
        }
        Self::new(events)
    }

    /// Inspect all history, including introduction and revoked decisions.
    #[must_use]
    pub fn events(&self) -> &[ResearchDecisionRecord] {
        &self.events
    }

    /// Latest explicit canonical decision only in the exact requested authority.
    /// An integration event, source context, or different community cannot grant it.
    #[must_use]
    pub fn canonical_decision(
        &self,
        authority: &ResearchAuthority,
        subject_kind: ResearchSubjectKind,
        subject_uuid: Uuid,
    ) -> Option<&ResearchDecisionRecord> {
        self.events.iter().rev().find(|event| {
            &event.authority == authority
                && event.subject_kind == subject_kind
                && event.subject_uuid == subject_uuid
                && event.kind != ResearchDecisionKind::Integrate
        })
    }
}

fn validate(event: &ResearchDecisionRecord) -> Result<(), KnowledgeError> {
    require_v7(event.decision_uuid, "decision_uuid")?;
    for (id, field) in [
        (event.operation_uuid, "operation_uuid"),
        (event.authority.project_uuid, "project_uuid"),
        (event.authority.context_uuid, "context_uuid"),
        (event.subject_uuid, "subject_uuid"),
        (event.creator_uuid, "creator_uuid"),
    ] {
        require_uuid(id, field)?;
    }
    if let Some(id) = event.authority.community_uuid {
        require_uuid(id, "community_uuid")?;
    }
    if let Some(id) = event.source_version_uuid {
        require_uuid(id, "source_version_uuid")?;
    }
    if event.subject_kind == ResearchSubjectKind::Assertion {
        require_v7(event.subject_uuid, "assertion_uuid")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn event(
        sequence: u64,
        authority: ResearchAuthority,
        subject_uuid: Uuid,
        kind: ResearchDecisionKind,
    ) -> ResearchDecisionRecord {
        ResearchDecisionRecord {
            sequence,
            decision_uuid: Uuid::now_v7(),
            operation_uuid: Uuid::now_v7(),
            request_sha256: [1; 32],
            authority,
            subject_kind: ResearchSubjectKind::Assertion,
            subject_uuid,
            kind,
            creator_uuid: Uuid::now_v7(),
            source_version_uuid: None,
            recorded_at: 100 - i64::try_from(sequence).unwrap(),
        }
    }
    #[test]
    fn integration_and_source_acceptance_do_not_promote_destination() {
        let parent = ResearchAuthority {
            project_uuid: Uuid::now_v7(),
            community_uuid: None,
            context_uuid: Uuid::now_v7(),
        };
        let child = ResearchAuthority {
            context_uuid: Uuid::now_v7(),
            ..parent.clone()
        };
        let subject = Uuid::now_v7();
        let accepted = event(1, parent.clone(), subject, ResearchDecisionKind::Promote);
        let integrated = event(2, child.clone(), subject, ResearchDecisionKind::Integrate);
        let ledger = ResearchDecisionLedger::new(vec![accepted.clone(), integrated]).unwrap();
        assert_eq!(
            ledger.canonical_decision(&parent, ResearchSubjectKind::Assertion, subject),
            Some(&accepted)
        );
        assert!(
            ledger
                .canonical_decision(&child, ResearchSubjectKind::Assertion, subject)
                .is_none()
        );
        let promoted = event(3, child.clone(), subject, ResearchDecisionKind::Promote);
        let revoked = event(4, child.clone(), subject, ResearchDecisionKind::Revoke);
        let ledger = ledger.append(vec![promoted, revoked.clone()]).unwrap();
        assert_eq!(
            ledger.canonical_decision(&child, ResearchSubjectKind::Assertion, subject),
            Some(&revoked)
        );
        assert_eq!(
            ledger.canonical_decision(&parent, ResearchSubjectKind::Assertion, subject),
            Some(&accepted)
        );
        let other_community = ResearchAuthority {
            community_uuid: Some(Uuid::now_v7()),
            ..child
        };
        assert!(
            ledger
                .canonical_decision(&other_community, ResearchSubjectKind::Assertion, subject)
                .is_none()
        );
        assert_eq!(ledger.events().len(), 4);
    }
    #[test]
    fn history_refuses_identity_rewrite_gaps_and_changed_operation_content() {
        let authority = ResearchAuthority {
            project_uuid: Uuid::now_v7(),
            community_uuid: None,
            context_uuid: Uuid::now_v7(),
        };
        let first = event(1, authority, Uuid::now_v7(), ResearchDecisionKind::Promote);
        let ledger = ResearchDecisionLedger::new(vec![first.clone()]).unwrap();
        assert_eq!(ledger.append(vec![first.clone()]).unwrap(), ledger);
        let mut changed = first.clone();
        changed.kind = ResearchDecisionKind::Revoke;
        assert!(matches!(
            ledger.append(vec![changed]),
            Err(KnowledgeError::Conflict("decision_uuid"))
        ));
        let mut next = first.clone();
        next.sequence = 2;
        next.decision_uuid = Uuid::now_v7();
        next.request_sha256 = [99; 32];
        assert!(matches!(
            ledger.append(vec![next]),
            Err(KnowledgeError::Conflict("research_decision.operation_uuid"))
        ));
        let mut gap = first;
        gap.sequence = 3;
        gap.decision_uuid = Uuid::now_v7();
        assert!(ledger.append(vec![gap]).is_err());
        assert_eq!(ledger.events().len(), 1);
    }
}
