//! Linear-time conceptual lineage propagation through immutable successor DAGs.
use super::MAX_RESEARCH_ROWS;
use crate::{AssertionSupersessionLedger, KnowledgeError, invalid};
use std::collections::{HashMap, VecDeque};
use uuid::Uuid;

pub(super) fn validate(
    concepts: &HashMap<Uuid, Uuid>,
    owner: &AssertionSupersessionLedger,
) -> Result<(), KnowledgeError> {
    if concepts.is_empty() {
        return Ok(());
    }
    resolve(concepts, owner).map(|_| ())
}

pub(super) fn resolve(
    concepts: &HashMap<Uuid, Uuid>,
    owner: &AssertionSupersessionLedger,
) -> Result<HashMap<Uuid, Option<Uuid>>, KnowledgeError> {
    if owner.relations().len() > MAX_RESEARCH_ROWS {
        return Err(KnowledgeError::Limit {
            participant: "research_supersession_closure",
            observed: owner.relations().len(),
            limit: MAX_RESEARCH_ROWS,
        });
    }
    let mut remaining: HashMap<Uuid, usize> = HashMap::new();
    let mut outgoing: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
    for row in owner.relations() {
        remaining.entry(row.prior_assertion_uuid).or_default();
        *remaining.entry(row.replacement_assertion_uuid).or_default() += 1;
        outgoing
            .entry(row.prior_assertion_uuid)
            .or_default()
            .push(row.replacement_assertion_uuid);
    }
    let mut queue: VecDeque<_> = remaining
        .iter()
        .filter_map(|(id, count)| (*count == 0).then_some(*id))
        .collect();
    // None denotes incompatible ancestral concepts, never an inferred winner.
    let mut inherited: HashMap<Uuid, Option<Uuid>> = HashMap::new();
    let mut resolved: HashMap<_, _> = concepts
        .iter()
        .map(|(id, concept)| (*id, Some(*concept)))
        .collect();
    let mut visited = 0;
    while let Some(id) = queue.pop_front() {
        visited += 1;
        let ancestor = inherited.get(&id).copied();
        let concept = if let Some(explicit) = concepts.get(&id) {
            if ancestor.is_some_and(|old| old != Some(*explicit)) {
                return Err(invalid(
                    "research_claim.conceptual_uuid",
                    "successors must preserve all conceptual ancestors",
                ));
            }
            Some(*explicit)
        } else {
            ancestor.unwrap_or(Some(id))
        };
        resolved.insert(id, concept);
        for next in outgoing.get(&id).into_iter().flatten() {
            inherited
                .entry(*next)
                .and_modify(|old| {
                    if *old != concept {
                        *old = None;
                    }
                })
                .or_insert(concept);
            let count = remaining.get_mut(next).expect("successor was registered");
            *count -= 1;
            if *count == 0 {
                queue.push_back(*next);
            }
        }
    }
    if visited != remaining.len() {
        return Err(invalid(
            "research_claim.lineage",
            "cyclic supersession ancestry",
        ));
    }
    Ok(resolved)
}
