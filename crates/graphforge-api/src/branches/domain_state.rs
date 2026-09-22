//! Selected-subject interpretation state; missing closure fails before publication.
use super::domains::has;
use crate::{
    GfError,
    knowledge::{knowledge_error, ledger as k},
};
use graphforge_knowledge::{
    ArtifactPreferenceLedger, AssertionStatusLedger, AssertionSupersessionLedger,
    AssertionValidityLedger, ConfidenceLedger, ReasoningLedger, RetentionDependencyLedger,
};
use graphforge_storage::{ProjectParticipant, ResolvedProjectGeneration};
use std::collections::BTreeSet;
use uuid::Uuid;
type Ids = BTreeSet<(String, Uuid)>;

pub(super) fn selected(
    g: &ResolvedProjectGeneration,
    ids: &mut Ids,
) -> Result<Vec<ProjectParticipant>, GfError> {
    let mut result = Vec::new();
    if g.capability("knowledge")?.is_some() {
        result.extend(confidence(g, ids)?);
        result.extend(source_state(g, ids)?);
    }
    if g.capability("epistemic")?.is_some() {
        result.extend(epistemic(g, ids)?);
    }
    if g.capability("valid_time")?.is_some() {
        let rows = crate::valid_time::read_ledger(g)?
            .events
            .into_iter()
            .filter(|r| has(ids, "assertion", r.assertion_uuid))
            .collect::<Vec<_>>();
        for row in &rows {
            if let Some(id) = row.reasoning_uuid {
                require(has(ids, "reasoning", id))?;
            }
            ids.insert(("provenance".into(), row.provenance_uuid));
        }
        result.extend(crate::valid_time::encode_ledger(
            &AssertionValidityLedger::new(rows).map_err(knowledge_error)?,
        )?);
    }
    Ok(result)
}
fn require(present: bool) -> Result<(), GfError> {
    if present {
        Ok(())
    } else {
        Err(GfError::Validation("selected Branch state requires additional historical dependencies; include their owning assertions or Artifacts explicitly".into()))
    }
}
fn confidence(
    g: &ResolvedProjectGeneration,
    ids: &mut Ids,
) -> Result<Vec<ProjectParticipant>, GfError> {
    let original = k::read_confidence_ledger(g)?;
    let rows = original
        .assessments
        .into_iter()
        .filter(|r| has(ids, "assertion", r.assertion_uuid))
        .collect::<Vec<_>>();
    for row in &rows {
        ids.insert(("confidence".into(), row.confidence_uuid));
        ids.insert(("provenance".into(), row.provenance_uuid));
    }
    let inputs = original
        .inputs
        .into_iter()
        .filter(|r| has(ids, "confidence", r.confidence_uuid))
        .collect::<Vec<_>>();
    for input in &inputs {
        require(has(ids, "confidence", input.input_confidence_uuid))?;
    }
    k::encode_confidence_ledger(&ConfidenceLedger::new(rows, inputs).map_err(knowledge_error)?)
}
fn source_state(
    g: &ResolvedProjectGeneration,
    ids: &mut Ids,
) -> Result<Vec<ProjectParticipant>, GfError> {
    let preferences = k::read_preference_ledger(g)?
        .events
        .into_iter()
        .filter(|r| has(ids, "source", r.source_uuid))
        .collect::<Vec<_>>();
    for row in &preferences {
        require(has(ids, "artifact", row.artifact_uuid))?;
        if let Some(id) = row.prior_artifact_uuid {
            require(has(ids, "artifact", id))?;
        }
        ids.insert(("provenance".into(), row.provenance_uuid));
    }
    let dependencies = k::read_retention_ledger(g)?
        .dependencies
        .into_iter()
        .filter(|r| ids.iter().any(|(_, id)| *id == r.scope_uuid))
        .collect::<Vec<_>>();
    for row in &dependencies {
        require(has(ids, row.required_kind.as_str(), row.required_uuid))?;
        ids.insert(("provenance".into(), row.provenance_uuid));
    }
    let mut result = k::encode_preference_ledger(
        &ArtifactPreferenceLedger::new(preferences).map_err(knowledge_error)?,
    )?;
    result.extend(k::encode_retention_ledger(
        &RetentionDependencyLedger::new(dependencies).map_err(knowledge_error)?,
    )?);
    Ok(result)
}
fn epistemic(
    g: &ResolvedProjectGeneration,
    ids: &mut Ids,
) -> Result<Vec<ProjectParticipant>, GfError> {
    let reasoning = k::read_reasoning_ledger(g)?
        .records
        .into_iter()
        .filter(|r| has(ids, "assertion", r.assertion_uuid))
        .collect::<Vec<_>>();
    for row in &reasoning {
        ids.insert(("reasoning".into(), row.reasoning_uuid));
        ids.insert(("provenance".into(), row.provenance_uuid));
    }
    let statuses = k::read_status_ledger(g)?
        .events
        .into_iter()
        .filter(|r| has(ids, "assertion", r.assertion_uuid))
        .collect::<Vec<_>>();
    for row in &statuses {
        ids.insert(("assertion_status".into(), row.status_event_uuid));
        ids.insert(("provenance".into(), row.provenance_uuid));
    }
    let relations = k::read_supersession_ledger(g)?
        .relations()
        .iter()
        .filter(|r| {
            has(ids, "assertion", r.prior_assertion_uuid)
                || has(ids, "assertion", r.replacement_assertion_uuid)
        })
        .cloned()
        .collect::<Vec<_>>();
    for row in &relations {
        require(
            has(ids, "assertion", row.prior_assertion_uuid)
                && has(ids, "assertion", row.replacement_assertion_uuid),
        )?;
        require(
            has(ids, "reasoning", row.reasoning_uuid)
                && has(ids, "assertion_status", row.status_event_uuid),
        )?;
        ids.insert(("provenance".into(), row.provenance_uuid));
    }
    let mut result =
        k::encode_reasoning_ledger(&ReasoningLedger::new(reasoning).map_err(knowledge_error)?)?;
    result.extend(k::encode_status_ledger(
        &AssertionStatusLedger::new(statuses).map_err(knowledge_error)?,
    )?);
    result.extend(k::encode_supersession_ledger(
        &AssertionSupersessionLedger::new(relations).map_err(knowledge_error)?,
    )?);
    Ok(result)
}
