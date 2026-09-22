//! Include immutable knowledge changes in the existing typed Branch field baseline.
use super::fields::{Fields, insert};
use crate::{CancellationToken, GfError, GraphForge, knowledge::knowledge_error};
use arrow::array::FixedSizeBinaryArray;
use uuid::Uuid;

pub(super) fn read(
    graph: &GraphForge,
    fields: &mut Fields,
    bytes: &mut usize,
    selected: Option<&super::fields::Objects>,
    cancellation: &CancellationToken,
) -> Result<(), GfError> {
    let generation = graph.generation_for_read()?;
    let claims = crate::research_claims::ledger::read_claims(&generation)?;
    for row in claims.claims() {
        if selected.is_some_and(|set| !set.contains(&("assertion".into(), row.assertion_uuid))) {
            continue;
        }
        insert(
            fields,
            bytes,
            (
                "assertion".into(),
                row.assertion_uuid,
                "$research_classification".into(),
            ),
            row.fingerprint().map_err(knowledge_error)?,
        )?;
    }
    for row in claims.relations() {
        if selected
            .is_some_and(|set| !set.contains(&("assertion".into(), row.source_assertion_uuid)))
        {
            continue;
        }
        insert(
            fields,
            bytes,
            (
                "assertion".into(),
                row.source_assertion_uuid,
                format!("$claim_relation:{}", row.relation_uuid),
            ),
            row.fingerprint().map_err(knowledge_error)?,
        )?;
    }
    for (cap, family, subject, identity) in [
        (
            "epistemic",
            "assertion_status_events",
            "assertion_uuid",
            "status_event_uuid",
        ),
        ("epistemic", "reasoning", "assertion_uuid", "reasoning_uuid"),
        (
            "epistemic",
            "assertion_supersessions",
            "prior_assertion_uuid",
            "supersession_uuid",
        ),
        (
            "epistemic",
            "claim_suppressions",
            "assertion_uuid",
            "suppression_uuid",
        ),
        (
            "knowledge",
            "confidence_assessments",
            "assertion_uuid",
            "confidence_uuid",
        ),
        ("knowledge", "evidence", "assertion_uuid", "evidence_uuid"),
    ] {
        let Some(snapshot) = generation.participant_snapshot(cap, family)? else {
            continue;
        };
        for batch in crate::knowledge::read_parquet(&snapshot.bytes)? {
            let ids = batch
                .column_by_name(subject)
                .and_then(|a| a.as_any().downcast_ref::<FixedSizeBinaryArray>())
                .ok_or_else(invalid)?;
            let events = batch
                .column_by_name(identity)
                .and_then(|a| a.as_any().downcast_ref::<FixedSizeBinaryArray>())
                .ok_or_else(invalid)?;
            for row in 0..batch.num_rows() {
                cancellation.checkpoint()?;
                let id = Uuid::from_slice(ids.value(row)).map_err(|_| invalid())?;
                if selected.is_some_and(|set| !set.contains(&("assertion".into(), id))) {
                    continue;
                }
                let event = Uuid::from_slice(events.value(row)).map_err(|_| invalid())?;
                let digest = crate::canonical_arrow::result_fingerprint(&[batch.slice(row, 1)])
                    .map_err(|e| GfError::Validation(e.to_string()))?;
                insert(
                    fields,
                    bytes,
                    ("assertion".into(), id, format!("${family}:{event}")),
                    digest,
                )?;
            }
        }
    }
    Ok(())
}
fn invalid() -> GfError {
    GfError::Validation("invalid Branch claim field record".into())
}
