//! Extract selected owner ledgers without retaining unrelated parent rows.
use crate::{GfError, knowledge::ledger as knowledge, slices::branch::BranchSelection};
use graphforge_knowledge::{
    AlgorithmRunLedger, ArtifactDerivationLedger, ArtifactLedger, AssertionLedger, EvidenceLedger,
    SourceLedger,
};
use graphforge_provenance::ProvenanceLedger;
use graphforge_storage::ProjectParticipant;
use std::collections::BTreeSet;
use uuid::Uuid;

pub(super) fn selected(selection: &BranchSelection) -> Result<Vec<ProjectParticipant>, GfError> {
    let generation = selection.view.generation_for_read()?;
    retained(
        &generation,
        selection
            .active
            .union(&selection.required)
            .cloned()
            .collect(),
    )
}

pub(super) fn retained(
    generation: &graphforge_storage::ResolvedProjectGeneration,
    mut ids: BTreeSet<(String, Uuid)>,
) -> Result<Vec<ProjectParticipant>, GfError> {
    super::domain_bounds::preflight(generation)?;
    let mut participants = Vec::new();
    if generation.capability("knowledge")?.is_some() {
        participants.extend(knowledge_rows(generation, &mut ids)?);
    }
    participants.extend(super::domain_state::selected(generation, &mut ids)?);
    if generation.capability("provenance")?.is_some() {
        let original = crate::provenance::read_ledger(generation)?;
        let lineage: Vec<_> = original
            .lineage
            .into_iter()
            .filter(|row| has(&ids, row.subject_kind.as_str(), row.subject_uuid))
            .collect();
        for row in &lineage {
            ids.insert(("provenance".into(), row.provenance_uuid));
        }
        let events = original
            .events
            .into_iter()
            .filter(|row| has(&ids, "provenance", row.provenance_uuid))
            .collect();
        let ledger =
            ProvenanceLedger::new(events, lineage).map_err(crate::knowledge::provenance_error)?;
        participants.extend(crate::provenance::encode_ledger(&ledger)?);
    }
    super::domain_bounds::refuse_unsupported_selected(generation, &ids, &participants)?;
    Ok(participants)
}
pub(super) fn has(ids: &BTreeSet<(String, Uuid)>, kind: &str, uuid: Uuid) -> bool {
    let kind = match kind {
        "graph_node" => "node",
        "graph_edge" => "edge",
        other => other,
    };
    ids.contains(&(kind.into(), uuid))
}
fn knowledge_rows(
    generation: &graphforge_storage::ResolvedProjectGeneration,
    ids: &mut BTreeSet<(String, Uuid)>,
) -> Result<Vec<ProjectParticipant>, GfError> {
    let error = crate::knowledge::knowledge_error;
    let sources = knowledge::read_source_ledger(generation)?
        .sources
        .into_iter()
        .filter(|r| has(ids, "source", r.source_uuid))
        .collect();
    let sources = SourceLedger::new(sources).map_err(error)?;
    let artifacts = knowledge::read_artifact_ledger(generation)?
        .artifacts
        .into_iter()
        .filter(|r| has(ids, "artifact", r.artifact_uuid))
        .collect();
    let artifacts = ArtifactLedger::new(artifacts).map_err(error)?;
    let original = knowledge::read_ledger(generation)?;
    let assertions = AssertionLedger::new(
        original
            .assertions
            .into_iter()
            .filter(|r| has(ids, "assertion", r.assertion_uuid))
            .collect(),
        original
            .graph_refs
            .into_iter()
            .filter(|r| has(ids, "assertion", r.assertion_uuid))
            .collect(),
    )
    .map_err(error)?;
    let evidence = EvidenceLedger::new(
        knowledge::read_evidence_ledger(generation)?
            .links
            .into_iter()
            .filter(|r| has(ids, "assertion", r.assertion_uuid))
            .collect(),
    )
    .map_err(error)?;
    let derivations = ArtifactDerivationLedger::new(
        knowledge::read_derivation_ledger(generation)?
            .derivations
            .into_iter()
            .filter(|r| has(ids, r.output_kind.as_str(), r.output_uuid))
            .collect(),
    )
    .map_err(error)?;
    for row in &derivations.derivations {
        if !has(ids, row.input_kind.as_str(), row.input_uuid) {
            return Err(GfError::Validation(
                "selected derivation dependency is unavailable".into(),
            ));
        }
        ids.insert(("provenance".into(), row.provenance_uuid));
    }
    for id in sources
        .sources
        .iter()
        .map(|r| r.provenance_uuid)
        .chain(artifacts.artifacts.iter().map(|r| r.provenance_uuid))
        .chain(assertions.assertions.iter().map(|r| r.provenance_uuid))
        .chain(evidence.links.iter().map(|r| r.provenance_uuid))
    {
        ids.insert(("provenance".into(), id));
    }
    for id in artifacts.artifacts.iter().filter_map(|r| r.run_uuid) {
        ids.insert(("algorithm_run".into(), id));
    }
    let original = crate::algorithm_runs::read_ledger(generation)?;
    let runs = AlgorithmRunLedger::new(
        original
            .runs
            .into_iter()
            .filter(|r| has(ids, "algorithm_run", r.run_uuid))
            .collect(),
        original
            .events
            .into_iter()
            .filter(|r| has(ids, "algorithm_run", r.run_uuid))
            .collect(),
    )
    .map_err(error)?;
    for id in runs
        .runs
        .iter()
        .map(|r| r.provenance_uuid)
        .chain(runs.events.iter().map(|r| r.provenance_uuid))
    {
        ids.insert(("provenance".into(), id));
    }
    let mut participants = knowledge::encode_source_ledger(&sources)?;
    participants.extend(knowledge::encode_artifact_ledger(&artifacts)?);
    participants.extend(knowledge::encode_ledger(&assertions)?);
    participants.extend(knowledge::encode_evidence_ledger(&evidence)?);
    participants.extend(knowledge::encode_derivation_ledger(&derivations)?);
    participants.extend(crate::algorithm_runs::encode_ledger(&runs)?);
    Ok(participants)
}
