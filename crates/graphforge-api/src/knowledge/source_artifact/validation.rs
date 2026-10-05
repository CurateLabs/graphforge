//! Operation-local membership for ordered artifact derivation inputs.

use std::collections::{HashMap, HashSet, hash_map::Entry};

use graphforge_core::{ApiErrorCode, GfError};
use graphforge_knowledge::{ArtifactLedger, DerivationSubjectKind, SourceLedger};
use graphforge_storage::ResolvedProjectGeneration;
use uuid::Uuid;

use super::DerivationInput;
use crate::GraphForge;
use crate::algorithm_runs::read_ledger as read_algorithm_run_ledger;
use crate::knowledge::{
    match_requested_edge_uuids, match_requested_node_uuids, read_artifact_ledger,
    read_evidence_ledger, read_ledger,
};

/// Retain an Artifact snapshot loaded for validation so publication uses it too.
pub(super) fn validate_derivation_inputs(
    graph: &GraphForge,
    parent: &ResolvedProjectGeneration,
    inputs: &[DerivationInput],
    sources: &SourceLedger,
) -> Result<Option<ArtifactLedger>, GfError> {
    let _validation = graphforge_storage::concurrency_attribution::RegionScope::named(
        "derivation_input_validation",
    );
    let mut membership = HashMap::new();
    let mut artifacts = None;
    for input in inputs {
        // Load kinds lazily in first-input order: an earlier missing subject
        // must still precede an unread later kind's corruption/error.
        let subjects = match membership.entry(input.input_kind) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(subject_membership(
                graph,
                parent,
                inputs,
                input.input_kind,
                sources,
                &mut artifacts,
            )?),
        };
        if !subjects.contains(&input.input_uuid) {
            return Err(GfError::Api {
                code: ApiErrorCode::NotFound,
                message: "derivation input subject was not found".into(),
            });
        }
    }
    Ok(artifacts)
}

fn subject_membership(
    graph: &GraphForge,
    parent: &ResolvedProjectGeneration,
    inputs: &[DerivationInput],
    kind: DerivationSubjectKind,
    sources: &SourceLedger,
    artifacts: &mut Option<ArtifactLedger>,
) -> Result<HashSet<Uuid>, GfError> {
    // Retain only requested membership, rather than indexing every ledger row.
    let requested = inputs
        .iter()
        .filter(|input| input.input_kind == kind)
        .map(|input| input.input_uuid)
        .collect::<HashSet<_>>();
    Ok(match kind {
        DerivationSubjectKind::Source => sources
            .sources
            .iter()
            .map(|row| row.source_uuid)
            .filter(|id| requested.contains(id))
            .collect(),
        DerivationSubjectKind::Artifact => {
            let ledger = read_artifact_ledger(parent)?;
            let ids = ledger
                .artifacts
                .iter()
                .map(|row| row.artifact_uuid)
                .filter(|id| requested.contains(id))
                .collect();
            *artifacts = Some(ledger);
            ids
        }
        DerivationSubjectKind::Assertion => read_ledger(parent)?
            .assertions
            .into_iter()
            .map(|row| row.assertion_uuid)
            .filter(|id| requested.contains(id))
            .collect(),
        DerivationSubjectKind::EvidenceLink => read_evidence_ledger(parent)?
            .links
            .into_iter()
            .map(|row| row.evidence_uuid)
            .filter(|id| requested.contains(id))
            .collect(),
        DerivationSubjectKind::AlgorithmRun => read_algorithm_run_ledger(parent)?
            .runs
            .into_iter()
            .map(|row| row.run_uuid)
            .filter(|id| requested.contains(id))
            .collect(),
        DerivationSubjectKind::Node | DerivationSubjectKind::Edge => {
            let mut requested = requested;
            let mut missing = requested.clone();
            if kind == DerivationSubjectKind::Node {
                match_requested_node_uuids(graph, &mut missing)?;
            } else {
                match_requested_edge_uuids(graph, &mut missing)?;
            }
            requested.retain(|id| !missing.contains(id));
            requested
        }
    })
}
