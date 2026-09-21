//! Bounded research lineage over Sources, Artifacts, and derivations (#1349).

use std::collections::{HashSet, VecDeque};

use graphforge_knowledge::{
    ARTIFACT_DERIVATION_SCHEMA, ArtifactDerivationLedger, DerivationSubjectKind,
};

use super::{
    ApiErrorCode, GfError, GraphForge, PageRequest, ResolvedProjectGeneration, Uuid,
    assertion_result, concat_or_empty, knowledge_error, match_requested_edge_uuids,
    match_requested_node_uuids, read_artifact_ledger, read_derivation_ledger, read_evidence_ledger,
    read_ledger, read_source_ledger, require_uuid, with_next_token,
};
use crate::PageToken;
use crate::algorithm_runs::read_ledger as read_algorithm_run_ledger;
use crate::paging::validate_page;

/// Traversal direction for research lineage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LineageDirection {
    /// Inputs and ancestors.
    Backward,
    /// Outputs and descendants.
    Forward,
}

/// Frozen bounded lineage request for one research subject.
#[derive(Clone, Debug, PartialEq)]
pub struct ResearchLineageRequest {
    /// Subject UUID.
    pub subject_uuid: Uuid,
    /// Closed subject kind.
    pub subject_kind: DerivationSubjectKind,
    /// Traversal direction.
    pub direction: LineageDirection,
    /// Maximum hop depth (inclusive).
    pub max_depth: u32,
    /// Generation-pinned bounded page.
    pub page: PageRequest,
}

impl GraphForge {
    /// Return one deterministic page of derivation edges for a research subject.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-knowledge-api/1 freezes owned request structs"
    )]
    pub fn research_lineage(
        &self,
        request: ResearchLineageRequest,
    ) -> Result<graphforge_exec::ExecutionResult, GfError> {
        require_uuid(request.subject_uuid, "subject_uuid")?;
        if request.max_depth == 0 {
            return Err(GfError::Validation("max_depth must be positive".into()));
        }
        let generation = self.generation_for_read()?;
        let derivations = read_derivation_ledger(&generation)?;
        let artifacts = read_artifact_ledger(&generation)?;
        let sources = read_source_ledger(&generation)?;
        validate_subject_exists(
            self,
            &generation,
            request.subject_uuid,
            request.subject_kind,
            &sources,
            &artifacts,
        )?;
        let indices = collect_derivation_indices_internal(
            &derivations,
            request.subject_uuid,
            request.subject_kind,
            request.direction,
            request.max_depth,
        );
        let batch = derivations.batch().map_err(knowledge_error)?;
        let rows = indices
            .iter()
            .map(|index| batch.slice(*index, 1))
            .collect::<Vec<_>>();
        page_derivation_rows(&rows, generation.generation_uuid(), &request.page)
    }
}

fn validate_subject_exists(
    graph: &GraphForge,
    generation: &ResolvedProjectGeneration,
    subject_uuid: Uuid,
    subject_kind: DerivationSubjectKind,
    sources: &graphforge_knowledge::SourceLedger,
    artifacts: &graphforge_knowledge::ArtifactLedger,
) -> Result<(), GfError> {
    let found = match subject_kind {
        DerivationSubjectKind::Source => sources
            .sources
            .iter()
            .any(|row| row.source_uuid == subject_uuid),
        DerivationSubjectKind::Artifact => artifacts
            .artifacts
            .iter()
            .any(|row| row.artifact_uuid == subject_uuid),
        DerivationSubjectKind::Node => {
            let mut pending = HashSet::from([subject_uuid]);
            match_requested_node_uuids(graph, &mut pending)?;
            pending.is_empty()
        }
        DerivationSubjectKind::Edge => {
            let mut pending = HashSet::from([subject_uuid]);
            match_requested_edge_uuids(graph, &mut pending)?;
            pending.is_empty()
        }
        DerivationSubjectKind::Assertion => read_ledger(generation)?
            .assertions
            .iter()
            .any(|row| row.assertion_uuid == subject_uuid),
        DerivationSubjectKind::EvidenceLink => read_evidence_ledger(generation)?
            .links
            .iter()
            .any(|row| row.evidence_uuid == subject_uuid),
        DerivationSubjectKind::AlgorithmRun => read_algorithm_run_ledger(generation)?
            .runs
            .iter()
            .any(|row| row.run_uuid == subject_uuid),
    };
    if found {
        Ok(())
    } else {
        Err(GfError::Api {
            code: ApiErrorCode::NotFound,
            message: "lineage subject was not found".into(),
        })
    }
}

pub(crate) fn collect_derivation_indices_internal(
    ledger: &ArtifactDerivationLedger,
    subject_uuid: Uuid,
    subject_kind: DerivationSubjectKind,
    direction: LineageDirection,
    max_depth: u32,
) -> Vec<usize> {
    let mut selected = Vec::new();
    let mut seen_edges = HashSet::new();
    let mut queue = VecDeque::from([(subject_uuid, subject_kind, 0_u32)]);
    let mut visited = HashSet::from([(subject_uuid, subject_kind)]);
    while let Some((current_uuid, current_kind, depth)) = queue.pop_front() {
        if depth >= max_depth {
            continue;
        }
        for (index, row) in ledger.derivations.iter().enumerate() {
            let matches = match direction {
                LineageDirection::Backward => {
                    row.output_uuid == current_uuid && row.output_kind == current_kind
                }
                LineageDirection::Forward => {
                    row.input_uuid == current_uuid && row.input_kind == current_kind
                }
            };
            if !matches || !seen_edges.insert(row.derivation_uuid) {
                continue;
            }
            selected.push(index);
            let next = match direction {
                LineageDirection::Backward => (row.input_uuid, row.input_kind),
                LineageDirection::Forward => (row.output_uuid, row.output_kind),
            };
            if visited.insert(next) {
                queue.push_back((next.0, next.1, depth + 1));
            }
        }
    }
    selected.sort_unstable();
    selected.dedup();
    selected
}

fn page_derivation_rows(
    rows: &[arrow::record_batch::RecordBatch],
    generation_uuid: Uuid,
    page: &PageRequest,
) -> Result<graphforge_exec::ExecutionResult, GfError> {
    let selected = (0..rows.len()).collect::<Vec<_>>();
    let (start, end) = validate_page(page, generation_uuid, selected.len())?;
    let slice = selected[start..end]
        .iter()
        .map(|index| rows[*index].clone())
        .collect::<Vec<_>>();
    let output = concat_or_empty(&slice, &ARTIFACT_DERIVATION_SCHEMA)?;
    let next = (end < selected.len()).then(|| PageToken::new(generation_uuid, end));
    Ok(assertion_result(with_next_token(&output, next.as_ref())?))
}
