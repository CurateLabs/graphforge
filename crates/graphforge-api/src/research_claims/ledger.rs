//! Bounded Parquet adapters for native knowledge-owned research records.
use crate::{
    GfError,
    knowledge::{knowledge_error, ledger as k},
};
use arrow::record_batch::RecordBatch;
use graphforge_knowledge::{
    research::{MAX_RESEARCH_ROWS, ResearchClaimLedger, ResearchDecisionLedger},
    schema_registry,
};
use graphforge_storage::{ProjectParticipant, ResolvedProjectGeneration};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

pub(crate) fn read_claims(g: &ResolvedProjectGeneration) -> Result<ResearchClaimLedger, GfError> {
    ResearchClaimLedger::from_batches(
        &read(g, "epistemic", "research_claims")?,
        &read(g, "epistemic", "claim_relations")?,
    )
    .map_err(knowledge_error)
}
pub(crate) fn read_decisions(
    g: &ResolvedProjectGeneration,
) -> Result<ResearchDecisionLedger, GfError> {
    ResearchDecisionLedger::from_batches(&read(g, "research", "canonical_decisions")?)
        .map_err(knowledge_error)
}
pub(crate) fn encode_claims(
    ledger: &ResearchClaimLedger,
) -> Result<Vec<ProjectParticipant>, GfError> {
    Ok(vec![
        encode(
            "research_claims",
            &ledger.claim_batch().map_err(knowledge_error)?,
        )?,
        encode(
            "claim_relations",
            &ledger.relation_batch().map_err(knowledge_error)?,
        )?,
    ])
}
pub(crate) fn encode_decisions(
    ledger: &ResearchDecisionLedger,
) -> Result<ProjectParticipant, GfError> {
    encode(
        "canonical_decisions",
        &ledger.batch().map_err(knowledge_error)?,
    )
}
fn encode(family: &str, batch: &RecordBatch) -> Result<ProjectParticipant, GfError> {
    let entry = schema_registry()
        .into_iter()
        .find(|entry| entry.record_family == family)
        .expect("registered research family");
    k::participant(&entry, batch)
}
fn read(
    g: &ResolvedProjectGeneration,
    capability: &str,
    family: &str,
) -> Result<Vec<RecordBatch>, GfError> {
    let Some(descriptor) = g
        .participant_descriptors()?
        .into_iter()
        .find(|entry| entry.capability_id == capability && entry.record_family_id == family)
    else {
        return Ok(Vec::new());
    };
    if descriptor.row_count > MAX_RESEARCH_ROWS as u64 {
        return Err(limit());
    }
    let file = std::fs::File::open(g.participant_path(capability, family)?)
        .map_err(|e| GfError::Storage(e.to_string()))?;
    if file
        .metadata()
        .map_err(|e| GfError::Storage(e.to_string()))?
        .len()
        > 64 * 1024 * 1024
    {
        return Err(limit());
    }
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| GfError::Validation(format!("invalid research participant: {e}")))?;
    let mut bytes = 0_u64;
    let mut rows = 0_u64;
    for group in reader.metadata().row_groups() {
        bytes = bytes.saturating_add(u64::try_from(group.total_byte_size()).map_err(|_| limit())?);
        rows = rows.saturating_add(u64::try_from(group.num_rows()).map_err(|_| limit())?);
    }
    if bytes > 64 * 1024 * 1024 || rows > MAX_RESEARCH_ROWS as u64 || rows != descriptor.row_count {
        return Err(limit());
    }
    let snapshot = g
        .participant_snapshot(capability, family)?
        .ok_or_else(|| GfError::Validation("research participant disappeared".into()))?;
    k::require_participant_contract(&snapshot, family)?;
    reader
        .build()
        .map_err(|e| GfError::Validation(format!("invalid research participant: {e}")))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| GfError::Validation(format!("invalid research participant: {e}")))
}
fn limit() -> GfError {
    GfError::Project {
        code: graphforge_core::ProjectErrorCode::ResourceLimit,
        message: "research metadata exceeds the bounded row or byte contract".into(),
    }
}
