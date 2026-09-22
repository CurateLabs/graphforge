//! Bounded metadata reads and explicit refusal of unsupported selected state.
use crate::GfError;
use graphforge_storage::{ProjectParticipant, ResolvedProjectGeneration};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::BTreeSet;
use uuid::Uuid;

pub(crate) fn preflight(g: &ResolvedProjectGeneration) -> Result<(), GfError> {
    let mut bytes = 0_u64;
    let mut rows = 0_u64;
    for p in g.participant_descriptors()?.iter().filter(|p| {
        matches!(
            p.capability_id.as_str(),
            "knowledge" | "provenance" | "epistemic" | "valid_time"
        ) || (p.capability_id == "workspace" && p.record_family_id == "branch_fields")
    }) {
        let file = std::fs::File::open(g.participant_path(&p.capability_id, &p.record_family_id)?)
            .map_err(|e| GfError::Storage(e.to_string()))?;
        let size = file
            .metadata()
            .map_err(|e| GfError::Storage(e.to_string()))?
            .len();
        bytes = bytes.saturating_add(size);
        bound(bytes, rows)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| GfError::Storage(e.to_string()))?;
        for group in reader.metadata().row_groups() {
            bytes = bytes
                .saturating_add(u64::try_from(group.total_byte_size()).map_err(|_| invalid())?);
        }
        rows = rows.saturating_add(p.row_count);
        bound(bytes, rows)?;
    }
    Ok(())
}
fn bound(bytes: u64, rows: u64) -> Result<(), GfError> {
    if bytes > 64 * 1024 * 1024 || rows > 1_000_000 {
        Err(invalid())
    } else {
        Ok(())
    }
}
fn invalid() -> GfError {
    GfError::Project {
        code: graphforge_core::ProjectErrorCode::ResourceLimit,
        message: "Branch domain metadata exceeds bounded creation resources".into(),
    }
}

pub(super) fn refuse_unsupported_selected(
    g: &ResolvedProjectGeneration,
    ids: &BTreeSet<(String, Uuid)>,
    replacements: &[ProjectParticipant],
) -> Result<(), GfError> {
    use arrow::array::{Array, FixedSizeBinaryArray};
    let selected: BTreeSet<_> = ids.iter().map(|(_, id)| *id).collect();
    for p in g.participant_descriptors()?.iter().filter(|p| {
        matches!(
            p.capability_id.as_str(),
            "knowledge" | "epistemic" | "valid_time"
        ) && p.row_count != 0
            && !replacements.iter().any(|r| {
                r.capability_id == p.capability_id && r.record_family_id == p.record_family_id
            })
    }) {
        let file = std::fs::File::open(g.participant_path(&p.capability_id, &p.record_family_id)?)
            .map_err(|e| GfError::Storage(e.to_string()))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .and_then(ParquetRecordBatchReaderBuilder::build)
            .map_err(|e| GfError::Storage(e.to_string()))?;
        for batch in reader {
            let batch = batch.map_err(|e| GfError::Storage(e.to_string()))?;
            for column in batch.columns() {
                if let Some(uuids) = column.as_any().downcast_ref::<FixedSizeBinaryArray>() {
                    for row in 0..uuids.len() {
                        if !uuids.is_null(row)
                            && Uuid::from_slice(uuids.value(row))
                                .is_ok_and(|id| selected.contains(&id))
                        {
                            return Err(GfError::Validation(format!(
                                "selected Branch state in {} requires a supported explicit dependency projection",
                                p.record_family_id
                            )));
                        }
                    }
                }
            }
        }
    }
    Ok(())
}
