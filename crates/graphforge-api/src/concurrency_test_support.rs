//! Hidden helpers for native-binding concurrency acceptance tests.
//!
//! These APIs are not part of the supported public surface. Bindings expose
//! thin probes so Python/Node suites can hold a writer lock without sleep-based
//! races while proving `GF_WRITER_BUSY` rejection before staging.

use std::path::Path;

use graphforge_storage::{
    ProjectCapability, ProjectGenerationRequest, ProjectParticipant, ProjectParticipantEncoding,
    ProjectStageOutcome, StagedProjectGeneration,
};
use uuid::Uuid;

use crate::GfError;

/// RAII holder for a staged project generation that owns the writer lock.
pub struct HeldWriter {
    _staged: Box<StagedProjectGeneration>,
}

fn publication_request(root: &Path) -> Result<ProjectGenerationRequest, GfError> {
    let selected = graphforge_storage::resolve_project_generation(root)?;
    selected.validate_complete_participant_inventory()?;
    let capabilities = selected
        .capabilities()
        .into_iter()
        .map(|entry| ProjectCapability {
            capability_id: entry.capability_id,
            capability_version: entry.capability_version,
        })
        .collect();
    let mut participants = Vec::new();
    for entry in selected.participant_snapshots()? {
        let encoding = match entry.encoding.as_str() {
            "arrow" => ProjectParticipantEncoding::Arrow,
            "json" => ProjectParticipantEncoding::Json,
            "parquet" => ProjectParticipantEncoding::Parquet,
            other => {
                return Err(GfError::Validation(format!(
                    "unsupported participant encoding for writer hold: {other}"
                )));
            }
        };
        participants.push(ProjectParticipant {
            capability_id: entry.capability_id,
            capability_version: entry.capability_version,
            record_family_id: entry.record_family_id,
            record_version: entry.record_version,
            encoding,
            schema_fingerprint: entry.schema_fingerprint,
            row_count: entry.row_count,
            bytes: entry.bytes,
        });
    }
    Ok(ProjectGenerationRequest {
        transaction_uuid: Uuid::now_v7(),
        generation_uuid: Uuid::now_v7(),
        capabilities,
        participants,
    })
}

/// Stage a no-op generation and retain the writer lock until the guard drops.
///
/// # Errors
///
/// Returns structured project/storage errors when the root is invalid or a
/// writer is already active.
pub fn hold_writer(root: impl AsRef<Path>) -> Result<HeldWriter, GfError> {
    let root = root.as_ref();
    let request = publication_request(root)?;
    match graphforge_storage::stage_project_generation(root, &request)? {
        ProjectStageOutcome::Staged(staged) => Ok(HeldWriter { _staged: staged }),
        ProjectStageOutcome::AlreadyPublished(_) => Err(GfError::Validation(
            "writer-hold staging unexpectedly replayed an identical publication".into(),
        )),
    }
}
