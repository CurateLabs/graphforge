//! Version-qualified citations are metadata, never active membership or retention roots.
use super::{ReferenceResearchBranchRequest, publication, unavailable};
use crate::{CancellationToken, ExecutionResult, GfError, GraphForge};
use graphforge_storage::{
    ProjectParticipant, ProjectParticipantEncoding,
    research_versions::{
        RegisterResearchVersion, ResearchMutation, ResearchOperationReceipt, ResearchVersionRecord,
        inspect_research_version, prepare_branch_selection, replace_prepared_branch_domains,
    },
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use uuid::Uuid;
const FAMILY: &str = "branch_references";
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Citation {
    reference_uuid: Uuid,
    source_version_uuid: Uuid,
    label: String,
}
fn fingerprint() -> [u8; 32] {
    Sha256::digest(b"graphforge-branch-references/1").into()
}
fn decode(bytes: &[u8]) -> Result<Vec<Citation>, GfError> {
    if bytes.len() > 1024 * 1024 {
        return Err(invalid());
    }
    let rows: Vec<Citation> = serde_json::from_slice(bytes).map_err(|_| invalid())?;
    if rows.len() > 10_000
        || rows.iter().any(|r| {
            r.reference_uuid.is_nil() || r.source_version_uuid.is_nil() || r.label.len() > 4096
        })
        || rows
            .windows(2)
            .any(|r| r[0].reference_uuid >= r[1].reference_uuid)
    {
        return Err(invalid());
    }
    Ok(rows)
}
fn invalid() -> GfError {
    GfError::Validation("invalid or oversized Branch reference metadata".into())
}
fn read(root: &std::path::Path, version: &ResearchVersionRecord) -> Result<Vec<Citation>, GfError> {
    let Some(p) = inspect_research_version(root, version)?
        .into_iter()
        .find(|p| p.capability_id == "workspace" && p.record_family_id == FAMILY)
    else {
        return Ok(Vec::new());
    };
    if p.record_version != 1 || p.schema_fingerprint != fingerprint() {
        return Err(invalid());
    }
    decode(&p.bytes)
}
impl GraphForge {
    /// Record an exact historical citation without expanding Branch research.
    pub fn reference_research_branch(
        &mut self,
        request: &ReferenceResearchBranchRequest,
        cancellation: &CancellationToken,
    ) -> Result<ResearchOperationReceipt, GfError> {
        if request.version_uuid.is_nil()
            || request.reference_uuid.is_nil()
            || request.label.len() > 4096
        {
            return Err(invalid());
        }
        let command = publication::begin(
            self,
            request.operation_uuid,
            request.expected_generation_uuid,
            request,
            cancellation,
        )?;
        if let Some(receipt) = command.replay(self)? {
            return Ok(receipt);
        }
        if !command
            .registry
            .identities
            .contains_key(&request.source_version_uuid)
            || !command.registry.branches.contains_key(&request.branch_uuid)
        {
            return Err(unavailable());
        }
        let head = command
            .registry
            .heads
            .get(&request.branch_uuid)
            .ok_or_else(unavailable)?;
        let version = command
            .registry
            .versions
            .get(head)
            .ok_or_else(unavailable)?;
        let mut rows = read(&command.root, version)?;
        if rows
            .iter()
            .any(|r| r.reference_uuid == request.reference_uuid)
        {
            return Err(invalid());
        }
        rows.push(Citation {
            reference_uuid: request.reference_uuid,
            source_version_uuid: request.source_version_uuid,
            label: request.label.clone(),
        });
        rows.sort_by_key(|r| r.reference_uuid);
        let bytes = serde_json::to_vec(&rows).map_err(|_| invalid())?;
        decode(&bytes)?;
        let spec = RegisterResearchVersion {
            version_uuid: request.version_uuid,
            context_uuid: request.branch_uuid,
            source_generation_uuid: version.content.generation_uuid,
            source_version: Some(*head),
            selection: None,
            required_versions: version.content.required_versions.clone(),
            label: version.label.clone(),
            description: version.description.clone(),
            created_at: request.created_at,
            evidence: version.content.evidence.clone(),
        };
        let mut prepared =
            prepare_branch_selection(&command.root, &spec, None, None, &[], cancellation.flag())?;
        prepared.version.content.source_version = version.content.source_version;
        let keep = prepared
            .version
            .content
            .participants
            .iter()
            .map(|p| p.key.clone())
            .collect();
        let participant = ProjectParticipant {
            capability_id: "workspace".into(),
            capability_version: 1,
            record_family_id: FAMILY.into(),
            record_version: 1,
            encoding: ProjectParticipantEncoding::Json,
            schema_fingerprint: fingerprint(),
            row_count: rows.len() as u64,
            bytes,
        };
        replace_prepared_branch_domains(
            &command.root,
            &mut prepared,
            &keep,
            &[participant],
            cancellation.flag(),
        )?;
        let mutation = ResearchMutation::PublishBranch {
            intent_sha256: command.intent,
            origin_capture: None,
            creation: None,
            version: Box::new(prepared.version.clone()),
        };
        let outcome = command.publish(self, mutation, cancellation);
        drop(prepared);
        outcome
    }
}
pub(super) fn inspect(graph: &GraphForge) -> Result<ExecutionResult, GfError> {
    use arrow::{
        array::{ArrayRef, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    let rows = graph
        .generation_for_read()?
        .participant_snapshot("workspace", FAMILY)?
        .map_or(Ok(Vec::new()), |p| decode(&p.bytes))?;
    let schema = Arc::new(Schema::new(
        [
            "reference_uuid",
            "source_version_uuid",
            "label",
            "disposition",
        ]
        .iter()
        .map(|n| Field::new(*n, DataType::Utf8, false))
        .collect::<Vec<_>>(),
    ));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(
            rows.iter()
                .map(|r| r.reference_uuid.to_string())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|r| r.source_version_uuid.to_string())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.label.as_str()).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(vec![
            "reference_only_payload_not_retained";
            rows.len()
        ])),
    ];
    Ok(crate::knowledge::assertion_result(
        RecordBatch::try_new(schema, columns).map_err(|_| invalid())?,
    ))
}
