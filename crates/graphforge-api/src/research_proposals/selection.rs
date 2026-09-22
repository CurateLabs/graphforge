//! Retain selected owner records and redact graph fields before freezing payloads.
use super::{SubmitResearchProposalRequest, identity, invalid};
use crate::{
    CancellationToken, GfError, GraphForge,
    branches::{fields, publication},
};
use graphforge_storage::research_versions::RegisterResearchVersion;
use std::collections::BTreeSet;
use uuid::Uuid;

pub(super) use crate::branches::field_selection::SelectedFields as Selected;

pub(super) fn freeze(
    owner: &GraphForge,
    command: &publication::Command,
    request: &SubmitResearchProposalRequest,
    cancellation: &CancellationToken,
) -> Result<Selected, GfError> {
    let selected = crate::slices::branch::authenticate(owner, &request.frozen_ipc, cancellation)?;
    if selected.version.version_uuid != request.source_version_uuid
        || selected.version.context_uuid != request.source_branch_uuid
    {
        return Err(invalid(
            "Proposal Slice does not identify the exact source Branch Version",
        ));
    }
    let mut spec = RegisterResearchVersion {
        version_uuid: identity(request.operation_uuid, "payload"),
        context_uuid: identity(request.operation_uuid, "payload_context"),
        source_generation_uuid: selected.version.content.generation_uuid,
        selection: None,
        source_version: Some(request.source_version_uuid),
        required_versions: BTreeSet::new(),
        label: None,
        description: None,
        created_at: request.created_at,
        evidence: selected.evidence.clone(),
    };
    crate::branches::field_selection::freeze(
        owner,
        &command.root,
        selected,
        &request.fields,
        &mut spec,
        cancellation,
    )
}

pub(super) fn item_identity(proposal: Uuid, unit: &fields::Key) -> Result<Uuid, GfError> {
    let encoded = serde_json::to_string(unit).map_err(|_| invalid("invalid Proposal unit"))?;
    Ok(identity(proposal, &encoded))
}

pub(super) fn digest(value: &str) -> Result<Option<[u8; 32]>, GfError> {
    if value.is_empty() {
        return Ok(None);
    }
    if value.len() != 64 {
        return Err(invalid("invalid native Branch field commitment"));
    }
    let mut digest = [0; 32];
    for (index, bytes) in value.as_bytes().chunks_exact(2).enumerate() {
        let byte = std::str::from_utf8(bytes).map_err(|_| invalid("invalid field commitment"))?;
        digest[index] =
            u8::from_str_radix(byte, 16).map_err(|_| invalid("invalid field commitment"))?;
    }
    Ok(Some(digest))
}
