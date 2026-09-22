//! A subset gets a distinct immutable identity with explicit source provenance.
use crate::{CancellationToken, GfError, GraphForge, ResearchFieldIdentity};
use graphforge_storage::research_versions::{PreparedResearchContent, RegisterResearchVersion};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use uuid::Uuid;

/// Explicit native membership and field redaction, never a partial complete-Version copy.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchExportProjection {
    /// Fresh identity for this exact selection; distinct from the source Version.
    pub version_uuid: Uuid,
    /// Authenticated frozen Slice from the source Version.
    pub frozen_ipc: Vec<u8>,
    /// Exact selected native fields; immutable records remain atomic.
    pub fields: Vec<ResearchFieldIdentity>,
    /// Recorded UTC microseconds for this derivative.
    pub created_at: i64,
}

pub(super) fn prepare(
    owner: &GraphForge,
    source: Uuid,
    request: &ResearchExportProjection,
    cancel: &CancellationToken,
) -> Result<(PreparedResearchContent, [u8; 32]), GfError> {
    if request.version_uuid.is_nil()
        || request.version_uuid == source
        || request.fields.is_empty()
        || request.fields.len() > 256
        || request.frozen_ipc.len() > 64 * 1024 * 1024
    {
        return Err(GfError::Validation(
            "research projection requires bounded fields and a distinct Version identity".into(),
        ));
    }
    let selection = crate::slices::branch::authenticate(owner, &request.frozen_ipc, cancel)?;
    if selection.version.version_uuid != source {
        return Err(GfError::Validation(
            "research export Slice differs from source Version".into(),
        ));
    }
    let mut fields = request.fields.clone();
    fields.sort_by(|a, b| {
        (&a.object_kind, a.object_uuid, &a.field).cmp(&(&b.object_kind, b.object_uuid, &b.field))
    });
    if fields.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(GfError::Validation(
            "research projection fields must be unique".into(),
        ));
    }
    let mut digest = Sha256::new();
    digest.update(b"graphforge-research-export-selection/1");
    digest.update(source.as_bytes());
    digest.update(selection.selector_sha256);
    digest.update(
        serde_json::to_vec(&fields)
            .map_err(|_| GfError::Validation("invalid projection fields".into()))?,
    );
    let fingerprint: [u8; 32] = digest.finalize().into();
    let original = selection.version.clone();
    let mut spec = RegisterResearchVersion {
        version_uuid: request.version_uuid,
        context_uuid: original.context_uuid,
        source_generation_uuid: original.content.generation_uuid,
        selection: None,
        source_version: Some(source),
        required_versions: BTreeSet::new(),
        label: None,
        description: None,
        created_at: request.created_at,
        evidence: selection.evidence.clone(),
    };
    let mut frozen = crate::branches::field_selection::freeze(
        owner,
        owner.resolved_generation.container_root(),
        selection,
        &fields,
        &mut spec,
        cancel,
    )?
    .prepared;
    frozen.version.content.source_version = Some(source);
    // Prepared content is CAS-backed. Keep the stable source locator as provenance,
    // rather than allowing temporary preparation UUIDs to perturb package identity.
    frozen.version.content.generation_uuid = original.content.generation_uuid;
    frozen.version.content.manifest_sha256 = original.content.manifest_sha256;
    Ok((frozen, fingerprint))
}
