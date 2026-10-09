//! Research revision 6 compatibility (ADR 0055).
//!
//! A revision 6 registry is a valid revision 7 registry with no commit fields:
//! every field revision 7 added is omitted when empty, so the same bytes decode,
//! validate and digest identically under either label. Revision 6 is therefore
//! read as-is, its Versions are parentless legacy roots, and publishers that do
//! not write research carry it unchanged. The first research write relabels the
//! research capability and its participants to revision 7 through the ordinary
//! atomic publication; no record's bytes are rewritten.
use super::{
    invalid, Digest, GfError, ProjectCapability, ProjectParticipant, ResearchRegistry,
    ResearchVersionRecord, Sha256, RESEARCH_CAPABILITY, RESEARCH_REGISTRY, RESEARCH_VERSION,
};

/// Oldest research revision this build reads; older revisions are refused.
pub const RESEARCH_LEGACY_VERSION: u32 = 6;

/// Whether this build reads a research capability or registry revision.
#[must_use]
pub fn research_revision_readable(version: u32) -> bool {
    registry_schema(version).is_some()
}

/// Registry participant schema identity of a readable revision.
pub(super) fn registry_schema(version: u32) -> Option<[u8; 32]> {
    match version {
        RESEARCH_LEGACY_VERSION => Some(Sha256::digest(b"graphforge-research-registry/6").into()),
        RESEARCH_VERSION => Some(Sha256::digest(super::REGISTRY_SCHEMA).into()),
        _ => None,
    }
}

/// A revision 6 record carries none of the commit fields.
pub(super) fn legacy_record(version: &ResearchVersionRecord) -> bool {
    version.parents.is_empty()
        && version.author.is_none()
        && version.committer.is_none()
        && version.provenance.is_none()
}

impl ResearchRegistry {
    /// Refuse a registry labelled revision 6 that holds revision 7 data.
    pub(super) fn validate_legacy(&self) -> Result<(), GfError> {
        if !self.ancestry.is_empty()
            || !self.versions.values().all(legacy_record)
            || self.interchange.values().any(|archive| {
                archive.research_capability_version != RESEARCH_LEGACY_VERSION
                    || !archive.ancestry.is_empty()
                    || !archive.versions.values().all(legacy_record)
            })
        {
            return Err(invalid(
                "research revision 6 registry carries revision 7 commit data",
            ));
        }
        Ok(())
    }
}

/// Registry participant schema identity of the current revision.
#[must_use]
pub fn current_registry_schema() -> [u8; 32] {
    registry_schema(RESEARCH_VERSION).expect("current registry schema")
}

/// Relabel a publication's research capability and participants to the current
/// revision. Participant bytes are unchanged: revision 6 bytes are valid
/// revision 7 bytes. Requests without a legacy research label are untouched.
pub fn upgrade_research_request<'a>(
    capabilities: &mut [ProjectCapability],
    participants: impl IntoIterator<Item = &'a mut ProjectParticipant>,
) {
    for capability in capabilities
        .iter_mut()
        .filter(|c| c.capability_id == RESEARCH_CAPABILITY)
    {
        capability.capability_version = RESEARCH_VERSION;
    }
    for participant in participants
        .into_iter()
        .filter(|p| p.capability_id == RESEARCH_CAPABILITY)
    {
        participant.capability_version = RESEARCH_VERSION;
        if participant.record_family_id == RESEARCH_REGISTRY {
            participant.record_version = RESEARCH_VERSION;
            participant.schema_fingerprint = current_registry_schema();
        }
    }
}
