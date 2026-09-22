//! Private complete Project preparation; no intermediate authoritative capture.
use super::{
    GfError, Path, PreparedResearchContent, RegisterResearchVersion, ResearchEvidenceReference,
    ResearchVersionRecord, Uuid, cancelled, invalid,
};
use std::sync::atomic::AtomicBool;

/// A complete Project draft seeded from an authenticated, pinned CURRENT.
/// The private container and original CAS lease live through final preparation.
/// Its identity cannot be supplied by an arbitrary selected Version.
pub struct PreparedProjectDraft {
    root: std::path::PathBuf,
    directory: tempfile::TempDir,
    seed: PreparedResearchContent,
    parent: crate::ResolvedProjectGeneration,
}

/// Prepare complete current Project content in a private ephemeral container.
/// This installs authenticated CAS objects but never publishes source CURRENT,
/// registry identities, heads, roots or receipts. `spec` must describe a fresh
/// complete capture of CURRENT, not a selected or historical projection.
pub fn prepare_project_draft(
    root: &Path,
    spec: &RegisterResearchVersion,
    cancellation: &AtomicBool,
) -> Result<PreparedProjectDraft, GfError> {
    cancelled(cancellation)?;
    let parent = crate::resolve_project_generation(root)?;
    let registry = super::read_research_registry(&parent)?;
    if spec.source_generation_uuid != parent.generation_uuid()
        || spec.selection.is_some()
        || spec.source_version.is_some()
        || !spec.required_versions.is_empty()
        || spec.version_uuid.is_nil()
        || spec.context_uuid.is_nil()
        || registry.identities.contains_key(&spec.version_uuid)
        || registry.branches.contains_key(&spec.context_uuid)
    {
        return Err(invalid(
            "Project preparation requires a fresh complete CURRENT identity",
        ));
    }
    let version = super::capture(root, spec, &registry)?;
    let seed = super::branch_content::prepare_content(root, &parent, version, cancellation)?;
    let directory =
        tempfile::tempdir().map_err(|_| invalid("cannot allocate private Project preparation"))?;
    super::project_restore::materialize(root, &seed.version, directory.path(), true)?;
    cancelled(cancellation)?;
    Ok(PreparedProjectDraft {
        root: root.to_path_buf(),
        directory,
        seed,
        parent,
    })
}

impl PreparedProjectDraft {
    /// Complete private Project for native domain-owner edits. Ephemeral only.
    #[must_use]
    pub fn path(&self) -> &Path {
        self.directory.path()
    }

    /// Optimistic authoritative generation from which this draft was prepared.
    #[must_use]
    pub fn parent_generation_uuid(&self) -> Uuid {
        self.parent.generation_uuid()
    }

    /// Seal the complete edited draft under a lease in the owning Project CAS.
    /// The domain owner supplies and validates the final selected evidence closure.
    /// The returned Version keeps the original complete Project capture identity;
    /// publication must still compare CURRENT and atomically install its receipt.
    pub fn finish(
        &self,
        evidence: Vec<ResearchEvidenceReference>,
        cancellation: &AtomicBool,
    ) -> Result<PreparedResearchContent, GfError> {
        cancelled(cancellation)?;
        let source = crate::resolve_project_generation(self.path())?;
        let mut version: ResearchVersionRecord = self.seed.version.clone();
        version.content.evidence = evidence;
        super::branch_content::prepare_content(&self.root, &source, version, cancellation)
    }
}
