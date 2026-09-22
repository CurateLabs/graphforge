//! Physical selected-content projection for a fresh unpublished derivative Version.
use super::super::{Digest, GfError, Path, Sha256, invalid, retained_content};
/// Repack a fresh derivative using native effective-property projection.
/// Deleted values and private operational index receipts never enter its payload.
/// Original registered Versions remain immutable.
pub fn canonicalize_prepared_research_projection(
    root: &Path,
    prepared: &mut super::super::PreparedResearchContent,
) -> Result<(), GfError> {
    let current = crate::resolve_project_generation(root)?;
    let registry = super::super::read_research_registry(&current)?;
    if registry
        .identities
        .contains_key(&prepared.version.version_uuid)
        || prepared.version.content.source_version.is_none()
    {
        return Err(invalid(
            "canonical projection requires a fresh unpublished derivative identity",
        ));
    }
    let snapshots = retained_content::inspect(root, &prepared.version, None)?;
    let Some(snapshot) = snapshots
        .iter()
        .find(|p| p.capability_id == "graph" && p.record_family_id == "files")
    else {
        return Ok(());
    };
    let directory =
        tempfile::tempdir().map_err(|_| invalid("cannot prepare canonical projection"))?;
    retained_content::materialize_graph_snapshot(root, snapshot, directory.path())?;
    let output =
        tempfile::tempdir().map_err(|_| invalid("cannot prepare compact research projection"))?;
    crate::graph_projection::repack_portable_graph_tree(directory.path(), output.path())?;
    let (inventory, _) = crate::capture_graph_files(output.path())?;
    let graph = retained_content::install_graph(&prepared.lease, output.path(), &inventory)?;
    let replacement = crate::graph_files::graph_files_root_participant(&graph)?;
    crate::graph_object_store::install_graph_object_bytes(root, &replacement.bytes)?;
    let commitment = prepared
        .version
        .content
        .participants
        .iter_mut()
        .find(|p| p.key.capability == "graph" && p.key.family == "files")
        .ok_or_else(|| invalid("prepared graph commitment disappeared"))?;
    commitment.record_version = replacement.record_version;
    commitment.schema_sha256 = replacement.schema_fingerprint;
    commitment.row_count = replacement.row_count;
    commitment.content_sha256 = Sha256::digest(&replacement.bytes).into();
    retained_content::inspect(root, &prepared.version, None)?;
    Ok(())
}
