//! Capture graph files using explicit topology membership and retained identities.

use super::{
    ARTIFACT_IDENTITY, GfError, GraphFilesInventory, KnownGraphFile, Path, PathBuf,
    ProjectParticipant, build_inventory_for_owned_layout, collect_source_files_from,
    encode_inventory, inventory_participant,
};

/// Capture a writable tree using explicit topology membership.
pub fn capture_graph_files_with_topology(
    source_root: &Path,
    topology: &crate::TopologyFiles,
) -> Result<(GraphFilesInventory, ProjectParticipant), GfError> {
    let (inventory, _) = build_inventory_for_owned_layout(
        source_root,
        false,
        None,
        ARTIFACT_IDENTITY,
        &mut || Ok(()),
        Some(topology),
    )?;
    let bytes = encode_inventory(&inventory)?;
    let participant = inventory_participant(bytes, inventory.file_count)?;
    Ok((inventory, participant))
}

pub(crate) fn capture_graph_files_reusing_digests_with_topology(
    source_root: &Path,
    known: &std::collections::HashMap<String, KnownGraphFile>,
    domain: graphforge_core::hash_observation::HashDomain,
    topology: &crate::TopologyFiles,
) -> Result<(GraphFilesInventory, ProjectParticipant), GfError> {
    let (inventory, _) = build_inventory_for_owned_layout(
        source_root,
        false,
        Some(known),
        domain,
        &mut || Ok(()),
        Some(topology),
    )?;
    let bytes = encode_inventory(&inventory)?;
    let participant = inventory_participant(bytes, inventory.file_count)?;
    Ok((inventory, participant))
}

pub(crate) fn collect_source_files_with_topology(
    root: &Path,
    paths: &mut Vec<PathBuf>,
    topology: Option<&crate::TopologyFiles>,
) -> Result<(), GfError> {
    collect_source_files_from(root, root, paths, topology.is_some())?;
    if let Some(files) = topology {
        paths.extend(files.paths().map(Path::to_path_buf));
    }
    Ok(())
}
