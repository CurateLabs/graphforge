//! Capture graph files using explicit topology membership and retained identities.

use super::{
    build_inventory_for_owned_layout, collect_source_files_from, encode_inventory,
    inventory_participant, GfError, GraphFilesInventory, Path, PathBuf, ProjectParticipant,
    ARTIFACT_IDENTITY,
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
        None,
        None,
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

/// Explicit mutable-workspace admission for migration. Published inventory
/// decoding and portable transport continue to use their versioned contracts.
pub(crate) fn capture_owned_route_migration_inventory(
    source_root: &Path,
) -> Result<GraphFilesInventory, GfError> {
    build_inventory_for_owned_layout(
        source_root,
        true,
        None,
        ARTIFACT_IDENTITY,
        &mut || Ok(()),
        None,
        None,
        None,
    )
    .map(|(inventory, _)| inventory)
}
