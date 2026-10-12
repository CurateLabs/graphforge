//! Publish the persisted property-equality index with the canonical inventory
//! (#1983), so the generation that holds the properties also holds the index
//! that answers `{id: $x}` anchors over them.

use std::collections::BTreeMap;
use std::io::Cursor;

use graphforge_core::GfError;

use super::{
    ConstructionEncodedArtifact, GraphConstructionEncodingEvidence, StableDirectory, copy_artifact,
    storage,
};
use crate::property_overlay::AuthenticatedPropertyInventory;
use crate::route_component::RouteTable;

/// Build the equality index of every node property route the encoding wrote
/// and register its files as artifacts of the same generation.
pub(super) fn write_property_equality_indexes(
    output: &StableDirectory,
    graph_root: &std::path::Path,
    routes: &RouteTable,
    artifacts: &mut Vec<ConstructionEncodedArtifact>,
    cancelled: &mut impl FnMut() -> bool,
    evidence: &mut GraphConstructionEncodingEvidence,
) -> Result<(), GfError> {
    let mut by_component: BTreeMap<String, Vec<crate::GraphFileEntry>> = BTreeMap::new();
    for artifact in artifacts.iter() {
        if !artifact.path.starts_with("properties/") {
            continue;
        }
        let Some(component) = crate::route_component::route_position(&artifact.path)? else {
            continue;
        };
        by_component
            .entry(component.to_owned())
            .or_default()
            .push(crate::GraphFileEntry {
                relative_path: artifact.path.clone(),
                byte_length: artifact.bytes,
                content_sha256: artifact.sha256.clone(),
                content_xxh64: artifact.xxh64,
                role: crate::GraphFileRole::Properties,
            });
    }
    let mut files = Vec::new();
    for (component, entries) in by_component {
        let route = routes.route(&component)?;
        let inventory = AuthenticatedPropertyInventory::capture_route_for_construction(
            graph_root, entries, routes, route,
        )?;
        files.extend(
            crate::property_equality_index::property_equality_index_files(
                &inventory,
                route,
                &mut || {
                    if cancelled() {
                        Err(storage("construction cancelled"))
                    } else {
                        Ok(())
                    }
                },
            )?,
        );
    }
    for file in files {
        copy_artifact(
            Cursor::new(file.bytes),
            output,
            &file.relative_path,
            artifacts,
            evidence,
        )?;
    }
    Ok(())
}
