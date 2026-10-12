//! Publish the persisted property-equality index with the canonical inventory
//! (#1983), so the generation that holds the properties also holds the index
//! that answers `{id: $x}` anchors over them.
//!
//! The index is collected from the property fragments as they are encoded, and
//! its files are written once, as artifacts of the same generation. Nothing is
//! read back from the encoded fragments.

use std::collections::BTreeMap;
use std::io::Cursor;

use arrow::record_batch::RecordBatch;
use graphforge_core::GfError;

use super::{
    ConstructionEncodedArtifact, GraphConstructionEncodingEvidence, StableDirectory, copy_artifact,
};
use crate::property_equality_index::{RouteIndexBuilder, source_digest};
use crate::route_component::RouteTable;

/// The `(value, node_uuid)` rows of every node property route, by route
/// component, collected as the route's fragments are written.
#[derive(Default)]
pub(super) struct PropertyIndexTap {
    routes: BTreeMap<String, RouteIndexBuilder>,
}

impl PropertyIndexTap {
    /// Observe a fragment about to be written at `path`. Only node property
    /// fragments are indexed.
    pub(super) fn observe(&mut self, path: &str, fragment: &RecordBatch) -> Result<(), GfError> {
        if !path.starts_with("properties/") {
            return Ok(());
        }
        let Some(component) = crate::route_component::route_position(path)? else {
            return Ok(());
        };
        self.routes
            .entry(component.to_owned())
            .or_insert_with(RouteIndexBuilder::new)
            .observe(fragment);
        Ok(())
    }
}

/// Write the equality index of every node property route the encoding wrote
/// and register its files as artifacts of the same generation.
pub(super) fn write_property_equality_indexes(
    output: &StableDirectory,
    routes: &RouteTable,
    tap: PropertyIndexTap,
    artifacts: &mut Vec<ConstructionEncodedArtifact>,
    evidence: &mut GraphConstructionEncodingEvidence,
) -> Result<(), GfError> {
    let mut files = Vec::new();
    for (component, builder) in tap.routes {
        let route = routes.route(&component)?;
        // The route's objects as the generation inventory will name them.
        let parts = artifacts
            .iter()
            .filter(|artifact| {
                artifact.path.starts_with("properties/")
                    && matches!(
                        crate::route_component::route_position(&artifact.path),
                        Ok(Some(candidate)) if candidate == component
                    )
            })
            .map(|artifact| (artifact.path.as_str(), artifact.bytes, artifact.xxh64))
            .collect::<Vec<_>>();
        files.extend(builder.finish(route, &source_digest(parts))?);
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
