//! Publish the persisted property indexes with the canonical inventory (#1983,
//! #1984), so the generation that holds the properties also holds the indexes
//! that answer `{id: $x}` anchors and edge property lookups over them.
//!
//! The node equality index is collected from the property fragments as they
//! are encoded and its files are written at the end. The edge property index is
//! cut into shards and packed as each fragment is pushed, because it carries the
//! property values and must not hold the route; a pack is written as soon as it
//! fills. Every file is written once, as an artifact of the same generation;
//! nothing is read back from the encoded fragments.

use std::collections::BTreeMap;
use std::io::Cursor;

use arrow::record_batch::RecordBatch;
use graphforge_core::GfError;

use super::{
    ConstructionEncodedArtifact, GraphConstructionEncodingEvidence, StableDirectory, copy_artifact,
};
use crate::edge_property_index::EdgeRouteIndexBuilder;
use crate::property_equality_index::{RouteIndexBuilder, source_digest};
use crate::route_component::RouteTable;

/// The rows the persisted property indexes cover, by route component,
/// collected as the route's fragments are written.
#[derive(Default)]
pub(super) struct PropertyIndexTap {
    routes: BTreeMap<String, RouteIndexBuilder>,
    edge_routes: BTreeMap<String, EdgeRouteIndexBuilder>,
}

impl PropertyIndexTap {
    /// Observe a fragment about to be written at `path`. Node property
    /// fragments feed the equality index; edge property fragments are cut into
    /// shards and packed, and each pack that fills is written to `output` at once.
    pub(super) fn observe(
        &mut self,
        path: &str,
        fragment: &RecordBatch,
        output: &StableDirectory,
        artifacts: &mut Vec<ConstructionEncodedArtifact>,
        evidence: &mut GraphConstructionEncodingEvidence,
    ) -> Result<(), GfError> {
        let edge = path.starts_with("edge_properties/");
        if !edge && !path.starts_with("properties/") {
            return Ok(());
        }
        let Some(component) = crate::route_component::route_position(path)? else {
            return Ok(());
        };
        if edge {
            return self
                .edge_routes
                .entry(component.to_owned())
                .or_insert_with(|| EdgeRouteIndexBuilder::new(format!("{component}.packs")))
                .observe(fragment, None, &mut |relative, bytes| {
                    copy_artifact(Cursor::new(bytes), output, &relative, artifacts, evidence)
                });
        }
        self.routes
            .entry(component.to_owned())
            .or_insert_with(RouteIndexBuilder::new)
            .observe(fragment);
        Ok(())
    }
}

/// The artifacts of the route with this component, as the generation inventory
/// will name them.
fn route_parts<'a>(
    artifacts: &'a [ConstructionEncodedArtifact],
    prefix: &str,
    component: &str,
) -> Vec<(&'a str, u64, u64)> {
    artifacts
        .iter()
        .filter(|artifact| {
            artifact.path.starts_with(prefix)
                && matches!(
                    crate::route_component::route_position(&artifact.path),
                    Ok(Some(candidate)) if candidate == component
                )
        })
        .map(|artifact| (artifact.path.as_str(), artifact.bytes, artifact.xxh64))
        .collect()
}

/// Write the persisted indexes of every property route the encoding wrote and
/// register their files as artifacts of the same generation.
pub(super) fn write_property_equality_indexes(
    output: &StableDirectory,
    routes: &RouteTable,
    tap: PropertyIndexTap,
    artifacts: &mut Vec<ConstructionEncodedArtifact>,
    evidence: &mut GraphConstructionEncodingEvidence,
) -> Result<(), GfError> {
    let mut files: Vec<(String, Vec<u8>)> = Vec::new();
    for (component, builder) in tap.routes {
        let route = routes.route(&component)?;
        let parts = route_parts(artifacts, "properties/", &component);
        files.extend(
            builder
                .finish(route, &source_digest(parts))?
                .into_iter()
                .map(|file| (file.relative_path, file.bytes)),
        );
    }
    for (component, builder) in tap.edge_routes {
        let route = routes.route(&component)?;
        let parts = route_parts(artifacts, "edge_properties/", &component);
        files.extend(
            builder
                .finish(route, &source_digest(parts), None)?
                .into_iter()
                .map(|file| (file.relative_path, file.bytes)),
        );
    }
    for (relative_path, bytes) in files {
        copy_artifact(
            Cursor::new(bytes),
            output,
            &relative_path,
            artifacts,
            evidence,
        )?;
    }
    Ok(())
}
