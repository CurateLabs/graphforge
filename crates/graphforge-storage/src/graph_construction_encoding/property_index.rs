//! Publish the persisted property indexes with the canonical inventory (#1983,
//! #1984), so the generation that holds the properties also holds the indexes
//! that answer `{id: $x}` anchors and edge property lookups over them.
//!
//! The node equality index is collected from the property fragments as they
//! are encoded and its files are written at the end. The edge property index is
//! cut into shards and packed as each fragment is pushed, because it carries the
//! property values and must not hold the route; a pack is written as soon as it
//! fills. Every file is written once, as an artifact of the same generation.
//!
//! An edge property fragment is also joined to the canonical edge files this
//! build already installed: each row's `edge_uuid` is resolved to its `edge_id`
//! with the same page-pruned read an identity probe uses, so the index can be
//! addressed by the id a traversal holds. Bulk construction ranks edges by UUID,
//! so those pages are disjoint and a fragment decodes about its own rows' pages.
//! A route whose topology cannot be joined (no edge file under its component,
//! overlapping pages, a row with no topology row) is published keyed by UUID
//! only.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::io::Cursor;
use std::path::{Path, PathBuf};

use arrow::array::{Array, FixedSizeBinaryArray};
use arrow::record_batch::RecordBatch;
use graphforge_core::GfError;

use super::{
    ConstructionEncodedArtifact, GraphConstructionEncodingEvidence, StableDirectory, copy_artifact,
};
use crate::edge_property_index::EdgeRouteIndexBuilder;
use crate::property_equality_index::{RouteIndexBuilder, source_digest};
use crate::route_component::RouteTable;
use crate::topology_identity::EdgeIdResolver;

/// The canonical edge files an edge property index is joined to: what the
/// encoding installed under `topology/edges/`, and where they are on disk.
pub(super) struct EdgeTopology {
    graph_root: PathBuf,
    artifacts: Vec<ConstructionEncodedArtifact>,
}

impl EdgeTopology {
    pub(super) fn new(graph_root: PathBuf, artifacts: Vec<ConstructionEncodedArtifact>) -> Self {
        Self {
            graph_root,
            artifacts,
        }
    }

    /// The join of the route with this component, or `None` when it has no edge
    /// file here or its UUID pages overlap too much for a join to be a set of
    /// point reads.
    fn open_join(&self, component: &str) -> Result<Option<EdgeJoin>, GfError> {
        let parts = route_parts(&self.artifacts, "topology/edges/", component);
        if parts.is_empty() {
            return Ok(None);
        }
        let paths: Vec<PathBuf> = parts
            .iter()
            .map(|(relative, _, _)| self.graph_root.join(relative))
            .collect();
        let paths: Vec<&Path> = paths.iter().map(PathBuf::as_path).collect();
        let resolver = EdgeIdResolver::open(&paths)?;
        Ok(resolver.is_point_readable().then(|| EdgeJoin {
            resolver,
            // The digest the inventory derives from the same objects, so a
            // reader can tell this topology is the one the ids came from.
            digest: source_digest(parts),
        }))
    }
}

/// One edge property route joined to its canonical topology.
struct EdgeJoin {
    resolver: EdgeIdResolver,
    digest: String,
}

/// The rows the persisted property indexes cover, by route component,
/// collected as the route's fragments are written.
pub(super) struct PropertyIndexTap {
    routes: BTreeMap<String, RouteIndexBuilder>,
    edge_routes: BTreeMap<String, EdgeRouteIndexBuilder>,
    topology: EdgeTopology,
    /// Each edge property route's join, `None` once it cannot be joined or has
    /// met a row with no topology row.
    edge_joins: BTreeMap<String, Option<EdgeJoin>>,
}

impl PropertyIndexTap {
    pub(super) fn new(topology: EdgeTopology) -> Self {
        Self {
            routes: BTreeMap::new(),
            edge_routes: BTreeMap::new(),
            topology,
            edge_joins: BTreeMap::new(),
        }
    }

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
            let ids = self.edge_ids(component, fragment)?;
            return self
                .edge_routes
                .entry(component.to_owned())
                .or_insert_with(|| EdgeRouteIndexBuilder::new(format!("{component}.packs")))
                .observe(fragment, ids.as_deref(), &mut |relative, bytes| {
                    copy_artifact(Cursor::new(bytes), output, &relative, artifacts, evidence)
                });
        }
        self.routes
            .entry(component.to_owned())
            .or_insert_with(RouteIndexBuilder::new)
            .observe(fragment);
        Ok(())
    }

    /// The topology `edge_id` of every row of `fragment`, or `None` when the
    /// route is not joined, no longer wants ids, or a row has no topology row
    /// (the route's ids are then withdrawn, not guessed).
    fn edge_ids(
        &mut self,
        component: &str,
        fragment: &RecordBatch,
    ) -> Result<Option<Vec<u64>>, GfError> {
        if self
            .edge_routes
            .get(component)
            .is_some_and(|builder| !builder.wants_ids())
        {
            return Ok(None);
        }
        let join = match self.edge_joins.entry(component.to_owned()) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(self.topology.open_join(component)?),
        };
        let Some(active) = join.as_ref() else {
            return Ok(None);
        };
        // A fragment that is not a sorted run of UUIDs is poisoned by the
        // builder; there is nothing to resolve for it.
        let Some(uuids) = sorted_uuids(fragment) else {
            return Ok(None);
        };
        let ids = active
            .resolver
            .resolve(&uuids)?
            .into_iter()
            .collect::<Option<Vec<u64>>>();
        if ids.is_none() {
            *join = None;
        }
        Ok(ids)
    }
}

/// The `edge_uuid` of every row of an edge property fragment, when they are
/// strictly ascending.
fn sorted_uuids(fragment: &RecordBatch) -> Option<Vec<[u8; 16]>> {
    let column = fragment
        .column_by_name("edge_uuid")?
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .filter(|uuids| uuids.value_length() == 16 && uuids.null_count() == 0)?;
    let uuids = (0..column.len())
        .map(|row| <[u8; 16]>::try_from(column.value(row)).ok())
        .collect::<Option<Vec<_>>>()?;
    uuids
        .windows(2)
        .all(|pair| pair[0] < pair[1])
        .then_some(uuids)
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
    let PropertyIndexTap {
        routes: node_routes,
        edge_routes,
        mut edge_joins,
        ..
    } = tap;
    let mut files: Vec<(String, Vec<u8>)> = Vec::new();
    for (component, builder) in node_routes {
        let route = routes.route(&component)?;
        let parts = route_parts(artifacts, "properties/", &component);
        files.extend(
            builder
                .finish(route, &source_digest(parts))?
                .into_iter()
                .map(|file| (file.relative_path, file.bytes)),
        );
    }
    for (component, builder) in edge_routes {
        let route = routes.route(&component)?;
        let parts = route_parts(artifacts, "edge_properties/", &component);
        let topology_digest = edge_joins
            .remove(&component)
            .flatten()
            .map(|join| join.digest);
        files.extend(
            builder
                .finish(route, &source_digest(parts), topology_digest.as_deref())?
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
