//! Route identity for derived indexes (#1983).

use std::path::Path;

use super::{AuthenticatedPropertyInventory, PropertyRouteKind};

impl AuthenticatedPropertyInventory {
    /// The routes of one kind that hold at least one fragment, in name order.
    pub(crate) fn route_names(&self, kind: PropertyRouteKind) -> Vec<String> {
        self.routes
            .iter()
            .filter(|((candidate, _), fragments)| *candidate == kind && !fragments.is_empty())
            .map(|((_, route), _)| route.clone())
            .collect()
    }

    /// Whether the route holds at least one fragment.
    pub(crate) fn has_property_route(&self, kind: PropertyRouteKind, route: &str) -> bool {
        self.routes
            .get(&(kind, route.to_owned()))
            .is_some_and(|fragments| !fragments.is_empty())
    }

    /// A digest of the exact bytes a route's newest-wins state derives from:
    /// every object's path, length and XXH64 as the manifest names them. Two
    /// inventories with equal digests hold the same route; a derived index
    /// stamped with it is current only while it matches. Reads no payload.
    pub(crate) fn route_source_digest(
        &self,
        kind: PropertyRouteKind,
        route: &str,
    ) -> Option<String> {
        let fragments = self.routes.get(&(kind, route.to_owned()))?;
        let parts = fragments
            .iter()
            .flat_map(|fragment| &fragment.parts)
            .map(|part| {
                (
                    part.entry.relative_path.as_str(),
                    part.entry.byte_length,
                    part.entry.content_xxh64,
                )
            })
            .collect::<Vec<_>>();
        Some(crate::property_equality_index::source_digest(parts))
    }

    /// A digest of the edge-topology files one relation route declares: each
    /// object's inventory path, length and XXH64 as the manifest names them.
    /// An edge's `edge_id` lives in these files, so a derived index that maps
    /// property rows to `edge_id` is current only while this digest matches.
    /// `None` when the inventory does not name the route's topology (it was
    /// narrowed to a property route, or the relation has no topology file).
    /// Reads no payload.
    pub(crate) fn edge_topology_source_digest(&self, route: &str) -> Option<String> {
        let files = self
            .edge_routes
            .get(route)
            .filter(|files| !files.is_empty())?;
        let parts = files
            .iter()
            .map(|file| {
                (
                    file.relative_path.as_str(),
                    file.byte_length,
                    file.content_xxh64,
                )
            })
            .collect::<Vec<_>>();
        Some(crate::property_equality_index::source_digest(parts))
    }

    /// The admitted topology files of one relation route, in declaration order.
    pub(crate) fn edge_topology_files(&self, route: &str) -> Vec<&Path> {
        self.edge_routes
            .get(route)
            .into_iter()
            .flatten()
            .map(|file| file.path.as_path())
            .collect()
    }
}
