//! Route identity for derived indexes (#1983).

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
}
