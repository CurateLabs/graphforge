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

    /// A digest of the exact bytes a route's newest-wins state derives from:
    /// every object's path, length and XXH64 as the manifest names them. Two
    /// inventories with equal digests hold the same route; a derived index
    /// stamped with it is current only while it matches. Reads no payload.
    pub(crate) fn route_source_digest(
        &self,
        kind: PropertyRouteKind,
        route: &str,
    ) -> Option<String> {
        use sha2::{Digest, Sha256};
        let fragments = self.routes.get(&(kind, route.to_owned()))?;
        let mut parts = fragments
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
        parts.sort_unstable();
        let mut hasher = Sha256::new();
        for (path, length, xxh64) in parts {
            hasher.update(u64::try_from(path.len()).unwrap_or(u64::MAX).to_le_bytes());
            hasher.update(path.as_bytes());
            hasher.update(length.to_le_bytes());
            hasher.update(xxh64.to_le_bytes());
        }
        Some(crate::property_equality_index::hex(&hasher.finalize()))
    }
}
