//! Shared admission for retained property-inventory structure and footers.

use std::mem::size_of;
use std::sync::Mutex;

use graphforge_core::{ApiErrorCode, GfError};

use super::{
    AuthenticatedPropertyFragment, AuthenticatedPropertyInventory, FragmentFooter,
    PropertyRouteKind,
};
use crate::read_cache_budget::ReadCacheBudget;

#[derive(Debug)]
pub(super) struct InventoryMemoryBudget(Mutex<Bytes>);

#[derive(Debug)]
struct Bytes {
    limit: usize,
    structure: usize,
    footers: usize,
}

impl Default for InventoryMemoryBudget {
    fn default() -> Self {
        Self(Mutex::new(Bytes {
            limit: usize::try_from(ReadCacheBudget::DEFAULT.inventory_bytes).unwrap_or(usize::MAX),
            structure: 0,
            footers: 0,
        }))
    }
}

fn refusal(requested: usize, limit: usize) -> GfError {
    GfError::Api {
        code: ApiErrorCode::ResourceLimit,
        message: format!(
            "property inventory metadata requires {requested} bytes against its {limit} byte read-cache share"
        ),
    }
}

impl InventoryMemoryBudget {
    fn configure(&self, limit: usize, structure: usize) -> Result<(), GfError> {
        let mut bytes = self
            .0
            .lock()
            .expect("property inventory memory budget lock");
        let requested = structure.saturating_add(bytes.footers);
        if requested > limit {
            return Err(refusal(requested, limit));
        }
        bytes.limit = limit;
        bytes.structure = structure;
        Ok(())
    }

    fn admit(&self, requested: usize) -> Result<(), GfError> {
        let mut bytes = self
            .0
            .lock()
            .expect("property inventory memory budget lock");
        let total = bytes
            .structure
            .saturating_add(bytes.footers)
            .saturating_add(requested);
        if total > bytes.limit {
            return Err(refusal(total, bytes.limit));
        }
        bytes.footers = bytes.footers.saturating_add(requested);
        Ok(())
    }

    fn ensure_structure(&self, structure: impl FnOnce() -> usize) -> Result<(), GfError> {
        let mut bytes = self
            .0
            .lock()
            .expect("property inventory memory budget lock");
        if bytes.structure == 0 {
            let structure = structure();
            let requested = structure.saturating_add(bytes.footers);
            if requested > bytes.limit {
                return Err(refusal(requested, bytes.limit));
            }
            bytes.structure = structure;
        }
        Ok(())
    }

    pub(super) fn retained(&self) -> usize {
        let bytes = self
            .0
            .lock()
            .expect("property inventory memory budget lock");
        bytes.structure.saturating_add(bytes.footers)
    }
}

pub(super) fn admit_footer(
    budget: &InventoryMemoryBudget,
    fragment: &AuthenticatedPropertyFragment,
    footer: &FragmentFooter,
) -> Result<(), GfError> {
    fragment
        .footer_admission
        .get_or_init(|| {
            // Include both the parsed Parquet metadata and Arrow schema. A second
            // copy allows the route summary to merge fields/metadata while the
            // source footer remains cached. Parquet's method counts capacities
            // and shared pointers conservatively, but excludes allocator overhead.
            let schema_bytes = schema_bytes(footer.schema.as_ref());
            budget.admit(
                footer
                    .metadata
                    .memory_size()
                    .saturating_add(schema_bytes)
                    .saturating_mul(2),
            )
        })
        .clone()
}

fn schema_bytes(schema: &arrow::datatypes::Schema) -> usize {
    size_of::<arrow::datatypes::Schema>()
        .saturating_add(
            schema
                .fields()
                .iter()
                .map(|field| field.size())
                .sum::<usize>(),
        )
        .saturating_add(
            schema
                .metadata()
                .iter()
                .map(|(key, value)| key.capacity() + value.capacity() + 64)
                .sum::<usize>(),
        )
}

impl AuthenticatedPropertyInventory {
    pub(super) fn ensure_structural_admission(&self) -> Result<(), GfError> {
        self.memory_budget
            .ensure_structure(|| self.structural_bytes())
    }

    /// Set the inventory share of the configured query budget before reads.
    /// Cached footers and route/path/map structure draw on this one ceiling.
    ///
    /// # Errors
    /// Fails with `GF_RESOURCE_LIMIT`, naming property inventory metadata, if
    /// existing retained structure and footers cannot fit. Zero is a zero-byte
    /// ceiling, never an unlimited-memory mode.
    pub fn set_query_memory_budget(&self, query_budget: u64) -> Result<(), GfError> {
        let limit =
            usize::try_from(ReadCacheBudget::from_query_budget(query_budget).inventory_bytes)
                .unwrap_or(usize::MAX);
        self.memory_budget.configure(limit, self.structural_bytes())
    }

    /// Aggregate inventory structure and memoized footer admission bytes.
    #[must_use]
    pub fn retained_metadata_bytes(&self) -> usize {
        self.memory_budget.retained()
    }

    /// Sound upper bound on logical rows for one route.
    ///
    /// The newest-wins merge and tombstones can only remove physical fragment
    /// rows, so their admitted footer counts are a safe planning estimate. It
    /// is deliberately not advertised as an exact logical count.
    ///
    /// # Errors
    /// Propagates content admission and footer errors.
    pub fn route_row_upper_bound(
        &self,
        kind: PropertyRouteKind,
        route: &str,
    ) -> Result<usize, GfError> {
        let Some(fragments) = self.routes.get(&(kind, route.to_owned())) else {
            return Ok(0);
        };
        fragments.iter().try_fold(0usize, |rows, fragment| {
            let physical_rows = self
                .fragment_footer_of(fragment, kind, route)?
                .physical_rows;
            Ok(rows.saturating_add(physical_rows))
        })
    }

    fn structural_bytes(&self) -> usize {
        // BTree nodes hold spare entries and links. Three times entry storage
        // plus 64 bytes per entry covers that slack; vectors count capacity.
        let mut bytes = size_of::<Self>();
        bytes = bytes.saturating_add(
            self.root_path
                .as_ref()
                .map_or(0, std::path::PathBuf::capacity),
        );
        for ((_, route), fragments) in &self.routes {
            bytes = bytes.saturating_add(
                3 * (size_of::<(String, Vec<AuthenticatedPropertyFragment>)>() + 64),
            );
            bytes = bytes.saturating_add(route.capacity());
            bytes = bytes
                .saturating_add(fragments.capacity() * size_of::<AuthenticatedPropertyFragment>());
            for fragment in fragments {
                // Three cells may memoize the named resource refusal. Their
                // diagnostic is at most 128 bytes; allow allocation slack.
                bytes = bytes.saturating_add(3 * 256);
                bytes = bytes.saturating_add(
                    fragment.entry.relative_path.capacity() + fragment.physical_relative.capacity(),
                );
                bytes = bytes.saturating_add(
                    fragment.parts.capacity() * size_of::<super::PropertyObjectPart>(),
                );
                for part in &fragment.parts {
                    bytes = bytes.saturating_add(
                        part.entry.relative_path.capacity() + part.physical_relative.capacity(),
                    );
                }
            }
        }
        for (route, files) in &self.edge_routes {
            bytes = bytes.saturating_add(
                route.capacity() + 3 * (size_of::<(String, Vec<super::AdmittedEdgeFile>)>() + 64),
            );
            bytes = bytes.saturating_add(edge_files_bytes(files));
        }
        if let Some(files) = &self.node_files {
            bytes = bytes.saturating_add(edge_files_bytes(files));
        }
        for ((_, route), schema) in &self.schemas {
            bytes = bytes.saturating_add(
                route.capacity()
                    + schema_bytes(schema)
                    + 3 * (size_of::<(String, arrow::datatypes::SchemaRef)>() + 64),
            );
        }
        for (_, route) in self.route_summaries.keys() {
            bytes = bytes.saturating_add(
                route.capacity()
                    + 3 * (size_of::<(String, std::sync::OnceLock<super::RouteSummaryOutcome>)>()
                        + 64),
            );
        }
        if let Some(files) = &self.node_topology_files {
            bytes = bytes
                .saturating_add(files.capacity() * size_of::<crate::catalog::AdmittedSourceFile>());
            for file in files {
                bytes = bytes.saturating_add(file.name.capacity());
            }
        }
        bytes
    }
}

fn edge_files_bytes(files: &Vec<super::AdmittedEdgeFile>) -> usize {
    files.capacity() * size_of::<super::AdmittedEdgeFile>()
        + files
            .iter()
            .map(|file| file.path.capacity() + file.relative_path.capacity())
            .sum::<usize>()
}
