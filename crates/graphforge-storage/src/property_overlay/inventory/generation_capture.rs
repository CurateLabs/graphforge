//! Generation-owned property authority with admission before eager capture.

#[cfg(test)]
use super::{Arc, AtomicU64, FragmentHandleCounts, Mutex};
use super::{
    AuthenticatedPropertyInventory, BTreeMap, FragmentAdmission, GfError, InventoryAdmission, Path,
    PropertyRouteKind, Read, Seek, corrupt, io_error, resolve_versioned_property_entries_for_route,
    topology_authority,
};

impl AuthenticatedPropertyInventory {
    /// Resolve property authority from a pinned project generation.
    ///
    /// Expanded V1 generations retain files beneath their authenticated graph
    /// tree. Compact V2 generations retain the exact digest-addressed CAS
    /// object named by each authenticated manifest entry.
    pub fn from_resolved_generation(
        generation: &crate::ResolvedProjectGeneration,
    ) -> Result<Self, GfError> {
        Self::from_resolved_generation_route(generation, None)
    }

    /// Resolve property authority with the query's final inventory allowance.
    /// Eager V1 footers and lazy V2 footers use this ceiling from first admission.
    pub fn from_resolved_generation_with_query_memory_budget(
        generation: &crate::ResolvedProjectGeneration,
        query_memory_budget: u64,
    ) -> Result<Self, GfError> {
        Self::from_resolved_generation_route_with_budget(
            generation,
            None,
            Some(query_memory_budget),
        )
    }

    /// Admit the private replay workspace of a pinned delta-bearing generation.
    ///
    /// The retained files come from `root`, while the generation lease remains
    /// the immutable authority that owns the verified base plus delta run.
    pub fn from_materialized_generation(
        generation: &crate::ResolvedProjectGeneration,
        root: &Path,
        entries: Vec<crate::GraphFileEntry>,
    ) -> Result<Self, GfError> {
        let mut admitted = Self::from_entries_at_root(root, entries)?;
        admitted.seed_semantic_property_schemas(generation)?;
        admitted.generation_lease = Some(generation.clone());
        Ok(admitted)
    }

    /// Admit a materialized workspace with its explicit captured layout authority.
    pub fn from_materialized_inventory(
        generation: &crate::ResolvedProjectGeneration,
        root: &Path,
        inventory: crate::GraphFilesInventory,
    ) -> Result<Self, GfError> {
        let mut admitted = Self::from_inventory_at_root(root, inventory, None)?;
        admitted.seed_semantic_property_schemas(generation)?;
        admitted.generation_lease = Some(generation.clone());
        Ok(admitted)
    }

    /// Admit a materialized workspace with the query's final inventory share
    /// before eager route validation, retaining its generation authority.
    pub fn from_materialized_inventory_with_query_memory_budget(
        generation: &crate::ResolvedProjectGeneration,
        root: &Path,
        inventory: crate::GraphFilesInventory,
        query_memory_budget: u64,
    ) -> Result<Self, GfError> {
        let mut admitted = Self::from_inventory_at_root_with_admission_and_budget(
            root,
            inventory,
            None,
            false,
            None,
            Some(query_memory_budget),
        )?;
        admitted.seed_semantic_property_schemas(generation)?;
        admitted.generation_lease = Some(generation.clone());
        admitted.set_query_memory_budget(query_memory_budget)?;
        Ok(admitted)
    }

    /// Admit checksum-only private replay files under their owning immutable generation.
    /// This read authority cannot be serialized as a CAS publication inventory.
    pub fn from_private_workspace_inventory(
        generation: &crate::ResolvedProjectGeneration,
        root: &Path,
        inventory: crate::GraphReadInventory,
    ) -> Result<Self, GfError> {
        let mut admitted = Self::from_read_inventory_at_root(root, inventory, None)?;
        admitted.seed_semantic_property_schemas(generation)?;
        admitted.generation_lease = Some(generation.clone());
        Ok(admitted)
    }

    /// Admit private replay authority using its query allowance before eager
    /// footer validation. The immutable generation still owns the read lease.
    pub fn from_private_workspace_inventory_with_query_memory_budget(
        generation: &crate::ResolvedProjectGeneration,
        root: &Path,
        inventory: crate::GraphReadInventory,
        query_memory_budget: u64,
    ) -> Result<Self, GfError> {
        let mut admitted = Self::from_read_inventory_at_root_for_routes_with_budget(
            root,
            inventory,
            None,
            Some(query_memory_budget),
        )?;
        admitted.seed_semantic_property_schemas(generation)?;
        admitted.generation_lease = Some(generation.clone());
        admitted.set_query_memory_budget(query_memory_budget)?;
        Ok(admitted)
    }

    /// Resolve one route without opening or hashing authenticated entries for
    /// unrelated routes in the pinned generation inventory.
    pub fn from_resolved_generation_for_route(
        generation: &crate::ResolvedProjectGeneration,
        kind: PropertyRouteKind,
        route: &str,
    ) -> Result<Self, GfError> {
        Self::from_resolved_generation_route(generation, Some((kind, route)))
    }

    fn from_resolved_generation_route(
        generation: &crate::ResolvedProjectGeneration,
        requested_route: Option<(PropertyRouteKind, &str)>,
    ) -> Result<Self, GfError> {
        Self::from_resolved_generation_route_with_budget(generation, requested_route, None)
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one constructor per participant version, each binding its own root and route authority"
    )]
    fn from_resolved_generation_route_with_budget(
        generation: &crate::ResolvedProjectGeneration,
        requested_route: Option<(PropertyRouteKind, &str)>,
        query_memory_budget: Option<u64>,
    ) -> Result<Self, GfError> {
        let Some(participant) = generation.declared_graph_files_participant()? else {
            return Ok(Self {
                memory_budget: query_memory_budget.map_or_else(
                    super::super::memory_budget::InventoryMemoryBudget::default,
                    super::super::memory_budget::InventoryMemoryBudget::for_query_memory_budget,
                ),
                root: None,
                root_path: None,
                generation_lease: Some(generation.clone()),
                routes: BTreeMap::new(),
                edge_routes: BTreeMap::new(),
                node_files: requested_route.is_none().then(Vec::new),
                schemas: BTreeMap::new(),
                route_summaries: BTreeMap::new(),
                node_topology_files: requested_route.is_none().then(Vec::new),
                authority_bytes: 0,
                authority_block_equivalents: 0,
                authority_read_calls: 0,
                #[cfg(test)]
                handle_counts: Arc::new(FragmentHandleCounts::default()),
                #[cfg(test)]
                late_decoder_failure_row_countdown: Arc::new(AtomicU64::new(0)),
                #[cfg(test)]
                mutation_barrier: Mutex::new(None),
            });
        };
        // Payload content is admitted on first touch (or, for property
        // fragments, when each fragment is admitted below).
        let inventory = generation
            .unadmitted_graph_files_inventory()?
            .ok_or_else(|| {
                corrupt("declared graph-files participant has no authenticated inventory")
            })?;
        let mut admitted = match participant {
            crate::graph_files::GraphFilesParticipant::V1(_) => {
                let root = generation.graph_tree_root();
                let route_table = crate::route_component::authenticate_manifest_routes(
                    inventory.format_version,
                    &inventory.files,
                    |entry| {
                        let mut retained =
                            crate::graph_files::resolve_v1_inventory_entry_retained(&root, entry)?;
                        retained.file.rewind().map_err(io_error)?;
                        let mut bytes = Vec::new();
                        retained
                            .file
                            .take(entry.byte_length + 1)
                            .read_to_end(&mut bytes)
                            .map_err(io_error)?;
                        Ok(bytes)
                    },
                )?;
                let topology = topology_authority(
                    &root,
                    &inventory.files,
                    route_table.as_ref(),
                    false,
                    requested_route,
                    None,
                )?;
                let entries = resolve_versioned_property_entries_for_route(
                    &root,
                    inventory.files,
                    requested_route,
                    route_table.as_ref(),
                    None,
                )?;
                let mut admitted = Self::admit_entries_with_admission(
                    &root,
                    entries,
                    requested_route,
                    route_table.as_ref(),
                    false,
                    None,
                    InventoryAdmission {
                        fragments: FragmentAdmission::Eager,
                        query_memory_budget,
                    },
                )?;
                admitted.edge_routes = topology.edge_routes;
                admitted.node_files = topology.node_files;
                Ok::<Self, GfError>(admitted)
            }
            crate::graph_files::GraphFilesParticipant::V2(_) => {
                let root = generation.container_root();
                let route_table = crate::route_component::authenticate_manifest_routes(
                    inventory.format_version,
                    &inventory.files,
                    |entry| {
                        crate::graph_object_store::read_graph_control_object_by_digest(
                            root,
                            &entry.content_sha256,
                            64 * 1024 * 1024,
                        )
                    },
                )?;
                let topology = topology_authority(
                    root,
                    &inventory.files,
                    route_table.as_ref(),
                    true,
                    requested_route,
                    None,
                )?;
                let entries = inventory
                    .files
                    .into_iter()
                    .map(|entry| {
                        let path = crate::graph_object_path(root, &entry.content_sha256)?;
                        let relative = path
                            .strip_prefix(root)
                            .map_err(|_| corrupt("graph object path escaped its container"))?;
                        Ok((entry, relative.to_path_buf()))
                    })
                    .collect::<Result<Vec<_>, GfError>>()?;
                let mut admitted = Self::admit_entries_with_admission(
                    root,
                    entries,
                    requested_route,
                    route_table.as_ref(),
                    false,
                    None,
                    InventoryAdmission {
                        fragments: FragmentAdmission::FirstTouch,
                        query_memory_budget,
                    },
                )?;
                admitted.edge_routes = topology.edge_routes;
                admitted.node_files = topology.node_files;
                Ok::<Self, GfError>(admitted)
            }
        }?;
        admitted.seed_semantic_property_schemas(generation)?;
        admitted.generation_lease = Some(generation.clone());
        if let Some(query_memory_budget) = query_memory_budget {
            admitted.set_query_memory_budget(query_memory_budget)?;
        }
        Ok(admitted)
    }
}
