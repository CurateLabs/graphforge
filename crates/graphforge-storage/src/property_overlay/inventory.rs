//! Inventory for authenticated property overlays.

#[cfg(test)]
use super::{FragmentHandleCounts, TestMutationBarrier};

use super::Ordering;
#[cfg(test)]
use super::{AtomicU64, Mutex};

use super::{
    AdmittedEdgeFile, Arc, AuthenticatedPropertyFragment, AuthenticatedPropertyInventory, BTreeMap,
    BTreeSet, Bytes, Deserialize, File, FragmentHandleGuard, GfError, HashMap,
    OpenPropertyFragment, PROPERTY_GENERATION_KEY, PROPERTY_KIND_KEY, PROPERTY_LIVE_SCHEMA_FORMAT,
    PROPERTY_LIVE_SCHEMA_KEY, PROPERTY_ORDINAL_KEY, PROPERTY_OVERLAY_FORMAT_KEY,
    PROPERTY_ROUTE_KEY, PROPERTY_TOMBSTONE_FIELD, ParquetRecordBatchReaderBuilder,
    PartAuthentication, Path, PathBuf, PropertyFile, PropertyFragment, PropertyFragmentId,
    PropertyFragmentLayout, PropertyInventoryOpenMetrics, PropertyObjectPart, PropertyRouteKind,
    PropertySnapshotRow, Read, Seek, Serialize, Write, corrupt, fs, io_error, json_error,
    parquet_error, retained_read_at, validate_fragment_schema,
};

impl AuthenticatedPropertyInventory {
    /// Authenticate the current property authority of a project or materialized
    /// workspace. Sessions should retain and share the resulting inventory.
    pub fn capture(project: &Path) -> Result<Self, GfError> {
        authenticated_property_inventory(project)
    }

    /// Capture an unpublished portable graph with the importer's existing
    /// Parquet resource policy applied before any property metadata is decoded.
    pub(crate) fn capture_for_import(
        graph_tree: &Path,
        cancelled: Option<&std::sync::atomic::AtomicBool>,
    ) -> Result<Self, GfError> {
        let inventory =
            crate::graph_files::capture_graph_files_with_cancellation(graph_tree, || {
                check_import_cancelled(cancelled)
            })?;
        Self::from_inventory_at_root_with_admission(graph_tree, inventory, None, true, cancelled)
    }

    /// Return semantic relation names and their admitted physical payload paths.
    /// The inventory retains the generation lease and route authority.
    #[must_use]
    pub fn edge_files(&self, relation: Option<&str>) -> Vec<(String, PathBuf)> {
        self.edge_routes
            .iter()
            .filter(|(route, _)| relation.is_none_or(|expected| expected == route.as_str()))
            .flat_map(|(route, paths)| paths.iter().map(|path| (route.clone(), path.path.clone())))
            .collect()
    }

    /// Every admitted edge payload with its inventory-relative path, from
    /// which planning takes a row-count hint without opening the file (#1388).
    #[must_use]
    pub fn edge_fragments(&self, relation: Option<&str>) -> Vec<(String, PathBuf, String)> {
        self.edge_routes
            .iter()
            .filter(|(route, _)| relation.is_none_or(|expected| expected == route.as_str()))
            .flat_map(|(route, paths)| {
                paths
                    .iter()
                    .map(|path| (route.clone(), path.path.clone(), path.relative_path.clone()))
            })
            .collect()
    }

    /// The node topology payloads this inventory declares, in canonical order
    /// (the legacy flat file first, then range shards), with their
    /// inventory-relative paths. `None` when the inventory is route-scoped and
    /// carries no topology authority.
    #[must_use]
    pub fn node_fragments(&self) -> Option<Vec<(PathBuf, String)>> {
        let files = self.node_files.as_ref()?;
        let mut fragments: Vec<_> = files
            .iter()
            .map(|file| (file.path.clone(), file.relative_path.clone()))
            .collect();
        fragments.sort_by(|a, b| a.1.cmp(&b.1));
        Some(fragments)
    }

    pub(crate) fn has_edge_route(&self, route: &str) -> bool {
        self.edge_routes.contains_key(route)
    }

    pub(crate) fn edge_rewrite_files(&self) -> impl Iterator<Item = (&str, &Path, &str)> {
        self.edge_routes.iter().flat_map(|(route, files)| {
            files.iter().map(move |file| {
                (
                    route.as_str(),
                    file.path.as_path(),
                    file.relative_path.as_str(),
                )
            })
        })
    }

    pub(crate) fn admitted_source_files(
        &self,
        kind: PropertyRouteKind,
    ) -> Vec<crate::catalog::AdmittedSourceFile> {
        let mut files = Vec::new();
        for ((candidate, _), fragments) in &self.routes {
            if *candidate != kind {
                continue;
            }
            for fragment in fragments {
                for part in &fragment.parts {
                    files.push(crate::catalog::AdmittedSourceFile {
                        name: part.entry.relative_path.clone(),
                        byte_length: part.entry.byte_length,
                        content_xxh64: part.entry.content_xxh64,
                    });
                }
            }
        }
        files.sort_unstable_by(|left, right| left.name.cmp(&right.name));
        files
    }

    #[cfg(test)]
    pub(super) fn live_fragment_handles(&self) -> u64 {
        self.handle_counts.live.load(Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(super) fn peak_fragment_handles(&self) -> u64 {
        self.handle_counts.peak.load(Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(super) fn reset_peak_fragment_handles(&self) {
        assert_eq!(self.live_fragment_handles(), 0);
        self.handle_counts.peak.store(0, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn fail_decoder_on_row(&self, row: u64) {
        assert!(row > 0);
        self.late_decoder_failure_row_countdown
            .store(row, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(super) fn arm_mutation_after_authentication(&self) -> Arc<TestMutationBarrier> {
        let barrier = Arc::new(TestMutationBarrier {
            authenticated: std::sync::Barrier::new(2),
            proceed: std::sync::Barrier::new(2),
            copied: std::sync::Barrier::new(2),
            restored: std::sync::Barrier::new(2),
        });
        *self.mutation_barrier.lock().expect("mutation barrier lock") = Some(Arc::clone(&barrier));
        barrier
    }

    pub(super) fn open_fragment(
        &self,
        fragment: &AuthenticatedPropertyFragment,
        scratch: &Path,
    ) -> Result<OpenPropertyFragment, GfError> {
        let root = self
            .root
            .as_ref()
            .ok_or_else(|| corrupt("property inventory lacks its retained root capability"))?;
        if let Some(layout) = fragment.envelope {
            return Ok(OpenPropertyFragment {
                file: Arc::new(segmented_file(
                    root,
                    &fragment.parts,
                    layout,
                    scratch,
                    #[cfg(test)]
                    self.mutation_barrier
                        .lock()
                        .expect("mutation barrier lock")
                        .take(),
                )?),
                authentication_bytes: 0,
                authentication_block_equivalents: 0,
                authentication_read_calls: 0,
                handle: FragmentHandleGuard::acquired(
                    #[cfg(test)]
                    &self.handle_counts,
                ),
            });
        }
        let file = open_retained_under(root, &fragment.physical_relative)?;
        let handle = FragmentHandleGuard::acquired(
            #[cfg(test)]
            &self.handle_counts,
        );
        if graphforge_filesystem::file_identity(&file).map_err(io_error)? != fragment.identity {
            return Err(corrupt(
                "property fragment identity changed after admission",
            ));
        }
        #[cfg(test)]
        let mutation_barrier = self
            .mutation_barrier
            .lock()
            .expect("mutation barrier lock")
            .take();
        let (
            snapshot,
            authentication_bytes,
            authentication_block_equivalents,
            authentication_read_calls,
        ) = authenticated_snapshot_file(
            &file,
            fragment.identity,
            &fragment.entry,
            scratch,
            #[cfg(test)]
            mutation_barrier,
        )?;
        Ok(OpenPropertyFragment {
            file: Arc::new(PropertyFile::Plain(snapshot)),
            authentication_bytes,
            authentication_block_equivalents,
            authentication_read_calls,
            handle,
        })
    }

    /// Committed generation retained by this inventory, when generation-backed.
    #[must_use]
    pub fn generation_uuid(&self) -> Option<uuid::Uuid> {
        self.generation_lease
            .as_ref()
            .map(crate::ResolvedProjectGeneration::generation_uuid)
    }

    pub(crate) fn generation_authority(&self) -> Option<(uuid::Uuid, PathBuf)> {
        self.generation_lease.as_ref().map(|generation| {
            (
                generation.generation_uuid(),
                generation.container_root().to_path_buf(),
            )
        })
    }

    // Authenticated snapshots require the source volume. Keep their temporary
    // files outside immutable generations and outside enumerated graph trees.
    pub(crate) fn create_snapshot_scratch(&self) -> Result<tempfile::TempDir, GfError> {
        let parent = self
            .generation_lease
            .as_ref()
            .map(crate::ResolvedProjectGeneration::container_root)
            .or_else(|| self.root_path.as_deref().and_then(Path::parent))
            .ok_or_else(|| corrupt("property inventory lacks a project-volume scratch parent"))?;
        tempfile::Builder::new()
            .prefix(".gf-property-scratch-")
            .tempdir_in(parent)
            .map_err(io_error)
    }

    /// Narrow an already authenticated inventory to transaction-owned property
    /// fragments while retaining the complete graph and semantic-route admission.
    pub(crate) fn retain_property_fragment_paths(
        &mut self,
        paths: &std::collections::HashSet<PathBuf>,
    ) {
        let Some(root) = &self.root_path else {
            return;
        };
        self.routes.retain(|_, fragments| {
            fragments.retain(|fragment| paths.contains(&root.join(&fragment.physical_relative)));
            !fragments.is_empty()
        });
    }

    /// Return admitted immutable property fragments in oldest-to-newest order.
    /// The caller must retain this inventory while using its physical paths.
    #[must_use]
    pub fn property_fragments(
        &self,
        kind: PropertyRouteKind,
        route: &str,
    ) -> Vec<PropertyFragment> {
        let Some(root) = &self.root_path else {
            return Vec::new();
        };
        self.routes
            .get(&(kind, route.to_owned()))
            .into_iter()
            .flatten()
            .map(|fragment| PropertyFragment {
                id: fragment.id,
                path: root.join(&fragment.physical_relative),
            })
            .collect()
    }

    /// Physical objects owned by a route, including continuation objects.
    pub(crate) fn property_object_paths(
        &self,
        kind: PropertyRouteKind,
        route: &str,
    ) -> Vec<PathBuf> {
        let Some(root) = &self.root_path else {
            return Vec::new();
        };
        self.routes
            .get(&(kind, route.to_owned()))
            .into_iter()
            .flatten()
            .flat_map(|fragment| {
                fragment
                    .parts
                    .iter()
                    .map(|part| root.join(&part.physical_relative))
            })
            .collect()
    }

    /// Stream one logical fragment without exposing its physical envelope schema.
    pub(crate) fn open_property_fragment_batches(
        &self,
        kind: PropertyRouteKind,
        route: &str,
        id: PropertyFragmentId,
        scratch: &Path,
        admit_resources: bool,
    ) -> Result<(arrow::datatypes::SchemaRef, super::PropertyFragmentBatches), GfError> {
        let fragment = self
            .routes
            .get(&(kind, route.to_owned()))
            .and_then(|fragments| fragments.iter().find(|fragment| fragment.id == id))
            .ok_or_else(|| corrupt("property fragment is not in retained authority"))?;
        let opened = self.open_fragment(fragment, scratch)?;
        let source = super::CountingChunkReader {
            file: Arc::clone(&opened.file),
            length: fragment.logical_length,
            counts: super::ReadCounts::new(false),
        };
        // Portable import already enforced this storage policy. Other callers
        // retain their own resource contracts rather than gaining new refusals.
        let builder = if admit_resources {
            crate::catalog::admitted_parquet_source(source)
                .map_err(|error| GfError::Storage(error.to_string()))?
        } else {
            ParquetRecordBatchReaderBuilder::try_new(source).map_err(parquet_error)?
        };
        let schema = builder.schema().clone();
        let reader = builder
            .with_batch_size(4096)
            .build()
            .map_err(parquet_error)?;
        Ok((
            schema,
            super::PropertyFragmentBatches {
                reader,
                _opened: opened,
            },
        ))
    }

    /// Canonical property routes admitted into this immutable snapshot.
    pub fn routes(&self, kind: PropertyRouteKind) -> impl Iterator<Item = &str> {
        self.routes
            .keys()
            .filter(move |(candidate, _)| *candidate == kind)
            .map(|(_, route)| route.as_str())
    }

    // Discovery controls wildcard membership; retained authority supplies the exact
    // semantic spelling without rediscovering or decoding an unauthenticated table.
    pub(crate) fn discovered_routes(
        &self,
        kind: PropertyRouteKind,
        physical_stems: &[String],
    ) -> Vec<String> {
        let physical_stems: BTreeSet<&str> = physical_stems.iter().map(String::as_str).collect();
        self.routes
            .iter()
            .filter(|((candidate, _), fragments)| {
                *candidate == kind
                    && fragments.iter().any(|fragment| {
                        let component =
                            Path::new(&fragment.entry.relative_path).components().nth(1);
                        component
                            .and_then(|part| {
                                let path = std::path::Path::new(part.as_os_str());
                                match fragment.layout {
                                    PropertyFragmentLayout::LegacyFlat => path.file_stem(),
                                    PropertyFragmentLayout::CanonicalNested => path.file_name(),
                                }
                            })
                            .and_then(|name| name.to_str())
                            .is_some_and(|name| physical_stems.contains(name))
                    })
            })
            .map(|((_, route), _)| route.clone())
            .collect()
    }

    /// Exact one-time admission evidence for this cached inventory.
    #[must_use]
    pub fn open_metrics(&self) -> PropertyInventoryOpenMetrics {
        let property_authentication_bytes =
            self.routes.values().flatten().fold(0_u64, |sum, fragment| {
                sum.saturating_add(fragment.authentication_bytes)
            });
        let property_authentication_block_equivalents =
            self.routes.values().flatten().fold(0_u64, |sum, fragment| {
                sum.saturating_add(fragment.authentication_block_equivalents)
            });
        let property_authentication_read_calls =
            self.routes.values().flatten().fold(0_u64, |sum, fragment| {
                sum.saturating_add(fragment.authentication_read_calls)
            });
        PropertyInventoryOpenMetrics {
            authority_authentication_bytes: self.authority_bytes,
            authority_authentication_block_equivalents: self.authority_block_equivalents,
            authority_authentication_read_calls: self.authority_read_calls,
            property_authentication_bytes,
            property_authentication_block_equivalents,
            property_authentication_read_calls,
            authentication_bytes: self
                .authority_bytes
                .saturating_add(property_authentication_bytes),
            authentication_block_equivalents: self
                .authority_block_equivalents
                .saturating_add(property_authentication_block_equivalents),
            authentication_read_calls: self
                .authority_read_calls
                .saturating_add(property_authentication_read_calls),
        }
    }

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

    fn from_read_inventory_at_root(
        root: &Path,
        inventory: crate::GraphReadInventory,
        requested_route: Option<(PropertyRouteKind, &str)>,
    ) -> Result<Self, GfError> {
        let table = inventory.authenticate_routes(root)?;
        let retained_root = graphforge_filesystem::StableDirectory::open(root).map_err(io_error)?;
        let mut entries = Vec::new();
        let mut edge_routes = BTreeMap::<String, Vec<AdmittedEdgeFile>>::new();
        let mut declared_nodes = Vec::new();
        for entry in inventory.files {
            crate::graph_files::wire_relative_path(&entry.relative_path)?;
            let semantic = match table.as_ref() {
                Some(table) => table.semantic_relative_path(&entry.relative_path)?,
                None => entry.relative_path.clone(),
            };
            if !inventory_entry_reaches_route(entry.role, &semantic, requested_route)? {
                continue;
            }
            let relative = PathBuf::from(&entry.relative_path);
            let retained = open_retained_under(&retained_root, &relative)?;
            authenticate_inventory_file(&retained, &entry)?;
            if requested_route.is_none() && entry.relative_path.starts_with("topology/edges/") {
                if entry.role != crate::GraphFileRole::Topology {
                    return Err(corrupt("edge topology entry has the wrong role"));
                }
                let route = crate::route_component::route_position(&semantic)?
                    .ok_or_else(|| corrupt("edge topology entry lacks relation route"))?;
                edge_routes
                    .entry(route.to_owned())
                    .or_default()
                    .push(AdmittedEdgeFile {
                        path: root.join(&relative),
                        relative_path: entry.relative_path.clone(),
                    });
            }
            if requested_route.is_none() && is_node_topology_path(&entry.relative_path) {
                if entry.role != crate::GraphFileRole::Topology {
                    return Err(corrupt("node topology entry has the wrong role"));
                }
                declared_nodes.push(AdmittedEdgeFile {
                    path: root.join(&relative),
                    relative_path: entry.relative_path.clone(),
                });
            }
            entries.push((entry, relative));
        }
        validate_declared_node_files(&mut declared_nodes)?;
        let node_files = requested_route.is_none().then_some(declared_nodes);
        let mut admitted =
            Self::admit_read_entries(root, entries, requested_route, table.as_ref(), false, None)?;
        admitted.edge_routes = edge_routes;
        admitted.node_files = node_files;
        Ok(admitted)
    }

    pub(crate) fn from_inventory_at_root(
        root: &Path,
        inventory: crate::GraphFilesInventory,
        requested_route: Option<(PropertyRouteKind, &str)>,
    ) -> Result<Self, GfError> {
        Self::from_inventory_at_root_with_admission(root, inventory, requested_route, false, None)
    }

    fn from_inventory_at_root_with_admission(
        root: &Path,
        inventory: crate::GraphFilesInventory,
        requested_route: Option<(PropertyRouteKind, &str)>,
        admit_resources: bool,
        cancelled: Option<&std::sync::atomic::AtomicBool>,
    ) -> Result<Self, GfError> {
        check_import_cancelled(cancelled)?;
        let table = crate::route_component::authenticate_manifest_routes(
            inventory.format_version,
            &inventory.files,
            |entry| {
                use std::io::{Read, Seek};
                check_import_cancelled(cancelled)?;
                let mut retained =
                    crate::graph_files::resolve_v1_inventory_entry_retained(root, entry)?;
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
            root,
            &inventory.files,
            table.as_ref(),
            false,
            requested_route,
            cancelled,
        )?;
        let entries = resolve_versioned_property_entries_for_route(
            root,
            inventory.files,
            requested_route,
            table.as_ref(),
            cancelled,
        )?;
        let mut admitted = Self::admit_entries_with_admission(
            root,
            entries,
            requested_route,
            table.as_ref(),
            admit_resources,
            cancelled,
        )?;
        admitted.edge_routes = topology.edge_routes;
        admitted.node_files = topology.node_files;
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

    #[expect(
        clippy::too_many_lines,
        reason = "one constructor per participant version, each binding its own root and route authority"
    )]
    pub(super) fn from_resolved_generation_route(
        generation: &crate::ResolvedProjectGeneration,
        requested_route: Option<(PropertyRouteKind, &str)>,
    ) -> Result<Self, GfError> {
        let Some(participant) = generation.declared_graph_files_participant()? else {
            return Ok(Self {
                root: None,
                root_path: None,
                generation_lease: Some(generation.clone()),
                routes: BTreeMap::new(),
                edge_routes: BTreeMap::new(),
                node_files: requested_route.is_none().then(Vec::new),
                schemas: BTreeMap::new(),
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
                let mut admitted =
                    Self::admit_entries(&root, entries, requested_route, route_table.as_ref())?;
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
                let mut admitted =
                    Self::admit_entries(root, entries, requested_route, route_table.as_ref())?;
                admitted.edge_routes = topology.edge_routes;
                admitted.node_files = topology.node_files;
                Ok::<Self, GfError>(admitted)
            }
        }?;
        admitted.seed_semantic_property_schemas(generation)?;
        admitted.generation_lease = Some(generation.clone());
        Ok(admitted)
    }

    pub(crate) fn from_entries_at_root(
        root: &Path,
        entries: Vec<crate::GraphFileEntry>,
    ) -> Result<Self, GfError> {
        let node_files = admit_node_paths(root, &entries, false, None)?;
        let entries = entries
            .into_iter()
            .map(|entry| {
                let relative = PathBuf::from(&entry.relative_path);
                (entry, relative)
            })
            .collect();
        let mut admitted = Self::admit_entries(root, entries, None, None)?;
        admitted.node_files = Some(node_files);
        Ok(admitted)
    }

    #[cfg(test)]
    pub(super) fn from_entries_at_root_for_route(
        root: &Path,
        entries: Vec<crate::GraphFileEntry>,
        kind: PropertyRouteKind,
        route: &str,
    ) -> Result<Self, GfError> {
        let entries = entries
            .into_iter()
            .map(|entry| {
                let relative = PathBuf::from(&entry.relative_path);
                (entry, relative)
            })
            .collect();
        Self::admit_entries(root, entries, Some((kind, route)), None)
    }

    pub(super) fn admit_entries(
        root_path: &Path,
        entries: Vec<(crate::GraphFileEntry, PathBuf)>,
        requested_route: Option<(PropertyRouteKind, &str)>,
        route_table: Option<&crate::route_component::RouteTable>,
    ) -> Result<Self, GfError> {
        Self::admit_entries_with_admission(
            root_path,
            entries,
            requested_route,
            route_table,
            false,
            None,
        )
    }

    fn admit_entries_with_admission(
        root_path: &Path,
        entries: Vec<(crate::GraphFileEntry, PathBuf)>,
        requested_route: Option<(PropertyRouteKind, &str)>,
        route_table: Option<&crate::route_component::RouteTable>,
        admit_resources: bool,
        cancelled: Option<&std::sync::atomic::AtomicBool>,
    ) -> Result<Self, GfError> {
        let entries = entries
            .into_iter()
            .map(|(entry, path)| {
                (
                    crate::GraphReadFileEntry {
                        relative_path: entry.relative_path,
                        byte_length: entry.byte_length,
                        content_xxh64: entry.content_xxh64,
                        role: entry.role,
                    },
                    path,
                )
            })
            .collect();
        Self::admit_read_entries(
            root_path,
            entries,
            requested_route,
            route_table,
            admit_resources,
            cancelled,
        )
    }

    fn admit_read_entries(
        root_path: &Path,
        entries: Vec<(crate::GraphReadFileEntry, PathBuf)>,
        requested_route: Option<(PropertyRouteKind, &str)>,
        route_table: Option<&crate::route_component::RouteTable>,
        admit_resources: bool,
        cancelled: Option<&std::sync::atomic::AtomicBool>,
    ) -> Result<Self, GfError> {
        let root = graphforge_filesystem::StableDirectory::open(root_path).map_err(io_error)?;
        #[cfg(test)]
        let handle_counts = Arc::new(FragmentHandleCounts::default());
        let groups = group_property_entries(entries, requested_route, route_table)?;
        let admission_scratch = tempfile::Builder::new()
            .prefix(".gf-property-admission-")
            .tempdir_in(root_path.parent().unwrap_or(root_path))
            .map_err(io_error)?;
        let mut routes: BTreeMap<(PropertyRouteKind, String), Vec<AuthenticatedPropertyFragment>> =
            BTreeMap::new();
        let admission = PropertyAdmission {
            root: &root,
            scratch: admission_scratch.path(),
            admit_resources,
            cancelled,
            #[cfg(test)]
            handle_counts: &handle_counts,
        };
        for ((kind, route, id), group) in groups {
            let fragment = admission.admit_fragment(kind, &route, id, group)?;
            routes.entry((kind, route)).or_default().push(fragment);
        }
        let mut schemas = BTreeMap::new();
        for ((kind, route), fragments) in &mut routes {
            fragments.sort_unstable_by_key(|fragment| fragment.id);
            validate_fragment_id_sequence(fragments.iter().map(|fragment| fragment.id))?;
            let summary_inputs = fragments
                .iter()
                .map(|fragment| (fragment.schema.as_ref(), fragment.physical_rows))
                .collect::<Vec<_>>();
            validate_live_schema_sequence(&summary_inputs)?;
            for fragment in fragments {
                merge_route_schema(&mut schemas, *kind, route, fragment.schema.as_ref())?;
            }
        }
        let schemas = finalize_route_schemas(schemas, &routes)?;
        Ok(Self {
            generation_lease: None,
            root: Some(root),
            root_path: Some(root_path.to_path_buf()),
            routes,
            edge_routes: BTreeMap::new(),
            node_files: None,
            schemas,
            authority_bytes: 0,
            authority_block_equivalents: 0,
            authority_read_calls: 0,
            #[cfg(test)]
            handle_counts,
            #[cfg(test)]
            late_decoder_failure_row_countdown: Arc::new(AtomicU64::new(0)),
            #[cfg(test)]
            mutation_barrier: Mutex::new(None),
        })
    }

    /// Admit the writable workspace's current property rows and statistics,
    /// retaining declared semantic owners from the transaction's pinned generation.
    /// The generation's historical live counts are not workspace statistics.
    pub fn capture_workspace(project: &Path, pinned: Option<&Self>) -> Result<Self, GfError> {
        let mut inventory = Self::capture(project)?;
        if let Some(generation) = pinned.and_then(|pinned| pinned.generation_lease.as_ref()) {
            inventory.seed_semantic_property_schemas(generation)?;
        }
        Ok(inventory)
    }

    /// Refresh property bytes while retaining explicit topology membership.
    pub fn capture_workspace_with_topology(
        project: &Path,
        pinned: Option<&Self>,
        topology: &crate::TopologyFiles,
    ) -> Result<Self, GfError> {
        let captured = crate::capture_graph_read_inventory_with_topology(project, topology)?;
        let mut inventory = Self::from_read_inventory_at_root(project, captured, None)?;
        if let Some(generation) = pinned.and_then(|pinned| pinned.generation_lease.as_ref()) {
            inventory.seed_semantic_property_schemas(generation)?;
        }
        Ok(inventory)
    }

    // A declared owner may not have a property payload yet. Its authenticated
    // binding still owns the metadata of the first fragment we publish.
    fn seed_semantic_property_schemas(
        &mut self,
        generation: &crate::ResolvedProjectGeneration,
    ) -> Result<(), GfError> {
        let Some(bindings) = crate::semantic_storage_bindings(generation)? else {
            return Ok(());
        };
        for binding in &bindings.bindings {
            let kind = match binding.route_kind {
                crate::SemanticRouteKind::NodeProperty => PropertyRouteKind::Node,
                crate::SemanticRouteKind::EdgeProperty => PropertyRouteKind::Edge,
                _ => continue,
            };
            self.schemas
                .entry((kind, binding.route.clone()))
                .or_insert_with(|| {
                    let schema = arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
                        kind.uuid_field(),
                        arrow::datatypes::DataType::FixedSizeBinary(16),
                        false,
                    )]);
                    Arc::new(crate::schemas::with_semantic_route_metadata(
                        &schema,
                        &binding.route,
                        &bindings.composition_fingerprint,
                    ))
                });
        }
        Ok(())
    }

    /// Canonical logical schema authenticated across every fragment in a route.
    #[must_use]
    pub fn route_schema(
        &self,
        kind: PropertyRouteKind,
        route: &str,
    ) -> Option<arrow::datatypes::SchemaRef> {
        self.schemas.get(&(kind, route.to_owned())).cloned()
    }

    /// Sound upper bound on logical rows for one route.
    ///
    /// The newest-wins merge and tombstones can only remove physical fragment
    /// rows, so their admitted footer counts are a safe planning estimate. It
    /// is deliberately not advertised as an exact logical count.
    #[must_use]
    pub fn route_row_upper_bound(&self, kind: PropertyRouteKind, route: &str) -> usize {
        self.routes
            .get(&(kind, route.to_owned()))
            .map_or(0, |fragments| {
                fragments.iter().fold(0usize, |rows, fragment| {
                    rows.saturating_add(fragment.physical_rows)
                })
            })
    }
}

#[derive(Debug)]
struct RouteSchemaBuilder {
    uuid: arrow::datatypes::FieldRef,
    fields: BTreeMap<String, arrow::datatypes::FieldRef>,
    metadata: HashMap<String, String>,
}

fn apply_authenticated_live_schema(
    schema: &mut RouteSchemaBuilder,
    latest: &arrow::datatypes::Schema,
) -> Result<(), GfError> {
    let Some(summary) = decode_live_schema_summary(latest)? else {
        return Ok(());
    };
    for name in summary.counts.keys() {
        if !schema.fields.contains_key(name) {
            return Err(corrupt(
                "property live schema names an absent physical field",
            ));
        }
    }
    for (name, field) in &mut schema.fields {
        if !summary.counts.contains_key(name) {
            *field = Arc::new(
                arrow::datatypes::Field::new(name, arrow::datatypes::DataType::Null, true)
                    .with_metadata(field.metadata().clone()),
            );
        }
    }
    schema.metadata.insert(
        PROPERTY_LIVE_SCHEMA_KEY.to_owned(),
        encode_live_schema_summary(summary.counts)?,
    );
    Ok(())
}

fn validate_live_schema_sequence(
    fragments: &[(&arrow::datatypes::Schema, usize)],
) -> Result<(), GfError> {
    let physical_rows = fragments.iter().try_fold(0_u64, |total, (_, rows)| {
        total
            .checked_add(
                u64::try_from(*rows)
                    .map_err(|_| corrupt("property route row count is not representable"))?,
            )
            .ok_or_else(|| corrupt("property route row count overflows"))
    })?;
    let mut summary_started = false;
    for (schema, _) in fragments {
        match decode_live_schema_summary(schema)? {
            Some(summary) => {
                summary_started = true;
                if summary.counts.values().any(|count| *count > physical_rows) {
                    return Err(corrupt(
                        "property live schema count exceeds physical row bound",
                    ));
                }
            }
            None if summary_started => {
                return Err(corrupt("property live schema authority regresses"));
            }
            None => {}
        }
    }
    Ok(())
}

fn merge_route_schema(
    schemas: &mut BTreeMap<(PropertyRouteKind, String), RouteSchemaBuilder>,
    kind: PropertyRouteKind,
    route: &str,
    fragment: &arrow::datatypes::Schema,
) -> Result<(), GfError> {
    const IDENTITY_KEYS: [&str; 6] = [
        PROPERTY_OVERLAY_FORMAT_KEY,
        PROPERTY_ROUTE_KEY,
        PROPERTY_KIND_KEY,
        PROPERTY_GENERATION_KEY,
        PROPERTY_ORDINAL_KEY,
        PROPERTY_LIVE_SCHEMA_KEY,
    ];
    let uuid = Arc::clone(&fragment.fields()[0]);
    let schema = schemas
        .entry((kind, route.to_owned()))
        .or_insert_with(|| RouteSchemaBuilder {
            uuid: Arc::clone(&uuid),
            fields: BTreeMap::new(),
            metadata: HashMap::new(),
        });
    if schema.uuid.as_ref() != uuid.as_ref() {
        return Err(corrupt("property route UUID schemas conflict"));
    }
    for (name, value) in fragment.metadata() {
        if IDENTITY_KEYS.contains(&name.as_str()) {
            continue;
        }
        if schema
            .metadata
            .insert(name.clone(), value.clone())
            .is_some_and(|prior| prior != *value)
        {
            return Err(corrupt("property route semantic metadata conflicts"));
        }
    }
    for field in fragment.fields().iter().skip(1) {
        if field.name() == PROPERTY_TOMBSTONE_FIELD {
            continue;
        }
        if let Some(prior) = schema.fields.get(field.name()) {
            if prior.as_ref() != field.as_ref() {
                if prior.data_type() == &arrow::datatypes::DataType::Null {
                    schema.fields.insert(
                        field.name().clone(),
                        Arc::new(field.as_ref().clone().with_nullable(true)),
                    );
                    continue;
                }
                if field.data_type() == &arrow::datatypes::DataType::Null {
                    continue;
                }
                if prior.name() == field.name()
                    && prior.data_type() == field.data_type()
                    && prior.metadata() == field.metadata()
                {
                    schema.fields.insert(
                        field.name().clone(),
                        Arc::new(
                            arrow::datatypes::Field::new(
                                field.name(),
                                field.data_type().clone(),
                                prior.is_nullable() || field.is_nullable(),
                            )
                            .with_metadata(prior.metadata().clone()),
                        ),
                    );
                    continue;
                }
                if prior.name() != field.name()
                    || prior.metadata() != field.metadata()
                    || !is_compatible_scalar(prior.data_type())
                    || !is_compatible_scalar(field.data_type())
                {
                    return Err(corrupt(
                        "property route field type or semantic metadata conflicts",
                    ));
                }
                schema.fields.insert(
                    field.name().clone(),
                    Arc::new(
                        arrow::datatypes::Field::new(
                            field.name(),
                            arrow::datatypes::DataType::Struct(
                                crate::writer::heterogeneous_scalar_fields(),
                            ),
                            prior.is_nullable() || field.is_nullable(),
                        )
                        .with_metadata(prior.metadata().clone()),
                    ),
                );
            }
        } else {
            schema
                .fields
                .insert(field.name().clone(), Arc::clone(field));
        }
    }
    Ok(())
}

fn is_compatible_scalar(data_type: &arrow::datatypes::DataType) -> bool {
    matches!(
        data_type,
        arrow::datatypes::DataType::Int64
            | arrow::datatypes::DataType::Float64
            | arrow::datatypes::DataType::Boolean
            | arrow::datatypes::DataType::Utf8
    ) || data_type
        == &arrow::datatypes::DataType::Struct(crate::writer::heterogeneous_scalar_fields())
}

pub(crate) fn merge_property_route_schemas<'a>(
    kind: PropertyRouteKind,
    route: &str,
    fragments: impl IntoIterator<Item = &'a arrow::datatypes::Schema>,
) -> Result<arrow::datatypes::SchemaRef, GfError> {
    let mut schemas = BTreeMap::new();
    for fragment in fragments {
        merge_route_schema(&mut schemas, kind, route, fragment)?;
    }
    let schema = schemas
        .remove(&(kind, route.to_owned()))
        .ok_or_else(|| corrupt("property route has no schema authority"))?;
    let mut fields = vec![schema.uuid];
    fields.extend(schema.fields.into_values());
    Ok(Arc::new(arrow::datatypes::Schema::new_with_metadata(
        fields,
        schema.metadata,
    )))
}

fn parse_inventory_property_path(
    relative: &str,
) -> Result<
    Option<(
        PropertyRouteKind,
        String,
        PropertyFragmentId,
        PropertyFragmentLayout,
    )>,
    GfError,
> {
    let (anchor, _) = canonical_part_anchor(relative)?;
    let parts = anchor.split('/').collect::<Vec<_>>();
    let kind = match parts.first().copied() {
        Some("properties") => PropertyRouteKind::Node,
        Some("edge_properties") => PropertyRouteKind::Edge,
        _ => return Ok(None),
    };
    match parts.as_slice() {
        [_, legacy] => {
            let route = legacy
                .strip_suffix(".parquet")
                .filter(|route| !route.is_empty())
                .ok_or_else(|| corrupt("legacy property inventory path is malformed"))?;
            Ok(Some((
                kind,
                route.to_owned(),
                PropertyFragmentId {
                    generation: 0,
                    ordinal: 0,
                },
                PropertyFragmentLayout::LegacyFlat,
            )))
        }
        [_, route, name] if !route.is_empty() => Ok(Some((
            kind,
            (*route).to_owned(),
            PropertyFragmentId::parse(name)?,
            PropertyFragmentLayout::CanonicalNested,
        ))),
        _ => Err(corrupt("property inventory path is not canonical")),
    }
}

fn inventory_entry_reaches_route(
    role: crate::GraphFileRole,
    canonical_relative: &str,
    requested_route: Option<(PropertyRouteKind, &str)>,
) -> Result<bool, GfError> {
    let parsed = parse_inventory_property_path(canonical_relative)?;
    if role != crate::GraphFileRole::Properties {
        if parsed.is_some() {
            return Err(corrupt("property inventory entry has the wrong role"));
        }
        // Preserve full-inventory admission: only the explicitly targeted
        // route path may avoid resolving unrelated graph payloads.
        return Ok(requested_route.is_none());
    }
    let Some((kind, route, _, _)) = parsed else {
        return Err(corrupt("properties role names a non-property path"));
    };
    Ok(requested_route.is_none_or(|requested| (kind, route.as_str()) == requested))
}

/// Apply each route's latest authenticated live schema and freeze the result.
fn finalize_route_schemas(
    schemas: BTreeMap<(PropertyRouteKind, String), RouteSchemaBuilder>,
    routes: &BTreeMap<(PropertyRouteKind, String), Vec<AuthenticatedPropertyFragment>>,
) -> Result<BTreeMap<(PropertyRouteKind, String), arrow::datatypes::SchemaRef>, GfError> {
    schemas
        .into_iter()
        .map(|(key, mut schema)| {
            if let Some(latest) = routes.get(&key).and_then(|fragments| fragments.last()) {
                apply_authenticated_live_schema(&mut schema, latest.schema.as_ref())?;
            }
            let mut fields = vec![schema.uuid];
            fields.extend(schema.fields.into_values());
            Ok((
                key,
                Arc::new(arrow::datatypes::Schema::new_with_metadata(
                    fields,
                    schema.metadata,
                )),
            ))
        })
        .collect()
}

/// The topology files an inventory declares: edge routes and node files.
struct TopologyAuthority {
    edge_routes: BTreeMap<String, Vec<AdmittedEdgeFile>>,
    node_files: Option<Vec<AdmittedEdgeFile>>,
}

/// Edge routes and declared node files of an inventory, or none of either for
/// a route-scoped request, which carries no topology authority.
fn topology_authority(
    root: &Path,
    entries: &[crate::GraphFileEntry],
    table: Option<&crate::route_component::RouteTable>,
    cas: bool,
    requested_route: Option<(PropertyRouteKind, &str)>,
    cancelled: Option<&std::sync::atomic::AtomicBool>,
) -> Result<TopologyAuthority, GfError> {
    if requested_route.is_some() {
        return Ok(TopologyAuthority {
            edge_routes: BTreeMap::new(),
            node_files: None,
        });
    }
    Ok(TopologyAuthority {
        edge_routes: admit_edge_route_paths(root, entries, table, cas, cancelled)?,
        node_files: Some(admit_node_paths(root, entries, cas, cancelled)?),
    })
}

/// The legacy flat node file or a `.parquet` directly beneath `topology/nodes/`.
/// Other names there (a rewrite's staged temporaries, for one) are ignored,
/// as `mutator::node_parquet_files` ignores them when it lists the directory.
fn is_node_topology_path(relative: &str) -> bool {
    relative == "topology/nodes.parquet"
        || relative
            .strip_prefix("topology/nodes/")
            .is_some_and(|name| !name.contains('/') && name.ends_with(".parquet"))
}

/// The declared node set must satisfy what `mutator::node_parquet_files`
/// requires of a directory listing: the legacy flat file first, then
/// `topology/nodes/<first>-<last>.parquet` shards with canonical padded
/// ranges that do not overlap. A shard that fails is refused, never read.
fn validate_declared_node_files(files: &mut [AdmittedEdgeFile]) -> Result<(), GfError> {
    files.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
    let mut prior_end = None;
    for file in files.iter() {
        if file.relative_path == "topology/nodes.parquet" {
            continue;
        }
        let relative = Path::new(&file.relative_path);
        let (first, last) = crate::mutator::canonical_topology_shard_range(relative, "node")?;
        if prior_end.is_some_and(|end| first <= end) {
            return Err(corrupt(&format!(
                "declared node shard ranges overlap at {}",
                file.relative_path
            )));
        }
        prior_end = Some(last);
    }
    Ok(())
}

/// The node topology files an inventory declares, resolved like edge routes:
/// the CAS object for a compact root, the tree file for an expanded one.
fn admit_node_paths(
    root: &Path,
    entries: &[crate::GraphFileEntry],
    cas: bool,
    cancelled: Option<&std::sync::atomic::AtomicBool>,
) -> Result<Vec<AdmittedEdgeFile>, GfError> {
    let mut files = Vec::new();
    for entry in entries {
        check_import_cancelled(cancelled)?;
        if !is_node_topology_path(&entry.relative_path) {
            continue;
        }
        if entry.role != crate::GraphFileRole::Topology {
            return Err(corrupt("node topology entry has the wrong role"));
        }
        let path = if cas {
            crate::graph_object_path(root, &entry.content_sha256)?
        } else {
            crate::graph_files::resolve_v1_inventory_entry(root, entry)?
        };
        files.push(AdmittedEdgeFile {
            path,
            relative_path: entry.relative_path.clone(),
        });
    }
    validate_declared_node_files(&mut files)?;
    Ok(files)
}

fn admit_edge_route_paths(
    root: &Path,
    entries: &[crate::GraphFileEntry],
    table: Option<&crate::route_component::RouteTable>,
    cas: bool,
    cancelled: Option<&std::sync::atomic::AtomicBool>,
) -> Result<BTreeMap<String, Vec<AdmittedEdgeFile>>, GfError> {
    let mut routes = BTreeMap::<String, Vec<AdmittedEdgeFile>>::new();
    for entry in entries {
        check_import_cancelled(cancelled)?;
        if !entry.relative_path.starts_with("topology/edges/") {
            continue;
        }
        if entry.role != crate::GraphFileRole::Topology {
            return Err(corrupt("edge topology entry has the wrong role"));
        }
        let semantic = match table {
            Some(table) => table.semantic_relative_path(&entry.relative_path)?,
            None => crate::graph_files::legacy_inventory_logical_text(&entry.relative_path)?,
        };
        let route = crate::route_component::route_position(&semantic)?
            .ok_or_else(|| corrupt("edge topology entry lacks relation route"))?;
        let path = if cas {
            crate::graph_object_path(root, &entry.content_sha256)?
        } else {
            crate::graph_files::resolve_v1_inventory_entry(root, entry)?
        };
        routes
            .entry(route.to_owned())
            .or_default()
            .push(AdmittedEdgeFile {
                path,
                relative_path: entry.relative_path.clone(),
            });
    }
    Ok(routes)
}

#[cfg(test)]
pub(super) fn resolve_v1_property_entries_for_route(
    root: &Path,
    entries: Vec<crate::GraphFileEntry>,
    requested_route: Option<(PropertyRouteKind, &str)>,
) -> Result<Vec<(crate::GraphFileEntry, PathBuf)>, GfError> {
    resolve_versioned_property_entries_for_route(root, entries, requested_route, None, None)
}

fn resolve_versioned_property_entries_for_route(
    root: &Path,
    entries: Vec<crate::GraphFileEntry>,
    requested_route: Option<(PropertyRouteKind, &str)>,
    route_table: Option<&crate::route_component::RouteTable>,
    cancelled: Option<&std::sync::atomic::AtomicBool>,
) -> Result<Vec<(crate::GraphFileEntry, PathBuf)>, GfError> {
    let mut selected = Vec::new();
    for mut entry in entries {
        check_import_cancelled(cancelled)?;
        let canonical = match route_table {
            Some(_) => {
                crate::graph_files::wire_relative_path(&entry.relative_path)?;
                entry.relative_path.clone()
            }
            None => crate::graph_files::legacy_inventory_logical_text(&entry.relative_path)?,
        };
        let semantic = match route_table {
            Some(table) => table.semantic_relative_path(&canonical)?,
            None => canonical.clone(),
        };
        if !inventory_entry_reaches_route(entry.role, &semantic, requested_route)? {
            continue;
        }
        // Resolve with the original spelling so a legacy backslash-authored
        // inventory can still authenticate its one unambiguous physical path.
        // Logical classification above uses the portable canonical spelling.
        let path = crate::graph_files::resolve_v1_inventory_entry(root, &entry)?;
        let relative = path
            .strip_prefix(root)
            .map_err(|_| corrupt("legacy graph file escaped its root"))?
            .to_path_buf();
        entry.relative_path = canonical;
        selected.push((entry, relative));
    }
    Ok(selected)
}

type PropertyPartEntry = (
    u64,
    PropertyFragmentLayout,
    crate::GraphReadFileEntry,
    PathBuf,
);
type PropertyEntryGroups =
    BTreeMap<(PropertyRouteKind, String, PropertyFragmentId), Vec<PropertyPartEntry>>;

fn group_property_entries(
    entries: Vec<(crate::GraphReadFileEntry, PathBuf)>,
    requested_route: Option<(PropertyRouteKind, &str)>,
    route_table: Option<&crate::route_component::RouteTable>,
) -> Result<PropertyEntryGroups, GfError> {
    let mut groups = PropertyEntryGroups::new();
    for (entry, physical_relative) in entries {
        let semantic_path = match route_table {
            Some(table) => table.semantic_relative_path(&entry.relative_path)?,
            None => entry.relative_path.clone(),
        };
        let (anchor, index) = canonical_part_anchor(&semantic_path)?;
        let parsed = parse_inventory_property_path(&anchor)?;
        if entry.role != crate::GraphFileRole::Properties {
            if parsed.is_some() {
                return Err(corrupt("property inventory entry has the wrong role"));
            }
            continue;
        }
        let Some((kind, route, id, layout)) = parsed else {
            return Err(corrupt("properties role names a non-property path"));
        };
        if requested_route.is_some_and(|requested| (kind, route.as_str()) != requested) {
            continue;
        }
        groups.entry((kind, route, id)).or_default().push((
            index,
            layout,
            entry,
            physical_relative,
        ));
    }
    Ok(groups)
}

struct PropertyAdmission<'a> {
    root: &'a graphforge_filesystem::StableDirectory,
    scratch: &'a Path,
    admit_resources: bool,
    cancelled: Option<&'a std::sync::atomic::AtomicBool>,
    #[cfg(test)]
    handle_counts: &'a Arc<FragmentHandleCounts>,
}

fn check_import_cancelled(
    cancelled: Option<&std::sync::atomic::AtomicBool>,
) -> Result<(), GfError> {
    if cancelled.is_some_and(|flag| flag.load(Ordering::Relaxed)) {
        Err(GfError::Api {
            code: graphforge_core::ApiErrorCode::Cancelled,
            message: "verification cancelled".to_owned(),
        })
    } else {
        Ok(())
    }
}

struct AdmittedPropertyParts {
    layout: PropertyFragmentLayout,
    parts: Vec<PropertyObjectPart>,
    envelope: Option<super::bounded_object::EnvelopeLayout>,
    plain_source: Option<File>,
    authentication_bytes: u64,
    authentication_block_equivalents: u64,
    authentication_read_calls: u64,
}

impl PropertyAdmission<'_> {
    fn admit_parts(
        &self,
        mut group: Vec<PropertyPartEntry>,
    ) -> Result<AdmittedPropertyParts, GfError> {
        group.sort_unstable_by_key(|part| part.0);
        if group.first().is_none_or(|part| part.0 != 0)
            || group.windows(2).any(|pair| pair[0].0 == pair[1].0)
        {
            return Err(corrupt("property fragment anchor missing or duplicated"));
        }
        let layout = group[0].1;
        let indices = group.iter().map(|part| part.0).collect::<Vec<_>>();
        let mut parts = Vec::new();
        let mut plain_source = None;
        let mut envelope = None;
        let mut authentication_bytes = 0;
        let mut authentication_block_equivalents = 0;
        let mut authentication_read_calls = 0;
        for (index, _, entry, physical_relative) in group {
            check_import_cancelled(self.cancelled)?;
            let file = open_retained_under(self.root, &physical_relative)?;
            let _handle = FragmentHandleGuard::acquired(
                #[cfg(test)]
                self.handle_counts,
            );
            let identity = graphforge_filesystem::file_identity(&file).map_err(io_error)?;
            let (snapshot, bytes, blocks, calls) = authenticated_snapshot_file(
                &file,
                identity,
                &entry,
                self.scratch,
                #[cfg(test)]
                None,
            )?;
            crate::lifecycle_io::record_read(
                crate::StorageIoPhase::HydrationVerification,
                bytes,
                calls,
            );
            crate::lifecycle_io::record_blocks(
                crate::StorageIoPhase::HydrationVerification,
                blocks,
            );
            authentication_bytes += bytes;
            authentication_block_equivalents += blocks;
            authentication_read_calls += calls;
            if self.admit_resources {
                crate::catalog::admitted_parquet_source(snapshot.try_clone().map_err(io_error)?)
                    .map_err(|error| GfError::Storage(error.to_string()))?;
            }
            let inspected =
                super::bounded_object::inspect_envelope(snapshot.try_clone().map_err(io_error)?)?;
            if index == 0 && inspected.is_none() {
                plain_source = Some(snapshot);
            }
            if index == 0 {
                envelope = inspected.map(|(layout, _)| layout);
                if inspected.is_some_and(|(_, actual)| actual != 0) {
                    return Err(corrupt("property anchor has wrong part index"));
                }
            } else if inspected != envelope.map(|layout| (layout, index)) || envelope.is_none() {
                return Err(corrupt("property fragment parts have conflicting layouts"));
            }
            parts.push(PropertyObjectPart {
                entry,
                physical_relative,
                identity,
            });
        }
        if let Some(envelope) = envelope {
            super::bounded_object::validate_parts(envelope, &indices)?;
        } else if parts.len() != 1 {
            return Err(corrupt("plain property fragment has unexpected parts"));
        }
        Ok(AdmittedPropertyParts {
            layout,
            parts,
            envelope,
            plain_source,
            authentication_bytes,
            authentication_block_equivalents,
            authentication_read_calls,
        })
    }

    fn admit_fragment(
        &self,
        kind: PropertyRouteKind,
        route: &str,
        id: PropertyFragmentId,
        group: Vec<PropertyPartEntry>,
    ) -> Result<AuthenticatedPropertyFragment, GfError> {
        check_import_cancelled(self.cancelled)?;
        let AdmittedPropertyParts {
            layout,
            parts,
            envelope,
            plain_source,
            mut authentication_bytes,
            mut authentication_block_equivalents,
            mut authentication_read_calls,
        } = self.admit_parts(group)?;
        let anchor = &parts[0];
        let logical_length =
            envelope.map_or(anchor.entry.byte_length, |layout| layout.logical_length);
        let source = if let Some(layout) = envelope {
            segmented_file(
                self.root,
                &parts,
                layout,
                self.scratch,
                #[cfg(test)]
                None,
            )?
        } else {
            PropertyFile::Plain(plain_source.expect("plain anchor snapshot"))
        };
        let source = Arc::new(source);
        let reader = super::CountingChunkReader {
            file: Arc::clone(&source),
            length: logical_length,
            counts: super::ReadCounts::new(false),
        };
        let builder = if self.admit_resources {
            crate::catalog::admitted_parquet_source(reader)
                .map_err(|error| GfError::Storage(error.to_string()))?
        } else {
            ParquetRecordBatchReaderBuilder::try_new(reader).map_err(parquet_error)?
        };
        let physical_rows = usize::try_from(builder.metadata().file_metadata().num_rows())
            .map_err(|_| corrupt("property fragment row count is not representable"))?;
        validate_fragment_schema(builder.schema().as_ref(), id, layout, kind, route)?;
        let schema = builder.schema().clone();
        let (bytes, blocks, calls) = source.authentication();
        let (read_bytes, read_calls) = source.physical_reads();
        crate::lifecycle_io::record_read(
            crate::StorageIoPhase::HydrationVerification,
            bytes.saturating_add(read_bytes),
            calls.saturating_add(read_calls),
        );
        crate::lifecycle_io::record_blocks(crate::StorageIoPhase::HydrationVerification, blocks);
        authentication_bytes += bytes;
        authentication_block_equivalents += blocks;
        authentication_read_calls += calls;
        Ok(AuthenticatedPropertyFragment {
            id,
            layout,
            entry: anchor.entry.clone(),
            physical_relative: anchor.physical_relative.clone(),
            identity: anchor.identity,
            parts,
            envelope,
            logical_length,
            physical_rows,
            schema,
            authentication_bytes,
            authentication_block_equivalents,
            authentication_read_calls,
        })
    }
}

fn canonical_part_anchor(relative: &str) -> Result<(String, u64), GfError> {
    let components = relative.split('/').collect::<Vec<_>>();
    if matches!(
        components.as_slice(),
        ["properties" | "edge_properties", _, _]
    ) && let Some((anchor, index)) = super::bounded_object::split_part_path(Path::new(relative))
    {
        let name = anchor
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| corrupt("property anchor is not UTF-8"))?;
        PropertyFragmentId::parse(name)?;
        return Ok((
            anchor
                .to_str()
                .ok_or_else(|| corrupt("property anchor is not UTF-8"))?
                .to_owned(),
            index,
        ));
    }
    Ok((relative.to_owned(), 0))
}

fn segmented_file(
    root: &graphforge_filesystem::StableDirectory,
    parts: &[PropertyObjectPart],
    layout: super::bounded_object::EnvelopeLayout,
    scratch: &Path,
    #[cfg(test)] mutation_barrier: Option<Arc<TestMutationBarrier>>,
) -> Result<PropertyFile, GfError> {
    let root = root.try_clone().map_err(io_error)?;
    let parts = parts.to_vec();
    let scratch = scratch.to_path_buf();
    let authentication = Arc::new(PartAuthentication::default());
    let counts = Arc::clone(&authentication);
    #[cfg(test)]
    let mutation_barrier = Mutex::new(mutation_barrier);
    let source = super::bounded_object::SegmentedSource::new(
        layout,
        Arc::new(move |index| {
            let part = parts
                .get(usize::try_from(index).map_err(|_| corrupt("property part index overflows"))?)
                .ok_or_else(|| corrupt("property part missing from authority"))?;
            if part.entry.byte_length > super::bounded_object::MAX_PROPERTY_OBJECT_BYTES as u64 {
                return Err(corrupt("property object exceeds physical cap"));
            }
            let file = open_retained_under(&root, &part.physical_relative)?;
            if graphforge_filesystem::file_identity(&file).map_err(io_error)? != part.identity {
                return Err(corrupt(
                    "property fragment identity changed after admission",
                ));
            }
            let (mut snapshot, bytes, blocks, calls) = authenticated_snapshot_file(
                &file,
                part.identity,
                &part.entry,
                &scratch,
                #[cfg(test)]
                mutation_barrier
                    .lock()
                    .expect("mutation barrier lock")
                    .take(),
            )?;
            counts.add(bytes, blocks, calls);
            let mut data = Vec::with_capacity(
                usize::try_from(part.entry.byte_length)
                    .map_err(|_| corrupt("property part length overflows"))?,
            );
            let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
            loop {
                let read = snapshot.read(&mut buffer).map_err(io_error)?;
                if read == 0 {
                    break;
                }
                counts.read_bytes.fetch_add(read as u64, Ordering::Relaxed);
                counts.read_calls.fetch_add(1, Ordering::Relaxed);
                data.extend_from_slice(&buffer[..read]);
            }
            Ok(Bytes::from(data))
        }),
    )?;
    Ok(PropertyFile::Segmented {
        source,
        authentication,
    })
}

fn open_retained_under(
    root: &graphforge_filesystem::StableDirectory,
    relative: &Path,
) -> Result<File, GfError> {
    let mut components = relative.components().peekable();
    let mut retained = Vec::new();
    while let Some(component) = components.next() {
        let std::path::Component::Normal(name) = component else {
            return Err(corrupt("property inventory path is not contained"));
        };
        let directory = retained.last().unwrap_or(root);
        if components.peek().is_none() {
            return directory.open_child_file(name).map_err(io_error);
        }
        let child = directory.open_child_directory(name).map_err(io_error)?;
        retained.push(child);
    }
    Err(corrupt("property inventory path is empty"))
}

fn authenticate_inventory_file(
    file: &File,
    entry: &crate::GraphReadFileEntry,
) -> Result<(u64, u64, u64), GfError> {
    let metadata = file.metadata().map_err(io_error)?;
    if !metadata.is_file() || metadata.len() != entry.byte_length {
        return Err(corrupt(
            "property handle length or kind conflicts with inventory",
        ));
    }
    let mut digest = crate::corruption_checksum::Checksum::new();
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
    let mut bytes = 0_u64;
    let mut read_calls = 0_u64;
    loop {
        let read = retained_read_at(file, &mut buffer, bytes).map_err(io_error)?;
        if read == 0 {
            break;
        }
        bytes = bytes
            .checked_add(u64::try_from(read).map_err(|_| corrupt("authentication byte overflow"))?)
            .ok_or_else(|| corrupt("authentication byte overflow"))?;
        read_calls = read_calls
            .checked_add(1)
            .ok_or_else(|| corrupt("authentication read call overflow"))?;
        digest.update(&buffer[..read]);
    }
    if bytes != entry.byte_length || digest.finish() != entry.content_xxh64 {
        return Err(corrupt("property handle digest conflicts with inventory"));
    }
    // #1449: these reads were computed and counted here but never reached the
    // lifecycle phase counters, so property-bearing opens under-reported the
    // verification row. Default to the hydration/verification row; a caller
    // whose phase differs overrides with a `PhaseScope`.
    crate::lifecycle_io::record_read(
        crate::StorageIoPhase::HydrationVerification,
        bytes,
        read_calls,
    );
    crate::lifecycle_io::record_blocks(
        crate::StorageIoPhase::HydrationVerification,
        bytes.div_ceil(64 * 1024),
    );
    Ok((bytes, bytes.div_ceil(64 * 1024), read_calls))
}

fn authenticated_snapshot_file(
    source: &File,
    expected_identity: graphforge_filesystem::FileIdentity,
    entry: &crate::GraphReadFileEntry,
    scratch: &Path,
    #[cfg(test)] mutation_barrier: Option<Arc<TestMutationBarrier>>,
) -> Result<(File, u64, u64, u64), GfError> {
    let metadata = source.metadata().map_err(io_error)?;
    if !metadata.is_file() || metadata.len() != entry.byte_length {
        return Err(corrupt(
            "property handle length or kind conflicts with inventory",
        ));
    }
    if graphforge_filesystem::file_identity(source).map_err(io_error)? != expected_identity {
        return Err(corrupt(
            "property fragment identity changed during snapshot",
        ));
    }
    fs::create_dir_all(scratch).map_err(io_error)?;
    let scratch_capability =
        graphforge_filesystem::StableDirectory::open(scratch).map_err(io_error)?;
    let snapshot_available_bytes = fs4::available_space(scratch).map_err(io_error)?;
    if snapshot_available_bytes < entry.byte_length {
        return Err(GfError::Storage(format!(
            "property snapshot scratch capacity is insufficient: available={snapshot_available_bytes} required={}",
            entry.byte_length
        )));
    }
    // Random exclusive creation plus immediate unlink makes planted names,
    // symlinks, and FIFOs unable to redirect the authenticated snapshot.
    let named = tempfile::Builder::new()
        .prefix(".gf-property-snapshot-")
        .tempfile_in(scratch)
        .map_err(io_error)?;
    scratch_capability.revalidate_named().map_err(io_error)?;
    let mut snapshot = named.into_file();
    if graphforge_filesystem::file_identity(&snapshot)
        .map_err(io_error)?
        .volume_serial
        != expected_identity.volume_serial
    {
        return Err(corrupt(
            "property snapshot scratch is not on the authenticated project volume",
        ));
    }
    let mut digest = crate::corruption_checksum::Checksum::new();
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
    let mut bytes = 0_u64;
    let mut read_calls = 0_u64;
    loop {
        let read = retained_read_at(source, &mut buffer, bytes).map_err(io_error)?;
        if read == 0 {
            break;
        }
        snapshot.write_all(&buffer[..read]).map_err(io_error)?;
        bytes = bytes
            .checked_add(u64::try_from(read).map_err(|_| corrupt("authentication byte overflow"))?)
            .ok_or_else(|| corrupt("authentication byte overflow"))?;
        read_calls = read_calls
            .checked_add(1)
            .ok_or_else(|| corrupt("authentication read call overflow"))?;
        digest.update(&buffer[..read]);
        #[cfg(test)]
        if read_calls == 1
            && let Some(barrier) = mutation_barrier.as_ref()
        {
            barrier.authenticated.wait();
            barrier.proceed.wait();
        } else if read_calls == 2
            && let Some(barrier) = mutation_barrier.as_ref()
        {
            barrier.copied.wait();
            barrier.restored.wait();
        }
    }
    if bytes != entry.byte_length || digest.finish() != entry.content_xxh64 {
        return Err(corrupt("property handle digest conflicts with inventory"));
    }
    if graphforge_filesystem::file_identity(source).map_err(io_error)? != expected_identity
        || source.metadata().map_err(io_error)?.len() != entry.byte_length
    {
        return Err(corrupt(
            "property fragment identity changed during snapshot",
        ));
    }
    snapshot.rewind().map_err(io_error)?;
    Ok((snapshot, bytes, bytes.div_ceil(64 * 1024), read_calls))
}

#[cfg(test)]
pub(super) fn digest_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(HEX[usize::from(byte >> 4)]));
        out.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    out
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PropertyLiveSchemaSummary {
    format: String,
    /// Exact number of newest live UUID snapshots containing each key.
    counts: BTreeMap<String, u64>,
}

fn decode_live_schema_summary(
    schema: &arrow::datatypes::Schema,
) -> Result<Option<PropertyLiveSchemaSummary>, GfError> {
    let Some(encoded) = schema.metadata().get(PROPERTY_LIVE_SCHEMA_KEY) else {
        return Ok(None);
    };
    let summary: PropertyLiveSchemaSummary = serde_json::from_str(encoded)
        .map_err(|_| corrupt("property live schema summary is invalid"))?;
    if summary.format != PROPERTY_LIVE_SCHEMA_FORMAT
        || summary.counts.values().any(|count| *count == 0)
    {
        return Err(corrupt("property live schema summary is invalid"));
    }
    Ok(Some(summary))
}

fn encode_live_schema_summary(counts: BTreeMap<String, u64>) -> Result<String, GfError> {
    serde_json::to_string(&PropertyLiveSchemaSummary {
        format: PROPERTY_LIVE_SCHEMA_FORMAT.to_owned(),
        counts,
    })
    .map_err(json_error)
}

pub(crate) fn rename_live_schema_summary(
    metadata: &mut HashMap<String, String>,
    renames: &BTreeMap<String, String>,
) -> Result<(), GfError> {
    let Some(encoded) = metadata.get(PROPERTY_LIVE_SCHEMA_KEY).cloned() else {
        return Ok(());
    };
    let schema = arrow::datatypes::Schema::new_with_metadata(
        Vec::<arrow::datatypes::Field>::new(),
        HashMap::from([(PROPERTY_LIVE_SCHEMA_KEY.to_owned(), encoded)]),
    );
    let summary = decode_live_schema_summary(&schema)?
        .ok_or_else(|| corrupt("property live schema summary disappeared"))?;
    let mut counts = BTreeMap::<String, u64>::new();
    for (name, count) in summary.counts {
        let name = renames.get(&name).cloned().unwrap_or(name);
        let next = counts
            .get(&name)
            .copied()
            .unwrap_or(0)
            .checked_add(count)
            .ok_or_else(|| corrupt("property live schema count overflows"))?;
        counts.insert(name, next);
    }
    metadata.insert(
        PROPERTY_LIVE_SCHEMA_KEY.to_owned(),
        encode_live_schema_summary(counts)?,
    );
    Ok(())
}

/// Apply an exact touched-UUID before/after delta to the authenticated route
/// summary. `None` means a legacy route without incremental summary authority;
/// callers preserve its historical union rather than inventing exact counts.
pub(crate) fn update_live_route_schema(
    kind: PropertyRouteKind,
    route: &str,
    authority: Option<&arrow::datatypes::SchemaRef>,
    inferred: arrow::datatypes::SchemaRef,
    before: &BTreeMap<[u8; 16], PropertySnapshotRow>,
    after: &[PropertySnapshotRow],
) -> Result<arrow::datatypes::SchemaRef, GfError> {
    let mut schema = match authority {
        Some(authority) => {
            merge_property_route_schemas(kind, route, [authority.as_ref(), inferred.as_ref()])?
        }
        None => inferred,
    };
    let existing = authority
        .map(|schema| decode_live_schema_summary(schema.as_ref()))
        .transpose()?
        .flatten();
    // A pre-summary legacy route cannot be upgraded from a targeted window:
    // untouched UUIDs may own any historical field. Preserve its union.
    if authority.is_some() && existing.is_none() {
        return Ok(schema);
    }
    let mut counts = existing.map_or_else(BTreeMap::new, |summary| summary.counts);
    let after = after
        .iter()
        .map(|row| (row.uuid, row))
        .collect::<BTreeMap<_, _>>();
    let mut touched = before.keys().copied().collect::<BTreeSet<_>>();
    touched.extend(after.keys().copied());
    for uuid in touched {
        let old = before.get(&uuid).filter(|row| !row.tombstone);
        let new = after.get(&uuid).copied().filter(|row| !row.tombstone);
        let mut keys = BTreeSet::new();
        if let Some(row) = old {
            keys.extend(row.values.keys().cloned());
        }
        if let Some(row) = new {
            keys.extend(row.values.keys().cloned());
        }
        for key in keys {
            let had = old.is_some_and(|row| row.values.contains_key(&key));
            let has = new.is_some_and(|row| row.values.contains_key(&key));
            match (had, has) {
                (false, true) => {
                    *counts.entry(key).or_default() = counts
                        .get(&key)
                        .copied()
                        .unwrap_or(0)
                        .checked_add(1)
                        .ok_or_else(|| corrupt("property live schema count overflows"))?;
                }
                (true, false) => {
                    let count = counts
                        .get_mut(&key)
                        .ok_or_else(|| corrupt("property live schema count underflows"))?;
                    *count = count
                        .checked_sub(1)
                        .ok_or_else(|| corrupt("property live schema count underflows"))?;
                    if *count == 0 {
                        counts.remove(&key);
                    }
                }
                _ => {}
            }
        }
    }
    let live_keys = counts.keys().cloned().collect::<BTreeSet<_>>();
    let encoded = encode_live_schema_summary(counts)?;
    let mut metadata = schema.metadata().clone();
    metadata.insert(PROPERTY_LIVE_SCHEMA_KEY.to_owned(), encoded);
    let fields = schema
        .fields()
        .iter()
        .filter(|field| {
            field.name() == "node_uuid"
                || field.name() == "edge_uuid"
                || live_keys.contains(field.name())
        })
        .cloned()
        .collect::<Vec<_>>();
    schema = Arc::new(arrow::datatypes::Schema::new_with_metadata(
        fields, metadata,
    ));
    Ok(schema)
}

pub(crate) fn authenticated_property_inventory_for_rewrite_route(
    project: &Path,
    kind: PropertyRouteKind,
    route: &str,
    rewrite: &crate::RewriteBatch,
) -> Result<AuthenticatedPropertyInventory, GfError> {
    if project.join(crate::CURRENT_FILE).is_file() {
        return authenticated_property_inventory_for_route(project, kind, route);
    }
    let inventory = crate::graph_files::capture_rewrite_baseline(project, rewrite)?;
    let read_calls = inventory.authority_read_calls();
    let authority_bytes = inventory.total_byte_length;
    let authority_block_equivalents = inventory.files.iter().fold(0_u64, |blocks, entry| {
        blocks.saturating_add(entry.byte_length.div_ceil(64 * 1024))
    });
    let mut admitted = AuthenticatedPropertyInventory::from_read_inventory_at_root(
        project,
        inventory,
        Some((kind, route)),
    )?;
    admitted.authority_bytes = authority_bytes;
    admitted.authority_block_equivalents = authority_block_equivalents;
    admitted.authority_read_calls = read_calls;
    Ok(admitted)
}

pub(crate) fn authenticated_property_inventory_for_rewrite(
    project: &Path,
    rewrite: &crate::RewriteBatch,
) -> Result<AuthenticatedPropertyInventory, GfError> {
    if project.join(crate::CURRENT_FILE).is_file() {
        return authenticated_property_inventory(project);
    }
    let inventory = crate::graph_files::capture_rewrite_baseline(project, rewrite)?;
    let read_calls = inventory.authority_read_calls();
    let authority_bytes = inventory.total_byte_length;
    let authority_block_equivalents = inventory.files.iter().fold(0_u64, |blocks, entry| {
        blocks.saturating_add(entry.byte_length.div_ceil(64 * 1024))
    });
    let mut admitted =
        AuthenticatedPropertyInventory::from_read_inventory_at_root(project, inventory, None)?;
    admitted.authority_bytes = authority_bytes;
    admitted.authority_block_equivalents = authority_block_equivalents;
    admitted.authority_read_calls = read_calls;
    Ok(admitted)
}

pub(crate) fn authenticated_property_inventory_for_route(
    project: &Path,
    kind: PropertyRouteKind,
    route: &str,
) -> Result<AuthenticatedPropertyInventory, GfError> {
    if project.join(crate::CURRENT_FILE).is_file() {
        let generation = crate::resolve_project_generation(project)?;
        return AuthenticatedPropertyInventory::from_resolved_generation_for_route(
            &generation,
            kind,
            route,
        );
    }
    let inventory = crate::capture_graph_read_inventory(project)?;
    let authority_read_calls = inventory.authority_read_calls();
    let authority_bytes = inventory.files.iter().map(|entry| entry.byte_length).sum();
    let authority_block_equivalents = inventory.files.iter().fold(0_u64, |blocks, entry| {
        blocks.saturating_add(entry.byte_length.div_ceil(64 * 1024))
    });
    let mut admitted = AuthenticatedPropertyInventory::from_read_inventory_at_root(
        project,
        inventory,
        Some((kind, route)),
    )?;
    admitted.authority_bytes = authority_bytes;
    admitted.authority_block_equivalents = authority_block_equivalents;
    admitted.authority_read_calls = authority_read_calls;
    Ok(admitted)
}

/// Admit one complete property authority for a raw project tree.
///
/// Catalog construction uses this once and shares the retained inventory with
/// every property provider, avoiding route-count-multiplied authentication.
pub(crate) fn authenticated_property_inventory(
    project: &Path,
) -> Result<AuthenticatedPropertyInventory, GfError> {
    if project.join(crate::CURRENT_FILE).is_file() {
        let generation = crate::resolve_project_generation(project)?;
        return AuthenticatedPropertyInventory::from_resolved_generation(&generation);
    }
    let inventory = crate::capture_graph_read_inventory(project)?;
    let authority_read_calls = inventory.authority_read_calls();
    let authority_bytes = inventory.files.iter().map(|entry| entry.byte_length).sum();
    let authority_block_equivalents = inventory.files.iter().fold(0_u64, |blocks, entry| {
        blocks.saturating_add(entry.byte_length.div_ceil(64 * 1024))
    });
    let mut admitted =
        AuthenticatedPropertyInventory::from_read_inventory_at_root(project, inventory, None)?;
    admitted.authority_bytes = authority_bytes;
    admitted.authority_block_equivalents = authority_block_equivalents;
    admitted.authority_read_calls = authority_read_calls;
    Ok(admitted)
}

/// Enumerate a route's immutable fragments in oldest-to-newest authority order.
///
/// The legacy flat file is admitted only as `(0, 0)`. Every entry in the
/// immutable directory must be a canonical regular file; near misses fail
/// closed instead of disappearing from the authority set.
pub fn enumerate_property_fragments(
    project: &Path,
    kind: PropertyRouteKind,
    route: &str,
) -> Result<Vec<PropertyFragment>, GfError> {
    validate_route(route)?;
    let root = project.join(kind.subdir());
    let mut fragments = Vec::new();
    let legacy = root.join(format!("{route}.parquet"));
    match fs::symlink_metadata(&legacy) {
        Ok(metadata) if metadata.file_type().is_file() => fragments.push(PropertyFragment {
            id: PropertyFragmentId {
                generation: 0,
                ordinal: 0,
            },
            path: legacy,
        }),
        Ok(_) => return Err(corrupt("legacy property fragment is not a regular file")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(GfError::Storage(error.to_string())),
    }
    let directory = root.join(route);
    let entries = match fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(fragments),
        Err(error) => return Err(GfError::Storage(error.to_string())),
    };
    let mut continuation_paths = BTreeMap::<PathBuf, Vec<u64>>::new();
    for entry in entries {
        let entry = entry.map_err(|error| GfError::Storage(error.to_string()))?;
        if crate::staging::is_staged_temp_name(&entry.file_name()) {
            continue;
        }
        let metadata = fs::symlink_metadata(entry.path())
            .map_err(|error| GfError::Storage(error.to_string()))?;
        if !metadata.file_type().is_file() {
            return Err(corrupt("property fragment is not a regular file"));
        }
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| corrupt("property fragment identity is not canonical UTF-8"))?;
        if let Some((anchor, index)) = super::bounded_object::split_part_path(&entry.path()) {
            PropertyFragmentId::parse(
                anchor
                    .file_name()
                    .and_then(|name| name.to_str())
                    .ok_or_else(|| corrupt("property anchor is not UTF-8"))?,
            )?;
            continuation_paths.entry(anchor).or_default().push(index);
        } else {
            fragments.push(PropertyFragment {
                id: PropertyFragmentId::parse(&name)?,
                path: entry.path(),
            });
        }
    }
    for (anchor, mut indices) in continuation_paths {
        if !fragments.iter().any(|fragment| fragment.path == anchor) {
            return Err(corrupt("property continuation has no anchor"));
        }
        let retained =
            graphforge_filesystem::StableDirectory::open(&directory).map_err(io_error)?;
        let file = retained
            .open_child_file(
                anchor
                    .file_name()
                    .ok_or_else(|| corrupt("property anchor has no filename"))?,
            )
            .map_err(io_error)?;
        let (layout, index) = super::bounded_object::inspect_envelope(file)?
            .ok_or_else(|| corrupt("property continuation belongs to a plain fragment"))?;
        if index != 0 {
            return Err(corrupt("property anchor has wrong part index"));
        }
        indices.push(0);
        indices.sort_unstable();
        super::bounded_object::validate_parts(layout, &indices)?;
    }
    fragments.sort_unstable_by_key(|fragment| fragment.id);
    if fragments.windows(2).any(|pair| pair[0].id == pair[1].id) {
        return Err(corrupt("duplicate property fragment identity"));
    }
    validate_fragment_id_sequence(fragments.iter().map(|fragment| fragment.id))?;
    Ok(fragments)
}

fn validate_fragment_id_sequence(
    ids: impl IntoIterator<Item = PropertyFragmentId>,
) -> Result<(), GfError> {
    let mut prior: Option<PropertyFragmentId> = None;
    for id in ids {
        if let Some(previous) = prior {
            if id.generation == previous.generation
                && Some(id.ordinal) != previous.ordinal.checked_add(1)
            {
                return Err(corrupt("property fragment ordinal sequence has a gap"));
            }
            if id.generation != previous.generation && id.ordinal != 0 {
                return Err(corrupt(
                    "property fragment generation does not start at ordinal zero",
                ));
            }
        } else if id.generation != 0 && id.ordinal != 0 {
            return Err(corrupt(
                "property fragment generation does not start at ordinal zero",
            ));
        }
        prior = Some(id);
    }
    Ok(())
}

fn validate_route(route: &str) -> Result<(), GfError> {
    if route.is_empty() || route == "." || route == ".." || route.contains(['/', '\0']) {
        return Err(corrupt("property route is not canonical"));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
