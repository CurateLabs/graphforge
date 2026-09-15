//! Authenticated workspace hydration and generation read authority.

use super::{
    CompositionBindingContext, CompositionBindingLimits, GfError, GraphForge, OntologyDoc,
    OntologyHandle, OntologyMode, ResolvedProjectGeneration, RuntimeCatalog, graph_snapshot,
};
use graphforge_ontology::OntologyCompiler;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// A selected graph path and the owner keeping its private resources alive.
/// Pinned generations can read a path different from the temporary guard root.
#[derive(Clone, Debug)]
pub(super) struct GraphWorkspace {
    pub(super) dir: PathBuf,
    pub(super) _owner: Arc<tempfile::TempDir>,
}

impl GraphWorkspace {
    pub(super) fn path(&self) -> &Path {
        &self.dir
    }
}

impl std::ops::Deref for GraphWorkspace {
    type Target = Path;
    fn deref(&self) -> &Path {
        self.path()
    }
}

impl AsRef<Path> for GraphWorkspace {
    fn as_ref(&self) -> &Path {
        self.path()
    }
}

pub(super) struct PreparedGenerationReadAuthority {
    pub(super) properties: Arc<graphforge_storage::AuthenticatedPropertyInventory>,
    pub(super) ordinal: Option<graphforge_storage::ordinal_identity_v4::V4OrdinalIdentityHandle>,
    pub(super) adjacency: Arc<graphforge_exec::PersistentAdjacencyProvider>,
}

pub(super) struct GenerationPropertyAuthority {
    pub(super) generation_uuid: uuid::Uuid,
    pub(super) inventory: Arc<graphforge_storage::AuthenticatedPropertyInventory>,
}

impl GraphForge {
    pub(super) fn generation_for_read(&self) -> Result<ResolvedProjectGeneration, GfError> {
        self.graph_visibility.health.check()?;
        if self.read_only {
            Ok(self.resolved_generation.clone())
        } else {
            graphforge_storage::resolve_project_generation(
                self.resolved_generation.container_root(),
            )
        }
    }

    pub(super) fn workspace_for_session(&self) -> GraphWorkspace {
        self.workspace_guard
            .read()
            .expect("workspace lock poisoned")
            .clone()
    }

    pub(super) fn dir(&self) -> GraphWorkspace {
        self.workspace_for_session()
    }

    pub(super) fn replace_workspace_owner(&self, workspace: GraphWorkspace) -> GraphWorkspace {
        std::mem::replace(
            &mut *self
                .workspace_guard
                .write()
                .expect("workspace lock poisoned"),
            workspace,
        )
    }

    pub(super) fn adjacency_provider_for_session(
        &self,
    ) -> Arc<graphforge_exec::PersistentAdjacencyProvider> {
        Arc::clone(
            &self
                .adjacency_provider
                .read()
                .expect("adjacency provider lock poisoned"),
        )
    }

    pub(super) fn property_inventory_for_session(
        &self,
    ) -> Arc<graphforge_storage::AuthenticatedPropertyInventory> {
        let authority = self
            .property_authority
            .lock()
            .expect("property authority lock poisoned");
        debug_assert_eq!(
            authority.inventory.generation_uuid(),
            Some(authority.generation_uuid)
        );
        Arc::clone(&authority.inventory)
    }

    pub(super) fn prepare_generation_read_authority(
        &self,
        generation: &ResolvedProjectGeneration,
        graph_root: &Path,
    ) -> Result<PreparedGenerationReadAuthority, GfError> {
        let properties = property_inventory_for_hydrated_generation(generation, graph_root)?;
        let ordinal = ordinal_identity_handle(generation, graph_root)?;
        let adjacency = Arc::new(adjacency_provider_for_graph(
            graph_root,
            self.ontology_mode,
            Arc::clone(&properties),
        )?);
        Ok(PreparedGenerationReadAuthority {
            properties,
            ordinal,
            adjacency,
        })
    }

    pub(super) fn install_prepared_generation_read_authority(
        &self,
        generation_uuid: uuid::Uuid,
        prepared: PreparedGenerationReadAuthority,
    ) {
        *self
            .property_authority
            .lock()
            .expect("property authority lock poisoned") = GenerationPropertyAuthority {
            generation_uuid,
            inventory: prepared.properties,
        };
        *self
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned") = generation_uuid;
        *self
            .adjacency_provider
            .write()
            .expect("adjacency provider lock poisoned") = prepared.adjacency;
        self.ordinal_identities.replace(prepared.ordinal);
    }

    pub(super) fn install_property_generation(
        &self,
        generation: &ResolvedProjectGeneration,
    ) -> Result<(), GfError> {
        let prepared = self.prepare_generation_read_authority(generation, &self.dir())?;
        self.install_prepared_generation_read_authority(generation.generation_uuid(), prepared);
        Ok(())
    }

    /// Install the exact compiled context reconstructed from the persisted
    /// composition participant. This is the narrow atomic hydration seam used
    /// by the composition lifecycle; ordinary execution consumes it by default.
    #[allow(dead_code)] // consumed by the generation composition publisher added by issue #840
    pub(crate) fn install_generation_composition_context(
        &self,
        context: &Arc<CompositionBindingContext>,
    ) -> Result<(), GfError> {
        let bindings = self
            .semantic_storage_bindings
            .lock()
            .expect("semantic storage binding lock poisoned")
            .clone()
            .ok_or_else(|| {
                GfError::Validation(
                    "persisted composition has no generation storage binding authority".into(),
                )
            })?;
        bindings.validate_against(context.composition())?;
        let context = context.with_generation_storage_ids(
            bindings
                .bindings
                .iter()
                .map(|binding| (binding.symbol.clone(), binding.storage_id)),
        )?;
        *self
            .default_composition_context
            .lock()
            .expect("default composition context lock poisoned") = Some(Arc::new(context));
        Ok(())
    }
}

/// Seed a [`RuntimeCatalog`] from `topology/runtime_catalog.parquet` if present,
/// else return a fresh one. Missing or empty files yield an empty catalog; a
/// present but malformed / undecodable catalog fails closed so reconciliation
/// cannot write the encoding marker against an incomplete identity map (#702/#725).
pub(super) fn load_runtime_catalog(dir: &std::path::Path) -> Result<RuntimeCatalog, GfError> {
    let path = dir.join("topology").join("runtime_catalog.parquet");
    if !path.exists() {
        return Ok(RuntimeCatalog::new());
    }
    read_runtime_catalog(&path)
}

/// Read and decode every batch of `runtime_catalog.parquet` into a [`RuntimeCatalog`].
pub(super) fn read_runtime_catalog(path: &std::path::Path) -> Result<RuntimeCatalog, GfError> {
    let file = std::fs::File::open(path).map_err(|e| {
        GfError::Storage(format!(
            "failed to open runtime catalog {}: {e}",
            path.display()
        ))
    })?;
    decode_runtime_catalog(file, path)
}

pub(super) fn decode_runtime_catalog<T: parquet::file::reader::ChunkReader + 'static>(
    file: T,
    path: &std::path::Path,
) -> Result<RuntimeCatalog, GfError> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| {
            GfError::Storage(format!("malformed runtime catalog {}: {e}", path.display()))
        })?
        .build()
        .map_err(|e| {
            GfError::Storage(format!("malformed runtime catalog {}: {e}", path.display()))
        })?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch.map_err(|e| {
            GfError::Storage(format!(
                "failed reading runtime catalog {}: {e}",
                path.display()
            ))
        })?);
    }
    // Zero-row / zero-batch parquet is equivalent to a missing catalog. Fail
    // closed only on malformed or undecodable content.
    if batches.is_empty() {
        return Ok(RuntimeCatalog::new());
    }
    let schema = batches[0].schema();
    let merged = arrow::compute::concat_batches(&schema, &batches).map_err(|e| {
        GfError::Storage(format!(
            "failed to merge runtime catalog batches in {}: {e}",
            path.display()
        ))
    })?;
    RuntimeCatalog::from_record_batch(&merged)
        .map_err(|e| GfError::Storage(format!("invalid runtime catalog {}: {e}", path.display())))
}

pub(super) fn property_inventory_for_hydrated_generation(
    generation: &ResolvedProjectGeneration,
    hydrated_root: &Path,
) -> Result<Arc<graphforge_storage::AuthenticatedPropertyInventory>, GfError> {
    property_and_graph_inventory_for_hydrated_generation(generation, hydrated_root)
        .map(|(properties, _)| properties)
}

// Hydration authenticates the published base and journal before deriving this
// private inventory. Both property and semantic validation must use that same
// effective graph, rather than compare replay output with the unchanged base.
pub(super) fn property_and_graph_inventory_for_hydrated_generation(
    generation: &ResolvedProjectGeneration,
    hydrated_root: &Path,
) -> Result<
    (
        Arc<graphforge_storage::AuthenticatedPropertyInventory>,
        graphforge_storage::GraphFilesInventory,
    ),
    GfError,
> {
    let inventory = generation.graph_files_inventory()?;
    let has_deltas = match inventory.as_ref() {
        Some(inventory) => !graphforge_storage::list_delta_runs(
            inventory,
            graphforge_storage::GraphDeltaJournalLimits::default(),
        )?
        .is_empty(),
        None => false,
    };
    // Snapshot-only generations have no graph-files inventory; hydration has
    // authenticated their snapshot payload into this private workspace.
    let (admitted, effective) = match inventory {
        Some(inventory) if !has_deltas => (
            graphforge_storage::AuthenticatedPropertyInventory::from_resolved_generation(
                generation,
            )?,
            inventory,
        ),
        _ => {
            let (materialized, _) = graphforge_storage::capture_graph_files(hydrated_root)?;
            let admitted =
                graphforge_storage::AuthenticatedPropertyInventory::from_materialized_inventory(
                    generation,
                    hydrated_root,
                    materialized.clone(),
                )?;
            (admitted, materialized)
        }
    };
    Ok((Arc::new(admitted), effective))
}

pub(super) fn ordinal_identity_handle(
    generation: &ResolvedProjectGeneration,
    graph_root: &Path,
) -> Result<Option<graphforge_storage::ordinal_identity_v4::V4OrdinalIdentityHandle>, GfError> {
    let Some(authority) = generation.authenticated_v4_ordinal_authority()? else {
        return Ok(None);
    };
    match authority
        .open(
            graph_root,
            graphforge_storage::V4OrdinalIdentityLimits::default(),
        )
        .map_err(|error| GfError::Storage(error.to_string()))?
    {
        graphforge_storage::V4OrdinalIdentityOpen::Ready(handle) => Ok(Some(*handle)),
        graphforge_storage::V4OrdinalIdentityOpen::RebuildRequired { found_version } => {
            Err(GfError::Validation(format!(
                "selected graph generation requires ordinal identity rebuild from version {found_version}"
            )))
        }
    }
}

pub(super) fn ordinal_identity_resolver(
    generation: &ResolvedProjectGeneration,
    graph_root: &Path,
) -> Result<Arc<graphforge_exec::V4OrdinalIdentityResolver>, GfError> {
    Ok(Arc::new(graphforge_exec::V4OrdinalIdentityResolver::new(
        ordinal_identity_handle(generation, graph_root)?,
    )))
}

pub(super) fn adjacency_provider_for_graph(
    dir: &Path,
    mode: OntologyMode,
    inventory: Arc<graphforge_storage::AuthenticatedPropertyInventory>,
) -> Result<graphforge_exec::PersistentAdjacencyProvider, GfError> {
    let provider = graphforge_exec::PersistentAdjacencyProvider::new(dir.to_path_buf(), mode)
        .with_inventory(inventory);
    // A pinned view must not mutate its generation. Writable workspaces also
    // have concurrent readers that enumerate graph files, so lazy build temps
    // must stay outside those trees. Each provider owns a unique cache root.
    let artifacts = tempfile::Builder::new()
        .prefix("graphforge-adjacency-cache-")
        .tempdir()
        .map_err(|error| GfError::Storage(format!("cannot create adjacency cache: {error}")))?;
    Ok(provider.with_rebuild_root(artifacts))
}

fn create_graph_workspace() -> Result<Arc<tempfile::TempDir>, GfError> {
    tempfile::Builder::new()
        .prefix("graphforge-graph-workspace-")
        .tempdir()
        .map(Arc::new)
        .map_err(|error| GfError::Storage(format!("failed to create graph workspace: {error}")))
}

pub(super) fn hydrate_graph_workspace(
    generation: &ResolvedProjectGeneration,
    read_only: bool,
) -> Result<
    (
        PathBuf,
        Arc<tempfile::TempDir>,
        graphforge_storage::GraphFilesOpenEvidence,
    ),
    GfError,
> {
    let files = generation.participant_snapshot(
        graphforge_storage::GRAPH_CAPABILITY_ID,
        graphforge_storage::GRAPH_FILES_FAMILY,
    )?;
    let snapshot = generation.participant_snapshot("graph", "snapshot")?;
    if files.is_some() && snapshot.is_some() {
        return Err(GfError::Validation(
            "graph generation cannot declare both snapshot and files participants".into(),
        ));
    }

    if let Some(files) = files {
        validate_graph_files_snapshot(&files)?;
        if matches!(
            files.record_version,
            graphforge_storage::GRAPH_FILES_V2_RECORD_VERSION
                | graphforge_storage::GRAPH_FILES_MAPPED_ROOT_RECORD_VERSION
        ) {
            let inventory = generation
                .graph_files_inventory()?
                .ok_or_else(|| GfError::Validation("compact graph root disappeared".into()))?;
            return hydrate_compact_graph_workspace(generation, &inventory);
        }
        let inventory = graphforge_storage::decode_inventory(&files.bytes)?;
        let tree = generation.graph_tree_root();
        graphforge_storage::verify_graph_tree(&tree, &inventory)?;
        let has_authoritative_deltas = !graphforge_storage::list_delta_runs(
            &inventory,
            graphforge_storage::GraphDeltaJournalLimits::default(),
        )?
        .is_empty();
        if has_authoritative_deltas {
            let workspace = Arc::new(
                tempfile::Builder::new()
                    .prefix("graphforge-graph-replay-")
                    .tempdir()
                    .map_err(|error| {
                        GfError::Storage(format!(
                            "failed to create graph replay workspace: {error}"
                        ))
                    })?,
            );
            let (evidence, _replay) = graphforge_storage::materialize_replayed_graph_tree(
                &tree,
                &inventory,
                workspace.path(),
                graphforge_storage::GraphDeltaJournalLimits::default(),
            )?;
            return Ok((workspace.path().to_path_buf(), workspace, evidence));
        }
        if read_only {
            let guard = Arc::new(
                tempfile::Builder::new()
                    .prefix("graphforge-graph-pinned-")
                    .tempdir()
                    .map_err(|error| {
                        GfError::Storage(format!("failed to create graph workspace guard: {error}"))
                    })?,
            );
            return Ok((
                tree,
                guard,
                graphforge_storage::pinned_open_evidence(&inventory),
            ));
        }
        let workspace = create_graph_workspace()?;
        let evidence =
            graphforge_storage::materialize_graph_tree(&tree, &inventory, workspace.path())?;
        return Ok((workspace.path().to_path_buf(), workspace, evidence));
    }

    let workspace = create_graph_workspace()?;
    let mut evidence = graphforge_storage::GraphFilesOpenEvidence {
        strategy: graphforge_storage::GraphFilesOpenStrategy::Empty,
        ..graphforge_storage::GraphFilesOpenEvidence::default()
    };
    if let Some(snapshot) = snapshot {
        if snapshot.capability_version != 1
            || snapshot.record_version != 1
            || snapshot.encoding != "arrow"
        {
            return Err(GfError::Validation(
                "unsupported graph snapshot participant contract".into(),
            ));
        }
        graph_snapshot::hydrate(&snapshot.bytes, workspace.path())?;
        evidence.strategy = graphforge_storage::GraphFilesOpenStrategy::LegacySnapshotHydrate;
        evidence.bytes_copied = u64::try_from(snapshot.bytes.len()).unwrap_or(u64::MAX);
        evidence.files_copied = 1;
    }
    Ok((workspace.path().to_path_buf(), workspace, evidence))
}

fn validate_graph_files_snapshot(
    files: &graphforge_storage::ProjectParticipantSnapshot,
) -> Result<(), GfError> {
    if files.capability_version != graphforge_storage::GRAPH_CAPABILITY_VERSION
        || !matches!(
            files.record_version,
            graphforge_storage::GRAPH_FILES_RECORD_VERSION
                | graphforge_storage::GRAPH_FILES_V2_RECORD_VERSION
                | graphforge_storage::GRAPH_FILES_MAPPED_RECORD_VERSION
                | graphforge_storage::GRAPH_FILES_MAPPED_ROOT_RECORD_VERSION
        )
        || files.encoding != "json"
    {
        return Err(GfError::Validation(
            "unsupported graph files participant contract".into(),
        ));
    }
    Ok(())
}

fn hydrate_compact_graph_workspace(
    generation: &ResolvedProjectGeneration,
    inventory: &graphforge_storage::GraphFilesInventory,
) -> Result<
    (
        PathBuf,
        Arc<tempfile::TempDir>,
        graphforge_storage::GraphFilesOpenEvidence,
    ),
    GfError,
> {
    let workspace = Arc::new(
        tempfile::Builder::new()
            .prefix("graphforge-graph-workspace-")
            .tempdir()
            .map_err(|error| {
                GfError::Storage(format!("failed to create graph workspace: {error}"))
            })?,
    );
    let evidence = materialize_compact_graph_target(generation, inventory, workspace.path())?;
    Ok((workspace.path().to_path_buf(), workspace, evidence))
}

fn materialize_compact_graph_target(
    generation: &ResolvedProjectGeneration,
    inventory: &graphforge_storage::GraphFilesInventory,
    target: &std::path::Path,
) -> Result<graphforge_storage::GraphFilesOpenEvidence, GfError> {
    let has_authoritative_deltas = !graphforge_storage::list_delta_runs(
        inventory,
        graphforge_storage::GraphDeltaJournalLimits::default(),
    )?
    .is_empty();
    if !has_authoritative_deltas {
        return graphforge_storage::materialize_graph_objects(
            generation.container_root(),
            inventory,
            target,
        );
    }
    let source = tempfile::Builder::new()
        .prefix("graphforge-graph-cas-source-")
        .tempdir()
        .map_err(|error| {
            GfError::Storage(format!(
                "failed to create graph CAS source workspace: {error}"
            ))
        })?;
    let reused = graphforge_storage::materialize_graph_objects(
        generation.container_root(),
        inventory,
        source.path(),
    )?;
    let (copied, _replay) = graphforge_storage::materialize_replayed_graph_tree(
        source.path(),
        inventory,
        target,
        graphforge_storage::GraphDeltaJournalLimits::default(),
    )?;
    let evidence = graphforge_storage::GraphFilesOpenEvidence {
        strategy: graphforge_storage::GraphFilesOpenStrategy::PrivateMaterialize,
        files_validated: reused
            .files_validated
            .checked_add(copied.files_validated)
            .ok_or_else(|| GfError::Storage("hydration validated-file count overflows".into()))?,
        bytes_validated: reused
            .bytes_validated
            .checked_add(copied.bytes_validated)
            .ok_or_else(|| GfError::Storage("hydration validated-byte count overflows".into()))?,
        files_copied: copied.files_copied,
        bytes_copied: copied.bytes_copied,
        files_opened_in_place: 0,
        files_reused: reused.files_reused,
        bytes_reused: reused.bytes_reused,
        application_read_bytes: reused
            .application_read_bytes
            .checked_add(copied.application_read_bytes)
            .ok_or_else(|| GfError::Storage("hydration read byte count overflows".into()))?,
        application_read_calls: reused
            .application_read_calls
            .checked_add(copied.application_read_calls)
            .ok_or_else(|| GfError::Storage("hydration read call count overflows".into()))?,
        application_write_bytes: reused
            .application_write_bytes
            .checked_add(copied.application_write_bytes)
            .ok_or_else(|| GfError::Storage("hydration write byte count overflows".into()))?,
        application_write_calls: reused
            .application_write_calls
            .checked_add(copied.application_write_calls)
            .ok_or_else(|| GfError::Storage("hydration write call count overflows".into()))?,
        fsync_calls: reused
            .fsync_calls
            .checked_add(copied.fsync_calls)
            .ok_or_else(|| GfError::Storage("hydration fsync count overflows".into()))?,
        file_fsync_calls: reused
            .file_fsync_calls
            .checked_add(copied.file_fsync_calls)
            .ok_or_else(|| GfError::Storage("hydration file barrier count overflows".into()))?,
        directory_fsync_calls: reused
            .directory_fsync_calls
            .checked_add(copied.directory_fsync_calls)
            .ok_or_else(|| {
                GfError::Storage("hydration directory barrier count overflows".into())
            })?,
    };
    Ok(evidence)
}

pub(crate) fn rematerialize_graph_workspace(
    generation: &ResolvedProjectGeneration,
    target: &std::path::Path,
) -> Result<(), GfError> {
    if target.exists() {
        for entry in std::fs::read_dir(target).map_err(|error| {
            GfError::Storage(format!(
                "failed to read graph workspace for restore: {error}"
            ))
        })? {
            let entry = entry.map_err(|error| {
                GfError::Storage(format!("failed to read graph workspace entry: {error}"))
            })?;
            let path = entry.path();
            let file_type = entry.file_type().map_err(|error| {
                GfError::Storage(format!("failed to inspect graph workspace entry: {error}"))
            })?;
            if file_type.is_dir() {
                std::fs::remove_dir_all(&path).map_err(|error| {
                    GfError::Storage(format!(
                        "failed to clear graph workspace directory: {error}"
                    ))
                })?;
            } else {
                std::fs::remove_file(&path).map_err(|error| {
                    GfError::Storage(format!("failed to clear graph workspace file: {error}"))
                })?;
            }
        }
    }
    if let Some(files) = generation.participant_snapshot(
        graphforge_storage::GRAPH_CAPABILITY_ID,
        graphforge_storage::GRAPH_FILES_FAMILY,
    )? {
        validate_graph_files_snapshot(&files)?;
        if matches!(
            files.record_version,
            graphforge_storage::GRAPH_FILES_V2_RECORD_VERSION
                | graphforge_storage::GRAPH_FILES_MAPPED_ROOT_RECORD_VERSION
        ) {
            let inventory = generation
                .graph_files_inventory()?
                .ok_or_else(|| GfError::Validation("compact graph root disappeared".into()))?;
            materialize_compact_graph_target(generation, &inventory, target)?;
        } else {
            let inventory = graphforge_storage::decode_inventory(&files.bytes)?;
            graphforge_storage::materialize_graph_tree(
                &generation.graph_tree_root(),
                &inventory,
                target,
            )?;
        }
        return Ok(());
    }
    if let Some(snapshot) = generation.participant_snapshot("graph", "snapshot")? {
        if snapshot.capability_version != 1
            || snapshot.record_version != 1
            || snapshot.encoding != "arrow"
        {
            return Err(GfError::Validation(
                "unsupported graph snapshot participant contract".into(),
            ));
        }
        graph_snapshot::hydrate(&snapshot.bytes, target)?;
    }
    Ok(())
}

pub(super) fn load_workspace_ontology(
    generation: &ResolvedProjectGeneration,
) -> Result<(OntologyMode, Option<OntologyHandle>, Option<OntologyDoc>), GfError> {
    generation.require_capability(
        graphforge_storage::WORKSPACE_CAPABILITY_ID,
        graphforge_storage::WORKSPACE_CAPABILITY_VERSION,
    )?;
    let ontology_snapshot = generation
        .participant_snapshot(
            graphforge_storage::WORKSPACE_CAPABILITY_ID,
            graphforge_storage::WORKSPACE_ONTOLOGY_FAMILY,
        )?
        .ok_or_else(|| {
            GfError::Validation("committed generation is missing workspace ontology".into())
        })?;
    let configuration_snapshot = generation
        .participant_snapshot(
            graphforge_storage::WORKSPACE_CAPABILITY_ID,
            graphforge_storage::WORKSPACE_CONFIGURATION_FAMILY,
        )?
        .ok_or_else(|| {
            GfError::Validation("committed generation is missing workspace configuration".into())
        })?;
    if ontology_snapshot.capability_version != 1
        || ontology_snapshot.record_version != 1
        || ontology_snapshot.encoding != "json"
        || configuration_snapshot.capability_version != 1
        || configuration_snapshot.record_version != 1
        || configuration_snapshot.encoding != "json"
    {
        return Err(GfError::Validation(
            "unsupported workspace participant contract".into(),
        ));
    }
    let ontology_record =
        graphforge_storage::WorkspaceOntology::from_canonical_json(&ontology_snapshot.bytes)?;
    let configuration = graphforge_storage::WorkspaceConfiguration::from_canonical_json(
        &configuration_snapshot.bytes,
    )?;
    let composition = generation
        .participant_snapshot(
            graphforge_storage::WORKSPACE_CAPABILITY_ID,
            graphforge_storage::WORKSPACE_ONTOLOGY_COMPOSITION_FAMILY,
        )?
        .map(|snapshot| {
            graphforge_storage::WorkspaceOntologyComposition::from_canonical_json(&snapshot.bytes)
        })
        .transpose()?;
    if let Some(composition) = &composition {
        let composition_mode = match composition.profile_default {
            graphforge_ontology::ActivationMode::Exploratory => {
                graphforge_storage::WorkspaceOntologyMode::None
            }
            graphforge_ontology::ActivationMode::Advisory => {
                graphforge_storage::WorkspaceOntologyMode::Advisory
            }
            graphforge_ontology::ActivationMode::Strict => {
                graphforge_storage::WorkspaceOntologyMode::Strict
            }
        };
        if composition_mode != configuration.ontology_mode {
            return Err(GfError::Validation(
                "workspace composition and configuration modes disagree".into(),
            ));
        }
    } else if ontology_record.mode != configuration.ontology_mode {
        return Err(GfError::Validation(
            "workspace ontology and configuration modes disagree".into(),
        ));
    }
    let mode = configuration.ontology_mode.execution_mode();
    let document = ontology_record
        .canonical_ontology
        .map(|document| {
            let document: graphforge_ontology::OntologyDoc = serde_json::from_value(document)
                .map_err(|error| GfError::Ontology(format!("invalid adopted ontology: {error}")))?;
            Ok::<OntologyDoc, GfError>(document)
        })
        .transpose()?;
    let ontology = document
        .as_ref()
        .map(|document| {
            let runtime = OntologyCompiler::compile(document).map_err(|error| {
                GfError::Ontology(format!("failed to compile ontology: {error}"))
            })?;
            Ok::<OntologyHandle, GfError>(OntologyHandle::new(runtime))
        })
        .transpose()?;
    Ok((mode, ontology, document))
}

pub(super) fn load_composition_binding(
    generation: &ResolvedProjectGeneration,
) -> Result<Option<Arc<CompositionBindingContext>>, GfError> {
    let Some(snapshot) = generation.participant_snapshot(
        graphforge_storage::WORKSPACE_CAPABILITY_ID,
        graphforge_storage::WORKSPACE_ONTOLOGY_COMPOSITION_FAMILY,
    )?
    else {
        return Ok(None);
    };
    let expected_schema: [u8; 32] = Sha256::digest(
        format!(
            "workspace/{}@1",
            graphforge_storage::WORKSPACE_ONTOLOGY_COMPOSITION_FAMILY
        )
        .as_bytes(),
    )
    .into();
    if snapshot.capability_version != graphforge_storage::WORKSPACE_CAPABILITY_VERSION
        || snapshot.record_version != graphforge_storage::WORKSPACE_ONTOLOGY_COMPOSITION_VERSION
        || snapshot.encoding != "json"
        || snapshot.schema_fingerprint != expected_schema
        || snapshot.row_count != 1
    {
        return Err(GfError::Validation(
            "unsupported workspace ontology composition participant contract".into(),
        ));
    }
    let authority =
        graphforge_storage::WorkspaceOntologyComposition::from_canonical_json(&snapshot.bytes)?;
    let compiled = authority.compile()?;
    Ok(Some(Arc::new(CompositionBindingContext::new(
        Arc::new(compiled),
        authority.bridges,
        CompositionBindingLimits::default(),
    ))))
}

#[cfg(test)]
mod tests;
