//! Selected graph materialization with separate immutable research identity.
use super::{
    BTreeSet, Deserialize, Digest, GfError, PRODUCER, Path, RegisterResearchVersion,
    ResearchParticipantKey, ResearchRegistry, ResearchVersionRecord, ResolvedProjectGeneration,
    Serialize, Sha256, Uuid, insert_version, inspect_with_registry, invalid, retained_content,
};

/// Explicit graph selection, never a mutable query against a live parent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchGraphSelection {
    /// Selected node UUIDs.
    pub nodes: BTreeSet<Uuid>,
    /// Selected edge UUIDs; referential selection adds their endpoints.
    pub edges: BTreeSet<Uuid>,
    /// Include all edges between selected nodes instead of explicit edges.
    pub induced_edges: bool,
    /// Property fields omitted from the separate projection identity.
    pub exclude_properties: BTreeSet<String>,
}

/// Exact semantic commitment for repacked graph data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchGraphProjection {
    /// Immutable requested scope.
    pub selection: ResearchGraphSelection,
    /// Logical graph equality independent of Parquet representation.
    pub fingerprint: [u8; 32],
    /// Creation read scope; this does not claim constant-time parent scanning.
    pub source_payload_bytes: u64,
    /// Exact graph payload bytes written and retained by the projection.
    pub selected_payload_bytes: u64,
    /// Measured payload copies while preparing the source for projection.
    pub source_materialization_bytes_copied: u64,
    /// Measured immutable payload reuse while preparing the source.
    pub source_materialization_bytes_reused: u64,
}

pub(super) fn register(
    root: &Path,
    registry: &mut ResearchRegistry,
    spec: &RegisterResearchVersion,
    selection: &ResearchGraphSelection,
) -> Result<Uuid, GfError> {
    let origin = validate_scope(registry, spec, selection)?;
    let keys = spec
        .selection
        .as_ref()
        .ok_or_else(|| invalid("graph projection requires explicit participant selection"))?;
    let snapshots = inspect_with_registry(root, &origin, registry)?;
    let graph = snapshots
        .iter()
        .find(|p| p.capability_id == "graph" && p.record_family_id == "files");
    let Some(graph) = graph else {
        if !selection.nodes.is_empty() || !selection.edges.is_empty() {
            return Err(invalid(
                "selected graph identities are absent from the source",
            ));
        }
        return register_graphless(root, registry, spec, &origin, &snapshots);
    };
    let graph_key = ResearchParticipantKey {
        capability: "graph".into(),
        family: "files".into(),
    };
    if !keys.contains(&graph_key)
        || keys
            .iter()
            .any(|key| !origin.content.participants.iter().any(|p| &p.key == key))
    {
        return Err(invalid(
            "projection participant selection is outside source content",
        ));
    }
    let private_source = tempfile::tempdir().map_err(|error| io(&error))?;
    let private_output = tempfile::tempdir().map_err(|error| io(&error))?;
    let source_generation = if registry.materialized.contains(&origin.version_uuid) {
        None
    } else {
        Some(crate::resolve_generation_by_uuid(
            root,
            origin.content.generation_uuid,
        )?)
    };
    let (source_path, source_bytes, source_copied, source_reused) = source_graph(
        root,
        graph,
        source_generation.as_ref(),
        private_source.path(),
    )?;
    let summary = crate::graph_projection::materialize_portable_graph_tree_projection(
        &source_path,
        private_output.path(),
        &native_selection(selection),
    )?;
    let (inventory, _) = crate::capture_graph_files(private_output.path())?;
    let graph_projection = ResearchGraphProjection {
        selection: selection.clone(),
        fingerprint: summary.graph_content_fingerprint,
        source_payload_bytes: source_bytes,
        source_materialization_bytes_copied: source_copied,
        source_materialization_bytes_reused: source_reused,
        selected_payload_bytes: inventory.total_byte_length,
    };
    let lease = crate::begin_graph_object_publication(root)?;
    let graph_root = retained_content::install_graph(&lease, private_output.path(), &inventory)?;
    let projected = crate::graph_files::graph_files_root_participant(&graph_root)?;
    let mut content = origin.content.clone();
    content.source_version = Some(origin.version_uuid);
    content.graph_projection = Some(graph_projection);
    content
        .required_versions
        .clone_from(&spec.required_versions);
    content.evidence.clone_from(&spec.evidence);
    content.producer = PRODUCER.into();
    content.participants.retain(|p| keys.contains(&p.key));
    for p in &mut content.participants {
        let bytes = if p.key == graph_key {
            p.record_version = projected.record_version;
            p.schema_sha256 = projected.schema_fingerprint;
            p.row_count = projected.row_count;
            p.content_sha256 = Sha256::digest(&projected.bytes).into();
            &projected.bytes
        } else {
            &snapshots
                .iter()
                .find(|s| s.capability_id == p.key.capability && s.record_family_id == p.key.family)
                .ok_or_else(|| invalid("projection participant unavailable"))?
                .bytes
        };
        crate::graph_object_store::install_graph_object_bytes(root, bytes)?;
    }
    let version = ResearchVersionRecord {
        version_uuid: spec.version_uuid,
        context_uuid: spec.context_uuid,
        label: spec.label.clone(),
        description: spec.description.clone(),
        created_at: spec.created_at,
        content,
    };
    retained_content::inspect(root, &version, None)?;
    let id = insert_version(registry, version)?;
    registry.materialized.insert(id);
    Ok(id)
}

// A research source can contain only native knowledge records. An empty graph
// selection must not invent graph data or copy unrelated domain participants.
fn register_graphless(
    root: &Path,
    registry: &mut ResearchRegistry,
    spec: &RegisterResearchVersion,
    origin: &ResearchVersionRecord,
    snapshots: &[crate::ProjectParticipantSnapshot],
) -> Result<Uuid, GfError> {
    let keys = spec.selection.as_ref().expect("validated selection");
    if keys
        .iter()
        .any(|key| !origin.content.participants.iter().any(|p| &p.key == key))
    {
        return Err(invalid(
            "projection participant selection is outside source content",
        ));
    }
    let mut content = origin.content.clone();
    content.source_version = Some(origin.version_uuid);
    content
        .required_versions
        .clone_from(&spec.required_versions);
    content.evidence.clone_from(&spec.evidence);
    content.producer = PRODUCER.into();
    content.participants.retain(|p| keys.contains(&p.key));
    for participant in &content.participants {
        let snapshot = snapshots
            .iter()
            .find(|p| {
                p.capability_id == participant.key.capability
                    && p.record_family_id == participant.key.family
            })
            .ok_or_else(|| invalid("projection participant unavailable"))?;
        crate::graph_object_store::install_graph_object_bytes(root, &snapshot.bytes)?;
    }
    let version = ResearchVersionRecord {
        version_uuid: spec.version_uuid,
        context_uuid: spec.context_uuid,
        label: spec.label.clone(),
        description: spec.description.clone(),
        created_at: spec.created_at,
        content,
    };
    retained_content::inspect(root, &version, None)?;
    let id = insert_version(registry, version)?;
    registry.materialized.insert(id);
    Ok(id)
}

fn native_selection(selection: &ResearchGraphSelection) -> crate::GraphProjectionSelection {
    crate::GraphProjectionSelection {
        node_uuids: selection.nodes.iter().map(|id| *id.as_bytes()).collect(),
        edge_uuids: selection.edges.iter().map(|id| *id.as_bytes()).collect(),
        closure: if selection.induced_edges {
            crate::GraphProjectionClosure::InducedEdges
        } else {
            crate::GraphProjectionClosure::Referential
        },
        exclude_properties: selection.exclude_properties.clone(),
    }
}

fn validate_scope(
    registry: &ResearchRegistry,
    spec: &RegisterResearchVersion,
    selection: &ResearchGraphSelection,
) -> Result<ResearchVersionRecord, GfError> {
    let origin = spec
        .source_version
        .and_then(|id| registry.versions.get(&id))
        .cloned()
        .ok_or_else(|| invalid("graph projection requires a retained source Version"))?;
    if selection.nodes.len().saturating_add(selection.edges.len()) > 1_000_000 {
        return Err(super::error(
            super::ProjectErrorCode::ResourceLimit,
            "research graph selection exceeds identity limit",
        ));
    }
    if origin.content.generation_uuid != spec.source_generation_uuid
        || spec
            .evidence
            .iter()
            .any(|e| !origin.content.evidence.contains(e))
        || (selection.induced_edges && !selection.edges.is_empty())
        || selection.exclude_properties.iter().any(|field| {
            matches!(
                field.as_str(),
                "node_uuid" | "edge_uuid" | "src_uuid" | "dst_uuid" | "node_id" | "edge_id"
            )
        })
    {
        return Err(invalid("invalid graph projection scope or evidence"));
    }
    Ok(origin)
}

fn source_graph(
    root: &Path,
    graph: &crate::ProjectParticipantSnapshot,
    source: Option<&ResolvedProjectGeneration>,
    target: &Path,
) -> Result<(std::path::PathBuf, u64, u64, u64), GfError> {
    if let Some(source) = source
        && let Some(crate::GraphFilesParticipant::V1(inventory)) =
            source.declared_graph_files_participant()?
    {
        source.graph_files_inventory()?;
        return Ok((source.graph_tree_root(), inventory.total_byte_length, 0, 0));
    }
    let (files, _) =
        retained_content::graph_closure(root, graph.record_version, &graph.bytes, None)?;
    let source_bytes = files.iter().map(|e| e.byte_length).sum();
    // Projection reads core rows, properties and route/catalog controls. Derived
    // ordinal/CSR files are not input and must not cause a whole-parent copy.
    let files: Vec<_> = files
        .into_iter()
        .filter(|e| {
            e.role != crate::GraphFileRole::Index
                && !e.relative_path.starts_with("topology/uuid-membership/")
        })
        .collect();
    let version = if matches!(
        graph.record_version,
        crate::graph_files::GRAPH_FILES_MAPPED_ROOT_RECORD_VERSION
            | crate::graph_files::GRAPH_FILES_MAPPED_RECORD_VERSION
    ) {
        3
    } else {
        1
    };
    let inventory = crate::graph_files::inventory_from_entries_with_version(files, version)?;
    let evidence = crate::materialize_graph_objects(root, &inventory, target)?;
    Ok((
        target.to_path_buf(),
        source_bytes,
        evidence.bytes_copied,
        evidence.bytes_reused,
    ))
}

fn io(error: &std::io::Error) -> GfError {
    GfError::Storage(format!("research projection temporary workspace: {error}"))
}
