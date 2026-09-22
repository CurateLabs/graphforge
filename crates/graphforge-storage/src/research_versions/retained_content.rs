//! Authenticated CAS placement of immutable research content.
use super::{
    BTreeSet, Digest, GfError, Path, ProjectErrorCode, ResearchRegistry, ResearchVersionRecord,
    Sha256, Uuid, error, hex, inspect_research_version, invalid, read_research_registry,
};

const MAX_PARTICIPANT_BYTES: u64 = 256 * 1024 * 1024;

pub(super) fn compact(
    root: &Path,
    registry: &mut ResearchRegistry,
    versions: &BTreeSet<Uuid>,
) -> Result<(), GfError> {
    if versions
        .iter()
        .any(|id| !registry.versions.contains_key(id))
    {
        return Err(invalid("compaction requires retained research Versions"));
    }
    for id in versions {
        if registry.materialized.contains(id) {
            inspect(root, &registry.versions[id], None)?;
            continue;
        }
        let version = &registry.versions[id];
        let source = crate::resolve_generation_by_uuid(root, version.content.generation_uuid)?;
        if source.manifest_sha256() != version.content.manifest_sha256 {
            return Err(invalid("compaction source identity changed"));
        }
        for p in &version.content.participants {
            let path = source.participant_path(&p.key.capability, &p.key.family)?;
            if std::fs::metadata(&path)
                .map_err(|_| invalid("missing retained participant"))?
                .len()
                > MAX_PARTICIPANT_BYTES
            {
                return Err(error(
                    ProjectErrorCode::ResourceLimit,
                    "research participant exceeds materialization limit",
                ));
            }
            let snapshot = source
                .participant_snapshot(&p.key.capability, &p.key.family)?
                .ok_or_else(|| invalid("missing retained participant"))?;
            if <[u8; 32]>::from(Sha256::digest(&snapshot.bytes)) != p.content_sha256 {
                return Err(invalid("retained participant identity changed"));
            }
            crate::graph_object_store::install_graph_object_bytes(root, &snapshot.bytes)?;
            if p.key.capability == "graph" && p.key.family == "files" {
                let inventory = source
                    .graph_files_inventory()?
                    .ok_or_else(|| invalid("retained graph inventory unavailable"))?;
                if matches!(
                    source.declared_graph_files_participant()?,
                    Some(crate::GraphFilesParticipant::V1(_))
                ) {
                    for entry in inventory.files {
                        let path = crate::graph_files::resolve_v1_inventory_entry(
                            &source.graph_tree_root(),
                            &entry,
                        )?;
                        crate::graph_object_store::install_graph_object_file(
                            root,
                            &path,
                            &entry.content_sha256,
                            entry.byte_length,
                        )?;
                    }
                }
            }
        }
        inspect(root, version, None)?;
        registry.materialized.insert(*id);
    }
    Ok(())
}

pub(super) fn inspect(
    root: &Path,
    version: &ResearchVersionRecord,
    guard: Option<&crate::graph_object_store::GraphObjectGcGuard>,
) -> Result<Vec<crate::ProjectParticipantSnapshot>, GfError> {
    for evidence in &version.content.evidence {
        if let super::ResearchEvidenceReference::Local {
            sha256,
            byte_length,
            ..
        } = evidence
        {
            verify(root, guard, &hex(sha256), *byte_length)?;
        }
    }
    let mut snapshots = Vec::new();
    let mut total = 0_u64;
    for p in &version.content.participants {
        let bytes = read(root, guard, &hex(&p.content_sha256), MAX_PARTICIPANT_BYTES)?;
        total = total
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| invalid("retained participant length overflow"))?;
        if total > MAX_PARTICIPANT_BYTES {
            return Err(error(
                ProjectErrorCode::ResourceLimit,
                "research materialization byte limit exceeded",
            ));
        }
        if p.key.capability == "graph" && p.key.family == "files" {
            let (files, _) = graph_closure(root, p.record_version, &bytes, guard)?;
            for entry in files {
                verify(root, guard, &entry.content_sha256, entry.byte_length)?;
            }
        }
        snapshots.push(crate::ProjectParticipantSnapshot {
            capability_id: p.key.capability.clone(),
            capability_version: p.capability_version,
            record_family_id: p.key.family.clone(),
            record_version: p.record_version,
            encoding: p.encoding.clone(),
            schema_fingerprint: p.schema_sha256,
            row_count: p.row_count,
            bytes,
        });
    }
    Ok(snapshots)
}

pub(super) fn object_roots(
    root: &Path,
    version: &ResearchVersionRecord,
    guard: Option<&crate::graph_object_store::GraphObjectGcGuard>,
) -> Result<BTreeSet<String>, GfError> {
    let snapshots = inspect(root, version, guard)?;
    let mut objects: BTreeSet<_> = version
        .content
        .participants
        .iter()
        .map(|p| hex(&p.content_sha256))
        .collect();
    for p in snapshots {
        if p.capability_id == "graph" && p.record_family_id == "files" {
            let (files, nodes) = graph_closure(root, p.record_version, &p.bytes, guard)?;
            objects.extend(nodes);
            objects.extend(files.into_iter().map(|e| e.content_sha256));
        }
    }
    Ok(objects)
}

pub(super) fn graph_closure(
    root: &Path,
    record_version: u32,
    bytes: &[u8],
    guard: Option<&crate::graph_object_store::GraphObjectGcGuard>,
) -> Result<(Vec<crate::GraphFileEntry>, BTreeSet<String>), GfError> {
    let mut objects = BTreeSet::new();
    if let crate::GraphFilesParticipant::V2(graph) =
        crate::graph_files::decode_versioned_graph_files_participant(record_version, bytes)?
    {
        let (files, _) = crate::resolve_graph_manifest(
            &graph,
            crate::GraphManifestLimits::default(),
            |digest| {
                objects.insert(digest.to_owned());
                read(
                    root,
                    guard,
                    digest,
                    crate::graph_manifest::GRAPH_MANIFEST_NODE_MAX_BYTES,
                )
            },
        )?;
        Ok((files, objects))
    } else {
        Ok((crate::graph_files::decode_inventory(bytes)?.files, objects))
    }
}

/// Materialize only a retained Version's graph into an empty private directory.
/// The Project is read-only; no live source or genealogy expansion is attempted.
pub fn materialize_research_graph(
    root: &Path,
    version: &ResearchVersionRecord,
    target: &Path,
) -> Result<(), GfError> {
    let current = crate::resolve_project_generation(root)?;
    let registry = read_research_registry(&current)?;
    let snapshots = inspect_research_version(root, version)?;
    let graph = snapshots
        .iter()
        .find(|p| p.capability_id == "graph" && p.record_family_id == "files")
        .ok_or_else(|| invalid("retained Version has no graph"))?;
    if target
        .canonicalize()
        .map_err(|error| io(&error))?
        .starts_with(root.canonicalize().map_err(|error| io(&error))?)
    {
        return Err(invalid(
            "historical graph target must be outside the Project",
        ));
    }
    let metadata = std::fs::symlink_metadata(target).map_err(|error| io(&error))?;
    if !metadata.is_dir()
        || metadata.file_type().is_symlink()
        || std::fs::read_dir(target)
            .map_err(|error| io(&error))?
            .next()
            .is_some()
    {
        return Err(invalid(
            "historical graph target must be an empty private directory",
        ));
    }
    if !registry.materialized.contains(&version.version_uuid) {
        let source = crate::resolve_generation_by_uuid(root, version.content.generation_uuid)?;
        if let Some(crate::GraphFilesParticipant::V1(inventory)) =
            source.declared_graph_files_participant()?
        {
            crate::materialize_graph_tree(&source.graph_tree_root(), &inventory, target)?;
            return Ok(());
        }
    }
    let (files, _) = graph_closure(root, graph.record_version, &graph.bytes, None)?;
    let inventory_version = if matches!(
        graph.record_version,
        crate::GRAPH_FILES_MAPPED_RECORD_VERSION | crate::GRAPH_FILES_MAPPED_ROOT_RECORD_VERSION
    ) {
        crate::GRAPH_FILES_MAPPED_RECORD_VERSION
    } else {
        crate::GRAPH_FILES_RECORD_VERSION
    };
    let inventory =
        crate::graph_files::inventory_from_entries_with_version(files, inventory_version)?;
    let routes =
        crate::route_component::materialize::MaterializationRoutes::prepare(&inventory, |entry| {
            crate::graph_object_store::read_graph_object(
                root,
                &entry.content_sha256,
                entry.byte_length,
            )
        })?;
    for (entry, destination) in inventory.files.iter().zip(&routes.destinations) {
        let mut reader =
            crate::open_graph_object_by_digest(root, &entry.content_sha256, entry.byte_length)?;
        std::io::Seek::rewind(&mut reader).map_err(|error| io(&error))?;
        let path = target.join(destination);
        std::fs::create_dir_all(
            path.parent()
                .ok_or_else(|| invalid("invalid historical graph path"))?,
        )
        .map_err(|error| io(&error))?;
        let mut output = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|error| io(&error))?;
        std::io::copy(&mut reader, &mut output).map_err(|error| io(&error))?;
        output.sync_all().map_err(|error| io(&error))?;
    }
    routes.install_table(target, &mut crate::GraphFilesOpenEvidence::default())?;
    Ok(())
}

fn io(error: &std::io::Error) -> GfError {
    GfError::Storage(format!("historical research materialization: {error}"))
}

pub(super) fn install_graph(
    lease: &crate::GraphObjectPublicationLease,
    workspace: &Path,
    inventory: &crate::GraphFilesInventory,
) -> Result<crate::GraphFilesRootV2, GfError> {
    if inventory.format_version == crate::GRAPH_FILES_MAPPED_RECORD_VERSION {
        let routes = crate::graph_files::authenticate_route_table(workspace, inventory)?;
        let paths = inventory
            .files
            .iter()
            .map(|e| std::path::PathBuf::from(&e.relative_path))
            .collect::<Vec<_>>();
        crate::graph_object_store::append_mapped_import_graph_files(
            lease, workspace, &paths, &routes,
        )
        .map(|v| v.0)
    } else {
        crate::graph_object_store::migrate_graph_files_v1_to_v2(lease, workspace, inventory)
            .map(|v| v.0)
    }
}

fn read(
    root: &Path,
    guard: Option<&crate::graph_object_store::GraphObjectGcGuard>,
    digest: &str,
    limit: u64,
) -> Result<Vec<u8>, GfError> {
    match guard {
        Some(guard) => guard.read_research_object(digest, limit),
        None => crate::read_graph_object_by_digest(root, digest, limit),
    }
}
fn verify(
    root: &Path,
    guard: Option<&crate::graph_object_store::GraphObjectGcGuard>,
    digest: &str,
    length: u64,
) -> Result<(), GfError> {
    match guard {
        Some(guard) => guard.verify_research_object(digest, length),
        None => crate::verify_graph_object(root, digest, length),
    }
}
