//! Import-time publication of the derived adjacency CSR (#1388).

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use graphforge_core::GfError;

use super::{
    PortableV2Error, PortableV2ErrorCode, ProjectFileParticipant, storage, storage_or_cancel,
};

/// Private spill root for the import-time adjacency build, inside the owned
/// stage so failure cleanup reclaims it with everything else.
const IMPORT_ADJACENCY_SPILL_ROOT: &str = ".adjacency-spill";

/// Build the derived adjacency CSR into the verified package graph tree when
/// the package carries none, so the imported generation ships it (#1388)
/// exactly as a construction-published generation does. A complete package of
/// such a generation already carries the index and is installed unchanged;
/// subset exports and packages made before #1388 lack it, and a clean import
/// of those would otherwise rebuild the whole CSR in every query process. The
/// tree is still private here: the compact CAS append that follows hashes and
/// installs the new files like any other package payload.
///
/// Only the compact (v2 / mapped-root) participant contract is covered. A v1
/// inventory participant is verified file-for-file against the package tree
/// at staging, so derived files cannot be added there; that path keeps the
/// lazy rebuild.
///
/// Returns the number of files added to the stage.
pub(super) fn persist_import_adjacency(
    stage: &Path,
    graph_tree: &Path,
    participants: &[ProjectFileParticipant],
    cancelled: Option<&AtomicBool>,
) -> Result<usize, PortableV2Error> {
    let Some(participant) = participants.iter().find(|participant| {
        participant.participant.capability_id == crate::GRAPH_CAPABILITY_ID
            && participant.participant.record_family_id == crate::GRAPH_FILES_FAMILY
    }) else {
        return Ok(0);
    };
    if !matches!(
        participant.participant.record_version,
        crate::GRAPH_FILES_V2_RECORD_VERSION
            | crate::graph_files::GRAPH_FILES_MAPPED_ROOT_RECORD_VERSION
    ) {
        return Ok(0);
    }
    let adjacency = crate::adjacency::adjacency_dir(graph_tree);
    if adjacency.exists() {
        return Ok(0);
    }
    let generation =
        crate::generation::read_topology_generation(graph_tree).map_err(|error| storage(&error))?;
    let edge_files = import_edge_files(graph_tree)?;
    let spill_root = stage.join(IMPORT_ADJACENCY_SPILL_ROOT);
    let options = crate::adjacency::AdjacencyBuildOptions {
        spill_dir: Some(spill_root.clone()),
        ..crate::adjacency::AdjacencyBuildOptions::default()
    };
    // Portable reconstruction has no durable wall-clock build observation.
    // Zero denotes unknown time, making exact package retries byte-identical.
    // Freshness is the topology generation; packaged indexes remain unchanged.
    let built_at_micros = 0;
    // Lifecycle attribution (#1449): the import-side build is construction
    // work, so scope it to the encoding row and keep `read_path_scan` for
    // committed read-path work only.
    let phase_scope = crate::lifecycle_io::PhaseScope::enter(
        crate::StorageIoPhase::EncodeWritePostwriteAuthentication,
    );
    let outcome = crate::adjacency::build_adjacency_index_for_edge_files(
        graph_tree,
        &edge_files,
        generation,
        built_at_micros,
        &options,
        || {
            if cancelled.is_some_and(|flag| flag.load(std::sync::atomic::Ordering::Relaxed)) {
                Err(GfError::Storage("import cancelled".into()))
            } else {
                Ok(())
            }
        },
    );
    drop(phase_scope);
    let _ = fs::remove_dir_all(&spill_root);
    outcome.map_err(|error| storage_or_cancel(&error, cancelled))?;
    let mut added = 0_usize;
    let mut pending = vec![adjacency];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory).map_err(|_| {
            PortableV2Error::new(PortableV2ErrorCode::Io, "cannot inventory staged adjacency")
        })? {
            let entry = entry.map_err(|_| {
                PortableV2Error::new(PortableV2ErrorCode::Io, "cannot inventory staged adjacency")
            })?;
            if entry.path().is_dir() {
                pending.push(entry.path());
            } else {
                added = added.saturating_add(1);
            }
        }
    }
    Ok(added)
}

/// The `(relation, path)` edge tables of a verified package graph tree,
/// resolved through its owned route authority when it carries one.
fn import_edge_files(graph_tree: &Path) -> Result<Vec<(String, PathBuf)>, PortableV2Error> {
    let directory = graphforge_filesystem::StableDirectory::open(graph_tree).map_err(|_| {
        PortableV2Error::new(
            PortableV2ErrorCode::Io,
            "cannot authenticate portable graph tree",
        )
    })?;
    let routes = crate::route_component::owned::read_owned_layout_table(&directory)
        .map_err(|error| storage(&error))?;
    let edges = graph_tree.join("topology").join("edges");
    let mut files = Vec::new();
    if !edges.is_dir() {
        return Ok(files);
    }
    let mut pending = vec![edges];
    while let Some(current) = pending.pop() {
        for entry in fs::read_dir(&current).map_err(|_| {
            PortableV2Error::new(PortableV2ErrorCode::Io, "cannot read portable edge tables")
        })? {
            let path = entry
                .map_err(|_| {
                    PortableV2Error::new(
                        PortableV2ErrorCode::Io,
                        "cannot read portable edge tables",
                    )
                })?
                .path();
            if path.is_dir() {
                pending.push(path);
                continue;
            }
            if path.extension().and_then(|extension| extension.to_str()) != Some("parquet") {
                continue;
            }
            let relative = path
                .strip_prefix(graph_tree)
                .map_err(|_| {
                    PortableV2Error::new(
                        PortableV2ErrorCode::InvalidStructure,
                        "portable edge table escaped the graph tree",
                    )
                })?
                .components()
                .map(|component| match component {
                    std::path::Component::Normal(value) => value.to_str().ok_or_else(|| {
                        PortableV2Error::new(
                            PortableV2ErrorCode::InvalidStructure,
                            "portable edge table name is not UTF-8",
                        )
                    }),
                    _ => Err(PortableV2Error::new(
                        PortableV2ErrorCode::InvalidStructure,
                        "portable edge table path is not normalized",
                    )),
                })
                .collect::<Result<Vec<_>, _>>()?
                .join("/");
            let semantic = match &routes {
                Some(table) => table.semantic_relative_path(&relative),
                None => crate::graph_files::legacy_inventory_logical_text(&relative),
            }
            .map_err(|error| storage(&error))?;
            let Some(route) = crate::route_component::route_position(&semantic)
                .map_err(|error| storage(&error))?
            else {
                continue;
            };
            files.push((route.to_owned(), path));
        }
    }
    files.sort();
    Ok(files)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;
    use std::sync::atomic::AtomicBool;

    use arrow::array::{ArrayRef, FixedSizeBinaryArray, StringArray};
    use arrow::record_batch::RecordBatch;
    use sha2::{Digest, Sha256};
    use std::sync::Arc;
    use uuid::Uuid;

    use super::super::tests::supported;
    use super::*;
    use crate::{PortableV2Limits, import_complete_portable_v2};

    fn fixed(ids: &[Uuid]) -> FixedSizeBinaryArray {
        FixedSizeBinaryArray::try_from_iter(ids.iter().map(|id| id.as_bytes().as_slice())).unwrap()
    }

    /// An initial construction of 8 nodes and 24 `KNOWS` edges, published as
    /// the project's current generation.
    fn construction_published_project() -> tempfile::TempDir {
        let node_ids = (0..8_u128)
            .map(|index| Uuid::from_u128(0x1000 + index))
            .collect::<Vec<_>>();
        let edge_ids = (0..24_u128)
            .map(|index| Uuid::from_u128(0x2000 + index))
            .collect::<Vec<_>>();
        let nodes = RecordBatch::try_new(
            crate::CONSTRUCTION_NODE_SCHEMA.clone(),
            vec![
                Arc::new(fixed(&node_ids)) as ArrayRef,
                Arc::new(StringArray::from(vec!["Person"; node_ids.len()])) as ArrayRef,
            ],
        )
        .unwrap();
        let sources = (0..edge_ids.len())
            .map(|index| node_ids[index % node_ids.len()])
            .collect::<Vec<_>>();
        let targets = (0..edge_ids.len())
            .map(|index| node_ids[(index + 3) % node_ids.len()])
            .collect::<Vec<_>>();
        let edges = RecordBatch::try_new(
            crate::CONSTRUCTION_EDGE_SCHEMA.clone(),
            vec![
                Arc::new(fixed(&edge_ids)) as ArrayRef,
                Arc::new(StringArray::from(vec!["KNOWS"; edge_ids.len()])) as ArrayRef,
                Arc::new(fixed(&sources)) as ArrayRef,
                Arc::new(fixed(&targets)) as ArrayRef,
            ],
        )
        .unwrap();
        let source = tempfile::tempdir().unwrap();
        crate::open_or_initialize_project(source.path()).unwrap();
        let mut session = crate::GraphConstructionSession::open_with_mode(
            source.path(),
            Uuid::from_u128(0x31),
            0,
            graphforge_core::OntologyMode::Exploratory,
            crate::GraphConstructionBudgets::default(),
        )
        .unwrap();
        session
            .append(crate::ConstructionChunkKind::Node, "nodes", &nodes)
            .unwrap();
        session
            .append(crate::ConstructionChunkKind::Edge, "edges", &edges)
            .unwrap();
        session.seal().unwrap();
        let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
        let encoding = session.encode_canonical(&shape, 1).unwrap();
        session
            .publish_canonical(&encoding, Uuid::from_u128(0x32), Uuid::from_u128(0x33))
            .unwrap();
        source
    }

    fn index_entries(inventory: &crate::GraphFilesInventory) -> Vec<String> {
        inventory
            .files
            .iter()
            .filter(|entry| entry.role == crate::GraphFileRole::Index)
            .map(|entry| entry.relative_path.clone())
            .collect()
    }

    /// The materialized tree at `root` carries a manifest stamped with its own
    /// topology generation and a CSR that validates against its topology.
    fn assert_index_current(root: &Path) {
        let topology_generation = crate::read_topology_generation(root).unwrap();
        let rows = crate::adjacency::read_manifest(root).unwrap();
        assert!(!rows.is_empty());
        assert!(
            rows.iter()
                .all(|row| row.topology_generation == topology_generation)
        );
        assert!(
            crate::adjacency::validate_adjacency_index(root)
                .unwrap()
                .is_empty()
        );
    }

    fn export_complete(source: &Path) -> (tempfile::TempDir, std::path::PathBuf) {
        let generation = crate::resolve_project_generation(source).unwrap();
        let package_parent = tempfile::tempdir().unwrap();
        let package = package_parent.path().join("adjacency.gfproject");
        let limits = crate::PortableV2ExportLimits::default();
        let plan = crate::plan_complete_portable_v2(&generation, limits).unwrap();
        crate::export_complete_portable_v2(
            &plan,
            &package,
            crate::PortableV2Output::Expanded,
            limits,
            &AtomicBool::new(false),
            |_| {},
        )
        .unwrap();
        (package_parent, package)
    }

    /// A complete package of a construction-published generation carries its
    /// adjacency CSR, and the import must publish it unchanged rather than
    /// building a second one (#1388).
    #[test]
    fn complete_import_publishes_the_derived_adjacency_index() {
        let source = construction_published_project();
        let generation = crate::resolve_project_generation(source.path()).unwrap();
        let source_index = index_entries(&generation.graph_files_inventory().unwrap().unwrap());
        assert!(source_index.contains(&"indexes/adjacency/index_manifest.parquet".to_owned()));

        let (_package_parent, package) = export_complete(source.path());
        assert!(
            package
                .join("data/components/graph-data/graph-tree/indexes/adjacency")
                .is_dir(),
            "a complete package carries the generation's derived index"
        );

        let target = tempfile::tempdir().unwrap();
        import_complete_portable_v2(
            &package,
            target.path(),
            Uuid::from_u128(0x34),
            Uuid::from_u128(0x35),
            &supported(),
            PortableV2Limits::default(),
            None,
        )
        .unwrap();
        let imported = crate::resolve_project_generation(target.path()).unwrap();
        let inventory = imported.graph_files_inventory().unwrap().unwrap();
        assert_eq!(index_entries(&inventory), source_index);
        let workspace = tempfile::tempdir().unwrap();
        crate::materialize_graph_objects(imported.container_root(), &inventory, workspace.path())
            .unwrap();
        assert_index_current(workspace.path());
    }

    /// A package without the index (a subset export, or one made before
    /// #1388) gets it built into the verified stage tree before the compact
    /// CAS append, so the imported generation still ships it.
    #[test]
    fn import_builds_the_index_a_package_lacks_and_never_twice() {
        let source = construction_published_project();
        let generation = crate::resolve_project_generation(source.path()).unwrap();
        let inventory = generation.graph_files_inventory().unwrap().unwrap();
        let stage = tempfile::tempdir().unwrap();
        let tree = stage.path().join("graph-tree");
        fs::create_dir(&tree).unwrap();
        crate::materialize_graph_objects(generation.container_root(), &inventory, &tree).unwrap();
        fs::remove_dir_all(tree.join("indexes")).unwrap();
        let placeholder = crate::graph_files_root_participant(&crate::GraphFilesRootV2 {
            format: "graphforge-graph-files-root".into(),
            format_version: crate::graph_files::GRAPH_FILES_MAPPED_ROOT_RECORD_VERSION,
            root_node_sha256: "0".repeat(64),
            logical_file_count: 0,
            logical_byte_length: 0,
        })
        .unwrap();
        let participants = vec![ProjectFileParticipant {
            participant: placeholder.clone(),
            source: stage.path().join("graph-files.json"),
            byte_length: placeholder.bytes.len() as u64,
            content_sha256: Sha256::digest(&placeholder.bytes).into(),
        }];
        let added = persist_import_adjacency(stage.path(), &tree, &participants, None).unwrap();
        assert_eq!(added, index_entries(&inventory).len());
        assert!(!stage.path().join(IMPORT_ADJACENCY_SPILL_ROOT).exists());
        assert_index_current(&tree);
        assert_eq!(
            persist_import_adjacency(stage.path(), &tree, &participants, None).unwrap(),
            0
        );
        let first = crate::capture_graph_files(&tree).unwrap().0;
        fs::remove_dir_all(tree.join("indexes")).unwrap();
        persist_import_adjacency(stage.path(), &tree, &participants, None).unwrap();
        assert_eq!(crate::capture_graph_files(&tree).unwrap().0, first);
    }
    fn indexless_package() -> (tempfile::TempDir, PathBuf) {
        let source = construction_published_project();
        let generation = crate::resolve_project_generation(source.path()).unwrap();
        let stage = tempfile::tempdir().unwrap();
        let tree = stage.path().join("graph-tree");
        fs::create_dir(&tree).unwrap();
        crate::materialize_graph_objects(
            generation.container_root(),
            &generation.graph_files_inventory().unwrap().unwrap(),
            &tree,
        )
        .unwrap();
        fs::remove_dir_all(tree.join("indexes")).unwrap();
        let placeholder = crate::graph_files_root_participant(&crate::GraphFilesRootV2 {
            format: "graphforge-graph-files-root".into(),
            format_version: crate::graph_files::GRAPH_FILES_MAPPED_ROOT_RECORD_VERSION,
            root_node_sha256: "0".repeat(64),
            logical_file_count: 0,
            logical_byte_length: 0,
        })
        .unwrap();
        let path = stage.path().join("graph-files.json");
        fs::write(&path, &placeholder.bytes).unwrap();
        let mut files = vec![ProjectFileParticipant {
            source: path,
            byte_length: placeholder.bytes.len() as u64,
            content_sha256: Sha256::digest(&placeholder.bytes).into(),
            participant: placeholder,
        }];
        let lease = super::super::prepare_compact_import_graph(
            source.path(),
            Some(&tree),
            &mut files,
            1024,
        )
        .unwrap()
        .unwrap();
        let mut participants = crate::empty_workspace_participants().unwrap();
        let mut graph = files.remove(0);
        graph.participant.bytes = fs::read(&graph.source).unwrap();
        participants.push(graph.participant);
        let request = crate::ProjectGenerationRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
            capabilities: supported(),
            participants,
        };
        let crate::ProjectStageOutcome::Staged(staged) =
            crate::stage_project_generation(source.path(), &request).unwrap()
        else {
            panic!("fresh fixture must stage")
        };
        staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .unwrap()
            .publish_with_graph_objects(&lease)
            .unwrap();
        drop(lease);
        export_complete(source.path())
    }

    #[test]
    fn reconstructed_adjacency_import_replays_and_changed_generation_conflicts() {
        let (_owner, package) = indexless_package();
        assert!(
            !package
                .join("data/components/graph-data/graph-tree/indexes")
                .exists()
        );
        let target = tempfile::tempdir().unwrap();
        let transaction = Uuid::now_v7();
        let generation = Uuid::now_v7();
        let import = |generation| {
            import_complete_portable_v2(
                &package,
                target.path(),
                transaction,
                generation,
                &supported(),
                PortableV2Limits::default(),
                None,
            )
        };
        let first = import(generation).unwrap();
        let before = crate::resolve_project_generation(target.path())
            .unwrap()
            .graph_files_inventory()
            .unwrap()
            .unwrap();
        let replay = import(generation).unwrap();
        assert!(replay.publication.idempotent_replay);
        assert_eq!(
            first.publication.generation_uuid,
            replay.publication.generation_uuid
        );
        assert_eq!(first.package_digest, replay.package_digest);
        assert_eq!(first.transport_digest, replay.transport_digest);
        assert_eq!(
            import(Uuid::now_v7()).unwrap_err().code,
            PortableV2ErrorCode::ConcurrentMutation,
        );
        let changed_source = construction_published_project();
        let (_changed_owner, changed_package) = export_complete(changed_source.path());
        let changed = import_complete_portable_v2(
            &changed_package,
            target.path(),
            transaction,
            generation,
            &supported(),
            PortableV2Limits::default(),
            None,
        )
        .unwrap_err();
        assert_eq!(changed.code, PortableV2ErrorCode::ConcurrentMutation);
        let reopened = crate::resolve_project_generation(target.path()).unwrap();
        assert_eq!(reopened.generation_uuid(), generation);
        assert_eq!(reopened.graph_files_inventory().unwrap().unwrap(), before);
        let tree = tempfile::tempdir().unwrap();
        crate::materialize_graph_objects(target.path(), &before, tree.path()).unwrap();
        assert_index_current(tree.path());
        assert!(
            crate::adjacency::read_manifest(tree.path())
                .unwrap()
                .iter()
                .all(|row| row.built_at_micros == 0)
        );
    }
}
