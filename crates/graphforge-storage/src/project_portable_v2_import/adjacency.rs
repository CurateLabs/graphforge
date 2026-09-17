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
    let built_at_micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            i64::try_from(elapsed.as_micros()).unwrap_or(i64::MAX)
        });
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
    use std::sync::atomic::AtomicBool;

    use sha2::{Digest, Sha256};
    use uuid::Uuid;

    use super::super::tests::supported;
    use super::*;
    use crate::{PortableV2Limits, import_complete_portable_v2};

    /// A complete package of a construction-published generation carries its
    /// adjacency CSR, and the import must publish it unchanged rather than
    /// building a second one (#1388).
    #[test]
    fn complete_import_publishes_the_derived_adjacency_index() {
        use arrow::array::{ArrayRef, FixedSizeBinaryArray, StringArray};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let node_ids = (0..8_u128)
            .map(|index| Uuid::from_u128(0x1000 + index))
            .collect::<Vec<_>>();
        let edge_ids = (0..24_u128)
            .map(|index| Uuid::from_u128(0x2000 + index))
            .collect::<Vec<_>>();
        let fixed = |ids: &[Uuid]| {
            FixedSizeBinaryArray::try_from_iter(ids.iter().map(|id| id.as_bytes().as_slice()))
                .unwrap()
        };
        let nodes = RecordBatch::try_new(
            crate::CONSTRUCTION_NODE_SCHEMA.clone(),
            vec![
                Arc::new(fixed(&node_ids)) as ArrayRef,
                Arc::new(StringArray::from(vec!["Person"; node_ids.len()])) as ArrayRef,
            ],
        )
        .unwrap();
        let sources = edge_ids
            .iter()
            .enumerate()
            .map(|(index, _)| node_ids[index % node_ids.len()])
            .collect::<Vec<_>>();
        let targets = edge_ids
            .iter()
            .enumerate()
            .map(|(index, _)| node_ids[(index + 3) % node_ids.len()])
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
        drop(session);
        let generation = crate::resolve_project_generation(source.path()).unwrap();
        let index_entries = |inventory: &crate::GraphFilesInventory| {
            inventory
                .files
                .iter()
                .filter(|entry| entry.role == crate::GraphFileRole::Index)
                .map(|entry| entry.relative_path.clone())
                .collect::<Vec<_>>()
        };
        let source_index = index_entries(&generation.graph_files_inventory().unwrap().unwrap());
        assert!(source_index.contains(&"indexes/adjacency/index_manifest.parquet".to_owned()));

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
        let imported_index = index_entries(&inventory);
        assert_eq!(imported_index, source_index);

        let workspace = tempfile::tempdir().unwrap();
        crate::materialize_graph_objects(imported.container_root(), &inventory, workspace.path())
            .unwrap();
        let topology_generation = crate::read_topology_generation(workspace.path()).unwrap();
        let rows = crate::adjacency::read_manifest(workspace.path()).unwrap();
        assert!(!rows.is_empty());
        assert!(
            rows.iter()
                .all(|row| row.topology_generation == topology_generation)
        );
        assert!(
            crate::adjacency::validate_adjacency_index(workspace.path())
                .unwrap()
                .is_empty()
        );

        // A package without the index (a subset export, or one made before
        // #1388) gets it built into the verified stage tree before the compact
        // CAS append, so the imported generation still ships it.
        let stage = tempfile::tempdir().unwrap();
        let tree = stage.path().join("graph-tree");
        fs::create_dir(&tree).unwrap();
        crate::materialize_graph_objects(imported.container_root(), &inventory, &tree).unwrap();
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
        assert_eq!(added, imported_index.len());
        assert!(!stage.path().join(IMPORT_ADJACENCY_SPILL_ROOT).exists());
        let rows = crate::adjacency::read_manifest(&tree).unwrap();
        assert!(
            rows.iter()
                .all(|row| row.topology_generation == topology_generation)
        );
        assert!(
            crate::adjacency::validate_adjacency_index(&tree)
                .unwrap()
                .is_empty()
        );
        // Already present: nothing is added twice.
        assert_eq!(
            persist_import_adjacency(stage.path(), &tree, &participants, None).unwrap(),
            0
        );
    }
}
