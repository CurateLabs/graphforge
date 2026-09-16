use super::super::migration::migrated_semantic_relative;
use super::super::tests::compiled;
use super::super::*;
use super::*;

fn legacy_migration_fixture() -> (tempfile::TempDir, CompiledComposition) {
    use std::collections::HashMap;

    let composition = compiled("1");
    let dir = tempfile::TempDir::new().unwrap();
    let mut writer =
        crate::GraphWriter::open_at(dir.path(), graphforge_core::OntologyMode::Strict, 1).unwrap();
    let left = graphforge_core::uuid::new_v7();
    let right = graphforge_core::uuid::new_v7();
    writer
        .create_node(
            left,
            graphforge_value::EntityTypeId::ontology(graphforge_core::TypeId(0)).unwrap(),
        )
        .unwrap();
    writer
        .create_node(
            right,
            graphforge_value::EntityTypeId::ontology(graphforge_core::TypeId(0)).unwrap(),
        )
        .unwrap();
    let edge = graphforge_core::uuid::new_v7();
    writer.create_edge(edge, "KNOWS", &left, &right).unwrap();
    writer
        .set_edge_properties(
            &edge,
            Some("KNOWS"),
            HashMap::from([("since".into(), graphforge_ir::IrLiteral::Int(2020))]),
        )
        .unwrap();
    writer.flush().unwrap();

    drop(writer);
    (dir, composition)
}

#[test]
fn unambiguous_legacy_routes_rewrite_with_metadata_and_reopen() {
    let (dir, composition) = legacy_migration_fixture();
    let projection =
        SemanticStorageBindings::project_legacy_unambiguous(&composition, dir.path()).unwrap();
    assert!(!projection.route_moves.is_empty());
    let mut migration =
        apply_legacy_route_moves(dir.path(), &projection.route_moves, &projection.bindings)
            .unwrap();
    projection
        .bindings
        .validate_physical_routes(dir.path())
        .unwrap();
    migration.commit();
    drop(migration);
    assert!(!dir.path().join("topology/edges/KNOWS.parquet").exists());
    projection
        .bindings
        .validate_physical_routes(dir.path())
        .unwrap();
}

#[test]
fn legacy_raw_layout_admission_is_preserved_after_semantic_rollback() {
    let (dir, composition) = legacy_migration_fixture();
    let mapped = crate::capture_graph_files(dir.path()).unwrap().0;
    let table = crate::graph_files::authenticate_route_table(dir.path(), &mapped).unwrap();
    // Produce an actual legacy raw layout; no inventory is relabeled.
    for entry in &mapped.files {
        if entry.relative_path == crate::route_component::TABLE_FILE {
            continue;
        }
        let logical = table.semantic_relative_path(&entry.relative_path).unwrap();
        if logical != entry.relative_path {
            let source = dir.path().join(&entry.relative_path);
            let destination = dir.path().join(logical);
            std::fs::create_dir_all(destination.parent().unwrap()).unwrap();
            std::fs::rename(&source, &destination).unwrap();
            let _ = std::fs::remove_dir(source.parent().unwrap());
        }
    }
    std::fs::remove_file(dir.path().join(crate::route_component::TABLE_FILE)).unwrap();
    let raw = crate::capture_graph_files(dir.path()).unwrap().0;
    assert_eq!(
        raw.format_version,
        crate::graph_files::GRAPH_FILES_RECORD_VERSION
    );
    let projection =
        SemanticStorageBindings::project_legacy_unambiguous(&composition, dir.path()).unwrap();
    assert_eq!(crate::capture_graph_files(dir.path()).unwrap().0, raw);
    drop(
        apply_legacy_route_moves(dir.path(), &projection.route_moves, &projection.bindings)
            .unwrap(),
    );
    let admitted = crate::capture_graph_files(dir.path()).unwrap().0;
    assert_eq!(
        admitted.format_version,
        crate::graph_files::GRAPH_FILES_MAPPED_RECORD_VERSION
    );
    let table = crate::graph_files::authenticate_route_table(dir.path(), &admitted).unwrap();
    table
        .validate_paths(
            admitted
                .files
                .iter()
                .map(|entry| entry.relative_path.as_str()),
        )
        .unwrap();
    for prior in &raw.files {
        if !prior.relative_path.ends_with(".parquet") {
            continue;
        }
        let current = admitted
            .files
            .iter()
            .find(|entry| {
                table.semantic_relative_path(&entry.relative_path).unwrap() == prior.relative_path
            })
            .unwrap();
        assert_eq!(current.content_sha256, prior.content_sha256);
        assert_eq!(current.byte_length, prior.byte_length);
    }
}

#[test]
fn legacy_migration_rollback_restores_admitted_table_and_refuses_foreign_table() {
    let (dir, composition) = legacy_migration_fixture();
    let baseline = crate::capture_graph_files(dir.path()).unwrap().0;
    let projection =
        SemanticStorageBindings::project_legacy_unambiguous(&composition, dir.path()).unwrap();
    assert_eq!(
        crate::capture_graph_files(dir.path()).unwrap().0,
        baseline,
        "planning is read-only"
    );
    let migration =
        apply_legacy_route_moves(dir.path(), &projection.route_moves, &projection.bindings)
            .unwrap();
    let transformed = crate::capture_graph_files(dir.path()).unwrap().0;
    let table = crate::graph_files::authenticate_route_table(dir.path(), &transformed).unwrap();
    table
        .validate_paths(
            transformed
                .files
                .iter()
                .map(|entry| entry.relative_path.as_str()),
        )
        .unwrap();
    assert!(
        transformed
            .files
            .iter()
            .filter(|entry| entry.relative_path != crate::route_component::TABLE_FILE)
            .all(|entry| !table
                .semantic_relative_path(&entry.relative_path)
                .unwrap()
                .contains("/KNOWS"))
    );
    drop(migration);
    assert_eq!(crate::capture_graph_files(dir.path()).unwrap().0, baseline);

    let migration =
        apply_legacy_route_moves(dir.path(), &projection.route_moves, &projection.bindings)
            .unwrap();
    let table_path = dir.path().join(crate::route_component::TABLE_FILE);
    let foreign = b"foreign replacement must be retained";
    std::fs::rename(&table_path, dir.path().join("retained-table.outside-graph")).unwrap();
    std::fs::write(&table_path, foreign).unwrap();
    let files = migration
        .completed
        .iter()
        .map(|(_, new, _)| (new.clone(), std::fs::read(new).unwrap()))
        .collect::<Vec<_>>();
    drop(migration);
    assert_eq!(std::fs::read(&table_path).unwrap(), foreign);
    for (path, bytes) in files {
        assert_eq!(std::fs::read(path).unwrap(), bytes);
    }
}

#[test]
fn semantic_route_moves_preserve_every_shard_basename() {
    let moves = BTreeMap::from([("s-old".to_owned(), "s-new".to_owned())]);
    for (source, target) in [
        (
            "properties/s-old/00000000000000000001.parquet",
            "properties/s-new/00000000000000000001.parquet",
        ),
        (
            "edge_properties/s-old/00000000000000000002.parquet",
            "edge_properties/s-new/00000000000000000002.parquet",
        ),
        (
            "topology/edges/s-old/00000000000000000003.parquet",
            "topology/edges/s-new/00000000000000000003.parquet",
        ),
        ("properties/s-old.parquet", "properties/s-new.parquet"),
    ] {
        assert_eq!(
            migrated_semantic_relative(Path::new(source), &moves),
            PathBuf::from(target)
        );
    }
}
