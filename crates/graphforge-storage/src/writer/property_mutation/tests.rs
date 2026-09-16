use super::*;
use crate::writer::BTreeMap;
use crate::writer::EntityTypeId;
use crate::writer::GfError;
use crate::writer::GraphWriter;
use crate::writer::HashMap;
use crate::writer::HashSet;
use crate::writer::IrLiteral;
use crate::writer::OntologyMode;
use crate::writer::ProjectErrorCode;
use crate::writer::RewriteBatch;
use crate::writer::fs;
use crate::writer::property_snapshots_to_batch;
use crate::writer::read_entity_properties;
use crate::writer::read_node_property_rows;
use crate::writer::tests::TS;
use crate::writer::tests::read_edge_props;
use crate::writer::tests::read_node_props;
use crate::writer::to_bytes;
use graphforge_core::uuid::new_v7;
use std::fs::File;
use tempfile::TempDir;

#[test]
fn set_node_properties_sets_new_and_overwrites_existing() {
    let dir = TempDir::new().unwrap();
    assert!(
        read_node_property_rows(dir.path(), "_untyped")
            .unwrap()
            .is_empty()
    );
    fs::create_dir_all(dir.path().join("properties")).unwrap();
    fs::write(dir.path().join("properties/_untyped.parquet"), b"invalid").unwrap();
    assert!(read_node_property_rows(dir.path(), "_untyped").is_err());
    fs::remove_file(dir.path().join("properties/_untyped.parquet")).unwrap();
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    let a = new_v7();
    w.create_node(a, EntityTypeId::decode(0).unwrap()).unwrap();
    w.set_properties(
        &a,
        None,
        HashMap::from([("age".to_owned(), IrLiteral::Int(30))]),
    )
    .unwrap();
    w.flush().unwrap();

    let ab = to_bytes(&a);
    let search_generation = crate::generation::read_search_generation(dir.path()).unwrap();
    // Overwrite `age` and add a new `name`.
    let updates = HashMap::from([(
        ab,
        HashMap::from([
            ("age".to_owned(), IrLiteral::Int(31)),
            ("name".to_owned(), IrLiteral::Str("Al".to_owned())),
        ]),
    )]);
    let touched = set_node_properties(dir.path(), "_untyped", &updates).unwrap();
    assert_eq!(touched, 1);
    assert_eq!(
        crate::generation::read_search_generation(dir.path()).unwrap(),
        search_generation + 1
    );

    let props = read_node_props(dir.path(), "_untyped");
    assert_eq!(props[&ab]["age"], IrLiteral::Int(31));
    assert_eq!(props[&ab]["name"], IrLiteral::Str("Al".to_owned()));
}

#[test]
fn set_node_properties_inserts_row_for_propertyless_node() {
    // A node with no property row yet must get a fresh row on SET.
    let dir = TempDir::new().unwrap();
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    let a = new_v7();
    w.create_node(a, EntityTypeId::decode(0).unwrap()).unwrap();
    w.flush().unwrap(); // no properties written → no _untyped file

    let ab = to_bytes(&a);
    let updates = HashMap::from([(ab, HashMap::from([("age".to_owned(), IrLiteral::Int(42))]))]);
    let touched = set_node_properties(dir.path(), "_untyped", &updates).unwrap();
    assert_eq!(touched, 1);

    let props = read_node_props(dir.path(), "_untyped");
    assert_eq!(props[&ab]["age"], IrLiteral::Int(42));
}

#[test]
fn set_node_properties_routes_by_stem_in_strict_mode() {
    // Strict/Advisory route to properties/<Entity>.parquet, not _untyped.
    let dir = TempDir::new().unwrap();
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
    let a = new_v7();
    w.create_node(a, EntityTypeId::decode(1).unwrap()).unwrap();
    w.set_properties(
        &a,
        Some("Person"),
        HashMap::from([("age".to_owned(), IrLiteral::Int(30))]),
    )
    .unwrap();
    w.flush().unwrap();

    let ab = to_bytes(&a);
    let updates = HashMap::from([(ab, HashMap::from([("age".to_owned(), IrLiteral::Int(99))]))]);
    set_node_properties(dir.path(), "Person", &updates).unwrap();

    assert!(
        !crate::property_overlay::enumerate_property_fragments(
            dir.path(),
            crate::property_overlay::PropertyRouteKind::Node,
            &crate::route_component::component("Person"),
        )
        .unwrap()
        .is_empty()
    );
    let props = read_node_props(dir.path(), "Person");
    assert_eq!(props[&ab]["age"], IrLiteral::Int(99));
}

#[test]
fn remove_node_properties_drops_key_and_column_when_last() {
    let dir = TempDir::new().unwrap();
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    let a = new_v7();
    w.create_node(a, EntityTypeId::decode(0).unwrap()).unwrap();
    w.set_properties(
        &a,
        None,
        HashMap::from([("age".to_owned(), IrLiteral::Int(30))]),
    )
    .unwrap();
    w.flush().unwrap();

    let ab = to_bytes(&a);
    let search_generation = crate::generation::read_search_generation(dir.path()).unwrap();
    let removals = HashMap::from([(ab, HashSet::from(["age".to_owned()]))]);
    let touched = remove_node_properties(dir.path(), "_untyped", &removals).unwrap();
    assert_eq!(touched, 1);
    assert_eq!(
        crate::generation::read_search_generation(dir.path()).unwrap(),
        search_generation + 1
    );

    // The only property was removed → the row's map is empty and the `age`
    // column is gone from the re-inferred schema.
    let props = read_node_props(dir.path(), "_untyped");
    assert!(props.get(&ab).map_or(true, HashMap::is_empty));
}

#[test]
fn remove_node_properties_missing_key_and_uuid_are_noops() {
    let dir = TempDir::new().unwrap();
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    let a = new_v7();
    w.create_node(a, EntityTypeId::decode(0).unwrap()).unwrap();
    w.set_properties(
        &a,
        None,
        HashMap::from([("age".to_owned(), IrLiteral::Int(30))]),
    )
    .unwrap();
    w.flush().unwrap();

    let ab = to_bytes(&a);
    // Remove a key that isn't there, plus a uuid that doesn't exist.
    let removals = HashMap::from([
        (ab, HashSet::from(["nope".to_owned()])),
        (to_bytes(&new_v7()), HashSet::from(["age".to_owned()])),
    ]);
    remove_node_properties(dir.path(), "_untyped", &removals).unwrap();

    // `age` survives untouched.
    let props = read_node_props(dir.path(), "_untyped");
    assert_eq!(props[&ab]["age"], IrLiteral::Int(30));
}

#[test]
fn set_and_remove_edge_properties_round_trip() {
    let dir = TempDir::new().unwrap();
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    let a = new_v7();
    let b = new_v7();
    w.create_node(a, EntityTypeId::decode(0).unwrap()).unwrap();
    w.create_node(b, EntityTypeId::decode(0).unwrap()).unwrap();
    let e = new_v7();
    w.create_edge(e, "KNOWS", &a, &b).unwrap();
    w.set_edge_properties(
        &e,
        Some("KNOWS"),
        HashMap::from([("since".to_owned(), IrLiteral::Int(2019))]),
    )
    .unwrap();
    w.flush().unwrap();

    let eb = to_bytes(&e);
    let search_generation = crate::generation::read_search_generation(dir.path()).unwrap();
    // SET overwrites since.
    let updates = HashMap::from([(
        eb,
        HashMap::from([("since".to_owned(), IrLiteral::Int(2020))]),
    )]);
    assert_eq!(
        set_edge_properties_rewrite(dir.path(), "KNOWS", &updates).unwrap(),
        1
    );
    assert_eq!(
        read_edge_props(dir.path(), "KNOWS")[&eb]["since"],
        IrLiteral::Int(2020)
    );

    // REMOVE since.
    let removals = HashMap::from([(eb, HashSet::from(["since".to_owned()]))]);
    assert_eq!(
        remove_edge_properties(dir.path(), "KNOWS", &removals).unwrap(),
        1
    );
    let props = read_edge_props(dir.path(), "KNOWS");
    assert!(props.get(&eb).map_or(true, HashMap::is_empty));
    assert_eq!(
        crate::generation::read_search_generation(dir.path()).unwrap(),
        search_generation
    );
}

#[test]
fn set_node_properties_empty_map_writes_nothing() {
    let dir = TempDir::new().unwrap();
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    w.create_node(new_v7(), EntityTypeId::decode(0).unwrap())
        .unwrap();
    w.flush().unwrap();

    let search_generation = crate::generation::read_search_generation(dir.path()).unwrap();
    let touched = set_node_properties(dir.path(), "_untyped", &HashMap::new()).unwrap();
    assert_eq!(touched, 0);
    assert_eq!(
        crate::generation::read_search_generation(dir.path()).unwrap(),
        search_generation
    );
    // No property file was created from an empty update set.
    assert!(
        !dir.path()
            .join("properties")
            .join("_untyped.parquet")
            .exists()
    );
}

#[test]
fn staged_set_is_invisible_until_commit_across_stems() {
    // One RewriteBatch spanning a node-property stem AND an edge-property
    // stem (#790): nothing changes until commit, then both apply at once.
    let dir = TempDir::new().unwrap();
    let (a, e) = (new_v7(), new_v7());
    let b = new_v7();
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    w.create_node(a, EntityTypeId::decode(0).unwrap()).unwrap();
    w.create_node(b, EntityTypeId::decode(0).unwrap()).unwrap();
    w.create_edge(e, "KNOWS", &a, &b).unwrap();
    w.set_properties(
        &a,
        None,
        HashMap::from([("name".to_owned(), IrLiteral::Str("old".into()))]),
    )
    .unwrap();
    w.set_edge_properties(
        &e,
        Some("KNOWS"),
        HashMap::from([("since".to_owned(), IrLiteral::Int(2000))]),
    )
    .unwrap();
    w.flush().unwrap();

    let (ab, eb) = (to_bytes(&a), to_bytes(&e));
    let node_updates = HashMap::from([(
        ab,
        HashMap::from([("name".to_owned(), IrLiteral::Str("new".into()))]),
    )]);
    let edge_updates = HashMap::from([(
        eb,
        HashMap::from([("since".to_owned(), IrLiteral::Int(2024))]),
    )]);

    let mut staged = RewriteBatch::new();
    let touched = stage_set_node_properties(&mut staged, dir.path(), "_untyped", &node_updates)
        .unwrap()
        + stage_set_edge_properties(&mut staged, dir.path(), "KNOWS", &edge_updates).unwrap();
    assert_eq!(touched, 2, "one node + one edge written");

    // Invisible while staged.
    assert_eq!(
        read_node_props(dir.path(), "_untyped")[&ab]["name"],
        IrLiteral::Str("old".into())
    );
    assert_eq!(
        read_edge_props(dir.path(), "KNOWS")[&eb]["since"],
        IrLiteral::Int(2000)
    );

    staged.commit_at(dir.path()).unwrap();
    assert_eq!(
        read_node_props(dir.path(), "_untyped")[&ab]["name"],
        IrLiteral::Str("new".into())
    );
    assert_eq!(
        read_edge_props(dir.path(), "KNOWS")[&eb]["since"],
        IrLiteral::Int(2024)
    );
}

#[test]
fn same_window_set_then_remove_seals_one_ordered_snapshot() {
    let dir = TempDir::new().unwrap();
    let node = new_v7();
    let uuid = to_bytes(&node);
    let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    writer
        .create_node(node, EntityTypeId::decode(0).unwrap())
        .unwrap();
    writer
        .set_properties(
            &node,
            None,
            HashMap::from([("base".into(), IrLiteral::Int(1))]),
        )
        .unwrap();
    writer.flush().unwrap();
    let prior_property_generation =
        crate::generation::read_property_generation(dir.path()).unwrap();

    let mut staged = RewriteBatch::new();
    stage_set_node_properties(
        &mut staged,
        dir.path(),
        "_untyped",
        &HashMap::from([(
            uuid,
            HashMap::from([
                ("keep".into(), IrLiteral::Int(2)),
                ("remove".into(), IrLiteral::Int(3)),
            ]),
        )]),
    )
    .unwrap();
    stage_remove_node_properties(
        &mut staged,
        dir.path(),
        "_untyped",
        &HashMap::from([(uuid, HashSet::from(["remove".into()]))]),
    )
    .unwrap();
    assert_eq!(staged.property_window_count(), 1);
    staged.commit_at(dir.path()).unwrap();
    assert_eq!(
        crate::generation::read_property_generation(dir.path()).unwrap(),
        prior_property_generation + 1
    );
    let properties = read_entity_properties(dir.path(), "_untyped", &uuid, false).unwrap();
    assert_eq!(properties.get("base"), Some(&IrLiteral::Int(1)));
    assert_eq!(properties.get("keep"), Some(&IrLiteral::Int(2)));
    assert!(!properties.contains_key("remove"));
}

#[test]
fn same_window_delete_remove_stays_deleted_but_set_explicitly_resurrects() {
    let dir = TempDir::new().unwrap();
    let removed = new_v7();
    let resurrected = new_v7();
    let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    for node in [removed, resurrected] {
        writer
            .create_node(node, EntityTypeId::decode(0).unwrap())
            .unwrap();
        writer
            .set_properties(
                &node,
                None,
                HashMap::from([("name".into(), IrLiteral::Str("before".into()))]),
            )
            .unwrap();
    }
    writer.flush().unwrap();

    let removed = to_bytes(&removed);
    let resurrected = to_bytes(&resurrected);
    let mut staged = RewriteBatch::new();
    let inventory = crate::property_overlay::authenticated_property_inventory_for_route(
        dir.path(),
        crate::PropertyRouteKind::Node,
        "_untyped",
    )
    .unwrap();
    stage_property_tombstones_authenticated(
        &mut staged,
        dir.path(),
        &inventory,
        crate::PropertyRouteKind::Node,
        "_untyped",
        &HashSet::from([removed, resurrected]),
    )
    .unwrap();
    stage_remove_node_properties(
        &mut staged,
        dir.path(),
        "_untyped",
        &HashMap::from([(removed, HashSet::from(["name".into()]))]),
    )
    .unwrap();
    stage_set_node_properties(
        &mut staged,
        dir.path(),
        "_untyped",
        &HashMap::from([(
            resurrected,
            HashMap::from([("name".into(), IrLiteral::Str("after".into()))]),
        )]),
    )
    .unwrap();
    staged.commit_at(dir.path()).unwrap();

    let properties = read_node_props(dir.path(), "_untyped");
    assert!(!properties.contains_key(&removed));
    assert_eq!(
        properties[&resurrected]["name"],
        IrLiteral::Str("after".into())
    );
}

#[test]
fn staged_property_mutation_conflicts_after_intervening_project_publication() {
    let graph = TempDir::new().unwrap();
    let node = new_v7();
    let node_bytes = to_bytes(&node);
    let mut writer = GraphWriter::open_at(graph.path(), OntologyMode::Exploratory, TS).unwrap();
    writer
        .create_node(node, EntityTypeId::decode(0).unwrap())
        .unwrap();
    writer
        .set_properties(
            &node,
            None,
            HashMap::from([("name".into(), IrLiteral::Str("baseline".into()))]),
        )
        .unwrap();
    writer.flush().unwrap();

    let container = TempDir::new().unwrap();
    crate::open_or_initialize_project(container.path()).unwrap();
    let graph_request = || {
        let (_, graph_files) = crate::capture_graph_files(graph.path()).unwrap();
        let mut participants = crate::empty_workspace_participants().unwrap();
        participants.insert(0, graph_files);
        crate::ProjectGenerationRequest {
            transaction_uuid: uuid::Uuid::now_v7(),
            generation_uuid: uuid::Uuid::now_v7(),
            capabilities: vec![
                crate::ProjectCapability {
                    capability_id: "graph".into(),
                    capability_version: 1,
                },
                crate::ProjectCapability {
                    capability_id: "workspace".into(),
                    capability_version: 1,
                },
            ],
            participants,
        }
    };

    let initial_request = graph_request();
    let crate::ProjectStageOutcome::Staged(initial) =
        crate::stage_project_generation_with_graph_tree(
            container.path(),
            &initial_request,
            Some(graph.path()),
        )
        .unwrap()
    else {
        panic!("fresh publication unexpectedly replayed");
    };
    initial
        .validate(|_| Ok(()), |_, _| Ok(()))
        .unwrap()
        .publish()
        .unwrap();
    let authority = crate::resolve_project_generation(container.path()).unwrap();
    let inventory =
        crate::AuthenticatedPropertyInventory::from_resolved_generation(&authority).unwrap();
    let mut mutation_a = RewriteBatch::new();
    stage_set_node_properties_authenticated(
        &mut mutation_a,
        graph.path(),
        &inventory,
        "_untyped",
        &HashMap::from([(
            node_bytes,
            HashMap::from([("name".into(), IrLiteral::Str("stale-a".into()))]),
        )]),
    )
    .unwrap();

    let request_b = graph_request();
    let transaction_b = request_b.transaction_uuid;
    let crate::ProjectStageOutcome::Staged(staged_b) =
        crate::stage_project_generation_optimistic_with_graph_tree(
            container.path(),
            &request_b,
            [7; 32],
            Some(graph.path()),
        )
        .unwrap()
    else {
        panic!("fresh optimistic publication unexpectedly replayed");
    };
    let (locked_tx, locked_rx) = std::sync::mpsc::sync_channel(0);
    let (resume_tx, resume_rx) = std::sync::mpsc::sync_channel(0);
    crate::project_publication::install_writer_lock_test_barrier(
        transaction_b,
        locked_tx,
        resume_rx,
    );
    let container_root = container.path().to_path_buf();
    let publisher_b = std::thread::spawn(move || {
        let receipt = staged_b
            .validate(|_| Ok(()), |_, _| Ok(()))
            .unwrap()
            .publish()
            .unwrap();
        let current = fs::read(container_root.join(crate::CURRENT_FILE)).unwrap();
        (receipt, current)
    });
    locked_rx.recv().unwrap();
    let graph_root = graph.path().to_path_buf();
    let (started_tx, started_rx) = std::sync::mpsc::sync_channel(0);
    let mutation_a = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        crate::generation::commit_topology_aware(mutation_a, &graph_root)
    });
    started_rx.recv().unwrap();
    resume_tx.send(()).unwrap();
    let (published_b, current_b) = publisher_b.join().unwrap();
    let error = mutation_a.join().unwrap().unwrap_err();
    let generation_b = crate::resolve_project_generation(container.path())
        .unwrap()
        .generation_uuid();

    assert_eq!(generation_b, published_b.generation_uuid);
    assert!(matches!(
        error,
        GfError::Project {
            code: ProjectErrorCode::WriteConflict,
            ..
        }
    ));
    assert_eq!(
        crate::resolve_project_generation(container.path())
            .unwrap()
            .generation_uuid(),
        generation_b
    );
    assert_eq!(
        fs::read(container.path().join(crate::CURRENT_FILE)).unwrap(),
        current_b
    );
    assert_eq!(
        read_node_props(graph.path(), "_untyped")[&node_bytes]["name"],
        IrLiteral::Str("baseline".into())
    );
}

#[test]
fn legacy_flat_baseline_survives_set_remove_and_reopen() {
    let dir = TempDir::new().unwrap();
    let (updated, untouched) = (new_v7(), new_v7());
    let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    writer
        .create_node(updated, EntityTypeId::decode(0).unwrap())
        .unwrap();
    writer
        .create_node(untouched, EntityTypeId::decode(0).unwrap())
        .unwrap();
    writer.flush().unwrap();

    let legacy_rows = vec![
        crate::property_overlay::PropertySnapshotRow {
            uuid: to_bytes(&updated),
            tombstone: false,
            values: BTreeMap::from([
                ("keep".into(), IrLiteral::Int(1)),
                ("remove".into(), IrLiteral::Int(2)),
            ]),
        },
        crate::property_overlay::PropertySnapshotRow {
            uuid: to_bytes(&untouched),
            tombstone: false,
            values: BTreeMap::from([("legacy_only".into(), IrLiteral::Str("preserved".into()))]),
        },
    ];
    let legacy = property_snapshots_to_batch("_untyped", false, legacy_rows)
        .unwrap()
        .unwrap();
    drop(writer);
    let table_path = dir.path().join(crate::route_component::TABLE_FILE);
    assert_eq!(
        crate::route_component::RouteTable::decode(
            &fs::read(&table_path).unwrap(),
            64 * 1024 * 1024,
            100_000
        )
        .unwrap(),
        crate::route_component::RouteTable::default()
    );
    fs::remove_file(&table_path).unwrap();
    fs::create_dir_all(dir.path().join("properties")).unwrap();
    let mut parquet = parquet::arrow::ArrowWriter::try_new(
        File::create(dir.path().join("properties/_untyped.parquet")).unwrap(),
        legacy.schema(),
        None,
    )
    .unwrap();
    parquet.write(&legacy).unwrap();
    parquet.close().unwrap();
    let migrated = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    drop(migrated);

    set_node_properties(
        dir.path(),
        "_untyped",
        &HashMap::from([(
            to_bytes(&updated),
            HashMap::from([("new".into(), IrLiteral::Int(3))]),
        )]),
    )
    .unwrap();
    remove_node_properties(
        dir.path(),
        "_untyped",
        &HashMap::from([(to_bytes(&updated), HashSet::from(["remove".into()]))]),
    )
    .unwrap();

    let fragments = crate::property_overlay::enumerate_property_fragments(
        dir.path(),
        crate::property_overlay::PropertyRouteKind::Node,
        &crate::route_component::component("_untyped"),
    )
    .unwrap();
    assert_eq!(fragments.first().unwrap().id.generation, 0);
    assert_eq!(fragments.first().unwrap().id.ordinal, 0);
    assert!(fragments.len() >= 3);
    let reopened = read_node_props(dir.path(), "_untyped");
    assert_eq!(reopened[&to_bytes(&updated)]["keep"], IrLiteral::Int(1));
    assert_eq!(reopened[&to_bytes(&updated)]["new"], IrLiteral::Int(3));
    assert!(!reopened[&to_bytes(&updated)].contains_key("remove"));
    assert_eq!(
        reopened[&to_bytes(&untouched)]["legacy_only"],
        IrLiteral::Str("preserved".into())
    );

    let project = TempDir::new().unwrap();
    let parent_generation = crate::open_or_initialize_project(project.path()).unwrap();
    let (_, inventory) = crate::capture_graph_files(dir.path()).unwrap();
    let mut participants = crate::empty_workspace_participants().unwrap();
    participants.insert(0, inventory);
    let request = crate::ProjectGenerationRequest {
        transaction_uuid: uuid::Uuid::now_v7(),
        generation_uuid: uuid::Uuid::now_v7(),
        capabilities: vec![
            crate::ProjectCapability {
                capability_id: "graph".into(),
                capability_version: 1,
            },
            crate::ProjectCapability {
                capability_id: "workspace".into(),
                capability_version: 1,
            },
        ],
        participants,
    };
    let crate::ProjectStageOutcome::Staged(staged) =
        crate::stage_project_generation_with_graph_tree(project.path(), &request, Some(dir.path()))
            .unwrap()
    else {
        panic!("fresh migration generation replayed");
    };
    staged
        .validate(|_| Ok(()), |_, _| Ok(()))
        .unwrap()
        .publish()
        .unwrap();
    drop(parent_generation);
    let generation = crate::resolve_project_generation(project.path()).unwrap();
    let limits = crate::PortableV2ExportLimits::default();
    let plan = crate::plan_complete_portable_v2(&generation, limits).unwrap();
    let package_parent = TempDir::new().unwrap();
    let package = package_parent.path().join("legacy-migration.gfproject");
    crate::export_complete_portable_v2(
        &plan,
        &package,
        crate::PortableV2Output::Expanded,
        limits,
        &std::sync::atomic::AtomicBool::new(false),
        |_| {},
    )
    .unwrap();
    crate::verify_portable_v2(&package, crate::PortableV2Mode::Full, limits, None).unwrap();
    let supported = generation
        .capabilities()
        .into_iter()
        .map(|capability| crate::ProjectCapability {
            capability_id: capability.capability_id,
            capability_version: capability.capability_version,
        })
        .collect::<Vec<_>>();
    let imported_parent = TempDir::new().unwrap();
    let imported = imported_parent.path().join("clean-import");
    crate::import_complete_portable_v2(
        &package,
        &imported,
        uuid::Uuid::now_v7(),
        uuid::Uuid::now_v7(),
        &supported,
        limits,
        None,
    )
    .unwrap();
    let imported_generation = crate::resolve_project_generation(&imported).unwrap();
    let imported_props = read_node_props(&imported_generation.graph_tree_root(), "_untyped");
    assert_eq!(imported_props, reopened);
}
