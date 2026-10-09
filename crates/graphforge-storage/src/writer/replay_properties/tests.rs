use super::*;
use crate::writer::fs;
use crate::writer::set_node_properties;
use crate::writer::tests::read_node_props;
use crate::writer::tests::TS;
use crate::writer::to_bytes;
use crate::writer::write_replay_overlay_streaming;
use crate::writer::BTreeMap;
use crate::writer::EntityTypeId;
use crate::writer::GraphWriter;
use crate::writer::HashMap;
use crate::writer::IrLiteral;
use crate::writer::OntologyMode;
use crate::writer::SchemaRef;
use graphforge_core::uuid::new_v7;
use tempfile::TempDir;

#[test]
fn closed_replay_fragment_reuses_writer_memory_for_bounded_objects() {
    use crate::property_overlay::bounded_object::{
        MAX_PROPERTY_OBJECT_BYTES, PROPERTY_OBJECT_ENCODER_MEMORY_BYTES,
    };
    let project = TempDir::new().unwrap();
    let mut routes = crate::route_component::owned::admit_owned_workspace(project.path()).unwrap();
    let (files, _) = crate::capture_graph_files(project.path()).unwrap();
    let inventory =
        crate::AuthenticatedPropertyInventory::from_inventory_at_root(project.path(), files, None)
            .unwrap();
    let schema = Arc::new(Schema::new(vec![
        uuid_field("node_uuid"),
        Field::new("payload", DataType::Utf8, true),
    ]));
    let uuid = new_v7();
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    let payload: String = (0..7 * 1024 * 1024)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            char::from(33 + u8::try_from(state % 94).unwrap())
        })
        .collect();
    let mut fragment = open_replay_property_fragment(
        project.path(),
        crate::PropertyRouteKind::Node,
        false,
        "Wide",
        1,
        &schema,
        1,
        0,
    )
    .unwrap();
    let batch = replay_property_snapshot_batch(
        &schema,
        &fragment.physical_schema,
        crate::PropertySnapshotRow {
            uuid: *uuid.as_bytes(),
            tombstone: false,
            values: BTreeMap::from([("payload".into(), IrLiteral::Str(payload.clone()))]),
        },
    )
    .unwrap();
    fragment.writer.write(&batch).unwrap();
    drop(batch);
    fragment.writer.close().unwrap();
    drop(fragment.logical_schema);
    drop(fragment.physical_schema);
    assert!(fs::metadata(&fragment.path).unwrap().len() > MAX_PROPERTY_OBJECT_BYTES as u64);

    let physical_schema = replay_property_resource_schema(&schema);
    let schema_bytes = crate::permanent_parquet::replay_schema_bytes(&physical_schema).unwrap();
    let writer_reservation_bytes = replay_writer_reservation(&physical_schema, 1, 0, 1).unwrap();
    assert!(writer_reservation_bytes > schema_bytes);
    let overlay = crate::graph_delta_journal::ReplayOverlay::default();
    let context = ReplayPropertyRouteContext {
        target: project.path(),
        inventory: &inventory,
        overlay: &overlay,
        operations: &overlay.node_properties,
        limits: crate::GraphDeltaJournalLimits {
            // The writer is gone: only schema copies and the framing encoder
            // coexist. Charging its full reservation again rejects this budget.
            max_replay_memory_bytes: schema_bytes + PROPERTY_OBJECT_ENCODER_MEMORY_BYTES,
            ..Default::default()
        },
        kind: crate::PropertyRouteKind::Node,
        edge: false,
        route: "Wide",
        overlay_bytes: 0,
        retained_target_bytes: 0,
        writer_reservation_bytes,
        logical_schema: schema,
    };
    context.bound_closed_fragment(&fragment.path, 0).unwrap();
    routes.insert("Wide", 64 * 1024 * 1024, 100_000).unwrap();
    replace_private_replay_route_table(project.path(), &routes, true, true).unwrap();
    assert_eq!(
        read_node_props(project.path(), "Wide")[uuid.as_bytes()]["payload"],
        IrLiteral::Str(payload)
    );
}

#[test]
fn replay_advances_property_authority_across_unequal_route_generations() {
    let dir = TempDir::new().unwrap();
    let a = new_v7();
    let b = new_v7();
    let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
    for (uuid, route) in [(a, "Person"), (b, "Company")] {
        writer
            .create_node(uuid, EntityTypeId::decode(1).unwrap())
            .unwrap();
        writer
            .set_properties(
                &uuid,
                Some(route),
                HashMap::from([("score".into(), IrLiteral::Int(1))]),
            )
            .unwrap();
    }
    writer.flush().unwrap();
    for value in [2, 3] {
        set_node_properties(
            dir.path(),
            "Person",
            &HashMap::from([(
                to_bytes(&a),
                HashMap::from([("score".into(), IrLiteral::Int(value))]),
            )]),
        )
        .unwrap();
    }
    let property_before = crate::generation::read_property_generation(dir.path()).unwrap();
    let search_before = crate::generation::read_search_generation(dir.path()).unwrap();
    let mut overlay = crate::graph_delta_journal::ReplayOverlay::default();
    for (uuid, route, value) in [(a, "Person", 4), (b, "Company", 5)] {
        overlay.node_properties.insert(
            (uuid.to_string(), route.into(), "score".into()),
            Some(IrLiteral::Int(value)),
        );
    }
    let (inventory, _) = crate::capture_graph_files(dir.path()).unwrap();
    let target = TempDir::new().unwrap();
    for file in &inventory.files {
        let destination = target.path().join(&file.relative_path);
        fs::create_dir_all(destination.parent().unwrap()).unwrap();
        fs::copy(dir.path().join(&file.relative_path), destination).unwrap();
    }
    write_replay_overlay_streaming(
        dir.path(),
        &inventory,
        target.path(),
        &overlay,
        Default::default(),
    )
    .unwrap();
    assert_eq!(
        crate::generation::read_property_generation(target.path()).unwrap(),
        property_before + 1
    );
    assert_eq!(
        crate::generation::read_search_generation(target.path()).unwrap(),
        search_before + 1
    );
    for (uuid, route, value) in [(a, "Person", 4), (b, "Company", 5)] {
        assert_eq!(
            read_node_props(target.path(), route)[&to_bytes(&uuid)]["score"],
            IrLiteral::Int(value)
        );
        set_node_properties(
            target.path(),
            route,
            &HashMap::from([(
                to_bytes(&uuid),
                HashMap::from([("score".into(), IrLiteral::Int(value + 10))]),
            )]),
        )
        .unwrap();
        assert_eq!(
            read_node_props(target.path(), route)[&to_bytes(&uuid)]["score"],
            IrLiteral::Int(value + 10)
        );
    }
}

#[test]
fn delta_replay_new_routes_start_and_continue_live_schema_authority() {
    let project = TempDir::new().unwrap();
    let (empty_files, _) = crate::capture_graph_files(project.path()).unwrap();
    let empty = crate::AuthenticatedPropertyInventory::from_inventory_at_root(
        project.path(),
        empty_files,
        None,
    )
    .unwrap();
    let mut limits = crate::GraphDeltaJournalLimits::default();
    limits.max_batch_rows = 1;
    let node_ids = [
        new_v7().hyphenated().to_string(),
        new_v7().hyphenated().to_string(),
    ];
    let edge_ids = [
        new_v7().hyphenated().to_string(),
        new_v7().hyphenated().to_string(),
    ];
    let populated = |ids: &[String], route: &str, key: &str| {
        ids.iter()
            .enumerate()
            .map(|(index, uuid)| {
                (
                    (uuid.clone(), route.to_owned(), key.to_owned()),
                    Some(IrLiteral::Int(index as i64)),
                )
            })
            .collect::<BTreeMap<_, _>>()
    };
    let mut overlay = crate::graph_delta_journal::ReplayOverlay::default();
    overlay.node_properties = populated(&node_ids, "NewNodeRoute", "score");
    overlay.edge_properties = populated(&edge_ids, "NewEdgeRoute", "weight");
    stream_replay_property_route(
        project.path(),
        &empty,
        &overlay,
        limits,
        false,
        "NewNodeRoute",
        project.path(),
    )
    .unwrap();
    stream_replay_property_route(
        project.path(),
        &empty,
        &overlay,
        limits,
        true,
        "NewEdgeRoute",
        project.path(),
    )
    .unwrap();

    let (files, _) = crate::capture_graph_files(project.path()).unwrap();
    let created =
        crate::AuthenticatedPropertyInventory::from_inventory_at_root(project.path(), files, None)
            .unwrap();
    let summary_count = |schema: &SchemaRef, key: &str| {
        serde_json::from_str::<serde_json::Value>(
            &schema.metadata()[crate::property_overlay::PROPERTY_LIVE_SCHEMA_KEY],
        )
        .unwrap()["counts"][key]
            .as_u64()
    };
    assert_eq!(
        summary_count(
            &created
                .route_schema(crate::PropertyRouteKind::Node, "NewNodeRoute")
                .unwrap()
                .unwrap(),
            "score"
        ),
        Some(2)
    );
    assert_eq!(
        summary_count(
            &created
                .route_schema(crate::PropertyRouteKind::Edge, "NewEdgeRoute")
                .unwrap()
                .unwrap(),
            "weight"
        ),
        Some(2)
    );

    let mut removed = crate::graph_delta_journal::ReplayOverlay::default();
    removed.node_properties = node_ids
        .iter()
        .map(|uuid| ((uuid.clone(), "NewNodeRoute".into(), "score".into()), None))
        .collect();
    removed.edge_properties = edge_ids
        .iter()
        .map(|uuid| ((uuid.clone(), "NewEdgeRoute".into(), "weight".into()), None))
        .collect();
    stream_replay_property_route(
        project.path(),
        &created,
        &removed,
        limits,
        false,
        "NewNodeRoute",
        project.path(),
    )
    .unwrap();
    stream_replay_property_route(
        project.path(),
        &created,
        &removed,
        limits,
        true,
        "NewEdgeRoute",
        project.path(),
    )
    .unwrap();

    let (files, _) = crate::capture_graph_files(project.path()).unwrap();
    let reopened =
        crate::AuthenticatedPropertyInventory::from_inventory_at_root(project.path(), files, None)
            .unwrap();
    assert_eq!(
        summary_count(
            &reopened
                .route_schema(crate::PropertyRouteKind::Node, "NewNodeRoute")
                .unwrap()
                .unwrap(),
            "score"
        ),
        None
    );
    assert_eq!(
        summary_count(
            &reopened
                .route_schema(crate::PropertyRouteKind::Edge, "NewEdgeRoute")
                .unwrap()
                .unwrap(),
            "weight"
        ),
        None
    );
}

#[test]
fn delta_replay_cuts_one_route_at_the_fragment_cap() {
    use crate::property_overlay::fragment_cap::tests::{assert_capped_fragments, wide_value};
    use crate::property_overlay::{PropertyRouteKind, MAX_PROPERTY_FRAGMENT_BYTES};
    let project = TempDir::new().unwrap();
    let (empty_files, _) = crate::capture_graph_files(project.path()).unwrap();
    let empty = crate::AuthenticatedPropertyInventory::from_inventory_at_root(
        project.path(),
        empty_files,
        None,
    )
    .unwrap();
    // 3,000 rows of about 4 KiB: three times the cap in one replay route.
    let rows = 3_000;
    let mut overlay = crate::graph_delta_journal::ReplayOverlay::default();
    for index in 0..rows as u64 {
        let uuid = uuid::Uuid::from_u128(u128::from(index) + 1)
            .hyphenated()
            .to_string();
        overlay.node_properties.insert(
            (uuid, "Wide".into(), "payload".into()),
            Some(IrLiteral::Str(wide_value(index, 4096))),
        );
    }
    stream_replay_property_route(
        project.path(),
        &empty,
        &overlay,
        crate::GraphDeltaJournalLimits::default(),
        false,
        "Wide",
        project.path(),
    )
    .unwrap();

    let fragments = crate::property_overlay::enumerate_property_fragments(
        project.path(),
        PropertyRouteKind::Node,
        &crate::route_component::component("Wide"),
    )
    .unwrap();
    let stats = assert_capped_fragments(&fragments, rows);
    assert!(
        stats.iter().map(|stat| stat.logical_bytes).sum::<u64>() > 2 * MAX_PROPERTY_FRAGMENT_BYTES
    );
    assert!(fragments.len() >= 3, "{fragments:?}");
    let props = read_node_props(project.path(), "Wide");
    assert_eq!(props.len(), rows);
    for index in 0..rows as u64 {
        assert_eq!(
            props[&(u128::from(index) + 1).to_be_bytes()]["payload"],
            IrLiteral::Str(wide_value(index, 4096))
        );
    }
}
