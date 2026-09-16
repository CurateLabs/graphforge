use super::*;
use crate::writer::BTreeMap;
use crate::writer::EntityTypeId;
use crate::writer::GraphWriter;
use crate::writer::HashMap;
use crate::writer::IrLiteral;
use crate::writer::OntologyMode;
use crate::writer::SchemaRef;
use crate::writer::fs;
use crate::writer::set_node_properties;
use crate::writer::tests::TS;
use crate::writer::tests::read_node_props;
use crate::writer::to_bytes;
use crate::writer::write_replay_overlay_streaming;
use graphforge_core::uuid::new_v7;
use tempfile::TempDir;

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
                .unwrap(),
            "score"
        ),
        Some(2)
    );
    assert_eq!(
        summary_count(
            &created
                .route_schema(crate::PropertyRouteKind::Edge, "NewEdgeRoute")
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
                .unwrap(),
            "score"
        ),
        None
    );
    assert_eq!(
        summary_count(
            &reopened
                .route_schema(crate::PropertyRouteKind::Edge, "NewEdgeRoute")
                .unwrap(),
            "weight"
        ),
        None
    );
}
