//! Public construction and interchange preserve compact-detail graph identity.

use std::sync::Arc;

use arrow::array::{Array, FixedSizeBinaryArray, Int64Array, ListArray, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use graphforge_core::portable::{
    PortableV2Limits, PortableV2Mode, PortableV2Output, PortableV2SelectionProfile,
};
use graphforge_storage::UuidMembershipIndex;
use uuid::Uuid;

use crate::{
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, GraphConstructionBudgets, GraphForge,
    OperationId, PortableSelection, PortableV2ExportRequest, PortableV2ImportRequest,
    PortableVerifyRequest, verify_portable_v2,
};

type NodeRow = (Uuid, Vec<String>);
type EdgeRow = (Uuid, String, Uuid, Uuid);

fn uuid_array(ids: &[Uuid]) -> FixedSizeBinaryArray {
    FixedSizeBinaryArray::try_from_iter(ids.iter().map(Uuid::as_bytes)).unwrap()
}

fn uuid_at(batch: &RecordBatch, column: usize, row: usize) -> Uuid {
    assert_eq!(batch.column(column).null_count(), 0);
    Uuid::from_slice(
        batch
            .column(column)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(row),
    )
    .unwrap()
}

fn graph_rows(graph: &GraphForge) -> (Vec<NodeRow>, Vec<EdgeRow>) {
    let nodes = graph
        .execute("MATCH (n) RETURN n.node_uuid, labels(n) ORDER BY n.node_uuid")
        .unwrap();
    let node_rows = nodes
        .batches
        .iter()
        .flat_map(|batch| {
            let labels = batch
                .column(1)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            assert_eq!(labels.null_count(), 0);
            (0..batch.num_rows())
                .map(|row| {
                    let values = labels.value(row);
                    let names = values.as_any().downcast_ref::<StringArray>().unwrap();
                    assert_eq!(names.null_count(), 0);
                    (
                        uuid_at(batch, 0, row),
                        names.iter().map(|name| name.unwrap().to_owned()).collect(),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect();
    let edges = graph.execute("MATCH (a)-[r]->(b) RETURN r.edge_uuid, type(r), a.node_uuid, b.node_uuid ORDER BY r.edge_uuid").unwrap();
    let edge_rows = edges
        .batches
        .iter()
        .flat_map(|batch| {
            let routes = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(routes.null_count(), 0);
            (0..batch.num_rows())
                .map(|row| {
                    (
                        uuid_at(batch, 0, row),
                        routes.value(row).to_owned(),
                        uuid_at(batch, 2, row),
                        uuid_at(batch, 3, row),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect();
    (node_rows, edge_rows)
}

fn ordinals(graph: &GraphForge, ids: &[Uuid]) -> Vec<Option<u64>> {
    UuidMembershipIndex::open(&graph.dir)
        .unwrap()
        .lookup_node_surrogates(ids)
        .unwrap()
        .0
}

fn assert_properties(graph: &GraphForge, nodes: &[Uuid], edges: &[Uuid]) {
    for (query, expected) in [
        (
            "MATCH (n) RETURN n.node_uuid, n.score ORDER BY n.node_uuid",
            nodes
                .iter()
                .enumerate()
                .map(|(i, &id)| {
                    (
                        id,
                        if i == 1 {
                            None
                        } else {
                            Some(41 + i64::try_from(i).unwrap())
                        },
                    )
                })
                .collect::<Vec<_>>(),
        ),
        (
            "MATCH ()-[r]->() RETURN r.edge_uuid, r.weight ORDER BY r.edge_uuid",
            edges
                .iter()
                .enumerate()
                .map(|(i, &id)| {
                    (
                        id,
                        if i == 1 {
                            None
                        } else {
                            Some(73 + i64::try_from(i).unwrap())
                        },
                    )
                })
                .collect::<Vec<_>>(),
        ),
    ] {
        let result = graph.execute(query).unwrap();
        let actual = result
            .batches
            .iter()
            .flat_map(|batch| {
                let values = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                values
                    .iter()
                    .enumerate()
                    .map(|(row, value)| (uuid_at(batch, 0, row), value))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }
}

fn property_schema(base: &Schema, name: &str) -> Arc<Schema> {
    let mut fields = base.fields().to_vec();
    fields.push(Arc::new(Field::new(name, DataType::Int64, true)));
    Arc::new(Schema::new(fields))
}

#[test]
fn compact_public_construction_reopens_and_round_trips_exact_graph() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let graph = GraphForge::new(source.to_str()).unwrap();
    let ids = [Uuid::from_u128(1), Uuid::from_u128(2), Uuid::from_u128(3)];
    let edge_ids = [
        Uuid::from_u128(11),
        Uuid::from_u128(12),
        Uuid::from_u128(13),
        Uuid::from_u128(14),
    ];
    let budgets = GraphConstructionBudgets {
        max_batch_rows: 2,
        max_run_records: 8,
        ..Default::default()
    };
    let mut session = graph.begin_graph_construction(budgets).unwrap();
    let session_id = session.session_uuid();
    let node_chunk = |indexes: &[usize], names: Vec<&str>| {
        RecordBatch::try_new(
            property_schema(&CONSTRUCTION_NODE_SCHEMA, "score"),
            vec![
                Arc::new(uuid_array(
                    &indexes.iter().map(|&i| ids[i]).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(names)),
                Arc::new(Int64Array::from(
                    indexes
                        .iter()
                        .map(|&i| {
                            if i == 1 {
                                None
                            } else {
                                Some(41 + i64::try_from(i).unwrap())
                            }
                        })
                        .collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    };
    session
        .append_nodes("first", &node_chunk(&[2, 0], vec!["Équipe", "Person"]))
        .unwrap();
    drop(session);
    let checkpoint = source
        .join(".graphforge-construction")
        .join(session_id.simple().to_string())
        .join("checkpoint.json");
    let control: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&checkpoint).unwrap()).unwrap();
    assert_eq!(
        control["format_version"], 8,
        "public new sessions use compact details with mapped output"
    );
    let mut session = graph
        .resume_graph_construction(session_id, budgets)
        .unwrap();
    session
        .append_nodes("second", &node_chunk(&[1], vec!["Person"]))
        .unwrap();
    let endpoints = [(0, 1), (0, 1), (2, 2), (1, 2)];
    for (chunk, indexes) in [[0, 2], [1, 3]].iter().enumerate() {
        let batch = RecordBatch::try_new(
            property_schema(&CONSTRUCTION_EDGE_SCHEMA, "weight"),
            vec![
                Arc::new(uuid_array(
                    &indexes.iter().map(|&i| edge_ids[i]).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    indexes
                        .iter()
                        .map(|&i| if i < 2 { "KNOWS" } else { "LIÉ" })
                        .collect::<Vec<_>>(),
                )),
                Arc::new(uuid_array(
                    &indexes
                        .iter()
                        .map(|&i| ids[endpoints[i].0])
                        .collect::<Vec<_>>(),
                )),
                Arc::new(uuid_array(
                    &indexes
                        .iter()
                        .map(|&i| ids[endpoints[i].1])
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    indexes
                        .iter()
                        .map(|&i| {
                            if i == 1 {
                                None
                            } else {
                                Some(73 + i64::try_from(i).unwrap())
                            }
                        })
                        .collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        session
            .append_edges(&format!("edges-{chunk}"), &batch)
            .unwrap();
    }
    let published = session.seal_and_publish().unwrap();
    assert!(!published.idempotent_replay);
    drop(session);
    let mut replay = graph
        .resume_graph_construction(session_id, budgets)
        .unwrap();
    let receipt = replay.seal_and_publish().unwrap();
    assert!(receipt.idempotent_replay);
    assert_eq!(receipt.generation_uuid, published.generation_uuid);
    assert_eq!(
        graphforge_storage::resolve_project_generation(&source)
            .unwrap()
            .generation_uuid(),
        published.generation_uuid
    );
    drop(replay);
    drop(graph);

    let graph = GraphForge::new(source.to_str()).unwrap();
    let expected_nodes = vec![
        (ids[0], vec!["Person".into()]),
        (ids[1], vec!["Person".into()]),
        (ids[2], vec!["Équipe".into()]),
    ];
    let expected_edges = edge_ids
        .iter()
        .enumerate()
        .map(|(i, &id)| {
            (
                id,
                if i < 2 { "KNOWS" } else { "LIÉ" }.into(),
                ids[endpoints[i].0],
                ids[endpoints[i].1],
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        graph_rows(&graph),
        (expected_nodes.clone(), expected_edges.clone())
    );
    let expected_ordinals = vec![Some(1), Some(2), Some(3)];
    assert_eq!(ordinals(&graph, &ids), expected_ordinals);
    assert_properties(&graph, &ids, &edge_ids);
    drop(graph);
    let graph = GraphForge::new(source.to_str()).unwrap();
    assert_properties(&graph, &ids, &edge_ids);
    assert_eq!(
        graph_rows(&graph),
        (expected_nodes.clone(), expected_edges.clone())
    );
    assert_eq!(ordinals(&graph, &ids), expected_ordinals);
    let limits = PortableV2Limits::default();
    let package = root.path().join("graph.gfpb");
    let exported = graph
        .export_portable_v2(
            &PortableV2ExportRequest {
                selection: PortableSelection::Current,
                output_path: package.clone(),
                representation: PortableV2Output::Bundle,
                profile: PortableV2SelectionProfile::Complete,
                subset: None,
                limits,
            },
            None,
            |_| {},
        )
        .unwrap();
    let verified = verify_portable_v2(
        &PortableVerifyRequest {
            input: package.clone(),
            mode: PortableV2Mode::Full,
            limits,
        },
        None,
    )
    .unwrap();
    assert_eq!(verified.package_digest, exported.package_digest);
    drop(graph);
    let target = root.path().join("imported");
    assert!(!target.exists());
    let imported = GraphForge::import_portable_v2(
        &target,
        &PortableV2ImportRequest {
            input: package,
            operation_id: OperationId(Uuid::from_u128(100)),
            limits,
        },
        None,
    )
    .unwrap();
    assert!(!imported.idempotent_replay);
    let graph = GraphForge::new(target.to_str()).unwrap();
    assert_eq!(
        graph.resolved_generation.generation_uuid(),
        imported.generation_uuid
    );
    assert_eq!(imported.package_digest, exported.package_digest);
    assert_eq!(graph_rows(&graph), (expected_nodes, expected_edges));
    assert_eq!(ordinals(&graph, &ids), expected_ordinals);
    assert_properties(&graph, &ids, &edge_ids);
}
