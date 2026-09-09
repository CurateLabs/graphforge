//! Public portable round trips preserve reserved and case-distinct semantic names.

use arrow::array::{Array, FixedSizeBinaryArray, Int64Array, ListArray, StringArray, StructArray};
use graphforge_core::portable::{
    PortableV2GraphSelector, PortableV2Limits, PortableV2Mode, PortableV2Output,
    PortableV2PropertyProjection, PortableV2SelectionProfile, PortableV2SubsetClosure,
    PortableV2SubsetRequest,
};
use uuid::Uuid;

use crate::{
    GraphForge, OperationId, PortableSelection, PortableV2ExportRequest, PortableV2ImportRequest,
    PortableVerifyRequest, verify_portable_v2,
};

type NodeRow = (Uuid, Vec<String>, String, i64);
type EdgeRow = (Uuid, String, Uuid, Uuid, i64);

fn uuid_at(batch: &arrow::record_batch::RecordBatch, column: usize, row: usize) -> Uuid {
    let array = batch
        .column(column)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert!(!array.is_null(row));
    Uuid::from_slice(array.value(row)).unwrap()
}

fn rows(graph: &GraphForge) -> (Vec<NodeRow>, Vec<EdgeRow>) {
    let mut nodes = Vec::new();
    for batch in graph
        .execute("MATCH (n) RETURN n.node_uuid, labels(n), n.name, n.score")
        .unwrap()
        .batches
    {
        let labels = batch
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let names = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let scores = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let values = labels.value(row);
            let values = values.as_any().downcast_ref::<StringArray>().unwrap();
            let mut labels = values
                .iter()
                .map(|value| value.unwrap().to_owned())
                .collect::<Vec<_>>();
            labels.sort();
            assert!(!names.is_null(row) && !scores.is_null(row));
            nodes.push((
                uuid_at(&batch, 0, row),
                labels,
                names.value(row).to_owned(),
                scores.value(row),
            ));
        }
    }
    let mut edges = Vec::new();
    for batch in graph
        .execute("MATCH (a)-[r]->(b) RETURN r.edge_uuid, type(r), a.node_uuid, b.node_uuid, r.cost")
        .unwrap()
        .batches
    {
        let routes = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let costs = batch.column(4);
        for row in 0..batch.num_rows() {
            assert!(!routes.is_null(row) && !costs.is_null(row));
            edges.push((
                uuid_at(&batch, 0, row),
                routes.value(row).to_owned(),
                uuid_at(&batch, 2, row),
                uuid_at(&batch, 3, row),
                match costs.as_any().downcast_ref::<Int64Array>() {
                    Some(values) => values.value(row),
                    None => {
                        let values =
                            costs
                                .as_any()
                                .downcast_ref::<StructArray>()
                                .unwrap_or_else(|| {
                                    panic!(
                                        "edge cost type {:?}, schema {:?}, values {:?}",
                                        costs.data_type(),
                                        batch.schema(),
                                        costs
                                    )
                                });
                        let value = graphforge_value::heterogeneous::decode_scalar(values, row)
                            .expect("valid canonical scalar property");
                        let graphforge_value::Literal::Int(value) = value else {
                            panic!("cost must retain its integer value: {value:?}");
                        };
                        value
                    }
                },
            ));
        }
    }
    nodes.sort();
    edges.sort();
    (nodes, edges)
}

#[test]
fn reserved_routes_full_expanded_verify_import_reopen() {
    round_trip(false, PortableV2Output::Expanded);
}
#[test]
fn reserved_routes_full_bundle_verify_import_reopen() {
    round_trip(false, PortableV2Output::Bundle);
}
#[test]
fn reserved_routes_projected_expanded_export_verify() {
    round_trip(true, PortableV2Output::Expanded);
}
#[test]
fn reserved_routes_projected_bundle_export_verify() {
    round_trip(true, PortableV2Output::Bundle);
}

fn round_trip(projected: bool, representation: PortableV2Output) {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let graph = GraphForge::new(source.to_str()).unwrap();
    graph.execute("CREATE (a:AUX {name: 'a', score: 11, secret: 'redact'}), (b:aux {name: 'b', score: 22, secret: 'redact'}), (c:COM1 {name: 'c', score: 33, secret: 'redact'}) CREATE (a)-[:CON {cost: 101}]->(a), (a)-[:con {cost: 202}]->(b), (b)-[:NUL {cost: 303}]->(c)").unwrap();
    let original = rows(&graph);
    assert_eq!(original.0.len(), 3);
    assert_eq!(original.1.len(), 3);
    let mut labels = original
        .0
        .iter()
        .map(|row| row.1[0].as_str())
        .collect::<Vec<_>>();
    labels.sort();
    assert_eq!(labels, vec!["AUX", "COM1", "aux"]);
    let mut relations = original
        .1
        .iter()
        .map(|row| row.1.as_str())
        .collect::<Vec<_>>();
    relations.sort();
    assert_eq!(relations, vec!["CON", "NUL", "con"]);
    let selected_edge = original.1.iter().find(|row| row.1 == "con").unwrap();
    let source_generation = graph
        .property_inventory_for_session()
        .generation_uuid()
        .unwrap();
    let limits = PortableV2Limits::default();
    let package = root.path().join("package");
    let subset = projected.then(|| PortableV2SubsetRequest {
        selector: PortableV2GraphSelector {
            node_uuids: Vec::new(),
            edge_uuids: vec![selected_edge.0.to_string()],
        },
        closure: PortableV2SubsetClosure::Referential,
        projection: PortableV2PropertyProjection {
            exclude: vec!["secret".to_owned()],
        },
    });
    let exported = graph
        .export_portable_v2(
            &PortableV2ExportRequest {
                selection: PortableSelection::Current,
                output_path: package.clone(),
                representation,
                profile: PortableV2SelectionProfile::Complete,
                subset,
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
    let target = root.path().join("imported");
    assert!(!target.exists());
    let import = GraphForge::import_portable_v2(
        &target,
        &PortableV2ImportRequest {
            input: package,
            operation_id: OperationId(Uuid::new_v4()),
            limits,
        },
        None,
    );
    if projected {
        let error = import.unwrap_err();
        assert_eq!(
            error.code,
            graphforge_core::portable::PortableV2ErrorCode::Incompatible
        );
        assert!(
            error
                .to_string()
                .contains("selective package requires an explicit class-specific consumer")
        );
        assert!(!target.exists());
    } else {
        let imported = import.unwrap();
        assert_eq!(imported.package_digest, exported.package_digest);
        assert!(!imported.idempotent_replay);
        let reopened = GraphForge::new(target.to_str()).unwrap();
        assert_eq!(
            reopened.resolved_generation.generation_uuid(),
            imported.generation_uuid
        );
        assert_eq!(rows(&reopened), original);
        let secrets = reopened
            .execute("MATCH (n) RETURN n.secret AS secret")
            .unwrap();
        for batch in secrets.batches {
            let strings = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert!(strings.iter().all(|value| value == Some("redact")));
        }
    }
    assert_eq!(
        graph.property_inventory_for_session().generation_uuid(),
        Some(source_generation)
    );
    assert_eq!(rows(&graph), original);
    drop(graph);
    let reopened_source = GraphForge::new(source.to_str()).unwrap();
    assert_eq!(
        reopened_source.resolved_generation.generation_uuid(),
        source_generation
    );
    assert_eq!(rows(&reopened_source), original);
}
