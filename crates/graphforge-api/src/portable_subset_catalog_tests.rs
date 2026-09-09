//! Sparse runtime catalog subsets export and verify without changing source authority.

use arrow::util::display::array_value_to_string;
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

fn rows(graph: &GraphForge, query: &str) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    for batch in graph.execute(query).unwrap().batches {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| array_value_to_string(column.as_ref(), row).unwrap())
                    .collect(),
            );
        }
    }
    rows.sort();
    rows
}

#[test]
fn sparse_catalog_expanded_export_verify() {
    round_trip(PortableV2Output::Expanded);
}

#[test]
fn sparse_catalog_bundle_export_verify() {
    round_trip(PortableV2Output::Bundle);
}

fn round_trip(representation: PortableV2Output) {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let graph = GraphForge::new(source.to_str()).unwrap();
    graph.execute("CREATE (a:Alpha {name: 'a', score: 11}), (b:Beta:Extra {name: 'b', score: 22}), (c:Gamma {name: 'c', score: 33}) CREATE (a)-[:FIRST {cost: 101}]->(a), (b)-[:SECOND {cost: 202}]->(c), (c)-[:THIRD {cost: 303}]->(a)").unwrap();
    let node_query = "MATCH (n) RETURN n.node_uuid, labels(n), n.name, n.score";
    let edge_query =
        "MATCH (a)-[r]->(b) RETURN r.edge_uuid, type(r), a.node_uuid, b.node_uuid, r.cost";
    let original_nodes = rows(&graph, node_query);
    let original_edges = rows(&graph, edge_query);
    assert_eq!(original_nodes.len(), 3);
    assert_eq!(original_edges.len(), 3);
    let selected = original_edges
        .iter()
        .find(|row| row[1] == "SECOND")
        .unwrap();
    assert_eq!(selected[4], "202");
    let expected_nodes = original_nodes
        .iter()
        .filter(|row| row[0] == selected[2] || row[0] == selected[3])
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(expected_nodes.len(), 2);
    let edge_batch = graph
        .execute("MATCH ()-[r:SECOND]->() RETURN r.edge_uuid")
        .unwrap();
    let ids = edge_batch.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
        .unwrap();
    let selected_uuid = Uuid::from_slice(ids.value(0)).unwrap();
    let generation = graph.generation_for_read().unwrap().generation_uuid();
    let limits = PortableV2Limits::default();
    let package = root.path().join("package");
    let exported = graph
        .export_portable_v2(
            &PortableV2ExportRequest {
                selection: PortableSelection::Current,
                output_path: package.clone(),
                representation,
                profile: PortableV2SelectionProfile::Complete,
                subset: Some(PortableV2SubsetRequest {
                    selector: PortableV2GraphSelector {
                        node_uuids: vec![],
                        edge_uuids: vec![selected_uuid.to_string()],
                    },
                    closure: PortableV2SubsetClosure::Referential,
                    projection: PortableV2PropertyProjection { exclude: vec![] },
                }),
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
    let error = GraphForge::import_portable_v2(
        &target,
        &PortableV2ImportRequest {
            input: package,
            operation_id: OperationId(Uuid::new_v4()),
            limits,
        },
        None,
    )
    .unwrap_err();
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
    assert_eq!(rows(&graph, node_query), original_nodes);
    assert_eq!(rows(&graph, edge_query), original_edges);
    assert_eq!(
        GraphForge::new(source.to_str())
            .unwrap()
            .resolved_generation
            .generation_uuid(),
        generation
    );
}
