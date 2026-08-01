//! Public persistent-project evidence for strict ontology property binding (#2594).

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use graphforge_api::{
    AdoptOntologyRequest, GfError, GraphForge, OntologyMode, OperationId, WriteContext,
};
use uuid::Uuid;

const STRICT_ONTOLOGY: &str = r#"
ontology_id: strict-runtime-properties
version: "1"
entity_types:
  - name: Asset
    abstract: false
  - name: Host
    abstract: false
    parent: Asset
relation_types:
  - name: CONNECTED_TO
    src: Host
    dst: Host
properties:
  - owner: Asset
    name: asset_name
    type: utf8
    nullable: false
  - owner: Host
    name: hostname
    type: utf8
    nullable: false
  - owner: CONNECTED_TO
    name: weight
    type: int64
    nullable: false
constraints: []
migrations: []
"#;

fn context(seed: u128) -> WriteContext {
    WriteContext {
        operation_uuid: OperationId(Uuid::from_u128(seed)),
        actor_uuid: None,
    }
}

fn only_batch(result: graphforge_api::ExecutionResult) -> RecordBatch {
    assert_eq!(result.batches.len(), 1, "expected one deterministic batch");
    result.batches.into_iter().next().unwrap()
}

fn schema_signature(batch: &RecordBatch) -> Vec<(String, DataType, bool)> {
    batch
        .schema()
        .fields()
        .iter()
        .map(|field| {
            (
                field.name().clone(),
                field.data_type().clone(),
                field.is_nullable(),
            )
        })
        .collect()
}

fn string_values(batch: &RecordBatch, column: &str) -> Vec<String> {
    batch
        .column_by_name(column)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|value| value.unwrap().to_owned())
        .collect()
}

#[test]
fn strict_runtime_properties_are_owner_validated_atomic_and_reopen_stable() {
    let project = tempfile::tempdir().unwrap();
    let path = project.path().to_str().unwrap();
    let ontology_path = project.path().join("strict-runtime-properties.yaml");

    // Open an empty project root first so FORMAT initialization can succeed.
    let mut graph = GraphForge::new(Some(path)).unwrap();
    std::fs::write(&ontology_path, STRICT_ONTOLOGY).unwrap();
    graph
        .adopt_ontology(AdoptOntologyRequest {
            context: context(0x2594),
            path: ontology_path,
            mode: OntologyMode::Strict,
        })
        .unwrap();

    // CREATE seeds direct Host properties, an inherited Asset property, and a
    // declared CONNECTED_TO relationship property. Relationship property writes
    // through CREATE bind owner-scoped runtime IDs; SET-on-edge still needs a
    // typed `rel_type_name` column in strict mode (#791 follow-up).
    graph
        .execute(
            "CREATE (a:Host {hostname: 'host-a', asset_name: 'Asset A'})\
             -[:CONNECTED_TO {weight: 9}]->\
             (b:Host {hostname: 'host-b', asset_name: 'Asset B'})",
        )
        .unwrap();
    graph
        .execute(
            "MATCH (host:Host {hostname: 'host-a'}) \
             SET host.asset_name = 'Asset A corrected'",
        )
        .unwrap();

    let node_query = "MATCH (host:Host) \
                      RETURN host.hostname AS hostname, host.asset_name AS asset_name \
                      ORDER BY hostname";
    let relation_query = "MATCH (:Host)-[connection:CONNECTED_TO]->(:Host) \
                          RETURN connection.weight AS weight";
    let nodes_before_reopen = only_batch(graph.execute(node_query).unwrap());
    let relation_before_reopen = only_batch(graph.execute(relation_query).unwrap());

    assert_eq!(
        schema_signature(&nodes_before_reopen),
        vec![
            ("hostname".into(), DataType::Utf8, true),
            ("asset_name".into(), DataType::Utf8, true),
        ]
    );
    assert_eq!(
        string_values(&nodes_before_reopen, "hostname"),
        vec!["host-a", "host-b"]
    );
    assert_eq!(
        string_values(&nodes_before_reopen, "asset_name"),
        vec!["Asset A corrected", "Asset B"]
    );
    assert_eq!(
        schema_signature(&relation_before_reopen),
        vec![("weight".into(), DataType::Int64, true)]
    );
    assert_eq!(
        relation_before_reopen
            .column_by_name("weight")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[9]
    );

    let catalog_before_failure = graph.runtime_catalog().lock().unwrap().to_record_batch();
    let generation_before_failure =
        graphforge_storage::resolve_project_generation(project.path()).unwrap();
    let graph_bytes_before_failure = generation_before_failure
        .participant_snapshot("graph", "snapshot")
        .unwrap()
        .unwrap()
        .bytes;
    let invalid_query = "MATCH (host:Host) WHERE host.hostname = 'host-a' SET host.weight = 99 RETURN host.hostname";
    let error = graph
        .execute(invalid_query)
        .expect_err("relation-owned property must fail on a Host before mutation");
    let GfError::Bind { msg, span } = error else {
        panic!("expected a span-rich bind error, got {error:?}");
    };
    assert!(msg.contains("property `weight` is not declared for entity `Host`"));
    assert_eq!(
        invalid_query[span.start..span.end].trim(),
        "host.weight",
        "bind span should identify the offending property access"
    );
    assert_eq!(
        graph.runtime_catalog().lock().unwrap().to_record_batch(),
        catalog_before_failure,
        "failed binding changed the shared runtime catalog"
    );
    let generation_after_failure =
        graphforge_storage::resolve_project_generation(project.path()).unwrap();
    assert_eq!(
        generation_after_failure.generation_uuid(),
        generation_before_failure.generation_uuid()
    );
    assert_eq!(
        generation_after_failure.manifest_sha256(),
        generation_before_failure.manifest_sha256()
    );
    assert_eq!(
        generation_after_failure
            .participant_snapshot("graph", "snapshot")
            .unwrap()
            .unwrap()
            .bytes,
        graph_bytes_before_failure,
        "failed binding changed the durable graph participant"
    );

    drop(graph);
    let reopened = GraphForge::new(Some(path)).unwrap();
    let nodes_after_reopen = only_batch(reopened.execute(node_query).unwrap());
    let relation_after_reopen = only_batch(reopened.execute(relation_query).unwrap());
    assert_eq!(
        schema_signature(&nodes_after_reopen),
        schema_signature(&nodes_before_reopen)
    );
    assert_eq!(
        string_values(&nodes_after_reopen, "hostname"),
        string_values(&nodes_before_reopen, "hostname")
    );
    assert_eq!(
        string_values(&nodes_after_reopen, "asset_name"),
        string_values(&nodes_before_reopen, "asset_name")
    );
    assert_eq!(
        schema_signature(&relation_after_reopen),
        schema_signature(&relation_before_reopen)
    );
    assert_eq!(
        relation_after_reopen
            .column_by_name("weight")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        relation_before_reopen
            .column_by_name("weight")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
    );
}
