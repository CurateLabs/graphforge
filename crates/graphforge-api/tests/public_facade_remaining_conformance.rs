//! Exact contracts for the remaining named public-facade release surfaces.

use std::collections::HashMap;
use std::fs::File;

use arrow::array::{Array, FixedSizeBinaryArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Schema};
use futures::TryStreamExt;
use graphforge_api::{
    AnalyzeAlgorithm, AnalyzeOptions, ApplyValidTimeRequest, AssertionStatus,
    BeliefProjectionPolicyV1, CapabilityId, CheckpointRequest, ClusterOptions,
    EmbeddingAnalyzeOptions, EmbeddingOptions, EnableCapabilityRequest, FastRpOptions, FindOptions,
    GraphForge, HypothesisSelectionPolicyV1, IrLiteral, ListAlgorithmRunsRequest,
    ListAssertionStatusRequest, ListAssertionSupersessionsRequest, ListAssertionValidityRequest,
    ListAssertionsRequest, ListConfidenceAssessmentsRequest, ListEvidenceLinksRequest,
    ListHypothesisGroupsRequest, ListHypothesisMembershipRequest, ListHypothesisSelectionRequest,
    ListReasoningRequest, OperationId, PageRequest, PathsOptions, PropValue,
    ProvenanceHistoryRequest, RankOptions, ResolveBeliefProjectionRequest, SimilarOptions,
    StatuslessPolicyV1, SupersessionBranchPolicyV1, WriteContext,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tempfile::TempDir;
use uuid::Uuid;

fn operation(seed: u128) -> OperationId {
    OperationId(Uuid::from_u128(seed))
}

fn uuid7(seed: u8) -> Uuid {
    let mut bytes = [seed; 16];
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn enable(graph: &GraphForge, capability_id: CapabilityId, seed: u128) {
    graph
        .enable_capability(EnableCapabilityRequest {
            context: WriteContext {
                operation_uuid: operation(seed),
                actor_uuid: None,
            },
            capability_id,
            capability_version: 1,
        })
        .unwrap();
}

fn field_names(result: &graphforge_api::ExecutionResult) -> Vec<&str> {
    result
        .schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect()
}

fn assert_uuid_query_schema(schema: &Schema, field_name: &str) {
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), field_name);
    assert_eq!(schema.field(0).data_type(), &DataType::FixedSizeBinary(16));
    assert!(!schema.field(0).is_nullable());
    assert_eq!(
        schema
            .metadata()
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>(),
        [
            "graphforge.ir_version".to_owned(),
            "graphforge.ontology_mode".to_owned(),
            "graphforge.query_id".to_owned(),
        ]
        .into_iter()
        .collect()
    );
    assert_eq!(schema.metadata()["graphforge.ir_version"], "0.3.0");
    assert_eq!(schema.metadata()["graphforge.ontology_mode"], "exploratory");
    let query_id = Uuid::parse_str(&schema.metadata()["graphforge.query_id"]).unwrap();
    assert_eq!(query_id.get_version_num(), 7);
    assert_eq!(
        query_id.hyphenated().to_string(),
        schema.metadata()["graphforge.query_id"]
    );
}

fn history_project() -> (TempDir, GraphForge, Uuid) {
    let directory = TempDir::new().unwrap();
    let graph = GraphForge::new(directory.path().to_str()).unwrap();
    let first = graph.add_node("Person", &HashMap::new()).unwrap();
    let second = graph.add_node("Person", &HashMap::new()).unwrap();
    graph
        .add_edge(&first, "KNOWS", &second, &HashMap::new())
        .unwrap();
    enable(&graph, CapabilityId::Provenance, 10);
    enable(&graph, CapabilityId::Knowledge, 11);
    enable(&graph, CapabilityId::Epistemic, 12);
    enable(&graph, CapabilityId::ValidTime, 13);
    (directory, graph, first.uuid)
}

#[test]
fn public_lifecycle_inventory_covers_remaining_facade_methods() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().to_str().unwrap();
    let graph = GraphForge::new(Some(path)).unwrap();
    graph
        .add_node(
            "Person",
            &HashMap::from([("name".into(), PropValue::Str("Alice".into()))]),
        )
        .unwrap();

    assert_eq!(graph.labels().unwrap(), ["Person".to_owned()]);
    assert!(graph.relationship_types().unwrap().is_empty());
    assert_eq!(graph.node_count("").unwrap(), 1);
    assert_eq!(graph.node_count("Person").unwrap(), 1);
    assert_eq!(graph.node_count("Missing").unwrap(), 0);
    let inspection = graph.schema().unwrap();
    assert_eq!(inspection.num_rows(), 1);
    assert_eq!(
        inspection
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["label", "node_count", "rel_type", "rel_count"]
    );

    let configuration = graph.workspace_configuration().unwrap();
    assert_eq!(configuration.contract_version, 1);
    assert!(configuration.capability_configuration.is_empty());
    assert!(configuration.embedding_configuration.is_empty());
    let ontology = graph.workspace_ontology().unwrap();
    assert_eq!(ontology.contract_version, 1);
    assert!(ontology.canonical_ontology.is_none());
    assert!(ontology.canonical_ontology_sha256.is_none());

    drop(graph);
    let reopened = GraphForge::new(Some(path)).unwrap();
    assert_eq!(reopened.workspace_configuration().unwrap(), configuration);
    assert_eq!(reopened.workspace_ontology().unwrap(), ontology);
}

#[test]
fn public_history_list_surfaces_are_exact() {
    let (_directory, graph, _first) = history_project();
    let runs = graph
        .list_algorithm_runs(ListAlgorithmRunsRequest::default())
        .unwrap();
    assert_eq!(runs.batches[0].num_rows(), 0);
    assert_eq!(
        field_names(&runs),
        [
            "run_uuid",
            "algorithm",
            "algorithm_version",
            "descriptor_version",
            "descriptor",
            "projection_fingerprint",
            "provenance_uuid",
            "started_at",
            "contract_version",
        ]
    );
    let groups = graph
        .list_hypothesis_groups(&ListHypothesisGroupsRequest::default())
        .unwrap();
    assert_eq!(groups.batches[0].num_rows(), 0);
    assert_eq!(
        field_names(&groups),
        [
            "group_uuid",
            "question_key",
            "provenance_uuid",
            "recorded_at",
            "contract_version",
        ]
    );
    let selections = graph
        .list_hypothesis_selection(&ListHypothesisSelectionRequest::default())
        .unwrap();
    assert_eq!(selections.batches[0].num_rows(), 0);
    assert_eq!(
        field_names(&selections),
        [
            "selection_event_uuid",
            "operation_uuid",
            "group_uuid",
            "selected_assertion_uuid",
            "reasoning_uuid",
            "provenance_uuid",
            "recorded_at",
            "contract_version",
        ]
    );
    let validity = graph
        .list_assertion_validity(ListAssertionValidityRequest::default())
        .unwrap();
    assert_eq!(validity.batches[0].num_rows(), 0);
    assert_eq!(
        field_names(&validity),
        [
            "validity_event_uuid",
            "assertion_uuid",
            "valid_from",
            "valid_to",
            "reasoning_uuid",
            "provenance_uuid",
            "recorded_at",
            "contract_version",
        ]
    );
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "the release inventory requires every CheckpointView read wrapper in one named test"
)]
fn checkpoint_view_inventory_covers_all_read_wrappers() {
    let (_directory, graph, first) = history_project();
    graph
        .checkpoint(CheckpointRequest {
            name: "History".into(),
            description: None,
            idempotency_key: operation(20),
            actor_uuid: None,
        })
        .unwrap();
    let view = graph.open_checkpoint("History").unwrap();

    assert_eq!(
        view.workspace_configuration().unwrap(),
        graph.workspace_configuration().unwrap()
    );
    assert_eq!(
        view.workspace_ontology().unwrap(),
        graph.workspace_ontology().unwrap()
    );
    let runs = view
        .list_algorithm_runs(ListAlgorithmRunsRequest::default())
        .unwrap();
    assert_eq!(field_names(&runs)[0], "run_uuid");
    let groups = view
        .list_hypothesis_groups(&ListHypothesisGroupsRequest::default())
        .unwrap();
    assert_eq!(field_names(&groups)[0], "group_uuid");
    let selection = view
        .list_hypothesis_selection(&ListHypothesisSelectionRequest::default())
        .unwrap();
    assert_eq!(field_names(&selection)[0], "selection_event_uuid");
    let validity = view
        .list_assertion_validity(ListAssertionValidityRequest::default())
        .unwrap();
    assert_eq!(field_names(&validity)[0], "validity_event_uuid");
    assert!(
        runs.batches[0].num_rows() == 0
            && groups.batches[0].num_rows() == 0
            && selection.batches[0].num_rows() == 0
            && validity.batches[0].num_rows() == 0
    );

    let unknown = uuid7(99);
    for error in [
        view.algorithm_run(unknown, None).unwrap_err(),
        view.provenance_event(unknown, None).unwrap_err(),
        view.assertion(unknown, None).unwrap_err(),
        view.confidence_assessment(unknown, None).unwrap_err(),
        view.evidence_link(unknown, None).unwrap_err(),
        view.reasoning(unknown, None).unwrap_err(),
        view.assertion_status(unknown).unwrap_err(),
    ] {
        assert_eq!(error.code(), "GF_NOT_FOUND");
    }
    assert_eq!(
        view.algorithm_run_events(unknown, PageRequest::default())
            .unwrap_err()
            .code(),
        "GF_NOT_FOUND"
    );
    assert_eq!(
        view.assertion_graph_refs(unknown, PageRequest::default())
            .unwrap_err()
            .code(),
        "GF_NOT_FOUND"
    );
    assert_eq!(
        view.confidence_inputs(unknown, PageRequest::default())
            .unwrap_err()
            .code(),
        "GF_NOT_FOUND"
    );

    assert_eq!(
        field_names(
            &view
                .list_provenance_history(ProvenanceHistoryRequest::default())
                .unwrap()
        )[0],
        "provenance_uuid"
    );
    assert_eq!(
        field_names(
            &view
                .list_assertions(ListAssertionsRequest::default())
                .unwrap()
        )[0],
        "assertion_uuid"
    );
    assert_eq!(
        field_names(
            &view
                .list_evidence_links(ListEvidenceLinksRequest::default())
                .unwrap()
        )[0],
        "evidence_uuid"
    );
    assert_eq!(
        field_names(
            &view
                .list_confidence_assessments(ListConfidenceAssessmentsRequest::default())
                .unwrap()
        )[0],
        "confidence_uuid"
    );
    assert_eq!(
        field_names(
            &view
                .list_reasoning(ListReasoningRequest::default())
                .unwrap()
        )[0],
        "reasoning_uuid"
    );
    assert_eq!(
        field_names(
            &view
                .list_assertion_status(ListAssertionStatusRequest::default())
                .unwrap()
        )[0],
        "status_event_uuid"
    );
    assert_eq!(
        field_names(
            &view
                .list_assertion_supersessions(ListAssertionSupersessionsRequest::default())
                .unwrap()
        )[0],
        "supersession_uuid"
    );
    assert_eq!(
        field_names(
            &view
                .list_hypothesis_membership(&ListHypothesisMembershipRequest::default())
                .unwrap()
        )[0],
        "membership_event_uuid"
    );
    let members = view.hypothesis_members(unknown).unwrap();
    assert_eq!(field_names(&members)[0], "membership_event_uuid");
    assert_eq!(members.batches[0].num_rows(), 0);
    let current_selection = view.hypothesis_selection(unknown).unwrap();
    assert_eq!(field_names(&current_selection)[0], "selection_event_uuid");
    assert_eq!(current_selection.batches[0].num_rows(), 0);

    assert_eq!(
        view.rank("Person", RankOptions::default())
            .unwrap()
            .num_rows(),
        2
    );
    assert_eq!(
        view.cluster("Person", ClusterOptions::default())
            .unwrap()
            .num_rows(),
        2
    );
    let source = graphforge_api::NodeSelector::Uuid(first);
    assert!(
        !view
            .paths(Some(&source), None, PathsOptions::default())
            .unwrap()
            .schema()
            .fields()
            .is_empty()
    );
    assert!(
        !view
            .analyze(Some("Person"), AnalyzeOptions::default())
            .unwrap()
            .schema()
            .fields()
            .is_empty()
    );
    let embedding = EmbeddingAnalyzeOptions {
        by: AnalyzeAlgorithm::FastRandomProjection,
        via: Some("KNOWS".into()),
        directed: true,
        weight: None,
        options: EmbeddingOptions::FastRandomProjection(FastRpOptions {
            dimensions: 4,
            seed: 7,
            ..FastRpOptions::default()
        }),
    };
    assert_eq!(
        view.analyze_embedding(Some("Person"), &embedding)
            .unwrap()
            .num_rows(),
        2
    );
    assert_eq!(
        view.similar("Person", SimilarOptions::default())
            .unwrap()
            .num_rows(),
        0
    );
    assert_eq!(
        view.find(FindOptions::default()).unwrap_err().code(),
        "GF_VALIDATION"
    );

    assert!(view.embedding_spaces().unwrap().is_empty());
    assert_eq!(
        view.embedding_space(None).unwrap_err().code(),
        "GF_VALIDATION"
    );
    assert_eq!(
        view.inspect_embedding_space_freshness(None, false)
            .unwrap_err()
            .code(),
        "GF_VALIDATION"
    );
    assert_eq!(
        view.inspect_embedding_refresh(None).unwrap_err().code(),
        "GF_VALIDATION"
    );

    assert_eq!(
        field_names(&view.epistemic_snapshot(i64::MAX).unwrap())[0],
        "entity_kind"
    );
    assert_eq!(
        field_names(
            &view
                .apply_valid_time(ApplyValidTimeRequest {
                    transaction_cutoff_micros: i64::MAX,
                    valid_time_micros: 0,
                })
                .unwrap()
        )[0],
        "assertion_uuid"
    );
    let projection = view
        .resolve_belief_projection(ResolveBeliefProjectionRequest {
            transaction_cutoff_micros: i64::MAX,
            valid_time_micros: None,
            policy: BeliefProjectionPolicyV1 {
                included_statuses: vec![AssertionStatus::Supported],
                statusless: StatuslessPolicyV1::Exclude,
                supersession_branches: SupersessionBranchPolicyV1::Reject,
                hypotheses: HypothesisSelectionPolicyV1::ExcludeUnselectedGroup,
            },
        })
        .unwrap();
    assert!(projection.source_record_uuids().is_empty());
}

#[test]
fn parameterized_stream_and_parquet_surfaces_are_exact() {
    let directory = TempDir::new().unwrap();
    let graph = GraphForge::new(directory.path().to_str()).unwrap();
    graph
        .execute("CREATE (:Person {name: 'Alice', score: 7}), (:Person {name: 'Bob', score: 8})")
        .unwrap();
    let params = HashMap::from([("minimum".into(), IrLiteral::Int(7))]);
    let stream = graph
        .execute_stream_with_params(
            "MATCH (n:Person) WHERE n.score >= $minimum RETURN n.name AS name, n.score AS score ORDER BY name",
            &params,
        )
        .unwrap();
    let batches = futures::executor::block_on(stream.try_collect::<Vec<_>>()).unwrap();
    assert_eq!(batches.len(), 1);
    let names = batches[0]
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!([names.value(0), names.value(1)], ["Alice", "Bob"]);

    let destination = directory.path().join("result.parquet");
    graph
        .execute_to_parquet_with_params(
            "MATCH (n:Person) WHERE n.score >= $minimum RETURN n.name AS name, n.score AS score ORDER BY name",
            &params,
            destination.to_str().unwrap(),
        )
        .unwrap();
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(File::open(destination).unwrap())
        .unwrap()
        .build()
        .unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.schema().field(0).name(), "name");
    assert_eq!(batch.schema().field(1).name(), "score");
    let scores = batch
        .column_by_name("score")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!([scores.value(0), scores.value(1)], [7, 8]);
    assert!(reader.next().is_none());
}

#[test]
fn typed_uuid_parameters_preserve_identity_across_surfaces_and_reopen() {
    let directory = TempDir::new().unwrap();
    let graph = GraphForge::new(directory.path().to_str()).unwrap();
    let alice = graph
        .add_node(
            "Person",
            &HashMap::from([("name".into(), PropValue::Str("Alice".into()))]),
        )
        .unwrap();
    let bob = graph
        .add_node(
            "Person",
            &HashMap::from([("name".into(), PropValue::Str("Bob".into()))]),
        )
        .unwrap();
    let carol = graph
        .add_node(
            "Person",
            &HashMap::from([("name".into(), PropValue::Str("Carol".into()))]),
        )
        .unwrap();
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}) CREATE (a)-[:KNOWS]->(b)",
        )
        .unwrap();

    let node_params = HashMap::from([("uuid".into(), IrLiteral::Uuid(*alice.uuid.as_bytes()))]);
    let query = "MATCH (n:Person) WHERE n.node_uuid = $uuid RETURN n.node_uuid AS node_uuid";
    let result = graph.execute_with_params(query, &node_params).unwrap();
    assert_eq!(result.stats.rows_produced, 1);
    assert_uuid_query_schema(result.batches[0].schema().as_ref(), "node_uuid");
    assert_eq!(
        result.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(0),
        alice.uuid.as_bytes()
    );
    assert_eq!(result.schema.as_ref(), result.batches[0].schema().as_ref());
    let reversed = graph
        .execute_with_params(
            "MATCH (n:Person) WHERE $uuid = n.node_uuid RETURN n.node_uuid AS node_uuid",
            &node_params,
        )
        .unwrap();
    assert_eq!(reversed.stats.rows_produced, 1);

    let ordinary_string = graph
        .execute_with_params(
            query,
            &HashMap::from([("uuid".into(), IrLiteral::Str(alice.uuid.to_string()))]),
        )
        .unwrap();
    assert_eq!(ordinary_string.stats.rows_produced, 0);

    for incompatible_query in [
        "MATCH (n:Person) WHERE n.node_uuid <> $uuid RETURN n.node_uuid",
        "MATCH (n:Person) WHERE n.node_uuid IN [$uuid] RETURN n.node_uuid",
        "RETURN $uuid",
        "RETURN toString($uuid)",
        "RETURN size([$uuid])",
        "RETURN [$uuid]",
        "RETURN 1 AS value SKIP $uuid",
        "RETURN 1 AS value LIMIT $uuid",
        "MATCH (n:Person) WHERE (($uuid = n.name) OR false) RETURN n.node_uuid",
        "MATCH (n:Person) WHERE n.name IN [$uuid] RETURN n.node_uuid",
        "MATCH (n:Person) RETURN n.name = $uuid AS bad",
        "MATCH (n:Person) WITH n, n.name = $uuid AS bad RETURN bad",
        "MATCH (n:Person) RETURN n.name ORDER BY n.name = $uuid",
        "MATCH (n:Person) UNWIND [n.name = $uuid] AS bad RETURN bad",
        "MATCH (n:Person {probe: n.name = $uuid}) RETURN n.node_uuid",
        "MATCH (n:Person) SET n.probe = (n.name = $uuid) RETURN n.node_uuid",
        "MATCH (n:Person) MERGE (m:Other {probe: n.name = $uuid}) RETURN m.node_uuid",
        "MATCH (n:Person) DELETE (n.name = $uuid)",
        "MATCH (n:Person)-[r:KNOWS]->() WHERE r.node_uuid = $uuid RETURN r.edge_uuid",
        "MATCH (n:Person)-[r:KNOWS]->() WHERE n.edge_uuid = $uuid RETURN n.node_uuid",
    ] {
        let incompatible = graph
            .execute_with_params(incompatible_query, &node_params)
            .unwrap_err();
        assert_eq!(incompatible.code(), "GF_VALIDATION", "{incompatible_query}");
        assert_eq!(
            incompatible.to_string(),
            "validation error: typed UUID parameter `$uuid` is only supported as a direct node_uuid or edge_uuid identity equality predicate",
            "{incompatible_query}"
        );
        let incompatible_stream = graph
            .execute_stream_with_params(incompatible_query, &node_params)
            .err()
            .unwrap();
        assert_eq!(
            incompatible_stream.code(),
            "GF_VALIDATION",
            "{incompatible_query}"
        );
        assert_eq!(
            incompatible_stream.to_string(),
            incompatible.to_string(),
            "{incompatible_query}"
        );
    }

    for nested in [
        IrLiteral::List(vec![IrLiteral::Uuid(*alice.uuid.as_bytes())]),
        IrLiteral::Map(vec![(
            "nested".into(),
            IrLiteral::List(vec![IrLiteral::Uuid(*alice.uuid.as_bytes())]),
        )]),
    ] {
        let error = graph
            .execute_with_params(
                "MATCH (n:Person) WHERE n.node_uuid = $uuid RETURN n.node_uuid",
                &HashMap::from([("uuid".into(), nested)]),
            )
            .unwrap_err();
        assert_eq!(error.code(), "GF_VALIDATION");
        assert_eq!(
            error.to_string(),
            "validation error: typed UUID parameter `$uuid` is only supported as a direct node_uuid or edge_uuid identity equality predicate"
        );
    }

    let deterministic = graph
        .execute_with_params(
            "RETURN $first, $second",
            &HashMap::from([
                ("second".into(), IrLiteral::Uuid([0x22; 16])),
                ("first".into(), IrLiteral::Uuid([0x11; 16])),
            ]),
        )
        .unwrap_err();
    assert_eq!(
        deterministic.to_string(),
        "validation error: typed UUID parameter `$first` is only supported as a direct node_uuid or edge_uuid identity equality predicate"
    );

    let ordered = graph
        .execute("MATCH (n:Person) RETURN n.node_uuid AS node_uuid ORDER BY node_uuid")
        .unwrap();
    assert_uuid_query_schema(ordered.batches[0].schema().as_ref(), "node_uuid");
    let ordered_uuids = ordered.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let actual = (0..ordered_uuids.len())
        .map(|row| ordered_uuids.value(row).to_vec())
        .collect::<Vec<_>>();
    let mut expected = [alice.uuid, bob.uuid, carol.uuid]
        .into_iter()
        .map(|uuid| uuid.as_bytes().to_vec())
        .collect::<Vec<_>>();
    expected.sort_unstable();
    assert_eq!(actual, expected, "UUID ordering must be byte-deterministic");

    let edge = graph
        .execute("MATCH ()-[r:KNOWS]->() RETURN r.edge_uuid AS edge_uuid")
        .unwrap();
    let edge_uuid = edge.batches[0]
        .column_by_name("edge_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap()
        .value(0)
        .try_into()
        .unwrap();
    let edge_params = HashMap::from([("uuid".into(), IrLiteral::Uuid(edge_uuid))]);
    let reversed_edge = graph
        .execute_with_params(
            "MATCH ()-[r:KNOWS]->() WHERE $uuid = r.edge_uuid RETURN r.edge_uuid AS edge_uuid",
            &edge_params,
        )
        .unwrap();
    assert_eq!(reversed_edge.stats.rows_produced, 1);
    let stream = graph
        .execute_stream_with_params(
            "MATCH ()-[r:KNOWS]->() WHERE r.edge_uuid = $uuid RETURN r.edge_uuid AS edge_uuid",
            &edge_params,
        )
        .unwrap();
    let streamed = futures::executor::block_on(stream.try_collect::<Vec<_>>()).unwrap();
    assert_eq!(
        streamed.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        1
    );
    let streamed_nonempty = streamed.iter().find(|batch| batch.num_rows() > 0).unwrap();
    assert_uuid_query_schema(streamed_nonempty.schema().as_ref(), "edge_uuid");
    assert_eq!(
        streamed_nonempty
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(0),
        edge_uuid
    );
    assert!(
        streamed
            .iter()
            .all(|batch| batch.schema().as_ref() == streamed_nonempty.schema().as_ref())
    );

    let destination = directory.path().join("uuid-result.parquet");
    graph
        .execute_to_parquet_with_params(query, &node_params, destination.to_str().unwrap())
        .unwrap();
    let parquet_builder =
        ParquetRecordBatchReaderBuilder::try_new(File::open(destination).unwrap()).unwrap();
    let graphforge_footer_keys = parquet_builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .into_iter()
        .flatten()
        .filter(|entry| entry.key.starts_with("graphforge."))
        .map(|entry| entry.key.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        graphforge_footer_keys,
        [
            "graphforge.ir_version",
            "graphforge.ontology_mode",
            "graphforge.query_id",
        ],
        "GraphForge Parquet metadata must be unique and key-sorted; reserved ARROW:schema is excluded"
    );
    assert_uuid_query_schema(parquet_builder.schema().as_ref(), "node_uuid");
    let parquet_fields = parquet_builder.schema().fields().clone();
    let exported = parquet_builder.build().unwrap().next().unwrap().unwrap();
    assert_eq!(exported.schema().fields(), &parquet_fields);
    assert_eq!(
        exported
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(0),
        alice.uuid.as_bytes()
    );
    drop(graph);

    let reopened = GraphForge::new(directory.path().to_str()).unwrap();
    let reopened_result = reopened.execute_with_params(query, &node_params).unwrap();
    assert_eq!(reopened_result.stats.rows_produced, 1);
    assert_uuid_query_schema(reopened_result.schema.as_ref(), "node_uuid");
    assert_eq!(
        reopened_result.schema.as_ref(),
        reopened_result.batches[0].schema().as_ref()
    );
    assert_eq!(
        reopened_result.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(0),
        alice.uuid.as_bytes()
    );
}
