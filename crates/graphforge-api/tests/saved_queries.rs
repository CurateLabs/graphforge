//! Real-engine evidence for durable, historical, and portable saved analyses.

#[cfg(feature = "research")]
use std::collections::BTreeSet;
use std::collections::{BTreeMap, HashMap};

use arrow::array::Int64Array;
use graphforge_api::*;
use uuid::Uuid;

fn definition(name: &str) -> SavedQuery {
    SavedQuery {
        query_uuid: Uuid::now_v7(),
        name: name.into(),
        description: Some("People above a chosen age".into()),
        query: "MATCH (n:Person) WHERE n.age >= $minimum RETURN count(n) AS people".into(),
        parameters: BTreeMap::from([("minimum".into(), SavedQueryParameterType::Integer)]),
    }
}

fn parameters(minimum: i64) -> HashMap<String, IrLiteral> {
    HashMap::from([("minimum".into(), IrLiteral::Int(minimum))])
}

fn count(result: ExecutionResult) -> i64 {
    assert_eq!(result.schema.field(0).name(), "people");
    assert!(result.side_effects.is_none());
    result.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

fn run(graph: &GraphForge, definition: &SavedQuery, source: &SavedQuerySource) -> i64 {
    count(
        graph
            .execute_saved_query(definition.query_uuid, &parameters(30), source, None)
            .unwrap(),
    )
}

#[test]
fn in_memory_lifecycle_executes_parameterized_aggregate_and_preserves_failed_update() {
    let mut graph = GraphForge::new(None).unwrap();
    graph
        .execute("CREATE (:Person {age:20}), (:Person {age:40})")
        .unwrap();
    let original = definition("Older people");
    assert!(graph.saved_queries().unwrap().is_empty());
    assert_eq!(
        graph.create_saved_query(original.clone()).unwrap(),
        original
    );
    assert_eq!(graph.saved_queries().unwrap(), vec![original.clone()]);
    assert_eq!(run(&graph, &original, &SavedQuerySource::Current), 1);
    assert!(graph.create_saved_query(original.clone()).is_err());
    let mut same_name = original.clone();
    same_name.query_uuid = Uuid::now_v7();
    assert!(graph.create_saved_query(same_name).is_err());

    #[cfg(feature = "research")]
    let before = graph.research_project_summary().unwrap().identity;
    let mut invalid = original.clone();
    invalid.query = "CREATE (:Person {age:60})".into();
    invalid.parameters.clear();
    assert_eq!(
        graph.update_saved_query(invalid).unwrap_err().code(),
        "GF_VALIDATION"
    );
    #[cfg(feature = "research")]
    assert_eq!(graph.research_project_summary().unwrap().identity, before);
    assert_eq!(graph.saved_query(original.query_uuid).unwrap(), original);

    for params in [
        HashMap::new(),
        HashMap::from([("minimum".into(), IrLiteral::Str("30".into()))]),
        HashMap::from([("other".into(), IrLiteral::Int(30))]),
        HashMap::from([
            ("minimum".into(), IrLiteral::Int(30)),
            ("extra".into(), IrLiteral::Int(40)),
        ]),
    ] {
        assert_eq!(
            graph
                .execute_saved_query(
                    original.query_uuid,
                    &params,
                    &SavedQuerySource::Current,
                    None
                )
                .unwrap_err()
                .code(),
            "GF_VALIDATION"
        );
    }
    let token = CancellationToken::new();
    token.cancel();
    assert_eq!(
        graph
            .execute_saved_query(
                original.query_uuid,
                &parameters(30),
                &SavedQuerySource::Current,
                Some(&token)
            )
            .unwrap_err()
            .code(),
        "GF_CANCELLED"
    );
    let mut renamed = original.clone();
    renamed.name = "People by threshold".into();
    graph.update_saved_query(renamed.clone()).unwrap();
    assert_eq!(graph.saved_query(original.query_uuid).unwrap(), renamed);
    graph.delete_saved_query(original.query_uuid).unwrap();
    assert!(graph.saved_queries().unwrap().is_empty());
    assert_eq!(
        graph.saved_query(original.query_uuid).unwrap_err().code(),
        "GF_NOT_FOUND"
    );
    assert_eq!(
        graph.update_saved_query(original).unwrap_err().code(),
        "GF_NOT_FOUND"
    );
    assert_eq!(
        count(
            graph
                .execute("MATCH (n:Person) RETURN count(n) AS people")
                .unwrap()
        ),
        2
    );
}

#[test]
fn incompatible_target_schema_is_reported_without_mutating_data() {
    let mut graph = GraphForge::new(None).unwrap();
    let mut saved = definition("Numeric ages");
    saved.query = "MATCH (n:Person) RETURN n.age + 1 AS age".into();
    saved.parameters.clear();
    graph.create_saved_query(saved.clone()).unwrap();
    graph.execute("CREATE (:Person {age:'forty'})").unwrap();
    let error = graph
        .execute_saved_query(
            saved.query_uuid,
            &HashMap::new(),
            &SavedQuerySource::Current,
            None,
        )
        .unwrap_err();
    assert_eq!(error.code(), "GF_PLAN");
    assert_eq!(graph.saved_query(saved.query_uuid).unwrap(), saved);
    assert_eq!(
        count(
            graph
                .execute("MATCH (n:Person) RETURN count(n) AS people")
                .unwrap()
        ),
        1
    );
}

#[test]
#[cfg(feature = "research")]
fn reopen_and_retained_version_preserve_definition_and_execution_context() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let original = definition("Original threshold");
    let version_uuid = Uuid::now_v7();
    {
        let mut graph = GraphForge::new(root.to_str()).unwrap();
        graph.execute("CREATE (:Person {age:40})").unwrap();
        graph.create_saved_query(original.clone()).unwrap();
        let operation = graph
            .prepare_research_version(PrepareResearchVersionRequest {
                operation_uuid: Uuid::now_v7(),
                version_uuid,
                context_uuid: Uuid::now_v7(),
                label: Some("Saved analysis".into()),
                description: None,
                created_at: 1,
                required_versions: BTreeSet::new(),
            })
            .unwrap();
        graph
            .commit_research_version_operation(operation, &CancellationToken::new())
            .unwrap();
        graph.execute("CREATE (:Person {age:50})").unwrap();
        let mut changed = original.clone();
        changed.name = "Current threshold".into();
        changed.query =
            "MATCH (n:Person) WHERE n.age >= $minimum RETURN count(n) + 10 AS people".into();
        graph.update_saved_query(changed).unwrap();
        assert_eq!(run(&graph, &original, &SavedQuerySource::Current), 12);
        graph.delete_saved_query(original.query_uuid).unwrap();
    }
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    assert!(graph.saved_queries().unwrap().is_empty());
    let source = SavedQuerySource::Version { version_uuid };
    let historical = graph.open_research_version(version_uuid).unwrap();
    assert_eq!(
        historical.saved_query(original.query_uuid).unwrap(),
        original
    );
    assert_eq!(historical.saved_queries().unwrap(), vec![original.clone()]);
    assert_eq!(
        graph.saved_query_at(original.query_uuid, &source).unwrap(),
        original
    );
    assert_eq!(
        graph.saved_queries_at(&source).unwrap(),
        vec![original.clone()]
    );
    assert_eq!(run(&graph, &original, &source), 1);
    graph.create_saved_query(original.clone()).unwrap();
    drop(graph);
    let graph = GraphForge::new(root.to_str()).unwrap();
    assert_eq!(graph.saved_query(original.query_uuid).unwrap(), original);
    assert_eq!(run(&graph, &original, &SavedQuerySource::Current), 2);
    let unknown_version = SavedQuerySource::Version {
        version_uuid: Uuid::now_v7(),
    };
    assert!(
        graph
            .execute_saved_query(original.query_uuid, &parameters(30), &unknown_version, None)
            .is_err()
    );
}

#[test]
#[cfg(feature = "portable")]
fn portable_roundtrip_preserves_definitions_without_execution_or_parameter_values() {
    let directory = tempfile::tempdir().unwrap();
    let mut graph = GraphForge::new(None).unwrap();
    graph.execute("CREATE (:Person {age:40})").unwrap();
    let original = definition("Portable threshold");
    graph.create_saved_query(original.clone()).unwrap();
    let unbound = SavedQuery {
        query_uuid: Uuid::now_v7(),
        name: "Bind only when explicitly run".into(),
        description: None,
        query: "RETURN 1 / 0 AS value".into(),
        parameters: BTreeMap::new(),
    };
    graph.create_saved_query(unbound.clone()).unwrap();
    // An actual prior execution must not become part of the saved definition.
    assert_eq!(run(&graph, &original, &SavedQuerySource::Current), 1);
    let package = directory.path().join("analysis-package");
    graph
        .export_portable_v2(
            &PortableV2ExportRequest {
                selection: PortableSelection::Current,
                output_path: package.clone(),
                representation: PortableV2Output::Expanded,
                profile: PortableV2SelectionProfile::Complete,
                subset: None,
                limits: PortableV2Limits::default(),
            },
            None,
            |_| {},
        )
        .unwrap();
    let imported = directory.path().join("imported");
    GraphForge::import_portable_v2(
        &imported,
        &PortableV2ImportRequest {
            input: package,
            operation_id: OperationId(Uuid::now_v7()),
            limits: PortableV2Limits::default(),
        },
        None,
    )
    .unwrap();
    let graph = GraphForge::new(imported.to_str()).unwrap();
    assert_eq!(graph.saved_query(original.query_uuid).unwrap(), original);
    assert_eq!(graph.saved_query(unbound.query_uuid).unwrap(), unbound);
    assert_eq!(run(&graph, &original, &SavedQuerySource::Current), 1);
    assert!(
        graph
            .execute_saved_query(
                unbound.query_uuid,
                &HashMap::new(),
                &SavedQuerySource::Current,
                None
            )
            .is_err()
    );
    let generation = graphforge_storage::resolve_project_generation(&imported).unwrap();
    let snapshot = generation
        .participant_snapshot(
            graphforge_storage::WORKSPACE_CAPABILITY_ID,
            graphforge_storage::WORKSPACE_SAVED_QUERIES_FAMILY,
        )
        .unwrap()
        .unwrap();
    let stored: serde_json::Value = serde_json::from_slice(&snapshot.bytes).unwrap();
    assert_eq!(
        stored["queries"][original.query_uuid.to_string()]["parameters"]["minimum"],
        "integer"
    );
    assert!(
        !String::from_utf8(snapshot.bytes)
            .unwrap()
            .contains("rows_produced")
    );
}

#[test]
fn stale_facade_cannot_overwrite_a_concurrent_saved_definition() {
    let directory = tempfile::tempdir().unwrap();
    let mut first = GraphForge::new(directory.path().to_str()).unwrap();
    let mut second = GraphForge::new(directory.path().to_str()).unwrap();
    let original = definition("First writer");
    first.create_saved_query(original.clone()).unwrap();
    assert_eq!(
        second
            .create_saved_query(definition("Stale writer"))
            .unwrap_err()
            .code(),
        "GF_WRITE_CONFLICT"
    );
    assert_eq!(first.saved_queries().unwrap(), vec![original]);
}

#[test]
fn json_parameters_follow_declarations_across_integral_numeric_representations() {
    let mut graph = GraphForge::new(None).unwrap();
    for (kind, value, data_type) in [
        (
            SavedQueryParameterType::Float,
            serde_json::json!(1),
            arrow::datatypes::DataType::Float64,
        ),
        (
            SavedQueryParameterType::Integer,
            serde_json::json!(4_294_967_296.0),
            arrow::datatypes::DataType::Int64,
        ),
        (
            SavedQueryParameterType::Boolean,
            serde_json::json!(true),
            arrow::datatypes::DataType::Boolean,
        ),
        (
            SavedQueryParameterType::String,
            serde_json::json!("text"),
            arrow::datatypes::DataType::Utf8,
        ),
    ] {
        let saved = SavedQuery {
            query_uuid: Uuid::now_v7(),
            name: format!("Parameter {kind:?}"),
            description: None,
            query: "RETURN $value AS value".into(),
            parameters: BTreeMap::from([("value".into(), kind)]),
        };
        graph.create_saved_query(saved.clone()).unwrap();
        let result = graph
            .execute_saved_query_json(
                saved.query_uuid,
                &HashMap::from([("value".into(), value)]),
                &SavedQuerySource::Current,
                None,
            )
            .unwrap();
        assert_eq!(result.schema.field(0).data_type(), &data_type);
        assert_eq!(result.stats.rows_produced, 1);
        if kind == SavedQueryParameterType::Integer {
            for invalid in [
                serde_json::json!(1.5),
                serde_json::json!(9_007_199_254_740_992.0),
                serde_json::Value::Null,
            ] {
                assert_eq!(
                    graph
                        .execute_saved_query_json(
                            saved.query_uuid,
                            &HashMap::from([("value".into(), invalid)]),
                            &SavedQuerySource::Current,
                            None
                        )
                        .unwrap_err()
                        .code(),
                    "GF_VALIDATION"
                );
            }
        }
    }
}

#[test]
#[cfg(not(feature = "research"))]
fn lean_saved_queries_refuse_historical_context_explicitly() {
    let graph = GraphForge::new(None).unwrap();
    let source = SavedQuerySource::Version {
        version_uuid: Uuid::now_v7(),
    };
    assert_eq!(
        graph.saved_queries_at(&source).unwrap_err().code(),
        "GF_CAPABILITY_DISABLED"
    );
}

#[test]
fn collection_bounds_fail_without_partial_arrow_results() {
    let mut graph = GraphForge::new(None).unwrap();
    let rows = SavedQuery {
        query_uuid: Uuid::now_v7(),
        name: "Too many output rows".into(),
        description: None,
        query: "UNWIND range(1, 1000001) AS i RETURN i".into(),
        parameters: BTreeMap::new(),
    };
    graph.create_saved_query(rows.clone()).unwrap();
    assert_eq!(
        graph
            .execute_saved_query(
                rows.query_uuid,
                &HashMap::new(),
                &SavedQuerySource::Current,
                None
            )
            .unwrap_err()
            .code(),
        "GF_RESOURCE_LIMIT"
    );

    let bytes = SavedQuery {
        query_uuid: Uuid::now_v7(),
        name: "Too much output data".into(),
        description: None,
        query: "RETURN $text AS value".into(),
        parameters: BTreeMap::from([("text".into(), SavedQueryParameterType::String)]),
    };
    graph.create_saved_query(bytes.clone()).unwrap();
    let params = HashMap::from([(
        "text".into(),
        IrLiteral::Str("x".repeat(MAX_SAVED_QUERY_RESULT_BYTES + 1)),
    )]);
    assert_eq!(
        graph
            .execute_saved_query(bytes.query_uuid, &params, &SavedQuerySource::Current, None)
            .unwrap_err()
            .code(),
        "GF_RESOURCE_LIMIT"
    );
}
