//! Direct release-conformance evidence for the public M18 Rust facade.

use std::collections::{BTreeMap, HashSet};

use arrow::array::{FixedSizeBinaryArray, Float64Array};
use arrow::datatypes::DataType;
use graphforge_api::{
    Algorithm, AnalyzeAlgorithm, AnalyzeOptions, EmbeddingAnalyzeOptions, EmbeddingOptions,
    FastRpOptions, GraphForge, InvocationDescriptor, InvocationParameter, RankAlgorithm,
    RankOptions, algorithm_descriptor_contracts,
};
use tempfile::TempDir;

#[test]
fn all_94_public_algorithm_contracts_are_unique_typed_and_repeatable() {
    let first = algorithm_descriptor_contracts();
    let second = algorithm_descriptor_contracts();
    assert_eq!(
        first, second,
        "catalog order and descriptors must be stable"
    );
    assert_eq!(first.len(), 94);

    let identities = first
        .iter()
        .map(|contract| {
            let algorithm = contract.algorithm;
            assert_eq!(
                Algorithm::parse(algorithm.verb(), algorithm.as_str()).unwrap(),
                algorithm
            );
            assert_eq!(contract.algorithm_version, 1);
            assert_eq!(contract.result_schema_version, 1);

            let schema = algorithm.result_schema();
            assert!(
                !schema.fields.is_empty(),
                "{algorithm:?} has no result schema"
            );
            for field in schema.fields {
                assert!(!field.name.is_empty(), "{algorithm:?} has an unnamed field");
            }
            (algorithm.verb().as_str(), algorithm.as_str())
        })
        .collect::<HashSet<_>>();
    assert_eq!(
        identities.len(),
        94,
        "every entry is classified exactly once"
    );

    let parameters = BTreeMap::from([
        ("directed".into(), InvocationParameter::Bool(true)),
        ("label".into(), InvocationParameter::Utf8("Person".into())),
        ("via".into(), InvocationParameter::Utf8("*".into())),
    ]);
    let descriptor = InvocationDescriptor::new(
        Algorithm::Rank(RankAlgorithm::Degree),
        [7; 32],
        parameters.clone(),
    )
    .unwrap();
    let repeated =
        InvocationDescriptor::new(Algorithm::Rank(RankAlgorithm::Degree), [7; 32], parameters)
            .unwrap();
    assert_eq!(descriptor.canonical_bytes(), repeated.canonical_bytes());
    assert_eq!(descriptor.fingerprint(), repeated.fingerprint());
    assert_eq!(
        descriptor.result_schema_fingerprint(),
        repeated.result_schema_fingerprint()
    );
}

#[test]
fn descriptor_byte_and_embedding_dispatch_are_publicly_covered() {
    let graph = GraphForge::new(None).unwrap();
    graph
        .execute("CREATE (:Person)-[:KNOWS]->(:Person)")
        .unwrap();
    let rank_options = RankOptions {
        by: RankAlgorithm::Degree,
        via: Some("KNOWS".into()),
        directed: true,
        write_property: None,
    };
    let rank_descriptor = graph
        .prepare_rank_invocation("Person", &rank_options)
        .unwrap();
    let bytes_result = graph
        .invoke_descriptor_bytes(rank_descriptor.canonical_bytes())
        .unwrap();
    assert_eq!(bytes_result, graph.rank("Person", rank_options).unwrap());
    assert_eq!(
        graph
            .invoke_descriptor_bytes(rank_descriptor.canonical_bytes())
            .unwrap(),
        bytes_result
    );

    let embedding_options = EmbeddingAnalyzeOptions {
        by: AnalyzeAlgorithm::FastRandomProjection,
        via: Some("KNOWS".into()),
        directed: true,
        weight: None,
        options: EmbeddingOptions::FastRandomProjection(FastRpOptions {
            dimensions: 4,
            seed: 17,
            ..FastRpOptions::default()
        }),
    };
    let embedding_descriptor = graph
        .prepare_embedding_invocation(Some("Person"), &embedding_options)
        .unwrap();
    let direct = graph
        .analyze_embedding(Some("Person"), &embedding_options)
        .unwrap();
    let dispatched = graph
        .invoke_embedding_descriptor(&embedding_descriptor)
        .unwrap();
    assert_eq!(dispatched, direct);
    assert_eq!(
        graph
            .invoke_embedding_descriptor(&embedding_descriptor)
            .unwrap(),
        dispatched
    );
}

#[test]
fn persisted_public_rank_is_exact_after_repeat_and_reopen_and_unavailable_is_stable() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().to_str().unwrap();
    let graph = GraphForge::new(Some(path)).unwrap();
    graph
        .execute("CREATE (:Person {name:'a'})-[:KNOWS]->(:Person {name:'b'})")
        .unwrap();

    let options = RankOptions {
        by: RankAlgorithm::Degree,
        via: Some("KNOWS".into()),
        directed: true,
        write_property: None,
    };
    let first = graph.rank("Person", options.clone()).unwrap();
    let repeated = graph.rank("Person", options.clone()).unwrap();
    assert_eq!(first, repeated);
    assert_eq!(
        first
            .schema()
            .fields()
            .iter()
            .map(|f| (f.name().as_str(), f.data_type()))
            .collect::<Vec<_>>(),
        [
            ("node_uuid", &DataType::FixedSizeBinary(16)),
            ("score", &DataType::Float64),
            ("name", &DataType::Utf8),
        ]
    );
    assert_eq!(first.num_rows(), 2);
    let ids = first
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(ids.value_length(), 16);
    let scores = first
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!((scores.value(0) - 1.0).abs() < f64::EPSILON);
    assert!(scores.value(1).abs() < f64::EPSILON);

    let unavailable = || {
        graph
            .analyze(
                Some("Person"),
                AnalyzeOptions {
                    by: AnalyzeAlgorithm::Node2Vec,
                    ..AnalyzeOptions::default()
                },
            )
            .unwrap_err()
            .to_string()
    };
    assert_eq!(unavailable(), unavailable());
    assert!(unavailable().contains("unavailable"));
    drop(graph);

    let reopened = GraphForge::new(Some(path)).unwrap();
    assert_eq!(reopened.rank("Person", options).unwrap(), first);
}
