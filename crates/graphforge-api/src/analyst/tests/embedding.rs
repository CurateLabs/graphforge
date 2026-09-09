use super::*;

#[test]
fn public_embedding_option_validation_stays_validation_only() {
    let valid = EmbeddingAnalyzeOptions {
        by: AnalyzeAlgorithm::Node2Vec,
        via: Some("KNOWS".to_owned()),
        directed: true,
        weight: None,
        options: EmbeddingOptions::Node2Vec(Node2VecOptions::default()),
    };
    validate_embedding_options(&valid).unwrap();

    let invalid = EmbeddingAnalyzeOptions {
        options: EmbeddingOptions::Node2Vec(Node2VecOptions {
            dimensions: 0,
            ..Node2VecOptions::default()
        }),
        ..valid
    };
    assert!(matches!(
        validate_embedding_options(&invalid),
        Err(GfError::Validation(_))
    ));
}

#[test]
fn node2vec_executes_through_typed_api_with_canonical_arrow_output() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap();
    let graph = GraphForge::new(Some(path)).unwrap();
    graph
        .execute(
            "CREATE (:Person {name:'Alice'})-[:KNOWS]->(:Person {name:'Bob'}), \
                 (:Person {name:'Carol'})",
        )
        .unwrap();
    let options = EmbeddingAnalyzeOptions {
        by: AnalyzeAlgorithm::Node2Vec,
        via: Some("KNOWS".to_owned()),
        directed: true,
        weight: None,
        options: EmbeddingOptions::Node2Vec(Node2VecOptions {
            dimensions: 2,
            walk_length: 2,
            walks_per_node: 1,
            window_size: 1,
            negative_samples: 1,
            epochs: 1,
            seed: 7,
            ..Node2VecOptions::default()
        }),
    };
    let first = graph.analyze_embedding(Some("Person"), &options).unwrap();
    assert_eq!(
        first,
        graph.analyze_embedding(Some("Person"), &options).unwrap()
    );
    assert_eq!(first.num_rows(), 3);
    assert_eq!(
        first
            .schema()
            .fields()
            .iter()
            .map(|field| (
                field.name().as_str(),
                field.data_type(),
                field.is_nullable()
            ))
            .collect::<Vec<_>>(),
        [
            ("node_uuid", &DataType::FixedSizeBinary(16), false),
            (
                "embedding",
                &DataType::FixedSizeList(
                    Arc::new(arrow::datatypes::Field::new(
                        "item",
                        DataType::Float32,
                        false
                    )),
                    2
                ),
                false
            )
        ]
    );
    assert_eq!(
        first.schema().metadata()["graphforge.algorithm"],
        "node2vec"
    );
    assert_eq!(
        first.schema().metadata()["graphforge.algorithm_version"],
        "node2vec-v1"
    );
    assert_eq!(first.schema().metadata()["graphforge.dimensions"], "2");
    assert_eq!(first.schema().metadata()["graphforge.seed"], "7");
    let uuids = first
        .column_by_name("node_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert!((1..uuids.len()).all(|row| uuids.value(row - 1) < uuids.value(row)));
    let embeddings = first
        .column_by_name("embedding")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .unwrap();
    assert_eq!(embeddings.value_length(), 2);
    assert_eq!(embeddings.null_count(), 0);
    assert!(
        embeddings
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .iter()
            .all(|value| value.is_some_and(f32::is_finite))
    );

    assert!(matches!(
        graph.analyze_embedding(
            Some(""),
            &EmbeddingAnalyzeOptions {
                by: AnalyzeAlgorithm::Node2Vec,
                via: None,
                directed: false,
                weight: None,
                options: EmbeddingOptions::Node2Vec(Node2VecOptions::default()),
            }
        ),
        Err(GfError::Validation(_))
    ));
    drop(graph);
    assert_eq!(
        first,
        GraphForge::new(Some(path))
            .unwrap()
            .analyze_embedding(Some("Person"), &options)
            .unwrap()
    );
}

#[test]
fn fastrp_executes_through_typed_api_with_features_and_canonical_arrow_output() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap();
    let graph = GraphForge::new(Some(path)).unwrap();
    graph
        .execute(
            "CREATE (:Person {name:'Alice', score:1.0})\
                 -[:KNOWS {strength:2.0}]->(:Person {name:'Bob', score:2.0}), \
                 (:Person {name:'Carol', score:3.0})",
        )
        .unwrap();
    let options = EmbeddingAnalyzeOptions {
        by: AnalyzeAlgorithm::FastRandomProjection,
        via: Some("KNOWS".to_owned()),
        directed: true,
        weight: Some("strength".to_owned()),
        options: EmbeddingOptions::FastRandomProjection(FastRpOptions {
            dimensions: 4,
            iteration_weights: vec![1.0, 1.0],
            feature_weight: 1.0,
            feature_properties: vec!["score".to_owned()],
            seed: 11,
            ..FastRpOptions::default()
        }),
    };
    let first = graph.analyze_embedding(Some("Person"), &options).unwrap();
    assert_eq!(
        first,
        graph.analyze_embedding(Some("Person"), &options).unwrap()
    );
    assert_eq!(first.num_rows(), 3);
    assert_eq!(
        first
            .schema()
            .fields()
            .iter()
            .map(|field| (
                field.name().as_str(),
                field.data_type(),
                field.is_nullable()
            ))
            .collect::<Vec<_>>(),
        [
            ("node_uuid", &DataType::FixedSizeBinary(16), false),
            (
                "embedding",
                &DataType::FixedSizeList(
                    Arc::new(arrow::datatypes::Field::new(
                        "item",
                        DataType::Float32,
                        false
                    )),
                    4
                ),
                false
            )
        ]
    );
    assert_eq!(
        first.schema().metadata()["graphforge.algorithm"],
        "fast_random_projection"
    );
    assert_eq!(
        first.schema().metadata()["graphforge.algorithm_version"],
        "fastrp-v1"
    );
    assert_eq!(first.schema().metadata()["graphforge.dimensions"], "4");
    assert_eq!(first.schema().metadata()["graphforge.seed"], "11");
    let uuids = first
        .column_by_name("node_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert!((1..uuids.len()).all(|row| uuids.value(row - 1) < uuids.value(row)));
    let embeddings = first
        .column_by_name("embedding")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .unwrap();
    assert_eq!(embeddings.value_length(), 4);
    assert_eq!(embeddings.null_count(), 0);
    assert!(
        embeddings
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .iter()
            .all(|value| value.is_some_and(f32::is_finite))
    );

    let mut invalid = options.clone();
    let EmbeddingOptions::FastRandomProjection(invalid_options) = &mut invalid.options else {
        unreachable!()
    };
    invalid_options.feature_properties = vec!["missing".to_owned()];
    assert!(matches!(
        graph.analyze_embedding(Some("Person"), &invalid),
        Err(GfError::Validation(message)) if message.contains("missing property")
    ));

    drop(graph);
    let reopened = GraphForge::new(Some(path)).unwrap();
    assert_eq!(
        first,
        reopened
            .analyze_embedding(Some("Person"), &options)
            .unwrap()
    );
}

#[test]
fn graphsage_executes_through_typed_api_with_scalar_and_list_features() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap();
    let graph = GraphForge::new(Some(path)).unwrap();
    graph
        .execute(
            "CREATE (:Person {name:'Alice', score:1.0, features:[1.0,0.0]})\
                 -[:KNOWS]->(:Person {name:'Bob', score:2.0, features:[0.0,1.0]}), \
                 (:Person {name:'Carol', score:3.0, features:[0.5,0.5]})",
        )
        .unwrap();
    let options = EmbeddingAnalyzeOptions {
        by: AnalyzeAlgorithm::GraphSage,
        via: Some("KNOWS".to_owned()),
        directed: false,
        weight: None,
        options: EmbeddingOptions::GraphSage(GraphSageOptions {
            dimensions: 2,
            hidden_dimensions: 2,
            layers: 1,
            sample_sizes: vec![1],
            epochs: 1,
            negative_samples: 1,
            learning_rate: 0.001,
            feature_properties: vec!["score".to_owned(), "features".to_owned()],
            seed: 13,
            ..GraphSageOptions::default()
        }),
    };
    let empty = GraphForge::new(None)
        .unwrap()
        .analyze_embedding(Some("Person"), &options)
        .unwrap();
    assert_eq!(empty.num_rows(), 0);
    assert_eq!(
        empty.schema().metadata()["graphforge.algorithm"],
        "graphsage"
    );
    assert_eq!(empty.schema().metadata()["graphforge.dimensions"], "2");

    let first = graph.analyze_embedding(Some("Person"), &options).unwrap();
    assert_eq!(
        first,
        graph.analyze_embedding(Some("Person"), &options).unwrap()
    );
    assert_eq!(first.num_rows(), 3);
    assert_eq!(
        first
            .schema()
            .fields()
            .iter()
            .map(|field| (
                field.name().as_str(),
                field.data_type(),
                field.is_nullable()
            ))
            .collect::<Vec<_>>(),
        [
            ("node_uuid", &DataType::FixedSizeBinary(16), false),
            (
                "embedding",
                &DataType::FixedSizeList(
                    Arc::new(arrow::datatypes::Field::new(
                        "item",
                        DataType::Float32,
                        false
                    )),
                    2
                ),
                false
            )
        ]
    );
    assert_eq!(
        first.schema().metadata()["graphforge.algorithm"],
        "graphsage"
    );
    assert_eq!(
        first.schema().metadata()["graphforge.algorithm_version"],
        "graphsage-unsupervised-v1"
    );
    assert_eq!(first.schema().metadata()["graphforge.dimensions"], "2");
    assert_eq!(first.schema().metadata()["graphforge.seed"], "13");
    let uuids = first
        .column_by_name("node_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert!((1..uuids.len()).all(|row| uuids.value(row - 1) < uuids.value(row)));
    let embeddings = first
        .column_by_name("embedding")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .unwrap();
    assert_eq!(embeddings.value_length(), 2);
    assert_eq!(embeddings.null_count(), 0);
    assert!(
        embeddings
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .iter()
            .all(|value| value.is_some_and(f32::is_finite))
    );

    let mut invalid = options.clone();
    let EmbeddingOptions::GraphSage(invalid_options) = &mut invalid.options else {
        unreachable!()
    };
    invalid_options.feature_properties = vec!["missing".to_owned()];
    assert!(matches!(
        graph.analyze_embedding(Some("Person"), &invalid),
        Err(GfError::Validation(message)) if message.contains("missing feature property")
    ));

    drop(graph);
    assert_eq!(
        first,
        GraphForge::new(Some(path))
            .unwrap()
            .analyze_embedding(Some("Person"), &options)
            .unwrap()
    );
}

#[test]
fn hashgnn_executes_through_typed_api_with_canonical_arrow_output() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap();
    let graph = GraphForge::new(Some(path)).unwrap();
    graph
        .execute(
            "CREATE (:Person {name:'Alice', kind:'human'})\
                 -[:KNOWS {kind:'friend'}]->(:Person {name:'Bob', kind:'human'}), \
                 (:Person {name:'Carol', kind:'human'})",
        )
        .unwrap();
    let options = EmbeddingAnalyzeOptions {
        by: AnalyzeAlgorithm::HashGnn,
        via: Some("KNOWS".to_owned()),
        directed: true,
        weight: None,
        options: EmbeddingOptions::HashGnn(HashGnnOptions {
            dimensions: 8,
            iterations: 2,
            embedding_density: 0.25,
            seed: 19,
            ..HashGnnOptions::default()
        }),
    };
    let empty = GraphForge::new(None)
        .unwrap()
        .analyze_embedding(Some("Person"), &options)
        .unwrap();
    assert_eq!(empty.num_rows(), 0);
    assert_eq!(empty.schema().metadata()["graphforge.algorithm"], "hashgnn");
    assert_eq!(empty.schema().metadata()["graphforge.dimensions"], "8");

    let first = graph.analyze_embedding(Some("Person"), &options).unwrap();
    assert_eq!(
        first,
        graph.analyze_embedding(Some("Person"), &options).unwrap()
    );
    assert_eq!(first.num_rows(), 3);
    assert_eq!(
        first
            .schema()
            .fields()
            .iter()
            .map(|field| (
                field.name().as_str(),
                field.data_type(),
                field.is_nullable()
            ))
            .collect::<Vec<_>>(),
        [
            ("node_uuid", &DataType::FixedSizeBinary(16), false),
            (
                "embedding",
                &DataType::FixedSizeList(
                    Arc::new(arrow::datatypes::Field::new(
                        "item",
                        DataType::Float32,
                        false
                    )),
                    8
                ),
                false
            )
        ]
    );
    assert_eq!(first.schema().metadata()["graphforge.algorithm"], "hashgnn");
    assert_eq!(
        first.schema().metadata()["graphforge.algorithm_version"],
        "hashgnn-v1"
    );
    assert_eq!(first.schema().metadata()["graphforge.dimensions"], "8");
    assert_eq!(first.schema().metadata()["graphforge.seed"], "19");
    let uuids = first
        .column_by_name("node_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert!((1..uuids.len()).all(|row| uuids.value(row - 1) < uuids.value(row)));
    let embeddings = first
        .column_by_name("embedding")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .unwrap();
    assert_eq!(embeddings.value_length(), 8);
    assert_eq!(embeddings.null_count(), 0);
    assert!(
        embeddings
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .iter()
            .all(|value| value.is_some_and(|value| value == 0.0 || value == 1.0))
    );

    let heterogeneous = EmbeddingAnalyzeOptions {
        options: EmbeddingOptions::HashGnn(HashGnnOptions {
            heterogeneous: true,
            node_type_property: Some("kind".to_owned()),
            relationship_type_property: Some("kind".to_owned()),
            ..match &options.options {
                EmbeddingOptions::HashGnn(options) => options.clone(),
                _ => unreachable!(),
            }
        }),
        ..options.clone()
    };
    let typed = graph
        .analyze_embedding(Some("Person"), &heterogeneous)
        .unwrap();
    assert_ne!(first, typed);
    assert_eq!(
        typed,
        graph
            .analyze_embedding(Some("Person"), &heterogeneous)
            .unwrap()
    );
    let mut missing = heterogeneous.clone();
    let EmbeddingOptions::HashGnn(missing_options) = &mut missing.options else {
        unreachable!()
    };
    missing_options.relationship_type_property = Some("missing".to_owned());
    assert!(matches!(
        graph.analyze_embedding(Some("Person"), &missing),
        Err(GfError::Validation(message))
            if message.contains("missing HashGNN type property")
    ));

    let integer_graph = GraphForge::new(None).unwrap();
    integer_graph
        .execute(
            "CREATE (:Person {kind:1})-[:KNOWS {kind:7}]->(:Person {kind:2}), \
                 (:Person {kind:3})",
        )
        .unwrap();
    let integer_result = integer_graph
        .analyze_embedding(Some("Person"), &heterogeneous)
        .unwrap();
    assert_eq!(integer_result.num_rows(), 3);
    assert_eq!(
        integer_result,
        integer_graph
            .analyze_embedding(Some("Person"), &heterogeneous)
            .unwrap()
    );

    drop(graph);
    let reopened = GraphForge::new(Some(path)).unwrap();
    assert_eq!(
        first,
        reopened
            .analyze_embedding(Some("Person"), &options)
            .unwrap()
    );
    assert_eq!(
        typed,
        reopened
            .analyze_embedding(Some("Person"), &heterogeneous)
            .unwrap()
    );
}
