use super::super::*;
use super::*;

fn node2vec_invocation(
    options: graphforge_core::embedding_options::Node2VecOptions,
) -> EmbeddingAnalyzeOptions {
    EmbeddingAnalyzeOptions {
        by: AnalyzeAlgorithm::Node2Vec,
        via: None,
        directed: true,
        weight: None,
        options: EmbeddingOptions::Node2Vec(options),
    }
}

fn execute_node2vec_with_controls(
    graph: &AdjacencyGraph,
    options: graphforge_core::embedding_options::Node2VecOptions,
    limits: AlgorithmLimits,
    cancellation: AlgorithmCancellation,
    resource_limits: EmbeddingResourceLimits,
) -> Result<RecordBatch, GfError> {
    let invocation = normalize_embedding_options(&node2vec_invocation(options))?;
    let control = AlgorithmControl::new(limits, cancellation);
    embedding_algorithm_with_controls(graph, &invocation, &control, resource_limits)
}

fn fastrp_invocation(
    options: graphforge_core::embedding_options::FastRpOptions,
) -> EmbeddingAnalyzeOptions {
    EmbeddingAnalyzeOptions {
        by: AnalyzeAlgorithm::FastRandomProjection,
        via: None,
        directed: true,
        weight: None,
        options: EmbeddingOptions::FastRandomProjection(options),
    }
}

fn graphsage_invocation(
    options: graphforge_core::embedding_options::GraphSageOptions,
) -> EmbeddingAnalyzeOptions {
    EmbeddingAnalyzeOptions {
        by: AnalyzeAlgorithm::GraphSage,
        via: None,
        directed: false,
        weight: None,
        options: EmbeddingOptions::GraphSage(options),
    }
}

fn hashgnn_invocation(
    options: graphforge_core::embedding_options::HashGnnOptions,
) -> EmbeddingAnalyzeOptions {
    EmbeddingAnalyzeOptions {
        by: AnalyzeAlgorithm::HashGnn,
        via: None,
        directed: true,
        weight: None,
        options: EmbeddingOptions::HashGnn(options),
    }
}

#[test]
fn node2vec_descriptor_is_deterministic_and_persistence_independent() {
    let graph = AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2)]);
    let options = graphforge_core::embedding_options::Node2VecOptions {
        dimensions: 4,
        walk_length: 3,
        walks_per_node: 2,
        window_size: 1,
        negative_samples: 1,
        epochs: 1,
        seed: 7,
        ..graphforge_core::embedding_options::Node2VecOptions::default()
    };
    let invocation = normalize_embedding_options(&node2vec_invocation(options.clone())).unwrap();
    let selector = EmbeddingProjectionSelector {
        label: Some("Person".into()),
        via: Some("KNOWS".into()),
        directed: true,
        weight: None,
    };
    let execute = || {
        embedding_algorithm_execution_with_controls(
            &graph,
            &invocation,
            selector.clone(),
            &AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default()),
            EmbeddingResourceLimits::default(),
            None,
        )
        .unwrap()
    };

    let first = execute();
    assert_eq!(
        first.descriptor.options,
        EmbeddingOptions::Node2Vec(options)
    );
    assert_eq!(first.descriptor.selector, selector);
    assert_eq!(first.descriptor.rng.seed, 7);
    assert_eq!(
        first.descriptor.projection_fingerprint,
        embedding_descriptor_projection_fingerprint(&graph, None).unwrap()
    );

    let directory = tempfile::tempdir().unwrap();
    let persisted = directory.path().join("invocation.bin");
    std::fs::write(&persisted, first.descriptor.canonical_bytes()).unwrap();
    let stored = std::fs::read(&persisted).unwrap();

    let second = execute();
    assert_eq!(stored, second.descriptor.canonical_bytes());
    assert_eq!(first.descriptor, second.descriptor);
    assert_eq!(first.result, second.result);
}

#[test]
fn public_embedding_facade_executes_an_empty_persisted_projection() {
    let project = tempfile::tempdir().unwrap();
    let provider = crate::adjacency::ScanBuildAdjacencyProvider::new(
        project.path().to_path_buf(),
        OntologyMode::Exploratory,
    );
    let invocation =
        node2vec_invocation(graphforge_core::embedding_options::Node2VecOptions::default());

    let result = embedding_algorithm(
        &provider,
        project.path(),
        OntologyMode::Exploratory,
        EntityTypeSelection::All,
        &invocation,
    )
    .unwrap();

    assert_eq!(result.num_rows(), 0);
    assert_eq!(result.num_columns(), 2);
}

#[test]
fn node2vec_dispatch_resource_failures_are_structured_and_atomic() {
    let graph = AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2)]);
    let options = graphforge_core::embedding_options::Node2VecOptions {
        dimensions: 4,
        walk_length: 3,
        walks_per_node: 2,
        window_size: 1,
        negative_samples: 1,
        epochs: 1,
        seed: 7,
        ..graphforge_core::embedding_options::Node2VecOptions::default()
    };
    let output = execute_node2vec_with_controls(
        &graph,
        options.clone(),
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
        EmbeddingResourceLimits::default(),
    )
    .expect("control invocation");
    assert_eq!(output.num_rows(), 3);

    let cancelled = AlgorithmCancellation::default();
    cancelled.cancel();
    for (name, result, expected) in [
        (
            "cancellation",
            execute_node2vec_with_controls(
                &graph,
                options.clone(),
                AlgorithmLimits::default(),
                cancelled,
                EmbeddingResourceLimits::default(),
            ),
            "algorithm execution cancelled",
        ),
        (
            "node limit",
            execute_node2vec_with_controls(
                &graph,
                options.clone(),
                AlgorithmLimits {
                    nodes: 2,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
                EmbeddingResourceLimits::default(),
            ),
            "algorithm node limit exceeded",
        ),
        (
            "output limit",
            execute_node2vec_with_controls(
                &graph,
                options.clone(),
                AlgorithmLimits {
                    output_rows: 2,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
                EmbeddingResourceLimits::default(),
            ),
            "algorithm output row limit exceeded",
        ),
        (
            "memory limit",
            execute_node2vec_with_controls(
                &graph,
                options.clone(),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
                EmbeddingResourceLimits {
                    memory_bytes: 0,
                    work: u64::MAX,
                },
            ),
            "embedding memory limit exceeded",
        ),
        (
            "work limit",
            execute_node2vec_with_controls(
                &graph,
                options.clone(),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
                EmbeddingResourceLimits {
                    memory_bytes: u64::MAX,
                    work: 0,
                },
            ),
            "embedding work limit exceeded",
        ),
        (
            "resource overflow",
            execute_node2vec_with_controls(
                &graph,
                graphforge_core::embedding_options::Node2VecOptions {
                    walks_per_node: usize::MAX,
                    ..options
                },
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
                EmbeddingResourceLimits {
                    memory_bytes: u64::MAX,
                    work: u64::MAX,
                },
            ),
            "embedding resource accounting exceeds UInt64 range",
        ),
    ] {
        let error = result.unwrap_err();
        assert!(
            error.to_string().contains(expected),
            "{name} returned unexpected error: {error}"
        );
    }
}

#[test]
fn fastrp_descriptor_replays_and_dispatch_controls_fail_atomically() {
    let graph = AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2)]);
    let options = graphforge_core::embedding_options::FastRpOptions {
        dimensions: 4,
        iteration_weights: vec![1.0, 1.0],
        seed: 11,
        ..graphforge_core::embedding_options::FastRpOptions::default()
    };
    let invocation = normalize_embedding_options(&fastrp_invocation(options.clone())).unwrap();
    let selector = EmbeddingProjectionSelector {
        label: Some("Person".into()),
        via: Some("KNOWS".into()),
        directed: true,
        weight: None,
    };
    let execute = |cancellation, resource_limits| {
        embedding_algorithm_execution_with_controls(
            &graph,
            &invocation,
            selector.clone(),
            &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            resource_limits,
            None,
        )
    };
    let first = execute(
        AlgorithmCancellation::default(),
        EmbeddingResourceLimits::default(),
    )
    .unwrap();
    let second = execute(
        AlgorithmCancellation::default(),
        EmbeddingResourceLimits::default(),
    )
    .unwrap();
    assert_eq!(
        first.descriptor.options,
        EmbeddingOptions::FastRandomProjection(options)
    );
    assert_eq!(first.descriptor.selector, selector);
    assert_eq!(
        first.descriptor.canonical_bytes(),
        second.descriptor.canonical_bytes()
    );
    assert_eq!(first.result, second.result);

    let cancelled = AlgorithmCancellation::default();
    cancelled.cancel();
    assert!(matches!(
        execute(cancelled, EmbeddingResourceLimits::default()),
        Err(GfError::Execution(message)) if message.contains("cancel")
    ));
    assert!(matches!(
        execute(
            AlgorithmCancellation::default(),
            EmbeddingResourceLimits {
                memory_bytes: 0,
                work: u64::MAX,
            },
        ),
        Err(GfError::Execution(message)) if message.contains("memory")
    ));
    assert!(matches!(
        execute(
            AlgorithmCancellation::default(),
            EmbeddingResourceLimits {
                memory_bytes: u64::MAX,
                work: 0,
            },
        ),
        Err(GfError::Execution(message)) if message.contains("work")
    ));
}

#[test]
fn graphsage_descriptor_replays_and_dispatch_controls_fail_atomically() {
    let mut graph = AdjacencyGraph::with_test_undirected_multigraph(3, &[(10, 0, 1), (11, 1, 2)]);
    graph
        .replace_node_vectors(HashMap::from([
            (0, vec![1.0, 0.0]),
            (1, vec![0.0, 1.0]),
            (2, vec![0.5, 0.5]),
        ]))
        .unwrap();
    let options = graphforge_core::embedding_options::GraphSageOptions {
        dimensions: 2,
        hidden_dimensions: 2,
        layers: 1,
        sample_sizes: vec![1],
        epochs: 1,
        negative_samples: 1,
        learning_rate: 0.001,
        feature_properties: vec!["features".into()],
        seed: 13,
        ..graphforge_core::embedding_options::GraphSageOptions::default()
    };
    let invocation = normalize_embedding_options(&graphsage_invocation(options.clone())).unwrap();
    let selector = EmbeddingProjectionSelector {
        label: Some("Person".into()),
        via: Some("KNOWS".into()),
        directed: false,
        weight: None,
    };
    let execute = |limits, cancellation, resource_limits| {
        embedding_algorithm_execution_with_controls(
            &graph,
            &invocation,
            selector.clone(),
            &AlgorithmControl::new(limits, cancellation),
            resource_limits,
            None,
        )
    };
    let first = execute(
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
        EmbeddingResourceLimits::default(),
    )
    .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let persisted_path = directory.path().join("graphsage-invocation.bin");
    std::fs::write(&persisted_path, first.descriptor.canonical_bytes()).unwrap();
    let persisted = std::fs::read(&persisted_path).unwrap();
    let second = execute(
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
        EmbeddingResourceLimits::default(),
    )
    .unwrap();
    assert_eq!(
        first.descriptor.options,
        EmbeddingOptions::GraphSage(options)
    );
    assert_eq!(first.descriptor.selector, selector);
    assert_eq!(first.descriptor.rng.seed, 13);
    assert_eq!(persisted, second.descriptor.canonical_bytes());
    assert_eq!(first.result, second.result);

    let cancelled = AlgorithmCancellation::default();
    cancelled.cancel();
    assert!(matches!(
        execute(
            AlgorithmLimits::default(),
            cancelled,
            EmbeddingResourceLimits::default()
        ),
        Err(GfError::Execution(message)) if message.contains("cancel")
    ));
    for (limits, expected) in [
        (
            AlgorithmLimits {
                nodes: 2,
                ..AlgorithmLimits::default()
            },
            AlgorithmError::NodeLimit {
                observed: 3,
                limit: 2,
            },
        ),
        (
            AlgorithmLimits {
                output_rows: 2,
                ..AlgorithmLimits::default()
            },
            AlgorithmError::OutputLimit {
                observed: 3,
                limit: 2,
            },
        ),
    ] {
        let error = execute(
            limits,
            AlgorithmCancellation::default(),
            EmbeddingResourceLimits::default(),
        )
        .unwrap_err();
        assert_eq!(error.code(), "GF_EXECUTION");
        assert_eq!(error.to_string(), format!("execution error: {expected}"));
        assert!(matches!(error, GfError::Algorithm(actual) if actual == expected));
    }
    let iteration_error = execute(
        AlgorithmLimits {
            iterations: 0,
            ..AlgorithmLimits::default()
        },
        AlgorithmCancellation::default(),
        EmbeddingResourceLimits::default(),
    )
    .unwrap_err();
    assert_eq!(iteration_error.code(), "GF_EXECUTION");
    assert!(matches!(
        iteration_error,
        GfError::Execution(message) if message.contains("iteration limit")
    ));
    let memory_error = execute(
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
        EmbeddingResourceLimits {
            memory_bytes: 0,
            work: u64::MAX,
        },
    )
    .unwrap_err()
    .to_string();
    let observed = memory_error
        .split_once("observed ")
        .and_then(|(_, suffix)| suffix.split_once(','))
        .and_then(|(value, _)| value.parse::<u64>().ok())
        .expect("structured GraphSAGE memory error reports observed bytes");
    assert!(
        execute(
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
            EmbeddingResourceLimits {
                memory_bytes: observed,
                work: u64::MAX,
            },
        )
        .is_ok()
    );
    assert!(matches!(
        execute(
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
            EmbeddingResourceLimits {
                memory_bytes: observed - 1,
                work: u64::MAX,
            },
        ),
        Err(GfError::Execution(message)) if message.contains("memory")
    ));
    assert!(matches!(
        execute(
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
            EmbeddingResourceLimits {
                memory_bytes: u64::MAX,
                work: 0,
            },
        ),
        Err(GfError::Execution(message)) if message.contains("work")
    ));
}

#[test]
fn hashgnn_descriptor_replays_and_dispatch_controls_fail_atomically() {
    let graph = AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2)]);
    let options = graphforge_core::embedding_options::HashGnnOptions {
        dimensions: 8,
        iterations: 2,
        embedding_density: 0.25,
        heterogeneous: true,
        node_type_property: Some("kind".into()),
        relationship_type_property: Some("kind".into()),
        seed: 19,
        ..graphforge_core::embedding_options::HashGnnOptions::default()
    };
    let type_tokens = HashGnnTypeTokens {
        nodes: BTreeMap::from([
            (0_u128.to_be_bytes(), "string:5:human".into()),
            (1_u128.to_be_bytes(), "string:5:human".into()),
            (2_u128.to_be_bytes(), "string:5:human".into()),
        ]),
        relationships: BTreeMap::from([
            (0_u128.to_be_bytes(), "string:6:friend".into()),
            (1_u128.to_be_bytes(), "string:6:friend".into()),
        ]),
    };
    let invocation = normalize_embedding_options(&hashgnn_invocation(options.clone())).unwrap();
    let selector = EmbeddingProjectionSelector {
        label: Some("Person".into()),
        via: Some("KNOWS".into()),
        directed: true,
        weight: None,
    };
    let execute = |limits, cancellation, resource_limits| {
        embedding_algorithm_execution_with_controls(
            &graph,
            &invocation,
            selector.clone(),
            &AlgorithmControl::new(limits, cancellation),
            resource_limits,
            Some(&type_tokens),
        )
    };
    let first = execute(
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
        EmbeddingResourceLimits::default(),
    )
    .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let persisted_path = directory.path().join("hashgnn-invocation.bin");
    std::fs::write(&persisted_path, first.descriptor.canonical_bytes()).unwrap();
    let persisted = std::fs::read(&persisted_path).unwrap();
    let second = execute(
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
        EmbeddingResourceLimits::default(),
    )
    .unwrap();
    assert_eq!(first.descriptor.options, EmbeddingOptions::HashGnn(options));
    assert_eq!(first.descriptor.selector, selector);
    assert_eq!(first.descriptor.rng.seed, 19);
    assert_eq!(persisted, second.descriptor.canonical_bytes());
    assert_eq!(first.result, second.result);

    let cancelled = AlgorithmCancellation::default();
    cancelled.cancel();
    assert!(matches!(
        execute(
            AlgorithmLimits::default(),
            cancelled,
            EmbeddingResourceLimits::default()
        ),
        Err(GfError::Execution(message)) if message.contains("cancel")
    ));
    assert!(matches!(
        execute(
            AlgorithmLimits {
                iterations: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
            EmbeddingResourceLimits::default(),
        ),
        Err(GfError::Execution(message)) if message.contains("iteration limit")
    ));
    let memory_error = execute(
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
        EmbeddingResourceLimits {
            memory_bytes: 0,
            work: u64::MAX,
        },
    )
    .unwrap_err()
    .to_string();
    let observed = memory_error
        .split_once("observed ")
        .and_then(|(_, suffix)| suffix.split_once(','))
        .and_then(|(value, _)| value.parse::<u64>().ok())
        .expect("structured HashGNN memory error reports observed bytes");
    assert!(
        execute(
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
            EmbeddingResourceLimits {
                memory_bytes: observed,
                work: u64::MAX,
            },
        )
        .is_ok()
    );
    assert!(matches!(
        execute(
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
            EmbeddingResourceLimits {
                memory_bytes: observed - 1,
                work: u64::MAX,
            },
        ),
        Err(GfError::Execution(message)) if message.contains("memory")
    ));
    assert!(matches!(
        execute(
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
            EmbeddingResourceLimits {
                memory_bytes: u64::MAX,
                work: 0,
            },
        ),
        Err(GfError::Execution(message)) if message.contains("work")
    ));
}

#[test]
fn hashgnn_type_tokens_are_canonical_and_reject_invalid_or_conflicting_values() {
    let uuid = 7_u128.to_be_bytes();
    assert_eq!(
        hashgnn_node_type_token("kind", &uuid, &IrLiteral::Str("hé".into())).unwrap(),
        (HashGnnTypeKind::String, "string:3:hé".into())
    );
    assert_eq!(
        hashgnn_node_type_token("kind", &uuid, &IrLiteral::Int(-7)).unwrap(),
        (HashGnnTypeKind::Integer, "integer:-7".into())
    );
    assert!(matches!(
        hashgnn_node_type_token("kind", &uuid, &IrLiteral::Null),
        Err(GfError::Validation(message)) if message.contains("non-null scalar")
    ));

    let strings = StringArray::from(vec!["friend"]);
    assert_eq!(
        hashgnn_edge_type_token("kind", &uuid, &strings, 0).unwrap(),
        (HashGnnTypeKind::String, "string:6:friend".into())
    );
    let integers = Int64Array::from(vec![9]);
    assert_eq!(
        hashgnn_edge_type_token("kind", &uuid, &integers, 0).unwrap(),
        (HashGnnTypeKind::Integer, "integer:9".into())
    );
    let unsupported = arrow::array::BooleanArray::from(vec![true]);
    assert!(matches!(
        hashgnn_edge_type_token("kind", &uuid, &unsupported, 0),
        Err(GfError::Validation(message)) if message.contains("non-null scalar")
    ));

    let mut kind = None;
    validate_hashgnn_type_kind("node", "kind", &mut kind, HashGnnTypeKind::String).unwrap();
    assert!(matches!(
        validate_hashgnn_type_kind(
            "node",
            "kind",
            &mut kind,
            HashGnnTypeKind::Integer
        ),
        Err(GfError::Validation(message)) if message.contains("mixes string and integer")
    ));

    let mut values = BTreeMap::new();
    insert_hashgnn_type_value(&mut values, uuid, "string:5:human".into(), "node", "kind").unwrap();
    assert!(matches!(
        insert_hashgnn_type_value(
            &mut values,
            uuid,
            "string:6:person".into(),
            "node",
            "kind"
        ),
        Err(GfError::Validation(message)) if message.contains("conflicting")
    ));
}

#[test]
fn graphsage_source_resource_contract_validates_feature_matrix() {
    let empty = AdjacencyGraph::with_test_counts(0, 0);
    assert!(graphsage_source_resources(&empty).is_err());

    let mut graph = AdjacencyGraph::with_test_directed_edges(2, &[(0, 1)]);
    assert_eq!(
        graphsage_source_resources(&graph).unwrap_err().to_string(),
        "validation error: graphsage selected node has no resolved feature vector"
    );
    graph
        .replace_node_vectors(HashMap::from([(0, vec![]), (1, vec![])]))
        .unwrap();
    assert!(
        graphsage_source_resources(&graph)
            .unwrap_err()
            .to_string()
            .contains("non-empty")
    );
    graph
        .replace_node_vectors(HashMap::from([(0, vec![1.0, 2.0]), (1, vec![3.0])]))
        .unwrap();
    assert!(
        graphsage_source_resources(&graph)
            .unwrap_err()
            .to_string()
            .contains("inconsistent shape")
    );
    graph
        .replace_node_vectors(HashMap::from([
            (0, vec![1.0, 2.0]),
            (1, vec![f64::NAN, 4.0]),
        ]))
        .unwrap();
    assert!(
        graphsage_source_resources(&graph)
            .unwrap_err()
            .to_string()
            .contains("must be finite")
    );
    graph
        .replace_node_vectors(HashMap::from([(0, vec![1.0, 2.0]), (1, vec![3.0, 4.0])]))
        .unwrap();
    let (width, retained) = graphsage_source_resources(&graph).unwrap();
    assert_eq!(width, 2);
    assert!(retained >= 96);
    let projection = graphsage_projection(&graph).expect("valid GraphSAGE projection");
    assert_eq!(projection.nodes().len(), 2);
    assert_eq!(projection.feature_width(), 2);
}
