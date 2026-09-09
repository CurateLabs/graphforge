use super::*;

#[test]
fn facade_label_and_clear_boundaries_are_exact_and_non_mutating() {
    let graph = GraphForge::new(None).unwrap();
    for invalid in ["", " Person", "Person ", "Per\nson", "\0Person"] {
        assert!(matches!(
            graph.algorithm_label(invalid, "rank"),
            Err(GfError::Validation(_))
        ));
    }
    let (unknown, stem) = graph.algorithm_label("Unknown", "rank").unwrap();
    assert_eq!(unknown, graphforge_value::EntityTypeSelection::Missing);
    assert_eq!(stem, "_untyped");

    graph
        .register_procedure(ProcedureDefinition {
            name: "test.clear".into(),
            inputs: vec![],
            outputs: vec![ProcedureField {
                name: "value".into(),
                type_name: "INTEGER".into(),
                nullable: false,
            }],
            rows: vec![vec![IrLiteral::Int(1)]],
        })
        .unwrap();
    graph.add_node("Person", &HashMap::new()).unwrap();
    assert_eq!(graph.node_count("Person").unwrap(), 1);
    graph.clear().unwrap();
    assert_eq!(graph.node_count("Person").unwrap(), 0);
    assert!(graph.labels().unwrap().is_empty());
    assert!(graph.execute("CALL test.clear()").is_err());

    let directory = tempfile::tempdir().unwrap();
    let persistent = GraphForge::new(directory.path().to_str()).unwrap();
    persistent.add_node("Person", &HashMap::new()).unwrap();
    let error = persistent.clear().unwrap_err();
    assert!(matches!(error, GfError::Storage(_)));
    assert_eq!(persistent.node_count("Person").unwrap(), 1);
    drop(persistent);
    let reopened = GraphForge::new(directory.path().to_str()).unwrap();
    assert_eq!(reopened.node_count("Person").unwrap(), 1);
}

#[test]
fn read_only_verb_options_have_no_write_property_surface() {
    let PathsOptions {
        by: _,
        via: _,
        directed: _,
        k: _,
        weight: _,
        capacity_property: _,
        cost_property: _,
        heuristic: _,
        walk_length: _,
        seed: _,
        terminal_uuids: _,
        prize_property: _,
    } = PathsOptions::default();
    let AnalyzeOptions {
        by: _,
        via: _,
        directed: _,
        weight: _,
        k: _,
        partition_property: _,
    } = AnalyzeOptions::default();
    let SimilarOptions {
        by: _,
        k: _,
        vector_property: _,
        via: _,
    } = SimilarOptions::default();
}

#[test]
fn prepared_embedding_descriptors_round_trip_all_non_node2vec_variants() {
    let graph = GraphForge::new(None).unwrap();
    graph
        .execute(
            "CREATE (:Person {score:1.0, features:[1.0,0.0], kind:'human'})\
                 -[:KNOWS {kind:'friend'}]->\
                 (:Person {score:2.0, features:[0.0,1.0], kind:'human'})",
        )
        .unwrap();
    let cases = [
        EmbeddingAnalyzeOptions {
            by: AnalyzeAlgorithm::GraphSage,
            via: Some("KNOWS".into()),
            directed: false,
            weight: None,
            options: EmbeddingOptions::GraphSage(GraphSageOptions {
                dimensions: 2,
                hidden_dimensions: 3,
                layers: 1,
                sample_sizes: vec![2],
                epochs: 1,
                negative_samples: 1,
                learning_rate: 0.001,
                feature_properties: vec!["score".into(), "features".into()],
                seed: 41,
                ..GraphSageOptions::default()
            }),
        },
        EmbeddingAnalyzeOptions {
            by: AnalyzeAlgorithm::FastRandomProjection,
            via: Some("KNOWS".into()),
            directed: false,
            weight: None,
            options: EmbeddingOptions::FastRandomProjection(FastRpOptions {
                dimensions: 3,
                iteration_weights: vec![0.5, 1.0],
                normalization_strength: -0.25,
                feature_weight: 0.75,
                feature_properties: vec!["score".into()],
                seed: 42,
            }),
        },
        EmbeddingAnalyzeOptions {
            by: AnalyzeAlgorithm::HashGnn,
            via: Some("KNOWS".into()),
            directed: true,
            weight: None,
            options: EmbeddingOptions::HashGnn(HashGnnOptions {
                dimensions: 8,
                iterations: 2,
                embedding_density: 0.25,
                heterogeneous: true,
                node_type_property: Some("kind".into()),
                relationship_type_property: Some("kind".into()),
                seed: 43,
            }),
        },
    ];

    for options in cases {
        let direct = graph.analyze_embedding(Some("Person"), &options).unwrap();
        let descriptor = graph
            .prepare_embedding_invocation(Some("Person"), &options)
            .unwrap();
        let decoded =
            InvocationDescriptor::from_canonical_bytes(descriptor.canonical_bytes()).unwrap();
        assert_eq!(decoded, descriptor);
        assert_eq!(descriptor.algorithm(), Algorithm::Analyze(options.by));
        assert_eq!(
            graph.invoke_embedding_descriptor(&descriptor).unwrap(),
            direct
        );
        assert_eq!(graph.invoke_descriptor(&descriptor).unwrap(), direct);
        assert_eq!(
            direct.schema().metadata()["graphforge.algorithm"],
            options.by.as_str()
        );
    }

    let rank_descriptor = graph
        .prepare_rank_invocation("Person", &degree_options(false, None))
        .unwrap();
    let error = graph
        .invoke_embedding_descriptor(&rank_descriptor)
        .unwrap_err();
    assert_eq!(error.code(), "GF_DESCRIPTOR_INVALID");
    assert!(error.to_string().contains("requires an analyze descriptor"));
}

#[test]
fn descriptor_dispatch_rejects_every_cross_verb_and_invalid_graphsage_aggregator() {
    let graph = GraphForge::new(None).unwrap();
    graph.execute("CREATE (:Person {score: 1.0})").unwrap();
    let rank = graph
        .prepare_rank_invocation("Person", &degree_options(false, None))
        .unwrap();
    for error in [
        graph.invoke_cluster_descriptor(&rank).unwrap_err(),
        graph.invoke_similar_descriptor(&rank).unwrap_err(),
        graph.invoke_embedding_descriptor(&rank).unwrap_err(),
        graph.invoke_analyze_descriptor(&rank).unwrap_err(),
        graph.invoke_paths_descriptor(&rank).unwrap_err(),
    ] {
        assert_eq!(error.code(), "GF_DESCRIPTOR_INVALID");
    }

    let options = EmbeddingAnalyzeOptions {
        by: AnalyzeAlgorithm::GraphSage,
        via: None,
        directed: false,
        weight: None,
        options: EmbeddingOptions::GraphSage(GraphSageOptions {
            dimensions: 2,
            hidden_dimensions: 2,
            layers: 1,
            sample_sizes: vec![1],
            epochs: 1,
            negative_samples: 1,
            learning_rate: 0.01,
            feature_properties: vec!["score".into()],
            seed: 7,
            ..GraphSageOptions::default()
        }),
    };
    let descriptor = graph
        .prepare_embedding_invocation(Some("Person"), &options)
        .unwrap();
    let mut parameters = descriptor.parameters().clone();
    parameters.insert(
        "aggregator".into(),
        InvocationParameter::Utf8("unsupported".into()),
    );
    let malformed = InvocationDescriptor::new(
        descriptor.algorithm(),
        *descriptor.projection_fingerprint(),
        parameters,
    )
    .unwrap();
    let error = graph.invoke_embedding_descriptor(&malformed).unwrap_err();
    assert_eq!(error.code(), "GF_DESCRIPTOR_INVALID");
    assert!(
        error
            .to_string()
            .contains("unsupported GraphSAGE aggregator")
    );
}

#[test]
fn neutral_descriptors_reject_writeback_and_detect_projection_changes() {
    let graph = GraphForge::new(None).unwrap();
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
    graph
        .add_edge(&alice, "KNOWS", &bob, &HashMap::new())
        .unwrap();

    let mut rank_options = degree_options(false, Some("KNOWS"));
    rank_options.write_property = Some("rank".into());
    let error = graph
        .prepare_rank_invocation("Person", &rank_options)
        .unwrap_err();
    assert_eq!(error.code(), "GF_DESCRIPTOR_INVALID");
    assert_eq!(
        error.to_string(),
        "invalid invocation descriptor: rank write_property is not part of a neutral invocation"
    );

    let mut cluster_options = components_options(false, Some("KNOWS"));
    cluster_options.write_property = Some("community".into());
    let error = graph
        .prepare_cluster_invocation("Person", &cluster_options)
        .unwrap_err();
    assert_eq!(error.code(), "GF_DESCRIPTOR_INVALID");
    assert_eq!(
        error.to_string(),
        "invalid invocation descriptor: cluster write_property is not part of a neutral invocation"
    );

    let rank = graph
        .prepare_rank_invocation("Person", &degree_options(false, Some("KNOWS")))
        .unwrap();
    let cluster = graph
        .prepare_cluster_invocation("Person", &components_options(false, Some("KNOWS")))
        .unwrap();
    let similar = graph
        .prepare_similar_invocation("Person", &node_similarity_options(2, Some("KNOWS")))
        .unwrap();
    let analyze = graph
        .prepare_analyze_invocation(None, &is_dag_options(true, Some("KNOWS")))
        .unwrap();
    let source = NodeSelector::Handle(alice);
    let target = NodeSelector::Handle(bob);
    let paths = graph
        .prepare_paths_invocation(
            Some(&source),
            Some(&target),
            &bfs_options(true, Some("KNOWS")),
        )
        .unwrap();

    let wrong_rank = graph.invoke_rank_descriptor(&cluster).unwrap_err();
    assert_eq!(wrong_rank.code(), "GF_DESCRIPTOR_INVALID");
    assert!(
        wrong_rank
            .to_string()
            .contains("rank dispatch requires a rank descriptor")
    );
    let wrong_embedding = graph.invoke_embedding_descriptor(&analyze).unwrap_err();
    assert_eq!(wrong_embedding.code(), "GF_DESCRIPTOR_INVALID");
    assert!(
        wrong_embedding
            .to_string()
            .contains("descriptor is not an embedding algorithm")
    );

    graph.add_node("Person", &HashMap::new()).unwrap();
    for error in [
        graph.invoke_rank_descriptor(&rank).unwrap_err(),
        graph.invoke_cluster_descriptor(&cluster).unwrap_err(),
        graph.invoke_similar_descriptor(&similar).unwrap_err(),
        graph.invoke_analyze_descriptor(&analyze).unwrap_err(),
        graph.invoke_paths_descriptor(&paths).unwrap_err(),
    ] {
        assert_eq!(error.code(), "GF_PROJECTION_CHANGED");
        assert_eq!(
            error.to_string(),
            "the graph projection changed after descriptor preparation"
        );
    }
}

#[test]
fn descriptor_preparation_materializes_optional_analysis_and_path_parameters() {
    let graph = GraphForge::new(None).unwrap();
    let analyze = AnalyzeOptions {
        by: AnalyzeAlgorithm::IsDag,
        via: Some("KNOWS".into()),
        directed: true,
        weight: Some("weight".into()),
        k: Some(3),
        partition_property: Some("partition".into()),
    };
    assert_eq!(
        graph
            .prepare_analyze_invocation(Some("Person"), &analyze)
            .unwrap_err()
            .code(),
        "GF_VALIDATION"
    );

    let source = graph
        .add_node(
            "Person",
            &HashMap::from([
                ("weight".into(), PropValue::Float(1.0)),
                ("partition".into(), PropValue::Int(0)),
                ("heuristic".into(), PropValue::Float(1.0)),
            ]),
        )
        .unwrap();
    let analyze_descriptor = graph
        .prepare_analyze_invocation(
            Some("Person"),
            &AnalyzeOptions {
                by: AnalyzeAlgorithm::MinimumKSpanningTree,
                via: Some("KNOWS".into()),
                directed: false,
                weight: Some("weight".into()),
                k: Some(3),
                partition_property: None,
            },
        )
        .unwrap();
    assert_eq!(
        analyze_descriptor.parameters()["weight"],
        InvocationParameter::Utf8("weight".into())
    );
    assert_eq!(
        analyze_descriptor.parameters()["k"],
        InvocationParameter::U64(3)
    );
    let modularity_descriptor = graph
        .prepare_analyze_invocation(
            Some("Person"),
            &AnalyzeOptions {
                by: AnalyzeAlgorithm::Modularity,
                via: Some("KNOWS".into()),
                directed: false,
                weight: Some("weight".into()),
                k: None,
                partition_property: Some("partition".into()),
            },
        )
        .unwrap();
    assert_eq!(
        modularity_descriptor.parameters()["partition_property"],
        InvocationParameter::Utf8("partition".into())
    );
    let target = graph
        .add_node(
            "Person",
            &HashMap::from([("heuristic".into(), PropValue::Float(0.0))]),
        )
        .unwrap();
    graph
        .add_edge(
            &source,
            "KNOWS",
            &target,
            &HashMap::from([
                ("weight".into(), PropValue::Float(1.0)),
                ("capacity".into(), PropValue::Float(2.0)),
                ("cost".into(), PropValue::Float(3.0)),
            ]),
        )
        .unwrap();
    let weighted_paths = PathsOptions {
        by: PathAlgorithm::AStar,
        directed: true,
        k: 1,
        via: Some("KNOWS".into()),
        weight: Some("weight".into()),
        capacity_property: None,
        cost_property: None,
        heuristic: Some("heuristic".into()),
        walk_length: None,
        seed: None,
        terminal_uuids: Vec::new(),
        prize_property: None,
    };
    let weighted_descriptor = graph
        .prepare_paths_invocation(
            Some(&NodeSelector::Handle(source.clone())),
            Some(&NodeSelector::Handle(target)),
            &weighted_paths,
        )
        .unwrap();
    assert_eq!(
        weighted_descriptor.parameters()["weight"],
        InvocationParameter::Utf8("weight".into())
    );
    assert_eq!(
        weighted_descriptor.parameters()["heuristic"],
        InvocationParameter::Utf8("heuristic".into())
    );
    let source_uuid = source.uuid;
    let source = NodeSelector::Handle(source);

    let random_walk = PathsOptions {
        by: PathAlgorithm::RandomWalk,
        directed: true,
        k: 2,
        via: Some("KNOWS".into()),
        weight: None,
        capacity_property: None,
        cost_property: None,
        heuristic: None,
        walk_length: None,
        seed: None,
        terminal_uuids: Vec::new(),
        prize_property: None,
    };
    let descriptor = graph
        .prepare_paths_invocation(Some(&source), None, &random_walk)
        .unwrap();
    assert_eq!(
        descriptor.parameters()["walk_length"],
        InvocationParameter::U64(10)
    );
    assert_eq!(descriptor.parameters()["seed"], InvocationParameter::U64(0));
    assert_eq!(
        descriptor.parameters()["via"],
        InvocationParameter::Utf8("KNOWS".into())
    );

    let mut explicit = random_walk;
    explicit.walk_length = Some(7);
    explicit.seed = Some(9);
    let descriptor = graph
        .prepare_paths_invocation(Some(&source), None, &explicit)
        .unwrap();
    assert_eq!(
        descriptor.parameters()["walk_length"],
        InvocationParameter::U64(7)
    );
    assert_eq!(descriptor.parameters()["seed"], InvocationParameter::U64(9));

    let terminals = PathsOptions {
        by: PathAlgorithm::MinSteinerTree,
        directed: false,
        k: 1,
        via: None,
        weight: None,
        capacity_property: None,
        cost_property: None,
        heuristic: None,
        walk_length: None,
        seed: None,
        terminal_uuids: vec![source_uuid.into_bytes()],
        prize_property: None,
    };
    let descriptor = graph
        .prepare_paths_invocation(None, None, &terminals)
        .unwrap();
    assert_eq!(
        descriptor.parameters()["terminal_uuids"],
        InvocationParameter::UuidList(vec![source_uuid.into_bytes()])
    );

    for by in [
        PathAlgorithm::MinSteinerTree,
        PathAlgorithm::PrizeCollectingSteinerTree,
        PathAlgorithm::GomoryHuTree,
    ] {
        let options = PathsOptions {
            by,
            directed: false,
            k: 1,
            via: None,
            weight: None,
            capacity_property: None,
            cost_property: None,
            heuristic: None,
            walk_length: None,
            seed: None,
            terminal_uuids: Vec::new(),
            prize_property: None,
        };
        let selector = NodeSelector::Uuid(uuid::Uuid::nil());
        let error = graph
            .prepare_paths_invocation(Some(&selector), None, &options)
            .unwrap_err();
        assert_eq!(error.code(), "GF_VALIDATION");
        assert!(error.to_string().contains("does not accept positional"));
    }
}

#[test]
fn vector_descriptor_preparation_requires_and_routes_declared_properties() {
    let graph = GraphForge::new(None).unwrap();
    for by in [ClusterAlgorithm::Hdbscan, ClusterAlgorithm::KMeans] {
        let options = ClusterOptions {
            by,
            vector_property: None,
            via: Some("IGNORED".into()),
            directed: true,
            write_property: None,
        };
        let error = graph
            .prepare_cluster_invocation("Person", &options)
            .unwrap_err();
        assert_eq!(error.code(), "GF_VALIDATION");
        assert_eq!(
            error.to_string(),
            format!("validation error: cluster.{by} requires vector_property")
        );
    }

    for by in [
        SimilarAlgorithm::Knn,
        SimilarAlgorithm::FilteredKnn,
        SimilarAlgorithm::Cosine,
    ] {
        let options = SimilarOptions {
            by,
            k: 3,
            vector_property: None,
            via: Some("KNOWS".into()),
        };
        let error = graph
            .prepare_similar_invocation("Person", &options)
            .unwrap_err();
        assert_eq!(error.code(), "GF_VALIDATION");
        assert_eq!(
            error.to_string(),
            format!("validation error: similar.{by} requires vector_property")
        );
    }

    let cluster = graph
        .prepare_cluster_invocation(
            "Person",
            &ClusterOptions {
                by: ClusterAlgorithm::KMeans,
                vector_property: Some("embedding".into()),
                via: None,
                directed: true,
                write_property: None,
            },
        )
        .unwrap();
    assert_eq!(
        cluster.parameters()["vector_property"],
        InvocationParameter::Utf8("embedding".into())
    );
    assert!(!cluster.parameters().contains_key("via"));

    let similar = graph
        .prepare_similar_invocation(
            "Person",
            &SimilarOptions {
                by: SimilarAlgorithm::FilteredKnn,
                k: 3,
                vector_property: Some("embedding".into()),
                via: Some("KNOWS".into()),
            },
        )
        .unwrap();
    assert_eq!(
        similar.parameters()["vector_property"],
        InvocationParameter::Utf8("embedding".into())
    );
    assert_eq!(
        similar.parameters()["via"],
        InvocationParameter::Utf8("KNOWS".into())
    );
}
