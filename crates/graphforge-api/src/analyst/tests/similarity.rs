use super::*;

fn filtered_node_similarity_options(k: usize, via: Option<&str>) -> SimilarOptions {
    SimilarOptions {
        by: SimilarAlgorithm::FilteredNodeSimilarity,
        k,
        vector_property: None,
        via: via.map(str::to_owned),
    }
}

fn knn_options(k: usize, vector_property: Option<&str>) -> SimilarOptions {
    SimilarOptions {
        by: SimilarAlgorithm::Knn,
        k,
        vector_property: vector_property.map(str::to_owned),
        via: None,
    }
}

fn cosine_options(k: usize, vector_property: Option<&str>) -> SimilarOptions {
    SimilarOptions {
        by: SimilarAlgorithm::Cosine,
        k,
        vector_property: vector_property.map(str::to_owned),
        via: None,
    }
}

fn filtered_knn_options(
    k: usize,
    vector_property: Option<&str>,
    via: Option<&str>,
) -> SimilarOptions {
    SimilarOptions {
        by: SimilarAlgorithm::FilteredKnn,
        k,
        vector_property: vector_property.map(str::to_owned),
        via: via.map(str::to_owned),
    }
}

#[test]
fn node_similarity_obeys_uuid_jaccard_top_k_via_and_order_contracts() {
    assert_eq!(SimilarOptions::default().k, 10);
    let graph = GraphForge::new(None).unwrap();
    let nodes = ["Alice", "Bob", "Carol", "Dan", "Eve"].map(|name| add_person(&graph, name));
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (e:Person {name:'Eve'}) \
                 CREATE (a)-[:KNOWS]->(d), (a)-[:KNOWS]->(e), \
                 (a)-[:KNOWS]->(e), (b)-[:KNOWS]->(d), \
                 (b)-[:KNOWS]->(e), (c)-[:KNOWS]->(d), \
                 (a)-[:OTHER]->(d), (c)-[:OTHER]->(d)",
        )
        .unwrap();

    let batch = graph
        .similar("Person", node_similarity_options(2, Some("KNOWS")))
        .unwrap();
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| (field.name().as_str(), field.data_type()))
            .collect::<Vec<_>>(),
        [
            ("node1_uuid", &DataType::FixedSizeBinary(16)),
            ("node2_uuid", &DataType::FixedSizeBinary(16)),
            ("similarity", &DataType::Float64),
        ]
    );
    assert_eq!(
        batch.schema().metadata()["graphforge.algorithm"],
        "node_similarity"
    );
    assert!(batch.column_by_name("node1_id").is_none());
    let left = batch
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let right = batch
        .column(1)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let expected_pairs = [(0, 1), (0, 2), (1, 0), (1, 2), (2, 0), (2, 1)];
    for (row, (source, target)) in expected_pairs.into_iter().enumerate() {
        assert_eq!(left.value(row), nodes[source].uuid.as_bytes());
        assert_eq!(right.value(row), nodes[target].uuid.as_bytes());
    }
    assert_eq!(
        batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values(),
        &[1.0, 0.5, 1.0, 0.5, 0.5, 0.5]
    );
    assert_eq!(
        batch,
        graph
            .similar("Person", node_similarity_options(2, Some("KNOWS")))
            .unwrap()
    );
    assert_eq!(
        graph
            .similar("Person", node_similarity_options(1, Some("KNOWS")))
            .unwrap()
            .num_rows(),
        3
    );
    let other = graph
        .similar("Person", node_similarity_options(10, Some("OTHER")))
        .unwrap();
    assert_eq!(other.num_rows(), 2);
    assert_eq!(
        other
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values(),
        &[1.0, 1.0]
    );
}

#[test]
fn node_similarity_empty_and_invalid_inputs_are_structured() {
    let graph = GraphForge::new(None).unwrap();
    let empty = graph
        .similar("Person", node_similarity_options(10, None))
        .unwrap();
    assert_eq!(empty.num_rows(), 0);
    assert_eq!(empty.schema().field(2).data_type(), &DataType::Float64);

    let mut vector = node_similarity_options(10, None);
    vector.vector_property = Some("embedding".into());
    for result in [
        graph.similar("", node_similarity_options(10, None)),
        graph.similar("Person", node_similarity_options(10, Some(" "))),
        graph.similar("Person", node_similarity_options(0, None)),
        graph.similar("Person", vector),
        graph.similar(
            "Person",
            SimilarOptions {
                by: SimilarAlgorithm::Knn,
                ..SimilarOptions::default()
            },
        ),
    ] {
        assert!(matches!(result, Err(GfError::Validation(_))));
    }
}

#[test]
fn filtered_node_similarity_filters_candidates_and_shapes_uuid_jaccard() {
    let graph = GraphForge::new(None).unwrap();
    let nodes = ["Alice", "Bob", "Carol", "Dan", "Eve"].map(|name| add_person(&graph, name));
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}) \
                 CREATE (a)-[:KNOWS]->(a), (a)-[:KNOWS]->(b), \
                 (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(c), \
                 (a)-[:KNOWS]->(d), (b)-[:KNOWS]->(a), \
                 (b)-[:KNOWS]->(c), (b)-[:KNOWS]->(d), \
                 (c)-[:KNOWS]->(d), (d)-[:KNOWS]->(c)",
        )
        .unwrap();

    let batch = graph
        .similar("Person", filtered_node_similarity_options(2, Some("KNOWS")))
        .unwrap();
    assert_eq!(
        batch.schema().metadata()["graphforge.algorithm"],
        "filtered_node_similarity"
    );
    assert!(
        batch
            .schema()
            .fields()
            .iter()
            .all(|field| !field.is_nullable())
    );
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.data_type())
            .collect::<Vec<_>>(),
        [
            &DataType::FixedSizeBinary(16),
            &DataType::FixedSizeBinary(16),
            &DataType::Float64
        ]
    );
    let left = batch
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let right = batch
        .column(1)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    for (row, (source, target)) in [(0, 1), (0, 2), (1, 0), (1, 2)].into_iter().enumerate() {
        assert_eq!(left.value(row), nodes[source].uuid.as_bytes());
        assert_eq!(right.value(row), nodes[target].uuid.as_bytes());
    }
    assert_eq!(
        batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values(),
        &[0.75, 0.25, 0.75, 1.0 / 3.0]
    );
    assert_eq!(
        batch,
        graph
            .similar("Person", filtered_node_similarity_options(2, Some("KNOWS")))
            .unwrap()
    );
    assert_eq!(
        graph
            .similar("Person", filtered_node_similarity_options(1, Some("KNOWS")))
            .unwrap()
            .num_rows(),
        2
    );
    assert_eq!(
        graph
            .similar("Person", filtered_node_similarity_options(10, None))
            .unwrap()
            .num_rows(),
        6
    );
    assert_eq!(
        graph
            .similar(
                "Person",
                filtered_node_similarity_options(10, Some("MISSING"))
            )
            .unwrap()
            .num_rows(),
        0
    );
}

#[test]
fn filtered_node_similarity_boundaries_and_validation_are_structured() {
    let graph = GraphForge::new(None).unwrap();
    assert_eq!(
        graph
            .similar("Person", filtered_node_similarity_options(10, None))
            .unwrap()
            .num_rows(),
        0
    );
    add_person(&graph, "Alice");
    assert_eq!(
        graph
            .similar("Person", filtered_node_similarity_options(10, None))
            .unwrap()
            .num_rows(),
        0
    );

    let mut vector = filtered_node_similarity_options(10, None);
    vector.vector_property = Some("embedding".into());
    for result in [
        graph.similar("", filtered_node_similarity_options(10, None)),
        graph.similar("Person", filtered_node_similarity_options(10, Some(" "))),
        graph.similar("Person", filtered_node_similarity_options(0, None)),
        graph.similar("Person", vector),
    ] {
        assert!(matches!(result, Err(GfError::Validation(_))));
    }
}

#[test]
fn knn_obeys_uuid_cosine_top_k_schema_and_topology_independence() {
    let graph = GraphForge::new(None).unwrap();
    graph
        .execute(
            "CREATE (:Person {name:'a', embedding:[1.0, 0.0]}), \
                 (:Person {name:'b', embedding:[1.0, 0.0]}), \
                 (:Person {name:'c', embedding:[1.0, 1.0]}), \
                 (:Person {name:'d', embedding:[0.0, 1.0]}), \
                 (:Person {name:'e', embedding:[-1.0, 0.0]})",
        )
        .unwrap();
    let batch = graph
        .similar("Person", knn_options(2, Some("embedding")))
        .unwrap();
    assert_eq!(
        batch
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
            ("node1_uuid", &DataType::FixedSizeBinary(16), false),
            ("node2_uuid", &DataType::FixedSizeBinary(16), false),
            ("similarity", &DataType::Float64, false),
        ]
    );
    assert_eq!(batch.schema().metadata()["graphforge.algorithm"], "knn");
    let identities = graph
        .execute("MATCH (n:Person) RETURN n.node_uuid AS uuid ORDER BY n.name")
        .unwrap();
    let identities = identities.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let left = batch
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let right = batch
        .column(1)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let expected = [
        (0, 1),
        (0, 2),
        (1, 0),
        (1, 2),
        (2, 0),
        (2, 1),
        (3, 2),
        (3, 0),
        (4, 3),
    ];
    for (row, (source, target)) in expected.into_iter().enumerate() {
        assert_eq!(left.value(row), identities.value(source));
        assert_eq!(right.value(row), identities.value(target));
    }
    let scores = batch
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(scores.value(0), 1.0);
    assert!((scores.value(1) - 2.0_f64.sqrt().recip()).abs() < 1e-12);
    assert_eq!(scores.value(8), 0.0);

    graph
        .execute(
            "MATCH (a:Person {name:'a'}), (b:Person {name:'b'}) \
                 CREATE (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(a)",
        )
        .unwrap();
    assert_eq!(
        batch,
        graph
            .similar("Person", knn_options(2, Some("embedding")))
            .unwrap()
    );
}

#[test]
fn knn_empty_and_invalid_vectors_are_structured() {
    let empty = GraphForge::new(None).unwrap();
    assert_eq!(
        empty
            .similar("Person", knn_options(10, Some("embedding")))
            .unwrap()
            .num_rows(),
        0
    );
    let zero = GraphForge::new(None).unwrap();
    zero.execute("CREATE (:Person {embedding:[0.0, 0.0]})")
        .unwrap();
    let ragged = GraphForge::new(None).unwrap();
    ragged
        .execute(
            "CREATE (:Person {embedding:[1.0]}), \
                 (:Person {embedding:[1.0, 2.0]})",
        )
        .unwrap();
    let mut via = knn_options(1, Some("embedding"));
    via.via = Some("KNOWS".into());
    for result in [
        empty.similar("Person", knn_options(1, None)),
        empty.similar("Person", via),
        zero.similar("Person", knn_options(1, Some("embedding"))),
        ragged.similar("Person", knn_options(1, Some("embedding"))),
    ] {
        assert!(matches!(result, Err(GfError::Validation(_))));
    }
}

#[test]
fn cosine_keeps_all_scores_with_uuid_schema_and_ignores_topology() {
    // Exploratory mode has no ontology/knowledge layer (#772).
    let graph = GraphForge::new(None).unwrap();
    graph
        .execute(
            "CREATE (:Person {name:'a', embedding:[1.0, 0.0]}), \
                 (:Person {name:'b', embedding:[0.0, 1.0]}), \
                 (:Person {name:'c', embedding:[-1.0, 0.0]}), \
                 (:Person {name:'d', embedding:[-1.0, -1.0]})",
        )
        .unwrap();
    let options = cosine_options(3, Some("embedding"));
    let batch = graph.similar("Person", options.clone()).unwrap();
    assert_eq!(batch, graph.similar("Person", options).unwrap());
    assert_eq!(batch.num_rows(), 12);
    assert_eq!(batch.schema().metadata()["graphforge.algorithm"], "cosine");
    for (field, (name, data_type)) in batch.schema().fields().iter().zip([
        ("node1_uuid", DataType::FixedSizeBinary(16)),
        ("node2_uuid", DataType::FixedSizeBinary(16)),
        ("similarity", DataType::Float64),
    ]) {
        assert_eq!(field.name(), name);
        assert_eq!(field.data_type(), &data_type);
        assert!(!field.is_nullable());
    }

    let scores = batch
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let root_half = 2.0_f64.sqrt().recip();
    for (actual, expected) in scores.values().iter().zip([
        0.0, -root_half, -1.0, 0.0, 0.0, -root_half, root_half, 0.0, -1.0, root_half, -root_half,
        -root_half,
    ]) {
        assert!((actual - expected).abs() < 1e-12);
    }
    let identities = graph
        .execute("MATCH (n:Person) RETURN n.node_uuid AS uuid ORDER BY n.name")
        .unwrap();
    let identities = identities.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let left = batch
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let right = batch
        .column(1)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    for (row, (source, target)) in [
        (0, 1),
        (0, 3),
        (0, 2),
        (1, 0),
        (1, 2),
        (1, 3),
        (2, 3),
        (2, 1),
        (2, 0),
        (3, 2),
        (3, 0),
        (3, 1),
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(left.value(row), identities.value(source));
        assert_eq!(right.value(row), identities.value(target));
    }

    graph
        .execute(
            "MATCH (a:Person {name:'a'}), (b:Person {name:'b'}) \
                 CREATE (a)-[:KNOWS]->(b)",
        )
        .unwrap();
    assert_eq!(
        batch,
        graph
            .similar("Person", cosine_options(3, Some("embedding")))
            .unwrap()
    );
}

#[test]
fn cosine_defaults_top_k_and_rejects_invalid_inputs() {
    let empty = GraphForge::new(None).unwrap();
    let defaults = SimilarOptions {
        by: SimilarAlgorithm::Cosine,
        vector_property: Some("embedding".into()),
        ..SimilarOptions::default()
    };
    assert_eq!(defaults.k, 10);
    assert_eq!(empty.similar("Person", defaults).unwrap().num_rows(), 0);

    let singleton = GraphForge::new(None).unwrap();
    singleton
        .execute("CREATE (:Person {embedding:[1.0]})")
        .unwrap();
    assert_eq!(
        singleton
            .similar("Person", cosine_options(1, Some("embedding")))
            .unwrap()
            .num_rows(),
        0
    );
    let zero = GraphForge::new(None).unwrap();
    zero.execute("CREATE (:Person {embedding:[0.0, 0.0]})")
        .unwrap();
    let ragged = GraphForge::new(None).unwrap();
    ragged
        .execute(
            "CREATE (:Person {embedding:[1.0]}), \
                 (:Person {embedding:[1.0, 2.0]})",
        )
        .unwrap();
    let missing = GraphForge::new(None).unwrap();
    missing.execute("CREATE (:Person {name:'a'})").unwrap();
    let non_finite = GraphForge::new(None).unwrap();
    non_finite
        .add_node(
            "Person",
            &HashMap::from([(
                "embedding".into(),
                PropValue::List(vec![PropValue::Float(f64::NAN)]),
            )]),
        )
        .unwrap();
    let mut via = cosine_options(1, Some("embedding"));
    via.via = Some("KNOWS".into());
    for result in [
        empty.similar("", cosine_options(1, Some("embedding"))),
        empty.similar("Person", cosine_options(1, None)),
        empty.similar("Person", cosine_options(0, Some("embedding"))),
        empty.similar("Person", cosine_options(1, Some(" embedding"))),
        empty.similar("Person", via),
        missing.similar("Person", cosine_options(1, Some("embedding"))),
        non_finite.similar("Person", cosine_options(1, Some("embedding"))),
        zero.similar("Person", cosine_options(1, Some("embedding"))),
        ragged.similar("Person", cosine_options(1, Some("embedding"))),
    ] {
        assert!(matches!(result, Err(GfError::Validation(_))));
    }
}

#[test]
fn filtered_knn_obeys_outgoing_via_uuid_schema_and_stable_top_k() {
    // Exploratory mode has no ontology/knowledge layer (#772).
    let graph = GraphForge::new(None).unwrap();
    graph
        .execute(
            "CREATE (:Person {name:'a', embedding:[1.0, 0.0]}), \
                 (:Person {name:'b', embedding:[1.0, 0.0]}), \
                 (:Person {name:'c', embedding:[1.0, 1.0]}), \
                 (:Person {name:'d', embedding:[0.0, 1.0]}), \
                 (:Person {name:'e', embedding:[-1.0, 0.0]})",
        )
        .unwrap();
    graph
        .execute(
            "MATCH (a:Person {name:'a'}), (b:Person {name:'b'}), \
                 (c:Person {name:'c'}), (d:Person {name:'d'}), \
                 (e:Person {name:'e'}) \
                 CREATE (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), \
                 (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(a), (a)-[:OTHER]->(e), \
                 (b)-[:OTHER]->(a), (c)-[:KNOWS]->(a), (c)-[:KNOWS]->(b), \
                 (d)-[:KNOWS]->(c), (d)-[:KNOWS]->(a), (d)-[:KNOWS]->(e), \
                 (e)-[:KNOWS]->(d)",
        )
        .unwrap();

    let options = filtered_knn_options(2, Some("embedding"), Some("KNOWS"));
    let batch = graph.similar("Person", options.clone()).unwrap();
    assert_eq!(batch, graph.similar("Person", options).unwrap());
    let schema = batch.schema();
    assert_eq!(schema.metadata()["graphforge.algorithm"], "filtered_knn");
    assert_eq!(schema.metadata()["graphforge.verb"], "similar");
    for (field, (name, data_type)) in schema.fields().iter().zip([
        ("node1_uuid", DataType::FixedSizeBinary(16)),
        ("node2_uuid", DataType::FixedSizeBinary(16)),
        ("similarity", DataType::Float64),
    ]) {
        assert_eq!(field.name(), name);
        assert_eq!(field.data_type(), &data_type);
        assert!(!field.is_nullable());
    }

    let identities = graph
        .execute("MATCH (n:Person) RETURN n.node_uuid AS uuid ORDER BY n.name")
        .unwrap();
    let identities = identities.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let left = batch
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let right = batch
        .column(1)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    for (row, (source, target)) in [(0, 1), (0, 2), (2, 0), (2, 1), (3, 2), (3, 0), (4, 3)]
        .into_iter()
        .enumerate()
    {
        assert_eq!(left.value(row), identities.value(source));
        assert_eq!(right.value(row), identities.value(target));
    }
    let scores = batch
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(scores.value(0), 1.0);
    assert!((scores.value(1) - 2.0_f64.sqrt().recip()).abs() < 1e-12);
    assert_eq!(scores.value(6), 0.0);

    for (k, via, expected_rows) in [
        (2, None, 8),
        (10, Some("KNOWS"), 8),
        (2, Some("MISSING"), 0),
    ] {
        assert_eq!(
            graph
                .similar("Person", filtered_knn_options(k, Some("embedding"), via))
                .unwrap()
                .num_rows(),
            expected_rows
        );
    }
}

#[test]
fn filtered_knn_empty_singleton_and_invalid_inputs_are_structured() {
    let empty = GraphForge::new(None).unwrap();
    assert_eq!(
        empty
            .similar("Person", filtered_knn_options(1, Some("embedding"), None))
            .unwrap()
            .num_rows(),
        0
    );
    let singleton = GraphForge::new(None).unwrap();
    singleton
        .execute("CREATE (a:Person {embedding:[1.0]})-[:KNOWS]->(a)")
        .unwrap();
    assert_eq!(
        singleton
            .similar(
                "Person",
                filtered_knn_options(1, Some("embedding"), Some("KNOWS")),
            )
            .unwrap()
            .num_rows(),
        0
    );
    let zero = GraphForge::new(None).unwrap();
    zero.execute("CREATE (:Person {embedding:[0.0, 0.0]})")
        .unwrap();
    let ragged = GraphForge::new(None).unwrap();
    ragged
        .execute(
            "CREATE (:Person {embedding:[1.0]}), \
                 (:Person {embedding:[1.0, 2.0]})",
        )
        .unwrap();
    for result in [
        empty.similar("Person", filtered_knn_options(1, None, None)),
        empty.similar("Person", filtered_knn_options(0, Some("embedding"), None)),
        empty.similar(
            "Person",
            filtered_knn_options(1, Some("embedding"), Some(" ")),
        ),
        zero.similar("Person", filtered_knn_options(1, Some("embedding"), None)),
        ragged.similar("Person", filtered_knn_options(1, Some("embedding"), None)),
    ] {
        assert!(matches!(result, Err(GfError::Validation(_))));
    }
}
