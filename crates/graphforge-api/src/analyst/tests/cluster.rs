use super::*;

fn strongly_connected_options(
    directed: bool,
    via: Option<&str>,
    write_property: Option<&str>,
) -> ClusterOptions {
    ClusterOptions {
        by: ClusterAlgorithm::StronglyConnected,
        vector_property: None,
        via: via.map(str::to_owned),
        directed,
        write_property: write_property.map(str::to_owned),
    }
}

fn biconnected_options(
    directed: bool,
    via: Option<&str>,
    write_property: Option<&str>,
) -> ClusterOptions {
    ClusterOptions {
        by: ClusterAlgorithm::Biconnected,
        vector_property: None,
        via: via.map(str::to_owned),
        directed,
        write_property: write_property.map(str::to_owned),
    }
}

fn k_core_decomposition_options(
    directed: bool,
    via: Option<&str>,
    write_property: Option<&str>,
) -> ClusterOptions {
    ClusterOptions {
        by: ClusterAlgorithm::KCoreDecomposition,
        vector_property: None,
        via: via.map(str::to_owned),
        directed,
        write_property: write_property.map(str::to_owned),
    }
}

fn approximate_max_cut_options(
    directed: bool,
    via: Option<&str>,
    write_property: Option<&str>,
) -> ClusterOptions {
    ClusterOptions {
        by: ClusterAlgorithm::ApproximateMaxKCut,
        vector_property: None,
        via: via.map(str::to_owned),
        directed,
        write_property: write_property.map(str::to_owned),
    }
}

#[test]
fn components_obeys_uuid_schema_direction_via_and_multigraph_contracts() {
    let graph = GraphForge::new(None).unwrap();
    graph
        .execute(
            "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), (e:Person), \
                 (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(a), \
                 (c)-[:OTHER]->(d)",
        )
        .unwrap();

    let directed = graph
        .cluster("Person", components_options(true, Some("KNOWS")))
        .unwrap();
    assert_eq!(community_ids(&directed), [0, 0, 1, 2, 3]);
    assert_eq!(
        directed
            .schema()
            .fields()
            .iter()
            .map(|field| field.name())
            .collect::<Vec<_>>(),
        ["node_uuid", "community_id", "name"]
    );
    assert_eq!(
        directed.schema().field(0).data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert_eq!(directed.schema().field(1).data_type(), &DataType::Int64);
    assert!(directed.column_by_name("node_id").is_none());
    assert_eq!(
        directed
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [Some("Alice"), Some("Bob"), Some("Carol"), Some("Dan"), None]
    );
    assert_eq!(
        directed,
        graph
            .cluster("Person", components_options(true, Some("KNOWS")))
            .unwrap()
    );
    assert_eq!(
        community_ids(
            &graph
                .cluster("Person", components_options(false, Some("KNOWS")))
                .unwrap()
        ),
        [0, 0, 1, 2, 3]
    );
    assert_eq!(
        community_ids(
            &graph
                .cluster("Person", components_options(false, None))
                .unwrap()
        ),
        [0, 0, 1, 1, 2]
    );
}

#[test]
fn components_writeback_empty_and_invalid_inputs_are_structured() {
    let graph = GraphForge::new(None).unwrap();
    let empty = graph
        .cluster("Person", components_options(false, None))
        .unwrap();
    assert_eq!(empty.num_rows(), 0);
    assert_eq!(empty.schema().field(1).data_type(), &DataType::Int64);

    graph
        .execute(
            "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (a)-[:KNOWS]->(b)",
        )
        .unwrap();
    assert_eq!(
        graph
            .execute("MATCH (p:Person) WHERE p.component IS NOT NULL RETURN p.name AS name")
            .unwrap()
            .stats
            .rows_produced,
        0
    );
    let mut options = components_options(false, Some("KNOWS"));
    options.write_property = Some("component".into());
    assert_eq!(
        community_ids(&graph.cluster("Person", options).unwrap()),
        [0, 0, 1]
    );
    let readback = graph
        .execute("MATCH (p:Person) RETURN p.component AS component ORDER BY p.name")
        .unwrap();
    assert_eq!(
        readback.batches[0]
            .column_by_name("component")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[0, 0, 1]
    );

    for result in [
        graph.cluster("", components_options(false, None)),
        graph.cluster("Person", components_options(false, Some(" "))),
        graph.cluster(
            "Person",
            ClusterOptions {
                by: ClusterAlgorithm::Hdbscan,
                ..ClusterOptions::default()
            },
        ),
    ] {
        assert!(matches!(result, Err(GfError::Validation(_))));
    }
}

#[test]
fn components_public_writeback_is_atomic_empty_and_persistent() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap();
    let graph = GraphForge::new(Some(path)).unwrap();
    graph
        .execute(
            "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (a)-[:KNOWS]->(b)",
        )
        .unwrap();

    let _read_only = graph
        .cluster("Person", components_options(false, Some("KNOWS")))
        .unwrap();
    assert_eq!(
        graph
            .execute("MATCH (n:Person) WHERE n.component IS NOT NULL RETURN n.component")
            .unwrap()
            .stats
            .rows_produced,
        0
    );

    graph
        .execute("MATCH (n:Person {name:'Alice'}) SET n.atomic_component = 'old'")
        .unwrap();
    for property in ["", "atomic_component"] {
        let mut options = components_options(false, Some("KNOWS"));
        options.write_property = Some(property.into());
        assert!(matches!(
            graph.cluster("Person", options),
            Err(GfError::Validation(_))
        ));
    }
    let unchanged = graph
        .execute(
            "MATCH (n:Person) WHERE n.atomic_component IS NOT NULL \
                 RETURN n.name AS name, n.atomic_component AS value ORDER BY name",
        )
        .unwrap();
    assert_eq!(unchanged.stats.rows_produced, 1);
    assert_eq!(
        unchanged.batches[0]
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "old"
    );

    let mut empty_options = components_options(false, Some("KNOWS"));
    empty_options.write_property = Some("empty_component".into());
    assert_eq!(
        graph.cluster("Missing", empty_options).unwrap().num_rows(),
        0
    );
    assert_eq!(
        graph
            .execute(
                "MATCH (n:Person) WHERE n.empty_component IS NOT NULL \
                     RETURN n.empty_component"
            )
            .unwrap()
            .stats
            .rows_produced,
        0
    );

    let expected = graph
        .cluster("Person", components_options(false, Some("KNOWS")))
        .unwrap();
    let mut options = components_options(false, Some("KNOWS"));
    options.write_property = Some("component".into());
    let written = graph.cluster("Person", options).unwrap();
    assert_eq!(written, expected);
    drop(graph);

    let reopened = GraphForge::new(Some(path)).unwrap();
    let persisted = reopened
        .execute("MATCH (n:Person) RETURN n.component AS component ORDER BY n.name")
        .unwrap();
    assert_eq!(
        persisted.batches[0]
            .column_by_name("component")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[0, 0, 1]
    );
}

#[test]
fn cluster_vector_property_is_typed_and_owned_by_vector_algorithms() {
    let graph = GraphForge::new(None).unwrap();
    let validation = |options| match graph.cluster("Person", options) {
        Err(GfError::Validation(message)) => message,
        other => panic!("expected validation error, got {other:?}"),
    };

    for by in [ClusterAlgorithm::Hdbscan, ClusterAlgorithm::KMeans] {
        assert_eq!(
            validation(ClusterOptions {
                by,
                ..ClusterOptions::default()
            }),
            format!("cluster.{} requires vector_property", by.as_str())
        );
    }

    for by in [ClusterAlgorithm::Hdbscan, ClusterAlgorithm::KMeans] {
        assert_eq!(
            validation(ClusterOptions {
                by,
                vector_property: Some("features".into()),
                via: Some("KNOWS".into()),
                ..ClusterOptions::default()
            }),
            format!("cluster.{} does not accept via", by.as_str())
        );
    }

    for by in [
        ClusterAlgorithm::Components,
        ClusterAlgorithm::ApproximateMaxKCut,
        ClusterAlgorithm::StronglyConnected,
        ClusterAlgorithm::Biconnected,
        ClusterAlgorithm::KCoreDecomposition,
    ] {
        assert_eq!(
            validation(ClusterOptions {
                by,
                vector_property: Some("features".into()),
                ..ClusterOptions::default()
            }),
            format!("cluster.{} does not accept vector_property", by.as_str())
        );
    }
    for property in ["", " features", "features ", "fea\ntures"] {
        assert_eq!(
            validation(ClusterOptions {
                by: ClusterAlgorithm::Hdbscan,
                vector_property: Some(property.into()),
                ..ClusterOptions::default()
            }),
            format!("invalid cluster vector property {property:?}")
        );
    }
}

#[test]
fn strongly_connected_obeys_direction_via_uuid_schema_and_atomic_writeback() {
    let empty = GraphForge::new(None)
        .unwrap()
        .cluster(
            "Person",
            strongly_connected_options(true, Some("KNOWS"), None),
        )
        .unwrap();
    assert_eq!(empty.num_rows(), 0);
    assert_eq!(
        empty.schema().field(0).data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert_eq!(empty.schema().field(1).data_type(), &DataType::Int64);

    let graph = GraphForge::new(None).unwrap();
    graph
        .execute(
            "CREATE (a:Person {name:'a'}), (b:Person {name:'b'}), \
                 (c:Person {name:'c'}), (d:Person {name:'d'}), \
                 (e:Person {name:'e'}), (f:Person {name:'f'}), \
                 (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), \
                 (b)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), \
                 (c)-[:KNOWS]->(a), (c)-[:KNOWS]->(d), \
                 (d)-[:KNOWS]->(e), (e)-[:KNOWS]->(d), \
                 (e)-[:KNOWS]->(f), (f)-[:OTHER]->(a)",
        )
        .unwrap();

    let directed = graph
        .cluster(
            "Person",
            strongly_connected_options(true, Some("KNOWS"), None),
        )
        .unwrap();
    assert_eq!(community_ids(&directed), [0, 0, 0, 1, 1, 2]);
    assert_eq!(
        directed
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["node_uuid", "community_id", "name"]
    );
    assert!(!directed.schema().field(0).is_nullable());
    assert!(!directed.schema().field(1).is_nullable());
    assert!(directed.column_by_name("node_id").is_none());
    assert_eq!(
        directed.schema().metadata()["graphforge.algorithm"],
        "strongly_connected"
    );
    assert_eq!(
        directed
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        ["a", "b", "c", "d", "e", "f"].map(Some)
    );
    let expected = graph
        .execute("MATCH (p:Person) RETURN p.node_uuid AS node_uuid ORDER BY p.name")
        .unwrap();
    assert_eq!(
        directed.column_by_name("node_uuid").unwrap(),
        expected.batches[0].column_by_name("node_uuid").unwrap()
    );
    assert_eq!(
        community_ids(
            &graph
                .cluster(
                    "Person",
                    strongly_connected_options(false, Some("KNOWS"), None),
                )
                .unwrap()
        ),
        [0, 0, 0, 0, 0, 0]
    );
    assert_eq!(
        community_ids(
            &graph
                .cluster("Person", strongly_connected_options(true, None, None))
                .unwrap()
        ),
        [0, 0, 0, 0, 0, 0]
    );
    assert_eq!(
        graph
            .execute("MATCH (p:Person) WHERE p.scc IS NOT NULL RETURN p")
            .unwrap()
            .stats
            .rows_produced,
        0
    );

    graph
        .execute("MATCH (p:Person {name:'a'}) SET p.scc_atomic = 'old'")
        .unwrap();
    assert!(matches!(
        graph.cluster(
            "Person",
            strongly_connected_options(true, Some("KNOWS"), Some("scc_atomic")),
        ),
        Err(GfError::Validation(_))
    ));
    let unchanged = graph
        .execute(
            "MATCH (p:Person) WHERE p.scc_atomic IS NOT NULL \
                 RETURN p.scc_atomic AS value",
        )
        .unwrap();
    assert_eq!(unchanged.stats.rows_produced, 1);
    assert_eq!(
        unchanged.batches[0]
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "old"
    );
    graph
        .cluster(
            "Person",
            strongly_connected_options(true, Some("KNOWS"), Some("scc")),
        )
        .unwrap();
    let written = graph
        .execute("MATCH (p:Person) RETURN p.scc AS scc ORDER BY p.name")
        .unwrap();
    assert_eq!(
        written.batches[0]
            .column_by_name("scc")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[0, 0, 0, 1, 1, 2]
    );
}

#[test]
fn biconnected_projects_overlap_direction_neutrally_and_writes_atomically() {
    let graph = GraphForge::new(None).unwrap();
    assert_eq!(
        graph
            .cluster("Person", biconnected_options(true, Some("KNOWS"), None))
            .unwrap()
            .num_rows(),
        0
    );
    graph
        .execute(
            "CREATE (a:Person {name:'a'}), (b:Person {name:'b'}), \
                 (c:Person {name:'c'}), (d:Person {name:'d'}), \
                 (e:Person {name:'e'}), (f:Person {name:'f'}), \
                 (g:Person {name:'g'}), (a)-[:KNOWS {weight:99}]->(b), \
                 (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), (c)-[:KNOWS]->(a), \
                 (c)-[:KNOWS]->(d), (d)-[:KNOWS]->(e), (e)-[:KNOWS]->(c), \
                 (e)-[:KNOWS]->(f), (f)-[:KNOWS]->(f), (g)-[:OTHER]->(a)",
        )
        .unwrap();

    let options = biconnected_options(true, Some("KNOWS"), None);
    let directed = graph.cluster("Person", options.clone()).unwrap();
    assert_eq!(community_ids(&directed), [0, 0, 0, 1, 1, 2, 3]);
    assert_eq!(
        community_ids(
            &graph
                .cluster("Person", biconnected_options(false, Some("KNOWS"), None))
                .unwrap()
        ),
        community_ids(&directed)
    );
    assert_eq!(
        community_ids(
            &graph
                .cluster("Person", biconnected_options(true, Some("OTHER"), None))
                .unwrap()
        ),
        [0, 1, 2, 3, 4, 5, 0]
    );
    assert_eq!(
        directed
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["node_uuid", "community_id", "name"]
    );
    assert_eq!(
        directed.schema().metadata()["graphforge.algorithm"],
        "biconnected"
    );
    assert!(!directed.schema().field(0).is_nullable());
    assert!(!directed.schema().field(1).is_nullable());
    assert!(directed.column_by_name("node_id").is_none());
    let expected = graph
        .execute("MATCH (p:Person) RETURN p.node_uuid AS node_uuid ORDER BY p.name")
        .unwrap();
    assert_eq!(
        directed.column_by_name("node_uuid").unwrap(),
        expected.batches[0].column_by_name("node_uuid").unwrap()
    );
    assert_eq!(
        graph
            .execute("MATCH (p:Person) WHERE p.block IS NOT NULL RETURN p")
            .unwrap()
            .stats
            .rows_produced,
        0
    );

    graph
        .execute("MATCH (p:Person {name:'a'}) SET p.atomic_block = 'old'")
        .unwrap();
    assert!(matches!(
        graph.cluster(
            "Person",
            biconnected_options(true, Some("KNOWS"), Some("atomic_block"))
        ),
        Err(GfError::Validation(_))
    ));
    let unchanged = graph
        .execute(
            "MATCH (p:Person) WHERE p.atomic_block IS NOT NULL \
                 RETURN p.atomic_block AS value",
        )
        .unwrap();
    assert_eq!(unchanged.stats.rows_produced, 1);
    assert_eq!(
        unchanged.batches[0]
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "old"
    );
    graph
        .cluster(
            "Person",
            biconnected_options(true, Some("KNOWS"), Some("block")),
        )
        .unwrap();
    let written = graph
        .execute("MATCH (p:Person) RETURN p.block AS block ORDER BY p.name")
        .unwrap();
    assert_eq!(
        written.batches[0]
            .column_by_name("block")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[0, 0, 0, 1, 1, 2, 3]
    );
}

#[test]
fn k_core_decomposition_shapes_exact_numbers_and_writes_atomically() {
    let graph = GraphForge::new(None).unwrap();
    assert_eq!(
        graph
            .cluster(
                "Person",
                k_core_decomposition_options(true, Some("KNOWS"), None)
            )
            .unwrap()
            .num_rows(),
        0
    );
    graph
        .execute(
            "CREATE (a:Person {name:'a'}), (b:Person {name:'b'}), \
                 (c:Person {name:'c'}), (d:Person {name:'d'}), \
                 (e:Person {name:'e'}), (f:Person {name:'f'}), \
                 (g:Person {name:'g'}), (h:Person {name:'h'}), \
                 (i:Person {name:'i'}), (j:Person {name:'j'}), \
                 (a)-[:KNOWS {weight:99}]->(b), (a)-[:KNOWS]->(b), \
                 (b)-[:KNOWS]->(a), (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(d), \
                 (b)-[:KNOWS]->(c), (b)-[:KNOWS]->(d), (c)-[:KNOWS]->(d), \
                 (c)-[:KNOWS]->(c), (a)-[:KNOWS]->(e), (e)-[:KNOWS]->(f), \
                 (h)-[:KNOWS]->(i), (i)-[:KNOWS]->(j), (j)-[:KNOWS]->(h), \
                 (f)-[:OTHER]->(a)",
        )
        .unwrap();

    let options = k_core_decomposition_options(true, Some("KNOWS"), None);
    let directed = graph.cluster("Person", options).unwrap();
    assert_eq!(community_ids(&directed), [3, 3, 3, 3, 1, 1, 0, 2, 2, 2]);
    assert_eq!(
        community_ids(
            &graph
                .cluster(
                    "Person",
                    k_core_decomposition_options(false, Some("KNOWS"), None)
                )
                .unwrap()
        ),
        community_ids(&directed)
    );
    assert_eq!(
        community_ids(
            &graph
                .cluster(
                    "Person",
                    k_core_decomposition_options(true, Some("OTHER"), None)
                )
                .unwrap()
        ),
        [1, 0, 0, 0, 0, 1, 0, 0, 0, 0]
    );
    assert_eq!(
        directed
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["node_uuid", "community_id", "name"]
    );
    assert_eq!(
        directed.schema().metadata()["graphforge.algorithm"],
        "k_core_decomposition"
    );
    assert!(!directed.schema().field(0).is_nullable());
    assert!(!directed.schema().field(1).is_nullable());
    assert!(directed.column_by_name("node_id").is_none());
    let expected = graph
        .execute("MATCH (p:Person) RETURN p.node_uuid AS node_uuid ORDER BY p.name")
        .unwrap();
    assert_eq!(
        directed.column_by_name("node_uuid").unwrap(),
        expected.batches[0].column_by_name("node_uuid").unwrap()
    );
    let read_only = graph
        .execute("MATCH (p:Person) WHERE p.core IS NOT NULL RETURN p")
        .unwrap();
    assert_eq!(read_only.stats.rows_produced, 0);

    graph
        .execute("MATCH (p:Person {name:'a'}) SET p.atomic_core = 'old'")
        .unwrap();
    assert!(matches!(
        graph.cluster(
            "Person",
            k_core_decomposition_options(true, Some("KNOWS"), Some("atomic_core"))
        ),
        Err(GfError::Validation(_))
    ));
    let unchanged = graph
        .execute(
            "MATCH (p:Person) WHERE p.atomic_core IS NOT NULL \
                 RETURN p.atomic_core AS value",
        )
        .unwrap();
    assert_eq!(unchanged.stats.rows_produced, 1);
    assert_eq!(
        unchanged.batches[0]
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "old"
    );
    graph
        .cluster(
            "Person",
            k_core_decomposition_options(true, Some("KNOWS"), Some("core")),
        )
        .unwrap();
    let written = graph
        .execute("MATCH (p:Person) RETURN p.core AS core ORDER BY p.name")
        .unwrap();
    assert_eq!(
        written.batches[0]
            .column_by_name("core")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[3, 3, 3, 3, 1, 1, 0, 2, 2, 2]
    );

    let edgeless = GraphForge::new(None).unwrap();
    edgeless.execute("CREATE (:Person), (:Person)").unwrap();
    assert_eq!(
        community_ids(
            &edgeless
                .cluster("Person", k_core_decomposition_options(true, None, None))
                .unwrap()
        ),
        [0, 0]
    );
}

#[test]
fn approximate_max_cut_dispatches_uuid_partition_and_atomic_writeback() {
    let empty = GraphForge::new(None)
        .unwrap()
        .cluster(
            "Person",
            approximate_max_cut_options(false, Some("KNOWS"), None),
        )
        .unwrap();
    assert_eq!(empty.num_rows(), 0);
    assert_eq!(
        empty.schema().field(0).data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert_eq!(empty.schema().field(1).data_type(), &DataType::Int64);

    let graph = GraphForge::new(None).unwrap();
    graph
        .execute(
            "CREATE (a:Person {name:'a'}), (b:Person {name:'b'}), \
                 (c:Person {name:'c'}), (d:Person {name:'d'}), \
                 (e:Person {name:'e'}), (a)-[:KNOWS]->(b), \
                 (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(b), \
                 (b)-[:KNOWS]->(c), (c)-[:KNOWS]->(d), \
                 (d)-[:KNOWS]->(a), (a)-[:OTHER]->(e)",
        )
        .unwrap();
    let result = graph
        .cluster(
            "Person",
            approximate_max_cut_options(false, Some("KNOWS"), None),
        )
        .unwrap();
    assert_eq!(
        result
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["node_uuid", "community_id", "name"]
    );
    assert!(!result.schema().field(0).is_nullable());
    assert!(!result.schema().field(1).is_nullable());
    assert!(result.column_by_name("node_id").is_none());
    assert_eq!(
        result.schema().metadata()["graphforge.algorithm"],
        "approximate_max_k_cut"
    );
    assert_eq!(community_ids(&result), [0, 1, 0, 1, 0]);
    assert_eq!(
        result
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [Some("a"), Some("b"), Some("c"), Some("d"), Some("e")]
    );
    let expected = graph
        .execute("MATCH (p:Person) RETURN p.node_uuid AS node_uuid ORDER BY p.name")
        .unwrap();
    assert_eq!(
        result.column_by_name("node_uuid").unwrap(),
        expected.batches[0].column_by_name("node_uuid").unwrap()
    );
    assert_eq!(
        result,
        graph
            .cluster(
                "Person",
                approximate_max_cut_options(true, Some("KNOWS"), None),
            )
            .unwrap()
    );
    assert_eq!(
        community_ids(
            &graph
                .cluster("Person", approximate_max_cut_options(false, None, None))
                .unwrap()
        ),
        [0, 1, 0, 1, 1]
    );
    assert_eq!(
        graph
            .execute("MATCH (p:Person) WHERE p.cluster IS NOT NULL RETURN p")
            .unwrap()
            .stats
            .rows_produced,
        0
    );

    graph
        .execute("MATCH (p:Person {name:'a'}) SET p.maxcut_atomic = 'old'")
        .unwrap();
    assert!(matches!(
        graph.cluster(
            "Person",
            approximate_max_cut_options(false, Some("KNOWS"), Some("maxcut_atomic")),
        ),
        Err(GfError::Validation(_))
    ));
    let unchanged = graph
        .execute(
            "MATCH (p:Person) WHERE p.maxcut_atomic IS NOT NULL \
                 RETURN p.maxcut_atomic AS value",
        )
        .unwrap();
    assert_eq!(unchanged.stats.rows_produced, 1);
    assert_eq!(
        unchanged.batches[0]
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "old"
    );
    graph
        .cluster(
            "Person",
            approximate_max_cut_options(false, Some("KNOWS"), Some("cluster")),
        )
        .unwrap();
    let written = graph
        .execute("MATCH (p:Person) RETURN p.cluster AS cluster ORDER BY p.name")
        .unwrap();
    assert_eq!(
        written.batches[0]
            .column_by_name("cluster")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[0, 1, 0, 1, 0]
    );
}

#[test]
fn kmeans_dispatches_exact_uuid_clusters_and_atomic_writeback() {
    let graph = GraphForge::new(None).unwrap();
    let nodes = (0..20)
        .map(|point| {
            let value = f64::from(point / 2 * 10) + f64::from(point % 2) * 0.25;
            format!("(:Point {{name:'p{point:02}', features:[{value:.2}]}})")
        })
        .collect::<Vec<_>>()
        .join(",");
    graph.execute(&format!("CREATE {nodes}")).unwrap();
    let options = |directed, write_property| ClusterOptions {
        by: ClusterAlgorithm::KMeans,
        vector_property: Some("features".into()),
        directed,
        write_property,
        ..ClusterOptions::default()
    };
    let result = graph.cluster("Point", options(false, None)).unwrap();
    assert_eq!(
        result
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["node_uuid", "community_id", "features", "name"]
    );
    assert_eq!(
        result.schema().field(0).data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert_eq!(result.schema().field(1).data_type(), &DataType::Int64);
    assert!(!result.schema().field(0).is_nullable());
    assert!(!result.schema().field(1).is_nullable());
    assert!(result.column_by_name("node_id").is_none());
    assert_eq!(
        result.schema().metadata()["graphforge.algorithm"],
        "k_means"
    );
    assert_eq!(
        community_ids(&result),
        (0..10).flat_map(|group| [group, group]).collect::<Vec<_>>()
    );
    let expected = graph
        .execute("MATCH (p:Point) RETURN p.node_uuid AS node_uuid ORDER BY p.name")
        .unwrap();
    assert_eq!(
        result.column_by_name("node_uuid").unwrap(),
        expected.batches[0].column_by_name("node_uuid").unwrap()
    );
    assert_eq!(result, graph.cluster("Point", options(true, None)).unwrap());
    assert_eq!(
        graph
            .execute("MATCH (p:Point) WHERE p.cluster IS NOT NULL RETURN p")
            .unwrap()
            .stats
            .rows_produced,
        0
    );

    graph
        .execute("MATCH (p:Point {name:'p00'}) SET p.kmeans_atomic = 'old'")
        .unwrap();
    assert!(matches!(
        graph.cluster("Point", options(false, Some("kmeans_atomic".into()))),
        Err(GfError::Validation(_))
    ));
    let unchanged = graph
        .execute(
            "MATCH (p:Point) WHERE p.kmeans_atomic IS NOT NULL \
                 RETURN p.name AS name, p.kmeans_atomic AS value",
        )
        .unwrap();
    assert_eq!(unchanged.stats.rows_produced, 1);
    assert_eq!(
        unchanged.batches[0]
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "old"
    );
    graph
        .cluster("Point", options(false, Some("cluster".into())))
        .unwrap();
    let written = graph
        .execute("MATCH (p:Point) RETURN p.cluster AS cluster ORDER BY p.name")
        .unwrap();
    assert_eq!(
        written.batches[0]
            .column_by_name("cluster")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9]
    );
}

#[test]
fn kmeans_empty_small_and_invalid_vectors_are_structured() {
    let options = || ClusterOptions {
        by: ClusterAlgorithm::KMeans,
        vector_property: Some("features".into()),
        ..ClusterOptions::default()
    };
    let empty = GraphForge::new(None)
        .unwrap()
        .cluster("Point", options())
        .unwrap();
    assert_eq!(empty.num_rows(), 0);
    assert_eq!(
        empty.schema().field(0).data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert_eq!(empty.schema().field(1).data_type(), &DataType::Int64);

    for query in [
        "CREATE (:Point {features:[0.0]}), (:Point {features:[1.0]})",
        "CREATE (:Point {features:[0.0]}), (:Point {name:'missing'})",
    ] {
        let graph = GraphForge::new(None).unwrap();
        graph.execute(query).unwrap();
        assert!(matches!(
            graph.cluster("Point", options()),
            Err(GfError::Validation(_) | GfError::Algorithm(AlgorithmError::Execution { .. }))
        ));
    }
}

#[test]
fn hdbscan_dispatches_stable_uuid_clusters_and_opt_in_writeback() {
    let graph = GraphForge::new(None).unwrap();
    graph
        .execute(
            "CREATE (:Person {name:'a0', features:[0.0]}), \
                 (:Person {name:'a1', features:[0.1]}), \
                 (:Person {name:'a2', features:[0.2]}), \
                 (:Person {name:'a3', features:[0.3]}), \
                 (:Person {name:'a4', features:[0.4]}), \
                 (:Person {name:'b0', features:[10.0]}), \
                 (:Person {name:'b1', features:[10.1]}), \
                 (:Person {name:'b2', features:[10.2]}), \
                 (:Person {name:'b3', features:[10.3]}), \
                 (:Person {name:'b4', features:[10.4]}), \
                 (:Person {name:'noise', features:[100.0]})",
        )
        .unwrap();
    let options = |directed, write_property| ClusterOptions {
        by: ClusterAlgorithm::Hdbscan,
        vector_property: Some("features".into()),
        via: None,
        directed,
        write_property,
    };

    let undirected = graph.cluster("Person", options(false, None)).unwrap();
    assert_eq!(
        undirected
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["node_uuid", "community_id", "features", "name"]
    );
    assert!(undirected.column_by_name("node_id").is_none());
    assert_eq!(
        undirected.schema().metadata()["graphforge.algorithm"],
        "hdbscan"
    );
    assert_eq!(
        undirected
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(Option::unwrap)
            .collect::<Vec<_>>(),
        [
            "a0", "a1", "a2", "a3", "a4", "b0", "b1", "b2", "b3", "b4", "noise"
        ]
    );
    assert_eq!(
        community_ids(&undirected),
        [0, 0, 0, 0, 0, 1, 1, 1, 1, 1, -1]
    );
    assert_eq!(
        community_ids(&graph.cluster("Person", options(true, None)).unwrap()),
        community_ids(&undirected)
    );
    assert_eq!(
        graph
            .execute("MATCH (p:Person) WHERE p.cluster IS NOT NULL RETURN p")
            .unwrap()
            .stats
            .rows_produced,
        0
    );

    graph
        .execute("MATCH (p:Person {name:'a0'}) SET p.hdbscan_atomic = 'old'")
        .unwrap();
    assert!(matches!(
        graph.cluster("Person", options(false, Some("hdbscan_atomic".into()))),
        Err(GfError::Validation(_))
    ));
    let unchanged = graph
        .execute(
            "MATCH (p:Person) WHERE p.hdbscan_atomic IS NOT NULL \
                 RETURN p.name AS name, p.hdbscan_atomic AS value",
        )
        .unwrap();
    assert_eq!(unchanged.stats.rows_produced, 1);
    for (column, expected) in [("name", "a0"), ("value", "old")] {
        assert_eq!(
            unchanged.batches[0]
                .column_by_name(column)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            expected
        );
    }

    graph
        .cluster("Person", options(false, Some("cluster".into())))
        .unwrap();
    let readback = graph
        .execute("MATCH (p:Person) RETURN p.cluster AS cluster ORDER BY p.name")
        .unwrap();
    assert_eq!(
        readback.batches[0]
            .column_by_name("cluster")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[0, 0, 0, 0, 0, 1, 1, 1, 1, 1, -1]
    );
}

#[test]
fn hdbscan_handles_empty_small_duplicate_and_all_noise_boundaries() {
    let empty = GraphForge::new(None).unwrap();
    let options = || ClusterOptions {
        by: ClusterAlgorithm::Hdbscan,
        vector_property: Some("features".into()),
        ..ClusterOptions::default()
    };
    let result = empty.cluster("Point", options()).unwrap();
    assert_eq!(result.num_rows(), 0);
    assert_eq!(result.schema().field(0).name(), "node_uuid");
    assert_eq!(result.schema().field(1).name(), "community_id");

    for (query, expected) in [
        (
            "CREATE (:Point {features:[0.0]}), (:Point {features:[1.0]}), \
                 (:Point {features:[2.0]}), (:Point {features:[3.0]})",
            vec![-1; 4],
        ),
        (
            "CREATE (:Point {features:[1.0]}), (:Point {features:[1.0]}), \
                 (:Point {features:[1.0]}), (:Point {features:[1.0]}), \
                 (:Point {features:[1.0]})",
            vec![-1; 5],
        ),
        (
            "CREATE (:Point {features:[0.0]}), (:Point {features:[10.0]}), \
                 (:Point {features:[20.0]}), (:Point {features:[30.0]}), \
                 (:Point {features:[40.0]})",
            vec![-1; 5],
        ),
    ] {
        let graph = GraphForge::new(None).unwrap();
        graph.execute(query).unwrap();
        assert_eq!(
            community_ids(&graph.cluster("Point", options()).unwrap()),
            expected
        );
    }
}

#[test]
fn hdbscan_vector_failures_are_structured_before_dispatch() {
    for query in [
        "CREATE (:Point {name:'missing'})",
        "CREATE (:Point {features:null})",
        "CREATE (:Point {features:[]})",
        "CREATE (:Point {features:[1.0]}), (:Point {features:[1.0,2.0]})",
    ] {
        let graph = GraphForge::new(None).unwrap();
        graph.execute(query).unwrap();
        assert!(matches!(
            graph.cluster(
                "Point",
                ClusterOptions {
                    by: ClusterAlgorithm::Hdbscan,
                    vector_property: Some("features".into()),
                    ..ClusterOptions::default()
                }
            ),
            Err(GfError::Validation(_))
        ));
    }
}
