use super::*;

fn louvain_options(
    directed: bool,
    via: Option<&str>,
    write_property: Option<&str>,
) -> ClusterOptions {
    ClusterOptions {
        by: ClusterAlgorithm::Louvain,
        vector_property: None,
        via: via.map(str::to_owned),
        directed,
        write_property: write_property.map(str::to_owned),
    }
}

fn leiden_options(
    directed: bool,
    via: Option<&str>,
    write_property: Option<&str>,
) -> ClusterOptions {
    ClusterOptions {
        by: ClusterAlgorithm::Leiden,
        vector_property: None,
        via: via.map(str::to_owned),
        directed,
        write_property: write_property.map(str::to_owned),
    }
}

fn label_propagation_options(
    directed: bool,
    via: Option<&str>,
    write_property: Option<&str>,
) -> ClusterOptions {
    ClusterOptions {
        by: ClusterAlgorithm::LabelPropagation,
        vector_property: None,
        via: via.map(str::to_owned),
        directed,
        write_property: write_property.map(str::to_owned),
    }
}

fn speaker_listener_options(
    directed: bool,
    via: Option<&str>,
    write_property: Option<&str>,
) -> ClusterOptions {
    ClusterOptions {
        by: ClusterAlgorithm::SpeakerListener,
        vector_property: None,
        via: via.map(str::to_owned),
        directed,
        write_property: write_property.map(str::to_owned),
    }
}

fn girvan_newman_options(
    directed: bool,
    via: Option<&str>,
    write_property: Option<&str>,
) -> ClusterOptions {
    ClusterOptions {
        by: ClusterAlgorithm::GirvanNewman,
        vector_property: None,
        via: via.map(str::to_owned),
        directed,
        write_property: write_property.map(str::to_owned),
    }
}

fn modularity_optimization_options(
    directed: bool,
    via: Option<&str>,
    write_property: Option<&str>,
) -> ClusterOptions {
    ClusterOptions {
        by: ClusterAlgorithm::ModularityOptimization,
        vector_property: None,
        via: via.map(str::to_owned),
        directed,
        write_property: write_property.map(str::to_owned),
    }
}

fn fastgreedy_options(
    directed: bool,
    via: Option<&str>,
    write_property: Option<&str>,
) -> ClusterOptions {
    ClusterOptions {
        by: ClusterAlgorithm::FastGreedy,
        vector_property: None,
        via: via.map(str::to_owned),
        directed,
        write_property: write_property.map(str::to_owned),
    }
}

fn infomap_options(
    directed: bool,
    via: Option<&str>,
    write_property: Option<&str>,
) -> ClusterOptions {
    ClusterOptions {
        by: ClusterAlgorithm::InfoMap,
        vector_property: None,
        via: via.map(str::to_owned),
        directed,
        write_property: write_property.map(str::to_owned),
    }
}

fn leading_eigenvector_options(
    directed: bool,
    via: Option<&str>,
    write_property: Option<&str>,
) -> ClusterOptions {
    ClusterOptions {
        by: ClusterAlgorithm::LeadingEigenvector,
        vector_property: None,
        via: via.map(str::to_owned),
        directed,
        write_property: write_property.map(str::to_owned),
    }
}

fn walktrap_options(
    directed: bool,
    via: Option<&str>,
    write_property: Option<&str>,
) -> ClusterOptions {
    ClusterOptions {
        by: ClusterAlgorithm::Walktrap,
        vector_property: None,
        via: via.map(str::to_owned),
        directed,
        write_property: write_property.map(str::to_owned),
    }
}

fn spinglass_options(
    directed: bool,
    via: Option<&str>,
    write_property: Option<&str>,
) -> ClusterOptions {
    ClusterOptions {
        by: ClusterAlgorithm::Spinglass,
        vector_property: None,
        via: via.map(str::to_owned),
        directed,
        write_property: write_property.map(str::to_owned),
    }
}

#[test]
fn louvain_obeys_uuid_partition_selection_and_writeback_contracts() {
    let graph = GraphForge::new(None).unwrap();
    graph
        .execute(
            "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (e:Person {name:'Eve'}), (f:Person {name:'Frank'}), (g:Person), \
                 (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(a), \
                 (b)-[:KNOWS]->(c), (c)-[:KNOWS]->(a), (a)-[:KNOWS]->(a), \
                 (c)-[:KNOWS]->(d), (d)-[:KNOWS]->(e), (e)-[:KNOWS]->(f), \
                 (f)-[:KNOWS]->(d), (f)-[:KNOWS]->(f), (a)-[:OTHER]->(g)",
        )
        .unwrap();

    let directed = graph
        .cluster("Person", louvain_options(true, Some("KNOWS"), None))
        .unwrap();
    assert_eq!(community_ids(&directed), [0, 0, 0, 1, 1, 1, 2]);
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
            .schema()
            .metadata()
            .get("graphforge.algorithm")
            .map(String::as_str),
        Some("louvain")
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
        [
            Some("Alice"),
            Some("Bob"),
            Some("Carol"),
            Some("Dan"),
            Some("Eve"),
            Some("Frank"),
            None,
        ]
    );
    assert_eq!(
        directed,
        graph
            .cluster("Person", louvain_options(false, Some("KNOWS"), None))
            .unwrap()
    );
    assert_eq!(
        community_ids(
            &graph
                .cluster("Person", louvain_options(true, None, None))
                .unwrap()
        ),
        [0, 0, 0, 1, 1, 1, 0]
    );
    assert_eq!(
        graph
            .execute("MATCH (n:Person) WHERE n.group_id IS NOT NULL RETURN n.node_uuid")
            .unwrap()
            .stats
            .rows_produced,
        0
    );
    let written = graph
        .cluster(
            "Person",
            louvain_options(true, Some("KNOWS"), Some("group_id")),
        )
        .unwrap();
    assert_eq!(community_ids(&written), [0, 0, 0, 1, 1, 1, 2]);
    let readback = graph
        .execute("MATCH (n:Person) RETURN n.group_id AS id ORDER BY id, n.name")
        .unwrap();
    assert_eq!(
        readback.batches[0]
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[0, 0, 0, 1, 1, 1, 2]
    );

    let edgeless = GraphForge::new(None).unwrap();
    edgeless
        .execute("CREATE (:Person), (:Person), (:Person)")
        .unwrap();
    assert_eq!(
        community_ids(
            &edgeless
                .cluster("Person", louvain_options(true, None, None))
                .unwrap()
        ),
        [0, 1, 2]
    );
    assert_eq!(
        GraphForge::new(None)
            .unwrap()
            .cluster("Person", louvain_options(true, None, None))
            .unwrap()
            .num_rows(),
        0
    );
}

#[test]
fn leiden_obeys_uuid_refinement_selection_and_writeback_contracts() {
    let graph = GraphForge::new(None).unwrap();
    graph
        .execute(
            "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), \
                 (c:Person {name:'C'}), (d:Person {name:'D'}), \
                 (e:Person {name:'E'}), (f:Person {name:'F'}), \
                 (g:Person {name:'G'}), (h:Person {name:'H'}), \
                 (a)-[:KNOWS]->(e), (a)-[:KNOWS]->(e), (e)-[:KNOWS]->(a), \
                 (a)-[:KNOWS]->(g), (b)-[:KNOWS]->(c), (b)-[:KNOWS]->(f), \
                 (b)-[:KNOWS]->(g), (c)-[:KNOWS]->(g), (d)-[:KNOWS]->(g), \
                 (e)-[:KNOWS]->(g), (f)-[:KNOWS]->(g), (a)-[:KNOWS]->(a), \
                 (a)-[:OTHER]->(h)",
        )
        .unwrap();

    let result = graph
        .cluster("Person", leiden_options(true, Some("KNOWS"), None))
        .unwrap();
    assert_eq!(community_ids(&result), [0, 1, 1, 0, 0, 1, 0, 2]);
    assert_eq!(
        result
            .schema()
            .fields()
            .iter()
            .map(|field| field.name())
            .collect::<Vec<_>>(),
        ["node_uuid", "community_id", "name"]
    );
    assert_eq!(
        result.schema().field(0).data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert_eq!(result.schema().field(1).data_type(), &DataType::Int64);
    assert!(result.column_by_name("node_id").is_none());
    assert_eq!(
        result
            .schema()
            .metadata()
            .get("graphforge.algorithm")
            .map(String::as_str),
        Some("leiden")
    );
    assert_eq!(
        result
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [
            Some("A"),
            Some("B"),
            Some("C"),
            Some("D"),
            Some("E"),
            Some("F"),
            Some("G"),
            Some("H"),
        ]
    );
    assert_eq!(
        result,
        graph
            .cluster("Person", leiden_options(false, Some("KNOWS"), None))
            .unwrap()
    );
    assert_eq!(
        result,
        graph
            .cluster("Person", leiden_options(true, Some("KNOWS"), None))
            .unwrap()
    );
    assert_ne!(
        community_ids(&result),
        community_ids(
            &graph
                .cluster("Person", louvain_options(true, Some("KNOWS"), None))
                .unwrap()
        )
    );
    assert_eq!(
        graph
            .execute("MATCH (n:Person) WHERE n.group_id IS NOT NULL RETURN n.node_uuid")
            .unwrap()
            .stats
            .rows_produced,
        0
    );
    let written = graph
        .cluster(
            "Person",
            leiden_options(true, Some("KNOWS"), Some("group_id")),
        )
        .unwrap();
    assert_eq!(community_ids(&written), [0, 1, 1, 0, 0, 1, 0, 2]);
    let readback = graph
        .execute("MATCH (n:Person) RETURN n.group_id AS id ORDER BY n.name")
        .unwrap();
    assert_eq!(
        readback.batches[0]
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[0, 1, 1, 0, 0, 1, 0, 2]
    );

    let edgeless = GraphForge::new(None).unwrap();
    edgeless
        .execute("CREATE (:Person), (:Person), (:Person)")
        .unwrap();
    assert_eq!(
        community_ids(
            &edgeless
                .cluster("Person", leiden_options(true, None, None))
                .unwrap()
        ),
        [0, 1, 2]
    );
    assert_eq!(
        GraphForge::new(None)
            .unwrap()
            .cluster("Person", leiden_options(true, None, None))
            .unwrap()
            .num_rows(),
        0
    );
}

#[test]
fn label_propagation_obeys_uuid_selection_and_writeback_contracts() {
    let graph = GraphForge::new(None).unwrap();
    graph
        .execute(
            "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), \
                 (c:Person {name:'C'}), (d:Person {name:'D'}), \
                 (e:Person {name:'E'}), (f:Person {name:'F'}), \
                 (g:Person {name:'G'}), (a)-[:KNOWS]->(b), \
                 (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), \
                 (c)-[:KNOWS]->(a), (a)-[:KNOWS]->(a), (d)-[:KNOWS]->(e), \
                 (e)-[:KNOWS]->(f), (f)-[:KNOWS]->(d), (f)-[:KNOWS]->(f), \
                 (c)-[:OTHER]->(d)",
        )
        .unwrap();

    let result = graph
        .cluster(
            "Person",
            label_propagation_options(true, Some("KNOWS"), None),
        )
        .unwrap();
    assert_eq!(community_ids(&result), [0, 0, 0, 1, 1, 1, 2]);
    assert_eq!(
        result
            .schema()
            .fields()
            .iter()
            .map(|field| field.name())
            .collect::<Vec<_>>(),
        ["node_uuid", "community_id", "name"]
    );
    assert_eq!(
        result.schema().field(0).data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert_eq!(result.schema().field(1).data_type(), &DataType::Int64);
    assert!(result.column_by_name("node_id").is_none());
    assert_eq!(
        result
            .schema()
            .metadata()
            .get("graphforge.algorithm")
            .map(String::as_str),
        Some("label_propagation")
    );
    assert_eq!(
        result
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [
            Some("A"),
            Some("B"),
            Some("C"),
            Some("D"),
            Some("E"),
            Some("F"),
            Some("G"),
        ]
    );
    assert_eq!(
        result,
        graph
            .cluster(
                "Person",
                label_propagation_options(false, Some("KNOWS"), None),
            )
            .unwrap()
    );
    assert_eq!(
        result,
        graph
            .cluster(
                "Person",
                label_propagation_options(true, Some("KNOWS"), None),
            )
            .unwrap()
    );
    assert_eq!(
        graph
            .execute("MATCH (n:Person) WHERE n.group_id IS NOT NULL RETURN n.node_uuid")
            .unwrap()
            .stats
            .rows_produced,
        0
    );
    let written = graph
        .cluster(
            "Person",
            label_propagation_options(true, Some("KNOWS"), Some("group_id")),
        )
        .unwrap();
    assert_eq!(community_ids(&written), [0, 0, 0, 1, 1, 1, 2]);
    let readback = graph
        .execute("MATCH (n:Person) RETURN n.group_id AS id ORDER BY n.name")
        .unwrap();
    assert_eq!(
        readback.batches[0]
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[0, 0, 0, 1, 1, 1, 2]
    );

    let edgeless = GraphForge::new(None).unwrap();
    edgeless
        .execute("CREATE (:Person), (:Person), (:Person)")
        .unwrap();
    assert_eq!(
        community_ids(
            &edgeless
                .cluster("Person", label_propagation_options(true, None, None))
                .unwrap()
        ),
        [0, 1, 2]
    );
    assert_eq!(
        GraphForge::new(None)
            .unwrap()
            .cluster("Person", label_propagation_options(true, None, None))
            .unwrap()
            .num_rows(),
        0
    );
}

#[test]
fn speaker_listener_obeys_uuid_selection_and_writeback_contracts() {
    // Exploratory mode has no ontology/knowledge layer; graph-native output must not depend on it.
    let graph = GraphForge::new(None).unwrap();
    graph
        .execute(
            "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), \
                 (c:Person {name:'C'}), (d:Person {name:'D'}), \
                 (e:Person {name:'E'}), (f:Person {name:'F'}), \
                 (g:Person {name:'G'}), (a)-[:KNOWS]->(b), \
                 (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), \
                 (c)-[:KNOWS]->(a), (a)-[:KNOWS]->(a), (d)-[:KNOWS]->(e), \
                 (e)-[:KNOWS]->(f), (f)-[:KNOWS]->(d), (f)-[:KNOWS]->(f), \
                 (c)-[:OTHER]->(d)",
        )
        .unwrap();

    let result = graph
        .cluster(
            "Person",
            speaker_listener_options(true, Some("KNOWS"), None),
        )
        .unwrap();
    let expected = [0, 0, 0, 1, 1, 1, 2];
    assert_eq!(community_ids(&result), expected);
    assert_eq!(
        result
            .schema()
            .fields()
            .iter()
            .map(|field| field.name())
            .collect::<Vec<_>>(),
        ["node_uuid", "community_id", "name"]
    );
    assert_eq!(
        result.schema().field(0).data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert_eq!(result.schema().field(1).data_type(), &DataType::Int64);
    assert!(result.column_by_name("node_id").is_none());
    assert_eq!(
        result
            .schema()
            .metadata()
            .get("graphforge.algorithm")
            .map(String::as_str),
        Some("speaker_listener")
    );
    assert_eq!(
        result
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [
            Some("A"),
            Some("B"),
            Some("C"),
            Some("D"),
            Some("E"),
            Some("F"),
            Some("G"),
        ]
    );
    assert_eq!(
        result,
        graph
            .cluster(
                "Person",
                speaker_listener_options(false, Some("KNOWS"), None),
            )
            .unwrap()
    );
    assert_eq!(
        result,
        graph
            .cluster(
                "Person",
                speaker_listener_options(true, Some("KNOWS"), None),
            )
            .unwrap()
    );
    assert_eq!(
        graph
            .execute("MATCH (n:Person) WHERE n.slpa_group IS NOT NULL RETURN n.node_uuid")
            .unwrap()
            .stats
            .rows_produced,
        0
    );
    let written = graph
        .cluster(
            "Person",
            speaker_listener_options(true, Some("KNOWS"), Some("slpa_group")),
        )
        .unwrap();
    assert_eq!(community_ids(&written), expected);
    let readback = graph
        .execute("MATCH (n:Person) RETURN n.slpa_group AS id ORDER BY n.name")
        .unwrap();
    assert_eq!(
        readback.batches[0]
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &expected
    );

    let edgeless = GraphForge::new(None).unwrap();
    edgeless
        .execute("CREATE (:Person), (:Person), (:Person)")
        .unwrap();
    assert_eq!(
        community_ids(
            &edgeless
                .cluster("Person", speaker_listener_options(true, None, None))
                .unwrap()
        ),
        [0, 1, 2]
    );
    assert_eq!(
        GraphForge::new(None)
            .unwrap()
            .cluster("Person", speaker_listener_options(true, None, None))
            .unwrap()
            .num_rows(),
        0
    );
}

#[test]
fn girvan_newman_obeys_uuid_selection_and_writeback_contracts() {
    // Exploratory mode has no ontology/knowledge layer; graph-native output must not depend on it.
    let graph = GraphForge::new(None).unwrap();
    graph
        .execute(
            "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), \
                 (c:Person {name:'C'}), (d:Person {name:'D'}), \
                 (e:Person {name:'E'}), (f:Person {name:'F'}), \
                 (g:Person {name:'G'}), (a)-[:KNOWS]->(b), \
                 (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), \
                 (c)-[:KNOWS]->(a), (a)-[:KNOWS]->(a), (c)-[:KNOWS]->(d), \
                 (d)-[:KNOWS]->(e), (e)-[:KNOWS]->(f), (f)-[:KNOWS]->(d), \
                 (f)-[:KNOWS]->(f), (a)-[:OTHER]->(e)",
        )
        .unwrap();

    let result = graph
        .cluster("Person", girvan_newman_options(true, Some("KNOWS"), None))
        .unwrap();
    let expected = [0, 0, 0, 1, 1, 1, 2];
    assert_eq!(community_ids(&result), expected);
    assert_eq!(
        result
            .schema()
            .fields()
            .iter()
            .map(|field| field.name())
            .collect::<Vec<_>>(),
        ["node_uuid", "community_id", "name"]
    );
    assert_eq!(
        result.schema().field(0).data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert_eq!(result.schema().field(1).data_type(), &DataType::Int64);
    assert!(result.column_by_name("node_id").is_none());
    assert_eq!(
        result
            .schema()
            .metadata()
            .get("graphforge.algorithm")
            .map(String::as_str),
        Some("girvan_newman")
    );
    assert_eq!(
        result
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [
            Some("A"),
            Some("B"),
            Some("C"),
            Some("D"),
            Some("E"),
            Some("F"),
            Some("G")
        ]
    );
    assert_eq!(
        result,
        graph
            .cluster("Person", girvan_newman_options(false, Some("KNOWS"), None))
            .unwrap()
    );
    assert_eq!(
        result,
        graph
            .cluster("Person", girvan_newman_options(true, Some("KNOWS"), None))
            .unwrap()
    );
    assert_eq!(
        graph
            .execute("MATCH (n:Person) WHERE n.gn_group IS NOT NULL RETURN n.node_uuid")
            .unwrap()
            .stats
            .rows_produced,
        0
    );
    let written = graph
        .cluster(
            "Person",
            girvan_newman_options(true, Some("KNOWS"), Some("gn_group")),
        )
        .unwrap();
    assert_eq!(community_ids(&written), expected);
    let readback = graph
        .execute("MATCH (n:Person) RETURN n.gn_group AS id ORDER BY n.name")
        .unwrap();
    assert_eq!(
        readback.batches[0]
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &expected
    );

    let edgeless = GraphForge::new(None).unwrap();
    edgeless
        .execute("CREATE (:Person), (:Person), (:Person)")
        .unwrap();
    assert_eq!(
        community_ids(
            &edgeless
                .cluster("Person", girvan_newman_options(true, None, None))
                .unwrap()
        ),
        [0, 1, 2]
    );
    assert_eq!(
        GraphForge::new(None)
            .unwrap()
            .cluster("Person", girvan_newman_options(true, None, None))
            .unwrap()
            .num_rows(),
        0
    );
}

#[test]
fn modularity_optimization_obeys_uuid_selection_and_writeback_contracts() {
    // Exploratory mode has no ontology/knowledge layer; graph-native output must not depend on it.
    let graph = GraphForge::new(None).unwrap();
    graph
        .execute(
            "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), \
                 (c:Person {name:'C'}), (d:Person {name:'D'}), \
                 (e:Person {name:'E'}), (f:Person {name:'F'}), \
                 (g:Person {name:'G'}), (a)-[:KNOWS]->(b), \
                 (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), \
                 (c)-[:KNOWS]->(a), (a)-[:KNOWS]->(a), (c)-[:KNOWS]->(d), \
                 (d)-[:KNOWS]->(e), (e)-[:KNOWS]->(f), (f)-[:KNOWS]->(d), \
                 (f)-[:KNOWS]->(f), (a)-[:OTHER]->(e)",
        )
        .unwrap();

    let options = modularity_optimization_options(true, Some("KNOWS"), None);
    let result = graph.cluster("Person", options.clone()).unwrap();
    let expected = [0, 0, 0, 1, 1, 1, 2];
    assert_eq!(community_ids(&result), expected);
    assert_eq!(
        result
            .schema()
            .fields()
            .iter()
            .map(|field| field.name())
            .collect::<Vec<_>>(),
        ["node_uuid", "community_id", "name"]
    );
    assert_eq!(
        result.schema().field(0).data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert_eq!(result.schema().field(1).data_type(), &DataType::Int64);
    assert!(result.column_by_name("node_id").is_none());
    assert_eq!(
        result
            .schema()
            .metadata()
            .get("graphforge.algorithm")
            .map(String::as_str),
        Some("modularity_optimization")
    );
    assert_eq!(
        result
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [
            Some("A"),
            Some("B"),
            Some("C"),
            Some("D"),
            Some("E"),
            Some("F"),
            Some("G")
        ]
    );
    assert_eq!(result, graph.cluster("Person", options).unwrap());
    assert_eq!(
        result,
        graph
            .cluster(
                "Person",
                modularity_optimization_options(false, Some("KNOWS"), None),
            )
            .unwrap()
    );
    assert_eq!(
        graph
            .execute("MATCH (n:Person) WHERE n.mod_group IS NOT NULL RETURN n.node_uuid")
            .unwrap()
            .stats
            .rows_produced,
        0
    );
    let written = graph
        .cluster(
            "Person",
            modularity_optimization_options(true, Some("KNOWS"), Some("mod_group")),
        )
        .unwrap();
    assert_eq!(community_ids(&written), expected);
    let readback = graph
        .execute("MATCH (n:Person) RETURN n.mod_group AS id ORDER BY n.name")
        .unwrap();
    assert_eq!(
        readback.batches[0]
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &expected
    );

    let edgeless = GraphForge::new(None).unwrap();
    edgeless
        .execute("CREATE (:Person), (:Person), (:Person)")
        .unwrap();
    assert_eq!(
        community_ids(
            &edgeless
                .cluster("Person", modularity_optimization_options(true, None, None),)
                .unwrap()
        ),
        [0, 1, 2]
    );
    assert_eq!(
        GraphForge::new(None)
            .unwrap()
            .cluster("Person", modularity_optimization_options(true, None, None),)
            .unwrap()
            .num_rows(),
        0
    );
}

#[test]
fn fastgreedy_obeys_uuid_selection_and_writeback_contracts() {
    // Exploratory mode has no ontology/knowledge layer; graph-native output must not depend on it.
    let graph = GraphForge::new(None).unwrap();
    graph
        .execute(
            "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), \
                 (c:Person {name:'C'}), (d:Person {name:'D'}), \
                 (e:Person {name:'E'}), (f:Person {name:'F'}), \
                 (g:Person {name:'G'}), (a)-[:KNOWS]->(b), \
                 (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), \
                 (c)-[:KNOWS]->(a), (a)-[:KNOWS]->(a), (c)-[:KNOWS]->(d), \
                 (d)-[:KNOWS]->(e), (e)-[:KNOWS]->(f), (f)-[:KNOWS]->(d), \
                 (f)-[:KNOWS]->(f), (a)-[:OTHER]->(e)",
        )
        .unwrap();

    let options = fastgreedy_options(true, Some("KNOWS"), None);
    let result = graph.cluster("Person", options.clone()).unwrap();
    let expected = [0, 0, 0, 1, 1, 1, 2];
    assert_eq!(community_ids(&result), expected);
    assert_eq!(
        result
            .schema()
            .fields()
            .iter()
            .map(|field| field.name())
            .collect::<Vec<_>>(),
        ["node_uuid", "community_id", "name"]
    );
    assert_eq!(
        result.schema().field(0).data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert_eq!(result.schema().field(1).data_type(), &DataType::Int64);
    assert!(result.column_by_name("node_id").is_none());
    assert_eq!(
        result
            .schema()
            .metadata()
            .get("graphforge.algorithm")
            .map(String::as_str),
        Some("fastgreedy")
    );
    assert_eq!(
        result
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [
            Some("A"),
            Some("B"),
            Some("C"),
            Some("D"),
            Some("E"),
            Some("F"),
            Some("G")
        ]
    );
    assert_eq!(result, graph.cluster("Person", options).unwrap());
    assert_eq!(
        result,
        graph
            .cluster("Person", fastgreedy_options(false, Some("KNOWS"), None),)
            .unwrap()
    );
    assert_eq!(
        graph
            .execute("MATCH (n:Person) WHERE n.fast_group IS NOT NULL RETURN n.node_uuid")
            .unwrap()
            .stats
            .rows_produced,
        0
    );
    let written = graph
        .cluster(
            "Person",
            fastgreedy_options(true, Some("KNOWS"), Some("fast_group")),
        )
        .unwrap();
    assert_eq!(community_ids(&written), expected);
    let readback = graph
        .execute("MATCH (n:Person) RETURN n.fast_group AS id ORDER BY n.name")
        .unwrap();
    assert_eq!(
        readback.batches[0]
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &expected
    );

    let edgeless = GraphForge::new(None).unwrap();
    edgeless
        .execute("CREATE (:Person), (:Person), (:Person)")
        .unwrap();
    assert_eq!(
        community_ids(
            &edgeless
                .cluster("Person", fastgreedy_options(true, None, None))
                .unwrap()
        ),
        [0, 1, 2]
    );
    assert_eq!(
        GraphForge::new(None)
            .unwrap()
            .cluster("Person", fastgreedy_options(true, None, None))
            .unwrap()
            .num_rows(),
        0
    );
}

#[test]
fn infomap_obeys_uuid_selection_flow_and_writeback_contracts() {
    // Exploratory mode has no ontology/knowledge layer (#772).
    let graph = GraphForge::new(None).unwrap();
    graph
        .execute(
            "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), \
                 (c:Person {name:'C'}), (d:Person {name:'D'}), \
                 (e:Person {name:'E'}), (a)-[:KNOWS]->(b), \
                 (b)-[:KNOWS]->(a), (c)-[:KNOWS]->(d), \
                 (d)-[:KNOWS]->(c), (b)-[:OTHER]->(c)",
        )
        .unwrap();

    let options = infomap_options(true, Some("KNOWS"), None);
    let result = graph.cluster("Person", options.clone()).unwrap();
    let expected = [0, 0, 1, 1, 2];
    assert_eq!(community_ids(&result), expected);
    assert_eq!(result, graph.cluster("Person", options).unwrap());
    assert_eq!(
        result,
        graph
            .cluster("Person", infomap_options(false, Some("KNOWS"), None))
            .unwrap()
    );
    assert_eq!(
        result
            .schema()
            .fields()
            .iter()
            .map(|field| field.name())
            .collect::<Vec<_>>(),
        ["node_uuid", "community_id", "name"]
    );
    assert_eq!(
        result.schema().field(0).data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert_eq!(result.schema().field(1).data_type(), &DataType::Int64);
    assert!(result.column_by_name("node_id").is_none());
    assert_eq!(
        result.schema().metadata()["graphforge.algorithm"],
        "infomap"
    );
    assert_eq!(
        result
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [Some("A"), Some("B"), Some("C"), Some("D"), Some("E")]
    );
    assert_eq!(
        graph
            .execute("MATCH (n:Person) WHERE n.flow_group IS NOT NULL RETURN n.node_uuid")
            .unwrap()
            .stats
            .rows_produced,
        0
    );
    let written = graph
        .cluster(
            "Person",
            infomap_options(true, Some("KNOWS"), Some("flow_group")),
        )
        .unwrap();
    assert_eq!(community_ids(&written), expected);
    let readback = graph
        .execute("MATCH (n:Person) RETURN n.flow_group AS id ORDER BY n.name")
        .unwrap();
    assert_eq!(
        readback.batches[0]
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &expected
    );

    let edgeless = GraphForge::new(None).unwrap();
    edgeless
        .execute("CREATE (:Person), (:Person), (:Person)")
        .unwrap();
    assert_eq!(
        community_ids(
            &edgeless
                .cluster("Person", infomap_options(true, None, None))
                .unwrap()
        ),
        [0, 1, 2]
    );
    assert_eq!(
        GraphForge::new(None)
            .unwrap()
            .cluster("Person", infomap_options(true, None, None))
            .unwrap()
            .num_rows(),
        0
    );
}

#[test]
fn leading_eigenvector_obeys_uuid_spectral_and_writeback_contracts() {
    // Exploratory mode has no ontology/knowledge layer (#772).
    let graph = GraphForge::new(None).unwrap();
    graph
        .execute(
            "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), \
                 (c:Person {name:'C'}), (d:Person {name:'D'}), \
                 (e:Person {name:'E'}), (f:Person {name:'F'}), \
                 (g:Person {name:'G'}), (a)-[:KNOWS]->(b), \
                 (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), \
                 (c)-[:KNOWS]->(a), (a)-[:KNOWS]->(a), (c)-[:KNOWS]->(d), \
                 (d)-[:KNOWS]->(e), (e)-[:KNOWS]->(f), (f)-[:KNOWS]->(d), \
                 (a)-[:OTHER]->(e)",
        )
        .unwrap();

    let options = leading_eigenvector_options(true, Some("KNOWS"), None);
    let result = graph.cluster("Person", options.clone()).unwrap();
    let expected = [0, 0, 0, 1, 1, 1, 2];
    assert_eq!(community_ids(&result), expected);
    assert_eq!(result, graph.cluster("Person", options).unwrap());
    assert_eq!(
        result,
        graph
            .cluster(
                "Person",
                leading_eigenvector_options(false, Some("KNOWS"), None),
            )
            .unwrap()
    );
    assert_eq!(
        result
            .schema()
            .fields()
            .iter()
            .map(|field| field.name())
            .collect::<Vec<_>>(),
        ["node_uuid", "community_id", "name"]
    );
    assert_eq!(
        result.schema().field(0).data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert_eq!(result.schema().field(1).data_type(), &DataType::Int64);
    assert!(result.column_by_name("node_id").is_none());
    assert_eq!(
        result.schema().metadata()["graphforge.algorithm"],
        "leading_eigenvector"
    );
    assert_eq!(
        result
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [
            Some("A"),
            Some("B"),
            Some("C"),
            Some("D"),
            Some("E"),
            Some("F"),
            Some("G")
        ]
    );
    assert_eq!(
        graph
            .execute("MATCH (n:Person) WHERE n.spectral_group IS NOT NULL RETURN n.node_uuid")
            .unwrap()
            .stats
            .rows_produced,
        0
    );
    graph
        .execute("MATCH (n:Person {name:'A'}) SET n.spectral_atomic = 'old'")
        .unwrap();
    assert!(matches!(
        graph.cluster(
            "Person",
            leading_eigenvector_options(true, Some("KNOWS"), Some("spectral_atomic"),),
        ),
        Err(GfError::Validation(_))
    ));
    let unchanged = graph
        .execute(
            "MATCH (n:Person) WHERE n.spectral_atomic IS NOT NULL \
                 RETURN n.name AS name, n.spectral_atomic AS value",
        )
        .unwrap();
    assert_eq!(unchanged.stats.rows_produced, 1);
    for (column, expected) in [("name", "A"), ("value", "old")] {
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
    let written = graph
        .cluster(
            "Person",
            leading_eigenvector_options(true, Some("KNOWS"), Some("spectral_group")),
        )
        .unwrap();
    assert_eq!(community_ids(&written), expected);
    let readback = graph
        .execute("MATCH (n:Person) RETURN n.spectral_group AS id ORDER BY n.name")
        .unwrap();
    assert_eq!(
        readback.batches[0]
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &expected
    );

    let edgeless = GraphForge::new(None).unwrap();
    edgeless
        .execute("CREATE (:Person), (:Person), (:Person)")
        .unwrap();
    assert_eq!(
        community_ids(
            &edgeless
                .cluster("Person", leading_eigenvector_options(true, None, None))
                .unwrap()
        ),
        [0, 1, 2]
    );
    assert_eq!(
        GraphForge::new(None)
            .unwrap()
            .cluster("Person", leading_eigenvector_options(true, None, None))
            .unwrap()
            .num_rows(),
        0
    );
}

#[test]
fn walktrap_obeys_uuid_partition_and_atomic_writeback_contracts() {
    // Exploratory mode has no ontology/knowledge layer (#772).
    let graph = GraphForge::new(None).unwrap();
    graph
        .execute(
            "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), \
                 (c:Person {name:'C'}), (d:Person {name:'D'}), \
                 (e:Person {name:'E'}), (f:Person {name:'F'}), \
                 (g:Person {name:'G'}), (a)-[:KNOWS]->(b), \
                 (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), \
                 (c)-[:KNOWS]->(a), (a)-[:KNOWS]->(a), (c)-[:KNOWS]->(d), \
                 (d)-[:KNOWS]->(e), (e)-[:KNOWS]->(f), (f)-[:KNOWS]->(d), \
                 (a)-[:OTHER]->(e)",
        )
        .unwrap();

    let options = walktrap_options(true, Some("KNOWS"), None);
    let result = graph.cluster("Person", options.clone()).unwrap();
    let expected = [0, 0, 0, 1, 1, 1, 2];
    assert_eq!(community_ids(&result), expected);
    assert_eq!(result, graph.cluster("Person", options).unwrap());
    assert_eq!(
        result,
        graph
            .cluster("Person", walktrap_options(false, Some("KNOWS"), None))
            .unwrap()
    );
    assert_eq!(
        result
            .schema()
            .fields()
            .iter()
            .map(|field| field.name())
            .collect::<Vec<_>>(),
        ["node_uuid", "community_id", "name"]
    );
    assert_eq!(
        result.schema().field(0).data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert_eq!(result.schema().field(1).data_type(), &DataType::Int64);
    assert!(result.column_by_name("node_id").is_none());
    assert_eq!(
        result.schema().metadata()["graphforge.algorithm"],
        "walktrap"
    );
    assert_eq!(
        result
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [
            Some("A"),
            Some("B"),
            Some("C"),
            Some("D"),
            Some("E"),
            Some("F"),
            Some("G")
        ]
    );
    assert_eq!(
        graph
            .execute("MATCH (n:Person) WHERE n.walktrap_group IS NOT NULL RETURN n.node_uuid")
            .unwrap()
            .stats
            .rows_produced,
        0
    );

    graph
        .execute("MATCH (n:Person {name:'A'}) SET n.walktrap_atomic = 'old'")
        .unwrap();
    assert!(matches!(
        graph.cluster(
            "Person",
            walktrap_options(true, Some("KNOWS"), Some("walktrap_atomic")),
        ),
        Err(GfError::Validation(_))
    ));
    let unchanged = graph
        .execute(
            "MATCH (n:Person) WHERE n.walktrap_atomic IS NOT NULL \
                 RETURN n.name AS name, n.walktrap_atomic AS value",
        )
        .unwrap();
    assert_eq!(unchanged.stats.rows_produced, 1);
    for (column, expected) in [("name", "A"), ("value", "old")] {
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

    let written = graph
        .cluster(
            "Person",
            walktrap_options(true, Some("KNOWS"), Some("walktrap_group")),
        )
        .unwrap();
    assert_eq!(community_ids(&written), expected);
    let readback = graph
        .execute("MATCH (n:Person) RETURN n.walktrap_group AS id ORDER BY n.name")
        .unwrap();
    assert_eq!(
        readback.batches[0]
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &expected
    );

    let edgeless = GraphForge::new(None).unwrap();
    edgeless
        .execute("CREATE (:Person), (:Person), (:Person)")
        .unwrap();
    assert_eq!(
        community_ids(
            &edgeless
                .cluster("Person", walktrap_options(true, None, None))
                .unwrap()
        ),
        [0, 1, 2]
    );
    assert_eq!(
        GraphForge::new(None)
            .unwrap()
            .cluster("Person", walktrap_options(true, None, None))
            .unwrap()
            .num_rows(),
        0
    );
}

#[test]
fn spinglass_obeys_uuid_partition_and_atomic_writeback_contracts() {
    // Exploratory mode has no ontology/knowledge layer (#772).
    let graph = GraphForge::new(None).unwrap();
    graph
        .execute(
            "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), \
                 (c:Person {name:'C'}), (d:Person {name:'D'}), \
                 (e:Person {name:'E'}), (f:Person {name:'F'}), \
                 (g:Person {name:'G'}), (a)-[:KNOWS]->(b), \
                 (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), \
                 (c)-[:KNOWS]->(a), (a)-[:KNOWS]->(a), (c)-[:KNOWS]->(d), \
                 (d)-[:KNOWS]->(e), (e)-[:KNOWS]->(f), (f)-[:KNOWS]->(d), \
                 (a)-[:OTHER]->(g), (b)-[:OTHER]->(g), (c)-[:OTHER]->(g)",
        )
        .unwrap();

    let options = spinglass_options(true, Some("KNOWS"), None);
    let result = graph.cluster("Person", options.clone()).unwrap();
    let expected = [0, 0, 0, 1, 1, 1, 2];
    assert_eq!(community_ids(&result), expected);
    assert_eq!(result, graph.cluster("Person", options).unwrap());
    assert_ne!(
        community_ids(&result),
        community_ids(
            &graph
                .cluster("Person", spinglass_options(true, None, None))
                .unwrap()
        )
    );
    assert_eq!(
        result,
        graph
            .cluster("Person", spinglass_options(false, Some("KNOWS"), None))
            .unwrap()
    );
    assert_eq!(
        result
            .schema()
            .fields()
            .iter()
            .map(|field| field.name())
            .collect::<Vec<_>>(),
        ["node_uuid", "community_id", "name"]
    );
    assert_eq!(
        result.schema().field(0).data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert_eq!(result.schema().field(1).data_type(), &DataType::Int64);
    assert!(result.column_by_name("node_id").is_none());
    assert_eq!(
        result.schema().metadata()["graphforge.algorithm"],
        "spinglass"
    );
    assert_eq!(
        result
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [
            Some("A"),
            Some("B"),
            Some("C"),
            Some("D"),
            Some("E"),
            Some("F"),
            Some("G")
        ]
    );
    assert_eq!(
        graph
            .execute("MATCH (n:Person) WHERE n.spin_group IS NOT NULL RETURN n.node_uuid")
            .unwrap()
            .stats
            .rows_produced,
        0
    );

    graph
        .execute("MATCH (n:Person {name:'A'}) SET n.spin_atomic = 'old'")
        .unwrap();
    assert!(matches!(
        graph.cluster(
            "Person",
            spinglass_options(true, Some("KNOWS"), Some("spin_atomic")),
        ),
        Err(GfError::Validation(_))
    ));
    let unchanged = graph
        .execute(
            "MATCH (n:Person) WHERE n.spin_atomic IS NOT NULL \
                 RETURN n.name AS name, n.spin_atomic AS value",
        )
        .unwrap();
    assert_eq!(unchanged.stats.rows_produced, 1);
    for (column, expected) in [("name", "A"), ("value", "old")] {
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

    let written = graph
        .cluster(
            "Person",
            spinglass_options(true, Some("KNOWS"), Some("spin_group")),
        )
        .unwrap();
    assert_eq!(community_ids(&written), expected);
    let readback = graph
        .execute("MATCH (n:Person) RETURN n.spin_group AS id ORDER BY n.name")
        .unwrap();
    assert_eq!(
        readback.batches[0]
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &expected
    );

    let edgeless = GraphForge::new(None).unwrap();
    edgeless
        .execute("CREATE (:Person), (:Person), (:Person)")
        .unwrap();
    assert_eq!(
        community_ids(
            &edgeless
                .cluster("Person", spinglass_options(true, None, None))
                .unwrap()
        ),
        [0, 1, 2]
    );
    let disconnected = GraphForge::new(None).unwrap();
    disconnected
        .execute(
            "CREATE (a:Person), (b:Person), (c:Person), (d:Person), \
                 (e:Person), (f:Person), (:Person), (a)-[:KNOWS]->(b), \
                 (b)-[:KNOWS]->(c), (c)-[:KNOWS]->(a), (d)-[:KNOWS]->(e), \
                 (e)-[:KNOWS]->(f), (f)-[:KNOWS]->(d)",
        )
        .unwrap();
    assert_eq!(
        community_ids(
            &disconnected
                .cluster("Person", spinglass_options(true, None, None))
                .unwrap()
        ),
        [0, 0, 0, 1, 1, 1, 2]
    );
    assert_eq!(
        GraphForge::new(None)
            .unwrap()
            .cluster("Person", spinglass_options(true, None, None))
            .unwrap()
            .num_rows(),
        0
    );
}
