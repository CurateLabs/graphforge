use super::*;

fn minimum_spanning_tree_options(via: Option<&str>, weight: Option<&str>) -> AnalyzeOptions {
    AnalyzeOptions {
        by: AnalyzeAlgorithm::MinimumSpanningTree,
        via: via.map(str::to_owned),
        directed: false,
        weight: weight.map(str::to_owned),
        k: None,
        partition_property: None,
    }
}

fn maximum_spanning_tree_options(via: Option<&str>, weight: Option<&str>) -> AnalyzeOptions {
    AnalyzeOptions {
        by: AnalyzeAlgorithm::MaximumSpanningTree,
        via: via.map(str::to_owned),
        directed: false,
        weight: weight.map(str::to_owned),
        k: None,
        partition_property: None,
    }
}

fn articulation_points_options(via: Option<&str>) -> AnalyzeOptions {
    AnalyzeOptions {
        by: AnalyzeAlgorithm::ArticulationPoints,
        via: via.map(str::to_owned),
        directed: false,
        weight: None,
        k: None,
        partition_property: None,
    }
}

fn bridges_options(via: Option<&str>) -> AnalyzeOptions {
    AnalyzeOptions {
        by: AnalyzeAlgorithm::Bridges,
        via: via.map(str::to_owned),
        directed: false,
        weight: None,
        k: None,
        partition_property: None,
    }
}

fn triangle_count_options(via: Option<&str>) -> AnalyzeOptions {
    AnalyzeOptions {
        by: AnalyzeAlgorithm::TriangleCount,
        via: via.map(str::to_owned),
        directed: false,
        weight: None,
        k: None,
        partition_property: None,
    }
}

fn count_automorphisms_options(directed: bool, via: Option<&str>) -> AnalyzeOptions {
    AnalyzeOptions {
        by: AnalyzeAlgorithm::CountAutomorphisms,
        via: via.map(str::to_owned),
        directed,
        weight: None,
        k: None,
        partition_property: None,
    }
}

fn transitivity_options(via: Option<&str>) -> AnalyzeOptions {
    AnalyzeOptions {
        by: AnalyzeAlgorithm::Transitivity,
        via: via.map(str::to_owned),
        directed: false,
        weight: None,
        k: None,
        partition_property: None,
    }
}

fn is_planar_options(via: Option<&str>) -> AnalyzeOptions {
    AnalyzeOptions {
        by: AnalyzeAlgorithm::IsPlanar,
        via: via.map(str::to_owned),
        directed: false,
        weight: None,
        k: None,
        partition_property: None,
    }
}

fn triad_census_options(via: Option<&str>) -> AnalyzeOptions {
    AnalyzeOptions {
        by: AnalyzeAlgorithm::TriadCensus,
        via: via.map(str::to_owned),
        directed: true,
        weight: None,
        k: None,
        partition_property: None,
    }
}

fn dyad_census_options(via: Option<&str>) -> AnalyzeOptions {
    AnalyzeOptions {
        by: AnalyzeAlgorithm::DyadCensus,
        via: via.map(str::to_owned),
        directed: true,
        weight: None,
        k: None,
        partition_property: None,
    }
}

fn node_coloring_options(via: Option<&str>) -> AnalyzeOptions {
    AnalyzeOptions {
        by: AnalyzeAlgorithm::NodeColoring,
        via: via.map(str::to_owned),
        directed: false,
        weight: None,
        k: None,
        partition_property: None,
    }
}

fn k1_coloring_options(via: Option<&str>) -> AnalyzeOptions {
    AnalyzeOptions {
        by: AnalyzeAlgorithm::K1Coloring,
        via: via.map(str::to_owned),
        directed: false,
        weight: None,
        k: None,
        partition_property: None,
    }
}

fn chromatic_number_options(via: Option<&str>) -> AnalyzeOptions {
    AnalyzeOptions {
        by: AnalyzeAlgorithm::ChromaticNumber,
        via: via.map(str::to_owned),
        directed: false,
        weight: None,
        k: None,
        partition_property: None,
    }
}

fn find_cycles_options(directed: bool, via: Option<&str>) -> AnalyzeOptions {
    AnalyzeOptions {
        by: AnalyzeAlgorithm::FindCycles,
        via: via.map(str::to_owned),
        directed,
        weight: None,
        k: None,
        partition_property: None,
    }
}

fn node_coloring_rows(batch: &arrow::record_batch::RecordBatch) -> Vec<([u8; 16], u64)> {
    let nodes = batch
        .column_by_name("node_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let colors = batch
        .column_by_name("color")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    (0..batch.num_rows())
        .map(|row| (nodes.value(row).try_into().unwrap(), colors.value(row)))
        .collect()
}

#[test]
fn minimum_spanning_tree_is_uuid_only_weighted_and_knowledge_independent() {
    // Exploratory mode has no ontology/knowledge layer (#772).
    let graph = GraphForge::new(None).unwrap();
    let nodes = ["Alice", "Bob", "Carol", "Dan", "Eve", "Fox"].map(|name| add_person(&graph, name));
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (e:Person {name:'Eve'}), (f:Person {name:'Fox'}) \
                 CREATE (a)-[:ROAD {cost:4.0}]->(b), \
                 (a)-[:ROAD {cost:3.0}]->(c), (b)-[:ROAD {cost:1.0}]->(c), \
                 (b)-[:ROAD {cost:2.0}]->(d), (c)-[:ROAD {cost:4.0}]->(d), \
                 (e)-[:ROAD {cost:-2.0}]->(f), (e)-[:ROAD {cost:3.0}]->(f), \
                 (d)-[:ROAD {cost:-10.0}]->(d), \
                 (a)-[:OTHER {cost:-100.0}]->(d)",
        )
        .unwrap();
    let options = minimum_spanning_tree_options(Some("ROAD"), Some("cost"));
    let batch = graph.analyze(Some("Person"), options.clone()).unwrap();

    assert_eq!(
        batch.schema().metadata()["graphforge.algorithm"],
        "minimum_spanning_tree"
    );
    assert_eq!(batch.schema().metadata()["graphforge.verb"], "analyze");
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| (field.name().as_str(), field.data_type()))
            .collect::<Vec<_>>(),
        [
            ("edge_uuid", &DataType::FixedSizeBinary(16)),
            ("source_uuid", &DataType::FixedSizeBinary(16)),
            ("target_uuid", &DataType::FixedSizeBinary(16)),
            ("weight", &DataType::Float64),
        ]
    );
    assert!(batch.column_by_name("edge_id").is_none());
    assert_eq!(
        batch
            .column_by_name("weight")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values(),
        &[-2.0, 1.0, 2.0, 3.0]
    );
    assert_eq!(batch.column_by_name("weight").unwrap().null_count(), 0);

    let sources = batch
        .column_by_name("source_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let targets = batch
        .column_by_name("target_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    for (row, (left, right)) in [(4, 5), (1, 2), (1, 3), (0, 2)].into_iter().enumerate() {
        let mut expected = [*nodes[left].uuid.as_bytes(), *nodes[right].uuid.as_bytes()];
        expected.sort_unstable();
        assert_eq!(sources.value(row), expected[0]);
        assert_eq!(targets.value(row), expected[1]);
    }
    let edge_uuids = batch
        .column_by_name("edge_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let unique = (0..edge_uuids.len())
        .map(|row| edge_uuids.value(row))
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(unique.len(), 4);
    assert_eq!(batch, graph.analyze(Some("Person"), options).unwrap());
    assert_eq!(
        graph
            .analyze(
                Some("Missing"),
                minimum_spanning_tree_options(Some("ROAD"), Some("cost")),
            )
            .unwrap()
            .num_rows(),
        0
    );
}

#[test]
fn minimum_spanning_tree_defaults_to_unit_weight_with_stable_ties() {
    let graph = GraphForge::new(None).unwrap();
    let mut nodes = ["Alice", "Bob", "Carol"].map(|name| add_person(&graph, name));
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}) \
                 CREATE (a)-[:ROAD]->(b), (a)-[:ROAD]->(b), \
                 (a)-[:ROAD]->(c), (b)-[:ROAD]->(c)",
        )
        .unwrap();
    nodes.sort_unstable_by_key(|node| *node.uuid.as_bytes());
    let batch = graph
        .analyze(None, minimum_spanning_tree_options(Some("ROAD"), None))
        .unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(
        batch
            .column_by_name("weight")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values(),
        &[1.0, 1.0]
    );
    let sources = batch
        .column_by_name("source_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let targets = batch
        .column_by_name("target_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    for (row, target) in [1, 2].into_iter().enumerate() {
        assert_eq!(sources.value(row), nodes[0].uuid.as_bytes());
        assert_eq!(targets.value(row), nodes[target].uuid.as_bytes());
    }

    let empty = GraphForge::new(None).unwrap();
    let empty_batch = empty
        .analyze(None, minimum_spanning_tree_options(None, None))
        .unwrap();
    assert_eq!(empty_batch.num_rows(), 0);
    assert_eq!(empty_batch.schema(), batch.schema());
}

#[test]
fn minimum_spanning_tree_rejects_directed_and_strict_weight_errors() {
    let graph = GraphForge::new(None).unwrap();
    add_person(&graph, "Alice");
    add_person(&graph, "Bob");
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}) \
                 CREATE (a)-[:ROAD {null_cost:null, text_cost:'heavy', \
                 infinite_cost:1e308 * 2.0}]->(b)",
        )
        .unwrap();

    let mut directed = minimum_spanning_tree_options(Some("ROAD"), None);
    directed.directed = true;
    assert!(matches!(
        graph.analyze(None, directed),
        Err(GfError::Validation(message)) if message.contains("directed=false")
    ));
    for options in [
        minimum_spanning_tree_options(Some(" "), None),
        minimum_spanning_tree_options(Some("ROAD"), Some(" ")),
        minimum_spanning_tree_options(Some("ROAD"), Some("missing")),
        minimum_spanning_tree_options(Some("ROAD"), Some("null_cost")),
        minimum_spanning_tree_options(Some("ROAD"), Some("text_cost")),
        minimum_spanning_tree_options(Some("ROAD"), Some("infinite_cost")),
        AnalyzeOptions {
            weight: Some("cost".into()),
            ..AnalyzeOptions::default()
        },
    ] {
        assert!(matches!(
            graph.analyze(None, options),
            Err(GfError::Validation(_))
        ));
    }
}

#[test]
fn maximum_spanning_tree_is_uuid_only_weighted_and_knowledge_independent() {
    // Exploratory mode has no ontology/knowledge layer (#772).
    let graph = GraphForge::new(None).unwrap();
    let nodes = ["Alice", "Bob", "Carol", "Dan", "Eve", "Fox"].map(|name| add_person(&graph, name));
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (e:Person {name:'Eve'}), (f:Person {name:'Fox'}) \
                 CREATE (a)-[:ROAD {cost:4.0}]->(b), \
                 (a)-[:ROAD {cost:9.0}]->(b), (b)-[:ROAD {cost:8.0}]->(a), \
                 (a)-[:ROAD {cost:7.0}]->(c), (b)-[:ROAD {cost:6.0}]->(c), \
                 (b)-[:ROAD {cost:-3.0}]->(d), (c)-[:ROAD {cost:-1.0}]->(d), \
                 (e)-[:ROAD {cost:-5.0}]->(f), (e)-[:ROAD {cost:-2.0}]->(f), \
                 (d)-[:ROAD {cost:1e308}]->(d), \
                 (a)-[:OTHER {cost:100.0}]->(d)",
        )
        .unwrap();
    let options = maximum_spanning_tree_options(Some("ROAD"), Some("cost"));
    let batch = graph.analyze(Some("Person"), options.clone()).unwrap();

    assert_eq!(
        batch.schema().metadata()["graphforge.algorithm"],
        "maximum_spanning_tree"
    );
    assert_eq!(batch.schema().metadata()["graphforge.verb"], "analyze");
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
            ("edge_uuid", &DataType::FixedSizeBinary(16), false),
            ("source_uuid", &DataType::FixedSizeBinary(16), false),
            ("target_uuid", &DataType::FixedSizeBinary(16), false),
            ("weight", &DataType::Float64, true),
        ]
    );
    assert!(batch.column_by_name("edge_id").is_none());
    assert_eq!(
        batch
            .column_by_name("weight")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values(),
        &[9.0, 7.0, -1.0, -2.0]
    );
    assert_eq!(batch.column_by_name("weight").unwrap().null_count(), 0);

    let sources = batch
        .column_by_name("source_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let targets = batch
        .column_by_name("target_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    for (row, (left, right)) in [(0, 1), (0, 2), (2, 3), (4, 5)].into_iter().enumerate() {
        let mut expected = [*nodes[left].uuid.as_bytes(), *nodes[right].uuid.as_bytes()];
        expected.sort_unstable();
        assert_eq!(sources.value(row), expected[0]);
        assert_eq!(targets.value(row), expected[1]);
    }
    assert_eq!(batch, graph.analyze(Some("Person"), options).unwrap());
    assert_eq!(
        graph
            .analyze(
                Some("Missing"),
                maximum_spanning_tree_options(Some("ROAD"), Some("cost")),
            )
            .unwrap()
            .num_rows(),
        0
    );
}

#[test]
fn maximum_spanning_tree_defaults_to_unit_weight_and_rejects_invalid_options() {
    let graph = GraphForge::new(None).unwrap();
    add_person(&graph, "Alice");
    add_person(&graph, "Bob");
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}) \
                 CREATE (a)-[:ROAD {null_cost:null, text_cost:'heavy', \
                 infinite_cost:1e308 * 2.0}]->(b)",
        )
        .unwrap();

    let unit = graph
        .analyze(None, maximum_spanning_tree_options(Some("ROAD"), None))
        .unwrap();
    assert_eq!(unit.num_rows(), 1);
    assert_eq!(
        unit.column_by_name("weight")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        1.0
    );

    let mut directed = maximum_spanning_tree_options(Some("ROAD"), None);
    directed.directed = true;
    assert!(matches!(
        graph.analyze(None, directed),
        Err(GfError::Validation(message)) if message.contains("directed=false")
    ));
    for options in [
        maximum_spanning_tree_options(Some(" "), None),
        maximum_spanning_tree_options(Some("ROAD"), Some(" ")),
        maximum_spanning_tree_options(Some("ROAD"), Some("missing")),
        maximum_spanning_tree_options(Some("ROAD"), Some("null_cost")),
        maximum_spanning_tree_options(Some("ROAD"), Some("text_cost")),
        maximum_spanning_tree_options(Some("ROAD"), Some("infinite_cost")),
    ] {
        assert!(matches!(
            graph.analyze(None, options),
            Err(GfError::Validation(_))
        ));
    }

    let empty = GraphForge::new(None).unwrap();
    let empty_batch = empty
        .analyze(None, maximum_spanning_tree_options(None, None))
        .unwrap();
    assert_eq!(empty_batch.num_rows(), 0);
    assert_eq!(empty_batch.schema(), unit.schema());
}

fn automorphism_count(batch: &arrow::record_batch::RecordBatch) -> u64 {
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 1);
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
        [("count", &DataType::UInt64, false)]
    );
    assert_eq!(batch.column(0).null_count(), 0);
    assert_eq!(
        batch.schema().metadata(),
        &HashMap::from([
            ("graphforge.algorithm".into(), "count_automorphisms".into()),
            ("graphforge.algorithm_schema_version".into(), "1".into()),
            ("graphforge.verb".into(), "analyze".into()),
        ])
    );
    for forbidden in [
        "node_uuid",
        "provenance",
        "confidence",
        "assertion",
        "evidence",
        "belief",
        "hypothesis",
        "valid_time",
        "algorithm_run_uuid",
        "run_uuid",
    ] {
        assert!(batch.column_by_name(forbidden).is_none());
    }
    batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .value(0)
}

fn build_automorphism_multigraph(graph: &GraphForge, property_prefix: &str) {
    for name in ["A", "B", "C", "D"] {
        graph
            .add_node(
                "Person",
                &HashMap::from([
                    ("name".into(), PropValue::Str(name.into())),
                    (
                        "payload".into(),
                        PropValue::Str(format!("{property_prefix}-{name}")),
                    ),
                ]),
            )
            .unwrap();
    }
    graph
        .execute(
            "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}), \
                 (c:Person {name:'C'}), (d:Person {name:'D'}) \
                 CREATE (a)-[:ROAD]->(a), (b)-[:ROAD]->(b), \
                 (a)-[:ROAD]->(b), (a)-[:ROAD]->(b), (b)-[:ROAD]->(a), \
                 (c)-[:ROAD]->(d), (d)-[:ROAD]->(c)",
        )
        .unwrap();
}

#[test]
fn count_automorphisms_is_exact_persisted_and_uuid_rename_invariant() {
    let dir = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
    build_automorphism_multigraph(&graph, "persisted");

    let directed = count_automorphisms_options(true, Some("ROAD"));
    let undirected = count_automorphisms_options(false, Some("ROAD"));
    let directed_batch = graph.analyze(Some("Person"), directed.clone()).unwrap();
    let undirected_batch = graph.analyze(Some("Person"), undirected.clone()).unwrap();
    assert_eq!(automorphism_count(&directed_batch), 2);
    assert_eq!(automorphism_count(&undirected_batch), 4);
    assert_eq!(
        directed_batch,
        graph.analyze(Some("Person"), directed.clone()).unwrap()
    );
    assert_eq!(
        undirected_batch,
        graph.analyze(Some("Person"), undirected.clone()).unwrap()
    );
    drop(graph);

    let reopened = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
    assert_eq!(
        directed_batch,
        reopened.analyze(Some("Person"), directed.clone()).unwrap()
    );
    assert_eq!(
        undirected_batch,
        reopened
            .analyze(Some("Person"), undirected.clone())
            .unwrap()
    );

    let renamed = GraphForge::new(None).unwrap();
    build_automorphism_multigraph(&renamed, "renamed-and-property-distinct");
    assert_eq!(
        automorphism_count(&renamed.analyze(Some("Person"), directed).unwrap()),
        2
    );
    assert_eq!(
        automorphism_count(&renamed.analyze(Some("Person"), undirected).unwrap()),
        4
    );
}

#[test]
fn count_automorphisms_reports_closed_options_and_overflow_structurally() {
    let graph = GraphForge::new(None).unwrap();
    build_automorphism_multigraph(&graph, "baseline");
    let baseline = graph
        .analyze(
            Some("Person"),
            count_automorphisms_options(true, Some("ROAD")),
        )
        .unwrap();
    for options in [
        AnalyzeOptions {
            weight: Some("weight".into()),
            ..count_automorphisms_options(true, Some("ROAD"))
        },
        AnalyzeOptions {
            k: Some(2),
            ..count_automorphisms_options(true, Some("ROAD"))
        },
        AnalyzeOptions {
            partition_property: Some("partition".into()),
            ..count_automorphisms_options(true, Some("ROAD"))
        },
        count_automorphisms_options(true, Some(" ")),
    ] {
        assert!(matches!(
            graph.analyze(Some("Person"), options),
            Err(GfError::Validation(_))
        ));
        assert_eq!(
            graph
                .analyze(
                    Some("Person"),
                    count_automorphisms_options(true, Some("ROAD"))
                )
                .unwrap(),
            baseline
        );
    }

    let overflow = GraphForge::new(None).unwrap();
    for index in 0..21 {
        add_person(&overflow, &format!("isolated-{index}"));
    }
    assert!(matches!(
        overflow
            .analyze(Some("Person"), count_automorphisms_options(false, None))
            .map_err(expect_algorithm_execution),
        Err(diagnostic @ AlgorithmError::AutomorphismCountOverflow) if diagnostic.to_string() == "automorphism count exceeds UInt64 range"
    ));
}

#[test]
fn triangle_count_is_exact_deterministic_and_projection_scoped() {
    let graph = GraphForge::new(None).unwrap();
    for (label, name) in [
        ("Person", "Alice"),
        ("Person", "Bob"),
        ("Person", "Carol"),
        ("Person", "Dan"),
        ("Person", "Eve"),
        ("Animal", "Fox"),
    ] {
        graph
            .add_node(
                label,
                &HashMap::from([("name".to_owned(), PropValue::Str(name.to_owned()))]),
            )
            .unwrap();
    }
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (e:Person {name:'Eve'}), (f:Animal {name:'Fox'}) \
                 CREATE (a)-[:ROAD]->(b), (a)-[:ROAD]->(b), (b)-[:ROAD]->(a), \
                 (b)-[:ROAD]->(c), (c)-[:ROAD]->(a), \
                 (b)-[:ROAD]->(d), (c)-[:ROAD]->(d), \
                 (d)-[:ROAD]->(d), (a)-[:OTHER]->(e), \
                 (e)-[:OTHER]->(c), (f)-[:ROAD]->(a)",
        )
        .unwrap();

    let options = triangle_count_options(Some("ROAD"));
    let batch = graph.analyze(Some("Person"), options.clone()).unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.schema().field(0).name(), "triangle_count");
    assert_eq!(batch.schema().field(0).data_type(), &DataType::UInt64);
    assert!(!batch.schema().field(0).is_nullable());
    assert_eq!(batch.column(0).null_count(), 0);
    assert_eq!(
        batch.schema().metadata()["graphforge.algorithm"],
        "triangle_count"
    );
    assert_eq!(batch.schema().metadata()["graphforge.verb"], "analyze");
    assert!(batch.column_by_name("node_id").is_none());
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        2
    );
    assert_eq!(batch, graph.analyze(Some("Person"), options).unwrap());

    let other = graph
        .analyze(Some("Person"), triangle_count_options(Some("OTHER")))
        .unwrap();
    assert_eq!(
        other
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        0
    );
}

#[test]
fn triangle_count_returns_zero_for_empty_and_rejects_invalid_options() {
    let graph = GraphForge::new(None).unwrap();
    let empty = graph
        .analyze(Some("Missing"), triangle_count_options(None))
        .unwrap();
    assert_eq!(empty.num_rows(), 1);
    assert_eq!(empty.num_columns(), 1);
    assert_eq!(empty.schema().field(0).name(), "triangle_count");
    assert_eq!(empty.schema().field(0).data_type(), &DataType::UInt64);
    assert!(!empty.schema().field(0).is_nullable());
    assert_eq!(empty.column(0).null_count(), 0);
    assert_eq!(
        empty
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        0
    );

    let mut directed = triangle_count_options(None);
    directed.directed = true;
    let mut weighted = triangle_count_options(None);
    weighted.weight = Some("cost".into());
    for result in [
        graph.analyze(None, directed),
        graph.analyze(None, weighted),
        graph.analyze(None, triangle_count_options(Some(" "))),
        graph.analyze(Some(""), triangle_count_options(None)),
        graph.analyze(Some(" Person"), triangle_count_options(None)),
    ] {
        assert!(matches!(result, Err(GfError::Validation(_))));
    }
}

#[test]
fn transitivity_is_exact_deterministic_and_projection_scoped() {
    let graph = GraphForge::new(None).unwrap();
    for (label, name) in [
        ("Person", "Alice"),
        ("Person", "Bob"),
        ("Person", "Carol"),
        ("Person", "Dan"),
        ("Person", "Eve"),
        ("Animal", "Fox"),
    ] {
        graph
            .add_node(
                label,
                &HashMap::from([("name".to_owned(), PropValue::Str(name.to_owned()))]),
            )
            .unwrap();
    }
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (e:Person {name:'Eve'}), (f:Animal {name:'Fox'}) \
                 CREATE (a)-[:ROAD]->(b), (a)-[:ROAD]->(b), (b)-[:ROAD]->(a), \
                 (b)-[:ROAD]->(c), (c)-[:ROAD]->(a), \
                 (b)-[:ROAD]->(d), (c)-[:ROAD]->(d), \
                 (d)-[:ROAD]->(d), (a)-[:OTHER]->(e), \
                 (e)-[:OTHER]->(c), (f)-[:ROAD]->(a)",
        )
        .unwrap();

    let options = transitivity_options(Some("ROAD"));
    let batch = graph.analyze(Some("Person"), options.clone()).unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.schema().field(0).name(), "transitivity");
    assert_eq!(batch.schema().field(0).data_type(), &DataType::Float64);
    assert!(!batch.schema().field(0).is_nullable());
    assert_eq!(batch.column(0).null_count(), 0);
    assert_eq!(
        batch.schema().metadata()["graphforge.algorithm"],
        "transitivity"
    );
    assert_eq!(batch.schema().metadata()["graphforge.verb"], "analyze");
    assert_eq!(
        batch.schema().metadata()["graphforge.algorithm_schema_version"],
        "1"
    );
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        0.75
    );
    assert_eq!(batch, graph.analyze(Some("Person"), options).unwrap());

    let other = graph
        .analyze(Some("Person"), transitivity_options(Some("OTHER")))
        .unwrap();
    assert_eq!(
        other
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        0.0
    );
}

#[test]
fn transitivity_returns_zero_for_empty_and_rejects_invalid_options() {
    let graph = GraphForge::new(None).unwrap();
    for label in [None, Some("Missing")] {
        let empty = graph.analyze(label, transitivity_options(None)).unwrap();
        assert_eq!(empty.num_rows(), 1);
        assert_eq!(empty.num_columns(), 1);
        assert_eq!(empty.schema().field(0).name(), "transitivity");
        assert_eq!(empty.schema().field(0).data_type(), &DataType::Float64);
        assert!(!empty.schema().field(0).is_nullable());
        assert_eq!(empty.column(0).null_count(), 0);
        assert_eq!(
            empty.schema().metadata()["graphforge.algorithm"],
            "transitivity"
        );
        assert_eq!(empty.schema().metadata()["graphforge.verb"], "analyze");
        assert_eq!(
            empty.schema().metadata()["graphforge.algorithm_schema_version"],
            "1"
        );
        assert_eq!(
            empty
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            0.0
        );
    }

    let mut directed = transitivity_options(None);
    directed.directed = true;
    let mut weighted = transitivity_options(None);
    weighted.weight = Some("cost".into());
    for result in [
        graph.analyze(None, directed),
        graph.analyze(None, weighted),
        graph.analyze(None, transitivity_options(Some(" "))),
        graph.analyze(Some(""), transitivity_options(None)),
        graph.analyze(Some(" Person"), transitivity_options(None)),
    ] {
        assert!(matches!(result, Err(GfError::Validation(_))));
    }
}

#[test]
fn is_planar_is_exact_deterministic_and_projection_scoped() {
    let graph = GraphForge::new(None).unwrap();
    for (label, name) in [
        ("Person", "A"),
        ("Person", "B"),
        ("Person", "C"),
        ("Person", "D"),
        ("Person", "E"),
        ("Person", "F"),
        ("Animal", "Fox"),
    ] {
        graph
            .add_node(
                label,
                &HashMap::from([("name".to_owned(), PropValue::Str(name.to_owned()))]),
            )
            .unwrap();
    }
    graph
        .execute(
            "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}), \
                 (c:Person {name:'C'}), (d:Person {name:'D'}), \
                 (e:Person {name:'E'}), (f:Person {name:'F'}), \
                 (fox:Animal {name:'Fox'}) \
                 CREATE (a)-[:ROAD]->(d), (a)-[:ROAD]->(e), (a)-[:ROAD]->(f), \
                 (b)-[:ROAD]->(d), (b)-[:ROAD]->(e), (b)-[:ROAD]->(f), \
                 (c)-[:ROAD]->(d), (c)-[:ROAD]->(e), (c)-[:ROAD]->(f), \
                 (a)-[:ROAD]->(d), (d)-[:ROAD]->(a), (a)-[:ROAD]->(a), \
                 (a)-[:OTHER]->(b), (fox)-[:ROAD]->(a)",
        )
        .unwrap();

    let options = is_planar_options(Some("ROAD"));
    let batch = graph.analyze(Some("Person"), options.clone()).unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.schema().field(0).name(), "is_planar");
    assert_eq!(batch.schema().field(0).data_type(), &DataType::Boolean);
    assert!(!batch.schema().field(0).is_nullable());
    assert_eq!(batch.column(0).null_count(), 0);
    assert_eq!(
        batch.schema().metadata()["graphforge.algorithm"],
        "is_planar"
    );
    assert_eq!(batch.schema().metadata()["graphforge.verb"], "analyze");
    assert_eq!(
        batch.schema().metadata()["graphforge.algorithm_schema_version"],
        "1"
    );
    assert!(
        !batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );
    assert_eq!(batch, graph.analyze(Some("Person"), options).unwrap());

    let other = graph
        .analyze(Some("Person"), is_planar_options(Some("OTHER")))
        .unwrap();
    assert!(
        other
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );
}

#[test]
fn is_planar_accepts_empty_and_forests_and_rejects_invalid_options() {
    let graph = GraphForge::new(None).unwrap();
    for label in [None, Some("Missing")] {
        let empty = graph.analyze(label, is_planar_options(None)).unwrap();
        assert_eq!(empty.num_rows(), 1);
        assert_eq!(empty.num_columns(), 1);
        assert_eq!(empty.schema().field(0).name(), "is_planar");
        assert_eq!(empty.schema().field(0).data_type(), &DataType::Boolean);
        assert!(!empty.schema().field(0).is_nullable());
        assert_eq!(empty.column(0).null_count(), 0);
        assert!(
            empty
                .column(0)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(0)
        );
    }

    graph
        .execute(
            "CREATE (:Person {name:'A'})-[:ROAD]->(:Person {name:'B'}), \
                 (:Person {name:'C'}), (:Person {name:'D'})-[:ROAD]->(:Person {name:'E'})",
        )
        .unwrap();
    let forest = graph
        .analyze(Some("Person"), is_planar_options(Some("ROAD")))
        .unwrap();
    assert!(
        forest
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );

    let mut directed = is_planar_options(None);
    directed.directed = true;
    let mut weighted = is_planar_options(None);
    weighted.weight = Some("cost".into());
    for result in [
        graph.analyze(None, directed),
        graph.analyze(None, weighted),
        graph.analyze(None, is_planar_options(Some(" "))),
        graph.analyze(Some(""), is_planar_options(None)),
        graph.analyze(Some(" Person"), is_planar_options(None)),
    ] {
        assert!(matches!(result, Err(GfError::Validation(_))));
    }
}

#[test]
fn triad_census_returns_canonical_sixteen_row_arrow_result() {
    let graph = GraphForge::new(None).unwrap();
    for name in ["Alice", "Bob", "Carol", "Isolate"] {
        graph
            .add_node(
                "Person",
                &HashMap::from([("name".to_owned(), PropValue::Str(name.to_owned()))]),
            )
            .unwrap();
    }
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}) \
                 CREATE (a)-[:ROAD]->(b), (b)-[:ROAD]->(c), (c)-[:ROAD]->(a), \
                 (a)-[:ROAD]->(a), (a)-[:ROAD]->(b), (a)-[:OTHER]->(c)",
        )
        .unwrap();

    let options = triad_census_options(Some("ROAD"));
    let batch = graph.analyze(Some("Person"), options.clone()).unwrap();
    assert_eq!(batch.num_rows(), 16);
    assert_eq!(batch.num_columns(), 2);
    assert_eq!(batch.schema().field(0).name(), "triad_type");
    assert_eq!(batch.schema().field(0).data_type(), &DataType::Utf8);
    assert!(!batch.schema().field(0).is_nullable());
    assert_eq!(batch.schema().field(1).name(), "count");
    assert_eq!(batch.schema().field(1).data_type(), &DataType::UInt64);
    assert!(!batch.schema().field(1).is_nullable());
    assert_eq!(
        batch.schema().metadata()["graphforge.algorithm"],
        "triad_census"
    );
    assert_eq!(batch.schema().metadata()["graphforge.verb"], "analyze");
    assert_eq!(
        batch.schema().metadata()["graphforge.algorithm_schema_version"],
        "1"
    );

    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let counts = batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let expected_names = [
        "003", "012", "102", "021D", "021U", "021C", "111D", "111U", "030T", "030C", "201", "120D",
        "120U", "120C", "210", "300",
    ];
    assert_eq!(
        names.iter().map(Option::unwrap).collect::<Vec<_>>(),
        expected_names
    );
    assert_eq!(
        counts.values().to_vec(),
        [0, 3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0]
    );
    assert_eq!(batch, graph.analyze(Some("Person"), options).unwrap());
}

#[test]
fn triad_census_preserves_empty_rows_and_rejects_invalid_options() {
    let graph = GraphForge::new(None).unwrap();
    let empty = graph
        .analyze(Some("Missing"), triad_census_options(None))
        .unwrap();
    assert_eq!(empty.num_rows(), 16);
    assert_eq!(
        empty
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .values()
            .iter()
            .sum::<u64>(),
        0
    );

    let mut undirected = triad_census_options(None);
    undirected.directed = false;
    let mut weighted = triad_census_options(None);
    weighted.weight = Some("cost".into());
    for result in [
        graph.analyze(None, undirected),
        graph.analyze(None, weighted),
        graph.analyze(None, triad_census_options(Some(" "))),
        graph.analyze(Some(""), triad_census_options(None)),
        graph.analyze(Some(" Person"), triad_census_options(None)),
    ] {
        assert!(matches!(result, Err(GfError::Validation(_))));
    }
}

#[test]
fn dyad_census_returns_canonical_three_row_arrow_result() {
    // Exploratory mode has no ontology/knowledge layer (#772).
    let graph = GraphForge::new(None).unwrap();
    for name in ["Alice", "Bob", "Carol", "Dan", "Isolate"] {
        graph
            .add_node(
                "Person",
                &HashMap::from([("name".to_owned(), PropValue::Str(name.to_owned()))]),
            )
            .unwrap();
    }
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}) \
                 CREATE (a)-[:ROAD]->(b), (b)-[:ROAD]->(a), \
                 (a)-[:ROAD]->(b), (a)-[:ROAD]->(c), (d)-[:ROAD]->(c), \
                 (a)-[:ROAD]->(a), (c)-[:OTHER]->(a)",
        )
        .unwrap();

    let options = dyad_census_options(Some("ROAD"));
    let batch = graph.analyze(Some("Person"), options.clone()).unwrap();
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 2);
    assert_eq!(batch.schema().field(0).name(), "dyad_type");
    assert_eq!(batch.schema().field(0).data_type(), &DataType::Utf8);
    assert!(!batch.schema().field(0).is_nullable());
    assert_eq!(batch.schema().field(1).name(), "count");
    assert_eq!(batch.schema().field(1).data_type(), &DataType::UInt64);
    assert!(!batch.schema().field(1).is_nullable());
    assert_eq!(
        batch.schema().metadata()["graphforge.algorithm"],
        "dyad_census"
    );
    assert_eq!(batch.schema().metadata()["graphforge.verb"], "analyze");
    assert_eq!(
        batch.schema().metadata()["graphforge.algorithm_schema_version"],
        "1"
    );

    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let counts = batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(
        names.iter().map(Option::unwrap).collect::<Vec<_>>(),
        ["mutual", "asymmetric", "null"]
    );
    assert_eq!(counts.values(), &[1, 2, 7]);
    assert_eq!(batch, graph.analyze(Some("Person"), options).unwrap());

    let all_relationships = graph
        .analyze(Some("Person"), dyad_census_options(None))
        .unwrap();
    assert_eq!(
        all_relationships
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .values(),
        &[2, 1, 7]
    );
}

#[test]
fn dyad_census_preserves_zero_rows_and_rejects_invalid_options() {
    let graph = GraphForge::new(None).unwrap();
    for name in ["Fox", "Owl"] {
        graph
            .add_node(
                "Animal",
                &HashMap::from([("name".to_owned(), PropValue::Str(name.to_owned()))]),
            )
            .unwrap();
    }
    for (label, expected) in [(Some("Missing"), [0, 0, 0]), (Some("Animal"), [0, 0, 1])] {
        let batch = graph.analyze(label, dyad_census_options(None)).unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .values(),
            &expected
        );
    }

    let singleton = GraphForge::new(None).unwrap();
    singleton
        .add_node(
            "Person",
            &HashMap::from([("name".to_owned(), PropValue::Str("Solo".to_owned()))]),
        )
        .unwrap();
    assert_eq!(
        singleton
            .analyze(Some("Person"), dyad_census_options(None))
            .unwrap()
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .values(),
        &[0, 0, 0]
    );

    let mut undirected = dyad_census_options(None);
    undirected.directed = false;
    let mut weighted = dyad_census_options(None);
    weighted.weight = Some("cost".into());
    for result in [
        graph.analyze(None, undirected),
        graph.analyze(None, weighted),
        graph.analyze(None, dyad_census_options(Some(" "))),
        graph.analyze(Some(""), dyad_census_options(None)),
        graph.analyze(Some(" Person"), dyad_census_options(None)),
    ] {
        assert!(matches!(result, Err(GfError::Validation(_))));
    }
}

#[test]
fn node_coloring_is_exact_deterministic_and_projection_scoped() {
    let graph = GraphForge::new(None).unwrap();
    let mut nodes = ["Alice", "Bob", "Carol", "Dan", "Eve"]
        .into_iter()
        .map(|name| {
            let node = add_person(&graph, name);
            (name, *node.uuid.as_bytes())
        })
        .collect::<Vec<_>>();
    graph
        .add_node(
            "Animal",
            &HashMap::from([("name".to_owned(), PropValue::Str("Fox".to_owned()))]),
        )
        .unwrap();
    nodes.sort_unstable_by_key(|(_, uuid)| *uuid);
    graph
        .execute(&format!(
            "MATCH (a:Person {{name:'{}'}}), (b:Person {{name:'{}'}}), \
                 (c:Person {{name:'{}'}}), (d:Person {{name:'{}'}}), \
                 (e:Person {{name:'{}'}}), (f:Animal {{name:'Fox'}}) \
                 CREATE (a)-[:ROAD]->(b), (a)-[:ROAD]->(c), \
                 (b)-[:ROAD]->(c), (c)-[:ROAD]->(d), \
                 (a)-[:ROAD]->(b), (b)-[:ROAD]->(a), \
                 (d)-[:OTHER]->(e), (f)-[:ROAD]->(a)",
            nodes[0].0, nodes[1].0, nodes[2].0, nodes[3].0, nodes[4].0,
        ))
        .unwrap();

    let options = node_coloring_options(Some("ROAD"));
    let batch = graph.analyze(Some("Person"), options.clone()).unwrap();
    assert_eq!(batch.num_rows(), 5);
    assert_eq!(batch.num_columns(), 2);
    assert_eq!(batch.schema().field(0).name(), "node_uuid");
    assert_eq!(
        batch.schema().field(0).data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert_eq!(batch.schema().field(1).name(), "color");
    assert_eq!(batch.schema().field(1).data_type(), &DataType::UInt64);
    assert!(!batch.schema().field(0).is_nullable());
    assert!(!batch.schema().field(1).is_nullable());
    assert_eq!(batch.column(0).null_count(), 0);
    assert_eq!(batch.column(1).null_count(), 0);
    assert_eq!(
        batch.schema().metadata()["graphforge.algorithm"],
        "node_coloring"
    );
    assert_eq!(batch.schema().metadata()["graphforge.verb"], "analyze");
    assert_eq!(
        batch.schema().metadata()["graphforge.algorithm_schema_version"],
        "1"
    );
    assert_eq!(
        node_coloring_rows(&batch),
        nodes
            .iter()
            .zip([0, 1, 2, 0, 0])
            .map(|((_, uuid), color)| (*uuid, color))
            .collect::<Vec<_>>()
    );
    assert_eq!(batch, graph.analyze(Some("Person"), options).unwrap());
}

#[test]
fn node_coloring_keeps_typed_empty_schema_and_rejects_invalid_options() {
    let graph = GraphForge::new(None).unwrap();
    for label in [None, Some("Missing")] {
        let empty = graph
            .analyze(label, node_coloring_options(Some("ROAD")))
            .unwrap();
        assert_eq!(empty.num_rows(), 0);
        assert_eq!(empty.num_columns(), 2);
        assert_eq!(empty.schema().field(0).name(), "node_uuid");
        assert_eq!(
            empty.schema().field(0).data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(empty.schema().field(1).name(), "color");
        assert_eq!(empty.schema().field(1).data_type(), &DataType::UInt64);
        assert!(!empty.schema().field(0).is_nullable());
        assert!(!empty.schema().field(1).is_nullable());
        assert_eq!(empty.column(0).null_count(), 0);
        assert_eq!(empty.column(1).null_count(), 0);
        assert_eq!(
            empty.schema().metadata()["graphforge.algorithm"],
            "node_coloring"
        );
        assert_eq!(empty.schema().metadata()["graphforge.verb"], "analyze");
        assert_eq!(
            empty.schema().metadata()["graphforge.algorithm_schema_version"],
            "1"
        );
    }

    let mut directed = node_coloring_options(None);
    directed.directed = true;
    let mut weighted = node_coloring_options(None);
    weighted.weight = Some("cost".into());
    for result in [
        graph.analyze(None, directed),
        graph.analyze(None, weighted),
        graph.analyze(None, node_coloring_options(Some(" "))),
        graph.analyze(Some(""), node_coloring_options(None)),
        graph.analyze(Some(" Person"), node_coloring_options(None)),
    ] {
        assert!(matches!(result, Err(GfError::Validation(_))));
    }
}

#[test]
fn k1_coloring_is_deterministic_uuid_only_and_distinct_from_chromatic_number() {
    // Exploratory mode reaches the Rust graph path without an ontology or knowledge layer.
    let graph = GraphForge::new(None).unwrap();
    let mut nodes = ["A", "B", "C", "D", "E", "F", "Isolate"]
        .into_iter()
        .map(|name| {
            let node = add_person(&graph, name);
            (name, *node.uuid.as_bytes())
        })
        .collect::<Vec<_>>();
    nodes.sort_unstable_by_key(|(_, uuid)| *uuid);

    // Crown graph K3,3 minus a perfect matching. Its UUID-interleaved greedy order
    // uses three colors even though its exact chromatic number is two.
    for left in [0, 2, 4] {
        for right in [1, 3, 5] {
            if left / 2 == right / 2 {
                continue;
            }
            graph
                .execute(&format!(
                    "MATCH (a:Person {{name:'{}'}}), (b:Person {{name:'{}'}}) \
                         CREATE (a)-[:ROAD]->(b)",
                    nodes[left].0, nodes[right].0
                ))
                .unwrap();
        }
    }
    graph
        .execute(&format!(
            "MATCH (a:Person {{name:'{}'}}), (b:Person {{name:'{}'}}) \
                 CREATE (a)-[:ROAD]->(b), (b)-[:ROAD]->(a)",
            nodes[0].0, nodes[3].0
        ))
        .unwrap();

    let options = k1_coloring_options(Some("ROAD"));
    let batch = graph.analyze(Some("Person"), options.clone()).unwrap();
    let expected = nodes
        .iter()
        .zip([0, 0, 1, 1, 2, 2, 0])
        .map(|((_, uuid), color)| (*uuid, color))
        .collect::<Vec<_>>();

    assert_eq!(batch.num_rows(), 7);
    assert_eq!(batch.num_columns(), 2);
    assert_eq!(batch.schema().field(0).name(), "node_uuid");
    assert_eq!(
        batch.schema().field(0).data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert_eq!(batch.schema().field(1).name(), "color");
    assert_eq!(batch.schema().field(1).data_type(), &DataType::UInt64);
    assert!(!batch.schema().field(0).is_nullable());
    assert!(!batch.schema().field(1).is_nullable());
    assert_eq!(batch.column(0).null_count(), 0);
    assert_eq!(batch.column(1).null_count(), 0);
    assert_eq!(
        batch.schema().metadata()["graphforge.algorithm"],
        "k1_coloring"
    );
    assert_eq!(batch.schema().metadata()["graphforge.verb"], "analyze");
    assert_eq!(
        batch.schema().metadata()["graphforge.algorithm_schema_version"],
        "1"
    );
    assert_eq!(batch.schema().metadata().len(), 3);
    assert_eq!(node_coloring_rows(&batch), expected);
    assert_eq!(batch, graph.analyze(Some("Person"), options).unwrap());

    let chromatic = graph
        .analyze(Some("Person"), chromatic_number_options(Some("ROAD")))
        .unwrap();
    assert_eq!(
        chromatic
            .column_by_name("chromatic_number")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        2
    );
}

#[test]
fn k1_coloring_rejects_self_loops_direction_and_weight_structurally() {
    let looped = GraphForge::new(None).unwrap();
    add_person(&looped, "Looped");
    looped
        .execute("MATCH (n:Person {name:'Looped'}) CREATE (n)-[:ROAD]->(n)")
        .unwrap();
    assert!(matches!(
        looped.analyze(Some("Person"), k1_coloring_options(Some("ROAD"))).map_err(expect_algorithm_execution),
        Err(AlgorithmError::Execution { message })
            if message.contains("k1_coloring cannot color a graph containing a self-loop")
    ));

    let graph = GraphForge::new(None).unwrap();
    add_person(&graph, "Solo");
    for options in [
        AnalyzeOptions {
            directed: true,
            ..k1_coloring_options(None)
        },
        AnalyzeOptions {
            weight: Some("cost".into()),
            ..k1_coloring_options(None)
        },
    ] {
        assert!(matches!(
            graph.analyze(Some("Person"), options),
            Err(GfError::Validation(_))
        ));
    }
}

#[test]
fn chromatic_number_is_exact_deterministic_and_projection_scoped() {
    // Exploratory mode exercises the Rust graph path without an ontology/knowledge layer (#772).
    let graph = GraphForge::new(None).unwrap();
    for name in ["Alice", "Bob", "Carol", "Dan", "Eve"] {
        add_person(&graph, name);
    }
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (e:Person {name:'Eve'}) \
                 CREATE (a)-[:ROAD]->(b), (b)-[:ROAD]->(c), \
                 (c)-[:ROAD]->(a), (a)-[:ROAD]->(b), \
                 (b)-[:ROAD]->(a), (d)-[:OTHER]->(e)",
        )
        .unwrap();

    let options = chromatic_number_options(Some("ROAD"));
    let batch = graph.analyze(Some("Person"), options.clone()).unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.schema().field(0).name(), "chromatic_number");
    assert_eq!(batch.schema().field(0).data_type(), &DataType::UInt64);
    assert!(!batch.schema().field(0).is_nullable());
    assert_eq!(batch.column(0).null_count(), 0);
    assert_eq!(
        batch.schema().metadata()["graphforge.algorithm"],
        "chromatic_number"
    );
    assert_eq!(batch.schema().metadata()["graphforge.verb"], "analyze");
    assert_eq!(
        batch.schema().metadata()["graphforge.algorithm_schema_version"],
        "1"
    );
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        3
    );
    assert_eq!(batch, graph.analyze(Some("Person"), options).unwrap());
    assert_eq!(
        graph
            .analyze(Some("Person"), chromatic_number_options(Some("MISSING")))
            .unwrap()
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        1
    );
}

#[test]
fn chromatic_number_keeps_typed_scalar_metadata_and_rejects_invalid_input() {
    let empty = GraphForge::new(None).unwrap();
    for label in [None, Some("Missing")] {
        let batch = empty
            .analyze(label, chromatic_number_options(None))
            .unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "chromatic_number");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::UInt64);
        assert!(!batch.schema().field(0).is_nullable());
        assert_eq!(batch.column(0).null_count(), 0);
        assert_eq!(
            batch.schema().metadata()["graphforge.algorithm"],
            "chromatic_number"
        );
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0),
            0
        );
    }

    let populated = GraphForge::new(None).unwrap();
    add_person(&populated, "Alice");
    let missing = populated
        .analyze(Some("Missing"), chromatic_number_options(None))
        .unwrap();
    assert_eq!(missing.num_rows(), 1);
    assert_eq!(missing.schema().field(0).data_type(), &DataType::UInt64);
    assert!(!missing.schema().field(0).is_nullable());
    assert_eq!(missing.column(0).null_count(), 0);
    assert_eq!(
        missing.schema().metadata()["graphforge.algorithm"],
        "chromatic_number"
    );
    assert_eq!(
        missing
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        0
    );

    let looped = GraphForge::new(None).unwrap();
    add_person(&looped, "Alice");
    looped
        .execute("MATCH (a:Person {name:'Alice'}) CREATE (a)-[:ROAD]->(a)")
        .unwrap();
    assert!(matches!(
        looped.analyze(None, chromatic_number_options(Some("ROAD"))).map_err(expect_algorithm_execution),
        Err(AlgorithmError::Execution { message })
            if message.contains("undefined for a graph containing a self-loop")
    ));

    for options in [
        AnalyzeOptions {
            directed: true,
            ..chromatic_number_options(None)
        },
        AnalyzeOptions {
            weight: Some("cost".into()),
            ..chromatic_number_options(None)
        },
        chromatic_number_options(Some(" ")),
    ] {
        assert!(matches!(
            empty.analyze(None, options),
            Err(GfError::Validation(_))
        ));
    }
}

#[test]
fn find_cycles_is_uuid_only_canonical_and_direction_aware() {
    let graph = GraphForge::new(None).unwrap();
    let nodes = ["Alice", "Bob", "Carol", "Dan", "Eve", "Fox"].map(|name| add_person(&graph, name));
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (e:Person {name:'Eve'}), (f:Person {name:'Fox'}) \
                 CREATE (a)-[:ROAD]->(b), (a)-[:ROAD]->(b), \
                 (b)-[:ROAD]->(c), (c)-[:ROAD]->(a), \
                 (b)-[:ROAD]->(d), (d)-[:ROAD]->(b), (d)-[:ROAD]->(d), \
                 (e)-[:ROAD]->(f), (a)-[:OTHER]->(d), (d)-[:OTHER]->(a)",
        )
        .unwrap();

    let cycle_rows = |batch: &arrow::record_batch::RecordBatch| {
        let lists = batch
            .column_by_name("cycle")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(lists.null_count(), 0);
        (0..lists.len())
            .map(|row| {
                let values = lists.value(row);
                let values = values
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap();
                assert_eq!(values.null_count(), 0);
                (0..values.len())
                    .map(|index| values.value(index).try_into().unwrap())
                    .collect::<Vec<[u8; 16]>>()
            })
            .collect::<Vec<_>>()
    };
    let canonical = |cycle: Vec<[u8; 16]>, directed: bool| {
        let rotations = |values: &[[u8; 16]]| {
            (0..values.len())
                .map(|offset| {
                    values[offset..]
                        .iter()
                        .chain(&values[..offset])
                        .copied()
                        .collect::<Vec<_>>()
                })
                .min()
                .unwrap()
        };
        let forward = rotations(&cycle);
        if directed || cycle.len() < 2 {
            forward
        } else {
            let reversed = cycle.into_iter().rev().collect::<Vec<_>>();
            forward.min(rotations(&reversed))
        }
    };
    let uuid = |index: usize| *nodes[index].uuid.as_bytes();

    let directed_options = find_cycles_options(true, Some("ROAD"));
    let directed = graph
        .analyze(Some("Person"), directed_options.clone())
        .unwrap();
    assert_eq!(directed.num_columns(), 1);
    let directed_schema = directed.schema();
    let field = directed_schema.field(0);
    assert_eq!(field.name(), "cycle");
    assert!(!field.is_nullable());
    let DataType::List(item) = field.data_type() else {
        panic!("cycle must be a List");
    };
    assert_eq!(item.data_type(), &DataType::FixedSizeBinary(16));
    assert!(!item.is_nullable());
    assert_eq!(
        directed_schema.metadata()["graphforge.algorithm"],
        "find_cycles"
    );
    assert_eq!(directed_schema.metadata()["graphforge.verb"], "analyze");
    assert_eq!(
        directed_schema.metadata()["graphforge.algorithm_schema_version"],
        "1"
    );
    let mut expected_directed = vec![
        canonical(vec![uuid(0), uuid(1), uuid(2)], true),
        canonical(vec![uuid(1), uuid(3)], true),
        vec![uuid(3)],
    ];
    expected_directed.sort();
    assert_eq!(cycle_rows(&directed), expected_directed);
    assert_eq!(
        directed,
        graph.analyze(Some("Person"), directed_options).unwrap()
    );

    let undirected = graph
        .analyze(Some("Person"), find_cycles_options(false, Some("ROAD")))
        .unwrap();
    let mut expected_undirected = vec![
        canonical(vec![uuid(0), uuid(1), uuid(2)], false),
        vec![uuid(3)],
    ];
    expected_undirected.sort();
    assert_eq!(cycle_rows(&undirected), expected_undirected);
}

#[test]
fn find_cycles_empty_schema_and_option_validation_are_stable() {
    let graph = GraphForge::new(None).unwrap();
    let empty = graph
        .analyze(Some("Missing"), find_cycles_options(true, None))
        .unwrap();
    assert_eq!(empty.num_rows(), 0);
    assert_eq!(empty.num_columns(), 1);
    let empty_schema = empty.schema();
    assert_eq!(empty_schema.field(0).name(), "cycle");
    assert!(!empty_schema.field(0).is_nullable());
    let DataType::List(item) = empty_schema.field(0).data_type() else {
        panic!("cycle must be a List");
    };
    assert_eq!(item.data_type(), &DataType::FixedSizeBinary(16));
    assert!(!item.is_nullable());
    assert_eq!(
        empty_schema.metadata()["graphforge.algorithm"],
        "find_cycles"
    );
    assert_eq!(empty_schema.metadata()["graphforge.verb"], "analyze");
    assert_eq!(
        empty_schema.metadata()["graphforge.algorithm_schema_version"],
        "1"
    );
    assert_eq!(
        empty
            .column_by_name("cycle")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .null_count(),
        0
    );

    let mut weighted = find_cycles_options(true, None);
    weighted.weight = Some("cost".into());
    for result in [
        graph.analyze(None, weighted),
        graph.analyze(None, find_cycles_options(true, Some(" "))),
        graph.analyze(Some(""), find_cycles_options(true, None)),
        graph.analyze(Some(" Person"), find_cycles_options(false, None)),
    ] {
        assert!(matches!(result, Err(GfError::Validation(_))));
    }
}

#[test]
fn articulation_points_is_uuid_only_multigraph_safe_and_deterministic() {
    let graph = GraphForge::new(None).unwrap();
    let nodes = ["Alice", "Bob", "Carol", "Dan", "Eve", "Fox", "Gus", "Hal"]
        .map(|name| add_person(&graph, name));
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (e:Person {name:'Eve'}), (f:Person {name:'Fox'}), \
                 (g:Person {name:'Gus'}) \
                 CREATE (a)-[:ROAD]->(b), (b)-[:ROAD]->(c), (c)-[:ROAD]->(a), \
                 (b)-[:ROAD]->(d), (d)-[:ROAD]->(b), (d)-[:ROAD]->(e), \
                 (d)-[:ROAD]->(d), (f)-[:ROAD]->(g), (a)-[:OTHER]->(e)",
        )
        .unwrap();
    let options = articulation_points_options(Some("ROAD"));
    let batch = graph.analyze(Some("Person"), options.clone()).unwrap();

    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.schema().field(0).name(), "node_uuid");
    assert_eq!(
        batch.schema().field(0).data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert!(!batch.schema().field(0).is_nullable());
    assert_eq!(
        batch.schema().metadata()["graphforge.algorithm"],
        "articulation_points"
    );
    assert_eq!(batch.schema().metadata()["graphforge.verb"], "analyze");
    let values = batch
        .column_by_name("node_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(values.null_count(), 0);
    let mut expected = [*nodes[1].uuid.as_bytes(), *nodes[3].uuid.as_bytes()];
    expected.sort_unstable();
    assert_eq!(
        (0..values.len())
            .map(|row| <[u8; 16]>::try_from(values.value(row)).unwrap())
            .collect::<Vec<_>>(),
        expected
    );
    assert_eq!(batch, graph.analyze(Some("Person"), options).unwrap());

    let no_articulation = graph
        .analyze(Some("Person"), articulation_points_options(Some("OTHER")))
        .unwrap();
    assert_eq!(no_articulation.num_rows(), 0);
    assert_eq!(no_articulation.schema(), batch.schema());
    assert_eq!(
        graph
            .analyze(Some("Missing"), articulation_points_options(Some("ROAD")))
            .unwrap()
            .num_rows(),
        0
    );
}

#[test]
fn articulation_points_rejects_directed_weight_and_invalid_selectors() {
    let graph = GraphForge::new(None).unwrap();
    let mut directed = articulation_points_options(None);
    directed.directed = true;
    let mut weighted = articulation_points_options(None);
    weighted.weight = Some("cost".into());
    for result in [
        graph.analyze(None, directed),
        graph.analyze(None, weighted),
        graph.analyze(None, articulation_points_options(Some(" "))),
    ] {
        assert!(matches!(result, Err(GfError::Validation(_))));
    }
}

#[test]
fn bridges_is_uuid_only_multigraph_safe_and_deterministic() {
    let graph = GraphForge::new(None).unwrap();
    let nodes = ["Alice", "Bob", "Carol", "Dan", "Eve", "Fox", "Gus", "Hal"]
        .map(|name| add_person(&graph, name));
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (e:Person {name:'Eve'}), (f:Person {name:'Fox'}), \
                 (g:Person {name:'Gus'}) \
                 CREATE (a)-[:ROAD]->(b), (b)-[:ROAD]->(c), (c)-[:ROAD]->(a), \
                 (b)-[:ROAD]->(d), (d)-[:ROAD]->(b), (d)-[:ROAD]->(e), \
                 (d)-[:ROAD]->(d), (f)-[:ROAD]->(g), (a)-[:OTHER]->(e)",
        )
        .unwrap();
    let options = bridges_options(Some("ROAD"));
    let batch = graph.analyze(Some("Person"), options.clone()).unwrap();

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
            ("edge_uuid", &DataType::FixedSizeBinary(16), false),
            ("source_uuid", &DataType::FixedSizeBinary(16), false),
            ("target_uuid", &DataType::FixedSizeBinary(16), false),
        ]
    );
    assert_eq!(batch.schema().metadata()["graphforge.algorithm"], "bridges");
    assert_eq!(batch.schema().metadata()["graphforge.verb"], "analyze");
    assert_eq!(batch.num_rows(), 2);
    let edge_uuids = batch
        .column_by_name("edge_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let sources = batch
        .column_by_name("source_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let targets = batch
        .column_by_name("target_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(edge_uuids.null_count(), 0);
    for row in 0..batch.num_rows() {
        assert!(sources.value(row) < targets.value(row));
        if row > 0 {
            assert!(
                (
                    sources.value(row - 1),
                    targets.value(row - 1),
                    edge_uuids.value(row - 1)
                ) < (
                    sources.value(row),
                    targets.value(row),
                    edge_uuids.value(row)
                )
            );
        }
    }
    let expected_endpoints = [(3, 4), (5, 6)]
        .map(|(left, right)| {
            let mut pair = [*nodes[left].uuid.as_bytes(), *nodes[right].uuid.as_bytes()];
            pair.sort_unstable();
            pair
        })
        .into_iter()
        .collect::<std::collections::HashSet<_>>();
    let actual_endpoints = (0..batch.num_rows())
        .map(|row| {
            [
                <[u8; 16]>::try_from(sources.value(row)).unwrap(),
                <[u8; 16]>::try_from(targets.value(row)).unwrap(),
            ]
        })
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(actual_endpoints, expected_endpoints);
    assert_eq!(batch, graph.analyze(Some("Person"), options).unwrap());

    for selection in [
        graph.analyze(Some("Person"), bridges_options(Some("MISSING"))),
        graph.analyze(Some("Missing"), bridges_options(Some("ROAD"))),
        GraphForge::new(None)
            .unwrap()
            .analyze(None, bridges_options(None)),
    ] {
        let empty = selection.unwrap();
        assert_eq!(empty.num_rows(), 0);
        assert_eq!(empty.schema(), batch.schema());
    }
}

#[test]
fn bridges_rejects_directed_weight_and_invalid_selectors() {
    let graph = GraphForge::new(None).unwrap();
    let mut directed = bridges_options(None);
    directed.directed = true;
    let mut weighted = bridges_options(None);
    weighted.weight = Some("cost".into());
    for result in [
        graph.analyze(None, directed),
        graph.analyze(None, weighted),
        graph.analyze(None, bridges_options(Some(" "))),
    ] {
        assert!(matches!(result, Err(GfError::Validation(_))));
    }
}
