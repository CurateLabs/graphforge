use super::*;

fn topological_sort_options(directed: bool, via: Option<&str>) -> AnalyzeOptions {
    AnalyzeOptions {
        by: AnalyzeAlgorithm::TopologicalSort,
        via: via.map(str::to_owned),
        directed,
        weight: None,
        k: None,
        partition_property: None,
    }
}

fn dag_longest_path_options(directed: bool, via: Option<&str>) -> AnalyzeOptions {
    AnalyzeOptions {
        by: AnalyzeAlgorithm::DagLongestPath,
        via: via.map(str::to_owned),
        directed,
        weight: None,
        k: None,
        partition_property: None,
    }
}

fn weighted_dag_longest_path_options(
    directed: bool,
    via: Option<&str>,
    weight: Option<&str>,
) -> AnalyzeOptions {
    AnalyzeOptions {
        by: AnalyzeAlgorithm::DagLongestPathWeighted,
        via: via.map(str::to_owned),
        directed,
        weight: weight.map(str::to_owned),
        k: None,
        partition_property: None,
    }
}

fn edge_coloring_options(via: Option<&str>) -> AnalyzeOptions {
    AnalyzeOptions {
        by: AnalyzeAlgorithm::EdgeColoring,
        via: via.map(str::to_owned),
        directed: false,
        weight: None,
        k: None,
        partition_property: None,
    }
}

fn has_euler_circuit_options(directed: bool, via: Option<&str>) -> AnalyzeOptions {
    AnalyzeOptions {
        by: AnalyzeAlgorithm::HasEulerCircuit,
        via: via.map(str::to_owned),
        directed,
        weight: None,
        k: None,
        partition_property: None,
    }
}

fn has_euler_path_options(directed: bool, via: Option<&str>) -> AnalyzeOptions {
    AnalyzeOptions {
        by: AnalyzeAlgorithm::HasEulerPath,
        via: via.map(str::to_owned),
        directed,
        weight: None,
        k: None,
        partition_property: None,
    }
}

fn euler_options(by: AnalyzeAlgorithm, directed: bool, via: Option<&str>) -> AnalyzeOptions {
    AnalyzeOptions {
        by,
        via: via.map(str::to_owned),
        directed,
        weight: None,
        k: None,
        partition_property: None,
    }
}

fn euler_uuid_list(batch: &arrow::record_batch::RecordBatch, column: &str) -> Vec<[u8; 16]> {
    let lists = batch
        .column_by_name(column)
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(lists.null_count(), 0);
    let values = lists.value(0);
    let values = values
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(values.null_count(), 0);
    (0..values.len())
        .map(|index| values.value(index).try_into().unwrap())
        .collect()
}

fn assert_euler_edge_alignment(
    node_path: &[[u8; 16]],
    edge_path: &[[u8; 16]],
    relationship_rows: &[([u8; 16], [u8; 16], [u8; 16])],
    directed: bool,
) {
    assert_eq!(node_path.len(), edge_path.len() + 1);
    let mut remaining = relationship_rows
        .iter()
        .copied()
        .map(|(edge, source, target)| (edge, (source, target)))
        .collect::<HashMap<_, _>>();
    for (edge, nodes) in edge_path.iter().zip(node_path.windows(2)) {
        let (source, target) = remaining.remove(edge).expect("edge UUID occurs once");
        assert!(
            (nodes[0] == source && nodes[1] == target)
                || (!directed && nodes[0] == target && nodes[1] == source),
            "edge UUID must align with its adjacent node UUIDs"
        );
    }
    assert!(
        remaining.is_empty(),
        "every selected edge UUID is preserved"
    );
}

fn assert_euler_schema(batch: &arrow::record_batch::RecordBatch, algorithm: &str) {
    let fields = batch.schema().fields().clone();
    assert_eq!(fields.len(), 2);
    for (field, name) in fields.iter().zip(["node_path", "edge_path"]) {
        assert_eq!(field.name(), name);
        assert!(!field.is_nullable());
        let DataType::List(item) = field.data_type() else {
            panic!("{name} must be a List");
        };
        assert_eq!(item.data_type(), &DataType::FixedSizeBinary(16));
        assert!(!item.is_nullable());
    }
    assert_eq!(
        batch.schema().metadata(),
        &HashMap::from([
            ("graphforge.algorithm".to_owned(), algorithm.to_owned()),
            (
                "graphforge.algorithm_schema_version".to_owned(),
                "1".to_owned()
            ),
            ("graphforge.verb".to_owned(), "analyze".to_owned()),
        ])
    );
}

fn is_dag_value(batch: &arrow::record_batch::RecordBatch) -> bool {
    batch
        .column_by_name("is_dag")
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .value(0)
}

fn has_euler_circuit_value(batch: &arrow::record_batch::RecordBatch) -> bool {
    let values = batch
        .column_by_name("has_euler_circuit")
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert_eq!(values.null_count(), 0);
    values.value(0)
}

fn has_euler_path_value(batch: &arrow::record_batch::RecordBatch) -> bool {
    let values = batch
        .column_by_name("has_euler_path")
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert_eq!(values.null_count(), 0);
    values.value(0)
}

fn topological_rows(batch: &arrow::record_batch::RecordBatch) -> Vec<([u8; 16], u64)> {
    let nodes = batch
        .column_by_name("node_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let orders = batch
        .column_by_name("order")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    (0..batch.num_rows())
        .map(|row| (nodes.value(row).try_into().unwrap(), orders.value(row)))
        .collect()
}

fn edge_color_rows(batch: &arrow::record_batch::RecordBatch) -> Vec<([u8; 16], u64)> {
    let edges = batch
        .column_by_name("edge_uuid")
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
    assert_eq!(edges.null_count(), 0);
    assert_eq!(colors.null_count(), 0);
    (0..batch.num_rows())
        .map(|row| (edges.value(row).try_into().unwrap(), colors.value(row)))
        .collect()
}

#[test]
fn is_dag_obeys_schema_label_via_direction_and_edge_contracts() {
    assert!(AnalyzeOptions::default().directed);
    let graph = GraphForge::new(None).unwrap();
    for (label, name) in [
        ("Person", "Alice"),
        ("Person", "Bob"),
        ("Person", "Carol"),
        ("Animal", "Fox"),
        ("Animal", "Wolf"),
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
                 (c:Person {name:'Carol'}), (f:Animal {name:'Fox'}), \
                 (w:Animal {name:'Wolf'}) \
                 CREATE (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), \
                 (b)-[:KNOWS]->(c), (f)-[:OTHER]->(w), (w)-[:OTHER]->(f)",
        )
        .unwrap();

    let global = graph.analyze(None, AnalyzeOptions::default()).unwrap();
    assert_eq!(global.num_rows(), 1);
    assert_eq!(global.num_columns(), 1);
    assert_eq!(global.schema().field(0).name(), "is_dag");
    assert_eq!(global.schema().field(0).data_type(), &DataType::Boolean);
    assert!(!global.schema().field(0).is_nullable());
    assert_eq!(global.schema().metadata()["graphforge.algorithm"], "is_dag");
    assert_eq!(global.schema().metadata()["graphforge.verb"], "analyze");
    assert!(!is_dag_value(&global));
    assert_eq!(
        global,
        graph.analyze(None, AnalyzeOptions::default()).unwrap()
    );

    assert!(is_dag_value(
        &graph
            .analyze(Some("Person"), AnalyzeOptions::default())
            .unwrap()
    ));
    assert!(is_dag_value(
        &graph
            .analyze(None, is_dag_options(true, Some("KNOWS")))
            .unwrap()
    ));
    assert!(!is_dag_value(
        &graph
            .analyze(Some("Person"), is_dag_options(false, None))
            .unwrap()
    ));
}

#[test]
fn is_dag_handles_empty_self_loop_and_invalid_inputs() {
    let empty = GraphForge::new(None).unwrap();
    assert!(is_dag_value(
        &empty.analyze(None, AnalyzeOptions::default()).unwrap()
    ));
    assert!(is_dag_value(
        &empty
            .analyze(Some("Missing"), AnalyzeOptions::default())
            .unwrap()
    ));

    let looped = GraphForge::new(None).unwrap();
    add_person(&looped, "Alice");
    looped
        .execute("MATCH (a:Person {name:'Alice'}) CREATE (a)-[:KNOWS]->(a)")
        .unwrap();
    assert!(!is_dag_value(
        &looped.analyze(None, AnalyzeOptions::default()).unwrap()
    ));

    for result in [
        empty.analyze(Some(""), AnalyzeOptions::default()),
        empty.analyze(Some(" Person"), AnalyzeOptions::default()),
        empty.analyze(None, is_dag_options(true, Some(" "))),
        empty.analyze(
            None,
            AnalyzeOptions {
                by: AnalyzeAlgorithm::MinimumSpanningTree,
                ..AnalyzeOptions::default()
            },
        ),
    ] {
        assert!(matches!(result, Err(GfError::Validation(_))));
    }
}

#[test]
fn euler_path_persisted_e2e_is_uuid_exact_directed_and_undirected() {
    let dir = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
    let nodes = ["Alice", "Bob", "Carol"].map(|name| add_person(&graph, name));
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}) \
                 CREATE (a)-[:TRAIL]->(b), (b)-[:TRAIL]->(b), (b)-[:TRAIL]->(c)",
        )
        .unwrap();
    drop(graph);

    let graph = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
    let relationships = relationship_rows(&graph, "TRAIL");
    let mut expected_edges = relationships.iter().map(|row| row.0).collect::<Vec<_>>();
    expected_edges.sort_unstable();
    for directed in [true, false] {
        let options = euler_options(AnalyzeAlgorithm::EulerPath, directed, Some("TRAIL"));
        let batch = graph.analyze(Some("Person"), options.clone()).unwrap();
        assert_euler_schema(&batch, "euler_path");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch, graph.analyze(Some("Person"), options).unwrap());

        let node_path = euler_uuid_list(&batch, "node_path");
        let edge_path = euler_uuid_list(&batch, "edge_path");
        assert_eq!(
            node_path,
            vec![
                *nodes[0].uuid.as_bytes(),
                *nodes[1].uuid.as_bytes(),
                *nodes[1].uuid.as_bytes(),
                *nodes[2].uuid.as_bytes(),
            ]
        );
        assert_eq!(edge_path, expected_edges);
        assert_euler_edge_alignment(&node_path, &edge_path, &relationships, directed);
    }
}

#[test]
fn euler_circuit_persisted_e2e_preserves_loops_parallel_edges_and_start() {
    let dir = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
    let nodes = ["Alice", "Bob"].map(|name| add_person(&graph, name));
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}) \
                 CREATE (a)-[:TRAIL]->(b), (a)-[:TRAIL]->(b), (a)-[:TRAIL]->(a)",
        )
        .unwrap();
    drop(graph);

    let graph = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
    let options = euler_options(AnalyzeAlgorithm::EulerCircuit, false, Some("TRAIL"));
    let batch = graph.analyze(Some("Person"), options.clone()).unwrap();
    assert_euler_schema(&batch, "euler_circuit");
    assert_eq!(batch, graph.analyze(Some("Person"), options).unwrap());
    let node_path = euler_uuid_list(&batch, "node_path");
    let edge_path = euler_uuid_list(&batch, "edge_path");
    let relationships = relationship_rows(&graph, "TRAIL");
    let lowest = nodes
        .iter()
        .map(|node| *node.uuid.as_bytes())
        .min()
        .unwrap();
    assert_eq!(node_path.first(), Some(&lowest));
    assert_eq!(node_path.last(), Some(&lowest));
    assert_euler_edge_alignment(&node_path, &edge_path, &relationships, false);
    let canonical_first = relationships
        .iter()
        .filter(|(_, source, target)| *source == lowest || *target == lowest)
        .map(|(edge, _, _)| *edge)
        .min()
        .unwrap();
    assert_eq!(edge_path.first(), Some(&canonical_first));

    let directed = GraphForge::new(None).unwrap();
    let directed_nodes = ["A", "B"].map(|name| add_person(&directed, name));
    directed
        .execute(
            "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}) \
                 CREATE (a)-[:ARC]->(b), (b)-[:ARC]->(a), (a)-[:ARC]->(a)",
        )
        .unwrap();
    let directed_options = euler_options(AnalyzeAlgorithm::EulerCircuit, true, Some("ARC"));
    let directed_batch = directed
        .analyze(Some("Person"), directed_options.clone())
        .unwrap();
    assert_euler_schema(&directed_batch, "euler_circuit");
    assert_eq!(
        directed_batch,
        directed.analyze(Some("Person"), directed_options).unwrap()
    );
    let directed_path = euler_uuid_list(&directed_batch, "node_path");
    let directed_edges = euler_uuid_list(&directed_batch, "edge_path");
    let directed_relationships = relationship_rows(&directed, "ARC");
    let directed_lowest = directed_nodes
        .iter()
        .map(|node| *node.uuid.as_bytes())
        .min()
        .unwrap();
    assert_eq!(directed_path.first(), Some(&directed_lowest));
    assert_eq!(directed_path.last(), Some(&directed_lowest));
    assert_euler_edge_alignment(
        &directed_path,
        &directed_edges,
        &directed_relationships,
        true,
    );
    let canonical_first = directed_relationships
        .iter()
        .filter(|(_, source, _)| *source == directed_lowest)
        .map(|(edge, _, _)| *edge)
        .min()
        .unwrap();
    assert_eq!(directed_edges.first(), Some(&canonical_first));
}

#[test]
fn euler_construction_boundaries_and_undefined_results_are_typed() {
    let empty = GraphForge::new(None).unwrap();
    for by in [AnalyzeAlgorithm::EulerCircuit, AnalyzeAlgorithm::EulerPath] {
        assert_eq!(
            empty
                .analyze(None, euler_options(by, false, None))
                .unwrap()
                .num_rows(),
            0
        );
        let edgeless = GraphForge::new(None).unwrap();
        let isolated = add_person(&edgeless, "Isolated");
        let batch = edgeless
            .analyze(Some("Person"), euler_options(by, false, None))
            .unwrap();
        assert_eq!(
            euler_uuid_list(&batch, "node_path"),
            vec![*isolated.uuid.as_bytes()]
        );
        assert!(euler_uuid_list(&batch, "edge_path").is_empty());
    }

    let circuit_undefined = GraphForge::new(None).unwrap();
    circuit_undefined
        .execute("CREATE (:Person)-[:TRAIL]->(:Person)")
        .unwrap();
    assert!(matches!(
        circuit_undefined
            .analyze(
                Some("Person"),
                euler_options(AnalyzeAlgorithm::EulerCircuit, false, Some("TRAIL")),
            )
            .map_err(expect_algorithm_execution),
        Err(diagnostic @ AlgorithmError::UndefinedEulerCircuit) if diagnostic.to_string() == "Euler circuit is undefined for the selected graph"
    ));

    let path_undefined = GraphForge::new(None).unwrap();
    path_undefined
        .execute(
            "CREATE (a:Person), (b:Person), (c:Person), (d:Person) \
                 CREATE (a)-[:TRAIL]->(b), (a)-[:TRAIL]->(c), (a)-[:TRAIL]->(d)",
        )
        .unwrap();
    assert!(matches!(
        path_undefined
            .analyze(
                Some("Person"),
                euler_options(AnalyzeAlgorithm::EulerPath, false, Some("TRAIL")),
            )
            .map_err(expect_algorithm_execution),
        Err(diagnostic @ AlgorithmError::UndefinedEulerPath) if diagnostic.to_string() == "Euler path is undefined for the selected graph"
    ));
}

#[test]
fn has_euler_circuit_returns_typed_boolean_for_undirected_and_directed_graphs() {
    // Exploratory mode has no ontology/knowledge layer (#772).
    let graph = GraphForge::new(None).unwrap();
    for name in ["Alice", "Bob", "Carol", "Isolate"] {
        add_person(&graph, name);
    }
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}) \
                 CREATE (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), (c)-[:KNOWS]->(a)",
        )
        .unwrap();

    let undirected = graph
        .analyze(
            Some("Person"),
            has_euler_circuit_options(false, Some("KNOWS")),
        )
        .unwrap();
    assert_eq!(undirected.num_rows(), 1);
    assert_eq!(undirected.num_columns(), 1);
    assert_eq!(undirected.schema().field(0).data_type(), &DataType::Boolean);
    assert!(!undirected.schema().field(0).is_nullable());
    assert_eq!(
        undirected.schema().metadata()["graphforge.algorithm"],
        "has_euler_circuit"
    );
    assert!(has_euler_circuit_value(&undirected));

    assert!(has_euler_circuit_value(
        &graph
            .analyze(
                Some("Person"),
                has_euler_circuit_options(true, Some("KNOWS")),
            )
            .unwrap()
    ));

    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}) \
                 CREATE (a)-[:PATH]->(b)",
        )
        .unwrap();
    assert!(!has_euler_circuit_value(
        &graph
            .analyze(None, has_euler_circuit_options(false, Some("PATH")))
            .unwrap()
    ));
    assert!(!has_euler_circuit_value(
        &graph
            .analyze(None, has_euler_circuit_options(true, Some("PATH")))
            .unwrap()
    ));
}

#[test]
fn has_euler_circuit_empty_and_missing_selection_are_non_null_true() {
    let graph = GraphForge::new(None).unwrap();
    for batch in [
        graph
            .analyze(None, has_euler_circuit_options(false, None))
            .unwrap(),
        graph
            .analyze(
                Some("Missing"),
                has_euler_circuit_options(true, Some("MISSING")),
            )
            .unwrap(),
    ] {
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Boolean);
        assert!(!batch.schema().field(0).is_nullable());
        assert!(has_euler_circuit_value(&batch));
    }
}

#[test]
fn has_euler_path_returns_typed_boolean_for_undirected_and_directed_graphs() {
    // Exploratory mode has no ontology/knowledge layer (#772).
    let graph = GraphForge::new(None).unwrap();
    for name in ["Alice", "Bob", "Carol", "Dan", "Isolate"] {
        add_person(&graph, name);
    }
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}) \
                 CREATE (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c)",
        )
        .unwrap();

    let undirected = graph
        .analyze(Some("Person"), has_euler_path_options(false, Some("KNOWS")))
        .unwrap();
    assert_eq!(undirected.num_rows(), 1);
    assert_eq!(undirected.num_columns(), 1);
    assert_eq!(undirected.schema().field(0).name(), "has_euler_path");
    assert_eq!(undirected.schema().field(0).data_type(), &DataType::Boolean);
    assert!(!undirected.schema().field(0).is_nullable());
    assert_eq!(
        undirected.schema().metadata()["graphforge.algorithm"],
        "has_euler_path"
    );
    assert_eq!(undirected.schema().metadata()["graphforge.verb"], "analyze");
    assert!(has_euler_path_value(&undirected));
    assert_eq!(
        undirected,
        graph
            .analyze(Some("Person"), has_euler_path_options(false, Some("KNOWS")),)
            .unwrap()
    );

    assert!(has_euler_path_value(
        &graph
            .analyze(Some("Person"), has_euler_path_options(true, Some("KNOWS")),)
            .unwrap()
    ));

    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}) \
                 CREATE (a)-[:STAR]->(b), (a)-[:STAR]->(c), (a)-[:STAR]->(d)",
        )
        .unwrap();
    assert!(!has_euler_path_value(
        &graph
            .analyze(None, has_euler_path_options(false, Some("STAR")))
            .unwrap()
    ));
    assert!(!has_euler_path_value(
        &graph
            .analyze(None, has_euler_path_options(true, Some("STAR")))
            .unwrap()
    ));
}

#[test]
fn has_euler_path_empty_and_missing_selection_are_non_null_true() {
    let graph = GraphForge::new(None).unwrap();
    for batch in [
        graph
            .analyze(None, has_euler_path_options(false, None))
            .unwrap(),
        graph
            .analyze(
                Some("Missing"),
                has_euler_path_options(true, Some("MISSING")),
            )
            .unwrap(),
    ] {
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Boolean);
        assert!(!batch.schema().field(0).is_nullable());
        assert!(has_euler_path_value(&batch));
    }
}

#[test]
fn topological_sort_returns_exact_uuid_order_schema_and_projection() {
    // Exploratory mode has no ontology/knowledge layer (#772).
    let graph = GraphForge::new(None).unwrap();
    let mut people = ["Alice", "Bob", "Carol", "Dan"]
        .map(|name| (name, add_person(&graph, name)))
        .to_vec();
    people.sort_unstable_by_key(|(_, node)| *node.uuid.as_bytes());
    for name in ["Fox", "Wolf"] {
        graph
            .add_node(
                "Animal",
                &HashMap::from([("name".to_owned(), PropValue::Str(name.to_owned()))]),
            )
            .unwrap();
    }
    graph
        .execute(&format!(
            "MATCH (a:Person {{name:'{}'}}), (b:Person {{name:'{}'}}), \
                 (c:Person {{name:'{}'}}), (f:Animal {{name:'Fox'}}), \
                 (w:Animal {{name:'Wolf'}}) \
                 CREATE (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(c), \
                 (b)-[:KNOWS]->(c), (f)-[:OTHER]->(w), (w)-[:OTHER]->(f)",
            people[0].0, people[1].0, people[2].0
        ))
        .unwrap();

    let options = topological_sort_options(true, None);
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
            ("node_uuid", &DataType::FixedSizeBinary(16), false),
            ("order", &DataType::UInt64, false),
        ]
    );
    assert_eq!(
        batch.schema().metadata()["graphforge.algorithm"],
        "topological_sort"
    );
    assert_eq!(batch.schema().metadata()["graphforge.verb"], "analyze");
    assert!(batch.column_by_name("node_id").is_none());
    assert_eq!(
        topological_rows(&batch),
        people
            .iter()
            .enumerate()
            .map(|(order, (_, node))| (*node.uuid.as_bytes(), u64::try_from(order).unwrap()))
            .collect::<Vec<_>>()
    );
    assert_eq!(batch, graph.analyze(Some("Person"), options).unwrap());

    let via = graph
        .analyze(None, topological_sort_options(true, Some("KNOWS")))
        .unwrap();
    assert_eq!(via.num_rows(), 6);
    assert_eq!(
        via.column_by_name("order")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .values(),
        &[0, 1, 2, 3, 4, 5]
    );
    assert!(matches!(
        graph.analyze(None, topological_sort_options(true, None)).map_err(expect_algorithm_execution),
        Err(AlgorithmError::Execution { message })
            if message == "selected graph contains a cycle"
    ));
}

#[test]
fn topological_sort_handles_empty_self_loop_and_invalid_options() {
    let empty = GraphForge::new(None).unwrap();
    let empty_batch = empty
        .analyze(None, topological_sort_options(true, None))
        .unwrap();
    assert_eq!(empty_batch.num_rows(), 0);
    assert_eq!(
        empty_batch.schema().field(0).data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert_eq!(empty_batch.schema().field(1).data_type(), &DataType::UInt64);
    assert_eq!(
        empty
            .analyze(Some("Missing"), topological_sort_options(true, None))
            .unwrap()
            .schema(),
        empty_batch.schema()
    );

    let looped = GraphForge::new(None).unwrap();
    add_person(&looped, "Alice");
    looped
        .execute("MATCH (a:Person {name:'Alice'}) CREATE (a)-[:KNOWS]->(a)")
        .unwrap();
    assert!(matches!(
        looped.analyze(None, topological_sort_options(true, None)).map_err(expect_algorithm_execution),
        Err(AlgorithmError::Execution { message })
            if message == "selected graph contains a cycle"
    ));

    assert!(matches!(
        empty.analyze(None, topological_sort_options(false, None)),
        Err(GfError::Validation(message))
            if message == "topological_sort requires directed=true"
    ));
    assert!(matches!(
        empty.analyze(
            None,
            AnalyzeOptions {
                weight: Some("cost".into()),
                ..topological_sort_options(true, None)
            }
        ),
        Err(GfError::Validation(message))
            if message == "topological_sort does not accept an edge weight property"
    ));
    for result in [
        empty.analyze(Some(""), topological_sort_options(true, None)),
        empty.analyze(None, topological_sort_options(true, Some(" "))),
    ] {
        assert!(matches!(result, Err(GfError::Validation(_))));
    }
}

#[test]
fn dag_longest_path_returns_exact_uuid_path_schema_and_isolation() {
    // Exploratory mode has no ontology/knowledge layer (#772).
    let graph = GraphForge::new(None).unwrap();
    let nodes =
        ["Alice", "Bob", "Carol", "Dan", "Eve"].map(|name| (name, add_person(&graph, name)));
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (e:Person {name:'Eve'}) \
                 CREATE (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(c), \
                 (b)-[:KNOWS]->(d), (c)-[:KNOWS]->(d), \
                 (a)-[:OTHER]->(e)",
        )
        .unwrap();

    let options = dag_longest_path_options(true, Some("KNOWS"));
    let batch = graph.analyze(Some("Person"), options.clone()).unwrap();
    assert_eq!(batch.num_rows(), 1);
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
            ("cost", &DataType::Float64, false),
            (
                "path",
                &DataType::List(Arc::new(arrow::datatypes::Field::new(
                    "item",
                    DataType::FixedSizeBinary(16),
                    false,
                ))),
                false,
            ),
        ]
    );
    assert_eq!(
        batch.schema().metadata()["graphforge.algorithm"],
        "dag_longest_path"
    );
    assert_eq!(batch.schema().metadata()["graphforge.verb"], "analyze");
    let costs = batch
        .column_by_name("cost")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(costs.null_count(), 0);
    assert_eq!(costs.values(), &[2.0]);
    let alice = &nodes[0].1;
    let bob = &nodes[1].1;
    let carol = &nodes[2].1;
    let dan = &nodes[3].1;
    let expected_middle = if bob.uuid < carol.uuid { bob } else { carol };
    assert_eq!(
        uuid_path(&batch, 0),
        [
            *alice.uuid.as_bytes(),
            *expected_middle.uuid.as_bytes(),
            *dan.uuid.as_bytes(),
        ]
    );
    assert_eq!(batch, graph.analyze(Some("Person"), options).unwrap());
}

#[test]
fn dag_longest_path_handles_typed_empty_cycle_and_invalid_options() {
    let empty = GraphForge::new(None).unwrap();
    let batch = empty
        .analyze(None, dag_longest_path_options(true, None))
        .unwrap();
    assert_eq!(batch.num_rows(), 1);
    let costs = batch
        .column_by_name("cost")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(costs.null_count(), 0);
    assert_eq!(costs.value(0), 0.0);
    let paths = batch
        .column_by_name("path")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(paths.null_count(), 0);
    assert!(uuid_path(&batch, 0).is_empty());
    assert_eq!(
        empty
            .analyze(Some("Missing"), dag_longest_path_options(true, None))
            .unwrap()
            .schema(),
        batch.schema()
    );

    let cyclic = GraphForge::new(None).unwrap();
    for name in ["Alice", "Bob"] {
        add_person(&cyclic, name);
    }
    cyclic
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}) \
                 CREATE (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(a)",
        )
        .unwrap();
    assert!(matches!(
        cyclic.analyze(None, dag_longest_path_options(true, None)).map_err(expect_algorithm_execution),
        Err(AlgorithmError::Execution { message })
            if message.contains("dag_longest_path requires a directed acyclic graph")
    ));
    assert!(matches!(
        empty.analyze(None, dag_longest_path_options(false, None)),
        Err(GfError::Validation(message))
            if message == "dag_longest_path requires directed=true"
    ));
    assert!(matches!(
        empty.analyze(
            None,
            AnalyzeOptions {
                weight: Some("cost".into()),
                ..dag_longest_path_options(true, None)
            }
        ),
        Err(GfError::Validation(message))
            if message == "dag_longest_path does not accept an edge weight property"
    ));
}

#[test]
fn weighted_dag_longest_path_is_exact_typed_and_knowledge_independent() {
    // Exploratory mode has no ontology/knowledge layer (#772).
    let graph = GraphForge::new(None).unwrap();
    let nodes =
        ["Alice", "Bob", "Carol", "Dan", "Eve"].map(|name| (name, add_person(&graph, name)));
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (e:Person {name:'Eve'}) \
                 CREATE (a)-[:ROAD {cost:2.0}]->(b), \
                 (b)-[:ROAD {cost:3.0}]->(d), \
                 (a)-[:ROAD {cost:2.0}]->(c), \
                 (c)-[:ROAD {cost:3.0}]->(d), \
                 (e)-[:ROAD {cost:-8.0}]->(d), \
                 (a)-[:OTHER {cost:100.0}]->(d)",
        )
        .unwrap();

    let options = weighted_dag_longest_path_options(true, Some("ROAD"), Some("cost"));
    let batch = graph.analyze(Some("Person"), options.clone()).unwrap();
    assert_eq!(batch.num_rows(), 1);
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
            ("cost", &DataType::Float64, false),
            (
                "path",
                &DataType::List(Arc::new(arrow::datatypes::Field::new(
                    "item",
                    DataType::FixedSizeBinary(16),
                    false,
                ))),
                false,
            ),
        ]
    );
    assert_eq!(
        batch.schema().metadata()["graphforge.algorithm"],
        "dag_longest_path_weighted"
    );
    assert_eq!(batch.schema().metadata()["graphforge.verb"], "analyze");
    let costs = batch
        .column_by_name("cost")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(costs.null_count(), 0);
    assert_eq!(costs.value(0), 5.0);
    let alice = &nodes[0].1;
    let bob = &nodes[1].1;
    let carol = &nodes[2].1;
    let dan = &nodes[3].1;
    let expected_middle = if bob.uuid < carol.uuid { bob } else { carol };
    assert_eq!(
        uuid_path(&batch, 0),
        [
            *alice.uuid.as_bytes(),
            *expected_middle.uuid.as_bytes(),
            *dan.uuid.as_bytes(),
        ]
    );
    assert_eq!(batch, graph.analyze(Some("Person"), options).unwrap());
}

#[test]
fn weighted_dag_longest_path_handles_empty_cycle_and_strict_weights() {
    let empty = GraphForge::new(None).unwrap();
    let options = weighted_dag_longest_path_options(true, None, Some("cost"));
    let batch = empty.analyze(None, options).unwrap();
    assert_eq!(batch.num_rows(), 1);
    let costs = batch
        .column_by_name("cost")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(costs.null_count(), 0);
    assert_eq!(costs.value(0), 0.0);
    let paths = batch
        .column_by_name("path")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(paths.null_count(), 0);
    assert!(uuid_path(&batch, 0).is_empty());

    let cyclic = GraphForge::new(None).unwrap();
    for name in ["Alice", "Bob"] {
        add_person(&cyclic, name);
    }
    cyclic
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}) \
                 CREATE (a)-[:ROAD {cost:1.0}]->(b), \
                 (b)-[:ROAD {cost:1.0}]->(a)",
        )
        .unwrap();
    assert!(matches!(
        cyclic.analyze(
            None,
            weighted_dag_longest_path_options(true, Some("ROAD"), Some("cost"))
        ).map_err(expect_algorithm_execution),
        Err(AlgorithmError::Execution { message })
            if message.contains("requires a directed acyclic graph")
    ));
    for options in [
        weighted_dag_longest_path_options(false, None, Some("cost")),
        weighted_dag_longest_path_options(true, None, None),
        weighted_dag_longest_path_options(true, None, Some(" ")),
        weighted_dag_longest_path_options(true, Some("ROAD"), Some("missing")),
    ] {
        assert!(matches!(
            cyclic.analyze(None, options),
            Err(GfError::Validation(_))
        ));
    }
}

#[test]
fn edge_coloring_is_uuid_only_deterministic_and_knowledge_independent() {
    // Exploratory mode has no ontology/knowledge layer (#772).
    let graph = GraphForge::new(None).unwrap();
    for name in ["Alice", "Bob", "Fox"] {
        add_person(&graph, name);
    }
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (f:Person {name:'Fox'}) \
                 CREATE (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), \
                 (b)-[:KNOWS]->(a), (a)-[:OTHER]->(f)",
        )
        .unwrap();

    let options = edge_coloring_options(Some("KNOWS"));
    let batch = graph.analyze(Some("Person"), options.clone()).unwrap();
    assert_eq!(batch.num_rows(), 3);
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
            ("color", &DataType::UInt64, false),
        ]
    );
    assert_eq!(
        batch.schema().metadata()["graphforge.algorithm"],
        "edge_coloring"
    );
    assert_eq!(batch.schema().metadata()["graphforge.verb"], "analyze");
    assert!(batch.column_by_name("edge_id").is_none());

    let rows = edge_color_rows(&batch);
    assert!(rows.windows(2).all(|pair| pair[0].0 < pair[1].0));
    assert_eq!(
        rows.iter().map(|(_, color)| *color).collect::<Vec<_>>(),
        [0, 1, 2]
    );
    assert_eq!(batch, graph.analyze(Some("Person"), options).unwrap());
    assert_eq!(
        graph
            .analyze(Some("Person"), edge_coloring_options(Some("MISSING")))
            .unwrap()
            .num_rows(),
        0
    );
}

#[test]
fn edge_coloring_handles_typed_empty_loops_and_invalid_options() {
    let empty = GraphForge::new(None).unwrap();
    let batch = empty.analyze(None, edge_coloring_options(None)).unwrap();
    assert_eq!(batch.num_rows(), 0);
    assert_eq!(batch.num_columns(), 2);
    assert_eq!(
        empty
            .analyze(Some("Missing"), edge_coloring_options(None))
            .unwrap()
            .schema(),
        batch.schema()
    );
    assert_eq!(
        batch.schema().field(0).data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert_eq!(batch.schema().field(1).data_type(), &DataType::UInt64);
    assert!(!batch.schema().field(0).is_nullable());
    assert!(!batch.schema().field(1).is_nullable());
    assert_eq!(
        batch.schema().metadata()["graphforge.algorithm"],
        "edge_coloring"
    );

    let looped = GraphForge::new(None).unwrap();
    add_person(&looped, "Alice");
    looped
        .execute("MATCH (a:Person {name:'Alice'}) CREATE (a)-[:KNOWS]->(a)")
        .unwrap();
    assert!(matches!(
        looped.analyze(None, edge_coloring_options(Some("KNOWS"))).map_err(expect_algorithm_execution),
        Err(AlgorithmError::Execution { message })
            if message.contains("edge_coloring cannot color a graph containing a self-loop")
    ));

    for options in [
        AnalyzeOptions {
            directed: true,
            ..edge_coloring_options(None)
        },
        AnalyzeOptions {
            weight: Some("cost".into()),
            ..edge_coloring_options(None)
        },
        edge_coloring_options(Some(" ")),
    ] {
        assert!(matches!(
            empty.analyze(None, options),
            Err(GfError::Validation(_))
        ));
    }
}
