use super::*;

fn random_walk_options(k: usize, walk_length: usize, seed: u64) -> PathsOptions {
    PathsOptions {
        by: PathAlgorithm::RandomWalk,
        directed: true,
        k,
        via: Some("KNOWS".into()),
        weight: None,
        capacity_property: None,
        cost_property: None,
        heuristic: None,
        walk_length: Some(walk_length),
        seed: Some(seed),
        terminal_uuids: Vec::new(),
        prize_property: None,
    }
}

fn dfs_options(directed: bool, via: Option<&str>) -> PathsOptions {
    PathsOptions {
        by: PathAlgorithm::Dfs,
        directed,
        k: 1,
        via: via.map(str::to_owned),
        weight: None,
        capacity_property: None,
        cost_property: None,
        heuristic: None,
        walk_length: None,
        seed: None,
        terminal_uuids: Vec::new(),
        prize_property: None,
    }
}

fn transitive_closure_options(directed: bool, via: Option<&str>) -> PathsOptions {
    PathsOptions {
        by: PathAlgorithm::TransitiveClosure,
        directed,
        k: 1,
        via: via.map(str::to_owned),
        weight: None,
        capacity_property: None,
        cost_property: None,
        heuristic: None,
        walk_length: None,
        seed: None,
        terminal_uuids: Vec::new(),
        prize_property: None,
    }
}

fn uuid_walk(batch: &arrow::record_batch::RecordBatch, row: usize) -> Vec<[u8; 16]> {
    let walks = batch
        .column_by_name("walk")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let values = walks.value(row);
    let values = values
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    (0..values.len())
        .map(|index| values.value(index).try_into().unwrap())
        .collect()
}

fn uuid_pairs(batch: &arrow::record_batch::RecordBatch) -> Vec<([u8; 16], [u8; 16])> {
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
    (0..batch.num_rows())
        .map(|row| {
            (
                sources.value(row).try_into().unwrap(),
                targets.value(row).try_into().unwrap(),
            )
        })
        .collect()
}

#[test]
fn bfs_obeys_uuid_schema_target_via_direction_and_order_contracts() {
    let graph = GraphForge::new(None).unwrap();
    let nodes = ["Alice", "Bob", "Carol", "Dan", "Eve"].map(|name| add_person(&graph, name));
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (e:Person {name:'Eve'}) \
                 CREATE (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(b), \
                 (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(a), \
                 (b)-[:KNOWS]->(d), (c)-[:KNOWS]->(d), (d)-[:OTHER]->(e)",
        )
        .unwrap();

    let source = NodeSelector::Handle(nodes[0].clone());
    let all = graph
        .paths(&source, None, bfs_options(true, Some("KNOWS")))
        .unwrap();
    assert_eq!(
        all.schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["source_uuid", "target_uuid", "cost", "path"]
    );
    assert_eq!(
        all.schema().field(0).data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert_eq!(
        all.schema().field(1).data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert_eq!(all.schema().field(2).data_type(), &DataType::Float64);
    assert!(matches!(
        all.schema().field(3).data_type(),
        DataType::List(field) if field.data_type() == &DataType::FixedSizeBinary(16)
    ));
    assert_eq!(all.schema().metadata()["graphforge.algorithm"], "bfs");
    assert!(all.column_by_name("source_id").is_none());
    let sources = all
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let targets = all
        .column(1)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    for row in 0..all.num_rows() {
        assert_eq!(sources.value(row), nodes[0].uuid.as_bytes());
    }
    assert_eq!(
        (0..all.num_rows())
            .map(|row| targets.value(row))
            .collect::<Vec<_>>(),
        nodes[..4]
            .iter()
            .map(|node| node.uuid.as_bytes())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        all.column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values(),
        &[0.0, 1.0, 1.0, 2.0]
    );
    assert_eq!(
        uuid_path(&all, 3),
        [nodes[0].uuid, nodes[1].uuid, nodes[3].uuid].map(|uuid| *uuid.as_bytes())
    );
    assert_eq!(
        all,
        graph
            .paths(&source, None, bfs_options(true, Some("KNOWS")))
            .unwrap()
    );

    let dan = NodeSelector::Handle(nodes[3].clone());
    let targeted = graph
        .paths(&source, Some(&dan), bfs_options(true, Some("KNOWS")))
        .unwrap();
    assert_eq!(targeted.num_rows(), 1);
    assert_eq!(uuid_path(&targeted, 0), uuid_path(&all, 3));
    let reverse = graph
        .paths(&dan, Some(&source), bfs_options(false, Some("KNOWS")))
        .unwrap();
    assert_eq!(
        uuid_path(&reverse, 0),
        [nodes[3].uuid, nodes[1].uuid, nodes[0].uuid].map(|uuid| *uuid.as_bytes())
    );
    let eve = NodeSelector::Handle(nodes[4].clone());
    assert_eq!(
        graph
            .paths(&dan, Some(&eve), bfs_options(true, Some("OTHER")))
            .unwrap()
            .num_rows(),
        1
    );
    assert_eq!(
        graph
            .paths(&source, Some(&eve), bfs_options(true, Some("KNOWS")))
            .unwrap()
            .num_rows(),
        0
    );
}

#[test]
fn bfs_singleton_and_invalid_inputs_are_structured() {
    let graph = GraphForge::new(None).unwrap();
    let alice = add_person(&graph, "Alice");
    let source = NodeSelector::Handle(alice);
    let singleton = graph.paths(&source, None, bfs_options(true, None)).unwrap();
    assert_eq!(singleton.num_rows(), 1);
    assert_eq!(uuid_path(&singleton, 0).len(), 1);

    let invalid = [
        PathsOptions {
            k: 2,
            ..bfs_options(true, None)
        },
        PathsOptions {
            weight: Some("distance".into()),
            ..bfs_options(true, None)
        },
        PathsOptions {
            via: Some(" ".into()),
            ..bfs_options(true, None)
        },
        PathsOptions {
            by: PathAlgorithm::AStar,
            ..bfs_options(true, None)
        },
    ];
    for options in invalid {
        assert!(matches!(
            graph.paths(&source, None, options),
            Err(GfError::Validation(_))
        ));
    }
    assert!(matches!(
        graph.paths(
            &NodeSelector::Uuid(graphforge_core::uuid::new_v7()),
            None,
            bfs_options(true, None),
        ),
        Err(GfError::Validation(_))
    ));
}

#[test]
fn random_walk_is_seeded_uuid_only_and_resource_bounded_through_public_api() {
    let graph = GraphForge::new(None).unwrap();
    let nodes = ["Alice", "Bob", "Carol", "Dan"].map(|name| add_person(&graph, name));
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}) \
                 CREATE (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(b), \
                 (b)-[:KNOWS]->(d), (c)-[:KNOWS]->(d)",
        )
        .unwrap();
    let source = NodeSelector::Handle(nodes[0].clone());
    let options = random_walk_options(2, 3, 42);
    let result = graph.paths(&source, None, options.clone()).unwrap();

    assert_eq!(result, graph.paths(&source, None, options).unwrap());
    assert_eq!(result.num_rows(), 2);
    assert_eq!(
        result
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["start_uuid", "walk"]
    );
    assert_eq!(
        result.schema().field(0).data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert!(matches!(
        result.schema().field(1).data_type(),
        DataType::List(field)
            if field.data_type() == &DataType::FixedSizeBinary(16) && !field.is_nullable()
    ));
    assert_eq!(
        result.schema().metadata()["graphforge.algorithm"],
        "random_walk"
    );
    assert!(result.column_by_name("node_id").is_none());
    let starts = result
        .column_by_name("start_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert!((0..2).all(|row| starts.value(row) == nodes[0].uuid.as_bytes()));

    let mut middle = [*nodes[1].uuid.as_bytes(), *nodes[2].uuid.as_bytes()];
    middle.sort_unstable();
    assert_eq!(
        uuid_walk(&result, 0),
        [
            *nodes[0].uuid.as_bytes(),
            middle[1],
            *nodes[3].uuid.as_bytes()
        ]
    );
    assert_eq!(
        uuid_walk(&result, 1),
        [
            *nodes[0].uuid.as_bytes(),
            middle[0],
            *nodes[3].uuid.as_bytes()
        ]
    );

    let zero = graph
        .paths(&source, None, random_walk_options(1, 0, 42))
        .unwrap();
    assert_eq!(uuid_walk(&zero, 0), [*nodes[0].uuid.as_bytes()]);

    assert!(matches!(
        graph.paths(&source, None, random_walk_options(0, 3, 42)),
        Err(GfError::Validation(message)) if message.contains("at least 1")
    ));
    assert!(matches!(
        graph.paths(
            &source,
            Some(&NodeSelector::Handle(nodes[3].clone())),
            random_walk_options(1, 3, 42),
        ),
        Err(GfError::Validation(message)) if message.contains("target selector")
    ));
    assert!(matches!(
        graph
            .paths(&source, None, random_walk_options(101, 100, 42))
            .map_err(expect_algorithm_execution),
        Err(diagnostic @ AlgorithmError::IterationLimit {
            observed: 10_100,
            limit: 10_000
        }) if diagnostic.to_string() == "algorithm iteration limit exceeded: observed 10100, limit 10000"
    ));
}

#[test]
fn dfs_is_uuid_only_deterministic_and_obeys_direction_and_relation_filters() {
    let graph = GraphForge::new(None).unwrap();
    let nodes = ["Alice", "Bob", "Carol", "Dan", "Eve"].map(|name| add_person(&graph, name));
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (e:Person {name:'Eve'}) \
                 CREATE (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(b), \
                 (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(a), \
                 (b)-[:KNOWS]->(d), (c)-[:KNOWS]->(d), (a)-[:OTHER]->(e)",
        )
        .unwrap();

    let source = NodeSelector::Handle(nodes[0].clone());
    let traversal = graph
        .paths(&source, None, dfs_options(true, Some("KNOWS")))
        .unwrap();
    assert_eq!(
        traversal
            .schema()
            .fields()
            .iter()
            .map(|field| (field.name().as_str(), field.data_type()))
            .collect::<Vec<_>>(),
        [
            ("node_uuid", &DataType::FixedSizeBinary(16)),
            ("depth", &DataType::UInt64),
            ("order", &DataType::UInt64),
        ]
    );
    assert_eq!(traversal.schema().metadata()["graphforge.algorithm"], "dfs");
    assert!(traversal.column_by_name("node_id").is_none());
    let uuids = traversal
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(
        (0..traversal.num_rows())
            .map(|row| uuids.value(row))
            .collect::<Vec<_>>(),
        [0, 1, 3, 2]
            .map(|index| nodes[index].uuid.as_bytes())
            .to_vec()
    );
    assert_eq!(
        traversal
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .values(),
        &[0, 1, 2, 1]
    );
    assert_eq!(
        traversal
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .values(),
        &[0, 1, 2, 3]
    );
    assert_eq!(
        traversal,
        graph
            .paths(&source, None, dfs_options(true, Some("KNOWS")))
            .unwrap()
    );

    let dan = NodeSelector::Handle(nodes[3].clone());
    assert_eq!(
        graph
            .paths(&dan, None, dfs_options(true, Some("KNOWS")))
            .unwrap()
            .num_rows(),
        1
    );
    assert_eq!(
        graph
            .paths(&dan, None, dfs_options(false, Some("KNOWS")))
            .unwrap()
            .num_rows(),
        4
    );
    let other = graph
        .paths(&source, None, dfs_options(true, Some("OTHER")))
        .unwrap();
    assert_eq!(other.num_rows(), 2);
    assert_eq!(
        other
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(1),
        nodes[4].uuid.as_bytes()
    );
}

#[test]
fn dfs_rejects_target_weight_k_and_malformed_relation_options() {
    let graph = GraphForge::new(None).unwrap();
    let nodes = ["Alice", "Bob"].map(|name| add_person(&graph, name));
    let source = NodeSelector::Handle(nodes[0].clone());
    let target = NodeSelector::Handle(nodes[1].clone());
    assert!(matches!(
        graph.paths(&source, Some(&target), dfs_options(true, None),),
        Err(GfError::Validation(_))
    ));
    for options in [
        PathsOptions {
            k: 2,
            ..dfs_options(true, None)
        },
        PathsOptions {
            weight: Some("distance".into()),
            ..dfs_options(true, None)
        },
        PathsOptions {
            via: Some(" ".into()),
            ..dfs_options(true, None)
        },
    ] {
        assert!(matches!(
            graph.paths(&source, None, options),
            Err(GfError::Validation(_))
        ));
    }
}

#[test]
fn transitive_closure_is_global_uuid_ordered_and_knowledge_independent() {
    // Exploratory mode has no ontology or knowledge sidecars.
    let dir = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
    let nodes = ["Alice", "Bob", "Carol", "Dan", "Eve"].map(|name| add_person(&graph, name));
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (e:Person {name:'Eve'}) \
                 CREATE (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), \
                 (b)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), \
                 (c)-[:KNOWS]->(c), (d)-[:OTHER]->(e)",
        )
        .unwrap();

    let source = NodeSelector::Match {
        label: "Person".into(),
        property: "name".into(),
        value: PropValue::Str("Eve".into()),
    };
    let options = transitive_closure_options(true, Some("KNOWS"));
    let batch = graph.paths(&source, None, options.clone()).unwrap();
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
            ("source_uuid", &DataType::FixedSizeBinary(16), false),
            ("target_uuid", &DataType::FixedSizeBinary(16), false),
        ]
    );
    assert_eq!(
        batch.schema().metadata()["graphforge.algorithm"],
        "transitive_closure"
    );
    assert_eq!(batch.schema().metadata()["graphforge.verb"], "paths");
    assert!(batch.column_by_name("source_id").is_none());
    assert!(batch.column_by_name("target_id").is_none());

    let uuids = nodes.map(|node| *node.uuid.as_bytes());
    let mut expected = vec![
        (uuids[0], uuids[0]),
        (uuids[0], uuids[1]),
        (uuids[0], uuids[2]),
        (uuids[1], uuids[0]),
        (uuids[1], uuids[1]),
        (uuids[1], uuids[2]),
        (uuids[2], uuids[2]),
    ];
    expected.sort_unstable();
    assert_eq!(uuid_pairs(&batch), expected);
    assert_eq!(batch, graph.paths(&source, None, options).unwrap());

    let undirected = graph
        .paths(
            &source,
            None,
            transitive_closure_options(false, Some("KNOWS")),
        )
        .unwrap();
    let mut expected_undirected = Vec::new();
    for source in &uuids[..3] {
        for target in &uuids[..3] {
            expected_undirected.push((*source, *target));
        }
    }
    expected_undirected.sort_unstable();
    assert_eq!(uuid_pairs(&undirected), expected_undirected);

    assert_eq!(
        uuid_pairs(
            &graph
                .paths(
                    &source,
                    None,
                    transitive_closure_options(true, Some("OTHER")),
                )
                .unwrap()
        ),
        vec![(uuids[3], uuids[4])]
    );

    let edgeless = GraphForge::new(None).unwrap();
    let isolated = add_person(&edgeless, "Isolated");
    assert_eq!(
        edgeless
            .paths(
                &NodeSelector::Handle(isolated),
                None,
                transitive_closure_options(true, None),
            )
            .unwrap()
            .num_rows(),
        0
    );
}

#[test]
fn transitive_closure_rejects_target_and_path_only_options() {
    let graph = GraphForge::new(None).unwrap();
    let source = add_person(&graph, "Alice");
    let target = add_person(&graph, "Bob");
    let source = NodeSelector::Handle(source);
    let target = NodeSelector::Handle(target);

    assert!(matches!(
        graph.paths(
            &source,
            Some(&target),
            transitive_closure_options(true, None),
        ),
        Err(GfError::Validation(message)) if message.contains("does not accept a target")
    ));
    for options in [
        PathsOptions {
            k: 2,
            ..transitive_closure_options(true, None)
        },
        PathsOptions {
            weight: Some("cost".into()),
            ..transitive_closure_options(true, None)
        },
        PathsOptions {
            heuristic: Some("estimate".into()),
            ..transitive_closure_options(true, None)
        },
        transitive_closure_options(true, Some(" ")),
    ] {
        assert!(matches!(
            graph.paths(&source, None, options),
            Err(GfError::Validation(_))
        ));
    }
    assert!(matches!(
        graph.paths(
            &NodeSelector::Uuid(graphforge_core::uuid::new_v7()),
            None,
            transitive_closure_options(true, None),
        ),
        Err(GfError::Validation(message)) if message.contains("matched no nodes")
    ));
}
