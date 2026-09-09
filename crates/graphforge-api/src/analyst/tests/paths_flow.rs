use super::*;

fn max_flow_options(by: PathAlgorithm, weight: Option<&str>) -> PathsOptions {
    PathsOptions {
        by,
        directed: true,
        k: 1,
        via: Some("PIPE".into()),
        weight: weight.map(str::to_owned),
        capacity_property: None,
        cost_property: None,
        heuristic: None,
        walk_length: None,
        seed: None,
        terminal_uuids: Vec::new(),
        prize_property: None,
    }
}

fn min_cut_options(by: PathAlgorithm, directed: bool, weight: Option<&str>) -> PathsOptions {
    PathsOptions {
        by,
        directed,
        k: 1,
        via: Some("PIPE".into()),
        weight: weight.map(str::to_owned),
        capacity_property: None,
        cost_property: None,
        heuristic: None,
        walk_length: None,
        seed: None,
        terminal_uuids: Vec::new(),
        prize_property: None,
    }
}

fn min_cost_flow_options(by: PathAlgorithm, directed: bool) -> PathsOptions {
    PathsOptions {
        by,
        directed,
        k: 1,
        via: Some("PIPE".into()),
        weight: None,
        capacity_property: Some("capacity".into()),
        cost_property: Some("cost".into()),
        heuristic: None,
        walk_length: None,
        seed: None,
        terminal_uuids: Vec::new(),
        prize_property: None,
    }
}

fn gomory_hu_options(weight: Option<&str>) -> PathsOptions {
    PathsOptions {
        by: PathAlgorithm::GomoryHuTree,
        directed: false,
        k: 1,
        via: Some("PIPE".into()),
        weight: weight.map(str::to_owned),
        capacity_property: None,
        cost_property: None,
        heuristic: None,
        walk_length: None,
        seed: None,
        terminal_uuids: Vec::new(),
        prize_property: None,
    }
}

fn min_steiner_options(terminals: &[&NodeHandle], weight: Option<&str>) -> PathsOptions {
    PathsOptions {
        by: PathAlgorithm::MinSteinerTree,
        directed: false,
        k: 1,
        via: Some("ROAD".into()),
        weight: weight.map(str::to_owned),
        capacity_property: None,
        cost_property: None,
        heuristic: None,
        walk_length: None,
        seed: None,
        terminal_uuids: terminals.iter().map(|node| *node.uuid.as_bytes()).collect(),
        prize_property: None,
    }
}

fn prize_steiner_options(
    terminals: &[&NodeHandle],
    weight: Option<&str>,
    prize_property: &str,
) -> PathsOptions {
    PathsOptions {
        by: PathAlgorithm::PrizeCollectingSteinerTree,
        directed: false,
        k: 1,
        via: Some("ROAD".into()),
        weight: weight.map(str::to_owned),
        terminal_uuids: terminals.iter().map(|node| *node.uuid.as_bytes()).collect(),
        prize_property: Some(prize_property.into()),
        ..PathsOptions::default()
    }
}

fn float_column<'a>(batch: &'a arrow::record_batch::RecordBatch, name: &str) -> &'a Float64Array {
    batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref()
        .unwrap()
}

fn ordered_uuid_pair(left: [u8; 16], right: [u8; 16]) -> ([u8; 16], [u8; 16]) {
    if left <= right {
        (left, right)
    } else {
        (right, left)
    }
}

#[test]
fn steiner_algorithms_reject_positional_endpoints() {
    let graph = GraphForge::new(None).unwrap();
    let alice = add_person(&graph, "Alice");
    let bob = add_person(&graph, "Bob");
    assert!(matches!(
        graph.paths(
            &NodeSelector::Handle(alice.clone()),
            None,
            PathsOptions {
                by: PathAlgorithm::PrizeCollectingSteinerTree,
                directed: false,
                terminal_uuids: vec![*alice.uuid.as_bytes(), *bob.uuid.as_bytes()],
                prize_property: Some("prize".into()),
                ..PathsOptions::default()
            },
        ),
            Err(GfError::Validation(message))
                if message == "prize_collecting_steiner_tree does not accept positional source or target selectors"
    ));

    assert!(matches!(
        graph.paths(None, None, bfs_options(true, None)),
        Err(GfError::Validation(message)) if message == "bfs requires a source selector"
    ));
    let missing = NodeSelector::Match {
        label: "Person".into(),
        property: "name".into(),
        value: PropValue::Str("Missing".into()),
    };
    assert!(matches!(
        graph.paths(
            &missing,
            None,
            PathsOptions {
                by: PathAlgorithm::MinSteinerTree,
                directed: false,
                terminal_uuids: vec![*alice.uuid.as_bytes(), *bob.uuid.as_bytes()],
                ..PathsOptions::default()
            },
        ),
        Err(GfError::Validation(message))
            if message == "min_steiner_tree does not accept positional source or target selectors"
    ));
    assert!(matches!(
        graph.paths(
            None,
            Some(&NodeSelector::Handle(bob.clone())),
            min_steiner_options(&[&alice, &bob], None),
        ),
        Err(GfError::Validation(message))
            if message == "min_steiner_tree does not accept positional source or target selectors"
    ));
}

#[test]
fn gomory_hu_persists_canonical_weighted_forest_and_closed_contract() {
    let dir = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
    let nodes = ["A", "B", "C", "Isolated"].map(|name| add_person(&graph, name));
    graph
        .execute(
            "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}), \
                 (c:Person {name:'C'}) \
                 CREATE (a)-[:PIPE {capacity:3.0, bad:3.0}]->(b), \
                 (a)-[:PIPE {capacity:1.0, bad:1.0}]->(b), \
                 (a)-[:PIPE {capacity:2.0, bad:-1.0}]->(c), \
                 (c)-[:PIPE {capacity:1.0, bad:1.0}]->(a), \
                 (b)-[:PIPE {capacity:4.0, bad:4.0}]->(c), \
                 (a)-[:PIPE {capacity:99.0, bad:0.0}]->(a), \
                 (a)-[:OTHER {capacity:99.0}]->(c)",
        )
        .unwrap();
    drop(graph);

    let reopened = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
    let weighted_options = gomory_hu_options(Some("capacity"));
    let weighted = reopened
        .paths(None, None, weighted_options.clone())
        .unwrap();
    assert_eq!(
        weighted,
        reopened
            .paths(None, None, weighted_options.clone())
            .unwrap()
    );
    assert_eq!(weighted.num_rows(), 2);
    assert_eq!(
        weighted
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
            ("cut_value", &DataType::Float64, false),
        ]
    );
    assert_eq!(
        weighted.schema().metadata()["graphforge.algorithm"],
        "gomory_hu_tree"
    );
    assert_eq!(weighted.schema().metadata()["graphforge.verb"], "paths");
    assert_eq!(
        weighted.schema().metadata()["graphforge.algorithm_schema_version"],
        "1"
    );
    let sources = uuid_column(&weighted, "source_uuid");
    let targets = uuid_column(&weighted, "target_uuid");
    assert!((0..weighted.num_rows()).all(|row| sources.value(row) < targets.value(row)));
    assert!((0..weighted.num_rows() - 1).all(|row| {
        (sources.value(row), targets.value(row)) < (sources.value(row + 1), targets.value(row + 1))
    }));
    let mut cuts = float_column(&weighted, "cut_value").values().to_vec();
    cuts.sort_by(f64::total_cmp);
    assert_eq!(cuts, vec![7.0, 7.0]);

    let unit = reopened.paths(None, None, gomory_hu_options(None)).unwrap();
    assert_eq!(float_column(&unit, "cut_value").values(), &[3.0, 3.0]);
    assert!(matches!(
        reopened.paths(
            &NodeSelector::Handle(nodes[0].clone()),
            None,
            weighted_options.clone(),
        ),
        Err(GfError::Validation(message))
            if message
                == "gomory_hu_tree does not accept positional source or target selectors"
    ));
    for invalid in [
        PathsOptions {
            directed: true,
            ..weighted_options.clone()
        },
        PathsOptions {
            k: 2,
            ..weighted_options.clone()
        },
        PathsOptions {
            heuristic: Some("capacity".into()),
            ..weighted_options.clone()
        },
        PathsOptions {
            capacity_property: Some("capacity".into()),
            ..weighted_options.clone()
        },
        PathsOptions {
            terminal_uuids: vec![[0; 16]],
            ..weighted_options.clone()
        },
        PathsOptions {
            prize_property: Some("capacity".into()),
            ..weighted_options.clone()
        },
        gomory_hu_options(Some("bad")),
        gomory_hu_options(Some("missing")),
    ] {
        assert!(reopened.paths(None, None, invalid).is_err());
    }
}

#[test]
fn minimum_steiner_persists_exact_uuid_only_weighted_tree() {
    let dir = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
    let nodes = ["A", "B", "Center", "Unused"].map(|name| add_person(&graph, name));
    graph
        .execute(
            "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}), \
                 (c:Person {name:'Center'}), (u:Person {name:'Unused'}) \
                 CREATE (a)-[:ROAD {cost:1.0}]->(c), \
                 (b)-[:ROAD {cost:1.0}]->(c), \
                 (a)-[:ROAD {cost:5.0}]->(b), \
                 (a)-[:ROAD {cost:1.0}]->(c), \
                 (c)-[:ROAD {cost:0.0}]->(c), \
                 (a)-[:OTHER {cost:0.0}]->(b), \
                 (u)-[:ROAD {cost:9.0}]->(u)",
        )
        .unwrap();
    let terminal_ids = [nodes[1].uuid, nodes[0].uuid, nodes[1].uuid];
    drop(graph);

    let reopened = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
    let options = PathsOptions {
        by: PathAlgorithm::MinSteinerTree,
        directed: false,
        k: 1,
        via: Some("ROAD".into()),
        weight: Some("cost".into()),
        terminal_uuids: terminal_ids.iter().map(|uuid| *uuid.as_bytes()).collect(),
        ..PathsOptions::default()
    };
    let result = reopened.paths(None, None, options.clone()).unwrap();
    assert_eq!(result, reopened.paths(None, None, options.clone()).unwrap());
    assert_eq!(result.num_rows(), 2);
    assert_eq!(
        result
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
            ("weight", &DataType::Float64, false),
        ]
    );
    assert_eq!(
        result.schema().metadata()["graphforge.algorithm"],
        "min_steiner_tree"
    );
    assert_eq!(result.schema().metadata()["graphforge.verb"], "paths");
    assert_eq!(
        result.schema().metadata()["graphforge.algorithm_schema_version"],
        "1"
    );
    assert_eq!(result.schema().metadata().len(), 3);
    assert!(
        result
            .columns()
            .iter()
            .all(|column| column.null_count() == 0)
    );
    for forbidden in [
        "node_id",
        "edge_id",
        "provenance_id",
        "confidence",
        "assertion_uuid",
        "belief_status",
        "valid_time",
    ] {
        assert!(result.column_by_name(forbidden).is_none());
    }
    let edge_ids = uuid_column(&result, "edge_uuid");
    assert!(edge_ids.value(0) < edge_ids.value(1));
    let mut expected_edge_ids = relationship_rows(&reopened, "ROAD")
        .into_iter()
        .filter_map(|(edge, source, target)| {
            let endpoints = ordered_uuid_pair(source, target);
            let a_center = ordered_uuid_pair(*nodes[0].uuid.as_bytes(), *nodes[2].uuid.as_bytes());
            let b_center = ordered_uuid_pair(*nodes[1].uuid.as_bytes(), *nodes[2].uuid.as_bytes());
            (endpoints == a_center || endpoints == b_center).then_some((endpoints, edge))
        })
        .collect::<Vec<_>>();
    expected_edge_ids.sort_unstable();
    let a_center = ordered_uuid_pair(*nodes[0].uuid.as_bytes(), *nodes[2].uuid.as_bytes());
    let b_center = ordered_uuid_pair(*nodes[1].uuid.as_bytes(), *nodes[2].uuid.as_bytes());
    let mut exact_expected = vec![
        expected_edge_ids
            .iter()
            .filter(|(endpoints, _)| *endpoints == a_center)
            .map(|(_, edge)| *edge)
            .min()
            .unwrap(),
        expected_edge_ids
            .iter()
            .find(|(endpoints, _)| *endpoints == b_center)
            .unwrap()
            .1,
    ];
    exact_expected.sort_unstable();
    assert_eq!(
        (0..result.num_rows())
            .map(|row| edge_ids.value(row).try_into().unwrap())
            .collect::<Vec<[u8; 16]>>(),
        exact_expected
    );
    let weights = float_column(&result, "weight");
    assert_eq!([weights.value(0), weights.value(1)], [1.0, 1.0]);
    let sources = uuid_column(&result, "source_uuid");
    let targets = uuid_column(&result, "target_uuid");
    let endpoints = (0..result.num_rows())
        .map(|row| {
            ordered_uuid_pair(
                sources.value(row).try_into().unwrap(),
                targets.value(row).try_into().unwrap(),
            )
        })
        .collect::<HashSet<_>>();
    assert_eq!(
        endpoints,
        HashSet::from([
            ordered_uuid_pair(*nodes[0].uuid.as_bytes(), *nodes[2].uuid.as_bytes()),
            ordered_uuid_pair(*nodes[1].uuid.as_bytes(), *nodes[2].uuid.as_bytes()),
        ])
    );
}

#[test]
fn minimum_steiner_validates_closed_contract_and_errors_atomically() {
    let graph = GraphForge::new(None).unwrap();
    let nodes = ["A", "B", "C"].map(|name| add_person(&graph, name));
    graph
        .execute(
            "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}) \
                 CREATE (a)-[:ROAD {cost:2.0, bad:-1.0}]->(b)",
        )
        .unwrap();
    let valid = min_steiner_options(&[&nodes[0], &nodes[1]], Some("cost"));
    let baseline = graph.paths(None, None, valid.clone()).unwrap();
    assert_eq!(baseline.num_rows(), 1);

    for invalid in [
        PathsOptions {
            directed: true,
            ..valid.clone()
        },
        PathsOptions {
            k: 2,
            ..valid.clone()
        },
        min_steiner_options(&[&nodes[0]], Some("cost")),
        min_steiner_options(&[&nodes[0], &nodes[2]], Some("cost")),
        min_steiner_options(&[&nodes[0], &nodes[1]], Some("bad")),
        min_steiner_options(&[&nodes[0], &nodes[1]], Some("missing")),
        PathsOptions {
            prize_property: Some("prize".into()),
            ..valid.clone()
        },
    ] {
        assert!(graph.paths(None, None, invalid).is_err());
        assert_eq!(graph.paths(None, None, valid.clone()).unwrap(), baseline);
    }

    let unit = graph
        .paths(
            None,
            None,
            min_steiner_options(&[&nodes[1], &nodes[0]], None),
        )
        .unwrap();
    assert_eq!(float_column(&unit, "weight").value(0), 1.0);
}

#[test]
fn prize_steiner_persists_exact_objective_schema_and_replay() {
    let dir = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
    let terminal = graph
        .add_node(
            "Person",
            &HashMap::from([
                ("name".into(), PropValue::Str("Terminal".into())),
                ("prize".into(), PropValue::Float(0.0)),
            ]),
        )
        .unwrap();
    let winner = graph
        .add_node(
            "Person",
            &HashMap::from([
                ("name".into(), PropValue::Str("Winner".into())),
                ("prize".into(), PropValue::Float(10.0)),
            ]),
        )
        .unwrap();
    let excluded = graph
        .add_node(
            "Person",
            &HashMap::from([
                ("name".into(), PropValue::Str("Excluded".into())),
                ("prize".into(), PropValue::Float(2.0)),
            ]),
        )
        .unwrap();
    graph
        .execute(
            "MATCH (t:Person {name:'Terminal'}), (w:Person {name:'Winner'}), \
                 (x:Person {name:'Excluded'}) \
                 CREATE (t)-[:ROAD {cost:3.0}]->(w), \
                 (t)-[:ROAD {cost:3.0}]->(w), \
                 (t)-[:ROAD {cost:5.0}]->(x), \
                 (w)-[:ROAD {cost:0.0}]->(w), \
                 (t)-[:OTHER {cost:0.0}]->(x)",
        )
        .unwrap();
    let expected_edge = relationship_rows(&graph, "ROAD")
        .into_iter()
        .filter(|(_, source, target)| {
            ordered_uuid_pair(*source, *target)
                == ordered_uuid_pair(*terminal.uuid.as_bytes(), *winner.uuid.as_bytes())
        })
        .map(|(edge, _, _)| edge)
        .min()
        .unwrap();
    drop(graph);

    let reopened = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
    let options = prize_steiner_options(&[&terminal], Some("cost"), "prize");
    let result = reopened.paths(None, None, options.clone()).unwrap();
    assert_eq!(result, reopened.paths(None, None, options).unwrap());
    assert_eq!(result.num_rows(), 1);
    assert_eq!(
        result
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
            ("weight", &DataType::Float64, false),
        ]
    );
    assert_eq!(
        result.schema().metadata()["graphforge.algorithm"],
        "prize_collecting_steiner_tree"
    );
    assert_eq!(result.schema().metadata()["graphforge.verb"], "paths");
    assert_eq!(
        result.schema().metadata()["graphforge.algorithm_schema_version"],
        "1"
    );
    assert_eq!(result.schema().metadata().len(), 3);
    assert!(
        result
            .columns()
            .iter()
            .all(|column| column.null_count() == 0)
    );
    for forbidden in [
        "node_id",
        "edge_id",
        "provenance_id",
        "confidence",
        "assertion_uuid",
        "belief_status",
        "valid_time",
    ] {
        assert!(result.column_by_name(forbidden).is_none());
    }
    assert_eq!(uuid_column(&result, "edge_uuid").value(0), expected_edge);
    assert_eq!(float_column(&result, "weight").value(0), 3.0);
    let endpoints = ordered_uuid_pair(
        uuid_column(&result, "source_uuid")
            .value(0)
            .try_into()
            .unwrap(),
        uuid_column(&result, "target_uuid")
            .value(0)
            .try_into()
            .unwrap(),
    );
    assert_eq!(
        endpoints,
        ordered_uuid_pair(*terminal.uuid.as_bytes(), *winner.uuid.as_bytes())
    );
    assert_ne!(endpoints.0, *excluded.uuid.as_bytes());
    assert_ne!(endpoints.1, *excluded.uuid.as_bytes());

    let unit_options = prize_steiner_options(&[&winner, &terminal, &winner], None, "prize");
    let unit = reopened.paths(None, None, unit_options.clone()).unwrap();
    assert_eq!(unit, reopened.paths(None, None, unit_options).unwrap());
    assert_eq!(unit.num_rows(), 2);
    let unit_edges = uuid_column(&unit, "edge_uuid");
    assert!(unit_edges.value(0) < unit_edges.value(1));
    assert!((0..2).any(|row| unit_edges.value(row) == expected_edge));
    assert_eq!(
        [
            float_column(&unit, "weight").value(0),
            float_column(&unit, "weight").value(1),
        ],
        [1.0, 1.0]
    );
}

#[test]
fn prize_steiner_one_terminal_and_closed_errors_are_atomic() {
    let graph = GraphForge::new(None).unwrap();
    let nodes = [("A", 0.0), ("B", 0.0), ("C", 0.0)].map(|(name, prize)| {
        graph
            .add_node(
                "Person",
                &HashMap::from([
                    ("name".into(), PropValue::Str(name.into())),
                    ("prize".into(), PropValue::Float(prize)),
                ]),
            )
            .unwrap()
    });
    graph
        .execute(
            "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}) \
                 CREATE (a)-[:ROAD {cost:2.0, bad:-1.0}]->(b)",
        )
        .unwrap();
    let valid = prize_steiner_options(&[&nodes[0]], Some("cost"), "prize");
    let baseline = graph.paths(None, None, valid.clone()).unwrap();
    assert_eq!(baseline.num_rows(), 0);

    for invalid in [
        PathsOptions {
            directed: true,
            ..valid.clone()
        },
        PathsOptions {
            k: 2,
            ..valid.clone()
        },
        PathsOptions {
            terminal_uuids: Vec::new(),
            ..valid.clone()
        },
        PathsOptions {
            terminal_uuids: vec![[0xff; 16]],
            ..valid.clone()
        },
        prize_steiner_options(&[&nodes[0], &nodes[2]], Some("cost"), "prize"),
        prize_steiner_options(&[&nodes[0]], Some("bad"), "prize"),
        prize_steiner_options(&[&nodes[0]], Some("missing"), "prize"),
        prize_steiner_options(&[&nodes[0]], Some("cost"), "missing"),
        PathsOptions {
            prize_property: None,
            ..valid.clone()
        },
    ] {
        assert!(graph.paths(None, None, invalid).is_err());
        assert_eq!(graph.paths(None, None, valid.clone()).unwrap(), baseline);
    }

    let missing_prize = add_person(&graph, "NoPrize");
    assert!(
        graph
            .paths(
                None,
                None,
                prize_steiner_options(&[&missing_prize], Some("cost"), "prize"),
            )
            .is_err()
    );
}

#[test]
fn prize_steiner_validates_property_types_before_kernel_dispatch() {
    for prize in [
        PropValue::Int(2),
        PropValue::Int(-1),
        PropValue::Int(i64::MAX),
        PropValue::Null,
        PropValue::Bool(true),
        PropValue::Str("not a number".into()),
    ] {
        let valid = prize == PropValue::Int(2);
        let graph = GraphForge::new(None).unwrap();
        let node = graph
            .add_node("Person", &HashMap::from([("prize".into(), prize.clone())]))
            .unwrap();
        let result = graph.paths(None, None, prize_steiner_options(&[&node], None, "prize"));
        if valid {
            assert_eq!(result.unwrap().num_rows(), 0);
        } else {
            assert!(result.is_err(), "invalid prize accepted: {prize:?}");
        }
        assert_eq!(graph.node_count("Person").unwrap(), 1);
    }
}

#[test]
fn maximum_flow_views_are_consistent_uuid_only_and_deterministic_through_public_api() {
    let graph = GraphForge::new(None).unwrap();
    let nodes = ["Source", "A", "B", "Sink", "Unreachable"].map(|name| add_person(&graph, name));
    graph
        .execute(
            "MATCH (s:Person {name:'Source'}), (a:Person {name:'A'}), \
                 (b:Person {name:'B'}), (t:Person {name:'Sink'}) \
                 CREATE (s)-[:PIPE {capacity:3.0}]->(a), \
                 (s)-[:PIPE {capacity:2.0}]->(b), \
                 (a)-[:PIPE {capacity:1.0}]->(b), \
                 (a)-[:PIPE {capacity:2.0}]->(t), \
                 (b)-[:PIPE {capacity:3.0}]->(t), \
                 (a)-[:PIPE {capacity:7.0}]->(a), \
                 (b)-[:PIPE {capacity:0.0}]->(a), \
                 (s)-[:OTHER {capacity:100.0}]->(t)",
        )
        .unwrap();
    let source = NodeSelector::Handle(nodes[0].clone());
    let sink = NodeSelector::Handle(nodes[3].clone());
    let scalar_options = max_flow_options(PathAlgorithm::MaxFlow, Some("capacity"));
    let edge_options = max_flow_options(PathAlgorithm::MaxFlowEdges, Some("capacity"));
    let scalar = graph
        .paths(&source, Some(&sink), scalar_options.clone())
        .unwrap();
    let edges = graph
        .paths(&source, Some(&sink), edge_options.clone())
        .unwrap();

    assert_eq!(
        scalar,
        graph.paths(&source, Some(&sink), scalar_options).unwrap()
    );
    assert_eq!(
        edges,
        graph.paths(&source, Some(&sink), edge_options).unwrap()
    );
    assert_eq!(
        scalar
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
            ("sink_uuid", &DataType::FixedSizeBinary(16), false),
            ("flow", &DataType::Float64, false),
        ]
    );
    assert_eq!(
        edges
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
            ("flow", &DataType::Float64, false),
        ]
    );
    assert_eq!(
        scalar.schema().metadata()["graphforge.algorithm"],
        "max_flow"
    );
    assert_eq!(
        edges.schema().metadata()["graphforge.algorithm"],
        "max_flow_edges"
    );
    assert_eq!(
        scalar.schema().metadata()["graphforge.algorithm_schema_version"],
        "1"
    );
    assert_eq!(
        edges.schema().metadata()["graphforge.algorithm_schema_version"],
        "1"
    );
    assert_eq!(scalar.schema().metadata()["graphforge.verb"], "paths");
    assert_eq!(edges.schema().metadata()["graphforge.verb"], "paths");

    let scalar_sources = scalar
        .column_by_name("source_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let scalar_sinks = scalar
        .column_by_name("sink_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(scalar_sources.value(0), nodes[0].uuid.as_bytes());
    assert_eq!(scalar_sinks.value(0), nodes[3].uuid.as_bytes());
    let scalar_flow = scalar
        .column_by_name("flow")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0);
    assert_eq!(scalar_flow, 5.0);
    let edge_uuids = edges
        .column_by_name("edge_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let sources = edges
        .column_by_name("source_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let targets = edges
        .column_by_name("target_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let flows = edges
        .column_by_name("flow")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(edges.num_rows(), 7);
    assert!((1..edges.num_rows()).all(|row| edge_uuids.value(row - 1) < edge_uuids.value(row)));
    let assignments = (0..edges.num_rows())
        .map(|row| {
            (
                (
                    sources.value(row).try_into().unwrap(),
                    targets.value(row).try_into().unwrap(),
                ),
                flows.value(row),
            )
        })
        .collect::<HashMap<([u8; 16], [u8; 16]), f64>>();
    assert_eq!(
        assignments,
        HashMap::from([
            ((*nodes[0].uuid.as_bytes(), *nodes[1].uuid.as_bytes()), 3.0),
            ((*nodes[0].uuid.as_bytes(), *nodes[2].uuid.as_bytes()), 2.0),
            ((*nodes[1].uuid.as_bytes(), *nodes[2].uuid.as_bytes()), 1.0),
            ((*nodes[1].uuid.as_bytes(), *nodes[3].uuid.as_bytes()), 2.0),
            ((*nodes[2].uuid.as_bytes(), *nodes[3].uuid.as_bytes()), 3.0),
            ((*nodes[1].uuid.as_bytes(), *nodes[1].uuid.as_bytes()), 0.0),
            ((*nodes[2].uuid.as_bytes(), *nodes[1].uuid.as_bytes()), 0.0),
        ])
    );

    let flow_at = |node: &[u8; 16], outgoing: bool| {
        (0..edges.num_rows())
            .filter(|&row| {
                if outgoing {
                    sources.value(row) == node
                } else {
                    targets.value(row) == node
                }
            })
            .map(|row| flows.value(row))
            .sum::<f64>()
    };
    assert_eq!(flow_at(nodes[0].uuid.as_bytes(), true), scalar_flow);
    assert_eq!(flow_at(nodes[3].uuid.as_bytes(), false), scalar_flow);
    for node in [&nodes[1], &nodes[2]] {
        assert_eq!(
            flow_at(node.uuid.as_bytes(), false),
            flow_at(node.uuid.as_bytes(), true)
        );
    }
    assert!((0..edges.num_rows()).all(|row| flows.value(row) >= 0.0));
    assert!((0..edges.num_rows()).any(|row| flows.value(row) == 0.0));

    let unreachable = NodeSelector::Handle(nodes[4].clone());
    let zero = graph
        .paths(
            &source,
            Some(&unreachable),
            max_flow_options(PathAlgorithm::MaxFlow, Some("capacity")),
        )
        .unwrap();
    assert_eq!(
        zero.column_by_name("flow")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        0.0
    );
    assert!(matches!(
        graph.paths(
            &source,
            None,
            max_flow_options(PathAlgorithm::MaxFlow, Some("capacity")),
        ),
        Err(GfError::Validation(message)) if message.contains("target selector")
    ));
    assert!(matches!(
        graph.paths(
            &source,
            Some(&source),
            max_flow_options(PathAlgorithm::MaxFlowEdges, Some("capacity")),
        ).map_err(expect_algorithm_execution),
        Err(AlgorithmError::Execution { message }) if message.contains("distinct endpoints")
    ));

    let invalid = GraphForge::new(None).unwrap();
    let invalid_nodes = ["Source", "Sink"].map(|name| add_person(&invalid, name));
    invalid
        .execute(
            "MATCH (s:Person {name:'Source'}), (t:Person {name:'Sink'}) \
                 CREATE (s)-[:PIPE {capacity:-1.0}]->(t)",
        )
        .unwrap();
    assert!(matches!(
        invalid.paths(
            &NodeSelector::Handle(invalid_nodes[0].clone()),
            Some(&NodeSelector::Handle(invalid_nodes[1].clone())),
            max_flow_options(PathAlgorithm::MaxFlow, Some("capacity")),
        ).map_err(expect_algorithm_execution),
        Err(AlgorithmError::Execution { message }) if message.contains("nonnegative")
    ));
}

#[test]
fn min_cost_flow_persists_and_shares_scalar_and_edge_solution() {
    let dir = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
    let nodes = ["Source", "A", "Sink"].map(|name| add_person(&graph, name));
    graph
        .execute(
            "MATCH (s:Person {name:'Source'}), (a:Person {name:'A'}), \
                 (t:Person {name:'Sink'}) \
                 CREATE (s)-[:PIPE {capacity:2.0, cost:-1.0}]->(a), \
                 (a)-[:PIPE {capacity:2.0, cost:3.0}]->(t), \
                 (s)-[:PIPE {capacity:1.0, cost:5.0}]->(t), \
                 (a)-[:PIPE {capacity:9.0, cost:-8.0}]->(a)",
        )
        .unwrap();
    let source_uuid = nodes[0].uuid;
    let sink_uuid = nodes[2].uuid;
    drop(graph);

    let reopened = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
    let source = NodeSelector::Uuid(source_uuid);
    let sink = NodeSelector::Uuid(sink_uuid);
    let scalar_options = min_cost_flow_options(PathAlgorithm::MinCostMaxFlow, true);
    let edge_options = min_cost_flow_options(PathAlgorithm::MinCostMaxFlowEdges, true);
    let scalar = reopened
        .paths(&source, Some(&sink), scalar_options.clone())
        .unwrap();
    let edges = reopened
        .paths(&source, Some(&sink), edge_options.clone())
        .unwrap();
    assert_eq!(
        scalar,
        reopened
            .paths(&source, Some(&sink), scalar_options)
            .unwrap()
    );
    assert_eq!(
        edges,
        reopened.paths(&source, Some(&sink), edge_options).unwrap()
    );
    assert_eq!(
        scalar.schema().metadata()["graphforge.algorithm"],
        "min_cost_max_flow"
    );
    assert_eq!(
        edges.schema().metadata()["graphforge.algorithm"],
        "min_cost_max_flow_edges"
    );
    assert_eq!(
        scalar
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
            ("sink_uuid", &DataType::FixedSizeBinary(16), false),
            ("flow", &DataType::Float64, false),
            ("cost", &DataType::Float64, false),
        ]
    );
    assert_eq!(
        edges
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
            ("flow", &DataType::Float64, false),
            ("unit_cost", &DataType::Float64, false),
            ("flow_cost", &DataType::Float64, false),
        ]
    );
    let flow = scalar
        .column_by_name("flow")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0);
    let cost = scalar
        .column_by_name("cost")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0);
    assert_eq!((flow, cost), (3.0, 9.0));
    for batch in [&scalar, &edges] {
        assert_eq!(batch.schema().metadata()["graphforge.verb"], "paths");
        assert_eq!(
            batch.schema().metadata()["graphforge.algorithm_schema_version"],
            "1"
        );
    }
    let edge_uuids = uuid_column(&edges, "edge_uuid");
    assert!((1..edges.num_rows()).all(|row| edge_uuids.value(row - 1) < edge_uuids.value(row)));
    let flows = edges
        .column_by_name("flow")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let sources = uuid_column(&edges, "source_uuid");
    let targets = uuid_column(&edges, "target_uuid");
    let unit_costs = float_column(&edges, "unit_cost");
    let flow_costs = edges
        .column_by_name("flow_cost")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(
        (0..edges.num_rows())
            .map(|row| flows.value(row))
            .sum::<f64>(),
        5.0
    );
    assert_eq!(
        (0..edges.num_rows())
            .map(|row| flow_costs.value(row))
            .sum::<f64>(),
        cost
    );
    assert!((0..edges.num_rows()).any(|row| flows.value(row) == 0.0));
    for row in 0..edges.num_rows() {
        assert!(flows.value(row) >= 0.0);
        assert!(
            flows.value(row)
                <= if unit_costs.value(row) == -8.0 {
                    9.0
                } else if unit_costs.value(row) == 5.0 {
                    1.0
                } else {
                    2.0
                }
        );
        assert_eq!(
            flow_costs.value(row),
            flows.value(row) * unit_costs.value(row)
        );
    }
    let balance = |node: &[u8]| {
        (0..edges.num_rows())
            .map(|row| {
                (if targets.value(row) == node {
                    flows.value(row)
                } else {
                    0.0
                }) - (if sources.value(row) == node {
                    flows.value(row)
                } else {
                    0.0
                })
            })
            .sum::<f64>()
    };
    assert_eq!(balance(nodes[1].uuid.as_bytes()), 0.0);

    let mut unit = min_cost_flow_options(PathAlgorithm::MinCostMaxFlow, true);
    unit.capacity_property = None;
    assert_eq!(
        reopened
            .paths(&source, Some(&sink), unit)
            .unwrap()
            .column_by_name("flow")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        2.0
    );
    let mut missing_cost = min_cost_flow_options(PathAlgorithm::MinCostMaxFlow, true);
    missing_cost.cost_property = None;
    assert!(matches!(
        reopened.paths(&source, Some(&sink), missing_cost),
        Err(GfError::Validation(message)) if message.contains("cost_property")
    ));
    assert!(matches!(
        reopened.paths(
            &source,
            Some(&source),
            min_cost_flow_options(PathAlgorithm::MinCostMaxFlow, true),
        ).map_err(expect_algorithm_execution),
        Err(AlgorithmError::Execution { message }) if message.contains("distinct endpoints")
    ));
}

#[test]
fn min_cost_flow_persisted_undirected_signed_parallel_and_failures() {
    let dir = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
    let nodes = ["Source", "Sink"].map(|name| add_person(&graph, name));
    graph
        .execute(
            "MATCH (s:Person {name:'Source'}), (t:Person {name:'Sink'}) \
             CREATE (s)-[:PIPE {capacity:1.0, cost:2.0}]->(t), \
             (s)-[:PIPE {capacity:1.0, cost:2.0}]->(t)",
        )
        .unwrap();
    let result = graph
        .paths(
            &NodeSelector::Handle(nodes[1].clone()),
            Some(&NodeSelector::Handle(nodes[0].clone())),
            min_cost_flow_options(PathAlgorithm::MinCostMaxFlowEdges, false),
        )
        .unwrap();
    let flows = float_column(&result, "flow");
    let ids = uuid_column(&result, "edge_uuid");
    assert_eq!(result.num_rows(), 2);
    assert!(ids.value(0) < ids.value(1));
    assert_eq!([flows.value(0), flows.value(1)], [-1.0, -1.0]);

    let invalid = GraphForge::new(None).unwrap();
    let bad = ["Source", "Sink"].map(|name| add_person(&invalid, name));
    invalid
        .execute(
            "MATCH (s:Person {name:'Source'}), (t:Person {name:'Sink'}) \
             CREATE (s)-[:PIPE {capacity:-1.0, cost:1.0}]->(t)",
        )
        .unwrap();
    assert!(matches!(
        invalid.paths(
            &NodeSelector::Handle(bad[0].clone()),
            Some(&NodeSelector::Handle(bad[1].clone())),
            min_cost_flow_options(PathAlgorithm::MinCostMaxFlow, true),
        ).map_err(expect_algorithm_execution),
        Err(AlgorithmError::Execution { message }) if message.contains("nonnegative")
    ));

    let cycle = GraphForge::new(None).unwrap();
    let cycle_nodes = ["Source", "A", "B", "Sink"].map(|name| add_person(&cycle, name));
    cycle
        .execute(
            "MATCH (s:Person {name:'Source'}), (a:Person {name:'A'}), \
             (b:Person {name:'B'}), (t:Person {name:'Sink'}) \
             CREATE (s)-[:PIPE {capacity:1.0, cost:0.0}]->(a), \
             (a)-[:PIPE {capacity:1.0, cost:-2.0}]->(b), \
             (b)-[:PIPE {capacity:1.0, cost:1.0}]->(a), \
             (b)-[:PIPE {capacity:1.0, cost:0.0}]->(t)",
        )
        .unwrap();
    assert!(matches!(
        cycle.paths(
            &NodeSelector::Handle(cycle_nodes[0].clone()),
            Some(&NodeSelector::Handle(cycle_nodes[3].clone())),
            min_cost_flow_options(PathAlgorithm::MinCostMaxFlow, true),
        ).map_err(expect_algorithm_execution),
        Err(AlgorithmError::Execution { message }) if message.contains("negative-cost residual cycle")
    ));
}

#[test]
fn minimum_cut_views_agree_on_exact_uuid_results_through_public_api() {
    let graph = GraphForge::new(None).unwrap();
    let nodes = ["Source", "A", "B", "Sink", "Unreachable"].map(|name| add_person(&graph, name));
    graph
        .execute(
            "MATCH (s:Person {name:'Source'}), (a:Person {name:'A'}), \
                 (b:Person {name:'B'}), (t:Person {name:'Sink'}) \
                 CREATE (s)-[:PIPE {capacity:3.0}]->(a), \
                 (s)-[:PIPE {capacity:2.0}]->(b), \
                 (a)-[:PIPE {capacity:1.0}]->(b), \
                 (a)-[:PIPE {capacity:2.0}]->(t), \
                 (b)-[:PIPE {capacity:4.0}]->(t), \
                 (a)-[:PIPE {capacity:7.0}]->(a), \
                 (b)-[:PIPE {capacity:0.0}]->(a), \
                 (s)-[:OTHER {capacity:100.0}]->(t)",
        )
        .unwrap();
    let source = NodeSelector::Handle(nodes[0].clone());
    let sink = NodeSelector::Handle(nodes[3].clone());
    let scalar_options = min_cut_options(PathAlgorithm::MinCut, true, Some("capacity"));
    let edge_options = min_cut_options(PathAlgorithm::MinCutEdges, true, Some("capacity"));
    let scalar = graph
        .paths(&source, Some(&sink), scalar_options.clone())
        .unwrap();
    let edges = graph
        .paths(&source, Some(&sink), edge_options.clone())
        .unwrap();

    assert_eq!(
        scalar,
        graph.paths(&source, Some(&sink), scalar_options).unwrap()
    );
    assert_eq!(
        edges,
        graph.paths(&source, Some(&sink), edge_options).unwrap()
    );
    assert_eq!(
        scalar
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
            ("sink_uuid", &DataType::FixedSizeBinary(16), false),
            ("cut_value", &DataType::Float64, false),
        ]
    );
    assert_eq!(
        edges
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
            ("capacity", &DataType::Float64, false),
        ]
    );
    assert_eq!(
        scalar.schema().metadata()["graphforge.algorithm"],
        "min_cut"
    );
    assert_eq!(
        edges.schema().metadata()["graphforge.algorithm"],
        "min_cut_edges"
    );
    for batch in [&scalar, &edges] {
        assert_eq!(batch.schema().metadata()["graphforge.verb"], "paths");
        assert_eq!(
            batch.schema().metadata()["graphforge.algorithm_schema_version"],
            "1"
        );
        assert!(
            batch
                .columns()
                .iter()
                .all(|column| column.null_count() == 0)
        );
        for forbidden in [
            "node_id",
            "edge_id",
            "provenance_id",
            "confidence",
            "assertion_uuid",
            "belief_status",
            "valid_time",
        ] {
            assert!(batch.column_by_name(forbidden).is_none());
        }
    }

    let scalar_sources = uuid_column(&scalar, "source_uuid");
    let scalar_sinks = uuid_column(&scalar, "sink_uuid");
    let cut_value = float_column(&scalar, "cut_value").value(0);
    assert_eq!(scalar_sources.value(0), nodes[0].uuid.as_bytes());
    assert_eq!(scalar_sinks.value(0), nodes[3].uuid.as_bytes());
    assert_eq!(cut_value, 5.0);

    let edge_uuids = uuid_column(&edges, "edge_uuid");
    let sources = uuid_column(&edges, "source_uuid");
    let targets = uuid_column(&edges, "target_uuid");
    let capacities = float_column(&edges, "capacity");
    assert_eq!(edges.num_rows(), 2);
    assert!(edge_uuids.value(0) < edge_uuids.value(1));
    let cut = (0..edges.num_rows())
        .map(|row| {
            (
                (
                    sources.value(row).try_into().unwrap(),
                    targets.value(row).try_into().unwrap(),
                ),
                capacities.value(row),
            )
        })
        .collect::<HashMap<([u8; 16], [u8; 16]), f64>>();
    assert_eq!(
        cut,
        HashMap::from([
            ((*nodes[0].uuid.as_bytes(), *nodes[1].uuid.as_bytes()), 3.0),
            ((*nodes[0].uuid.as_bytes(), *nodes[2].uuid.as_bytes()), 2.0),
        ])
    );
    assert_eq!(capacities.values().iter().copied().sum::<f64>(), cut_value);

    let unit = graph
        .paths(
            &source,
            Some(&sink),
            min_cut_options(PathAlgorithm::MinCut, true, None),
        )
        .unwrap();
    assert_eq!(float_column(&unit, "cut_value").value(0), 2.0);

    let unreachable = NodeSelector::Handle(nodes[4].clone());
    let zero = graph
        .paths(
            &source,
            Some(&unreachable),
            min_cut_options(PathAlgorithm::MinCut, true, Some("capacity")),
        )
        .unwrap();
    let no_edges = graph
        .paths(
            &source,
            Some(&unreachable),
            min_cut_options(PathAlgorithm::MinCutEdges, true, Some("capacity")),
        )
        .unwrap();
    assert_eq!(float_column(&zero, "cut_value").value(0), 0.0);
    assert_eq!(no_edges.num_rows(), 0);
    assert_eq!(no_edges.schema(), edges.schema());
}

#[test]
fn minimum_cut_public_facade_preserves_undirected_orientation_and_structured_errors() {
    let graph = GraphForge::new(None).unwrap();
    let nodes = ["Left", "Middle", "Right"].map(|name| add_person(&graph, name));
    graph
        .execute(
            "MATCH (l:Person {name:'Left'}), (m:Person {name:'Middle'}), \
                 (r:Person {name:'Right'}) \
                 CREATE (l)-[:PIPE {capacity:2.0}]->(m), \
                 (m)-[:PIPE {capacity:2.0}]->(r)",
        )
        .unwrap();
    let left = NodeSelector::Handle(nodes[0].clone());
    let right = NodeSelector::Handle(nodes[2].clone());
    let edges = graph
        .paths(
            &right,
            Some(&left),
            min_cut_options(PathAlgorithm::MinCutEdges, false, Some("capacity")),
        )
        .unwrap();
    assert_eq!(edges.num_rows(), 1);
    let sources = uuid_column(&edges, "source_uuid");
    let targets = uuid_column(&edges, "target_uuid");
    let capacities = float_column(&edges, "capacity");
    assert_eq!(sources.value(0), nodes[0].uuid.as_bytes());
    assert_eq!(targets.value(0), nodes[1].uuid.as_bytes());
    assert_eq!(capacities.value(0), 2.0);

    assert!(matches!(
        graph.paths(
            &left,
            None,
            min_cut_options(PathAlgorithm::MinCut, true, Some("capacity")),
        ),
        Err(GfError::Validation(message)) if message.contains("target selector")
    ));
    assert!(matches!(
        graph.paths(
            &left,
            Some(&left),
            min_cut_options(PathAlgorithm::MinCutEdges, true, Some("capacity")),
        ).map_err(expect_algorithm_execution),
        Err(AlgorithmError::Execution { message }) if message.contains("distinct endpoints")
    ));

    let invalid = GraphForge::new(None).unwrap();
    let invalid_nodes = ["Source", "Sink"].map(|name| add_person(&invalid, name));
    invalid
        .execute(
            "MATCH (s:Person {name:'Source'}), (t:Person {name:'Sink'}) \
                 CREATE (s)-[:PIPE {capacity:-1.0}]->(t)",
        )
        .unwrap();
    assert!(matches!(
        invalid.paths(
            &NodeSelector::Handle(invalid_nodes[0].clone()),
            Some(&NodeSelector::Handle(invalid_nodes[1].clone())),
            min_cut_options(PathAlgorithm::MinCut, true, Some("capacity")),
        ).map_err(expect_algorithm_execution),
        Err(AlgorithmError::Execution { message }) if message.contains("nonnegative")
    ));
}
