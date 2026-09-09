use super::*;

fn dijkstra_options(directed: bool, via: Option<&str>, weight: Option<&str>) -> PathsOptions {
    PathsOptions {
        by: PathAlgorithm::Dijkstra,
        directed,
        k: 1,
        via: via.map(str::to_owned),
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

fn dijkstra_all_pairs_options(
    directed: bool,
    via: Option<&str>,
    weight: Option<&str>,
) -> PathsOptions {
    PathsOptions {
        by: PathAlgorithm::DijkstraAllPairs,
        directed,
        k: 1,
        via: via.map(str::to_owned),
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

fn astar_options(
    directed: bool,
    via: Option<&str>,
    weight: Option<&str>,
    heuristic: Option<&str>,
) -> PathsOptions {
    PathsOptions {
        by: PathAlgorithm::AStar,
        directed,
        k: 1,
        via: via.map(str::to_owned),
        weight: weight.map(str::to_owned),
        capacity_property: None,
        cost_property: None,
        heuristic: heuristic.map(str::to_owned),
        walk_length: None,
        seed: None,
        terminal_uuids: Vec::new(),
        prize_property: None,
    }
}

fn bellman_ford_options(directed: bool, via: Option<&str>, weight: Option<&str>) -> PathsOptions {
    PathsOptions {
        by: PathAlgorithm::BellmanFord,
        directed,
        k: 1,
        via: via.map(str::to_owned),
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

fn delta_stepping_options(directed: bool, via: Option<&str>, weight: Option<&str>) -> PathsOptions {
    PathsOptions {
        by: PathAlgorithm::DeltaStepping,
        directed,
        k: 1,
        via: via.map(str::to_owned),
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

fn yens_options(directed: bool, k: usize, via: Option<&str>, weight: Option<&str>) -> PathsOptions {
    PathsOptions {
        by: PathAlgorithm::Yens,
        directed,
        k,
        via: via.map(str::to_owned),
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

fn floyd_warshall_options(directed: bool, via: Option<&str>, weight: Option<&str>) -> PathsOptions {
    PathsOptions {
        by: PathAlgorithm::FloydWarshall,
        directed,
        k: 1,
        via: via.map(str::to_owned),
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

fn arrow_batch_fingerprint(batch: &arrow::record_batch::RecordBatch) -> String {
    let mut hasher = sha2::Sha256::new();
    for field in batch.schema().fields() {
        hasher.update(field.name().as_bytes());
        hasher.update([0]);
        hasher.update(format!("{:?}", field.data_type()).as_bytes());
        hasher.update([0]);
        hasher.update([u8::from(field.is_nullable())]);
    }
    hasher.update(batch.num_rows().to_le_bytes());
    for column in batch.columns() {
        hasher.update(column.len().to_le_bytes());
        hasher.update(column.null_count().to_le_bytes());
        hasher.update(format!("{column:?}").as_bytes());
    }
    let digest: [u8; 32] = hasher.finalize().into();
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(64);
    for byte in digest {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0xf) as usize] as char);
    }
    out
}

fn add_person_with_heuristic(graph: &GraphForge, name: &str, heuristic: f64) -> NodeHandle {
    add_person_with_heuristic_value(graph, name, PropValue::Float(heuristic))
}

#[test]
fn dijkstra_is_uuid_only_weighted_deterministic_and_knowledge_independent() {
    let graph = GraphForge::new(None).unwrap();
    let nodes = ["Alice", "Bob", "Carol", "Dan", "Eve"].map(|name| add_person(&graph, name));
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (e:Person {name:'Eve'}) \
                 CREATE (a)-[:ROAD {cost:1.0}]->(c), \
                 (a)-[:ROAD {cost:1.0}]->(b), (b)-[:ROAD {cost:2.0}]->(d), \
                 (c)-[:ROAD {cost:2.0}]->(d), (a)-[:ROAD {cost:9.0}]->(d), \
                 (d)-[:OTHER {cost:0.5}]->(e)",
        )
        .unwrap();
    let source = NodeSelector::Handle(nodes[0].clone());
    let options = dijkstra_options(true, Some("ROAD"), Some("cost"));
    let all = graph.paths(&source, None, options.clone()).unwrap();

    assert_eq!(all.schema().metadata()["graphforge.algorithm"], "dijkstra");
    assert_eq!(
        all.schema()
            .fields()
            .iter()
            .map(|field| (field.name().as_str(), field.is_nullable()))
            .collect::<Vec<_>>(),
        [
            ("source_uuid", false),
            ("target_uuid", false),
            ("cost", false),
            ("path", false),
        ]
    );
    assert!(all.column_by_name("source_id").is_none());
    assert_eq!(
        all.column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values(),
        &[0.0, 1.0, 1.0, 3.0]
    );
    assert_eq!(
        uuid_path(&all, 3),
        [nodes[0].uuid, nodes[1].uuid, nodes[3].uuid].map(|uuid| *uuid.as_bytes())
    );
    assert_eq!(all, graph.paths(&source, None, options.clone()).unwrap());

    let dan = NodeSelector::Handle(nodes[3].clone());
    let target = graph.paths(&source, Some(&dan), options).unwrap();
    assert_eq!(target.num_rows(), 1);
    assert_eq!(uuid_path(&target, 0), uuid_path(&all, 3));
    assert_eq!(
        graph
            .paths(
                &dan,
                Some(&source),
                dijkstra_options(true, Some("ROAD"), Some("cost")),
            )
            .unwrap()
            .num_rows(),
        0
    );
    assert_eq!(
        graph
            .paths(
                &dan,
                Some(&source),
                dijkstra_options(false, Some("ROAD"), Some("cost")),
            )
            .unwrap()
            .num_rows(),
        1
    );
}

#[test]
fn dijkstra_all_pairs_is_uuid_only_ordered_and_source_validated_only() {
    let graph = GraphForge::new(None).unwrap();
    let nodes = ["Alice", "Bob", "Carol", "Dan", "Eve"].map(|name| add_person(&graph, name));
    graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}),                  (c:Person {name:'Carol'}), (d:Person {name:'Dan'}),                  (e:Person {name:'Eve'})                  CREATE (a)-[:ROAD {cost:1.0}]->(c),                  (a)-[:ROAD {cost:1.0}]->(b), (b)-[:ROAD {cost:2.0}]->(d),                  (c)-[:ROAD {cost:2.0}]->(d), (a)-[:ROAD {cost:9.0}]->(d),                  (d)-[:ROAD {cost:0.5}]->(e)",
            )
            .unwrap();
    let source = NodeSelector::Handle(nodes[4].clone());
    let options = dijkstra_all_pairs_options(true, Some("ROAD"), Some("cost"));
    let batch = graph.paths(&source, None, options.clone()).unwrap();

    assert_eq!(
        batch.schema().metadata()["graphforge.algorithm"],
        "dijkstra_all_pairs"
    );
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| (field.name().as_str(), field.is_nullable()))
            .collect::<Vec<_>>(),
        [
            ("source_uuid", false),
            ("target_uuid", false),
            ("cost", false),
            ("path", false),
        ]
    );
    assert!(batch.column_by_name("source_id").is_none());
    assert_eq!(batch.num_rows(), 9);
    let expected = [
        (0, 1, 1.0, vec![0, 1]),
        (0, 2, 1.0, vec![0, 2]),
        (0, 3, 3.0, vec![0, 1, 3]),
        (0, 4, 3.5, vec![0, 1, 3, 4]),
        (1, 3, 2.0, vec![1, 3]),
        (1, 4, 2.5, vec![1, 3, 4]),
        (2, 3, 2.0, vec![2, 3]),
        (2, 4, 2.5, vec![2, 3, 4]),
        (3, 4, 0.5, vec![3, 4]),
    ];
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
    let costs = batch
        .column_by_name("cost")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    for (row, (source, target, cost, path)) in expected.iter().enumerate() {
        assert_eq!(sources.value(row), nodes[*source].uuid.as_bytes());
        assert_eq!(targets.value(row), nodes[*target].uuid.as_bytes());
        assert_eq!(costs.value(row), *cost);
        assert_eq!(
            uuid_path(&batch, row),
            path.iter()
                .map(|index| *nodes[*index].uuid.as_bytes())
                .collect::<Vec<_>>()
        );
    }
    assert_eq!(batch, graph.paths(&source, None, options).unwrap());

    let target = NodeSelector::Handle(nodes[0].clone());
    assert!(matches!(
        graph.paths(&source, Some(&target), dijkstra_all_pairs_options(true, Some("ROAD"), Some("cost"))),
        Err(GfError::Validation(message)) if message.contains("target")
    ));
    assert!(matches!(
        graph.paths(
            &source,
            None,
            PathsOptions { k: 2, ..dijkstra_all_pairs_options(true, None, None) }
        ),
        Err(GfError::Validation(message)) if message.contains("k must be 1")
    ));
}

#[test]
fn dijkstra_all_pairs_public_fingerprint_matches_thread_configs() {
    const NODE_COUNT: usize = 48;
    const OFFSETS: [usize; 4] = [1, 5, 17, 31];

    fn policy(workers: usize) -> ExecutionResourcePolicy {
        ExecutionResourcePolicy {
            mode: ResourcePolicyMode::Explicit,
            tokio_worker_threads: Some(workers),
            target_partitions: Some(workers),
            batch_size: Some(8_192),
            memory_budget_bytes: Some(512 * 1024 * 1024),
            spill: SpillPolicy::default(),
            io_concurrency: Some(workers),
            max_concurrent_heavy_queries: Some(1),
            compute_threads: Some(workers),
        }
    }

    fn automatic_policy() -> ExecutionResourcePolicy {
        ExecutionResourcePolicy {
            mode: ResourcePolicyMode::Automatic,
            tokio_worker_threads: None,
            target_partitions: None,
            batch_size: None,
            memory_budget_bytes: None,
            spill: SpillPolicy::default(),
            io_concurrency: None,
            max_concurrent_heavy_queries: None,
            compute_threads: None,
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let source_uuid = {
        let graph = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
        let nodes = (0..NODE_COUNT)
            .map(|index| add_person(&graph, &format!("n{index}")))
            .collect::<Vec<_>>();
        let match_clause = (0..NODE_COUNT)
            .map(|index| format!("(n{index}:Person {{name:'n{index}'}})"))
            .collect::<Vec<_>>()
            .join(", ");
        let create_clause = (0..NODE_COUNT)
            .flat_map(|source| {
                OFFSETS.into_iter().map(move |offset| {
                    let target = (source + offset) % NODE_COUNT;
                    let cost = 1.0 + ((source + target) % 7) as f64 / 10.0;
                    format!("(n{source})-[:ROAD {{cost:{cost:.1}}}]->(n{target})")
                })
            })
            .collect::<Vec<_>>()
            .join(", ");
        graph
            .execute(&format!("MATCH {match_clause} CREATE {create_clause}"))
            .unwrap();
        nodes[0].uuid
    };

    let configs = [
        ("threads-1", policy(1)),
        ("threads-2", policy(2)),
        ("threads-4", policy(4)),
        ("threads-8", policy(8)),
        ("threads-automatic", automatic_policy()),
    ];
    let mut baseline: Option<(Vec<(String, String, bool)>, usize, String)> = None;
    let mut executed = 0_usize;
    for (id, resource) in configs {
        let graph = match GraphForge::new_with_options(
            Some(dir.path().to_str().unwrap()),
            GraphForgeOptions {
                resource,
                ..GraphForgeOptions::default()
            },
        ) {
            Ok(graph) => graph,
            Err(error) => {
                eprintln!("{id}: unavailable resource policy: {error}");
                continue;
            }
        };
        let batch = graph
            .paths(
                &NodeSelector::Uuid(source_uuid),
                None,
                dijkstra_all_pairs_options(true, Some("ROAD"), Some("cost")),
            )
            .unwrap_or_else(|error| panic!("{id}: {error}"));
        let schema = batch
            .schema()
            .fields()
            .iter()
            .map(|field| {
                (
                    field.name().clone(),
                    format!("{:?}", field.data_type()),
                    field.is_nullable(),
                )
            })
            .collect::<Vec<_>>();
        let fingerprint = arrow_batch_fingerprint(&batch);
        eprintln!("{id}: rows={} fingerprint={fingerprint}", batch.num_rows());
        let observed = (schema, batch.num_rows(), fingerprint);
        if let Some(expected) = &baseline {
            assert_eq!(&observed, expected, "{id}: dijkstra_all_pairs parity");
        } else {
            assert_eq!(batch.num_rows(), NODE_COUNT * (NODE_COUNT - 1));
            baseline = Some(observed);
        }
        executed += 1;
    }
    assert!(
        executed >= 2,
        "expected at least threads-1 and one multi-thread/automatic dijkstra_all_pairs cell"
    );
}

#[test]
fn dijkstra_all_pairs_covers_empty_disconnected_undirected_and_weight_errors() {
    let empty = GraphForge::new(None).unwrap();
    let missing = NodeSelector::Uuid(graphforge_core::uuid::new_v7());
    assert!(matches!(
        empty.paths(&missing, None, dijkstra_all_pairs_options(true, None, None)),
        Err(GfError::Validation(_))
    ));

    let graph = GraphForge::new(None).unwrap();
    let alice = add_person(&graph, "Alice");
    let bob = add_person(&graph, "Bob");
    let carol = add_person(&graph, "Carol");
    graph
            .execute(
                "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'})                  CREATE (a)-[:ROAD {cost:1.0, bad:-1.0}]->(b)",
            )
            .unwrap();
    let source = NodeSelector::Handle(carol);
    assert_eq!(
        graph
            .paths(
                &source,
                None,
                dijkstra_all_pairs_options(true, Some("ROAD"), Some("cost"))
            )
            .unwrap()
            .num_rows(),
        1
    );
    assert_eq!(
        graph
            .paths(
                &source,
                None,
                dijkstra_all_pairs_options(false, Some("ROAD"), Some("cost"))
            )
            .unwrap()
            .num_rows(),
        2
    );
    assert!(matches!(
        graph.paths(
            &NodeSelector::Handle(alice),
            None,
            dijkstra_all_pairs_options(true, Some("ROAD"), Some("bad"))
        ),
        Err(GfError::Validation(_)) | Err(GfError::Algorithm(AlgorithmError::Execution { .. }))
    ));
    assert!(matches!(
        graph.paths(
            &NodeSelector::Handle(bob),
            None,
            dijkstra_all_pairs_options(true, Some("ROAD"), Some("missing"))
        ),
        Err(GfError::Validation(_)) | Err(GfError::Algorithm(AlgorithmError::Execution { .. }))
    ));
}

#[test]
fn dijkstra_defaults_to_unit_cost_and_rejects_invalid_weight_contracts() {
    let graph = GraphForge::new(None).unwrap();
    let alice = add_person(&graph, "Alice");
    let bob = add_person(&graph, "Bob");
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}) \
                 CREATE (a)-[:ROAD {negative:-1.0}]->(b)",
        )
        .unwrap();
    let source = NodeSelector::Handle(alice);
    let target = NodeSelector::Handle(bob);
    let unit = graph
        .paths(
            &source,
            Some(&target),
            dijkstra_options(true, Some("ROAD"), None),
        )
        .unwrap();
    assert_eq!(
        unit.column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        1.0
    );
    for options in [
        PathsOptions {
            k: 2,
            ..dijkstra_options(true, None, None)
        },
        dijkstra_options(true, Some("ROAD"), Some(" ")),
        dijkstra_options(true, Some("ROAD"), Some("missing")),
        dijkstra_options(true, Some("ROAD"), Some("negative")),
    ] {
        assert!(matches!(
            graph.paths(&source, Some(&target), options),
            Err(GfError::Validation(_)) | Err(GfError::Algorithm(AlgorithmError::Execution { .. }))
        ));
    }
}

#[test]
fn bellman_ford_is_exact_uuid_only_deterministic_and_knowledge_independent() {
    // Exploratory mode has no ontology/knowledge layer (#772).
    let graph = GraphForge::new(None).unwrap();
    let nodes = ["Alice", "Bob", "Carol", "Dan", "Eve"].map(|name| add_person(&graph, name));
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (e:Person {name:'Eve'}) \
                 CREATE (a)-[:ROAD {cost:5.0}]->(c), \
                 (a)-[:ROAD {cost:4.0}]->(b), (b)-[:ROAD {cost:-2.0}]->(c), \
                 (b)-[:ROAD {cost:6.0}]->(d), (c)-[:ROAD {cost:3.0}]->(d), \
                 (d)-[:ROAD {cost:-1.0}]->(e), \
                 (a)-[:UNIT]->(b), (b)-[:UNIT]->(e), (d)-[:BACK]->(a)",
        )
        .unwrap();
    let source = NodeSelector::Handle(nodes[0].clone());
    let options = bellman_ford_options(true, Some("ROAD"), Some("cost"));
    let all = graph.paths(&source, None, options.clone()).unwrap();

    assert_eq!(
        all.schema().metadata()["graphforge.algorithm"],
        "bellman_ford"
    );
    assert_eq!(
        all.schema()
            .fields()
            .iter()
            .map(|field| (field.name().as_str(), field.is_nullable()))
            .collect::<Vec<_>>(),
        [
            ("source_uuid", false),
            ("target_uuid", false),
            ("cost", false),
            ("path", false),
        ]
    );
    assert!(all.column_by_name("source_id").is_none());
    let sources = all
        .column_by_name("source_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let targets = all
        .column_by_name("target_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert!((0..all.num_rows()).all(|row| sources.value(row) == nodes[0].uuid.as_bytes()));
    assert_eq!(
        (0..all.num_rows())
            .map(|row| targets.value(row))
            .collect::<Vec<_>>(),
        nodes
            .iter()
            .map(|node| node.uuid.as_bytes())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        all.column_by_name("cost")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values(),
        &[0.0, 4.0, 2.0, 5.0, 4.0]
    );
    assert_eq!(
        uuid_path(&all, 4),
        [
            nodes[0].uuid,
            nodes[1].uuid,
            nodes[2].uuid,
            nodes[3].uuid,
            nodes[4].uuid,
        ]
        .map(|uuid| *uuid.as_bytes())
    );
    assert_eq!(all, graph.paths(&source, None, options.clone()).unwrap());

    let eve = NodeSelector::Handle(nodes[4].clone());
    let target = graph.paths(&source, Some(&eve), options).unwrap();
    assert_eq!(target.num_rows(), 1);
    assert_eq!(uuid_path(&target, 0), uuid_path(&all, 4));

    let unit = graph
        .paths(
            &source,
            Some(&eve),
            bellman_ford_options(true, Some("UNIT"), None),
        )
        .unwrap();
    assert_eq!(
        unit.column_by_name("cost")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        2.0
    );
    let dan = NodeSelector::Handle(nodes[3].clone());
    assert_eq!(
        graph
            .paths(
                &source,
                Some(&dan),
                bellman_ford_options(true, Some("BACK"), None),
            )
            .unwrap()
            .num_rows(),
        0
    );
    assert_eq!(
        graph
            .paths(
                &source,
                Some(&dan),
                bellman_ford_options(false, Some("BACK"), None),
            )
            .unwrap()
            .num_rows(),
        1
    );
    let singleton = graph
        .paths(
            &source,
            Some(&source),
            bellman_ford_options(true, Some("ROAD"), Some("cost")),
        )
        .unwrap();
    assert_eq!(uuid_path(&singleton, 0), [*nodes[0].uuid.as_bytes()]);
}

#[test]
fn bellman_ford_negative_cycle_scope_is_structured() {
    let reachable = GraphForge::new(None).unwrap();
    let source = add_person(&reachable, "Alice");
    let target = add_person(&reachable, "Dan");
    add_person(&reachable, "Bob");
    reachable
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (d:Person {name:'Dan'}) \
                 CREATE (a)-[:ROAD {cost:1.0}]->(b), \
                 (b)-[:ROAD {cost:-2.0}]->(a), (a)-[:ROAD {cost:5.0}]->(d)",
        )
        .unwrap();
    assert!(matches!(
        reachable.paths(
            &NodeSelector::Handle(source),
            Some(&NodeSelector::Handle(target)),
            bellman_ford_options(true, Some("ROAD"), Some("cost")),
        ).map_err(expect_algorithm_execution),
        Err(AlgorithmError::Execution { message }) if message.contains("negative cycle")
    ));

    let unreachable = GraphForge::new(None).unwrap();
    let source = add_person(&unreachable, "Alice");
    add_person(&unreachable, "Bob");
    add_person(&unreachable, "Carol");
    add_person(&unreachable, "Dan");
    unreachable
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}) \
                 CREATE (a)-[:ROAD {cost:2.0}]->(b), \
                 (c)-[:ROAD {cost:-2.0}]->(d), (d)-[:ROAD {cost:1.0}]->(c)",
        )
        .unwrap();
    assert_eq!(
        unreachable
            .paths(
                &NodeSelector::Handle(source),
                None,
                bellman_ford_options(true, Some("ROAD"), Some("cost")),
            )
            .unwrap()
            .num_rows(),
        2
    );
}

#[test]
fn bellman_ford_rejects_invalid_options_and_strict_weight_values() {
    let graph = GraphForge::new(None).unwrap();
    let source = add_person(&graph, "Alice");
    let target = add_person(&graph, "Bob");
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}) \
                 CREATE (a)-[:ROAD {null_cost:null, text_cost:'heavy', \
                 infinite_cost:1e308 * 2.0}]->(b)",
        )
        .unwrap();
    let source = NodeSelector::Handle(source);
    let target = NodeSelector::Handle(target);
    for options in [
        PathsOptions {
            k: 2,
            ..bellman_ford_options(true, Some("ROAD"), None)
        },
        PathsOptions {
            heuristic: Some("estimate".into()),
            ..bellman_ford_options(true, Some("ROAD"), None)
        },
        bellman_ford_options(true, Some(" "), None),
        bellman_ford_options(true, Some("ROAD"), Some(" ")),
        bellman_ford_options(true, Some("ROAD"), Some("missing")),
        bellman_ford_options(true, Some("ROAD"), Some("null_cost")),
        bellman_ford_options(true, Some("ROAD"), Some("text_cost")),
        bellman_ford_options(true, Some("ROAD"), Some("infinite_cost")),
    ] {
        assert!(matches!(
            graph.paths(&source, Some(&target), options),
            Err(GfError::Validation(_))
        ));
    }
    assert!(matches!(
        graph.paths(
            &NodeSelector::Uuid(graphforge_core::uuid::new_v7()),
            Some(&target),
            bellman_ford_options(true, None, None),
        ),
        Err(GfError::Validation(_))
    ));
}

#[test]
fn floyd_warshall_is_exact_uuid_only_deterministic_and_knowledge_independent() {
    // Exploratory mode has no ontology/knowledge layer (#772).
    let graph = GraphForge::new(None).unwrap();
    let nodes = ["Alice", "Bob", "Carol", "Dan", "Eve"].map(|name| add_person(&graph, name));
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}), \
                 (e:Person {name:'Eve'}) \
                 CREATE (a)-[:ROAD {cost:5.0}]->(c), \
                 (a)-[:ROAD {cost:4.0}]->(b), (b)-[:ROAD {cost:-2.0}]->(c), \
                 (b)-[:ROAD {cost:6.0}]->(d), (c)-[:ROAD {cost:3.0}]->(d), \
                 (d)-[:ROAD {cost:-1.0}]->(e), \
                 (a)-[:UNIT]->(b), (b)-[:UNIT]->(e), (d)-[:BACK]->(a)",
        )
        .unwrap();
    let source = NodeSelector::Handle(nodes[4].clone());
    let options = floyd_warshall_options(true, Some("ROAD"), Some("cost"));
    let batch = graph.paths(&source, None, options.clone()).unwrap();

    assert_eq!(
        batch.schema().metadata()["graphforge.algorithm"],
        "floyd_warshall"
    );
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| (field.name().as_str(), field.is_nullable()))
            .collect::<Vec<_>>(),
        [
            ("source_uuid", false),
            ("target_uuid", false),
            ("cost", false),
            ("path", false),
        ]
    );
    assert!(batch.column_by_name("source_id").is_none());
    assert_eq!(
        batch
            .column_by_name("cost")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values(),
        &[4.0, 2.0, 5.0, 4.0, -2.0, 1.0, 0.0, 3.0, 2.0, -1.0]
    );
    assert_eq!(
        uuid_path(&batch, 3),
        nodes
            .iter()
            .map(|node| *node.uuid.as_bytes())
            .collect::<Vec<_>>()
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
    let pairs = (0..batch.num_rows())
        .map(|row| (sources.value(row), targets.value(row)))
        .collect::<Vec<_>>();
    assert_eq!(
        pairs,
        vec![
            (
                nodes[0].uuid.as_bytes().as_slice(),
                nodes[1].uuid.as_bytes().as_slice()
            ),
            (
                nodes[0].uuid.as_bytes().as_slice(),
                nodes[2].uuid.as_bytes().as_slice()
            ),
            (
                nodes[0].uuid.as_bytes().as_slice(),
                nodes[3].uuid.as_bytes().as_slice()
            ),
            (
                nodes[0].uuid.as_bytes().as_slice(),
                nodes[4].uuid.as_bytes().as_slice()
            ),
            (
                nodes[1].uuid.as_bytes().as_slice(),
                nodes[2].uuid.as_bytes().as_slice()
            ),
            (
                nodes[1].uuid.as_bytes().as_slice(),
                nodes[3].uuid.as_bytes().as_slice()
            ),
            (
                nodes[1].uuid.as_bytes().as_slice(),
                nodes[4].uuid.as_bytes().as_slice()
            ),
            (
                nodes[2].uuid.as_bytes().as_slice(),
                nodes[3].uuid.as_bytes().as_slice()
            ),
            (
                nodes[2].uuid.as_bytes().as_slice(),
                nodes[4].uuid.as_bytes().as_slice()
            ),
            (
                nodes[3].uuid.as_bytes().as_slice(),
                nodes[4].uuid.as_bytes().as_slice()
            ),
        ]
    );
    assert_eq!(batch, graph.paths(&source, None, options.clone()).unwrap());
    assert!(matches!(
        graph.paths(
            &source,
            Some(&NodeSelector::Handle(nodes[0].clone())),
            options
        ),
        Err(GfError::Validation(message))
            if message == "floyd_warshall does not accept a target selector"
    ));
}

#[test]
fn floyd_warshall_covers_projection_cycles_and_strict_errors() {
    let graph = GraphForge::new(None).unwrap();
    let nodes = ["Alice", "Bob", "Carol", "Dan"].map(|name| add_person(&graph, name));
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}) \
                 CREATE (a)-[:ROAD {cost:2.0, null_cost:null, text_cost:'heavy', \
                 infinite_cost:1e308 * 2.0}]->(b), \
                 (c)-[:CYCLE {cost:-2.0}]->(d), (d)-[:CYCLE {cost:1.0}]->(c), \
                 (a)-[:UNIT]->(b), (d)-[:BACK]->(a)",
        )
        .unwrap();
    let source = NodeSelector::Handle(nodes[0].clone());
    assert_eq!(
        graph
            .paths(
                &source,
                None,
                floyd_warshall_options(true, Some("UNIT"), None)
            )
            .unwrap()
            .num_rows(),
        1
    );
    assert_eq!(
        graph
            .paths(
                &source,
                None,
                floyd_warshall_options(false, Some("BACK"), None)
            )
            .unwrap()
            .num_rows(),
        2
    );
    assert!(matches!(
        graph.paths(
            &source,
            None,
            floyd_warshall_options(true, Some("CYCLE"), Some("cost"))
        ).map_err(expect_algorithm_execution),
        Err(AlgorithmError::Execution { message }) if message.contains("negative cycle")
    ));
    for options in [
        PathsOptions {
            k: 2,
            ..floyd_warshall_options(true, Some("ROAD"), None)
        },
        PathsOptions {
            heuristic: Some("estimate".into()),
            ..floyd_warshall_options(true, Some("ROAD"), None)
        },
        floyd_warshall_options(true, Some(" "), None),
        floyd_warshall_options(true, Some("ROAD"), Some(" ")),
        floyd_warshall_options(true, Some("ROAD"), Some("missing")),
        floyd_warshall_options(true, Some("ROAD"), Some("null_cost")),
        floyd_warshall_options(true, Some("ROAD"), Some("text_cost")),
        floyd_warshall_options(true, Some("ROAD"), Some("infinite_cost")),
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
            floyd_warshall_options(true, None, None),
        ),
        Err(GfError::Validation(_))
    ));
}

#[test]
fn delta_stepping_is_exact_uuid_only_deterministic_and_knowledge_independent() {
    // Exploratory mode proves graph algorithms do not require a knowledge layer (#772).
    let graph = GraphForge::new(None).unwrap();
    let nodes = ["Alice", "Bob", "Carol", "Dan", "Eve"].map(|name| add_person(&graph, name));
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}) \
                 CREATE (a)-[:ROAD {cost:1.0}]->(c), \
                 (a)-[:ROAD {cost:0.5}]->(b), (b)-[:ROAD {cost:0.5}]->(c), \
                 (a)-[:ROAD {cost:5.0}]->(d), (c)-[:ROAD {cost:2.0}]->(d), \
                 (a)-[:UNIT]->(b), (b)-[:UNIT]->(d), (d)-[:BACK]->(a)",
        )
        .unwrap();
    let source = NodeSelector::Handle(nodes[0].clone());
    let options = delta_stepping_options(true, Some("ROAD"), Some("cost"));
    let all = graph.paths(&source, None, options.clone()).unwrap();

    assert_eq!(
        all.schema().metadata()["graphforge.algorithm"],
        "delta_stepping"
    );
    assert_eq!(
        all.schema()
            .fields()
            .iter()
            .map(|field| (field.name().as_str(), field.is_nullable()))
            .collect::<Vec<_>>(),
        [
            ("source_uuid", false),
            ("target_uuid", false),
            ("cost", false),
            ("path", false),
        ]
    );
    assert!(all.column_by_name("source_id").is_none());
    let sources = all
        .column_by_name("source_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let targets = all
        .column_by_name("target_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert!((0..all.num_rows()).all(|row| sources.value(row) == nodes[0].uuid.as_bytes()));
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
        all.column_by_name("cost")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values(),
        &[0.0, 0.5, 1.0, 3.0]
    );
    assert_eq!(
        uuid_path(&all, 2),
        [nodes[0].uuid, nodes[1].uuid, nodes[2].uuid].map(|uuid| *uuid.as_bytes())
    );
    assert_eq!(all, graph.paths(&source, None, options.clone()).unwrap());

    let dan = NodeSelector::Handle(nodes[3].clone());
    let target = graph.paths(&source, Some(&dan), options).unwrap();
    assert_eq!(target.num_rows(), 1);
    assert_eq!(uuid_path(&target, 0), uuid_path(&all, 3));
    let eve = NodeSelector::Handle(nodes[4].clone());
    assert_eq!(
        graph
            .paths(
                &source,
                Some(&eve),
                delta_stepping_options(true, Some("ROAD"), Some("cost")),
            )
            .unwrap()
            .num_rows(),
        0
    );
    let unit = graph
        .paths(
            &source,
            Some(&dan),
            delta_stepping_options(true, Some("UNIT"), None),
        )
        .unwrap();
    assert_eq!(
        unit.column_by_name("cost")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        2.0
    );
    assert_eq!(
        graph
            .paths(
                &source,
                Some(&dan),
                delta_stepping_options(true, Some("BACK"), None),
            )
            .unwrap()
            .num_rows(),
        0
    );
    assert_eq!(
        graph
            .paths(
                &source,
                Some(&dan),
                delta_stepping_options(false, Some("BACK"), None),
            )
            .unwrap()
            .num_rows(),
        1
    );
    let singleton = graph
        .paths(
            &source,
            Some(&source),
            delta_stepping_options(true, Some("ROAD"), Some("cost")),
        )
        .unwrap();
    assert_eq!(uuid_path(&singleton, 0), [*nodes[0].uuid.as_bytes()]);
}

#[test]
fn delta_stepping_rejects_invalid_options_and_strict_weight_values() {
    let graph = GraphForge::new(None).unwrap();
    let source = add_person(&graph, "Alice");
    let target = add_person(&graph, "Bob");
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}) \
                 CREATE (a)-[:ROAD {negative:-1.0, null_cost:null, \
                 text_cost:'heavy', infinite_cost:1e308 * 2.0}]->(b)",
        )
        .unwrap();
    let source = NodeSelector::Handle(source);
    let target = NodeSelector::Handle(target);
    for options in [
        PathsOptions {
            k: 2,
            ..delta_stepping_options(true, Some("ROAD"), None)
        },
        PathsOptions {
            heuristic: Some("estimate".into()),
            ..delta_stepping_options(true, Some("ROAD"), None)
        },
        delta_stepping_options(true, Some(" "), None),
        delta_stepping_options(true, Some("ROAD"), Some(" ")),
        delta_stepping_options(true, Some("ROAD"), Some("missing")),
        delta_stepping_options(true, Some("ROAD"), Some("null_cost")),
        delta_stepping_options(true, Some("ROAD"), Some("text_cost")),
        delta_stepping_options(true, Some("ROAD"), Some("infinite_cost")),
    ] {
        assert!(matches!(
            graph.paths(&source, Some(&target), options),
            Err(GfError::Validation(_))
        ));
    }
    assert!(matches!(
        graph.paths(
            &source,
            Some(&target),
            delta_stepping_options(true, Some("ROAD"), Some("negative")),
        ).map_err(expect_algorithm_execution),
        Err(AlgorithmError::Execution { message })
            if message.contains("requires finite non-negative edge weights")
    ));
    assert!(matches!(
        graph.paths(
            &NodeSelector::Uuid(graphforge_core::uuid::new_v7()),
            Some(&target),
            delta_stepping_options(true, None, None),
        ),
        Err(GfError::Validation(_))
    ));
}

#[test]
fn yens_is_ranked_uuid_only_deterministic_and_knowledge_independent() {
    // Exploratory mode proves graph algorithms do not require a knowledge layer (#772).
    let graph = GraphForge::new(None).unwrap();
    let nodes = ["Alice", "Bob", "Carol", "Dan", "Eve"].map(|name| add_person(&graph, name));
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}) \
                 CREATE (a)-[:ROAD {cost:4.0}]->(b), \
                 (a)-[:ROAD {cost:1.0}]->(b), (b)-[:ROAD {cost:2.0}]->(d), \
                 (a)-[:ROAD {cost:1.0}]->(c), (c)-[:ROAD {cost:2.0}]->(d), \
                 (b)-[:ROAD {cost:0.5}]->(c), (a)-[:ROAD {cost:4.0}]->(d), \
                 (a)-[:ROAD {cost:0.0}]->(a), (c)-[:ROAD {cost:0.0}]->(a), \
                 (a)-[:UNIT]->(d), (a)-[:UNIT]->(b), (b)-[:UNIT]->(d)",
        )
        .unwrap();
    let source = NodeSelector::Handle(nodes[0].clone());
    let target = NodeSelector::Handle(nodes[3].clone());
    let options = yens_options(true, 10, Some("ROAD"), Some("cost"));
    let batch = graph
        .paths(&source, Some(&target), options.clone())
        .unwrap();

    assert_eq!(batch.schema().metadata()["graphforge.algorithm"], "yens");
    assert_eq!(batch.schema().metadata()["graphforge.verb"], "paths");
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| (field.name().as_str(), field.is_nullable()))
            .collect::<Vec<_>>(),
        [
            ("source_uuid", false),
            ("target_uuid", false),
            ("rank", false),
            ("cost", false),
            ("path", false),
        ]
    );
    assert_eq!(batch.schema().field(2).data_type(), &DataType::UInt64);
    assert!(matches!(
        batch.schema().field(4).data_type(),
        DataType::List(field)
            if field.data_type() == &DataType::FixedSizeBinary(16) && !field.is_nullable()
    ));
    assert!(batch.column_by_name("source_id").is_none());
    assert_eq!(
        batch
            .column_by_name("rank")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .values(),
        &[1, 2, 3, 4]
    );
    assert_eq!(
        batch
            .column_by_name("cost")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values(),
        &[3.0, 3.0, 3.5, 4.0]
    );
    let expected = [vec![0, 1, 3], vec![0, 2, 3], vec![0, 1, 2, 3], vec![0, 3]];
    for (row, path) in expected.iter().enumerate() {
        assert_eq!(
            uuid_path(&batch, row),
            path.iter()
                .map(|index| *nodes[*index].uuid.as_bytes())
                .collect::<Vec<_>>()
        );
    }
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
    assert!((0..4).all(|row| sources.value(row) == nodes[0].uuid.as_bytes()));
    assert!((0..4).all(|row| targets.value(row) == nodes[3].uuid.as_bytes()));
    assert_eq!(batch, graph.paths(&source, Some(&target), options).unwrap());

    let unit = graph
        .paths(
            &source,
            Some(&target),
            yens_options(true, 2, Some("UNIT"), None),
        )
        .unwrap();
    assert_eq!(
        unit.column_by_name("cost")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values(),
        &[1.0, 2.0]
    );
    assert_eq!(
        graph
            .paths(
                &target,
                Some(&source),
                yens_options(true, 2, Some("ROAD"), Some("cost")),
            )
            .unwrap()
            .num_rows(),
        0
    );
    assert!(
        graph
            .paths(
                &target,
                Some(&source),
                yens_options(false, 2, Some("ROAD"), Some("cost")),
            )
            .unwrap()
            .num_rows()
            > 0
    );
    assert_eq!(
        graph
            .paths(
                &source,
                Some(&NodeSelector::Handle(nodes[4].clone())),
                yens_options(true, 2, Some("ROAD"), Some("cost")),
            )
            .unwrap()
            .num_rows(),
        0
    );
    let singleton = graph
        .paths(
            &source,
            Some(&source),
            yens_options(true, 4, Some("ROAD"), Some("cost")),
        )
        .unwrap();
    assert_eq!(
        (
            singleton.num_rows(),
            singleton
                .column_by_name("rank")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0),
            uuid_path(&singleton, 0),
        ),
        (1, 1, vec![*nodes[0].uuid.as_bytes()])
    );
}

#[test]
fn yens_rejects_missing_target_invalid_options_and_strict_weights() {
    let graph = GraphForge::new(None).unwrap();
    let source = add_person(&graph, "Alice");
    let target = add_person(&graph, "Bob");
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}) \
                 CREATE (a)-[:ROAD {cost:-1.0, null_cost:null, text_cost:'heavy', \
                 infinite_cost:1e308 * 2.0}]->(b)",
        )
        .unwrap();
    let source = NodeSelector::Handle(source);
    let target = NodeSelector::Handle(target);
    assert!(matches!(
        graph.paths(&source, None, yens_options(true, 2, None, None)),
        Err(GfError::Validation(message)) if message == "yens requires a target selector"
    ));
    assert!(matches!(
        graph.paths(
            &source,
            Some(&target),
            yens_options(true, 0, None, None)
        ),
        Err(GfError::Validation(message)) if message == "yens k must be at least 1"
    ));
    assert!(matches!(
        graph.paths(
            &source,
            Some(&target),
            PathsOptions {
                heuristic: Some("estimate".into()),
                ..yens_options(true, 2, None, None)
            }
        ),
        Err(GfError::Validation(message))
            if message == "yens does not accept a heuristic property"
    ));
    for options in [
        yens_options(true, 2, Some(" "), None),
        yens_options(true, 2, Some("ROAD"), Some(" ")),
        yens_options(true, 2, Some("ROAD"), Some("missing")),
        yens_options(true, 2, Some("ROAD"), Some("null_cost")),
        yens_options(true, 2, Some("ROAD"), Some("text_cost")),
        yens_options(true, 2, Some("ROAD"), Some("infinite_cost")),
    ] {
        assert!(matches!(
            graph.paths(&source, Some(&target), options),
            Err(GfError::Validation(_))
        ));
    }
    assert!(matches!(
        graph.paths(
            &source,
            Some(&target),
            yens_options(true, 2, Some("ROAD"), Some("cost"))
        ).map_err(expect_algorithm_execution),
        Err(AlgorithmError::Execution { message })
            if message.contains("finite non-negative edge weights")
    ));
}

#[test]
fn astar_is_uuid_only_exact_deterministic_and_knowledge_independent() {
    let graph = GraphForge::new(None).unwrap();
    let nodes = [
        add_person_with_heuristic(&graph, "Alice", 3.0),
        add_person_with_heuristic(&graph, "Bob", 2.0),
        add_person_with_heuristic(&graph, "Carol", 2.0),
        add_person_with_heuristic(&graph, "Dan", 0.0),
        add_person_with_heuristic(&graph, "Eve", 8.0),
    ];
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (c:Person {name:'Carol'}), (d:Person {name:'Dan'}) \
                 CREATE (a)-[:ROAD {cost:1.0}]->(c), \
                 (a)-[:ROAD {cost:1.0}]->(b), (b)-[:ROAD {cost:2.0}]->(d), \
                 (c)-[:ROAD {cost:2.0}]->(d), (a)-[:ROAD {cost:9.0}]->(d)",
        )
        .unwrap();
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (e:Person {name:'Eve'}) \
                 CREATE (a)-[:UNIT]->(b), (b)-[:UNIT]->(e)",
        )
        .unwrap();
    let source = NodeSelector::Handle(nodes[0].clone());
    let target = NodeSelector::Handle(nodes[3].clone());
    let options = astar_options(true, Some("ROAD"), Some("cost"), Some("heuristic"));
    let batch = graph
        .paths(&source, Some(&target), options.clone())
        .unwrap();

    assert_eq!(batch.schema().metadata()["graphforge.algorithm"], "astar");
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| (field.name().as_str(), field.is_nullable()))
            .collect::<Vec<_>>(),
        [
            ("source_uuid", false),
            ("target_uuid", false),
            ("cost", false),
            ("path", false),
        ]
    );
    assert!(batch.column_by_name("source_id").is_none());
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column_by_name("cost")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        3.0
    );
    assert_eq!(
        uuid_path(&batch, 0),
        [nodes[0].uuid, nodes[1].uuid, nodes[3].uuid].map(|uuid| *uuid.as_bytes())
    );
    assert_eq!(batch, graph.paths(&source, Some(&target), options).unwrap());

    let zero = graph
        .paths(
            &source,
            Some(&target),
            astar_options(true, Some("ROAD"), Some("cost"), None),
        )
        .unwrap();
    assert_eq!(uuid_path(&zero, 0), uuid_path(&batch, 0));
    assert_eq!(
        graph
            .paths(
                &target,
                Some(&source),
                astar_options(true, Some("ROAD"), Some("cost"), None),
            )
            .unwrap()
            .num_rows(),
        0
    );
    assert_eq!(
        graph
            .paths(
                &target,
                Some(&source),
                astar_options(false, Some("ROAD"), Some("cost"), None),
            )
            .unwrap()
            .num_rows(),
        1
    );
    let eve = NodeSelector::Handle(nodes[4].clone());
    let unit = graph
        .paths(
            &source,
            Some(&eve),
            astar_options(true, Some("UNIT"), None, None),
        )
        .unwrap();
    assert_eq!(
        unit.column_by_name("cost")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        2.0
    );
    assert_eq!(
        uuid_path(&unit, 0),
        [nodes[0].uuid, nodes[1].uuid, nodes[4].uuid].map(|uuid| *uuid.as_bytes())
    );
    let singleton = graph
        .paths(
            &target,
            Some(&target),
            astar_options(true, None, None, Some("heuristic")),
        )
        .unwrap();
    assert_eq!(uuid_path(&singleton, 0), [*nodes[3].uuid.as_bytes()]);
}

#[test]
fn astar_rejects_missing_target_and_invalid_options_or_properties() {
    let graph = GraphForge::new(None).unwrap();
    let source = add_person_with_heuristic(&graph, "Alice", 1.0);
    let target = add_person_with_heuristic(&graph, "Bob", 0.0);
    graph
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}) \
                 CREATE (a)-[:ROAD {cost:1.0, negative:-1.0}]->(b)",
        )
        .unwrap();
    let source = NodeSelector::Handle(source);
    let target = NodeSelector::Handle(target);

    assert!(matches!(
        graph.paths(
            &source,
            None,
            astar_options(true, Some("ROAD"), Some("cost"), None)
        ),
        Err(GfError::Validation(message)) if message.contains("target")
    ));
    for options in [
        PathsOptions {
            k: 2,
            ..astar_options(true, Some("ROAD"), Some("cost"), None)
        },
        astar_options(true, Some(" "), Some("cost"), None),
        astar_options(true, Some("ROAD"), Some(" "), None),
        astar_options(true, Some("ROAD"), Some("negative"), None),
        astar_options(true, Some("ROAD"), Some("cost"), Some(" ")),
        astar_options(true, Some("ROAD"), Some("cost"), Some("missing")),
    ] {
        assert!(matches!(
            graph.paths(&source, Some(&target), options),
            Err(GfError::Validation(_)) | Err(GfError::Algorithm(AlgorithmError::Execution { .. }))
        ));
    }
    assert!(matches!(
        graph.paths(
            &source,
            Some(&target),
            PathsOptions {
                heuristic: Some("heuristic".into()),
                ..dijkstra_options(true, Some("ROAD"), Some("cost"))
            },
        ),
        Err(GfError::Validation(message)) if message.contains("does not accept")
    ));

    let invalid_target = GraphForge::new(None).unwrap();
    let source = add_person_with_heuristic(&invalid_target, "Alice", 1.0);
    let target = add_person_with_heuristic(&invalid_target, "Bob", 1.0);
    assert!(matches!(
        invalid_target.paths(
            &NodeSelector::Handle(source),
            Some(&NodeSelector::Handle(target)),
            astar_options(true, None, None, Some("heuristic")),
        ).map_err(expect_algorithm_execution),
        Err(AlgorithmError::Execution { message }) if message.contains("target heuristic")
    ));

    for invalid in [
        PropValue::Float(-1.0),
        PropValue::Float(f64::NAN),
        PropValue::Float(f64::INFINITY),
        PropValue::Float(f64::NEG_INFINITY),
        PropValue::Str("near".into()),
    ] {
        let invalid_graph = GraphForge::new(None).unwrap();
        let source = add_person_with_heuristic_value(&invalid_graph, "Alice", invalid);
        let target = add_person_with_heuristic(&invalid_graph, "Bob", 0.0);
        assert!(matches!(
            invalid_graph.paths(
                &NodeSelector::Handle(source),
                Some(&NodeSelector::Handle(target)),
                astar_options(true, None, None, Some("heuristic")),
            ),
            Err(GfError::Validation(_)) | Err(GfError::Algorithm(AlgorithmError::Execution { .. }))
        ));
    }

    let overflow = GraphForge::new(None).unwrap();
    let source = add_person_with_heuristic(&overflow, "Alice", f64::MAX);
    add_person_with_heuristic(&overflow, "Bob", f64::MAX);
    let target = add_person_with_heuristic(&overflow, "Dan", 0.0);
    overflow
        .execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), \
                 (d:Person {name:'Dan'}) \
                 CREATE (a)-[:ROAD {cost:1.7976931348623157e308}]->(b), \
                 (b)-[:ROAD {cost:1.0}]->(d)",
        )
        .unwrap();
    assert!(matches!(
        overflow.paths(
            &NodeSelector::Handle(source),
            Some(&NodeSelector::Handle(target)),
            astar_options(true, Some("ROAD"), Some("cost"), Some("heuristic")),
        ),
        Err(GfError::Validation(_)) | Err(GfError::Algorithm(AlgorithmError::Execution { .. }))
    ));
}
