use super::super::tests::execute;
use super::super::*;

fn execute_minimum_k_spanning_tree(
    graph: &AdjacencyGraph,
    k: usize,
    limits: AlgorithmLimits,
    cancellation: AlgorithmCancellation,
) -> Result<AlgorithmOutput, AlgorithmError> {
    let options = normalize_analyze_options(&AnalyzeOptions {
        by: AnalyzeAlgorithm::MinimumKSpanningTree,
        directed: false,
        k: Some(k),
        ..AnalyzeOptions::default()
    })
    .expect("test k is valid");
    let mut registry = AlgorithmRegistry::default();
    register_option_analyze_algorithm(&mut registry, &options, None)
        .expect("minimum-k registration does not read external options");
    registry.execute(
        Algorithm::Analyze(AnalyzeAlgorithm::MinimumKSpanningTree),
        graph,
        &AlgorithmControl::new(limits, cancellation),
    )
}

#[test]
fn is_dag_handles_empty_disconnected_and_parallel_graphs() {
    for graph in [
        AdjacencyGraph::default(),
        AdjacencyGraph::with_test_edges(6, &[(0, 1), (0, 1), (2, 3), (3, 4)]),
    ] {
        let output = execute(
            &graph,
            AnalyzeAlgorithm::IsDag,
            true,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_eq!(output.rows(), [vec![AlgorithmValue::Boolean(true)]]);
        assert_eq!(
            output.schema,
            Algorithm::Analyze(AnalyzeAlgorithm::IsDag).result_schema()
        );
    }
}

#[test]
fn has_euler_circuit_shapes_boolean_for_empty_and_representative_graphs() {
    for (graph, directed, expected) in [
        (AdjacencyGraph::default(), false, true),
        (
            AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 2), (2, 0)]),
            false,
            true,
        ),
        (
            AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]),
            false,
            false,
        ),
        (
            AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2), (2, 0)]),
            true,
            true,
        ),
        (
            AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2)]),
            true,
            false,
        ),
    ] {
        let output = execute(
            &graph,
            AnalyzeAlgorithm::HasEulerCircuit,
            directed,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_eq!(
            output.schema,
            Algorithm::Analyze(AnalyzeAlgorithm::HasEulerCircuit).result_schema()
        );
        assert_eq!(output.rows(), [vec![AlgorithmValue::Boolean(expected)]]);
    }
}

#[test]
fn has_euler_circuit_uses_shared_limits_and_cancellation() {
    let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2), (2, 0)]);
    assert!(matches!(
        execute(
            &graph,
            AnalyzeAlgorithm::HasEulerCircuit,
            false,
            AlgorithmLimits {
                output_rows: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::OutputLimit { .. })
    ));

    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        execute(
            &graph,
            AnalyzeAlgorithm::HasEulerCircuit,
            false,
            AlgorithmLimits::default(),
            cancellation,
        ),
        Err(AlgorithmError::Cancelled)
    );
}

#[test]
fn has_euler_path_shapes_boolean_for_empty_and_representative_graphs() {
    for (graph, directed, expected) in [
        (AdjacencyGraph::default(), false, true),
        (
            AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 2)]),
            false,
            true,
        ),
        (
            AdjacencyGraph::with_test_edges(4, &[(0, 1), (0, 2), (0, 3)]),
            false,
            false,
        ),
        (
            AdjacencyGraph::with_test_directed_edges(4, &[(0, 1), (1, 2)]),
            true,
            true,
        ),
        (
            AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (0, 2)]),
            true,
            false,
        ),
    ] {
        let output = execute(
            &graph,
            AnalyzeAlgorithm::HasEulerPath,
            directed,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_eq!(
            output.schema,
            Algorithm::Analyze(AnalyzeAlgorithm::HasEulerPath).result_schema()
        );
        assert_eq!(output.rows(), [vec![AlgorithmValue::Boolean(expected)]]);
    }
}

#[test]
fn has_euler_path_uses_shared_limits_and_cancellation() {
    let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
    assert!(matches!(
        execute(
            &graph,
            AnalyzeAlgorithm::HasEulerPath,
            false,
            AlgorithmLimits {
                output_rows: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::OutputLimit { .. })
    ));

    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        execute(
            &graph,
            AnalyzeAlgorithm::HasEulerPath,
            false,
            AlgorithmLimits::default(),
            cancellation,
        ),
        Err(AlgorithmError::Cancelled)
    );
}

fn euler_output(
    graph: &AdjacencyGraph,
    algorithm: AnalyzeAlgorithm,
    directed: bool,
) -> Result<AlgorithmOutput, AlgorithmError> {
    execute(
        graph,
        algorithm,
        directed,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
}

fn uuid(value: u128) -> [u8; 16] {
    value.to_be_bytes()
}

#[test]
fn euler_constructions_shape_empty_and_singleton_selections() {
    for algorithm in [AnalyzeAlgorithm::EulerCircuit, AnalyzeAlgorithm::EulerPath] {
        let empty = euler_output(&AdjacencyGraph::default(), algorithm, false).unwrap();
        assert_eq!(empty.schema, Algorithm::Analyze(algorithm).result_schema());
        assert!(empty.rows().is_empty());

        let singleton =
            euler_output(&AdjacencyGraph::with_test_edges(1, &[]), algorithm, false).unwrap();
        assert_eq!(
            singleton.rows(),
            [vec![
                AlgorithmValue::UuidList(vec![uuid(0)]),
                AlgorithmValue::UuidList(Vec::new()),
            ]]
        );
    }
}

#[test]
fn euler_constructions_dispatch_directed_and_undirected_open_and_closed_trails() {
    let undirected_open =
        AdjacencyGraph::with_test_undirected_multigraph(3, &[(10, 0, 1), (11, 1, 2)]);
    assert_eq!(
        euler_output(&undirected_open, AnalyzeAlgorithm::EulerPath, false)
            .unwrap()
            .rows(),
        [vec![
            AlgorithmValue::UuidList(vec![uuid(0), uuid(1), uuid(2)]),
            AlgorithmValue::UuidList(vec![uuid(10), uuid(11)]),
        ]]
    );
    assert_eq!(
        euler_output(&undirected_open, AnalyzeAlgorithm::EulerCircuit, false),
        Err(AlgorithmError::UndefinedEulerCircuit)
    );

    let directed_open = AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2)]);
    assert_eq!(
        euler_output(&directed_open, AnalyzeAlgorithm::EulerPath, true)
            .unwrap()
            .rows(),
        [vec![
            AlgorithmValue::UuidList(vec![uuid(0), uuid(1), uuid(2)]),
            AlgorithmValue::UuidList(vec![uuid(0), uuid(1)]),
        ]]
    );
    assert_eq!(
        euler_output(&directed_open, AnalyzeAlgorithm::EulerCircuit, true),
        Err(AlgorithmError::UndefinedEulerCircuit)
    );

    for (graph, directed) in [
        (
            AdjacencyGraph::with_test_undirected_multigraph(2, &[(10, 0, 1), (11, 0, 1)]),
            false,
        ),
        (
            AdjacencyGraph::with_test_directed_edges(2, &[(0, 1), (1, 0)]),
            true,
        ),
    ] {
        let circuit = euler_output(&graph, AnalyzeAlgorithm::EulerCircuit, directed).unwrap();
        assert_eq!(circuit.rows().len(), 1);
        assert_eq!(
            circuit.rows()[0][0],
            AlgorithmValue::UuidList(vec![uuid(0), uuid(1), uuid(0)])
        );
        assert_eq!(
            circuit.rows()[0][1],
            AlgorithmValue::UuidList(if directed {
                vec![uuid(0), uuid(1)]
            } else {
                vec![uuid(10), uuid(11)]
            })
        );
    }
}

#[test]
fn euler_constructions_preserve_loops_parallel_edges_and_structured_undefined_errors() {
    let graph =
        AdjacencyGraph::with_test_undirected_multigraph(2, &[(12, 0, 0), (10, 0, 1), (11, 0, 1)]);
    let first = euler_output(&graph, AnalyzeAlgorithm::EulerCircuit, false).unwrap();
    let second = euler_output(&graph, AnalyzeAlgorithm::EulerCircuit, false).unwrap();
    assert_eq!(first, second);
    let rows = first.rows();
    let [row] = rows.as_slice() else {
        panic!("Euler circuit must be one row");
    };
    let [
        AlgorithmValue::UuidList(nodes),
        AlgorithmValue::UuidList(edges),
    ] = row.as_slice()
    else {
        panic!("Euler row must contain UUID lists");
    };
    assert_eq!(nodes.len(), 4);
    assert_eq!(edges.len(), 3);
    assert!(edges.contains(&uuid(10)) && edges.contains(&uuid(11)) && edges.contains(&uuid(12)));

    let non_eulerian =
        AdjacencyGraph::with_test_undirected_multigraph(4, &[(10, 0, 1), (11, 0, 2), (12, 0, 3)]);
    assert_eq!(
        euler_output(&non_eulerian, AnalyzeAlgorithm::EulerPath, false),
        Err(AlgorithmError::UndefinedEulerPath)
    );
}

#[test]
fn euler_constructions_are_repeatable_uuid_rename_equivariant_and_registered_once() {
    let graph = AdjacencyGraph::with_test_directed_edges_and_uuids(
        &[uuid(90), uuid(20), uuid(70)],
        &[(0, 1), (1, 2)],
    );
    let output = euler_output(&graph, AnalyzeAlgorithm::EulerPath, true).unwrap();
    assert_eq!(
        output,
        euler_output(&graph, AnalyzeAlgorithm::EulerPath, true).unwrap()
    );
    assert_eq!(
        output.rows(),
        [vec![
            AlgorithmValue::UuidList(vec![uuid(90), uuid(20), uuid(70)]),
            AlgorithmValue::UuidList(vec![uuid(0), uuid(1)]),
        ]]
    );

    let mut registry = AlgorithmRegistry::default();
    register_analyze_algorithms(&mut registry, true).unwrap();
    for algorithm in [AnalyzeAlgorithm::EulerCircuit, AnalyzeAlgorithm::EulerPath] {
        assert_eq!(
            registry
                .capabilities()
                .iter()
                .filter(|capability| capability.algorithm == Algorithm::Analyze(algorithm))
                .count(),
            1
        );
    }
}

#[test]
fn euler_constructions_propagate_cancellation_and_resource_limits_atomically() {
    let graph = AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2)]);
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        execute(
            &graph,
            AnalyzeAlgorithm::EulerPath,
            true,
            AlgorithmLimits::default(),
            cancellation,
        ),
        Err(AlgorithmError::Cancelled)
    );
    let run = |limits| {
        execute(
            &graph,
            AnalyzeAlgorithm::EulerPath,
            true,
            limits,
            AlgorithmCancellation::default(),
        )
    };
    assert!(matches!(
        run(AlgorithmLimits {
            nodes: 2,
            ..AlgorithmLimits::default()
        }),
        Err(AlgorithmError::NodeLimit { .. })
    ));
    assert!(matches!(
        run(AlgorithmLimits {
            edges: 1,
            ..AlgorithmLimits::default()
        }),
        Err(AlgorithmError::EdgeLimit { .. })
    ));
    assert!(matches!(
        run(AlgorithmLimits {
            output_rows: 0,
            ..AlgorithmLimits::default()
        }),
        Err(AlgorithmError::OutputLimit { .. })
    ));
    assert!(matches!(
        run(AlgorithmLimits {
            iterations: 0,
            ..AlgorithmLimits::default()
        }),
        Err(AlgorithmError::IterationLimit { .. })
    ));
}

#[test]
fn topological_sort_shapes_stable_uuid_order_and_positions() {
    let graph = AdjacencyGraph::with_test_directed_edges_and_uuids(
        &[[40; 16], [10; 16], [30; 16], [20; 16], [50; 16], [60; 16]],
        &[(0, 4), (0, 4), (1, 4), (2, 5), (3, 5)],
    );
    let output = execute(
        &graph,
        AnalyzeAlgorithm::TopologicalSort,
        true,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();

    assert_eq!(
        output.schema,
        Algorithm::Analyze(AnalyzeAlgorithm::TopologicalSort).result_schema()
    );
    assert_eq!(
        output.rows(),
        [1_u64, 3, 2, 0, 4, 5]
            .into_iter()
            .enumerate()
            .map(|(order, node)| vec![
                AlgorithmValue::Uuid(graph.node_uuid(node).unwrap()),
                AlgorithmValue::UInt64(u64::try_from(order).unwrap()),
            ])
            .collect::<Vec<_>>()
    );
}

#[test]
fn dag_longest_path_dispatches_exact_deterministic_cost_and_path() {
    let graph = AdjacencyGraph::with_test_directed_edges_and_uuids(
        &[[40; 16], [10; 16], [30; 16], [20; 16], [50; 16], [60; 16]],
        &[(1, 3), (3, 0), (1, 2), (2, 0), (4, 5)],
    );
    let output = execute(
        &graph,
        AnalyzeAlgorithm::DagLongestPath,
        true,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();

    assert_eq!(
        output.schema,
        Algorithm::Analyze(AnalyzeAlgorithm::DagLongestPath).result_schema()
    );
    assert_eq!(
        output.rows(),
        [vec![
            AlgorithmValue::Float64(2.0),
            AlgorithmValue::UuidList(vec![[10; 16], [20; 16], [40; 16]]),
        ]]
    );
}

#[test]
fn dag_longest_path_handles_empty_cycles_and_shared_controls() {
    assert_eq!(
        execute(
            &AdjacencyGraph::default(),
            AnalyzeAlgorithm::DagLongestPath,
            true,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap()
        .rows(),
        [vec![
            AlgorithmValue::Float64(0.0),
            AlgorithmValue::UuidList(Vec::new()),
        ]]
    );
    assert!(matches!(
        execute(
            &AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2), (2, 0)]),
            AnalyzeAlgorithm::DagLongestPath,
            true,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::Execution { message })
            if message == "dag_longest_path requires a directed acyclic graph"
    ));
    assert!(matches!(
        execute(
            &AdjacencyGraph::default(),
            AnalyzeAlgorithm::DagLongestPath,
            true,
            AlgorithmLimits {
                output_rows: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::OutputLimit { .. })
    ));
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        execute(
            &AdjacencyGraph::default(),
            AnalyzeAlgorithm::DagLongestPath,
            true,
            AlgorithmLimits::default(),
            cancellation,
        ),
        Err(AlgorithmError::Cancelled)
    );
}

#[test]
fn weighted_dag_longest_path_dispatches_signed_cost_and_uuid_path() {
    let graph = AdjacencyGraph::with_test_directed_edges_and_uuids(
        &[[40; 16], [10; 16], [30; 16], [20; 16], [50; 16]],
        &[(1, 3), (3, 0), (1, 2), (2, 0), (4, 0)],
    )
    .with_test_edge_weights(&[2.0, 3.0, 2.0, 3.0, -8.0]);
    let output = execute(
        &graph,
        AnalyzeAlgorithm::DagLongestPathWeighted,
        true,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();

    assert_eq!(
        output.schema,
        Algorithm::Analyze(AnalyzeAlgorithm::DagLongestPathWeighted).result_schema()
    );
    assert_eq!(
        output.rows(),
        [vec![
            AlgorithmValue::Float64(5.0),
            AlgorithmValue::UuidList(vec![[10; 16], [20; 16], [40; 16]]),
        ]]
    );
}

#[test]
fn weighted_dag_longest_path_handles_empty_cycle_and_controls() {
    assert_eq!(
        execute(
            &AdjacencyGraph::default(),
            AnalyzeAlgorithm::DagLongestPathWeighted,
            true,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap()
        .rows(),
        [vec![
            AlgorithmValue::Float64(0.0),
            AlgorithmValue::UuidList(Vec::new()),
        ]]
    );
    assert!(matches!(
        execute(
            &AdjacencyGraph::with_test_directed_edges(2, &[(0, 1), (1, 0)])
                .with_test_edge_weights(&[1.0, 1.0]),
            AnalyzeAlgorithm::DagLongestPathWeighted,
            true,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::Execution { message })
            if message == "dag_longest_path_weighted requires a directed acyclic graph"
    ));
    assert!(matches!(
        execute(
            &AdjacencyGraph::default(),
            AnalyzeAlgorithm::DagLongestPathWeighted,
            true,
            AlgorithmLimits {
                output_rows: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::OutputLimit { .. })
    ));
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        execute(
            &AdjacencyGraph::default(),
            AnalyzeAlgorithm::DagLongestPathWeighted,
            true,
            AlgorithmLimits::default(),
            cancellation,
        ),
        Err(AlgorithmError::Cancelled)
    );
}

#[test]
fn topological_sort_cycles_and_shared_controls_are_structured() {
    for graph in [
        AdjacencyGraph::with_test_directed_edges(1, &[(0, 0)]),
        AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2), (2, 0)]),
    ] {
        assert_eq!(
            execute(
                &graph,
                AnalyzeAlgorithm::TopologicalSort,
                true,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap_err(),
            AlgorithmError::Execution {
                message: "selected graph contains a cycle".into()
            }
        );
    }

    let graph = AdjacencyGraph::with_test_directed_edges(2, &[(0, 1)]);
    assert!(matches!(
        execute(
            &graph,
            AnalyzeAlgorithm::TopologicalSort,
            true,
            AlgorithmLimits {
                output_rows: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::OutputLimit { .. })
    ));
    assert!(matches!(
        execute(
            &graph,
            AnalyzeAlgorithm::TriangleCount,
            false,
            AlgorithmLimits {
                iterations: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::IterationLimit { .. })
    ));
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        execute(
            &graph,
            AnalyzeAlgorithm::TopologicalSort,
            true,
            AlgorithmLimits::default(),
            cancellation,
        ),
        Err(AlgorithmError::Cancelled)
    );
}

#[test]
fn is_dag_rejects_directed_cycles_and_undirected_interpretation() {
    for graph in [
        AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0)]),
        AdjacencyGraph::with_test_edges(2, &[(0, 0)]),
        AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 2), (2, 0)]),
    ] {
        assert_eq!(
            execute(
                &graph,
                AnalyzeAlgorithm::IsDag,
                true,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows(),
            [vec![AlgorithmValue::Boolean(false)]]
        );
    }
    assert_eq!(
        execute(
            &AdjacencyGraph::default(),
            AnalyzeAlgorithm::IsDag,
            false,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap()
        .rows(),
        [vec![AlgorithmValue::Boolean(false)]]
    );
}

#[test]
fn is_dag_uses_shared_limits_cancellation_and_rust_metadata() {
    let graph = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
    assert!(matches!(
        execute(
            &graph,
            AnalyzeAlgorithm::IsDag,
            true,
            AlgorithmLimits {
                nodes: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::NodeLimit { .. })
    ));
    assert!(matches!(
        execute(
            &graph,
            AnalyzeAlgorithm::IsDag,
            true,
            AlgorithmLimits {
                output_rows: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::OutputLimit { .. })
    ));
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        execute(
            &graph,
            AnalyzeAlgorithm::IsDag,
            true,
            AlgorithmLimits::default(),
            cancellation
        ),
        Err(AlgorithmError::Cancelled)
    );

    let mut registry = AlgorithmRegistry::default();
    register_analyze_algorithms(&mut registry, true).unwrap();
    assert_eq!(registry.capabilities()[0].dependency, BUILTIN_REVIEW);
    assert!(matches!(
        registry.execute(
            Algorithm::Analyze(AnalyzeAlgorithm::MinimumKSpanningTree),
            &graph,
            &AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default()),
        ),
        Err(AlgorithmError::Unavailable { .. })
    ));
}

#[test]
fn minimum_spanning_tree_shapes_uuid_forest_and_shared_controls() {
    let graph = AdjacencyGraph::with_test_edges(
        6,
        &[(0, 1), (1, 0), (0, 2), (1, 2), (1, 3), (4, 5), (4, 4)],
    )
    .with_test_edge_weights(&[4.0, 4.0, 3.0, 1.0, 2.0, -2.0, -10.0]);
    let output = execute(
        &graph,
        AnalyzeAlgorithm::MinimumSpanningTree,
        false,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    assert_eq!(
        output.schema,
        Algorithm::Analyze(AnalyzeAlgorithm::MinimumSpanningTree).result_schema()
    );
    assert_eq!(
        output.rows(),
        [
            vec![
                AlgorithmValue::Uuid(5_u128.to_be_bytes()),
                AlgorithmValue::Uuid(4_u128.to_be_bytes()),
                AlgorithmValue::Uuid(5_u128.to_be_bytes()),
                AlgorithmValue::Float64(-2.0),
            ],
            vec![
                AlgorithmValue::Uuid(3_u128.to_be_bytes()),
                AlgorithmValue::Uuid(1_u128.to_be_bytes()),
                AlgorithmValue::Uuid(2_u128.to_be_bytes()),
                AlgorithmValue::Float64(1.0),
            ],
            vec![
                AlgorithmValue::Uuid(4_u128.to_be_bytes()),
                AlgorithmValue::Uuid(1_u128.to_be_bytes()),
                AlgorithmValue::Uuid(3_u128.to_be_bytes()),
                AlgorithmValue::Float64(2.0),
            ],
            vec![
                AlgorithmValue::Uuid(2_u128.to_be_bytes()),
                AlgorithmValue::Uuid(0_u128.to_be_bytes()),
                AlgorithmValue::Uuid(2_u128.to_be_bytes()),
                AlgorithmValue::Float64(3.0),
            ],
        ]
    );

    assert!(matches!(
        execute(
            &graph,
            AnalyzeAlgorithm::MinimumSpanningTree,
            false,
            AlgorithmLimits {
                output_rows: 3,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::OutputLimit { .. })
    ));
    for limits in [
        AlgorithmLimits {
            nodes: 5,
            ..AlgorithmLimits::default()
        },
        AlgorithmLimits {
            edges: 6,
            ..AlgorithmLimits::default()
        },
    ] {
        assert!(matches!(
            execute(
                &graph,
                AnalyzeAlgorithm::MinimumSpanningTree,
                false,
                limits,
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::NodeLimit { .. } | AlgorithmError::EdgeLimit { .. })
        ));
    }
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        execute(
            &graph,
            AnalyzeAlgorithm::MinimumSpanningTree,
            false,
            AlgorithmLimits::default(),
            cancellation,
        ),
        Err(AlgorithmError::Cancelled)
    );
}

#[test]
fn minimum_k_spanning_tree_dispatches_canonical_ranked_rows() {
    let graph = AdjacencyGraph::with_test_undirected_multigraph(
        3,
        &[(10, 0, 1), (11, 0, 1), (12, 1, 2), (13, 0, 2), (14, 2, 2)],
    )
    .with_test_edge_weights(&[1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 2.0, 2.0, 0.0]);
    let output = execute_minimum_k_spanning_tree(
        &graph,
        3,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    assert_eq!(
        output.schema,
        Algorithm::Analyze(AnalyzeAlgorithm::MinimumKSpanningTree).result_schema()
    );
    assert_eq!(
        output.rows(),
        [
            vec![
                AlgorithmValue::UInt64(0),
                AlgorithmValue::Uuid(10_u128.to_be_bytes()),
                AlgorithmValue::Uuid(0_u128.to_be_bytes()),
                AlgorithmValue::Uuid(1_u128.to_be_bytes()),
                AlgorithmValue::Float64(1.0),
            ],
            vec![
                AlgorithmValue::UInt64(0),
                AlgorithmValue::Uuid(12_u128.to_be_bytes()),
                AlgorithmValue::Uuid(1_u128.to_be_bytes()),
                AlgorithmValue::Uuid(2_u128.to_be_bytes()),
                AlgorithmValue::Float64(1.0),
            ],
            vec![
                AlgorithmValue::UInt64(1),
                AlgorithmValue::Uuid(11_u128.to_be_bytes()),
                AlgorithmValue::Uuid(0_u128.to_be_bytes()),
                AlgorithmValue::Uuid(1_u128.to_be_bytes()),
                AlgorithmValue::Float64(1.0),
            ],
            vec![
                AlgorithmValue::UInt64(1),
                AlgorithmValue::Uuid(12_u128.to_be_bytes()),
                AlgorithmValue::Uuid(1_u128.to_be_bytes()),
                AlgorithmValue::Uuid(2_u128.to_be_bytes()),
                AlgorithmValue::Float64(1.0),
            ],
            vec![
                AlgorithmValue::UInt64(2),
                AlgorithmValue::Uuid(10_u128.to_be_bytes()),
                AlgorithmValue::Uuid(0_u128.to_be_bytes()),
                AlgorithmValue::Uuid(1_u128.to_be_bytes()),
                AlgorithmValue::Float64(1.0),
            ],
            vec![
                AlgorithmValue::UInt64(2),
                AlgorithmValue::Uuid(13_u128.to_be_bytes()),
                AlgorithmValue::Uuid(0_u128.to_be_bytes()),
                AlgorithmValue::Uuid(2_u128.to_be_bytes()),
                AlgorithmValue::Float64(2.0),
            ],
        ]
    );
    assert_eq!(
        output,
        execute_minimum_k_spanning_tree(
            &graph,
            3,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap()
    );

    let batch = shape_algorithm_output(
        Algorithm::Analyze(AnalyzeAlgorithm::MinimumKSpanningTree),
        &output,
    )
    .unwrap();
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| (field.name().as_str(), field.is_nullable()))
            .collect::<Vec<_>>(),
        [
            ("tree_id", false),
            ("edge_uuid", false),
            ("source_uuid", false),
            ("target_uuid", false),
            ("weight", false),
        ]
    );
}

#[test]
fn minimum_k_spanning_tree_dispatch_preserves_boundaries_and_controls() {
    for graph in [
        AdjacencyGraph::default(),
        AdjacencyGraph::with_test_edges(1, &[]),
    ] {
        assert!(
            execute_minimum_k_spanning_tree(
                &graph,
                1,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows()
            .is_empty()
        );
    }

    let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2), (0, 2)]);
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        execute_minimum_k_spanning_tree(&graph, 2, AlgorithmLimits::default(), cancellation,),
        Err(AlgorithmError::Cancelled)
    );
    assert!(matches!(
        execute_minimum_k_spanning_tree(
            &graph,
            2,
            AlgorithmLimits {
                output_rows: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::OutputLimit { .. })
    ));
}

#[test]
fn maximum_spanning_tree_shapes_descending_uuid_forest_and_shared_controls() {
    let graph = AdjacencyGraph::with_test_edges(
        7,
        &[(0, 1), (1, 0), (0, 2), (1, 2), (1, 3), (4, 5), (4, 4)],
    )
    .with_test_edge_weights(&[4.0, 4.0, 3.0, 1.0, 2.0, -2.0, f64::MAX]);
    let output = execute(
        &graph,
        AnalyzeAlgorithm::MaximumSpanningTree,
        false,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    assert_eq!(
        output.schema,
        Algorithm::Analyze(AnalyzeAlgorithm::MaximumSpanningTree).result_schema()
    );
    assert_eq!(
        output.rows(),
        [
            vec![
                AlgorithmValue::Uuid(0_u128.to_be_bytes()),
                AlgorithmValue::Uuid(0_u128.to_be_bytes()),
                AlgorithmValue::Uuid(1_u128.to_be_bytes()),
                AlgorithmValue::Float64(4.0),
            ],
            vec![
                AlgorithmValue::Uuid(2_u128.to_be_bytes()),
                AlgorithmValue::Uuid(0_u128.to_be_bytes()),
                AlgorithmValue::Uuid(2_u128.to_be_bytes()),
                AlgorithmValue::Float64(3.0),
            ],
            vec![
                AlgorithmValue::Uuid(4_u128.to_be_bytes()),
                AlgorithmValue::Uuid(1_u128.to_be_bytes()),
                AlgorithmValue::Uuid(3_u128.to_be_bytes()),
                AlgorithmValue::Float64(2.0),
            ],
            vec![
                AlgorithmValue::Uuid(5_u128.to_be_bytes()),
                AlgorithmValue::Uuid(4_u128.to_be_bytes()),
                AlgorithmValue::Uuid(5_u128.to_be_bytes()),
                AlgorithmValue::Float64(-2.0),
            ],
        ]
    );

    assert!(matches!(
        execute(
            &graph,
            AnalyzeAlgorithm::MaximumSpanningTree,
            false,
            AlgorithmLimits {
                output_rows: 3,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::OutputLimit { .. })
    ));
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        execute(
            &graph,
            AnalyzeAlgorithm::MaximumSpanningTree,
            false,
            AlgorithmLimits::default(),
            cancellation,
        ),
        Err(AlgorithmError::Cancelled)
    );
}

#[test]
fn find_cycles_dispatches_canonical_directed_and_undirected_uuid_lists() {
    let directed = AdjacencyGraph::with_test_directed_edges_and_uuids(
        &[[10; 16], [20; 16], [30; 16], [40; 16], [50; 16]],
        &[(0, 1), (1, 2), (2, 0), (1, 3), (3, 1), (3, 3), (4, 0)],
    );
    let output = execute(
        &directed,
        AnalyzeAlgorithm::FindCycles,
        true,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    assert_eq!(
        output.schema,
        Algorithm::Analyze(AnalyzeAlgorithm::FindCycles).result_schema()
    );
    assert_eq!(
        output.rows(),
        [
            vec![AlgorithmValue::UuidList(vec![[10; 16], [20; 16], [30; 16]])],
            vec![AlgorithmValue::UuidList(vec![[20; 16], [40; 16]])],
            vec![AlgorithmValue::UuidList(vec![[40; 16]])],
        ]
    );

    let undirected = AdjacencyGraph::with_test_undirected_multigraph(
        5,
        &[
            (10, 0, 1),
            (11, 0, 1),
            (12, 1, 0),
            (13, 1, 2),
            (14, 2, 0),
            (15, 2, 3),
            (16, 3, 0),
            (17, 4, 4),
        ],
    );
    assert_eq!(
        execute(
            &undirected,
            AnalyzeAlgorithm::FindCycles,
            false,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap()
        .rows(),
        [
            vec![AlgorithmValue::UuidList(
                [0_u128, 1, 2].map(u128::to_be_bytes).to_vec()
            )],
            vec![AlgorithmValue::UuidList(
                [0_u128, 1, 2, 3].map(u128::to_be_bytes).to_vec()
            )],
            vec![AlgorithmValue::UuidList(
                [0_u128, 2, 3].map(u128::to_be_bytes).to_vec()
            )],
            vec![AlgorithmValue::UuidList(vec![4_u128.to_be_bytes()])],
        ]
    );
}

#[test]
fn find_cycles_dispatch_preserves_empty_and_shared_controls() {
    assert_eq!(
        execute(
            &AdjacencyGraph::default(),
            AnalyzeAlgorithm::FindCycles,
            true,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap()
        .rows(),
        Vec::<Vec<AlgorithmValue>>::new()
    );
    let graph = AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2), (2, 0)]);
    assert!(matches!(
        execute(
            &graph,
            AnalyzeAlgorithm::FindCycles,
            true,
            AlgorithmLimits {
                output_rows: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::OutputLimit { .. })
    ));
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        execute(
            &graph,
            AnalyzeAlgorithm::FindCycles,
            true,
            AlgorithmLimits::default(),
            cancellation,
        ),
        Err(AlgorithmError::Cancelled)
    );
}
