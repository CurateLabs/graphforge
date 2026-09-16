use super::super::tests::assert_scores_close;
use super::super::*;
use super::*;

fn execute_eigenvector(
    graph: &AdjacencyGraph,
    limits: AlgorithmLimits,
    cancellation: AlgorithmCancellation,
) -> Result<AlgorithmOutput, AlgorithmError> {
    let mut registry = AlgorithmRegistry::default();
    register_rank_algorithms(&mut registry)?;
    registry.execute(
        Algorithm::Rank(RankAlgorithm::Eigenvector),
        graph,
        &AlgorithmControl::new(limits, cancellation),
    )
}

fn execute_eigenvector_with_pool(
    graph: &AdjacencyGraph,
    threads: usize,
    cancellation: AlgorithmCancellation,
) -> Result<AlgorithmOutput, AlgorithmError> {
    let pool = Arc::new(crate::ComputePool::new(threads).unwrap());
    let mut registry = AlgorithmRegistry::default();
    register_rank_algorithms(&mut registry)?;
    let control = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(threads),
        cancellation,
    )
    .with_compute_pool(pool);
    registry.execute(Algorithm::Rank(RankAlgorithm::Eigenvector), graph, &control)
}

fn eigenvector_scores(output: &AlgorithmOutput) -> Vec<f64> {
    output
        .rows()
        .iter()
        .map(|row| match row[1] {
            AlgorithmValue::Float64(score) => score,
            _ => panic!("eigenvector score must be Float64"),
        })
        .collect()
}

fn eigenvector_bits(output: &AlgorithmOutput) -> Vec<u64> {
    eigenvector_scores(output)
        .into_iter()
        .map(f64::to_bits)
        .collect()
}

fn eigenvector_scores_for(graph: &AdjacencyGraph) -> Vec<f64> {
    eigenvector_scores(
        &execute_eigenvector(
            graph,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap(),
    )
}

fn dense_eigenvector_graph(nodes: usize) -> AdjacencyGraph {
    let fanout = ((EIGENVECTOR_PARALLEL_CROSSOVER_EDGES as usize) / nodes.max(1)).saturating_add(2);
    let edges = (0..nodes)
        .flat_map(|node| (1..=fanout).map(move |hop| (node as u64, ((node + hop) % nodes) as u64)))
        .collect::<Vec<_>>();
    AdjacencyGraph::with_test_edges(nodes as u64, &edges)
}

fn assert_scores_within(actual: &[f64], expected: &[f64], tolerance: f64) {
    assert_eq!(actual.len(), expected.len());
    assert!(
        actual
            .iter()
            .zip(expected)
            .all(|(actual, expected)| (actual - expected).abs() <= tolerance)
    );
}

#[test]
fn eigenvector_scores_shifted_power_fixtures_deterministically() {
    let cycle = AdjacencyGraph::with_test_edges(2, &[(0, 1), (1, 0)]);
    let first = execute_eigenvector(
        &cycle,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    assert_scores_close(
        &eigenvector_scores(&first),
        &[1.0 / 2.0_f64.sqrt(), 1.0 / 2.0_f64.sqrt()],
    );
    assert_eq!(
        first,
        execute_eigenvector(
            &cycle,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap()
    );
    assert_eq!(
        first.schema,
        Algorithm::Rank(RankAlgorithm::Eigenvector).result_schema()
    );

    let star = AdjacencyGraph::with_test_edges(3, &[(0, 1), (0, 2), (1, 0), (2, 0)]);
    assert_scores_within(
        &eigenvector_scores_for(&star),
        &[1.0 / 2.0_f64.sqrt(), 0.5, 0.5],
        EIGENVECTOR_TOLERANCE,
    );
}

#[test]
fn eigenvector_handles_direction_multigraph_disconnected_and_empty_graphs() {
    let directed = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
    let denominator = (1.0_f64 + 21.0_f64.powi(2)).sqrt();
    assert_scores_close(
        &eigenvector_scores_for(&directed),
        &[1.0 / denominator, 21.0 / denominator],
    );
    let parallel = AdjacencyGraph::with_test_edges(2, &[(0, 1), (0, 1)]);
    assert!(eigenvector_scores_for(&parallel)[1] > eigenvector_scores_for(&directed)[1]);
    let with_self_loop = AdjacencyGraph::with_test_edges(2, &[(0, 1), (0, 0)]);
    assert_ne!(
        eigenvector_scores_for(&with_self_loop),
        eigenvector_scores_for(&directed)
    );

    let disconnected = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0)]);
    assert_scores_close(
        &eigenvector_scores_for(&disconnected),
        &[
            1.0 / (2.0 + 2.0_f64.powi(-40)).sqrt(),
            1.0 / (2.0 + 2.0_f64.powi(-40)).sqrt(),
            2.0_f64.powi(-20) / (2.0 + 2.0_f64.powi(-40)).sqrt(),
        ],
    );
    assert_scores_close(
        &eigenvector_scores_for(&AdjacencyGraph::with_test_counts(4, 0)),
        &[0.5; 4],
    );
    assert_eq!(
        eigenvector_scores_for(&AdjacencyGraph::with_test_counts(1, 0)),
        [1.0]
    );
    assert!(
        execute_eigenvector(
            &AdjacencyGraph::default(),
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap()
        .rows()
        .is_empty()
    );
}

#[test]
fn eigenvector_uses_shared_limits_cancellation_and_dependency_metadata() {
    let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
    assert_eq!(
        execute_eigenvector(
            &graph,
            AlgorithmLimits {
                iterations: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::IterationLimit {
            observed: 1,
            limit: 0,
        })
    );
    assert!(matches!(
        execute_eigenvector(
            &graph,
            AlgorithmLimits {
                nodes: 2,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::NodeLimit { .. })
    ));
    assert!(matches!(
        execute_eigenvector(
            &graph,
            AlgorithmLimits {
                output_rows: 2,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::OutputLimit { .. })
    ));
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        execute_eigenvector(&graph, AlgorithmLimits::default(), cancellation),
        Err(AlgorithmError::Cancelled)
    );
    let edge_heavy = AdjacencyGraph::with_test_edges(1, &vec![(0, 0); 1025]);
    assert_eq!(
        execute_eigenvector(
            &edge_heavy,
            AlgorithmLimits {
                iterations: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::IterationLimit {
            observed: 2,
            limit: 1,
        })
    );
    let mut registry = AlgorithmRegistry::default();
    register_rank_algorithms(&mut registry).unwrap();
    let capability = registry
        .capabilities()
        .into_iter()
        .find(|capability| capability.algorithm == Algorithm::Rank(RankAlgorithm::Eigenvector))
        .unwrap();
    assert_eq!(capability.dependency, BUILTIN_REVIEW);
}

#[test]
fn eigenvector_path_selection_respects_crossover_and_one_thread() {
    let serial_control = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(4),
        AlgorithmCancellation::default(),
    );
    assert_eq!(
        select_eigenvector_path(
            &serial_control,
            EIGENVECTOR_PARALLEL_CROSSOVER_EDGES - 1,
            64
        ),
        EigenvectorExecutionPath::Serial
    );
    let one = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(1),
        AlgorithmCancellation::default(),
    )
    .with_compute_pool(Arc::new(crate::ComputePool::new(1).unwrap()));
    assert_eq!(
        select_eigenvector_path(&one, EIGENVECTOR_PARALLEL_CROSSOVER_EDGES, 64),
        EigenvectorExecutionPath::Serial
    );
    let parallel = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(4),
        AlgorithmCancellation::default(),
    )
    .with_compute_pool(Arc::new(crate::ComputePool::new(4).unwrap()));
    assert_eq!(
        select_eigenvector_path(&parallel, EIGENVECTOR_PARALLEL_CROSSOVER_EDGES, 64),
        EigenvectorExecutionPath::Parallel {
            threads: 4,
            chunks: 4
        }
    );
}

#[test]
fn eigenvector_thread_matrix_matches_one_thread_bits_and_ordering() {
    let graph = dense_eigenvector_graph(128);
    assert!(graph.edge_entry_count() >= EIGENVECTOR_PARALLEL_CROSSOVER_EDGES);
    let serial =
        execute_eigenvector_with_pool(&graph, 1, AlgorithmCancellation::default()).unwrap();
    let serial_bits = eigenvector_bits(&serial);
    let serial_rows = serial.rows();
    for threads in [2_usize, 4, 8] {
        let parallel =
            execute_eigenvector_with_pool(&graph, threads, AlgorithmCancellation::default())
                .unwrap();
        assert_eq!(parallel.schema, serial.schema);
        assert_eq!(parallel.rows(), serial_rows);
        assert_eq!(eigenvector_bits(&parallel), serial_bits);
    }
}

#[test]
fn eigenvector_parallel_preserves_multigraph_self_loop_and_disconnected_bits() {
    let fixtures = [
        AdjacencyGraph::with_test_edges(3, &[(0, 1), (0, 1), (0, 2), (1, 1)]),
        AdjacencyGraph::with_test_edges(2, &[(0, 1)]),
        AdjacencyGraph::with_test_edges(2, &[(0, 1), (1, 0)]),
        AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0), (2, 3), (3, 2)]),
        AdjacencyGraph::default(),
        AdjacencyGraph::with_test_edges(1, &[]),
    ];
    for graph in &fixtures {
        let serial =
            execute_eigenvector_with_pool(graph, 1, AlgorithmCancellation::default()).unwrap();
        for threads in [2_usize, 4, 8] {
            let parallel =
                execute_eigenvector_with_pool(graph, threads, AlgorithmCancellation::default())
                    .unwrap();
            assert_eq!(eigenvector_bits(&parallel), eigenvector_bits(&serial));
            assert_eq!(parallel.rows(), serial.rows());
        }
    }

    let nodes = 512_u64;
    let mut edges = Vec::new();
    for source in 0..nodes {
        let degree = 16 + (source % 11) as usize;
        for hop in 0..degree {
            edges.push((source, (source + hop as u64) % nodes));
        }
    }
    let adversarial = AdjacencyGraph::with_test_edges(nodes, &edges);
    assert!(adversarial.edge_entry_count() >= EIGENVECTOR_PARALLEL_CROSSOVER_EDGES);
    let serial =
        execute_eigenvector_with_pool(&adversarial, 1, AlgorithmCancellation::default()).unwrap();
    for threads in [2_usize, 4, 8] {
        let parallel =
            execute_eigenvector_with_pool(&adversarial, threads, AlgorithmCancellation::default())
                .unwrap();
        assert_eq!(eigenvector_bits(&parallel), eigenvector_bits(&serial));
    }
}

#[test]
fn eigenvector_parallel_cancellation_returns_structured_cancelled() {
    let graph = dense_eigenvector_graph(128);
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        execute_eigenvector_with_pool(&graph, 4, cancellation),
        Err(AlgorithmError::Cancelled)
    );
}

#[test]
fn eigenvector_pull_matches_serial_scatter_contribution_order() {
    let fixtures = [
        AdjacencyGraph::with_test_edges(3, &[(0, 1), (0, 1), (0, 2), (1, 1)]),
        AdjacencyGraph::with_test_edges(2, &[(0, 1)]),
        AdjacencyGraph::with_test_edges(2, &[(0, 1), (1, 0)]),
        AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0), (2, 3), (3, 2)]),
        AdjacencyGraph::with_test_edges(1, &[]),
        dense_eigenvector_graph(64),
    ];
    for graph in &fixtures {
        if graph.node_ids().is_empty() {
            continue;
        }
        let indices = graph
            .node_ids()
            .iter()
            .enumerate()
            .map(|(index, &node)| (node, index))
            .collect::<HashMap<_, _>>();
        let inbound = prepare_eigenvector_inbound(graph, &indices).unwrap();
        let scores = (0..graph.node_ids().len())
            .map(|index| (index + 1) as f64 / (graph.node_ids().len() + 1) as f64)
            .collect::<Vec<_>>();
        let control =
            AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
        let scatter =
            eigenvector_scatter_serial(graph, &indices, graph.node_ids(), &scores, &control)
                .unwrap();
        let pull = (0..scores.len())
            .map(|dest| eigenvector_pull_destination(&inbound, &scores, dest))
            .collect::<Vec<_>>();
        assert_eq!(
            scatter
                .iter()
                .map(|value| value.to_bits())
                .collect::<Vec<_>>(),
            pull.iter().map(|value| value.to_bits()).collect::<Vec<_>>(),
            "pull must apply contributions in serial source/edge order"
        );
    }
}

#[test]
fn eigenvector_worker_panic_returns_structured_error() {
    let pool = crate::ComputePool::new(2).unwrap();
    assert_eq!(
        run_eigenvector_on_pool(&pool, || -> Result<(), AlgorithmError> {
            panic!("worker panic is converted")
        }),
        Err(AlgorithmError::Execution {
            message: "Eigenvector worker panicked".into()
        })
    );
}

#[test]
#[ignore = "manual crossover measurement; run in release with --ignored --nocapture"]
fn measure_eigenvector_parallel_crossover() {
    use std::time::Instant;

    let mut cases = Vec::new();
    for (nodes, fanout) in [(128_usize, 32_usize), (512, 64)] {
        let edges = (0..nodes)
            .flat_map(|node| {
                (1..=fanout).map(move |hop| (node as u64, ((node + hop) % nodes) as u64))
            })
            .collect::<Vec<_>>();
        cases.push((format!("regular nodes={nodes} fanout={fanout}"), edges));
    }
    for (nodes, base, spread) in [
        (512_usize, 9_usize, 17_usize),
        (512, 32, 33),
        (2_048, 16, 33),
        (2_048, 32, 65),
    ] {
        let edges = (0..nodes)
            .flat_map(|node| {
                let degree = base + (node % spread);
                (0..degree).map(move |hop| {
                    let step = 1 + ((hop * 17 + node) % nodes);
                    (node as u64, ((node + step) % nodes) as u64)
                })
            })
            .collect::<Vec<_>>();
        cases.push((
            format!("irregular nodes={nodes} base={base} spread={spread}"),
            edges,
        ));
    }

    for (label, edges) in cases {
        let nodes = edges
            .iter()
            .flat_map(|(source, target)| [*source, *target])
            .max()
            .unwrap_or(0)
            + 1;
        let graph = AdjacencyGraph::with_test_edges(nodes, &edges);
        let edges = graph.edge_entry_count();
        let _ = execute_eigenvector_with_pool(&graph, 1, AlgorithmCancellation::default()).unwrap();
        let _ = execute_eigenvector_with_pool(&graph, 4, AlgorithmCancellation::default()).unwrap();

        let mut serial_ns = u128::MAX;
        let mut parallel_ns = u128::MAX;
        for _ in 0..5 {
            let t0 = Instant::now();
            let serial =
                execute_eigenvector_with_pool(&graph, 1, AlgorithmCancellation::default()).unwrap();
            serial_ns = serial_ns.min(t0.elapsed().as_nanos());

            let t1 = Instant::now();
            let parallel =
                execute_eigenvector_with_pool(&graph, 4, AlgorithmCancellation::default()).unwrap();
            parallel_ns = parallel_ns.min(t1.elapsed().as_nanos());
            assert_eq!(eigenvector_bits(&parallel), eigenvector_bits(&serial));
        }
        println!(
            "{label} edges={edges} serial_ns={serial_ns} parallel_ns={parallel_ns} ratio={}",
            parallel_ns as f64 / serial_ns as f64
        );
    }
}
