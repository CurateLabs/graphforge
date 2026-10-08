use super::super::tests::assert_scores_close;
use super::super::tests::hits_hub_scores;
use super::super::*;
use super::*;

fn execute_clustering_coefficient(
    graph: &AdjacencyGraph,
    limits: AlgorithmLimits,
    cancellation: AlgorithmCancellation,
) -> Result<AlgorithmOutput, AlgorithmError> {
    let mut registry = AlgorithmRegistry::default();
    register_rank_algorithms(&mut registry)?;
    registry.execute(
        Algorithm::Rank(RankAlgorithm::ClusteringCoefficient),
        graph,
        &AlgorithmControl::new(limits, cancellation),
    )
}

fn execute_clustering_coefficient_with_pool(
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
    registry.execute(
        Algorithm::Rank(RankAlgorithm::ClusteringCoefficient),
        graph,
        &control,
    )
}

fn clustering_coefficient_output_scores(output: &AlgorithmOutput) -> Vec<f64> {
    hits_hub_scores(output)
}

fn clustering_coefficient_bits(output: &AlgorithmOutput) -> Vec<u64> {
    clustering_coefficient_output_scores(output)
        .into_iter()
        .map(f64::to_bits)
        .collect()
}

fn dense_clustering_graph(nodes: usize) -> AdjacencyGraph {
    let fanout = 32_usize.min(nodes.saturating_sub(1));
    let edges = (0..nodes)
        .flat_map(|node| (1..=fanout).map(move |hop| (node as u64, ((node + hop) % nodes) as u64)))
        .collect::<Vec<_>>();
    AdjacencyGraph::with_test_edges(nodes as u64, &edges)
}

#[test]
fn clustering_coefficient_matches_directed_and_undirected_triangles() {
    let directed_cycle = execute_clustering_coefficient(
        &AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2), (2, 0)]),
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    assert_scores_close(
        &clustering_coefficient_output_scores(&directed_cycle),
        &[0.5, 0.5, 0.5],
    );

    let undirected_triangle = execute_clustering_coefficient(
        &AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0), (1, 2), (2, 1), (2, 0), (0, 2)]),
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    assert_scores_close(
        &clustering_coefficient_output_scores(&undirected_triangle),
        &[1.0, 1.0, 1.0],
    );
}

#[test]
fn clustering_coefficient_simplifies_multigraphs_and_retains_all_nodes() {
    let graph = AdjacencyGraph::with_test_edges(
        5,
        &[
            (0, 1),
            (0, 1),
            (1, 0),
            (1, 2),
            (2, 1),
            (2, 0),
            (0, 2),
            (0, 0),
            (3, 4),
        ],
    );
    let first = execute_clustering_coefficient(
        &graph,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    assert_scores_close(
        &clustering_coefficient_output_scores(&first),
        &[1.0, 1.0, 1.0, 0.0, 0.0],
    );
    assert_eq!(
        first,
        execute_clustering_coefficient(
            &graph,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap()
    );
    assert!(
        execute_clustering_coefficient(
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
fn clustering_coefficient_path_selection_respects_crossover_and_one_thread() {
    let serial_control = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(4),
        AlgorithmCancellation::default(),
    );
    assert_eq!(
        select_clustering_coefficient_path(
            &serial_control,
            CLUSTERING_COEFFICIENT_PARALLEL_CROSSOVER_WORK,
            64
        ),
        ClusteringCoefficientExecutionPath::Serial
    );

    let one = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(1),
        AlgorithmCancellation::default(),
    )
    .with_compute_pool(Arc::new(crate::ComputePool::new(1).unwrap()));
    assert_eq!(
        select_clustering_coefficient_path(
            &one,
            CLUSTERING_COEFFICIENT_PARALLEL_CROSSOVER_WORK,
            64
        ),
        ClusteringCoefficientExecutionPath::Serial
    );

    let parallel = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(4),
        AlgorithmCancellation::default(),
    )
    .with_compute_pool(Arc::new(crate::ComputePool::new(4).unwrap()));
    assert_eq!(
        select_clustering_coefficient_path(
            &parallel,
            CLUSTERING_COEFFICIENT_PARALLEL_CROSSOVER_WORK - 1,
            64
        ),
        ClusteringCoefficientExecutionPath::Serial
    );
    assert_eq!(
        select_clustering_coefficient_path(
            &parallel,
            CLUSTERING_COEFFICIENT_PARALLEL_CROSSOVER_WORK,
            64
        ),
        ClusteringCoefficientExecutionPath::Parallel {
            threads: 4,
            chunks: 4
        }
    );
}

#[test]
fn clustering_coefficient_thread_matrix_matches_one_thread_bits_and_ordering() {
    let graph = dense_clustering_graph(128);
    let prepared = prepare_clustering_coefficient(
        &graph,
        &AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default()),
    )
    .unwrap();
    assert!(
        prepared.work_units >= CLUSTERING_COEFFICIENT_PARALLEL_CROSSOVER_WORK,
        "fixture should exercise the parallel path"
    );

    let serial =
        execute_clustering_coefficient_with_pool(&graph, 1, AlgorithmCancellation::default())
            .unwrap();
    let serial_bits = clustering_coefficient_bits(&serial);
    let serial_rows = serial.rows();
    for threads in [2_usize, 4, 8] {
        let parallel = execute_clustering_coefficient_with_pool(
            &graph,
            threads,
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_eq!(parallel.schema, serial.schema);
        assert_eq!(parallel.rows(), serial_rows);
        assert_eq!(clustering_coefficient_bits(&parallel), serial_bits);
    }
}

#[test]
fn clustering_coefficient_parallel_cancels_and_worker_panics_are_structured() {
    let graph = dense_clustering_graph(128);
    let prepared = prepare_clustering_coefficient(
        &graph,
        &AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default()),
    )
    .unwrap();
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    let cancelled_control = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(4),
        cancellation,
    )
    .with_compute_pool(Arc::new(crate::ComputePool::new(4).unwrap()));
    assert_eq!(
        clustering_coefficient_scores_parallel(&prepared, &cancelled_control),
        Err(AlgorithmError::Cancelled)
    );

    let pool = crate::ComputePool::new(2).unwrap();
    assert_eq!(
        run_clustering_coefficient_on_pool(&pool, || -> () { panic!("boom") }),
        Err(AlgorithmError::Execution {
            message: "clustering coefficient worker panicked".into()
        })
    );
}

#[test]
fn clustering_coefficient_uses_shared_controls_and_canonical_metadata() {
    let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2), (2, 0)]);
    assert!(
        execute_clustering_coefficient(
            &graph,
            AlgorithmLimits {
                iterations: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default()
        )
        .is_ok(),
        "a single-pass algorithm never consumes the iteration budget"
    );
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        execute_clustering_coefficient(&graph, AlgorithmLimits::default(), cancellation),
        Err(AlgorithmError::Cancelled)
    );
    assert!(matches!(
        execute_clustering_coefficient(
            &graph,
            AlgorithmLimits {
                output_rows: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default()
        ),
        Err(AlgorithmError::OutputLimit { .. })
    ));
    let mut registry = AlgorithmRegistry::default();
    register_rank_algorithms(&mut registry).unwrap();
    let capability = registry
        .capabilities()
        .into_iter()
        .find(|capability| {
            capability.algorithm == Algorithm::Rank(RankAlgorithm::ClusteringCoefficient)
        })
        .unwrap();
    assert_eq!(capability.dependency, BUILTIN_REVIEW);
    assert_eq!(capability.algorithm.as_str(), "clustering_coefficient");
}

fn neighbor_edges_output(
    graph: &AdjacencyGraph,
    threads: usize,
    cancellation: AlgorithmCancellation,
) -> Result<AlgorithmOutput, AlgorithmError> {
    let options = RankOptions {
        by: RankAlgorithm::ClusteringCoefficient,
        clustering_normalization: Some(ClusteringNormalization::NeighborEdges),
        ..RankOptions::default()
    };
    let control = AlgorithmControl::new(AlgorithmLimits::default(), cancellation)
        .with_rank_options(&options)
        .with_compute_pool(Arc::new(crate::ComputePool::new(threads).unwrap()));
    ClusteringCoefficient.execute(graph, &control)
}

#[test]
fn neighbor_edges_lcc_distinguishes_reciprocal_normalization_and_simplifies_edges() {
    // Vertex 0 sees {1,2,3}; only 1->2 connects those neighbors.
    // Its reciprocal 0<->1 arc makes Fagiolo 1/5, versus neighbor-edges 1/6.
    let graph = AdjacencyGraph::with_test_directed_edges(
        5,
        &[(0, 1), (1, 0), (0, 2), (0, 3), (1, 2), (1, 2), (0, 0)],
    );
    let output = neighbor_edges_output(&graph, 1, AlgorithmCancellation::default()).unwrap();
    assert_scores_close(
        &clustering_coefficient_output_scores(&output),
        &[1.0 / 6.0, 0.5, 1.0, 0.0, 0.0],
    );
    let default = execute_clustering_coefficient(
        &graph,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    assert_ne!(
        clustering_coefficient_output_scores(&default),
        clustering_coefficient_output_scores(&output)
    );
    let triangle =
        AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0), (1, 2), (2, 1), (2, 0), (0, 2)]);
    assert_scores_close(
        &clustering_coefficient_output_scores(
            &neighbor_edges_output(&triangle, 1, AlgorithmCancellation::default()).unwrap(),
        ),
        &[1.0, 1.0, 1.0],
    );
}

#[test]
fn neighbor_edges_lcc_uses_private_pool_with_identical_bits_and_cancellation() {
    let graph = dense_clustering_graph(128);
    let prepared = prepare_clustering_coefficient(
        &graph,
        &AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default()),
    )
    .unwrap();
    assert!(prepared.work_units >= CLUSTERING_COEFFICIENT_PARALLEL_CROSSOVER_WORK);
    let serial = neighbor_edges_output(&graph, 1, AlgorithmCancellation::default()).unwrap();
    for threads in [2, 4, 8] {
        let parallel =
            neighbor_edges_output(&graph, threads, AlgorithmCancellation::default()).unwrap();
        assert_eq!(
            clustering_coefficient_bits(&parallel),
            clustering_coefficient_bits(&serial)
        );
        assert_eq!(parallel.rows(), serial.rows());
    }
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        neighbor_edges_output(&graph, 4, cancellation),
        Err(AlgorithmError::Cancelled)
    );
}

/// More nodes than the default iteration budget of 10,000 (#1922).
const LARGE_NODES: usize = 20_000;

/// A ring where each node links to the next two, so every node closes a triangle.
fn large_ring_graph() -> AdjacencyGraph {
    let edges = (0..LARGE_NODES)
        .flat_map(|node| [1, 2].map(move |hop| (node as u64, ((node + hop) % LARGE_NODES) as u64)))
        .collect::<Vec<_>>();
    AdjacencyGraph::with_test_edges(LARGE_NODES as u64, &edges)
}

/// One hub linked both ways to `leaves` leaves, which also form a two-way
/// chain: the hub's pair loop alone is `leaves^2` units of work, far past any
/// per-1,024 charge.
fn hub_graph(leaves: usize) -> AdjacencyGraph {
    let leaves = leaves as u64;
    let edges = (1..=leaves)
        .flat_map(|leaf| [(0, leaf), (leaf, 0)])
        .chain((1..leaves).flat_map(|leaf| [(leaf, leaf + 1), (leaf + 1, leaf)]))
        .collect::<Vec<_>>();
    AdjacencyGraph::with_test_edges(leaves + 1, &edges)
}

fn lcc_control(
    limits: AlgorithmLimits,
    normalization: ClusteringNormalization,
    cancellation: AlgorithmCancellation,
) -> AlgorithmControl {
    AlgorithmControl::new(limits, cancellation).with_rank_options(&RankOptions {
        by: RankAlgorithm::ClusteringCoefficient,
        clustering_normalization: Some(normalization),
        ..RankOptions::default()
    })
}

const NORMALIZATIONS: [ClusteringNormalization; 2] = [
    ClusteringNormalization::Fagiolo,
    ClusteringNormalization::NeighborEdges,
];

#[test]
fn clustering_coefficient_is_single_pass_and_ignores_the_iteration_budget() {
    let graph = large_ring_graph();
    for normalization in NORMALIZATIONS {
        let mut by_budget = Vec::new();
        for iterations in [AlgorithmLimits::default().iterations, 0] {
            let control = lcc_control(
                AlgorithmLimits {
                    iterations,
                    ..AlgorithmLimits::default()
                },
                normalization,
                AlgorithmCancellation::default(),
            );
            let output = ClusteringCoefficient
                .execute(&graph, &control)
                .unwrap_or_else(|error| {
                    panic!("{normalization:?} iterations={iterations}: {error:?}")
                });
            assert_eq!(output.num_rows(), LARGE_NODES);
            // Every node closes triangles, so the scores are not vacuous.
            assert!(
                clustering_coefficient_output_scores(&output)
                    .iter()
                    .all(|score| *score > 0.0)
            );
            by_budget.push(clustering_coefficient_bits(&output));
        }
        // The budget never reaches the result.
        assert_eq!(by_budget[0], by_budget[1]);
    }
}

#[test]
fn clustering_coefficient_hub_pair_work_never_consumes_the_iteration_budget() {
    // 3,000 leaves is 9,000,000 hub pairs: 8,789 per-1,024 charges for the hub alone.
    let graph = hub_graph(3_000);
    for normalization in NORMALIZATIONS {
        let control = lcc_control(
            AlgorithmLimits {
                iterations: 0,
                ..AlgorithmLimits::default()
            },
            normalization,
            AlgorithmCancellation::default(),
        );
        let output = ClusteringCoefficient.execute(&graph, &control).unwrap();
        let scores = clustering_coefficient_output_scores(&output);
        assert_eq!(scores.len(), 3_001);
        // The hub closes one triangle per chain link; a chain leaf closes one
        // per chain neighbour; the chain ends see just the hub and one leaf.
        assert_eq!(scores[0], 2.0 / 3_000.0);
        assert_eq!(scores[1], 1.0);
        assert_eq!(scores[3_000], 1.0);
        assert!(scores[2..3_000].iter().all(|score| *score == 2.0 / 3.0));
    }
}

/// Run `execute` while another thread cancels it after `delay`; the work is
/// sized to run for seconds, so only an in-loop poll can end it sooner.
fn cancelled_midway(
    graph: &AdjacencyGraph,
    normalization: ClusteringNormalization,
    threads: usize,
) -> (Result<AlgorithmOutput, AlgorithmError>, std::time::Duration) {
    let cancellation = AlgorithmCancellation::default();
    let mut control = lcc_control(
        AlgorithmLimits::default().with_compute_threads(threads),
        normalization,
        cancellation.clone(),
    );
    if threads > 1 {
        control = control.with_compute_pool(Arc::new(crate::ComputePool::new(threads).unwrap()));
    }
    let canceller = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(100));
        cancellation.cancel();
    });
    let started = std::time::Instant::now();
    let result = ClusteringCoefficient.execute(graph, &control);
    let elapsed = started.elapsed();
    canceller.join().unwrap();
    (result, elapsed)
}

#[test]
fn clustering_coefficient_cancels_inside_a_large_hub_pair_loop() {
    // 12,000 leaves is 144,000,000 hub pairs, and 12,001 nodes exceed the
    // iteration budget: cancellation must still end the run promptly.
    let graph = hub_graph(12_000);
    for normalization in NORMALIZATIONS {
        for threads in [1, 4] {
            let (result, elapsed) = cancelled_midway(&graph, normalization, threads);
            assert_eq!(
                result,
                Err(AlgorithmError::Cancelled),
                "{normalization:?} threads={threads} ran {elapsed:?}"
            );
            assert!(
                elapsed < std::time::Duration::from_secs(5),
                "{normalization:?} threads={threads} took {elapsed:?} to observe cancellation"
            );
        }
    }
}

#[test]
fn clustering_coefficient_cancels_while_preparing_a_large_graph() {
    let graph = large_ring_graph();
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    let control = lcc_control(
        AlgorithmLimits::default(),
        ClusteringNormalization::Fagiolo,
        cancellation,
    );
    assert_eq!(
        prepare_clustering_coefficient(&graph, &control).err(),
        Some(AlgorithmError::Cancelled)
    );
}

#[test]
fn clustering_coefficient_polls_cancellation_for_every_node() {
    // Isolated nodes have no pairs, so the per-node poll is the only one that can fire.
    let graph = AdjacencyGraph::with_test_edges(LARGE_NODES as u64, &[]);
    let prepared = prepare_clustering_coefficient(
        &graph,
        &lcc_control(
            AlgorithmLimits::default(),
            ClusteringNormalization::Fagiolo,
            AlgorithmCancellation::default(),
        ),
    )
    .unwrap();
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    let control = lcc_control(
        AlgorithmLimits::default(),
        ClusteringNormalization::Fagiolo,
        cancellation,
    );
    assert!(
        matches!(
            clustering_coefficient_scores_serial(&prepared, &control),
            Err(AlgorithmError::Cancelled)
        ),
        "a cancelled run must stop at the first node"
    );
}

#[test]
fn clustering_coefficient_keeps_node_edge_and_output_limits_on_large_graphs() {
    let graph = large_ring_graph();
    let mut registry = AlgorithmRegistry::default();
    register_rank_algorithms(&mut registry).unwrap();
    let dispatched = |limits: AlgorithmLimits| {
        registry
            .execute(
                Algorithm::Rank(RankAlgorithm::ClusteringCoefficient),
                &graph,
                &AlgorithmControl::new(limits, AlgorithmCancellation::default()),
            )
            .map(|_| ())
    };
    assert!(matches!(
        dispatched(AlgorithmLimits {
            nodes: 19_999,
            ..AlgorithmLimits::default()
        }),
        Err(AlgorithmError::NodeLimit { .. })
    ));
    assert!(matches!(
        dispatched(AlgorithmLimits {
            edges: 39_999,
            ..AlgorithmLimits::default()
        }),
        Err(AlgorithmError::EdgeLimit { .. })
    ));
    assert!(matches!(
        dispatched(AlgorithmLimits {
            output_rows: 19_999,
            ..AlgorithmLimits::default()
        }),
        Err(AlgorithmError::OutputLimit { .. })
    ));
    assert_eq!(dispatched(AlgorithmLimits::default()), Ok(()));
}
