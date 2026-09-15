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
    assert!(matches!(
        execute_clustering_coefficient(
            &graph,
            AlgorithmLimits {
                iterations: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default()
        ),
        Err(AlgorithmError::IterationLimit { .. })
    ));
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
