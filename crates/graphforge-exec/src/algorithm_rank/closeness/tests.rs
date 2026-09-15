use super::super::tests::assert_scores_close;
use super::super::*;
use super::*;

fn execute_closeness(
    graph: &AdjacencyGraph,
    limits: AlgorithmLimits,
    cancellation: AlgorithmCancellation,
) -> Result<AlgorithmOutput, AlgorithmError> {
    let mut registry = AlgorithmRegistry::default();
    register_rank_algorithms(&mut registry)?;
    registry.execute(
        Algorithm::Rank(RankAlgorithm::Closeness),
        graph,
        &AlgorithmControl::new(limits, cancellation),
    )
}

fn execute_closeness_with_pool(
    graph: &AdjacencyGraph,
    threads: usize,
    cancellation: AlgorithmCancellation,
) -> Result<AlgorithmOutput, AlgorithmError> {
    execute_closeness_with_pool_and_limits(graph, threads, AlgorithmLimits::default(), cancellation)
}

fn execute_closeness_with_pool_and_limits(
    graph: &AdjacencyGraph,
    threads: usize,
    limits: AlgorithmLimits,
    cancellation: AlgorithmCancellation,
) -> Result<AlgorithmOutput, AlgorithmError> {
    let pool = Arc::new(crate::ComputePool::new(threads).unwrap());
    let mut registry = AlgorithmRegistry::default();
    register_rank_algorithms(&mut registry)?;
    let control = AlgorithmControl::new(limits.with_compute_threads(threads), cancellation)
        .with_compute_pool(pool);
    registry.execute(Algorithm::Rank(RankAlgorithm::Closeness), graph, &control)
}

fn dense_closeness_graph(nodes: usize) -> AdjacencyGraph {
    let fanout = ((CLOSENESS_PARALLEL_CROSSOVER_EDGE_VISITS as usize) / nodes.max(1).pow(2))
        .saturating_add(2)
        .max(2);
    let edges = (0..nodes)
        .flat_map(|node| (1..=fanout).map(move |hop| (node as u64, ((node + hop) % nodes) as u64)))
        .collect::<Vec<_>>();
    AdjacencyGraph::with_test_edges(nodes as u64, &edges)
}

fn closeness_bits(output: &AlgorithmOutput) -> Vec<u64> {
    closeness_scores(output)
        .into_iter()
        .map(f64::to_bits)
        .collect()
}

fn closeness_scores(output: &AlgorithmOutput) -> Vec<f64> {
    output
        .rows()
        .iter()
        .map(|row| match row[1] {
            AlgorithmValue::Float64(score) => score,
            _ => panic!("closeness score must be Float64"),
        })
        .collect()
}

#[test]
fn closeness_scores_directed_and_undirected_chains_deterministically() {
    let directed = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
    let first = execute_closeness(
        &directed,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    assert_scores_close(&closeness_scores(&first), &[2.0 / 3.0, 0.5, 0.0]);
    assert_eq!(
        first,
        execute_closeness(
            &directed,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap()
    );
    assert_eq!(
        first.schema,
        Algorithm::Rank(RankAlgorithm::Closeness).result_schema()
    );

    let undirected = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0), (1, 2), (2, 1)]);
    assert_scores_close(
        &closeness_scores(
            &execute_closeness(
                &undirected,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap(),
        ),
        &[2.0 / 3.0, 1.0, 2.0 / 3.0],
    );
}

#[test]
fn closeness_handles_parallel_self_loop_disconnected_and_empty_graphs() {
    let graph = AdjacencyGraph::with_test_edges(4, &[(0, 1), (0, 1), (1, 2), (1, 1)]);
    assert_scores_close(
        &closeness_scores(
            &execute_closeness(
                &graph,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap(),
        ),
        &[4.0 / 9.0, 1.0 / 3.0, 0.0, 0.0],
    );
    assert!(
        execute_closeness(
            &AdjacencyGraph::default(),
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap()
        .rows()
        .is_empty()
    );
    assert_eq!(
        closeness_scores(
            &execute_closeness(
                &AdjacencyGraph::with_test_counts(1, 0),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
        ),
        [0.0]
    );
}

#[test]
fn closeness_uses_shared_limits_cancellation_and_dependency_metadata() {
    let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
    assert_eq!(
        execute_closeness(
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
        execute_closeness(
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
        execute_closeness(
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
        execute_closeness(&graph, AlgorithmLimits::default(), cancellation),
        Err(AlgorithmError::Cancelled)
    );
    let edge_heavy = AdjacencyGraph::with_test_edges(1, &vec![(0, 0); 1025]);
    assert_eq!(
        execute_closeness(
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
        .find(|capability| capability.algorithm == Algorithm::Rank(RankAlgorithm::Closeness))
        .unwrap();
    assert_eq!(capability.dependency, BUILTIN_REVIEW);
}

#[test]
fn closeness_path_selection_respects_crossover_and_one_thread() {
    let serial_control = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(4),
        AlgorithmCancellation::default(),
    );
    assert_eq!(
        select_closeness_path(
            &serial_control,
            64,
            (CLOSENESS_PARALLEL_CROSSOVER_EDGE_VISITS / 64) - 1
        ),
        ClosenessExecutionPath::Serial
    );
    let one = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(1),
        AlgorithmCancellation::default(),
    )
    .with_compute_pool(Arc::new(crate::ComputePool::new(1).unwrap()));
    assert_eq!(
        select_closeness_path(&one, 64, CLOSENESS_PARALLEL_CROSSOVER_EDGE_VISITS),
        ClosenessExecutionPath::Serial
    );
    let parallel = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(4),
        AlgorithmCancellation::default(),
    )
    .with_compute_pool(Arc::new(crate::ComputePool::new(4).unwrap()));
    assert_eq!(
        select_closeness_path(&parallel, 64, CLOSENESS_PARALLEL_CROSSOVER_EDGE_VISITS),
        ClosenessExecutionPath::Parallel {
            threads: 4,
            chunks: 4
        }
    );
}

#[test]
fn closeness_thread_matrix_matches_one_thread_bits_and_ordering() {
    let graph = dense_closeness_graph(128);
    assert!(
        estimated_closeness_edge_visits(graph.node_ids().len(), graph.edge_entry_count())
            >= CLOSENESS_PARALLEL_CROSSOVER_EDGE_VISITS
    );
    let serial = execute_closeness_with_pool(&graph, 1, AlgorithmCancellation::default()).unwrap();
    let serial_bits = closeness_bits(&serial);
    let serial_rows = serial.rows();
    for threads in [2_usize, 4, 8] {
        let parallel =
            execute_closeness_with_pool(&graph, threads, AlgorithmCancellation::default()).unwrap();
        assert_eq!(parallel.schema, serial.schema);
        assert_eq!(parallel.rows(), serial_rows);
        assert_eq!(closeness_bits(&parallel), serial_bits);
    }
}

#[test]
fn closeness_parallel_preserves_boundary_graph_bits() {
    let multigraph = AdjacencyGraph::with_test_edges(4, &[(0, 1), (0, 1), (1, 2), (1, 1)]);
    let directed = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
    let undirected = AdjacencyGraph::with_test_edges(2, &[(0, 1), (1, 0)]);
    let empty = AdjacencyGraph::default();
    let single = AdjacencyGraph::with_test_edges(1, &[]);
    let disconnected = AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 2)]);
    for graph in [
        &multigraph,
        &directed,
        &undirected,
        &empty,
        &single,
        &disconnected,
    ] {
        let serial =
            execute_closeness_with_pool(graph, 1, AlgorithmCancellation::default()).unwrap();
        for threads in [2_usize, 4, 8] {
            let parallel =
                execute_closeness_with_pool(graph, threads, AlgorithmCancellation::default())
                    .unwrap();
            assert_eq!(closeness_bits(&parallel), closeness_bits(&serial));
            assert_eq!(parallel.rows(), serial.rows());
        }
    }
}

#[test]
fn closeness_parallel_cancellation_and_limits_are_structured() {
    let graph = dense_closeness_graph(128);
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        execute_closeness_with_pool(&graph, 4, cancellation),
        Err(AlgorithmError::Cancelled)
    );
    assert_eq!(
        execute_closeness_with_pool_and_limits(
            &graph,
            4,
            AlgorithmLimits {
                iterations: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::IterationLimit {
            observed: 1,
            limit: 0
        })
    );
    assert!(matches!(
        execute_closeness_with_pool_and_limits(
            &graph,
            4,
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
fn closeness_source_chunks_cover_canonical_ranges() {
    assert_eq!(source_chunks(0, 4), Vec::<(usize, usize)>::new());
    assert_eq!(source_chunks(5, 1), vec![(0, 5)]);
    assert_eq!(source_chunks(5, 2), vec![(0, 3), (3, 5)]);
    assert_eq!(source_chunks(8, 4), vec![(0, 2), (2, 4), (4, 6), (6, 8)]);
    assert_eq!(source_chunks(3, 8), vec![(0, 1), (1, 2), (2, 3)]);
}

#[test]
fn closeness_worker_panic_returns_structured_error() {
    let pool = crate::ComputePool::new(2).unwrap();
    assert_eq!(
        run_closeness_on_pool(&pool, || -> Result<(), AlgorithmError> {
            panic!("synthetic closeness panic");
        }),
        Err(execution("Closeness worker panicked"))
    );
}
