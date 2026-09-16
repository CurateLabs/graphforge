use super::super::tests::assert_scores_close;
use super::super::*;
use super::*;

fn execute_betweenness(
    graph: &AdjacencyGraph,
    limits: AlgorithmLimits,
    cancellation: AlgorithmCancellation,
) -> Result<AlgorithmOutput, AlgorithmError> {
    let mut registry = AlgorithmRegistry::default();
    register_rank_algorithms(&mut registry)?;
    registry.execute(
        Algorithm::Rank(RankAlgorithm::Betweenness),
        graph,
        &AlgorithmControl::new(limits, cancellation),
    )
}

fn execute_betweenness_with_pool(
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
    registry.execute(Algorithm::Rank(RankAlgorithm::Betweenness), graph, &control)
}

fn betweenness_scores(output: &AlgorithmOutput) -> Vec<f64> {
    output
        .rows()
        .iter()
        .map(|row| match row[1] {
            AlgorithmValue::Float64(score) => score,
            _ => panic!("betweenness score must be Float64"),
        })
        .collect()
}

fn betweenness_bits(output: &AlgorithmOutput) -> Vec<u64> {
    betweenness_scores(output)
        .into_iter()
        .map(f64::to_bits)
        .collect()
}

fn betweenness_parallel_graph() -> AdjacencyGraph {
    let nodes = 72_usize;
    let mut edges = Vec::new();
    for source in 0..nodes {
        let degree = 8 + (source % 17);
        for hop in 1..=degree {
            edges.push((source as u64, ((source + hop) % nodes) as u64));
        }
        if source.is_multiple_of(5) {
            edges.push((source as u64, source as u64));
            edges.push((source as u64, ((source + 1) % nodes) as u64));
        }
    }
    let graph = AdjacencyGraph::with_test_edges(nodes as u64, &edges);
    assert!(
        betweenness_work_estimate(graph.node_ids().len(), graph.edge_entry_count())
            >= BETWEENNESS_PARALLEL_CROSSOVER_WORK
    );
    graph
}

#[test]
fn betweenness_scores_directed_and_undirected_chains_deterministically() {
    let directed = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
    let first = execute_betweenness(
        &directed,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    assert_scores_close(&betweenness_scores(&first), &[0.0, 0.5, 0.0]);
    assert_eq!(
        first,
        execute_betweenness(
            &directed,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap()
    );
    assert_eq!(
        first.schema,
        Algorithm::Rank(RankAlgorithm::Betweenness).result_schema()
    );

    let undirected = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0), (1, 2), (2, 1)]);
    assert_scores_close(
        &betweenness_scores(
            &execute_betweenness(
                &undirected,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap(),
        ),
        &[0.0, 1.0, 0.0],
    );
}

#[test]
fn betweenness_handles_parallel_self_loop_disconnected_and_empty_graphs() {
    let multigraph =
        AdjacencyGraph::with_test_edges(4, &[(0, 1), (0, 1), (1, 2), (0, 3), (3, 2), (1, 1)]);
    assert_scores_close(
        &betweenness_scores(
            &execute_betweenness(
                &multigraph,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap(),
        ),
        &[0.0, 1.0 / 9.0, 0.0, 1.0 / 18.0],
    );

    let disconnected = AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 2)]);
    assert_scores_close(
        &betweenness_scores(
            &execute_betweenness(
                &disconnected,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap(),
        ),
        &[0.0, 1.0 / 6.0, 0.0, 0.0],
    );
    assert!(
        execute_betweenness(
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
fn betweenness_uses_shared_limits_cancellation_and_dependency_metadata() {
    let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
    assert_eq!(
        execute_betweenness(
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
        execute_betweenness(
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
        execute_betweenness(
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
        execute_betweenness(&graph, AlgorithmLimits::default(), cancellation),
        Err(AlgorithmError::Cancelled)
    );
    let edge_heavy = AdjacencyGraph::with_test_edges(1, &vec![(0, 0); 1025]);
    assert_eq!(
        execute_betweenness(
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
        .find(|capability| capability.algorithm == Algorithm::Rank(RankAlgorithm::Betweenness))
        .unwrap();
    assert_eq!(capability.dependency, BUILTIN_REVIEW);
}

#[test]
fn betweenness_path_selection_respects_crossover_and_one_thread() {
    let no_pool = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(4),
        AlgorithmCancellation::default(),
    );
    assert_eq!(
        select_betweenness_path(&no_pool, 128, BETWEENNESS_PARALLEL_CROSSOVER_WORK),
        BetweennessExecutionPath::Serial
    );
    let one = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(1),
        AlgorithmCancellation::default(),
    )
    .with_compute_pool(Arc::new(crate::ComputePool::new(1).unwrap()));
    assert_eq!(
        select_betweenness_path(&one, 128, BETWEENNESS_PARALLEL_CROSSOVER_WORK),
        BetweennessExecutionPath::Serial
    );
    let parallel = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(4),
        AlgorithmCancellation::default(),
    )
    .with_compute_pool(Arc::new(crate::ComputePool::new(4).unwrap()));
    assert_eq!(
        select_betweenness_path(&parallel, 128, 1),
        BetweennessExecutionPath::Serial
    );
    assert_eq!(
        select_betweenness_path(&parallel, 128, BETWEENNESS_PARALLEL_CROSSOVER_WORK),
        BetweennessExecutionPath::Parallel {
            threads: 4,
            chunks: 4
        }
    );
}

#[test]
fn betweenness_thread_matrix_matches_one_thread_bits_and_ordering() {
    let graph = betweenness_parallel_graph();
    let serial = execute_betweenness_with_pool(
        &graph,
        1,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    let serial_bits = betweenness_bits(&serial);
    let serial_rows = serial.rows();
    for threads in [2_usize, 4, 8] {
        let parallel = execute_betweenness_with_pool(
            &graph,
            threads,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_eq!(parallel.schema, serial.schema);
        assert_eq!(parallel.rows(), serial_rows);
        assert_eq!(betweenness_bits(&parallel), serial_bits);
    }
}

#[test]
fn betweenness_parallel_limits_and_cancellation_are_structured() {
    let graph = betweenness_parallel_graph();
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        execute_betweenness_with_pool(&graph, 4, AlgorithmLimits::default(), cancellation),
        Err(AlgorithmError::Cancelled)
    );
    assert_eq!(
        execute_betweenness_with_pool(
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
            limit: 0,
        })
    );
}
