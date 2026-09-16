use super::super::tests::assert_scores_close;
use super::super::tests::hits_hub_scores;
use super::super::*;
use super::*;

fn execute_hits_hub(
    graph: &AdjacencyGraph,
    limits: AlgorithmLimits,
    cancellation: AlgorithmCancellation,
) -> Result<AlgorithmOutput, AlgorithmError> {
    let mut registry = AlgorithmRegistry::default();
    register_rank_algorithms(&mut registry)?;
    registry.execute(
        Algorithm::Rank(RankAlgorithm::HitsHub),
        graph,
        &AlgorithmControl::new(limits, cancellation),
    )
}

fn execute_hits_hub_with_pool(
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
    registry.execute(Algorithm::Rank(RankAlgorithm::HitsHub), graph, &control)
}

fn hits_hub_bits(output: &AlgorithmOutput) -> Vec<u64> {
    hits_hub_scores(output)
        .into_iter()
        .map(f64::to_bits)
        .collect()
}

fn dense_hits_graph(nodes: usize) -> AdjacencyGraph {
    let fanout = ((HITS_PARALLEL_CROSSOVER_EDGES as usize) / nodes.max(1)).saturating_add(3);
    let edges = (0..nodes)
        .flat_map(|source| {
            (0..fanout).map(move |hop| {
                let target = (source + hop + usize::from(hop % 3 == 0)) % nodes;
                (source as u64, target as u64)
            })
        })
        .collect::<Vec<_>>();
    AdjacencyGraph::with_test_edges(nodes as u64, &edges)
}

fn execute_hits_authority(
    graph: &AdjacencyGraph,
    limits: AlgorithmLimits,
    cancellation: AlgorithmCancellation,
) -> Result<AlgorithmOutput, AlgorithmError> {
    let mut registry = AlgorithmRegistry::default();
    register_rank_algorithms(&mut registry)?;
    registry.execute(
        Algorithm::Rank(RankAlgorithm::HitsAuthority),
        graph,
        &AlgorithmControl::new(limits, cancellation),
    )
}

fn execute_hits_authority_with_pool(
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
        Algorithm::Rank(RankAlgorithm::HitsAuthority),
        graph,
        &control,
    )
}

fn hits_authority_scores(output: &AlgorithmOutput) -> Vec<f64> {
    output
        .rows()
        .iter()
        .map(|row| match row[1] {
            AlgorithmValue::Float64(score) => score,
            _ => panic!("HITS authority score must be Float64"),
        })
        .collect()
}

fn hits_authority_bits(output: &AlgorithmOutput) -> Vec<u64> {
    hits_authority_scores(output)
        .into_iter()
        .map(f64::to_bits)
        .collect()
}

#[test]
fn hits_hub_scores_the_canonical_recurrence_deterministically() {
    let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
    let first = execute_hits_hub(
        &graph,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    assert_scores_close(
        &hits_hub_scores(&first),
        &[1.0 / 2.0_f64.sqrt(), 1.0 / 2.0_f64.sqrt(), 0.0],
    );
    assert_eq!(
        first,
        execute_hits_hub(
            &graph,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap()
    );
    assert_eq!(
        first.schema,
        Algorithm::Rank(RankAlgorithm::HitsHub).result_schema()
    );
}

#[test]
fn hits_hub_handles_direction_multigraph_disconnected_and_empty_graphs() {
    let parallel = AdjacencyGraph::with_test_edges(3, &[(0, 2), (0, 2), (1, 2)]);
    assert_scores_close(
        &hits_hub_scores(
            &execute_hits_hub(
                &parallel,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap(),
        ),
        &[2.0 / 5.0_f64.sqrt(), 1.0 / 5.0_f64.sqrt(), 0.0],
    );
    let self_loop = AdjacencyGraph::with_test_edges(1, &[(0, 0)]);
    assert_scores_close(
        &hits_hub_scores(
            &execute_hits_hub(
                &self_loop,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap(),
        ),
        &[1.0],
    );
    let undirected_disconnected = AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0)]);
    assert_scores_close(
        &hits_hub_scores(
            &execute_hits_hub(
                &undirected_disconnected,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap(),
        ),
        &[1.0 / 2.0_f64.sqrt(), 1.0 / 2.0_f64.sqrt(), 0.0, 0.0],
    );
    assert_scores_close(
        &hits_hub_scores(
            &execute_hits_hub(
                &AdjacencyGraph::with_test_counts(2, 0),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap(),
        ),
        &[0.0, 0.0],
    );
    assert!(
        execute_hits_hub(
            &AdjacencyGraph::default(),
            AlgorithmLimits::default(),
            AlgorithmCancellation::default()
        )
        .unwrap()
        .rows()
        .is_empty()
    );
}

#[test]
fn hits_hub_uses_shared_limits_cancellation_and_dependency_metadata() {
    let graph = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
    assert_eq!(
        execute_hits_hub(
            &graph,
            AlgorithmLimits {
                iterations: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default()
        ),
        Err(AlgorithmError::IterationLimit {
            observed: 1,
            limit: 0
        })
    );
    assert!(matches!(
        execute_hits_hub(
            &graph,
            AlgorithmLimits {
                output_rows: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default()
        ),
        Err(AlgorithmError::OutputLimit { .. })
    ));
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        execute_hits_hub(&graph, AlgorithmLimits::default(), cancellation),
        Err(AlgorithmError::Cancelled)
    );
    let edge_heavy = AdjacencyGraph::with_test_edges(1, &vec![(0, 0); 1025]);
    assert!(matches!(
        execute_hits_hub(
            &edge_heavy,
            AlgorithmLimits {
                iterations: 1,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default()
        ),
        Err(AlgorithmError::IterationLimit { .. })
    ));
    let mut registry = AlgorithmRegistry::default();
    register_rank_algorithms(&mut registry).unwrap();
    let capability = registry
        .capabilities()
        .into_iter()
        .find(|capability| capability.algorithm == Algorithm::Rank(RankAlgorithm::HitsHub))
        .unwrap();
    assert_eq!(capability.dependency, BUILTIN_REVIEW);
}

#[test]
fn hits_hub_path_selection_respects_crossover_and_one_thread() {
    let serial_control = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(4),
        AlgorithmCancellation::default(),
    );
    assert_eq!(
        select_hits_path(&serial_control, HITS_PARALLEL_CROSSOVER_EDGES - 1, 64),
        HitsExecutionPath::Serial
    );
    let one = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(1),
        AlgorithmCancellation::default(),
    )
    .with_compute_pool(Arc::new(crate::ComputePool::new(1).unwrap()));
    assert_eq!(
        select_hits_path(&one, HITS_PARALLEL_CROSSOVER_EDGES, 64),
        HitsExecutionPath::Serial
    );
    let parallel = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(4),
        AlgorithmCancellation::default(),
    )
    .with_compute_pool(Arc::new(crate::ComputePool::new(4).unwrap()));
    assert_eq!(
        select_hits_path(&parallel, HITS_PARALLEL_CROSSOVER_EDGES, 64),
        HitsExecutionPath::Parallel {
            threads: 4,
            chunks: 4
        }
    );
}

#[test]
fn hits_hub_thread_matrix_matches_one_thread_bits_and_ordering() {
    let graph = dense_hits_graph(128);
    assert!(graph.edge_entry_count() >= HITS_PARALLEL_CROSSOVER_EDGES);
    let serial = execute_hits_hub_with_pool(&graph, 1, AlgorithmCancellation::default()).unwrap();
    let serial_bits = hits_hub_bits(&serial);
    let serial_rows = serial.rows();
    for threads in [2_usize, 4, 8] {
        let parallel =
            execute_hits_hub_with_pool(&graph, threads, AlgorithmCancellation::default()).unwrap();
        assert_eq!(parallel.schema, serial.schema);
        assert_eq!(parallel.rows(), serial_rows);
        assert_eq!(hits_hub_bits(&parallel), serial_bits);
    }
}

#[test]
fn hits_hub_parallel_preserves_multigraph_self_loop_and_disconnected_bits() {
    let mut edges = Vec::new();
    let nodes = 256_u64;
    for source in 0..nodes {
        edges.push((source, source));
        edges.push((source, (source + 1) % nodes));
        edges.push((source, (source + 1) % nodes));
        for hop in 2..18 {
            edges.push((source, (source + hop) % nodes));
        }
    }
    let graph = AdjacencyGraph::with_test_edges(nodes, &edges);
    assert!(graph.edge_entry_count() >= HITS_PARALLEL_CROSSOVER_EDGES);
    let serial = execute_hits_hub_with_pool(&graph, 1, AlgorithmCancellation::default()).unwrap();
    for threads in [2_usize, 4, 8] {
        let parallel =
            execute_hits_hub_with_pool(&graph, threads, AlgorithmCancellation::default()).unwrap();
        assert_eq!(hits_hub_bits(&parallel), hits_hub_bits(&serial));
        assert_eq!(parallel.rows(), serial.rows());
    }
}

#[test]
fn hits_hub_parallel_cancellation_and_worker_panic_are_structured() {
    let graph = dense_hits_graph(128);
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        execute_hits_hub_with_pool(&graph, 4, cancellation),
        Err(AlgorithmError::Cancelled)
    );

    let pool = crate::ComputePool::new(2).unwrap();
    assert_eq!(
        run_hits_on_pool(&pool, || -> Result<(), AlgorithmError> {
            panic!("synthetic HITS worker panic");
        }),
        Err(AlgorithmError::Execution {
            message: "HITS worker panicked".to_string()
        })
    );
}

#[test]
fn hits_authority_scores_the_canonical_recurrence() {
    let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
    let first = execute_hits_authority(
        &graph,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    assert_scores_close(
        &hits_authority_scores(&first),
        &[0.0, 1.0 / 2.0_f64.sqrt(), 1.0 / 2.0_f64.sqrt()],
    );
    assert_eq!(
        first.schema,
        Algorithm::Rank(RankAlgorithm::HitsAuthority).result_schema()
    );
}

#[test]
fn hits_authority_handles_multigraph_disconnected_and_empty_graphs() {
    let parallel = AdjacencyGraph::with_test_edges(3, &[(0, 1), (0, 1), (0, 2)]);
    assert_scores_close(
        &hits_authority_scores(
            &execute_hits_authority(
                &parallel,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap(),
        ),
        &[0.0, 2.0 / 5.0_f64.sqrt(), 1.0 / 5.0_f64.sqrt()],
    );
    let self_loop = AdjacencyGraph::with_test_edges(1, &[(0, 0)]);
    assert_scores_close(
        &hits_authority_scores(
            &execute_hits_authority(
                &self_loop,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap(),
        ),
        &[1.0],
    );
    let undirected_disconnected = AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0)]);
    assert_scores_close(
        &hits_authority_scores(
            &execute_hits_authority(
                &undirected_disconnected,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap(),
        ),
        &[1.0 / 2.0_f64.sqrt(), 1.0 / 2.0_f64.sqrt(), 0.0, 0.0],
    );
    assert_scores_close(
        &hits_authority_scores(
            &execute_hits_authority(
                &AdjacencyGraph::with_test_counts(2, 0),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap(),
        ),
        &[0.0, 0.0],
    );
    assert!(
        execute_hits_authority(
            &AdjacencyGraph::default(),
            AlgorithmLimits::default(),
            AlgorithmCancellation::default()
        )
        .unwrap()
        .rows()
        .is_empty()
    );
}

#[test]
fn hits_authority_uses_shared_limits_cancellation_and_dependency_metadata() {
    let graph = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
    assert_eq!(
        execute_hits_authority(
            &graph,
            AlgorithmLimits {
                iterations: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default()
        ),
        Err(AlgorithmError::IterationLimit {
            observed: 1,
            limit: 0
        })
    );
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        execute_hits_authority(&graph, AlgorithmLimits::default(), cancellation),
        Err(AlgorithmError::Cancelled)
    );
    let mut registry = AlgorithmRegistry::default();
    register_rank_algorithms(&mut registry).unwrap();
    let capability = registry
        .capabilities()
        .into_iter()
        .find(|capability| capability.algorithm == Algorithm::Rank(RankAlgorithm::HitsAuthority))
        .unwrap();
    assert_eq!(capability.dependency, BUILTIN_REVIEW);
}

#[test]
fn hits_authority_path_selection_reuses_shared_hits_crossover() {
    let parallel = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(8),
        AlgorithmCancellation::default(),
    )
    .with_compute_pool(Arc::new(crate::ComputePool::new(8).unwrap()));
    assert_eq!(
        select_hits_path(&parallel, HITS_PARALLEL_CROSSOVER_EDGES, 128),
        HitsExecutionPath::Parallel {
            threads: 8,
            chunks: 8
        }
    );
    assert_eq!(
        select_hits_path(&parallel, HITS_PARALLEL_CROSSOVER_EDGES - 1, 128),
        HitsExecutionPath::Serial
    );
}

#[test]
fn hits_authority_thread_matrix_matches_one_thread_bits_and_ordering() {
    let graph = dense_hits_graph(128);
    assert!(graph.edge_entry_count() >= HITS_PARALLEL_CROSSOVER_EDGES);
    let serial =
        execute_hits_authority_with_pool(&graph, 1, AlgorithmCancellation::default()).unwrap();
    let serial_bits = hits_authority_bits(&serial);
    let serial_rows = serial.rows();
    for threads in [2_usize, 4, 8] {
        let parallel =
            execute_hits_authority_with_pool(&graph, threads, AlgorithmCancellation::default())
                .unwrap();
        assert_eq!(parallel.schema, serial.schema);
        assert_eq!(parallel.rows(), serial_rows);
        assert_eq!(hits_authority_bits(&parallel), serial_bits);
    }
}

#[test]
fn hits_authority_parallel_preserves_multigraph_self_loop_and_disconnected_bits() {
    let mut edges = Vec::new();
    let nodes = 256_u64;
    for source in 0..nodes {
        edges.push((source, source));
        edges.push((source, (source + 1) % nodes));
        edges.push((source, (source + 1) % nodes));
        for hop in 2..18 {
            edges.push((source, (source + hop) % nodes));
        }
    }
    let graph = AdjacencyGraph::with_test_edges(nodes, &edges);
    assert!(graph.edge_entry_count() >= HITS_PARALLEL_CROSSOVER_EDGES);
    let serial =
        execute_hits_authority_with_pool(&graph, 1, AlgorithmCancellation::default()).unwrap();
    for threads in [2_usize, 4, 8] {
        let parallel =
            execute_hits_authority_with_pool(&graph, threads, AlgorithmCancellation::default())
                .unwrap();
        assert_eq!(hits_authority_bits(&parallel), hits_authority_bits(&serial));
        assert_eq!(parallel.rows(), serial.rows());
    }
}

#[test]
fn hits_authority_parallel_cancellation_returns_structured_cancelled() {
    let graph = dense_hits_graph(128);
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        execute_hits_authority_with_pool(&graph, 4, cancellation),
        Err(AlgorithmError::Cancelled)
    );
}
