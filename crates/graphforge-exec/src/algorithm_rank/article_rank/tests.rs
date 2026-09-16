use super::super::tests::assert_scores_close;
use super::super::*;
use super::*;

fn execute_article_rank(
    graph: &AdjacencyGraph,
    limits: AlgorithmLimits,
    cancellation: AlgorithmCancellation,
) -> Result<AlgorithmOutput, AlgorithmError> {
    let mut registry = AlgorithmRegistry::default();
    register_rank_algorithms(&mut registry)?;
    registry.execute(
        Algorithm::Rank(RankAlgorithm::ArticleRank),
        graph,
        &AlgorithmControl::new(limits, cancellation),
    )
}

fn execute_article_rank_with_pool(
    graph: &AdjacencyGraph,
    threads: usize,
    cancellation: AlgorithmCancellation,
) -> Result<AlgorithmOutput, AlgorithmError> {
    let pool = Arc::new(crate::ComputePool::new(threads).unwrap());
    execute_article_rank_with_shared_pool(graph, pool, cancellation)
}

fn execute_article_rank_with_shared_pool(
    graph: &AdjacencyGraph,
    pool: Arc<crate::ComputePool>,
    cancellation: AlgorithmCancellation,
) -> Result<AlgorithmOutput, AlgorithmError> {
    let threads = pool.num_threads();
    let mut registry = AlgorithmRegistry::default();
    register_rank_algorithms(&mut registry)?;
    let control = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(threads),
        cancellation,
    )
    .with_compute_pool(pool);
    registry.execute(Algorithm::Rank(RankAlgorithm::ArticleRank), graph, &control)
}

fn article_rank_scores(output: &AlgorithmOutput) -> Vec<f64> {
    output
        .rows()
        .iter()
        .map(|row| match row[1] {
            AlgorithmValue::Float64(score) => score,
            _ => panic!("ArticleRank score must be Float64"),
        })
        .collect()
}

fn article_rank_bits(output: &AlgorithmOutput) -> Vec<u64> {
    article_rank_scores(output)
        .into_iter()
        .map(f64::to_bits)
        .collect()
}

fn article_rank_fingerprint(output: &AlgorithmOutput) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update((output.rows().len() as u64).to_le_bytes());
    for row in output.rows() {
        match row.as_slice() {
            [AlgorithmValue::Uuid(uuid), AlgorithmValue::Float64(score)] => {
                hasher.update(uuid);
                hasher.update(score.to_bits().to_le_bytes());
            }
            _ => panic!("ArticleRank output row must contain uuid and score"),
        }
    }
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn dense_article_rank_graph(nodes: usize) -> AdjacencyGraph {
    let fanout =
        ((ARTICLE_RANK_PARALLEL_CROSSOVER_EDGES as usize) / nodes.max(1)).saturating_add(2);
    let edges = (0..nodes)
        .flat_map(|node| (1..=fanout).map(move |hop| (node as u64, ((node + hop) % nodes) as u64)))
        .collect::<Vec<_>>();
    AdjacencyGraph::with_test_edges(nodes as u64, &edges)
}

#[test]
fn article_rank_scores_the_canonical_recurrence_deterministically() {
    let graph = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
    let first = execute_article_rank(
        &graph,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    assert_scores_close(&article_rank_scores(&first), &[0.15, 0.235]);
    assert_eq!(
        first,
        execute_article_rank(
            &graph,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap()
    );
    assert_eq!(
        first.schema,
        Algorithm::Rank(RankAlgorithm::ArticleRank).result_schema()
    );
}

#[test]
fn article_rank_handles_direction_multigraph_disconnected_and_empty_graphs() {
    let directed = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
    let undirected = AdjacencyGraph::with_test_edges(2, &[(0, 1), (1, 0)]);
    assert_ne!(
        article_rank_scores(
            &execute_article_rank(
                &directed,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default()
            )
            .unwrap()
        ),
        article_rank_scores(
            &execute_article_rank(
                &undirected,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default()
            )
            .unwrap()
        )
    );
    let multigraph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (0, 1), (0, 2), (1, 1)]);
    let scores = article_rank_scores(
        &execute_article_rank(
            &multigraph,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap(),
    );
    assert!(scores[1] > scores[2]);
    assert!(scores[1] > scores[0]);
    let disconnected = AdjacencyGraph::with_test_edges(3, &[(0, 1)]);
    assert_scores_close(
        &article_rank_scores(
            &execute_article_rank(
                &disconnected,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap(),
        ),
        &[0.15, 0.245_625, 0.15],
    );
    assert_scores_close(
        &article_rank_scores(
            &execute_article_rank(
                &AdjacencyGraph::with_test_counts(3, 0),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap(),
        ),
        &[0.15; 3],
    );
    assert!(
        execute_article_rank(
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
fn article_rank_uses_shared_limits_cancellation_and_dependency_metadata() {
    let graph = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
    assert_eq!(
        execute_article_rank(
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
        execute_article_rank(
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
        execute_article_rank(&graph, AlgorithmLimits::default(), cancellation),
        Err(AlgorithmError::Cancelled)
    );
    let edge_heavy = AdjacencyGraph::with_test_edges(1, &vec![(0, 0); 1025]);
    assert!(matches!(
        execute_article_rank(
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
        .find(|capability| capability.algorithm == Algorithm::Rank(RankAlgorithm::ArticleRank))
        .unwrap();
    assert_eq!(capability.dependency, BUILTIN_REVIEW);
}

#[test]
fn article_rank_path_selection_respects_crossover_and_one_thread() {
    let serial_control = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(4),
        AlgorithmCancellation::default(),
    );
    assert_eq!(
        select_article_rank_path(
            &serial_control,
            ARTICLE_RANK_PARALLEL_CROSSOVER_EDGES - 1,
            64
        ),
        ArticleRankExecutionPath::Serial
    );
    let one = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(1),
        AlgorithmCancellation::default(),
    )
    .with_compute_pool(Arc::new(crate::ComputePool::new(1).unwrap()));
    assert_eq!(
        select_article_rank_path(&one, ARTICLE_RANK_PARALLEL_CROSSOVER_EDGES, 64),
        ArticleRankExecutionPath::Serial
    );
    let parallel = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(4),
        AlgorithmCancellation::default(),
    )
    .with_compute_pool(Arc::new(crate::ComputePool::new(4).unwrap()));
    assert_eq!(
        select_article_rank_path(&parallel, ARTICLE_RANK_PARALLEL_CROSSOVER_EDGES, 64),
        ArticleRankExecutionPath::Parallel {
            threads: 4,
            chunks: 4
        }
    );
}

#[test]
fn article_rank_thread_matrix_matches_one_thread_bits_and_ordering() {
    let graph = dense_article_rank_graph(128);
    assert!(graph.edge_entry_count() >= ARTICLE_RANK_PARALLEL_CROSSOVER_EDGES);
    let serial =
        execute_article_rank_with_pool(&graph, 1, AlgorithmCancellation::default()).unwrap();
    let serial_bits = article_rank_bits(&serial);
    let serial_rows = serial.rows();
    for threads in [2_usize, 4, 8] {
        let parallel =
            execute_article_rank_with_pool(&graph, threads, AlgorithmCancellation::default())
                .unwrap();
        assert_eq!(parallel.schema, serial.schema);
        assert_eq!(parallel.rows(), serial_rows);
        assert_eq!(article_rank_bits(&parallel), serial_bits);
    }
}

#[test]
fn article_rank_parallel_preserves_multigraph_direction_and_disconnected_bits() {
    let multigraph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (0, 1), (0, 2), (1, 1)]);
    let directed = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
    let undirected = AdjacencyGraph::with_test_edges(2, &[(0, 1), (1, 0)]);
    let empty = AdjacencyGraph::default();
    let single = AdjacencyGraph::with_test_edges(1, &[]);
    let disconnected = AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0), (2, 3), (3, 2)]);
    for graph in [
        &multigraph,
        &directed,
        &undirected,
        &empty,
        &single,
        &disconnected,
    ] {
        let serial =
            execute_article_rank_with_pool(graph, 1, AlgorithmCancellation::default()).unwrap();
        for threads in [2_usize, 4, 8] {
            let parallel =
                execute_article_rank_with_pool(graph, threads, AlgorithmCancellation::default())
                    .unwrap();
            assert_eq!(article_rank_bits(&parallel), article_rank_bits(&serial));
            assert_eq!(parallel.rows(), serial.rows());
        }
    }

    let nodes = 512_u64;
    let mut edges = Vec::new();
    for source in 0..nodes {
        let degree = 256 + (source % 13) as usize;
        for hop in 0..degree {
            edges.push((source, (source + hop as u64) % nodes));
        }
    }
    let adversarial = AdjacencyGraph::with_test_edges(nodes, &edges);
    assert!(adversarial.edge_entry_count() >= ARTICLE_RANK_PARALLEL_CROSSOVER_EDGES);
    let serial =
        execute_article_rank_with_pool(&adversarial, 1, AlgorithmCancellation::default()).unwrap();
    for threads in [2_usize, 4, 8] {
        let parallel =
            execute_article_rank_with_pool(&adversarial, threads, AlgorithmCancellation::default())
                .unwrap();
        assert_eq!(article_rank_bits(&parallel), article_rank_bits(&serial));
    }
}

#[test]
#[ignore = "manual crossover measurement; run with --ignored --nocapture"]
fn measure_article_rank_parallel_crossover() {
    use std::time::Instant;

    let serial_pool = Arc::new(crate::ComputePool::new(1).unwrap());
    let parallel_pool = Arc::new(crate::ComputePool::new(4).unwrap());
    for &(nodes, fanout) in &[
        (64usize, 16usize),
        (64, 32),
        (128, 32),
        (128, 64),
        (256, 64),
        (512, 64),
        (1024, 128),
        (2048, 128),
    ] {
        let edges = (0..nodes)
            .flat_map(|node| {
                (0..fanout).map(move |hop| (node as u64, ((node + hop) % nodes) as u64))
            })
            .collect::<Vec<_>>();
        let graph = AdjacencyGraph::with_test_edges(nodes as u64, &edges);
        let edge_count = graph.edge_entry_count();
        let serial = execute_article_rank_with_shared_pool(
            &graph,
            serial_pool.clone(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        let expected = article_rank_fingerprint(&serial);
        // Warm once so timings emphasize the kernel path over first-use setup.
        let parallel = execute_article_rank_with_shared_pool(
            &graph,
            parallel_pool.clone(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_eq!(article_rank_fingerprint(&parallel), expected);

        let mut serial_ns = u128::MAX;
        let mut parallel_ns = u128::MAX;
        for _ in 0..5 {
            let t0 = Instant::now();
            let serial = execute_article_rank_with_shared_pool(
                &graph,
                serial_pool.clone(),
                AlgorithmCancellation::default(),
            )
            .unwrap();
            serial_ns = serial_ns.min(t0.elapsed().as_nanos());

            let t1 = Instant::now();
            let parallel = execute_article_rank_with_shared_pool(
                &graph,
                parallel_pool.clone(),
                AlgorithmCancellation::default(),
            )
            .unwrap();
            parallel_ns = parallel_ns.min(t1.elapsed().as_nanos());
            assert_eq!(article_rank_fingerprint(&serial), expected);
            assert_eq!(article_rank_fingerprint(&parallel), expected);
        }
        eprintln!(
            "article_rank nodes={nodes} fanout={fanout} edges={edge_count} serial_ns={serial_ns} parallel_ns={parallel_ns} ratio={} fingerprint={expected}",
            parallel_ns as f64 / serial_ns as f64
        );
    }
}

#[test]
fn article_rank_parallel_cancellation_and_worker_panic_are_structured() {
    let graph = dense_article_rank_graph(128);
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        execute_article_rank_with_pool(&graph, 4, cancellation),
        Err(AlgorithmError::Cancelled)
    );

    let pool = crate::ComputePool::new(2).unwrap();
    assert_eq!(
        run_article_rank_on_pool(&pool, || -> Result<(), AlgorithmError> {
            panic!("synthetic ArticleRank worker panic")
        }),
        Err(execution("ArticleRank worker panicked"))
    );
}
