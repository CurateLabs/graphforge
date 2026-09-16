use super::super::tests::hits_hub_scores;
use super::super::*;
use super::*;

fn execute_common_neighbors(
    graph: &AdjacencyGraph,
    limits: AlgorithmLimits,
    cancellation: AlgorithmCancellation,
) -> Result<AlgorithmOutput, AlgorithmError> {
    let mut registry = AlgorithmRegistry::default();
    register_rank_algorithms(&mut registry)?;
    registry.execute(
        Algorithm::Rank(RankAlgorithm::CommonNeighbors),
        graph,
        &AlgorithmControl::new(limits, cancellation),
    )
}

fn execute_common_neighbors_with_pool(
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
    registry.execute(
        Algorithm::Rank(RankAlgorithm::CommonNeighbors),
        graph,
        &control,
    )
}

fn common_neighbor_output_scores(output: &AlgorithmOutput) -> Vec<f64> {
    hits_hub_scores(output)
}

fn common_neighbor_bits(output: &AlgorithmOutput) -> Vec<u64> {
    common_neighbor_output_scores(output)
        .into_iter()
        .map(f64::to_bits)
        .collect()
}

fn dense_common_neighbors_graph(nodes: usize) -> AdjacencyGraph {
    let max_fanout = nodes.saturating_sub(1).max(1) / 2;
    let fanout = ((COMMON_NEIGHBORS_PARALLEL_CROSSOVER_WORK as usize)
        / (nodes.max(1) * nodes.max(1)))
    .clamp(4, max_fanout.max(1));
    common_neighbors_ring_graph(nodes, fanout)
}

fn common_neighbors_ring_graph(nodes: usize, fanout: usize) -> AdjacencyGraph {
    let fanout = fanout.clamp(1, (nodes.saturating_sub(1) / 2).max(1));
    let edges = (0..nodes)
        .flat_map(|node| (1..=fanout).map(move |hop| (node as u64, ((node + hop) % nodes) as u64)))
        .collect::<Vec<_>>();
    AdjacencyGraph::with_test_edges(nodes as u64, &edges)
}

#[test]
fn common_neighbors_aggregates_missing_directed_links_deterministically() {
    let graph = AdjacencyGraph::with_test_edges(
        5,
        &[
            (0, 2),
            (0, 2),
            (0, 3),
            (0, 0),
            (1, 2),
            (1, 3),
            (2, 0),
            (2, 4),
            (3, 4),
        ],
    );
    let output = execute_common_neighbors(
        &graph,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    assert_eq!(
        common_neighbor_output_scores(&output),
        [2.0, 2.0, 1.0, 1.0, 0.0]
    );
    assert_eq!(
        output,
        execute_common_neighbors(
            &graph,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap()
    );
}

#[test]
fn common_neighbors_obeys_undirected_and_boundary_contracts() {
    let undirected = AdjacencyGraph::with_test_edges(
        5,
        &[
            (0, 2),
            (2, 0),
            (0, 3),
            (3, 0),
            (1, 2),
            (2, 1),
            (1, 3),
            (3, 1),
            (2, 4),
            (4, 2),
            (3, 4),
            (4, 3),
        ],
    );
    assert_eq!(
        common_neighbor_output_scores(
            &execute_common_neighbors(
                &undirected,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap(),
        ),
        [4.0, 4.0, 3.0, 3.0, 4.0]
    );
    for graph in [
        AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0), (0, 2), (2, 0), (1, 2), (2, 1)]),
        AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0), (2, 3), (3, 2)]),
        AdjacencyGraph::with_test_counts(3, 0),
    ] {
        assert!(
            common_neighbor_output_scores(
                &execute_common_neighbors(
                    &graph,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            )
            .into_iter()
            .all(|score| score == 0.0)
        );
    }
    assert!(
        execute_common_neighbors(
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
fn common_neighbors_uses_shared_controls_and_dependency_metadata() {
    let graph = AdjacencyGraph::with_test_edges(3, &[(0, 2), (1, 2)]);
    assert!(matches!(
        execute_common_neighbors(
            &graph,
            AlgorithmLimits {
                iterations: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        ),
        Err(AlgorithmError::IterationLimit { .. })
    ));
    assert!(matches!(
        execute_common_neighbors(
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
        execute_common_neighbors(&graph, AlgorithmLimits::default(), cancellation),
        Err(AlgorithmError::Cancelled)
    );
    assert!(matches!(
        exact_u64_as_f64((1_u64 << 53) + 1, "common-neighbors score"),
        Err(AlgorithmError::Execution { .. })
    ));
    let mut registry = AlgorithmRegistry::default();
    register_rank_algorithms(&mut registry).unwrap();
    let capability = registry
        .capabilities()
        .into_iter()
        .find(|capability| capability.algorithm == Algorithm::Rank(RankAlgorithm::CommonNeighbors))
        .unwrap();
    assert_eq!(capability.dependency, BUILTIN_REVIEW);
    assert_eq!(capability.algorithm.as_str(), "common_neighbors");
}

#[test]
fn common_neighbors_path_selection_respects_crossover_and_one_thread() {
    let no_pool = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(4),
        AlgorithmCancellation::default(),
    );
    assert_eq!(
        select_common_neighbors_path(&no_pool, 64, COMMON_NEIGHBORS_PARALLEL_CROSSOVER_WORK),
        CommonNeighborsExecutionPath::Serial
    );
    let one = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(1),
        AlgorithmCancellation::default(),
    )
    .with_compute_pool(Arc::new(crate::ComputePool::new(1).unwrap()));
    assert_eq!(
        select_common_neighbors_path(&one, 64, COMMON_NEIGHBORS_PARALLEL_CROSSOVER_WORK),
        CommonNeighborsExecutionPath::Serial
    );
    let small = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(4),
        AlgorithmCancellation::default(),
    )
    .with_compute_pool(Arc::new(crate::ComputePool::new(4).unwrap()));
    assert_eq!(
        select_common_neighbors_path(&small, 64, COMMON_NEIGHBORS_PARALLEL_CROSSOVER_WORK - 1),
        CommonNeighborsExecutionPath::Serial
    );
    assert_eq!(
        select_common_neighbors_path(&small, 64, COMMON_NEIGHBORS_PARALLEL_CROSSOVER_WORK),
        CommonNeighborsExecutionPath::Parallel {
            threads: 4,
            chunks: 4
        }
    );
}

#[test]
fn common_neighbors_thread_matrix_matches_one_thread_bits_and_ordering() {
    let graph = dense_common_neighbors_graph(128);
    let neighbors = simple_neighbors(
        &graph,
        &AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default()),
        false,
    )
    .unwrap();
    assert!(
        estimated_common_neighbors_work(&neighbors) >= COMMON_NEIGHBORS_PARALLEL_CROSSOVER_WORK
    );
    let serial = execute_common_neighbors_with_pool(
        &graph,
        1,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    let serial_rows = serial.rows();
    let serial_bits = common_neighbor_bits(&serial);
    for threads in [2_usize, 4, 8] {
        let parallel = execute_common_neighbors_with_pool(
            &graph,
            threads,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_eq!(parallel.schema, serial.schema);
        assert_eq!(parallel.rows(), serial_rows);
        assert_eq!(common_neighbor_bits(&parallel), serial_bits);
    }
}

#[test]
fn common_neighbors_parallel_preserves_boundary_bits() {
    let multigraph = AdjacencyGraph::with_test_edges(
        5,
        &[
            (0, 2),
            (0, 2),
            (0, 3),
            (0, 0),
            (1, 2),
            (1, 3),
            (2, 0),
            (2, 4),
            (3, 4),
        ],
    );
    let directed = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
    let undirected = AdjacencyGraph::with_test_edges(2, &[(0, 1), (1, 0)]);
    let complete =
        AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0), (0, 2), (2, 0), (1, 2), (2, 1)]);
    let empty = AdjacencyGraph::default();
    let single = AdjacencyGraph::with_test_edges(1, &[]);
    let disconnected = AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0), (2, 3), (3, 2)]);
    for graph in [
        &multigraph,
        &directed,
        &undirected,
        &complete,
        &empty,
        &single,
        &disconnected,
    ] {
        let serial = execute_common_neighbors_with_pool(
            graph,
            1,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        for threads in [2_usize, 4, 8] {
            let parallel = execute_common_neighbors_with_pool(
                graph,
                threads,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap();
            assert_eq!(
                common_neighbor_bits(&parallel),
                common_neighbor_bits(&serial)
            );
            assert_eq!(parallel.rows(), serial.rows());
        }
    }
}

#[test]
fn common_neighbors_parallel_limits_and_cancellation_return_structured() {
    let graph = dense_common_neighbors_graph(128);
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        execute_common_neighbors_with_pool(&graph, 4, AlgorithmLimits::default(), cancellation),
        Err(AlgorithmError::Cancelled)
    );
    assert!(matches!(
        execute_common_neighbors_with_pool(
            &graph,
            4,
            AlgorithmLimits {
                iterations: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default()
        ),
        Err(AlgorithmError::IterationLimit { .. })
    ));
    assert!(matches!(
        execute_common_neighbors_with_pool(
            &graph,
            4,
            AlgorithmLimits {
                output_rows: 2,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default()
        ),
        Err(AlgorithmError::OutputLimit { .. })
    ));
}

#[test]
fn common_neighbors_source_chunks_cover_canonical_ranges() {
    assert_eq!(source_chunks(0, 4), Vec::<(usize, usize)>::new());
    assert_eq!(source_chunks(5, 1), vec![(0, 5)]);
    assert_eq!(source_chunks(5, 2), vec![(0, 3), (3, 5)]);
    assert_eq!(source_chunks(8, 4), vec![(0, 2), (2, 4), (4, 6), (6, 8)]);
    assert_eq!(source_chunks(3, 8), vec![(0, 1), (1, 2), (2, 3)]);
}

#[test]
#[ignore = "manual crossover measurement; run with --ignored --nocapture"]
fn measure_common_neighbors_parallel_crossover() {
    use std::time::Instant;

    for (nodes, fanout) in [
        (64_usize, 8_usize),
        (96, 12),
        (128, 16),
        (192, 16),
        (256, 16),
        (512, 32),
        (1024, 32),
    ] {
        let graph = common_neighbors_ring_graph(nodes, fanout);
        let neighbors = simple_neighbors(
            &graph,
            &AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default()),
            false,
        )
        .unwrap();
        let work = estimated_common_neighbors_work(&neighbors);
        let measurement_limits = AlgorithmLimits {
            iterations: 1_000_000,
            ..AlgorithmLimits::default()
        };
        let serial_ctl = AlgorithmControl::new(
            measurement_limits.with_compute_threads(1),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(Arc::new(crate::ComputePool::new(1).unwrap()));
        let parallel_ctl = AlgorithmControl::new(
            measurement_limits.with_compute_threads(4),
            AlgorithmCancellation::default(),
        )
        .with_compute_pool(Arc::new(crate::ComputePool::new(4).unwrap()));
        let mut serial_ns = u128::MAX;
        let mut parallel_ns = u128::MAX;
        for _ in 0..5 {
            let t0 = Instant::now();
            let serial = common_neighbor_scores_serial(&neighbors, &serial_ctl).unwrap();
            serial_ns = serial_ns.min(t0.elapsed().as_nanos());
            let serial_bits = serial.iter().copied().map(f64::to_bits).collect::<Vec<_>>();

            let t1 = Instant::now();
            let parallel = common_neighbor_scores_parallel(&neighbors, &parallel_ctl).unwrap();
            parallel_ns = parallel_ns.min(t1.elapsed().as_nanos());
            let parallel_bits = parallel
                .iter()
                .copied()
                .map(f64::to_bits)
                .collect::<Vec<_>>();
            assert_eq!(parallel_bits, serial_bits);
        }
        println!(
            "nodes={nodes} fanout={fanout} work={work} serial_ns={serial_ns} parallel_ns={parallel_ns} ratio={}",
            parallel_ns as f64 / serial_ns as f64
        );
    }
}
