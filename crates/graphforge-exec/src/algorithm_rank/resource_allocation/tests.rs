use super::super::tests::assert_scores_close;
use super::super::tests::hits_hub_scores;
use super::super::*;
use super::*;

fn execute_resource_allocation(
    graph: &AdjacencyGraph,
    limits: AlgorithmLimits,
    cancellation: AlgorithmCancellation,
) -> Result<AlgorithmOutput, AlgorithmError> {
    let mut registry = AlgorithmRegistry::default();
    register_rank_algorithms(&mut registry)?;
    registry.execute(
        Algorithm::Rank(RankAlgorithm::ResourceAllocation),
        graph,
        &AlgorithmControl::new(limits, cancellation),
    )
}

fn resource_allocation_output_scores(output: &AlgorithmOutput) -> Vec<f64> {
    hits_hub_scores(output)
}

#[test]
fn resource_allocation_aggregates_missing_directed_links_deterministically() {
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
    let output = execute_resource_allocation(
        &graph,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    assert_scores_close(
        &resource_allocation_output_scores(&output),
        &[1.0, 1.0, 0.5, 0.5, 0.0],
    );
    assert_eq!(
        output,
        execute_resource_allocation(
            &graph,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap()
    );
}

#[test]
fn resource_allocation_obeys_undirected_and_boundary_contracts() {
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
    assert_scores_close(
        &resource_allocation_output_scores(
            &execute_resource_allocation(
                &undirected,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap(),
        ),
        &[4.0 / 3.0, 4.0 / 3.0, 1.5, 1.5, 4.0 / 3.0],
    );
    for graph in [
        AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0), (0, 2), (2, 0), (1, 2), (2, 1)]),
        AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0), (2, 3), (3, 2)]),
        AdjacencyGraph::with_test_counts(3, 0),
    ] {
        assert!(
            resource_allocation_output_scores(
                &execute_resource_allocation(
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
        execute_resource_allocation(
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
fn resource_allocation_uses_shared_controls_and_dependency_metadata() {
    let graph = AdjacencyGraph::with_test_edges(3, &[(0, 2), (1, 2)]);
    assert!(matches!(
        execute_resource_allocation(
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
        execute_resource_allocation(
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
        execute_resource_allocation(&graph, AlgorithmLimits::default(), cancellation),
        Err(AlgorithmError::Cancelled)
    );
    assert!(matches!(
        resource_allocation_discount(1),
        Err(AlgorithmError::Execution { .. })
    ));
    let mut registry = AlgorithmRegistry::default();
    register_rank_algorithms(&mut registry).unwrap();
    let capability = registry
        .capabilities()
        .into_iter()
        .find(|capability| {
            capability.algorithm == Algorithm::Rank(RankAlgorithm::ResourceAllocation)
        })
        .unwrap();
    assert_eq!(capability.dependency, BUILTIN_REVIEW);
    assert_eq!(capability.algorithm.as_str(), "resource_allocation");
}

fn execute_resource_allocation_with_pool(
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
        Algorithm::Rank(RankAlgorithm::ResourceAllocation),
        graph,
        &control,
    )
}

fn dense_resource_allocation_graph(nodes: usize) -> AdjacencyGraph {
    let max_fanout = nodes.saturating_sub(1).max(1) / 2;
    let fanout = ((RESOURCE_ALLOCATION_PARALLEL_CROSSOVER_WORK as usize)
        / (nodes.max(1) * nodes.max(1)))
    .clamp(4, max_fanout.max(1));
    resource_allocation_ring_graph(nodes, fanout)
}

fn resource_allocation_ring_graph(nodes: usize, fanout: usize) -> AdjacencyGraph {
    let fanout = fanout.clamp(1, (nodes.saturating_sub(1) / 2).max(1));
    let edges = (0..nodes)
        .flat_map(|node| (1..=fanout).map(move |hop| (node as u64, ((node + hop) % nodes) as u64)))
        .collect::<Vec<_>>();
    AdjacencyGraph::with_test_edges(nodes as u64, &edges)
}

#[test]
#[ignore = "manual crossover measurement; run with --ignored --nocapture"]
fn measure_resource_allocation_parallel_crossover() {
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
        let graph = resource_allocation_ring_graph(nodes, fanout);
        let control =
            AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
        let neighbors = simple_neighbors(&graph, &control, false).unwrap();
        let work = estimated_pairwise_source_work(&neighbors);
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
            let serial = resource_allocation_scores(&graph, &serial_ctl).unwrap();
            serial_ns = serial_ns.min(t0.elapsed().as_nanos());
            let serial_bits = serial.iter().copied().map(f64::to_bits).collect::<Vec<_>>();

            let t1 = Instant::now();
            let parallel = resource_allocation_scores(&graph, &parallel_ctl).unwrap();
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

fn resource_allocation_bits(output: &AlgorithmOutput) -> Vec<u64> {
    resource_allocation_output_scores(output)
        .into_iter()
        .map(f64::to_bits)
        .collect()
}

#[test]
fn resource_allocation_path_selection_respects_crossover_and_one_thread() {
    let no_pool = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(4),
        AlgorithmCancellation::default(),
    );
    assert_eq!(
        select_resource_allocation_path(&no_pool, 64, RESOURCE_ALLOCATION_PARALLEL_CROSSOVER_WORK),
        ResourceAllocationExecutionPath::Serial
    );
    let one = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(1),
        AlgorithmCancellation::default(),
    )
    .with_compute_pool(Arc::new(crate::ComputePool::new(1).unwrap()));
    assert_eq!(
        select_resource_allocation_path(&one, 64, RESOURCE_ALLOCATION_PARALLEL_CROSSOVER_WORK),
        ResourceAllocationExecutionPath::Serial
    );
    let small = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(4),
        AlgorithmCancellation::default(),
    )
    .with_compute_pool(Arc::new(crate::ComputePool::new(4).unwrap()));
    assert_eq!(
        select_resource_allocation_path(
            &small,
            64,
            RESOURCE_ALLOCATION_PARALLEL_CROSSOVER_WORK - 1
        ),
        ResourceAllocationExecutionPath::Serial
    );
    assert_eq!(
        select_resource_allocation_path(&small, 64, RESOURCE_ALLOCATION_PARALLEL_CROSSOVER_WORK),
        ResourceAllocationExecutionPath::Parallel {
            threads: 4,
            chunks: 4
        }
    );
}

#[test]
fn resource_allocation_parallel_preserves_boundary_bits() {
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
        let serial = execute_resource_allocation_with_pool(
            graph,
            1,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        for threads in [2_usize, 4, 8] {
            let parallel = execute_resource_allocation_with_pool(
                graph,
                threads,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap();
            assert_eq!(
                resource_allocation_bits(&parallel),
                resource_allocation_bits(&serial)
            );
            assert_eq!(parallel.rows(), serial.rows());
        }
    }
}

#[test]
fn resource_allocation_thread_matrix_matches_one_thread_bits_and_ordering() {
    let graph = dense_resource_allocation_graph(128);
    let control =
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
    let neighbors = simple_neighbors(&graph, &control, false).unwrap();
    assert!(
        estimated_pairwise_source_work(&neighbors) >= RESOURCE_ALLOCATION_PARALLEL_CROSSOVER_WORK
    );
    let serial = execute_resource_allocation_with_pool(
        &graph,
        1,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    let serial_rows = serial.rows();
    let serial_bits = resource_allocation_bits(&serial);
    for threads in [2_usize, 4, 8] {
        let parallel = execute_resource_allocation_with_pool(
            &graph,
            threads,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_eq!(parallel.schema, serial.schema);
        assert_eq!(parallel.rows(), serial_rows);
        assert_eq!(resource_allocation_bits(&parallel), serial_bits);
    }
}

#[test]
fn resource_allocation_parallel_limits_and_cancellation_return_structured() {
    let graph = dense_resource_allocation_graph(128);
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        execute_resource_allocation_with_pool(&graph, 4, AlgorithmLimits::default(), cancellation),
        Err(AlgorithmError::Cancelled)
    );
    assert!(matches!(
        execute_resource_allocation_with_pool(
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
        execute_resource_allocation_with_pool(
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
fn resource_allocation_worker_panic_returns_structured_error() {
    let pool = crate::ComputePool::new(2).unwrap();
    let error = run_resource_allocation_on_pool(&pool, || -> Result<(), AlgorithmError> {
        panic!("synthetic resource-allocation worker failure")
    })
    .unwrap_err();
    assert_eq!(
        error,
        AlgorithmError::Execution {
            message: "resource-allocation worker panicked".into()
        }
    );
}
