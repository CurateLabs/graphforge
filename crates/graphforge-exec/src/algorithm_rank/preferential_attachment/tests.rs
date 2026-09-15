use super::super::tests::assert_scores_close;
use super::super::tests::hits_hub_scores;
use super::super::*;
use super::*;

fn execute_preferential_attachment(
    graph: &AdjacencyGraph,
    limits: AlgorithmLimits,
    cancellation: AlgorithmCancellation,
) -> Result<AlgorithmOutput, AlgorithmError> {
    let mut registry = AlgorithmRegistry::default();
    register_rank_algorithms(&mut registry)?;
    registry.execute(
        Algorithm::Rank(RankAlgorithm::PreferentialAttachment),
        graph,
        &AlgorithmControl::new(limits, cancellation),
    )
}

fn execute_preferential_attachment_with_pool(
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
        Algorithm::Rank(RankAlgorithm::PreferentialAttachment),
        graph,
        &control,
    )
}

fn preferential_attachment_bits(output: &AlgorithmOutput) -> Vec<u64> {
    preferential_attachment_output_scores(output)
        .into_iter()
        .map(f64::to_bits)
        .collect()
}

fn preferential_attachment_ring_graph(nodes: usize, fanout: usize) -> AdjacencyGraph {
    let edges = (0..nodes)
        .flat_map(|node| (1..=fanout).map(move |hop| (node as u64, ((node + hop) % nodes) as u64)))
        .collect::<Vec<_>>();
    AdjacencyGraph::with_test_edges(nodes as u64, &edges)
}

fn dense_preferential_attachment_graph(nodes: usize) -> AdjacencyGraph {
    let fanout = ((PREFERENTIAL_ATTACHMENT_PARALLEL_CROSSOVER_WORK as usize) / nodes.max(1))
        .saturating_add(2)
        .min(nodes.saturating_sub(1).max(1));
    preferential_attachment_ring_graph(nodes, fanout)
}

fn preferential_attachment_output_scores(output: &AlgorithmOutput) -> Vec<f64> {
    hits_hub_scores(output)
}

#[test]
fn preferential_attachment_aggregates_missing_directed_links() {
    let graph = AdjacencyGraph::with_test_edges(
        5,
        &[(0, 1), (0, 1), (0, 2), (0, 0), (1, 2), (2, 0), (3, 2)],
    );
    let output = execute_preferential_attachment(
        &graph,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    assert_scores_close(
        &preferential_attachment_output_scores(&output),
        &[2.0, 3.0, 2.0, 3.0, 0.0],
    );
    assert_eq!(
        output,
        execute_preferential_attachment(
            &graph,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap()
    );
}

#[test]
fn preferential_attachment_obeys_undirected_and_boundary_contracts() {
    let undirected = AdjacencyGraph::with_test_edges(
        5,
        &[
            (0, 1),
            (1, 0),
            (0, 2),
            (2, 0),
            (1, 2),
            (2, 1),
            (2, 3),
            (3, 2),
        ],
    );
    assert_scores_close(
        &preferential_attachment_output_scores(
            &execute_preferential_attachment(
                &undirected,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap(),
        ),
        &[2.0, 2.0, 0.0, 4.0, 0.0],
    );

    let disconnected = AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0), (2, 3), (3, 2)]);
    assert_scores_close(
        &preferential_attachment_output_scores(
            &execute_preferential_attachment(
                &disconnected,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap(),
        ),
        &[2.0, 2.0, 2.0, 2.0],
    );
    let complete =
        AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0), (0, 2), (2, 0), (1, 2), (2, 1)]);
    assert!(
        preferential_attachment_output_scores(
            &execute_preferential_attachment(
                &complete,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap(),
        )
        .into_iter()
        .all(|score| score == 0.0)
    );
    assert!(
        preferential_attachment_output_scores(
            &execute_preferential_attachment(
                &AdjacencyGraph::with_test_counts(3, 0),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap(),
        )
        .into_iter()
        .all(|score| score == 0.0)
    );
    assert!(
        execute_preferential_attachment(
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
fn preferential_attachment_uses_shared_controls_and_dependency_metadata() {
    let graph = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
    assert!(matches!(
        execute_preferential_attachment(
            &graph,
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
        execute_preferential_attachment(&graph, AlgorithmLimits::default(), cancellation),
        Err(AlgorithmError::Cancelled)
    );
    assert!(matches!(
        exact_u64_as_f64((1_u64 << 53) + 1, "preferential-attachment score"),
        Err(AlgorithmError::Execution { .. })
    ));
    let mut registry = AlgorithmRegistry::default();
    register_rank_algorithms(&mut registry).unwrap();
    let capability = registry
        .capabilities()
        .into_iter()
        .find(|capability| {
            capability.algorithm == Algorithm::Rank(RankAlgorithm::PreferentialAttachment)
        })
        .unwrap();
    assert_eq!(capability.dependency, BUILTIN_REVIEW);
    assert_eq!(capability.algorithm.as_str(), "preferential_attachment");
}

#[test]
fn preferential_attachment_path_selection_respects_crossover_and_one_thread() {
    let no_pool = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(4),
        AlgorithmCancellation::default(),
    );
    assert_eq!(
        select_preferential_attachment_path(
            &no_pool,
            64,
            PREFERENTIAL_ATTACHMENT_PARALLEL_CROSSOVER_WORK
        ),
        PreferentialAttachmentExecutionPath::Serial
    );
    let one = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(1),
        AlgorithmCancellation::default(),
    )
    .with_compute_pool(Arc::new(crate::ComputePool::new(1).unwrap()));
    assert_eq!(
        select_preferential_attachment_path(
            &one,
            64,
            PREFERENTIAL_ATTACHMENT_PARALLEL_CROSSOVER_WORK
        ),
        PreferentialAttachmentExecutionPath::Serial
    );
    let parallel = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(4),
        AlgorithmCancellation::default(),
    )
    .with_compute_pool(Arc::new(crate::ComputePool::new(4).unwrap()));
    assert_eq!(
        select_preferential_attachment_path(
            &parallel,
            64,
            PREFERENTIAL_ATTACHMENT_PARALLEL_CROSSOVER_WORK - 1
        ),
        PreferentialAttachmentExecutionPath::Serial
    );
    assert_eq!(
        select_preferential_attachment_path(
            &parallel,
            64,
            PREFERENTIAL_ATTACHMENT_PARALLEL_CROSSOVER_WORK
        ),
        PreferentialAttachmentExecutionPath::Parallel {
            threads: 4,
            chunks: 4
        }
    );
}

#[test]
fn preferential_attachment_thread_matrix_matches_one_thread_bits_and_ordering() {
    let graph = dense_preferential_attachment_graph(4_096);
    let neighbors = simple_neighbors(
        &graph,
        &AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default()),
        false,
    )
    .unwrap();
    assert!(
        estimated_preferential_attachment_work(&neighbors)
            >= PREFERENTIAL_ATTACHMENT_PARALLEL_CROSSOVER_WORK
    );
    let serial = execute_preferential_attachment_with_pool(
        &graph,
        1,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    let serial_bits = preferential_attachment_bits(&serial);
    let serial_rows = serial.rows();
    for threads in [2_usize, 4, 8] {
        let parallel = execute_preferential_attachment_with_pool(
            &graph,
            threads,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_eq!(parallel.schema, serial.schema);
        assert_eq!(parallel.rows(), serial_rows);
        assert_eq!(preferential_attachment_bits(&parallel), serial_bits);
    }
}

#[test]
fn preferential_attachment_parallel_preserves_boundary_bits() {
    let multigraph = AdjacencyGraph::with_test_edges(
        5,
        &[(0, 1), (0, 1), (0, 2), (0, 0), (1, 2), (2, 0), (3, 2)],
    );
    let disconnected = AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0), (2, 3), (3, 2)]);
    let complete =
        AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0), (0, 2), (2, 0), (1, 2), (2, 1)]);
    let edgeless = AdjacencyGraph::with_test_counts(3, 0);
    let empty = AdjacencyGraph::default();
    let single = AdjacencyGraph::with_test_edges(1, &[]);
    for graph in [
        &multigraph,
        &disconnected,
        &complete,
        &edgeless,
        &empty,
        &single,
    ] {
        let serial = execute_preferential_attachment_with_pool(
            graph,
            1,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        for threads in [2_usize, 4, 8] {
            let parallel = execute_preferential_attachment_with_pool(
                graph,
                threads,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap();
            assert_eq!(
                preferential_attachment_bits(&parallel),
                preferential_attachment_bits(&serial)
            );
            assert_eq!(parallel.rows(), serial.rows());
        }
    }
}

#[test]
fn preferential_attachment_parallel_limits_and_cancellation_return_structured() {
    let graph = dense_preferential_attachment_graph(4_096);
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        execute_preferential_attachment_with_pool(
            &graph,
            4,
            AlgorithmLimits::default(),
            cancellation
        ),
        Err(AlgorithmError::Cancelled)
    );
    assert!(matches!(
        execute_preferential_attachment_with_pool(
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
        execute_preferential_attachment_with_pool(
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
fn preferential_attachment_worker_panic_returns_structured_error() {
    let pool = crate::ComputePool::new(2).unwrap();
    let error = run_preferential_attachment_on_pool(&pool, || -> Result<(), AlgorithmError> {
        panic!("synthetic preferential-attachment worker failure")
    })
    .unwrap_err();
    assert_eq!(
        error,
        AlgorithmError::Execution {
            message: "preferential-attachment worker panicked".into()
        }
    );
}

#[test]
fn preferential_attachment_source_chunks_cover_canonical_ranges() {
    assert_eq!(source_chunks(0, 4), Vec::<(usize, usize)>::new());
    assert_eq!(source_chunks(5, 1), vec![(0, 5)]);
    assert_eq!(source_chunks(5, 2), vec![(0, 3), (3, 5)]);
    assert_eq!(source_chunks(8, 4), vec![(0, 2), (2, 4), (4, 6), (6, 8)]);
    assert_eq!(source_chunks(3, 8), vec![(0, 1), (1, 2), (2, 3)]);
}

#[test]
#[ignore = "manual crossover measurement; run with --ignored --nocapture"]
fn measure_preferential_attachment_parallel_crossover() {
    use std::time::Instant;

    for (nodes, fanout) in [
        (1_024_usize, 16_usize),
        (2_048, 32),
        (4_096, 64),
        (8_192, 128),
        (16_384, 128),
    ] {
        let graph = preferential_attachment_ring_graph(nodes, fanout);
        let control =
            AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
        let neighbors = simple_neighbors(&graph, &control, false).unwrap();
        let work = estimated_preferential_attachment_work(&neighbors);
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
            let serial = preferential_attachment_scores(&graph, &serial_ctl).unwrap();
            serial_ns = serial_ns.min(t0.elapsed().as_nanos());
            let serial_bits = serial.iter().copied().map(f64::to_bits).collect::<Vec<_>>();

            let t1 = Instant::now();
            let parallel = preferential_attachment_scores(&graph, &parallel_ctl).unwrap();
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
