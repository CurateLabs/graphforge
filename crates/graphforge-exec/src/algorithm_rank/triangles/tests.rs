use super::super::tests::assert_scores_close;
use super::super::tests::hits_hub_scores;
use super::super::*;
use super::*;

fn triangle_thread_matrix_graph() -> AdjacencyGraph {
    let nodes = TRIANGLES_PARALLEL_CROSSOVER_NODES + 32;
    let mut edges = Vec::with_capacity(nodes * 4);
    for node in 0..nodes {
        let a = node as u64;
        let b = ((node + 1) % nodes) as u64;
        let c = ((node + 2) % nodes) as u64;
        edges.push((a, b));
        edges.push((b, c));
        edges.push((c, a));
        if node.is_multiple_of(17) {
            edges.push((a, b));
        }
        if node.is_multiple_of(23) {
            edges.push((a, a));
        }
    }
    AdjacencyGraph::with_test_edges(nodes as u64, &edges)
}

fn execute_triangles(
    graph: &AdjacencyGraph,
    limits: AlgorithmLimits,
    cancellation: AlgorithmCancellation,
) -> Result<AlgorithmOutput, AlgorithmError> {
    let mut registry = AlgorithmRegistry::default();
    register_rank_algorithms(&mut registry)?;
    registry.execute(
        Algorithm::Rank(RankAlgorithm::Triangles),
        graph,
        &AlgorithmControl::new(limits, cancellation),
    )
}

fn execute_triangles_with_pool(
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
    registry.execute(Algorithm::Rank(RankAlgorithm::Triangles), graph, &control)
}

fn triangle_output_scores(output: &AlgorithmOutput) -> Vec<f64> {
    hits_hub_scores(output)
}

fn triangle_bits(output: &AlgorithmOutput) -> Vec<u64> {
    triangle_output_scores(output)
        .into_iter()
        .map(f64::to_bits)
        .collect()
}

#[test]
fn triangles_count_overlapping_cliques_in_stable_node_order() {
    let graph = AdjacencyGraph::with_test_edges(
        6,
        &[(0, 1), (1, 2), (2, 0), (0, 2), (2, 3), (3, 0), (4, 5)],
    );
    let output = execute_triangles(
        &graph,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    assert_scores_close(
        &triangle_output_scores(&output),
        &[2.0, 1.0, 2.0, 1.0, 0.0, 0.0],
    );
    assert_eq!(
        output,
        execute_triangles(
            &graph,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap()
    );
}

#[test]
fn triangles_ignore_direction_multiplicity_and_self_loops() {
    let directed =
        AdjacencyGraph::with_test_edges(4, &[(0, 1), (0, 1), (1, 0), (1, 2), (2, 0), (0, 0)]);
    let reciprocal =
        AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0), (1, 2), (2, 1), (2, 0), (0, 2)]);
    for graph in [&directed, &reciprocal] {
        assert_scores_close(
            &triangle_output_scores(
                &execute_triangles(
                    graph,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[1.0, 1.0, 1.0, 0.0],
        );
    }
    assert!(
        execute_triangles(
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
fn triangles_use_shared_controls_and_dependency_metadata() {
    let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2), (2, 0)]);
    assert!(matches!(
        execute_triangles(
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
        execute_triangles(&graph, AlgorithmLimits::default(), cancellation),
        Err(AlgorithmError::Cancelled)
    );
    let mut registry = AlgorithmRegistry::default();
    register_rank_algorithms(&mut registry).unwrap();
    let capability = registry
        .capabilities()
        .into_iter()
        .find(|capability| capability.algorithm == Algorithm::Rank(RankAlgorithm::Triangles))
        .unwrap();
    assert_eq!(capability.dependency, BUILTIN_REVIEW);
    assert_eq!(capability.algorithm.as_str(), "triangles");
}

#[test]
fn triangles_path_selection_respects_crossover_pool_and_one_thread() {
    let no_pool = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(4),
        AlgorithmCancellation::default(),
    );
    assert_eq!(
        select_triangles_path(&no_pool, TRIANGLES_PARALLEL_CROSSOVER_NODES),
        TrianglesExecutionPath::Serial
    );

    let below = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(4),
        AlgorithmCancellation::default(),
    )
    .with_compute_pool(Arc::new(crate::ComputePool::new(4).unwrap()));
    assert_eq!(
        select_triangles_path(&below, TRIANGLES_PARALLEL_CROSSOVER_NODES - 1),
        TrianglesExecutionPath::Serial
    );

    let one = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(1),
        AlgorithmCancellation::default(),
    )
    .with_compute_pool(Arc::new(crate::ComputePool::new(1).unwrap()));
    assert_eq!(
        select_triangles_path(&one, TRIANGLES_PARALLEL_CROSSOVER_NODES),
        TrianglesExecutionPath::Serial
    );

    let parallel = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(4),
        AlgorithmCancellation::default(),
    )
    .with_compute_pool(Arc::new(crate::ComputePool::new(4).unwrap()));
    assert_eq!(
        select_triangles_path(&parallel, TRIANGLES_PARALLEL_CROSSOVER_NODES),
        TrianglesExecutionPath::Parallel {
            threads: 4,
            chunks: 4
        }
    );
}

#[test]
fn triangles_thread_matrix_matches_one_thread_fingerprints_and_ordering() {
    let graph = triangle_thread_matrix_graph();
    assert!(graph.node_ids().len() >= TRIANGLES_PARALLEL_CROSSOVER_NODES);

    let one_thread =
        execute_triangles_with_pool(&graph, 1, AlgorithmCancellation::default()).unwrap();
    let expected_rows = one_thread.rows();
    let expected_bits = triangle_bits(&one_thread);

    for threads in [1_usize, 2, 4, 8] {
        let output =
            execute_triangles_with_pool(&graph, threads, AlgorithmCancellation::default()).unwrap();
        assert_eq!(output.schema, one_thread.schema);
        assert_eq!(output.rows(), expected_rows);
        assert_eq!(triangle_bits(&output), expected_bits);
    }
}

#[test]
fn triangles_parallel_cancellation_returns_structured_cancelled() {
    let graph = AdjacencyGraph::with_test_edges(TRIANGLES_PARALLEL_CROSSOVER_NODES as u64, &[]);
    let control = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(4),
        AlgorithmCancellation::default(),
    )
    .with_compute_pool(Arc::new(crate::ComputePool::new(4).unwrap()));
    assert!(matches!(
        select_triangles_path(&control, graph.node_ids().len()),
        TrianglesExecutionPath::Parallel {
            threads: 4,
            chunks: 4
        }
    ));

    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        execute_triangles_with_pool(&graph, 4, cancellation),
        Err(AlgorithmError::Cancelled)
    );
}
