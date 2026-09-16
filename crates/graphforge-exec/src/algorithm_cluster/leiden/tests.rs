use super::super::tests::community_ids;
use super::super::tests::execute_louvain;
use super::super::*;

fn execute_leiden(
    graph: &AdjacencyGraph,
    limits: AlgorithmLimits,
) -> Result<AlgorithmOutput, AlgorithmError> {
    execute_cluster(graph, Algorithm::Cluster(ClusterAlgorithm::Leiden), limits)
}

fn execute_leiden_with_threads(
    graph: &AdjacencyGraph,
    threads: usize,
    limits: AlgorithmLimits,
    cancellation: AlgorithmCancellation,
) -> Result<AlgorithmOutput, AlgorithmError> {
    let mut registry = AlgorithmRegistry::default();
    register_cluster_algorithms(&mut registry)?;
    let control = AlgorithmControl::new(limits.with_compute_threads(threads), cancellation)
        .with_compute_pool(Arc::new(crate::ComputePool::new(threads).unwrap()));
    registry.execute(
        Algorithm::Cluster(ClusterAlgorithm::Leiden),
        graph,
        &control,
    )
}

#[test]
fn leiden_serial_disposition_holds_across_thread_budgets() {
    let graph = AdjacencyGraph::with_test_edges(
        8,
        &[
            (0, 4),
            (0, 6),
            (1, 2),
            (1, 5),
            (1, 6),
            (2, 6),
            (3, 6),
            (4, 6),
            (5, 6),
        ],
    );
    let control = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(8),
        AlgorithmCancellation::default(),
    )
    .with_compute_pool(Arc::new(crate::ComputePool::new(8).unwrap()));
    assert_eq!(
        select_leiden_path(&control, graph.node_ids().len(), graph.edge_entry_count()),
        LeidenExecutionPath::SerialRefinement
    );

    let oracle = execute_leiden_with_threads(
        &graph,
        1,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    for threads in [2_usize, 4, 8] {
        let output = execute_leiden_with_threads(
            &graph,
            threads,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_eq!(output.schema, oracle.schema);
        assert_eq!(output.rows(), oracle.rows());
    }
}

#[test]
fn leiden_serial_path_preserves_limits_cancellation_and_arrow_shaping() {
    let graph = AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 2), (2, 3)]);
    assert!(matches!(
        execute_leiden_with_threads(
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

    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert_eq!(
        execute_leiden_with_threads(&graph, 4, AlgorithmLimits::default(), cancellation),
        Err(AlgorithmError::Cancelled)
    );

    let shaped = execute_leiden_with_threads(
        &graph,
        4,
        AlgorithmLimits::default().with_batch_size(2),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    assert_eq!(shaped.num_rows(), 4);
    assert_eq!(shaped.record_batch().num_rows(), 4);
    assert!(shaped.internal_batch_count > 1);
    assert!(shaped.peak_builder_rows <= 2);
}

#[test]
fn leiden_refines_a_hand_verifiable_partition_deterministically() {
    let graph = AdjacencyGraph::with_test_edges(
        8,
        &[
            (0, 4),
            (0, 6),
            (1, 2),
            (1, 5),
            (1, 6),
            (2, 6),
            (3, 6),
            (4, 6),
            (5, 6),
        ],
    );
    let first = execute_leiden(&graph, AlgorithmLimits::default()).unwrap();
    // Leiden refines Louvain's partition into the connected sets
    // {0,3,4,6}, {1,2,5}, and the isolate {7}.
    assert_eq!(community_ids(&first), [0, 1, 1, 0, 0, 1, 0, 2]);
    assert_ne!(
        community_ids(&execute_louvain(&graph, AlgorithmLimits::default()).unwrap()),
        community_ids(&first)
    );
    assert_eq!(
        execute_leiden(&graph, AlgorithmLimits::default()).unwrap(),
        first
    );
}

#[test]
fn leiden_normalizes_boundaries_and_uses_shared_controls() {
    let graph = AdjacencyGraph::with_test_edges(
        7,
        &[
            (0, 1),
            (1, 0),
            (0, 1),
            (1, 2),
            (2, 0),
            (0, 0),
            (3, 4),
            (4, 5),
            (5, 3),
        ],
    );
    assert_eq!(
        community_ids(&execute_leiden(&graph, AlgorithmLimits::default()).unwrap()),
        [0, 0, 0, 1, 1, 1, 2]
    );
    assert!(
        execute_leiden(&AdjacencyGraph::default(), AlgorithmLimits::default())
            .unwrap()
            .rows()
            .is_empty()
    );
    assert_eq!(
        execute_leiden(
            &graph,
            AlgorithmLimits {
                iterations: 0,
                ..AlgorithmLimits::default()
            }
        ),
        Err(AlgorithmError::IterationLimit {
            observed: 1,
            limit: 0
        })
    );
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    let mut registry = AlgorithmRegistry::default();
    register_cluster_algorithms(&mut registry).unwrap();
    assert_eq!(
        registry.execute(
            Algorithm::Cluster(ClusterAlgorithm::Leiden),
            &graph,
            &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
        ),
        Err(AlgorithmError::Cancelled)
    );
    let capability = registry
        .capabilities()
        .into_iter()
        .find(|entry| entry.algorithm == Algorithm::Cluster(ClusterAlgorithm::Leiden))
        .unwrap();
    assert_eq!(capability.backend, "rust");
    assert_eq!(capability.dependency, BUILTIN_REVIEW);
}
