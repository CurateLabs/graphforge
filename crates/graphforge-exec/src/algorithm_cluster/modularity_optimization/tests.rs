use super::super::tests::community_ids;
use super::super::*;
use super::*;

fn execute_modularity_optimization(
    graph: &AdjacencyGraph,
    limits: AlgorithmLimits,
) -> Result<AlgorithmOutput, AlgorithmError> {
    execute_cluster(
        graph,
        Algorithm::Cluster(ClusterAlgorithm::ModularityOptimization),
        limits,
    )
}

#[test]
fn modularity_optimization_is_deterministic_single_level_local_moving() {
    let graph = AdjacencyGraph::with_test_edges(
        7,
        &[
            (0, 1),
            (1, 0),
            (0, 1),
            (1, 2),
            (2, 0),
            (2, 3),
            (3, 4),
            (4, 5),
            (5, 3),
            (5, 5),
        ],
    );
    let first = execute_modularity_optimization(&graph, AlgorithmLimits::default()).unwrap();
    assert_eq!(community_ids(&first), [0, 0, 0, 1, 1, 1, 2]);
    assert_eq!(
        execute_modularity_optimization(&graph, AlgorithmLimits::default()).unwrap(),
        first
    );

    let control =
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
    let (weights, _) = normalized_communities(&graph, &control).unwrap();
    assert_eq!(
        modularity_optimization_communities(&graph, &control).unwrap(),
        local_moves_from(&weights, None, "Modularity optimization", &control).unwrap()
    );
    for (boundary, expected) in [
        (AdjacencyGraph::with_test_edges(3, &[]), vec![0, 1, 2]),
        (AdjacencyGraph::default(), vec![]),
    ] {
        assert_eq!(
            community_ids(
                &execute_modularity_optimization(&boundary, AlgorithmLimits::default()).unwrap()
            ),
            expected
        );
    }
}

#[test]
fn modularity_optimization_uses_shared_controls_and_rust_registration() {
    let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
    assert_eq!(
        execute_modularity_optimization(
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
            Algorithm::Cluster(ClusterAlgorithm::ModularityOptimization),
            &graph,
            &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
        ),
        Err(AlgorithmError::Cancelled)
    );
    let capability = registry
        .capabilities()
        .into_iter()
        .find(|entry| {
            entry.algorithm == Algorithm::Cluster(ClusterAlgorithm::ModularityOptimization)
        })
        .unwrap();
    assert_eq!(capability.backend, "rust");
    assert_eq!(capability.dependency, BUILTIN_REVIEW);

    let invalid = vec![
        BTreeMap::from([(1, f64::INFINITY)]),
        BTreeMap::from([(0, f64::INFINITY)]),
    ];
    let control =
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
    assert_eq!(
        local_moves_from(&invalid, None, "Modularity optimization", &control),
        Err(execution(
            "Modularity optimization total edge weight is not finite"
        ))
    );

    let graph = AdjacencyGraph::with_test_counts(2, 500_000);
    let cancellation = AlgorithmCancellation::default();
    let cancel = cancellation.clone();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        modularity_optimization_communities(
            &graph,
            &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
        )
    });
    started_rx.recv().unwrap();
    std::thread::sleep(std::time::Duration::from_millis(2));
    cancel.cancel();
    assert_eq!(worker.join().unwrap(), Err(AlgorithmError::Cancelled));
}
