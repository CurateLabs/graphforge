use super::super::tests::community_ids;
use super::super::*;
use super::*;

fn execute_fastgreedy(
    graph: &AdjacencyGraph,
    limits: AlgorithmLimits,
) -> Result<AlgorithmOutput, AlgorithmError> {
    execute_cluster(
        graph,
        Algorithm::Cluster(ClusterAlgorithm::FastGreedy),
        limits,
    )
}

#[test]
fn fastgreedy_selects_the_deterministic_best_agglomerative_partition() {
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
    let first = execute_fastgreedy(&graph, AlgorithmLimits::default()).unwrap();
    assert_eq!(community_ids(&first), [0, 0, 0, 1, 1, 1, 2]);
    assert_eq!(
        execute_fastgreedy(&graph, AlgorithmLimits::default()).unwrap(),
        first
    );
    for (boundary, expected) in [
        (AdjacencyGraph::with_test_edges(3, &[]), vec![0, 1, 2]),
        (AdjacencyGraph::default(), vec![]),
    ] {
        assert_eq!(
            community_ids(&execute_fastgreedy(&boundary, AlgorithmLimits::default()).unwrap()),
            expected
        );
    }
}

#[test]
fn fastgreedy_updates_only_affected_candidates_without_stale_merges() {
    let edges: Vec<_> = (0..63).map(|node| (node, node + 1)).collect();
    let graph = AdjacencyGraph::with_test_edges(64, &edges);
    let control =
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
    let (weights, _) = normalized_communities(&graph, &control).unwrap();
    let mut merges = Vec::new();

    let result = fastgreedy_from_weights_with_updates(
        &weights,
        &control,
        || {},
        |left, right, updates| merges.push((left, right, updates)),
    )
    .unwrap();

    let mut live: BTreeSet<_> = (0..64).collect();
    for &(left, right, _) in &merges {
        assert!(live.contains(&left), "surviving community must be live");
        assert!(live.remove(&right), "absorbed community must be live");
    }
    assert_eq!(merges.len(), 63, "each legal path merge happens once");
    assert!(
        merges.iter().all(|&(_, _, updates)| updates <= 2),
        "path merges update only the surviving community's neighbors: {merges:?}"
    );
    let expected: Vec<_> = (0..64).map(|node| node / 8).collect();
    assert_eq!(result, expected);
}

#[test]
fn fastgreedy_uses_shared_controls_cancellation_and_rust_registration() {
    let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
    assert_eq!(
        execute_fastgreedy(
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
            Algorithm::Cluster(ClusterAlgorithm::FastGreedy),
            &graph,
            &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
        ),
        Err(AlgorithmError::Cancelled)
    );
    let capability = registry
        .capabilities()
        .into_iter()
        .find(|entry| entry.algorithm == Algorithm::Cluster(ClusterAlgorithm::FastGreedy))
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
        fastgreedy_from_weights(&invalid, &control, || {}),
        Err(execution("Fastgreedy modularity is not finite"))
    );

    let cancellation = AlgorithmCancellation::default();
    let cancel = cancellation.clone();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let control = AlgorithmControl::new(AlgorithmLimits::default(), cancellation);
        let mut rendezvous = Some((started_tx, resume_rx));
        fastgreedy_communities_with_progress(&graph, &control, || {
            if let Some((started, resume)) = rendezvous.take() {
                started.send(()).unwrap();
                resume.recv().unwrap();
            }
        })
    });
    started_rx.recv().unwrap();
    cancel.cancel();
    resume_tx.send(()).unwrap();
    assert_eq!(worker.join().unwrap(), Err(AlgorithmError::Cancelled));
}
