use super::super::fastgreedy::fastgreedy_communities_with_progress;
use super::super::tests::community_ids;
use super::super::*;
use super::*;

fn execute_girvan_newman(
    graph: &AdjacencyGraph,
    limits: AlgorithmLimits,
) -> Result<AlgorithmOutput, AlgorithmError> {
    execute_cluster(
        graph,
        Algorithm::Cluster(ClusterAlgorithm::GirvanNewman),
        limits,
    )
}

#[test]
fn girvan_newman_selects_the_deterministic_best_modularity_level() {
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
    let first = execute_girvan_newman(&graph, AlgorithmLimits::default()).unwrap();
    assert_eq!(community_ids(&first), [0, 0, 0, 1, 1, 1, 2]);
    assert_eq!(
        execute_girvan_newman(&graph, AlgorithmLimits::default()).unwrap(),
        first
    );
    for (boundary, expected) in [
        (AdjacencyGraph::with_test_edges(3, &[]), vec![0, 1, 2]),
        (AdjacencyGraph::default(), vec![]),
    ] {
        assert_eq!(
            community_ids(&execute_girvan_newman(&boundary, AlgorithmLimits::default()).unwrap()),
            expected
        );
    }

    let single_edge = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
    let control =
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
    let mut merges = 0;
    assert_eq!(
        fastgreedy_communities_with_progress(&single_edge, &control, || merges += 1).unwrap(),
        [0, 0]
    );
    assert_eq!(merges, 1, "the terminal candidate pass is not a merge");
}

#[test]
fn girvan_newman_uses_shared_controls_cancellation_and_rust_registration() {
    let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
    assert_eq!(
        execute_girvan_newman(
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
            Algorithm::Cluster(ClusterAlgorithm::GirvanNewman),
            &graph,
            &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
        ),
        Err(AlgorithmError::Cancelled)
    );
    let capability = registry
        .capabilities()
        .into_iter()
        .find(|entry| entry.algorithm == Algorithm::Cluster(ClusterAlgorithm::GirvanNewman))
        .unwrap();
    assert_eq!(capability.backend, "rust");
    assert_eq!(capability.dependency, BUILTIN_REVIEW);

    let cancellation = AlgorithmCancellation::default();
    let cancel = cancellation.clone();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let control = AlgorithmControl::new(AlgorithmLimits::default(), cancellation);
        let mut rendezvous = Some((started_tx, resume_rx));
        girvan_newman_communities_with_progress(&graph, &control, || {
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
