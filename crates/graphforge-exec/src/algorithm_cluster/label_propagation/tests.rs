use super::super::tests::community_ids;
use super::super::*;
use super::*;

fn execute_label_propagation(
    graph: &AdjacencyGraph,
    limits: AlgorithmLimits,
) -> Result<AlgorithmOutput, AlgorithmError> {
    execute_cluster(
        graph,
        Algorithm::Cluster(ClusterAlgorithm::LabelPropagation),
        limits,
    )
}

#[test]
fn label_propagation_is_deterministic_and_normalizes_boundaries() {
    let simple =
        AdjacencyGraph::with_test_edges(7, &[(0, 1), (1, 2), (2, 0), (3, 4), (4, 5), (5, 3)]);
    let noisy = AdjacencyGraph::with_test_edges(
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
            (5, 5),
        ],
    );
    let first = execute_label_propagation(&simple, AlgorithmLimits::default()).unwrap();
    assert_eq!(community_ids(&first), [0, 0, 0, 1, 1, 1, 2]);
    assert_eq!(
        execute_label_propagation(&simple, AlgorithmLimits::default()).unwrap(),
        first
    );
    assert_eq!(
        community_ids(&execute_label_propagation(&noisy, AlgorithmLimits::default()).unwrap()),
        [0, 0, 0, 1, 1, 1, 2]
    );
    assert_eq!(
        community_ids(
            &execute_label_propagation(
                &AdjacencyGraph::with_test_edges(3, &[]),
                AlgorithmLimits::default(),
            )
            .unwrap()
        ),
        [0, 1, 2]
    );
    assert!(
        execute_label_propagation(&AdjacencyGraph::default(), AlgorithmLimits::default())
            .unwrap()
            .rows()
            .is_empty()
    );
}

#[test]
fn label_propagation_uses_shared_controls_and_rust_registration() {
    let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
    assert_eq!(
        execute_label_propagation(
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
            Algorithm::Cluster(ClusterAlgorithm::LabelPropagation),
            &graph,
            &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
        ),
        Err(AlgorithmError::Cancelled)
    );
    let capability = registry
        .capabilities()
        .into_iter()
        .find(|entry| entry.algorithm == Algorithm::Cluster(ClusterAlgorithm::LabelPropagation))
        .unwrap();
    assert_eq!(capability.backend, "rust");
    assert_eq!(capability.dependency, BUILTIN_REVIEW);
}

#[test]
fn label_propagation_observes_cancellation_after_propagation_starts() {
    let graph = AdjacencyGraph::with_test_counts(2, 500_000);
    let cancellation = AlgorithmCancellation::default();
    let cancel = cancellation.clone();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let control = AlgorithmControl::new(AlgorithmLimits::default(), cancellation);
        let mut rendezvous = Some((started_tx, resume_rx));
        result_tx
            .send(label_propagation_communities_with_progress(
                &graph,
                &control,
                || {
                    if let Some((started, resume)) = rendezvous.take() {
                        started.send(()).unwrap();
                        resume.recv().unwrap();
                    }
                },
            ))
            .unwrap();
    });
    started_rx.recv().unwrap();
    cancel.cancel();
    resume_tx.send(()).unwrap();
    assert_eq!(result_rx.recv().unwrap(), Err(AlgorithmError::Cancelled));
    worker.join().unwrap();
}

#[test]
fn synchronous_rounds_preserve_bipartite_oscillation_and_isolates() {
    let graph = AdjacencyGraph::with_test_directed_edges(3, &[(0, 1)]);
    let control =
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
    for (iterations, expected) in [
        (0, vec![0, 1, 2]),
        (1, vec![1, 0, 2]),
        (2, vec![0, 1, 2]),
        (3, vec![1, 0, 2]),
    ] {
        let options = graphforge_core::SynchronousLabelPropagationOptions {
            iterations,
            initial_label_property: None,
        };
        assert_eq!(
            synchronous_labels(&graph, &options, &control).unwrap(),
            expected
        );
    }
}

#[test]
fn synchronous_directed_votes_count_reciprocal_neighbors_twice() {
    let graph = AdjacencyGraph::with_test_directed_edges(4, &[(0, 1), (0, 2), (2, 0)]);
    let options = graphforge_core::SynchronousLabelPropagationOptions {
        iterations: 1,
        initial_label_property: None,
    };
    let control =
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
    assert_eq!(
        synchronous_labels(&graph, &options, &control).unwrap(),
        [2, 0, 0, 3]
    );
    let tied = AdjacencyGraph::with_test_directed_edges(3, &[(0, 2), (0, 1)]);
    assert_eq!(
        synchronous_labels(&tied, &options, &control).unwrap(),
        [1, 0, 0]
    );
}

#[test]
fn synchronous_mode_preserves_cancellation_and_work_limits() {
    let graph = AdjacencyGraph::with_test_directed_edges(2, &[(0, 1)]);
    let options = graphforge_core::SynchronousLabelPropagationOptions {
        iterations: 1,
        initial_label_property: None,
    };
    let cancelled = AlgorithmCancellation::default();
    cancelled.cancel();
    assert_eq!(
        synchronous_labels(
            &graph,
            &options,
            &AlgorithmControl::new(AlgorithmLimits::default(), cancelled)
        ),
        Err(AlgorithmError::Cancelled)
    );
    assert!(matches!(
        synchronous_labels(
            &graph,
            &options,
            &AlgorithmControl::new(
                AlgorithmLimits {
                    iterations: 0,
                    ..Default::default()
                },
                AlgorithmCancellation::default()
            )
        ),
        Err(AlgorithmError::IterationLimit { .. })
    ));
}
