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

/// A directed graph whose every round does many 16,384-unit chunks of work: three
/// out-arcs per node, so a round visits `nodes` nodes and about `6 * nodes` votes.
fn generated_directed_arcs(nodes: u64) -> Vec<(u64, u64)> {
    (0..nodes)
        .flat_map(|node| {
            [
                (node, (node + 1) % nodes),
                (node, (node * 7 + 3) % nodes),
                (node, (node * 13 + 5) % nodes),
            ]
        })
        .collect()
}

/// Synchronous label propagation from its definition, sharing no code with the
/// kernel: each round every node takes the most frequent label among its
/// distinct out-neighbors and its distinct in-neighbors (a reciprocal pair votes
/// twice), the smallest label on a tie, and keeps its label when it has no vote.
fn reference_synchronous_labels(nodes: u64, arcs: &[(u64, u64)], rounds: u32) -> Vec<i64> {
    let nodes = usize::try_from(nodes).unwrap();
    let mut outbound = vec![BTreeSet::new(); nodes];
    let mut inbound = vec![BTreeSet::new(); nodes];
    for &(source, target) in arcs {
        let (source, target) = (
            usize::try_from(source).unwrap(),
            usize::try_from(target).unwrap(),
        );
        if source != target {
            outbound[source].insert(target);
            inbound[target].insert(source);
        }
    }
    let mut labels: Vec<i64> = (0..i64::try_from(nodes).unwrap()).collect();
    for _ in 0..rounds {
        labels = (0..nodes)
            .map(|node| {
                let mut counts = BTreeMap::<i64, usize>::new();
                for &voter in outbound[node].iter().chain(&inbound[node]) {
                    *counts.entry(labels[voter]).or_default() += 1;
                }
                counts.values().copied().max().map_or(labels[node], |most| {
                    *counts.iter().find(|(_, count)| **count == most).unwrap().0
                })
            })
            .collect();
    }
    labels
}

#[test]
fn synchronous_rounds_finish_when_work_alone_exceeds_the_iteration_limit() {
    const NODES: u64 = 30_000;
    const ROUNDS: u32 = 10;
    let arcs = generated_directed_arcs(NODES);
    let graph = AdjacencyGraph::with_test_directed_edges(NODES, &arcs);
    let options = graphforge_core::SynchronousLabelPropagationOptions {
        iterations: ROUNDS,
        initial_label_property: None,
    };
    let expected = reference_synchronous_labels(NODES, &arcs, ROUNDS);

    // A reference run under the default limit of 10,000.
    let reference = synchronous_labels(
        &graph,
        &options,
        &AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default()),
    )
    .unwrap();
    assert_eq!(reference, expected);

    // Each round is about 13 chunks of 16,384 work units, so ten rounds read as
    // over a hundred iterations if work were charged to the limit. The limit
    // counts rounds: exactly `ROUNDS` suffices and gives the same labels.
    let control = AlgorithmControl::new(
        AlgorithmLimits {
            iterations: u64::from(ROUNDS),
            ..AlgorithmLimits::default()
        },
        AlgorithmCancellation::default(),
    );
    assert_eq!(
        synchronous_labels(&graph, &options, &control).unwrap(),
        expected
    );
}

#[test]
fn the_iteration_limit_still_stops_synchronous_rounds_that_exceed_it() {
    let graph = AdjacencyGraph::with_test_directed_edges(4, &[(0, 1), (1, 2), (2, 3)]);
    let options = graphforge_core::SynchronousLabelPropagationOptions {
        iterations: 5,
        initial_label_property: None,
    };
    let limited = |iterations| {
        AlgorithmControl::new(
            AlgorithmLimits {
                iterations,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        )
    };
    // A tiny graph does almost no work, so only the round count can exceed 3.
    assert_eq!(
        synchronous_labels(&graph, &options, &limited(3)),
        Err(AlgorithmError::IterationLimit {
            observed: 4,
            limit: 3
        })
    );
    assert!(synchronous_labels(&graph, &options, &limited(5)).is_ok());
}

#[test]
fn asynchronous_propagation_finishes_when_work_alone_exceeds_the_iteration_limit() {
    // Disjoint 8-cliques: converges in a few rounds, with hundreds of chunks of work.
    const CLIQUES: u64 = 5_000;
    let arcs = (0..CLIQUES)
        .flat_map(|clique| {
            (0..8).flat_map(move |left| {
                (left + 1..8).map(move |right| (clique * 8 + left, clique * 8 + right))
            })
        })
        .collect::<Vec<_>>();
    let graph = AdjacencyGraph::with_test_edges(CLIQUES * 8, &arcs);
    let reference = execute_label_propagation(&graph, AlgorithmLimits::default()).unwrap();
    assert_eq!(community_ids(&reference).len(), 40_000);

    let limited = AlgorithmLimits {
        iterations: 25,
        ..AlgorithmLimits::default()
    };
    assert_eq!(
        execute_label_propagation(&graph, limited).unwrap(),
        reference
    );
}
