use super::super::tests::community_ids;
use super::super::tests::execute_louvain;
use super::super::*;
use super::*;

fn execute_louvain_with_threads(
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
        Algorithm::Cluster(ClusterAlgorithm::Louvain),
        graph,
        &control,
    )
}

#[test]
fn louvain_serial_disposition_holds_across_thread_budgets() {
    let graph = AdjacencyGraph::with_test_edges(
        8,
        &[
            (0, 1),
            (1, 2),
            (2, 0),
            (3, 4),
            (4, 5),
            (5, 3),
            (2, 3),
            (6, 7),
        ],
    );
    let control = AlgorithmControl::new(
        AlgorithmLimits::default().with_compute_threads(8),
        AlgorithmCancellation::default(),
    )
    .with_compute_pool(Arc::new(crate::ComputePool::new(8).unwrap()));
    assert_eq!(
        select_louvain_path(&control, graph.node_ids().len(), graph.edge_entry_count()),
        LouvainExecutionPath::SerialLocalMoves
    );

    let oracle = execute_louvain_with_threads(
        &graph,
        1,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    for threads in [2_usize, 4, 8] {
        let output = execute_louvain_with_threads(
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
fn louvain_serial_path_preserves_limits_cancellation_and_arrow_shaping() {
    let graph = AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 2), (2, 3)]);
    assert!(matches!(
        execute_louvain_with_threads(
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
        execute_louvain_with_threads(&graph, 4, AlgorithmLimits::default(), cancellation),
        Err(AlgorithmError::Cancelled)
    );

    let shaped = execute_louvain_with_threads(
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
fn louvain_finds_stable_multilevel_communities() {
    let graph = AdjacencyGraph::with_test_edges(
        6,
        &[(0, 1), (1, 2), (2, 0), (2, 3), (3, 4), (4, 5), (5, 3)],
    );
    let first = execute_louvain(&graph, AlgorithmLimits::default()).unwrap();
    assert_eq!(community_ids(&first), [0, 0, 0, 1, 1, 1]);
    assert_eq!(
        execute_louvain(&graph, AlgorithmLimits::default()).unwrap(),
        first
    );
    assert_eq!(
        first.schema,
        Algorithm::Cluster(ClusterAlgorithm::Louvain).result_schema()
    );
    assert_eq!(
        first.rows()[0][0],
        AlgorithmValue::Uuid(0_u128.to_be_bytes())
    );
}

#[test]
fn louvain_normalizes_multigraphs_and_retains_boundaries() {
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
    let expected = [0, 0, 0, 1, 1, 1, 2];
    assert_eq!(
        community_ids(&execute_louvain(&simple, AlgorithmLimits::default()).unwrap()),
        expected
    );
    assert_eq!(
        community_ids(&execute_louvain(&noisy, AlgorithmLimits::default()).unwrap()),
        expected
    );
    assert_eq!(
        community_ids(
            &execute_louvain(
                &AdjacencyGraph::with_test_edges(3, &[]),
                AlgorithmLimits::default(),
            )
            .unwrap()
        ),
        [0, 1, 2]
    );
    assert!(
        execute_louvain(&AdjacencyGraph::default(), AlgorithmLimits::default())
            .unwrap()
            .rows()
            .is_empty()
    );
}

#[test]
fn louvain_uses_shared_controls_and_single_rust_registration() {
    let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
    assert_eq!(
        execute_louvain(
            &graph,
            AlgorithmLimits {
                iterations: 0,
                ..AlgorithmLimits::default()
            },
        ),
        Err(AlgorithmError::IterationLimit {
            observed: 1,
            limit: 0,
        })
    );
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    let mut registry = AlgorithmRegistry::default();
    register_cluster_algorithms(&mut registry).unwrap();
    assert_eq!(
        registry.execute(
            Algorithm::Cluster(ClusterAlgorithm::Louvain),
            &graph,
            &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
        ),
        Err(AlgorithmError::Cancelled)
    );
    let capabilities = registry.capabilities();
    assert_eq!(capabilities.len(), ClusterAlgorithm::ALL.len());
    let capability = capabilities
        .into_iter()
        .find(|entry| entry.algorithm == Algorithm::Cluster(ClusterAlgorithm::Louvain))
        .unwrap();
    assert_eq!(capability.backend, "rust");
    assert_eq!(capability.dependency, BUILTIN_REVIEW);
}

#[test]
fn louvain_observes_cancellation_during_high_degree_work() {
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
            .send(louvain_communities_with_progress(
                &graph,
                &control,
                |observed| {
                    if let Some((started, resume)) = rendezvous.take() {
                        started.send(observed).unwrap();
                        resume.recv().unwrap();
                    }
                },
            ))
            .unwrap();
    });
    // Pause after real adjacency work, immediately before the next existing
    // cancellation checkpoint. No scheduler-speed assumption is needed.
    assert_eq!(started_rx.recv().unwrap(), 16_384);
    assert!(
        matches!(
            result_rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ),
        "execution finished before cancellation"
    );
    cancel.cancel();
    resume_tx.send(()).unwrap();
    assert_eq!(result_rx.recv().unwrap(), Err(AlgorithmError::Cancelled));
    worker.join().unwrap();
}
