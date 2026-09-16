use super::super::tests::community_ids;
use super::super::*;
use super::*;

fn execute_infomap(
    graph: &AdjacencyGraph,
    limits: AlgorithmLimits,
) -> Result<AlgorithmOutput, AlgorithmError> {
    execute_cluster(graph, Algorithm::Cluster(ClusterAlgorithm::InfoMap), limits)
}

#[test]
fn infomap_flow_normalizes_topology_and_orders_weak_components() {
    let graph =
        AdjacencyGraph::with_test_directed_edges(5, &[(0, 0), (0, 1), (0, 1), (1, 0), (2, 3)]);
    let control =
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
    let flow = infomap_flow(&graph, &control).unwrap();

    assert!(flow.directed);
    assert_eq!(flow.outgoing[0], BTreeSet::from([1]));
    assert_eq!(flow.incident[3], BTreeSet::from([2]));
    assert_eq!(flow.components, [vec![0, 1], vec![2, 3], vec![4]]);
}

#[test]
fn infomap_stationary_flow_and_map_equation_are_hand_verifiable() {
    let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0), (1, 2), (2, 1)]);
    let control =
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
    let flow = infomap_flow(&graph, &control).unwrap();
    let component = &flow.components[0];
    let visits = infomap_stationary(component, &flow.outgoing, flow.directed, &control).unwrap();

    assert!(!flow.directed);
    assert_eq!(visits, [0.25, 0.5, 0.25]);
    let singleton =
        infomap_codelength(component, &flow.outgoing, &visits, false, &[0, 1, 2]).unwrap();
    let joined = infomap_codelength(component, &flow.outgoing, &visits, false, &[0, 0, 0]).unwrap();
    assert!(joined < singleton);
}

#[test]
fn infomap_directed_flow_is_deterministic_bounded_and_finite() {
    let graph = AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2)]);
    let control =
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
    let flow = infomap_flow(&graph, &control).unwrap();
    let first = infomap_stationary(&flow.components[0], &flow.outgoing, true, &control).unwrap();
    let second = infomap_stationary(&flow.components[0], &flow.outgoing, true, &control).unwrap();
    assert_eq!(first, second);
    assert!(first.iter().all(|value| value.is_finite() && *value > 0.0));
    assert!((first.iter().sum::<f64>() - 1.0).abs() <= 1e-12);

    let limited = AlgorithmControl::new(
        AlgorithmLimits {
            iterations: 0,
            ..AlgorithmLimits::default()
        },
        AlgorithmCancellation::default(),
    );
    assert!(matches!(
        infomap_stationary(&flow.components[0], &flow.outgoing, true, &limited),
        Err(AlgorithmError::IterationLimit { .. })
    ));
    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert!(matches!(
        infomap_flow(
            &graph,
            &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
        ),
        Err(AlgorithmError::Cancelled)
    ));
    assert_eq!(
        infomap_codelength(
            &flow.components[0],
            &flow.outgoing,
            &[f64::INFINITY, 0.0, 0.0],
            true,
            &[0, 1, 2],
        ),
        Err(execution("Infomap codelength is not finite"))
    );
}

#[test]
fn infomap_selects_a_stable_two_level_flow_partition() {
    let graph = AdjacencyGraph::with_test_edges(5, &[(0, 1), (1, 0), (2, 3), (3, 2)]);
    let first = execute_infomap(&graph, AlgorithmLimits::default()).unwrap();
    assert_eq!(community_ids(&first), [0, 0, 1, 1, 2]);
    assert_eq!(
        execute_infomap(&graph, AlgorithmLimits::default()).unwrap(),
        first
    );
    let directed = AdjacencyGraph::with_test_directed_edges(
        6,
        &[(0, 1), (1, 2), (2, 0), (2, 3), (3, 4), (4, 5), (5, 3)],
    );
    assert_eq!(
        community_ids(&execute_infomap(&directed, AlgorithmLimits::default()).unwrap()),
        [0, 0, 0, 1, 1, 1]
    );
    for (boundary, expected) in [
        (AdjacencyGraph::with_test_edges(3, &[]), vec![0, 1, 2]),
        (AdjacencyGraph::default(), vec![]),
    ] {
        assert_eq!(
            community_ids(&execute_infomap(&boundary, AlgorithmLimits::default()).unwrap()),
            expected
        );
    }
}

#[test]
fn infomap_uses_shared_controls_and_single_rust_registration() {
    let graph = AdjacencyGraph::with_test_directed_edges(3, &[(0, 1), (1, 2)]);
    assert!(matches!(
        execute_infomap(
            &graph,
            AlgorithmLimits {
                iterations: 0,
                ..AlgorithmLimits::default()
            }
        ),
        Err(AlgorithmError::IterationLimit { .. })
    ));
    let mut registry = AlgorithmRegistry::default();
    register_cluster_algorithms(&mut registry).unwrap();
    let setup = AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
    let flow = infomap_flow(&graph, &setup).unwrap();
    let visits = infomap_stationary(&flow.components[0], &flow.outgoing, true, &setup).unwrap();
    let search = InfomapSearch {
        component: &flow.components[0],
        flow: &flow,
        visits: &visits,
    };
    let cancellation = AlgorithmCancellation::default();
    let cancel = cancellation.clone();
    let mut assignment = vec![0, 1, 2];
    assert_eq!(
        search.node_sweep(
            &mut assignment,
            &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            || cancel.cancel(),
        ),
        Err(AlgorithmError::Cancelled)
    );
    let capability = registry
        .capabilities()
        .into_iter()
        .find(|entry| entry.algorithm == Algorithm::Cluster(ClusterAlgorithm::InfoMap))
        .unwrap();
    assert_eq!(capability.backend, "rust");
    assert_eq!(capability.dependency, BUILTIN_REVIEW);
}
