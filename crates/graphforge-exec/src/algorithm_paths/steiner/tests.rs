use super::super::tests::uuid;
use super::super::tests::value;
use super::super::*;
use super::*;

#[test]
fn source_and_steiner_fields_follow_closed_catalog_policy() {
    for by in [PathAlgorithm::Bfs, PathAlgorithm::RandomWalk] {
        assert!(matches!(
            validate_path_options(
                None,
                None,
                &PathsOptions {
                    by,
                    ..PathsOptions::default()
                },
            ),
            Err(GfError::Validation(message))
                if message == format!("{by} requires a source selector")
        ));
    }
    for options in [
        PathsOptions {
            by: PathAlgorithm::Bfs,
            terminal_uuids: vec![uuid(1)],
            ..PathsOptions::default()
        },
        PathsOptions {
            by: PathAlgorithm::Bfs,
            prize_property: Some("prize".into()),
            ..PathsOptions::default()
        },
    ] {
        assert!(matches!(
            validate_path_options(Some(uuid(0)), None, &options),
            Err(GfError::Validation(_))
        ));
    }
    for by in [
        PathAlgorithm::MinSteinerTree,
        PathAlgorithm::PrizeCollectingSteinerTree,
    ] {
        assert!(
            validate_path_options(
                None,
                None,
                &PathsOptions {
                    by,
                    ..PathsOptions::default()
                },
            )
            .is_ok()
        );
    }
}

#[test]
fn minimum_steiner_dispatch_preserves_atomic_shared_controls() {
    let graph = AdjacencyGraph::with_test_undirected_multigraph(
        4,
        &[(9, 0, 1), (8, 1, 2), (7, 2, 3), (6, 0, 3)],
    );
    let output = execute_min_steiner(
        &graph,
        &[0, 2],
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    assert_eq!(output.rows().len(), 2);
    assert_eq!(
        output.rows().iter().map(|row| &row[0]).collect::<Vec<_>>(),
        [&value(6), &value(7)]
    );

    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert!(matches!(
        execute_min_steiner(&graph, &[0, 2], AlgorithmLimits::default(), cancellation,),
        Err(AlgorithmError::Cancelled)
    ));
    for limits in [
        AlgorithmLimits {
            output_rows: 0,
            ..AlgorithmLimits::default()
        },
        AlgorithmLimits {
            states: 0,
            ..AlgorithmLimits::default()
        },
    ] {
        assert!(
            execute_min_steiner(&graph, &[0, 2], limits, AlgorithmCancellation::default(),)
                .is_err()
        );
    }
    assert_eq!(
        execute_min_steiner(
            &graph,
            &[0, 2],
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap()
        .rows(),
        output.rows()
    );
}

#[test]
fn prize_steiner_dispatch_preserves_atomic_shared_controls() {
    let graph = AdjacencyGraph::with_test_undirected_multigraph(
        3,
        &[(9, 0, 1), (8, 0, 1), (7, 0, 2), (6, 1, 1)],
    );
    let prizes = [(0, 0.0), (1, 3.0), (2, 0.0)];
    let output = execute_prize_steiner(
        &graph,
        &[0],
        &prizes,
        AlgorithmLimits::default(),
        AlgorithmCancellation::default(),
    )
    .unwrap();
    assert_eq!(
        output.rows(),
        vec![vec![
            value(8),
            value(0),
            value(1),
            AlgorithmValue::Float64(1.0)
        ]]
    );

    let cancellation = AlgorithmCancellation::default();
    cancellation.cancel();
    assert!(matches!(
        execute_prize_steiner(
            &graph,
            &[0],
            &prizes,
            AlgorithmLimits::default(),
            cancellation,
        ),
        Err(AlgorithmError::Cancelled)
    ));
    for limits in [
        AlgorithmLimits {
            output_rows: 0,
            ..AlgorithmLimits::default()
        },
        AlgorithmLimits {
            states: 0,
            ..AlgorithmLimits::default()
        },
    ] {
        assert!(
            execute_prize_steiner(
                &graph,
                &[0],
                &prizes,
                limits,
                AlgorithmCancellation::default(),
            )
            .is_err()
        );
    }
    assert_eq!(
        execute_prize_steiner(
            &graph,
            &[0],
            &prizes,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap()
        .rows(),
        output.rows()
    );
}

fn execute_min_steiner(
    graph: &AdjacencyGraph,
    terminals: &[u64],
    limits: AlgorithmLimits,
    cancellation: AlgorithmCancellation,
) -> Result<AlgorithmOutput, AlgorithmError> {
    let mut registry = AlgorithmRegistry::default();
    registry.register(Arc::new(MinSteinerTree {
        terminals: terminals.iter().copied().map(uuid).collect(),
    }))?;
    registry.execute(
        Algorithm::Paths(PathAlgorithm::MinSteinerTree),
        graph,
        &AlgorithmControl::new(limits, cancellation),
    )
}

fn execute_prize_steiner(
    graph: &AdjacencyGraph,
    terminals: &[u64],
    prizes: &[(u64, f64)],
    limits: AlgorithmLimits,
    cancellation: AlgorithmCancellation,
) -> Result<AlgorithmOutput, AlgorithmError> {
    let mut registry = AlgorithmRegistry::default();
    registry.register(Arc::new(PrizeCollectingSteinerTree {
        terminals: terminals.iter().copied().map(uuid).collect(),
        prizes: prizes
            .iter()
            .map(|(node, prize)| NodePrize {
                node_uuid: uuid(*node),
                prize: ResolvedNumber::Float64(*prize),
            })
            .collect(),
    }))?;
    registry.execute(
        Algorithm::Paths(PathAlgorithm::PrizeCollectingSteinerTree),
        graph,
        &AlgorithmControl::new(limits, cancellation),
    )
}
