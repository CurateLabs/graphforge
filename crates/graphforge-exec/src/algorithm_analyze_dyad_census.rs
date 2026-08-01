use std::collections::{BTreeMap, BTreeSet};

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};

const CHECKPOINT_INTERVAL: usize = 4_096;
type NodeUuid = [u8; 16];
type DirectedPair = (NodeUuid, NodeUuid);

/// One directed stored edge entry in the selected public-identity projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DyadEdge {
    pub edge: NodeUuid,
    pub source: NodeUuid,
    pub target: NodeUuid,
}

/// Counts for the three canonical directed dyad categories.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct DyadCounts {
    pub mutual: u64,
    pub asymmetric: u64,
    pub null: u64,
}

/// Classify every unordered pair of distinct selected nodes by edge presence.
pub(crate) fn dyad_census(
    nodes: &[NodeUuid],
    edges: &[DyadEdge],
    control: &AlgorithmControl,
) -> Result<DyadCounts, AlgorithmError> {
    control.checkpoint()?;
    control.check_output_rows(3)?;
    let mut work = 0_usize;
    let selected = index_nodes(nodes, control, &mut work)?;
    let directed_pairs = normalize_edges(edges, &selected, control, &mut work)?;
    let mut seen_pairs = BTreeMap::<([u8; 16], [u8; 16]), u8>::new();

    for (source, target) in directed_pairs {
        checkpoint(control, &mut work)?;
        let (pair, direction) = if source < target {
            ((source, target), 0b01)
        } else {
            ((target, source), 0b10)
        };
        *seen_pairs.entry(pair).or_default() |= direction;
    }

    let mut counts = DyadCounts::default();
    for directions in seen_pairs.values() {
        checkpoint(control, &mut work)?;
        if *directions == 0b11 {
            counts.mutual = increment(counts.mutual, "mutual")?;
        } else {
            counts.asymmetric = increment(counts.asymmetric, "asymmetric")?;
        }
    }
    let nodes = u64::try_from(selected.len())
        .map_err(|_| execution("dyad_census node count exceeds UInt64 range"))?;
    let total = nodes
        .checked_mul(nodes.saturating_sub(1))
        .and_then(|value| value.checked_div(2))
        .ok_or_else(|| execution("dyad_census pair count exceeds supported range"))?;
    let present = counts
        .mutual
        .checked_add(counts.asymmetric)
        .ok_or_else(|| execution("dyad_census category sum exceeds supported range"))?;
    counts.null = total
        .checked_sub(present)
        .ok_or_else(|| execution("dyad_census category sum exceeds pair count"))?;
    Ok(counts)
}

fn index_nodes(
    nodes: &[[u8; 16]],
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<BTreeSet<NodeUuid>, AlgorithmError> {
    let mut selected = BTreeSet::new();
    for &node in nodes {
        checkpoint(control, work)?;
        if !selected.insert(node) {
            return Err(execution("dyad_census node UUIDs must be unique"));
        }
    }
    Ok(selected)
}

fn normalize_edges(
    edges: &[DyadEdge],
    selected: &BTreeSet<NodeUuid>,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<BTreeSet<DirectedPair>, AlgorithmError> {
    let mut stored = BTreeMap::new();
    let mut directed_pairs = BTreeSet::new();
    for &edge in edges {
        checkpoint(control, work)?;
        if !selected.contains(&edge.source) || !selected.contains(&edge.target) {
            return Err(execution(
                "dyad_census edge endpoint is outside node selection",
            ));
        }
        if let Some(previous) = stored.insert(edge.edge, (edge.source, edge.target))
            && previous != (edge.source, edge.target)
        {
            return Err(execution(
                "dyad_census edge UUID has inconsistent adjacency entries",
            ));
        }
        if edge.source != edge.target {
            directed_pairs.insert((edge.source, edge.target));
        }
    }
    Ok(directed_pairs)
}

fn increment(value: u64, category: &str) -> Result<u64, AlgorithmError> {
    value.checked_add(1).ok_or_else(|| {
        execution(format!(
            "dyad_census {category} count exceeds supported range"
        ))
    })
}

fn checkpoint(control: &AlgorithmControl, work: &mut usize) -> Result<(), AlgorithmError> {
    *work = work.saturating_add(1);
    if work.is_multiple_of(CHECKPOINT_INTERVAL) {
        control.checkpoint()?;
    } else {
        control.check_cancelled()?;
    }
    Ok(())
}

fn execution(message: impl Into<String>) -> AlgorithmError {
    AlgorithmError::Execution {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algorithm_dispatch::{AlgorithmCancellation, AlgorithmLimits};

    fn uuid(value: u8) -> [u8; 16] {
        [value; 16]
    }

    fn edge(id: u8, source: u8, target: u8) -> DyadEdge {
        DyadEdge {
            edge: uuid(id),
            source: uuid(source),
            target: uuid(target),
        }
    }

    fn control() -> AlgorithmControl {
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default())
    }

    #[test]
    fn classifies_hand_verifiable_directed_fixture() {
        let nodes = (0..5).map(uuid).collect::<Vec<_>>();
        let edges = [
            edge(10, 0, 1),
            edge(11, 1, 0),
            edge(12, 0, 2),
            edge(13, 3, 2),
        ];
        assert_eq!(
            dyad_census(&nodes, &edges, &control()).unwrap(),
            DyadCounts {
                mutual: 1,
                asymmetric: 2,
                null: 7,
            }
        );
    }

    #[test]
    fn normalizes_parallel_duplicate_reciprocal_and_loop_entries() {
        let nodes = [uuid(0), uuid(1), uuid(2)];
        let edges = [
            edge(10, 0, 1),
            edge(10, 0, 1),
            edge(11, 0, 1),
            edge(12, 1, 0),
            edge(13, 2, 2),
        ];
        assert_eq!(
            dyad_census(&nodes, &edges, &control()).unwrap(),
            DyadCounts {
                mutual: 1,
                asymmetric: 0,
                null: 2,
            }
        );
    }

    #[test]
    fn empty_singleton_and_edgeless_selections_keep_three_category_counts() {
        for (nodes, expected_null) in [
            (vec![], 0),
            (vec![uuid(0)], 0),
            (vec![uuid(0), uuid(1), uuid(2), uuid(3)], 6),
        ] {
            assert_eq!(
                dyad_census(&nodes, &[], &control()).unwrap(),
                DyadCounts {
                    null: expected_null,
                    ..DyadCounts::default()
                }
            );
        }
    }

    #[test]
    fn rejects_invalid_identity_topology_atomically() {
        for result in [
            dyad_census(&[uuid(0), uuid(0)], &[], &control()),
            dyad_census(&[uuid(0)], &[edge(1, 0, 2)], &control()),
            dyad_census(
                &[uuid(0), uuid(1), uuid(2)],
                &[edge(1, 0, 1), edge(1, 0, 2)],
                &control(),
            ),
        ] {
            assert!(matches!(result, Err(AlgorithmError::Execution { .. })));
        }
    }

    #[test]
    fn shared_output_iteration_and_cancellation_controls_are_structured() {
        let no_output = AlgorithmControl::new(
            AlgorithmLimits {
                output_rows: 2,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            dyad_census(&[], &[], &no_output),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let no_iterations = AlgorithmControl::new(
            AlgorithmLimits {
                iterations: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            dyad_census(&[], &[], &no_iterations),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            dyad_census(
                &[],
                &[],
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation)
            ),
            Err(AlgorithmError::Cancelled)
        );
    }
}
