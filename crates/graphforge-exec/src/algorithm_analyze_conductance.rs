use std::collections::{BTreeMap, BTreeSet};

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};
use crate::algorithm_partition::ResolvedPartitionMap;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct ConductanceEdge {
    pub edge_uuid: [u8; 16],
    pub source_uuid: [u8; 16],
    pub target_uuid: [u8; 16],
    pub weight: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ConductanceRow {
    pub partition_id: String,
    pub conductance: f64,
}

pub(crate) fn conductance(
    nodes: &[[u8; 16]],
    edges: &[ConductanceEdge],
    directed: bool,
    partitions: &ResolvedPartitionMap,
    control: &AlgorithmControl,
) -> Result<Vec<ConductanceRow>, AlgorithmError> {
    control.check_graph_size(nodes.len(), u64::try_from(edges.len()).unwrap_or(u64::MAX))?;
    control.checkpoint()?;
    if directed {
        return Err(execution("conductance requires an undirected graph"));
    }
    let selected = nodes.iter().copied().collect::<BTreeSet<_>>();
    if selected.len() != nodes.len() {
        return Err(execution("conductance node UUIDs must be unique"));
    }
    if !partitions.iter().map(|(node, _)| node).eq(selected.iter()) {
        return Err(execution("partition mapping must exactly match selection"));
    }

    let mut volumes = partitions
        .iter()
        .map(|(_, id)| (id.as_str().to_owned(), 0.0))
        .collect::<BTreeMap<_, _>>();
    if volumes.len() < 2 {
        return Err(execution("conductance requires two non-empty partitions"));
    }
    control.check_output_rows(volumes.len())?;
    let mut cuts = volumes.clone();
    let mut stored = BTreeMap::new();
    let mut work = 0;
    for &raw in edges {
        checkpoint(control, &mut work)?;
        if !raw.weight.is_finite() || raw.weight < 0.0 {
            return Err(execution(
                "conductance weights must be finite and nonnegative",
            ));
        }
        let edge = canonical(raw);
        if !selected.contains(&edge.source_uuid) || !selected.contains(&edge.target_uuid) {
            return Err(execution("conductance edge endpoint is outside selection"));
        }
        if let Some(previous) = stored.insert(edge.edge_uuid, edge)
            && previous != edge
        {
            return Err(execution("conductance edge UUID is inconsistent"));
        }
    }
    for edge in stored.into_values() {
        checkpoint(control, &mut work)?;
        let source = partitions
            .get(&edge.source_uuid)
            .expect("validated")
            .as_str();
        let target = partitions
            .get(&edge.target_uuid)
            .expect("validated")
            .as_str();
        add(&mut volumes, source, edge.weight)?;
        add(&mut volumes, target, edge.weight)?;
        if source != target {
            add(&mut cuts, source, edge.weight)?;
            add(&mut cuts, target, edge.weight)?;
        }
    }

    let mut rows = Vec::with_capacity(volumes.len());
    for (partition, &volume) in &volumes {
        checkpoint(control, &mut work)?;
        let complement = volumes
            .iter()
            .filter(|(other, _)| *other != partition)
            .try_fold(0.0, |sum, (_, value)| finite(sum + value))?;
        let denominator = volume.min(complement);
        if denominator == 0.0 {
            return Err(AlgorithmError::UndefinedConductance {
                partition: partition.clone(),
            });
        }
        rows.push(ConductanceRow {
            partition_id: partition.clone(),
            conductance: cuts[partition] / denominator,
        });
    }
    control.check_cancelled()?;
    Ok(rows)
}

fn canonical(mut edge: ConductanceEdge) -> ConductanceEdge {
    if edge.target_uuid < edge.source_uuid {
        std::mem::swap(&mut edge.source_uuid, &mut edge.target_uuid);
    }
    edge
}

fn add(values: &mut BTreeMap<String, f64>, key: &str, value: f64) -> Result<(), AlgorithmError> {
    let current = values.get_mut(key).expect("initialized partition");
    *current = finite(*current + value)?;
    Ok(())
}

fn finite(value: f64) -> Result<f64, AlgorithmError> {
    value
        .is_finite()
        .then_some(value)
        .ok_or_else(|| execution("conductance accumulation exceeds finite range"))
}

fn checkpoint(control: &AlgorithmControl, work: &mut usize) -> Result<(), AlgorithmError> {
    *work += 1;
    if work.is_multiple_of(4_096) {
        control.checkpoint()?;
    } else {
        control.check_cancelled()?;
    }
    Ok(())
}

fn execution(message: &str) -> AlgorithmError {
    AlgorithmError::Execution {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algorithm_dispatch::{AlgorithmCancellation, AlgorithmLimits};
    use crate::algorithm_partition::PartitionValue;

    fn uuid(n: u8) -> [u8; 16] {
        [n; 16]
    }
    fn edge(id: u8, source: u8, target: u8, weight: f64) -> ConductanceEdge {
        ConductanceEdge {
            edge_uuid: uuid(id),
            source_uuid: uuid(source),
            target_uuid: uuid(target),
            weight,
        }
    }
    fn mapping(values: &[(u8, &str)]) -> ResolvedPartitionMap {
        ResolvedPartitionMap::try_new(
            values.iter().map(|(node, _)| uuid(*node)),
            values
                .iter()
                .map(|(node, id)| (uuid(*node), PartitionValue::String((*id).to_owned()))),
        )
        .unwrap()
    }
    fn control() -> AlgorithmControl {
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default())
    }
    fn run(
        nodes: &[[u8; 16]],
        edges: &[ConductanceEdge],
        directed: bool,
        map: &ResolvedPartitionMap,
    ) -> Result<Vec<ConductanceRow>, AlgorithmError> {
        conductance(nodes, edges, directed, map, &control())
    }

    #[test]
    fn exact_weighted_cut_volume_loop_parallel_and_ordering() {
        let nodes = [uuid(1), uuid(2), uuid(3), uuid(4)];
        let map = mapping(&[(1, "z"), (2, "a"), (3, "z"), (4, "m")]);
        let edges = [
            edge(10, 1, 2, 2.0),
            edge(10, 2, 1, 2.0),
            edge(11, 1, 2, 3.0),
            edge(12, 1, 3, 7.0),
            edge(13, 3, 3, 5.0),
            edge(14, 2, 4, 4.0),
        ];
        let values = run(&nodes, &edges, false, &map).unwrap();
        assert_eq!(
            values
                .iter()
                .map(|row| (row.partition_id.as_str(), row.conductance))
                .collect::<Vec<_>>(),
            [("a", 1.0), ("m", 1.0), ("z", 5.0 / 13.0)]
        );
        let nodes = [uuid(1), uuid(2), uuid(3)];
        let first = mapping(&[(1, "left"), (2, "right"), (3, "right")]);
        let second = mapping(&[(3, "right"), (1, "left"), (2, "right")]);
        assert_eq!(
            run(&nodes, &[edge(1, 1, 2, 2.0)], false, &first),
            run(&nodes, &[edge(1, 1, 2, 2.0)], false, &second)
        );
        assert_eq!(
            run(&nodes, &[], false, &first),
            Err(AlgorithmError::UndefinedConductance {
                partition: "left".into(),
            })
        );
    }

    #[test]
    fn invalid_inputs_are_atomic() {
        let nodes = [uuid(1), uuid(2)];
        let map = mapping(&[(1, "a"), (2, "b")]);
        let inconsistent = [edge(1, 1, 2, 1.0), edge(1, 1, 2, 2.0)];
        for result in [
            run(&nodes, &[], true, &map),
            run(&nodes, &[edge(1, 1, 2, f64::NAN)], false, &map),
            run(&nodes, &[edge(1, 1, 2, f64::INFINITY)], false, &map),
            run(&nodes, &[edge(1, 1, 2, -1.0)], false, &map),
            run(&nodes, &[edge(1, 1, 3, 1.0)], false, &map),
            run(&nodes, &inconsistent, false, &map),
            run(&[uuid(1), uuid(1)], &[], false, &mapping(&[(1, "a")])),
            run(&nodes, &[], false, &mapping(&[(1, "a"), (2, "a")])),
        ] {
            assert!(matches!(result, Err(AlgorithmError::Execution { .. })));
        }
    }

    #[test]
    fn cancellation_limits_and_overflow_are_structured() {
        let nodes = [uuid(1), uuid(2)];
        let map = mapping(&[(1, "a"), (2, "b")]);
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        let cancelled = AlgorithmControl::new(AlgorithmLimits::default(), cancellation);
        assert_eq!(
            conductance(&nodes, &[], false, &map, &cancelled),
            Err(AlgorithmError::Cancelled)
        );
        macro_rules! assert_limit {
            ($values:expr, $pattern:pat) => {
                let limits = $values;
                let control = AlgorithmControl::new(
                    AlgorithmLimits {
                        nodes: limits.0,
                        edges: limits.1,
                        output_rows: limits.2,
                        iterations: limits.3,
                        states: AlgorithmLimits::default().states,
                        batch_size: AlgorithmLimits::default().batch_size,
                    },
                    AlgorithmCancellation::default(),
                );
                assert!(matches!(
                    conductance(&nodes, &[edge(1, 1, 2, 1.0)], false, &map, &control),
                    Err($pattern)
                ));
            };
        }
        assert_limit!((1, 10, 10, 10), AlgorithmError::NodeLimit { .. });
        assert_limit!((10, 0, 10, 10), AlgorithmError::EdgeLimit { .. });
        assert_limit!((10, 10, 1, 10), AlgorithmError::OutputLimit { .. });
        assert_limit!((10, 10, 10, 0), AlgorithmError::IterationLimit { .. });
        let overflow = [edge(1, 1, 2, f64::MAX), edge(2, 1, 2, f64::MAX)];
        assert!(run(&nodes, &overflow, false, &map).is_err());
    }
}
