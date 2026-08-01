use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};
use crate::algorithm_partition::ResolvedPartitionMap;

/// One graph-native undirected edge supplied to the bipartite kernel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BipartiteEdge {
    pub edge: [u8; 16],
    pub source: [u8; 16],
    pub target: [u8; 16],
}

/// A validated, canonically oriented bipartite projection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BipartiteProjection {
    pub left_nodes: Vec<[u8; 16]>,
    pub right_nodes: Vec<[u8; 16]>,
    pub edges: Vec<BipartiteEdge>,
}

/// Validate an explicit partition mapping or deterministically infer one.
///
/// Validation finishes before a projection is returned. Parallel edges remain
/// present and isolates remain valid.
pub(crate) fn resolve_bipartite_projection(
    nodes: &[[u8; 16]],
    edges: &[BipartiteEdge],
    partitions: Option<&ResolvedPartitionMap>,
    control: &AlgorithmControl,
) -> Result<BipartiteProjection, AlgorithmError> {
    control.checkpoint()?;
    let node_count = nodes.len();
    let nodes = nodes.iter().copied().collect::<BTreeSet<_>>();
    if nodes.len() != node_count {
        return execution("selected projection contains duplicate node UUIDs");
    }
    let mut adjacency = BTreeMap::<[u8; 16], Vec<[u8; 16]>>::new();
    for &node in &nodes {
        adjacency.insert(node, Vec::new());
    }
    for edge in edges {
        control.check_cancelled()?;
        if !nodes.contains(&edge.source) || !nodes.contains(&edge.target) {
            return execution("edge endpoint is outside the selected projection");
        }
        if edge.source == edge.target {
            return execution("selected graph is not bipartite: self-loop");
        }
        adjacency
            .get_mut(&edge.source)
            .expect("validated endpoint")
            .push(edge.target);
        adjacency
            .get_mut(&edge.target)
            .expect("validated endpoint")
            .push(edge.source);
    }
    for neighbors in adjacency.values_mut() {
        neighbors.sort_unstable();
        neighbors.dedup();
    }

    let sides = match partitions {
        Some(mapping) => explicit_sides(&nodes, edges, mapping)?,
        None => inferred_sides(&adjacency, control)?,
    };
    let mut left_nodes = Vec::new();
    let mut right_nodes = Vec::new();
    for node in nodes {
        if sides[&node] {
            right_nodes.push(node);
        } else {
            left_nodes.push(node);
        }
    }

    let mut oriented = edges
        .iter()
        .map(|edge| {
            if sides[&edge.source] {
                BipartiteEdge {
                    source: edge.target,
                    target: edge.source,
                    ..*edge
                }
            } else {
                *edge
            }
        })
        .collect::<Vec<_>>();
    oriented.sort_unstable_by_key(|edge| (edge.source, edge.target, edge.edge));
    Ok(BipartiteProjection {
        left_nodes,
        right_nodes,
        edges: oriented,
    })
}

fn explicit_sides(
    nodes: &BTreeSet<[u8; 16]>,
    edges: &[BipartiteEdge],
    mapping: &ResolvedPartitionMap,
) -> Result<BTreeMap<[u8; 16], bool>, AlgorithmError> {
    if !mapping.iter().map(|(node, _)| node).eq(nodes.iter()) {
        return execution("partition mapping does not exactly match the selected projection");
    }
    let edge_nodes = edges
        .iter()
        .flat_map(|edge| [edge.source, edge.target])
        .collect::<BTreeSet<_>>();
    let partitions = edge_nodes
        .iter()
        .map(|node| {
            mapping.get(node).ok_or_else(|| AlgorithmError::Execution {
                message: "partition mapping is incomplete".into(),
            })
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    if !edge_nodes.is_empty() && partitions.len() != 2 {
        return execution("edge-bearing projection must contain exactly two partitions");
    }
    let left = partitions.first().copied();
    let mut sides = BTreeMap::new();
    for &node in nodes {
        let partition = mapping
            .get(&node)
            .ok_or_else(|| AlgorithmError::Execution {
                message: "partition mapping is incomplete".into(),
            })?;
        sides.insert(node, left.is_some_and(|left| partition != left));
    }
    for edge in edges {
        if sides[&edge.source] == sides[&edge.target] {
            return execution("selected edge connects nodes in the same partition");
        }
    }
    Ok(sides)
}

fn inferred_sides(
    adjacency: &BTreeMap<[u8; 16], Vec<[u8; 16]>>,
    control: &AlgorithmControl,
) -> Result<BTreeMap<[u8; 16], bool>, AlgorithmError> {
    let mut sides = BTreeMap::new();
    for &start in adjacency.keys() {
        if sides.contains_key(&start) {
            continue;
        }
        sides.insert(start, false);
        let mut queue = VecDeque::from([start]);
        while let Some(node) = queue.pop_front() {
            control.check_cancelled()?;
            let side = sides[&node];
            for &neighbor in &adjacency[&node] {
                match sides.get(&neighbor) {
                    Some(neighbor_side) if *neighbor_side == side => {
                        return execution("selected graph is not bipartite: odd cycle");
                    }
                    Some(_) => {}
                    None => {
                        sides.insert(neighbor, !side);
                        queue.push_back(neighbor);
                    }
                }
            }
        }
    }
    Ok(sides)
}

fn execution<T>(message: &str) -> Result<T, AlgorithmError> {
    Err(AlgorithmError::Execution {
        message: message.into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algorithm_dispatch::{AlgorithmCancellation, AlgorithmLimits};
    use crate::algorithm_partition::PartitionValue;

    fn uuid(value: u128) -> [u8; 16] {
        value.to_be_bytes()
    }

    fn edge(id: u128, source: u128, target: u128) -> BipartiteEdge {
        BipartiteEdge {
            edge: uuid(id),
            source: uuid(source),
            target: uuid(target),
        }
    }

    fn control() -> AlgorithmControl {
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default())
    }

    #[test]
    fn explicit_mapping_orients_by_normalized_partition_and_preserves_parallel_edges() {
        let nodes = [uuid(1), uuid(2), uuid(3), uuid(4), uuid(5)];
        let mapping = ResolvedPartitionMap::try_new(
            nodes,
            [
                (uuid(1), PartitionValue::String("z".into())),
                (uuid(2), PartitionValue::String("a".into())),
                (uuid(3), PartitionValue::String("z".into())),
                (uuid(4), PartitionValue::String("a".into())),
                (uuid(5), PartitionValue::String("z".into())),
            ],
        )
        .unwrap();
        let projection = resolve_bipartite_projection(
            &nodes,
            &[edge(9, 1, 2), edge(8, 2, 1), edge(7, 3, 4)],
            Some(&mapping),
            &control(),
        )
        .unwrap();

        assert_eq!(projection.left_nodes, [uuid(2), uuid(4)]);
        assert_eq!(projection.right_nodes, [uuid(1), uuid(3), uuid(5)]);
        assert_eq!(
            projection.edges,
            [edge(8, 2, 1), edge(9, 2, 1), edge(7, 4, 3)]
        );
    }

    #[test]
    fn inference_orients_each_component_from_its_lowest_uuid_and_keeps_isolates() {
        let projection = resolve_bipartite_projection(
            &[uuid(1), uuid(2), uuid(5), uuid(6), uuid(9)],
            &[edge(1, 2, 1), edge(2, 6, 5)],
            None,
            &control(),
        )
        .unwrap();
        assert_eq!(projection.left_nodes, [uuid(1), uuid(5), uuid(9)]);
        assert_eq!(projection.right_nodes, [uuid(2), uuid(6)]);
        assert_eq!(projection.edges, [edge(1, 1, 2), edge(2, 5, 6)]);
    }

    #[test]
    fn rejects_invalid_explicit_and_inferred_graphs_atomically() {
        let nodes = [uuid(1), uuid(2), uuid(3)];
        let same = ResolvedPartitionMap::try_new(
            nodes,
            nodes.map(|node| (node, PartitionValue::Integer(0))),
        )
        .unwrap();
        assert!(
            resolve_bipartite_projection(&nodes, &[edge(1, 1, 2)], Some(&same), &control())
                .is_err()
        );
        assert!(
            resolve_bipartite_projection(
                &nodes,
                &[edge(1, 1, 2), edge(2, 2, 3), edge(3, 3, 1)],
                None,
                &control()
            )
            .is_err()
        );
        assert!(resolve_bipartite_projection(&nodes, &[edge(1, 1, 1)], None, &control()).is_err());
        let superset = ResolvedPartitionMap::try_new(
            [uuid(1), uuid(2), uuid(3), uuid(4)],
            (1..=4).map(|node| (uuid(node), PartitionValue::Integer(node as i64 % 2))),
        )
        .unwrap();
        assert!(resolve_bipartite_projection(&nodes, &[], Some(&superset), &control()).is_err());
        assert!(resolve_bipartite_projection(&[uuid(1), uuid(1)], &[], None, &control()).is_err());
    }

    #[test]
    fn cancellation_is_observed_before_any_projection_is_returned() {
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        let control = AlgorithmControl::new(AlgorithmLimits::default(), cancellation);
        assert_eq!(
            resolve_bipartite_projection(&[uuid(1)], &[], None, &control),
            Err(AlgorithmError::Cancelled)
        );
    }
}
