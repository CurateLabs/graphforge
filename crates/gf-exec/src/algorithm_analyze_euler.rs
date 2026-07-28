use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};

const CHECKPOINT_INTERVAL: usize = 4_096;

/// One stored edge in the selected public-identity projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct EulerEdge {
    pub edge: [u8; 16],
    pub source: [u8; 16],
    pub target: [u8; 16],
}

/// The requested Euler construction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EulerTrailKind {
    Circuit,
    Path,
}

/// One coherent Euler trail over public node and stored-edge identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct EulerTrail {
    pub node_path: Vec<[u8; 16]>,
    pub edge_path: Vec<[u8; 16]>,
}

/// Kernel outcome before the facade maps mathematically undefined requests to
/// the catalog-specific structured error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum EulerTrailOutcome {
    EmptySelection,
    Trail(EulerTrail),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ProjectedEdge {
    edge: [u8; 16],
    source: usize,
    target: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct AdjacencyEntry {
    edge_index: usize,
    target: usize,
}

/// Canonical UUID multigraph projection shared by Euler predicates and
/// construction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct EulerProjection {
    nodes: Vec<[u8; 16]>,
    edges: Vec<ProjectedEdge>,
    directed: bool,
}

impl EulerProjection {
    pub(crate) fn new(
        nodes: &[[u8; 16]],
        edges: &[EulerEdge],
        directed: bool,
        control: &AlgorithmControl,
    ) -> Result<Self, AlgorithmError> {
        control.checkpoint()?;
        control.check_graph_size(nodes.len(), 0)?;

        let mut work = 0_usize;
        let mut ordered_nodes = Vec::new();
        reserve(&mut ordered_nodes, nodes.len(), "Euler node projection")?;
        ordered_nodes.extend_from_slice(nodes);
        ordered_nodes.sort_unstable();
        for pair in ordered_nodes.windows(2) {
            checkpoint(control, &mut work)?;
            if pair[0] == pair[1] {
                return Err(execution("Euler node UUIDs must be unique"));
            }
        }

        let mut stored = Vec::new();
        reserve(&mut stored, edges.len(), "Euler stored-edge projection")?;
        for &raw in edges {
            checkpoint(control, &mut work)?;
            let (source_uuid, target_uuid) = canonical_endpoints(raw.source, raw.target, directed);
            let source = ordered_nodes
                .binary_search(&source_uuid)
                .map_err(|_| execution("Euler edge endpoint is outside node selection"))?;
            let target = ordered_nodes
                .binary_search(&target_uuid)
                .map_err(|_| execution("Euler edge endpoint is outside node selection"))?;
            stored.push(ProjectedEdge {
                edge: raw.edge,
                source,
                target,
            });
        }
        stored.sort_unstable_by_key(|edge| edge.edge);
        control.check_cancelled()?;

        let mut unique_edge_count = 0_usize;
        let mut adjacency_entry_count = 0_u64;
        for (index, edge) in stored.iter().enumerate() {
            checkpoint(control, &mut work)?;
            if index == 0 || stored[index - 1].edge != edge.edge {
                unique_edge_count = unique_edge_count
                    .checked_add(1)
                    .ok_or_else(|| execution("Euler unique edge count exceeds platform range"))?;
                let entries = if directed || edge.source == edge.target {
                    1
                } else {
                    2
                };
                adjacency_entry_count =
                    checked_adjacency_entry_add(adjacency_entry_count, entries)?;
            } else if stored[index - 1].source != edge.source
                || stored[index - 1].target != edge.target
            {
                return Err(execution(
                    "Euler edge UUID has inconsistent adjacency entries",
                ));
            }
        }
        control.check_graph_size(ordered_nodes.len(), adjacency_entry_count)?;
        let mut projected: Vec<ProjectedEdge> = Vec::new();
        reserve(&mut projected, unique_edge_count, "Euler edge projection")?;
        for (index, edge) in stored.into_iter().enumerate() {
            checkpoint(control, &mut work)?;
            if index == 0
                || projected
                    .last()
                    .is_none_or(|previous| previous.edge != edge.edge)
            {
                projected.push(edge);
            }
        }

        Ok(Self {
            nodes: ordered_nodes,
            edges: projected,
            directed,
        })
    }

    pub(crate) fn has_circuit(&self, control: &AlgorithmControl) -> Result<bool, AlgorithmError> {
        let mut work = 0_usize;
        let classification = self.classify(control, &mut work)?;
        Ok(classification.circuit_start.is_some() || self.nodes.is_empty())
    }

    pub(crate) fn has_path(&self, control: &AlgorithmControl) -> Result<bool, AlgorithmError> {
        let mut work = 0_usize;
        let classification = self.classify(control, &mut work)?;
        Ok(classification.path_start.is_some() || self.nodes.is_empty())
    }

    pub(crate) fn trail(
        &self,
        kind: EulerTrailKind,
        control: &AlgorithmControl,
    ) -> Result<EulerTrailOutcome, AlgorithmError> {
        if self.nodes.is_empty() {
            return Ok(EulerTrailOutcome::EmptySelection);
        }
        control.check_output_rows(1)?;

        let mut work = 0_usize;
        let classification = self.classify(control, &mut work)?;
        let start = match kind {
            EulerTrailKind::Circuit => classification.circuit_start,
            EulerTrailKind::Path => classification.path_start,
        };
        let Some(start) = start else {
            return Err(match kind {
                EulerTrailKind::Circuit => AlgorithmError::UndefinedEulerCircuit,
                EulerTrailKind::Path => AlgorithmError::UndefinedEulerPath,
            });
        };

        let (node_len, edge_len) = checked_path_lengths(self.edges.len())?;
        if self.edges.is_empty() {
            let mut node_path = Vec::new();
            reserve(&mut node_path, node_len, "Euler node path")?;
            node_path.push(self.nodes[start]);
            return Ok(EulerTrailOutcome::Trail(EulerTrail {
                node_path,
                edge_path: Vec::new(),
            }));
        }

        self.hierholzer(start, node_len, edge_len, control, &mut work)
            .map(EulerTrailOutcome::Trail)
    }

    fn hierholzer(
        &self,
        start: usize,
        node_len: usize,
        edge_len: usize,
        control: &AlgorithmControl,
        work: &mut usize,
    ) -> Result<EulerTrail, AlgorithmError> {
        let mut node_path = Vec::new();
        reserve(&mut node_path, node_len, "Euler node path")?;
        let mut edge_path = Vec::new();
        reserve(&mut edge_path, edge_len, "Euler edge path")?;
        let adjacency_counts =
            traversal_adjacency_counts(self.nodes.len(), &self.edges, self.directed)?;
        let mut adjacency = Vec::new();
        reserve(&mut adjacency, self.nodes.len(), "Euler adjacency")?;
        adjacency.resize_with(self.nodes.len(), Vec::new);
        for (neighbors, &count) in adjacency.iter_mut().zip(&adjacency_counts) {
            reserve(neighbors, count, "Euler adjacency entries")?;
        }
        for (edge_index, edge) in self.edges.iter().enumerate() {
            checkpoint(control, work)?;
            adjacency[edge.source].push(AdjacencyEntry {
                edge_index,
                target: edge.target,
            });
            if !self.directed && edge.source != edge.target {
                adjacency[edge.target].push(AdjacencyEntry {
                    edge_index,
                    target: edge.source,
                });
            }
        }

        let mut cursors = Vec::new();
        reserve(&mut cursors, self.nodes.len(), "Euler adjacency cursors")?;
        cursors.resize(self.nodes.len(), 0);
        let mut used = Vec::new();
        reserve(&mut used, self.edges.len(), "Euler used-edge bitmap")?;
        used.resize(self.edges.len(), false);
        let mut node_stack = Vec::new();
        reserve(&mut node_stack, node_len, "Euler traversal node stack")?;
        node_stack.push(start);
        let mut edge_stack = Vec::new();
        reserve(&mut edge_stack, edge_len, "Euler traversal edge stack")?;
        let mut reversed_nodes = Vec::new();
        reserve(&mut reversed_nodes, node_len, "Euler reversed node path")?;
        let mut reversed_edges = Vec::new();
        reserve(&mut reversed_edges, edge_len, "Euler reversed edge path")?;

        while let Some(&node) = node_stack.last() {
            checkpoint(control, work)?;
            while cursors[node] < adjacency[node].len()
                && used[adjacency[node][cursors[node]].edge_index]
            {
                checkpoint(control, work)?;
                cursors[node] += 1;
            }
            if let Some(&entry) = adjacency[node].get(cursors[node]) {
                cursors[node] += 1;
                if used[entry.edge_index] {
                    continue;
                }
                used[entry.edge_index] = true;
                node_stack.push(entry.target);
                edge_stack.push(self.edges[entry.edge_index].edge);
            } else {
                reversed_nodes.push(node_stack.pop().expect("stack is non-empty"));
                if let Some(edge) = edge_stack.pop() {
                    reversed_edges.push(edge);
                }
            }
        }

        if used.iter().any(|is_used| !is_used)
            || reversed_nodes.len() != node_len
            || reversed_edges.len() != edge_len
        {
            return Err(execution(
                "Euler traversal did not consume every selected stored edge",
            ));
        }
        reversed_nodes.reverse();
        reversed_edges.reverse();
        node_path.extend(
            reversed_nodes
                .into_iter()
                .map(|position| self.nodes[position]),
        );
        edge_path.extend(reversed_edges);
        if node_path.len() != edge_path.len() + 1 {
            return Err(execution(
                "Euler traversal produced incoherent node and edge paths",
            ));
        }

        Ok(EulerTrail {
            node_path,
            edge_path,
        })
    }

    fn classify(
        &self,
        control: &AlgorithmControl,
        work: &mut usize,
    ) -> Result<Classification, AlgorithmError> {
        if self.directed {
            self.classify_directed(control, work)
        } else {
            self.classify_undirected(control, work)
        }
    }

    fn classify_undirected(
        &self,
        control: &AlgorithmControl,
        work: &mut usize,
    ) -> Result<Classification, AlgorithmError> {
        let mut degree = zeroed(self.nodes.len(), "Euler undirected degrees")?;
        let mut adjacency = empty_adjacency(self.nodes.len())?;
        let adjacency_counts = traversal_adjacency_counts(self.nodes.len(), &self.edges, false)?;
        for (neighbors, &count) in adjacency.iter_mut().zip(&adjacency_counts) {
            reserve(neighbors, count, "Euler connectivity adjacency entries")?;
        }
        for edge in &self.edges {
            checkpoint(control, work)?;
            increment(
                &mut degree[edge.source],
                "Euler degree exceeds UInt64 range",
            )?;
            increment(
                &mut degree[edge.target],
                "Euler degree exceeds UInt64 range",
            )?;
            adjacency[edge.source].push(edge.target);
            if edge.source != edge.target {
                adjacency[edge.target].push(edge.source);
            }
        }
        let mut active = Vec::new();
        reserve(&mut active, degree.len(), "Euler active-node flags")?;
        active.extend(degree.iter().map(|&value| value > 0));
        if !reachable_all_active(&adjacency, &active, control, work)? {
            return Ok(Classification::undefined());
        }
        let mut odd = Vec::new();
        reserve(&mut odd, degree.len(), "Euler odd-degree nodes")?;
        odd.extend(
            degree
                .iter()
                .enumerate()
                .filter_map(|(node, &value)| (value % 2 == 1).then_some(node)),
        );
        let active_start = active.iter().position(|&value| value);
        let edgeless_start = (!self.nodes.is_empty()).then_some(0);
        Ok(Classification {
            circuit_start: odd
                .is_empty()
                .then_some(active_start.or(edgeless_start))
                .flatten(),
            path_start: match odd.as_slice() {
                [] => active_start.or(edgeless_start),
                [first, _second] => Some(*first),
                _ => None,
            },
        })
    }

    fn classify_directed(
        &self,
        control: &AlgorithmControl,
        work: &mut usize,
    ) -> Result<Classification, AlgorithmError> {
        let mut incoming = zeroed(self.nodes.len(), "Euler incoming degrees")?;
        let mut outgoing = zeroed(self.nodes.len(), "Euler outgoing degrees")?;
        let mut forward = empty_adjacency(self.nodes.len())?;
        let mut reverse = empty_adjacency(self.nodes.len())?;
        let mut weak = empty_adjacency(self.nodes.len())?;
        let forward_counts = directed_adjacency_counts(self.nodes.len(), &self.edges, false)?;
        let reverse_counts = directed_adjacency_counts(self.nodes.len(), &self.edges, true)?;
        let weak_counts = traversal_adjacency_counts(self.nodes.len(), &self.edges, false)?;
        for (neighbors, &count) in forward.iter_mut().zip(&forward_counts) {
            reserve(neighbors, count, "Euler forward adjacency entries")?;
        }
        for (neighbors, &count) in reverse.iter_mut().zip(&reverse_counts) {
            reserve(neighbors, count, "Euler reverse adjacency entries")?;
        }
        for (neighbors, &count) in weak.iter_mut().zip(&weak_counts) {
            reserve(neighbors, count, "Euler weak adjacency entries")?;
        }
        for edge in &self.edges {
            checkpoint(control, work)?;
            increment(
                &mut outgoing[edge.source],
                "Euler degree exceeds UInt64 range",
            )?;
            increment(
                &mut incoming[edge.target],
                "Euler degree exceeds UInt64 range",
            )?;
            forward[edge.source].push(edge.target);
            reverse[edge.target].push(edge.source);
            weak[edge.source].push(edge.target);
            if edge.source != edge.target {
                weak[edge.target].push(edge.source);
            }
        }
        let mut active = Vec::new();
        reserve(&mut active, incoming.len(), "Euler active-node flags")?;
        active.extend(
            incoming
                .iter()
                .zip(&outgoing)
                .map(|(&incoming, &outgoing)| incoming > 0 || outgoing > 0),
        );
        let active_start = active.iter().position(|&value| value);
        let edgeless_start = (!self.nodes.is_empty()).then_some(0);

        let balanced = incoming == outgoing;
        let circuit_start = if balanced
            && reachable_all_active(&forward, &active, control, work)?
            && reachable_all_active(&reverse, &active, control, work)?
        {
            active_start.or(edgeless_start)
        } else {
            None
        };

        let mut path_start = None;
        let mut path_end = None;
        let mut path_degrees_valid = true;
        for node in 0..self.nodes.len() {
            checkpoint(control, work)?;
            match outgoing[node].cmp(&incoming[node]) {
                std::cmp::Ordering::Equal => {}
                std::cmp::Ordering::Greater
                    if incoming[node].checked_add(1) == Some(outgoing[node])
                        && path_start.is_none() =>
                {
                    path_start = Some(node);
                }
                std::cmp::Ordering::Less
                    if outgoing[node].checked_add(1) == Some(incoming[node])
                        && path_end.is_none() =>
                {
                    path_end = Some(node);
                }
                _ => path_degrees_valid = false,
            }
        }
        let path_start = if path_degrees_valid
            && ((path_start.is_none() && path_end.is_none())
                || (path_start.is_some() && path_end.is_some()))
            && reachable_all_active(&weak, &active, control, work)?
        {
            path_start.or(active_start).or(edgeless_start)
        } else {
            None
        };

        Ok(Classification {
            circuit_start,
            path_start,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Classification {
    circuit_start: Option<usize>,
    path_start: Option<usize>,
}

impl Classification {
    const fn undefined() -> Self {
        Self {
            circuit_start: None,
            path_start: None,
        }
    }
}

fn canonical_endpoints(source: [u8; 16], target: [u8; 16], directed: bool) -> ([u8; 16], [u8; 16]) {
    if !directed && target < source {
        (target, source)
    } else {
        (source, target)
    }
}

fn traversal_adjacency_counts(
    node_count: usize,
    edges: &[ProjectedEdge],
    directed: bool,
) -> Result<Vec<usize>, AlgorithmError> {
    let mut counts = zeroed_usize(node_count, "Euler adjacency entry counts")?;
    for edge in edges {
        increment_usize(
            &mut counts[edge.source],
            "Euler adjacency entry count exceeds platform range",
        )?;
        if !directed && edge.source != edge.target {
            increment_usize(
                &mut counts[edge.target],
                "Euler adjacency entry count exceeds platform range",
            )?;
        }
    }
    Ok(counts)
}

fn directed_adjacency_counts(
    node_count: usize,
    edges: &[ProjectedEdge],
    reverse: bool,
) -> Result<Vec<usize>, AlgorithmError> {
    let mut counts = zeroed_usize(node_count, "Euler directed adjacency entry counts")?;
    for edge in edges {
        let node = if reverse { edge.target } else { edge.source };
        increment_usize(
            &mut counts[node],
            "Euler adjacency entry count exceeds platform range",
        )?;
    }
    Ok(counts)
}

fn reachable_all_active(
    adjacency: &[Vec<usize>],
    active: &[bool],
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<bool, AlgorithmError> {
    let Some(start) = active.iter().position(|&value| value) else {
        return Ok(true);
    };
    let mut visited = Vec::new();
    reserve(
        &mut visited,
        adjacency.len(),
        "Euler connectivity visited flags",
    )?;
    visited.resize(adjacency.len(), false);
    let mut stack = Vec::new();
    reserve(&mut stack, adjacency.len(), "Euler connectivity stack")?;
    visited[start] = true;
    stack.push(start);
    while let Some(node) = stack.pop() {
        checkpoint(control, work)?;
        for &neighbor in &adjacency[node] {
            checkpoint(control, work)?;
            if !visited[neighbor] {
                visited[neighbor] = true;
                stack.push(neighbor);
            }
        }
    }
    Ok(active
        .iter()
        .enumerate()
        .all(|(node, &is_active)| !is_active || visited[node]))
}

fn checked_path_lengths(edge_count: usize) -> Result<(usize, usize), AlgorithmError> {
    let node_count = edge_count
        .checked_add(1)
        .ok_or_else(|| execution("Euler path length exceeds platform range"))?;
    Ok((node_count, edge_count))
}

fn increment(value: &mut u64, message: &'static str) -> Result<(), AlgorithmError> {
    *value = value.checked_add(1).ok_or_else(|| execution(message))?;
    Ok(())
}

fn increment_usize(value: &mut usize, message: &'static str) -> Result<(), AlgorithmError> {
    *value = value.checked_add(1).ok_or_else(|| execution(message))?;
    Ok(())
}

fn checkpoint(control: &AlgorithmControl, work: &mut usize) -> Result<(), AlgorithmError> {
    *work = work
        .checked_add(1)
        .ok_or_else(|| execution("Euler cooperative work count exceeds platform range"))?;
    if work.is_multiple_of(CHECKPOINT_INTERVAL) {
        control.checkpoint()?;
    } else {
        control.check_cancelled()?;
    }
    Ok(())
}

fn empty_adjacency(length: usize) -> Result<Vec<Vec<usize>>, AlgorithmError> {
    let mut adjacency = Vec::new();
    reserve(&mut adjacency, length, "Euler connectivity adjacency")?;
    adjacency.resize_with(length, Vec::new);
    Ok(adjacency)
}

fn zeroed(length: usize, context: &'static str) -> Result<Vec<u64>, AlgorithmError> {
    let mut values = Vec::new();
    reserve(&mut values, length, context)?;
    values.resize(length, 0);
    Ok(values)
}

fn zeroed_usize(length: usize, context: &'static str) -> Result<Vec<usize>, AlgorithmError> {
    let mut values = Vec::new();
    reserve(&mut values, length, context)?;
    values.resize(length, 0);
    Ok(values)
}

fn reserve<T>(
    values: &mut Vec<T>,
    additional: usize,
    context: &'static str,
) -> Result<(), AlgorithmError> {
    values
        .try_reserve_exact(additional)
        .map_err(|_| execution(format!("{context} allocation failed")))
}

fn checked_adjacency_entry_add(total: u64, additional: u64) -> Result<u64, AlgorithmError> {
    total
        .checked_add(additional)
        .ok_or_else(|| execution("Euler adjacency entry count exceeds UInt64 range"))
}

fn execution(message: impl Into<String>) -> AlgorithmError {
    AlgorithmError::Execution {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algorithm_dispatch::{AlgorithmCancellation, AlgorithmLimits, AlgorithmOutput};
    use crate::algorithm_output::shape_algorithm_output;
    use arrow::datatypes::{DataType, Field};
    use gf_core::algorithms::{Algorithm, AnalyzeAlgorithm};
    use std::sync::Arc;

    fn uuid(value: u8) -> [u8; 16] {
        [value; 16]
    }

    fn edge(id: u8, source: u8, target: u8) -> EulerEdge {
        EulerEdge {
            edge: uuid(id),
            source: uuid(source),
            target: uuid(target),
        }
    }

    fn control() -> AlgorithmControl {
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default())
    }

    fn projection(nodes: &[u8], edges: &[(u8, u8, u8)], directed: bool) -> EulerProjection {
        EulerProjection::new(
            &nodes.iter().copied().map(uuid).collect::<Vec<_>>(),
            &edges
                .iter()
                .map(|&(id, source, target)| edge(id, source, target))
                .collect::<Vec<_>>(),
            directed,
            &control(),
        )
        .unwrap()
    }

    fn trail(
        nodes: &[u8],
        edges: &[(u8, u8, u8)],
        directed: bool,
        kind: EulerTrailKind,
    ) -> EulerTrailOutcome {
        projection(nodes, edges, directed)
            .trail(kind, &control())
            .unwrap()
    }

    #[test]
    fn empty_and_edgeless_outcomes_are_distinct() {
        assert_eq!(
            trail(&[], &[], false, EulerTrailKind::Circuit),
            EulerTrailOutcome::EmptySelection
        );
        assert_eq!(
            trail(&[9, 2, 7], &[], false, EulerTrailKind::Path),
            EulerTrailOutcome::Trail(EulerTrail {
                node_path: vec![uuid(2)],
                edge_path: vec![],
            })
        );
    }

    #[test]
    fn directed_open_and_closed_trails_use_authoritative_starts() {
        assert_eq!(
            trail(
                &[4, 1, 3],
                &[(30, 1, 3), (20, 3, 4)],
                true,
                EulerTrailKind::Path,
            ),
            EulerTrailOutcome::Trail(EulerTrail {
                node_path: vec![uuid(1), uuid(3), uuid(4)],
                edge_path: vec![uuid(30), uuid(20)],
            })
        );
        assert_eq!(
            trail(
                &[4, 1, 3],
                &[(30, 1, 3), (20, 3, 4), (10, 4, 1)],
                true,
                EulerTrailKind::Circuit,
            ),
            EulerTrailOutcome::Trail(EulerTrail {
                node_path: vec![uuid(1), uuid(3), uuid(4), uuid(1)],
                edge_path: vec![uuid(30), uuid(20), uuid(10)],
            })
        );
    }

    #[test]
    fn undirected_open_and_closed_trails_consume_raw_edge_uuid_order() {
        assert_eq!(
            trail(
                &[0, 1, 2],
                &[(30, 0, 1), (10, 0, 2)],
                false,
                EulerTrailKind::Path,
            ),
            EulerTrailOutcome::Trail(EulerTrail {
                node_path: vec![uuid(1), uuid(0), uuid(2)],
                edge_path: vec![uuid(30), uuid(10)],
            })
        );
        assert_eq!(
            trail(
                &[0, 1, 2],
                &[(30, 0, 1), (10, 0, 2), (20, 1, 2)],
                false,
                EulerTrailKind::Circuit,
            ),
            EulerTrailOutcome::Trail(EulerTrail {
                node_path: vec![uuid(0), uuid(2), uuid(1), uuid(0)],
                edge_path: vec![uuid(10), uuid(20), uuid(30)],
            })
        );
    }

    #[test]
    fn loops_parallel_edges_and_mirrored_records_are_consumed_once() {
        let result = trail(
            &[0, 1],
            &[(20, 0, 0), (10, 0, 1), (10, 1, 0), (11, 0, 1), (11, 1, 0)],
            false,
            EulerTrailKind::Circuit,
        );
        let EulerTrailOutcome::Trail(result) = result else {
            panic!("expected a trail");
        };
        assert_eq!(result.node_path.len(), result.edge_path.len() + 1);
        assert_eq!(result.edge_path, [uuid(10), uuid(11), uuid(20)]);
        assert_eq!(
            result
                .edge_path
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            3
        );
    }

    #[test]
    fn predicates_and_construction_share_classification() {
        for (nodes, edges, directed, circuit, path) in [
            (vec![], vec![], false, true, true),
            (
                vec![0, 1, 2],
                vec![(1, 0, 1), (2, 1, 2)],
                false,
                false,
                true,
            ),
            (
                vec![0, 1, 2],
                vec![(1, 0, 1), (2, 1, 2), (3, 2, 0)],
                false,
                true,
                true,
            ),
            (
                vec![0, 1, 2, 3],
                vec![(1, 0, 1), (2, 2, 3)],
                false,
                false,
                false,
            ),
            (vec![0, 1, 2], vec![(1, 0, 1), (2, 1, 2)], true, false, true),
        ] {
            let projection = projection(&nodes, &edges, directed);
            assert_eq!(projection.has_circuit(&control()).unwrap(), circuit);
            assert_eq!(projection.has_path(&control()).unwrap(), path);
            if !nodes.is_empty() {
                assert_eq!(
                    projection
                        .trail(EulerTrailKind::Circuit, &control())
                        .is_ok(),
                    circuit
                );
                assert_eq!(
                    projection.trail(EulerTrailKind::Path, &control()).is_ok(),
                    path
                );
            }
        }
    }

    #[test]
    fn mathematically_invalid_requests_use_typed_undefined_errors() {
        let open = projection(&[0, 1, 2], &[(1, 0, 1), (2, 1, 2)], false);
        assert_eq!(
            open.trail(EulerTrailKind::Circuit, &control()),
            Err(AlgorithmError::UndefinedEulerCircuit)
        );

        let disconnected = projection(&[0, 1, 2, 3], &[(1, 0, 1), (2, 2, 3)], false);
        assert_eq!(
            disconnected.trail(EulerTrailKind::Path, &control()),
            Err(AlgorithmError::UndefinedEulerPath)
        );
    }

    #[test]
    fn malformed_public_identity_is_rejected_atomically() {
        for result in [
            EulerProjection::new(&[uuid(0), uuid(0)], &[], false, &control()),
            EulerProjection::new(&[uuid(0)], &[edge(1, 0, 2)], false, &control()),
            EulerProjection::new(
                &[uuid(0), uuid(1), uuid(2)],
                &[edge(1, 0, 1), edge(1, 0, 2)],
                false,
                &control(),
            ),
            EulerProjection::new(
                &[uuid(0), uuid(1)],
                &[edge(1, 0, 1), edge(1, 1, 0)],
                true,
                &control(),
            ),
        ] {
            assert!(matches!(result, Err(AlgorithmError::Execution { .. })));
        }
    }

    #[test]
    fn cancellation_output_iteration_and_graph_limits_are_atomic() {
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            EulerProjection::new(
                &[],
                &[],
                false,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation)
            ),
            Err(AlgorithmError::Cancelled)
        );

        let no_iterations = AlgorithmControl::new(
            AlgorithmLimits {
                iterations: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            EulerProjection::new(&[], &[], false, &no_iterations),
            Err(AlgorithmError::IterationLimit { .. })
        ));

        let node_limited = AlgorithmControl::new(
            AlgorithmLimits {
                nodes: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            EulerProjection::new(&[uuid(0)], &[], false, &node_limited),
            Err(AlgorithmError::NodeLimit { .. })
        ));

        let edge_limited = AlgorithmControl::new(
            AlgorithmLimits {
                edges: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            EulerProjection::new(&[uuid(0)], &[edge(1, 0, 0)], false, &edge_limited),
            Err(AlgorithmError::EdgeLimit { .. })
        ));

        let projection = projection(&[0], &[], false);
        let no_output = AlgorithmControl::new(
            AlgorithmLimits {
                output_rows: 0,
                ..AlgorithmLimits::default()
            },
            AlgorithmCancellation::default(),
        );
        assert!(matches!(
            projection.trail(EulerTrailKind::Path, &no_output),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        assert!(matches!(
            checked_path_lengths(usize::MAX),
            Err(AlgorithmError::Execution { .. })
        ));
        assert!(matches!(
            checked_adjacency_entry_add(u64::MAX, 1),
            Err(AlgorithmError::Execution { .. })
        ));
    }

    #[test]
    fn graph_edge_limit_counts_normalized_adjacency_entries_exactly() {
        let limited = |edges| {
            AlgorithmControl::new(
                AlgorithmLimits {
                    edges,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            )
        };

        assert!(
            EulerProjection::new(&[uuid(0), uuid(1)], &[edge(1, 0, 1)], true, &limited(1),).is_ok()
        );
        assert!(EulerProjection::new(&[uuid(0)], &[edge(1, 0, 0)], false, &limited(1),).is_ok());

        let nonloop =
            EulerProjection::new(&[uuid(0), uuid(1)], &[edge(1, 0, 1)], false, &limited(1));
        assert_eq!(
            nonloop,
            Err(AlgorithmError::EdgeLimit {
                observed: 2,
                limit: 1,
            })
        );
        assert!(
            EulerProjection::new(&[uuid(0), uuid(1)], &[edge(1, 0, 1)], false, &limited(2),)
                .is_ok()
        );

        assert!(
            EulerProjection::new(
                &[uuid(0), uuid(1)],
                &[edge(1, 0, 1), edge(1, 1, 0)],
                false,
                &limited(2),
            )
            .is_ok()
        );
    }

    #[test]
    fn canonical_arrow_schema_preserves_euler_identity_contract() {
        let uuid_list = DataType::List(Arc::new(Field::new(
            "item",
            DataType::FixedSizeBinary(16),
            false,
        )));

        for algorithm in [AnalyzeAlgorithm::EulerCircuit, AnalyzeAlgorithm::EulerPath] {
            let algorithm = Algorithm::Analyze(algorithm);
            let batch = shape_algorithm_output(
                algorithm,
                &AlgorithmOutput {
                    schema: algorithm.result_schema(),
                    rows: Vec::new(),
                },
            )
            .unwrap();
            let schema = batch.schema();
            assert_eq!(schema.fields().len(), 2);
            assert_eq!(schema.field(0).name(), "node_path");
            assert_eq!(schema.field(0).data_type(), &uuid_list);
            assert!(!schema.field(0).is_nullable());
            assert_eq!(schema.field(1).name(), "edge_path");
            assert_eq!(schema.field(1).data_type(), &uuid_list);
            assert!(!schema.field(1).is_nullable());
            assert_eq!(
                schema.metadata()["graphforge.algorithm"],
                algorithm.as_str()
            );
            assert_eq!(schema.metadata()["graphforge.verb"], "analyze");
            assert_eq!(
                schema.metadata()["graphforge.algorithm_schema_version"],
                "1"
            );
        }
    }
}
