//! Deterministic dense spectral kernel for Rust-owned cluster algorithms.
use std::collections::{BTreeSet, HashMap, VecDeque};

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};
use crate::algorithm_graph::AdjacencyGraph;

pub(crate) const MAX_SPECTRAL_NODES: usize = 4_096;
const EIGEN_TOLERANCE: f64 = 1e-12;

#[derive(Clone, Debug)]
pub(crate) struct DenseSymmetric {
    size: usize,
    values: Vec<f64>,
}

impl DenseSymmetric {
    fn zeroed(size: usize) -> Result<Self, AlgorithmError> {
        let len = size
            .checked_mul(size)
            .ok_or_else(|| execution("spectral matrix size overflows address space"))?;
        Ok(Self {
            size,
            values: vec![0.0; len],
        })
    }

    pub(crate) fn get(&self, row: usize, column: usize) -> f64 {
        self.values[row * self.size + column]
    }

    fn set(&mut self, row: usize, column: usize, value: f64) {
        self.values[row * self.size + column] = value;
    }
}

#[derive(Debug)]
pub(crate) struct SpectralGraph {
    adjacency: Vec<BTreeSet<usize>>,
    degree: Vec<f64>,
    edge_entries: f64,
}

impl SpectralGraph {
    pub(crate) fn from_graph(
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<Self, AlgorithmError> {
        control.checkpoint()?;
        let node_count = graph.node_ids().len();
        if node_count > MAX_SPECTRAL_NODES {
            return Err(AlgorithmError::NodeLimit {
                observed: node_count as u64,
                limit: MAX_SPECTRAL_NODES as u64,
            });
        }
        let indices: HashMap<_, _> = graph
            .node_ids()
            .iter()
            .enumerate()
            .map(|(index, &node)| (node, index))
            .collect();
        let mut adjacency = vec![BTreeSet::new(); node_count];
        let mut work = 0_usize;
        for (source, &node) in graph.node_ids().iter().enumerate() {
            for edge in graph.neighbors(node) {
                checkpoint_chunk(control, &mut work)?;
                let target = indices
                    .get(&edge.neighbor_id)
                    .copied()
                    .ok_or_else(|| execution("adjacency references an unselected node"))?;
                if source != target {
                    adjacency[source].insert(target);
                    adjacency[target].insert(source);
                }
            }
        }
        let degree = adjacency
            .iter()
            .map(|neighbors| count(neighbors.len()))
            .collect::<Result<_, _>>()?;
        let edge_entries = count(adjacency.iter().map(BTreeSet::len).sum())?;
        Ok(Self {
            adjacency,
            degree,
            edge_entries,
        })
    }

    pub(crate) fn components(
        &self,
        control: &AlgorithmControl,
    ) -> Result<Vec<Vec<usize>>, AlgorithmError> {
        let mut seen = vec![false; self.adjacency.len()];
        let mut components = Vec::new();
        for start in 0..seen.len() {
            if seen[start] {
                continue;
            }
            let mut component = Vec::new();
            let mut queue = VecDeque::from([start]);
            seen[start] = true;
            while let Some(node) = queue.pop_front() {
                control.checkpoint()?;
                component.push(node);
                for &neighbor in &self.adjacency[node] {
                    if !seen[neighbor] {
                        seen[neighbor] = true;
                        queue.push_back(neighbor);
                    }
                }
            }
            components.push(component);
        }
        Ok(components)
    }

    pub(crate) fn modularity_matrix(
        &self,
        community: &[usize],
        control: &AlgorithmControl,
    ) -> Result<DenseSymmetric, AlgorithmError> {
        control.checkpoint()?;
        let mut matrix = DenseSymmetric::zeroed(community.len())?;
        if self.edge_entries == 0.0 {
            return Ok(matrix);
        }
        let mut row_sums = vec![0.0; community.len()];
        let mut work = 0_usize;
        for (row, &source) in community.iter().enumerate() {
            for (column, &target) in community.iter().enumerate() {
                checkpoint_chunk(control, &mut work)?;
                let edge = f64::from(self.adjacency[source].contains(&target));
                let value = edge - self.degree[source] * self.degree[target] / self.edge_entries;
                matrix.set(row, column, value);
                row_sums[row] += value;
            }
        }
        for (index, row_sum) in row_sums.into_iter().enumerate() {
            matrix.set(index, index, matrix.get(index, index) - row_sum);
        }
        Ok(matrix)
    }

    fn split_gain(
        &self,
        community: &[usize],
        signs: &[f64],
        control: &AlgorithmControl,
    ) -> Result<f64, AlgorithmError> {
        if self.edge_entries == 0.0 {
            return Ok(0.0);
        }
        let mut quadratic = 0.0;
        let mut work = 0_usize;
        for (row, &source) in community.iter().enumerate() {
            for (column, &target) in community.iter().enumerate() {
                checkpoint_chunk(control, &mut work)?;
                let edge = f64::from(self.adjacency[source].contains(&target));
                let value = edge - self.degree[source] * self.degree[target] / self.edge_entries;
                quadratic += value * (signs[row] * signs[column] - 1.0);
            }
        }
        let gain = quadratic / (2.0 * self.edge_entries);
        if gain.is_finite() {
            Ok(gain)
        } else {
            Err(execution("spectral modularity gain is not finite"))
        }
    }
}

pub(crate) fn leading_eigenpair(
    matrix: DenseSymmetric,
    control: &AlgorithmControl,
) -> Result<Option<(f64, Vec<f64>)>, AlgorithmError> {
    let rotations = matrix
        .size
        .checked_mul(matrix.size)
        .and_then(|value| value.checked_mul(50))
        .ok_or_else(|| execution("spectral rotation limit overflows address space"))?;
    leading_eigenpair_bounded(matrix, control, rotations)
}

pub(crate) fn leading_eigenvector_communities(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<usize>, AlgorithmError> {
    leading_eigenvector_communities_with_progress(graph, control, || {})
}

fn leading_eigenvector_communities_with_progress(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
    mut progress: impl FnMut(),
) -> Result<Vec<usize>, AlgorithmError> {
    let spectral = SpectralGraph::from_graph(graph, control)?;
    let mut pending = spectral.components(control)?;
    let mut finished = Vec::new();
    while !pending.is_empty() {
        let community = pending.remove(0);
        let matrix = spectral.modularity_matrix(&community, control)?;
        progress();
        let Some((eigenvalue, vector)) = leading_eigenpair(matrix, control)? else {
            finished.push(community);
            continue;
        };
        let signs: Vec<_> = vector
            .into_iter()
            .map(|value| if value < 0.0 { -1.0 } else { 1.0 })
            .collect();
        let gain = spectral.split_gain(&community, &signs, control)?;
        let mut left = Vec::new();
        let mut right = Vec::new();
        for (&node, &sign) in community.iter().zip(&signs) {
            if sign > 0.0 {
                left.push(node);
            } else {
                right.push(node);
            }
        }
        if eigenvalue <= EIGEN_TOLERANCE
            || gain <= EIGEN_TOLERANCE
            || left.is_empty()
            || right.is_empty()
        {
            finished.push(community);
            continue;
        }
        control.checkpoint()?;
        pending.extend([left, right]);
        pending.sort_by_key(|members| members[0]);
    }
    finished.sort_by_key(|members| members[0]);
    let mut assignment = vec![0; spectral.adjacency.len()];
    for (community, members) in finished.into_iter().enumerate() {
        for node in members {
            assignment[node] = community;
        }
    }
    Ok(assignment)
}

fn leading_eigenpair_bounded(
    mut matrix: DenseSymmetric,
    control: &AlgorithmControl,
    max_rotations: usize,
) -> Result<Option<(f64, Vec<f64>)>, AlgorithmError> {
    let size = matrix.size;
    if size == 0 {
        return Ok(None);
    }
    if matrix.values.iter().any(|value| !value.is_finite()) {
        return Err(execution("spectral matrix contains a non-finite value"));
    }
    let mut vectors = DenseSymmetric::zeroed(size)?;
    for index in 0..size {
        vectors.set(index, index, 1.0);
    }
    let mut rotations = 0_usize;
    while let Some((pivot, column, magnitude)) = largest_off_diagonal(&matrix) {
        if magnitude <= EIGEN_TOLERANCE {
            break;
        }
        if rotations == max_rotations {
            return Err(control.non_convergence());
        }
        control.checkpoint()?;
        rotate(&mut matrix, &mut vectors, pivot, column);
        rotations += 1;
    }
    let column = (1..size).fold(0, |best, candidate| {
        if matrix.get(candidate, candidate) > matrix.get(best, best) {
            candidate
        } else {
            best
        }
    });
    let eigenvalue = matrix.get(column, column);
    let mut vector: Vec<_> = (0..size).map(|row| vectors.get(row, column)).collect();
    if vector
        .iter()
        .copied()
        .find(|value| value.abs() > EIGEN_TOLERANCE)
        .is_some_and(|value| value < 0.0)
    {
        for value in &mut vector {
            *value = -*value;
        }
    }
    if !eigenvalue.is_finite() || vector.iter().any(|value| !value.is_finite()) {
        return Err(execution("spectral eigenpair is not finite"));
    }
    Ok(Some((eigenvalue, vector)))
}

fn largest_off_diagonal(matrix: &DenseSymmetric) -> Option<(usize, usize, f64)> {
    let first = (matrix.size > 1).then(|| (0, 1, matrix.get(0, 1).abs()))?;
    Some(
        (0..matrix.size)
            .flat_map(|row| {
                ((row + 1)..matrix.size)
                    .map(move |column| (row, column, matrix.get(row, column).abs()))
            })
            .fold(first, |best, candidate| {
                if candidate.2 > best.2 {
                    candidate
                } else {
                    best
                }
            }),
    )
}

fn rotate(matrix: &mut DenseSymmetric, vectors: &mut DenseSymmetric, p: usize, q: usize) {
    let app = matrix.get(p, p);
    let aqq = matrix.get(q, q);
    let apq = matrix.get(p, q);
    let tau = (aqq - app) / (2.0 * apq);
    let tangent = if tau == 0.0 {
        1.0
    } else {
        tau.signum() / (tau.abs() + tau.hypot(1.0))
    };
    let cosine = 1.0 / tangent.hypot(1.0);
    let sine = tangent * cosine;
    for row in 0..matrix.size {
        if row != p && row != q {
            let arp = matrix.get(row, p);
            let arq = matrix.get(row, q);
            matrix.set(row, p, cosine * arp - sine * arq);
            matrix.set(p, row, matrix.get(row, p));
            matrix.set(row, q, sine * arp + cosine * arq);
            matrix.set(q, row, matrix.get(row, q));
        }
        let vrp = vectors.get(row, p);
        let vrq = vectors.get(row, q);
        vectors.set(row, p, cosine * vrp - sine * vrq);
        vectors.set(row, q, sine * vrp + cosine * vrq);
    }
    matrix.set(p, p, app - tangent * apq);
    matrix.set(q, q, aqq + tangent * apq);
    matrix.set(p, q, 0.0);
    matrix.set(q, p, 0.0);
}

fn checkpoint_chunk(control: &AlgorithmControl, work: &mut usize) -> Result<(), AlgorithmError> {
    *work += 1;
    if (*work).is_multiple_of(1_024) {
        control.checkpoint()?;
    }
    Ok(())
}

fn execution(message: &str) -> AlgorithmError {
    AlgorithmError::Execution {
        message: message.to_owned(),
    }
}

fn count(value: usize) -> Result<f64, AlgorithmError> {
    u32::try_from(value)
        .map(f64::from)
        .map_err(|_| execution("spectral graph count exceeds numeric range"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algorithm_dispatch::{AlgorithmCancellation, AlgorithmLimits};

    fn control(limits: AlgorithmLimits) -> AlgorithmControl {
        AlgorithmControl::new(limits, AlgorithmCancellation::default())
    }

    fn cancelled_control() -> AlgorithmControl {
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        AlgorithmControl::new(AlgorithmLimits::default(), cancellation)
    }

    #[test]
    fn generalized_modularity_matrix_is_hand_verifiable() {
        let graph = AdjacencyGraph::with_test_directed_edges(4, &[(0, 1), (2, 3)]);
        let setup = control(AlgorithmLimits::default());
        let spectral = SpectralGraph::from_graph(&graph, &setup).unwrap();
        let matrix = spectral.modularity_matrix(&[0, 1, 2, 3], &setup).unwrap();
        assert_eq!(
            matrix.values,
            [
                -0.25, 0.75, -0.25, -0.25, 0.75, -0.25, -0.25, -0.25, -0.25, -0.25, -0.25, 0.75,
                -0.25, -0.25, 0.75, -0.25,
            ]
        );
        let edgeless =
            SpectralGraph::from_graph(&AdjacencyGraph::with_test_edges(2, &[]), &setup).unwrap();
        assert_eq!(
            edgeless.modularity_matrix(&[0, 1], &setup).unwrap().values,
            [0.0; 4]
        );
        assert!(matches!(
            DenseSymmetric::zeroed(usize::MAX),
            Err(AlgorithmError::Execution { .. })
        ));
    }

    #[test]
    fn projection_normalizes_direction_multiplicity_loops_and_boundaries() {
        let graph =
            AdjacencyGraph::with_test_directed_edges(5, &[(0, 1), (1, 0), (0, 1), (0, 0), (2, 3)]);
        let setup = control(AlgorithmLimits::default());
        let spectral = SpectralGraph::from_graph(&graph, &setup).unwrap();
        assert_eq!(
            spectral.components(&setup).unwrap(),
            [vec![0, 1], vec![2, 3], vec![4]]
        );
        assert_eq!(
            SpectralGraph::from_graph(&AdjacencyGraph::default(), &setup)
                .unwrap()
                .components(&setup)
                .unwrap(),
            Vec::<Vec<usize>>::new()
        );
        assert!(matches!(
            SpectralGraph::from_graph(&AdjacencyGraph::with_test_edges(4_097, &[]), &setup),
            Err(AlgorithmError::NodeLimit {
                observed: 4_097,
                limit: 4_096
            })
        ));
    }

    #[test]
    fn projection_observes_shared_limits_and_cancellation() {
        assert!(matches!(
            SpectralGraph::from_graph(
                &AdjacencyGraph::default(),
                &control(AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                })
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        assert!(matches!(
            SpectralGraph::from_graph(&AdjacencyGraph::default(), &cancelled_control()),
            Err(AlgorithmError::Cancelled)
        ));
        let graph = SpectralGraph::from_graph(
            &AdjacencyGraph::with_test_edges(1, &[]),
            &control(AlgorithmLimits::default()),
        )
        .unwrap();
        assert_eq!(
            graph.components(&cancelled_control()),
            Err(AlgorithmError::Cancelled)
        );
        assert!(matches!(
            graph.modularity_matrix(&[0], &cancelled_control()),
            Err(AlgorithmError::Cancelled)
        ));
    }

    #[test]
    fn jacobi_returns_the_stable_hand_verifiable_leading_pair() {
        let setup = control(AlgorithmLimits::default());
        let graph = AdjacencyGraph::with_test_edges(4, &[(0, 1), (2, 3)]);
        let spectral = SpectralGraph::from_graph(&graph, &setup).unwrap();
        let matrix = spectral.modularity_matrix(&[0, 1, 2, 3], &setup).unwrap();
        let (eigenvalue, vector) = leading_eigenpair(matrix, &setup).unwrap().unwrap();
        assert!((eigenvalue - 1.0).abs() <= EIGEN_TOLERANCE);
        assert!(vector[0] > 0.0 && vector[1] > 0.0);
        assert!(vector[2] < 0.0 && vector[3] < 0.0);

        let tied = DenseSymmetric {
            size: 2,
            values: vec![1.0, 0.0, 0.0, 1.0],
        };
        assert_eq!(
            leading_eigenpair(tied, &setup).unwrap(),
            Some((1.0, vec![1.0, 0.0]))
        );
    }

    #[test]
    fn jacobi_reports_limits_cancellation_nonconvergence_and_numeric_errors() {
        let matrix = DenseSymmetric {
            size: 2,
            values: vec![0.0, 1.0, 1.0, 0.0],
        };
        assert!(matches!(
            leading_eigenpair(
                matrix.clone(),
                &control(AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                })
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        assert!(matches!(
            leading_eigenpair_bounded(matrix.clone(), &control(AlgorithmLimits::default()), 0),
            Err(AlgorithmError::NonConvergence { .. })
        ));
        assert_eq!(
            leading_eigenpair(matrix, &cancelled_control()),
            Err(AlgorithmError::Cancelled)
        );
        assert!(matches!(
            leading_eigenpair(
                DenseSymmetric {
                    size: 1,
                    values: vec![f64::NAN]
                },
                &control(AlgorithmLimits::default())
            ),
            Err(AlgorithmError::Execution { .. })
        ));
    }

    #[test]
    fn recursive_splits_find_stable_hand_verifiable_communities() {
        let graph = AdjacencyGraph::with_test_edges(
            7,
            &[(0, 1), (1, 2), (2, 0), (2, 3), (3, 4), (4, 5), (5, 3)],
        );
        let setup = control(AlgorithmLimits::default());
        let first = leading_eigenvector_communities(&graph, &setup).unwrap();
        assert_eq!(first, [0, 0, 0, 1, 1, 1, 2]);
        assert_eq!(
            leading_eigenvector_communities(&graph, &control(AlgorithmLimits::default())).unwrap(),
            first
        );
    }

    #[test]
    fn recursive_splits_observe_boundaries_limits_and_midflight_cancellation() {
        assert_eq!(
            leading_eigenvector_communities(
                &AdjacencyGraph::default(),
                &control(AlgorithmLimits::default())
            )
            .unwrap(),
            Vec::<usize>::new()
        );
        assert_eq!(
            leading_eigenvector_communities(
                &AdjacencyGraph::with_test_edges(3, &[]),
                &control(AlgorithmLimits::default())
            )
            .unwrap(),
            [0, 1, 2]
        );
        assert!(matches!(
            leading_eigenvector_communities(
                &AdjacencyGraph::with_test_edges(2, &[(0, 1)]),
                &control(AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                })
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        let graph = AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 2), (2, 3)]);
        let cancellation = AlgorithmCancellation::default();
        let cancel = cancellation.clone();
        assert_eq!(
            leading_eigenvector_communities_with_progress(
                &graph,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
                || cancel.cancel(),
            ),
            Err(AlgorithmError::Cancelled)
        );
    }
}
