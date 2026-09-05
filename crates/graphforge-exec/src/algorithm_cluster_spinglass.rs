//! Deterministic Potts-energy kernel for Rust-owned Spinglass clustering.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

use crate::algorithm_dispatch::{AlgorithmControl, AlgorithmError};
use crate::algorithm_graph::AdjacencyGraph;

pub(crate) const MAX_SPINGLASS_NODES: usize = 4_096;
pub(crate) const MAX_SPINS: usize = 25;
const START_TEMPERATURE: f64 = 1.0;
const STOP_TEMPERATURE: f64 = 0.01;
const COOLING_FACTOR: f64 = 0.99;
const ENERGY_TOLERANCE: f64 = 1e-12;
const RANDOM_SEED: u64 = 0x0053_5049_4e01;

#[derive(Debug)]
pub(crate) struct SpinglassGraph {
    adjacency: Vec<BTreeSet<usize>>,
    degree: Vec<f64>,
}

impl SpinglassGraph {
    pub(crate) fn from_graph(
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<Self, AlgorithmError> {
        control.checkpoint()?;
        let node_count = graph.node_ids().len();
        if node_count > MAX_SPINGLASS_NODES {
            return Err(AlgorithmError::NodeLimit {
                observed: node_count as u64,
                limit: MAX_SPINGLASS_NODES as u64,
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
        Ok(Self { adjacency, degree })
    }

    fn anneal_component(
        &self,
        component: &[usize],
        control: &AlgorithmControl,
        random: &mut u64,
        progress: &mut impl FnMut(),
    ) -> Result<Vec<usize>, AlgorithmError> {
        let spin_count = component.len().min(MAX_SPINS);
        let mut spins = vec![0_usize; self.adjacency.len()];
        for (position, &node) in component.iter().enumerate() {
            spins[node] = position % spin_count;
        }
        let volume = component.iter().map(|&node| self.degree[node]).sum();
        let mut current_energy = self.energy(component, &spins, spin_count, control)?;
        let mut best_energy = current_energy;
        let mut work = 0_usize;
        let mut best = canonical_assignment(component, &spins, control, &mut work)?;
        let mut order = component.to_vec();
        let mut temperature = START_TEMPERATURE;

        while temperature >= STOP_TEMPERATURE {
            progress();
            control.checkpoint()?;
            shuffle(&mut order, random, control, &mut work)?;
            for &node in &order {
                checkpoint_chunk(control, &mut work)?;
                let old_spin = spins[node];
                let choice = random_index(random, spin_count - 1)?;
                let new_spin = if choice >= old_spin {
                    choice + 1
                } else {
                    choice
                };
                let delta = self.move_delta_inner(
                    component, &spins, node, new_spin, volume, control, &mut work,
                )?;
                let prior = (delta.abs() <= ENERGY_TOLERANCE)
                    .then(|| canonical_assignment(component, &spins, control, &mut work))
                    .transpose()?;
                spins[node] = new_spin;
                let candidate = canonical_assignment(component, &spins, control, &mut work)?;
                let accept = if delta < -ENERGY_TOLERANCE {
                    true
                } else if delta.abs() <= ENERGY_TOLERANCE {
                    candidate < prior.expect("energy tie has a prior partition")
                } else {
                    next_unit(random) < (-delta / temperature).exp()
                };
                if !accept {
                    spins[node] = old_spin;
                    continue;
                }
                current_energy = finite(
                    current_energy + delta,
                    "Spinglass annealing energy is not finite",
                )?;
                if current_energy < best_energy - ENERGY_TOLERANCE
                    || ((current_energy - best_energy).abs() <= ENERGY_TOLERANCE
                        && candidate < best)
                {
                    best_energy = current_energy;
                    best = candidate;
                }
            }
            let next = temperature * COOLING_FACTOR;
            if !next.is_finite() || next >= temperature {
                return Err(control.non_convergence());
            }
            temperature = next;
        }
        Ok(best)
    }

    #[allow(clippy::too_many_arguments)]
    fn move_delta_inner(
        &self,
        component: &[usize],
        spins: &[usize],
        node: usize,
        new_spin: usize,
        volume: f64,
        control: &AlgorithmControl,
        work: &mut usize,
    ) -> Result<f64, AlgorithmError> {
        let old_spin = spins[node];
        let mut delta = 0.0;
        for &other in component {
            if other == node {
                continue;
            }
            checkpoint_chunk(control, work)?;
            let coupling = self.coupling(node, other, volume);
            if spins[other] == old_spin {
                delta += coupling;
            }
            if spins[other] == new_spin {
                delta -= coupling;
            }
        }
        finite(delta, "Spinglass move energy is not finite")
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

    pub(crate) fn energy(
        &self,
        component: &[usize],
        spins: &[usize],
        spin_count: usize,
        control: &AlgorithmControl,
    ) -> Result<f64, AlgorithmError> {
        self.validate(component, spins, spin_count)?;
        control.checkpoint()?;
        let volume = component.iter().map(|&node| self.degree[node]).sum::<f64>();
        if volume == 0.0 {
            return Ok(0.0);
        }
        let mut energy = 0.0;
        let mut work = 0_usize;
        for (offset, &source) in component.iter().enumerate() {
            for &target in &component[(offset + 1)..] {
                checkpoint_chunk(control, &mut work)?;
                if spins[source] == spins[target] {
                    energy -= self.coupling(source, target, volume);
                }
            }
        }
        finite(energy, "Spinglass energy is not finite")
    }

    #[cfg(test)]
    pub(crate) fn move_delta(
        &self,
        component: &[usize],
        spins: &[usize],
        node: usize,
        new_spin: usize,
        spin_count: usize,
        control: &AlgorithmControl,
    ) -> Result<f64, AlgorithmError> {
        self.validate(component, spins, spin_count)?;
        control.checkpoint()?;
        if !component.contains(&node) || new_spin >= spin_count {
            return Err(execution(
                "Spinglass move references an invalid spin or node",
            ));
        }
        let old_spin = spins[node];
        if old_spin == new_spin {
            return Ok(0.0);
        }
        let volume = component
            .iter()
            .map(|&member| self.degree[member])
            .sum::<f64>();
        if volume == 0.0 {
            return Ok(0.0);
        }
        let mut delta = 0.0;
        let mut work = 0_usize;
        for &other in component {
            if other == node {
                continue;
            }
            checkpoint_chunk(control, &mut work)?;
            let coupling = self.coupling(node, other, volume);
            if spins[other] == old_spin {
                delta += coupling;
            }
            if spins[other] == new_spin {
                delta -= coupling;
            }
        }
        finite(delta, "Spinglass move energy is not finite")
    }

    fn coupling(&self, source: usize, target: usize, volume: f64) -> f64 {
        f64::from(self.adjacency[source].contains(&target))
            - self.degree[source] * self.degree[target] / volume
    }

    fn validate(
        &self,
        component: &[usize],
        spins: &[usize],
        spin_count: usize,
    ) -> Result<(), AlgorithmError> {
        if spins.len() != self.adjacency.len()
            || spin_count == 0
            || spin_count > MAX_SPINS
            || component.iter().any(|&node| node >= spins.len())
            || spins.iter().any(|&spin| spin >= spin_count)
        {
            return Err(execution("Spinglass spin assignment is invalid"));
        }
        Ok(())
    }
}

pub(crate) fn spinglass_communities(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<usize>, AlgorithmError> {
    spinglass_communities_with_progress(graph, control, || {})
}

fn spinglass_communities_with_progress(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
    mut progress: impl FnMut(),
) -> Result<Vec<usize>, AlgorithmError> {
    let projected = SpinglassGraph::from_graph(graph, control)?;
    let mut result = vec![0_usize; graph.node_ids().len()];
    let mut next_community = 0_usize;
    let mut random = RANDOM_SEED;
    for mut component in projected.components(control)? {
        component.sort_unstable();
        if component.len() == 1 {
            result[component[0]] = next_community;
            next_community += 1;
            continue;
        }
        let partition =
            projected.anneal_component(&component, control, &mut random, &mut progress)?;
        for (&node, community) in component.iter().zip(&partition) {
            result[node] = next_community + community;
        }
        next_community += partition.iter().copied().max().unwrap_or(0) + 1;
    }
    Ok(result)
}

fn canonical_assignment(
    component: &[usize],
    spins: &[usize],
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<Vec<usize>, AlgorithmError> {
    let mut ids = BTreeMap::new();
    let mut assignment = Vec::with_capacity(component.len());
    for &node in component {
        checkpoint_chunk(control, work)?;
        let next = ids.len();
        assignment.push(*ids.entry(spins[node]).or_insert(next));
    }
    Ok(assignment)
}

fn shuffle(
    values: &mut [usize],
    random: &mut u64,
    control: &AlgorithmControl,
    work: &mut usize,
) -> Result<(), AlgorithmError> {
    for end in (1..values.len()).rev() {
        checkpoint_chunk(control, work)?;
        values.swap(end, random_index(random, end + 1)?);
    }
    Ok(())
}

fn random_index(random: &mut u64, upper: usize) -> Result<usize, AlgorithmError> {
    let upper = u64::try_from(upper).map_err(|_| execution("spin choice exceeds UInt64 range"))?;
    let threshold = upper.wrapping_neg() % upper;
    loop {
        let value = next_random(random);
        if value >= threshold {
            return usize::try_from(value % upper)
                .map_err(|_| execution("spin choice exceeds platform range"));
        }
    }
}

fn next_unit(random: &mut u64) -> f64 {
    let value = next_random(random);
    let high = u32::try_from(value >> 32).expect("upper random bits fit UInt32");
    let low = u32::try_from((value >> 11) & 0x1f_ffff).expect("lower random bits fit UInt32");
    (f64::from(high) * 2_097_152.0 + f64::from(low)) / 9_007_199_254_740_992.0
}

fn next_random(random: &mut u64) -> u64 {
    *random = random.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut value = *random;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn checkpoint_chunk(control: &AlgorithmControl, work: &mut usize) -> Result<(), AlgorithmError> {
    *work += 1;
    if *work == 1_024 {
        control.checkpoint()?;
        *work = 0;
    }
    Ok(())
}

fn count(value: usize) -> Result<f64, AlgorithmError> {
    u32::try_from(value)
        .map(f64::from)
        .map_err(|_| execution("Spinglass graph count exceeds numeric range"))
}

fn finite(value: f64, message: &str) -> Result<f64, AlgorithmError> {
    value
        .is_finite()
        .then_some(value)
        .ok_or_else(|| execution(message))
}

fn execution(message: &str) -> AlgorithmError {
    AlgorithmError::Execution {
        message: message.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algorithm_dispatch::{AlgorithmCancellation, AlgorithmLimits};

    fn control(limits: AlgorithmLimits) -> AlgorithmControl {
        AlgorithmControl::new(limits, AlgorithmCancellation::default())
    }

    fn run(graph: &AdjacencyGraph) -> Vec<usize> {
        spinglass_communities(graph, &control(AlgorithmLimits::default())).unwrap()
    }

    #[test]
    fn potts_energy_and_move_delta_are_hand_verifiable() {
        let setup = control(AlgorithmLimits::default());
        let graph = SpinglassGraph::from_graph(
            &AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]),
            &setup,
        )
        .unwrap();
        assert_eq!(graph.coupling(0, 2, 4.0), -0.25);
        assert_eq!(
            graph.energy(&[0, 1, 2], &[0, 1, 2], 3, &setup).unwrap(),
            0.0
        );
        assert_eq!(
            graph
                .move_delta(&[0, 1, 2], &[0, 1, 2], 0, 1, 3, &setup)
                .unwrap(),
            -0.5
        );
        assert_eq!(
            graph.energy(&[0, 1, 2], &[1, 1, 2], 3, &setup).unwrap(),
            -0.5
        );
    }

    #[test]
    fn projection_normalizes_topology_and_boundaries() {
        let setup = control(AlgorithmLimits::default());
        let graph = SpinglassGraph::from_graph(
            &AdjacencyGraph::with_test_directed_edges(5, &[(0, 1), (1, 0), (0, 1), (0, 0), (2, 3)]),
            &setup,
        )
        .unwrap();
        assert_eq!(
            graph.components(&setup).unwrap(),
            [vec![0, 1], vec![2, 3], vec![4]]
        );
        let empty = SpinglassGraph::from_graph(&AdjacencyGraph::default(), &setup).unwrap();
        assert!(empty.components(&setup).unwrap().is_empty());
        assert!(matches!(
            SpinglassGraph::from_graph(&AdjacencyGraph::with_test_edges(4_097, &[]), &setup),
            Err(AlgorithmError::NodeLimit {
                observed: 4_097,
                limit: 4_096
            })
        ));
    }

    #[test]
    fn controls_and_invalid_spins_are_structured() {
        assert!(matches!(
            SpinglassGraph::from_graph(
                &AdjacencyGraph::default(),
                &control(AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                })
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert!(matches!(
            SpinglassGraph::from_graph(
                &AdjacencyGraph::default(),
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation)
            ),
            Err(AlgorithmError::Cancelled)
        ));
        let setup = control(AlgorithmLimits::default());
        let mut graph =
            SpinglassGraph::from_graph(&AdjacencyGraph::with_test_edges(2, &[(0, 1)]), &setup)
                .unwrap();
        let invalid = graph.energy(&[0, 1], &[0], 2, &setup);
        assert!(matches!(invalid, Err(AlgorithmError::Execution { .. })));
        graph.degree[0] = f64::NAN;
        let non_finite = graph.energy(&[0, 1], &[0, 0], 2, &setup);
        assert!(matches!(non_finite, Err(AlgorithmError::Execution { .. })));
    }

    #[test]
    fn annealing_finds_the_stable_two_community_partition() {
        let graph = AdjacencyGraph::with_test_edges(
            7,
            &[(0, 1), (1, 2), (2, 0), (2, 3), (3, 4), (4, 5), (5, 3)],
        );
        let first = run(&graph);
        assert_eq!(first, [0, 0, 0, 1, 1, 1, 2]);
        assert_eq!(run(&graph), first);
    }

    #[test]
    fn annealing_keeps_components_and_edgeless_nodes_separate() {
        assert!(run(&AdjacencyGraph::default()).is_empty());
        assert_eq!(run(&AdjacencyGraph::with_test_edges(3, &[])), [0, 1, 2]);
        let disconnected =
            AdjacencyGraph::with_test_edges(7, &[(0, 1), (1, 2), (2, 0), (3, 4), (4, 5), (5, 3)]);
        assert_eq!(run(&disconnected), [0, 0, 0, 1, 1, 1, 2]);
    }

    #[test]
    fn annealing_freezes_schedule_randomness_and_cooperative_controls() {
        let mut random = RANDOM_SEED;
        assert_eq!(next_random(&mut random), 0x6f17_d7d5_5f74_8a26);
        assert_eq!(next_random(&mut random), 0x34bd_c8b9_7dee_df7b);
        assert_eq!(next_random(&mut random), 0x0471_0826_0bbc_7296);

        let edge = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
        let mut sweeps = 0;
        spinglass_communities_with_progress(&edge, &control(AlgorithmLimits::default()), || {
            sweeps += 1
        })
        .unwrap();
        assert_eq!(sweeps, 459);
        assert!(matches!(
            spinglass_communities(
                &edge,
                &control(AlgorithmLimits {
                    iterations: 4,
                    ..AlgorithmLimits::default()
                })
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));

        let cancellation = AlgorithmCancellation::default();
        let cancel = cancellation.clone();
        assert_eq!(
            spinglass_communities_with_progress(
                &edge,
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
                || cancel.cancel(),
            ),
            Err(AlgorithmError::Cancelled)
        );
    }
}
