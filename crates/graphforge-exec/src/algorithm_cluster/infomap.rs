//! Infomap execution.

use super::{
    AdjacencyGraph, Algorithm, AlgorithmCapability, AlgorithmControl, AlgorithmError,
    AlgorithmOutput, BTreeMap, BTreeSet, BUILTIN_REVIEW, ClusterAlgorithm, HashMap, RustAlgorithm,
    VecDeque, canonicalize_partition, checkpoint_chunk, community_output, execution,
};

pub(super) struct InfoMap;

type SimpleAdjacency = Vec<BTreeSet<usize>>;

impl RustAlgorithm for InfoMap {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Cluster(ClusterAlgorithm::InfoMap),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let communities = infomap_communities(graph, control)?;
        community_output(graph, &communities, ClusterAlgorithm::InfoMap, control)
    }
}

#[derive(Debug)]
struct InfomapFlow {
    outgoing: SimpleAdjacency,
    incident: SimpleAdjacency,
    components: Vec<Vec<usize>>,
    directed: bool,
}

fn infomap_flow(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<InfomapFlow, AlgorithmError> {
    let node_count = graph.node_ids().len();
    let indices: HashMap<_, _> = graph
        .node_ids()
        .iter()
        .enumerate()
        .map(|(index, &node)| (node, index))
        .collect();
    let mut outgoing = vec![BTreeSet::new(); node_count];
    let mut incident = vec![BTreeSet::new(); node_count];
    let mut work = 0_usize;
    for (source, &node) in graph.node_ids().iter().enumerate() {
        for edge in graph.neighbors(node) {
            checkpoint_chunk(control, &mut work)?;
            let target = indices
                .get(&edge.neighbor_id)
                .copied()
                .ok_or_else(|| execution("adjacency references an unselected node"))?;
            if source != target && outgoing[source].insert(target) {
                incident[source].insert(target);
                incident[target].insert(source);
            }
        }
    }
    let directed = graph.is_directed();
    let mut seen = vec![false; node_count];
    let mut components = Vec::new();
    for start in 0..node_count {
        if seen[start] {
            continue;
        }
        let mut component = Vec::new();
        let mut queue = VecDeque::from([start]);
        seen[start] = true;
        while let Some(node) = queue.pop_front() {
            checkpoint_chunk(control, &mut work)?;
            component.push(node);
            for &neighbor in &incident[node] {
                if !seen[neighbor] {
                    seen[neighbor] = true;
                    queue.push_back(neighbor);
                }
            }
        }
        components.push(component);
    }
    Ok(InfomapFlow {
        outgoing,
        incident,
        components,
        directed,
    })
}

fn infomap_communities(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<usize>, AlgorithmError> {
    let flow = infomap_flow(graph, control)?;
    let mut assignment: Vec<_> = (0..graph.node_ids().len()).collect();
    for component in &flow.components {
        if component.len() == 1 {
            continue;
        }
        let visits = infomap_stationary(component, &flow.outgoing, flow.directed, control)?;
        let search = InfomapSearch {
            component,
            flow: &flow,
            visits: &visits,
        };
        loop {
            control.checkpoint()?;
            let moved = search.node_sweep(&mut assignment, control, || {})?;
            infomap_representative_labels(component, &mut assignment);
            let merged = search.module_merge(&mut assignment, control)?;
            if !moved && !merged {
                break;
            }
        }
    }
    canonicalize_partition(&mut assignment, control)?;
    Ok(assignment)
}

struct InfomapSearch<'a> {
    component: &'a [usize],
    flow: &'a InfomapFlow,
    visits: &'a [f64],
}

impl InfomapSearch<'_> {
    fn score(&self, assignment: &[usize]) -> Result<f64, AlgorithmError> {
        infomap_codelength(
            self.component,
            &self.flow.outgoing,
            self.visits,
            self.flow.directed,
            assignment,
        )
    }

    fn node_sweep(
        &self,
        assignment: &mut [usize],
        control: &AlgorithmControl,
        mut progress: impl FnMut(),
    ) -> Result<bool, AlgorithmError> {
        let mut changed = false;
        let mut work = 0_usize;
        for &node in self.component {
            let current = assignment[node];
            let current_score = self.score(assignment)?;
            let mut counts = BTreeMap::new();
            for &member in self.component {
                *counts.entry(assignment[member]).or_insert(0_usize) += 1;
            }
            let mut candidates: BTreeSet<_> = self.flow.incident[node]
                .iter()
                .map(|&neighbor| assignment[neighbor])
                .collect();
            if counts[&current] > 1
                && let Some(empty) = self
                    .component
                    .iter()
                    .copied()
                    .find(|label| !counts.contains_key(label))
            {
                candidates.insert(empty);
            }
            let mut best = (current_score, usize::MAX, current);
            for candidate in candidates {
                if candidate == current {
                    continue;
                }
                assignment[node] = candidate;
                let score = self.score(assignment)?;
                let representative = self
                    .component
                    .iter()
                    .copied()
                    .filter(|&member| assignment[member] == candidate)
                    .min()
                    .unwrap_or(node);
                if score < best.0 - 1e-12
                    || ((score - best.0).abs() <= 1e-12 && representative < best.1)
                {
                    best = (score, representative, candidate);
                }
                progress();
                checkpoint_chunk(control, &mut work)?;
            }
            assignment[node] = current;
            if best.0 < current_score - 1e-12 {
                assignment[node] = best.2;
                changed = true;
            }
        }
        Ok(changed)
    }

    fn module_merge(
        &self,
        assignment: &mut [usize],
        control: &AlgorithmControl,
    ) -> Result<bool, AlgorithmError> {
        let current = self.score(assignment)?;
        let mut pairs = BTreeSet::new();
        for &node in self.component {
            for &neighbor in &self.flow.incident[node] {
                let pair = (
                    assignment[node].min(assignment[neighbor]),
                    assignment[node].max(assignment[neighbor]),
                );
                if pair.0 != pair.1 {
                    pairs.insert(pair);
                }
            }
        }
        let mut best = (current, (usize::MAX, usize::MAX), None);
        let mut work = 0_usize;
        for (left, right) in pairs {
            let moved: Vec<_> = self
                .component
                .iter()
                .copied()
                .filter(|&node| assignment[node] == right)
                .collect();
            for &node in &moved {
                assignment[node] = left;
            }
            let score = self.score(assignment)?;
            for &node in &moved {
                assignment[node] = right;
            }
            if score < best.0 - 1e-12 || ((score - best.0).abs() <= 1e-12 && (left, right) < best.1)
            {
                best = (score, (left, right), Some((left, right)));
            }
            checkpoint_chunk(control, &mut work)?;
        }
        let Some((left, right)) = best.2.filter(|_| best.0 < current - 1e-12) else {
            return Ok(false);
        };
        for &node in self.component {
            if assignment[node] == right {
                assignment[node] = left;
            }
        }
        Ok(true)
    }
}

fn infomap_representative_labels(component: &[usize], assignment: &mut [usize]) {
    let mut representatives = BTreeMap::new();
    for &node in component {
        representatives
            .entry(assignment[node])
            .and_modify(|representative: &mut usize| *representative = (*representative).min(node))
            .or_insert(node);
    }
    for &node in component {
        assignment[node] = representatives[&assignment[node]];
    }
}

fn infomap_stationary(
    component: &[usize],
    outgoing: &SimpleAdjacency,
    directed: bool,
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let mut visits = vec![0.0; outgoing.len()];
    if !directed {
        let total: usize = component.iter().map(|&node| outgoing[node].len()).sum();
        let total = infomap_count(total, "component edge-entry count")?;
        for &node in component {
            visits[node] = infomap_count(outgoing[node].len(), "node degree")? / total;
        }
        return Ok(visits);
    }
    let size = infomap_count(component.len(), "component node count")?;
    for &node in component {
        visits[node] = 1.0 / size;
    }
    for iteration in 1..=1_000 {
        control.checkpoint()?;
        let dangling: f64 = component
            .iter()
            .filter(|&&node| outgoing[node].is_empty())
            .map(|&node| visits[node])
            .sum();
        let base = (0.15 + 0.85 * dangling) / size;
        let mut next = vec![0.0; outgoing.len()];
        for &node in component {
            next[node] = base;
        }
        for &source in component {
            if !outgoing[source].is_empty() {
                let degree = infomap_count(outgoing[source].len(), "node outdegree")?;
                let share = 0.85 * visits[source] / degree;
                for &target in &outgoing[source] {
                    next[target] += share;
                }
            }
        }
        let delta: f64 = component
            .iter()
            .map(|&node| (next[node] - visits[node]).abs())
            .sum();
        visits = next;
        if delta <= 1e-12 {
            return Ok(visits);
        }
        if iteration == 1_000 {
            return Err(AlgorithmError::NonConvergence { iterations: 1_000 });
        }
    }
    unreachable!()
}

fn infomap_codelength(
    component: &[usize],
    outgoing: &SimpleAdjacency,
    visits: &[f64],
    directed: bool,
    assignment: &[usize],
) -> Result<f64, AlgorithmError> {
    let mut module_visits = BTreeMap::new();
    let mut sizes = BTreeMap::new();
    for &node in component {
        *module_visits.entry(assignment[node]).or_insert(0.0) += visits[node];
        *sizes.entry(assignment[node]).or_insert(0_usize) += 1;
    }
    let mut exits = BTreeMap::new();
    let size = infomap_count(component.len(), "component node count")?;
    for &source in component {
        let module = assignment[source];
        let external = infomap_count(
            outgoing[source]
                .iter()
                .filter(|&&target| assignment[target] != module)
                .count(),
            "external edge count",
        )?;
        let outside = infomap_count(
            component.len() - sizes[&module],
            "outside-module node count",
        )? / size;
        let exit_probability = if outgoing[source].is_empty() {
            outside
        } else if directed {
            0.15 * outside
                + 0.85 * external / infomap_count(outgoing[source].len(), "node outdegree")?
        } else {
            external / infomap_count(outgoing[source].len(), "node degree")?
        };
        *exits.entry(module).or_insert(0.0) += visits[source] * exit_probability;
    }
    let exit_total: f64 = exits.values().sum();
    let codelength = xlogx(exit_total)
        - 2.0 * exits.values().copied().map(xlogx).sum::<f64>()
        - component
            .iter()
            .map(|&node| xlogx(visits[node]))
            .sum::<f64>()
        + module_visits
            .iter()
            .map(|(module, visit)| xlogx(visit + exits.get(module).copied().unwrap_or(0.0)))
            .sum::<f64>();
    if codelength.is_finite() {
        Ok(codelength)
    } else {
        Err(execution("Infomap codelength is not finite"))
    }
}

fn xlogx(value: f64) -> f64 {
    if value == 0.0 {
        0.0
    } else {
        value * value.log2()
    }
}

fn infomap_count(value: usize, name: &str) -> Result<f64, AlgorithmError> {
    let value = u32::try_from(value)
        .map_err(|_| execution(&format!("Infomap {name} exceeds supported numeric range")))?;
    Ok(f64::from(value))
}

#[cfg(test)]
mod tests;
