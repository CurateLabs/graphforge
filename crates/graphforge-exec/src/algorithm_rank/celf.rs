//! Celf rank execution and deterministic worker paths.

use super::{
    AdjacencyGraph, Algorithm, AlgorithmCapability, AlgorithmControl, AlgorithmError,
    AlgorithmOutput, BUILTIN_REVIEW, HashMap, RankAlgorithm, RustAlgorithm, VecDeque, exact_u32,
    execution, rank_scores_output,
};

pub(super) struct Celf;

const CELF_SIMULATIONS: u32 = 100;
const CELF_LIVE_EDGE_THRESHOLD: u64 = u64::MAX / 10;

impl RustAlgorithm for Celf {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::Celf),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::Celf);
        rank_scores_output(algorithm, graph, celf_scores(graph, control)?, control)
    }
}

#[derive(Clone, Copy)]
struct CelfCandidate {
    node: usize,
    uuid: [u8; 16],
    gain: f64,
    updated_at: usize,
    selected: bool,
}

fn celf_scores(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let node_ids = graph.node_ids();
    let indices: HashMap<u64, usize> = node_ids
        .iter()
        .enumerate()
        .map(|(index, &node)| (node, index))
        .collect();
    let mut candidates = Vec::with_capacity(node_ids.len());
    for (node, &node_id) in node_ids.iter().enumerate() {
        let uuid = graph
            .node_uuid(node_id)
            .ok_or_else(|| execution("selected node has no UUID identity"))?;
        candidates.push(CelfCandidate {
            node,
            uuid,
            gain: celf_spread(graph, &indices, &[node], control)?,
            updated_at: 0,
            selected: false,
        });
    }

    let mut seeds = Vec::with_capacity(node_ids.len());
    let mut scores = vec![0.0; node_ids.len()];
    let mut current_spread = 0.0;
    for round in 0..node_ids.len() {
        loop {
            let best = celf_best(&candidates)
                .ok_or_else(|| execution("CELF candidate queue became empty"))?;
            if candidates[best].updated_at == round {
                let candidate = &mut candidates[best];
                candidate.selected = true;
                scores[candidate.node] = candidate.gain;
                seeds.push(candidate.node);
                current_spread += candidate.gain;
                break;
            }
            let mut candidate_seeds = seeds.clone();
            candidate_seeds.push(candidates[best].node);
            let total_spread = celf_spread(graph, &indices, &candidate_seeds, control)?;
            let gain = total_spread - current_spread;
            if !gain.is_finite() || gain < -1.0e-12 {
                return Err(execution("CELF marginal spread is negative or non-finite"));
            }
            candidates[best].gain = gain.max(0.0);
            candidates[best].updated_at = round;
        }
    }
    Ok(scores)
}

fn celf_best(candidates: &[CelfCandidate]) -> Option<usize> {
    let mut best: Option<usize> = None;
    for (index, candidate) in candidates
        .iter()
        .enumerate()
        .filter(|(_, value)| !value.selected)
    {
        let replace = best.is_none_or(|current| {
            let current = &candidates[current];
            match candidate.gain.total_cmp(&current.gain) {
                std::cmp::Ordering::Greater => true,
                std::cmp::Ordering::Equal => candidate.uuid < current.uuid,
                std::cmp::Ordering::Less => false,
            }
        });
        if replace {
            best = Some(index);
        }
    }
    best
}

fn celf_spread(
    graph: &AdjacencyGraph,
    indices: &HashMap<u64, usize>,
    seeds: &[usize],
    control: &AlgorithmControl,
) -> Result<f64, AlgorithmError> {
    control.checkpoint()?;
    let node_ids = graph.node_ids();
    let mut total = 0.0_f64;
    let mut traversed_edges = 0_usize;
    for simulation in 0..u64::from(CELF_SIMULATIONS) {
        let mut active = vec![false; node_ids.len()];
        let mut queue = VecDeque::new();
        for &seed in seeds {
            active[seed] = true;
            queue.push_back(seed);
        }
        while let Some(source) = queue.pop_front() {
            let source_uuid = graph
                .node_uuid(node_ids[source])
                .ok_or_else(|| execution("selected node has no UUID identity"))?;
            for edge in graph.neighbors(node_ids[source]) {
                if traversed_edges > 0 && traversed_edges.is_multiple_of(1024) {
                    control.checkpoint()?;
                }
                traversed_edges += 1;
                let target = indices
                    .get(&edge.neighbor_id)
                    .copied()
                    .ok_or_else(|| execution("adjacency references an unselected node"))?;
                if !active[target] && celf_live_edge(simulation, source_uuid, edge.edge_uuid) {
                    active[target] = true;
                    queue.push_back(target);
                }
            }
        }
        total += f64::from(exact_u32(
            active.iter().filter(|&&value| value).count(),
            "CELF activated-node count",
        )?);
    }
    Ok(total / f64::from(CELF_SIMULATIONS))
}

fn celf_live_edge(simulation: u64, source_uuid: [u8; 16], edge_uuid: [u8; 16]) -> bool {
    let mut state = splitmix64(simulation);
    for bytes in [source_uuid, edge_uuid] {
        for chunk in bytes.chunks_exact(8) {
            state = splitmix64(state ^ u64::from_be_bytes(chunk.try_into().expect("eight bytes")));
        }
    }
    state < CELF_LIVE_EDGE_THRESHOLD
}

fn splitmix64(value: u64) -> u64 {
    let mut mixed = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    mixed ^ (mixed >> 31)
}

#[cfg(test)]
mod tests;
