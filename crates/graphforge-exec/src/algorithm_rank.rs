//! Rust-owned rank handlers registered under the shared M18 dispatch contract.

use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use graphforge_core::algorithms::{Algorithm, RankAlgorithm};
use graphforge_core::{GfError, OntologyMode, RankOptions, TypeId};
use graphforge_ir::Direction;

use crate::AdjacencyProvider;
use crate::algorithm_dispatch::{
    AlgorithmCancellation, AlgorithmCapability, AlgorithmControl, AlgorithmError, AlgorithmLimits,
    AlgorithmOutput, AlgorithmRegistry, AlgorithmValue, DependencyReview, RustAlgorithm,
};
use crate::algorithm_graph::{AdjacencyGraph, AdjacencySelection, export_adjacency};
use crate::algorithm_k_core::k_core_numbers;
use crate::algorithm_neighbors::{simple_neighbors, simple_undirected_neighbors};
use crate::algorithm_output::{materialize_node_properties, shape_algorithm_output};

const BUILTIN_REVIEW: DependencyReview = DependencyReview {
    implementation: "graphforge-exec built-in",
    license: "Apache-2.0",
    maintenance: "GraphForge workspace",
    security: "workspace cargo-deny and CodeQL",
    binary_size: "no additional dependency",
    determinism: "stable surrogate-ordered rows and iterative accumulation",
    platforms: "Rust workspace targets",
};

struct Degree;

struct PageRank;

struct Betweenness;

struct Closeness;

struct HarmonicCloseness;

struct Eigenvector;

struct ArticleRank;

struct HitsHub;

struct HitsAuthority;

struct Celf;

struct ClusteringCoefficient;

struct Triangles;

struct KCore;

struct PreferentialAttachment;

struct AdamicAdar;

struct CommonNeighbors;

struct ResourceAllocation;

struct TotalNeighbors;

const PAGERANK_DAMPING: f64 = 0.85;
const PAGERANK_TOLERANCE: f64 = 1.0e-10;
const EIGENVECTOR_MAX_ITERATIONS: usize = 20;
const EIGENVECTOR_TOLERANCE: f64 = 1.0e-7;
const ARTICLE_RANK_DAMPING: f64 = 0.85;
const ARTICLE_RANK_ALPHA: f64 = 1.0 - ARTICLE_RANK_DAMPING;
const ARTICLE_RANK_MAX_ITERATIONS: usize = 20;
const ARTICLE_RANK_TOLERANCE: f64 = 1.0e-7;
const HITS_ITERATIONS: usize = 20;
const CELF_SIMULATIONS: u32 = 100;
const CELF_LIVE_EDGE_THRESHOLD: u64 = u64::MAX / 10;

impl RustAlgorithm for Degree {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::Degree),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let denominator = exact_u32(
            graph.node_ids().len().saturating_sub(1).max(1),
            "node count",
        )?;
        let algorithm = Algorithm::Rank(RankAlgorithm::Degree);
        let mut sink = control.output_sink(algorithm)?;
        for (index, &node_id) in graph.node_ids().iter().enumerate() {
            if index % 1024 == 0 {
                control.checkpoint()?;
            }
            let uuid = graph
                .node_uuid(node_id)
                .ok_or_else(|| execution("selected node has no UUID identity"))?;
            let degree = exact_u32(graph.neighbors(node_id).len(), "node degree")?;
            sink.append_row(&[
                AlgorithmValue::Uuid(uuid),
                AlgorithmValue::Float64(f64::from(degree) / f64::from(denominator)),
            ])?;
        }
        sink.finish()
    }
}

impl RustAlgorithm for PageRank {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::PageRank),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::PageRank);
        let node_count = graph.node_ids().len();
        if node_count == 0 {
            return AlgorithmOutput::empty(algorithm, control);
        }

        let indices: HashMap<u64, usize> = graph
            .node_ids()
            .iter()
            .enumerate()
            .map(|(index, &node)| (node, index))
            .collect();
        let node_count = f64::from(exact_u32(node_count, "node count")?);
        let mut scores = vec![1.0 / node_count; graph.node_ids().len()];
        loop {
            control.checkpoint()?;
            let dangling: f64 = graph
                .node_ids()
                .iter()
                .enumerate()
                .filter(|(_, node)| graph.neighbors(**node).is_empty())
                .map(|(index, _)| scores[index])
                .sum();
            let base =
                (1.0 - PAGERANK_DAMPING) / node_count + PAGERANK_DAMPING * dangling / node_count;
            let mut next = vec![base; scores.len()];
            for (source_index, &source) in graph.node_ids().iter().enumerate() {
                let edges = graph.neighbors(source);
                if edges.is_empty() {
                    continue;
                }
                let outdegree = f64::from(exact_u32(edges.len(), "node degree")?);
                let contribution = PAGERANK_DAMPING * scores[source_index] / outdegree;
                for edge in edges {
                    let target = indices
                        .get(&edge.neighbor_id)
                        .copied()
                        .ok_or_else(|| execution("adjacency references an unselected node"))?;
                    next[target] += contribution;
                }
            }
            let delta: f64 = scores
                .iter()
                .zip(&next)
                .map(|(previous, current)| (previous - current).abs())
                .sum();
            scores = next;
            if delta <= node_count * PAGERANK_TOLERANCE {
                break;
            }
        }

        let rows = graph
            .node_ids()
            .iter()
            .enumerate()
            .map(|(index, &node)| {
                let uuid = graph
                    .node_uuid(node)
                    .ok_or_else(|| execution("selected node has no UUID identity"))?;
                Ok(vec![
                    AlgorithmValue::Uuid(uuid),
                    AlgorithmValue::Float64(scores[index]),
                ])
            })
            .collect::<Result<Vec<_>, AlgorithmError>>()?;
        AlgorithmOutput::from_rows(algorithm, control, rows)
    }
}

impl RustAlgorithm for Betweenness {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::Betweenness),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::Betweenness);
        let node_ids = graph.node_ids();
        if node_ids.is_empty() {
            return AlgorithmOutput::empty(algorithm, control);
        }

        let indices: HashMap<u64, usize> = node_ids
            .iter()
            .enumerate()
            .map(|(index, &node)| (node, index))
            .collect();
        let mut scores = vec![0.0; node_ids.len()];
        for source in 0..node_ids.len() {
            control.checkpoint()?;
            let mut stack = Vec::with_capacity(node_ids.len());
            let mut predecessors = vec![Vec::new(); node_ids.len()];
            let mut paths = vec![0.0_f64; node_ids.len()];
            paths[source] = 1.0;
            let mut distance = vec![usize::MAX; node_ids.len()];
            distance[source] = 0;
            let mut queue = VecDeque::from([source]);
            let mut visited = 0_usize;
            let mut traversed_edges = 0_usize;

            while let Some(vertex) = queue.pop_front() {
                if visited > 0 && visited.is_multiple_of(1024) {
                    control.checkpoint()?;
                }
                visited += 1;
                stack.push(vertex);
                for edge in graph.neighbors(node_ids[vertex]) {
                    if traversed_edges > 0 && traversed_edges.is_multiple_of(1024) {
                        control.checkpoint()?;
                    }
                    traversed_edges += 1;
                    let target = indices
                        .get(&edge.neighbor_id)
                        .copied()
                        .ok_or_else(|| execution("adjacency references an unselected node"))?;
                    if distance[target] == usize::MAX {
                        distance[target] = distance[vertex] + 1;
                        queue.push_back(target);
                    }
                    if distance[target] == distance[vertex] + 1 {
                        paths[target] += paths[vertex];
                        if !paths[target].is_finite() {
                            return Err(execution(
                                "shortest-path multiplicity exceeds supported score range",
                            ));
                        }
                        predecessors[target].push(vertex);
                    }
                }
            }

            let mut dependency = vec![0.0_f64; node_ids.len()];
            let mut traversed_predecessors = 0_usize;
            while let Some(target) = stack.pop() {
                for &predecessor in &predecessors[target] {
                    if traversed_predecessors > 0 && traversed_predecessors.is_multiple_of(1024) {
                        control.checkpoint()?;
                    }
                    traversed_predecessors += 1;
                    dependency[predecessor] +=
                        paths[predecessor] / paths[target] * (1.0 + dependency[target]);
                    if !dependency[predecessor].is_finite() {
                        return Err(execution("betweenness dependency exceeds score range"));
                    }
                }
                if target != source {
                    scores[target] += dependency[target];
                }
            }
        }

        if node_ids.len() > 2 {
            let nodes = exact_u32(node_ids.len(), "node count")?;
            let scale = 1.0 / (f64::from(nodes - 1) * f64::from(nodes - 2));
            for score in &mut scores {
                *score *= scale;
            }
        }

        let rows = node_ids
            .iter()
            .enumerate()
            .map(|(index, &node)| {
                let uuid = graph
                    .node_uuid(node)
                    .ok_or_else(|| execution("selected node has no UUID identity"))?;
                Ok(vec![
                    AlgorithmValue::Uuid(uuid),
                    AlgorithmValue::Float64(scores[index]),
                ])
            })
            .collect::<Result<Vec<_>, AlgorithmError>>()?;
        AlgorithmOutput::from_rows(algorithm, control, rows)
    }
}

impl RustAlgorithm for Closeness {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::Closeness),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::Closeness);
        let node_ids = graph.node_ids();
        if node_ids.is_empty() {
            return AlgorithmOutput::empty(algorithm, control);
        }

        let indices: HashMap<u64, usize> = node_ids
            .iter()
            .enumerate()
            .map(|(index, &node)| (node, index))
            .collect();
        let node_count = f64::from(exact_u32(node_ids.len(), "node count")?);
        let mut scores = Vec::with_capacity(node_ids.len());
        for source in 0..node_ids.len() {
            control.checkpoint()?;
            let mut distance = vec![usize::MAX; node_ids.len()];
            distance[source] = 0;
            let mut queue = VecDeque::from([source]);
            let mut traversed_edges = 0_usize;

            while let Some(vertex) = queue.pop_front() {
                for edge in graph.neighbors(node_ids[vertex]) {
                    if traversed_edges > 0 && traversed_edges.is_multiple_of(1024) {
                        control.checkpoint()?;
                    }
                    traversed_edges += 1;
                    let target = indices
                        .get(&edge.neighbor_id)
                        .copied()
                        .ok_or_else(|| execution("adjacency references an unselected node"))?;
                    if distance[target] == usize::MAX {
                        distance[target] = distance[vertex] + 1;
                        queue.push_back(target);
                    }
                }
            }

            let mut reachable = 0_u32;
            let mut distance_sum = 0.0_f64;
            for hops in distance
                .into_iter()
                .filter(|&hops| hops != 0 && hops != usize::MAX)
            {
                reachable += 1;
                distance_sum += f64::from(exact_u32(hops, "shortest-path distance")?);
            }
            let reachable = f64::from(reachable);
            let score = if node_count > 1.0 && reachable > 0.0 {
                reachable * reachable / ((node_count - 1.0) * distance_sum)
            } else {
                0.0
            };
            if !score.is_finite() {
                return Err(execution("closeness score exceeds supported range"));
            }
            scores.push(score);
        }

        let rows = node_ids
            .iter()
            .zip(scores)
            .map(|(&node, score)| {
                let uuid = graph
                    .node_uuid(node)
                    .ok_or_else(|| execution("selected node has no UUID identity"))?;
                Ok(vec![
                    AlgorithmValue::Uuid(uuid),
                    AlgorithmValue::Float64(score),
                ])
            })
            .collect::<Result<Vec<_>, AlgorithmError>>()?;
        AlgorithmOutput::from_rows(algorithm, control, rows)
    }
}

impl RustAlgorithm for HarmonicCloseness {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::HarmonicCloseness),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::HarmonicCloseness);
        let node_ids = graph.node_ids();
        if node_ids.is_empty() {
            return AlgorithmOutput::empty(algorithm, control);
        }

        let indices: HashMap<u64, usize> = node_ids
            .iter()
            .enumerate()
            .map(|(index, &node)| (node, index))
            .collect();
        let denominator = f64::from(exact_u32(
            node_ids.len().saturating_sub(1).max(1),
            "node count",
        )?);
        let mut scores = Vec::with_capacity(node_ids.len());
        for source in 0..node_ids.len() {
            control.checkpoint()?;
            let mut distance = vec![usize::MAX; node_ids.len()];
            distance[source] = 0;
            let mut queue = VecDeque::from([source]);
            let mut traversed_edges = 0_usize;

            while let Some(vertex) = queue.pop_front() {
                for edge in graph.neighbors(node_ids[vertex]) {
                    if traversed_edges > 0 && traversed_edges.is_multiple_of(1024) {
                        control.checkpoint()?;
                    }
                    traversed_edges += 1;
                    let target = indices
                        .get(&edge.neighbor_id)
                        .copied()
                        .ok_or_else(|| execution("adjacency references an unselected node"))?;
                    if distance[target] == usize::MAX {
                        distance[target] = distance[vertex] + 1;
                        queue.push_back(target);
                    }
                }
            }

            let mut reciprocal_sum = 0.0_f64;
            for hops in distance
                .into_iter()
                .filter(|&hops| hops != 0 && hops != usize::MAX)
            {
                reciprocal_sum += 1.0 / f64::from(exact_u32(hops, "shortest-path distance")?);
            }
            let score = reciprocal_sum / denominator;
            if !score.is_finite() {
                return Err(execution(
                    "harmonic closeness score exceeds supported range",
                ));
            }
            scores.push(score);
        }

        let rows = node_ids
            .iter()
            .zip(scores)
            .map(|(&node, score)| {
                let uuid = graph
                    .node_uuid(node)
                    .ok_or_else(|| execution("selected node has no UUID identity"))?;
                Ok(vec![
                    AlgorithmValue::Uuid(uuid),
                    AlgorithmValue::Float64(score),
                ])
            })
            .collect::<Result<Vec<_>, AlgorithmError>>()?;
        AlgorithmOutput::from_rows(algorithm, control, rows)
    }
}

impl RustAlgorithm for Eigenvector {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::Eigenvector),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::Eigenvector);
        let node_ids = graph.node_ids();
        if node_ids.is_empty() {
            return AlgorithmOutput::empty(algorithm, control);
        }

        let indices: HashMap<u64, usize> = node_ids
            .iter()
            .enumerate()
            .map(|(index, &node)| (node, index))
            .collect();
        let node_count = f64::from(exact_u32(node_ids.len(), "node count")?);
        let mut scores = vec![1.0 / node_count; node_ids.len()];
        for iteration in 0..EIGENVECTOR_MAX_ITERATIONS {
            control.checkpoint()?;
            let mut next = scores.clone();
            let mut traversed_edges = 0_usize;
            for (source_index, &source) in node_ids.iter().enumerate() {
                for edge in graph.neighbors(source) {
                    if traversed_edges > 0 && traversed_edges.is_multiple_of(1024) {
                        control.checkpoint()?;
                    }
                    traversed_edges += 1;
                    let target = indices
                        .get(&edge.neighbor_id)
                        .copied()
                        .ok_or_else(|| execution("adjacency references an unselected node"))?;
                    next[target] += scores[source_index];
                    if !next[target].is_finite() {
                        return Err(execution("eigenvector score exceeds supported range"));
                    }
                }
            }

            let norm = next.iter().map(|score| score * score).sum::<f64>().sqrt();
            if !norm.is_finite() || norm == 0.0 {
                return Err(execution("eigenvector L2 norm is not finite and positive"));
            }
            for score in &mut next {
                *score /= norm;
            }
            let converged = next
                .iter()
                .zip(&scores)
                .all(|(current, previous)| (current - previous).abs() <= EIGENVECTOR_TOLERANCE);
            scores = next;
            if iteration > 0 && converged {
                break;
            }
        }

        let rows = node_ids
            .iter()
            .zip(scores)
            .map(|(&node, score)| {
                let uuid = graph
                    .node_uuid(node)
                    .ok_or_else(|| execution("selected node has no UUID identity"))?;
                Ok(vec![
                    AlgorithmValue::Uuid(uuid),
                    AlgorithmValue::Float64(score),
                ])
            })
            .collect::<Result<Vec<_>, AlgorithmError>>()?;
        AlgorithmOutput::from_rows(algorithm, control, rows)
    }
}

impl RustAlgorithm for ArticleRank {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::ArticleRank),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::ArticleRank);
        let node_ids = graph.node_ids();
        if node_ids.is_empty() {
            return AlgorithmOutput::empty(algorithm, control);
        }

        let indices: HashMap<u64, usize> = node_ids
            .iter()
            .enumerate()
            .map(|(index, &node)| (node, index))
            .collect();
        let edge_count = node_ids.iter().try_fold(0_usize, |total, &node| {
            total
                .checked_add(graph.neighbors(node).len())
                .ok_or_else(|| execution("selected edge count exceeds supported score range"))
        })?;
        let average_degree = f64::from(exact_u32(edge_count, "selected edge count")?)
            / f64::from(exact_u32(node_ids.len(), "node count")?);
        let mut scores = vec![ARTICLE_RANK_ALPHA; node_ids.len()];
        let mut deltas = scores.clone();

        for _ in 0..ARTICLE_RANK_MAX_ITERATIONS {
            control.checkpoint()?;
            let mut next = vec![0.0; node_ids.len()];
            let mut traversed_edges = 0_usize;
            for (source_index, &source) in node_ids.iter().enumerate() {
                let edges = graph.neighbors(source);
                if edges.is_empty() {
                    continue;
                }
                let degree = f64::from(exact_u32(edges.len(), "node degree")?);
                let message = deltas[source_index] / (degree + average_degree);
                for edge in edges {
                    if traversed_edges > 0 && traversed_edges.is_multiple_of(1024) {
                        control.checkpoint()?;
                    }
                    traversed_edges += 1;
                    let target = indices
                        .get(&edge.neighbor_id)
                        .copied()
                        .ok_or_else(|| execution("adjacency references an unselected node"))?;
                    next[target] += message;
                }
            }
            let mut converged = true;
            for (score, delta) in scores.iter_mut().zip(&mut next) {
                *delta *= ARTICLE_RANK_DAMPING;
                if !delta.is_finite() {
                    return Err(execution("ArticleRank score exceeds supported range"));
                }
                *score += *delta;
                if !score.is_finite() {
                    return Err(execution("ArticleRank score exceeds supported range"));
                }
                converged &= *delta <= ARTICLE_RANK_TOLERANCE;
            }
            deltas = next;
            if converged {
                break;
            }
        }

        let rows = node_ids
            .iter()
            .zip(scores)
            .map(|(&node, score)| {
                let uuid = graph
                    .node_uuid(node)
                    .ok_or_else(|| execution("selected node has no UUID identity"))?;
                Ok(vec![
                    AlgorithmValue::Uuid(uuid),
                    AlgorithmValue::Float64(score),
                ])
            })
            .collect::<Result<Vec<_>, AlgorithmError>>()?;
        AlgorithmOutput::from_rows(algorithm, control, rows)
    }
}

impl RustAlgorithm for HitsHub {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::HitsHub),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::HitsHub);
        let (_, hubs) = hits_scores(graph, control)?;
        rank_scores_output(algorithm, graph, hubs, control)
    }
}

impl RustAlgorithm for HitsAuthority {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::HitsAuthority),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::HitsAuthority);
        let (authorities, _) = hits_scores(graph, control)?;
        rank_scores_output(algorithm, graph, authorities, control)
    }
}

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

impl RustAlgorithm for ClusteringCoefficient {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::ClusteringCoefficient),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::ClusteringCoefficient);
        rank_scores_output(algorithm, graph, clustering_coefficient_scores(graph, control)?, control)
    }
}

impl RustAlgorithm for Triangles {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::Triangles),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::Triangles);
        rank_scores_output(algorithm, graph, triangle_scores(graph, control)?, control)
    }
}

impl RustAlgorithm for KCore {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::KCore),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::KCore);
        rank_scores_output(algorithm, graph, k_core_scores(graph, control)?, control)
    }
}

impl RustAlgorithm for PreferentialAttachment {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::PreferentialAttachment),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::PreferentialAttachment);
        rank_scores_output(algorithm, graph, preferential_attachment_scores(graph, control)?, control)
    }
}

impl RustAlgorithm for AdamicAdar {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::AdamicAdar),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::AdamicAdar);
        rank_scores_output(algorithm, graph, adamic_adar_scores(graph, control)?, control)
    }
}

impl RustAlgorithm for CommonNeighbors {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::CommonNeighbors),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::CommonNeighbors);
        rank_scores_output(algorithm, graph, common_neighbor_scores(graph, control)?, control)
    }
}

impl RustAlgorithm for ResourceAllocation {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::ResourceAllocation),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::ResourceAllocation);
        rank_scores_output(algorithm, graph, resource_allocation_scores(graph, control)?, control)
    }
}

impl RustAlgorithm for TotalNeighbors {
    fn capability(&self) -> AlgorithmCapability {
        AlgorithmCapability {
            algorithm: Algorithm::Rank(RankAlgorithm::TotalNeighbors),
            backend: "rust",
            dependency: BUILTIN_REVIEW,
        }
    }

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let algorithm = Algorithm::Rank(RankAlgorithm::TotalNeighbors);
        rank_scores_output(algorithm, graph, total_neighbor_scores(graph, control)?, control)
    }
}

fn triangle_scores(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let neighbors = simple_undirected_neighbors(graph, control)?;
    let mut scores = Vec::with_capacity(graph.node_ids().len());
    let mut visited_pairs = 0_usize;
    for node in 0..graph.node_ids().len() {
        control.checkpoint()?;
        let mut count = 0_u64;
        for (offset, &first) in neighbors[node].iter().enumerate() {
            for &second in &neighbors[node][offset + 1..] {
                if visited_pairs.is_multiple_of(1024) {
                    control.checkpoint()?;
                }
                visited_pairs += 1;
                if has_arc(&neighbors, first, second) {
                    count = count
                        .checked_add(1)
                        .ok_or_else(|| execution("triangle count exceeds supported range"))?;
                }
            }
        }
        scores.push(exact_u64_as_f64(count, "triangle count")?);
    }
    Ok(scores)
}

fn preferential_attachment_scores(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    // For each node u, sum deg(u) * deg(v) over every missing outgoing
    // candidate v. Algebraic aggregation avoids materializing O(V^2) pairs.
    let neighbors = simple_neighbors(graph, control, false)?;
    let degrees: Vec<u64> = neighbors
        .iter()
        .map(|adjacent| {
            u64::try_from(adjacent.len())
                .map_err(|_| execution("preferential-attachment degree exceeds supported range"))
        })
        .collect::<Result<_, _>>()?;
    let total_degree = degrees.iter().try_fold(0_u64, |total, degree| {
        total
            .checked_add(*degree)
            .ok_or_else(|| execution("preferential-attachment degree sum exceeds supported range"))
    })?;
    let mut visited_neighbors = 0_usize;
    neighbors
        .iter()
        .enumerate()
        .map(|(node, adjacent)| {
            if node.is_multiple_of(1024) {
                control.checkpoint()?;
            }
            let linked_degree = adjacent.iter().try_fold(0_u64, |total, &neighbor| {
                if visited_neighbors.is_multiple_of(1024) {
                    control.checkpoint()?;
                }
                visited_neighbors += 1;
                total.checked_add(degrees[neighbor]).ok_or_else(|| {
                    execution("preferential-attachment neighbor sum exceeds supported range")
                })
            })?;
            let candidate_degree = total_degree
                .checked_sub(degrees[node])
                .and_then(|remaining| remaining.checked_sub(linked_degree))
                .ok_or_else(|| execution("preferential-attachment candidate sum underflow"))?;
            let score = degrees[node].checked_mul(candidate_degree).ok_or_else(|| {
                execution("preferential-attachment score exceeds supported range")
            })?;
            exact_u64_as_f64(score, "preferential-attachment score")
        })
        .collect()
}

fn adamic_adar_scores(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let neighbors = simple_neighbors(graph, control, false)?;
    let mut discount_degrees = vec![0_u64; neighbors.len()];
    for adjacent in &neighbors {
        for &neighbor in adjacent {
            discount_degrees[neighbor] = discount_degrees[neighbor]
                .checked_add(1)
                .ok_or_else(|| execution("Adamic-Adar neighbor degree exceeds supported range"))?;
        }
    }

    let mut visited = 0_usize;
    let mut scores = Vec::with_capacity(neighbors.len());
    for (source, source_neighbors) in neighbors.iter().enumerate() {
        let mut score = 0.0_f64;
        let mut compensation = 0.0_f64;
        for (candidate, candidate_neighbors) in neighbors.iter().enumerate() {
            if visited.is_multiple_of(1024) {
                control.checkpoint()?;
            }
            visited += 1;
            if source == candidate || source_neighbors.binary_search(&candidate).is_ok() {
                continue;
            }
            let (mut left, mut right) = (0, 0);
            while left < source_neighbors.len() && right < candidate_neighbors.len() {
                if visited.is_multiple_of(1024) {
                    control.checkpoint()?;
                }
                visited += 1;
                match source_neighbors[left].cmp(&candidate_neighbors[right]) {
                    std::cmp::Ordering::Less => left += 1,
                    std::cmp::Ordering::Greater => right += 1,
                    std::cmp::Ordering::Equal => {
                        let common = source_neighbors[left];
                        let term = adamic_discount(discount_degrees[common])?;
                        let adjusted = term - compensation;
                        let updated = score + adjusted;
                        compensation = (updated - score) - adjusted;
                        score = updated;
                        left += 1;
                        right += 1;
                    }
                }
            }
        }
        if !score.is_finite() {
            return Err(execution("Adamic-Adar score is not finite"));
        }
        scores.push(score);
    }
    Ok(scores)
}

fn adamic_discount(degree: u64) -> Result<f64, AlgorithmError> {
    if degree < 2 {
        return Err(execution(
            "Adamic-Adar common-neighbor degree must be at least two",
        ));
    }
    #[allow(
        clippy::cast_precision_loss,
        reason = "the logarithmic discount does not require an exact integer conversion"
    )]
    let term = 1.0 / (degree as f64).ln();
    if !term.is_finite() {
        return Err(execution("Adamic-Adar discount is not finite"));
    }
    Ok(term)
}

fn common_neighbor_scores(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let neighbors = simple_neighbors(graph, control, false)?;
    let mut visited = 0_usize;
    let mut scores = Vec::with_capacity(neighbors.len());
    for (source, source_neighbors) in neighbors.iter().enumerate() {
        let mut score = 0_u64;
        for (candidate, candidate_neighbors) in neighbors.iter().enumerate() {
            if visited.is_multiple_of(1024) {
                control.checkpoint()?;
            }
            visited += 1;
            if source == candidate || source_neighbors.binary_search(&candidate).is_ok() {
                continue;
            }
            let (mut left, mut right) = (0, 0);
            while left < source_neighbors.len() && right < candidate_neighbors.len() {
                if visited.is_multiple_of(1024) {
                    control.checkpoint()?;
                }
                visited += 1;
                match source_neighbors[left].cmp(&candidate_neighbors[right]) {
                    std::cmp::Ordering::Less => left += 1,
                    std::cmp::Ordering::Greater => right += 1,
                    std::cmp::Ordering::Equal => {
                        score = score.checked_add(1).ok_or_else(|| {
                            execution("common-neighbors score exceeds supported range")
                        })?;
                        left += 1;
                        right += 1;
                    }
                }
            }
        }
        scores.push(exact_u64_as_f64(score, "common-neighbors score")?);
    }
    Ok(scores)
}

fn resource_allocation_scores(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let neighbors = simple_neighbors(graph, control, false)?;
    let mut discount_degrees = vec![0_u64; neighbors.len()];
    for adjacent in &neighbors {
        for &neighbor in adjacent {
            discount_degrees[neighbor] = discount_degrees[neighbor]
                .checked_add(1)
                .ok_or_else(|| execution("resource-allocation degree exceeds supported range"))?;
        }
    }

    let mut visited = 0_usize;
    let mut scores = Vec::with_capacity(neighbors.len());
    for (source, source_neighbors) in neighbors.iter().enumerate() {
        let mut score = 0.0_f64;
        let mut compensation = 0.0_f64;
        for (candidate, candidate_neighbors) in neighbors.iter().enumerate() {
            if visited.is_multiple_of(1024) {
                control.checkpoint()?;
            }
            visited += 1;
            if source == candidate || source_neighbors.binary_search(&candidate).is_ok() {
                continue;
            }
            let (mut left, mut right) = (0, 0);
            while left < source_neighbors.len() && right < candidate_neighbors.len() {
                if visited.is_multiple_of(1024) {
                    control.checkpoint()?;
                }
                visited += 1;
                match source_neighbors[left].cmp(&candidate_neighbors[right]) {
                    std::cmp::Ordering::Less => left += 1,
                    std::cmp::Ordering::Greater => right += 1,
                    std::cmp::Ordering::Equal => {
                        let term =
                            resource_allocation_discount(discount_degrees[source_neighbors[left]])?;
                        let adjusted = term - compensation;
                        let updated = score + adjusted;
                        compensation = (updated - score) - adjusted;
                        score = updated;
                        left += 1;
                        right += 1;
                    }
                }
            }
        }
        if !score.is_finite() {
            return Err(execution("resource-allocation score is not finite"));
        }
        scores.push(score);
    }
    Ok(scores)
}

fn resource_allocation_discount(degree: u64) -> Result<f64, AlgorithmError> {
    if degree < 2 {
        return Err(execution(
            "resource-allocation common-neighbor degree must be at least two",
        ));
    }
    #[allow(
        clippy::cast_precision_loss,
        reason = "the reciprocal discount does not require an exact integer conversion"
    )]
    let term = 1.0 / degree as f64;
    if !term.is_finite() {
        return Err(execution("resource-allocation discount is not finite"));
    }
    Ok(term)
}

fn total_neighbor_scores(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let neighbors = simple_neighbors(graph, control, false)?;
    let degrees: Vec<u64> = neighbors
        .iter()
        .map(|adjacent| {
            u64::try_from(adjacent.len())
                .map_err(|_| execution("total-neighbors degree exceeds supported range"))
        })
        .collect::<Result<_, _>>()?;
    let mut visited = 0_usize;
    let mut scores = Vec::with_capacity(neighbors.len());
    for (source, source_neighbors) in neighbors.iter().enumerate() {
        let mut score = 0_u64;
        for (candidate, candidate_neighbors) in neighbors.iter().enumerate() {
            if visited.is_multiple_of(1024) {
                control.checkpoint()?;
            }
            visited += 1;
            if source == candidate || source_neighbors.binary_search(&candidate).is_ok() {
                continue;
            }
            let mut union = degrees[source]
                .checked_add(degrees[candidate])
                .ok_or_else(|| execution("total-neighbors pair score exceeds supported range"))?;
            let (mut left, mut right) = (0, 0);
            while left < source_neighbors.len() && right < candidate_neighbors.len() {
                if visited.is_multiple_of(1024) {
                    control.checkpoint()?;
                }
                visited += 1;
                match source_neighbors[left].cmp(&candidate_neighbors[right]) {
                    std::cmp::Ordering::Less => left += 1,
                    std::cmp::Ordering::Greater => right += 1,
                    std::cmp::Ordering::Equal => {
                        union = union
                            .checked_sub(1)
                            .ok_or_else(|| execution("total-neighbors pair score underflow"))?;
                        left += 1;
                        right += 1;
                    }
                }
            }
            score = score
                .checked_add(union)
                .ok_or_else(|| execution("total-neighbors score exceeds supported range"))?;
        }
        scores.push(exact_u64_as_f64(score, "total-neighbors score")?);
    }
    Ok(scores)
}

fn k_core_scores(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    k_core_numbers(graph, control)?
        .into_iter()
        .map(|core| {
            let core = u64::try_from(core)
                .map_err(|_| execution("k-core score exceeds supported range"))?;
            exact_u64_as_f64(core, "k-core score")
        })
        .collect()
}

fn clustering_coefficient_scores(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<Vec<f64>, AlgorithmError> {
    let node_ids = graph.node_ids();
    let indices: HashMap<u64, usize> = node_ids
        .iter()
        .enumerate()
        .map(|(index, &node)| (node, index))
        .collect();
    let mut outgoing = vec![Vec::new(); node_ids.len()];
    let mut traversed_edges = 0_usize;
    for (source, &node_id) in node_ids.iter().enumerate() {
        for edge in graph.neighbors(node_id) {
            if traversed_edges.is_multiple_of(1024) {
                control.checkpoint()?;
            }
            traversed_edges += 1;
            let target = indices
                .get(&edge.neighbor_id)
                .copied()
                .ok_or_else(|| execution("adjacency references an unselected node"))?;
            if source != target {
                outgoing[source].push(target);
            }
        }
        outgoing[source].sort_unstable();
        outgoing[source].dedup();
    }

    let mut incoming = vec![Vec::new(); node_ids.len()];
    for (source, targets) in outgoing.iter().enumerate() {
        for &target in targets {
            incoming[target].push(source);
        }
    }
    for sources in &mut incoming {
        sources.sort_unstable();
    }

    let mut scores = Vec::with_capacity(node_ids.len());
    let mut visited_pairs = 0_usize;
    for node in 0..node_ids.len() {
        control.checkpoint()?;
        let mut neighbors = outgoing[node].clone();
        neighbors.extend_from_slice(&incoming[node]);
        neighbors.sort_unstable();
        neighbors.dedup();

        let total_degree = u64::try_from(outgoing[node].len() + incoming[node].len())
            .map_err(|_| execution("clustering coefficient degree exceeds supported range"))?;
        let reciprocal_degree = u64::try_from(
            outgoing[node]
                .iter()
                .filter(|&&neighbor| has_arc(&outgoing, neighbor, node))
                .count(),
        )
        .map_err(|_| execution("reciprocal degree exceeds supported range"))?;
        let denominator = total_degree
            .checked_mul(total_degree.saturating_sub(1))
            .and_then(|value| value.checked_sub(reciprocal_degree.checked_mul(2)?))
            .and_then(|value| value.checked_mul(2))
            .ok_or_else(|| {
                execution("clustering coefficient denominator exceeds supported range")
            })?;

        let mut triangles = 0_u64;
        for &first in &neighbors {
            for &second in &neighbors {
                if visited_pairs.is_multiple_of(1024) {
                    control.checkpoint()?;
                }
                visited_pairs += 1;
                let contribution = arc_strength(&outgoing, node, first)
                    * arc_strength(&outgoing, first, second)
                    * arc_strength(&outgoing, second, node);
                triangles = triangles.checked_add(contribution).ok_or_else(|| {
                    execution("clustering coefficient triangle count exceeds supported range")
                })?;
            }
        }
        let score = if denominator == 0 {
            0.0
        } else {
            exact_u64_as_f64(triangles, "clustering coefficient triangle count")?
                / exact_u64_as_f64(denominator, "clustering coefficient denominator")?
        };
        if !score.is_finite() {
            return Err(execution("clustering coefficient score is not finite"));
        }
        scores.push(score);
    }
    Ok(scores)
}

fn has_arc(outgoing: &[Vec<usize>], source: usize, target: usize) -> bool {
    outgoing[source].binary_search(&target).is_ok()
}

fn arc_strength(outgoing: &[Vec<usize>], source: usize, target: usize) -> u64 {
    u64::from(has_arc(outgoing, source, target)) + u64::from(has_arc(outgoing, target, source))
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

fn hits_scores(
    graph: &AdjacencyGraph,
    control: &AlgorithmControl,
) -> Result<(Vec<f64>, Vec<f64>), AlgorithmError> {
    let node_ids = graph.node_ids();
    if node_ids.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }

    let indices: HashMap<u64, usize> = node_ids
        .iter()
        .enumerate()
        .map(|(index, &node)| (node, index))
        .collect();
    let mut authorities = vec![1.0; node_ids.len()];
    let mut hubs = vec![1.0; node_ids.len()];
    for _ in 0..HITS_ITERATIONS {
        control.checkpoint()?;
        authorities.fill(0.0);
        accumulate_hits_phase(
            graph,
            node_ids,
            control,
            |source, _| hubs[source],
            |_, target, value| authorities[target] += value,
            &indices,
        )?;
        normalize_hits(&mut authorities, "authority")?;

        control.checkpoint()?;
        let mut next_hubs = vec![0.0; node_ids.len()];
        accumulate_hits_phase(
            graph,
            node_ids,
            control,
            |_, target| authorities[target],
            |source, _, value| next_hubs[source] += value,
            &indices,
        )?;
        normalize_hits(&mut next_hubs, "hub")?;
        hubs = next_hubs;
    }
    Ok((authorities, hubs))
}

fn rank_scores_output(
    algorithm: Algorithm,
    graph: &AdjacencyGraph,
    scores: Vec<f64>,
    control: &AlgorithmControl,
) -> Result<AlgorithmOutput, AlgorithmError> {
    let mut sink = control.output_sink(algorithm)?;
    for (&node, score) in graph.node_ids().iter().zip(scores) {
        let uuid = graph
            .node_uuid(node)
            .ok_or_else(|| execution("selected node has no UUID identity"))?;
        sink.append_row(&[
            AlgorithmValue::Uuid(uuid),
            AlgorithmValue::Float64(score),
        ])?;
    }
    sink.finish()
}

fn accumulate_hits_phase(
    graph: &AdjacencyGraph,
    node_ids: &[u64],
    control: &AlgorithmControl,
    value: impl Fn(usize, usize) -> f64,
    mut add: impl FnMut(usize, usize, f64),
    indices: &HashMap<u64, usize>,
) -> Result<(), AlgorithmError> {
    let mut traversed_edges = 0_usize;
    for (source, &node) in node_ids.iter().enumerate() {
        for edge in graph.neighbors(node) {
            if traversed_edges > 0 && traversed_edges.is_multiple_of(1024) {
                control.checkpoint()?;
            }
            traversed_edges += 1;
            let target = indices
                .get(&edge.neighbor_id)
                .copied()
                .ok_or_else(|| execution("adjacency references an unselected node"))?;
            add(source, target, value(source, target));
        }
    }
    Ok(())
}

fn normalize_hits(scores: &mut [f64], kind: &str) -> Result<(), AlgorithmError> {
    let norm = scores.iter().map(|score| score * score).sum::<f64>().sqrt();
    if !norm.is_finite() {
        return Err(execution(format!("HITS {kind} norm is not finite")));
    }
    if norm == 0.0 {
        return Ok(());
    }
    for score in scores {
        *score /= norm;
        if !score.is_finite() {
            return Err(execution(format!("HITS {kind} score is not finite")));
        }
    }
    Ok(())
}

pub(crate) fn register_rank_algorithms(
    registry: &mut AlgorithmRegistry,
) -> Result<(), AlgorithmError> {
    registry.register(Arc::new(Degree))?;
    registry.register(Arc::new(PageRank))?;
    registry.register(Arc::new(Betweenness))?;
    registry.register(Arc::new(Closeness))?;
    registry.register(Arc::new(HarmonicCloseness))?;
    registry.register(Arc::new(Eigenvector))?;
    registry.register(Arc::new(ArticleRank))?;
    registry.register(Arc::new(HitsHub))?;
    registry.register(Arc::new(HitsAuthority))?;
    registry.register(Arc::new(Celf))?;
    registry.register(Arc::new(ClusteringCoefficient))?;
    registry.register(Arc::new(Triangles))?;
    registry.register(Arc::new(KCore))?;
    registry.register(Arc::new(PreferentialAttachment))?;
    registry.register(Arc::new(AdamicAdar))?;
    registry.register(Arc::new(CommonNeighbors))?;
    registry.register(Arc::new(ResourceAllocation))?;
    registry.register(Arc::new(TotalNeighbors))
}

/// Execute a typed rank algorithm through Rust dispatch and return its
/// canonical UUID-only Arrow batch with node properties materialized.
///
/// # Errors
/// Returns structured validation/execution errors for invalid relationship
/// selection, unavailable algorithms, adjacency reads, limits, or shaping.
pub fn rank_algorithm(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    label: TypeId,
    property_stems: &[String],
    options: &RankOptions,
) -> Result<RecordBatch, GfError> {
    let graph = rank_projection(provider, dir, mode, label, options)?;
    let algorithm = Algorithm::Rank(options.by);
    let output = execute_rank(&graph, algorithm, AlgorithmLimits::default())?;
    let batch = shape_algorithm_output(algorithm, &output)?;
    materialize_node_properties(dir, property_stems, &batch).map_err(Into::into)
}

/// Fingerprint the exact logical topology consumed by a rank invocation.
///
/// # Errors
/// Returns the same projection and selector failures as [`rank_algorithm`].
pub fn rank_projection_fingerprint(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    label: TypeId,
    options: &RankOptions,
) -> Result<[u8; 32], GfError> {
    rank_projection(provider, dir, mode, label, options)
        .and_then(|graph| graph.descriptor_projection_fingerprint())
        .map(|fingerprint| *fingerprint.as_bytes())
}

fn rank_projection(
    provider: &dyn AdjacencyProvider,
    dir: &Path,
    mode: OntologyMode,
    label: TypeId,
    options: &RankOptions,
) -> Result<AdjacencyGraph, GfError> {
    let via = options.via.as_deref().unwrap_or("*");
    if via.is_empty() || via.trim() != via || via.chars().any(char::is_control) {
        return Err(GfError::Validation(format!(
            "invalid rank relationship selector {via:?}"
        )));
    }
    let direction = if options.directed {
        Direction::Out
    } else {
        Direction::Undirected
    };
    export_adjacency(
        provider,
        dir,
        mode,
        AdjacencySelection {
            label: Some(label),
            via,
            direction,
            weight: None,
        },
    )
}

fn execute_rank(
    graph: &AdjacencyGraph,
    algorithm: Algorithm,
    limits: AlgorithmLimits,
) -> Result<AlgorithmOutput, AlgorithmError> {
    let mut registry = AlgorithmRegistry::default();
    register_rank_algorithms(&mut registry)?;
    registry.execute(
        algorithm,
        graph,
        &AlgorithmControl::new(limits, AlgorithmCancellation::default()),
    )
}

fn exact_u32(value: usize, kind: &str) -> Result<u32, AlgorithmError> {
    u32::try_from(value).map_err(|_| execution(format!("{kind} exceeds supported score range")))
}

fn exact_u64_as_f64(value: u64, kind: &str) -> Result<f64, AlgorithmError> {
    const MAX_EXACT_INTEGER: u64 = 1_u64 << 53;
    if value > MAX_EXACT_INTEGER {
        return Err(execution(format!("{kind} exceeds supported score range")));
    }
    // Guarded by the exact IEEE-754 integer range above.
    #[allow(clippy::cast_precision_loss)]
    let converted = value as f64;
    Ok(converted)
}

fn execution(message: impl Into<String>) -> AlgorithmError {
    AlgorithmError::Execution {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn execute_degree(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        execute_rank(graph, Algorithm::Rank(RankAlgorithm::Degree), limits)
    }

    fn execute_pagerank(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        registry.execute(
            Algorithm::Rank(RankAlgorithm::PageRank),
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn pagerank_scores(output: &AlgorithmOutput) -> Vec<f64> {
        output
            .rows()
            .iter()
            .map(|row| match row[1] {
                AlgorithmValue::Float64(score) => score,
                _ => panic!("pagerank score must be Float64"),
            })
            .collect()
    }

    fn execute_betweenness(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        registry.execute(
            Algorithm::Rank(RankAlgorithm::Betweenness),
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn betweenness_scores(output: &AlgorithmOutput) -> Vec<f64> {
        output
            .rows()
            .iter()
            .map(|row| match row[1] {
                AlgorithmValue::Float64(score) => score,
                _ => panic!("betweenness score must be Float64"),
            })
            .collect()
    }

    fn execute_closeness(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        registry.execute(
            Algorithm::Rank(RankAlgorithm::Closeness),
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn closeness_scores(output: &AlgorithmOutput) -> Vec<f64> {
        output
            .rows()
            .iter()
            .map(|row| match row[1] {
                AlgorithmValue::Float64(score) => score,
                _ => panic!("closeness score must be Float64"),
            })
            .collect()
    }

    fn execute_harmonic_closeness(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        registry.execute(
            Algorithm::Rank(RankAlgorithm::HarmonicCloseness),
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn harmonic_closeness_scores(output: &AlgorithmOutput) -> Vec<f64> {
        output
            .rows()
            .iter()
            .map(|row| match row[1] {
                AlgorithmValue::Float64(score) => score,
                _ => panic!("harmonic closeness score must be Float64"),
            })
            .collect()
    }

    fn execute_eigenvector(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        registry.execute(
            Algorithm::Rank(RankAlgorithm::Eigenvector),
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn eigenvector_scores(output: &AlgorithmOutput) -> Vec<f64> {
        output
            .rows()
            .iter()
            .map(|row| match row[1] {
                AlgorithmValue::Float64(score) => score,
                _ => panic!("eigenvector score must be Float64"),
            })
            .collect()
    }

    fn eigenvector_scores_for(graph: &AdjacencyGraph) -> Vec<f64> {
        eigenvector_scores(
            &execute_eigenvector(
                graph,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap(),
        )
    }

    fn execute_article_rank(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        registry.execute(
            Algorithm::Rank(RankAlgorithm::ArticleRank),
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn article_rank_scores(output: &AlgorithmOutput) -> Vec<f64> {
        output
            .rows()
            .iter()
            .map(|row| match row[1] {
                AlgorithmValue::Float64(score) => score,
                _ => panic!("ArticleRank score must be Float64"),
            })
            .collect()
    }

    fn execute_hits_hub(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        registry.execute(
            Algorithm::Rank(RankAlgorithm::HitsHub),
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn hits_hub_scores(output: &AlgorithmOutput) -> Vec<f64> {
        output
            .rows()
            .iter()
            .map(|row| match row[1] {
                AlgorithmValue::Float64(score) => score,
                _ => panic!("HITS hub score must be Float64"),
            })
            .collect()
    }

    fn execute_hits_authority(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        registry.execute(
            Algorithm::Rank(RankAlgorithm::HitsAuthority),
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn hits_authority_scores(output: &AlgorithmOutput) -> Vec<f64> {
        output
            .rows()
            .iter()
            .map(|row| match row[1] {
                AlgorithmValue::Float64(score) => score,
                _ => panic!("HITS authority score must be Float64"),
            })
            .collect()
    }

    fn execute_celf(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        registry.execute(
            Algorithm::Rank(RankAlgorithm::Celf),
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn celf_output_scores(output: &AlgorithmOutput) -> Vec<f64> {
        hits_hub_scores(output)
    }

    fn execute_clustering_coefficient(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        registry.execute(
            Algorithm::Rank(RankAlgorithm::ClusteringCoefficient),
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn clustering_coefficient_output_scores(output: &AlgorithmOutput) -> Vec<f64> {
        hits_hub_scores(output)
    }

    fn execute_triangles(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        registry.execute(
            Algorithm::Rank(RankAlgorithm::Triangles),
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn triangle_output_scores(output: &AlgorithmOutput) -> Vec<f64> {
        hits_hub_scores(output)
    }

    fn execute_k_core(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        registry.execute(
            Algorithm::Rank(RankAlgorithm::KCore),
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn k_core_output_scores(output: &AlgorithmOutput) -> Vec<f64> {
        hits_hub_scores(output)
    }

    fn execute_preferential_attachment(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        registry.execute(
            Algorithm::Rank(RankAlgorithm::PreferentialAttachment),
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn preferential_attachment_output_scores(output: &AlgorithmOutput) -> Vec<f64> {
        hits_hub_scores(output)
    }

    fn execute_adamic_adar(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        registry.execute(
            Algorithm::Rank(RankAlgorithm::AdamicAdar),
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn adamic_adar_output_scores(output: &AlgorithmOutput) -> Vec<f64> {
        hits_hub_scores(output)
    }

    fn execute_common_neighbors(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        registry.execute(
            Algorithm::Rank(RankAlgorithm::CommonNeighbors),
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn common_neighbor_output_scores(output: &AlgorithmOutput) -> Vec<f64> {
        hits_hub_scores(output)
    }

    fn execute_resource_allocation(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        registry.execute(
            Algorithm::Rank(RankAlgorithm::ResourceAllocation),
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn resource_allocation_output_scores(output: &AlgorithmOutput) -> Vec<f64> {
        hits_hub_scores(output)
    }

    fn execute_total_neighbors(
        graph: &AdjacencyGraph,
        limits: AlgorithmLimits,
        cancellation: AlgorithmCancellation,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry)?;
        registry.execute(
            Algorithm::Rank(RankAlgorithm::TotalNeighbors),
            graph,
            &AlgorithmControl::new(limits, cancellation),
        )
    }

    fn total_neighbor_output_scores(output: &AlgorithmOutput) -> Vec<f64> {
        hits_hub_scores(output)
    }

    fn assert_scores_close(actual: &[f64], expected: &[f64]) {
        assert_eq!(actual.len(), expected.len());
        for (actual, expected) in actual.iter().zip(expected) {
            assert!(
                (actual - expected).abs() < 1.0e-12,
                "{actual} != {expected}"
            );
        }
    }

    fn assert_scores_within(actual: &[f64], expected: &[f64], tolerance: f64) {
        assert_eq!(actual.len(), expected.len());
        assert!(
            actual
                .iter()
                .zip(expected)
                .all(|(actual, expected)| (actual - expected).abs() <= tolerance)
        );
    }

    #[test]
    fn degree_scores_a_hand_verifiable_fixture_in_stable_uuid_order() {
        let output = execute_degree(
            &AdjacencyGraph::with_test_counts(3, 4),
            AlgorithmLimits::default(),
        )
        .unwrap();
        assert_eq!(
            output.rows(),
            vec![
                vec![AlgorithmValue::Uuid([0; 16]), AlgorithmValue::Float64(2.0)],
                vec![
                    AlgorithmValue::Uuid(u128::from(1_u64).to_be_bytes()),
                    AlgorithmValue::Float64(0.0),
                ],
                vec![
                    AlgorithmValue::Uuid(u128::from(2_u64).to_be_bytes()),
                    AlgorithmValue::Float64(0.0),
                ],
            ]
        );
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry).unwrap();
        assert_eq!(registry.capabilities()[0].dependency, BUILTIN_REVIEW);
    }

    #[test]
    fn degree_handles_empty_graphs_and_shared_resource_limits() {
        assert!(
            execute_degree(&AdjacencyGraph::default(), AlgorithmLimits::default())
                .unwrap()
                .rows()
                .is_empty()
        );
        let limits = AlgorithmLimits {
            nodes: 2,
            ..AlgorithmLimits::default()
        };
        assert_eq!(
            execute_degree(&AdjacencyGraph::with_test_counts(3, 0), limits),
            Err(AlgorithmError::NodeLimit {
                observed: 3,
                limit: 2,
            })
        );
    }

    #[test]
    fn pagerank_scores_hand_verifiable_graphs_deterministically() {
        let cycle = AdjacencyGraph::with_test_edges(2, &[(0, 1), (1, 0)]);
        let first = execute_pagerank(
            &cycle,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_eq!(pagerank_scores(&first), [0.5, 0.5]);
        assert_eq!(
            first,
            execute_pagerank(
                &cycle,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
        );
        assert_eq!(
            first.schema,
            Algorithm::Rank(RankAlgorithm::PageRank).result_schema()
        );

        let disconnected = AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0), (2, 3), (3, 2)]);
        assert_eq!(
            pagerank_scores(
                &execute_pagerank(
                    &disconnected,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap()
            ),
            [0.25, 0.25, 0.25, 0.25]
        );

        let empty = execute_pagerank(
            &AdjacencyGraph::default(),
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert!(empty.rows().is_empty());
    }

    #[test]
    fn pagerank_handles_dangling_parallel_self_loop_and_direction_semantics() {
        let multigraph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (0, 1), (0, 2), (1, 1)]);
        let scores = pagerank_scores(
            &execute_pagerank(
                &multigraph,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap(),
        );
        assert!((scores.iter().sum::<f64>() - 1.0).abs() < 1.0e-9);
        assert!(scores[1] > scores[2]);
        assert!(scores[1] > scores[0]);

        let directed = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
        let undirected = AdjacencyGraph::with_test_edges(2, &[(0, 1), (1, 0)]);
        assert_ne!(
            pagerank_scores(
                &execute_pagerank(
                    &directed,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap()
            ),
            pagerank_scores(
                &execute_pagerank(
                    &undirected,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap()
            )
        );
    }

    #[test]
    fn pagerank_uses_shared_limits_cancellation_and_dependency_metadata() {
        let graph = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
        assert_eq!(
            execute_pagerank(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::IterationLimit {
                observed: 1,
                limit: 0,
            })
        );
        assert!(matches!(
            execute_pagerank(
                &graph,
                AlgorithmLimits {
                    output_rows: 1,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_pagerank(&graph, AlgorithmLimits::default(), cancellation),
            Err(AlgorithmError::Cancelled)
        );
        let registry = {
            let mut registry = AlgorithmRegistry::default();
            register_rank_algorithms(&mut registry).unwrap();
            registry
        };
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|capability| capability.algorithm == Algorithm::Rank(RankAlgorithm::PageRank))
            .unwrap();
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
    }

    #[test]
    fn betweenness_scores_directed_and_undirected_chains_deterministically() {
        let directed = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
        let first = execute_betweenness(
            &directed,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_scores_close(&betweenness_scores(&first), &[0.0, 0.5, 0.0]);
        assert_eq!(
            first,
            execute_betweenness(
                &directed,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
        );
        assert_eq!(
            first.schema,
            Algorithm::Rank(RankAlgorithm::Betweenness).result_schema()
        );

        let undirected = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0), (1, 2), (2, 1)]);
        assert_scores_close(
            &betweenness_scores(
                &execute_betweenness(
                    &undirected,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[0.0, 1.0, 0.0],
        );
    }

    #[test]
    fn betweenness_handles_parallel_self_loop_disconnected_and_empty_graphs() {
        let multigraph =
            AdjacencyGraph::with_test_edges(4, &[(0, 1), (0, 1), (1, 2), (0, 3), (3, 2), (1, 1)]);
        assert_scores_close(
            &betweenness_scores(
                &execute_betweenness(
                    &multigraph,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[0.0, 1.0 / 9.0, 0.0, 1.0 / 18.0],
        );

        let disconnected = AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 2)]);
        assert_scores_close(
            &betweenness_scores(
                &execute_betweenness(
                    &disconnected,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[0.0, 1.0 / 6.0, 0.0, 0.0],
        );
        assert!(
            execute_betweenness(
                &AdjacencyGraph::default(),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows()
            .is_empty()
        );
    }

    #[test]
    fn betweenness_uses_shared_limits_cancellation_and_dependency_metadata() {
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
        assert_eq!(
            execute_betweenness(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::IterationLimit {
                observed: 1,
                limit: 0,
            })
        );
        assert!(matches!(
            execute_betweenness(
                &graph,
                AlgorithmLimits {
                    nodes: 2,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::NodeLimit { .. })
        ));
        assert!(matches!(
            execute_betweenness(
                &graph,
                AlgorithmLimits {
                    output_rows: 2,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_betweenness(&graph, AlgorithmLimits::default(), cancellation),
            Err(AlgorithmError::Cancelled)
        );
        let edge_heavy = AdjacencyGraph::with_test_edges(1, &vec![(0, 0); 1025]);
        assert_eq!(
            execute_betweenness(
                &edge_heavy,
                AlgorithmLimits {
                    iterations: 1,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::IterationLimit {
                observed: 2,
                limit: 1,
            })
        );
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry).unwrap();
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|capability| capability.algorithm == Algorithm::Rank(RankAlgorithm::Betweenness))
            .unwrap();
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
    }

    #[test]
    fn closeness_scores_directed_and_undirected_chains_deterministically() {
        let directed = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
        let first = execute_closeness(
            &directed,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_scores_close(&closeness_scores(&first), &[2.0 / 3.0, 0.5, 0.0]);
        assert_eq!(
            first,
            execute_closeness(
                &directed,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
        );
        assert_eq!(
            first.schema,
            Algorithm::Rank(RankAlgorithm::Closeness).result_schema()
        );

        let undirected = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0), (1, 2), (2, 1)]);
        assert_scores_close(
            &closeness_scores(
                &execute_closeness(
                    &undirected,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[2.0 / 3.0, 1.0, 2.0 / 3.0],
        );
    }

    #[test]
    fn closeness_handles_parallel_self_loop_disconnected_and_empty_graphs() {
        let graph = AdjacencyGraph::with_test_edges(4, &[(0, 1), (0, 1), (1, 2), (1, 1)]);
        assert_scores_close(
            &closeness_scores(
                &execute_closeness(
                    &graph,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[4.0 / 9.0, 1.0 / 3.0, 0.0, 0.0],
        );
        assert!(
            execute_closeness(
                &AdjacencyGraph::default(),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows()
            .is_empty()
        );
        assert_eq!(
            closeness_scores(
                &execute_closeness(
                    &AdjacencyGraph::with_test_counts(1, 0),
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap()
            ),
            [0.0]
        );
    }

    #[test]
    fn closeness_uses_shared_limits_cancellation_and_dependency_metadata() {
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
        assert_eq!(
            execute_closeness(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::IterationLimit {
                observed: 1,
                limit: 0,
            })
        );
        assert!(matches!(
            execute_closeness(
                &graph,
                AlgorithmLimits {
                    nodes: 2,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::NodeLimit { .. })
        ));
        assert!(matches!(
            execute_closeness(
                &graph,
                AlgorithmLimits {
                    output_rows: 2,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_closeness(&graph, AlgorithmLimits::default(), cancellation),
            Err(AlgorithmError::Cancelled)
        );
        let edge_heavy = AdjacencyGraph::with_test_edges(1, &vec![(0, 0); 1025]);
        assert_eq!(
            execute_closeness(
                &edge_heavy,
                AlgorithmLimits {
                    iterations: 1,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::IterationLimit {
                observed: 2,
                limit: 1,
            })
        );
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry).unwrap();
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|capability| capability.algorithm == Algorithm::Rank(RankAlgorithm::Closeness))
            .unwrap();
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
    }

    #[test]
    fn harmonic_closeness_scores_directed_and_undirected_chains_deterministically() {
        let directed = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
        let first = execute_harmonic_closeness(
            &directed,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_scores_close(&harmonic_closeness_scores(&first), &[0.75, 0.5, 0.0]);
        assert_eq!(
            first,
            execute_harmonic_closeness(
                &directed,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
        );
        assert_eq!(
            first.schema,
            Algorithm::Rank(RankAlgorithm::HarmonicCloseness).result_schema()
        );

        let undirected = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0), (1, 2), (2, 1)]);
        assert_scores_close(
            &harmonic_closeness_scores(
                &execute_harmonic_closeness(
                    &undirected,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[0.75, 1.0, 0.75],
        );
    }

    #[test]
    fn harmonic_closeness_handles_parallel_self_loop_disconnected_and_empty_graphs() {
        let graph = AdjacencyGraph::with_test_edges(4, &[(0, 1), (0, 1), (1, 2), (1, 1)]);
        assert_scores_close(
            &harmonic_closeness_scores(
                &execute_harmonic_closeness(
                    &graph,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[0.5, 1.0 / 3.0, 0.0, 0.0],
        );
        assert!(
            execute_harmonic_closeness(
                &AdjacencyGraph::default(),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows()
            .is_empty()
        );
        assert_eq!(
            harmonic_closeness_scores(
                &execute_harmonic_closeness(
                    &AdjacencyGraph::with_test_counts(1, 0),
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap()
            ),
            [0.0]
        );
    }

    #[test]
    fn harmonic_closeness_uses_shared_limits_cancellation_and_dependency_metadata() {
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
        assert_eq!(
            execute_harmonic_closeness(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::IterationLimit {
                observed: 1,
                limit: 0,
            })
        );
        assert!(matches!(
            execute_harmonic_closeness(
                &graph,
                AlgorithmLimits {
                    nodes: 2,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::NodeLimit { .. })
        ));
        assert!(matches!(
            execute_harmonic_closeness(
                &graph,
                AlgorithmLimits {
                    output_rows: 2,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_harmonic_closeness(&graph, AlgorithmLimits::default(), cancellation),
            Err(AlgorithmError::Cancelled)
        );
        let edge_heavy = AdjacencyGraph::with_test_edges(1, &vec![(0, 0); 1025]);
        assert_eq!(
            execute_harmonic_closeness(
                &edge_heavy,
                AlgorithmLimits {
                    iterations: 1,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::IterationLimit {
                observed: 2,
                limit: 1,
            })
        );
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry).unwrap();
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|capability| {
                capability.algorithm == Algorithm::Rank(RankAlgorithm::HarmonicCloseness)
            })
            .unwrap();
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
    }

    #[test]
    fn eigenvector_scores_shifted_power_fixtures_deterministically() {
        let cycle = AdjacencyGraph::with_test_edges(2, &[(0, 1), (1, 0)]);
        let first = execute_eigenvector(
            &cycle,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_scores_close(
            &eigenvector_scores(&first),
            &[1.0 / 2.0_f64.sqrt(), 1.0 / 2.0_f64.sqrt()],
        );
        assert_eq!(
            first,
            execute_eigenvector(
                &cycle,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
        );
        assert_eq!(
            first.schema,
            Algorithm::Rank(RankAlgorithm::Eigenvector).result_schema()
        );

        let star = AdjacencyGraph::with_test_edges(3, &[(0, 1), (0, 2), (1, 0), (2, 0)]);
        assert_scores_within(
            &eigenvector_scores_for(&star),
            &[1.0 / 2.0_f64.sqrt(), 0.5, 0.5],
            EIGENVECTOR_TOLERANCE,
        );
    }

    #[test]
    fn eigenvector_handles_direction_multigraph_disconnected_and_empty_graphs() {
        let directed = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
        let denominator = (1.0_f64 + 21.0_f64.powi(2)).sqrt();
        assert_scores_close(
            &eigenvector_scores_for(&directed),
            &[1.0 / denominator, 21.0 / denominator],
        );
        let parallel = AdjacencyGraph::with_test_edges(2, &[(0, 1), (0, 1)]);
        assert!(eigenvector_scores_for(&parallel)[1] > eigenvector_scores_for(&directed)[1]);
        let with_self_loop = AdjacencyGraph::with_test_edges(2, &[(0, 1), (0, 0)]);
        assert_ne!(
            eigenvector_scores_for(&with_self_loop),
            eigenvector_scores_for(&directed)
        );

        let disconnected = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0)]);
        assert_scores_close(
            &eigenvector_scores_for(&disconnected),
            &[
                1.0 / (2.0 + 2.0_f64.powi(-40)).sqrt(),
                1.0 / (2.0 + 2.0_f64.powi(-40)).sqrt(),
                2.0_f64.powi(-20) / (2.0 + 2.0_f64.powi(-40)).sqrt(),
            ],
        );
        assert_scores_close(
            &eigenvector_scores_for(&AdjacencyGraph::with_test_counts(4, 0)),
            &[0.5; 4],
        );
        assert_eq!(
            eigenvector_scores_for(&AdjacencyGraph::with_test_counts(1, 0)),
            [1.0]
        );
        assert!(
            execute_eigenvector(
                &AdjacencyGraph::default(),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows()
            .is_empty()
        );
    }

    #[test]
    fn eigenvector_uses_shared_limits_cancellation_and_dependency_metadata() {
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
        assert_eq!(
            execute_eigenvector(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::IterationLimit {
                observed: 1,
                limit: 0,
            })
        );
        assert!(matches!(
            execute_eigenvector(
                &graph,
                AlgorithmLimits {
                    nodes: 2,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::NodeLimit { .. })
        ));
        assert!(matches!(
            execute_eigenvector(
                &graph,
                AlgorithmLimits {
                    output_rows: 2,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_eigenvector(&graph, AlgorithmLimits::default(), cancellation),
            Err(AlgorithmError::Cancelled)
        );
        let edge_heavy = AdjacencyGraph::with_test_edges(1, &vec![(0, 0); 1025]);
        assert_eq!(
            execute_eigenvector(
                &edge_heavy,
                AlgorithmLimits {
                    iterations: 1,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::IterationLimit {
                observed: 2,
                limit: 1,
            })
        );
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry).unwrap();
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|capability| capability.algorithm == Algorithm::Rank(RankAlgorithm::Eigenvector))
            .unwrap();
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
    }

    #[test]
    fn article_rank_scores_the_canonical_recurrence_deterministically() {
        let graph = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
        let first = execute_article_rank(
            &graph,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_scores_close(&article_rank_scores(&first), &[0.15, 0.235]);
        assert_eq!(
            first,
            execute_article_rank(
                &graph,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
        );
        assert_eq!(
            first.schema,
            Algorithm::Rank(RankAlgorithm::ArticleRank).result_schema()
        );
    }

    #[test]
    fn article_rank_handles_direction_multigraph_disconnected_and_empty_graphs() {
        let directed = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
        let undirected = AdjacencyGraph::with_test_edges(2, &[(0, 1), (1, 0)]);
        assert_ne!(
            article_rank_scores(
                &execute_article_rank(
                    &directed,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default()
                )
                .unwrap()
            ),
            article_rank_scores(
                &execute_article_rank(
                    &undirected,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default()
                )
                .unwrap()
            )
        );
        let multigraph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (0, 1), (0, 2), (1, 1)]);
        let scores = article_rank_scores(
            &execute_article_rank(
                &multigraph,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap(),
        );
        assert!(scores[1] > scores[2]);
        assert!(scores[1] > scores[0]);
        let disconnected = AdjacencyGraph::with_test_edges(3, &[(0, 1)]);
        assert_scores_close(
            &article_rank_scores(
                &execute_article_rank(
                    &disconnected,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[0.15, 0.245_625, 0.15],
        );
        assert_scores_close(
            &article_rank_scores(
                &execute_article_rank(
                    &AdjacencyGraph::with_test_counts(3, 0),
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[0.15; 3],
        );
        assert!(
            execute_article_rank(
                &AdjacencyGraph::default(),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default()
            )
            .unwrap()
            .rows()
            .is_empty()
        );
    }

    #[test]
    fn article_rank_uses_shared_limits_cancellation_and_dependency_metadata() {
        let graph = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
        assert_eq!(
            execute_article_rank(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default()
            ),
            Err(AlgorithmError::IterationLimit {
                observed: 1,
                limit: 0
            })
        );
        assert!(matches!(
            execute_article_rank(
                &graph,
                AlgorithmLimits {
                    output_rows: 1,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default()
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_article_rank(&graph, AlgorithmLimits::default(), cancellation),
            Err(AlgorithmError::Cancelled)
        );
        let edge_heavy = AdjacencyGraph::with_test_edges(1, &vec![(0, 0); 1025]);
        assert!(matches!(
            execute_article_rank(
                &edge_heavy,
                AlgorithmLimits {
                    iterations: 1,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default()
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry).unwrap();
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|capability| capability.algorithm == Algorithm::Rank(RankAlgorithm::ArticleRank))
            .unwrap();
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
    }

    #[test]
    fn hits_hub_scores_the_canonical_recurrence_deterministically() {
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
        let first = execute_hits_hub(
            &graph,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_scores_close(
            &hits_hub_scores(&first),
            &[1.0 / 2.0_f64.sqrt(), 1.0 / 2.0_f64.sqrt(), 0.0],
        );
        assert_eq!(
            first,
            execute_hits_hub(
                &graph,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
        );
        assert_eq!(
            first.schema,
            Algorithm::Rank(RankAlgorithm::HitsHub).result_schema()
        );
    }

    #[test]
    fn hits_hub_handles_direction_multigraph_disconnected_and_empty_graphs() {
        let parallel = AdjacencyGraph::with_test_edges(3, &[(0, 2), (0, 2), (1, 2)]);
        assert_scores_close(
            &hits_hub_scores(
                &execute_hits_hub(
                    &parallel,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[2.0 / 5.0_f64.sqrt(), 1.0 / 5.0_f64.sqrt(), 0.0],
        );
        let self_loop = AdjacencyGraph::with_test_edges(1, &[(0, 0)]);
        assert_scores_close(
            &hits_hub_scores(
                &execute_hits_hub(
                    &self_loop,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[1.0],
        );
        let undirected_disconnected = AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0)]);
        assert_scores_close(
            &hits_hub_scores(
                &execute_hits_hub(
                    &undirected_disconnected,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[1.0 / 2.0_f64.sqrt(), 1.0 / 2.0_f64.sqrt(), 0.0, 0.0],
        );
        assert_scores_close(
            &hits_hub_scores(
                &execute_hits_hub(
                    &AdjacencyGraph::with_test_counts(2, 0),
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[0.0, 0.0],
        );
        assert!(
            execute_hits_hub(
                &AdjacencyGraph::default(),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default()
            )
            .unwrap()
            .rows()
            .is_empty()
        );
    }

    #[test]
    fn hits_hub_uses_shared_limits_cancellation_and_dependency_metadata() {
        let graph = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
        assert_eq!(
            execute_hits_hub(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default()
            ),
            Err(AlgorithmError::IterationLimit {
                observed: 1,
                limit: 0
            })
        );
        assert!(matches!(
            execute_hits_hub(
                &graph,
                AlgorithmLimits {
                    output_rows: 1,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default()
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_hits_hub(&graph, AlgorithmLimits::default(), cancellation),
            Err(AlgorithmError::Cancelled)
        );
        let edge_heavy = AdjacencyGraph::with_test_edges(1, &vec![(0, 0); 1025]);
        assert!(matches!(
            execute_hits_hub(
                &edge_heavy,
                AlgorithmLimits {
                    iterations: 1,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default()
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry).unwrap();
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|capability| capability.algorithm == Algorithm::Rank(RankAlgorithm::HitsHub))
            .unwrap();
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
    }

    #[test]
    fn hits_authority_scores_the_canonical_recurrence() {
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]);
        let first = execute_hits_authority(
            &graph,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_scores_close(
            &hits_authority_scores(&first),
            &[0.0, 1.0 / 2.0_f64.sqrt(), 1.0 / 2.0_f64.sqrt()],
        );
        assert_eq!(
            first.schema,
            Algorithm::Rank(RankAlgorithm::HitsAuthority).result_schema()
        );
    }

    #[test]
    fn hits_authority_handles_multigraph_disconnected_and_empty_graphs() {
        let parallel = AdjacencyGraph::with_test_edges(3, &[(0, 1), (0, 1), (0, 2)]);
        assert_scores_close(
            &hits_authority_scores(
                &execute_hits_authority(
                    &parallel,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[0.0, 2.0 / 5.0_f64.sqrt(), 1.0 / 5.0_f64.sqrt()],
        );
        let self_loop = AdjacencyGraph::with_test_edges(1, &[(0, 0)]);
        assert_scores_close(
            &hits_authority_scores(
                &execute_hits_authority(
                    &self_loop,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[1.0],
        );
        let undirected_disconnected = AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0)]);
        assert_scores_close(
            &hits_authority_scores(
                &execute_hits_authority(
                    &undirected_disconnected,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[1.0 / 2.0_f64.sqrt(), 1.0 / 2.0_f64.sqrt(), 0.0, 0.0],
        );
        assert_scores_close(
            &hits_authority_scores(
                &execute_hits_authority(
                    &AdjacencyGraph::with_test_counts(2, 0),
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[0.0, 0.0],
        );
        assert!(
            execute_hits_authority(
                &AdjacencyGraph::default(),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default()
            )
            .unwrap()
            .rows()
            .is_empty()
        );
    }

    #[test]
    fn hits_authority_uses_shared_limits_cancellation_and_dependency_metadata() {
        let graph = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
        assert_eq!(
            execute_hits_authority(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default()
            ),
            Err(AlgorithmError::IterationLimit {
                observed: 1,
                limit: 0
            })
        );
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_hits_authority(&graph, AlgorithmLimits::default(), cancellation),
            Err(AlgorithmError::Cancelled)
        );
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry).unwrap();
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|capability| {
                capability.algorithm == Algorithm::Rank(RankAlgorithm::HitsAuthority)
            })
            .unwrap();
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
    }

    #[test]
    fn celf_scores_edgeless_graphs_deterministically() {
        let graph = AdjacencyGraph::with_test_counts(3, 0);
        let first = execute_celf(
            &graph,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_scores_close(&celf_output_scores(&first), &[1.0, 1.0, 1.0]);
    }

    #[test]
    fn celf_handles_direction_multigraph_self_loop_and_empty_graphs() {
        let single = celf_output_scores(
            &execute_celf(
                &AdjacencyGraph::with_test_edges(3, &[(0, 1)]),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap(),
        );
        assert!(single[0] > 1.0 && single[1] < 1.0 && (single[2] - 1.0).abs() <= 1.0e-12);
        let parallel = celf_output_scores(
            &execute_celf(
                &AdjacencyGraph::with_test_edges(2, &[(0, 1), (0, 1)]),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap(),
        );
        assert!(parallel[0] > single[0]);
        assert_scores_close(
            &celf_output_scores(
                &execute_celf(
                    &AdjacencyGraph::with_test_edges(1, &[(0, 0)]),
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[1.0],
        );
        let directed_chain = celf_output_scores(
            &execute_celf(
                &AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2)]),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap(),
        );
        let symmetric_chain = celf_output_scores(
            &execute_celf(
                &AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0), (1, 2), (2, 1)]),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap(),
        );
        assert_ne!(symmetric_chain, directed_chain);
    }

    #[test]
    fn celf_uses_shared_limits_cancellation_and_dependency_metadata() {
        let graph = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
        assert!(matches!(
            execute_celf(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default()
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_celf(&graph, AlgorithmLimits::default(), cancellation),
            Err(AlgorithmError::Cancelled)
        );
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry).unwrap();
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|capability| capability.algorithm == Algorithm::Rank(RankAlgorithm::Celf))
            .unwrap();
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
    }

    #[test]
    fn clustering_coefficient_matches_directed_and_undirected_triangles() {
        let directed_cycle = execute_clustering_coefficient(
            &AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2), (2, 0)]),
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_scores_close(
            &clustering_coefficient_output_scores(&directed_cycle),
            &[0.5, 0.5, 0.5],
        );

        let undirected_triangle = execute_clustering_coefficient(
            &AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0), (1, 2), (2, 1), (2, 0), (0, 2)]),
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_scores_close(
            &clustering_coefficient_output_scores(&undirected_triangle),
            &[1.0, 1.0, 1.0],
        );
    }

    #[test]
    fn clustering_coefficient_simplifies_multigraphs_and_retains_all_nodes() {
        let graph = AdjacencyGraph::with_test_edges(
            5,
            &[
                (0, 1),
                (0, 1),
                (1, 0),
                (1, 2),
                (2, 1),
                (2, 0),
                (0, 2),
                (0, 0),
                (3, 4),
            ],
        );
        let first = execute_clustering_coefficient(
            &graph,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_scores_close(
            &clustering_coefficient_output_scores(&first),
            &[1.0, 1.0, 1.0, 0.0, 0.0],
        );
        assert_eq!(
            first,
            execute_clustering_coefficient(
                &graph,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
        );
        assert!(
            execute_clustering_coefficient(
                &AdjacencyGraph::default(),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows()
            .is_empty()
        );
    }

    #[test]
    fn clustering_coefficient_uses_shared_controls_and_canonical_metadata() {
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2), (2, 0)]);
        assert!(matches!(
            execute_clustering_coefficient(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default()
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_clustering_coefficient(&graph, AlgorithmLimits::default(), cancellation),
            Err(AlgorithmError::Cancelled)
        );
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry).unwrap();
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|capability| {
                capability.algorithm == Algorithm::Rank(RankAlgorithm::ClusteringCoefficient)
            })
            .unwrap();
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
        assert_eq!(capability.algorithm.as_str(), "clustering_coefficient");
    }

    #[test]
    fn triangles_count_overlapping_cliques_in_stable_node_order() {
        let graph = AdjacencyGraph::with_test_edges(
            6,
            &[(0, 1), (1, 2), (2, 0), (0, 2), (2, 3), (3, 0), (4, 5)],
        );
        let output = execute_triangles(
            &graph,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_scores_close(
            &triangle_output_scores(&output),
            &[2.0, 1.0, 2.0, 1.0, 0.0, 0.0],
        );
        assert_eq!(
            output,
            execute_triangles(
                &graph,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
        );
    }

    #[test]
    fn triangles_ignore_direction_multiplicity_and_self_loops() {
        let directed =
            AdjacencyGraph::with_test_edges(4, &[(0, 1), (0, 1), (1, 0), (1, 2), (2, 0), (0, 0)]);
        let reciprocal =
            AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0), (1, 2), (2, 1), (2, 0), (0, 2)]);
        for graph in [&directed, &reciprocal] {
            assert_scores_close(
                &triangle_output_scores(
                    &execute_triangles(
                        graph,
                        AlgorithmLimits::default(),
                        AlgorithmCancellation::default(),
                    )
                    .unwrap(),
                ),
                &[1.0, 1.0, 1.0, 0.0],
            );
        }
        assert!(
            execute_triangles(
                &AdjacencyGraph::default(),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows()
            .is_empty()
        );
    }

    #[test]
    fn triangles_use_shared_controls_and_dependency_metadata() {
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2), (2, 0)]);
        assert!(matches!(
            execute_triangles(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_triangles(&graph, AlgorithmLimits::default(), cancellation),
            Err(AlgorithmError::Cancelled)
        );
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry).unwrap();
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|capability| capability.algorithm == Algorithm::Rank(RankAlgorithm::Triangles))
            .unwrap();
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
        assert_eq!(capability.algorithm.as_str(), "triangles");
    }

    #[test]
    fn k_core_peels_hand_verifiable_disconnected_layers() {
        let graph = AdjacencyGraph::with_test_edges(
            10,
            &[
                (0, 1),
                (0, 2),
                (0, 3),
                (1, 2),
                (1, 3),
                (2, 3),
                (0, 4),
                (4, 5),
                (7, 8),
                (8, 9),
                (9, 7),
            ],
        );
        let output = execute_k_core(
            &graph,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_scores_close(
            &k_core_output_scores(&output),
            &[3.0, 3.0, 3.0, 3.0, 1.0, 1.0, 0.0, 2.0, 2.0, 2.0],
        );
        assert_eq!(
            output,
            execute_k_core(
                &graph,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
        );
    }

    #[test]
    fn k_core_ignores_direction_multiplicity_and_self_loops() {
        let directed =
            AdjacencyGraph::with_test_edges(4, &[(0, 1), (0, 1), (1, 0), (1, 2), (2, 0), (0, 0)]);
        let reciprocal =
            AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0), (1, 2), (2, 1), (2, 0), (0, 2)]);
        for graph in [&directed, &reciprocal] {
            assert_scores_close(
                &k_core_output_scores(
                    &execute_k_core(
                        graph,
                        AlgorithmLimits::default(),
                        AlgorithmCancellation::default(),
                    )
                    .unwrap(),
                ),
                &[2.0, 2.0, 2.0, 0.0],
            );
        }
        assert!(
            execute_k_core(
                &AdjacencyGraph::default(),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows()
            .is_empty()
        );
    }

    #[test]
    fn k_core_uses_shared_controls_and_dependency_metadata() {
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 2), (2, 0)]);
        assert!(matches!(
            execute_k_core(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_k_core(&graph, AlgorithmLimits::default(), cancellation),
            Err(AlgorithmError::Cancelled)
        );
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry).unwrap();
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|capability| capability.algorithm == Algorithm::Rank(RankAlgorithm::KCore))
            .unwrap();
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
        assert_eq!(capability.algorithm.as_str(), "k_core");
    }

    #[test]
    fn k_core_batches_heap_entry_checkpoints() {
        let edges: Vec<(u64, u64)> = (1..=6_000).map(|leaf| (leaf, 0)).collect();
        let output = execute_k_core(
            &AdjacencyGraph::with_test_edges(6_001, &edges),
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert!(
            k_core_output_scores(&output)
                .into_iter()
                .all(|score| score == 1.0)
        );
    }

    #[test]
    fn preferential_attachment_aggregates_missing_directed_links() {
        let graph = AdjacencyGraph::with_test_edges(
            5,
            &[(0, 1), (0, 1), (0, 2), (0, 0), (1, 2), (2, 0), (3, 2)],
        );
        let output = execute_preferential_attachment(
            &graph,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_scores_close(
            &preferential_attachment_output_scores(&output),
            &[2.0, 3.0, 2.0, 3.0, 0.0],
        );
        assert_eq!(
            output,
            execute_preferential_attachment(
                &graph,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
        );
    }

    #[test]
    fn preferential_attachment_obeys_undirected_and_boundary_contracts() {
        let undirected = AdjacencyGraph::with_test_edges(
            5,
            &[
                (0, 1),
                (1, 0),
                (0, 2),
                (2, 0),
                (1, 2),
                (2, 1),
                (2, 3),
                (3, 2),
            ],
        );
        assert_scores_close(
            &preferential_attachment_output_scores(
                &execute_preferential_attachment(
                    &undirected,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[2.0, 2.0, 0.0, 4.0, 0.0],
        );

        let disconnected = AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0), (2, 3), (3, 2)]);
        assert_scores_close(
            &preferential_attachment_output_scores(
                &execute_preferential_attachment(
                    &disconnected,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[2.0, 2.0, 2.0, 2.0],
        );
        let complete =
            AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0), (0, 2), (2, 0), (1, 2), (2, 1)]);
        assert!(
            preferential_attachment_output_scores(
                &execute_preferential_attachment(
                    &complete,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            )
            .into_iter()
            .all(|score| score == 0.0)
        );
        assert!(
            preferential_attachment_output_scores(
                &execute_preferential_attachment(
                    &AdjacencyGraph::with_test_counts(3, 0),
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            )
            .into_iter()
            .all(|score| score == 0.0)
        );
        assert!(
            execute_preferential_attachment(
                &AdjacencyGraph::default(),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows()
            .is_empty()
        );
    }

    #[test]
    fn preferential_attachment_uses_shared_controls_and_dependency_metadata() {
        let graph = AdjacencyGraph::with_test_edges(2, &[(0, 1)]);
        assert!(matches!(
            execute_preferential_attachment(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_preferential_attachment(&graph, AlgorithmLimits::default(), cancellation),
            Err(AlgorithmError::Cancelled)
        );
        assert!(matches!(
            exact_u64_as_f64((1_u64 << 53) + 1, "preferential-attachment score"),
            Err(AlgorithmError::Execution { .. })
        ));
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry).unwrap();
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|capability| {
                capability.algorithm == Algorithm::Rank(RankAlgorithm::PreferentialAttachment)
            })
            .unwrap();
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
        assert_eq!(capability.algorithm.as_str(), "preferential_attachment");
    }

    #[test]
    fn adamic_adar_aggregates_missing_directed_links_deterministically() {
        let graph = AdjacencyGraph::with_test_edges(
            5,
            &[
                (0, 2),
                (0, 2),
                (0, 3),
                (0, 0),
                (1, 2),
                (1, 3),
                (2, 0),
                (2, 4),
                (3, 4),
            ],
        );
        let output = execute_adamic_adar(
            &graph,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        let inverse_log_two = 1.0 / 2.0_f64.ln();
        assert_scores_close(
            &adamic_adar_output_scores(&output),
            &[
                2.0 * inverse_log_two,
                2.0 * inverse_log_two,
                inverse_log_two,
                inverse_log_two,
                0.0,
            ],
        );
        assert_eq!(
            output,
            execute_adamic_adar(
                &graph,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
        );
    }

    #[test]
    fn adamic_adar_obeys_undirected_and_boundary_contracts() {
        let undirected = AdjacencyGraph::with_test_edges(
            5,
            &[
                (0, 2),
                (2, 0),
                (0, 3),
                (3, 0),
                (1, 2),
                (2, 1),
                (1, 3),
                (3, 1),
                (2, 4),
                (4, 2),
                (3, 4),
                (4, 3),
            ],
        );
        let inverse_log_two = 1.0 / 2.0_f64.ln();
        let inverse_log_three = 1.0 / 3.0_f64.ln();
        assert_scores_close(
            &adamic_adar_output_scores(
                &execute_adamic_adar(
                    &undirected,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[
                4.0 * inverse_log_three,
                4.0 * inverse_log_three,
                3.0 * inverse_log_two,
                3.0 * inverse_log_two,
                4.0 * inverse_log_three,
            ],
        );
        for graph in [
            AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0), (0, 2), (2, 0), (1, 2), (2, 1)]),
            AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0), (2, 3), (3, 2)]),
            AdjacencyGraph::with_test_counts(3, 0),
        ] {
            assert!(
                adamic_adar_output_scores(
                    &execute_adamic_adar(
                        &graph,
                        AlgorithmLimits::default(),
                        AlgorithmCancellation::default(),
                    )
                    .unwrap(),
                )
                .into_iter()
                .all(|score| score == 0.0)
            );
        }
        assert!(
            execute_adamic_adar(
                &AdjacencyGraph::default(),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows()
            .is_empty()
        );
    }

    #[test]
    fn adamic_adar_uses_shared_controls_and_dependency_metadata() {
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 2), (1, 2)]);
        assert!(matches!(
            execute_adamic_adar(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        assert!(matches!(
            execute_adamic_adar(
                &graph,
                AlgorithmLimits {
                    output_rows: 2,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_adamic_adar(&graph, AlgorithmLimits::default(), cancellation),
            Err(AlgorithmError::Cancelled)
        );
        assert!(matches!(
            adamic_discount(1),
            Err(AlgorithmError::Execution { .. })
        ));
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry).unwrap();
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|capability| capability.algorithm == Algorithm::Rank(RankAlgorithm::AdamicAdar))
            .unwrap();
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
        assert_eq!(capability.algorithm.as_str(), "adamic_adar");
    }

    #[test]
    fn common_neighbors_aggregates_missing_directed_links_deterministically() {
        let graph = AdjacencyGraph::with_test_edges(
            5,
            &[
                (0, 2),
                (0, 2),
                (0, 3),
                (0, 0),
                (1, 2),
                (1, 3),
                (2, 0),
                (2, 4),
                (3, 4),
            ],
        );
        let output = execute_common_neighbors(
            &graph,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_eq!(
            common_neighbor_output_scores(&output),
            [2.0, 2.0, 1.0, 1.0, 0.0]
        );
        assert_eq!(
            output,
            execute_common_neighbors(
                &graph,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
        );
    }

    #[test]
    fn common_neighbors_obeys_undirected_and_boundary_contracts() {
        let undirected = AdjacencyGraph::with_test_edges(
            5,
            &[
                (0, 2),
                (2, 0),
                (0, 3),
                (3, 0),
                (1, 2),
                (2, 1),
                (1, 3),
                (3, 1),
                (2, 4),
                (4, 2),
                (3, 4),
                (4, 3),
            ],
        );
        assert_eq!(
            common_neighbor_output_scores(
                &execute_common_neighbors(
                    &undirected,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            [4.0, 4.0, 3.0, 3.0, 4.0]
        );
        for graph in [
            AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0), (0, 2), (2, 0), (1, 2), (2, 1)]),
            AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0), (2, 3), (3, 2)]),
            AdjacencyGraph::with_test_counts(3, 0),
        ] {
            assert!(
                common_neighbor_output_scores(
                    &execute_common_neighbors(
                        &graph,
                        AlgorithmLimits::default(),
                        AlgorithmCancellation::default(),
                    )
                    .unwrap(),
                )
                .into_iter()
                .all(|score| score == 0.0)
            );
        }
        assert!(
            execute_common_neighbors(
                &AdjacencyGraph::default(),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows()
            .is_empty()
        );
    }

    #[test]
    fn common_neighbors_uses_shared_controls_and_dependency_metadata() {
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 2), (1, 2)]);
        assert!(matches!(
            execute_common_neighbors(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        assert!(matches!(
            execute_common_neighbors(
                &graph,
                AlgorithmLimits {
                    output_rows: 2,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_common_neighbors(&graph, AlgorithmLimits::default(), cancellation),
            Err(AlgorithmError::Cancelled)
        );
        assert!(matches!(
            exact_u64_as_f64((1_u64 << 53) + 1, "common-neighbors score"),
            Err(AlgorithmError::Execution { .. })
        ));
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry).unwrap();
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|capability| {
                capability.algorithm == Algorithm::Rank(RankAlgorithm::CommonNeighbors)
            })
            .unwrap();
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
        assert_eq!(capability.algorithm.as_str(), "common_neighbors");
    }

    #[test]
    fn resource_allocation_aggregates_missing_directed_links_deterministically() {
        let graph = AdjacencyGraph::with_test_edges(
            5,
            &[
                (0, 2),
                (0, 2),
                (0, 3),
                (0, 0),
                (1, 2),
                (1, 3),
                (2, 0),
                (2, 4),
                (3, 4),
            ],
        );
        let output = execute_resource_allocation(
            &graph,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_scores_close(
            &resource_allocation_output_scores(&output),
            &[1.0, 1.0, 0.5, 0.5, 0.0],
        );
        assert_eq!(
            output,
            execute_resource_allocation(
                &graph,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
        );
    }

    #[test]
    fn resource_allocation_obeys_undirected_and_boundary_contracts() {
        let undirected = AdjacencyGraph::with_test_edges(
            5,
            &[
                (0, 2),
                (2, 0),
                (0, 3),
                (3, 0),
                (1, 2),
                (2, 1),
                (1, 3),
                (3, 1),
                (2, 4),
                (4, 2),
                (3, 4),
                (4, 3),
            ],
        );
        assert_scores_close(
            &resource_allocation_output_scores(
                &execute_resource_allocation(
                    &undirected,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap(),
            ),
            &[4.0 / 3.0, 4.0 / 3.0, 1.5, 1.5, 4.0 / 3.0],
        );
        for graph in [
            AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0), (0, 2), (2, 0), (1, 2), (2, 1)]),
            AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0), (2, 3), (3, 2)]),
            AdjacencyGraph::with_test_counts(3, 0),
        ] {
            assert!(
                resource_allocation_output_scores(
                    &execute_resource_allocation(
                        &graph,
                        AlgorithmLimits::default(),
                        AlgorithmCancellation::default(),
                    )
                    .unwrap(),
                )
                .into_iter()
                .all(|score| score == 0.0)
            );
        }
        assert!(
            execute_resource_allocation(
                &AdjacencyGraph::default(),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows()
            .is_empty()
        );
    }

    #[test]
    fn resource_allocation_uses_shared_controls_and_dependency_metadata() {
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 2), (1, 2)]);
        assert!(matches!(
            execute_resource_allocation(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        assert!(matches!(
            execute_resource_allocation(
                &graph,
                AlgorithmLimits {
                    output_rows: 2,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_resource_allocation(&graph, AlgorithmLimits::default(), cancellation),
            Err(AlgorithmError::Cancelled)
        );
        assert!(matches!(
            resource_allocation_discount(1),
            Err(AlgorithmError::Execution { .. })
        ));
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry).unwrap();
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|capability| {
                capability.algorithm == Algorithm::Rank(RankAlgorithm::ResourceAllocation)
            })
            .unwrap();
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
        assert_eq!(capability.algorithm.as_str(), "resource_allocation");
    }

    #[test]
    fn total_neighbors_aggregates_missing_directed_links_deterministically() {
        let graph = AdjacencyGraph::with_test_edges(
            5,
            &[
                (0, 2),
                (0, 2),
                (0, 3),
                (0, 0),
                (1, 2),
                (1, 3),
                (2, 0),
                (2, 4),
                (3, 4),
            ],
        );
        let output = execute_total_neighbors(
            &graph,
            AlgorithmLimits::default(),
            AlgorithmCancellation::default(),
        )
        .unwrap();
        assert_eq!(
            total_neighbor_output_scores(&output),
            [4.0, 4.0, 6.0, 8.0, 7.0]
        );
        assert_eq!(
            output,
            execute_total_neighbors(
                &graph,
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
        );
    }

    #[test]
    fn total_neighbors_obeys_undirected_and_boundary_contracts() {
        let undirected = AdjacencyGraph::with_test_edges(
            5,
            &[
                (0, 2),
                (2, 0),
                (0, 3),
                (3, 0),
                (1, 2),
                (2, 1),
                (1, 3),
                (3, 1),
                (2, 4),
                (4, 2),
                (3, 4),
                (4, 3),
            ],
        );
        assert_eq!(
            total_neighbor_output_scores(
                &execute_total_neighbors(
                    &undirected,
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap()
            ),
            [4.0, 4.0, 3.0, 3.0, 4.0]
        );
        assert_eq!(
            total_neighbor_output_scores(
                &execute_total_neighbors(
                    &AdjacencyGraph::with_test_edges(4, &[(0, 1), (1, 0), (2, 3), (3, 2)]),
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap()
            ),
            [4.0, 4.0, 4.0, 4.0]
        );
        assert_eq!(
            total_neighbor_output_scores(
                &execute_total_neighbors(
                    &AdjacencyGraph::with_test_edges(3, &[(0, 1)]),
                    AlgorithmLimits::default(),
                    AlgorithmCancellation::default(),
                )
                .unwrap()
            ),
            [1.0, 1.0, 1.0]
        );
        for graph in [
            AdjacencyGraph::with_test_edges(3, &[(0, 1), (1, 0), (0, 2), (2, 0), (1, 2), (2, 1)]),
            AdjacencyGraph::with_test_counts(3, 0),
        ] {
            assert!(
                total_neighbor_output_scores(
                    &execute_total_neighbors(
                        &graph,
                        AlgorithmLimits::default(),
                        AlgorithmCancellation::default(),
                    )
                    .unwrap(),
                )
                .into_iter()
                .all(|score| score == 0.0)
            );
        }
        assert!(
            execute_total_neighbors(
                &AdjacencyGraph::default(),
                AlgorithmLimits::default(),
                AlgorithmCancellation::default(),
            )
            .unwrap()
            .rows()
            .is_empty()
        );
    }

    #[test]
    fn total_neighbors_uses_shared_controls_and_dependency_metadata() {
        let graph = AdjacencyGraph::with_test_edges(3, &[(0, 1)]);
        assert!(matches!(
            execute_total_neighbors(
                &graph,
                AlgorithmLimits {
                    iterations: 0,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::IterationLimit { .. })
        ));
        assert!(matches!(
            execute_total_neighbors(
                &graph,
                AlgorithmLimits {
                    output_rows: 2,
                    ..AlgorithmLimits::default()
                },
                AlgorithmCancellation::default(),
            ),
            Err(AlgorithmError::OutputLimit { .. })
        ));
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        assert_eq!(
            execute_total_neighbors(&graph, AlgorithmLimits::default(), cancellation),
            Err(AlgorithmError::Cancelled)
        );
        assert!(matches!(
            exact_u64_as_f64((1_u64 << 53) + 1, "total-neighbors score"),
            Err(AlgorithmError::Execution { .. })
        ));
        let mut registry = AlgorithmRegistry::default();
        register_rank_algorithms(&mut registry).unwrap();
        let capability = registry
            .capabilities()
            .into_iter()
            .find(|capability| {
                capability.algorithm == Algorithm::Rank(RankAlgorithm::TotalNeighbors)
            })
            .unwrap();
        assert_eq!(capability.dependency, BUILTIN_REVIEW);
        assert_eq!(capability.algorithm.as_str(), "total_neighbors");
    }
}
